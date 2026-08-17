//! The block watcher.
//!
//! One task per chain. Each iteration:
//!   1. reconcile lineage (reorg detection) -- *before* scanning anything new,
//!      so a fork is never built on top of;
//!   2. compute the next block range from the cursor, bounded by the batch size;
//!   3. scan it through the chain adapter;
//!   4. persist atomically and advance the cursor.
//!
//! Blocks are processed in strictly ascending order and never skipped: the
//! cursor only advances as part of the transaction that persisted the batch.

use std::sync::Arc;
use std::time::Duration;

use chainrail_chains_evm::{ChainAdapter, ScanRequest};
use chainrail_common::config::ChainConfig;
use chainrail_common::Result;
use chainrail_database::{repo, Db};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::context::ChainContext;
use crate::engine::{self, PersistStats};
use crate::reorg::{self, ReorgOutcome};

pub struct Watcher {
    db: Db,
    adapter: Arc<dyn ChainAdapter>,
    ctx: Arc<ChainContext>,
    chain: String,
    poll_interval: Duration,
    batch_size: u64,
    reorg_scan_depth: u64,
    start_block: Option<u64>,
    /// Confirmations required, used to bound how far behind head we scan when
    /// deciding whether we are caught up.
    required_confirmations: u64,
}

/// Result of one watcher iteration, so the loop and tests share one code path.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Tick {
    /// No new blocks.
    Idle {
        head: u64,
        cursor: u64,
    },
    Scanned {
        from: u64,
        to: u64,
        head: u64,
        stats: PersistStats,
    },
    Reorged(reorg::ReorgReport),
    /// Cold start: the cursor was initialised without scanning.
    Initialised {
        at: u64,
    },
}

impl Watcher {
    pub fn new(
        db: Db,
        adapter: Arc<dyn ChainAdapter>,
        ctx: Arc<ChainContext>,
        cfg: &ChainConfig,
    ) -> Self {
        Watcher {
            db,
            adapter,
            ctx,
            chain: cfg.id.to_string(),
            poll_interval: cfg.poll_interval(),
            batch_size: cfg.block_batch_size.max(1),
            reorg_scan_depth: cfg.reorg_scan_depth,
            start_block: cfg.start_block,
            required_confirmations: cfg.finality.required_confirmations().unwrap_or(1),
        }
    }

    pub fn chain(&self) -> &str {
        &self.chain
    }

    /// One iteration. Safe to call repeatedly; safe to abandon mid-way.
    pub async fn tick(&self) -> Result<Tick> {
        let correlation_id = format!("watcher-{}-{}", self.chain, Uuid::now_v7());
        self.ctx.refresh_if_stale().await?;

        let head = self.adapter.head().await?;
        metrics::gauge!("chainrail_chain_head_height", "chain" => self.chain.clone())
            .set(head.height as f64);

        // Reorg check first. Scanning forward on a stale lineage would persist
        // blocks whose parents we believe in but the chain does not.
        match reorg::reconcile(
            &self.db,
            Arc::clone(&self.adapter),
            &self.chain,
            self.reorg_scan_depth,
            &correlation_id,
        )
        .await?
        {
            ReorgOutcome::Handled(report) => return Ok(Tick::Reorged(report)),
            ReorgOutcome::Uninitialised => {
                let at = self.initialise_cursor(head.height).await?;
                return Ok(Tick::Initialised { at });
            }
            ReorgOutcome::NoReorg => {}
        }

        let cursor = repo::chain::get_cursor(self.db.pool(), &self.chain)
            .await?
            .map(|c| u64::try_from(c.last_processed_height).unwrap_or(0))
            .unwrap_or(0);

        if cursor >= head.height {
            self.report_lag(head.height, cursor);
            return Ok(Tick::Idle {
                head: head.height,
                cursor,
            });
        }

        let from = cursor + 1;
        let to = head.height.min(from + self.batch_size - 1);

        let tracked = self.ctx.tracked_addresses();
        let scan = self
            .adapter
            .scan(ScanRequest {
                from_height: from,
                to_height: to,
                tracked_addresses: &tracked,
                assets: self.ctx.tracked_assets(),
            })
            .await?;

        let stats =
            engine::persist_scan(&self.db, &self.ctx, &scan, head.height, &correlation_id).await?;
        self.report_lag(head.height, to);

        // Bounded retention: block metadata older than the reorg window cannot
        // be needed again, and unbounded growth is its own outage.
        if to > self.reorg_scan_depth * 4 {
            let prune_below = to - self.reorg_scan_depth * 4;
            if let Err(e) =
                repo::chain::prune_blocks_below(self.db.pool(), &self.chain, prune_below).await
            {
                tracing::warn!(chain = %self.chain, error = %e, "block pruning failed (non-fatal)");
            }
        }

        Ok(Tick::Scanned {
            from,
            to,
            head: head.height,
            stats,
        })
    }

    /// Cold start. Defaults to the current head minus the confirmation window,
    /// so a fresh deployment does not silently miss deposits that are already
    /// in flight but not yet confirmed.
    async fn initialise_cursor(&self, head: u64) -> Result<u64> {
        let at = match self.start_block {
            Some(b) => b.saturating_sub(1),
            None => head.saturating_sub(self.required_confirmations),
        };
        let hash = match self.adapter.header_at(at).await? {
            Some(h) => h.hash,
            None => self.adapter.head().await?.hash,
        };
        let mut tx = self.db.begin().await?;
        repo::chain::insert_canonical_block(
            &mut *tx,
            &self.chain,
            &hash,
            at,
            &chainrail_common::Hash32::from_storage(format!("0x{}", "0".repeat(64))),
        )
        .await?;
        repo::chain::set_cursor(&mut *tx, &self.chain, at, &hash, head).await?;
        tx.commit().await.map_err(chainrail_database::map_sqlx)?;

        tracing::info!(chain = %self.chain, at, head, "watcher cursor initialised");
        Ok(at)
    }

    fn report_lag(&self, head: u64, cursor: u64) {
        metrics::gauge!("chainrail_watcher_lag_blocks", "chain" => self.chain.clone())
            .set(head.saturating_sub(cursor) as f64);
    }

    pub async fn run(self: Arc<Self>, cancel: CancellationToken) {
        tracing::info!(chain = %self.chain, "watcher started");
        // Fail fast if the node is not the chain we think it is: signing with
        // the wrong chain id, or crediting deposits from the wrong network, is
        // far worse than refusing to start.
        if let Err(e) = self.adapter.verify_identity().await {
            tracing::error!(chain = %self.chain, error = %e, "chain identity verification failed; watcher will not start");
            return;
        }

        let mut consecutive_errors: u32 = 0;
        let backoff = chainrail_common::retry::Backoff::new(500, 30_000, u32::MAX);
        loop {
            if cancel.is_cancelled() {
                break;
            }
            match self.tick().await {
                Ok(tick) => {
                    consecutive_errors = 0;
                    let caught_up = match &tick {
                        Tick::Idle { .. } | Tick::Initialised { .. } => true,
                        Tick::Scanned { to, head, .. } => to >= head,
                        Tick::Reorged(_) => false,
                    };
                    if let Tick::Scanned {
                        from, to, stats, ..
                    } = &tick
                    {
                        if stats.deposits_created > 0 || stats.blocks > 0 {
                            tracing::debug!(
                                chain = %self.chain, from, to,
                                blocks = stats.blocks,
                                deposits = stats.deposits_created,
                                duplicates = stats.duplicates_skipped,
                                "blocks processed"
                            );
                        }
                    }
                    // Only sleep once caught up; otherwise drain the backlog.
                    if caught_up {
                        tokio::select! {
                            _ = tokio::time::sleep(self.poll_interval) => {}
                            _ = cancel.cancelled() => break,
                        }
                    }
                }
                Err(e) => {
                    consecutive_errors += 1;
                    let delay = backoff.delay(consecutive_errors.min(8));
                    metrics::counter!(
                        "chainrail_watcher_errors_total",
                        "chain" => self.chain.clone(),
                    )
                    .increment(1);
                    tracing::error!(
                        chain = %self.chain, consecutive_errors, ?delay, error = %e,
                        "watcher tick failed"
                    );
                    tokio::select! {
                        _ = tokio::time::sleep(delay) => {}
                        _ = cancel.cancelled() => break,
                    }
                }
            }
        }
        tracing::info!(chain = %self.chain, "watcher stopped");
    }
}

/// Next scan range from a cursor, head and batch size. Extracted so the
/// arithmetic -- an easy place to introduce an off-by-one that skips a block --
/// is directly testable.
pub fn next_range(cursor: u64, head: u64, batch_size: u64) -> Option<(u64, u64)> {
    if cursor >= head {
        return None;
    }
    let from = cursor + 1;
    let to = head.min(from + batch_size.max(1) - 1);
    Some((from, to))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn range_starts_immediately_after_the_cursor() {
        assert_eq!(next_range(100, 110, 50), Some((101, 110)));
    }

    #[test]
    fn range_is_bounded_by_the_batch_size() {
        assert_eq!(next_range(100, 1_000, 10), Some((101, 110)));
        assert_eq!(next_range(0, 1_000, 1), Some((1, 1)));
    }

    #[test]
    fn no_range_when_caught_up_or_ahead() {
        assert_eq!(next_range(110, 110, 50), None);
        // Head behind the cursor: a stale RPC replica. Must not scan backwards.
        assert_eq!(next_range(110, 105, 50), None);
    }

    #[test]
    fn single_new_block_produces_a_one_block_range() {
        assert_eq!(next_range(100, 101, 50), Some((101, 101)));
    }

    #[test]
    fn ranges_tile_the_chain_without_gaps_or_overlaps() {
        // The property that actually matters: iterating with the batch size must
        // visit every height exactly once.
        let head = 237u64;
        let mut cursor = 0u64;
        let mut visited = Vec::new();
        while let Some((from, to)) = next_range(cursor, head, 25) {
            visited.extend(from..=to);
            cursor = to;
        }
        assert_eq!(visited.len() as u64, head);
        assert_eq!(visited.first(), Some(&1));
        assert_eq!(visited.last(), Some(&head));
        // Strictly ascending with no repeats.
        assert!(visited.windows(2).all(|w| w[1] == w[0] + 1));
    }

    #[test]
    fn zero_batch_size_is_treated_as_one_rather_than_stalling() {
        assert_eq!(next_range(10, 20, 0), Some((11, 11)));
    }
}
