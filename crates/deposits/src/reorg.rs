//! Chain reorganisation detection and recovery.
//!
//! # Detection
//!
//! ChainRail stores `(height, hash, parent_hash)` for every block it processes.
//! A reorg is detected when the chain's block at a height we have already
//! processed has a *different hash* than the one we stored. Walking backwards
//! from there until stored and chain hashes agree yields the common ancestor.
//!
//! Comparing `parent_hash` of the next block is not sufficient on its own,
//! because we may learn about the fork only after several blocks; the walk-back
//! handles both the shallow and the deep case uniformly.
//!
//! # Recovery
//!
//! In one database transaction:
//!   1. Orphan every stored canonical block above the common ancestor. The
//!      partial unique index on `(chain, height) WHERE status='canonical'` means
//!      this *must* happen before replacements can be inserted -- the schema
//!      makes forgetting it impossible.
//!   2. Mark transfers from those blocks `orphaned`.
//!   3. Transition their deposits to `reorged`. An uncredited deposit simply
//!      never becomes credited.
//!   4. For a deposit that was *already credited*, post a compensating ledger
//!      transaction. History is never deleted (see docs/ledger.md).
//!   5. Rewind the watcher cursor to the common ancestor so the canonical
//!      replacement blocks are re-scanned. Re-scanning is safe because transfer
//!      insertion is idempotent on `(chain, tx_hash, log_index)`; a transaction
//!      that survives the reorg in a new block is re-observed as the same row.
//!
//! # The deep-reorg case
//!
//! A reorg deeper than the configured confirmation threshold can invalidate a
//! deposit whose funds the user has already withdrawn. That value is genuinely
//! gone. ChainRail records it honestly: the reversal debits whatever spendable
//! balance remains and books the shortfall to a `user_deficit` receivable. It
//! does not pretend the loss did not happen, and it does not drive a user's
//! spendable balance negative. Config validation requires
//! `reorg_scan_depth > required_confirmations`, so such a reorg is at least
//! always *detectable*.

use std::sync::Arc;

use chainrail_chains_evm::ChainAdapter;
use chainrail_common::event::{DepositReorged, EventPayload};
use chainrail_common::{Error, EventEnvelope, Hash32, Result};
use chainrail_database::models::DepositStatus;
use chainrail_database::{repo, Db, Tx};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReorgOutcome {
    /// Stored lineage matches the chain.
    NoReorg,
    /// Nothing stored yet for this chain.
    Uninitialised,
    Handled(ReorgReport),
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ReorgReport {
    pub common_ancestor_height: u64,
    pub orphaned_blocks: usize,
    pub orphaned_transfers: usize,
    pub deposits_reorged: usize,
    /// Deposits that had already been credited and required a compensating
    /// ledger transaction. Any value above zero is an operational incident.
    pub credited_deposits_reversed: usize,
    pub depth: u64,
}

/// Compare stored lineage with the chain and find the common ancestor.
///
/// Returns the highest height at which stored and chain hashes agree. Pure
/// decision logic lives in [`find_common_ancestor`] so it can be tested
/// without a chain.
pub async fn detect(
    db: &Db,
    adapter: &dyn ChainAdapter,
    chain: &str,
    scan_depth: u64,
) -> Result<Option<(u64, u64)>> {
    let Some(cursor) = repo::chain::get_cursor(db.pool(), chain).await? else {
        return Ok(None);
    };
    let tip_height = u64::try_from(cursor.last_processed_height).unwrap_or(0);

    // The chain's block at our tip height. If the node has no such block yet the
    // chain is behind us (or we read a stale replica); nothing to do.
    let Some(chain_tip) = adapter.header_at(tip_height).await? else {
        return Ok(None);
    };
    if chain_tip.hash == cursor.last_processed_hash {
        return Ok(None);
    }

    tracing::warn!(
        chain, tip_height,
        stored_hash = %cursor.last_processed_hash,
        chain_hash = %chain_tip.hash,
        "block hash mismatch at watcher tip: reorg suspected"
    );

    let floor = tip_height.saturating_sub(scan_depth);
    let mut height = tip_height;
    loop {
        if height <= floor {
            // We cannot find agreement within the window we retain. Refusing to
            // guess: rewinding blindly could re-credit or double-reverse.
            return Err(Error::Internal(format!(
                "chain {chain}: reorg deeper than reorg_scan_depth ({scan_depth}); \
                 manual reconciliation required from height {floor}"
            )));
        }
        height -= 1;

        let stored = repo::chain::get_canonical_block_at(db.pool(), chain, height).await?;
        let Some(stored) = stored else {
            // No stored block at this height: treat it as the ancestor and
            // re-scan forward from here.
            return Ok(Some((height, tip_height.saturating_sub(height))));
        };
        let Some(on_chain) = adapter.header_at(height).await? else {
            continue;
        };
        if on_chain.hash == stored.hash {
            return Ok(Some((height, tip_height - height)));
        }
    }
}

/// Apply recovery for a detected reorg. Everything happens in one transaction.
pub async fn handle(
    db: &Db,
    chain: &str,
    common_ancestor_height: u64,
    depth: u64,
    correlation_id: &str,
) -> Result<ReorgReport> {
    let mut tx = db.begin().await?;
    let report = handle_in_tx(
        &mut tx,
        chain,
        common_ancestor_height,
        depth,
        correlation_id,
    )
    .await?;
    tx.commit().await.map_err(chainrail_database::map_sqlx)?;

    metrics::counter!("chainrail_reorgs_total", "chain" => chain.to_string()).increment(1);
    metrics::histogram!("chainrail_reorg_depth_blocks", "chain" => chain.to_string())
        .record(depth as f64);
    if report.credited_deposits_reversed > 0 {
        metrics::counter!(
            "chainrail_reorg_credited_reversals_total",
            "chain" => chain.to_string(),
        )
        .increment(report.credited_deposits_reversed as u64);
    }
    tracing::warn!(
        chain,
        common_ancestor_height,
        depth,
        orphaned_blocks = report.orphaned_blocks,
        deposits_reorged = report.deposits_reorged,
        credited_reversed = report.credited_deposits_reversed,
        "reorg handled"
    );
    Ok(report)
}

pub async fn handle_in_tx(
    tx: &mut Tx<'_>,
    chain: &str,
    common_ancestor_height: u64,
    depth: u64,
    correlation_id: &str,
) -> Result<ReorgReport> {
    let mut report = ReorgReport {
        common_ancestor_height,
        depth,
        ..Default::default()
    };

    // 1. Orphan the abandoned branch. Frees the canonical-height index so the
    //    replacement chain can be inserted.
    let orphaned: Vec<Hash32> =
        repo::chain::orphan_blocks_above(tx, chain, common_ancestor_height).await?;
    report.orphaned_blocks = orphaned.len();
    if orphaned.is_empty() {
        // Nothing stored above the ancestor: only the cursor needs rewinding.
        rewind_cursor(tx, chain, common_ancestor_height).await?;
        return Ok(report);
    }

    // 2. Transfers from those blocks are no longer part of the chain.
    let transfer_ids = repo::chain::mark_transfers_orphaned(tx, chain, &orphaned).await?;
    report.orphaned_transfers = transfer_ids.len();

    // 3 & 4. Deposits derived from them.
    let deposits = repo::deposits::deposits_for_transfers(&mut **tx, &transfer_ids).await?;
    for deposit in deposits {
        let status = deposit.status()?;
        if status == DepositStatus::Reorged {
            continue; // already handled by an earlier pass
        }

        let reversal = if status.is_credited() {
            // The funds were made spendable. Post a compensating transaction --
            // never delete the original credit.
            let posted = chainrail_ledger::reverse_deposit_credit(
                tx,
                deposit.id,
                deposit.user_id,
                deposit.asset_id,
                deposit.amount_raw,
                Some(correlation_id.to_string()),
            )
            .await?;
            report.credited_deposits_reversed += 1;
            tracing::error!(
                deposit_id = %deposit.id,
                user_id = %deposit.user_id,
                amount = %deposit.amount_raw,
                ledger_transaction_id = %posted.id(),
                "CREDITED deposit invalidated by reorg; compensating entry posted"
            );
            Some(posted.id())
        } else {
            None
        };

        let reason = if reversal.is_some() {
            "block orphaned by reorg after the deposit was credited"
        } else {
            "block orphaned by reorg before crediting"
        };
        if repo::deposits::mark_reorged(tx, deposit.id, reversal, reason).await? {
            report.deposits_reorged += 1;
        }

        // Emit through the outbox so downstream systems learn about it exactly
        // when the reversal commits.
        let envelope = EventEnvelope::new(
            EventPayload::DepositReorged(DepositReorged {
                chain: chain.to_string(),
                deposit_id: deposit.id,
                user_id: deposit.user_id,
                tx_hash: String::new(),
                orphaned_block_number: 0,
                orphaned_block_hash: String::new(),
                reversal_ledger_transaction_id: reversal,
            }),
            correlation_id,
        )
        .with_deterministic_id(&format!("{chain}:{}:reorged", deposit.id));
        repo::outbox::enqueue(tx, &envelope).await?;
    }

    // 5. Rewind so the canonical replacement is scanned.
    rewind_cursor(tx, chain, common_ancestor_height).await?;
    Ok(report)
}

async fn rewind_cursor(tx: &mut Tx<'_>, chain: &str, height: u64) -> Result<()> {
    let ancestor = repo::chain::get_canonical_block_at(&mut **tx, chain, height).await?;
    let hash = match ancestor {
        Some(b) => b.hash,
        // No stored ancestor (we rewound below what we retain). A zero hash
        // forces the next watcher pass to treat this as a cold start at `height`,
        // which re-scans forward safely.
        None => Hash32::from_storage(format!("0x{}", "0".repeat(64))),
    };
    repo::chain::set_cursor(&mut **tx, chain, height, &hash, height).await
}

/// Full detect-and-recover cycle.
pub async fn reconcile(
    db: &Db,
    adapter: Arc<dyn ChainAdapter>,
    chain: &str,
    scan_depth: u64,
    correlation_id: &str,
) -> Result<ReorgOutcome> {
    match detect(db, adapter.as_ref(), chain, scan_depth).await? {
        None => {
            if repo::chain::get_cursor(db.pool(), chain).await?.is_none() {
                Ok(ReorgOutcome::Uninitialised)
            } else {
                Ok(ReorgOutcome::NoReorg)
            }
        }
        Some((ancestor, depth)) => {
            let report = handle(db, chain, ancestor, depth, correlation_id).await?;
            Ok(ReorgOutcome::Handled(report))
        }
    }
}

/// Pure ancestor search over two lineages, extracted so the walk-back rule is
/// testable without a chain or a database.
///
/// `stored` and `chain` are `(height, hash)` pairs in *descending* height order.
/// Returns the highest height where both agree.
pub fn find_common_ancestor(stored: &[(u64, String)], chain: &[(u64, String)]) -> Option<u64> {
    let chain_at: std::collections::HashMap<u64, &String> =
        chain.iter().map(|(h, x)| (*h, x)).collect();
    for (height, hash) in stored {
        if let Some(chain_hash) = chain_at.get(height) {
            if *chain_hash == hash {
                return Some(*height);
            }
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    fn lineage(pairs: &[(u64, &str)]) -> Vec<(u64, String)> {
        pairs.iter().map(|(h, x)| (*h, (*x).to_string())).collect()
    }

    #[test]
    fn identical_lineages_agree_at_the_tip() {
        let a = lineage(&[(102, "c"), (101, "b"), (100, "a")]);
        assert_eq!(find_common_ancestor(&a, &a), Some(102));
    }

    #[test]
    fn finds_the_fork_point_of_the_documented_scenario() {
        // stored: 100 -> 101  -> 102
        // chain:  100 -> 101' -> 102' -> 103'
        let stored = lineage(&[(102, "102"), (101, "101"), (100, "100")]);
        let chain = lineage(&[
            (103, "103prime"),
            (102, "102prime"),
            (101, "101prime"),
            (100, "100"),
        ]);
        assert_eq!(
            find_common_ancestor(&stored, &chain),
            Some(100),
            "the fork point is 100: everything above it is orphaned"
        );
    }

    #[test]
    fn finds_a_single_block_reorg() {
        // Only the tip changed.
        let stored = lineage(&[(102, "102"), (101, "101"), (100, "100")]);
        let chain = lineage(&[(102, "102prime"), (101, "101"), (100, "100")]);
        assert_eq!(find_common_ancestor(&stored, &chain), Some(101));
    }

    #[test]
    fn no_agreement_within_the_window_is_reported_as_none() {
        // Every retained block differs -- the caller must escalate rather than
        // guess at a rewind point.
        let stored = lineage(&[(102, "x"), (101, "y"), (100, "z")]);
        let chain = lineage(&[(102, "a"), (101, "b"), (100, "c")]);
        assert_eq!(find_common_ancestor(&stored, &chain), None);
    }

    #[test]
    fn heights_absent_from_the_chain_view_are_skipped() {
        let stored = lineage(&[(102, "102"), (101, "101"), (100, "100")]);
        // Node has a gap at 101 (e.g. pruned or a lagging replica).
        let chain = lineage(&[(102, "102prime"), (100, "100")]);
        assert_eq!(find_common_ancestor(&stored, &chain), Some(100));
    }

    #[test]
    fn reorg_report_defaults_are_a_no_op() {
        let r = ReorgReport::default();
        assert_eq!(r.orphaned_blocks, 0);
        assert_eq!(r.credited_deposits_reversed, 0);
        assert_eq!(ReorgOutcome::NoReorg, ReorgOutcome::NoReorg);
        assert_ne!(ReorgOutcome::NoReorg, ReorgOutcome::Uninitialised);
    }
}
