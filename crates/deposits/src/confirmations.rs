//! Confirmation engine.
//!
//! Advances deposits `observed -> confirming -> confirmed` as the chain head
//! moves past their including block, then emits `deposit.confirmed`. Crediting
//! is a separate consumer (see [`crate::credit`]) so that the decision "is this
//! final?" and the decision "move the money" are independently retryable.
//!
//! Confirmation is recomputed from `(head, including_block)` on every pass
//! rather than incremented, so a restart, a duplicate pass, or a rewound head
//! all converge on the same answer.

use std::sync::Arc;
use std::time::Duration;

use chainrail_chains_evm::ChainAdapter;
use chainrail_common::event::{DepositConfirmed, EventPayload};
use chainrail_common::{EventEnvelope, FinalityPolicy, Result};
use chainrail_database::models::DepositStatus;
use chainrail_database::{repo, Db};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct ConfirmationPass {
    pub examined: usize,
    pub advanced_to_confirming: usize,
    pub advanced_to_confirmed: usize,
    /// Deposits whose block is no longer canonical. Left for the reorg engine.
    pub non_canonical: usize,
}

impl ConfirmationPass {
    pub fn did_work(&self) -> bool {
        self.advanced_to_confirming + self.advanced_to_confirmed > 0
    }
}

pub struct ConfirmationEngine {
    db: Db,
    adapter: Arc<dyn ChainAdapter>,
    chain: String,
    policy: FinalityPolicy,
    batch_size: i64,
    interval: Duration,
}

impl ConfirmationEngine {
    pub fn new(
        db: Db,
        adapter: Arc<dyn ChainAdapter>,
        chain: &str,
        policy: FinalityPolicy,
        interval: Duration,
    ) -> Self {
        ConfirmationEngine {
            db,
            adapter,
            chain: chain.to_string(),
            policy,
            batch_size: 200,
            interval,
        }
    }

    pub async fn run_once(&self) -> Result<ConfirmationPass> {
        let head = self.adapter.head().await?.height;
        let correlation_id = format!("confirm-{}-{}", self.chain, Uuid::now_v7());

        let mut tx = self.db.begin().await?;
        let pending =
            repo::deposits::claim_pending_deposits(&mut tx, &self.chain, self.batch_size).await?;
        let mut pass = ConfirmationPass {
            examined: pending.len(),
            ..Default::default()
        };

        for d in pending {
            let status = DepositStatus::from_str_public(&d.status)?;
            let block_height = u64::try_from(d.block_number).unwrap_or(0);

            // The deposit's block must still be canonical. Confirming a deposit
            // from an orphaned block would credit funds that do not exist.
            let canonical =
                repo::chain::get_canonical_block_at(&mut *tx, &self.chain, block_height).await?;
            let still_canonical = canonical.map(|b| b.hash == d.block_hash).unwrap_or(false);
            if !still_canonical {
                pass.non_canonical += 1;
                tracing::warn!(
                    chain = %self.chain, deposit_id = %d.deposit_id,
                    block_number = block_height, block_hash = %d.block_hash,
                    "deposit block is not canonical; leaving it for the reorg engine"
                );
                continue;
            }

            let confirmations = FinalityPolicy::confirmations_for(head, block_height);
            let confirmed = self.policy.is_confirmed(head, block_height);

            if confirmed && status != DepositStatus::Confirmed {
                if repo::deposits::update_confirmations(
                    &mut *tx,
                    d.deposit_id,
                    confirmations,
                    status,
                    DepositStatus::Confirmed,
                )
                .await?
                {
                    pass.advanced_to_confirmed += 1;
                    let envelope = EventEnvelope::new(
                        EventPayload::DepositConfirmed(DepositConfirmed {
                            chain: self.chain.clone(),
                            deposit_id: d.deposit_id,
                            user_id: d.user_id,
                            tx_hash: d.tx_hash.to_string(),
                            block_number: block_height,
                            confirmations,
                            asset: d.asset_symbol.clone(),
                            amount_raw: d.amount_raw,
                        }),
                        &correlation_id,
                    )
                    // Deterministic: re-confirming the same deposit must not
                    // enqueue a second credit instruction.
                    .with_deterministic_id(&format!("{}:{}", self.chain, d.deposit_id));
                    repo::outbox::enqueue(&mut tx, &envelope).await?;

                    tracing::info!(
                        chain = %self.chain, deposit_id = %d.deposit_id,
                        confirmations, head, "deposit confirmed"
                    );
                }
            } else if !confirmed && status == DepositStatus::Observed {
                if repo::deposits::update_confirmations(
                    &mut *tx,
                    d.deposit_id,
                    confirmations,
                    DepositStatus::Observed,
                    DepositStatus::Confirming,
                )
                .await?
                {
                    pass.advanced_to_confirming += 1;
                }
            } else if !confirmed {
                // Still confirming: record progress so the API can show it.
                repo::deposits::update_confirmations(
                    &mut *tx,
                    d.deposit_id,
                    confirmations,
                    DepositStatus::Confirming,
                    DepositStatus::Confirming,
                )
                .await?;
            }
        }

        tx.commit().await.map_err(chainrail_database::map_sqlx)?;
        Ok(pass)
    }

    pub async fn run(self: Arc<Self>, cancel: CancellationToken) {
        tracing::info!(chain = %self.chain, "confirmation engine started");
        let backoff = chainrail_common::retry::Backoff::new(500, 30_000, u32::MAX);
        let mut errors: u32 = 0;
        loop {
            if cancel.is_cancelled() {
                break;
            }
            match self.run_once().await {
                Ok(pass) => {
                    errors = 0;
                    if pass.did_work() {
                        tracing::debug!(
                            chain = %self.chain,
                            confirming = pass.advanced_to_confirming,
                            confirmed = pass.advanced_to_confirmed,
                            "confirmation pass"
                        );
                    }
                    tokio::select! {
                        _ = tokio::time::sleep(self.interval) => {}
                        _ = cancel.cancelled() => break,
                    }
                }
                Err(e) => {
                    errors += 1;
                    let delay = backoff.delay(errors.min(8));
                    tracing::error!(chain = %self.chain, error = %e, ?delay, "confirmation pass failed");
                    tokio::select! {
                        _ = tokio::time::sleep(delay) => {}
                        _ = cancel.cancelled() => break,
                    }
                }
            }
        }
        tracing::info!(chain = %self.chain, "confirmation engine stopped");
    }
}

/// Helper so the engine can parse a status string without importing `FromStr`
/// everywhere.
trait StatusParse: Sized {
    fn from_str_public(s: &str) -> Result<Self>;
}

impl StatusParse for DepositStatus {
    fn from_str_public(s: &str) -> Result<Self> {
        use std::str::FromStr;
        DepositStatus::from_str(s)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn confirmation_pass_reports_progress_only_for_advances() {
        assert!(!ConfirmationPass::default().did_work());
        assert!(!ConfirmationPass {
            examined: 10,
            non_canonical: 10,
            ..Default::default()
        }
        .did_work());
        assert!(ConfirmationPass {
            advanced_to_confirmed: 1,
            ..Default::default()
        }
        .did_work());
        assert!(ConfirmationPass {
            advanced_to_confirming: 1,
            ..Default::default()
        }
        .did_work());
    }

    #[test]
    fn confirmation_threshold_is_inclusive_of_the_including_block() {
        let policy = FinalityPolicy::Confirmations { blocks: 10 };
        // Block 100 included; head 109 gives 10 confirmations.
        assert_eq!(FinalityPolicy::confirmations_for(109, 100), 10);
        assert!(policy.is_confirmed(109, 100));
        assert!(!policy.is_confirmed(108, 100));
    }

    #[test]
    fn recomputing_from_head_is_idempotent() {
        // The engine never increments a counter; it recomputes. So a repeated
        // pass at the same head yields the same answer, which is what makes a
        // restart or a duplicate pass harmless.
        let policy = FinalityPolicy::Confirmations { blocks: 12 };
        for _ in 0..3 {
            assert_eq!(FinalityPolicy::confirmations_for(200, 100), 101);
            assert!(policy.is_confirmed(200, 100));
        }
    }

    #[test]
    fn a_rewound_head_does_not_underflow_or_unconfirm_arithmetically() {
        // Stale replica reports a head below the deposit's block.
        assert_eq!(FinalityPolicy::confirmations_for(99, 100), 1);
        assert!(!FinalityPolicy::Confirmations { blocks: 10 }.is_confirmed(99, 100));
    }
}
