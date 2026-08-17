//! The asynchronous withdrawal pipeline: sign, broadcast, confirm, settle.
//!
//! # The "broadcast succeeded but the database write failed" problem
//!
//! Broadcasting is an irreversible side effect on a system we do not control.
//! The only safe ordering is:
//!
//!   1. Allocate the nonce and persist the *signed transaction's hash* —
//!      **commit**. Signing is deterministic, so the hash is known before the
//!      transaction ever leaves the process.
//!   2. Broadcast.
//!   3. Record the broadcast — commit.
//!
//! A crash between (1) and (3) leaves a withdrawal in `signing` with a durable
//! `tx_hash`. Recovery is then a *lookup*, not a guess: ask the chain whether
//! that hash exists. If it does, move forward; if not, re-sign (same nonce, same
//! bytes, same hash) and re-broadcast. Because the nonce comes from the database
//! and signing is deterministic, retrying can never produce a second transfer.
//!
//! The reverse ordering — broadcast then persist — would lose the hash on a
//! crash and strand funds with no way to reconcile them.

use std::sync::Arc;
use std::time::Duration;

use chainrail_chains_evm::{ChainAdapter, TxStatus, WithdrawalIntent};
use chainrail_common::config::AppConfig;
use chainrail_common::event::{
    EventPayload, WithdrawalBroadcast, WithdrawalCompleted, WithdrawalFailed,
};
use chainrail_common::{Address, Amount, Error, EventEnvelope, Result};
use chainrail_database::models::{Withdrawal, WithdrawalStatus};
use chainrail_database::{repo, Db};
use chainrail_signer::SharedSigner;
use std::collections::HashMap;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

pub struct WithdrawalPipeline {
    db: Db,
    cfg: Arc<AppConfig>,
    signer: SharedSigner,
    adapters: HashMap<String, Arc<dyn ChainAdapter>>,
    batch_size: i64,
    interval: Duration,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct PipelinePass {
    pub signed: usize,
    pub broadcast: usize,
    pub completed: usize,
    pub failed: usize,
    pub recovered: usize,
    pub still_pending: usize,
}

impl PipelinePass {
    pub fn did_work(&self) -> bool {
        self.signed + self.broadcast + self.completed + self.failed + self.recovered > 0
    }
}

impl WithdrawalPipeline {
    pub fn new(
        db: Db,
        cfg: Arc<AppConfig>,
        signer: SharedSigner,
        adapters: HashMap<String, Arc<dyn ChainAdapter>>,
        interval: Duration,
    ) -> Arc<Self> {
        Arc::new(WithdrawalPipeline {
            db,
            cfg,
            signer,
            adapters,
            batch_size: 25,
            interval,
        })
    }

    fn adapter(&self, chain: &str) -> Result<Arc<dyn ChainAdapter>> {
        self.adapters
            .get(chain)
            .cloned()
            .ok_or_else(|| Error::UnsupportedChain(chain.to_string()))
    }

    /// One full pass over every configured chain.
    pub async fn run_once(&self) -> Result<PipelinePass> {
        let mut total = PipelinePass::default();
        for chain_cfg in &self.cfg.chains {
            let chain = chain_cfg.id.as_str();
            // Recovery first: a withdrawal stuck in `signing` may already be on
            // chain, and must not be re-signed with a fresh nonce.
            let recovered = self.recover_signing(chain).await?;
            total.recovered += recovered;

            let pass = self.process_approved(chain).await?;
            total.signed += pass.signed;
            total.broadcast += pass.broadcast;
            total.failed += pass.failed;

            let confirm = self.process_in_flight(chain).await?;
            total.completed += confirm.completed;
            total.failed += confirm.failed;
            total.still_pending += confirm.still_pending;
        }
        Ok(total)
    }

    /// `approved` -> `signing` -> `broadcast`.
    pub async fn process_approved(&self, chain: &str) -> Result<PipelinePass> {
        let adapter = self.adapter(chain)?;
        let chain_cfg = crate::service::chain_config(&self.cfg, chain)?;
        let hot_wallet = chain_cfg
            .hot_wallet_address
            .as_deref()
            .ok_or_else(|| Error::Config(format!("chain {chain} has no hot wallet")))?;
        let hot_wallet = Address::from_storage(hot_wallet.to_ascii_lowercase());

        let mut tx = self.db.begin().await?;
        let claimed = repo::withdrawals::claim_by_status(
            &mut tx,
            chain,
            WithdrawalStatus::Approved,
            self.batch_size,
        )
        .await?;
        if claimed.is_empty() {
            tx.commit().await.ok();
            return Ok(PipelinePass::default());
        }
        // Release the claim: each withdrawal is then processed in its own
        // transaction so one failure does not roll back the whole batch.
        let ids: Vec<Uuid> = claimed.iter().map(|w| w.id).collect();
        tx.rollback().await.ok();

        let mut pass = PipelinePass::default();
        for id in ids {
            match self.sign_and_broadcast(id, &adapter, &hot_wallet).await {
                Ok(true) => {
                    pass.signed += 1;
                    pass.broadcast += 1;
                }
                Ok(false) => {}
                Err(e) => {
                    tracing::error!(withdrawal_id = %id, chain, error = %e, "withdrawal broadcast failed");
                    if self.fail_before_broadcast(id, &e).await? {
                        pass.failed += 1;
                    }
                }
            }
        }
        Ok(pass)
    }

    async fn sign_and_broadcast(
        &self,
        withdrawal_id: Uuid,
        adapter: &Arc<dyn ChainAdapter>,
        hot_wallet: &Address,
    ) -> Result<bool> {
        // --- phase 1: allocate the nonce, sign, persist the hash, COMMIT ---
        let mut tx = self.db.begin().await?;
        let w = repo::withdrawals::lock_withdrawal(&mut tx, withdrawal_id).await?;
        if w.status()? != WithdrawalStatus::Approved {
            tx.rollback().await.ok();
            return Ok(false); // another worker took it
        }
        if !repo::withdrawals::transition(
            &mut tx,
            w.id,
            WithdrawalStatus::Approved,
            WithdrawalStatus::Signing,
        )
        .await?
        {
            tx.rollback().await.ok();
            return Ok(false);
        }

        let asset = repo::reference::get_asset_by_id(&mut *tx, w.asset_id).await?;
        // Chain nonce as a hint only; `allocate_nonce` never moves backwards and
        // is atomic, so two signers cannot receive the same nonce.
        let hint = adapter.nonce_hint(hot_wallet).await.unwrap_or(0);
        let nonce =
            repo::withdrawals::allocate_nonce(&mut tx, &w.chain, &hot_wallet.storage_key(), hint)
                .await?;

        let intent = WithdrawalIntent {
            destination: w.destination_address.clone(),
            amount: w.amount_raw,
            contract: asset.contract_address.as_deref().map(Address::from_storage),
            nonce,
            from: hot_wallet.clone(),
        };
        let unsigned = adapter.build_withdrawal(&intent).await?;
        let signed = self.signer.sign(&unsigned).await?;

        repo::withdrawals::record_signed_transaction(
            &mut tx,
            w.id,
            repo::withdrawals::SignedTxFacts {
                tx_hash: &signed.tx_hash,
                nonce,
                gas_limit: match &unsigned {
                    chainrail_signer::UnsignedTransaction::Evm(r) => r.gas_limit,
                },
                max_fee_per_gas: match &unsigned {
                    chainrail_signer::UnsignedTransaction::Evm(r) => r.max_fee_per_gas,
                },
                max_priority_fee_per_gas: match &unsigned {
                    chainrail_signer::UnsignedTransaction::Evm(r) => r.max_priority_fee_per_gas,
                },
            },
        )
        .await?;
        // COMMIT BEFORE BROADCAST. This is the crash-safety hinge.
        tx.commit().await.map_err(chainrail_database::map_sqlx)?;

        tracing::info!(
            withdrawal_id = %w.id, tx_hash = %signed.tx_hash, nonce,
            "withdrawal signed; hash persisted before broadcast"
        );

        // --- phase 2: broadcast ---
        adapter.broadcast(&signed).await?;

        // --- phase 3: record it ---
        self.mark_broadcast(w.id, &signed.tx_hash, nonce).await?;
        Ok(true)
    }

    async fn mark_broadcast(
        &self,
        withdrawal_id: Uuid,
        tx_hash: &chainrail_common::Hash32,
        nonce: u64,
    ) -> Result<()> {
        let mut tx = self.db.begin().await?;
        let w = repo::withdrawals::lock_withdrawal(&mut tx, withdrawal_id).await?;
        if w.status()? != WithdrawalStatus::Signing {
            tx.rollback().await.ok();
            return Ok(());
        }
        repo::withdrawals::mark_broadcast(&mut tx, w.id).await?;
        // Value leaves custody into the clearing account.
        chainrail_ledger::record_broadcast(
            &mut tx,
            w.id,
            w.asset_id,
            w.amount_raw,
            w.correlation_id.clone(),
        )
        .await?;

        let envelope = EventEnvelope::new(
            EventPayload::WithdrawalBroadcast(WithdrawalBroadcast {
                withdrawal_id: w.id,
                user_id: w.user_id,
                chain: w.chain.clone(),
                tx_hash: tx_hash.to_string(),
                nonce,
            }),
            w.correlation_id.as_deref().unwrap_or("unknown"),
        )
        .with_deterministic_id(&format!("broadcast:{}", w.id));
        repo::outbox::enqueue(&mut tx, &envelope).await?;
        tx.commit().await.map_err(chainrail_database::map_sqlx)?;

        metrics::counter!(
            "chainrail_withdrawals_broadcast_total",
            "chain" => w.chain.clone(),
        )
        .increment(1);
        tracing::info!(withdrawal_id = %w.id, tx_hash = %tx_hash, "withdrawal broadcast");
        Ok(())
    }

    /// Recover withdrawals stranded in `signing`.
    ///
    /// This is the reconciliation half of the crash-safe broadcast ordering.
    /// It never re-signs with a *new* nonce: the nonce is read back from the
    /// database, so a retry reproduces the identical transaction.
    pub async fn recover_signing(&self, chain: &str) -> Result<usize> {
        let adapter = self.adapter(chain)?;
        let mut tx = self.db.begin().await?;
        let stuck = repo::withdrawals::claim_by_status(
            &mut tx,
            chain,
            WithdrawalStatus::Signing,
            self.batch_size,
        )
        .await?;
        let candidates: Vec<Withdrawal> = stuck;
        tx.rollback().await.ok();
        if candidates.is_empty() {
            return Ok(0);
        }

        let head = adapter.head().await?.height;
        let mut recovered = 0usize;
        for w in candidates {
            let Some(tx_hash) = w.tx_hash.clone() else {
                // No hash means signing never completed; safe to return to
                // `approved` on the next pass by failing and releasing.
                tracing::warn!(withdrawal_id = %w.id, "withdrawal in signing with no tx hash; releasing");
                self.fail_and_release(&w, "signing_incomplete", "signing did not complete")
                    .await?;
                recovered += 1;
                continue;
            };

            match adapter.transaction_status(&tx_hash, head).await? {
                TxStatus::Unknown => {
                    // The transaction never reached the network. Re-broadcasting
                    // the identical signed bytes is safe, so hand it back to the
                    // normal path by re-signing with the *recorded* nonce.
                    tracing::warn!(
                        withdrawal_id = %w.id, %tx_hash,
                        "signed transaction is unknown to the chain; re-broadcasting"
                    );
                    if let Err(e) = self.rebroadcast(&w, &adapter).await {
                        tracing::error!(withdrawal_id = %w.id, error = %e, "re-broadcast failed");
                    } else {
                        recovered += 1;
                    }
                }
                TxStatus::Pending | TxStatus::Mined { .. } => {
                    // It did make it out. Record the broadcast we never got to
                    // record, then let the confirmation stage take over.
                    tracing::info!(
                        withdrawal_id = %w.id, %tx_hash,
                        "recovered a broadcast that was not recorded before the crash"
                    );
                    let nonce = u64::try_from(w.nonce.unwrap_or(0)).unwrap_or(0);
                    self.mark_broadcast(w.id, &tx_hash, nonce).await?;
                    recovered += 1;
                }
            }
        }
        metrics::counter!(
            "chainrail_withdrawal_recoveries_total",
            "chain" => chain.to_string(),
        )
        .increment(recovered as u64);
        Ok(recovered)
    }

    async fn rebroadcast(&self, w: &Withdrawal, adapter: &Arc<dyn ChainAdapter>) -> Result<()> {
        let asset = repo::reference::get_asset_by_id(self.db.pool(), w.asset_id).await?;
        let chain_cfg = crate::service::chain_config(&self.cfg, &w.chain)?;
        let hot_wallet = Address::from_storage(
            chain_cfg
                .hot_wallet_address
                .as_deref()
                .ok_or_else(|| Error::Config("missing hot wallet".into()))?
                .to_ascii_lowercase(),
        );
        // Reuse the recorded nonce: this is what makes the retry produce the same
        // transaction rather than a second transfer.
        let nonce = u64::try_from(w.nonce.ok_or_else(|| {
            Error::Internal("cannot re-broadcast without a recorded nonce".into())
        })?)
        .unwrap_or(0);

        let intent = WithdrawalIntent {
            destination: w.destination_address.clone(),
            amount: w.amount_raw,
            contract: asset.contract_address.as_deref().map(Address::from_storage),
            nonce,
            from: hot_wallet,
        };
        let unsigned = adapter.build_withdrawal(&intent).await?;
        let signed = self.signer.sign(&unsigned).await?;
        adapter.broadcast(&signed).await?;
        self.mark_broadcast(w.id, &signed.tx_hash, nonce).await?;
        Ok(())
    }

    /// `broadcast`/`confirming` -> `completed` or `failed`.
    pub async fn process_in_flight(&self, chain: &str) -> Result<PipelinePass> {
        let adapter = self.adapter(chain)?;
        let chain_cfg = crate::service::chain_config(&self.cfg, chain)?;
        let required = chain_cfg.finality.required_confirmations().unwrap_or(1);
        let head = adapter.head().await?.height;

        let mut candidates = Vec::new();
        for status in [WithdrawalStatus::Broadcast, WithdrawalStatus::Confirming] {
            let mut tx = self.db.begin().await?;
            candidates.extend(
                repo::withdrawals::claim_by_status(&mut tx, chain, status, self.batch_size).await?,
            );
            tx.rollback().await.ok();
        }

        let mut pass = PipelinePass::default();
        for w in candidates {
            let Some(tx_hash) = w.tx_hash.clone() else {
                // Schema forbids this (withdrawals_broadcast_has_hash), so it
                // would mean corruption rather than a normal state.
                tracing::error!(withdrawal_id = %w.id, "broadcast withdrawal has no tx hash");
                continue;
            };
            match adapter.transaction_status(&tx_hash, head).await? {
                TxStatus::Mined {
                    block_number,
                    success,
                    fee_paid,
                    confirmations,
                    ..
                } => {
                    if !success {
                        // Mined but reverted: the transfer did not happen, though
                        // gas was still spent.
                        tracing::error!(
                            withdrawal_id = %w.id, %tx_hash, block_number,
                            "withdrawal transaction reverted on chain"
                        );
                        self.settle_failure(&w, fee_paid).await?;
                        pass.failed += 1;
                        continue;
                    }
                    if confirmations >= required {
                        self.settle_success(&w, block_number, confirmations, fee_paid)
                            .await?;
                        pass.completed += 1;
                    } else {
                        let mut tx = self.db.begin().await?;
                        repo::withdrawals::update_confirmations(
                            &mut tx,
                            w.id,
                            block_number,
                            confirmations,
                        )
                        .await?;
                        tx.commit().await.map_err(chainrail_database::map_sqlx)?;
                        pass.still_pending += 1;
                    }
                }
                TxStatus::Pending => pass.still_pending += 1,
                TxStatus::Unknown => {
                    // Dropped from the mempool after we recorded the broadcast.
                    // Do NOT fail it: the nonce may still be consumed by this
                    // transaction later. Log and keep watching.
                    tracing::warn!(
                        withdrawal_id = %w.id, %tx_hash,
                        "broadcast transaction is not visible to the node; continuing to monitor"
                    );
                    metrics::counter!(
                        "chainrail_withdrawal_missing_tx_total",
                        "chain" => chain.to_string(),
                    )
                    .increment(1);
                    pass.still_pending += 1;
                }
            }
        }
        Ok(pass)
    }

    async fn settle_success(
        &self,
        w: &Withdrawal,
        block_number: u64,
        confirmations: u64,
        fee_paid: Amount,
    ) -> Result<()> {
        let mut tx = self.db.begin().await?;
        let locked = repo::withdrawals::lock_withdrawal(&mut tx, w.id).await?;
        let status = locked.status()?;
        if status == WithdrawalStatus::Completed {
            tx.rollback().await.ok();
            return Ok(());
        }
        if !status.funds_may_have_left() {
            tx.rollback().await.ok();
            return Err(Error::InvalidStateTransition {
                entity: "withdrawal",
                from: status.as_str().into(),
                to: "completed".into(),
            });
        }

        repo::withdrawals::update_confirmations(&mut tx, w.id, block_number, confirmations).await?;
        let settle = chainrail_ledger::settle_withdrawal(
            &mut tx,
            w.id,
            w.user_id,
            w.asset_id,
            w.amount_raw,
            w.correlation_id.clone(),
        )
        .await?;

        // Gas is paid in the chain's native asset, which is a different asset
        // from the one withdrawn.
        if let Ok(native) =
            repo::reference::get_asset(&mut *tx, &w.chain, &native_symbol(&w.chain)).await
        {
            chainrail_ledger::record_network_fee(
                &mut tx,
                w.id,
                native.id,
                fee_paid,
                w.correlation_id.clone(),
            )
            .await?;
        }

        if !repo::withdrawals::mark_completed(&mut tx, w.id, settle.id(), fee_paid).await? {
            tx.rollback().await.ok();
            return Ok(());
        }

        let asset = repo::reference::get_asset_by_id(&mut *tx, w.asset_id).await?;
        let envelope = EventEnvelope::new(
            EventPayload::WithdrawalCompleted(WithdrawalCompleted {
                withdrawal_id: w.id,
                user_id: w.user_id,
                chain: w.chain.clone(),
                tx_hash: w
                    .tx_hash
                    .as_ref()
                    .map(|h| h.to_string())
                    .unwrap_or_default(),
                asset: asset.symbol.clone(),
                amount_raw: w.amount_raw,
                fee_raw: fee_paid,
                ledger_transaction_id: settle.id(),
            }),
            w.correlation_id.as_deref().unwrap_or("unknown"),
        )
        .with_deterministic_id(&format!("completed:{}", w.id));
        repo::outbox::enqueue(&mut tx, &envelope).await?;
        tx.commit().await.map_err(chainrail_database::map_sqlx)?;

        metrics::counter!(
            "chainrail_withdrawals_total",
            "chain" => w.chain.clone(),
            "result" => "completed",
        )
        .increment(1);
        tracing::info!(
            withdrawal_id = %w.id, confirmations,
            fee = %fee_paid, "withdrawal completed and settled"
        );
        Ok(())
    }

    /// A broadcast transaction that reverted on chain.
    ///
    /// The value never left, so the clearing leg is reversed and the user's
    /// reservation is released. Gas *was* spent and is booked as an expense.
    async fn settle_failure(&self, w: &Withdrawal, fee_paid: Amount) -> Result<()> {
        let mut tx = self.db.begin().await?;
        let locked = repo::withdrawals::lock_withdrawal(&mut tx, w.id).await?;
        let status = locked.status()?;
        if status.is_terminal() {
            tx.rollback().await.ok();
            return Ok(());
        }

        chainrail_ledger::reverse_broadcast(
            &mut tx,
            w.id,
            w.asset_id,
            w.amount_raw,
            w.correlation_id.clone(),
        )
        .await?;
        chainrail_ledger::release_reservation(
            &mut tx,
            w.id,
            w.user_id,
            w.asset_id,
            w.amount_raw,
            w.correlation_id.clone(),
        )
        .await?;
        if let Ok(native) =
            repo::reference::get_asset(&mut *tx, &w.chain, &native_symbol(&w.chain)).await
        {
            chainrail_ledger::record_network_fee(
                &mut tx,
                w.id,
                native.id,
                fee_paid,
                w.correlation_id.clone(),
            )
            .await?;
        }
        repo::withdrawals::mark_failed(
            &mut tx,
            w.id,
            status,
            "onchain_revert",
            "the withdrawal transaction reverted on chain; funds were returned",
        )
        .await?;
        self.enqueue_failure(
            &mut tx,
            w,
            "onchain_revert",
            "transaction reverted on chain",
        )
        .await?;
        tx.commit().await.map_err(chainrail_database::map_sqlx)?;

        metrics::counter!(
            "chainrail_withdrawal_failures_total",
            "chain" => w.chain.clone(),
            "reason" => "onchain_revert".to_string(),
        )
        .increment(1);
        Ok(())
    }

    /// Fail a withdrawal that never reached the network and release its funds.
    async fn fail_and_release(&self, w: &Withdrawal, code: &str, reason: &str) -> Result<bool> {
        let mut tx = self.db.begin().await?;
        let locked = repo::withdrawals::lock_withdrawal(&mut tx, w.id).await?;
        let status = locked.status()?;
        if status.is_terminal() || status.funds_may_have_left() {
            tx.rollback().await.ok();
            return Ok(false);
        }
        if locked.reserve_ledger_transaction_id.is_some() {
            chainrail_ledger::release_reservation(
                &mut tx,
                w.id,
                w.user_id,
                w.asset_id,
                w.amount_raw,
                w.correlation_id.clone(),
            )
            .await?;
        }
        let done = repo::withdrawals::mark_failed(&mut tx, w.id, status, code, reason).await?;
        self.enqueue_failure(&mut tx, w, code, reason).await?;
        tx.commit().await.map_err(chainrail_database::map_sqlx)?;

        metrics::counter!(
            "chainrail_withdrawal_failures_total",
            "chain" => w.chain.clone(),
            "reason" => code.to_string(),
        )
        .increment(1);
        Ok(done)
    }

    async fn fail_before_broadcast(&self, withdrawal_id: Uuid, error: &Error) -> Result<bool> {
        let w = repo::withdrawals::get_withdrawal(self.db.pool(), withdrawal_id).await?;
        let status = w.status()?;
        // If we cannot prove the transaction did not go out, do not release the
        // funds -- leave it for `recover_signing` to reconcile against the chain.
        if status == WithdrawalStatus::Signing && w.tx_hash.is_some() {
            tracing::warn!(
                %withdrawal_id, error = %error,
                "broadcast outcome is ambiguous; leaving the withdrawal for reconciliation"
            );
            return Ok(false);
        }
        self.fail_and_release(&w, error.code(), &error.to_string())
            .await
    }

    async fn enqueue_failure(
        &self,
        tx: &mut chainrail_database::Tx<'_>,
        w: &Withdrawal,
        code: &str,
        reason: &str,
    ) -> Result<()> {
        let envelope = EventEnvelope::new(
            EventPayload::WithdrawalFailed(WithdrawalFailed {
                withdrawal_id: w.id,
                user_id: w.user_id,
                chain: w.chain.clone(),
                reason_code: code.to_string(),
                reason: reason.to_string(),
            }),
            w.correlation_id.as_deref().unwrap_or("unknown"),
        )
        .with_deterministic_id(&format!("failed:{}:{code}", w.id));
        repo::outbox::enqueue(tx, &envelope).await
    }

    pub async fn run(self: Arc<Self>, cancel: CancellationToken) {
        tracing::info!(
            signer = self.signer.backend_name(),
            "withdrawal pipeline started"
        );
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
                            broadcast = pass.broadcast,
                            completed = pass.completed,
                            failed = pass.failed,
                            recovered = pass.recovered,
                            "withdrawal pipeline pass"
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
                    tracing::error!(error = %e, ?delay, "withdrawal pipeline pass failed");
                    tokio::select! {
                        _ = tokio::time::sleep(delay) => {}
                        _ = cancel.cancelled() => break,
                    }
                }
            }
        }
        tracing::info!("withdrawal pipeline stopped");
    }
}

/// Native gas asset symbol per chain family.
///
/// Config-independent because gas accounting must work even for a chain whose
/// native asset was not registered for deposits.
fn native_symbol(chain: &str) -> String {
    if chain.starts_with("bsc") || chain.contains("binance") {
        "BNB".to_string()
    } else if chain.starts_with("polygon") {
        "POL".to_string()
    } else {
        "ETH".to_string()
    }
}

/// Whether a withdrawal in `status` with `tx_hash` may be safely re-signed.
///
/// Extracted because getting this wrong is how a system double-spends.
pub fn may_resign(status: WithdrawalStatus, has_recorded_nonce: bool) -> bool {
    // Only from `signing`, and only when the original nonce is known so the
    // retry reproduces the identical transaction.
    status == WithdrawalStatus::Signing && has_recorded_nonce
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn native_symbols_map_per_chain_family() {
        assert_eq!(native_symbol("base-sepolia"), "ETH");
        assert_eq!(native_symbol("ethereum"), "ETH");
        assert_eq!(native_symbol("bsc-testnet"), "BNB");
        assert_eq!(native_symbol("polygon-amoy"), "POL");
    }

    #[test]
    fn resigning_is_permitted_only_from_signing_with_a_known_nonce() {
        use WithdrawalStatus::*;
        assert!(may_resign(Signing, true));
        // Without the original nonce a retry would create a *second* transfer.
        assert!(!may_resign(Signing, false));
        // Never from any other state -- especially not after broadcast.
        for s in [
            Requested, Validated, Approved, Broadcast, Confirming, Completed, Failed, Cancelled,
        ] {
            assert!(!may_resign(s, true), "{s} must not be re-signable");
        }
    }

    #[test]
    fn pipeline_pass_reports_work_but_not_mere_waiting() {
        assert!(!PipelinePass::default().did_work());
        assert!(!PipelinePass {
            still_pending: 5,
            ..Default::default()
        }
        .did_work());
        assert!(PipelinePass {
            completed: 1,
            ..Default::default()
        }
        .did_work());
        assert!(PipelinePass {
            recovered: 1,
            ..Default::default()
        }
        .did_work());
    }
}
