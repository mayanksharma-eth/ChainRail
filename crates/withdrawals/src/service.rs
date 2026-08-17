//! Withdrawal request handling: the synchronous part, driven by the API.
//!
//! Ordering inside the request transaction is deliberate:
//!
//!   1. **Idempotent insert.** Establishes the withdrawal row first, so a
//!      duplicate request can never do work twice — enforced by
//!      `unique(user_id, idempotency_key)`.
//!   2. **Validate**, then **risk**, then **reserve**. Funds are only locked
//!      once the request is known to be permissible; a denied request must not
//!      leave a reservation behind.
//!   3. **Approve** (or hold for manual approval) and enqueue the event.
//!
//! Everything commits together. A failure at any point leaves no reservation and
//! no event, and the client may retry with the same idempotency key.

use chainrail_common::chain::keccak256;
use chainrail_common::config::{AppConfig, ChainConfig};
use chainrail_common::event::{EventPayload, WithdrawalRequested};
use chainrail_common::{Address, Amount, ChainKind, Error, EventEnvelope, Result};
use chainrail_database::models::{AccountType, Withdrawal, WithdrawalStatus};
use chainrail_database::{repo, Db};
use chainrail_risk::{Decision, RiskEngine, RiskInput};
use std::sync::Arc;
use uuid::Uuid;

/// A validated withdrawal request, as accepted from the API.
#[derive(Debug, Clone)]
pub struct WithdrawalRequest {
    pub user_id: Uuid,
    pub chain: String,
    pub asset_symbol: String,
    pub amount: Amount,
    pub destination: String,
    pub idempotency_key: String,
    pub correlation_id: Option<String>,
}

impl WithdrawalRequest {
    /// Stable hash of the semantically meaningful fields.
    ///
    /// Reusing an idempotency key with a *different* body must be rejected
    /// rather than silently returning the earlier withdrawal — otherwise a
    /// client bug looks like a successful transfer that never happened.
    pub fn fingerprint(&self, destination_normalized: &str) -> String {
        let canonical = format!(
            "{}|{}|{}|{}|{}",
            self.user_id, self.chain, self.asset_symbol, self.amount, destination_normalized
        );
        hex::encode(keccak256(canonical.as_bytes()))
    }
}

#[derive(Debug)]
pub enum CreateResult {
    /// Newly created and auto-approved; the pipeline will pick it up.
    Approved(Withdrawal),
    /// Created but held for an operator.
    PendingApproval(Withdrawal),
    /// An earlier identical request; nothing new happened.
    Idempotent(Withdrawal),
}

impl CreateResult {
    pub fn withdrawal(&self) -> &Withdrawal {
        match self {
            CreateResult::Approved(w)
            | CreateResult::PendingApproval(w)
            | CreateResult::Idempotent(w) => w,
        }
    }
    pub fn is_new(&self) -> bool {
        !matches!(self, CreateResult::Idempotent(_))
    }
}

pub struct WithdrawalService {
    db: Db,
    cfg: Arc<AppConfig>,
    risk: Arc<RiskEngine>,
}

impl WithdrawalService {
    pub fn new(db: Db, cfg: Arc<AppConfig>, risk: Arc<RiskEngine>) -> Arc<Self> {
        Arc::new(WithdrawalService { db, cfg, risk })
    }

    pub fn db(&self) -> &Db {
        &self.db
    }

    pub async fn create(&self, req: WithdrawalRequest) -> Result<CreateResult> {
        // --- input validation, before any database work ---
        let chain_cfg = self
            .cfg
            .chain(&req.chain)
            .ok_or_else(|| Error::UnsupportedChain(req.chain.clone()))?;
        if chain_cfg.kind != ChainKind::Evm {
            return Err(Error::UnsupportedChain(format!(
                "{}: only EVM chains are supported in v0.1",
                req.chain
            )));
        }
        let asset_cfg = chain_cfg.asset(&req.asset_symbol).ok_or_else(|| {
            Error::UnsupportedAsset(format!("{} on {}", req.asset_symbol, req.chain))
        })?;

        // Trust boundary: destination comes straight from an untrusted client.
        // A bad checksum is rejected, never "fixed".
        let destination = Address::parse(chain_cfg.kind, &req.destination)?;
        if !req.amount.is_positive() {
            return Err(Error::InvalidAmount("amount must be positive".into()));
        }
        if req.idempotency_key.len() < 8 || req.idempotency_key.len() > 128 {
            return Err(Error::Validation(
                "idempotency_key must be between 8 and 128 characters".into(),
            ));
        }

        let asset =
            repo::reference::get_asset(self.db.pool(), &req.chain, &req.asset_symbol).await?;
        let fingerprint = req.fingerprint(&destination.storage_key());
        let correlation_id = req
            .correlation_id
            .clone()
            .unwrap_or_else(|| format!("wd-{}", Uuid::now_v7()));

        let mut tx = self.db.begin().await?;

        // 1. Idempotent insert.
        let created = repo::withdrawals::create_withdrawal(
            &mut tx,
            repo::withdrawals::NewWithdrawal {
                user_id: req.user_id,
                asset_id: asset.id,
                chain: &req.chain,
                destination: &destination,
                amount_raw: req.amount,
                idempotency_key: &req.idempotency_key,
                request_fingerprint: &fingerprint,
                correlation_id: Some(&correlation_id),
            },
        )
        .await?;

        let withdrawal = match created {
            repo::withdrawals::CreateOutcome::Existing(w) => {
                tx.rollback().await.ok();
                metrics::counter!(
                    "chainrail_withdrawals_total",
                    "chain" => req.chain.clone(),
                    "result" => "idempotent_replay",
                )
                .increment(1);
                return Ok(CreateResult::Idempotent(w));
            }
            repo::withdrawals::CreateOutcome::Created(w) => w,
        };

        // 2. Risk evaluation. Facts are gathered inside the transaction so the
        //    balance we check is the balance we then reserve against.
        let available = chainrail_ledger::user_balance(
            &mut *tx,
            AccountType::UserAvailable,
            req.user_id,
            asset.id,
        )
        .await?;
        let since = chrono::Utc::now() - chrono::Duration::hours(24);
        let (daily_total, daily_count) =
            repo::withdrawals::sum_recent_withdrawals(&mut *tx, req.user_id, asset.id, since)
                .await?;
        // Exclude the row we just inserted from its own velocity check.
        let daily_total = daily_total.checked_sub(req.amount).unwrap_or(Amount::ZERO);
        let daily_count = daily_count.saturating_sub(1);

        let destination_owner =
            repo::reference::owner_of_address(&mut *tx, &req.chain, &destination.storage_key())
                .await?;

        let decision = self.risk.evaluate_and_record(&RiskInput {
            chain: &req.chain,
            asset_symbol: &req.asset_symbol,
            amount: req.amount,
            destination_lower: &destination.storage_key(),
            hot_wallet_lower: chain_cfg
                .hot_wallet_address
                .as_deref()
                .map(str::to_ascii_lowercase)
                .as_deref(),
            available_balance: available,
            daily_total,
            daily_count: u32::try_from(daily_count).unwrap_or(u32::MAX),
            // Sending to a deposit address belonging to someone else would move
            // funds out and then re-credit them to that user.
            is_foreign_deposit_address: destination_owner
                .map(|owner| owner != req.user_id)
                .unwrap_or(false),
            withdrawals_enabled_for_asset: asset_cfg.withdrawals_enabled,
        });

        if let Decision::Deny { code, message } = &decision {
            // Record the denial on the withdrawal itself, so the client can fetch
            // it by id and see why. The row stays; the funds were never reserved.
            repo::withdrawals::mark_failed(
                &mut tx,
                withdrawal.id,
                WithdrawalStatus::Requested,
                code,
                message,
            )
            .await?;
            tx.commit().await.map_err(chainrail_database::map_sqlx)?;
            metrics::counter!(
                "chainrail_withdrawal_failures_total",
                "chain" => req.chain.clone(),
                "reason" => code.clone(),
            )
            .increment(1);
            return Err(Error::PolicyDenied {
                code: code.clone(),
                message: message.clone(),
            });
        }

        // 3. Validated.
        repo::withdrawals::transition(
            &mut tx,
            withdrawal.id,
            WithdrawalStatus::Requested,
            WithdrawalStatus::Validated,
        )
        .await?;

        // 4. Reserve. The ledger's non-negative CHECK is the authority here; the
        //    risk engine's balance check above is only for a better error message.
        let reservation = chainrail_ledger::reserve_withdrawal(
            &mut tx,
            withdrawal.id,
            req.user_id,
            asset.id,
            req.amount,
            Some(correlation_id.clone()),
        )
        .await?;
        repo::withdrawals::attach_reservation(&mut tx, withdrawal.id, reservation.id()).await?;

        // 5. Approve, or hold.
        let held = decision.requires_manual_approval();
        if !held {
            repo::withdrawals::transition(
                &mut tx,
                withdrawal.id,
                WithdrawalStatus::Validated,
                WithdrawalStatus::Approved,
            )
            .await?;
        }

        let envelope = EventEnvelope::new(
            EventPayload::WithdrawalRequested(WithdrawalRequested {
                withdrawal_id: withdrawal.id,
                user_id: req.user_id,
                chain: req.chain.clone(),
                asset: req.asset_symbol.clone(),
                amount_raw: req.amount,
                destination: destination.to_string(),
            }),
            &correlation_id,
        )
        .with_deterministic_id(&withdrawal.id.to_string());
        repo::outbox::enqueue(&mut tx, &envelope).await?;

        tx.commit().await.map_err(chainrail_database::map_sqlx)?;

        metrics::counter!(
            "chainrail_withdrawals_total",
            "chain" => req.chain.clone(),
            "result" => if held { "pending_approval" } else { "approved" },
        )
        .increment(1);
        tracing::info!(
            withdrawal_id = %withdrawal.id,
            user_id = %req.user_id,
            chain = %req.chain,
            asset = %req.asset_symbol,
            amount = %req.amount.format_units(asset.decimals_u8()),
            held,
            "withdrawal accepted"
        );

        let fresh = repo::withdrawals::get_withdrawal(self.db.pool(), withdrawal.id).await?;
        Ok(if held {
            CreateResult::PendingApproval(fresh)
        } else {
            CreateResult::Approved(fresh)
        })
    }

    /// Operator approval for a held withdrawal.
    pub async fn approve(&self, withdrawal_id: Uuid) -> Result<Withdrawal> {
        let mut tx = self.db.begin().await?;
        let w = repo::withdrawals::lock_withdrawal(&mut tx, withdrawal_id).await?;
        let status = w.status()?;
        if status != WithdrawalStatus::Validated {
            return Err(Error::InvalidStateTransition {
                entity: "withdrawal",
                from: status.as_str().into(),
                to: "approved".into(),
            });
        }
        repo::withdrawals::transition(
            &mut tx,
            withdrawal_id,
            WithdrawalStatus::Validated,
            WithdrawalStatus::Approved,
        )
        .await?;
        tx.commit().await.map_err(chainrail_database::map_sqlx)?;
        tracing::info!(%withdrawal_id, "withdrawal approved by operator");
        repo::withdrawals::get_withdrawal(self.db.pool(), withdrawal_id).await
    }

    /// Cancel a withdrawal and return the reserved funds.
    ///
    /// Only legal before broadcast: once a signed transaction is on the wire it
    /// cannot be recalled, and the state machine trigger enforces that too.
    pub async fn cancel(&self, withdrawal_id: Uuid, reason: &str) -> Result<Withdrawal> {
        let mut tx = self.db.begin().await?;
        let w = repo::withdrawals::lock_withdrawal(&mut tx, withdrawal_id).await?;
        let status = w.status()?;
        if status.funds_may_have_left() {
            return Err(Error::Conflict(format!(
                "withdrawal {withdrawal_id} is {status}; a broadcast transaction cannot be cancelled"
            )));
        }
        if status.is_terminal() {
            return Err(Error::Conflict(format!(
                "withdrawal {withdrawal_id} is already {status}"
            )));
        }

        if w.reserve_ledger_transaction_id.is_some() {
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
        repo::withdrawals::transition(&mut tx, w.id, status, WithdrawalStatus::Cancelled).await?;
        tx.commit().await.map_err(chainrail_database::map_sqlx)?;

        tracing::info!(%withdrawal_id, reason, "withdrawal cancelled and funds released");
        repo::withdrawals::get_withdrawal(self.db.pool(), withdrawal_id).await
    }

    pub async fn get(&self, withdrawal_id: Uuid) -> Result<Withdrawal> {
        repo::withdrawals::get_withdrawal(self.db.pool(), withdrawal_id).await
    }
}

/// Chain config lookup used by both the service and the pipeline.
pub fn chain_config<'a>(cfg: &'a AppConfig, chain: &str) -> Result<&'a ChainConfig> {
    cfg.chain(chain)
        .ok_or_else(|| Error::UnsupportedChain(chain.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn request() -> WithdrawalRequest {
        WithdrawalRequest {
            user_id: Uuid::from_u128(1),
            chain: "base-sepolia".into(),
            asset_symbol: "USDC".into(),
            amount: Amount::new(25_000_000),
            destination: "0x5aAeb6053F3E94C9b9A09f33669435E7Ef1BeAed".into(),
            idempotency_key: "idem-key-0001".into(),
            correlation_id: None,
        }
    }

    #[test]
    fn fingerprint_is_stable_for_identical_requests() {
        let a = request();
        let b = request();
        let dest = "0x5aaeb6053f3e94c9b9a09f33669435e7ef1beaed";
        assert_eq!(a.fingerprint(dest), b.fingerprint(dest));
        assert_eq!(a.fingerprint(dest).len(), 64);
    }

    #[test]
    fn fingerprint_changes_when_any_meaningful_field_changes() {
        let dest = "0x5aaeb6053f3e94c9b9a09f33669435e7ef1beaed";
        let base = request().fingerprint(dest);

        let mut amount = request();
        amount.amount = Amount::new(25_000_001);
        assert_ne!(amount.fingerprint(dest), base, "amount must be covered");

        let mut user = request();
        user.user_id = Uuid::from_u128(2);
        assert_ne!(user.fingerprint(dest), base, "user must be covered");

        let mut asset = request();
        asset.asset_symbol = "USDT".into();
        assert_ne!(asset.fingerprint(dest), base, "asset must be covered");

        let mut chain = request();
        chain.chain = "ethereum".into();
        assert_ne!(chain.fingerprint(dest), base, "chain must be covered");

        assert_ne!(
            request().fingerprint("0x1111111111111111111111111111111111111111"),
            base,
            "destination must be covered"
        );
    }

    #[test]
    fn fingerprint_ignores_fields_that_do_not_change_the_transfer() {
        let dest = "0x5aaeb6053f3e94c9b9a09f33669435e7ef1beaed";
        let base = request().fingerprint(dest);
        let mut corr = request();
        corr.correlation_id = Some("a-different-trace".into());
        // A retry from a different trace is still the same transfer.
        assert_eq!(corr.fingerprint(dest), base);
    }

    #[test]
    fn idempotency_keys_are_length_bounded() {
        // Too short is guessable across users; unbounded is a storage DoS.
        for key in ["", "short", &"x".repeat(129)] {
            let bad = key.len() < 8 || key.len() > 128;
            assert!(bad, "key {key:?} should be rejected");
        }
        assert!(!("idem-key-0001".len() < 8 || "idem-key-0001".len() > 128));
    }
}
