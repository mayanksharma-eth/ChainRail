//! Withdrawal persistence, including idempotent creation and nonce allocation.

use chainrail_common::{Address, Amount, Error, Hash32, Result};
use chrono::{DateTime, Utc};
use sqlx::{Executor, Postgres};
use uuid::Uuid;

use crate::models::{Withdrawal, WithdrawalStatus};
use crate::{SqlxResultExt, Tx};

/// Compile-time withdrawal SELECT. See `deposit_select!` for why this is a
/// macro and not a runtime `format!`.
macro_rules! withdrawal_select {
    ($tail:literal) => {
        concat!(
            "SELECT id, user_id, asset_id, chain, destination_address, amount_raw, status, ",
            "idempotency_key, request_fingerprint, tx_hash, nonce, gas_limit, ",
            "max_fee_per_gas, max_priority_fee_per_gas, fee_paid_raw, block_number, ",
            "confirmations, broadcast_attempts, reserve_ledger_transaction_id, ",
            "settle_ledger_transaction_id, failure_code, failure_reason, correlation_id, ",
            "approved_at, broadcast_at, completed_at, created_at, updated_at FROM withdrawals ",
            $tail
        )
    };
}

pub struct NewWithdrawal<'a> {
    pub user_id: Uuid,
    pub asset_id: Uuid,
    pub chain: &'a str,
    pub destination: &'a Address,
    pub amount_raw: Amount,
    pub idempotency_key: &'a str,
    pub request_fingerprint: &'a str,
    pub correlation_id: Option<&'a str>,
}

pub enum CreateOutcome {
    Created(Withdrawal),
    /// The same (user, idempotency_key) already exists with an identical
    /// request body -- return the original rather than moving funds twice.
    Existing(Withdrawal),
}

/// Idempotent create.
///
/// A retried POST with the same key returns the original withdrawal. A reused
/// key carrying a *different* body is rejected: silently returning the old
/// withdrawal would make the client believe a different transfer happened.
pub async fn create_withdrawal(tx: &mut Tx<'_>, new: NewWithdrawal<'_>) -> Result<CreateOutcome> {
    let inserted = sqlx::query_as::<_, Withdrawal>(
        r#"
        INSERT INTO withdrawals
            (user_id, asset_id, chain, destination_address, amount_raw,
             idempotency_key, request_fingerprint, correlation_id, status)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 'requested')
        ON CONFLICT (user_id, idempotency_key) DO NOTHING
        RETURNING id, user_id, asset_id, chain, destination_address, amount_raw, status,
                  idempotency_key, request_fingerprint, tx_hash, nonce, gas_limit,
                  max_fee_per_gas, max_priority_fee_per_gas, fee_paid_raw, block_number,
                  confirmations, broadcast_attempts, reserve_ledger_transaction_id,
                  settle_ledger_transaction_id, failure_code, failure_reason, correlation_id,
                  approved_at, broadcast_at, completed_at, created_at, updated_at
        "#,
    )
    .bind(new.user_id)
    .bind(new.asset_id)
    .bind(new.chain)
    .bind(new.destination.storage_key())
    .bind(new.amount_raw)
    .bind(new.idempotency_key)
    .bind(new.request_fingerprint)
    .bind(new.correlation_id)
    .fetch_optional(&mut **tx)
    .await
    .map_db()?;

    if let Some(w) = inserted {
        return Ok(CreateOutcome::Created(w));
    }

    let existing = sqlx::query_as::<_, Withdrawal>(withdrawal_select!(
        "WHERE user_id = $1 AND idempotency_key = $2"
    ))
    .bind(new.user_id)
    .bind(new.idempotency_key)
    .fetch_optional(&mut **tx)
    .await
    .map_db()?
    .ok_or_else(|| Error::Internal("idempotency conflict with no conflicting row".into()))?;

    if existing.request_fingerprint != new.request_fingerprint {
        return Err(Error::IdempotencyKeyConflict);
    }
    Ok(CreateOutcome::Existing(existing))
}

pub async fn get_withdrawal<'e, E>(ex: E, id: Uuid) -> Result<Withdrawal>
where
    E: Executor<'e, Database = Postgres>,
{
    sqlx::query_as::<_, Withdrawal>(withdrawal_select!("WHERE id = $1"))
        .bind(id)
        .fetch_optional(ex)
        .await
        .map_db()?
        .ok_or(Error::NotFound {
            entity: "withdrawal",
        })
}

pub async fn lock_withdrawal(tx: &mut Tx<'_>, id: Uuid) -> Result<Withdrawal> {
    sqlx::query_as::<_, Withdrawal>(withdrawal_select!("WHERE id = $1 FOR UPDATE"))
        .bind(id)
        .fetch_optional(&mut **tx)
        .await
        .map_db()?
        .ok_or(Error::NotFound {
            entity: "withdrawal",
        })
}

pub async fn list_withdrawals<'e, E>(
    ex: E,
    user_id: Option<Uuid>,
    status: Option<WithdrawalStatus>,
    after: Option<(DateTime<Utc>, Uuid)>,
    limit: i64,
) -> Result<Vec<Withdrawal>>
where
    E: Executor<'e, Database = Postgres>,
{
    sqlx::query_as::<_, Withdrawal>(
        r#"
        SELECT id, user_id, asset_id, chain, destination_address, amount_raw, status,
               idempotency_key, request_fingerprint, tx_hash, nonce, gas_limit,
               max_fee_per_gas, max_priority_fee_per_gas, fee_paid_raw, block_number,
               confirmations, broadcast_attempts, reserve_ledger_transaction_id,
               settle_ledger_transaction_id, failure_code, failure_reason, correlation_id,
               approved_at, broadcast_at, completed_at, created_at, updated_at
          FROM withdrawals
         WHERE ($1::uuid IS NULL OR user_id = $1)
           AND ($2::text IS NULL OR status = $2)
           AND ($3::timestamptz IS NULL OR (created_at, id) < ($3, $4))
         ORDER BY created_at DESC, id DESC
         LIMIT $5
        "#,
    )
    .bind(user_id)
    .bind(status.map(|s| s.as_str()))
    .bind(after.map(|(t, _)| t))
    .bind(after.map(|(_, i)| i).unwrap_or_else(Uuid::nil))
    .bind(limit)
    .fetch_all(ex)
    .await
    .map_db()
}

/// Claim withdrawals in a given state for processing.
///
/// `SKIP LOCKED` means N worker replicas partition the queue between
/// themselves with no coordination and no double-processing. The row lock is
/// held for the life of the caller's transaction.
pub async fn claim_by_status(
    tx: &mut Tx<'_>,
    chain: &str,
    status: WithdrawalStatus,
    limit: i64,
) -> Result<Vec<Withdrawal>> {
    sqlx::query_as::<_, Withdrawal>(withdrawal_select!(
        "WHERE chain = $1 AND status = $2 ORDER BY created_at ASC LIMIT $3 FOR UPDATE SKIP LOCKED"
    ))
    .bind(chain)
    .bind(status.as_str())
    .bind(limit)
    .fetch_all(&mut **tx)
    .await
    .map_db()
}

/// Compare-and-set state transition. Returns false when another worker already
/// moved the withdrawal, which the caller treats as "someone else handled it".
pub async fn transition(
    tx: &mut Tx<'_>,
    id: Uuid,
    from: WithdrawalStatus,
    to: WithdrawalStatus,
) -> Result<bool> {
    // Validate in Rust first so the error is a clean domain error rather than a
    // Postgres exception; the trigger remains the authoritative backstop.
    from.transition_to(to)?;
    let r = sqlx::query(
        r#"
        UPDATE withdrawals
           SET status = $3,
               approved_at = CASE WHEN $3 = 'approved' THEN now() ELSE approved_at END
         WHERE id = $1 AND status = $2
        "#,
    )
    .bind(id)
    .bind(from.as_str())
    .bind(to.as_str())
    .execute(&mut **tx)
    .await
    .map_db()?;
    Ok(r.rows_affected() == 1)
}

pub async fn attach_reservation(
    tx: &mut Tx<'_>,
    id: Uuid,
    ledger_transaction_id: Uuid,
) -> Result<()> {
    sqlx::query("UPDATE withdrawals SET reserve_ledger_transaction_id = $2 WHERE id = $1")
        .bind(id)
        .bind(ledger_transaction_id)
        .execute(&mut **tx)
        .await
        .map_db()?;
    Ok(())
}

pub struct SignedTxFacts<'a> {
    pub tx_hash: &'a Hash32,
    pub nonce: u64,
    pub gas_limit: u64,
    pub max_fee_per_gas: Amount,
    pub max_priority_fee_per_gas: Amount,
}

/// Persist the signed transaction's identity *before* broadcasting it.
///
/// This ordering is deliberate and is the core of the
/// "broadcast succeeded but the database write failed" failure mode: because
/// the hash is durable first, a crash mid-broadcast leaves a recoverable record
/// that the reconciler can look up on chain. The reverse order would lose the
/// hash and strand funds. See docs/failure-modes.md.
pub async fn record_signed_transaction(
    tx: &mut Tx<'_>,
    id: Uuid,
    facts: SignedTxFacts<'_>,
) -> Result<()> {
    sqlx::query(
        r#"
        UPDATE withdrawals
           SET tx_hash = $2, nonce = $3, gas_limit = $4,
               max_fee_per_gas = $5, max_priority_fee_per_gas = $6,
               broadcast_attempts = broadcast_attempts + 1
         WHERE id = $1
        "#,
    )
    .bind(id)
    .bind(facts.tx_hash)
    .bind(facts.nonce as i64)
    .bind(facts.gas_limit as i64)
    .bind(facts.max_fee_per_gas)
    .bind(facts.max_priority_fee_per_gas)
    .execute(&mut **tx)
    .await
    .map_db()?;
    Ok(())
}

pub async fn mark_broadcast(tx: &mut Tx<'_>, id: Uuid) -> Result<bool> {
    let r = sqlx::query(
        "UPDATE withdrawals SET status = 'broadcast', broadcast_at = now()
          WHERE id = $1 AND status = 'signing'",
    )
    .bind(id)
    .execute(&mut **tx)
    .await
    .map_db()?;
    Ok(r.rows_affected() == 1)
}

pub async fn update_confirmations(
    tx: &mut Tx<'_>,
    id: Uuid,
    block_number: u64,
    confirmations: u64,
) -> Result<()> {
    sqlx::query(
        r#"
        UPDATE withdrawals
           SET block_number = $2,
               confirmations = $3,
               status = CASE WHEN status = 'broadcast' THEN 'confirming' ELSE status END
         WHERE id = $1 AND status IN ('broadcast', 'confirming')
        "#,
    )
    .bind(id)
    .bind(block_number as i64)
    .bind(confirmations as i64)
    .execute(&mut **tx)
    .await
    .map_db()?;
    Ok(())
}

pub async fn mark_completed(
    tx: &mut Tx<'_>,
    id: Uuid,
    settle_ledger_transaction_id: Uuid,
    fee_paid_raw: Amount,
) -> Result<bool> {
    let r = sqlx::query(
        r#"
        UPDATE withdrawals
           SET status = 'completed',
               settle_ledger_transaction_id = $2,
               fee_paid_raw = $3,
               completed_at = now()
         WHERE id = $1 AND status IN ('broadcast', 'confirming')
        "#,
    )
    .bind(id)
    .bind(settle_ledger_transaction_id)
    .bind(fee_paid_raw)
    .execute(&mut **tx)
    .await
    .map_db()?;
    Ok(r.rows_affected() == 1)
}

pub async fn mark_failed(
    tx: &mut Tx<'_>,
    id: Uuid,
    from: WithdrawalStatus,
    code: &str,
    reason: &str,
) -> Result<bool> {
    let r = sqlx::query(
        "UPDATE withdrawals SET status = 'failed', failure_code = $3, failure_reason = $4
          WHERE id = $1 AND status = $2",
    )
    .bind(id)
    .bind(from.as_str())
    .bind(code)
    .bind(reason)
    .execute(&mut **tx)
    .await
    .map_db()?;
    Ok(r.rows_affected() == 1)
}

/// Sum of non-failed withdrawals for a user/asset in a rolling window. Feeds
/// the daily velocity policy.
pub async fn sum_recent_withdrawals<'e, E>(
    ex: E,
    user_id: Uuid,
    asset_id: Uuid,
    since: DateTime<Utc>,
) -> Result<(Amount, i64)>
where
    E: Executor<'e, Database = Postgres>,
{
    let row = sqlx::query_as::<_, (Option<Amount>, i64)>(
        r#"
        SELECT COALESCE(SUM(amount_raw), 0), COUNT(*)
          FROM withdrawals
         WHERE user_id = $1 AND asset_id = $2 AND created_at >= $3
           AND status NOT IN ('failed', 'cancelled')
        "#,
    )
    .bind(user_id)
    .bind(asset_id)
    .bind(since)
    .fetch_one(ex)
    .await
    .map_db()?;
    Ok((row.0.unwrap_or(Amount::ZERO), row.1))
}

/// Allocate the next nonce for a hot wallet.
///
/// `INSERT ... ON CONFLICT DO UPDATE ... RETURNING` performs the read and the
/// increment in one atomic statement while holding the row lock, so two
/// concurrent signers can never receive the same nonce -- which would strand
/// one of the two transactions permanently.
pub async fn allocate_nonce(
    tx: &mut Tx<'_>,
    chain: &str,
    address_lower: &str,
    chain_nonce_hint: u64,
) -> Result<u64> {
    let nonce = sqlx::query_scalar::<_, i64>(
        r#"
        INSERT INTO chain_nonces (chain, address, next_nonce)
        VALUES ($1, $2, $3 + 1)
        ON CONFLICT (chain, address) DO UPDATE
            -- Never move backwards: if the chain reports a higher nonce than we
            -- have recorded (e.g. the wallet was used out of band), adopt it.
            SET next_nonce = GREATEST(chain_nonces.next_nonce, $3) + 1,
                updated_at = now()
        RETURNING next_nonce - 1
        "#,
    )
    .bind(chain)
    .bind(address_lower)
    .bind(chain_nonce_hint as i64)
    .fetch_one(&mut **tx)
    .await
    .map_db()?;
    Ok(nonce.max(0) as u64)
}

pub async fn count_by_status<'e, E>(ex: E) -> Result<Vec<(String, i64)>>
where
    E: Executor<'e, Database = Postgres>,
{
    sqlx::query_as::<_, (String, i64)>("SELECT status, COUNT(*) FROM withdrawals GROUP BY status")
        .fetch_all(ex)
        .await
        .map_db()
}
