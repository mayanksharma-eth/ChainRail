//! Deposit persistence.

use chainrail_common::{Amount, Error, Result};
use chrono::{DateTime, Utc};
use sqlx::{Executor, Postgres};
use uuid::Uuid;

use crate::models::{Deposit, DepositStatus, PendingDeposit};
use crate::{SqlxResultExt, Tx};

/// Builds a deposit SELECT at compile time.
///
/// A macro rather than a runtime `format!` so the resulting query is a string
/// *literal*: sqlx can then statically guarantee no dynamic data reached the
/// SQL text, which is exactly the property we want on a money path.
macro_rules! deposit_select {
    ($tail:literal) => {
        concat!(
            "SELECT id, user_id, blockchain_transaction_id, asset_id, amount_raw, confirmations, ",
            "status, ledger_transaction_id, reversal_ledger_transaction_id, ",
            "failure_reason, credited_at, created_at, updated_at FROM deposits ",
            $tail
        )
    };
}

/// Create the deposit record for an observed transfer.
///
/// Returns the existing row if one is already present. Combined with the
/// `deposits_blockchain_transaction_key` unique constraint, this makes
/// double-crediting structurally impossible rather than merely unlikely.
pub async fn create_deposit<'e, E>(
    ex: E,
    user_id: Uuid,
    blockchain_transaction_id: Uuid,
    asset_id: Uuid,
    amount_raw: Amount,
) -> Result<Deposit>
where
    E: Executor<'e, Database = Postgres>,
{
    sqlx::query_as::<_, Deposit>(
        r#"
        INSERT INTO deposits (user_id, blockchain_transaction_id, asset_id, amount_raw, status)
        VALUES ($1, $2, $3, $4, 'observed')
        ON CONFLICT (blockchain_transaction_id) DO UPDATE
            SET blockchain_transaction_id = EXCLUDED.blockchain_transaction_id
        RETURNING id, user_id, blockchain_transaction_id, asset_id, amount_raw, confirmations,
                  status, ledger_transaction_id, reversal_ledger_transaction_id,
                  failure_reason, credited_at, created_at, updated_at
        "#,
    )
    .bind(user_id)
    .bind(blockchain_transaction_id)
    .bind(asset_id)
    .bind(amount_raw)
    .fetch_one(ex)
    .await
    .map_db()
}

pub async fn get_deposit<'e, E>(ex: E, id: Uuid) -> Result<Deposit>
where
    E: Executor<'e, Database = Postgres>,
{
    sqlx::query_as::<_, Deposit>(deposit_select!("WHERE id = $1"))
        .bind(id)
        .fetch_optional(ex)
        .await
        .map_db()?
        .ok_or(Error::NotFound { entity: "deposit" })
}

/// Keyset pagination: `after` is the `created_at` of the last row on the
/// previous page. Offset pagination would degrade badly once a user has many
/// deposits, and shifts rows under the client as new deposits arrive.
pub async fn list_deposits<'e, E>(
    ex: E,
    user_id: Option<Uuid>,
    status: Option<DepositStatus>,
    after: Option<(DateTime<Utc>, Uuid)>,
    limit: i64,
) -> Result<Vec<Deposit>>
where
    E: Executor<'e, Database = Postgres>,
{
    sqlx::query_as::<_, Deposit>(
        r#"
        SELECT id, user_id, blockchain_transaction_id, asset_id, amount_raw, confirmations,
               status, ledger_transaction_id, reversal_ledger_transaction_id,
               failure_reason, credited_at, created_at, updated_at
          FROM deposits
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

/// Deposits that still need confirmation tracking, joined with their chain
/// facts. `FOR UPDATE SKIP LOCKED` lets several confirmation workers run
/// concurrently without processing the same deposit twice or blocking.
pub async fn claim_pending_deposits(
    tx: &mut Tx<'_>,
    chain: &str,
    limit: i64,
) -> Result<Vec<PendingDeposit>> {
    sqlx::query_as::<_, PendingDeposit>(
        r#"
        SELECT d.id            AS deposit_id,
               d.user_id       AS user_id,
               d.asset_id      AS asset_id,
               a.symbol        AS asset_symbol,
               bt.chain        AS chain,
               bt.tx_hash      AS tx_hash,
               bt.block_number AS block_number,
               bt.block_hash   AS block_hash,
               d.amount_raw    AS amount_raw,
               d.status        AS status,
               d.confirmations AS confirmations
          FROM deposits d
          JOIN blockchain_transactions bt ON bt.id = d.blockchain_transaction_id
          JOIN assets a ON a.id = d.asset_id
         WHERE bt.chain = $1
           AND d.status IN ('observed', 'confirming', 'confirmed')
         ORDER BY bt.block_number ASC
         LIMIT $2
           FOR UPDATE OF d SKIP LOCKED
        "#,
    )
    .bind(chain)
    .bind(limit)
    .fetch_all(&mut **tx)
    .await
    .map_db()
}

/// Record progress toward the confirmation threshold.
///
/// The `status = $3` guard makes this a compare-and-set: a concurrent worker
/// that already advanced the deposit wins, and this update becomes a no-op
/// instead of moving the deposit backwards.
pub async fn update_confirmations<'e, E>(
    ex: E,
    deposit_id: Uuid,
    confirmations: u64,
    expected_status: DepositStatus,
    next_status: DepositStatus,
) -> Result<bool>
where
    E: Executor<'e, Database = Postgres>,
{
    let r = sqlx::query(
        "UPDATE deposits SET confirmations = $2, status = $3 WHERE id = $1 AND status = $4",
    )
    .bind(deposit_id)
    .bind(confirmations as i64)
    .bind(next_status.as_str())
    .bind(expected_status.as_str())
    .execute(ex)
    .await
    .map_db()?;
    Ok(r.rows_affected() == 1)
}

/// Mark a deposit credited and link it to its ledger posting.
///
/// Guarded on `status = 'confirmed'`, so a duplicate credit attempt affects
/// zero rows and the caller can detect it. This is the last of three
/// independent defences against double-crediting (the others being the
/// transfer natural key and the ledger idempotency key).
pub async fn mark_credited(
    tx: &mut Tx<'_>,
    deposit_id: Uuid,
    ledger_transaction_id: Uuid,
) -> Result<bool> {
    let r = sqlx::query(
        r#"
        UPDATE deposits
           SET status = 'credited',
               ledger_transaction_id = $2,
               credited_at = now()
         WHERE id = $1 AND status = 'confirmed'
        "#,
    )
    .bind(deposit_id)
    .bind(ledger_transaction_id)
    .execute(&mut **tx)
    .await
    .map_db()?;
    Ok(r.rows_affected() == 1)
}

/// Move a deposit to `reorged`. `reversal` is set only when the deposit had
/// already been credited and a compensating ledger transaction was posted.
pub async fn mark_reorged(
    tx: &mut Tx<'_>,
    deposit_id: Uuid,
    reversal: Option<Uuid>,
    reason: &str,
) -> Result<bool> {
    let r = sqlx::query(
        r#"
        UPDATE deposits
           SET status = 'reorged',
               reversal_ledger_transaction_id = $2,
               failure_reason = $3
         WHERE id = $1 AND status <> 'reorged'
        "#,
    )
    .bind(deposit_id)
    .bind(reversal)
    .bind(reason)
    .execute(&mut **tx)
    .await
    .map_db()?;
    Ok(r.rows_affected() == 1)
}

/// Deposits belonging to transfers that were just orphaned.
pub async fn deposits_for_transfers<'e, E>(ex: E, transfer_ids: &[Uuid]) -> Result<Vec<Deposit>>
where
    E: Executor<'e, Database = Postgres>,
{
    if transfer_ids.is_empty() {
        return Ok(vec![]);
    }
    sqlx::query_as::<_, Deposit>(deposit_select!("WHERE blockchain_transaction_id = ANY($1)"))
        .bind(transfer_ids)
        .fetch_all(ex)
        .await
        .map_db()
}

/// Lock a single deposit row for update. Used on the credit path so that the
/// read-check-write of "is this still confirmed?" is atomic.
pub async fn lock_deposit(tx: &mut Tx<'_>, deposit_id: Uuid) -> Result<Deposit> {
    sqlx::query_as::<_, Deposit>(deposit_select!("WHERE id = $1 FOR UPDATE"))
        .bind(deposit_id)
        .fetch_optional(&mut **tx)
        .await
        .map_db()?
        .ok_or(Error::NotFound { entity: "deposit" })
}

pub async fn count_by_status<'e, E>(ex: E) -> Result<Vec<(String, i64)>>
where
    E: Executor<'e, Database = Postgres>,
{
    sqlx::query_as::<_, (String, i64)>("SELECT status, COUNT(*) FROM deposits GROUP BY status")
        .fetch_all(ex)
        .await
        .map_db()
}
