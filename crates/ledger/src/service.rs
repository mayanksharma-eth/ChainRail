//! The core ledger operations: posting, balances, statements.

use chainrail_common::{Amount, Error, Result};
use chainrail_database::models::{AccountBalance, LedgerEntryView, LedgerTransaction};
use chainrail_database::{SqlxResultExt, Tx};
use chrono::{DateTime, Utc};
use sqlx::{Executor, Postgres};
use uuid::Uuid;

use crate::posting::Posting;

#[derive(Debug, Clone)]
pub enum PostResult {
    /// Entries were written by this call.
    Posted(LedgerTransaction),
    /// An earlier call already posted this idempotency key; nothing was
    /// written and the original transaction is returned unchanged.
    AlreadyPosted(LedgerTransaction),
}

impl PostResult {
    pub fn transaction(&self) -> &LedgerTransaction {
        match self {
            PostResult::Posted(t) | PostResult::AlreadyPosted(t) => t,
        }
    }
    pub fn id(&self) -> Uuid {
        self.transaction().id
    }
    pub fn was_posted(&self) -> bool {
        matches!(self, PostResult::Posted(_))
    }
}

const TX_COLUMNS: &str =
    "id, kind, reference_type, reference_id, idempotency_key, correlation_id, description, created_at";

/// Post a balanced set of entries atomically.
///
/// Guarantees:
///   * Runs inside the caller's transaction, so the posting commits together
///     with whatever business change caused it.
///   * Idempotent on `posting.idempotency_key`: a second call with the same key
///     writes nothing and returns the original transaction.
///   * Entries are inserted in account-id order, so concurrent postings touching
///     the same accounts cannot deadlock.
///   * Balance and non-negativity are enforced by database triggers, not here.
pub async fn post_transaction(tx: &mut Tx<'_>, posting: &Posting) -> Result<PostResult> {
    posting.validate()?;

    let inserted = sqlx::query_as::<_, LedgerTransaction>(concat!(
        "INSERT INTO ledger_transactions",
        " (kind, reference_type, reference_id, idempotency_key, correlation_id, description)",
        " VALUES ($1, $2, $3, $4, $5, $6)",
        " ON CONFLICT (idempotency_key) DO NOTHING",
        " RETURNING id, kind, reference_type, reference_id, idempotency_key,",
        " correlation_id, description, created_at"
    ))
    .bind(posting.kind.as_str())
    .bind(posting.reference_type.as_str())
    .bind(posting.reference_id)
    .bind(&posting.idempotency_key)
    .bind(&posting.correlation_id)
    .bind(&posting.description)
    .fetch_optional(&mut **tx)
    .await
    .map_db()?;

    let Some(ledger_tx) = inserted else {
        // Another caller (or an earlier attempt) already posted this key. Under
        // READ COMMITTED the ON CONFLICT above waited for that transaction to
        // finish, so the row is visible to this fresh statement.
        let existing = get_transaction_by_key(&mut **tx, &posting.idempotency_key).await?;
        metrics::counter!("chainrail_ledger_postings_total", "result" => "duplicate").increment(1);
        return Ok(PostResult::AlreadyPosted(existing));
    };

    for entry in posting.ordered_entries() {
        sqlx::query(concat!(
            "INSERT INTO ledger_entries",
            " (ledger_transaction_id, account_id, asset_id, amount, direction)",
            " VALUES ($1, $2, $3, $4, $5)"
        ))
        .bind(ledger_tx.id)
        .bind(entry.account_id)
        .bind(entry.asset_id)
        .bind(entry.amount)
        .bind(entry.direction.as_str())
        .execute(&mut **tx)
        .await
        .map_db()?;
    }

    metrics::counter!("chainrail_ledger_postings_total", "result" => "posted").increment(1);
    tracing::info!(
        ledger_transaction_id = %ledger_tx.id,
        kind = posting.kind.as_str(),
        entries = posting.entries.len(),
        "ledger transaction posted"
    );
    Ok(PostResult::Posted(ledger_tx))
}

pub async fn get_transaction_by_key<'e, E>(
    ex: E,
    idempotency_key: &str,
) -> Result<LedgerTransaction>
where
    E: Executor<'e, Database = Postgres>,
{
    sqlx::query_as::<_, LedgerTransaction>(concat!(
        "SELECT id, kind, reference_type, reference_id, idempotency_key,",
        " correlation_id, description, created_at",
        " FROM ledger_transactions WHERE idempotency_key = $1"
    ))
    .bind(idempotency_key)
    .fetch_optional(ex)
    .await
    .map_db()?
    .ok_or(Error::NotFound {
        entity: "ledger transaction",
    })
}

pub async fn get_transaction<'e, E>(ex: E, id: Uuid) -> Result<LedgerTransaction>
where
    E: Executor<'e, Database = Postgres>,
{
    sqlx::query_as::<_, LedgerTransaction>(concat!(
        "SELECT id, kind, reference_type, reference_id, idempotency_key,",
        " correlation_id, description, created_at",
        " FROM ledger_transactions WHERE id = $1"
    ))
    .bind(id)
    .fetch_optional(ex)
    .await
    .map_db()?
    .ok_or(Error::NotFound {
        entity: "ledger transaction",
    })
}

/// A user's spendable balance for one asset.
pub async fn get_balance<'e, E>(ex: E, user_id: Uuid, asset_id: Uuid) -> Result<Amount>
where
    E: Executor<'e, Database = Postgres>,
{
    crate::accounts::user_balance(
        ex,
        chainrail_database::models::AccountType::UserAvailable,
        user_id,
        asset_id,
    )
    .await
}

/// Every non-zero balance a user holds, across assets and account types.
pub async fn get_balances<'e, E>(ex: E, user_id: Uuid) -> Result<Vec<AccountBalance>>
where
    E: Executor<'e, Database = Postgres>,
{
    sqlx::query_as::<_, AccountBalance>(
        r#"
        SELECT la.account_type   AS account_type,
               la.asset_id       AS asset_id,
               a.symbol          AS asset_symbol,
               a.decimals        AS asset_decimals,
               a.chain           AS chain,
               la.balance        AS balance
          FROM ledger_accounts la
          JOIN assets a ON a.id = la.asset_id
         WHERE la.owner_user_id = $1
         ORDER BY a.chain, a.symbol, la.account_type
        "#,
    )
    .bind(user_id)
    .fetch_all(ex)
    .await
    .map_db()
}

/// A user's statement: every entry against any of their accounts, newest first.
///
/// Keyset pagination on `(created_at, id)` -- see `repo::deposits::list_deposits`
/// for why offsets are avoided.
pub async fn get_ledger_entries<'e, E>(
    ex: E,
    user_id: Uuid,
    after: Option<(DateTime<Utc>, Uuid)>,
    limit: i64,
) -> Result<Vec<LedgerEntryView>>
where
    E: Executor<'e, Database = Postgres>,
{
    sqlx::query_as::<_, LedgerEntryView>(
        r#"
        SELECT e.id                    AS id,
               e.ledger_transaction_id AS ledger_transaction_id,
               la.account_type         AS account_type,
               a.symbol                AS asset_symbol,
               a.decimals              AS asset_decimals,
               e.amount                AS amount,
               e.direction             AS direction,
               e.balance_after         AS balance_after,
               lt.kind                 AS kind,
               lt.reference_type       AS reference_type,
               lt.reference_id         AS reference_id,
               e.created_at            AS created_at
          FROM ledger_entries e
          JOIN ledger_accounts la ON la.id = e.account_id
          JOIN ledger_transactions lt ON lt.id = e.ledger_transaction_id
          JOIN assets a ON a.id = e.asset_id
         WHERE la.owner_user_id = $1
           AND ($2::timestamptz IS NULL OR (e.created_at, e.id) < ($2, $3))
         ORDER BY e.created_at DESC, e.id DESC
         LIMIT $4
        "#,
    )
    .bind(user_id)
    .bind(after.map(|(t, _)| t))
    .bind(after.map(|(_, i)| i).unwrap_or_else(Uuid::nil))
    .bind(limit)
    .fetch_all(ex)
    .await
    .map_db()
}

/// All entries belonging to one ledger transaction, for audit and for proving
/// the transaction balances.
pub async fn get_transaction_entries<'e, E>(
    ex: E,
    ledger_transaction_id: Uuid,
) -> Result<Vec<LedgerEntryView>>
where
    E: Executor<'e, Database = Postgres>,
{
    sqlx::query_as::<_, LedgerEntryView>(
        r#"
        SELECT e.id                    AS id,
               e.ledger_transaction_id AS ledger_transaction_id,
               la.account_type         AS account_type,
               a.symbol                AS asset_symbol,
               a.decimals              AS asset_decimals,
               e.amount                AS amount,
               e.direction             AS direction,
               e.balance_after         AS balance_after,
               lt.kind                 AS kind,
               lt.reference_type       AS reference_type,
               lt.reference_id         AS reference_id,
               e.created_at            AS created_at
          FROM ledger_entries e
          JOIN ledger_accounts la ON la.id = e.account_id
          JOIN ledger_transactions lt ON lt.id = e.ledger_transaction_id
          JOIN assets a ON a.id = e.asset_id
         WHERE e.ledger_transaction_id = $1
         ORDER BY e.direction DESC, e.id
        "#,
    )
    .bind(ledger_transaction_id)
    .fetch_all(ex)
    .await
    .map_db()
}

pub fn tx_columns() -> &'static str {
    TX_COLUMNS
}
