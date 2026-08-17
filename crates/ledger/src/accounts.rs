//! Account resolution.
//!
//! Accounts are created lazily and idempotently on first use, so there is no
//! provisioning step to forget and no window where a deposit arrives for a user
//! whose accounts do not exist yet.

use chainrail_common::{Error, Result};
use chainrail_database::models::{AccountType, LedgerAccount};
use chainrail_database::{SqlxResultExt, Tx};
use sqlx::{Executor, Postgres};
use uuid::Uuid;

const ACCOUNT_COLUMNS: &str =
    "id, account_type, owner_user_id, asset_id, normal_balance, balance, allow_negative";

/// Resolve (creating if needed) a user-scoped account.
pub async fn user_account(
    tx: &mut Tx<'_>,
    account_type: AccountType,
    user_id: Uuid,
    asset_id: Uuid,
) -> Result<Uuid> {
    if !account_type.is_user_scoped() {
        return Err(Error::Internal(format!(
            "{account_type} is not a user-scoped account type"
        )));
    }
    let existing = sqlx::query_scalar::<_, Uuid>(
        "SELECT id FROM ledger_accounts
          WHERE account_type = $1 AND owner_user_id = $2 AND asset_id = $3",
    )
    .bind(account_type.as_str())
    .bind(user_id)
    .bind(asset_id)
    .fetch_optional(&mut **tx)
    .await
    .map_db()?;
    if let Some(id) = existing {
        return Ok(id);
    }

    // `DO NOTHING`, deliberately not `DO UPDATE`.
    //
    // `DO UPDATE` would take a row lock on the conflicting account and hold it
    // for the rest of the transaction. A posting resolves two accounts, so two
    // concurrent postings that resolve them in opposite orders would deadlock
    // -- which is exactly what happened before this was changed. `DO NOTHING`
    // takes no lasting lock, so account creation can never contribute to a
    // lock cycle.
    let created = sqlx::query_scalar::<_, Uuid>(
        r#"
        INSERT INTO ledger_accounts
            (account_type, owner_user_id, asset_id, normal_balance, allow_negative)
        VALUES ($1, $2, $3, $4, $5)
        ON CONFLICT (account_type, owner_user_id, asset_id) WHERE owner_user_id IS NOT NULL
        DO NOTHING
        RETURNING id
        "#,
    )
    .bind(account_type.as_str())
    .bind(user_id)
    .bind(asset_id)
    .bind(account_type.normal_balance().as_str())
    .bind(account_type.allows_negative())
    .fetch_optional(&mut **tx)
    .await
    .map_db()?;
    if let Some(id) = created {
        return Ok(id);
    }

    // Lost the creation race; the winner has committed by now.
    sqlx::query_scalar::<_, Uuid>(
        "SELECT id FROM ledger_accounts
          WHERE account_type = $1 AND owner_user_id = $2 AND asset_id = $3",
    )
    .bind(account_type.as_str())
    .bind(user_id)
    .bind(asset_id)
    .fetch_optional(&mut **tx)
    .await
    .map_db()?
    .ok_or_else(|| Error::Internal("ledger account vanished after insert conflict".into()))
}

/// Resolve (creating if needed) a system account: custody, clearing, fees.
pub async fn system_account(
    tx: &mut Tx<'_>,
    account_type: AccountType,
    asset_id: Uuid,
) -> Result<Uuid> {
    if account_type.is_user_scoped() {
        return Err(Error::Internal(format!(
            "{account_type} requires an owning user"
        )));
    }
    let existing = sqlx::query_scalar::<_, Uuid>(
        "SELECT id FROM ledger_accounts
          WHERE account_type = $1 AND owner_user_id IS NULL AND asset_id = $2",
    )
    .bind(account_type.as_str())
    .bind(asset_id)
    .fetch_optional(&mut **tx)
    .await
    .map_db()?;
    if let Some(id) = existing {
        return Ok(id);
    }

    // `DO NOTHING` for the same lock-ordering reason as `user_account`.
    let created = sqlx::query_scalar::<_, Uuid>(
        r#"
        INSERT INTO ledger_accounts
            (account_type, owner_user_id, asset_id, normal_balance, allow_negative)
        VALUES ($1, NULL, $2, $3, $4)
        ON CONFLICT (account_type, asset_id) WHERE owner_user_id IS NULL
        DO NOTHING
        RETURNING id
        "#,
    )
    .bind(account_type.as_str())
    .bind(asset_id)
    .bind(account_type.normal_balance().as_str())
    .bind(account_type.allows_negative())
    .fetch_optional(&mut **tx)
    .await
    .map_db()?;
    if let Some(id) = created {
        return Ok(id);
    }

    sqlx::query_scalar::<_, Uuid>(
        "SELECT id FROM ledger_accounts
          WHERE account_type = $1 AND owner_user_id IS NULL AND asset_id = $2",
    )
    .bind(account_type.as_str())
    .bind(asset_id)
    .fetch_optional(&mut **tx)
    .await
    .map_db()?
    .ok_or_else(|| Error::Internal("ledger account vanished after insert conflict".into()))
}

pub async fn get_account<'e, E>(ex: E, id: Uuid) -> Result<LedgerAccount>
where
    E: Executor<'e, Database = Postgres>,
{
    sqlx::query_as::<_, LedgerAccount>(concat!(
        "SELECT id, account_type, owner_user_id, asset_id, normal_balance, balance, allow_negative",
        " FROM ledger_accounts WHERE id = $1"
    ))
    .bind(id)
    .fetch_optional(ex)
    .await
    .map_db()?
    .ok_or(Error::NotFound {
        entity: "ledger account",
    })
}

/// Read a user account's balance without creating it. A user who has never
/// transacted has no account row, which is a zero balance -- not an error.
pub async fn user_balance<'e, E>(
    ex: E,
    account_type: AccountType,
    user_id: Uuid,
    asset_id: Uuid,
) -> Result<chainrail_common::Amount>
where
    E: Executor<'e, Database = Postgres>,
{
    let bal = sqlx::query_scalar::<_, chainrail_common::Amount>(
        "SELECT balance FROM ledger_accounts
          WHERE account_type = $1 AND owner_user_id = $2 AND asset_id = $3",
    )
    .bind(account_type.as_str())
    .bind(user_id)
    .bind(asset_id)
    .fetch_optional(ex)
    .await
    .map_db()?;
    Ok(bal.unwrap_or(chainrail_common::Amount::ZERO))
}

/// Lock a user's spendable account for the duration of the transaction.
///
/// Used by the withdrawal path to serialise the read-check-reserve sequence
/// against concurrent withdrawals for the same user. The database CHECK
/// constraint is the ultimate guarantee, but taking the lock up front turns a
/// constraint violation into an orderly queue and yields far better error
/// messages under contention.
pub async fn lock_user_account(
    tx: &mut Tx<'_>,
    account_type: AccountType,
    user_id: Uuid,
    asset_id: Uuid,
) -> Result<Option<LedgerAccount>> {
    sqlx::query_as::<_, LedgerAccount>(concat!(
        "SELECT id, account_type, owner_user_id, asset_id, normal_balance, balance, allow_negative",
        " FROM ledger_accounts",
        " WHERE account_type = $1 AND owner_user_id = $2 AND asset_id = $3 FOR UPDATE"
    ))
    .bind(account_type.as_str())
    .bind(user_id)
    .bind(asset_id)
    .fetch_optional(&mut **tx)
    .await
    .map_db()
}

pub fn columns() -> &'static str {
    ACCOUNT_COLUMNS
}
