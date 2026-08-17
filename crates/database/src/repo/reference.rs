//! Users, assets and deposit addresses.

use chainrail_common::{Address, Error, Result};
use sqlx::{Executor, Postgres};
use uuid::Uuid;

use crate::models::{Asset, DepositAddress, User};
use crate::{SqlxResultExt, Tx};

/// Create a user, or return the existing one for the same `external_id`.
///
/// Idempotent by construction: `ON CONFLICT ... DO UPDATE` (a no-op update)
/// rather than `DO NOTHING`, because `DO NOTHING` returns no row and would
/// force a second round trip.
pub async fn create_user<'e, E>(ex: E, external_id: &str) -> Result<User>
where
    E: Executor<'e, Database = Postgres>,
{
    sqlx::query_as::<_, User>(
        r#"
        INSERT INTO users (external_id) VALUES ($1)
        ON CONFLICT (external_id) DO UPDATE SET external_id = EXCLUDED.external_id
        RETURNING id, external_id, created_at
        "#,
    )
    .bind(external_id)
    .fetch_one(ex)
    .await
    .map_db()
}

pub async fn get_user<'e, E>(ex: E, id: Uuid) -> Result<User>
where
    E: Executor<'e, Database = Postgres>,
{
    sqlx::query_as::<_, User>("SELECT id, external_id, created_at FROM users WHERE id = $1")
        .bind(id)
        .fetch_optional(ex)
        .await
        .map_db()?
        .ok_or(Error::NotFound { entity: "user" })
}

pub async fn get_user_by_external_id<'e, E>(ex: E, external_id: &str) -> Result<Option<User>>
where
    E: Executor<'e, Database = Postgres>,
{
    sqlx::query_as::<_, User>(
        "SELECT id, external_id, created_at FROM users WHERE external_id = $1",
    )
    .bind(external_id)
    .fetch_optional(ex)
    .await
    .map_db()
}

/// Register (or refresh) an asset from configuration. Called at boot so the
/// configured chains/assets and the database cannot drift apart.
pub async fn upsert_asset<'e, E>(
    ex: E,
    chain: &str,
    symbol: &str,
    contract_address: Option<&str>,
    decimals: u8,
) -> Result<Asset>
where
    E: Executor<'e, Database = Postgres>,
{
    sqlx::query_as::<_, Asset>(
        r#"
        INSERT INTO assets (chain, symbol, contract_address, decimals)
        VALUES ($1, $2, $3, $4)
        ON CONFLICT (chain, symbol) DO UPDATE
            SET contract_address = EXCLUDED.contract_address,
                decimals = EXCLUDED.decimals
        RETURNING id, symbol, chain, contract_address, decimals
        "#,
    )
    .bind(chain)
    .bind(symbol)
    .bind(contract_address.map(str::to_ascii_lowercase))
    .bind(i16::from(decimals))
    .fetch_one(ex)
    .await
    .map_db()
}

pub async fn get_asset<'e, E>(ex: E, chain: &str, symbol: &str) -> Result<Asset>
where
    E: Executor<'e, Database = Postgres>,
{
    sqlx::query_as::<_, Asset>(
        "SELECT id, symbol, chain, contract_address, decimals
           FROM assets WHERE chain = $1 AND symbol = $2",
    )
    .bind(chain)
    .bind(symbol)
    .fetch_optional(ex)
    .await
    .map_db()?
    .ok_or_else(|| Error::UnsupportedAsset(format!("{symbol} on {chain}")))
}

pub async fn get_asset_by_id<'e, E>(ex: E, id: Uuid) -> Result<Asset>
where
    E: Executor<'e, Database = Postgres>,
{
    sqlx::query_as::<_, Asset>(
        "SELECT id, symbol, chain, contract_address, decimals FROM assets WHERE id = $1",
    )
    .bind(id)
    .fetch_optional(ex)
    .await
    .map_db()?
    .ok_or(Error::NotFound { entity: "asset" })
}

pub async fn list_assets_for_chain<'e, E>(ex: E, chain: &str) -> Result<Vec<Asset>>
where
    E: Executor<'e, Database = Postgres>,
{
    sqlx::query_as::<_, Asset>(
        "SELECT id, symbol, chain, contract_address, decimals
           FROM assets WHERE chain = $1 ORDER BY symbol",
    )
    .bind(chain)
    .fetch_all(ex)
    .await
    .map_db()
}

pub async fn assign_deposit_address<'e, E>(
    ex: E,
    user_id: Uuid,
    chain: &str,
    address: &Address,
    derivation_path: Option<&str>,
) -> Result<DepositAddress>
where
    E: Executor<'e, Database = Postgres>,
{
    // Idempotent per (user, chain): repeating the request returns the existing
    // address instead of minting a second one the user might deposit to and
    // then find unmonitored.
    sqlx::query_as::<_, DepositAddress>(
        r#"
        INSERT INTO deposit_addresses (user_id, chain, address, derivation_path)
        VALUES ($1, $2, $3, $4)
        ON CONFLICT (user_id, chain) DO UPDATE SET user_id = EXCLUDED.user_id
        RETURNING id, user_id, chain, address, derivation_path, created_at
        "#,
    )
    .bind(user_id)
    .bind(chain)
    .bind(address.storage_key())
    .bind(derivation_path)
    .fetch_one(ex)
    .await
    .map_db()
}

/// Resolve the owner of a tracked deposit address. Returns `None` for
/// addresses ChainRail does not monitor, which is the common case when
/// scanning blocks.
pub async fn owner_of_address<'e, E>(
    ex: E,
    chain: &str,
    address_lower: &str,
) -> Result<Option<Uuid>>
where
    E: Executor<'e, Database = Postgres>,
{
    sqlx::query_scalar::<_, Uuid>(
        "SELECT user_id FROM deposit_addresses WHERE chain = $1 AND address = $2",
    )
    .bind(chain)
    .bind(address_lower)
    .fetch_optional(ex)
    .await
    .map_db()
}

/// Every monitored address on a chain. The watcher loads this into memory and
/// refreshes it periodically rather than querying per transaction.
pub async fn tracked_addresses<'e, E>(ex: E, chain: &str) -> Result<Vec<(String, Uuid)>>
where
    E: Executor<'e, Database = Postgres>,
{
    sqlx::query_as::<_, (String, Uuid)>(
        "SELECT address, user_id FROM deposit_addresses WHERE chain = $1",
    )
    .bind(chain)
    .fetch_all(ex)
    .await
    .map_db()
}

pub async fn get_deposit_address<'e, E>(
    ex: E,
    user_id: Uuid,
    chain: &str,
) -> Result<Option<DepositAddress>>
where
    E: Executor<'e, Database = Postgres>,
{
    sqlx::query_as::<_, DepositAddress>(
        "SELECT id, user_id, chain, address, derivation_path, created_at
           FROM deposit_addresses WHERE user_id = $1 AND chain = $2",
    )
    .bind(user_id)
    .bind(chain)
    .fetch_optional(ex)
    .await
    .map_db()
}

// ------------------------------------------------------------ address pool ---

/// Add an address to the pool. Idempotent on `(chain, address)`.
///
/// Called by the seeding utility with addresses exported from the custody
/// system; ChainRail never generates these itself.
pub async fn add_pool_address<'e, E>(
    ex: E,
    chain: &str,
    address: &Address,
    derivation_path: Option<&str>,
) -> Result<bool>
where
    E: Executor<'e, Database = Postgres>,
{
    let r = sqlx::query(
        "INSERT INTO deposit_address_pool (chain, address, derivation_path)
         VALUES ($1, $2, $3)
         ON CONFLICT (chain, address) DO NOTHING",
    )
    .bind(chain)
    .bind(address.storage_key())
    .bind(derivation_path)
    .execute(ex)
    .await
    .map_db()?;
    Ok(r.rows_affected() == 1)
}

/// Claim the next free pool address for a user and register it as their deposit
/// address, atomically.
///
/// `FOR UPDATE SKIP LOCKED` means concurrent requests take *different*
/// addresses rather than blocking or colliding; the partial unique index is the
/// backstop that guarantees an address is never handed out twice.
pub async fn claim_pool_address(
    tx: &mut Tx<'_>,
    user_id: Uuid,
    chain: &str,
) -> Result<DepositAddress> {
    // Already assigned? Return it -- assignment must be idempotent, or a retried
    // request would burn a second address the user might also deposit to.
    if let Some(existing) = get_deposit_address(&mut **tx, user_id, chain).await? {
        return Ok(existing);
    }

    let claimed = sqlx::query_as::<_, (Uuid, String, Option<String>)>(
        // The NOT EXISTS guard skips a pool row whose address is somehow already
        // a live deposit address. Without it, one inconsistent row sits at the
        // head of the queue and every assignment fails on the
        // `(chain, address)` unique constraint -- breaking address assignment for
        // every user, not just one.
        "SELECT p.id, p.address, p.derivation_path
           FROM deposit_address_pool p
          WHERE p.chain = $1
            AND p.assigned_to IS NULL
            AND NOT EXISTS (
                SELECT 1 FROM deposit_addresses d
                 WHERE d.chain = p.chain AND d.address = p.address
            )
          ORDER BY p.created_at
          LIMIT 1
            FOR UPDATE OF p SKIP LOCKED",
    )
    .bind(chain)
    .fetch_optional(&mut **tx)
    .await
    .map_db()?;

    let (pool_id, address, derivation_path) = claimed.ok_or_else(|| {
        // Running dry is an operational failure, not a client error: the caller
        // did nothing wrong and retrying will not help until the pool is topped up.
        Error::Unavailable(format!(
            "no unassigned deposit addresses remain for chain `{chain}`; top up the pool"
        ))
    })?;

    sqlx::query(
        "UPDATE deposit_address_pool SET assigned_to = $2, assigned_at = now() WHERE id = $1",
    )
    .bind(pool_id)
    .bind(user_id)
    .execute(&mut **tx)
    .await
    .map_db()?;

    let addr = Address::from_storage(address);
    assign_deposit_address(&mut **tx, user_id, chain, &addr, derivation_path.as_deref()).await
}

/// Free and assigned counts per chain, for the pool-exhaustion metric.
pub async fn pool_stats<'e, E>(ex: E) -> Result<Vec<(String, i64, i64)>>
where
    E: Executor<'e, Database = Postgres>,
{
    sqlx::query_as::<_, (String, Option<i64>, Option<i64>)>(
        "SELECT chain, available, assigned FROM deposit_address_pool_stats ORDER BY chain",
    )
    .fetch_all(ex)
    .await
    .map_db()
    .map(|rows| {
        rows.into_iter()
            .map(|(c, a, b)| (c, a.unwrap_or(0), b.unwrap_or(0)))
            .collect()
    })
}
