//! Block lineage, watcher cursor, and observed transfers.

use chainrail_common::{Hash32, Result};
use sqlx::{Executor, Postgres};
use uuid::Uuid;

use crate::models::{BlockchainTransaction, ChainBlock, ChainCursor, ObservedTransfer};
use crate::{SqlxResultExt, Tx};

pub async fn get_cursor<'e, E>(ex: E, chain: &str) -> Result<Option<ChainCursor>>
where
    E: Executor<'e, Database = Postgres>,
{
    sqlx::query_as::<_, ChainCursor>(
        "SELECT chain, last_processed_height, last_processed_hash, head_height, updated_at
           FROM chain_cursors WHERE chain = $1",
    )
    .bind(chain)
    .fetch_optional(ex)
    .await
    .map_db()
}

/// Advance the watcher cursor. Called only after the block's contents are
/// durably persisted in the same transaction, so a crash re-processes rather
/// than skips (at-least-once, made safe by the idempotent transfer insert).
pub async fn set_cursor<'e, E>(
    ex: E,
    chain: &str,
    height: u64,
    hash: &Hash32,
    head_height: u64,
) -> Result<()>
where
    E: Executor<'e, Database = Postgres>,
{
    sqlx::query(
        r#"
        INSERT INTO chain_cursors (chain, last_processed_height, last_processed_hash, head_height)
        VALUES ($1, $2, $3, $4)
        ON CONFLICT (chain) DO UPDATE
            SET last_processed_height = EXCLUDED.last_processed_height,
                last_processed_hash   = EXCLUDED.last_processed_hash,
                head_height           = EXCLUDED.head_height,
                updated_at            = now()
        "#,
    )
    .bind(chain)
    .bind(height as i64)
    .bind(hash)
    .bind(head_height as i64)
    .execute(ex)
    .await
    .map_db()?;
    Ok(())
}

/// Record a block as canonical.
///
/// Fails with a conflict if another block already holds that height as
/// canonical -- by design. The caller must orphan the incumbent first, which
/// forces reorg handling to be explicit rather than accidental.
pub async fn insert_canonical_block<'e, E>(
    ex: E,
    chain: &str,
    hash: &Hash32,
    height: u64,
    parent_hash: &Hash32,
) -> Result<()>
where
    E: Executor<'e, Database = Postgres>,
{
    sqlx::query(
        r#"
        INSERT INTO chain_blocks (chain, hash, height, parent_hash, status)
        VALUES ($1, $2, $3, $4, 'canonical')
        ON CONFLICT (chain, hash) DO UPDATE
            SET status = 'canonical', orphaned_at = NULL
        "#,
    )
    .bind(chain)
    .bind(hash)
    .bind(height as i64)
    .bind(parent_hash)
    .execute(ex)
    .await
    .map_db()?;
    Ok(())
}

pub async fn get_canonical_block_at<'e, E>(
    ex: E,
    chain: &str,
    height: u64,
) -> Result<Option<ChainBlock>>
where
    E: Executor<'e, Database = Postgres>,
{
    sqlx::query_as::<_, ChainBlock>(
        "SELECT chain, hash, height, parent_hash, status, observed_at
           FROM chain_blocks WHERE chain = $1 AND height = $2 AND status = 'canonical'",
    )
    .bind(chain)
    .bind(height as i64)
    .fetch_optional(ex)
    .await
    .map_db()
}

pub async fn get_block<'e, E>(ex: E, chain: &str, hash: &Hash32) -> Result<Option<ChainBlock>>
where
    E: Executor<'e, Database = Postgres>,
{
    sqlx::query_as::<_, ChainBlock>(
        "SELECT chain, hash, height, parent_hash, status, observed_at
           FROM chain_blocks WHERE chain = $1 AND hash = $2",
    )
    .bind(chain)
    .bind(hash)
    .fetch_optional(ex)
    .await
    .map_db()
}

/// Canonical blocks at or above `from_height`, highest first. Used by the reorg
/// engine when walking back to the common ancestor.
pub async fn canonical_blocks_from<'e, E>(
    ex: E,
    chain: &str,
    from_height: u64,
) -> Result<Vec<ChainBlock>>
where
    E: Executor<'e, Database = Postgres>,
{
    sqlx::query_as::<_, ChainBlock>(
        "SELECT chain, hash, height, parent_hash, status, observed_at
           FROM chain_blocks
          WHERE chain = $1 AND height >= $2 AND status = 'canonical'
          ORDER BY height DESC",
    )
    .bind(chain)
    .bind(from_height as i64)
    .fetch_all(ex)
    .await
    .map_db()
}

/// Demote every canonical block above `above_height` to orphaned, returning the
/// hashes that were orphaned. Freeing the unique canonical-height index is what
/// lets the replacement chain be inserted.
pub async fn orphan_blocks_above(
    tx: &mut Tx<'_>,
    chain: &str,
    above_height: u64,
) -> Result<Vec<Hash32>> {
    sqlx::query_scalar::<_, Hash32>(
        r#"
        UPDATE chain_blocks
           SET status = 'orphaned', orphaned_at = now()
         WHERE chain = $1 AND height > $2 AND status = 'canonical'
        RETURNING hash
        "#,
    )
    .bind(chain)
    .bind(above_height as i64)
    .fetch_all(&mut **tx)
    .await
    .map_db()
}

pub async fn highest_canonical_height<'e, E>(ex: E, chain: &str) -> Result<Option<i64>>
where
    E: Executor<'e, Database = Postgres>,
{
    sqlx::query_scalar::<_, Option<i64>>(
        "SELECT MAX(height) FROM chain_blocks WHERE chain = $1 AND status = 'canonical'",
    )
    .bind(chain)
    .fetch_one(ex)
    .await
    .map_db()
}

/// Delete block metadata older than the reorg scan window. Bounded retention
/// keeps `chain_blocks` from growing without limit; blocks this old cannot be
/// reorganised in any realistic scenario.
pub async fn prune_blocks_below<'e, E>(ex: E, chain: &str, height: u64) -> Result<u64>
where
    E: Executor<'e, Database = Postgres>,
{
    // Blocks still referenced by a transfer are kept: the FK from
    // blockchain_transactions is ON DELETE RESTRICT precisely so pruning can
    // never orphan a transfer's provenance.
    let r = sqlx::query(
        r#"
        DELETE FROM chain_blocks b
         WHERE b.chain = $1 AND b.height < $2
           AND NOT EXISTS (
               SELECT 1 FROM blockchain_transactions t
                WHERE t.chain = b.chain AND t.block_hash = b.hash
           )
        "#,
    )
    .bind(chain)
    .bind(height as i64)
    .execute(ex)
    .await
    .map_db()?;
    Ok(r.rows_affected())
}

/// Persist an observed transfer.
///
/// Returns `None` when the transfer was already recorded. This is the primary
/// deduplication point for the deposit path: block replays, duplicate Kafka
/// deliveries, and racing watcher replicas all collapse here, enforced by the
/// `(chain, tx_hash, log_index)` unique constraint rather than by a lock.
pub async fn insert_transfer<'e, E>(
    ex: E,
    transfer: &ObservedTransfer,
    asset_id: Uuid,
) -> Result<Option<BlockchainTransaction>>
where
    E: Executor<'e, Database = Postgres>,
{
    sqlx::query_as::<_, BlockchainTransaction>(
        r#"
        INSERT INTO blockchain_transactions
            (chain, tx_hash, log_index, block_number, block_hash,
             from_address, to_address, asset_id, amount_raw, status)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, 'observed')
        ON CONFLICT (chain, tx_hash, log_index) DO NOTHING
        RETURNING id, chain, tx_hash, log_index, block_number, block_hash,
                  from_address, to_address, asset_id, amount_raw, status, observed_at
        "#,
    )
    .bind(transfer.chain.as_str())
    .bind(&transfer.tx_hash)
    .bind(transfer.log_index)
    .bind(transfer.block_number as i64)
    .bind(&transfer.block_hash)
    .bind(transfer.from_address.storage_key())
    .bind(transfer.to_address.storage_key())
    .bind(asset_id)
    .bind(transfer.amount_raw)
    .fetch_optional(ex)
    .await
    .map_db()
}

pub async fn get_transfer_by_hash<'e, E>(
    ex: E,
    tx_hash: &Hash32,
) -> Result<Vec<BlockchainTransaction>>
where
    E: Executor<'e, Database = Postgres>,
{
    sqlx::query_as::<_, BlockchainTransaction>(
        "SELECT id, chain, tx_hash, log_index, block_number, block_hash,
                from_address, to_address, asset_id, amount_raw, status, observed_at
           FROM blockchain_transactions WHERE tx_hash = $1 ORDER BY log_index",
    )
    .bind(tx_hash)
    .fetch_all(ex)
    .await
    .map_db()
}

/// Mark every transfer that came from an orphaned block. Returns the affected
/// transfer ids so the deposit engine can decide what to do with each deposit.
pub async fn mark_transfers_orphaned(
    tx: &mut Tx<'_>,
    chain: &str,
    block_hashes: &[Hash32],
) -> Result<Vec<Uuid>> {
    if block_hashes.is_empty() {
        return Ok(vec![]);
    }
    let hashes: Vec<String> = block_hashes
        .iter()
        .map(|h| h.as_str().to_string())
        .collect();
    sqlx::query_scalar::<_, Uuid>(
        r#"
        UPDATE blockchain_transactions
           SET status = 'orphaned'
         WHERE chain = $1 AND block_hash = ANY($2) AND status <> 'orphaned'
        RETURNING id
        "#,
    )
    .bind(chain)
    .bind(&hashes)
    .fetch_all(&mut **tx)
    .await
    .map_db()
}
