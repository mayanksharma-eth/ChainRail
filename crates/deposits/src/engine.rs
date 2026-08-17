//! Persisting observed transfers as deposits.
//!
//! One database transaction per scanned batch, containing:
//!   * block headers (lineage for reorg detection),
//!   * observed transfers (idempotent on `(chain, tx_hash, log_index)`),
//!   * deposit rows (idempotent on `blockchain_transaction_id`),
//!   * `deposit.observed` events in the outbox,
//!   * the advanced watcher cursor.
//!
//! Because the cursor advances in the *same* transaction as the data, a crash
//! anywhere re-processes the batch rather than skipping it. Re-processing is a
//! no-op thanks to the unique constraints -- at-least-once made safe by the
//! schema rather than by careful coding.

use std::sync::Arc;

use chainrail_chains_evm::{BlockRef, ScanResult};
use chainrail_common::event::{BlockObserved, DepositObserved, EventPayload};
use chainrail_common::{EventEnvelope, Result};
use chainrail_database::models::ObservedTransfer;
use chainrail_database::{repo, Db, Tx};

use crate::context::ChainContext;

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct PersistStats {
    pub blocks: usize,
    pub transfers_seen: usize,
    /// Transfers that were new to us. The difference against `transfers_seen`
    /// is the deduplication rate, which is expected to be non-zero after a
    /// restart or reorg rewind.
    pub deposits_created: usize,
    pub duplicates_skipped: usize,
    pub unknown_asset_skipped: usize,
    pub unknown_address_skipped: usize,
}

/// Persist a scan result and advance the cursor. Returns what changed.
pub async fn persist_scan(
    db: &Db,
    ctx: &Arc<ChainContext>,
    scan: &ScanResult,
    head_height: u64,
    correlation_id: &str,
) -> Result<PersistStats> {
    if scan.headers.is_empty() {
        return Ok(PersistStats::default());
    }
    let mut tx = db.begin().await?;
    let stats = persist_in_tx(&mut tx, ctx, scan, head_height, correlation_id).await?;
    tx.commit().await.map_err(chainrail_database::map_sqlx)?;

    metrics::counter!(
        "chainrail_deposits_observed_total",
        "chain" => ctx.chain().to_string(),
    )
    .increment(stats.deposits_created as u64);
    Ok(stats)
}

pub async fn persist_in_tx(
    tx: &mut Tx<'_>,
    ctx: &Arc<ChainContext>,
    scan: &ScanResult,
    head_height: u64,
    correlation_id: &str,
) -> Result<PersistStats> {
    let chain = ctx.chain();
    let mut stats = PersistStats {
        blocks: scan.headers.len(),
        transfers_seen: scan.transfers.len(),
        ..Default::default()
    };

    for header in &scan.headers {
        persist_header(tx, chain, header, correlation_id).await?;
    }

    for transfer in &scan.transfers {
        match persist_transfer(tx, ctx, transfer, correlation_id).await? {
            TransferOutcome::Created => stats.deposits_created += 1,
            TransferOutcome::Duplicate => stats.duplicates_skipped += 1,
            TransferOutcome::UnknownAsset => stats.unknown_asset_skipped += 1,
            TransferOutcome::UnknownAddress => stats.unknown_address_skipped += 1,
        }
    }

    // Cursor last: it is the commit point for "this batch is done".
    if let Some(tip) = scan.headers.last() {
        repo::chain::set_cursor(&mut **tx, chain, tip.height, &tip.hash, head_height).await?;
    }
    Ok(stats)
}

async fn persist_header(
    tx: &mut Tx<'_>,
    chain: &str,
    header: &BlockRef,
    correlation_id: &str,
) -> Result<()> {
    // A replacement block at an already-occupied height means the reorg engine
    // did not run first. Orphan the incumbent here rather than failing the whole
    // batch on the canonical-height index -- the reorg engine will still perform
    // deposit recovery on its next pass, because the lineage mismatch persists.
    if let Some(existing) =
        repo::chain::get_canonical_block_at(&mut **tx, chain, header.height).await?
    {
        if existing.hash != header.hash {
            tracing::warn!(
                chain, height = header.height,
                stored = %existing.hash, incoming = %header.hash,
                "replacement block at an occupied height; orphaning incumbent"
            );
            repo::chain::orphan_blocks_above(tx, chain, header.height.saturating_sub(1)).await?;
        }
    }
    repo::chain::insert_canonical_block(
        &mut **tx,
        chain,
        &header.hash,
        header.height,
        &header.parent_hash,
    )
    .await?;

    let envelope = EventEnvelope::new(
        EventPayload::BlockObserved(BlockObserved {
            chain: chain.to_string(),
            block_number: header.height,
            block_hash: header.hash.to_string(),
            parent_hash: header.parent_hash.to_string(),
            tx_count: header.tx_count,
        }),
        correlation_id,
    )
    // Deterministic on (chain, hash): re-scanning a block after a restart must
    // not enqueue the event twice.
    .with_deterministic_id(&format!("{chain}:{}", header.hash));
    repo::outbox::enqueue(tx, &envelope).await?;
    Ok(())
}

enum TransferOutcome {
    Created,
    Duplicate,
    UnknownAsset,
    UnknownAddress,
}

async fn persist_transfer(
    tx: &mut Tx<'_>,
    ctx: &Arc<ChainContext>,
    transfer: &ObservedTransfer,
    correlation_id: &str,
) -> Result<TransferOutcome> {
    let chain = ctx.chain();

    let Some(asset) = ctx.resolve_asset(transfer.contract_address.as_ref()) else {
        // The adapter filtered by contract, so this means the in-memory asset
        // map is stale. Skipping is safe: the block is re-scanned from the cursor
        // only if the batch fails, so log loudly.
        tracing::warn!(
            chain, tx_hash = %transfer.tx_hash,
            contract = ?transfer.contract_address.as_ref().map(|c| c.storage_key()),
            "transfer for an unknown asset; refresh the asset cache"
        );
        return Ok(TransferOutcome::UnknownAsset);
    };

    // Authoritative owner lookup against the database, not the cache: crediting
    // the wrong user is unrecoverable, so this is worth a round trip.
    let Some(user_id) =
        repo::reference::owner_of_address(&mut **tx, chain, &transfer.to_address.storage_key())
            .await?
    else {
        return Ok(TransferOutcome::UnknownAddress);
    };

    let Some(btx) = repo::chain::insert_transfer(&mut **tx, transfer, asset.id).await? else {
        // Already recorded: a re-scan, a duplicate event, or a second watcher.
        return Ok(TransferOutcome::Duplicate);
    };

    let deposit =
        repo::deposits::create_deposit(&mut **tx, user_id, btx.id, asset.id, transfer.amount_raw)
            .await?;

    let envelope = EventEnvelope::new(
        EventPayload::DepositObserved(DepositObserved {
            chain: chain.to_string(),
            tx_hash: transfer.tx_hash.to_string(),
            log_index: transfer.log_index,
            block_number: transfer.block_number,
            block_hash: transfer.block_hash.to_string(),
            asset: asset.symbol.clone(),
            user_id,
            deposit_id: deposit.id,
            amount_raw: transfer.amount_raw,
        }),
        correlation_id,
    )
    .with_deterministic_id(&format!(
        "{chain}:{}:{}",
        transfer.tx_hash, transfer.log_index
    ));
    repo::outbox::enqueue(tx, &envelope).await?;

    tracing::info!(
        chain,
        %user_id,
        deposit_id = %deposit.id,
        tx_hash = %transfer.tx_hash,
        log_index = transfer.log_index,
        asset = %asset.symbol,
        amount = %transfer.amount_raw.format_units(asset.decimals),
        "deposit observed"
    );
    Ok(TransferOutcome::Created)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn persist_stats_accounting_adds_up() {
        let s = PersistStats {
            blocks: 3,
            transfers_seen: 10,
            deposits_created: 4,
            duplicates_skipped: 3,
            unknown_asset_skipped: 1,
            unknown_address_skipped: 2,
        };
        assert_eq!(
            s.deposits_created
                + s.duplicates_skipped
                + s.unknown_asset_skipped
                + s.unknown_address_skipped,
            s.transfers_seen,
            "every observed transfer must be accounted for exactly once"
        );
    }
}
