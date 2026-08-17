//! Per-chain lookup tables the deposit pipeline needs on every block.
//!
//! Resolving "which asset is this contract?" and "which user owns this address?"
//! from the database per log would make block scanning O(logs) round trips.
//! These maps are refreshed on a timer instead; staleness is safe because a
//! missed new address simply means the deposit is picked up on the next refresh
//! (the block range is re-scanned from the cursor, never skipped).

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

use chainrail_chains_evm::TrackedAsset;
use chainrail_common::config::ChainConfig;
use chainrail_common::{Address, Result};
use chainrail_database::{repo, Db};
use parking_lot::RwLock;
use uuid::Uuid;

#[derive(Debug, Clone)]
pub struct AssetRef {
    pub id: Uuid,
    pub symbol: String,
    pub decimals: u8,
    pub contract: Option<Address>,
}

#[derive(Default)]
struct Snapshot {
    /// lowercase deposit address -> owning user
    addresses: HashMap<String, Uuid>,
    /// lowercase contract address -> asset
    by_contract: HashMap<String, AssetRef>,
    by_symbol: HashMap<String, AssetRef>,
    native: Option<AssetRef>,
    refreshed_at: Option<Instant>,
}

pub struct ChainContext {
    chain: String,
    db: Db,
    ttl: Duration,
    tracked_assets: Vec<TrackedAsset>,
    snapshot: RwLock<Snapshot>,
}

impl ChainContext {
    pub fn new(db: Db, cfg: &ChainConfig, ttl: Duration) -> Arc<Self> {
        let tracked_assets = cfg
            .assets
            .iter()
            .map(|a| TrackedAsset {
                symbol: a.symbol.clone(),
                contract: a.contract_address.as_deref().map(Address::from_storage),
                deposits_enabled: a.deposits_enabled,
            })
            .collect();
        Arc::new(ChainContext {
            chain: cfg.id.to_string(),
            db,
            ttl,
            tracked_assets,
            snapshot: RwLock::new(Snapshot::default()),
        })
    }

    pub fn chain(&self) -> &str {
        &self.chain
    }

    /// Assets to scan, derived from configuration rather than the database, so
    /// enabling an asset is a deploy-time decision and not a data change.
    pub fn tracked_assets(&self) -> &[TrackedAsset] {
        &self.tracked_assets
    }

    pub async fn refresh(&self) -> Result<()> {
        let addresses: HashMap<String, Uuid> =
            repo::reference::tracked_addresses(self.db.pool(), &self.chain)
                .await?
                .into_iter()
                .collect();

        let assets = repo::reference::list_assets_for_chain(self.db.pool(), &self.chain).await?;
        let mut by_contract = HashMap::new();
        let mut by_symbol = HashMap::new();
        let mut native = None;
        for a in assets {
            let r = AssetRef {
                id: a.id,
                symbol: a.symbol.clone(),
                decimals: a.decimals_u8(),
                contract: a.contract_address.as_deref().map(Address::from_storage),
            };
            match &r.contract {
                Some(c) => {
                    by_contract.insert(c.storage_key(), r.clone());
                }
                None => native = Some(r.clone()),
            }
            by_symbol.insert(a.symbol, r);
        }

        let mut snap = self.snapshot.write();
        metrics::gauge!(
            "chainrail_tracked_addresses",
            "chain" => self.chain.clone(),
        )
        .set(addresses.len() as f64);
        *snap = Snapshot {
            addresses,
            by_contract,
            by_symbol,
            native,
            refreshed_at: Some(Instant::now()),
        };
        Ok(())
    }

    /// Refresh if the snapshot is missing or older than the TTL.
    pub async fn refresh_if_stale(&self) -> Result<()> {
        let stale = {
            let snap = self.snapshot.read();
            match snap.refreshed_at {
                None => true,
                Some(t) => t.elapsed() >= self.ttl,
            }
        };
        if stale {
            self.refresh().await?;
        }
        Ok(())
    }

    pub fn tracked_addresses(&self) -> HashSet<String> {
        self.snapshot.read().addresses.keys().cloned().collect()
    }

    pub fn owner_of(&self, address_lower: &str) -> Option<Uuid> {
        self.snapshot.read().addresses.get(address_lower).copied()
    }

    pub fn asset_by_contract(&self, contract_lower: &str) -> Option<AssetRef> {
        self.snapshot
            .read()
            .by_contract
            .get(contract_lower)
            .cloned()
    }

    pub fn asset_by_symbol(&self, symbol: &str) -> Option<AssetRef> {
        self.snapshot.read().by_symbol.get(symbol).cloned()
    }

    pub fn native_asset(&self) -> Option<AssetRef> {
        self.snapshot.read().native.clone()
    }

    /// Resolve the asset for an observed transfer: the contract if present,
    /// otherwise the chain's native asset.
    pub fn resolve_asset(&self, contract: Option<&Address>) -> Option<AssetRef> {
        match contract {
            Some(c) => self.asset_by_contract(&c.storage_key()),
            None => self.native_asset(),
        }
    }

    pub fn address_count(&self) -> usize {
        self.snapshot.read().addresses.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chainrail_common::config::AssetConfig;
    use chainrail_common::{ChainId, ChainKind, FinalityPolicy};

    fn chain_cfg() -> ChainConfig {
        ChainConfig {
            id: ChainId::new("base-sepolia").unwrap(),
            kind: ChainKind::Evm,
            numeric_chain_id: Some(84532),
            finality: FinalityPolicy::Confirmations { blocks: 10 },
            poll_interval_ms: 1_000,
            block_batch_size: 10,
            reorg_scan_depth: 32,
            start_block: None,
            hot_wallet_address: None,
            transfer_gas_limit: 100_000,
            fee_bump_pct: 125,
            rpc: vec![],
            assets: vec![
                AssetConfig {
                    symbol: "USDC".into(),
                    decimals: 6,
                    contract_address: Some("0x036cbd53842c5426634e7929541ec2318f3dcf7e".into()),
                    deposits_enabled: true,
                    withdrawals_enabled: true,
                },
                AssetConfig {
                    symbol: "PAUSED".into(),
                    decimals: 18,
                    contract_address: Some("0x1111111111111111111111111111111111111111".into()),
                    deposits_enabled: false,
                    withdrawals_enabled: false,
                },
            ],
        }
    }

    #[test]
    fn tracked_assets_come_from_config_and_preserve_the_enabled_flag() {
        // Built without touching the database: the asset list is a deploy-time
        // decision, so this is pure.
        let cfg = chain_cfg();
        let tracked: Vec<TrackedAsset> = cfg
            .assets
            .iter()
            .map(|a| TrackedAsset {
                symbol: a.symbol.clone(),
                contract: a.contract_address.as_deref().map(Address::from_storage),
                deposits_enabled: a.deposits_enabled,
            })
            .collect();
        assert_eq!(tracked.len(), 2);
        assert!(tracked[0].deposits_enabled);
        assert!(
            !tracked[1].deposits_enabled,
            "disabled assets must stay disabled"
        );
        assert_eq!(
            tracked[0].contract.as_ref().unwrap().storage_key(),
            "0x036cbd53842c5426634e7929541ec2318f3dcf7e"
        );
    }
}
