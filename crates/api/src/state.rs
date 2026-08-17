//! Shared application state and the readiness model.

use std::collections::HashMap;
use std::sync::Arc;

use chainrail_chains_evm::ChainAdapter;
use chainrail_common::config::AppConfig;
use chainrail_common::{Error, Result};
use chainrail_database::Db;
use chainrail_events::SharedBus;
use chainrail_observability::Metrics;
use chainrail_risk::RiskEngine;
use chainrail_rpc::RpcRegistry;
use chainrail_signer::SharedSigner;
use chainrail_withdrawals::WithdrawalService;

#[derive(Clone)]
pub struct AppState {
    pub db: Db,
    pub cfg: Arc<AppConfig>,
    pub bus: SharedBus,
    pub rpc: Arc<RpcRegistry>,
    pub adapters: Arc<HashMap<String, Arc<dyn ChainAdapter>>>,
    pub signer: SharedSigner,
    pub risk: Arc<RiskEngine>,
    pub withdrawals: Arc<WithdrawalService>,
    pub metrics: Option<Metrics>,
    pub started_at: std::time::Instant,
}

impl AppState {
    /// Wire everything together and register configured assets in the database.
    pub async fn build(cfg: Arc<AppConfig>, metrics: Option<Metrics>) -> Result<AppState> {
        let db = Db::connect(&cfg.database).await?;
        if cfg.database.run_migrations_on_boot {
            db.migrate().await?;
            tracing::info!("migrations applied");
        }

        let rpc = RpcRegistry::build(&cfg.chains)?;
        let adapters = Arc::new(chainrail_chains_evm::build_adapters(&cfg.chains, &rpc)?);
        let signer = chainrail_signer::from_config(&cfg.signer)?;
        let bus = chainrail_events::build_bus(&cfg.kafka)?;
        let risk = Arc::new(RiskEngine::new(cfg.risk.clone()));
        let withdrawals = WithdrawalService::new(db.clone(), Arc::clone(&cfg), Arc::clone(&risk));

        // Configuration is the source of truth for which assets exist, so sync
        // it into the database at boot. Otherwise a newly configured asset would
        // be invisible until someone ran a manual insert.
        sync_assets(&db, &cfg).await?;

        Ok(AppState {
            db,
            cfg,
            bus,
            rpc,
            adapters,
            signer,
            risk,
            withdrawals,
            metrics,
            started_at: std::time::Instant::now(),
        })
    }

    pub fn adapter(&self, chain: &str) -> Result<Arc<dyn ChainAdapter>> {
        self.adapters
            .get(chain)
            .cloned()
            .ok_or_else(|| Error::UnsupportedChain(chain.to_string()))
    }

    pub fn page_size(&self, requested: Option<u32>) -> i64 {
        let n = requested.unwrap_or(self.cfg.http.default_page_size);
        i64::from(n.clamp(1, self.cfg.http.max_page_size))
    }
}

/// Register every configured asset. Idempotent.
pub async fn sync_assets(db: &Db, cfg: &AppConfig) -> Result<()> {
    for chain in &cfg.chains {
        for asset in &chain.assets {
            chainrail_database::repo::reference::upsert_asset(
                db.pool(),
                chain.id.as_str(),
                &asset.symbol,
                asset.contract_address.as_deref(),
                asset.decimals,
            )
            .await?;
        }
        // Ensure the native gas asset exists even if it is not configured for
        // deposits, so withdrawal fee accounting has somewhere to post.
        let native_symbol = native_symbol_for(chain.id.as_str());
        if chain.assets.iter().all(|a| a.contract_address.is_some()) {
            chainrail_database::repo::reference::upsert_asset(
                db.pool(),
                chain.id.as_str(),
                &native_symbol,
                None,
                18,
            )
            .await?;
        }
    }
    Ok(())
}

fn native_symbol_for(chain: &str) -> String {
    if chain.starts_with("bsc") || chain.contains("binance") {
        "BNB".into()
    } else if chain.starts_with("polygon") {
        "POL".into()
    } else {
        "ETH".into()
    }
}

/// Per-dependency readiness, used by `/ready`.
#[derive(Debug, serde::Serialize)]
pub struct Readiness {
    pub ready: bool,
    pub database: DependencyStatus,
    pub event_bus: DependencyStatus,
    pub chains: Vec<ChainStatus>,
}

#[derive(Debug, serde::Serialize)]
pub struct DependencyStatus {
    pub ok: bool,
    /// Whether the service can serve traffic without this dependency.
    pub required: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
}

#[derive(Debug, serde::Serialize)]
pub struct ChainStatus {
    pub chain: String,
    pub rpc_available: bool,
    pub endpoints: Vec<chainrail_rpc::EndpointStatus>,
}

impl AppState {
    /// Readiness check.
    ///
    /// The database is the only hard requirement: without it nothing can be
    /// served correctly. Kafka and RPC outages degrade the system (deposits stop
    /// being detected, withdrawals stop being broadcast) but balance and history
    /// reads remain correct, so they do not fail readiness. That distinction
    /// keeps a provider outage from cascading into a full API outage.
    pub async fn readiness(&self) -> Readiness {
        let database = match self.db.ping().await {
            Ok(()) => DependencyStatus {
                ok: true,
                required: true,
                detail: None,
            },
            Err(e) => DependencyStatus {
                ok: false,
                required: true,
                detail: Some(e.to_string()),
            },
        };

        let event_bus = match self.bus.health().await {
            Ok(()) => DependencyStatus {
                ok: true,
                required: false,
                detail: None,
            },
            Err(e) => DependencyStatus {
                ok: false,
                required: false,
                detail: Some(e.to_string()),
            },
        };

        let chains = self
            .rpc
            .all()
            .map(|(chain, gw)| ChainStatus {
                chain: chain.clone(),
                rpc_available: gw.is_available(),
                endpoints: gw.status(),
            })
            .collect();

        Readiness {
            ready: database.ok,
            database,
            event_bus,
            chains,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn native_symbols_follow_the_chain_family() {
        assert_eq!(native_symbol_for("base-sepolia"), "ETH");
        assert_eq!(native_symbol_for("ethereum-sepolia"), "ETH");
        assert_eq!(native_symbol_for("bsc-testnet"), "BNB");
        assert_eq!(native_symbol_for("polygon-amoy"), "POL");
    }
}
