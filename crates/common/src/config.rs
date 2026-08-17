//! Configuration: layered file + environment, validated at boot.
//!
//! Layering (later wins):
//!   1. `config/default.toml`      -- committed, non-secret defaults
//!   2. `config/{APP_ENV}.toml`    -- committed, per-environment overrides
//!   3. `CHAINRAIL__*` env vars    -- secrets and deployment specifics
//!
//! Secrets (database URL, signer material) are only ever read from the
//! environment. `validate()` runs before any listener binds so a
//! misconfigured process fails fast and loudly instead of half-working.

use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::time::Duration;

use crate::chain::{ChainId, ChainKind, FinalityPolicy};
use crate::error::{Error, Result};
use crate::money::Amount;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppConfig {
    #[serde(default = "default_environment")]
    pub environment: String,
    #[serde(default = "default_service_name")]
    pub service_name: String,
    pub http: HttpConfig,
    pub database: DatabaseConfig,
    pub redis: RedisConfig,
    pub kafka: KafkaConfig,
    pub observability: ObservabilityConfig,
    pub signer: SignerConfig,
    pub risk: RiskConfig,
    #[serde(default)]
    pub chains: Vec<ChainConfig>,
    #[serde(default)]
    pub worker: WorkerConfig,
}

fn default_environment() -> String {
    "local".into()
}
fn default_service_name() -> String {
    "chainrail".into()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HttpConfig {
    #[serde(default = "default_bind")]
    pub bind: String,
    #[serde(default = "ms_30_000")]
    pub request_timeout_ms: u64,
    /// Hard cap on request body size; prevents memory-exhaustion DoS.
    #[serde(default = "default_max_body_bytes")]
    pub max_body_bytes: usize,
    #[serde(default = "default_rate_limit_rps")]
    pub rate_limit_rps: u32,
    #[serde(default = "default_rate_limit_burst")]
    pub rate_limit_burst: u32,
    #[serde(default = "default_page_size")]
    pub default_page_size: u32,
    #[serde(default = "max_page_size")]
    pub max_page_size: u32,
    /// When set, `Authorization: Bearer <token>` is required on `/v1/*`.
    /// Absent in local dev; see `docs/threat-model.md` on why this is a
    /// placeholder for a real authN/authZ service.
    #[serde(default)]
    pub api_token: Option<String>,
}

fn default_bind() -> String {
    "0.0.0.0:8080".into()
}
fn ms_30_000() -> u64 {
    30_000
}
fn default_max_body_bytes() -> usize {
    64 * 1024
}
fn default_rate_limit_rps() -> u32 {
    100
}
fn default_rate_limit_burst() -> u32 {
    200
}
fn default_page_size() -> u32 {
    50
}
fn max_page_size() -> u32 {
    200
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatabaseConfig {
    pub url: String,
    #[serde(default = "default_db_max_conns")]
    pub max_connections: u32,
    #[serde(default = "default_db_min_conns")]
    pub min_connections: u32,
    #[serde(default = "ms_10_000")]
    pub acquire_timeout_ms: u64,
    #[serde(default = "ms_30_000")]
    pub statement_timeout_ms: u64,
    #[serde(default = "default_true")]
    pub run_migrations_on_boot: bool,
}

fn default_db_max_conns() -> u32 {
    20
}
fn default_db_min_conns() -> u32 {
    2
}
fn ms_10_000() -> u64 {
    10_000
}
fn default_true() -> bool {
    true
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RedisConfig {
    pub url: String,
    #[serde(default = "ms_2_000")]
    pub timeout_ms: u64,
    /// Redis is a *availability* dependency, not a correctness one: when it is
    /// down, locks degrade to database-level locking and caches miss.
    #[serde(default = "default_true")]
    pub required: bool,
}

fn ms_2_000() -> u64 {
    2_000
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KafkaConfig {
    pub brokers: String,
    #[serde(default = "default_kafka_group")]
    pub consumer_group: String,
    #[serde(default = "default_topic_prefix")]
    pub topic_prefix: String,
    #[serde(default = "ms_5_000")]
    pub request_timeout_ms: u64,
    #[serde(default = "default_max_delivery_attempts")]
    pub max_delivery_attempts: u32,
    #[serde(default = "ms_500")]
    pub retry_backoff_base_ms: u64,
    #[serde(default = "ms_30_000")]
    pub retry_backoff_max_ms: u64,
    /// Publish through the transactional outbox rather than directly.
    #[serde(default = "default_true")]
    pub use_outbox: bool,
    #[serde(default = "ms_1_000")]
    pub outbox_poll_interval_ms: u64,
    #[serde(default = "default_outbox_batch")]
    pub outbox_batch_size: i64,
}

fn default_kafka_group() -> String {
    "chainrail".into()
}
fn default_topic_prefix() -> String {
    "".into()
}
fn ms_5_000() -> u64 {
    5_000
}
fn ms_500() -> u64 {
    500
}
fn ms_1_000() -> u64 {
    1_000
}
fn default_max_delivery_attempts() -> u32 {
    5
}
fn default_outbox_batch() -> i64 {
    200
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObservabilityConfig {
    #[serde(default = "default_log_level")]
    pub log_level: String,
    /// `json` in every non-local environment; `pretty` is dev-only.
    #[serde(default = "default_log_format")]
    pub log_format: String,
    #[serde(default)]
    pub otlp_endpoint: Option<String>,
    #[serde(default = "default_trace_sample_ratio")]
    pub trace_sample_ratio: f64,
    #[serde(default = "default_metrics_bind")]
    pub metrics_bind: String,
}

fn default_log_level() -> String {
    "info".into()
}
fn default_log_format() -> String {
    "json".into()
}
fn default_trace_sample_ratio() -> f64 {
    0.1
}
fn default_metrics_bind() -> String {
    "0.0.0.0:9090".into()
}

/// Which signer backend to use.
///
/// ChainRail ships *development* signers only. Production key custody is
/// deliberately out of scope: see `docs/threat-model.md#key-custody`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "backend", rename_all = "snake_case")]
pub enum SignerConfig {
    /// Deterministic in-memory key. Refuses to start outside `local`/`test`.
    LocalDevelopment {
        /// Hex private key, supplied via `CHAINRAIL__SIGNER__PRIVATE_KEY`.
        /// Never commit this; `.env.example` ships a well-known throwaway.
        private_key: String,
    },
    /// Signs nothing real; returns deterministic fake signatures for tests.
    Mock,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RiskConfig {
    /// Global kill switch: when true, all withdrawals are denied.
    #[serde(default)]
    pub maintenance_mode: bool,
    /// Withdrawals above this require manual approval instead of auto-approve.
    #[serde(default)]
    pub manual_approval_threshold: HashMap<String, Amount>,
    #[serde(default)]
    pub max_per_request: HashMap<String, Amount>,
    #[serde(default)]
    pub min_per_request: HashMap<String, Amount>,
    #[serde(default)]
    pub max_per_user_per_day: HashMap<String, Amount>,
    #[serde(default)]
    pub max_withdrawals_per_user_per_day: Option<u32>,
    /// Chains withdrawals may target. Empty means "every configured chain".
    #[serde(default)]
    pub allowed_chains: Vec<String>,
    /// Destination addresses that must never receive funds (case-insensitive).
    #[serde(default)]
    pub destination_denylist: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct WorkerConfig {
    #[serde(default = "default_true")]
    pub run_watcher: bool,
    #[serde(default = "default_true")]
    pub run_confirmations: bool,
    #[serde(default = "default_true")]
    pub run_deposit_consumer: bool,
    #[serde(default = "default_true")]
    pub run_withdrawal_pipeline: bool,
    #[serde(default = "default_true")]
    pub run_outbox_relay: bool,
    #[serde(default = "ms_2_000")]
    pub tick_interval_ms: u64,
    #[serde(default = "ms_30_000")]
    pub shutdown_grace_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChainConfig {
    pub id: ChainId,
    pub kind: ChainKind,
    /// EIP-155 numeric chain id; required for EVM replay protection.
    #[serde(default)]
    pub numeric_chain_id: Option<u64>,
    pub finality: FinalityPolicy,
    #[serde(default = "ms_2_000")]
    pub poll_interval_ms: u64,
    /// Max blocks scanned per watcher iteration.
    #[serde(default = "default_block_batch")]
    pub block_batch_size: u64,
    /// How far back the reorg engine keeps block metadata and is willing to
    /// rewind. Must exceed the confirmation requirement.
    #[serde(default = "default_reorg_depth")]
    pub reorg_scan_depth: u64,
    /// Block to begin indexing from on a cold start. `None` = current head.
    #[serde(default)]
    pub start_block: Option<u64>,
    /// Hot wallet that funds withdrawals on this chain.
    #[serde(default)]
    pub hot_wallet_address: Option<String>,
    #[serde(default = "default_gas_limit")]
    pub transfer_gas_limit: u64,
    /// Multiplier (percent) applied to suggested fees, e.g. 120 = +20%.
    #[serde(default = "default_fee_bump_pct")]
    pub fee_bump_pct: u64,
    pub rpc: Vec<RpcEndpointConfig>,
    #[serde(default)]
    pub assets: Vec<AssetConfig>,
}

fn default_block_batch() -> u64 {
    50
}
fn default_reorg_depth() -> u64 {
    64
}
fn default_gas_limit() -> u64 {
    120_000
}
fn default_fee_bump_pct() -> u64 {
    125
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RpcEndpointConfig {
    pub name: String,
    pub url: String,
    /// Static preference; higher wins when health is equal.
    #[serde(default = "default_weight")]
    pub weight: u32,
    #[serde(default = "ms_5_000")]
    pub timeout_ms: u64,
    #[serde(default = "default_max_inflight")]
    pub max_inflight: usize,
    /// Consecutive failures that trip the circuit breaker open.
    #[serde(default = "default_failure_threshold")]
    pub failure_threshold: u32,
    /// How long the breaker stays open before a probe request is allowed.
    #[serde(default = "ms_10_000")]
    pub breaker_reset_ms: u64,
}

fn default_weight() -> u32 {
    100
}
fn default_max_inflight() -> usize {
    32
}
fn default_failure_threshold() -> u32 {
    5
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AssetConfig {
    pub symbol: String,
    pub decimals: u8,
    /// `None` means the chain's native asset (ETH, BNB, ...).
    #[serde(default)]
    pub contract_address: Option<String>,
    #[serde(default = "default_true")]
    pub deposits_enabled: bool,
    #[serde(default = "default_true")]
    pub withdrawals_enabled: bool,
}

impl AssetConfig {
    pub fn is_native(&self) -> bool {
        self.contract_address.is_none()
    }
}

impl ChainConfig {
    pub fn asset(&self, symbol: &str) -> Option<&AssetConfig> {
        self.assets.iter().find(|a| a.symbol == symbol)
    }

    pub fn poll_interval(&self) -> Duration {
        Duration::from_millis(self.poll_interval_ms)
    }
}

impl AppConfig {
    /// Load configuration from files + environment and validate it.
    pub fn load() -> Result<AppConfig> {
        // Best-effort: a missing .env is normal in containers.
        let _ = dotenvy::dotenv();
        let env = std::env::var("APP_ENV").unwrap_or_else(|_| default_environment());
        let base = std::env::var("CHAINRAIL_CONFIG_DIR").unwrap_or_else(|_| "config".into());

        let cfg = ::config::Config::builder()
            .add_source(::config::File::with_name(&format!("{base}/default")).required(false))
            .add_source(::config::File::with_name(&format!("{base}/{env}")).required(false))
            .add_source(
                ::config::Environment::with_prefix("CHAINRAIL")
                    .separator("__")
                    .try_parsing(true),
            )
            .set_override("environment", env.clone())
            .map_err(|e| Error::Config(e.to_string()))?
            .build()
            .map_err(|e| Error::Config(e.to_string()))?;

        let app: AppConfig = cfg
            .try_deserialize()
            .map_err(|e| Error::Config(e.to_string()))?;
        app.validate()?;
        Ok(app)
    }

    pub fn is_production_like(&self) -> bool {
        !matches!(self.environment.as_str(), "local" | "test" | "ci")
    }

    pub fn chain(&self, id: &str) -> Option<&ChainConfig> {
        self.chains.iter().find(|c| c.id.as_str() == id)
    }

    /// Fail fast on any configuration that would produce silent misbehaviour.
    pub fn validate(&self) -> Result<()> {
        let bad = |m: String| Err(Error::Config(m));

        if self.http.max_page_size < self.http.default_page_size {
            return bad("http.max_page_size < http.default_page_size".into());
        }
        if self.http.max_body_bytes == 0 {
            return bad("http.max_body_bytes must be > 0".into());
        }
        if self.http.rate_limit_rps == 0 {
            return bad("http.rate_limit_rps must be > 0".into());
        }
        if self.database.url.is_empty() {
            return bad("database.url is required".into());
        }
        if self.database.max_connections < self.database.min_connections {
            return bad("database.max_connections < min_connections".into());
        }
        if !(0.0..=1.0).contains(&self.observability.trace_sample_ratio) {
            return bad("observability.trace_sample_ratio must be within [0,1]".into());
        }
        if !matches!(self.observability.log_format.as_str(), "json" | "pretty") {
            return bad("observability.log_format must be `json` or `pretty`".into());
        }

        // --- production guard rails ---
        if self.is_production_like() {
            if matches!(
                self.signer,
                SignerConfig::LocalDevelopment { .. } | SignerConfig::Mock
            ) {
                return bad(format!(
                    "environment `{}` refuses to start with a development signer; \
                     ChainRail ships no production key custody backend",
                    self.environment
                ));
            }
            if self.http.api_token.is_none() {
                return bad("http.api_token is required outside local/test".into());
            }
            if self.observability.log_format != "json" {
                return bad("structured JSON logging is required outside local/test".into());
            }
        }
        if let SignerConfig::LocalDevelopment { private_key } = &self.signer {
            let k = private_key.trim().trim_start_matches("0x");
            if k.len() != 64 || !k.chars().all(|c| c.is_ascii_hexdigit()) {
                return bad("signer.private_key must be 32 bytes of hex".into());
            }
        }

        // --- chains ---
        if self.chains.is_empty() {
            return bad("at least one chain must be configured".into());
        }
        let mut seen_chains = HashSet::new();
        for c in &self.chains {
            if !seen_chains.insert(c.id.clone()) {
                return bad(format!("duplicate chain id `{}`", c.id));
            }
            if c.rpc.is_empty() {
                return bad(format!("chain `{}` has no rpc endpoints", c.id));
            }
            if c.kind == ChainKind::Evm && c.numeric_chain_id.is_none() {
                return bad(format!(
                    "chain `{}` is EVM and must set numeric_chain_id (EIP-155 replay protection)",
                    c.id
                ));
            }
            if c.poll_interval_ms == 0 || c.block_batch_size == 0 {
                return bad(format!(
                    "chain `{}` has a zero poll interval or batch",
                    c.id
                ));
            }
            if let Some(req) = c.finality.required_confirmations() {
                if c.reorg_scan_depth <= req {
                    return bad(format!(
                        "chain `{}`: reorg_scan_depth ({}) must exceed required confirmations ({req}); \
                         otherwise a reorg deeper than the credit threshold cannot be detected",
                        c.id, c.reorg_scan_depth
                    ));
                }
            }
            let mut seen_ep = HashSet::new();
            for ep in &c.rpc {
                if !seen_ep.insert(ep.name.clone()) {
                    return bad(format!(
                        "chain `{}` has duplicate rpc name `{}`",
                        c.id, ep.name
                    ));
                }
                if !ep.url.starts_with("http://") && !ep.url.starts_with("https://") {
                    return bad(format!(
                        "chain `{}` rpc `{}` must be http(s)",
                        c.id, ep.name
                    ));
                }
                if self.is_production_like() && ep.url.starts_with("http://") {
                    return bad(format!(
                        "chain `{}` rpc `{}` uses plaintext http outside local/test",
                        c.id, ep.name
                    ));
                }
                if ep.timeout_ms == 0 || ep.max_inflight == 0 || ep.failure_threshold == 0 {
                    return bad(format!(
                        "chain `{}` rpc `{}` has a zero limit",
                        c.id, ep.name
                    ));
                }
            }
            let mut seen_asset = HashSet::new();
            for a in &c.assets {
                if !seen_asset.insert(a.symbol.clone()) {
                    return bad(format!(
                        "chain `{}` has duplicate asset `{}`",
                        c.id, a.symbol
                    ));
                }
                if a.decimals > 36 {
                    return bad(format!("asset `{}` has implausible decimals", a.symbol));
                }
                if let Some(addr) = &a.contract_address {
                    crate::chain::Address::parse(c.kind, addr).map_err(|e| {
                        Error::Config(format!("asset `{}` contract address: {e}", a.symbol))
                    })?;
                }
                if a.withdrawals_enabled && c.hot_wallet_address.is_none() {
                    return bad(format!(
                        "chain `{}` enables withdrawals for `{}` but has no hot_wallet_address",
                        c.id, a.symbol
                    ));
                }
            }
            if let Some(hw) = &c.hot_wallet_address {
                crate::chain::Address::parse(c.kind, hw)
                    .map_err(|e| Error::Config(format!("chain `{}` hot wallet: {e}", c.id)))?;
            }
            if c.kind == ChainKind::Solana {
                return bad(format!(
                    "chain `{}`: Solana is not implemented in v0.1; remove it from config",
                    c.id
                ));
            }
        }

        // --- risk ---
        for chain in &self.risk.allowed_chains {
            if self.chain(chain).is_none() {
                return bad(format!(
                    "risk.allowed_chains references unknown chain `{chain}`"
                ));
            }
        }
        for (key, min) in &self.risk.min_per_request {
            if let Some(max) = self.risk.max_per_request.get(key) {
                if min > max {
                    return bad(format!(
                        "risk: min_per_request > max_per_request for `{key}`"
                    ));
                }
            }
            if !min.is_positive() {
                return bad(format!(
                    "risk: min_per_request for `{key}` must be positive"
                ));
            }
        }
        for d in &self.risk.destination_denylist {
            if d.trim().is_empty() {
                return bad("risk.destination_denylist contains an empty entry".into());
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn base() -> AppConfig {
        AppConfig {
            environment: "local".into(),
            service_name: "chainrail".into(),
            http: HttpConfig {
                bind: default_bind(),
                request_timeout_ms: 30_000,
                max_body_bytes: 65_536,
                rate_limit_rps: 100,
                rate_limit_burst: 200,
                default_page_size: 50,
                max_page_size: 200,
                api_token: None,
            },
            database: DatabaseConfig {
                url: "postgres://localhost/chainrail".into(),
                max_connections: 10,
                min_connections: 1,
                acquire_timeout_ms: 5_000,
                statement_timeout_ms: 30_000,
                run_migrations_on_boot: true,
            },
            redis: RedisConfig {
                url: "redis://localhost".into(),
                timeout_ms: 2_000,
                required: true,
            },
            kafka: KafkaConfig {
                brokers: "localhost:9092".into(),
                consumer_group: "chainrail".into(),
                topic_prefix: String::new(),
                request_timeout_ms: 5_000,
                max_delivery_attempts: 5,
                retry_backoff_base_ms: 500,
                retry_backoff_max_ms: 30_000,
                use_outbox: true,
                outbox_poll_interval_ms: 1_000,
                outbox_batch_size: 200,
            },
            observability: ObservabilityConfig {
                log_level: "info".into(),
                log_format: "json".into(),
                otlp_endpoint: None,
                trace_sample_ratio: 0.1,
                metrics_bind: default_metrics_bind(),
            },
            signer: SignerConfig::Mock,
            risk: RiskConfig {
                maintenance_mode: false,
                manual_approval_threshold: HashMap::new(),
                max_per_request: HashMap::new(),
                min_per_request: HashMap::new(),
                max_per_user_per_day: HashMap::new(),
                max_withdrawals_per_user_per_day: None,
                allowed_chains: vec![],
                destination_denylist: vec![],
            },
            chains: vec![ChainConfig {
                id: ChainId::new("base-sepolia").unwrap(),
                kind: ChainKind::Evm,
                numeric_chain_id: Some(84532),
                finality: FinalityPolicy::Confirmations { blocks: 10 },
                poll_interval_ms: 2_000,
                block_batch_size: 50,
                reorg_scan_depth: 64,
                start_block: None,
                hot_wallet_address: Some("0x5aAeb6053F3E94C9b9A09f33669435E7Ef1BeAed".into()),
                transfer_gas_limit: 120_000,
                fee_bump_pct: 125,
                rpc: vec![RpcEndpointConfig {
                    name: "primary".into(),
                    url: "https://sepolia.base.org".into(),
                    weight: 100,
                    timeout_ms: 5_000,
                    max_inflight: 32,
                    failure_threshold: 5,
                    breaker_reset_ms: 10_000,
                }],
                assets: vec![AssetConfig {
                    symbol: "USDC".into(),
                    decimals: 6,
                    contract_address: Some("0x036CbD53842c5426634e7929541eC2318f3dCF7e".into()),
                    deposits_enabled: true,
                    withdrawals_enabled: true,
                }],
            }],
            worker: WorkerConfig::default(),
        }
    }

    #[test]
    fn baseline_config_is_valid() {
        base().validate().unwrap();
    }

    #[test]
    fn reorg_depth_must_exceed_confirmations() {
        let mut c = base();
        c.chains[0].reorg_scan_depth = 10; // == required confirmations
        assert!(c.validate().is_err());
    }

    #[test]
    fn evm_chain_requires_numeric_chain_id() {
        let mut c = base();
        c.chains[0].numeric_chain_id = None;
        assert!(c.validate().is_err());
    }

    #[test]
    fn production_refuses_development_signer() {
        let mut c = base();
        c.environment = "production".into();
        c.http.api_token = Some("t".into());
        assert!(c.validate().is_err());
        c.signer = SignerConfig::LocalDevelopment {
            private_key: "11".repeat(32),
        };
        assert!(c.validate().is_err());
    }

    #[test]
    fn production_requires_api_token_and_tls_rpc() {
        let mut c = base();
        c.environment = "staging".into();
        assert!(c.validate().is_err(), "missing api token accepted");
        c.http.api_token = Some("token".into());
        c.chains[0].rpc[0].url = "http://insecure".into();
        assert!(c.validate().is_err(), "plaintext rpc accepted");
    }

    #[test]
    fn withdrawal_enabled_asset_requires_hot_wallet() {
        let mut c = base();
        c.chains[0].hot_wallet_address = None;
        assert!(c.validate().is_err());
    }

    #[test]
    fn duplicate_chain_and_rpc_names_rejected() {
        let mut c = base();
        let dup = c.chains[0].clone();
        c.chains.push(dup);
        assert!(c.validate().is_err());

        let mut c = base();
        let ep = c.chains[0].rpc[0].clone();
        c.chains[0].rpc.push(ep);
        assert!(c.validate().is_err());
    }

    #[test]
    fn malformed_contract_address_rejected() {
        let mut c = base();
        c.chains[0].assets[0].contract_address = Some("0xnot-an-address".into());
        assert!(c.validate().is_err());
    }

    #[test]
    fn risk_bounds_are_sane() {
        let mut c = base();
        c.risk
            .min_per_request
            .insert("base-sepolia:USDC".into(), Amount::new(100));
        c.risk
            .max_per_request
            .insert("base-sepolia:USDC".into(), Amount::new(10));
        assert!(c.validate().is_err());

        let mut c = base();
        c.risk.allowed_chains = vec!["nope".into()];
        assert!(c.validate().is_err());
    }

    #[test]
    fn solana_is_rejected_until_implemented() {
        let mut c = base();
        c.chains[0].kind = ChainKind::Solana;
        assert!(c.validate().is_err());
    }

    #[test]
    fn dev_signer_key_must_be_32_bytes() {
        let mut c = base();
        c.signer = SignerConfig::LocalDevelopment {
            private_key: "0xdead".into(),
        };
        assert!(c.validate().is_err());
        c.signer = SignerConfig::LocalDevelopment {
            private_key: format!("0x{}", "ab".repeat(32)),
        };
        c.validate().unwrap();
    }
}
