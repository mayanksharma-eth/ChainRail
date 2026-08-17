//! Integration-test harness.
//!
//! Tests run against a real Postgres (no mocked database -- the invariants
//! under test are enforced *by* Postgres, so mocking it would test nothing).
//!
//! Isolation: tests share one database and serialise on a process-wide lock,
//! truncating between runs. That keeps the harness ~20 lines instead of the
//! ~100 a per-test-schema scheme needs, and the suite is fast enough that
//! serialisation costs nothing. If the suite grows past a few seconds, switch
//! to a schema per test.
//!
//! Set `TEST_DATABASE_URL` to point at a scratch database. When it is unset the
//! tests skip rather than fail, so `cargo test --all` works without Docker.

pub mod mock_chain;
pub use mock_chain::{mined, reverted, tx_hash, MockChain, MockTransfer};

use chainrail_common::config::DatabaseConfig;
use chainrail_common::{Address, Amount, ChainKind, Hash32};
use chainrail_database::models::AccountType;
use chainrail_database::{repo, Db};
use tokio::sync::{Mutex, MutexGuard};
use uuid::Uuid;

pub const TEST_CHAIN: &str = "base-sepolia";

static DB_LOCK: Mutex<()> = Mutex::const_new(());

pub struct TestDb {
    pub db: Db,
    _guard: MutexGuard<'static, ()>,
}

impl std::ops::Deref for TestDb {
    type Target = Db;
    fn deref(&self) -> &Db {
        &self.db
    }
}

pub fn test_database_url() -> Option<String> {
    let url = std::env::var("TEST_DATABASE_URL").ok()?;
    // The harness TRUNCATEs every table between tests. Running that against the
    // database a live worker is using wipes its cursor mid-run, which surfaces as
    // a baffling flake rather than the self-inflicted wound it is. Refuse loudly.
    let looks_like_a_test_db = url.rsplit('/').next().is_some_and(|db| {
        let db = db.split('?').next().unwrap_or(db);
        db.contains("test")
    });
    assert!(
        looks_like_a_test_db,
        "TEST_DATABASE_URL must name a dedicated test database (one containing \"test\"), \
         got `{url}`. The harness truncates every table; pointing it at the application \
         database would destroy a running worker's state. docker-compose creates \
         `chainrail_test` for this."
    );
    Some(url)
}

/// Acquire an exclusive, empty database. Returns `None` when no test database
/// is configured, so callers can skip.
pub async fn setup() -> Option<TestDb> {
    let url = test_database_url()?;
    let guard = DB_LOCK.lock().await;

    let cfg = DatabaseConfig {
        url,
        max_connections: 32,
        min_connections: 1,
        acquire_timeout_ms: 10_000,
        statement_timeout_ms: 30_000,
        run_migrations_on_boot: true,
    };
    let db = Db::connect(&cfg).await.expect("connect to test database");
    db.migrate().await.expect("run migrations");
    truncate_all(&db).await;
    Some(TestDb { db, _guard: guard })
}

/// Wipe every table. `RESTART IDENTITY CASCADE` in one statement so ordering
/// and foreign keys are Postgres's problem, not ours.
async fn truncate_all(db: &Db) {
    sqlx::query(
        "TRUNCATE users, assets, deposit_addresses, chain_blocks, chain_cursors,
                  blockchain_transactions, deposits, withdrawals, chain_nonces,
                  ledger_accounts, ledger_transactions, ledger_entries,
                  outbox, processed_events, dead_letters
         RESTART IDENTITY CASCADE",
    )
    .execute(db.pool())
    .await
    .expect("truncate");
}

/// Skip the test body when no database is configured.
#[macro_export]
macro_rules! require_db {
    () => {
        match $crate::setup().await {
            Some(db) => db,
            None => {
                eprintln!("skipping: TEST_DATABASE_URL not set");
                return;
            }
        }
    };
}

// ------------------------------------------------------------- fixtures ---

pub struct Fixture {
    pub user_id: Uuid,
    pub asset_id: Uuid,
    pub native_asset_id: Uuid,
}

/// A user, a USDC asset, and the chain's native asset. Enough for every money
/// path in the suite.
pub async fn fixture(db: &Db) -> Fixture {
    let user = repo::reference::create_user(db.pool(), &format!("user-{}", Uuid::new_v4()))
        .await
        .expect("create user");
    let asset = repo::reference::upsert_asset(
        db.pool(),
        TEST_CHAIN,
        "USDC",
        Some("0x036cbd53842c5426634e7929541ec2318f3dcf7e"),
        6,
    )
    .await
    .expect("create asset");
    let native = repo::reference::upsert_asset(db.pool(), TEST_CHAIN, "ETH", None, 18)
        .await
        .expect("create native asset");
    Fixture {
        user_id: user.id,
        asset_id: asset.id,
        native_asset_id: native.id,
    }
}

pub async fn create_user(db: &Db, label: &str) -> Uuid {
    repo::reference::create_user(db.pool(), label)
        .await
        .expect("create user")
        .id
}

/// Give a user a spendable balance by posting a deposit credit directly.
/// Uses the real ledger path, so the balance is backed by real entries.
pub async fn fund(db: &Db, user_id: Uuid, asset_id: Uuid, amount: i64) {
    let mut tx = db.begin().await.expect("begin");
    chainrail_ledger::credit_deposit(
        &mut tx,
        Uuid::new_v4(),
        user_id,
        asset_id,
        Amount::from(amount),
        None,
    )
    .await
    .expect("fund user");
    tx.commit().await.expect("commit");
}

/// Fund the exchange's custody account, as an operator would before enabling
/// withdrawals. Required before any path that draws custody down (broadcasts,
/// gas, reorg reversals).
pub async fn fund_custody(db: &Db, asset_id: Uuid, amount: i64) {
    let mut tx = db.begin().await.expect("begin");
    chainrail_ledger::fund_custody(
        &mut tx,
        asset_id,
        Amount::from(amount),
        &format!("test-{}", Uuid::new_v4()),
        None,
    )
    .await
    .expect("fund custody");
    tx.commit().await.expect("commit");
}

pub async fn available_balance(db: &Db, user_id: Uuid, asset_id: Uuid) -> Amount {
    chainrail_ledger::get_balance(db.pool(), user_id, asset_id)
        .await
        .expect("read balance")
}

pub async fn balance_of(db: &Db, kind: AccountType, user_id: Uuid, asset_id: Uuid) -> Amount {
    chainrail_ledger::user_balance(db.pool(), kind, user_id, asset_id)
        .await
        .expect("read balance")
}

pub async fn system_balance(db: &Db, kind: AccountType, asset_id: Uuid) -> Amount {
    sqlx::query_scalar::<_, Amount>(
        "SELECT COALESCE((SELECT balance FROM ledger_accounts
           WHERE account_type = $1 AND owner_user_id IS NULL AND asset_id = $2), 0)",
    )
    .bind(kind.as_str())
    .bind(asset_id)
    .fetch_one(db.pool())
    .await
    .expect("read system balance")
}

// --------------------------------------------------------- chain helpers ---

pub fn hash(seed: &str) -> Hash32 {
    let mut h = String::from("0x");
    let bytes = seed.as_bytes();
    for i in 0..32 {
        h.push_str(&format!(
            "{:02x}",
            bytes.get(i % bytes.len().max(1)).copied().unwrap_or(0)
        ));
    }
    Hash32::parse(&h).expect("build hash")
}

pub fn address(seed: u8) -> Address {
    let body: String = (0..20)
        .map(|i| format!("{:02x}", seed.wrapping_add(i)))
        .collect();
    Address::parse(ChainKind::Evm, &format!("0x{body}")).expect("build address")
}

/// Insert a canonical block, orphaning whatever currently holds that height.
/// Mirrors what the reorg engine does, for tests that need to stage a chain.
pub async fn insert_block(db: &Db, height: u64, hash_seed: &str, parent_seed: &str) -> Hash32 {
    let h = hash(hash_seed);
    let p = hash(parent_seed);
    let mut tx = db.begin().await.expect("begin");
    sqlx::query(
        "UPDATE chain_blocks SET status = 'orphaned', orphaned_at = now()
          WHERE chain = $1 AND height = $2 AND status = 'canonical' AND hash <> $3",
    )
    .bind(TEST_CHAIN)
    .bind(height as i64)
    .bind(&h)
    .execute(&mut *tx)
    .await
    .expect("orphan incumbent");
    repo::chain::insert_canonical_block(&mut *tx, TEST_CHAIN, &h, height, &p)
        .await
        .expect("insert block");
    tx.commit().await.expect("commit");
    h
}

// ------------------------------------------------------- config fixtures ---

/// A minimal but *complete* app config for the test chain. Built in code rather
/// than loaded from a file so a test can tweak one field without a fixture file.
pub fn test_chain_config() -> chainrail_common::config::ChainConfig {
    use chainrail_common::config::{AssetConfig, RpcEndpointConfig};
    use chainrail_common::{ChainId, ChainKind, FinalityPolicy};
    chainrail_common::config::ChainConfig {
        id: ChainId::new(TEST_CHAIN).unwrap(),
        kind: ChainKind::Evm,
        numeric_chain_id: Some(84532),
        finality: FinalityPolicy::Confirmations { blocks: 3 },
        poll_interval_ms: 10,
        block_batch_size: 10,
        // Must exceed the confirmation requirement; config validation enforces it.
        reorg_scan_depth: 32,
        start_block: Some(1),
        hot_wallet_address: Some("0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266".into()),
        transfer_gas_limit: 100_000,
        fee_bump_pct: 125,
        rpc: vec![RpcEndpointConfig {
            name: "mock".into(),
            url: "http://127.0.0.1:1".into(),
            weight: 100,
            timeout_ms: 100,
            max_inflight: 8,
            failure_threshold: 3,
            breaker_reset_ms: 1_000,
        }],
        assets: vec![
            AssetConfig {
                symbol: "USDC".into(),
                decimals: 6,
                contract_address: Some("0x036cbd53842c5426634e7929541ec2318f3dcf7e".into()),
                deposits_enabled: true,
                withdrawals_enabled: true,
            },
            AssetConfig {
                symbol: "ETH".into(),
                decimals: 18,
                contract_address: None,
                deposits_enabled: false,
                withdrawals_enabled: false,
            },
        ],
    }
}

pub fn test_app_config() -> chainrail_common::config::AppConfig {
    use chainrail_common::config::*;
    use std::collections::HashMap;
    let cfg = AppConfig {
        environment: "test".into(),
        service_name: "chainrail-test".into(),
        http: HttpConfig {
            bind: "127.0.0.1:0".into(),
            request_timeout_ms: 5_000,
            max_body_bytes: 65_536,
            rate_limit_rps: 100_000,
            rate_limit_burst: 100_000,
            default_page_size: 50,
            max_page_size: 200,
            api_token: None,
        },
        database: DatabaseConfig {
            url: test_database_url().unwrap_or_default(),
            max_connections: 8,
            min_connections: 1,
            acquire_timeout_ms: 5_000,
            statement_timeout_ms: 30_000,
            run_migrations_on_boot: false,
        },
        redis: RedisConfig {
            url: "redis://127.0.0.1:56379".into(),
            timeout_ms: 500,
            required: false,
        },
        kafka: KafkaConfig {
            brokers: String::new(), // in-memory bus
            consumer_group: "test".into(),
            topic_prefix: String::new(),
            request_timeout_ms: 1_000,
            max_delivery_attempts: 3,
            retry_backoff_base_ms: 1,
            retry_backoff_max_ms: 10,
            use_outbox: true,
            outbox_poll_interval_ms: 10,
            outbox_batch_size: 100,
        },
        observability: ObservabilityConfig {
            log_level: "warn".into(),
            log_format: "pretty".into(),
            otlp_endpoint: None,
            trace_sample_ratio: 0.0,
            metrics_bind: "127.0.0.1:0".into(),
        },
        signer: SignerConfig::LocalDevelopment {
            private_key: "ac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80".into(),
        },
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
        chains: vec![test_chain_config()],
        worker: WorkerConfig::default(),
    };
    cfg.validate().expect("test config must be valid");
    cfg
}

/// Register a deposit address for a user so the watcher will track it.
pub async fn register_deposit_address(db: &Db, user_id: Uuid, addr: &Address) {
    repo::reference::assign_deposit_address(db.pool(), user_id, TEST_CHAIN, addr, None)
        .await
        .expect("assign deposit address");
}

/// Drain the outbox through an in-memory bus and return what was published.
pub async fn drain_outbox(
    db: &Db,
    cfg: &chainrail_common::config::AppConfig,
) -> std::sync::Arc<chainrail_events::InMemoryEventBus> {
    let bus = chainrail_events::InMemoryEventBus::new();
    let relay = std::sync::Arc::new(chainrail_events::OutboxRelay::new(
        db.clone(),
        bus.clone(),
        &cfg.kafka,
    ));
    // Loop until a pass does no work, so the whole backlog is drained.
    for _ in 0..50 {
        let pass = relay.run_once().await.expect("relay pass");
        if !pass.did_work() {
            break;
        }
    }
    bus
}
