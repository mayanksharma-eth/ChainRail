//! ChainRail load generator.
//!
//! Three scenarios, each measuring a different bottleneck:
//!
//!   ledger    -- ledger posting throughput straight against Postgres. Isolates
//!                the accounting hot path from HTTP and serialisation.
//!   api       -- HTTP read/write mix against a running server.
//!   contended -- N concurrent withdrawals against ONE account, which is the
//!                worst case for row-lock contention and the number that matters
//!                for a busy user.
//!
//! Every number printed is measured from the run that printed it. Latency
//! percentiles come from the full sample set (not a reservoir), because at these
//! sample sizes exactness is free and an approximated p99 is easy to misread.
//!
//! Usage:
//!   chainrail-load ledger    --operations 10000 --concurrency 64
//!   chainrail-load api       --url http://127.0.0.1:8088 --operations 5000 --concurrency 128
//!   chainrail-load contended --operations 500 --concurrency 100

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use chainrail_common::config::DatabaseConfig;
use chainrail_common::Amount;
use chainrail_database::{repo, Db};
use uuid::Uuid;

#[derive(Debug)]
struct Args {
    scenario: String,
    operations: usize,
    concurrency: usize,
    url: String,
    database_url: String,
}

fn parse_args() -> Args {
    let mut args = Args {
        scenario: "ledger".into(),
        operations: 10_000,
        concurrency: 64,
        url: std::env::var("CHAINRAIL_URL").unwrap_or_else(|_| "http://127.0.0.1:8088".into()),
        database_url: std::env::var("TEST_DATABASE_URL")
            .or_else(|_| std::env::var("CHAINRAIL__DATABASE__URL"))
            .unwrap_or_else(|_| "postgres://chainrail:chainrail@127.0.0.1:55432/chainrail".into()),
    };
    let raw: Vec<String> = std::env::args().skip(1).collect();
    let mut i = 0;
    while i < raw.len() {
        match raw[i].as_str() {
            "--operations" | "-n" => {
                args.operations = raw
                    .get(i + 1)
                    .and_then(|v| v.parse().ok())
                    .unwrap_or(args.operations);
                i += 2;
            }
            "--concurrency" | "-c" => {
                args.concurrency = raw
                    .get(i + 1)
                    .and_then(|v| v.parse().ok())
                    .unwrap_or(args.concurrency);
                i += 2;
            }
            "--url" => {
                if let Some(v) = raw.get(i + 1) {
                    args.url = v.clone();
                }
                i += 2;
            }
            "--database-url" => {
                if let Some(v) = raw.get(i + 1) {
                    args.database_url = v.clone();
                }
                i += 2;
            }
            other if !other.starts_with('-') => {
                args.scenario = other.to_string();
                i += 1;
            }
            _ => i += 1,
        }
    }
    args
}

/// Latency samples plus outcome counts.
struct Samples {
    latencies_us: Vec<u64>,
    ok: u64,
    errors: u64,
    /// Rejections that are a *correct* outcome (e.g. insufficient balance in the
    /// contended scenario) rather than a failure. Counting these as errors would
    /// make a working system look broken.
    expected_rejections: u64,
    wall: Duration,
}

impl Samples {
    fn percentile(&self, p: f64) -> Duration {
        if self.latencies_us.is_empty() {
            return Duration::ZERO;
        }
        // Nearest-rank on a sorted sample.
        let idx = ((p / 100.0) * (self.latencies_us.len() - 1) as f64).round() as usize;
        Duration::from_micros(self.latencies_us[idx.min(self.latencies_us.len() - 1)])
    }

    fn report(&self, scenario: &str, concurrency: usize) {
        let total = self.ok + self.errors + self.expected_rejections;
        let secs = self.wall.as_secs_f64().max(1e-9);
        println!("\n=== {scenario} ===");
        println!("concurrency          {concurrency}");
        println!("operations           {total}");
        println!("wall time            {:.3}s", secs);
        println!("throughput           {:.0} ops/s", total as f64 / secs);
        println!("successful           {}", self.ok);
        if self.expected_rejections > 0 {
            println!("expected rejections  {}", self.expected_rejections);
        }
        println!(
            "errors               {} ({:.3}%)",
            self.errors,
            if total == 0 {
                0.0
            } else {
                self.errors as f64 / total as f64 * 100.0
            }
        );
        println!(
            "latency p50          {:.2}ms",
            self.percentile(50.0).as_secs_f64() * 1000.0
        );
        println!(
            "latency p95          {:.2}ms",
            self.percentile(95.0).as_secs_f64() * 1000.0
        );
        println!(
            "latency p99          {:.2}ms",
            self.percentile(99.0).as_secs_f64() * 1000.0
        );
        println!(
            "latency max          {:.2}ms",
            self.percentile(100.0).as_secs_f64() * 1000.0
        );
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = parse_args();
    println!(
        "scenario={} operations={} concurrency={}",
        args.scenario, args.operations, args.concurrency
    );

    match args.scenario.as_str() {
        "ledger" => ledger_scenario(&args).await?,
        "api" => api_scenario(&args).await?,
        "contended" => contended_scenario(&args).await?,
        other => anyhow::bail!("unknown scenario `{other}` (expected ledger | api | contended)"),
    }
    Ok(())
}

async fn connect(args: &Args) -> anyhow::Result<Db> {
    let cfg = DatabaseConfig {
        url: args.database_url.clone(),
        // Pool at least as wide as the concurrency, or the measurement becomes a
        // measurement of pool queueing rather than of the ledger.
        max_connections: (args.concurrency as u32).clamp(8, 200),
        min_connections: 4,
        acquire_timeout_ms: 30_000,
        statement_timeout_ms: 60_000,
        run_migrations_on_boot: true,
    };
    let db = Db::connect(&cfg).await?;
    db.migrate().await?;
    Ok(db)
}

/// Deposit-credit postings, each in its own transaction, spread across many
/// users so lock contention is realistic rather than pathological.
async fn ledger_scenario(args: &Args) -> anyhow::Result<()> {
    let db = connect(args).await?;

    println!("preparing fixtures...");
    let asset = repo::reference::upsert_asset(
        db.pool(),
        "load-test",
        "USDC",
        Some("0x036cbd53842c5426634e7929541ec2318f3dcf7e"),
        6,
    )
    .await?;

    // One user per 100 operations, floor 16: enough spread to exercise several
    // account rows without becoming a pure single-row contention test.
    let user_count = (args.operations / 100).max(16);
    let mut users = Vec::with_capacity(user_count);
    for i in 0..user_count {
        users.push(
            repo::reference::create_user(db.pool(), &format!("load-user-{i}-{}", Uuid::new_v4()))
                .await?
                .id,
        );
    }
    println!("prepared {user_count} users; starting run");

    let latencies = Arc::new(tokio::sync::Mutex::new(Vec::with_capacity(args.operations)));
    let ok = Arc::new(AtomicU64::new(0));
    let errors = Arc::new(AtomicU64::new(0));
    let semaphore = Arc::new(tokio::sync::Semaphore::new(args.concurrency));

    let started = Instant::now();
    let mut handles = Vec::with_capacity(args.operations);
    for i in 0..args.operations {
        let db = db.clone();
        let user_id = users[i % users.len()];
        let asset_id = asset.id;
        let (latencies, ok, errors) = (latencies.clone(), ok.clone(), errors.clone());
        let permit = semaphore.clone().acquire_owned().await?;

        handles.push(tokio::spawn(async move {
            let _permit = permit;
            let op_started = Instant::now();
            let result = async {
                let mut tx = db.begin().await?;
                chainrail_ledger::credit_deposit(
                    &mut tx,
                    Uuid::new_v4(),
                    user_id,
                    asset_id,
                    Amount::new(1_000),
                    None,
                )
                .await?;
                tx.commit().await.map_err(chainrail_database::map_sqlx)?;
                Ok::<(), chainrail_common::Error>(())
            }
            .await;
            let elapsed = op_started.elapsed();
            latencies.lock().await.push(elapsed.as_micros() as u64);
            match result {
                Ok(()) => ok.fetch_add(1, Ordering::Relaxed),
                Err(e) => {
                    if errors.fetch_add(1, Ordering::Relaxed) < 5 {
                        eprintln!("error: {e}");
                    }
                    0
                }
            };
        }));
    }
    for h in handles {
        h.await?;
    }
    let wall = started.elapsed();

    let mut latencies_us = latencies.lock().await.clone();
    latencies_us.sort_unstable();
    let samples = Samples {
        latencies_us,
        ok: ok.load(Ordering::Relaxed),
        errors: errors.load(Ordering::Relaxed),
        expected_rejections: 0,
        wall,
    };
    samples.report("ledger: deposit credits", args.concurrency);

    // A load run that leaves the ledger inconsistent is a failed run, however
    // good the throughput looked.
    println!("\nverifying ledger integrity...");
    let report = chainrail_ledger::verify_ledger_integrity(db.pool()).await?;
    if report.is_clean() {
        println!(
            "ledger CLEAN: {} transactions, {} accounts, 0 violations",
            report.transactions_checked, report.accounts_checked
        );
    } else {
        eprintln!("LEDGER INTEGRITY VIOLATED: {report:#?}");
        std::process::exit(1);
    }
    Ok(())
}

/// The contention worst case: many simultaneous withdrawals against one balance.
/// Measures how the per-account row lock behaves under pressure, and proves the
/// non-negative invariant holds at load.
async fn contended_scenario(args: &Args) -> anyhow::Result<()> {
    let db = connect(args).await?;

    let asset = repo::reference::upsert_asset(
        db.pool(),
        "load-test",
        "USDC",
        Some("0x036cbd53842c5426634e7929541ec2318f3dcf7e"),
        6,
    )
    .await?;
    let user = repo::reference::create_user(db.pool(), &format!("contended-{}", Uuid::new_v4()))
        .await?
        .id;

    // Fund exactly half the requested operations, so precisely half must succeed
    // and half must be refused. Any other split is a bug.
    let per_op = 1_000i128;
    let affordable = args.operations / 2;
    {
        let mut tx = db.begin().await?;
        chainrail_ledger::credit_deposit(
            &mut tx,
            Uuid::new_v4(),
            user,
            asset.id,
            Amount::new(per_op * affordable as i128),
            None,
        )
        .await?;
        tx.commit().await.map_err(chainrail_database::map_sqlx)?;
    }
    println!(
        "funded {} operations' worth; issuing {}",
        affordable, args.operations
    );

    let latencies = Arc::new(tokio::sync::Mutex::new(Vec::with_capacity(args.operations)));
    let ok = Arc::new(AtomicU64::new(0));
    let rejected = Arc::new(AtomicU64::new(0));
    let errors = Arc::new(AtomicU64::new(0));
    let semaphore = Arc::new(tokio::sync::Semaphore::new(args.concurrency));

    let started = Instant::now();
    let mut handles = Vec::with_capacity(args.operations);
    for _ in 0..args.operations {
        let db = db.clone();
        let asset_id = asset.id;
        let (latencies, ok, rejected, errors) = (
            latencies.clone(),
            ok.clone(),
            rejected.clone(),
            errors.clone(),
        );
        let permit = semaphore.clone().acquire_owned().await?;

        handles.push(tokio::spawn(async move {
            let _permit = permit;
            let op_started = Instant::now();
            let mut tx = match db.begin().await {
                Ok(t) => t,
                Err(_) => {
                    errors.fetch_add(1, Ordering::Relaxed);
                    return;
                }
            };
            let result = chainrail_ledger::reserve_withdrawal(
                &mut tx,
                Uuid::new_v4(),
                user,
                asset_id,
                Amount::new(per_op),
                None,
            )
            .await;
            match result {
                Ok(_) => {
                    if tx.commit().await.is_ok() {
                        ok.fetch_add(1, Ordering::Relaxed);
                    } else {
                        errors.fetch_add(1, Ordering::Relaxed);
                    }
                }
                Err(chainrail_common::Error::InsufficientBalance { .. }) => {
                    let _ = tx.rollback().await;
                    rejected.fetch_add(1, Ordering::Relaxed);
                }
                Err(e) => {
                    let _ = tx.rollback().await;
                    if errors.fetch_add(1, Ordering::Relaxed) < 5 {
                        eprintln!("unexpected error: {e}");
                    }
                }
            }
            latencies
                .lock()
                .await
                .push(op_started.elapsed().as_micros() as u64);
        }));
    }
    for h in handles {
        h.await?;
    }
    let wall = started.elapsed();

    let mut latencies_us = latencies.lock().await.clone();
    latencies_us.sort_unstable();
    let samples = Samples {
        latencies_us,
        ok: ok.load(Ordering::Relaxed),
        errors: errors.load(Ordering::Relaxed),
        expected_rejections: rejected.load(Ordering::Relaxed),
        wall,
    };
    samples.report(
        "contended: withdrawals against one balance",
        args.concurrency,
    );

    let succeeded = ok.load(Ordering::Relaxed) as usize;
    let final_balance = chainrail_ledger::get_balance(db.pool(), user, asset.id).await?;
    println!("\nsucceeded            {succeeded} (expected exactly {affordable})");
    println!("final available      {final_balance}");

    let mut failures = Vec::new();
    if succeeded != affordable {
        failures.push(format!("expected {affordable} successes, got {succeeded}"));
    }
    if !final_balance.is_zero() {
        failures.push(format!(
            "balance should be exactly drained, is {final_balance}"
        ));
    }
    if final_balance.is_negative() {
        failures.push("NEGATIVE BALANCE".to_string());
    }
    if errors.load(Ordering::Relaxed) > 0 {
        failures.push(format!(
            "{} unexpected errors",
            errors.load(Ordering::Relaxed)
        ));
    }
    let report = chainrail_ledger::verify_ledger_integrity(db.pool()).await?;
    if !report.is_clean() {
        failures.push(format!("ledger integrity violated: {report:#?}"));
    }

    if failures.is_empty() {
        println!("\nCORRECTNESS OK: exactly the affordable number succeeded, no negative balance, ledger clean");
    } else {
        eprintln!("\nCORRECTNESS FAILURES:");
        for f in failures {
            eprintln!("  - {f}");
        }
        std::process::exit(1);
    }
    Ok(())
}

/// HTTP load: a read-heavy mix, which is what a balance-checking client does.
async fn api_scenario(args: &Args) -> anyhow::Result<()> {
    let client = reqwest::Client::builder()
        .pool_max_idle_per_host(args.concurrency)
        .timeout(Duration::from_secs(30))
        .build()?;

    // Create a user through the API, so this scenario needs no database access.
    let user: serde_json::Value = client
        .post(format!("{}/v1/users", args.url))
        .json(&serde_json::json!({ "external_id": format!("load-{}", Uuid::new_v4()) }))
        .send()
        .await?
        .json()
        .await?;
    let user_id = user["id"]
        .as_str()
        .ok_or_else(|| anyhow::anyhow!("could not create a user: {user}"))?
        .to_string();
    println!("using user {user_id}");

    let latencies = Arc::new(tokio::sync::Mutex::new(Vec::with_capacity(args.operations)));
    let ok = Arc::new(AtomicU64::new(0));
    let errors = Arc::new(AtomicU64::new(0));
    let semaphore = Arc::new(tokio::sync::Semaphore::new(args.concurrency));

    let started = Instant::now();
    let mut handles = Vec::with_capacity(args.operations);
    for i in 0..args.operations {
        let client = client.clone();
        let url = args.url.clone();
        let user_id = user_id.clone();
        let (latencies, ok, errors) = (latencies.clone(), ok.clone(), errors.clone());
        let permit = semaphore.clone().acquire_owned().await?;

        handles.push(tokio::spawn(async move {
            let _permit = permit;
            // 3:1 read mix across the two most common read paths.
            let path = match i % 4 {
                0 => format!("{url}/v1/ledger/{user_id}?limit=20"),
                _ => format!("{url}/v1/balances/{user_id}"),
            };
            let op_started = Instant::now();
            let result = client.get(&path).send().await;
            latencies
                .lock()
                .await
                .push(op_started.elapsed().as_micros() as u64);
            match result {
                Ok(r) if r.status().is_success() => {
                    ok.fetch_add(1, Ordering::Relaxed);
                }
                Ok(r) => {
                    if errors.fetch_add(1, Ordering::Relaxed) < 5 {
                        eprintln!("http {} for {path}", r.status());
                    }
                }
                Err(e) => {
                    if errors.fetch_add(1, Ordering::Relaxed) < 5 {
                        eprintln!("request failed: {e}");
                    }
                }
            }
        }));
    }
    for h in handles {
        h.await?;
    }
    let wall = started.elapsed();

    let mut latencies_us = latencies.lock().await.clone();
    latencies_us.sort_unstable();
    Samples {
        latencies_us,
        ok: ok.load(Ordering::Relaxed),
        errors: errors.load(Ordering::Relaxed),
        expected_rejections: 0,
        wall,
    }
    .report("api: balance and ledger reads", args.concurrency);
    Ok(())
}
