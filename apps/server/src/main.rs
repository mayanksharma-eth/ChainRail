//! ChainRail API server.
//!
//! Serves the HTTP API and the metrics endpoint. Runs no background workers --
//! those live in `chainrail-worker` so that API capacity and chain-processing
//! capacity can be scaled and deployed independently.

use std::sync::Arc;

use chainrail_common::config::AppConfig;
use tokio_util::sync::CancellationToken;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // `--healthcheck` performs a one-shot probe against a running instance and
    // exits with a status code. Used by the container healthcheck so the image
    // does not need curl installed.
    if std::env::args().any(|a| a == "--healthcheck") {
        return healthcheck().await;
    }

    let cfg = Arc::new(AppConfig::load()?);
    let metrics = chainrail_observability::init(&cfg)?;

    tracing::info!(
        version = env!("CARGO_PKG_VERSION"),
        environment = %cfg.environment,
        chains = cfg.chains.len(),
        "chainrail-server starting"
    );

    let state =
        Arc::new(chainrail_api::AppState::build(Arc::clone(&cfg), Some(metrics.clone())).await?);

    // Verify chain identity up front. A misconfigured RPC (testnet config
    // pointing at mainnet) is worth failing the boot over, but a *transient*
    // provider outage is not -- so this warns rather than aborts.
    for (chain, adapter) in state.adapters.iter() {
        match adapter.verify_identity().await {
            Ok(()) => tracing::info!(chain, "chain identity verified"),
            Err(e) => tracing::warn!(chain, error = %e, "could not verify chain identity at boot"),
        }
    }

    let cancel = CancellationToken::new();
    let mut tasks = tokio::task::JoinSet::new();

    tasks.spawn({
        let state = Arc::clone(&state);
        let cancel = cancel.clone();
        async move {
            if let Err(e) = chainrail_api::serve(state, cancel).await {
                tracing::error!(error = %e, "http server stopped with an error");
            }
        }
    });

    // Metrics on a separate port so scraping is not exposed on the public
    // listener and does not share its rate limit or timeout.
    tasks.spawn({
        let bind = cfg.observability.metrics_bind.clone();
        let metrics = metrics.clone();
        async move {
            if let Err(e) = chainrail_observability::serve_metrics(metrics, &bind).await {
                tracing::error!(error = %e, "metrics server stopped");
            }
        }
    });

    // Periodic ledger verification. Cheap insurance against a class of bug that
    // is otherwise invisible until an auditor finds it.
    tasks.spawn({
        let state = Arc::clone(&state);
        let cancel = cancel.clone();
        async move { ledger_verification_loop(state, cancel).await }
    });

    wait_for_shutdown().await;
    tracing::info!("shutdown signal received; draining");
    cancel.cancel();

    let grace = std::time::Duration::from_millis(cfg.worker.shutdown_grace_ms);
    match tokio::time::timeout(grace, async { while tasks.join_next().await.is_some() {} }).await {
        Ok(()) => tracing::info!("all tasks stopped cleanly"),
        Err(_) => tracing::warn!(
            ?grace,
            "shutdown grace period elapsed; aborting remaining tasks"
        ),
    }
    tasks.abort_all();

    chainrail_observability::shutdown();
    Ok(())
}

/// Verify ledger invariants on a timer and expose the result as a gauge.
async fn ledger_verification_loop(state: Arc<chainrail_api::AppState>, cancel: CancellationToken) {
    let interval = std::time::Duration::from_secs(300);
    loop {
        tokio::select! {
            _ = tokio::time::sleep(interval) => {}
            _ = cancel.cancelled() => break,
        }
        match chainrail_ledger::verify_ledger_integrity(state.db.pool()).await {
            Ok(report) if report.is_clean() => {
                tracing::debug!(
                    transactions = report.transactions_checked,
                    accounts = report.accounts_checked,
                    "ledger integrity verified"
                );
            }
            Ok(report) => {
                // Already logged at ERROR inside the verifier; repeat the summary
                // so the count is visible at the call site too.
                tracing::error!(
                    problems = report.problem_count(),
                    "periodic ledger verification found violations"
                );
            }
            Err(e) => tracing::warn!(error = %e, "ledger verification pass failed"),
        }
    }
}

/// Probe a locally running instance. Exits non-zero when not ready.
async fn healthcheck() -> anyhow::Result<()> {
    let port = std::env::var("CHAINRAIL__HTTP__BIND")
        .ok()
        .and_then(|b| b.rsplit(':').next().map(String::from))
        .unwrap_or_else(|| "8080".to_string());
    let url = format!("http://127.0.0.1:{port}/ready");

    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(3))
        .build()?;
    match client.get(&url).send().await {
        Ok(r) if r.status().is_success() => Ok(()),
        Ok(r) => {
            eprintln!("healthcheck: {url} returned {}", r.status());
            std::process::exit(1);
        }
        Err(e) => {
            eprintln!("healthcheck: {url} unreachable: {e}");
            std::process::exit(1);
        }
    }
}

/// Wait for SIGTERM or SIGINT. SIGTERM is what a container runtime sends, so
/// handling only Ctrl-C would mean every deploy killed in-flight requests.
async fn wait_for_shutdown() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};
        let mut term = match signal(SignalKind::terminate()) {
            Ok(s) => s,
            Err(e) => {
                tracing::error!(error = %e, "cannot install SIGTERM handler");
                let _ = tokio::signal::ctrl_c().await;
                return;
            }
        };
        tokio::select! {
            _ = tokio::signal::ctrl_c() => tracing::info!("SIGINT received"),
            _ = term.recv() => tracing::info!("SIGTERM received"),
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}
