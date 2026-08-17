//! ChainRail background worker.
//!
//! Runs everything that is not a request: block watching, confirmation tracking,
//! deposit crediting, the withdrawal pipeline, and the outbox relay.
//!
//! Every component is independently toggleable via `worker.*` config, so a
//! single chain's watcher can be isolated onto its own replica, or a component
//! disabled during an incident without redeploying the API.
//!
//! Horizontal scaling: multiple worker replicas are safe. Queue claims use
//! `FOR UPDATE SKIP LOCKED`, Kafka consumer groups partition events, and every
//! side effect is idempotent -- so replicas partition work between themselves
//! without any coordination service.

use std::sync::Arc;
use std::time::Duration;

use chainrail_common::config::AppConfig;
use chainrail_deposits::{ChainContext, ConfirmationEngine, DepositCreditHandler, Watcher};
use chainrail_events::{ConsumerRunner, OutboxRelay};
use chainrail_withdrawals::WithdrawalPipeline;
use tokio_util::sync::CancellationToken;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cfg = Arc::new(AppConfig::load()?);
    let metrics = chainrail_observability::init(&cfg)?;

    tracing::info!(
        version = env!("CARGO_PKG_VERSION"),
        environment = %cfg.environment,
        chains = cfg.chains.len(),
        "chainrail-worker starting"
    );

    let state = Arc::new(chainrail_api::AppState::build(Arc::clone(&cfg), None).await?);
    let cancel = CancellationToken::new();
    let mut tasks = tokio::task::JoinSet::new();

    tasks.spawn({
        let bind = cfg.observability.metrics_bind.clone();
        async move {
            if let Err(e) = chainrail_observability::serve_metrics(metrics, &bind).await {
                tracing::error!(error = %e, "metrics server stopped");
            }
        }
    });

    // --- per-chain: watcher + confirmation engine ---
    for chain_cfg in &cfg.chains {
        let chain = chain_cfg.id.to_string();
        let adapter = state.adapter(&chain)?;
        // Refresh tracked addresses every 30s. A new deposit address becomes
        // monitored within that window; nothing is ever missed because block
        // ranges are re-scanned from the cursor rather than skipped.
        let ctx = ChainContext::new(state.db.clone(), chain_cfg, Duration::from_secs(30));
        ctx.refresh().await?;

        if cfg.worker.run_watcher {
            let watcher = Arc::new(Watcher::new(
                state.db.clone(),
                Arc::clone(&adapter),
                Arc::clone(&ctx),
                chain_cfg,
            ));
            let cancel = cancel.clone();
            tasks.spawn(async move { watcher.run(cancel).await });
        }

        if cfg.worker.run_confirmations {
            let engine = Arc::new(ConfirmationEngine::new(
                state.db.clone(),
                Arc::clone(&adapter),
                &chain,
                chain_cfg.finality.clone(),
                chain_cfg.poll_interval(),
            ));
            let cancel = cancel.clone();
            tasks.spawn(async move { engine.run(cancel).await });
        }
    }

    // --- outbox relay: database -> Kafka ---
    if cfg.worker.run_outbox_relay {
        let relay = Arc::new(OutboxRelay::new(
            state.db.clone(),
            Arc::clone(&state.bus),
            &cfg.kafka,
        ));
        let cancel = cancel.clone();
        tasks.spawn(async move { relay.run(cancel).await });
    }

    // --- deposit crediting: consumes deposit.confirmed ---
    if cfg.worker.run_deposit_consumer {
        let handler = DepositCreditHandler::new(state.db.clone());
        // Fail fast on a mis-wired handler rather than silently never running.
        chainrail_events::validate_handler(handler.as_ref())?;
        let runner = Arc::new(ConsumerRunner::new(
            state.db.clone(),
            handler,
            cfg.kafka.clone(),
        ));
        let cancel = cancel.clone();
        tasks.spawn(async move { runner.run(cancel).await });
    }

    // --- withdrawal pipeline ---
    if cfg.worker.run_withdrawal_pipeline {
        if !state.signer.is_production_grade() && cfg.is_production_like() {
            anyhow::bail!(
                "refusing to run the withdrawal pipeline in `{}` with the `{}` signer: \
                 ChainRail ships no production key custody backend",
                cfg.environment,
                state.signer.backend_name()
            );
        }
        let pipeline = WithdrawalPipeline::new(
            state.db.clone(),
            Arc::clone(&cfg),
            Arc::clone(&state.signer),
            (*state.adapters).clone(),
            Duration::from_millis(cfg.worker.tick_interval_ms),
        );
        let cancel = cancel.clone();
        tasks.spawn(async move { pipeline.run(cancel).await });
    }

    // --- housekeeping: bounded retention for event bookkeeping ---
    tasks.spawn({
        let db = state.db.clone();
        let cancel = cancel.clone();
        async move { housekeeping_loop(db, cancel).await }
    });

    tracing::info!(tasks = tasks.len(), "worker components started");

    wait_for_shutdown().await;
    tracing::info!("shutdown signal received; stopping components");
    cancel.cancel();

    let grace = Duration::from_millis(cfg.worker.shutdown_grace_ms);
    match tokio::time::timeout(grace, async { while tasks.join_next().await.is_some() {} }).await {
        Ok(()) => tracing::info!("all components stopped cleanly"),
        Err(_) => {
            // Aborting mid-transaction is safe: uncommitted work rolls back, and
            // every stage re-derives its state on restart.
            tracing::warn!(
                ?grace,
                "grace period elapsed; aborting remaining components"
            );
        }
    }
    tasks.abort_all();

    chainrail_observability::shutdown();
    Ok(())
}

/// Prune published outbox rows and old processed-event records.
///
/// Both tables are append-only in the hot path and would otherwise grow without
/// bound. The retention window must comfortably exceed the longest plausible
/// redelivery delay, or pruning would resurrect the duplicate-processing risk
/// the table exists to prevent.
async fn housekeeping_loop(db: chainrail_database::Db, cancel: CancellationToken) {
    const RETENTION_DAYS: i64 = 7;
    let interval = Duration::from_secs(3_600);
    loop {
        tokio::select! {
            _ = tokio::time::sleep(interval) => {}
            _ = cancel.cancelled() => break,
        }
        // Warn before the address pool runs dry: once empty, no new user can be
        // given a deposit address, and topping it up needs the custody system.
        match chainrail_database::repo::reference::pool_stats(db.pool()).await {
            Ok(stats) => {
                for (chain, available, assigned) in stats {
                    metrics::gauge!(
                        "chainrail_deposit_address_pool_available",
                        "chain" => chain.clone(),
                    )
                    .set(available as f64);
                    metrics::gauge!(
                        "chainrail_deposit_address_pool_assigned",
                        "chain" => chain,
                    )
                    .set(assigned as f64);
                }
            }
            Err(e) => tracing::warn!(error = %e, "could not read address pool stats"),
        }

        let cutoff = chrono::Utc::now() - chrono::Duration::days(RETENTION_DAYS);
        match chainrail_database::repo::outbox::prune_published(db.pool(), cutoff).await {
            Ok(n) if n > 0 => tracing::info!(rows = n, "pruned published outbox rows"),
            Ok(_) => {}
            Err(e) => tracing::warn!(error = %e, "outbox pruning failed"),
        }
        match chainrail_database::repo::outbox::prune_processed(db.pool(), cutoff).await {
            Ok(n) if n > 0 => tracing::info!(rows = n, "pruned processed-event records"),
            Ok(_) => {}
            Err(e) => tracing::warn!(error = %e, "processed-event pruning failed"),
        }
    }
}

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
