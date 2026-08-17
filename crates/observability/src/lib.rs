//! Logging, metrics and tracing setup.
//!
//! Logging is structured JSON outside local development, because the fields that
//! matter during an incident (`request_id`, `correlation_id`, `chain`,
//! `tx_hash`, `user_id`) are only useful if they are queryable rather than
//! embedded in prose.
//!
//! Nothing here ever logs secrets. Private keys are wrapped in redacting types
//! at their source (see `chainrail_signer`), SQL parameters are logged only at
//! TRACE, and provider URLs are scrubbed in the RPC layer.

use chainrail_common::config::{AppConfig, ObservabilityConfig};
use chainrail_common::{Error, Result};
use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};
use std::net::SocketAddr;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::EnvFilter;

/// Handle to the Prometheus registry, so the API can serve `/metrics` from the
/// same process that records into it.
#[derive(Clone)]
pub struct Metrics {
    handle: PrometheusHandle,
}

impl Metrics {
    /// Current metrics in Prometheus text exposition format.
    pub fn render(&self) -> String {
        self.handle.render()
    }
}

/// Initialise logging, metrics and (optionally) OTLP tracing.
///
/// Returns the metrics handle. Safe to call once per process; a second call
/// fails rather than silently installing a second subscriber.
pub fn init(cfg: &AppConfig) -> Result<Metrics> {
    let obs = &cfg.observability;
    let filter = EnvFilter::try_from_default_env()
        .or_else(|_| EnvFilter::try_new(&obs.log_level))
        .map_err(|e| Error::Config(format!("invalid log filter: {e}")))?;

    let metrics = init_metrics()?;

    // Common fields on every event, so logs from different services join up.
    let service = cfg.service_name.clone();
    let environment = cfg.environment.clone();

    match obs.log_format.as_str() {
        "json" => {
            let layer = tracing_subscriber::fmt::layer()
                .json()
                .flatten_event(true)
                .with_current_span(true)
                .with_span_list(false)
                .with_target(true)
                .with_file(false)
                .with_line_number(false);
            let registry = tracing_subscriber::registry().with(filter).with(layer);
            install(registry, obs, &service)?;
        }
        _ => {
            let layer = tracing_subscriber::fmt::layer()
                .with_target(true)
                .with_ansi(true);
            let registry = tracing_subscriber::registry().with(filter).with(layer);
            install(registry, obs, &service)?;
        }
    }

    tracing::info!(
        service = %service,
        environment = %environment,
        log_format = %obs.log_format,
        otlp = obs.otlp_endpoint.is_some(),
        "observability initialised"
    );
    Ok(metrics)
}

fn install<S>(registry: S, obs: &ObservabilityConfig, service: &str) -> Result<()>
where
    S: SubscriberExt + Send + Sync + 'static + Into<tracing::Dispatch>,
    S: tracing::Subscriber,
    for<'a> S: tracing_subscriber::registry::LookupSpan<'a>,
{
    match &obs.otlp_endpoint {
        Some(endpoint) => {
            let tracer = build_tracer(endpoint, service, obs.trace_sample_ratio)?;
            registry
                .with(tracing_opentelemetry::layer().with_tracer(tracer))
                .try_init()
                .map_err(|e| Error::Config(format!("tracing already initialised: {e}")))
        }
        None => registry
            .try_init()
            .map_err(|e| Error::Config(format!("tracing already initialised: {e}"))),
    }
}

fn build_tracer(
    endpoint: &str,
    service: &str,
    sample_ratio: f64,
) -> Result<opentelemetry_sdk::trace::Tracer> {
    use opentelemetry::trace::TracerProvider as _;
    use opentelemetry_otlp::WithExportConfig;

    let exporter = opentelemetry_otlp::SpanExporter::builder()
        .with_tonic()
        .with_endpoint(endpoint)
        .build()
        .map_err(|e| Error::Config(format!("otlp exporter: {e}")))?;

    let provider = opentelemetry_sdk::trace::SdkTracerProvider::builder()
        .with_batch_exporter(exporter)
        // Head-based sampling: tracing every request on a money path is
        // affordable at low volume but not at scale, so the ratio is configurable.
        .with_sampler(opentelemetry_sdk::trace::Sampler::TraceIdRatioBased(
            sample_ratio,
        ))
        .with_resource(
            opentelemetry_sdk::Resource::builder()
                .with_service_name(service.to_string())
                .build(),
        )
        .build();

    let tracer = provider.tracer("chainrail");
    opentelemetry::global::set_tracer_provider(provider.clone());
    // Retained so `shutdown()` can flush buffered spans on a clean exit.
    let _ = TRACER_PROVIDER.set(provider);
    Ok(tracer)
}

fn init_metrics() -> Result<Metrics> {
    let builder = PrometheusBuilder::new()
        // Explicit buckets: the defaults are summaries, which cannot be
        // aggregated across replicas. Latency SLOs need histograms.
        .set_buckets(&[
            0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0,
        ])
        .map_err(|e| Error::Config(format!("prometheus buckets: {e}")))?;

    let handle = builder
        .install_recorder()
        .map_err(|e| Error::Config(format!("installing prometheus recorder: {e}")))?;

    describe_metrics();
    Ok(Metrics { handle })
}

/// Register metric descriptions up front so `/metrics` is self-documenting even
/// before a counter is first incremented.
fn describe_metrics() {
    use metrics::{describe_counter, describe_gauge, describe_histogram, Unit};

    describe_counter!(
        "chainrail_blocks_processed_total",
        "Blocks scanned and persisted by the watcher"
    );
    describe_counter!(
        "chainrail_deposits_observed_total",
        "Deposits detected on chain"
    );
    describe_counter!(
        "chainrail_deposits_credited_total",
        "Deposits credited to a user's spendable balance"
    );
    describe_counter!("chainrail_reorgs_total", "Chain reorganisations handled");
    describe_histogram!(
        "chainrail_reorg_depth_blocks",
        "Depth in blocks of handled reorganisations"
    );
    describe_counter!(
        "chainrail_reorg_credited_reversals_total",
        "Already-credited deposits reversed by a reorg (each one is an incident)"
    );
    describe_counter!(
        "chainrail_rpc_requests_total",
        "JSON-RPC requests attempted"
    );
    describe_counter!("chainrail_rpc_failures_total", "JSON-RPC request failures");
    describe_histogram!(
        "chainrail_rpc_latency_seconds",
        Unit::Seconds,
        "JSON-RPC request latency"
    );
    describe_gauge!(
        "chainrail_rpc_endpoint_healthy",
        "1 when an RPC endpoint's circuit breaker is closed"
    );
    describe_gauge!(
        "chainrail_kafka_consumer_lag",
        "Messages behind the partition high watermark"
    );
    describe_counter!(
        "chainrail_withdrawals_total",
        "Withdrawal requests by outcome"
    );
    describe_counter!(
        "chainrail_withdrawal_failures_total",
        "Withdrawals that reached a failed state"
    );
    describe_counter!(
        "chainrail_withdrawal_recoveries_total",
        "Withdrawals reconciled against the chain after an ambiguous broadcast"
    );
    describe_gauge!("chainrail_outbox_pending", "Unpublished outbox rows");
    describe_gauge!(
        "chainrail_outbox_oldest_pending_seconds",
        Unit::Seconds,
        "Age of the oldest unpublished outbox row -- the best signal that the relay is wedged"
    );
    describe_counter!(
        "chainrail_events_dead_lettered_total",
        "Events that exhausted their retry budget"
    );
    describe_counter!(
        "chainrail_events_duplicates_total",
        "Redelivered events skipped by the idempotency check"
    );
    describe_gauge!(
        "chainrail_ledger_integrity_problems",
        "Ledger integrity violations found by the last verification pass; must be 0"
    );
    describe_counter!(
        "chainrail_ledger_deficits_total",
        "Reorg reversals that had to book a receivable against a user"
    );
    describe_gauge!(
        "chainrail_watcher_lag_blocks",
        "Blocks behind the chain head"
    );
    describe_gauge!(
        "chainrail_chain_head_height",
        "Chain head height as last observed"
    );
    describe_histogram!(
        "chainrail_http_request_seconds",
        Unit::Seconds,
        "HTTP request latency"
    );
    describe_counter!(
        "chainrail_http_requests_total",
        "HTTP requests by route and status"
    );
}

/// Serve `/metrics` on a dedicated port, separate from the public API.
///
/// Keeping metrics off the public listener means the scrape endpoint is not
/// exposed to the internet and does not compete with user traffic for the
/// request-timeout and rate-limit middleware.
pub async fn serve_metrics(metrics: Metrics, bind: &str) -> Result<()> {
    let addr: SocketAddr = bind
        .parse()
        .map_err(|e| Error::Config(format!("invalid metrics bind address `{bind}`: {e}")))?;
    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .map_err(|e| Error::Config(format!("binding metrics listener: {e}")))?;
    tracing::info!(%addr, "metrics listener started");

    loop {
        let (mut socket, _) = match listener.accept().await {
            Ok(s) => s,
            Err(e) => {
                tracing::warn!(error = %e, "metrics accept failed");
                continue;
            }
        };
        let body = metrics.render();
        tokio::spawn(async move {
            use tokio::io::AsyncWriteExt;
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: text/plain; version=0.0.4\r\n\
                 Content-Length: {}\r\nConnection: close\r\n\r\n{}",
                body.len(),
                body
            );
            let _ = socket.write_all(response.as_bytes()).await;
            let _ = socket.shutdown().await;
        });
    }
}

/// Flush pending spans on shutdown. Without this, the last few traces before a
/// deploy -- often the interesting ones -- are lost.
///
/// The provider is stored here at init so shutdown can flush it; the global
/// registry in opentelemetry 0.32 no longer exposes a shutdown hook.
pub fn shutdown() {
    if let Some(provider) = TRACER_PROVIDER.get() {
        if let Err(e) = provider.shutdown() {
            tracing::warn!(error = %e, "tracer shutdown reported an error");
        }
    }
}

static TRACER_PROVIDER: std::sync::OnceLock<opentelemetry_sdk::trace::SdkTracerProvider> =
    std::sync::OnceLock::new();
