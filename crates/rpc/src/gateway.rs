//! JSON-RPC gateway: multi-endpoint failover with health-aware selection,
//! bounded concurrency, timeouts, retries and circuit breaking.
//!
//! The retry policy is the interesting part. Blind retries are unacceptable on
//! a money path, so every call declares its idempotency:
//!
//!   * `Idempotency::Safe` -- pure reads (`eth_getBlockByNumber`, `eth_call`).
//!     Retried freely across endpoints.
//!   * `Idempotency::UnsafeOnTimeout` -- `eth_sendRawTransaction`. Retried only
//!     when we know the request never reached a node (connect/DNS failure). A
//!     *timeout* is ambiguous: the transaction may be in the mempool, so we
//!     surface the ambiguity instead of resending and risking a double spend.
//!
//! Note that re-broadcasting an identical signed EVM transaction is harmless
//! (same nonce, same hash). The danger is a caller that reacts to the error by
//! re-*signing* with a new nonce, so the ambiguity must reach it.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use chainrail_common::config::{ChainConfig, RpcEndpointConfig};
use chainrail_common::retry::Backoff;
use chainrail_common::{Error, Result};
use parking_lot::Mutex;
use serde::de::DeserializeOwned;
use serde_json::{json, Value};
use tokio::sync::Semaphore;

use crate::health::{BreakerState, EndpointHealth, HealthConfig};

/// Whether a call may be retried after an ambiguous failure.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Idempotency {
    /// Pure read, or an operation whose repetition has no external effect.
    Safe,
    /// Has a side effect. Retry only when delivery definitively did not happen.
    UnsafeOnTimeout,
}

/// Why a single attempt failed. Drives whether failover is permitted.
#[derive(Debug, Clone, PartialEq, Eq)]
enum AttemptFailure {
    /// Never reached the server: connection refused, DNS failure, TLS error.
    /// Safe to retry even for non-idempotent calls.
    NotDelivered(String),
    /// Sent, but no usable answer: timeout, or a 5xx/decode error. Ambiguous.
    Ambiguous(String),
    /// The node answered with a JSON-RPC error. Retrying the same call on
    /// another endpoint is pointless for deterministic errors (e.g. "nonce too
    /// low") but useful for endpoint-specific ones. We do not retry: a node
    /// answering is a node working.
    Rejected { code: i64, message: String },
}

impl AttemptFailure {
    fn allows_retry(&self, idem: Idempotency) -> bool {
        match self {
            AttemptFailure::NotDelivered(_) => true,
            AttemptFailure::Ambiguous(_) => idem == Idempotency::Safe,
            AttemptFailure::Rejected { .. } => false,
        }
    }

    fn into_error(self, chain: &str) -> Error {
        match self {
            AttemptFailure::NotDelivered(m) => Error::Rpc(format!("{chain}: not delivered: {m}")),
            AttemptFailure::Ambiguous(m) => Error::Rpc(format!("{chain}: ambiguous outcome: {m}")),
            AttemptFailure::Rejected { code, message } => Error::Rpc(format!(
                "{chain}: node rejected request ({code}): {message}"
            )),
        }
    }

    fn label(&self) -> &'static str {
        match self {
            AttemptFailure::NotDelivered(_) => "not_delivered",
            AttemptFailure::Ambiguous(_) => "ambiguous",
            AttemptFailure::Rejected { .. } => "rejected",
        }
    }
}

struct Endpoint {
    name: String,
    url: String,
    weight: u32,
    timeout: Duration,
    health: Mutex<EndpointHealth>,
    /// Bounded concurrency: protects both us and the provider from a stampede,
    /// and stops one slow endpoint from consuming the whole task budget.
    permits: Arc<Semaphore>,
}

pub struct RpcGateway {
    chain: String,
    endpoints: Vec<Endpoint>,
    client: reqwest::Client,
    backoff: Backoff,
}

/// Snapshot for `/health` and for tests.
#[derive(Debug, Clone, serde::Serialize)]
pub struct EndpointStatus {
    pub name: String,
    pub breaker: String,
    pub consecutive_failures: u32,
    pub total_requests: u64,
    pub failure_rate: f64,
    pub ewma_latency_ms: Option<u64>,
    pub seconds_since_success: Option<u64>,
    pub available_permits: usize,
}

impl RpcGateway {
    pub fn new(chain: &str, endpoints: &[RpcEndpointConfig]) -> Result<Arc<RpcGateway>> {
        if endpoints.is_empty() {
            return Err(Error::Config(format!("chain {chain} has no rpc endpoints")));
        }
        let client = reqwest::Client::builder()
            // Pool connections; the handshake cost dominates for small JSON-RPC
            // payloads against a remote provider.
            .pool_max_idle_per_host(16)
            .pool_idle_timeout(Duration::from_secs(60))
            .tcp_nodelay(true)
            .build()
            .map_err(|e| Error::Rpc(format!("building http client: {e}")))?;

        let endpoints = endpoints
            .iter()
            .map(|c| Endpoint {
                name: c.name.clone(),
                url: c.url.clone(),
                weight: c.weight,
                timeout: Duration::from_millis(c.timeout_ms),
                health: Mutex::new(EndpointHealth::new(HealthConfig {
                    failure_threshold: c.failure_threshold,
                    reset_timeout: Duration::from_millis(c.breaker_reset_ms),
                    ..Default::default()
                })),
                permits: Arc::new(Semaphore::new(c.max_inflight)),
            })
            .collect();

        Ok(Arc::new(RpcGateway {
            chain: chain.to_string(),
            endpoints,
            client,
            backoff: Backoff::new(100, 2_000, 3),
        }))
    }

    pub fn from_chain_config(cfg: &ChainConfig) -> Result<Arc<RpcGateway>> {
        RpcGateway::new(cfg.id.as_str(), &cfg.rpc)
    }

    pub fn chain(&self) -> &str {
        &self.chain
    }

    /// Endpoints ordered best-first, skipping those with an open breaker.
    fn select_order(&self) -> Vec<usize> {
        let now = Instant::now();
        let mut scored: Vec<(usize, f64)> = self
            .endpoints
            .iter()
            .enumerate()
            .filter_map(|(i, e)| e.health.lock().score(e.weight, now).map(|s| (i, s)))
            .collect();
        scored.sort_by(|a, b| b.1.total_cmp(&a.1));
        scored.into_iter().map(|(i, _)| i).collect()
    }

    /// Issue a JSON-RPC call, failing over across endpoints as the idempotency
    /// declaration permits.
    pub async fn call<T: DeserializeOwned>(
        &self,
        method: &str,
        params: Value,
        idem: Idempotency,
    ) -> Result<T> {
        let order = self.select_order();
        if order.is_empty() {
            metrics::counter!(
                "chainrail_rpc_failures_total",
                "chain" => self.chain.clone(),
                "endpoint" => "none",
                "reason" => "all_unhealthy",
            )
            .increment(1);
            return Err(Error::NoHealthyRpcEndpoint {
                chain: self.chain.clone(),
            });
        }

        let mut last: Option<AttemptFailure> = None;

        for (attempt, &idx) in order.iter().enumerate() {
            let attempt = attempt as u32;
            // Back off between endpoints so a provider-wide incident does not
            // turn into a tight retry loop.
            if attempt > 0 {
                tokio::time::sleep(self.backoff.delay(attempt)).await;
            }

            match self.attempt(idx, method, &params).await {
                Ok(raw) => {
                    return serde_json::from_value(raw).map_err(|e| {
                        Error::Rpc(format!(
                            "{}: could not decode {method} response: {e}",
                            self.chain
                        ))
                    })
                }
                Err(failure) => {
                    let retryable = failure.allows_retry(idem);
                    tracing::warn!(
                        chain = %self.chain,
                        endpoint = %self.endpoints[idx].name,
                        method,
                        failure = failure.label(),
                        retryable,
                        "rpc attempt failed"
                    );
                    if !retryable {
                        return Err(failure.into_error(&self.chain));
                    }
                    last = Some(failure);
                }
            }
        }

        Err(last
            .map(|f| f.into_error(&self.chain))
            .unwrap_or(Error::NoHealthyRpcEndpoint {
                chain: self.chain.clone(),
            }))
    }

    /// Convenience wrapper for reads.
    pub async fn call_safe<T: DeserializeOwned>(&self, method: &str, params: Value) -> Result<T> {
        self.call(method, params, Idempotency::Safe).await
    }

    async fn attempt(
        &self,
        idx: usize,
        method: &str,
        params: &Value,
    ) -> std::result::Result<Value, AttemptFailure> {
        let ep = &self.endpoints[idx];
        let now = Instant::now();
        if !ep.health.lock().allows_request(now) {
            return Err(AttemptFailure::NotDelivered("circuit breaker open".into()));
        }

        // Acquiring a permit bounds in-flight requests per endpoint. If the
        // endpoint is saturated we treat it as undelivered and move on rather
        // than queueing behind a stalled provider.
        let permit = match Arc::clone(&ep.permits).try_acquire_owned() {
            Ok(p) => p,
            Err(_) => {
                metrics::counter!(
                    "chainrail_rpc_failures_total",
                    "chain" => self.chain.clone(),
                    "endpoint" => ep.name.clone(),
                    "reason" => "saturated",
                )
                .increment(1);
                return Err(AttemptFailure::NotDelivered(
                    "endpoint concurrency limit reached".into(),
                ));
            }
        };

        let body = json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": method,
            "params": params,
        });

        let started = Instant::now();
        let response = self
            .client
            .post(&ep.url)
            .timeout(ep.timeout)
            .json(&body)
            .send()
            .await;
        let elapsed = started.elapsed();
        drop(permit);

        metrics::counter!(
            "chainrail_rpc_requests_total",
            "chain" => self.chain.clone(),
            "endpoint" => ep.name.clone(),
            "method" => method.to_string(),
        )
        .increment(1);
        metrics::histogram!(
            "chainrail_rpc_latency_seconds",
            "chain" => self.chain.clone(),
            "endpoint" => ep.name.clone(),
            "method" => method.to_string(),
        )
        .record(elapsed.as_secs_f64());

        let outcome = self.interpret(response, elapsed).await;
        match &outcome {
            Ok(_) => ep.health.lock().record_success(elapsed, Instant::now()),
            Err(f) => {
                // A `Rejected` result means the node is healthy and answered;
                // counting it against endpoint health would trip breakers on
                // every "nonce too low".
                if !matches!(f, AttemptFailure::Rejected { .. }) {
                    ep.health.lock().record_failure(Instant::now());
                }
                metrics::counter!(
                    "chainrail_rpc_failures_total",
                    "chain" => self.chain.clone(),
                    "endpoint" => ep.name.clone(),
                    "reason" => f.label(),
                )
                .increment(1);
            }
        }
        outcome
    }

    async fn interpret(
        &self,
        response: std::result::Result<reqwest::Response, reqwest::Error>,
        elapsed: Duration,
    ) -> std::result::Result<Value, AttemptFailure> {
        let response = match response {
            Ok(r) => r,
            Err(e) if e.is_timeout() => {
                return Err(AttemptFailure::Ambiguous(format!(
                    "timeout after {}ms",
                    elapsed.as_millis()
                )))
            }
            Err(e) if e.is_connect() || e.is_request() => {
                return Err(AttemptFailure::NotDelivered(scrub(&e.to_string())))
            }
            Err(e) => return Err(AttemptFailure::Ambiguous(scrub(&e.to_string()))),
        };

        let status = response.status();
        if status.as_u16() == 429 {
            return Err(AttemptFailure::Ambiguous("rate limited by provider".into()));
        }
        if !status.is_success() {
            return Err(AttemptFailure::Ambiguous(format!("http {status}")));
        }

        let payload: Value = response.json().await.map_err(|e| {
            AttemptFailure::Ambiguous(format!("malformed json: {}", scrub(&e.to_string())))
        })?;

        // A malformed-but-parseable response is a real risk with third-party
        // providers, so the envelope is validated rather than assumed.
        if let Some(err) = payload.get("error") {
            let code = err.get("code").and_then(Value::as_i64).unwrap_or(-1);
            let message = err
                .get("message")
                .and_then(Value::as_str)
                .unwrap_or("unknown")
                .to_string();
            return Err(AttemptFailure::Rejected {
                code,
                message: chainrail_common::chain::truncate_for_log(&message),
            });
        }
        match payload.get("result") {
            Some(r) => Ok(r.clone()),
            None => Err(AttemptFailure::Ambiguous(
                "response has neither `result` nor `error`".into(),
            )),
        }
    }

    /// Probe every endpoint. Runs on a timer so breakers recover without
    /// waiting for user traffic to discover the outage is over.
    pub async fn health_check(&self) -> Vec<EndpointStatus> {
        let futures = self
            .endpoints
            .iter()
            .enumerate()
            .map(|(idx, _)| async move {
                let _ = self.attempt(idx, "eth_blockNumber", &json!([])).await;
            });
        futures::future::join_all(futures).await;
        self.status()
    }

    pub fn status(&self) -> Vec<EndpointStatus> {
        let now = Instant::now();
        self.endpoints
            .iter()
            .map(|e| {
                let h = e.health.lock();
                let status = EndpointStatus {
                    name: e.name.clone(),
                    breaker: h.state().label().to_string(),
                    consecutive_failures: h.consecutive_failures(),
                    total_requests: h.total_requests(),
                    failure_rate: h.failure_rate(),
                    ewma_latency_ms: h.ewma_latency().map(|d| d.as_millis() as u64),
                    seconds_since_success: h
                        .last_success()
                        .map(|t| now.saturating_duration_since(t).as_secs()),
                    available_permits: e.permits.available_permits(),
                };
                metrics::gauge!(
                    "chainrail_rpc_endpoint_healthy",
                    "chain" => self.chain.clone(),
                    "endpoint" => e.name.clone(),
                )
                .set(if h.state() == BreakerState::Closed {
                    1.0
                } else {
                    0.0
                });
                status
            })
            .collect()
    }

    /// True when at least one endpoint can serve a request.
    pub fn is_available(&self) -> bool {
        let now = Instant::now();
        self.endpoints
            .iter()
            .any(|e| e.health.lock().score(e.weight, now).is_some())
    }
}

/// Strip anything that could carry a credential out of an error string.
/// Provider URLs routinely embed API keys in the path.
fn scrub(msg: &str) -> String {
    let mut out = String::with_capacity(msg.len());
    for token in msg.split_whitespace() {
        if token.contains("://") {
            match token.split_once("://") {
                Some((scheme, rest)) => {
                    let host = rest.split('/').next().unwrap_or("");
                    out.push_str(&format!("{scheme}://{host}/<redacted>"));
                }
                None => out.push_str("<redacted>"),
            }
        } else {
            out.push_str(token);
        }
        out.push(' ');
    }
    chainrail_common::chain::truncate_for_log(out.trim())
}

/// One gateway per configured chain.
pub struct RpcRegistry {
    gateways: HashMap<String, Arc<RpcGateway>>,
}

impl RpcRegistry {
    pub fn build(chains: &[ChainConfig]) -> Result<Arc<RpcRegistry>> {
        let mut gateways = HashMap::new();
        for c in chains {
            gateways.insert(c.id.to_string(), RpcGateway::from_chain_config(c)?);
        }
        Ok(Arc::new(RpcRegistry { gateways }))
    }

    pub fn get(&self, chain: &str) -> Result<Arc<RpcGateway>> {
        self.gateways
            .get(chain)
            .cloned()
            .ok_or_else(|| Error::UnsupportedChain(chain.to_string()))
    }

    pub fn chains(&self) -> Vec<&str> {
        self.gateways.keys().map(String::as_str).collect()
    }

    pub fn all(&self) -> impl Iterator<Item = (&String, &Arc<RpcGateway>)> {
        self.gateways.iter()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retry_policy_respects_idempotency() {
        let not_delivered = AttemptFailure::NotDelivered("connection refused".into());
        let ambiguous = AttemptFailure::Ambiguous("timeout after 5000ms".into());
        let rejected = AttemptFailure::Rejected {
            code: -32000,
            message: "nonce too low".into(),
        };

        // A request that never left the process is always safe to retry.
        assert!(not_delivered.allows_retry(Idempotency::Safe));
        assert!(not_delivered.allows_retry(Idempotency::UnsafeOnTimeout));

        // A timeout is ambiguous: safe to retry for reads, NOT for a broadcast.
        assert!(ambiguous.allows_retry(Idempotency::Safe));
        assert!(
            !ambiguous.allows_retry(Idempotency::UnsafeOnTimeout),
            "a timed-out broadcast must not be blindly resent"
        );

        // The node answered; retrying elsewhere will not change a deterministic
        // rejection.
        assert!(!rejected.allows_retry(Idempotency::Safe));
        assert!(!rejected.allows_retry(Idempotency::UnsafeOnTimeout));
    }

    #[test]
    fn failures_carry_the_chain_and_reason_into_the_error() {
        let e = AttemptFailure::Ambiguous("timeout".into()).into_error("base-sepolia");
        assert!(e.to_string().contains("base-sepolia"));
        assert!(e.to_string().contains("ambiguous"));
        assert!(e.is_retryable());
    }

    #[test]
    fn urls_and_api_keys_are_scrubbed_from_errors() {
        let msg = "error sending request for url https://base-mainnet.example.com/v2/SECRETKEY123";
        let scrubbed = scrub(msg);
        assert!(!scrubbed.contains("SECRETKEY123"), "leaked key: {scrubbed}");
        assert!(scrubbed.contains("<redacted>"));
        assert!(
            scrubbed.contains("base-mainnet.example.com"),
            "host is useful, keep it"
        );
    }

    #[test]
    fn scrub_bounds_output_length() {
        let long = format!("failed {}", "x".repeat(5_000));
        assert!(scrub(&long).len() < 200);
    }

    fn endpoint_cfg(name: &str, weight: u32) -> RpcEndpointConfig {
        RpcEndpointConfig {
            name: name.into(),
            url: format!("http://127.0.0.1:1/{name}"),
            weight,
            timeout_ms: 50,
            max_inflight: 4,
            failure_threshold: 2,
            breaker_reset_ms: 10_000,
        }
    }

    #[test]
    fn gateway_requires_at_least_one_endpoint() {
        assert!(RpcGateway::new("base-sepolia", &[]).is_err());
    }

    #[test]
    fn selection_prefers_higher_weight_when_health_is_equal() {
        let gw = RpcGateway::new(
            "base-sepolia",
            &[endpoint_cfg("low", 10), endpoint_cfg("high", 500)],
        )
        .unwrap();
        let order = gw.select_order();
        assert_eq!(gw.endpoints[order[0]].name, "high");
        assert!(gw.is_available());
    }

    #[test]
    fn endpoints_with_open_breakers_drop_out_of_selection() {
        let gw = RpcGateway::new(
            "base-sepolia",
            &[endpoint_cfg("a", 100), endpoint_cfg("b", 100)],
        )
        .unwrap();
        let now = Instant::now();
        // Trip endpoint `a` (failure_threshold = 2).
        {
            let mut h = gw.endpoints[0].health.lock();
            h.record_failure(now);
            h.record_failure(now);
        }
        let order = gw.select_order();
        assert_eq!(order.len(), 1);
        assert_eq!(gw.endpoints[order[0]].name, "b");
        assert!(gw.is_available());

        // Trip `b` as well: nothing is selectable.
        {
            let mut h = gw.endpoints[1].health.lock();
            h.record_failure(now);
            h.record_failure(now);
        }
        assert!(gw.select_order().is_empty());
        assert!(!gw.is_available());
    }

    #[tokio::test]
    async fn all_endpoints_down_yields_a_distinct_error() {
        let gw = RpcGateway::new("base-sepolia", &[endpoint_cfg("a", 100)]).unwrap();
        let now = Instant::now();
        {
            let mut h = gw.endpoints[0].health.lock();
            h.record_failure(now);
            h.record_failure(now);
        }
        let err = gw
            .call::<Value>("eth_blockNumber", json!([]), Idempotency::Safe)
            .await
            .expect_err("must fail");
        assert!(
            matches!(err, Error::NoHealthyRpcEndpoint { .. }),
            "got {err:?}"
        );
    }

    #[tokio::test]
    async fn unreachable_endpoint_is_reported_and_marked_unhealthy() {
        // Port 1 refuses connections, so this exercises the NotDelivered path
        // without needing a mock server.
        let gw = RpcGateway::new("base-sepolia", &[endpoint_cfg("dead", 100)]).unwrap();
        let err = gw
            .call::<Value>("eth_blockNumber", json!([]), Idempotency::Safe)
            .await
            .expect_err("must fail");
        assert!(err.is_retryable());
        let status = gw.status();
        assert_eq!(status.len(), 1);
        assert!(status[0].consecutive_failures >= 1);
        assert_eq!(status[0].total_requests, 1);
    }

    #[tokio::test]
    async fn registry_resolves_only_configured_chains() {
        use chainrail_common::{ChainId, ChainKind, FinalityPolicy};
        let chains = vec![ChainConfig {
            id: ChainId::new("base-sepolia").unwrap(),
            kind: ChainKind::Evm,
            numeric_chain_id: Some(84532),
            finality: FinalityPolicy::Confirmations { blocks: 10 },
            poll_interval_ms: 2_000,
            block_batch_size: 50,
            reorg_scan_depth: 64,
            start_block: None,
            hot_wallet_address: None,
            transfer_gas_limit: 120_000,
            fee_bump_pct: 125,
            rpc: vec![endpoint_cfg("a", 100)],
            assets: vec![],
        }];
        let reg = RpcRegistry::build(&chains).unwrap();
        assert!(reg.get("base-sepolia").is_ok());
        assert!(matches!(
            reg.get("ethereum"),
            Err(Error::UnsupportedChain(_))
        ));
        assert_eq!(reg.chains(), vec!["base-sepolia"]);
    }
}
