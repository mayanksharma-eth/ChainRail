//! Request middleware: request ids, correlation, auth, rate limiting, metrics.

use std::net::{IpAddr, SocketAddr};
use std::num::NonZeroU32;
use std::sync::Arc;

use axum::extract::{ConnectInfo, State};
use axum::http::{HeaderName, HeaderValue, Request, StatusCode};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use chainrail_common::Error;
use governor::clock::DefaultClock;
use governor::state::{InMemoryState, NotKeyed};
use governor::{Quota, RateLimiter};

use crate::error::ApiError;
use crate::state::AppState;

pub const REQUEST_ID_HEADER: &str = "x-request-id";
pub const CORRELATION_ID_HEADER: &str = "x-correlation-id";

/// Per-request identifiers, attached as an extension and echoed in responses.
#[derive(Debug, Clone)]
pub struct RequestContext {
    pub request_id: String,
    /// Ties this request to every event and log line it causes, across services.
    pub correlation_id: String,
    pub client_ip: Option<IpAddr>,
}

impl RequestContext {
    pub fn from_parts(headers: &axum::http::HeaderMap, client_ip: Option<IpAddr>) -> Self {
        // Accept a caller-supplied id so a client can correlate its own logs,
        // but bound and sanitise it: this value ends up in log output.
        let request_id = headers
            .get(REQUEST_ID_HEADER)
            .and_then(|v| v.to_str().ok())
            .map(sanitize_id)
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| uuid::Uuid::now_v7().to_string());
        let correlation_id = headers
            .get(CORRELATION_ID_HEADER)
            .and_then(|v| v.to_str().ok())
            .map(sanitize_id)
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| request_id.clone());
        RequestContext {
            request_id,
            correlation_id,
            client_ip,
        }
    }
}

/// Keep only characters that are safe in a log field, and bound the length.
/// An unsanitised header could otherwise inject newlines into log output.
fn sanitize_id(raw: &str) -> String {
    raw.chars()
        .filter(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.' | ':'))
        .take(64)
        .collect()
}

/// Attach a request context, open a span, and record metrics.
///
/// The peer address is read *optionally* from extensions rather than taken as a
/// required `ConnectInfo` extractor: it is absent when the service is driven
/// directly (tests, tower composition) and behind proxies that do not populate
/// it, and a missing client IP must never turn a request into a 500.
pub async fn context_layer(mut req: Request<axum::body::Body>, next: Next) -> Response {
    let peer = req
        .extensions()
        .get::<ConnectInfo<SocketAddr>>()
        .map(|ConnectInfo(addr)| addr.ip());
    let ctx = RequestContext::from_parts(req.headers(), peer);
    let method = req.method().to_string();
    // Use the matched route pattern, not the raw path: `/v1/deposits/:id` keeps
    // metric cardinality bounded, where the raw path would not.
    let route = req
        .extensions()
        .get::<axum::extract::MatchedPath>()
        .map(|p| p.as_str().to_string())
        .unwrap_or_else(|| "unmatched".to_string());

    let span = tracing::info_span!(
        "http_request",
        request_id = %ctx.request_id,
        correlation_id = %ctx.correlation_id,
        method = %method,
        route = %route,
    );

    let request_id = ctx.request_id.clone();
    req.extensions_mut().insert(ctx);

    let started = std::time::Instant::now();
    let mut response = {
        use tracing::Instrument;
        next.run(req).instrument(span).await
    };
    let elapsed = started.elapsed();
    let status = response.status();

    metrics::counter!(
        "chainrail_http_requests_total",
        "route" => route.clone(),
        "method" => method,
        "status" => status.as_u16().to_string(),
    )
    .increment(1);
    metrics::histogram!("chainrail_http_request_seconds", "route" => route)
        .record(elapsed.as_secs_f64());

    // Echo the id so a client can quote it, and so a proxy log can be joined.
    if let Ok(v) = HeaderValue::from_str(&request_id) {
        response
            .headers_mut()
            .insert(HeaderName::from_static(REQUEST_ID_HEADER), v);
    }
    response
}

/// Bearer-token authentication.
///
/// A deliberate placeholder: a real deployment needs per-user identity, scopes,
/// and key rotation, which belongs in an auth service rather than here. The
/// *shape* is what matters -- every `/v1` route passes through one checkpoint,
/// so replacing this with real authorization is a single-file change.
/// `AppConfig::validate` refuses to boot a production-like environment without a
/// token configured. See `docs/threat-model.md#authentication`.
pub async fn auth_layer(
    State(state): State<Arc<AppState>>,
    req: Request<axum::body::Body>,
    next: Next,
) -> Response {
    let Some(expected) = state.cfg.http.api_token.as_deref() else {
        return next.run(req).await; // open in local development
    };

    let presented = req
        .headers()
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "));

    let ok = presented
        .map(|t| constant_time_eq(t, expected))
        .unwrap_or(false);
    if !ok {
        let request_id = req
            .extensions()
            .get::<RequestContext>()
            .map(|c| c.request_id.clone())
            .unwrap_or_default();
        // No detail about *why*: distinguishing "no token" from "wrong token"
        // helps an attacker more than a client.
        return ApiError {
            status: StatusCode::UNAUTHORIZED,
            inner: Error::Validation("missing or invalid credentials".into()),
            request_id,
        }
        .into_response();
    }
    next.run(req).await
}

/// Length-independent comparison, so token verification does not leak the
/// correct prefix through timing.
fn constant_time_eq(a: &str, b: &str) -> bool {
    let (a, b) = (a.as_bytes(), b.as_bytes());
    if a.len() != b.len() {
        return false;
    }
    a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

/// Process-wide rate limiter.
///
/// A single global bucket, not per-IP: with no auth in local development, per-IP
/// state would be trivially exhausted, and a real deployment rate-limits at the
/// edge (API gateway) with per-key quotas. This is a crude last-resort backstop
/// against a runaway client, and is documented as such.
pub struct GlobalRateLimiter {
    inner: RateLimiter<NotKeyed, InMemoryState, DefaultClock>,
}

impl GlobalRateLimiter {
    pub fn new(rps: u32, burst: u32) -> Arc<Self> {
        let rps = NonZeroU32::new(rps.max(1)).expect("rps >= 1");
        let burst = NonZeroU32::new(burst.max(rps.get())).expect("burst >= rps");
        Arc::new(GlobalRateLimiter {
            inner: RateLimiter::direct(Quota::per_second(rps).allow_burst(burst)),
        })
    }

    pub fn check(&self) -> bool {
        self.inner.check().is_ok()
    }
}

pub async fn rate_limit_layer(
    State(limiter): State<Arc<GlobalRateLimiter>>,
    req: Request<axum::body::Body>,
    next: Next,
) -> Response {
    if !limiter.check() {
        let request_id = req
            .extensions()
            .get::<RequestContext>()
            .map(|c| c.request_id.clone())
            .unwrap_or_default();
        metrics::counter!("chainrail_http_rate_limited_total").increment(1);
        return ApiError {
            status: StatusCode::TOO_MANY_REQUESTS,
            inner: Error::RateLimited,
            request_id,
        }
        .into_response();
    }
    next.run(req).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::HeaderMap;

    #[test]
    fn generates_a_request_id_when_absent() {
        let ctx = RequestContext::from_parts(&HeaderMap::new(), None);
        assert!(!ctx.request_id.is_empty());
        assert_eq!(
            ctx.correlation_id, ctx.request_id,
            "correlation defaults to the request id"
        );
    }

    #[test]
    fn honours_client_supplied_ids() {
        let mut h = HeaderMap::new();
        h.insert(REQUEST_ID_HEADER, HeaderValue::from_static("req-abc-123"));
        h.insert(CORRELATION_ID_HEADER, HeaderValue::from_static("trace-xyz"));
        let ctx = RequestContext::from_parts(&h, None);
        assert_eq!(ctx.request_id, "req-abc-123");
        assert_eq!(ctx.correlation_id, "trace-xyz");
    }

    #[test]
    fn sanitizes_ids_against_log_injection() {
        // A newline in a log field would let a client forge log entries.
        assert_eq!(sanitize_id("abc\ndef INJECTED"), "abcdefINJECTED");
        assert_eq!(sanitize_id("a b\tc"), "abc");
        assert_eq!(sanitize_id("../../etc/passwd"), "....etcpasswd");
        assert_eq!(sanitize_id("ok-id_1.2:3"), "ok-id_1.2:3");
    }

    #[test]
    fn bounds_id_length() {
        assert_eq!(sanitize_id(&"a".repeat(500)).len(), 64);
    }

    #[test]
    fn empty_after_sanitising_falls_back_to_a_generated_id() {
        let mut h = HeaderMap::new();
        h.insert(REQUEST_ID_HEADER, HeaderValue::from_static("!!!!"));
        let ctx = RequestContext::from_parts(&h, None);
        // Not the sanitised-empty string.
        assert!(!ctx.request_id.is_empty());
        assert!(uuid::Uuid::parse_str(&ctx.request_id).is_ok());
    }

    #[test]
    fn token_comparison_rejects_mismatches_and_length_differences() {
        assert!(constant_time_eq("secret", "secret"));
        assert!(!constant_time_eq("secret", "secrez"));
        assert!(!constant_time_eq("secret", "secretx"));
        assert!(!constant_time_eq("", "x"));
        assert!(constant_time_eq("", ""));
    }

    #[test]
    fn rate_limiter_allows_a_burst_then_throttles() {
        let limiter = GlobalRateLimiter::new(1, 5);
        let mut allowed = 0;
        for _ in 0..20 {
            if limiter.check() {
                allowed += 1;
            }
        }
        // Burst of 5 is consumed; the rest are rejected within the same second.
        assert!((5..=6).contains(&allowed), "allowed {allowed}");
    }

    #[test]
    fn rate_limiter_normalises_degenerate_config() {
        // Zero would panic inside governor; validation also rejects it, but the
        // constructor must not be a landmine.
        let l = GlobalRateLimiter::new(0, 0);
        assert!(l.check());
    }
}
