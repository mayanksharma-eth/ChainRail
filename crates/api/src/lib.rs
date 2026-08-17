//! HTTP API.
//!
//! Versioned under `/v1`. Operational endpoints (`/health`, `/ready`,
//! `/metrics`) sit outside the version prefix and outside authentication, since
//! a load balancer must be able to probe them.
//!
//! Middleware order (outermost first) is deliberate:
//!   1. body limit      -- reject oversized payloads before allocating
//!   2. timeout         -- bound every request
//!   3. request context -- ids and spans, so everything below is traceable
//!   4. rate limit      -- cheap rejection, but still logged with a request id
//!   5. auth           -- only on `/v1`
//!
//! Putting the body limit outermost means a multi-gigabyte upload is refused
//! without being read; putting the context above auth means rejected requests
//! still produce a correlatable log line.

pub mod dto;
pub mod error;
pub mod middleware;
pub mod openapi;
pub mod routes;
pub mod state;

use std::sync::Arc;
use std::time::Duration;

use axum::routing::{get, post};
use axum::Router;
use tower_http::limit::RequestBodyLimitLayer;
use tower_http::timeout::TimeoutLayer;

pub use state::AppState;

/// Build the router.
pub fn router(state: Arc<AppState>) -> Router {
    let limiter = middleware::GlobalRateLimiter::new(
        state.cfg.http.rate_limit_rps,
        state.cfg.http.rate_limit_burst,
    );

    let v1 = Router::new()
        .route("/users", post(routes::create_user))
        .route("/users/{user_id}", get(routes::get_user))
        .route("/deposit-addresses", post(routes::create_deposit_address))
        .route(
            "/deposit-addresses/{user_id}/{chain}",
            get(routes::get_deposit_address),
        )
        .route("/deposits", get(routes::list_deposits))
        .route("/deposits/{id}", get(routes::get_deposit))
        .route("/transactions/{hash}", get(routes::get_transaction))
        .route("/balances/{user_id}", get(routes::get_balances))
        .route("/ledger/{user_id}", get(routes::get_ledger))
        .route(
            "/withdrawals",
            post(routes::create_withdrawal).get(routes::list_withdrawals),
        )
        .route("/withdrawals/{id}", get(routes::get_withdrawal))
        .route("/withdrawals/{id}/cancel", post(routes::cancel_withdrawal))
        .route(
            "/withdrawals/{id}/approve",
            post(routes::approve_withdrawal),
        )
        .route_layer(axum::middleware::from_fn_with_state(
            Arc::clone(&state),
            middleware::auth_layer,
        ))
        .with_state(Arc::clone(&state));

    let operational = Router::new()
        .route("/health", get(routes::health))
        .route("/ready", get(routes::ready))
        .route("/metrics", get(routes::metrics))
        .route("/internal/ledger-integrity", get(routes::ledger_integrity))
        .route("/openapi.json", get(openapi::spec))
        .with_state(Arc::clone(&state));

    Router::new()
        .nest("/v1", v1)
        .merge(operational)
        .layer(axum::middleware::from_fn_with_state(
            limiter,
            middleware::rate_limit_layer,
        ))
        .layer(axum::middleware::from_fn(middleware::context_layer))
        // 504 rather than the default 408: the request did not time out on the
        // client's side, our upstream work did.
        .layer(TimeoutLayer::with_status_code(
            axum::http::StatusCode::GATEWAY_TIMEOUT,
            Duration::from_millis(state.cfg.http.request_timeout_ms),
        ))
        // Outermost: refuse oversized bodies before reading them.
        .layer(RequestBodyLimitLayer::new(state.cfg.http.max_body_bytes))
}

/// Serve the API until `cancel` fires, then drain in-flight requests.
pub async fn serve(
    state: Arc<AppState>,
    cancel: tokio_util::sync::CancellationToken,
) -> chainrail_common::Result<()> {
    use chainrail_common::Error;

    let bind = state.cfg.http.bind.clone();
    let app = router(state);
    let listener = tokio::net::TcpListener::bind(&bind)
        .await
        .map_err(|e| Error::Config(format!("binding {bind}: {e}")))?;
    tracing::info!(%bind, "http api listening");

    axum::serve(
        listener,
        app.into_make_service_with_connect_info::<std::net::SocketAddr>(),
    )
    // Graceful shutdown: stop accepting, let in-flight requests finish. Without
    // this a deploy would drop requests mid-transaction.
    .with_graceful_shutdown(async move { cancel.cancelled().await })
    .await
    .map_err(|e| Error::Internal(format!("http server: {e}")))
}
