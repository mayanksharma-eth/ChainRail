//! HTTP error representation.
//!
//! One shape for every error the API can return, so clients can branch on
//! `error.code` rather than parsing prose or guessing from the status.
//!
//! Server-side errors deliberately do **not** echo their internal message to
//! the client: a database error string can contain schema details, and an RPC
//! error can contain a provider URL with an embedded API key. The full detail
//! goes to the logs, keyed by `request_id`, so support can correlate the two.

use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use chainrail_common::Error;
use serde::Serialize;

#[derive(Debug, Serialize)]
pub struct ErrorBody {
    pub error: ErrorDetail,
}

#[derive(Debug, Serialize)]
pub struct ErrorDetail {
    /// Stable machine-readable code. Safe to branch on.
    pub code: String,
    /// Human-readable message. Safe to display; never contains internals.
    pub message: String,
    /// Echoed so a user can quote it to support.
    pub request_id: String,
    /// Present when the client may retry the identical request.
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    pub retryable: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub details: Option<serde_json::Value>,
}

pub struct ApiError {
    pub status: StatusCode,
    pub inner: Error,
    pub request_id: String,
}

impl ApiError {
    pub fn new(inner: Error, request_id: impl Into<String>) -> Self {
        ApiError {
            status: status_for(&inner),
            inner,
            request_id: request_id.into(),
        }
    }
}

/// HTTP status for a domain error.
pub fn status_for(err: &Error) -> StatusCode {
    match err {
        Error::Validation(_)
        | Error::InvalidAmount(_)
        | Error::AmountExceedsRepresentableRange(_)
        | Error::InvalidAddress { .. }
        | Error::UnsupportedChain(_)
        | Error::UnsupportedAsset(_) => StatusCode::BAD_REQUEST,

        Error::NotFound { .. } => StatusCode::NOT_FOUND,

        // 409: the request was well-formed but conflicts with current state.
        Error::Conflict(_)
        | Error::IdempotencyKeyConflict
        | Error::InvalidStateTransition { .. }
        | Error::InsufficientBalance { .. }
        | Error::UnbalancedLedgerTransaction { .. } => StatusCode::CONFLICT,

        // 422: understood and consistent, but refused by policy.
        Error::PolicyDenied { .. } => StatusCode::UNPROCESSABLE_ENTITY,

        Error::RateLimited => StatusCode::TOO_MANY_REQUESTS,
        Error::Timeout(_) => StatusCode::GATEWAY_TIMEOUT,

        Error::Unavailable(_)
        | Error::NoHealthyRpcEndpoint { .. }
        | Error::EventBus(_)
        | Error::Cache(_)
        | Error::Rpc(_) => StatusCode::SERVICE_UNAVAILABLE,

        Error::Database(_)
        | Error::Signer(_)
        | Error::Config(_)
        | Error::Internal(_)
        | Error::AmountOverflow => StatusCode::INTERNAL_SERVER_ERROR,
    }
}

/// Client-safe message. 5xx errors are replaced with a generic string.
fn safe_message(err: &Error) -> String {
    if err.is_client_error() {
        err.to_string()
    } else {
        match err {
            Error::Unavailable(_) | Error::NoHealthyRpcEndpoint { .. } => {
                "a downstream dependency is temporarily unavailable".into()
            }
            Error::Timeout(_) => "the request timed out".into(),
            Error::EventBus(_) | Error::Cache(_) | Error::Rpc(_) => {
                "a downstream dependency is temporarily unavailable".into()
            }
            // Anything else is an internal fault; the detail is in the logs.
            _ => "an internal error occurred".into(),
        }
    }
}

/// Structured extra fields for errors where a client can act on the specifics.
fn details(err: &Error) -> Option<serde_json::Value> {
    match err {
        Error::InsufficientBalance {
            available,
            requested,
        } => Some(serde_json::json!({
            "available": available,
            "requested": requested,
        })),
        Error::InvalidStateTransition { entity, from, to } => Some(serde_json::json!({
            "entity": entity,
            "from": from,
            "to": to,
        })),
        Error::PolicyDenied { code, .. } => Some(serde_json::json!({ "policy": code })),
        Error::InvalidAddress { chain, reason } => Some(serde_json::json!({
            "chain": chain,
            "reason": reason,
        })),
        _ => None,
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        // Log the *full* error server-side, keyed by request id.
        if self.status.is_server_error() {
            tracing::error!(
                request_id = %self.request_id,
                code = self.inner.code(),
                status = self.status.as_u16(),
                error = %self.inner,
                "request failed"
            );
        } else {
            tracing::info!(
                request_id = %self.request_id,
                code = self.inner.code(),
                status = self.status.as_u16(),
                error = %self.inner,
                "request rejected"
            );
        }

        let body = ErrorBody {
            error: ErrorDetail {
                code: self.inner.code().to_string(),
                message: safe_message(&self.inner),
                request_id: self.request_id,
                retryable: self.inner.is_retryable(),
                details: details(&self.inner),
            },
        };
        (self.status, Json(body)).into_response()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn client_errors_map_to_4xx_and_server_errors_to_5xx() {
        assert_eq!(
            status_for(&Error::Validation("x".into())),
            StatusCode::BAD_REQUEST
        );
        assert_eq!(
            status_for(&Error::NotFound { entity: "deposit" }),
            StatusCode::NOT_FOUND
        );
        assert_eq!(
            status_for(&Error::IdempotencyKeyConflict),
            StatusCode::CONFLICT
        );
        assert_eq!(
            status_for(&Error::PolicyDenied {
                code: "above_maximum".into(),
                message: "too big".into()
            }),
            StatusCode::UNPROCESSABLE_ENTITY
        );
        assert_eq!(
            status_for(&Error::RateLimited),
            StatusCode::TOO_MANY_REQUESTS
        );
        assert_eq!(
            status_for(&Error::Database("boom".into())),
            StatusCode::INTERNAL_SERVER_ERROR
        );
        assert_eq!(
            status_for(&Error::NoHealthyRpcEndpoint {
                chain: "base".into()
            }),
            StatusCode::SERVICE_UNAVAILABLE
        );
    }

    #[test]
    fn insufficient_balance_is_a_conflict_not_a_bad_request() {
        // The request was valid; the account state made it impossible.
        assert_eq!(
            status_for(&Error::InsufficientBalance {
                available: "1".into(),
                requested: "2".into()
            }),
            StatusCode::CONFLICT
        );
    }

    #[test]
    fn server_error_messages_never_leak_internals() {
        for err in [
            Error::Database("relation \"secret_table\" does not exist".into()),
            Error::Rpc("https://provider.example/v2/APIKEY123 failed".into()),
            Error::Signer("key 0xdeadbeef rejected".into()),
            Error::Internal("panic in module foo".into()),
        ] {
            let msg = safe_message(&err);
            assert!(!msg.contains("secret_table"), "leaked: {msg}");
            assert!(!msg.contains("APIKEY123"), "leaked: {msg}");
            assert!(!msg.contains("0xdeadbeef"), "leaked: {msg}");
            assert!(!msg.contains("panic"), "leaked: {msg}");
        }
    }

    #[test]
    fn client_error_messages_are_preserved_because_they_are_actionable() {
        let e = Error::InvalidAddress {
            chain: "evm".into(),
            reason: "EIP-55 checksum mismatch".into(),
        };
        assert!(safe_message(&e).contains("checksum"));
    }

    #[test]
    fn actionable_errors_carry_structured_details() {
        let d = details(&Error::InsufficientBalance {
            available: "100".into(),
            requested: "250".into(),
        })
        .unwrap();
        assert_eq!(d["available"], "100");
        assert_eq!(d["requested"], "250");
        assert!(details(&Error::Internal("x".into())).is_none());
    }

    #[test]
    fn retryability_is_surfaced_to_the_client() {
        assert!(Error::Unavailable("db".into()).is_retryable());
        assert!(!Error::Validation("bad".into()).is_retryable());
    }
}
