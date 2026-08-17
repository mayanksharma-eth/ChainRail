//! Shared error type.
//!
//! Errors carry enough structure for the API layer to map them onto stable
//! machine-readable codes without string matching, and deliberately never carry
//! secrets (keys, connection strings, raw request bodies).

use std::fmt;

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    // --- money / arithmetic ---
    #[error("arithmetic overflow on monetary amount")]
    AmountOverflow,
    #[error("invalid amount: {0}")]
    InvalidAmount(String),
    #[error("amount exceeds representable range: {0}")]
    AmountExceedsRepresentableRange(String),

    // --- validation ---
    #[error("validation failed: {0}")]
    Validation(String),
    #[error("unsupported chain: {0}")]
    UnsupportedChain(String),
    #[error("unsupported asset: {0}")]
    UnsupportedAsset(String),
    #[error("invalid address for {chain}: {reason}")]
    InvalidAddress { chain: String, reason: String },

    // --- domain ---
    #[error("{entity} not found")]
    NotFound { entity: &'static str },
    #[error("conflict: {0}")]
    Conflict(String),
    #[error("insufficient balance: available {available}, requested {requested}")]
    InsufficientBalance {
        available: String,
        requested: String,
    },
    #[error("invalid state transition: {entity} cannot go {from} -> {to}")]
    InvalidStateTransition {
        entity: &'static str,
        from: String,
        to: String,
    },
    #[error("ledger transaction does not balance: net {net}")]
    UnbalancedLedgerTransaction { net: String },
    #[error("policy denied: {code}: {message}")]
    PolicyDenied { code: String, message: String },
    #[error("idempotency key reused with a different request payload")]
    IdempotencyKeyConflict,

    // --- infrastructure ---
    #[error("database error: {0}")]
    Database(String),
    #[error("cache error: {0}")]
    Cache(String),
    #[error("event bus error: {0}")]
    EventBus(String),
    #[error("rpc error: {0}")]
    Rpc(String),
    #[error("all rpc endpoints unhealthy for chain {chain}")]
    NoHealthyRpcEndpoint { chain: String },
    #[error("signer error: {0}")]
    Signer(String),
    #[error("configuration error: {0}")]
    Config(String),
    #[error("operation timed out after {0}ms")]
    Timeout(u64),
    #[error("service unavailable: {0}")]
    Unavailable(String),
    #[error("rate limited")]
    RateLimited,
    #[error("internal error: {0}")]
    Internal(String),
}

impl Error {
    /// Stable, machine-readable error code returned to API clients.
    pub fn code(&self) -> &'static str {
        match self {
            Error::AmountOverflow => "amount_overflow",
            Error::InvalidAmount(_) => "invalid_amount",
            Error::AmountExceedsRepresentableRange(_) => "amount_out_of_range",
            Error::Validation(_) => "validation_error",
            Error::UnsupportedChain(_) => "unsupported_chain",
            Error::UnsupportedAsset(_) => "unsupported_asset",
            Error::InvalidAddress { .. } => "invalid_address",
            Error::NotFound { .. } => "not_found",
            Error::Conflict(_) => "conflict",
            Error::InsufficientBalance { .. } => "insufficient_balance",
            Error::InvalidStateTransition { .. } => "invalid_state_transition",
            Error::UnbalancedLedgerTransaction { .. } => "unbalanced_ledger_transaction",
            Error::PolicyDenied { .. } => "policy_denied",
            Error::IdempotencyKeyConflict => "idempotency_key_conflict",
            Error::Database(_) => "database_error",
            Error::Cache(_) => "cache_error",
            Error::EventBus(_) => "event_bus_error",
            Error::Rpc(_) => "rpc_error",
            Error::NoHealthyRpcEndpoint { .. } => "no_healthy_rpc_endpoint",
            Error::Signer(_) => "signer_error",
            Error::Config(_) => "config_error",
            Error::Timeout(_) => "timeout",
            Error::Unavailable(_) => "service_unavailable",
            Error::RateLimited => "rate_limited",
            Error::Internal(_) => "internal_error",
        }
    }

    /// Whether the *same* request may be retried without a correctness risk.
    /// Note this is about the error, not the operation: an operation with
    /// external side effects must additionally be idempotent to be retried.
    pub fn is_retryable(&self) -> bool {
        matches!(
            self,
            Error::Database(_)
                | Error::Cache(_)
                | Error::EventBus(_)
                | Error::Rpc(_)
                | Error::NoHealthyRpcEndpoint { .. }
                | Error::Timeout(_)
                | Error::Unavailable(_)
                | Error::RateLimited
        )
    }

    /// Client error (4xx) vs server error (5xx).
    pub fn is_client_error(&self) -> bool {
        matches!(
            self,
            Error::InvalidAmount(_)
                | Error::AmountExceedsRepresentableRange(_)
                | Error::Validation(_)
                | Error::UnsupportedChain(_)
                | Error::UnsupportedAsset(_)
                | Error::InvalidAddress { .. }
                | Error::NotFound { .. }
                | Error::Conflict(_)
                | Error::InsufficientBalance { .. }
                | Error::InvalidStateTransition { .. }
                | Error::PolicyDenied { .. }
                | Error::IdempotencyKeyConflict
                | Error::RateLimited
        )
    }

    pub fn validation(msg: impl fmt::Display) -> Self {
        Error::Validation(msg.to_string())
    }

    pub fn internal(msg: impl fmt::Display) -> Self {
        Error::Internal(msg.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn codes_are_stable_and_distinct_per_class() {
        assert_eq!(Error::RateLimited.code(), "rate_limited");
        assert_eq!(Error::NotFound { entity: "deposit" }.code(), "not_found");
    }

    #[test]
    fn retryability_matches_class() {
        assert!(Error::Timeout(500).is_retryable());
        assert!(Error::Database("pool closed".into()).is_retryable());
        assert!(!Error::Validation("bad".into()).is_retryable());
        assert!(!Error::InsufficientBalance {
            available: "1".into(),
            requested: "2".into()
        }
        .is_retryable());
    }

    #[test]
    fn client_vs_server_classification() {
        assert!(Error::Validation("x".into()).is_client_error());
        assert!(!Error::Database("x".into()).is_client_error());
        assert!(!Error::Internal("x".into()).is_client_error());
    }
}
