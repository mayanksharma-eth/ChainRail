//! Mapping Postgres errors onto domain errors.
//!
//! Constraint *names* are the contract between the schema and the application.
//! Translating them here means a unique-violation surfaces as
//! `Error::Conflict`/`InsufficientBalance` with a useful message instead of a
//! generic 500, and it keeps that knowledge in exactly one place.

use chainrail_common::{Error, Result};

/// SQLSTATE classes we care about.
const UNIQUE_VIOLATION: &str = "23505";
const CHECK_VIOLATION: &str = "23514";
const FOREIGN_KEY_VIOLATION: &str = "23503";
const RESTRICT_VIOLATION: &str = "23001";
const SERIALIZATION_FAILURE: &str = "40001";
const DEADLOCK_DETECTED: &str = "40P01";
const LOCK_NOT_AVAILABLE: &str = "55P03";
const QUERY_CANCELED: &str = "57014";

pub fn map_sqlx(err: sqlx::Error) -> Error {
    match &err {
        sqlx::Error::RowNotFound => Error::NotFound { entity: "row" },
        sqlx::Error::PoolTimedOut => {
            Error::Unavailable("database connection pool exhausted".into())
        }
        sqlx::Error::Database(db) => map_db_error(db.as_ref(), &err),
        _ => Error::Database(err.to_string()),
    }
}

fn map_db_error(db: &dyn sqlx::error::DatabaseError, original: &sqlx::Error) -> Error {
    let code = db.code().unwrap_or_default().to_string();
    let constraint = db.constraint().unwrap_or_default().to_string();
    let message = db.message().to_string();

    match code.as_str() {
        UNIQUE_VIOLATION => match constraint.as_str() {
            "users_external_id_key" => {
                Error::Conflict("a user with that external_id exists".into())
            }
            "deposit_addresses_chain_address_key" => {
                Error::Conflict("that deposit address is already assigned".into())
            }
            "deposit_addresses_user_chain_key" => {
                Error::Conflict("user already has a deposit address on that chain".into())
            }
            "blockchain_transactions_natural_key" => {
                Error::Conflict("transfer already observed".into())
            }
            "deposits_blockchain_transaction_key" => {
                Error::Conflict("deposit already exists for that transfer".into())
            }
            "withdrawals_idempotency_key" => Error::Conflict("idempotency key already used".into()),
            "ledger_transactions_idempotency_key" => {
                Error::Conflict("ledger transaction already posted".into())
            }
            "chain_blocks_canonical_height_key" => Error::Conflict(
                "another block is already canonical at that height; orphan it first".into(),
            ),
            "outbox_event_id_key" => Error::Conflict("event already enqueued".into()),
            "assets_chain_symbol_key" | "assets_chain_contract_key" => {
                Error::Conflict("asset already registered on that chain".into())
            }
            other => Error::Conflict(format!("unique constraint violated: {other}")),
        },

        CHECK_VIOLATION => match constraint.as_str() {
            // The DB is the authority on solvency; surface it as a domain error
            // so the API returns 409 with a precise reason, not a 500.
            "ledger_accounts_non_negative" => Error::InsufficientBalance {
                available: "insufficient".into(),
                requested: "requested".into(),
            },
            _ if message.contains("does not balance") => Error::UnbalancedLedgerTransaction {
                net: message
                    .rsplit_once("= ")
                    .map(|(_, n)| n.to_string())
                    .unwrap_or_else(|| "unknown".into()),
            },
            _ if message.contains("double-entry requires") => Error::UnbalancedLedgerTransaction {
                net: "single-sided".into(),
            },
            _ if message.contains("illegal withdrawal transition") => {
                // "illegal withdrawal transition X -> Y (withdrawal Z)"
                let (from, to) = parse_transition(&message);
                Error::InvalidStateTransition {
                    entity: "withdrawal",
                    from,
                    to,
                }
            }
            other => Error::Validation(format!("check constraint violated: {other}")),
        },

        FOREIGN_KEY_VIOLATION => Error::Validation(format!(
            "referenced row does not exist ({})",
            if constraint.is_empty() {
                "foreign key"
            } else {
                &constraint
            }
        )),

        RESTRICT_VIOLATION if message.contains("append-only") => {
            Error::Internal("attempted to mutate append-only ledger history".into())
        }

        // Transient: safe to retry the whole transaction.
        SERIALIZATION_FAILURE | DEADLOCK_DETECTED | LOCK_NOT_AVAILABLE => Error::Database(format!(
            "transient conflict ({code}), retry the transaction"
        )),
        QUERY_CANCELED => Error::Timeout(0),

        _ => Error::Database(original.to_string()),
    }
}

fn parse_transition(message: &str) -> (String, String) {
    let after = message
        .split_once("transition ")
        .map(|(_, r)| r)
        .unwrap_or(message);
    let head = after.split_once(" (").map(|(l, _)| l).unwrap_or(after);
    match head.split_once(" -> ") {
        Some((f, t)) => (f.trim().to_string(), t.trim().to_string()),
        None => ("unknown".into(), "unknown".into()),
    }
}

/// True when a failed transaction may be retried verbatim.
pub fn is_transient(err: &sqlx::Error) -> bool {
    match err {
        sqlx::Error::PoolTimedOut | sqlx::Error::Io(_) => true,
        sqlx::Error::Database(db) => matches!(
            db.code().as_deref(),
            Some(SERIALIZATION_FAILURE) | Some(DEADLOCK_DETECTED) | Some(LOCK_NOT_AVAILABLE)
        ),
        _ => false,
    }
}

pub trait SqlxResultExt<T> {
    fn map_db(self) -> Result<T>;
}

impl<T> SqlxResultExt<T> for std::result::Result<T, sqlx::Error> {
    fn map_db(self) -> Result<T> {
        self.map_err(map_sqlx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn transition_messages_parse() {
        let (f, t) = parse_transition(
            "illegal withdrawal transition requested -> completed (withdrawal abc)",
        );
        assert_eq!((f.as_str(), t.as_str()), ("requested", "completed"));
    }

    #[test]
    fn malformed_transition_message_degrades_gracefully() {
        let (f, t) = parse_transition("something unexpected");
        assert_eq!((f.as_str(), t.as_str()), ("unknown", "unknown"));
    }

    #[test]
    fn row_not_found_maps_to_not_found() {
        assert!(matches!(
            map_sqlx(sqlx::Error::RowNotFound),
            Error::NotFound { .. }
        ));
    }

    #[test]
    fn pool_timeout_is_retryable() {
        let e = map_sqlx(sqlx::Error::PoolTimedOut);
        assert!(e.is_retryable());
    }
}
