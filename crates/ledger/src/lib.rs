//! Append-only double-entry ledger.
//!
//! ChainRail's source of financial truth. Every balance the API reports is
//! derived from entries in this ledger; nothing else may move money.
//!
//! Design rules, all enforced rather than merely documented:
//!   1. Entries are immutable. Corrections are compensating transactions.
//!   2. Every transaction balances: `sum(debits) == sum(credits)`.
//!   3. Amounts are positive integers; `direction` carries the sign.
//!   4. Postings are idempotent on a natural key, so every caller is retryable.
//!   5. Spendable balances cannot go negative -- a database CHECK, not an `if`.
//!
//! See `docs/ledger.md` for the accounting convention and worked examples.

pub mod accounts;
pub mod integrity;
pub mod operations;
pub mod posting;
pub mod service;

pub use accounts::{system_account, user_account, user_balance};
pub use integrity::{verify_ledger_integrity, IntegrityReport};
pub use operations::{
    credit_deposit, fund_custody, record_broadcast, record_network_fee, release_reservation,
    reserve_withdrawal, reverse_broadcast, reverse_deposit_credit, settle_withdrawal,
};
pub use posting::{EntryRequest, Posting, ReferenceType, TransactionKind};
pub use service::{
    get_balance, get_balances, get_ledger_entries, get_transaction, get_transaction_entries,
    post_transaction, PostResult,
};
