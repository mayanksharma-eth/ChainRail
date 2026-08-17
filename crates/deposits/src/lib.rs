//! Deposit pipeline: watching, observing, confirming, crediting, and reorg
//! recovery.
//!
//! ```text
//! chain -> Watcher -> engine::persist_scan -> outbox(deposit.observed)
//!                          |
//!                   ConfirmationEngine -> outbox(deposit.confirmed)
//!                          |
//!                   DepositCreditHandler -> ledger -> outbox(deposit.credited)
//!
//!          ReorgEngine (runs before every scan) -> orphan + compensate
//! ```
//!
//! Each stage is independently retryable and idempotent, so any of them can
//! crash, be duplicated, or be replayed without double-crediting.

pub mod confirmations;
pub mod context;
pub mod credit;
pub mod engine;
pub mod reorg;
pub mod watcher;

pub use confirmations::{ConfirmationEngine, ConfirmationPass};
pub use context::{AssetRef, ChainContext};
pub use credit::{CreditOutcome, DepositCreditHandler};
pub use engine::{persist_scan, PersistStats};
pub use reorg::{find_common_ancestor, ReorgOutcome, ReorgReport};
pub use watcher::{next_range, Tick, Watcher};
