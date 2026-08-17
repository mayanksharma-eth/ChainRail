//! Withdrawal handling: request validation and reservation (synchronous), then
//! signing, broadcast, confirmation and settlement (asynchronous).
//!
//! ```text
//! POST /v1/withdrawals
//!   -> validate -> risk -> idempotency -> reserve -> approved
//!                                                      |
//!                                     WithdrawalPipeline (worker)
//!                                       signing -> broadcast -> confirming
//!                                                      |
//!                                              settle or reverse
//! ```

pub mod pipeline;
pub mod service;

pub use pipeline::{may_resign, PipelinePass, WithdrawalPipeline};
pub use service::{CreateResult, WithdrawalRequest, WithdrawalService};
