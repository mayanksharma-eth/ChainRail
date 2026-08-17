//! Shared primitives for ChainRail: money, chain identity, configuration,
//! errors, and the event envelope. This crate deliberately has no I/O
//! dependencies so it can be used from every layer and tested in isolation.

pub mod chain;
pub mod config;
pub mod error;
pub mod event;
pub mod money;
pub mod retry;

#[cfg(feature = "sqlx")]
mod sqlx_impls;

pub use chain::{Address, ChainId, ChainKind, FinalityPolicy, Hash32};
pub use error::{Error, Result};
pub use event::{EventEnvelope, EventPayload};
pub use money::{Amount, Direction};
