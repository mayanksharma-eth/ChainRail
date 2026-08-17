//! Event transport: Kafka producer/consumer, the transactional outbox relay,
//! and the idempotency machinery that makes at-least-once delivery safe.

pub mod bus;
pub mod consumer;
pub mod outbox;

pub use bus::{prefixed, EventBus, InMemoryEventBus, KafkaEventBus, SharedBus};
pub use consumer::{
    claim, deterministic_event_id, handle_once, validate_handler, ConsumerRunner, EventHandler,
    HandlerOutcome,
};
pub use outbox::{OutboxRelay, RelayPass};

use chainrail_common::config::KafkaConfig;
use chainrail_common::Result;
use std::sync::Arc;

/// Build the configured bus. Falls back to the in-memory bus when no brokers
/// are configured, so the system runs end-to-end without Kafka -- useful for
/// tests, and explicitly logged so it can never be mistaken for production.
pub fn build_bus(cfg: &KafkaConfig) -> Result<SharedBus> {
    if cfg.brokers.trim().is_empty() {
        tracing::warn!(
            "no kafka brokers configured; using the in-memory event bus. \
             Events will not leave this process."
        );
        return Ok(InMemoryEventBus::new());
    }
    Ok(Arc::new(KafkaEventBus::new(cfg)?))
}
