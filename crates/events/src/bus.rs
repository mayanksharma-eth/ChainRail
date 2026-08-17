//! Event transport abstraction and its Kafka / in-memory implementations.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use chainrail_common::config::KafkaConfig;
use chainrail_common::{Error, EventEnvelope, Result};
use parking_lot::Mutex;

#[async_trait]
pub trait EventBus: Send + Sync {
    /// Publish one event. Must be durable before returning: the outbox relay
    /// marks a row published on success, so a lost message here would be a lost
    /// event.
    async fn publish(&self, envelope: &EventEnvelope) -> Result<()>;

    /// Publish a batch. Implementations may pipeline, but must only return
    /// `Ok` once every event is durable.
    async fn publish_batch(&self, envelopes: &[EventEnvelope]) -> Result<()> {
        for e in envelopes {
            self.publish(e).await?;
        }
        Ok(())
    }

    fn backend_name(&self) -> &'static str;

    async fn health(&self) -> Result<()>;
}

pub type SharedBus = Arc<dyn EventBus>;

/// Apply the configured topic prefix. Lets several environments share a broker.
pub fn prefixed(prefix: &str, topic: &str) -> String {
    if prefix.is_empty() {
        topic.to_string()
    } else {
        format!("{prefix}{topic}")
    }
}

// ------------------------------------------------------------------ kafka ---

pub struct KafkaEventBus {
    producer: rdkafka::producer::FutureProducer,
    topic_prefix: String,
    timeout: Duration,
}

impl KafkaEventBus {
    pub fn new(cfg: &KafkaConfig) -> Result<Self> {
        use rdkafka::config::ClientConfig;

        let producer: rdkafka::producer::FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", &cfg.brokers)
            .set("message.timeout.ms", cfg.request_timeout_ms.to_string())
            // `acks=all` plus idempotence: the producer will not silently drop
            // or duplicate on broker failover. The cost is latency, which is
            // the right trade for financial events.
            .set("acks", "all")
            .set("enable.idempotence", "true")
            .set("max.in.flight.requests.per.connection", "5")
            .set("retries", "10")
            .set("compression.type", "lz4")
            .set("linger.ms", "5")
            .create()
            .map_err(|e| Error::EventBus(format!("creating kafka producer: {e}")))?;

        Ok(KafkaEventBus {
            producer,
            topic_prefix: cfg.topic_prefix.clone(),
            timeout: Duration::from_millis(cfg.request_timeout_ms),
        })
    }
}

#[async_trait]
impl EventBus for KafkaEventBus {
    async fn publish(&self, envelope: &EventEnvelope) -> Result<()> {
        use rdkafka::producer::{FutureRecord, Producer};
        let _ = Producer::client(&self.producer);

        let topic = prefixed(&self.topic_prefix, envelope.topic());
        let payload = serde_json::to_vec(envelope)
            .map_err(|e| Error::EventBus(format!("serializing event: {e}")))?;
        let key = envelope.partition_key();
        let event_id = envelope.event_id.to_string();

        let record = FutureRecord::to(&topic)
            .payload(&payload)
            .key(&key)
            // Carried in headers so a consumer can dedupe without parsing the
            // body, and so `kcat` output is legible during an incident.
            .headers(
                rdkafka::message::OwnedHeaders::new()
                    .insert(rdkafka::message::Header {
                        key: "event_id",
                        value: Some(&event_id),
                    })
                    .insert(rdkafka::message::Header {
                        key: "event_type",
                        value: Some(&envelope.event_type),
                    })
                    .insert(rdkafka::message::Header {
                        key: "correlation_id",
                        value: Some(&envelope.correlation_id),
                    }),
            );

        match self.producer.send(record, self.timeout).await {
            Ok(_) => {
                metrics::counter!(
                    "chainrail_events_published_total",
                    "topic" => topic,
                )
                .increment(1);
                Ok(())
            }
            Err((e, _)) => Err(Error::EventBus(format!("publishing to {topic}: {e}"))),
        }
    }

    fn backend_name(&self) -> &'static str {
        "kafka"
    }

    async fn health(&self) -> Result<()> {
        use rdkafka::producer::Producer;
        self.producer
            .client()
            .fetch_metadata(None, Duration::from_secs(3))
            .map(|_| ())
            .map_err(|e| Error::EventBus(format!("kafka metadata: {e}")))
    }
}

// -------------------------------------------------------------- in-memory ---

/// In-process bus for tests and for running the worker without a broker.
///
/// This is a genuine implementation of the interface, not a stub that discards
/// events: published events are retained and can be asserted against, so tests
/// verify real publish behaviour.
#[derive(Default)]
pub struct InMemoryEventBus {
    published: Mutex<Vec<EventEnvelope>>,
    /// When set, `publish` fails. Used to exercise outbox retry paths.
    fail: Mutex<Option<String>>,
}

impl InMemoryEventBus {
    pub fn new() -> Arc<Self> {
        Arc::new(InMemoryEventBus::default())
    }

    pub fn events(&self) -> Vec<EventEnvelope> {
        self.published.lock().clone()
    }

    pub fn events_of_type(&self, event_type: &str) -> Vec<EventEnvelope> {
        self.published
            .lock()
            .iter()
            .filter(|e| e.event_type == event_type)
            .cloned()
            .collect()
    }

    pub fn len(&self) -> usize {
        self.published.lock().len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn clear(&self) {
        self.published.lock().clear();
    }

    /// Make subsequent publishes fail, simulating a broker outage.
    pub fn set_failing(&self, reason: Option<&str>) {
        *self.fail.lock() = reason.map(String::from);
    }

    /// Topics grouped by event type, for assertions.
    pub fn counts_by_type(&self) -> HashMap<String, usize> {
        let mut m = HashMap::new();
        for e in self.published.lock().iter() {
            *m.entry(e.event_type.clone()).or_insert(0) += 1;
        }
        m
    }
}

#[async_trait]
impl EventBus for InMemoryEventBus {
    async fn publish(&self, envelope: &EventEnvelope) -> Result<()> {
        if let Some(reason) = self.fail.lock().clone() {
            return Err(Error::EventBus(reason));
        }
        self.published.lock().push(envelope.clone());
        Ok(())
    }

    fn backend_name(&self) -> &'static str {
        "in_memory"
    }

    async fn health(&self) -> Result<()> {
        match self.fail.lock().clone() {
            Some(r) => Err(Error::EventBus(r)),
            None => Ok(()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chainrail_common::event::{topics, DepositObserved, EventPayload};
    use chainrail_common::Amount;
    use uuid::Uuid;

    fn envelope(tx: &str) -> EventEnvelope {
        EventEnvelope::new(
            EventPayload::DepositObserved(DepositObserved {
                chain: "base-sepolia".into(),
                tx_hash: tx.into(),
                log_index: 0,
                block_number: 1,
                block_hash: "0xbb".into(),
                asset: "USDC".into(),
                user_id: Uuid::nil(),
                deposit_id: Uuid::nil(),
                amount_raw: Amount::new(10),
            }),
            "corr",
        )
    }

    #[test]
    fn topic_prefixing_is_opt_in() {
        assert_eq!(prefixed("", topics::DEPOSITS_OBSERVED), "deposits.observed");
        assert_eq!(
            prefixed("staging.", topics::DEPOSITS_OBSERVED),
            "staging.deposits.observed"
        );
    }

    #[tokio::test]
    async fn in_memory_bus_retains_what_it_publishes() {
        let bus = InMemoryEventBus::new();
        assert!(bus.is_empty());
        bus.publish(&envelope("0xa")).await.unwrap();
        bus.publish(&envelope("0xb")).await.unwrap();
        assert_eq!(bus.len(), 2);
        assert_eq!(bus.events_of_type("deposit.observed").len(), 2);
        assert_eq!(bus.counts_by_type().get("deposit.observed"), Some(&2));
        bus.clear();
        assert!(bus.is_empty());
    }

    #[tokio::test]
    async fn in_memory_bus_can_simulate_an_outage() {
        let bus = InMemoryEventBus::new();
        bus.set_failing(Some("broker unreachable"));
        let err = bus.publish(&envelope("0xa")).await.unwrap_err();
        assert!(matches!(err, Error::EventBus(_)));
        assert!(err.is_retryable(), "a broker outage must be retryable");
        assert!(bus.health().await.is_err());
        assert!(bus.is_empty(), "a failed publish must not be recorded");

        bus.set_failing(None);
        bus.publish(&envelope("0xa")).await.unwrap();
        assert_eq!(bus.len(), 1);
        assert!(bus.health().await.is_ok());
    }

    #[tokio::test]
    async fn batch_publish_is_all_or_error() {
        let bus = InMemoryEventBus::new();
        bus.publish_batch(&[envelope("0xa"), envelope("0xb"), envelope("0xc")])
            .await
            .unwrap();
        assert_eq!(bus.len(), 3);
    }
}
