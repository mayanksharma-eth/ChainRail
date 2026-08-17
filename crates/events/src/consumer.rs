//! Consumer runtime: at-least-once delivery turned into exactly-once
//! *processing*, with bounded retries and a dead-letter path.
//!
//! Contract every handler must satisfy:
//!
//!   1. Claim the event inside its own database transaction via
//!      [`claim`]. A redelivery then sees the claim and skips.
//!   2. Be safe to run twice anyway. The claim is a fast path, not the only
//!      defence -- unique constraints in the schema are.
//!   3. Return `Err` only for *transient* failures. A permanent failure (bad
//!      payload, unknown asset) should be reported as
//!      [`HandlerOutcome::Rejected`] so it goes straight to the dead letter
//!      instead of retrying forever.
//!
//! Offsets are committed only after a message is either handled or
//! dead-lettered, so a crash mid-handling replays rather than skips.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use chainrail_common::config::KafkaConfig;
use chainrail_common::retry::Backoff;
use chainrail_common::{Error, EventEnvelope, Result};
use chainrail_database::{repo, Db, Tx};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::bus::prefixed;

/// What a handler decided about an event.
#[derive(Debug)]
pub enum HandlerOutcome {
    /// Handled, or already handled (duplicate).
    Done,
    /// Permanently unprocessable. Dead-letter immediately; do not retry.
    Rejected(String),
}

#[async_trait]
pub trait EventHandler: Send + Sync {
    /// Stable consumer identity. Used as the `processed_events.consumer` key,
    /// so changing it makes every past event look unprocessed -- treat it as
    /// part of the persistent contract.
    fn name(&self) -> &'static str;

    fn topics(&self) -> Vec<&'static str>;

    async fn handle(&self, envelope: &EventEnvelope) -> Result<HandlerOutcome>;
}

/// Record that `consumer` is processing `event_id`, inside the handler's own
/// transaction. Returns `false` when it was already processed.
pub async fn claim(tx: &mut Tx<'_>, consumer: &str, envelope: &EventEnvelope) -> Result<bool> {
    repo::outbox::claim_event(tx, consumer, envelope.event_id, &envelope.event_type).await
}

/// Drives one handler against a Kafka subscription.
pub struct ConsumerRunner {
    db: Db,
    handler: Arc<dyn EventHandler>,
    cfg: KafkaConfig,
    backoff: Backoff,
}

impl ConsumerRunner {
    pub fn new(db: Db, handler: Arc<dyn EventHandler>, cfg: KafkaConfig) -> Self {
        let backoff = Backoff::new(
            cfg.retry_backoff_base_ms,
            cfg.retry_backoff_max_ms,
            cfg.max_delivery_attempts,
        );
        ConsumerRunner {
            db,
            handler,
            cfg,
            backoff,
        }
    }

    /// Process one already-decoded envelope with retries and a dead-letter
    /// fallback. Separated from Kafka so it can be driven directly by tests and
    /// by the in-memory bus.
    pub async fn process(&self, envelope: &EventEnvelope, raw: &serde_json::Value) -> Result<()> {
        let consumer = self.handler.name();

        // Reject unknown versions rather than guessing at the payload shape.
        if !envelope.is_supported_version() {
            tracing::error!(
                consumer, event_id = %envelope.event_id, version = envelope.version,
                "unsupported event version; dead-lettering"
            );
            self.dead_letter(envelope, raw, "unsupported event version", 0)
                .await?;
            return Ok(());
        }

        // Cheap pre-check outside the transaction. The authoritative check is
        // the handler's own `claim` call; this only avoids pointless work on a
        // hot redelivery path.
        if repo::outbox::was_processed(self.db.pool(), consumer, envelope.event_id).await? {
            metrics::counter!(
                "chainrail_events_duplicates_total",
                "consumer" => consumer.to_string(),
            )
            .increment(1);
            return Ok(());
        }

        let mut attempt = 0u32;
        loop {
            attempt += 1;
            let started = std::time::Instant::now();
            let outcome = self.handler.handle(envelope).await;
            metrics::histogram!(
                "chainrail_event_handler_seconds",
                "consumer" => consumer.to_string(),
                "event_type" => envelope.event_type.clone(),
            )
            .record(started.elapsed().as_secs_f64());

            match outcome {
                Ok(HandlerOutcome::Done) => {
                    metrics::counter!(
                        "chainrail_events_processed_total",
                        "consumer" => consumer.to_string(),
                        "event_type" => envelope.event_type.clone(),
                    )
                    .increment(1);
                    return Ok(());
                }
                Ok(HandlerOutcome::Rejected(reason)) => {
                    tracing::error!(
                        consumer, event_id = %envelope.event_id,
                        event_type = %envelope.event_type, %reason,
                        "handler permanently rejected event; dead-lettering"
                    );
                    self.dead_letter(envelope, raw, &reason, attempt as i32)
                        .await?;
                    return Ok(());
                }
                Err(e) if attempt >= self.cfg.max_delivery_attempts => {
                    tracing::error!(
                        consumer, event_id = %envelope.event_id, attempt, error = %e,
                        "handler exhausted its retry budget; dead-lettering"
                    );
                    self.dead_letter(envelope, raw, &e.to_string(), attempt as i32)
                        .await?;
                    return Ok(());
                }
                Err(e) => {
                    let delay = self.backoff.delay(attempt);
                    tracing::warn!(
                        consumer, event_id = %envelope.event_id, attempt, ?delay, error = %e,
                        "handler failed; retrying"
                    );
                    metrics::counter!(
                        "chainrail_event_handler_retries_total",
                        "consumer" => consumer.to_string(),
                    )
                    .increment(1);
                    tokio::time::sleep(delay).await;
                }
            }
        }
    }

    async fn dead_letter(
        &self,
        envelope: &EventEnvelope,
        raw: &serde_json::Value,
        error: &str,
        attempts: i32,
    ) -> Result<()> {
        repo::outbox::record_dead_letter(
            self.db.pool(),
            repo::outbox::DeadLetter {
                source_topic: envelope.topic(),
                consumer: Some(self.handler.name()),
                event_id: Some(envelope.event_id),
                event_type: Some(&envelope.event_type),
                payload: raw.clone(),
                error,
                attempts,
                correlation_id: Some(&envelope.correlation_id),
            },
        )
        .await?;
        metrics::counter!(
            "chainrail_events_dead_lettered_total",
            "consumer" => self.handler.name().to_string(),
        )
        .increment(1);
        Ok(())
    }

    /// Subscribe to Kafka and process until cancelled.
    pub async fn run(self: Arc<Self>, cancel: CancellationToken) {
        use rdkafka::config::ClientConfig;
        use rdkafka::consumer::{CommitMode, Consumer, StreamConsumer};
        use rdkafka::Message;

        let consumer_name = self.handler.name();
        // Group per handler, so each handler gets its own copy of every event
        // and one slow handler cannot starve another.
        let group = format!("{}-{}", self.cfg.consumer_group, consumer_name);

        let consumer: StreamConsumer = match ClientConfig::new()
            .set("bootstrap.servers", &self.cfg.brokers)
            .set("group.id", &group)
            // Manual commits: an offset must only advance once the event is
            // durably handled or dead-lettered.
            .set("enable.auto.commit", "false")
            .set("auto.offset.reset", "earliest")
            .set("session.timeout.ms", "45000")
            .set("max.poll.interval.ms", "300000")
            .create()
        {
            Ok(c) => c,
            Err(e) => {
                tracing::error!(consumer = consumer_name, error = %e, "could not create kafka consumer");
                return;
            }
        };

        let topics: Vec<String> = self
            .handler
            .topics()
            .iter()
            .map(|t| prefixed(&self.cfg.topic_prefix, t))
            .collect();
        let refs: Vec<&str> = topics.iter().map(String::as_str).collect();
        if let Err(e) = consumer.subscribe(&refs) {
            tracing::error!(consumer = consumer_name, error = %e, "kafka subscribe failed");
            return;
        }
        tracing::info!(consumer = consumer_name, group, ?topics, "consumer started");

        loop {
            let message = tokio::select! {
                m = consumer.recv() => m,
                _ = cancel.cancelled() => break,
            };

            let message = match message {
                Ok(m) => m,
                Err(e) => {
                    tracing::warn!(consumer = consumer_name, error = %e, "kafka receive error");
                    tokio::time::sleep(Duration::from_millis(500)).await;
                    continue;
                }
            };

            let Some(payload) = message.payload() else {
                // Nothing to process, but the offset must still advance or the
                // consumer wedges on this message forever.
                let _ = consumer.commit_message(&message, CommitMode::Async);
                continue;
            };

            let raw: serde_json::Value = match serde_json::from_slice(payload) {
                Ok(v) => v,
                Err(e) => {
                    tracing::error!(consumer = consumer_name, error = %e, "message is not json; dead-lettering");
                    let _ = repo::outbox::record_dead_letter(
                        self.db.pool(),
                        repo::outbox::DeadLetter {
                            source_topic: message.topic(),
                            consumer: Some(consumer_name),
                            event_id: None,
                            event_type: None,
                            payload: serde_json::json!({
                                "raw_base64_len": payload.len(),
                            }),
                            error: &format!("invalid json: {e}"),
                            attempts: 1,
                            correlation_id: None,
                        },
                    )
                    .await;
                    let _ = consumer.commit_message(&message, CommitMode::Async);
                    continue;
                }
            };

            match serde_json::from_value::<EventEnvelope>(raw.clone()) {
                Ok(envelope) => {
                    let span = tracing::info_span!(
                        "consume",
                        consumer = consumer_name,
                        event_id = %envelope.event_id,
                        event_type = %envelope.event_type,
                        correlation_id = %envelope.correlation_id,
                    );
                    let result = {
                        use tracing::Instrument;
                        self.process(&envelope, &raw).instrument(span).await
                    };
                    match result {
                        Ok(()) => {
                            let _ = consumer.commit_message(&message, CommitMode::Async);
                        }
                        Err(e) => {
                            // Only reached when even dead-lettering failed --
                            // i.e. the database is down. Do NOT commit: replay
                            // once we recover.
                            tracing::error!(
                                consumer = consumer_name, error = %e,
                                "could not finalise event handling; leaving offset uncommitted"
                            );
                            tokio::time::sleep(Duration::from_secs(1)).await;
                        }
                    }
                }
                Err(e) => {
                    tracing::error!(consumer = consumer_name, error = %e, "envelope decode failed; dead-lettering");
                    let _ = repo::outbox::record_dead_letter(
                        self.db.pool(),
                        repo::outbox::DeadLetter {
                            source_topic: message.topic(),
                            consumer: Some(consumer_name),
                            event_id: None,
                            event_type: None,
                            payload: raw,
                            error: &format!("envelope decode: {e}"),
                            attempts: 1,
                            correlation_id: None,
                        },
                    )
                    .await;
                    let _ = consumer.commit_message(&message, CommitMode::Async);
                }
            }

            self.report_lag(&consumer);
        }
        tracing::info!(consumer = consumer_name, "consumer stopped");
    }

    fn report_lag(&self, consumer: &rdkafka::consumer::StreamConsumer) {
        use rdkafka::consumer::Consumer;
        // Best-effort: lag needs broker watermarks, and failing to read them
        // must never interrupt processing.
        if let Ok(list) = consumer.assignment() {
            let mut total: i64 = 0;
            for elem in list.elements() {
                if let Ok((_low, high)) = consumer.fetch_watermarks(
                    elem.topic(),
                    elem.partition(),
                    Duration::from_millis(200),
                ) {
                    if let rdkafka::Offset::Offset(pos) = elem.offset() {
                        total += (high - pos).max(0);
                    }
                }
            }
            metrics::gauge!(
                "chainrail_kafka_consumer_lag",
                "consumer" => self.handler.name().to_string(),
            )
            .set(total as f64);
        }
    }
}

/// Convenience for handlers: the standard "claim then act" wrapper.
///
/// Runs `f` inside a transaction after claiming the event; if the event was
/// already processed, `f` is skipped entirely and the transaction rolls back.
pub async fn handle_once<F>(
    db: &Db,
    consumer: &str,
    envelope: &EventEnvelope,
    f: F,
) -> Result<HandlerOutcome>
where
    F: for<'a> FnOnce(
        &'a mut Tx<'static>,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<HandlerOutcome>> + Send + 'a>,
    >,
{
    let mut tx = db.begin().await?;
    if !claim(&mut tx, consumer, envelope).await? {
        tx.rollback().await.ok();
        metrics::counter!(
            "chainrail_events_duplicates_total",
            "consumer" => consumer.to_string(),
        )
        .increment(1);
        return Ok(HandlerOutcome::Done);
    }
    let outcome = f(&mut tx).await;
    match outcome {
        Ok(HandlerOutcome::Done) => {
            tx.commit().await.map_err(chainrail_database::map_sqlx)?;
            Ok(HandlerOutcome::Done)
        }
        // A rejection must not persist the claim: the event goes to the dead
        // letter, and if an operator replays it after a fix, it must be treated
        // as unprocessed.
        Ok(HandlerOutcome::Rejected(r)) => {
            tx.rollback().await.ok();
            Ok(HandlerOutcome::Rejected(r))
        }
        Err(e) => {
            tx.rollback().await.ok();
            Err(e)
        }
    }
}

/// Deterministic event id for a natural key, so re-derived events dedupe.
pub fn deterministic_event_id(event_type: &str, natural_key: &str) -> Uuid {
    Uuid::new_v5(
        &Uuid::NAMESPACE_URL,
        format!("chainrail:{event_type}:{natural_key}").as_bytes(),
    )
}

/// Guard against a handler being registered for a topic it does not consume.
pub fn validate_handler(handler: &dyn EventHandler) -> Result<()> {
    if handler.topics().is_empty() {
        return Err(Error::Config(format!(
            "handler `{}` subscribes to no topics",
            handler.name()
        )));
    }
    for t in handler.topics() {
        if !chainrail_common::event::topics::ALL.contains(&t) {
            return Err(Error::Config(format!(
                "handler `{}` subscribes to unknown topic `{t}`",
                handler.name()
            )));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use chainrail_common::event::topics;

    struct Good;
    #[async_trait]
    impl EventHandler for Good {
        fn name(&self) -> &'static str {
            "good"
        }
        fn topics(&self) -> Vec<&'static str> {
            vec![topics::DEPOSITS_CONFIRMED]
        }
        async fn handle(&self, _e: &EventEnvelope) -> Result<HandlerOutcome> {
            Ok(HandlerOutcome::Done)
        }
    }

    struct NoTopics;
    #[async_trait]
    impl EventHandler for NoTopics {
        fn name(&self) -> &'static str {
            "none"
        }
        fn topics(&self) -> Vec<&'static str> {
            vec![]
        }
        async fn handle(&self, _e: &EventEnvelope) -> Result<HandlerOutcome> {
            Ok(HandlerOutcome::Done)
        }
    }

    struct BadTopic;
    #[async_trait]
    impl EventHandler for BadTopic {
        fn name(&self) -> &'static str {
            "bad"
        }
        fn topics(&self) -> Vec<&'static str> {
            vec!["deposits.obsrved"] // typo
        }
        async fn handle(&self, _e: &EventEnvelope) -> Result<HandlerOutcome> {
            Ok(HandlerOutcome::Done)
        }
    }

    #[test]
    fn handler_topic_registration_is_validated() {
        validate_handler(&Good).unwrap();
        // A typo'd topic would mean a handler that silently never runs.
        assert!(validate_handler(&NoTopics).is_err());
        assert!(validate_handler(&BadTopic).is_err());
    }

    #[test]
    fn deterministic_ids_are_stable_and_key_specific() {
        let a = deterministic_event_id("deposit.observed", "base-sepolia:0xaa:0");
        let b = deterministic_event_id("deposit.observed", "base-sepolia:0xaa:0");
        let c = deterministic_event_id("deposit.observed", "base-sepolia:0xaa:1");
        let d = deterministic_event_id("deposit.credited", "base-sepolia:0xaa:0");
        assert_eq!(a, b);
        assert_ne!(a, c);
        assert_ne!(a, d, "event type must be part of the key");
    }
}
