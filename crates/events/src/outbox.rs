//! Transactional outbox relay.
//!
//! # Why an outbox
//!
//! Crediting a deposit and publishing `deposit.credited` are two writes to two
//! systems. Doing them independently gives a window where one succeeds and the
//! other does not:
//!
//!   * publish-then-commit -> an event for a credit that never happened;
//!   * commit-then-publish -> a credit no downstream consumer ever hears about.
//!
//! Instead, the business change and a row in `outbox` commit in one database
//! transaction. This relay then moves rows to Kafka at-least-once. Consumers are
//! idempotent (see `dedupe`), so at-least-once delivery is sufficient.
//!
//! Ordering: rows are claimed oldest-first, but with `SKIP LOCKED` and several
//! relay instances, global ordering is not guaranteed. Per-*entity* ordering is
//! what matters and is preserved by the Kafka partition key (the user id).

use std::sync::Arc;
use std::time::Duration;

use chainrail_common::config::KafkaConfig;
use chainrail_common::retry::Backoff;
use chainrail_common::{EventEnvelope, Result};
use chainrail_database::{repo, Db};
use tokio_util::sync::CancellationToken;

use crate::bus::SharedBus;

pub struct OutboxRelay {
    db: Db,
    bus: SharedBus,
    batch_size: i64,
    poll_interval: Duration,
    backoff: Backoff,
    max_attempts: u32,
}

/// Outcome of one relay pass, returned so callers (and tests) can drive the
/// relay a single step at a time instead of only as a background loop.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct RelayPass {
    pub published: usize,
    pub failed: usize,
    pub dead_lettered: usize,
}

impl RelayPass {
    pub fn did_work(&self) -> bool {
        self.published + self.failed + self.dead_lettered > 0
    }
}

impl OutboxRelay {
    pub fn new(db: Db, bus: SharedBus, cfg: &KafkaConfig) -> Self {
        OutboxRelay {
            db,
            bus,
            batch_size: cfg.outbox_batch_size.clamp(1, 5_000),
            poll_interval: Duration::from_millis(cfg.outbox_poll_interval_ms),
            backoff: Backoff::new(
                cfg.retry_backoff_base_ms,
                cfg.retry_backoff_max_ms,
                cfg.max_delivery_attempts,
            ),
            max_attempts: cfg.max_delivery_attempts,
        }
    }

    /// Publish one batch.
    ///
    /// Each row is claimed under `FOR UPDATE SKIP LOCKED`, published, then
    /// marked. A crash between publish and mark re-delivers the event, which is
    /// exactly why consumers must be idempotent.
    pub async fn run_once(&self) -> Result<RelayPass> {
        let mut tx = self.db.begin().await?;
        let batch = repo::outbox::claim_batch(&mut tx, self.batch_size).await?;
        if batch.is_empty() {
            tx.commit().await.ok();
            return Ok(RelayPass::default());
        }

        let mut pass = RelayPass::default();
        let mut published_ids = Vec::with_capacity(batch.len());

        for row in batch {
            let envelope: EventEnvelope = match serde_json::from_value(row.payload.clone()) {
                Ok(e) => e,
                Err(e) => {
                    // Unparseable payload will never succeed; retrying forever
                    // would block the queue head. Straight to the dead letter.
                    tracing::error!(
                        outbox_id = %row.id, error = %e,
                        "outbox row payload is not a valid event envelope; dead-lettering"
                    );
                    repo::outbox::record_dead_letter(
                        &mut *tx,
                        repo::outbox::DeadLetter {
                            source_topic: &row.topic,
                            consumer: Some("outbox_relay"),
                            event_id: Some(row.event_id),
                            event_type: None,
                            payload: row.payload.clone(),
                            error: &format!("undeserializable envelope: {e}"),
                            attempts: row.attempts,
                            correlation_id: row.correlation_id.as_deref(),
                        },
                    )
                    .await?;
                    published_ids.push(row.id); // remove from the queue
                    pass.dead_lettered += 1;
                    continue;
                }
            };

            match self.bus.publish(&envelope).await {
                Ok(()) => {
                    published_ids.push(row.id);
                    pass.published += 1;
                }
                Err(e) => {
                    let attempts = u32::try_from(row.attempts).unwrap_or(u32::MAX) + 1;
                    if attempts >= self.max_attempts {
                        tracing::error!(
                            outbox_id = %row.id, event_id = %row.event_id,
                            topic = %row.topic, attempts, error = %e,
                            "outbox delivery exhausted its retry budget; dead-lettering"
                        );
                        repo::outbox::record_dead_letter(
                            &mut *tx,
                            repo::outbox::DeadLetter {
                                source_topic: &row.topic,
                                consumer: Some("outbox_relay"),
                                event_id: Some(row.event_id),
                                event_type: Some(&envelope.event_type),
                                payload: row.payload.clone(),
                                error: &e.to_string(),
                                attempts: attempts as i32,
                                correlation_id: row.correlation_id.as_deref(),
                            },
                        )
                        .await?;
                        published_ids.push(row.id);
                        pass.dead_lettered += 1;
                        metrics::counter!("chainrail_events_dead_lettered_total").increment(1);
                    } else {
                        let delay = self.backoff.delay(attempts);
                        repo::outbox::mark_failed(&mut tx, row.id, &e.to_string(), delay).await?;
                        pass.failed += 1;
                        metrics::counter!("chainrail_events_publish_failures_total").increment(1);
                    }
                }
            }
        }

        repo::outbox::mark_published(&mut tx, &published_ids).await?;
        tx.commit().await.map_err(chainrail_database::map_sqlx)?;

        if pass.published > 0 {
            tracing::debug!(published = pass.published, "outbox batch relayed");
        }
        Ok(pass)
    }

    /// Background loop. Sleeps when idle; returns on cancellation.
    pub async fn run(self: Arc<Self>, cancel: CancellationToken) {
        tracing::info!(
            backend = self.bus.backend_name(),
            batch_size = self.batch_size,
            "outbox relay started"
        );
        let mut consecutive_errors: u32 = 0;
        loop {
            if cancel.is_cancelled() {
                break;
            }
            match self.run_once().await {
                Ok(pass) => {
                    consecutive_errors = 0;
                    self.report_lag().await;
                    // Only sleep when there was nothing to do, so a backlog is
                    // drained as fast as the broker allows.
                    if !pass.did_work() {
                        tokio::select! {
                            _ = tokio::time::sleep(self.poll_interval) => {}
                            _ = cancel.cancelled() => break,
                        }
                    }
                }
                Err(e) => {
                    consecutive_errors += 1;
                    // The database itself is unavailable. Back off rather than
                    // spinning; the relay is not on a latency-critical path.
                    let delay = self.backoff.delay(consecutive_errors.min(6));
                    tracing::error!(error = %e, consecutive_errors, ?delay, "outbox relay pass failed");
                    tokio::select! {
                        _ = tokio::time::sleep(delay) => {}
                        _ = cancel.cancelled() => break,
                    }
                }
            }
        }
        tracing::info!("outbox relay stopped");
    }

    async fn report_lag(&self) {
        if let Ok(pending) = repo::outbox::pending_count(self.db.pool()).await {
            metrics::gauge!("chainrail_outbox_pending").set(pending as f64);
        }
        if let Ok(Some(age)) = repo::outbox::oldest_pending_age_secs(self.db.pool()).await {
            metrics::gauge!("chainrail_outbox_oldest_pending_seconds").set(age);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn relay_pass_reports_whether_it_did_anything() {
        assert!(!RelayPass::default().did_work());
        assert!(RelayPass {
            published: 1,
            ..Default::default()
        }
        .did_work());
        assert!(RelayPass {
            failed: 1,
            ..Default::default()
        }
        .did_work());
        assert!(RelayPass {
            dead_lettered: 1,
            ..Default::default()
        }
        .did_work());
    }

    #[test]
    fn batch_size_is_clamped_to_something_sane() {
        let mk = |n: i64| KafkaConfig {
            brokers: "localhost:9092".into(),
            consumer_group: "g".into(),
            topic_prefix: String::new(),
            request_timeout_ms: 1_000,
            max_delivery_attempts: 5,
            retry_backoff_base_ms: 100,
            retry_backoff_max_ms: 1_000,
            use_outbox: true,
            outbox_poll_interval_ms: 100,
            outbox_batch_size: n,
        };
        // A zero or negative batch size would make the relay a no-op loop that
        // silently never delivers anything -- the worst possible failure mode.
        for (input, expected) in [(0i64, 1i64), (-5, 1), (100, 100), (999_999, 5_000)] {
            let cfg = mk(input);
            let clamped = cfg.outbox_batch_size.clamp(1, 5_000);
            assert_eq!(clamped, expected);
        }
    }
}
