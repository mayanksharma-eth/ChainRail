//! Transactional outbox (producer side) and processed-event log (consumer side).
//!
//! The outbox removes the dual-write problem: the business change and the
//! intent to publish commit in the *same* database transaction, so it is
//! impossible to credit a deposit without eventually emitting
//! `deposit.credited`, or to emit it for a credit that rolled back.

use chainrail_common::{EventEnvelope, Result};
use chrono::{DateTime, Utc};
use sqlx::{Executor, Postgres};
use uuid::Uuid;

use crate::{SqlxResultExt, Tx};

#[derive(Debug, Clone, sqlx::FromRow)]
pub struct OutboxRow {
    pub id: Uuid,
    pub event_id: Uuid,
    pub topic: String,
    pub partition_key: String,
    pub payload: serde_json::Value,
    pub correlation_id: Option<String>,
    pub attempts: i32,
}

/// Enqueue an event. Must be called with the same transaction as the state
/// change that produced it.
///
/// `ON CONFLICT DO NOTHING` on `event_id` means a retried business operation
/// carrying a deterministic event id enqueues once, not twice.
pub async fn enqueue(tx: &mut Tx<'_>, envelope: &EventEnvelope) -> Result<()> {
    let payload = serde_json::to_value(envelope)
        .map_err(|e| chainrail_common::Error::Internal(format!("event serialization: {e}")))?;
    sqlx::query(
        r#"
        INSERT INTO outbox (event_id, topic, partition_key, payload, correlation_id)
        VALUES ($1, $2, $3, $4, $5)
        ON CONFLICT (event_id) DO NOTHING
        "#,
    )
    .bind(envelope.event_id)
    .bind(envelope.topic())
    .bind(envelope.partition_key())
    .bind(payload)
    .bind(&envelope.correlation_id)
    .execute(&mut **tx)
    .await
    .map_db()?;
    Ok(())
}

/// Claim a batch of due, unpublished events.
///
/// `SKIP LOCKED` lets multiple relay instances run concurrently; each takes a
/// disjoint slice and neither blocks the other.
pub async fn claim_batch(tx: &mut Tx<'_>, limit: i64) -> Result<Vec<OutboxRow>> {
    sqlx::query_as::<_, OutboxRow>(
        r#"
        SELECT id, event_id, topic, partition_key, payload, correlation_id, attempts
          FROM outbox
         WHERE published_at IS NULL AND next_attempt_at <= now()
         ORDER BY created_at ASC
         LIMIT $1
           FOR UPDATE SKIP LOCKED
        "#,
    )
    .bind(limit)
    .fetch_all(&mut **tx)
    .await
    .map_db()
}

pub async fn mark_published(tx: &mut Tx<'_>, ids: &[Uuid]) -> Result<()> {
    if ids.is_empty() {
        return Ok(());
    }
    sqlx::query("UPDATE outbox SET published_at = now(), last_error = NULL WHERE id = ANY($1)")
        .bind(ids)
        .execute(&mut **tx)
        .await
        .map_db()?;
    Ok(())
}

/// Record a publish failure and schedule the retry.
pub async fn mark_failed(
    tx: &mut Tx<'_>,
    id: Uuid,
    error: &str,
    retry_in: std::time::Duration,
) -> Result<()> {
    sqlx::query(
        r#"
        UPDATE outbox
           SET attempts = attempts + 1,
               last_error = left($2, 500),
               next_attempt_at = now() + make_interval(secs => $3)
         WHERE id = $1
        "#,
    )
    .bind(id)
    .bind(error)
    .bind(retry_in.as_secs_f64())
    .execute(&mut **tx)
    .await
    .map_db()?;
    Ok(())
}

pub async fn pending_count<'e, E>(ex: E) -> Result<i64>
where
    E: Executor<'e, Database = Postgres>,
{
    sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM outbox WHERE published_at IS NULL")
        .fetch_one(ex)
        .await
        .map_db()
}

/// Age of the oldest unpublished event, in seconds. The single best signal
/// that the relay is wedged.
pub async fn oldest_pending_age_secs<'e, E>(ex: E) -> Result<Option<f64>>
where
    E: Executor<'e, Database = Postgres>,
{
    sqlx::query_scalar::<_, Option<f64>>(
        "SELECT EXTRACT(EPOCH FROM (now() - MIN(created_at)))::float8
           FROM outbox WHERE published_at IS NULL",
    )
    .fetch_one(ex)
    .await
    .map_db()
}

pub async fn prune_published<'e, E>(ex: E, older_than: DateTime<Utc>) -> Result<u64>
where
    E: Executor<'e, Database = Postgres>,
{
    let r = sqlx::query("DELETE FROM outbox WHERE published_at IS NOT NULL AND published_at < $1")
        .bind(older_than)
        .execute(ex)
        .await
        .map_db()?;
    Ok(r.rows_affected())
}

// ----------------------------------------------------------- consumer side ---

/// Claim an event for processing by `consumer`.
///
/// Returns `true` if this is the first time this consumer has seen the event.
/// Called inside the same transaction as the consumer's side effect, so the
/// claim and the effect commit together: a redelivery after a crash either sees
/// no claim (and reprocesses safely) or sees the claim (and skips).
pub async fn claim_event(
    tx: &mut Tx<'_>,
    consumer: &str,
    event_id: Uuid,
    event_type: &str,
) -> Result<bool> {
    let r = sqlx::query(
        r#"
        INSERT INTO processed_events (consumer, event_id, event_type)
        VALUES ($1, $2, $3)
        ON CONFLICT (consumer, event_id) DO NOTHING
        "#,
    )
    .bind(consumer)
    .bind(event_id)
    .bind(event_type)
    .execute(&mut **tx)
    .await
    .map_db()?;
    Ok(r.rows_affected() == 1)
}

pub async fn was_processed<'e, E>(ex: E, consumer: &str, event_id: Uuid) -> Result<bool>
where
    E: Executor<'e, Database = Postgres>,
{
    sqlx::query_scalar::<_, bool>(
        "SELECT EXISTS (SELECT 1 FROM processed_events WHERE consumer = $1 AND event_id = $2)",
    )
    .bind(consumer)
    .bind(event_id)
    .fetch_one(ex)
    .await
    .map_db()
}

pub async fn prune_processed<'e, E>(ex: E, older_than: DateTime<Utc>) -> Result<u64>
where
    E: Executor<'e, Database = Postgres>,
{
    let r = sqlx::query("DELETE FROM processed_events WHERE processed_at < $1")
        .bind(older_than)
        .execute(ex)
        .await
        .map_db()?;
    Ok(r.rows_affected())
}

// -------------------------------------------------------------- dead letter ---

pub struct DeadLetter<'a> {
    pub source_topic: &'a str,
    pub consumer: Option<&'a str>,
    pub event_id: Option<Uuid>,
    pub event_type: Option<&'a str>,
    pub payload: serde_json::Value,
    pub error: &'a str,
    pub attempts: i32,
    pub correlation_id: Option<&'a str>,
}

pub async fn record_dead_letter<'e, E>(ex: E, dl: DeadLetter<'_>) -> Result<Uuid>
where
    E: Executor<'e, Database = Postgres>,
{
    sqlx::query_scalar::<_, Uuid>(
        r#"
        INSERT INTO dead_letters
            (source_topic, consumer, event_id, event_type, payload, error, attempts, correlation_id)
        VALUES ($1, $2, $3, $4, $5, left($6, 2000), $7, $8)
        RETURNING id
        "#,
    )
    .bind(dl.source_topic)
    .bind(dl.consumer)
    .bind(dl.event_id)
    .bind(dl.event_type)
    .bind(dl.payload)
    .bind(dl.error)
    .bind(dl.attempts)
    .bind(dl.correlation_id)
    .fetch_one(ex)
    .await
    .map_db()
}

pub async fn dead_letter_count<'e, E>(ex: E) -> Result<i64>
where
    E: Executor<'e, Database = Postgres>,
{
    sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM dead_letters")
        .fetch_one(ex)
        .await
        .map_db()
}
