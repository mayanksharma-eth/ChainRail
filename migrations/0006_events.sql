-- Event plumbing: transactional outbox (producer side) and a processed-event
-- log (consumer side).
--
-- Together these turn Kafka's at-least-once delivery into effectively
-- exactly-once *processing*:
--   * outbox      -- an event is published if and only if the database change
--                    that caused it committed. No dual-write window.
--   * processed_events -- a consumer records the event_id it handled in the
--                    same transaction as its side effect, so redelivery is a
--                    cheap no-op.

CREATE TABLE outbox (
    id             UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    -- The envelope's event_id. UNIQUE so that a retried business operation
    -- carrying a deterministic event id cannot enqueue the same event twice.
    event_id       UUID        NOT NULL,
    topic          TEXT        NOT NULL,
    partition_key  TEXT        NOT NULL,
    payload        JSONB       NOT NULL,
    correlation_id TEXT,

    published_at   TIMESTAMPTZ,
    attempts       INTEGER     NOT NULL DEFAULT 0,
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_error     TEXT,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT outbox_event_id_key UNIQUE (event_id),
    CONSTRAINT outbox_attempts_positive CHECK (attempts >= 0)
);

-- The relay's hot path: unpublished rows that are due, oldest first.
CREATE INDEX outbox_pending_idx ON outbox (next_attempt_at, created_at)
    WHERE published_at IS NULL;

CREATE TABLE processed_events (
    consumer     TEXT        NOT NULL,
    event_id     UUID        NOT NULL,
    event_type   TEXT        NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT now(),

    -- Scoped per consumer: two independent consumers must each get the event
    -- once, but neither may process it twice.
    PRIMARY KEY (consumer, event_id)
);

CREATE INDEX processed_events_processed_at_idx ON processed_events (processed_at);

-- Events that exhausted their retry budget. Retained for operator triage;
-- nothing reads this table automatically.
CREATE TABLE dead_letters (
    id             UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    source_topic   TEXT        NOT NULL,
    consumer       TEXT,
    event_id       UUID,
    event_type     TEXT,
    payload        JSONB       NOT NULL,
    error          TEXT        NOT NULL,
    attempts       INTEGER     NOT NULL DEFAULT 0,
    correlation_id TEXT,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX dead_letters_created_idx ON dead_letters (created_at DESC);
CREATE INDEX dead_letters_topic_idx ON dead_letters (source_topic, created_at DESC);
