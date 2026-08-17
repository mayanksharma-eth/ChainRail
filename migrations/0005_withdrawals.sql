-- Withdrawal lifecycle.
--
--   requested -> validated -> approved -> signing -> broadcast -> confirming -> completed
--        \___________\___________\___________\-> failed
--        \-> cancelled  (only before funds leave)
--
-- Legal transitions are enforced twice: once in Rust (WithdrawalStatus::can_transition_to)
-- and once here by a trigger, so a buggy or out-of-date worker cannot corrupt state.

CREATE TABLE withdrawals (
    id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id             UUID           NOT NULL REFERENCES users (id) ON DELETE RESTRICT,
    asset_id            UUID           NOT NULL REFERENCES assets (id) ON DELETE RESTRICT,
    chain               TEXT           NOT NULL,
    destination_address TEXT           NOT NULL,
    amount_raw          NUMERIC(78, 0) NOT NULL,
    status              TEXT           NOT NULL DEFAULT 'requested',

    -- Client-supplied idempotency key. UNIQUE per user, so a retried POST
    -- returns the original withdrawal instead of sending funds twice.
    idempotency_key     TEXT           NOT NULL,
    -- Hash of the request body; a reused key with a different body is a client
    -- bug and must be rejected loudly rather than silently returning the old one.
    request_fingerprint TEXT           NOT NULL,

    tx_hash             TEXT,
    nonce               BIGINT,
    gas_limit           BIGINT,
    max_fee_per_gas     NUMERIC(78, 0),
    max_priority_fee_per_gas NUMERIC(78, 0),
    fee_paid_raw        NUMERIC(78, 0),
    block_number        BIGINT,
    confirmations       BIGINT         NOT NULL DEFAULT 0,
    broadcast_attempts  INTEGER        NOT NULL DEFAULT 0,

    reserve_ledger_transaction_id  UUID REFERENCES ledger_transactions (id),
    settle_ledger_transaction_id   UUID REFERENCES ledger_transactions (id),

    failure_code        TEXT,
    failure_reason      TEXT,
    correlation_id      TEXT,
    approved_at         TIMESTAMPTZ,
    broadcast_at        TIMESTAMPTZ,
    completed_at        TIMESTAMPTZ,
    created_at          TIMESTAMPTZ    NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ    NOT NULL DEFAULT now(),

    CONSTRAINT withdrawals_idempotency_key UNIQUE (user_id, idempotency_key),
    CONSTRAINT withdrawals_amount_positive CHECK (amount_raw > 0),
    CONSTRAINT withdrawals_status_check CHECK (status IN (
        'requested', 'validated', 'approved', 'signing',
        'broadcast', 'confirming', 'completed', 'failed', 'cancelled'
    )),
    CONSTRAINT withdrawals_destination_lowercase CHECK (
        destination_address = lower(destination_address)
    ),
    CONSTRAINT withdrawals_tx_hash_lowercase CHECK (tx_hash IS NULL OR tx_hash = lower(tx_hash)),
    -- Once broadcast, a transaction hash must exist. Without this a withdrawal
    -- could reach a terminal state with no on-chain evidence to reconcile against.
    CONSTRAINT withdrawals_broadcast_has_hash CHECK (
        status NOT IN ('broadcast', 'confirming', 'completed') OR tx_hash IS NOT NULL
    ),
    CONSTRAINT withdrawals_completed_consistency CHECK (
        (status = 'completed') = (settle_ledger_transaction_id IS NOT NULL AND completed_at IS NOT NULL)
    ),
    CONSTRAINT withdrawals_failure_consistency CHECK (
        status <> 'failed' OR failure_code IS NOT NULL
    ),
    CONSTRAINT withdrawals_key_len CHECK (length(idempotency_key) BETWEEN 8 AND 128)
);

CREATE INDEX withdrawals_user_idx ON withdrawals (user_id, created_at DESC);
CREATE INDEX withdrawals_status_idx ON withdrawals (status, created_at);
CREATE INDEX withdrawals_pending_idx ON withdrawals (status, chain)
    WHERE status IN ('approved', 'signing', 'broadcast', 'confirming');
CREATE INDEX withdrawals_tx_hash_idx ON withdrawals (chain, tx_hash) WHERE tx_hash IS NOT NULL;
-- Supports the per-user daily velocity check in the risk engine.
CREATE INDEX withdrawals_user_daily_idx ON withdrawals (user_id, asset_id, created_at)
    WHERE status <> 'failed' AND status <> 'cancelled';

-- Nonce allocation per hot wallet. Serialising nonce issuance is what prevents
-- two withdrawals from being signed with the same nonce, which would strand one
-- of them permanently.
CREATE TABLE chain_nonces (
    chain      TEXT   NOT NULL,
    address    TEXT   NOT NULL,
    next_nonce BIGINT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),

    PRIMARY KEY (chain, address),
    CONSTRAINT chain_nonces_positive CHECK (next_nonce >= 0),
    CONSTRAINT chain_nonces_lowercase CHECK (address = lower(address))
);

-- State machine enforcement.
CREATE OR REPLACE FUNCTION withdrawals_guard_transition() RETURNS TRIGGER AS $$
DECLARE
    allowed TEXT[];
BEGIN
    IF NEW.status = OLD.status THEN
        RETURN NEW;
    END IF;

    allowed := CASE OLD.status
        WHEN 'requested'  THEN ARRAY['validated', 'failed', 'cancelled']
        WHEN 'validated'  THEN ARRAY['approved', 'failed', 'cancelled']
        WHEN 'approved'   THEN ARRAY['signing', 'failed', 'cancelled']
        WHEN 'signing'    THEN ARRAY['broadcast', 'failed']
        -- Note: no path from 'broadcast' back to 'cancelled'. Once a signed
        -- transaction is on the wire we cannot un-send it; the only honest
        -- outcomes are confirmation or an observed on-chain failure.
        WHEN 'broadcast'  THEN ARRAY['confirming', 'completed', 'failed']
        WHEN 'confirming' THEN ARRAY['completed', 'failed']
        ELSE ARRAY[]::TEXT[]   -- completed / failed / cancelled are terminal
    END;

    IF NOT (NEW.status = ANY (allowed)) THEN
        RAISE EXCEPTION 'illegal withdrawal transition % -> % (withdrawal %)',
            OLD.status, NEW.status, NEW.id
            USING ERRCODE = 'check_violation';
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER withdrawals_guard_transition
    BEFORE UPDATE OF status ON withdrawals
    FOR EACH ROW EXECUTE FUNCTION withdrawals_guard_transition();

CREATE TRIGGER withdrawals_touch_updated_at
    BEFORE UPDATE ON withdrawals
    FOR EACH ROW EXECUTE FUNCTION touch_updated_at();
