-- Deposit lifecycle.
--
--   observed -> confirming -> confirmed -> credited
--        \-> reorged            \-> failed
--
-- A deposit is the *accounting* view of an observed on-chain transfer; the
-- transfer itself lives in blockchain_transactions.

CREATE TABLE deposits (
    id                        UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id                   UUID           NOT NULL REFERENCES users (id) ON DELETE RESTRICT,
    blockchain_transaction_id UUID           NOT NULL
                                  REFERENCES blockchain_transactions (id) ON DELETE RESTRICT,
    asset_id                  UUID           NOT NULL REFERENCES assets (id) ON DELETE RESTRICT,
    amount_raw                NUMERIC(78, 0) NOT NULL,
    confirmations             BIGINT         NOT NULL DEFAULT 0,
    status                    TEXT           NOT NULL DEFAULT 'observed',
    -- Set exactly when the credit posting succeeds.
    ledger_transaction_id     UUID           REFERENCES ledger_transactions (id),
    reversal_ledger_transaction_id UUID      REFERENCES ledger_transactions (id),
    failure_reason            TEXT,
    credited_at               TIMESTAMPTZ,
    created_at                TIMESTAMPTZ    NOT NULL DEFAULT now(),
    updated_at                TIMESTAMPTZ    NOT NULL DEFAULT now(),

    -- One deposit per on-chain transfer. Combined with the natural key on
    -- blockchain_transactions, this makes double-crediting structurally
    -- impossible rather than merely unlikely.
    CONSTRAINT deposits_blockchain_transaction_key UNIQUE (blockchain_transaction_id),
    CONSTRAINT deposits_amount_positive CHECK (amount_raw > 0),
    CONSTRAINT deposits_status_check CHECK (
        status IN ('observed', 'confirming', 'confirmed', 'credited', 'reorged', 'failed')
    ),
    -- A deposit is credited if and only if it points at its credit posting.
    CONSTRAINT deposits_credited_consistency CHECK (
        (status = 'credited') = (ledger_transaction_id IS NOT NULL AND credited_at IS NOT NULL)
        OR (status = 'reorged' AND ledger_transaction_id IS NOT NULL)
    ),
    CONSTRAINT deposits_confirmations_positive CHECK (confirmations >= 0)
);

CREATE INDEX deposits_user_idx ON deposits (user_id, created_at DESC);
CREATE INDEX deposits_status_idx ON deposits (status, created_at);
-- Drives the confirmation engine's "what still needs confirming" scan.
CREATE INDEX deposits_pending_idx ON deposits (status)
    WHERE status IN ('observed', 'confirming', 'confirmed');

CREATE OR REPLACE FUNCTION touch_updated_at() RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at := now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER deposits_touch_updated_at
    BEFORE UPDATE ON deposits
    FOR EACH ROW EXECUTE FUNCTION touch_updated_at();
