-- Chain observation state: block lineage (for reorg detection), the watcher
-- cursor, and observed on-chain value transfers.

-- Block lineage. Keeping parent_hash is what makes reorg detection possible:
-- a new block whose parent_hash does not match the stored canonical block at
-- height-1 proves the chain reorganised.
CREATE TABLE chain_blocks (
    chain       TEXT        NOT NULL,
    hash        TEXT        NOT NULL,
    height      BIGINT      NOT NULL,
    parent_hash TEXT        NOT NULL,
    status      TEXT        NOT NULL DEFAULT 'canonical',
    observed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    orphaned_at TIMESTAMPTZ,

    PRIMARY KEY (chain, hash),
    CONSTRAINT chain_blocks_status_check CHECK (status IN ('canonical', 'orphaned')),
    CONSTRAINT chain_blocks_height_positive CHECK (height >= 0),
    CONSTRAINT chain_blocks_lowercase CHECK (
        hash = lower(hash) AND parent_hash = lower(parent_hash)
    ),
    CONSTRAINT chain_blocks_orphan_time CHECK (
        (status = 'orphaned') = (orphaned_at IS NOT NULL)
    )
);

-- THE central reorg invariant: at most one canonical block per height.
-- Promoting a replacement block therefore *requires* orphaning the incumbent
-- in the same transaction; the database will not allow a forked view to persist.
CREATE UNIQUE INDEX chain_blocks_canonical_height_key
    ON chain_blocks (chain, height)
    WHERE status = 'canonical';

CREATE INDEX chain_blocks_height_idx ON chain_blocks (chain, height DESC);

-- Watcher progress. One row per chain; the watcher advances it only after the
-- block's contents are durably persisted, so a crash re-processes the block
-- rather than skipping it (at-least-once, made safe by idempotent inserts).
CREATE TABLE chain_cursors (
    chain                TEXT PRIMARY KEY,
    last_processed_height BIGINT      NOT NULL,
    last_processed_hash   TEXT        NOT NULL,
    head_height           BIGINT      NOT NULL DEFAULT 0,
    updated_at            TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT chain_cursors_height_positive CHECK (last_processed_height >= 0)
);

-- An observed on-chain value transfer into a tracked address.
--
-- Uniqueness is (chain, tx_hash, log_index). log_index = -1 encodes a native
-- (non-log) transfer so the same constraint covers both cases without a
-- nullable column that would silently disable the uniqueness check.
CREATE TABLE blockchain_transactions (
    id           UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    chain        TEXT           NOT NULL,
    tx_hash      TEXT           NOT NULL,
    log_index    BIGINT         NOT NULL DEFAULT -1,
    block_number BIGINT         NOT NULL,
    block_hash   TEXT           NOT NULL,
    from_address TEXT           NOT NULL,
    to_address   TEXT           NOT NULL,
    asset_id     UUID           NOT NULL REFERENCES assets (id) ON DELETE RESTRICT,
    amount_raw   NUMERIC(78, 0) NOT NULL,
    status       TEXT           NOT NULL DEFAULT 'observed',
    observed_at  TIMESTAMPTZ    NOT NULL DEFAULT now(),

    -- The single most important idempotency constraint in the deposit path:
    -- replaying a block, a duplicate Kafka delivery, or two watcher replicas
    -- racing all collapse onto this one row.
    CONSTRAINT blockchain_transactions_natural_key UNIQUE (chain, tx_hash, log_index),
    CONSTRAINT blockchain_transactions_amount_positive CHECK (amount_raw > 0),
    CONSTRAINT blockchain_transactions_status_check CHECK (
        status IN ('observed', 'confirmed', 'orphaned', 'failed')
    ),
    CONSTRAINT blockchain_transactions_lowercase CHECK (
        tx_hash = lower(tx_hash)
        AND block_hash = lower(block_hash)
        AND from_address = lower(from_address)
        AND to_address = lower(to_address)
    ),
    FOREIGN KEY (chain, block_hash) REFERENCES chain_blocks (chain, hash) ON DELETE RESTRICT
);

CREATE INDEX blockchain_transactions_block_idx
    ON blockchain_transactions (chain, block_number DESC);
CREATE INDEX blockchain_transactions_hash_idx ON blockchain_transactions (tx_hash);
CREATE INDEX blockchain_transactions_to_idx ON blockchain_transactions (chain, to_address);
-- Supports "which observed transfers belong to blocks that just got orphaned".
CREATE INDEX blockchain_transactions_block_hash_idx
    ON blockchain_transactions (chain, block_hash);
