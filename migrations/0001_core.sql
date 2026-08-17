-- Core identity and asset reference data.
--
-- Convention used throughout the schema:
--   * money is NUMERIC(78, 0) -- exact integers in the asset's smallest unit.
--     78 digits covers the full EVM uint256 range. Never FLOAT/DOUBLE.
--   * chain addresses and hashes are stored lowercase so uniqueness is exact;
--     the API re-derives the EIP-55 checksummed form for display.
--   * every timestamp is TIMESTAMPTZ in UTC.

CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE users (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    -- Caller-supplied stable identity. Unique so that user creation is
    -- idempotent: retrying a create with the same external_id conflicts
    -- rather than producing a second account.
    external_id TEXT        NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT users_external_id_key UNIQUE (external_id),
    CONSTRAINT users_external_id_len CHECK (length(external_id) BETWEEN 1 AND 128)
);

CREATE TABLE assets (
    id               UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    symbol           TEXT     NOT NULL,
    chain            TEXT     NOT NULL,
    -- NULL means the chain's native asset (ETH, BNB, ...).
    contract_address TEXT,
    decimals         SMALLINT NOT NULL,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT assets_chain_symbol_key UNIQUE (chain, symbol),
    CONSTRAINT assets_decimals_range CHECK (decimals BETWEEN 0 AND 36),
    CONSTRAINT assets_contract_lowercase CHECK (
        contract_address IS NULL OR contract_address = lower(contract_address)
    )
);

-- One contract may back only one asset row per chain. A partial unique index
-- (rather than a table constraint) so that multiple native assets -- which have
-- NULL contract_address -- do not collide.
CREATE UNIQUE INDEX assets_chain_contract_key
    ON assets (chain, contract_address)
    WHERE contract_address IS NOT NULL;

-- Exactly one native asset per chain.
CREATE UNIQUE INDEX assets_chain_native_key
    ON assets (chain)
    WHERE contract_address IS NULL;

CREATE TABLE deposit_addresses (
    id         UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id    UUID        NOT NULL REFERENCES users (id) ON DELETE RESTRICT,
    chain      TEXT        NOT NULL,
    address    TEXT        NOT NULL,
    -- Recorded for operational recovery; never contains key material.
    derivation_path TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),

    -- An address must map to at most one user, or deposit attribution is
    -- ambiguous and funds can be credited to the wrong account.
    CONSTRAINT deposit_addresses_chain_address_key UNIQUE (chain, address),
    CONSTRAINT deposit_addresses_lowercase CHECK (address = lower(address)),
    -- One deposit address per (user, chain) in v0.1. Lifting this later only
    -- requires dropping this constraint; nothing else assumes it.
    CONSTRAINT deposit_addresses_user_chain_key UNIQUE (user_id, chain)
);

CREATE INDEX deposit_addresses_user_idx ON deposit_addresses (user_id);
