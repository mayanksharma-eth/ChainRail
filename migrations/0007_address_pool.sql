-- Pre-provisioned deposit addresses.
--
-- ChainRail does not derive deposit addresses, because deriving them requires
-- holding a master key in the application process -- exactly what the threat
-- model rules out (docs/threat-model.md#key-custody). Instead the custody
-- system (HSM / KMS / MPC provider) generates addresses and exports the public
-- side into this pool; ChainRail assigns from it.
--
-- This is how it works with real custody, and it keeps the API usable without
-- pretending to own key material.

CREATE TABLE deposit_address_pool (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    chain           TEXT        NOT NULL,
    address         TEXT        NOT NULL,
    -- Recorded so an operator can locate the key in the custody system. Never
    -- key material itself.
    derivation_path TEXT,
    -- Set when the address is handed to a user. A pool row is claimed exactly
    -- once, enforced by the partial index below.
    assigned_to     UUID        REFERENCES users (id) ON DELETE RESTRICT,
    assigned_at     TIMESTAMPTZ,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT deposit_address_pool_key UNIQUE (chain, address),
    CONSTRAINT deposit_address_pool_lowercase CHECK (address = lower(address)),
    CONSTRAINT deposit_address_pool_assignment CHECK (
        (assigned_to IS NULL) = (assigned_at IS NULL)
    )
);

-- Drives "give me the next free address on this chain".
CREATE INDEX deposit_address_pool_free_idx ON deposit_address_pool (chain, created_at)
    WHERE assigned_to IS NULL;

-- An address may be assigned to at most one user, ever. Belt and braces
-- alongside the FK in deposit_addresses.
CREATE UNIQUE INDEX deposit_address_pool_assigned_once_idx
    ON deposit_address_pool (chain, address)
    WHERE assigned_to IS NOT NULL;

-- How full the pool is, for the operational metric that warns before it runs dry.
CREATE VIEW deposit_address_pool_stats AS
SELECT chain,
       COUNT(*) FILTER (WHERE assigned_to IS NULL) AS available,
       COUNT(*) FILTER (WHERE assigned_to IS NOT NULL) AS assigned
  FROM deposit_address_pool
 GROUP BY chain;
