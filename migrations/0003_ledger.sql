-- Append-only double-entry ledger.
--
-- ACCOUNTING CONVENTION (also documented in docs/ledger.md):
--   Every account has a `normal_balance` of 'debit' or 'credit'.
--     debit-normal  : assets, expenses          (custody holdings, gas spent)
--     credit-normal : liabilities, equity, income (what we owe users)
--   `balance` is stored in the account's *natural* sign, i.e. it increases when
--   an entry matches the account's normal balance. A user liability account
--   therefore reads positive when the user has funds, which is what the API
--   surfaces.
--
--   Every ledger_transaction must satisfy sum(debits) == sum(credits). This is
--   enforced by a deferred constraint trigger, so an unbalanced transaction
--   cannot be committed even if application code is wrong.
--
-- IMMUTABILITY: ledger_transactions and ledger_entries are append-only. UPDATE
-- and DELETE are blocked by triggers. Corrections are made by posting a new,
-- compensating transaction -- never by rewriting history.

CREATE TABLE ledger_accounts (
    id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    account_type  TEXT           NOT NULL,
    -- Set for per-user accounts, NULL for system accounts.
    owner_user_id UUID           REFERENCES users (id) ON DELETE RESTRICT,
    asset_id      UUID           NOT NULL REFERENCES assets (id) ON DELETE RESTRICT,
    normal_balance TEXT          NOT NULL,
    balance       NUMERIC(78, 0) NOT NULL DEFAULT 0,
    -- Clearing and deficit accounts legitimately hold a negative natural
    -- balance during settlement; user spendable balances never may.
    allow_negative BOOLEAN       NOT NULL DEFAULT FALSE,
    created_at    TIMESTAMPTZ    NOT NULL DEFAULT now(),
    updated_at    TIMESTAMPTZ    NOT NULL DEFAULT now(),

    CONSTRAINT ledger_accounts_type_check CHECK (account_type IN (
        'user_available',       -- liability: spendable user funds
        'user_reserved',        -- liability: user funds locked for a withdrawal
        'user_deficit',         -- asset: receivable created by a post-credit reorg
        'exchange_custody',     -- asset: crypto ChainRail actually holds
        'withdrawal_clearing',  -- asset: value in flight between broadcast and confirmation
        'network_fee',          -- expense: gas paid to the network
        'treasury'              -- equity: exchange-owned capital backing custody
    )),
    CONSTRAINT ledger_accounts_normal_check CHECK (normal_balance IN ('debit', 'credit')),
    -- User-scoped types must have an owner; system types must not.
    CONSTRAINT ledger_accounts_ownership_check CHECK (
        (account_type IN ('user_available', 'user_reserved', 'user_deficit'))
        = (owner_user_id IS NOT NULL)
    ),
    -- THE non-negative-balance invariant. This is what actually stops a race
    -- between concurrent withdrawals from overdrawing an account: the losing
    -- transaction fails here, in the database, not in application logic.
    CONSTRAINT ledger_accounts_non_negative CHECK (allow_negative OR balance >= 0)
);

-- One account per (type, owner, asset). Account resolution is an idempotent
-- upsert against this constraint.
CREATE UNIQUE INDEX ledger_accounts_user_key
    ON ledger_accounts (account_type, owner_user_id, asset_id)
    WHERE owner_user_id IS NOT NULL;

CREATE UNIQUE INDEX ledger_accounts_system_key
    ON ledger_accounts (account_type, asset_id)
    WHERE owner_user_id IS NULL;

CREATE INDEX ledger_accounts_owner_idx ON ledger_accounts (owner_user_id, asset_id);

-- An immutable financial event.
CREATE TABLE ledger_transactions (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    kind            TEXT        NOT NULL,
    -- What caused this posting, for reconciliation and audit.
    reference_type  TEXT        NOT NULL,
    reference_id    UUID,
    -- Idempotency: re-posting the same logical event is a no-op that returns
    -- the original transaction. Enforced by the database, not by a cache.
    idempotency_key TEXT        NOT NULL,
    correlation_id  TEXT,
    description     TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT ledger_transactions_idempotency_key UNIQUE (idempotency_key),
    CONSTRAINT ledger_transactions_kind_check CHECK (kind IN (
        'deposit_credit',
        'deposit_reversal',
        'withdrawal_reserve',
        'withdrawal_release',
        'withdrawal_broadcast',
        'withdrawal_settle',
        'withdrawal_broadcast_reversal',
        'network_fee',
        'adjustment'
    )),
    CONSTRAINT ledger_transactions_reference_check CHECK (
        reference_type IN ('deposit', 'withdrawal', 'manual', 'reorg')
    )
);

CREATE INDEX ledger_transactions_reference_idx
    ON ledger_transactions (reference_type, reference_id);
CREATE INDEX ledger_transactions_created_idx ON ledger_transactions (created_at DESC);

CREATE TABLE ledger_entries (
    id                     UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    ledger_transaction_id  UUID           NOT NULL
                               REFERENCES ledger_transactions (id) ON DELETE RESTRICT,
    account_id             UUID           NOT NULL
                               REFERENCES ledger_accounts (id) ON DELETE RESTRICT,
    asset_id               UUID           NOT NULL REFERENCES assets (id) ON DELETE RESTRICT,
    -- Always strictly positive; `direction` carries the sign. Storing signed
    -- amounts would make "sum(debits) == sum(credits)" unverifiable.
    amount                 NUMERIC(78, 0) NOT NULL,
    direction              TEXT           NOT NULL,
    -- Running balance of the account *after* this entry, captured by the
    -- balance trigger. Makes statement reconstruction and audit cheap.
    balance_after          NUMERIC(78, 0) NOT NULL,
    created_at             TIMESTAMPTZ    NOT NULL DEFAULT now(),

    CONSTRAINT ledger_entries_amount_positive CHECK (amount > 0),
    CONSTRAINT ledger_entries_direction_check CHECK (direction IN ('debit', 'credit'))
);

CREATE INDEX ledger_entries_transaction_idx ON ledger_entries (ledger_transaction_id);
CREATE INDEX ledger_entries_account_idx ON ledger_entries (account_id, created_at DESC);
CREATE INDEX ledger_entries_asset_idx ON ledger_entries (asset_id);

--------------------------------------------------------------------------------
-- Invariant enforcement
--------------------------------------------------------------------------------

-- 1. Entries mutate account balances. Doing this in a trigger rather than in
--    application code means no code path can insert an entry without moving the
--    balance. The UPDATE also takes a row lock, which serialises concurrent
--    postings against the same account.
--
--    DEADLOCK AVOIDANCE: callers must insert a transaction's entries ordered by
--    account_id, so all writers acquire account locks in the same order.
CREATE OR REPLACE FUNCTION ledger_apply_entry() RETURNS TRIGGER AS $$
DECLARE
    acct   ledger_accounts%ROWTYPE;
    delta  NUMERIC(78, 0);
BEGIN
    SELECT * INTO acct FROM ledger_accounts WHERE id = NEW.account_id FOR UPDATE;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'ledger account % does not exist', NEW.account_id
            USING ERRCODE = 'foreign_key_violation';
    END IF;

    IF acct.asset_id <> NEW.asset_id THEN
        RAISE EXCEPTION 'entry asset % does not match account asset % (account %)',
            NEW.asset_id, acct.asset_id, NEW.account_id
            USING ERRCODE = 'check_violation';
    END IF;

    -- Natural sign: an entry matching the account's normal balance increases it.
    IF NEW.direction = acct.normal_balance THEN
        delta := NEW.amount;
    ELSE
        delta := -NEW.amount;
    END IF;

    UPDATE ledger_accounts
       SET balance = balance + delta,
           updated_at = now()
     WHERE id = NEW.account_id
    RETURNING balance INTO NEW.balance_after;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER ledger_entries_apply
    BEFORE INSERT ON ledger_entries
    FOR EACH ROW EXECUTE FUNCTION ledger_apply_entry();

-- 2. Every transaction must balance. Deferred to COMMIT so that a transaction
--    is legitimately unbalanced only *within* the posting, never once durable.
CREATE OR REPLACE FUNCTION ledger_assert_balanced() RETURNS TRIGGER AS $$
DECLARE
    net       NUMERIC(78, 0);
    entry_cnt INTEGER;
BEGIN
    SELECT COALESCE(SUM(CASE WHEN direction = 'debit' THEN amount ELSE -amount END), 0),
           COUNT(*)
      INTO net, entry_cnt
      FROM ledger_entries
     WHERE ledger_transaction_id = NEW.ledger_transaction_id;

    IF entry_cnt < 2 THEN
        RAISE EXCEPTION 'ledger transaction % has % entries; double-entry requires at least 2',
            NEW.ledger_transaction_id, entry_cnt
            USING ERRCODE = 'check_violation';
    END IF;

    IF net <> 0 THEN
        RAISE EXCEPTION 'ledger transaction % does not balance: sum(debits) - sum(credits) = %',
            NEW.ledger_transaction_id, net
            USING ERRCODE = 'check_violation';
    END IF;

    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE CONSTRAINT TRIGGER ledger_entries_balanced
    AFTER INSERT ON ledger_entries
    DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW EXECUTE FUNCTION ledger_assert_balanced();

-- 3. Append-only. History is corrected by compensating entries, never rewritten.
CREATE OR REPLACE FUNCTION ledger_reject_mutation() RETURNS TRIGGER AS $$
BEGIN
    RAISE EXCEPTION 'ledger table %.% is append-only; post a compensating transaction instead',
        TG_TABLE_SCHEMA, TG_TABLE_NAME
        USING ERRCODE = 'restrict_violation';
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER ledger_entries_immutable
    BEFORE UPDATE OR DELETE ON ledger_entries
    FOR EACH ROW EXECUTE FUNCTION ledger_reject_mutation();

CREATE TRIGGER ledger_transactions_immutable
    BEFORE UPDATE OR DELETE ON ledger_transactions
    FOR EACH ROW EXECUTE FUNCTION ledger_reject_mutation();

--------------------------------------------------------------------------------
-- Reconciliation view: the authoritative balance is always the entries, and the
-- cached `ledger_accounts.balance` must equal it. verify_ledger_integrity()
-- compares the two; any row here with a mismatch is a bug or corruption.
--------------------------------------------------------------------------------
CREATE VIEW ledger_account_balances AS
SELECT a.id AS account_id,
       a.account_type,
       a.owner_user_id,
       a.asset_id,
       a.normal_balance,
       a.balance AS cached_balance,
       COALESCE(SUM(
           CASE WHEN e.direction = a.normal_balance THEN e.amount ELSE -e.amount END
       ), 0) AS derived_balance
  FROM ledger_accounts a
  LEFT JOIN ledger_entries e ON e.account_id = a.id
 GROUP BY a.id;
