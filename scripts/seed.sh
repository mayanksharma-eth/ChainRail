#!/usr/bin/env bash
# Seed local development data: a user, a deposit-address pool, and hot-wallet
# custody funding.
#
# Deposit addresses are *not* derived by ChainRail (it holds no master key), so
# this script plays the role the custody system would: it exports addresses into
# the pool. The addresses below are well-known Hardhat test accounts.
set -euo pipefail
# ON_ERROR_STOP is set per-invocation below; -e makes a failed psql abort the script.

DB_URL="${TEST_DATABASE_URL:-${CHAINRAIL__DATABASE__URL:-postgres://chainrail:chainrail@127.0.0.1:55432/chainrail}}"
CHAIN="${CHAIN:-base-sepolia}"
API="${API:-http://127.0.0.1:8088}"

echo "==> seeding $CHAIN via $DB_URL"

# Hardhat accounts #1..#5. Public, worthless, and deliberately not the hot
# wallet (account #0), because sending to our own hot wallet is denied by policy.
psql "$DB_URL" -v ON_ERROR_STOP=1 -q <<SQL
-- Single quotes inside a string literal are doubled; double quotes would make
-- Postgres read the derivation path as a column identifier.
INSERT INTO deposit_address_pool (chain, address, derivation_path) VALUES
  ('$CHAIN', '0x70997970c51812dc3a010c7d01b50e0d17dc79c8', 'm/44''/60''/0''/0/1'),
  ('$CHAIN', '0x3c44cdddb6a900fa2b585dd299e03d12fa4293bc', 'm/44''/60''/0''/0/2'),
  ('$CHAIN', '0x90f79bf6eb2c4f870365e785982e1f101e93b906', 'm/44''/60''/0''/0/3'),
  ('$CHAIN', '0x15d34aaf54267db7d7c367839aaf71a00a2c6a65', 'm/44''/60''/0''/0/4'),
  ('$CHAIN', '0x9965507d1a55bcc2695c58ba16fb37d819b0a4dc', 'm/44''/60''/0''/0/5')
ON CONFLICT (chain, address) DO NOTHING;
SQL
echo "    deposit address pool topped up"

# Fund custody so withdrawals and gas have backing. In production this posting
# records a real on-chain transfer into the hot wallet; here it just establishes
# the opening balance.
psql "$DB_URL" -v ON_ERROR_STOP=1 -q <<SQL
DO \$\$
DECLARE
    v_asset   UUID;
    v_native  UUID;
    v_ltx     UUID;
    v_custody UUID;
    v_treasury UUID;
BEGIN
    SELECT id INTO v_asset  FROM assets WHERE chain = '$CHAIN' AND symbol = 'USDC';
    SELECT id INTO v_native FROM assets WHERE chain = '$CHAIN' AND contract_address IS NULL;
    IF v_asset IS NULL THEN
        RAISE NOTICE 'assets not registered yet -- start the API once, then re-run';
        RETURN;
    END IF;

    -- USDC custody: 1,000,000 USDC
    IF NOT EXISTS (SELECT 1 FROM ledger_transactions WHERE idempotency_key = 'custody_funding:seed-usdc') THEN
        INSERT INTO ledger_accounts (account_type, asset_id, normal_balance, allow_negative)
        VALUES ('exchange_custody', v_asset, 'debit', FALSE)
        ON CONFLICT (account_type, asset_id) WHERE owner_user_id IS NULL DO NOTHING;
        INSERT INTO ledger_accounts (account_type, asset_id, normal_balance, allow_negative)
        VALUES ('treasury', v_asset, 'credit', TRUE)
        ON CONFLICT (account_type, asset_id) WHERE owner_user_id IS NULL DO NOTHING;

        SELECT id INTO v_custody  FROM ledger_accounts WHERE account_type='exchange_custody' AND asset_id=v_asset AND owner_user_id IS NULL;
        SELECT id INTO v_treasury FROM ledger_accounts WHERE account_type='treasury'         AND asset_id=v_asset AND owner_user_id IS NULL;

        INSERT INTO ledger_transactions (kind, reference_type, idempotency_key, description)
        VALUES ('adjustment', 'manual', 'custody_funding:seed-usdc', 'seed: hot wallet funded from treasury')
        RETURNING id INTO v_ltx;
        INSERT INTO ledger_entries (ledger_transaction_id, account_id, asset_id, amount, direction) VALUES
          (v_ltx, LEAST(v_custody, v_treasury), v_asset, 1000000000000,
             CASE WHEN LEAST(v_custody, v_treasury) = v_custody THEN 'debit' ELSE 'credit' END),
          (v_ltx, GREATEST(v_custody, v_treasury), v_asset, 1000000000000,
             CASE WHEN GREATEST(v_custody, v_treasury) = v_custody THEN 'debit' ELSE 'credit' END);
        RAISE NOTICE 'funded USDC custody with 1,000,000';
    END IF;

    -- Native gas: 10 ETH
    IF v_native IS NOT NULL
       AND NOT EXISTS (SELECT 1 FROM ledger_transactions WHERE idempotency_key = 'custody_funding:seed-native') THEN
        INSERT INTO ledger_accounts (account_type, asset_id, normal_balance, allow_negative)
        VALUES ('exchange_custody', v_native, 'debit', FALSE)
        ON CONFLICT (account_type, asset_id) WHERE owner_user_id IS NULL DO NOTHING;
        INSERT INTO ledger_accounts (account_type, asset_id, normal_balance, allow_negative)
        VALUES ('treasury', v_native, 'credit', TRUE)
        ON CONFLICT (account_type, asset_id) WHERE owner_user_id IS NULL DO NOTHING;

        SELECT id INTO v_custody  FROM ledger_accounts WHERE account_type='exchange_custody' AND asset_id=v_native AND owner_user_id IS NULL;
        SELECT id INTO v_treasury FROM ledger_accounts WHERE account_type='treasury'         AND asset_id=v_native AND owner_user_id IS NULL;

        INSERT INTO ledger_transactions (kind, reference_type, idempotency_key, description)
        VALUES ('adjustment', 'manual', 'custody_funding:seed-native', 'seed: gas funded from treasury')
        RETURNING id INTO v_ltx;
        INSERT INTO ledger_entries (ledger_transaction_id, account_id, asset_id, amount, direction) VALUES
          (v_ltx, LEAST(v_custody, v_treasury), v_native, 10000000000000000000,
             CASE WHEN LEAST(v_custody, v_treasury) = v_custody THEN 'debit' ELSE 'credit' END),
          (v_ltx, GREATEST(v_custody, v_treasury), v_native, 10000000000000000000,
             CASE WHEN GREATEST(v_custody, v_treasury) = v_custody THEN 'debit' ELSE 'credit' END);
        RAISE NOTICE 'funded native custody with 10 ETH';
    END IF;
END
\$\$;
SQL

echo "==> creating a demo user via the API"
if USER_JSON=$(curl -fsS -X POST "$API/v1/users" \
        -H 'content-type: application/json' \
        -d '{"external_id":"demo-user-1"}' 2>/dev/null); then
    USER_ID=$(printf '%s' "$USER_JSON" | sed -n 's/.*"id":"\([^"]*\)".*/\1/p')
    echo "    user_id=$USER_ID"
    ADDR_JSON=$(curl -fsS -X POST "$API/v1/deposit-addresses" \
        -H 'content-type: application/json' \
        -d "{\"user_id\":\"$USER_ID\",\"chain\":\"$CHAIN\"}")
    echo "    deposit address: $(printf '%s' "$ADDR_JSON" | sed -n 's/.*"address":"\([^"]*\)".*/\1/p')"
    echo
    echo "Next:"
    echo "  curl -s $API/v1/balances/$USER_ID | jq"
    echo "  curl -s '$API/v1/deposits?user_id=$USER_ID' | jq"
else
    echo "    API not reachable at $API -- start it with 'docker compose up -d api', then re-run"
fi
echo "==> seed complete"
