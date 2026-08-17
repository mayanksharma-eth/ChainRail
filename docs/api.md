# API

Base URL in local docker compose: `http://localhost:8088`
Machine-readable spec: `GET /openapi.json`

## Conventions

**Money is always a decimal string of raw units** — the asset's smallest
indivisible unit (1 USDC = `"1000000"`, 1 ETH = `"1000000000000000000"`). JSON
numbers are never used for money: above 2^53 an IEEE-754 double silently loses
precision, and a rounding error on a balance is unacceptable. A JSON float in an
amount field is rejected with 400.

Responses also include a `*_formatted` field for display. It is derived, never
authoritative.

**Errors share one shape.** Branch on `code`, never on `message`.

```json
{
  "error": {
    "code": "insufficient_balance",
    "message": "insufficient balance: available 100, requested 250",
    "request_id": "01936f2a-...",
    "retryable": false,
    "details": { "available": "100", "requested": "250" }
  }
}
```

`retryable` tells you whether resubmitting the identical request may succeed.
Server-side errors return a generic `message` — the detail is in the logs, keyed
by `request_id`, because a database error can name schema internals and an RPC
error can contain a provider API key.

**Status codes**

| Code | Meaning |
|---|---|
| 200 | Success, or an idempotent replay that created nothing |
| 201 | Created |
| 400 | Malformed request: bad address checksum, invalid hash, float amount, unknown chain/asset |
| 401 | Missing or invalid credentials |
| 404 | Not found |
| 409 | Valid request, conflicting state: insufficient balance, idempotency key reused with a different body, illegal state transition |
| 422 | Understood and consistent, but refused by policy |
| 429 | Rate limited |
| 500 | Internal error |
| 503 | A dependency is unavailable (also: address pool exhausted) |
| 504 | Request timed out |

The 409/422 split matters: 409 is "your account state does not permit this", 422
is "a policy rule forbids this", with the specific rule in
`error.details.policy`.

**Headers.** Send `X-Request-Id` and/or `X-Correlation-Id` to thread your own
identifiers through ChainRail's logs and events; both are sanitised and bounded.
`X-Request-Id` is always echoed on the response.

**Authentication.** `Authorization: Bearer <token>` on `/v1/*` when
`http.api_token` is configured (mandatory outside local/test/ci). `/health`,
`/ready` and `/metrics` are always open so a load balancer can probe them.

> The token carries **no user identity** — any holder can act for any `user_id`.
> This is a documented gap, not a design choice. See
> `threat-model.md#4-malicious-withdrawal-request`.

**Pagination is keyset-based.** Pass `after` and `after_id` from the previous
page's `next_cursor`. Offset pagination degrades on large accounts and shifts rows
under the client as new records arrive. `limit` is clamped to `max_page_size`.

---

## Operational endpoints

### `GET /health`

Liveness. 200 whenever the process is up.

```json
{
  "status": "ok",
  "service": "chainrail",
  "version": "0.1.0",
  "environment": "local",
  "uptime_seconds": 412,
  "signer_backend": "local_development",
  "signer_production_grade": false
}
```

`signer_production_grade` is surfaced deliberately: no shipped signer backend
returns `true`, so no dashboard can mistake this for real custody.

### `GET /ready`

Readiness. 503 **only** when Postgres is unreachable. Kafka and RPC outages are
reported but do not fail readiness, because balance and history reads remain
correct without them — a provider incident must not cascade into a full API
outage.

```json
{
  "ready": true,
  "database": { "ok": true, "required": true },
  "event_bus": { "ok": true, "required": false },
  "chains": [{
    "chain": "base-sepolia",
    "rpc_available": true,
    "endpoints": [{
      "name": "base-public",
      "breaker": "closed",
      "consecutive_failures": 0,
      "total_requests": 1284,
      "failure_rate": 0.004,
      "ewma_latency_ms": 187,
      "seconds_since_success": 0,
      "available_permits": 16
    }]
  }]
}
```

### `GET /metrics`

Prometheus text exposition. Also served on a dedicated port
(`observability.metrics_bind`, default `:9090`) so scraping is not exposed on the
public listener.

### `GET /internal/ledger-integrity`

Full ledger verification. Returns **500** when any invariant is violated, so
monitoring alerts without parsing the body. Scans every entry — not for a
request path.

---

## Users

### `POST /v1/users`

Idempotent on `external_id`: **201** when created, **200** when it already
existed.

```bash
curl -sX POST localhost:8088/v1/users \
  -H 'content-type: application/json' \
  -d '{"external_id":"user-42"}'
```

```json
{ "id": "0193…", "external_id": "user-42", "created_at": "2026-08-17T08:12:00Z" }
```

### `GET /v1/users/{user_id}`

---

## Deposit addresses

ChainRail does **not** derive addresses — that would require a master key in this
process. Addresses are exported from the custody system into
`deposit_address_pool`; assignment claims the next free one atomically.

### `POST /v1/deposit-addresses`

```bash
curl -sX POST localhost:8088/v1/deposit-addresses \
  -H 'content-type: application/json' \
  -d '{"user_id":"0193…","chain":"base-sepolia"}'
```

**201** on first assignment, **200** on repeat (the same address — a retry must
never burn a second address the user might also deposit to). **503** when the
pool is empty.

Returned addresses are EIP-55 checksummed; storage is lowercase so uniqueness is
case-insensitive.

### `GET /v1/deposit-addresses/{user_id}/{chain}`

---

## Deposits

### `GET /v1/deposits`

Query: `user_id`, `status`, `limit`, `after`, `after_id`.

```json
{
  "items": [{
    "id": "0193…",
    "user_id": "0193…",
    "chain": "base-sepolia",
    "asset": "USDC",
    "amount_raw": "100000000",
    "amount_formatted": "100",
    "status": "credited",
    "confirmations": 14,
    "required_confirmations": 10,
    "tx_hash": "0x…",
    "block_number": 45593489,
    "credited_at": "2026-08-17T08:14:22Z",
    "created_at": "2026-08-17T08:12:05Z"
  }],
  "next_cursor": { "after": "2026-08-17T08:12:05Z", "after_id": "0193…" }
}
```

Statuses: `observed`, `confirming`, `confirmed`, `credited`, `reorged`, `failed`.
Only `credited` means the funds are spendable. `reorged` deposits carry a
`failure_reason`.

### `GET /v1/deposits/{id}`

---

## Transactions

### `GET /v1/transactions/{hash}`

Every tracked transfer in one transaction — a single transaction can contain
several (batch transfers), disambiguated by `log_index`.

---

## Balances and ledger

### `GET /v1/balances/{user_id}`

Derived from the ledger, never from a mutable counter.

```json
{
  "user_id": "0193…",
  "balances": [{
    "chain": "base-sepolia",
    "asset": "USDC",
    "decimals": 6,
    "available_raw": "75000000",
    "available_formatted": "75",
    "reserved_raw": "25000000",
    "reserved_formatted": "25",
    "total_raw": "100000000"
  }]
}
```

`available` is spendable now; `reserved` is locked against in-flight withdrawals.
`deficit_raw` appears **only when non-zero** and indicates a receivable created by
a post-credit reorg — an exceptional condition (see `ledger.md`).

404 for an unknown user rather than an empty list, which would be
indistinguishable from a real user holding nothing.

### `GET /v1/ledger/{user_id}`

The immutable statement: every entry against the user's accounts, newest first,
with the running balance after each.

```json
{
  "items": [{
    "id": "0193…",
    "ledger_transaction_id": "0193…",
    "account_type": "user_available",
    "asset": "USDC",
    "amount_raw": "25000000",
    "amount_formatted": "25",
    "direction": "debit",
    "balance_after_raw": "75000000",
    "kind": "withdrawal_reserve",
    "reference_type": "withdrawal",
    "reference_id": "0193…",
    "created_at": "2026-08-17T08:20:00Z"
  }]
}
```

Entries are append-only. A correction appears as a new compensating entry, never
as a modification.

---

## Withdrawals

### `POST /v1/withdrawals`

```bash
curl -sX POST localhost:8088/v1/withdrawals \
  -H 'content-type: application/json' \
  -d '{
    "user_id": "0193…",
    "chain": "base-sepolia",
    "asset": "USDC",
    "amount": "25000000",
    "destination": "0x70997970C51812dc3A010C7d01b50e0d17dc79C8",
    "idempotency_key": "client-generated-unique-value"
  }'
```

**Idempotency contract**

| Situation | Result |
|---|---|
| New key | 201, created |
| Same key, identical body | 200, the original withdrawal — nothing new happened |
| Same key, **different** body | 409 `idempotency_key_conflict` |

The third case is why the key alone is not sufficient: silently returning the
earlier withdrawal would make the client believe a *different* transfer
succeeded. The fingerprint covers `user_id`, `chain`, `asset`, `amount` and
`destination` — but not `correlation_id`, since a retry from a different trace is
still the same transfer.

Keys must be 8–128 characters: shorter is guessable across users, unbounded is a
storage DoS.

**Validation, in order.** Chain and asset supported → destination parses (EIP-55
checksum *verified*, not repaired; zero address rejected) → amount positive → key
length → idempotent insert → policy → reserve → approve.

A mis-checksummed address is a **400**, never silently corrected — a "fixed"
address can send funds somewhere unrecoverable.

**Policy denials** return 422 with the rule in `error.details.policy`:
`maintenance_mode`, `asset_withdrawals_disabled`, `chain_not_allowed`,
`destination_denylisted`, `destination_is_hot_wallet`, `destination_is_internal`,
`below_minimum`, `above_maximum`, `daily_count_exceeded`, `daily_limit_exceeded`,
`insufficient_balance`.

Above `risk.manual_approval_threshold` the withdrawal is **held** in `validated`
rather than denied: funds are reserved but it will not broadcast until approved.

### `GET /v1/withdrawals/{id}`

```json
{
  "id": "0193…",
  "status": "completed",
  "amount_raw": "25000000",
  "destination": "0x70997970C51812dc3A010C7d01b50e0d17dc79C8",
  "tx_hash": "0x…",
  "confirmations": 12,
  "fee_paid_raw": "21000000000000"
}
```

Statuses: `requested`, `validated`, `approved`, `signing`, `broadcast`,
`confirming`, `completed`, `failed`, `cancelled`. Failures carry `failure_code`
and `failure_reason`.

### `GET /v1/withdrawals`

Query: `user_id`, `status`, `limit`, `after`, `after_id`.

### `POST /v1/withdrawals/{id}/cancel`

Releases the reservation. Legal **only before broadcast** — a signed transaction
cannot be recalled, so this returns 409 once funds may have left. Enforced by the
state machine in both Rust and Postgres.

### `POST /v1/withdrawals/{id}/approve`

Operator approval for a withdrawal held by the manual-approval threshold. 409 if
it is not awaiting approval.

> This endpoint sits behind the same bearer token as everything else, so it is
> **not** a real segregation-of-duties control. A production deployment needs a
> separate operator identity and an audit trail of who approved what.

---

## Worked example

```bash
API=http://localhost:8088

# 1. user
USER=$(curl -sX POST $API/v1/users -H 'content-type: application/json' \
  -d '{"external_id":"demo"}' | jq -r .id)

# 2. deposit address (pool must be seeded: ./scripts/seed.sh)
curl -sX POST $API/v1/deposit-addresses -H 'content-type: application/json' \
  -d "{\"user_id\":\"$USER\",\"chain\":\"base-sepolia\"}" | jq

# 3. watch it arrive
curl -s "$API/v1/deposits?user_id=$USER" | jq '.items[] | {status, confirmations, amount_formatted}'

# 4. balance
curl -s $API/v1/balances/$USER | jq

# 5. withdraw
curl -sX POST $API/v1/withdrawals -H 'content-type: application/json' \
  -d "{\"user_id\":\"$USER\",\"chain\":\"base-sepolia\",\"asset\":\"USDC\",
       \"amount\":\"1000000\",\"destination\":\"0x70997970C51812dc3A010C7d01b50e0d17dc79C8\",
       \"idempotency_key\":\"demo-$(date +%s)\"}" | jq

# 6. audit trail
curl -s "$API/v1/ledger/$USER?limit=10" | jq '.items[] | {kind, direction, amount_formatted, balance_after_raw}'
```
