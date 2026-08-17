# ChainRail

Multi-chain deposit and withdrawal infrastructure in Rust — the part of an
exchange backend that watches blockchains for incoming funds, credits them into a
double-entry ledger exactly once, and sends funds out again without ever
double-spending.

**What this is:** production-*style* architecture, built to be honest about
correctness under failure. **What this is not:** a production deployment. It ships
no production key custody and no per-user authentication — both are documented
gaps, not oversights. See [threat model](docs/threat-model.md).

---

## Why it exists

Most "crypto deposit" examples poll for a transaction, add a number to a balance
column, and stop. The hard parts are the ones they skip:

- A chain can **change its mind**. Ten blocks later, the deposit you credited may
  never have happened.
- Every message bus delivers **at least once**. Naïve consumers credit twice.
- Broadcasting a transaction is **irreversible and unacknowledged**. If the
  process dies mid-send, you cannot tell whether the funds left.
- Twenty simultaneous withdrawals against one balance of 100 must produce
  **exactly** the affordable number of successes, not 19 and a negative balance.
- RPC providers **lie, stall, and rate-limit**, and they are the only window onto
  the chain.

ChainRail exists to handle those, with the guarantees enforced by the database
rather than by hopeful application code.

## What is verified, not just claimed

Every number and behaviour below came from a run in this repository.

| Property | Evidence |
|---|---|
| 20 concurrent withdrawals vs. balance of 10 → exactly 10 succeed, none negative | `twenty_concurrent_withdrawals_against_a_balance_of_ten` |
| 1000 withdrawals, 1 account, concurrency 100 → exactly 500 succeed, drained to 0 | Load run, below |
| Unbalanced ledger transaction is impossible even bypassing Rust | `unbalanced_posting_is_rejected_by_the_database_even_if_rust_is_bypassed` |
| Ledger history cannot be rewritten via SQL | `ledger_history_cannot_be_rewritten` |
| 5 duplicate credit deliveries → one credit, two entries | `a_duplicate_event_delivery_is_processed_exactly_once` |
| Crash after broadcast → reconciled from chain, **one** transaction on the wire | `a_crash_after_broadcast_is_reconciled_from_the_chain_not_re_sent` |
| 100→101→102 forked to 100→101'→102'→103' handled correctly | `orphaned_blocks_are_demoted_and_the_replacement_chain_is_indexed` |
| Credited deposit orphaned → compensating entry, original preserved | `a_credited_deposit_orphaned_by_a_deep_reorg_is_compensated_not_deleted` |
| Rust state machine ≡ Postgres trigger, all 72 ordered pairs | `the_rust_state_machine_matches_the_database_trigger` |
| Kafka outage loses no events; backlog drains after | `a_broker_outage_does_not_lose_events_and_the_outbox_drains_after` |
| Real Base Sepolia blocks indexed with unbroken lineage | Run below |

A genuine deadlock (SQLSTATE 40P01) and a genuine ledger modelling gap were both
found *by these tests* during development and fixed — see
[ledger.md](docs/ledger.md#deadlock-avoidance).

---

## Architecture

```mermaid
flowchart TB
    RPCS[(RPC providers<br/>multiple per chain)]

    subgraph W["chainrail-worker"]
        GW[RPC Gateway<br/>health · failover · circuit breaker]
        WATCH[Chain Watcher]
        REORG[Reorg Engine]
        CONF[Confirmation Engine]
        CREDIT[Credit Handler]
        PIPE[Withdrawal Pipeline]
        RELAY[Outbox Relay]
    end

    subgraph S["chainrail-server"]
        API[HTTP API]
        RISK[Risk / Policy Engine]
    end

    LEDGER[[Append-only<br/>double-entry ledger]]
    PG[(PostgreSQL<br/>source of truth)]
    KAFKA[(Kafka / Redpanda)]

    RPCS --> GW --> WATCH --> REORG
    GW --> PIPE
    WATCH --> PG
    CONF --> PG
    REORG --> LEDGER
    CREDIT --> LEDGER
    PIPE --> LEDGER
    API --> RISK --> LEDGER
    LEDGER --> PG
    PG -.->|transactional outbox| RELAY --> KAFKA -.-> CREDIT
```

A **modular monolith**: twelve library crates, two binaries. Crate boundaries sit
where service boundaries would go, so components can be extracted later — but
nothing pays for a network hop it does not need yet. Details in
[architecture.md](docs/architecture.md).

### How deposits work

```
chain → watcher (ordered, never skips) → observed
      → confirmation engine (recomputed, never incremented) → confirmed
      → credit handler (four independent dedupe layers) → credited → balance
```

The watcher advances its cursor **inside the same transaction** that persisted the
block's contents, so a crash re-processes rather than skips. Reorg reconciliation
runs *before* every scan, so a fork is never built on top of.

### How withdrawals work

```
request → validate → risk → reserve → approved
        → sign + persist hash → COMMIT → broadcast → record → confirming → settled
```

The commit before broadcast is the hinge of crash safety: signing is deterministic
(RFC-6979), so the transaction hash is durable before the transaction exists on
the network. Recovery is then a *lookup* — ask the chain whether that hash exists
— not a guess.

### Ledger guarantees

Nine invariants, all enforced by Postgres triggers and constraints rather than by
application convention: balanced transactions, positive amounts, immutable
history, non-negative spendable balances, no balance drift, at-most-once postings.
Full table in [ledger.md](docs/ledger.md#invariants-and-where-each-is-enforced).

### Reorg handling

Block lineage `(height, hash, parent_hash)` is stored for every processed block. A
hash mismatch at the cursor triggers a walk back to the common ancestor; blocks
above it are orphaned, their deposits moved to `reorged`, and already-credited
deposits get a **compensating** ledger transaction — history is never deleted. A
reorg deeper than the retained window is escalated, not guessed.

### Failure model

Twelve failure modes with tested behaviour, a retry policy table, and a recovery
matrix marking which cases need a human: [failure-modes.md](docs/failure-modes.md).

---

## Quick start

Requires Docker and (for local builds) Rust 1.85+.

```bash
git clone <repo> && cd chainrail
cp .env.example .env

docker compose up -d                 # postgres, redis, redpanda, prometheus, grafana, api, worker
./scripts/seed.sh                    # address pool + custody funding + a demo user

curl -s localhost:8088/health | jq
curl -s localhost:8088/ready  | jq
```

| Service | URL |
|---|---|
| API | http://localhost:8088 |
| API metrics | http://localhost:9090/metrics |
| Worker metrics | http://localhost:9091/metrics |
| Prometheus | http://localhost:9095 |
| Grafana | http://localhost:3001 (anonymous viewer) |
| Postgres | `localhost:55432` (`chainrail`/`chainrail`) |
| Redpanda | `localhost:19092` |

> Host ports are deliberately unconventional (55432, 56379, 8088). 5432 and 8080
> are usually already taken, and connecting to the wrong database is a nasty way
> to lose an hour. Override with `POSTGRES_PORT`, `API_PORT`, etc.

### Developing from source

```bash
./scripts/dev.sh        # deps in Docker, API + worker from source
./scripts/check.sh      # fmt + clippy -D warnings + full test suite
./scripts/reset-db.sh   # drop and recreate the schema
```

## Configuration

Layered: `config/default.toml` → `config/{APP_ENV}.toml` → `CHAINRAIL__*`
environment variables (`__` nests). Secrets come from the environment only.

`AppConfig::validate()` runs before any listener binds and **refuses to boot** on:

- an EVM chain without `numeric_chain_id` (EIP-155 replay protection)
- `reorg_scan_depth <= required_confirmations` (a reorg that could invalidate a
  credited deposit would be undetectable)
- a development signer outside `local`/`test`/`ci`
- a missing API token, plaintext-HTTP RPC, or non-JSON logs outside local/test
- an asset with withdrawals enabled but no hot wallet
- a Solana chain (not implemented — refused rather than silently ignored)

Adding an EVM network (Ethereum, BSC, Base mainnet) is a config change:

```toml
[[chains]]
id = "bsc-testnet"
kind = "evm"
numeric_chain_id = 97
reorg_scan_depth = 96
[chains.finality]
mode = "confirmations"
blocks = 15
[[chains.rpc]]
name = "primary"
url = "https://…"
weight = 200
```

## API

Full reference: [api.md](docs/api.md). Machine-readable: `GET /openapi.json`.

```
POST   /v1/users                            GET /v1/users/{id}
POST   /v1/deposit-addresses                GET /v1/deposit-addresses/{user}/{chain}
GET    /v1/deposits                         GET /v1/deposits/{id}
GET    /v1/transactions/{hash}
GET    /v1/balances/{user}                  GET /v1/ledger/{user}
POST   /v1/withdrawals                      GET /v1/withdrawals  ·  /v1/withdrawals/{id}
POST   /v1/withdrawals/{id}/cancel          POST /v1/withdrawals/{id}/approve
GET    /health  ·  /ready  ·  /metrics  ·  /internal/ledger-integrity
```

All money is a **decimal string of raw units** — never a JSON number. A float
amount is rejected with 400.

```bash
curl -sX POST localhost:8088/v1/withdrawals -H 'content-type: application/json' -d '{
  "user_id": "…", "chain": "base-sepolia", "asset": "USDC",
  "amount": "25000000",
  "destination": "0x70997970C51812dc3A010C7d01b50e0d17dc79C8",
  "idempotency_key": "unique-per-request"
}'
```

201 when created, 200 on an identical replay, **409** when the same key arrives
with a different body — silently returning the earlier withdrawal would make a
client believe a different transfer succeeded.

## Testing

```bash
cargo test --all                                        # unit tests; integration tests skip

# A SEPARATE database -- the harness truncates every table between tests, so
# pointing it at the application database would wipe a running worker's state.
# docker-compose creates chainrail_test on first init.
export TEST_DATABASE_URL=postgres://chainrail:chainrail@127.0.0.1:55432/chainrail_test
cargo test --all                                        # everything
cargo test -p chainrail-integration --test concurrency  # one suite
```

Integration tests run against a **real Postgres** — the invariants under test are
enforced *by* Postgres, so mocking it would test nothing. They skip themselves
when `TEST_DATABASE_URL` is unset, so `cargo test --all` works without Docker.

The chain is replaced by a scriptable in-memory `MockChain` implementing
`ChainAdapter`. The watcher, reorg engine, confirmation engine and withdrawal
pipeline all run for real against it — you cannot ask a testnet to fork on cue.

## Benchmarking

```bash
cargo build --release -p chainrail-load
./target/release/chainrail-load ledger    -n 10000 -c 64
./target/release/chainrail-load contended -n 1000  -c 100
./target/release/chainrail-load api       -n 5000  -c 128 --url http://127.0.0.1:8088
```

Both non-API scenarios **verify ledger integrity afterwards and exit non-zero if
it is violated** — a load run that leaves the ledger inconsistent is a failed run
however good the throughput looked.

### Measured

Apple Silicon laptop, Postgres 17 in Docker, debug-free release build. Everything
on one machine, so these are *lower* bounds shaped by local contention — not
projections.

**Ledger postings** — 10,000 deposit credits across 100 users, concurrency 64:

```
throughput   1150 ops/s      p50 36.5ms   p95 175.5ms   p99 277.0ms
errors       0 (0.000%)      ledger CLEAN: 10000 transactions, 101 accounts
```

Each operation is a full transaction: resolve two accounts, insert a ledger
transaction, insert two entries, fire the balance trigger twice, run the deferred
balance check, commit. Roughly 1150 balanced double-entry postings per second with
every invariant checked.

**Contended withdrawals** — 1000 against a *single* account, concurrency 100:

```
throughput   740 ops/s       p50 38.3ms   p95 535.2ms   p99 810.5ms
successful   500  (exactly the affordable number)
rejected     500  (insufficient balance — the correct outcome)
errors       0                final balance 0, never negative, ledger CLEAN
```

The worst case for row-lock contention. Latency degrades as expected under
serialisation, correctness does not.

**API reads** — 5000 balance/ledger requests, concurrency 128:

```
throughput   5121 ops/s      p50 22.2ms   p95 42.2ms   p99 58.3ms
errors       0 (0.000%)
```

**Bottleneck:** Postgres commit latency on the write paths. The ledger scenario is
`fsync`-bound, not CPU-bound — p99 tracks group-commit batching. The API scenario
is bound by connection-pool width at concurrency 128. Neither is bound by Rust.

### Real chain validation

Pointed at live Base Sepolia through the real RPC gateway:

```
blocks indexed   45,593,489 → 45,593,519  (31 blocks)
watcher lag      0
broken lineage   0            (every parent_hash matches the prior canonical block)
outbox           37 events enqueued, 37 published to Kafka
RPC calls        96 eth_getBlockByNumber, 15 eth_getLogs, 1 eth_chainId
failover         1 request served by the secondary endpoint
```

Events were consumed back off the `chain.blocks` topic with correct headers, so
the whole path — gateway → watcher → Postgres → outbox → Kafka — works against a
real network.

## Observability

Structured JSON logs carrying `request_id`, `correlation_id`, `user_id`, `chain`
and `tx_hash`. Prometheus metrics with `HELP` text (see
`crates/observability/src/lib.rs`). Optional OTLP tracing. A Grafana dashboard
provisioned automatically, with the **correctness** row first — ledger integrity,
reorg reversals, user deficits, dead letters — because those are the ones that
mean money is wrong rather than slow.

Alerts are split by intent: `chainrail-correctness` pages immediately;
`chainrail-liveness` warns after a few minutes, because a stalled watcher makes
users wait but makes nothing incorrect.

## Security limitations

Read [threat-model.md](docs/threat-model.md) before doing anything with real
funds. The two critical gaps:

1. **No production key custody.** `LocalDevelopmentSigner` holds a key in process
   memory. Mitigations are blast-radius only (redacting `Debug`, no key in logs,
   refuses to boot outside local/test, `/health` advertises
   `signer_production_grade: false`). Production needs KMS/Vault/HSM/MPC **with
   policy enforced at the signer** — ChainRail's risk engine is bypassed by a
   compromised ChainRail process.
2. **No per-user authentication.** A single shared bearer token, constant-time
   compared, with no identity. Any holder can request a withdrawal for any
   `user_id`.

Also absent: supply-chain controls (`cargo-deny`, SBOM), an independent on-chain
reconciler, hot/warm/cold wallet tiering, cross-provider quorum reads, encryption
at rest, and a signed audit log.

Implemented: strict input validation (EIP-55 checksums *verified*, not repaired),
parameterised queries throughout, no secrets in the repo, sanitised logs, scrubbed
provider URLs, generic 5xx messages, body size limits, timeouts, rate limiting,
overflow-checked money arithmetic, least-privilege DB grants documented, and
config validation that refuses unsafe combinations at boot.

## Known limitations

- **Native (non-ERC-20) deposits are not detected.** Token deposits work fully;
  the native asset exists only for gas accounting.
- **Solana is not implemented.** `ChainAdapter` is the seam and
  `architecture.md#non-evm-chains` describes what it needs; config validation
  refuses a Solana chain rather than pretending.
- **A transaction surviving a reorg in a new block stays `reorged`** and needs
  operator reconciliation. Conservative by choice — preferring a stuck deposit
  over a possible double credit.
- **A permanently dropped broadcast blocks the nonce sequence.** No automated
  fee-bump replacement.
- **Redis is wired but unused.** Nothing on a correctness path needs it yet.
- **Ledger verification scans every entry.** Fine at these volumes, needs
  snapshots past ~10M entries.
- **One watcher per chain per deployment.** Two are safe but wasteful.

## Next five improvements

1. **Real custody integration (AWS KMS or an MPC provider) with signer-side
   policy.** The single highest-value change. Everything else is defence in depth
   around a key sitting in RAM.
2. **Per-user authentication and authorization.** Today any token holder can move
   any user's funds. Needs an identity service, scoped tokens, and a check that
   the authenticated principal owns the `user_id` in the request.
3. **Independent on-chain reconciler.** A separate process comparing
   `eth_getBalance` / ERC-20 `balanceOf` for every custody wallet against
   `exchange_custody` in the ledger, alerting on divergence. Currently nothing
   detects "the ledger says we hold 100 but the chain says 90".
4. **Automated stuck-transaction replacement.** Fee-bump a dropped transaction
   with the same nonce, and expose a nonce-gap detector — a single stuck
   withdrawal currently stalls every later one on that wallet.
5. **Cross-provider quorum reads before crediting.** Require two independent RPC
   providers to agree on a block hash before a deposit is credited, closing the
   single-lying-provider window that confirmations alone do not.

---

## Repository layout

```
crates/
  common/          money · chain identity · config · errors · events   (no I/O)
  database/        pool · migrations · row types · repositories
  rpc/             JSON-RPC gateway: health · failover · breaker
  chains-evm/      ABI codec · typed client · ChainAdapter
  signer/          Signer trait + development backends
  ledger/          postings · balances · statements · integrity
  events/          Kafka bus · outbox relay · consumer runtime
  risk/            policy evaluation                                   (no I/O)
  deposits/        watcher · confirmations · reorg · crediting
  withdrawals/     request service · state machine · pipeline
  observability/   logs · metrics · tracing
  api/             router · DTOs · middleware · error mapping
apps/
  server/          HTTP API + metrics + periodic ledger verification
  worker/          watchers · confirmations · consumers · outbox · pipeline
migrations/        0001…0007, plain SQL
tests/
  integration/     real Postgres + scriptable MockChain
  load/            three load scenarios with correctness verification
infra/             prometheus · alerts · grafana · kubernetes
docs/              architecture · ledger · threat-model · failure-modes · api
```

## License

MIT
