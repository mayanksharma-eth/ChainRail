# Threat Model

Scope: ChainRail as it exists in this repository. Where a control is missing, that
is stated plainly rather than described aspirationally.

**Overall posture:** ChainRail is *production-style architecture*. It is **not** a
production deployment and it contains **no certified custody infrastructure**.
The single largest gap is key custody (§1). Nothing here should hold real funds.

Trust boundaries, from least to most trusted:

```
untrusted:  API clients, RPC providers, chain data
semi:       Kafka, Redis
trusted:    Postgres, the ChainRail process
critical:   signing keys  (NOT protected in this codebase)
```

---

## 1. Compromised signer / key custody

**Threat.** An attacker with code execution in the ChainRail process, or read
access to its memory or environment, extracts the signing key and drains the hot
wallet. This is the highest-impact threat in the system.

**What exists.** Nothing that stops it. `LocalDevelopmentSigner` holds a
secp256k1 key in process memory. Mitigations are limited to blast-radius and
accident reduction:

- The key is wrapped in a `Debug`-redacting type with no accessor returning raw
  bytes, so it cannot leak through a log line or panic message
  (`debug_output_never_contains_key_material`).
- Parse failures never echo the input, so a key pasted into the wrong field does
  not land in logs (`rejects_malformed_keys_without_echoing_them`).
- `AppConfig::validate` **refuses to boot** any environment other than
  `local`/`test`/`ci` with a development signer, and the worker refuses to run the
  withdrawal pipeline in a production-like environment with one.
- `/health` reports `signer_production_grade: false` so no operator or dashboard
  can mistake it for real custody.
- Withdrawal destinations pass a policy engine (denylist, hot-wallet check,
  per-request and daily caps, manual-approval threshold).

**What production requires.** Replace the `Signer` implementation with AWS KMS,
GCP Cloud KMS, HashiCorp Vault Transit, a dedicated HSM, or an MPC/threshold
custody provider. The trait is deliberately narrow — build an unsigned
transaction, hand it over, receive bytes and a hash — so the swap is contained.

Critically, **policy must also be enforced at the signer**, not only in ChainRail:
destination allowlists, per-period value caps, and quorum approval for large
transfers. ChainRail's risk engine is a business control that a compromised
ChainRail process bypasses entirely. Only signer-side policy survives that.

Additionally required and absent: hot/warm/cold wallet tiering so the hot wallet
holds a bounded float, and an independent reconciler comparing on-chain balances
against ledger custody.

---

## 2. Compromised or malicious RPC provider

**Threat.** A provider lies: fabricates a block, reports a transfer that never
happened, hides one that did, returns a stale head, or replays old data.

**Controls.**

| Attack | Control |
|---|---|
| Fabricated deposit | Deposits require N confirmations; a fabricated block cannot produce a consistent `parent_hash` lineage across the confirmation window |
| Stale head | Confirmations recomputed from `(head, block)`; `confirmations_for` saturates so a head *behind* the deposit yields 1, never a wrap-around |
| Wrong network | `verify_chain_id` at startup compares `eth_chainId` against config and refuses to proceed on mismatch |
| Malformed response | Every field validated; a `null` block number, missing `blockHash`, or non-hex log data is an error, never a default (`pending_blocks_are_rejected_not_guessed`) |
| Amount overflow | Transfer values above `i128::MAX` are rejected, never truncated (`transfer_values_above_i128_are_rejected`) |
| Dirty address topic | Non-zero padding in an address topic is rejected rather than truncated |
| Silent unavailability | Circuit breaker per endpoint; `AllRpcEndpointsUnhealthy` alert |
| Single-provider dependency | Multiple endpoints per chain with weighted, health-aware selection |
| Credential leakage in errors | Provider URLs are scrubbed before logging (`urls_and_api_keys_are_scrubbed_from_errors`) |

**Residual risk.** ChainRail trusts the *majority* view of its configured
providers implicitly — it does not cross-check two providers against each other
for the same block. A single compromised provider configured as the only endpoint
can lie for the length of the confirmation window. Mitigation: configure
independent providers, and add quorum reads before crediting (listed as a next
improvement).

**Not implemented.** Light-client or header-chain verification. ChainRail does not
independently verify proof-of-work/stake.

---

## 3. Chain reorganisation

**Threat.** A deposit is credited, then the block containing it is orphaned. The
user withdraws in between; the exchange loses the funds.

**Controls.** See `docs/architecture.md#reorg-handling`. Summary:

- Reorg reconciliation runs *before* every scan, so a fork is never built on.
- The partial unique index on canonical height makes orphaning mandatory before a
  replacement can be stored — the schema prevents forgetting.
- Uncredited deposits become `reorged` and can never subsequently be credited
  (`a_reorged_deposit_can_never_subsequently_be_credited`).
- The confirmation engine independently refuses to confirm a deposit whose block
  is no longer canonical.
- Already-credited deposits get a compensating ledger transaction; a shortfall is
  booked to `user_deficit` and pages.
- A reorg deeper than `reorg_scan_depth` is escalated, not guessed.
- Config validation requires `reorg_scan_depth > required_confirmations`.

**Residual risk.** Confirmation thresholds are a business judgement about
acceptable loss. Ten confirmations on Base is conventional, not a guarantee. A
reorg deeper than the threshold *will* cause a realised loss if funds have
already left; ChainRail's job is to detect and record it, not to prevent it.

---

## 4. Malicious withdrawal request

**Threat.** An attacker with API access drains an account, or exfiltrates funds
to an address they control.

**Controls.**

- **Address validation is strict.** EIP-55 checksums are *verified*, never
  repaired; a mixed-case address with a bad checksum is rejected. The zero
  address is rejected. Both tested.
- **Balance cannot be overdrawn.** Enforced by a database CHECK inside the
  balance trigger, not application logic.
- **Idempotency is body-bound.** Reusing a key with a different payload returns
  409, so an attacker cannot ride a legitimate key to move a different amount.
- **Destination controls.** Configurable denylist; sending to ChainRail's own hot
  wallet is refused (it would be re-credited as a deposit, inflating the ledger);
  sending to another user's deposit address is refused.
- **Velocity limits.** Per-request min/max, per-user daily value and count caps.
- **Manual approval** above a configurable threshold.
- **Global kill switch** (`risk.maintenance_mode`) checked before every other rule.
- Rules evaluate in fixed order, so denial reasons are deterministic and support
  answers are reproducible (`rule_order_is_deterministic`).

**Gap — authentication.** `/v1` is protected by a single shared bearer token
compared in constant time. It carries **no user identity**: any caller holding the
token can request a withdrawal for *any* `user_id`. This is the second-largest gap
after key custody. A real deployment needs per-user authentication, scoped
authorization, and an independent check that the authenticated principal owns the
`user_id` in the request body. The middleware is a single checkpoint precisely so
this is one file to replace.

---

## 5. Database compromise

**Threat.** An attacker with SQL access alters balances or erases history.

**Controls.**

- Ledger tables are **append-only by trigger**. `UPDATE` and `DELETE` on
  `ledger_entries` / `ledger_transactions` raise an exception — so even a direct
  `psql` session cannot rewrite financial history
  (`ledger_history_cannot_be_rewritten`).
- Cached balances are reconciled against `SUM(entries)` by
  `verify_ledger_integrity`, run every 5 minutes by the server and exposed as
  `chainrail_ledger_integrity_problems`. Tampering with a balance directly is
  detected (`integrity_check_detects_manually_corrupted_balances`).
- **Least privilege is documented** in `.env.example`: the application role gets
  `SELECT, INSERT, UPDATE` only — no `DELETE`, `TRUNCATE`, `DROP` or `ALTER`, and
  it owns nothing. Migrations run as a separate owner role. Withholding `DELETE`
  means even a SQL-injection bug cannot erase history.

**Not implemented.** Encryption at rest, an append-only WAL archive shipped to
separate credentials, and a signed audit log. `verify_ledger_integrity` detects
tampering with a *balance*, but an attacker who inserts a *balanced* fraudulent
transaction passes every check — that requires an external audit trail.

---

## 6. Duplicate events and Kafka duplication

**Threat.** At-least-once delivery credits a deposit twice.

**Controls.** Four independent layers on the credit path (see the idempotency
inventory in `docs/architecture.md`). Tested directly by
`a_duplicate_event_delivery_is_processed_exactly_once` (five deliveries, one
credit) and `concurrent_duplicate_postings_collapse_to_one` (25 concurrent
workers, one posting).

Consumers claim events in the *same transaction* as their side effect, so a crash
mid-handling either leaves no claim (safe reprocess) or a claim (safe skip).

**Residual risk.** `processed_events` is pruned after 7 days. A redelivery older
than that would be reprocessed — protected by the remaining three layers, but the
window is a deliberate trade against unbounded table growth.

---

## 7. Race conditions

**Threat.** Concurrent requests overspend one balance, or two signers reuse a
nonce.

**Controls.**

- Balance: row lock plus a database CHECK. 20 concurrent withdrawals against a
  balance of 10 → exactly 10 succeed, verified in tests and at load (500/1000 at
  concurrency 100).
- Nonces: allocated by a single atomic `INSERT ... ON CONFLICT DO UPDATE ...
  RETURNING`, which never moves backwards and cannot hand two callers the same
  value. Two signers reusing a nonce would strand one transaction permanently.
- Deadlocks: entries inserted in `account_id` order, so all writers take locks in
  the same order. A real deadlock was found and fixed this way.
- Worker replicas: `FOR UPDATE SKIP LOCKED` on every queue claim.

---

## 8. Replay

**Threat.** A signed transaction is replayed on another chain, or an old API
request is resubmitted.

**Controls.**

- **EIP-155.** `numeric_chain_id` is mandatory for EVM chains (config validation
  rejects its absence) and is bound into every signature. Verified by
  `chain_id_is_bound_into_the_signature`: the same transaction on chain 1 and
  chain 84532 produces different hashes.
- **API replay** is neutralised by idempotency keys — a resubmitted request
  returns the original withdrawal rather than creating a second.

**Not implemented.** Request signing and timestamp/nonce windows on API calls. A
captured bearer token can be replayed indefinitely until rotated.

---

## 9. Denial of service

**Controls.**

- Request body limit (64 KiB default) applied *outermost*, so an oversized upload
  is refused without being read.
- Per-request timeout returning 504.
- Global rate limit with burst.
- Bounded pagination (`max_page_size`, clamped, keyset-based so deep pages do not
  degrade).
- Bounded RPC concurrency per endpoint; a saturated endpoint is skipped rather
  than queued behind.
- Statement and idle-in-transaction timeouts on every database connection.
- Backoff with jitter everywhere, so a dependency recovering does not get a
  thundering herd (`jitter_stays_within_the_window`).
- Bounded retention: outbox, processed events, and block metadata are all pruned.

**Gaps.** The rate limit is a single global bucket, not per-client — one caller
can consume the whole budget. Real deployments rate-limit per key at the edge.
There is no request queue depth limit or load shedding beyond the rate limiter.

---

## 10. Secret leakage

**Controls.**

- No secrets in the repository. `.env` is gitignored; `.env.example` contains only
  the published Hardhat test key, explicitly labelled worthless.
- Config validation rejects plaintext-HTTP RPC URLs outside local/test.
- SQL statements are logged at TRACE only, so parameters never reach INFO logs.
- Provider URLs (which routinely embed API keys) are scrubbed from error strings.
- Server-side error messages are **not** echoed to clients: a database error
  naming a table, an RPC error containing a key, or a signer error are all
  replaced with a generic message, with full detail in the logs keyed by
  `request_id` (`server_error_messages_never_leak_internals`).
- Request-id headers are sanitised before logging, preventing log injection via
  newline (`sanitizes_ids_against_log_injection`).

**Gaps.** No secret rotation mechanism, no envelope encryption, no audit of who
read which secret. Secrets arrive via environment variables, which are visible to
anything that can read `/proc/<pid>/environ`.

---

## 11. Stale chain data

**Threat.** A lagging RPC replica reports an old head; deposits appear
unconfirmed or withdrawals appear unmined.

**Controls.** Confirmations are recomputed, never accumulated, so a stale head
produces a *lower* count rather than a wrong one. `confirmations_for` saturates
on underflow. `chainrail_watcher_lag_blocks` and the `WatcherStalled` alert make
a persistently stale provider visible. Endpoint health scoring deprioritises slow
endpoints.

**Residual risk.** ChainRail cannot distinguish "the chain has not advanced" from
"this provider has not advanced" with a single provider. Cross-provider head
comparison is a next improvement.

---

## 12. Supply chain

**Not addressed.** Dependencies are pinned by `Cargo.lock` but there is no
`cargo-deny` policy, no SBOM, no vulnerability scanning in CI, and no
reproducible-build verification. For a system that signs transactions, a
compromised transitive dependency is a direct path to key theft. This would be a
blocking gap before any real deployment.

---

## Summary of gaps, by severity

| Gap | Severity | Notes |
|---|---|---|
| No production key custody | **Critical** | Deliberate; ChainRail ships dev signers only and refuses to run otherwise |
| No per-user authentication | **Critical** | Any token holder can act as any user |
| No supply-chain controls | High | No `cargo-deny`, SBOM, or vuln scanning |
| No signer-side policy enforcement | High | ChainRail-side policy is bypassed by a compromised process |
| No independent on-chain reconciler | High | Ledger custody is never compared against real balances |
| No hot/warm/cold tiering | High | The hot wallet is the whole float |
| No cross-provider quorum reads | Medium | A single lying provider can mislead for the confirmation window |
| No encryption at rest / signed audit log | Medium | Balanced fraudulent postings pass integrity checks |
| Global rather than per-client rate limit | Medium | One client can exhaust the budget |
| No API request signing | Low–Medium | A captured token replays until rotated |
