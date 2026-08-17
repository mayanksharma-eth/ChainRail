# Failure Modes

What breaks, what happens, and what ChainRail guarantees. Every row marked
"tested" names a real test.

## Guarantees

These hold across every failure below:

1. **No double credit.** A deposit is credited at most once, enforced by four
   independent database constraints.
2. **No negative spendable balance.** Enforced by a `CHECK` inside the balance
   trigger, not by application logic.
3. **No lost financial history.** Ledger tables are append-only by trigger;
   corrections are compensating transactions.
4. **No skipped blocks.** The watcher cursor advances only inside the transaction
   that persisted the block's contents.
5. **No double send.** Signing is deterministic and the nonce is
   database-authoritative, so a retry reproduces the identical transaction.
6. **No event lost between a state change and its publication.** The change and
   the outbox row commit together.

What is *not* guaranteed: liveness during a dependency outage, and recovery of
value genuinely lost to a reorg deeper than the confirmation threshold.

---

## Dependency outages

### RPC provider goes offline

| | |
|---|---|
| **Detection** | Circuit breaker trips after `failure_threshold` consecutive failures; `chainrail_rpc_endpoint_healthy` → 0 |
| **Behaviour** | Failover to the next healthiest endpoint. If all are open, `NoHealthyRpcEndpoint`; the watcher backs off exponentially with jitter |
| **Data impact** | None. The cursor does not advance, so no block is skipped |
| **Recovery** | Automatic. Breaker half-opens after `breaker_reset_ms`, one probe closes it after `success_threshold` successes |
| **Tested** | `an_rpc_outage_stalls_the_watcher_and_it_resumes_cleanly` — asserts the cursor does not move and the deposit produced during the outage is picked up afterwards |
| **Alert** | `AllRpcEndpointsUnhealthy` (critical, 2m) |

### RPC returns a malformed response

Every field is validated at the boundary. A `null` block number, missing
`blockHash`, non-hex log data, dirty address-topic padding, or a transfer value
above `i128::MAX` is an **error**, never a silently-defaulted value. A log that
*claims* to be a `Transfer` but cannot be decoded is logged at ERROR and counted
(`chainrail_evm_undecodable_logs_total`) rather than skipped quietly — that would
lose a user's deposit.

Tested: `pending_blocks_are_rejected_not_guessed`,
`malformed_transfer_logs_are_errors_not_silent_skips`,
`transfer_values_above_i128_are_rejected`,
`address_topics_with_dirty_padding_are_rejected`.

### Kafka unavailable

| | |
|---|---|
| **Detection** | `chainrail_outbox_pending` and `chainrail_outbox_oldest_pending_seconds` climb |
| **Behaviour** | Business state commits regardless — that is the point of the outbox. Events queue durably in Postgres. The relay retries with exponential backoff |
| **Data impact** | None. Deposits are still detected, confirmed and credited; withdrawals still process. Only downstream *notification* is delayed |
| **Recovery** | Automatic. The backlog drains when the broker returns |
| **Tested** | `a_broker_outage_does_not_lose_events_and_the_outbox_drains_after` — asserts the deposit exists while nothing is published, nothing is marked published during the outage, and the backlog fully drains after |
| **Alert** | `OutboxRelayWedged` (warning, 5m) |

An event that exhausts `max_delivery_attempts` moves to `dead_letters` so the
queue head cannot block forever
(`events_exceeding_their_retry_budget_are_dead_lettered_not_retried_forever`).

### Postgres unavailable

| | |
|---|---|
| **Detection** | `/ready` returns 503; `PoolTimedOut` mapped to `Unavailable` |
| **Behaviour** | The API returns 503 for anything touching the database. Workers back off and retry. No partial writes: everything money-related is in a transaction |
| **Data impact** | None. Uncommitted work rolls back |
| **Recovery** | Automatic once connections are available. Every stage re-derives its state from the database rather than from memory |

Postgres is the only **hard** dependency: `/ready` fails on its loss and succeeds
on Kafka's or an RPC provider's, because balance and history reads stay correct
without those. That distinction stops a provider outage cascading into a full API
outage.

### Redis unavailable

No impact. Redis is configured (`redis.required = false` by default) but nothing
on a correctness path depends on it. It is reserved for cross-process advisory
locks and read caching; the ledger's row locks and CHECK constraints do that work
today.

---

## Process failures

### Worker crashes mid-event

At-least-once processing. The event is redelivered because the Kafka offset was
never committed. The handler claims the event in the same transaction as its side
effect, so redelivery either sees no claim (safe reprocess) or the claim (safe
skip). Tested: `a_duplicate_event_delivery_is_processed_exactly_once`.

### Worker crashes mid-block-scan

The cursor and the block's contents commit together, so the batch is
re-processed from the last committed cursor. Re-processing is a no-op because
transfer insertion is idempotent on `(chain, tx_hash, log_index)`.
Tested: `rescanning_the_same_blocks_creates_no_duplicate_deposits`.

### Withdrawal broadcast succeeds but the database update fails

**The most dangerous failure in the system.** Handled by ordering:

```
1. allocate nonce, sign, persist tx_hash  -> COMMIT
2. broadcast
3. record broadcast                       -> COMMIT
```

A crash between 1 and 3 leaves the withdrawal in `signing` with a durable
`tx_hash`. `recover_signing` then asks the chain:

- **Hash exists (pending or mined)** → record the broadcast we never recorded and
  hand it to the confirmation stage. No second transaction.
  Tested: `a_crash_after_broadcast_is_reconciled_from_the_chain_not_re_sent`
  (asserts `broadcasts().len() == 1` and the nonce is unchanged).
- **Hash unknown** → re-sign with the **recorded** nonce, producing byte-identical
  output, and re-broadcast. Tested:
  `a_broadcast_that_never_landed_is_re_sent_as_the_same_transaction`.

The reverse ordering (broadcast, then persist) would lose the hash and strand
funds with no way to reconcile.

Additionally, the gateway marks `eth_sendRawTransaction` as
`Idempotency::UnsafeOnTimeout`: it fails over only when the request provably never
reached a node. A *timeout* is ambiguous and surfaces as an error, so the caller
reconciles by hash rather than re-signing.
Tested: `retry_policy_respects_idempotency`.

### Confirmation worker restarts

No state is held in memory. Confirmations are recomputed from
`(head, including_block)` on every pass, so a restart converges on the same
answer. Tested: `recomputing_from_head_is_idempotent`.

### Process restarts / deployment

Both binaries handle `SIGTERM` and `SIGINT`. The API stops accepting and drains
in-flight requests; the worker cancels its components and waits up to
`shutdown_grace_ms`. Aborting past the grace period is safe: uncommitted work
rolls back and every stage re-derives its state.

---

## Chain failures

### Reorg, deposit not yet credited

Blocks above the common ancestor are orphaned, their transfers marked orphaned,
their deposits moved to `reorged`. No ledger activity — nothing had been credited.
The deposit can never subsequently be credited, even if a stale credit event
arrives afterwards. Tested:
`orphaned_blocks_are_demoted_and_the_replacement_chain_is_indexed`,
`a_reorged_deposit_can_never_subsequently_be_credited`.

### Reorg, deposit already credited

A compensating `deposit_reversal` transaction is posted. The original credit is
untouched. If the user still holds the funds, the reversal simply debits their
available balance. If they have already withdrawn, the shortfall is booked to
`user_deficit` — a receivable — rather than driving the balance negative.

This is a **realised loss** requiring a business decision. It pages
(`CreditedDepositReversedByReorg`, `UserDeficitBooked`).

Tested: `a_credited_deposit_orphaned_by_a_deep_reorg_is_compensated_not_deleted`,
`reorg_reversal_books_a_deficit_when_funds_were_already_spent`.

### Reorg deeper than `reorg_scan_depth`

**Escalated, not guessed.** `detect` returns an error naming the height from which
manual reconciliation is required. Rewinding to an arbitrary point could
double-reverse or re-credit. Config validation requires
`reorg_scan_depth > required_confirmations`, so any reorg able to invalidate a
credited deposit is at least always *detectable*.
Tested: `a_reorg_deeper_than_the_scan_window_is_escalated_not_guessed`.

### Transaction survives a reorg in a new block

The deposit is marked `reorged` when its original block is orphaned. On re-scan
the transfer is recognised as already-known (unique on
`(chain, tx_hash, log_index)`), so **no second deposit is created**.

The conservative consequence: a surviving transaction stays `reorged` rather than
being resurrected, and needs operator reconciliation. Preferring a stuck deposit
over a possible double-credit is the right trade, but it is a real limitation —
automatic re-validation of surviving transactions is a next improvement.
Tested: `a_transaction_surviving_the_reorg_in_a_new_block_is_re_observed_once`.

### Withdrawal transaction reverts on chain

The value never left. The clearing leg is reversed, the user's reservation is
released, and the gas — genuinely spent — is booked as an expense.
Tested: `an_onchain_revert_returns_the_funds_and_still_books_the_gas`.

### Broadcast transaction disappears from the mempool

**Not failed.** The nonce may still be consumed by that exact transaction later,
so releasing the funds now risks a double spend. The withdrawal stays `broadcast`
with funds reserved, and `chainrail_withdrawal_missing_tx_total` increments.

This is a deliberate stall requiring operator attention: a permanently dropped
transaction blocks the nonce sequence. Automated fee-bump replacement is a next
improvement. Tested: `a_dropped_transaction_is_monitored_rather_than_failed`.

---

## Operational failures

### Deposit address pool exhausted

`POST /v1/deposit-addresses` returns 503 `service_unavailable` — an operational
failure, not the client's fault, and retrying will not help until the pool is
topped up from the custody system. `chainrail_deposit_address_pool_available` and
the `DepositAddressPoolLow` alert warn before it happens.
Tested: `deposit_addresses_are_assigned_from_the_pool_and_are_idempotent`.

### Hot wallet out of gas

Broadcast fails at the RPC. The withdrawal has a persisted hash but never reaches
the network, so `recover_signing` retries it — indefinitely, until the wallet is
funded. Funds stay reserved throughout; nothing is lost. There is no dedicated
alert for hot-wallet gas balance, which is a gap.

### Ledger integrity violation

Should be impossible. If `verify_ledger_integrity` reports a problem it means a
bad migration, a dropped trigger, or direct database manipulation.
`LedgerIntegrityViolation` pages immediately at severity critical. The runbook
response is: stop withdrawals (`risk.maintenance_mode = true`), then reconcile
from `ledger_entries`, which is the authoritative record.

### Clock skew

Only `created_at`/`updated_at` and the 24-hour velocity window depend on wall
time, all from `now()` on the database server — so all timestamps share one clock.
Correctness never depends on time: confirmations come from block heights,
idempotency from natural keys.

---

## Retry policy

| Layer | Policy |
|---|---|
| RPC call | Failover across endpoints, backoff 100ms → 2s, 3 attempts; failover restricted by idempotency class |
| Watcher tick | Exponential backoff 500ms → 30s, unbounded attempts |
| Confirmation pass | Same |
| Outbox relay | Per-row backoff `retry_backoff_base_ms` → `retry_backoff_max_ms`, then dead-letter after `max_delivery_attempts` |
| Event handler | Same budget; a `Rejected` outcome dead-letters immediately without retrying |
| Database transaction | `Db::transaction` retries the whole closure on serialization failure or deadlock |
| Withdrawal broadcast | Recovery path only, never a blind retry |

All backoff uses **jitter** (±50% by default), because synchronised retries across
replicas are the usual cause of a dependency never recovering.
Tested: `jitter_stays_within_the_window`, `grows_exponentially_and_saturates`.

## Recovery matrix

| Failure | Automatic? | Data loss | Operator action |
|---|---|---|---|
| RPC endpoint down | Yes | None | None if another endpoint is healthy |
| All RPC endpoints down | Yes, on return | None | Add/fix an endpoint |
| Kafka down | Yes | None | None |
| Postgres down | Yes | None | Restore the database |
| Redis down | Yes | None | None |
| Worker crash | Yes | None | None |
| API crash | Yes | None | None |
| Crash mid-broadcast | Yes | None | None |
| Reorg, uncredited | Yes | None | None |
| Reorg, credited, funds held | Yes | None | Review confirmation policy |
| Reorg, credited, funds spent | Partial | **Realised loss** | Business decision on the deficit |
| Reorg deeper than scan depth | **No** | Unknown | Manual reconciliation |
| Surviving tx after reorg | **No** | None | Re-validate the deposit |
| Dropped broadcast tx | **No** | None | Fee-bump or cancel via nonce replacement |
| Address pool empty | **No** | None | Export more addresses from custody |
| Ledger integrity violation | **No** | Unknown | Halt withdrawals, reconcile from entries |
