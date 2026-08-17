//! Failure injection.
//!
//! Each test breaks one dependency and asserts that the system either keeps
//! working or stops safely — never that it loses or duplicates money.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use chainrail_chains_evm::ChainAdapter;
use chainrail_common::event::{DepositConfirmed, EventPayload};
use chainrail_common::{Amount, EventEnvelope};
use chainrail_database::models::{AccountType, WithdrawalStatus};
use chainrail_deposits::{ChainContext, ConfirmationEngine, DepositCreditHandler, Tick, Watcher};
use chainrail_events::{EventHandler, HandlerOutcome, OutboxRelay};
use chainrail_integration::*;
use chainrail_risk::RiskEngine;
use chainrail_withdrawals::{WithdrawalPipeline, WithdrawalRequest, WithdrawalService};
use uuid::Uuid;

const USDC: &str = "0x036cbd53842c5426634e7929541ec2318f3dcf7e";
const DEST: &str = "0x70997970C51812dc3A010C7d01b50e0d17dc79C8";

fn transfer(label: &str, to: &chainrail_common::Address, amount: i128) -> MockTransfer {
    MockTransfer {
        tx_hash: tx_hash(label),
        log_index: 0,
        from: address(0xaa),
        to: to.clone(),
        amount: Amount::new(amount),
        contract: Some(chainrail_common::Address::from_storage(USDC)),
    }
}

// ------------------------------------------------------------------- RPC ---

#[tokio::test]
async fn an_rpc_outage_stalls_the_watcher_and_it_resumes_cleanly() {
    let db = require_db!();
    let f = fixture(&db).await;
    let addr = address(0x11);
    register_deposit_address(&db, f.user_id, &addr).await;

    let cfg = test_app_config();
    let chain_cfg = cfg.chains[0].clone();
    let chain = MockChain::new(TEST_CHAIN);
    let ctx = ChainContext::new(db.clone(), &chain_cfg, Duration::from_millis(0));
    ctx.refresh().await.unwrap();
    let watcher = Arc::new(Watcher::new(
        db.clone(),
        chain.clone() as Arc<dyn ChainAdapter>,
        ctx,
        &chain_cfg,
    ));

    chain.mine("a1", vec![transfer("before-outage", &addr, 10_000_000)]);
    while !matches!(watcher.tick().await.unwrap(), Tick::Idle { .. }) {}
    let cursor_before = chainrail_database::repo::chain::get_cursor(db.pool(), TEST_CHAIN)
        .await
        .unwrap()
        .unwrap()
        .last_processed_height;

    // The provider goes down while new blocks are produced.
    chain.set_failing(Some("connection refused"));
    chain.mine("a2", vec![transfer("during-outage", &addr, 20_000_000)]);

    for _ in 0..3 {
        let err = watcher
            .tick()
            .await
            .expect_err("tick must fail while RPC is down");
        assert!(
            err.is_retryable(),
            "an RPC outage must be classified retryable"
        );
    }
    // The cursor did not move, so nothing was skipped.
    let cursor_during = chainrail_database::repo::chain::get_cursor(db.pool(), TEST_CHAIN)
        .await
        .unwrap()
        .unwrap()
        .last_processed_height;
    assert_eq!(
        cursor_during, cursor_before,
        "cursor must not advance blindly"
    );

    // Recovery: the block produced during the outage is picked up.
    chain.set_failing(None);
    while !matches!(watcher.tick().await.unwrap(), Tick::Idle { .. }) {}

    let deposits =
        chainrail_database::repo::deposits::list_deposits(db.pool(), None, None, None, 10)
            .await
            .unwrap();
    assert_eq!(
        deposits.len(),
        2,
        "the deposit during the outage must not be lost"
    );
    assert!(chainrail_ledger::verify_ledger_integrity(db.pool())
        .await
        .unwrap()
        .is_clean());
}

// ----------------------------------------------------------------- Kafka ---

#[tokio::test]
async fn a_broker_outage_does_not_lose_events_and_the_outbox_drains_after() {
    let db = require_db!();
    let f = fixture(&db).await;
    let addr = address(0x11);
    register_deposit_address(&db, f.user_id, &addr).await;

    let cfg = test_app_config();
    let chain_cfg = cfg.chains[0].clone();
    let chain = MockChain::new(TEST_CHAIN);
    let ctx = ChainContext::new(db.clone(), &chain_cfg, Duration::from_millis(0));
    ctx.refresh().await.unwrap();
    let watcher = Arc::new(Watcher::new(
        db.clone(),
        chain.clone() as Arc<dyn ChainAdapter>,
        ctx,
        &chain_cfg,
    ));

    chain.mine("a1", vec![transfer("dep", &addr, 5_000_000)]);
    while !matches!(watcher.tick().await.unwrap(), Tick::Idle { .. }) {}

    // The business state committed regardless of the broker -- that is the whole
    // point of the outbox.
    let pending = chainrail_database::repo::outbox::pending_count(db.pool())
        .await
        .unwrap();
    assert!(pending > 0, "events must be durably queued");
    let deposits =
        chainrail_database::repo::deposits::list_deposits(db.pool(), None, None, None, 10)
            .await
            .unwrap();
    assert_eq!(
        deposits.len(),
        1,
        "the deposit exists even though nothing was published"
    );

    // Broker down: the relay makes no progress but loses nothing.
    let bus = chainrail_events::InMemoryEventBus::new();
    bus.set_failing(Some("all brokers down"));
    let relay = Arc::new(OutboxRelay::new(db.clone(), bus.clone(), &cfg.kafka));
    let pass = relay.run_once().await.unwrap();
    assert_eq!(pass.published, 0);
    assert!(pass.failed > 0, "failures must be recorded for retry");
    assert!(bus.is_empty());
    assert_eq!(
        chainrail_database::repo::outbox::pending_count(db.pool())
            .await
            .unwrap(),
        pending,
        "nothing may be marked published"
    );

    // Broker back: the backlog drains. Retries are scheduled with backoff, so
    // clear the schedule to avoid sleeping in a test.
    bus.set_failing(None);
    sqlx::query("UPDATE outbox SET next_attempt_at = now() WHERE published_at IS NULL")
        .execute(db.pool())
        .await
        .unwrap();
    for _ in 0..20 {
        if !relay.run_once().await.unwrap().did_work() {
            break;
        }
    }
    assert_eq!(
        chainrail_database::repo::outbox::pending_count(db.pool())
            .await
            .unwrap(),
        0,
        "the backlog must drain once the broker returns"
    );
    assert!(bus.len() >= pending as usize);
}

#[tokio::test]
async fn events_exceeding_their_retry_budget_are_dead_lettered_not_retried_forever() {
    let db = require_db!();
    let f = fixture(&db).await;
    let addr = address(0x11);
    register_deposit_address(&db, f.user_id, &addr).await;

    let mut cfg = test_app_config();
    cfg.kafka.max_delivery_attempts = 2;
    let chain_cfg = cfg.chains[0].clone();
    let chain = MockChain::new(TEST_CHAIN);
    let ctx = ChainContext::new(db.clone(), &chain_cfg, Duration::from_millis(0));
    ctx.refresh().await.unwrap();
    let watcher = Arc::new(Watcher::new(
        db.clone(),
        chain.clone() as Arc<dyn ChainAdapter>,
        ctx,
        &chain_cfg,
    ));
    chain.mine("a1", vec![transfer("dep", &addr, 1_000).clone()]);
    while !matches!(watcher.tick().await.unwrap(), Tick::Idle { .. }) {}

    let bus = chainrail_events::InMemoryEventBus::new();
    bus.set_failing(Some("permanently broken topic"));
    let relay = Arc::new(OutboxRelay::new(db.clone(), bus.clone(), &cfg.kafka));

    for _ in 0..6 {
        sqlx::query("UPDATE outbox SET next_attempt_at = now() WHERE published_at IS NULL")
            .execute(db.pool())
            .await
            .unwrap();
        if !relay.run_once().await.unwrap().did_work() {
            break;
        }
    }

    let dead = chainrail_database::repo::outbox::dead_letter_count(db.pool())
        .await
        .unwrap();
    assert!(dead > 0, "events must eventually be dead-lettered");
    assert_eq!(
        chainrail_database::repo::outbox::pending_count(db.pool())
            .await
            .unwrap(),
        0,
        "the queue head must not stay blocked forever"
    );
}

#[tokio::test]
async fn a_duplicate_event_delivery_is_processed_exactly_once() {
    let db = require_db!();
    let f = fixture(&db).await;

    // Stage a confirmed deposit directly, then deliver its credit event twice.
    let addr = address(0x11);
    register_deposit_address(&db, f.user_id, &addr).await;
    let cfg = test_app_config();
    let chain_cfg = cfg.chains[0].clone();
    let chain = MockChain::new(TEST_CHAIN);
    let ctx = ChainContext::new(db.clone(), &chain_cfg, Duration::from_millis(0));
    ctx.refresh().await.unwrap();
    let watcher = Arc::new(Watcher::new(
        db.clone(),
        chain.clone() as Arc<dyn ChainAdapter>,
        ctx,
        &chain_cfg,
    ));
    let confirmations = ConfirmationEngine::new(
        db.clone(),
        chain.clone() as Arc<dyn ChainAdapter>,
        TEST_CHAIN,
        chain_cfg.finality.clone(),
        Duration::from_millis(10),
    );

    chain.mine("a1", vec![transfer("dup-dep", &addr, 33_000_000)]);
    chain.mine_empty(3);
    while !matches!(watcher.tick().await.unwrap(), Tick::Idle { .. }) {}
    confirmations.run_once().await.unwrap();

    let deposits =
        chainrail_database::repo::deposits::list_deposits(db.pool(), None, None, None, 10)
            .await
            .unwrap();
    let deposit_id = deposits[0].id;

    let handler = DepositCreditHandler::new(db.clone());
    // One envelope, delivered repeatedly -- identical event_id each time, which
    // is exactly what a Kafka rebalance produces.
    let envelope = EventEnvelope::new(
        EventPayload::DepositConfirmed(DepositConfirmed {
            chain: TEST_CHAIN.into(),
            deposit_id,
            user_id: f.user_id,
            tx_hash: tx_hash("dup-dep").to_string(),
            block_number: 1,
            confirmations: 3,
            asset: "USDC".into(),
            amount_raw: Amount::new(33_000_000),
        }),
        "dup-corr",
    )
    .with_deterministic_id(&format!("{TEST_CHAIN}:{deposit_id}"));

    for i in 0..5 {
        match handler.handle(&envelope).await.unwrap() {
            HandlerOutcome::Done => {}
            HandlerOutcome::Rejected(r) => panic!("delivery {i} was rejected: {r}"),
        }
    }

    assert_eq!(
        available_balance(&db, f.user_id, f.asset_id).await,
        Amount::new(33_000_000),
        "credited exactly once across five deliveries"
    );
    let entries: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM ledger_entries")
        .fetch_one(db.pool())
        .await
        .unwrap();
    assert_eq!(entries, 2);
    // The dedupe record exists so a later redelivery short-circuits.
    assert!(chainrail_database::repo::outbox::was_processed(
        db.pool(),
        "deposit_credit",
        envelope.event_id
    )
    .await
    .unwrap());
}

// ------------------------------------------------- withdrawal crash recovery ---

/// The critical case: the process dies *after* broadcasting but *before*
/// recording it. The withdrawal is left in `signing` with a durable tx hash.
#[tokio::test]
async fn a_crash_after_broadcast_is_reconciled_from_the_chain_not_re_sent() {
    let db = require_db!();
    let f = fixture(&db).await;
    fund_custody(&db, f.asset_id, 1_000_000_000).await;
    fund_custody(&db, f.native_asset_id, 1_000_000_000_000_000_000).await;
    fund(&db, f.user_id, f.asset_id, 100_000_000).await;

    let cfg = Arc::new(test_app_config());
    let chain = MockChain::new(TEST_CHAIN);
    chain.mine_empty(1);
    let risk = Arc::new(RiskEngine::new(cfg.risk.clone()));
    let signer = chainrail_signer::from_config(&cfg.signer).unwrap();
    let mut adapters: HashMap<String, Arc<dyn ChainAdapter>> = HashMap::new();
    adapters.insert(
        TEST_CHAIN.to_string(),
        chain.clone() as Arc<dyn ChainAdapter>,
    );

    let service = WithdrawalService::new(db.clone(), Arc::clone(&cfg), risk);
    let pipeline = WithdrawalPipeline::new(
        db.clone(),
        Arc::clone(&cfg),
        signer,
        adapters,
        Duration::from_millis(10),
    );

    let created = service
        .create(WithdrawalRequest {
            user_id: f.user_id,
            chain: TEST_CHAIN.into(),
            asset_symbol: "USDC".into(),
            amount: Amount::new(20_000_000),
            destination: DEST.into(),
            idempotency_key: "crash-test-001".into(),
            correlation_id: None,
        })
        .await
        .unwrap();
    let id = created.withdrawal().id;

    pipeline.process_approved(TEST_CHAIN).await.unwrap();
    let w = chainrail_database::repo::withdrawals::get_withdrawal(db.pool(), id)
        .await
        .unwrap();
    let tx_hash = w.tx_hash.clone().unwrap();
    let nonce = w.nonce.unwrap();
    assert_eq!(chain.broadcasts().len(), 1);

    // Rewind the withdrawal to the exact post-crash state: broadcast happened,
    // the database write did not. `broadcast -> signing` is not a legal
    // transition, so the trigger is bypassed deliberately to stage it.
    sqlx::query("ALTER TABLE withdrawals DISABLE TRIGGER withdrawals_guard_transition")
        .execute(db.pool())
        .await
        .unwrap();
    sqlx::query("UPDATE withdrawals SET status = 'signing', broadcast_at = NULL WHERE id = $1")
        .bind(id)
        .execute(db.pool())
        .await
        .unwrap();
    sqlx::query("ALTER TABLE withdrawals ENABLE TRIGGER withdrawals_guard_transition")
        .execute(db.pool())
        .await
        .unwrap();

    // Recovery finds the transaction on chain and moves forward, rather than
    // re-signing with a new nonce and sending the funds twice.
    let recovered = pipeline.recover_signing(TEST_CHAIN).await.unwrap();
    assert_eq!(recovered, 1);
    let w = chainrail_database::repo::withdrawals::get_withdrawal(db.pool(), id)
        .await
        .unwrap();
    assert_eq!(w.status().unwrap(), WithdrawalStatus::Broadcast);
    assert_eq!(
        w.tx_hash,
        Some(tx_hash),
        "the same transaction, not a new one"
    );
    assert_eq!(w.nonce, Some(nonce), "the nonce must not be re-allocated");
    assert_eq!(
        chain.broadcasts().len(),
        1,
        "recovery must not put a second transaction on the wire"
    );
    assert!(chainrail_ledger::verify_ledger_integrity(db.pool())
        .await
        .unwrap()
        .is_clean());
}

/// The other half: the signed transaction never reached the network. Because
/// signing is deterministic and the nonce is read back from the database, the
/// retry reproduces the identical transaction.
#[tokio::test]
async fn a_broadcast_that_never_landed_is_re_sent_as_the_same_transaction() {
    let db = require_db!();
    let f = fixture(&db).await;
    fund_custody(&db, f.asset_id, 1_000_000_000).await;
    fund(&db, f.user_id, f.asset_id, 100_000_000).await;

    let cfg = Arc::new(test_app_config());
    let chain = MockChain::new(TEST_CHAIN);
    chain.mine_empty(1);
    let risk = Arc::new(RiskEngine::new(cfg.risk.clone()));
    let signer = chainrail_signer::from_config(&cfg.signer).unwrap();
    let mut adapters: HashMap<String, Arc<dyn ChainAdapter>> = HashMap::new();
    adapters.insert(
        TEST_CHAIN.to_string(),
        chain.clone() as Arc<dyn ChainAdapter>,
    );
    let service = WithdrawalService::new(db.clone(), Arc::clone(&cfg), risk);
    let pipeline = WithdrawalPipeline::new(
        db.clone(),
        Arc::clone(&cfg),
        signer,
        adapters,
        Duration::from_millis(10),
    );

    let created = service
        .create(WithdrawalRequest {
            user_id: f.user_id,
            chain: TEST_CHAIN.into(),
            asset_symbol: "USDC".into(),
            amount: Amount::new(15_000_000),
            destination: DEST.into(),
            idempotency_key: "lost-bcast-001".into(),
            correlation_id: None,
        })
        .await
        .unwrap();
    let id = created.withdrawal().id;

    // Stage: signed and hash persisted, but the broadcast never happened.
    let mut tx = db.begin().await.unwrap();
    let w = chainrail_database::repo::withdrawals::lock_withdrawal(&mut tx, id)
        .await
        .unwrap();
    chainrail_database::repo::withdrawals::transition(
        &mut tx,
        id,
        WithdrawalStatus::Approved,
        WithdrawalStatus::Signing,
    )
    .await
    .unwrap();
    let phantom = tx_hash("never-sent");
    chainrail_database::repo::withdrawals::record_signed_transaction(
        &mut tx,
        id,
        chainrail_database::repo::withdrawals::SignedTxFacts {
            tx_hash: &phantom,
            nonce: 7,
            gas_limit: 100_000,
            max_fee_per_gas: Amount::new(2_000_000_000),
            max_priority_fee_per_gas: Amount::new(1_000_000_000),
        },
    )
    .await
    .unwrap();
    tx.commit().await.unwrap();
    let _ = w;

    assert!(chain.broadcasts().is_empty());
    let recovered = pipeline.recover_signing(TEST_CHAIN).await.unwrap();
    assert_eq!(recovered, 1);

    let w = chainrail_database::repo::withdrawals::get_withdrawal(db.pool(), id)
        .await
        .unwrap();
    assert_eq!(w.status().unwrap(), WithdrawalStatus::Broadcast);
    assert_eq!(w.nonce, Some(7), "the recorded nonce must be reused");
    assert_eq!(chain.broadcasts().len(), 1, "sent exactly once");
    assert!(chainrail_ledger::verify_ledger_integrity(db.pool())
        .await
        .unwrap()
        .is_clean());
}

#[tokio::test]
async fn a_dropped_transaction_is_monitored_rather_than_failed() {
    let db = require_db!();
    let f = fixture(&db).await;
    fund_custody(&db, f.asset_id, 1_000_000_000).await;
    fund(&db, f.user_id, f.asset_id, 100_000_000).await;

    let cfg = Arc::new(test_app_config());
    let chain = MockChain::new(TEST_CHAIN);
    chain.mine_empty(1);
    let risk = Arc::new(RiskEngine::new(cfg.risk.clone()));
    let signer = chainrail_signer::from_config(&cfg.signer).unwrap();
    let mut adapters: HashMap<String, Arc<dyn ChainAdapter>> = HashMap::new();
    adapters.insert(
        TEST_CHAIN.to_string(),
        chain.clone() as Arc<dyn ChainAdapter>,
    );
    let service = WithdrawalService::new(db.clone(), Arc::clone(&cfg), risk);
    let pipeline = WithdrawalPipeline::new(
        db.clone(),
        Arc::clone(&cfg),
        signer,
        adapters,
        Duration::from_millis(10),
    );

    let created = service
        .create(WithdrawalRequest {
            user_id: f.user_id,
            chain: TEST_CHAIN.into(),
            asset_symbol: "USDC".into(),
            amount: Amount::new(5_000_000),
            destination: DEST.into(),
            idempotency_key: "dropped-tx-001".into(),
            correlation_id: None,
        })
        .await
        .unwrap();
    let id = created.withdrawal().id;
    pipeline.process_approved(TEST_CHAIN).await.unwrap();

    let w = chainrail_database::repo::withdrawals::get_withdrawal(db.pool(), id)
        .await
        .unwrap();
    let tx_hash = w.tx_hash.clone().unwrap();
    // The node forgets the transaction (mempool eviction).
    chain.set_tx_status(&tx_hash, chainrail_chains_evm::TxStatus::Unknown);

    let pass = pipeline.process_in_flight(TEST_CHAIN).await.unwrap();
    // Deliberately NOT failed: the nonce may still be consumed by this exact
    // transaction later, and releasing the funds now could double-spend.
    assert_eq!(pass.failed, 0);
    assert_eq!(pass.still_pending, 1);
    assert_eq!(status_of(&db, id).await, WithdrawalStatus::Broadcast);
    assert_eq!(
        balance_of(&db, AccountType::UserReserved, f.user_id, f.asset_id).await,
        Amount::new(5_000_000),
        "funds stay reserved while the outcome is unknown"
    );
}

async fn status_of(db: &chainrail_database::Db, id: Uuid) -> WithdrawalStatus {
    chainrail_database::repo::withdrawals::get_withdrawal(db.pool(), id)
        .await
        .unwrap()
        .status()
        .unwrap()
}

// ------------------------------------------------------------- state machine ---

/// The Rust state machine and the database trigger must agree exactly. If they
/// drift, a worker could attempt a transition Rust allows and Postgres rejects,
/// crashing mid-pipeline.
///
/// Isolates the *transition* guard: the `completed`/`failed` consistency CHECKs
/// are satisfied for every staged state so that any error we see is the guard.
#[tokio::test]
async fn the_rust_state_machine_matches_the_database_trigger() {
    let db = require_db!();
    let f = fixture(&db).await;
    // A real ledger transaction to satisfy withdrawals_completed_consistency.
    fund_custody(&db, f.asset_id, 1_000).await;
    let settle_id: Uuid = sqlx::query_scalar("SELECT id FROM ledger_transactions LIMIT 1")
        .fetch_one(db.pool())
        .await
        .unwrap();

    let all = [
        WithdrawalStatus::Requested,
        WithdrawalStatus::Validated,
        WithdrawalStatus::Approved,
        WithdrawalStatus::Signing,
        WithdrawalStatus::Broadcast,
        WithdrawalStatus::Confirming,
        WithdrawalStatus::Completed,
        WithdrawalStatus::Failed,
        WithdrawalStatus::Cancelled,
    ];

    /// Columns that must accompany a given status for the row-level CHECKs to hold.
    fn consistency_sql(status: WithdrawalStatus) -> &'static str {
        match status {
            WithdrawalStatus::Completed => {
                "settle_ledger_transaction_id = $3, completed_at = now(), failure_code = NULL"
            }
            WithdrawalStatus::Failed => {
                "settle_ledger_transaction_id = NULL, completed_at = NULL, failure_code = 'test'"
            }
            _ => "settle_ledger_transaction_id = NULL, completed_at = NULL, failure_code = NULL",
        }
    }

    let mut checked = 0usize;
    for from in all {
        for to in all {
            if from == to {
                continue;
            }
            let rust_allows = from.can_transition_to(to);

            let id = Uuid::new_v4();
            sqlx::query(
                "INSERT INTO withdrawals
                    (id, user_id, asset_id, chain, destination_address, amount_raw,
                     idempotency_key, request_fingerprint, status, tx_hash)
                 VALUES ($1, $2, $3, $4, $5, 1000, $6, 'fp', 'requested',
                         '0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa')",
            )
            .bind(id)
            .bind(f.user_id)
            .bind(f.asset_id)
            .bind(TEST_CHAIN)
            .bind("0x70997970c51812dc3a010c7d01b50e0d17dc79c8")
            .bind(format!("sm-{id}"))
            .execute(db.pool())
            .await
            .unwrap();

            // Stage the `from` state with the guard off, so any starting state is
            // reachable regardless of what the machine normally permits.
            sqlx::query("ALTER TABLE withdrawals DISABLE TRIGGER withdrawals_guard_transition")
                .execute(db.pool())
                .await
                .unwrap();
            // AssertSqlSafe: the interpolated fragment comes from a closed set of
            // `&'static str` literals in `consistency_sql`, never from input.
            sqlx::query(sqlx::AssertSqlSafe(format!(
                "UPDATE withdrawals SET status = $2, {} WHERE id = $1",
                consistency_sql(from)
            )))
            .bind(id)
            .bind(from.as_str())
            .bind(settle_id)
            .execute(db.pool())
            .await
            .expect("staging the from-state must succeed");
            sqlx::query("ALTER TABLE withdrawals ENABLE TRIGGER withdrawals_guard_transition")
                .execute(db.pool())
                .await
                .unwrap();

            let result = sqlx::query(sqlx::AssertSqlSafe(format!(
                "UPDATE withdrawals SET status = $2, {} WHERE id = $1",
                consistency_sql(to)
            )))
            .bind(id)
            .bind(to.as_str())
            .bind(settle_id)
            .execute(db.pool())
            .await;

            let db_rejected = match &result {
                Ok(_) => false,
                Err(e) => {
                    let msg = e.to_string();
                    assert!(
                        msg.contains("illegal withdrawal transition"),
                        "{from} -> {to} failed on an unrelated constraint: {msg}"
                    );
                    true
                }
            };

            assert_eq!(
                rust_allows, !db_rejected,
                "disagreement on {from} -> {to}: rust allows {rust_allows}, db rejected {db_rejected}"
            );
            checked += 1;

            sqlx::query("DELETE FROM withdrawals WHERE id = $1")
                .bind(id)
                .execute(db.pool())
                .await
                .ok();
        }
    }
    assert_eq!(
        checked,
        9 * 8,
        "every ordered pair of distinct states must be checked"
    );
}
