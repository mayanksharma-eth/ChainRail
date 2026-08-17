//! End-to-end deposit lifecycle:
//! observed -> confirming -> confirmed -> credited -> balance updated.
//!
//! The watcher, confirmation engine and credit handler all run for real; only
//! the chain is in-memory.

use std::sync::Arc;
use std::time::Duration;

use chainrail_common::Amount;
use chainrail_database::models::{AccountType, DepositStatus};
use chainrail_deposits::{ChainContext, ConfirmationEngine, DepositCreditHandler, Tick, Watcher};
use chainrail_integration::*;

const USDC: &str = "0x036cbd53842c5426634e7929541ec2318f3dcf7e";

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

struct Rig {
    chain: Arc<MockChain>,
    watcher: Arc<Watcher>,
    confirmations: Arc<ConfirmationEngine>,
    credit: Arc<DepositCreditHandler>,
    cfg: chainrail_common::config::AppConfig,
}

async fn rig(db: &chainrail_database::Db) -> Rig {
    let cfg = test_app_config();
    let chain_cfg = cfg.chains[0].clone();
    let chain = MockChain::new(TEST_CHAIN);
    let ctx = ChainContext::new(db.clone(), &chain_cfg, Duration::from_millis(0));
    ctx.refresh().await.unwrap();

    Rig {
        watcher: Arc::new(Watcher::new(
            db.clone(),
            chain.clone() as Arc<dyn chainrail_chains_evm::ChainAdapter>,
            ctx,
            &chain_cfg,
        )),
        confirmations: Arc::new(ConfirmationEngine::new(
            db.clone(),
            chain.clone() as Arc<dyn chainrail_chains_evm::ChainAdapter>,
            TEST_CHAIN,
            chain_cfg.finality.clone(),
            Duration::from_millis(10),
        )),
        credit: DepositCreditHandler::new(db.clone()),
        chain,
        cfg,
    }
}

/// Run the watcher until it has caught up with the chain head.
async fn catch_up(watcher: &Arc<Watcher>) {
    for _ in 0..50 {
        match watcher.tick().await.expect("watcher tick") {
            Tick::Idle { .. } => return,
            _ => continue,
        }
    }
    panic!("watcher never caught up");
}

#[tokio::test]
async fn deposit_flows_from_detection_to_credited_balance() {
    let db = require_db!();
    let f = fixture(&db).await;
    let deposit_addr = address(0x11);
    register_deposit_address(&db, f.user_id, &deposit_addr).await;

    let r = rig(&db).await;

    // A block containing a 100 USDC transfer to the tracked address.
    r.chain
        .mine("a", vec![transfer("dep-1", &deposit_addr, 100_000_000)]);
    catch_up(&r.watcher).await;

    // 1. observed
    let deposits = chainrail_database::repo::deposits::list_deposits(
        db.pool(),
        Some(f.user_id),
        None,
        None,
        10,
    )
    .await
    .unwrap();
    assert_eq!(deposits.len(), 1, "deposit must be detected");
    let deposit_id = deposits[0].id;
    assert_eq!(deposits[0].status().unwrap(), DepositStatus::Observed);
    assert_eq!(deposits[0].amount_raw, Amount::new(100_000_000));
    // Not credited yet: nothing spendable.
    assert_eq!(
        available_balance(&db, f.user_id, f.asset_id).await,
        Amount::ZERO
    );

    // 2. confirming -- below the 3-confirmation threshold
    let pass = r.confirmations.run_once().await.unwrap();
    assert_eq!(pass.advanced_to_confirming, 1);
    let d = chainrail_database::repo::deposits::get_deposit(db.pool(), deposit_id)
        .await
        .unwrap();
    assert_eq!(d.status().unwrap(), DepositStatus::Confirming);
    assert_eq!(
        available_balance(&db, f.user_id, f.asset_id).await,
        Amount::ZERO
    );

    // 3. confirmed, once the head is 3 blocks past the including block
    r.chain.mine_empty(2);
    catch_up(&r.watcher).await;
    let pass = r.confirmations.run_once().await.unwrap();
    assert_eq!(pass.advanced_to_confirmed, 1);
    let d = chainrail_database::repo::deposits::get_deposit(db.pool(), deposit_id)
        .await
        .unwrap();
    assert_eq!(d.status().unwrap(), DepositStatus::Confirmed);
    // Still not credited: confirmation and crediting are separate stages.
    assert_eq!(
        available_balance(&db, f.user_id, f.asset_id).await,
        Amount::ZERO
    );

    // 4. credited
    let outcome = r.credit.credit(deposit_id, "test-corr").await.unwrap();
    assert_eq!(outcome, chainrail_deposits::CreditOutcome::Credited);
    let d = chainrail_database::repo::deposits::get_deposit(db.pool(), deposit_id)
        .await
        .unwrap();
    assert_eq!(d.status().unwrap(), DepositStatus::Credited);
    assert!(d.ledger_transaction_id.is_some());
    assert!(d.credited_at.is_some());

    // 5. balance updated, and custody reflects that we hold the coins
    assert_eq!(
        available_balance(&db, f.user_id, f.asset_id).await,
        Amount::new(100_000_000)
    );
    assert_eq!(
        system_balance(&db, AccountType::ExchangeCustody, f.asset_id).await,
        Amount::new(100_000_000)
    );
    assert!(chainrail_ledger::verify_ledger_integrity(db.pool())
        .await
        .unwrap()
        .is_clean());

    // 6. the whole chain of events reached the outbox
    let bus = drain_outbox(&db, &r.cfg).await;
    let counts = bus.counts_by_type();
    assert!(counts.get("deposit.observed").copied().unwrap_or(0) >= 1);
    assert!(counts.get("deposit.confirmed").copied().unwrap_or(0) >= 1);
    assert!(counts.get("deposit.credited").copied().unwrap_or(0) >= 1);
    assert!(counts.get("chain.block_observed").copied().unwrap_or(0) >= 3);
}

#[tokio::test]
async fn transfers_to_untracked_addresses_are_ignored() {
    let db = require_db!();
    let f = fixture(&db).await;
    register_deposit_address(&db, f.user_id, &address(0x11)).await;
    let r = rig(&db).await;

    // A transfer to somebody else's address entirely.
    r.chain
        .mine("a", vec![transfer("stranger", &address(0x99), 500_000)]);
    catch_up(&r.watcher).await;

    let deposits =
        chainrail_database::repo::deposits::list_deposits(db.pool(), None, None, None, 10)
            .await
            .unwrap();
    assert!(
        deposits.is_empty(),
        "must not create deposits for untracked addresses"
    );
    // The block itself is still recorded: reorg detection needs unbroken lineage.
    let blocks: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM chain_blocks WHERE chain = $1")
        .bind(TEST_CHAIN)
        .fetch_one(db.pool())
        .await
        .unwrap();
    assert!(blocks >= 1, "block lineage must be recorded regardless");
}

#[tokio::test]
async fn rescanning_the_same_blocks_creates_no_duplicate_deposits() {
    let db = require_db!();
    let f = fixture(&db).await;
    let addr = address(0x11);
    register_deposit_address(&db, f.user_id, &addr).await;
    let r = rig(&db).await;

    r.chain
        .mine("a", vec![transfer("dep-1", &addr, 42_000_000)]);
    catch_up(&r.watcher).await;

    // Rewind the cursor and re-scan, exactly as a crash-restart or a reorg
    // rewind would. Height and hash move together: a cursor whose hash does not
    // match its height is corruption, and the reorg engine correctly refuses to
    // guess at it rather than silently rewinding.
    let genesis = r.chain.hash_at(0).expect("genesis");
    sqlx::query(
        "UPDATE chain_cursors SET last_processed_height = 0, last_processed_hash = $2
          WHERE chain = $1",
    )
    .bind(TEST_CHAIN)
    .bind(&genesis)
    .execute(db.pool())
    .await
    .unwrap();
    catch_up(&r.watcher).await;
    catch_up(&r.watcher).await;

    let deposits =
        chainrail_database::repo::deposits::list_deposits(db.pool(), None, None, None, 10)
            .await
            .unwrap();
    assert_eq!(
        deposits.len(),
        1,
        "re-scanning must not duplicate a deposit"
    );
    let transfers: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM blockchain_transactions")
        .fetch_one(db.pool())
        .await
        .unwrap();
    assert_eq!(transfers, 1);
}

#[tokio::test]
async fn crediting_is_idempotent_across_repeated_delivery() {
    let db = require_db!();
    let f = fixture(&db).await;
    let addr = address(0x11);
    register_deposit_address(&db, f.user_id, &addr).await;
    let r = rig(&db).await;

    r.chain.mine("a", vec![transfer("dep-1", &addr, 7_000_000)]);
    r.chain.mine_empty(3);
    catch_up(&r.watcher).await;
    r.confirmations.run_once().await.unwrap();

    let deposits =
        chainrail_database::repo::deposits::list_deposits(db.pool(), None, None, None, 10)
            .await
            .unwrap();
    let deposit_id = deposits[0].id;

    // Deliver the credit instruction five times, as an at-least-once broker
    // eventually will.
    assert_eq!(
        r.credit.credit(deposit_id, "c").await.unwrap(),
        chainrail_deposits::CreditOutcome::Credited
    );
    for _ in 0..4 {
        assert_eq!(
            r.credit.credit(deposit_id, "c").await.unwrap(),
            chainrail_deposits::CreditOutcome::AlreadyCredited
        );
    }

    assert_eq!(
        available_balance(&db, f.user_id, f.asset_id).await,
        Amount::new(7_000_000),
        "credited exactly once"
    );
    let entries: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM ledger_entries")
        .fetch_one(db.pool())
        .await
        .unwrap();
    assert_eq!(entries, 2, "one balanced pair of entries, not five");
}

#[tokio::test]
async fn unconfirmed_deposits_are_never_credited() {
    let db = require_db!();
    let f = fixture(&db).await;
    let addr = address(0x11);
    register_deposit_address(&db, f.user_id, &addr).await;
    let r = rig(&db).await;

    r.chain.mine("a", vec![transfer("dep-1", &addr, 1_000)]);
    catch_up(&r.watcher).await;
    r.confirmations.run_once().await.unwrap(); // -> confirming only

    let deposits =
        chainrail_database::repo::deposits::list_deposits(db.pool(), None, None, None, 10)
            .await
            .unwrap();
    let deposit_id = deposits[0].id;

    // A credit instruction that arrives early must be refused, not honoured.
    let err = r.credit.credit(deposit_id, "c").await.unwrap_err();
    assert!(
        matches!(err, chainrail_common::Error::Conflict(_)),
        "expected a retryable conflict, got {err:?}"
    );
    assert_eq!(
        available_balance(&db, f.user_id, f.asset_id).await,
        Amount::ZERO
    );
}

#[tokio::test]
async fn multiple_transfers_in_one_transaction_are_distinct_deposits() {
    let db = require_db!();
    let f = fixture(&db).await;
    let addr = address(0x11);
    register_deposit_address(&db, f.user_id, &addr).await;
    let r = rig(&db).await;

    // Same tx hash, different log indexes -- a batch transfer contract.
    let mut a = transfer("batch", &addr, 1_000_000);
    a.log_index = 0;
    let mut b = transfer("batch", &addr, 2_000_000);
    b.log_index = 1;
    r.chain.mine("a", vec![a, b]);
    catch_up(&r.watcher).await;

    let deposits =
        chainrail_database::repo::deposits::list_deposits(db.pool(), None, None, None, 10)
            .await
            .unwrap();
    assert_eq!(deposits.len(), 2, "log_index must disambiguate");
    let total: i128 = deposits.iter().map(|d| d.amount_raw.raw()).sum();
    assert_eq!(total, 3_000_000);
}

#[tokio::test]
async fn watcher_initialises_its_cursor_without_scanning_history() {
    let db = require_db!();
    let _f = fixture(&db).await;
    let r = rig(&db).await;
    r.chain.mine_empty(20);

    // Cold start: start_block = 1 in the test config, so the cursor lands at 0.
    match r.watcher.tick().await.unwrap() {
        Tick::Initialised { at } => assert_eq!(at, 0),
        other => panic!("expected initialisation, got {other:?}"),
    }
    let cursor = chainrail_database::repo::chain::get_cursor(db.pool(), TEST_CHAIN)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(cursor.last_processed_height, 0);
}

#[tokio::test]
async fn watcher_advances_in_bounded_batches_and_never_skips_a_block() {
    let db = require_db!();
    let f = fixture(&db).await;
    let addr = address(0x11);
    register_deposit_address(&db, f.user_id, &addr).await;
    let r = rig(&db).await;

    // 25 blocks, batch size 10 -> at least three scanning ticks.
    r.chain.mine_empty(25);
    let mut scanned_ranges = Vec::new();
    for _ in 0..20 {
        match r.watcher.tick().await.unwrap() {
            Tick::Scanned { from, to, .. } => scanned_ranges.push((from, to)),
            Tick::Idle { .. } => break,
            Tick::Initialised { .. } => continue,
            Tick::Reorged(_) => panic!("unexpected reorg"),
        }
    }
    assert!(
        scanned_ranges.len() >= 3,
        "expected batching: {scanned_ranges:?}"
    );

    // Every height must be recorded exactly once, with no gaps. Height 0 is the
    // anchor written at cursor initialisation, so the run starts there.
    let heights: Vec<i64> = sqlx::query_scalar(
        "SELECT height FROM chain_blocks WHERE chain = $1 AND status = 'canonical' ORDER BY height",
    )
    .bind(TEST_CHAIN)
    .fetch_all(db.pool())
    .await
    .unwrap();
    assert_eq!(
        heights,
        (0..=25).collect::<Vec<i64>>(),
        "no gaps, no repeats"
    );
}

#[tokio::test]
async fn disabled_assets_are_not_credited() {
    let db = require_db!();
    let f = fixture(&db).await;
    let addr = address(0x11);
    register_deposit_address(&db, f.user_id, &addr).await;
    let r = rig(&db).await;

    // A transfer of a token we do not track at all.
    let mut t = transfer("unknown-token", &addr, 1_000_000);
    t.contract = Some(chainrail_common::Address::from_storage(
        "0x1111111111111111111111111111111111111111",
    ));
    r.chain.mine("a", vec![t]);
    catch_up(&r.watcher).await;

    let deposits =
        chainrail_database::repo::deposits::list_deposits(db.pool(), None, None, None, 10)
            .await
            .unwrap();
    assert!(deposits.is_empty(), "untracked assets must be ignored");
}
