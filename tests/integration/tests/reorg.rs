//! Chain reorganisation handling.
//!
//! The scenario from the brief:
//!
//!   100 -> 101 -> 102        (what we indexed)
//!   100 -> 101' -> 102' -> 103'   (what the chain decided)
//!
//! plus the harder cases: a deposit that was already credited when its block is
//! orphaned, and a transaction that survives the reorg in a new block.

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
    }
}

async fn catch_up(watcher: &Arc<Watcher>) {
    for _ in 0..80 {
        match watcher.tick().await.expect("watcher tick") {
            Tick::Idle { .. } => return,
            _ => continue,
        }
    }
    panic!("watcher never caught up");
}

/// Run ticks until idle, returning any reorg reports produced along the way.
async fn catch_up_collecting_reorgs(
    watcher: &Arc<Watcher>,
) -> Vec<chainrail_deposits::ReorgReport> {
    let mut reports = Vec::new();
    for _ in 0..80 {
        match watcher.tick().await.expect("watcher tick") {
            Tick::Idle { .. } => return reports,
            Tick::Reorged(r) => reports.push(r),
            _ => continue,
        }
    }
    panic!("watcher never caught up");
}

async fn canonical_heights(db: &chainrail_database::Db) -> Vec<i64> {
    sqlx::query_scalar(
        "SELECT height FROM chain_blocks WHERE chain = $1 AND status = 'canonical' ORDER BY height",
    )
    .bind(TEST_CHAIN)
    .fetch_all(db.pool())
    .await
    .unwrap()
}

#[tokio::test]
async fn orphaned_blocks_are_demoted_and_the_replacement_chain_is_indexed() {
    let db = require_db!();
    let f = fixture(&db).await;
    let addr = address(0x11);
    register_deposit_address(&db, f.user_id, &addr).await;
    let r = rig(&db).await;

    // Build 1 -> 2 -> 3, with a deposit in block 3.
    r.chain.mine("a1", vec![]);
    r.chain.mine("a2", vec![]);
    r.chain
        .mine("a3", vec![transfer("orphaned-dep", &addr, 50_000_000)]);
    catch_up(&r.watcher).await;

    let before = chainrail_database::repo::deposits::list_deposits(db.pool(), None, None, None, 10)
        .await
        .unwrap();
    assert_eq!(before.len(), 1, "deposit observed on the original chain");
    let deposit_id = before[0].id;
    assert_eq!(canonical_heights(&db).await, vec![0, 1, 2, 3]);

    // The chain forks at height 1: 2' and 3' replace 2 and 3, and 4' extends it.
    // The deposit's transaction is NOT in the replacement chain.
    r.chain
        .reorg_to(1, &["b2", "b3", "b4"], vec![vec![], vec![], vec![]]);

    let reports = catch_up_collecting_reorgs(&r.watcher).await;
    assert_eq!(reports.len(), 1, "exactly one reorg should be reported");
    let report = &reports[0];
    assert_eq!(report.common_ancestor_height, 1, "fork point is height 1");
    assert_eq!(report.orphaned_blocks, 2, "blocks 2 and 3 are orphaned");
    assert_eq!(report.orphaned_transfers, 1);
    assert_eq!(report.deposits_reorged, 1);
    assert_eq!(
        report.credited_deposits_reversed, 0,
        "the deposit was never credited, so nothing to compensate"
    );

    // The uncredited deposit is now `reorged` and can never become credited.
    let d = chainrail_database::repo::deposits::get_deposit(db.pool(), deposit_id)
        .await
        .unwrap();
    assert_eq!(d.status().unwrap(), DepositStatus::Reorged);
    assert!(d.ledger_transaction_id.is_none());
    assert_eq!(
        available_balance(&db, f.user_id, f.asset_id).await,
        Amount::ZERO
    );

    // The replacement chain is fully indexed, with exactly one canonical block
    // per height -- the database's partial unique index guarantees it.
    assert_eq!(canonical_heights(&db).await, vec![0, 1, 2, 3, 4]);
    let orphaned: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM chain_blocks WHERE chain = $1 AND status = 'orphaned'",
    )
    .bind(TEST_CHAIN)
    .fetch_one(db.pool())
    .await
    .unwrap();
    assert_eq!(orphaned, 2);

    // The canonical hashes match the replacement chain, not the abandoned one.
    for h in 2..=4u64 {
        let stored: chainrail_common::Hash32 = sqlx::query_scalar(
            "SELECT hash FROM chain_blocks WHERE chain = $1 AND height = $2 AND status = 'canonical'",
        )
        .bind(TEST_CHAIN)
        .bind(h as i64)
        .fetch_one(db.pool())
        .await
        .unwrap();
        assert_eq!(
            stored,
            r.chain.hash_at(h).unwrap(),
            "height {h} must be the replacement block"
        );
    }
}

#[tokio::test]
async fn a_credited_deposit_orphaned_by_a_deep_reorg_is_compensated_not_deleted() {
    let db = require_db!();
    let f = fixture(&db).await;
    let addr = address(0x11);
    register_deposit_address(&db, f.user_id, &addr).await;
    // The exchange holds its own capital, as in reality; the reversal draws
    // custody down and must not push it negative.
    fund_custody(&db, f.asset_id, 1_000_000_000).await;
    let r = rig(&db).await;

    // Deposit in block 1, then enough blocks to confirm and credit it.
    r.chain
        .mine("a1", vec![transfer("credited-dep", &addr, 80_000_000)]);
    r.chain.mine_empty(3);
    catch_up(&r.watcher).await;
    r.confirmations.run_once().await.unwrap();

    let deposits =
        chainrail_database::repo::deposits::list_deposits(db.pool(), None, None, None, 10)
            .await
            .unwrap();
    let deposit_id = deposits[0].id;
    r.credit.credit(deposit_id, "corr").await.unwrap();
    assert_eq!(
        available_balance(&db, f.user_id, f.asset_id).await,
        Amount::new(80_000_000),
        "funds are spendable"
    );
    let credit_ltx = chainrail_database::repo::deposits::get_deposit(db.pool(), deposit_id)
        .await
        .unwrap()
        .ledger_transaction_id
        .expect("credited");

    // A reorg deeper than the 3-confirmation threshold wipes out block 1.
    r.chain
        .reorg_to(0, &["b1", "b2", "b3", "b4", "b5"], vec![vec![]; 5]);
    let reports = catch_up_collecting_reorgs(&r.watcher).await;
    let report = reports.first().expect("a reorg must be reported");
    assert_eq!(report.common_ancestor_height, 0);
    assert_eq!(
        report.credited_deposits_reversed, 1,
        "the credited deposit must be compensated"
    );

    // The deposit is `reorged`, and a *separate* reversal transaction exists.
    let d = chainrail_database::repo::deposits::get_deposit(db.pool(), deposit_id)
        .await
        .unwrap();
    assert_eq!(d.status().unwrap(), DepositStatus::Reorged);
    let reversal = d.reversal_ledger_transaction_id.expect("reversal posted");
    assert_ne!(reversal, credit_ltx, "must be a new transaction");

    // History is intact: the original credit is still there, unmodified.
    let original = chainrail_ledger::get_transaction(db.pool(), credit_ltx)
        .await
        .unwrap();
    assert_eq!(original.kind, "deposit_credit");
    let reversal_tx = chainrail_ledger::get_transaction(db.pool(), reversal)
        .await
        .unwrap();
    assert_eq!(reversal_tx.kind, "deposit_reversal");
    assert_eq!(reversal_tx.reference_type, "reorg");

    // The funds are gone from the user's balance, and no deficit was needed
    // because they had not been spent.
    assert_eq!(
        available_balance(&db, f.user_id, f.asset_id).await,
        Amount::ZERO
    );
    assert_eq!(
        balance_of(&db, AccountType::UserDeficit, f.user_id, f.asset_id).await,
        Amount::ZERO
    );

    let integrity = chainrail_ledger::verify_ledger_integrity(db.pool())
        .await
        .unwrap();
    assert!(integrity.is_clean(), "{integrity:#?}");
}

#[tokio::test]
async fn a_reorged_deposit_can_never_subsequently_be_credited() {
    let db = require_db!();
    let f = fixture(&db).await;
    let addr = address(0x11);
    register_deposit_address(&db, f.user_id, &addr).await;
    let r = rig(&db).await;

    r.chain
        .mine("a1", vec![transfer("racy-dep", &addr, 30_000_000)]);
    r.chain.mine_empty(3);
    catch_up(&r.watcher).await;
    r.confirmations.run_once().await.unwrap();

    let deposits =
        chainrail_database::repo::deposits::list_deposits(db.pool(), None, None, None, 10)
            .await
            .unwrap();
    let deposit_id = deposits[0].id;
    assert_eq!(deposits[0].status().unwrap(), DepositStatus::Confirmed);

    // The reorg lands *after* the deposit was confirmed but *before* the credit
    // event was processed -- the exact race the design must survive.
    r.chain
        .reorg_to(0, &["b1", "b2", "b3", "b4", "b5"], vec![vec![]; 5]);
    catch_up_collecting_reorgs(&r.watcher).await;

    // A late credit instruction must be refused.
    let outcome = r.credit.credit(deposit_id, "late").await.unwrap();
    assert_eq!(
        outcome,
        chainrail_deposits::CreditOutcome::NoLongerValid(DepositStatus::Reorged)
    );
    assert_eq!(
        available_balance(&db, f.user_id, f.asset_id).await,
        Amount::ZERO
    );
}

#[tokio::test]
async fn a_transaction_surviving_the_reorg_in_a_new_block_is_re_observed_once() {
    let db = require_db!();
    let f = fixture(&db).await;
    let addr = address(0x11);
    register_deposit_address(&db, f.user_id, &addr).await;
    let r = rig(&db).await;

    // The transfer is in block 2 of the original chain.
    r.chain.mine("a1", vec![]);
    r.chain
        .mine("a2", vec![transfer("survivor", &addr, 60_000_000)]);
    catch_up(&r.watcher).await;
    let before = chainrail_database::repo::deposits::list_deposits(db.pool(), None, None, None, 10)
        .await
        .unwrap();
    assert_eq!(before.len(), 1);

    // Reorg, but the miner re-includes the same transaction in the new block 2'.
    // This is the common case: the transaction is valid, just in a different block.
    r.chain.reorg_to(
        1,
        &["b2", "b3"],
        vec![vec![transfer("survivor", &addr, 60_000_000)], vec![]],
    );
    catch_up_collecting_reorgs(&r.watcher).await;

    // Exactly one deposit still, because (chain, tx_hash, log_index) is unique.
    let after = chainrail_database::repo::deposits::list_deposits(db.pool(), None, None, None, 10)
        .await
        .unwrap();
    assert_eq!(
        after.len(),
        1,
        "the surviving transaction must not become a second deposit"
    );
    let transfers: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM blockchain_transactions")
        .fetch_one(db.pool())
        .await
        .unwrap();
    assert_eq!(transfers, 1);

    // It was marked reorged when its original block was orphaned. A conservative
    // outcome: the deposit is not silently resurrected, and an operator can
    // reconcile it -- see docs/failure-modes.md.
    let d = chainrail_database::repo::deposits::get_deposit(db.pool(), after[0].id)
        .await
        .unwrap();
    assert_eq!(d.status().unwrap(), DepositStatus::Reorged);
    assert!(
        d.ledger_transaction_id.is_none(),
        "it was never credited, so nothing was reversed"
    );
}

#[tokio::test]
async fn confirmation_engine_refuses_to_confirm_deposits_from_orphaned_blocks() {
    let db = require_db!();
    let f = fixture(&db).await;
    let addr = address(0x11);
    register_deposit_address(&db, f.user_id, &addr).await;
    let r = rig(&db).await;

    r.chain.mine("a1", vec![transfer("dep", &addr, 10_000_000)]);
    catch_up(&r.watcher).await;

    // Orphan the block directly, without letting the reorg engine touch the
    // deposit, so the confirmation engine faces a stale deposit on its own.
    sqlx::query(
        "UPDATE chain_blocks SET status = 'orphaned', orphaned_at = now()
          WHERE chain = $1 AND height = 1",
    )
    .bind(TEST_CHAIN)
    .execute(db.pool())
    .await
    .unwrap();

    r.chain.mine_empty(5);
    let pass = r.confirmations.run_once().await.unwrap();
    assert_eq!(pass.non_canonical, 1, "must detect the non-canonical block");
    assert_eq!(pass.advanced_to_confirmed, 0, "must not confirm it");

    let deposits =
        chainrail_database::repo::deposits::list_deposits(db.pool(), None, None, None, 10)
            .await
            .unwrap();
    assert_ne!(deposits[0].status().unwrap(), DepositStatus::Confirmed);
    assert_eq!(
        available_balance(&db, f.user_id, f.asset_id).await,
        Amount::ZERO
    );
}

#[tokio::test]
async fn a_single_block_reorg_at_the_tip_is_handled() {
    let db = require_db!();
    let _f = fixture(&db).await;
    let r = rig(&db).await;

    r.chain.mine("a1", vec![]);
    r.chain.mine("a2", vec![]);
    catch_up(&r.watcher).await;
    assert_eq!(canonical_heights(&db).await, vec![0, 1, 2]);

    // Only the tip changes.
    r.chain.reorg_to(1, &["b2"], vec![vec![]]);
    let reports = catch_up_collecting_reorgs(&r.watcher).await;
    assert_eq!(reports.len(), 1);
    assert_eq!(reports[0].common_ancestor_height, 1);
    assert_eq!(reports[0].orphaned_blocks, 1);
    assert_eq!(reports[0].depth, 1);

    assert_eq!(canonical_heights(&db).await, vec![0, 1, 2]);
    let tip: chainrail_common::Hash32 = sqlx::query_scalar(
        "SELECT hash FROM chain_blocks WHERE chain = $1 AND height = 2 AND status = 'canonical'",
    )
    .bind(TEST_CHAIN)
    .fetch_one(db.pool())
    .await
    .unwrap();
    assert_eq!(tip, r.chain.hash_at(2).unwrap());
}

#[tokio::test]
async fn reorg_events_are_emitted_for_affected_deposits() {
    let db = require_db!();
    let f = fixture(&db).await;
    let addr = address(0x11);
    register_deposit_address(&db, f.user_id, &addr).await;
    let r = rig(&db).await;
    let cfg = test_app_config();

    r.chain.mine("a1", vec![transfer("dep", &addr, 5_000_000)]);
    catch_up(&r.watcher).await;
    r.chain.reorg_to(0, &["b1", "b2"], vec![vec![], vec![]]);
    catch_up_collecting_reorgs(&r.watcher).await;

    let bus = drain_outbox(&db, &cfg).await;
    let reorged = bus.events_of_type("deposit.reorged");
    assert_eq!(reorged.len(), 1, "downstream systems must be told");
    // Deterministic id: re-running recovery must not enqueue a second event.
    let first_id = reorged[0].event_id;
    let again = drain_outbox(&db, &cfg).await;
    assert!(again.events_of_type("deposit.reorged").is_empty());
    assert!(!first_id.is_nil());
}

#[tokio::test]
async fn a_reorg_deeper_than_the_scan_window_is_escalated_not_guessed() {
    let db = require_db!();
    let _f = fixture(&db).await;
    let cfg = test_app_config();
    let mut chain_cfg = cfg.chains[0].clone();
    // Tiny window: 5 confirmations of history retained.
    chain_cfg.reorg_scan_depth = 4;
    chain_cfg.finality = chainrail_common::FinalityPolicy::Confirmations { blocks: 2 };

    let chain = MockChain::new(TEST_CHAIN);
    let ctx = ChainContext::new(db.clone(), &chain_cfg, Duration::from_millis(0));
    ctx.refresh().await.unwrap();
    let watcher = Arc::new(Watcher::new(
        db.clone(),
        chain.clone() as Arc<dyn chainrail_chains_evm::ChainAdapter>,
        ctx,
        &chain_cfg,
    ));

    chain.mine_empty(20);
    for _ in 0..20 {
        if matches!(watcher.tick().await.unwrap(), Tick::Idle { .. }) {
            break;
        }
    }

    // Fork 15 blocks deep, far beyond the 4-block window.
    let tags: Vec<&str> = vec![
        "z1", "z2", "z3", "z4", "z5", "z6", "z7", "z8", "z9", "z10", "z11", "z12", "z13", "z14",
        "z15", "z16",
    ];
    chain.reorg_to(4, &tags, vec![vec![]; tags.len()]);

    // The engine must refuse rather than pick an arbitrary rewind point: a wrong
    // guess could double-reverse or re-credit.
    let err = watcher.tick().await.expect_err("must escalate");
    assert!(
        err.to_string().contains("deeper than reorg_scan_depth"),
        "unexpected error: {err}"
    );
}
