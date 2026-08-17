//! Withdrawal lifecycle:
//! request -> reservation -> sign -> broadcast -> confirmation -> completed,
//! plus every failure branch that must return funds.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use chainrail_chains_evm::ChainAdapter;
use chainrail_common::{Amount, Error};
use chainrail_database::models::{AccountType, WithdrawalStatus};
use chainrail_integration::*;
use chainrail_risk::RiskEngine;
use chainrail_withdrawals::{WithdrawalPipeline, WithdrawalRequest, WithdrawalService};
use uuid::Uuid;

const DEST: &str = "0x70997970C51812dc3A010C7d01b50e0d17dc79C8";

struct Rig {
    chain: Arc<MockChain>,
    service: Arc<WithdrawalService>,
    pipeline: Arc<WithdrawalPipeline>,
    cfg: Arc<chainrail_common::config::AppConfig>,
}

async fn rig_with(
    db: &chainrail_database::Db,
    mutate: impl FnOnce(&mut chainrail_common::config::AppConfig),
) -> Rig {
    let mut raw = test_app_config();
    mutate(&mut raw);
    let cfg = Arc::new(raw);
    let chain = MockChain::new(TEST_CHAIN);
    let risk = Arc::new(RiskEngine::new(cfg.risk.clone()));
    let signer = chainrail_signer::from_config(&cfg.signer).unwrap();

    let mut adapters: HashMap<String, Arc<dyn ChainAdapter>> = HashMap::new();
    adapters.insert(
        TEST_CHAIN.to_string(),
        chain.clone() as Arc<dyn ChainAdapter>,
    );

    Rig {
        service: WithdrawalService::new(db.clone(), Arc::clone(&cfg), risk),
        pipeline: WithdrawalPipeline::new(
            db.clone(),
            Arc::clone(&cfg),
            signer,
            adapters,
            Duration::from_millis(10),
        ),
        chain,
        cfg,
    }
}

async fn rig(db: &chainrail_database::Db) -> Rig {
    rig_with(db, |_| {}).await
}

fn request(user_id: Uuid, amount: i128, key: &str) -> WithdrawalRequest {
    WithdrawalRequest {
        user_id,
        chain: TEST_CHAIN.into(),
        asset_symbol: "USDC".into(),
        amount: Amount::new(amount),
        destination: DEST.into(),
        idempotency_key: key.into(),
        correlation_id: None,
    }
}

async fn status(db: &chainrail_database::Db, id: Uuid) -> WithdrawalStatus {
    chainrail_database::repo::withdrawals::get_withdrawal(db.pool(), id)
        .await
        .unwrap()
        .status()
        .unwrap()
}

#[tokio::test]
async fn full_lifecycle_from_request_to_completion() {
    let db = require_db!();
    let f = fixture(&db).await;
    fund_custody(&db, f.asset_id, 1_000_000_000).await;
    fund_custody(&db, f.native_asset_id, 1_000_000_000_000_000_000).await;
    fund(&db, f.user_id, f.asset_id, 100_000_000).await;
    let r = rig(&db).await;
    r.chain.mine_empty(1);

    // --- request: validated, reserved, approved ---
    let created = r
        .service
        .create(request(f.user_id, 25_000_000, "wd-key-000001"))
        .await
        .unwrap();
    let id = created.withdrawal().id;
    assert!(created.is_new());
    assert_eq!(status(&db, id).await, WithdrawalStatus::Approved);

    // Funds moved from available to reserved, and the ledger recorded it.
    assert_eq!(
        available_balance(&db, f.user_id, f.asset_id).await,
        Amount::new(75_000_000)
    );
    assert_eq!(
        balance_of(&db, AccountType::UserReserved, f.user_id, f.asset_id).await,
        Amount::new(25_000_000)
    );
    let w = chainrail_database::repo::withdrawals::get_withdrawal(db.pool(), id)
        .await
        .unwrap();
    assert!(w.reserve_ledger_transaction_id.is_some());

    // --- sign + broadcast ---
    let pass = r.pipeline.process_approved(TEST_CHAIN).await.unwrap();
    assert_eq!(pass.broadcast, 1);
    assert_eq!(status(&db, id).await, WithdrawalStatus::Broadcast);

    let w = chainrail_database::repo::withdrawals::get_withdrawal(db.pool(), id)
        .await
        .unwrap();
    let tx_hash = w.tx_hash.clone().expect("hash persisted before broadcast");
    assert_eq!(w.nonce, Some(0), "first nonce for this hot wallet");
    assert_eq!(r.chain.broadcasts().len(), 1);
    assert_eq!(r.chain.broadcasts()[0].tx_hash, tx_hash);

    // Value left custody into the clearing account.
    assert_eq!(
        system_balance(&db, AccountType::WithdrawalClearing, f.asset_id).await,
        Amount::new(25_000_000)
    );

    // --- confirmation: not yet enough ---
    r.chain
        .set_tx_status(&tx_hash, mined(r.chain.height(), 21_000_000_000_000));
    let pass = r.pipeline.process_in_flight(TEST_CHAIN).await.unwrap();
    assert_eq!(pass.completed, 0);
    assert_eq!(pass.still_pending, 1);
    assert_eq!(status(&db, id).await, WithdrawalStatus::Confirming);

    // --- confirmation: threshold reached (3 blocks) ---
    r.chain.mine_empty(3);
    let pass = r.pipeline.process_in_flight(TEST_CHAIN).await.unwrap();
    assert_eq!(pass.completed, 1);
    assert_eq!(status(&db, id).await, WithdrawalStatus::Completed);

    // --- settlement: reservation extinguished, clearing drained ---
    assert_eq!(
        balance_of(&db, AccountType::UserReserved, f.user_id, f.asset_id).await,
        Amount::ZERO
    );
    assert_eq!(
        system_balance(&db, AccountType::WithdrawalClearing, f.asset_id).await,
        Amount::ZERO
    );
    assert_eq!(
        available_balance(&db, f.user_id, f.asset_id).await,
        Amount::new(75_000_000),
        "the user keeps exactly what they did not withdraw"
    );
    let w = chainrail_database::repo::withdrawals::get_withdrawal(db.pool(), id)
        .await
        .unwrap();
    assert!(w.settle_ledger_transaction_id.is_some());
    assert_eq!(w.fee_paid_raw, Some(Amount::new(21_000_000_000_000)));

    // Gas was booked as an expense against the native asset.
    let fee_balance = system_balance(&db, AccountType::NetworkFee, f.native_asset_id).await;
    assert_eq!(fee_balance, Amount::new(21_000_000_000_000));

    let integrity = chainrail_ledger::verify_ledger_integrity(db.pool())
        .await
        .unwrap();
    assert!(integrity.is_clean(), "{integrity:#?}");

    // Events for the whole journey reached the outbox.
    let bus = drain_outbox(&db, &r.cfg).await;
    let counts = bus.counts_by_type();
    assert_eq!(counts.get("withdrawal.requested").copied(), Some(1));
    assert_eq!(counts.get("withdrawal.broadcast").copied(), Some(1));
    assert_eq!(counts.get("withdrawal.completed").copied(), Some(1));
}

#[tokio::test]
async fn repeating_a_request_with_the_same_key_creates_nothing_new() {
    let db = require_db!();
    let f = fixture(&db).await;
    fund(&db, f.user_id, f.asset_id, 100_000_000).await;
    let r = rig(&db).await;

    let first = r
        .service
        .create(request(f.user_id, 10_000_000, "same-key-0001"))
        .await
        .unwrap();
    assert!(first.is_new());

    for _ in 0..4 {
        let again = r
            .service
            .create(request(f.user_id, 10_000_000, "same-key-0001"))
            .await
            .unwrap();
        assert!(!again.is_new(), "must be recognised as a replay");
        assert_eq!(again.withdrawal().id, first.withdrawal().id);
    }

    // Reserved exactly once.
    assert_eq!(
        available_balance(&db, f.user_id, f.asset_id).await,
        Amount::new(90_000_000)
    );
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM withdrawals")
        .fetch_one(db.pool())
        .await
        .unwrap();
    assert_eq!(count, 1);
}

#[tokio::test]
async fn reusing_a_key_with_a_different_body_is_rejected() {
    let db = require_db!();
    let f = fixture(&db).await;
    fund(&db, f.user_id, f.asset_id, 100_000_000).await;
    let r = rig(&db).await;

    r.service
        .create(request(f.user_id, 10_000_000, "reused-key-01"))
        .await
        .unwrap();

    // Same key, different amount. Silently returning the first withdrawal would
    // make the client believe a 50 USDC transfer happened.
    let err = r
        .service
        .create(request(f.user_id, 50_000_000, "reused-key-01"))
        .await
        .unwrap_err();
    assert!(matches!(err, Error::IdempotencyKeyConflict), "got {err:?}");

    // And with a different destination.
    let mut other_dest = request(f.user_id, 10_000_000, "reused-key-01");
    other_dest.destination = "0x3C44CdDdB6a900fa2b585dd299e03d12FA4293BC".into();
    assert!(matches!(
        r.service.create(other_dest).await.unwrap_err(),
        Error::IdempotencyKeyConflict
    ));
}

#[tokio::test]
async fn insufficient_balance_is_refused_and_leaves_no_reservation() {
    let db = require_db!();
    let f = fixture(&db).await;
    fund(&db, f.user_id, f.asset_id, 10_000_000).await;
    let r = rig(&db).await;

    let err = r
        .service
        .create(request(f.user_id, 25_000_000, "too-big-0001"))
        .await
        .unwrap_err();
    // Surfaced through the policy engine, which checks balance before reserving.
    assert!(
        matches!(err, Error::PolicyDenied { .. })
            || matches!(err, Error::InsufficientBalance { .. }),
        "got {err:?}"
    );

    assert_eq!(
        available_balance(&db, f.user_id, f.asset_id).await,
        Amount::new(10_000_000),
        "balance untouched"
    );
    assert_eq!(
        balance_of(&db, AccountType::UserReserved, f.user_id, f.asset_id).await,
        Amount::ZERO,
        "no reservation left behind"
    );
    // The row is retained in `failed` so the client can fetch the reason.
    let rows =
        chainrail_database::repo::withdrawals::list_withdrawals(db.pool(), None, None, None, 10)
            .await
            .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].status().unwrap(), WithdrawalStatus::Failed);
    assert!(rows[0].failure_code.is_some());
}

#[tokio::test]
async fn invalid_destinations_are_rejected_before_any_database_write() {
    let db = require_db!();
    let f = fixture(&db).await;
    fund(&db, f.user_id, f.asset_id, 100_000_000).await;
    let r = rig(&db).await;

    for bad in [
        "0x70997970c51812dc3a010c7d01b50e0d17dc79C8", // bad EIP-55 checksum
        "70997970C51812dc3A010C7d01b50e0d17dc79C8",   // no 0x
        "0x70997970C51812dc3A010C7d01b50e0d17dc79",   // too short
        "0x0000000000000000000000000000000000000000", // zero address
    ] {
        let mut req = request(f.user_id, 1_000_000, "bad-dest-0001");
        req.destination = bad.into();
        let err = r.service.create(req).await.unwrap_err();
        assert!(
            matches!(err, Error::InvalidAddress { .. }),
            "destination {bad} gave {err:?}"
        );
    }

    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM withdrawals")
        .fetch_one(db.pool())
        .await
        .unwrap();
    assert_eq!(
        count, 0,
        "nothing should be persisted for a malformed request"
    );
}

#[tokio::test]
async fn policy_denials_are_specific_and_leave_funds_alone() {
    let db = require_db!();
    let f = fixture(&db).await;
    fund(&db, f.user_id, f.asset_id, 1_000_000_000).await;

    let r = rig_with(&db, |cfg| {
        cfg.risk
            .max_per_request
            .insert("USDC".into(), Amount::new(10_000_000));
    })
    .await;

    let err = r
        .service
        .create(request(f.user_id, 50_000_000, "over-limit-01"))
        .await
        .unwrap_err();
    match err {
        Error::PolicyDenied { code, .. } => assert_eq!(code, "above_maximum"),
        other => panic!("expected a policy denial, got {other:?}"),
    }
    assert_eq!(
        available_balance(&db, f.user_id, f.asset_id).await,
        Amount::new(1_000_000_000)
    );
}

#[tokio::test]
async fn large_withdrawals_are_held_for_manual_approval() {
    let db = require_db!();
    let f = fixture(&db).await;
    fund_custody(&db, f.asset_id, 1_000_000_000).await;
    fund(&db, f.user_id, f.asset_id, 1_000_000_000).await;

    let r = rig_with(&db, |cfg| {
        cfg.risk
            .manual_approval_threshold
            .insert("USDC".into(), Amount::new(10_000_000));
    })
    .await;
    r.chain.mine_empty(1);

    let created = r
        .service
        .create(request(f.user_id, 50_000_000, "needs-approval1"))
        .await
        .unwrap();
    let id = created.withdrawal().id;
    // Held, not denied: funds are already reserved but it will not be broadcast.
    assert_eq!(status(&db, id).await, WithdrawalStatus::Validated);
    assert_eq!(
        balance_of(&db, AccountType::UserReserved, f.user_id, f.asset_id).await,
        Amount::new(50_000_000)
    );

    let pass = r.pipeline.process_approved(TEST_CHAIN).await.unwrap();
    assert_eq!(pass.broadcast, 0, "a held withdrawal must not be broadcast");

    // Operator approval releases it into the pipeline.
    r.service.approve(id).await.unwrap();
    assert_eq!(status(&db, id).await, WithdrawalStatus::Approved);
    let pass = r.pipeline.process_approved(TEST_CHAIN).await.unwrap();
    assert_eq!(pass.broadcast, 1);
}

#[tokio::test]
async fn cancelling_before_broadcast_returns_the_funds() {
    let db = require_db!();
    let f = fixture(&db).await;
    fund(&db, f.user_id, f.asset_id, 100_000_000).await;
    let r = rig(&db).await;

    let created = r
        .service
        .create(request(f.user_id, 40_000_000, "cancel-me-001"))
        .await
        .unwrap();
    let id = created.withdrawal().id;
    assert_eq!(
        available_balance(&db, f.user_id, f.asset_id).await,
        Amount::new(60_000_000)
    );

    r.service
        .cancel(id, "user changed their mind")
        .await
        .unwrap();
    assert_eq!(status(&db, id).await, WithdrawalStatus::Cancelled);
    assert_eq!(
        available_balance(&db, f.user_id, f.asset_id).await,
        Amount::new(100_000_000),
        "funds returned in full"
    );
    assert_eq!(
        balance_of(&db, AccountType::UserReserved, f.user_id, f.asset_id).await,
        Amount::ZERO
    );
    assert!(chainrail_ledger::verify_ledger_integrity(db.pool())
        .await
        .unwrap()
        .is_clean());
}

#[tokio::test]
async fn a_broadcast_withdrawal_cannot_be_cancelled() {
    let db = require_db!();
    let f = fixture(&db).await;
    fund_custody(&db, f.asset_id, 1_000_000_000).await;
    fund(&db, f.user_id, f.asset_id, 100_000_000).await;
    let r = rig(&db).await;
    r.chain.mine_empty(1);

    let created = r
        .service
        .create(request(f.user_id, 10_000_000, "no-cancel-001"))
        .await
        .unwrap();
    let id = created.withdrawal().id;
    r.pipeline.process_approved(TEST_CHAIN).await.unwrap();
    assert_eq!(status(&db, id).await, WithdrawalStatus::Broadcast);

    // We cannot un-send a signed transaction, so this must be refused.
    let err = r.service.cancel(id, "too late").await.unwrap_err();
    assert!(matches!(err, Error::Conflict(_)), "got {err:?}");
    assert_eq!(status(&db, id).await, WithdrawalStatus::Broadcast);
}

#[tokio::test]
async fn an_onchain_revert_returns_the_funds_and_still_books_the_gas() {
    let db = require_db!();
    let f = fixture(&db).await;
    fund_custody(&db, f.asset_id, 1_000_000_000).await;
    fund_custody(&db, f.native_asset_id, 1_000_000_000_000_000_000).await;
    fund(&db, f.user_id, f.asset_id, 100_000_000).await;
    let r = rig(&db).await;
    r.chain.mine_empty(1);

    let created = r
        .service
        .create(request(f.user_id, 30_000_000, "will-revert-01"))
        .await
        .unwrap();
    let id = created.withdrawal().id;
    r.pipeline.process_approved(TEST_CHAIN).await.unwrap();

    let w = chainrail_database::repo::withdrawals::get_withdrawal(db.pool(), id)
        .await
        .unwrap();
    let tx_hash = w.tx_hash.clone().unwrap();

    // Mined, but reverted: the transfer did not happen although gas was spent.
    r.chain
        .set_tx_status(&tx_hash, reverted(r.chain.height(), 15_000_000_000_000));
    let pass = r.pipeline.process_in_flight(TEST_CHAIN).await.unwrap();
    assert_eq!(pass.failed, 1);
    assert_eq!(status(&db, id).await, WithdrawalStatus::Failed);

    // Funds returned to spendable, clearing drained, gas still expensed.
    assert_eq!(
        available_balance(&db, f.user_id, f.asset_id).await,
        Amount::new(100_000_000),
        "the user's funds must come back"
    );
    assert_eq!(
        balance_of(&db, AccountType::UserReserved, f.user_id, f.asset_id).await,
        Amount::ZERO
    );
    assert_eq!(
        system_balance(&db, AccountType::WithdrawalClearing, f.asset_id).await,
        Amount::ZERO
    );
    assert_eq!(
        system_balance(&db, AccountType::NetworkFee, f.native_asset_id).await,
        Amount::new(15_000_000_000_000),
        "gas is a real cost even on a reverted transaction"
    );
    assert!(chainrail_ledger::verify_ledger_integrity(db.pool())
        .await
        .unwrap()
        .is_clean());
}

#[tokio::test]
async fn nonces_are_strictly_sequential_across_many_withdrawals() {
    let db = require_db!();
    let f = fixture(&db).await;
    fund_custody(&db, f.asset_id, 10_000_000_000).await;
    fund(&db, f.user_id, f.asset_id, 100_000_000).await;
    let r = rig(&db).await;
    r.chain.mine_empty(1);

    for i in 0..5 {
        r.service
            .create(request(f.user_id, 1_000_000, &format!("seq-key-{i:04}")))
            .await
            .unwrap();
    }
    let pass = r.pipeline.process_approved(TEST_CHAIN).await.unwrap();
    assert_eq!(pass.broadcast, 5);

    let mut nonces: Vec<i64> =
        sqlx::query_scalar("SELECT nonce FROM withdrawals WHERE nonce IS NOT NULL ORDER BY nonce")
            .fetch_all(db.pool())
            .await
            .unwrap();
    nonces.dedup();
    assert_eq!(
        nonces,
        vec![0, 1, 2, 3, 4],
        "every withdrawal must get a distinct, gap-free nonce"
    );
    // Five distinct transactions on the wire.
    let hashes: std::collections::HashSet<_> = r
        .chain
        .broadcasts()
        .into_iter()
        .map(|b| b.tx_hash)
        .collect();
    assert_eq!(hashes.len(), 5);
}

#[tokio::test]
async fn withdrawals_for_unknown_chains_or_assets_are_rejected() {
    let db = require_db!();
    let f = fixture(&db).await;
    fund(&db, f.user_id, f.asset_id, 100_000_000).await;
    let r = rig(&db).await;

    let mut bad_chain = request(f.user_id, 1_000_000, "bad-chain-001");
    bad_chain.chain = "ethereum-mainnet".into();
    assert!(matches!(
        r.service.create(bad_chain).await.unwrap_err(),
        Error::UnsupportedChain(_)
    ));

    let mut bad_asset = request(f.user_id, 1_000_000, "bad-asset-001");
    bad_asset.asset_symbol = "DOGE".into();
    assert!(matches!(
        r.service.create(bad_asset).await.unwrap_err(),
        Error::UnsupportedAsset(_)
    ));
}

#[tokio::test]
async fn maintenance_mode_blocks_every_withdrawal() {
    let db = require_db!();
    let f = fixture(&db).await;
    fund(&db, f.user_id, f.asset_id, 1_000_000_000).await;
    let r = rig_with(&db, |cfg| cfg.risk.maintenance_mode = true).await;

    let err = r
        .service
        .create(request(f.user_id, 1_000_000, "during-maint01"))
        .await
        .unwrap_err();
    match err {
        Error::PolicyDenied { code, .. } => assert_eq!(code, "maintenance_mode"),
        other => panic!("expected the kill switch, got {other:?}"),
    }
    assert_eq!(
        available_balance(&db, f.user_id, f.asset_id).await,
        Amount::new(1_000_000_000)
    );
}

#[tokio::test]
async fn sending_to_another_users_deposit_address_is_blocked() {
    let db = require_db!();
    let f = fixture(&db).await;
    fund(&db, f.user_id, f.asset_id, 100_000_000).await;

    // Another user owns the destination as their deposit address.
    let other = create_user(&db, "other-user").await;
    let dest = chainrail_common::Address::parse(chainrail_common::ChainKind::Evm, DEST).unwrap();
    register_deposit_address(&db, other, &dest).await;

    let r = rig(&db).await;
    let err = r
        .service
        .create(request(f.user_id, 1_000_000, "internal-dest1"))
        .await
        .unwrap_err();
    match err {
        Error::PolicyDenied { code, .. } => assert_eq!(code, "destination_is_internal"),
        other => panic!("expected an internal-destination denial, got {other:?}"),
    }
}
