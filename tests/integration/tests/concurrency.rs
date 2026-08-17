//! Concurrency correctness.
//!
//! The scenario the brief calls for: one balance, many simultaneous
//! withdrawals, and a hard guarantee that exactly the affordable number
//! succeed with no negative balance and no lost update.
//!
//! ISOLATION LEVEL: READ COMMITTED (Postgres's default). We do *not* rely on
//! SERIALIZABLE. Correctness comes from two mechanisms instead:
//!   1. `SELECT ... FOR UPDATE` on the user's account row, so the
//!      read-check-reserve sequence is serialised per account.
//!   2. The `ledger_accounts_non_negative` CHECK constraint, which is
//!      evaluated inside the balance trigger's `UPDATE ... RETURNING` and
//!      therefore cannot be raced past even if (1) were removed.
//!
//! Mechanism (2) is the real guarantee; (1) turns a storm of constraint
//! violations into an orderly queue and produces better error messages.

use chainrail_common::{Amount, Error};
use chainrail_database::models::AccountType;
use chainrail_integration::*;
use chainrail_ledger as ledger;
use std::sync::Arc;
use uuid::Uuid;

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn twenty_concurrent_withdrawals_against_a_balance_of_ten() {
    let db = require_db!();
    let f = fixture(&db).await;

    // balance = 100 USDC (raw), each withdrawal = 10 => exactly 10 may succeed.
    fund(&db, f.user_id, f.asset_id, 100).await;

    let inner = Arc::new(db.db.clone());
    let mut handles = Vec::new();
    for _ in 0..20 {
        let db = Arc::clone(&inner);
        let (user_id, asset_id) = (f.user_id, f.asset_id);
        handles.push(tokio::spawn(async move {
            let mut tx = db.begin().await.expect("begin");
            let outcome = ledger::reserve_withdrawal(
                &mut tx,
                Uuid::new_v4(),
                user_id,
                asset_id,
                Amount::new(10),
                None,
            )
            .await;
            match outcome {
                Ok(_) => {
                    tx.commit().await.expect("commit");
                    true
                }
                Err(e) => {
                    let _ = tx.rollback().await;
                    // Only ever an insufficient-balance failure -- never a
                    // deadlock, serialization error or constraint surprise.
                    assert!(
                        matches!(e, Error::InsufficientBalance { .. }),
                        "unexpected failure mode: {e:?}"
                    );
                    false
                }
            }
        }));
    }

    let mut succeeded = 0;
    for h in handles {
        if h.await.expect("task panicked") {
            succeeded += 1;
        }
    }

    assert_eq!(succeeded, 10, "exactly 10 of 20 withdrawals must succeed");
    assert_eq!(
        available_balance(&db, f.user_id, f.asset_id).await,
        Amount::ZERO,
        "available balance must be exactly drained, never negative"
    );
    assert_eq!(
        balance_of(&db, AccountType::UserReserved, f.user_id, f.asset_id).await,
        Amount::new(100),
        "every successful reservation must be accounted for"
    );

    let report = ledger::verify_ledger_integrity(db.pool()).await.unwrap();
    assert!(report.is_clean(), "{report:#?}");
    assert!(report.illegal_negative_balances.is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn concurrent_credits_to_one_account_do_not_lose_updates() {
    let db = require_db!();
    let f = fixture(&db).await;

    // 50 concurrent deposit credits of 7 each. A lost update (read-modify-write
    // without a lock) would show up as a balance below 350.
    let inner = Arc::new(db.db.clone());
    let mut handles = Vec::new();
    for _ in 0..50 {
        let db = Arc::clone(&inner);
        let (user_id, asset_id) = (f.user_id, f.asset_id);
        handles.push(tokio::spawn(async move {
            let mut tx = db.begin().await.expect("begin");
            ledger::credit_deposit(
                &mut tx,
                Uuid::new_v4(),
                user_id,
                asset_id,
                Amount::new(7),
                None,
            )
            .await
            .expect("credit");
            tx.commit().await.expect("commit");
        }));
    }
    for h in handles {
        h.await.expect("task panicked");
    }

    assert_eq!(
        available_balance(&db, f.user_id, f.asset_id).await,
        Amount::new(350)
    );
    assert!(ledger::verify_ledger_integrity(db.pool())
        .await
        .unwrap()
        .is_clean());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn concurrent_duplicate_postings_collapse_to_one() {
    let db = require_db!();
    let f = fixture(&db).await;
    let deposit_id = Uuid::new_v4();

    // 25 workers all try to credit the *same* deposit at the same time, which
    // is what a Kafka partition rebalance can produce.
    let inner = Arc::new(db.db.clone());
    let mut handles = Vec::new();
    for _ in 0..25 {
        let db = Arc::clone(&inner);
        let (user_id, asset_id) = (f.user_id, f.asset_id);
        handles.push(tokio::spawn(async move {
            let mut tx = db.begin().await.expect("begin");
            let r = ledger::credit_deposit(
                &mut tx,
                deposit_id,
                user_id,
                asset_id,
                Amount::new(1_000),
                None,
            )
            .await
            .expect("credit");
            let posted = r.was_posted();
            tx.commit().await.expect("commit");
            posted
        }));
    }

    let mut posted = 0;
    for h in handles {
        if h.await.expect("task panicked") {
            posted += 1;
        }
    }

    assert_eq!(
        posted, 1,
        "exactly one worker may post; the rest are no-ops"
    );
    assert_eq!(
        available_balance(&db, f.user_id, f.asset_id).await,
        Amount::new(1_000),
        "credited exactly once"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn postings_touching_the_same_accounts_in_opposite_orders_do_not_deadlock() {
    let db = require_db!();
    let f = fixture(&db).await;
    fund(&db, f.user_id, f.asset_id, 10_000).await;

    // Reserve (available -> reserved) and release (reserved -> available) touch
    // the same two accounts in opposite logical directions. Without the
    // account-id ordering in `Posting::ordered_entries` these would deadlock.
    let inner = Arc::new(db.db.clone());
    let mut handles = Vec::new();
    for i in 0..30 {
        let db = Arc::clone(&inner);
        let (user_id, asset_id) = (f.user_id, f.asset_id);
        handles.push(tokio::spawn(async move {
            let w = Uuid::new_v4();
            let mut tx = db.begin().await.expect("begin");
            if i % 2 == 0 {
                ledger::reserve_withdrawal(&mut tx, w, user_id, asset_id, Amount::new(10), None)
                    .await
                    .expect("reserve");
            } else {
                ledger::reserve_withdrawal(&mut tx, w, user_id, asset_id, Amount::new(10), None)
                    .await
                    .expect("reserve");
                ledger::release_reservation(&mut tx, w, user_id, asset_id, Amount::new(10), None)
                    .await
                    .expect("release");
            }
            tx.commit().await.expect("commit");
        }));
    }
    for h in handles {
        h.await.expect("deadlock or panic");
    }

    // 15 net reservations of 10 each.
    assert_eq!(
        balance_of(&db, AccountType::UserReserved, f.user_id, f.asset_id).await,
        Amount::new(150)
    );
    assert_eq!(
        available_balance(&db, f.user_id, f.asset_id).await,
        Amount::new(9_850)
    );
    assert!(ledger::verify_ledger_integrity(db.pool())
        .await
        .unwrap()
        .is_clean());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn independent_users_do_not_block_each_other() {
    let db = require_db!();
    let f = fixture(&db).await;

    let mut users = Vec::new();
    for i in 0..10 {
        let u = create_user(&db, &format!("parallel-user-{i}")).await;
        fund(&db, u, f.asset_id, 100).await;
        users.push(u);
    }

    let inner = Arc::new(db.db.clone());
    let started = std::time::Instant::now();
    let mut handles = Vec::new();
    for u in users.clone() {
        let db = Arc::clone(&inner);
        let asset_id = f.asset_id;
        handles.push(tokio::spawn(async move {
            for _ in 0..10 {
                let mut tx = db.begin().await.expect("begin");
                ledger::reserve_withdrawal(
                    &mut tx,
                    Uuid::new_v4(),
                    u,
                    asset_id,
                    Amount::new(10),
                    None,
                )
                .await
                .expect("reserve");
                tx.commit().await.expect("commit");
            }
        }));
    }
    for h in handles {
        h.await.expect("task panicked");
    }
    let elapsed = started.elapsed();

    for u in users {
        assert_eq!(available_balance(&db, u, f.asset_id).await, Amount::ZERO);
    }
    // Per-account locking, not a global lock: 100 postings across 10 users
    // should be well under a second locally.
    assert!(
        elapsed < std::time::Duration::from_secs(10),
        "suspiciously slow ({elapsed:?}); check for global lock contention"
    );
}
