//! Ledger correctness against a real database.

use chainrail_common::{Amount, Direction, Error};
use chainrail_database::models::AccountType;
use chainrail_integration::*;
use chainrail_ledger as ledger;
use chainrail_ledger::{Posting, ReferenceType, TransactionKind};
use uuid::Uuid;

#[tokio::test]
async fn deposit_credit_posts_balanced_entries_and_moves_both_sides() {
    let db = require_db!();
    let f = fixture(&db).await;
    let deposit_id = Uuid::new_v4();

    let mut tx = db.begin().await.unwrap();
    let result = ledger::credit_deposit(
        &mut tx,
        deposit_id,
        f.user_id,
        f.asset_id,
        Amount::new(100_000_000),
        Some("corr-1".into()),
    )
    .await
    .unwrap();
    tx.commit().await.unwrap();
    assert!(result.was_posted());

    // The user is owed the funds, and custody records that we hold them.
    assert_eq!(
        available_balance(&db, f.user_id, f.asset_id).await,
        Amount::new(100_000_000)
    );
    assert_eq!(
        system_balance(&db, AccountType::ExchangeCustody, f.asset_id).await,
        Amount::new(100_000_000)
    );

    let entries = ledger::get_transaction_entries(db.pool(), result.id())
        .await
        .unwrap();
    assert_eq!(entries.len(), 2);
    let debits: Amount = sum(&entries, Direction::Debit);
    let credits: Amount = sum(&entries, Direction::Credit);
    assert_eq!(debits, credits, "sum(debits) must equal sum(credits)");
}

fn sum(entries: &[chainrail_database::models::LedgerEntryView], dir: Direction) -> Amount {
    entries
        .iter()
        .filter(|e| e.direction == dir)
        .fold(Amount::ZERO, |a, e| a.checked_add(e.amount).unwrap())
}

#[tokio::test]
async fn every_posted_transaction_balances() {
    let db = require_db!();
    let f = fixture(&db).await;

    // Exercise every posting shape the system can produce. Custody must be
    // funded first -- gas is drawn from the native-asset hot wallet.
    fund_custody(&db, f.native_asset_id, 1_000_000).await;
    fund(&db, f.user_id, f.asset_id, 500).await;
    let w = Uuid::new_v4();
    let mut tx = db.begin().await.unwrap();
    ledger::reserve_withdrawal(&mut tx, w, f.user_id, f.asset_id, Amount::new(200), None)
        .await
        .unwrap();
    ledger::record_broadcast(&mut tx, w, f.asset_id, Amount::new(200), None)
        .await
        .unwrap();
    ledger::settle_withdrawal(&mut tx, w, f.user_id, f.asset_id, Amount::new(200), None)
        .await
        .unwrap();
    ledger::record_network_fee(&mut tx, w, f.native_asset_id, Amount::new(21_000), None)
        .await
        .unwrap();
    tx.commit().await.unwrap();

    let report = ledger::verify_ledger_integrity(db.pool()).await.unwrap();
    assert!(report.is_clean(), "integrity violations: {:#?}", report);
    assert!(report.transactions_checked >= 5);
    assert!(report.unbalanced_transactions.is_empty());
}

#[tokio::test]
async fn posting_is_idempotent_on_its_key() {
    let db = require_db!();
    let f = fixture(&db).await;
    let deposit_id = Uuid::new_v4();

    let mut first_id = None;
    // Post the same credit five times, each in its own transaction, exactly as
    // a retrying consumer would.
    for i in 0..5 {
        let mut tx = db.begin().await.unwrap();
        let r = ledger::credit_deposit(
            &mut tx,
            deposit_id,
            f.user_id,
            f.asset_id,
            Amount::new(1_000),
            None,
        )
        .await
        .unwrap();
        tx.commit().await.unwrap();
        match i {
            0 => {
                assert!(r.was_posted(), "first call must post");
                first_id = Some(r.id());
            }
            _ => {
                assert!(!r.was_posted(), "call {i} must be a no-op");
                assert_eq!(r.id(), first_id.unwrap(), "must return the original tx");
            }
        }
    }

    // Credited exactly once despite five attempts.
    assert_eq!(
        available_balance(&db, f.user_id, f.asset_id).await,
        Amount::new(1_000)
    );
    let entry_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM ledger_entries")
        .fetch_one(db.pool())
        .await
        .unwrap();
    assert_eq!(entry_count, 2, "exactly one balanced pair of entries");
}

#[tokio::test]
async fn unbalanced_posting_is_rejected_by_the_database_even_if_rust_is_bypassed() {
    let db = require_db!();
    let f = fixture(&db).await;

    let mut tx = db.begin().await.unwrap();
    let custody = ledger::system_account(&mut tx, AccountType::ExchangeCustody, f.asset_id)
        .await
        .unwrap();
    let available =
        ledger::user_account(&mut tx, AccountType::UserAvailable, f.user_id, f.asset_id)
            .await
            .unwrap();
    tx.commit().await.unwrap();

    // Write entries by hand, skipping Posting::validate entirely.
    let mut tx = db.begin().await.unwrap();
    let ltx_id: Uuid = sqlx::query_scalar(
        "INSERT INTO ledger_transactions (kind, reference_type, idempotency_key)
         VALUES ('adjustment', 'manual', 'hand-written') RETURNING id",
    )
    .fetch_one(&mut *tx)
    .await
    .unwrap();
    for (account, amount, dir) in [(custody, 100i64, "debit"), (available, 99i64, "credit")] {
        sqlx::query(
            "INSERT INTO ledger_entries (ledger_transaction_id, account_id, asset_id, amount, direction)
             VALUES ($1, $2, $3, $4, $5)",
        )
        .bind(ltx_id)
        .bind(account)
        .bind(f.asset_id)
        .bind(amount)
        .bind(dir)
        .execute(&mut *tx)
        .await
        .unwrap();
    }
    // The deferred constraint trigger fires at COMMIT.
    let err = tx.commit().await.expect_err("unbalanced commit must fail");
    assert!(
        err.to_string().contains("does not balance"),
        "unexpected error: {err}"
    );
}

#[tokio::test]
async fn ledger_history_cannot_be_rewritten() {
    let db = require_db!();
    let f = fixture(&db).await;
    fund(&db, f.user_id, f.asset_id, 100).await;

    for stmt in [
        "UPDATE ledger_entries SET amount = 1",
        "DELETE FROM ledger_entries",
        "UPDATE ledger_transactions SET kind = 'adjustment'",
        "DELETE FROM ledger_transactions",
    ] {
        let err = sqlx::query(stmt)
            .execute(db.pool())
            .await
            .expect_err("append-only violation must be rejected");
        assert!(
            err.to_string().contains("append-only"),
            "statement `{stmt}` was not blocked: {err}"
        );
    }
}

#[tokio::test]
async fn balance_never_goes_negative() {
    let db = require_db!();
    let f = fixture(&db).await;
    fund(&db, f.user_id, f.asset_id, 100).await;

    let mut tx = db.begin().await.unwrap();
    let err = ledger::reserve_withdrawal(
        &mut tx,
        Uuid::new_v4(),
        f.user_id,
        f.asset_id,
        Amount::new(101),
        None,
    )
    .await
    .expect_err("overdraw must fail");
    assert!(
        matches!(err, Error::InsufficientBalance { .. }),
        "got {err:?}"
    );
    drop(tx);

    assert_eq!(
        available_balance(&db, f.user_id, f.asset_id).await,
        Amount::new(100)
    );
}

#[tokio::test]
async fn withdrawal_lifecycle_conserves_value() {
    let db = require_db!();
    let f = fixture(&db).await;
    fund(&db, f.user_id, f.asset_id, 1_000).await;
    let w = Uuid::new_v4();

    let mut tx = db.begin().await.unwrap();
    ledger::reserve_withdrawal(&mut tx, w, f.user_id, f.asset_id, Amount::new(250), None)
        .await
        .unwrap();
    tx.commit().await.unwrap();
    assert_eq!(
        available_balance(&db, f.user_id, f.asset_id).await,
        Amount::new(750)
    );
    assert_eq!(
        balance_of(&db, AccountType::UserReserved, f.user_id, f.asset_id).await,
        Amount::new(250)
    );

    let mut tx = db.begin().await.unwrap();
    ledger::record_broadcast(&mut tx, w, f.asset_id, Amount::new(250), None)
        .await
        .unwrap();
    tx.commit().await.unwrap();
    // Custody drops; the value is in flight.
    assert_eq!(
        system_balance(&db, AccountType::ExchangeCustody, f.asset_id).await,
        Amount::new(750)
    );
    assert_eq!(
        system_balance(&db, AccountType::WithdrawalClearing, f.asset_id).await,
        Amount::new(250)
    );

    let mut tx = db.begin().await.unwrap();
    ledger::settle_withdrawal(&mut tx, w, f.user_id, f.asset_id, Amount::new(250), None)
        .await
        .unwrap();
    tx.commit().await.unwrap();

    // Clearing drains, the reservation is extinguished, the user keeps 750.
    assert_eq!(
        system_balance(&db, AccountType::WithdrawalClearing, f.asset_id).await,
        Amount::ZERO
    );
    assert_eq!(
        balance_of(&db, AccountType::UserReserved, f.user_id, f.asset_id).await,
        Amount::ZERO
    );
    assert_eq!(
        available_balance(&db, f.user_id, f.asset_id).await,
        Amount::new(750)
    );

    let report = ledger::verify_ledger_integrity(db.pool()).await.unwrap();
    assert!(report.is_clean(), "{report:#?}");
    let solvency = report
        .solvency
        .iter()
        .find(|s| s.asset_symbol == "USDC")
        .unwrap();
    assert_eq!(solvency.surplus().unwrap(), Amount::ZERO, "value conserved");
    assert_eq!(solvency.total_user_liability, Amount::new(750));
}

#[tokio::test]
async fn cancelled_withdrawal_returns_funds_exactly() {
    let db = require_db!();
    let f = fixture(&db).await;
    fund(&db, f.user_id, f.asset_id, 1_000).await;
    let w = Uuid::new_v4();

    let mut tx = db.begin().await.unwrap();
    ledger::reserve_withdrawal(&mut tx, w, f.user_id, f.asset_id, Amount::new(400), None)
        .await
        .unwrap();
    ledger::release_reservation(&mut tx, w, f.user_id, f.asset_id, Amount::new(400), None)
        .await
        .unwrap();
    tx.commit().await.unwrap();

    assert_eq!(
        available_balance(&db, f.user_id, f.asset_id).await,
        Amount::new(1_000)
    );
    assert_eq!(
        balance_of(&db, AccountType::UserReserved, f.user_id, f.asset_id).await,
        Amount::ZERO
    );
    assert!(ledger::verify_ledger_integrity(db.pool())
        .await
        .unwrap()
        .is_clean());
}

#[tokio::test]
async fn reorg_reversal_books_a_deficit_when_funds_were_already_spent() {
    let db = require_db!();
    let f = fixture(&db).await;
    let deposit_id = Uuid::new_v4();

    // The exchange holds its own capital, as it would in reality; without this
    // the reversal below would drive custody negative, which the ledger
    // correctly refuses (we cannot hold fewer than zero coins).
    fund_custody(&db, f.asset_id, 5_000).await;

    // Credit 1000, then the user withdraws 800 (reserved + settled).
    let mut tx = db.begin().await.unwrap();
    ledger::credit_deposit(
        &mut tx,
        deposit_id,
        f.user_id,
        f.asset_id,
        Amount::new(1_000),
        None,
    )
    .await
    .unwrap();
    tx.commit().await.unwrap();

    let w = Uuid::new_v4();
    let mut tx = db.begin().await.unwrap();
    ledger::reserve_withdrawal(&mut tx, w, f.user_id, f.asset_id, Amount::new(800), None)
        .await
        .unwrap();
    ledger::record_broadcast(&mut tx, w, f.asset_id, Amount::new(800), None)
        .await
        .unwrap();
    ledger::settle_withdrawal(&mut tx, w, f.user_id, f.asset_id, Amount::new(800), None)
        .await
        .unwrap();
    tx.commit().await.unwrap();
    assert_eq!(
        available_balance(&db, f.user_id, f.asset_id).await,
        Amount::new(200)
    );

    // Now the deposit's block is orphaned by a deep reorg.
    let mut tx = db.begin().await.unwrap();
    let r = ledger::reverse_deposit_credit(
        &mut tx,
        deposit_id,
        f.user_id,
        f.asset_id,
        Amount::new(1_000),
        None,
    )
    .await
    .unwrap();
    tx.commit().await.unwrap();
    assert!(r.was_posted());

    // Spendable balance is drained to zero, never negative...
    assert_eq!(
        available_balance(&db, f.user_id, f.asset_id).await,
        Amount::ZERO
    );
    // ...and the 800 the user already took is recorded as a receivable.
    assert_eq!(
        balance_of(&db, AccountType::UserDeficit, f.user_id, f.asset_id).await,
        Amount::new(800)
    );

    let report = ledger::verify_ledger_integrity(db.pool()).await.unwrap();
    assert!(report.is_clean(), "{report:#?}");
    assert!(report.illegal_negative_balances.is_empty());
}

#[tokio::test]
async fn reorg_reversal_with_untouched_funds_takes_it_all_from_available() {
    let db = require_db!();
    let f = fixture(&db).await;
    let deposit_id = Uuid::new_v4();

    fund_custody(&db, f.asset_id, 1_000).await;
    let mut tx = db.begin().await.unwrap();
    ledger::credit_deposit(
        &mut tx,
        deposit_id,
        f.user_id,
        f.asset_id,
        Amount::new(500),
        None,
    )
    .await
    .unwrap();
    tx.commit().await.unwrap();

    let mut tx = db.begin().await.unwrap();
    ledger::reverse_deposit_credit(
        &mut tx,
        deposit_id,
        f.user_id,
        f.asset_id,
        Amount::new(500),
        None,
    )
    .await
    .unwrap();
    tx.commit().await.unwrap();

    assert_eq!(
        available_balance(&db, f.user_id, f.asset_id).await,
        Amount::ZERO
    );
    assert_eq!(
        balance_of(&db, AccountType::UserDeficit, f.user_id, f.asset_id).await,
        Amount::ZERO,
        "no deficit when the funds were still there"
    );
    // Custody returns to just the exchange's own capital.
    assert_eq!(
        system_balance(&db, AccountType::ExchangeCustody, f.asset_id).await,
        Amount::new(1_000)
    );
}

#[tokio::test]
async fn asset_mismatch_between_entry_and_account_is_rejected() {
    let db = require_db!();
    let f = fixture(&db).await;

    let mut tx = db.begin().await.unwrap();
    let usdc_custody = ledger::system_account(&mut tx, AccountType::ExchangeCustody, f.asset_id)
        .await
        .unwrap();
    let user_available =
        ledger::user_account(&mut tx, AccountType::UserAvailable, f.user_id, f.asset_id)
            .await
            .unwrap();
    tx.commit().await.unwrap();

    // Post ETH amounts against USDC accounts -- a class of bug that would
    // silently corrupt balances if the database did not check it.
    let posting = Posting::new(
        TransactionKind::Adjustment,
        ReferenceType::Manual,
        None,
        "asset-mismatch",
    )
    .debit(usdc_custody, f.native_asset_id, Amount::new(10))
    .credit(user_available, f.native_asset_id, Amount::new(10));

    let mut tx = db.begin().await.unwrap();
    let err = ledger::post_transaction(&mut tx, &posting)
        .await
        .expect_err("asset mismatch must be rejected");
    assert!(
        err.to_string().contains("does not match account asset")
            || matches!(err, Error::Validation(_)),
        "unexpected error: {err}"
    );
}

#[tokio::test]
async fn integrity_check_detects_manually_corrupted_balances() {
    let db = require_db!();
    let f = fixture(&db).await;
    fund(&db, f.user_id, f.asset_id, 1_000).await;
    assert!(ledger::verify_ledger_integrity(db.pool())
        .await
        .unwrap()
        .is_clean());

    // Simulate corruption that bypasses the entry trigger entirely.
    sqlx::query(
        "UPDATE ledger_accounts SET balance = balance + 1
          WHERE account_type = 'user_available' AND owner_user_id = $1",
    )
    .bind(f.user_id)
    .execute(db.pool())
    .await
    .unwrap();

    let report = ledger::verify_ledger_integrity(db.pool()).await.unwrap();
    assert!(!report.is_clean(), "drift must be detected");
    assert_eq!(report.balance_drift.len(), 1);
    assert_eq!(report.balance_drift[0].cached_balance, Amount::new(1_001));
    assert_eq!(report.balance_drift[0].derived_balance, Amount::new(1_000));
}
