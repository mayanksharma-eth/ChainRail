//! Domain postings.
//!
//! Every money movement in ChainRail goes through exactly one of these
//! functions. Keeping the entry shapes in one file makes the accounting model
//! auditable at a glance -- see docs/ledger.md for the same table in prose.
//!
//!   deposit credit        DR custody            CR user_available
//!   deposit reversal      DR user_available     CR custody
//!                        (+DR user_deficit for any shortfall)
//!   withdrawal reserve    DR user_available     CR user_reserved
//!   withdrawal release    DR user_reserved      CR user_available
//!   withdrawal broadcast  DR clearing           CR custody
//!   withdrawal settle     DR user_reserved      CR clearing
//!   broadcast reversal    DR custody            CR clearing
//!   network fee           DR network_fee        CR custody
//!   custody funding       DR custody            CR treasury

use chainrail_common::{Amount, Error, Result};
use chainrail_database::models::AccountType;
use chainrail_database::Tx;
use uuid::Uuid;

use crate::accounts::{lock_user_account, system_account, user_account};
use crate::posting::{Posting, ReferenceType, TransactionKind};
use crate::service::{post_transaction, PostResult};

/// Credit a confirmed deposit.
///
///   DR exchange_custody   (asset increases: we now hold the coins)
///   CR user_available     (liability increases: we owe the user)
pub async fn credit_deposit(
    tx: &mut Tx<'_>,
    deposit_id: Uuid,
    user_id: Uuid,
    asset_id: Uuid,
    amount: Amount,
    correlation_id: Option<String>,
) -> Result<PostResult> {
    require_positive(amount)?;
    let custody = system_account(tx, AccountType::ExchangeCustody, asset_id).await?;
    let available = user_account(tx, AccountType::UserAvailable, user_id, asset_id).await?;

    let posting = Posting::new(
        TransactionKind::DepositCredit,
        ReferenceType::Deposit,
        Some(deposit_id),
        format!("deposit_credit:{deposit_id}"),
    )
    .with_correlation(correlation_id)
    .with_description("deposit credited after confirmation")
    .debit(custody, asset_id, amount)
    .credit(available, asset_id, amount);

    post_transaction(tx, &posting).await
}

/// Reverse an already-credited deposit whose block was orphaned by a reorg.
///
/// History is never rewritten; this posts a *compensating* transaction.
///
/// If the user has already spent some of the credited funds, the shortfall is
/// booked to `user_deficit` -- a receivable from the user -- rather than
/// driving their spendable balance negative. That keeps the non-negative
/// invariant on `user_available` intact while still recording the whole truth.
pub async fn reverse_deposit_credit(
    tx: &mut Tx<'_>,
    deposit_id: Uuid,
    user_id: Uuid,
    asset_id: Uuid,
    amount: Amount,
    correlation_id: Option<String>,
) -> Result<PostResult> {
    require_positive(amount)?;
    let custody = system_account(tx, AccountType::ExchangeCustody, asset_id).await?;
    let available = user_account(tx, AccountType::UserAvailable, user_id, asset_id).await?;

    // Lock first: the available balance we read decides how the reversal is
    // split, and it must not move underneath us.
    let current = lock_user_account(tx, AccountType::UserAvailable, user_id, asset_id)
        .await?
        .map(|a| a.balance)
        .unwrap_or(Amount::ZERO);

    let from_available = if current >= amount {
        amount
    } else {
        current.max(Amount::ZERO)
    };
    let shortfall = amount.checked_sub(from_available)?;

    let mut posting = Posting::new(
        TransactionKind::DepositReversal,
        ReferenceType::Reorg,
        Some(deposit_id),
        format!("deposit_reversal:{deposit_id}"),
    )
    .with_correlation(correlation_id)
    .with_description("credited deposit invalidated by chain reorganisation")
    .credit(custody, asset_id, amount);

    if from_available.is_positive() {
        posting = posting.debit(available, asset_id, from_available);
    }
    if shortfall.is_positive() {
        let deficit = user_account(tx, AccountType::UserDeficit, user_id, asset_id).await?;
        tracing::warn!(
            %user_id, %deposit_id, shortfall = %shortfall,
            "reorg reversal exceeds spendable balance; booking receivable to user_deficit"
        );
        metrics::counter!("chainrail_ledger_deficits_total").increment(1);
        posting = posting.debit(deficit, asset_id, shortfall);
    }

    post_transaction(tx, &posting).await
}

/// Lock funds for a withdrawal.
///
///   DR user_available  (spendable falls)
///   CR user_reserved   (locked rises)
///
/// The `ledger_accounts_non_negative` CHECK on `user_available` is what makes
/// concurrent withdrawals safe: the loser of a race fails here rather than
/// overdrawing.
pub async fn reserve_withdrawal(
    tx: &mut Tx<'_>,
    withdrawal_id: Uuid,
    user_id: Uuid,
    asset_id: Uuid,
    amount: Amount,
    correlation_id: Option<String>,
) -> Result<PostResult> {
    require_positive(amount)?;
    let available = user_account(tx, AccountType::UserAvailable, user_id, asset_id).await?;
    let reserved = user_account(tx, AccountType::UserReserved, user_id, asset_id).await?;

    // Take the row lock before the balance check so the check-then-act sequence
    // is atomic and contending requests queue rather than all failing.
    let current = lock_user_account(tx, AccountType::UserAvailable, user_id, asset_id)
        .await?
        .map(|a| a.balance)
        .unwrap_or(Amount::ZERO);
    if current < amount {
        return Err(Error::InsufficientBalance {
            available: current.to_string(),
            requested: amount.to_string(),
        });
    }

    let posting = Posting::new(
        TransactionKind::WithdrawalReserve,
        ReferenceType::Withdrawal,
        Some(withdrawal_id),
        format!("withdrawal_reserve:{withdrawal_id}"),
    )
    .with_correlation(correlation_id)
    .with_description("funds reserved for withdrawal")
    .debit(available, asset_id, amount)
    .credit(reserved, asset_id, amount);

    post_transaction(tx, &posting).await
}

/// Return reserved funds to the user. Only legal before broadcast.
///
///   DR user_reserved   CR user_available
pub async fn release_reservation(
    tx: &mut Tx<'_>,
    withdrawal_id: Uuid,
    user_id: Uuid,
    asset_id: Uuid,
    amount: Amount,
    correlation_id: Option<String>,
) -> Result<PostResult> {
    require_positive(amount)?;
    let available = user_account(tx, AccountType::UserAvailable, user_id, asset_id).await?;
    let reserved = user_account(tx, AccountType::UserReserved, user_id, asset_id).await?;

    let posting = Posting::new(
        TransactionKind::WithdrawalRelease,
        ReferenceType::Withdrawal,
        Some(withdrawal_id),
        format!("withdrawal_release:{withdrawal_id}"),
    )
    .with_correlation(correlation_id)
    .with_description("withdrawal cancelled or failed before broadcast")
    .debit(reserved, asset_id, amount)
    .credit(available, asset_id, amount);

    post_transaction(tx, &posting).await
}

/// Record value leaving custody onto the wire.
///
///   DR withdrawal_clearing  CR exchange_custody
///
/// Clearing holds the value between broadcast and confirmation, so custody
/// always reflects what we actually control on chain.
pub async fn record_broadcast(
    tx: &mut Tx<'_>,
    withdrawal_id: Uuid,
    asset_id: Uuid,
    amount: Amount,
    correlation_id: Option<String>,
) -> Result<PostResult> {
    require_positive(amount)?;
    let custody = system_account(tx, AccountType::ExchangeCustody, asset_id).await?;
    let clearing = system_account(tx, AccountType::WithdrawalClearing, asset_id).await?;

    let posting = Posting::new(
        TransactionKind::WithdrawalBroadcast,
        ReferenceType::Withdrawal,
        Some(withdrawal_id),
        format!("withdrawal_broadcast:{withdrawal_id}"),
    )
    .with_correlation(correlation_id)
    .with_description("withdrawal transaction broadcast")
    .debit(clearing, asset_id, amount)
    .credit(custody, asset_id, amount);

    post_transaction(tx, &posting).await
}

/// Settle a confirmed withdrawal: the user's liability is extinguished and the
/// clearing account is drained.
///
///   DR user_reserved  CR withdrawal_clearing
pub async fn settle_withdrawal(
    tx: &mut Tx<'_>,
    withdrawal_id: Uuid,
    user_id: Uuid,
    asset_id: Uuid,
    amount: Amount,
    correlation_id: Option<String>,
) -> Result<PostResult> {
    require_positive(amount)?;
    let reserved = user_account(tx, AccountType::UserReserved, user_id, asset_id).await?;
    let clearing = system_account(tx, AccountType::WithdrawalClearing, asset_id).await?;

    let posting = Posting::new(
        TransactionKind::WithdrawalSettle,
        ReferenceType::Withdrawal,
        Some(withdrawal_id),
        format!("withdrawal_settle:{withdrawal_id}"),
    )
    .with_correlation(correlation_id)
    .with_description("withdrawal confirmed on chain")
    .debit(reserved, asset_id, amount)
    .credit(clearing, asset_id, amount);

    post_transaction(tx, &posting).await
}

/// Undo the broadcast leg when a broadcast transaction is observed to have
/// failed (reverted) on chain. The value never left, so it returns to custody.
///
///   DR exchange_custody  CR withdrawal_clearing
pub async fn reverse_broadcast(
    tx: &mut Tx<'_>,
    withdrawal_id: Uuid,
    asset_id: Uuid,
    amount: Amount,
    correlation_id: Option<String>,
) -> Result<PostResult> {
    require_positive(amount)?;
    let custody = system_account(tx, AccountType::ExchangeCustody, asset_id).await?;
    let clearing = system_account(tx, AccountType::WithdrawalClearing, asset_id).await?;

    let posting = Posting::new(
        TransactionKind::WithdrawalBroadcastReversal,
        ReferenceType::Withdrawal,
        Some(withdrawal_id),
        format!("withdrawal_broadcast_reversal:{withdrawal_id}"),
    )
    .with_correlation(correlation_id)
    .with_description("broadcast transaction failed on chain; value returned to custody")
    .debit(custody, asset_id, amount)
    .credit(clearing, asset_id, amount);

    post_transaction(tx, &posting).await
}

/// Book gas actually paid. Charged to the exchange, not to the user, in v0.1.
///
///   DR network_fee (expense)  CR exchange_custody (native asset leaves)
pub async fn record_network_fee(
    tx: &mut Tx<'_>,
    withdrawal_id: Uuid,
    native_asset_id: Uuid,
    fee: Amount,
    correlation_id: Option<String>,
) -> Result<Option<PostResult>> {
    if fee.is_zero() {
        return Ok(None);
    }
    require_positive(fee)?;
    let custody = system_account(tx, AccountType::ExchangeCustody, native_asset_id).await?;
    let fee_account = system_account(tx, AccountType::NetworkFee, native_asset_id).await?;

    let posting = Posting::new(
        TransactionKind::NetworkFee,
        ReferenceType::Withdrawal,
        Some(withdrawal_id),
        format!("network_fee:{withdrawal_id}"),
    )
    .with_correlation(correlation_id)
    .with_description("gas paid for withdrawal")
    .debit(fee_account, native_asset_id, fee)
    .credit(custody, native_asset_id, fee);

    post_transaction(tx, &posting).await.map(Some)
}

/// Record the exchange funding its own custody (hot-wallet top-up).
///
///   DR exchange_custody (we now hold more coins)
///   CR treasury         (backed by exchange capital, not by a user liability)
///
/// Without this, custody could only ever be increased by a user deposit, and
/// paying gas -- which draws down custody of the native asset -- would be
/// impossible. An operator action, not something a user request can trigger.
pub async fn fund_custody(
    tx: &mut Tx<'_>,
    asset_id: Uuid,
    amount: Amount,
    reference: &str,
    correlation_id: Option<String>,
) -> Result<PostResult> {
    require_positive(amount)?;
    let custody = system_account(tx, AccountType::ExchangeCustody, asset_id).await?;
    let treasury = system_account(tx, AccountType::Treasury, asset_id).await?;

    let posting = Posting::new(
        TransactionKind::Adjustment,
        ReferenceType::Manual,
        None,
        format!("custody_funding:{reference}"),
    )
    .with_correlation(correlation_id)
    .with_description("hot wallet funded from exchange treasury")
    .debit(custody, asset_id, amount)
    .credit(treasury, asset_id, amount);

    post_transaction(tx, &posting).await
}

fn require_positive(amount: Amount) -> Result<()> {
    if amount.is_positive() {
        Ok(())
    } else {
        Err(Error::InvalidAmount(format!(
            "posting amount must be positive, got {amount}"
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn non_positive_amounts_are_refused_before_touching_the_database() {
        assert!(require_positive(Amount::ZERO).is_err());
        assert!(require_positive(Amount::new(-1)).is_err());
        assert!(require_positive(Amount::new(1)).is_ok());
    }
}
