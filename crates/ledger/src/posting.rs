//! Construction and validation of a ledger posting.
//!
//! A `Posting` is validated in Rust *before* it reaches the database. The
//! database re-checks everything (see migrations/0003_ledger.sql); validating
//! twice is deliberate -- the Rust layer gives precise errors, the SQL layer
//! guarantees no other writer can bypass them.

use chainrail_common::{Amount, Direction, Error, Result};
use std::fmt;
use uuid::Uuid;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransactionKind {
    DepositCredit,
    DepositReversal,
    WithdrawalReserve,
    WithdrawalRelease,
    WithdrawalBroadcast,
    WithdrawalSettle,
    WithdrawalBroadcastReversal,
    NetworkFee,
    Adjustment,
}

impl TransactionKind {
    pub fn as_str(self) -> &'static str {
        match self {
            TransactionKind::DepositCredit => "deposit_credit",
            TransactionKind::DepositReversal => "deposit_reversal",
            TransactionKind::WithdrawalReserve => "withdrawal_reserve",
            TransactionKind::WithdrawalRelease => "withdrawal_release",
            TransactionKind::WithdrawalBroadcast => "withdrawal_broadcast",
            TransactionKind::WithdrawalSettle => "withdrawal_settle",
            TransactionKind::WithdrawalBroadcastReversal => "withdrawal_broadcast_reversal",
            TransactionKind::NetworkFee => "network_fee",
            TransactionKind::Adjustment => "adjustment",
        }
    }
}

impl fmt::Display for TransactionKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReferenceType {
    Deposit,
    Withdrawal,
    Manual,
    Reorg,
}

impl ReferenceType {
    pub fn as_str(self) -> &'static str {
        match self {
            ReferenceType::Deposit => "deposit",
            ReferenceType::Withdrawal => "withdrawal",
            ReferenceType::Manual => "manual",
            ReferenceType::Reorg => "reorg",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EntryRequest {
    pub account_id: Uuid,
    pub asset_id: Uuid,
    pub amount: Amount,
    pub direction: Direction,
}

impl EntryRequest {
    pub fn debit(account_id: Uuid, asset_id: Uuid, amount: Amount) -> Self {
        EntryRequest {
            account_id,
            asset_id,
            amount,
            direction: Direction::Debit,
        }
    }
    pub fn credit(account_id: Uuid, asset_id: Uuid, amount: Amount) -> Self {
        EntryRequest {
            account_id,
            asset_id,
            amount,
            direction: Direction::Credit,
        }
    }
}

#[derive(Debug, Clone)]
pub struct Posting {
    pub kind: TransactionKind,
    pub reference_type: ReferenceType,
    pub reference_id: Option<Uuid>,
    /// Natural key of the financial event. Re-posting the same key is a no-op
    /// that returns the original transaction.
    pub idempotency_key: String,
    pub correlation_id: Option<String>,
    pub description: Option<String>,
    pub entries: Vec<EntryRequest>,
}

impl Posting {
    pub fn new(
        kind: TransactionKind,
        reference_type: ReferenceType,
        reference_id: Option<Uuid>,
        idempotency_key: impl Into<String>,
    ) -> Self {
        Posting {
            kind,
            reference_type,
            reference_id,
            idempotency_key: idempotency_key.into(),
            correlation_id: None,
            description: None,
            entries: Vec::new(),
        }
    }

    pub fn with_correlation(mut self, correlation_id: Option<String>) -> Self {
        self.correlation_id = correlation_id;
        self
    }

    pub fn with_description(mut self, description: impl Into<String>) -> Self {
        self.description = Some(description.into());
        self
    }

    pub fn debit(mut self, account_id: Uuid, asset_id: Uuid, amount: Amount) -> Self {
        self.entries
            .push(EntryRequest::debit(account_id, asset_id, amount));
        self
    }

    pub fn credit(mut self, account_id: Uuid, asset_id: Uuid, amount: Amount) -> Self {
        self.entries
            .push(EntryRequest::credit(account_id, asset_id, amount));
        self
    }

    /// Net of `sum(debits) - sum(credits)`. Zero means the posting balances.
    pub fn net(&self) -> Result<i128> {
        let mut net: i128 = 0;
        for e in &self.entries {
            let signed = e
                .direction
                .sign()
                .checked_mul(e.amount.raw())
                .ok_or(Error::AmountOverflow)?;
            net = net.checked_add(signed).ok_or(Error::AmountOverflow)?;
        }
        Ok(net)
    }

    pub fn validate(&self) -> Result<()> {
        if self.idempotency_key.trim().is_empty() || self.idempotency_key.len() > 200 {
            return Err(Error::Validation(
                "ledger idempotency key must be 1..=200 chars".into(),
            ));
        }
        if self.entries.len() < 2 {
            return Err(Error::UnbalancedLedgerTransaction {
                net: format!("only {} entries", self.entries.len()),
            });
        }
        for e in &self.entries {
            if !e.amount.is_positive() {
                return Err(Error::InvalidAmount(
                    "ledger entry amounts must be strictly positive; use `direction` for sign"
                        .into(),
                ));
            }
        }
        let net = self.net()?;
        if net != 0 {
            return Err(Error::UnbalancedLedgerTransaction {
                net: net.to_string(),
            });
        }
        Ok(())
    }

    /// Entries sorted by account id.
    ///
    /// The balance trigger takes a row lock per entry, so inserting in a
    /// consistent global order means two concurrent postings touching the same
    /// pair of accounts acquire those locks in the same order and cannot
    /// deadlock.
    pub fn ordered_entries(&self) -> Vec<EntryRequest> {
        let mut e = self.entries.clone();
        e.sort_by_key(|x| (x.account_id, x.direction.as_str()));
        e
    }

    pub fn total_debits(&self) -> Result<Amount> {
        chainrail_common::money::checked_sum(
            self.entries
                .iter()
                .filter(|e| e.direction == Direction::Debit)
                .map(|e| e.amount),
        )
    }

    pub fn total_credits(&self) -> Result<Amount> {
        chainrail_common::money::checked_sum(
            self.entries
                .iter()
                .filter(|e| e.direction == Direction::Credit)
                .map(|e| e.amount),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ids() -> (Uuid, Uuid, Uuid) {
        (
            Uuid::from_u128(1), // account a
            Uuid::from_u128(2), // account b
            Uuid::from_u128(9), // asset
        )
    }

    #[test]
    fn balanced_posting_validates() {
        let (a, b, asset) = ids();
        let p = Posting::new(
            TransactionKind::DepositCredit,
            ReferenceType::Deposit,
            None,
            "dep:1",
        )
        .debit(a, asset, Amount::new(100))
        .credit(b, asset, Amount::new(100));
        p.validate().unwrap();
        assert_eq!(p.net().unwrap(), 0);
        assert_eq!(p.total_debits().unwrap(), Amount::new(100));
        assert_eq!(p.total_credits().unwrap(), Amount::new(100));
    }

    #[test]
    fn unbalanced_posting_is_rejected_with_the_net() {
        let (a, b, asset) = ids();
        let p = Posting::new(
            TransactionKind::Adjustment,
            ReferenceType::Manual,
            None,
            "k",
        )
        .debit(a, asset, Amount::new(100))
        .credit(b, asset, Amount::new(99));
        match p.validate() {
            Err(Error::UnbalancedLedgerTransaction { net }) => assert_eq!(net, "1"),
            other => panic!("expected unbalanced error, got {other:?}"),
        }
    }

    #[test]
    fn multi_leg_postings_balance() {
        let (a, b, asset) = ids();
        let c = Uuid::from_u128(3);
        let p = Posting::new(
            TransactionKind::DepositReversal,
            ReferenceType::Reorg,
            None,
            "k",
        )
        .debit(a, asset, Amount::new(30))
        .debit(b, asset, Amount::new(70))
        .credit(c, asset, Amount::new(100));
        p.validate().unwrap();
    }

    #[test]
    fn single_sided_posting_is_rejected() {
        let (a, _, asset) = ids();
        let p = Posting::new(
            TransactionKind::Adjustment,
            ReferenceType::Manual,
            None,
            "k",
        )
        .debit(a, asset, Amount::new(1));
        assert!(p.validate().is_err());
    }

    #[test]
    fn non_positive_entry_amounts_are_rejected() {
        let (a, b, asset) = ids();
        for bad in [Amount::ZERO, Amount::new(-5)] {
            let p = Posting::new(
                TransactionKind::Adjustment,
                ReferenceType::Manual,
                None,
                "k",
            )
            .debit(a, asset, bad)
            .credit(b, asset, bad);
            assert!(p.validate().is_err(), "accepted amount {bad}");
        }
    }

    #[test]
    fn empty_or_oversized_idempotency_keys_rejected() {
        let (a, b, asset) = ids();
        for key in ["", "  ", &"x".repeat(201)] {
            let p = Posting::new(
                TransactionKind::Adjustment,
                ReferenceType::Manual,
                None,
                key,
            )
            .debit(a, asset, Amount::new(1))
            .credit(b, asset, Amount::new(1));
            assert!(p.validate().is_err(), "accepted key {key:?}");
        }
    }

    #[test]
    fn entries_are_ordered_by_account_for_deadlock_freedom() {
        let asset = Uuid::from_u128(9);
        let hi = Uuid::from_u128(999);
        let lo = Uuid::from_u128(1);
        let p = Posting::new(
            TransactionKind::Adjustment,
            ReferenceType::Manual,
            None,
            "k",
        )
        .debit(hi, asset, Amount::new(5))
        .credit(lo, asset, Amount::new(5));
        let ordered = p.ordered_entries();
        assert_eq!(ordered[0].account_id, lo);
        assert_eq!(ordered[1].account_id, hi);

        // The reverse construction order must produce the identical sequence,
        // which is precisely the property that prevents lock-order deadlocks.
        let q = Posting::new(
            TransactionKind::Adjustment,
            ReferenceType::Manual,
            None,
            "k",
        )
        .credit(lo, asset, Amount::new(5))
        .debit(hi, asset, Amount::new(5));
        assert_eq!(
            ordered.iter().map(|e| e.account_id).collect::<Vec<_>>(),
            q.ordered_entries()
                .iter()
                .map(|e| e.account_id)
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn overflow_in_net_is_detected_not_wrapped() {
        let (a, b, asset) = ids();
        let p = Posting::new(
            TransactionKind::Adjustment,
            ReferenceType::Manual,
            None,
            "k",
        )
        .debit(a, asset, Amount::new(i128::MAX))
        .debit(b, asset, Amount::new(i128::MAX));
        assert!(matches!(p.net(), Err(Error::AmountOverflow)));
    }
}
