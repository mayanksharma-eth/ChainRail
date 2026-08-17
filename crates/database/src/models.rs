//! Row types. These mirror the schema exactly; domain logic lives elsewhere.

use chainrail_common::{Address, Amount, ChainId, Direction, Hash32};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::fmt;
use std::str::FromStr;
use uuid::Uuid;

use chainrail_common::Error;

#[derive(Debug, Clone, sqlx::FromRow, Serialize)]
pub struct User {
    pub id: Uuid,
    pub external_id: String,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, sqlx::FromRow, Serialize)]
pub struct Asset {
    pub id: Uuid,
    pub symbol: String,
    pub chain: String,
    pub contract_address: Option<String>,
    pub decimals: i16,
}

impl Asset {
    pub fn is_native(&self) -> bool {
        self.contract_address.is_none()
    }
    pub fn decimals_u8(&self) -> u8 {
        self.decimals.clamp(0, 36) as u8
    }
}

#[derive(Debug, Clone, sqlx::FromRow, Serialize)]
pub struct DepositAddress {
    pub id: Uuid,
    pub user_id: Uuid,
    pub chain: String,
    pub address: String,
    pub derivation_path: Option<String>,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum BlockStatus {
    Canonical,
    Orphaned,
}

impl BlockStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            BlockStatus::Canonical => "canonical",
            BlockStatus::Orphaned => "orphaned",
        }
    }
}

#[derive(Debug, Clone, sqlx::FromRow)]
pub struct ChainBlock {
    pub chain: String,
    pub hash: Hash32,
    pub height: i64,
    pub parent_hash: Hash32,
    pub status: String,
    pub observed_at: DateTime<Utc>,
}

impl ChainBlock {
    pub fn is_canonical(&self) -> bool {
        self.status == "canonical"
    }
}

#[derive(Debug, Clone, sqlx::FromRow)]
pub struct ChainCursor {
    pub chain: String,
    pub last_processed_height: i64,
    pub last_processed_hash: Hash32,
    pub head_height: i64,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, sqlx::FromRow, Serialize)]
pub struct BlockchainTransaction {
    pub id: Uuid,
    pub chain: String,
    pub tx_hash: Hash32,
    /// `-1` encodes a native transfer, which has no log index.
    pub log_index: i64,
    pub block_number: i64,
    pub block_hash: Hash32,
    pub from_address: Address,
    pub to_address: Address,
    pub asset_id: Uuid,
    pub amount_raw: Amount,
    pub status: String,
    pub observed_at: DateTime<Utc>,
}

/// A transfer detected on chain, before it has been persisted.
#[derive(Debug, Clone, PartialEq)]
pub struct ObservedTransfer {
    pub chain: ChainId,
    pub tx_hash: Hash32,
    pub log_index: i64,
    pub block_number: u64,
    pub block_hash: Hash32,
    pub from_address: Address,
    pub to_address: Address,
    pub amount_raw: Amount,
    /// `None` for the chain's native asset.
    pub contract_address: Option<Address>,
}

// ---------------------------------------------------------------- deposits ---

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum DepositStatus {
    Observed,
    Confirming,
    Confirmed,
    Credited,
    Reorged,
    Failed,
}

impl DepositStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            DepositStatus::Observed => "observed",
            DepositStatus::Confirming => "confirming",
            DepositStatus::Confirmed => "confirmed",
            DepositStatus::Credited => "credited",
            DepositStatus::Reorged => "reorged",
            DepositStatus::Failed => "failed",
        }
    }

    pub fn is_terminal(self) -> bool {
        matches!(
            self,
            DepositStatus::Credited | DepositStatus::Reorged | DepositStatus::Failed
        )
    }

    /// Whether funds have been made spendable for this deposit.
    pub fn is_credited(self) -> bool {
        matches!(self, DepositStatus::Credited)
    }

    pub fn can_transition_to(self, next: DepositStatus) -> bool {
        use DepositStatus::*;
        match (self, next) {
            (a, b) if a == b => true,
            (Observed, Confirming | Confirmed | Reorged | Failed) => true,
            (Confirming, Confirmed | Reorged | Failed) => true,
            (Confirmed, Credited | Reorged | Failed) => true,
            // A credited deposit may still be invalidated by a deep reorg; the
            // transition is legal but requires a compensating ledger posting.
            (Credited, Reorged) => true,
            _ => false,
        }
    }
}

impl FromStr for DepositStatus {
    type Err = Error;
    fn from_str(s: &str) -> Result<Self, Error> {
        Ok(match s {
            "observed" => DepositStatus::Observed,
            "confirming" => DepositStatus::Confirming,
            "confirmed" => DepositStatus::Confirmed,
            "credited" => DepositStatus::Credited,
            "reorged" => DepositStatus::Reorged,
            "failed" => DepositStatus::Failed,
            other => return Err(Error::Validation(format!("unknown deposit status {other}"))),
        })
    }
}

impl fmt::Display for DepositStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, sqlx::FromRow)]
pub struct Deposit {
    pub id: Uuid,
    pub user_id: Uuid,
    pub blockchain_transaction_id: Uuid,
    pub asset_id: Uuid,
    pub amount_raw: Amount,
    pub confirmations: i64,
    pub status: String,
    pub ledger_transaction_id: Option<Uuid>,
    pub reversal_ledger_transaction_id: Option<Uuid>,
    pub failure_reason: Option<String>,
    pub credited_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl Deposit {
    pub fn status(&self) -> Result<DepositStatus, Error> {
        DepositStatus::from_str(&self.status)
    }
}

/// A deposit joined with the chain facts needed to confirm or reorg it.
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct PendingDeposit {
    pub deposit_id: Uuid,
    pub user_id: Uuid,
    pub asset_id: Uuid,
    pub asset_symbol: String,
    pub chain: String,
    pub tx_hash: Hash32,
    pub block_number: i64,
    pub block_hash: Hash32,
    pub amount_raw: Amount,
    pub status: String,
    pub confirmations: i64,
}

// ------------------------------------------------------------- withdrawals ---

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum WithdrawalStatus {
    Requested,
    Validated,
    Approved,
    Signing,
    Broadcast,
    Confirming,
    Completed,
    Failed,
    Cancelled,
}

impl WithdrawalStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            WithdrawalStatus::Requested => "requested",
            WithdrawalStatus::Validated => "validated",
            WithdrawalStatus::Approved => "approved",
            WithdrawalStatus::Signing => "signing",
            WithdrawalStatus::Broadcast => "broadcast",
            WithdrawalStatus::Confirming => "confirming",
            WithdrawalStatus::Completed => "completed",
            WithdrawalStatus::Failed => "failed",
            WithdrawalStatus::Cancelled => "cancelled",
        }
    }

    pub fn is_terminal(self) -> bool {
        matches!(
            self,
            WithdrawalStatus::Completed | WithdrawalStatus::Failed | WithdrawalStatus::Cancelled
        )
    }

    /// True once a signed transaction may exist on the network. Past this point
    /// the withdrawal can no longer be cancelled -- we cannot un-send a
    /// transaction -- and any failure requires on-chain reconciliation.
    pub fn funds_may_have_left(self) -> bool {
        matches!(
            self,
            WithdrawalStatus::Broadcast
                | WithdrawalStatus::Confirming
                | WithdrawalStatus::Completed
        )
    }

    /// Mirrors the `withdrawals_guard_transition` trigger exactly. Kept in sync
    /// by `state_machine_matches_database_trigger` in tests/integration.
    pub fn can_transition_to(self, next: WithdrawalStatus) -> bool {
        use WithdrawalStatus::*;
        if self == next {
            return true;
        }
        match self {
            Requested => matches!(next, Validated | Failed | Cancelled),
            Validated => matches!(next, Approved | Failed | Cancelled),
            Approved => matches!(next, Signing | Failed | Cancelled),
            Signing => matches!(next, Broadcast | Failed),
            Broadcast => matches!(next, Confirming | Completed | Failed),
            Confirming => matches!(next, Completed | Failed),
            Completed | Failed | Cancelled => false,
        }
    }

    pub fn transition_to(self, next: WithdrawalStatus) -> Result<WithdrawalStatus, Error> {
        if self.can_transition_to(next) {
            Ok(next)
        } else {
            Err(Error::InvalidStateTransition {
                entity: "withdrawal",
                from: self.as_str().into(),
                to: next.as_str().into(),
            })
        }
    }
}

impl FromStr for WithdrawalStatus {
    type Err = Error;
    fn from_str(s: &str) -> Result<Self, Error> {
        Ok(match s {
            "requested" => WithdrawalStatus::Requested,
            "validated" => WithdrawalStatus::Validated,
            "approved" => WithdrawalStatus::Approved,
            "signing" => WithdrawalStatus::Signing,
            "broadcast" => WithdrawalStatus::Broadcast,
            "confirming" => WithdrawalStatus::Confirming,
            "completed" => WithdrawalStatus::Completed,
            "failed" => WithdrawalStatus::Failed,
            "cancelled" => WithdrawalStatus::Cancelled,
            other => {
                return Err(Error::Validation(format!(
                    "unknown withdrawal status {other}"
                )))
            }
        })
    }
}

impl fmt::Display for WithdrawalStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, sqlx::FromRow)]
pub struct Withdrawal {
    pub id: Uuid,
    pub user_id: Uuid,
    pub asset_id: Uuid,
    pub chain: String,
    pub destination_address: Address,
    pub amount_raw: Amount,
    pub status: String,
    pub idempotency_key: String,
    pub request_fingerprint: String,
    pub tx_hash: Option<Hash32>,
    pub nonce: Option<i64>,
    pub gas_limit: Option<i64>,
    pub max_fee_per_gas: Option<Amount>,
    pub max_priority_fee_per_gas: Option<Amount>,
    pub fee_paid_raw: Option<Amount>,
    pub block_number: Option<i64>,
    pub confirmations: i64,
    pub broadcast_attempts: i32,
    pub reserve_ledger_transaction_id: Option<Uuid>,
    pub settle_ledger_transaction_id: Option<Uuid>,
    pub failure_code: Option<String>,
    pub failure_reason: Option<String>,
    pub correlation_id: Option<String>,
    pub approved_at: Option<DateTime<Utc>>,
    pub broadcast_at: Option<DateTime<Utc>>,
    pub completed_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl Withdrawal {
    pub fn status(&self) -> Result<WithdrawalStatus, Error> {
        WithdrawalStatus::from_str(&self.status)
    }
}

// ------------------------------------------------------------------ ledger ---

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AccountType {
    UserAvailable,
    UserReserved,
    UserDeficit,
    ExchangeCustody,
    WithdrawalClearing,
    NetworkFee,
    Treasury,
}

impl AccountType {
    pub fn as_str(self) -> &'static str {
        match self {
            AccountType::UserAvailable => "user_available",
            AccountType::UserReserved => "user_reserved",
            AccountType::UserDeficit => "user_deficit",
            AccountType::ExchangeCustody => "exchange_custody",
            AccountType::WithdrawalClearing => "withdrawal_clearing",
            AccountType::NetworkFee => "network_fee",
            AccountType::Treasury => "treasury",
        }
    }

    /// Assets and expenses are debit-normal; liabilities are credit-normal.
    pub fn normal_balance(self) -> Direction {
        match self {
            AccountType::UserAvailable | AccountType::UserReserved | AccountType::Treasury => {
                Direction::Credit
            }
            AccountType::UserDeficit
            | AccountType::ExchangeCustody
            | AccountType::WithdrawalClearing
            | AccountType::NetworkFee => Direction::Debit,
        }
    }

    pub fn is_user_scoped(self) -> bool {
        matches!(
            self,
            AccountType::UserAvailable | AccountType::UserReserved | AccountType::UserDeficit
        )
    }

    /// Whether the natural balance may legitimately go negative.
    ///
    /// Clearing accounts swing negative between the settle and broadcast legs
    /// of a withdrawal; user spendable balances never may, which is what stops
    /// concurrent withdrawals from overdrawing an account.
    pub fn allows_negative(self) -> bool {
        matches!(
            self,
            AccountType::WithdrawalClearing | AccountType::Treasury
        )
    }
}

impl FromStr for AccountType {
    type Err = Error;
    fn from_str(s: &str) -> Result<Self, Error> {
        Ok(match s {
            "user_available" => AccountType::UserAvailable,
            "user_reserved" => AccountType::UserReserved,
            "user_deficit" => AccountType::UserDeficit,
            "exchange_custody" => AccountType::ExchangeCustody,
            "withdrawal_clearing" => AccountType::WithdrawalClearing,
            "network_fee" => AccountType::NetworkFee,
            "treasury" => AccountType::Treasury,
            other => return Err(Error::Validation(format!("unknown account type {other}"))),
        })
    }
}

impl fmt::Display for AccountType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, sqlx::FromRow)]
pub struct LedgerAccount {
    pub id: Uuid,
    pub account_type: String,
    pub owner_user_id: Option<Uuid>,
    pub asset_id: Uuid,
    pub normal_balance: Direction,
    pub balance: Amount,
    pub allow_negative: bool,
}

#[derive(Debug, Clone, sqlx::FromRow, Serialize)]
pub struct LedgerTransaction {
    pub id: Uuid,
    pub kind: String,
    pub reference_type: String,
    pub reference_id: Option<Uuid>,
    pub idempotency_key: String,
    pub correlation_id: Option<String>,
    pub description: Option<String>,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, sqlx::FromRow)]
pub struct LedgerEntry {
    pub id: Uuid,
    pub ledger_transaction_id: Uuid,
    pub account_id: Uuid,
    pub asset_id: Uuid,
    pub amount: Amount,
    pub direction: Direction,
    pub balance_after: Amount,
    pub created_at: DateTime<Utc>,
}

/// An entry joined with human-readable context, for `GET /v1/ledger/:user_id`.
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct LedgerEntryView {
    pub id: Uuid,
    pub ledger_transaction_id: Uuid,
    pub account_type: String,
    pub asset_symbol: String,
    pub asset_decimals: i16,
    pub amount: Amount,
    pub direction: Direction,
    pub balance_after: Amount,
    pub kind: String,
    pub reference_type: String,
    pub reference_id: Option<Uuid>,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, sqlx::FromRow)]
pub struct AccountBalance {
    pub account_type: String,
    pub asset_id: Uuid,
    pub asset_symbol: String,
    pub asset_decimals: i16,
    pub chain: String,
    pub balance: Amount,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn withdrawal_state_machine_is_a_dag_to_terminal_states() {
        use WithdrawalStatus::*;
        assert!(Requested.can_transition_to(Validated));
        assert!(!Requested.can_transition_to(Broadcast));
        assert!(!Requested.can_transition_to(Completed));
        assert!(Signing.can_transition_to(Broadcast));
        // Cannot un-send a broadcast transaction.
        assert!(!Broadcast.can_transition_to(Cancelled));
        assert!(!Signing.can_transition_to(Cancelled));
        // Terminal states are absorbing.
        for t in [Completed, Failed, Cancelled] {
            for n in [
                Requested, Validated, Approved, Signing, Broadcast, Confirming,
            ] {
                assert!(!t.can_transition_to(n), "{t} -> {n} allowed");
            }
            assert!(t.is_terminal());
        }
    }

    #[test]
    fn withdrawal_transition_errors_name_both_states() {
        let e = WithdrawalStatus::Requested
            .transition_to(WithdrawalStatus::Completed)
            .unwrap_err();
        assert!(matches!(e, Error::InvalidStateTransition { .. }));
        assert!(e.to_string().contains("requested"));
        assert!(e.to_string().contains("completed"));
    }

    #[test]
    fn funds_at_risk_boundary_is_broadcast() {
        use WithdrawalStatus::*;
        for s in [Requested, Validated, Approved, Signing, Cancelled] {
            assert!(!s.funds_may_have_left(), "{s}");
        }
        for s in [Broadcast, Confirming, Completed] {
            assert!(s.funds_may_have_left(), "{s}");
        }
    }

    #[test]
    fn deposit_credit_is_reachable_only_after_confirmation() {
        use DepositStatus::*;
        assert!(!Observed.can_transition_to(Credited));
        assert!(!Confirming.can_transition_to(Credited));
        assert!(Confirmed.can_transition_to(Credited));
        // A deep reorg can invalidate an already-credited deposit.
        assert!(Credited.can_transition_to(Reorged));
        assert!(!Reorged.can_transition_to(Credited));
        assert!(!Failed.can_transition_to(Credited));
    }

    #[test]
    fn account_normal_balances_follow_accounting_rules() {
        assert_eq!(
            AccountType::UserAvailable.normal_balance(),
            Direction::Credit
        );
        assert_eq!(
            AccountType::UserReserved.normal_balance(),
            Direction::Credit
        );
        assert_eq!(
            AccountType::ExchangeCustody.normal_balance(),
            Direction::Debit
        );
        assert_eq!(AccountType::NetworkFee.normal_balance(), Direction::Debit);
        assert_eq!(AccountType::Treasury.normal_balance(), Direction::Credit);
        // Only clearing and treasury may swing negative. Custody must not:
        // a negative custody balance asserts we hold coins we do not.
        assert!(AccountType::WithdrawalClearing.allows_negative());
        assert!(AccountType::Treasury.allows_negative());
        assert!(!AccountType::UserAvailable.allows_negative());
        assert!(!AccountType::ExchangeCustody.allows_negative());
    }

    #[test]
    fn status_strings_round_trip() {
        for s in [
            WithdrawalStatus::Requested,
            WithdrawalStatus::Broadcast,
            WithdrawalStatus::Completed,
        ] {
            assert_eq!(WithdrawalStatus::from_str(s.as_str()).unwrap(), s);
        }
        for s in [DepositStatus::Observed, DepositStatus::Credited] {
            assert_eq!(DepositStatus::from_str(s.as_str()).unwrap(), s);
        }
        for a in [AccountType::UserAvailable, AccountType::NetworkFee] {
            assert_eq!(AccountType::from_str(a.as_str()).unwrap(), a);
        }
        assert!(WithdrawalStatus::from_str("bogus").is_err());
    }
}
