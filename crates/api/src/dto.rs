//! Request and response bodies.
//!
//! Kept separate from the database row types on purpose: the wire format is a
//! contract with clients, and coupling it to the schema would make any migration
//! a breaking API change.
//!
//! Every monetary value is a decimal *string* of raw units, plus a
//! human-readable `*_formatted` field. No JSON numbers are used for money.

use chainrail_common::{Amount, Direction};
use chainrail_database::models::{
    AccountBalance, BlockchainTransaction, Deposit, DepositAddress, LedgerEntryView, User,
    Withdrawal,
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

// ------------------------------------------------------------------ input ---

#[derive(Debug, Deserialize)]
pub struct CreateUserRequest {
    /// Caller's stable identifier for this user.
    pub external_id: String,
}

#[derive(Debug, Deserialize)]
pub struct CreateDepositAddressRequest {
    pub user_id: Uuid,
    pub chain: String,
}

#[derive(Debug, Deserialize)]
pub struct CreateWithdrawalRequest {
    pub user_id: Uuid,
    pub chain: String,
    pub asset: String,
    /// Raw units as a string, e.g. `"25000000"` for 25 USDC.
    pub amount: Amount,
    pub destination: String,
    pub idempotency_key: String,
}

#[derive(Debug, Deserialize, Default)]
pub struct Pagination {
    pub limit: Option<u32>,
    /// Keyset cursor: the `created_at` of the last item on the previous page.
    pub after: Option<DateTime<Utc>>,
    /// Tie-breaker id, paired with `after`.
    pub after_id: Option<Uuid>,
}

impl Pagination {
    pub fn cursor(&self) -> Option<(DateTime<Utc>, Uuid)> {
        self.after
            .map(|t| (t, self.after_id.unwrap_or_else(Uuid::nil)))
    }
}

#[derive(Debug, Deserialize, Default)]
pub struct DepositFilter {
    pub user_id: Option<Uuid>,
    pub status: Option<String>,
    #[serde(flatten)]
    pub page: Pagination,
}

#[derive(Debug, Deserialize, Default)]
pub struct WithdrawalFilter {
    pub user_id: Option<Uuid>,
    pub status: Option<String>,
    #[serde(flatten)]
    pub page: Pagination,
}

// ----------------------------------------------------------------- output ---

#[derive(Debug, Serialize)]
pub struct Page<T> {
    pub items: Vec<T>,
    /// Cursor for the next page; absent when this is the last page.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<Cursor>,
}

#[derive(Debug, Serialize)]
pub struct Cursor {
    pub after: DateTime<Utc>,
    pub after_id: Uuid,
}

impl<T> Page<T> {
    pub fn new(items: Vec<T>, next_cursor: Option<Cursor>) -> Self {
        Page { items, next_cursor }
    }
}

#[derive(Debug, Serialize)]
pub struct UserResponse {
    pub id: Uuid,
    pub external_id: String,
    pub created_at: DateTime<Utc>,
}

impl From<User> for UserResponse {
    fn from(u: User) -> Self {
        UserResponse {
            id: u.id,
            external_id: u.external_id,
            created_at: u.created_at,
        }
    }
}

#[derive(Debug, Serialize)]
pub struct DepositAddressResponse {
    pub id: Uuid,
    pub user_id: Uuid,
    pub chain: String,
    /// EIP-55 checksummed for display; storage is lowercase.
    pub address: String,
    pub created_at: DateTime<Utc>,
}

impl DepositAddressResponse {
    pub fn new(a: DepositAddress, checksummed: String) -> Self {
        DepositAddressResponse {
            id: a.id,
            user_id: a.user_id,
            chain: a.chain,
            address: checksummed,
            created_at: a.created_at,
        }
    }
}

#[derive(Debug, Serialize)]
pub struct DepositResponse {
    pub id: Uuid,
    pub user_id: Uuid,
    pub chain: String,
    pub asset: String,
    pub amount_raw: Amount,
    pub amount_formatted: String,
    pub status: String,
    pub confirmations: i64,
    pub required_confirmations: Option<u64>,
    pub tx_hash: Option<String>,
    pub block_number: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub failure_reason: Option<String>,
    pub credited_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
}

pub struct DepositView {
    pub deposit: Deposit,
    pub asset_symbol: String,
    pub asset_decimals: u8,
    pub chain: String,
    pub tx_hash: Option<String>,
    pub block_number: Option<i64>,
    pub required_confirmations: Option<u64>,
}

impl From<DepositView> for DepositResponse {
    fn from(v: DepositView) -> Self {
        DepositResponse {
            id: v.deposit.id,
            user_id: v.deposit.user_id,
            chain: v.chain,
            asset: v.asset_symbol,
            amount_raw: v.deposit.amount_raw,
            amount_formatted: v.deposit.amount_raw.format_units(v.asset_decimals),
            status: v.deposit.status,
            confirmations: v.deposit.confirmations,
            required_confirmations: v.required_confirmations,
            tx_hash: v.tx_hash,
            block_number: v.block_number,
            failure_reason: v.deposit.failure_reason,
            credited_at: v.deposit.credited_at,
            created_at: v.deposit.created_at,
        }
    }
}

#[derive(Debug, Serialize)]
pub struct WithdrawalResponse {
    pub id: Uuid,
    pub user_id: Uuid,
    pub chain: String,
    pub asset: String,
    pub amount_raw: Amount,
    pub amount_formatted: String,
    pub destination: String,
    pub status: String,
    pub tx_hash: Option<String>,
    pub confirmations: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fee_paid_raw: Option<Amount>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub failure_code: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub failure_reason: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl WithdrawalResponse {
    pub fn new(
        w: Withdrawal,
        asset_symbol: String,
        decimals: u8,
        checksummed_dest: String,
    ) -> Self {
        WithdrawalResponse {
            id: w.id,
            user_id: w.user_id,
            chain: w.chain,
            asset: asset_symbol,
            amount_raw: w.amount_raw,
            amount_formatted: w.amount_raw.format_units(decimals),
            destination: checksummed_dest,
            status: w.status,
            tx_hash: w.tx_hash.map(|h| h.to_string()),
            confirmations: w.confirmations,
            fee_paid_raw: w.fee_paid_raw,
            failure_code: w.failure_code,
            failure_reason: w.failure_reason,
            created_at: w.created_at,
            updated_at: w.updated_at,
        }
    }
}

#[derive(Debug, Serialize)]
pub struct BalanceResponse {
    pub user_id: Uuid,
    pub balances: Vec<AssetBalance>,
}

#[derive(Debug, Serialize)]
pub struct AssetBalance {
    pub chain: String,
    pub asset: String,
    pub decimals: u8,
    /// Spendable now.
    pub available_raw: Amount,
    pub available_formatted: String,
    /// Locked against in-flight withdrawals.
    pub reserved_raw: Amount,
    pub reserved_formatted: String,
    /// available + reserved.
    pub total_raw: Amount,
    /// A receivable created by a post-credit reorg. Non-zero is exceptional.
    #[serde(skip_serializing_if = "is_zero_amount")]
    pub deficit_raw: Amount,
}

fn is_zero_amount(a: &Amount) -> bool {
    a.is_zero()
}

/// Fold per-account rows into one entry per (chain, asset).
pub fn build_balances(user_id: Uuid, rows: Vec<AccountBalance>) -> BalanceResponse {
    use std::collections::BTreeMap;
    let mut grouped: BTreeMap<(String, String), (u8, Amount, Amount, Amount)> = BTreeMap::new();

    for r in rows {
        let key = (r.chain.clone(), r.asset_symbol.clone());
        let entry = grouped.entry(key).or_insert((
            r.asset_decimals.clamp(0, 36) as u8,
            Amount::ZERO,
            Amount::ZERO,
            Amount::ZERO,
        ));
        match r.account_type.as_str() {
            "user_available" => entry.1 = r.balance,
            "user_reserved" => entry.2 = r.balance,
            "user_deficit" => entry.3 = r.balance,
            _ => {}
        }
    }

    let balances = grouped
        .into_iter()
        .map(
            |((chain, asset), (decimals, available, reserved, deficit))| AssetBalance {
                chain,
                asset,
                decimals,
                available_raw: available,
                available_formatted: available.format_units(decimals),
                reserved_raw: reserved,
                reserved_formatted: reserved.format_units(decimals),
                total_raw: available.checked_add(reserved).unwrap_or(available),
                deficit_raw: deficit,
            },
        )
        .collect();

    BalanceResponse { user_id, balances }
}

#[derive(Debug, Serialize)]
pub struct LedgerEntryResponse {
    pub id: Uuid,
    pub ledger_transaction_id: Uuid,
    pub account_type: String,
    pub asset: String,
    pub amount_raw: Amount,
    pub amount_formatted: String,
    pub direction: String,
    /// Account balance immediately after this entry.
    pub balance_after_raw: Amount,
    pub kind: String,
    pub reference_type: String,
    pub reference_id: Option<Uuid>,
    pub created_at: DateTime<Utc>,
}

impl From<LedgerEntryView> for LedgerEntryResponse {
    fn from(e: LedgerEntryView) -> Self {
        let decimals = e.asset_decimals.clamp(0, 36) as u8;
        LedgerEntryResponse {
            id: e.id,
            ledger_transaction_id: e.ledger_transaction_id,
            account_type: e.account_type,
            asset: e.asset_symbol,
            amount_raw: e.amount,
            amount_formatted: e.amount.format_units(decimals),
            direction: match e.direction {
                Direction::Debit => "debit".into(),
                Direction::Credit => "credit".into(),
            },
            balance_after_raw: e.balance_after,
            kind: e.kind,
            reference_type: e.reference_type,
            reference_id: e.reference_id,
            created_at: e.created_at,
        }
    }
}

#[derive(Debug, Serialize)]
pub struct TransactionResponse {
    pub tx_hash: String,
    pub chain: String,
    pub transfers: Vec<TransferResponse>,
}

#[derive(Debug, Serialize)]
pub struct TransferResponse {
    pub log_index: i64,
    pub block_number: i64,
    pub block_hash: String,
    pub from: String,
    pub to: String,
    pub amount_raw: Amount,
    pub status: String,
    pub observed_at: DateTime<Utc>,
}

impl From<BlockchainTransaction> for TransferResponse {
    fn from(t: BlockchainTransaction) -> Self {
        TransferResponse {
            log_index: t.log_index,
            block_number: t.block_number,
            block_hash: t.block_hash.to_string(),
            from: t.from_address.to_string(),
            to: t.to_address.to_string(),
            amount_raw: t.amount_raw,
            status: t.status,
            observed_at: t.observed_at,
        }
    }
}

#[derive(Debug, Serialize)]
pub struct HealthResponse {
    pub status: &'static str,
    pub service: String,
    pub version: &'static str,
    pub environment: String,
    pub uptime_seconds: u64,
    pub signer_backend: &'static str,
    /// Explicitly surfaced so nobody can mistake a dev signer for real custody.
    pub signer_production_grade: bool,
}

// ------------------------------------------------------------- extraction ---

/// JSON body extractor that reports failures in ChainRail's error shape.
///
/// `axum::Json`'s own rejection returns 422 with a plain-text body. Two problems:
/// clients would receive an error that does not match the documented
/// `{error:{code,message,request_id}}` contract, and 422 is reserved here for a
/// *policy* denial -- a syntactically bad body is a 400.
pub struct ValidJson<T>(pub T);

impl<S, T> axum::extract::FromRequest<S> for ValidJson<T>
where
    T: serde::de::DeserializeOwned,
    S: Send + Sync,
{
    type Rejection = crate::error::ApiError;

    async fn from_request(req: axum::extract::Request, state: &S) -> Result<Self, Self::Rejection> {
        // Recover the request id before consuming the request, so the error can
        // be correlated with its log line.
        let request_id = req
            .extensions()
            .get::<crate::middleware::RequestContext>()
            .map(|c| c.request_id.clone())
            .unwrap_or_default();

        match axum::Json::<T>::from_request(req, state).await {
            Ok(axum::Json(value)) => Ok(ValidJson(value)),
            Err(rejection) => Err(crate::error::ApiError {
                status: axum::http::StatusCode::BAD_REQUEST,
                inner: chainrail_common::Error::Validation(sanitize_rejection(
                    &rejection.body_text(),
                )),
                request_id,
            }),
        }
    }
}

/// Serde's message names the offending field and type, which is genuinely useful
/// to a client. Bound it so a hostile payload cannot inflate the response.
fn sanitize_rejection(text: &str) -> String {
    chainrail_common::chain::truncate_for_log(text.trim())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(account_type: &str, balance: i128) -> AccountBalance {
        AccountBalance {
            account_type: account_type.into(),
            asset_id: Uuid::nil(),
            asset_symbol: "USDC".into(),
            asset_decimals: 6,
            chain: "base-sepolia".into(),
            balance: Amount::new(balance),
        }
    }

    #[test]
    fn balances_fold_account_types_into_one_entry_per_asset() {
        let r = build_balances(
            Uuid::nil(),
            vec![
                row("user_available", 750_000_000),
                row("user_reserved", 250_000_000),
            ],
        );
        assert_eq!(r.balances.len(), 1);
        let b = &r.balances[0];
        assert_eq!(b.available_raw, Amount::new(750_000_000));
        assert_eq!(b.reserved_raw, Amount::new(250_000_000));
        assert_eq!(b.total_raw, Amount::new(1_000_000_000));
        assert_eq!(b.available_formatted, "750");
        assert_eq!(b.reserved_formatted, "250");
    }

    #[test]
    fn missing_account_types_read_as_zero_not_as_absent() {
        // A user who has never withdrawn has no reserved account row.
        let r = build_balances(Uuid::nil(), vec![row("user_available", 100)]);
        assert_eq!(r.balances[0].reserved_raw, Amount::ZERO);
        assert_eq!(r.balances[0].total_raw, Amount::new(100));
    }

    #[test]
    fn system_accounts_are_never_reported_as_user_balances() {
        let mut custody = row("exchange_custody", 999);
        custody.account_type = "exchange_custody".into();
        let r = build_balances(Uuid::nil(), vec![row("user_available", 10), custody]);
        assert_eq!(r.balances.len(), 1);
        assert_eq!(r.balances[0].available_raw, Amount::new(10));
        assert_eq!(r.balances[0].total_raw, Amount::new(10));
    }

    #[test]
    fn deficit_is_hidden_when_zero_and_shown_when_not() {
        let clean = build_balances(Uuid::nil(), vec![row("user_available", 10)]);
        let json = serde_json::to_string(&clean).unwrap();
        assert!(
            !json.contains("deficit_raw"),
            "zero deficit should be omitted"
        );

        let bad = build_balances(
            Uuid::nil(),
            vec![row("user_available", 0), row("user_deficit", 500)],
        );
        let json = serde_json::to_string(&bad).unwrap();
        assert!(json.contains("\"deficit_raw\":\"500\""));
    }

    #[test]
    fn multiple_assets_and_chains_stay_separate_and_sorted() {
        let mut eth = row("user_available", 5);
        eth.asset_symbol = "ETH".into();
        eth.asset_decimals = 18;
        let mut other_chain = row("user_available", 7);
        other_chain.chain = "ethereum".into();
        let r = build_balances(
            Uuid::nil(),
            vec![row("user_available", 1), eth, other_chain],
        );
        assert_eq!(r.balances.len(), 3);
        // BTreeMap ordering: (chain, asset) ascending.
        assert_eq!(
            r.balances
                .iter()
                .map(|b| (b.chain.as_str(), b.asset.as_str()))
                .collect::<Vec<_>>(),
            vec![
                ("base-sepolia", "ETH"),
                ("base-sepolia", "USDC"),
                ("ethereum", "USDC")
            ]
        );
    }

    #[test]
    fn amounts_serialize_as_strings_everywhere() {
        let r = build_balances(
            Uuid::nil(),
            vec![row("user_available", 9_007_199_254_740_993)],
        );
        let json = serde_json::to_string(&r).unwrap();
        assert!(json.contains("\"9007199254740993\""), "{json}");
        // No bare numeric money fields.
        assert!(!json.contains(":9007199254740993"));
    }

    #[test]
    fn pagination_cursor_requires_both_parts_or_defaults_the_id() {
        let p = Pagination {
            limit: Some(10),
            after: None,
            after_id: None,
        };
        assert!(p.cursor().is_none());
        let now = Utc::now();
        let p = Pagination {
            limit: None,
            after: Some(now),
            after_id: None,
        };
        assert_eq!(p.cursor(), Some((now, Uuid::nil())));
    }

    #[test]
    fn withdrawal_amounts_are_formatted_with_asset_decimals() {
        // 25 USDC at 6 decimals.
        assert_eq!(Amount::new(25_000_000).format_units(6), "25");
        // 0.5 ETH at 18 decimals.
        assert_eq!(Amount::new(500_000_000_000_000_000).format_units(18), "0.5");
    }
}
