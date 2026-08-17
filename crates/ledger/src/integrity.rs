//! Ledger integrity verification.
//!
//! Three independent checks, each of which should be impossible to fail given
//! the schema's triggers and constraints. They exist because "impossible"
//! failures in an accounting system are exactly the ones you must detect: a
//! bad migration, a manual `psql` session, or a trigger accidentally dropped
//! would all show up here.
//!
//! Run on a schedule and expose the result as a metric; a non-clean report is
//! a page-the-operator event.

use chainrail_common::{Amount, Result};
use chainrail_database::SqlxResultExt;
use serde::Serialize;
use sqlx::{Executor, Postgres};
use uuid::Uuid;

#[derive(Debug, Clone, Serialize)]
pub struct IntegrityReport {
    pub transactions_checked: i64,
    pub accounts_checked: i64,
    /// Transactions where `sum(debits) != sum(credits)`. Must always be empty.
    pub unbalanced_transactions: Vec<UnbalancedTransaction>,
    /// Accounts whose cached balance disagrees with the sum of their entries.
    pub balance_drift: Vec<BalanceDrift>,
    /// Accounts holding a negative balance that are not permitted to.
    pub illegal_negative_balances: Vec<IllegalNegative>,
    /// Assets where user liabilities exceed what custody says we hold.
    pub solvency: Vec<SolvencyRow>,
}

impl IntegrityReport {
    pub fn is_clean(&self) -> bool {
        self.unbalanced_transactions.is_empty()
            && self.balance_drift.is_empty()
            && self.illegal_negative_balances.is_empty()
    }

    /// Problems that indicate corruption, as opposed to an under-funded hot
    /// wallet (which `solvency` reports but which is an operational matter).
    pub fn problem_count(&self) -> usize {
        self.unbalanced_transactions.len()
            + self.balance_drift.len()
            + self.illegal_negative_balances.len()
    }
}

#[derive(Debug, Clone, Serialize, sqlx::FromRow)]
pub struct UnbalancedTransaction {
    pub ledger_transaction_id: Uuid,
    pub kind: String,
    pub net: Amount,
}

#[derive(Debug, Clone, Serialize, sqlx::FromRow)]
pub struct BalanceDrift {
    pub account_id: Uuid,
    pub account_type: String,
    pub cached_balance: Amount,
    pub derived_balance: Amount,
}

#[derive(Debug, Clone, Serialize, sqlx::FromRow)]
pub struct IllegalNegative {
    pub account_id: Uuid,
    pub account_type: String,
    pub balance: Amount,
}

#[derive(Debug, Clone, Serialize, sqlx::FromRow)]
pub struct SolvencyRow {
    pub asset_symbol: String,
    pub chain: String,
    /// What we owe users: available + reserved, less any recorded deficits.
    pub total_user_liability: Amount,
    /// What the ledger says we hold on chain.
    pub custody_balance: Amount,
    pub clearing_balance: Amount,
}

impl SolvencyRow {
    /// Custody plus in-flight value, minus what we owe. Negative means the
    /// ledger believes we are short.
    pub fn surplus(&self) -> Result<Amount> {
        self.custody_balance
            .checked_add(self.clearing_balance)?
            .checked_sub(self.total_user_liability)
    }
}

/// Full verification pass. `O(entries)`; intended for a periodic job, not a
/// request path.
pub async fn verify_ledger_integrity<'e, E>(ex: E) -> Result<IntegrityReport>
where
    E: Executor<'e, Database = Postgres> + Copy,
{
    let unbalanced = sqlx::query_as::<_, UnbalancedTransaction>(
        r#"
        SELECT lt.id AS ledger_transaction_id,
               lt.kind AS kind,
               SUM(CASE WHEN e.direction = 'debit' THEN e.amount ELSE -e.amount END) AS net
          FROM ledger_transactions lt
          JOIN ledger_entries e ON e.ledger_transaction_id = lt.id
         GROUP BY lt.id, lt.kind
        HAVING SUM(CASE WHEN e.direction = 'debit' THEN e.amount ELSE -e.amount END) <> 0
         LIMIT 100
        "#,
    )
    .fetch_all(ex)
    .await
    .map_db()?;

    let drift = sqlx::query_as::<_, BalanceDrift>(
        r#"
        SELECT account_id, account_type, cached_balance, derived_balance
          FROM ledger_account_balances
         WHERE cached_balance <> derived_balance
         LIMIT 100
        "#,
    )
    .fetch_all(ex)
    .await
    .map_db()?;

    let negatives = sqlx::query_as::<_, IllegalNegative>(
        r#"
        SELECT id AS account_id, account_type, balance
          FROM ledger_accounts
         WHERE balance < 0 AND NOT allow_negative
         LIMIT 100
        "#,
    )
    .fetch_all(ex)
    .await
    .map_db()?;

    let solvency = sqlx::query_as::<_, SolvencyRow>(
        r#"
        SELECT a.symbol AS asset_symbol,
               a.chain  AS chain,
               COALESCE(SUM(la.balance) FILTER (
                   WHERE la.account_type IN ('user_available', 'user_reserved')
               ), 0)
               - COALESCE(SUM(la.balance) FILTER (
                   WHERE la.account_type = 'user_deficit'
               ), 0)                                              AS total_user_liability,
               COALESCE(SUM(la.balance) FILTER (
                   WHERE la.account_type = 'exchange_custody'
               ), 0)                                              AS custody_balance,
               COALESCE(SUM(la.balance) FILTER (
                   WHERE la.account_type = 'withdrawal_clearing'
               ), 0)                                              AS clearing_balance
          FROM assets a
          LEFT JOIN ledger_accounts la ON la.asset_id = a.id
         GROUP BY a.symbol, a.chain
         ORDER BY a.chain, a.symbol
        "#,
    )
    .fetch_all(ex)
    .await
    .map_db()?;

    let transactions_checked =
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM ledger_transactions")
            .fetch_one(ex)
            .await
            .map_db()?;
    let accounts_checked = sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM ledger_accounts")
        .fetch_one(ex)
        .await
        .map_db()?;

    let report = IntegrityReport {
        transactions_checked,
        accounts_checked,
        unbalanced_transactions: unbalanced,
        balance_drift: drift,
        illegal_negative_balances: negatives,
        solvency,
    };

    metrics::gauge!("chainrail_ledger_integrity_problems").set(report.problem_count() as f64);
    if !report.is_clean() {
        tracing::error!(
            problems = report.problem_count(),
            "LEDGER INTEGRITY VIOLATION -- this should be impossible; investigate immediately"
        );
    }
    Ok(report)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(liability: i128, custody: i128, clearing: i128) -> SolvencyRow {
        SolvencyRow {
            asset_symbol: "USDC".into(),
            chain: "base-sepolia".into(),
            total_user_liability: Amount::new(liability),
            custody_balance: Amount::new(custody),
            clearing_balance: Amount::new(clearing),
        }
    }

    #[test]
    fn solvency_surplus_accounts_for_in_flight_value() {
        // Fully backed: custody covers liabilities exactly.
        assert_eq!(row(100, 100, 0).surplus().unwrap(), Amount::ZERO);
        // Mid-withdrawal: value moved from custody into clearing, still backed.
        assert_eq!(row(100, 75, 25).surplus().unwrap(), Amount::ZERO);
        // Short.
        assert_eq!(row(100, 80, 0).surplus().unwrap(), Amount::new(-20));
        // Surplus.
        assert_eq!(row(100, 130, 0).surplus().unwrap(), Amount::new(30));
    }

    #[test]
    fn clean_report_requires_all_three_checks_empty() {
        let mut r = IntegrityReport {
            transactions_checked: 5,
            accounts_checked: 4,
            unbalanced_transactions: vec![],
            balance_drift: vec![],
            illegal_negative_balances: vec![],
            solvency: vec![row(100, 100, 0)],
        };
        assert!(r.is_clean());
        assert_eq!(r.problem_count(), 0);

        r.balance_drift.push(BalanceDrift {
            account_id: Uuid::nil(),
            account_type: "user_available".into(),
            cached_balance: Amount::new(10),
            derived_balance: Amount::new(9),
        });
        assert!(!r.is_clean());
        assert_eq!(r.problem_count(), 1);
    }

    #[test]
    fn insolvency_alone_does_not_mean_corruption() {
        // An under-funded hot wallet is an ops problem, not ledger corruption.
        let r = IntegrityReport {
            transactions_checked: 1,
            accounts_checked: 1,
            unbalanced_transactions: vec![],
            balance_drift: vec![],
            illegal_negative_balances: vec![],
            solvency: vec![row(100, 10, 0)],
        };
        assert!(r.is_clean());
        assert!(r.solvency[0].surplus().unwrap().is_negative());
    }
}
