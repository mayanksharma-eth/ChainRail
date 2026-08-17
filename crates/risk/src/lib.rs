//! Risk and policy engine.
//!
//! Pure evaluation: the caller gathers facts (balances, daily totals), the
//! engine decides. No I/O means every rule and every boundary is unit-testable,
//! and a policy decision can be replayed exactly from its inputs — which is what
//! makes an "why was my withdrawal blocked?" investigation tractable.
//!
//! Rules are evaluated in a fixed order and the **first** denial wins, so denial
//! reasons are deterministic rather than dependent on map iteration order.
//!
//! This engine is a *business* control, not a security boundary. A compromised
//! ChainRail process can bypass it entirely; only enforcing policy at the signer
//! (KMS/HSM/MPC) removes that possibility. See
//! `docs/threat-model.md#compromised-signer`.

use chainrail_common::config::RiskConfig;
use chainrail_common::{Amount, Error};
use serde::Serialize;
use std::collections::HashSet;

/// Everything the engine needs to decide. Assembled by the caller.
#[derive(Debug, Clone)]
pub struct RiskInput<'a> {
    pub chain: &'a str,
    pub asset_symbol: &'a str,
    pub amount: Amount,
    /// Already-normalized (lowercase) destination.
    pub destination_lower: &'a str,
    /// The chain's own hot wallet, which must never be a withdrawal destination.
    pub hot_wallet_lower: Option<&'a str>,
    pub available_balance: Amount,
    /// Sum of non-failed withdrawals for this user/asset in the rolling window.
    pub daily_total: Amount,
    pub daily_count: u32,
    /// Deposit addresses belonging to *other* users on this chain, if the caller
    /// chose to check. Sending to another user's deposit address would move
    /// funds off-ledger in a way ChainRail would then re-credit.
    pub is_foreign_deposit_address: bool,
    pub withdrawals_enabled_for_asset: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub enum Decision {
    /// May proceed to automatic approval.
    Allow,
    /// Permitted, but must be approved by an operator first. The withdrawal
    /// stays in `validated` rather than advancing to `approved`.
    ManualApproval {
        code: String,
        message: String,
    },
    Deny {
        code: String,
        message: String,
    },
}

impl Decision {
    pub fn is_allowed(&self) -> bool {
        matches!(self, Decision::Allow)
    }

    pub fn is_denied(&self) -> bool {
        matches!(self, Decision::Deny { .. })
    }

    pub fn requires_manual_approval(&self) -> bool {
        matches!(self, Decision::ManualApproval { .. })
    }

    pub fn code(&self) -> &str {
        match self {
            Decision::Allow => "allow",
            Decision::ManualApproval { code, .. } | Decision::Deny { code, .. } => code,
        }
    }

    /// Convert a denial into the API-facing error. `Allow` and
    /// `ManualApproval` are not errors, so they map to `None`.
    pub fn as_error(&self) -> Option<Error> {
        match self {
            Decision::Deny { code, message } => Some(Error::PolicyDenied {
                code: code.clone(),
                message: message.clone(),
            }),
            _ => None,
        }
    }
}

fn deny(code: &str, message: impl Into<String>) -> Decision {
    Decision::Deny {
        code: code.to_string(),
        message: message.into(),
    }
}

pub struct RiskEngine {
    cfg: RiskConfig,
    allowed_chains: HashSet<String>,
    denylist: HashSet<String>,
}

impl RiskEngine {
    pub fn new(cfg: RiskConfig) -> Self {
        let allowed_chains = cfg.allowed_chains.iter().cloned().collect();
        let denylist = cfg
            .destination_denylist
            .iter()
            .map(|d| d.trim().to_ascii_lowercase())
            .collect();
        RiskEngine {
            cfg,
            allowed_chains,
            denylist,
        }
    }

    pub fn maintenance_mode(&self) -> bool {
        self.cfg.maintenance_mode
    }

    /// Per-asset limit key. Falls back from `chain:SYMBOL` to `SYMBOL`, so an
    /// operator can set a global USDC limit or override it for one chain.
    fn limit<'a>(
        map: &'a std::collections::HashMap<String, Amount>,
        chain: &str,
        symbol: &str,
    ) -> Option<&'a Amount> {
        map.get(&format!("{chain}:{symbol}"))
            .or_else(|| map.get(symbol))
    }

    pub fn evaluate(&self, input: &RiskInput<'_>) -> Decision {
        // 1. Kill switch. Checked first so it cannot be bypassed by any other rule.
        if self.cfg.maintenance_mode {
            return deny(
                "maintenance_mode",
                "withdrawals are temporarily disabled for maintenance",
            );
        }

        // 2. Structural validity.
        if !input.amount.is_positive() {
            return deny("invalid_amount", "withdrawal amount must be positive");
        }
        if !input.withdrawals_enabled_for_asset {
            return deny(
                "asset_withdrawals_disabled",
                format!("withdrawals are disabled for {}", input.asset_symbol),
            );
        }
        if !self.allowed_chains.is_empty() && !self.allowed_chains.contains(input.chain) {
            return deny(
                "chain_not_allowed",
                format!("withdrawals to {} are not permitted", input.chain),
            );
        }

        // 3. Destination checks, before any balance is touched.
        if self.denylist.contains(input.destination_lower) {
            return deny(
                "destination_denylisted",
                "the destination address is not permitted",
            );
        }
        if let Some(hot) = input.hot_wallet_lower {
            if hot == input.destination_lower {
                // Would send funds to ourselves and then re-credit them as a
                // deposit, inflating the ledger.
                return deny(
                    "destination_is_hot_wallet",
                    "the destination is ChainRail's own hot wallet",
                );
            }
        }
        if input.is_foreign_deposit_address {
            return deny(
                "destination_is_internal",
                "the destination is another ChainRail deposit address; use an internal transfer",
            );
        }

        // 4. Per-request bounds.
        if let Some(min) = Self::limit(&self.cfg.min_per_request, input.chain, input.asset_symbol) {
            if input.amount < *min {
                return deny(
                    "below_minimum",
                    format!("minimum withdrawal is {min} (requested {})", input.amount),
                );
            }
        }
        if let Some(max) = Self::limit(&self.cfg.max_per_request, input.chain, input.asset_symbol) {
            if input.amount > *max {
                return deny(
                    "above_maximum",
                    format!(
                        "maximum per withdrawal is {max} (requested {})",
                        input.amount
                    ),
                );
            }
        }

        // 5. Velocity. Checked before balance so a user near their daily cap gets
        //    the informative error rather than a balance error.
        if let Some(cap) = self.cfg.max_withdrawals_per_user_per_day {
            if input.daily_count >= cap {
                return deny(
                    "daily_count_exceeded",
                    format!("at most {cap} withdrawals per day"),
                );
            }
        }
        if let Some(cap) = Self::limit(
            &self.cfg.max_per_user_per_day,
            input.chain,
            input.asset_symbol,
        ) {
            match input.daily_total.checked_add(input.amount) {
                Ok(projected) if projected > *cap => {
                    return deny(
                        "daily_limit_exceeded",
                        format!(
                            "daily limit {cap} would be exceeded ({} already withdrawn today)",
                            input.daily_total
                        ),
                    )
                }
                Err(_) => return deny("amount_overflow", "amount arithmetic overflowed"),
                _ => {}
            }
        }

        // 6. Solvency. The ledger enforces this again at reservation time; this
        //    check exists to produce a clear error before doing any work.
        if input.available_balance < input.amount {
            return deny(
                "insufficient_balance",
                format!(
                    "available balance {} is less than {}",
                    input.available_balance, input.amount
                ),
            );
        }

        // 7. Large withdrawals are permitted but held for a human.
        if let Some(threshold) = Self::limit(
            &self.cfg.manual_approval_threshold,
            input.chain,
            input.asset_symbol,
        ) {
            if input.amount > *threshold {
                return Decision::ManualApproval {
                    code: "manual_approval_required".into(),
                    message: format!("withdrawals above {threshold} require operator approval"),
                };
            }
        }

        Decision::Allow
    }

    /// Evaluate and record the outcome as a metric.
    pub fn evaluate_and_record(&self, input: &RiskInput<'_>) -> Decision {
        let decision = self.evaluate(input);
        metrics::counter!(
            "chainrail_risk_decisions_total",
            "chain" => input.chain.to_string(),
            "asset" => input.asset_symbol.to_string(),
            "decision" => match &decision {
                Decision::Allow => "allow".to_string(),
                Decision::ManualApproval { .. } => "manual_approval".to_string(),
                Decision::Deny { code, .. } => format!("deny:{code}"),
            },
        )
        .increment(1);
        if let Decision::Deny { code, message } = &decision {
            tracing::info!(
                chain = input.chain,
                asset = input.asset_symbol,
                amount = %input.amount,
                code = %code,
                "withdrawal denied by policy: {message}"
            );
        }
        decision
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn cfg() -> RiskConfig {
        RiskConfig {
            maintenance_mode: false,
            manual_approval_threshold: HashMap::new(),
            max_per_request: HashMap::new(),
            min_per_request: HashMap::new(),
            max_per_user_per_day: HashMap::new(),
            max_withdrawals_per_user_per_day: None,
            allowed_chains: vec![],
            destination_denylist: vec![],
        }
    }

    fn input<'a>(amount: i128, balance: i128) -> RiskInput<'a> {
        RiskInput {
            chain: "base-sepolia",
            asset_symbol: "USDC",
            amount: Amount::new(amount),
            destination_lower: "0x5aaeb6053f3e94c9b9a09f33669435e7ef1beaed",
            hot_wallet_lower: Some("0xf39fd6e51aad88f6f4ce6ab8827279cfffb92266"),
            available_balance: Amount::new(balance),
            daily_total: Amount::ZERO,
            daily_count: 0,
            is_foreign_deposit_address: false,
            withdrawals_enabled_for_asset: true,
        }
    }

    #[test]
    fn a_clean_request_is_allowed() {
        let e = RiskEngine::new(cfg());
        assert_eq!(e.evaluate(&input(100, 1_000)), Decision::Allow);
        assert!(e.evaluate(&input(100, 1_000)).is_allowed());
    }

    #[test]
    fn maintenance_mode_denies_everything_first() {
        let mut c = cfg();
        c.maintenance_mode = true;
        let e = RiskEngine::new(c);
        // Even an otherwise-perfect request is denied, and the reason is the
        // kill switch rather than some later rule.
        let d = e.evaluate(&input(100, 1_000_000));
        assert_eq!(d.code(), "maintenance_mode");
        assert!(d.is_denied());
    }

    #[test]
    fn non_positive_amounts_are_denied() {
        let e = RiskEngine::new(cfg());
        assert_eq!(e.evaluate(&input(0, 1_000)).code(), "invalid_amount");
        assert_eq!(e.evaluate(&input(-5, 1_000)).code(), "invalid_amount");
    }

    #[test]
    fn disabled_assets_are_denied() {
        let e = RiskEngine::new(cfg());
        let mut i = input(100, 1_000);
        i.withdrawals_enabled_for_asset = false;
        assert_eq!(e.evaluate(&i).code(), "asset_withdrawals_disabled");
    }

    #[test]
    fn chain_allowlist_is_enforced_only_when_non_empty() {
        let mut c = cfg();
        c.allowed_chains = vec!["ethereum".into()];
        let e = RiskEngine::new(c);
        assert_eq!(e.evaluate(&input(100, 1_000)).code(), "chain_not_allowed");

        // Empty allowlist means "every configured chain", not "none".
        let e = RiskEngine::new(cfg());
        assert!(e.evaluate(&input(100, 1_000)).is_allowed());
    }

    #[test]
    fn denylisted_destinations_are_blocked_case_insensitively() {
        let mut c = cfg();
        c.destination_denylist = vec!["0x5AAEB6053F3E94C9B9A09F33669435E7EF1BEAED".into()];
        let e = RiskEngine::new(c);
        assert_eq!(
            e.evaluate(&input(100, 1_000)).code(),
            "destination_denylisted"
        );
    }

    #[test]
    fn sending_to_our_own_hot_wallet_is_blocked() {
        let e = RiskEngine::new(cfg());
        let mut i = input(100, 1_000);
        i.destination_lower = "0xf39fd6e51aad88f6f4ce6ab8827279cfffb92266";
        // Would leave custody and immediately be re-observed as a deposit,
        // double-counting the funds.
        assert_eq!(e.evaluate(&i).code(), "destination_is_hot_wallet");
    }

    #[test]
    fn sending_to_another_users_deposit_address_is_blocked() {
        let e = RiskEngine::new(cfg());
        let mut i = input(100, 1_000);
        i.is_foreign_deposit_address = true;
        assert_eq!(e.evaluate(&i).code(), "destination_is_internal");
    }

    #[test]
    fn per_request_bounds_are_enforced_at_the_boundary() {
        let mut c = cfg();
        c.min_per_request.insert("USDC".into(), Amount::new(100));
        c.max_per_request.insert("USDC".into(), Amount::new(1_000));
        let e = RiskEngine::new(c);

        assert_eq!(e.evaluate(&input(99, 10_000)).code(), "below_minimum");
        assert!(
            e.evaluate(&input(100, 10_000)).is_allowed(),
            "min is inclusive"
        );
        assert!(
            e.evaluate(&input(1_000, 10_000)).is_allowed(),
            "max is inclusive"
        );
        assert_eq!(e.evaluate(&input(1_001, 10_000)).code(), "above_maximum");
    }

    #[test]
    fn chain_scoped_limits_override_global_ones() {
        let mut c = cfg();
        c.max_per_request.insert("USDC".into(), Amount::new(1_000));
        c.max_per_request
            .insert("base-sepolia:USDC".into(), Amount::new(50));
        let e = RiskEngine::new(c);
        // The chain-specific limit is the tighter one and must win.
        assert_eq!(e.evaluate(&input(100, 10_000)).code(), "above_maximum");
        assert!(e.evaluate(&input(50, 10_000)).is_allowed());
    }

    #[test]
    fn daily_value_limit_considers_what_was_already_withdrawn() {
        let mut c = cfg();
        c.max_per_user_per_day
            .insert("USDC".into(), Amount::new(1_000));
        let e = RiskEngine::new(c);

        let mut i = input(300, 10_000);
        i.daily_total = Amount::new(700);
        assert!(
            e.evaluate(&i).is_allowed(),
            "700 + 300 == 1000 is at the cap"
        );

        i.daily_total = Amount::new(701);
        assert_eq!(e.evaluate(&i).code(), "daily_limit_exceeded");
    }

    #[test]
    fn daily_count_limit_is_enforced() {
        let mut c = cfg();
        c.max_withdrawals_per_user_per_day = Some(3);
        let e = RiskEngine::new(c);

        let mut i = input(10, 10_000);
        i.daily_count = 2;
        assert!(e.evaluate(&i).is_allowed());
        i.daily_count = 3;
        assert_eq!(e.evaluate(&i).code(), "daily_count_exceeded");
    }

    #[test]
    fn insufficient_balance_is_denied_with_both_figures() {
        let e = RiskEngine::new(cfg());
        let d = e.evaluate(&input(1_000, 999));
        assert_eq!(d.code(), "insufficient_balance");
        match d {
            Decision::Deny { message, .. } => {
                assert!(message.contains("999"));
                assert!(message.contains("1000"));
            }
            other => panic!("expected denial, got {other:?}"),
        }
    }

    #[test]
    fn exactly_the_available_balance_is_allowed() {
        let e = RiskEngine::new(cfg());
        assert!(e.evaluate(&input(1_000, 1_000)).is_allowed());
    }

    #[test]
    fn large_withdrawals_require_manual_approval_rather_than_being_denied() {
        let mut c = cfg();
        c.manual_approval_threshold
            .insert("USDC".into(), Amount::new(10_000));
        let e = RiskEngine::new(c);

        assert!(
            e.evaluate(&input(10_000, 1_000_000)).is_allowed(),
            "at threshold"
        );
        let d = e.evaluate(&input(10_001, 1_000_000));
        assert!(d.requires_manual_approval());
        assert!(!d.is_denied(), "a large withdrawal is held, not rejected");
        assert!(d.as_error().is_none());
    }

    #[test]
    fn rule_order_is_deterministic() {
        // A request that violates several rules at once must always report the
        // same (earliest) reason, so support answers are reproducible.
        let mut c = cfg();
        c.min_per_request.insert("USDC".into(), Amount::new(1_000));
        c.destination_denylist = vec!["0x5aaeb6053f3e94c9b9a09f33669435e7ef1beaed".into()];
        let e = RiskEngine::new(c);
        // Denylist (rule 3) precedes the minimum (rule 4).
        for _ in 0..10 {
            assert_eq!(e.evaluate(&input(1, 0)).code(), "destination_denylisted");
        }
    }

    #[test]
    fn denials_convert_to_policy_errors() {
        let e = RiskEngine::new(cfg());
        let err = e.evaluate(&input(1_000, 1)).as_error().unwrap();
        assert!(matches!(err, Error::PolicyDenied { .. }));
        assert_eq!(err.code(), "policy_denied");
        assert!(err.is_client_error());
        assert!(!err.is_retryable());
    }

    #[test]
    fn daily_total_overflow_is_denied_not_wrapped() {
        let mut c = cfg();
        c.max_per_user_per_day
            .insert("USDC".into(), Amount::new(i128::MAX));
        let e = RiskEngine::new(c);
        let mut i = input(i128::MAX, i128::MAX);
        i.daily_total = Amount::new(i128::MAX);
        assert_eq!(e.evaluate(&i).code(), "amount_overflow");
    }
}
