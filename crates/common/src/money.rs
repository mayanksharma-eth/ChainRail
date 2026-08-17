//! Monetary primitives.
//!
//! INVARIANT: money is *never* represented as a floating point number anywhere in
//! ChainRail. All amounts are integers in an asset's smallest indivisible unit
//! ("raw" units, e.g. wei for ETH, 1e-6 USDC for USDC).
//!
//! We use `i128` as the in-memory carrier and `NUMERIC(78, 0)` as the storage type.
//! `NUMERIC(78,0)` can represent the full EVM `uint256` range; `i128` cannot
//! (`i128::MAX ~= 1.7e38` vs `uint256::MAX ~= 1.16e77`). This is a deliberate
//! trade-off: signed arithmetic and cheap overflow-checked math matter more than
//! representing balances that cannot physically exist. Every value crossing the
//! chain->ChainRail trust boundary is validated to fit `i128` (see
//! `Amount::from_u256_decimal_str`); values that do not fit are rejected as
//! anomalous rather than silently truncated.

use bigdecimal::{BigDecimal, ToPrimitive, Zero};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::fmt;
use std::str::FromStr;

use crate::error::{Error, Result};

/// A signed integer amount in an asset's smallest indivisible unit.
///
/// Serialized as a JSON *string* to avoid the IEEE-754 precision loss that
/// JSON numbers would introduce for values above 2^53.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct Amount(i128);

impl Amount {
    pub const ZERO: Amount = Amount(0);

    #[inline]
    pub const fn new(raw: i128) -> Self {
        Amount(raw)
    }

    #[inline]
    pub const fn raw(self) -> i128 {
        self.0
    }

    #[inline]
    pub const fn is_zero(self) -> bool {
        self.0 == 0
    }

    #[inline]
    pub const fn is_positive(self) -> bool {
        self.0 > 0
    }

    #[inline]
    pub const fn is_negative(self) -> bool {
        self.0 < 0
    }

    #[inline]
    pub const fn abs(self) -> Amount {
        Amount(self.0.abs())
    }

    #[inline]
    pub const fn negate(self) -> Amount {
        // `i128::MIN` has no positive counterpart; we never construct it because
        // every constructor rejects magnitudes at that boundary.
        Amount(-self.0)
    }

    pub fn checked_add(self, rhs: Amount) -> Result<Amount> {
        self.0
            .checked_add(rhs.0)
            .map(Amount)
            .ok_or(Error::AmountOverflow)
    }

    pub fn checked_sub(self, rhs: Amount) -> Result<Amount> {
        self.0
            .checked_sub(rhs.0)
            .map(Amount)
            .ok_or(Error::AmountOverflow)
    }

    pub fn checked_mul_u32(self, rhs: u32) -> Result<Amount> {
        self.0
            .checked_mul(i128::from(rhs))
            .map(Amount)
            .ok_or(Error::AmountOverflow)
    }

    /// Parse a decimal integer string produced by a chain (which may be up to
    /// 78 digits wide). Rejects values that do not fit `i128` rather than
    /// truncating: see the module docs for why.
    pub fn from_u256_decimal_str(s: &str) -> Result<Amount> {
        let s = s.trim();
        if s.is_empty() {
            return Err(Error::InvalidAmount("empty".into()));
        }
        if s.starts_with('-') {
            return Err(Error::InvalidAmount(
                "chain amounts must be unsigned".into(),
            ));
        }
        i128::from_str(s)
            .map(Amount)
            .map_err(|_| Error::AmountExceedsRepresentableRange(s.to_string()))
    }

    /// Parse a `0x`-prefixed (or bare) hex quantity from a JSON-RPC response.
    pub fn from_hex_quantity(s: &str) -> Result<Amount> {
        let stripped = s.trim().trim_start_matches("0x").trim_start_matches("0X");
        if stripped.is_empty() {
            return Ok(Amount::ZERO);
        }
        // 32 hex chars == 128 bits; anything wider than 31 chars may overflow i128,
        // so parse wide values through BigDecimal to get a clean rejection.
        if stripped.len() > 31 {
            let trimmed = stripped.trim_start_matches('0');
            if trimmed.len() > 31 {
                return Err(Error::AmountExceedsRepresentableRange(s.to_string()));
            }
        }
        i128::from_str_radix(stripped, 16)
            .map(Amount)
            .map_err(|_| Error::AmountExceedsRepresentableRange(s.to_string()))
    }

    pub fn to_bigdecimal(self) -> BigDecimal {
        BigDecimal::from(self.0)
    }

    pub fn from_bigdecimal(v: &BigDecimal) -> Result<Amount> {
        if !v.is_integer() {
            return Err(Error::InvalidAmount(format!(
                "non-integer amount from storage: {v}"
            )));
        }
        v.to_i128()
            .map(Amount)
            .ok_or_else(|| Error::AmountExceedsRepresentableRange(v.to_string()))
    }

    /// Human-readable rendering for logs and API responses. Performs exact
    /// decimal-string formatting; no floating point is involved.
    pub fn format_units(self, decimals: u8) -> String {
        let negative = self.0 < 0;
        let magnitude = self.0.unsigned_abs();
        let s = magnitude.to_string();
        let d = usize::from(decimals);
        let body = if d == 0 {
            s
        } else if s.len() > d {
            let (int, frac) = s.split_at(s.len() - d);
            format!("{int}.{frac}")
        } else {
            format!("0.{}{}", "0".repeat(d - s.len()), s)
        };
        let body = if d > 0 {
            let trimmed = body.trim_end_matches('0').trim_end_matches('.');
            if trimmed.is_empty() {
                "0".to_string()
            } else {
                trimmed.to_string()
            }
        } else {
            body
        };
        if negative {
            format!("-{body}")
        } else {
            body
        }
    }
}

impl fmt::Display for Amount {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl FromStr for Amount {
    type Err = Error;
    fn from_str(s: &str) -> Result<Amount> {
        i128::from_str(s.trim())
            .map(Amount)
            .map_err(|_| Error::InvalidAmount(s.to_string()))
    }
}

impl From<i64> for Amount {
    fn from(v: i64) -> Self {
        Amount(i128::from(v))
    }
}

impl Serialize for Amount {
    fn serialize<S: Serializer>(&self, s: S) -> std::result::Result<S::Ok, S::Error> {
        s.serialize_str(&self.0.to_string())
    }
}

impl<'de> Deserialize<'de> for Amount {
    fn deserialize<D: Deserializer<'de>>(d: D) -> std::result::Result<Amount, D::Error> {
        // Accept both `"100"` and `100` so hand-written API payloads are forgiving,
        // but only integers -- a JSON float is always rejected.
        let v = serde_json::Value::deserialize(d)?;
        match v {
            serde_json::Value::String(s) => Amount::from_str(&s).map_err(serde::de::Error::custom),
            serde_json::Value::Number(n) if n.is_i64() || n.is_u64() => {
                let raw = n
                    .as_i64()
                    .map(i128::from)
                    .or_else(|| n.as_u64().map(i128::from))
                    .ok_or_else(|| serde::de::Error::custom("amount out of range"))?;
                Ok(Amount(raw))
            }
            other => Err(serde::de::Error::custom(format!(
                "amount must be an integer string, got {other}"
            ))),
        }
    }
}

/// Sum that fails loudly instead of wrapping.
pub fn checked_sum<I: IntoIterator<Item = Amount>>(iter: I) -> Result<Amount> {
    iter.into_iter()
        .try_fold(Amount::ZERO, |acc, x| acc.checked_add(x))
}

/// Direction of a single ledger entry. See `docs/ledger.md` for the sign convention.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Direction {
    Debit,
    Credit,
}

impl Direction {
    /// Signed contribution of an entry to the *balance expression*
    /// `sum(debits) - sum(credits)`.
    #[inline]
    pub const fn sign(self) -> i128 {
        match self {
            Direction::Debit => 1,
            Direction::Credit => -1,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Direction::Debit => "debit",
            Direction::Credit => "credit",
        }
    }
}

impl FromStr for Direction {
    type Err = Error;
    fn from_str(s: &str) -> Result<Direction> {
        match s {
            "debit" => Ok(Direction::Debit),
            "credit" => Ok(Direction::Credit),
            other => Err(Error::InvalidAmount(format!("unknown direction {other}"))),
        }
    }
}

impl fmt::Display for Direction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// True when a set of entries balances, i.e. `sum(debits) == sum(credits)`.
pub fn entries_balance<'a, I>(entries: I) -> Result<bool>
where
    I: IntoIterator<Item = (&'a Direction, &'a Amount)>,
{
    let mut net: i128 = 0;
    for (dir, amount) in entries {
        if !amount.is_positive() {
            return Err(Error::InvalidAmount(
                "ledger entry amounts must be strictly positive".into(),
            ));
        }
        net = net
            .checked_add(
                dir.sign()
                    .checked_mul(amount.raw())
                    .ok_or(Error::AmountOverflow)?,
            )
            .ok_or(Error::AmountOverflow)?;
    }
    Ok(net.is_zero())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_and_renders_raw_units() {
        let a = Amount::from_u256_decimal_str("1000000").unwrap();
        assert_eq!(a.raw(), 1_000_000);
        assert_eq!(a.to_string(), "1000000");
        assert_eq!(a.format_units(6), "1");
        assert_eq!(Amount::new(1_234_567).format_units(6), "1.234567");
        assert_eq!(Amount::new(1).format_units(18), "0.000000000000000001");
        assert_eq!(Amount::new(0).format_units(6), "0");
        assert_eq!(Amount::new(-1_500_000).format_units(6), "-1.5");
        assert_eq!(Amount::new(42).format_units(0), "42");
    }

    #[test]
    fn rejects_amounts_wider_than_i128() {
        // uint256 max -- must be rejected, never truncated.
        let u256_max =
            "115792089237316195423570985008687907853269984665640564039457584007913129639935";
        assert!(matches!(
            Amount::from_u256_decimal_str(u256_max),
            Err(Error::AmountExceedsRepresentableRange(_))
        ));
        assert!(Amount::from_hex_quantity(&format!("0x{}", "f".repeat(64))).is_err());
    }

    #[test]
    fn rejects_negative_and_malformed_chain_amounts() {
        assert!(Amount::from_u256_decimal_str("-5").is_err());
        assert!(Amount::from_u256_decimal_str("").is_err());
        assert!(Amount::from_u256_decimal_str("12a").is_err());
    }

    #[test]
    fn hex_quantities_round_trip() {
        assert_eq!(Amount::from_hex_quantity("0x0").unwrap(), Amount::ZERO);
        assert_eq!(Amount::from_hex_quantity("0x").unwrap(), Amount::ZERO);
        assert_eq!(
            Amount::from_hex_quantity("0xf4240").unwrap().raw(),
            1_000_000
        );
        // leading zeros beyond 31 chars are fine once trimmed
        assert_eq!(
            Amount::from_hex_quantity(&format!("0x{}f4240", "0".repeat(40)))
                .unwrap()
                .raw(),
            1_000_000
        );
    }

    #[test]
    fn arithmetic_is_overflow_checked() {
        let max = Amount::new(i128::MAX);
        assert!(matches!(
            max.checked_add(Amount::new(1)),
            Err(Error::AmountOverflow)
        ));
        assert_eq!(
            Amount::new(10).checked_sub(Amount::new(4)).unwrap().raw(),
            6
        );
    }

    #[test]
    fn serde_uses_strings_not_floats() {
        let a = Amount::new(9_007_199_254_740_993); // 2^53 + 1: unrepresentable as f64
        let json = serde_json::to_string(&a).unwrap();
        assert_eq!(json, "\"9007199254740993\"");
        let back: Amount = serde_json::from_str(&json).unwrap();
        assert_eq!(back, a);
        // integers accepted, floats rejected
        assert!(serde_json::from_str::<Amount>("123").is_ok());
        assert!(serde_json::from_str::<Amount>("1.5").is_err());
    }

    #[test]
    fn balanced_entries_detected() {
        let d = Direction::Debit;
        let c = Direction::Credit;
        let hundred = Amount::new(100);
        let sixty = Amount::new(60);
        let forty = Amount::new(40);
        assert!(entries_balance([(&d, &hundred), (&c, &hundred)]).unwrap());
        assert!(entries_balance([(&d, &hundred), (&c, &sixty), (&c, &forty)]).unwrap());
        assert!(!entries_balance([(&d, &hundred), (&c, &sixty)]).unwrap());
        // zero-amount entries are a bug, not a balanced no-op
        assert!(entries_balance([(&d, &Amount::ZERO), (&c, &Amount::ZERO)]).is_err());
    }

    #[test]
    fn bigdecimal_round_trip() {
        let a = Amount::new(-123_456_789_012_345_678);
        assert_eq!(Amount::from_bigdecimal(&a.to_bigdecimal()).unwrap(), a);
    }
}
