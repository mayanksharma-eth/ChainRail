//! Chain identity and address handling.
//!
//! Chains are identified by a *string slug* (`base-sepolia`, `ethereum`, `bsc`)
//! rather than an enum, because supporting a new EVM network must be a config
//! change, not a code change. The `ChainKind` enum selects which adapter and
//! which address grammar applies.

use serde::{Deserialize, Serialize};
use std::fmt;
use std::str::FromStr;

use crate::error::{Error, Result};

/// Opaque chain slug, e.g. `base-sepolia`.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ChainId(String);

impl ChainId {
    pub fn new(s: impl Into<String>) -> Result<Self> {
        let s = s.into();
        let ok = !s.is_empty()
            && s.len() <= 64
            && s.chars()
                .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-');
        if !ok {
            return Err(Error::UnsupportedChain(s));
        }
        Ok(ChainId(s))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for ChainId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl FromStr for ChainId {
    type Err = Error;
    fn from_str(s: &str) -> Result<Self> {
        ChainId::new(s)
    }
}

/// Which family of adapter/address grammar a chain uses.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ChainKind {
    Evm,
    /// Reserved: the adapter interface is defined but no Solana implementation
    /// ships in v0.1. See `docs/architecture.md#non-evm-chains`.
    Solana,
}

impl fmt::Display for ChainKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ChainKind::Evm => f.write_str("evm"),
            ChainKind::Solana => f.write_str("solana"),
        }
    }
}

/// How a chain decides a block is final.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "mode", rename_all = "snake_case")]
pub enum FinalityPolicy {
    /// Probabilistic finality: N confirmations after the including block.
    Confirmations { blocks: u64 },
    /// Deterministic finality tag exposed by the node (e.g. `finalized`).
    Tag { tag: String },
}

impl FinalityPolicy {
    /// Confirmations required, if this is a confirmation-count policy.
    pub fn required_confirmations(&self) -> Option<u64> {
        match self {
            FinalityPolicy::Confirmations { blocks } => Some(*blocks),
            FinalityPolicy::Tag { .. } => None,
        }
    }

    /// Given the chain head and the block a transaction landed in, how many
    /// confirmations it currently has. The including block counts as 1.
    pub fn confirmations_for(head: u64, included_at: u64) -> u64 {
        head.saturating_sub(included_at).saturating_add(1)
    }

    /// Whether a transaction included at `included_at` is confirmed at `head`.
    pub fn is_confirmed(&self, head: u64, included_at: u64) -> bool {
        match self {
            FinalityPolicy::Confirmations { blocks } => {
                Self::confirmations_for(head, included_at) >= (*blocks).max(1)
            }
            // Tag-based finality is decided by the node, not by arithmetic; the
            // adapter answers this via a dedicated query instead.
            FinalityPolicy::Tag { .. } => false,
        }
    }
}

/// A chain address, normalized to its canonical on-chain representation.
///
/// For EVM this is the EIP-55 checksummed form. Storage always uses the
/// *lowercase* form (`storage_key`) so that uniqueness is case-insensitive;
/// APIs return the checksummed form.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Address(String);

impl Address {
    /// Parse and validate an address for the given chain kind.
    ///
    /// This is a trust boundary: withdrawal destinations arrive here from
    /// untrusted API input. A malformed or mis-checksummed address is rejected
    /// rather than normalized, because a silently "fixed" address can send funds
    /// to an unrecoverable destination.
    pub fn parse(kind: ChainKind, input: &str) -> Result<Address> {
        match kind {
            ChainKind::Evm => Self::parse_evm(input),
            ChainKind::Solana => Err(Error::InvalidAddress {
                chain: "solana".into(),
                reason: "solana address validation is not implemented in v0.1".into(),
            }),
        }
    }

    fn parse_evm(input: &str) -> Result<Address> {
        let s = input.trim();
        let body = s.strip_prefix("0x").ok_or_else(|| Error::InvalidAddress {
            chain: "evm".into(),
            reason: "must start with 0x".into(),
        })?;
        if body.len() != 40 {
            return Err(Error::InvalidAddress {
                chain: "evm".into(),
                reason: format!("expected 40 hex chars, got {}", body.len()),
            });
        }
        if !body.chars().all(|c| c.is_ascii_hexdigit()) {
            return Err(Error::InvalidAddress {
                chain: "evm".into(),
                reason: "non-hex characters".into(),
            });
        }
        let has_upper = body.chars().any(|c| c.is_ascii_uppercase());
        let has_lower = body.chars().any(|c| c.is_ascii_lowercase());
        let canonical = eip55_checksum(&body.to_ascii_lowercase());
        // Mixed case means the sender asserted an EIP-55 checksum; verify it.
        // All-lower or all-upper carries no checksum information, so accept it.
        if has_upper && has_lower && canonical[2..] != *body {
            return Err(Error::InvalidAddress {
                chain: "evm".into(),
                reason: "EIP-55 checksum mismatch".into(),
            });
        }
        if body.chars().all(|c| c == '0') {
            return Err(Error::InvalidAddress {
                chain: "evm".into(),
                reason: "zero address is not a valid destination".into(),
            });
        }
        Ok(Address(canonical))
    }

    /// Construct from a value already known to be canonical (e.g. read back from
    /// the database). Does not re-validate.
    pub fn from_storage(s: impl Into<String>) -> Address {
        Address(s.into())
    }

    /// Canonical (checksummed, for EVM) display form.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Case-folded form used as the database key and for all comparisons.
    pub fn storage_key(&self) -> String {
        self.0.to_ascii_lowercase()
    }
}

impl fmt::Display for Address {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// EIP-55 checksum encoding. `lower_body` must be 40 lowercase hex chars.
fn eip55_checksum(lower_body: &str) -> String {
    let hash = keccak256(lower_body.as_bytes());
    let mut out = String::with_capacity(42);
    out.push_str("0x");
    for (i, c) in lower_body.chars().enumerate() {
        let nibble = if i % 2 == 0 {
            hash[i / 2] >> 4
        } else {
            hash[i / 2] & 0x0f
        };
        if c.is_ascii_digit() || nibble < 8 {
            out.push(c);
        } else {
            out.push(c.to_ascii_uppercase());
        }
    }
    out
}

pub fn keccak256(bytes: &[u8]) -> [u8; 32] {
    use sha3::{Digest, Keccak256};
    let mut hasher = Keccak256::new();
    hasher.update(bytes);
    hasher.finalize().into()
}

/// A 32-byte chain hash (block hash or transaction hash), stored lowercase-hex
/// with an `0x` prefix so that database uniqueness is exact.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Hash32(String);

impl Hash32 {
    pub fn parse(input: &str) -> Result<Hash32> {
        let s = input.trim();
        let body = s.strip_prefix("0x").unwrap_or(s);
        if body.len() != 64 || !body.chars().all(|c| c.is_ascii_hexdigit()) {
            return Err(Error::Validation(format!(
                "invalid 32-byte hash: {}",
                truncate_for_log(s)
            )));
        }
        Ok(Hash32(format!("0x{}", body.to_ascii_lowercase())))
    }

    pub fn from_storage(s: impl Into<String>) -> Hash32 {
        Hash32(s.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for Hash32 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// Bound untrusted strings before they reach a log line.
pub fn truncate_for_log(s: &str) -> String {
    const MAX: usize = 80;
    if s.len() <= MAX {
        s.to_string()
    } else {
        format!("{}...[{} bytes]", &s[..MAX], s.len())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn chain_ids_reject_junk() {
        assert!(ChainId::new("base-sepolia").is_ok());
        assert!(ChainId::new("Base-Sepolia").is_err());
        assert!(ChainId::new("").is_err());
        assert!(ChainId::new("a".repeat(65)).is_err());
    }

    #[test]
    fn eip55_matches_reference_vectors() {
        // Vectors from EIP-55.
        for a in [
            "0x5aAeb6053F3E94C9b9A09f33669435E7Ef1BeAed",
            "0xfB6916095ca1df60bB79Ce92cE3Ea74c37c5d359",
            "0xdbF03B407c01E7cD3CBea99509d93f8DDDC8C6FB",
            "0xD1220A0cf47c7B9Be7A2E6BA89F429762e7b9aDb",
        ] {
            let parsed = Address::parse(ChainKind::Evm, a).unwrap();
            assert_eq!(parsed.as_str(), a);
        }
    }

    #[test]
    fn bad_checksum_is_rejected_not_normalized() {
        // Same address with two characters' case flipped.
        let bad = "0x5aAeb6053f3E94C9b9A09f33669435E7Ef1BeAed";
        assert!(matches!(
            Address::parse(ChainKind::Evm, bad),
            Err(Error::InvalidAddress { .. })
        ));
    }

    #[test]
    fn case_insensitive_forms_accepted_and_normalized() {
        let lower = "0x5aaeb6053f3e94c9b9a09f33669435e7ef1beaed";
        let parsed = Address::parse(ChainKind::Evm, lower).unwrap();
        assert_eq!(
            parsed.as_str(),
            "0x5aAeb6053F3E94C9b9A09f33669435E7Ef1BeAed"
        );
        assert_eq!(parsed.storage_key(), lower);
    }

    #[test]
    fn structural_address_failures() {
        for bad in [
            "5aAeb6053F3E94C9b9A09f33669435E7Ef1BeAed",   // no 0x
            "0x5aAeb6053F3E94C9b9A09f33669435E7Ef1BeAe",  // short
            "0xzzzzb6053F3E94C9b9A09f33669435E7Ef1BeAed", // non-hex
            "0x0000000000000000000000000000000000000000", // zero address
        ] {
            assert!(
                Address::parse(ChainKind::Evm, bad).is_err(),
                "accepted {bad}"
            );
        }
    }

    #[test]
    fn hashes_normalize_to_lowercase() {
        let h = Hash32::parse(&format!("0x{}", "AB".repeat(32))).unwrap();
        assert_eq!(h.as_str(), format!("0x{}", "ab".repeat(32)));
        assert!(Hash32::parse("0xdeadbeef").is_err());
    }

    #[test]
    fn confirmation_math_counts_including_block() {
        assert_eq!(FinalityPolicy::confirmations_for(100, 100), 1);
        assert_eq!(FinalityPolicy::confirmations_for(110, 100), 11);
        // head behind the tx (stale read) must not underflow
        assert_eq!(FinalityPolicy::confirmations_for(99, 100), 1);

        let p = FinalityPolicy::Confirmations { blocks: 10 };
        assert!(!p.is_confirmed(108, 100));
        assert!(p.is_confirmed(109, 100));
        assert!(p.is_confirmed(200, 100));
    }

    #[test]
    fn tag_finality_never_confirms_by_arithmetic() {
        let p = FinalityPolicy::Tag {
            tag: "finalized".into(),
        };
        assert!(!p.is_confirmed(u64::MAX, 1));
        assert_eq!(p.required_confirmations(), None);
    }

    #[test]
    fn log_truncation_bounds_untrusted_input() {
        let long = "x".repeat(500);
        assert!(truncate_for_log(&long).len() < 120);
    }
}
