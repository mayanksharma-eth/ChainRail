//! Transaction signing abstraction.
//!
//! # What this is, and what it is emphatically not
//!
//! ChainRail ships **development signers only**:
//!
//!   * [`LocalDevelopmentSigner`] holds a secp256k1 key in process memory. It
//!     refuses to start outside `local`/`test`/`ci` (enforced in
//!     `AppConfig::validate`). Intended for testnets with worthless keys.
//!   * [`MockSigner`] produces deterministic, invalid signatures. For tests.
//!
//! Real custody is **not implemented** and is not claimed to be. A production
//! deployment would replace this trait's implementation with one of:
//!
//!   * AWS KMS / GCP Cloud KMS -- keys never leave the HSM boundary; ChainRail
//!     would send a digest and receive a signature.
//!   * HashiCorp Vault Transit -- same shape, self-hosted.
//!   * A dedicated HSM (Thales, Utimaco) behind a signing service.
//!   * An MPC/threshold custody provider (Fireblocks, Copper, Qredo), where no
//!     single machine ever holds a complete key.
//!
//! Each of those additionally requires policy enforcement *at the signer*
//! (per-address allowlists, velocity limits, quorum approval for large
//! transfers), because a compromised ChainRail process must not be able to sign
//! arbitrary transactions. ChainRail's risk engine is a first line of defence,
//! not a substitute. See `docs/threat-model.md#compromised-signer`.
//!
//! The trait boundary is deliberately narrow -- build an unsigned transaction,
//! hand it over, get bytes and a hash back -- precisely so that swap is a
//! contained change.

mod local;
mod mock;

pub use local::LocalDevelopmentSigner;
pub use mock::MockSigner;

use async_trait::async_trait;
use chainrail_common::{Address, Amount, ChainKind, Hash32, Result};
use std::sync::Arc;

/// An EVM EIP-1559 transfer, fully specified and ready to sign.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EvmTransactionRequest {
    /// EIP-155 chain id. Required: without it a signature is replayable on
    /// every other EVM chain.
    pub chain_id: u64,
    pub nonce: u64,
    pub to: Address,
    /// Native value. Zero for ERC-20 transfers, where value moves via `data`.
    pub value: Amount,
    /// ABI-encoded calldata; empty for a native transfer.
    pub data: Vec<u8>,
    pub gas_limit: u64,
    pub max_fee_per_gas: Amount,
    pub max_priority_fee_per_gas: Amount,
}

impl EvmTransactionRequest {
    /// Worst-case fee this transaction can consume.
    pub fn max_fee_cost(&self) -> Result<Amount> {
        self.max_fee_per_gas
            .checked_mul_u32(u32::try_from(self.gas_limit).unwrap_or(u32::MAX))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UnsignedTransaction {
    Evm(EvmTransactionRequest),
    // A Solana variant would be added here alongside a Solana signer; the
    // interface exists, the implementation deliberately does not.
}

impl UnsignedTransaction {
    pub fn chain_kind(&self) -> ChainKind {
        match self {
            UnsignedTransaction::Evm(_) => ChainKind::Evm,
        }
    }
}

#[derive(Debug, Clone)]
pub struct SignedTransaction {
    /// Wire-format bytes for `eth_sendRawTransaction`.
    pub raw: Vec<u8>,
    /// The transaction hash, known *before* broadcast. This is what makes the
    /// "broadcast succeeded but the database write failed" case recoverable: we
    /// persist the hash first, then send.
    pub tx_hash: Hash32,
    pub from: Address,
}

impl SignedTransaction {
    pub fn raw_hex(&self) -> String {
        format!("0x{}", hex::encode(&self.raw))
    }
}

#[async_trait]
pub trait Signer: Send + Sync {
    /// Sign a fully-specified transaction. Implementations must not mutate the
    /// request: the caller has already reserved a nonce and computed fees, and
    /// silently changing either would break accounting.
    async fn sign(&self, tx: &UnsignedTransaction) -> Result<SignedTransaction>;

    /// The address this signer signs for -- the hot wallet.
    fn address(&self, kind: ChainKind) -> Result<Address>;

    /// Backend name for logs and `/health`. Never includes key material.
    fn backend_name(&self) -> &'static str;

    /// Whether this backend is acceptable outside development. Every shipped
    /// implementation returns `false`; a real custody backend would return
    /// `true`. `AppConfig::validate` refuses to boot a production-like
    /// environment when this is false.
    fn is_production_grade(&self) -> bool {
        false
    }
}

pub type SharedSigner = Arc<dyn Signer>;

/// Build a signer from configuration.
pub fn from_config(cfg: &chainrail_common::config::SignerConfig) -> Result<SharedSigner> {
    use chainrail_common::config::SignerConfig;
    match cfg {
        SignerConfig::LocalDevelopment { private_key } => {
            Ok(Arc::new(LocalDevelopmentSigner::from_hex(private_key)?))
        }
        SignerConfig::Mock => Ok(Arc::new(MockSigner::new())),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn request() -> EvmTransactionRequest {
        EvmTransactionRequest {
            chain_id: 84532,
            nonce: 7,
            to: Address::parse(ChainKind::Evm, "0x5aAeb6053F3E94C9b9A09f33669435E7Ef1BeAed")
                .unwrap(),
            value: Amount::ZERO,
            data: vec![],
            gas_limit: 100_000,
            max_fee_per_gas: Amount::new(2_000_000_000),
            max_priority_fee_per_gas: Amount::new(1_000_000_000),
        }
    }

    #[test]
    fn max_fee_cost_is_gas_times_price() {
        let r = request();
        assert_eq!(
            r.max_fee_cost().unwrap(),
            Amount::new(2_000_000_000i128 * 100_000)
        );
    }

    #[test]
    fn no_shipped_signer_claims_to_be_production_grade() {
        assert!(!MockSigner::new().is_production_grade());
        let dev = LocalDevelopmentSigner::from_hex(&"11".repeat(32)).unwrap();
        assert!(!dev.is_production_grade());
    }
}
