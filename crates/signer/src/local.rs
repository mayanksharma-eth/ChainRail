//! In-process key signer. Development only -- see the crate docs.

use alloy::consensus::{SignableTransaction, TxEip1559};
use alloy::eips::eip2718::Encodable2718;
use alloy::primitives::{Address as AlloyAddress, Bytes, TxKind, U256};
use alloy::signers::local::PrivateKeySigner;
use alloy::signers::SignerSync;
use async_trait::async_trait;
use chainrail_common::{Address, ChainKind, Error, Hash32, Result};
use std::str::FromStr;

use crate::{EvmTransactionRequest, SignedTransaction, Signer, UnsignedTransaction};

/// Wraps the key so that `Debug` and any accidental log/format call cannot leak
/// it. There is no accessor that returns the raw bytes.
struct RedactedKey(PrivateKeySigner);

impl std::fmt::Debug for RedactedKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("PrivateKeySigner(<redacted>)")
    }
}

#[derive(Debug)]
pub struct LocalDevelopmentSigner {
    key: RedactedKey,
    address: Address,
}

impl LocalDevelopmentSigner {
    pub fn from_hex(private_key: &str) -> Result<Self> {
        let cleaned = private_key.trim().trim_start_matches("0x");
        if cleaned.len() != 64 || !cleaned.chars().all(|c| c.is_ascii_hexdigit()) {
            // Deliberately does not echo the input: an operator pasting a real
            // key into the wrong field must not have it land in a log.
            return Err(Error::Signer(
                "private key must be 32 bytes of hex (value not echoed)".into(),
            ));
        }
        let key = PrivateKeySigner::from_str(cleaned)
            .map_err(|_| Error::Signer("could not parse secp256k1 private key".into()))?;
        let address = Address::parse(ChainKind::Evm, &key.address().to_checksum(None))
            .map_err(|e| Error::Signer(format!("derived address is invalid: {e}")))?;
        tracing::warn!(
            signer_address = %address,
            "using LocalDevelopmentSigner: key material is held in process memory. \
             Never use this outside local/test environments."
        );
        Ok(LocalDevelopmentSigner {
            key: RedactedKey(key),
            address,
        })
    }

    fn sign_evm(&self, req: &EvmTransactionRequest) -> Result<SignedTransaction> {
        let to = AlloyAddress::from_str(req.to.as_str())
            .map_err(|e| Error::Signer(format!("invalid destination: {e}")))?;

        let tx = TxEip1559 {
            chain_id: req.chain_id,
            nonce: req.nonce,
            gas_limit: req.gas_limit,
            max_fee_per_gas: u128::try_from(req.max_fee_per_gas.raw())
                .map_err(|_| Error::Signer("negative max_fee_per_gas".into()))?,
            max_priority_fee_per_gas: u128::try_from(req.max_priority_fee_per_gas.raw())
                .map_err(|_| Error::Signer("negative max_priority_fee_per_gas".into()))?,
            to: TxKind::Call(to),
            value: U256::from(
                u128::try_from(req.value.raw())
                    .map_err(|_| Error::Signer("negative transfer value".into()))?,
            ),
            input: Bytes::from(req.data.clone()),
            access_list: Default::default(),
        };

        let hash = tx.signature_hash();
        let signature = self
            .key
            .0
            .sign_hash_sync(&hash)
            .map_err(|e| Error::Signer(format!("signing failed: {e}")))?;
        let signed = tx.into_signed(signature);
        let tx_hash = Hash32::parse(&format!("{:#x}", signed.hash()))
            .map_err(|e| Error::Signer(format!("bad tx hash: {e}")))?;

        let mut raw = Vec::with_capacity(256);
        signed.encode_2718(&mut raw);

        Ok(SignedTransaction {
            raw,
            tx_hash,
            from: self.address.clone(),
        })
    }
}

#[async_trait]
impl Signer for LocalDevelopmentSigner {
    async fn sign(&self, tx: &UnsignedTransaction) -> Result<SignedTransaction> {
        match tx {
            UnsignedTransaction::Evm(req) => self.sign_evm(req),
        }
    }

    fn address(&self, kind: ChainKind) -> Result<Address> {
        match kind {
            ChainKind::Evm => Ok(self.address.clone()),
            ChainKind::Solana => Err(Error::Signer(
                "LocalDevelopmentSigner has no Solana implementation".into(),
            )),
        }
    }

    fn backend_name(&self) -> &'static str {
        "local_development"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chainrail_common::Amount;

    /// Hardhat account #0. Public, worthless, and used by every EVM test suite
    /// in existence -- which is exactly why it is safe to hardcode here.
    const TEST_KEY: &str = "ac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80";
    const TEST_ADDRESS: &str = "0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266";

    fn signer() -> LocalDevelopmentSigner {
        LocalDevelopmentSigner::from_hex(TEST_KEY).unwrap()
    }

    fn request(nonce: u64) -> EvmTransactionRequest {
        EvmTransactionRequest {
            chain_id: 84532,
            nonce,
            to: Address::parse(ChainKind::Evm, "0x5aAeb6053F3E94C9b9A09f33669435E7Ef1BeAed")
                .unwrap(),
            value: Amount::ZERO,
            data: hex::decode("a9059cbb").unwrap(),
            gas_limit: 100_000,
            max_fee_per_gas: Amount::new(2_000_000_000),
            max_priority_fee_per_gas: Amount::new(1_000_000_000),
        }
    }

    #[test]
    fn derives_the_expected_address() {
        assert_eq!(
            signer().address(ChainKind::Evm).unwrap().as_str(),
            TEST_ADDRESS
        );
    }

    #[test]
    fn accepts_keys_with_or_without_0x() {
        let a = LocalDevelopmentSigner::from_hex(TEST_KEY).unwrap();
        let b = LocalDevelopmentSigner::from_hex(&format!("0x{TEST_KEY}")).unwrap();
        assert_eq!(
            a.address(ChainKind::Evm).unwrap(),
            b.address(ChainKind::Evm).unwrap()
        );
    }

    #[test]
    fn rejects_malformed_keys_without_echoing_them() {
        for bad in ["", "0x", "deadbeef", &"z".repeat(64)] {
            let err = LocalDevelopmentSigner::from_hex(bad).unwrap_err();
            assert!(
                !err.to_string().contains(bad) || bad.is_empty(),
                "error echoed the key material: {err}"
            );
        }
    }

    #[test]
    fn debug_output_never_contains_key_material() {
        let s = signer();
        let dbg = format!("{s:?}");
        assert!(!dbg.contains(TEST_KEY), "Debug leaked the private key");
        assert!(dbg.contains("<redacted>"));
    }

    #[tokio::test]
    async fn signing_is_deterministic_and_produces_a_usable_envelope() {
        let s = signer();
        let tx = UnsignedTransaction::Evm(request(7));
        let a = s.sign(&tx).await.unwrap();
        let b = s.sign(&tx).await.unwrap();

        // ECDSA over secp256k1 with RFC-6979 deterministic k: identical input
        // must yield an identical hash, which is what makes broadcast retries
        // safe.
        assert_eq!(a.tx_hash, b.tx_hash);
        assert_eq!(a.raw, b.raw);
        assert_eq!(a.from.as_str(), TEST_ADDRESS);
        // EIP-1559 typed transaction envelope.
        assert_eq!(a.raw[0], 0x02, "expected an EIP-1559 (type 2) envelope");
        assert!(a.raw_hex().starts_with("0x02"));
    }

    #[tokio::test]
    async fn different_nonces_produce_different_hashes() {
        let s = signer();
        let a = s.sign(&UnsignedTransaction::Evm(request(1))).await.unwrap();
        let b = s.sign(&UnsignedTransaction::Evm(request(2))).await.unwrap();
        assert_ne!(a.tx_hash, b.tx_hash);
    }

    #[tokio::test]
    async fn chain_id_is_bound_into_the_signature() {
        let s = signer();
        let mut other_chain = request(1);
        other_chain.chain_id = 1; // mainnet
        let base = s.sign(&UnsignedTransaction::Evm(request(1))).await.unwrap();
        let mainnet = s
            .sign(&UnsignedTransaction::Evm(other_chain))
            .await
            .unwrap();
        assert_ne!(
            base.tx_hash, mainnet.tx_hash,
            "EIP-155 replay protection: the chain id must affect the signature"
        );
    }

    #[test]
    fn solana_is_refused_rather_than_faked() {
        assert!(signer().address(ChainKind::Solana).is_err());
    }
}
