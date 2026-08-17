//! Deterministic fake signer for tests.
//!
//! Produces syntactically plausible but cryptographically invalid output. It
//! exists so that withdrawal-pipeline tests can run without key handling, not
//! to stand in for real signing: nothing it produces would be accepted by a
//! node.

use async_trait::async_trait;
use chainrail_common::chain::keccak256;
use chainrail_common::{Address, ChainKind, Error, Hash32, Result};

use crate::{SignedTransaction, Signer, UnsignedTransaction};

#[derive(Debug, Clone)]
pub struct MockSigner {
    address: Address,
}

impl Default for MockSigner {
    fn default() -> Self {
        MockSigner::new()
    }
}

impl MockSigner {
    pub fn new() -> Self {
        MockSigner {
            // Recognisable, non-zero, and not a real key's address.
            address: Address::from_storage("0x00000000000000000000000000000000000c0ffee"),
        }
    }

    pub fn with_address(address: Address) -> Self {
        MockSigner { address }
    }
}

#[async_trait]
impl Signer for MockSigner {
    async fn sign(&self, tx: &UnsignedTransaction) -> Result<SignedTransaction> {
        let UnsignedTransaction::Evm(req) = tx;
        // Hash the request fields so the resulting tx_hash is a deterministic
        // function of the transaction, exactly as a real signer's would be.
        let mut preimage = Vec::new();
        preimage.extend_from_slice(&req.chain_id.to_be_bytes());
        preimage.extend_from_slice(&req.nonce.to_be_bytes());
        preimage.extend_from_slice(req.to.storage_key().as_bytes());
        preimage.extend_from_slice(&req.value.raw().to_be_bytes());
        preimage.extend_from_slice(&req.gas_limit.to_be_bytes());
        preimage.extend_from_slice(&req.max_fee_per_gas.raw().to_be_bytes());
        preimage.extend_from_slice(&req.data);

        let digest = keccak256(&preimage);
        let tx_hash = Hash32::parse(&format!("0x{}", hex::encode(digest)))
            .map_err(|e| Error::Signer(format!("mock hash: {e}")))?;

        let mut raw = vec![0x02];
        raw.extend_from_slice(&digest);
        Ok(SignedTransaction {
            raw,
            tx_hash,
            from: self.address.clone(),
        })
    }

    fn address(&self, _kind: ChainKind) -> Result<Address> {
        Ok(self.address.clone())
    }

    fn backend_name(&self) -> &'static str {
        "mock"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::EvmTransactionRequest;
    use chainrail_common::Amount;

    fn request(nonce: u64) -> UnsignedTransaction {
        UnsignedTransaction::Evm(EvmTransactionRequest {
            chain_id: 84532,
            nonce,
            to: Address::from_storage("0x5aaeb6053f3e94c9b9a09f33669435e7ef1beaed"),
            value: Amount::ZERO,
            data: vec![1, 2, 3],
            gas_limit: 100_000,
            max_fee_per_gas: Amount::new(1_000),
            max_priority_fee_per_gas: Amount::new(100),
        })
    }

    #[tokio::test]
    async fn hashes_are_deterministic_per_transaction() {
        let s = MockSigner::new();
        assert_eq!(
            s.sign(&request(1)).await.unwrap().tx_hash,
            s.sign(&request(1)).await.unwrap().tx_hash
        );
        assert_ne!(
            s.sign(&request(1)).await.unwrap().tx_hash,
            s.sign(&request(2)).await.unwrap().tx_hash
        );
    }

    #[tokio::test]
    async fn reports_its_own_address_consistently() {
        let s = MockSigner::new();
        let signed = s.sign(&request(1)).await.unwrap();
        assert_eq!(signed.from, s.address(ChainKind::Evm).unwrap());
        assert_eq!(s.backend_name(), "mock");
    }
}
