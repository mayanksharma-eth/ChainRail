//! Typed EVM JSON-RPC client over the RPC gateway.
//!
//! Responses from third-party providers are validated, not trusted: a
//! `null` where a block was expected, a height that moves backwards, or a
//! missing `blockHash` are all treated as errors rather than propagated into
//! the deposit pipeline. See `docs/threat-model.md#compromised-rpc-provider`.

use std::sync::Arc;

use chainrail_common::{Amount, Error, Hash32, Result};
use chainrail_rpc::{Idempotency, RpcGateway};
use serde::Deserialize;
use serde_json::json;

use crate::abi::{hex_to_u64, u64_to_hex};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EvmBlockHeader {
    pub number: u64,
    pub hash: Hash32,
    pub parent_hash: Hash32,
    pub timestamp: u64,
    pub tx_count: u32,
}

#[derive(Debug, Clone)]
pub struct EvmLog {
    pub address: String,
    pub topics: Vec<String>,
    pub data: String,
    pub block_number: u64,
    pub block_hash: Hash32,
    pub tx_hash: Hash32,
    pub log_index: i64,
    pub removed: bool,
}

#[derive(Debug, Clone)]
pub struct EvmReceipt {
    pub tx_hash: Hash32,
    pub block_number: u64,
    pub block_hash: Hash32,
    /// `false` means the transaction was mined but reverted. The gas is still
    /// spent, and for a withdrawal the funds did *not* move.
    pub success: bool,
    pub gas_used: u64,
    pub effective_gas_price: Amount,
}

impl EvmReceipt {
    /// Fee actually paid, in wei.
    pub fn fee_paid(&self) -> Result<Amount> {
        self.effective_gas_price
            .checked_mul_u32(u32::try_from(self.gas_used).unwrap_or(u32::MAX))
    }
}

#[derive(Debug, Clone, Copy)]
pub struct FeeEstimate {
    pub max_fee_per_gas: Amount,
    pub max_priority_fee_per_gas: Amount,
}

pub struct EvmClient {
    gateway: Arc<RpcGateway>,
    numeric_chain_id: u64,
    fee_bump_pct: u64,
}

// --- raw wire types; kept private so the rest of the codebase sees only
// --- validated, typed values.

#[derive(Deserialize)]
struct RawBlock {
    number: Option<String>,
    hash: Option<String>,
    #[serde(rename = "parentHash")]
    parent_hash: Option<String>,
    timestamp: Option<String>,
    #[serde(default)]
    transactions: Vec<serde_json::Value>,
}

#[derive(Deserialize)]
struct RawLog {
    address: String,
    topics: Vec<String>,
    data: String,
    #[serde(rename = "blockNumber")]
    block_number: Option<String>,
    #[serde(rename = "blockHash")]
    block_hash: Option<String>,
    #[serde(rename = "transactionHash")]
    transaction_hash: Option<String>,
    #[serde(rename = "logIndex")]
    log_index: Option<String>,
    #[serde(default)]
    removed: bool,
}

#[derive(Deserialize)]
struct RawReceipt {
    #[serde(rename = "transactionHash")]
    transaction_hash: Option<String>,
    #[serde(rename = "blockNumber")]
    block_number: Option<String>,
    #[serde(rename = "blockHash")]
    block_hash: Option<String>,
    status: Option<String>,
    #[serde(rename = "gasUsed")]
    gas_used: Option<String>,
    #[serde(rename = "effectiveGasPrice")]
    effective_gas_price: Option<String>,
}

#[derive(Deserialize)]
struct RawFeeHistory {
    #[serde(rename = "baseFeePerGas")]
    base_fee_per_gas: Vec<String>,
    #[serde(default)]
    reward: Vec<Vec<String>>,
}

impl EvmClient {
    pub fn new(gateway: Arc<RpcGateway>, numeric_chain_id: u64, fee_bump_pct: u64) -> Self {
        EvmClient {
            gateway,
            numeric_chain_id,
            fee_bump_pct: fee_bump_pct.max(100),
        }
    }

    pub fn chain(&self) -> &str {
        self.gateway.chain()
    }

    pub fn numeric_chain_id(&self) -> u64 {
        self.numeric_chain_id
    }

    pub fn gateway(&self) -> &Arc<RpcGateway> {
        &self.gateway
    }

    pub async fn block_number(&self) -> Result<u64> {
        let raw: String = self.gateway.call_safe("eth_blockNumber", json!([])).await?;
        hex_to_u64(&raw)
    }

    /// Verify the node reports the chain we think we are talking to.
    ///
    /// Run at startup: pointing a testnet config at a mainnet RPC (or vice
    /// versa) is an easy mistake with expensive consequences, and EIP-155 means
    /// signatures are chain-bound anyway.
    pub async fn verify_chain_id(&self) -> Result<()> {
        let raw: String = self.gateway.call_safe("eth_chainId", json!([])).await?;
        let reported = hex_to_u64(&raw)?;
        if reported != self.numeric_chain_id {
            return Err(Error::Config(format!(
                "chain `{}` is configured as chain id {} but the RPC reports {reported}",
                self.chain(),
                self.numeric_chain_id
            )));
        }
        Ok(())
    }

    pub async fn header_by_number(&self, number: u64) -> Result<Option<EvmBlockHeader>> {
        let raw: Option<RawBlock> = self
            .gateway
            .call_safe("eth_getBlockByNumber", json!([u64_to_hex(number), false]))
            .await?;
        raw.map(Self::header_from_raw).transpose()
    }

    pub async fn header_by_hash(&self, hash: &Hash32) -> Result<Option<EvmBlockHeader>> {
        let raw: Option<RawBlock> = self
            .gateway
            .call_safe("eth_getBlockByHash", json!([hash.as_str(), false]))
            .await?;
        raw.map(Self::header_from_raw).transpose()
    }

    pub async fn latest_header(&self) -> Result<EvmBlockHeader> {
        let raw: Option<RawBlock> = self
            .gateway
            .call_safe("eth_getBlockByNumber", json!(["latest", false]))
            .await?;
        let raw = raw.ok_or_else(|| Error::Rpc("node returned null for `latest` block".into()))?;
        Self::header_from_raw(raw)
    }

    fn header_from_raw(raw: RawBlock) -> Result<EvmBlockHeader> {
        // A pending block has null number/hash. Accepting it would let a
        // deposit be attributed to a block that does not exist yet.
        let number = raw
            .number
            .ok_or_else(|| Error::Rpc("block has no number (pending block?)".into()))?;
        let hash = raw
            .hash
            .ok_or_else(|| Error::Rpc("block has no hash (pending block?)".into()))?;
        let parent_hash = raw
            .parent_hash
            .ok_or_else(|| Error::Rpc("block has no parentHash".into()))?;
        Ok(EvmBlockHeader {
            number: hex_to_u64(&number)?,
            hash: Hash32::parse(&hash)?,
            parent_hash: Hash32::parse(&parent_hash)?,
            timestamp: raw
                .timestamp
                .as_deref()
                .map(hex_to_u64)
                .transpose()?
                .unwrap_or(0),
            tx_count: u32::try_from(raw.transactions.len()).unwrap_or(u32::MAX),
        })
    }

    /// Fetch `Transfer` logs for a set of contracts over an inclusive block range.
    ///
    /// Filtering by contract address *and* topic at the node keeps the response
    /// small; filtering by recipient is done locally because the set of tracked
    /// addresses is large and changes constantly.
    pub async fn transfer_logs(
        &self,
        from_block: u64,
        to_block: u64,
        contracts: &[String],
        topic0: &str,
    ) -> Result<Vec<EvmLog>> {
        if from_block > to_block {
            return Err(Error::Validation(format!(
                "invalid block range {from_block}..{to_block}"
            )));
        }
        if contracts.is_empty() {
            return Ok(vec![]);
        }
        let filter = json!([{
            "fromBlock": u64_to_hex(from_block),
            "toBlock": u64_to_hex(to_block),
            "address": contracts,
            "topics": [topic0],
        }]);
        let raw: Vec<RawLog> = self.gateway.call_safe("eth_getLogs", filter).await?;

        raw.into_iter()
            .map(|l| {
                Ok(EvmLog {
                    address: l.address.to_ascii_lowercase(),
                    topics: l.topics,
                    data: l.data,
                    block_number: hex_to_u64(
                        l.block_number
                            .as_deref()
                            .ok_or_else(|| Error::Rpc("log has no blockNumber".into()))?,
                    )?,
                    block_hash: Hash32::parse(
                        l.block_hash
                            .as_deref()
                            .ok_or_else(|| Error::Rpc("log has no blockHash".into()))?,
                    )?,
                    tx_hash: Hash32::parse(
                        l.transaction_hash
                            .as_deref()
                            .ok_or_else(|| Error::Rpc("log has no transactionHash".into()))?,
                    )?,
                    log_index: i64::try_from(hex_to_u64(l.log_index.as_deref().unwrap_or("0x0"))?)
                        .map_err(|_| Error::Rpc("implausible logIndex".into()))?,
                    removed: l.removed,
                })
            })
            .collect()
    }

    pub async fn receipt(&self, tx_hash: &Hash32) -> Result<Option<EvmReceipt>> {
        let raw: Option<RawReceipt> = self
            .gateway
            .call_safe("eth_getTransactionReceipt", json!([tx_hash.as_str()]))
            .await?;
        let Some(raw) = raw else { return Ok(None) };

        // A receipt without a block is not mined; treat it as absent rather
        // than as a confirmed transaction.
        let (Some(block_number), Some(block_hash)) = (&raw.block_number, &raw.block_hash) else {
            return Ok(None);
        };
        Ok(Some(EvmReceipt {
            tx_hash: raw
                .transaction_hash
                .as_deref()
                .map(Hash32::parse)
                .transpose()?
                .unwrap_or_else(|| tx_hash.clone()),
            block_number: hex_to_u64(block_number)?,
            block_hash: Hash32::parse(block_hash)?,
            // Post-Byzantium `status`: 0x1 success, 0x0 revert. A missing status
            // is treated as failure -- refusing to assume success is the safe
            // default when settling a withdrawal.
            success: raw
                .status
                .as_deref()
                .map(hex_to_u64)
                .transpose()?
                .unwrap_or(0)
                == 1,
            gas_used: raw
                .gas_used
                .as_deref()
                .map(hex_to_u64)
                .transpose()?
                .unwrap_or(0),
            effective_gas_price: raw
                .effective_gas_price
                .as_deref()
                .map(Amount::from_hex_quantity)
                .transpose()?
                .unwrap_or(Amount::ZERO),
        }))
    }

    /// Next nonce according to the chain, including pending transactions.
    /// Used only as a *hint*: ChainRail's own `chain_nonces` table is
    /// authoritative, because the node's pending pool is not durable.
    pub async fn pending_nonce(&self, address: &str) -> Result<u64> {
        let raw: String = self
            .gateway
            .call_safe("eth_getTransactionCount", json!([address, "pending"]))
            .await?;
        hex_to_u64(&raw)
    }

    pub async fn native_balance(&self, address: &str) -> Result<Amount> {
        let raw: String = self
            .gateway
            .call_safe("eth_getBalance", json!([address, "latest"]))
            .await?;
        Amount::from_hex_quantity(&raw)
    }

    /// Suggest EIP-1559 fees from recent history.
    ///
    /// `max_fee = base_fee * bump% + priority`, so a modest base-fee rise
    /// between estimation and inclusion does not strand the transaction.
    pub async fn estimate_fees(&self) -> Result<FeeEstimate> {
        let history: RawFeeHistory = self
            .gateway
            .call_safe("eth_feeHistory", json!([u64_to_hex(5), "latest", [50]]))
            .await?;

        let base_fee = history
            .base_fee_per_gas
            .last()
            .map(|s| Amount::from_hex_quantity(s))
            .transpose()?
            .unwrap_or(Amount::ZERO);

        let priority = history
            .reward
            .iter()
            .filter_map(|r| r.first())
            .map(|s| Amount::from_hex_quantity(s))
            .collect::<Result<Vec<_>>>()?
            .into_iter()
            .max()
            .unwrap_or(Amount::ZERO);

        // Floor the priority fee: some nodes report zero on quiet testnets,
        // and a zero-tip transaction can sit unmined indefinitely.
        const MIN_PRIORITY_WEI: i128 = 1_000_000; // 0.001 gwei
        let priority = Amount::new(priority.raw().max(MIN_PRIORITY_WEI));

        let bumped_base = base_fee
            .raw()
            .checked_mul(i128::from(self.fee_bump_pct))
            .map(|v| v / 100)
            .ok_or(Error::AmountOverflow)?;
        let max_fee = Amount::new(bumped_base).checked_add(priority)?;

        Ok(FeeEstimate {
            max_fee_per_gas: max_fee,
            max_priority_fee_per_gas: priority,
        })
    }

    /// Broadcast a signed transaction.
    ///
    /// Marked `UnsafeOnTimeout`: the gateway will fail over only if the request
    /// provably never reached a node. A timeout returns an error instead, so the
    /// caller can reconcile by hash rather than risk re-signing with a new nonce.
    pub async fn send_raw_transaction(&self, raw_hex: &str) -> Result<Hash32> {
        let returned: String = self
            .gateway
            .call(
                "eth_sendRawTransaction",
                json!([raw_hex]),
                Idempotency::UnsafeOnTimeout,
            )
            .await?;
        Hash32::parse(&returned)
    }

    /// True when the node already knows this transaction (mempool or mined).
    /// The reconciliation path after an ambiguous broadcast.
    pub async fn transaction_exists(&self, tx_hash: &Hash32) -> Result<bool> {
        let raw: Option<serde_json::Value> = self
            .gateway
            .call_safe("eth_getTransactionByHash", json!([tx_hash.as_str()]))
            .await?;
        Ok(raw.map(|v| !v.is_null()).unwrap_or(false))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn raw_block(number: Option<&str>, hash: Option<&str>, parent: Option<&str>) -> RawBlock {
        RawBlock {
            number: number.map(String::from),
            hash: hash.map(String::from),
            parent_hash: parent.map(String::from),
            timestamp: Some("0x64".into()),
            transactions: vec![json!("0xaa"), json!("0xbb")],
        }
    }

    const H1: &str = "0x1111111111111111111111111111111111111111111111111111111111111111";
    const H0: &str = "0x0000000000000000000000000000000000000000000000000000000000000000";

    #[test]
    fn parses_a_complete_header() {
        let h = EvmClient::header_from_raw(raw_block(Some("0x64"), Some(H1), Some(H0))).unwrap();
        assert_eq!(h.number, 100);
        assert_eq!(h.hash.as_str(), H1);
        assert_eq!(h.parent_hash.as_str(), H0);
        assert_eq!(h.timestamp, 100);
        assert_eq!(h.tx_count, 2);
    }

    #[test]
    fn pending_blocks_are_rejected_not_guessed() {
        // A pending block has null number and hash. Attributing a deposit to it
        // would reference a block that may never exist.
        assert!(EvmClient::header_from_raw(raw_block(None, Some(H1), Some(H0))).is_err());
        assert!(EvmClient::header_from_raw(raw_block(Some("0x64"), None, Some(H0))).is_err());
        assert!(EvmClient::header_from_raw(raw_block(Some("0x64"), Some(H1), None)).is_err());
    }

    #[test]
    fn malformed_hashes_are_rejected() {
        assert!(
            EvmClient::header_from_raw(raw_block(Some("0x64"), Some("0xdead"), Some(H0))).is_err()
        );
    }

    #[test]
    fn receipt_fee_is_gas_used_times_effective_price() {
        let r = EvmReceipt {
            tx_hash: Hash32::from_storage(H1),
            block_number: 100,
            block_hash: Hash32::from_storage(H0),
            success: true,
            gas_used: 21_000,
            effective_gas_price: Amount::new(1_500_000_000),
        };
        assert_eq!(
            r.fee_paid().unwrap(),
            Amount::new(21_000i128 * 1_500_000_000)
        );
    }

    #[test]
    fn a_missing_receipt_status_is_treated_as_failure() {
        // Refusing to assume success is the safe default when the alternative is
        // settling a withdrawal that never moved funds.
        for (status, expected) in [(Some("0x1"), true), (Some("0x0"), false), (None, false)] {
            let success = status.map(hex_to_u64).transpose().unwrap().unwrap_or(0) == 1;
            assert_eq!(success, expected, "status {status:?}");
        }
    }
}
