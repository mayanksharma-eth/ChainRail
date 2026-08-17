//! The chain-adapter interface, and its EVM implementation.
//!
//! Everything above this layer (watcher, deposit engine, withdrawal pipeline)
//! is written against [`ChainAdapter`] and knows nothing about EVM specifics.
//! Adding Solana means implementing this trait -- see
//! `docs/architecture.md#non-evm-chains` for what that entails and why it is
//! not in v0.1.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use async_trait::async_trait;
use chainrail_common::config::ChainConfig;
use chainrail_common::{Address, Amount, ChainId, ChainKind, Error, Hash32, Result};
use chainrail_database::models::ObservedTransfer;
use chainrail_rpc::RpcGateway;
use chainrail_signer::{EvmTransactionRequest, SignedTransaction, UnsignedTransaction};

use crate::abi::{self, TRANSFER_TOPIC};
use crate::client::{EvmClient, FeeEstimate};

/// A block's identity and lineage -- everything reorg detection needs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlockRef {
    pub height: u64,
    pub hash: Hash32,
    pub parent_hash: Hash32,
    pub tx_count: u32,
}

/// An asset ChainRail watches on a chain.
#[derive(Debug, Clone)]
pub struct TrackedAsset {
    pub symbol: String,
    /// `None` for the chain's native asset.
    pub contract: Option<Address>,
    pub deposits_enabled: bool,
}

/// What the watcher needs in order to scan a range.
pub struct ScanRequest<'a> {
    pub from_height: u64,
    pub to_height: u64,
    /// Lowercase deposit addresses ChainRail monitors.
    pub tracked_addresses: &'a HashSet<String>,
    pub assets: &'a [TrackedAsset],
}

#[derive(Debug, Default)]
pub struct ScanResult {
    /// Headers for every block in the range, ascending. Persisted for reorg
    /// detection even when a block contains nothing interesting.
    pub headers: Vec<BlockRef>,
    pub transfers: Vec<ObservedTransfer>,
}

/// On-chain state of a transaction we broadcast.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TxStatus {
    /// Not visible to the node at all -- dropped, or never delivered.
    Unknown,
    /// In the mempool, not yet mined.
    Pending,
    Mined {
        block_number: u64,
        block_hash: Hash32,
        confirmations: u64,
        success: bool,
        fee_paid: Amount,
    },
}

/// What a withdrawal needs in order to become a transaction.
#[derive(Debug, Clone)]
pub struct WithdrawalIntent {
    pub destination: Address,
    pub amount: Amount,
    /// `None` for the chain's native asset.
    pub contract: Option<Address>,
    pub nonce: u64,
    pub from: Address,
}

#[async_trait]
pub trait ChainAdapter: Send + Sync {
    fn chain_id(&self) -> &ChainId;
    fn kind(&self) -> ChainKind;

    /// Validate that the node agrees about which chain this is.
    async fn verify_identity(&self) -> Result<()>;

    async fn head(&self) -> Result<BlockRef>;
    async fn header_at(&self, height: u64) -> Result<Option<BlockRef>>;
    async fn header_by_hash(&self, hash: &Hash32) -> Result<Option<BlockRef>>;

    /// Scan an inclusive block range for transfers into tracked addresses.
    async fn scan(&self, req: ScanRequest<'_>) -> Result<ScanResult>;

    /// Build an unsigned transaction. Fees are fetched here so the signer never
    /// needs network access.
    async fn build_withdrawal(&self, intent: &WithdrawalIntent) -> Result<UnsignedTransaction>;

    async fn broadcast(&self, signed: &SignedTransaction) -> Result<Hash32>;

    async fn transaction_status(&self, tx_hash: &Hash32, head: u64) -> Result<TxStatus>;

    /// The chain's view of the next nonce. A hint only; ChainRail's own nonce
    /// table is authoritative.
    async fn nonce_hint(&self, address: &Address) -> Result<u64>;

    async fn native_balance(&self, address: &Address) -> Result<Amount>;

    /// True when at least one RPC endpoint is usable.
    fn is_available(&self) -> bool;
}

pub struct EvmAdapter {
    chain_id: ChainId,
    client: EvmClient,
    gas_limit: u64,
}

impl EvmAdapter {
    pub fn new(cfg: &ChainConfig, gateway: Arc<RpcGateway>) -> Result<Self> {
        let numeric = cfg
            .numeric_chain_id
            .ok_or_else(|| Error::Config(format!("chain {} has no numeric_chain_id", cfg.id)))?;
        Ok(EvmAdapter {
            chain_id: cfg.id.clone(),
            client: EvmClient::new(gateway, numeric, cfg.fee_bump_pct),
            gas_limit: cfg.transfer_gas_limit,
        })
    }

    pub fn client(&self) -> &EvmClient {
        &self.client
    }

    /// ERC-20 deposits: one `eth_getLogs` covers the whole range, which is far
    /// cheaper than fetching every block's transactions.
    async fn scan_erc20(
        &self,
        req: &ScanRequest<'_>,
        by_contract: &HashMap<String, &TrackedAsset>,
    ) -> Result<Vec<ObservedTransfer>> {
        if by_contract.is_empty() {
            return Ok(vec![]);
        }
        let contracts: Vec<String> = by_contract.keys().cloned().collect();
        let logs = self
            .client
            .transfer_logs(req.from_height, req.to_height, &contracts, TRANSFER_TOPIC)
            .await?;

        let mut out = Vec::new();
        for log in logs {
            // `removed: true` means the provider is telling us this log belonged
            // to a block that has since been reorganised out. Skip it; the reorg
            // engine handles the consequences from block lineage, which is a
            // more trustworthy signal than a provider flag.
            if log.removed {
                metrics::counter!(
                    "chainrail_evm_removed_logs_skipped_total",
                    "chain" => self.chain_id.to_string(),
                )
                .increment(1);
                continue;
            }
            if !by_contract.contains_key(&log.address) {
                continue; // provider returned a contract we did not ask for
            }

            let transfer = match abi::decode_transfer_log(&log.topics, &log.data) {
                Ok(Some(t)) => t,
                Ok(None) => continue,
                Err(e) => {
                    // A malformed Transfer is loud, not silent: it may be a lost
                    // deposit, or a token whose ABI we do not handle.
                    tracing::error!(
                        chain = %self.chain_id,
                        tx_hash = %log.tx_hash,
                        log_index = log.log_index,
                        error = %e,
                        "could not decode a Transfer log; deposit may be missed"
                    );
                    metrics::counter!(
                        "chainrail_evm_undecodable_logs_total",
                        "chain" => self.chain_id.to_string(),
                    )
                    .increment(1);
                    continue;
                }
            };

            if !req.tracked_addresses.contains(&transfer.to.storage_key()) {
                continue;
            }
            out.push(ObservedTransfer {
                chain: self.chain_id.clone(),
                tx_hash: log.tx_hash,
                log_index: log.log_index,
                block_number: log.block_number,
                block_hash: log.block_hash,
                from_address: transfer.from,
                to_address: transfer.to,
                amount_raw: transfer.value,
                contract_address: Some(Address::from_storage(log.address)),
            });
        }
        Ok(out)
    }
}

#[async_trait]
impl ChainAdapter for EvmAdapter {
    fn chain_id(&self) -> &ChainId {
        &self.chain_id
    }

    fn kind(&self) -> ChainKind {
        ChainKind::Evm
    }

    async fn verify_identity(&self) -> Result<()> {
        self.client.verify_chain_id().await
    }

    async fn head(&self) -> Result<BlockRef> {
        let h = self.client.latest_header().await?;
        Ok(BlockRef {
            height: h.number,
            hash: h.hash,
            parent_hash: h.parent_hash,
            tx_count: h.tx_count,
        })
    }

    async fn header_at(&self, height: u64) -> Result<Option<BlockRef>> {
        Ok(self
            .client
            .header_by_number(height)
            .await?
            .map(|h| BlockRef {
                height: h.number,
                hash: h.hash,
                parent_hash: h.parent_hash,
                tx_count: h.tx_count,
            }))
    }

    async fn header_by_hash(&self, hash: &Hash32) -> Result<Option<BlockRef>> {
        Ok(self.client.header_by_hash(hash).await?.map(|h| BlockRef {
            height: h.number,
            hash: h.hash,
            parent_hash: h.parent_hash,
            tx_count: h.tx_count,
        }))
    }

    async fn scan(&self, req: ScanRequest<'_>) -> Result<ScanResult> {
        if req.from_height > req.to_height {
            return Err(Error::Validation(format!(
                "invalid scan range {}..{}",
                req.from_height, req.to_height
            )));
        }

        // Headers first, and for *every* block in the range. Reorg detection
        // depends on unbroken lineage, so a block with nothing interesting in it
        // still has to be recorded.
        let mut headers = Vec::with_capacity((req.to_height - req.from_height + 1) as usize);
        for height in req.from_height..=req.to_height {
            match self.header_at(height).await? {
                Some(h) => headers.push(h),
                None => {
                    // The node does not have a block it just told us existed.
                    // Stop the range here rather than leaving a gap in lineage.
                    tracing::warn!(
                        chain = %self.chain_id, height,
                        "node returned no block for a height inside the scan range; truncating"
                    );
                    break;
                }
            }
        }
        if headers.is_empty() {
            return Ok(ScanResult::default());
        }
        let effective_to = headers.last().map(|h| h.height).unwrap_or(req.from_height);

        let by_contract: HashMap<String, &TrackedAsset> = req
            .assets
            .iter()
            .filter(|a| a.deposits_enabled)
            .filter_map(|a| a.contract.as_ref().map(|c| (c.storage_key(), a)))
            .collect();

        let effective = ScanRequest {
            from_height: req.from_height,
            to_height: effective_to,
            tracked_addresses: req.tracked_addresses,
            assets: req.assets,
        };
        let transfers = self.scan_erc20(&effective, &by_contract).await?;

        metrics::counter!(
            "chainrail_blocks_processed_total",
            "chain" => self.chain_id.to_string(),
        )
        .increment(headers.len() as u64);

        Ok(ScanResult { headers, transfers })
    }

    async fn build_withdrawal(&self, intent: &WithdrawalIntent) -> Result<UnsignedTransaction> {
        if !intent.amount.is_positive() {
            return Err(Error::InvalidAmount(
                "withdrawal amount must be positive".into(),
            ));
        }
        let FeeEstimate {
            max_fee_per_gas,
            max_priority_fee_per_gas,
        } = self.client.estimate_fees().await?;

        // ERC-20: value is zero and the transfer rides in calldata addressed to
        // the token contract. Native: value carries the amount to the recipient.
        let (to, value, data) = match &intent.contract {
            Some(contract) => (
                contract.clone(),
                Amount::ZERO,
                abi::encode_transfer_call(&intent.destination, intent.amount)?,
            ),
            None => (intent.destination.clone(), intent.amount, Vec::new()),
        };

        Ok(UnsignedTransaction::Evm(EvmTransactionRequest {
            chain_id: self.client.numeric_chain_id(),
            nonce: intent.nonce,
            to,
            value,
            data,
            gas_limit: self.gas_limit,
            max_fee_per_gas,
            max_priority_fee_per_gas,
        }))
    }

    async fn broadcast(&self, signed: &SignedTransaction) -> Result<Hash32> {
        let returned = self.client.send_raw_transaction(&signed.raw_hex()).await?;
        // The node must agree with our locally computed hash. A mismatch means
        // something rewrote the transaction in flight, so we refuse to record it.
        if returned != signed.tx_hash {
            return Err(Error::Rpc(format!(
                "node returned tx hash {returned} but the signed transaction hashes to {}",
                signed.tx_hash
            )));
        }
        Ok(returned)
    }

    async fn transaction_status(&self, tx_hash: &Hash32, head: u64) -> Result<TxStatus> {
        match self.client.receipt(tx_hash).await? {
            Some(r) => Ok(TxStatus::Mined {
                block_number: r.block_number,
                block_hash: r.block_hash.clone(),
                confirmations: chainrail_common::FinalityPolicy::confirmations_for(
                    head,
                    r.block_number,
                ),
                success: r.success,
                fee_paid: r.fee_paid()?,
            }),
            None => {
                if self.client.transaction_exists(tx_hash).await? {
                    Ok(TxStatus::Pending)
                } else {
                    Ok(TxStatus::Unknown)
                }
            }
        }
    }

    async fn nonce_hint(&self, address: &Address) -> Result<u64> {
        self.client.pending_nonce(address.as_str()).await
    }

    async fn native_balance(&self, address: &Address) -> Result<Amount> {
        self.client.native_balance(address.as_str()).await
    }

    fn is_available(&self) -> bool {
        self.client.gateway().is_available()
    }
}

/// Build one adapter per configured chain.
pub fn build_adapters(
    chains: &[ChainConfig],
    registry: &chainrail_rpc::RpcRegistry,
) -> Result<HashMap<String, Arc<dyn ChainAdapter>>> {
    let mut out: HashMap<String, Arc<dyn ChainAdapter>> = HashMap::new();
    for cfg in chains {
        match cfg.kind {
            ChainKind::Evm => {
                let gateway = registry.get(cfg.id.as_str())?;
                out.insert(
                    cfg.id.to_string(),
                    Arc::new(EvmAdapter::new(cfg, gateway)?) as Arc<dyn ChainAdapter>,
                );
            }
            ChainKind::Solana => {
                return Err(Error::UnsupportedChain(format!(
                    "{}: no Solana adapter in v0.1",
                    cfg.id
                )))
            }
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tx_status_variants_are_distinguishable() {
        let mined = TxStatus::Mined {
            block_number: 100,
            block_hash: Hash32::from_storage("0xab"),
            confirmations: 12,
            success: true,
            fee_paid: Amount::new(1_000),
        };
        assert_ne!(mined, TxStatus::Pending);
        assert_ne!(TxStatus::Pending, TxStatus::Unknown);
        // A mined-but-reverted transaction is distinct from a successful one:
        // for a withdrawal, the funds did not move.
        let reverted = TxStatus::Mined {
            block_number: 100,
            block_hash: Hash32::from_storage("0xab"),
            confirmations: 12,
            success: false,
            fee_paid: Amount::new(1_000),
        };
        assert_ne!(mined, reverted);
    }

    #[test]
    fn solana_config_is_refused_rather_than_silently_ignored() {
        use chainrail_common::FinalityPolicy;
        let chains = vec![ChainConfig {
            id: ChainId::new("solana-devnet").unwrap(),
            kind: ChainKind::Solana,
            numeric_chain_id: None,
            finality: FinalityPolicy::Tag {
                tag: "finalized".into(),
            },
            poll_interval_ms: 1_000,
            block_batch_size: 10,
            reorg_scan_depth: 32,
            start_block: None,
            hot_wallet_address: None,
            transfer_gas_limit: 0,
            fee_bump_pct: 100,
            rpc: vec![chainrail_common::config::RpcEndpointConfig {
                name: "a".into(),
                url: "http://127.0.0.1:1".into(),
                weight: 100,
                timeout_ms: 100,
                max_inflight: 1,
                failure_threshold: 1,
                breaker_reset_ms: 1_000,
            }],
            assets: vec![],
        }];
        let registry = chainrail_rpc::RpcRegistry::build(&chains).unwrap();
        assert!(matches!(
            build_adapters(&chains, &registry),
            Err(Error::UnsupportedChain(_))
        ));
    }

    #[test]
    fn evm_adapter_requires_a_numeric_chain_id() {
        use chainrail_common::FinalityPolicy;
        let mut cfg = ChainConfig {
            id: ChainId::new("base-sepolia").unwrap(),
            kind: ChainKind::Evm,
            numeric_chain_id: None,
            finality: FinalityPolicy::Confirmations { blocks: 10 },
            poll_interval_ms: 1_000,
            block_batch_size: 10,
            reorg_scan_depth: 32,
            start_block: None,
            hot_wallet_address: None,
            transfer_gas_limit: 100_000,
            fee_bump_pct: 125,
            rpc: vec![chainrail_common::config::RpcEndpointConfig {
                name: "a".into(),
                url: "http://127.0.0.1:1".into(),
                weight: 100,
                timeout_ms: 100,
                max_inflight: 1,
                failure_threshold: 1,
                breaker_reset_ms: 1_000,
            }],
            assets: vec![],
        };
        let gw = RpcGateway::from_chain_config(&cfg).unwrap();
        assert!(EvmAdapter::new(&cfg, Arc::clone(&gw)).is_err());
        cfg.numeric_chain_id = Some(84532);
        assert!(EvmAdapter::new(&cfg, gw).is_ok());
    }
}
