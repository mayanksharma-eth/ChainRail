//! A scriptable in-memory chain implementing [`ChainAdapter`].
//!
//! This is *not* a mock that stubs out the logic under test. The watcher, reorg
//! engine, confirmation engine and withdrawal pipeline all run for real against
//! it; only the network is replaced. That is the only way to test reorg handling
//! deterministically — you cannot ask a real testnet to fork on cue.
//!
//! Everything a real adapter must answer is answered here: block lineage, log
//! scanning, receipts, nonces and broadcast.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use chainrail_chains_evm::{
    BlockRef, ChainAdapter, ScanRequest, ScanResult, TxStatus, WithdrawalIntent,
};
use chainrail_common::{Address, Amount, ChainId, ChainKind, Error, Hash32, Result};
use chainrail_database::models::ObservedTransfer;
use chainrail_signer::{EvmTransactionRequest, SignedTransaction, UnsignedTransaction};
use parking_lot::Mutex;

#[derive(Debug, Clone)]
pub struct MockTransfer {
    pub tx_hash: Hash32,
    pub log_index: i64,
    pub from: Address,
    pub to: Address,
    pub amount: Amount,
    pub contract: Option<Address>,
}

#[derive(Debug, Clone)]
struct MockBlock {
    height: u64,
    hash: Hash32,
    parent_hash: Hash32,
    transfers: Vec<MockTransfer>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BroadcastRecord {
    pub tx_hash: Hash32,
    pub raw_len: usize,
}

#[derive(Default)]
struct State {
    /// Canonical chain, ascending by height.
    blocks: Vec<MockBlock>,
    /// Blocks that were once canonical but have been forked away. Kept so
    /// `header_by_hash` still answers, as a real node would.
    orphaned: HashMap<String, MockBlock>,
    broadcasts: Vec<BroadcastRecord>,
    /// tx_hash -> on-chain status, so a test can script "mined and reverted".
    tx_status: HashMap<String, TxStatus>,
    next_nonce: u64,
    /// When set, every RPC-ish call fails, simulating a provider outage.
    fail_with: Option<String>,
    /// Counts calls, so a test can assert failover/retry behaviour.
    head_calls: usize,
}

pub struct MockChain {
    chain_id: ChainId,
    numeric_chain_id: u64,
    state: Mutex<State>,
}

impl MockChain {
    pub fn new(chain: &str) -> Arc<Self> {
        let genesis = MockBlock {
            height: 0,
            hash: block_hash(0, "genesis"),
            parent_hash: zero_hash(),
            transfers: vec![],
        };
        Arc::new(MockChain {
            chain_id: ChainId::new(chain).expect("valid chain id"),
            numeric_chain_id: 84532,
            state: Mutex::new(State {
                blocks: vec![genesis],
                next_nonce: 0,
                ..Default::default()
            }),
        })
    }

    /// Append a block, optionally containing transfers. `fork_tag` distinguishes
    /// competing blocks at the same height (`"a"` vs `"b"`).
    pub fn mine(&self, fork_tag: &str, transfers: Vec<MockTransfer>) -> BlockRef {
        let mut s = self.state.lock();
        let parent = s.blocks.last().expect("chain is never empty").clone();
        let height = parent.height + 1;
        let block = MockBlock {
            height,
            hash: block_hash(height, fork_tag),
            parent_hash: parent.hash.clone(),
            transfers,
        };
        let r = BlockRef {
            height,
            hash: block.hash.clone(),
            parent_hash: block.parent_hash.clone(),
            tx_count: block.transfers.len() as u32,
        };
        s.blocks.push(block);
        r
    }

    /// Mine `n` empty blocks. Used to accumulate confirmations.
    pub fn mine_empty(&self, n: u64) {
        for i in 0..n {
            self.mine(&format!("filler-{i}-{}", self.height()), vec![]);
        }
    }

    /// Fork the chain: discard everything above `keep_height` and replace it with
    /// `replacement_tags.len()` new blocks carrying `replacement_transfers`.
    ///
    /// Models the scenario in the brief:
    ///   100 -> 101 -> 102   becomes   100 -> 101' -> 102' -> 103'
    pub fn reorg_to(
        &self,
        keep_height: u64,
        replacement_tags: &[&str],
        replacement_transfers: Vec<Vec<MockTransfer>>,
    ) {
        let mut s = self.state.lock();
        // Retain the abandoned blocks so hash lookups still resolve, like a node.
        let discarded: Vec<MockBlock> = s
            .blocks
            .iter()
            .filter(|b| b.height > keep_height)
            .cloned()
            .collect();
        for b in discarded {
            s.orphaned.insert(b.hash.as_str().to_string(), b);
        }
        s.blocks.retain(|b| b.height <= keep_height);

        let mut parent = s.blocks.last().expect("ancestor must exist").clone();
        for (i, tag) in replacement_tags.iter().enumerate() {
            let height = parent.height + 1;
            let block = MockBlock {
                height,
                hash: block_hash(height, tag),
                parent_hash: parent.hash.clone(),
                transfers: replacement_transfers.get(i).cloned().unwrap_or_default(),
            };
            parent = block.clone();
            s.blocks.push(block);
        }
    }

    pub fn height(&self) -> u64 {
        self.state
            .lock()
            .blocks
            .last()
            .map(|b| b.height)
            .unwrap_or(0)
    }

    pub fn hash_at(&self, height: u64) -> Option<Hash32> {
        self.state
            .lock()
            .blocks
            .iter()
            .find(|b| b.height == height)
            .map(|b| b.hash.clone())
    }

    pub fn broadcasts(&self) -> Vec<BroadcastRecord> {
        self.state.lock().broadcasts.clone()
    }

    /// Script the on-chain outcome for a transaction hash.
    pub fn set_tx_status(&self, tx_hash: &Hash32, status: TxStatus) {
        self.state
            .lock()
            .tx_status
            .insert(tx_hash.as_str().to_string(), status);
    }

    /// Simulate a provider outage; every call fails until cleared.
    pub fn set_failing(&self, reason: Option<&str>) {
        self.state.lock().fail_with = reason.map(String::from);
    }

    pub fn head_calls(&self) -> usize {
        self.state.lock().head_calls
    }

    pub fn set_nonce(&self, nonce: u64) {
        self.state.lock().next_nonce = nonce;
    }

    fn guard(&self) -> Result<()> {
        match self.state.lock().fail_with.clone() {
            Some(reason) => Err(Error::Rpc(reason)),
            None => Ok(()),
        }
    }
}

fn block_hash(height: u64, tag: &str) -> Hash32 {
    // Deterministic and collision-free for test purposes: height and tag are
    // both encoded, so `mine("a")` and `mine("b")` at one height differ.
    let digest = chainrail_common::chain::keccak256(format!("{height}:{tag}").as_bytes());
    Hash32::parse(&format!("0x{}", hex_encode(&digest))).expect("valid hash")
}

fn zero_hash() -> Hash32 {
    Hash32::from_storage(format!("0x{}", "0".repeat(64)))
}

fn hex_encode(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// Deterministic transfer hash from a label, so tests can refer to one.
pub fn tx_hash(label: &str) -> Hash32 {
    let digest = chainrail_common::chain::keccak256(format!("tx:{label}").as_bytes());
    Hash32::parse(&format!("0x{}", hex_encode(&digest))).expect("valid hash")
}

#[async_trait]
impl ChainAdapter for MockChain {
    fn chain_id(&self) -> &ChainId {
        &self.chain_id
    }

    fn kind(&self) -> ChainKind {
        ChainKind::Evm
    }

    async fn verify_identity(&self) -> Result<()> {
        self.guard()
    }

    async fn head(&self) -> Result<BlockRef> {
        self.guard()?;
        let mut s = self.state.lock();
        s.head_calls += 1;
        let b = s.blocks.last().cloned().expect("chain is never empty");
        Ok(BlockRef {
            height: b.height,
            hash: b.hash,
            parent_hash: b.parent_hash,
            tx_count: b.transfers.len() as u32,
        })
    }

    async fn header_at(&self, height: u64) -> Result<Option<BlockRef>> {
        self.guard()?;
        let s = self.state.lock();
        Ok(s.blocks
            .iter()
            .find(|b| b.height == height)
            .map(|b| BlockRef {
                height: b.height,
                hash: b.hash.clone(),
                parent_hash: b.parent_hash.clone(),
                tx_count: b.transfers.len() as u32,
            }))
    }

    async fn header_by_hash(&self, hash: &Hash32) -> Result<Option<BlockRef>> {
        self.guard()?;
        let s = self.state.lock();
        let found = s
            .blocks
            .iter()
            .find(|b| &b.hash == hash)
            .cloned()
            .or_else(|| s.orphaned.get(hash.as_str()).cloned());
        Ok(found.map(|b| BlockRef {
            height: b.height,
            hash: b.hash,
            parent_hash: b.parent_hash,
            tx_count: b.transfers.len() as u32,
        }))
    }

    async fn scan(&self, req: ScanRequest<'_>) -> Result<ScanResult> {
        self.guard()?;
        let s = self.state.lock();
        let mut headers = Vec::new();
        let mut transfers = Vec::new();

        // Only assets configured with deposits enabled are scanned, exactly as
        // the real adapter filters at the node.
        let enabled: Vec<String> = req
            .assets
            .iter()
            .filter(|a| a.deposits_enabled)
            .filter_map(|a| a.contract.as_ref().map(|c| c.storage_key()))
            .collect();

        for b in s
            .blocks
            .iter()
            .filter(|b| b.height >= req.from_height && b.height <= req.to_height)
        {
            headers.push(BlockRef {
                height: b.height,
                hash: b.hash.clone(),
                parent_hash: b.parent_hash.clone(),
                tx_count: b.transfers.len() as u32,
            });
            for t in &b.transfers {
                let contract_ok = match &t.contract {
                    Some(c) => enabled.contains(&c.storage_key()),
                    None => false, // native deposits are out of scope in v0.1
                };
                if !contract_ok {
                    continue;
                }
                if !req.tracked_addresses.contains(&t.to.storage_key()) {
                    continue;
                }
                transfers.push(ObservedTransfer {
                    chain: self.chain_id.clone(),
                    tx_hash: t.tx_hash.clone(),
                    log_index: t.log_index,
                    block_number: b.height,
                    block_hash: b.hash.clone(),
                    from_address: t.from.clone(),
                    to_address: t.to.clone(),
                    amount_raw: t.amount,
                    contract_address: t.contract.clone(),
                });
            }
        }
        Ok(ScanResult { headers, transfers })
    }

    async fn build_withdrawal(&self, intent: &WithdrawalIntent) -> Result<UnsignedTransaction> {
        self.guard()?;
        let (to, value, data) = match &intent.contract {
            Some(c) => (
                c.clone(),
                Amount::ZERO,
                chainrail_chains_evm::abi::encode_transfer_call(
                    &intent.destination,
                    intent.amount,
                )?,
            ),
            None => (intent.destination.clone(), intent.amount, vec![]),
        };
        Ok(UnsignedTransaction::Evm(EvmTransactionRequest {
            chain_id: self.numeric_chain_id,
            nonce: intent.nonce,
            to,
            value,
            data,
            gas_limit: 100_000,
            max_fee_per_gas: Amount::new(2_000_000_000),
            max_priority_fee_per_gas: Amount::new(1_000_000_000),
        }))
    }

    async fn broadcast(&self, signed: &SignedTransaction) -> Result<Hash32> {
        self.guard()?;
        let mut s = self.state.lock();
        s.broadcasts.push(BroadcastRecord {
            tx_hash: signed.tx_hash.clone(),
            raw_len: signed.raw.len(),
        });
        // Default to pending; a test can override with `set_tx_status`.
        s.tx_status
            .entry(signed.tx_hash.as_str().to_string())
            .or_insert(TxStatus::Pending);
        Ok(signed.tx_hash.clone())
    }

    async fn transaction_status(&self, tx_hash: &Hash32, head: u64) -> Result<TxStatus> {
        self.guard()?;
        let s = self.state.lock();
        Ok(match s.tx_status.get(tx_hash.as_str()) {
            Some(TxStatus::Mined {
                block_number,
                block_hash,
                success,
                fee_paid,
                ..
            }) => {
                // Recompute confirmations against the live head, as a real
                // adapter does, so mining more blocks advances confirmations.
                TxStatus::Mined {
                    block_number: *block_number,
                    block_hash: block_hash.clone(),
                    confirmations: chainrail_common::FinalityPolicy::confirmations_for(
                        head,
                        *block_number,
                    ),
                    success: *success,
                    fee_paid: *fee_paid,
                }
            }
            Some(other) => other.clone(),
            None => TxStatus::Unknown,
        })
    }

    async fn nonce_hint(&self, _address: &Address) -> Result<u64> {
        self.guard()?;
        Ok(self.state.lock().next_nonce)
    }

    async fn native_balance(&self, _address: &Address) -> Result<Amount> {
        self.guard()?;
        Ok(Amount::new(10_000_000_000_000_000_000))
    }

    fn is_available(&self) -> bool {
        self.state.lock().fail_with.is_none()
    }
}

/// Mark a broadcast transaction as mined successfully in the given block.
pub fn mined(block_number: u64, fee_paid: i128) -> TxStatus {
    TxStatus::Mined {
        block_number,
        block_hash: block_hash(block_number, "mined"),
        confirmations: 1,
        success: true,
        fee_paid: Amount::new(fee_paid),
    }
}

/// Mark a broadcast transaction as mined but reverted. Gas is still spent.
pub fn reverted(block_number: u64, fee_paid: i128) -> TxStatus {
    TxStatus::Mined {
        block_number,
        block_hash: block_hash(block_number, "reverted"),
        confirmations: 1,
        success: false,
        fee_paid: Amount::new(fee_paid),
    }
}
