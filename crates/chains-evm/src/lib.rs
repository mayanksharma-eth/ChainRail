//! EVM chain support: ABI codec, typed JSON-RPC client, and the
//! [`ChainAdapter`] implementation the rest of the system programs against.

pub mod abi;
pub mod adapter;
pub mod client;

pub use adapter::{
    build_adapters, BlockRef, ChainAdapter, EvmAdapter, ScanRequest, ScanResult, TrackedAsset,
    TxStatus, WithdrawalIntent,
};
pub use client::{EvmBlockHeader, EvmClient, EvmLog, EvmReceipt, FeeEstimate};
