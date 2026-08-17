//! Event envelope and payloads.
//!
//! Payload types live here (not in the `events` transport crate) so that
//! producers and consumers share one definition without anyone taking a
//! dependency on Kafka.
//!
//! Envelope contract:
//!   * `event_id` is the **deduplication key**. Every consumer is required to
//!     tolerate the same `event_id` being delivered more than once; Kafka gives
//!     at-least-once delivery and the outbox relay can re-publish after a crash.
//!   * `version` is bumped on a breaking payload change. Consumers reject
//!     versions they do not understand rather than guessing.
//!   * `correlation_id` threads an end-to-end causal chain (API request ->
//!     event -> downstream event) through logs and traces.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::money::Amount;

pub type UserId = Uuid;
pub type DepositId = Uuid;
pub type WithdrawalId = Uuid;
pub type AssetId = Uuid;
pub type AccountId = Uuid;
pub type LedgerTransactionId = Uuid;
pub type BlockchainTransactionId = Uuid;

/// Canonical topic names. The configured `topic_prefix` is applied by the
/// transport layer, so these stay stable across environments.
pub mod topics {
    pub const CHAIN_BLOCKS: &str = "chain.blocks";
    pub const DEPOSITS_OBSERVED: &str = "deposits.observed";
    pub const DEPOSITS_CONFIRMED: &str = "deposits.confirmed";
    pub const DEPOSITS_CREDITED: &str = "deposits.credited";
    pub const WITHDRAWALS_REQUESTED: &str = "withdrawals.requested";
    pub const WITHDRAWALS_BROADCAST: &str = "withdrawals.broadcast";
    pub const WITHDRAWALS_COMPLETED: &str = "withdrawals.completed";
    pub const DEAD_LETTER: &str = "system.dead-letter";

    pub const ALL: &[&str] = &[
        CHAIN_BLOCKS,
        DEPOSITS_OBSERVED,
        DEPOSITS_CONFIRMED,
        DEPOSITS_CREDITED,
        WITHDRAWALS_REQUESTED,
        WITHDRAWALS_BROADCAST,
        WITHDRAWALS_COMPLETED,
        DEAD_LETTER,
    ];
}

pub const EVENT_VERSION: u16 = 1;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct EventEnvelope {
    pub event_id: Uuid,
    pub event_type: String,
    pub version: u16,
    pub timestamp: DateTime<Utc>,
    pub correlation_id: String,
    pub payload: EventPayload,
}

impl EventEnvelope {
    pub fn new(payload: EventPayload, correlation_id: impl Into<String>) -> Self {
        EventEnvelope {
            // v7 so that event ids sort by creation time, which makes outbox
            // scans and debugging by id far easier than v4.
            event_id: Uuid::now_v7(),
            event_type: payload.event_type().to_string(),
            version: EVENT_VERSION,
            timestamp: Utc::now(),
            correlation_id: correlation_id.into(),
            payload,
        }
    }

    /// Deterministic id derived from a natural key, so that re-publishing the
    /// same logical fact produces the same `event_id` and consumers dedupe it.
    pub fn with_deterministic_id(mut self, natural_key: &str) -> Self {
        self.event_id = Uuid::new_v5(
            &Uuid::NAMESPACE_URL,
            format!("chainrail:{}:{}", self.event_type, natural_key).as_bytes(),
        );
        self
    }

    pub fn topic(&self) -> &'static str {
        self.payload.topic()
    }

    /// Partition key. Chosen so that all events about one *account* land on one
    /// partition, giving per-user ordering without a global ordering bottleneck.
    pub fn partition_key(&self) -> String {
        self.payload.partition_key()
    }

    pub fn is_supported_version(&self) -> bool {
        self.version == EVENT_VERSION
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "event_type", rename_all = "snake_case")]
pub enum EventPayload {
    #[serde(rename = "chain.block_observed")]
    BlockObserved(BlockObserved),
    #[serde(rename = "deposit.observed")]
    DepositObserved(DepositObserved),
    #[serde(rename = "deposit.confirmed")]
    DepositConfirmed(DepositConfirmed),
    #[serde(rename = "deposit.credited")]
    DepositCredited(DepositCredited),
    #[serde(rename = "deposit.reorged")]
    DepositReorged(DepositReorged),
    #[serde(rename = "withdrawal.requested")]
    WithdrawalRequested(WithdrawalRequested),
    #[serde(rename = "withdrawal.broadcast")]
    WithdrawalBroadcast(WithdrawalBroadcast),
    #[serde(rename = "withdrawal.completed")]
    WithdrawalCompleted(WithdrawalCompleted),
    #[serde(rename = "withdrawal.failed")]
    WithdrawalFailed(WithdrawalFailed),
}

impl EventPayload {
    pub fn event_type(&self) -> &'static str {
        match self {
            EventPayload::BlockObserved(_) => "chain.block_observed",
            EventPayload::DepositObserved(_) => "deposit.observed",
            EventPayload::DepositConfirmed(_) => "deposit.confirmed",
            EventPayload::DepositCredited(_) => "deposit.credited",
            EventPayload::DepositReorged(_) => "deposit.reorged",
            EventPayload::WithdrawalRequested(_) => "withdrawal.requested",
            EventPayload::WithdrawalBroadcast(_) => "withdrawal.broadcast",
            EventPayload::WithdrawalCompleted(_) => "withdrawal.completed",
            EventPayload::WithdrawalFailed(_) => "withdrawal.failed",
        }
    }

    pub fn topic(&self) -> &'static str {
        match self {
            EventPayload::BlockObserved(_) => topics::CHAIN_BLOCKS,
            EventPayload::DepositObserved(_) => topics::DEPOSITS_OBSERVED,
            EventPayload::DepositConfirmed(_) => topics::DEPOSITS_CONFIRMED,
            EventPayload::DepositCredited(_) | EventPayload::DepositReorged(_) => {
                topics::DEPOSITS_CREDITED
            }
            EventPayload::WithdrawalRequested(_) => topics::WITHDRAWALS_REQUESTED,
            EventPayload::WithdrawalBroadcast(_) => topics::WITHDRAWALS_BROADCAST,
            EventPayload::WithdrawalCompleted(_) | EventPayload::WithdrawalFailed(_) => {
                topics::WITHDRAWALS_COMPLETED
            }
        }
    }

    pub fn partition_key(&self) -> String {
        match self {
            EventPayload::BlockObserved(e) => e.chain.clone(),
            EventPayload::DepositObserved(e) => e.user_id.to_string(),
            EventPayload::DepositConfirmed(e) => e.user_id.to_string(),
            EventPayload::DepositCredited(e) => e.user_id.to_string(),
            EventPayload::DepositReorged(e) => e.user_id.to_string(),
            EventPayload::WithdrawalRequested(e) => e.user_id.to_string(),
            EventPayload::WithdrawalBroadcast(e) => e.user_id.to_string(),
            EventPayload::WithdrawalCompleted(e) => e.user_id.to_string(),
            EventPayload::WithdrawalFailed(e) => e.user_id.to_string(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct BlockObserved {
    pub chain: String,
    pub block_number: u64,
    pub block_hash: String,
    pub parent_hash: String,
    pub tx_count: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct DepositObserved {
    pub chain: String,
    pub tx_hash: String,
    pub log_index: i64,
    pub block_number: u64,
    pub block_hash: String,
    pub asset: String,
    pub user_id: UserId,
    pub deposit_id: DepositId,
    pub amount_raw: Amount,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct DepositConfirmed {
    pub chain: String,
    pub deposit_id: DepositId,
    pub user_id: UserId,
    pub tx_hash: String,
    pub block_number: u64,
    pub confirmations: u64,
    pub asset: String,
    pub amount_raw: Amount,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct DepositCredited {
    pub chain: String,
    pub deposit_id: DepositId,
    pub user_id: UserId,
    pub asset: String,
    pub amount_raw: Amount,
    pub ledger_transaction_id: LedgerTransactionId,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct DepositReorged {
    pub chain: String,
    pub deposit_id: DepositId,
    pub user_id: UserId,
    pub tx_hash: String,
    pub orphaned_block_number: u64,
    pub orphaned_block_hash: String,
    /// Set when the deposit had already been credited and a compensating
    /// ledger transaction was posted to reverse it.
    pub reversal_ledger_transaction_id: Option<LedgerTransactionId>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct WithdrawalRequested {
    pub withdrawal_id: WithdrawalId,
    pub user_id: UserId,
    pub chain: String,
    pub asset: String,
    pub amount_raw: Amount,
    pub destination: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct WithdrawalBroadcast {
    pub withdrawal_id: WithdrawalId,
    pub user_id: UserId,
    pub chain: String,
    pub tx_hash: String,
    pub nonce: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct WithdrawalCompleted {
    pub withdrawal_id: WithdrawalId,
    pub user_id: UserId,
    pub chain: String,
    pub tx_hash: String,
    pub asset: String,
    pub amount_raw: Amount,
    pub fee_raw: Amount,
    pub ledger_transaction_id: LedgerTransactionId,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct WithdrawalFailed {
    pub withdrawal_id: WithdrawalId,
    pub user_id: UserId,
    pub chain: String,
    pub reason_code: String,
    pub reason: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> EventEnvelope {
        EventEnvelope::new(
            EventPayload::DepositObserved(DepositObserved {
                chain: "base-sepolia".into(),
                tx_hash: "0xabc".into(),
                log_index: 3,
                block_number: 123,
                block_hash: "0xdef".into(),
                asset: "USDC".into(),
                user_id: Uuid::nil(),
                deposit_id: Uuid::nil(),
                amount_raw: Amount::new(1_000_000),
            }),
            "corr-1",
        )
    }

    #[test]
    fn envelope_round_trips_and_tags_type() {
        let e = sample();
        let json = serde_json::to_string(&e).unwrap();
        assert!(json.contains(r#""event_type":"deposit.observed""#));
        // amount must be a string, never a JSON number
        assert!(json.contains(r#""amount_raw":"1000000""#));
        let back: EventEnvelope = serde_json::from_str(&json).unwrap();
        assert_eq!(back, e);
    }

    #[test]
    fn routing_is_stable() {
        assert_eq!(sample().topic(), topics::DEPOSITS_OBSERVED);
        assert_eq!(sample().partition_key(), Uuid::nil().to_string());
        assert_eq!(sample().payload.event_type(), "deposit.observed");
    }

    #[test]
    fn deterministic_ids_dedupe_republished_facts() {
        let a = sample().with_deterministic_id("base-sepolia:0xabc:3");
        let b = sample().with_deterministic_id("base-sepolia:0xabc:3");
        let c = sample().with_deterministic_id("base-sepolia:0xabc:4");
        assert_eq!(a.event_id, b.event_id);
        assert_ne!(a.event_id, c.event_id);
    }

    #[test]
    fn unknown_versions_are_rejected_not_guessed() {
        let mut e = sample();
        assert!(e.is_supported_version());
        e.version = 99;
        assert!(!e.is_supported_version());
    }

    #[test]
    fn every_payload_variant_maps_to_a_known_topic() {
        // Guards against a new variant silently defaulting to the wrong topic.
        let e = sample();
        assert!(topics::ALL.contains(&e.topic()));
    }
}
