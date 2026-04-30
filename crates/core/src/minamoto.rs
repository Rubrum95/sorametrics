//! Minamoto / Iroha 3 (SORA Nexus) domain types.
//!
//! These shapes are the indexer-flat projection of what Torii REST exposes
//! at `/v1/explorer/*`. Where possible we plan to migrate fields to types
//! re-exported from `iroha_data_model` so that upstream breaking changes
//! surface at compile time rather than at runtime parse.
//!
//! Phase 0 ships only the placeholder types needed for the schema design
//! and end-to-end serde testing. Phase 2 (iroha-ingest) will fill in the
//! exact `iroha_data_model::*` mappings.

use crate::chain::{Address, AssetId, BlockHash, BlockHeight, Bytes32};
use crate::time::Timestamp;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

/// One block of the Minamoto chain.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct MinamotoBlock {
    /// Block height (monotonic).
    pub height: BlockHeight,
    /// Block hash (32 bytes).
    pub hash: BlockHash,
    /// Wall-clock timestamp from on-chain.
    pub timestamp: Timestamp,
    /// Number of transactions in the block.
    pub transaction_count: u32,
    /// QC commit certificate hash, if available.
    pub commit_qc: Option<Bytes32>,
}

/// Status of a Minamoto transaction.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MinamotoTxStatus {
    /// Successfully committed.
    Committed,
    /// Rejected by validation.
    Rejected,
}

/// One Minamoto transaction.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct MinamotoTx {
    /// Transaction hash (32 bytes).
    pub hash: Bytes32,
    /// Block where the transaction was committed.
    pub block_height: BlockHeight,
    /// Authority that submitted.
    pub authority: Address,
    /// Outcome.
    pub status: MinamotoTxStatus,
    /// If the transaction was sponsored by another account, the sponsor.
    pub fee_sponsor: Option<Address>,
    /// Wall-clock timestamp.
    pub timestamp: Timestamp,
}

/// Asset registered on Iroha 3.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct MinamotoAsset {
    /// Fully-qualified id (`name#domain.dataspace`).
    pub id: AssetId,
    /// Account that owns this balance row.
    pub owned_by: Address,
    /// Raw quantity. Iroha 3 stores `NUMERIC(78,0)` which fits in `Decimal`.
    pub quantity: Decimal,
    /// Decimal scale (e.g. 18 for XOR bridged from SORA v2). Indexed lazily.
    pub scale: Option<u8>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::DateTime;

    #[test]
    fn tx_status_serializes_snake_case() {
        assert_eq!(
            serde_json::to_string(&MinamotoTxStatus::Committed).unwrap(),
            "\"committed\""
        );
        assert_eq!(
            serde_json::to_string(&MinamotoTxStatus::Rejected).unwrap(),
            "\"rejected\""
        );
    }

    #[test]
    fn block_roundtrip() {
        let block = MinamotoBlock {
            height: BlockHeight(42),
            hash: Bytes32::new([0xab; 32]),
            timestamp: Timestamp::new(DateTime::from_timestamp(1_700_000_000, 0).unwrap()),
            transaction_count: 3,
            commit_qc: None,
        };
        let json = serde_json::to_string(&block).unwrap();
        let back: MinamotoBlock = serde_json::from_str(&json).unwrap();
        assert_eq!(block, back);
    }
}
