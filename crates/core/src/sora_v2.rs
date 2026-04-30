//! SORA v2 (Substrate) domain types.
//!
//! These shapes mirror what the chain emits via `subxt`-decoded events.
//! Field names follow the on-chain pallet conventions (snake_case in JSON
//! is forced via `#[serde(rename_all = "snake_case")]` at the type level).
//!
//! Phase 0 ships placeholders for the most central event types. Each will
//! be filled out in Phase 1 (substrate-ingest) with the exact subxt-decoded
//! shape and corresponding DB schema mapping.

use crate::chain::{Address, AssetId, BlockHash, BlockHeight};
use crate::time::Timestamp;
use bigdecimal::BigDecimal;
use serde::{Deserialize, Serialize};

/// One block of the SORA v2 Substrate chain, indexer-flat shape.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct V2Block {
    /// Block height (monotonic).
    pub height: BlockHeight,
    /// Block hash (32 bytes).
    pub hash: BlockHash,
    /// Wall-clock timestamp at block production.
    pub timestamp: Timestamp,
    /// Number of extrinsics in the block.
    pub extrinsic_count: u32,
}

/// A DEX swap (`liquidityProxy.Exchange` event flattened).
///
/// Idempotency key is `(block_height, extrinsic_id, event_id)`. `event_id`
/// is the position of the event within the block's event list (not within
/// a single extrinsic) — that is the index returned by subxt's
/// `EventDetails::index()`.
///
/// All amount-shaped fields use [`BigDecimal`] (arbitrary precision)
/// because raw on-chain SORA balances exceed `rust_decimal::Decimal::MAX`
/// (~7.9e28) for pre-denomination historical blocks. We standardize
/// even `usd_value` on `BigDecimal` for consistency at the storage
/// layer (PostgreSQL `NUMERIC(38,6)`).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct V2Swap {
    /// Block where the swap occurred.
    pub block_height: BlockHeight,
    /// Extrinsic id within the block (0-based).
    pub extrinsic_id: u32,
    /// Event index within the block (0-based, monotonic across all events).
    pub event_id: u32,
    /// Caller address.
    pub caller: Address,
    /// Input asset id.
    pub input_asset: AssetId,
    /// Input asset amount (raw, post-denomination, arbitrary precision).
    pub input_amount: BigDecimal,
    /// Output asset id.
    pub output_asset: AssetId,
    /// Output asset amount (raw, post-denomination, arbitrary precision).
    pub output_amount: BigDecimal,
    /// USD value of the swap at execution time, derived via DAI ratio.
    pub usd_value: Option<BigDecimal>,
    /// Wall-clock timestamp from the block's `timestamp.set` inherent.
    pub timestamp: Timestamp,
}

/// A token transfer (`assets.Transfer` event flattened).
///
/// Idempotency key is `(block_height, extrinsic_id, event_id)`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct V2Transfer {
    /// Block where the transfer occurred.
    pub block_height: BlockHeight,
    /// Extrinsic id within the block (0-based).
    pub extrinsic_id: u32,
    /// Event index within the block (0-based, monotonic across all events).
    pub event_id: u32,
    /// Sender address.
    pub from: Address,
    /// Recipient address.
    pub to: Address,
    /// Asset transferred.
    pub asset: AssetId,
    /// Amount (raw, post-denomination, arbitrary precision).
    pub amount: BigDecimal,
    /// USD value at transfer time.
    pub usd_value: Option<BigDecimal>,
    /// Wall-clock timestamp.
    pub timestamp: Timestamp,
}

/// Direction of a bridge event relative to SORA v2.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BridgeDirection {
    /// Asset was burned/locked on SORA v2 (going out).
    Out,
    /// Asset was minted/released on SORA v2 (coming in).
    In,
}

/// Discriminator for the kind of fee event captured in [`V2FeeBurn`].
///
/// Both `FeeWithdrawn` and `ReferrerRewarded` are emitted by the SORA
/// `XorFee` pallet; we keep them in one logical row type because they
/// share the same `(block, ext, event)` PK pattern and downstream
/// analytics (fee flow, network burn volume) want to see them together.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FeeBurnKind {
    /// `XorFee::FeeWithdrawn` — XOR amount burned from a payer.
    FeeWithdrawn,
    /// `XorFee::ReferrerRewarded` — referrer share of a fee was paid out.
    ReferrerRewarded,
}

/// One fee-related event from the `XorFee` pallet.
///
/// Idempotency key is `(block_height, extrinsic_id, event_id)`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct V2FeeBurn {
    /// Block where the event occurred.
    pub block_height: BlockHeight,
    /// Extrinsic id within the block (0-based).
    pub extrinsic_id: u32,
    /// Event index within the block (0-based).
    pub event_id: u32,
    /// Discriminator: which `XorFee::*` variant produced this row.
    pub kind: FeeBurnKind,
    /// `FeeWithdrawn`: the account whose XOR was burned.
    /// `ReferrerRewarded`: the referee (the user who triggered the fee).
    pub payer: Address,
    /// Only `Some` when [`kind`] is `ReferrerRewarded`.
    /// Always `None` for `FeeWithdrawn` (no referrer involved).
    pub referrer: Option<Address>,
    /// Amount burned / rewarded (raw on-chain XOR planck, BigDecimal).
    pub amount: BigDecimal,
    /// Wall-clock timestamp from the block's `timestamp.set` inherent.
    pub timestamp: Timestamp,
}

/// Bridge transfer (Hashi v2: substrate / parachain / TON).
///
/// Idempotency key is `(block_height, extrinsic_id, event_id)`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct V2Bridge {
    /// Block where the bridge event occurred.
    pub block_height: BlockHeight,
    /// Extrinsic id within the block (0-based).
    pub extrinsic_id: u32,
    /// Event index within the block (0-based, monotonic across all events).
    pub event_id: u32,
    /// Direction of the bridge event.
    pub direction: BridgeDirection,
    /// Network label (e.g. "Substrate: Liberland", "Parachain: Karura", "TON").
    pub network: String,
    /// Caller address on SORA v2 side.
    pub caller: Address,
    /// Asset bridged.
    pub asset: AssetId,
    /// Amount (raw, post-denomination, arbitrary precision).
    pub amount: BigDecimal,
    /// Wall-clock timestamp.
    pub timestamp: Timestamp,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::chain::Bytes32;
    use chrono::DateTime;

    #[test]
    fn v2_block_serializes_snake_case() {
        let block = V2Block {
            height: BlockHeight(1234),
            hash: Bytes32::new([0x01; 32]),
            timestamp: Timestamp::new(DateTime::from_timestamp(1_700_000_000, 0).unwrap()),
            extrinsic_count: 5,
        };
        let json = serde_json::to_string(&block).unwrap();
        assert!(json.contains("\"extrinsic_count\":5"));
        assert!(json.contains("\"height\":1234"));
    }

    #[test]
    fn bridge_direction_lowercase() {
        assert_eq!(
            serde_json::to_string(&BridgeDirection::In).unwrap(),
            "\"in\""
        );
        assert_eq!(
            serde_json::to_string(&BridgeDirection::Out).unwrap(),
            "\"out\""
        );
    }
}
