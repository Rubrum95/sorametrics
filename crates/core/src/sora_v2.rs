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
    /// Hash of the originating extrinsic (`0x`-hex). `None` for events
    /// emitted outside an extrinsic (Initialization/Finalization) and
    /// for legacy ETL rows that never recorded it.
    pub extrinsic_hash: Option<String>,
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
    /// USD value of the INPUT leg at index time (legacy `in_usd`).
    pub usd_value: Option<BigDecimal>,
    /// USD value of the OUTPUT leg at index time (legacy `out_usd`).
    pub output_usd_value: Option<BigDecimal>,
    /// Wall-clock timestamp from the block's `timestamp.set` inherent.
    pub timestamp: Timestamp,
}

/// A token transfer (`Balances::Transfer` for XOR / `Tokens::Transfer` for
/// every other asset, technical-account legs excluded).
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
    /// Hash of the originating extrinsic (`0x`-hex), if any.
    pub extrinsic_hash: Option<String>,
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
    /// Hash of the originating extrinsic (`0x`-hex), if any.
    pub extrinsic_hash: Option<String>,
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

/// Legacy fee category of an extrinsic (Node `live_fees.type`):
/// decided by the events the extrinsic emitted.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum FeeType {
    /// Emitted a `LiquidityProxy::Exchange`.
    Swap,
    /// Emitted an `EthBridge` / `Bridge` / `Multisig` event.
    Bridge,
    /// Emitted an `Assets`/`Balances` `*Transfer*` event.
    Transfer,
    /// Anything else that paid a fee.
    Other,
}

impl FeeType {
    /// Legacy text label.
    pub const fn label(self) -> &'static str {
        match self {
            Self::Swap => "Swap",
            Self::Bridge => "Bridge",
            Self::Transfer => "Transfer",
            Self::Other => "Other",
        }
    }
}

/// Network fee paid by one extrinsic
/// (`TransactionPayment::TransactionFeePaid`), the Node's `live_fees`
/// row. Idempotency key is `(block_height, extrinsic_id)`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct V2Fee {
    /// Block of the extrinsic.
    pub block_height: BlockHeight,
    /// Extrinsic index within the block.
    pub extrinsic_id: u32,
    /// Category per the extrinsic's events.
    pub fee_type: FeeType,
    /// Account that paid.
    pub payer: Address,
    /// `actual_fee` in raw XOR planck.
    pub amount: BigDecimal,
    /// USD value at index time (XOR price).
    pub usd_value: Option<BigDecimal>,
    /// Wall-clock timestamp from the block's `timestamp.set` inherent.
    pub timestamp: Timestamp,
}

/// Direction of a pool liquidity event.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum LiquidityKind {
    /// `poolXYK.depositLiquidity`.
    Deposit,
    /// `poolXYK.withdrawLiquidity`.
    Withdraw,
}

impl LiquidityKind {
    /// Lowercase label stored in `sm.liquidity_events.kind`.
    pub const fn label(self) -> &'static str {
        match self {
            Self::Deposit => "deposit",
            Self::Withdraw => "withdraw",
        }
    }
}

/// One successful `poolXYK` deposit / withdraw extrinsic (the Node's
/// `live_liquidity_events` row). Idempotency key is
/// `(block_height, extrinsic_id, event_id)` with `event_id = 0`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct V2Liquidity {
    /// Block of the extrinsic.
    pub block_height: BlockHeight,
    /// Extrinsic index within the block.
    pub extrinsic_id: u32,
    /// Extrinsic hash (`0x`-hex).
    pub extrinsic_hash: Option<String>,
    /// Signer.
    pub caller: Address,
    /// Pool base asset (`input_asset_a` / `output_asset_a`).
    pub base_asset: AssetId,
    /// Pool target asset (`input_asset_b` / `output_asset_b`).
    pub target_asset: AssetId,
    /// Base amount moved (raw planck), from the transfer events.
    pub base_amount: BigDecimal,
    /// Target amount moved (raw planck).
    pub target_amount: BigDecimal,
    /// USD value at index time (base × price + target × price).
    pub usd_value: Option<BigDecimal>,
    /// Deposit or withdraw.
    pub kind: LiquidityKind,
    /// Wall-clock timestamp from the block's `timestamp.set` inherent.
    pub timestamp: Timestamp,
}

/// Kind of order-book event (the Node's `live_order_book_events.event_type`).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum OrderBookEventType {
    /// `orderBook.LimitOrderPlaced`.
    Placed,
    /// `orderBook.LimitOrderCanceled`.
    Canceled,
    /// `orderBook.LimitOrderExecuted` (a resting order matched).
    Executed,
    /// `orderBook.LimitOrderFilled` (a resting order fully consumed).
    Filled,
    /// `orderBook.MarketOrderExecuted`.
    Market,
}

impl OrderBookEventType {
    /// Lowercase label stored in `sm.order_book_events.event_type`.
    pub const fn label(self) -> &'static str {
        match self {
            Self::Placed => "placed",
            Self::Canceled => "canceled",
            Self::Executed => "executed",
            Self::Filled => "filled",
            Self::Market => "market",
        }
    }
}

/// Order side; `None` for events without one (canceled / filled).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum OrderSide {
    /// Bid.
    Buy,
    /// Ask.
    Sell,
}

impl OrderSide {
    /// Lowercase label stored in `sm.order_book_events.side`.
    pub const fn label(self) -> &'static str {
        match self {
            Self::Buy => "buy",
            Self::Sell => "sell",
        }
    }
}

/// One `orderBook` pallet event (the Node's `live_order_book_events`
/// row). Idempotency key is `(block_height, extrinsic_id, event_id)`.
/// `price` and `amount` are the chain's 18-decimal fixed-point values
/// already scaled to human units (the Node stores `inner / 1e18`).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct V2OrderBookEvent {
    /// Block of the event.
    pub block_height: BlockHeight,
    /// Extrinsic index within the block.
    pub extrinsic_id: u32,
    /// Event index within the block.
    pub event_id: u32,
    /// Extrinsic hash (`0x`-hex), if the event belongs to one.
    pub extrinsic_hash: Option<String>,
    /// Event kind.
    pub event_type: OrderBookEventType,
    /// Order owner (SS58).
    pub wallet: Address,
    /// Limit order id; `None` for market orders.
    pub order_id: Option<u128>,
    /// Order book base asset.
    pub base_asset: AssetId,
    /// Order book quote asset.
    pub quote_asset: AssetId,
    /// Side, when the event carries one.
    pub side: Option<OrderSide>,
    /// Price in quote per base (human units).
    pub price: Option<BigDecimal>,
    /// Amount in base units (human units).
    pub amount: Option<BigDecimal>,
    /// Amount expressed in the quote asset (human units) — the basis of
    /// `usd_value`: `amount × price` for base-denominated amounts, the
    /// amount itself when the chain reports it in quote.
    pub quote_amount: Option<BigDecimal>,
    /// USD value at index time (`quote_amount × quote price`).
    pub usd_value: Option<BigDecimal>,
    /// Wall-clock timestamp from the block's `timestamp.set` inherent.
    pub timestamp: Timestamp,
}

/// One `xorFee.ValStakingRewardPaid` event (the Node's
/// `sm.val_staking_rewards` row): a VAL payout of `era` / `page` from
/// `validator_stash` to `destination`. Idempotency key is
/// `(era, page, validator_stash, destination, block_height)`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct V2ValStakingReward {
    /// Block of the payout.
    pub block_height: BlockHeight,
    /// Block hash (`0x`-hex).
    pub block_hash: String,
    /// Era paid.
    pub era: u32,
    /// Exposure page paid.
    pub page: u32,
    /// Validator stash (SS58).
    pub validator_stash: Address,
    /// Reward destination (SS58).
    pub destination: Address,
    /// VAL amount (raw planck).
    pub amount: BigDecimal,
    /// Wall-clock timestamp from the block's `timestamp.set` inherent.
    pub timestamp: Timestamp,
}

/// Per-block fee / burn aggregate (the Node's `fee_burns_live` row,
/// `fee_burns_indexer.js`). Amounts are human units (planck / 1e18).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct V2FeeBurnAggregate {
    /// Block.
    pub block_height: BlockHeight,
    /// Block time in unix milliseconds.
    pub ts_millis: i64,
    /// Σ `xorFee.FeeWithdrawn`.
    pub fees_paid_xor: BigDecimal,
    /// Σ `xorFee.ReferrerRewarded`.
    pub ref_paid_xor: BigDecimal,
    /// `max(0, fees × ref/total − ref_paid)`: the referrer share with no referrer.
    pub ref_redirected_xor: BigDecimal,
    /// `fees × xor/total`: XOR burnt directly.
    pub remint_xor_burned: BigDecimal,
    /// VAL withdrawn in a remint block.
    pub remint_val_burned: BigDecimal,
    /// KUSD withdrawn in a remint block.
    pub remint_kusd_burned: BigDecimal,
    /// TBCD withdrawn in a remint block.
    pub remint_tbcd_burned: BigDecimal,
}

impl V2FeeBurnAggregate {
    /// The Node only stores blocks with some activity.
    pub fn has_activity(&self) -> bool {
        use bigdecimal::Zero;
        !(self.fees_paid_xor.is_zero()
            && self.ref_paid_xor.is_zero()
            && self.ref_redirected_xor.is_zero()
            && self.remint_xor_burned.is_zero()
            && self.remint_val_burned.is_zero()
            && self.remint_kusd_burned.is_zero()
            && self.remint_tbcd_burned.is_zero())
    }
}

/// A Polkamarkt market as the Node's `sm.polkamarkt_markets` row
/// (hydrated from `Markets` / `Conditions` storage on `MarketCreated`).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct PmMarket {
    /// Market id.
    pub market_id: u32,
    /// Condition id.
    pub condition_id: u32,
    /// Creator (SS58), `Unknown` when storage was unavailable.
    pub creator: String,
    /// Close block.
    pub close_block: u32,
    /// Collateral asset id (`0x`-hex), `""` when unavailable.
    pub collateral_asset: String,
    /// Seed liquidity (raw).
    pub seed_liquidity: BigDecimal,
    /// `Open | Locked | Resolved | Cancelled`.
    pub status: String,
    /// Question text.
    pub question: Option<String>,
    /// Oracle text.
    pub oracle: Option<String>,
    /// Resolution source text.
    pub resolution_source: Option<String>,
    /// `LegacyAmm | OrderBook | DynamicPariMutuel | MigratedLegacy`.
    pub mechanism: Option<String>,
    /// Block of `MarketCreated`.
    pub block_height: BlockHeight,
    /// Block time in unix milliseconds.
    pub ts_millis: i64,
}

/// One `polkamarkt` event that changes the replica.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PmChange {
    /// `MarketCreated(market_id, seed_liquidity)` — hydrated by the processor.
    MarketCreated {
        /// Market id.
        market_id: u32,
        /// Seed liquidity (raw).
        seed_liquidity: BigDecimal,
    },
    /// `TradeExecuted`.
    Trade {
        /// Market id.
        market_id: u32,
        /// Trader (SS58).
        trader: String,
        /// `Buy | Sell`.
        side: String,
        /// `Yes | No`.
        outcome: String,
        /// Collateral (raw).
        collateral: BigDecimal,
        /// Shares (raw).
        shares: BigDecimal,
        /// Fee (raw).
        fee: BigDecimal,
    },
    /// `MarketLocked` / `MarketResolved` / `MarketCancelled` / `MarketEmergencyCancelled`.
    Status {
        /// Market id.
        market_id: u32,
        /// New status.
        status: String,
        /// `Yes | No` on resolution.
        resolution: Option<String>,
    },
    /// `LegacyMarketMigrated(market_id, status)` → reconcile status + `MigratedLegacy`.
    LegacyMigrated {
        /// Market id.
        market_id: u32,
        /// Final status stamped by the migration.
        status: String,
    },
    /// `MarketClaimed` (`payout`) / `CreatorFeesClaimed` (`creator_fees`).
    Claim {
        /// Market id.
        market_id: u32,
        /// Account (SS58).
        account: String,
        /// `payout | creator_fees`.
        kind: String,
        /// Amount (raw).
        amount: BigDecimal,
    },
    /// `DpmResidualBurned` (`dpm_residual`) / `LegacyMigrationResidualRouted` (`legacy_migration_residual`).
    Burn {
        /// Market id, `None` for the migration residual.
        market_id: Option<u32>,
        /// Kind label.
        kind: String,
        /// Amount (raw).
        amount: BigDecimal,
    },
    /// `XorBuybackSwept(collateral, xor_burned)`.
    Buyback {
        /// KUSD spent (raw).
        kusd_spent: BigDecimal,
        /// XOR burned (raw).
        xor_burned: BigDecimal,
    },
}

/// A decoded `polkamarkt` event with its coordinates.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct V2PolkamarktEvent {
    /// Block.
    pub block_height: BlockHeight,
    /// Event index within the block.
    pub event_id: u32,
    /// Extrinsic hash (`0x`-hex), if any.
    pub extrinsic_hash: Option<String>,
    /// Block time in unix milliseconds.
    pub ts_millis: i64,
    /// The change.
    pub change: PmChange,
}

/// One `preimage.*` event (the Node's `preimage_events` row of
/// `preimage_indexer.js`). Idempotency key is `(block_height, event_index)`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct V2PreimageEvent {
    /// Block.
    pub block_height: BlockHeight,
    /// Event index within the block.
    pub event_index: u32,
    /// Block time in unix milliseconds.
    pub ts_millis: i64,
    /// `Noted | Requested | Cleared | Unnoted`.
    pub method: String,
    /// Preimage hash (`0x`-hex).
    pub hash: String,
    /// Event data as polkadot-js `toJSON()` (`[hash]`).
    pub data: serde_json::Value,
    /// Why a `Cleared` / `Unnoted` happened, when inferable.
    pub reason: Option<String>,
    /// Human detail of `reason`.
    pub reason_detail: Option<String>,
}

/// One extrinsic of a block (the Node's `live_extrinsics` row).
/// Idempotency key is `(block_height, extrinsic_index)`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct V2Extrinsic {
    /// Block of the extrinsic.
    pub block_height: BlockHeight,
    /// Index within the block.
    pub extrinsic_index: u32,
    /// Extrinsic hash (`0x`-hex).
    pub hash: String,
    /// Pallet in polkadot-js camelCase (`liquidityProxy`).
    pub section: String,
    /// Call in polkadot-js camelCase (`transferToSidechain`).
    pub method: String,
    /// Signer (SS58) or `System` for unsigned extrinsics.
    pub signer: String,
    /// `System::ExtrinsicSuccess` seen in the phase.
    pub success: bool,
    /// `pallet.Error: docs` for module errors, else the error label; `""` on success.
    pub error_msg: String,
    /// Decoded call arguments (toHuman-like JSON object).
    pub args: serde_json::Value,
    /// Events of the phase as `[{s, m, d}]` (no success/failed markers).
    pub events: serde_json::Value,
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
    /// Hash of the originating extrinsic (`0x`-hex), if any.
    pub extrinsic_hash: Option<String>,
    /// Direction of the bridge event.
    pub direction: BridgeDirection,
    /// Network label (e.g. "Substrate: Liberland", "Parachain: Karura", "TON").
    pub network: String,
    /// Caller address on SORA v2 side.
    pub caller: Address,
    /// The other side of the bridge, rendered per its kind: SS58 for
    /// Substrate accounts, `0x`-hex for EVM, `workchain:hash` for TON,
    /// `xcm:0x<scale>` for parachain locations. `None` when the event
    /// carries no counterparty (`Unknown`, or a parachain mint without
    /// origin).
    pub counterparty: Option<String>,
    /// Asset bridged.
    pub asset: AssetId,
    /// Amount (raw, post-denomination, arbitrary precision).
    pub amount: BigDecimal,
    /// USD value at index time (amount × DAI price of the asset).
    pub usd_value: Option<BigDecimal>,
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
