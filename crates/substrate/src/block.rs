//! Per-block event processor: takes a single subxt `Block`, runs every
//! decoder, persists matches to `sm.live_*`. Reused by:
//!
//! - the live finalized subscriber in `sorametrics-ingest`
//! - the `decode-block` operation in `sorametrics-ops`
//!
//! Both call into [`decode_block_events`]; the difference is just where
//! the block came from (subscription vs `client.blocks().at(height)`).
//!
//! The processor is **stateless** with respect to cursors: it doesn't
//! touch `sm.indexer_state`. Cursor advancement is the caller's
//! responsibility — the live subscriber updates it on every successful
//! block; the ops CLI does not (a one-off decode shouldn't move the
//! cursor and accidentally break the live indexer's resume).

use crate::decoder::{
    decode_bridge, decode_fee_burn, decode_swap, decode_transfer, timestamp_from_millis,
    EventCoords,
};
use crate::eth_bridge::{
    decode_eth_incoming, decode_eth_outgoing, eth_incoming_hash, outgoing_calls,
};
use crate::extrinsics::{extrinsic_row, ExtrinsicFacts};
use crate::fee_burns_agg::{aggregate, is_remint, weights_for, WithdrawnByAsset};
use crate::fees::ExtrinsicFeeFacts;
use crate::governance::{preimage_event, PreimageBlockFacts};
use crate::liquidity::{liquidity_calls, LiquidityFacts};
use crate::order_book::decode_order_book;
use crate::polkamarkt::{decode_polkamarkt, hydrate_market};
use crate::price::{PriceError, PriceResolver};
use crate::runtime::sora;
use crate::val_staking::decode_val_staking_reward;
use bigdecimal::BigDecimal;
use num_bigint::BigInt;
use sorametrics_core::chain::AssetId;
use sorametrics_core::chain::BlockHeight;
use sorametrics_core::sora_v2::PmChange;
use sorametrics_core::time::Timestamp;
use sorametrics_db::sm::{
    insert_bridges_batch, insert_extrinsics_batch, insert_fee_burns_batch, insert_fees_batch,
    insert_liquidity_batch, insert_order_book_batch, insert_preimage_events, insert_swaps_batch,
    insert_transfers_batch, insert_val_staking_rewards_batch, pm_apply_event, pm_insert_market,
    upsert_fee_burns_aggregate,
};
use sqlx::PgPool;
use std::collections::BTreeMap;
use subxt::blocks::Block;
use subxt::events::Phase;
use subxt::{OnlineClient, SubstrateConfig};
use thiserror::Error;
use tracing::warn;

/// XOR asset id — the currency every network fee is paid in.
const XOR_ASSET_ID: &str = "0x0200000000000000000000000000000000000000000000000000000000000000";

/// Per-block tally of decoder hits.
///
/// `decoded_*` counts events the decoder claimed; `inserted_*` counts
/// rows that were actually new in the DB (the rest were skipped due to
/// the `ON CONFLICT DO NOTHING` UPSERT). Re-running the processor on
/// the same block is idempotent: `decoded` stays the same, `inserted`
/// goes to zero.
#[derive(Default, Debug, Clone, Copy, PartialEq, Eq)]
pub struct BlockDecodeStats {
    /// Total events seen in the block.
    pub events: u32,
    /// Number of `LiquidityProxy::Exchange` events successfully decoded.
    pub decoded_swaps: u32,
    /// Number of swaps that resulted in a new row.
    pub inserted_swaps: u32,
    /// Number of `Balances::Transfer` / `Tokens::Transfer` events decoded
    /// (technical-account legs excluded).
    pub decoded_transfers: u32,
    /// Number of transfers that resulted in a new row.
    pub inserted_transfers: u32,
    /// Number of bridge events decoded (Hashi v2 pallets + classic
    /// EthBridge outgoing/incoming transfers).
    pub decoded_bridges: u32,
    /// Number of bridges that resulted in a new row.
    pub inserted_bridges: u32,
    /// Number of XorFee events (FeeWithdrawn / ReferrerRewarded) decoded.
    pub decoded_fee_burns: u32,
    /// Number of fee-burn rows that resulted in a new row.
    pub inserted_fee_burns: u32,
    /// Number of extrinsics that paid a network fee (`TransactionFeePaid`).
    pub decoded_fees: u32,
    /// Number of fee rows that were new.
    pub inserted_fees: u32,
    /// Number of successful poolXYK deposit/withdraw extrinsics decoded.
    pub decoded_liquidity: u32,
    /// Number of liquidity rows that were new.
    pub inserted_liquidity: u32,
    /// Number of `orderBook` events decoded.
    pub decoded_order_book: u32,
    /// Number of order book rows that were new.
    pub inserted_order_book: u32,
    /// Number of `xorFee.ValStakingRewardPaid` events decoded.
    pub decoded_val_rewards: u32,
    /// Number of VAL payout rows that were new.
    pub inserted_val_rewards: u32,
    /// 1 when the block produced a fee/burn aggregate row.
    pub fee_burn_aggregates: u32,
    /// Number of `polkamarkt` events applied to the replica.
    pub polkamarkt_events: u32,
    /// Number of new `preimage.*` event rows.
    pub preimage_events: u32,
    /// Extrinsics in the block (every one produces a row).
    pub decoded_extrinsics: u32,
    /// Extrinsic rows that were new.
    pub inserted_extrinsics: u32,
}

impl BlockDecodeStats {
    /// `true` if the block produced at least one decoder hit.
    pub fn has_any(&self) -> bool {
        self.decoded_swaps
            + self.decoded_transfers
            + self.decoded_bridges
            + self.decoded_fee_burns
            + self.decoded_fees
            + self.decoded_liquidity
            + self.decoded_order_book
            + self.decoded_val_rewards
            > 0
    }
}

/// Errors surfaced by [`decode_block_events`].
///
/// The subxt error is boxed for the same reason as in
/// `decoder::DecodeError`: `subxt::Error` is large enough to trip
/// `clippy::result_large_err` on every sync function returning this.
#[derive(Debug, Error)]
pub enum BlockProcessError {
    /// Subxt-level error fetching block components or events.
    #[error("subxt: {0}")]
    Subxt(#[source] Box<subxt::Error>),

    /// DB error during insert.
    #[error("db: {0}")]
    Db(#[from] sorametrics_db::DbError),

    /// Price lookup failed (RPC or DB) while valuing an event.
    #[error("price: {0}")]
    Price(#[from] PriceError),
}

impl From<subxt::Error> for BlockProcessError {
    fn from(e: subxt::Error) -> Self {
        Self::Subxt(Box::new(e))
    }
}

/// Process a single finalized block: fetch its timestamp + events,
/// run all decoders in priority order, persist matches.
///
/// Three phases: decode the whole block into per-type vectors, value
/// swaps / transfers / bridges in USD through `prices`, then land each
/// family in ONE batched UPSERT (one round-trip per type per block
/// instead of one per event — the difference dominates backfill
/// throughput).
///
/// Returns per-decoder counters. Decoder-internal failures (one bad
/// event) are logged at `warn` and skipped, NOT bubbled up — a single
/// malformed event must not stop the whole block. A price failure IS
/// bubbled up: it means the RPC or the DB is down, and inserting rows
/// with a silently missing `usd_value` would be indistinguishable from
/// "no price exists".
pub async fn decode_block_events(
    block: &Block<SubstrateConfig, OnlineClient<SubstrateConfig>>,
    db: &PgPool,
    prices: &PriceResolver,
    metadata: &subxt::Metadata,
    spec_version: u32,
    client: &OnlineClient<SubstrateConfig>,
) -> Result<BlockDecodeStats, BlockProcessError> {
    let height = BlockHeight(block.number().into());
    let block_hash: [u8; 32] = block.hash().0;

    let extrinsics = block.extrinsics().await?;
    let block_timestamp = timestamp_from_inherent(&extrinsics, height)?;
    let events = block.events().await?;
    let extrinsics_len = extrinsics.len() as u32;
    // Extrinsic hashes by in-block index, for `ApplyExtrinsic(i)` events.
    // Collected once from the already-fetched body — no extra RPC.
    let extrinsic_hashes: Vec<[u8; 32]> = extrinsics.iter().map(|ext| ext.hash().0).collect();
    // Classic ETH bridge outgoing transfers are read from the call args
    // of `transfer_to_sidechain`, keyed by extrinsic index.
    let eth_outgoing = outgoing_calls(&extrinsics);
    // poolXYK deposit / withdraw calls, keyed by extrinsic index; their
    // amounts come from the transfer events of the same phase.
    let liq_calls = liquidity_calls(&extrinsics);
    let mut liq_facts: BTreeMap<u32, (LiquidityFacts, EventCoords)> = BTreeMap::new();
    // Every extrinsic gets a row; its facts come from the phase's events.
    let mut ext_facts: BTreeMap<u32, ExtrinsicFacts> = BTreeMap::new();
    let mut stats = BlockDecodeStats::default();
    let mut events_seen: u32 = 0;

    let mut swaps = Vec::new();
    let mut transfers = Vec::new();
    let mut bridges = Vec::new();
    let mut fee_burns = Vec::new();
    let mut order_book = Vec::new();
    let mut val_rewards = Vec::new();
    let mut polkamarkt = Vec::new();
    let mut preimage_events = Vec::new();
    let mut preimage_facts = PreimageBlockFacts::default();
    preimage_facts.observe_extrinsics(&extrinsics);
    let mut withdrawn = WithdrawnByAsset::default();
    // Per-extrinsic fee facts; every event feeds its extrinsic's entry.
    let mut fee_facts: BTreeMap<u32, ExtrinsicFeeFacts> = BTreeMap::new();

    // Phase 1: decode.
    for ev in events.iter() {
        // events.iter() yields Result<_, subxt_core::Error>; bridge through
        // the top-level subxt::Error for a uniform conversion.
        let ev = ev.map_err(subxt::Error::from)?;

        let extrinsic_hash = match ev.phase() {
            Phase::ApplyExtrinsic(i) => extrinsic_hashes.get(i as usize).copied(),
            Phase::Initialization | Phase::Finalization => None,
        };

        let coords = EventCoords {
            block_height: height,
            block_timestamp,
            extrinsic_id: extrinsic_index_from_phase(ev.phase(), extrinsics_len),
            // The event's own position in the block event list, as
            // reported by subxt — the PK component documented in
            // `core::sora_v2` (not a locally maintained counter, which
            // could drift from it if subxt ever skipped an entry).
            event_id: ev.index(),
            extrinsic_hash,
        };
        events_seen += 1;

        preimage_facts.observe(&ev);
        if let Some(pe) = preimage_event(&ev, height, block_timestamp.0.timestamp_millis()) {
            preimage_events.push(pe);
        }
        if let Err(e) = withdrawn.observe(&ev) {
            warn!(
                error = %e,
                block = height.0,
                event_id = coords.event_id,
                "tokens withdrawn decode failed"
            );
        }

        if let Phase::ApplyExtrinsic(i) = ev.phase() {
            ext_facts.entry(i).or_default().observe(&ev, metadata);
            if liq_calls.contains_key(&i) {
                let entry = liq_facts
                    .entry(i)
                    .or_insert_with(|| (LiquidityFacts::default(), coords));
                if let Err(e) = entry.0.observe(&ev) {
                    warn!(
                        error = %e,
                        block = height.0,
                        event_id = coords.event_id,
                        "liquidity decode failed"
                    );
                }
            }
            if let Err(e) = fee_facts.entry(i).or_default().observe(&ev, coords) {
                warn!(
                    error = %e,
                    block = height.0,
                    event_id = coords.event_id,
                    "fee decode failed"
                );
            }
        }

        // Dispatch in priority order. After a hit we `continue` so we don't
        // run subsequent decoders on the same event (they'd all return
        // `Ok(None)` anyway, but skipping saves the `pallet_name`/`variant_name`
        // string compares).
        match decode_swap(&ev, coords) {
            Ok(Some(swap)) => {
                swaps.push(swap);
                continue;
            }
            Ok(None) => {}
            Err(e) => warn!(
                error = %e,
                block = height.0,
                event_id = coords.event_id,
                "swap decode failed"
            ),
        }

        match decode_transfer(&ev, coords) {
            Ok(Some(transfer)) => {
                transfers.push(transfer);
                continue;
            }
            Ok(None) => {}
            Err(e) => warn!(
                error = %e,
                block = height.0,
                event_id = coords.event_id,
                "transfer decode failed"
            ),
        }

        match decode_bridge(&ev, coords) {
            Ok(Some(bridge)) => {
                bridges.push(bridge);
                continue;
            }
            Ok(None) => {}
            Err(e) => warn!(
                error = %e,
                block = height.0,
                event_id = coords.event_id,
                "bridge decode failed"
            ),
        }

        match decode_eth_outgoing(&ev, coords, &eth_outgoing) {
            Ok(Some(bridge)) => {
                bridges.push(bridge);
                continue;
            }
            Ok(None) => {}
            Err(e) => warn!(
                error = %e,
                block = height.0,
                event_id = coords.event_id,
                "eth bridge outgoing decode failed"
            ),
        }

        match eth_incoming_hash(&ev) {
            Ok(Some(hash)) => {
                // Storage read at this block; a transport failure here is
                // a block failure (the row would otherwise silently vanish).
                if let Some(bridge) = decode_eth_incoming(block, coords, hash).await? {
                    bridges.push(bridge);
                }
                continue;
            }
            Ok(None) => {}
            Err(e) => warn!(
                error = %e,
                block = height.0,
                event_id = coords.event_id,
                "eth bridge incoming decode failed"
            ),
        }

        match decode_order_book(&ev, coords) {
            Ok(Some(row)) => {
                order_book.push(row);
                continue;
            }
            Ok(None) => {}
            Err(e) => warn!(
                error = %e,
                block = height.0,
                event_id = coords.event_id,
                "order book decode failed"
            ),
        }

        match decode_polkamarkt(&ev, coords) {
            Ok(Some(row)) => {
                polkamarkt.push(row);
                continue;
            }
            Ok(None) => {}
            Err(e) => warn!(
                error = %e,
                block = height.0,
                event_id = coords.event_id,
                "polkamarkt decode failed"
            ),
        }

        match decode_val_staking_reward(&ev, coords, &block_hash) {
            Ok(Some(row)) => {
                val_rewards.push(row);
                continue;
            }
            Ok(None) => {}
            Err(e) => warn!(
                error = %e,
                block = height.0,
                event_id = coords.event_id,
                "val staking reward decode failed"
            ),
        }

        match decode_fee_burn(&ev, coords) {
            Ok(Some(fee_burn)) => {
                fee_burns.push(fee_burn);
                continue;
            }
            Ok(None) => {}
            Err(e) => warn!(
                error = %e,
                block = height.0,
                event_id = coords.event_id,
                "fee_burn decode failed"
            ),
        }
    }

    let mut fees: Vec<_> = fee_facts
        .into_values()
        .filter_map(ExtrinsicFeeFacts::into_fee)
        .collect();
    let extrinsic_rows: Vec<_> = extrinsics
        .iter()
        .map(|ext| {
            let i = ext.index();
            let facts = ext_facts.remove(&i).unwrap_or_default();
            let coords = EventCoords {
                block_height: height,
                block_timestamp,
                extrinsic_id: i,
                event_id: 0,
                extrinsic_hash: None,
            };
            extrinsic_row(&ext, facts, coords, metadata)
        })
        .collect();
    let mut liquidity: Vec<_> = liq_facts
        .into_iter()
        .filter_map(|(i, (facts, coords))| {
            let call = liq_calls.get(&i)?;
            let coords = EventCoords {
                extrinsic_id: i,
                event_id: 0,
                ..coords
            };
            facts.into_event(call, coords)
        })
        .collect();

    // Phase 2: USD valuation (swaps: both legs, as the legacy in_usd/out_usd).
    for swap in swaps.iter_mut() {
        swap.usd_value = prices
            .usd_value_at(&swap.input_asset, &swap.input_amount, swap.timestamp)
            .await?;
        swap.output_usd_value = prices
            .usd_value_at(&swap.output_asset, &swap.output_amount, swap.timestamp)
            .await?;
    }
    for transfer in transfers.iter_mut() {
        transfer.usd_value = prices
            .usd_value_at(&transfer.asset, &transfer.amount, transfer.timestamp)
            .await?;
    }
    for bridge in bridges.iter_mut() {
        bridge.usd_value = prices
            .usd_value_at(&bridge.asset, &bridge.amount, bridge.timestamp)
            .await?;
    }
    let xor = AssetId::new(XOR_ASSET_ID);
    for fee in fees.iter_mut() {
        fee.usd_value = prices
            .usd_value_at(&xor, &fee.amount, fee.timestamp)
            .await?;
    }
    for liq in liquidity.iter_mut() {
        // Node: base × price + target × price; a leg without a price
        // contributes nothing, the row still carries the other leg.
        let base = prices
            .usd_value_at(&liq.base_asset, &liq.base_amount, liq.timestamp)
            .await?;
        let target = prices
            .usd_value_at(&liq.target_asset, &liq.target_amount, liq.timestamp)
            .await?;
        liq.usd_value = match (base, target) {
            (None, None) => None,
            (b, t) => Some(b.unwrap_or_default() + t.unwrap_or_default()),
        };
    }

    for row in order_book.iter_mut() {
        // mv_order_book_events: amount × price × quote price. The
        // quote-terms amount is human; scale to planck for the resolver.
        let Some(q) = row.quote_amount.as_ref() else {
            continue;
        };
        let planck = q * BigDecimal::new(
            BigInt::from(1),
            -(prices.decimals_of(&row.quote_asset) as i64),
        );
        row.usd_value = prices
            .usd_value_at(&row.quote_asset, &planck, row.timestamp)
            .await?;
    }

    // Phase 3: one batched upsert per family.
    stats.decoded_swaps = swaps.len() as u32;
    stats.decoded_transfers = transfers.len() as u32;
    stats.decoded_bridges = bridges.len() as u32;
    stats.decoded_fee_burns = fee_burns.len() as u32;
    stats.inserted_swaps = insert_swaps_batch(db, &swaps).await? as u32;
    stats.inserted_transfers = insert_transfers_batch(db, &transfers).await? as u32;
    stats.inserted_bridges = insert_bridges_batch(db, &bridges).await? as u32;
    stats.inserted_fee_burns = insert_fee_burns_batch(db, &fee_burns).await? as u32;
    stats.decoded_fees = fees.len() as u32;
    stats.inserted_fees = insert_fees_batch(db, &fees).await? as u32;
    stats.decoded_liquidity = liquidity.len() as u32;
    stats.inserted_liquidity = insert_liquidity_batch(db, &liquidity).await? as u32;
    stats.decoded_order_book = order_book.len() as u32;
    stats.inserted_order_book = insert_order_book_batch(db, &order_book).await? as u32;
    // Per-block fee/burn aggregate (fee_burns_indexer.js). A remint is a
    // drop of the xorFee buckets vs the parent block; the buckets are
    // read only when the block withdrew a remint asset.
    let remint = if withdrawn.is_empty() {
        false
    } else {
        let now = at_block_buckets(&block.storage()).await?;
        let parent = client.storage().at(block.header().parent_hash);
        let prev = at_block_buckets(&parent).await?;
        is_remint(prev, now)
    };
    let agg = aggregate(
        height,
        block_timestamp.0.timestamp_millis(),
        &fee_burns,
        &withdrawn,
        weights_for(spec_version),
        remint,
    );
    if agg.has_activity() {
        upsert_fee_burns_aggregate(db, &agg).await?;
        stats.fee_burn_aggregates = 1;
    }

    // Polkamarkt replica: MarketCreated is hydrated from storage at this
    // block, the rest applied in event order.
    for ev in &polkamarkt {
        if let PmChange::MarketCreated {
            market_id,
            seed_liquidity,
        } = &ev.change
        {
            let m = hydrate_market(
                client,
                block.hash(),
                *market_id,
                seed_liquidity.clone(),
                height,
                ev.ts_millis,
            )
            .await?;
            pm_insert_market(db, &m).await?;
        } else {
            pm_apply_event(db, ev).await?;
        }
    }
    stats.polkamarkt_events = polkamarkt.len() as u32;

    // Preimage events with the indexer's cleared-reason inference.
    if let Some((reason, detail)) = preimage_facts.reason() {
        for pe in preimage_events.iter_mut() {
            if pe.method == "Cleared" || pe.method == "Unnoted" {
                pe.reason = Some(reason.to_string());
                pe.reason_detail = Some(detail.to_string());
            }
        }
    }
    stats.preimage_events = insert_preimage_events(db, &preimage_events).await? as u32;

    stats.decoded_val_rewards = val_rewards.len() as u32;
    stats.inserted_val_rewards = insert_val_staking_rewards_batch(db, &val_rewards).await? as u32;
    stats.decoded_extrinsics = extrinsic_rows.len() as u32;
    stats.inserted_extrinsics = insert_extrinsics_batch(db, &extrinsic_rows).await? as u32;

    stats.events = events_seen;
    Ok(stats)
}

/// Read the wall-clock timestamp from the block's own `timestamp.set`
/// inherent — no extra RPC round-trip (the extrinsics are already
/// fetched for phase mapping). Every non-genesis Substrate block
/// carries exactly one; its absence is an error, not a default.
///
/// This replaces the earlier `Timestamp::Now` storage fetch, which cost
/// one additional RPC per block — irrelevant live, dominant in backfill.
/// `(xorToVal, xorToBuyBack)` raw at a storage view.
async fn at_block_buckets(
    at: &subxt::storage::Storage<SubstrateConfig, OnlineClient<SubstrateConfig>>,
) -> Result<(u128, u128), subxt::Error> {
    let s = sora::storage();
    Ok((
        at.fetch(&s.xor_fee().xor_to_val()).await?.unwrap_or(0),
        at.fetch(&s.xor_fee().xor_to_buy_back()).await?.unwrap_or(0),
    ))
}

fn timestamp_from_inherent(
    extrinsics: &subxt::blocks::Extrinsics<SubstrateConfig, OnlineClient<SubstrateConfig>>,
    height: BlockHeight,
) -> Result<Timestamp, BlockProcessError> {
    for ext in extrinsics.iter() {
        match ext.as_extrinsic::<sora::timestamp::calls::types::Set>() {
            Ok(Some(set)) => return Ok(timestamp_from_millis(set.now)),
            Ok(None) => continue,
            // A decode failure of an unrelated extrinsic must not mask
            // the timestamp lookup; only fail if we never find `set`.
            Err(_) => continue,
        }
    }
    Err(BlockProcessError::from(subxt::Error::Other(format!(
        "block {height} has no timestamp.set inherent"
    ))))
}

/// Maps a `Phase` to a deterministic extrinsic index.
///
/// `ApplyExtrinsic(i)` → `i`. `Initialization` / `Finalization` events
/// have no associated extrinsic; we map them to `block_extrinsics_len`
/// so the `(block_height, extrinsic_id, event_id)` PK stays unique
/// without colliding with any real extrinsic index.
fn extrinsic_index_from_phase(phase: Phase, block_extrinsics_len: u32) -> u32 {
    match phase {
        Phase::ApplyExtrinsic(i) => i,
        Phase::Finalization | Phase::Initialization => block_extrinsics_len,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extrinsic_index_apply() {
        assert_eq!(extrinsic_index_from_phase(Phase::ApplyExtrinsic(7), 12), 7);
    }

    #[test]
    fn extrinsic_index_initialization_maps_to_len() {
        assert_eq!(extrinsic_index_from_phase(Phase::Initialization, 12), 12);
    }

    #[test]
    fn extrinsic_index_finalization_maps_to_len() {
        assert_eq!(extrinsic_index_from_phase(Phase::Finalization, 12), 12);
    }

    #[test]
    fn block_decode_stats_default_is_empty() {
        let s = BlockDecodeStats::default();
        assert!(!s.has_any());
        assert_eq!(s.events, 0);
    }

    #[test]
    fn block_decode_stats_has_any_only_after_decoded() {
        let s = BlockDecodeStats {
            events: 50,
            ..BlockDecodeStats::default()
        };
        assert!(!s.has_any(), "high event count alone is not 'decoded'");

        let with_transfer = BlockDecodeStats {
            decoded_transfers: 1,
            ..s
        };
        assert!(with_transfer.has_any());
    }
}
