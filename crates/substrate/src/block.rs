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
use crate::fees::ExtrinsicFeeFacts;
use crate::price::{PriceError, PriceResolver};
use crate::runtime::sora;
use sorametrics_core::chain::AssetId;
use sorametrics_core::chain::BlockHeight;
use sorametrics_core::time::Timestamp;
use sorametrics_db::sm::{
    insert_bridges_batch, insert_fee_burns_batch, insert_fees_batch, insert_swaps_batch,
    insert_transfers_batch,
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
}

impl BlockDecodeStats {
    /// `true` if the block produced at least one decoder hit.
    pub fn has_any(&self) -> bool {
        self.decoded_swaps
            + self.decoded_transfers
            + self.decoded_bridges
            + self.decoded_fee_burns
            + self.decoded_fees
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
) -> Result<BlockDecodeStats, BlockProcessError> {
    let height = BlockHeight(block.number().into());

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
    let mut stats = BlockDecodeStats::default();
    let mut events_seen: u32 = 0;

    let mut swaps = Vec::new();
    let mut transfers = Vec::new();
    let mut bridges = Vec::new();
    let mut fee_burns = Vec::new();
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

        if let Phase::ApplyExtrinsic(i) = ev.phase() {
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
