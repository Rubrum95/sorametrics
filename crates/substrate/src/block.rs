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
use crate::runtime::sora;
use sorametrics_core::chain::BlockHeight;
use sorametrics_core::time::Timestamp;
use sorametrics_db::sm::{
    insert_bridge, insert_fee_burn, insert_swap, insert_transfer, UpsertOutcome,
};
use sqlx::PgPool;
use subxt::blocks::Block;
use subxt::events::Phase;
use subxt::{OnlineClient, SubstrateConfig};
use thiserror::Error;
use tracing::warn;

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
    /// Number of `Assets::Transfer` events successfully decoded.
    pub decoded_transfers: u32,
    /// Number of transfers that resulted in a new row.
    pub inserted_transfers: u32,
    /// Number of bridge events (any of the 3 Hashi v2 pallets) decoded.
    pub decoded_bridges: u32,
    /// Number of bridges that resulted in a new row.
    pub inserted_bridges: u32,
    /// Number of XorFee events (FeeWithdrawn / ReferrerRewarded) decoded.
    pub decoded_fee_burns: u32,
    /// Number of fee-burn rows that resulted in a new row.
    pub inserted_fee_burns: u32,
}

impl BlockDecodeStats {
    /// `true` if the block produced at least one decoder hit.
    pub fn has_any(&self) -> bool {
        self.decoded_swaps + self.decoded_transfers + self.decoded_bridges + self.decoded_fee_burns
            > 0
    }
}

/// Errors surfaced by [`decode_block_events`].
#[derive(Debug, Error)]
pub enum BlockProcessError {
    /// Subxt-level error fetching block components or events.
    #[error("subxt: {0}")]
    Subxt(#[from] subxt::Error),

    /// DB error during insert.
    #[error("db: {0}")]
    Db(#[from] sorametrics_db::DbError),
}

/// Process a single finalized block: fetch its timestamp + events,
/// run all decoders in priority order, persist matches.
///
/// Returns per-decoder counters. Decoder-internal failures (one bad
/// event) are logged at `warn` and skipped, NOT bubbled up — a single
/// malformed event must not stop the whole block.
pub async fn decode_block_events(
    client: &OnlineClient<SubstrateConfig>,
    block: &Block<SubstrateConfig, OnlineClient<SubstrateConfig>>,
    db: &PgPool,
) -> Result<BlockDecodeStats, BlockProcessError> {
    let height = BlockHeight(block.number().into());
    let block_timestamp = fetch_block_timestamp(client, block).await?;

    let extrinsics = block.extrinsics().await?;
    let events = block.events().await?;
    let extrinsics_len = extrinsics.len() as u32;
    let mut stats = BlockDecodeStats::default();
    let mut event_id: u32 = 0;

    for ev in events.iter() {
        // events.iter() yields Result<_, subxt_core::Error>; bridge through
        // the top-level subxt::Error for a uniform conversion.
        let ev = ev.map_err(subxt::Error::from)?;

        let coords = EventCoords {
            block_height: height,
            block_timestamp,
            extrinsic_id: extrinsic_index_from_phase(ev.phase(), extrinsics_len),
            event_id,
        };
        event_id += 1;

        // Dispatch in priority order. After a hit we `continue` so we don't
        // run subsequent decoders on the same event (they'd all return
        // `Ok(None)` anyway, but skipping saves the `pallet_name`/`variant_name`
        // string compares).
        match decode_swap(&ev, coords) {
            Ok(Some(swap)) => {
                stats.decoded_swaps += 1;
                if matches!(insert_swap(db, &swap).await?, UpsertOutcome::Inserted) {
                    stats.inserted_swaps += 1;
                }
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
                stats.decoded_transfers += 1;
                if matches!(
                    insert_transfer(db, &transfer).await?,
                    UpsertOutcome::Inserted
                ) {
                    stats.inserted_transfers += 1;
                }
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
                stats.decoded_bridges += 1;
                if matches!(insert_bridge(db, &bridge).await?, UpsertOutcome::Inserted) {
                    stats.inserted_bridges += 1;
                }
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

        match decode_fee_burn(&ev, coords) {
            Ok(Some(fee_burn)) => {
                stats.decoded_fee_burns += 1;
                if matches!(
                    insert_fee_burn(db, &fee_burn).await?,
                    UpsertOutcome::Inserted
                ) {
                    stats.inserted_fee_burns += 1;
                }
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

    stats.events = event_id;
    Ok(stats)
}

/// Read the wall-clock timestamp from `Timestamp::Now` storage at the
/// given block. Returns Unix epoch (1970-01-01T00:00:00Z) if absent
/// (which would only happen on a chain that has not yet executed a
/// `timestamp.set` extrinsic — i.e., genesis-only).
async fn fetch_block_timestamp(
    client: &OnlineClient<SubstrateConfig>,
    block: &Block<SubstrateConfig, OnlineClient<SubstrateConfig>>,
) -> Result<Timestamp, BlockProcessError> {
    let now_query = sora::storage().timestamp().now();
    let now_ms: u64 = client
        .storage()
        .at(block.hash())
        .fetch(&now_query)
        .await?
        .unwrap_or(0);
    Ok(timestamp_from_millis(now_ms))
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
