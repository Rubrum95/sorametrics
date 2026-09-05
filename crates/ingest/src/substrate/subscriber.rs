//! Live finalized-block subscriber.
//!
//! Streams finalized blocks via subxt, delegates per-block decode +
//! insert to [`sorametrics_substrate::decode_block_events`], advances
//! the `sm.indexer_state` cursor on success, records errors there on
//! failure. Reconnects with backoff and rotates URLs in the failover
//! list when the stream ends or errors out.

use sorametrics_core::chain::BlockHeight;
use sorametrics_db::sm::{get_cursor, set_cursor, set_error};
use sorametrics_substrate::{decode_block_events, BlockProcessError};
use sqlx::PgPool;
use std::time::Duration;
use subxt::backend::legacy::LegacyRpcMethods;
use subxt::backend::rpc::RpcClient;
use subxt::{OnlineClient, SubstrateConfig};
use thiserror::Error;
use tokio::time::sleep;
use tracing::{debug, info, warn};

/// Stable job name used as the `sm.indexer_state.job_name`.
///
/// Single cursor for the whole live decoder pipeline (swaps, transfers,
/// bridges): they decode from the same finalized stream in lock-step,
/// so a single cursor accurately represents progress for all of them.
const JOB_NAME_LIVE: &str = "substrate_live";

/// Errors surfaced by [`run_decoder_loop`]. The loop reconnects on most
/// of these; only DB errors that prevent cursor advancement are fatal.
#[derive(Debug, Error)]
pub enum SubscriberError {
    /// Underlying subxt error during connect / subscribe / fetch.
    #[error("subxt: {0}")]
    Subxt(#[from] subxt::Error),

    /// DB error during cursor / error update.
    #[error("db: {0}")]
    Db(#[from] sorametrics_db::DbError),

    /// Error processing one block (decoder or insert path).
    #[error("block process: {0}")]
    BlockProcess(#[from] BlockProcessError),
}

/// Connects via subxt to the first reachable URL and runs the
/// decode-and-insert loop until the cancel signal fires.
///
/// Reconnect strategy: on stream end or non-fatal error, sleep
/// `reconnect_backoff` and try the next URL in the list.
pub async fn run_decoder_loop(
    endpoints: Vec<url::Url>,
    db: PgPool,
    reconnect_backoff: Duration,
    mut cancel: tokio::sync::watch::Receiver<bool>,
) -> Result<(), SubscriberError> {
    assert!(
        !endpoints.is_empty(),
        "subscriber called with empty endpoints — guard at config layer"
    );

    let mut endpoint_idx = 0usize;

    loop {
        if *cancel.borrow_and_update() {
            info!("subscriber: cancel signalled");
            return Ok(());
        }

        let url = &endpoints[endpoint_idx];
        info!(endpoint = %url, "subxt connecting");

        match try_subscribe_once(url, &db, &mut cancel).await {
            Ok(()) => {
                info!("subscriber loop exited cleanly (cancel)");
                return Ok(());
            }
            Err(e) => {
                let job_msg = format!("{e}");
                warn!(
                    error = %e,
                    endpoint = %url,
                    "subscriber session ended; will rotate after backoff"
                );
                // Best-effort: record the error in indexer_state so /health
                // surfaces it. A failure here is not fatal to the loop.
                if let Err(db_err) = set_error(&db, JOB_NAME_LIVE, &job_msg).await {
                    warn!(error = %db_err, "could not record error in indexer_state");
                }
            }
        }

        endpoint_idx = (endpoint_idx + 1) % endpoints.len();

        tokio::select! {
            _ = sleep(reconnect_backoff) => {}
            _ = cancel.changed() => {
                if *cancel.borrow_and_update() {
                    return Ok(());
                }
            }
        }
    }
}

/// Given the last processed height (cursor) and the height of an
/// incoming finalized block, returns the inclusive range of missed
/// blocks that must be filled before processing the incoming one.
///
/// `None` when there is no gap: fresh DB (no cursor), contiguous next
/// block, or a replayed/older block (processing those is idempotent and
/// requires no fill).
fn plan_gap(cursor: Option<u64>, incoming: u64) -> Option<(u64, u64)> {
    match cursor {
        Some(last) if incoming > last + 1 => Some((last + 1, incoming - 1)),
        _ => None,
    }
}

/// Sequentially fetch + decode a range of missed blocks.
///
/// Called inline from the subscription loop when [`plan_gap`] detects a
/// hole (blocks finalized while we were disconnected). Sequential on
/// purpose: the live loop must not compete with itself for DB writes,
/// and gaps are normally small (seconds to minutes of outage). Large
/// gaps still complete — the WS stream buffers behind us — and the
/// cursor advances per filled block, so an interrupted fill resumes
/// exactly where it stopped on the next session.
async fn fill_gap(
    client: &OnlineClient<SubstrateConfig>,
    legacy: &LegacyRpcMethods<SubstrateConfig>,
    db: &PgPool,
    from: u64,
    to: u64,
    cancel: &mut tokio::sync::watch::Receiver<bool>,
) -> Result<(), SubscriberError> {
    warn!(
        from,
        to,
        missed = to - from + 1,
        "gap detected, filling missed finalized blocks"
    );

    for height in from..=to {
        if *cancel.borrow_and_update() {
            info!(
                height,
                "gap fill interrupted by cancel; cursor marks resume point"
            );
            return Ok(());
        }

        let height_u32: u32 = height.try_into().map_err(|_| {
            SubscriberError::Subxt(subxt::Error::Other(format!(
                "gap block height {height} does not fit in u32"
            )))
        })?;
        let hash = legacy
            .chain_get_block_hash(Some(height_u32.into()))
            .await?
            .ok_or_else(|| {
                SubscriberError::Subxt(subxt::Error::Other(format!(
                    "no block hash at height {height} during gap fill"
                )))
            })?;
        let block = client.blocks().at(hash).await?;
        let stats = decode_block_events(&block, db).await?;
        set_cursor(db, JOB_NAME_LIVE, BlockHeight(height), "running").await?;

        if stats.has_any() {
            info!(
                height,
                swaps = stats.decoded_swaps,
                transfers = stats.decoded_transfers,
                bridges = stats.decoded_bridges,
                fee_burns = stats.decoded_fee_burns,
                "gap block decoded"
            );
        }
        if (height - from) % 100 == 99 {
            info!(height, to, "gap fill progress");
        }
    }

    info!(from, to, "gap fill complete");
    Ok(())
}

/// One subscription session: connect, subscribe finalized, dispatch to
/// the per-block processor for each new block. Before processing each
/// incoming block, any hole between the persisted cursor and the block
/// is filled via [`fill_gap`] so reconnects never silently skip blocks.
async fn try_subscribe_once(
    url: &url::Url,
    db: &PgPool,
    cancel: &mut tokio::sync::watch::Receiver<bool>,
) -> Result<(), SubscriberError> {
    // Low-level RPC client first: we keep `LegacyRpcMethods` around for
    // height → hash lookups during gap fills (same pattern as ops
    // backfill), then upgrade the same connection to an `OnlineClient`.
    let rpc_client = RpcClient::from_url(url.as_str()).await?;
    let legacy = LegacyRpcMethods::<SubstrateConfig>::new(rpc_client.clone());
    let client = OnlineClient::<SubstrateConfig>::from_rpc_client(rpc_client).await?;
    info!(endpoint = %url, "subxt connected, subscribing finalized blocks");

    let mut blocks = client.blocks().subscribe_finalized().await?;

    // Resume point: the persisted cursor survives reconnects and
    // restarts. In-memory it only moves forward.
    let mut last_processed: Option<u64> = get_cursor(db, JOB_NAME_LIVE).await?.map(|h| h.0);
    if let Some(lp) = last_processed {
        info!(cursor = lp, "resuming from persisted cursor");
    }

    loop {
        tokio::select! {
            maybe_block = blocks.next() => {
                let block = match maybe_block {
                    Some(Ok(b)) => b,
                    Some(Err(e)) => return Err(e.into()),
                    None => return Err(subxt::Error::Other(
                        "finalized blocks stream ended".into(),
                    ).into()),
                };

                let height = BlockHeight(block.number().into());

                if let Some((from, to)) = plan_gap(last_processed, height.0) {
                    fill_gap(&client, &legacy, db, from, to, cancel).await?;
                    if *cancel.borrow_and_update() {
                        return Ok(());
                    }
                }

                let stats = decode_block_events(&block, db).await?;
                // Never move the cursor backwards: a replayed older block
                // (idempotent no-op in the DB) must not regress the resume
                // point.
                if last_processed.is_none_or(|lp| height.0 > lp) {
                    set_cursor(db, JOB_NAME_LIVE, height, "running").await?;
                    last_processed = Some(height.0);
                }

                if stats.has_any() {
                    info!(
                        height = height.0,
                        events = stats.events,
                        swaps = stats.decoded_swaps,
                        transfers = stats.decoded_transfers,
                        bridges = stats.decoded_bridges,
                        fee_burns = stats.decoded_fee_burns,
                        "finalized block decoded"
                    );
                } else {
                    debug!(
                        height = height.0,
                        events = stats.events,
                        "finalized block (no decoded events)"
                    );
                }
            }

            _ = cancel.changed() => {
                if *cancel.borrow_and_update() {
                    return Ok(());
                }
            }
        }
    }
}

/// Stable name of the cursor job — exposed for tests / diagnostics.
#[allow(dead_code)]
pub fn job_name_live() -> &'static str {
    JOB_NAME_LIVE
}

#[cfg(test)]
mod tests {
    use super::plan_gap;

    #[test]
    fn no_cursor_means_no_gap() {
        assert_eq!(plan_gap(None, 500), None);
    }

    #[test]
    fn contiguous_block_means_no_gap() {
        assert_eq!(plan_gap(Some(99), 100), None);
    }

    #[test]
    fn replayed_or_older_block_means_no_gap() {
        assert_eq!(plan_gap(Some(100), 100), None);
        assert_eq!(plan_gap(Some(100), 42), None);
    }

    #[test]
    fn single_missed_block() {
        assert_eq!(plan_gap(Some(100), 102), Some((101, 101)));
    }

    #[test]
    fn multi_block_gap_spans_cursor_to_incoming_exclusive() {
        assert_eq!(plan_gap(Some(100), 200), Some((101, 199)));
    }
}
