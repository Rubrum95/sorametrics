//! Live finalized-block subscriber.
//!
//! Streams finalized blocks via subxt, delegates per-block decode +
//! insert to [`sorametrics_substrate::decode_block_events`], advances
//! the `sm.indexer_state` cursor on success, records errors there on
//! failure. Reconnects with backoff and rotates URLs in the failover
//! list when the stream ends or errors out.

use sorametrics_core::chain::BlockHeight;
use sorametrics_db::sm::{get_cursor, set_cursor, set_error};
use sorametrics_substrate::{decode_block_events, BlockProcessError, PriceError, PriceResolver};
use sqlx::PgPool;
use std::time::Duration;
use subxt::backend::legacy::LegacyRpcMethods;
use subxt::backend::rpc::RpcClient;
use subxt::{OnlineClient, SubstrateConfig};
use thiserror::Error;
use tokio::time::sleep;
use tracing::{info, warn};

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

    /// Price resolver could not be built (registry load failed).
    #[error("price resolver: {0}")]
    Price(#[from] PriceError),
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
    gap_concurrency: usize,
    price_archive_rpc: Option<url::Url>,
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

        match try_subscribe_once(
            url,
            &db,
            gap_concurrency,
            price_archive_rpc.as_ref(),
            &mut cancel,
        )
        .await
        {
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

/// Fetch + decode a range of missed blocks, `concurrency` at a time.
///
/// Called inline from the subscription loop when [`plan_gap`] detects a
/// hole (blocks finalized while we were disconnected). Blocks are
/// processed in chunks of `concurrency`; the cursor advances only after
/// a whole chunk succeeded, so an interrupted fill resumes from that
/// chunk's first height (re-decoding is idempotent). The WS stream
/// buffers behind the fill, so large gaps still complete in order.
/// One subscription session's handles, shared by the gap workers.
struct Session<'a> {
    client: &'a OnlineClient<SubstrateConfig>,
    rpc: &'a RpcClient,
    db: &'a PgPool,
    prices: &'a PriceResolver,
}

async fn fill_gap(
    session: &Session<'_>,
    from: u64,
    to: u64,
    concurrency: usize,
    cancel: &mut tokio::sync::watch::Receiver<bool>,
) -> Result<(), SubscriberError> {
    let Session {
        client,
        rpc,
        db,
        prices,
    } = *session;
    warn!(
        from,
        to,
        missed = to - from + 1,
        concurrency,
        "gap detected, filling missed finalized blocks"
    );
    let concurrency = concurrency.max(1) as u64;
    let started = std::time::Instant::now();
    let mut next = from;
    while next <= to {
        if *cancel.borrow_and_update() {
            info!(
                height = next,
                "gap fill interrupted by cancel; cursor marks resume point"
            );
            return Ok(());
        }
        let end = (next + concurrency - 1).min(to);
        let mut tasks = tokio::task::JoinSet::new();
        for height in next..=end {
            let client = client.clone();
            let legacy = LegacyRpcMethods::<SubstrateConfig>::new(rpc.clone());
            let db = db.clone();
            let prices = prices.clone();
            tasks.spawn(async move {
                let stats = fill_one(&client, &legacy, &db, &prices, height).await?;
                Ok::<_, SubscriberError>((height, stats))
            });
        }
        let mut results = Vec::with_capacity((end - next + 1) as usize);
        while let Some(joined) = tasks.join_next().await {
            let (height, stats) = joined.map_err(|e| {
                SubscriberError::Subxt(subxt::Error::Other(format!("gap worker panicked: {e}")))
            })??;
            results.push((height, stats));
        }
        results.sort_by_key(|(h, _)| *h);
        for (height, stats) in &results {
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
        }
        set_cursor(db, JOB_NAME_LIVE, BlockHeight(end), "running").await?;
        if (end - from + 1) % 100 < concurrency || end == to {
            let done = end - from + 1;
            let rate = done as f64 / started.elapsed().as_secs_f64().max(0.001);
            info!(
                height = end,
                to,
                done,
                rate_blocks_s = format!("{rate:.1}"),
                "gap fill progress"
            );
        }
        next = end + 1;
    }
    info!(
        from,
        to,
        elapsed_s = format!("{:.1}", started.elapsed().as_secs_f64()),
        "gap fill complete"
    );
    Ok(())
}

/// Height → hash → block → decode, for one gap block.
async fn fill_one(
    client: &OnlineClient<SubstrateConfig>,
    legacy: &LegacyRpcMethods<SubstrateConfig>,
    db: &PgPool,
    prices: &PriceResolver,
    height: u64,
) -> Result<sorametrics_substrate::BlockDecodeStats, SubscriberError> {
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
    Ok(decode_block_events(
        &block,
        db,
        prices,
        &client.metadata(),
        client.runtime_version().spec_version,
        client,
    )
    .await?)
}

/// One subscription session: connect, subscribe finalized, dispatch to
/// the per-block processor for each new block. Before processing each
/// incoming block, any hole between the persisted cursor and the block
/// is filled via [`fill_gap`] so reconnects never silently skip blocks.
async fn try_subscribe_once(
    url: &url::Url,
    db: &PgPool,
    gap_concurrency: usize,
    price_archive_rpc: Option<&url::Url>,
    cancel: &mut tokio::sync::watch::Receiver<bool>,
) -> Result<(), SubscriberError> {
    // Low-level RPC client first: we keep `LegacyRpcMethods` around for
    // height → hash lookups during gap fills (same pattern as ops
    // backfill), then upgrade the same connection to an `OnlineClient`.
    let rpc_client = RpcClient::from_url(url.as_str()).await?;
    // Live pricing shares this session's RPC connection: events inside
    // the live window are quoted on demand, older ones (long gap fills)
    // fall back to their hourly bucket.
    let prices = PriceResolver::live(db.clone(), rpc_client.clone()).await?;
    let prices = match price_archive_rpc {
        Some(archive) => {
            let archive_rpc = RpcClient::from_url(archive.as_str()).await?;
            info!(endpoint = %archive, "historical quotes at block enabled");
            prices.with_archive(archive_rpc)
        }
        None => prices,
    };
    let client =
        sorametrics_substrate::online_client::<SubstrateConfig>(rpc_client.clone()).await?;
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
                    let session = Session {
                        client: &client,
                        rpc: &rpc_client,
                        db,
                        prices: &prices,
                    };
                    fill_gap(&session, from, to, gap_concurrency, cancel).await?;
                    if *cancel.borrow_and_update() {
                        return Ok(());
                    }
                }

                let stats = decode_block_events(&block, db, &prices, &client.metadata(), client.runtime_version().spec_version, &client).await?;
                // Never move the cursor backwards: a replayed older block
                // (idempotent no-op in the DB) must not regress the resume
                // point.
                if last_processed.is_none_or(|lp| height.0 > lp) {
                    set_cursor(db, JOB_NAME_LIVE, height, "running").await?;
                    last_processed = Some(height.0);
                }

                // Freshness: wall clock minus the block's on-chain time.
                let lag_ms = chrono::Utc::now().timestamp_millis() - stats.block_timestamp_ms;
                if stats.has_any() {
                    info!(
                        height = height.0,
                        events = stats.events,
                        swaps = stats.decoded_swaps,
                        transfers = stats.decoded_transfers,
                        bridges = stats.decoded_bridges,
                        fee_burns = stats.decoded_fee_burns,
                        lag_ms,
                        "finalized block decoded"
                    );
                } else {
                    info!(
                        height = height.0,
                        events = stats.events,
                        lag_ms,
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
