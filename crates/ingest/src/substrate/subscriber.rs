//! Live finalized-block subscriber.
//!
//! Streams finalized blocks via subxt, delegates per-block decode +
//! insert to [`sorametrics_substrate::decode_block_events`], advances
//! the `sm.indexer_state` cursor on success, records errors there on
//! failure. Reconnects with backoff and rotates URLs in the failover
//! list when the stream ends or errors out.

use sorametrics_core::chain::BlockHeight;
use sorametrics_db::sm::{set_cursor, set_error};
use sorametrics_substrate::{decode_block_events, BlockProcessError};
use sqlx::PgPool;
use std::time::Duration;
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

/// One subscription session: connect, subscribe finalized, dispatch to
/// the per-block processor for each new block.
async fn try_subscribe_once(
    url: &url::Url,
    db: &PgPool,
    cancel: &mut tokio::sync::watch::Receiver<bool>,
) -> Result<(), SubscriberError> {
    use subxt::{OnlineClient, SubstrateConfig};

    let client = OnlineClient::<SubstrateConfig>::from_url(url.as_str()).await?;
    info!(endpoint = %url, "subxt connected, subscribing finalized blocks");

    let mut blocks = client.blocks().subscribe_finalized().await?;

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
                let stats = decode_block_events(&client, &block, db).await?;
                set_cursor(db, JOB_NAME_LIVE, height, "running").await?;

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
