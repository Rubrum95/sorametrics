//! Periodic price sampler — the Node's `setInterval(updateKeyPrices, 60000)`.
//!
//! Every `interval`, quotes the popular asset set through
//! [`PriceResolver::sample_popular`] and folds the results into
//! `ts.price_history`. Runs on its own RPC connection so a stuck
//! subscriber cannot stall price history and vice versa. On RPC
//! failure the loop reconnects to the next endpoint after `backoff`.

use sorametrics_substrate::{PriceError, PriceResolver};
use sqlx::PgPool;
use std::time::Duration;
use subxt::backend::rpc::RpcClient;
use tokio::sync::watch;
use tokio::time::{interval, sleep, MissedTickBehavior};
use tracing::{info, warn};

/// Runs until `cancel` fires. Returns only on cancel or a DB error
/// (an RPC error rotates the endpoint instead).
pub async fn run_price_sampler(
    endpoints: Vec<url::Url>,
    db: PgPool,
    period: Duration,
    sweep_period: Duration,
    backoff: Duration,
    mut cancel: watch::Receiver<bool>,
) -> Result<(), PriceError> {
    assert!(
        !endpoints.is_empty(),
        "price sampler called with empty endpoints — guard at config layer"
    );
    let mut endpoint_idx = 0usize;

    loop {
        if *cancel.borrow_and_update() {
            return Ok(());
        }
        let url = &endpoints[endpoint_idx];
        match sample_session(url, &db, period, sweep_period, &mut cancel).await {
            Ok(()) => return Ok(()),
            Err(PriceError::Db(e)) => return Err(PriceError::Db(e)),
            Err(e) => warn!(error = %e, endpoint = %url, "price sampler session ended; rotating"),
        }
        endpoint_idx = (endpoint_idx + 1) % endpoints.len();
        tokio::select! {
            _ = sleep(backoff) => {}
            _ = cancel.changed() => {
                if *cancel.borrow_and_update() {
                    return Ok(());
                }
            }
        }
    }
}

async fn sample_session(
    url: &url::Url,
    db: &PgPool,
    period: Duration,
    sweep_period: Duration,
    cancel: &mut watch::Receiver<bool>,
) -> Result<(), PriceError> {
    let rpc = RpcClient::from_url(url.as_str()).await?;
    let resolver = PriceResolver::live(db.clone(), rpc).await?;
    let popular = resolver.popular_assets().len();
    let whitelisted = resolver.whitelisted_assets().len();
    info!(endpoint = %url, popular, whitelisted, "price sampler connected");

    let mut ticker = interval(period);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let mut sweep = interval(sweep_period);
    sweep.set_missed_tick_behavior(MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            _ = ticker.tick() => {
                let mut outcome = resolver.sample_popular().await?;
                if outcome.looks_disconnected() {
                    if let Some(e) = outcome.last_error.take() {
                        return Err(e);
                    }
                }
                info!(
                    priced = outcome.priced,
                    no_route = outcome.no_route,
                    failed = outcome.failed,
                    popular,
                    "price samples recorded"
                );
            }
            _ = sweep.tick() => {
                let mut outcome = resolver.sample_whitelist().await?;
                if outcome.looks_disconnected() {
                    if let Some(e) = outcome.last_error.take() {
                        return Err(e);
                    }
                }
                info!(
                    priced = outcome.priced,
                    no_route = outcome.no_route,
                    failed = outcome.failed,
                    whitelisted,
                    "whitelist price sweep recorded"
                );
            }
            _ = cancel.changed() => {
                if *cancel.borrow_and_update() {
                    return Ok(());
                }
            }
        }
    }
}
