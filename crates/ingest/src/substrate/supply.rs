//! Circulating-supply snapshots — the Node's `takeSupplySnapshots`
//! (every 30 min): for XOR, VAL, PSWAP, TBCD and KUSD ask the MOF
//! (`https://mof.sora.org/qty/<symbol>`, mirrors `mof2` / `mof3`; KUSD
//! is `xstusd` there) and store the figure in `sm.supply_snapshots`.
//! A mirror answer counts when it parses as a positive number (for XOR
//! also below 1e9); otherwise the next mirror is tried and, failing all,
//! the symbol is skipped this round. Never the on-chain issuance: it
//! includes locked / vesting balances.

use sorametrics_core::mof::{fetch_mof_supply, SUPPLY_TOKENS};
use sorametrics_db::sm::insert_supply_snapshot;
use sorametrics_db::DbError;
use sqlx::PgPool;
use std::time::Duration;
use thiserror::Error;
use tokio::sync::watch;
use tokio::time::{interval, MissedTickBehavior};
use tracing::{info, warn};

/// Take one round of snapshots; returns how many symbols were stored.
pub async fn take_supply_snapshots(http: &reqwest::Client, db: &PgPool) -> Result<usize, DbError> {
    let at = chrono::Utc::now();
    let mut stored = 0;
    for (symbol, mof, asset_id) in SUPPLY_TOKENS {
        match fetch_mof_supply(http, symbol, mof).await {
            Some(v) => {
                insert_supply_snapshot(db, symbol, Some(asset_id), at, v).await?;
                stored += 1;
            }
            None => warn!(symbol, "supply snapshot skipped (MOF unavailable)"),
        }
    }
    Ok(stored)
}

/// Errors that stop the sampler.
#[derive(Debug, Error)]
pub enum SupplyError {
    /// Database write failed.
    #[error("db: {0}")]
    Db(#[from] DbError),
    /// The HTTP client could not be built.
    #[error("http client: {0}")]
    Http(#[from] reqwest::Error),
}

/// Runs until `cancel` fires; returns only on cancel, a DB error or an
/// unusable HTTP client.
pub async fn run_supply_sampler(
    db: PgPool,
    period: Duration,
    mut cancel: watch::Receiver<bool>,
) -> Result<(), SupplyError> {
    let http = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()?;
    let mut ticker = interval(period);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            _ = ticker.tick() => {
                let stored = take_supply_snapshots(&http, &db).await?;
                info!(stored, total = SUPPLY_TOKENS.len(), "supply snapshots recorded");
            }
            _ = cancel.changed() => {
                if *cancel.borrow_and_update() {
                    return Ok(());
                }
            }
        }
    }
}
