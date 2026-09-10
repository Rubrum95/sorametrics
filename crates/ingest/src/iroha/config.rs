//! Env-driven configuration of the iroha poller. Defaults are the
//! Node's `minamoto/config.js`.

use crate::config::ConfigError;
use sorametrics_iroha::ToriiConfig;
use std::time::Duration;

/// Poll intervals + Torii client settings.
#[derive(Clone, Debug)]
pub struct IrohaConfig {
    /// Torii base URL + HTTP policy (`MINAMOTO_*`).
    pub torii: ToriiConfig,
    /// `network_state` job period.
    pub poll_network: Duration,
    /// `blocks` job period.
    pub poll_blocks: Duration,
    /// `transactions` / `instructions` job period.
    pub poll_tx: Duration,
    /// `domains` job period.
    pub poll_domains: Duration,
    /// `accounts` job period.
    pub poll_accounts: Duration,
    /// `assets` / `asset_definitions` job period.
    pub poll_assets: Duration,
    /// `peers` job period.
    pub poll_peers: Duration,
    /// `prometheus` job period.
    pub poll_prom: Duration,
    /// Snapshot retention.
    pub metrics_retention_days: i32,
    /// `metrics_cleanup` job period.
    pub metrics_cleanup: Duration,
    /// Cursor pages a backfill walks at most (100 rows each).
    pub backfill_max_pages: u32,
}

impl IrohaConfig {
    /// Reads the env.
    pub fn from_env() -> Result<Self, ConfigError> {
        let torii = ToriiConfig::from_env().map_err(|e| ConfigError::Invalid {
            name: "MINAMOTO_TORII",
            reason: e.to_string(),
        })?;
        Ok(Self {
            torii,
            poll_network: millis("MINAMOTO_POLL_NETWORK_MS", 10_000)?,
            poll_blocks: millis("MINAMOTO_POLL_BLOCKS_MS", 30_000)?,
            poll_tx: millis("MINAMOTO_POLL_TX_MS", 30_000)?,
            poll_domains: millis("MINAMOTO_POLL_DOMAINS_MS", 300_000)?,
            poll_accounts: millis("MINAMOTO_POLL_ACCOUNTS_MS", 300_000)?,
            poll_assets: millis("MINAMOTO_POLL_ASSETS_MS", 300_000)?,
            poll_peers: millis("MINAMOTO_POLL_PEERS_MS", 60_000)?,
            poll_prom: millis("MINAMOTO_POLL_PROM_MS", 60_000)?,
            metrics_retention_days: int("MINAMOTO_METRICS_RETENTION_DAYS", 30)?,
            metrics_cleanup: millis("MINAMOTO_METRICS_CLEANUP_MS", 3_600_000)?,
            backfill_max_pages: int("MINAMOTO_BACKFILL_MAX_PAGES", 400)? as u32,
        })
    }
}

fn millis(name: &'static str, default_ms: u64) -> Result<Duration, ConfigError> {
    match std::env::var(name) {
        Ok(v) => v
            .parse::<u64>()
            .map(Duration::from_millis)
            .map_err(|e| ConfigError::Invalid {
                name,
                reason: format!("not a u64 of milliseconds: {e}"),
            }),
        Err(_) => Ok(Duration::from_millis(default_ms)),
    }
}

fn int(name: &'static str, default: i32) -> Result<i32, ConfigError> {
    match std::env::var(name) {
        Ok(v) => v.parse::<i32>().map_err(|e| ConfigError::Invalid {
            name,
            reason: format!("not an integer: {e}"),
        }),
        Err(_) => Ok(default),
    }
}
