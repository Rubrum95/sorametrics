//! Runtime configuration.
//!
//! Source of truth: env vars (dev: loaded from `.env` via `dotenvy`,
//! prod: set by PM2). CLI flags (via [`Cli`]) only select the worker
//! mode; everything else lives in env.

use clap::{Parser, ValueEnum};
use std::time::Duration;
use thiserror::Error;
use url::Url;

/// Top-level CLI for `sorametrics-ingest`.
#[derive(Debug, Parser)]
#[command(name = "sorametrics-ingest", version, about)]
pub struct Cli {
    /// Which chain/source to ingest.
    #[arg(long, value_enum, env = "INGEST_SOURCE", default_value = "substrate")]
    pub source: Source,
}

/// Mode flag for `sorametrics-ingest`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum Source {
    /// SORA v2 (Substrate). Subscribes finalized blocks via `WS_ENDPOINTS`.
    Substrate,
    /// Minamoto / Iroha 3. Polls `MINAMOTO_TORII` REST endpoints.
    Iroha,
    /// SoraFS observability. Phase ≥ 6.
    Sorafs,
}

/// Errors when loading configuration.
#[derive(Debug, Error)]
pub enum ConfigError {
    /// A required env var is missing or empty.
    #[error("env var {0} is required and was not set")]
    MissingEnv(&'static str),

    /// An env var failed to parse to its expected type.
    #[error("env var {name} has invalid value: {reason}")]
    Invalid {
        /// Variable name.
        name: &'static str,
        /// Free-form reason.
        reason: String,
    },

    /// `WS_ENDPOINTS` was set but contained zero usable URLs.
    #[error("WS_ENDPOINTS must contain at least one URL")]
    NoWsEndpoints,
}

/// Parsed substrate-side configuration.
#[derive(Debug, Clone)]
pub struct SubstrateConfig {
    /// Ordered list of WS endpoints, primary first. Failover rotates through.
    pub ws_endpoints: Vec<Url>,
    /// How often the healthcheck task pings the active endpoint.
    pub healthcheck_interval: Duration,
    /// How often (when on a non-primary) the task probes the primary for recovery.
    pub primary_probe_interval: Duration,
    /// Connect timeout for each individual `connect()` attempt.
    pub connect_timeout: Duration,
    /// How often the popular-asset price sampler quotes the chain.
    pub price_sample_interval: Duration,
    /// How often the sampler quotes the whole whitelist.
    pub price_sweep_interval: Duration,
}

impl SubstrateConfig {
    /// Reads from process env (assumes `dotenvy::dotenv()` has run if dev).
    pub fn from_env() -> Result<Self, ConfigError> {
        let raw =
            std::env::var("WS_ENDPOINTS").map_err(|_| ConfigError::MissingEnv("WS_ENDPOINTS"))?;
        let ws_endpoints = parse_ws_endpoints(&raw)?;

        let healthcheck_interval = duration_secs("SUBSTRATE_HEALTHCHECK_INTERVAL_SECS", 30)?;
        let primary_probe_interval = duration_secs("SUBSTRATE_PRIMARY_PROBE_INTERVAL_SECS", 120)?;
        let connect_timeout = duration_secs("SUBSTRATE_CONNECT_TIMEOUT_SECS", 10)?;
        let price_sample_interval = duration_secs("PRICE_SAMPLE_INTERVAL_SECS", 60)?;
        let price_sweep_interval = duration_secs("PRICE_SWEEP_INTERVAL_SECS", 600)?;

        Ok(Self {
            ws_endpoints,
            healthcheck_interval,
            primary_probe_interval,
            connect_timeout,
            price_sample_interval,
            price_sweep_interval,
        })
    }
}

/// Parses a comma-separated `WS_ENDPOINTS` string into a non-empty list of URLs.
fn parse_ws_endpoints(raw: &str) -> Result<Vec<Url>, ConfigError> {
    let mut out = Vec::new();
    for part in raw.split(',') {
        let trimmed = part.trim();
        if trimmed.is_empty() {
            continue;
        }
        let url = Url::parse(trimmed).map_err(|e| ConfigError::Invalid {
            name: "WS_ENDPOINTS",
            reason: format!("'{trimmed}' is not a valid URL: {e}"),
        })?;
        if !matches!(url.scheme(), "ws" | "wss") {
            return Err(ConfigError::Invalid {
                name: "WS_ENDPOINTS",
                reason: format!(
                    "'{trimmed}' must use ws:// or wss:// scheme, got '{}'",
                    url.scheme()
                ),
            });
        }
        out.push(url);
    }
    if out.is_empty() {
        return Err(ConfigError::NoWsEndpoints);
    }
    Ok(out)
}

/// Reads a `Duration` in seconds from an env var with a default.
fn duration_secs(name: &'static str, default_secs: u64) -> Result<Duration, ConfigError> {
    match std::env::var(name) {
        Ok(v) => {
            let secs: u64 = v.parse().map_err(|e| ConfigError::Invalid {
                name,
                reason: format!("not a u64: {e}"),
            })?;
            Ok(Duration::from_secs(secs))
        }
        Err(_) => Ok(Duration::from_secs(default_secs)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_single_url() {
        let urls = parse_ws_endpoints("wss://ws.mof.sora.org").unwrap();
        assert_eq!(urls.len(), 1);
        assert_eq!(urls[0].as_str(), "wss://ws.mof.sora.org/");
    }

    #[test]
    fn parse_multiple_urls_comma_separated() {
        let urls =
            parse_ws_endpoints("ws://127.0.0.1:9944,wss://ws.mof.sora.org,wss://mof2.sora.org")
                .unwrap();
        assert_eq!(urls.len(), 3);
        assert_eq!(urls[0].scheme(), "ws");
        assert_eq!(urls[1].scheme(), "wss");
    }

    #[test]
    fn skips_empty_segments() {
        let urls = parse_ws_endpoints(",ws://a.example, ,wss://b.example,").unwrap();
        assert_eq!(urls.len(), 2);
    }

    #[test]
    fn rejects_empty_input() {
        let err = parse_ws_endpoints("").unwrap_err();
        assert!(matches!(err, ConfigError::NoWsEndpoints));
    }

    #[test]
    fn rejects_only_whitespace_and_commas() {
        let err = parse_ws_endpoints(" , ,  ").unwrap_err();
        assert!(matches!(err, ConfigError::NoWsEndpoints));
    }

    #[test]
    fn rejects_invalid_url() {
        let err = parse_ws_endpoints("not-a-url").unwrap_err();
        assert!(matches!(
            err,
            ConfigError::Invalid {
                name: "WS_ENDPOINTS",
                ..
            }
        ));
    }

    #[test]
    fn rejects_http_scheme() {
        let err = parse_ws_endpoints("https://ws.mof.sora.org").unwrap_err();
        match err {
            ConfigError::Invalid {
                name: "WS_ENDPOINTS",
                reason,
            } => {
                assert!(reason.contains("ws:// or wss://"));
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }
}
