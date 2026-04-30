//! Telemetry primitives shared by all binaries.
//!
//! Phase 0 ships only `tracing` setup. Prometheus metrics export will land
//! in the API crate once the first endpoint exists, since the registry is
//! collected at HTTP layer.

#![forbid(unsafe_code)]
#![deny(rust_2018_idioms, missing_docs)]

use anyhow::Result;
use tracing_subscriber::{fmt, prelude::*, EnvFilter};

/// Output format for log lines.
#[derive(Clone, Copy, Debug)]
pub enum LogFormat {
    /// Human-readable, color-aware. Default for local dev.
    Pretty,
    /// Single-line JSON per record. Default for production.
    Json,
}

/// Initialize global tracing subscriber.
///
/// Reads filter from `RUST_LOG` env var (defaults to `info` if unset).
/// Should be called exactly once per process at startup.
pub fn init(format: LogFormat) -> Result<()> {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));

    let registry = tracing_subscriber::registry().with(filter);

    match format {
        LogFormat::Pretty => registry.with(fmt::layer().with_target(true)).try_init()?,
        LogFormat::Json => registry
            .with(
                fmt::layer()
                    .json()
                    .with_current_span(false)
                    .with_span_list(false),
            )
            .try_init()?,
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn log_format_is_copy() {
        let f = LogFormat::Pretty;
        let _g = f;
        let _h = f;
    }
}
