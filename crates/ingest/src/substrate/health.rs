//! Health-check loop for the substrate WS connection.
//!
//! Two responsibilities:
//!
//! 1. Liveness: every `healthcheck_interval`, call `system_health` on the
//!    active connection. On consecutive failures, force a rotation (the
//!    next URL in `WS_ENDPOINTS`).
//!
//! 2. Primary recovery: when the active endpoint is *not* the primary
//!    (index 0), every `primary_probe_interval` poke the primary. If it
//!    answers, return [`HealthOutcome::PrimaryRecovered`]. The caller is
//!    expected to `process::exit(0)` so PM2 restarts the binary cleanly
//!    on the primary. (Replicates the Node legacy behaviour.)
//!
//! The loop never returns `Ok(())` of its own accord — it runs until
//! either cancelled or [`HealthOutcome::PrimaryRecovered`].

use crate::substrate::connection::WsConnection;
use jsonrpsee::core::client::ClientT;
use jsonrpsee::rpc_params;
use jsonrpsee::ws_client::WsClientBuilder;
use serde_json::Value;
use std::time::{Duration, Instant};
use tokio::time::sleep;
use tracing::{info, warn};
use url::Url;

/// Outcome of [`run_health_loop`].
#[derive(Debug, PartialEq, Eq)]
pub enum HealthOutcome {
    /// Primary endpoint became reachable while we were on a fallback.
    /// Caller should `exit(0)` to let PM2 reconnect on the primary.
    PrimaryRecovered { primary: Url },
    /// Loop exited because the cancellation signal fired.
    Cancelled,
}

/// Number of consecutive `system_health` failures before forcing a rotate.
const ROTATE_AFTER_FAILURES: u32 = 3;

/// Runs the healthcheck loop until [`HealthOutcome`] resolves.
///
/// `healthcheck_interval` is the cadence of the liveness probe.
/// `primary_probe_interval` is the cadence of the primary-recovery probe
/// (only active when the connection is on a non-primary endpoint).
/// `connect_timeout` bounds each individual probe attempt.
pub async fn run_health_loop(
    conn: WsConnection,
    healthcheck_interval: Duration,
    primary_probe_interval: Duration,
    connect_timeout: Duration,
    mut cancel: tokio::sync::watch::Receiver<bool>,
) -> HealthOutcome {
    let mut consecutive_failures: u32 = 0;
    let mut last_primary_probe = Instant::now();
    let primary = conn.endpoints()[0].clone();

    loop {
        // Honor cancellation.
        if *cancel.borrow_and_update() {
            return HealthOutcome::Cancelled;
        }

        // Liveness probe on active connection.
        match conn.system_health().await {
            Ok(h) => {
                if consecutive_failures > 0 {
                    info!(
                        peers = h.peers,
                        is_syncing = h.is_syncing,
                        "WS health recovered"
                    );
                }
                consecutive_failures = 0;
            }
            Err(e) => {
                consecutive_failures += 1;
                warn!(
                    error = %e,
                    consecutive = consecutive_failures,
                    "WS health probe failed"
                );
                if consecutive_failures >= ROTATE_AFTER_FAILURES {
                    match conn.rotate().await {
                        Ok(idx) => {
                            info!(new_index = idx, "rotated after consecutive failures");
                            consecutive_failures = 0;
                        }
                        Err(rotate_err) => {
                            // All endpoints down. Keep looping; back off via the sleep below.
                            warn!(error = %rotate_err, "rotate failed (all endpoints down?)");
                        }
                    }
                }
            }
        }

        // Primary-recovery probe (only when on a non-primary).
        if !conn.is_on_primary().await && last_primary_probe.elapsed() >= primary_probe_interval {
            last_primary_probe = Instant::now();
            if probe_primary(&primary, connect_timeout).await {
                info!(primary = %primary, "primary endpoint recovered — caller should exit(0)");
                return HealthOutcome::PrimaryRecovered { primary };
            }
        }

        // Sleep until next iteration, but wake on cancel.
        tokio::select! {
            _ = sleep(healthcheck_interval) => {}
            _ = cancel.changed() => {
                if *cancel.borrow_and_update() {
                    return HealthOutcome::Cancelled;
                }
            }
        }
    }
}

/// One-shot connect-and-call to the primary URL. Returns `true` on success.
///
/// Deliberately uses a fresh connection (not the failover client) so we
/// don't disturb the active session. The connection is dropped immediately.
async fn probe_primary(primary: &Url, timeout: Duration) -> bool {
    match WsClientBuilder::default()
        .connection_timeout(timeout)
        .build(primary.as_str())
        .await
    {
        Ok(c) => {
            // Cheap probe: any response is good enough.
            c.request::<Value, _>("system_health", rpc_params![])
                .await
                .is_ok()
        }
        Err(_) => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn probe_primary_returns_false_for_closed_port() {
        let url = Url::parse("ws://127.0.0.1:1").unwrap();
        assert!(!probe_primary(&url, Duration::from_millis(300)).await);
    }
}
