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
use sorametrics_db::sm::get_cursor;
use sqlx::PgPool;
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
    /// The live cursor stopped moving while the chain kept finalizing
    /// blocks (a stalled subscription). Caller should exit so the
    /// supervisor restarts the process; the subscriber fills the gap on
    /// resume.
    Stalled {
        /// Persisted cursor.
        cursor: u64,
        /// Finalized head on the node.
        head: u64,
    },
    /// Loop exited because the cancellation signal fired.
    Cancelled,
}

/// Consecutive probes with the cursor stuck behind the alert threshold
/// before the loop reports [`HealthOutcome::Stalled`].
const STALL_PROBES: u32 = 3;

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
    db: PgPool,
    lag_alert_blocks: u64,
    healthcheck_interval: Duration,
    primary_probe_interval: Duration,
    connect_timeout: Duration,
    mut cancel: tokio::sync::watch::Receiver<bool>,
) -> HealthOutcome {
    let mut consecutive_failures: u32 = 0;
    let mut last_primary_probe = Instant::now();
    let primary = conn.endpoints()[0].clone();
    let mut stall = StallTracker::default();

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
                match lag_probe(&conn, &db).await {
                    Ok((cursor, head)) => {
                        if let Some(outcome) = stall.observe(cursor, head, lag_alert_blocks) {
                            return outcome;
                        }
                    }
                    Err(e) => warn!(error = %e, "lag probe failed"),
                }
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

/// `(cursor, finalized head)` for the lag monitor.
async fn lag_probe(conn: &WsConnection, db: &PgPool) -> Result<(u64, u64), String> {
    let head = conn.finalized_number().await.map_err(|e| e.to_string())?;
    let cursor = get_cursor(db, "substrate_live")
        .await
        .map_err(|e| e.to_string())?
        .map(|h| h.0)
        .unwrap_or(0);
    Ok((cursor, head))
}

/// Tracks whether the cursor moves between probes while behind the head.
#[derive(Default)]
struct StallTracker {
    last_cursor: Option<u64>,
    stuck_probes: u32,
}

impl StallTracker {
    /// Logs the lag; returns `Stalled` after [`STALL_PROBES`] probes with
    /// the cursor unchanged and further than `alert` blocks behind.
    fn observe(&mut self, cursor: u64, head: u64, alert: u64) -> Option<HealthOutcome> {
        let lag = head.saturating_sub(cursor);
        let moved = self.last_cursor.is_some_and(|c| cursor > c);
        if lag > alert {
            if moved || self.last_cursor.is_none() {
                self.stuck_probes = 0;
            } else {
                self.stuck_probes += 1;
            }
            warn!(
                cursor,
                head,
                lag_blocks = lag,
                stuck_probes = self.stuck_probes,
                "indexer behind the finalized head"
            );
        } else {
            self.stuck_probes = 0;
        }
        self.last_cursor = Some(cursor);
        if self.stuck_probes >= STALL_PROBES {
            return Some(HealthOutcome::Stalled { cursor, head });
        }
        None
    }
}

#[cfg(test)]
mod stall_tests {
    use super::*;

    #[test]
    fn stalled_only_when_behind_and_not_moving() {
        let mut t = StallTracker::default();
        assert!(t.observe(100, 105, 20).is_none());
        // Behind but moving: never stalls.
        assert!(t.observe(101, 150, 20).is_none());
        assert!(t.observe(102, 200, 20).is_none());
        assert!(t.observe(103, 250, 20).is_none());
        // Behind and stuck: three probes → stalled.
        assert!(t.observe(103, 300, 20).is_none());
        assert!(t.observe(103, 350, 20).is_none());
        assert!(matches!(
            t.observe(103, 400, 20),
            Some(HealthOutcome::Stalled {
                cursor: 103,
                head: 400
            })
        ));
    }

    #[test]
    fn stuck_but_within_threshold_is_fine() {
        let mut t = StallTracker::default();
        for _ in 0..10 {
            assert!(t.observe(500, 510, 20).is_none());
        }
    }
}
