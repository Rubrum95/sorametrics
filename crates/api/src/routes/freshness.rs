//! `/health/freshness` — per-table data lag derived from
//! `sm.indexer_state.last_updated_at`.
//!
//! Status thresholds (default per `core::time::FreshnessStatus`):
//! - healthy: lag < 30s
//! - degraded: lag < 5min
//! - stale: lag ≥ 5min
//!
//! Useful as a structured health endpoint: a load balancer can hit
//! `/health` for liveness and a monitoring system can hit
//! `/health/freshness` for "is the indexer keeping up?".

use crate::{error::ApiError, AppState};
use axum::{extract::State, routing::get, Json, Router};
use chrono::{DateTime, Utc};
use serde::Serialize;
use sorametrics_core::time::FreshnessStatus;

/// Build the `/health/freshness` sub-router.
pub fn router() -> Router<AppState> {
    Router::new().route("/health/freshness", get(freshness))
}

#[derive(Serialize)]
struct JobFreshness {
    job_name: String,
    last_processed_block: i64,
    last_updated_at: DateTime<Utc>,
    lag_seconds: i64,
    status: FreshnessStatus,
    error_message: Option<String>,
    indexer_status: String,
}

#[derive(Serialize)]
struct FreshnessResponse {
    jobs: Vec<JobFreshness>,
    /// Worst per-table status, useful as a single-pixel signal.
    worst_status: FreshnessStatus,
}

async fn freshness(State(state): State<AppState>) -> Result<Json<FreshnessResponse>, ApiError> {
    let rows = sqlx::query!(
        r#"
        SELECT job_name, last_processed_block, last_updated_at, status, error_message
        FROM sm.indexer_state
        ORDER BY job_name
        "#,
    )
    .fetch_all(&state.db)
    .await?;

    let now = Utc::now();
    let mut worst = FreshnessStatus::Healthy;
    let mut jobs = Vec::with_capacity(rows.len());

    for r in rows {
        let lag = now - r.last_updated_at;
        let status = FreshnessStatus::classify_default(lag);
        worst = worst_of(worst, status);

        jobs.push(JobFreshness {
            job_name: r.job_name,
            last_processed_block: r.last_processed_block,
            last_updated_at: r.last_updated_at,
            lag_seconds: lag.num_seconds(),
            status,
            error_message: r.error_message,
            indexer_status: r.status,
        });
    }

    Ok(Json(FreshnessResponse {
        jobs,
        worst_status: worst,
    }))
}

/// Returns the worse of two statuses (Stale > Degraded > Healthy).
fn worst_of(a: FreshnessStatus, b: FreshnessStatus) -> FreshnessStatus {
    use FreshnessStatus::*;
    match (a, b) {
        (Stale, _) | (_, Stale) => Stale,
        (Degraded, _) | (_, Degraded) => Degraded,
        _ => Healthy,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn worst_of_picks_stale() {
        assert_eq!(
            worst_of(FreshnessStatus::Healthy, FreshnessStatus::Stale),
            FreshnessStatus::Stale
        );
        assert_eq!(
            worst_of(FreshnessStatus::Stale, FreshnessStatus::Degraded),
            FreshnessStatus::Stale
        );
    }

    #[test]
    fn worst_of_picks_degraded_over_healthy() {
        assert_eq!(
            worst_of(FreshnessStatus::Healthy, FreshnessStatus::Degraded),
            FreshnessStatus::Degraded
        );
    }

    #[test]
    fn worst_of_all_healthy_stays_healthy() {
        assert_eq!(
            worst_of(FreshnessStatus::Healthy, FreshnessStatus::Healthy),
            FreshnessStatus::Healthy
        );
    }
}
