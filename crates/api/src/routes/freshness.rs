//! `/health/freshness` — indexer lag from two independent angles:
//!
//! 1. **Jobs** (`sm.indexer_state.last_updated_at`): is the ingest loop
//!    alive and advancing its cursor?
//! 2. **Tables** (`MAX(block_timestamp)` per `sm.live_*` table): is real
//!    data actually landing?
//!
//! Both are required. The cursor advances on every finalized block even
//! when every decoder is broken (decode failures are per-event warns),
//! so job lag alone would report "healthy" while tables silently freeze —
//! the exact failure mode of the legacy Node indexer (38 days of frozen
//! `extrinsic_events` behind a green `/health`).
//!
//! Job thresholds (default per `core::time::FreshnessStatus`):
//! - healthy: lag < 30s · degraded: < 5min · stale: ≥ 5min
//!
//! Table thresholds encode the *expected event cadence* of each table,
//! not the block cadence — a table with no new rows for an hour is fine
//! if that event type only happens a few times a day:
//! - `fee_events`: fee events fire on nearly every signed extrinsic
//!   → healthy < 10min, stale ≥ 1h.
//! - `transfers`: regular activity → healthy < 30min, stale ≥ 6h.
//! - `swaps`: moderate activity → healthy < 1h, stale ≥ 12h.
//! - `bridges`: intrinsically sparse (days between bridge txs are
//!   normal) → lag reported for information, `status` is `null`.
//!
//! Empty tables report `null` lag and `null` status (expected at
//! bootstrap; not classifiable as stale without history).

use crate::{error::ApiError, AppState};
use axum::{extract::State, routing::get, Json, Router};
use chrono::{DateTime, TimeDelta, Utc};
use serde::Serialize;
use sorametrics_core::time::FreshnessStatus;
use sqlx::PgPool;

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
struct TableFreshness {
    table: String,
    /// `block_timestamp` of the newest row, `null` for an empty table.
    latest_block_timestamp: Option<DateTime<Utc>>,
    /// `null` when the table is empty.
    lag_seconds: Option<i64>,
    /// `null` for activity-sparse tables (bridges) and empty tables.
    status: Option<FreshnessStatus>,
}

#[derive(Serialize)]
struct FreshnessResponse {
    jobs: Vec<JobFreshness>,
    tables: Vec<TableFreshness>,
    /// Worst status across jobs and classified tables — a single-pixel
    /// signal for monitoring.
    worst_status: FreshnessStatus,
}

/// Per-table freshness thresholds: `(healthy_below, degraded_below)`.
/// `None` = report lag but never classify (activity-sparse table).
fn table_thresholds(table: &str) -> Option<(TimeDelta, TimeDelta)> {
    match table {
        "fee_events" => Some((TimeDelta::minutes(10), TimeDelta::hours(1))),
        "transfers" => Some((TimeDelta::minutes(30), TimeDelta::hours(6))),
        "swaps" => Some((TimeDelta::hours(1), TimeDelta::hours(12))),
        _ => None,
    }
}

/// `MAX(block_timestamp)` for one `sm.live_*` table.
///
/// One tiny query per table (4 total): `MAX` on the indexed
/// `block_timestamp` column resolves via a reverse index scan. Table
/// names cannot be bind parameters, so this is a closed match over
/// four `query_scalar!` calls — each still compile-time checked.
async fn latest_block_timestamp(
    db: &PgPool,
    table: &str,
) -> Result<Option<DateTime<Utc>>, ApiError> {
    let ts = match table {
        "swaps" => {
            sqlx::query_scalar!(r#"SELECT MAX(block_timestamp) FROM sm.swaps"#)
                .fetch_one(db)
                .await?
        }
        "transfers" => {
            sqlx::query_scalar!(r#"SELECT MAX(block_timestamp) FROM sm.transfers"#)
                .fetch_one(db)
                .await?
        }
        "bridges" => {
            sqlx::query_scalar!(r#"SELECT MAX(block_timestamp) FROM sm.bridges"#)
                .fetch_one(db)
                .await?
        }
        "fee_events" => {
            sqlx::query_scalar!(r#"SELECT MAX(block_timestamp) FROM sm.fee_events"#)
                .fetch_one(db)
                .await?
        }
        other => {
            return Err(ApiError::Internal(format!(
                "unknown freshness table: {other}"
            )))
        }
    };
    Ok(ts)
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

    const TABLES: [&str; 4] = ["swaps", "transfers", "bridges", "fee_events"];

    let mut tables = Vec::with_capacity(TABLES.len());
    for table in TABLES {
        let latest = latest_block_timestamp(&state.db, table).await?;
        let lag = latest.map(|ts| now - ts);
        let status = match (lag, table_thresholds(table)) {
            (Some(lag), Some((healthy_below, degraded_below))) => {
                let s = FreshnessStatus::classify(lag, healthy_below, degraded_below);
                worst = worst_of(worst, s);
                Some(s)
            }
            _ => None,
        };

        tables.push(TableFreshness {
            table: table.to_string(),
            latest_block_timestamp: latest,
            lag_seconds: lag.map(|l| l.num_seconds()),
            status,
        });
    }

    Ok(Json(FreshnessResponse {
        jobs,
        tables,
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

    #[test]
    fn bridges_and_unknown_tables_have_no_thresholds() {
        assert!(table_thresholds("bridges").is_none());
        assert!(table_thresholds("something_else").is_none());
    }

    #[test]
    fn classified_tables_have_thresholds() {
        for t in ["swaps", "transfers", "fee_events"] {
            assert!(table_thresholds(t).is_some(), "{t} must be classified");
        }
    }
}
