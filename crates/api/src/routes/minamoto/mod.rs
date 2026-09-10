//! `/api/minamoto/*` — the Minamoto (Iroha 3) contract of
//! `minamoto/routes.js`: DB-backed reads over `mn.*` and live
//! passthroughs to Torii.
//!
//! Torii routes the `optimizations` catalogue removed or restricted to
//! operators (`/peers`, `/v1/sumeragi/{telemetry,collectors}`,
//! `/v1/gov/council/current`, `/v1/kaigi/relays`, `/v1/gov/unlocks/stats`)
//! answer 503 with the reason instead of invented data.

use crate::error::ApiError;
use crate::AppState;
use axum::extract::{Query, State};
use axum::routing::get;
use axum::{Json, Router};
use serde::Deserialize;
use serde_json::{json, Value};
use sorametrics_iroha::ToriiClient;

pub mod chain;
pub mod cross_chain;
pub mod telemetry;

/// Largest `per_page` accepted (`MAX_PAGE_SIZE`).
pub const MAX_PAGE_SIZE: i64 = 100;

/// Build the sub-router (mounted at `/api/minamoto`).
pub fn router() -> Router<AppState> {
    let inner = Router::new()
        .route("/health", get(health))
        .route("/torii/health", get(torii_health))
        .route("/status", get(status))
        .route("/network-state", get(network_state))
        .route("/network-state/live", get(network_state_live))
        .route("/indexer/state", get(indexer_state))
        .route("/prometheus/raw", get(prometheus_raw))
        .route("/prometheus/parsed", get(prometheus_parsed))
        .route("/prometheus/metric/:name", get(prometheus_metric))
        .merge(chain::router())
        .merge(telemetry::router())
        .merge(cross_chain::router());
    Router::new().nest("/api/minamoto", inner)
}

/// `?page&per_page` as the Node's `clampPage`.
#[derive(Debug, Default, Deserialize)]
pub struct PageQuery {
    /// 1-based page.
    pub page: Option<String>,
    /// Page size.
    pub per_page: Option<String>,
}

/// `(page, per_page)`: `page ≥ 1`, `1 ≤ per_page ≤ 100`, non-numbers →
/// defaults (`parseInt || default`).
pub fn clamp_page(q: &PageQuery, default_size: i64) -> (i64, i64) {
    let page = q
        .page
        .as_deref()
        .and_then(js_parse_int)
        .filter(|p| *p != 0)
        .unwrap_or(1)
        .max(1);
    let per_page = q
        .per_page
        .as_deref()
        .and_then(js_parse_int)
        .filter(|p| *p != 0)
        .unwrap_or(default_size)
        .clamp(1, MAX_PAGE_SIZE);
    (page, per_page)
}

/// `parseInt(x, 10)`: leading integer prefix, `None` when none.
pub fn js_parse_int(raw: &str) -> Option<i64> {
    let t = raw.trim_start();
    let (sign, rest) = match t.strip_prefix('-') {
        Some(r) => (-1, r),
        None => (1, t.strip_prefix('+').unwrap_or(t)),
    };
    let digits: String = rest.chars().take_while(|c| c.is_ascii_digit()).collect();
    if digits.is_empty() {
        return None;
    }
    digits.parse::<i64>().ok().map(|v| v * sign)
}

/// The Torii client or 503.
pub fn torii(state: &AppState) -> Result<&ToriiClient, ApiError> {
    state.torii.as_ref().ok_or(ApiError::NoTorii)
}

/// Non-empty query string, as the Node's `req.query.x ? String(x) : null`.
pub fn opt_str(v: &Option<String>) -> Option<String> {
    v.as_deref().filter(|s| !s.is_empty()).map(String::from)
}

async fn health(State(state): State<AppState>) -> Result<Json<Value>, ApiError> {
    let db: (String,) = sqlx::query_as("SELECT current_database()")
        .fetch_one(&state.db)
        .await?;
    let torii = state
        .torii
        .as_ref()
        .map(|t| t.base_url())
        .unwrap_or_default();
    Ok(Json(json!({ "ok": true, "db": db.0, "torii": torii })))
}

async fn torii_health(State(state): State<AppState>) -> Result<Json<Value>, ApiError> {
    let t = torii(&state)?.health().await?;
    Ok(Json(json!({ "ok": true, "message": t.trim() })))
}

async fn status(State(state): State<AppState>) -> Result<Json<Value>, ApiError> {
    Ok(Json(torii(&state)?.status().await?))
}

async fn network_state(State(state): State<AppState>) -> Result<Json<Value>, ApiError> {
    let row = sorametrics_db::mn::get_network_state(&state.db).await?;
    let updated_at = row.as_ref().map(|r| {
        r.updated_at
            .to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
    });
    Ok(Json(
        json!({ "state": row, "source": "db", "updated_at": updated_at }),
    ))
}

/// The Node proxied `/v1/explorer/metrics`, which now requires an
/// account signature; the same keys are built from the public `/status`
/// plus the indexed counts.
async fn network_state_live(State(state): State<AppState>) -> Result<Json<Value>, ApiError> {
    let s = torii(&state)?.status_snapshot().await?;
    let counts = sorametrics_db::mn::indexed_counts(&state.db).await?;
    let block_created_at = counts
        .last_block_at
        .map(|t| t.to_rfc3339_opts(chrono::SecondsFormat::Millis, true));
    let metrics = json!({
        "peers": s.peers,
        "domains": counts.domains,
        "accounts": counts.accounts,
        "assets": counts.assets,
        "transactions_accepted": s.txs_approved,
        "transactions_rejected": s.txs_rejected,
        "block": s.blocks,
        "block_created_at": block_created_at,
        "finalized_block": s.blocks,
        "avg_commit_time": { "ms": s.commit_time_ms },
        "avg_block_time": counts.avg_block_ms.map(|ms| json!({ "ms": ms })),
    });
    Ok(Json(json!({
        "state": metrics,
        "source": "torii",
        "updated_at": chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
    })))
}

async fn indexer_state(State(state): State<AppState>) -> Result<Json<Value>, ApiError> {
    let items = sorametrics_db::mn::list_indexer_state(&state.db).await?;
    let items: Vec<Value> = items
        .into_iter()
        .map(|r| {
            json!({
                "name": r.name,
                "last_run_at": r.last_run_at.to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
                "last_run_status": r.last_run_status,
                "error_count": r.error_count,
                "last_error": r.last_error,
                "last_value": r.last_value,
            })
        })
        .collect();
    Ok(Json(json!({ "items": items })))
}

async fn prometheus_raw(
    State(state): State<AppState>,
) -> Result<axum::response::Response, ApiError> {
    use axum::response::IntoResponse;
    let text = torii(&state)?.metrics().await?;
    Ok((
        [(
            axum::http::header::CONTENT_TYPE,
            "text/plain; charset=utf-8",
        )],
        text,
    )
        .into_response())
}

async fn prometheus_parsed(State(state): State<AppState>) -> Result<Json<Value>, ApiError> {
    let text = torii(&state)?.metrics().await?;
    let samples: Vec<Value> = sorametrics_iroha::prom::parse(&text)
        .into_iter()
        .map(|s| json!({ "name": s.name, "labels": s.labels, "value": s.value }))
        .collect();
    Ok(Json(json!({ "count": samples.len(), "samples": samples })))
}

/// `?hours` (1..=168, default 24).
#[derive(Debug, Deserialize)]
pub struct HoursQuery {
    /// Window in hours.
    pub hours: Option<String>,
}

async fn prometheus_metric(
    State(state): State<AppState>,
    axum::extract::Path(name): axum::extract::Path<String>,
    Query(q): Query<HoursQuery>,
) -> Result<Json<Value>, ApiError> {
    let hours = q
        .hours
        .as_deref()
        .and_then(js_parse_int)
        .filter(|h| *h != 0)
        .unwrap_or(24)
        .clamp(1, 168);
    let series = sorametrics_db::mn::get_metric_series(&state.db, &name, hours as i32).await?;
    Ok(Json(
        json!({ "name": name, "hours": hours, "series": series }),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn page_clamping_matches_node() {
        let q = PageQuery {
            page: Some("3".into()),
            per_page: Some("500".into()),
        };
        assert_eq!(clamp_page(&q, 20), (3, 100));
        let q = PageQuery {
            page: Some("0".into()),
            per_page: Some("0".into()),
        };
        assert_eq!(clamp_page(&q, 20), (1, 20));
        let q = PageQuery {
            page: Some("-4".into()),
            per_page: Some("abc".into()),
        };
        assert_eq!(clamp_page(&q, 50), (1, 50));
        let q = PageQuery {
            page: Some("2x".into()),
            per_page: Some("7".into()),
        };
        assert_eq!(clamp_page(&q, 50), (2, 7));
        assert_eq!(clamp_page(&PageQuery::default(), 20), (1, 20));
    }

    #[test]
    fn js_parse_int_prefixes() {
        assert_eq!(js_parse_int("12ab"), Some(12));
        assert_eq!(js_parse_int(" -3"), Some(-3));
        assert_eq!(js_parse_int("+8"), Some(8));
        assert_eq!(js_parse_int("x1"), None);
        assert_eq!(js_parse_int(""), None);
    }
}
