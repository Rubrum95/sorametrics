//! Price-history reads: `/chart/:symbol` and `/tools/price-series`.
//!
//! `/chart/:symbol?res=<minutes>` (Node: `getCandles`): OHLC candles of
//! `res` minutes (default 60) built from the hourly buckets of the last
//! `res × 1000` minutes — open = first bucket, close = last, high/low
//! over the candle. Buckets with a non-positive price are ignored.
//! Unknown symbol → `[]`. Symbols are `[A-Za-z0-9]{1,12}` (400 else).
//!
//! `/tools/price-series?assets=<id,…>&window=<7d|30d|90d|365d|all>`
//! (Node: `getPriceSeries`): mean price per resample bucket for up to 4
//! assets; `{ window, bucketSec, series: { <id>: [{t, p}] } }`, every
//! requested id present (empty when no data). No valid id → 400.

use crate::state::Registry;
use crate::{error::ApiError, AppState};
use axum::{
    extract::{Path, Query, State},
    routing::get,
    Json, Router,
};
use chrono::Utc;
use serde::{Deserialize, Serialize};
use sorametrics_db::ts::{price_buckets_since, price_series, PricePoint, SeriesPoint};
use std::collections::BTreeMap;

/// Build the sub-router.
pub fn router() -> Router<AppState> {
    Router::new()
        .route("/chart/:symbol", get(chart))
        .route("/tools/price-series", get(tools_price_series))
}

/// Node: `VALID_SYMBOL = /^[A-Za-z0-9]{1,12}$/`.
fn is_valid_symbol(s: &str) -> bool {
    !s.is_empty() && s.len() <= 12 && s.chars().all(|c| c.is_ascii_alphanumeric())
}

/// One OHLC candle; `time` is the candle start in unix seconds.
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct Candle {
    time: i64,
    open: f64,
    high: f64,
    low: f64,
    close: f64,
}

/// Node `getCandles` aggregation over hourly buckets.
pub fn candles(points: &[PricePoint], interval_sec: i64, limit: usize) -> Vec<Candle> {
    let mut out: BTreeMap<i64, Candle> = BTreeMap::new();
    for p in points {
        if p.price_usd <= 0.0 {
            continue;
        }
        let t = p.hour_bucket.div_euclid(interval_sec) * interval_sec;
        out.entry(t)
            .and_modify(|c| {
                c.high = c.high.max(p.price_usd);
                c.low = c.low.min(p.price_usd);
                c.close = p.price_usd;
            })
            .or_insert(Candle {
                time: t,
                open: p.price_usd,
                high: p.price_usd,
                low: p.price_usd,
                close: p.price_usd,
            });
    }
    let all: Vec<Candle> = out.into_values().collect();
    let skip = all.len().saturating_sub(limit);
    all.into_iter().skip(skip).collect()
}

#[derive(Debug, Deserialize)]
struct ChartQuery {
    res: Option<i64>,
}

async fn chart(
    State(state): State<AppState>,
    Path(symbol): Path<String>,
    Query(q): Query<ChartQuery>,
) -> Result<Json<Vec<Candle>>, ApiError> {
    if !is_valid_symbol(&symbol) {
        return Err(ApiError::BadRequest("Invalid symbol format".into()));
    }
    let resolution = q.res.unwrap_or(60);
    if resolution < 1 {
        return Err(ApiError::BadRequest("res must be ≥ 1 minute".into()));
    }
    const LIMIT: i64 = 1000;
    let asset_id = {
        let registry: tokio::sync::RwLockReadGuard<'_, Registry> = state.registry.read().await;
        registry.asset_id_for_symbol(&symbol).map(str::to_string)
    };
    let asset_id = match asset_id {
        Some(id) => id,
        None => return Ok(Json(Vec::new())),
    };
    let interval_sec = resolution * 60;
    let start = Utc::now().timestamp() - interval_sec * LIMIT;
    let points = price_buckets_since(&state.db, &asset_id, start).await?;
    Ok(Json(candles(&points, interval_sec, LIMIT as usize)))
}

/// Node `PRICE_SERIES_WINDOWS`: `(seconds back, resample bucket)`;
/// `None` back = since genesis.
fn series_window(key: &str) -> (&'static str, Option<i64>, i64) {
    match key {
        "7d" => ("7d", Some(604_800), 3_600),
        "90d" => ("90d", Some(7_776_000), 43_200),
        "365d" => ("365d", Some(31_536_000), 172_800),
        "all" => ("all", None, 604_800),
        _ => ("30d", Some(2_592_000), 14_400),
    }
}

fn is_asset_id(s: &str) -> bool {
    s.len() == 66 && s.starts_with("0x") && s[2..].chars().all(|c| c.is_ascii_hexdigit())
}

#[derive(Debug, Deserialize)]
struct SeriesQuery {
    assets: Option<String>,
    window: Option<String>,
}

#[derive(Serialize)]
struct SeriesResponse {
    window: &'static str,
    #[serde(rename = "bucketSec")]
    bucket_sec: i64,
    series: BTreeMap<String, Vec<SeriesOut>>,
}

#[derive(Serialize)]
struct SeriesOut {
    t: i64,
    p: f64,
}

/// Group flat rows into the per-asset map, every requested id present.
pub fn group_series(ids: &[String], rows: Vec<SeriesPoint>) -> BTreeMap<String, Vec<(i64, f64)>> {
    let mut out: BTreeMap<String, Vec<(i64, f64)>> =
        ids.iter().map(|id| (id.clone(), Vec::new())).collect();
    for r in rows {
        out.entry(r.asset_id).or_default().push((r.t, r.p));
    }
    out
}

async fn tools_price_series(
    State(state): State<AppState>,
    Query(q): Query<SeriesQuery>,
) -> Result<Json<SeriesResponse>, ApiError> {
    let ids: Vec<String> = q
        .assets
        .as_deref()
        .unwrap_or("")
        .split(',')
        .map(str::trim)
        .filter(|s| is_asset_id(s))
        .map(str::to_string)
        .take(4)
        .collect();
    if ids.is_empty() {
        return Err(ApiError::BadRequest(
            "no valid asset ids (expect 0x… 64-hex, comma-separated)".into(),
        ));
    }
    let (window, back, bucket_sec) = series_window(q.window.as_deref().unwrap_or("30d"));
    let from_secs = back.map(|b| Utc::now().timestamp() - b).unwrap_or(0);
    let rows = price_series(&state.db, &ids, bucket_sec, from_secs).await?;
    let series = group_series(&ids, rows)
        .into_iter()
        .map(|(id, pts)| {
            (
                id,
                pts.into_iter().map(|(t, p)| SeriesOut { t, p }).collect(),
            )
        })
        .collect();
    Ok(Json(SeriesResponse {
        window,
        bucket_sec,
        series,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pt(h: i64, p: f64) -> PricePoint {
        PricePoint {
            hour_bucket: h * 3600,
            price_usd: p,
        }
    }

    #[test]
    fn symbol_validation_matches_node_regex() {
        assert!(is_valid_symbol("XOR"));
        assert!(is_valid_symbol("xstusd2"));
        assert!(!is_valid_symbol(""));
        assert!(!is_valid_symbol("TOOLONGSYMBOL1"));
        assert!(!is_valid_symbol("X-OR"));
    }

    #[test]
    fn candles_aggregate_ohlc_per_interval() {
        // 4h candles over 6 hourly points.
        let pts = [
            pt(0, 1.0),
            pt(1, 3.0),
            pt(2, 0.5),
            pt(3, 2.0),
            pt(4, 5.0),
            pt(5, 0.0), // ignored
        ];
        let c = candles(&pts, 4 * 3600, 1000);
        assert_eq!(c.len(), 2);
        assert_eq!(
            c[0],
            Candle {
                time: 0,
                open: 1.0,
                high: 3.0,
                low: 0.5,
                close: 2.0
            }
        );
        assert_eq!(c[1].open, 5.0);
        assert_eq!(c[1].time, 4 * 3600);
    }

    #[test]
    fn candles_keep_only_the_last_limit() {
        let pts: Vec<PricePoint> = (0..10).map(|h| pt(h, 1.0 + h as f64)).collect();
        let c = candles(&pts, 3600, 3);
        assert_eq!(c.len(), 3);
        assert_eq!(c[0].time, 7 * 3600);
    }

    #[test]
    fn windows_match_node_table() {
        assert_eq!(series_window("7d"), ("7d", Some(604_800), 3_600));
        assert_eq!(series_window("all"), ("all", None, 604_800));
        assert_eq!(series_window("junk"), ("30d", Some(2_592_000), 14_400));
    }

    #[test]
    fn series_groups_and_keeps_empty_ids() {
        let ids = vec!["0xa".to_string(), "0xb".to_string()];
        let rows = vec![SeriesPoint {
            asset_id: "0xa".into(),
            t: 10,
            p: 1.5,
        }];
        let g = group_series(&ids, rows);
        assert_eq!(g["0xa"], vec![(10, 1.5)]);
        assert!(g["0xb"].is_empty());
    }

    #[test]
    fn asset_id_shape() {
        assert!(is_asset_id(&format!("0x{}", "0".repeat(64))));
        assert!(!is_asset_id("0x1234"));
    }
}
