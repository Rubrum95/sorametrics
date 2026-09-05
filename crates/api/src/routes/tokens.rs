//! `/tokens` — the legacy token list (`index.js` `app.get('/tokens')`).
//!
//! Contract:
//! - `?page` (1-based, default 1), `?limit` (default 20, max 100).
//! - `?symbols=A,B` exact symbol allow-list, else `?search=` (lowercase
//!   substring over symbol, name, asset id).
//! - `?timeframe=` one of `1h 4h 24h 1d 7d 30d 1m 1y all` (default 24h)
//!   for `change24h` and the sparkline window.
//! - `?sparkline=false` skips sparklines; `?onlySparklines=true`
//!   returns `{symbol, sparkline}` rows only.
//! - `{ data, total, page, totalPages }`; rows
//!   `{symbol, name, decimals, assetId, logo, price, change24h, sparkline}`.
//!
//! Mechanism: the list is the official whitelist (`whitelisted` rows of
//! the registry), ordered fixed-top → ecosystem (alphabetical) → the
//! rest (alphabetical). `price` is the latest DAI quote the ingest
//! sampler persisted (`ts.price_latest`; the Node used its in-process
//! 60 s quote cache). `change24h` compares it with the last hourly
//! bucket at or before `now − timeframe`; the sparkline is the hourly
//! buckets since then, downsampled to ≤ 20 points + the last one.
//!
//! `/asset/:asset_id` is a v33 extra (registry row by id).

use crate::{error::ApiError, legacy::page_bounds, AppState};
use axum::{
    extract::{Path, Query, State},
    routing::get,
    Json, Router,
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sorametrics_db::sm::RegistryAsset;
use sorametrics_db::ts::{latest_prices, price_at_or_before, price_buckets_since, PricePoint};
use std::collections::HashMap;

/// Build the tokens sub-router.
pub fn router() -> Router<AppState> {
    Router::new()
        .route("/tokens", get(list_tokens))
        .route("/asset/:asset_id", get(get_asset))
}

/// Fixed order at the top of the list (Node: `FIXED_TOP`).
const FIXED_TOP: [&str; 5] = ["XOR", "TBCD", "VAL", "PSWAP", "KUSD"];
/// Second group, alphabetical (Node: `ECOSYSTEM`).
const ECOSYSTEM: [&str; 11] = [
    "ETH", "DAI", "DEO", "KEN", "KGOLD", "KXOR", "VXOR", "XSTUSD", "XST", "KARMA", "CERES",
];

/// Timeframe keyword → milliseconds (Node: `TIMEFRAME_MS`); unknown → 24h.
pub fn timeframe_ms(key: &str) -> i64 {
    match key {
        "1h" => 3_600_000,
        "4h" => 14_400_000,
        "24h" | "1d" => 86_400_000,
        "7d" => 604_800_000,
        "30d" | "1m" => 2_592_000_000,
        "1y" => 31_536_000_000,
        "all" => 0,
        _ => 86_400_000,
    }
}

/// Hour bucket (unix seconds) of `now − timeframe`, the Node's
/// `pastBucket` / `startBucket`.
pub fn window_start_bucket(now_ms: i64, timeframe_ms: i64) -> i64 {
    ((now_ms - timeframe_ms) / 1000).div_euclid(3600) * 3600
}

/// Node sort: fixed top in order, then ecosystem alphabetical, then the
/// rest alphabetical.
fn sort_rank(symbol: &str) -> (u8, usize, &str) {
    if let Some(i) = FIXED_TOP.iter().position(|s| *s == symbol) {
        return (0, i, symbol);
    }
    if ECOSYSTEM.contains(&symbol) {
        return (1, 0, symbol);
    }
    (2, 0, symbol)
}

/// Sort a token list the way `/tokens` does (stable for equal ranks).
pub fn sort_tokens(assets: &mut [&RegistryAsset]) {
    assets.sort_by(|a, b| sort_rank(&a.symbol).cmp(&sort_rank(&b.symbol)));
}

/// Percent change from the last bucket at/before the window start to
/// the current price; `0` when there is no usable past price.
pub fn percent_change(current: f64, past: Option<f64>) -> f64 {
    match past {
        Some(old) if old > 0.0 => (current - old) / old * 100.0,
        _ => 0.0,
    }
}

/// Node `getSparkline`: keep positive prices, ≤ 20 evenly-stepped
/// points, always ending with the last one.
pub fn downsample(points: &[PricePoint]) -> Vec<SparkPoint> {
    let prices: Vec<&PricePoint> = points.iter().filter(|p| p.price_usd > 0.0).collect();
    let to_point = |p: &PricePoint| SparkPoint {
        value: p.price_usd,
        time: p.hour_bucket * 1000,
    };
    if prices.is_empty() {
        return Vec::new();
    }
    if prices.len() <= 20 {
        return prices.into_iter().map(to_point).collect();
    }
    let step = prices.len() / 20;
    let mut sampled: Vec<&PricePoint> = prices
        .iter()
        .enumerate()
        .filter(|(i, _)| i % step == 0)
        .map(|(_, p)| *p)
        .take(20)
        .collect();
    let last = prices[prices.len() - 1];
    if sampled.last().map(|p| std::ptr::eq(*p, last)) != Some(true) {
        sampled.push(last);
    }
    sampled.into_iter().map(to_point).collect()
}

/// One sparkline point (`{value, time}` with `time` in ms).
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct SparkPoint {
    value: f64,
    time: i64,
}

#[derive(Debug, Default, Deserialize)]
struct TokensQuery {
    page: Option<i64>,
    limit: Option<i64>,
    search: Option<String>,
    symbols: Option<String>,
    timeframe: Option<String>,
    sparkline: Option<String>,
    #[serde(rename = "onlySparklines")]
    only_sparklines: Option<String>,
}

#[derive(Serialize)]
struct TokenRow {
    symbol: String,
    name: String,
    decimals: i16,
    #[serde(rename = "assetId")]
    asset_id: String,
    logo: String,
    price: f64,
    change24h: f64,
    sparkline: Vec<SparkPoint>,
}

#[derive(Serialize)]
struct SparkRow {
    symbol: String,
    sparkline: Vec<SparkPoint>,
}

#[derive(Serialize)]
#[serde(untagged)]
enum TokenItem {
    Full(TokenRow),
    Spark(SparkRow),
}

#[derive(Serialize)]
struct Page<T> {
    data: Vec<T>,
    total: i64,
    page: i64,
    #[serde(rename = "totalPages")]
    total_pages: i64,
}

async fn list_tokens(
    State(state): State<AppState>,
    Query(q): Query<TokensQuery>,
) -> Result<Json<Page<TokenItem>>, ApiError> {
    let page = q.page.unwrap_or(1);
    if page < 1 {
        return Err(ApiError::BadRequest("page must be ≥ 1".into()));
    }
    let limit = q.limit.unwrap_or(20);
    if !(1..=100).contains(&limit) {
        return Err(ApiError::BadRequest(
            "limit must be between 1 and 100".into(),
        ));
    }
    let tf_ms = timeframe_ms(q.timeframe.as_deref().unwrap_or("24h"));
    let include_sparkline = q.sparkline.as_deref() != Some("false");
    let only_sparklines = q.only_sparklines.as_deref() == Some("true");
    let search = q
        .search
        .as_deref()
        .map(str::to_lowercase)
        .filter(|s| !s.is_empty());
    let symbols: Option<Vec<String>> = q.symbols.as_deref().map(|s| {
        s.split(',')
            .map(str::trim)
            .filter(|x| !x.is_empty())
            .map(str::to_string)
            .collect()
    });

    let registry = state.registry.read().await;
    let mut filtered: Vec<&RegistryAsset> = registry
        .whitelisted()
        .into_iter()
        .filter(|a| match (&symbols, &search) {
            (Some(list), _) => list.contains(&a.symbol),
            (None, Some(needle)) => {
                a.symbol.to_lowercase().contains(needle)
                    || a.name
                        .as_deref()
                        .unwrap_or("")
                        .to_lowercase()
                        .contains(needle)
                    || a.asset_id.to_lowercase().contains(needle)
            }
            (None, None) => true,
        })
        .collect();
    sort_tokens(&mut filtered);

    let total = filtered.len() as i64;
    let (total_pages, _) = page_bounds(total, limit, page);
    let start = ((page - 1) * limit) as usize;
    let paginated: Vec<RegistryAsset> = filtered
        .into_iter()
        .skip(start)
        .take(limit as usize)
        .cloned()
        .collect();
    drop(registry);

    let now_ms = Utc::now().timestamp_millis();
    let start_bucket = window_start_bucket(now_ms, tf_ms);
    let ids: Vec<String> = paginated.iter().map(|a| a.asset_id.clone()).collect();
    let prices: HashMap<String, f64> = if only_sparklines {
        HashMap::new()
    } else {
        latest_prices(&state.db, &ids)
            .await?
            .into_iter()
            .map(|p| (p.asset_id, p.price_usd))
            .collect()
    };

    let mut data = Vec::with_capacity(paginated.len());
    for a in paginated {
        let sparkline = if include_sparkline || only_sparklines {
            downsample(&price_buckets_since(&state.db, &a.asset_id, start_bucket).await?)
        } else {
            Vec::new()
        };
        if only_sparklines {
            data.push(TokenItem::Spark(SparkRow {
                symbol: a.symbol,
                sparkline,
            }));
            continue;
        }
        let price = prices.get(&a.asset_id).copied().unwrap_or(0.0);
        let past = price_at_or_before(&state.db, &a.asset_id, start_bucket).await?;
        data.push(TokenItem::Full(TokenRow {
            symbol: a.symbol,
            name: a.name.unwrap_or_default(),
            decimals: a.decimals,
            asset_id: a.asset_id,
            logo: a.logo.unwrap_or_default(),
            price,
            change24h: percent_change(price, past),
            sparkline,
        }));
    }

    Ok(Json(Page {
        data,
        total,
        page,
        total_pages,
    }))
}

// =============================================================
// /asset/:asset_id (v33 extra)
// =============================================================

#[derive(Serialize)]
struct AssetItem {
    asset_id: String,
    symbol: String,
    name: Option<String>,
    decimals: i16,
    logo: Option<String>,
    whitelisted: bool,
    updated_at: DateTime<Utc>,
}

async fn get_asset(
    State(state): State<AppState>,
    Path(asset_id): Path<String>,
) -> Result<Json<AssetItem>, ApiError> {
    // Asset IDs are 0x + 64 hex chars — same shape constraint as wallet
    // addresses. Reuse the validator for consistency.
    let asset_id = crate::util::validate_address(&asset_id)?;

    let item = sqlx::query_as!(
        AssetItem,
        r#"
        SELECT asset_id, symbol, name, decimals, logo, whitelisted, updated_at
        FROM sm.asset_registry
        WHERE asset_id = $1
        "#,
        asset_id,
    )
    .fetch_optional(&state.db)
    .await?
    .ok_or_else(|| ApiError::NotFound(format!("asset {asset_id}")))?;

    Ok(Json(item))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn asset(sym: &str) -> RegistryAsset {
        RegistryAsset {
            asset_id: format!("0x{sym}"),
            symbol: sym.to_string(),
            name: None,
            decimals: 18,
            logo: None,
            whitelisted: true,
        }
    }

    #[test]
    fn sort_is_fixed_top_then_ecosystem_then_alpha() {
        let rows = ["ZZZ", "DAI", "VAL", "AAA", "XOR", "CERES", "KUSD"].map(asset);
        let mut refs: Vec<&RegistryAsset> = rows.iter().collect();
        sort_tokens(&mut refs);
        let order: Vec<&str> = refs.iter().map(|a| a.symbol.as_str()).collect();
        assert_eq!(order, ["XOR", "VAL", "KUSD", "CERES", "DAI", "AAA", "ZZZ"]);
    }

    #[test]
    fn timeframes_match_the_node_table() {
        assert_eq!(timeframe_ms("24h"), 86_400_000);
        assert_eq!(timeframe_ms("1d"), 86_400_000);
        assert_eq!(timeframe_ms("1m"), timeframe_ms("30d"));
        assert_eq!(timeframe_ms("all"), 0);
        assert_eq!(timeframe_ms("bogus"), 86_400_000);
    }

    #[test]
    fn window_start_is_hour_aligned() {
        // 2026-09-05T09:47:31Z minus 24h → 2026-09-04T09:00:00Z
        assert_eq!(
            window_start_bucket(1_788_601_651_000, 86_400_000),
            1_788_512_400
        );
    }

    #[test]
    fn percent_change_guards_missing_past() {
        assert_eq!(percent_change(110.0, Some(100.0)), 10.0);
        assert_eq!(percent_change(110.0, Some(0.0)), 0.0);
        assert_eq!(percent_change(110.0, None), 0.0);
    }

    fn pts(n: usize) -> Vec<PricePoint> {
        (0..n)
            .map(|i| PricePoint {
                hour_bucket: 3600 * i as i64,
                price_usd: 1.0 + i as f64,
            })
            .collect()
    }

    #[test]
    fn downsample_keeps_small_series_verbatim() {
        let s = downsample(&pts(20));
        assert_eq!(s.len(), 20);
        assert_eq!(
            s[0],
            SparkPoint {
                value: 1.0,
                time: 0
            }
        );
        assert_eq!(s[19].time, 19 * 3600 * 1000);
    }

    #[test]
    fn downsample_steps_and_appends_last() {
        // 45 points: step 2 → indexes 0,2,…,38 (20 points) + last (44).
        let s = downsample(&pts(45));
        assert_eq!(s.len(), 21);
        assert_eq!(s[1].time, 2 * 3600 * 1000);
        assert_eq!(s[20].time, 44 * 3600 * 1000);
    }

    #[test]
    fn downsample_drops_non_positive_prices() {
        let mut p = pts(3);
        p[1].price_usd = 0.0;
        assert_eq!(downsample(&p).len(), 2);
    }
}
