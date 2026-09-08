//! Burn tracker on the legacy contract (`index.js` "BURN TRACKER
//! ENDPOINTS" + `/stats/fee-burns-live`).
//!
//! Symbols are `XOR | VAL | PSWAP | TBCD | KUSD` (400 `Invalid symbol`
//! otherwise). Supply of a token (`getTokenTotalSupply`, cached 60 s):
//! XOR = on-chain `balances.totalIssuance / 1e18`; the others = the MOF
//! figure (`mof.sora.org/qty/<sym>`, KUSD as `xstusd`), else the latest
//! stored snapshot.
//!
//! - `/burns/series/:symbol?days` (≤ 90): daily + cumulative burns of
//!   XOR / VAL / KUSD / TBCD from the per-block aggregates; PSWAP is
//!   not tracked (`points: []` + note).
//! - `/burns/supply/:symbol`: `{ symbol, totalSupply, price, marketCap }`.
//! - `/burns/supply-history/:symbol?timeframe=4h|1d|7d|1m|1y|all`
//!   (`getSupplyHistory`): non-XOR = MOF snapshots, with the daily
//!   on-chain issuance before the MOF era shifted by a linearly
//!   interpolated offset so the junction is seamless; XOR = snapshots
//!   when the window starts within 2 days, else the daily on-chain
//!   points. One point per day (first kept).
//! - `/burns/stats/:symbol`: per timeframe (24h / 7d / 30d / all). XOR:
//!   snapshot delta plus `feeBased = fees × 0.20` (which replaces
//!   `totalBurned` when positive); other tokens: snapshot delta, and
//!   `genesis − current` for `all` when a genesis supply is known.
//!   `denomFactor` = `denomination.denominator` as text.
//! - `/burns/fee-flow`: 24 h fees split with the runtime weights.
//! - `/burns/holders/:symbol?page`: the holders scan, 15 per page, with
//!   `totalSupply` and the identity display `name`.
//! - `/stats/fee-burns-live?window=1h|4h|6h|24h|7d|30d`: sums of the
//!   per-block aggregates in the window (cached 15 s).

use crate::routes::chain_state::{cached_or_scan, scan_holders, Holder};
use crate::routes::identity::display_names;
use crate::routes::stats::fee_totals_since;
use crate::{error::ApiError, AppState};
use axum::{
    extract::{Path, Query, State},
    routing::get,
    Json, Router,
};
use bigdecimal::{BigDecimal, ToPrimitive};
use chrono::{DateTime, Utc};
use num_bigint::BigInt;
use serde::{Deserialize, Serialize};
use sorametrics_core::mof::{fetch_mof_supply, supply_token};
use sorametrics_db::ts::latest_prices;
use sorametrics_substrate::fee_burns_agg::{weights_for, Weights};
use sorametrics_substrate::runtime::sora;
use std::collections::{HashMap, HashSet};
use std::time::Duration;

/// Build the sub-router.
pub fn router() -> Router<AppState> {
    Router::new()
        .route("/burns/series/:symbol", get(series))
        .route("/burns/supply/:symbol", get(supply))
        .route("/burns/supply-history/:symbol", get(supply_history))
        .route("/burns/stats/:symbol", get(stats))
        .route("/burns/fee-flow", get(fee_flow))
        .route("/stats/fee-burns-live", get(fee_burns_live))
}

/// The holders scan, on the long-timeout router.
pub fn scan_router() -> Router<AppState> {
    Router::new().route("/burns/holders/:symbol", get(holders))
}

const SUPPLY_TTL: Duration = Duration::from_secs(60);
const HOLDERS_TTL: Duration = Duration::from_secs(300);
const HOLDERS_PAGE: usize = 15;
const FEE_BURNS_LIVE_TTL: Duration = Duration::from_secs(15);
const DAY_MS: i64 = 86_400_000;

/// Node `BURN_TOKENS` + `MOF_SYMBOL_MAP`: `(asset id, MOF symbol)`.
fn token(symbol: &str) -> Option<(&'static str, &'static str)> {
    supply_token(symbol)
}

/// Node `GENESIS_SUPPLY`: `(supply, timestamp ms)` — 2021-04-26.
fn genesis(symbol: &str) -> Option<(f64, i64)> {
    match symbol {
        "VAL" => Some((100_000_000.0, 1_619_395_200_000)),
        "PSWAP" => Some((10_000_000_000.0, 1_619_395_200_000)),
        _ => None,
    }
}

fn valid_symbol(raw: &str) -> Result<String, ApiError> {
    let s = raw.to_uppercase();
    if token(&s).is_some() {
        Ok(s)
    } else {
        Err(ApiError::BadRequest("Invalid symbol".into()))
    }
}

async fn price_of(state: &AppState, asset_id: &str) -> Result<f64, ApiError> {
    Ok(latest_prices(&state.db, &[asset_id.to_string()])
        .await?
        .into_iter()
        .find(|p| p.asset_id == asset_id)
        .map(|p| p.price_usd)
        .unwrap_or(0.0))
}

async fn latest_snapshot(state: &AppState, symbol: &str) -> Result<Option<f64>, ApiError> {
    Ok(sqlx::query_scalar!(
        r#"SELECT total_supply FROM sm.supply_snapshots WHERE symbol = $1 ORDER BY ts DESC LIMIT 1"#,
        symbol
    )
    .fetch_optional(&state.db)
    .await?)
}

async fn on_chain_xor_issuance(state: &AppState) -> Option<f64> {
    let chain = state.chain.as_ref()?;
    let raw = chain
        .with_client(|client| async move {
            client
                .storage()
                .at_latest()
                .await?
                .fetch(&sora::storage().balances().total_issuance())
                .await
        })
        .await
        .ok()??;
    Some(planck_to_f64(raw))
}

fn planck_to_f64(raw: u128) -> f64 {
    (BigDecimal::from(BigInt::from(raw)) / BigDecimal::new(BigInt::from(1), -18))
        .to_f64()
        .unwrap_or(0.0)
}

/// Node `getTokenTotalSupply` (cached 60 s per symbol).
async fn token_supply(state: &AppState, symbol: &str) -> Result<Option<f64>, ApiError> {
    let key = format!("burns:supply:{symbol}");
    if let Some(v) = state.cached_scan(&key, SUPPLY_TTL).await {
        return Ok(v.as_f64());
    }
    let (_, mof) = token(symbol).ok_or_else(|| ApiError::BadRequest("Invalid symbol".into()))?;
    let mut supply = None;
    if symbol == "XOR" {
        supply = on_chain_xor_issuance(state).await;
    }
    if supply.is_none() {
        if let Ok(http) = reqwest::Client::builder()
            .timeout(Duration::from_secs(10))
            .build()
        {
            supply = fetch_mof_supply(&http, symbol, mof).await;
        }
    }
    if supply.is_none() && symbol != "XOR" {
        supply = latest_snapshot(state, symbol).await?;
    }
    if let Some(v) = supply {
        state.store_scan(&key, serde_json::json!(v)).await;
    }
    Ok(supply)
}

// ---------------------------------------------------------------------
// /burns/series/:symbol
// ---------------------------------------------------------------------

#[derive(Debug, Deserialize)]
struct SeriesQuery {
    days: Option<i64>,
}

#[derive(Serialize)]
struct SeriesPoint {
    ts: i64,
    daily: f64,
    cumulative: f64,
}

#[derive(Serialize)]
struct SeriesResponse {
    symbol: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    days: Option<i64>,
    points: Vec<SeriesPoint>,
    #[serde(skip_serializing_if = "Option::is_none")]
    note: Option<&'static str>,
}

/// Node `getFeeBurnsSeries` + cumulative fold.
fn cumulative(daily: Vec<(i64, f64)>) -> Vec<SeriesPoint> {
    let mut cum = 0.0;
    daily
        .into_iter()
        .map(|(ts, d)| {
            cum += d;
            SeriesPoint {
                ts,
                daily: d,
                cumulative: cum,
            }
        })
        .collect()
}

async fn series(
    State(state): State<AppState>,
    Path(symbol): Path<String>,
    Query(q): Query<SeriesQuery>,
) -> Result<Json<SeriesResponse>, ApiError> {
    let symbol = valid_symbol(&symbol)?;
    let days = q.days.filter(|d| *d > 0).unwrap_or(30).min(90);
    if symbol == "PSWAP" {
        return Ok(Json(SeriesResponse {
            symbol,
            days: None,
            points: Vec::new(),
            note: Some("not tracked by fee-burns indexer"),
        }));
    }
    let since = Utc::now().timestamp_millis() - days * DAY_MS;
    let rows = sqlx::query!(
        r#"
        SELECT (ts / 86400000)::bigint AS "day!",
               COALESCE(SUM(remint_xor_burned), 0)::float8 AS "xor!",
               COALESCE(SUM(remint_val_burned), 0)::float8 AS "val!",
               COALESCE(SUM(remint_kusd_burned), 0)::float8 AS "kusd!",
               COALESCE(SUM(remint_tbcd_burned), 0)::float8 AS "tbcd!"
        FROM sm.fee_burns_aggregate
        WHERE ts >= $1
        GROUP BY 1 ORDER BY 1
        "#,
        since
    )
    .fetch_all(&state.db)
    .await?;
    let daily: Vec<(i64, f64)> = rows
        .into_iter()
        .map(|r| {
            let v = match symbol.as_str() {
                "XOR" => r.xor,
                "VAL" => r.val,
                "KUSD" => r.kusd,
                _ => r.tbcd,
            };
            (r.day * DAY_MS, v)
        })
        .collect();
    Ok(Json(SeriesResponse {
        symbol,
        days: Some(days),
        points: cumulative(daily),
        note: None,
    }))
}

// ---------------------------------------------------------------------
// /burns/supply/:symbol
// ---------------------------------------------------------------------

#[derive(Serialize)]
struct SupplyResponse {
    symbol: String,
    #[serde(rename = "totalSupply")]
    total_supply: Option<f64>,
    price: f64,
    #[serde(rename = "marketCap")]
    market_cap: f64,
}

async fn supply(
    State(state): State<AppState>,
    Path(symbol): Path<String>,
) -> Result<Json<SupplyResponse>, ApiError> {
    let symbol = valid_symbol(&symbol)?;
    let (asset_id, _) =
        token(&symbol).ok_or_else(|| ApiError::BadRequest("Invalid symbol".into()))?;
    let total_supply = token_supply(&state, &symbol).await?;
    let price = price_of(&state, asset_id).await?;
    Ok(Json(SupplyResponse {
        symbol,
        total_supply,
        price,
        market_cap: total_supply.unwrap_or(0.0) * price,
    }))
}

// ---------------------------------------------------------------------
// /burns/supply-history/:symbol
// ---------------------------------------------------------------------

#[derive(Debug, Deserialize)]
struct TimeframeQuery {
    timeframe: Option<String>,
}

#[derive(Serialize, Clone, Debug, PartialEq)]
struct SupplyPoint {
    timestamp: i64,
    total_supply: f64,
}

/// Node `msMap` of `/burns/supply-history`.
fn history_window_ms(tf: &str) -> Option<i64> {
    match tf {
        "4h" => Some(14_400_000),
        "1d" => Some(86_400_000),
        "7d" => Some(604_800_000),
        "1m" => Some(2_592_000_000),
        "1y" => Some(31_536_000_000),
        "all" => Some(0),
        _ => None,
    }
}

async fn snapshots_since(
    state: &AppState,
    symbol: &str,
    start_ms: i64,
) -> Result<Vec<SupplyPoint>, ApiError> {
    let start = DateTime::from_timestamp_millis(start_ms).unwrap_or(DateTime::<Utc>::UNIX_EPOCH);
    let rows = sqlx::query!(
        r#"SELECT ts, total_supply FROM sm.supply_snapshots WHERE symbol = $1 AND ts >= $2 ORDER BY ts ASC"#,
        symbol,
        start
    )
    .fetch_all(&state.db)
    .await?;
    Ok(rows
        .into_iter()
        .map(|r| SupplyPoint {
            timestamp: r.ts.timestamp_millis(),
            total_supply: r.total_supply,
        })
        .collect())
}

/// Daily on-chain points (both legacy sources) with `ts_secs` in
/// `[from, to)`; `to = None` = open-ended.
async fn chain_points(
    state: &AppState,
    symbol: &str,
    from_secs: i64,
    to_secs: Option<i64>,
) -> Result<Vec<SupplyPoint>, ApiError> {
    let rows = sqlx::query!(
        r#"SELECT ts_secs, total_supply FROM sm.supply_history
           WHERE symbol = $1 AND ts_secs >= $2 AND ($3::bigint IS NULL OR ts_secs < $3)
           ORDER BY ts_secs ASC"#,
        symbol,
        from_secs,
        to_secs
    )
    .fetch_all(&state.db)
    .await?;
    Ok(rows
        .into_iter()
        .map(|r| SupplyPoint {
            timestamp: r.ts_secs * 1000,
            total_supply: r.total_supply,
        })
        .collect())
}

/// One point per day, first kept (input sorted ascending).
fn dedupe_daily(mut rows: Vec<SupplyPoint>) -> Vec<SupplyPoint> {
    rows.sort_by_key(|r| r.timestamp);
    let mut seen = HashSet::new();
    rows.into_iter()
        .filter(|r| seen.insert(r.timestamp.div_euclid(DAY_MS)))
        .collect()
}

/// Node: shift the pre-MOF on-chain points by a linearly interpolated
/// offset so the last chain point meets the first MOF value.
fn shift_to_junction(
    chain: &[SupplyPoint],
    mof_first: f64,
    genesis_ts: Option<i64>,
) -> Vec<SupplyPoint> {
    let Some(last) = chain.last() else {
        return Vec::new();
    };
    let total_offset = last.total_supply - mof_first;
    let genesis_ts = genesis_ts.unwrap_or(chain[0].timestamp);
    let span = last.timestamp - genesis_ts;
    chain
        .iter()
        .map(|r| {
            let progress = if span > 0 {
                (r.timestamp - genesis_ts) as f64 / span as f64
            } else {
                1.0
            };
            SupplyPoint {
                timestamp: r.timestamp,
                total_supply: r.total_supply - total_offset * progress,
            }
        })
        .collect()
}

/// Node `getSupplyHistory`.
async fn get_supply_history(
    state: &AppState,
    symbol: &str,
    start_ms: i64,
) -> Result<Vec<SupplyPoint>, ApiError> {
    let start_secs = start_ms.div_euclid(1000);
    let now = Utc::now().timestamp_millis();
    let mut rows: Vec<SupplyPoint>;
    if symbol != "XOR" {
        rows = snapshots_since(state, symbol, start_ms).await?;
        let mof_start = rows.first().map(|r| r.timestamp).unwrap_or(now);
        if start_ms < mof_start {
            if let Some(mof_first) = rows.first().map(|r| r.total_supply) {
                let chain = dedupe_daily(
                    chain_points(state, symbol, start_secs, Some(mof_start.div_euclid(1000)))
                        .await?,
                );
                if !chain.is_empty() {
                    let genesis_ts = genesis(symbol).map(|(_, ts)| ts);
                    let mut historical = shift_to_junction(&chain, mof_first, genesis_ts);
                    historical.append(&mut rows);
                    rows = historical;
                }
            }
        }
    } else if start_ms > now - 2 * DAY_MS {
        rows = snapshots_since(state, symbol, start_ms).await?;
    } else {
        rows = chain_points(state, symbol, start_secs, None).await?;
    }
    Ok(dedupe_daily(rows))
}

async fn supply_history(
    State(state): State<AppState>,
    Path(symbol): Path<String>,
    Query(q): Query<TimeframeQuery>,
) -> Result<Json<Vec<SupplyPoint>>, ApiError> {
    let symbol = valid_symbol(&symbol)?;
    let ms = history_window_ms(q.timeframe.as_deref().unwrap_or("7d"))
        .ok_or_else(|| ApiError::BadRequest("Invalid timeframe".into()))?;
    let start = if ms == 0 {
        0
    } else {
        Utc::now().timestamp_millis() - ms
    };
    Ok(Json(get_supply_history(&state, &symbol, start).await?))
}

// ---------------------------------------------------------------------
// /burns/stats/:symbol
// ---------------------------------------------------------------------

#[derive(Serialize, Default)]
struct BurnStat {
    #[serde(rename = "totalBurned")]
    total_burned: f64,
    #[serde(rename = "startSupply", skip_serializing_if = "Option::is_none")]
    start_supply: Option<f64>,
    #[serde(rename = "endSupply", skip_serializing_if = "Option::is_none")]
    end_supply: Option<f64>,
    #[serde(rename = "startTime", skip_serializing_if = "Option::is_none")]
    start_time: Option<String>,
    #[serde(rename = "endTime", skip_serializing_if = "Option::is_none")]
    end_time: Option<String>,
    #[serde(rename = "feeBased", skip_serializing_if = "Option::is_none")]
    fee_based: Option<f64>,
    #[serde(rename = "feeBasedUsd", skip_serializing_if = "Option::is_none")]
    fee_based_usd: Option<f64>,
    #[serde(rename = "totalBurnedUsd", skip_serializing_if = "Option::is_none")]
    total_burned_usd: Option<f64>,
    #[serde(rename = "totalBurn", skip_serializing_if = "Option::is_none")]
    total_burn: Option<f64>,
    #[serde(rename = "firstSupply", skip_serializing_if = "Option::is_none")]
    first_supply: Option<f64>,
    #[serde(rename = "lastSupply", skip_serializing_if = "Option::is_none")]
    last_supply: Option<f64>,
    #[serde(rename = "genesisSupply", skip_serializing_if = "Option::is_none")]
    genesis_supply: Option<f64>,
}

/// First / last snapshot in the window: `(ts ms, supply)`.
async fn snapshot_bounds(
    state: &AppState,
    symbol: &str,
    start_ms: i64,
) -> Result<Option<((i64, f64), (i64, f64))>, ApiError> {
    let start = DateTime::from_timestamp_millis(start_ms).unwrap_or(DateTime::<Utc>::UNIX_EPOCH);
    let first = sqlx::query!(
        r#"SELECT ts, total_supply FROM sm.supply_snapshots WHERE symbol = $1 AND ts >= $2 ORDER BY ts ASC LIMIT 1"#,
        symbol,
        start
    )
    .fetch_optional(&state.db)
    .await?;
    let last = sqlx::query!(
        r#"SELECT ts, total_supply FROM sm.supply_snapshots WHERE symbol = $1 AND ts >= $2 ORDER BY ts DESC LIMIT 1"#,
        symbol,
        start
    )
    .fetch_optional(&state.db)
    .await?;
    Ok(match (first, last) {
        (Some(f), Some(l)) => Some((
            (f.ts.timestamp_millis(), f.total_supply),
            (l.ts.timestamp_millis(), l.total_supply),
        )),
        _ => None,
    })
}

/// Node `getBurnStats`.
async fn burn_stats(state: &AppState, symbol: &str, start_ms: i64) -> Result<BurnStat, ApiError> {
    Ok(match snapshot_bounds(state, symbol, start_ms).await? {
        Some((first, last)) if first.0 != last.0 => BurnStat {
            total_burned: first.1 - last.1,
            start_supply: Some(first.1),
            end_supply: Some(last.1),
            start_time: Some(first.0.to_string()),
            end_time: Some(last.0.to_string()),
            ..Default::default()
        },
        _ => BurnStat {
            total_burned: 0.0,
            start_supply: Some(0.0),
            end_supply: Some(0.0),
            ..Default::default()
        },
    })
}

#[derive(Serialize)]
struct StatsResponse {
    symbol: String,
    #[serde(rename = "currentSupply")]
    current_supply: Option<f64>,
    stats: HashMap<&'static str, BurnStat>,
    #[serde(rename = "denomFactor")]
    denom_factor: String,
}

async fn denom_factor(state: &AppState) -> String {
    let Some(chain) = state.chain.as_ref() else {
        return "1".into();
    };
    chain
        .with_client(|client| async move {
            client
                .storage()
                .at_latest()
                .await?
                .fetch(&sora::storage().denomination().denominator())
                .await
        })
        .await
        .ok()
        .flatten()
        .filter(|d| *d > 0)
        .map(|d| d.to_string())
        .unwrap_or_else(|| "1".into())
}

async fn stats(
    State(state): State<AppState>,
    Path(symbol): Path<String>,
) -> Result<Json<StatsResponse>, ApiError> {
    let symbol = valid_symbol(&symbol)?;
    let (asset_id, _) =
        token(&symbol).ok_or_else(|| ApiError::BadRequest("Invalid symbol".into()))?;
    let now = Utc::now();
    let now_ms = now.timestamp_millis();
    let timeframes: [(&str, i64); 4] = [
        ("24h", 86_400_000),
        ("7d", 604_800_000),
        ("30d", 2_592_000_000),
        ("all", 0),
    ];
    let price = price_of(&state, asset_id).await?;
    let current = token_supply(&state, &symbol).await?;
    let mut out = HashMap::new();
    for (tf, ms) in timeframes {
        let start_ms = if ms == 0 { 0 } else { now_ms - ms };
        let stat = if symbol == "XOR" {
            let mut s = burn_stats(&state, &symbol, start_ms).await?;
            let since =
                DateTime::from_timestamp_millis(start_ms).unwrap_or(DateTime::<Utc>::UNIX_EPOCH);
            let (fees_xor, fees_usd) = fee_totals_since(&state, since).await?;
            let fee_burn = fees_xor * 0.20;
            let fee_burn_usd = fees_usd * 0.20;
            s.fee_based = Some(fee_burn);
            s.fee_based_usd = Some(fee_burn_usd);
            if fee_burn > 0.0 {
                s.total_burned = fee_burn;
                s.total_burned_usd = Some(fee_burn_usd);
            }
            s
        } else if tf == "all" && genesis(&symbol).is_some() && current.is_some() {
            let (g, _) = genesis(&symbol).unwrap_or((0.0, 0));
            let burned = g - current.unwrap_or(0.0);
            BurnStat {
                total_burned: burned,
                total_burned_usd: Some(burned * price),
                total_burn: Some(burned),
                genesis_supply: Some(g),
                ..Default::default()
            }
        } else {
            let (first, last) = snapshot_bounds(&state, &symbol, start_ms)
                .await?
                .map(|(f, l)| (f.1, l.1))
                .unwrap_or((0.0, 0.0));
            let burned = (first - last).max(0.0);
            BurnStat {
                total_burned: burned,
                total_burned_usd: Some(burned * price),
                total_burn: Some(burned),
                first_supply: Some(first),
                last_supply: Some(last),
                ..Default::default()
            }
        };
        out.insert(tf, stat);
    }
    Ok(Json(StatsResponse {
        symbol,
        current_supply: current,
        stats: out,
        denom_factor: denom_factor(&state).await,
    }))
}

// ---------------------------------------------------------------------
// /burns/fee-flow
// ---------------------------------------------------------------------

#[derive(Serialize)]
struct WeightsOut {
    #[serde(rename = "ref")]
    referrer: u32,
    xor: u32,
    val: u32,
    kusd: u32,
    total: u32,
}

impl From<Weights> for WeightsOut {
    fn from(w: Weights) -> Self {
        Self {
            referrer: w.referrer,
            xor: w.xor,
            val: w.val,
            kusd: w.kusd,
            total: w.total,
        }
    }
}

#[derive(Serialize)]
struct Distribution {
    #[serde(rename = "xorBurn")]
    xor_burn: f64,
    #[serde(rename = "valStaking", skip_serializing_if = "Option::is_none")]
    val_staking: Option<f64>,
    #[serde(rename = "valBurn")]
    val_burn: f64,
    #[serde(rename = "kusdBuyback", skip_serializing_if = "Option::is_none")]
    kusd_buyback: Option<f64>,
    referrer: f64,
}

/// Node `/burns/fee-flow` distribution model.
fn distribution(total_xor: f64, spec_version: u32) -> (Distribution, Weights) {
    let w = weights_for(spec_version);
    if spec_version >= 128 {
        let t = f64::from(w.total);
        let referrer = total_xor * f64::from(w.referrer) / t;
        let xor_burn_direct = total_xor * f64::from(w.xor) / t;
        let xor_to_val_bucket = total_xor * f64::from(w.val) / t;
        let xor_burn_remint = xor_to_val_bucket * 0.40;
        let xor_to_val = xor_to_val_bucket * (1.0 - 0.40);
        let val_staking = xor_to_val * (1.0 - 0.10);
        let val_burn = xor_to_val * 0.10;
        (
            Distribution {
                xor_burn: xor_burn_direct + xor_burn_remint,
                val_staking: Some(val_staking),
                val_burn,
                kusd_buyback: None,
                referrer,
            },
            w,
        )
    } else {
        let val_slice = total_xor * 0.50;
        let xor_burn_extra = val_slice * 0.01;
        let after = val_slice - xor_burn_extra;
        (
            Distribution {
                xor_burn: total_xor * 0.20 + xor_burn_extra,
                val_staking: None,
                val_burn: after * 0.61,
                kusd_buyback: Some(after * 0.39 + total_xor * 0.05),
                referrer: total_xor * 0.10,
            },
            Weights {
                referrer: 10,
                xor: 20,
                val: 50,
                kusd: 5,
                total: 85,
            },
        )
    }
}

#[derive(Serialize)]
struct FeeFlow {
    #[serde(rename = "specVersion")]
    spec_version: u32,
    #[serde(rename = "totalXorFees")]
    total_xor_fees: f64,
    distribution: Distribution,
    weights: WeightsOut,
    supplies: HashMap<&'static str, Option<f64>>,
}

async fn spec_version(state: &AppState) -> u32 {
    match state.chain.as_ref() {
        Some(c) => c
            .client()
            .await
            .map(|cl| cl.runtime_version().spec_version)
            .unwrap_or(0),
        None => 0,
    }
}

async fn fee_flow(State(state): State<AppState>) -> Result<Json<FeeFlow>, ApiError> {
    let since = Utc::now() - chrono::Duration::milliseconds(DAY_MS);
    let (total_xor, _) = fee_totals_since(&state, since).await?;
    let sv = spec_version(&state).await;
    let (dist, w) = distribution(total_xor, sv);
    let mut supplies = HashMap::new();
    for sym in ["XOR", "VAL", "PSWAP", "TBCD", "KUSD"] {
        supplies.insert(sym, token_supply(&state, sym).await?);
    }
    Ok(Json(FeeFlow {
        spec_version: sv,
        total_xor_fees: total_xor,
        distribution: dist,
        weights: w.into(),
        supplies,
    }))
}

// ---------------------------------------------------------------------
// /burns/holders/:symbol
// ---------------------------------------------------------------------

#[derive(Debug, Deserialize)]
struct PageQuery {
    page: Option<i64>,
}

#[derive(Serialize)]
struct NamedHolder {
    #[serde(flatten)]
    holder: Holder,
    name: Option<String>,
}

#[derive(Serialize)]
struct HoldersResponse {
    page: i64,
    #[serde(rename = "totalHolders")]
    total_holders: usize,
    #[serde(rename = "totalPages")]
    total_pages: usize,
    #[serde(rename = "totalSupply")]
    total_supply: f64,
    data: Vec<NamedHolder>,
}

async fn holders(
    State(state): State<AppState>,
    Path(symbol): Path<String>,
    Query(q): Query<PageQuery>,
) -> Result<Json<HoldersResponse>, ApiError> {
    let symbol = valid_symbol(&symbol)?;
    let (asset_id, _) =
        token(&symbol).ok_or_else(|| ApiError::BadRequest("Asset ID not resolved".into()))?;
    let page = q.page.unwrap_or(1).max(1);
    let key = format!("holders:{asset_id}");
    let id = asset_id.to_string();
    let list: Vec<Holder> = cached_or_scan(&state, &key, HOLDERS_TTL, move |st| async move {
        scan_holders(&st, &id).await
    })
    .await?;
    let total_holders = list.len();
    let total_pages = total_holders.div_ceil(HOLDERS_PAGE);
    let start = ((page - 1) as usize) * HOLDERS_PAGE;
    let page_items: Vec<Holder> = list.into_iter().skip(start).take(HOLDERS_PAGE).collect();
    let addrs: Vec<String> = page_items
        .iter()
        .map(|h| h.address.clone())
        .filter(|a| a.len() > 40)
        .collect();
    let names = display_names(&state, &addrs).await;
    let total_supply = token_supply(&state, &symbol).await?.unwrap_or(0.0);
    Ok(Json(HoldersResponse {
        page,
        total_holders,
        total_pages,
        total_supply,
        data: page_items
            .into_iter()
            .map(|h| NamedHolder {
                name: names.get(&h.address).cloned(),
                holder: h,
            })
            .collect(),
    }))
}

// ---------------------------------------------------------------------
// /stats/fee-burns-live
// ---------------------------------------------------------------------

#[derive(Debug, Deserialize)]
struct WindowQuery {
    window: Option<String>,
}

/// Node `FEE_BURNS_WINDOWS`.
fn window_seconds(w: &str) -> Option<i64> {
    match w {
        "1h" => Some(3600),
        "4h" => Some(14_400),
        "6h" => Some(21_600),
        "24h" => Some(86_400),
        "7d" => Some(604_800),
        "30d" => Some(2_592_000),
        _ => None,
    }
}

#[derive(Serialize, Deserialize)]
struct FeesOut {
    #[serde(rename = "totalXor")]
    total_xor: f64,
}

#[derive(Serialize, Deserialize)]
struct ReferrerOut {
    #[serde(rename = "paidXor")]
    paid_xor: f64,
    #[serde(rename = "redirectedToKusdXor")]
    redirected_to_kusd_xor: f64,
}

#[derive(Serialize, Deserialize)]
struct BurnsOut {
    xor: f64,
    val: f64,
    kusd: f64,
}

#[derive(Serialize, Deserialize)]
struct FeeBurnsLive {
    window: String,
    #[serde(rename = "startTime")]
    start_time: i64,
    #[serde(rename = "endTime")]
    end_time: i64,
    seconds: i64,
    weights: serde_json::Value,
    fees: FeesOut,
    referrer: ReferrerOut,
    burns: BurnsOut,
    blocks: i64,
    #[serde(rename = "firstTs")]
    first_ts: i64,
    #[serde(rename = "lastTs")]
    last_ts: i64,
}

async fn fee_burns_live(
    State(state): State<AppState>,
    Query(q): Query<WindowQuery>,
) -> Result<Json<FeeBurnsLive>, ApiError> {
    let window = q.window.unwrap_or_else(|| "24h".into());
    let seconds = window_seconds(&window)
        .ok_or_else(|| ApiError::BadRequest("Invalid window. Use 1h, 4h, 6h, 24h, 7d.".into()))?;
    let key = format!("burns:live:{window}");
    if let Some(v) = state.cached_scan(&key, FEE_BURNS_LIVE_TTL).await {
        return serde_json::from_value(v)
            .map(Json)
            .map_err(|e| ApiError::Internal(e.to_string()));
    }
    let now = Utc::now().timestamp_millis();
    let since = now - seconds * 1000;
    let r = sqlx::query!(
        r#"
        SELECT COALESCE(SUM(fees_paid_xor), 0)::float8 AS "fees!",
               COALESCE(SUM(ref_paid_xor), 0)::float8 AS "ref_paid!",
               COALESCE(SUM(ref_redirected_xor), 0)::float8 AS "ref_redirected!",
               COALESCE(SUM(remint_xor_burned), 0)::float8 AS "xor!",
               COALESCE(SUM(remint_val_burned), 0)::float8 AS "val!",
               COALESCE(SUM(remint_kusd_burned), 0)::float8 AS "kusd!",
               COUNT(*)::bigint AS "rows!",
               COALESCE(MIN(ts), 0)::bigint AS "min_ts!",
               COALESCE(MAX(ts), 0)::bigint AS "max_ts!"
        FROM sm.fee_burns_aggregate
        WHERE ts >= $1
        "#,
        since
    )
    .fetch_one(&state.db)
    .await?;
    let w: WeightsOut = weights_for(spec_version(&state).await).into();
    let out = FeeBurnsLive {
        window,
        start_time: since,
        end_time: now,
        seconds,
        weights: serde_json::to_value(w).map_err(|e| ApiError::Internal(e.to_string()))?,
        fees: FeesOut { total_xor: r.fees },
        referrer: ReferrerOut {
            paid_xor: r.ref_paid,
            redirected_to_kusd_xor: r.ref_redirected,
        },
        burns: BurnsOut {
            xor: r.xor,
            val: r.val,
            kusd: r.kusd,
        },
        blocks: r.rows,
        first_ts: r.min_ts,
        last_ts: r.max_ts,
    };
    let json = serde_json::to_value(&out).map_err(|e| ApiError::Internal(e.to_string()))?;
    state.store_scan(&key, json).await;
    Ok(Json(out))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn p(ts: i64, v: f64) -> SupplyPoint {
        SupplyPoint {
            timestamp: ts,
            total_supply: v,
        }
    }

    #[test]
    fn symbols_and_windows_match_node() {
        assert_eq!(valid_symbol("val").unwrap(), "VAL");
        assert!(valid_symbol("DAI").is_err());
        assert_eq!(history_window_ms("1m"), Some(2_592_000_000));
        assert_eq!(history_window_ms("24h"), None);
        assert_eq!(window_seconds("30d"), Some(2_592_000));
    }

    #[test]
    fn cumulative_folds_daily_burns() {
        let c = cumulative(vec![(0, 2.0), (DAY_MS, 3.0)]);
        assert_eq!(c[1].cumulative, 5.0);
        assert_eq!(c[1].daily, 3.0);
    }

    #[test]
    fn daily_dedupe_keeps_the_first_point_of_each_day() {
        let d = dedupe_daily(vec![p(DAY_MS + 5, 2.0), p(DAY_MS + 1, 1.0), p(0, 9.0)]);
        assert_eq!(d.len(), 2);
        assert_eq!(d[1].total_supply, 1.0);
    }

    #[test]
    fn junction_shift_interpolates_from_genesis() {
        // Chain says 110 at the junction, MOF says 100: offset 10 grows
        // linearly from genesis (0) to the junction (day 10).
        let chain = vec![p(0, 100.0), p(5 * DAY_MS, 105.0), p(10 * DAY_MS, 110.0)];
        let s = shift_to_junction(&chain, 100.0, Some(0));
        assert_eq!(s[0].total_supply, 100.0);
        assert_eq!(s[1].total_supply, 100.0);
        assert_eq!(s[2].total_supply, 100.0);
    }

    #[test]
    fn distribution_matches_prod_for_spec_130() {
        // prod 2026-09-08: totalXorFees 34.233944079115965
        let (d, w) = distribution(34.233944079115965, 130);
        assert_eq!(w.total, 85);
        assert!((d.xor_burn - 20.540366447469577).abs() < 1e-9);
        assert!((d.val_staking.unwrap() - 8.699449318928291).abs() < 1e-9);
        assert!((d.val_burn - 0.9666054798809212).abs() < 1e-9);
        assert!((d.referrer - 4.027522832837172).abs() < 1e-9);
    }
}
