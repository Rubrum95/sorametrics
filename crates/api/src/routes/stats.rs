//! `/stats/network`, `/stats/overview`, `/stats/header` — aggregates
//! over `sm.swaps` / `sm.transfers` / `sm.bridges` on the legacy
//! contract (`index.js` + `db_pg.js::getNetworkStats/getMarketTrends/
//! getTransferVolume/getFilteredStats`).
//!
//! - `/stats/network`: `{ stats24h, stats7d, tps }` where a stats block
//!   is `{ volume, users, txCount }` over swaps (USD of the input leg,
//!   distinct callers, count) and `tps` is `txCount24h / 86400` as a
//!   2-decimal string.
//! - `/stats/overview?timeframe=`: `{ pegs: {KUSD, XSTUSD, TBCD},
//!   network: {…stats, lpVolume, transferVolume}, trends: [{symbol,
//!   volume}] }` — pegs are the latest quotes, trends the top 5 symbols
//!   by swap USD volume (both legs) in the window.
//! - `/stats/header?timeframe=`: `{ block, swaps, transfers, bridges }`
//!   — the last indexed block and row counts since the window start
//!   (`all` → since genesis).
//!
//! - `/stats/fees?timeframe=`: `[{type, total_xor, total_usd}]` over
//!   `sm.fees` with the legacy outlier caps (`usd_value > 0 AND
//!   usd_value <= 10000 AND amount <= 100` XOR); `all` → since genesis.
//! - `/stats/fees/trend?timeframe=`: `[{bucket, total_usd}]`, hourly
//!   buckets (`YYYY-MM-DD HH24:00:00`) or daily (`YYYY-MM-DD`) for
//!   `7d/1m/1y/all`, rendered in `API_TIME_ZONE` (the Node's `TO_CHAR`
//!   used the DB session zone).
//! - `/stats/network/trend?timeframe=`: `{swaps, transfers, lp,
//!   accounts}` bucketed series (`val` = USD / distinct wallets).
//! - `/stats/stablecoins?timeframe=`: KUSD/XSTUSD/TBCD price, swap and
//!   transfer USD volume, sparkline.
//! - `/stats/trending-tokens?timeframe=`: top 5 `{symbol, volume, logo}`.
//! - `/stats/accumulation?symbol=&timeframe=`: top 10 buyers of a symbol
//!   (`total_bought_usd`, `total_bought_amount`, `swap_count` as text,
//!   `last_buy` ms as text — the legacy pg row shape).
//!
//! KNOWN NON-PARITY: `lpVolume` / `lp` are the net USD of pool
//! liquidity events, which v33 does not index yet (no `poolXYK`
//! decoder). Reported as `0` / `[]` until that family lands — see
//! CLAUDE.md.

use crate::legacy::logo_for;
use crate::routes::tokens::{downsample, timeframe_ms, window_start_bucket, SparkPoint};
use crate::{error::ApiError, AppState};
use axum::{
    extract::{Query, State},
    routing::get,
    Json, Router,
};
use bigdecimal::{BigDecimal, ToPrimitive};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sorametrics_db::sm::get_cursor;
use sorametrics_db::ts::{latest_prices, price_buckets_since};
use std::collections::HashMap;

/// Build the sub-router.
pub fn router() -> Router<AppState> {
    Router::new()
        .route("/stats/network", get(network))
        .route("/stats/overview", get(overview))
        .route("/stats/header", get(header))
        .route("/stats/fees", get(fees))
        .route("/stats/fees/trend", get(fees_trend))
        .route("/stats/network/trend", get(network_trend))
        .route("/stats/stablecoins", get(stablecoins))
        .route("/stats/trending-tokens", get(trending_tokens))
        .route("/stats/accumulation", get(accumulation))
}

/// Legacy outlier caps for fee aggregates (`FEE_USD_CAP`, `FEE_AMOUNT_CAP`).
const FEE_USD_CAP: f64 = 10_000.0;
const FEE_AMOUNT_CAP: f64 = 100.0;

/// Cursor job the live subscriber advances.
const LIVE_JOB: &str = "substrate_live";

/// Window start for a timeframe (`all`/unknown-zero → epoch).
fn window_start(now: DateTime<Utc>, ms: i64) -> DateTime<Utc> {
    if ms <= 0 {
        DateTime::<Utc>::UNIX_EPOCH
    } else {
        now - chrono::Duration::milliseconds(ms)
    }
}

fn to_f64(v: Option<BigDecimal>) -> f64 {
    v.and_then(|d| d.to_f64()).unwrap_or(0.0)
}

/// `{ volume, users, txCount }` over swaps since `since`.
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct NetworkStats {
    volume: f64,
    users: i64,
    #[serde(rename = "txCount")]
    tx_count: i64,
}

async fn network_stats(state: &AppState, since: DateTime<Utc>) -> Result<NetworkStats, ApiError> {
    let row = sqlx::query!(
        r#"
        SELECT COALESCE(SUM(usd_value), 0) AS "volume: BigDecimal",
               COUNT(DISTINCT caller)     AS "users!",
               COUNT(*)                   AS "tx_count!"
        FROM sm.swaps
        WHERE block_timestamp > $1
        "#,
        since,
    )
    .fetch_one(&state.db)
    .await?;
    Ok(NetworkStats {
        volume: to_f64(row.volume),
        users: row.users,
        tx_count: row.tx_count,
    })
}

/// Node: `(txCount / 86400).toFixed(2)`.
fn tps_string(tx_count_24h: i64) -> String {
    format!("{:.2}", tx_count_24h as f64 / 86_400.0)
}

#[derive(Serialize)]
struct NetworkResponse {
    stats24h: NetworkStats,
    stats7d: NetworkStats,
    tps: String,
}

async fn network(State(state): State<AppState>) -> Result<Json<NetworkResponse>, ApiError> {
    let now = Utc::now();
    let stats24h = network_stats(&state, window_start(now, 86_400_000)).await?;
    let stats7d = network_stats(&state, window_start(now, 604_800_000)).await?;
    let tps = tps_string(stats24h.tx_count);
    Ok(Json(NetworkResponse {
        stats24h,
        stats7d,
        tps,
    }))
}

#[derive(Debug, Deserialize)]
struct TimeframeQuery {
    timeframe: Option<String>,
}

#[derive(Serialize)]
struct Pegs {
    #[serde(rename = "KUSD")]
    kusd: f64,
    #[serde(rename = "XSTUSD")]
    xstusd: f64,
    #[serde(rename = "TBCD")]
    tbcd: f64,
}

#[derive(Serialize)]
struct OverviewNetwork {
    volume: f64,
    users: i64,
    #[serde(rename = "txCount")]
    tx_count: i64,
    #[serde(rename = "lpVolume")]
    lp_volume: f64,
    #[serde(rename = "transferVolume")]
    transfer_volume: f64,
}

#[derive(Clone, Debug, PartialEq, Serialize)]
struct Trend {
    symbol: String,
    volume: f64,
}

#[derive(Serialize)]
struct OverviewResponse {
    pegs: Pegs,
    network: OverviewNetwork,
    trends: Vec<Trend>,
}

/// Per-asset swap USD volume (both legs) → top 5 symbols. Symbols are
/// resolved here so duplicate registry ids for one symbol merge, as the
/// Node's `GROUP BY symbol` did.
fn top_trends(per_asset: Vec<(String, f64)>, symbol_of: impl Fn(&str) -> String) -> Vec<Trend> {
    let mut by_symbol: HashMap<String, f64> = HashMap::new();
    for (asset, vol) in per_asset {
        *by_symbol.entry(symbol_of(&asset)).or_insert(0.0) += vol;
    }
    let mut trends: Vec<Trend> = by_symbol
        .into_iter()
        .map(|(symbol, volume)| Trend { symbol, volume })
        .collect();
    trends.sort_by(|a, b| {
        b.volume
            .partial_cmp(&a.volume)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| a.symbol.cmp(&b.symbol))
    });
    trends.truncate(5);
    trends
}

async fn overview(
    State(state): State<AppState>,
    Query(q): Query<TimeframeQuery>,
) -> Result<Json<OverviewResponse>, ApiError> {
    let ms = timeframe_ms(q.timeframe.as_deref().unwrap_or("1d"));
    let now = Utc::now();
    // Node: `TIMEFRAME_MS[tf] || 86400000` — `all` (0) also falls back to 1d.
    let since = window_start(now, if ms == 0 { 86_400_000 } else { ms });

    let net = network_stats(&state, since).await?;

    let transfer_volume = sqlx::query!(
        r#"
        SELECT COALESCE(SUM(usd_value), 0) AS "total: BigDecimal"
        FROM sm.transfers
        WHERE block_timestamp >= $1
        "#,
        since,
    )
    .fetch_one(&state.db)
    .await?
    .total;

    let legs = sqlx::query!(
        r#"
        SELECT asset_id AS "asset_id!", COALESCE(SUM(vol), 0) AS "volume: BigDecimal"
        FROM (
            SELECT input_asset_id  AS asset_id, usd_value        AS vol
            FROM sm.swaps WHERE block_timestamp > $1
            UNION ALL
            SELECT output_asset_id AS asset_id, output_usd_value AS vol
            FROM sm.swaps WHERE block_timestamp > $1
        ) AS legs
        GROUP BY asset_id
        "#,
        since,
    )
    .fetch_all(&state.db)
    .await?;

    let registry = state.registry.read().await;
    let per_asset: Vec<(String, f64)> = legs
        .into_iter()
        .map(|r| (r.asset_id, to_f64(r.volume)))
        .collect();
    let trends = top_trends(per_asset, |id| crate::legacy::symbol_for(&registry, id));

    let peg_ids: Vec<(String, &str)> = ["KUSD", "XSTUSD", "TBCD"]
        .iter()
        .filter_map(|s| {
            registry
                .asset_id_for_symbol(s)
                .map(|id| (id.to_string(), *s))
        })
        .collect();
    drop(registry);
    let ids: Vec<String> = peg_ids.iter().map(|(id, _)| id.clone()).collect();
    let prices: HashMap<String, f64> = latest_prices(&state.db, &ids)
        .await?
        .into_iter()
        .map(|p| (p.asset_id, p.price_usd))
        .collect();
    let peg = |sym: &str| {
        peg_ids
            .iter()
            .find(|(_, s)| *s == sym)
            .and_then(|(id, _)| prices.get(id).copied())
            .unwrap_or(0.0)
    };

    Ok(Json(OverviewResponse {
        pegs: Pegs {
            kusd: peg("KUSD"),
            xstusd: peg("XSTUSD"),
            tbcd: peg("TBCD"),
        },
        network: OverviewNetwork {
            volume: net.volume,
            users: net.users,
            tx_count: net.tx_count,
            lp_volume: 0.0,
            transfer_volume: to_f64(transfer_volume),
        },
        trends,
    }))
}

#[derive(Serialize)]
struct HeaderResponse {
    block: i64,
    swaps: i64,
    transfers: i64,
    bridges: i64,
}

async fn header(
    State(state): State<AppState>,
    Query(q): Query<TimeframeQuery>,
) -> Result<Json<HeaderResponse>, ApiError> {
    let ms = timeframe_ms(q.timeframe.as_deref().unwrap_or("1d"));
    let since = window_start(Utc::now(), ms);
    let counts = sqlx::query!(
        r#"
        SELECT
            (SELECT COUNT(*) FROM sm.swaps     WHERE block_timestamp >= $1) AS "swaps!",
            (SELECT COUNT(*) FROM sm.transfers WHERE block_timestamp >= $1) AS "transfers!",
            (SELECT COUNT(*) FROM sm.bridges   WHERE block_timestamp >= $1) AS "bridges!"
        "#,
        since,
    )
    .fetch_one(&state.db)
    .await?;
    let block = get_cursor(&state.db, LIVE_JOB)
        .await?
        .map(|h| h.0 as i64)
        .unwrap_or(0);
    Ok(Json(HeaderResponse {
        block,
        swaps: counts.swaps,
        transfers: counts.transfers,
        bridges: counts.bridges,
    }))
}

// =============================================================
// Fees
// =============================================================

#[derive(Serialize)]
struct FeeTypeTotals {
    #[serde(rename = "type")]
    fee_type: String,
    total_xor: f64,
    total_usd: f64,
}

async fn fees(
    State(state): State<AppState>,
    Query(q): Query<TimeframeQuery>,
) -> Result<Json<Vec<FeeTypeTotals>>, ApiError> {
    let ms = timeframe_ms(q.timeframe.as_deref().unwrap_or("1d"));
    let since = window_start(Utc::now(), ms);
    let rows = sqlx::query!(
        r#"
        SELECT fee_type,
               SUM(CASE WHEN usd_value > 0 AND usd_value <= $2 AND amount_xor <= $3 THEN amount_xor ELSE 0 END) AS "total_xor: BigDecimal",
               SUM(CASE WHEN usd_value > 0 AND usd_value <= $2 AND amount_xor <= $3 THEN usd_value  ELSE 0 END) AS "total_usd: BigDecimal"
        FROM sm.fees
        WHERE block_timestamp >= $1
        GROUP BY fee_type
        ORDER BY fee_type
        "#,
        since,
        BigDecimal::try_from(FEE_USD_CAP).unwrap_or_default(),
        BigDecimal::try_from(FEE_AMOUNT_CAP).unwrap_or_default(),
    )
    .fetch_all(&state.db)
    .await?;
    Ok(Json(
        rows.into_iter()
            .map(|r| FeeTypeTotals {
                fee_type: r.fee_type,
                total_xor: to_f64(r.total_xor),
                total_usd: to_f64(r.total_usd),
            })
            .collect(),
    ))
}

/// Node `getFeeTrend` / `getNetworkTrend` bucket format: hourly unless
/// the timeframe is one of the long ones.
fn trend_interval_is_day(timeframe: &str) -> bool {
    matches!(timeframe, "7d" | "1m" | "1y" | "all")
}

/// `TO_CHAR` pattern for a bucket.
fn bucket_format(day: bool) -> &'static str {
    if day {
        "YYYY-MM-DD"
    } else {
        "YYYY-MM-DD HH24:00:00"
    }
}

#[derive(Clone, Debug, PartialEq, Serialize)]
struct BucketUsd {
    bucket: String,
    total_usd: f64,
}

async fn fees_trend(
    State(state): State<AppState>,
    Query(q): Query<TimeframeQuery>,
) -> Result<Json<Vec<BucketUsd>>, ApiError> {
    let tf = q.timeframe.as_deref().unwrap_or("1d");
    let since = window_start(Utc::now(), timeframe_ms(tf));
    let fmt = bucket_format(trend_interval_is_day(tf));
    let zone = state.time_zone.name();
    let rows = sqlx::query!(
        r#"
        SELECT TO_CHAR(block_timestamp AT TIME ZONE $2, $3) AS "bucket!",
               SUM(CASE WHEN usd_value > 0 AND usd_value <= $4 AND amount_xor <= $5 THEN usd_value ELSE 0 END) AS "total_usd: BigDecimal"
        FROM sm.fees
        WHERE block_timestamp >= $1
        GROUP BY 1
        ORDER BY 1 ASC
        "#,
        since,
        zone,
        fmt,
        BigDecimal::try_from(FEE_USD_CAP).unwrap_or_default(),
        BigDecimal::try_from(FEE_AMOUNT_CAP).unwrap_or_default(),
    )
    .fetch_all(&state.db)
    .await?;
    Ok(Json(
        rows.into_iter()
            .map(|r| BucketUsd {
                bucket: r.bucket,
                total_usd: to_f64(r.total_usd),
            })
            .collect(),
    ))
}

// =============================================================
// Network trend
// =============================================================

#[derive(Clone, Debug, PartialEq, Serialize)]
struct BucketVal {
    bucket: String,
    val: f64,
}

#[derive(Serialize)]
struct NetworkTrend {
    swaps: Vec<BucketVal>,
    transfers: Vec<BucketVal>,
    lp: Vec<BucketVal>,
    accounts: Vec<BucketVal>,
}

/// Node `/stats/network/trend`: `(start, day-interval)` from the
/// timeframe; default 24h hourly.
fn network_trend_window(timeframe: Option<&str>, now: DateTime<Utc>) -> (DateTime<Utc>, bool) {
    match timeframe {
        Some("7d") => (now - chrono::Duration::days(7), true),
        Some("30d") => (now - chrono::Duration::days(30), true),
        Some("1h") => (now - chrono::Duration::hours(1), false),
        Some("4h") => (now - chrono::Duration::hours(4), false),
        _ => (now - chrono::Duration::hours(24), false),
    }
}

async fn network_trend(
    State(state): State<AppState>,
    Query(q): Query<TimeframeQuery>,
) -> Result<Json<NetworkTrend>, ApiError> {
    let (since, day) = network_trend_window(q.timeframe.as_deref(), Utc::now());
    let fmt = bucket_format(day);
    let zone = state.time_zone.name();

    let swaps = sqlx::query!(
        r#"
        SELECT TO_CHAR(block_timestamp AT TIME ZONE $2, $3) AS "bucket!",
               SUM(usd_value) AS "val: BigDecimal"
        FROM sm.swaps WHERE block_timestamp >= $1
        GROUP BY 1 ORDER BY 1
        "#,
        since,
        zone,
        fmt,
    )
    .fetch_all(&state.db)
    .await?
    .into_iter()
    .map(|r| BucketVal {
        bucket: r.bucket,
        val: to_f64(r.val),
    })
    .collect();

    let transfers = sqlx::query!(
        r#"
        SELECT TO_CHAR(block_timestamp AT TIME ZONE $2, $3) AS "bucket!",
               SUM(usd_value) AS "val: BigDecimal"
        FROM sm.transfers WHERE block_timestamp >= $1
        GROUP BY 1 ORDER BY 1
        "#,
        since,
        zone,
        fmt,
    )
    .fetch_all(&state.db)
    .await?
    .into_iter()
    .map(|r| BucketVal {
        bucket: r.bucket,
        val: to_f64(r.val),
    })
    .collect();

    let accounts = sqlx::query!(
        r#"
        SELECT TO_CHAR(block_timestamp AT TIME ZONE $2, $3) AS "bucket!",
               COUNT(DISTINCT caller) AS "val!"
        FROM sm.swaps WHERE block_timestamp >= $1
        GROUP BY 1 ORDER BY 1
        "#,
        since,
        zone,
        fmt,
    )
    .fetch_all(&state.db)
    .await?
    .into_iter()
    .map(|r| BucketVal {
        bucket: r.bucket,
        val: r.val as f64,
    })
    .collect();

    Ok(Json(NetworkTrend {
        swaps,
        transfers,
        lp: Vec::new(),
        accounts,
    }))
}

// =============================================================
// Stablecoins
// =============================================================

#[derive(Serialize)]
struct StablecoinRow {
    symbol: String,
    price: f64,
    logo: String,
    #[serde(rename = "swapVolume")]
    swap_volume: f64,
    #[serde(rename = "transferVolume")]
    transfer_volume: f64,
    sparkline: Vec<SparkPoint>,
}

/// Node `/stats/stablecoins` window: explicit branches, default 24h.
fn stablecoin_window(timeframe: Option<&str>, now: DateTime<Utc>) -> DateTime<Utc> {
    match timeframe {
        Some("7d") => now - chrono::Duration::days(7),
        Some("30d") => now - chrono::Duration::days(30),
        Some("1h") => now - chrono::Duration::hours(1),
        Some("4h") => now - chrono::Duration::hours(4),
        _ => now - chrono::Duration::hours(24),
    }
}

async fn stablecoins(
    State(state): State<AppState>,
    Query(q): Query<TimeframeQuery>,
) -> Result<Json<Vec<StablecoinRow>>, ApiError> {
    let now = Utc::now();
    let since = stablecoin_window(q.timeframe.as_deref(), now);
    let tf_ms = q
        .timeframe
        .as_deref()
        .map(timeframe_ms)
        .unwrap_or(86_400_000);
    // Node: `TIMEFRAME_MS[tf] || 86400000` — a zero (`all`) falls back too.
    let spark_start = window_start_bucket(
        now.timestamp_millis(),
        if tf_ms == 0 { 86_400_000 } else { tf_ms },
    );

    let registry = state.registry.read().await;
    let coins: Vec<(String, Option<String>, String)> = ["KUSD", "XSTUSD", "TBCD"]
        .iter()
        .map(|s| {
            let id = registry.asset_id_for_symbol(s).map(str::to_string);
            let logo = id
                .as_deref()
                .map(|i| logo_for(&registry, i))
                .unwrap_or_default();
            (s.to_string(), id, logo)
        })
        .collect();
    drop(registry);

    let ids: Vec<String> = coins.iter().filter_map(|(_, id, _)| id.clone()).collect();
    let prices: HashMap<String, f64> = latest_prices(&state.db, &ids)
        .await?
        .into_iter()
        .map(|p| (p.asset_id, p.price_usd))
        .collect();

    let mut out = Vec::with_capacity(3);
    for (symbol, id, logo) in coins {
        let (price, swap_volume, transfer_volume, sparkline) = match &id {
            None => (0.0, 0.0, 0.0, Vec::new()),
            Some(id) => {
                let vols = sqlx::query!(
                    r#"
                    SELECT
                        (SELECT COALESCE(SUM(
                            CASE WHEN input_asset_id  = $1 THEN usd_value        ELSE 0 END
                          + CASE WHEN output_asset_id = $1 THEN output_usd_value ELSE 0 END), 0)
                         FROM sm.swaps
                         WHERE (input_asset_id = $1 OR output_asset_id = $1) AND block_timestamp >= $2) AS "swap_vol: BigDecimal",
                        (SELECT COALESCE(SUM(usd_value), 0)
                         FROM sm.transfers
                         WHERE asset_id = $1 AND block_timestamp >= $2) AS "transfer_vol: BigDecimal"
                    "#,
                    id,
                    since,
                )
                .fetch_one(&state.db)
                .await?;
                let spark = downsample(&price_buckets_since(&state.db, id, spark_start).await?);
                (
                    prices.get(id).copied().unwrap_or(0.0),
                    to_f64(vols.swap_vol),
                    to_f64(vols.transfer_vol),
                    spark,
                )
            }
        };
        out.push(StablecoinRow {
            symbol,
            price,
            logo,
            swap_volume,
            transfer_volume,
            sparkline,
        });
    }
    Ok(Json(out))
}

// =============================================================
// Trending tokens
// =============================================================

#[derive(Serialize)]
struct TrendingToken {
    symbol: String,
    volume: f64,
    logo: String,
}

/// Node `/stats/trending-tokens`: explicit branches, default 24h, `all` → epoch.
fn trending_window(timeframe: Option<&str>, now: DateTime<Utc>) -> DateTime<Utc> {
    match timeframe {
        Some("all") => DateTime::<Utc>::UNIX_EPOCH,
        other => stablecoin_window(other, now),
    }
}

async fn trending_tokens(
    State(state): State<AppState>,
    Query(q): Query<TimeframeQuery>,
) -> Result<Json<Vec<TrendingToken>>, ApiError> {
    let since = trending_window(q.timeframe.as_deref(), Utc::now());
    let legs = sqlx::query!(
        r#"
        SELECT asset_id AS "asset_id!", COALESCE(SUM(vol), 0) AS "volume: BigDecimal"
        FROM (
            SELECT input_asset_id  AS asset_id, usd_value        AS vol
            FROM sm.swaps WHERE block_timestamp >= $1
            UNION ALL
            SELECT output_asset_id AS asset_id, output_usd_value AS vol
            FROM sm.swaps WHERE block_timestamp >= $1
        ) AS legs
        GROUP BY asset_id
        "#,
        since,
    )
    .fetch_all(&state.db)
    .await?;
    let registry = state.registry.read().await;
    let per_asset: Vec<(String, f64)> = legs
        .into_iter()
        .map(|r| (r.asset_id, to_f64(r.volume)))
        .collect();
    let trends = top_trends(per_asset, |id| crate::legacy::symbol_for(&registry, id));
    let out = trends
        .into_iter()
        .map(|t| {
            let logo = registry
                .asset_id_for_symbol(&t.symbol)
                .map(|id| logo_for(&registry, id))
                .unwrap_or_default();
            TrendingToken {
                symbol: t.symbol,
                volume: t.volume,
                logo,
            }
        })
        .collect();
    Ok(Json(out))
}

// =============================================================
// Accumulation (top buyers of a symbol)
// =============================================================

#[derive(Debug, Deserialize)]
struct AccumulationQuery {
    symbol: Option<String>,
    timeframe: Option<String>,
}

#[derive(Serialize)]
struct Accumulator {
    wallet: String,
    total_bought_usd: f64,
    total_bought_amount: f64,
    /// Legacy pg `COUNT(*)` came through as text.
    swap_count: String,
    /// Legacy ms timestamp came through as text.
    last_buy: String,
}

#[derive(Serialize)]
struct AccumulationResponse {
    symbol: String,
    timeframe: String,
    data: Vec<Accumulator>,
}

async fn accumulation(
    State(state): State<AppState>,
    Query(q): Query<AccumulationQuery>,
) -> Result<Json<AccumulationResponse>, ApiError> {
    let symbol = q.symbol.unwrap_or_else(|| "XOR".to_string());
    if symbol.is_empty() || symbol.len() > 12 || !symbol.chars().all(|c| c.is_ascii_alphanumeric())
    {
        return Err(ApiError::BadRequest("Invalid symbol format".into()));
    }
    let timeframe = q.timeframe.unwrap_or_else(|| "24h".to_string());
    let ms = timeframe_ms(&timeframe);
    let since = window_start(Utc::now(), if ms == 0 { 86_400_000 } else { ms });

    let (asset_id, decimals) = {
        let registry = state.registry.read().await;
        match registry.asset_id_for_symbol(&symbol) {
            Some(id) => (
                Some(id.to_string()),
                crate::legacy::decimals_for(&registry, id),
            ),
            None => (None, 18),
        }
    };
    let mut data = Vec::new();
    if let Some(id) = asset_id {
        let scale = BigDecimal::new(num_bigint::BigInt::from(1), -(decimals as i64));
        let rows = sqlx::query!(
            r#"
            SELECT caller,
                   SUM(output_usd_value) AS "total_usd: BigDecimal",
                   SUM(output_amount)    AS "total_amount!: BigDecimal",
                   COUNT(*)              AS "swap_count!",
                   MAX(block_timestamp)  AS "last_buy!"
            FROM sm.swaps
            WHERE output_asset_id = $1 AND block_timestamp > $2
            GROUP BY caller
            ORDER BY SUM(output_usd_value) DESC NULLS LAST
            LIMIT 10
            "#,
            id,
            since,
        )
        .fetch_all(&state.db)
        .await?;
        data = rows
            .into_iter()
            .map(|r| Accumulator {
                wallet: r.caller,
                total_bought_usd: to_f64(r.total_usd),
                total_bought_amount: (r.total_amount / &scale).to_f64().unwrap_or(0.0),
                swap_count: r.swap_count.to_string(),
                last_buy: r.last_buy.timestamp_millis().to_string(),
            })
            .collect();
    }
    Ok(Json(AccumulationResponse {
        symbol,
        timeframe,
        data,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn trend_buckets_are_hourly_unless_long_window() {
        assert!(!trend_interval_is_day("1d"));
        assert!(trend_interval_is_day("7d"));
        assert!(trend_interval_is_day("all"));
        assert_eq!(bucket_format(true), "YYYY-MM-DD");
        assert_eq!(bucket_format(false), "YYYY-MM-DD HH24:00:00");
    }

    #[test]
    fn network_trend_window_matches_node_branches() {
        let now = Utc::now();
        assert_eq!(
            network_trend_window(Some("7d"), now),
            (now - chrono::Duration::days(7), true)
        );
        assert_eq!(
            network_trend_window(Some("1h"), now),
            (now - chrono::Duration::hours(1), false)
        );
        assert_eq!(
            network_trend_window(None, now),
            (now - chrono::Duration::hours(24), false)
        );
        assert_eq!(
            trending_window(Some("all"), now),
            DateTime::<Utc>::UNIX_EPOCH
        );
    }

    #[test]
    fn tps_is_two_decimals_of_daily_average() {
        assert_eq!(tps_string(44), "0.00");
        assert_eq!(tps_string(86_400), "1.00");
        assert_eq!(tps_string(129_600), "1.50");
    }

    #[test]
    fn window_start_all_is_epoch() {
        let now = Utc::now();
        assert_eq!(window_start(now, 0), DateTime::<Utc>::UNIX_EPOCH);
        assert_eq!(
            window_start(now, 3_600_000),
            now - chrono::Duration::hours(1)
        );
    }

    #[test]
    fn trends_merge_duplicate_symbols_and_take_top_five() {
        let per_asset = vec![
            ("0x1".to_string(), 10.0),
            ("0x2".to_string(), 5.0), // also XOR
            ("0x3".to_string(), 7.0),
            ("0x4".to_string(), 1.0),
            ("0x5".to_string(), 2.0),
            ("0x6".to_string(), 3.0),
            ("0x7".to_string(), 0.5),
        ];
        let sym = |id: &str| match id {
            "0x1" | "0x2" => "XOR".to_string(),
            other => other.to_uppercase(),
        };
        let t = top_trends(per_asset, sym);
        assert_eq!(t.len(), 5);
        assert_eq!(
            t[0],
            Trend {
                symbol: "XOR".into(),
                volume: 15.0
            }
        );
        assert_eq!(t[1].symbol, "0X3");
        assert_eq!(t[4].symbol, "0X4");
    }
}
