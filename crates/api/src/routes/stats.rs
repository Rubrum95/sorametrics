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
//! KNOWN NON-PARITY: `lpVolume` is the net USD of pool liquidity
//! events, which v33 does not index yet (no `poolXYK` decoder). It is
//! reported as `0` until that family lands — see CLAUDE.md.

use crate::routes::tokens::timeframe_ms;
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
use sorametrics_db::ts::latest_prices;
use std::collections::HashMap;

/// Build the sub-router.
pub fn router() -> Router<AppState> {
    Router::new()
        .route("/stats/network", get(network))
        .route("/stats/overview", get(overview))
        .route("/stats/header", get(header))
}

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

#[cfg(test)]
mod tests {
    use super::*;

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
