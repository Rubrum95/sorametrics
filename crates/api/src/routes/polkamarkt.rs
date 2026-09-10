//! Polkamarkt on the legacy contract (`index.js`): `/polkamarkt/state`,
//! `/polkamarkt/markets`, `/polkamarkt/market/:id`,
//! `/polkamarkt/positions/:addr`, `/polkamarkt/buybacks`.
//!
//! Rows come from the `sm.polkamarkt_*` replica with the Node's
//! `pg` typing: `BIGINT` and `NUMERIC` columns are strings, `INTEGER`
//! columns numbers. Live pricing is `PolkamarktAPI.market_state` (implied
//! probability and marginal price in bps, cached 20 s per market);
//! the market detail is enriched from storage: creator fees
//! (`MarketCreatorFees`), liquidity providers and totals, and per-trader
//! positions — from `MarketPositions` marked at the implied probability
//! while Open / Locked, from the indexed trades once Resolved (winning
//! shares) or Cancelled (refund). The status reconcile
//! ([`spawn_reconcile`]) re-reads `Markets` / `MarketResolution` every
//! 5 min, as the Node did, because the weekly governance batch resolves
//! markets without the events our indexer keys on.

use crate::{error::ApiError, AppState};
use axum::{
    extract::{Path, Query, State},
    routing::get,
    Json, Router,
};
use bigdecimal::BigDecimal;
use num_bigint::BigInt;
use serde::{Deserialize, Serialize};
use sorametrics_core::chain::ss58_encode_sora;
use sorametrics_db::sm::pm_reconcile_status;
use sorametrics_substrate::polkamarkt::{mechanism_label, outcome_label, status_label};
use sorametrics_substrate::runtime::sora;
use std::time::Duration;
use subxt::{OnlineClient, SubstrateConfig};

/// Build the sub-router.
pub fn router() -> Router<AppState> {
    Router::new()
        .route("/polkamarkt/state", get(state_route))
        .route("/polkamarkt/markets", get(markets))
        .route("/polkamarkt/market/:id", get(market))
        .route("/polkamarkt/positions/:addr", get(positions))
        .route("/polkamarkt/buybacks", get(buybacks))
}

const STATE_TTL: Duration = Duration::from_secs(20);
const RECONCILE_EVERY: Duration = Duration::from_secs(300);
const RECONCILE_FIRST: Duration = Duration::from_secs(8);
const CREATOR_FEE_BPS: u32 = 50;
const UNAVAILABLE_REASON: &str =
    "Polkamarkt pallet not active in current runtime (needs SORA runtime ≥ 4.8.x via on-chain governance)";

fn big(v: &BigDecimal) -> String {
    v.with_scale(0).to_string()
}

fn opt_big(v: Option<BigDecimal>) -> String {
    v.map(|b| big(&b)).unwrap_or_else(|| "0".into())
}

// ---------------------------------------------------------------------
// Live market state (PolkamarktAPI.market_state)
// ---------------------------------------------------------------------

#[derive(Clone, Serialize, Deserialize)]
struct LiveState {
    mechanism: Option<String>,
    implied_yes_bps: u32,
    implied_no_bps: u32,
    marginal_yes_bps: u32,
    marginal_no_bps: u32,
    dpm_collateral: String,
    virtual_depth: String,
}

async fn live_state(
    state: &AppState,
    client: &OnlineClient<SubstrateConfig>,
    id: u32,
) -> Option<LiveState> {
    let key = format!("pm:state:{id}");
    if let Some(v) = state.cached_scan(&key, STATE_TTL).await {
        return serde_json::from_value(v).ok();
    }
    let out = client
        .runtime_api()
        .at_latest()
        .await
        .ok()?
        .call(sora::apis().polkamarkt_api().market_state(id))
        .await
        .ok()??;
    let s = LiveState {
        mechanism: Some(out.mechanism),
        implied_yes_bps: out.implied_yes_probability_bps,
        implied_no_bps: out.implied_no_probability_bps,
        marginal_yes_bps: out.marginal_yes_price_bps,
        marginal_no_bps: out.marginal_no_price_bps,
        dpm_collateral: out.dpm_collateral.to_string(),
        virtual_depth: out.virtual_depth.to_string(),
    };
    if let Ok(v) = serde_json::to_value(&s) {
        state.store_scan(&key, v).await;
    }
    Some(s)
}

// ---------------------------------------------------------------------
// Rows
// ---------------------------------------------------------------------

#[derive(Serialize, Clone)]
struct MarketRow {
    market_id: String,
    condition_id: String,
    creator: String,
    close_block: i32,
    collateral_asset: String,
    seed_liquidity: String,
    status: String,
    resolution: Option<String>,
    question: Option<String>,
    oracle: Option<String>,
    resolution_source: Option<String>,
    opengov_network: Option<String>,
    opengov_parachain: Option<i32>,
    opengov_track: Option<i32>,
    opengov_referendum: Option<i32>,
    created_at_block: i32,
    created_at_ts: String,
    resolved_at_block: Option<i32>,
    resolved_at_ts: Option<String>,
    mechanism: Option<String>,
    volume: String,
    yes_shares: String,
    no_shares: String,
    coll_yes: String,
    coll_no: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    implied_yes_bps: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    implied_no_bps: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    marginal_yes_bps: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    marginal_no_bps: Option<u32>,
}

impl MarketRow {
    fn apply_live(&mut self, s: &LiveState) {
        self.implied_yes_bps = Some(s.implied_yes_bps);
        self.implied_no_bps = Some(s.implied_no_bps);
        self.marginal_yes_bps = Some(s.marginal_yes_bps);
        self.marginal_no_bps = Some(s.marginal_no_bps);
        if self.mechanism.is_none() {
            self.mechanism = s.mechanism.clone();
        }
    }
}

/// `pmGetMarkets` / `pmGetMarketDetail` market rows with trade aggregates.
async fn market_rows(
    state: &AppState,
    status: Option<&str>,
    market_id: Option<i64>,
    limit: i64,
    offset: i64,
) -> Result<Vec<MarketRow>, ApiError> {
    let rows = sqlx::query!(
        r#"
        SELECT m.market_id, m.condition_id, m.creator, m.close_block, m.collateral_asset,
               m.seed_liquidity, m.status, m.resolution, m.question, m.oracle, m.resolution_source,
               m.opengov_network, m.opengov_parachain, m.opengov_track, m.opengov_referendum,
               m.created_at_block, m.created_at_ts, m.resolved_at_block, m.resolved_at_ts, m.mechanism,
               v.volume AS "volume: BigDecimal", v.yes_shares AS "yes_shares: BigDecimal",
               v.no_shares AS "no_shares: BigDecimal", v.coll_yes AS "coll_yes: BigDecimal",
               v.coll_no AS "coll_no: BigDecimal"
        FROM sm.polkamarkt_markets m
        LEFT JOIN LATERAL (
            SELECT COALESCE(SUM(collateral), 0) AS volume,
                   COALESCE(SUM(CASE WHEN side = 'Buy'  AND outcome = 'Yes' THEN shares ELSE 0 END)
                          - SUM(CASE WHEN side = 'Sell' AND outcome = 'Yes' THEN shares ELSE 0 END), 0) AS yes_shares,
                   COALESCE(SUM(CASE WHEN side = 'Buy'  AND outcome = 'No'  THEN shares ELSE 0 END)
                          - SUM(CASE WHEN side = 'Sell' AND outcome = 'No'  THEN shares ELSE 0 END), 0) AS no_shares,
                   COALESCE(SUM(CASE WHEN outcome = 'Yes' THEN (CASE WHEN side = 'Sell' THEN -collateral ELSE collateral END) ELSE 0 END), 0) AS coll_yes,
                   COALESCE(SUM(CASE WHEN outcome = 'No'  THEN (CASE WHEN side = 'Sell' THEN -collateral ELSE collateral END) ELSE 0 END), 0) AS coll_no
            FROM sm.polkamarkt_trades t WHERE t.market_id = m.market_id
        ) v ON true
        WHERE ($1::text IS NULL OR m.status = $1)
          AND ($2::bigint IS NULL OR m.market_id = $2)
        ORDER BY m.market_id DESC
        LIMIT $3 OFFSET $4
        "#,
        status,
        market_id,
        limit,
        offset
    )
    .fetch_all(&state.db)
    .await?;
    Ok(rows
        .into_iter()
        .map(|r| MarketRow {
            market_id: r.market_id.to_string(),
            condition_id: r.condition_id.to_string(),
            creator: r.creator,
            close_block: r.close_block,
            collateral_asset: r.collateral_asset,
            seed_liquidity: big(&r.seed_liquidity),
            status: r.status,
            resolution: r.resolution,
            question: r.question,
            oracle: r.oracle,
            resolution_source: r.resolution_source,
            opengov_network: r.opengov_network,
            opengov_parachain: r.opengov_parachain,
            opengov_track: r.opengov_track,
            opengov_referendum: r.opengov_referendum,
            created_at_block: r.created_at_block,
            created_at_ts: r.created_at_ts.to_string(),
            resolved_at_block: r.resolved_at_block,
            resolved_at_ts: r.resolved_at_ts.map(|t| t.to_string()),
            mechanism: r.mechanism,
            volume: opt_big(r.volume),
            yes_shares: opt_big(r.yes_shares),
            no_shares: opt_big(r.no_shares),
            coll_yes: opt_big(r.coll_yes),
            coll_no: opt_big(r.coll_no),
            implied_yes_bps: None,
            implied_no_bps: None,
            marginal_yes_bps: None,
            marginal_no_bps: None,
        })
        .collect())
}

// ---------------------------------------------------------------------
// /polkamarkt/state
// ---------------------------------------------------------------------

#[derive(Serialize)]
struct Totals {
    markets: i64,
    active: i64,
    resolved: i64,
    volume: String,
    collaterals: Vec<String>,
}

#[derive(Serialize)]
struct StateResponse {
    available: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    reason: Option<&'static str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    totals: Option<Totals>,
}

async fn available(state: &AppState) -> bool {
    match state.chain.as_ref() {
        Some(c) => c
            .client()
            .await
            .map(|cl| cl.metadata().pallet_by_name("Polkamarkt").is_some())
            .unwrap_or(false),
        None => false,
    }
}

async fn state_route(State(state): State<AppState>) -> Result<Json<StateResponse>, ApiError> {
    if !available(&state).await {
        return Ok(Json(StateResponse {
            available: false,
            reason: Some(UNAVAILABLE_REASON),
            totals: None,
        }));
    }
    let r = sqlx::query!(
        r#"
        SELECT COUNT(*)::bigint AS "markets!",
               COUNT(*) FILTER (WHERE status = 'Open')::bigint AS "active!",
               COUNT(*) FILTER (WHERE status = 'Resolved')::bigint AS "resolved!",
               (SELECT COALESCE(SUM(collateral), 0) FROM sm.polkamarkt_trades) AS "volume: BigDecimal"
        FROM sm.polkamarkt_markets
        "#
    )
    .fetch_one(&state.db)
    .await?;
    let collaterals = sqlx::query_scalar!(
        r#"SELECT DISTINCT collateral_asset FROM sm.polkamarkt_markets WHERE collateral_asset LIKE '0x%'"#
    )
    .fetch_all(&state.db)
    .await?;
    Ok(Json(StateResponse {
        available: true,
        reason: None,
        totals: Some(Totals {
            markets: r.markets,
            active: r.active,
            resolved: r.resolved,
            volume: opt_big(r.volume),
            collaterals,
        }),
    }))
}

// ---------------------------------------------------------------------
// /polkamarkt/markets
// ---------------------------------------------------------------------

#[derive(Deserialize)]
struct MarketsQuery {
    page: Option<i64>,
    limit: Option<i64>,
    status: Option<String>,
}

#[derive(Serialize)]
struct MarketsResponse {
    #[serde(skip_serializing_if = "Option::is_none")]
    available: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    reason: Option<&'static str>,
    data: Vec<MarketRow>,
    total: i64,
    page: i64,
    #[serde(rename = "totalPages")]
    total_pages: i64,
}

async fn markets(
    State(state): State<AppState>,
    Query(q): Query<MarketsQuery>,
) -> Result<Json<MarketsResponse>, ApiError> {
    if !available(&state).await {
        return Ok(Json(MarketsResponse {
            available: Some(false),
            reason: Some(UNAVAILABLE_REASON),
            data: Vec::new(),
            total: 0,
            page: 1,
            total_pages: 1,
        }));
    }
    let page = q.page.filter(|p| *p > 0).unwrap_or(1);
    let limit = q.limit.filter(|l| *l > 0).unwrap_or(25).min(100);
    let status = q.status.filter(|s| !s.is_empty() && s != "all");
    let mut data = market_rows(&state, status.as_deref(), None, limit, (page - 1) * limit).await?;
    let total = sqlx::query_scalar!(
        r#"SELECT COUNT(*)::bigint AS "c!" FROM sm.polkamarkt_markets WHERE ($1::text IS NULL OR status = $1)"#,
        status
    )
    .fetch_one(&state.db)
    .await?;
    if let Some(chain) = state.chain.as_ref() {
        if let Ok(client) = chain.client().await {
            for m in data.iter_mut() {
                if let Ok(id) = m.market_id.parse::<u32>() {
                    if let Some(s) = live_state(&state, &client, id).await {
                        m.apply_live(&s);
                    }
                }
            }
        }
    }
    Ok(Json(MarketsResponse {
        available: None,
        reason: None,
        data,
        total,
        page,
        total_pages: (total as f64 / limit as f64).ceil().max(1.0) as i64,
    }))
}

// ---------------------------------------------------------------------
// /polkamarkt/market/:id
// ---------------------------------------------------------------------

#[derive(Serialize)]
struct TradeRow {
    id: String,
    market_id: String,
    trader: String,
    side: String,
    outcome: String,
    collateral: String,
    shares: String,
    fee: String,
    block: i32,
    ts: String,
    hash: Option<String>,
}

#[derive(Serialize, Clone)]
struct TopPosition {
    trader: String,
    yes_shares: String,
    no_shares: String,
    net_collateral: String,
}

#[derive(Serialize)]
struct ProbPoint {
    bucket: String,
    yes_delta: String,
    no_delta: String,
}

#[derive(Serialize)]
struct Creator {
    address: String,
    #[serde(rename = "feesRaw")]
    fees_raw: String,
    #[serde(rename = "feeBps")]
    fee_bps: u32,
}

#[derive(Serialize)]
struct Provider {
    account: String,
    shares: String,
    contributed: String,
}

#[derive(Serialize)]
struct LiquidityTotalsOut {
    #[serde(rename = "totalShares")]
    total_shares: String,
    #[serde(rename = "totalContributed")]
    total_contributed: String,
}

#[derive(Serialize)]
struct Liquidity {
    providers: Vec<Provider>,
    totals: Option<LiquidityTotalsOut>,
    #[serde(rename = "dpmCollateral")]
    dpm_collateral: Option<String>,
    seed: String,
}

#[derive(Serialize, Clone)]
struct Position {
    trader: String,
    yes_shares: String,
    no_shares: String,
    paid: String,
    value: String,
    pnl: String,
    basis: &'static str,
}

#[derive(Serialize)]
struct MarketDetail {
    available: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    reason: Option<&'static str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    market: Option<MarketRow>,
    #[serde(rename = "recentTrades", skip_serializing_if = "Option::is_none")]
    recent_trades: Option<Vec<TradeRow>>,
    #[serde(rename = "topPositions", skip_serializing_if = "Option::is_none")]
    top_positions: Option<Vec<TopPosition>>,
    #[serde(rename = "probHistory", skip_serializing_if = "Option::is_none")]
    prob_history: Option<Vec<ProbPoint>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    creator: Option<Creator>,
    #[serde(skip_serializing_if = "Option::is_none")]
    liquidity: Option<Liquidity>,
    #[serde(skip_serializing_if = "Option::is_none")]
    positions: Option<Vec<Position>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    data: Option<()>,
}

fn bigint(s: &str) -> BigInt {
    s.parse().unwrap_or_default()
}

/// Node `pmEnrichMarketDetail` positions: marked at the implied
/// probability while trading, settled / refunded afterwards. Sorted by
/// `paid` desc, top 12.
fn settle_positions(
    status: &str,
    resolution: Option<&str>,
    top: &[TopPosition],
    chain_positions: Vec<(String, u128, u128, u128)>,
    implied: (u32, u32),
) -> Vec<Position> {
    let mut rows: Vec<Position> = if status == "Open" || status == "Locked" {
        chain_positions
            .into_iter()
            .map(|(trader, yes, no, paid)| {
                let value = (BigInt::from(yes) * BigInt::from(implied.0)
                    + BigInt::from(no) * BigInt::from(implied.1))
                    / BigInt::from(10_000u32);
                Position {
                    trader,
                    yes_shares: yes.to_string(),
                    no_shares: no.to_string(),
                    paid: paid.to_string(),
                    pnl: (&value - BigInt::from(paid)).to_string(),
                    value: value.to_string(),
                    basis: "mark",
                }
            })
            .collect()
    } else {
        top.iter()
            .map(|p| {
                let yes = bigint(&p.yes_shares);
                let no = bigint(&p.no_shares);
                let paid = bigint(&p.net_collateral);
                let (value, basis) = if status == "Resolved" {
                    let v = match resolution {
                        Some("Yes") => yes.clone(),
                        Some("No") => no.clone(),
                        _ => BigInt::from(0),
                    };
                    (v, "settled")
                } else {
                    (paid.clone(), "refunded")
                };
                Position {
                    trader: p.trader.clone(),
                    yes_shares: yes.to_string(),
                    no_shares: no.to_string(),
                    paid: paid.to_string(),
                    pnl: (&value - &paid).to_string(),
                    value: value.to_string(),
                    basis,
                }
            })
            .collect()
    };
    rows.sort_by(|a, b| bigint(&b.paid).cmp(&bigint(&a.paid)));
    rows.truncate(12);
    rows
}

async fn market(
    State(state): State<AppState>,
    Path(raw_id): Path<String>,
) -> Result<Json<MarketDetail>, ApiError> {
    let id: u32 = raw_id
        .parse()
        .map_err(|_| ApiError::BadRequest("Invalid market id".into()))?;
    if !available(&state).await {
        return Ok(Json(MarketDetail {
            available: false,
            reason: Some(UNAVAILABLE_REASON),
            market: None,
            recent_trades: None,
            top_positions: None,
            prob_history: None,
            creator: None,
            liquidity: None,
            positions: None,
            data: Some(()),
        }));
    }
    let mut rows = market_rows(&state, None, Some(i64::from(id)), 1, 0).await?;
    let Some(mut m) = rows.pop() else {
        return Err(ApiError::NotFound("Market not found".into()));
    };
    let trades = sqlx::query!(
        r#"SELECT id, market_id, trader, side, outcome, collateral, shares, fee, block, ts, hash
           FROM sm.polkamarkt_trades WHERE market_id = $1 ORDER BY ts DESC LIMIT 20"#,
        i64::from(id)
    )
    .fetch_all(&state.db)
    .await?;
    let recent: Vec<TradeRow> = trades
        .into_iter()
        .map(|t| TradeRow {
            id: t.id.to_string(),
            market_id: t.market_id.to_string(),
            trader: t.trader,
            side: t.side,
            outcome: t.outcome,
            collateral: big(&t.collateral),
            shares: big(&t.shares),
            fee: big(&t.fee),
            block: t.block,
            ts: t.ts.to_string(),
            hash: t.hash,
        })
        .collect();
    let top_rows = sqlx::query!(
        r#"
        SELECT trader,
               SUM(CASE WHEN side = 'Buy'  AND outcome = 'Yes' THEN shares ELSE 0 END)
             - SUM(CASE WHEN side = 'Sell' AND outcome = 'Yes' THEN shares ELSE 0 END) AS "yes_shares: BigDecimal",
               SUM(CASE WHEN side = 'Buy'  AND outcome = 'No'  THEN shares ELSE 0 END)
             - SUM(CASE WHEN side = 'Sell' AND outcome = 'No'  THEN shares ELSE 0 END) AS "no_shares: BigDecimal",
               SUM(CASE WHEN side = 'Buy' THEN collateral ELSE -collateral END) AS "net_collateral: BigDecimal"
        FROM sm.polkamarkt_trades WHERE market_id = $1
        GROUP BY trader
        ORDER BY ABS(SUM(CASE WHEN side = 'Buy' THEN collateral ELSE -collateral END)) DESC
        LIMIT 10
        "#,
        i64::from(id)
    )
    .fetch_all(&state.db)
    .await?;
    let top: Vec<TopPosition> = top_rows
        .into_iter()
        .map(|r| TopPosition {
            trader: r.trader,
            yes_shares: opt_big(r.yes_shares),
            no_shares: opt_big(r.no_shares),
            net_collateral: opt_big(r.net_collateral),
        })
        .collect();
    let prob_rows = sqlx::query!(
        r#"
        SELECT date_trunc('hour', to_timestamp(ts / 1000)) AS "bucket: chrono::DateTime<chrono::Utc>",
               SUM(CASE WHEN outcome = 'Yes' AND side = 'Buy'  THEN shares ELSE 0 END)
             - SUM(CASE WHEN outcome = 'Yes' AND side = 'Sell' THEN shares ELSE 0 END) AS "yes_delta: BigDecimal",
               SUM(CASE WHEN outcome = 'No'  AND side = 'Buy'  THEN shares ELSE 0 END)
             - SUM(CASE WHEN outcome = 'No'  AND side = 'Sell' THEN shares ELSE 0 END) AS "no_delta: BigDecimal"
        FROM sm.polkamarkt_trades WHERE market_id = $1
        GROUP BY 1 ORDER BY 1
        "#,
        i64::from(id)
    )
    .fetch_all(&state.db)
    .await?;
    let prob: Vec<ProbPoint> = prob_rows
        .into_iter()
        .map(|r| ProbPoint {
            bucket: r
                .bucket
                .map(|b| b.to_rfc3339_opts(chrono::SecondsFormat::Millis, true))
                .unwrap_or_default(),
            yes_delta: opt_big(r.yes_delta),
            no_delta: opt_big(r.no_delta),
        })
        .collect();

    let mut creator = None;
    let mut liquidity = None;
    let mut positions = None;
    if let Some(chain) = state.chain.as_ref() {
        if let Ok(client) = chain.client().await {
            let live = live_state(&state, &client, id).await;
            if let Some(s) = &live {
                m.apply_live(s);
            }
            let enrich = chain
                .with_client(|client| async move {
                    let at = client.storage().at_latest().await?;
                    let s = sora::storage().polkamarkt();
                    let fees = at.fetch(&s.market_creator_fees(id)).await?.unwrap_or(0);
                    let mut providers = Vec::new();
                    let mut stream = at.iter(s.liquidity_positions_iter1(id)).await?;
                    while let Some(kv) = stream.next().await {
                        let kv = kv?;
                        let n = kv.key_bytes.len();
                        let acc: [u8; 32] = kv.key_bytes[n - 32..].try_into().unwrap_or([0; 32]);
                        providers.push(Provider {
                            account: ss58_encode_sora(&acc),
                            shares: kv.value.shares.to_string(),
                            contributed: kv.value.collateral_contributed.to_string(),
                        });
                    }
                    let totals = at.fetch(&s.liquidity_position_totals(id)).await?.map(|t| {
                        LiquidityTotalsOut {
                            total_shares: t.total_shares.to_string(),
                            total_contributed: t.total_collateral_contributed.to_string(),
                        }
                    });
                    let mut chain_positions = Vec::new();
                    let mut stream = at.iter(s.market_positions_iter1(id)).await?;
                    while let Some(kv) = stream.next().await {
                        let kv = kv?;
                        let n = kv.key_bytes.len();
                        let acc: [u8; 32] = kv.key_bytes[n - 32..].try_into().unwrap_or([0; 32]);
                        chain_positions.push((
                            ss58_encode_sora(&acc),
                            kv.value.yes_shares,
                            kv.value.no_shares,
                            kv.value.net_collateral_paid,
                        ));
                    }
                    Ok((fees, providers, totals, chain_positions))
                })
                .await;
            match enrich {
                Ok((fees, providers, totals, chain_positions)) => {
                    creator = Some(Creator {
                        address: m.creator.clone(),
                        fees_raw: fees.to_string(),
                        fee_bps: CREATOR_FEE_BPS,
                    });
                    liquidity = Some(Liquidity {
                        providers,
                        totals,
                        dpm_collateral: live.as_ref().map(|s| s.dpm_collateral.clone()),
                        seed: m.seed_liquidity.clone(),
                    });
                    let implied = live
                        .as_ref()
                        .map(|s| (s.implied_yes_bps, s.implied_no_bps))
                        .unwrap_or((0, 0));
                    positions = Some(settle_positions(
                        &m.status,
                        m.resolution.as_deref(),
                        &top,
                        chain_positions,
                        implied,
                    ));
                }
                Err(e) => {
                    tracing::warn!(error = %e, market = id, "polkamarkt detail enrich failed")
                }
            }
        }
    }
    Ok(Json(MarketDetail {
        available: true,
        reason: None,
        market: Some(m),
        recent_trades: Some(recent),
        top_positions: Some(top),
        prob_history: Some(prob),
        creator,
        liquidity,
        positions,
        data: None,
    }))
}

// ---------------------------------------------------------------------
// /polkamarkt/positions/:addr
// ---------------------------------------------------------------------

#[derive(Serialize)]
struct UserPosition {
    market_id: String,
    question: Option<String>,
    status: String,
    resolution: Option<String>,
    yes_shares: String,
    no_shares: String,
    net_collateral: String,
}

#[derive(Serialize)]
struct PositionsResponse {
    available: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    reason: Option<&'static str>,
    positions: Vec<UserPosition>,
}

/// Node: `/^cn[A-Za-z0-9]{46,50}$/`.
pub fn is_sora_address_shape(s: &str) -> bool {
    s.starts_with("cn")
        && (48..=52).contains(&s.len())
        && s.chars().all(|c| c.is_ascii_alphanumeric())
}

async fn positions(
    State(state): State<AppState>,
    Path(addr): Path<String>,
) -> Result<Json<PositionsResponse>, ApiError> {
    if !is_sora_address_shape(&addr) {
        return Err(ApiError::BadRequest("Invalid SORA address".into()));
    }
    if !available(&state).await {
        return Ok(Json(PositionsResponse {
            available: false,
            reason: Some(UNAVAILABLE_REASON),
            positions: Vec::new(),
        }));
    }
    let rows = sqlx::query!(
        r#"
        SELECT t.market_id, m.question, m.status, m.resolution,
               SUM(CASE WHEN t.side = 'Buy'  AND t.outcome = 'Yes' THEN t.shares ELSE 0 END)
             - SUM(CASE WHEN t.side = 'Sell' AND t.outcome = 'Yes' THEN t.shares ELSE 0 END) AS "yes_shares: BigDecimal",
               SUM(CASE WHEN t.side = 'Buy'  AND t.outcome = 'No'  THEN t.shares ELSE 0 END)
             - SUM(CASE WHEN t.side = 'Sell' AND t.outcome = 'No'  THEN t.shares ELSE 0 END) AS "no_shares: BigDecimal",
               SUM(CASE WHEN t.side = 'Buy' THEN t.collateral ELSE -t.collateral END) AS "net_collateral: BigDecimal"
        FROM sm.polkamarkt_trades t
        JOIN sm.polkamarkt_markets m ON m.market_id = t.market_id
        WHERE t.trader = $1
        GROUP BY t.market_id, m.question, m.status, m.resolution
        HAVING SUM(CASE WHEN t.side = 'Buy'  AND t.outcome = 'Yes' THEN t.shares ELSE 0 END)
             - SUM(CASE WHEN t.side = 'Sell' AND t.outcome = 'Yes' THEN t.shares ELSE 0 END) > 0
            OR SUM(CASE WHEN t.side = 'Buy'  AND t.outcome = 'No'  THEN t.shares ELSE 0 END)
             - SUM(CASE WHEN t.side = 'Sell' AND t.outcome = 'No'  THEN t.shares ELSE 0 END) > 0
        ORDER BY t.market_id DESC
        "#,
        addr
    )
    .fetch_all(&state.db)
    .await?;
    Ok(Json(PositionsResponse {
        available: true,
        reason: None,
        positions: rows
            .into_iter()
            .map(|r| UserPosition {
                market_id: r.market_id.to_string(),
                question: r.question,
                status: r.status,
                resolution: r.resolution,
                yes_shares: opt_big(r.yes_shares),
                no_shares: opt_big(r.no_shares),
                net_collateral: opt_big(r.net_collateral),
            })
            .collect(),
    }))
}

// ---------------------------------------------------------------------
// /polkamarkt/buybacks
// ---------------------------------------------------------------------

#[derive(Deserialize)]
struct BuybacksQuery {
    limit: Option<i64>,
}

#[derive(Serialize)]
struct BuybackStats {
    #[serde(rename = "sweepCount")]
    sweep_count: i64,
    #[serde(rename = "kusdSpentTotal")]
    kusd_spent_total: String,
    #[serde(rename = "xorBurnedTotal")]
    xor_burned_total: String,
    #[serde(rename = "lastTs")]
    last_ts: Option<i64>,
    #[serde(rename = "lastBlock")]
    last_block: Option<i32>,
}

#[derive(Serialize)]
struct BuybackRow {
    block: i32,
    ts: i64,
    hash: Option<String>,
    #[serde(rename = "kusdSpent")]
    kusd_spent: String,
    #[serde(rename = "xorBurned")]
    xor_burned: String,
}

#[derive(Serialize)]
struct BurnStats {
    count: i64,
    total: String,
}

#[derive(Serialize)]
struct BuybacksResponse {
    available: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    reason: Option<&'static str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pending: Option<&'static str>,
    #[serde(rename = "pendingKusd", skip_serializing_if = "Option::is_none")]
    pending_kusd: Option<String>,
    stats: Option<BuybackStats>,
    history: Vec<BuybackRow>,
    #[serde(skip_serializing_if = "Option::is_none")]
    burns: Option<BurnStats>,
}

async fn buybacks(
    State(state): State<AppState>,
    Query(q): Query<BuybacksQuery>,
) -> Result<Json<BuybacksResponse>, ApiError> {
    if !available(&state).await {
        return Ok(Json(BuybacksResponse {
            available: false,
            reason: Some(UNAVAILABLE_REASON),
            pending: Some("0"),
            pending_kusd: None,
            stats: None,
            history: Vec::new(),
            burns: None,
        }));
    }
    let limit = q.limit.unwrap_or(10).clamp(1, 50);
    let pending = match state.chain.as_ref() {
        Some(chain) => chain
            .with_client(|client| async move {
                client
                    .storage()
                    .at_latest()
                    .await?
                    .fetch(
                        &sora::storage()
                            .polkamarkt()
                            .pending_xor_buyback_collateral(),
                    )
                    .await
            })
            .await
            .ok()
            .flatten()
            .map(|v| v.to_string())
            .unwrap_or_else(|| "0".into()),
        None => "0".into(),
    };
    let s = sqlx::query!(
        r#"SELECT COUNT(*)::bigint AS "sweep_count!",
                  COALESCE(SUM(kusd_spent), 0) AS "kusd: BigDecimal",
                  COALESCE(SUM(xor_burned), 0) AS "xor: BigDecimal",
                  MAX(ts) AS "last_ts", MAX(block) AS "last_block"
           FROM sm.polkamarkt_buybacks"#
    )
    .fetch_one(&state.db)
    .await?;
    let history = sqlx::query!(
        r#"SELECT block, ts, hash, kusd_spent, xor_burned FROM sm.polkamarkt_buybacks ORDER BY ts DESC LIMIT $1"#,
        limit
    )
    .fetch_all(&state.db)
    .await?;
    let b = sqlx::query!(
        r#"SELECT COUNT(*)::bigint AS "cnt!", COALESCE(SUM(amount), 0) AS "total: BigDecimal" FROM sm.polkamarkt_burns"#
    )
    .fetch_one(&state.db)
    .await?;
    Ok(Json(BuybacksResponse {
        available: true,
        reason: None,
        pending: None,
        pending_kusd: Some(pending),
        stats: Some(BuybackStats {
            sweep_count: s.sweep_count,
            kusd_spent_total: opt_big(s.kusd),
            xor_burned_total: opt_big(s.xor),
            last_ts: s.last_ts,
            last_block: s.last_block,
        }),
        history: history
            .into_iter()
            .map(|r| BuybackRow {
                block: r.block,
                ts: r.ts,
                hash: r.hash,
                kusd_spent: big(&r.kusd_spent),
                xor_burned: big(&r.xor_burned),
            })
            .collect(),
        burns: Some(BurnStats {
            count: b.cnt,
            total: opt_big(b.total),
        }),
    }))
}

// ---------------------------------------------------------------------
// Status reconcile (Node pmReconcileMarketsFromChain, 8 s after boot, then every 5 min)
// ---------------------------------------------------------------------

async fn reconcile_once(state: &AppState) -> Result<usize, ApiError> {
    let chain = state.chain.as_ref().ok_or(ApiError::NoChain)?;
    let markets: Vec<(u32, String, String, Option<String>)> = chain
        .with_client(|client| async move {
            let at = client.storage().at_latest().await?;
            let s = sora::storage().polkamarkt();
            let mut out = Vec::new();
            let mut stream = at.iter(s.markets_iter()).await?;
            while let Some(kv) = stream.next().await {
                let kv = kv?;
                let n = kv.key_bytes.len();
                let id = u32::from_le_bytes(kv.key_bytes[n - 4..].try_into().unwrap_or([0; 4]));
                let status = status_label(&kv.value.status).to_string();
                let mechanism = mechanism_label(&kv.value.mechanism).to_string();
                let resolution = if status == "Resolved" {
                    at.fetch(&s.market_resolution(id))
                        .await?
                        .map(|o| outcome_label(&o).to_string())
                } else {
                    None
                };
                out.push((id, status, mechanism, resolution));
            }
            Ok(out)
        })
        .await?;
    let mut changed = 0;
    for (id, status, mechanism, resolution) in markets {
        if pm_reconcile_status(
            &state.db,
            id,
            Some(&status),
            resolution.as_deref(),
            Some(&mechanism),
        )
        .await?
        {
            changed += 1;
        }
    }
    Ok(changed)
}

/// Periodic market lifecycle reconcile from chain storage.
pub fn spawn_reconcile(state: AppState) {
    if state.chain.is_none() {
        return;
    }
    tokio::spawn(async move {
        tokio::time::sleep(RECONCILE_FIRST).await;
        loop {
            match reconcile_once(&state).await {
                Ok(n) if n > 0 => {
                    tracing::info!(changed = n, "polkamarkt reconcile synced markets")
                }
                Ok(_) => {}
                Err(e) => tracing::warn!(error = %e, "polkamarkt reconcile failed"),
            }
            tokio::time::sleep(RECONCILE_EVERY).await;
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    fn top(trader: &str, yes: &str, no: &str, paid: &str) -> TopPosition {
        TopPosition {
            trader: trader.into(),
            yes_shares: yes.into(),
            no_shares: no.into(),
            net_collateral: paid.into(),
        }
    }

    #[test]
    fn settled_positions_match_prod_market_0() {
        // prod market 0 (Resolved No): creator paid 75 KUSD, holds 116.06 NO shares.
        let rows = settle_positions(
            "Resolved",
            Some("No"),
            &[top(
                "a",
                "0",
                "116065855663354195987",
                "75000000000000000000",
            )],
            Vec::new(),
            (0, 0),
        );
        assert_eq!(rows[0].value, "116065855663354195987");
        assert_eq!(rows[0].pnl, "41065855663354195987");
        assert_eq!(rows[0].basis, "settled");
        let refunded = settle_positions(
            "Cancelled",
            None,
            &[top("a", "1", "0", "5")],
            Vec::new(),
            (0, 0),
        );
        assert_eq!(refunded[0].pnl, "0");
        assert_eq!(refunded[0].basis, "refunded");
    }

    #[test]
    fn marked_positions_use_implied_probability() {
        let rows = settle_positions(
            "Open",
            None,
            &[],
            vec![("t".into(), 100, 0, 40)],
            (5000, 5000),
        );
        assert_eq!(rows[0].value, "50");
        assert_eq!(rows[0].pnl, "10");
        assert_eq!(rows[0].basis, "mark");
    }

    #[test]
    fn address_shape_matches_node_regex() {
        assert!(is_sora_address_shape(
            "cnTTL4ihCzLkRT7wyjZA2JQeTjmiwwczxJAsih2eJwZG6ttfq"
        ));
        assert!(!is_sora_address_shape("0x12"));
    }
}
