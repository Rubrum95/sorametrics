//! Blocks, transactions, accounts, domains, assets, definitions,
//! instructions, permissions and the wallet drill-down.

use super::{clamp_page, js_parse_int, opt_str, torii, PageQuery};
use crate::error::ApiError;
use crate::AppState;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};
use chrono::{DateTime, SecondsFormat, Utc};
use serde::Deserialize;
use serde_json::{json, Value};
use sorametrics_core::minamoto::hash32_from_hex;
use sorametrics_db::mn;

/// Build the sub-router.
pub fn router() -> Router<AppState> {
    Router::new()
        .route("/blocks", get(blocks))
        .route("/blocks/stats", get(blocks_stats))
        .route("/transactions", get(transactions))
        .route("/transactions/stats", get(transactions_stats))
        .route("/transactions/fee-sponsorship", get(fee_sponsorship))
        .route("/wallet/:addr/info", get(wallet_info))
        .route("/accounts", get(accounts))
        .route("/accounts/stats", get(accounts_stats))
        .route("/accounts/:id/assets", get(account_assets))
        .route("/accounts/:id/transactions", get(account_transactions))
        .route("/accounts/:id/permissions", get(account_permissions))
        .route("/domains", get(domains))
        .route("/domains/stats", get(domains_stats))
        .route("/assets", get(assets))
        .route("/asset-definitions", get(asset_definitions))
        .route("/asset-definitions/stats", get(asset_definitions_stats))
        .route("/asset/:id_or_alias", get(asset))
        .route("/asset/:id_or_alias/holders", get(asset_holders))
        .route("/block/:id_or_height", get(block))
        .route("/tx/:hash", get(tx))
        .route("/instructions", get(instructions))
        .route("/instructions/kinds", get(instruction_kinds))
        .route("/transfers/stats", get(transfers_stats))
        .route("/permissions/stats", get(permissions_stats))
        .route("/permissions/grants", get(permission_grants))
        .route("/lane-staking/lifecycle", get(lane_staking_lifecycle))
}

/// The Node's `res.status(404).json({ error })`.
fn node_404(msg: &str) -> Response {
    (StatusCode::NOT_FOUND, Json(json!({ "error": msg }))).into_response()
}

fn iso(t: DateTime<Utc>) -> String {
    t.to_rfc3339_opts(SecondsFormat::Millis, true)
}

async fn blocks(
    State(state): State<AppState>,
    Query(q): Query<PageQuery>,
) -> Result<Json<Value>, ApiError> {
    let (page, per_page) = clamp_page(&q, 20);
    Ok(Json(json!(
        mn::list_blocks(&state.db, page, per_page).await?
    )))
}

async fn blocks_stats(State(state): State<AppState>) -> Result<Json<Value>, ApiError> {
    Ok(Json(json!(mn::get_blocks_stats(&state.db).await?)))
}

/// `/transactions` query.
#[derive(Debug, Deserialize)]
pub struct TxQuery {
    /// Page.
    #[serde(flatten)]
    pub page: PageQuery,
    /// Status filter.
    pub status: Option<String>,
    /// Block filter.
    pub block: Option<String>,
    /// Authority filter.
    pub authority: Option<String>,
}

async fn transactions(
    State(state): State<AppState>,
    Query(q): Query<TxQuery>,
) -> Result<Json<Value>, ApiError> {
    let (page, per_page) = clamp_page(&q.page, 20);
    let f = mn::TxListFilter {
        status: opt_str(&q.status),
        block: opt_str(&q.block).and_then(|b| js_parse_int(&b)),
        authority: opt_str(&q.authority),
    };
    Ok(Json(json!(
        mn::list_transactions(&state.db, page, per_page, &f).await?
    )))
}

async fn transactions_stats(State(state): State<AppState>) -> Result<Json<Value>, ApiError> {
    Ok(Json(mn::get_transactions_stats(&state.db).await?))
}

async fn fee_sponsorship(State(state): State<AppState>) -> Result<Json<Value>, ApiError> {
    Ok(Json(mn::get_fee_sponsorship_stats(&state.db).await?))
}

fn clamp01(x: f64) -> f64 {
    x.clamp(0.0, 100.0)
}

/// `/wallet/:addr/info` — mirror of v2's `/wallet/info/:addr` over `mn.*`.
async fn wallet_info(
    State(state): State<AppState>,
    Path(addr): Path<String>,
) -> Result<Response, ApiError> {
    if addr.is_empty() {
        return Ok(node_404("wallet not found"));
    }
    let db = &state.db;
    let tx = mn::wallet_tx_stats(db, &addr).await?;
    let inc = mn::wallet_incoming(db, &addr).await?;
    let (out_count, _out_volume, out_max, out_avg) = mn::wallet_out_transfers(db, &addr).await?;
    let top_tokens = mn::wallet_top_tokens(db, &addr).await?;
    let kinds = mn::instruction_kinds_of(db, &addr).await?;
    let domains = mn::domains_owned_by(db, &addr).await?;
    let asset_defs = mn::asset_definitions_owned_by(db, &addr).await?;
    let (claims_received, xor_claimed) = mn::wallet_claims_received(db, &addr).await?;
    let assets_held = mn::assets_held_count(db, &addr).await?;

    let incoming_total = inc.in_transfers + inc.in_mints;
    let unique_tokens = top_tokens.len();
    let dates: Vec<DateTime<Utc>> = [
        tx.first_tx_at,
        tx.last_tx_at,
        inc.in_first_at,
        inc.in_last_at,
    ]
    .into_iter()
    .flatten()
    .collect();
    let first_at = dates.iter().min().map(|t| iso(*t));
    let last_at = dates.iter().max().map(|t| iso(*t));

    let xor_row = top_tokens.iter().find(|r| {
        r.alias.as_deref() == Some("xor#universal")
            || r.name.as_deref().map(|n| n.to_lowercase()) == Some("xor".to_string())
    });
    let xor_volume = xor_row
        .map(|r| r.volume.clone())
        .unwrap_or_else(|| "0".to_string());
    let xor_volume_num: f64 = xor_volume.parse().unwrap_or(0.0);
    let total_activity = (tx.tx_count + incoming_total) as f64;
    let volume_score = clamp01((xor_volume_num + 1.0).log10() * 12.0);
    let frequency_score = clamp01((total_activity + 1.0).log10() * 25.0);
    let diversity_score = clamp01(unique_tokens as f64 * 12.0);
    let whale_total = ((volume_score + frequency_score + diversity_score) / 3.0).round() as i64;
    let whale_class = match whale_total {
        t if t >= 80 => "WHALE",
        t if t >= 60 => "ORCA",
        t if t >= 40 => "DOLPHIN",
        t if t >= 20 => "FISH",
        _ => "SHRIMP",
    };
    let success_rate = (tx.tx_count > 0).then(|| tx.tx_committed as f64 / tx.tx_count as f64);

    let body = json!({
        "first_at": first_at,
        "last_at": last_at,
        "first_tx_at": tx.first_tx_at.map(iso),
        "last_tx_at": tx.last_tx_at.map(iso),
        "tx_count": tx.tx_count,
        "tx_committed": tx.tx_committed,
        "success_rate": success_rate,
        "days_active": tx.days_active.max(inc.in_days),
        "incoming": {
            "transfers": inc.in_transfers,
            "mints": inc.in_mints,
            "total": incoming_total,
            "first_at": inc.in_first_at.map(iso),
            "last_at": inc.in_last_at.map(iso),
        },
        "transfers": {
            "out_count": out_count,
            "in_count": inc.in_transfers,
            "volume_xor": xor_volume,
            "max_xor": out_max,
            "avg_xor": out_avg,
        },
        "unique_tokens": unique_tokens,
        "top_tokens": top_tokens,
        "assets_held": assets_held,
        "instruction_kinds": kinds,
        "domains_owned": domains,
        "asset_defs_created": asset_defs,
        "cross_chain": {
            "claims_received": claims_received,
            "xor_claimed": xor_claimed,
        },
        "whale_score": {
            "total": whale_total,
            "class": whale_class,
            "volume": volume_score.round() as i64,
            "frequency": frequency_score.round() as i64,
            "diversity": diversity_score.round() as i64,
        },
    });
    Ok(Json(body).into_response())
}

async fn accounts(
    State(state): State<AppState>,
    Query(q): Query<PageQuery>,
) -> Result<Json<Value>, ApiError> {
    let (page, per_page) = clamp_page(&q, 50);
    Ok(Json(json!(
        mn::list_accounts(&state.db, page, per_page).await?
    )))
}

async fn accounts_stats(State(state): State<AppState>) -> Result<Json<Value>, ApiError> {
    Ok(Json(mn::get_accounts_stats(&state.db).await?))
}

async fn account_assets(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<Value>, ApiError> {
    Ok(Json(torii(&state)?.account_assets(&id).await?))
}

async fn account_transactions(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<Value>, ApiError> {
    Ok(Json(torii(&state)?.account_transactions(&id).await?))
}

async fn account_permissions(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<Value>, ApiError> {
    Ok(Json(torii(&state)?.account_permissions(&id).await?))
}

async fn domains(State(state): State<AppState>) -> Result<Json<Value>, ApiError> {
    Ok(Json(json!({ "items": mn::list_domains(&state.db).await? })))
}

async fn domains_stats(State(state): State<AppState>) -> Result<Json<Value>, ApiError> {
    Ok(Json(mn::get_domains_stats(&state.db).await?))
}

async fn assets(
    State(state): State<AppState>,
    Query(q): Query<PageQuery>,
) -> Result<Json<Value>, ApiError> {
    let (page, per_page) = clamp_page(&q, 50);
    Ok(Json(json!(
        mn::list_assets(&state.db, page, per_page).await?
    )))
}

async fn asset_definitions(State(state): State<AppState>) -> Result<Json<Value>, ApiError> {
    Ok(Json(
        json!({ "items": mn::list_asset_definitions(&state.db).await? }),
    ))
}

async fn asset_definitions_stats(State(state): State<AppState>) -> Result<Json<Value>, ApiError> {
    Ok(Json(mn::get_assets_stats(&state.db).await?))
}

async fn asset(
    State(state): State<AppState>,
    Path(needle): Path<String>,
) -> Result<Response, ApiError> {
    match mn::get_asset_supply(&state.db, &needle).await? {
        Some(v) => Ok(Json(v).into_response()),
        None => Ok(node_404("asset not found")),
    }
}

async fn asset_holders(
    State(state): State<AppState>,
    Path(needle): Path<String>,
) -> Result<Response, ApiError> {
    match mn::get_asset_holders(&state.db, &needle).await? {
        Some(v) => Ok(Json(v).into_response()),
        None => Ok(node_404("asset not found")),
    }
}

async fn block(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<Value>, ApiError> {
    Ok(Json(torii(&state)?.block(&id).await?))
}

async fn tx(
    State(state): State<AppState>,
    Path(hash): Path<String>,
) -> Result<Json<Value>, ApiError> {
    Ok(Json(torii(&state)?.transaction(&hash).await?))
}

/// `/instructions` query.
#[derive(Debug, Deserialize)]
pub struct IsiQuery {
    /// Page.
    #[serde(flatten)]
    pub page: PageQuery,
    /// Kind filter.
    pub kind: Option<String>,
    /// Authority filter.
    pub authority: Option<String>,
    /// Block filter.
    pub block: Option<String>,
    /// Transaction hash filter.
    pub tx: Option<String>,
}

async fn instructions(
    State(state): State<AppState>,
    Query(q): Query<IsiQuery>,
) -> Result<Json<Value>, ApiError> {
    let (page, per_page) = clamp_page(&q.page, 50);
    let tx_hash = match opt_str(&q.tx) {
        Some(h) => Some(hash32_from_hex(&h).map_err(ApiError::BadRequest)?),
        None => None,
    };
    let f = mn::IsiListFilter {
        kind: opt_str(&q.kind),
        authority: opt_str(&q.authority),
        block: opt_str(&q.block).and_then(|b| js_parse_int(&b)),
        tx_hash,
    };
    Ok(Json(json!(
        mn::list_instructions(&state.db, page, per_page, &f).await?
    )))
}

async fn instruction_kinds(State(state): State<AppState>) -> Result<Json<Value>, ApiError> {
    Ok(Json(
        json!({ "items": mn::list_instruction_kinds(&state.db).await? }),
    ))
}

async fn transfers_stats(State(state): State<AppState>) -> Result<Json<Value>, ApiError> {
    Ok(Json(mn::get_transfers_stats(&state.db).await?))
}

async fn permissions_stats(State(state): State<AppState>) -> Result<Json<Value>, ApiError> {
    Ok(Json(mn::get_permissions_stats(&state.db).await?))
}

/// `/permissions/grants` query.
#[derive(Debug, Deserialize)]
pub struct GrantQuery {
    /// Page.
    #[serde(flatten)]
    pub page: PageQuery,
    /// Permission name.
    pub perm_name: Option<String>,
    /// Grantor.
    pub authority: Option<String>,
    /// Grantee.
    pub destination: Option<String>,
    /// Variant.
    pub variant: Option<String>,
}

async fn permission_grants(
    State(state): State<AppState>,
    Query(q): Query<GrantQuery>,
) -> Result<Json<Value>, ApiError> {
    let (page, per_page) = clamp_page(&q.page, 50);
    let f = mn::GrantFilter {
        perm_name: opt_str(&q.perm_name),
        authority: opt_str(&q.authority),
        destination: opt_str(&q.destination),
        variant: opt_str(&q.variant),
    };
    Ok(Json(json!(
        mn::list_permission_grants(&state.db, page, per_page, &f).await?
    )))
}

/// Replays Register/Activate events per validator pubkey.
async fn lane_staking_lifecycle(State(state): State<AppState>) -> Result<Json<Value>, ApiError> {
    let events = mn::lane_staking_events(&state.db).await?;
    let mut order: Vec<String> = Vec::new();
    let mut by_validator: std::collections::HashMap<String, Value> = Default::default();
    for e in &events {
        let v = by_validator.entry(e.pubkey.clone()).or_insert_with(|| {
            order.push(e.pubkey.clone());
            json!({
                "pubkey": e.pubkey,
                "registered_at": null, "registered_tx": null, "registered_block": null,
                "activated_at": null, "activated_tx": null, "activated_block": null,
                "status": "unknown",
                "events": [],
            })
        });
        let at = iso(e.created_at);
        if let Some(a) = v["events"].as_array_mut() {
            a.push(json!({ "kind": e.kind, "tx": e.tx_hash, "block": e.block, "at": at, "status": e.status }));
        }
        if e.kind == "RegisterPublicLaneValidator"
            && e.status == "Committed"
            && v["registered_at"].is_null()
        {
            v["registered_at"] = json!(at);
            v["registered_tx"] = json!(e.tx_hash);
            v["registered_block"] = json!(e.block);
            v["status"] = json!("pending_activation");
        }
        if e.kind == "ActivatePublicLaneValidator" && e.status == "Committed" {
            v["activated_at"] = json!(at);
            v["activated_tx"] = json!(e.tx_hash);
            v["activated_block"] = json!(e.block);
            v["status"] = json!("active");
        }
    }
    let mut validators: Vec<Value> = order
        .into_iter()
        .filter_map(|k| by_validator.remove(&k))
        .collect();
    validators.sort_by_key(|v| {
        v["registered_at"]
            .as_str()
            .and_then(|s| DateTime::parse_from_rfc3339(s).ok())
            .map(|t| t.timestamp_millis())
            .unwrap_or(0)
    });
    Ok(Json(
        json!({ "total_events": events.len(), "validators": validators }),
    ))
}
