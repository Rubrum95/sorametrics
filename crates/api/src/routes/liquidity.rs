//! Pool liquidity reads on the legacy contract:
//! `/history/global/liquidity` and `/pool/activity`.
//!
//! Row (`LIQ_COLS` + logos / `time`): `{ timestamp (ms string), block,
//! wallet, pool_base, pool_target (symbols), base_amount, target_amount
//! (4-dp strings), usd_value (1 dp), type ('deposit'|'withdraw'), hash,
//! extrinsic_id }`. The global feed adds `base_logo` / `target_logo`
//! and pages `{ data, total, page, totalPages }` (limit default 20,
//! `?timestamp` upper bound). `/pool/activity?base=SYM&target=SYM&limit`
//! (default 50, max 200) returns the bare array with `time`, newest
//! first; 400 without both symbols.

use crate::legacy::{
    decimals_for, fmt_amount, fmt_extrinsic_id, fmt_millis, fmt_time, fmt_usd, logo_for,
    page_bounds, symbol_for,
};
use crate::state::Registry;
use crate::{error::ApiError, AppState};
use axum::{
    extract::{Query, State},
    routing::get,
    Json, Router,
};
use bigdecimal::BigDecimal;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Build the sub-router.
pub fn router() -> Router<AppState> {
    Router::new()
        .route("/history/global/liquidity", get(global))
        .route("/pool/activity", get(activity))
}

struct LiqRecord {
    block_height: i64,
    extrinsic_id: String,
    hash: Option<String>,
    block_timestamp: DateTime<Utc>,
    caller: String,
    base_asset_id: String,
    target_asset_id: String,
    base_amount: BigDecimal,
    target_amount: BigDecimal,
    usd_value: Option<BigDecimal>,
    kind: String,
}

#[derive(Serialize)]
struct LiqRow {
    timestamp: String,
    block: i64,
    wallet: String,
    pool_base: String,
    pool_target: String,
    base_amount: String,
    target_amount: String,
    usd_value: f64,
    #[serde(rename = "type")]
    kind: String,
    hash: String,
    extrinsic_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    base_logo: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    target_logo: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    time: Option<String>,
}

fn row(r: &LiqRecord, registry: &Registry, zone: chrono_tz::Tz, logos: bool) -> LiqRow {
    LiqRow {
        timestamp: fmt_millis(r.block_timestamp),
        block: r.block_height,
        wallet: r.caller.clone(),
        pool_base: symbol_for(registry, &r.base_asset_id),
        pool_target: symbol_for(registry, &r.target_asset_id),
        base_amount: fmt_amount(&r.base_amount, decimals_for(registry, &r.base_asset_id)),
        target_amount: fmt_amount(&r.target_amount, decimals_for(registry, &r.target_asset_id)),
        usd_value: fmt_usd(r.usd_value.as_ref()),
        kind: r.kind.clone(),
        hash: r.hash.clone().unwrap_or_default(),
        extrinsic_id: fmt_extrinsic_id(r.block_height, &r.extrinsic_id),
        base_logo: logos.then(|| logo_for(registry, &r.base_asset_id)),
        target_logo: logos.then(|| logo_for(registry, &r.target_asset_id)),
        time: (!logos).then(|| fmt_time(r.block_timestamp, zone)),
    }
}

#[derive(Debug, Deserialize)]
struct GlobalQuery {
    page: Option<i64>,
    limit: Option<i64>,
    timestamp: Option<String>,
}

#[derive(Serialize)]
struct Page {
    data: Vec<LiqRow>,
    total: i64,
    page: i64,
    #[serde(rename = "totalPages")]
    total_pages: i64,
}

async fn global(
    State(state): State<AppState>,
    Query(q): Query<GlobalQuery>,
) -> Result<Json<Page>, ApiError> {
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
    let until: Option<DateTime<Utc>> = match q.timestamp.as_deref().map(str::trim) {
        Some("") | None => None,
        Some(raw) => {
            let ms: i64 = raw
                .parse()
                .map_err(|_| ApiError::BadRequest("timestamp must be unix milliseconds".into()))?;
            Some(
                DateTime::from_timestamp_millis(ms)
                    .ok_or_else(|| ApiError::BadRequest("timestamp out of range".into()))?,
            )
        }
    };

    let total = match until {
        None => {
            let est = sqlx::query!(
                r#"
                SELECT c.reltuples::bigint AS "estimate!"
                FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
                WHERE n.nspname = 'sm' AND c.relname = 'liquidity_events'
                "#
            )
            .fetch_optional(&state.db)
            .await?
            .map(|r| r.estimate)
            .filter(|e| *e >= 0);
            match est {
                Some(e) => e,
                None => exact_count(&state, None).await?,
            }
        }
        Some(_) => exact_count(&state, until).await?,
    };
    let (total_pages, safe_page) = page_bounds(total, limit, page);
    let offset = (safe_page - 1) * limit;

    let rows = sqlx::query_as!(
        LiqRecord,
        r#"
        SELECT block_height, extrinsic_id, hash, block_timestamp, caller,
               base_asset_id, target_asset_id,
               base_amount   AS "base_amount!: BigDecimal",
               target_amount AS "target_amount!: BigDecimal",
               usd_value     AS "usd_value: BigDecimal",
               kind
        FROM sm.liquidity_events
        WHERE ($3::timestamptz IS NULL OR block_timestamp <= $3)
        ORDER BY block_height DESC, event_id DESC
        LIMIT $1 OFFSET $2
        "#,
        limit,
        offset,
        until,
    )
    .fetch_all(&state.db)
    .await?;

    let registry = state.registry.read().await;
    let data = rows
        .iter()
        .map(|r| row(r, &registry, state.time_zone, true))
        .collect();
    Ok(Json(Page {
        data,
        total,
        page: safe_page,
        total_pages,
    }))
}

async fn exact_count(state: &AppState, until: Option<DateTime<Utc>>) -> Result<i64, ApiError> {
    Ok(sqlx::query!(
        r#"
        SELECT COUNT(*) AS "count!"
        FROM sm.liquidity_events
        WHERE ($1::timestamptz IS NULL OR block_timestamp <= $1)
        "#,
        until,
    )
    .fetch_one(&state.db)
    .await?
    .count)
}

#[derive(Debug, Deserialize)]
struct ActivityQuery {
    base: Option<String>,
    target: Option<String>,
    limit: Option<i64>,
}

async fn activity(
    State(state): State<AppState>,
    Query(q): Query<ActivityQuery>,
) -> Result<Json<Vec<LiqRow>>, ApiError> {
    let (Some(base), Some(target)) = (q.base, q.target) else {
        return Err(ApiError::BadRequest("Missing base or target".into()));
    };
    let limit = q.limit.unwrap_or(50).clamp(1, 200);
    let registry = state.registry.read().await;
    // Symbols → canonical asset ids (the legacy tables stored symbols).
    let (base_id, target_id) = match (
        registry.asset_id_for_symbol(&base),
        registry.asset_id_for_symbol(&target),
    ) {
        (Some(b), Some(t)) => (b.to_string(), t.to_string()),
        _ => return Ok(Json(Vec::new())),
    };
    drop(registry);

    let rows = sqlx::query_as!(
        LiqRecord,
        r#"
        SELECT block_height, extrinsic_id, hash, block_timestamp, caller,
               base_asset_id, target_asset_id,
               base_amount   AS "base_amount!: BigDecimal",
               target_amount AS "target_amount!: BigDecimal",
               usd_value     AS "usd_value: BigDecimal",
               kind
        FROM sm.liquidity_events
        WHERE base_asset_id = $1 AND target_asset_id = $2
        ORDER BY block_height DESC, event_id DESC
        LIMIT $3
        "#,
        base_id,
        target_id,
        limit,
    )
    .fetch_all(&state.db)
    .await?;
    let registry = state.registry.read().await;
    Ok(Json(
        rows.iter()
            .map(|r| row(r, &registry, state.time_zone, false))
            .collect(),
    ))
}
