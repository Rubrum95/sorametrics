//! Order book reads on the legacy contract:
//! `/history/global/orderbook` and `/history/orderbook/:address`.
//!
//! Row (Node `mapOrderBook`): `{ time (dd/mm/yyyy HH:MM:SS), block,
//! hash, extrinsic_id, event_type, wallet, order_id ('' when none),
//! base_asset, quote_asset (symbols), side ('' when none), price,
//! amount (JSON numbers printed like JS: `100000000`, `0.001`; 0 when
//! the event has none), usd_value (1 dp) }`
//! paged as `{ data, total, page, totalPages }` (limit default 25, max
//! 100; page clamped like the Node). Filters: `?type=<event_type>` and
//! `?timestamp=<ms>` (upper bound); `?before=<ms>` is accepted as an
//! alias of `timestamp` (the frontend sends it; the Node ignored it).
//! Unfiltered totals use the planner estimate like the Node's
//! `reltuples` shortcut.

use crate::legacy::{fmt_extrinsic_id, fmt_time, fmt_usd, page_bounds, ser_js_number, symbol_for};
use crate::state::Registry;
use crate::{error::ApiError, AppState};
use axum::{
    extract::{Path, Query, State},
    routing::get,
    Json, Router,
};
use bigdecimal::{BigDecimal, ToPrimitive};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Build the sub-router.
pub fn router() -> Router<AppState> {
    Router::new()
        .route("/history/global/orderbook", get(global))
        .route("/history/orderbook/:address", get(by_address))
}

struct ObRecord {
    block_height: i64,
    extrinsic_id: String,
    hash: Option<String>,
    block_timestamp: DateTime<Utc>,
    event_type: String,
    wallet: String,
    order_id: Option<String>,
    base_asset_id: Option<String>,
    quote_asset_id: Option<String>,
    side: Option<String>,
    price: Option<BigDecimal>,
    amount: Option<BigDecimal>,
    usd_value: Option<BigDecimal>,
}

#[derive(Serialize)]
struct ObRow {
    time: String,
    block: i64,
    hash: String,
    extrinsic_id: String,
    event_type: String,
    wallet: String,
    order_id: String,
    base_asset: String,
    quote_asset: String,
    side: String,
    #[serde(serialize_with = "ser_js_number")]
    price: f64,
    #[serde(serialize_with = "ser_js_number")]
    amount: f64,
    #[serde(serialize_with = "ser_js_number")]
    usd_value: f64,
}

/// Node: the stored decimal text is `JSON.parse`d into a number; a
/// missing value renders as `0` (the MV's `COALESCE(…, '0')`).
pub fn fmt_number(v: Option<&BigDecimal>) -> f64 {
    v.and_then(ToPrimitive::to_f64).unwrap_or(0.0)
}

fn row(r: &ObRecord, registry: &Registry, zone: chrono_tz::Tz) -> ObRow {
    ObRow {
        time: fmt_time(r.block_timestamp, zone),
        block: r.block_height,
        hash: r.hash.clone().unwrap_or_default(),
        extrinsic_id: fmt_extrinsic_id(r.block_height, &r.extrinsic_id),
        event_type: r.event_type.clone(),
        wallet: r.wallet.clone(),
        order_id: r.order_id.clone().unwrap_or_default(),
        base_asset: r
            .base_asset_id
            .as_deref()
            .map(|id| symbol_for(registry, id))
            .unwrap_or_default(),
        quote_asset: r
            .quote_asset_id
            .as_deref()
            .map(|id| symbol_for(registry, id))
            .unwrap_or_default(),
        side: r.side.clone().unwrap_or_default(),
        price: fmt_number(r.price.as_ref()),
        amount: fmt_number(r.amount.as_ref()),
        usd_value: fmt_usd(r.usd_value.as_ref()),
    }
}

#[derive(Debug, Default, Deserialize)]
struct GlobalQuery {
    page: Option<i64>,
    limit: Option<i64>,
    #[serde(rename = "type")]
    event_type: Option<String>,
    timestamp: Option<String>,
    before: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
struct PageQuery {
    page: Option<i64>,
    limit: Option<i64>,
}

#[derive(Serialize)]
struct Page {
    data: Vec<ObRow>,
    total: i64,
    page: i64,
    #[serde(rename = "totalPages")]
    total_pages: i64,
}

struct Filters {
    wallet: Option<String>,
    event_type: Option<String>,
    until: Option<DateTime<Utc>>,
}

fn parse_page(page: Option<i64>, limit: Option<i64>) -> Result<(i64, i64), ApiError> {
    let page = page.unwrap_or(1);
    if page < 1 {
        return Err(ApiError::BadRequest("page must be ≥ 1".into()));
    }
    let limit = limit.unwrap_or(25);
    if !(1..=100).contains(&limit) {
        return Err(ApiError::BadRequest(
            "limit must be between 1 and 100".into(),
        ));
    }
    Ok((page, limit))
}

fn parse_until(raw: Option<&str>) -> Result<Option<DateTime<Utc>>, ApiError> {
    match raw.map(str::trim) {
        Some("") | None => Ok(None),
        Some(raw) => {
            let ms: i64 = raw
                .parse()
                .map_err(|_| ApiError::BadRequest("timestamp must be unix milliseconds".into()))?;
            DateTime::from_timestamp_millis(ms)
                .map(Some)
                .ok_or_else(|| ApiError::BadRequest("timestamp out of range".into()))
        }
    }
}

async fn count(state: &AppState, f: &Filters) -> Result<i64, ApiError> {
    if f.wallet.is_none() && f.event_type.is_none() && f.until.is_none() {
        let est = sqlx::query!(
            r#"
            SELECT c.reltuples::bigint AS "estimate!"
            FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = 'sm' AND c.relname = 'order_book_events'
            "#
        )
        .fetch_optional(&state.db)
        .await?
        .map(|r| r.estimate)
        .filter(|e| *e >= 0);
        if let Some(e) = est {
            return Ok(e);
        }
    }
    Ok(sqlx::query!(
        r#"
        SELECT COUNT(*) AS "count!"
        FROM sm.order_book_events
        WHERE ($1::text IS NULL OR wallet = $1)
          AND ($2::text IS NULL OR event_type = $2)
          AND ($3::timestamptz IS NULL OR block_timestamp <= $3)
        "#,
        f.wallet,
        f.event_type,
        f.until,
    )
    .fetch_one(&state.db)
    .await?
    .count)
}

async fn listing(state: &AppState, f: &Filters, page: i64, limit: i64) -> Result<Page, ApiError> {
    let total = count(state, f).await?;
    let (total_pages, safe_page) = page_bounds(total, limit, page);
    let offset = (safe_page - 1) * limit;
    let rows = sqlx::query_as!(
        ObRecord,
        r#"
        SELECT block_height, extrinsic_id, hash, block_timestamp, event_type, wallet, order_id,
               base_asset_id, quote_asset_id, side,
               price     AS "price: BigDecimal",
               amount    AS "amount: BigDecimal",
               usd_value AS "usd_value: BigDecimal"
        FROM sm.order_book_events
        WHERE ($3::text IS NULL OR wallet = $3)
          AND ($4::text IS NULL OR event_type = $4)
          AND ($5::timestamptz IS NULL OR block_timestamp <= $5)
        ORDER BY block_height DESC, event_id DESC
        LIMIT $1 OFFSET $2
        "#,
        limit,
        offset,
        f.wallet,
        f.event_type,
        f.until,
    )
    .fetch_all(&state.db)
    .await?;
    let registry = state.registry.read().await;
    Ok(Page {
        data: rows
            .iter()
            .map(|r| row(r, &registry, state.time_zone))
            .collect(),
        total,
        page: safe_page,
        total_pages,
    })
}

async fn global(
    State(state): State<AppState>,
    Query(q): Query<GlobalQuery>,
) -> Result<Json<Page>, ApiError> {
    let (page, limit) = parse_page(q.page, q.limit)?;
    let until = parse_until(q.timestamp.as_deref().or(q.before.as_deref()))?;
    let f = Filters {
        wallet: None,
        event_type: q.event_type.filter(|t| !t.is_empty()),
        until,
    };
    Ok(Json(listing(&state, &f, page, limit).await?))
}

async fn by_address(
    State(state): State<AppState>,
    Path(address): Path<String>,
    Query(q): Query<PageQuery>,
) -> Result<Json<Page>, ApiError> {
    let address = crate::util::validate_address(&address)?;
    let (page, limit) = parse_page(q.page, q.limit)?;
    let f = Filters {
        wallet: Some(address),
        event_type: None,
        until: None,
    };
    Ok(Json(listing(&state, &f, page, limit).await?))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    #[test]
    fn numbers_render_like_json_parse_of_the_stored_text() {
        assert_eq!(fmt_number(None), 0.0);
        assert_eq!(
            fmt_number(Some(&BigDecimal::from_str("0.001000000000000000").unwrap())),
            0.001
        );
        assert_eq!(
            fmt_number(Some(&BigDecimal::from_str("100000000").unwrap())),
            100_000_000.0
        );
    }

    #[test]
    fn page_defaults_and_bounds() {
        assert_eq!(parse_page(None, None).unwrap(), (1, 25));
        assert!(parse_page(Some(0), None).is_err());
        assert!(parse_page(None, Some(101)).is_err());
    }

    #[test]
    fn until_accepts_millis_only() {
        assert!(parse_until(None).unwrap().is_none());
        assert!(parse_until(Some("")).unwrap().is_none());
        assert_eq!(
            parse_until(Some("1772115000000"))
                .unwrap()
                .map(|t| t.timestamp_millis()),
            Some(1_772_115_000_000)
        );
        assert!(parse_until(Some("x")).is_err());
    }
}
