//! Extrinsic routes on the legacy contract (`index.js`, `db_pg.js`):
//!
//! - `/history/global/extrinsics?page&limit&section&method&timestamp&block&success`
//!   (`getLatestExtrinsics`): `timestamp.set` always excluded; `section`
//!   exact, `method` substring, `timestamp` upper bound (ms), `block`
//!   exact, `success` 0/1. Unfiltered `total` = planner estimate.
//! - `/history/extrinsics/:address` (`getExtrinsicsByAddress`): by signer.
//! - `/history/extrinsic/:block/:index` (`getExtrinsicDetail`): the row
//!   with `args_json` / `events_json` populated; 404 when missing.
//! - `/history/extrinsic-sections`: distinct sections except `timestamp`.
//! - `/stats/extrinsics-24h` (`getExtrinsicStats24h`).
//! - `/history/extrinsic-fees?blocks=` (`getFeesByBlocks`): per-block
//!   fee totals from `sm.fees` (amount in (0, 100] XOR).
//! - `/search?q=` (`globalSearch`): extrinsic id, block number, tx hash,
//!   wallet, or token symbols.
//!
//! Row (`mapExtrinsics`): `{ time, block, extrinsic_index, extrinsic_id,
//! hash, section, method, signer, success (0/1), args_json, error_msg,
//! events_json }`. Listings carry `args_json: "{}"` and `events_json:
//! null` (the Node's `EXT_COLS` had neither); the detail carries both
//! as JSON strings and `extrinsic_index` as a STRING (a Node quirk the
//! frontend tolerates).

use crate::legacy::{fmt_time, page_bounds};
use crate::{error::ApiError, AppState};
use axum::{
    extract::{Path, Query, State},
    routing::get,
    Json, Router,
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value as Json_;
use std::collections::BTreeMap;
use std::time::Duration;

/// Build the sub-router.
pub fn router() -> Router<AppState> {
    Router::new()
        .route("/history/global/extrinsics", get(global))
        .route("/history/extrinsics/:address", get(by_address))
        .route("/history/extrinsic/:block/:index", get(detail))
        .route("/history/extrinsic-sections", get(sections))
        .route("/stats/extrinsics-24h", get(stats_24h))
        .route("/history/extrinsic-fees", get(fees_by_blocks))
        .route("/search", get(search))
}

struct ExtRecord {
    block_height: i64,
    extrinsic_index: i32,
    block_timestamp: DateTime<Utc>,
    hash: String,
    section: String,
    method: String,
    signer: String,
    success: bool,
    error_msg: String,
    args: Option<Json_>,
    events: Option<Json_>,
}

/// `extrinsic_index` is an integer in listings and a string in the
/// detail / search (the Node built the detail from path params).
#[derive(Serialize)]
#[serde(untagged)]
enum Index {
    Num(i32),
    Text(String),
}

#[derive(Serialize)]
struct ExtRow {
    time: String,
    block: i64,
    extrinsic_index: Index,
    extrinsic_id: String,
    hash: String,
    section: String,
    method: String,
    signer: String,
    success: u8,
    args_json: String,
    error_msg: String,
    events_json: Option<String>,
}

fn row(r: &ExtRecord, zone: chrono_tz::Tz, detail: bool) -> ExtRow {
    let (args_json, events_json, index) = if detail {
        let args = r
            .args
            .as_ref()
            .map(|a| match a {
                Json_::String(s) => s.clone(),
                other => other.to_string(),
            })
            .unwrap_or_else(|| "{}".to_string());
        let events = match &r.events {
            Some(Json_::Array(v)) if !v.is_empty() => Some(Json_::Array(v.clone()).to_string()),
            _ => None,
        };
        (args, events, Index::Text(r.extrinsic_index.to_string()))
    } else {
        ("{}".to_string(), None, Index::Num(r.extrinsic_index))
    };
    ExtRow {
        time: fmt_time(r.block_timestamp, zone),
        block: r.block_height,
        extrinsic_index: index,
        extrinsic_id: format!("{}-{}", r.block_height, r.extrinsic_index),
        hash: r.hash.clone(),
        section: r.section.clone(),
        method: r.method.clone(),
        signer: r.signer.clone(),
        success: u8::from(r.success),
        args_json,
        error_msg: r.error_msg.clone(),
        events_json,
    }
}

#[derive(Serialize)]
struct Page {
    data: Vec<ExtRow>,
    total: i64,
    page: i64,
    #[serde(rename = "totalPages")]
    total_pages: i64,
}

#[derive(Debug, Default, Deserialize)]
struct GlobalQuery {
    page: Option<i64>,
    limit: Option<i64>,
    section: Option<String>,
    method: Option<String>,
    timestamp: Option<String>,
    block: Option<i64>,
    success: Option<i32>,
}

/// Validated filters shared by the global and per-address listings.
struct Filters {
    signer: Option<String>,
    section: Option<String>,
    method_like: Option<String>,
    until: Option<DateTime<Utc>>,
    block: Option<i64>,
    success: Option<bool>,
}

impl Filters {
    fn is_unfiltered(&self) -> bool {
        self.signer.is_none()
            && self.section.is_none()
            && self.method_like.is_none()
            && self.until.is_none()
            && self.block.is_none()
            && self.success.is_none()
    }
}

fn parse_page(
    page: Option<i64>,
    limit: Option<i64>,
    default_limit: i64,
) -> Result<(i64, i64), ApiError> {
    let page = page.unwrap_or(1);
    if page < 1 {
        return Err(ApiError::BadRequest("page must be ≥ 1".into()));
    }
    let limit = limit.unwrap_or(default_limit);
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
    if f.is_unfiltered() {
        let est = sqlx::query!(
            r#"
            SELECT c.reltuples::bigint AS "estimate!"
            FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = 'sm' AND c.relname = 'extrinsics'
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
        FROM sm.extrinsics
        WHERE NOT (section = 'timestamp' AND method = 'set')
          AND ($1::text IS NULL OR signer = $1)
          AND ($2::text IS NULL OR section = $2)
          AND ($3::text IS NULL OR method LIKE $3)
          AND ($4::timestamptz IS NULL OR block_timestamp <= $4)
          AND ($5::bigint IS NULL OR block_height = $5)
          AND ($6::bool IS NULL OR success = $6)
        "#,
        f.signer,
        f.section,
        f.method_like,
        f.until,
        f.block,
        f.success,
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
        ExtRecord,
        r#"
        SELECT block_height, extrinsic_index, block_timestamp, hash, section, method,
               signer, success, error_msg, NULL::jsonb AS "args", NULL::jsonb AS "events"
        FROM sm.extrinsics
        WHERE NOT (section = 'timestamp' AND method = 'set')
          AND ($3::text IS NULL OR signer = $3)
          AND ($4::text IS NULL OR section = $4)
          AND ($5::text IS NULL OR method LIKE $5)
          AND ($6::timestamptz IS NULL OR block_timestamp <= $6)
          AND ($7::bigint IS NULL OR block_height = $7)
          AND ($8::bool IS NULL OR success = $8)
        ORDER BY block_height DESC, extrinsic_index DESC
        LIMIT $1 OFFSET $2
        "#,
        limit,
        offset,
        f.signer,
        f.section,
        f.method_like,
        f.until,
        f.block,
        f.success,
    )
    .fetch_all(&state.db)
    .await?;
    Ok(Page {
        data: rows
            .iter()
            .map(|r| row(r, state.time_zone, false))
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
    let (page, limit) = parse_page(q.page, q.limit, 25)?;
    let f = Filters {
        signer: None,
        section: q.section.filter(|s| !s.is_empty()),
        method_like: q.method.filter(|m| !m.is_empty()).map(|m| format!("%{m}%")),
        until: parse_until(q.timestamp.as_deref())?,
        block: q.block,
        success: q.success.map(|s| s != 0),
    };
    Ok(Json(listing(&state, &f, page, limit).await?))
}

#[derive(Debug, Default, Deserialize)]
struct PageQuery {
    page: Option<i64>,
    limit: Option<i64>,
}

async fn by_address(
    State(state): State<AppState>,
    Path(address): Path<String>,
    Query(q): Query<PageQuery>,
) -> Result<Json<Page>, ApiError> {
    let address = crate::util::validate_address(&address)?;
    let (page, limit) = parse_page(q.page, q.limit, 25)?;
    let f = Filters {
        signer: Some(address),
        section: None,
        method_like: None,
        until: None,
        block: None,
        success: None,
    };
    Ok(Json(listing(&state, &f, page, limit).await?))
}

async fn fetch_detail(
    state: &AppState,
    block: i64,
    index: i32,
) -> Result<Option<ExtRecord>, ApiError> {
    Ok(sqlx::query_as!(
        ExtRecord,
        r#"
        SELECT block_height, extrinsic_index, block_timestamp, hash, section, method,
               signer, success, error_msg, args, events
        FROM sm.extrinsics
        WHERE block_height = $1 AND extrinsic_index = $2
        "#,
        block,
        index,
    )
    .fetch_optional(&state.db)
    .await?)
}

async fn detail(
    State(state): State<AppState>,
    Path((block, index)): Path<(String, String)>,
) -> Result<Json<ExtRow>, ApiError> {
    let (Ok(block), Ok(index)) = (block.parse::<i64>(), index.parse::<i32>()) else {
        return Err(ApiError::BadRequest("Invalid block or index".into()));
    };
    if block < 0 || index < 0 {
        return Err(ApiError::BadRequest("Invalid block or index".into()));
    }
    match fetch_detail(&state, block, index).await? {
        Some(r) => Ok(Json(row(&r, state.time_zone, true))),
        None => Err(ApiError::NotFound("Not found".into())),
    }
}

async fn sections(State(state): State<AppState>) -> Result<Json<Vec<String>>, ApiError> {
    if let Some(v) = state
        .cached_scan("extrinsic-sections", Duration::from_secs(300))
        .await
    {
        return serde_json::from_value(v)
            .map(Json)
            .map_err(|e| ApiError::Internal(e.to_string()));
    }
    let rows = sqlx::query!(
        r#"SELECT DISTINCT section AS "section!" FROM sm.extrinsics
           WHERE section <> 'timestamp' ORDER BY section ASC"#
    )
    .fetch_all(&state.db)
    .await?;
    let list: Vec<String> = rows.into_iter().map(|r| r.section).collect();
    let v = serde_json::to_value(&list).map_err(|e| ApiError::Internal(e.to_string()))?;
    state.store_scan("extrinsic-sections", v).await;
    Ok(Json(list))
}

#[derive(Serialize, Deserialize)]
struct Stats24h {
    total: i64,
    success: i64,
    failed: i64,
    #[serde(rename = "successRate")]
    success_rate: f64,
    #[serde(rename = "topPallet")]
    top_pallet: Option<String>,
    #[serde(rename = "topPalletCount")]
    top_pallet_count: i64,
    #[serde(rename = "windowMs")]
    window_ms: i64,
}

async fn stats_24h(State(state): State<AppState>) -> Result<Json<Stats24h>, ApiError> {
    if let Some(v) = state
        .cached_scan("extrinsics-24h", Duration::from_secs(30))
        .await
    {
        return serde_json::from_value(v)
            .map(Json)
            .map_err(|e| ApiError::Internal(e.to_string()));
    }
    let window_ms: i64 = 86_400_000;
    let since = Utc::now() - chrono::Duration::milliseconds(window_ms);
    let totals = sqlx::query!(
        r#"
        SELECT COUNT(*) AS "total!", COUNT(*) FILTER (WHERE success) AS "success!"
        FROM sm.extrinsics
        WHERE block_timestamp >= $1 AND NOT (section = 'timestamp' AND method = 'set')
        "#,
        since,
    )
    .fetch_one(&state.db)
    .await?;
    let top = sqlx::query!(
        r#"
        SELECT section AS "section!", COUNT(*) AS "cnt!"
        FROM sm.extrinsics
        WHERE block_timestamp >= $1 AND NOT (section = 'timestamp' AND method = 'set')
        GROUP BY section ORDER BY COUNT(*) DESC LIMIT 1
        "#,
        since,
    )
    .fetch_optional(&state.db)
    .await?;
    let total = totals.total;
    let success = totals.success;
    let stats = Stats24h {
        total,
        success,
        failed: total - success,
        // Node: `+(success / total * 100).toFixed(1)`.
        success_rate: if total > 0 {
            (success as f64 / total as f64 * 1000.0).round() / 10.0
        } else {
            0.0
        },
        top_pallet: top.as_ref().map(|t| t.section.clone()),
        top_pallet_count: top.map(|t| t.cnt).unwrap_or(0),
        window_ms,
    };
    let v = serde_json::to_value(&stats).map_err(|e| ApiError::Internal(e.to_string()))?;
    state.store_scan("extrinsics-24h", v).await;
    Ok(Json(stats))
}

#[derive(Debug, Default, Deserialize)]
struct FeesQuery {
    blocks: Option<String>,
}

#[derive(Serialize)]
struct BlockFees {
    #[serde(rename = "totalXor")]
    total_xor: f64,
    #[serde(rename = "totalUsd")]
    total_usd: f64,
    rows: i64,
}

async fn fees_by_blocks(
    State(state): State<AppState>,
    Query(q): Query<FeesQuery>,
) -> Result<Json<BTreeMap<i64, BlockFees>>, ApiError> {
    let blocks: Vec<i64> = q
        .blocks
        .as_deref()
        .unwrap_or("")
        .split(',')
        .filter_map(|s| s.trim().parse::<i64>().ok())
        .take(100)
        .collect();
    if blocks.is_empty() {
        return Ok(Json(BTreeMap::new()));
    }
    let rows = sqlx::query!(
        r#"
        SELECT block_height,
               SUM(amount_xor)              AS "total_xor: bigdecimal::BigDecimal",
               COALESCE(SUM(usd_value), 0)  AS "total_usd: bigdecimal::BigDecimal",
               COUNT(*)                     AS "rows!"
        FROM sm.fees
        WHERE block_height = ANY($1) AND amount_xor > 0 AND amount_xor <= 100
        GROUP BY block_height
        "#,
        &blocks,
    )
    .fetch_all(&state.db)
    .await?;
    use bigdecimal::ToPrimitive;
    let out = rows
        .into_iter()
        .map(|r| {
            (
                r.block_height,
                BlockFees {
                    total_xor: r.total_xor.and_then(|v| v.to_f64()).unwrap_or(0.0),
                    total_usd: r.total_usd.and_then(|v| v.to_f64()).unwrap_or(0.0),
                    rows: r.rows,
                },
            )
        })
        .collect();
    Ok(Json(out))
}

#[derive(Debug, Default, Deserialize)]
struct SearchQuery {
    q: Option<String>,
}

#[derive(Serialize)]
#[serde(untagged)]
enum SearchData {
    Extrinsic(Box<ExtRow>),
    Block { block: i64 },
    Wallet { address: String },
    Token(TokenHit),
    Tokens(Vec<TokenHit>),
}

#[derive(Serialize, Clone)]
struct TokenHit {
    symbol: String,
    #[serde(rename = "assetId")]
    asset_id: String,
}

#[derive(Serialize)]
struct SearchResponse {
    #[serde(rename = "type")]
    kind: Option<&'static str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    data: Option<SearchData>,
}

fn is_tx_hash(s: &str) -> bool {
    s.len() == 66 && s.starts_with("0x") && s[2..].chars().all(|c| c.is_ascii_hexdigit())
}

fn is_wallet(s: &str) -> bool {
    s.starts_with("cn") && s.len() >= 48 && s[2..].chars().all(|c| c.is_ascii_alphanumeric())
}

async fn search(
    State(state): State<AppState>,
    Query(q): Query<SearchQuery>,
) -> Result<Json<SearchResponse>, ApiError> {
    let q = q.q.unwrap_or_default().trim().to_string();
    if q.len() < 3 || q.len() > 128 {
        return Err(ApiError::BadRequest("Invalid query".into()));
    }
    // Extrinsic id "block-index".
    if let Some((b, i)) = q.split_once('-') {
        if let (Ok(block), Ok(index)) = (b.parse::<i64>(), i.parse::<i32>()) {
            return Ok(Json(match fetch_detail(&state, block, index).await? {
                Some(r) => SearchResponse {
                    kind: Some("extrinsic"),
                    data: Some(SearchData::Extrinsic(Box::new(row(
                        &r,
                        state.time_zone,
                        true,
                    )))),
                },
                None => SearchResponse {
                    kind: Some("hash_not_found"),
                    data: None,
                },
            }));
        }
    }
    if let Ok(block) = q.parse::<i64>() {
        return Ok(Json(SearchResponse {
            kind: Some("block"),
            data: Some(SearchData::Block { block }),
        }));
    }
    if is_tx_hash(&q) {
        let found = sqlx::query_as!(
            ExtRecord,
            r#"
            SELECT block_height, extrinsic_index, block_timestamp, hash, section, method,
                   signer, success, error_msg, args, NULL::jsonb AS "events"
            FROM sm.extrinsics WHERE hash = $1 LIMIT 1
            "#,
            q.to_lowercase(),
        )
        .fetch_optional(&state.db)
        .await?;
        return Ok(Json(match found {
            Some(r) => SearchResponse {
                kind: Some("extrinsic"),
                data: Some(SearchData::Extrinsic(Box::new(row(
                    &r,
                    state.time_zone,
                    true,
                )))),
            },
            None => SearchResponse {
                kind: Some("hash_not_found"),
                data: None,
            },
        }));
    }
    if is_wallet(&q) {
        return Ok(Json(SearchResponse {
            kind: Some("wallet"),
            data: Some(SearchData::Wallet { address: q }),
        }));
    }
    let hits: Vec<TokenHit> = state
        .registry
        .read()
        .await
        .symbols_matching(&q)
        .into_iter()
        .map(|(symbol, asset_id)| TokenHit { symbol, asset_id })
        .collect();
    Ok(Json(match hits.len() {
        0 => SearchResponse {
            kind: None,
            data: None,
        },
        1 => SearchResponse {
            kind: Some("token"),
            data: Some(SearchData::Token(hits[0].clone())),
        },
        _ => SearchResponse {
            kind: Some("tokens"),
            data: Some(SearchData::Tokens(hits.into_iter().take(10).collect())),
        },
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn search_shape_checks() {
        assert!(is_tx_hash(&format!("0x{}", "ab".repeat(32))));
        assert!(!is_tx_hash("0x1234"));
        assert!(is_wallet(
            "cnWeiModLdWS4hC75QeZxEANGNUmekog7YaaJ9PevFH1UTnhh"
        ));
        assert!(!is_wallet("cnShort"));
    }

    #[test]
    fn page_and_timestamp_parsing() {
        assert_eq!(parse_page(None, None, 25).unwrap(), (1, 25));
        assert!(parse_page(Some(0), None, 25).is_err());
        assert!(parse_page(None, Some(500), 25).is_err());
        assert_eq!(
            parse_until(Some("1788623172000"))
                .unwrap()
                .unwrap()
                .timestamp(),
            1_788_623_172
        );
        assert!(parse_until(Some("x")).is_err());
        assert!(parse_until(Some("")).unwrap().is_none());
    }
}
