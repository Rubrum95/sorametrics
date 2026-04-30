//! `/tokens` and `/asset/:asset_id` — read-only views of
//! `sm.asset_registry`.
//!
//! `sm.asset_registry` is populated either by the inline bootstrap
//! migration (10 SORA essentials) or by the `sorametrics-ops
//! load-asset-registry` command which fetches the full whitelist
//! (~962 tokens) from the sora-xor GitHub repo.
//!
//! Modern shape, consistent with `/history/global/*`:
//! - `?page=N` (0-indexed) `?limit=M` (1..=100, default 100 — small
//!   table, callers usually want the lot in one shot)
//! - `{ items, page, limit, total_known }`
//! - `total_known` is populated here because asset_registry is small
//!   and a full COUNT is cheap.

use crate::{error::ApiError, AppState};
use axum::{
    extract::{Path, Query, State},
    routing::get,
    Json, Router,
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Build the tokens sub-router.
pub fn router() -> Router<AppState> {
    Router::new()
        .route("/tokens", get(list_tokens))
        .route("/asset/:asset_id", get(get_asset))
}

#[derive(Serialize)]
struct AssetItem {
    asset_id: String,
    symbol: String,
    name: Option<String>,
    decimals: i16,
    logo: Option<String>,
    updated_at: DateTime<Utc>,
}

#[derive(Serialize)]
struct Page<T> {
    items: Vec<T>,
    page: i64,
    limit: i64,
    total_known: Option<i64>,
}

#[derive(Debug, Deserialize)]
struct Pagination {
    #[serde(default = "Pagination::default_page")]
    page: i64,
    #[serde(default = "Pagination::default_limit")]
    limit: i64,
}

impl Pagination {
    fn default_page() -> i64 {
        0
    }
    fn default_limit() -> i64 {
        100
    }

    fn validate(&self) -> Result<(i64, i64), ApiError> {
        if self.page < 0 {
            return Err(ApiError::BadRequest("page must be ≥ 0".into()));
        }
        if !(1..=1000).contains(&self.limit) {
            return Err(ApiError::BadRequest(
                "limit must be between 1 and 1000".into(),
            ));
        }
        Ok((self.limit, self.page * self.limit))
    }
}

async fn list_tokens(
    State(state): State<AppState>,
    Query(p): Query<Pagination>,
) -> Result<Json<Page<AssetItem>>, ApiError> {
    let (limit, offset) = p.validate()?;

    let items = sqlx::query_as!(
        AssetItem,
        r#"
        SELECT
            asset_id,
            symbol,
            name,
            decimals,
            logo,
            updated_at
        FROM sm.asset_registry
        ORDER BY symbol
        LIMIT $1 OFFSET $2
        "#,
        limit,
        offset,
    )
    .fetch_all(&state.db)
    .await?;

    let total: i64 = sqlx::query_scalar!(r#"SELECT COUNT(*) AS "c!" FROM sm.asset_registry"#)
        .fetch_one(&state.db)
        .await?;

    Ok(Json(Page {
        items,
        page: p.page,
        limit: p.limit,
        total_known: Some(total),
    }))
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
        SELECT
            asset_id,
            symbol,
            name,
            decimals,
            logo,
            updated_at
        FROM sm.asset_registry
        WHERE asset_id = $1
        "#,
        asset_id,
    )
    .fetch_optional(&state.db)
    .await?;

    match item {
        Some(item) => Ok(Json(item)),
        None => Err(ApiError::NotFound(format!(
            "asset {asset_id} not in sm.asset_registry"
        ))),
    }
}
