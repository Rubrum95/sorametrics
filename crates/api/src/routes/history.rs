//! `/history/global/{swaps,transfers,bridges,fee_burns}` — paginated
//! reads of the `sm.live_*` tables.
//!
//! Pagination contract (modern, clean):
//! - `?limit=N` (default 25, max 100, min 1)
//! - `?page=N` (default 0). Offset = `page * limit`.
//! - response: `{ items: [...], page, limit, total_known }`. `total_known`
//!   is `null` until we wire `pg_class.reltuples` (later phase). For now
//!   the frontend paginates by incrementing `page` until `items.len() <
//!   limit`.
//!
//! Sort order: newest first by `(block_height DESC, event_id DESC)`.
//!
//! Field shapes are intentionally flat and untransformed:
//! - timestamps: ISO 8601 (`2026-04-21T18:53:06Z`)
//! - amounts: raw on-chain `BigDecimal` (planck integer). Frontend is
//!   responsible for dividing by `10^decimals` and resolving symbols
//!   from `sm.asset_registry`.
//! - asset/account ids: `0x`-prefixed lowercase hex (whatever the
//!   ingest path stored).

use crate::{error::ApiError, util::validate_address, AppState};
use axum::{
    extract::{Path, Query, State},
    routing::get,
    Json, Router,
};
use bigdecimal::BigDecimal;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Build the `/history/*` sub-router.
///
/// Two parallel families:
/// - `/history/global/{table}` — paginated full feed
/// - `/history/{table}/:address` — paginated, filtered by an account
pub fn router() -> Router<AppState> {
    Router::new()
        .route("/history/global/swaps", get(swaps))
        .route("/history/global/transfers", get(transfers))
        .route("/history/global/bridges", get(bridges))
        .route("/history/global/fee_burns", get(fee_burns))
        .route("/history/swaps/:address", get(wallet_swaps))
        .route("/history/transfers/:address", get(wallet_transfers))
        .route("/history/bridges/:address", get(wallet_bridges))
        .route("/history/fee_burns/:address", get(wallet_fee_burns))
}

/// Common pagination query parameters.
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
        25
    }

    /// Validate and normalise. Caps `limit` at 100 to bound the cost
    /// of any single request.
    fn validate(&self) -> Result<(i64, i64), ApiError> {
        if self.page < 0 {
            return Err(ApiError::BadRequest("page must be ≥ 0".into()));
        }
        if !(1..=100).contains(&self.limit) {
            return Err(ApiError::BadRequest(
                "limit must be between 1 and 100".into(),
            ));
        }
        Ok((self.limit, self.page * self.limit))
    }
}

/// Wrapper for a paginated list response.
#[derive(Serialize)]
struct Page<T> {
    items: Vec<T>,
    page: i64,
    limit: i64,
    /// Total row count if cheaply known (`pg_class.reltuples`). `None`
    /// for now — caller paginates until `items.len() < limit`.
    total_known: Option<i64>,
}

// =============================================================
// /history/global/swaps
// =============================================================

#[derive(Serialize)]
struct SwapItem {
    block_height: i64,
    extrinsic_id: i32,
    event_id: i32,
    block_timestamp: DateTime<Utc>,
    caller: String,
    input_asset_id: String,
    input_amount: BigDecimal,
    output_asset_id: String,
    output_amount: BigDecimal,
    usd_value: Option<BigDecimal>,
}

async fn swaps(
    State(state): State<AppState>,
    Query(p): Query<Pagination>,
) -> Result<Json<Page<SwapItem>>, ApiError> {
    let (limit, offset) = p.validate()?;

    let items = sqlx::query_as!(
        SwapItem,
        r#"
        SELECT
            block_height,
            extrinsic_id,
            event_id,
            block_timestamp,
            caller,
            input_asset_id,
            input_amount  AS "input_amount!: BigDecimal",
            output_asset_id,
            output_amount AS "output_amount!: BigDecimal",
            usd_value     AS "usd_value: BigDecimal"
        FROM sm.live_swaps
        ORDER BY block_height DESC, event_id DESC
        LIMIT $1 OFFSET $2
        "#,
        limit,
        offset,
    )
    .fetch_all(&state.db)
    .await?;

    Ok(Json(Page {
        items,
        page: p.page,
        limit: p.limit,
        total_known: None,
    }))
}

// =============================================================
// /history/global/transfers
// =============================================================

#[derive(Serialize)]
struct TransferItem {
    block_height: i64,
    extrinsic_id: i32,
    event_id: i32,
    block_timestamp: DateTime<Utc>,
    from_address: String,
    to_address: String,
    asset_id: String,
    amount: BigDecimal,
    usd_value: Option<BigDecimal>,
}

async fn transfers(
    State(state): State<AppState>,
    Query(p): Query<Pagination>,
) -> Result<Json<Page<TransferItem>>, ApiError> {
    let (limit, offset) = p.validate()?;

    let items = sqlx::query_as!(
        TransferItem,
        r#"
        SELECT
            block_height,
            extrinsic_id,
            event_id,
            block_timestamp,
            from_address,
            to_address,
            asset_id,
            amount    AS "amount!: BigDecimal",
            usd_value AS "usd_value: BigDecimal"
        FROM sm.live_transfers
        ORDER BY block_height DESC, event_id DESC
        LIMIT $1 OFFSET $2
        "#,
        limit,
        offset,
    )
    .fetch_all(&state.db)
    .await?;

    Ok(Json(Page {
        items,
        page: p.page,
        limit: p.limit,
        total_known: None,
    }))
}

// =============================================================
// /history/global/bridges
// =============================================================

#[derive(Serialize)]
struct BridgeItem {
    block_height: i64,
    extrinsic_id: i32,
    event_id: i32,
    block_timestamp: DateTime<Utc>,
    direction: String,
    network: String,
    caller: String,
    asset_id: String,
    amount: BigDecimal,
}

async fn bridges(
    State(state): State<AppState>,
    Query(p): Query<Pagination>,
) -> Result<Json<Page<BridgeItem>>, ApiError> {
    let (limit, offset) = p.validate()?;

    // The `direction` column is a Postgres ENUM (`sm.bridge_direction`).
    // Cast to TEXT for transport so we can keep `BridgeItem.direction`
    // as plain `String` without writing a custom sqlx Type for the
    // read path (read shape is intentionally string-typed for the JSON
    // contract — frontend already expects "in" / "out").
    let items = sqlx::query_as!(
        BridgeItem,
        r#"
        SELECT
            block_height,
            extrinsic_id,
            event_id,
            block_timestamp,
            direction::text AS "direction!",
            network,
            caller,
            asset_id,
            amount AS "amount!: BigDecimal"
        FROM sm.live_bridges
        ORDER BY block_height DESC, event_id DESC
        LIMIT $1 OFFSET $2
        "#,
        limit,
        offset,
    )
    .fetch_all(&state.db)
    .await?;

    Ok(Json(Page {
        items,
        page: p.page,
        limit: p.limit,
        total_known: None,
    }))
}

// =============================================================
// /history/global/fee_burns
// =============================================================

#[derive(Serialize)]
struct FeeBurnItem {
    block_height: i64,
    extrinsic_id: i32,
    event_id: i32,
    block_timestamp: DateTime<Utc>,
    kind: String,
    payer: String,
    referrer: Option<String>,
    amount: BigDecimal,
}

async fn fee_burns(
    State(state): State<AppState>,
    Query(p): Query<Pagination>,
) -> Result<Json<Page<FeeBurnItem>>, ApiError> {
    let (limit, offset) = p.validate()?;

    let items = sqlx::query_as!(
        FeeBurnItem,
        r#"
        SELECT
            block_height,
            extrinsic_id,
            event_id,
            block_timestamp,
            kind::text AS "kind!",
            payer,
            referrer,
            amount AS "amount!: BigDecimal"
        FROM sm.live_fee_burns
        ORDER BY block_height DESC, event_id DESC
        LIMIT $1 OFFSET $2
        "#,
        limit,
        offset,
    )
    .fetch_all(&state.db)
    .await?;

    Ok(Json(Page {
        items,
        page: p.page,
        limit: p.limit,
        total_known: None,
    }))
}

// =============================================================
// Per-wallet variants: /history/{swaps,transfers,bridges,fee_burns}/:address
// =============================================================

async fn wallet_swaps(
    State(state): State<AppState>,
    Path(address): Path<String>,
    Query(p): Query<Pagination>,
) -> Result<Json<Page<SwapItem>>, ApiError> {
    let address = validate_address(&address)?;
    let (limit, offset) = p.validate()?;

    let items = sqlx::query_as!(
        SwapItem,
        r#"
        SELECT
            block_height,
            extrinsic_id,
            event_id,
            block_timestamp,
            caller,
            input_asset_id,
            input_amount  AS "input_amount!: BigDecimal",
            output_asset_id,
            output_amount AS "output_amount!: BigDecimal",
            usd_value     AS "usd_value: BigDecimal"
        FROM sm.live_swaps
        WHERE caller = $1
        ORDER BY block_height DESC, event_id DESC
        LIMIT $2 OFFSET $3
        "#,
        address,
        limit,
        offset,
    )
    .fetch_all(&state.db)
    .await?;

    Ok(Json(Page {
        items,
        page: p.page,
        limit: p.limit,
        total_known: None,
    }))
}

async fn wallet_transfers(
    State(state): State<AppState>,
    Path(address): Path<String>,
    Query(p): Query<Pagination>,
) -> Result<Json<Page<TransferItem>>, ApiError> {
    let address = validate_address(&address)?;
    let (limit, offset) = p.validate()?;

    // Single bind for the address — used as both the `from` and `to` filter.
    // Postgres reuses `$1` across both predicates without re-sending the value.
    let items = sqlx::query_as!(
        TransferItem,
        r#"
        SELECT
            block_height,
            extrinsic_id,
            event_id,
            block_timestamp,
            from_address,
            to_address,
            asset_id,
            amount    AS "amount!: BigDecimal",
            usd_value AS "usd_value: BigDecimal"
        FROM sm.live_transfers
        WHERE from_address = $1 OR to_address = $1
        ORDER BY block_height DESC, event_id DESC
        LIMIT $2 OFFSET $3
        "#,
        address,
        limit,
        offset,
    )
    .fetch_all(&state.db)
    .await?;

    Ok(Json(Page {
        items,
        page: p.page,
        limit: p.limit,
        total_known: None,
    }))
}

async fn wallet_bridges(
    State(state): State<AppState>,
    Path(address): Path<String>,
    Query(p): Query<Pagination>,
) -> Result<Json<Page<BridgeItem>>, ApiError> {
    let address = validate_address(&address)?;
    let (limit, offset) = p.validate()?;

    let items = sqlx::query_as!(
        BridgeItem,
        r#"
        SELECT
            block_height,
            extrinsic_id,
            event_id,
            block_timestamp,
            direction::text AS "direction!",
            network,
            caller,
            asset_id,
            amount AS "amount!: BigDecimal"
        FROM sm.live_bridges
        WHERE caller = $1
        ORDER BY block_height DESC, event_id DESC
        LIMIT $2 OFFSET $3
        "#,
        address,
        limit,
        offset,
    )
    .fetch_all(&state.db)
    .await?;

    Ok(Json(Page {
        items,
        page: p.page,
        limit: p.limit,
        total_known: None,
    }))
}

async fn wallet_fee_burns(
    State(state): State<AppState>,
    Path(address): Path<String>,
    Query(p): Query<Pagination>,
) -> Result<Json<Page<FeeBurnItem>>, ApiError> {
    let address = validate_address(&address)?;
    let (limit, offset) = p.validate()?;

    // For fee_burns, "this address paid the fee" maps to `payer = $1`.
    // The referrer share of a payer's fee is not their own activity, so
    // we filter strictly on `payer`.
    let items = sqlx::query_as!(
        FeeBurnItem,
        r#"
        SELECT
            block_height,
            extrinsic_id,
            event_id,
            block_timestamp,
            kind::text AS "kind!",
            payer,
            referrer,
            amount AS "amount!: BigDecimal"
        FROM sm.live_fee_burns
        WHERE payer = $1
        ORDER BY block_height DESC, event_id DESC
        LIMIT $2 OFFSET $3
        "#,
        address,
        limit,
        offset,
    )
    .fetch_all(&state.db)
    .await?;

    Ok(Json(Page {
        items,
        page: p.page,
        limit: p.limit,
        total_known: None,
    }))
}
