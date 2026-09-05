//! `/history/global/{swaps,transfers,bridges,fee_events}` — paginated
//! reads of the `sm.*` event tables.
//!
//! Pagination contract:
//! - `?limit=N` (default 25, max 100, min 1)
//! - `?before=<block_height>-<event_id>` — keyset cursor, PREFERRED.
//!   Returns rows strictly older than that position. The response's
//!   `next_before` feeds the next request; `null` means last page.
//! - `?page=N` (default 0) — legacy OFFSET fallback, kept for
//!   compatibility. Ignored when `before` is present. OFFSET cost grows
//!   linearly with depth (the legacy Node paid a 35× lesson for this);
//!   deep iteration must use `before`.
//! - response: `{ items, page, limit, next_before, total_known }`.
//!
//! Sort order: newest first by `(block_height DESC, event_id DESC)`.
//!
//! Field shapes are intentionally flat and untransformed:
//! - timestamps: ISO 8601 (`2026-04-21T18:53:06Z`)
//! - amounts: raw on-chain `BigDecimal` (planck integer). Frontend is
//!   responsible for dividing by `10^decimals` and resolving symbols
//!   from `sm.asset_registry`.
//! - asset ids: `0x`-prefixed lowercase hex. Account addresses: SS58
//!   ("cn…", SORA prefix 69) — whatever the ingest path stored.
//! - `extrinsic_id`: TEXT since migration 0006 (in-block index for live
//!   rows, legacy identifier for ETL rows). `hash` is the extrinsic
//!   hash, `null` for non-extrinsic events and legacy rows without it.

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
        .route("/history/global/fee_events", get(fee_burns))
        .route("/history/swaps/:address", get(wallet_swaps))
        .route("/history/transfers/:address", get(wallet_transfers))
        .route("/history/bridges/:address", get(wallet_bridges))
        .route("/history/fee_events/:address", get(wallet_fee_burns))
}

/// Common pagination query parameters.
#[derive(Debug, Deserialize)]
struct Pagination {
    #[serde(default = "Pagination::default_page")]
    page: i64,
    #[serde(default = "Pagination::default_limit")]
    limit: i64,
    /// Keyset cursor `"<block_height>-<event_id>"`.
    before: Option<String>,
}

/// Validated pagination: one uniform shape drives a single SQL form.
/// Without `before`, the keyset degenerates to `(i64::MAX, i32::MAX)`
/// (matches everything) + the legacy OFFSET.
struct PageSpec {
    limit: i64,
    offset: i64,
    before_block: i64,
    before_event: i32,
}

impl Pagination {
    fn default_page() -> i64 {
        0
    }
    fn default_limit() -> i64 {
        25
    }

    fn validate(&self) -> Result<PageSpec, ApiError> {
        if self.page < 0 {
            return Err(ApiError::BadRequest("page must be ≥ 0".into()));
        }
        if !(1..=100).contains(&self.limit) {
            return Err(ApiError::BadRequest(
                "limit must be between 1 and 100".into(),
            ));
        }

        match &self.before {
            Some(cursor) => {
                let (b, e) = cursor.split_once('-').ok_or_else(|| {
                    ApiError::BadRequest("before must be '<block_height>-<event_id>'".into())
                })?;
                let before_block: i64 = b.parse().map_err(|_| {
                    ApiError::BadRequest("before: block_height is not a number".into())
                })?;
                let before_event: i32 = e
                    .parse()
                    .map_err(|_| ApiError::BadRequest("before: event_id is not a number".into()))?;
                if before_block < 0 || before_event < 0 {
                    return Err(ApiError::BadRequest(
                        "before: components must be ≥ 0".into(),
                    ));
                }
                Ok(PageSpec {
                    limit: self.limit,
                    offset: 0,
                    before_block,
                    before_event,
                })
            }
            None => Ok(PageSpec {
                limit: self.limit,
                offset: self.page * self.limit,
                before_block: i64::MAX,
                before_event: i32::MAX,
            }),
        }
    }
}

/// Wrapper for a paginated list response.
#[derive(Serialize)]
struct Page<T> {
    items: Vec<T>,
    page: i64,
    limit: i64,
    /// Keyset cursor for the next page (`null` on the last page). Feed
    /// it back as `?before=` — O(1) at any depth, unlike `page`.
    next_before: Option<String>,
    /// Total row count if cheaply known (`pg_class.reltuples`). `None`
    /// for now — caller paginates until `items.len() < limit`.
    total_known: Option<i64>,
}

/// Cursor for the page after this one: position of the last row, only
/// when the page came back full (a short page IS the last page).
fn next_cursor(len: usize, limit: i64, last: Option<(i64, i32)>) -> Option<String> {
    if len < limit as usize {
        return None;
    }
    last.map(|(b, e)| format!("{b}-{e}"))
}

// =============================================================
// /history/global/swaps
// =============================================================

#[derive(Serialize)]
struct SwapItem {
    block_height: i64,
    extrinsic_id: String,
    event_id: i32,
    hash: Option<String>,
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
    let spec = p.validate()?;

    let items = sqlx::query_as!(
        SwapItem,
        r#"
        SELECT
            block_height,
            extrinsic_id,
            event_id,
            hash,
            block_timestamp,
            caller,
            input_asset_id,
            input_amount  AS "input_amount!: BigDecimal",
            output_asset_id,
            output_amount AS "output_amount!: BigDecimal",
            usd_value     AS "usd_value: BigDecimal"
        FROM sm.swaps
        WHERE (block_height, event_id) < ($1, $2)
        ORDER BY block_height DESC, event_id DESC
        LIMIT $3 OFFSET $4
        "#,
        spec.before_block,
        spec.before_event,
        spec.limit,
        spec.offset,
    )
    .fetch_all(&state.db)
    .await?;

    let next = next_cursor(
        items.len(),
        spec.limit,
        items.last().map(|i| (i.block_height, i.event_id)),
    );
    Ok(Json(Page {
        items,
        page: p.page,
        limit: p.limit,
        next_before: next,
        total_known: None,
    }))
}

// =============================================================
// /history/global/transfers
// =============================================================

#[derive(Serialize)]
struct TransferItem {
    block_height: i64,
    extrinsic_id: String,
    event_id: i32,
    hash: Option<String>,
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
    let spec = p.validate()?;

    let items = sqlx::query_as!(
        TransferItem,
        r#"
        SELECT
            block_height,
            extrinsic_id,
            event_id,
            hash,
            block_timestamp,
            from_address,
            to_address,
            asset_id,
            amount    AS "amount!: BigDecimal",
            usd_value AS "usd_value: BigDecimal"
        FROM sm.transfers
        WHERE (block_height, event_id) < ($1, $2)
        ORDER BY block_height DESC, event_id DESC
        LIMIT $3 OFFSET $4
        "#,
        spec.before_block,
        spec.before_event,
        spec.limit,
        spec.offset,
    )
    .fetch_all(&state.db)
    .await?;

    let next = next_cursor(
        items.len(),
        spec.limit,
        items.last().map(|i| (i.block_height, i.event_id)),
    );
    Ok(Json(Page {
        items,
        page: p.page,
        limit: p.limit,
        next_before: next,
        total_known: None,
    }))
}

// =============================================================
// /history/global/bridges
// =============================================================

#[derive(Serialize)]
struct BridgeItem {
    block_height: i64,
    extrinsic_id: String,
    event_id: i32,
    hash: Option<String>,
    block_timestamp: DateTime<Utc>,
    direction: String,
    network: String,
    caller: String,
    counterparty: Option<String>,
    asset_id: String,
    amount: BigDecimal,
    usd_value: Option<BigDecimal>,
}

async fn bridges(
    State(state): State<AppState>,
    Query(p): Query<Pagination>,
) -> Result<Json<Page<BridgeItem>>, ApiError> {
    let spec = p.validate()?;

    // The `direction` column is a Postgres ENUM (`sm.bridge_direction`).
    // Cast to TEXT for transport so we can keep `BridgeItem.direction`
    // as plain `String` (frontend already expects "in" / "out").
    let items = sqlx::query_as!(
        BridgeItem,
        r#"
        SELECT
            block_height,
            extrinsic_id,
            event_id,
            hash,
            block_timestamp,
            direction::text AS "direction!",
            network,
            caller,
            counterparty,
            asset_id,
            amount    AS "amount!: BigDecimal",
            usd_value AS "usd_value: BigDecimal"
        FROM sm.bridges
        WHERE (block_height, event_id) < ($1, $2)
        ORDER BY block_height DESC, event_id DESC
        LIMIT $3 OFFSET $4
        "#,
        spec.before_block,
        spec.before_event,
        spec.limit,
        spec.offset,
    )
    .fetch_all(&state.db)
    .await?;

    let next = next_cursor(
        items.len(),
        spec.limit,
        items.last().map(|i| (i.block_height, i.event_id)),
    );
    Ok(Json(Page {
        items,
        page: p.page,
        limit: p.limit,
        next_before: next,
        total_known: None,
    }))
}

// =============================================================
// /history/global/fee_events
// =============================================================

#[derive(Serialize)]
struct FeeBurnItem {
    block_height: i64,
    extrinsic_id: String,
    event_id: i32,
    hash: Option<String>,
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
    let spec = p.validate()?;

    let items = sqlx::query_as!(
        FeeBurnItem,
        r#"
        SELECT
            block_height,
            extrinsic_id,
            event_id,
            hash,
            block_timestamp,
            kind::text AS "kind!",
            payer,
            referrer,
            amount AS "amount!: BigDecimal"
        FROM sm.fee_events
        WHERE (block_height, event_id) < ($1, $2)
        ORDER BY block_height DESC, event_id DESC
        LIMIT $3 OFFSET $4
        "#,
        spec.before_block,
        spec.before_event,
        spec.limit,
        spec.offset,
    )
    .fetch_all(&state.db)
    .await?;

    let next = next_cursor(
        items.len(),
        spec.limit,
        items.last().map(|i| (i.block_height, i.event_id)),
    );
    Ok(Json(Page {
        items,
        page: p.page,
        limit: p.limit,
        next_before: next,
        total_known: None,
    }))
}

// =============================================================
// Per-wallet variants: /history/{swaps,transfers,bridges,fee_events}/:address
// =============================================================

async fn wallet_swaps(
    State(state): State<AppState>,
    Path(address): Path<String>,
    Query(p): Query<Pagination>,
) -> Result<Json<Page<SwapItem>>, ApiError> {
    let address = validate_address(&address)?;
    let spec = p.validate()?;

    let items = sqlx::query_as!(
        SwapItem,
        r#"
        SELECT
            block_height,
            extrinsic_id,
            event_id,
            hash,
            block_timestamp,
            caller,
            input_asset_id,
            input_amount  AS "input_amount!: BigDecimal",
            output_asset_id,
            output_amount AS "output_amount!: BigDecimal",
            usd_value     AS "usd_value: BigDecimal"
        FROM sm.swaps
        WHERE caller = $1 AND (block_height, event_id) < ($2, $3)
        ORDER BY block_height DESC, event_id DESC
        LIMIT $4 OFFSET $5
        "#,
        address,
        spec.before_block,
        spec.before_event,
        spec.limit,
        spec.offset,
    )
    .fetch_all(&state.db)
    .await?;

    let next = next_cursor(
        items.len(),
        spec.limit,
        items.last().map(|i| (i.block_height, i.event_id)),
    );
    Ok(Json(Page {
        items,
        page: p.page,
        limit: p.limit,
        next_before: next,
        total_known: None,
    }))
}

async fn wallet_transfers(
    State(state): State<AppState>,
    Path(address): Path<String>,
    Query(p): Query<Pagination>,
) -> Result<Json<Page<TransferItem>>, ApiError> {
    let address = validate_address(&address)?;
    let spec = p.validate()?;

    // Single bind for the address — used as both the `from` and `to` filter.
    let items = sqlx::query_as!(
        TransferItem,
        r#"
        SELECT
            block_height,
            extrinsic_id,
            event_id,
            hash,
            block_timestamp,
            from_address,
            to_address,
            asset_id,
            amount    AS "amount!: BigDecimal",
            usd_value AS "usd_value: BigDecimal"
        FROM sm.transfers
        WHERE (from_address = $1 OR to_address = $1)
          AND (block_height, event_id) < ($2, $3)
        ORDER BY block_height DESC, event_id DESC
        LIMIT $4 OFFSET $5
        "#,
        address,
        spec.before_block,
        spec.before_event,
        spec.limit,
        spec.offset,
    )
    .fetch_all(&state.db)
    .await?;

    let next = next_cursor(
        items.len(),
        spec.limit,
        items.last().map(|i| (i.block_height, i.event_id)),
    );
    Ok(Json(Page {
        items,
        page: p.page,
        limit: p.limit,
        next_before: next,
        total_known: None,
    }))
}

async fn wallet_bridges(
    State(state): State<AppState>,
    Path(address): Path<String>,
    Query(p): Query<Pagination>,
) -> Result<Json<Page<BridgeItem>>, ApiError> {
    let address = validate_address(&address)?;
    let spec = p.validate()?;

    let items = sqlx::query_as!(
        BridgeItem,
        r#"
        SELECT
            block_height,
            extrinsic_id,
            event_id,
            hash,
            block_timestamp,
            direction::text AS "direction!",
            network,
            caller,
            counterparty,
            asset_id,
            amount    AS "amount!: BigDecimal",
            usd_value AS "usd_value: BigDecimal"
        FROM sm.bridges
        WHERE caller = $1 AND (block_height, event_id) < ($2, $3)
        ORDER BY block_height DESC, event_id DESC
        LIMIT $4 OFFSET $5
        "#,
        address,
        spec.before_block,
        spec.before_event,
        spec.limit,
        spec.offset,
    )
    .fetch_all(&state.db)
    .await?;

    let next = next_cursor(
        items.len(),
        spec.limit,
        items.last().map(|i| (i.block_height, i.event_id)),
    );
    Ok(Json(Page {
        items,
        page: p.page,
        limit: p.limit,
        next_before: next,
        total_known: None,
    }))
}

async fn wallet_fee_burns(
    State(state): State<AppState>,
    Path(address): Path<String>,
    Query(p): Query<Pagination>,
) -> Result<Json<Page<FeeBurnItem>>, ApiError> {
    let address = validate_address(&address)?;
    let spec = p.validate()?;

    // "This address paid the fee" maps to `payer = $1`; the referrer
    // share of someone else's fee is not this wallet's own activity.
    let items = sqlx::query_as!(
        FeeBurnItem,
        r#"
        SELECT
            block_height,
            extrinsic_id,
            event_id,
            hash,
            block_timestamp,
            kind::text AS "kind!",
            payer,
            referrer,
            amount AS "amount!: BigDecimal"
        FROM sm.fee_events
        WHERE payer = $1 AND (block_height, event_id) < ($2, $3)
        ORDER BY block_height DESC, event_id DESC
        LIMIT $4 OFFSET $5
        "#,
        address,
        spec.before_block,
        spec.before_event,
        spec.limit,
        spec.offset,
    )
    .fetch_all(&state.db)
    .await?;

    let next = next_cursor(
        items.len(),
        spec.limit,
        items.last().map(|i| (i.block_height, i.event_id)),
    );
    Ok(Json(Page {
        items,
        page: p.page,
        limit: p.limit,
        next_before: next,
        total_known: None,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pag(page: i64, limit: i64, before: Option<&str>) -> Pagination {
        Pagination {
            page,
            limit,
            before: before.map(String::from),
        }
    }

    #[test]
    fn offset_mode_when_no_before() {
        let s = pag(3, 25, None).validate().unwrap();
        assert_eq!(s.offset, 75);
        assert_eq!(s.before_block, i64::MAX);
        assert_eq!(s.before_event, i32::MAX);
    }

    #[test]
    fn keyset_mode_parses_cursor_and_zeroes_offset() {
        let s = pag(9, 25, Some("27245127-34")).validate().unwrap();
        assert_eq!(s.offset, 0, "page must be ignored with before");
        assert_eq!(s.before_block, 27245127);
        assert_eq!(s.before_event, 34);
    }

    #[test]
    fn keyset_rejects_malformed() {
        for bad in ["27245127", "a-b", "12-x", "-5-3", ""] {
            assert!(
                pag(0, 25, Some(bad)).validate().is_err(),
                "should reject {bad:?}"
            );
        }
    }

    #[test]
    fn limit_bounds_still_enforced() {
        assert!(pag(0, 0, None).validate().is_err());
        assert!(pag(0, 101, None).validate().is_err());
        assert!(pag(-1, 10, None).validate().is_err());
    }

    #[test]
    fn next_cursor_only_on_full_pages() {
        assert_eq!(
            next_cursor(10, 25, Some((100, 5))),
            None,
            "short page = last"
        );
        assert_eq!(
            next_cursor(25, 25, Some((100, 5))),
            Some("100-5".into()),
            "full page continues"
        );
        assert_eq!(next_cursor(0, 25, None), None);
    }
}
