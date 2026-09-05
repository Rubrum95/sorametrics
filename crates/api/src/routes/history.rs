//! `/history/global/{swaps,transfers,bridges}` and
//! `/history/{swaps,transfers,bridges}/:address` — the legacy Node
//! contract, served from the `sm.*` event tables.
//!
//! Contract (from `index.js` + `db_pg.js::paginatedQuery`), which the
//! unchanged frontend depends on:
//! - `?page=N` 1-based (default 1), `?limit=M` (default 25, bridges and
//!   per-wallet transfers/bridges 20; max 100).
//! - `?token=` / `?filter=`: case-insensitive substring over the
//!   symbols (swaps), symbols + addresses (transfers), addresses +
//!   network + asset id (bridges). `?timestamp=<unix ms>`: rows at or
//!   before that instant.
//! - response `{ data, total, page, totalPages }`. `total` is the
//!   planner estimate (`pg_class.reltuples`) when unfiltered — the same
//!   shortcut the Node takes — and an exact COUNT when filtered. `page`
//!   is clamped to `[1, totalPages]`.
//! - row shapes: see [`crate::legacy`].
//!
//! v33 addition (additive, ignored by the legacy frontend):
//! `?before=<block_height>-<event_id>` keyset cursor + `next_before` in
//! the response. O(1) at any depth where `page` degrades linearly.
//!
//! `/history/global/fee_events` and `/history/fee_events/:address`
//! have no Node counterpart; they use the same envelope with flat rows.

use crate::legacy::{
    bridge_direction_label, bridge_parties, decimals_for, fmt_amount, fmt_extrinsic_id, fmt_millis,
    fmt_time, fmt_usd, logo_for, page_bounds, symbol_for,
};
use crate::state::Registry;
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

/// Query parameters shared by every history endpoint.
#[derive(Debug, Default, Deserialize)]
struct Pagination {
    page: Option<i64>,
    limit: Option<i64>,
    /// Keyset cursor `"<block_height>-<event_id>"` (v33 addition).
    before: Option<String>,
    /// Legacy substring filter (`?token=` is the swaps alias).
    filter: Option<String>,
    token: Option<String>,
    /// Legacy "rows at or before" bound, unix milliseconds.
    timestamp: Option<String>,
}

/// Validated pagination driving one uniform SQL form: keyset sentinel
/// `(before_block, before_event)` (`(MAX, MAX)` when paging by number)
/// + OFFSET (0 when a cursor is present).
#[derive(Debug, Clone, PartialEq, Eq)]
struct PageSpec {
    page: i64,
    limit: i64,
    /// `true` when `?before=` drives the page (OFFSET is then 0).
    keyset: bool,
    before_block: i64,
    before_event: i32,
    /// Upper bound on `block_timestamp`, if `?timestamp=` was given.
    until: Option<DateTime<Utc>>,
    /// Trimmed substring filter, if any.
    needle: Option<String>,
}

impl PageSpec {
    /// Node: `page` is clamped to `[1, totalPages]` BEFORE the offset is
    /// taken, so an out-of-range page returns the last page, not an
    /// empty one. Keyset requests ignore page/offset.
    fn resolve(&self, total: i64) -> (i64, i64, i64) {
        let (total_pages, page) = page_bounds(total, self.limit, self.page);
        let offset = if self.keyset {
            0
        } else {
            (page - 1) * self.limit
        };
        (total_pages, page, offset)
    }

    /// `%needle%` for ILIKE, if a filter was given.
    fn like_pattern(&self) -> Option<String> {
        self.needle.as_ref().map(|n| format!("%{n}%"))
    }

    /// `true` when the request carries no row filter (so the planner
    /// estimate stands in for the total, as in the Node).
    fn is_unfiltered(&self) -> bool {
        self.needle.is_none() && self.until.is_none()
    }
}

impl Pagination {
    fn validate(&self, default_limit: i64) -> Result<PageSpec, ApiError> {
        let page = self.page.unwrap_or(1);
        if page < 1 {
            return Err(ApiError::BadRequest("page must be ≥ 1".into()));
        }
        let limit = self.limit.unwrap_or(default_limit);
        if !(1..=100).contains(&limit) {
            return Err(ApiError::BadRequest(
                "limit must be between 1 and 100".into(),
            ));
        }

        let until = match self.timestamp.as_deref().map(str::trim) {
            Some("") | None => None,
            Some(raw) => {
                let ms: i64 = raw.parse().map_err(|_| {
                    ApiError::BadRequest("timestamp must be unix milliseconds".into())
                })?;
                Some(
                    DateTime::from_timestamp_millis(ms)
                        .ok_or_else(|| ApiError::BadRequest("timestamp out of range".into()))?,
                )
            }
        };

        let needle = self
            .token
            .as_deref()
            .or(self.filter.as_deref())
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(str::to_string);

        let (keyset, before_block, before_event) = match &self.before {
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
                (true, before_block, before_event)
            }
            None => (false, i64::MAX, i32::MAX),
        };

        Ok(PageSpec {
            page,
            limit,
            keyset,
            before_block,
            before_event,
            until,
            needle,
        })
    }
}

/// Legacy list envelope.
#[derive(Serialize)]
struct Page<T> {
    data: Vec<T>,
    total: i64,
    page: i64,
    #[serde(rename = "totalPages")]
    total_pages: i64,
    /// v33 keyset cursor for the next page (`null` on the last page).
    next_before: Option<String>,
}

impl<T> Page<T> {
    fn build(
        data: Vec<T>,
        total: i64,
        (total_pages, page): (i64, i64),
        limit: i64,
        last: Option<(i64, i32)>,
    ) -> Self {
        let next_before = next_cursor(data.len(), limit, last);
        Self {
            data,
            total,
            page,
            total_pages,
            next_before,
        }
    }
}

/// Cursor for the page after this one: position of the last row, only
/// when the page came back full (a short page IS the last page).
fn next_cursor(len: usize, limit: i64, last: Option<(i64, i32)>) -> Option<String> {
    if len < limit as usize {
        return None;
    }
    last.map(|(b, e)| format!("{b}-{e}"))
}

/// Planner row estimate for an `sm.*` table (`pg_class.reltuples`, the
/// Node's unfiltered `total`). `None` when the table was never analysed
/// (PG14 reports `-1`) so the caller falls back to an exact count.
async fn estimated_rows(state: &AppState, table: &str) -> Result<Option<i64>, ApiError> {
    let row = sqlx::query!(
        r#"
        SELECT c.reltuples::bigint AS "estimate!"
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = 'sm' AND c.relname = $1
        "#,
        table,
    )
    .fetch_optional(&state.db)
    .await?;
    Ok(row.map(|r| r.estimate).filter(|e| *e >= 0))
}

// =============================================================
// Swaps
// =============================================================

/// Legacy swap leg.
#[derive(Serialize)]
struct SwapLeg {
    symbol: String,
    amount: String,
    logo: String,
    usd: f64,
}

/// Legacy swap row (`db_pg.js::mapSwaps`).
#[derive(Serialize)]
struct SwapRow {
    time: String,
    block: i64,
    hash: String,
    extrinsic_id: String,
    wallet: String,
    #[serde(rename = "in")]
    input: SwapLeg,
    out: SwapLeg,
}

struct SwapRecord {
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
    output_usd_value: Option<BigDecimal>,
}

fn swap_row(r: &SwapRecord, registry: &Registry, zone: chrono_tz::Tz) -> SwapRow {
    let leg = |asset: &str, amount: &BigDecimal, usd: Option<&BigDecimal>| SwapLeg {
        symbol: symbol_for(registry, asset),
        amount: fmt_amount(amount, decimals_for(registry, asset)),
        logo: logo_for(registry, asset),
        usd: fmt_usd(usd),
    };
    SwapRow {
        time: fmt_time(r.block_timestamp, zone),
        block: r.block_height,
        hash: r.hash.clone().unwrap_or_default(),
        extrinsic_id: fmt_extrinsic_id(r.block_height, &r.extrinsic_id),
        wallet: r.caller.clone(),
        input: leg(&r.input_asset_id, &r.input_amount, r.usd_value.as_ref()),
        out: leg(
            &r.output_asset_id,
            &r.output_amount,
            r.output_usd_value.as_ref(),
        ),
    }
}

async fn swaps_page(
    state: &AppState,
    spec: &PageSpec,
    wallet: Option<&str>,
) -> Result<Json<Page<SwapRow>>, ApiError> {
    let registry = state.registry.read().await;
    // Symbol filter resolved to asset ids up front; a needle that
    // matches no symbol must match no row (empty array, not NULL).
    let asset_ids: Option<Vec<String>> = spec
        .needle
        .as_deref()
        .map(|n| registry.asset_ids_matching(n));

    let total = match (wallet, spec.is_unfiltered()) {
        (None, true) => match estimated_rows(state, "swaps").await? {
            Some(e) => e,
            None => exact_swaps_count(state, spec, wallet, asset_ids.as_deref()).await?,
        },
        _ => exact_swaps_count(state, spec, wallet, asset_ids.as_deref()).await?,
    };

    let (total_pages, page, offset) = spec.resolve(total);

    let rows = sqlx::query_as!(
        SwapRecord,
        r#"
        SELECT
            block_height,
            extrinsic_id,
            event_id,
            hash,
            block_timestamp,
            caller,
            input_asset_id,
            input_amount     AS "input_amount!: BigDecimal",
            output_asset_id,
            output_amount    AS "output_amount!: BigDecimal",
            usd_value        AS "usd_value: BigDecimal",
            output_usd_value AS "output_usd_value: BigDecimal"
        FROM sm.swaps
        WHERE (block_height, event_id) < ($1, $2)
          AND ($5::text IS NULL OR caller = $5)
          AND ($6::text[] IS NULL OR input_asset_id = ANY($6) OR output_asset_id = ANY($6))
          AND ($7::timestamptz IS NULL OR block_timestamp <= $7)
        ORDER BY block_height DESC, event_id DESC
        LIMIT $3 OFFSET $4
        "#,
        spec.before_block,
        spec.before_event,
        spec.limit,
        offset,
        wallet,
        asset_ids.as_deref(),
        spec.until,
    )
    .fetch_all(&state.db)
    .await?;

    let last = rows.last().map(|r| (r.block_height, r.event_id));
    let data = rows
        .iter()
        .map(|r| swap_row(r, &registry, state.time_zone))
        .collect();
    Ok(Json(Page::build(
        data,
        total,
        (total_pages, page),
        spec.limit,
        last,
    )))
}

async fn exact_swaps_count(
    state: &AppState,
    spec: &PageSpec,
    wallet: Option<&str>,
    asset_ids: Option<&[String]>,
) -> Result<i64, ApiError> {
    let row = sqlx::query!(
        r#"
        SELECT COUNT(*) AS "count!"
        FROM sm.swaps
        WHERE ($1::text IS NULL OR caller = $1)
          AND ($2::text[] IS NULL OR input_asset_id = ANY($2) OR output_asset_id = ANY($2))
          AND ($3::timestamptz IS NULL OR block_timestamp <= $3)
        "#,
        wallet,
        asset_ids,
        spec.until,
    )
    .fetch_one(&state.db)
    .await?;
    Ok(row.count)
}

async fn swaps(
    State(state): State<AppState>,
    Query(p): Query<Pagination>,
) -> Result<Json<Page<SwapRow>>, ApiError> {
    let spec = p.validate(25)?;
    swaps_page(&state, &spec, None).await
}

async fn wallet_swaps(
    State(state): State<AppState>,
    Path(address): Path<String>,
    Query(p): Query<Pagination>,
) -> Result<Json<Page<SwapRow>>, ApiError> {
    let address = validate_address(&address)?;
    let spec = p.validate(25)?;
    swaps_page(&state, &spec, Some(&address)).await
}

// =============================================================
// Transfers
// =============================================================

/// Legacy transfer row (`db_pg.js::mapTransfers`).
#[derive(Serialize)]
struct TransferRow {
    time: String,
    block: i64,
    hash: String,
    extrinsic_id: String,
    from: String,
    to: String,
    amount: String,
    symbol: String,
    logo: String,
    #[serde(rename = "usdValue")]
    usd_value: f64,
    #[serde(rename = "assetId")]
    asset_id: String,
}

struct TransferRecord {
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

fn transfer_row(r: &TransferRecord, registry: &Registry, zone: chrono_tz::Tz) -> TransferRow {
    TransferRow {
        time: fmt_time(r.block_timestamp, zone),
        block: r.block_height,
        hash: r.hash.clone().unwrap_or_default(),
        extrinsic_id: fmt_extrinsic_id(r.block_height, &r.extrinsic_id),
        from: r.from_address.clone(),
        to: r.to_address.clone(),
        amount: fmt_amount(&r.amount, decimals_for(registry, &r.asset_id)),
        symbol: symbol_for(registry, &r.asset_id),
        logo: logo_for(registry, &r.asset_id),
        usd_value: fmt_usd(r.usd_value.as_ref()),
        asset_id: r.asset_id.clone(),
    }
}

async fn transfers_page(
    state: &AppState,
    spec: &PageSpec,
    wallet: Option<&str>,
) -> Result<Json<Page<TransferRow>>, ApiError> {
    let registry = state.registry.read().await;
    let asset_ids: Option<Vec<String>> = spec
        .needle
        .as_deref()
        .map(|n| registry.asset_ids_matching(n));
    let pattern = spec.like_pattern();

    let total = match (wallet, spec.is_unfiltered()) {
        (None, true) => match estimated_rows(state, "transfers").await? {
            Some(e) => e,
            None => exact_transfers_count(state, spec, wallet, asset_ids.as_deref()).await?,
        },
        _ => exact_transfers_count(state, spec, wallet, asset_ids.as_deref()).await?,
    };

    let (total_pages, page, offset) = spec.resolve(total);

    let rows = sqlx::query_as!(
        TransferRecord,
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
          AND ($5::text IS NULL OR from_address = $5 OR to_address = $5)
          AND ($6::text[] IS NULL OR asset_id = ANY($6)
               OR from_address ILIKE $7 OR to_address ILIKE $7)
          AND ($8::timestamptz IS NULL OR block_timestamp <= $8)
        ORDER BY block_height DESC, event_id DESC
        LIMIT $3 OFFSET $4
        "#,
        spec.before_block,
        spec.before_event,
        spec.limit,
        offset,
        wallet,
        asset_ids.as_deref(),
        pattern,
        spec.until,
    )
    .fetch_all(&state.db)
    .await?;

    let last = rows.last().map(|r| (r.block_height, r.event_id));
    let data = rows
        .iter()
        .map(|r| transfer_row(r, &registry, state.time_zone))
        .collect();
    Ok(Json(Page::build(
        data,
        total,
        (total_pages, page),
        spec.limit,
        last,
    )))
}

async fn exact_transfers_count(
    state: &AppState,
    spec: &PageSpec,
    wallet: Option<&str>,
    asset_ids: Option<&[String]>,
) -> Result<i64, ApiError> {
    let pattern = spec.like_pattern();
    let row = sqlx::query!(
        r#"
        SELECT COUNT(*) AS "count!"
        FROM sm.transfers
        WHERE ($1::text IS NULL OR from_address = $1 OR to_address = $1)
          AND ($2::text[] IS NULL OR asset_id = ANY($2)
               OR from_address ILIKE $3 OR to_address ILIKE $3)
          AND ($4::timestamptz IS NULL OR block_timestamp <= $4)
        "#,
        wallet,
        asset_ids,
        pattern,
        spec.until,
    )
    .fetch_one(&state.db)
    .await?;
    Ok(row.count)
}

async fn transfers(
    State(state): State<AppState>,
    Query(p): Query<Pagination>,
) -> Result<Json<Page<TransferRow>>, ApiError> {
    let spec = p.validate(25)?;
    transfers_page(&state, &spec, None).await
}

async fn wallet_transfers(
    State(state): State<AppState>,
    Path(address): Path<String>,
    Query(p): Query<Pagination>,
) -> Result<Json<Page<TransferRow>>, ApiError> {
    let address = validate_address(&address)?;
    let spec = p.validate(20)?;
    transfers_page(&state, &spec, Some(&address)).await
}

// =============================================================
// Bridges
// =============================================================

/// Legacy bridge row (`BRIDGE_COLS` + `time`/`logo`, `getLatestBridges`).
#[derive(Serialize)]
struct BridgeRow {
    timestamp: String,
    block: i64,
    network: String,
    direction: &'static str,
    sender: String,
    recipient: String,
    asset_id: String,
    symbol: String,
    amount: String,
    usd_value: f64,
    hash: String,
    extrinsic_id: String,
    time: String,
    logo: String,
}

struct BridgeRecord {
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

fn bridge_row(r: &BridgeRecord, registry: &Registry, zone: chrono_tz::Tz) -> BridgeRow {
    let (sender, recipient) = bridge_parties(&r.direction, &r.caller, r.counterparty.as_deref());
    BridgeRow {
        timestamp: fmt_millis(r.block_timestamp),
        block: r.block_height,
        network: r.network.clone(),
        direction: bridge_direction_label(&r.direction),
        sender,
        recipient,
        asset_id: r.asset_id.clone(),
        symbol: symbol_for(registry, &r.asset_id),
        amount: fmt_amount(&r.amount, decimals_for(registry, &r.asset_id)),
        usd_value: fmt_usd(r.usd_value.as_ref()),
        hash: r.hash.clone().unwrap_or_default(),
        extrinsic_id: fmt_extrinsic_id(r.block_height, &r.extrinsic_id),
        time: fmt_time(r.block_timestamp, zone),
        logo: logo_for(registry, &r.asset_id),
    }
}

async fn bridges_page(
    state: &AppState,
    spec: &PageSpec,
    wallet: Option<&str>,
) -> Result<Json<Page<BridgeRow>>, ApiError> {
    let registry = state.registry.read().await;
    let pattern = spec.like_pattern();

    let total = match (wallet, spec.is_unfiltered()) {
        (None, true) => match estimated_rows(state, "bridges").await? {
            Some(e) => e,
            None => exact_bridges_count(state, spec, wallet).await?,
        },
        _ => exact_bridges_count(state, spec, wallet).await?,
    };

    let (total_pages, page, offset) = spec.resolve(total);

    // `direction` is a Postgres ENUM; cast to TEXT for transport.
    let rows = sqlx::query_as!(
        BridgeRecord,
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
          AND ($5::text IS NULL OR caller = $5 OR counterparty = $5)
          AND ($6::text IS NULL OR caller ILIKE $6 OR counterparty ILIKE $6
               OR network ILIKE $6 OR asset_id ILIKE $6)
          AND ($7::timestamptz IS NULL OR block_timestamp <= $7)
        ORDER BY block_height DESC, event_id DESC
        LIMIT $3 OFFSET $4
        "#,
        spec.before_block,
        spec.before_event,
        spec.limit,
        offset,
        wallet,
        pattern,
        spec.until,
    )
    .fetch_all(&state.db)
    .await?;

    let last = rows.last().map(|r| (r.block_height, r.event_id));
    let data = rows
        .iter()
        .map(|r| bridge_row(r, &registry, state.time_zone))
        .collect();
    Ok(Json(Page::build(
        data,
        total,
        (total_pages, page),
        spec.limit,
        last,
    )))
}

async fn exact_bridges_count(
    state: &AppState,
    spec: &PageSpec,
    wallet: Option<&str>,
) -> Result<i64, ApiError> {
    let pattern = spec.like_pattern();
    let row = sqlx::query!(
        r#"
        SELECT COUNT(*) AS "count!"
        FROM sm.bridges
        WHERE ($1::text IS NULL OR caller = $1 OR counterparty = $1)
          AND ($2::text IS NULL OR caller ILIKE $2 OR counterparty ILIKE $2
               OR network ILIKE $2 OR asset_id ILIKE $2)
          AND ($3::timestamptz IS NULL OR block_timestamp <= $3)
        "#,
        wallet,
        pattern,
        spec.until,
    )
    .fetch_one(&state.db)
    .await?;
    Ok(row.count)
}

async fn bridges(
    State(state): State<AppState>,
    Query(p): Query<Pagination>,
) -> Result<Json<Page<BridgeRow>>, ApiError> {
    let spec = p.validate(20)?;
    bridges_page(&state, &spec, None).await
}

async fn wallet_bridges(
    State(state): State<AppState>,
    Path(address): Path<String>,
    Query(p): Query<Pagination>,
) -> Result<Json<Page<BridgeRow>>, ApiError> {
    let address = validate_address(&address)?;
    let spec = p.validate(20)?;
    bridges_page(&state, &spec, Some(&address)).await
}

// =============================================================
// Fee events (v33 only — no Node counterpart)
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

async fn fee_burns_page(
    state: &AppState,
    spec: &PageSpec,
    payer: Option<&str>,
) -> Result<Json<Page<FeeBurnItem>>, ApiError> {
    let total = match (payer, spec.is_unfiltered()) {
        (None, true) => estimated_rows(state, "fee_events").await?.unwrap_or(0),
        _ => {
            sqlx::query!(
                r#"
                SELECT COUNT(*) AS "count!"
                FROM sm.fee_events
                WHERE ($1::text IS NULL OR payer = $1)
                  AND ($2::timestamptz IS NULL OR block_timestamp <= $2)
                "#,
                payer,
                spec.until,
            )
            .fetch_one(&state.db)
            .await?
            .count
        }
    };

    let (total_pages, page, offset) = spec.resolve(total);

    // "This address paid the fee" maps to `payer = $5`; the referrer
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
        WHERE (block_height, event_id) < ($1, $2)
          AND ($5::text IS NULL OR payer = $5)
          AND ($6::timestamptz IS NULL OR block_timestamp <= $6)
        ORDER BY block_height DESC, event_id DESC
        LIMIT $3 OFFSET $4
        "#,
        spec.before_block,
        spec.before_event,
        spec.limit,
        offset,
        payer,
        spec.until,
    )
    .fetch_all(&state.db)
    .await?;

    let last = items.last().map(|i| (i.block_height, i.event_id));
    Ok(Json(Page::build(
        items,
        total,
        (total_pages, page),
        spec.limit,
        last,
    )))
}

async fn fee_burns(
    State(state): State<AppState>,
    Query(p): Query<Pagination>,
) -> Result<Json<Page<FeeBurnItem>>, ApiError> {
    let spec = p.validate(25)?;
    fee_burns_page(&state, &spec, None).await
}

async fn wallet_fee_burns(
    State(state): State<AppState>,
    Path(address): Path<String>,
    Query(p): Query<Pagination>,
) -> Result<Json<Page<FeeBurnItem>>, ApiError> {
    let address = validate_address(&address)?;
    let spec = p.validate(25)?;
    fee_burns_page(&state, &spec, Some(&address)).await
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pag(page: Option<i64>, limit: Option<i64>, before: Option<&str>) -> Pagination {
        Pagination {
            page,
            limit,
            before: before.map(String::from),
            ..Pagination::default()
        }
    }

    #[test]
    fn defaults_are_page_one_and_endpoint_limit() {
        let s = pag(None, None, None).validate(20).unwrap();
        assert_eq!((s.page, s.limit, s.keyset), (1, 20, false));
        assert_eq!(s.resolve(100), (5, 1, 0));
        assert_eq!((s.before_block, s.before_event), (i64::MAX, i32::MAX));
        assert!(s.is_unfiltered());
    }

    #[test]
    fn page_is_one_based_offset_and_clamped_like_the_node() {
        let s = pag(Some(3), Some(25), None).validate(25).unwrap();
        assert_eq!(s.resolve(1000), (40, 3, 50));
        // Out of range → last page, with its own offset (not an empty page).
        assert_eq!(s.resolve(60), (3, 3, 50));
        let s = pag(Some(999), Some(25), None).validate(25).unwrap();
        assert_eq!(s.resolve(60), (3, 3, 50));
        // Empty table → page 1, offset 0.
        assert_eq!(s.resolve(0), (0, 1, 0));
    }

    #[test]
    fn keyset_mode_parses_cursor_and_zeroes_offset() {
        let s = pag(Some(7), Some(10), Some("27000000-5"))
            .validate(25)
            .unwrap();
        assert_eq!(
            (s.before_block, s.before_event, s.keyset),
            (27_000_000, 5, true)
        );
        assert_eq!(s.resolve(1000).2, 0);
    }

    #[test]
    fn keyset_rejects_malformed() {
        for bad in ["nodash", "-1-2", "abc-1", "1-abc"] {
            assert!(pag(None, None, Some(bad)).validate(25).is_err(), "{bad}");
        }
    }

    #[test]
    fn limit_and_page_bounds_enforced() {
        assert!(pag(Some(0), None, None).validate(25).is_err());
        assert!(pag(None, Some(0), None).validate(25).is_err());
        assert!(pag(None, Some(101), None).validate(25).is_err());
        assert!(pag(None, Some(100), None).validate(25).is_ok());
    }

    #[test]
    fn token_alias_wins_over_filter_and_is_trimmed() {
        let p = Pagination {
            filter: Some("dai".into()),
            token: Some("  xor ".into()),
            ..Pagination::default()
        };
        let s = p.validate(25).unwrap();
        assert_eq!(s.needle.as_deref(), Some("xor"));
        assert_eq!(s.like_pattern().as_deref(), Some("%xor%"));
        assert!(!s.is_unfiltered());
    }

    #[test]
    fn timestamp_is_unix_millis() {
        let p = Pagination {
            timestamp: Some("1788623172000".into()),
            ..Pagination::default()
        };
        let s = p.validate(25).unwrap();
        assert_eq!(s.until.unwrap().timestamp(), 1_788_623_172);
        let bad = Pagination {
            timestamp: Some("yesterday".into()),
            ..Pagination::default()
        };
        assert!(bad.validate(25).is_err());
    }

    #[test]
    fn next_cursor_only_on_full_pages() {
        assert_eq!(next_cursor(10, 10, Some((5, 1))), Some("5-1".into()));
        assert_eq!(next_cursor(9, 10, Some((5, 1))), None);
        assert_eq!(next_cursor(0, 10, None), None);
    }
}
