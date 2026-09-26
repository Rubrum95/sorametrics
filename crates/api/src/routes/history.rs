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
//! `?before=<block_height>-<event_id>[-<extrinsic_id>]` keyset cursor + `next_before` in
//! the response. O(1) at any depth where `page` degrades linearly.
//!
//! `/history/global/fee_events` and `/history/fee_events/:address`
//! have no Node counterpart; they use the same envelope with flat rows.

use super::deep::{deep_start, DeepStart, Stream};
use crate::legacy::{
    bridge_direction_label, bridge_parties, decimals_for, fmt_amount, fmt_extrinsic_id, fmt_millis,
    fmt_time, fmt_usd, logo_for, page_bounds, seek, swap_usd, symbol_for, Seek,
};
use crate::state::Registry;
use crate::{
    error::ApiError,
    util::{validate_address, WalletSet},
    AppState,
};
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
    #[serde(default, deserialize_with = "crate::util::lenient_i64")]
    page: Option<i64>,
    #[serde(default, deserialize_with = "crate::util::lenient_i64")]
    limit: Option<i64>,
    /// Keyset cursor `"<block_height>-<event_id>"` (v33 addition).
    before: Option<String>,
    /// Legacy substring filter (`?token=` is the swaps alias).
    filter: Option<String>,
    token: Option<String>,
    /// Legacy "rows at or before" bound, unix milliseconds.
    timestamp: Option<String>,
    /// Exact token symbol, resolved to its canonical asset id (v33 addition).
    symbol: Option<String>,
    /// Exact bridge network label (v33 addition).
    network: Option<String>,
    /// Comma-separated wallets merged into one listing (v33 addition).
    wallets: Option<String>,
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
    /// Extrinsic id of the cursor row (`""` for a two-part cursor), the
    /// tiebreak between legacy rows of one block that share `event_id = 0`.
    before_ext: String,
    /// Upper bound on `block_timestamp`, if `?timestamp=` was given.
    until: Option<DateTime<Utc>>,
    /// Trimmed substring filter, if any.
    needle: Option<String>,
    /// Trimmed `?symbol=`, if any.
    symbol: Option<String>,
    /// Trimmed `?network=`, if any.
    network: Option<String>,
    /// Validated `?wallets=`, if any.
    wallets: Option<WalletSet>,
}

impl PageSpec {
    /// Node: `page` is clamped to `[1, totalPages]` BEFORE the offset is
    /// taken, so an out-of-range page returns the last page, not an
    /// empty one. Keyset requests ignore page/offset. `tail_ok` = some
    /// index yields the active filter oldest-first.
    fn resolve(&self, total: i64, tail_ok: bool) -> (i64, i64, Seek) {
        let (total_pages, page) = page_bounds(total, self.limit, self.page);
        let seek = match seek(total, self.limit, page) {
            _ if self.keyset => Seek::Head { offset: 0 },
            Seek::Tail { .. } if !tail_ok => Seek::Head {
                offset: (page - 1) * self.limit,
            },
            other => other,
        };
        (total_pages, page, seek)
    }

    /// `%needle%` for ILIKE, if a filter was given.
    fn like_pattern(&self) -> Option<String> {
        self.needle.as_ref().map(|n| format!("%{n}%"))
    }

    /// `?wallets=` stands alone: the merged listing supports paging only.
    fn wallets_alone(&self) -> Result<Option<&WalletSet>, ApiError> {
        let Some(wallets) = self.wallets.as_ref() else {
            return Ok(None);
        };
        let other = self.keyset
            || self.until.is_some()
            || self.needle.is_some()
            || self.symbol.is_some()
            || self.network.is_some();
        if other {
            return Err(ApiError::BadRequest(
                "wallets cannot be combined with other filters".into(),
            ));
        }
        Ok(Some(wallets))
    }

    /// Keyset sentinel for a newest-first page, lowered to `cap`.
    fn before(&self, cap: Option<(i64, i32)>) -> (i64, i32) {
        let own = (self.before_block, self.before_event);
        cap.map_or(own, |c| own.min(c))
    }

    /// `before` plus the extrinsic-id tiebreak, for
    /// `(block_height, event_id, extrinsic_id) < (…)`.
    fn head_bound(&self, cap: Option<(i64, i32)>) -> (i64, i32, &str) {
        let (block, event) = self.before(cap);
        let own = (block, event) == (self.before_block, self.before_event);
        (block, event, if own { &self.before_ext } else { "" })
    }
}

impl Pagination {
    fn validate(&self, default_limit: i64) -> Result<PageSpec, ApiError> {
        let page = crate::util::clamp_page(self.page);
        let limit = crate::util::clamp_limit(self.limit, default_limit, 100);

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

        let trimmed = |v: Option<&str>| {
            v.map(str::trim)
                .filter(|s| !s.is_empty())
                .map(str::to_string)
        };
        let needle = trimmed(self.token.as_deref().or(self.filter.as_deref()));

        let (keyset, before_block, before_event, before_ext) = match &self.before {
            Some(cursor) => {
                let mut parts = cursor.splitn(3, '-');
                let (Some(b), Some(e)) = (parts.next(), parts.next()) else {
                    return Err(ApiError::BadRequest(
                        "before must be '<block_height>-<event_id>[-<extrinsic_id>]'".into(),
                    ));
                };
                let ext = parts.next().unwrap_or_default().to_string();
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
                (true, before_block, before_event, ext)
            }
            None => (false, i64::MAX, i32::MAX, String::new()),
        };

        Ok(PageSpec {
            page,
            limit,
            keyset,
            before_block,
            before_event,
            before_ext,
            until,
            needle,
            symbol: trimmed(self.symbol.as_deref()),
            network: trimmed(self.network.as_deref()),
            wallets: self
                .wallets
                .as_deref()
                .map(crate::util::parse_wallets)
                .transpose()?,
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
    /// `?wallets=` entries that are not SORA accounts (left out).
    #[serde(skip_serializing_if = "Option::is_none")]
    invalid_wallets: Option<Vec<String>>,
    /// `?wallets=` entries and the canonical address each was read as.
    #[serde(skip_serializing_if = "Option::is_none")]
    resolved_wallets: Option<Vec<ResolvedWallet>>,
}

#[derive(Serialize)]
pub(super) struct ResolvedWallet {
    input: String,
    address: String,
}

pub(super) fn resolved_wallets(set: &WalletSet) -> Vec<ResolvedWallet> {
    set.resolved
        .iter()
        .map(|(input, address)| ResolvedWallet {
            input: input.clone(),
            address: address.clone(),
        })
        .collect()
}

impl<T> Page<T> {
    fn build(
        data: Vec<T>,
        total: i64,
        (total_pages, page): (i64, i64),
        limit: i64,
        last: Option<(i64, i32, String)>,
    ) -> Self {
        let next_before = next_cursor(data.len(), limit, last);
        Self {
            data,
            total,
            page,
            total_pages,
            next_before,
            invalid_wallets: None,
            resolved_wallets: None,
        }
    }
}

/// Keyset position of a row.
trait Keyed {
    fn key(&self) -> (i64, i32, String);
}

fn page_json<R: Keyed, T>(
    rows: Vec<R>,
    spec: &PageSpec,
    total: i64,
    pages: (i64, i64),
    to_row: impl Fn(&R) -> T,
) -> Json<Page<T>> {
    let last = rows.last().map(Keyed::key);
    let data = rows.iter().map(to_row).collect();
    Json(Page::build(data, total, pages, spec.limit, last))
}

/// A `?wallets=` page: no keyset cursor (the merged listing pages by
/// number only) and the per-entry resolution.
fn wallets_json<T>(
    data: Vec<T>,
    set: &WalletSet,
    total: i64,
    (total_pages, page): (i64, i64),
) -> Json<Page<T>> {
    Json(Page {
        data,
        total,
        page,
        total_pages,
        next_before: None,
        invalid_wallets: Some(set.invalid.clone()),
        resolved_wallets: Some(resolved_wallets(set)),
    })
}

macro_rules! keyed {
    ($($t:ty),*) => {$(
        impl Keyed for $t {
            fn key(&self) -> (i64, i32, String) {
                (self.block_height, self.event_id, self.extrinsic_id.clone())
            }
        }
    )*};
}
keyed!(SwapRecord, TransferRecord, BridgeRecord, FeeBurnItem);

/// Cursor for the page after this one: position of the last row, only
/// when the page came back full (a short page IS the last page).
fn next_cursor(len: usize, limit: i64, last: Option<(i64, i32, String)>) -> Option<String> {
    if len < limit as usize {
        return None;
    }
    last.map(|(b, e, x)| format!("{b}-{e}-{x}"))
}

/// Planner row estimate for an `sm.*` table (`pg_class.reltuples`, the
/// Node's unfiltered `total`). `None` when the table was never analysed
/// (PG14 reports `-1`) so the caller falls back to an exact count.
pub(super) async fn estimated_rows(state: &AppState, table: &str) -> Result<Option<i64>, ApiError> {
    let row = sqlx::query!(
        r#"
        SELECT c.reltuples::bigint AS "estimate!"
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = 'sm' AND c.relname = $1
        "#,
        table,
    )
    .fetch_optional(&state.listing_db)
    .await?;
    Ok(row.map(|r| r.estimate).filter(|e| *e >= 0))
}

#[derive(Debug, Clone, Copy)]
pub(super) enum Table {
    Swaps,
    Transfers,
    Bridges,
    FeeEvents,
    Extrinsics,
}

impl Table {
    pub(super) fn name(self) -> &'static str {
        match self {
            Table::Swaps => "swaps",
            Table::Transfers => "transfers",
            Table::Bridges => "bridges",
            Table::FeeEvents => "fee_events",
            Table::Extrinsics => "extrinsics",
        }
    }
}

/// Keyset bound for `?timestamp=`: rows at or before the instant are the
/// rows below `(last block at or before it + 1, 0)`; `(0, 0)` if none.
pub(super) async fn until_bound(
    state: &AppState,
    table: Table,
    until: DateTime<Utc>,
) -> Result<(i64, i32), ApiError> {
    let block = match table {
        Table::Swaps => {
            sqlx::query_scalar!(
                "SELECT block_height FROM sm.swaps WHERE block_timestamp <= $1
                 ORDER BY block_timestamp DESC LIMIT 1",
                until
            )
            .fetch_optional(&state.listing_db)
            .await?
        }
        Table::Transfers => {
            sqlx::query_scalar!(
                "SELECT block_height FROM sm.transfers WHERE block_timestamp <= $1
                 ORDER BY block_timestamp DESC LIMIT 1",
                until
            )
            .fetch_optional(&state.listing_db)
            .await?
        }
        Table::Bridges => {
            sqlx::query_scalar!(
                "SELECT block_height FROM sm.bridges WHERE block_timestamp <= $1
                 ORDER BY block_timestamp DESC LIMIT 1",
                until
            )
            .fetch_optional(&state.listing_db)
            .await?
        }
        Table::FeeEvents => {
            sqlx::query_scalar!(
                "SELECT block_height FROM sm.fee_events WHERE block_timestamp <= $1
                 ORDER BY block_timestamp DESC LIMIT 1",
                until
            )
            .fetch_optional(&state.listing_db)
            .await?
        }
        Table::Extrinsics => {
            sqlx::query_scalar!(
                "SELECT block_height FROM sm.extrinsics WHERE block_timestamp <= $1
                 ORDER BY block_timestamp DESC LIMIT 1",
                until
            )
            .fetch_optional(&state.listing_db)
            .await?
        }
    };
    Ok(block.map_or((0, 0), |b| (b + 1, 0)))
}

/// Planner arithmetic for `block_height <= upto`: the table estimate times
/// the share of the `block_height` histogram at or below `upto`.
pub(super) async fn estimated_rows_upto(
    state: &AppState,
    table: Table,
    upto: i64,
) -> Result<Option<i64>, ApiError> {
    let row = sqlx::query!(
        r#"
        SELECT c.reltuples::bigint AS "estimate!",
               (SELECT s.histogram_bounds::text::bigint[] FROM pg_stats s
                WHERE s.schemaname = 'sm' AND s.tablename = $1
                  AND s.attname = 'block_height') AS bounds
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = 'sm' AND c.relname = $1
        "#,
        table.name(),
    )
    .fetch_optional(&state.listing_db)
    .await?;
    Ok(row.and_then(|r| {
        let share = histogram_share(r.bounds.as_deref()?, upto)?;
        (r.estimate >= 0).then(|| (r.estimate as f64 * share).round() as i64)
    }))
}

/// Fraction of an equi-depth histogram at or below `x`.
fn histogram_share(bounds: &[i64], x: i64) -> Option<f64> {
    if bounds.len() < 2 {
        return None;
    }
    let (first, last) = (bounds[0], bounds[bounds.len() - 1]);
    if x < first {
        return Some(0.0);
    }
    if x >= last {
        return Some(1.0);
    }
    let i = bounds.partition_point(|b| *b <= x) - 1;
    let (lo, hi) = (bounds[i], bounds[i + 1]);
    let within = (x - lo) as f64 / (hi - lo) as f64;
    Some((i as f64 + within) / (bounds.len() - 1) as f64)
}

/// Keyset cap for `?timestamp=`, if given.
async fn until_cap(
    state: &AppState,
    spec: &PageSpec,
    table: Table,
) -> Result<Option<(i64, i32)>, ApiError> {
    match spec.until {
        Some(until) => Ok(Some(until_bound(state, table, until).await?)),
        None => Ok(None),
    }
}

/// `total` and the keyset cap for a listing: the planner estimate when
/// `estimable` (no row filter besides `?timestamp=`), else `exact`.
async fn page_total<F, Fut>(
    state: &AppState,
    spec: &PageSpec,
    table: Table,
    estimable: bool,
    exact: F,
) -> Result<(i64, Option<(i64, i32)>), ApiError>
where
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = Result<i64, ApiError>>,
{
    let cap = until_cap(state, spec, table).await?;
    let estimate = match (estimable, cap) {
        (true, None) => estimated_rows(state, table.name()).await?,
        (true, Some((block, _))) => estimated_rows_upto(state, table, block - 1).await?,
        (false, _) => None,
    };
    let total = match estimate {
        Some(e) => e,
        None => exact().await?,
    };
    Ok((total, cap))
}

/// Asset columns indexed for `?symbol=`.
#[derive(Debug, Clone, Copy)]
enum AssetColumn {
    SwapInput,
    SwapOutput,
    Transfer,
}

impl AssetColumn {
    fn table(self) -> Table {
        match self {
            AssetColumn::SwapInput | AssetColumn::SwapOutput => Table::Swaps,
            AssetColumn::Transfer => Table::Transfers,
        }
    }

    fn column(self) -> &'static str {
        match self {
            AssetColumn::SwapInput => "input_asset_id",
            AssetColumn::SwapOutput => "output_asset_id",
            AssetColumn::Transfer => "asset_id",
        }
    }
}

/// Exact counts stop here; above it the planner arithmetic takes over.
const EXACT_COUNT_CAP: i64 = 200_000;

/// Rows holding `asset` in `col` below `cap`. Without a cap a frequent
/// asset uses its most-common-value frequency times the table estimate.
/// Otherwise an index-only count up to `EXACT_COUNT_CAP`, and past it
/// that frequency times the rows below the cap (never less than counted).
async fn asset_rows(
    state: &AppState,
    col: AssetColumn,
    asset: &str,
    cap: Option<(i64, i32)>,
) -> Result<i64, ApiError> {
    let freq = sqlx::query_scalar!(
        r#"
        SELECT m.freq AS "freq!"
        FROM pg_stats s,
             unnest(s.most_common_vals::text::text[], s.most_common_freqs) AS m(val, freq)
        WHERE s.schemaname = 'sm' AND s.tablename = $1 AND s.attname = $2 AND m.val = $3
        "#,
        col.table().name(),
        col.column(),
        asset,
    )
    .fetch_optional(&state.listing_db)
    .await?;
    let estimate = |rows: Option<i64>| {
        freq.zip(rows)
            .map(|(f, r)| (r as f64 * f64::from(f)).round() as i64)
    };
    if cap.is_none() {
        if let Some(e) = estimate(estimated_rows(state, col.table().name()).await?) {
            return Ok(e);
        }
    }
    let (block, event) = cap.unwrap_or((i64::MAX, i32::MAX));
    let counted = match col {
        AssetColumn::SwapInput => {
            sqlx::query_scalar!(
                r#"SELECT COUNT(*) AS "count!" FROM (
                       SELECT 1 FROM sm.swaps
                       WHERE input_asset_id = $1 AND (block_height, event_id) < ($2, $3)
                       LIMIT $4) capped"#,
                asset,
                block,
                event,
                EXACT_COUNT_CAP,
            )
            .fetch_one(&state.listing_db)
            .await?
        }
        AssetColumn::SwapOutput => {
            sqlx::query_scalar!(
                r#"SELECT COUNT(*) AS "count!" FROM (
                       SELECT 1 FROM sm.swaps
                       WHERE output_asset_id = $1 AND (block_height, event_id) < ($2, $3)
                       LIMIT $4) capped"#,
                asset,
                block,
                event,
                EXACT_COUNT_CAP,
            )
            .fetch_one(&state.listing_db)
            .await?
        }
        AssetColumn::Transfer => {
            sqlx::query_scalar!(
                r#"SELECT COUNT(*) AS "count!" FROM (
                       SELECT 1 FROM sm.transfers
                       WHERE asset_id = $1 AND (block_height, event_id) < ($2, $3)
                       LIMIT $4) capped"#,
                asset,
                block,
                event,
                EXACT_COUNT_CAP,
            )
            .fetch_one(&state.listing_db)
            .await?
        }
    };
    if counted < EXACT_COUNT_CAP {
        return Ok(counted);
    }
    let below = match cap {
        Some((block, _)) => estimated_rows_upto(state, col.table(), block - 1).await?,
        None => estimated_rows(state, col.table().name()).await?,
    };
    Ok(estimate(below).map_or(counted, |e| e.max(counted)))
}

fn empty_page<T>(spec: &PageSpec) -> Json<Page<T>> {
    Json(Page::build(Vec::new(), 0, (0, 1), spec.limit, None))
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

/// Both legs carry the swap's USD value (`swap_usd`), not their own quote.
fn swap_row(r: &SwapRecord, registry: &Registry, zone: chrono_tz::Tz) -> SwapRow {
    let usd = fmt_usd(swap_usd(r.usd_value.as_ref(), r.output_usd_value.as_ref()).as_ref());
    let leg = |asset: &str, amount: &BigDecimal| SwapLeg {
        symbol: symbol_for(registry, asset),
        amount: fmt_amount(amount, decimals_for(registry, asset)),
        logo: logo_for(registry, asset),
        usd,
    };
    SwapRow {
        time: fmt_time(r.block_timestamp, zone),
        block: r.block_height,
        hash: r.hash.clone().unwrap_or_default(),
        extrinsic_id: fmt_extrinsic_id(r.block_height, &r.extrinsic_id),
        wallet: r.caller.clone(),
        input: leg(&r.input_asset_id, &r.input_amount),
        out: leg(&r.output_asset_id, &r.output_amount),
    }
}

async fn swaps_page(
    state: &AppState,
    spec: &PageSpec,
    wallet: Option<&str>,
) -> Result<Json<Page<SwapRow>>, ApiError> {
    let registry = state.registry.read().await;
    let asset = spec
        .symbol
        .as_deref()
        .map(|sym| registry.asset_id_for_symbol(sym).map(str::to_string));
    if let (None, Some(asset)) = (wallet, &asset) {
        let Some(asset) = asset else {
            return Ok(empty_page(spec));
        };
        return swaps_by_asset(state, spec, &registry, asset).await;
    }
    // Symbol filter resolved to asset ids up front; a needle that
    // matches no symbol must match no row (empty array, not NULL).
    let asset_ids: Option<Vec<String>> = match asset {
        Some(id) => Some(id.into_iter().collect()),
        None => spec
            .needle
            .as_deref()
            .map(|n| registry.asset_ids_matching(n)),
    };

    let estimable = wallet.is_none() && asset_ids.is_none();
    let (total, cap) = page_total(state, spec, Table::Swaps, estimable, || {
        exact_swaps_count(state, spec, wallet, asset_ids.as_deref())
    })
    .await?;

    let (total_pages, page, seek) = spec.resolve(total, wallet.is_some() || asset_ids.is_none());
    let stream = asset_ids.is_none().then_some(Stream::Swaps { wallet });
    let (offset, before) = match seek {
        Seek::Head { offset } => match deep_start(state, stream, cap, offset, true).await? {
            DeepStart::Plain => (offset, spec.head_bound(cap)),
            DeepStart::At(d) => (d.skip, (d.block + 1, 0, "")),
            DeepStart::PastEnd => (offset, (0, 0, "")),
        },
        Seek::Tail { offset, take } => {
            let (lo, offset) = match deep_start(state, stream, cap, offset, false).await? {
                DeepStart::At(d) => (d.block, d.skip),
                DeepStart::Plain | DeepStart::PastEnd => (0, offset),
            };
            let rows =
                swaps_tail(state, spec, wallet, asset_ids.as_deref(), lo, offset, take).await?;
            return Ok(page_json(rows, spec, total, (total_pages, page), |r| {
                swap_row(r, &registry, state.time_zone)
            }));
        }
    };

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
        WHERE (block_height, event_id, extrinsic_id) < ($1, $2, $8)
          AND ($5::text IS NULL OR caller = $5)
          AND ($6::text[] IS NULL OR input_asset_id = ANY($6) OR output_asset_id = ANY($6))
          AND ($7::timestamptz IS NULL OR block_timestamp <= $7)
        ORDER BY block_height DESC, event_id DESC, extrinsic_id DESC
        LIMIT $3 OFFSET $4
        "#,
        before.0,
        before.1,
        spec.limit,
        offset,
        wallet,
        asset_ids.as_deref(),
        spec.until,
        before.2,
    )
    .fetch_all(&state.listing_db)
    .await?;

    Ok(page_json(rows, spec, total, (total_pages, page), |r| {
        swap_row(r, &registry, state.time_zone)
    }))
}

/// A back-half page, read oldest first and returned newest first.
async fn swaps_tail(
    state: &AppState,
    spec: &PageSpec,
    wallet: Option<&str>,
    asset_ids: Option<&[String]>,
    lo: i64,
    offset: i64,
    take: i64,
) -> Result<Vec<SwapRecord>, ApiError> {
    let mut rows = sqlx::query_as!(
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
        WHERE ($3::text IS NULL OR caller = $3)
          AND ($4::text[] IS NULL OR input_asset_id = ANY($4) OR output_asset_id = ANY($4))
          AND ($5::timestamptz IS NULL OR block_timestamp <= $5)
          AND block_height >= $6
        ORDER BY block_height ASC, event_id ASC, extrinsic_id ASC
        LIMIT $1 OFFSET $2
        "#,
        take,
        offset,
        wallet,
        asset_ids,
        spec.until,
        lo,
    )
    .fetch_all(&state.listing_db)
    .await?;
    rows.reverse();
    Ok(rows)
}

/// Global swaps of one asset (`?symbol=`): each leg walks its own
/// `(asset, block_height, event_id)` index and the two runs are merged.
async fn swaps_by_asset(
    state: &AppState,
    spec: &PageSpec,
    registry: &Registry,
    asset: &str,
) -> Result<Json<Page<SwapRow>>, ApiError> {
    let cap = until_cap(state, spec, Table::Swaps).await?;
    let total = asset_rows(state, AssetColumn::SwapInput, asset, cap).await?
        + asset_rows(state, AssetColumn::SwapOutput, asset, cap).await?;
    let (total_pages, page, seek) = spec.resolve(total, true);
    let (block, event, ext) = spec.head_bound(cap);
    let rows = match seek {
        Seek::Head { offset } => {
            sqlx::query_as!(
                SwapRecord,
                r#"
                SELECT
                    block_height     AS "block_height!",
                    extrinsic_id     AS "extrinsic_id!",
                    event_id         AS "event_id!",
                    hash,
                    block_timestamp  AS "block_timestamp!",
                    caller           AS "caller!",
                    input_asset_id   AS "input_asset_id!",
                    input_amount     AS "input_amount!: BigDecimal",
                    output_asset_id  AS "output_asset_id!",
                    output_amount    AS "output_amount!: BigDecimal",
                    usd_value        AS "usd_value: BigDecimal",
                    output_usd_value AS "output_usd_value: BigDecimal"
                FROM (
                    (SELECT block_height, extrinsic_id, event_id, hash, block_timestamp, caller,
                            input_asset_id, input_amount, output_asset_id, output_amount,
                            usd_value, output_usd_value
                     FROM sm.swaps
                     WHERE input_asset_id = $1 AND (block_height, event_id, extrinsic_id) < ($2, $3, $7)
                     ORDER BY block_height DESC, event_id DESC, extrinsic_id DESC
                     LIMIT $4)
                    UNION
                    (SELECT block_height, extrinsic_id, event_id, hash, block_timestamp, caller,
                            input_asset_id, input_amount, output_asset_id, output_amount,
                            usd_value, output_usd_value
                     FROM sm.swaps
                     WHERE output_asset_id = $1 AND (block_height, event_id, extrinsic_id) < ($2, $3, $7)
                     ORDER BY block_height DESC, event_id DESC, extrinsic_id DESC
                     LIMIT $4)
                ) legs
                ORDER BY block_height DESC, event_id DESC, extrinsic_id DESC
                LIMIT $5 OFFSET $6
                "#,
                asset,
                block,
                event,
                offset + spec.limit,
                spec.limit,
                offset,
                ext,
            )
            .fetch_all(&state.listing_db)
            .await?
        }
        Seek::Tail { offset, take } => {
            let mut rows = sqlx::query_as!(
                SwapRecord,
                r#"
                SELECT
                    block_height     AS "block_height!",
                    extrinsic_id     AS "extrinsic_id!",
                    event_id         AS "event_id!",
                    hash,
                    block_timestamp  AS "block_timestamp!",
                    caller           AS "caller!",
                    input_asset_id   AS "input_asset_id!",
                    input_amount     AS "input_amount!: BigDecimal",
                    output_asset_id  AS "output_asset_id!",
                    output_amount    AS "output_amount!: BigDecimal",
                    usd_value        AS "usd_value: BigDecimal",
                    output_usd_value AS "output_usd_value: BigDecimal"
                FROM (
                    (SELECT block_height, extrinsic_id, event_id, hash, block_timestamp, caller,
                            input_asset_id, input_amount, output_asset_id, output_amount,
                            usd_value, output_usd_value
                     FROM sm.swaps
                     WHERE input_asset_id = $1 AND (block_height, event_id) < ($2, $3)
                     ORDER BY block_height ASC, event_id ASC, extrinsic_id ASC
                     LIMIT $4)
                    UNION
                    (SELECT block_height, extrinsic_id, event_id, hash, block_timestamp, caller,
                            input_asset_id, input_amount, output_asset_id, output_amount,
                            usd_value, output_usd_value
                     FROM sm.swaps
                     WHERE output_asset_id = $1 AND (block_height, event_id) < ($2, $3)
                     ORDER BY block_height ASC, event_id ASC, extrinsic_id ASC
                     LIMIT $4)
                ) legs
                ORDER BY block_height ASC, event_id ASC, extrinsic_id ASC
                LIMIT $5 OFFSET $6
                "#,
                asset,
                block,
                event,
                offset + take,
                take,
                offset,
            )
            .fetch_all(&state.listing_db)
            .await?;
            rows.reverse();
            rows
        }
    };
    Ok(page_json(rows, spec, total, (total_pages, page), |r| {
        swap_row(r, registry, state.time_zone)
    }))
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
    .fetch_one(&state.listing_db)
    .await?;
    Ok(row.count)
}

async fn swaps(
    State(state): State<AppState>,
    Query(p): Query<Pagination>,
) -> Result<Json<Page<SwapRow>>, ApiError> {
    let spec = p.validate(25)?;
    if let Some(wallets) = spec.wallets_alone()? {
        return swaps_of_wallets(&state, &spec, wallets).await;
    }
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

/// Swaps of several wallets merged newest first (`?wallets=`): each
/// wallet walks its caller index from the page's start block and
/// contributes at most the rows the page can need.
async fn swaps_of_wallets(
    state: &AppState,
    spec: &PageSpec,
    set: &WalletSet,
) -> Result<Json<Page<SwapRow>>, ApiError> {
    let wallets = set.addresses();
    let total = sqlx::query_scalar!(
        r#"SELECT COUNT(*) AS "count!" FROM sm.swaps WHERE caller = ANY($1)"#,
        &wallets
    )
    .fetch_one(&state.listing_db)
    .await?;
    let (total_pages, page, seek) = spec.resolve(total, true);
    let stream = Some(Stream::SwapsOf { wallets: &wallets });
    let rows = match seek {
        Seek::Head { offset } => {
            let (skip, top) = match deep_start(state, stream, None, offset, true).await? {
                DeepStart::Plain => (offset, i64::MAX),
                DeepStart::At(d) => (d.skip, d.block),
                DeepStart::PastEnd => (offset, -1),
            };
            sqlx::query_as!(
                SwapRecord,
                r#"
                SELECT
                    x.block_height     AS "block_height!",
                    x.extrinsic_id     AS "extrinsic_id!",
                    x.event_id         AS "event_id!",
                    x.hash,
                    x.block_timestamp  AS "block_timestamp!",
                    x.caller           AS "caller!",
                    x.input_asset_id   AS "input_asset_id!",
                    x.input_amount     AS "input_amount!: BigDecimal",
                    x.output_asset_id  AS "output_asset_id!",
                    x.output_amount    AS "output_amount!: BigDecimal",
                    x.usd_value        AS "usd_value: BigDecimal",
                    x.output_usd_value AS "output_usd_value: BigDecimal"
                FROM unnest($1::text[]) AS w(addr)
                CROSS JOIN LATERAL (
                    SELECT block_height, extrinsic_id, event_id, hash, block_timestamp, caller,
                           input_asset_id, input_amount, output_asset_id, output_amount,
                           usd_value, output_usd_value
                    FROM sm.swaps WHERE caller = w.addr AND block_height <= $5
                    ORDER BY block_height DESC, event_id DESC, extrinsic_id DESC
                    LIMIT $2
                ) x
                ORDER BY x.block_height DESC, x.event_id DESC, x.extrinsic_id DESC
                LIMIT $3 OFFSET $4
                "#,
                &wallets,
                skip + spec.limit,
                spec.limit,
                skip,
                top,
            )
            .fetch_all(&state.listing_db)
            .await?
        }
        Seek::Tail { offset, take } => {
            let (lo, skip) = match deep_start(state, stream, None, offset, false).await? {
                DeepStart::At(d) => (d.block, d.skip),
                DeepStart::Plain | DeepStart::PastEnd => (0, offset),
            };
            let mut rows = sqlx::query_as!(
                SwapRecord,
                r#"
                SELECT
                    x.block_height     AS "block_height!",
                    x.extrinsic_id     AS "extrinsic_id!",
                    x.event_id         AS "event_id!",
                    x.hash,
                    x.block_timestamp  AS "block_timestamp!",
                    x.caller           AS "caller!",
                    x.input_asset_id   AS "input_asset_id!",
                    x.input_amount     AS "input_amount!: BigDecimal",
                    x.output_asset_id  AS "output_asset_id!",
                    x.output_amount    AS "output_amount!: BigDecimal",
                    x.usd_value        AS "usd_value: BigDecimal",
                    x.output_usd_value AS "output_usd_value: BigDecimal"
                FROM unnest($1::text[]) AS w(addr)
                CROSS JOIN LATERAL (
                    SELECT block_height, extrinsic_id, event_id, hash, block_timestamp, caller,
                           input_asset_id, input_amount, output_asset_id, output_amount,
                           usd_value, output_usd_value
                    FROM sm.swaps WHERE caller = w.addr AND block_height >= $5
                    ORDER BY block_height ASC, event_id ASC, extrinsic_id ASC
                    LIMIT $2
                ) x
                ORDER BY x.block_height ASC, x.event_id ASC, x.extrinsic_id ASC
                LIMIT $3 OFFSET $4
                "#,
                &wallets,
                skip + take,
                take,
                skip,
                lo,
            )
            .fetch_all(&state.listing_db)
            .await?;
            rows.reverse();
            rows
        }
    };
    let registry = state.registry.read().await;
    let data = rows
        .iter()
        .map(|r| swap_row(r, &registry, state.time_zone))
        .collect();
    Ok(wallets_json(data, set, total, (total_pages, page)))
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
    let asset = spec
        .symbol
        .as_deref()
        .map(|sym| registry.asset_id_for_symbol(sym).map(str::to_string));
    if let (None, Some(asset)) = (wallet, &asset) {
        let Some(asset) = asset else {
            return Ok(empty_page(spec));
        };
        return transfers_by_asset(state, spec, &registry, asset).await;
    }
    let asset_ids: Option<Vec<String>> = match asset {
        Some(id) => Some(id.into_iter().collect()),
        None => spec
            .needle
            .as_deref()
            .map(|n| registry.asset_ids_matching(n)),
    };
    let pattern = spec.like_pattern();
    if let Some(w) = wallet.filter(|_| {
        asset_ids.is_none() && pattern.is_none() && spec.until.is_none() && !spec.keyset
    }) {
        let (rows, total, pages) = transfers_of(state, spec, &[w.to_string()]).await?;
        return Ok(page_json(rows, spec, total, pages, |r| {
            transfer_row(r, &registry, state.time_zone)
        }));
    }

    let estimable = wallet.is_none() && asset_ids.is_none();
    let (total, cap) = page_total(state, spec, Table::Transfers, estimable, || {
        exact_transfers_count(state, spec, wallet, asset_ids.as_deref())
    })
    .await?;

    let (total_pages, page, seek) = spec.resolve(
        total,
        wallet.is_some() || (asset_ids.is_none() && pattern.is_none()),
    );
    let stream =
        (wallet.is_none() && asset_ids.is_none() && pattern.is_none()).then_some(Stream::Transfers);
    let (offset, before) = match seek {
        Seek::Head { offset } => match deep_start(state, stream, cap, offset, true).await? {
            DeepStart::Plain => (offset, spec.head_bound(cap)),
            DeepStart::At(d) => (d.skip, (d.block + 1, 0, "")),
            DeepStart::PastEnd => (offset, (0, 0, "")),
        },
        Seek::Tail { offset, take } => {
            let (lo, offset) = match deep_start(state, stream, cap, offset, false).await? {
                DeepStart::At(d) => (d.block, d.skip),
                DeepStart::Plain | DeepStart::PastEnd => (0, offset),
            };
            let rows = transfers_tail(
                state,
                spec,
                wallet,
                asset_ids.as_deref(),
                pattern.as_deref(),
                (lo, offset),
                take,
            )
            .await?;
            return Ok(page_json(rows, spec, total, (total_pages, page), |r| {
                transfer_row(r, &registry, state.time_zone)
            }));
        }
    };

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
        WHERE (block_height, event_id, extrinsic_id) < ($1, $2, $9)
          AND ($5::text IS NULL OR from_address = $5 OR to_address = $5)
          AND ($6::text[] IS NULL OR asset_id = ANY($6)
               OR from_address ILIKE $7 OR to_address ILIKE $7)
          AND ($8::timestamptz IS NULL OR block_timestamp <= $8)
        ORDER BY block_height DESC, event_id DESC, extrinsic_id DESC
        LIMIT $3 OFFSET $4
        "#,
        before.0,
        before.1,
        spec.limit,
        offset,
        wallet,
        asset_ids.as_deref(),
        pattern,
        spec.until,
        before.2,
    )
    .fetch_all(&state.listing_db)
    .await?;

    Ok(page_json(rows, spec, total, (total_pages, page), |r| {
        transfer_row(r, &registry, state.time_zone)
    }))
}

/// A back-half page, read oldest first and returned newest first.
async fn transfers_tail(
    state: &AppState,
    spec: &PageSpec,
    wallet: Option<&str>,
    asset_ids: Option<&[String]>,
    pattern: Option<&str>,
    (lo, offset): (i64, i64),
    take: i64,
) -> Result<Vec<TransferRecord>, ApiError> {
    let mut rows = sqlx::query_as!(
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
        WHERE ($3::text IS NULL OR from_address = $3 OR to_address = $3)
          AND ($4::text[] IS NULL OR asset_id = ANY($4)
               OR from_address ILIKE $5 OR to_address ILIKE $5)
          AND ($6::timestamptz IS NULL OR block_timestamp <= $6)
          AND block_height >= $7
        ORDER BY block_height ASC, event_id ASC, extrinsic_id ASC
        LIMIT $1 OFFSET $2
        "#,
        take,
        offset,
        wallet,
        asset_ids,
        pattern,
        spec.until,
        lo,
    )
    .fetch_all(&state.listing_db)
    .await?;
    rows.reverse();
    Ok(rows)
}

/// Global transfers of one asset (`?symbol=`), on its
/// `(asset_id, block_height, event_id)` index.
async fn transfers_by_asset(
    state: &AppState,
    spec: &PageSpec,
    registry: &Registry,
    asset: &str,
) -> Result<Json<Page<TransferRow>>, ApiError> {
    let cap = until_cap(state, spec, Table::Transfers).await?;
    let total = asset_rows(state, AssetColumn::Transfer, asset, cap).await?;
    let (total_pages, page, seek) = spec.resolve(total, true);
    let (block, event, ext) = spec.head_bound(cap);
    let rows = match seek {
        Seek::Head { offset } => {
            sqlx::query_as!(
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
                WHERE asset_id = $1 AND (block_height, event_id, extrinsic_id) < ($2, $3, $6)
                ORDER BY block_height DESC, event_id DESC, extrinsic_id DESC
                LIMIT $4 OFFSET $5
                "#,
                asset,
                block,
                event,
                spec.limit,
                offset,
                ext,
            )
            .fetch_all(&state.listing_db)
            .await?
        }
        Seek::Tail { offset, take } => {
            let mut rows = sqlx::query_as!(
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
                WHERE asset_id = $1 AND (block_height, event_id) < ($2, $3)
                ORDER BY block_height ASC, event_id ASC, extrinsic_id ASC
                LIMIT $4 OFFSET $5
                "#,
                asset,
                block,
                event,
                take,
                offset,
            )
            .fetch_all(&state.listing_db)
            .await?;
            rows.reverse();
            rows
        }
    };
    Ok(page_json(rows, spec, total, (total_pages, page), |r| {
        transfer_row(r, registry, state.time_zone)
    }))
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
    .fetch_one(&state.listing_db)
    .await?;
    Ok(row.count)
}

async fn transfers(
    State(state): State<AppState>,
    Query(p): Query<Pagination>,
) -> Result<Json<Page<TransferRow>>, ApiError> {
    let spec = p.validate(25)?;
    if let Some(wallets) = spec.wallets_alone()? {
        return transfers_of_wallets(&state, &spec, wallets).await;
    }
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

/// Transfers of several wallets merged newest first (`?wallets=`).
async fn transfers_of_wallets(
    state: &AppState,
    spec: &PageSpec,
    set: &WalletSet,
) -> Result<Json<Page<TransferRow>>, ApiError> {
    let (rows, total, pages) = transfers_of(state, spec, &set.addresses()).await?;
    let registry = state.registry.read().await;
    let data = rows
        .iter()
        .map(|r| transfer_row(r, &registry, state.time_zone))
        .collect();
    Ok(wallets_json(data, set, total, pages))
}

/// Transfers touching any of `wallets`: sent by one of them, or received
/// by one of them from outside the set (disjoint, so a transfer between
/// two of them appears once). Each wallet walks its from/to indexes from
/// the page's start block.
async fn transfers_of(
    state: &AppState,
    spec: &PageSpec,
    wallets: &[String],
) -> Result<(Vec<TransferRecord>, i64, (i64, i64)), ApiError> {
    let total = sqlx::query_scalar!(
        r#"SELECT (SELECT COUNT(*) FROM sm.transfers WHERE from_address = ANY($1))
                + (SELECT COUNT(*) FROM sm.transfers
                   WHERE to_address = ANY($1) AND NOT (from_address = ANY($1))) AS "count!""#,
        wallets
    )
    .fetch_one(&state.listing_db)
    .await?;
    let (total_pages, page, seek) = spec.resolve(total, true);
    let stream = Some(Stream::TransfersOf { wallets });
    let rows = match seek {
        Seek::Head { offset } => {
            let (skip, top) = match deep_start(state, stream, None, offset, true).await? {
                DeepStart::Plain => (offset, i64::MAX),
                DeepStart::At(d) => (d.skip, d.block),
                DeepStart::PastEnd => (offset, -1),
            };
            sqlx::query_as!(
                TransferRecord,
                r#"
                SELECT
                    x.block_height    AS "block_height!",
                    x.extrinsic_id    AS "extrinsic_id!",
                    x.event_id        AS "event_id!",
                    x.hash,
                    x.block_timestamp AS "block_timestamp!",
                    x.from_address    AS "from_address!",
                    x.to_address      AS "to_address!",
                    x.asset_id        AS "asset_id!",
                    x.amount          AS "amount!: BigDecimal",
                    x.usd_value       AS "usd_value: BigDecimal"
                FROM unnest($1::text[]) AS w(addr)
                CROSS JOIN LATERAL (
                    (SELECT block_height, extrinsic_id, event_id, hash, block_timestamp,
                            from_address, to_address, asset_id, amount, usd_value
                     FROM sm.transfers
                     WHERE from_address = w.addr AND block_height <= $5
                     ORDER BY block_height DESC, event_id DESC, extrinsic_id DESC
                     LIMIT $2)
                    UNION ALL
                    (SELECT block_height, extrinsic_id, event_id, hash, block_timestamp,
                            from_address, to_address, asset_id, amount, usd_value
                     FROM sm.transfers
                     WHERE to_address = w.addr AND NOT (from_address = ANY($1))
                       AND block_height <= $5
                     ORDER BY block_height DESC, event_id DESC, extrinsic_id DESC
                     LIMIT $2)
                ) x
                ORDER BY x.block_height DESC, x.event_id DESC, x.extrinsic_id DESC
                LIMIT $3 OFFSET $4
                "#,
                wallets,
                skip + spec.limit,
                spec.limit,
                skip,
                top,
            )
            .fetch_all(&state.listing_db)
            .await?
        }
        Seek::Tail { offset, take } => {
            let (lo, skip) = match deep_start(state, stream, None, offset, false).await? {
                DeepStart::At(d) => (d.block, d.skip),
                DeepStart::Plain | DeepStart::PastEnd => (0, offset),
            };
            let mut rows = sqlx::query_as!(
                TransferRecord,
                r#"
                SELECT
                    x.block_height    AS "block_height!",
                    x.extrinsic_id    AS "extrinsic_id!",
                    x.event_id        AS "event_id!",
                    x.hash,
                    x.block_timestamp AS "block_timestamp!",
                    x.from_address    AS "from_address!",
                    x.to_address      AS "to_address!",
                    x.asset_id        AS "asset_id!",
                    x.amount          AS "amount!: BigDecimal",
                    x.usd_value       AS "usd_value: BigDecimal"
                FROM unnest($1::text[]) AS w(addr)
                CROSS JOIN LATERAL (
                    (SELECT block_height, extrinsic_id, event_id, hash, block_timestamp,
                            from_address, to_address, asset_id, amount, usd_value
                     FROM sm.transfers
                     WHERE from_address = w.addr AND block_height >= $5
                     ORDER BY block_height ASC, event_id ASC, extrinsic_id ASC
                     LIMIT $2)
                    UNION ALL
                    (SELECT block_height, extrinsic_id, event_id, hash, block_timestamp,
                            from_address, to_address, asset_id, amount, usd_value
                     FROM sm.transfers
                     WHERE to_address = w.addr AND NOT (from_address = ANY($1))
                       AND block_height >= $5
                     ORDER BY block_height ASC, event_id ASC, extrinsic_id ASC
                     LIMIT $2)
                ) x
                ORDER BY x.block_height ASC, x.event_id ASC, x.extrinsic_id ASC
                LIMIT $3 OFFSET $4
                "#,
                wallets,
                skip + take,
                take,
                skip,
                lo,
            )
            .fetch_all(&state.listing_db)
            .await?;
            rows.reverse();
            rows
        }
    };
    Ok((rows, total, (total_pages, page)))
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

    let estimable = wallet.is_none() && spec.needle.is_none() && spec.network.is_none();
    let (total, cap) = page_total(state, spec, Table::Bridges, estimable, || {
        exact_bridges_count(state, spec, wallet)
    })
    .await?;

    let (total_pages, page, seek) = spec.resolve(total, wallet.is_some() || pattern.is_none());
    let offset = match seek {
        Seek::Head { offset } => offset,
        Seek::Tail { offset, take } => {
            let rows = bridges_tail(state, spec, wallet, pattern.as_deref(), offset, take).await?;
            return Ok(page_json(rows, spec, total, (total_pages, page), |r| {
                bridge_row(r, &registry, state.time_zone)
            }));
        }
    };
    let before = spec.head_bound(cap);

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
        WHERE (block_height, event_id, extrinsic_id) < ($1, $2, $9)
          AND ($5::text IS NULL OR caller = $5 OR counterparty = $5)
          AND ($6::text IS NULL OR caller ILIKE $6 OR counterparty ILIKE $6
               OR network ILIKE $6 OR asset_id ILIKE $6)
          AND ($7::timestamptz IS NULL OR block_timestamp <= $7)
          AND ($8::text IS NULL OR network = $8)
        ORDER BY block_height DESC, event_id DESC, extrinsic_id DESC
        LIMIT $3 OFFSET $4
        "#,
        before.0,
        before.1,
        spec.limit,
        offset,
        wallet,
        pattern,
        spec.until,
        spec.network,
        before.2,
    )
    .fetch_all(&state.listing_db)
    .await?;

    Ok(page_json(rows, spec, total, (total_pages, page), |r| {
        bridge_row(r, &registry, state.time_zone)
    }))
}

/// A back-half page, read oldest first and returned newest first.
async fn bridges_tail(
    state: &AppState,
    spec: &PageSpec,
    wallet: Option<&str>,
    pattern: Option<&str>,
    offset: i64,
    take: i64,
) -> Result<Vec<BridgeRecord>, ApiError> {
    let mut rows = sqlx::query_as!(
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
        WHERE ($3::text IS NULL OR caller = $3 OR counterparty = $3)
          AND ($4::text IS NULL OR caller ILIKE $4 OR counterparty ILIKE $4
               OR network ILIKE $4 OR asset_id ILIKE $4)
          AND ($5::timestamptz IS NULL OR block_timestamp <= $5)
          AND ($6::text IS NULL OR network = $6)
        ORDER BY block_height ASC, event_id ASC, extrinsic_id ASC
        LIMIT $1 OFFSET $2
        "#,
        take,
        offset,
        wallet,
        pattern,
        spec.until,
        spec.network,
    )
    .fetch_all(&state.listing_db)
    .await?;
    rows.reverse();
    Ok(rows)
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
          AND ($4::text IS NULL OR network = $4)
        "#,
        wallet,
        pattern,
        spec.until,
        spec.network,
    )
    .fetch_one(&state.listing_db)
    .await?;
    Ok(row.count)
}

async fn bridges(
    State(state): State<AppState>,
    Query(p): Query<Pagination>,
) -> Result<Json<Page<BridgeRow>>, ApiError> {
    let spec = p.validate(20)?;
    if let Some(wallets) = spec.wallets_alone()? {
        return bridges_of_wallets(&state, &spec, wallets).await;
    }
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

/// Bridge operations of several wallets merged newest first
/// (`?wallets=`); one between two of them appears once.
async fn bridges_of_wallets(
    state: &AppState,
    spec: &PageSpec,
    set: &WalletSet,
) -> Result<Json<Page<BridgeRow>>, ApiError> {
    let wallets = set.addresses();
    let total = sqlx::query_scalar!(
        r#"SELECT COUNT(*) AS "count!" FROM sm.bridges
           WHERE caller = ANY($1) OR counterparty = ANY($1)"#,
        &wallets
    )
    .fetch_one(&state.listing_db)
    .await?;
    let (total_pages, page, seek) = spec.resolve(total, true);
    let rows = match seek {
        Seek::Head { offset } => {
            sqlx::query_as!(
                BridgeRecord,
                r#"
                SELECT DISTINCT ON (x.block_height, x.event_id, x.extrinsic_id)
                    x.block_height    AS "block_height!",
                    x.extrinsic_id    AS "extrinsic_id!",
                    x.event_id        AS "event_id!",
                    x.hash,
                    x.block_timestamp AS "block_timestamp!",
                    x.direction::text AS "direction!",
                    x.network         AS "network!",
                    x.caller          AS "caller!",
                    x.counterparty,
                    x.asset_id        AS "asset_id!",
                    x.amount          AS "amount!: BigDecimal",
                    x.usd_value       AS "usd_value: BigDecimal"
                FROM unnest($1::text[]) AS w(addr)
                CROSS JOIN LATERAL (
                    SELECT block_height, extrinsic_id, event_id, hash, block_timestamp, direction,
                           network, caller, counterparty, asset_id, amount, usd_value
                    FROM sm.bridges WHERE caller = w.addr OR counterparty = w.addr
                    ORDER BY block_height DESC, event_id DESC, extrinsic_id DESC
                    LIMIT $2
                ) x
                ORDER BY x.block_height DESC, x.event_id DESC, x.extrinsic_id DESC
                LIMIT $3 OFFSET $4
                "#,
                &wallets,
                offset + spec.limit,
                spec.limit,
                offset,
            )
            .fetch_all(&state.listing_db)
            .await?
        }
        Seek::Tail { offset, take } => {
            let mut rows = sqlx::query_as!(
                BridgeRecord,
                r#"
                SELECT DISTINCT ON (x.block_height, x.event_id, x.extrinsic_id)
                    x.block_height    AS "block_height!",
                    x.extrinsic_id    AS "extrinsic_id!",
                    x.event_id        AS "event_id!",
                    x.hash,
                    x.block_timestamp AS "block_timestamp!",
                    x.direction::text AS "direction!",
                    x.network         AS "network!",
                    x.caller          AS "caller!",
                    x.counterparty,
                    x.asset_id        AS "asset_id!",
                    x.amount          AS "amount!: BigDecimal",
                    x.usd_value       AS "usd_value: BigDecimal"
                FROM unnest($1::text[]) AS w(addr)
                CROSS JOIN LATERAL (
                    SELECT block_height, extrinsic_id, event_id, hash, block_timestamp, direction,
                           network, caller, counterparty, asset_id, amount, usd_value
                    FROM sm.bridges WHERE caller = w.addr OR counterparty = w.addr
                    ORDER BY block_height ASC, event_id ASC, extrinsic_id ASC
                    LIMIT $2
                ) x
                ORDER BY x.block_height ASC, x.event_id ASC, x.extrinsic_id ASC
                LIMIT $3 OFFSET $4
                "#,
                &wallets,
                offset + take,
                take,
                offset,
            )
            .fetch_all(&state.listing_db)
            .await?;
            rows.reverse();
            rows
        }
    };
    let registry = state.registry.read().await;
    let data = rows
        .iter()
        .map(|r| bridge_row(r, &registry, state.time_zone))
        .collect();
    Ok(wallets_json(data, set, total, (total_pages, page)))
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
    let estimable = payer.is_none() && spec.needle.is_none();
    let (total, cap) = page_total(state, spec, Table::FeeEvents, estimable, || async {
        Ok(sqlx::query!(
            r#"
            SELECT COUNT(*) AS "count!"
            FROM sm.fee_events
            WHERE ($1::text IS NULL OR payer = $1)
              AND ($2::timestamptz IS NULL OR block_timestamp <= $2)
            "#,
            payer,
            spec.until,
        )
        .fetch_one(&state.listing_db)
        .await?
        .count)
    })
    .await?;

    let (total_pages, page, seek) = spec.resolve(total, true);
    let offset = match seek {
        Seek::Head { offset } => offset,
        Seek::Tail { offset, take } => {
            let items = fee_burns_tail(state, spec, payer, offset, take).await?;
            let last = items.last().map(Keyed::key);
            return Ok(Json(Page::build(
                items,
                total,
                (total_pages, page),
                spec.limit,
                last,
            )));
        }
    };
    let before = spec.head_bound(cap);

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
        WHERE (block_height, event_id, extrinsic_id) < ($1, $2, $7)
          AND ($5::text IS NULL OR payer = $5)
          AND ($6::timestamptz IS NULL OR block_timestamp <= $6)
        ORDER BY block_height DESC, event_id DESC, extrinsic_id DESC
        LIMIT $3 OFFSET $4
        "#,
        before.0,
        before.1,
        spec.limit,
        offset,
        payer,
        spec.until,
        before.2,
    )
    .fetch_all(&state.listing_db)
    .await?;

    let last = items.last().map(Keyed::key);
    Ok(Json(Page::build(
        items,
        total,
        (total_pages, page),
        spec.limit,
        last,
    )))
}

/// A back-half page, read oldest first and returned newest first.
async fn fee_burns_tail(
    state: &AppState,
    spec: &PageSpec,
    payer: Option<&str>,
    offset: i64,
    take: i64,
) -> Result<Vec<FeeBurnItem>, ApiError> {
    let mut items = sqlx::query_as!(
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
        WHERE ($3::text IS NULL OR payer = $3)
          AND ($4::timestamptz IS NULL OR block_timestamp <= $4)
        ORDER BY block_height ASC, event_id ASC, extrinsic_id ASC
        LIMIT $1 OFFSET $2
        "#,
        take,
        offset,
        payer,
        spec.until,
    )
    .fetch_all(&state.listing_db)
    .await?;
    items.reverse();
    Ok(items)
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
        assert_eq!(s.resolve(100, true), (5, 1, Seek::Head { offset: 0 }));
        assert_eq!((s.before_block, s.before_event), (i64::MAX, i32::MAX));
        assert!(s.needle.is_none() && s.until.is_none());
    }

    #[test]
    fn page_is_one_based_offset_and_clamped_like_the_node() {
        let s = pag(Some(3), Some(25), None).validate(25).unwrap();
        assert_eq!(s.resolve(1000, true), (40, 3, Seek::Head { offset: 50 }));
        // Out of range → last page (not an empty page), read from the oldest end.
        let last = Seek::Tail {
            offset: 0,
            take: 10,
        };
        assert_eq!(s.resolve(60, true), (3, 3, last));
        let s = pag(Some(999), Some(25), None).validate(25).unwrap();
        assert_eq!(s.resolve(60, true), (3, 3, last));
        assert_eq!(s.resolve(0, true), (0, 1, Seek::Head { offset: 0 }));
        assert_eq!(s.resolve(60, false), (3, 3, Seek::Head { offset: 50 }));
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
        assert_eq!(s.resolve(1000, true).2, Seek::Head { offset: 0 });
        assert_eq!(s.head_bound(None), (27_000_000, 5, ""));
    }

    #[test]
    fn cursor_carries_the_extrinsic_tiebreak() {
        let s = pag(None, None, Some("2929-0-0xabc")).validate(25).unwrap();
        assert_eq!(s.head_bound(None), (2929, 0, "0xabc"));
        let s = pag(None, None, Some("27538740-3-27538740-1"))
            .validate(25)
            .unwrap();
        assert_eq!(s.head_bound(None), (27_538_740, 3, "27538740-1"));
        assert_eq!(s.head_bound(Some((100, 0))), (100, 0, ""));
        assert_eq!(s.head_bound(Some((30_000_000, 0))).2, "27538740-1");
    }

    #[test]
    fn keyset_rejects_malformed() {
        for bad in ["nodash", "-1-2", "abc-1", "1-abc"] {
            assert!(pag(None, None, Some(bad)).validate(25).is_err(), "{bad}");
        }
    }

    #[test]
    fn limit_and_page_are_clamped_like_the_node() {
        assert_eq!(pag(Some(0), None, None).validate(25).unwrap().page, 1);
        assert_eq!(pag(None, Some(0), None).validate(25).unwrap().limit, 25);
        assert_eq!(pag(None, Some(101), None).validate(25).unwrap().limit, 100);
        assert_eq!(pag(None, Some(100), None).validate(25).unwrap().limit, 100);
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
    }

    #[test]
    fn histogram_share_interpolates_within_the_bucket() {
        let b = [0, 100, 200, 400, 1000];
        assert_eq!(histogram_share(&b, -1), Some(0.0));
        assert_eq!(histogram_share(&b, 0), Some(0.0));
        assert_eq!(histogram_share(&b, 50), Some(0.125));
        assert_eq!(histogram_share(&b, 300), Some(0.625));
        assert_eq!(histogram_share(&b, 1000), Some(1.0));
        assert_eq!(histogram_share(&[5], 5), None);
    }

    #[test]
    fn date_cap_lowers_the_keyset_sentinel() {
        let s = pag(None, None, None).validate(25).unwrap();
        assert_eq!(s.before(None), (i64::MAX, i32::MAX));
        assert_eq!(s.before(Some((1_000, 0))), (1_000, 0));
        let s = pag(None, None, Some("500-3")).validate(25).unwrap();
        assert_eq!(s.before(Some((1_000, 0))), (500, 3));
    }

    #[test]
    fn symbol_and_network_are_trimmed_exact_filters() {
        let p = Pagination {
            symbol: Some(" XOR ".into()),
            network: Some("Substrate: Liberland".into()),
            ..Pagination::default()
        };
        let s = p.validate(25).unwrap();
        assert_eq!(s.symbol.as_deref(), Some("XOR"));
        assert_eq!(s.network.as_deref(), Some("Substrate: Liberland"));
        assert!(s.needle.is_none());
        let p = Pagination {
            symbol: Some("  ".into()),
            ..Pagination::default()
        };
        assert!(p.validate(25).unwrap().symbol.is_none());
    }

    #[test]
    fn wallets_stand_alone() {
        let bot = "cnVcgVYJqhyuQohhYrZraVs85dujMDCBsBhMj5z8QPHq91C84";
        let p = Pagination {
            wallets: Some(bot.into()),
            ..Pagination::default()
        };
        let s = p.validate(25).unwrap();
        let set = s.wallets_alone().unwrap().unwrap();
        assert_eq!(set.addresses(), vec![bot.to_string()]);
        assert!(set.invalid.is_empty());
        let p = Pagination {
            wallets: Some(bot.into()),
            symbol: Some("XOR".into()),
            ..Pagination::default()
        };
        assert!(p.validate(25).unwrap().wallets_alone().is_err());
        assert!(pag(None, None, None)
            .validate(25)
            .unwrap()
            .wallets_alone()
            .unwrap()
            .is_none());
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
        let last = || Some((5, 1, "0xab".to_string()));
        assert_eq!(next_cursor(10, 10, last()), Some("5-1-0xab".into()));
        assert_eq!(next_cursor(9, 10, last()), None);
        assert_eq!(next_cursor(0, 10, None), None);
    }
}
