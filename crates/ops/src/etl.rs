//! `migrate-legacy` — one-way ETL from the legacy SoraMetrics PostgreSQL
//! (`squid` DB on the production VPS, or a tunnel/mock of it) into the
//! v33 schema. READ-ONLY on the source, idempotent on the target.
//!
//! Sources → targets (transformations mirror the legacy mechanisms —
//! see `scripts/create_materialized_views.sql` in the Node repo):
//!
//! | source                | target                  | notes |
//! |-----------------------|-------------------------|-------|
//! | `sm.mv_swaps`         | `sm.swaps`              | human → planck, event_id=0 |
//! | `sm.mv_transfers`     | `sm.transfers`          | idem |
//! | `sm.mv_bridges`       | `sm.bridges`            | direction map, caller/counterparty split |
//! | `sm.mv_fees`          | `sm.fees`               | verbatim (amount already XOR) |
//! | `sm.fee_burns_live`   | `sm.fee_burns_aggregate`| verbatim per-block aggregates |
//! | `sm.price_history`    | `ts.price_history`      | verbatim (hour_bucket = unix seconds) |
//! | `sm.asset_registry`   | `sm.asset_registry`     | upsert, legacy wins (962 > 277) |
//!
//! Mechanics:
//! - Keyset-paginated batches (`_row_id` / PK order), cursor persisted in
//!   `sm.etl_state` → resumable after interruption.
//! - Target inserts are batched `UNNEST` UPSERTs (`ON CONFLICT DO
//!   NOTHING`) — re-runs are no-ops, matching the ingest idempotency.
//! - Human → planck uses `('1' || repeat('0', decimals))::numeric` —
//!   exact by construction (numeric `^`/`power()` go through float paths).
//!   Missing registry decimals default to 18, the same fallback the Node
//!   indexer uses (`assetInfo?.decimals || 18`).
//! - Rows that cannot satisfy target NOT NULLs (NULL asset/amount/
//!   address in the source MV) are SKIPPED and REPORTED — never silently
//!   dropped: `reconcile` prints the exact skipped count per table.
//!
//! Compile-time SQL checking (`sqlx::query!`) only works against OUR
//! database. Source queries target an external schema and are therefore
//! runtime `sqlx::query` — the one documented exception to the "todo
//! `sqlx::query!`" rule. Every source read is still typed at the
//! extraction site via `try_get::<T, _>`.

use anyhow::{bail, Context, Result};
use bigdecimal::BigDecimal;
use chrono::{DateTime, NaiveDate, Utc};
use serde_json::Value as Json;
use sqlx::postgres::PgPoolOptions;
use sqlx::{PgPool, Row};
use std::collections::HashMap;
use std::time::Instant;
use tracing::{info, warn};

/// All known ETL tables, in dependency order (asset_registry first: the
/// planck conversions of the event tables JOIN it on the SOURCE side,
/// so order only matters for operator sanity, not correctness).
pub const ALL_TABLES: [&str; 21] = [
    "asset_registry",
    "swaps",
    "transfers",
    "bridges",
    "fees",
    "fee_burns",
    "price_history",
    "liquidity",
    "extrinsics",
    "order_book",
    "val_staking_rewards",
    "supply_snapshots",
    "supply_history",
    "news_episodes",
    "polkamarkt_markets",
    "polkamarkt_trades",
    "polkamarkt_claims",
    "polkamarkt_buybacks",
    "polkamarkt_burns",
    "site_daily",
    "site_events",
];

/// Options for one `migrate-legacy` run.
pub struct EtlOpts {
    /// Connection string of the LEGACY database (read-only usage).
    pub source_url: String,
    /// Which tables to migrate (subset of [`ALL_TABLES`]).
    pub tables: Vec<String>,
    /// Rows per batch.
    pub batch_size: i64,
    /// Skip the post-copy reconciliation (NOT recommended; the project
    /// treats reconciliation as a mandatory step).
    pub skip_reconcile: bool,
}

/// Entry point for the `migrate-legacy` subcommand.
pub async fn migrate_legacy(target: PgPool, opts: EtlOpts) -> Result<()> {
    for t in &opts.tables {
        if !ALL_TABLES.contains(&t.as_str()) && !crate::etl_mn::MN_TABLES.contains(&t.as_str()) {
            bail!(
                "unknown table '{t}' — valid: {},{}",
                ALL_TABLES.join(","),
                crate::etl_mn::MN_TABLES.join(",")
            );
        }
    }

    let source = PgPoolOptions::new()
        .max_connections(4)
        .connect(&opts.source_url)
        .await
        .context("connecting to legacy source DB")?;

    for table in &opts.tables {
        let started = Instant::now();
        info!(table, "ETL start");
        let copied = match table.as_str() {
            "asset_registry" => copy_asset_registry(&source, &target).await?,
            "swaps" => copy_swaps(&source, &target, opts.batch_size).await?,
            "transfers" => copy_transfers(&source, &target, opts.batch_size).await?,
            "bridges" => copy_bridges(&source, &target, opts.batch_size).await?,
            "fees" => copy_fees(&source, &target, opts.batch_size).await?,
            "fee_burns" => copy_fee_burns(&source, &target, opts.batch_size).await?,
            "price_history" => copy_price_history(&source, &target, opts.batch_size).await?,
            "liquidity" => copy_liquidity(&source, &target, opts.batch_size).await?,
            "extrinsics" => copy_extrinsics(&source, &target, opts.batch_size).await?,
            "order_book" => copy_order_book(&source, &target, opts.batch_size).await?,
            "val_staking_rewards" => {
                copy_val_staking_rewards(&source, &target, opts.batch_size).await?
            }
            "supply_snapshots" => copy_supply_snapshots(&source, &target, opts.batch_size).await?,
            "supply_history" => copy_supply_history(&source, &target, opts.batch_size).await?,
            "news_episodes" => copy_news_episodes(&source, &target, opts.batch_size).await?,
            "site_daily" => copy_site_daily(&source, &target).await?,
            "site_events" => copy_site_events(&source, &target, opts.batch_size).await?,
            "polkamarkt_markets" => copy_pm_markets(&source, &target, opts.batch_size).await?,
            "polkamarkt_trades" => copy_pm_trades(&source, &target, opts.batch_size).await?,
            "polkamarkt_claims" => copy_pm_claims(&source, &target, opts.batch_size).await?,
            "polkamarkt_buybacks" => copy_pm_buybacks(&source, &target, opts.batch_size).await?,
            "polkamarkt_burns" => copy_pm_burns(&source, &target, opts.batch_size).await?,
            t if t.starts_with("mn_") => {
                crate::etl_mn::copy(&source, &target, t, opts.batch_size).await?
            }
            _ => unreachable!("validated above"),
        };
        info!(
            table,
            copied,
            elapsed_s = format!("{:.1}", started.elapsed().as_secs_f64()),
            "ETL table done"
        );
    }

    if opts.skip_reconcile {
        warn!("reconciliation SKIPPED by flag — the migration is NOT verified");
        return Ok(());
    }

    let mut failures = 0u32;
    for table in &opts.tables {
        if !reconcile_table(&source, &target, table).await? {
            failures += 1;
        }
    }
    if failures > 0 {
        bail!("{failures} table(s) failed reconciliation — see log above");
    }
    info!("reconciliation OK for all migrated tables");
    Ok(())
}

// =============================================================
// Cursor state
// =============================================================

async fn get_cursor(target: &PgPool, table: &str) -> Result<Option<String>> {
    let row = sqlx::query!(
        r#"SELECT last_cursor FROM sm.etl_state WHERE table_name = $1"#,
        table
    )
    .fetch_optional(target)
    .await?;
    Ok(row.and_then(|r| r.last_cursor))
}

async fn set_cursor(target: &PgPool, table: &str, cursor: &str, batch_rows: i64) -> Result<()> {
    sqlx::query!(
        r#"
        INSERT INTO sm.etl_state (table_name, last_cursor, rows_copied, updated_at)
        VALUES ($1, $2, $3, NOW())
        ON CONFLICT (table_name) DO UPDATE
            SET last_cursor = EXCLUDED.last_cursor,
                rows_copied = sm.etl_state.rows_copied + EXCLUDED.rows_copied,
                updated_at  = NOW()
        "#,
        table,
        cursor,
        batch_rows,
    )
    .execute(target)
    .await?;
    Ok(())
}

// =============================================================
// asset_registry (small, single pass, legacy wins)
// =============================================================

async fn copy_asset_registry(source: &PgPool, target: &PgPool) -> Result<u64> {
    // Legacy registry includes non-canonical rows; the target CHECK
    // requires 0x + 64 hex. Off-shape ids are skipped and counted.
    let rows = sqlx::query(
        r#"
        SELECT asset_id, symbol, COALESCE(name, '') AS name,
               COALESCE(decimals, 18)::int AS decimals, logo
        FROM sm.asset_registry
        WHERE length(asset_id) = 66
        ORDER BY asset_id
        "#,
    )
    .fetch_all(source)
    .await
    .context("reading legacy asset_registry")?;

    let mut copied = 0u64;
    for r in &rows {
        let asset_id: String = r.try_get("asset_id")?;
        let symbol: String = r.try_get("symbol")?;
        let name: String = r.try_get("name")?;
        let decimals: i32 = r.try_get("decimals")?;
        let logo: Option<String> = r.try_get("logo")?;

        if !(0..=38).contains(&decimals) {
            warn!(
                asset_id,
                decimals, "skipping asset with out-of-range decimals"
            );
            continue;
        }
        sqlx::query!(
            r#"
            INSERT INTO sm.asset_registry (asset_id, symbol, name, decimals, logo, updated_at)
            VALUES ($1, $2, NULLIF($3, ''), $4, $5, NOW())
            ON CONFLICT (asset_id) DO UPDATE
                SET symbol = EXCLUDED.symbol,
                    name = COALESCE(EXCLUDED.name, sm.asset_registry.name),
                    decimals = EXCLUDED.decimals,
                    logo = COALESCE(EXCLUDED.logo, sm.asset_registry.logo),
                    updated_at = NOW()
            "#,
            asset_id,
            symbol,
            name,
            decimals as i16,
            logo,
        )
        .execute(target)
        .await?;
        copied += 1;
    }
    Ok(copied)
}

// =============================================================
// swaps
// =============================================================

/// Exact power-of-ten scaling: `'1' || repeat('0', d)` builds 10^d as a
/// string → numeric. Never goes through a float path.
const SCALE: &str = "('1' || repeat('0', GREATEST(COALESCE({AR}.decimals, 18), 0)))::numeric";

fn scale_expr(alias: &str) -> String {
    SCALE.replace("{AR}", alias)
}

/// Source filter shared by copy + reconciliation so both count the same
/// row population (skipped rows = source total − filtered total).
const SWAPS_FILTER: &str = "s.in_asset_id IS NOT NULL AND s.out_asset_id IS NOT NULL \
     AND s.in_amount IS NOT NULL AND s.out_amount IS NOT NULL AND s.wallet IS NOT NULL";

async fn copy_swaps(source: &PgPool, target: &PgPool, batch: i64) -> Result<u64> {
    let sql = format!(
        r#"
        SELECT s._row_id,
               s.block::bigint AS block_height,
               to_timestamp(s.timestamp / 1000.0) AS block_timestamp,
               s.wallet AS caller,
               s.in_asset_id, s.out_asset_id,
               (s.in_amount::numeric  * {sc_in})::numeric(78,0)  AS input_amount,
               (s.out_amount::numeric * {sc_out})::numeric(78,0) AS output_amount,
               s.in_usd::numeric(38,6) AS usd_value,
               s.out_usd::numeric(38,6) AS output_usd_value,
               s.hash, s.extrinsic_id
        FROM sm.mv_swaps s
        LEFT JOIN sm.asset_registry ar_in  ON ar_in.asset_id  = s.in_asset_id
        LEFT JOIN sm.asset_registry ar_out ON ar_out.asset_id = s.out_asset_id
        WHERE s._row_id > $1 AND {SWAPS_FILTER}
        ORDER BY s._row_id
        LIMIT $2
        "#,
        sc_in = scale_expr("ar_in"),
        sc_out = scale_expr("ar_out"),
    );

    let mut copied = 0u64;
    let mut cursor = get_cursor(target, "swaps").await?.unwrap_or_default();
    loop {
        let rows = sqlx::query(&sql)
            .bind(&cursor)
            .bind(batch)
            .fetch_all(source)
            .await
            .context("reading legacy mv_swaps batch")?;
        if rows.is_empty() {
            break;
        }

        let n = rows.len();
        let mut blocks = Vec::with_capacity(n);
        let mut ext_ids = Vec::with_capacity(n);
        let mut tss = Vec::with_capacity(n);
        let mut callers = Vec::with_capacity(n);
        let mut in_assets = Vec::with_capacity(n);
        let mut in_amounts = Vec::with_capacity(n);
        let mut out_assets = Vec::with_capacity(n);
        let mut out_amounts = Vec::with_capacity(n);
        let mut usds: Vec<Option<BigDecimal>> = Vec::with_capacity(n);
        let mut out_usds: Vec<Option<BigDecimal>> = Vec::with_capacity(n);
        let mut hashes: Vec<Option<String>> = Vec::with_capacity(n);

        for r in &rows {
            blocks.push(r.try_get::<i64, _>("block_height")?);
            ext_ids.push(r.try_get::<String, _>("extrinsic_id")?);
            tss.push(r.try_get::<DateTime<Utc>, _>("block_timestamp")?);
            callers.push(r.try_get::<String, _>("caller")?);
            in_assets.push(r.try_get::<String, _>("in_asset_id")?);
            in_amounts.push(r.try_get::<BigDecimal, _>("input_amount")?);
            out_assets.push(r.try_get::<String, _>("out_asset_id")?);
            out_amounts.push(r.try_get::<BigDecimal, _>("output_amount")?);
            usds.push(r.try_get::<Option<BigDecimal>, _>("usd_value")?);
            out_usds.push(r.try_get::<Option<BigDecimal>, _>("output_usd_value")?);
            hashes.push(r.try_get::<Option<String>, _>("hash")?);
            cursor = r.try_get::<String, _>("_row_id")?;
        }

        sqlx::query!(
            r#"
            INSERT INTO sm.swaps (
                block_height, extrinsic_id, event_id, block_timestamp,
                caller, input_asset_id, input_amount, output_asset_id, output_amount,
                usd_value, output_usd_value, hash, origin
            )
            SELECT b, e, 0, t, c, ia, iam, oa, oam, u, ou, h, 'legacy'
            FROM UNNEST(
                $1::bigint[], $2::text[], $3::timestamptz[], $4::text[],
                $5::text[], $6::numeric[], $7::text[], $8::numeric[],
                $9::numeric[], $10::numeric[], $11::text[]
            ) AS x(b, e, t, c, ia, iam, oa, oam, u, ou, h)
            ON CONFLICT (block_height, extrinsic_id, event_id) DO NOTHING
            "#,
            &blocks,
            &ext_ids,
            &tss,
            &callers,
            &in_assets,
            &in_amounts,
            &out_assets,
            &out_amounts,
            &usds as &[Option<BigDecimal>],
            &out_usds as &[Option<BigDecimal>],
            &hashes as &[Option<String>],
        )
        .execute(target)
        .await
        .context("inserting swaps batch")?;

        copied += n as u64;
        set_cursor(target, "swaps", &cursor, n as i64).await?;
        info!(copied, cursor = %cursor, "swaps progress");
    }
    Ok(copied)
}

// =============================================================
// transfers
// =============================================================

const TRANSFERS_FILTER: &str = "t.from_addr IS NOT NULL AND t.to_addr IS NOT NULL \
     AND t.asset_id IS NOT NULL AND t.amount IS NOT NULL";

async fn copy_transfers(source: &PgPool, target: &PgPool, batch: i64) -> Result<u64> {
    let sql = format!(
        r#"
        SELECT t._row_id,
               t.block::bigint AS block_height,
               to_timestamp(t.timestamp / 1000.0) AS block_timestamp,
               t.from_addr, t.to_addr, t.asset_id,
               (t.amount::numeric * {sc})::numeric(78,0) AS amount,
               t.usd_value::numeric(38,6) AS usd_value,
               t.hash, t.extrinsic_id
        FROM sm.mv_transfers t
        LEFT JOIN sm.asset_registry ar ON ar.asset_id = t.asset_id
        WHERE t._row_id > $1 AND {TRANSFERS_FILTER}
        ORDER BY t._row_id
        LIMIT $2
        "#,
        sc = scale_expr("ar"),
    );

    let mut copied = 0u64;
    let mut cursor = get_cursor(target, "transfers").await?.unwrap_or_default();
    loop {
        let rows = sqlx::query(&sql)
            .bind(&cursor)
            .bind(batch)
            .fetch_all(source)
            .await
            .context("reading legacy mv_transfers batch")?;
        if rows.is_empty() {
            break;
        }

        let n = rows.len();
        let mut blocks = Vec::with_capacity(n);
        let mut ext_ids = Vec::with_capacity(n);
        let mut tss = Vec::with_capacity(n);
        let mut froms = Vec::with_capacity(n);
        let mut tos = Vec::with_capacity(n);
        let mut assets = Vec::with_capacity(n);
        let mut amounts = Vec::with_capacity(n);
        let mut usds: Vec<Option<BigDecimal>> = Vec::with_capacity(n);
        let mut hashes: Vec<Option<String>> = Vec::with_capacity(n);

        for r in &rows {
            blocks.push(r.try_get::<i64, _>("block_height")?);
            ext_ids.push(r.try_get::<String, _>("extrinsic_id")?);
            tss.push(r.try_get::<DateTime<Utc>, _>("block_timestamp")?);
            froms.push(r.try_get::<String, _>("from_addr")?);
            tos.push(r.try_get::<String, _>("to_addr")?);
            assets.push(r.try_get::<String, _>("asset_id")?);
            amounts.push(r.try_get::<BigDecimal, _>("amount")?);
            usds.push(r.try_get::<Option<BigDecimal>, _>("usd_value")?);
            hashes.push(r.try_get::<Option<String>, _>("hash")?);
            cursor = r.try_get::<String, _>("_row_id")?;
        }

        sqlx::query!(
            r#"
            INSERT INTO sm.transfers (
                block_height, extrinsic_id, event_id, block_timestamp,
                from_address, to_address, asset_id, amount, usd_value, hash, origin
            )
            SELECT b, e, 0, t, f, "to", a, am, u, h, 'legacy'
            FROM UNNEST(
                $1::bigint[], $2::text[], $3::timestamptz[], $4::text[],
                $5::text[], $6::text[], $7::numeric[], $8::numeric[], $9::text[]
            ) AS x(b, e, t, f, "to", a, am, u, h)
            ON CONFLICT (block_height, extrinsic_id, event_id) DO NOTHING
            "#,
            &blocks,
            &ext_ids,
            &tss,
            &froms,
            &tos,
            &assets,
            &amounts,
            &usds as &[Option<BigDecimal>],
            &hashes as &[Option<String>],
        )
        .execute(target)
        .await
        .context("inserting transfers batch")?;

        copied += n as u64;
        set_cursor(target, "transfers", &cursor, n as i64).await?;
        info!(copied, cursor = %cursor, "transfers progress");
    }
    Ok(copied)
}

// =============================================================
// bridges
// =============================================================

const BRIDGES_FILTER: &str = "b.asset_id IS NOT NULL AND b.amount IS NOT NULL \
     AND b.direction IS NOT NULL \
     AND (CASE WHEN b.direction = 'Outgoing' THEN b.sender ELSE COALESCE(b.recipient, b.sender) END) IS NOT NULL";

async fn copy_bridges(source: &PgPool, target: &PgPool, batch: i64) -> Result<u64> {
    // caller = the SORA-side address (same convention as the live
    // decoder: sender on Outgoing/Burned, recipient on Incoming/Minted).
    // counterparty = the other side, when the legacy row has it.
    let sql = format!(
        r#"
        SELECT b._row_id,
               b.block::bigint AS block_height,
               to_timestamp(b.timestamp / 1000.0) AS block_timestamp,
               CASE b.direction WHEN 'Outgoing' THEN 'out' ELSE 'in' END AS direction,
               b.network,
               CASE WHEN b.direction = 'Outgoing' THEN b.sender
                    ELSE COALESCE(b.recipient, b.sender) END AS caller,
               CASE WHEN b.direction = 'Outgoing' THEN b.recipient
                    ELSE NULLIF(b.sender, COALESCE(b.recipient, b.sender)) END AS counterparty,
               b.asset_id,
               (b.amount::numeric * {sc})::numeric(78,0) AS amount,
               b.usd_value::numeric(38,6) AS usd_value,
               b.hash, b.extrinsic_id
        FROM sm.mv_bridges b
        LEFT JOIN sm.asset_registry ar ON ar.asset_id = b.asset_id
        WHERE b._row_id > $1 AND {BRIDGES_FILTER}
        ORDER BY b._row_id
        LIMIT $2
        "#,
        sc = scale_expr("ar"),
    );

    let mut copied = 0u64;
    let mut cursor = get_cursor(target, "bridges").await?.unwrap_or_default();
    loop {
        let rows = sqlx::query(&sql)
            .bind(&cursor)
            .bind(batch)
            .fetch_all(source)
            .await
            .context("reading legacy mv_bridges batch")?;
        if rows.is_empty() {
            break;
        }

        let n = rows.len();
        let mut blocks = Vec::with_capacity(n);
        let mut ext_ids = Vec::with_capacity(n);
        let mut tss = Vec::with_capacity(n);
        let mut directions = Vec::with_capacity(n);
        let mut networks = Vec::with_capacity(n);
        let mut callers = Vec::with_capacity(n);
        let mut counterparties: Vec<Option<String>> = Vec::with_capacity(n);
        let mut assets = Vec::with_capacity(n);
        let mut amounts = Vec::with_capacity(n);
        let mut usds: Vec<Option<BigDecimal>> = Vec::with_capacity(n);
        let mut hashes: Vec<Option<String>> = Vec::with_capacity(n);

        for r in &rows {
            blocks.push(r.try_get::<i64, _>("block_height")?);
            ext_ids.push(r.try_get::<String, _>("extrinsic_id")?);
            tss.push(r.try_get::<DateTime<Utc>, _>("block_timestamp")?);
            directions.push(r.try_get::<String, _>("direction")?);
            networks.push(r.try_get::<String, _>("network")?);
            callers.push(r.try_get::<String, _>("caller")?);
            counterparties.push(r.try_get::<Option<String>, _>("counterparty")?);
            assets.push(r.try_get::<String, _>("asset_id")?);
            amounts.push(r.try_get::<BigDecimal, _>("amount")?);
            usds.push(r.try_get::<Option<BigDecimal>, _>("usd_value")?);
            hashes.push(r.try_get::<Option<String>, _>("hash")?);
            cursor = r.try_get::<String, _>("_row_id")?;
        }

        sqlx::query!(
            r#"
            INSERT INTO sm.bridges (
                block_height, extrinsic_id, event_id, block_timestamp,
                direction, network, caller, counterparty, asset_id, amount,
                usd_value, hash, origin
            )
            SELECT b, e, 0, t, d::sm.bridge_direction, nw, c, cp, a, am, u, h, 'legacy'
            FROM UNNEST(
                $1::bigint[], $2::text[], $3::timestamptz[], $4::text[],
                $5::text[], $6::text[], $7::text[], $8::text[],
                $9::numeric[], $10::numeric[], $11::text[]
            ) AS x(b, e, t, d, nw, c, cp, a, am, u, h)
            ON CONFLICT (block_height, extrinsic_id, event_id) DO NOTHING
            "#,
            &blocks,
            &ext_ids,
            &tss,
            &directions,
            &networks,
            &callers,
            &counterparties as &[Option<String>],
            &assets,
            &amounts,
            &usds as &[Option<BigDecimal>],
            &hashes as &[Option<String>],
        )
        .execute(target)
        .await
        .context("inserting bridges batch")?;

        copied += n as u64;
        set_cursor(target, "bridges", &cursor, n as i64).await?;
        info!(copied, cursor = %cursor, "bridges progress");
    }
    Ok(copied)
}

// =============================================================
// fees (legacy mv_fees, amounts already human XOR)
// =============================================================

async fn copy_fees(source: &PgPool, target: &PgPool, batch: i64) -> Result<u64> {
    let sql = r#"
        SELECT f._row_id,
               f.block::bigint AS block_height,
               to_timestamp(f.timestamp / 1000.0) AS block_timestamp,
               f.type AS fee_type,
               f.amount::numeric(38,18) AS amount_xor,
               f.usd_value::numeric(38,6) AS usd_value
        FROM sm.mv_fees f
        WHERE f._row_id > $1
        ORDER BY f._row_id
        LIMIT $2
        "#;

    let mut copied = 0u64;
    let mut cursor = get_cursor(target, "fees").await?.unwrap_or_default();
    loop {
        let rows = sqlx::query(sql)
            .bind(&cursor)
            .bind(batch)
            .fetch_all(source)
            .await
            .context("reading legacy mv_fees batch")?;
        if rows.is_empty() {
            break;
        }

        let n = rows.len();
        let mut ids = Vec::with_capacity(n);
        let mut blocks = Vec::with_capacity(n);
        let mut tss = Vec::with_capacity(n);
        let mut types = Vec::with_capacity(n);
        let mut amounts = Vec::with_capacity(n);
        let mut usds: Vec<Option<BigDecimal>> = Vec::with_capacity(n);

        for r in &rows {
            ids.push(r.try_get::<String, _>("_row_id")?);
            blocks.push(r.try_get::<i64, _>("block_height")?);
            tss.push(r.try_get::<DateTime<Utc>, _>("block_timestamp")?);
            types.push(r.try_get::<String, _>("fee_type")?);
            amounts.push(r.try_get::<BigDecimal, _>("amount_xor")?);
            usds.push(r.try_get::<Option<BigDecimal>, _>("usd_value")?);
            cursor = ids.last().cloned().unwrap_or_default();
        }

        sqlx::query!(
            r#"
            INSERT INTO sm.fees (legacy_id, block_height, block_timestamp, fee_type, amount_xor, usd_value)
            SELECT i, b, t, ty, a, u
            FROM UNNEST($1::text[], $2::bigint[], $3::timestamptz[], $4::text[], $5::numeric[], $6::numeric[])
                AS x(i, b, t, ty, a, u)
            ON CONFLICT (legacy_id) DO NOTHING
            "#,
            &ids,
            &blocks,
            &tss,
            &types,
            &amounts,
            &usds as &[Option<BigDecimal>],
        )
        .execute(target)
        .await
        .context("inserting fees batch")?;

        copied += n as u64;
        set_cursor(target, "fees", &cursor, n as i64).await?;
        info!(copied, cursor = %cursor, "fees progress");
    }
    Ok(copied)
}

// =============================================================
// fee_burns (legacy fee_burns_live, verbatim)
// =============================================================

async fn copy_fee_burns(source: &PgPool, target: &PgPool, batch: i64) -> Result<u64> {
    let sql = r#"
        SELECT block_height, ts,
               fees_paid_xor, ref_paid_xor, ref_redirected_xor,
               remint_xor_burned, remint_val_burned, remint_kusd_burned, remint_tbcd_burned
        FROM sm.fee_burns_live
        WHERE block_height > $1
        ORDER BY block_height
        LIMIT $2
        "#;

    let mut copied = 0u64;
    let mut cursor: i64 = get_cursor(target, "fee_burns")
        .await?
        .and_then(|c| c.parse().ok())
        .unwrap_or(-1);
    loop {
        let rows = sqlx::query(sql)
            .bind(cursor)
            .bind(batch)
            .fetch_all(source)
            .await
            .context("reading legacy fee_burns_live batch")?;
        if rows.is_empty() {
            break;
        }

        let n = rows.len();
        let mut blocks = Vec::with_capacity(n);
        let mut tss = Vec::with_capacity(n);
        let mut cols: [Vec<BigDecimal>; 7] = Default::default();
        const NAMES: [&str; 7] = [
            "fees_paid_xor",
            "ref_paid_xor",
            "ref_redirected_xor",
            "remint_xor_burned",
            "remint_val_burned",
            "remint_kusd_burned",
            "remint_tbcd_burned",
        ];

        for r in &rows {
            blocks.push(r.try_get::<i64, _>("block_height")?);
            tss.push(r.try_get::<i64, _>("ts")?);
            for (i, name) in NAMES.iter().enumerate() {
                cols[i].push(r.try_get::<BigDecimal, _>(name)?);
            }
            let Some(last) = blocks.last() else { break };
            cursor = *last;
        }

        sqlx::query!(
            r#"
            INSERT INTO sm.fee_burns_aggregate (
                block_height, ts, fees_paid_xor, ref_paid_xor, ref_redirected_xor,
                remint_xor_burned, remint_val_burned, remint_kusd_burned, remint_tbcd_burned
            )
            SELECT b, t, c1, c2, c3, c4, c5, c6, c7
            FROM UNNEST(
                $1::bigint[], $2::bigint[], $3::numeric[], $4::numeric[], $5::numeric[],
                $6::numeric[], $7::numeric[], $8::numeric[], $9::numeric[]
            ) AS x(b, t, c1, c2, c3, c4, c5, c6, c7)
            ON CONFLICT (block_height) DO NOTHING
            "#,
            &blocks,
            &tss,
            &cols[0],
            &cols[1],
            &cols[2],
            &cols[3],
            &cols[4],
            &cols[5],
            &cols[6],
        )
        .execute(target)
        .await
        .context("inserting fee_burns batch")?;

        copied += n as u64;
        set_cursor(target, "fee_burns", &cursor.to_string(), n as i64).await?;
        info!(copied, cursor, "fee_burns progress");
    }
    Ok(copied)
}

// =============================================================
// price_history (verbatim, composite keyset)
// =============================================================

/// Symbol → canonical asset id and decimals from the TARGET registry
/// (whitelisted first, then the lowest id — the API's resolution rule).
/// The legacy `mv_liquidity_events` only kept symbols.
async fn target_symbol_map(target: &PgPool) -> Result<HashMap<String, (String, u32)>> {
    let rows = sqlx::query!(
        r#"SELECT asset_id, symbol, decimals, whitelisted FROM sm.asset_registry
           ORDER BY whitelisted DESC, asset_id ASC"#
    )
    .fetch_all(target)
    .await?;
    let mut map = HashMap::new();
    for r in rows {
        map.entry(r.symbol)
            .or_insert((r.asset_id, r.decimals.max(0) as u32));
    }
    Ok(map)
}

/// Human decimal text → exact planck (`BigDecimal × 10^decimals`),
/// rejecting fractional planck.
fn human_to_planck(text: &str, decimals: u32) -> Option<BigDecimal> {
    let v: BigDecimal = text.trim().parse().ok()?;
    let scaled = v * BigDecimal::new(1.into(), -(decimals as i64));
    scaled.is_integer().then(|| scaled.with_scale(0))
}

/// Rows of `sm.mv_liquidity_events` → `sm.liquidity_events`. Symbols
/// that do not resolve in the target registry (incl. the MV's `0xABCD`
/// fallbacks) are SKIPPED and counted — the reconciliation checks
/// `source = copied + skipped`.
async fn copy_liquidity(source: &PgPool, target: &PgPool, batch: i64) -> Result<u64> {
    let symbols = target_symbol_map(target).await?;
    let sql = r#"
        SELECT l._row_id,
               l.block::bigint AS block_height,
               to_timestamp(l.timestamp / 1000.0) AS block_timestamp,
               l.wallet, l.pool_base, l.pool_target,
               l.base_amount, l.target_amount,
               l.usd_value::numeric(38,6) AS usd_value,
               lower(l.type) AS kind, l.hash, l.extrinsic_id
        FROM sm.mv_liquidity_events l
        WHERE l._row_id > $1 AND l.wallet IS NOT NULL
        ORDER BY l._row_id
        LIMIT $2
        "#;
    let mut copied = 0u64;
    let mut skipped = 0u64;
    let mut cursor = get_cursor(target, "liquidity").await?.unwrap_or_default();
    loop {
        let rows = sqlx::query(sql)
            .bind(&cursor)
            .bind(batch)
            .fetch_all(source)
            .await
            .context("reading legacy mv_liquidity_events batch")?;
        if rows.is_empty() {
            break;
        }
        let n = rows.len();
        let mut blocks = Vec::with_capacity(n);
        let mut ext_ids = Vec::with_capacity(n);
        let mut tss = Vec::with_capacity(n);
        let mut callers = Vec::with_capacity(n);
        let mut bases = Vec::with_capacity(n);
        let mut targets = Vec::with_capacity(n);
        let mut base_amounts = Vec::with_capacity(n);
        let mut target_amounts = Vec::with_capacity(n);
        let mut usds: Vec<Option<BigDecimal>> = Vec::with_capacity(n);
        let mut kinds = Vec::with_capacity(n);
        let mut hashes: Vec<Option<String>> = Vec::with_capacity(n);
        for r in &rows {
            cursor = r.try_get::<String, _>("_row_id")?;
            let kind: String = r.try_get("kind")?;
            let base_sym: Option<String> = r.try_get("pool_base")?;
            let target_sym: Option<String> = r.try_get("pool_target")?;
            let resolved = match (
                base_sym.as_deref().and_then(|s| symbols.get(s)),
                target_sym.as_deref().and_then(|s| symbols.get(s)),
            ) {
                (Some(b), Some(t)) if kind == "deposit" || kind == "withdraw" => Some((b, t)),
                _ => None,
            };
            let Some(((base_id, base_dec), (target_id, target_dec))) = resolved else {
                skipped += 1;
                continue;
            };
            let ba: Option<String> = r.try_get("base_amount")?;
            let ta: Option<String> = r.try_get("target_amount")?;
            let (Some(ba), Some(ta)) = (
                ba.as_deref().and_then(|v| human_to_planck(v, *base_dec)),
                ta.as_deref().and_then(|v| human_to_planck(v, *target_dec)),
            ) else {
                skipped += 1;
                continue;
            };
            blocks.push(r.try_get::<i64, _>("block_height")?);
            ext_ids.push(r.try_get::<String, _>("extrinsic_id")?);
            tss.push(r.try_get::<DateTime<Utc>, _>("block_timestamp")?);
            callers.push(r.try_get::<String, _>("wallet")?);
            bases.push(base_id.clone());
            targets.push(target_id.clone());
            base_amounts.push(ba);
            target_amounts.push(ta);
            usds.push(r.try_get::<Option<BigDecimal>, _>("usd_value")?);
            kinds.push(kind);
            hashes.push(r.try_get::<Option<String>, _>("hash")?);
        }
        if !blocks.is_empty() {
            sqlx::query!(
                r#"
                INSERT INTO sm.liquidity_events (
                    block_height, extrinsic_id, event_id, block_timestamp, caller,
                    base_asset_id, target_asset_id, base_amount, target_amount,
                    usd_value, kind, hash, origin
                )
                SELECT b, e, 0, t, c, ba, ta, bam, tam, u, k, h, 'legacy'
                FROM UNNEST(
                    $1::bigint[], $2::text[], $3::timestamptz[], $4::text[], $5::text[],
                    $6::text[], $7::numeric[], $8::numeric[], $9::numeric[], $10::text[], $11::text[]
                ) AS x(b, e, t, c, ba, ta, bam, tam, u, k, h)
                ON CONFLICT (block_height, extrinsic_id, event_id) DO NOTHING
                "#,
                &blocks,
                &ext_ids,
                &tss,
                &callers,
                &bases,
                &targets,
                &base_amounts,
                &target_amounts,
                &usds as &[Option<BigDecimal>],
                &kinds,
                &hashes as &[Option<String>],
            )
            .execute(target)
            .await
            .context("inserting liquidity batch")?;
        }
        copied += blocks.len() as u64;
        set_cursor(target, "liquidity", &cursor, blocks.len() as i64).await?;
        info!(copied, skipped, cursor = %cursor, "liquidity progress");
    }
    if skipped > 0 {
        warn!(
            skipped,
            "liquidity rows skipped (unresolvable symbol / non-integer planck) — review before cutover"
        );
    }
    set_skipped(target, "liquidity", skipped).await?;
    Ok(copied)
}

const EXTRINSICS_FILTER: &str =
    "x.block IS NOT NULL AND x.extrinsic_index IS NOT NULL AND x.section IS NOT NULL AND x.method IS NOT NULL AND x.signer IS NOT NULL AND x.timestamp IS NOT NULL";

/// Rows of `sm.mv_extrinsics` → `sm.extrinsics` (origin 'legacy').
/// The MV's `extrinsic_index` is SYNTHETIC (ROW_NUMBER per block) and
/// carries no args/events; the Node resolves those on demand in
/// `getExtrinsicDetail`, and this copy resolves them the same way:
/// `args` = `public.history_element.data::text` of the row whose `id` is
/// the hash (kept as a JSON string, the exact text the Node serves);
/// `events` = the first 100 rows of `sm.extrinsic_events` for
/// (block, index) by `event_index`, minus `System.ExtrinsicSuccess/
/// Failed`, as `[{s, m, d}]` with `d` the compact JSON of `data`.
/// No row / no event → NULL (the API serves `{}` / `null`).
async fn copy_extrinsics(source: &PgPool, target: &PgPool, batch: i64) -> Result<u64> {
    let sql = format!(
        r#"
        SELECT x._row_id,
               x.block::bigint AS block_height,
               x.extrinsic_index::int AS extrinsic_index,
               to_timestamp(x.timestamp / 1000.0) AS block_timestamp,
               COALESCE(x.hash, '') AS hash,
               x.section, x.method, x.signer,
               (x.success = 1) AS success,
               COALESCE(x.error_msg, '') AS error_msg
        FROM sm.mv_extrinsics x
        WHERE x._row_id > $1 AND {EXTRINSICS_FILTER}
        ORDER BY x._row_id
        LIMIT $2
        "#
    );
    let mut copied = 0u64;
    let mut cursor = get_cursor(target, "extrinsics").await?.unwrap_or_default();
    loop {
        let rows = sqlx::query(&sql)
            .bind(&cursor)
            .bind(batch)
            .fetch_all(source)
            .await
            .context("reading legacy mv_extrinsics batch")?;
        if rows.is_empty() {
            break;
        }
        let n = rows.len();
        let mut blocks = Vec::with_capacity(n);
        let mut idxs = Vec::with_capacity(n);
        let mut tss = Vec::with_capacity(n);
        let mut hashes = Vec::with_capacity(n);
        let mut sections = Vec::with_capacity(n);
        let mut methods = Vec::with_capacity(n);
        let mut signers = Vec::with_capacity(n);
        let mut successes = Vec::with_capacity(n);
        let mut errors = Vec::with_capacity(n);
        for r in &rows {
            blocks.push(r.try_get::<i64, _>("block_height")?);
            idxs.push(r.try_get::<i32, _>("extrinsic_index")?);
            tss.push(r.try_get::<DateTime<Utc>, _>("block_timestamp")?);
            hashes.push(r.try_get::<String, _>("hash")?);
            sections.push(r.try_get::<String, _>("section")?);
            methods.push(r.try_get::<String, _>("method")?);
            signers.push(r.try_get::<String, _>("signer")?);
            successes.push(r.try_get::<bool, _>("success")?);
            errors.push(r.try_get::<String, _>("error_msg")?);
            cursor = r.try_get::<String, _>("_row_id")?;
        }
        let args_by_hash = legacy_args(source, &hashes).await?;
        let events_by_key = legacy_events(source, &blocks, &idxs).await?;
        let args: Vec<Option<String>> = hashes
            .iter()
            .map(|h| {
                args_by_hash
                    .get(h)
                    .map(|a| Json::String(a.clone()).to_string())
            })
            .collect();
        let events: Vec<Option<String>> = blocks
            .iter()
            .zip(&idxs)
            .map(|(b, i)| events_by_key.get(&(*b, *i)).map(|e| e.to_string()))
            .collect();
        sqlx::query!(
            r#"
            INSERT INTO sm.extrinsics (
                block_height, extrinsic_index, block_timestamp, hash, section, method,
                signer, success, error_msg, args, events, origin
            )
            SELECT b, i, t, h, s, m, sg, ok, e, a::jsonb, ev::jsonb, 'legacy'
            FROM UNNEST(
                $1::bigint[], $2::int[], $3::timestamptz[], $4::text[], $5::text[], $6::text[],
                $7::text[], $8::bool[], $9::text[], $10::text[], $11::text[]
            ) AS x(b, i, t, h, s, m, sg, ok, e, a, ev)
            ON CONFLICT (block_height, extrinsic_index) DO NOTHING
            "#,
            &blocks,
            &idxs,
            &tss,
            &hashes,
            &sections,
            &methods,
            &signers,
            &successes,
            &errors,
            &args as &[Option<String>],
            &events as &[Option<String>],
        )
        .execute(target)
        .await
        .context("inserting extrinsics batch")?;
        copied += n as u64;
        set_cursor(target, "extrinsics", &cursor, n as i64).await?;
        info!(copied, cursor = %cursor, "extrinsics progress");
    }
    Ok(copied)
}

/// Node: `SELECT COALESCE(data::text, '{}') FROM history_element WHERE id = $1`.
/// Only rows with data are returned; a missing row is served as `{}`.
async fn legacy_args(source: &PgPool, hashes: &[String]) -> Result<HashMap<String, String>> {
    let rows = sqlx::query(
        "SELECT id, data::text AS args FROM public.history_element \
         WHERE id = ANY($1) AND data IS NOT NULL",
    )
    .bind(hashes)
    .fetch_all(source)
    .await
    .context("reading legacy history_element args")?;
    rows.iter()
        .map(|r| {
            Ok((
                r.try_get::<String, _>("id")?,
                r.try_get::<String, _>("args")?,
            ))
        })
        .collect()
}

/// Node: the first 100 `sm.extrinsic_events` rows of (block, index) by
/// `event_index`, then `System.ExtrinsicSuccess/Failed` dropped, each as
/// `{s, m, d}` with `d = JSON.stringify(data)` (null when no data).
/// Keys with no event left are absent.
async fn legacy_events(
    source: &PgPool,
    blocks: &[i64],
    idxs: &[i32],
) -> Result<HashMap<(i64, i32), Json>> {
    let rows = sqlx::query(
        "SELECT e.block_height::bigint AS b, e.extrinsic_index::int AS i, \
                e.event_index::int AS n, e.section, e.method, e.data \
         FROM sm.extrinsic_events e \
         JOIN UNNEST($1::bigint[], $2::int[]) AS p(b, i) \
           ON p.b = e.block_height AND p.i = e.extrinsic_index \
         ORDER BY 1, 2, 3",
    )
    .bind(blocks)
    .bind(idxs)
    .fetch_all(source)
    .await
    .context("reading legacy extrinsic_events")?;
    let mut events = Vec::with_capacity(rows.len());
    for r in &rows {
        events.push(LegacyEvent {
            block: r.try_get("b")?,
            index: r.try_get("i")?,
            section: r.try_get("section")?,
            method: r.try_get("method")?,
            data: r.try_get("data")?,
        });
    }
    Ok(shape_legacy_events(events))
}

/// One `sm.extrinsic_events` row, already ordered by (block, index, event_index).
struct LegacyEvent {
    block: i64,
    index: i32,
    section: String,
    method: String,
    data: Option<Json>,
}

/// The Node's shaping of `getExtrinsicDetail`: `LIMIT 100` per extrinsic
/// first, then `System.ExtrinsicSuccess/Failed` dropped, `d` the compact
/// JSON text of `data` (null without data). Integers keep every digit:
/// the Node rounds anything above 2^53 through a JS double (a known,
/// documented deviation).
fn shape_legacy_events(rows: Vec<LegacyEvent>) -> HashMap<(i64, i32), Json> {
    let mut seen: HashMap<(i64, i32), usize> = HashMap::new();
    let mut out: HashMap<(i64, i32), Vec<Json>> = HashMap::new();
    for ev in rows {
        let key = (ev.block, ev.index);
        let n = seen.entry(key).or_insert(0);
        if *n >= LEGACY_EVENTS_LIMIT {
            continue;
        }
        *n += 1;
        if ev.section == "System"
            && (ev.method == "ExtrinsicSuccess" || ev.method == "ExtrinsicFailed")
        {
            continue;
        }
        out.entry(key).or_default().push(serde_json::json!({
            "s": ev.section,
            "m": ev.method,
            "d": ev.data.map(|d| d.to_string()),
        }));
    }
    out.into_iter().map(|(k, v)| (k, Json::Array(v))).collect()
}

/// Node's `LIMIT 100` on the events query (applied before the filter).
const LEGACY_EVENTS_LIMIT: usize = 100;

/// The Node's daily rollup (`sm.site_daily`): small, copied whole each
/// run, `ON CONFLICT DO NOTHING` (the target rolls its own days up).
async fn copy_site_daily(source: &PgPool, target: &PgPool) -> Result<u64> {
    let rows = sqlx::query(
        "SELECT day, section, pageviews, section_views, sessions, uniques, avg_session_ms \
         FROM sm.site_daily ORDER BY day, section",
    )
    .fetch_all(source)
    .await
    .context("reading legacy site_daily")?;
    let mut copied = 0u64;
    for r in &rows {
        let res = sqlx::query!(
            r#"
            INSERT INTO sm.site_daily (day, section, pageviews, section_views, sessions, uniques, avg_session_ms)
            VALUES ($1, $2, $3, $4, $5, $6, $7)
            ON CONFLICT (day, section) DO NOTHING
            "#,
            r.try_get::<NaiveDate, _>("day")?,
            r.try_get::<String, _>("section")?,
            r.try_get::<i64, _>("pageviews")?,
            r.try_get::<i64, _>("section_views")?,
            r.try_get::<i64, _>("sessions")?,
            r.try_get::<i64, _>("uniques")?,
            r.try_get::<i64, _>("avg_session_ms")?,
        )
        .execute(target)
        .await
        .context("inserting site_daily row")?;
        copied += res.rows_affected();
    }
    info!(copied, source_rows = rows.len(), "site_daily copied");
    Ok(copied)
}

/// Raw beacons (`sm.site_events`, the Node keeps 30 days): copied by
/// `id` cursor as `origin = 'legacy'`, so a final run at cutover picks
/// up only the rows added since the previous one.
async fn copy_site_events(source: &PgPool, target: &PgPool, batch: i64) -> Result<u64> {
    let sql = r#"
        SELECT id, ts, type, section, visitor, session_id, path, referrer, country, device,
               duration_ms, meta::text AS meta
        FROM sm.site_events
        WHERE id > $1
        ORDER BY id
        LIMIT $2
        "#;
    let mut copied = 0u64;
    let mut cursor: i64 = get_cursor(target, "site_events")
        .await?
        .and_then(|c| c.parse().ok())
        .unwrap_or(0);
    loop {
        let rows = sqlx::query(sql)
            .bind(cursor)
            .bind(batch)
            .fetch_all(source)
            .await
            .context("reading legacy site_events batch")?;
        if rows.is_empty() {
            break;
        }
        let n = rows.len();
        let mut tss = Vec::with_capacity(n);
        let mut kinds = Vec::with_capacity(n);
        let mut sections = Vec::with_capacity(n);
        let mut visitors = Vec::with_capacity(n);
        let mut sessions = Vec::with_capacity(n);
        let mut paths = Vec::with_capacity(n);
        let mut referrers = Vec::with_capacity(n);
        let mut countries = Vec::with_capacity(n);
        let mut devices = Vec::with_capacity(n);
        let mut durations = Vec::with_capacity(n);
        let mut metas = Vec::with_capacity(n);
        for r in &rows {
            cursor = r.try_get::<i64, _>("id")?;
            tss.push(r.try_get::<DateTime<Utc>, _>("ts")?);
            kinds.push(r.try_get::<String, _>("type")?);
            sections.push(r.try_get::<Option<String>, _>("section")?);
            visitors.push(r.try_get::<Option<String>, _>("visitor")?);
            sessions.push(r.try_get::<Option<String>, _>("session_id")?);
            paths.push(r.try_get::<Option<String>, _>("path")?);
            referrers.push(r.try_get::<Option<String>, _>("referrer")?);
            countries.push(r.try_get::<Option<String>, _>("country")?);
            devices.push(r.try_get::<Option<String>, _>("device")?);
            durations.push(r.try_get::<Option<i64>, _>("duration_ms")?);
            metas.push(r.try_get::<Option<String>, _>("meta")?);
        }
        sqlx::query!(
            r#"
            INSERT INTO sm.site_events
                (ts, type, section, visitor, session_id, path, referrer, country, device, duration_ms, meta, origin)
            SELECT t, k, s, v, sid, p, r, c, d, dur, m::jsonb, 'legacy'
            FROM UNNEST($1::timestamptz[], $2::text[], $3::text[], $4::text[], $5::text[], $6::text[],
                        $7::text[], $8::text[], $9::text[], $10::bigint[], $11::text[])
                 AS x(t, k, s, v, sid, p, r, c, d, dur, m)
            "#,
            &tss,
            &kinds,
            &sections as &[Option<String>],
            &visitors as &[Option<String>],
            &sessions as &[Option<String>],
            &paths as &[Option<String>],
            &referrers as &[Option<String>],
            &countries as &[Option<String>],
            &devices as &[Option<String>],
            &durations as &[Option<i64>],
            &metas as &[Option<String>],
        )
        .execute(target)
        .await
        .context("inserting site_events batch")?;
        copied += n as u64;
        set_cursor(target, "site_events", &cursor.to_string(), n as i64).await?;
        info!(copied, cursor, "site_events progress");
    }
    Ok(copied)
}

/// Legacy `event_type` (call-based) → live vocabulary. `CancelBatch`
/// is a batch of cancellations: one legacy row, `canceled`.
fn legacy_order_event_type(raw: &str) -> Option<&'static str> {
    match raw {
        "Place" => Some("placed"),
        "Cancel" | "CancelBatch" => Some("canceled"),
        _ => None,
    }
}

fn legacy_order_side(raw: &str) -> Option<&'static str> {
    match raw {
        "Buy" => Some("buy"),
        "Sell" => Some("sell"),
        _ => None,
    }
}

/// Rows of `sm.mv_order_book_events` → `sm.order_book_events`. Symbols
/// resolve through the target registry (whitelist first, lowest id);
/// cancel rows legitimately carry no pair and are copied with NULL
/// assets. Rows with an unknown event type, an unresolvable symbol, or
/// unparsable price/amount are SKIPPED and counted — the reconciliation
/// checks `source = copied + skipped`.
async fn copy_order_book(source: &PgPool, target: &PgPool, batch: i64) -> Result<u64> {
    let symbols = target_symbol_map(target).await?;
    let sql = r#"
        SELECT o._row_id,
               o.block::bigint AS block_height,
               to_timestamp(o.timestamp / 1000.0) AS block_timestamp,
               o.event_type, o.wallet, o.order_id, o.base_asset, o.quote_asset,
               o.side, o.price, o.amount,
               o.usd_value::numeric(38,6) AS usd_value,
               o.hash, o.extrinsic_id
        FROM sm.mv_order_book_events o
        WHERE o._row_id > $1 AND o.wallet IS NOT NULL
        ORDER BY o._row_id
        LIMIT $2
        "#;
    let mut copied = 0u64;
    let mut skipped = 0u64;
    let mut cursor = get_cursor(target, "order_book").await?.unwrap_or_default();
    loop {
        let rows = sqlx::query(sql)
            .bind(&cursor)
            .bind(batch)
            .fetch_all(source)
            .await
            .context("reading legacy mv_order_book_events batch")?;
        if rows.is_empty() {
            break;
        }
        let n = rows.len();
        let mut blocks = Vec::with_capacity(n);
        let mut ext_ids = Vec::with_capacity(n);
        let mut tss = Vec::with_capacity(n);
        let mut kinds = Vec::with_capacity(n);
        let mut wallets = Vec::with_capacity(n);
        let mut order_ids: Vec<Option<String>> = Vec::with_capacity(n);
        let mut bases: Vec<Option<String>> = Vec::with_capacity(n);
        let mut quotes: Vec<Option<String>> = Vec::with_capacity(n);
        let mut sides: Vec<Option<String>> = Vec::with_capacity(n);
        let mut prices: Vec<Option<BigDecimal>> = Vec::with_capacity(n);
        let mut amounts: Vec<Option<BigDecimal>> = Vec::with_capacity(n);
        let mut usds: Vec<Option<BigDecimal>> = Vec::with_capacity(n);
        let mut hashes: Vec<Option<String>> = Vec::with_capacity(n);
        for r in &rows {
            cursor = r.try_get::<String, _>("_row_id")?;
            let raw_kind: Option<String> = r.try_get("event_type")?;
            let Some(kind) = raw_kind.as_deref().and_then(legacy_order_event_type) else {
                skipped += 1;
                continue;
            };
            let base_sym: Option<String> = r.try_get("base_asset")?;
            let quote_sym: Option<String> = r.try_get("quote_asset")?;
            let resolve = |sym: &Option<String>| match sym.as_deref().filter(|s| !s.is_empty()) {
                None => Some(None),
                Some(s) => symbols.get(s).map(|(id, _)| Some(id.clone())),
            };
            let (Some(base_id), Some(quote_id)) = (resolve(&base_sym), resolve(&quote_sym)) else {
                skipped += 1;
                continue;
            };
            let side: Option<String> = r.try_get("side")?;
            let side = side.as_deref().and_then(legacy_order_side);
            // Only rows with a side carry price / amount; the MV pads the
            // rest with '0'.
            let (price, amount) = if side.is_some() {
                let p: Option<String> = r.try_get("price")?;
                let a: Option<String> = r.try_get("amount")?;
                let parsed = (
                    p.as_deref()
                        .and_then(|v| v.trim().parse::<BigDecimal>().ok()),
                    a.as_deref()
                        .and_then(|v| v.trim().parse::<BigDecimal>().ok()),
                );
                match parsed {
                    (Some(p), Some(a)) => (Some(p), Some(a)),
                    _ => {
                        skipped += 1;
                        continue;
                    }
                }
            } else {
                (None, None)
            };
            blocks.push(r.try_get::<i64, _>("block_height")?);
            ext_ids.push(r.try_get::<String, _>("extrinsic_id")?);
            tss.push(r.try_get::<DateTime<Utc>, _>("block_timestamp")?);
            kinds.push(kind.to_string());
            wallets.push(r.try_get::<String, _>("wallet")?);
            let order_id: Option<String> = r.try_get("order_id")?;
            order_ids.push(order_id.filter(|o| !o.is_empty()));
            bases.push(base_id);
            quotes.push(quote_id);
            sides.push(side.map(str::to_string));
            prices.push(price);
            amounts.push(amount);
            usds.push(r.try_get::<Option<BigDecimal>, _>("usd_value")?);
            hashes.push(r.try_get::<Option<String>, _>("hash")?);
        }
        if !blocks.is_empty() {
            sqlx::query!(
                r#"
                INSERT INTO sm.order_book_events (
                    block_height, extrinsic_id, event_id, block_timestamp, event_type, wallet,
                    order_id, base_asset_id, quote_asset_id, side, price, amount, usd_value,
                    hash, origin
                )
                SELECT b, e, 0, t, k, w, o, ba, qa, s, p, a, u, h, 'legacy'
                FROM UNNEST(
                    $1::bigint[], $2::text[], $3::timestamptz[], $4::text[], $5::text[],
                    $6::text[], $7::text[], $8::text[], $9::text[], $10::numeric[],
                    $11::numeric[], $12::numeric[], $13::text[]
                ) AS x(b, e, t, k, w, o, ba, qa, s, p, a, u, h)
                ON CONFLICT (block_height, extrinsic_id, event_id) DO NOTHING
                "#,
                &blocks,
                &ext_ids,
                &tss,
                &kinds,
                &wallets,
                &order_ids as &[Option<String>],
                &bases as &[Option<String>],
                &quotes as &[Option<String>],
                &sides as &[Option<String>],
                &prices as &[Option<BigDecimal>],
                &amounts as &[Option<BigDecimal>],
                &usds as &[Option<BigDecimal>],
                &hashes as &[Option<String>],
            )
            .execute(target)
            .await
            .context("inserting order book batch")?;
        }
        copied += blocks.len() as u64;
        set_cursor(target, "order_book", &cursor, blocks.len() as i64).await?;
        info!(copied, skipped, cursor = %cursor, "order_book progress");
    }
    if skipped > 0 {
        warn!(
            skipped,
            "order book rows skipped (unknown event type / unresolvable symbol / unparsable price) — review before cutover"
        );
    }
    set_skipped(target, "order_book", skipped).await?;
    Ok(copied)
}

/// Rows of the Node's live `sm.val_staking_rewards` → v33's table,
/// verbatim (same natural key); keyset on the legacy serial `id`.
/// The Node's `ts` (insert time) becomes `block_timestamp`.
async fn copy_val_staking_rewards(source: &PgPool, target: &PgPool, batch: i64) -> Result<u64> {
    let sql = r#"
        SELECT id, era, page, validator_stash, destination, amount,
               block_num::bigint AS block_height, block_hash, ts
        FROM sm.val_staking_rewards
        WHERE id > $1
        ORDER BY id
        LIMIT $2
        "#;
    let mut copied = 0u64;
    let mut cursor: i64 = get_cursor(target, "val_staking_rewards")
        .await?
        .and_then(|c| c.parse().ok())
        .unwrap_or(-1);
    loop {
        let rows = sqlx::query(sql)
            .bind(cursor)
            .bind(batch)
            .fetch_all(source)
            .await
            .context("reading legacy val_staking_rewards batch")?;
        if rows.is_empty() {
            break;
        }
        let n = rows.len();
        let mut eras = Vec::with_capacity(n);
        let mut pages = Vec::with_capacity(n);
        let mut stashes = Vec::with_capacity(n);
        let mut dests = Vec::with_capacity(n);
        let mut amounts = Vec::with_capacity(n);
        let mut blocks = Vec::with_capacity(n);
        let mut hashes: Vec<Option<String>> = Vec::with_capacity(n);
        let mut tss = Vec::with_capacity(n);
        for r in &rows {
            cursor = r.try_get::<i64, _>("id")?;
            eras.push(r.try_get::<i32, _>("era")?);
            pages.push(r.try_get::<i32, _>("page")?);
            stashes.push(r.try_get::<String, _>("validator_stash")?);
            dests.push(r.try_get::<String, _>("destination")?);
            amounts.push(r.try_get::<BigDecimal, _>("amount")?);
            blocks.push(r.try_get::<i64, _>("block_height")?);
            hashes.push(r.try_get::<Option<String>, _>("block_hash")?);
            tss.push(r.try_get::<DateTime<Utc>, _>("ts")?);
        }
        sqlx::query!(
            r#"
            INSERT INTO sm.val_staking_rewards (
                era, page, validator_stash, destination, amount, block_height, block_hash,
                block_timestamp, origin
            )
            SELECT e, p, s, d, a, b, h, t, 'legacy'
            FROM UNNEST(
                $1::int[], $2::int[], $3::text[], $4::text[], $5::numeric[], $6::bigint[],
                $7::text[], $8::timestamptz[]
            ) AS x(e, p, s, d, a, b, h, t)
            ON CONFLICT (era, page, validator_stash, destination, block_height) DO NOTHING
            "#,
            &eras,
            &pages,
            &stashes,
            &dests,
            &amounts,
            &blocks,
            &hashes as &[Option<String>],
            &tss,
        )
        .execute(target)
        .await
        .context("inserting val_staking_rewards batch")?;
        copied += n as u64;
        set_cursor(target, "val_staking_rewards", &cursor.to_string(), n as i64).await?;
        info!(copied, cursor, "val_staking_rewards progress");
    }
    Ok(copied)
}

/// The Node's `sm.supply_snapshots` (MOF circulating supply every 30
/// min; `timestamp` in ms) → v33's table, keyset on the serial `id`.
/// Duplicate `(symbol, ts)` pairs collapse into one row.
async fn copy_supply_snapshots(source: &PgPool, target: &PgPool, batch: i64) -> Result<u64> {
    let sql = r#"
        SELECT id, symbol, asset_id, total_supply::float8 AS total_supply,
               to_timestamp(timestamp / 1000.0) AS ts
        FROM sm.supply_snapshots
        WHERE id > $1 AND symbol IS NOT NULL AND total_supply IS NOT NULL AND timestamp IS NOT NULL
        ORDER BY id
        LIMIT $2
        "#;
    let mut copied = 0u64;
    let mut cursor: i64 = get_cursor(target, "supply_snapshots")
        .await?
        .and_then(|c| c.parse().ok())
        .unwrap_or(-1);
    loop {
        let rows = sqlx::query(sql)
            .bind(cursor)
            .bind(batch)
            .fetch_all(source)
            .await
            .context("reading legacy supply_snapshots batch")?;
        if rows.is_empty() {
            break;
        }
        let n = rows.len();
        let mut symbols = Vec::with_capacity(n);
        let mut assets: Vec<Option<String>> = Vec::with_capacity(n);
        let mut supplies = Vec::with_capacity(n);
        let mut tss = Vec::with_capacity(n);
        for r in &rows {
            cursor = r.try_get::<i64, _>("id")?;
            symbols.push(r.try_get::<String, _>("symbol")?);
            assets.push(r.try_get::<Option<String>, _>("asset_id")?);
            supplies.push(r.try_get::<f64, _>("total_supply")?);
            tss.push(r.try_get::<DateTime<Utc>, _>("ts")?);
        }
        sqlx::query!(
            r#"
            INSERT INTO sm.supply_snapshots (symbol, ts, asset_id, total_supply, origin)
            SELECT s, t, a, v, 'legacy'
            FROM UNNEST($1::text[], $2::timestamptz[], $3::text[], $4::float8[]) AS x(s, t, a, v)
            ON CONFLICT (symbol, ts) DO NOTHING
            "#,
            &symbols,
            &tss,
            &assets as &[Option<String>],
            &supplies,
        )
        .execute(target)
        .await
        .context("inserting supply_snapshots batch")?;
        copied += n as u64;
        set_cursor(target, "supply_snapshots", &cursor.to_string(), n as i64).await?;
        info!(copied, cursor, "supply_snapshots progress");
    }
    Ok(copied)
}

/// Daily on-chain issuance points → `sm.supply_history`: the subsquid
/// `asset_snapshot` (type DAY, `supply / 1e18`, symbol through the
/// target registry) and the Node's `sm.supply_history` backfill
/// (`total_issuance`), each tagged with its source. Both are copied in
/// full in `(symbol, ts)` order with a composite cursor.
async fn copy_supply_history(source: &PgPool, target: &PgPool, batch: i64) -> Result<u64> {
    let symbols = target_symbol_map(target).await?;
    let by_id: HashMap<String, String> = symbols
        .iter()
        .map(|(sym, (id, _))| (id.clone(), sym.clone()))
        .collect();
    let mut copied = 0u64;

    // Source 1: sm.supply_history (symbol, timestamp secs, total_issuance).
    let sql = r#"
        SELECT symbol, timestamp::bigint AS ts_secs, total_issuance::float8 AS total_supply
        FROM sm.supply_history
        WHERE (symbol, timestamp::bigint) > ($1, $2) AND total_issuance IS NOT NULL
        ORDER BY symbol, timestamp
        LIMIT $3
        "#;
    let mut cursor = get_cursor(target, "supply_history")
        .await?
        .and_then(|c| {
            c.split_once('|')
                .map(|(s, t)| (s.to_string(), t.parse::<i64>().unwrap_or(-1)))
        })
        .unwrap_or((String::new(), -1));
    loop {
        let rows = sqlx::query(sql)
            .bind(&cursor.0)
            .bind(cursor.1)
            .bind(batch)
            .fetch_all(source)
            .await
            .context("reading legacy supply_history batch")?;
        if rows.is_empty() {
            break;
        }
        let n = rows.len();
        let mut syms = Vec::with_capacity(n);
        let mut tss = Vec::with_capacity(n);
        let mut vals = Vec::with_capacity(n);
        for r in &rows {
            let sym: String = r.try_get("symbol")?;
            let ts: i64 = r.try_get("ts_secs")?;
            cursor = (sym.clone(), ts);
            syms.push(sym);
            tss.push(ts);
            vals.push(r.try_get::<f64, _>("total_supply")?);
        }
        sqlx::query!(
            r#"
            INSERT INTO sm.supply_history (symbol, ts_secs, total_supply, source)
            SELECT s, t, v, 'supply_history'
            FROM UNNEST($1::text[], $2::bigint[], $3::float8[]) AS x(s, t, v)
            ON CONFLICT (symbol, ts_secs, source) DO NOTHING
            "#,
            &syms,
            &tss,
            &vals,
        )
        .execute(target)
        .await
        .context("inserting supply_history batch")?;
        copied += n as u64;
        set_cursor(
            target,
            "supply_history",
            &format!("{}|{}", cursor.0, cursor.1),
            n as i64,
        )
        .await?;
        info!(copied, cursor = %format!("{}|{}", cursor.0, cursor.1), "supply_history progress");
    }

    // Source 2: public.asset_snapshot DAY rows (asset_id, timestamp secs, supply planck).
    let sql2 = r#"
        SELECT asset_id, timestamp::bigint AS ts_secs, (supply::numeric / 1e18)::float8 AS total_supply
        FROM asset_snapshot
        WHERE type = 'DAY' AND (asset_id, timestamp::bigint) > ($1, $2) AND supply IS NOT NULL
        ORDER BY asset_id, timestamp
        LIMIT $3
        "#;
    let mut cursor2 = get_cursor(target, "asset_snapshot")
        .await?
        .and_then(|c| {
            c.split_once('|')
                .map(|(s, t)| (s.to_string(), t.parse::<i64>().unwrap_or(-1)))
        })
        .unwrap_or((String::new(), -1));
    let mut skipped = 0u64;
    loop {
        let rows = sqlx::query(sql2)
            .bind(&cursor2.0)
            .bind(cursor2.1)
            .bind(batch)
            .fetch_all(source)
            .await
            .context("reading legacy asset_snapshot batch")?;
        if rows.is_empty() {
            break;
        }
        let n = rows.len();
        let mut syms = Vec::with_capacity(n);
        let mut tss = Vec::with_capacity(n);
        let mut vals = Vec::with_capacity(n);
        for r in &rows {
            let id: String = r.try_get("asset_id")?;
            let ts: i64 = r.try_get("ts_secs")?;
            cursor2 = (id.clone(), ts);
            let Some(sym) = by_id.get(&id) else {
                skipped += 1;
                continue;
            };
            syms.push(sym.clone());
            tss.push(ts);
            vals.push(r.try_get::<f64, _>("total_supply")?);
        }
        if !syms.is_empty() {
            sqlx::query!(
                r#"
                INSERT INTO sm.supply_history (symbol, ts_secs, total_supply, source)
                SELECT s, t, v, 'asset_snapshot'
                FROM UNNEST($1::text[], $2::bigint[], $3::float8[]) AS x(s, t, v)
                ON CONFLICT (symbol, ts_secs, source) DO NOTHING
                "#,
                &syms,
                &tss,
                &vals,
            )
            .execute(target)
            .await
            .context("inserting asset_snapshot batch")?;
        }
        copied += syms.len() as u64;
        set_cursor(
            target,
            "asset_snapshot",
            &format!("{}|{}", cursor2.0, cursor2.1),
            syms.len() as i64,
        )
        .await?;
        info!(copied, skipped, "asset_snapshot progress");
    }
    if skipped > 0 {
        warn!(
            skipped,
            "asset_snapshot rows skipped (asset id not in the target registry)"
        );
    }
    set_skipped(target, "asset_snapshot", skipped).await?;
    Ok(copied)
}

/// The Node's `sm.news_episodes` → v33's identical table, keyset on `slug`.
async fn copy_news_episodes(source: &PgPool, target: &PgPool, batch: i64) -> Result<u64> {
    let sql = r#"
        SELECT slug, published_at, title_es, title_en, summary_es, summary_en,
               cover_path, audio_path_es, audio_path_en, video_path_es, video_path_en,
               duration_s, source_url, tags
        FROM sm.news_episodes
        WHERE slug > $1
        ORDER BY slug
        LIMIT $2
        "#;
    let mut copied = 0u64;
    let mut cursor = get_cursor(target, "news_episodes")
        .await?
        .unwrap_or_default();
    loop {
        let rows = sqlx::query(sql)
            .bind(&cursor)
            .bind(batch)
            .fetch_all(source)
            .await
            .context("reading legacy news_episodes batch")?;
        if rows.is_empty() {
            break;
        }
        for r in &rows {
            cursor = r.try_get::<String, _>("slug")?;
            let tags: Option<Vec<String>> = r.try_get("tags")?;
            sqlx::query!(
                r#"
                INSERT INTO sm.news_episodes (
                    slug, published_at, title_es, title_en, summary_es, summary_en,
                    cover_path, audio_path_es, audio_path_en, video_path_es, video_path_en,
                    duration_s, source_url, tags
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
                ON CONFLICT (slug) DO NOTHING
                "#,
                cursor,
                r.try_get::<DateTime<Utc>, _>("published_at")?,
                r.try_get::<String, _>("title_es")?,
                r.try_get::<String, _>("title_en")?,
                r.try_get::<Option<String>, _>("summary_es")?,
                r.try_get::<Option<String>, _>("summary_en")?,
                r.try_get::<String, _>("cover_path")?,
                r.try_get::<String, _>("audio_path_es")?,
                r.try_get::<String, _>("audio_path_en")?,
                r.try_get::<Option<String>, _>("video_path_es")?,
                r.try_get::<Option<String>, _>("video_path_en")?,
                r.try_get::<Option<i32>, _>("duration_s")?,
                r.try_get::<Option<String>, _>("source_url")?,
                tags.as_deref(),
            )
            .execute(target)
            .await
            .context("inserting news episode")?;
        }
        copied += rows.len() as u64;
        set_cursor(target, "news_episodes", &cursor, rows.len() as i64).await?;
        info!(copied, cursor = %cursor, "news_episodes progress");
    }
    Ok(copied)
}

// =============================================================
// polkamarkt_* (the Node's live tables, verbatim; keyset on their ids)
// =============================================================

async fn copy_pm_markets(source: &PgPool, target: &PgPool, batch: i64) -> Result<u64> {
    let sql = r#"
        SELECT market_id, condition_id, creator, close_block, collateral_asset, seed_liquidity,
               status, resolution, question, oracle, resolution_source,
               opengov_network, opengov_parachain, opengov_track, opengov_referendum,
               created_at_block, created_at_ts, resolved_at_block, resolved_at_ts, mechanism
        FROM sm.polkamarkt_markets
        WHERE market_id > $1
        ORDER BY market_id
        LIMIT $2
        "#;
    let mut copied = 0u64;
    let mut cursor: i64 = get_cursor(target, "polkamarkt_markets")
        .await?
        .and_then(|c| c.parse().ok())
        .unwrap_or(-1);
    loop {
        let rows = sqlx::query(sql)
            .bind(cursor)
            .bind(batch)
            .fetch_all(source)
            .await
            .context("reading legacy polkamarkt_markets batch")?;
        if rows.is_empty() {
            break;
        }
        for r in &rows {
            cursor = r.try_get::<i64, _>("market_id")?;
            sqlx::query!(
                r#"
                INSERT INTO sm.polkamarkt_markets (
                    market_id, condition_id, creator, close_block, collateral_asset, seed_liquidity,
                    status, resolution, question, oracle, resolution_source,
                    opengov_network, opengov_parachain, opengov_track, opengov_referendum,
                    created_at_block, created_at_ts, resolved_at_block, resolved_at_ts, mechanism, origin
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, 'legacy')
                ON CONFLICT (market_id) DO NOTHING
                "#,
                cursor,
                r.try_get::<i64, _>("condition_id")?,
                r.try_get::<String, _>("creator")?,
                r.try_get::<i32, _>("close_block")?,
                r.try_get::<String, _>("collateral_asset")?,
                r.try_get::<BigDecimal, _>("seed_liquidity")?,
                r.try_get::<String, _>("status")?,
                r.try_get::<Option<String>, _>("resolution")?,
                r.try_get::<Option<String>, _>("question")?,
                r.try_get::<Option<String>, _>("oracle")?,
                r.try_get::<Option<String>, _>("resolution_source")?,
                r.try_get::<Option<String>, _>("opengov_network")?,
                r.try_get::<Option<i32>, _>("opengov_parachain")?,
                r.try_get::<Option<i32>, _>("opengov_track")?,
                r.try_get::<Option<i32>, _>("opengov_referendum")?,
                r.try_get::<i32, _>("created_at_block")?,
                r.try_get::<i64, _>("created_at_ts")?,
                r.try_get::<Option<i32>, _>("resolved_at_block")?,
                r.try_get::<Option<i64>, _>("resolved_at_ts")?,
                r.try_get::<Option<String>, _>("mechanism")?,
            )
            .execute(target)
            .await
            .context("inserting polkamarkt market")?;
        }
        copied += rows.len() as u64;
        set_cursor(
            target,
            "polkamarkt_markets",
            &cursor.to_string(),
            rows.len() as i64,
        )
        .await?;
        info!(copied, cursor, "polkamarkt_markets progress");
    }
    Ok(copied)
}

async fn copy_pm_trades(source: &PgPool, target: &PgPool, batch: i64) -> Result<u64> {
    let sql = r#"
        SELECT id, market_id, trader, side, outcome, collateral, shares, fee, block, ts, hash
        FROM sm.polkamarkt_trades WHERE id > $1 ORDER BY id LIMIT $2
        "#;
    let mut copied = 0u64;
    let mut cursor: i64 = get_cursor(target, "polkamarkt_trades")
        .await?
        .and_then(|c| c.parse().ok())
        .unwrap_or(-1);
    loop {
        let rows = sqlx::query(sql)
            .bind(cursor)
            .bind(batch)
            .fetch_all(source)
            .await
            .context("reading legacy polkamarkt_trades batch")?;
        if rows.is_empty() {
            break;
        }
        for r in &rows {
            cursor = r.try_get::<i64, _>("id")?;
            sqlx::query!(
                r#"
                INSERT INTO sm.polkamarkt_trades
                    (market_id, trader, side, outcome, collateral, shares, fee, block, ts, hash, legacy_id, origin)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, 'legacy')
                ON CONFLICT (legacy_id) DO NOTHING
                "#,
                r.try_get::<i64, _>("market_id")?,
                r.try_get::<String, _>("trader")?,
                r.try_get::<String, _>("side")?,
                r.try_get::<String, _>("outcome")?,
                r.try_get::<BigDecimal, _>("collateral")?,
                r.try_get::<BigDecimal, _>("shares")?,
                r.try_get::<BigDecimal, _>("fee")?,
                r.try_get::<i32, _>("block")?,
                r.try_get::<i64, _>("ts")?,
                r.try_get::<Option<String>, _>("hash")?,
                cursor,
            )
            .execute(target)
            .await
            .context("inserting polkamarkt trade")?;
        }
        copied += rows.len() as u64;
        set_cursor(
            target,
            "polkamarkt_trades",
            &cursor.to_string(),
            rows.len() as i64,
        )
        .await?;
        info!(copied, cursor, "polkamarkt_trades progress");
    }
    Ok(copied)
}

async fn copy_pm_claims(source: &PgPool, target: &PgPool, batch: i64) -> Result<u64> {
    let sql = r#"
        SELECT id, market_id, account, kind, amount, block, ts
        FROM sm.polkamarkt_claims WHERE id > $1 ORDER BY id LIMIT $2
        "#;
    let mut copied = 0u64;
    let mut cursor: i64 = get_cursor(target, "polkamarkt_claims")
        .await?
        .and_then(|c| c.parse().ok())
        .unwrap_or(-1);
    loop {
        let rows = sqlx::query(sql)
            .bind(cursor)
            .bind(batch)
            .fetch_all(source)
            .await
            .context("reading legacy polkamarkt_claims batch")?;
        if rows.is_empty() {
            break;
        }
        for r in &rows {
            cursor = r.try_get::<i64, _>("id")?;
            sqlx::query!(
                r#"
                INSERT INTO sm.polkamarkt_claims (market_id, account, kind, amount, block, ts, legacy_id, origin)
                VALUES ($1, $2, $3, $4, $5, $6, $7, 'legacy')
                ON CONFLICT (legacy_id) DO NOTHING
                "#,
                r.try_get::<i64, _>("market_id")?,
                r.try_get::<String, _>("account")?,
                r.try_get::<String, _>("kind")?,
                r.try_get::<BigDecimal, _>("amount")?,
                r.try_get::<i32, _>("block")?,
                r.try_get::<i64, _>("ts")?,
                cursor,
            )
            .execute(target)
            .await
            .context("inserting polkamarkt claim")?;
        }
        copied += rows.len() as u64;
        set_cursor(
            target,
            "polkamarkt_claims",
            &cursor.to_string(),
            rows.len() as i64,
        )
        .await?;
        info!(copied, cursor, "polkamarkt_claims progress");
    }
    Ok(copied)
}

async fn copy_pm_buybacks(source: &PgPool, target: &PgPool, batch: i64) -> Result<u64> {
    let sql = r#"
        SELECT id, block, ts, hash, kusd_spent, xor_burned
        FROM sm.polkamarkt_buybacks WHERE id > $1 ORDER BY id LIMIT $2
        "#;
    let mut copied = 0u64;
    let mut cursor: i64 = get_cursor(target, "polkamarkt_buybacks")
        .await?
        .and_then(|c| c.parse().ok())
        .unwrap_or(-1);
    loop {
        let rows = sqlx::query(sql)
            .bind(cursor)
            .bind(batch)
            .fetch_all(source)
            .await
            .context("reading legacy polkamarkt_buybacks batch")?;
        if rows.is_empty() {
            break;
        }
        for r in &rows {
            cursor = r.try_get::<i64, _>("id")?;
            sqlx::query!(
                r#"
                INSERT INTO sm.polkamarkt_buybacks (block, ts, hash, kusd_spent, xor_burned, legacy_id, origin)
                VALUES ($1, $2, $3, $4, $5, $6, 'legacy')
                ON CONFLICT (legacy_id) DO NOTHING
                "#,
                r.try_get::<i32, _>("block")?,
                r.try_get::<i64, _>("ts")?,
                r.try_get::<Option<String>, _>("hash")?,
                r.try_get::<BigDecimal, _>("kusd_spent")?,
                r.try_get::<BigDecimal, _>("xor_burned")?,
                cursor,
            )
            .execute(target)
            .await
            .context("inserting polkamarkt buyback")?;
        }
        copied += rows.len() as u64;
        set_cursor(
            target,
            "polkamarkt_buybacks",
            &cursor.to_string(),
            rows.len() as i64,
        )
        .await?;
        info!(copied, cursor, "polkamarkt_buybacks progress");
    }
    Ok(copied)
}

async fn copy_pm_burns(source: &PgPool, target: &PgPool, batch: i64) -> Result<u64> {
    let sql = r#"
        SELECT id, block, ts, hash, market_id, kind, amount
        FROM sm.polkamarkt_burns WHERE id > $1 ORDER BY id LIMIT $2
        "#;
    let mut copied = 0u64;
    let mut cursor: i64 = get_cursor(target, "polkamarkt_burns")
        .await?
        .and_then(|c| c.parse().ok())
        .unwrap_or(-1);
    loop {
        let rows = sqlx::query(sql)
            .bind(cursor)
            .bind(batch)
            .fetch_all(source)
            .await
            .context("reading legacy polkamarkt_burns batch")?;
        if rows.is_empty() {
            break;
        }
        for r in &rows {
            cursor = r.try_get::<i64, _>("id")?;
            sqlx::query!(
                r#"
                INSERT INTO sm.polkamarkt_burns (block, ts, hash, market_id, kind, amount, legacy_id, origin)
                VALUES ($1, $2, $3, $4, $5, $6, $7, 'legacy')
                ON CONFLICT (legacy_id) DO NOTHING
                "#,
                r.try_get::<i32, _>("block")?,
                r.try_get::<i64, _>("ts")?,
                r.try_get::<Option<String>, _>("hash")?,
                r.try_get::<Option<i64>, _>("market_id")?,
                r.try_get::<String, _>("kind")?,
                r.try_get::<BigDecimal, _>("amount")?,
                cursor,
            )
            .execute(target)
            .await
            .context("inserting polkamarkt burn")?;
        }
        copied += rows.len() as u64;
        set_cursor(
            target,
            "polkamarkt_burns",
            &cursor.to_string(),
            rows.len() as i64,
        )
        .await?;
        info!(copied, cursor, "polkamarkt_burns progress");
    }
    Ok(copied)
}

/// Exact id-set comparison for a small serial-keyed legacy table.
async fn reconcile_ids(
    source: &PgPool,
    table: &str,
    source_sql: &str,
    target_ids: Vec<i64>,
) -> Result<bool> {
    let src: Vec<i64> = sqlx::query(source_sql)
        .fetch_all(source)
        .await?
        .iter()
        .map(|r| r.try_get::<i64, _>("id"))
        .collect::<Result<_, _>>()?;
    let missing: Vec<i64> = src
        .iter()
        .filter(|i| !target_ids.contains(i))
        .copied()
        .collect();
    if missing.is_empty() {
        Ok(true)
    } else {
        warn!(table, ?missing, "RECONCILE FAIL: ids missing in target");
        Ok(false)
    }
}

/// Accumulate the skipped count so reconciliation can check
/// `source = copied + skipped` across resumed runs (each run only sees
/// the rows past its cursor). A cursor reset implies truncating the
/// target table AND this row.
async fn set_skipped(target: &PgPool, table: &str, skipped: u64) -> Result<()> {
    sqlx::query!(
        r#"INSERT INTO sm.etl_skipped (table_name, skipped) VALUES ($1, $2)
           ON CONFLICT (table_name) DO UPDATE
               SET skipped = sm.etl_skipped.skipped + EXCLUDED.skipped"#,
        table,
        skipped as i64,
    )
    .execute(target)
    .await?;
    Ok(())
}

async fn copy_price_history(source: &PgPool, target: &PgPool, batch: i64) -> Result<u64> {
    let sql = r#"
        SELECT asset_id, hour_bucket::bigint AS hour_bucket,
               price_usd::double precision AS price_usd,
               COALESCE(sample_count, 0)::int AS sample_count
        FROM sm.price_history
        WHERE (asset_id, hour_bucket) > ($1, $2)
        ORDER BY asset_id, hour_bucket
        LIMIT $3
        "#;

    let mut copied = 0u64;
    let (mut cur_asset, mut cur_bucket) = get_cursor(target, "price_history")
        .await?
        .and_then(|c| {
            c.split_once('|')
                .map(|(a, b)| (a.to_string(), b.parse::<i64>().unwrap_or(-1)))
        })
        .unwrap_or((String::new(), -1));

    loop {
        let rows = sqlx::query(sql)
            .bind(&cur_asset)
            .bind(cur_bucket)
            .bind(batch)
            .fetch_all(source)
            .await
            .context("reading legacy price_history batch")?;
        if rows.is_empty() {
            break;
        }

        let n = rows.len();
        let mut assets = Vec::with_capacity(n);
        let mut buckets = Vec::with_capacity(n);
        let mut prices = Vec::with_capacity(n);
        let mut samples = Vec::with_capacity(n);

        for r in &rows {
            assets.push(r.try_get::<String, _>("asset_id")?);
            buckets.push(r.try_get::<i64, _>("hour_bucket")?);
            prices.push(r.try_get::<f64, _>("price_usd")?);
            samples.push(r.try_get::<i32, _>("sample_count")?);
        }
        let (Some(last_asset), Some(last_bucket)) = (assets.last(), buckets.last()) else {
            break;
        };
        cur_asset = last_asset.clone();
        cur_bucket = *last_bucket;

        sqlx::query!(
            r#"
            INSERT INTO ts.price_history (asset_id, hour_bucket, price_usd, sample_count, origin)
            SELECT a, b, p, s, 'legacy'
            FROM UNNEST($1::text[], $2::bigint[], $3::float8[], $4::int[]) AS x(a, b, p, s)
            ON CONFLICT (asset_id, hour_bucket) DO NOTHING
            "#,
            &assets,
            &buckets,
            &prices,
            &samples,
        )
        .execute(target)
        .await
        .context("inserting price_history batch")?;

        copied += n as u64;
        set_cursor(
            target,
            "price_history",
            &format!("{cur_asset}|{cur_bucket}"),
            n as i64,
        )
        .await?;
        info!(copied, asset = %cur_asset, bucket = cur_bucket, "price_history progress");
    }
    Ok(copied)
}

// =============================================================
// Reconciliation — MANDATORY project step. Counts + exact sums per
// block bucket (100K blocks) on both sides; any mismatch fails the run.
// =============================================================

/// Counts and reports source rows excluded by a copy filter (NULL
/// required fields). Skipped rows are never silent: they surface here on
/// every reconciliation, so the operator decides before cutover.
async fn report_skipped(source: &PgPool, table: &str, from: &str, filter: &str) -> Result<()> {
    let skipped: i64 = sqlx::query(&format!(
        "SELECT COUNT(*)::bigint AS c FROM {from} WHERE NOT ({filter})"
    ))
    .fetch_one(source)
    .await?
    .try_get("c")?;
    if skipped > 0 {
        warn!(
            table,
            skipped, "source rows skipped (NULL fields) — review before cutover"
        );
    }
    Ok(())
}

/// One reconciliation bucket: `(bucket, count, checksum)`.
type Buckets = Vec<(i64, i64, BigDecimal)>;

async fn source_buckets(source: &PgPool, sql: &str) -> Result<Buckets> {
    let rows = sqlx::query(sql).fetch_all(source).await?;
    rows.iter()
        .map(|r| {
            Ok((
                r.try_get::<i64, _>("bucket")?,
                r.try_get::<i64, _>("cnt")?,
                r.try_get::<BigDecimal, _>("checksum")?,
            ))
        })
        .collect()
}

fn compare_buckets(table: &str, src: &Buckets, dst: &Buckets) -> bool {
    // STRICT equality per source bucket. Target buckets are filtered to
    // `origin = 'legacy'` rows, so live ingest can never mask a
    // mismatch (the earlier ≥-count escape hatch was shown to swallow
    // real corruption when live rows shared a bucket).
    let mut ok = true;
    for (bucket, s_cnt, s_sum) in src {
        match dst.iter().find(|(b, _, _)| b == bucket) {
            None => {
                warn!(table, bucket, "RECONCILE FAIL: bucket missing in target");
                ok = false;
            }
            Some((_, d_cnt, d_sum)) => {
                if d_cnt != s_cnt || d_sum != s_sum {
                    warn!(
                        table,
                        bucket,
                        source_count = s_cnt,
                        target_count = d_cnt,
                        source_sum = %s_sum,
                        target_sum = %d_sum,
                        "RECONCILE FAIL: bucket mismatch"
                    );
                    ok = false;
                }
            }
        }
    }
    ok
}

async fn reconcile_table(source: &PgPool, target: &PgPool, table: &str) -> Result<bool> {
    if table.starts_with("mn_") {
        return crate::etl_mn::reconcile(source, target, table).await;
    }
    let ok = match table {
        "swaps" => {
            let src = source_buckets(
                source,
                &format!(
                    "SELECT (s.block / 100000)::bigint AS bucket, COUNT(*)::bigint AS cnt, \
                     COALESCE(SUM((s.in_amount::numeric * {sc})::numeric(78,0)), 0)::numeric AS checksum \
                     FROM sm.mv_swaps s \
                     LEFT JOIN sm.asset_registry ar_in ON ar_in.asset_id = s.in_asset_id \
                     WHERE {SWAPS_FILTER} GROUP BY 1 ORDER BY 1",
                    sc = scale_expr("ar_in"),
                ),
            )
            .await?;
            report_skipped(source, table, "sm.mv_swaps s", SWAPS_FILTER).await?;
            let dst_rows = sqlx::query!(
                r#"SELECT (block_height / 100000) AS "bucket!", COUNT(*)::bigint AS "cnt!",
                   COALESCE(SUM(input_amount), 0)::numeric AS "checksum!"
                   FROM sm.swaps WHERE origin = 'legacy' GROUP BY 1 ORDER BY 1"#
            )
            .fetch_all(target)
            .await?;
            let dst: Buckets = dst_rows
                .into_iter()
                .map(|r| (r.bucket, r.cnt, r.checksum))
                .collect();
            compare_buckets(table, &src, &dst)
        }
        "transfers" => {
            report_skipped(source, table, "sm.mv_transfers t", TRANSFERS_FILTER).await?;
            let src = source_buckets(
                source,
                &format!(
                    "SELECT (t.block / 100000)::bigint AS bucket, COUNT(*)::bigint AS cnt, \
                     COALESCE(SUM((t.amount::numeric * {sc})::numeric(78,0)), 0)::numeric AS checksum \
                     FROM sm.mv_transfers t \
                     LEFT JOIN sm.asset_registry ar ON ar.asset_id = t.asset_id \
                     WHERE {TRANSFERS_FILTER} GROUP BY 1 ORDER BY 1",
                    sc = scale_expr("ar"),
                ),
            )
            .await?;
            let dst_rows = sqlx::query!(
                r#"SELECT (block_height / 100000) AS "bucket!", COUNT(*)::bigint AS "cnt!",
                   COALESCE(SUM(amount), 0)::numeric AS "checksum!"
                   FROM sm.transfers WHERE origin = 'legacy' GROUP BY 1 ORDER BY 1"#
            )
            .fetch_all(target)
            .await?;
            let dst: Buckets = dst_rows
                .into_iter()
                .map(|r| (r.bucket, r.cnt, r.checksum))
                .collect();
            compare_buckets(table, &src, &dst)
        }
        "bridges" => {
            report_skipped(source, table, "sm.mv_bridges b", BRIDGES_FILTER).await?;
            let src = source_buckets(
                source,
                &format!(
                    "SELECT (b.block / 100000)::bigint AS bucket, COUNT(*)::bigint AS cnt, \
                     COALESCE(SUM((b.amount::numeric * {sc})::numeric(78,0)), 0)::numeric AS checksum \
                     FROM sm.mv_bridges b \
                     LEFT JOIN sm.asset_registry ar ON ar.asset_id = b.asset_id \
                     WHERE {BRIDGES_FILTER} GROUP BY 1 ORDER BY 1",
                    sc = scale_expr("ar"),
                ),
            )
            .await?;
            let dst_rows = sqlx::query!(
                r#"SELECT (block_height / 100000) AS "bucket!", COUNT(*)::bigint AS "cnt!",
                   COALESCE(SUM(amount), 0)::numeric AS "checksum!"
                   FROM sm.bridges WHERE origin = 'legacy' GROUP BY 1 ORDER BY 1"#
            )
            .fetch_all(target)
            .await?;
            let dst: Buckets = dst_rows
                .into_iter()
                .map(|r| (r.bucket, r.cnt, r.checksum))
                .collect();
            compare_buckets(table, &src, &dst)
        }
        "fees" => {
            let src = source_buckets(
                source,
                "SELECT (f.block / 100000)::bigint AS bucket, COUNT(*)::bigint AS cnt, \
                 COALESCE(SUM(f.amount::numeric(38,18)), 0)::numeric AS checksum \
                 FROM sm.mv_fees f GROUP BY 1 ORDER BY 1",
            )
            .await?;
            let dst_rows = sqlx::query!(
                r#"SELECT (block_height / 100000) AS "bucket!", COUNT(*)::bigint AS "cnt!",
                   COALESCE(SUM(amount_xor), 0)::numeric AS "checksum!"
                   FROM sm.fees GROUP BY 1 ORDER BY 1"#
            )
            .fetch_all(target)
            .await?;
            let dst: Buckets = dst_rows
                .into_iter()
                .map(|r| (r.bucket, r.cnt, r.checksum))
                .collect();
            compare_buckets(table, &src, &dst)
        }
        "fee_burns" => {
            let src = source_buckets(
                source,
                "SELECT (block_height / 100000)::bigint AS bucket, COUNT(*)::bigint AS cnt, \
                 COALESCE(SUM(fees_paid_xor), 0)::numeric AS checksum \
                 FROM sm.fee_burns_live GROUP BY 1 ORDER BY 1",
            )
            .await?;
            let dst_rows = sqlx::query!(
                r#"SELECT (block_height / 100000) AS "bucket!", COUNT(*)::bigint AS "cnt!",
                   COALESCE(SUM(fees_paid_xor), 0)::numeric AS "checksum!"
                   FROM sm.fee_burns_aggregate GROUP BY 1 ORDER BY 1"#
            )
            .fetch_all(target)
            .await?;
            let dst: Buckets = dst_rows
                .into_iter()
                .map(|r| (r.bucket, r.cnt, r.checksum))
                .collect();
            compare_buckets(table, &src, &dst)
        }
        "price_history" => {
            // Buckets by month of hour_bucket; checksum = SUM(hour_bucket)
            // (exact integer — float price sums are order-dependent).
            let src = source_buckets(
                source,
                "SELECT (hour_bucket / 2592000)::bigint AS bucket, COUNT(*)::bigint AS cnt, \
                 COALESCE(SUM(hour_bucket), 0)::numeric AS checksum \
                 FROM sm.price_history GROUP BY 1 ORDER BY 1",
            )
            .await?;
            let dst_rows = sqlx::query!(
                r#"SELECT (hour_bucket / 2592000) AS "bucket!", COUNT(*)::bigint AS "cnt!",
                   COALESCE(SUM(hour_bucket), 0)::numeric AS "checksum!"
                   FROM ts.price_history WHERE origin = 'legacy' GROUP BY 1 ORDER BY 1"#
            )
            .fetch_all(target)
            .await?;
            let dst: Buckets = dst_rows
                .into_iter()
                .map(|r| (r.bucket, r.cnt, r.checksum))
                .collect();
            compare_buckets(table, &src, &dst)
        }
        "extrinsics" => {
            report_skipped(source, table, "sm.mv_extrinsics x", EXTRINSICS_FILTER).await?;
            // Checksum = SUM(block × 1000 + index): exact, order-independent.
            let src = source_buckets(
                source,
                &format!(
                    "SELECT (x.block / 100000)::bigint AS bucket, COUNT(*)::bigint AS cnt, \
                     COALESCE(SUM(x.block::numeric * 1000 + x.extrinsic_index), 0)::numeric AS checksum \
                     FROM sm.mv_extrinsics x WHERE {EXTRINSICS_FILTER} GROUP BY 1 ORDER BY 1"
                ),
            )
            .await?;
            let dst_rows = sqlx::query!(
                r#"SELECT (block_height / 100000) AS "bucket!", COUNT(*)::bigint AS "cnt!",
                   COALESCE(SUM(block_height::numeric * 1000 + extrinsic_index), 0)::numeric AS "checksum!"
                   FROM sm.extrinsics WHERE origin = 'legacy' GROUP BY 1 ORDER BY 1"#
            )
            .fetch_all(target)
            .await?;
            let dst: Buckets = dst_rows
                .into_iter()
                .map(|r| (r.bucket, r.cnt, r.checksum))
                .collect();
            // Detail: rows with events (cnt) and rows with args (checksum),
            // resolved on the source the way the copy resolves them.
            let src_detail = source_buckets(
                source,
                &format!(
                    "SELECT (x.block / 100000)::bigint AS bucket, \
                     COUNT(*) FILTER (WHERE EXISTS (SELECT 1 FROM sm.extrinsic_events e \
                        WHERE e.block_height = x.block AND e.extrinsic_index = x.extrinsic_index \
                          AND NOT (e.section = 'System' AND e.method IN ('ExtrinsicSuccess','ExtrinsicFailed'))))::bigint AS cnt, \
                     COUNT(*) FILTER (WHERE EXISTS (SELECT 1 FROM public.history_element h \
                        WHERE h.id = x.hash AND h.data IS NOT NULL))::numeric AS checksum \
                     FROM sm.mv_extrinsics x WHERE {EXTRINSICS_FILTER} GROUP BY 1 ORDER BY 1"
                ),
            )
            .await?;
            let dst_detail_rows = sqlx::query!(
                r#"SELECT (block_height / 100000) AS "bucket!",
                   COUNT(*) FILTER (WHERE events IS NOT NULL)::bigint AS "cnt!",
                   COUNT(*) FILTER (WHERE args IS NOT NULL)::numeric AS "checksum!"
                   FROM sm.extrinsics WHERE origin = 'legacy' GROUP BY 1 ORDER BY 1"#
            )
            .fetch_all(target)
            .await?;
            let dst_detail: Buckets = dst_detail_rows
                .into_iter()
                .map(|r| (r.bucket, r.cnt, r.checksum))
                .collect();
            compare_buckets(table, &src, &dst)
                && compare_buckets("extrinsics(detail)", &src_detail, &dst_detail)
        }
        "liquidity" => {
            // Symbol-keyed source: no planck checksum is computable on the
            // source side. STRICT count equation: source = copied + skipped.
            let src_cnt: i64 = sqlx::query(
                "SELECT COUNT(*)::bigint AS c FROM sm.mv_liquidity_events WHERE wallet IS NOT NULL",
            )
            .fetch_one(source)
            .await?
            .try_get("c")?;
            let dst_cnt = sqlx::query_scalar!(
                r#"SELECT COUNT(*)::bigint AS "c!" FROM sm.liquidity_events WHERE origin = 'legacy'"#
            )
            .fetch_one(target)
            .await?;
            let skipped = sqlx::query_scalar!(
                r#"SELECT skipped FROM sm.etl_skipped WHERE table_name = 'liquidity'"#
            )
            .fetch_optional(target)
            .await?
            .unwrap_or(0);
            if src_cnt == dst_cnt + skipped {
                true
            } else {
                warn!(
                    table,
                    src_cnt, dst_cnt, skipped, "RECONCILE FAIL: source ≠ copied + skipped"
                );
                false
            }
        }
        "order_book" => {
            // Symbol-keyed source, like liquidity: STRICT count equation.
            let src_cnt: i64 = sqlx::query(
                "SELECT COUNT(*)::bigint AS c FROM sm.mv_order_book_events WHERE wallet IS NOT NULL",
            )
            .fetch_one(source)
            .await?
            .try_get("c")?;
            let dst_cnt = sqlx::query_scalar!(
                r#"SELECT COUNT(*)::bigint AS "c!" FROM sm.order_book_events WHERE origin = 'legacy'"#
            )
            .fetch_one(target)
            .await?;
            let skipped = sqlx::query_scalar!(
                r#"SELECT skipped FROM sm.etl_skipped WHERE table_name = 'order_book'"#
            )
            .fetch_optional(target)
            .await?
            .unwrap_or(0);
            if src_cnt == dst_cnt + skipped {
                true
            } else {
                warn!(
                    table,
                    src_cnt, dst_cnt, skipped, "RECONCILE FAIL: source ≠ copied + skipped"
                );
                false
            }
        }
        "val_staking_rewards" => {
            // Buckets of 100 eras; checksum = SUM(amount) (exact numeric).
            let src = source_buckets(
                source,
                "SELECT (era / 100)::bigint AS bucket, COUNT(*)::bigint AS cnt, \
                 COALESCE(SUM(amount), 0)::numeric AS checksum \
                 FROM sm.val_staking_rewards GROUP BY 1 ORDER BY 1",
            )
            .await?;
            let dst_rows = sqlx::query!(
                r#"SELECT (era / 100)::bigint AS "bucket!", COUNT(*)::bigint AS "cnt!",
                   COALESCE(SUM(amount), 0)::numeric AS "checksum!"
                   FROM sm.val_staking_rewards WHERE origin = 'legacy' GROUP BY 1 ORDER BY 1"#
            )
            .fetch_all(target)
            .await?;
            let dst: Buckets = dst_rows
                .into_iter()
                .map(|r| (r.bucket, r.cnt, r.checksum))
                .collect();
            compare_buckets(table, &src, &dst)
        }
        "supply_snapshots" => {
            // Distinct (symbol, ms) pairs per month bucket; checksum = SUM(ms).
            let src = source_buckets(
                source,
                "SELECT (timestamp / 2592000000)::bigint AS bucket, COUNT(*)::bigint AS cnt, \
                 COALESCE(SUM(timestamp), 0)::numeric AS checksum FROM ( \
                   SELECT DISTINCT symbol, timestamp::bigint AS timestamp FROM sm.supply_snapshots \
                   WHERE symbol IS NOT NULL AND total_supply IS NOT NULL AND timestamp IS NOT NULL) d \
                 GROUP BY 1 ORDER BY 1",
            )
            .await?;
            let dst_rows = sqlx::query!(
                r#"SELECT ((EXTRACT(EPOCH FROM ts) * 1000)::bigint / 2592000000)::bigint AS "bucket!",
                   COUNT(*)::bigint AS "cnt!",
                   COALESCE(SUM((EXTRACT(EPOCH FROM ts) * 1000)::bigint), 0)::numeric AS "checksum!"
                   FROM sm.supply_snapshots WHERE origin = 'legacy' GROUP BY 1 ORDER BY 1"#
            )
            .fetch_all(target)
            .await?;
            let dst: Buckets = dst_rows
                .into_iter()
                .map(|r| (r.bucket, r.cnt, r.checksum))
                .collect();
            compare_buckets(table, &src, &dst)
        }
        "supply_history" => {
            // Both sources: rows per year bucket, checksum = SUM(ts_secs).
            let src_a = source_buckets(
                source,
                "SELECT (timestamp::bigint / 31536000)::bigint AS bucket, COUNT(*)::bigint AS cnt, \
                 COALESCE(SUM(timestamp::bigint), 0)::numeric AS checksum \
                 FROM sm.supply_history WHERE total_issuance IS NOT NULL GROUP BY 1 ORDER BY 1",
            )
            .await?;
            let dst_a_rows = sqlx::query!(
                r#"SELECT (ts_secs / 31536000)::bigint AS "bucket!", COUNT(*)::bigint AS "cnt!",
                   COALESCE(SUM(ts_secs), 0)::numeric AS "checksum!"
                   FROM sm.supply_history WHERE source = 'supply_history' GROUP BY 1 ORDER BY 1"#
            )
            .fetch_all(target)
            .await?;
            let dst_a: Buckets = dst_a_rows
                .into_iter()
                .map(|r| (r.bucket, r.cnt, r.checksum))
                .collect();
            let ok_a = compare_buckets("supply_history", &src_a, &dst_a);
            let src_b: i64 = sqlx::query(
                "SELECT COUNT(*)::bigint AS c FROM asset_snapshot WHERE type = 'DAY' AND supply IS NOT NULL",
            )
            .fetch_one(source)
            .await?
            .try_get("c")?;
            let dst_b = sqlx::query_scalar!(
                r#"SELECT COUNT(*)::bigint AS "c!" FROM sm.supply_history WHERE source = 'asset_snapshot'"#
            )
            .fetch_one(target)
            .await?;
            let skipped = sqlx::query_scalar!(
                r#"SELECT skipped FROM sm.etl_skipped WHERE table_name = 'asset_snapshot'"#
            )
            .fetch_optional(target)
            .await?
            .unwrap_or(0);
            let ok_b = src_b == dst_b + skipped;
            if !ok_b {
                warn!(
                    table,
                    src_b, dst_b, skipped, "RECONCILE FAIL: asset_snapshot ≠ copied + skipped"
                );
            }
            ok_a && ok_b
        }
        "site_daily" => {
            // Every source (day, section) must exist in the target with the same pageviews.
            let src = source_buckets(
                source,
                "SELECT EXTRACT(EPOCH FROM day)::bigint / 86400 AS bucket, COUNT(*)::bigint AS cnt, \
                 COALESCE(SUM(pageviews), 0)::numeric AS checksum FROM sm.site_daily GROUP BY 1 ORDER BY 1",
            )
            .await?;
            let dst_rows = sqlx::query!(
                r#"SELECT (EXTRACT(EPOCH FROM day)::bigint / 86400) AS "bucket!", COUNT(*)::bigint AS "cnt!",
                   COALESCE(SUM(pageviews), 0)::numeric AS "checksum!" FROM sm.site_daily GROUP BY 1 ORDER BY 1"#
            )
            .fetch_all(target)
            .await?;
            let dst: Buckets = dst_rows
                .into_iter()
                .map(|r| (r.bucket, r.cnt, r.checksum))
                .collect();
            compare_buckets(table, &src, &dst)
        }
        "site_events" => {
            // Per UTC day, rows and the sum of ids' parity-free checksum
            // (count of pageviews) for days strictly before today: the
            // Node keeps writing today's rows during the parallel run.
            let src = source_buckets(
                source,
                "SELECT EXTRACT(EPOCH FROM (ts AT TIME ZONE 'UTC')::date)::bigint / 86400 AS bucket, \
                 COUNT(*)::bigint AS cnt, COUNT(*) FILTER (WHERE type = 'pageview')::numeric AS checksum \
                 FROM sm.site_events WHERE ts < date_trunc('day', now()) GROUP BY 1 ORDER BY 1",
            )
            .await?;
            let dst_rows = sqlx::query!(
                r#"SELECT (EXTRACT(EPOCH FROM (ts AT TIME ZONE 'UTC')::date)::bigint / 86400) AS "bucket!",
                   COUNT(*)::bigint AS "cnt!", COUNT(*) FILTER (WHERE type = 'pageview')::numeric AS "checksum!"
                   FROM sm.site_events WHERE origin = 'legacy' AND ts < date_trunc('day', now())
                   GROUP BY 1 ORDER BY 1"#
            )
            .fetch_all(target)
            .await?;
            let dst: Buckets = dst_rows
                .into_iter()
                .map(|r| (r.bucket, r.cnt, r.checksum))
                .collect();
            compare_buckets(table, &src, &dst)
        }
        "news_episodes" => {
            // Small table: exact slug set comparison.
            let src: Vec<String> = sqlx::query("SELECT slug FROM sm.news_episodes ORDER BY slug")
                .fetch_all(source)
                .await?
                .iter()
                .map(|r| r.try_get::<String, _>("slug"))
                .collect::<Result<_, _>>()?;
            let dst: Vec<String> =
                sqlx::query_scalar!(r#"SELECT slug FROM sm.news_episodes ORDER BY slug"#)
                    .fetch_all(target)
                    .await?;
            let missing: Vec<&String> = src.iter().filter(|s| !dst.contains(s)).collect();
            if missing.is_empty() {
                true
            } else {
                warn!(table, missing = ?missing, "RECONCILE FAIL: slugs missing in target");
                false
            }
        }
        "polkamarkt_markets" => {
            let dst = sqlx::query_scalar!(
                r#"SELECT market_id FROM sm.polkamarkt_markets WHERE origin = 'legacy' ORDER BY market_id"#
            )
            .fetch_all(target)
            .await?;
            reconcile_ids(
                source,
                table,
                "SELECT market_id AS id FROM sm.polkamarkt_markets ORDER BY 1",
                dst,
            )
            .await?
        }
        "polkamarkt_trades" => {
            let dst = sqlx::query_scalar!(
                r#"SELECT legacy_id AS "id!" FROM sm.polkamarkt_trades WHERE legacy_id IS NOT NULL ORDER BY 1"#
            )
            .fetch_all(target)
            .await?;
            reconcile_ids(
                source,
                table,
                "SELECT id FROM sm.polkamarkt_trades ORDER BY 1",
                dst,
            )
            .await?
        }
        "polkamarkt_claims" => {
            let dst = sqlx::query_scalar!(
                r#"SELECT legacy_id AS "id!" FROM sm.polkamarkt_claims WHERE legacy_id IS NOT NULL ORDER BY 1"#
            )
            .fetch_all(target)
            .await?;
            reconcile_ids(
                source,
                table,
                "SELECT id FROM sm.polkamarkt_claims ORDER BY 1",
                dst,
            )
            .await?
        }
        "polkamarkt_buybacks" => {
            let dst = sqlx::query_scalar!(
                r#"SELECT legacy_id AS "id!" FROM sm.polkamarkt_buybacks WHERE legacy_id IS NOT NULL ORDER BY 1"#
            )
            .fetch_all(target)
            .await?;
            reconcile_ids(
                source,
                table,
                "SELECT id FROM sm.polkamarkt_buybacks ORDER BY 1",
                dst,
            )
            .await?
        }
        "polkamarkt_burns" => {
            let dst = sqlx::query_scalar!(
                r#"SELECT legacy_id AS "id!" FROM sm.polkamarkt_burns WHERE legacy_id IS NOT NULL ORDER BY 1"#
            )
            .fetch_all(target)
            .await?;
            reconcile_ids(
                source,
                table,
                "SELECT id FROM sm.polkamarkt_burns ORDER BY 1",
                dst,
            )
            .await?
        }
        "asset_registry" => {
            let src_cnt: i64 = sqlx::query(
                "SELECT COUNT(*)::bigint AS c FROM sm.asset_registry WHERE length(asset_id) = 66",
            )
            .fetch_one(source)
            .await?
            .try_get("c")?;
            let dst_cnt =
                sqlx::query_scalar!(r#"SELECT COUNT(*)::bigint AS "c!" FROM sm.asset_registry"#)
                    .fetch_one(target)
                    .await?;
            // Target may exceed source (whitelist entries not in legacy).
            if dst_cnt >= src_cnt {
                true
            } else {
                warn!(
                    table,
                    src_cnt, dst_cnt, "RECONCILE FAIL: target has fewer assets than source"
                );
                false
            }
        }
        other => bail!("no reconciliation for table '{other}'"),
    };

    if ok {
        info!(table, "reconcile OK");
    }
    Ok(ok)
}

#[cfg(test)]
mod legacy_detail_tests {
    use super::*;

    fn ev(index: i32, section: &str, method: &str, data: Option<Json>) -> LegacyEvent {
        LegacyEvent {
            block: 20_000_000,
            index,
            section: section.to_string(),
            method: method.to_string(),
            data,
        }
    }

    #[test]
    fn drops_success_and_failed_and_keeps_shape() {
        let rows = vec![
            ev(
                1,
                "Tokens",
                "Deposited",
                Some(serde_json::json!({"amount": 2378450880699805283_u64})),
            ),
            ev(
                1,
                "System",
                "ExtrinsicSuccess",
                Some(serde_json::json!({"dispatch_info": {}})),
            ),
            ev(2, "System", "ExtrinsicFailed", None),
        ];
        let out = shape_legacy_events(rows);
        let got = out.get(&(20_000_000, 1)).expect("index 1 kept");
        assert_eq!(
            got,
            &serde_json::json!([{"s": "Tokens", "m": "Deposited", "d": "{\"amount\":2378450880699805283}"}])
        );
        assert!(
            !out.contains_key(&(20_000_000, 2)),
            "only a failed event → absent"
        );
    }

    #[test]
    fn data_null_gives_d_null() {
        let out = shape_legacy_events(vec![ev(1, "Balances", "Withdraw", None)]);
        assert_eq!(
            out[&(20_000_000, 1)],
            serde_json::json!([{"s": "Balances", "m": "Withdraw", "d": null}])
        );
    }

    #[test]
    fn limit_applies_before_the_filter() {
        let mut rows: Vec<LegacyEvent> = (0..100)
            .map(|_| ev(1, "Tokens", "Transfer", Some(serde_json::json!(1))))
            .collect();
        rows.push(ev(1, "System", "ExtrinsicSuccess", None));
        rows.push(ev(1, "Tokens", "Transfer", Some(serde_json::json!(2))));
        let out = shape_legacy_events(rows);
        let kept = out[&(20_000_000, 1)].as_array().map(Vec::len);
        assert_eq!(kept, Some(100), "101st and later rows are never read");
    }
}
