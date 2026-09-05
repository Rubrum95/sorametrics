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
use chrono::{DateTime, Utc};
use sqlx::postgres::PgPoolOptions;
use sqlx::{PgPool, Row};
use std::time::Instant;
use tracing::{info, warn};

/// All known ETL tables, in dependency order (asset_registry first: the
/// planck conversions of the event tables JOIN it on the SOURCE side,
/// so order only matters for operator sanity, not correctness).
pub const ALL_TABLES: [&str; 7] = [
    "asset_registry",
    "swaps",
    "transfers",
    "bridges",
    "fees",
    "fee_burns",
    "price_history",
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
        if !ALL_TABLES.contains(&t.as_str()) {
            bail!("unknown table '{t}' — valid: {}", ALL_TABLES.join(","));
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
            cursor = *blocks.last().expect("just pushed");
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
        cur_asset = assets.last().cloned().expect("non-empty");
        cur_bucket = *buckets.last().expect("non-empty");

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
