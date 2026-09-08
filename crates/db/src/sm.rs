//! SORA v2 (`sm.*` schema) typed query helpers.
//!
//! Every public function here uses `sqlx::query!` / `sqlx::query_as!` so
//! the SQL is checked at compile time against the schema captured in
//! `.sqlx/`. To regenerate that cache after a migration:
//!
//! ```bash
//! make dev-up
//! make migrate
//! DATABASE_URL=postgres://... cargo sqlx prepare --workspace
//! ```
//!
//! Inserts are idempotent UPSERTs keyed on `(block_height, extrinsic_id, event_id)`.
//! Re-processing the same event produces a no-op.

use crate::DbError;
use sorametrics_core::chain::{Address, AssetId, BlockHeight};
use sorametrics_core::sora_v2::{
    BridgeDirection, FeeBurnKind, V2Bridge, V2Extrinsic, V2Fee, V2FeeBurn, V2FeeBurnAggregate,
    V2Liquidity, V2OrderBookEvent, V2Swap, V2Transfer, V2ValStakingReward,
};
use sqlx::PgPool;

/// Outcome of an UPSERT call.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum UpsertOutcome {
    /// Row was new and got inserted.
    Inserted,
    /// Row already existed (conflict on PK), nothing changed.
    AlreadyPresent,
}

// =============================================================
// indexer_state
// =============================================================

/// Reads the cursor for a given job.
pub async fn get_cursor(pool: &PgPool, job_name: &str) -> Result<Option<BlockHeight>, DbError> {
    let row = sqlx::query!(
        r#"
        SELECT last_processed_block
        FROM sm.indexer_state
        WHERE job_name = $1
        "#,
        job_name
    )
    .fetch_optional(pool)
    .await?;
    Ok(row.map(|r| BlockHeight(r.last_processed_block as u64)))
}

/// UPSERTs the cursor for a given job.
///
/// `last_processed_block` is the highest block successfully processed.
/// The status string is free-form; conventional values are
/// `idle | running | error | paused`.
pub async fn set_cursor(
    pool: &PgPool,
    job_name: &str,
    last_processed_block: BlockHeight,
    status: &str,
) -> Result<(), DbError> {
    sqlx::query!(
        r#"
        INSERT INTO sm.indexer_state (job_name, last_processed_block, status, last_updated_at)
        VALUES ($1, $2, $3, NOW())
        ON CONFLICT (job_name) DO UPDATE
            SET last_processed_block = EXCLUDED.last_processed_block,
                status               = EXCLUDED.status,
                last_updated_at      = NOW(),
                error_message        = NULL
        "#,
        job_name,
        last_processed_block.0 as i64,
        status,
    )
    .execute(pool)
    .await?;
    Ok(())
}

/// Records an error against a job. Leaves `last_processed_block` untouched.
pub async fn set_error(pool: &PgPool, job_name: &str, message: &str) -> Result<(), DbError> {
    sqlx::query!(
        r#"
        UPDATE sm.indexer_state
        SET status          = 'error',
            error_message   = $2,
            last_updated_at = NOW()
        WHERE job_name = $1
        "#,
        job_name,
        message,
    )
    .execute(pool)
    .await?;
    Ok(())
}

// =============================================================
// live_swaps
// =============================================================

/// Inserts a swap, idempotent on `(block_height, extrinsic_id, event_id)`.
pub async fn insert_swap(pool: &PgPool, swap: &V2Swap) -> Result<UpsertOutcome, DbError> {
    let row = sqlx::query!(
        r#"
        INSERT INTO sm.swaps (
            block_height, extrinsic_id, event_id, block_timestamp,
            caller, input_asset_id, input_amount, output_asset_id, output_amount,
            usd_value, output_usd_value, hash
        ) VALUES (
            $1, $2, $3, $4,
            $5, $6, $7, $8, $9,
            $10, $11, $12
        )
        ON CONFLICT (block_height, extrinsic_id, event_id) DO NOTHING
        RETURNING block_height
        "#,
        swap.block_height.0 as i64,
        swap.extrinsic_id.to_string(),
        swap.event_id as i32,
        swap.timestamp.0,
        swap.caller.0,
        swap.input_asset.0,
        swap.input_amount,
        swap.output_asset.0,
        swap.output_amount,
        swap.usd_value,
        swap.output_usd_value,
        swap.extrinsic_hash.as_deref(),
    )
    .fetch_optional(pool)
    .await?;

    Ok(if row.is_some() {
        UpsertOutcome::Inserted
    } else {
        UpsertOutcome::AlreadyPresent
    })
}

// =============================================================
// live_transfers
// =============================================================

/// Inserts a transfer, idempotent on `(block_height, extrinsic_id, event_id)`.
pub async fn insert_transfer(
    pool: &PgPool,
    transfer: &V2Transfer,
) -> Result<UpsertOutcome, DbError> {
    let row = sqlx::query!(
        r#"
        INSERT INTO sm.transfers (
            block_height, extrinsic_id, event_id, block_timestamp,
            from_address, to_address, asset_id, amount,
            usd_value, hash
        ) VALUES (
            $1, $2, $3, $4,
            $5, $6, $7, $8,
            $9, $10
        )
        ON CONFLICT (block_height, extrinsic_id, event_id) DO NOTHING
        RETURNING block_height
        "#,
        transfer.block_height.0 as i64,
        transfer.extrinsic_id.to_string(),
        transfer.event_id as i32,
        transfer.timestamp.0,
        transfer.from.0,
        transfer.to.0,
        transfer.asset.0,
        transfer.amount,
        transfer.usd_value,
        transfer.extrinsic_hash.as_deref(),
    )
    .fetch_optional(pool)
    .await?;

    Ok(if row.is_some() {
        UpsertOutcome::Inserted
    } else {
        UpsertOutcome::AlreadyPresent
    })
}

// =============================================================
// live_bridges
// =============================================================

/// Postgres ENUM mapping for `sm.bridge_direction`.
///
/// `type_name` is schema-qualified because the ENUM lives in `sm`, not
/// `public`. Without the qualifier sqlx fails to resolve it on a session
/// whose `search_path` doesn't include `sm` (which is the default).
#[derive(sqlx::Type, Debug, Clone, Copy, PartialEq, Eq)]
#[sqlx(type_name = "sm.bridge_direction", rename_all = "lowercase")]
enum PgBridgeDirection {
    In,
    Out,
}

impl From<BridgeDirection> for PgBridgeDirection {
    fn from(d: BridgeDirection) -> Self {
        match d {
            BridgeDirection::In => Self::In,
            BridgeDirection::Out => Self::Out,
        }
    }
}

/// Inserts a bridge event, idempotent on `(block_height, extrinsic_id, event_id)`.
pub async fn insert_bridge(pool: &PgPool, bridge: &V2Bridge) -> Result<UpsertOutcome, DbError> {
    let direction: PgBridgeDirection = bridge.direction.into();
    let row = sqlx::query!(
        r#"
        INSERT INTO sm.bridges (
            block_height, extrinsic_id, event_id, block_timestamp,
            direction, network, caller, counterparty, asset_id, amount, usd_value, hash
        ) VALUES (
            $1, $2, $3, $4,
            $5, $6, $7, $8, $9, $10, $11, $12
        )
        ON CONFLICT (block_height, extrinsic_id, event_id) DO NOTHING
        RETURNING block_height
        "#,
        bridge.block_height.0 as i64,
        bridge.extrinsic_id.to_string(),
        bridge.event_id as i32,
        bridge.timestamp.0,
        direction as PgBridgeDirection,
        bridge.network,
        bridge.caller.0,
        bridge.counterparty.as_deref(),
        bridge.asset.0,
        bridge.amount,
        bridge.usd_value,
        bridge.extrinsic_hash.as_deref(),
    )
    .fetch_optional(pool)
    .await?;

    Ok(if row.is_some() {
        UpsertOutcome::Inserted
    } else {
        UpsertOutcome::AlreadyPresent
    })
}

/// Counts rows in a live table for testing / freshness checks.
pub async fn count_swaps(pool: &PgPool) -> Result<i64, DbError> {
    let row = sqlx::query!("SELECT COUNT(*) AS c FROM sm.swaps")
        .fetch_one(pool)
        .await?;
    Ok(row.c.unwrap_or(0))
}

/// Counts transfers.
pub async fn count_transfers(pool: &PgPool) -> Result<i64, DbError> {
    let row = sqlx::query!("SELECT COUNT(*) AS c FROM sm.transfers")
        .fetch_one(pool)
        .await?;
    Ok(row.c.unwrap_or(0))
}

/// Counts bridge events.
pub async fn count_bridges(pool: &PgPool) -> Result<i64, DbError> {
    let row = sqlx::query!("SELECT COUNT(*) AS c FROM sm.bridges")
        .fetch_one(pool)
        .await?;
    Ok(row.c.unwrap_or(0))
}

// =============================================================
// asset_registry
// =============================================================

/// One asset registry row, as needed by the price pipeline.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RegistryAsset {
    /// `0x`-hex asset id.
    pub asset_id: String,
    /// Ticker symbol (e.g. `XOR`).
    pub symbol: String,
    /// Display name, if known.
    pub name: Option<String>,
    /// On-chain decimals.
    pub decimals: i16,
    /// Logo (data URI or URL) for the legacy API rows, if known.
    pub logo: Option<String>,
    /// In the official sora-xor whitelist (listed by `/tokens`).
    pub whitelisted: bool,
}

/// Every asset in `sm.asset_registry`.
pub async fn load_asset_registry(pool: &PgPool) -> Result<Vec<RegistryAsset>, DbError> {
    let rows = sqlx::query_as!(
        RegistryAsset,
        r#"SELECT asset_id, symbol, name, decimals, logo, whitelisted FROM sm.asset_registry"#
    )
    .fetch_all(pool)
    .await?;
    Ok(rows)
}

// =============================================================
// live_fee_burns
// =============================================================

/// Postgres ENUM mapping for `sm.fee_burn_kind`.
///
/// Schema-qualified `type_name` is required because the ENUM lives in
/// the `sm` schema (default `search_path` does not include `sm`).
#[derive(sqlx::Type, Debug, Clone, Copy, PartialEq, Eq)]
#[sqlx(type_name = "sm.fee_burn_kind", rename_all = "snake_case")]
enum PgFeeBurnKind {
    FeeWithdrawn,
    ReferrerRewarded,
}

impl From<FeeBurnKind> for PgFeeBurnKind {
    fn from(k: FeeBurnKind) -> Self {
        match k {
            FeeBurnKind::FeeWithdrawn => Self::FeeWithdrawn,
            FeeBurnKind::ReferrerRewarded => Self::ReferrerRewarded,
        }
    }
}

/// Inserts a fee-burn row, idempotent on `(block_height, extrinsic_id, event_id)`.
///
/// The DB-level CHECK constraint enforces the `kind`/`referrer` invariant
/// (FeeWithdrawn → referrer NULL, ReferrerRewarded → referrer NOT NULL),
/// so we don't re-validate here — wrong combos surface as a `DbError` at
/// insert time, which is the correct failure mode (decoder bug).
pub async fn insert_fee_burn(pool: &PgPool, burn: &V2FeeBurn) -> Result<UpsertOutcome, DbError> {
    let kind: PgFeeBurnKind = burn.kind.into();
    let referrer = burn.referrer.as_ref().map(|a| a.as_str());

    let row = sqlx::query!(
        r#"
        INSERT INTO sm.fee_events (
            block_height, extrinsic_id, event_id, block_timestamp,
            kind, payer, referrer, amount, hash
        ) VALUES (
            $1, $2, $3, $4,
            $5, $6, $7, $8, $9
        )
        ON CONFLICT (block_height, extrinsic_id, event_id) DO NOTHING
        RETURNING block_height
        "#,
        burn.block_height.0 as i64,
        burn.extrinsic_id.to_string(),
        burn.event_id as i32,
        burn.timestamp.0,
        kind as PgFeeBurnKind,
        burn.payer.0,
        referrer,
        burn.amount,
        burn.extrinsic_hash.as_deref(),
    )
    .fetch_optional(pool)
    .await?;

    Ok(if row.is_some() {
        UpsertOutcome::Inserted
    } else {
        UpsertOutcome::AlreadyPresent
    })
}

/// Counts fee-burn rows (both kinds).
pub async fn count_fee_burns(pool: &PgPool) -> Result<i64, DbError> {
    let row = sqlx::query!("SELECT COUNT(*) AS c FROM sm.fee_events")
        .fetch_one(pool)
        .await?;
    Ok(row.c.unwrap_or(0))
}

// =============================================================
// Batched inserts — one round-trip per event type per block.
//
// The live subscriber and backfill decode a whole block first, then
// land each event family in a single UNNEST upsert (same pattern as
// `ops migrate-legacy`). `rows_affected` counts the genuinely new rows
// (ON CONFLICT skips don't count), which is exactly the "inserted"
// number BlockDecodeStats wants.
// =============================================================

/// Batch-insert swaps. Returns the number of NEW rows.
pub async fn insert_swaps_batch(pool: &PgPool, swaps: &[V2Swap]) -> Result<u64, DbError> {
    if swaps.is_empty() {
        return Ok(0);
    }
    let n = swaps.len();
    let mut blocks = Vec::with_capacity(n);
    let mut ext_ids = Vec::with_capacity(n);
    let mut event_ids = Vec::with_capacity(n);
    let mut tss = Vec::with_capacity(n);
    let mut callers = Vec::with_capacity(n);
    let mut in_assets = Vec::with_capacity(n);
    let mut in_amounts = Vec::with_capacity(n);
    let mut out_assets = Vec::with_capacity(n);
    let mut out_amounts = Vec::with_capacity(n);
    let mut usds: Vec<Option<bigdecimal::BigDecimal>> = Vec::with_capacity(n);
    let mut out_usds: Vec<Option<bigdecimal::BigDecimal>> = Vec::with_capacity(n);
    let mut hashes: Vec<Option<String>> = Vec::with_capacity(n);
    for s in swaps {
        blocks.push(s.block_height.0 as i64);
        ext_ids.push(s.extrinsic_id.to_string());
        event_ids.push(s.event_id as i32);
        tss.push(s.timestamp.0);
        callers.push(s.caller.0.clone());
        in_assets.push(s.input_asset.0.clone());
        in_amounts.push(s.input_amount.clone());
        out_assets.push(s.output_asset.0.clone());
        out_amounts.push(s.output_amount.clone());
        usds.push(s.usd_value.clone());
        out_usds.push(s.output_usd_value.clone());
        hashes.push(s.extrinsic_hash.clone());
    }
    let res = sqlx::query!(
        r#"
        INSERT INTO sm.swaps (
            block_height, extrinsic_id, event_id, block_timestamp,
            caller, input_asset_id, input_amount, output_asset_id, output_amount,
            usd_value, output_usd_value, hash
        )
        SELECT b, e, ev, t, c, ia, iam, oa, oam, u, ou, h
        FROM UNNEST(
            $1::bigint[], $2::text[], $3::int[], $4::timestamptz[], $5::text[],
            $6::text[], $7::numeric[], $8::text[], $9::numeric[],
            $10::numeric[], $11::numeric[], $12::text[]
        ) AS x(b, e, ev, t, c, ia, iam, oa, oam, u, ou, h)
        ON CONFLICT (block_height, extrinsic_id, event_id) DO NOTHING
        "#,
        &blocks,
        &ext_ids,
        &event_ids,
        &tss,
        &callers,
        &in_assets,
        &in_amounts,
        &out_assets,
        &out_amounts,
        &usds as &[Option<bigdecimal::BigDecimal>],
        &out_usds as &[Option<bigdecimal::BigDecimal>],
        &hashes as &[Option<String>],
    )
    .execute(pool)
    .await?;
    Ok(res.rows_affected())
}

/// Batch-insert transfers. Returns the number of NEW rows.
pub async fn insert_transfers_batch(
    pool: &PgPool,
    transfers: &[V2Transfer],
) -> Result<u64, DbError> {
    if transfers.is_empty() {
        return Ok(0);
    }
    let n = transfers.len();
    let mut blocks = Vec::with_capacity(n);
    let mut ext_ids = Vec::with_capacity(n);
    let mut event_ids = Vec::with_capacity(n);
    let mut tss = Vec::with_capacity(n);
    let mut froms = Vec::with_capacity(n);
    let mut tos = Vec::with_capacity(n);
    let mut assets = Vec::with_capacity(n);
    let mut amounts = Vec::with_capacity(n);
    let mut usds: Vec<Option<bigdecimal::BigDecimal>> = Vec::with_capacity(n);
    let mut hashes: Vec<Option<String>> = Vec::with_capacity(n);
    for t in transfers {
        blocks.push(t.block_height.0 as i64);
        ext_ids.push(t.extrinsic_id.to_string());
        event_ids.push(t.event_id as i32);
        tss.push(t.timestamp.0);
        froms.push(t.from.0.clone());
        tos.push(t.to.0.clone());
        assets.push(t.asset.0.clone());
        amounts.push(t.amount.clone());
        usds.push(t.usd_value.clone());
        hashes.push(t.extrinsic_hash.clone());
    }
    let res = sqlx::query!(
        r#"
        INSERT INTO sm.transfers (
            block_height, extrinsic_id, event_id, block_timestamp,
            from_address, to_address, asset_id, amount, usd_value, hash
        )
        SELECT b, e, ev, t, f, "to", a, am, u, h
        FROM UNNEST(
            $1::bigint[], $2::text[], $3::int[], $4::timestamptz[], $5::text[],
            $6::text[], $7::text[], $8::numeric[], $9::numeric[], $10::text[]
        ) AS x(b, e, ev, t, f, "to", a, am, u, h)
        ON CONFLICT (block_height, extrinsic_id, event_id) DO NOTHING
        "#,
        &blocks,
        &ext_ids,
        &event_ids,
        &tss,
        &froms,
        &tos,
        &assets,
        &amounts,
        &usds as &[Option<bigdecimal::BigDecimal>],
        &hashes as &[Option<String>],
    )
    .execute(pool)
    .await?;
    Ok(res.rows_affected())
}

/// Batch-insert bridge events. Returns the number of NEW rows.
pub async fn insert_bridges_batch(pool: &PgPool, bridges: &[V2Bridge]) -> Result<u64, DbError> {
    if bridges.is_empty() {
        return Ok(0);
    }
    let n = bridges.len();
    let mut blocks = Vec::with_capacity(n);
    let mut ext_ids = Vec::with_capacity(n);
    let mut event_ids = Vec::with_capacity(n);
    let mut tss = Vec::with_capacity(n);
    let mut directions = Vec::with_capacity(n);
    let mut networks = Vec::with_capacity(n);
    let mut callers = Vec::with_capacity(n);
    let mut counterparties: Vec<Option<String>> = Vec::with_capacity(n);
    let mut assets = Vec::with_capacity(n);
    let mut amounts = Vec::with_capacity(n);
    let mut usds: Vec<Option<bigdecimal::BigDecimal>> = Vec::with_capacity(n);
    let mut hashes: Vec<Option<String>> = Vec::with_capacity(n);
    for b in bridges {
        blocks.push(b.block_height.0 as i64);
        ext_ids.push(b.extrinsic_id.to_string());
        event_ids.push(b.event_id as i32);
        tss.push(b.timestamp.0);
        directions.push(match b.direction {
            BridgeDirection::In => "in".to_string(),
            BridgeDirection::Out => "out".to_string(),
        });
        networks.push(b.network.clone());
        callers.push(b.caller.0.clone());
        counterparties.push(b.counterparty.clone());
        assets.push(b.asset.0.clone());
        amounts.push(b.amount.clone());
        usds.push(b.usd_value.clone());
        hashes.push(b.extrinsic_hash.clone());
    }
    let res = sqlx::query!(
        r#"
        INSERT INTO sm.bridges (
            block_height, extrinsic_id, event_id, block_timestamp,
            direction, network, caller, counterparty, asset_id, amount, usd_value, hash
        )
        SELECT b, e, ev, t, d::sm.bridge_direction, nw, c, cp, a, am, u, h
        FROM UNNEST(
            $1::bigint[], $2::text[], $3::int[], $4::timestamptz[], $5::text[],
            $6::text[], $7::text[], $8::text[], $9::text[], $10::numeric[], $11::numeric[], $12::text[]
        ) AS x(b, e, ev, t, d, nw, c, cp, a, am, u, h)
        ON CONFLICT (block_height, extrinsic_id, event_id) DO NOTHING
        "#,
        &blocks,
        &ext_ids,
        &event_ids,
        &tss,
        &directions,
        &networks,
        &callers,
        &counterparties as &[Option<String>],
        &assets,
        &amounts,
        &usds as &[Option<bigdecimal::BigDecimal>],
        &hashes as &[Option<String>],
    )
    .execute(pool)
    .await?;
    Ok(res.rows_affected())
}

// =============================================================
// extrinsics
// =============================================================

/// Batch-insert extrinsic rows, idempotent on `(block, index)`. Returns NEW rows.
pub async fn insert_extrinsics_batch(pool: &PgPool, rows: &[V2Extrinsic]) -> Result<u64, DbError> {
    if rows.is_empty() {
        return Ok(0);
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
    let mut args = Vec::with_capacity(n);
    let mut events = Vec::with_capacity(n);
    for r in rows {
        blocks.push(r.block_height.0 as i64);
        idxs.push(r.extrinsic_index as i32);
        tss.push(r.timestamp.0);
        hashes.push(r.hash.clone());
        sections.push(r.section.clone());
        methods.push(r.method.clone());
        signers.push(r.signer.clone());
        successes.push(r.success);
        errors.push(r.error_msg.clone());
        args.push(r.args.clone());
        events.push(r.events.clone());
    }
    let res = sqlx::query!(
        r#"
        INSERT INTO sm.extrinsics (
            block_height, extrinsic_index, block_timestamp, hash, section, method,
            signer, success, error_msg, args, events
        )
        SELECT b, i, t, h, s, m, sg, ok, e, a, ev
        FROM UNNEST(
            $1::bigint[], $2::int[], $3::timestamptz[], $4::text[], $5::text[], $6::text[],
            $7::text[], $8::bool[], $9::text[], $10::jsonb[], $11::jsonb[]
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
        &args,
        &events,
    )
    .execute(pool)
    .await?;
    Ok(res.rows_affected())
}

// =============================================================
// liquidity_events
// =============================================================

/// Batch-insert liquidity events, idempotent on the PK. Returns NEW rows.
pub async fn insert_liquidity_batch(pool: &PgPool, rows: &[V2Liquidity]) -> Result<u64, DbError> {
    if rows.is_empty() {
        return Ok(0);
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
    let mut usds: Vec<Option<bigdecimal::BigDecimal>> = Vec::with_capacity(n);
    let mut kinds = Vec::with_capacity(n);
    let mut hashes: Vec<Option<String>> = Vec::with_capacity(n);
    for r in rows {
        blocks.push(r.block_height.0 as i64);
        ext_ids.push(r.extrinsic_id.to_string());
        tss.push(r.timestamp.0);
        callers.push(r.caller.0.clone());
        bases.push(r.base_asset.0.clone());
        targets.push(r.target_asset.0.clone());
        base_amounts.push(r.base_amount.clone());
        target_amounts.push(r.target_amount.clone());
        usds.push(r.usd_value.clone());
        kinds.push(r.kind.label().to_string());
        hashes.push(r.extrinsic_hash.clone());
    }
    let res = sqlx::query!(
        r#"
        INSERT INTO sm.liquidity_events (
            block_height, extrinsic_id, event_id, block_timestamp, caller,
            base_asset_id, target_asset_id, base_amount, target_amount, usd_value, kind, hash
        )
        SELECT b, e, 0, t, c, ba, ta, bam, tam, u, k, h
        FROM UNNEST(
            $1::bigint[], $2::text[], $3::timestamptz[], $4::text[],
            $5::text[], $6::text[], $7::numeric[], $8::numeric[], $9::numeric[], $10::text[], $11::text[]
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
        &usds as &[Option<bigdecimal::BigDecimal>],
        &kinds,
        &hashes as &[Option<String>],
    )
    .execute(pool)
    .await?;
    Ok(res.rows_affected())
}

// =============================================================
// fees (per-extrinsic network fee, live rows)
// =============================================================

/// Batch-insert live network fees into `sm.fees`. `legacy_id` is
/// `"<block>-<extrinsic index>"`, `amount_xor` is human XOR (raw planck
/// / 1e18, the legacy unit), `origin = 'live'`. Idempotent on the id.
/// Returns the number of NEW rows.
pub async fn insert_fees_batch(pool: &PgPool, fees: &[V2Fee]) -> Result<u64, DbError> {
    if fees.is_empty() {
        return Ok(0);
    }
    let n = fees.len();
    let mut ids = Vec::with_capacity(n);
    let mut blocks = Vec::with_capacity(n);
    let mut tss = Vec::with_capacity(n);
    let mut types = Vec::with_capacity(n);
    let mut amounts = Vec::with_capacity(n);
    let mut usds: Vec<Option<bigdecimal::BigDecimal>> = Vec::with_capacity(n);
    for f in fees {
        ids.push(format!("{}-{}", f.block_height.0, f.extrinsic_id));
        blocks.push(f.block_height.0 as i64);
        tss.push(f.timestamp.0);
        types.push(f.fee_type.label().to_string());
        amounts.push(f.amount.clone());
        usds.push(f.usd_value.clone());
    }
    let res = sqlx::query!(
        r#"
        INSERT INTO sm.fees (legacy_id, block_height, block_timestamp, fee_type, amount_xor, usd_value, origin)
        SELECT i, b, t, ty, am / 1000000000000000000::numeric, u, 'live'
        FROM UNNEST($1::text[], $2::bigint[], $3::timestamptz[], $4::text[], $5::numeric[], $6::numeric[])
             AS x(i, b, t, ty, am, u)
        ON CONFLICT (legacy_id) DO NOTHING
        "#,
        &ids,
        &blocks,
        &tss,
        &types,
        &amounts,
        &usds as &[Option<bigdecimal::BigDecimal>],
    )
    .execute(pool)
    .await?;
    Ok(res.rows_affected())
}

/// Batch-insert fee events. Returns the number of NEW rows.
pub async fn insert_fee_burns_batch(pool: &PgPool, burns: &[V2FeeBurn]) -> Result<u64, DbError> {
    if burns.is_empty() {
        return Ok(0);
    }
    let n = burns.len();
    let mut blocks = Vec::with_capacity(n);
    let mut ext_ids = Vec::with_capacity(n);
    let mut event_ids = Vec::with_capacity(n);
    let mut tss = Vec::with_capacity(n);
    let mut kinds = Vec::with_capacity(n);
    let mut payers = Vec::with_capacity(n);
    let mut referrers: Vec<Option<String>> = Vec::with_capacity(n);
    let mut amounts = Vec::with_capacity(n);
    let mut hashes: Vec<Option<String>> = Vec::with_capacity(n);
    for f in burns {
        blocks.push(f.block_height.0 as i64);
        ext_ids.push(f.extrinsic_id.to_string());
        event_ids.push(f.event_id as i32);
        tss.push(f.timestamp.0);
        kinds.push(match f.kind {
            FeeBurnKind::FeeWithdrawn => "fee_withdrawn".to_string(),
            FeeBurnKind::ReferrerRewarded => "referrer_rewarded".to_string(),
        });
        payers.push(f.payer.0.clone());
        referrers.push(f.referrer.as_ref().map(|a| a.0.clone()));
        amounts.push(f.amount.clone());
        hashes.push(f.extrinsic_hash.clone());
    }
    let res = sqlx::query!(
        r#"
        INSERT INTO sm.fee_events (
            block_height, extrinsic_id, event_id, block_timestamp,
            kind, payer, referrer, amount, hash
        )
        SELECT b, e, ev, t, k::sm.fee_burn_kind, p, r, am, h
        FROM UNNEST(
            $1::bigint[], $2::text[], $3::int[], $4::timestamptz[], $5::text[],
            $6::text[], $7::text[], $8::numeric[], $9::text[]
        ) AS x(b, e, ev, t, k, p, r, am, h)
        ON CONFLICT (block_height, extrinsic_id, event_id) DO NOTHING
        "#,
        &blocks,
        &ext_ids,
        &event_ids,
        &tss,
        &kinds,
        &payers,
        &referrers as &[Option<String>],
        &amounts,
        &hashes as &[Option<String>],
    )
    .execute(pool)
    .await?;
    Ok(res.rows_affected())
}

// Suppress unused-imports for the public type aliases that downstream
// crates reach for via `use sorametrics_db::sm::{...}`.
#[allow(dead_code)]
fn _types_smoke() {
    let _ = std::any::type_name::<Address>();
    let _ = std::any::type_name::<AssetId>();
}

// =============================================================
// order_book_events
// =============================================================

/// Batch-insert order book rows, idempotent on `(block, extrinsic, event)`.
/// Returns NEW rows.
pub async fn insert_order_book_batch(
    pool: &PgPool,
    rows: &[V2OrderBookEvent],
) -> Result<u64, DbError> {
    if rows.is_empty() {
        return Ok(0);
    }
    let n = rows.len();
    let mut blocks = Vec::with_capacity(n);
    let mut ext_ids = Vec::with_capacity(n);
    let mut event_ids = Vec::with_capacity(n);
    let mut tss = Vec::with_capacity(n);
    let mut kinds = Vec::with_capacity(n);
    let mut wallets = Vec::with_capacity(n);
    let mut order_ids: Vec<Option<String>> = Vec::with_capacity(n);
    let mut bases = Vec::with_capacity(n);
    let mut quotes = Vec::with_capacity(n);
    let mut sides: Vec<Option<String>> = Vec::with_capacity(n);
    let mut prices: Vec<Option<bigdecimal::BigDecimal>> = Vec::with_capacity(n);
    let mut amounts: Vec<Option<bigdecimal::BigDecimal>> = Vec::with_capacity(n);
    let mut usds: Vec<Option<bigdecimal::BigDecimal>> = Vec::with_capacity(n);
    let mut hashes: Vec<Option<String>> = Vec::with_capacity(n);
    for r in rows {
        blocks.push(r.block_height.0 as i64);
        ext_ids.push(r.extrinsic_id.to_string());
        event_ids.push(r.event_id as i32);
        tss.push(r.timestamp.0);
        kinds.push(r.event_type.label().to_string());
        wallets.push(r.wallet.0.clone());
        order_ids.push(r.order_id.map(|id| id.to_string()));
        bases.push(r.base_asset.0.clone());
        quotes.push(r.quote_asset.0.clone());
        sides.push(r.side.map(|s| s.label().to_string()));
        prices.push(r.price.clone());
        amounts.push(r.amount.clone());
        usds.push(r.usd_value.clone());
        hashes.push(r.extrinsic_hash.clone());
    }
    let res = sqlx::query!(
        r#"
        INSERT INTO sm.order_book_events (
            block_height, extrinsic_id, event_id, block_timestamp, event_type, wallet,
            order_id, base_asset_id, quote_asset_id, side, price, amount, usd_value, hash
        )
        SELECT b, e, ev, t, k, w, o, ba, qa, s, p, a, u, h
        FROM UNNEST(
            $1::bigint[], $2::text[], $3::int[], $4::timestamptz[], $5::text[], $6::text[],
            $7::text[], $8::text[], $9::text[], $10::text[], $11::numeric[], $12::numeric[],
            $13::numeric[], $14::text[]
        ) AS x(b, e, ev, t, k, w, o, ba, qa, s, p, a, u, h)
        ON CONFLICT (block_height, extrinsic_id, event_id) DO NOTHING
        "#,
        &blocks,
        &ext_ids,
        &event_ids,
        &tss,
        &kinds,
        &wallets,
        &order_ids as &[Option<String>],
        &bases,
        &quotes,
        &sides as &[Option<String>],
        &prices as &[Option<bigdecimal::BigDecimal>],
        &amounts as &[Option<bigdecimal::BigDecimal>],
        &usds as &[Option<bigdecimal::BigDecimal>],
        &hashes as &[Option<String>],
    )
    .execute(pool)
    .await?;
    Ok(res.rows_affected())
}

// =============================================================
// val_staking_rewards
// =============================================================

/// Batch-insert VAL payout rows, idempotent on the natural key. Returns NEW rows.
pub async fn insert_val_staking_rewards_batch(
    pool: &PgPool,
    rows: &[V2ValStakingReward],
) -> Result<u64, DbError> {
    if rows.is_empty() {
        return Ok(0);
    }
    let n = rows.len();
    let mut eras = Vec::with_capacity(n);
    let mut pages = Vec::with_capacity(n);
    let mut stashes = Vec::with_capacity(n);
    let mut dests = Vec::with_capacity(n);
    let mut amounts = Vec::with_capacity(n);
    let mut blocks = Vec::with_capacity(n);
    let mut hashes = Vec::with_capacity(n);
    let mut tss = Vec::with_capacity(n);
    for r in rows {
        eras.push(r.era as i32);
        pages.push(r.page as i32);
        stashes.push(r.validator_stash.0.clone());
        dests.push(r.destination.0.clone());
        amounts.push(r.amount.clone());
        blocks.push(r.block_height.0 as i64);
        hashes.push(r.block_hash.clone());
        tss.push(r.timestamp.0);
    }
    let res = sqlx::query!(
        r#"
        INSERT INTO sm.val_staking_rewards (
            era, page, validator_stash, destination, amount, block_height, block_hash, block_timestamp
        )
        SELECT e, p, s, d, a, b, h, t
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
        &hashes,
        &tss,
    )
    .execute(pool)
    .await?;
    Ok(res.rows_affected())
}

// =============================================================
// fee_burns_aggregate
// =============================================================

/// The Node's `insertFeeBurnRow`: upsert the per-block aggregate.
pub async fn upsert_fee_burns_aggregate(
    pool: &PgPool,
    row: &V2FeeBurnAggregate,
) -> Result<(), DbError> {
    sqlx::query!(
        r#"
        INSERT INTO sm.fee_burns_aggregate (
            block_height, ts, fees_paid_xor, ref_paid_xor, ref_redirected_xor,
            remint_xor_burned, remint_val_burned, remint_kusd_burned, remint_tbcd_burned
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
        ON CONFLICT (block_height) DO UPDATE SET
            ts = EXCLUDED.ts,
            fees_paid_xor = EXCLUDED.fees_paid_xor,
            ref_paid_xor = EXCLUDED.ref_paid_xor,
            ref_redirected_xor = EXCLUDED.ref_redirected_xor,
            remint_xor_burned = EXCLUDED.remint_xor_burned,
            remint_val_burned = EXCLUDED.remint_val_burned,
            remint_kusd_burned = EXCLUDED.remint_kusd_burned,
            remint_tbcd_burned = EXCLUDED.remint_tbcd_burned
        "#,
        row.block_height.0 as i64,
        row.ts_millis,
        row.fees_paid_xor,
        row.ref_paid_xor,
        row.ref_redirected_xor,
        row.remint_xor_burned,
        row.remint_val_burned,
        row.remint_kusd_burned,
        row.remint_tbcd_burned,
    )
    .execute(pool)
    .await?;
    Ok(())
}

// =============================================================
// supply_snapshots
// =============================================================

/// One MOF circulating-supply sample (the Node's `insertSupplySnapshot`).
pub async fn insert_supply_snapshot(
    pool: &PgPool,
    symbol: &str,
    asset_id: Option<&str>,
    at: chrono::DateTime<chrono::Utc>,
    total_supply: f64,
) -> Result<(), DbError> {
    sqlx::query!(
        r#"INSERT INTO sm.supply_snapshots (symbol, ts, asset_id, total_supply)
           VALUES ($1, $2, $3, $4) ON CONFLICT (symbol, ts) DO NOTHING"#,
        symbol,
        at,
        asset_id,
        total_supply,
    )
    .execute(pool)
    .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    //! Integration tests against a live Postgres dev instance.
    //!
    //! These tests are gated on `DATABASE_URL` being set. They use
    //! `#[sqlx::test(migrator = ...)]` to spin up a fresh schema-migrated
    //! database per test. To run:
    //!
    //! ```bash
    //! make dev-up
    //! DATABASE_URL=postgres://sorametrics:sorametrics_dev@localhost:5432/sorametrics_v33 \
    //!   cargo test -p sorametrics-db
    //! ```

    use super::*;
    use bigdecimal::BigDecimal;
    use chrono::DateTime;
    use sorametrics_core::chain::{Address, AssetId};
    use sorametrics_core::sora_v2::{BridgeDirection, V2Bridge, V2Swap, V2Transfer};
    use sorametrics_core::time::Timestamp;
    use sqlx::PgPool;

    static MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!("../../migrations");

    fn sample_swap(block: u64) -> V2Swap {
        V2Swap {
            block_height: BlockHeight(block),
            extrinsic_id: 1,
            event_id: 4,
            extrinsic_hash: Some(format!("0x{}", "ab".repeat(32))),
            caller: Address::new("cnXXX"),
            input_asset: AssetId::new(
                "0x0200000000000000000000000000000000000000000000000000000000000000",
            ),
            input_amount: BigDecimal::from(1_000_000_u64),
            output_asset: AssetId::new(
                "0x0200080000000000000000000000000000000000000000000000000000000000",
            ),
            output_amount: BigDecimal::from(2_500_000_u64),
            // 1.50 USD as BigDecimal: integer 150 with scale 2.
            usd_value: Some(BigDecimal::new(num_bigint::BigInt::from(150_i64), 2)),
            output_usd_value: None,
            timestamp: Timestamp::new(DateTime::from_timestamp(1_700_000_000, 0).unwrap()),
        }
    }

    #[sqlx::test(migrator = "MIGRATOR")]
    async fn insert_swap_is_idempotent(pool: PgPool) {
        let swap = sample_swap(1234);

        let first = insert_swap(&pool, &swap).await.unwrap();
        assert_eq!(first, UpsertOutcome::Inserted);

        let second = insert_swap(&pool, &swap).await.unwrap();
        assert_eq!(second, UpsertOutcome::AlreadyPresent);

        assert_eq!(count_swaps(&pool).await.unwrap(), 1);
    }

    #[sqlx::test(migrator = "MIGRATOR")]
    async fn insert_transfer_idempotent(pool: PgPool) {
        let transfer = V2Transfer {
            block_height: BlockHeight(42),
            extrinsic_id: 2,
            event_id: 5,
            extrinsic_hash: None,
            from: Address::new("from1"),
            to: Address::new("to1"),
            asset: AssetId::new("xor"),
            amount: BigDecimal::from(100_u64),
            usd_value: None,
            timestamp: Timestamp::new(DateTime::from_timestamp(1_700_000_100, 0).unwrap()),
        };

        assert_eq!(
            insert_transfer(&pool, &transfer).await.unwrap(),
            UpsertOutcome::Inserted
        );
        assert_eq!(
            insert_transfer(&pool, &transfer).await.unwrap(),
            UpsertOutcome::AlreadyPresent
        );
        assert_eq!(count_transfers(&pool).await.unwrap(), 1);
    }

    #[sqlx::test(migrator = "MIGRATOR")]
    async fn insert_bridge_with_enum_direction(pool: PgPool) {
        let bridge = V2Bridge {
            block_height: BlockHeight(99),
            extrinsic_id: 3,
            event_id: 6,
            extrinsic_hash: Some(format!("0x{}", "cd".repeat(32))),
            direction: BridgeDirection::Out,
            network: "Substrate: Liberland".to_string(),
            caller: Address::new("cnAAA"),
            counterparty: Some("0xB".to_string()),
            asset: AssetId::new("xor"),
            amount: BigDecimal::from(500_u64),
            usd_value: None,
            timestamp: Timestamp::new(DateTime::from_timestamp(1_700_000_200, 0).unwrap()),
        };

        let outcome = insert_bridge(&pool, &bridge).await.unwrap();
        assert_eq!(outcome, UpsertOutcome::Inserted);
        assert_eq!(count_bridges(&pool).await.unwrap(), 1);
    }

    #[sqlx::test(migrator = "MIGRATOR")]
    async fn cursor_roundtrip(pool: PgPool) {
        // No row yet
        assert!(get_cursor(&pool, "live_swaps").await.unwrap().is_none());

        // Insert
        set_cursor(&pool, "live_swaps", BlockHeight(1000), "running")
            .await
            .unwrap();
        assert_eq!(
            get_cursor(&pool, "live_swaps").await.unwrap(),
            Some(BlockHeight(1000))
        );

        // Update (UPSERT)
        set_cursor(&pool, "live_swaps", BlockHeight(2000), "running")
            .await
            .unwrap();
        assert_eq!(
            get_cursor(&pool, "live_swaps").await.unwrap(),
            Some(BlockHeight(2000))
        );

        // Set error preserves cursor
        set_error(&pool, "live_swaps", "ws disconnect")
            .await
            .unwrap();
        assert_eq!(
            get_cursor(&pool, "live_swaps").await.unwrap(),
            Some(BlockHeight(2000))
        );
    }

    #[sqlx::test(migrator = "MIGRATOR")]
    async fn different_block_heights_coexist(pool: PgPool) {
        for block in [10, 11, 12] {
            insert_swap(&pool, &sample_swap(block)).await.unwrap();
        }
        assert_eq!(count_swaps(&pool).await.unwrap(), 3);
    }
}
