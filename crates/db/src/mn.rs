//! Minamoto (`mn.*` schema) typed query helpers.
//!
//! Writes are idempotent UPSERTs mirroring `minamoto/db.js`; reads
//! return the exact row shapes the `/api/minamoto/*` contract renders
//! (BIGINT and NUMERIC columns come back as text where the Node's `pg`
//! driver returned strings). Every query is `sqlx::query!` checked
//! against `.sqlx/`.

use crate::DbError;
use chrono::{DateTime, Utc};
use serde::Serialize;
use sorametrics_core::minamoto::{
    MetricSample, MnAccount, MnAsset, MnAssetDefinition, MnBlock, MnClaimMetadata, MnDomain,
    MnInstruction, MnNetworkState, MnPeer, MnTransaction,
};
use sqlx::types::BigDecimal;
use sqlx::PgPool;

fn check32(bytes: &[u8], what: &str) -> Result<(), DbError> {
    if bytes.len() == 32 {
        Ok(())
    } else {
        Err(DbError::Invalid(format!(
            "{what} must be 32 bytes, got {}",
            bytes.len()
        )))
    }
}

/// Validates decimal text. The text itself is bound and cast in SQL
/// (`$n::TEXT::NUMERIC`): binding a `BigDecimal` re-scales the value to
/// base-10000 digit groups (`3462.445388` → `3462.44538800`), and the
/// contract renders these columns as text.
fn numeric(text: &str, what: &str) -> Result<(), DbError> {
    text.parse::<BigDecimal>()
        .map(|_| ())
        .map_err(|e| DbError::Invalid(format!("{what} '{text}' is not numeric: {e}")))
}

// =============================================================
// network_state
// =============================================================

/// Replaces the single `mn.network_state` row.
pub async fn upsert_network_state(pool: &PgPool, s: &MnNetworkState) -> Result<(), DbError> {
    sqlx::query!(
        r#"
        INSERT INTO mn.network_state
            (id, peers, domains, accounts, assets,
             transactions_accepted, transactions_rejected,
             block_height, finalized_block,
             avg_commit_time_ms, avg_block_time_ms,
             last_block_at, iroha_version, updated_at)
        VALUES (1, $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, NOW())
        ON CONFLICT (id) DO UPDATE SET
            peers = EXCLUDED.peers,
            domains = EXCLUDED.domains,
            accounts = EXCLUDED.accounts,
            assets = EXCLUDED.assets,
            transactions_accepted = EXCLUDED.transactions_accepted,
            transactions_rejected = EXCLUDED.transactions_rejected,
            block_height = EXCLUDED.block_height,
            finalized_block = EXCLUDED.finalized_block,
            avg_commit_time_ms = EXCLUDED.avg_commit_time_ms,
            avg_block_time_ms = EXCLUDED.avg_block_time_ms,
            last_block_at = EXCLUDED.last_block_at,
            iroha_version = EXCLUDED.iroha_version,
            updated_at = NOW()
        "#,
        s.peers,
        s.domains,
        s.accounts,
        s.assets,
        s.transactions_accepted,
        s.transactions_rejected,
        s.block_height,
        s.finalized_block,
        s.avg_commit_time_ms,
        s.avg_block_time_ms,
        s.last_block_at,
        s.iroha_version
    )
    .execute(pool)
    .await?;
    Ok(())
}

/// Counts the indexer feeds into the `domains / accounts / assets`
/// columns of the network state (the explorer metrics route that the
/// Node read now requires an account signature).
pub struct IndexedCounts {
    /// `mn.domains` rows.
    pub domains: i32,
    /// `mn.accounts` rows.
    pub accounts: i32,
    /// `mn.assets` rows.
    pub assets: i32,
    /// Mean spacing of the last 100 blocks, ms (`None` with < 2 blocks).
    pub avg_block_ms: Option<i64>,
    /// Creation time of the highest block.
    pub last_block_at: Option<DateTime<Utc>>,
}

/// Reads the counts / timings derived from the indexed tables.
pub async fn indexed_counts(pool: &PgPool) -> Result<IndexedCounts, DbError> {
    let row = sqlx::query!(
        r#"
        SELECT
            (SELECT COUNT(*)::INT FROM mn.domains)  AS "domains!",
            (SELECT COUNT(*)::INT FROM mn.accounts) AS "accounts!",
            (SELECT COUNT(*)::INT FROM mn.assets)   AS "assets!",
            (WITH last100 AS (SELECT created_at FROM mn.blocks ORDER BY height DESC LIMIT 100),
                  diffs AS (SELECT EXTRACT(EPOCH FROM (LEAD(created_at) OVER (ORDER BY created_at) - created_at)) * 1000 AS ms FROM last100)
             SELECT AVG(ms)::FLOAT8 FROM diffs WHERE ms IS NOT NULL AND ms > 0) AS avg_block_ms,
            (SELECT created_at FROM mn.blocks ORDER BY height DESC LIMIT 1) AS last_block_at
        "#
    )
    .fetch_one(pool)
    .await?;
    Ok(IndexedCounts {
        domains: row.domains,
        accounts: row.accounts,
        assets: row.assets,
        avg_block_ms: row.avg_block_ms.map(|v| v.round() as i64),
        last_block_at: row.last_block_at,
    })
}

// =============================================================
// blocks
// =============================================================

/// UPSERTs a batch of blocks (`ON CONFLICT (height) DO UPDATE`).
pub async fn upsert_blocks(pool: &PgPool, blocks: &[MnBlock]) -> Result<u64, DbError> {
    if blocks.is_empty() {
        return Ok(0);
    }
    let mut heights = Vec::with_capacity(blocks.len());
    let mut hashes = Vec::with_capacity(blocks.len());
    let mut prevs: Vec<Option<Vec<u8>>> = Vec::with_capacity(blocks.len());
    let mut tx_hashes: Vec<Option<Vec<u8>>> = Vec::with_capacity(blocks.len());
    let mut created = Vec::with_capacity(blocks.len());
    let mut committed = Vec::with_capacity(blocks.len());
    let mut rejected = Vec::with_capacity(blocks.len());
    for b in blocks {
        check32(&b.hash, "block hash")?;
        if let Some(p) = &b.prev_hash {
            check32(p, "prev hash")?;
        }
        if let Some(t) = &b.transactions_hash {
            check32(t, "transactions hash")?;
        }
        heights.push(b.height);
        hashes.push(b.hash.clone());
        prevs.push(b.prev_hash.clone());
        tx_hashes.push(b.transactions_hash.clone());
        created.push(b.created_at);
        committed.push(b.transactions_committed);
        rejected.push(b.transactions_rejected);
    }
    let res = sqlx::query!(
        r#"
        INSERT INTO mn.blocks
            (height, hash, prev_hash, transactions_hash, created_at,
             transactions_committed, transactions_rejected)
        SELECT * FROM UNNEST($1::BIGINT[], $2::BYTEA[], $3::BYTEA[], $4::BYTEA[],
                             $5::TIMESTAMPTZ[], $6::INT[], $7::INT[])
        ON CONFLICT (height) DO UPDATE SET
            hash = EXCLUDED.hash,
            prev_hash = EXCLUDED.prev_hash,
            transactions_hash = EXCLUDED.transactions_hash,
            created_at = EXCLUDED.created_at,
            transactions_committed = EXCLUDED.transactions_committed,
            transactions_rejected = EXCLUDED.transactions_rejected
        "#,
        &heights,
        &hashes,
        &prevs as &[Option<Vec<u8>>],
        &tx_hashes as &[Option<Vec<u8>>],
        &created,
        &committed,
        &rejected
    )
    .execute(pool)
    .await?;
    Ok(res.rows_affected())
}

/// Highest indexed height and its hash.
pub async fn max_block(pool: &PgPool) -> Result<Option<(i64, Vec<u8>)>, DbError> {
    let row = sqlx::query!(r#"SELECT height, hash FROM mn.blocks ORDER BY height DESC LIMIT 1"#)
        .fetch_optional(pool)
        .await?;
    Ok(row.map(|r| (r.height, r.hash)))
}

/// Hash of the block at `height`, when indexed.
pub async fn block_hash_at(pool: &PgPool, height: i64) -> Result<Option<Vec<u8>>, DbError> {
    let row = sqlx::query!(r#"SELECT hash FROM mn.blocks WHERE height = $1"#, height)
        .fetch_optional(pool)
        .await?;
    Ok(row.map(|r| r.hash))
}

/// Heights in `[1, max]` with no row (gaps left by a partial page walk).
pub async fn missing_heights(pool: &PgPool, max: i64, limit: i64) -> Result<Vec<i64>, DbError> {
    let rows = sqlx::query!(
        r#"
        SELECT g.h AS "h!"
        FROM generate_series(1, $1::BIGINT) AS g(h)
        LEFT JOIN mn.blocks b ON b.height = g.h
        WHERE b.height IS NULL
        ORDER BY g.h DESC
        LIMIT $2
        "#,
        max,
        limit
    )
    .fetch_all(pool)
    .await?;
    Ok(rows.into_iter().map(|r| r.h).collect())
}

/// Number of indexed blocks.
pub async fn count_blocks(pool: &PgPool) -> Result<i64, DbError> {
    let row = sqlx::query!(r#"SELECT COUNT(*) AS "c!" FROM mn.blocks"#)
        .fetch_one(pool)
        .await?;
    Ok(row.c)
}

/// Drops every `mn.*` chain row (blocks cascade to transactions;
/// instructions are keyed by tx hash, so they are truncated too). Used
/// when the chain restarted from genesis with a different block #1.
pub async fn truncate_chain_tables(pool: &PgPool) -> Result<(), DbError> {
    sqlx::query!(r#"TRUNCATE mn.instructions, mn.transactions, mn.blocks"#)
        .execute(pool)
        .await?;
    Ok(())
}

// =============================================================
// transactions
// =============================================================

/// UPSERTs a batch of transactions. Rows whose block is not indexed yet
/// are skipped (FK), as the Node did; the count of skipped rows is
/// returned so the caller can log it.
pub async fn upsert_transactions(
    pool: &PgPool,
    txs: &[MnTransaction],
) -> Result<(u64, u64), DbError> {
    if txs.is_empty() {
        return Ok((0, 0));
    }
    let mut hashes = Vec::with_capacity(txs.len());
    let mut heights = Vec::with_capacity(txs.len());
    let mut authorities = Vec::with_capacity(txs.len());
    let mut created = Vec::with_capacity(txs.len());
    let mut kinds = Vec::with_capacity(txs.len());
    let mut statuses = Vec::with_capacity(txs.len());
    for t in txs {
        check32(&t.hash, "transaction hash")?;
        hashes.push(t.hash.clone());
        heights.push(t.block_height);
        authorities.push(t.authority.clone());
        created.push(t.created_at);
        kinds.push(t.executable_kind.clone());
        statuses.push(t.status.clone());
    }
    let res = sqlx::query!(
        r#"
        INSERT INTO mn.transactions
            (hash, block_height, authority, created_at, executable_kind, status)
        SELECT u.hash, u.block_height, u.authority, u.created_at, u.executable_kind, u.status
        FROM UNNEST($1::BYTEA[], $2::BIGINT[], $3::TEXT[], $4::TIMESTAMPTZ[], $5::TEXT[], $6::TEXT[])
             AS u(hash, block_height, authority, created_at, executable_kind, status)
        WHERE EXISTS (SELECT 1 FROM mn.blocks b WHERE b.height = u.block_height)
        ON CONFLICT (hash) DO UPDATE SET
            block_height = EXCLUDED.block_height,
            authority = EXCLUDED.authority,
            created_at = EXCLUDED.created_at,
            executable_kind = EXCLUDED.executable_kind,
            status = EXCLUDED.status
        "#,
        &hashes,
        &heights,
        &authorities,
        &created,
        &kinds,
        &statuses
    )
    .execute(pool)
    .await?;
    let written = res.rows_affected();
    Ok((written, txs.len() as u64 - written))
}

/// Stores the cross-chain claim metadata of one transaction.
pub async fn update_transaction_metadata(
    pool: &PgPool,
    hash: &[u8],
    meta: &MnClaimMetadata,
) -> Result<(), DbError> {
    check32(hash, "transaction hash")?;
    sqlx::query!(
        r#"
        UPDATE mn.transactions
        SET sora_v2_claim_tx_hash = $2,
            sora_nexus_claim_recipient = $3,
            fee_sponsor = $4
        WHERE hash = $1
        "#,
        hash,
        meta.sora_v2_claim_tx_hash,
        meta.sora_nexus_claim_recipient,
        meta.fee_sponsor
    )
    .execute(pool)
    .await?;
    Ok(())
}

/// Committed transactions without claim metadata yet, newest first.
pub async fn list_claims_to_enrich(pool: &PgPool, limit: i64) -> Result<Vec<Vec<u8>>, DbError> {
    let rows = sqlx::query!(
        r#"
        SELECT hash FROM mn.transactions
        WHERE sora_v2_claim_tx_hash IS NULL AND status = 'Committed'
        ORDER BY created_at DESC
        LIMIT $1
        "#,
        limit
    )
    .fetch_all(pool)
    .await?;
    Ok(rows.into_iter().map(|r| r.hash).collect())
}

/// Claims whose SORA v2 side (block + signer) is not resolved yet.
pub async fn list_claims_missing_v2(
    pool: &PgPool,
    limit: i64,
) -> Result<Vec<(Vec<u8>, String)>, DbError> {
    let rows = sqlx::query!(
        r#"
        SELECT hash, sora_v2_claim_tx_hash AS "v2_hash!"
        FROM mn.transactions
        WHERE sora_v2_claim_tx_hash IS NOT NULL AND sora_v2_signer IS NULL
        ORDER BY created_at DESC
        LIMIT $1
        "#,
        limit
    )
    .fetch_all(pool)
    .await?;
    Ok(rows.into_iter().map(|r| (r.hash, r.v2_hash)).collect())
}

/// Looks the burn extrinsic up in `sm.extrinsics` (read-only): block +
/// signer. Both `0x`-prefixed and bare hashes are accepted.
pub async fn lookup_v2_burn_extrinsic(
    pool: &PgPool,
    hash: &str,
) -> Result<Option<(i64, String)>, DbError> {
    let bare = hash.strip_prefix("0x").unwrap_or(hash);
    let candidates = vec![format!("0x{bare}"), bare.to_string()];
    let row = sqlx::query!(
        r#"
        SELECT block_height, signer FROM sm.extrinsics
        WHERE hash = ANY($1::TEXT[])
        LIMIT 1
        "#,
        &candidates
    )
    .fetch_optional(pool)
    .await?;
    Ok(row.map(|r| (r.block_height, r.signer)))
}

/// Stores the SORA v2 side of a claim.
pub async fn update_transaction_v2_side(
    pool: &PgPool,
    mn_hash: &[u8],
    v2_block: i64,
    v2_signer: &str,
) -> Result<(), DbError> {
    check32(mn_hash, "transaction hash")?;
    sqlx::query!(
        r#"UPDATE mn.transactions SET sora_v2_block = $2, sora_v2_signer = $3 WHERE hash = $1"#,
        mn_hash,
        v2_block,
        v2_signer
    )
    .execute(pool)
    .await?;
    Ok(())
}

// =============================================================
// accounts / domains / assets / definitions / peers
// =============================================================

/// UPSERTs one account (full projection; `last_seen_at = NOW()`).
pub async fn upsert_account(pool: &PgPool, a: &MnAccount) -> Result<(), DbError> {
    sqlx::query!(
        r#"
        INSERT INTO mn.accounts
            (id, network_prefix, has_primary_alias,
             primary_alias, primary_alias_dataspace, primary_alias_domain, primary_alias_name,
             multisig_quorum, multisig_signatories_count, metadata, last_seen_at)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, NOW())
        ON CONFLICT (id) DO UPDATE SET
            network_prefix = EXCLUDED.network_prefix,
            has_primary_alias = EXCLUDED.has_primary_alias,
            primary_alias = EXCLUDED.primary_alias,
            primary_alias_dataspace = EXCLUDED.primary_alias_dataspace,
            primary_alias_domain = EXCLUDED.primary_alias_domain,
            primary_alias_name = EXCLUDED.primary_alias_name,
            multisig_quorum = EXCLUDED.multisig_quorum,
            multisig_signatories_count = EXCLUDED.multisig_signatories_count,
            metadata = EXCLUDED.metadata,
            last_seen_at = NOW()
        "#,
        a.id,
        a.network_prefix,
        a.primary_alias.is_some(),
        a.primary_alias,
        a.primary_alias_dataspace,
        a.primary_alias_domain,
        a.primary_alias_name,
        a.multisig_quorum,
        a.multisig_signatories_count,
        a.metadata
    )
    .execute(pool)
    .await?;
    Ok(())
}

/// Inserts a bare account row if missing (owner referenced before seen).
pub async fn ensure_account_stub(pool: &PgPool, id: &str) -> Result<(), DbError> {
    sqlx::query!(
        r#"INSERT INTO mn.accounts (id) VALUES ($1) ON CONFLICT (id) DO NOTHING"#,
        id
    )
    .execute(pool)
    .await?;
    Ok(())
}

/// UPSERTs one domain.
pub async fn upsert_domain(pool: &PgPool, d: &MnDomain) -> Result<(), DbError> {
    sqlx::query!(
        r#"
        INSERT INTO mn.domains
            (id, owned_by, accounts_count, assets_count, nfts_count, metadata, updated_at)
        VALUES ($1, $2, $3, $4, $5, $6, NOW())
        ON CONFLICT (id) DO UPDATE SET
            owned_by = EXCLUDED.owned_by,
            accounts_count = EXCLUDED.accounts_count,
            assets_count = EXCLUDED.assets_count,
            nfts_count = EXCLUDED.nfts_count,
            metadata = EXCLUDED.metadata,
            updated_at = NOW()
        "#,
        d.id,
        d.owned_by,
        d.accounts_count,
        d.assets_count,
        d.nfts_count,
        d.metadata
    )
    .execute(pool)
    .await?;
    Ok(())
}

/// UPSERTs one balance row (creating the account stub first).
pub async fn upsert_asset(pool: &PgPool, a: &MnAsset) -> Result<(), DbError> {
    numeric(&a.value, "asset value")?;
    ensure_account_stub(pool, &a.account_id).await?;
    sqlx::query!(
        r#"
        INSERT INTO mn.assets (definition_id, account_id, value, updated_at)
        VALUES ($1, $2, $3::TEXT::NUMERIC, NOW())
        ON CONFLICT (definition_id, account_id) DO UPDATE SET
            value = EXCLUDED.value,
            updated_at = NOW()
        "#,
        a.definition_id,
        a.account_id,
        a.value
    )
    .execute(pool)
    .await?;
    Ok(())
}

/// UPSERTs one asset definition.
pub async fn upsert_asset_definition(pool: &PgPool, d: &MnAssetDefinition) -> Result<(), DbError> {
    if let Some(t) = &d.total_quantity {
        numeric(t, "total_quantity")?;
    }
    sqlx::query!(
        r#"
        INSERT INTO mn.asset_definitions
            (id, alias, name, description, owned_by, mintable,
             confidential_mode, balance_scope_policy, total_quantity, metadata, updated_at)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9::TEXT::NUMERIC, $10, NOW())
        ON CONFLICT (id) DO UPDATE SET
            alias = EXCLUDED.alias,
            name = EXCLUDED.name,
            description = EXCLUDED.description,
            owned_by = EXCLUDED.owned_by,
            mintable = EXCLUDED.mintable,
            confidential_mode = EXCLUDED.confidential_mode,
            balance_scope_policy = EXCLUDED.balance_scope_policy,
            total_quantity = EXCLUDED.total_quantity,
            metadata = EXCLUDED.metadata,
            updated_at = NOW()
        "#,
        d.id,
        d.alias,
        d.name,
        d.description,
        d.owned_by,
        d.mintable,
        d.confidential_mode,
        d.balance_scope_policy,
        d.total_quantity,
        d.metadata
    )
    .execute(pool)
    .await?;
    Ok(())
}

/// UPSERTs one peer as active.
pub async fn upsert_peer(pool: &PgPool, p: &MnPeer) -> Result<(), DbError> {
    sqlx::query!(
        r#"
        INSERT INTO mn.peers (multiaddr, public_key, ip_address, port, last_seen_at, is_active)
        VALUES ($1, $2, $3, $4, NOW(), TRUE)
        ON CONFLICT (multiaddr) DO UPDATE SET
            public_key = EXCLUDED.public_key,
            ip_address = EXCLUDED.ip_address,
            port = EXCLUDED.port,
            last_seen_at = NOW(),
            is_active = TRUE
        "#,
        p.multiaddr,
        p.public_key,
        p.ip_address,
        p.port
    )
    .execute(pool)
    .await?;
    Ok(())
}

/// Marks every active peer not in `current` as inactive.
pub async fn deactivate_stale_peers(pool: &PgPool, current: &[String]) -> Result<u64, DbError> {
    let res = sqlx::query!(
        r#"UPDATE mn.peers SET is_active = FALSE WHERE is_active = TRUE AND multiaddr <> ALL($1::TEXT[])"#,
        current
    )
    .execute(pool)
    .await?;
    Ok(res.rows_affected())
}

// =============================================================
// instructions
// =============================================================

/// UPSERTs a batch of instructions keyed by `(transaction_hash, index)`.
pub async fn upsert_instructions(pool: &PgPool, rows: &[MnInstruction]) -> Result<u64, DbError> {
    if rows.is_empty() {
        return Ok(0);
    }
    let mut hashes = Vec::with_capacity(rows.len());
    let mut indexes = Vec::with_capacity(rows.len());
    let mut heights = Vec::with_capacity(rows.len());
    let mut authorities = Vec::with_capacity(rows.len());
    let mut kinds = Vec::with_capacity(rows.len());
    let mut payloads = Vec::with_capacity(rows.len());
    let mut statuses = Vec::with_capacity(rows.len());
    let mut created = Vec::with_capacity(rows.len());
    for r in rows {
        check32(&r.transaction_hash, "transaction hash")?;
        hashes.push(r.transaction_hash.clone());
        indexes.push(r.instruction_index);
        heights.push(r.block_height);
        authorities.push(r.authority.clone());
        kinds.push(r.kind.clone());
        payloads.push(r.payload.clone());
        statuses.push(r.transaction_status.clone());
        created.push(r.created_at);
    }
    let res = sqlx::query!(
        r#"
        INSERT INTO mn.instructions
            (transaction_hash, instruction_index, block_height, authority,
             kind, payload, transaction_status, created_at)
        SELECT * FROM UNNEST($1::BYTEA[], $2::INT[], $3::BIGINT[], $4::TEXT[],
                             $5::TEXT[], $6::JSONB[], $7::TEXT[], $8::TIMESTAMPTZ[])
        ON CONFLICT (transaction_hash, instruction_index) DO UPDATE SET
            block_height = EXCLUDED.block_height,
            authority = EXCLUDED.authority,
            kind = EXCLUDED.kind,
            payload = EXCLUDED.payload,
            transaction_status = EXCLUDED.transaction_status,
            created_at = EXCLUDED.created_at
        "#,
        &hashes,
        &indexes,
        &heights,
        &authorities,
        &kinds,
        &payloads,
        &statuses,
        &created
    )
    .execute(pool)
    .await?;
    Ok(res.rows_affected())
}

// =============================================================
// metrics_snapshots
// =============================================================

/// Bulk-inserts samples at `ts` (histogram `_bucket` series dropped, as
/// the Node did: 62 % of rows, unused by the UI).
pub async fn insert_metric_samples(
    pool: &PgPool,
    samples: &[MetricSample],
    ts: DateTime<Utc>,
) -> Result<u64, DbError> {
    let kept: Vec<&MetricSample> = samples
        .iter()
        .filter(|s| !s.metric_name.ends_with("_bucket"))
        .collect();
    if kept.is_empty() {
        return Ok(0);
    }
    let mut total = 0u64;
    for chunk in kept.chunks(500) {
        let names: Vec<String> = chunk.iter().map(|s| s.metric_name.clone()).collect();
        let labels: Vec<serde_json::Value> = chunk.iter().map(|s| s.labels.clone()).collect();
        let values: Vec<f64> = chunk.iter().map(|s| s.value).collect();
        let res = sqlx::query!(
            r#"
            INSERT INTO mn.metrics_snapshots (ts, metric_name, labels, value)
            SELECT $1, * FROM UNNEST($2::TEXT[], $3::JSONB[], $4::FLOAT8[])
            "#,
            ts,
            &names,
            &labels,
            &values
        )
        .execute(pool)
        .await?;
        total += res.rows_affected();
    }
    Ok(total)
}

/// Deletes samples older than `retention_days`.
pub async fn prune_metric_samples(pool: &PgPool, retention_days: i32) -> Result<u64, DbError> {
    let res = sqlx::query!(
        r#"DELETE FROM mn.metrics_snapshots WHERE ts < NOW() - make_interval(days => $1)"#,
        retention_days
    )
    .execute(pool)
    .await?;
    Ok(res.rows_affected())
}

// =============================================================
// indexer_state
// =============================================================

/// Records one job run (`error_count` resets on `ok`, increments on error).
pub async fn record_indexer_run(
    pool: &PgPool,
    name: &str,
    ok: bool,
    last_value: &serde_json::Value,
    error: Option<&str>,
) -> Result<(), DbError> {
    let status = if ok { "ok" } else { "error" };
    sqlx::query!(
        r#"
        INSERT INTO mn.indexer_state (name, last_value, last_run_at, last_run_status, error_count, last_error)
        VALUES ($1, $2, NOW(), $3, CASE WHEN $3 = 'ok' THEN 0 ELSE 1 END, $4)
        ON CONFLICT (name) DO UPDATE SET
            last_value = EXCLUDED.last_value,
            last_run_at = NOW(),
            last_run_status = EXCLUDED.last_run_status,
            error_count = CASE WHEN EXCLUDED.last_run_status = 'ok' THEN 0
                               ELSE mn.indexer_state.error_count + 1 END,
            last_error = EXCLUDED.last_error
        "#,
        name,
        last_value,
        status,
        error
    )
    .execute(pool)
    .await?;
    Ok(())
}

/// `mn.indexer_state` row as `/api/minamoto/indexer/state` renders it.
#[derive(Clone, Debug, Serialize)]
pub struct IndexerStateRow {
    /// Job name.
    pub name: String,
    /// Last run time.
    pub last_run_at: DateTime<Utc>,
    /// `ok` | `error`.
    pub last_run_status: Option<String>,
    /// Consecutive errors.
    pub error_count: i32,
    /// Last error message.
    pub last_error: Option<String>,
    /// Last successful result.
    pub last_value: serde_json::Value,
}

/// All job rows ordered by name.
pub async fn list_indexer_state(pool: &PgPool) -> Result<Vec<IndexerStateRow>, DbError> {
    let rows = sqlx::query!(
        r#"SELECT name, last_run_at, last_run_status, error_count, last_error, last_value FROM mn.indexer_state ORDER BY name"#
    )
    .fetch_all(pool)
    .await?;
    Ok(rows
        .into_iter()
        .map(|r| IndexerStateRow {
            name: r.name,
            last_run_at: r.last_run_at,
            last_run_status: r.last_run_status,
            error_count: r.error_count,
            last_error: r.last_error,
            last_value: r.last_value,
        })
        .collect())
}

// =============================================================
// Read side — `/api/minamoto/*` row shapes
// =============================================================

// Text orderings that the Node left to the database collation
// (`ORDER BY name`) use the ICU `en-US` collation explicitly: the
// production database sorts with glibc `en_US.utf8` (case-insensitive
// at the first level), while the Alpine/musl images used for v33 make
// `en_US.utf8` behave like `C`.

/// Serde helpers reproducing node-pg + `JSON.stringify` rendering.
pub mod ser {
    use chrono::{DateTime, SecondsFormat, Utc};
    use serde::Serializer;

    /// `Date.toISOString()`: always 3 fraction digits, `Z`.
    pub fn iso_ms<S: Serializer>(t: &DateTime<Utc>, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(&t.to_rfc3339_opts(SecondsFormat::Millis, true))
    }

    /// Optional variant of [`iso_ms`].
    pub fn iso_ms_opt<S: Serializer>(t: &Option<DateTime<Utc>>, s: S) -> Result<S::Ok, S::Error> {
        match t {
            Some(t) => iso_ms(t, s),
            None => s.serialize_none(),
        }
    }

    /// node-pg returns `BIGINT` as a decimal string.
    pub fn bigint_text<S: Serializer>(v: &i64, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(&v.to_string())
    }
}

/// Text of a 32-byte hash as the Node rendered it (`byteaToHex`).
pub fn hex_of(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn pages(total: i64, per_page: i64) -> i64 {
    std::cmp::max(1, (total + per_page - 1) / per_page)
}

/// `{ page, per_page, total, total_pages, items }` envelope.
#[derive(Clone, Debug, Serialize)]
pub struct Page<T: Serialize> {
    /// Requested page (1-based).
    pub page: i64,
    /// Page size.
    pub per_page: i64,
    /// Total matching rows.
    pub total: i64,
    /// `max(1, ceil(total / per_page))`.
    pub total_pages: i64,
    /// Rows.
    pub items: Vec<T>,
}

impl<T: Serialize> Page<T> {
    fn new(page: i64, per_page: i64, total: i64, items: Vec<T>) -> Self {
        Self {
            page,
            per_page,
            total,
            total_pages: pages(total, per_page),
            items,
        }
    }
}

/// `mn.network_state` as `/network-state.state` renders it.
#[derive(Clone, Debug, Serialize)]
pub struct NetworkStateRow {
    /// Always 1.
    pub id: i32,
    /// Peers.
    pub peers: i32,
    /// Domains.
    pub domains: i32,
    /// Accounts.
    pub accounts: i32,
    /// Assets.
    pub assets: i32,
    /// BIGINT → text.
    #[serde(serialize_with = "ser::bigint_text")]
    pub transactions_accepted: i64,
    /// BIGINT → text.
    #[serde(serialize_with = "ser::bigint_text")]
    pub transactions_rejected: i64,
    /// BIGINT → text.
    #[serde(serialize_with = "ser::bigint_text")]
    pub block_height: i64,
    /// BIGINT → text.
    #[serde(serialize_with = "ser::bigint_text")]
    pub finalized_block: i64,
    /// Latest commit latency.
    pub avg_commit_time_ms: i32,
    /// BIGINT → text.
    #[serde(serialize_with = "ser::bigint_text")]
    pub avg_block_time_ms: i64,
    /// Creation time of the latest block.
    #[serde(serialize_with = "ser::iso_ms_opt")]
    pub last_block_at: Option<DateTime<Utc>>,
    /// Node version.
    pub iroha_version: Option<String>,
    /// Last refresh.
    #[serde(serialize_with = "ser::iso_ms")]
    pub updated_at: DateTime<Utc>,
}

/// The single network-state row, if the indexer wrote it.
pub async fn get_network_state(pool: &PgPool) -> Result<Option<NetworkStateRow>, DbError> {
    let row = sqlx::query!(r#"SELECT * FROM mn.network_state WHERE id = 1"#)
        .fetch_optional(pool)
        .await?;
    Ok(row.map(|r| NetworkStateRow {
        id: r.id,
        peers: r.peers,
        domains: r.domains,
        accounts: r.accounts,
        assets: r.assets,
        transactions_accepted: r.transactions_accepted,
        transactions_rejected: r.transactions_rejected,
        block_height: r.block_height,
        finalized_block: r.finalized_block,
        avg_commit_time_ms: r.avg_commit_time_ms,
        avg_block_time_ms: r.avg_block_time_ms,
        last_block_at: r.last_block_at,
        iroha_version: r.iroha_version,
        updated_at: r.updated_at,
    }))
}

/// `/blocks` item.
#[derive(Clone, Debug, Serialize)]
pub struct BlockRow {
    /// Height as a JS number.
    pub height: i64,
    /// Hash, hex.
    pub hash: String,
    /// Previous hash, hex.
    pub prev_hash: Option<String>,
    /// Transactions merkle root, hex.
    pub transactions_hash: Option<String>,
    /// Creation time.
    #[serde(serialize_with = "ser::iso_ms")]
    pub created_at: DateTime<Utc>,
    /// Transactions in the block.
    pub transactions_committed: i32,
    /// Rejected transactions.
    pub transactions_rejected: i32,
    /// Index time.
    #[serde(serialize_with = "ser::iso_ms")]
    pub indexed_at: DateTime<Utc>,
}

/// `/blocks?page&per_page`, newest first.
pub async fn list_blocks(
    pool: &PgPool,
    page: i64,
    per_page: i64,
) -> Result<Page<BlockRow>, DbError> {
    let offset = (page.max(1) - 1) * per_page;
    let total = sqlx::query!(r#"SELECT COUNT(*) AS "c!" FROM mn.blocks"#)
        .fetch_one(pool)
        .await?
        .c;
    let rows = sqlx::query!(
        r#"
        SELECT height, hash, prev_hash, transactions_hash, created_at,
               transactions_committed, transactions_rejected, indexed_at
        FROM mn.blocks ORDER BY height DESC LIMIT $1 OFFSET $2
        "#,
        per_page,
        offset
    )
    .fetch_all(pool)
    .await?;
    let items = rows
        .into_iter()
        .map(|r| BlockRow {
            height: r.height,
            hash: hex_of(&r.hash),
            prev_hash: r.prev_hash.as_deref().map(hex_of),
            transactions_hash: r.transactions_hash.as_deref().map(hex_of),
            created_at: r.created_at,
            transactions_committed: r.transactions_committed,
            transactions_rejected: r.transactions_rejected,
            indexed_at: r.indexed_at,
        })
        .collect();
    Ok(Page::new(page, per_page, total, items))
}

/// `/blocks/stats`.
#[derive(Clone, Debug, Serialize)]
pub struct BlocksStats {
    /// Indexed blocks.
    pub total: i32,
    /// Highest height.
    pub latest_height: Option<i32>,
    /// Blocks created in the last 24 h.
    pub blocks_24h: i32,
    /// Transactions created in the last 24 h.
    pub tx_24h: i32,
    /// 24 h blocks without a transaction.
    pub empty_24h: i32,
    /// Mean spacing of the last 100 blocks, ms.
    pub avg_block_ms: Option<i64>,
}

/// `/blocks/stats`.
pub async fn get_blocks_stats(pool: &PgPool) -> Result<BlocksStats, DbError> {
    let r = sqlx::query!(
        r#"
        SELECT COUNT(*)::INT AS "total!", MAX(height)::INT AS latest_height,
               COUNT(*) FILTER (WHERE created_at >= NOW() - INTERVAL '24 hours')::INT AS "blocks_24h!"
        FROM mn.blocks
        "#
    )
    .fetch_one(pool)
    .await?;
    let t = sqlx::query!(
        r#"
        SELECT COUNT(*)::INT AS "tx_24h!", COUNT(DISTINCT block_height)::INT AS "blocks_with_tx_24h!"
        FROM mn.transactions WHERE created_at >= NOW() - INTERVAL '24 hours'
        "#
    )
    .fetch_one(pool)
    .await?;
    let avg = sqlx::query!(
        r#"
        WITH last100 AS (SELECT created_at FROM mn.blocks ORDER BY height DESC LIMIT 100),
             diffs AS (SELECT EXTRACT(EPOCH FROM (LEAD(created_at) OVER (ORDER BY created_at) - created_at)) * 1000 AS ms FROM last100)
        SELECT AVG(ms)::FLOAT8 AS avg_ms FROM diffs WHERE ms IS NOT NULL AND ms > 0
        "#
    )
    .fetch_one(pool)
    .await?;
    Ok(BlocksStats {
        total: r.total,
        latest_height: r.latest_height,
        blocks_24h: r.blocks_24h,
        tx_24h: t.tx_24h,
        empty_24h: (r.blocks_24h - t.blocks_with_tx_24h).max(0),
        avg_block_ms: avg.avg_ms.map(|v| v.round() as i64),
    })
}

/// `/transactions` item.
#[derive(Clone, Debug, Serialize)]
pub struct TransactionRow {
    /// Hash, hex.
    pub hash: String,
    /// Block height as a JS number.
    pub block: i64,
    /// Signing account.
    pub authority: String,
    /// Creation time.
    #[serde(serialize_with = "ser::iso_ms")]
    pub created_at: DateTime<Utc>,
    /// Executable kind.
    pub executable: String,
    /// Status.
    pub status: String,
    /// Index time.
    #[serde(serialize_with = "ser::iso_ms")]
    pub indexed_at: DateTime<Utc>,
}

/// Filters of `/transactions`.
#[derive(Clone, Debug, Default)]
pub struct TxListFilter {
    /// `status = $`.
    pub status: Option<String>,
    /// `block_height = $`.
    pub block: Option<i64>,
    /// `authority = $`.
    pub authority: Option<String>,
}

/// `/transactions?page&per_page&status&block&authority`, newest first.
pub async fn list_transactions(
    pool: &PgPool,
    page: i64,
    per_page: i64,
    f: &TxListFilter,
) -> Result<Page<TransactionRow>, DbError> {
    let offset = (page.max(1) - 1) * per_page;
    let total = sqlx::query!(
        r#"
        SELECT COUNT(*) AS "c!" FROM mn.transactions
        WHERE ($1::TEXT IS NULL OR status = $1)
          AND ($2::BIGINT IS NULL OR block_height = $2)
          AND ($3::TEXT IS NULL OR authority = $3)
        "#,
        f.status,
        f.block,
        f.authority
    )
    .fetch_one(pool)
    .await?
    .c;
    let rows = sqlx::query!(
        r#"
        SELECT hash, block_height, authority, created_at, executable_kind, status, indexed_at
        FROM mn.transactions
        WHERE ($1::TEXT IS NULL OR status = $1)
          AND ($2::BIGINT IS NULL OR block_height = $2)
          AND ($3::TEXT IS NULL OR authority = $3)
        ORDER BY created_at DESC
        LIMIT $4 OFFSET $5
        "#,
        f.status,
        f.block,
        f.authority,
        per_page,
        offset
    )
    .fetch_all(pool)
    .await?;
    let items = rows
        .into_iter()
        .map(|r| TransactionRow {
            hash: hex_of(&r.hash),
            block: r.block_height,
            authority: r.authority,
            created_at: r.created_at,
            executable: r.executable_kind,
            status: r.status,
            indexed_at: r.indexed_at,
        })
        .collect();
    Ok(Page::new(page, per_page, total, items))
}

/// `/transactions/stats`.
pub async fn get_transactions_stats(pool: &PgPool) -> Result<serde_json::Value, DbError> {
    let r = sqlx::query!(
        r#"
        SELECT COUNT(*)::INT AS "total!",
               COALESCE(SUM(CASE WHEN status = 'Committed' THEN 1 ELSE 0 END), 0)::INT AS "committed!",
               COUNT(*) FILTER (WHERE created_at >= NOW() - INTERVAL '24 hours')::INT AS "total_24h!",
               COALESCE(SUM(CASE WHEN status = 'Committed' AND created_at >= NOW() - INTERVAL '24 hours' THEN 1 ELSE 0 END), 0)::INT AS "committed_24h!",
               COUNT(DISTINCT authority)::INT AS "unique_signers!"
        FROM mn.transactions
        "#
    )
    .fetch_one(pool)
    .await?;
    let top = sqlx::query!(
        r#"SELECT authority, COUNT(*)::INT AS "c!" FROM mn.transactions GROUP BY authority ORDER BY COUNT(*) DESC LIMIT 1"#
    )
    .fetch_optional(pool)
    .await?;
    let rate = |num: i32, den: i32| (den > 0).then(|| num as f64 / den as f64);
    Ok(serde_json::json!({
        "total": r.total,
        "committed": r.committed,
        "success_rate": rate(r.committed, r.total),
        "total_24h": r.total_24h,
        "success_rate_24h": rate(r.committed_24h, r.total_24h),
        "unique_signers": r.unique_signers,
        "top_authority": top.map(|t| serde_json::json!({"authority": t.authority, "count": t.c})),
    }))
}

/// `/transactions/fee-sponsorship`.
pub async fn get_fee_sponsorship_stats(pool: &PgPool) -> Result<serde_json::Value, DbError> {
    let r = sqlx::query!(
        r#"
        SELECT COUNT(*)::INT AS "total_tx!",
               COUNT(*) FILTER (WHERE fee_sponsor IS NOT NULL)::INT AS "sponsored!",
               COUNT(*) FILTER (WHERE fee_sponsor IS NOT NULL AND created_at >= NOW() - INTERVAL '24 hours')::INT AS "sponsored_24h!",
               COUNT(DISTINCT fee_sponsor)::INT AS "distinct_sponsors!",
               COUNT(DISTINCT authority) FILTER (WHERE fee_sponsor IS NOT NULL)::INT AS "distinct_sponsored_signers!"
        FROM mn.transactions
        "#
    )
    .fetch_one(pool)
    .await?;
    let top = sqlx::query!(
        r#"SELECT fee_sponsor AS "sponsor!", COUNT(*)::INT AS "count!" FROM mn.transactions WHERE fee_sponsor IS NOT NULL GROUP BY fee_sponsor ORDER BY COUNT(*) DESC LIMIT 10"#
    )
    .fetch_all(pool)
    .await?;
    let top: Vec<serde_json::Value> = top
        .into_iter()
        .map(|t| serde_json::json!({"sponsor": t.sponsor, "count": t.count}))
        .collect();
    Ok(serde_json::json!({
        "total_tx": r.total_tx,
        "sponsored": r.sponsored,
        "sponsored_24h": r.sponsored_24h,
        "distinct_sponsors": r.distinct_sponsors,
        "distinct_sponsored_signers": r.distinct_sponsored_signers,
        "top_sponsors": top,
    }))
}

/// `/accounts` item (every column of `mn.accounts`).
#[derive(Clone, Debug, Serialize)]
pub struct AccountRow {
    /// Account id.
    pub id: String,
    /// Network prefix.
    pub network_prefix: i32,
    /// Alias flag.
    pub has_primary_alias: bool,
    /// Alias.
    pub primary_alias: Option<String>,
    /// Alias dataspace.
    pub primary_alias_dataspace: Option<String>,
    /// Alias domain.
    pub primary_alias_domain: Option<String>,
    /// Alias name.
    pub primary_alias_name: Option<String>,
    /// Multisig quorum.
    pub multisig_quorum: Option<i32>,
    /// Multisig signatories.
    pub multisig_signatories_count: Option<i32>,
    /// Metadata.
    pub metadata: serde_json::Value,
    /// First seen.
    #[serde(serialize_with = "ser::iso_ms")]
    pub first_seen_at: DateTime<Utc>,
    /// Last seen.
    #[serde(serialize_with = "ser::iso_ms")]
    pub last_seen_at: DateTime<Utc>,
}

/// `/accounts?page&per_page`, most recently seen first.
pub async fn list_accounts(
    pool: &PgPool,
    page: i64,
    per_page: i64,
) -> Result<Page<AccountRow>, DbError> {
    let offset = (page.max(1) - 1) * per_page;
    let total = sqlx::query!(r#"SELECT COUNT(*) AS "c!" FROM mn.accounts"#)
        .fetch_one(pool)
        .await?
        .c;
    let rows = sqlx::query!(
        r#"
        SELECT id, network_prefix, has_primary_alias, primary_alias, primary_alias_dataspace,
               primary_alias_domain, primary_alias_name, multisig_quorum, multisig_signatories_count,
               metadata, first_seen_at, last_seen_at
        FROM mn.accounts ORDER BY last_seen_at DESC LIMIT $1 OFFSET $2
        "#,
        per_page,
        offset
    )
    .fetch_all(pool)
    .await?;
    let items = rows
        .into_iter()
        .map(|r| AccountRow {
            id: r.id,
            network_prefix: r.network_prefix,
            has_primary_alias: r.has_primary_alias,
            primary_alias: r.primary_alias,
            primary_alias_dataspace: r.primary_alias_dataspace,
            primary_alias_domain: r.primary_alias_domain,
            primary_alias_name: r.primary_alias_name,
            multisig_quorum: r.multisig_quorum,
            multisig_signatories_count: r.multisig_signatories_count,
            metadata: r.metadata,
            first_seen_at: r.first_seen_at,
            last_seen_at: r.last_seen_at,
        })
        .collect();
    Ok(Page::new(page, per_page, total, items))
}

/// `/accounts/stats`.
pub async fn get_accounts_stats(pool: &PgPool) -> Result<serde_json::Value, DbError> {
    let r = sqlx::query!(
        r#"
        SELECT COUNT(*)::INT AS "total!",
               COUNT(*) FILTER (WHERE multisig_quorum IS NOT NULL)::INT AS "multisig!",
               COUNT(*) FILTER (WHERE has_primary_alias)::INT AS "aliased!",
               COUNT(*) FILTER (WHERE last_seen_at >= NOW() - INTERVAL '24 hours')::INT AS "active_24h!",
               COUNT(*) FILTER (WHERE first_seen_at >= NOW() - INTERVAL '7 days')::INT AS "new_7d!",
               MAX(last_seen_at) AS last_activity
        FROM mn.accounts
        "#
    )
    .fetch_one(pool)
    .await?;
    Ok(serde_json::json!({
        "total": r.total,
        "multisig": r.multisig,
        "aliased": r.aliased,
        "active_24h": r.active_24h,
        "new_7d": r.new_7d,
        "last_activity": r.last_activity.map(|t| t.to_rfc3339_opts(chrono::SecondsFormat::Millis, true)),
    }))
}

/// `/domains` item.
#[derive(Clone, Debug, Serialize)]
pub struct DomainRow {
    /// Domain id.
    pub id: String,
    /// Owner.
    pub owned_by: String,
    /// Accounts.
    pub accounts_count: i32,
    /// Asset definitions.
    pub assets_count: i32,
    /// NFTs.
    pub nfts_count: i32,
    /// Metadata.
    pub metadata: serde_json::Value,
    /// Last update.
    #[serde(serialize_with = "ser::iso_ms")]
    pub updated_at: DateTime<Utc>,
}

/// `/domains` (all, by id).
pub async fn list_domains(pool: &PgPool) -> Result<Vec<DomainRow>, DbError> {
    let rows = sqlx::query!(
        r#"SELECT id, owned_by, accounts_count, assets_count, nfts_count, metadata, updated_at FROM mn.domains ORDER BY id"#
    )
    .fetch_all(pool)
    .await?;
    Ok(rows
        .into_iter()
        .map(|r| DomainRow {
            id: r.id,
            owned_by: r.owned_by,
            accounts_count: r.accounts_count,
            assets_count: r.assets_count,
            nfts_count: r.nfts_count,
            metadata: r.metadata,
            updated_at: r.updated_at,
        })
        .collect())
}

/// Domains owned by `owner` (wallet info section).
pub async fn domains_owned_by(
    pool: &PgPool,
    owner: &str,
) -> Result<Vec<serde_json::Value>, DbError> {
    let rows = sqlx::query!(
        r#"SELECT id, accounts_count, assets_count, nfts_count, updated_at FROM mn.domains WHERE owned_by = $1 ORDER BY id"#,
        owner
    )
    .fetch_all(pool)
    .await?;
    Ok(rows
        .into_iter()
        .map(|r| {
            serde_json::json!({
                "id": r.id,
                "accounts_count": r.accounts_count,
                "assets_count": r.assets_count,
                "nfts_count": r.nfts_count,
                "updated_at": r.updated_at.to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
            })
        })
        .collect())
}

/// `/domains/stats`.
pub async fn get_domains_stats(pool: &PgPool) -> Result<serde_json::Value, DbError> {
    let r = sqlx::query!(
        r#"
        SELECT COUNT(*)::INT AS "total!",
               COALESCE(SUM(accounts_count), 0)::INT AS "accounts_total!",
               COALESCE(SUM(assets_count), 0)::INT AS "assets_total!",
               COALESCE(SUM(nfts_count), 0)::INT AS "nfts_total!"
        FROM mn.domains
        "#
    )
    .fetch_one(pool)
    .await?;
    let by_accounts = sqlx::query!(
        r#"SELECT id, accounts_count FROM mn.domains ORDER BY accounts_count DESC NULLS LAST LIMIT 1"#
    )
    .fetch_optional(pool)
    .await?;
    let by_assets = sqlx::query!(
        r#"SELECT id, assets_count FROM mn.domains ORDER BY assets_count DESC NULLS LAST LIMIT 1"#
    )
    .fetch_optional(pool)
    .await?;
    Ok(serde_json::json!({
        "total": r.total,
        "accounts_total": r.accounts_total,
        "assets_total": r.assets_total,
        "nfts_total": r.nfts_total,
        "largest_by_accounts": by_accounts.map(|d| serde_json::json!({"id": d.id, "accounts_count": d.accounts_count})),
        "largest_by_assets": by_assets.map(|d| serde_json::json!({"id": d.id, "assets_count": d.assets_count})),
    }))
}

/// `/assets` item (balance + definition labels).
#[derive(Clone, Debug, Serialize)]
pub struct AssetRow {
    /// Definition id.
    pub definition_id: String,
    /// Holder.
    pub account_id: String,
    /// Balance, decimal text.
    pub value: String,
    /// Last update.
    #[serde(serialize_with = "ser::iso_ms")]
    pub updated_at: DateTime<Utc>,
    /// Definition alias.
    pub alias: Option<String>,
    /// Definition name.
    pub name: Option<String>,
}

/// `/assets?page&per_page`, most recently updated first.
pub async fn list_assets(
    pool: &PgPool,
    page: i64,
    per_page: i64,
) -> Result<Page<AssetRow>, DbError> {
    let offset = (page.max(1) - 1) * per_page;
    let total = sqlx::query!(r#"SELECT COUNT(*) AS "c!" FROM mn.assets"#)
        .fetch_one(pool)
        .await?
        .c;
    let rows = sqlx::query!(
        r#"
        SELECT a.definition_id, a.account_id, a.value::TEXT AS "value!", a.updated_at, d.alias, d.name
        FROM mn.assets a
        LEFT JOIN mn.asset_definitions d ON d.id = a.definition_id
        ORDER BY a.updated_at DESC LIMIT $1 OFFSET $2
        "#,
        per_page,
        offset
    )
    .fetch_all(pool)
    .await?;
    let items = rows
        .into_iter()
        .map(|r| AssetRow {
            definition_id: r.definition_id,
            account_id: r.account_id,
            value: r.value,
            updated_at: r.updated_at,
            alias: r.alias,
            name: r.name,
        })
        .collect();
    Ok(Page::new(page, per_page, total, items))
}

/// Balances above zero held by `account` (wallet info).
pub async fn assets_held_count(pool: &PgPool, account: &str) -> Result<i32, DbError> {
    let r = sqlx::query!(
        r#"SELECT COUNT(*)::INT AS "c!" FROM mn.assets WHERE account_id = $1 AND value > 0"#,
        account
    )
    .fetch_one(pool)
    .await?;
    Ok(r.c)
}

/// `/asset-definitions` item.
#[derive(Clone, Debug, Serialize)]
pub struct AssetDefinitionRow {
    /// Definition id.
    pub id: String,
    /// Alias.
    pub alias: Option<String>,
    /// Name.
    pub name: Option<String>,
    /// Description.
    pub description: Option<String>,
    /// Owner.
    pub owned_by: String,
    /// Mintable policy.
    pub mintable: Option<String>,
    /// Confidential mode.
    pub confidential_mode: Option<String>,
    /// Balance scope policy.
    pub balance_scope_policy: Option<String>,
    /// Total minted, decimal text.
    pub total_quantity: Option<String>,
    /// Metadata.
    pub metadata: serde_json::Value,
    /// Last update.
    #[serde(serialize_with = "ser::iso_ms")]
    pub updated_at: DateTime<Utc>,
    /// Balance rows referencing the definition.
    pub holders: i32,
    /// Sum of indexed balances, decimal text.
    pub held_supply: String,
}

/// `/asset-definitions` (XOR first, then by name).
pub async fn list_asset_definitions(pool: &PgPool) -> Result<Vec<AssetDefinitionRow>, DbError> {
    let rows = sqlx::query!(
        r#"
        SELECT d.id, d.alias, d.name, d.description, d.owned_by, d.mintable,
               d.confidential_mode, d.balance_scope_policy, d.total_quantity::TEXT AS total_quantity,
               d.metadata, d.updated_at,
               COALESCE((SELECT COUNT(*)::INT FROM mn.assets a WHERE a.definition_id = d.id), 0) AS "holders!",
               COALESCE((SELECT SUM(a.value) FROM mn.assets a WHERE a.definition_id = d.id), 0)::TEXT AS "held_supply!"
        FROM mn.asset_definitions d
        ORDER BY (CASE WHEN d.alias = 'xor#universal' THEN 0 WHEN d.name ILIKE 'xor' THEN 0 ELSE 1 END), d.name COLLATE "en-US-x-icu"
        "#
    )
    .fetch_all(pool)
    .await?;
    Ok(rows
        .into_iter()
        .map(|r| AssetDefinitionRow {
            id: r.id,
            alias: r.alias,
            name: r.name,
            description: r.description,
            owned_by: r.owned_by,
            mintable: r.mintable,
            confidential_mode: r.confidential_mode,
            balance_scope_policy: r.balance_scope_policy,
            total_quantity: r.total_quantity,
            metadata: r.metadata,
            updated_at: r.updated_at,
            holders: r.holders,
            held_supply: r.held_supply,
        })
        .collect())
}

/// Definitions owned by `owner` (wallet info section).
pub async fn asset_definitions_owned_by(
    pool: &PgPool,
    owner: &str,
) -> Result<Vec<serde_json::Value>, DbError> {
    let rows = sqlx::query!(
        r#"SELECT id, alias, name, total_quantity::TEXT AS total_quantity, confidential_mode FROM mn.asset_definitions WHERE owned_by = $1 ORDER BY name COLLATE "en-US-x-icu" NULLS LAST"#,
        owner
    )
    .fetch_all(pool)
    .await?;
    Ok(rows
        .into_iter()
        .map(|r| {
            serde_json::json!({
                "id": r.id, "alias": r.alias, "name": r.name,
                "total_quantity": r.total_quantity, "confidential_mode": r.confidential_mode,
            })
        })
        .collect())
}

/// `/asset/:idOrAlias` — alias, id or case-insensitive name.
pub async fn get_asset_supply(
    pool: &PgPool,
    needle: &str,
) -> Result<Option<serde_json::Value>, DbError> {
    let r = sqlx::query!(
        r#"
        SELECT d.id, d.alias, d.name, d.total_quantity::TEXT AS total_quantity,
               d.confidential_mode, d.mintable, d.owned_by, d.updated_at,
               COALESCE((SELECT COUNT(*)::INT FROM mn.assets a WHERE a.definition_id = d.id), 0) AS "holders!",
               COALESCE((SELECT SUM(a.value) FROM mn.assets a WHERE a.definition_id = d.id), 0)::TEXT AS "held_supply!"
        FROM mn.asset_definitions d
        WHERE d.alias = $1 OR d.id = $1 OR LOWER(d.name) = LOWER($1)
        LIMIT 1
        "#,
        needle
    )
    .fetch_optional(pool)
    .await?;
    Ok(r.map(|r| {
        serde_json::json!({
            "id": r.id, "alias": r.alias, "name": r.name, "total_quantity": r.total_quantity,
            "confidential_mode": r.confidential_mode, "mintable": r.mintable, "owned_by": r.owned_by,
            "updated_at": r.updated_at.to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
            "holders": r.holders, "held_supply": r.held_supply,
        })
    }))
}

/// `/asset/:idOrAlias/holders`.
pub async fn get_asset_holders(
    pool: &PgPool,
    needle: &str,
) -> Result<Option<serde_json::Value>, DbError> {
    let Some(d) = sqlx::query!(
        r#"
        SELECT id, alias, name, total_quantity::TEXT AS total_quantity, confidential_mode, mintable, owned_by
        FROM mn.asset_definitions
        WHERE alias = $1 OR id = $1 OR LOWER(name) = LOWER($1)
        LIMIT 1
        "#,
        needle
    )
    .fetch_optional(pool)
    .await?
    else {
        return Ok(None);
    };
    let rows = sqlx::query!(
        r#"SELECT account_id, value::TEXT AS "value!", updated_at FROM mn.assets WHERE definition_id = $1 ORDER BY value::TEXT DESC"#,
        d.id
    )
    .fetch_all(pool)
    .await?;
    let total: f64 = d
        .total_quantity
        .as_deref()
        .and_then(|t| t.parse::<f64>().ok())
        .unwrap_or(0.0);
    let holders: Vec<serde_json::Value> = rows
        .into_iter()
        .map(|r| {
            let pct =
                (total > 0.0).then(|| r.value.parse::<f64>().unwrap_or(f64::NAN) / total * 100.0);
            serde_json::json!({
                "account_id": r.account_id,
                "balance": r.value,
                "pct": pct,
                "updated_at": r.updated_at.to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
            })
        })
        .collect();
    Ok(Some(serde_json::json!({
        "definition": {
            "id": d.id, "alias": d.alias, "name": d.name, "total_quantity": d.total_quantity,
            "confidential_mode": d.confidential_mode, "mintable": d.mintable, "owned_by": d.owned_by,
        },
        "holders": holders,
    })))
}

/// `/asset-definitions/stats`.
pub async fn get_assets_stats(pool: &PgPool) -> Result<serde_json::Value, DbError> {
    let r = sqlx::query!(
        r#"
        SELECT COUNT(*)::INT AS "total!",
               COUNT(*) FILTER (WHERE confidential_mode = 'Convertible')::INT AS "zk_convertible!",
               COUNT(*) FILTER (WHERE mintable = 'Infinitely')::INT AS "mintable_inf!",
               COUNT(*) FILTER (WHERE mintable = 'Once')::INT AS "mintable_once!"
        FROM mn.asset_definitions
        "#
    )
    .fetch_one(pool)
    .await?;
    Ok(serde_json::json!({
        "total": r.total, "zk_convertible": r.zk_convertible,
        "mintable_inf": r.mintable_inf, "mintable_once": r.mintable_once,
    }))
}

/// `/peers` item.
#[derive(Clone, Debug, Serialize)]
pub struct PeerRow {
    /// Canonical key.
    pub multiaddr: String,
    /// Public key.
    pub public_key: Option<String>,
    /// IP.
    pub ip_address: Option<String>,
    /// Port.
    pub port: Option<i32>,
    /// First seen.
    #[serde(serialize_with = "ser::iso_ms")]
    pub first_seen_at: DateTime<Utc>,
    /// Last seen.
    #[serde(serialize_with = "ser::iso_ms")]
    pub last_seen_at: DateTime<Utc>,
    /// Active flag.
    pub is_active: bool,
}

/// `/peers` (active first, then most recently seen).
pub async fn list_peers(pool: &PgPool) -> Result<Vec<PeerRow>, DbError> {
    let rows = sqlx::query!(
        r#"SELECT multiaddr, public_key, ip_address, port, first_seen_at, last_seen_at, is_active FROM mn.peers ORDER BY is_active DESC, last_seen_at DESC"#
    )
    .fetch_all(pool)
    .await?;
    Ok(rows
        .into_iter()
        .map(|r| PeerRow {
            multiaddr: r.multiaddr,
            public_key: r.public_key,
            ip_address: r.ip_address,
            port: r.port,
            first_seen_at: r.first_seen_at,
            last_seen_at: r.last_seen_at,
            is_active: r.is_active,
        })
        .collect())
}

// =============================================================
// instructions / permissions / lane staking / metrics
// =============================================================

/// `/instructions` item.
#[derive(Clone, Debug, Serialize)]
pub struct InstructionRow {
    /// Parent transaction hash, hex.
    pub transaction_hash: String,
    /// Index in the transaction.
    pub instruction_index: i32,
    /// Block height as a JS number.
    pub block: i64,
    /// Signing account.
    pub authority: String,
    /// Kind.
    pub kind: String,
    /// Payload.
    pub payload: serde_json::Value,
    /// Parent transaction status.
    pub transaction_status: String,
    /// Creation time.
    #[serde(serialize_with = "ser::iso_ms")]
    pub created_at: DateTime<Utc>,
}

/// Filters of `/instructions`.
#[derive(Clone, Debug, Default)]
pub struct IsiListFilter {
    /// `kind = $`.
    pub kind: Option<String>,
    /// `authority = $`.
    pub authority: Option<String>,
    /// `block_height = $`.
    pub block: Option<i64>,
    /// `transaction_hash = $` (32 bytes).
    pub tx_hash: Option<Vec<u8>>,
}

/// `/instructions?page&per_page&kind&authority&block&tx`, newest first.
pub async fn list_instructions(
    pool: &PgPool,
    page: i64,
    per_page: i64,
    f: &IsiListFilter,
) -> Result<Page<InstructionRow>, DbError> {
    if let Some(h) = &f.tx_hash {
        check32(h, "transaction hash")?;
    }
    let offset = (page.max(1) - 1) * per_page;
    let total = sqlx::query!(
        r#"
        SELECT COUNT(*) AS "c!" FROM mn.instructions
        WHERE ($1::TEXT IS NULL OR kind = $1)
          AND ($2::TEXT IS NULL OR authority = $2)
          AND ($3::BIGINT IS NULL OR block_height = $3)
          AND ($4::BYTEA IS NULL OR transaction_hash = $4)
        "#,
        f.kind,
        f.authority,
        f.block,
        f.tx_hash
    )
    .fetch_one(pool)
    .await?
    .c;
    let rows = sqlx::query!(
        r#"
        SELECT transaction_hash, instruction_index, block_height, authority, kind, payload, transaction_status, created_at
        FROM mn.instructions
        WHERE ($1::TEXT IS NULL OR kind = $1)
          AND ($2::TEXT IS NULL OR authority = $2)
          AND ($3::BIGINT IS NULL OR block_height = $3)
          AND ($4::BYTEA IS NULL OR transaction_hash = $4)
        ORDER BY created_at DESC, instruction_index ASC
        LIMIT $5 OFFSET $6
        "#,
        f.kind,
        f.authority,
        f.block,
        f.tx_hash,
        per_page,
        offset
    )
    .fetch_all(pool)
    .await?;
    let items = rows
        .into_iter()
        .map(|r| InstructionRow {
            transaction_hash: hex_of(&r.transaction_hash),
            instruction_index: r.instruction_index,
            block: r.block_height,
            authority: r.authority,
            kind: r.kind,
            payload: r.payload,
            transaction_status: r.transaction_status,
            created_at: r.created_at,
        })
        .collect();
    Ok(Page::new(page, per_page, total, items))
}

/// `/instructions/kinds` — `{kind, count}` by count desc.
pub async fn list_instruction_kinds(pool: &PgPool) -> Result<Vec<serde_json::Value>, DbError> {
    let rows = sqlx::query!(
        r#"SELECT kind, COUNT(*)::INT AS "count!" FROM mn.instructions GROUP BY kind ORDER BY COUNT(*) DESC"#
    )
    .fetch_all(pool)
    .await?;
    Ok(rows
        .into_iter()
        .map(|r| serde_json::json!({"kind": r.kind, "count": r.count}))
        .collect())
}

/// Kinds emitted by `authority` (wallet info), top 10.
pub async fn instruction_kinds_of(
    pool: &PgPool,
    authority: &str,
) -> Result<Vec<serde_json::Value>, DbError> {
    let rows = sqlx::query!(
        r#"SELECT kind, COUNT(*)::INT AS "count!" FROM mn.instructions WHERE authority = $1 GROUP BY kind ORDER BY COUNT(*) DESC LIMIT 10"#,
        authority
    )
    .fetch_all(pool)
    .await?;
    Ok(rows
        .into_iter()
        .map(|r| serde_json::json!({"kind": r.kind, "count": r.count}))
        .collect())
}

/// `/transfers/stats`. Only the `Asset` variant carries a numeric
/// `value.object`; the others put a hash there.
pub async fn get_transfers_stats(pool: &PgPool) -> Result<serde_json::Value, DbError> {
    let r = sqlx::query!(
        r#"
        SELECT COUNT(*)::INT AS "total!",
               COUNT(*) FILTER (WHERE created_at >= NOW() - INTERVAL '24 hours')::INT AS "total_24h!",
               COUNT(DISTINCT authority)::INT AS "unique_senders!",
               COUNT(DISTINCT payload->'value'->>'destination')::INT AS "unique_recipients!",
               COALESCE(SUM((payload->'value'->>'object')::NUMERIC) FILTER (WHERE payload->>'variant' = 'Asset'), 0)::TEXT AS "volume_total!",
               COALESCE(SUM((payload->'value'->>'object')::NUMERIC) FILTER (WHERE payload->>'variant' = 'Asset' AND created_at >= NOW() - INTERVAL '24 hours'), 0)::TEXT AS "volume_24h!"
        FROM mn.instructions
        WHERE kind = 'Transfer'
        "#
    )
    .fetch_one(pool)
    .await?;
    let top = sqlx::query!(
        r#"
        SELECT SPLIT_PART(payload->'value'->>'source', '#', 1) AS "asset_id!",
               COUNT(*)::INT AS "c!",
               COALESCE(SUM((payload->'value'->>'object')::NUMERIC), 0)::TEXT AS "volume!"
        FROM mn.instructions
        WHERE kind = 'Transfer' AND payload->>'variant' = 'Asset'
        GROUP BY 1
        ORDER BY COUNT(*) DESC
        LIMIT 1
        "#
    )
    .fetch_optional(pool)
    .await?;
    let top_asset = match top {
        Some(t) => {
            let def = sqlx::query!(
                r#"SELECT alias, name FROM mn.asset_definitions WHERE id = $1"#,
                t.asset_id
            )
            .fetch_optional(pool)
            .await?;
            let (alias, name) = def.map(|d| (d.alias, d.name)).unwrap_or((None, None));
            serde_json::json!({
                "asset_id": t.asset_id, "alias": alias, "name": name, "count": t.c, "volume": t.volume,
            })
        }
        None => serde_json::Value::Null,
    };
    Ok(serde_json::json!({
        "total": r.total,
        "total_24h": r.total_24h,
        "unique_senders": r.unique_senders,
        "unique_recipients": r.unique_recipients,
        "volume_total": r.volume_total,
        "volume_24h": r.volume_24h,
        "top_asset": top_asset,
    }))
}

/// `/permissions/stats` over the `Grant` instructions.
pub async fn get_permissions_stats(pool: &PgPool) -> Result<serde_json::Value, DbError> {
    let r = sqlx::query!(
        r#"
        SELECT COUNT(*)::INT AS "total!",
               COUNT(*) FILTER (WHERE payload->>'variant' = 'PermissionToAccount')::INT AS "perm_to_account!",
               COUNT(*) FILTER (WHERE payload->>'variant' = 'RoleToAccount')::INT AS "role_to_account!",
               COUNT(DISTINCT payload->'value'->'object'->>'name')::INT AS "distinct_perms!",
               COUNT(DISTINCT authority)::INT AS "distinct_grantors!",
               COUNT(DISTINCT payload->'value'->>'destination')::INT AS "distinct_recipients!"
        FROM mn.instructions
        WHERE kind = 'Grant'
        "#
    )
    .fetch_one(pool)
    .await?;
    let top = sqlx::query!(
        r#"
        SELECT payload->'value'->'object'->>'name' AS name,
               payload->>'variant' AS variant,
               COUNT(*)::INT AS "count!"
        FROM mn.instructions
        WHERE kind = 'Grant'
        GROUP BY 1, 2
        ORDER BY COUNT(*) DESC
        LIMIT 20
        "#
    )
    .fetch_all(pool)
    .await?;
    let top: Vec<serde_json::Value> = top
        .into_iter()
        .map(|t| serde_json::json!({"name": t.name, "variant": t.variant, "count": t.count}))
        .collect();
    Ok(serde_json::json!({
        "total": r.total,
        "perm_to_account": r.perm_to_account,
        "role_to_account": r.role_to_account,
        "distinct_perms": r.distinct_perms,
        "distinct_grantors": r.distinct_grantors,
        "distinct_recipients": r.distinct_recipients,
        "top_permissions": top,
    }))
}

/// Filters of `/permissions/grants`.
#[derive(Clone, Debug, Default)]
pub struct GrantFilter {
    /// `payload.value.object.name`.
    pub perm_name: Option<String>,
    /// Grantor.
    pub authority: Option<String>,
    /// `payload.value.destination`.
    pub destination: Option<String>,
    /// `payload.variant`.
    pub variant: Option<String>,
}

/// `/permissions/grants` item. The hoisted fields are omitted when the
/// payload does not carry them (`JSON.stringify` drops `undefined`).
#[derive(Clone, Debug, Serialize)]
pub struct GrantRow {
    /// Parent transaction hash, hex.
    pub transaction_hash: String,
    /// Index in the transaction.
    pub instruction_index: i32,
    /// Block height.
    pub block: i64,
    /// Grantor.
    pub authority: String,
    /// Permission name.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub permission_name: Option<String>,
    /// Permission arguments.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub permission_args: Option<serde_json::Value>,
    /// Grantee.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub destination: Option<String>,
    /// Grant variant.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub variant: Option<String>,
    /// Parent transaction status.
    pub transaction_status: String,
    /// Creation time.
    #[serde(serialize_with = "ser::iso_ms")]
    pub created_at: DateTime<Utc>,
}

/// `/permissions/grants?page&per_page&perm_name&authority&destination&variant`.
pub async fn list_permission_grants(
    pool: &PgPool,
    page: i64,
    per_page: i64,
    f: &GrantFilter,
) -> Result<Page<GrantRow>, DbError> {
    let offset = (page.max(1) - 1) * per_page;
    let total = sqlx::query!(
        r#"
        SELECT COUNT(*) AS "c!" FROM mn.instructions
        WHERE kind = 'Grant'
          AND ($1::TEXT IS NULL OR payload->'value'->'object'->>'name' = $1)
          AND ($2::TEXT IS NULL OR authority = $2)
          AND ($3::TEXT IS NULL OR payload->'value'->>'destination' = $3)
          AND ($4::TEXT IS NULL OR payload->>'variant' = $4)
        "#,
        f.perm_name,
        f.authority,
        f.destination,
        f.variant
    )
    .fetch_one(pool)
    .await?
    .c;
    let rows = sqlx::query!(
        r#"
        SELECT transaction_hash, instruction_index, block_height, authority, payload, transaction_status, created_at
        FROM mn.instructions
        WHERE kind = 'Grant'
          AND ($1::TEXT IS NULL OR payload->'value'->'object'->>'name' = $1)
          AND ($2::TEXT IS NULL OR authority = $2)
          AND ($3::TEXT IS NULL OR payload->'value'->>'destination' = $3)
          AND ($4::TEXT IS NULL OR payload->>'variant' = $4)
        ORDER BY created_at DESC, instruction_index ASC
        LIMIT $5 OFFSET $6
        "#,
        f.perm_name,
        f.authority,
        f.destination,
        f.variant,
        per_page,
        offset
    )
    .fetch_all(pool)
    .await?;
    let items = rows
        .into_iter()
        .map(|r| {
            let value = r.payload.get("value");
            let object = value.and_then(|v| v.get("object"));
            GrantRow {
                transaction_hash: hex_of(&r.transaction_hash),
                instruction_index: r.instruction_index,
                block: r.block_height,
                authority: r.authority,
                permission_name: object
                    .and_then(|o| o.get("name"))
                    .and_then(|n| n.as_str())
                    .map(String::from),
                permission_args: object.and_then(|o| o.get("payload")).cloned(),
                destination: value
                    .and_then(|v| v.get("destination"))
                    .and_then(|d| d.as_str())
                    .map(String::from),
                variant: r
                    .payload
                    .get("variant")
                    .and_then(|v| v.as_str())
                    .map(String::from),
                transaction_status: r.transaction_status,
                created_at: r.created_at,
            }
        })
        .collect();
    Ok(Page::new(page, per_page, total, items))
}

/// One `RegisterPublicLaneValidator` / `ActivatePublicLaneValidator` row
/// with the ASCII pubkey extracted from the Norito hex blob.
#[derive(Clone, Debug)]
pub struct LaneEvent {
    /// Validator public key (`ea0130…`, 102 chars).
    pub pubkey: String,
    /// Kind.
    pub kind: String,
    /// Authority.
    pub authority: String,
    /// Transaction hash, hex.
    pub tx_hash: String,
    /// Block height.
    pub block: i64,
    /// Transaction status.
    pub status: String,
    /// Creation time.
    pub created_at: DateTime<Utc>,
}

/// Lane-staking events oldest first (rows without a recognisable pubkey
/// are dropped, as the Node did).
pub async fn lane_staking_events(pool: &PgPool) -> Result<Vec<LaneEvent>, DbError> {
    let rows = sqlx::query!(
        r#"
        SELECT kind, authority, payload->'value'->>'encoded' AS encoded, transaction_hash, block_height, transaction_status, created_at
        FROM mn.instructions
        WHERE kind IN ('RegisterPublicLaneValidator', 'ActivatePublicLaneValidator')
        ORDER BY created_at ASC
        "#
    )
    .fetch_all(pool)
    .await?;
    Ok(rows
        .into_iter()
        .filter_map(|r| {
            let pubkey = r.encoded.as_deref().and_then(extract_lane_pubkey)?;
            Some(LaneEvent {
                pubkey,
                kind: r.kind,
                authority: r.authority,
                tx_hash: hex_of(&r.transaction_hash),
                block: r.block_height,
                status: r.transaction_status,
                created_at: r.created_at,
            })
        })
        .collect())
}

/// The pubkey appears inside the hex blob as ASCII: `ea0130` is
/// `656130313330`, followed by 96 pubkey chars = 192 hex chars.
pub fn extract_lane_pubkey(encoded_hex: &str) -> Option<String> {
    let lower = encoded_hex.to_ascii_lowercase();
    let start = lower.find("656130313330")?;
    let slice = lower.get(start..start + 204)?;
    if slice.len() != 204 || !slice.bytes().all(|b| b.is_ascii_hexdigit()) {
        return None;
    }
    let bytes: Vec<u8> = (0..slice.len())
        .step_by(2)
        .filter_map(|i| u8::from_str_radix(&slice[i..i + 2], 16).ok())
        .collect();
    String::from_utf8(bytes).ok()
}

/// `/prometheus/metric/:name` series (`{ts, labels, value}` ascending).
pub async fn get_metric_series(
    pool: &PgPool,
    name: &str,
    hours: i32,
) -> Result<Vec<serde_json::Value>, DbError> {
    let rows = sqlx::query!(
        r#"
        SELECT ts, labels, value FROM mn.metrics_snapshots
        WHERE metric_name = $1 AND ts > NOW() - make_interval(hours => $2)
        ORDER BY ts ASC
        "#,
        name,
        hours
    )
    .fetch_all(pool)
    .await?;
    Ok(rows
        .into_iter()
        .map(|r| {
            serde_json::json!({
                "ts": r.ts.to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
                "labels": r.labels,
                "value": r.value,
            })
        })
        .collect())
}

// =============================================================
// wallet info pieces
// =============================================================

/// Outgoing transaction stats of one authority.
pub struct WalletTxStats {
    /// Transactions signed.
    pub tx_count: i32,
    /// Committed ones.
    pub tx_committed: i32,
    /// First transaction.
    pub first_tx_at: Option<DateTime<Utc>>,
    /// Last transaction.
    pub last_tx_at: Option<DateTime<Utc>>,
    /// Distinct days with a transaction.
    pub days_active: i32,
}

/// Outgoing tx stats.
pub async fn wallet_tx_stats(pool: &PgPool, addr: &str) -> Result<WalletTxStats, DbError> {
    let r = sqlx::query!(
        r#"
        SELECT COUNT(*)::INT AS "tx_count!",
               COALESCE(SUM(CASE WHEN status = 'Committed' THEN 1 ELSE 0 END), 0)::INT AS "tx_committed!",
               MIN(created_at) AS first_tx_at,
               MAX(created_at) AS last_tx_at,
               COUNT(DISTINCT DATE(created_at))::INT AS "days_active!"
        FROM mn.transactions WHERE authority = $1
        "#,
        addr
    )
    .fetch_one(pool)
    .await?;
    Ok(WalletTxStats {
        tx_count: r.tx_count,
        tx_committed: r.tx_committed,
        first_tx_at: r.first_tx_at,
        last_tx_at: r.last_tx_at,
        days_active: r.days_active,
    })
}

/// Incoming activity (Transfer / Mint destination) of one account.
pub struct WalletIncoming {
    /// Transfers received.
    pub in_transfers: i32,
    /// Mints received.
    pub in_mints: i32,
    /// First incoming.
    pub in_first_at: Option<DateTime<Utc>>,
    /// Last incoming.
    pub in_last_at: Option<DateTime<Utc>>,
    /// Distinct days with incoming activity.
    pub in_days: i32,
}

/// Incoming stats.
pub async fn wallet_incoming(pool: &PgPool, addr: &str) -> Result<WalletIncoming, DbError> {
    let r = sqlx::query!(
        r#"
        SELECT COUNT(*) FILTER (WHERE kind = 'Transfer')::INT AS "in_transfers!",
               COUNT(*) FILTER (WHERE kind = 'Mint')::INT AS "in_mints!",
               MIN(created_at) AS in_first_at,
               MAX(created_at) AS in_last_at,
               COUNT(DISTINCT DATE(created_at))::INT AS "in_days!"
        FROM mn.instructions
        WHERE (kind = 'Transfer' AND payload->'value'->>'destination' = $1)
           OR (kind = 'Mint' AND payload->'value'->>'destination' LIKE '%#' || $1)
        "#,
        addr
    )
    .fetch_one(pool)
    .await?;
    Ok(WalletIncoming {
        in_transfers: r.in_transfers,
        in_mints: r.in_mints,
        in_first_at: r.in_first_at,
        in_last_at: r.in_last_at,
        in_days: r.in_days,
    })
}

/// Outgoing transfer volume `{count, volume, max, avg}` (decimal text).
pub async fn wallet_out_transfers(
    pool: &PgPool,
    addr: &str,
) -> Result<(i32, String, String, String), DbError> {
    let r = sqlx::query!(
        r#"
        SELECT COUNT(*)::INT AS "count!",
               COALESCE(SUM((payload->'value'->>'object')::NUMERIC), 0)::TEXT AS "volume!",
               COALESCE(MAX((payload->'value'->>'object')::NUMERIC), 0)::TEXT AS "max!",
               COALESCE(AVG((payload->'value'->>'object')::NUMERIC), 0)::TEXT AS "avg!"
        FROM mn.instructions WHERE authority = $1 AND kind = 'Transfer'
        "#,
        addr
    )
    .fetch_one(pool)
    .await?;
    Ok((r.count, r.volume, r.max, r.avg))
}

/// One "top token" row of the wallet info.
#[derive(Clone, Debug, Serialize)]
pub struct WalletToken {
    /// Asset definition id.
    pub asset_id: String,
    /// Alias.
    pub alias: Option<String>,
    /// Name.
    pub name: Option<String>,
    /// Interactions.
    pub trades: i32,
    /// Volume, decimal text.
    pub volume: String,
}

/// Tokens the wallet touched (in + out), top 10 by trades then volume.
pub async fn wallet_top_tokens(pool: &PgPool, addr: &str) -> Result<Vec<WalletToken>, DbError> {
    let rows = sqlx::query!(
        r#"
        WITH involved AS (
            SELECT SPLIT_PART(payload->'value'->>'source', '#', 1)::TEXT AS asset_token,
                   COALESCE((payload->'value'->>'object')::NUMERIC, 0) AS amount
            FROM mn.instructions
            WHERE kind = 'Transfer' AND (authority = $1 OR payload->'value'->>'destination' = $1)
            UNION ALL
            SELECT SPLIT_PART(payload->'value'->>'destination', '#', 1)::TEXT AS asset_token,
                   COALESCE((payload->'value'->>'object')::NUMERIC, 0) AS amount
            FROM mn.instructions
            WHERE kind = 'Mint' AND payload->'value'->>'destination' LIKE '%#' || $1
        )
        SELECT i.asset_token AS "asset_token!", ad.alias, ad.name,
               COUNT(*)::INT AS "trades!", SUM(i.amount)::TEXT AS "volume!"
        FROM involved i
        LEFT JOIN mn.asset_definitions ad ON ad.id = i.asset_token
        WHERE i.asset_token IS NOT NULL AND i.asset_token <> ''
        GROUP BY i.asset_token, ad.alias, ad.name
        ORDER BY COUNT(*) DESC, SUM(i.amount) DESC NULLS LAST
        LIMIT 10
        "#,
        addr
    )
    .fetch_all(pool)
    .await?;
    Ok(rows
        .into_iter()
        .map(|r| WalletToken {
            asset_id: r.asset_token,
            alias: r.alias,
            name: r.name,
            trades: r.trades,
            volume: r.volume,
        })
        .collect())
}

/// Cross-chain claims received by `addr`: `(claims_received, xor_claimed)`.
pub async fn wallet_claims_received(pool: &PgPool, addr: &str) -> Result<(i32, String), DbError> {
    let r = sqlx::query!(
        r#"
        SELECT COUNT(*)::INT AS "claims_received!",
               COALESCE((
                   SELECT SUM((i.payload->'value'->>'object')::NUMERIC)
                   FROM mn.transactions t
                   JOIN mn.instructions i ON i.transaction_hash = t.hash
                   WHERE t.sora_nexus_claim_recipient = $1 AND i.kind = 'Mint'
               ), 0)::TEXT AS "xor_claimed!"
        FROM mn.transactions WHERE sora_nexus_claim_recipient = $1
        "#,
        addr
    )
    .fetch_one(pool)
    .await?;
    Ok((r.claims_received, r.xor_claimed))
}

// =============================================================
// cross-chain
// =============================================================

/// One XOR mint row for the mint history.
pub struct MintRow {
    /// `genesis_premint` | `cross_chain_claim` | `other_mint`.
    pub source: String,
    /// Block height.
    pub block: i64,
    /// Creation time.
    pub created_at: DateTime<Utc>,
    /// Minted amount, decimal text.
    pub amount_raw: Option<String>,
    /// Minting authority.
    pub minter: String,
    /// Recipient account.
    pub recipient: Option<String>,
    /// Transaction hash, hex.
    pub tx_hash: String,
    /// SORA v2 burn tx.
    pub v2_burn_tx: Option<String>,
    /// SORA v2 block.
    pub v2_block: Option<i64>,
    /// SORA v2 signer.
    pub v2_signer: Option<String>,
}

/// XOR definition id (`alias = 'xor#universal'` or `name = 'xor'`).
pub async fn xor_definition_id(pool: &PgPool) -> Result<Option<String>, DbError> {
    let r = sqlx::query!(
        r#"SELECT id FROM mn.asset_definitions WHERE alias = 'xor#universal' OR LOWER(name) = 'xor' LIMIT 1"#
    )
    .fetch_optional(pool)
    .await?;
    Ok(r.map(|r| r.id))
}

/// Every Mint of `xor_id`, chronological, categorised by source.
pub async fn xor_mints(pool: &PgPool, xor_id: &str) -> Result<Vec<MintRow>, DbError> {
    let rows = sqlx::query!(
        r#"
        SELECT CASE WHEN i.block_height = 1 THEN 'genesis_premint'
                    WHEN t.sora_v2_claim_tx_hash IS NOT NULL THEN 'cross_chain_claim'
                    ELSE 'other_mint' END AS "source!",
               i.block_height, i.created_at,
               (i.payload->'value'->>'object')::TEXT AS amount_raw,
               i.authority AS minter,
               SUBSTRING(i.payload->'value'->>'destination' FROM POSITION('#' IN i.payload->'value'->>'destination') + 1) AS recipient,
               encode(i.transaction_hash, 'hex') AS "tx_hash!",
               t.sora_v2_claim_tx_hash AS v2_burn_tx, t.sora_v2_block AS v2_block, t.sora_v2_signer AS v2_signer
        FROM mn.instructions i
        JOIN mn.transactions t ON t.hash = i.transaction_hash
        WHERE i.kind = 'Mint' AND i.payload->'value'->>'destination' LIKE $1 || '#%'
        ORDER BY i.created_at ASC, i.block_height ASC
        "#,
        xor_id
    )
    .fetch_all(pool)
    .await?;
    Ok(rows
        .into_iter()
        .map(|r| MintRow {
            source: r.source,
            block: r.block_height,
            created_at: r.created_at,
            amount_raw: r.amount_raw,
            minter: r.minter,
            recipient: r.recipient,
            tx_hash: r.tx_hash,
            v2_burn_tx: r.v2_burn_tx,
            v2_block: r.v2_block,
            v2_signer: r.v2_signer,
        })
        .collect())
}

/// `/cross-chain/claims` item.
#[derive(Clone, Debug, Serialize)]
pub struct ClaimRow {
    /// Minamoto block.
    pub mn_block: i64,
    /// Minamoto tx hash, hex.
    pub mn_tx_hash: String,
    /// Recipient.
    pub mn_recipient: Option<String>,
    /// Claim authority.
    pub mn_authority: String,
    /// SORA v2 block.
    pub v2_block: Option<i64>,
    /// SORA v2 burn tx.
    pub v2_tx_hash: Option<String>,
    /// SORA v2 signer.
    pub v2_signer: Option<String>,
    /// Fee sponsor.
    pub fee_sponsor: Option<String>,
    /// Creation time.
    #[serde(serialize_with = "ser::iso_ms")]
    pub created_at: DateTime<Utc>,
    /// Status.
    pub status: String,
    /// Sum of the tx's Mint amounts, decimal text.
    pub claimed_amount: String,
}

/// `/cross-chain/claims?page&per_page`, newest first.
pub async fn list_claims(
    pool: &PgPool,
    page: i64,
    per_page: i64,
) -> Result<Page<ClaimRow>, DbError> {
    let offset = (page.max(1) - 1) * per_page;
    let total = sqlx::query!(
        r#"SELECT COUNT(*) AS "c!" FROM mn.transactions WHERE sora_v2_claim_tx_hash IS NOT NULL"#
    )
    .fetch_one(pool)
    .await?
    .c;
    let rows = sqlx::query!(
        r#"
        SELECT t.hash, t.block_height, t.authority, t.created_at, t.status,
               t.sora_v2_claim_tx_hash, t.sora_nexus_claim_recipient, t.fee_sponsor,
               t.sora_v2_block, t.sora_v2_signer,
               COALESCE((SELECT SUM((i.payload->'value'->>'object')::NUMERIC)
                         FROM mn.instructions i
                         WHERE i.transaction_hash = t.hash AND i.kind = 'Mint'), 0)::TEXT AS "claimed_amount!"
        FROM mn.transactions t
        WHERE t.sora_v2_claim_tx_hash IS NOT NULL
        ORDER BY t.created_at DESC
        LIMIT $1 OFFSET $2
        "#,
        per_page,
        offset
    )
    .fetch_all(pool)
    .await?;
    let items = rows
        .into_iter()
        .map(|r| ClaimRow {
            mn_block: r.block_height,
            mn_tx_hash: hex_of(&r.hash),
            mn_recipient: r.sora_nexus_claim_recipient,
            mn_authority: r.authority,
            v2_block: r.sora_v2_block,
            v2_tx_hash: r.sora_v2_claim_tx_hash,
            v2_signer: r.sora_v2_signer,
            fee_sponsor: r.fee_sponsor,
            created_at: r.created_at,
            status: r.status,
            claimed_amount: r.claimed_amount,
        })
        .collect();
    Ok(Page::new(page, per_page, total, items))
}

/// `/cross-chain/stats`.
pub async fn get_cross_chain_stats(pool: &PgPool) -> Result<serde_json::Value, DbError> {
    let r = sqlx::query!(
        r#"
        SELECT (SELECT COUNT(*)::INT FROM mn.transactions WHERE sora_v2_claim_tx_hash IS NOT NULL) AS "total_claims!",
               (SELECT COUNT(DISTINCT sora_nexus_claim_recipient)::INT FROM mn.transactions WHERE sora_nexus_claim_recipient IS NOT NULL) AS "unique_recipients!",
               (SELECT COALESCE(SUM((i.payload->'value'->>'object')::NUMERIC), 0)
                FROM mn.transactions t JOIN mn.instructions i ON i.transaction_hash = t.hash
                WHERE t.sora_v2_claim_tx_hash IS NOT NULL AND i.kind = 'Mint')::TEXT AS "total_xor_claimed!",
               (SELECT MIN(created_at) FROM mn.transactions WHERE sora_v2_claim_tx_hash IS NOT NULL) AS first_claim_at,
               (SELECT MAX(created_at) FROM mn.transactions WHERE sora_v2_claim_tx_hash IS NOT NULL) AS last_claim_at
        "#
    )
    .fetch_one(pool)
    .await?;
    let iso = |t: Option<DateTime<Utc>>| {
        t.map(|t| t.to_rfc3339_opts(chrono::SecondsFormat::Millis, true))
    };
    Ok(serde_json::json!({
        "total_claims": r.total_claims,
        "unique_recipients": r.unique_recipients,
        "total_xor_claimed": r.total_xor_claimed,
        "first_claim_at": iso(r.first_claim_at),
        "last_claim_at": iso(r.last_claim_at),
    }))
}

/// `/cross-chain/timeseries?hours` — hourly claim buckets.
pub async fn get_cross_chain_timeseries(
    pool: &PgPool,
    hours: i32,
) -> Result<Vec<serde_json::Value>, DbError> {
    let rows = sqlx::query!(
        r#"
        SELECT date_trunc('hour', t.created_at) AS "bucket!",
               COUNT(*)::INT AS "claims!",
               COALESCE(SUM((i.payload->'value'->>'object')::NUMERIC), 0)::TEXT AS "xor_claimed!"
        FROM mn.transactions t
        LEFT JOIN mn.instructions i ON i.transaction_hash = t.hash AND i.kind = 'Mint'
        WHERE t.sora_v2_claim_tx_hash IS NOT NULL
          AND t.created_at > NOW() - make_interval(hours => $1)
        GROUP BY 1
        ORDER BY 1 ASC
        "#,
        hours
    )
    .fetch_all(pool)
    .await?;
    Ok(rows
        .into_iter()
        .map(|r| {
            serde_json::json!({
                "bucket": r.bucket.to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
                "claims": r.claims,
                "xor_claimed": r.xor_claimed,
            })
        })
        .collect())
}

/// Creation time of block #1 of the current chain (reset detection and
/// the `pre_reset` cutoff of the pending-burns view).
pub async fn genesis_created_at(pool: &PgPool) -> Result<Option<DateTime<Utc>>, DbError> {
    let r = sqlx::query!(r#"SELECT created_at FROM mn.blocks WHERE height = 1"#)
        .fetch_optional(pool)
        .await?;
    Ok(r.map(|r| r.created_at))
}

/// One SORA v2 burn carrying the `soraNexusXorClaim` remark, joined
/// with its Minamoto claim when one exists.
pub struct PendingBurnRow {
    /// SORA v2 extrinsic hash (`0x…`).
    pub v2_tx_hash: String,
    /// SORA v2 block.
    pub v2_block: i64,
    /// SORA v2 signer.
    pub v2_signer: String,
    /// SORA v2 block time.
    pub v2_at: DateTime<Utc>,
    /// Burned amount in planck as rendered by `toHuman` (thousands separators).
    pub raw_amount: Option<String>,
    /// Recipient from the remark JSON.
    pub recipient_i105: Option<String>,
    /// Minamoto claim tx hash, when claimed.
    pub mn_tx_hash: Option<Vec<u8>>,
    /// Minamoto block, when claimed.
    pub mn_block: Option<i64>,
    /// Minamoto claim time, when claimed.
    pub mn_at: Option<DateTime<Utc>>,
}

/// Burns from `sm.extrinsics` (`utility.batchAll` = `assets.burn` +
/// `system.remark` `{"type":"soraNexusXorClaim",…}`) at or after
/// `since_block`, newest first. Read-only on `sm.*`.
pub async fn list_cross_chain_burns(
    pool: &PgPool,
    since_block: i64,
    limit: i64,
) -> Result<Vec<PendingBurnRow>, DbError> {
    let rows = sqlx::query!(
        r#"
        SELECT e.hash AS "v2_tx_hash!", e.block_height AS "v2_block!", e.signer AS "v2_signer!",
               e.block_timestamp AS "v2_at!",
               (SELECT c->'args'->>'amount' FROM jsonb_array_elements(e.args->'calls') c
                 WHERE c->>'section' = 'assets' AND c->>'method' = 'burn' LIMIT 1) AS raw_amount,
               (SELECT convert_from(decode(substring(c->'args'->>'remark' FROM 3), 'hex'), 'UTF8')::jsonb->>'recipient'
                  FROM jsonb_array_elements(e.args->'calls') c
                 WHERE c->>'section' = 'system' AND c->>'method' = 'remark'
                   AND c->'args'->>'remark' LIKE '0x7b%736f72614e65787573586f72436c61696d%' LIMIT 1) AS recipient_i105,
               t.hash AS "mn_tx_hash?", t.block_height AS "mn_block?", t.created_at AS "mn_at?"
        FROM sm.extrinsics e
        LEFT JOIN mn.transactions t ON t.sora_v2_claim_tx_hash = e.hash
        WHERE e.block_height >= $1
          AND e.section = 'utility'
          AND jsonb_typeof(e.args->'calls') = 'array'
          AND EXISTS (SELECT 1 FROM jsonb_array_elements(e.args->'calls') c
                       WHERE c->>'section' = 'system' AND c->>'method' = 'remark'
                         AND c->'args'->>'remark' LIKE '0x7b%736f72614e65787573586f72436c61696d%')
          AND EXISTS (SELECT 1 FROM jsonb_array_elements(e.args->'calls') c
                       WHERE c->>'section' = 'assets' AND c->>'method' = 'burn')
        ORDER BY e.block_height DESC
        LIMIT $2
        "#,
        since_block,
        limit
    )
    .fetch_all(pool)
    .await?;
    Ok(rows
        .into_iter()
        .map(|r| PendingBurnRow {
            v2_tx_hash: r.v2_tx_hash,
            v2_block: r.v2_block,
            v2_signer: r.v2_signer,
            v2_at: r.v2_at,
            raw_amount: r.raw_amount,
            recipient_i105: r.recipient_i105,
            mn_tx_hash: r.mn_tx_hash,
            mn_block: r.mn_block,
            mn_at: r.mn_at,
        })
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hex_and_pages() {
        assert_eq!(hex_of(&[0x0f, 0xab]), "0fab");
        assert_eq!(pages(0, 20), 1);
        assert_eq!(pages(438, 2), 219);
        assert_eq!(pages(41, 20), 3);
    }

    #[test]
    fn lane_pubkey_extraction() {
        let pk = format!("ea0130{}", "A".repeat(96));
        let ascii_hex: String = pk.bytes().map(|b| format!("{b:02x}")).collect();
        let blob = format!("0000{ascii_hex}ffff");
        assert_eq!(extract_lane_pubkey(&blob).as_deref(), Some(pk.as_str()));
        assert!(extract_lane_pubkey("656130313330ab").is_none());
    }
}
