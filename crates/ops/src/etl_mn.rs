//! ETL of the legacy `mn.*` (Minamoto) tables. Same layout on both
//! sides (the v33 migration mirrors the Node's `schema.sql`), so every
//! copy is verbatim: keyset-paginated on the primary key, `ON CONFLICT
//! DO NOTHING`, cursor in `sm.etl_state` under the `mn_<table>` name.
//! Reconciliation = exact row count plus a checksum of the key columns.

use anyhow::{bail, Context, Result};
use chrono::{DateTime, Utc};
use sqlx::{PgPool, Row};
use tracing::{info, warn};

/// Table names accepted by `migrate-legacy` for this schema.
pub const MN_TABLES: [&str; 11] = [
    "mn_blocks",
    "mn_accounts",
    "mn_transactions",
    "mn_instructions",
    "mn_domains",
    "mn_asset_definitions",
    "mn_assets",
    "mn_peers",
    "mn_network_state",
    "mn_indexer_state",
    "mn_metrics_snapshots",
];

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

/// Dispatches one `mn_*` table copy.
pub async fn copy(source: &PgPool, target: &PgPool, table: &str, batch: i64) -> Result<u64> {
    match table {
        "mn_blocks" => copy_blocks(source, target, batch).await,
        "mn_accounts" => copy_accounts(source, target, batch).await,
        "mn_transactions" => copy_transactions(source, target, batch).await,
        "mn_instructions" => copy_instructions(source, target, batch).await,
        "mn_domains" => copy_domains(source, target, batch).await,
        "mn_asset_definitions" => copy_asset_definitions(source, target, batch).await,
        "mn_assets" => copy_assets(source, target, batch).await,
        "mn_peers" => copy_peers(source, target, batch).await,
        "mn_network_state" => copy_network_state(source, target).await,
        "mn_indexer_state" => copy_indexer_state(source, target).await,
        "mn_metrics_snapshots" => copy_metrics_snapshots(source, target, batch).await,
        other => bail!("unknown mn table '{other}'"),
    }
}

async fn copy_blocks(source: &PgPool, target: &PgPool, batch: i64) -> Result<u64> {
    let mut cursor: i64 = get_cursor(target, "mn_blocks")
        .await?
        .map(|c| c.parse())
        .transpose()?
        .unwrap_or(0);
    let mut copied = 0u64;
    loop {
        let rows = sqlx::query(
            r#"SELECT height, hash, prev_hash, transactions_hash, created_at, transactions_committed,
                      transactions_rejected, indexed_at
               FROM mn.blocks WHERE height > $1 ORDER BY height LIMIT $2"#,
        )
        .bind(cursor)
        .bind(batch)
        .fetch_all(source)
        .await
        .context("reading legacy mn.blocks batch")?;
        if rows.is_empty() {
            break;
        }
        for r in &rows {
            cursor = r.try_get("height")?;
            sqlx::query!(
                r#"
                INSERT INTO mn.blocks (height, hash, prev_hash, transactions_hash, created_at,
                                       transactions_committed, transactions_rejected, indexed_at)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                ON CONFLICT (height) DO NOTHING
                "#,
                cursor,
                r.try_get::<Vec<u8>, _>("hash")?,
                r.try_get::<Option<Vec<u8>>, _>("prev_hash")?,
                r.try_get::<Option<Vec<u8>>, _>("transactions_hash")?,
                r.try_get::<DateTime<Utc>, _>("created_at")?,
                r.try_get::<i32, _>("transactions_committed")?,
                r.try_get::<i32, _>("transactions_rejected")?,
                r.try_get::<DateTime<Utc>, _>("indexed_at")?,
            )
            .execute(target)
            .await
            .context("inserting mn block")?;
        }
        copied += rows.len() as u64;
        set_cursor(target, "mn_blocks", &cursor.to_string(), rows.len() as i64).await?;
        info!(copied, cursor, "mn_blocks progress");
    }
    Ok(copied)
}

async fn copy_accounts(source: &PgPool, target: &PgPool, batch: i64) -> Result<u64> {
    let mut cursor = get_cursor(target, "mn_accounts").await?.unwrap_or_default();
    let mut copied = 0u64;
    loop {
        let rows = sqlx::query(
            r#"SELECT id, network_prefix, has_primary_alias, primary_alias, primary_alias_dataspace,
                      primary_alias_domain, primary_alias_name, multisig_quorum, multisig_signatories_count,
                      metadata, first_seen_at, last_seen_at
               FROM mn.accounts WHERE id > $1 ORDER BY id LIMIT $2"#,
        )
        .bind(&cursor)
        .bind(batch)
        .fetch_all(source)
        .await
        .context("reading legacy mn.accounts batch")?;
        if rows.is_empty() {
            break;
        }
        for r in &rows {
            cursor = r.try_get("id")?;
            sqlx::query!(
                r#"
                INSERT INTO mn.accounts (id, network_prefix, has_primary_alias, primary_alias, primary_alias_dataspace,
                                         primary_alias_domain, primary_alias_name, multisig_quorum,
                                         multisig_signatories_count, metadata, first_seen_at, last_seen_at)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
                ON CONFLICT (id) DO NOTHING
                "#,
                cursor,
                r.try_get::<i32, _>("network_prefix")?,
                r.try_get::<bool, _>("has_primary_alias")?,
                r.try_get::<Option<String>, _>("primary_alias")?,
                r.try_get::<Option<String>, _>("primary_alias_dataspace")?,
                r.try_get::<Option<String>, _>("primary_alias_domain")?,
                r.try_get::<Option<String>, _>("primary_alias_name")?,
                r.try_get::<Option<i32>, _>("multisig_quorum")?,
                r.try_get::<Option<i32>, _>("multisig_signatories_count")?,
                r.try_get::<serde_json::Value, _>("metadata")?,
                r.try_get::<DateTime<Utc>, _>("first_seen_at")?,
                r.try_get::<DateTime<Utc>, _>("last_seen_at")?,
            )
            .execute(target)
            .await
            .context("inserting mn account")?;
        }
        copied += rows.len() as u64;
        set_cursor(target, "mn_accounts", &cursor, rows.len() as i64).await?;
        info!(copied, "mn_accounts progress");
    }
    Ok(copied)
}

async fn copy_transactions(source: &PgPool, target: &PgPool, batch: i64) -> Result<u64> {
    // Keyset on the hex form of the hash (bytea ordering = hex ordering).
    let mut cursor = get_cursor(target, "mn_transactions")
        .await?
        .unwrap_or_default();
    let mut copied = 0u64;
    loop {
        let rows = sqlx::query(
            r#"SELECT encode(hash, 'hex') AS hex, hash, block_height, authority, created_at, executable_kind, status,
                      indexed_at, sora_v2_claim_tx_hash, sora_nexus_claim_recipient, fee_sponsor,
                      sora_v2_block, sora_v2_signer
               FROM mn.transactions WHERE encode(hash, 'hex') > $1 ORDER BY encode(hash, 'hex') LIMIT $2"#,
        )
        .bind(&cursor)
        .bind(batch)
        .fetch_all(source)
        .await
        .context("reading legacy mn.transactions batch")?;
        if rows.is_empty() {
            break;
        }
        for r in &rows {
            cursor = r.try_get("hex")?;
            sqlx::query!(
                r#"
                INSERT INTO mn.transactions (hash, block_height, authority, created_at, executable_kind, status,
                                             indexed_at, sora_v2_claim_tx_hash, sora_nexus_claim_recipient,
                                             fee_sponsor, sora_v2_block, sora_v2_signer)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
                ON CONFLICT (hash) DO NOTHING
                "#,
                r.try_get::<Vec<u8>, _>("hash")?,
                r.try_get::<i64, _>("block_height")?,
                r.try_get::<String, _>("authority")?,
                r.try_get::<DateTime<Utc>, _>("created_at")?,
                r.try_get::<String, _>("executable_kind")?,
                r.try_get::<String, _>("status")?,
                r.try_get::<DateTime<Utc>, _>("indexed_at")?,
                r.try_get::<Option<String>, _>("sora_v2_claim_tx_hash")?,
                r.try_get::<Option<String>, _>("sora_nexus_claim_recipient")?,
                r.try_get::<Option<String>, _>("fee_sponsor")?,
                r.try_get::<Option<i64>, _>("sora_v2_block")?,
                r.try_get::<Option<String>, _>("sora_v2_signer")?,
            )
            .execute(target)
            .await
            .context("inserting mn transaction")?;
        }
        copied += rows.len() as u64;
        set_cursor(target, "mn_transactions", &cursor, rows.len() as i64).await?;
        info!(copied, "mn_transactions progress");
    }
    Ok(copied)
}

async fn copy_instructions(source: &PgPool, target: &PgPool, batch: i64) -> Result<u64> {
    // Composite keyset: "<hex hash>:<index padded>" text.
    let mut cursor = get_cursor(target, "mn_instructions")
        .await?
        .unwrap_or_default();
    let mut copied = 0u64;
    loop {
        let rows = sqlx::query(
            r#"SELECT encode(transaction_hash, 'hex') || ':' || lpad(instruction_index::text, 10, '0') AS k,
                      transaction_hash, instruction_index, block_height, authority, kind, payload,
                      transaction_status, created_at, indexed_at
               FROM mn.instructions
               WHERE encode(transaction_hash, 'hex') || ':' || lpad(instruction_index::text, 10, '0') > $1
               ORDER BY 1 LIMIT $2"#,
        )
        .bind(&cursor)
        .bind(batch)
        .fetch_all(source)
        .await
        .context("reading legacy mn.instructions batch")?;
        if rows.is_empty() {
            break;
        }
        for r in &rows {
            cursor = r.try_get("k")?;
            sqlx::query!(
                r#"
                INSERT INTO mn.instructions (transaction_hash, instruction_index, block_height, authority, kind,
                                             payload, transaction_status, created_at, indexed_at)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                ON CONFLICT (transaction_hash, instruction_index) DO NOTHING
                "#,
                r.try_get::<Vec<u8>, _>("transaction_hash")?,
                r.try_get::<i32, _>("instruction_index")?,
                r.try_get::<i64, _>("block_height")?,
                r.try_get::<String, _>("authority")?,
                r.try_get::<String, _>("kind")?,
                r.try_get::<serde_json::Value, _>("payload")?,
                r.try_get::<String, _>("transaction_status")?,
                r.try_get::<DateTime<Utc>, _>("created_at")?,
                r.try_get::<DateTime<Utc>, _>("indexed_at")?,
            )
            .execute(target)
            .await
            .context("inserting mn instruction")?;
        }
        copied += rows.len() as u64;
        set_cursor(target, "mn_instructions", &cursor, rows.len() as i64).await?;
        info!(copied, "mn_instructions progress");
    }
    Ok(copied)
}

async fn copy_domains(source: &PgPool, target: &PgPool, batch: i64) -> Result<u64> {
    let mut cursor = get_cursor(target, "mn_domains").await?.unwrap_or_default();
    let mut copied = 0u64;
    loop {
        let rows = sqlx::query(
            r#"SELECT id, owned_by, accounts_count, assets_count, nfts_count, metadata, indexed_at, updated_at
               FROM mn.domains WHERE id > $1 ORDER BY id LIMIT $2"#,
        )
        .bind(&cursor)
        .bind(batch)
        .fetch_all(source)
        .await
        .context("reading legacy mn.domains batch")?;
        if rows.is_empty() {
            break;
        }
        for r in &rows {
            cursor = r.try_get("id")?;
            sqlx::query!(
                r#"
                INSERT INTO mn.domains (id, owned_by, accounts_count, assets_count, nfts_count, metadata, indexed_at, updated_at)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                ON CONFLICT (id) DO NOTHING
                "#,
                cursor,
                r.try_get::<String, _>("owned_by")?,
                r.try_get::<i32, _>("accounts_count")?,
                r.try_get::<i32, _>("assets_count")?,
                r.try_get::<i32, _>("nfts_count")?,
                r.try_get::<serde_json::Value, _>("metadata")?,
                r.try_get::<DateTime<Utc>, _>("indexed_at")?,
                r.try_get::<DateTime<Utc>, _>("updated_at")?,
            )
            .execute(target)
            .await
            .context("inserting mn domain")?;
        }
        copied += rows.len() as u64;
        set_cursor(target, "mn_domains", &cursor, rows.len() as i64).await?;
    }
    Ok(copied)
}

async fn copy_asset_definitions(source: &PgPool, target: &PgPool, batch: i64) -> Result<u64> {
    let mut cursor = get_cursor(target, "mn_asset_definitions")
        .await?
        .unwrap_or_default();
    let mut copied = 0u64;
    loop {
        let rows = sqlx::query(
            r#"SELECT id, alias, name, description, owned_by, mintable, confidential_mode, balance_scope_policy,
                      total_quantity::text AS total_quantity, metadata, indexed_at, updated_at
               FROM mn.asset_definitions WHERE id > $1 ORDER BY id LIMIT $2"#,
        )
        .bind(&cursor)
        .bind(batch)
        .fetch_all(source)
        .await
        .context("reading legacy mn.asset_definitions batch")?;
        if rows.is_empty() {
            break;
        }
        for r in &rows {
            cursor = r.try_get("id")?;
            sqlx::query!(
                r#"
                INSERT INTO mn.asset_definitions (id, alias, name, description, owned_by, mintable, confidential_mode,
                                                  balance_scope_policy, total_quantity, metadata, indexed_at, updated_at)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9::TEXT::NUMERIC, $10, $11, $12)
                ON CONFLICT (id) DO NOTHING
                "#,
                cursor,
                r.try_get::<Option<String>, _>("alias")?,
                r.try_get::<Option<String>, _>("name")?,
                r.try_get::<Option<String>, _>("description")?,
                r.try_get::<String, _>("owned_by")?,
                r.try_get::<Option<String>, _>("mintable")?,
                r.try_get::<Option<String>, _>("confidential_mode")?,
                r.try_get::<Option<String>, _>("balance_scope_policy")?,
                r.try_get::<Option<String>, _>("total_quantity")?,
                r.try_get::<serde_json::Value, _>("metadata")?,
                r.try_get::<DateTime<Utc>, _>("indexed_at")?,
                r.try_get::<DateTime<Utc>, _>("updated_at")?,
            )
            .execute(target)
            .await
            .context("inserting mn asset definition")?;
        }
        copied += rows.len() as u64;
        set_cursor(target, "mn_asset_definitions", &cursor, rows.len() as i64).await?;
    }
    Ok(copied)
}

async fn copy_assets(source: &PgPool, target: &PgPool, batch: i64) -> Result<u64> {
    let mut cursor = get_cursor(target, "mn_assets").await?.unwrap_or_default();
    let mut copied = 0u64;
    loop {
        let rows = sqlx::query(
            r#"SELECT definition_id || E'\x1f' || account_id AS k, definition_id, account_id, value::text AS value, updated_at
               FROM mn.assets WHERE definition_id || E'\x1f' || account_id > $1 ORDER BY 1 LIMIT $2"#,
        )
        .bind(&cursor)
        .bind(batch)
        .fetch_all(source)
        .await
        .context("reading legacy mn.assets batch")?;
        if rows.is_empty() {
            break;
        }
        for r in &rows {
            cursor = r.try_get("k")?;
            sqlx::query!(
                r#"
                INSERT INTO mn.assets (definition_id, account_id, value, updated_at)
                VALUES ($1, $2, $3::TEXT::NUMERIC, $4)
                ON CONFLICT (definition_id, account_id) DO NOTHING
                "#,
                r.try_get::<String, _>("definition_id")?,
                r.try_get::<String, _>("account_id")?,
                r.try_get::<String, _>("value")?,
                r.try_get::<DateTime<Utc>, _>("updated_at")?,
            )
            .execute(target)
            .await
            .context("inserting mn asset")?;
        }
        copied += rows.len() as u64;
        set_cursor(target, "mn_assets", &cursor, rows.len() as i64).await?;
    }
    Ok(copied)
}

async fn copy_peers(source: &PgPool, target: &PgPool, batch: i64) -> Result<u64> {
    let mut cursor = get_cursor(target, "mn_peers").await?.unwrap_or_default();
    let mut copied = 0u64;
    loop {
        let rows = sqlx::query(
            r#"SELECT multiaddr, public_key, ip_address, port, first_seen_at, last_seen_at, is_active
               FROM mn.peers WHERE multiaddr > $1 ORDER BY multiaddr LIMIT $2"#,
        )
        .bind(&cursor)
        .bind(batch)
        .fetch_all(source)
        .await
        .context("reading legacy mn.peers batch")?;
        if rows.is_empty() {
            break;
        }
        for r in &rows {
            cursor = r.try_get("multiaddr")?;
            sqlx::query!(
                r#"
                INSERT INTO mn.peers (multiaddr, public_key, ip_address, port, first_seen_at, last_seen_at, is_active)
                VALUES ($1, $2, $3, $4, $5, $6, $7)
                ON CONFLICT (multiaddr) DO NOTHING
                "#,
                cursor,
                r.try_get::<Option<String>, _>("public_key")?,
                r.try_get::<Option<String>, _>("ip_address")?,
                r.try_get::<Option<i32>, _>("port")?,
                r.try_get::<DateTime<Utc>, _>("first_seen_at")?,
                r.try_get::<DateTime<Utc>, _>("last_seen_at")?,
                r.try_get::<bool, _>("is_active")?,
            )
            .execute(target)
            .await
            .context("inserting mn peer")?;
        }
        copied += rows.len() as u64;
        set_cursor(target, "mn_peers", &cursor, rows.len() as i64).await?;
    }
    Ok(copied)
}

/// Single row; the legacy snapshot is only written when the target has
/// none (the live poller owns it afterwards).
async fn copy_network_state(source: &PgPool, target: &PgPool) -> Result<u64> {
    let Some(r) = sqlx::query(r#"SELECT * FROM mn.network_state WHERE id = 1"#)
        .fetch_optional(source)
        .await
        .context("reading legacy mn.network_state")?
    else {
        return Ok(0);
    };
    let res = sqlx::query!(
        r#"
        INSERT INTO mn.network_state (id, peers, domains, accounts, assets, transactions_accepted,
                                      transactions_rejected, block_height, finalized_block, avg_commit_time_ms,
                                      avg_block_time_ms, last_block_at, iroha_version, updated_at)
        VALUES (1, $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
        ON CONFLICT (id) DO NOTHING
        "#,
        r.try_get::<i32, _>("peers")?,
        r.try_get::<i32, _>("domains")?,
        r.try_get::<i32, _>("accounts")?,
        r.try_get::<i32, _>("assets")?,
        r.try_get::<i64, _>("transactions_accepted")?,
        r.try_get::<i64, _>("transactions_rejected")?,
        r.try_get::<i64, _>("block_height")?,
        r.try_get::<i64, _>("finalized_block")?,
        r.try_get::<i32, _>("avg_commit_time_ms")?,
        r.try_get::<i64, _>("avg_block_time_ms")?,
        r.try_get::<Option<DateTime<Utc>>, _>("last_block_at")?,
        r.try_get::<Option<String>, _>("iroha_version")?,
        r.try_get::<DateTime<Utc>, _>("updated_at")?,
    )
    .execute(target)
    .await
    .context("inserting mn network_state")?;
    Ok(res.rows_affected())
}

async fn copy_indexer_state(source: &PgPool, target: &PgPool) -> Result<u64> {
    let rows = sqlx::query(r#"SELECT name, last_value, last_run_at, last_run_status, error_count, last_error FROM mn.indexer_state ORDER BY name"#)
        .fetch_all(source)
        .await
        .context("reading legacy mn.indexer_state")?;
    let mut copied = 0u64;
    for r in &rows {
        let res = sqlx::query!(
            r#"
            INSERT INTO mn.indexer_state (name, last_value, last_run_at, last_run_status, error_count, last_error)
            VALUES ($1, $2, $3, $4, $5, $6)
            ON CONFLICT (name) DO NOTHING
            "#,
            r.try_get::<String, _>("name")?,
            r.try_get::<serde_json::Value, _>("last_value")?,
            r.try_get::<DateTime<Utc>, _>("last_run_at")?,
            r.try_get::<Option<String>, _>("last_run_status")?,
            r.try_get::<i32, _>("error_count")?,
            r.try_get::<Option<String>, _>("last_error")?,
        )
        .execute(target)
        .await
        .context("inserting mn indexer_state")?;
        copied += res.rows_affected();
    }
    Ok(copied)
}

async fn copy_metrics_snapshots(source: &PgPool, target: &PgPool, batch: i64) -> Result<u64> {
    let mut cursor: i64 = get_cursor(target, "mn_metrics_snapshots")
        .await?
        .map(|c| c.parse())
        .transpose()?
        .unwrap_or(0);
    let mut copied = 0u64;
    loop {
        let rows = sqlx::query(
            r#"SELECT id, ts, metric_name, labels, value FROM mn.metrics_snapshots WHERE id > $1 ORDER BY id LIMIT $2"#,
        )
        .bind(cursor)
        .bind(batch)
        .fetch_all(source)
        .await
        .context("reading legacy mn.metrics_snapshots batch")?;
        if rows.is_empty() {
            break;
        }
        let mut ids = Vec::with_capacity(rows.len());
        let mut ts = Vec::with_capacity(rows.len());
        let mut names = Vec::with_capacity(rows.len());
        let mut labels = Vec::with_capacity(rows.len());
        let mut values = Vec::with_capacity(rows.len());
        for r in &rows {
            cursor = r.try_get("id")?;
            ids.push(cursor);
            ts.push(r.try_get::<DateTime<Utc>, _>("ts")?);
            names.push(r.try_get::<String, _>("metric_name")?);
            labels.push(r.try_get::<serde_json::Value, _>("labels")?);
            values.push(r.try_get::<f64, _>("value")?);
        }
        sqlx::query!(
            r#"
            INSERT INTO mn.metrics_snapshots (id, ts, metric_name, labels, value)
            SELECT * FROM UNNEST($1::BIGINT[], $2::TIMESTAMPTZ[], $3::TEXT[], $4::JSONB[], $5::FLOAT8[])
            ON CONFLICT (id) DO NOTHING
            "#,
            &ids,
            &ts,
            &names,
            &labels,
            &values
        )
        .execute(target)
        .await
        .context("inserting mn metrics batch")?;
        copied += rows.len() as u64;
        set_cursor(
            target,
            "mn_metrics_snapshots",
            &cursor.to_string(),
            rows.len() as i64,
        )
        .await?;
        info!(copied, cursor, "mn_metrics_snapshots progress");
    }
    // Keep the sequence beyond the copied ids so live inserts never collide.
    sqlx::query!(
        r#"SELECT setval('mn.metrics_snapshots_id_seq', GREATEST((SELECT COALESCE(MAX(id), 1) FROM mn.metrics_snapshots), 1))"#
    )
    .fetch_one(target)
    .await?;
    Ok(copied)
}

/// `(count, checksum)` of a table on one side. The checksum folds the
/// key columns so a count match with different rows still fails.
async fn fingerprint(pool: &PgPool, sql: &str) -> Result<(i64, String)> {
    let r = sqlx::query(sql).fetch_one(pool).await?;
    Ok((r.try_get::<i64, _>("cnt")?, r.try_get::<String, _>("sum")?))
}

fn fingerprint_sql(table: &str) -> &'static str {
    match table {
        "mn_blocks" => "SELECT COUNT(*)::bigint AS cnt, COALESCE(SUM(height)::text, '0') || ':' || COALESCE(md5(string_agg(encode(hash,'hex'), ',' ORDER BY height)), '') AS sum FROM mn.blocks",
        "mn_accounts" => "SELECT COUNT(*)::bigint AS cnt, COALESCE(md5(string_agg(id, ',' ORDER BY id)), '') AS sum FROM mn.accounts",
        "mn_transactions" => "SELECT COUNT(*)::bigint AS cnt, COALESCE(md5(string_agg(encode(hash,'hex') || COALESCE(sora_v2_claim_tx_hash, ''), ',' ORDER BY encode(hash,'hex'))), '') AS sum FROM mn.transactions",
        "mn_instructions" => "SELECT COUNT(*)::bigint AS cnt, COALESCE(md5(string_agg(encode(transaction_hash,'hex') || ':' || instruction_index, ',' ORDER BY encode(transaction_hash,'hex'), instruction_index)), '') AS sum FROM mn.instructions",
        "mn_domains" => "SELECT COUNT(*)::bigint AS cnt, COALESCE(md5(string_agg(id, ',' ORDER BY id)), '') AS sum FROM mn.domains",
        "mn_asset_definitions" => "SELECT COUNT(*)::bigint AS cnt, COALESCE(md5(string_agg(id || ':' || COALESCE(total_quantity::text, ''), ',' ORDER BY id)), '') AS sum FROM mn.asset_definitions",
        "mn_assets" => "SELECT COUNT(*)::bigint AS cnt, COALESCE(md5(string_agg(definition_id || '#' || account_id || ':' || value::text, ',' ORDER BY definition_id, account_id)), '') AS sum FROM mn.assets",
        "mn_peers" => "SELECT COUNT(*)::bigint AS cnt, COALESCE(md5(string_agg(multiaddr, ',' ORDER BY multiaddr)), '') AS sum FROM mn.peers",
        "mn_network_state" => "SELECT COUNT(*)::bigint AS cnt, COALESCE(MAX(block_height)::text, '') AS sum FROM mn.network_state",
        "mn_indexer_state" => "SELECT COUNT(*)::bigint AS cnt, COALESCE(md5(string_agg(name, ',' ORDER BY name)), '') AS sum FROM mn.indexer_state",
        "mn_metrics_snapshots" => "SELECT COUNT(*)::bigint AS cnt, COALESCE(SUM(id)::text, '0') AS sum FROM mn.metrics_snapshots",
        _ => "SELECT 0::bigint AS cnt, '' AS sum",
    }
}

/// Exact count + key checksum on both sides. `mn_network_state` and
/// `mn_indexer_state` only require the target to hold at least the
/// legacy rows (the live poller rewrites them).
pub async fn reconcile(source: &PgPool, target: &PgPool, table: &str) -> Result<bool> {
    let sql = fingerprint_sql(table);
    let (s_cnt, s_sum) = fingerprint(source, sql).await?;
    let (d_cnt, d_sum) = fingerprint(target, sql).await?;
    let ok = match table {
        "mn_network_state" | "mn_indexer_state" => d_cnt >= s_cnt,
        _ => s_cnt == d_cnt && s_sum == d_sum,
    };
    if ok {
        info!(table, count = s_cnt, "RECONCILE OK");
    } else {
        warn!(table, source_count = s_cnt, target_count = d_cnt, source_sum = %s_sum, target_sum = %d_sum, "RECONCILE FAIL");
    }
    Ok(ok)
}
