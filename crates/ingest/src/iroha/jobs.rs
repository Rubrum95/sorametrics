//! The polling jobs. Each returns the `last_value` JSON stored in
//! `mn.indexer_state` (the Node's `{ upserts, total_seen }` style).

use anyhow::{anyhow, Context, Result};
use chrono::{DateTime, Utc};
use futures::future::BoxFuture;
use serde_json::{json, Value};
use sorametrics_core::minamoto::{
    hash32_from_hex, MetricSample, MnAccount, MnAsset, MnAssetDefinition, MnBlock, MnClaimMetadata,
    MnDomain, MnInstruction, MnNetworkState, MnPeer, MnTransaction,
};
use sorametrics_db::mn;
use sorametrics_iroha::dto::{
    AccountDto, AssetDefinitionDto, AssetDto, BlockDto, DomainDto, InstructionDto, TransactionDto,
};
use sorametrics_iroha::prom;
use sorametrics_iroha::torii::{IsiFilters, TxFilters, EXPLORER_MAX_LIMIT};
use sorametrics_iroha::ToriiClient;
use sqlx::PgPool;
use tracing::{info, warn};

/// Shared job context.
pub struct Ctx {
    /// Target pool.
    pub db: PgPool,
    /// Torii client.
    pub torii: ToriiClient,
    /// Pages a backfill walks at most.
    pub backfill_max_pages: u32,
    /// Snapshot retention, days.
    pub metrics_retention_days: i32,
}

/// A job: borrows the context, returns its `last_value`.
pub type JobFn = for<'a> fn(&'a Ctx) -> BoxFuture<'a, Result<Value>>;

/// Rows per page of the incremental (newest-first) polls.
const LIVE_PAGE: u32 = 25;

fn parse_time(raw: &str, what: &str) -> Result<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(raw)
        .map(|t| t.with_timezone(&Utc))
        .with_context(|| format!("{what} '{raw}' is not RFC 3339"))
}

fn block_row(b: &BlockDto) -> Result<MnBlock> {
    Ok(MnBlock {
        height: i64::try_from(b.height).context("block height overflows i64")?,
        hash: hash32_from_hex(&b.hash).map_err(|e| anyhow!("block {}: {e}", b.height))?,
        prev_hash: b
            .prev_block_hash
            .as_deref()
            .map(|h| hash32_from_hex(h).map_err(|e| anyhow!("block {}: {e}", b.height)))
            .transpose()?,
        transactions_hash: b
            .transactions_hash
            .as_deref()
            .map(|h| hash32_from_hex(h).map_err(|e| anyhow!("block {}: {e}", b.height)))
            .transpose()?,
        created_at: parse_time(&b.created_at, "block created_at")?,
        transactions_committed: i32::try_from(b.transactions_total)
            .context("transactions_total")?,
        transactions_rejected: i32::try_from(b.transactions_rejected)
            .context("transactions_rejected")?,
    })
}

fn tx_row(t: &TransactionDto) -> Result<MnTransaction> {
    Ok(MnTransaction {
        hash: hash32_from_hex(&t.hash).map_err(|e| anyhow!("transaction: {e}"))?,
        block_height: i64::try_from(t.block).context("tx block overflows i64")?,
        authority: t.authority.clone(),
        created_at: parse_time(&t.created_at, "transaction created_at")?,
        executable_kind: t.executable.clone(),
        status: t.status.clone(),
    })
}

fn isi_row(i: &InstructionDto) -> Result<MnInstruction> {
    let payload = i.payload().cloned().ok_or_else(|| {
        anyhow!(
            "instruction {}#{} has no box.json.payload",
            i.transaction_hash,
            i.index
        )
    })?;
    Ok(MnInstruction {
        transaction_hash: hash32_from_hex(&i.transaction_hash)
            .map_err(|e| anyhow!("instruction: {e}"))?,
        instruction_index: i32::try_from(i.index).context("instruction index")?,
        block_height: i64::try_from(i.block).context("instruction block")?,
        authority: i.authority.clone(),
        kind: i.kind.clone(),
        payload,
        transaction_status: i.transaction_status.clone(),
        created_at: parse_time(&i.created_at, "instruction created_at")?,
    })
}

fn account_row(a: &AccountDto) -> MnAccount {
    let multisig = a.metadata.get("multisig/spec");
    MnAccount {
        id: a.id.clone(),
        network_prefix: i32::from(a.network_prefix),
        primary_alias: None,
        primary_alias_dataspace: None,
        primary_alias_domain: None,
        primary_alias_name: None,
        multisig_quorum: multisig
            .and_then(|m| m.get("quorum"))
            .and_then(|q| q.as_i64())
            .and_then(|q| i32::try_from(q).ok()),
        multisig_signatories_count: multisig
            .and_then(|m| m.get("signatories"))
            .and_then(|s| s.as_object())
            .and_then(|s| i32::try_from(s.len()).ok()),
        metadata: a.metadata.clone(),
    }
}

fn domain_row(d: &DomainDto) -> Result<MnDomain> {
    Ok(MnDomain {
        id: d.id.clone(),
        owned_by: d.owned_by.clone(),
        accounts_count: i32::try_from(d.accounts).context("domain accounts")?,
        assets_count: i32::try_from(d.assets).context("domain assets")?,
        nfts_count: i32::try_from(d.nfts).context("domain nfts")?,
        metadata: d.metadata.clone(),
    })
}

fn asset_row(a: &AssetDto) -> MnAsset {
    MnAsset {
        definition_id: a.definition_id.clone(),
        account_id: a.account_id.clone(),
        value: a.value.clone(),
    }
}

fn definition_row(d: &AssetDefinitionDto) -> MnAssetDefinition {
    MnAssetDefinition {
        id: d.id.clone(),
        alias: d.alias.clone(),
        name: d.name.clone(),
        description: d.description.clone(),
        owned_by: d.owned_by.clone(),
        mintable: d.mintable.clone(),
        confidential_mode: d.confidential_mode(),
        balance_scope_policy: d.balance_scope_policy.clone(),
        total_quantity: d.total_quantity_text(),
        metadata: d.metadata.clone(),
    }
}

// ---------------------------------------------------------------------
// network state
// ---------------------------------------------------------------------

/// `/status` + indexed counts → `mn.network_state`.
pub fn network_state(ctx: &Ctx) -> BoxFuture<'_, Result<Value>> {
    Box::pin(async move {
        let s = ctx.torii.status_snapshot().await.context("GET /status")?;
        let counts = mn::indexed_counts(&ctx.db).await?;
        let blocks = i64::try_from(s.blocks).context("blocks overflows i64")?;
        let state = MnNetworkState {
            peers: i32::try_from(s.peers).context("peers")?,
            domains: counts.domains,
            accounts: counts.accounts,
            assets: counts.assets,
            transactions_accepted: i64::try_from(s.txs_approved).context("txs_approved")?,
            transactions_rejected: i64::try_from(s.txs_rejected).context("txs_rejected")?,
            block_height: blocks,
            finalized_block: blocks,
            avg_commit_time_ms: i32::try_from(s.commit_time_ms.min(i32::MAX as u64))
                .context("commit_time_ms")?,
            avg_block_time_ms: counts.avg_block_ms.unwrap_or(0),
            last_block_at: counts.last_block_at,
            iroha_version: Some(s.build.version.clone()),
        };
        mn::upsert_network_state(&ctx.db, &state).await?;
        Ok(json!({
            "block": s.blocks,
            "peers": s.peers,
            "version": s.build.version,
            "git_commit_sha": s.build.git_commit_sha,
        }))
    })
}

// ---------------------------------------------------------------------
// blocks
// ---------------------------------------------------------------------

/// Newest page of blocks. A height already indexed with a different hash
/// means the chain restarted: the chain tables are truncated and the
/// history is backfilled again.
pub fn blocks(ctx: &Ctx) -> BoxFuture<'_, Result<Value>> {
    Box::pin(async move {
        let page = ctx
            .torii
            .explorer_blocks(None, LIVE_PAGE)
            .await
            .context("GET /v1/explorer/blocks")?;
        let rows = page
            .items
            .iter()
            .map(block_row)
            .collect::<Result<Vec<_>>>()?;
        let reset = detect_reset(ctx, &rows).await?;
        if reset {
            warn!("chain reset detected (height re-served with another hash): truncating mn chain tables");
            mn::truncate_chain_tables(&ctx.db).await?;
        }
        let upserts = mn::upsert_blocks(&ctx.db, &rows).await?;
        if reset {
            let r = blocks_backfill(ctx).await.context("backfill after reset")?;
            info!(result = %r, "backfill after reset done");
        }
        Ok(json!({
            "upserts": upserts,
            "snapshot_height": page.pagination.snapshot_height,
            "reset": reset,
        }))
    })
}

async fn detect_reset(ctx: &Ctx, rows: &[MnBlock]) -> Result<bool> {
    for b in rows {
        if let Some(stored) = mn::block_hash_at(&ctx.db, b.height).await? {
            if stored != b.hash {
                return Ok(true);
            }
        }
    }
    Ok(false)
}

/// Walks every block page (newest first) once.
pub fn blocks_backfill(ctx: &Ctx) -> BoxFuture<'_, Result<Value>> {
    Box::pin(async move {
        let mut cursor: Option<String> = None;
        let mut total = 0u64;
        let mut pages = 0u32;
        let mut snapshot = None;
        loop {
            let page = ctx
                .torii
                .explorer_blocks(cursor.as_deref(), EXPLORER_MAX_LIMIT)
                .await
                .context("GET /v1/explorer/blocks (backfill)")?;
            snapshot.get_or_insert(page.pagination.snapshot_height);
            let rows = page
                .items
                .iter()
                .map(block_row)
                .collect::<Result<Vec<_>>>()?;
            total += mn::upsert_blocks(&ctx.db, &rows).await?;
            pages += 1;
            cursor = page.pagination.next_cursor.clone();
            if !page.pagination.has_more || cursor.is_none() || pages >= ctx.backfill_max_pages {
                break;
            }
        }
        let missing = match snapshot {
            Some(h) => mn::missing_heights(&ctx.db, i64::try_from(h)?, 1)
                .await?
                .len(),
            None => 0,
        };
        Ok(
            json!({ "total": total, "pages": pages, "snapshot_height": snapshot, "gaps": missing > 0 }),
        )
    })
}

// ---------------------------------------------------------------------
// transactions / instructions
// ---------------------------------------------------------------------

/// Newest page of transactions (rows whose block is not indexed yet are
/// skipped; the blocks job catches up).
pub fn transactions(ctx: &Ctx) -> BoxFuture<'_, Result<Value>> {
    Box::pin(async move {
        let page = ctx
            .torii
            .explorer_transactions(&TxFilters::default(), None, LIVE_PAGE)
            .await
            .context("GET /v1/explorer/transactions")?;
        let rows = page.items.iter().map(tx_row).collect::<Result<Vec<_>>>()?;
        let (upserts, skipped) = mn::upsert_transactions(&ctx.db, &rows).await?;
        Ok(
            json!({ "upserts": upserts, "skipped": skipped, "snapshot_height": page.pagination.snapshot_height }),
        )
    })
}

/// Walks every transaction page once.
pub fn transactions_backfill(ctx: &Ctx) -> BoxFuture<'_, Result<Value>> {
    Box::pin(async move {
        let mut cursor: Option<String> = None;
        let (mut ok, mut skipped, mut pages) = (0u64, 0u64, 0u32);
        loop {
            let page = ctx
                .torii
                .explorer_transactions(&TxFilters::default(), cursor.as_deref(), EXPLORER_MAX_LIMIT)
                .await
                .context("GET /v1/explorer/transactions (backfill)")?;
            let rows = page.items.iter().map(tx_row).collect::<Result<Vec<_>>>()?;
            let (w, s) = mn::upsert_transactions(&ctx.db, &rows).await?;
            ok += w;
            skipped += s;
            pages += 1;
            cursor = page.pagination.next_cursor.clone();
            if !page.pagination.has_more || cursor.is_none() || pages >= ctx.backfill_max_pages {
                break;
            }
        }
        Ok(json!({ "upserts": ok, "skipped": skipped, "pages": pages }))
    })
}

/// Newest page of instructions.
pub fn instructions(ctx: &Ctx) -> BoxFuture<'_, Result<Value>> {
    Box::pin(async move {
        let page = ctx
            .torii
            .explorer_instructions(&IsiFilters::default(), None, LIVE_PAGE)
            .await
            .context("GET /v1/explorer/instructions")?;
        let rows = page.items.iter().map(isi_row).collect::<Result<Vec<_>>>()?;
        let upserts = mn::upsert_instructions(&ctx.db, &rows).await?;
        Ok(json!({ "upserts": upserts, "snapshot_height": page.pagination.snapshot_height }))
    })
}

/// Walks every instruction page once.
pub fn instructions_backfill(ctx: &Ctx) -> BoxFuture<'_, Result<Value>> {
    Box::pin(async move {
        let mut cursor: Option<String> = None;
        let (mut total, mut pages) = (0u64, 0u32);
        loop {
            let page = ctx
                .torii
                .explorer_instructions(
                    &IsiFilters::default(),
                    cursor.as_deref(),
                    EXPLORER_MAX_LIMIT,
                )
                .await
                .context("GET /v1/explorer/instructions (backfill)")?;
            let rows = page.items.iter().map(isi_row).collect::<Result<Vec<_>>>()?;
            total += mn::upsert_instructions(&ctx.db, &rows).await?;
            pages += 1;
            cursor = page.pagination.next_cursor.clone();
            if !page.pagination.has_more || cursor.is_none() || pages >= ctx.backfill_max_pages {
                break;
            }
        }
        Ok(json!({ "total": total, "pages": pages }))
    })
}

// ---------------------------------------------------------------------
// world collections
// ---------------------------------------------------------------------

/// All domains (owner stub first, as the Node did).
pub fn domains(ctx: &Ctx) -> BoxFuture<'_, Result<Value>> {
    Box::pin(async move {
        let mut cursor: Option<String> = None;
        let mut upserts = 0u64;
        loop {
            let page = ctx
                .torii
                .explorer_domains(cursor.as_deref(), EXPLORER_MAX_LIMIT)
                .await
                .context("GET /v1/explorer/domains")?;
            for d in &page.items {
                mn::ensure_account_stub(&ctx.db, &d.owned_by).await?;
                mn::upsert_domain(&ctx.db, &domain_row(d)?).await?;
                upserts += 1;
            }
            cursor = page.pagination.next_cursor.clone();
            if !page.pagination.has_more || cursor.is_none() {
                break;
            }
        }
        Ok(json!({ "upserts": upserts }))
    })
}

/// All accounts.
pub fn accounts(ctx: &Ctx) -> BoxFuture<'_, Result<Value>> {
    Box::pin(async move {
        let mut cursor: Option<String> = None;
        let mut upserts = 0u64;
        loop {
            let page = ctx
                .torii
                .explorer_accounts(cursor.as_deref(), EXPLORER_MAX_LIMIT)
                .await
                .context("GET /v1/explorer/accounts")?;
            for a in &page.items {
                mn::upsert_account(&ctx.db, &account_row(a)).await?;
                upserts += 1;
            }
            cursor = page.pagination.next_cursor.clone();
            if !page.pagination.has_more || cursor.is_none() {
                break;
            }
        }
        Ok(json!({ "upserts": upserts }))
    })
}

/// All balances.
pub fn assets(ctx: &Ctx) -> BoxFuture<'_, Result<Value>> {
    Box::pin(async move {
        let mut cursor: Option<String> = None;
        let mut upserts = 0u64;
        loop {
            let page = ctx
                .torii
                .explorer_assets(cursor.as_deref(), EXPLORER_MAX_LIMIT)
                .await
                .context("GET /v1/explorer/assets")?;
            for a in &page.items {
                mn::upsert_asset(&ctx.db, &asset_row(a)).await?;
                upserts += 1;
            }
            cursor = page.pagination.next_cursor.clone();
            if !page.pagination.has_more || cursor.is_none() {
                break;
            }
        }
        Ok(json!({ "upserts": upserts }))
    })
}

/// All asset definitions (offset pages).
pub fn asset_definitions(ctx: &Ctx) -> BoxFuture<'_, Result<Value>> {
    Box::pin(async move {
        let mut offset = 0u64;
        let mut upserts = 0u64;
        let mut total = None;
        loop {
            let page = ctx
                .torii
                .asset_definitions(EXPLORER_MAX_LIMIT, offset)
                .await
                .context("GET /v1/assets/definitions")?;
            total = total.or(page.total);
            for d in &page.items {
                mn::ensure_account_stub(&ctx.db, &d.owned_by).await?;
                mn::upsert_asset_definition(&ctx.db, &definition_row(d)).await?;
                upserts += 1;
            }
            offset += page.items.len() as u64;
            if !page.has_more || page.items.is_empty() {
                break;
            }
        }
        Ok(json!({ "upserts": upserts, "total": total }))
    })
}

/// Connected peers = distinct `connected_peers` pubkeys of every source
/// (Iroha rc2 dropped `/peers`; no ip:port available).
pub fn peers(ctx: &Ctx) -> BoxFuture<'_, Result<Value>> {
    Box::pin(async move {
        let sources = ctx
            .torii
            .peers_info()
            .await
            .context("GET /v1/telemetry/peers-info")?;
        let mut pubkeys: Vec<String> = sources
            .iter()
            .flat_map(|s| s.connected_peers.iter().flatten().cloned())
            .collect();
        pubkeys.sort();
        pubkeys.dedup();
        for pk in &pubkeys {
            mn::upsert_peer(
                &ctx.db,
                &MnPeer {
                    multiaddr: pk.clone(),
                    public_key: Some(pk.clone()),
                    ip_address: None,
                    port: None,
                },
            )
            .await?;
        }
        let deactivated = mn::deactivate_stale_peers(&ctx.db, &pubkeys).await?;
        Ok(json!({ "upserts": pubkeys.len(), "deactivated": deactivated }))
    })
}

// ---------------------------------------------------------------------
// prometheus
// ---------------------------------------------------------------------

/// One `/metrics` scrape → `mn.metrics_snapshots`.
pub fn prometheus(ctx: &Ctx) -> BoxFuture<'_, Result<Value>> {
    Box::pin(async move {
        let text = ctx.torii.metrics().await.context("GET /metrics")?;
        let samples: Vec<MetricSample> = prom::parse(&text)
            .into_iter()
            .map(|s| MetricSample {
                metric_name: s.name,
                labels: json!(s.labels),
                value: s.value,
            })
            .collect();
        let inserted = mn::insert_metric_samples(&ctx.db, &samples, Utc::now()).await?;
        Ok(json!({ "inserted": inserted, "total_parsed": samples.len() }))
    })
}

/// Drops snapshots older than the retention.
pub fn metrics_cleanup(ctx: &Ctx) -> BoxFuture<'_, Result<Value>> {
    Box::pin(async move {
        let deleted = mn::prune_metric_samples(&ctx.db, ctx.metrics_retention_days).await?;
        Ok(json!({ "deleted": deleted }))
    })
}

// ---------------------------------------------------------------------
// cross-chain claims
// ---------------------------------------------------------------------

/// Transaction detail `metadata` → claim columns, for committed
/// transactions not checked yet. A failed detail fetch is counted and
/// logged; the row stays unchecked and is retried next pass.
pub fn claims_enrich(ctx: &Ctx) -> BoxFuture<'_, Result<Value>> {
    Box::pin(async move {
        let hashes = mn::list_claims_to_enrich(&ctx.db, 50).await?;
        let (mut enriched, mut failed) = (0u32, 0u32);
        for h in &hashes {
            let hex = mn::hex_of(h);
            let detail = match ctx.torii.transaction(&hex).await {
                Ok(d) => d,
                Err(e) => {
                    failed += 1;
                    warn!(tx = %hex, error = %e, "claims_enrich: transaction detail failed");
                    continue;
                }
            };
            let md = detail.get("metadata").cloned().unwrap_or(Value::Null);
            let Some(v2_hash) = md.get("sora_v2_claim_tx_hash").and_then(|v| v.as_str()) else {
                continue;
            };
            let meta = MnClaimMetadata {
                sora_v2_claim_tx_hash: v2_hash.to_string(),
                sora_nexus_claim_recipient: md
                    .get("sora_nexus_claim_recipient")
                    .and_then(|v| v.as_str())
                    .map(String::from),
                fee_sponsor: md
                    .get("fee_sponsor")
                    .and_then(|v| v.as_str())
                    .map(String::from),
            };
            mn::update_transaction_metadata(&ctx.db, h, &meta).await?;
            enriched += 1;
        }
        Ok(json!({ "scanned": hashes.len(), "enriched": enriched, "failed": failed }))
    })
}

/// Resolves the SORA v2 side of each claim from `sm.extrinsics`
/// (read-only). Unindexed burns stay pending and are retried.
pub fn claims_v2_resolve(ctx: &Ctx) -> BoxFuture<'_, Result<Value>> {
    Box::pin(async move {
        let pending = mn::list_claims_missing_v2(&ctx.db, 50).await?;
        let mut resolved = 0u32;
        for (mn_hash, v2_hash) in &pending {
            if let Some((block, signer)) = mn::lookup_v2_burn_extrinsic(&ctx.db, v2_hash).await? {
                mn::update_transaction_v2_side(&ctx.db, mn_hash, block, &signer).await?;
                resolved += 1;
            }
        }
        Ok(json!({ "scanned": pending.len(), "resolved": resolved }))
    })
}

#[allow(dead_code)]
fn _assert_job_signatures() {
    let _jobs: [JobFn; 16] = [
        network_state,
        blocks,
        blocks_backfill,
        transactions,
        transactions_backfill,
        instructions,
        instructions_backfill,
        domains,
        accounts,
        assets,
        asset_definitions,
        peers,
        prometheus,
        metrics_cleanup,
        claims_enrich,
        claims_v2_resolve,
    ];
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn block_row_conversion() {
        let b = BlockDto {
            hash: "ab".repeat(32),
            height: 438,
            created_at: "2026-06-12T22:29:35.805Z".into(),
            prev_block_hash: Some("cd".repeat(32)),
            transactions_hash: None,
            transactions_rejected: 1,
            transactions_total: 3,
        };
        let r = block_row(&b).unwrap();
        assert_eq!(r.height, 438);
        assert_eq!(r.transactions_committed, 3);
        assert_eq!(r.created_at.timestamp_millis(), 1_781_303_375_805);
        let bad = BlockDto {
            hash: "zz".into(),
            ..b
        };
        assert!(block_row(&bad).is_err());
    }

    #[test]
    fn account_multisig_from_metadata() {
        let a = AccountDto {
            id: "sora1".into(),
            network_prefix: 753,
            metadata: json!({"multisig/spec": {"quorum": 2, "signatories": {"a": 1, "b": 1, "c": 1}}}),
            owned_domains: 0,
            owned_assets: 0,
            owned_nfts: 0,
        };
        let r = account_row(&a);
        assert_eq!(r.multisig_quorum, Some(2));
        assert_eq!(r.multisig_signatories_count, Some(3));
        assert!(r.primary_alias.is_none());
    }

    #[test]
    fn instruction_requires_structured_payload() {
        let raw = r#"{"authority":"a","created_at":"2026-05-21T02:24:54.632Z","kind":"Burn","box":{"encoded":"00","framed_sha256":"ff","json":{"kind":"Burn"}},"transaction_hash":"ab","transaction_status":"Committed","block":429,"index":0}"#;
        let i: InstructionDto = serde_json::from_str(raw).unwrap();
        assert!(isi_row(&i).is_err());
    }
}
