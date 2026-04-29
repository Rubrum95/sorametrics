'use strict';
// ============================================================
// minamoto/indexer.js — long-running poller for Minamoto Torii
// Standalone process (PM2-managed). Polls Torii REST + /metrics,
// upserts into the mn.* schema. One job per resource, independent
// intervals, isolated failure handling.
// ============================================================

const cfg = require('./config');
const db = require('./db');
const torii = require('./torii_client');
const prom = require('./prom_parser');

let _shuttingDown = false;

function log(level, ...args) {
    const ts = new Date().toISOString();
    const fn = level === 'err' ? console.error : console.log;
    fn(`[minamoto.indexer ${ts}]`, ...args);
}

// ------------------------------------------------------------
// Job runner: schedules `fn` every `intervalMs` (after first run).
// Catches errors so one job can never crash the others.
// ------------------------------------------------------------
function scheduleJob(name, intervalMs, fn) {
    const tick = async () => {
        if (_shuttingDown) return;
        const t0 = Date.now();
        try {
            const result = await fn();
            await db.recordIndexerRun(name, 'ok', result || {}, null);
            log('info', `${name} ok in ${Date.now() - t0}ms`, result || '');
        } catch (e) {
            log('err', `${name} FAILED:`, e.message);
            try {
                await db.recordIndexerRun(name, 'error', {}, e.message);
            } catch (dbErr) {
                log('err', `${name} could not record error:`, dbErr.message);
            }
        } finally {
            if (!_shuttingDown) setTimeout(tick, intervalMs);
        }
    };
    // Stagger initial runs slightly so we don't hammer Torii on boot
    const initialDelay = Math.floor(Math.random() * 2000);
    setTimeout(tick, initialDelay);
}

// ------------------------------------------------------------
// Jobs
// ------------------------------------------------------------

async function jobNetworkState() {
    const m = await torii.getExplorerMetrics();
    let irohaVersion = null;
    try {
        const s = await torii.getStatus();
        irohaVersion = s && s.build && s.build.version ? s.build.version : null;
    } catch (_) { /* status is best-effort */ }
    await db.upsertNetworkState({
        peers: m.peers | 0,
        domains: m.domains | 0,
        accounts: m.accounts | 0,
        assets: m.assets | 0,
        transactions_accepted: m.transactions_accepted | 0,
        transactions_rejected: m.transactions_rejected | 0,
        block_height: m.block | 0,
        finalized_block: m.finalized_block | 0,
        avg_commit_time_ms: m.avg_commit_time ? (m.avg_commit_time.ms | 0) : 0,
        avg_block_time_ms: m.avg_block_time ? (m.avg_block_time.ms | 0) : 0,
        last_block_at: m.block_created_at || null,
        iroha_version: irohaVersion,
    });
    return { block: m.block, peers: m.peers };
}

async function jobBlocks() {
    // Pull first page (newest); upsert keeps it idempotent.
    const r = await torii.getExplorerBlocks(1, 20);
    let upserts = 0;
    for (const b of (r.items || [])) {
        await db.upsertBlock(b);
        upserts++;
    }
    return { upserts, total_seen: r.pagination ? r.pagination.total_items : null };
}

// One-shot historical backfill. Pages through every block on Torii in DESC
// order and upserts. Stops when we've covered the full range OR hit a hard
// cap. Idempotent: re-running just hits ON CONFLICT and exits cheaply, so
// safe to re-trigger via the indexer_state row if needed.
async function jobBlocksBackfill() {
    const PER_PAGE = 50;
    const MAX_PAGES = 200; // 10k blocks ceiling, generous for early Minamoto
    let page = 1, total = 0, totalSeen = null, totalPages = 1;
    while (page <= totalPages && page <= MAX_PAGES) {
        const r = await torii.getExplorerBlocks(page, PER_PAGE);
        if (r && r.pagination) {
            totalSeen = r.pagination.total_items;
            totalPages = r.pagination.total_pages;
        }
        for (const b of (r.items || [])) {
            await db.upsertBlock(b);
            total++;
        }
        page++;
    }
    return { total, total_seen: totalSeen, total_pages: totalPages };
}

async function jobTransactionsBackfill() {
    const PER_PAGE = 50;
    const MAX_PAGES = 400; // 20k txs ceiling
    let page = 1, ok = 0, skipped = 0, totalSeen = null, totalPages = 1;
    while (page <= totalPages && page <= MAX_PAGES) {
        const r = await torii.getExplorerTransactions(page, PER_PAGE);
        if (r && r.pagination) {
            totalSeen = r.pagination.total_items;
            totalPages = r.pagination.total_pages;
        }
        for (const tx of (r.items || [])) {
            if (tx.block == null) { skipped++; continue; }
            try { await db.upsertTransaction(tx); ok++; }
            catch (e) {
                // FK violation: parent block not yet indexed. Skip and let
                // the next blocks-backfill pass catch us up.
                if (e.code === '23503') { skipped++; continue; }
                throw e;
            }
        }
        page++;
    }
    return { upserts: ok, skipped, total_seen: totalSeen };
}

async function jobTransactions() {
    const r = await torii.getExplorerTransactions(1, 50);
    let upserts = 0;
    for (const tx of (r.items || [])) {
        // tx.block may be missing for pending; skip those
        if (tx.block == null) continue;
        // Ensure parent block exists; if not, fetch shallow stub
        try { await db.upsertTransaction(tx); upserts++; }
        catch (e) {
            if (e.code === '23503') {
                // foreign key violation: block not yet indexed. Skip silently;
                // jobBlocks runs alongside and will catch up.
                continue;
            }
            throw e;
        }
    }
    return { upserts, total_seen: r.pagination ? r.pagination.total_items : null };
}

async function jobDomains() {
    const r = await torii.getExplorerDomains(1, 100);
    let upserts = 0;
    for (const d of (r.items || [])) {
        // Domain owner must exist as account (accounts have FK target via metadata).
        await db.upsertAccount({ id: d.owned_by });
        await db.upsertDomain(d);
        upserts++;
    }
    return { upserts };
}

async function jobAccounts() {
    const r = await torii.getExplorerAccounts(1, 100);
    let upserts = 0;
    for (const a of (r.items || [])) {
        const ms = a.metadata || {};
        const multisig = ms['multisig/spec'];
        await db.upsertAccount({
            id: a.id,
            network_prefix: a.network_prefix,
            has_primary_alias: !!a.primary_alias,
            primary_alias: a.primary_alias || null,
            primary_alias_dataspace: a.primary_alias_dataspace || null,
            primary_alias_domain: a.primary_alias_domain || null,
            primary_alias_name: a.primary_alias_name || null,
            multisig_quorum: multisig ? (multisig.quorum | 0) : null,
            multisig_signatories_count: multisig && multisig.signatories ? Object.keys(multisig.signatories).length : null,
            metadata: ms,
        });
        upserts++;
    }
    return { upserts };
}

async function jobAssets() {
    const r = await torii.getExplorerAssets(1, 100);
    let upserts = 0;
    for (const a of (r.items || [])) {
        await db.upsertAsset({
            definition_id: a.definition_id,
            account_id: a.account_id,
            value: a.value,
        });
        upserts++;
    }
    return { upserts };
}

async function jobAssetDefinitions() {
    const r = await torii.getAssetDefinitions();
    let upserts = 0;
    for (const d of (r.items || [])) {
        // Owner must exist as account (FK is implicit since asset_definitions
        // doesn't FK to accounts, but we still insert a stub to make joins clean).
        await db.upsertAccount({ id: d.owned_by });
        await db.upsertAssetDefinition(d);
        upserts++;
    }
    return { upserts, total: r.total };
}

// Pull tx detail for unenriched claims and copy the cross-chain metadata
// (sora_v2_claim_tx_hash, sora_nexus_claim_recipient, fee_sponsor) into typed
// columns. Cheap (one HTTP call per tx). Runs every poll cycle but only on
// tx that aren't yet checked.
async function jobClaimsEnrich() {
    const hashes = await db.listClaimsToEnrich(50);
    let enriched = 0;
    for (const h of hashes) {
        try {
            const detail = await torii.getTransactionByHash(h);
            const md = (detail && detail.metadata) || {};
            // Only persist if we found cross-chain metadata; otherwise the tx
            // is a regular non-claim and we save nothing (it'll be re-checked
            // on the next pass — acceptable for current low volume).
            if (md.sora_v2_claim_tx_hash) {
                await db.updateTransactionMetadata(h, {
                    sora_v2_claim_tx_hash: md.sora_v2_claim_tx_hash,
                    sora_nexus_claim_recipient: md.sora_nexus_claim_recipient,
                    fee_sponsor: md.fee_sponsor,
                });
                enriched++;
            }
        } catch (e) {
            // Swallow per-item errors so one bad tx doesn't kill the batch.
        }
    }
    return { scanned: hashes.length, enriched };
}

// Resolves the v2 side of a claim (block + signer) by looking the burn-tx
// hash up in the same Postgres' sm.live_extrinsics table (populated by the
// SORA v2 sorametrics indexer). Cheap — local SQL, no HTTP. Runs after the
// metadata enrich so it sees claims as soon as they're known.
async function jobClaimsResolveV2() {
    const pending = await db.listClaimsMissingV2Resolution(50);
    let resolved = 0;
    for (const p of pending) {
        try {
            const v2 = await db.lookupV2BurnExtrinsic(p.v2_hash);
            if (v2) {
                await db.updateTransactionV2Side(p.mn_hash, v2.block, v2.signer);
                resolved++;
            }
            // If v2 isn't indexed yet (the v2 indexer is behind), we skip and
            // try again next pass. Idempotent.
        } catch (e) { /* per-item swallow */ }
    }
    return { scanned: pending.length, resolved };
}

async function jobInstructions() {
    // Pull first page (newest) for incremental indexing.
    const r = await torii.getExplorerInstructions(1, 50);
    let upserts = 0;
    for (const isi of (r.items || [])) {
        await db.upsertInstruction({
            transaction_hash: isi.transaction_hash,
            instruction_index: isi.index,
            block: isi.block,
            authority: isi.authority,
            kind: isi.kind,
            // Prefer the decoded JSON when present; fallback to the raw box.
            payload: (isi.r && isi.r['#box'] && isi.r['#box'].json && isi.r['#box'].json.payload)
                ? isi.r['#box'].json.payload
                : (isi['r#box'] && isi['r#box'].json && isi['r#box'].json.payload
                    ? isi['r#box'].json.payload
                    : (isi.payload || {})),
            transaction_status: isi.transaction_status,
            created_at: isi.created_at,
        });
        upserts++;
    }
    return { upserts, total_seen: r.pagination ? r.pagination.total_items : null };
}

async function jobInstructionsBackfill() {
    const PER_PAGE = 50;
    const MAX_PAGES = 200; // 10k ceiling, generous for early Minamoto
    let page = 1, total = 0, totalSeen = null, totalPages = 1;
    while (page <= totalPages && page <= MAX_PAGES) {
        const r = await torii.getExplorerInstructions(page, PER_PAGE);
        if (r && r.pagination) {
            totalSeen = r.pagination.total_items;
            totalPages = r.pagination.total_pages;
        }
        for (const isi of (r.items || [])) {
            const box = isi['r#box'] || isi.r;
            const payload = box && box.json && box.json.payload
                ? box.json.payload
                : (isi.payload || {});
            await db.upsertInstruction({
                transaction_hash: isi.transaction_hash,
                instruction_index: isi.index,
                block: isi.block,
                authority: isi.authority,
                kind: isi.kind,
                payload,
                transaction_status: isi.transaction_status,
                created_at: isi.created_at,
            });
            total++;
        }
        page++;
    }
    return { total, total_seen: totalSeen, total_pages: totalPages };
}

async function jobPeers() {
    const list = await torii.getPeers();
    if (!Array.isArray(list)) return { upserts: 0, deactivated: 0 };
    let upserts = 0;
    for (const m of list) {
        // multiaddr format: ea01<pubkey-hex>@<ip>:<port>
        const at = m.indexOf('@');
        let publicKey = null, ip = null, port = null;
        if (at !== -1) {
            publicKey = m.slice(0, at);
            const tail = m.slice(at + 1);
            const colon = tail.lastIndexOf(':');
            if (colon !== -1) {
                ip = tail.slice(0, colon);
                port = parseInt(tail.slice(colon + 1), 10) || null;
            } else {
                ip = tail;
            }
        }
        await db.upsertPeer({ multiaddr: m, public_key: publicKey, ip_address: ip, port });
        upserts++;
    }
    // Mark anyone that vanished from the current Torii response as inactive,
    // so the UI doesn't keep showing disconnected peers as "Active". Truth
    // = current /peers response, not history.
    const deactivated = await db.deactivateStalePeers(list);
    return { upserts, deactivated };
}

async function jobPrometheus() {
    const text = await torii.getPrometheus();
    const samples = prom.parse(text);
    const stamp = new Date();
    const inserted = await db.insertMetricsSamples(samples, stamp);
    return { inserted, total_parsed: samples.length };
}

async function jobMetricsCleanup() {
    const deleted = await db.pruneMetricsSnapshots(cfg.METRICS_RETENTION_DAYS);
    return { deleted };
}

// ------------------------------------------------------------
// Boot
// ------------------------------------------------------------

async function main() {
    log('info', 'starting Minamoto indexer');
    log('info', 'torii base:', cfg.TORII_BASE);
    log('info', 'pg target:', `${process.env.PG_HOST || 'localhost'}:${process.env.PG_PORT || 23798}`);

    log('info', 'applying mn.* schema (idempotent)...');
    await db.applySchema();
    const ping = await db.ping();
    log('info', 'pg ping:', ping);

    // Schedule all jobs
    scheduleJob('network_state', cfg.POLL_NETWORK_STATE_MS, jobNetworkState);
    scheduleJob('blocks',         cfg.POLL_BLOCKS_MS,         jobBlocks);
    scheduleJob('transactions',   cfg.POLL_TX_MS,             jobTransactions);
    scheduleJob('domains',        cfg.POLL_DOMAINS_MS,        jobDomains);
    scheduleJob('accounts',       cfg.POLL_ACCOUNTS_MS,       jobAccounts);
    scheduleJob('assets',         cfg.POLL_ASSETS_MS,         jobAssets);
    scheduleJob('asset_definitions', cfg.POLL_ASSETS_MS,      jobAssetDefinitions);
    scheduleJob('peers',          cfg.POLL_PEERS_MS,          jobPeers);
    scheduleJob('instructions',   cfg.POLL_TX_MS,             jobInstructions);
    scheduleJob('claims_enrich',     cfg.POLL_TX_MS * 2,      jobClaimsEnrich);
    scheduleJob('claims_v2_resolve', cfg.POLL_TX_MS * 2,      jobClaimsResolveV2);
    scheduleJob('prometheus',     cfg.POLL_PROMETHEUS_MS,     jobPrometheus);
    scheduleJob('metrics_cleanup', cfg.METRICS_CLEANUP_PERIOD_MS, jobMetricsCleanup);

    log('info', 'all jobs scheduled');

    // One-shot historical backfill: blocks first (so the FK is satisfied),
    // then transactions. Doesn't reschedule. Errors are logged but don't
    // crash the indexer — the regular periodic jobs continue.
    setTimeout(async () => {
        try {
            const r1 = await jobBlocksBackfill();
            log('info', 'blocks_backfill ok', r1);
            await db.recordIndexerRun('blocks_backfill', 'ok', r1, null);
        } catch (e) {
            log('err', 'blocks_backfill FAILED:', e.message);
            try { await db.recordIndexerRun('blocks_backfill', 'error', {}, e.message); } catch (_) {}
        }
        try {
            const r2 = await jobTransactionsBackfill();
            log('info', 'transactions_backfill ok', r2);
            await db.recordIndexerRun('transactions_backfill', 'ok', r2, null);
        } catch (e) {
            log('err', 'transactions_backfill FAILED:', e.message);
            try { await db.recordIndexerRun('transactions_backfill', 'error', {}, e.message); } catch (_) {}
        }
        try {
            const r3 = await jobInstructionsBackfill();
            log('info', 'instructions_backfill ok', r3);
            await db.recordIndexerRun('instructions_backfill', 'ok', r3, null);
        } catch (e) {
            log('err', 'instructions_backfill FAILED:', e.message);
            try { await db.recordIndexerRun('instructions_backfill', 'error', {}, e.message); } catch (_) {}
        }
    }, 3000);
}

function gracefulShutdown(sig) {
    log('info', `${sig} received, stopping new jobs...`);
    _shuttingDown = true;
    setTimeout(() => process.exit(0), 5000);
}
process.on('SIGTERM', () => gracefulShutdown('SIGTERM'));
process.on('SIGINT',  () => gracefulShutdown('SIGINT'));

main().catch(err => {
    log('err', 'fatal during boot:', err.message, err.stack);
    process.exit(1);
});
