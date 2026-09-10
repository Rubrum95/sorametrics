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

// Cursor pages (100 rows each) a backfill walks at most per feed.
const BACKFILL_MAX_PAGES = parseInt(process.env.MINAMOTO_BACKFILL_MAX_PAGES, 10) || 400;

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
    // /status is fetched every cycle: it is what decides the API generation.
    const gen = await torii.apiGeneration({ refresh: true });
    if (gen.kind === 'cursor') {
        const s = await torii.getStatus();
        const counts = await db.getIndexedCounts();
        await db.upsertNetworkState({
            peers: s.peers | 0,
            domains: counts.domains,
            accounts: counts.accounts,
            assets: counts.assets,
            transactions_accepted: Number(s.txs_approved) || 0,
            transactions_rejected: Number(s.txs_rejected) || 0,
            block_height: Number(s.blocks) || 0,
            finalized_block: Number(s.blocks) || 0,
            avg_commit_time_ms: Number(s.commit_time_ms) || 0,
            avg_block_time_ms: counts.avg_block_ms || 0,
            last_block_at: counts.last_block_at || null,
            iroha_version: gen.version,
        });
        return { block: s.blocks, peers: s.peers, generation: gen.kind, git_commit_sha: gen.git_commit_sha };
    }
    const m = await torii.getExplorerMetrics();
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
        iroha_version: gen.version,
    });
    return { block: m.block, peers: m.peers, generation: gen.kind };
}

// Walks a cursor-paginated feed to the end (or MAX_PAGES), calling
// `onItems` per page. Returns the pages walked.
async function walkCursor(fetchPage, onItems, maxPages) {
    let cursor = null, pages = 0;
    for (;;) {
        const r = await fetchPage(cursor);
        await onItems(r.items || [], r.pagination || {});
        pages++;
        cursor = r.pagination ? r.pagination.next_cursor : null;
        if (!r.pagination || !r.pagination.has_more || !cursor || pages >= maxPages) break;
    }
    return pages;
}

async function jobBlocks() {
    const gen = await torii.apiGeneration();
    let items;
    if (gen.kind === 'cursor') {
        items = (await torii.getExplorerBlocksCursor(null, 25)).items || [];
    } else {
        // Iroha rc2 Torii caps reliable pages at 7 (cursor store drops continuations).
        items = (await torii.getExplorerBlocks(1, 7)).items || [];
    }
    // Reset detection: a height already indexed with another hash means
    // the chain restarted from genesis (Iroha has no reorgs).
    let reset = false;
    for (const b of items) {
        const stored = await db.getBlockHashHex(b.height);
        if (stored && stored !== String(b.hash).toLowerCase()) { reset = true; break; }
    }
    if (reset) {
        log('err', 'chain reset detected (height re-served with another hash): truncating mn chain tables');
        await db.truncateChainTables();
    }
    let upserts = 0;
    for (const b of items) {
        await db.upsertBlock(b);
        upserts++;
    }
    if (reset) {
        await runBackfills();
    }
    return { upserts, reset, generation: gen.kind };
}

// One-shot historical backfill. Idempotent: re-running just hits ON CONFLICT.
async function jobBlocksBackfill() {
    const gen = await torii.apiGeneration();
    if (gen.kind === 'cursor') {
        let total = 0, snapshot = null;
        const pages = await walkCursor(
            (c) => torii.getExplorerBlocksCursor(c, 100),
            async (items, pg) => {
                if (snapshot == null && pg.snapshot_height != null) snapshot = pg.snapshot_height;
                for (const b of items) { await db.upsertBlock(b); total++; }
            },
            BACKFILL_MAX_PAGES,
        );
        return { total, pages, snapshot_height: snapshot };
    }
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

async function upsertTxItems(items) {
    let ok = 0, skipped = 0;
    for (const tx of items) {
        if (tx.block == null) { skipped++; continue; }
        try { await db.upsertTransaction(tx); ok++; }
        catch (e) {
            // FK violation: parent block not yet indexed. Skip and let the
            // blocks job / backfill catch us up.
            if (e.code === '23503') { skipped++; continue; }
            throw e;
        }
    }
    return { ok, skipped };
}

async function jobTransactionsBackfill() {
    const gen = await torii.apiGeneration();
    if (gen.kind === 'cursor') {
        let ok = 0, skipped = 0;
        const pages = await walkCursor(
            (c) => torii.getExplorerTransactionsCursor(c, 100),
            async (items) => { const r = await upsertTxItems(items); ok += r.ok; skipped += r.skipped; },
            BACKFILL_MAX_PAGES,
        );
        return { upserts: ok, skipped, pages };
    }
    const PER_PAGE = 50;
    const MAX_PAGES = 400; // 20k txs ceiling
    let page = 1, ok = 0, skipped = 0, totalSeen = null, totalPages = 1;
    while (page <= totalPages && page <= MAX_PAGES) {
        const r = await torii.getExplorerTransactions(page, PER_PAGE);
        if (r && r.pagination) {
            totalSeen = r.pagination.total_items;
            totalPages = r.pagination.total_pages;
        }
        const u = await upsertTxItems(r.items || []);
        ok += u.ok; skipped += u.skipped;
        page++;
    }
    return { upserts: ok, skipped, total_seen: totalSeen };
}

async function jobTransactions() {
    const gen = await torii.apiGeneration();
    const r = gen.kind === 'cursor'
        ? await torii.getExplorerTransactionsCursor(null, 25)
        : await torii.getExplorerTransactions(1, 7);
    const u = await upsertTxItems(r.items || []);
    return { upserts: u.ok, skipped: u.skipped, total_seen: r.pagination ? (r.pagination.total_items != null ? r.pagination.total_items : r.pagination.snapshot_height) : null };
}

async function jobDomains() {
    const gen = await torii.apiGeneration();
    let upserts = 0;
    const onItems = async (items) => {
        for (const d of items) {
            // Domain owner must exist as account (accounts have FK target via metadata).
            await db.upsertAccount({ id: d.owned_by });
            await db.upsertDomain(d);
            upserts++;
        }
    };
    if (gen.kind === 'cursor') await walkCursor((c) => torii.getExplorerDomainsCursor(c, 100), onItems, BACKFILL_MAX_PAGES);
    else await onItems((await torii.getExplorerDomains(1, 100)).items || []);
    return { upserts };
}

async function jobAccounts() {
    const gen = await torii.apiGeneration();
    let upserts = 0;
    const onItems = async (items) => {
        for (const a of items) {
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
    };
    if (gen.kind === 'cursor') await walkCursor((c) => torii.getExplorerAccountsCursor(c, 100), onItems, BACKFILL_MAX_PAGES);
    else await onItems((await torii.getExplorerAccounts(1, 100)).items || []);
    return { upserts };
}

async function jobAssets() {
    const gen = await torii.apiGeneration();
    let upserts = 0;
    const onItems = async (items) => {
        for (const a of items) {
            await db.upsertAsset({
                definition_id: a.definition_id,
                account_id: a.account_id,
                // Quantity may arrive as a decimal string or a JSON number.
                value: typeof a.value === 'number' ? String(a.value) : a.value,
            });
            upserts++;
        }
    };
    if (gen.kind === 'cursor') await walkCursor((c) => torii.getExplorerAssetsCursor(c, 100), onItems, BACKFILL_MAX_PAGES);
    else await onItems((await torii.getExplorerAssets(1, 100)).items || []);
    return { upserts };
}

async function jobAssetDefinitions() {
    const gen = await torii.apiGeneration();
    let upserts = 0, total = null;
    const onItems = async (items) => {
        for (const d of items) {
            // Owner must exist as account (asset_definitions doesn't FK to
            // accounts, but a stub keeps joins clean).
            await db.upsertAccount({ id: d.owned_by });
            await db.upsertAssetDefinition(d);
            upserts++;
        }
    };
    if (gen.kind === 'cursor') {
        let offset = 0;
        for (;;) {
            const r = await torii.getAssetDefinitionsPage(100, offset);
            if (total == null && r.total != null) total = r.total;
            await onItems(r.items || []);
            offset += (r.items || []).length;
            if (!r.has_more || !(r.items || []).length) break;
        }
    } else {
        const r = await torii.getAssetDefinitions();
        total = r.total;
        await onItems(r.items || []);
    }
    return { upserts, total };
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

// The structured payload lives in `box.json.payload` (Torii's
// ExplorerInstructionDto; `#[norito(rename = "box")]`). Older builds
// exposed a bare Norito base64 `payload` string, which db.js coerces to
// `{ encoded }`.
function instructionPayload(isi) {
    const box = isi.box || isi['r#box'] || (isi.r && isi.r['#box']);
    if (box && box.json && box.json.payload != null) return box.json.payload;
    return isi.payload != null ? isi.payload : {};
}

async function upsertIsiItems(items) {
    let n = 0;
    for (const isi of items) {
        await db.upsertInstruction({
            transaction_hash: isi.transaction_hash,
            instruction_index: isi.index,
            block: isi.block,
            authority: isi.authority,
            kind: isi.kind,
            payload: instructionPayload(isi),
            transaction_status: isi.transaction_status,
            created_at: isi.created_at,
        });
        n++;
    }
    return n;
}

async function jobInstructions() {
    const gen = await torii.apiGeneration();
    const r = gen.kind === 'cursor'
        ? await torii.getExplorerInstructionsCursor(null, 25)
        : await torii.getExplorerInstructions(1, 7);
    const upserts = await upsertIsiItems(r.items || []);
    return { upserts, total_seen: r.pagination ? (r.pagination.total_items != null ? r.pagination.total_items : r.pagination.snapshot_height) : null };
}

async function jobInstructionsBackfill() {
    const gen = await torii.apiGeneration();
    if (gen.kind === 'cursor') {
        let total = 0;
        const pages = await walkCursor(
            (c) => torii.getExplorerInstructionsCursor(c, 100),
            async (items) => { total += await upsertIsiItems(items); },
            BACKFILL_MAX_PAGES,
        );
        return { total, pages };
    }
    const PER_PAGE = 50;
    const MAX_PAGES = 200; // 10k ceiling, generous for early Minamoto
    let page = 1, total = 0, totalSeen = null, totalPages = 1;
    while (page <= totalPages && page <= MAX_PAGES) {
        const r = await torii.getExplorerInstructions(page, PER_PAGE);
        if (r && r.pagination) {
            totalSeen = r.pagination.total_items;
            totalPages = r.pagination.total_pages;
        }
        total += await upsertIsiItems(r.items || []);
        page++;
    }
    return { total, total_seen: totalSeen, total_pages: totalPages };
}

async function jobPeers() {
    // Iroha rc2 dropped /peers; connected peers now come from the per-source
    // telemetry as bare public keys (no ip:port). Collect the distinct set.
    const sources = await torii.getPeersInfo();
    if (!Array.isArray(sources)) return { upserts: 0, deactivated: 0 };
    const pubkeys = new Set();
    for (const s of sources) {
        for (const pk of (s.connected_peers || [])) pubkeys.add(pk);
    }
    const list = [...pubkeys];
    let upserts = 0;
    for (const pk of list) {
        await db.upsertPeer({ multiaddr: pk, public_key: pk, ip_address: null, port: null });
        upserts++;
    }
    // Mark anyone that vanished from the current response as inactive, so the
    // UI doesn't keep showing disconnected peers as "Active". Truth = now.
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
    // then transactions, then instructions. Doesn't reschedule. Errors are
    // logged but don't crash the indexer — the regular periodic jobs continue.
    setTimeout(runBackfills, 3000);
}

async function runBackfills() {
    for (const [name, fn] of [
        ['blocks_backfill', jobBlocksBackfill],
        ['transactions_backfill', jobTransactionsBackfill],
        ['instructions_backfill', jobInstructionsBackfill],
    ]) {
        try {
            const r = await fn();
            log('info', `${name} ok`, r);
            await db.recordIndexerRun(name, 'ok', r, null);
        } catch (e) {
            log('err', `${name} FAILED:`, e.message);
            try { await db.recordIndexerRun(name, 'error', {}, e.message); } catch (_) {}
        }
    }
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
