'use strict';
// ============================================================
// preimage_indexer.js — Dedicated indexer for the `preimage` pallet.
// Maintains an SQLite DB of all Preimage.Noted / Requested / Cleared
// / Unnoted events. Two modes run in parallel:
//   1) live — subscribes to new heads and ingests events as they land
//   2) backfill — scans older blocks downward until TARGET is reached
// Queries against the resulting DB are instant (indexed by hash).
// ============================================================
const { ApiPromise, WsProvider } = require('@polkadot/api');
const { options } = require('@sora-substrate/api');
const Database = require('better-sqlite3');
const path = require('path');

const WS = process.env.WS || 'wss://mof2.sora.org';
const DB_PATH = process.env.PREIMAGE_DB || path.join(__dirname, 'preimage_index.db');
const BACKFILL_BATCH = parseInt(process.env.BACKFILL_BATCH || '200');
const BACKFILL_CONCURRENCY = parseInt(process.env.BACKFILL_CONCURRENCY || '15');
// Default backfill: last ~60 days (for SORA, ~6s blocks → 864000 blocks)
const DEFAULT_BACKFILL_SPAN = parseInt(process.env.BACKFILL_SPAN || '864000');

const db = new Database(DB_PATH);
db.pragma('journal_mode = WAL');
db.pragma('synchronous = NORMAL');
db.exec(`
CREATE TABLE IF NOT EXISTS preimage_events (
    block_height  INTEGER NOT NULL,
    event_index   INTEGER NOT NULL,
    timestamp     INTEGER,
    section       TEXT NOT NULL,
    method        TEXT NOT NULL,
    hash          TEXT NOT NULL,
    data          TEXT,
    reason        TEXT,
    reason_detail TEXT,
    PRIMARY KEY (block_height, event_index)
);
CREATE INDEX IF NOT EXISTS idx_pe_hash ON preimage_events(hash);
CREATE INDEX IF NOT EXISTS idx_pe_block ON preimage_events(block_height);
CREATE INDEX IF NOT EXISTS idx_pe_ts ON preimage_events(timestamp);

CREATE TABLE IF NOT EXISTS indexer_state (
    key   TEXT PRIMARY KEY,
    value TEXT
);
`);
try { db.exec('ALTER TABLE preimage_events ADD COLUMN reason TEXT'); } catch {}
try { db.exec('ALTER TABLE preimage_events ADD COLUMN reason_detail TEXT'); } catch {}

const insertEvent = db.prepare(
    `INSERT OR IGNORE INTO preimage_events
     (block_height, event_index, timestamp, section, method, hash, data, reason, reason_detail)
     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`
);

function inferClearedReason(records, exts) {
    const matches = (s, m) => records.some(r => r.event.section === s && r.event.method === m);
    if (matches('system', 'CodeUpdated')) return { reason: 'runtime_upgrade', detail: 'Scheduler ran system.setCode in this block' };
    if (matches('scheduler', 'Dispatched') || matches('scheduler', 'Called')) return { reason: 'scheduler_dispatched', detail: 'Consumed by scheduler running a scheduled call' };
    if (exts) {
        for (const ext of exts) {
            try {
                const m = String(ext.method.method).toLowerCase();
                if (ext.method.section === 'preimage' && m.includes('unnotepreimage')) {
                    return { reason: 'unnote_manual', detail: 'Depositor called preimage.unnotePreimage to reclaim the deposit' };
                }
            } catch {}
        }
    }
    return { reason: null, detail: null };
}
const getState = db.prepare(`SELECT value FROM indexer_state WHERE key = ?`);
const setState = db.prepare(`INSERT OR REPLACE INTO indexer_state(key, value) VALUES (?, ?)`);

function log(...args) { console.log(`[${new Date().toISOString()}]`, ...args); }

async function processBlock(api, blockNumber) {
    const h = await api.rpc.chain.getBlockHash(blockNumber);
    const events = await api.query.system.events.at(h);
    const preimageEvents = [];
    let hasClearedOrUnnoted = false;
    let idx = 0;
    for (const r of events) {
        const ev = r.event;
        if (ev.section === 'preimage') {
            const hash = (ev.data[0] && ev.data[0].toHex) ? ev.data[0].toHex() : String(ev.data[0]);
            const method = String(ev.method);
            if (method === 'Cleared' || method === 'Unnoted') hasClearedOrUnnoted = true;
            preimageEvents.push({
                event_index: idx,
                method,
                hash: hash.toLowerCase(),
                data: JSON.stringify(ev.data.toJSON())
            });
        }
        idx++;
    }
    if (preimageEvents.length === 0) return 0;
    let ts = null;
    try {
        const tsRaw = await api.query.timestamp.now.at(h);
        ts = typeof tsRaw.toNumber === 'function' ? tsRaw.toNumber() : Number(tsRaw.toString());
    } catch {}
    let reasonInfo = { reason: null, detail: null };
    if (hasClearedOrUnnoted) {
        try {
            const signedBlock = await api.rpc.chain.getBlock(h);
            reasonInfo = inferClearedReason(events, signedBlock.block.extrinsics);
        } catch {}
    }
    const tx = db.transaction((evs) => {
        for (const e of evs) {
            const isCleared = e.method === 'Cleared' || e.method === 'Unnoted';
            insertEvent.run(
                blockNumber, e.event_index, ts, 'preimage', e.method, e.hash, e.data,
                isCleared ? reasonInfo.reason : null,
                isCleared ? reasonInfo.detail : null
            );
        }
    });
    tx(preimageEvents);
    return preimageEvents.length;
}

async function main() {
    log(`Connecting to ${WS}...`);
    const api = await ApiPromise.create(options({ provider: new WsProvider(WS, 2500) }));
    await api.isReady;
    log('API ready');

    // Catch-up phase: fill the gap between last_live_block and current tip.
    // Without this, any blocks that elapsed while the indexer was down
    // (RPC timeout, restart, WS drop) are silently lost — backfill only
    // goes downward from tip, never upward to fill recent gaps.
    try {
        const tipNow = (await api.rpc.chain.getHeader()).number.toNumber();
        const lastLive = Number(getState.get('live_last_block')?.value || '0');
        if (lastLive > 0 && tipNow > lastLive) {
            const gap = tipNow - lastLive;
            log(`Catch-up: from ${lastLive + 1} to ${tipNow} (${gap} blocks)`);
            const CATCHUP_CONCURRENCY = parseInt(process.env.CATCHUP_CONCURRENCY || '8');
            const blocks = [];
            for (let n = lastLive + 1; n <= tipNow; n++) blocks.push(n);
            let i = 0, processed = 0, errors = 0;
            async function catchupWorker() {
                while (true) {
                    const myIdx = i++;
                    if (myIdx >= blocks.length) return;
                    const n = blocks[myIdx];
                    try {
                        const c = await processBlock(api, n);
                        // Don't update live_last_block per-block here — the live
                        // subscription may race ahead while we backfill the gap.
                        // We set live_last_block once at end of catch-up.
                        processed++;
                        if (c > 0) log(`catchup block ${n}: ${c} preimage event(s)`);
                        if (processed % 1000 === 0) log(`Catch-up progress: ${processed}/${blocks.length}`);
                    } catch (e) {
                        errors++;
                        log(`catchup block ${n} error: ${e.message}`);
                    }
                }
            }
            const workers = [];
            for (let w = 0; w < CATCHUP_CONCURRENCY; w++) workers.push(catchupWorker());
            await Promise.all(workers);
            // Advance live_last_block, but never regress (live subscription may
            // have moved ahead during catch-up).
            const cur = Number(getState.get('live_last_block')?.value || '0');
            const newVal = Math.max(cur, tipNow);
            setState.run('live_last_block', String(newVal));
            log(`Catch-up complete: processed=${processed} errors=${errors}, live_last_block=${newVal}`);
        } else {
            log(`Catch-up skipped: lastLive=${lastLive} tipNow=${tipNow}`);
        }
    } catch (e) {
        log(`Catch-up phase failed: ${e.message} — continuing with live subscription`);
    }

    // Live subscription
    api.rpc.chain.subscribeNewHeads(async (header) => {
        const n = header.number.toNumber();
        try {
            const count = await processBlock(api, n);
            setState.run('live_last_block', String(n));
            if (count > 0) log(`live block ${n}: ${count} preimage event(s)`);
        } catch (e) {
            log(`live block ${n} error: ${e.message}`);
        }
    });
    log('Live subscription active');

    // Backfill loop
    const tip = (await api.rpc.chain.getHeader()).number.toNumber();
    const resumeCursor = Number(getState.get('backfill_cursor')?.value || '0');
    const target = Math.max(0, tip - DEFAULT_BACKFILL_SPAN);
    let cursor = resumeCursor > target ? resumeCursor : tip;
    log(`Backfill starting: cursor=${cursor} target=${target} (${cursor - target} blocks remaining)`);

    while (cursor > target) {
        const batchTo = cursor;
        const batchFrom = Math.max(target, cursor - BACKFILL_BATCH + 1);
        const blocks = [];
        for (let n = batchTo; n >= batchFrom; n--) blocks.push(n);

        let idx = 0;
        async function worker() {
            while (true) {
                const myIdx = idx++;
                if (myIdx >= blocks.length) return;
                try { await processBlock(api, blocks[myIdx]); }
                catch (e) { /* skip — block is retried next run via cursor */ }
            }
        }
        const workers = [];
        for (let w = 0; w < BACKFILL_CONCURRENCY; w++) workers.push(worker());
        await Promise.all(workers);

        cursor = batchFrom - 1;
        setState.run('backfill_cursor', String(cursor));
        if ((batchTo - batchFrom + 1) >= BACKFILL_BATCH) {
            log(`Backfill cursor=${cursor}, ${Math.max(0, cursor - target)} blocks remaining`);
        }
    }
    log(`Backfill complete at block ${target}`);
    setState.run('backfill_complete_at', String(Date.now()));
}

main().catch(e => { console.error('FATAL:', e); process.exit(1); });
