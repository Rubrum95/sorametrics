// events_backfiller.js - Gap-filler for extrinsic events_json
// Fills existing extrinsics that have events_json = NULL
// One-shot script: auto-exits when done. Run with: pm2 start events_backfiller.js --name sorametrics-events-backfill --no-autorestart

const { ApiPromise, WsProvider } = require('@polkadot/api');
const { options } = require('@sora-substrate/api');
const fs = require('fs');
const path = require('path');
const Database = require('better-sqlite3');

const { WS_ENDPOINT_BACKFILL } = require('./config');
const WS_ENDPOINT = WS_ENDPOINT_BACKFILL;

// Database paths
const HISTORY_DB_PATH = path.join(__dirname, 'database.db');
const MAIN_DB_PATH = path.join(__dirname, 'database_30d.db');

// Config
const BLOCKS_PER_BATCH = 200;
const DELAY_BETWEEN_BATCHES_MS = 300;
const LOG_INTERVAL = 100;

// Stats
let stats = { blocksProcessed: 0, extrinsicsUpdated: 0, errors: 0, startTime: Date.now() };

// Serialize extrinsic events to condensed JSON for storage
function serializeEvents(extrinsicEvents, maxSize = 8192) {
    try {
        const events = [];
        for (const record of extrinsicEvents) {
            try {
                const { event } = record;
                if (event.section === 'system' && (event.method === 'ExtrinsicSuccess' || event.method === 'ExtrinsicFailed')) continue;
                events.push({
                    s: event.section,
                    m: event.method,
                    d: event.data ? event.data.toHuman() : null
                });
            } catch (e) { /* skip malformed event */ }
        }
        if (events.length === 0) return '[]';
        const json = JSON.stringify(events);
        if (json.length <= maxSize) return json;
        const slim = events.map(e => ({ s: e.s, m: e.m }));
        const slimJson = JSON.stringify(slim);
        return slimJson.length <= maxSize ? slimJson : null;
    } catch (e) {
        return null;
    }
}

function openDB(dbPath) {
    if (!fs.existsSync(dbPath)) return null;
    const db = new Database(dbPath);
    db.pragma('journal_mode = WAL');
    db.pragma('synchronous = NORMAL');
    db.pragma('busy_timeout = 5000');
    db.pragma('cache_size = -16000'); // 16MB — lighter than main processes
    // Ensure events_json column exists
    try { db.exec(`ALTER TABLE extrinsics ADD COLUMN events_json TEXT DEFAULT NULL`); } catch (e) { /* already exists */ }
    return db;
}

function logProgress() {
    const elapsed = ((Date.now() - stats.startTime) / 1000).toFixed(0);
    const rate = stats.blocksProcessed > 0 ? (stats.blocksProcessed / (elapsed || 1)).toFixed(1) : '0';
    console.log(`[events-backfiller] ${stats.blocksProcessed} blocks | ${stats.extrinsicsUpdated} extrinsics updated | ${stats.errors} errors | ${rate} blocks/s | ${elapsed}s elapsed`);
}

async function run() {
    console.log('[events-backfiller] Starting gap-filler for extrinsic events...');

    // Open databases
    const histDb = openDB(HISTORY_DB_PATH);
    const mainDb = openDB(MAIN_DB_PATH);

    if (!histDb && !mainDb) {
        console.error('[events-backfiller] No databases found. Exiting.');
        process.exit(1);
    }

    const databases = [];
    if (histDb) databases.push({ name: 'history (database.db)', db: histDb });
    if (mainDb) databases.push({ name: 'main (database_30d.db)', db: mainDb });

    console.log(`[events-backfiller] Opened ${databases.length} database(s): ${databases.map(d => d.name).join(', ')}`);

    // Prepare statements for each DB
    for (const entry of databases) {
        entry.getBlocks = entry.db.prepare(
            `SELECT DISTINCT block FROM extrinsics WHERE events_json IS NULL ORDER BY block DESC LIMIT ?`
        );
        entry.getExtrinsicsByBlock = entry.db.prepare(
            `SELECT extrinsic_index FROM extrinsics WHERE block = ? AND events_json IS NULL`
        );
        entry.updateEvents = entry.db.prepare(
            `UPDATE extrinsics SET events_json = ? WHERE block = ? AND extrinsic_index = ?`
        );
    }

    // Count total gap
    let totalGap = 0;
    for (const entry of databases) {
        const count = entry.db.prepare(`SELECT COUNT(*) as c FROM extrinsics WHERE events_json IS NULL`).get().c;
        console.log(`[events-backfiller] ${entry.name}: ${count.toLocaleString()} extrinsics missing events_json`);
        totalGap += count;
    }

    if (totalGap === 0) {
        console.log('[events-backfiller] No gap to fill. All extrinsics have events_json. Exiting.');
        for (const entry of databases) entry.db.close();
        process.exit(0);
    }

    // Connect to blockchain
    console.log(`[events-backfiller] Connecting to ${WS_ENDPOINT}...`);
    const provider = new WsProvider(WS_ENDPOINT);
    const api = await ApiPromise.create(options({ provider }));
    console.log(`[events-backfiller] Connected. Chain: ${(await api.rpc.system.chain()).toString()}`);

    // Main loop
    let hasWork = true;
    while (hasWork) {
        hasWork = false;

        for (const entry of databases) {
            const blocks = entry.getBlocks.all(BLOCKS_PER_BATCH).map(r => r.block);
            if (blocks.length === 0) continue;
            hasWork = true;

            for (const blockNum of blocks) {
                try {
                    // Get extrinsic indices that need events for this block
                    const indices = entry.getExtrinsicsByBlock.all(blockNum).map(r => r.extrinsic_index);
                    if (indices.length === 0) continue;

                    // Fetch block and events from chain
                    const blockHash = await api.rpc.chain.getBlockHash(blockNum);
                    const [signedBlock, allEvents] = await Promise.all([
                        api.rpc.chain.getBlock(blockHash),
                        api.query.system.events.at(blockHash)
                    ]);

                    // Update each extrinsic in a transaction
                    const updateBatch = entry.db.transaction((items) => {
                        for (const { idx, eventsJson } of items) {
                            entry.updateEvents.run(eventsJson, blockNum, idx);
                        }
                    });

                    const updates = [];
                    for (const idx of indices) {
                        const extrinsicEvents = allEvents.filter(({ phase }) =>
                            phase.isApplyExtrinsic && phase.asApplyExtrinsic.toNumber() === idx
                        );
                        const eventsJson = serializeEvents(extrinsicEvents);
                        // Even if null (too large), store '[]' to mark as processed
                        updates.push({ idx, eventsJson: eventsJson || '[]' });
                        stats.extrinsicsUpdated++;
                    }

                    updateBatch(updates);
                    stats.blocksProcessed++;

                    if (stats.blocksProcessed % LOG_INTERVAL === 0) {
                        logProgress();
                    }
                } catch (e) {
                    stats.errors++;
                    // Mark extrinsics in this block as processed with empty events to avoid retrying forever
                    try {
                        const indices = entry.getExtrinsicsByBlock.all(blockNum).map(r => r.extrinsic_index);
                        const markBatch = entry.db.transaction((idxs) => {
                            for (const idx of idxs) {
                                entry.updateEvents.run('[]', blockNum, idx);
                            }
                        });
                        markBatch(indices);
                    } catch (e2) { /* ignore */ }

                    if (stats.errors % 50 === 0) {
                        console.error(`[events-backfiller] Error at block ${blockNum} (${entry.name}): ${e.message}`);
                    }
                }
            }

            // Small delay between batches to avoid RPC pressure
            await new Promise(r => setTimeout(r, DELAY_BETWEEN_BATCHES_MS));
        }
    }

    // Done
    logProgress();
    console.log('[events-backfiller] Gap filling complete. Shutting down...');

    for (const entry of databases) entry.db.close();
    await api.disconnect();
    process.exit(0);
}

// Handle graceful shutdown
process.on('SIGINT', () => {
    console.log('\n[events-backfiller] Interrupted. Progress so far:');
    logProgress();
    process.exit(0);
});

process.on('SIGTERM', () => {
    console.log('\n[events-backfiller] Terminated. Progress so far:');
    logProgress();
    process.exit(0);
});

run().catch(err => {
    console.error('[events-backfiller] Fatal error:', err);
    process.exit(1);
});
