// pg_to_sqlite.js - Export sora-subsquid PostgreSQL data to SoraMetrics SQLite
// Reads from subsquid's history_element + asset_snapshot tables,
// transforms into the exact schema expected by db_better.js,
// and writes to database.db (history) + database_30d.db (main/recent)
//
// Fixes applied:
// - Cursor-based pagination (no OFFSET, avoids O(n²))
// - Native XOR transfers (balances.Transfer) included
// - Dedup via DELETE + re-insert for crash recovery
// - networkFee format auto-detection (raw planck vs formatted)
// - Supply snapshots incremental filtering
// - Order book event type mapping complete
// - Explicit date formatting (no locale dependency)

const { Pool } = require('pg');
const Database = require('better-sqlite3');
const BigNumber = require('bignumber.js');
const fs = require('fs');
const path = require('path');

// ============================================================
// CONFIGURATION
// ============================================================

const PG_CONFIG = {
    host: process.env.PG_HOST || 'localhost',
    port: parseInt(process.env.PG_PORT) || 23798,
    database: process.env.PG_DB || 'squid',
    user: process.env.PG_USER || 'postgres',
    password: process.env.PG_PASS || 'squid',
    max: 3,
};

const APP_DIR = path.join(__dirname, '..');
const HISTORY_DB_PATH = path.join(APP_DIR, 'database.db');
const MAIN_DB_PATH = path.join(APP_DIR, 'database_30d.db');
const STATE_FILE = path.join(__dirname, 'export_state.json');
const WHITELIST_URL = 'https://raw.githubusercontent.com/sora-xor/polkaswap-token-whitelist-config/master/whitelist.json';

const BLOCK_RANGE = 100000; // process blocks in ranges (cursor-based, no OFFSET)
const THIRTY_DAYS_MS = 30 * 24 * 60 * 60 * 1000;
const IS_FULL = process.argv.includes('--full');

// Known asset IDs
const XOR_ID = '0x0200000000000000000000000000000000000000000000000000000000000000';
const DAI_ID = '0x0200060000000000000000000000000000000000000000000000000000000000';

// Supply tracking tokens
const SUPPLY_TOKENS = ['XOR', 'VAL', 'PSWAP', 'TBCD', 'KUSD'];

// networkFee format: auto-detected on first run
let networkFeeDivisor = null; // null = auto-detect, BigNumber(1) or BigNumber(1e18)

// ============================================================
// CACHES
// ============================================================

const assetCache = new Map();  // assetId -> {symbol, decimals, logo}
const priceCache = new Map();  // `${assetId}_${hourBucket}` -> priceUSD

// ============================================================
// STATE MANAGEMENT
// ============================================================

function loadState() {
    try {
        if (fs.existsSync(STATE_FILE)) return JSON.parse(fs.readFileSync(STATE_FILE, 'utf8'));
    } catch (e) { }
    return { lastExportedBlock: 0, lastExportedTimestamp: 0, lastRun: null, stats: {} };
}

function saveState(state) {
    state.lastRun = new Date().toISOString();
    fs.writeFileSync(STATE_FILE, JSON.stringify(state, null, 2));
}

// ============================================================
// ASSET METADATA
// ============================================================

async function loadAssetMetadata() {
    try {
        const res = await fetch(WHITELIST_URL);
        const data = await res.json();
        for (const item of data) {
            assetCache.set(item.address, {
                symbol: item.symbol,
                decimals: item.decimals || 18,
                logo: item.icon || '',
            });
        }
        console.log(`  Loaded ${assetCache.size} assets from whitelist`);
    } catch (e) {
        console.error('  Failed to load whitelist, using minimal defaults');
        assetCache.set(XOR_ID, { symbol: 'XOR', decimals: 18, logo: '' });
        assetCache.set(DAI_ID, { symbol: 'DAI', decimals: 18, logo: '' });
    }
}

function getAsset(assetId) {
    if (!assetId) return null;
    return assetCache.get(assetId) || null;
}

// ============================================================
// PRICE RESOLUTION
// ============================================================

async function loadPriceSnapshots(pgPool, fromTimestamp) {
    console.log('  Loading price snapshots from asset_snapshot...');

    // Only load prices for assets we know about (limits memory)
    const knownAssetIds = [...assetCache.keys()];
    if (knownAssetIds.length === 0) {
        console.log('  No known assets, skipping price loading');
        return;
    }

    // Load in batches of assets to manage memory
    for (let i = 0; i < knownAssetIds.length; i += 50) {
        const assetBatch = knownAssetIds.slice(i, i + 50);
        const placeholders = assetBatch.map((_, idx) => `$${idx + 2}`).join(',');

        const result = await pgPool.query(`
            SELECT asset_id, timestamp, price_usd
            FROM asset_snapshot
            WHERE type = 'HOUR' AND timestamp >= $1
              AND asset_id IN (${placeholders})
            ORDER BY timestamp ASC
        `, [fromTimestamp, ...assetBatch]);

        for (const row of result.rows) {
            try {
                const priceObj = typeof row.price_usd === 'string'
                    ? JSON.parse(row.price_usd) : row.price_usd;
                const closePrice = parseFloat(priceObj?.close || priceObj?.open || '0');
                if (closePrice > 0) {
                    const hourBucket = Math.floor(row.timestamp / 3600) * 3600;
                    priceCache.set(`${row.asset_id}_${hourBucket}`, closePrice);
                }
            } catch (e) { /* skip malformed price */ }
        }
    }
    console.log(`  Loaded ${priceCache.size} price snapshots`);
}

function getPriceAtTimestamp(assetId, timestampSeconds) {
    if (!assetId) return 0;
    if (assetId === DAI_ID) return 1;

    const hourBucket = Math.floor(timestampSeconds / 3600) * 3600;
    return priceCache.get(`${assetId}_${hourBucket}`)
        || priceCache.get(`${assetId}_${hourBucket - 3600}`)
        || priceCache.get(`${assetId}_${hourBucket + 3600}`)
        || 0;
}

// ============================================================
// NETWORK FEE FORMAT DETECTION
// ============================================================

async function detectNetworkFeeFormat(pgPool) {
    // sora-subsquid stores networkFee as the raw BigInt from XorFee.FeeWithdrawn
    // which is in planck (needs /1e18). But verify by sampling.
    const sample = await pgPool.query(`
        SELECT network_fee FROM history_element
        WHERE network_fee IS NOT NULL AND network_fee != '0'
        LIMIT 10
    `);

    if (sample.rows.length === 0) {
        networkFeeDivisor = new BigNumber('1e18');
        return;
    }

    // If values are large (> 1e12), they're raw planck → divide by 1e18
    // If values are small (< 1e6), they're already formatted → divide by 1
    const sampleValues = sample.rows.map(r => new BigNumber(r.network_fee));
    const avgValue = sampleValues.reduce((a, b) => a.plus(b), new BigNumber(0)).div(sampleValues.length);

    if (avgValue.gt('1e12')) {
        networkFeeDivisor = new BigNumber('1e18');
        console.log('  networkFee format: raw planck (dividing by 1e18)');
    } else {
        networkFeeDivisor = new BigNumber('1');
        console.log('  networkFee format: already formatted (no division needed)');
    }
}

// ============================================================
// SQLITE SETUP
// ============================================================

function initSqliteDb(dbPath) {
    const db = new Database(dbPath);
    db.pragma('journal_mode = WAL');
    db.pragma('synchronous = NORMAL');
    db.pragma('cache_size = -32000');
    db.pragma('temp_store = MEMORY');

    db.exec(`CREATE TABLE IF NOT EXISTS swaps (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        timestamp INTEGER, formatted_time TEXT, block INTEGER, wallet TEXT,
        in_symbol TEXT, in_amount TEXT, in_logo TEXT, in_usd REAL,
        out_symbol TEXT, out_amount TEXT, out_logo TEXT, out_usd REAL,
        hash TEXT, extrinsic_id TEXT
    )`);

    db.exec(`CREATE TABLE IF NOT EXISTS transfers (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        timestamp INTEGER, formatted_time TEXT, block INTEGER,
        from_addr TEXT, to_addr TEXT, amount TEXT, symbol TEXT, logo TEXT,
        usd_value REAL, asset_id TEXT, hash TEXT, extrinsic_id TEXT
    )`);

    db.exec(`CREATE TABLE IF NOT EXISTS bridges (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        timestamp INTEGER, block INTEGER, network TEXT, direction TEXT,
        sender TEXT, recipient TEXT, asset_id TEXT, symbol TEXT, logo TEXT,
        amount TEXT, usd_value REAL, hash TEXT, extrinsic_id TEXT
    )`);

    db.exec(`CREATE TABLE IF NOT EXISTS fees (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        timestamp INTEGER, block INTEGER, type TEXT,
        amount REAL, usd_value REAL, denom_factor TEXT DEFAULT '1'
    )`);

    db.exec(`CREATE TABLE IF NOT EXISTS liquidity_events (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        timestamp INTEGER, block INTEGER, wallet TEXT,
        pool_base TEXT, pool_target TEXT,
        base_amount TEXT, target_amount TEXT, usd_value REAL,
        type TEXT, hash TEXT, extrinsic_id TEXT
    )`);

    db.exec(`CREATE TABLE IF NOT EXISTS order_book_events (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        timestamp INTEGER, formatted_time TEXT, block INTEGER,
        event_type TEXT, wallet TEXT, order_id TEXT,
        base_asset TEXT, quote_asset TEXT, side TEXT,
        price TEXT, amount TEXT, usd_value REAL,
        hash TEXT, extrinsic_id TEXT
    )`);

    db.exec(`CREATE TABLE IF NOT EXISTS extrinsics (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        timestamp INTEGER, formatted_time TEXT, block INTEGER,
        extrinsic_index INTEGER, hash TEXT,
        section TEXT, method TEXT, signer TEXT,
        success INTEGER, args_json TEXT,
        error_msg TEXT DEFAULT '', events_json TEXT DEFAULT NULL
    )`);

    db.exec(`CREATE TABLE IF NOT EXISTS supply_snapshots (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        timestamp INTEGER, symbol TEXT, asset_id TEXT, total_supply REAL
    )`);

    return db;
}

function createIndices(db) {
    db.exec(`
        CREATE INDEX IF NOT EXISTS idx_swaps_timestamp ON swaps(timestamp);
        CREATE INDEX IF NOT EXISTS idx_swaps_block ON swaps(block);
        CREATE INDEX IF NOT EXISTS idx_swaps_in_symbol ON swaps(in_symbol);
        CREATE INDEX IF NOT EXISTS idx_swaps_out_symbol ON swaps(out_symbol);
        CREATE INDEX IF NOT EXISTS idx_swaps_wallet ON swaps(wallet);
        CREATE INDEX IF NOT EXISTS idx_transfers_timestamp ON transfers(timestamp);
        CREATE INDEX IF NOT EXISTS idx_transfers_from ON transfers(from_addr);
        CREATE INDEX IF NOT EXISTS idx_transfers_to ON transfers(to_addr);
        CREATE INDEX IF NOT EXISTS idx_transfers_block ON transfers(block);
        CREATE INDEX IF NOT EXISTS idx_bridges_timestamp ON bridges(timestamp);
        CREATE INDEX IF NOT EXISTS idx_bridges_sender ON bridges(sender);
        CREATE INDEX IF NOT EXISTS idx_bridges_block ON bridges(block);
        CREATE INDEX IF NOT EXISTS idx_fees_timestamp ON fees(timestamp);
        CREATE INDEX IF NOT EXISTS idx_liquidity_timestamp ON liquidity_events(timestamp);
        CREATE INDEX IF NOT EXISTS idx_liquidity_wallet ON liquidity_events(wallet);
        CREATE INDEX IF NOT EXISTS idx_liquidity_block ON liquidity_events(block);
        CREATE INDEX IF NOT EXISTS idx_extrinsics_timestamp ON extrinsics(timestamp);
        CREATE INDEX IF NOT EXISTS idx_extrinsics_block ON extrinsics(block);
        CREATE INDEX IF NOT EXISTS idx_extrinsics_section ON extrinsics(section);
        CREATE INDEX IF NOT EXISTS idx_extrinsics_signer ON extrinsics(signer);
        CREATE INDEX IF NOT EXISTS idx_extrinsics_section_timestamp ON extrinsics(section, timestamp);
        CREATE INDEX IF NOT EXISTS idx_orderbook_timestamp ON order_book_events(timestamp);
        CREATE INDEX IF NOT EXISTS idx_orderbook_wallet ON order_book_events(wallet);
        CREATE INDEX IF NOT EXISTS idx_orderbook_event_type ON order_book_events(event_type);
        CREATE INDEX IF NOT EXISTS idx_orderbook_block ON order_book_events(block);
        CREATE INDEX IF NOT EXISTS idx_orderbook_timestamp_type ON order_book_events(timestamp, event_type);
        CREATE INDEX IF NOT EXISTS idx_supply_symbol_timestamp ON supply_snapshots(symbol, timestamp);
    `);
}

// ============================================================
// HELPER FUNCTIONS
// ============================================================

function tsToMs(timestampSeconds) {
    return timestampSeconds * 1000;
}

// FIX Bug 3: Explicit date formatting (no locale dependency)
function formatTime(timestampMs) {
    const d = new Date(timestampMs);
    const dd = String(d.getDate()).padStart(2, '0');
    const mm = String(d.getMonth() + 1).padStart(2, '0');
    const yyyy = d.getFullYear();
    const hh = String(d.getHours()).padStart(2, '0');
    const min = String(d.getMinutes()).padStart(2, '0');
    const ss = String(d.getSeconds()).padStart(2, '0');
    return `${dd}/${mm}/${yyyy} ${hh}:${min}:${ss}`;
}

function formatAmount(amountStr) {
    const num = parseFloat(amountStr);
    return isNaN(num) ? '0.0000' : num.toFixed(4);
}

// FIX Bug 4: Clean fee classification with camelCase module names
function classifyFeeType(module) {
    if (!module) return 'Other';
    const m = module.toLowerCase();
    if (m === 'liquidityproxy') return 'Swap';
    if (m === 'ethbridge' || m === 'bridgeproxy') return 'Bridge';
    if (m === 'assets' || m === 'balances' || m === 'tokens') return 'Transfer';
    if (m === 'poolxyk') return 'Swap';
    if (m === 'orderbook') return 'Swap';
    return 'Other';
}

function parseJson(val) {
    if (!val) return null;
    if (typeof val === 'object') return val;
    try { return JSON.parse(val); } catch (e) { return null; }
}

// FIX Bug 13: Complete order book event type mapping
function mapOrderBookEventType(method) {
    if (!method) return 'other';
    const m = method.toLowerCase();
    if (m.includes('limitorderplaced') || m === 'placelimitorder') return 'placed';
    if (m.includes('marketorderexecuted')) return 'market';
    if (m.includes('limitorderexecuted') || m.includes('executed')) return 'executed';
    if (m.includes('limitorderfilled') || m.includes('filled')) return 'filled';
    if (m.includes('limitordercanceled') || m.includes('canceled') || m === 'cancellimitorder') return 'canceled';
    return 'other';
}

// FIX Bug 11: Clean existing data for incremental re-insert (crash recovery)
function cleanBlockRange(db, table, fromBlock) {
    db.prepare(`DELETE FROM ${table} WHERE block > ?`).run(fromBlock);
}

// ============================================================
// CURSOR-BASED PAGINATION HELPER
// FIX Bug 21: No more OFFSET, use block ranges instead
// ============================================================

async function queryByBlockRange(pgPool, sql, fromBlock, maxBlock, extraParams = []) {
    const allRows = [];
    for (let rangeStart = fromBlock + 1; rangeStart <= maxBlock; rangeStart += BLOCK_RANGE) {
        const rangeEnd = Math.min(rangeStart + BLOCK_RANGE - 1, maxBlock);
        const result = await pgPool.query(sql, [rangeStart, rangeEnd, ...extraParams]);
        if (result.rows.length > 0) {
            allRows.push(...result.rows);
        }
    }
    return allRows;
}

// Streaming version for large tables - processes in chunks
async function* streamByBlockRange(pgPool, sql, fromBlock, maxBlock, extraParams = []) {
    for (let rangeStart = fromBlock + 1; rangeStart <= maxBlock; rangeStart += BLOCK_RANGE) {
        const rangeEnd = Math.min(rangeStart + BLOCK_RANGE - 1, maxBlock);
        const result = await pgPool.query(sql, [rangeStart, rangeEnd, ...extraParams]);
        if (result.rows.length > 0) {
            yield { rows: result.rows, rangeStart, rangeEnd };
        }
    }
}

// ============================================================
// EXPORT FUNCTIONS
// ============================================================

async function exportSwaps(pgPool, histDb, mainDb, fromBlock, maxBlock) {
    console.log('\n📊 Exporting swaps...');

    // FIX Bug 11: Clean partial data from previous failed run
    if (!IS_FULL) {
        cleanBlockRange(histDb, 'swaps', fromBlock);
        cleanBlockRange(mainDb, 'swaps', fromBlock);
    }

    const insertHist = histDb.prepare(`INSERT INTO swaps
        (timestamp, formatted_time, block, wallet, in_symbol, in_amount, in_logo, in_usd,
         out_symbol, out_amount, out_logo, out_usd, hash, extrinsic_id)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`);
    const insertMain = mainDb.prepare(`INSERT INTO swaps
        (timestamp, formatted_time, block, wallet, in_symbol, in_amount, in_logo, in_usd,
         out_symbol, out_amount, out_logo, out_usd, hash, extrinsic_id)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`);

    const thirtyDaysAgo = Math.floor((Date.now() - THIRTY_DAYS_MS) / 1000);
    let total = 0;

    const sql = `
        SELECT id, block_height, address, timestamp, data, execution
        FROM history_element
        WHERE type = 'CALL'
          AND module = 'liquidityProxy'
          AND method IN ('swap', 'swapTransfer', 'swapTransferBatch')
          AND block_height >= $1 AND block_height <= $2
        ORDER BY block_height ASC
    `;

    for await (const chunk of streamByBlockRange(pgPool, sql, fromBlock, maxBlock)) {
        const histBatch = [];
        const mainBatch = [];

        for (const row of chunk.rows) {
            const exec = parseJson(row.execution);
            if (!exec || !exec.success) continue;

            const data = parseJson(row.data);
            if (!data || !data.baseAssetId || !data.targetAssetId) continue;

            const inAsset = getAsset(data.baseAssetId);
            const outAsset = getAsset(data.targetAssetId);
            if (!inAsset || !outAsset) continue;

            const inPrice = getPriceAtTimestamp(data.baseAssetId, row.timestamp);
            const outPrice = getPriceAtTimestamp(data.targetAssetId, row.timestamp);

            const inAmountNum = parseFloat(data.baseAssetAmount || '0');
            const outAmountNum = parseFloat(data.targetAssetAmount || '0');

            const timestampMs = tsToMs(row.timestamp);
            // FIX Bug 7/8: Use extrinsic hash as both hash and extrinsic_id
            const hash = row.id; // For CALLs, subsquid id = extrinsic hash
            const record = [
                timestampMs, formatTime(timestampMs), row.block_height, row.address,
                inAsset.symbol, formatAmount(data.baseAssetAmount), inAsset.logo,
                inAmountNum * inPrice,
                outAsset.symbol, formatAmount(data.targetAssetAmount), outAsset.logo,
                outAmountNum * outPrice,
                hash, `${row.block_height}-${hash.substring(0, 8)}`
            ];

            histBatch.push(record);
            if (row.timestamp >= thirtyDaysAgo) mainBatch.push(record);
        }

        if (histBatch.length > 0) {
            histDb.transaction(() => { for (const r of histBatch) insertHist.run(...r); })();
        }
        if (mainBatch.length > 0) {
            mainDb.transaction(() => { for (const r of mainBatch) insertMain.run(...r); })();
        }

        total += histBatch.length;
        console.log(`  Swaps: ${total.toLocaleString()} (blocks ${chunk.rangeStart.toLocaleString()}-${chunk.rangeEnd.toLocaleString()})`);
    }

    console.log(`  ✅ Swaps total: ${total.toLocaleString()}`);
    return total;
}

// FIX Bug 1: Export BOTH assets.transfer CALLs AND balances/tokens Transfer EVENTs
async function exportTransfers(pgPool, histDb, mainDb, fromBlock, maxBlock) {
    console.log('\n📊 Exporting transfers...');

    if (!IS_FULL) {
        cleanBlockRange(histDb, 'transfers', fromBlock);
        cleanBlockRange(mainDb, 'transfers', fromBlock);
    }

    const insertHist = histDb.prepare(`INSERT INTO transfers
        (timestamp, formatted_time, block, from_addr, to_addr, amount, symbol, logo,
         usd_value, asset_id, hash, extrinsic_id)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`);
    const insertMain = mainDb.prepare(`INSERT INTO transfers
        (timestamp, formatted_time, block, from_addr, to_addr, amount, symbol, logo,
         usd_value, asset_id, hash, extrinsic_id)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`);

    const thirtyDaysAgo = Math.floor((Date.now() - THIRTY_DAYS_MS) / 1000);
    let total = 0;

    // --- Part 1: assets.transfer CALL type (token transfers) ---
    const sqlCalls = `
        SELECT id, block_height, address, timestamp, data, execution
        FROM history_element
        WHERE type = 'CALL'
          AND module = 'assets'
          AND method = 'transfer'
          AND block_height >= $1 AND block_height <= $2
        ORDER BY block_height ASC
    `;

    for await (const chunk of streamByBlockRange(pgPool, sqlCalls, fromBlock, maxBlock)) {
        const histBatch = [];
        const mainBatch = [];

        for (const row of chunk.rows) {
            const exec = parseJson(row.execution);
            if (!exec || !exec.success) continue;

            const data = parseJson(row.data);
            if (!data) continue;

            const assetId = data.assetId || XOR_ID;
            const asset = getAsset(assetId);
            if (!asset) continue;

            const from = data.from || row.address || '';
            const to = data.to || '';
            const amountStr = data.amount || '0';
            const amountNum = parseFloat(amountStr);

            if (from.startsWith('cnTQ') || to.startsWith('cnTQ')) continue;

            const price = getPriceAtTimestamp(assetId, row.timestamp);
            const timestampMs = tsToMs(row.timestamp);
            const hash = row.id;
            const record = [
                timestampMs, formatTime(timestampMs), row.block_height,
                from, to, formatAmount(amountStr), asset.symbol, asset.logo,
                amountNum * price, assetId, hash, `${row.block_height}-${hash.substring(0, 8)}`
            ];

            histBatch.push(record);
            if (row.timestamp >= thirtyDaysAgo) mainBatch.push(record);
        }

        if (histBatch.length > 0) {
            histDb.transaction(() => { for (const r of histBatch) insertHist.run(...r); })();
        }
        if (mainBatch.length > 0) {
            mainDb.transaction(() => { for (const r of mainBatch) insertMain.run(...r); })();
        }
        total += histBatch.length;
    }
    console.log(`  Transfers from calls: ${total.toLocaleString()}`);

    // --- Part 2: balances.Transfer EVENT type (native XOR transfers) ---
    // FIX Bug 1: These were completely missing before
    const sqlXorEvents = `
        SELECT id, block_height, address, timestamp, data, data_from, data_to
        FROM history_element
        WHERE type = 'EVENT'
          AND module = 'balances'
          AND method = 'Transfer'
          AND block_height >= $1 AND block_height <= $2
        ORDER BY block_height ASC
    `;

    let xorTotal = 0;
    for await (const chunk of streamByBlockRange(pgPool, sqlXorEvents, fromBlock, maxBlock)) {
        const histBatch = [];
        const mainBatch = [];

        for (const row of chunk.rows) {
            const data = parseJson(row.data);
            const from = data?.from || row.data_from || row.address || '';
            const to = data?.to || row.data_to || '';
            const amountStr = data?.amount || '0';
            const amountNum = parseFloat(amountStr);

            if (from.startsWith('cnTQ') || to.startsWith('cnTQ')) continue;
            if (amountNum <= 0) continue;

            const price = getPriceAtTimestamp(XOR_ID, row.timestamp);
            const timestampMs = tsToMs(row.timestamp);
            // FIX Bug 8: EVENT ids are "blockHeight-eventIndex", use as-is
            const hash = row.id;
            const record = [
                timestampMs, formatTime(timestampMs), row.block_height,
                from, to, formatAmount(amountStr), 'XOR', getAsset(XOR_ID)?.logo || '',
                amountNum * price, XOR_ID, hash, `${row.block_height}-evt`
            ];

            histBatch.push(record);
            if (row.timestamp >= thirtyDaysAgo) mainBatch.push(record);
        }

        if (histBatch.length > 0) {
            histDb.transaction(() => { for (const r of histBatch) insertHist.run(...r); })();
        }
        if (mainBatch.length > 0) {
            mainDb.transaction(() => { for (const r of mainBatch) insertMain.run(...r); })();
        }
        xorTotal += histBatch.length;
    }
    total += xorTotal;
    console.log(`  XOR transfers from events: ${xorTotal.toLocaleString()}`);

    console.log(`  ✅ Transfers total: ${total.toLocaleString()}`);
    return total;
}

async function exportBridges(pgPool, histDb, mainDb, fromBlock, maxBlock) {
    console.log('\n📊 Exporting bridges...');

    if (!IS_FULL) {
        cleanBlockRange(histDb, 'bridges', fromBlock);
        cleanBlockRange(mainDb, 'bridges', fromBlock);
    }

    const insertHist = histDb.prepare(`INSERT INTO bridges
        (timestamp, block, network, direction, sender, recipient,
         asset_id, symbol, logo, amount, usd_value, hash, extrinsic_id)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`);
    const insertMain = mainDb.prepare(`INSERT INTO bridges
        (timestamp, block, network, direction, sender, recipient,
         asset_id, symbol, logo, amount, usd_value, hash, extrinsic_id)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`);

    const thirtyDaysAgo = Math.floor((Date.now() - THIRTY_DAYS_MS) / 1000);
    let total = 0;

    // --- Outgoing bridges (CALLs) ---
    const sqlOut = `
        SELECT id, block_height, address, timestamp, data, execution
        FROM history_element
        WHERE type = 'CALL'
          AND module = 'ethBridge'
          AND method = 'transferToSidechain'
          AND block_height >= $1 AND block_height <= $2
        ORDER BY block_height ASC
    `;

    for await (const chunk of streamByBlockRange(pgPool, sqlOut, fromBlock, maxBlock)) {
        const histBatch = [];
        const mainBatch = [];

        for (const row of chunk.rows) {
            const exec = parseJson(row.execution);
            if (!exec || !exec.success) continue;

            const data = parseJson(row.data);
            if (!data) continue;

            const assetId = data.assetId || '';
            const asset = getAsset(assetId);
            const amountStr = data.amount || '0';
            const amountNum = parseFloat(amountStr);
            const price = getPriceAtTimestamp(assetId, row.timestamp);

            const timestampMs = tsToMs(row.timestamp);
            const hash = row.id;
            const record = [
                timestampMs, row.block_height, 'Ethereum', 'Outgoing',
                row.address || '', data.to || data.sidechainAddress || '',
                assetId, asset?.symbol || 'UNK', asset?.logo || '',
                formatAmount(amountStr), amountNum * price,
                hash, `${row.block_height}-${hash.substring(0, 8)}`
            ];

            histBatch.push(record);
            if (row.timestamp >= thirtyDaysAgo) mainBatch.push(record);
        }

        if (histBatch.length > 0) {
            histDb.transaction(() => { for (const r of histBatch) insertHist.run(...r); })();
        }
        if (mainBatch.length > 0) {
            mainDb.transaction(() => { for (const r of mainBatch) insertMain.run(...r); })();
        }
        total += histBatch.length;
    }

    // --- Incoming bridges (EVENTs) ---
    const sqlIn = `
        SELECT id, block_height, address, timestamp, data
        FROM history_element
        WHERE type = 'EVENT'
          AND module = 'ethBridge'
          AND method = 'IncomingRequestFinalized'
          AND block_height >= $1 AND block_height <= $2
        ORDER BY block_height ASC
    `;

    for await (const chunk of streamByBlockRange(pgPool, sqlIn, fromBlock, maxBlock)) {
        const histBatch = [];
        const mainBatch = [];

        for (const row of chunk.rows) {
            const data = parseJson(row.data);
            const assetId = data?.assetId || '';
            const asset = getAsset(assetId);
            const amountStr = data?.amount || '0';
            const amountNum = parseFloat(amountStr);
            const price = getPriceAtTimestamp(assetId, row.timestamp);

            const timestampMs = tsToMs(row.timestamp);
            const record = [
                timestampMs, row.block_height, 'Ethereum', 'Incoming',
                data?.from || 'Ethereum', data?.to || row.address || '',
                assetId, asset?.symbol || 'UNK', asset?.logo || '',
                formatAmount(amountStr), amountNum * price,
                row.id, 'ETH'
            ];

            histBatch.push(record);
            if (row.timestamp >= thirtyDaysAgo) mainBatch.push(record);
        }

        if (histBatch.length > 0) {
            histDb.transaction(() => { for (const r of histBatch) insertHist.run(...r); })();
        }
        if (mainBatch.length > 0) {
            mainDb.transaction(() => { for (const r of mainBatch) insertMain.run(...r); })();
        }
        total += histBatch.length;
    }

    console.log(`  ✅ Bridges total: ${total.toLocaleString()}`);
    return total;
}

// FIX Bug 6: Auto-detect networkFee format
async function exportFees(pgPool, histDb, mainDb, fromBlock, maxBlock) {
    console.log('\n📊 Exporting fees...');

    if (!IS_FULL) {
        cleanBlockRange(histDb, 'fees', fromBlock);
        cleanBlockRange(mainDb, 'fees', fromBlock);
    }

    const insertHist = histDb.prepare(`INSERT INTO fees
        (timestamp, block, type, amount, usd_value, denom_factor)
        VALUES (?, ?, ?, ?, ?, ?)`);
    const insertMain = mainDb.prepare(`INSERT INTO fees
        (timestamp, block, type, amount, usd_value, denom_factor)
        VALUES (?, ?, ?, ?, ?, ?)`);

    const thirtyDaysAgo = Math.floor((Date.now() - THIRTY_DAYS_MS) / 1000);
    let total = 0;

    const sql = `
        SELECT block_height, timestamp, module, method, network_fee
        FROM history_element
        WHERE type = 'CALL'
          AND network_fee IS NOT NULL
          AND network_fee != '0'
          AND block_height >= $1 AND block_height <= $2
        ORDER BY block_height ASC
    `;

    for await (const chunk of streamByBlockRange(pgPool, sql, fromBlock, maxBlock)) {
        const histBatch = [];
        const mainBatch = [];

        for (const row of chunk.rows) {
            const feeRaw = row.network_fee;
            if (!feeRaw || feeRaw === '0') continue;

            const feeXor = new BigNumber(feeRaw).div(networkFeeDivisor);
            if (feeXor.isZero() || feeXor.isNaN()) continue;

            const xorPrice = getPriceAtTimestamp(XOR_ID, row.timestamp);
            const feeUsd = feeXor.times(xorPrice).toNumber();
            const feeType = classifyFeeType(row.module);

            const timestampMs = tsToMs(row.timestamp);
            // FIX Bug 5: denom_factor '1' is acceptable since subsquid doesn't track it
            // The denomination was a one-time event; fees after denomination are already correct
            const record = [
                timestampMs, row.block_height, feeType,
                feeXor.toNumber(), feeUsd, '1'
            ];

            histBatch.push(record);
            if (row.timestamp >= thirtyDaysAgo) mainBatch.push(record);
        }

        if (histBatch.length > 0) {
            histDb.transaction(() => { for (const r of histBatch) insertHist.run(...r); })();
        }
        if (mainBatch.length > 0) {
            mainDb.transaction(() => { for (const r of mainBatch) insertMain.run(...r); })();
        }

        total += histBatch.length;
        if (chunk.rangeEnd % (BLOCK_RANGE * 5) === 0) {
            console.log(`  Fees: ${total.toLocaleString()} (block ${chunk.rangeEnd.toLocaleString()})`);
        }
    }

    console.log(`  ✅ Fees total: ${total.toLocaleString()}`);
    return total;
}

async function exportLiquidity(pgPool, histDb, mainDb, fromBlock, maxBlock) {
    console.log('\n📊 Exporting liquidity events...');

    if (!IS_FULL) {
        cleanBlockRange(histDb, 'liquidity_events', fromBlock);
        cleanBlockRange(mainDb, 'liquidity_events', fromBlock);
    }

    const insertHist = histDb.prepare(`INSERT INTO liquidity_events
        (timestamp, block, wallet, pool_base, pool_target,
         base_amount, target_amount, usd_value, type, hash, extrinsic_id)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`);
    const insertMain = mainDb.prepare(`INSERT INTO liquidity_events
        (timestamp, block, wallet, pool_base, pool_target,
         base_amount, target_amount, usd_value, type, hash, extrinsic_id)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`);

    const thirtyDaysAgo = Math.floor((Date.now() - THIRTY_DAYS_MS) / 1000);
    let total = 0;

    const sql = `
        SELECT id, block_height, address, timestamp, data, method, execution
        FROM history_element
        WHERE type = 'CALL'
          AND module = 'poolXyk'
          AND method IN ('depositLiquidity', 'withdrawLiquidity')
          AND block_height >= $1 AND block_height <= $2
        ORDER BY block_height ASC
    `;

    for await (const chunk of streamByBlockRange(pgPool, sql, fromBlock, maxBlock)) {
        const histBatch = [];
        const mainBatch = [];

        for (const row of chunk.rows) {
            const exec = parseJson(row.execution);
            if (!exec || !exec.success) continue;

            const data = parseJson(row.data);
            if (!data) continue;

            const baseAssetId = data.baseAssetId || data.inputAssetA || '';
            const targetAssetId = data.targetAssetId || data.inputAssetB || '';
            const baseAsset = getAsset(baseAssetId);
            const targetAsset = getAsset(targetAssetId);

            const baseAmountStr = data.baseAssetAmount || data.inputADesired || '0';
            const targetAmountStr = data.targetAssetAmount || data.inputBDesired || '0';
            const baseAmountNum = parseFloat(baseAmountStr);
            const targetAmountNum = parseFloat(targetAmountStr);

            const basePrice = getPriceAtTimestamp(baseAssetId, row.timestamp);
            const targetPrice = getPriceAtTimestamp(targetAssetId, row.timestamp);
            const usdValue = (baseAmountNum * basePrice) + (targetAmountNum * targetPrice);

            // FIX Bug 2: Use 'deposit'/'withdraw' consistently (matches db_better.js queries)
            const type = row.method === 'depositLiquidity' ? 'deposit' : 'withdraw';

            const timestampMs = tsToMs(row.timestamp);
            const hash = row.id;
            const record = [
                timestampMs, row.block_height, row.address,
                baseAsset?.symbol || baseAssetId.slice(0, 10),
                targetAsset?.symbol || targetAssetId.slice(0, 10),
                formatAmount(baseAmountStr), formatAmount(targetAmountStr),
                usdValue, type, hash, `${row.block_height}-${hash.substring(0, 8)}`
            ];

            histBatch.push(record);
            if (row.timestamp >= thirtyDaysAgo) mainBatch.push(record);
        }

        if (histBatch.length > 0) {
            histDb.transaction(() => { for (const r of histBatch) insertHist.run(...r); })();
        }
        if (mainBatch.length > 0) {
            mainDb.transaction(() => { for (const r of mainBatch) insertMain.run(...r); })();
        }
        total += histBatch.length;
    }

    console.log(`  ✅ Liquidity events total: ${total.toLocaleString()}`);
    return total;
}

// FIX Bugs 13, 14: Complete order book event handling with asset pair resolution
async function exportOrderBook(pgPool, histDb, mainDb, fromBlock, maxBlock) {
    console.log('\n📊 Exporting order book events...');

    if (!IS_FULL) {
        cleanBlockRange(histDb, 'order_book_events', fromBlock);
        cleanBlockRange(mainDb, 'order_book_events', fromBlock);
    }

    const insertHist = histDb.prepare(`INSERT INTO order_book_events
        (timestamp, formatted_time, block, event_type, wallet, order_id,
         base_asset, quote_asset, side, price, amount, usd_value, hash, extrinsic_id)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`);
    const insertMain = mainDb.prepare(`INSERT INTO order_book_events
        (timestamp, formatted_time, block, event_type, wallet, order_id,
         base_asset, quote_asset, side, price, amount, usd_value, hash, extrinsic_id)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`);

    const thirtyDaysAgo = Math.floor((Date.now() - THIRTY_DAYS_MS) / 1000);
    let total = 0;

    // --- Order book CALLs ---
    const sqlCalls = `
        SELECT id, block_height, address, timestamp, data, method, execution
        FROM history_element
        WHERE type = 'CALL'
          AND module = 'orderBook'
          AND block_height >= $1 AND block_height <= $2
        ORDER BY block_height ASC
    `;

    for await (const chunk of streamByBlockRange(pgPool, sqlCalls, fromBlock, maxBlock)) {
        const histBatch = [];
        const mainBatch = [];

        for (const row of chunk.rows) {
            const exec = parseJson(row.execution);
            if (!exec || !exec.success) continue;

            const data = parseJson(row.data);
            if (!data) continue;

            const eventType = mapOrderBookEventType(row.method);

            // FIX Bug 14: Parse asset pair from data
            const baseAssetId = data.baseAssetId || data.orderBookId?.base || '';
            const quoteAssetId = data.quoteAssetId || data.orderBookId?.quote || '';
            const baseAsset = getAsset(baseAssetId);
            const quoteAsset = getAsset(quoteAssetId);

            const side = data.side === 'Buy' ? 'buy' : (data.side === 'Sell' ? 'sell' : '');
            const price = data.price || '';
            const amount = data.amount || '';
            const orderId = data.orderId?.toString() || '';

            const timestampMs = tsToMs(row.timestamp);
            const hash = row.id;
            const record = [
                timestampMs, formatTime(timestampMs), row.block_height,
                eventType, row.address, orderId,
                baseAsset?.symbol || '', quoteAsset?.symbol || '',
                side, price, amount, 0,
                hash, `${row.block_height}-${hash.substring(0, 8)}`
            ];

            histBatch.push(record);
            if (row.timestamp >= thirtyDaysAgo) mainBatch.push(record);
        }

        if (histBatch.length > 0) {
            histDb.transaction(() => { for (const r of histBatch) insertHist.run(...r); })();
        }
        if (mainBatch.length > 0) {
            mainDb.transaction(() => { for (const r of mainBatch) insertMain.run(...r); })();
        }
        total += histBatch.length;
    }

    // --- Order book EVENTs (fills, executions) ---
    const sqlEvents = `
        SELECT id, block_height, address, timestamp, data, method
        FROM history_element
        WHERE type = 'EVENT'
          AND module = 'orderBook'
          AND block_height >= $1 AND block_height <= $2
        ORDER BY block_height ASC
    `;

    for await (const chunk of streamByBlockRange(pgPool, sqlEvents, fromBlock, maxBlock)) {
        const histBatch = [];
        const mainBatch = [];

        for (const row of chunk.rows) {
            const data = parseJson(row.data);
            const eventType = mapOrderBookEventType(row.method);

            // FIX Bug 14: Extract asset pair from event data
            const obId = data?.orderBookId || {};
            const baseAsset = getAsset(obId.base || '');
            const quoteAsset = getAsset(obId.quote || '');

            const timestampMs = tsToMs(row.timestamp);
            const record = [
                timestampMs, formatTime(timestampMs), row.block_height,
                eventType, data?.owner || row.address || '', data?.orderId?.toString() || '',
                baseAsset?.symbol || '', quoteAsset?.symbol || '',
                data?.side === 'Buy' ? 'buy' : (data?.side === 'Sell' ? 'sell' : ''),
                data?.price || '', data?.amount || '', 0,
                row.id, `${row.block_height}-evt`
            ];

            histBatch.push(record);
            if (row.timestamp >= thirtyDaysAgo) mainBatch.push(record);
        }

        if (histBatch.length > 0) {
            histDb.transaction(() => { for (const r of histBatch) insertHist.run(...r); })();
        }
        if (mainBatch.length > 0) {
            mainDb.transaction(() => { for (const r of mainBatch) insertMain.run(...r); })();
        }
        total += histBatch.length;
    }

    console.log(`  ✅ Order book events total: ${total.toLocaleString()}`);
    return total;
}

async function exportExtrinsics(pgPool, histDb, mainDb, fromBlock, maxBlock) {
    console.log('\n📊 Exporting extrinsics...');

    if (!IS_FULL) {
        cleanBlockRange(histDb, 'extrinsics', fromBlock);
        cleanBlockRange(mainDb, 'extrinsics', fromBlock);
    }

    const insertHist = histDb.prepare(`INSERT INTO extrinsics
        (timestamp, formatted_time, block, extrinsic_index, hash,
         section, method, signer, success, args_json, error_msg, events_json)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`);
    const insertMain = mainDb.prepare(`INSERT INTO extrinsics
        (timestamp, formatted_time, block, extrinsic_index, hash,
         section, method, signer, success, args_json, error_msg, events_json)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`);

    const thirtyDaysAgo = Math.floor((Date.now() - THIRTY_DAYS_MS) / 1000);
    let total = 0;

    // FIX Bug 16: Use ROW_NUMBER in PG to assign extrinsic indices per block
    const sql = `
        SELECT id, block_height, address, timestamp, module, method,
               data, execution, network_fee,
               ROW_NUMBER() OVER (PARTITION BY block_height ORDER BY id) - 1 AS ex_index
        FROM history_element
        WHERE type = 'CALL'
          AND block_height >= $1 AND block_height <= $2
        ORDER BY block_height ASC, id ASC
    `;

    for await (const chunk of streamByBlockRange(pgPool, sql, fromBlock, maxBlock)) {
        const histBatch = [];
        const mainBatch = [];

        for (const row of chunk.rows) {
            const exec = parseJson(row.execution);
            const success = exec?.success ? 1 : 0;

            let errorMsg = '';
            if (exec?.error) {
                if (exec.error.moduleErrorIndex !== undefined) {
                    errorMsg = `Module error: index=${exec.error.moduleErrorIndex}, id=${exec.error.moduleErrorId || ''}`;
                } else if (exec.error.nonModuleErrorMessage) {
                    errorMsg = exec.error.nonModuleErrorMessage;
                }
            }

            const data = parseJson(row.data);
            let argsJson = '{}';
            try {
                const str = JSON.stringify(data || {});
                argsJson = str.length > 2048 ? str.substring(0, 2048) + '...' : str;
            } catch (e) { }

            const timestampMs = tsToMs(row.timestamp);
            const record = [
                timestampMs, formatTime(timestampMs), row.block_height,
                row.ex_index || 0, row.id,
                row.module || '', row.method || '',
                row.address || 'System', success, argsJson, errorMsg, null
            ];

            histBatch.push(record);
            if (row.timestamp >= thirtyDaysAgo) mainBatch.push(record);
        }

        if (histBatch.length > 0) {
            histDb.transaction(() => { for (const r of histBatch) insertHist.run(...r); })();
        }
        if (mainBatch.length > 0) {
            mainDb.transaction(() => { for (const r of mainBatch) insertMain.run(...r); })();
        }

        total += histBatch.length;
        if (chunk.rangeEnd % (BLOCK_RANGE * 5) === 0) {
            console.log(`  Extrinsics: ${total.toLocaleString()} (block ${chunk.rangeEnd.toLocaleString()})`);
        }
    }

    console.log(`  ✅ Extrinsics total: ${total.toLocaleString()}`);
    return total;
}

// FIX Bug 23: Add timestamp filter for incremental runs
async function exportSupplySnapshots(pgPool, histDb, mainDb, fromTimestamp) {
    console.log('\n📊 Exporting supply snapshots...');

    if (!IS_FULL) {
        // For incremental: delete snapshots newer than last export timestamp
        histDb.prepare(`DELETE FROM supply_snapshots WHERE timestamp > ?`).run(fromTimestamp);
        mainDb.prepare(`DELETE FROM supply_snapshots WHERE timestamp > ?`).run(fromTimestamp);
    }

    const insertHist = histDb.prepare(`INSERT INTO supply_snapshots
        (timestamp, symbol, asset_id, total_supply) VALUES (?, ?, ?, ?)`);
    const insertMain = mainDb.prepare(`INSERT INTO supply_snapshots
        (timestamp, symbol, asset_id, total_supply) VALUES (?, ?, ?, ?)`);

    const thirtyDaysAgo = Math.floor((Date.now() - THIRTY_DAYS_MS) / 1000);
    let total = 0;

    // Find asset IDs for supply tokens
    const supplyAssetIds = [];
    for (const [assetId, info] of assetCache.entries()) {
        if (SUPPLY_TOKENS.includes(info.symbol)) {
            supplyAssetIds.push({ assetId, symbol: info.symbol, decimals: info.decimals });
        }
    }

    // FIX Bug 23: Filter by timestamp for incremental
    const fromTs = IS_FULL ? 0 : Math.floor(fromTimestamp / 1000); // convert ms to seconds

    for (const { assetId, symbol, decimals } of supplyAssetIds) {
        const result = await pgPool.query(`
            SELECT timestamp, supply
            FROM asset_snapshot
            WHERE asset_id = $1 AND type = 'HOUR'
              AND supply IS NOT NULL
              AND timestamp > $2
            ORDER BY timestamp ASC
        `, [assetId, fromTs]);

        const histBatch = [];
        const mainBatch = [];

        for (const row of result.rows) {
            // FIX Bug 9: Handle supply format (could be raw bigint or numeric)
            let supplyNum;
            const rawSupply = new BigNumber(row.supply);
            if (rawSupply.gt('1e12')) {
                // Raw planck, divide by 10^decimals
                supplyNum = rawSupply.div(new BigNumber(10).pow(decimals)).toNumber();
            } else {
                // Already formatted
                supplyNum = rawSupply.toNumber();
            }
            if (supplyNum <= 0 || isNaN(supplyNum)) continue;

            const timestampMs = tsToMs(row.timestamp);
            const record = [timestampMs, symbol, assetId, supplyNum];

            histBatch.push(record);
            if (row.timestamp >= thirtyDaysAgo) mainBatch.push(record);
        }

        if (histBatch.length > 0) {
            histDb.transaction(() => { for (const r of histBatch) insertHist.run(...r); })();
        }
        if (mainBatch.length > 0) {
            mainDb.transaction(() => { for (const r of mainBatch) insertMain.run(...r); })();
        }

        total += histBatch.length;
        console.log(`  ${symbol}: ${histBatch.length.toLocaleString()} snapshots`);
    }

    console.log(`  ✅ Supply snapshots total: ${total.toLocaleString()}`);
    return total;
}

// ============================================================
// MAIN
// ============================================================

async function main() {
    console.log('='.repeat(60));
    console.log('  SoraMetrics Export: PostgreSQL → SQLite');
    console.log('='.repeat(60));

    const startTime = Date.now();
    const state = loadState();

    const fromBlock = IS_FULL ? 0 : state.lastExportedBlock;
    console.log(`\nMode: ${IS_FULL ? 'FULL EXPORT' : 'INCREMENTAL'}`);
    console.log(`From block: ${fromBlock.toLocaleString()}`);

    // Connect to PostgreSQL
    const pgPool = new Pool(PG_CONFIG);
    let maxBlock;
    try {
        const testResult = await pgPool.query('SELECT MAX(block_height) as max_block FROM history_element');
        maxBlock = testResult.rows[0]?.max_block || 0;
        console.log(`\nPostgreSQL connected. Max block in subsquid: ${maxBlock.toLocaleString()}`);

        if (maxBlock <= fromBlock && !IS_FULL) {
            console.log('No new data to export. Exiting.');
            await pgPool.end();
            return;
        }
    } catch (e) {
        console.error('❌ Cannot connect to PostgreSQL:', e.message);
        console.error('   Make sure sora-subsquid is running and PG is accessible at', `${PG_CONFIG.host}:${PG_CONFIG.port}`);
        process.exit(1);
    }

    // Load metadata
    console.log('\n📦 Loading metadata...');
    await loadAssetMetadata();

    // Detect networkFee format
    await detectNetworkFeeFormat(pgPool);

    // Load price data (limit to known assets to save memory)
    const priceFromTs = IS_FULL ? 0 : Math.floor((Date.now() - 365 * 24 * 60 * 60 * 1000) / 1000);
    await loadPriceSnapshots(pgPool, priceFromTs);

    // FIX Bug 12: For full export, use DELETE FROM instead of file deletion
    // This is safer when the app might be running
    if (IS_FULL) {
        console.log('\n🗑️  Full export: clearing existing SQLite data...');
        for (const dbPath of [HISTORY_DB_PATH, MAIN_DB_PATH]) {
            if (fs.existsSync(dbPath)) {
                const tempDb = new Database(dbPath);
                tempDb.pragma('journal_mode = WAL');
                const tables = tempDb.prepare(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
                ).all();
                for (const t of tables) {
                    tempDb.exec(`DELETE FROM ${t.name}`);
                }
                tempDb.close();
            }
        }
    }

    // Initialize SQLite databases
    console.log('\n📁 Initializing SQLite databases...');
    const histDb = initSqliteDb(HISTORY_DB_PATH);
    const mainDb = initSqliteDb(MAIN_DB_PATH);

    // Run all exports
    const stats = {};
    stats.swaps = await exportSwaps(pgPool, histDb, mainDb, fromBlock, maxBlock);
    stats.transfers = await exportTransfers(pgPool, histDb, mainDb, fromBlock, maxBlock);
    stats.bridges = await exportBridges(pgPool, histDb, mainDb, fromBlock, maxBlock);
    stats.fees = await exportFees(pgPool, histDb, mainDb, fromBlock, maxBlock);
    stats.liquidity = await exportLiquidity(pgPool, histDb, mainDb, fromBlock, maxBlock);
    stats.orderBook = await exportOrderBook(pgPool, histDb, mainDb, fromBlock, maxBlock);
    stats.extrinsics = await exportExtrinsics(pgPool, histDb, mainDb, fromBlock, maxBlock);
    stats.supply = await exportSupplySnapshots(pgPool, histDb, mainDb,
        IS_FULL ? 0 : (state.lastExportedTimestamp || 0));

    // Create indices (after bulk insert for performance)
    console.log('\n📇 Creating indices...');
    createIndices(histDb);
    createIndices(mainDb);

    // Get final max block and timestamp
    const maxResult = await pgPool.query(
        'SELECT MAX(block_height) as max_block, MAX(timestamp) as max_ts FROM history_element'
    );
    const finalBlock = maxResult.rows[0]?.max_block || fromBlock;
    const finalTimestamp = tsToMs(maxResult.rows[0]?.max_ts || 0);

    // Save state
    state.lastExportedBlock = finalBlock;
    state.lastExportedTimestamp = finalTimestamp;
    state.stats = stats;
    saveState(state);

    // Close connections
    histDb.close();
    mainDb.close();
    await pgPool.end();

    const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
    const totalRecords = Object.values(stats).reduce((a, b) => a + b, 0);

    console.log('\n' + '='.repeat(60));
    console.log(`  ✅ Export complete!`);
    console.log(`  Records: ${totalRecords.toLocaleString()}`);
    console.log(`  Time: ${elapsed}s`);
    console.log(`  Block range: ${fromBlock.toLocaleString()} → ${finalBlock.toLocaleString()}`);
    console.log('='.repeat(60));
}

main().catch(e => {
    console.error('❌ Fatal error:', e);
    process.exit(1);
});
