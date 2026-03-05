// db_better.js - Optimized database layer using better-sqlite3
// Drop-in replacement for db.js with:
// - Synchronous better-sqlite3 (prepared statement caching)
// - WAL mode + performance pragmas
// - Indices on both main AND history databases
// - Timestamp filters on getCandles/getSparkline
// - Explicit column lists instead of SELECT *

const Database = require('better-sqlite3');
const path = require('path');
const fs = require('fs');

const DB_PATH = path.join(__dirname, 'database_30d.db');
const HISTORY_DB_PATH = path.join(__dirname, 'database.db');

let db;
let historyAttached = false;

// Prepared statement cache
const stmtCache = new Map();

function getStmt(key, sql) {
    let stmt = stmtCache.get(key);
    if (!stmt) {
        stmt = db.prepare(sql);
        stmtCache.set(key, stmt);
    }
    return stmt;
}

// --- DB INITIALIZATION ---
function initDB() {
    return new Promise((resolve, reject) => {
        try {
            db = new Database(DB_PATH);

            // Performance pragmas
            db.pragma('journal_mode = WAL');
            db.pragma('synchronous = NORMAL');
            db.pragma('cache_size = -64000');    // 64MB
            db.pragma('temp_store = MEMORY');
            db.pragma('mmap_size = 268435456');  // 256MB

            console.log('💾 SQLite conectado (live) [better-sqlite3].');

            createTables();

            // Attach History DB
            if (fs.existsSync(HISTORY_DB_PATH)) {
                try {
                    const attachPath = HISTORY_DB_PATH.replace(/\\/g, '/');
                    db.exec(`ATTACH DATABASE '${attachPath}' AS history`);
                    historyAttached = true;
                    console.log('✅ History DB attached successfully.');

                    // Pragmas for history too
                    db.pragma('history.journal_mode = WAL');
                    db.pragma('history.synchronous = NORMAL');
                    db.pragma('history.cache_size = -64000');
                    db.pragma('history.mmap_size = 268435456');

                    createHistoryIndices();
                } catch (e) {
                    console.warn('⚠️ Could not attach history DB:', e.message);
                }
            } else {
                console.warn('⚠️ History DB not found, running with main only.');
            }

            // PRAGMA optimize every 6 hours
            setInterval(() => {
                try { if (db) db.pragma('optimize'); } catch (e) { }
            }, 6 * 60 * 60 * 1000);

            resolve();
        } catch (e) {
            reject(e);
        }
    });
}

function createTables() {
    db.exec(`CREATE TABLE IF NOT EXISTS transfers (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        timestamp INTEGER,
        formatted_time TEXT,
        block INTEGER,
        from_addr TEXT,
        to_addr TEXT,
        amount TEXT,
        symbol TEXT,
        logo TEXT,
        usd_value REAL,
        asset_id TEXT,
        hash TEXT,
        extrinsic_id TEXT
    )`);

    db.exec(`CREATE TABLE IF NOT EXISTS swaps (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        timestamp INTEGER,
        formatted_time TEXT,
        block INTEGER,
        wallet TEXT,
        in_symbol TEXT,
        in_amount TEXT,
        in_logo TEXT,
        in_usd REAL,
        out_symbol TEXT,
        out_amount TEXT,
        out_logo TEXT,
        out_usd REAL,
        hash TEXT,
        extrinsic_id TEXT
    )`);

    db.exec(`CREATE TABLE IF NOT EXISTS bridges (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        timestamp INTEGER,
        block INTEGER,
        network TEXT,
        direction TEXT,
        sender TEXT,
        recipient TEXT,
        asset_id TEXT,
        symbol TEXT,
        logo TEXT,
        amount TEXT,
        usd_value REAL,
        hash TEXT,
        extrinsic_id TEXT
    )`);

    db.exec(`CREATE TABLE IF NOT EXISTS fees (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        timestamp INTEGER,
        block INTEGER,
        type TEXT,
        amount REAL,
        usd_value REAL
    )`);

    db.exec(`CREATE TABLE IF NOT EXISTS liquidity_events (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        timestamp INTEGER,
        block INTEGER,
        wallet TEXT,
        pool_base TEXT,
        pool_target TEXT,
        base_amount TEXT,
        target_amount TEXT,
        usd_value REAL,
        type TEXT,
        hash TEXT,
        extrinsic_id TEXT
    )`);

    // Safe migrations
    const safeAlter = (table, col, type) => {
        try { db.exec(`ALTER TABLE ${table} ADD COLUMN ${col} ${type}`); } catch (e) { /* already exists */ }
    };

    safeAlter('transfers', 'timestamp', 'INTEGER');
    safeAlter('transfers', 'block', 'INTEGER');
    safeAlter('transfers', 'hash', 'TEXT');
    safeAlter('transfers', 'extrinsic_id', 'TEXT');
    safeAlter('swaps', 'timestamp', 'INTEGER');
    safeAlter('swaps', 'block', 'INTEGER');
    safeAlter('swaps', 'in_logo', 'TEXT');
    safeAlter('swaps', 'out_logo', 'TEXT');
    safeAlter('swaps', 'hash', 'TEXT');
    safeAlter('swaps', 'extrinsic_id', 'TEXT');
    safeAlter('bridges', 'hash', 'TEXT');
    safeAlter('bridges', 'extrinsic_id', 'TEXT');
    safeAlter('bridges', 'symbol', 'TEXT');
    safeAlter('bridges', 'logo', 'TEXT');
    safeAlter('liquidity_events', 'hash', 'TEXT');
    safeAlter('liquidity_events', 'extrinsic_id', 'TEXT');

    // Main DB indices
    const indices = [
        `CREATE INDEX IF NOT EXISTS idx_swaps_timestamp ON swaps(timestamp)`,
        `CREATE INDEX IF NOT EXISTS idx_swaps_in_symbol ON swaps(in_symbol)`,
        `CREATE INDEX IF NOT EXISTS idx_swaps_out_symbol ON swaps(out_symbol)`,
        `CREATE INDEX IF NOT EXISTS idx_swaps_wallet ON swaps(wallet)`,
        `CREATE INDEX IF NOT EXISTS idx_transfers_timestamp ON transfers(timestamp)`,
        `CREATE INDEX IF NOT EXISTS idx_transfers_from ON transfers(from_addr)`,
        `CREATE INDEX IF NOT EXISTS idx_transfers_to ON transfers(to_addr)`,
        `CREATE INDEX IF NOT EXISTS idx_bridges_timestamp ON bridges(timestamp)`,
        `CREATE INDEX IF NOT EXISTS idx_fees_timestamp ON fees(timestamp)`,
        `CREATE INDEX IF NOT EXISTS idx_liquidity_timestamp ON liquidity_events(timestamp)`,
        // Composite indices for common query patterns
        `CREATE INDEX IF NOT EXISTS idx_swaps_block ON swaps(block)`,
        `CREATE INDEX IF NOT EXISTS idx_swaps_timestamp_symbol ON swaps(timestamp, in_symbol, out_symbol)`,
        `CREATE INDEX IF NOT EXISTS idx_transfers_asset ON transfers(asset_id)`,
        `CREATE INDEX IF NOT EXISTS idx_transfers_timestamp_from_to ON transfers(timestamp, from_addr, to_addr)`,
        `CREATE INDEX IF NOT EXISTS idx_bridges_sender ON bridges(sender)`,
        `CREATE INDEX IF NOT EXISTS idx_bridges_recipient ON bridges(recipient)`,
        `CREATE INDEX IF NOT EXISTS idx_bridges_timestamp_sender_recipient ON bridges(timestamp, sender, recipient)`,
        `CREATE INDEX IF NOT EXISTS idx_fees_type ON fees(type)`,
        `CREATE INDEX IF NOT EXISTS idx_liquidity_type ON liquidity_events(type)`,
        `CREATE INDEX IF NOT EXISTS idx_liquidity_pool ON liquidity_events(pool_base, pool_target)`,
        `CREATE INDEX IF NOT EXISTS idx_liquidity_wallet ON liquidity_events(wallet)`
    ];

    for (const idx of indices) {
        db.exec(idx);
    }
}

function createHistoryIndices() {
    if (!historyAttached) return;
    const historyIndices = [
        `CREATE INDEX IF NOT EXISTS history.idx_h_swaps_timestamp ON swaps(timestamp)`,
        `CREATE INDEX IF NOT EXISTS history.idx_h_swaps_in_symbol ON swaps(in_symbol)`,
        `CREATE INDEX IF NOT EXISTS history.idx_h_swaps_out_symbol ON swaps(out_symbol)`,
        `CREATE INDEX IF NOT EXISTS history.idx_h_swaps_wallet ON swaps(wallet)`,
        `CREATE INDEX IF NOT EXISTS history.idx_h_swaps_block ON swaps(block)`,
        `CREATE INDEX IF NOT EXISTS history.idx_h_swaps_timestamp_symbol ON swaps(timestamp, in_symbol, out_symbol)`,
        `CREATE INDEX IF NOT EXISTS history.idx_h_transfers_timestamp ON transfers(timestamp)`,
        `CREATE INDEX IF NOT EXISTS history.idx_h_transfers_from ON transfers(from_addr)`,
        `CREATE INDEX IF NOT EXISTS history.idx_h_transfers_to ON transfers(to_addr)`,
        `CREATE INDEX IF NOT EXISTS history.idx_h_transfers_asset ON transfers(asset_id)`,
        `CREATE INDEX IF NOT EXISTS history.idx_h_transfers_timestamp_from_to ON transfers(timestamp, from_addr, to_addr)`,
        `CREATE INDEX IF NOT EXISTS history.idx_h_bridges_timestamp ON bridges(timestamp)`,
        `CREATE INDEX IF NOT EXISTS history.idx_h_bridges_sender ON bridges(sender)`,
        `CREATE INDEX IF NOT EXISTS history.idx_h_bridges_recipient ON bridges(recipient)`,
        `CREATE INDEX IF NOT EXISTS history.idx_h_bridges_timestamp_sender_recipient ON bridges(timestamp, sender, recipient)`,
        `CREATE INDEX IF NOT EXISTS history.idx_h_fees_timestamp ON fees(timestamp)`,
        `CREATE INDEX IF NOT EXISTS history.idx_h_fees_type ON fees(type)`,
        `CREATE INDEX IF NOT EXISTS history.idx_h_liquidity_timestamp ON liquidity_events(timestamp)`,
        `CREATE INDEX IF NOT EXISTS history.idx_h_liquidity_type ON liquidity_events(type)`,
        `CREATE INDEX IF NOT EXISTS history.idx_h_liquidity_pool ON liquidity_events(pool_base, pool_target)`,
        `CREATE INDEX IF NOT EXISTS history.idx_h_liquidity_wallet ON liquidity_events(wallet)`
    ];

    for (const idx of historyIndices) {
        try { db.exec(idx); } catch (e) { /* table may not exist in history */ }
    }
    console.log('📊 History DB indices created.');
}

// --- Helper: Check if history has a specific table ---
const historyTableCache = new Map();
function historyHasTable(table) {
    if (!historyAttached) return false;
    if (historyTableCache.has(table)) return historyTableCache.get(table);
    try {
        const row = db.prepare(`SELECT name FROM history.sqlite_master WHERE type='table' AND name=?`).get(table);
        const exists = !!row;
        historyTableCache.set(table, exists);
        return exists;
    } catch (e) {
        return false;
    }
}

// --- Helper: Deduplication ---
function dedup(rows, keyFn) {
    const seen = new Set();
    return rows.filter(r => {
        const key = keyFn(r);
        if (seen.has(key)) return false;
        seen.add(key);
        return true;
    });
}

// --- Mappers ---
function formatTimestamp(ts) {
    if (!ts) return null;
    const d = new Date(ts);
    if (isNaN(d.getTime())) return null;
    return `${d.getDate().toString().padStart(2, '0')}/${(d.getMonth() + 1).toString().padStart(2, '0')}/${d.getFullYear()} ${d.getHours().toString().padStart(2, '0')}:${d.getMinutes().toString().padStart(2, '0')}:${d.getSeconds().toString().padStart(2, '0')}`;
}

function mapTransfers(rows) {
    return rows.map(r => ({
        time: formatTimestamp(r.timestamp) || r.formatted_time,
        block: r.block,
        hash: r.hash,
        extrinsic_id: r.extrinsic_id,
        from: r.from_addr,
        to: r.to_addr,
        amount: r.amount,
        symbol: r.symbol,
        logo: r.logo,
        usdValue: r.usd_value ? r.usd_value.toFixed(2) : '0.00',
        assetId: r.asset_id
    }));
}

function mapSwaps(rows) {
    return rows.map(r => ({
        time: formatTimestamp(r.timestamp) || r.formatted_time,
        block: r.block,
        hash: r.hash,
        extrinsic_id: r.extrinsic_id,
        wallet: r.wallet,
        in: { symbol: r.in_symbol, amount: r.in_amount, logo: r.in_logo, usd: r.in_usd.toFixed(2) },
        out: { symbol: r.out_symbol, amount: r.out_amount, logo: r.out_logo, usd: r.out_usd.toFixed(2) }
    }));
}

function transferDedupKey(r) {
    return `${r.block}_${r.from_addr}_${r.to_addr}_${String(r.amount).replace(/,/g, '')}_${r.asset_id}`;
}

function swapDedupKey(r) {
    return `${r.block}_${r.wallet}_${r.in_symbol}_${String(r.in_amount).replace(/,/g, '')}_${r.out_symbol}_${String(r.out_amount).replace(/,/g, '')}`;
}

function bridgeDedupKey(r) {
    return `${r.block}_${r.sender}_${r.recipient}_${String(r.amount).replace(/,/g, '')}`;
}

// --- INSERT FUNCTIONS (fire-and-forget, no need to await) ---

const insertStmts = {};

function insertTransfer(t) {
    try {
        if (!insertStmts.transfer) {
            insertStmts.transfer = db.prepare(`INSERT INTO transfers (timestamp, formatted_time, from_addr, to_addr, amount, symbol, logo, usd_value, asset_id, block, hash, extrinsic_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`);
        }
        insertStmts.transfer.run(Date.now(), t.time, t.from, t.to, t.amount, t.symbol, t.logo, parseFloat(t.usdValue), t.assetId, t.block || 0, t.hash || '', t.extrinsic_id || '');
    } catch (e) {
        console.error('Error insertTransfer:', e.message);
    }
}

function insertSwap(s) {
    try {
        if (!insertStmts.swap) {
            insertStmts.swap = db.prepare(`INSERT INTO swaps (timestamp, formatted_time, block, wallet, in_symbol, in_amount, in_logo, in_usd, out_symbol, out_amount, out_logo, out_usd, hash, extrinsic_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`);
        }
        insertStmts.swap.run(
            Date.now(), s.time, s.block, s.wallet,
            s.in.symbol, s.in.amount, s.in.logo, parseFloat(s.in.usd),
            s.out.symbol, s.out.amount, s.out.logo, parseFloat(s.out.usd),
            s.hash || '', s.extrinsic_id || ''
        );
    } catch (e) {
        console.error('Error insertSwap:', e.message);
    }
}

function insertBridge(b) {
    try {
        if (!insertStmts.bridge) {
            insertStmts.bridge = db.prepare(`INSERT INTO bridges (timestamp, block, network, direction, sender, recipient, asset_id, symbol, logo, amount, usd_value, hash, extrinsic_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`);
        }
        insertStmts.bridge.run(Date.now(), b.block, b.network, b.direction, b.sender, b.recipient, b.assetId || b.asset_id, b.symbol || 'UNK', b.logo || '', b.amount, b.usdValue || b.usd_value, b.hash || '', b.extrinsic_id || '');
    } catch (e) {
        console.error('Error insertBridge:', e.message);
    }
}

function insertFee(f) {
    try {
        if (!insertStmts.fee) {
            insertStmts.fee = db.prepare(`INSERT INTO fees (timestamp, block, type, amount, usd_value) VALUES (?, ?, ?, ?, ?)`);
        }
        insertStmts.fee.run(Date.now(), f.block, f.type, f.amount, f.usdValue);
    } catch (e) {
        console.error('Error insertFee:', e.message);
    }
}

function insertLiquidityEvent(event) {
    try {
        if (!insertStmts.liquidity) {
            insertStmts.liquidity = db.prepare(`INSERT INTO liquidity_events (timestamp, block, wallet, pool_base, pool_target, base_amount, target_amount, usd_value, type, hash, extrinsic_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`);
        }
        insertStmts.liquidity.run(
            Date.now(), event.block || 0, event.wallet,
            event.poolBase, event.poolTarget,
            event.baseAmount, event.targetAmount,
            event.usdValue || 0, event.type,
            event.hash || '', event.extrinsic_id || ''
        );
    } catch (e) {
        console.error('Error insertLiquidityEvent:', e.message);
    }
}

// --- TRANSFERS READ ---

function getTransfers(address, page = 1, limit = 25) {
    const offset = (page - 1) * limit;
    const cols = `id, timestamp, formatted_time, from_addr, to_addr, amount, symbol, logo, usd_value, asset_id, block, hash, extrinsic_id`;

    let total, rows;

    if (historyHasTable('transfers')) {
        total = getStmt('getTransfers_count',
            `SELECT (SELECT COUNT(*) FROM main.transfers WHERE from_addr = ? OR to_addr = ?) + (SELECT COUNT(*) FROM history.transfers WHERE from_addr = ? OR to_addr = ?) as total`
        ).get(address, address, address, address).total || 0;

        rows = getStmt('getTransfers_data',
            `SELECT ${cols} FROM main.transfers WHERE from_addr = ? OR to_addr = ?
             UNION ALL
             SELECT ${cols} FROM history.transfers WHERE from_addr = ? OR to_addr = ?
             ORDER BY timestamp DESC LIMIT ? OFFSET ?`
        ).all(address, address, address, address, limit, offset);
    } else {
        total = getStmt('getTransfers_count_main',
            `SELECT COUNT(*) as total FROM main.transfers WHERE from_addr = ? OR to_addr = ?`
        ).get(address, address).total || 0;

        rows = getStmt('getTransfers_data_main',
            `SELECT ${cols} FROM main.transfers WHERE from_addr = ? OR to_addr = ? ORDER BY timestamp DESC LIMIT ? OFFSET ?`
        ).all(address, address, limit, offset);
    }

    const unique = dedup(rows, transferDedupKey);
    const totalPages = Math.ceil(total / limit);
    return { data: mapTransfers(unique), total, page, totalPages };
}

function getLatestTransfers(page = 1, limit = 25, filter = null, timestamp = null) {
    const offset = (page - 1) * limit;
    const cols = `id, timestamp, formatted_time, from_addr, to_addr, amount, symbol, logo, usd_value, asset_id, block, hash, extrinsic_id`;

    let conditions = [];
    let params = [];

    if (filter) {
        const f = `%${filter.toUpperCase()}%`;
        conditions.push(`(symbol LIKE ? OR from_addr LIKE ? OR to_addr LIKE ?)`);
        params.push(f, f, f);
    }
    if (timestamp) {
        conditions.push(`timestamp <= ?`);
        params.push(timestamp);
    }

    const where = conditions.length > 0 ? ' WHERE ' + conditions.join(' AND ') : '';

    let total, rows;

    // Dynamic queries (with filter) can't be cached as prepared statements
    if (historyHasTable('transfers')) {
        const countSql = `SELECT (SELECT COUNT(*) FROM main.transfers${where}) + (SELECT COUNT(*) FROM history.transfers${where}) as total`;
        total = db.prepare(countSql).get(...params, ...params)?.total || 0;

        const dataSql = `SELECT ${cols} FROM main.transfers${where} UNION ALL SELECT ${cols} FROM history.transfers${where} ORDER BY timestamp DESC LIMIT ? OFFSET ?`;
        rows = db.prepare(dataSql).all(...params, ...params, limit, offset);
    } else {
        const countSql = `SELECT COUNT(*) as total FROM main.transfers${where}`;
        total = db.prepare(countSql).get(...params)?.total || 0;

        const dataSql = `SELECT ${cols} FROM main.transfers${where} ORDER BY timestamp DESC LIMIT ? OFFSET ?`;
        rows = db.prepare(dataSql).all(...params, limit, offset);
    }

    const unique = dedup(rows, transferDedupKey);
    const totalPages = Math.ceil(total / limit);
    return { data: mapTransfers(unique), total, page, totalPages };
}

// --- SWAPS READ ---

function getSwaps(address, page = 1, limit = 25) {
    const offset = (page - 1) * limit;
    const cols = `id, timestamp, formatted_time, block, wallet, in_symbol, in_amount, in_logo, in_usd, out_symbol, out_amount, out_logo, out_usd, hash, extrinsic_id`;

    let total, rows;

    if (historyHasTable('swaps')) {
        total = getStmt('getSwaps_count',
            `SELECT (SELECT COUNT(*) FROM main.swaps WHERE wallet = ?) + (SELECT COUNT(*) FROM history.swaps WHERE wallet = ?) as total`
        ).get(address, address).total || 0;

        rows = getStmt('getSwaps_data',
            `SELECT ${cols} FROM main.swaps WHERE wallet = ?
             UNION ALL
             SELECT ${cols} FROM history.swaps WHERE wallet = ?
             ORDER BY timestamp DESC LIMIT ? OFFSET ?`
        ).all(address, address, limit, offset);
    } else {
        total = getStmt('getSwaps_count_main',
            `SELECT COUNT(*) as total FROM main.swaps WHERE wallet = ?`
        ).get(address).total || 0;

        rows = getStmt('getSwaps_data_main',
            `SELECT ${cols} FROM main.swaps WHERE wallet = ? ORDER BY timestamp DESC LIMIT ? OFFSET ?`
        ).all(address, limit, offset);
    }

    const unique = dedup(rows, swapDedupKey);
    const totalPages = Math.ceil(total / limit);
    return { data: mapSwaps(unique), total, page, totalPages };
}

function getLatestSwaps(page = 1, limit = 25, filter = null, timestamp = null) {
    const offset = (page - 1) * limit;
    const cols = `id, timestamp, formatted_time, block, wallet, in_symbol, in_amount, in_logo, in_usd, out_symbol, out_amount, out_logo, out_usd, hash, extrinsic_id`;

    let conditions = [];
    let params = [];

    if (filter) {
        const f = `%${filter.toUpperCase()}%`;
        conditions.push(`(in_symbol LIKE ? OR out_symbol LIKE ?)`);
        params.push(f, f);
    }
    if (timestamp) {
        conditions.push(`timestamp <= ?`);
        params.push(timestamp);
    }

    const where = conditions.length > 0 ? ' WHERE ' + conditions.join(' AND ') : '';

    let total, rows;

    if (historyHasTable('swaps')) {
        const countSql = `SELECT COUNT(*) as count FROM (SELECT id FROM main.swaps${where} UNION ALL SELECT id FROM history.swaps${where})`;
        total = db.prepare(countSql).get(...params, ...params)?.count || 0;

        const dataSql = `SELECT ${cols} FROM (SELECT ${cols} FROM main.swaps${where} UNION ALL SELECT ${cols} FROM history.swaps${where}) ORDER BY timestamp DESC LIMIT ? OFFSET ?`;
        rows = db.prepare(dataSql).all(...params, ...params, limit, offset);
    } else {
        const countSql = `SELECT COUNT(*) as count FROM main.swaps${where}`;
        total = db.prepare(countSql).get(...params)?.count || 0;

        const dataSql = `SELECT ${cols} FROM main.swaps${where} ORDER BY timestamp DESC LIMIT ? OFFSET ?`;
        rows = db.prepare(dataSql).all(...params, limit, offset);
    }

    const unique = dedup(rows, swapDedupKey);
    const totalPages = Math.ceil(total / limit);
    return { data: mapSwaps(unique), total, page, totalPages };
}

// --- CHARTING ---

function getCandles(symbol, resolution = 60, limit = 1000) {
    const intervalMs = resolution * 60 * 1000;
    // FIX: Add timestamp filter - only fetch data needed for the requested candle count
    const timeWindow = intervalMs * limit;
    const startTime = Date.now() - timeWindow;

    const cols = `timestamp, in_symbol, out_symbol, in_amount, out_amount, in_usd, out_usd`;
    let rows;

    if (historyHasTable('swaps')) {
        rows = getStmt('getCandles_unified',
            `SELECT ${cols} FROM main.swaps WHERE (in_symbol = ? OR out_symbol = ?) AND timestamp >= ?
             UNION ALL
             SELECT ${cols} FROM history.swaps WHERE (in_symbol = ? OR out_symbol = ?) AND timestamp >= ?
             ORDER BY timestamp ASC`
        ).all(symbol, symbol, startTime, symbol, symbol, startTime);
    } else {
        rows = getStmt('getCandles_main',
            `SELECT ${cols} FROM main.swaps WHERE (in_symbol = ? OR out_symbol = ?) AND timestamp >= ? ORDER BY timestamp ASC`
        ).all(symbol, symbol, startTime);
    }

    const candles = [];
    let currentBucket = null;
    let open = 0, high = 0, low = 0, close = 0;
    let bucketStartTime = 0;

    for (const r of rows) {
        let price = 0;
        if (r.in_symbol === symbol) {
            price = r.in_usd / parseFloat(r.in_amount);
        } else {
            price = r.out_usd / parseFloat(r.out_amount);
        }
        if (isNaN(price) || price === 0) continue;

        const bucket = Math.floor(r.timestamp / intervalMs) * intervalMs;

        if (currentBucket !== bucket) {
            if (currentBucket !== null) {
                candles.push({ time: bucketStartTime / 1000, open, high, low, close });
            }
            currentBucket = bucket;
            bucketStartTime = bucket;
            open = price;
            high = price;
            low = price;
            close = price;
        } else {
            high = Math.max(high, price);
            low = Math.min(low, price);
            close = price;
        }
    }

    if (currentBucket !== null) {
        candles.push({ time: bucketStartTime / 1000, open, high, low, close });
    }

    return candles.slice(-limit);
}

function getPriceChange(symbol, currentPrice, timeframeMs) {
    if (!currentPrice || currentPrice === 0) return Promise.resolve(0);

    const pastTime = Date.now() - timeframeMs;
    const cols = `in_symbol, out_symbol, in_amount, out_amount, in_usd, out_usd`;

    let row;
    if (historyHasTable('swaps')) {
        row = getStmt('getPriceChange_unified',
            `SELECT ${cols} FROM main.swaps WHERE (in_symbol = ? OR out_symbol = ?) AND timestamp <= ?
             UNION ALL
             SELECT ${cols} FROM history.swaps WHERE (in_symbol = ? OR out_symbol = ?) AND timestamp <= ?
             ORDER BY timestamp DESC LIMIT 1`
        ).get(symbol, symbol, pastTime, symbol, symbol, pastTime);
    } else {
        row = getStmt('getPriceChange_main',
            `SELECT ${cols} FROM main.swaps WHERE (in_symbol = ? OR out_symbol = ?) AND timestamp <= ? ORDER BY timestamp DESC LIMIT 1`
        ).get(symbol, symbol, pastTime);
    }

    if (!row) return Promise.resolve(0);

    let oldPrice = 0;
    if (row.in_symbol === symbol) {
        oldPrice = row.in_usd / parseFloat(row.in_amount);
    } else {
        oldPrice = row.out_usd / parseFloat(row.out_amount);
    }
    if (oldPrice === 0) return Promise.resolve(0);

    return Promise.resolve(((currentPrice - oldPrice) / oldPrice) * 100);
}

function getSparkline(symbol, timeframeMs) {
    // FIX: Push timestamp filter to SQL instead of fetching 1000 rows and filtering in JS
    const startTime = Date.now() - timeframeMs;
    const cols = `timestamp, in_symbol, out_symbol, in_amount, out_amount, in_usd, out_usd`;

    let rows;
    if (historyHasTable('swaps')) {
        rows = getStmt('getSparkline_unified',
            `SELECT ${cols} FROM main.swaps WHERE (in_symbol = ? OR out_symbol = ?) AND timestamp >= ?
             UNION ALL
             SELECT ${cols} FROM history.swaps WHERE (in_symbol = ? OR out_symbol = ?) AND timestamp >= ?
             ORDER BY timestamp ASC`
        ).all(symbol, symbol, startTime, symbol, symbol, startTime);
    } else {
        rows = getStmt('getSparkline_main',
            `SELECT ${cols} FROM main.swaps WHERE (in_symbol = ? OR out_symbol = ?) AND timestamp >= ? ORDER BY timestamp ASC`
        ).all(symbol, symbol, startTime);
    }

    if (!rows || rows.length === 0) return Promise.resolve([]);

    const points = [];
    for (const r of rows) {
        let val = 0;
        if (r.in_symbol === symbol) val = r.in_usd / r.in_amount;
        else val = r.out_usd / r.out_amount;
        if (isNaN(val) || val === 0) continue;
        points.push({ value: val, time: r.timestamp });
    }

    if (points.length === 0) return Promise.resolve([]);
    if (points.length <= 20) return Promise.resolve(points);

    // Downsample to ~20 points
    const result = [];
    const step = Math.ceil(points.length / 20);
    for (let i = 0; i < points.length; i += step) {
        result.push(points[i]);
    }
    if (result[result.length - 1].time !== points[points.length - 1].time) {
        result.push(points[points.length - 1]);
    }
    return Promise.resolve(result);
}

// --- ACCUMULATORS & STATS ---

function getTopAccumulators(symbol, timeframeMs) {
    const startTime = Date.now() - timeframeMs;
    // FIX: Explicit columns instead of SELECT *
    if (historyHasTable('swaps')) {
        return getStmt('getTopAccumulators_unified',
            `SELECT wallet, SUM(out_usd) as total_bought_usd, SUM(out_amount) as total_bought_amount, COUNT(*) as swap_count, MAX(timestamp) as last_buy
             FROM (
                 SELECT wallet, out_usd, out_amount, out_symbol, timestamp FROM main.swaps WHERE out_symbol = ? AND timestamp > ?
                 UNION ALL
                 SELECT wallet, out_usd, out_amount, out_symbol, timestamp FROM history.swaps WHERE out_symbol = ? AND timestamp > ?
             )
             GROUP BY wallet ORDER BY total_bought_usd DESC LIMIT 10`
        ).all(symbol, startTime, symbol, startTime);
    } else {
        return getStmt('getTopAccumulators_main',
            `SELECT wallet, SUM(out_usd) as total_bought_usd, SUM(out_amount) as total_bought_amount, COUNT(*) as swap_count, MAX(timestamp) as last_buy
             FROM main.swaps WHERE out_symbol = ? AND timestamp > ? GROUP BY wallet ORDER BY total_bought_usd DESC LIMIT 10`
        ).all(symbol, startTime);
    }
}

function getNetworkStats(timeframeMs) {
    const startTime = Date.now() - timeframeMs;

    let volume, users, txCount;

    if (historyHasTable('swaps')) {
        volume = getStmt('getNetworkStats_vol',
            `SELECT SUM(in_usd) as total_vol FROM (SELECT in_usd FROM main.swaps WHERE timestamp > ? UNION ALL SELECT in_usd FROM history.swaps WHERE timestamp > ?)`
        ).get(startTime, startTime)?.total_vol || 0;

        users = getStmt('getNetworkStats_users',
            `SELECT COUNT(DISTINCT wallet) as active_users FROM (SELECT wallet FROM main.swaps WHERE timestamp > ? UNION ALL SELECT wallet FROM history.swaps WHERE timestamp > ?)`
        ).get(startTime, startTime)?.active_users || 0;

        txCount = getStmt('getNetworkStats_count',
            `SELECT COUNT(*) as tx_count FROM (SELECT id FROM main.swaps WHERE timestamp > ? UNION ALL SELECT id FROM history.swaps WHERE timestamp > ?)`
        ).get(startTime, startTime)?.tx_count || 0;
    } else {
        volume = getStmt('getNetworkStats_vol_main',
            `SELECT SUM(in_usd) as total_vol FROM main.swaps WHERE timestamp > ?`
        ).get(startTime)?.total_vol || 0;

        users = getStmt('getNetworkStats_users_main',
            `SELECT COUNT(DISTINCT wallet) as active_users FROM main.swaps WHERE timestamp > ?`
        ).get(startTime)?.active_users || 0;

        txCount = getStmt('getNetworkStats_count_main',
            `SELECT COUNT(*) as tx_count FROM main.swaps WHERE timestamp > ?`
        ).get(startTime)?.tx_count || 0;
    }

    return { volume, users, txCount };
}

function getMarketTrends(timeframeMs) {
    const startTime = Date.now() - timeframeMs;
    // FIX: Explicit columns instead of SELECT *
    if (historyHasTable('swaps')) {
        return getStmt('getMarketTrends_unified',
            `SELECT CASE WHEN in_usd > 0 THEN in_symbol ELSE out_symbol END as symbol, SUM(in_usd + out_usd) as volume
             FROM (
                 SELECT in_symbol, out_symbol, in_usd, out_usd, timestamp FROM main.swaps WHERE timestamp > ?
                 UNION ALL
                 SELECT in_symbol, out_symbol, in_usd, out_usd, timestamp FROM history.swaps WHERE timestamp > ?
             )
             GROUP BY symbol ORDER BY volume DESC LIMIT 5`
        ).all(startTime, startTime);
    } else {
        return getStmt('getMarketTrends_main',
            `SELECT CASE WHEN in_usd > 0 THEN in_symbol ELSE out_symbol END as symbol, SUM(in_usd + out_usd) as volume
             FROM main.swaps WHERE timestamp > ? GROUP BY symbol ORDER BY volume DESC LIMIT 5`
        ).all(startTime);
    }
}

function getTotalStats() {
    let swaps, transfers;

    if (historyHasTable('swaps') && historyHasTable('transfers')) {
        swaps = getStmt('getTotalStats_swaps',
            `SELECT (SELECT COUNT(*) FROM main.swaps) + (SELECT COUNT(*) FROM history.swaps) as count`
        ).get().count || 0;

        transfers = getStmt('getTotalStats_transfers',
            `SELECT (SELECT COUNT(*) FROM main.transfers) + (SELECT COUNT(*) FROM history.transfers) as count`
        ).get().count || 0;
    } else {
        swaps = getStmt('getTotalStats_swaps_main',
            `SELECT COUNT(*) as count FROM main.swaps`
        ).get().count || 0;

        transfers = getStmt('getTotalStats_transfers_main',
            `SELECT COUNT(*) as count FROM main.transfers`
        ).get().count || 0;
    }

    return { swaps, transfers };
}

function getFilteredStats(startTime) {
    let swaps, transfers, bridges;

    if (historyHasTable('swaps') && historyHasTable('transfers') && historyHasTable('bridges')) {
        swaps = getStmt('getFilteredStats_swaps',
            `SELECT (SELECT COUNT(*) FROM main.swaps WHERE timestamp >= ?) + (SELECT COUNT(*) FROM history.swaps WHERE timestamp >= ?) as count`
        ).get(startTime, startTime).count || 0;

        transfers = getStmt('getFilteredStats_transfers',
            `SELECT (SELECT COUNT(*) FROM main.transfers WHERE timestamp >= ?) + (SELECT COUNT(*) FROM history.transfers WHERE timestamp >= ?) as count`
        ).get(startTime, startTime).count || 0;

        bridges = getStmt('getFilteredStats_bridges',
            `SELECT (SELECT COUNT(*) FROM main.bridges WHERE timestamp >= ?) + (SELECT COUNT(*) FROM history.bridges WHERE timestamp >= ?) as count`
        ).get(startTime, startTime).count || 0;
    } else {
        swaps = getStmt('getFilteredStats_swaps_main',
            `SELECT COUNT(*) as count FROM main.swaps WHERE timestamp >= ?`
        ).get(startTime).count || 0;

        transfers = getStmt('getFilteredStats_transfers_main',
            `SELECT COUNT(*) as count FROM main.transfers WHERE timestamp >= ?`
        ).get(startTime).count || 0;

        bridges = getStmt('getFilteredStats_bridges_main',
            `SELECT COUNT(*) as count FROM main.bridges WHERE timestamp >= ?`
        ).get(startTime).count || 0;
    }

    return { swaps, transfers, bridges };
}

// --- FEES ---

function getFeeStats(startTime) {
    if (historyHasTable('fees')) {
        return getStmt('getFeeStats_unified',
            `SELECT type, SUM(amount) as total_xor, SUM(usd_value) as total_usd
             FROM (
                 SELECT type, amount, usd_value FROM main.fees WHERE timestamp >= ?
                 UNION ALL
                 SELECT type, amount, usd_value FROM history.fees WHERE timestamp >= ?
             )
             GROUP BY type`
        ).all(startTime, startTime);
    } else {
        return getStmt('getFeeStats_main',
            `SELECT type, SUM(amount) as total_xor, SUM(usd_value) as total_usd FROM main.fees WHERE timestamp >= ? GROUP BY type`
        ).all(startTime);
    }
}

function getFeeTrend(startTime, interval) {
    let fmt = '%Y-%m-%d %H:00:00';
    if (interval === 'day') fmt = '%Y-%m-%d';

    // Can't cache because fmt varies
    const sql = historyHasTable('fees')
        ? `SELECT strftime('${fmt}', timestamp / 1000, 'unixepoch') as bucket, SUM(usd_value) as total_usd
           FROM (
               SELECT timestamp, usd_value FROM main.fees WHERE timestamp >= ?
               UNION ALL
               SELECT timestamp, usd_value FROM history.fees WHERE timestamp >= ?
           )
           GROUP BY bucket ORDER BY bucket ASC`
        : `SELECT strftime('${fmt}', timestamp / 1000, 'unixepoch') as bucket, SUM(usd_value) as total_usd
           FROM main.fees WHERE timestamp >= ? GROUP BY bucket ORDER BY bucket ASC`;

    const params = historyHasTable('fees') ? [startTime, startTime] : [startTime];
    return db.prepare(sql).all(...params);
}

// --- BRIDGES ---

function getWalletBridges(address, page = 1, limit = 20) {
    const offset = (page - 1) * limit;
    const cols = `id, timestamp, block, network, direction, sender, recipient, asset_id, amount, usd_value`;

    let total, rows;

    if (historyHasTable('bridges')) {
        total = getStmt('getWalletBridges_count',
            `SELECT (SELECT COUNT(*) FROM main.bridges WHERE sender = ? OR recipient = ?) + (SELECT COUNT(*) FROM history.bridges WHERE sender = ? OR recipient = ?) as total`
        ).get(address, address, address, address).total || 0;

        rows = getStmt('getWalletBridges_data',
            `SELECT ${cols} FROM main.bridges WHERE sender = ? OR recipient = ?
             UNION ALL
             SELECT ${cols} FROM history.bridges WHERE sender = ? OR recipient = ?
             ORDER BY timestamp DESC LIMIT ? OFFSET ?`
        ).all(address, address, address, address, limit, offset);
    } else {
        total = getStmt('getWalletBridges_count_main',
            `SELECT COUNT(*) as total FROM main.bridges WHERE sender = ? OR recipient = ?`
        ).get(address, address).total || 0;

        rows = getStmt('getWalletBridges_data_main',
            `SELECT ${cols} FROM main.bridges WHERE sender = ? OR recipient = ? ORDER BY timestamp DESC LIMIT ? OFFSET ?`
        ).all(address, address, limit, offset);
    }

    const unique = dedup(rows, bridgeDedupKey);
    const totalPages = Math.ceil(total / limit);
    return {
        data: unique.map(r => ({ ...r, time: new Date(r.timestamp).toLocaleString() })),
        total, totalPages, page
    };
}

function getLatestBridges(page = 1, limit = 20, filter = null, timestamp = null) {
    const offset = (page - 1) * limit;
    const cols = `id, timestamp, block, network, direction, sender, recipient, asset_id, amount, usd_value, hash, extrinsic_id`;

    let conditions = [];
    let params = [];

    if (filter) {
        const f = `%${filter.toUpperCase()}%`;
        conditions.push(`(sender LIKE ? OR recipient LIKE ? OR network LIKE ? OR asset_id LIKE ?)`);
        params.push(f, f, f, f);
    }
    if (timestamp) {
        conditions.push(`timestamp <= ?`);
        params.push(timestamp);
    }

    const where = conditions.length > 0 ? ' WHERE ' + conditions.join(' AND ') : '';

    let total, rows;

    if (historyHasTable('bridges')) {
        const countSql = `SELECT (SELECT COUNT(*) FROM main.bridges${where}) + (SELECT COUNT(*) FROM history.bridges${where}) as total`;
        total = db.prepare(countSql).get(...params, ...params)?.total || 0;

        const dataSql = `SELECT ${cols} FROM main.bridges${where} UNION ALL SELECT ${cols} FROM history.bridges${where} ORDER BY timestamp DESC LIMIT ? OFFSET ?`;
        rows = db.prepare(dataSql).all(...params, ...params, limit, offset);
    } else {
        const countSql = `SELECT COUNT(*) as total FROM main.bridges${where}`;
        total = db.prepare(countSql).get(...params)?.total || 0;

        const dataSql = `SELECT ${cols} FROM main.bridges${where} ORDER BY timestamp DESC LIMIT ? OFFSET ?`;
        rows = db.prepare(dataSql).all(...params, limit, offset);
    }

    const unique = dedup(rows, bridgeDedupKey);
    const totalPages = Math.ceil(total / limit);
    return {
        data: unique.map(r => ({ ...r, time: new Date(r.timestamp).toLocaleString() })),
        total, totalPages, page
    };
}

// --- LIQUIDITY ---

function getLpVolume(msWindow) {
    const startTime = Date.now() - msWindow;

    if (historyHasTable('liquidity_events')) {
        return getStmt('getLpVolume_unified',
            `SELECT COALESCE(SUM(val), 0) as total FROM (
                SELECT COALESCE(SUM(CASE WHEN type = 'deposit' THEN usd_value ELSE -usd_value END), 0) as val FROM main.liquidity_events WHERE timestamp >= ?
                UNION ALL
                SELECT COALESCE(SUM(CASE WHEN type = 'deposit' THEN usd_value ELSE -usd_value END), 0) as val FROM history.liquidity_events WHERE timestamp >= ?
            )`
        ).get(startTime, startTime)?.total || 0;
    } else {
        return getStmt('getLpVolume_main',
            `SELECT COALESCE(SUM(CASE WHEN type = 'deposit' THEN usd_value ELSE -usd_value END), 0) as total FROM main.liquidity_events WHERE timestamp >= ?`
        ).get(startTime)?.total || 0;
    }
}

function getTransferVolume(msWindow) {
    const startTime = Date.now() - msWindow;

    if (historyHasTable('transfers')) {
        return getStmt('getTransferVolume_unified',
            `SELECT COALESCE(SUM(val), 0) as total FROM (
                SELECT COALESCE(SUM(usd_value), 0) as val FROM main.transfers WHERE timestamp >= ?
                UNION ALL
                SELECT COALESCE(SUM(usd_value), 0) as val FROM history.transfers WHERE timestamp >= ?
            )`
        ).get(startTime, startTime)?.total || 0;
    } else {
        return getStmt('getTransferVolume_main',
            `SELECT COALESCE(SUM(usd_value), 0) as total FROM main.transfers WHERE timestamp >= ?`
        ).get(startTime)?.total || 0;
    }
}

function getPoolActivity(base, target, limit = 10) {
    let rows;

    if (historyHasTable('liquidity_events')) {
        rows = getStmt('getPoolActivity_unified',
            `SELECT id, timestamp, block, wallet, pool_base, pool_target, base_amount, target_amount, usd_value, type, hash, extrinsic_id FROM (
                SELECT id, timestamp, block, wallet, pool_base, pool_target, base_amount, target_amount, usd_value, type, hash, extrinsic_id FROM main.liquidity_events WHERE pool_base = ? AND pool_target = ?
                UNION ALL
                SELECT id, timestamp, block, wallet, pool_base, pool_target, base_amount, target_amount, usd_value, type, hash, extrinsic_id FROM history.liquidity_events WHERE pool_base = ? AND pool_target = ?
            ) ORDER BY timestamp DESC LIMIT ?`
        ).all(base, target, base, target, limit);
    } else {
        rows = getStmt('getPoolActivity_main',
            `SELECT id, timestamp, block, wallet, pool_base, pool_target, base_amount, target_amount, usd_value, type, hash, extrinsic_id FROM main.liquidity_events WHERE pool_base = ? AND pool_target = ? ORDER BY timestamp DESC LIMIT ?`
        ).all(base, target, limit);
    }

    return (rows || []).map(r => ({ ...r, time: new Date(r.timestamp).toLocaleString() }));
}

function getLiquidityEvents(page = 1, limit = 25, timestamp = null) {
    const offset = (page - 1) * limit;
    let where = '';
    let params = [];

    if (timestamp) {
        where = ' WHERE timestamp <= ?';
        params.push(timestamp);
    }

    let total, rows;

    if (historyHasTable('liquidity_events')) {
        const countSql = `SELECT COUNT(*) as total FROM (SELECT timestamp FROM main.liquidity_events${where} UNION ALL SELECT timestamp FROM history.liquidity_events${where})`;
        total = db.prepare(countSql).get(...params, ...params)?.total || 0;

        const dataSql = `SELECT * FROM (SELECT * FROM main.liquidity_events${where} UNION ALL SELECT * FROM history.liquidity_events${where}) ORDER BY timestamp DESC LIMIT ? OFFSET ?`;
        rows = db.prepare(dataSql).all(...params, ...params, limit, offset);
    } else {
        const countSql = `SELECT COUNT(*) as total FROM main.liquidity_events${where}`;
        total = db.prepare(countSql).get(...params)?.total || 0;

        const dataSql = `SELECT * FROM main.liquidity_events${where} ORDER BY timestamp DESC LIMIT ? OFFSET ?`;
        rows = db.prepare(dataSql).all(...params, limit, offset);
    }

    return { data: rows || [], total };
}

// --- NETWORK TREND ---

function getNetworkTrend(startTime, interval) {
    let fmt = '%Y-%m-%d %H:00:00';
    if (interval === 'day') fmt = '%Y-%m-%d';

    const hasHistSwaps = historyHasTable('swaps');
    const hasHistTransfers = historyHasTable('transfers');
    const hasHistLiquidity = historyHasTable('liquidity_events');

    // Swaps trend
    let swaps;
    if (hasHistSwaps) {
        swaps = db.prepare(
            `SELECT strftime('${fmt}', timestamp / 1000, 'unixepoch') as bucket, SUM(in_usd) as val FROM (
                SELECT timestamp, in_usd FROM main.swaps WHERE timestamp >= ?
                UNION ALL SELECT timestamp, in_usd FROM history.swaps WHERE timestamp >= ?
            ) GROUP BY bucket`
        ).all(startTime, startTime);
    } else {
        swaps = db.prepare(
            `SELECT strftime('${fmt}', timestamp / 1000, 'unixepoch') as bucket, SUM(in_usd) as val FROM main.swaps WHERE timestamp >= ? GROUP BY bucket`
        ).all(startTime);
    }

    // Transfers trend
    let transfers;
    if (hasHistTransfers) {
        transfers = db.prepare(
            `SELECT strftime('${fmt}', timestamp / 1000, 'unixepoch') as bucket, SUM(usd_value) as val FROM (
                SELECT timestamp, usd_value FROM main.transfers WHERE timestamp >= ?
                UNION ALL SELECT timestamp, usd_value FROM history.transfers WHERE timestamp >= ?
            ) GROUP BY bucket`
        ).all(startTime, startTime);
    } else {
        transfers = db.prepare(
            `SELECT strftime('${fmt}', timestamp / 1000, 'unixepoch') as bucket, SUM(usd_value) as val FROM main.transfers WHERE timestamp >= ? GROUP BY bucket`
        ).all(startTime);
    }

    // LP trend
    let lp;
    if (hasHistLiquidity) {
        lp = db.prepare(
            `SELECT strftime('${fmt}', timestamp / 1000, 'unixepoch') as bucket, SUM(val) as val FROM (
                SELECT timestamp, (CASE WHEN type = 'deposit' THEN usd_value ELSE -usd_value END) as val FROM main.liquidity_events WHERE timestamp >= ?
                UNION ALL
                SELECT timestamp, (CASE WHEN type = 'deposit' THEN usd_value ELSE -usd_value END) as val FROM history.liquidity_events WHERE timestamp >= ?
            ) GROUP BY bucket`
        ).all(startTime, startTime);
    } else {
        lp = db.prepare(
            `SELECT strftime('${fmt}', timestamp / 1000, 'unixepoch') as bucket, SUM(CASE WHEN type = 'deposit' THEN usd_value ELSE -usd_value END) as val FROM main.liquidity_events WHERE timestamp >= ? GROUP BY bucket`
        ).all(startTime);
    }

    // Active accounts trend
    let accounts;
    if (hasHistSwaps && hasHistTransfers) {
        accounts = db.prepare(
            `SELECT strftime('${fmt}', ms/1000, 'unixepoch') as bucket, COUNT(DISTINCT wallet) as val FROM (
                SELECT timestamp as ms, wallet FROM main.swaps WHERE timestamp >= ?
                UNION ALL SELECT timestamp as ms, wallet FROM history.swaps WHERE timestamp >= ?
                UNION ALL SELECT timestamp as ms, from_addr as wallet FROM main.transfers WHERE timestamp >= ?
                UNION ALL SELECT timestamp as ms, from_addr as wallet FROM history.transfers WHERE timestamp >= ?
            ) GROUP BY bucket`
        ).all(startTime, startTime, startTime, startTime);
    } else {
        accounts = db.prepare(
            `SELECT strftime('${fmt}', ms/1000, 'unixepoch') as bucket, COUNT(DISTINCT wallet) as val FROM (
                SELECT timestamp as ms, wallet FROM main.swaps WHERE timestamp >= ?
                UNION ALL SELECT timestamp as ms, from_addr as wallet FROM main.transfers WHERE timestamp >= ?
            ) GROUP BY bucket`
        ).all(startTime, startTime);
    }

    return { swaps, transfers, lp, accounts };
}

// --- TOP TOKENS ---

function getTopTokens(startTime) {
    if (historyHasTable('swaps')) {
        return getStmt('getTopTokens_unified',
            `SELECT symbol, SUM(usd_value) as volume, logo
             FROM (
                 SELECT in_symbol as symbol, in_usd as usd_value, in_logo as logo FROM main.swaps WHERE timestamp >= ?
                 UNION ALL
                 SELECT in_symbol as symbol, in_usd as usd_value, in_logo as logo FROM history.swaps WHERE timestamp >= ?
                 UNION ALL
                 SELECT out_symbol as symbol, out_usd as usd_value, out_logo as logo FROM main.swaps WHERE timestamp >= ?
                 UNION ALL
                 SELECT out_symbol as symbol, out_usd as usd_value, out_logo as logo FROM history.swaps WHERE timestamp >= ?
             )
             GROUP BY symbol ORDER BY volume DESC LIMIT 5`
        ).all(startTime, startTime, startTime, startTime);
    } else {
        return getStmt('getTopTokens_main',
            `SELECT symbol, SUM(usd_value) as volume, logo
             FROM (
                 SELECT in_symbol as symbol, in_usd as usd_value, in_logo as logo FROM main.swaps WHERE timestamp >= ?
                 UNION ALL
                 SELECT out_symbol as symbol, out_usd as usd_value, out_logo as logo FROM main.swaps WHERE timestamp >= ?
             )
             GROUP BY symbol ORDER BY volume DESC LIMIT 5`
        ).all(startTime, startTime);
    }
}

// --- STABLECOIN STATS ---

function getStablecoinStats(startTime) {
    const tokens = ['KUSD', 'XSTUSD', 'TBCD'];
    const results = {};

    for (const symbol of tokens) {
        let row;

        if (historyHasTable('swaps') && historyHasTable('transfers')) {
            row = db.prepare(
                `SELECT
                    (SELECT COALESCE(SUM(in_usd), 0) + COALESCE(SUM(out_usd), 0) FROM (
                         SELECT in_usd, out_usd FROM main.swaps WHERE (in_symbol = ? OR out_symbol = ?) AND timestamp >= ?
                         UNION ALL
                         SELECT in_usd, out_usd FROM history.swaps WHERE (in_symbol = ? OR out_symbol = ?) AND timestamp >= ?
                    )) as swap_vol,
                    (SELECT COALESCE(SUM(usd_value), 0) FROM (
                         SELECT usd_value FROM main.transfers WHERE symbol = ? AND timestamp >= ?
                         UNION ALL
                         SELECT usd_value FROM history.transfers WHERE symbol = ? AND timestamp >= ?
                    )) as transfer_vol`
            ).get(symbol, symbol, startTime, symbol, symbol, startTime, symbol, startTime, symbol, startTime);
        } else {
            row = db.prepare(
                `SELECT
                    (SELECT COALESCE(SUM(in_usd), 0) + COALESCE(SUM(out_usd), 0) FROM main.swaps WHERE (in_symbol = ? OR out_symbol = ?) AND timestamp >= ?) as swap_vol,
                    (SELECT COALESCE(SUM(usd_value), 0) FROM main.transfers WHERE symbol = ? AND timestamp >= ?) as transfer_vol`
            ).get(symbol, symbol, startTime, symbol, startTime);
        }

        results[symbol] = {
            symbol,
            swapVolume: row?.swap_vol || 0,
            transferVolume: row?.transfer_vol || 0
        };
    }

    return results;
}

module.exports = {
    initDB,
    insertTransfer,
    getTransfers,
    getLatestTransfers,
    insertSwap,
    getSwaps,
    getLatestSwaps,
    getCandles,
    getPriceChange,
    getSparkline,
    getTopAccumulators,
    getNetworkStats,
    getMarketTrends,
    getTotalStats,
    insertBridge,
    getFilteredStats,
    insertFee,
    getFeeStats,
    getFeeTrend,
    getWalletBridges,
    getLatestBridges,
    getLpVolume,
    insertLiquidityEvent,
    getTransferVolume,
    getPoolActivity,
    getNetworkTrend,
    getTopTokens,
    getStablecoinStats,
    getLiquidityEvents
};
