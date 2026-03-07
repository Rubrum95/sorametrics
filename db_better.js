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
                    const attachPath = path.resolve(HISTORY_DB_PATH).replace(/\\/g, '/');
                    if (!attachPath.startsWith(path.resolve(__dirname).replace(/\\/g, '/'))) {
                        throw new Error('History DB path outside allowed directory');
                    }
                    db.exec(`ATTACH DATABASE '${attachPath}' AS history`);
                    historyAttached = true;
                    console.log('✅ History DB attached successfully.');

                    // Pragmas for history too
                    db.pragma('history.journal_mode = WAL');
                    db.pragma('history.synchronous = NORMAL');
                    db.pragma('history.cache_size = -64000');
                    db.pragma('history.mmap_size = 268435456');

                    createHistoryIndices();

                    // Add error_msg column to history extrinsics if missing
                    try { db.exec(`ALTER TABLE history.extrinsics ADD COLUMN error_msg TEXT DEFAULT ''`); } catch (e) { /* already exists */ }
                    // Add denom_factor column to history fees if missing
                    try { db.exec(`ALTER TABLE history.fees ADD COLUMN denom_factor TEXT DEFAULT '1'`); } catch (e) { /* already exists */ }
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

    // Migration: add denom_factor column for denomination-aware XOR burn tracking
    try { db.exec(`ALTER TABLE fees ADD COLUMN denom_factor TEXT DEFAULT '1'`); } catch (e) { /* already exists */ }

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

    db.exec(`CREATE TABLE IF NOT EXISTS order_book_events (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        timestamp INTEGER,
        formatted_time TEXT,
        block INTEGER,
        event_type TEXT,
        wallet TEXT,
        order_id TEXT,
        base_asset TEXT,
        quote_asset TEXT,
        side TEXT,
        price TEXT,
        amount TEXT,
        usd_value REAL,
        hash TEXT,
        extrinsic_id TEXT
    )`);

    db.exec(`CREATE TABLE IF NOT EXISTS extrinsics (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        timestamp INTEGER,
        formatted_time TEXT,
        block INTEGER,
        extrinsic_index INTEGER,
        hash TEXT,
        section TEXT,
        method TEXT,
        signer TEXT,
        success INTEGER,
        args_json TEXT
    )`);

    // Add error_msg column if it doesn't exist yet
    try { db.exec(`ALTER TABLE extrinsics ADD COLUMN error_msg TEXT DEFAULT ''`); } catch (e) { /* already exists */ }

    db.exec(`CREATE TABLE IF NOT EXISTS identity_cache (
        address TEXT PRIMARY KEY,
        display TEXT,
        email TEXT,
        web TEXT,
        twitter TEXT,
        discord TEXT,
        updated_at INTEGER
    )`);
    db.exec(`CREATE INDEX IF NOT EXISTS idx_identity_updated ON identity_cache(updated_at)`);

    db.exec(`CREATE TABLE IF NOT EXISTS supply_snapshots (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        timestamp INTEGER,
        symbol TEXT,
        asset_id TEXT,
        total_supply REAL
    )`);

    // Safe migrations
    const safeAlter = (table, col, type) => {
        if (!/^[a-zA-Z_]+$/.test(table) || !/^[a-zA-Z_]+$/.test(col) || !/^[a-zA-Z_ ()]+$/.test(type)) return;
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
        `CREATE INDEX IF NOT EXISTS idx_liquidity_wallet ON liquidity_events(wallet)`,
        `CREATE INDEX IF NOT EXISTS idx_extrinsics_timestamp ON extrinsics(timestamp)`,
        `CREATE INDEX IF NOT EXISTS idx_extrinsics_block ON extrinsics(block)`,
        `CREATE INDEX IF NOT EXISTS idx_extrinsics_section ON extrinsics(section)`,
        `CREATE INDEX IF NOT EXISTS idx_extrinsics_signer ON extrinsics(signer)`,
        `CREATE INDEX IF NOT EXISTS idx_extrinsics_section_timestamp ON extrinsics(section, timestamp)`,
        `CREATE INDEX IF NOT EXISTS idx_orderbook_timestamp ON order_book_events(timestamp)`,
        `CREATE INDEX IF NOT EXISTS idx_orderbook_wallet ON order_book_events(wallet)`,
        `CREATE INDEX IF NOT EXISTS idx_orderbook_event_type ON order_book_events(event_type)`,
        `CREATE INDEX IF NOT EXISTS idx_orderbook_block ON order_book_events(block)`,
        `CREATE INDEX IF NOT EXISTS idx_orderbook_timestamp_type ON order_book_events(timestamp, event_type)`,
        `CREATE INDEX IF NOT EXISTS idx_supply_symbol_timestamp ON supply_snapshots(symbol, timestamp)`
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
        `CREATE INDEX IF NOT EXISTS history.idx_h_liquidity_wallet ON liquidity_events(wallet)`,
        `CREATE INDEX IF NOT EXISTS history.idx_h_extrinsics_timestamp ON extrinsics(timestamp)`,
        `CREATE INDEX IF NOT EXISTS history.idx_h_extrinsics_block ON extrinsics(block)`,
        `CREATE INDEX IF NOT EXISTS history.idx_h_extrinsics_section ON extrinsics(section)`,
        `CREATE INDEX IF NOT EXISTS history.idx_h_extrinsics_signer ON extrinsics(signer)`,
        `CREATE INDEX IF NOT EXISTS history.idx_h_extrinsics_section_timestamp ON extrinsics(section, timestamp)`,
        `CREATE INDEX IF NOT EXISTS history.idx_h_orderbook_timestamp ON order_book_events(timestamp)`,
        `CREATE INDEX IF NOT EXISTS history.idx_h_orderbook_wallet ON order_book_events(wallet)`,
        `CREATE INDEX IF NOT EXISTS history.idx_h_orderbook_event_type ON order_book_events(event_type)`,
        `CREATE INDEX IF NOT EXISTS history.idx_h_orderbook_block ON order_book_events(block)`,
        `CREATE INDEX IF NOT EXISTS history.idx_h_orderbook_timestamp_type ON order_book_events(timestamp, event_type)`
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

function extrinsicDedupKey(r) {
    return `${r.block}_${r.extrinsic_index}`;
}

function mapExtrinsics(rows) {
    return rows.map(r => ({
        time: formatTimestamp(r.timestamp) || r.formatted_time,
        block: r.block,
        extrinsic_index: r.extrinsic_index,
        extrinsic_id: `${r.block}-${r.extrinsic_index}`,
        hash: r.hash,
        section: r.section,
        method: r.method,
        signer: r.signer,
        success: r.success,
        args_json: r.args_json,
        error_msg: r.error_msg || ''
    }));
}

function orderBookDedupKey(r) {
    return `${r.block}-${r.event_type}-${r.order_id || r.wallet}`;
}

function mapOrderBook(rows) {
    return rows.map(r => ({
        time: formatTimestamp(r.timestamp) || r.formatted_time,
        block: r.block,
        event_type: r.event_type,
        wallet: r.wallet,
        order_id: r.order_id,
        base_asset: r.base_asset,
        quote_asset: r.quote_asset,
        side: r.side,
        price: r.price,
        amount: r.amount,
        usd_value: r.usd_value ? r.usd_value.toFixed(2) : '0.00',
        hash: r.hash,
        extrinsic_id: r.extrinsic_id
    }));
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
            insertStmts.fee = db.prepare(`INSERT INTO fees (timestamp, block, type, amount, usd_value, denom_factor) VALUES (?, ?, ?, ?, ?, ?)`);
        }
        insertStmts.fee.run(Date.now(), f.block, f.type, f.amount, f.usdValue, f.denomFactor || '1');
    } catch (e) {
        console.error('Error insertFee:', e.message);
    }
}

// Fix legacy fees that have denom_factor='1' (from before we tracked it)
// Updates both main DB and history DB (attached as 'history')
function fixFeeDenomFactor(correctFactor) {
    let total = 0;
    try {
        const r1 = db.prepare(
            `UPDATE main.fees SET denom_factor = ? WHERE denom_factor = '1' OR denom_factor IS NULL`
        ).run(correctFactor);
        total += r1.changes;
    } catch (e) { /* ignore */ }
    try {
        const r2 = db.prepare(
            `UPDATE history.fees SET denom_factor = ? WHERE denom_factor = '1' OR denom_factor IS NULL`
        ).run(correctFactor);
        total += r2.changes;
    } catch (e) { /* history DB may not have fees table or denom_factor column */ }
    return total;
}

function insertExtrinsic(e) {
    try {
        if (!insertStmts.extrinsic) {
            insertStmts.extrinsic = db.prepare(
                `INSERT INTO extrinsics (timestamp, formatted_time, block, extrinsic_index, hash, section, method, signer, success, args_json, error_msg)
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
            );
        }
        insertStmts.extrinsic.run(
            e.timestamp || Date.now(), e.formatted_time || '', e.block || 0, e.extrinsic_index || 0,
            e.hash || '', e.section || '', e.method || '',
            e.signer || 'System', e.success ? 1 : 0, e.args_json || '{}', e.error_msg || ''
        );
    } catch (err) {
        console.error('Error insertExtrinsic:', err.message);
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

function insertOrderBookEvent(e) {
    try {
        if (!insertStmts.orderBook) {
            insertStmts.orderBook = db.prepare(
                `INSERT INTO order_book_events (timestamp, formatted_time, block, event_type, wallet, order_id, base_asset, quote_asset, side, price, amount, usd_value, hash, extrinsic_id)
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
            );
        }
        insertStmts.orderBook.run(
            e.timestamp || Date.now(), e.formatted_time || '', e.block || 0,
            e.event_type || '', e.wallet || '', e.order_id || '',
            e.base_asset || '', e.quote_asset || '', e.side || '',
            e.price || '', e.amount || '', e.usd_value || 0,
            e.hash || '', e.extrinsic_id || ''
        );
    } catch (err) {
        console.error('Error insertOrderBookEvent:', err.message);
    }
}

// --- EXTRINSICS READ ---

function getLatestExtrinsics(page = 1, limit = 25, section = null, timestamp = null, block = null, success = null, method = null) {
    const offset = (page - 1) * limit;
    const cols = `id, timestamp, formatted_time, block, extrinsic_index, hash, section, method, signer, success, args_json, error_msg`;

    let conditions = [];
    let params = [];

    // Filter out noisy system extrinsics
    conditions.push(`NOT (section = 'timestamp' AND method = 'set')`);

    if (section) {
        conditions.push(`section = ?`);
        params.push(section);
    }
    if (method) {
        conditions.push(`method LIKE ?`);
        params.push('%' + method + '%');
    }
    if (timestamp) {
        conditions.push(`timestamp <= ?`);
        params.push(timestamp);
    }
    if (block) {
        conditions.push(`block = ?`);
        params.push(block);
    }
    if (success !== null && success !== undefined) {
        conditions.push(`success = ?`);
        params.push(parseInt(success));
    }

    const where = conditions.length > 0 ? ' WHERE ' + conditions.join(' AND ') : '';

    let total, rows;

    if (historyHasTable('extrinsics')) {
        const countSql = `SELECT (SELECT COUNT(*) FROM main.extrinsics${where}) + (SELECT COUNT(*) FROM history.extrinsics${where}) as total`;
        total = db.prepare(countSql).get(...params, ...params)?.total || 0;

        const dataSql = `SELECT ${cols} FROM main.extrinsics${where} UNION ALL SELECT ${cols} FROM history.extrinsics${where} ORDER BY timestamp DESC LIMIT ? OFFSET ?`;
        rows = db.prepare(dataSql).all(...params, ...params, limit, offset);
    } else {
        const countSql = `SELECT COUNT(*) as total FROM main.extrinsics${where}`;
        total = db.prepare(countSql).get(...params)?.total || 0;

        const dataSql = `SELECT ${cols} FROM main.extrinsics${where} ORDER BY timestamp DESC LIMIT ? OFFSET ?`;
        rows = db.prepare(dataSql).all(...params, limit, offset);
    }

    const unique = dedup(rows, extrinsicDedupKey);
    const totalPages = Math.ceil(total / limit);
    return { data: mapExtrinsics(unique), total, page, totalPages };
}

function getExtrinsicSections() {
    try {
        let sections;
        if (historyHasTable('extrinsics')) {
            sections = db.prepare(
                `SELECT DISTINCT section FROM (SELECT section FROM main.extrinsics UNION SELECT section FROM history.extrinsics) ORDER BY section ASC`
            ).all();
        } else {
            sections = db.prepare(
                `SELECT DISTINCT section FROM main.extrinsics ORDER BY section ASC`
            ).all();
        }
        return sections.map(r => r.section).filter(s => s && s !== 'timestamp');
    } catch (e) {
        return [];
    }
}

function getExtrinsicsByAddress(address, page = 1, limit = 25) {
    const offset = (page - 1) * limit;
    const cols = `id, timestamp, formatted_time, block, extrinsic_index, hash, section, method, signer, success, args_json`;

    let total, rows;

    if (historyHasTable('extrinsics')) {
        total = getStmt('getExtrinsicsByAddr_count',
            `SELECT (SELECT COUNT(*) FROM main.extrinsics WHERE signer = ?) + (SELECT COUNT(*) FROM history.extrinsics WHERE signer = ?) as total`
        ).get(address, address).total || 0;

        rows = getStmt('getExtrinsicsByAddr_data',
            `SELECT ${cols} FROM main.extrinsics WHERE signer = ?
             UNION ALL
             SELECT ${cols} FROM history.extrinsics WHERE signer = ?
             ORDER BY timestamp DESC LIMIT ? OFFSET ?`
        ).all(address, address, limit, offset);
    } else {
        total = getStmt('getExtrinsicsByAddr_count_main',
            `SELECT COUNT(*) as total FROM main.extrinsics WHERE signer = ?`
        ).get(address).total || 0;

        rows = getStmt('getExtrinsicsByAddr_data_main',
            `SELECT ${cols} FROM main.extrinsics WHERE signer = ? ORDER BY timestamp DESC LIMIT ? OFFSET ?`
        ).all(address, limit, offset);
    }

    const unique = dedup(rows, extrinsicDedupKey);
    const totalPages = Math.ceil(total / limit);
    return { data: mapExtrinsics(unique), total, page, totalPages };
}

// --- IDENTITY CACHE ---

function upsertIdentity(address, display, email, web, twitter, discord) {
    try {
        if (!insertStmts.identityUpsert) {
            insertStmts.identityUpsert = db.prepare(
                `INSERT OR REPLACE INTO identity_cache (address, display, email, web, twitter, discord, updated_at)
                 VALUES (?, ?, ?, ?, ?, ?, ?)`
            );
        }
        insertStmts.identityUpsert.run(address, display || null, email || null, web || null, twitter || null, discord || null, Date.now());
    } catch (e) {
        console.error('Error upsertIdentity:', e.message);
    }
}

function upsertIdentityBatch(identities) {
    const upsert = getStmt('identityBatchUpsert',
        `INSERT OR REPLACE INTO identity_cache (address, display, email, web, twitter, discord, updated_at)
         VALUES (?, ?, ?, ?, ?, ?, ?)`
    );
    const now = Date.now();
    const runBatch = db.transaction((items) => {
        for (const i of items) {
            upsert.run(i.address, i.display || null, i.email || null, i.web || null, i.twitter || null, i.discord || null, now);
        }
    });
    try {
        runBatch(identities);
    } catch (e) {
        console.error('Error upsertIdentityBatch:', e.message);
    }
}

function getIdentities(addresses) {
    if (!addresses || addresses.length === 0) return {};
    const placeholders = addresses.map(() => '?').join(',');
    const rows = db.prepare(`SELECT address, display, email, web, twitter, discord, updated_at FROM identity_cache WHERE address IN (${placeholders})`).all(...addresses);
    const result = {};
    for (const row of rows) {
        result[row.address] = row;
    }
    return result;
}

function getAllCachedIdentities() {
    return getStmt('allIdentities', `SELECT address, display FROM identity_cache WHERE display IS NOT NULL`).all();
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
    const cols = `in_symbol, out_symbol, in_amount, out_amount, in_usd, out_usd, timestamp`;

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

function getFeeStats(startTime, currentDenomFactor) {
    // Normalize amounts across denomination eras:
    // Each fee's amount is in its era's packaged units. To sum correctly,
    // divide by 10^(len(currentDenom) - len(feeDenom)) to normalize to current scale.
    // When all fees share the same denom_factor, this is a no-op (POWER(10,0)=1).
    const denomLen = currentDenomFactor ? currentDenomFactor.length : 1;
    if (historyHasTable('fees')) {
        return db.prepare(
            `SELECT type,
                    SUM(amount / POWER(10, ? - LENGTH(COALESCE(denom_factor, '1')))) as total_xor,
                    SUM(usd_value) as total_usd
             FROM (
                 SELECT type, amount, usd_value, denom_factor FROM main.fees WHERE timestamp >= ?
                 UNION ALL
                 SELECT type, amount, usd_value, denom_factor FROM history.fees WHERE timestamp >= ?
             )
             GROUP BY type`
        ).all(denomLen, startTime, startTime);
    } else {
        return db.prepare(
            `SELECT type,
                    SUM(amount / POWER(10, ? - LENGTH(COALESCE(denom_factor, '1')))) as total_xor,
                    SUM(usd_value) as total_usd
             FROM main.fees WHERE timestamp >= ?
             GROUP BY type`
        ).all(denomLen, startTime);
    }
}

// Query fees from main DB only (skip history DB which may have different scale data)
function getFeeStatsMainOnly(startTime, currentDenomFactor) {
    const denomLen = currentDenomFactor ? currentDenomFactor.length : 1;
    return db.prepare(
        `SELECT type,
                SUM(amount / POWER(10, ? - LENGTH(COALESCE(denom_factor, '1')))) as total_xor,
                SUM(usd_value) as total_usd
         FROM main.fees WHERE timestamp >= ?
         GROUP BY type`
    ).all(denomLen, startTime);
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

// --- ORDER BOOK ---

function getLatestOrderBookEvents(page = 1, limit = 25, type = null, timestamp = null) {
    const offset = (page - 1) * limit;
    const cols = `id, timestamp, formatted_time, block, event_type, wallet, order_id, base_asset, quote_asset, side, price, amount, usd_value, hash, extrinsic_id`;

    let conditions = [];
    let params = [];

    if (type) {
        conditions.push(`event_type = ?`);
        params.push(type);
    }
    if (timestamp) {
        conditions.push(`timestamp <= ?`);
        params.push(timestamp);
    }

    const where = conditions.length > 0 ? ' WHERE ' + conditions.join(' AND ') : '';

    let total, rows;

    if (historyHasTable('order_book_events')) {
        const countSql = `SELECT (SELECT COUNT(*) FROM main.order_book_events${where}) + (SELECT COUNT(*) FROM history.order_book_events${where}) as total`;
        total = db.prepare(countSql).get(...params, ...params)?.total || 0;

        const dataSql = `SELECT ${cols} FROM main.order_book_events${where} UNION ALL SELECT ${cols} FROM history.order_book_events${where} ORDER BY timestamp DESC LIMIT ? OFFSET ?`;
        rows = db.prepare(dataSql).all(...params, ...params, limit, offset);
    } else {
        const countSql = `SELECT COUNT(*) as total FROM main.order_book_events${where}`;
        total = db.prepare(countSql).get(...params)?.total || 0;

        const dataSql = `SELECT ${cols} FROM main.order_book_events${where} ORDER BY timestamp DESC LIMIT ? OFFSET ?`;
        rows = db.prepare(dataSql).all(...params, limit, offset);
    }

    const unique = dedup(rows, orderBookDedupKey);
    const totalPages = Math.ceil(total / limit);
    return { data: mapOrderBook(unique), total, totalPages, page };
}

function getOrderBookByAddress(address, page = 1, limit = 25) {
    const offset = (page - 1) * limit;
    const cols = `id, timestamp, formatted_time, block, event_type, wallet, order_id, base_asset, quote_asset, side, price, amount, usd_value, hash, extrinsic_id`;

    let total, rows;

    if (historyHasTable('order_book_events')) {
        total = getStmt('getOrderBookByAddr_count',
            `SELECT (SELECT COUNT(*) FROM main.order_book_events WHERE wallet = ?) + (SELECT COUNT(*) FROM history.order_book_events WHERE wallet = ?) as total`
        ).get(address, address).total || 0;

        rows = getStmt('getOrderBookByAddr_data',
            `SELECT ${cols} FROM main.order_book_events WHERE wallet = ?
             UNION ALL
             SELECT ${cols} FROM history.order_book_events WHERE wallet = ?
             ORDER BY timestamp DESC LIMIT ? OFFSET ?`
        ).all(address, address, limit, offset);
    } else {
        total = getStmt('getOrderBookByAddr_count_main',
            `SELECT COUNT(*) as total FROM main.order_book_events WHERE wallet = ?`
        ).get(address).total || 0;

        rows = getStmt('getOrderBookByAddr_data_main',
            `SELECT ${cols} FROM main.order_book_events WHERE wallet = ? ORDER BY timestamp DESC LIMIT ? OFFSET ?`
        ).all(address, limit, offset);
    }

    const unique = dedup(rows, orderBookDedupKey);
    const totalPages = Math.ceil(total / limit);
    return { data: mapOrderBook(unique), total, totalPages, page };
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

// --- SUPPLY SNAPSHOTS (BURN TRACKER) ---

function insertSupplySnapshot(symbol, assetId, totalSupply) {
    try {
        if (!insertStmts.supply) {
            insertStmts.supply = db.prepare(`INSERT INTO supply_snapshots (timestamp, symbol, asset_id, total_supply) VALUES (?, ?, ?, ?)`);
        }
        insertStmts.supply.run(Date.now(), symbol, assetId, totalSupply);
    } catch (e) {
        console.error('Error insertSupplySnapshot:', e.message);
    }
}

function getSupplyHistory(symbol, startTime) {
    try {
        if (historyHasTable('supply_snapshots')) {
            return getStmt('getSupplyHistory_unified',
                `SELECT timestamp, total_supply FROM (
                    SELECT timestamp, total_supply FROM main.supply_snapshots WHERE symbol = ? AND timestamp >= ?
                    UNION ALL
                    SELECT timestamp, total_supply FROM history.supply_snapshots WHERE symbol = ? AND timestamp >= ?
                ) ORDER BY timestamp ASC`
            ).all(symbol, startTime, symbol, startTime);
        }
        return getStmt('getSupplyHistory_main',
            `SELECT timestamp, total_supply FROM main.supply_snapshots WHERE symbol = ? AND timestamp >= ? ORDER BY timestamp ASC`
        ).all(symbol, startTime);
    } catch (e) {
        console.error('Error getSupplyHistory:', e.message);
        return [];
    }
}

function getLatestSupplySnapshot(symbol) {
    try {
        return getStmt('getLatestSupply',
            `SELECT total_supply, timestamp FROM main.supply_snapshots WHERE symbol = ? ORDER BY timestamp DESC LIMIT 1`
        ).get(symbol) || null;
    } catch (e) {
        return null;
    }
}

function purgeSupplySnapshotsForSymbol(symbol) {
    try {
        const result = db.prepare('DELETE FROM supply_snapshots WHERE symbol = ?').run(symbol);
        return result.changes;
    } catch (e) {
        console.error('Error purging snapshots for', symbol, ':', e.message);
        return 0;
    }
}

function getBurnStats(symbol, startTime) {
    try {
        const sql = historyHasTable('supply_snapshots')
            ? `SELECT total_supply, timestamp FROM (
                SELECT total_supply, timestamp FROM main.supply_snapshots WHERE symbol = ? AND timestamp >= ?
                UNION ALL
                SELECT total_supply, timestamp FROM history.supply_snapshots WHERE symbol = ? AND timestamp >= ?
            ) ORDER BY timestamp ASC`
            : `SELECT total_supply, timestamp FROM main.supply_snapshots WHERE symbol = ? AND timestamp >= ? ORDER BY timestamp ASC`;

        const params = historyHasTable('supply_snapshots')
            ? [symbol, startTime, symbol, startTime]
            : [symbol, startTime];

        const rows = db.prepare(sql).all(...params);
        if (rows.length < 2) return { totalBurned: 0, startSupply: 0, endSupply: 0 };

        const first = rows[0];
        const last = rows[rows.length - 1];
        const burned = first.total_supply - last.total_supply;

        return {
            totalBurned: Math.max(burned, 0),
            startSupply: first.total_supply,
            endSupply: last.total_supply,
            startTime: first.timestamp,
            endTime: last.timestamp
        };
    } catch (e) {
        console.error('Error getBurnStats:', e.message);
        return { totalBurned: 0, startSupply: 0, endSupply: 0 };
    }
}

// Lookup USD value for an extrinsic by extrinsic_id (cross-table search)
function lookupExtrinsicUsdValue(extrinsicId) {
    if (!extrinsicId) return null;
    try {
        // 1. Transfers
        const trSql = historyHasTable('transfers')
            ? `SELECT usd_value FROM (SELECT usd_value FROM main.transfers WHERE extrinsic_id = ? UNION ALL SELECT usd_value FROM history.transfers WHERE extrinsic_id = ?) LIMIT 1`
            : `SELECT usd_value FROM main.transfers WHERE extrinsic_id = ? LIMIT 1`;
        const trParams = historyHasTable('transfers') ? [extrinsicId, extrinsicId] : [extrinsicId];
        const tr = db.prepare(trSql).get(...trParams);
        if (tr && tr.usd_value) return { usd_value: tr.usd_value, source: 'transfer' };

        // 2. Swaps (use in_usd as main value)
        const swSql = historyHasTable('swaps')
            ? `SELECT in_usd, out_usd FROM (SELECT in_usd, out_usd FROM main.swaps WHERE extrinsic_id = ? UNION ALL SELECT in_usd, out_usd FROM history.swaps WHERE extrinsic_id = ?) LIMIT 1`
            : `SELECT in_usd, out_usd FROM main.swaps WHERE extrinsic_id = ? LIMIT 1`;
        const swParams = historyHasTable('swaps') ? [extrinsicId, extrinsicId] : [extrinsicId];
        const sw = db.prepare(swSql).get(...swParams);
        if (sw && (sw.in_usd || sw.out_usd)) return { usd_value: sw.in_usd || sw.out_usd, source: 'swap' };

        // 3. Bridges
        const brSql = historyHasTable('bridges')
            ? `SELECT usd_value FROM (SELECT usd_value FROM main.bridges WHERE extrinsic_id = ? UNION ALL SELECT usd_value FROM history.bridges WHERE extrinsic_id = ?) LIMIT 1`
            : `SELECT usd_value FROM main.bridges WHERE extrinsic_id = ? LIMIT 1`;
        const brParams = historyHasTable('bridges') ? [extrinsicId, extrinsicId] : [extrinsicId];
        const br = db.prepare(brSql).get(...brParams);
        if (br && br.usd_value) return { usd_value: br.usd_value, source: 'bridge' };

        // 4. Liquidity events
        const lqSql = historyHasTable('liquidity_events')
            ? `SELECT usd_value FROM (SELECT usd_value FROM main.liquidity_events WHERE extrinsic_id = ? UNION ALL SELECT usd_value FROM history.liquidity_events WHERE extrinsic_id = ?) LIMIT 1`
            : `SELECT usd_value FROM main.liquidity_events WHERE extrinsic_id = ? LIMIT 1`;
        const lqParams = historyHasTable('liquidity_events') ? [extrinsicId, extrinsicId] : [extrinsicId];
        const lq = db.prepare(lqSql).get(...lqParams);
        if (lq && lq.usd_value) return { usd_value: lq.usd_value, source: 'liquidity' };

        // 5. Order book
        const obSql = historyHasTable('order_book_events')
            ? `SELECT usd_value FROM (SELECT usd_value FROM main.order_book_events WHERE extrinsic_id = ? UNION ALL SELECT usd_value FROM history.order_book_events WHERE extrinsic_id = ?) LIMIT 1`
            : `SELECT usd_value FROM main.order_book_events WHERE extrinsic_id = ? LIMIT 1`;
        const obParams = historyHasTable('order_book_events') ? [extrinsicId, extrinsicId] : [extrinsicId];
        const ob = db.prepare(obSql).get(...obParams);
        if (ob && ob.usd_value) return { usd_value: ob.usd_value, source: 'orderbook' };

        // 6. Fees
        const feSql = historyHasTable('fees')
            ? `SELECT total_usd FROM (SELECT total_usd FROM main.fees WHERE extrinsic_id = ? UNION ALL SELECT total_usd FROM history.fees WHERE extrinsic_id = ?) LIMIT 1`
            : `SELECT total_usd FROM main.fees WHERE extrinsic_id = ? LIMIT 1`;
        const feParams = historyHasTable('fees') ? [extrinsicId, extrinsicId] : [extrinsicId];
        const fe = db.prepare(feSql).get(...feParams);
        if (fe && fe.total_usd) return { usd_value: fe.total_usd, source: 'fee' };

        return null;
    } catch (e) {
        return null;
    }
}

// Global search: find entity by query (wallet address, tx hash, extrinsic_id, block)
function globalSearch(query) {
    if (!query || query.length < 3) return { type: null };
    query = query.trim();

    // Extrinsic ID pattern: number-number (e.g., 25135395-1)
    if (/^\d+-\d+$/.test(query)) {
        try {
            const sql = historyHasTable('extrinsics')
                ? `SELECT extrinsic_id, section, method, block FROM (SELECT extrinsic_id, section, method, block FROM main.extrinsics WHERE extrinsic_id = ? UNION ALL SELECT extrinsic_id, section, method, block FROM history.extrinsics WHERE extrinsic_id = ?) LIMIT 1`
                : `SELECT extrinsic_id, section, method, block FROM main.extrinsics WHERE extrinsic_id = ? LIMIT 1`;
            const params = historyHasTable('extrinsics') ? [query, query] : [query];
            const row = db.prepare(sql).get(...params);
            if (row) return { type: 'extrinsic', data: row };
        } catch (e) { /* continue */ }
    }

    // Block number (pure digits)
    if (/^\d+$/.test(query)) {
        const block = parseInt(query);
        if (block > 0 && block < 100000000) {
            return { type: 'block', data: { block } };
        }
    }

    // Transaction hash (0x...)
    if (/^0x[a-fA-F0-9]{64}$/.test(query)) {
        // Search in extrinsics table
        try {
            const sql = historyHasTable('extrinsics')
                ? `SELECT extrinsic_id, section, method, block, signer FROM (SELECT extrinsic_id, section, method, block, signer FROM main.extrinsics WHERE hash = ? UNION ALL SELECT extrinsic_id, section, method, block, signer FROM history.extrinsics WHERE hash = ?) LIMIT 1`
                : `SELECT extrinsic_id, section, method, block, signer FROM main.extrinsics WHERE hash = ? LIMIT 1`;
            const params = historyHasTable('extrinsics') ? [query, query] : [query];
            const row = db.prepare(sql).get(...params);
            if (row) return { type: 'extrinsic', data: row };
        } catch (e) { /* continue */ }
        return { type: 'hash_not_found', data: { hash: query } };
    }

    // Wallet address (cn... , 48+ chars)
    if (/^cn[a-zA-Z0-9]{46,}$/.test(query)) {
        return { type: 'wallet', data: { address: query } };
    }

    return { type: null };
}

// Total swap volume in USD for a given time range (for PSWAP fee calculations)
function getSwapVolumeUsd(startTime) {
    try {
        if (historyHasTable('swaps')) {
            const row = getStmt('getSwapVolumeUsd_unified',
                `SELECT COALESCE(SUM(vol), 0) as total FROM (
                    SELECT COALESCE(in_usd, 0) as vol FROM main.swaps WHERE timestamp >= ?
                    UNION ALL
                    SELECT COALESCE(in_usd, 0) as vol FROM history.swaps WHERE timestamp >= ?
                )`
            ).get(startTime, startTime);
            return row?.total || 0;
        }
        const row = getStmt('getSwapVolumeUsd_main',
            `SELECT COALESCE(SUM(in_usd), 0) as total FROM main.swaps WHERE timestamp >= ?`
        ).get(startTime);
        return row?.total || 0;
    } catch (e) {
        console.error('Error getSwapVolumeUsd:', e.message);
        return 0;
    }
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
    fixFeeDenomFactor,
    getFeeStats,
    getFeeStatsMainOnly,
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
    getLiquidityEvents,
    insertExtrinsic,
    getLatestExtrinsics,
    getExtrinsicSections,
    getExtrinsicsByAddress,
    insertOrderBookEvent,
    getLatestOrderBookEvents,
    getOrderBookByAddress,
    upsertIdentity,
    upsertIdentityBatch,
    getIdentities,
    getAllCachedIdentities,
    insertSupplySnapshot,
    getSupplyHistory,
    getLatestSupplySnapshot,
    getBurnStats,
    purgeSupplySnapshotsForSymbol,
    lookupExtrinsicUsdValue,
    globalSearch,
    getSwapVolumeUsd
};
