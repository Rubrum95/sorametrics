const sqlite3 = require('sqlite3').verbose();
const path = require('path');

const DB_PATH = path.join(__dirname, 'database_30d.db');
const HISTORY_DB_PATH = path.join(__dirname, 'database.db'); // Usar database.db como history
let db;
let historyDb;

// --- DB INITIALIZATION ---
function initDB() {
    return new Promise((resolve, reject) => {
        db = new sqlite3.Database(DB_PATH, async (err) => {
            if (err) return reject(err);
            console.log('💾 SQLite conectado (live).');
            try {
                await createTables();

                // Attach History DB for Unified Queries
                const attachSql = `ATTACH DATABASE '${HISTORY_DB_PATH.replace(/\\/g, '/')}' AS history`;
                console.log('🔗 Attaching history DB:', attachSql);

                db.run(attachSql, (err) => {
                    if (err) {
                        console.warn('⚠️ Could not attach history DB:', err.message);
                    } else {
                        console.log('✅ History DB attached successfully.');
                    }
                    resolve();
                });
            } catch (e) {
                reject(e);
            }
        });
    });
}

function initHistoryDB() {
    return new Promise((resolve, reject) => {
        const fs = require('fs');
        if (!fs.existsSync(HISTORY_DB_PATH)) {
            return reject(new Error('History database does not exist yet'));
        }
        historyDb = new sqlite3.Database(HISTORY_DB_PATH, sqlite3.OPEN_READONLY, (err) => {
            if (err) return reject(err);
            resolve();
        });
    });
}

function run(sql, params = []) {
    return new Promise((resolve, reject) => {
        db.run(sql, params, function (err) {
            if (err) reject(err);
            else resolve(this);
        });
    });
}

async function createTables() {
    // 1. Crear Tablas Básicas
    await run(`CREATE TABLE IF NOT EXISTS transfers (
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

    await run(`CREATE TABLE IF NOT EXISTS swaps (
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

    await run(`CREATE TABLE IF NOT EXISTS bridges (
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

    await run(`CREATE TABLE IF NOT EXISTS fees (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        timestamp INTEGER,
        block INTEGER,
        type TEXT,
        amount REAL,
        usd_value REAL
    )`);

    await run(`CREATE TABLE IF NOT EXISTS liquidity_events (
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

    // 2. Migraciones automáticas (Safe Alter)
    const safeAlter = async (table, col, type) => {
        try {
            await run(`ALTER TABLE ${table} ADD COLUMN ${col} ${type}`);
        } catch (e) {
            // Ignorar error si la columna ya existe
        }
    };

    // Asegurar columnas críticas que podrían faltar en DBs antiguas
    await safeAlter('transfers', 'timestamp', 'INTEGER');
    await safeAlter('transfers', 'block', 'INTEGER');
    await safeAlter('transfers', 'hash', 'TEXT');
    await safeAlter('transfers', 'extrinsic_id', 'TEXT');
    await safeAlter('swaps', 'timestamp', 'INTEGER');
    await safeAlter('swaps', 'block', 'INTEGER');
    await safeAlter('swaps', 'in_logo', 'TEXT');
    await safeAlter('swaps', 'out_logo', 'TEXT');
    await safeAlter('swaps', 'hash', 'TEXT');
    await safeAlter('swaps', 'extrinsic_id', 'TEXT');
    await safeAlter('bridges', 'hash', 'TEXT');
    await safeAlter('bridges', 'extrinsic_id', 'TEXT');
    await safeAlter('bridges', 'symbol', 'TEXT');
    await safeAlter('bridges', 'logo', 'TEXT');
    await safeAlter('liquidity_events', 'hash', 'TEXT');
    await safeAlter('liquidity_events', 'extrinsic_id', 'TEXT');

    // 3. Índices
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
        `CREATE INDEX IF NOT EXISTS idx_liquidity_timestamp ON liquidity_events(timestamp)`
    ];

    for (const idx of indices) {
        await run(idx);
    }
}

function insertBridge(b) {
    const sql = `INSERT INTO bridges (timestamp, block, network, direction, sender, recipient, asset_id, symbol, logo, amount, usd_value, hash, extrinsic_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`;
    const params = [Date.now(), b.block, b.network, b.direction, b.sender, b.recipient, b.assetId || b.asset_id, b.symbol || 'UNK', b.logo || '', b.amount, b.usdValue || b.usd_value, b.hash || '', b.extrinsic_id || ''];
    db.run(sql, params, (err) => {
        if (err) console.error('Error insertBridge:', err.message);
    });
}

function insertFee(f) {
    const sql = `INSERT INTO fees (timestamp, block, type, amount, usd_value) VALUES (?, ?, ?, ?, ?)`;
    const params = [Date.now(), f.block, f.type, f.amount, f.usdValue];
    db.run(sql, params, (err) => {
        if (err) console.error('Error insertFee:', err.message);
    });
}

// --- TRANSFERS METHODS ---
function insertTransfer(t) {
    const sql = `INSERT INTO transfers (timestamp, formatted_time, from_addr, to_addr, amount, symbol, logo, usd_value, asset_id, block, hash, extrinsic_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`;
    const params = [Date.now(), t.time, t.from, t.to, t.amount, t.symbol, t.logo, parseFloat(t.usdValue), t.assetId, t.block || 0, t.hash || '', t.extrinsic_id || ''];
    db.run(sql, params, (err) => {
        if (err) console.error('Error insertTransfer:', err.message);
    });
}

function getTransfers(address, page = 1, limit = 25) {
    return new Promise((resolve, reject) => {
        const offset = (page - 1) * limit;
        const countSql = `SELECT (SELECT COUNT(*) FROM main.transfers WHERE from_addr = ? OR to_addr = ?) + (SELECT COUNT(*) FROM history.transfers WHERE from_addr = ? OR to_addr = ?) as total`;

        const cols = `id, timestamp, formatted_time, from_addr, to_addr, amount, symbol, logo, usd_value, asset_id, block, hash, extrinsic_id`;
        const sql = `
            SELECT ${cols} FROM main.transfers WHERE from_addr = ? OR to_addr = ?
            UNION ALL
            SELECT ${cols} FROM history.transfers WHERE from_addr = ? OR to_addr = ?
            ORDER BY timestamp DESC LIMIT ? OFFSET ?
        `;

        const paramsCount = [address, address, address, address];
        const paramsData = [address, address, address, address, limit, offset];

        db.get(countSql, paramsCount, (err, row) => {
            // Handle missing table gracefully
            if (err && err.message.includes('no such table')) {
                db.get(`SELECT COUNT(*) as count FROM main.transfers WHERE from_addr = ? OR to_addr = ?`, [address, address], (e, r) => {
                    if (e) return reject(e);
                    const total = r?.count || 0;
                    const totalPages = Math.ceil(total / limit);
                    db.all(`SELECT * FROM main.transfers WHERE from_addr = ? OR to_addr = ? ORDER BY timestamp DESC LIMIT ? OFFSET ?`, [address, address, limit, offset], (e2, rows) => {
                        if (e2) return reject(e2);
                        resolve({ data: mapTransfers(rows), total, page, totalPages });
                    });
                });
                return;
            } else if (err) return reject(err);

            const total = row.total || 0;
            const totalPages = Math.ceil(total / limit);

            db.all(sql, paramsData, (err, rows) => {
                if (err) return reject(err);

                // Deduplicate Unified Result
                const seen = new Set();
                const unique = rows.filter(r => {
                    const amt = String(r.amount).replace(/,/g, '');
                    const key = `${r.block}_${r.from_addr}_${r.to_addr}_${amt}_${r.asset_id}`;
                    if (seen.has(key)) return false;
                    seen.add(key);
                    return true;
                });

                resolve({
                    data: mapTransfers(unique),
                    total: total, // Approximate total combining both
                    page: page,
                    totalPages: totalPages
                });
            });
        });
    });
}



function getLatestTransfers(page = 1, limit = 25, filter = null, timestamp = null) {
    return new Promise(async (resolve, reject) => {
        const offset = (page - 1) * limit;

        let whereMain = '';
        let whereHistory = '';
        let paramsMain = [];
        let paramsHistory = [];

        if (filter) {
            const f = `%${filter.toUpperCase()}%`;
            whereMain += ` WHERE (symbol LIKE ? OR from_addr LIKE ? OR to_addr LIKE ?)`;
            whereHistory += ` WHERE (symbol LIKE ? OR from_addr LIKE ? OR to_addr LIKE ?)`;
            paramsMain.push(f, f, f);
            paramsHistory.push(f, f, f);
        }

        if (timestamp) {
            const prefixMain = whereMain ? ' AND ' : ' WHERE ';
            const prefixHistory = whereHistory ? ' AND ' : ' WHERE ';
            whereMain += `${prefixMain}timestamp <= ?`;
            whereHistory += `${prefixHistory}timestamp <= ?`;
            paramsMain.push(timestamp);
            paramsHistory.push(timestamp);
        }

        const countSql = `
            SELECT 
                (SELECT COUNT(*) FROM main.transfers ${whereMain}) + 
                (SELECT COUNT(*) FROM history.transfers ${whereHistory}) as total
        `;

        // Select specific columns for safety and consistency
        const cols = `id, timestamp, formatted_time, from_addr, to_addr, amount, symbol, logo, usd_value, asset_id, block, hash, extrinsic_id`;

        const dataSql = `
            SELECT ${cols} FROM main.transfers ${whereMain}
            UNION ALL
            SELECT ${cols} FROM history.transfers ${whereHistory}
            ORDER BY timestamp DESC
            LIMIT ? OFFSET ?
        `;

        const dataParams = [...paramsMain, ...paramsHistory, limit, offset];

        try {
            const total = await new Promise((res, rej) => {
                db.get(countSql, [...paramsMain, ...paramsHistory], (err, row) => {
                    if (err && err.message.includes('no such table')) {
                        db.get(`SELECT COUNT(*) as total FROM main.transfers ${whereMain}`, paramsMain, (e, r) => e ? res(0) : res(r?.total || 0));
                    } else if (err) {
                        rej(err);
                    } else {
                        res(row?.total || 0);
                    }
                });
            });

            const totalPages = Math.ceil(total / limit);

            db.all(dataSql, dataParams, (err, rows) => {
                if (err) {
                    if (err.message.includes('no such table')) {
                        const fallbackSql = `SELECT * FROM main.transfers ${whereMain} ORDER BY timestamp DESC LIMIT ? OFFSET ?`;
                        db.all(fallbackSql, [...paramsMain, limit, offset], (e, r) => {
                            if (e) return reject(e);
                            resolve({ data: mapTransfers(r), total, page, totalPages });
                        });
                        return;
                    }
                    return reject(err);
                }

                // Deduplicate Unified Result
                const seen = new Set();
                const unique = rows.filter(r => {
                    // Key without timestamp & comma normalization
                    const amt = String(r.amount).replace(/,/g, '');
                    const key = `${r.block}_${r.from_addr}_${r.to_addr}_${amt}_${r.asset_id}`;
                    if (seen.has(key)) return false;
                    seen.add(key);
                    return true;
                });

                resolve({
                    data: mapTransfers(unique),
                    total: total,
                    page: page,
                    totalPages: totalPages
                });
            });

        } catch (err) {
            reject(err);
        }
    });
}

function mapTransfers(rows) {
    return rows.map(r => {
        let formatted = r.formatted_time;
        if (r.timestamp) {
            const date = new Date(r.timestamp);
            if (!isNaN(date.getTime())) {
                formatted = `${date.getDate().toString().padStart(2, '0')}/${(date.getMonth() + 1).toString().padStart(2, '0')}/${date.getFullYear()} ${date.getHours().toString().padStart(2, '0')}:${date.getMinutes().toString().padStart(2, '0')}:${date.getSeconds().toString().padStart(2, '0')}`;
            }
        }
        return {
            time: formatted,
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
        };
    });
}

// --- SWAPS METHODS ---
function insertSwap(s) {
    const sql = `INSERT INTO swaps (timestamp, formatted_time, block, wallet, in_symbol, in_amount, in_logo, in_usd, out_symbol, out_amount, out_logo, out_usd, hash, extrinsic_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`;
    const params = [
        Date.now(), s.time, s.block, s.wallet,
        s.in.symbol, s.in.amount, s.in.logo, parseFloat(s.in.usd),
        s.out.symbol, s.out.amount, s.out.logo, parseFloat(s.out.usd),
        s.hash || '', s.extrinsic_id || ''
    ];
    db.run(sql, params, (err) => {
        if (err) console.error('Error insertSwap:', err.message);
    });
}

function getSwaps(address, page = 1, limit = 25) {
    return new Promise((resolve, reject) => {
        const offset = (page - 1) * limit;
        const countSql = `SELECT (SELECT COUNT(*) FROM main.swaps WHERE wallet = ?) + (SELECT COUNT(*) FROM history.swaps WHERE wallet = ?) as total`;

        const cols = `id, timestamp, formatted_time, block, wallet, in_symbol, in_amount, in_logo, in_usd, out_symbol, out_amount, out_logo, out_usd`;
        const sql = `
            SELECT ${cols} FROM main.swaps WHERE wallet = ?
            UNION ALL
            SELECT ${cols} FROM history.swaps WHERE wallet = ?
            ORDER BY timestamp DESC LIMIT ? OFFSET ?
        `;

        const paramsCount = [address, address];
        const paramsData = [address, address, limit, offset];

        db.get(countSql, paramsCount, (err, row) => {
            if (err && err.message.includes('no such table')) {
                // Fallback
                db.get(`SELECT COUNT(*) as count FROM main.swaps WHERE wallet = ?`, [address], (e, r) => {
                    if (e) return reject(e);
                    const total = r?.count || 0;
                    db.all(`SELECT * FROM main.swaps WHERE wallet = ? ORDER BY timestamp DESC LIMIT ? OFFSET ?`, [address, limit, offset], (e2, rows) => {
                        if (e2) return reject(e2);
                        resolve({ data: mapSwaps(rows), total, page, totalPages: Math.ceil(total / limit) });
                    });
                });
                return;
            } else if (err) return reject(err);

            const total = row.total || 0;
            const totalPages = Math.ceil(total / limit);

            db.all(sql, paramsData, (err, rows) => {
                if (err) return reject(err);

                // Deduplicate Unified Result (in case of overlap)
                const seen = new Set();
                const unique = rows.filter(r => {
                    const inAmt = String(r.in_amount).replace(/,/g, '');
                    const outAmt = String(r.out_amount).replace(/,/g, '');
                    const key = `${r.block}_${r.wallet}_${r.in_symbol}_${inAmt}_${r.out_symbol}_${outAmt}`;
                    if (seen.has(key)) return false;
                    seen.add(key);
                    return true;
                });

                resolve({
                    data: mapSwaps(unique),
                    total: total,
                    page: page,
                    totalPages: totalPages
                });
            });
        });
    });
}

function getLatestSwaps(page = 1, limit = 25, filter = null, timestamp = null) {
    return new Promise(async (resolve, reject) => {
        const offset = (page - 1) * limit;

        // Build WHERE clause
        let whereClauses = [];
        let params = [];

        if (filter) {
            const f = `%${filter.toUpperCase()}%`;
            whereClauses.push(`(in_symbol LIKE ? OR out_symbol LIKE ?)`);
            params.push(f, f);
        }

        if (timestamp) {
            whereClauses.push(`timestamp <= ?`);
            params.push(timestamp);
        }

        const whereMain = whereClauses.length > 0 ? ' WHERE ' + whereClauses.join(' AND ') : '';
        const whereHistory = whereMain; // Same filter for history

        // UNION ALL query combining main and history
        const cols = `id, timestamp, formatted_time, block, wallet, in_symbol, in_amount, in_logo, in_usd, out_symbol, out_amount, out_logo, out_usd, hash, extrinsic_id`;

        const countSql = `
            SELECT COUNT(*) as count FROM (
                SELECT id FROM main.swaps ${whereMain}
                UNION ALL
                SELECT id FROM history.swaps ${whereHistory}
            )
        `;

        const dataSql = `
            SELECT ${cols} FROM (
                SELECT ${cols} FROM main.swaps ${whereMain}
                UNION ALL
                SELECT ${cols} FROM history.swaps ${whereHistory}
            )
            ORDER BY timestamp DESC
            LIMIT ? OFFSET ?
        `;

        // Double params for both UNION branches
        const countParams = [...params, ...params];
        const dataParams = [...params, ...params, limit, offset];

        try {
            // Get total count
            const countRow = await new Promise((res, rej) => {
                db.get(countSql, countParams, (err, row) => {
                    if (err) {
                        // Fallback to main only if history doesn't exist
                        if (err.message.includes('no such table')) {
                            db.get(`SELECT COUNT(*) as count FROM main.swaps ${whereMain}`, params, (e, r) => {
                                e ? rej(e) : res(r);
                            });
                            return;
                        }
                        rej(err);
                    } else {
                        res(row);
                    }
                });
            });

            // Get data
            const rows = await new Promise((res, rej) => {
                db.all(dataSql, dataParams, (err, rows) => {
                    if (err) {
                        // Fallback to main only if history doesn't exist
                        if (err.message.includes('no such table')) {
                            const fallbackSql = `SELECT ${cols} FROM main.swaps ${whereMain} ORDER BY timestamp DESC LIMIT ? OFFSET ?`;
                            db.all(fallbackSql, [...params, limit, offset], (e, r) => {
                                e ? rej(e) : res(r || []);
                            });
                            return;
                        }
                        rej(err);
                    } else {
                        res(rows || []);
                    }
                });
            });

            // Deduplicate by block+wallet+amounts
            const seen = new Set();
            const unique = rows.filter(row => {
                const inAmt = String(row.in_amount).replace(/,/g, '');
                const outAmt = String(row.out_amount).replace(/,/g, '');
                const key = `${row.block}_${row.wallet}_${row.in_symbol}_${inAmt}_${row.out_symbol}_${outAmt}`;
                if (seen.has(key)) return false;
                seen.add(key);
                return true;
            });

            const total = countRow?.count || 0;
            const totalPages = Math.ceil(total / limit);

            resolve({
                data: mapSwaps(unique),
                total: total,
                page: page,
                totalPages: totalPages
            });
        } catch (err) {
            reject(err);
        }
    });
}

function mapSwaps(rows) {
    return rows.map(r => {
        let formatted = r.formatted_time;
        if (r.timestamp) {
            const date = new Date(r.timestamp);
            if (!isNaN(date.getTime())) {
                formatted = `${date.getDate().toString().padStart(2, '0')}/${(date.getMonth() + 1).toString().padStart(2, '0')}/${date.getFullYear()} ${date.getHours().toString().padStart(2, '0')}:${date.getMinutes().toString().padStart(2, '0')}:${date.getSeconds().toString().padStart(2, '0')}`;
            }
        }
        return {
            time: formatted,
            block: r.block,
            hash: r.hash,
            extrinsic_id: r.extrinsic_id,
            wallet: r.wallet,
            in: { symbol: r.in_symbol, amount: r.in_amount, logo: r.in_logo, usd: r.in_usd.toFixed(2) },
            out: { symbol: r.out_symbol, amount: r.out_amount, logo: r.out_logo, usd: r.out_usd.toFixed(2) }
        };
    });
}

// --- CHARTING METHODS ---
function getCandles(symbol, resolution = 60, limit = 1000) {
    return new Promise((resolve, reject) => {
        const intervalMs = resolution * 60 * 1000;
        // Unified Query: Fetch from both Main and History
        const sql = `
            SELECT timestamp, in_symbol, out_symbol, in_amount, out_amount, in_usd, out_usd 
            FROM main.swaps 
            WHERE in_symbol = ? OR out_symbol = ?
            UNION ALL
            SELECT timestamp, in_symbol, out_symbol, in_amount, out_amount, in_usd, out_usd 
            FROM history.swaps 
            WHERE in_symbol = ? OR out_symbol = ?
            ORDER BY timestamp ASC
        `;

        db.all(sql, [symbol, symbol, symbol, symbol], (err, rows) => {
            if (err) {
                // Fallback if history missing
                if (err.message.includes('no such table')) {
                    db.all(`SELECT timestamp, in_symbol, out_symbol, in_amount, out_amount, in_usd, out_usd FROM main.swaps WHERE in_symbol = ? OR out_symbol = ? ORDER BY timestamp ASC`, [symbol, symbol], (e, r) => {
                        if (e) return reject(e);
                        processRows(r);
                    });
                    return;
                }
                return reject(err);
            }
            processRows(rows);
        });

        function processRows(rows) {
            const candles = [];
            let currentBucket = null;
            let open = 0, high = 0, low = 0, close = 0;
            let bucketStartTime = 0;

            rows.forEach(r => {
                let price = 0;
                if (r.in_symbol === symbol) {
                    price = r.in_usd / parseFloat(r.in_amount);
                } else {
                    price = r.out_usd / parseFloat(r.out_amount);
                }

                if (isNaN(price) || price === 0) return;

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
            });

            if (currentBucket !== null) {
                candles.push({ time: bucketStartTime / 1000, open, high, low, close });
            }

            resolve(candles.slice(-limit));
        }
    });
}


function getPriceChange(symbol, currentPrice, timeframeMs) {
    return new Promise((resolve) => {
        if (!currentPrice || currentPrice === 0) return resolve(0);

        const now = Date.now();
        const pastTime = now - timeframeMs;

        // Optimization: Find the last swap occurring BEFORE the cutoff time.
        // This relies on the index and is O(1) or O(logN) vs scanning a window.
        const sql = `
            SELECT * FROM main.swaps 
            WHERE (in_symbol = ? OR out_symbol = ?) 
            AND timestamp <= ?
            UNION ALL
            SELECT * FROM history.swaps 
            WHERE (in_symbol = ? OR out_symbol = ?) 
            AND timestamp <= ?
            ORDER BY timestamp DESC 
            LIMIT 1
        `;

        db.get(sql, [symbol, symbol, pastTime, symbol, symbol, pastTime], (err, row) => {
            // Fallback logic handled by DB error or empty row
            if (err && err.message.includes('no such table')) {
                db.get(`SELECT * FROM main.swaps WHERE (in_symbol = ? OR out_symbol = ?) AND timestamp <= ? ORDER BY timestamp DESC LIMIT 1`, [symbol, symbol, pastTime], (e, r) => {
                    finishParams(e, r);
                });
            } else {
                finishParams(err, row);
            }
        });

        function finishParams(err, row) {
            if (err || !row) return resolve(0);

            let oldPrice = 0;
            if (row.in_symbol === symbol) {
                oldPrice = row.in_usd / parseFloat(row.in_amount);
            } else {
                oldPrice = row.out_usd / parseFloat(row.out_amount);
            }

            if (oldPrice === 0) return resolve(0);

            const change = ((currentPrice - oldPrice) / oldPrice) * 100;
            resolve(change);
        }
    });
}

function getSparkline(symbol, timeframeMs) {
    return new Promise((resolve) => {
        const now = Date.now();
        // Ignore timeframeMs for query limit, but we can filter if needed.
        // Crucial Optimization: fetch only the latest 1000 swaps.
        // Unified Sparkline Query
        const sql = `
            SELECT timestamp, in_symbol, out_symbol, in_amount, out_amount, in_usd, out_usd
            FROM main.swaps
            WHERE (in_symbol = ? OR out_symbol = ?)
            UNION ALL
            SELECT timestamp, in_symbol, out_symbol, in_amount, out_amount, in_usd, out_usd
            FROM history.swaps
            WHERE (in_symbol = ? OR out_symbol = ?)
            ORDER BY timestamp DESC
            LIMIT 1000
        `;

        db.all(sql, [symbol, symbol, symbol, symbol], (err, rows) => {
            if (err) {
                if (err.message.includes('no such table')) {
                    db.all(`SELECT timestamp, in_symbol, out_symbol, in_amount, out_amount, in_usd, out_usd FROM main.swaps WHERE (in_symbol = ? OR out_symbol = ?) ORDER BY timestamp DESC LIMIT 1000`, [symbol, symbol], (e, r) => {
                        if (e) return resolve([]);
                        processSparkline(r);
                    });
                    return;
                }
                return resolve([]);
            }
            processSparkline(rows);
        });

        function processSparkline(rows) {
            if (!rows || rows.length === 0) return resolve([]);

            // Rows are DESC (newest first). Reverse them for processing.
            rows.reverse();

            const startTime = now - timeframeMs;
            const filtered = rows.filter(r => r.timestamp >= startTime);

            const points = filtered.map(r => {
                let val = 0;
                if (r.in_symbol === symbol) val = r.in_usd / r.in_amount;
                else val = r.out_usd / r.out_amount;
                return { value: val, time: r.timestamp };
            });

            if (points.length === 0) return resolve([]);

            // Simple downsampling
            if (points.length <= 20) return resolve(points);

            const result = [];
            const step = Math.ceil(points.length / 20);
            for (let i = 0; i < points.length; i += step) {
                result.push(points[i]);
            }
            if (result.length > 0 && result[result.length - 1].time !== points[points.length - 1].time) {
                result.push(points[points.length - 1]);
            }

            resolve(result);
        }
    });
}

function getTopAccumulators(symbol, timeframeMs) {
    return new Promise((resolve, reject) => {
        const startTime = Date.now() - timeframeMs;
        // Unified Query with Subquery for Aggregation
        const sql = `
            SELECT wallet, 
                   SUM(out_usd) as total_bought_usd, 
                   SUM(out_amount) as total_bought_amount,
                   COUNT(*) as swap_count,
                   MAX(timestamp) as last_buy
            FROM (
                SELECT * FROM main.swaps
                UNION ALL
                SELECT * FROM history.swaps
            )
            WHERE out_symbol = ? AND timestamp > ?
            GROUP BY wallet 
            ORDER BY total_bought_usd DESC 
            LIMIT 10
        `;
        db.all(sql, [symbol, startTime], (err, rows) => {
            if (err && err.message.includes('no such table')) {
                // Fallback
                const fallbackSql = `
                    SELECT wallet, SUM(out_usd) as total_bought_usd, SUM(out_amount) as total_bought_amount, COUNT(*) as swap_count, MAX(timestamp) as last_buy
                    FROM main.swaps WHERE out_symbol = ? AND timestamp > ? GROUP BY wallet ORDER BY total_bought_usd DESC LIMIT 10`;
                db.all(fallbackSql, [symbol, startTime], (e, r) => {
                    if (e) return reject(e);
                    resolve(r);
                });
                return;
            }
            if (err) return reject(err);
            resolve(rows);
        });
    });
}

function getNetworkStats(timeframeMs) {
    return new Promise((resolve, reject) => {
        const startTime = Date.now() - timeframeMs;

        // Unified Queries
        // 1. Volume
        const volSql = `
            SELECT SUM(in_usd) as total_vol FROM (
                SELECT in_usd FROM main.swaps WHERE timestamp > ?
                UNION ALL
                SELECT in_usd FROM history.swaps WHERE timestamp > ?
            )
        `;

        // 2. Active Users
        const usersSql = `
            SELECT COUNT(DISTINCT wallet) as active_users FROM (
                SELECT wallet FROM main.swaps WHERE timestamp > ?
                UNION ALL
                SELECT wallet FROM history.swaps WHERE timestamp > ?
            )
        `;

        // 3. Tx Count
        const countSql = `
            SELECT COUNT(*) as tx_count FROM (
                SELECT id FROM main.swaps WHERE timestamp > ?
                UNION ALL
                SELECT id FROM history.swaps WHERE timestamp > ?
            )
        `;

        db.serialize(() => {
            let volume = 0;
            let users = 0;
            let txCount = 0;

            const handleErr = (err, fallbackSql, cb) => {
                if (err && err.message.includes('no such table')) {
                    db.get(fallbackSql, [startTime], cb);
                    return true;
                }
                return false;
            };

            db.get(volSql, [startTime, startTime], (err, row) => {
                if (handleErr(err, `SELECT SUM(in_usd) as total_vol FROM main.swaps WHERE timestamp > ?`, (e, r) => { if (r) volume = r.total_vol || 0; })) return;
                if (row) volume = row.total_vol || 0;
            });

            db.get(usersSql, [startTime, startTime], (err, row) => {
                if (handleErr(err, `SELECT COUNT(DISTINCT wallet) as active_users FROM main.swaps WHERE timestamp > ?`, (e, r) => { if (r) users = r.active_users || 0; })) return;
                if (row) users = row.active_users || 0;
            });

            db.get(countSql, [startTime, startTime], (err, row) => {
                if (handleErr(err, `SELECT COUNT(*) as tx_count FROM main.swaps WHERE timestamp > ?`, (e, r) => {
                    if (r) txCount = r.tx_count || 0;
                    resolve({ volume, users, txCount });
                })) return;

                if (row) txCount = row.tx_count || 0;
                resolve({ volume, users, txCount });
            });
        });
    });
}

function getMarketTrends(timeframeMs) {
    return new Promise((resolve, reject) => {
        const startTime = Date.now() - timeframeMs;
        // Unified Market Trends
        const sql = `
            SELECT 
                CASE WHEN in_usd > 0 THEN in_symbol ELSE out_symbol END as symbol,
                SUM(in_usd + out_usd) as volume
            FROM (
                SELECT * FROM main.swaps
                UNION ALL
                SELECT * FROM history.swaps
            )
            WHERE timestamp > ?
            GROUP BY symbol
            ORDER BY volume DESC
            LIMIT 5
        `;

        db.all(sql, [startTime], (err, rows) => {
            if (err) {
                if (err.message.includes('no such table')) {
                    const fbSql = `SELECT CASE WHEN in_usd > 0 THEN in_symbol ELSE out_symbol END as symbol, SUM(in_usd + out_usd) as volume FROM main.swaps WHERE timestamp > ? GROUP BY symbol ORDER BY volume DESC LIMIT 5`;
                    db.all(fbSql, [startTime], (e, r) => {
                        if (e) return reject(e);
                        resolve(r);
                    });
                    return;
                }
                return reject(err);
            }
            resolve(rows);
        });
    });
}

function getTotalStats() {
    return new Promise((resolve, reject) => {
        // Unified Total Stats
        const swapsSql = `SELECT (SELECT COUNT(*) FROM main.swaps) + (SELECT COUNT(*) FROM history.swaps) as count`;
        const transfersSql = `SELECT (SELECT COUNT(*) FROM main.transfers) + (SELECT COUNT(*) FROM history.transfers) as count`;

        db.serialize(() => {
            let swaps = 0;
            let transfers = 0;

            db.get(swapsSql, (err, row) => {
                if (err && err.message.includes('no such table')) {
                    db.get(`SELECT COUNT(*) as count FROM main.swaps`, (e, r) => { if (r) swaps = r.count; });
                } else if (row) swaps = row.count;
            });
            db.get(transfersSql, (err, row) => {
                if (err && err.message.includes('no such table')) {
                    db.get(`SELECT COUNT(*) as count FROM main.transfers`, (e, r) => {
                        if (r) transfers = r.count;
                        resolve({ swaps, transfers });
                    });
                } else {
                    if (row) transfers = row.count;
                    resolve({ swaps, transfers });
                }
            });
        });
    });
}

function getFilteredStats(startTime) {
    return new Promise((resolve, reject) => {
        const swapsSql = `
            SELECT (SELECT COUNT(*) FROM main.swaps WHERE timestamp >= ?) + 
                   (SELECT COUNT(*) FROM history.swaps WHERE timestamp >= ?) as count`;
        const transfersSql = `
            SELECT (SELECT COUNT(*) FROM main.transfers WHERE timestamp >= ?) + 
                   (SELECT COUNT(*) FROM history.transfers WHERE timestamp >= ?) as count`;
        const bridgesSql = `
            SELECT (SELECT COUNT(*) FROM main.bridges WHERE timestamp >= ?) + 
                   (SELECT COUNT(*) FROM history.bridges WHERE timestamp >= ?) as count`;

        db.serialize(() => {
            let swaps = 0;
            let transfers = 0;
            let bridges = 0;

            db.get(swapsSql, [startTime, startTime], (err, row) => {
                if (err && err.message.includes('no such table')) {
                    db.get(`SELECT COUNT(*) as count FROM main.swaps WHERE timestamp >= ?`, [startTime], (e, r) => { if (r) swaps = r.count; });
                } else if (row) swaps = row.count;
            });

            db.get(transfersSql, [startTime, startTime], (err, row) => {
                if (err && err.message.includes('no such table')) {
                    db.get(`SELECT COUNT(*) as count FROM main.transfers WHERE timestamp >= ?`, [startTime], (e, r) => { if (r) transfers = r.count; });
                } else if (row) transfers = row.count;
            });

            db.get(bridgesSql, [startTime, startTime], (err, row) => {
                if (err && err.message.includes('no such table')) {
                    db.get(`SELECT COUNT(*) as count FROM main.bridges WHERE timestamp >= ?`, [startTime], (e, r) => {
                        if (r) bridges = r.count;
                        resolve({ swaps, transfers, bridges });
                    });
                } else {
                    if (row) bridges = row.count;
                    resolve({ swaps, transfers, bridges });
                }
            });
        });
    });
}

function getFeeStats(startTime) {
    return new Promise((resolve, reject) => {
        // Unified query: combine main and history databases
        const sql = `
            SELECT type, SUM(amount) as total_xor, SUM(usd_value) as total_usd 
            FROM (
                SELECT type, amount, usd_value FROM main.fees WHERE timestamp >= ?
                UNION ALL
                SELECT type, amount, usd_value FROM history.fees WHERE timestamp >= ?
            )
            GROUP BY type
        `;
        db.all(sql, [startTime, startTime], (err, rows) => {
            if (err) {
                // Fallback if history table doesn't exist
                if (err.message.includes('no such table')) {
                    const fallbackSql = `SELECT type, SUM(amount) as total_xor, SUM(usd_value) as total_usd FROM main.fees WHERE timestamp >= ? GROUP BY type`;
                    db.all(fallbackSql, [startTime], (e, r) => e ? reject(e) : resolve(r || []));
                    return;
                }
                reject(err);
            } else {
                resolve(rows || []);
            }
        });
    });
}

function getFeeTrend(startTime, interval) {
    return new Promise((resolve, reject) => {
        // Interval: 'hour' or 'day'
        let fmt = '%Y-%m-%d %H:00:00';
        if (interval === 'day') fmt = '%Y-%m-%d';

        // Unified query: combine main and history databases (now using USD for better visual stability)
        const sql = `
            SELECT strftime('${fmt}', timestamp / 1000, 'unixepoch') as bucket, SUM(usd_value) as total_usd 
            FROM (
                SELECT timestamp, usd_value FROM main.fees WHERE timestamp >= ?
                UNION ALL
                SELECT timestamp, usd_value FROM history.fees WHERE timestamp >= ?
            )
            GROUP BY bucket 
            ORDER BY bucket ASC
        `;

        db.all(sql, [startTime, startTime], (err, rows) => {
            if (err) {
                // Fallback if history table doesn't exist
                if (err.message.includes('no such table')) {
                    const fallbackSql = `SELECT strftime('${fmt}', timestamp / 1000, 'unixepoch') as bucket, SUM(usd_value) as total_usd FROM main.fees WHERE timestamp >= ? GROUP BY bucket ORDER BY bucket ASC`;
                    db.all(fallbackSql, [startTime], (e, r) => e ? reject(e) : resolve(r || []));
                    return;
                }
                reject(err);
            } else {
                resolve(rows || []);
            }
        });
    });
}

function getWalletBridges(address, page = 1, limit = 20) {
    return new Promise(async (resolve, reject) => {
        const offset = (page - 1) * limit;

        const countSql = `SELECT (SELECT COUNT(*) FROM main.bridges WHERE sender = ? OR recipient = ?) + (SELECT COUNT(*) FROM history.bridges WHERE sender = ? OR recipient = ?) as total`;

        const cols = `id, timestamp, block, network, direction, sender, recipient, asset_id, amount, usd_value`;
        const sql = `
            SELECT ${cols} FROM main.bridges WHERE sender = ? OR recipient = ?
            UNION ALL
            SELECT ${cols} FROM history.bridges WHERE sender = ? OR recipient = ?
            ORDER BY timestamp DESC LIMIT ? OFFSET ?
        `;

        const paramsCount = [address, address, address, address];
        const paramsData = [address, address, address, address, limit, offset];

        try {
            const total = await new Promise((res, rej) => {
                db.get(countSql, paramsCount, (err, row) => {
                    if (err && err.message.includes('no such table')) {
                        db.get(`SELECT COUNT(*) as total FROM main.bridges WHERE sender = ? OR recipient = ?`, [address, address], (e, r) => e ? res(0) : res(r?.total || 0));
                    } else if (err) rej(err);
                    else res(row?.total || 0);
                });
            });

            const totalPages = Math.ceil(total / limit);

            db.all(sql, paramsData, (err, rows) => {
                if (err) {
                    if (err.message.includes('no such table')) {
                        db.all(`SELECT * FROM main.bridges WHERE sender = ? OR recipient = ? ORDER BY timestamp DESC LIMIT ? OFFSET ?`, [address, address, limit, offset], (e, rows) => {
                            if (e) return reject(e);
                            resolve({ data: rows.map(r => ({ ...r, time: new Date(r.timestamp).toLocaleString() })), total, totalPages, page });
                        });
                        return;
                    }
                    return reject(err);
                }

                // Deduplicate Unified Result
                const seen = new Set();
                const unique = rows.filter(r => {
                    const amt = String(r.amount).replace(/,/g, '');
                    const key = `${r.block}_${r.sender}_${r.recipient}_${amt}`;
                    if (seen.has(key)) return false;
                    seen.add(key);
                    return true;
                });

                resolve({
                    data: unique.map(r => ({
                        ...r,
                        time: new Date(r.timestamp).toLocaleString()
                    })),
                    total: total,
                    totalPages: totalPages,
                    page: page
                });
            });
        } catch (err) {
            reject(err);
        }
    });
}

function getLatestBridges(page = 1, limit = 20, filter = null, timestamp = null) {
    return new Promise(async (resolve, reject) => {
        const offset = (page - 1) * limit;

        let whereMain = '';
        let whereHistory = '';
        let paramsMain = [];
        let paramsHistory = [];

        if (filter) {
            const f = `%${filter.toUpperCase()}%`;
            whereMain += ` WHERE (sender LIKE ? OR recipient LIKE ? OR network LIKE ? OR asset_id LIKE ?)`;
            whereHistory += ` WHERE (sender LIKE ? OR recipient LIKE ? OR network LIKE ? OR asset_id LIKE ?)`;
            paramsMain.push(f, f, f, f);
            paramsHistory.push(f, f, f, f);
        }

        if (timestamp) {
            const prefixMain = whereMain ? ' AND ' : ' WHERE ';
            const prefixHistory = whereHistory ? ' AND ' : ' WHERE ';
            whereMain += `${prefixMain}timestamp <= ?`;
            whereHistory += `${prefixHistory}timestamp <= ?`;
            paramsMain.push(timestamp);
            paramsHistory.push(timestamp);
        }

        const countSql = `
            SELECT 
                (SELECT COUNT(*) FROM main.bridges ${whereMain}) + 
                (SELECT COUNT(*) FROM history.bridges ${whereHistory}) as total
        `;

        const cols = `id, timestamp, block, network, direction, sender, recipient, asset_id, amount, usd_value, hash, extrinsic_id`;

        const dataSql = `
            SELECT ${cols} FROM main.bridges ${whereMain}
            UNION ALL
            SELECT ${cols} FROM history.bridges ${whereHistory}
            ORDER BY timestamp DESC
            LIMIT ? OFFSET ?
        `;

        const dataParams = [...paramsMain, ...paramsHistory, limit, offset];

        try {
            const total = await new Promise((res, rej) => {
                db.get(countSql, [...paramsMain, ...paramsHistory], (err, row) => {
                    if (err && err.message.includes('no such table')) {
                        db.get(`SELECT COUNT(*) as total FROM main.bridges ${whereMain}`, paramsMain, (e, r) => e ? res(0) : res(r?.total || 0));
                    } else if (err) {
                        rej(err);
                    } else {
                        res(row?.total || 0);
                    }
                });
            });

            const totalPages = Math.ceil(total / limit);

            db.all(dataSql, dataParams, (err, rows) => {
                if (err) {
                    if (err.message.includes('no such table')) {
                        const fallbackSql = `SELECT * FROM main.bridges ${whereMain} ORDER BY timestamp DESC LIMIT ? OFFSET ?`;
                        db.all(fallbackSql, [...paramsMain, limit, offset], (e, r) => {
                            if (e) return reject(e);
                            resolve({
                                data: rows.map(r => ({
                                    ...r,
                                    time: new Date(r.timestamp).toLocaleString()
                                })), total, page, totalPages
                            });
                        });
                        return;
                    }
                    return reject(err);
                }

                // Deduplicate Unified Result
                const seen = new Set();
                const unique = rows.filter(r => {
                    // Key without timestamp
                    const amt = String(r.amount).replace(/,/g, '');
                    const key = `${r.block}_${r.sender}_${r.recipient}_${amt}`;
                    if (seen.has(key)) return false;
                    seen.add(key);
                    return true;
                });

                const data = unique.map(r => ({
                    ...r,
                    time: new Date(r.timestamp).toLocaleString()
                }));

                resolve({ data, total, totalPages, page });
            });

        } catch (err) {
            reject(err);
        }
    });
}

function insertLiquidityEvent(event) {
    const sql = `INSERT INTO liquidity_events (timestamp, block, wallet, pool_base, pool_target, base_amount, target_amount, usd_value, type, hash, extrinsic_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`;
    const params = [
        Date.now(),
        event.block || 0,
        event.wallet,
        event.poolBase,
        event.poolTarget,
        event.baseAmount,
        event.targetAmount,
        event.usdValue || 0,
        event.type, // 'deposit' or 'withdraw'
        event.hash || '',
        event.extrinsic_id || ''
    ];
    db.run(sql, params, (err) => {
        if (err) console.error('Error insertLiquidityEvent:', err.message);
    });
}

function getLpVolume(msWindow) {
    return new Promise((resolve, reject) => {
        const startTime = Date.now() - msWindow;
        // Unified LP Volume
        const sql = `
            SELECT COALESCE(SUM(val), 0) as total FROM (
                SELECT COALESCE(SUM(CASE WHEN type = 'deposit' THEN usd_value ELSE -usd_value END), 0) as val FROM main.liquidity_events WHERE timestamp >= ?
                UNION ALL
                SELECT COALESCE(SUM(CASE WHEN type = 'deposit' THEN usd_value ELSE -usd_value END), 0) as val FROM history.liquidity_events WHERE timestamp >= ?
            )
        `;
        db.get(sql, [startTime, startTime], (err, row) => {
            if (err) {
                if (err.message.includes('no such table')) {
                    db.get(`SELECT COALESCE(SUM(CASE WHEN type = 'deposit' THEN usd_value ELSE -usd_value END), 0) as total FROM main.liquidity_events WHERE timestamp >= ?`, [startTime], (e, r) => resolve(r?.total || 0));
                    return;
                }
                resolve(0);
            } else {
                resolve(row.total || 0);
            }
        });
    });
}

function getTransferVolume(msWindow) {
    return new Promise((resolve, reject) => {
        const startTime = Date.now() - msWindow;
        // Unified Transfer Volume
        const sql = `
            SELECT COALESCE(SUM(val), 0) as total FROM (
                SELECT COALESCE(SUM(usd_value), 0) as val FROM main.transfers WHERE timestamp >= ?
                UNION ALL
                SELECT COALESCE(SUM(usd_value), 0) as val FROM history.transfers WHERE timestamp >= ?
            )
        `;
        db.get(sql, [startTime, startTime], (err, row) => {
            if (err) {
                if (err.message.includes('no such table')) {
                    db.get(`SELECT COALESCE(SUM(usd_value), 0) as total FROM main.transfers WHERE timestamp >= ?`, [startTime], (e, r) => resolve(r?.total || 0));
                    return;
                }
                resolve(0);
            } else {
                resolve(row.total || 0);
            }
        });
    });
}

function getPoolActivity(base, target, limit = 10) {
    return new Promise((resolve, reject) => {
        const sql = `
            SELECT * FROM (
                SELECT * FROM main.liquidity_events WHERE pool_base = ? AND pool_target = ? 
                UNION ALL
                SELECT * FROM history.liquidity_events WHERE pool_base = ? AND pool_target = ? 
            )
            ORDER BY timestamp DESC
            LIMIT ?
        `;
        // Fallback if no history
        const fallbackSql = `SELECT * FROM main.liquidity_events WHERE pool_base = ? AND pool_target = ? ORDER BY timestamp DESC LIMIT ?`;

        db.all(sql, [base, target, base, target, limit], (err, rows) => {
            if (err && err.message.includes('no such table')) {
                db.all(fallbackSql, [base, target, limit], (e2, r2) => {
                    if (e2) resolve([]);
                    else resolve(formatRows(r2 || []));
                });
            } else if (err) {
                resolve([]);
            } else {
                resolve(formatRows(rows || []));
            }
        });

        function formatRows(rows) {
            return rows.map(r => ({
                ...r,
                time: new Date(r.timestamp).toLocaleString()
            }));
        }
    });
}

function getNetworkTrend(startTime, interval) {
    return new Promise(async (resolve, reject) => {
        let fmt = '%Y-%m-%d %H:00:00';
        if (interval === 'day') fmt = '%Y-%m-%d';

        const runQuery = (sql, params = [startTime]) => new Promise((res, rej) => {
            db.all(sql, params, (err, rows) => {
                if (err && err.message.includes('no such table')) {
                    // Simple fallback to main tables if history fails
                    return res([]);
                }
                return err ? rej(err) : res(rows);
            });
        });

        // Helper specifically for handling unified series fallback (complex)
        // For brevity in this fix, we will assume history table exists or correct logic via catch.
        // But optimally we should define the fallback SQLs.
        // Given complexity, we construct the unified SQLs directly.

        try {
            const pSwaps = runQuery(`
                SELECT strftime('${fmt}', timestamp / 1000, 'unixepoch') as bucket, SUM(in_usd) as val FROM (
                    SELECT timestamp, in_usd FROM main.swaps WHERE timestamp >= ?
                    UNION ALL SELECT timestamp, in_usd FROM history.swaps WHERE timestamp >= ?
                ) GROUP BY bucket`, [startTime, startTime]);

            const pTransfers = runQuery(`
                SELECT strftime('${fmt}', timestamp / 1000, 'unixepoch') as bucket, SUM(usd_value) as val FROM (
                    SELECT timestamp, usd_value FROM main.transfers WHERE timestamp >= ?
                    UNION ALL SELECT timestamp, usd_value FROM history.transfers WHERE timestamp >= ?
                ) GROUP BY bucket`, [startTime, startTime]);

            const pLp = runQuery(`
                SELECT strftime('${fmt}', timestamp / 1000, 'unixepoch') as bucket, SUM(val) as val FROM (
                   SELECT timestamp, (CASE WHEN type = 'deposit' THEN usd_value ELSE -usd_value END) as val FROM main.liquidity_events WHERE timestamp >= ?
                   UNION ALL
                   SELECT timestamp, (CASE WHEN type = 'deposit' THEN usd_value ELSE -usd_value END) as val FROM history.liquidity_events WHERE timestamp >= ?
                ) GROUP BY bucket`, [startTime, startTime]);

            const sqlAcc = `SELECT strftime('${fmt}', ms/1000, 'unixepoch') as bucket, COUNT(DISTINCT wallet) as val FROM (
                    SELECT timestamp as ms, wallet FROM main.swaps WHERE timestamp >= ?
                    UNION ALL
                    SELECT timestamp as ms, wallet FROM history.swaps WHERE timestamp >= ?
                    UNION ALL
                    SELECT timestamp as ms, from_addr as wallet FROM main.transfers WHERE timestamp >= ?
                    UNION ALL
                    SELECT timestamp as ms, from_addr as wallet FROM history.transfers WHERE timestamp >= ?
                ) GROUP BY bucket`;
            const pAccounts = runQuery(sqlAcc, [startTime, startTime, startTime, startTime]);

            const [swaps, transfers, lp, accounts] = await Promise.all([pSwaps, pTransfers, pLp, pAccounts]);
            resolve({ swaps, transfers, lp, accounts });
        } catch (e) {
            // Only retry with Main DB if it was a table error, otherwise return empty
            console.error("getNetworkTrend error (Unified):", e.message);
            resolve({ swaps: [], transfers: [], lp: [], accounts: [] });
        }
    });
}


function getTopTokens(startTime) {
    return new Promise((resolve, reject) => {
        // UNION to sum both incoming and outgoing volume
        // Unified Top Tokens
        const sql = `
            SELECT symbol, SUM(usd_value) as volume, logo 
            FROM (
                SELECT in_symbol as symbol, in_usd as usd_value, in_logo as logo FROM main.swaps WHERE timestamp >= ?
                UNION ALL
                SELECT in_symbol as symbol, in_usd as usd_value, in_logo as logo FROM history.swaps WHERE timestamp >= ?
                UNION ALL
                SELECT out_symbol as symbol, out_usd as usd_value, out_logo as logo FROM main.swaps WHERE timestamp >= ?
                UNION ALL
                SELECT out_symbol as symbol, out_usd as usd_value, out_logo as logo FROM history.swaps WHERE timestamp >= ?
            ) 
            GROUP BY symbol 
            ORDER BY volume DESC 
            LIMIT 5
        `;
        db.all(sql, [startTime, startTime, startTime, startTime], (err, rows) => {
            if (err && err.message.includes('no such table')) {
                const fb = `
                    SELECT symbol, SUM(usd_value) as volume, logo 
                    FROM (
                        SELECT in_symbol as symbol, in_usd as usd_value, in_logo as logo FROM main.swaps WHERE timestamp >= ?
                        UNION ALL
                        SELECT out_symbol as symbol, out_usd as usd_value, out_logo as logo FROM main.swaps WHERE timestamp >= ?
                    ) 
                    GROUP BY symbol ORDER BY volume DESC LIMIT 5
                 `;
                db.all(fb, [startTime, startTime], (e, r) => e ? reject(e) : resolve(r));
                return;
            }
            if (err) reject(err);
            else resolve(rows);
        });
    });
}


function getStablecoinStats(startTime) {
    return new Promise((resolve, reject) => {
        const tokens = ['KUSD', 'XSTUSD', 'TBCD'];
        const results = {};

        // Helper to run query for a specific symbol
        // Helper to run query for a specific symbol with Unified DBs
        const runForToken = (symbol) => {
            return new Promise((res, rej) => {
                const sql = `
                    SELECT 
                        (SELECT COALESCE(SUM(in_usd), 0) + COALESCE(SUM(out_usd), 0) FROM (
                             SELECT in_usd, out_usd FROM main.swaps WHERE (in_symbol = ? OR out_symbol = ?) AND timestamp >= ?
                             UNION ALL
                             SELECT in_usd, out_usd FROM history.swaps WHERE (in_symbol = ? OR out_symbol = ?) AND timestamp >= ?
                        )) as swap_vol,
                        (SELECT COALESCE(SUM(usd_value), 0) FROM (
                             SELECT usd_value FROM main.transfers WHERE symbol = ? AND timestamp >= ?
                             UNION ALL
                             SELECT usd_value FROM history.transfers WHERE symbol = ? AND timestamp >= ?
                        )) as transfer_vol
                `;
                const params = [
                    symbol, symbol, startTime, symbol, symbol, startTime, // Swaps (Main + History)
                    symbol, startTime, symbol, startTime // Transfers (Main + History)
                ];

                db.get(sql, params, (err, row) => {
                    if (err) {
                        if (err.message.includes('no such table')) {
                            // Fallback
                            const fb = `
                                SELECT 
                                    (SELECT COALESCE(SUM(in_usd), 0) + COALESCE(SUM(out_usd), 0) FROM main.swaps WHERE (in_symbol = ? OR out_symbol = ?) AND timestamp >= ?) as swap_vol,
                                    (SELECT COALESCE(SUM(usd_value), 0) FROM main.transfers WHERE symbol = ? AND timestamp >= ?) as transfer_vol
                             `;
                            db.get(fb, [symbol, symbol, startTime, symbol, startTime], (e, r) => {
                                if (e) return rej(e);
                                res({ symbol, swapVolume: r?.swap_vol || 0, transferVolume: r?.transfer_vol || 0 });
                            });
                            return;
                        }
                        rej(err);
                    } else res({
                        symbol,
                        swapVolume: row?.swap_vol || 0,
                        transferVolume: row?.transfer_vol || 0
                    });
                });
            });
        };

        Promise.all(tokens.map(t => runForToken(t)))
            .then(data => {
                data.forEach(item => results[item.symbol] = item);
                resolve(results);
            })
            .catch(reject);
    });
}

function getLiquidityEvents(page = 1, limit = 25, timestamp = null) {
    return new Promise((resolve, reject) => {
        const offset = (page - 1) * limit;
        let where = "";
        let params = [];

        if (timestamp) {
            where = "WHERE timestamp <= ?";
            params.push(timestamp);
        }

        const countSql = `
            SELECT COUNT(*) as total FROM (
                SELECT timestamp FROM main.liquidity_events ${where}
                UNION ALL
                SELECT timestamp FROM history.liquidity_events ${where}
            )`;

        // Fallback if history missing
        const fallbackCount = `SELECT COUNT(*) as total FROM main.liquidity_events ${where}`;

        db.get(countSql, params.concat(params), (err, row) => {
            if (err && err.message.includes('no such table')) {
                db.get(fallbackCount, params, (e2, r2) => {
                    const total = r2?.total || 0;
                    fetchData(total, true);
                });
            } else {
                const total = row?.total || 0;
                fetchData(total, false);
            }
        });

        function fetchData(total, fallback) {
            const sql = `
                SELECT * FROM (
                    SELECT * FROM main.liquidity_events ${where}
                    UNION ALL
                    SELECT * FROM history.liquidity_events ${where}
                )
                ORDER BY timestamp DESC
                LIMIT ? OFFSET ?
            `;
            const fallbackSql = `SELECT * FROM main.liquidity_events ${where} ORDER BY timestamp DESC LIMIT ? OFFSET ?`;

            const query = fallback ? fallbackSql : sql;
            const qParams = fallback ? [...params, limit, offset] : [...params, ...params, limit, offset];

            db.all(query, qParams, (err, rows) => {
                if (err) return reject(err);
                resolve({ data: rows || [], total });
            });
        }
    });
}

module.exports = { initDB, insertTransfer, getTransfers, getLatestTransfers, insertSwap, getSwaps, getLatestSwaps, getCandles, getPriceChange, getSparkline, getTopAccumulators, getNetworkStats, getMarketTrends, getTotalStats, insertBridge, getFilteredStats, insertFee, getFeeStats, getFeeTrend, getWalletBridges, getLatestBridges, getLpVolume, insertLiquidityEvent, getTransferVolume, getPoolActivity, getNetworkTrend, getTopTokens, getStablecoinStats, getLiquidityEvents };


