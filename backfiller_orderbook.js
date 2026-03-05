// backfiller_orderbook.js - Re-indexer for Order Book events
// Scans blocks already processed by the main backfiller, extracting ONLY orderBook events.
// Much faster than main backfiller since it skips price queries.
// Usage: node backfiller_orderbook.js   (or via PM2)

const { ApiPromise, WsProvider } = require('@polkadot/api');
const { options } = require('@sora-substrate/api');
const fs = require('fs');
const path = require('path');
const Database = require('better-sqlite3');

const { WS_ENDPOINT_BACKFILL, WHITELIST_URL } = require('./config');
const WS_ENDPOINT = WS_ENDPOINT_BACKFILL;

const STATE_FILE = path.join(__dirname, 'backfill_orderbook_state.json');
const MAIN_STATE_FILE = path.join(__dirname, 'backfill_state.json');
const BLOCKS_PER_BATCH = 200;
const DELAY_BETWEEN_BATCHES_MS = 300;
const PROGRESS_SAVE_INTERVAL = 500;

// Writes to database.db (the history DB)
const DB_PATH = path.join(__dirname, 'database.db');
let db;
let stmts = {};
let api = null;
let ASSETS = [];
let stats = { orderbook: 0, blocks: 0, skipped: 0 };
let startTime = Date.now();

function initDB() {
    db = new Database(DB_PATH);
    db.pragma('journal_mode = WAL');
    db.pragma('synchronous = NORMAL');
    db.pragma('cache_size = -16000');
    db.pragma('temp_store = MEMORY');

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

    db.exec(`
        CREATE INDEX IF NOT EXISTS idx_orderbook_timestamp ON order_book_events(timestamp);
        CREATE INDEX IF NOT EXISTS idx_orderbook_wallet ON order_book_events(wallet);
        CREATE INDEX IF NOT EXISTS idx_orderbook_event_type ON order_book_events(event_type);
        CREATE INDEX IF NOT EXISTS idx_orderbook_block ON order_book_events(block);
        CREATE INDEX IF NOT EXISTS idx_orderbook_timestamp_type ON order_book_events(timestamp, event_type);
    `);

    stmts.insertOrderBook = db.prepare(`INSERT INTO order_book_events (timestamp, formatted_time, block, event_type, wallet, order_id, base_asset, quote_asset, side, price, amount, usd_value, hash, extrinsic_id)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`);
    stmts.checkBlock = db.prepare('SELECT 1 FROM order_book_events WHERE block = ? LIMIT 1');

    console.log('💾 Database ready.');
}

function withTransaction(fn) {
    const transaction = db.transaction(fn);
    return transaction();
}

function loadState() {
    try {
        if (fs.existsSync(STATE_FILE)) return JSON.parse(fs.readFileSync(STATE_FILE, 'utf8'));
    } catch (e) { }
    return { lastProcessedBlock: null, totalBlocksProcessed: 0, startedAt: new Date().toISOString() };
}

function saveState(state) {
    fs.writeFileSync(STATE_FILE, JSON.stringify(state, null, 2));
}

function loadMainState() {
    try {
        if (fs.existsSync(MAIN_STATE_FILE)) return JSON.parse(fs.readFileSync(MAIN_STATE_FILE, 'utf8'));
    } catch (e) { }
    return null;
}

async function loadAssets() {
    try {
        const res = await fetch(WHITELIST_URL);
        const data = await res.json();
        ASSETS = data.map(item => ({
            symbol: item.symbol, name: item.name, decimals: item.decimals,
            assetId: item.address, logo: item.icon
        }));
        console.log(`✅ Loaded ${ASSETS.length} assets.`);
    } catch (e) {
        ASSETS = [];
        console.warn('⚠️ Could not load assets, symbols will be raw IDs.');
    }
}

function getAssetInfo(rawId) {
    if (!rawId) return null;
    let str = rawId.toString();
    if (typeof rawId === 'object' && rawId.code) str = rawId.code;
    return ASSETS.find(a => a.assetId === str || a.assetId?.toLowerCase() === str?.toLowerCase());
}

async function processBlock(blockNumber) {
    try {
        // Skip if already processed
        if (stmts.checkBlock.get(blockNumber)) {
            stats.skipped++;
            return true;
        }

        const blockHash = await api.rpc.chain.getBlockHash(blockNumber);
        const [signedBlock, allEvents, timestamp] = await Promise.all([
            api.rpc.chain.getBlock(blockHash),
            api.query.system.events.at(blockHash),
            api.query.timestamp.now.at(blockHash)
        ]);

        const blockTimestamp = timestamp.toNumber();

        const orderBookEvts = allEvents.filter(({ event }) =>
            event.section === 'orderBook'
        );

        if (orderBookEvts.length === 0) {
            stats.blocks++;
            return true;
        }

        const pending = [];

        for (const { event, phase } of orderBookEvts) {
            try {
                const m = event.method;
                const d = event.data.toJSON();
                let eventType = '', wallet = '', orderId = '', baseAsset = '', quoteAsset = '', side = '', price = '', amount = '';

                const obId = d[0];
                if (obId && typeof obId === 'object') {
                    const bInfo = getAssetInfo(obId.base);
                    const qInfo = getAssetInfo(obId.quote);
                    baseAsset = bInfo ? bInfo.symbol : (obId.base || '');
                    quoteAsset = qInfo ? qInfo.symbol : (obId.quote || '');
                }

                if (m === 'LimitOrderPlaced') {
                    eventType = 'placed'; orderId = String(d[1] || ''); wallet = d[2] || '';
                    side = d[3] === 'Buy' ? 'buy' : 'sell'; price = d[4] || ''; amount = d[5] || '';
                } else if (m === 'LimitOrderCanceled') {
                    eventType = 'canceled'; orderId = String(d[1] || ''); wallet = d[2] || '';
                } else if (m === 'LimitOrderExecuted') {
                    eventType = 'executed'; orderId = String(d[1] || ''); wallet = d[2] || '';
                    side = d[3] === 'Buy' ? 'buy' : 'sell'; price = d[4] || ''; amount = d[5] || '';
                } else if (m === 'LimitOrderFilled') {
                    eventType = 'filled'; orderId = String(d[1] || ''); wallet = d[2] || '';
                } else if (m === 'MarketOrderExecuted') {
                    eventType = 'market'; wallet = d[1] || '';
                    side = d[2] === 'Buy' ? 'buy' : 'sell'; amount = d[3] || ''; price = d[4] || '';
                } else { continue; }

                if (typeof price === 'string') price = price.replace(/,/g, '');
                if (typeof amount === 'string') amount = amount.replace(/,/g, '');

                let hash = '', extrinsic_id = '';
                if (phase && phase.isApplyExtrinsic) {
                    const exIdx = phase.asApplyExtrinsic.toNumber();
                    const ex = signedBlock.block.extrinsics[exIdx];
                    if (ex) { hash = ex.hash.toHex(); extrinsic_id = `${blockNumber}-${exIdx}`; }
                }

                const formattedTime = new Date(blockTimestamp).toLocaleString('es-ES');
                pending.push({
                    timestamp: blockTimestamp, formatted_time: formattedTime,
                    block: blockNumber, event_type: eventType,
                    wallet, order_id: orderId,
                    base_asset: baseAsset, quote_asset: quoteAsset,
                    side, price, amount, usd_value: 0,
                    hash, extrinsic_id
                });
            } catch (e) { }
        }

        if (pending.length > 0) {
            withTransaction(() => {
                for (const ob of pending) {
                    stmts.insertOrderBook.run(
                        ob.timestamp, ob.formatted_time, ob.block,
                        ob.event_type, ob.wallet, ob.order_id,
                        ob.base_asset, ob.quote_asset, ob.side,
                        ob.price, ob.amount, ob.usd_value,
                        ob.hash, ob.extrinsic_id
                    );
                    stats.orderbook++;
                }
            });
            console.log(`📋 Block ${blockNumber}: ${pending.length} orderBook events inserted`);
        }

        stats.blocks++;
        return true;
    } catch (e) {
        console.error(`❌ Error block ${blockNumber}:`, e.message);
        return false;
    }
}

async function run() {
    console.log('📋 Order Book Re-indexer starting...');
    console.log(`📡 Connecting to ${WS_ENDPOINT}...`);

    initDB();
    await loadAssets();

    const provider = new WsProvider(WS_ENDPOINT);
    api = await ApiPromise.create(options({ provider }));
    await api.isReady;
    console.log('✅ Connected to blockchain.');

    const header = await api.rpc.chain.getHeader();
    const currentBlock = header.number.toNumber();

    // Determine range: from current block down to where main backfiller has reached
    const mainState = loadMainState();
    const lowestBlock = mainState?.lastProcessedBlock || 1;

    let state = loadState();
    let startBlock = state.lastProcessedBlock !== null
        ? Math.min(state.lastProcessedBlock - 1, currentBlock)
        : currentBlock;

    console.log(`🔄 Scanning blocks ${startBlock.toLocaleString()} → ${lowestBlock.toLocaleString()}`);
    console.log(`📊 Range: ~${(startBlock - lowestBlock).toLocaleString()} blocks`);

    startTime = Date.now();
    let blocksProcessedThisSession = 0;

    for (let block = startBlock; block >= lowestBlock; block -= BLOCKS_PER_BATCH) {
        const batchEnd = Math.max(block - BLOCKS_PER_BATCH + 1, lowestBlock);

        for (let b = block; b >= batchEnd; b--) {
            await processBlock(b);
            blocksProcessedThisSession++;
            state.lastProcessedBlock = b;
            state.totalBlocksProcessed++;

            if (blocksProcessedThisSession % 500 === 0) {
                const elapsed = (Date.now() - startTime) / 1000;
                const speed = blocksProcessedThisSession / elapsed;
                const remaining = b - lowestBlock;
                const eta = remaining / speed / 3600;
                console.log(`📈 Block ${b.toLocaleString()} | ${speed.toFixed(1)} blk/s | ETA: ${eta.toFixed(1)}h | OB:${stats.orderbook} | Skip:${stats.skipped}`);
            }

            if (blocksProcessedThisSession % PROGRESS_SAVE_INTERVAL === 0) {
                saveState(state);
            }
        }
        await new Promise(r => setTimeout(r, DELAY_BETWEEN_BATCHES_MS));
    }

    saveState(state);
    console.log(`✅ Order Book re-index complete! Found ${stats.orderbook} events in ${stats.blocks} blocks.`);
    db.close();
    process.exit(0);
}

process.on('SIGINT', () => {
    console.log('\n🛑 Saving state...');
    try {
        const state = loadState();
        saveState(state);
        if (db) db.close();
    } catch (e) { }
    process.exit(0);
});

process.on('SIGTERM', () => {
    console.log('\n🛑 Saving state...');
    try {
        const state = loadState();
        saveState(state);
        if (db) db.close();
    } catch (e) { }
    process.exit(0);
});

run().catch(e => { console.error('Fatal:', e); process.exit(1); });
