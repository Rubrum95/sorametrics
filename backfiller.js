// backfiller.js - Historical Blockchain Indexer for SoraMetrics
// Processes blocks from newest to oldest, extracting swaps, transfers, bridges with HISTORICAL PRICES
// Optimized: better-sqlite3, WAL mode, prepared statements, batch transactions

const { ApiPromise, WsProvider } = require('@polkadot/api');
const { options } = require('@sora-substrate/api');
const { resolveEthSender } = require('./eth_helper');
const BigNumber = require('bignumber.js');
const fs = require('fs');
const path = require('path');
const Database = require('better-sqlite3');

// Configuration
const { WS_ENDPOINT_BACKFILL, WHITELIST_URL } = require('./config');
const WS_ENDPOINT = WS_ENDPOINT_BACKFILL;
const STATE_FILE = path.join(__dirname, 'backfill_state.json');
const BLOCKS_PER_BATCH = 100;
const DELAY_BETWEEN_BATCHES_MS = 500;
const PROGRESS_SAVE_INTERVAL = 500;
const SAFETY_OFFSET = 10;

// Database - writes to database.db (the history DB that the main app reads)
const DB_PATH = path.join(__dirname, 'database.db');
let db;

// Prepared statements (lazy-initialized)
let stmts = {};

// Globals
let api = null;
let ASSETS = [];
let stats = { swaps: 0, transfers: 0, bridges: 0, liquidity: 0, fees: 0, extrinsics: 0, blocks: 0, skipped: 0 };
let startTime = Date.now();

// Asset IDs
const XOR_ID = '0x0200000000000000000000000000000000000000000000000000000000000000';
const DAI_ID = '0x0200060000000000000000000000000000000000000000000000000000000000';
const XSTUS_ID = '0x0200080000000000000000000000000000000000000000000000000000000000';
const XST_ID = '0x0200090000000000000000000000000000000000000000000000000000000000';

// --- DATABASE FUNCTIONS ---
function initDB() {
    db = new Database(DB_PATH);

    // Performance pragmas - WAL allows concurrent reads from web server
    db.pragma('journal_mode = WAL');
    db.pragma('synchronous = NORMAL');
    db.pragma('cache_size = -32000');    // 32MB cache
    db.pragma('temp_store = MEMORY');
    db.pragma('mmap_size = 134217728');  // 128MB mmap
    console.log('💾 Database connected with WAL mode.');

    // Create tables
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

    db.exec(`CREATE TABLE IF NOT EXISTS fees (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        timestamp INTEGER,
        block INTEGER,
        type TEXT,
        amount REAL,
        usd_value REAL
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

    // Indices
    db.exec(`
        CREATE INDEX IF NOT EXISTS idx_swaps_timestamp ON swaps(timestamp);
        CREATE INDEX IF NOT EXISTS idx_swaps_block ON swaps(block);
        CREATE INDEX IF NOT EXISTS idx_swaps_in_symbol ON swaps(in_symbol);
        CREATE INDEX IF NOT EXISTS idx_swaps_out_symbol ON swaps(out_symbol);
        CREATE INDEX IF NOT EXISTS idx_swaps_wallet ON swaps(wallet);
        CREATE INDEX IF NOT EXISTS idx_transfers_timestamp ON transfers(timestamp);
        CREATE INDEX IF NOT EXISTS idx_transfers_from ON transfers(from_addr);
        CREATE INDEX IF NOT EXISTS idx_transfers_to ON transfers(to_addr);
        CREATE INDEX IF NOT EXISTS idx_bridges_timestamp ON bridges(timestamp);
        CREATE INDEX IF NOT EXISTS idx_bridges_sender ON bridges(sender);
        CREATE INDEX IF NOT EXISTS idx_bridges_block ON bridges(block);
        CREATE INDEX IF NOT EXISTS idx_fees_timestamp ON fees(timestamp);
        CREATE INDEX IF NOT EXISTS idx_liquidity_timestamp ON liquidity_events(timestamp);
        CREATE INDEX IF NOT EXISTS idx_liquidity_wallet ON liquidity_events(wallet);
        CREATE INDEX IF NOT EXISTS idx_transfers_block ON transfers(block);
        CREATE INDEX IF NOT EXISTS idx_liquidity_block ON liquidity_events(block);
        CREATE INDEX IF NOT EXISTS idx_extrinsics_timestamp ON extrinsics(timestamp);
        CREATE INDEX IF NOT EXISTS idx_extrinsics_block ON extrinsics(block);
        CREATE INDEX IF NOT EXISTS idx_extrinsics_section ON extrinsics(section);
        CREATE INDEX IF NOT EXISTS idx_extrinsics_signer ON extrinsics(signer);
        CREATE INDEX IF NOT EXISTS idx_extrinsics_section_timestamp ON extrinsics(section, timestamp);
    `);

    // Prepare statements (compiled once, reused for every insert)
    stmts.insertSwap = db.prepare(`INSERT INTO swaps (timestamp, formatted_time, block, wallet, in_symbol, in_amount, in_logo, in_usd, out_symbol, out_amount, out_logo, out_usd, hash, extrinsic_id)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`);

    stmts.insertTransfer = db.prepare(`INSERT INTO transfers (timestamp, formatted_time, from_addr, to_addr, amount, symbol, logo, usd_value, asset_id, block, hash, extrinsic_id)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`);

    stmts.insertBridge = db.prepare(`INSERT INTO bridges (timestamp, block, network, direction, sender, recipient, asset_id, symbol, logo, amount, usd_value, hash, extrinsic_id)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`);

    stmts.insertLiquidity = db.prepare(`INSERT INTO liquidity_events (timestamp, block, wallet, pool_base, pool_target, base_amount, target_amount, usd_value, type, hash, extrinsic_id)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`);

    stmts.insertFee = db.prepare(`INSERT INTO fees (timestamp, block, type, amount, usd_value)
        VALUES (?, ?, ?, ?, ?)`);

    stmts.insertExtrinsic = db.prepare(`INSERT INTO extrinsics (timestamp, formatted_time, block, extrinsic_index, hash, section, method, signer, success, args_json)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`);

    stmts.checkBlockSwaps = db.prepare('SELECT 1 FROM swaps WHERE block = ? LIMIT 1');
    stmts.checkBlockTransfers = db.prepare('SELECT 1 FROM transfers WHERE block = ? LIMIT 1');
    stmts.checkBlockBridges = db.prepare('SELECT 1 FROM bridges WHERE block = ? LIMIT 1');
    stmts.checkBlockLiquidity = db.prepare('SELECT 1 FROM liquidity_events WHERE block = ? LIMIT 1');
    stmts.checkBlockExtrinsics = db.prepare('SELECT 1 FROM extrinsics WHERE block = ? LIMIT 1');

    console.log('✅ Tables, indices, and prepared statements ready.');
}

// Batch transaction wrapper - all inserts for a block run in one transaction
const runInTransaction = db ? undefined : null; // initialized after db
function withTransaction(fn) {
    const transaction = db.transaction(fn);
    return transaction();
}

// Check if block already processed (checks ALL tables, not just swaps)
function blockAlreadyProcessed(blockNumber) {
    return !!(
        stmts.checkBlockExtrinsics.get(blockNumber) ||
        stmts.checkBlockSwaps.get(blockNumber) ||
        stmts.checkBlockTransfers.get(blockNumber) ||
        stmts.checkBlockBridges.get(blockNumber) ||
        stmts.checkBlockLiquidity.get(blockNumber)
    );
}

// Insert functions (synchronous - called inside transactions)
function insertExtrinsicRecord(e) {
    stmts.insertExtrinsic.run(
        e.timestamp, e.formatted_time, e.block, e.extrinsic_index,
        e.hash || '', e.section || '', e.method || '',
        e.signer || 'System', e.success ? 1 : 0, e.args_json || '{}'
    );
    stats.extrinsics++;
}

function insertSwap(swap) {
    const formattedTime = new Date(swap.timestamp).toLocaleString('es-ES');
    stmts.insertSwap.run(swap.timestamp, formattedTime, swap.block, swap.wallet,
        swap.inSymbol, swap.inAmount, swap.inLogo || '', swap.inUsd || 0,
        swap.outSymbol, swap.outAmount, swap.outLogo || '', swap.outUsd || 0,
        swap.hash || '', swap.extrinsic_id || '');
    stats.swaps++;
}

function insertTransfer(t) {
    const formattedTime = new Date(t.timestamp).toLocaleString('es-ES');
    stmts.insertTransfer.run(t.timestamp, formattedTime, t.from, t.to,
        t.amount, t.symbol, t.logo || '', t.usdValue || 0,
        t.assetId || '', t.block, t.hash || '', t.extrinsic_id || '');
    stats.transfers++;
}

function insertBridge(b) {
    stmts.insertBridge.run(b.timestamp, b.block, b.network, b.direction,
        b.sender, b.recipient, b.assetId, b.symbol || 'UNK', b.logo || '',
        b.amount, b.usdValue || 0, b.hash || '', b.extrinsic_id || '');
    stats.bridges++;
}

function insertLiquidity(l) {
    stmts.insertLiquidity.run(l.timestamp, l.block, l.wallet,
        l.poolBase, l.poolTarget, l.baseAmount, l.targetAmount,
        l.usdValue || 0, l.type, l.hash || '', l.extrinsic_id || '');
    stats.liquidity++;
}

function insertFee(f) {
    let type = 'Other';
    if (f.extrinsicType) {
        if (f.extrinsicType.includes('liquidityProxy') || f.extrinsicType.includes('swap')) type = 'Swap';
        else if (f.extrinsicType.includes('ethBridge') || f.extrinsicType.includes('bridge')) type = 'Bridge';
        else if (f.extrinsicType.includes('assets') || f.extrinsicType.includes('balances')) type = 'Transfer';
    }
    stmts.insertFee.run(f.timestamp, f.block, type, parseFloat(f.feeXor) || 0, f.feeUsd || 0);
    stats.fees++;
}

// --- STATE ---
function loadState() {
    try {
        if (fs.existsSync(STATE_FILE)) return JSON.parse(fs.readFileSync(STATE_FILE, 'utf8'));
    } catch (e) { }
    return { lastProcessedBlock: null, totalBlocksProcessed: 0, startedAt: new Date().toISOString() };
}

function saveState(state) {
    fs.writeFileSync(STATE_FILE, JSON.stringify(state, null, 2));
}

// --- ASSETS ---
async function loadAssets() {
    try {
        const res = await fetch(WHITELIST_URL);
        const data = await res.json();
        ASSETS = data.map(item => ({
            symbol: item.symbol, name: item.name, decimals: item.decimals,
            assetId: item.address, logo: item.icon
        }));
        console.log(`✅ Loaded ${ASSETS.length} assets from whitelist.`);
    } catch (e) {
        ASSETS = [
            { symbol: 'XOR', decimals: 18, assetId: XOR_ID, logo: '' },
            { symbol: 'DAI', decimals: 18, assetId: DAI_ID, logo: '' }
        ];
    }
}

function getAssetInfo(rawId) {
    if (!rawId) return null;
    let str = rawId.toString();
    if (typeof rawId === 'object' && rawId.code) str = rawId.code;
    return ASSETS.find(a => a.assetId === str || a.assetId?.toLowerCase() === str?.toLowerCase());
}

// --- PRICE CALCULATION (RESERVE BASED) ---
async function getPriceInDaiAtBlock(assetId, decimals, blockHash) {
    if (assetId === DAI_ID) return 1;
    try {
        const apiAt = await api.at(blockHash);

        if (assetId === XST_ID) {
            const xstusdPrice = await getPriceInDaiAtBlock(XSTUS_ID, 18, blockHash);
            if (xstusdPrice === 0) return 0;
            const reserves = await apiAt.query.poolXYK.reserves(XSTUS_ID, XST_ID);
            const jsonData = reserves.toJSON();
            if (jsonData && jsonData.length >= 2) {
                const baseRes = new BigNumber(jsonData[0]);
                const targetRes = new BigNumber(jsonData[1]);
                if (!targetRes.isZero()) {
                    return baseRes.div('1e18').div(targetRes.div('1e18')).toNumber() * xstusdPrice;
                }
            }
        }

        const xorPrice = await getXorPriceInDai(apiAt);
        if (assetId === XOR_ID) return xorPrice;
        if (xorPrice === 0) return 0;

        const tokenPriceInXor = await getTokenPriceInXor(apiAt, assetId, decimals);
        return tokenPriceInXor * xorPrice;
    } catch (e) {
        return 0;
    }
}

async function getXorPriceInDai(apiAt) {
    try {
        const reserves = await apiAt.query.poolXYK.reserves(XOR_ID, DAI_ID);
        const jsonData = reserves.toJSON();
        if (!jsonData || jsonData.length < 2) return 0;
        const xorRes = new BigNumber(jsonData[0]);
        const daiRes = new BigNumber(jsonData[1]);
        if (xorRes.isZero()) return 0;
        return daiRes.div(xorRes).toNumber();
    } catch (e) { return 0; }
}

async function getTokenPriceInXor(apiAt, assetId, tokenDecimals) {
    try {
        const reserves = await apiAt.query.poolXYK.reserves(XOR_ID, assetId);
        const jsonData = reserves.toJSON();
        if (!jsonData || jsonData.length < 2) return 0;
        const xorRes = new BigNumber(jsonData[0]);
        const tokenRes = new BigNumber(jsonData[1]);
        if (tokenRes.isZero()) return 0;
        return xorRes.div('1e18').div(tokenRes.div(new BigNumber(10).pow(tokenDecimals))).toNumber();
    } catch (e) { return 0; }
}

// --- BLOCK PROCESSING ---
async function processBlock(blockNumber) {
    try {
        if (blockAlreadyProcessed(blockNumber)) {
            stats.skipped++;
            stats.blocks++;
            return true;
        }

        const blockHash = await api.rpc.chain.getBlockHash(blockNumber);
        const [signedBlock, allEvents, timestamp] = await Promise.all([
            api.rpc.chain.getBlock(blockHash),
            api.query.system.events.at(blockHash),
            api.query.timestamp.now.at(blockHash)
        ]);

        const blockTimestamp = timestamp.toNumber();

        // Collect all inserts for this block, then write in one transaction
        const pendingSwaps = [];
        const pendingTransfers = [];
        const pendingBridges = [];
        const pendingLiquidity = [];
        const pendingFees = [];

        // --- SWAPS ---
        const swapEvents = allEvents.filter(({ event }) =>
            event.section === 'liquidityProxy' && event.method === 'Exchange'
        );

        for (const { event, phase } of swapEvents) {
            try {
                const data = event.data.toJSON();
                const wallet = data[0];
                const inAssetId = data[2]?.code || data[2];
                const outAssetId = data[3]?.code || data[3];
                const inAmountRaw = data[4];
                const outAmountRaw = data[5];

                const inAsset = getAssetInfo(inAssetId);
                const outAsset = getAssetInfo(outAssetId);

                if (inAsset && outAsset) {
                    const inDecimals = inAsset.decimals || 18;
                    const outDecimals = outAsset.decimals || 18;
                    const inAmount = new BigNumber(inAmountRaw).div(new BigNumber(10).pow(inDecimals));
                    const outAmount = new BigNumber(outAmountRaw).div(new BigNumber(10).pow(outDecimals));

                    let inPrice = 0, outPrice = 0;
                    try {
                        inPrice = await getPriceInDaiAtBlock(inAssetId, inDecimals, blockHash);
                        outPrice = await getPriceInDaiAtBlock(outAssetId, outDecimals, blockHash);
                    } catch (e) { }

                    if (inPrice === 0 || outPrice === 0) continue;

                    let hash = '', extrinsic_id = '';
                    if (phase && phase.isApplyExtrinsic) {
                        const exIdx = phase.asApplyExtrinsic.toNumber();
                        const ex = signedBlock.block.extrinsics[exIdx];
                        if (ex) { hash = ex.hash.toHex(); extrinsic_id = `${blockNumber}-${exIdx}`; }
                    }

                    pendingSwaps.push({
                        timestamp: blockTimestamp, block: blockNumber, wallet,
                        inSymbol: inAsset.symbol, inAmount: inAmount.toFixed(4), inLogo: inAsset.logo,
                        inUsd: inAmount.times(inPrice).toNumber(),
                        outSymbol: outAsset.symbol, outAmount: outAmount.toFixed(4), outLogo: outAsset.logo,
                        outUsd: outAmount.times(outPrice).toNumber(),
                        hash, extrinsic_id
                    });
                }
            } catch (e) { }
        }

        // --- TRANSFERS ---
        const transferEvents = allEvents.filter(({ event }) =>
            (event.section === 'balances' && event.method === 'Transfer') ||
            (event.section === 'tokens' && event.method === 'Transfer')
        );

        for (const { event, phase } of transferEvents) {
            try {
                const data = event.data.toJSON();
                let from, to, amountRaw, assetId;

                if (event.section === 'balances') {
                    from = data[0]; to = data[1]; amountRaw = data[2];
                    assetId = XOR_ID;
                } else {
                    assetId = data[0]?.code || data[0];
                    from = data[1]; to = data[2]; amountRaw = data[3];
                }

                if (typeof from === 'string' && from.startsWith('cnTQ')) continue;
                if (typeof to === 'string' && to.startsWith('cnTQ')) continue;

                const asset = getAssetInfo(assetId);
                if (asset) {
                    const decimals = asset.decimals || 18;
                    const amountNum = new BigNumber(amountRaw).div(new BigNumber(10).pow(decimals));
                    const price = await getPriceInDaiAtBlock(assetId, decimals, blockHash);
                    const usdValue = amountNum.times(price).toNumber();

                    let hash = '', extrinsic_id = '';
                    if (phase && phase.isApplyExtrinsic) {
                        const exIdx = phase.asApplyExtrinsic.toNumber();
                        const ex = signedBlock.block.extrinsics[exIdx];
                        if (ex) { hash = ex.hash.toHex(); extrinsic_id = `${blockNumber}-${exIdx}`; }
                    }

                    pendingTransfers.push({
                        timestamp: blockTimestamp, block: blockNumber,
                        from, to, amount: amountNum.toFixed(4),
                        symbol: asset.symbol, logo: asset.logo, usdValue, assetId,
                        hash, extrinsic_id
                    });
                }
            } catch (e) { }
        }

        // --- OUTGOING BRIDGES ---
        for (let i = 0; i < signedBlock.block.extrinsics.length; i++) {
            try {
                const ex = signedBlock.block.extrinsics[i];
                const decoded = ex.toHuman();
                if (!decoded || !decoded.method) continue;

                const { section, method, args } = decoded.method;
                if (section === 'ethBridge' && method === 'transferToSidechain') {
                    const extrinsicEvents = allEvents.filter(({ phase }) =>
                        phase.isApplyExtrinsic && phase.asApplyExtrinsic.toNumber() === i
                    );
                    const isSuccess = extrinsicEvents.some(({ event }) =>
                        event.section === 'system' && event.method === 'ExtrinsicSuccess'
                    );

                    if (isSuccess) {
                        let assetId = args.asset_id || args[0];
                        let recipient = args.to || args[1];
                        let amount = args.amount || args[2];

                        if (typeof assetId === 'object' && assetId?.code) assetId = assetId.code;
                        if (typeof assetId === 'object' && assetId?.assetId) assetId = assetId.assetId;

                        const assetInfo = getAssetInfo(assetId);
                        const decimals = assetInfo?.decimals || 18;
                        const rawAmount = typeof amount === 'string' ? amount.replace(/,/g, '') : String(amount);
                        const amountNum = new BigNumber(rawAmount).div(new BigNumber(10).pow(decimals));

                        if (amountNum.gt(0)) {
                            const price = await getPriceInDaiAtBlock(assetId, decimals, blockHash);
                            pendingBridges.push({
                                timestamp: blockTimestamp, block: blockNumber,
                                network: 'Ethereum', direction: 'Outgoing',
                                sender: decoded.signer || 'Unknown', recipient: recipient || 'Unknown',
                                assetId: assetId || '', symbol: assetInfo?.symbol || 'UNK',
                                logo: assetInfo?.logo || '', amount: amountNum.toFixed(4),
                                usdValue: amountNum.times(price).toNumber(),
                                hash: ex.hash.toHex(), extrinsic_id: `${blockNumber}-${i}`
                            });
                        }
                    }
                }
            } catch (e) { }
        }

        // --- INCOMING BRIDGES ---
        const incomingEvents = allEvents.filter(({ event }) =>
            event.section === 'ethBridge' && event.method === 'IncomingRequestFinalized'
        );

        for (const { event, phase } of incomingEvents) {
            try {
                const hash = event.data[0].toString();
                let ethSender = null;
                if (phase.isApplyExtrinsic) {
                    const exIndex = phase.asApplyExtrinsic.toNumber();
                    const registeredEvent = allEvents.find(r =>
                        r.phase.isApplyExtrinsic &&
                        r.phase.asApplyExtrinsic.toNumber() === exIndex &&
                        r.event.section === 'ethBridge' &&
                        r.event.method === 'RequestRegistered'
                    );
                    if (registeredEvent) {
                        const ethTxHash = registeredEvent.event.data[0].toString();
                        ethSender = await resolveEthSender(ethTxHash);
                    }
                }

                if (api.query.ethBridge && api.query.ethBridge.requests) {
                    try {
                        const req = await api.query.ethBridge.requests.at(blockHash, 0, hash);
                        const json = req.toJSON();

                        let transferData = null;
                        if (Array.isArray(json)) transferData = json[0]?.transfer;
                        else if (json?.transfer) transferData = json.transfer;
                        else if (json?.incoming?.[0]?.transfer) transferData = json.incoming[0].transfer;

                        if (transferData) {
                            const assetId = transferData.assetId?.code || transferData.assetId || '';
                            const recipient = transferData.to || '';
                            const amount = transferData.amount;

                            if (amount && amount !== '0' && amount !== 0) {
                                const asset = getAssetInfo(assetId);
                                const decimals = asset?.decimals || 18;
                                const amountNum = new BigNumber(String(amount)).div(new BigNumber(10).pow(decimals));

                                if (amountNum.gt(0)) {
                                    const price = await getPriceInDaiAtBlock(assetId, decimals, blockHash);
                                    pendingBridges.push({
                                        timestamp: blockTimestamp, block: blockNumber,
                                        network: 'Ethereum', direction: 'Incoming',
                                        sender: ethSender || transferData.from || 'Ethereum',
                                        recipient: recipient || 'Unknown',
                                        assetId: assetId || '', symbol: asset?.symbol || 'UNK',
                                        logo: asset?.logo || '', amount: amountNum.toFixed(4),
                                        usdValue: amountNum.times(price).toNumber(),
                                        hash, extrinsic_id: 'ETH'
                                    });
                                }
                            }
                        }
                    } catch (e) { }
                }
            } catch (e) { }
        }

        // --- LIQUIDITY (from events) ---
        const liquidityEvents = allEvents.filter(({ event }) =>
            event.section === 'poolXYK' && (event.method === 'LiquidityDeposited' || event.method === 'LiquidityWithdrawn')
        );

        for (const { event, phase } of liquidityEvents) {
            try {
                const data = event.data.toJSON();
                const wallet = data[0];
                const baseAssetId = data[2]?.code || data[2];
                const targetAssetId = data[3]?.code || data[3];

                const baseAsset = getAssetInfo(baseAssetId);
                const targetAsset = getAssetInfo(targetAssetId);

                if (baseAsset && targetAsset) {
                    const baseNum = new BigNumber(data[4]).div(new BigNumber(10).pow(baseAsset.decimals || 18));
                    const targetNum = new BigNumber(data[5]).div(new BigNumber(10).pow(targetAsset.decimals || 18));

                    const basePrice = await getPriceInDaiAtBlock(baseAssetId, baseAsset.decimals || 18, blockHash);
                    const targetPrice = await getPriceInDaiAtBlock(targetAssetId, targetAsset.decimals || 18, blockHash);

                    let hash = '', extrinsic_id = '';
                    if (phase && phase.isApplyExtrinsic) {
                        const exIdx = phase.asApplyExtrinsic.toNumber();
                        const ex = signedBlock.block.extrinsics[exIdx];
                        if (ex) { hash = ex.hash.toHex(); extrinsic_id = `${blockNumber}-${exIdx}`; }
                    }

                    pendingLiquidity.push({
                        timestamp: blockTimestamp, block: blockNumber, wallet,
                        poolBase: baseAsset.symbol, poolTarget: targetAsset.symbol,
                        baseAmount: baseNum.toFixed(4), targetAmount: targetNum.toFixed(4),
                        usdValue: baseNum.times(basePrice).plus(targetNum.times(targetPrice)).toNumber(),
                        type: event.method === 'LiquidityDeposited' ? 'add' : 'remove',
                        hash, extrinsic_id
                    });
                }
            } catch (e) { }
        }

        // --- LIQUIDITY (from extrinsics) ---
        for (let i = 0; i < signedBlock.block.extrinsics.length; i++) {
            const ex = signedBlock.block.extrinsics[i];
            const { method: { section, method } } = ex;

            if (section === 'poolXYK' && (method === 'depositLiquidity' || method === 'withdrawLiquidity')) {
                try {
                    const extrinsicEvents = allEvents.filter(({ phase }) =>
                        phase.isApplyExtrinsic && phase.asApplyExtrinsic.toNumber() === i
                    );
                    if (!extrinsicEvents.some(({ event }) =>
                        event.section === 'system' && event.method === 'ExtrinsicSuccess'
                    )) continue;

                    const args = ex.method.args;
                    let baseAssetId = args[1].toJSON()?.code || args[1].toString();
                    let targetAssetId = args[2].toJSON()?.code || args[2].toString();
                    const wallet = ex.signer.toString();

                    let baseAmount = '0', targetAmount = '0';

                    const tokTransfers = extrinsicEvents.filter(({ event }) =>
                        event.section === 'tokens' && event.method === 'Transfer'
                    );
                    for (const { event } of tokTransfers) {
                        const tData = event.data;
                        let currencyId = tData[0].toString();
                        try {
                            const cJson = tData[0].toJSON();
                            if (cJson && cJson.code) currencyId = cJson.code;
                        } catch (e) { }
                        if (currencyId.startsWith('{') && currencyId.includes('code')) {
                            try { currencyId = JSON.parse(currencyId).code || currencyId; } catch (e) { }
                        }
                        const amt = tData[3].toString();
                        if (currencyId.toLowerCase() === targetAssetId.toLowerCase()) targetAmount = amt;
                        if (currencyId.toLowerCase() === baseAssetId.toLowerCase()) baseAmount = amt;
                    }

                    const balTransfers = extrinsicEvents.filter(({ event }) =>
                        event.section === 'balances' && event.method === 'Transfer'
                    );
                    if (balTransfers.length > 0) baseAmount = balTransfers[0].event.data[2].toString();

                    const baseInfo = getAssetInfo(baseAssetId);
                    const targetInfo = getAssetInfo(targetAssetId);
                    const baseDecimals = baseInfo?.decimals || 18;
                    const targetDecimals = targetInfo?.decimals || 18;
                    const baseAmountNum = new BigNumber(baseAmount).div(new BigNumber(10).pow(baseDecimals)).toNumber();
                    const targetAmountNum = new BigNumber(targetAmount).div(new BigNumber(10).pow(targetDecimals)).toNumber();

                    const basePrice = await getPriceInDaiAtBlock(baseAssetId, baseDecimals, blockHash);
                    const targetPrice = await getPriceInDaiAtBlock(targetAssetId, targetDecimals, blockHash);

                    pendingLiquidity.push({
                        timestamp: blockTimestamp, block: blockNumber, wallet,
                        poolBase: baseInfo?.symbol || baseAssetId.slice(0, 10),
                        poolTarget: targetInfo?.symbol || targetAssetId.slice(0, 10),
                        baseAmount: baseAmountNum.toFixed(4), targetAmount: targetAmountNum.toFixed(4),
                        usdValue: (baseAmountNum * basePrice) + (targetAmountNum * targetPrice),
                        type: method === 'depositLiquidity' ? 'deposit' : 'withdraw',
                        hash: ex.hash.toHex(), extrinsic_id: `${blockNumber}-${i}`
                    });
                } catch (e) { }
            }
        }

        // --- NETWORK FEES ---
        const feeEvents = allEvents.filter(({ event }) =>
            event.section === 'transactionPayment' && event.method === 'TransactionFeePaid'
        );

        for (const { event, phase } of feeEvents) {
            try {
                const data = event.data.toJSON();
                const feeRaw = data.actual_fee || data.actualFee || data[1];

                if (feeRaw && feeRaw !== '0' && feeRaw !== 0) {
                    const feeNum = new BigNumber(String(feeRaw)).div(new BigNumber(10).pow(18));
                    const xorPrice = await getPriceInDaiAtBlock(XOR_ID, 18, blockHash);

                    let extrinsicType = '';
                    if (phase.isApplyExtrinsic) {
                        try {
                            const idx = phase.asApplyExtrinsic.toNumber();
                            const ex = signedBlock.block.extrinsics[idx];
                            if (ex) {
                                const decoded = ex.toHuman();
                                if (decoded?.method) extrinsicType = `${decoded.method.section}.${decoded.method.method}`;
                            }
                        } catch (e) { }
                    }

                    pendingFees.push({
                        timestamp: blockTimestamp, block: blockNumber,
                        feeXor: feeNum.toFixed(8), feeUsd: feeNum.times(xorPrice).toNumber(),
                        extrinsicType
                    });
                }
            } catch (e) { }
        }

        // --- RAW EXTRINSICS ---
        const pendingExtrinsics = [];
        for (let i = 0; i < signedBlock.block.extrinsics.length; i++) {
            try {
                const ex = signedBlock.block.extrinsics[i];
                const decoded = ex.toHuman();
                if (!decoded || !decoded.method) continue;

                const { section, method } = decoded.method;

                const extrinsicEvents = allEvents.filter(({ phase }) =>
                    phase.isApplyExtrinsic && phase.asApplyExtrinsic.toNumber() === i
                );
                const isSuccess = extrinsicEvents.some(({ event }) =>
                    event.section === 'system' && event.method === 'ExtrinsicSuccess'
                );

                let errorMsg = '';
                if (!isSuccess) {
                    const failedEvent = extrinsicEvents.find(({ event }) =>
                        event.section === 'system' && event.method === 'ExtrinsicFailed'
                    );
                    if (failedEvent) {
                        try {
                            const dispatchError = failedEvent.event.data[0];
                            if (dispatchError.isModule) {
                                const decoded2 = api.registry.findMetaError(dispatchError.asModule);
                                errorMsg = `${decoded2.section}.${decoded2.name}: ${decoded2.docs.join(' ')}`;
                            } else {
                                errorMsg = dispatchError.toString();
                            }
                        } catch (e) { errorMsg = 'Unknown error'; }
                    }
                }

                const signer = decoded.signer?.Id || decoded.signer || 'System';

                let argsJson = '{}';
                try {
                    const argsStr = JSON.stringify(decoded.method.args || {});
                    argsJson = argsStr.length > 2048 ? argsStr.substring(0, 2048) + '...' : argsStr;
                } catch (e) { argsJson = '{}'; }

                const formattedTime = new Date(blockTimestamp).toLocaleString('es-ES');

                pendingExtrinsics.push({
                    timestamp: blockTimestamp,
                    formatted_time: formattedTime,
                    block: blockNumber,
                    extrinsic_index: i,
                    hash: ex.hash.toHex(),
                    section,
                    method,
                    signer: typeof signer === 'string' ? signer : 'System',
                    success: isSuccess,
                    args_json: argsJson,
                    error_msg: errorMsg
                });
            } catch (e) { /* skip malformed extrinsic */ }
        }

        // --- BATCH WRITE: all inserts for this block in ONE transaction ---
        const totalInserts = pendingSwaps.length + pendingTransfers.length +
            pendingBridges.length + pendingLiquidity.length + pendingFees.length + pendingExtrinsics.length;

        if (totalInserts > 0) {
            withTransaction(() => {
                for (const s of pendingSwaps) insertSwap(s);
                for (const t of pendingTransfers) insertTransfer(t);
                for (const b of pendingBridges) insertBridge(b);
                for (const l of pendingLiquidity) insertLiquidity(l);
                for (const f of pendingFees) insertFee(f);
                for (const e of pendingExtrinsics) insertExtrinsicRecord(e);
            });
        }

        stats.blocks++;
        return true;
    } catch (e) {
        console.error(`❌ Error processing block ${blockNumber}:`, e.message);
        return false;
    }
}

// --- MAIN LOOP ---
async function runBackfill() {
    console.log('🚀 Starting Historical Indexer (Optimized)...');
    console.log(`📡 Connecting to ${WS_ENDPOINT}...`);

    initDB();
    await loadAssets();

    const provider = new WsProvider(WS_ENDPOINT);
    api = await ApiPromise.create(options({ provider }));
    await api.isReady;
    console.log('✅ Connected to blockchain.');

    const header = await api.rpc.chain.getHeader();
    const currentBlock = header.number.toNumber();
    console.log(`📊 Current block: ${currentBlock.toLocaleString()}`);

    let state = loadState();
    const safeMaxBlock = currentBlock - SAFETY_OFFSET;
    let startBlock = state.lastProcessedBlock !== null
        ? Math.min(state.lastProcessedBlock - 1, safeMaxBlock)
        : safeMaxBlock;

    console.log(`🔄 Starting from block ${startBlock.toLocaleString()} backwards.`);
    console.log(`⚡ Optimized: better-sqlite3 + WAL + batch transactions`);

    startTime = Date.now();
    let blocksProcessedThisSession = 0;

    for (let block = startBlock; block >= 1; block -= BLOCKS_PER_BATCH) {
        const batchEnd = Math.max(block - BLOCKS_PER_BATCH + 1, 1);

        for (let b = block; b >= batchEnd; b--) {
            await processBlock(b);
            blocksProcessedThisSession++;
            state.lastProcessedBlock = b;
            state.totalBlocksProcessed++;

            if (blocksProcessedThisSession % 100 === 0) {
                const elapsed = (Date.now() - startTime) / 1000;
                const speed = blocksProcessedThisSession / elapsed;
                const eta = b / speed / 3600;
                console.log(`📈 Block ${b.toLocaleString()} | ${speed.toFixed(1)} blk/s | ETA: ${eta.toFixed(1)}h | S:${stats.swaps} T:${stats.transfers} B:${stats.bridges} F:${stats.fees} E:${stats.extrinsics} | Skip:${stats.skipped}`);
            }

            if (blocksProcessedThisSession % PROGRESS_SAVE_INTERVAL === 0) {
                saveState(state);
            }
        }
        await new Promise(r => setTimeout(r, DELAY_BETWEEN_BATCHES_MS));
    }

    saveState(state);
    console.log('✅ Backfill complete!');
    db.close();
    process.exit(0);
}

// Graceful shutdown
process.on('SIGINT', () => {
    console.log('\n🛑 SIGINT received. Saving state...');
    try {
        const state = loadState();
        saveState(state);
        if (db) db.close();
    } catch (e) { }
    process.exit(0);
});

process.on('SIGTERM', () => {
    console.log('\n🛑 SIGTERM received. Saving state...');
    try {
        const state = loadState();
        saveState(state);
        if (db) db.close();
    } catch (e) { }
    process.exit(0);
});

runBackfill().catch(e => { console.error('Fatal:', e); process.exit(1); });
