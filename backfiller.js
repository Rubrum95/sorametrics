// backfiller.js - Historical Blockchain Indexer for SoraMetrics
// Processes blocks from newest to oldest, extracting swaps, transfers, bridges with HISTORICAL PRICES

const { ApiPromise, WsProvider } = require('@polkadot/api');
const { options } = require('@sora-substrate/api');
const { resolveEthSender } = require('./eth_helper');
const BigNumber = require('bignumber.js');
const fs = require('fs');
const path = require('path');

// Configuration
const { WS_ENDPOINT_BACKFILL, WHITELIST_URL } = require('./config');
const WS_ENDPOINT = WS_ENDPOINT_BACKFILL; // Override local with archive for backfiller
const STATE_FILE = path.join(__dirname, 'backfill_state.json');
const BLOCKS_PER_BATCH = 100;
const DELAY_BETWEEN_BATCHES_MS = 500;
const PROGRESS_SAVE_INTERVAL = 500;
const SAFETY_OFFSET = 10; // Process closer to head (was 1000)

// Database
const sqlite3 = require('sqlite3').verbose();
const DB_PATH = path.join(__dirname, 'database_history_v2.db'); // New file to avoid I/O locks on old one
let db;

// Globals
let api = null;
let ASSETS = [];
let stats = { swaps: 0, transfers: 0, bridges: 0, liquidity: 0, fees: 0, blocks: 0, skipped: 0 };
let startTime = Date.now();

// DAI Asset ID for price calculation
const DAI_ID = '0x0200060000000000000000000000000000000000000000000000000000000000';

// --- DATABASE FUNCTIONS ---
function initDB() {
    return new Promise((resolve, reject) => {
        db = new sqlite3.Database(DB_PATH, async (err) => {
            if (err) return reject(err);
            console.log('💾 Database connected.');

            // Enable WAL mode for concurrent reading - DISABLED for stability
            // db.run('PRAGMA journal_mode = WAL;', (err) => {
            //     if (err) console.warn('⚠️ Could not set WAL mode:', err.message);
            //     else console.log('✅ WAL mode enabled for concurrency.');
            // });

            try {
                await run('ALTER TABLE transfers ADD COLUMN block INTEGER');
                console.log('✅ Added block column to transfers table.');
            } catch (e) { /* Column likely exists, ignore */ }

            try {
                await createTables();
                console.log('✅ Tables initialization checked.');
            } catch (e) {
                console.error('❌ FATAL: Failed to create tables:', e.message);
                throw e;
            }
            resolve();
        });
    });
}


async function createTables() {
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

    await run(`CREATE TABLE IF NOT EXISTS fees (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        timestamp INTEGER,
        block INTEGER,
        type TEXT,
        amount REAL,
        usd_value REAL
    )`);
    // Indices for performance
    await run(`CREATE INDEX IF NOT EXISTS idx_swaps_timestamp ON swaps(timestamp)`);
    await run(`CREATE INDEX IF NOT EXISTS idx_swaps_block ON swaps(block)`);
    await run(`CREATE INDEX IF NOT EXISTS idx_transfers_timestamp ON transfers(timestamp)`);
    await run(`CREATE INDEX IF NOT EXISTS idx_bridges_timestamp ON bridges(timestamp)`);
    await run(`CREATE INDEX IF NOT EXISTS idx_fees_timestamp ON fees(timestamp)`);
    await run(`CREATE INDEX IF NOT EXISTS idx_liquidity_timestamp ON liquidity_events(timestamp)`);

    console.log('✅ Tables and indices ensured.');
}

function run(sql, params = []) {
    return new Promise((resolve, reject) => {
        db.run(sql, params, function (err) {
            if (err) reject(err);
            else resolve(this);
        });
    });
}

function get(sql, params = []) {
    return new Promise((resolve, reject) => {
        db.get(sql, params, (err, row) => {
            if (err) reject(err);
            else resolve(row);
        });
    });
}

// Check if block already has data in any table (prevents duplicates)
async function blockAlreadyProcessed(blockNumber) {
    try {
        const swap = await get('SELECT 1 FROM swaps WHERE block = ? LIMIT 1', [blockNumber]);
        if (swap) return true;
        return false;
    } catch (e) {
        return false;
    }
}


// --- PRICE CALCULATION (RESERVE BASED - Required for Historical Data) ---
const XOR_ID = '0x0200000000000000000000000000000000000000000000000000000000000000';
const XSTUS_ID = '0x0200080000000000000000000000000000000000000000000000000000000000'; // XSTUSD
const XST_ID = '0x0200090000000000000000000000000000000000000000000000000000000000'; // XST

async function getPriceInDaiAtBlock(assetId, decimals, blockHash) {
    if (assetId === DAI_ID) return 1;

    try {
        const apiAt = await api.at(blockHash);

        // --- SPECIAL CASE: XST ---
        if (assetId === XST_ID) {
            // 1. Get XSTUSD Price in DAI
            const xstusdPrice = await getPriceInDaiAtBlock(XSTUS_ID, 18, blockHash);
            if (xstusdPrice === 0) return 0;

            // 2. Get XST Price relative to XSTUSD (XST-XSTUSD pool)
            // Pair order: XSTUSD (0x020008...) < XST (0x020009...)
            const reserves = await apiAt.query.poolXYK.reserves(XSTUS_ID, XST_ID);
            const jsonData = reserves.toJSON();

            if (jsonData && jsonData.length >= 2) {
                const baseRes = new BigNumber(jsonData[0]); // XSTUSD
                const targetRes = new BigNumber(jsonData[1]); // XST
                if (!targetRes.isZero()) {
                    const baseNormal = baseRes.div('1e18');
                    const targetNormal = targetRes.div('1e18'); // Both 18 dec
                    const priceInXstUsd = baseNormal.div(targetNormal).toNumber();

                    return priceInXstUsd * xstusdPrice;
                }
            }
            // Fallback to XOR pool if XSTUSD pool empty? (Unlikely but safe)
        }
        // -------------------------

        // 1. Get XOR Price in DAI (Anchor)
        const xorPrice = await getXorPriceInDai(apiAt);
        if (assetId === XOR_ID) return xorPrice;
        if (xorPrice === 0) return 0;

        // 2. Get Token Price relative to XOR
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

        // Reserves: [XOR, DAI] or [DAI, XOR]? 
        // Sorted by AssetID. XOR (0x020...00) < DAI (0x020...06)
        // So [0] = XOR, [1] = DAI
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

        // Sort order check: XOR is usually first because of ID structure, unless assetId < XOR_ID
        // But XOR_ID is ...0000, so it's likely first.
        const xorRes = new BigNumber(jsonData[0]);
        const tokenRes = new BigNumber(jsonData[1]);

        if (tokenRes.isZero()) return 0;

        // Price of 1 Token in XOR (Token -> XOR)
        // Constant Product: x * y = k
        // Price = InputReserve / OutputReserve ? No.
        // Spot Price = Output / Input
        // If we want price of Token in XOR: how much XOR for 1 Token?
        // Spot = XOR_Reserves / Token_Reserves

        const xorNormal = xorRes.div('1e18');
        const tokenNormal = tokenRes.div(new BigNumber(10).pow(tokenDecimals));

        return xorNormal.div(tokenNormal).toNumber();
    } catch (e) { return 0; }
}

// Insert functions
async function insertHistoricalSwap(swap) {
    const sql = `INSERT INTO swaps (timestamp, formatted_time, block, wallet, in_symbol, in_amount, in_logo, in_usd, out_symbol, out_amount, out_logo, out_usd, hash, extrinsic_id) 
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`;
    const formattedTime = new Date(swap.timestamp).toLocaleString('es-ES');
    await run(sql, [swap.timestamp, formattedTime, swap.block, swap.wallet, swap.inSymbol, swap.inAmount, swap.inLogo || '', swap.inUsd || 0, swap.outSymbol, swap.outAmount, swap.outLogo || '', swap.outUsd || 0, swap.hash || '', swap.extrinsic_id || '']);
    stats.swaps++;
}

async function insertHistoricalTransfer(t) {
    const sql = `INSERT INTO transfers (timestamp, formatted_time, from_addr, to_addr, amount, symbol, logo, usd_value, asset_id, block, hash, extrinsic_id)
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`;
    const formattedTime = new Date(t.timestamp).toLocaleString('es-ES');
    await run(sql, [t.timestamp, formattedTime, t.from, t.to, t.amount, t.symbol, t.logo || '', t.usdValue || 0, t.assetId || '', t.block, t.hash || '', t.extrinsic_id || '']);
    stats.transfers++;
}

async function insertHistoricalBridge(b) {
    const sql = `INSERT INTO bridges (timestamp, block, network, direction, sender, recipient, asset_id, symbol, logo, amount, usd_value, hash, extrinsic_id)
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`;
    await run(sql, [b.timestamp, b.block, b.network, b.direction, b.sender, b.recipient, b.assetId, b.symbol || 'UNK', b.logo || '', b.amount, b.usdValue || 0, b.hash || '', b.extrinsic_id || '']);
    stats.bridges++;
}

async function insertHistoricalLiquidity(l) {
    const sql = `INSERT INTO liquidity_events (timestamp, block, wallet, pool_base, pool_target, base_amount, target_amount, usd_value, type, hash, extrinsic_id)
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`;
    await run(sql, [l.timestamp, l.block, l.wallet, l.poolBase, l.poolTarget, l.baseAmount, l.targetAmount, l.usdValue || 0, l.type, l.hash || '', l.extrinsic_id || '']);
    stats.liquidity++;
}

async function insertNetworkFee(f) {
    // Unified schema: same as index.js fees table
    // Map extrinsicType to simplified type categories
    let type = 'Other';
    if (f.extrinsicType) {
        if (f.extrinsicType.includes('liquidityProxy') || f.extrinsicType.includes('swap')) type = 'Swap';
        else if (f.extrinsicType.includes('ethBridge') || f.extrinsicType.includes('bridge')) type = 'Bridge';
        else if (f.extrinsicType.includes('assets') || f.extrinsicType.includes('balances')) type = 'Transfer';
    }

    const sql = `INSERT INTO fees (timestamp, block, type, amount, usd_value)
                 VALUES (?, ?, ?, ?, ?)`;
    await run(sql, [f.timestamp, f.block, type, parseFloat(f.feeXor) || 0, f.feeUsd || 0]);
    stats.fees++;
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

async function loadAssets() {
    try {
        const fetch = (...args) => import('node-fetch').then(({ default: f }) => f(...args));
        const res = await fetch(WHITELIST_URL);
        const data = await res.json();
        ASSETS = data.map(item => ({
            symbol: item.symbol, name: item.name, decimals: item.decimals, assetId: item.address, logo: item.icon
        }));
        console.log(`✅ Loaded ${ASSETS.length} assets from whitelist.`);
    } catch (e) {
        ASSETS = [
            { symbol: 'XOR', decimals: 18, assetId: '0x0200000000000000000000000000000000000000000000000000000000000000', logo: '' },
            { symbol: 'DAI', decimals: 18, assetId: '0x0200060000000000000000000000000000000000000000000000000000000000', logo: '' }
        ];
    }
}

function getAssetInfo(rawId) {
    if (!rawId) return null;
    let str = rawId.toString();
    if (typeof rawId === 'object' && rawId.code) str = rawId.code;
    return ASSETS.find(a => a.assetId === str || a.assetId?.toLowerCase() === str?.toLowerCase());
}

// --- BLOCK PROCESSING ---
async function processBlock(blockNumber) {
    try {
        // Skip if block already processed (prevents duplicates)
        if (await blockAlreadyProcessed(blockNumber)) {
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

        // --- SWAPS ---
        const swapEvents = allEvents.filter(({ event }) =>
            event.section === 'liquidityProxy' && event.method === 'Exchange'
        );

        for (const { event, phase } of swapEvents) {
            try {
                const data = event.data.toJSON(); // Use toJSON for consistent parsing
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

                    // --- PRICE CALCULATION (no fake fallbacks) ---
                    let inPrice = 0;
                    let outPrice = 0;

                    try {
                        inPrice = await getPriceInDaiAtBlock(inAssetId, inDecimals, blockHash);
                        outPrice = await getPriceInDaiAtBlock(outAssetId, outDecimals, blockHash);
                    } catch (e) {
                        console.error(`Error fetching price for block ${blockNumber}:`, e.message);
                    }

                    const inUsd = inAmount.times(inPrice).toNumber();
                    const outUsd = outAmount.times(outPrice).toNumber();

                    // CRITICAL: Skip swap if EITHER price is 0 (corrupted data is worse than no data)
                    if (inPrice === 0 || outPrice === 0) {
                        console.warn(`⚠️ Block ${blockNumber}: Skipping swap ${inAsset.symbol}→${outAsset.symbol} (Price 0: In ${inPrice} | Out ${outPrice})`);
                        continue; // Don't save corrupted data
                    }

                    // Extract hash and extrinsic_id from the associated extrinsic
                    let hash = '';
                    let extrinsic_id = '';
                    if (phase && phase.isApplyExtrinsic) {
                        const exIdx = phase.asApplyExtrinsic.toNumber();
                        const ex = signedBlock.block.extrinsics[exIdx];
                        if (ex) {
                            hash = ex.hash.toHex();
                            extrinsic_id = `${blockNumber}-${exIdx}`;
                        }
                    }

                    console.log(`✅ ${blockNumber}: Inserting Swap ${inAsset.symbol} -> ${outAsset.symbol} ($ ${inUsd})`);
                    await insertHistoricalSwap({
                        timestamp: blockTimestamp,
                        block: blockNumber,
                        wallet,
                        inSymbol: inAsset.symbol,
                        inAmount: inAmount.toFixed(4),
                        inLogo: inAsset.logo,
                        inUsd,
                        outSymbol: outAsset.symbol,
                        outAmount: outAmount.toFixed(4),
                        outLogo: outAsset.logo,
                        outUsd,
                        hash,
                        extrinsic_id
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
                const data = event.data.toJSON(); // Use toJSON for consistent parsing
                let from, to, amountRaw, assetId;

                if (event.section === 'balances') {
                    // balances.Transfer: [from, to, amount]
                    from = data[0];
                    to = data[1];
                    amountRaw = data[2];
                    assetId = '0x0200000000000000000000000000000000000000000000000000000000000000';
                } else {
                    // tokens.Transfer: [currency_id, from, to, amount]
                    assetId = data[0]?.code || data[0];
                    from = data[1];
                    to = data[2];
                    amountRaw = data[3];
                }

                // Skip bad addresses logic from index.js
                if (typeof from === 'string' && from.startsWith('cnTQ')) continue;
                if (typeof to === 'string' && to.startsWith('cnTQ')) continue;

                const asset = getAssetInfo(assetId);
                if (asset) {
                    const decimals = asset.decimals || 18;
                    const amountNum = new BigNumber(amountRaw).div(new BigNumber(10).pow(decimals));

                    const price = await getPriceInDaiAtBlock(assetId, decimals, blockHash);
                    const usdValue = amountNum.times(price).toNumber();

                    // Extract hash and extrinsic_id from the associated extrinsic
                    let hash = '';
                    let extrinsic_id = '';
                    if (phase && phase.isApplyExtrinsic) {
                        const exIdx = phase.asApplyExtrinsic.toNumber();
                        const ex = signedBlock.block.extrinsics[exIdx];
                        if (ex) {
                            hash = ex.hash.toHex();
                            extrinsic_id = `${blockNumber}-${exIdx}`;
                        }
                    }

                    console.log(`✅ ${blockNumber}: Inserting Transfer ${asset.symbol} ($ ${usdValue})`);
                    await insertHistoricalTransfer({
                        timestamp: blockTimestamp,
                        block: blockNumber,
                        from: from,
                        to: to,
                        amount: amountNum.toFixed(4),
                        symbol: asset.symbol,
                        logo: asset.logo,
                        usdValue,
                        assetId,
                        hash,
                        extrinsic_id
                    });
                }
            } catch (e) { }
        }

        // --- BRIDGES (Rewritten to match index.js approach) ---

        // 1. OUTGOING BRIDGES via Extrinsics (ethBridge.transferToSidechain)
        for (let i = 0; i < signedBlock.block.extrinsics.length; i++) {
            try {
                const ex = signedBlock.block.extrinsics[i];
                const decoded = ex.toHuman();
                if (!decoded || !decoded.method) continue;

                const { section, method, args } = decoded.method;
                const signer = decoded.signer;

                if (section === 'ethBridge' && method === 'transferToSidechain') {
                    // Check if extrinsic succeeded
                    const extrinsicEvents = allEvents.filter(({ phase }) =>
                        phase.isApplyExtrinsic && phase.asApplyExtrinsic.toNumber() === i
                    );
                    const isSuccess = extrinsicEvents.some(({ event }) =>
                        event.section === 'system' && event.method === 'ExtrinsicSuccess'
                    );

                    if (isSuccess) {
                        console.log(`🌉 ${blockNumber}: Outgoing Bridge Extrinsic detected!`);

                        let assetId = args.asset_id || args[0];
                        let recipient = args.to || args[1];
                        let amount = args.amount || args[2];

                        if (typeof assetId === 'object' && assetId?.code) assetId = assetId.code;
                        if (typeof assetId === 'object' && assetId?.assetId) assetId = assetId.assetId;

                        const assetInfo = getAssetInfo(assetId);
                        const decimals = assetInfo?.decimals || 18;
                        const symbol = assetInfo?.symbol || 'UNK';

                        const rawAmount = typeof amount === 'string' ? amount.replace(/,/g, '') : String(amount);
                        const amountNum = new BigNumber(rawAmount).div(new BigNumber(10).pow(decimals));

                        if (amountNum.gt(0)) {
                            const price = await getPriceInDaiAtBlock(assetId, decimals, blockHash);
                            const usdValue = amountNum.times(price).toNumber();

                            console.log(`✅ ${blockNumber}: Inserting Bridge Outgoing ${symbol} $${usdValue.toFixed(8)}`);
                            await insertHistoricalBridge({
                                timestamp: blockTimestamp,
                                block: blockNumber,
                                network: 'Ethereum',
                                direction: 'Outgoing',
                                sender: signer || 'Unknown',
                                recipient: recipient || 'Unknown',
                                assetId: assetId || '',
                                symbol: symbol,
                                logo: assetInfo?.logo || '',
                                amount: amountNum.toFixed(4),
                                usdValue,
                                hash: ex.hash.toHex(),
                                extrinsic_id: `${blockNumber}-${i}`
                            });
                        }
                    }
                }
            } catch (e) { }
        }

        // 2. INCOMING BRIDGES via Events (ethBridge.IncomingRequestFinalized)
        const incomingEvents = allEvents.filter(({ event }) =>
            event.section === 'ethBridge' && event.method === 'IncomingRequestFinalized'
        );

        for (const { event, phase } of incomingEvents) {
            try {
                const hash = event.data[0].toString();
                console.log(`🌉 ${blockNumber}: Incoming Bridge Finalized (hash: ${hash.substring(0, 18)}...)`);

                // ---------------------------------------------------------
                // NEW: Resolve Ethereum Sender via RequestRegistered Event
                // ---------------------------------------------------------
                let ethSender = null;
                if (phase.isApplyExtrinsic) {
                    const exIndex = phase.asApplyExtrinsic.toNumber();
                    // Find RequestRegistered in the same extrinsic
                    const registeredEvent = allEvents.find(r =>
                        r.phase.isApplyExtrinsic &&
                        r.phase.asApplyExtrinsic.toNumber() === exIndex &&
                        r.event.section === 'ethBridge' &&
                        r.event.method === 'RequestRegistered'
                    );

                    if (registeredEvent) {
                        const ethTxHash = registeredEvent.event.data[0].toString();
                        console.log(`   🔎 Found RequestRegistered with ETH Hash: ${ethTxHash}`);
                        ethSender = await resolveEthSender(ethTxHash);
                        if (ethSender) console.log(`   ✅ Resolved ETH Sender: ${ethSender}`);
                    }
                }
                // ---------------------------------------------------------

                // Try to fetch request details from storage
                if (api.query.ethBridge && api.query.ethBridge.requests) {
                    try {
                        const req = await api.query.ethBridge.requests.at(blockHash, 0, hash);
                        const json = req.toJSON();
                        console.log(`   Request data:`, JSON.stringify(json).substring(0, 150));

                        // Parse the incoming structure: it's an array where first element has 'transfer'
                        let transferData = null;
                        if (Array.isArray(json)) {
                            transferData = json[0]?.transfer;
                        } else if (json?.transfer) {
                            transferData = json.transfer;
                        } else if (json?.incoming?.[0]?.transfer) {
                            transferData = json.incoming[0].transfer;
                        }

                        if (transferData) {
                            // Structure: { from, to, assetId: { code }, amount, ... }
                            const assetId = transferData.assetId?.code || transferData.assetId || '';
                            const recipient = transferData.to || '';
                            const amount = transferData.amount;

                            console.log(`   ✅ Parsed: to=${recipient?.substring(0, 15)}... asset=${assetId?.substring(0, 15)}...`);

                            if (amount && amount !== '0' && amount !== 0) {
                                const asset = getAssetInfo(assetId);
                                const decimals = asset?.decimals || 18;
                                const amountNum = new BigNumber(String(amount)).div(new BigNumber(10).pow(decimals));

                                if (amountNum.gt(0)) {
                                    const price = await getPriceInDaiAtBlock(assetId, decimals, blockHash);
                                    const usdValue = amountNum.times(price).toNumber();

                                    console.log(`✅ ${blockNumber}: Inserting Bridge Incoming ${asset?.symbol || assetId} $${usdValue.toFixed(8)}`);
                                    await insertHistoricalBridge({
                                        timestamp: blockTimestamp,
                                        block: blockNumber,
                                        network: 'Ethereum',
                                        direction: 'Incoming',
                                        sender: ethSender || transferData.from || 'Ethereum', // Use Resolved Sender!
                                        recipient: recipient || 'Unknown',
                                        assetId: assetId || '',
                                        symbol: asset?.symbol || 'UNK',
                                        logo: asset?.logo || '',
                                        amount: amountNum.toFixed(4),
                                        usdValue,
                                        hash: hash, // Ethereum request hash
                                        extrinsic_id: 'ETH' // Mark as Ethereum transaction
                                    });
                                }
                            }
                        } else {
                            console.log(`   ⚠️ Could not parse transfer data`);
                        }
                    } catch (e) {
                        console.log(`   ⚠️ Could not fetch request: ${e.message}`);
                    }
                }
            } catch (e) {
                console.error(`❌ Incoming bridge error:`, e.message);
            }
        }

        // --- LIQUIDITY ---
        const liquidityEvents = allEvents.filter(({ event }) =>
            event.section === 'poolXYK' && (event.method === 'LiquidityDeposited' || event.method === 'LiquidityWithdrawn')
        );

        for (const { event, phase } of liquidityEvents) {
            try {
                const data = event.data.toJSON();
                const wallet = data[0];
                const baseAssetId = data[2]?.code || data[2];
                const targetAssetId = data[3]?.code || data[3];
                const baseAmountRaw = data[4];
                const targetAmountRaw = data[5];

                const baseAsset = getAssetInfo(baseAssetId);
                const targetAsset = getAssetInfo(targetAssetId);

                if (baseAsset && targetAsset) {
                    const baseNum = new BigNumber(baseAmountRaw).div(new BigNumber(10).pow(baseAsset.decimals || 18));
                    const targetNum = new BigNumber(targetAmountRaw).div(new BigNumber(10).pow(targetAsset.decimals || 18));

                    const basePrice = await getPriceInDaiAtBlock(baseAssetId, baseAsset.decimals || 18, blockHash);
                    const targetPrice = await getPriceInDaiAtBlock(targetAssetId, targetAsset.decimals || 18, blockHash);
                    const usdValue = baseNum.times(basePrice).plus(targetNum.times(targetPrice)).toNumber();

                    // Extract hash and extrinsic_id from the associated extrinsic
                    let hash = '';
                    let extrinsic_id = '';
                    if (phase && phase.isApplyExtrinsic) {
                        const exIdx = phase.asApplyExtrinsic.toNumber();
                        const ex = signedBlock.block.extrinsics[exIdx];
                        if (ex) {
                            hash = ex.hash.toHex();
                            extrinsic_id = `${blockNumber}-${exIdx}`;
                        }
                    }

                    await insertHistoricalLiquidity({
                        timestamp: blockTimestamp,
                        block: blockNumber,
                        wallet,
                        poolBase: baseAsset.symbol,
                        poolTarget: targetAsset.symbol,
                        baseAmount: baseNum.toFixed(4),
                        targetAmount: targetNum.toFixed(4),
                        usdValue,
                        type: event.method === 'LiquidityDeposited' ? 'add' : 'remove',
                        hash,
                        extrinsic_id
                    });
                }
            } catch (e) { }
        }

        // --- NETWORK FEES ---
        const feeEvents = allEvents.filter(({ event }) =>
            event.section === 'transactionPayment' && event.method === 'TransactionFeePaid'
        );

        for (const { event, phase } of feeEvents) {
            try {
                const data = event.data.toJSON();
                const wallet = data.who || data[0];
                const feeRaw = data.actual_fee || data.actualFee || data[1];

                if (feeRaw && feeRaw !== '0' && feeRaw !== 0) {
                    const feeNum = new BigNumber(String(feeRaw)).div(new BigNumber(10).pow(18));

                    // Get XOR price at this block
                    const xorPrice = await getPriceInDaiAtBlock(XOR_ID, 18, blockHash);
                    const feeUsd = feeNum.times(xorPrice).toNumber();

                    // Try to get extrinsic type
                    let extrinsicType = '';
                    if (phase.isApplyExtrinsic) {
                        try {
                            const idx = phase.asApplyExtrinsic.toNumber();
                            const ex = signedBlock.block.extrinsics[idx];
                            if (ex) {
                                const decoded = ex.toHuman();
                                if (decoded?.method) {
                                    extrinsicType = `${decoded.method.section}.${decoded.method.method}`;
                                }
                            }
                        } catch (e) { }
                    }

                    await insertNetworkFee({
                        timestamp: blockTimestamp,
                        block: blockNumber,
                        wallet: wallet || '',
                        feeXor: feeNum.toFixed(8),
                        feeUsd,
                        extrinsicType
                    });
                }
            } catch (e) { }
        }

        // --- LIQUIDITY EVENTS (via extrinsics, not events) ---
        for (let i = 0; i < signedBlock.block.extrinsics.length; i++) {
            const ex = signedBlock.block.extrinsics[i];
            const { method: { section, method } } = ex;

            // Check for liquidity deposit/withdraw extrinsics
            if (section === 'poolXYK' && (method === 'depositLiquidity' || method === 'withdrawLiquidity')) {
                try {
                    // Check if extrinsic succeeded by looking for ExtrinsicSuccess event
                    const extrinsicEvents = allEvents.filter(({ phase }) =>
                        phase.isApplyExtrinsic && phase.asApplyExtrinsic.toNumber() === i
                    );
                    const succeeded = extrinsicEvents.some(({ event }) =>
                        event.section === 'system' && event.method === 'ExtrinsicSuccess'
                    );

                    if (!succeeded) continue;

                    // Parse extrinsic args
                    const args = ex.method.args;
                    let baseAssetId = args[1].toJSON()?.code || args[1].toString();
                    let targetAssetId = args[2].toJSON()?.code || args[2].toString();

                    // Get wallet from signer
                    const wallet = ex.signer.toString();

                    // Get actual amounts from transfer events
                    let baseAmount = '0';
                    let targetAmount = '0';

                    const transferEvents = extrinsicEvents.filter(({ event }) =>
                        event.section === 'tokens' && event.method === 'Transfer'
                    );

                    // Find amounts from transfer events
                    for (const { event } of transferEvents) {
                        const data = event.data;

                        // Robust ID Extraction
                        let currencyId = data[0].toString();
                        try {
                            const cJson = data[0].toJSON();
                            if (cJson && cJson.code) currencyId = cJson.code;
                        } catch (e) { }

                        // Fallback: If string looks like JSON (some API versions do this)
                        if (currencyId.startsWith('{') && currencyId.includes('code')) {
                            try {
                                const p = JSON.parse(currencyId);
                                if (p.code) currencyId = p.code;
                            } catch (e) { }
                        }

                        const amount = data[3].toString();

                        // Normalize for comparison
                        const cIdLower = currencyId.toLowerCase();
                        const tIdLower = targetAssetId.toLowerCase();
                        const bIdLower = baseAssetId.toLowerCase();

                        if (cIdLower === tIdLower) {
                            targetAmount = amount;
                        }
                        if (cIdLower === bIdLower) {
                            baseAmount = amount;
                        }
                    }

                    // For base asset (XOR), check balances.Transfer
                    const balanceTransfers = extrinsicEvents.filter(({ event }) =>
                        event.section === 'balances' && event.method === 'Transfer'
                    );

                    if (balanceTransfers.length > 0) {
                        const data = balanceTransfers[0].event.data;
                        baseAmount = data[2].toString();
                    }

                    const type = method === 'depositLiquidity' ? 'deposit' : 'withdraw';

                    // Get asset info
                    const baseInfo = getAssetInfo(baseAssetId);
                    const targetInfo = getAssetInfo(targetAssetId);
                    const baseDecimals = baseInfo?.decimals || 18;
                    const targetDecimals = targetInfo?.decimals || 18;

                    const baseAmountNum = new BigNumber(baseAmount).div(new BigNumber(10).pow(baseDecimals)).toNumber();
                    const targetAmountNum = new BigNumber(targetAmount).div(new BigNumber(10).pow(targetDecimals)).toNumber();

                    // Get prices at block
                    const basePrice = await getPriceInDaiAtBlock(baseAssetId, baseDecimals, blockHash);
                    const targetPrice = await getPriceInDaiAtBlock(targetAssetId, targetDecimals, blockHash);

                    const usdValue = (baseAmountNum * basePrice) + (targetAmountNum * targetPrice);

                    await insertHistoricalLiquidity({
                        timestamp: blockTimestamp,
                        block: blockNumber,
                        wallet,
                        poolBase: baseInfo?.symbol || baseAssetId.slice(0, 10),
                        poolTarget: targetInfo?.symbol || targetAssetId.slice(0, 10),
                        baseAmount: baseAmountNum.toFixed(4),
                        targetAmount: targetAmountNum.toFixed(4),
                        usdValue,
                        type,
                        hash: ex.hash.toHex(),
                        extrinsic_id: `${blockNumber}-${i}`
                    });

                } catch (e) {
                    console.error('Error parsing liquidity extrinsic:', e.message);
                }
            }
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
    console.log('🚀 Starting Historical Indexer (Robust Version)...');
    console.log(`📡 Connecting to ${WS_ENDPOINT}...`);

    await initDB();
    await loadAssets();

    const provider = new WsProvider(WS_ENDPOINT);
    api = await ApiPromise.create(options({ provider }));
    await api.isReady;
    console.log('✅ Connected to blockchain.');

    const header = await api.rpc.chain.getHeader();
    const currentBlock = header.number.toNumber();
    console.log(`📊 Current block: ${currentBlock.toLocaleString()}`);

    let state = loadState();
    // Apply safety offset: never process blocks within SAFETY_OFFSET of current
    const safeMaxBlock = currentBlock - SAFETY_OFFSET;
    let startBlock = state.lastProcessedBlock !== null
        ? Math.min(state.lastProcessedBlock - 1, safeMaxBlock)
        : safeMaxBlock;

    console.log(`🔄 Starting from block ${startBlock.toLocaleString()} backwards (${SAFETY_OFFSET} block safety buffer).`);
    console.log(`💵 Historical prices + Transfer Fix (toJSON) ENABLED.`);

    startTime = Date.now();
    let blocksProcessedThisSession = 0;

    for (let block = startBlock; block >= 1; block -= BLOCKS_PER_BATCH) {
        const batchStart = block;
        const batchEnd = Math.max(block - BLOCKS_PER_BATCH + 1, 1);

        for (let b = batchStart; b >= batchEnd; b--) {
            await processBlock(b);
            blocksProcessedThisSession++;
            state.lastProcessedBlock = b;
            state.totalBlocksProcessed++;

            if (blocksProcessedThisSession % 100 === 0) {
                const elapsed = (Date.now() - startTime) / 1000;
                const speed = blocksProcessedThisSession / elapsed;
                const eta = b / speed / 3600;
                console.log(`📈 Block ${b.toLocaleString()} | Speed: ${speed.toFixed(1)} blk/s | ETA: ${eta.toFixed(1)}h | Swaps: ${stats.swaps} | Transfers: ${stats.transfers} | Bridges: ${stats.bridges} | Fees: ${stats.fees}`);
            }

            if (blocksProcessedThisSession % PROGRESS_SAVE_INTERVAL === 0) {
                saveState(state);
            }
        }
        await new Promise(r => setTimeout(r, DELAY_BETWEEN_BATCHES_MS));
    }

    saveState(state);
    process.exit(0);
}

runBackfill().catch(e => { console.error('Fatal:', e); process.exit(1); });
