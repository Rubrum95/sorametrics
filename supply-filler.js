// supply-filler.js - One-shot script to fill supply snapshot gap
// Fills from current block backwards to backfiller position
// Run once: node supply-filler.js
// Run for specific tokens only: node supply-filler.js VAL PSWAP

const { ApiPromise, WsProvider } = require('@polkadot/api');
const { options } = require('@sora-substrate/api');
const BigNumber = require('bignumber.js');
const Database = require('better-sqlite3');
const fs = require('fs');
const path = require('path');

const { WS_ENDPOINT } = require('./config');
const STATE_FILE = path.join(__dirname, 'backfill_state.json');
const MAIN_DB_PATH = path.join(__dirname, 'database_30d.db');
const HISTORY_DB_PATH = path.join(__dirname, 'database.db');

// Snapshot every 1800 blocks ≈ 3 hours (6s/block)
const SNAPSHOT_INTERVAL = 1800;

// Tokens to track (all asset IDs hardcoded from SORA chain)
const SUPPLY_TOKENS = {
    XOR:   { assetId: '0x0200000000000000000000000000000000000000000000000000000000000000', decimals: 18, isNative: true },
    VAL:   { assetId: '0x0200040000000000000000000000000000000000000000000000000000000000', decimals: 18 },
    PSWAP: { assetId: '0x0200050000000000000000000000000000000000000000000000000000000000', decimals: 18 },
    TBCD:  { assetId: '0x02000a0000000000000000000000000000000000000000000000000000000000', decimals: 18 },
    KUSD:  { assetId: '0x0200080000000000000000000000000000000000000000000000000000000000', decimals: 18 }
};

async function getSupplyAtBlock(apiAt, symbol, config) {
    try {
        let raw;
        if (config.isNative) {
            raw = await apiAt.query.balances.totalIssuance();
        } else {
            if (!config.assetId) return null;
            raw = await apiAt.query.tokens.totalIssuance({ code: config.assetId });
        }
        const rawStr = raw.toString().replace(/,/g, '');
        return new BigNumber(rawStr).div(new BigNumber(10).pow(config.decimals)).toNumber();
    } catch (e) {
        return null;
    }
}

async function main() {
    // Parse CLI args for token filter
    const cliTokens = process.argv.slice(2).map(t => t.toUpperCase());
    const tokensToProcess = cliTokens.length > 0
        ? Object.fromEntries(Object.entries(SUPPLY_TOKENS).filter(([k]) => cliTokens.includes(k)))
        : SUPPLY_TOKENS;

    const tokenNames = Object.keys(tokensToProcess);
    if (tokenNames.length === 0) {
        console.error('❌ No valid tokens specified. Available: XOR, VAL, PSWAP, TBCD, KUSD');
        process.exit(1);
    }

    console.log(`🔄 Supply Filler — Filling snapshot gap`);
    console.log(`📋 Tokens: ${tokenNames.join(', ')}\n`);

    // 1. Read backfiller state
    let backfillerBlock = 0;
    if (fs.existsSync(STATE_FILE)) {
        const state = JSON.parse(fs.readFileSync(STATE_FILE, 'utf8'));
        backfillerBlock = state.lastProcessedBlock || 0;
        console.log(`📍 Backfiller position: block ${backfillerBlock.toLocaleString()}`);
    } else {
        console.log('⚠️  No backfill_state.json found, will fill from current block to block 1');
    }

    // 2. Connect to blockchain
    console.log(`🔗 Connecting to ${WS_ENDPOINT}...`);
    const provider = new WsProvider(WS_ENDPOINT);
    const api = await ApiPromise.create(options({ provider }));
    console.log('✅ Connected to blockchain\n');

    // 3. Get current block
    const currentHeader = await api.rpc.chain.getHeader();
    const currentBlock = currentHeader.number.toNumber();
    console.log(`📦 Current block: ${currentBlock.toLocaleString()}`);

    const startBlock = currentBlock;
    const endBlock = Math.max(backfillerBlock, 1);
    const totalBlocks = startBlock - endBlock;
    const totalSnapshots = Math.ceil(totalBlocks / SNAPSHOT_INTERVAL);
    console.log(`📊 Gap: ${totalBlocks.toLocaleString()} blocks (${totalSnapshots} snapshots to take)\n`);

    // 4. Open main DB
    const db = new Database(MAIN_DB_PATH);
    db.pragma('journal_mode = WAL');

    db.exec(`CREATE TABLE IF NOT EXISTS supply_snapshots (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        timestamp INTEGER,
        symbol TEXT,
        asset_id TEXT,
        total_supply REAL
    )`);
    db.exec(`CREATE INDEX IF NOT EXISTS idx_supply_symbol_timestamp ON supply_snapshots(symbol, timestamp)`);

    const insertStmt = db.prepare('INSERT INTO supply_snapshots (timestamp, symbol, asset_id, total_supply) VALUES (?, ?, ?, ?)');
    const insertBatch = db.transaction((rows) => {
        for (const r of rows) insertStmt.run(r.timestamp, r.symbol, r.assetId, r.supply);
    });

    // 5. Also open history DB for writing there too
    let histDb = null;
    if (fs.existsSync(HISTORY_DB_PATH)) {
        histDb = new Database(HISTORY_DB_PATH);
        histDb.pragma('journal_mode = WAL');
        histDb.exec(`CREATE TABLE IF NOT EXISTS supply_snapshots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp INTEGER,
            symbol TEXT,
            asset_id TEXT,
            total_supply REAL
        )`);
        histDb.exec(`CREATE INDEX IF NOT EXISTS idx_supply_symbol_timestamp ON supply_snapshots(symbol, timestamp)`);
    }
    const histInsertStmt = histDb ? histDb.prepare('INSERT INTO supply_snapshots (timestamp, symbol, asset_id, total_supply) VALUES (?, ?, ?, ?)') : null;
    const histInsertBatch = histDb ? histDb.transaction((rows) => {
        for (const r of rows) histInsertStmt.run(r.timestamp, r.symbol, r.assetId, r.supply);
    }) : null;

    // 6. Process blocks from current backwards
    let snapshotCount = 0;
    let errorCount = 0;
    let recordCount = 0;

    for (let block = startBlock; block >= endBlock; block -= SNAPSHOT_INTERVAL) {
        try {
            const blockHash = await api.rpc.chain.getBlockHash(block);
            const apiAt = await api.at(blockHash);

            // Get block timestamp
            const tsRaw = await apiAt.query.timestamp.now();
            const blockTimestamp = parseInt(tsRaw.toString());

            const rows = [];
            for (const [symbol, config] of Object.entries(tokensToProcess)) {
                const supply = await getSupplyAtBlock(apiAt, symbol, config);
                if (supply !== null && supply > 0) {
                    rows.push({
                        timestamp: blockTimestamp,
                        symbol,
                        assetId: config.assetId || '',
                        supply
                    });
                }
            }

            if (rows.length > 0) {
                insertBatch(rows);
                if (histInsertBatch) histInsertBatch(rows);
                snapshotCount++;
                recordCount += rows.length;
            }

            const pct = (((startBlock - block) / totalBlocks) * 100).toFixed(1);
            if (snapshotCount % 5 === 1 || block <= endBlock + SNAPSHOT_INTERVAL) {
                console.log(`  📸 Block ${block.toLocaleString()} | ${rows.length} tokens | ${pct}% done | ${snapshotCount}/${totalSnapshots} snapshots`);
            }
        } catch (e) {
            errorCount++;
            if (errorCount <= 3) console.error(`  ⚠️ Error at block ${block}: ${e.message}`);
        }
    }

    console.log(`\n✅ Done! ${snapshotCount} snapshots, ${recordCount} records inserted`);
    if (errorCount > 0) console.log(`⚠️  ${errorCount} errors skipped`);

    // Verify
    const count = db.prepare('SELECT COUNT(*) as c FROM supply_snapshots').get();
    console.log(`📊 Total snapshots in main DB: ${count.c}`);

    if (histDb) {
        const hcount = histDb.prepare('SELECT COUNT(*) as c FROM supply_snapshots').get();
        console.log(`📊 Total snapshots in history DB: ${hcount.c}`);
        histDb.close();
    }

    db.close();
    await api.disconnect();
    console.log('\n🏁 Supply filler complete. Exiting.');
    process.exit(0);
}

main().catch(e => {
    console.error('Fatal error:', e);
    process.exit(1);
});
