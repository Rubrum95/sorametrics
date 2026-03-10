#!/usr/bin/env node
// gap_filler_fees.js — One-off script to fill the fee gap between history and main DBs
// Gap: blocks 25,125,405 → 25,126,077 (674 blocks)
// Run on VPS: node gap_filler_fees.js

const Database = require('better-sqlite3');
const { ApiPromise, WsProvider } = require('@polkadot/api');
const { options } = require('@sora-substrate/api');
const BigNumber = require('bignumber.js');
const path = require('path');

const DB_PATH = path.join(__dirname, 'database.db');
const WS_ENDPOINT = 'wss://mof2.sora.org';

const GAP_START = 25125405;
const GAP_END   = 25126077;

const XOR_ID = '0x0200000000000000000000000000000000000000000000000000000000000000';
const DAI_ID = '0x0200060000000000000000000000000000000000000000000000000000000000';

let db, api;

function initDB() {
    db = new Database(DB_PATH);
    db.pragma('journal_mode = WAL');
    db.pragma('synchronous = NORMAL');
    // Ensure denom_factor column exists
    try { db.exec(`ALTER TABLE fees ADD COLUMN denom_factor TEXT DEFAULT '1'`); } catch (e) { /* exists */ }
    console.log('💾 DB connected:', DB_PATH);
}

function feeType(extrinsicType) {
    if (!extrinsicType) return 'Other';
    if (extrinsicType.includes('liquidityProxy') || extrinsicType.includes('swap')) return 'Swap';
    if (extrinsicType.includes('ethBridge') || extrinsicType.includes('bridge')) return 'Bridge';
    if (extrinsicType.includes('assets') || extrinsicType.includes('balances')) return 'Transfer';
    return 'Other';
}

async function getXorPriceInDai(apiAt) {
    try {
        const reserves = await apiAt.query.poolXYK.reserves(XOR_ID, DAI_ID);
        if (!reserves || reserves.length < 2) return 0;
        const xorRes = new BigNumber(reserves[0].toString());
        const daiRes = new BigNumber(reserves[1].toString());
        if (xorRes.isZero()) return 0;
        return daiRes.div(xorRes).toNumber();
    } catch (e) { return 0; }
}

async function processBlock(blockNumber) {
    const blockHash = await api.rpc.chain.getBlockHash(blockNumber);
    const [signedBlock, allEvents, timestamp] = await Promise.all([
        api.rpc.chain.getBlock(blockHash),
        api.query.system.events.at(blockHash),
        api.query.timestamp.now.at(blockHash)
    ]);

    const blockTimestamp = timestamp.toNumber();

    // Filter fee events
    const feeEvents = [];
    allEvents.forEach((record, idx) => {
        const { event, phase } = record;
        if (event.section === 'transactionPayment' && event.method === 'TransactionFeePaid') {
            const d = event.data;
            const feeRaw = d[1].toString();
            const feeNum = new BigNumber(feeRaw).div(new BigNumber(10).pow(18));

            let extrinsicType = '';
            if (phase.isApplyExtrinsic) {
                try {
                    const exIdx = phase.asApplyExtrinsic.toNumber();
                    const ex = signedBlock.block.extrinsics[exIdx];
                    if (ex) {
                        const decoded = ex.toHuman();
                        if (decoded?.method) extrinsicType = `${decoded.method.section}.${decoded.method.method}`;
                    }
                } catch (e) { }
            }

            feeEvents.push({ feeNum, extrinsicType });
        }
    });

    if (feeEvents.length === 0) return 0;

    // Query denomination factor and XOR price once per block
    const apiAt = await api.at(blockHash);

    let blockDenomFactor = '1';
    try {
        const denom = await apiAt.query.denomination.denominator();
        blockDenomFactor = denom.toString().replace(/,/g, '');
    } catch (e) { /* no denomination pallet */ }

    const xorPrice = await getXorPriceInDai(apiAt);

    // Insert fees
    const insertStmt = db.prepare(
        `INSERT INTO fees (timestamp, block, type, amount, usd_value, denom_factor) VALUES (?, ?, ?, ?, ?, ?)`
    );

    const insertBatch = db.transaction((fees) => {
        for (const f of fees) {
            insertStmt.run(
                blockTimestamp,
                blockNumber,
                feeType(f.extrinsicType),
                parseFloat(f.feeNum.toFixed(8)) || 0,
                f.feeNum.times(xorPrice).toNumber() || 0,
                blockDenomFactor
            );
        }
    });

    insertBatch(feeEvents);
    return feeEvents.length;
}

async function main() {
    initDB();

    // Check which blocks in the gap already have fees (avoid duplicates)
    const existing = new Set(
        db.prepare(`SELECT DISTINCT block FROM fees WHERE block >= ? AND block <= ?`)
          .all(GAP_START, GAP_END)
          .map(r => r.block)
    );
    console.log(`📋 ${existing.size} blocks in gap already have fees, skipping those.`);

    console.log(`🔌 Connecting to ${WS_ENDPOINT}...`);
    const provider = new WsProvider(WS_ENDPOINT);
    api = await ApiPromise.create(options({ provider }));
    await api.isReady;
    console.log('✅ API ready.');

    const totalBlocks = GAP_END - GAP_START + 1;
    let processed = 0, totalFees = 0, errors = 0;

    console.log(`🔧 Processing gap: blocks ${GAP_START} → ${GAP_END} (${totalBlocks} blocks)`);

    for (let block = GAP_START; block <= GAP_END; block++) {
        if (existing.has(block)) {
            processed++;
            continue;
        }
        try {
            const fees = await processBlock(block);
            totalFees += fees;
            processed++;
            if (processed % 50 === 0 || fees > 0) {
                console.log(`  📦 ${processed}/${totalBlocks} blocks | ${totalFees} fees found${fees > 0 ? ` (+${fees} in block ${block})` : ''}`);
            }
        } catch (e) {
            errors++;
            console.error(`  ❌ Block ${block}: ${e.message}`);
        }
    }

    console.log(`\n✅ Gap fill complete!`);
    console.log(`   Blocks processed: ${processed}/${totalBlocks}`);
    console.log(`   Fees inserted: ${totalFees}`);
    console.log(`   Errors: ${errors}`);

    // Verify
    const verify = db.prepare(`SELECT COUNT(*) as c FROM fees WHERE block >= ? AND block <= ?`).get(GAP_START, GAP_END);
    console.log(`   Fees in gap range after fill: ${verify.c}`);

    await api.disconnect();
    db.close();
    process.exit(0);
}

main().catch(e => {
    console.error('Fatal:', e);
    process.exit(1);
});
