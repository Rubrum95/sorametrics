// backfill_xor_supply.js — Backfill XOR totalIssuance to sm.supply_history
// Uses @sora-substrate/api which handles denomination transparently.
// Run: node scripts/backfill_xor_supply.js
//
// XOR is the native SORA token affected by 6 denomination events (10^38 cumulative).
// The Rust backfiller can't decode XOR correctly — this JS script uses the SORA
// polkadot.js API which handles denomination automatically.

const { ApiPromise, WsProvider } = require('@polkadot/api');
const { options } = require('@sora-substrate/api');
const BigNumber = require('bignumber.js');
const { Pool } = require('pg');

const WS_ENDPOINT = process.env.WS_ENDPOINT || 'wss://ws.mof.sora.org';
const ASSET_ID = '0x0200000000000000000000000000000000000000000000000000000000000000';
const DECIMALS = 18;
const BLOCK_TIME_SECS = 6;
const SNAPSHOT_INTERVAL_BLOCKS = 14400; // ~1 day

const pgPool = new Pool({
    host: process.env.PG_HOST || 'localhost',
    port: parseInt(process.env.PG_PORT) || 23798,
    database: process.env.PG_DB || 'squid',
    user: process.env.PG_USER || 'postgres',
    password: process.env.PG_PASS || 'squid',
});

async function main() {
    console.log('🔗 Connecting to SORA...');
    const provider = new WsProvider(WS_ENDPOINT);
    const api = await ApiPromise.create(options({ provider }));
    console.log('✅ Connected\n');

    const currentHeader = await api.rpc.chain.getHeader();
    const currentBlock = currentHeader.number.toNumber();
    console.log(`📦 Current block: ${currentBlock.toLocaleString()}`);

    // Target: fill from block 1 to current, every ~1 day (14400 blocks)
    // Check existing data to know where to start
    const pgClient = await pgPool.connect();
    const existing = await pgClient.query(
        "SELECT COUNT(*) as c FROM sm.supply_history WHERE symbol = 'XOR'"
    );
    console.log(`📊 Existing XOR rows: ${existing.rows[0].c}`);

    const existingTs = await pgClient.query(
        "SELECT timestamp FROM sm.supply_history WHERE symbol = 'XOR' ORDER BY timestamp"
    );
    const existingSet = new Set(existingTs.rows.map(r => Number(r.timestamp)));

    let inserted = 0;
    let errors = 0;
    const totalSteps = Math.ceil(currentBlock / SNAPSHOT_INTERVAL_BLOCKS);

    for (let block = SNAPSHOT_INTERVAL_BLOCKS; block <= currentBlock; block += SNAPSHOT_INTERVAL_BLOCKS) {
        try {
            const blockHash = await api.rpc.chain.getBlockHash(block);
            const apiAt = await api.at(blockHash);

            // Get block timestamp
            const tsRaw = await apiAt.query.timestamp.now();
            const blockTimestamp = parseInt(tsRaw.toString());
            const timestampSec = Math.floor(blockTimestamp / 1000);

            // Skip if already exists
            if (existingSet.has(timestampSec)) continue;

            // Get XOR totalIssuance (denomination-adjusted by SORA API)
            const raw = await apiAt.query.balances.totalIssuance();
            const rawStr = raw.toString().replace(/,/g, '');
            const supply = new BigNumber(rawStr).div(new BigNumber(10).pow(DECIMALS)).toNumber();

            if (supply > 0 && supply < 1e12) { // sanity check
                await pgClient.query(
                    `INSERT INTO sm.supply_history (timestamp, symbol, asset_id, total_issuance, source)
                     VALUES ($1, 'XOR', $2, $3, 'backfill-js')
                     ON CONFLICT (symbol, timestamp) DO NOTHING`,
                    [timestampSec, ASSET_ID, supply]
                );
                inserted++;
            }

            const step = Math.floor(block / SNAPSHOT_INTERVAL_BLOCKS);
            if (step % 50 === 0 || block >= currentBlock - SNAPSHOT_INTERVAL_BLOCKS) {
                console.log(`  📸 Block ${block.toLocaleString()} | XOR = ${supply.toFixed(2)} | ${step}/${totalSteps} | inserted: ${inserted}`);
            }
        } catch (e) {
            errors++;
            if (errors <= 5) console.error(`  ⚠️ Block ${block}: ${e.message}`);
        }
    }

    console.log(`\n✅ Done! ${inserted} XOR rows inserted, ${errors} errors`);

    pgClient.release();
    await pgPool.end();
    await api.disconnect();
    process.exit(0);
}

main().catch(e => {
    console.error('Fatal:', e);
    process.exit(1);
});
