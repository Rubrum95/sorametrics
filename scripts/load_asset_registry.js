#!/usr/bin/env node
// ============================================================
// Load SORA asset registry into sm.asset_registry
// Fetches from GitHub whitelist + hardcoded essentials
// Usage: node scripts/load_asset_registry.js
// ============================================================

'use strict';

const { Pool } = require('pg');

const WHITELIST_URL = 'https://raw.githubusercontent.com/sora-xor/polkaswap-token-whitelist-config/master/whitelist.json';

const PG_CONFIG = {
    host: process.env.PG_HOST || 'localhost',
    port: parseInt(process.env.PG_PORT) || 23798,
    database: process.env.PG_DB || 'squid',
    user: process.env.PG_USER || 'postgres',
    password: process.env.PG_PASS || 'squid',
    max: 3,
};

// Essential assets that must always be present (fallback)
const ESSENTIALS = [
    { address: '0x0200000000000000000000000000000000000000000000000000000000000000', symbol: 'XOR', name: 'SORA', decimals: 18 },
    { address: '0x0200040000000000000000000000000000000000000000000000000000000000', symbol: 'VAL', name: 'SORA Validator Token', decimals: 18 },
    { address: '0x0200050000000000000000000000000000000000000000000000000000000000', symbol: 'PSWAP', name: 'Polkaswap', decimals: 18 },
    { address: '0x0200060000000000000000000000000000000000000000000000000000000000', symbol: 'DAI', name: 'Dai Stablecoin', decimals: 18 },
    { address: '0x0200070000000000000000000000000000000000000000000000000000000000', symbol: 'ETH', name: 'Ether', decimals: 18 },
    { address: '0x0200080000000000000000000000000000000000000000000000000000000000', symbol: 'XSTUSD', name: 'SORA Synthetic USD', decimals: 18 },
    { address: '0x0200090000000000000000000000000000000000000000000000000000000000', symbol: 'XST', name: 'SORA Synthetics', decimals: 18 },
    { address: '0x02000a0000000000000000000000000000000000000000000000000000000000', symbol: 'TBCD', name: 'TBCD', decimals: 18 },
    { address: '0x02000b0000000000000000000000000000000000000000000000000000000000', symbol: 'KEN', name: 'Kensetsu', decimals: 18 },
    { address: '0x02000c0000000000000000000000000000000000000000000000000000000000', symbol: 'KUSD', name: 'Kensetsu USD', decimals: 18 },
];

// Stablecoin IDs for reference (exported for use by other scripts)
const STABLECOIN_IDS = [
    '0x0200060000000000000000000000000000000000000000000000000000000000', // DAI
    '0x02000c0000000000000000000000000000000000000000000000000000000000', // KUSD
    '0x0200080000000000000000000000000000000000000000000000000000000000', // XSTUSD
];

async function main() {
    const pool = new Pool(PG_CONFIG);
    console.log('Loading asset registry...');

    let assets = [];

    // 1. Fetch from GitHub whitelist
    try {
        const res = await fetch(WHITELIST_URL);
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        const data = await res.json();

        for (const item of data) {
            if (!item.address) continue;
            assets.push({
                asset_id: item.address,
                symbol: item.symbol || 'UNK',
                name: item.name || '',
                decimals: item.decimals || 18,
                logo: item.icon || '',
            });
        }
        console.log(`  Fetched ${assets.length} assets from whitelist`);
    } catch (err) {
        console.error(`  Failed to fetch whitelist: ${err.message}`);
        console.log('  Using essential fallbacks only');
    }

    // 2. Ensure essentials are present
    const assetIds = new Set(assets.map(a => a.asset_id));
    for (const e of ESSENTIALS) {
        if (!assetIds.has(e.address)) {
            assets.push({
                asset_id: e.address,
                symbol: e.symbol,
                name: e.name,
                decimals: e.decimals,
                logo: '',
            });
        }
    }

    // 3. Upsert into sm.asset_registry
    const client = await pool.connect();
    try {
        await client.query('BEGIN');

        const upsertSql = `
            INSERT INTO sm.asset_registry (asset_id, symbol, name, decimals, logo)
            VALUES ($1, $2, $3, $4, $5)
            ON CONFLICT (asset_id) DO UPDATE SET
                symbol = EXCLUDED.symbol,
                name = EXCLUDED.name,
                decimals = EXCLUDED.decimals,
                logo = EXCLUDED.logo
        `;

        let count = 0;
        for (const a of assets) {
            await client.query(upsertSql, [a.asset_id, a.symbol, a.name, a.decimals, a.logo]);
            count++;
        }

        await client.query('COMMIT');
        console.log(`  Upserted ${count} assets into sm.asset_registry`);
    } catch (err) {
        await client.query('ROLLBACK');
        console.error('  Error upserting assets:', err.message);
        throw err;
    } finally {
        client.release();
    }

    // 4. Verify
    const result = await pool.query('SELECT COUNT(*) as cnt FROM sm.asset_registry');
    console.log(`  Total assets in registry: ${result.rows[0].cnt}`);

    const spotCheck = await pool.query(
        "SELECT symbol FROM sm.asset_registry WHERE asset_id IN ($1, $2, $3)",
        [ESSENTIALS[0].address, ESSENTIALS[1].address, ESSENTIALS[2].address]
    );
    console.log(`  Spot check: ${spotCheck.rows.map(r => r.symbol).join(', ')}`);

    await pool.end();
    console.log('Done.');
}

main().catch(err => {
    console.error('Fatal:', err);
    process.exit(1);
});

module.exports = { STABLECOIN_IDS, PG_CONFIG, ESSENTIALS };
