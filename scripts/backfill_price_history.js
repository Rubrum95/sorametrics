#!/usr/bin/env node
// ============================================================
// Backfill sm.price_history from stablecoin swap ratios
//
// Strategy:
//   Pass 1: Extract prices from swaps where one side is DAI
//           (the only reliable $1 stablecoin on SORA).
//           KUSD and XSTUSD are NOT used — they are depegged.
//   Pass 2: For tokens only traded against XOR (no DAI pair),
//           derive price = (xor_amount/token_amount) * xor_usd_price
//           using XOR prices from Pass 1.
//
// Usage: node scripts/backfill_price_history.js [--reset]
// ============================================================

'use strict';

const { Pool } = require('pg');

const PG_CONFIG = {
    host: process.env.PG_HOST || 'localhost',
    port: parseInt(process.env.PG_PORT) || 23798,
    database: process.env.PG_DB || 'squid',
    user: process.env.PG_USER || 'postgres',
    password: process.env.PG_PASS || 'squid',
    max: 5,
};

const XOR_ID = '0x0200000000000000000000000000000000000000000000000000000000000000';
const DAI_ID = '0x0200060000000000000000000000000000000000000000000000000000000000';

// Only DAI is a reliable $1 reference on SORA.
// KUSD and XSTUSD are depegged and MUST NOT be used for price derivation.
const STABLECOIN_IDS = [DAI_ID];
const BLOCK_RANGE = 200000;
const BATCH_INSERT_SIZE = 5000;

async function getMaxBlock(pool) {
    const res = await pool.query('SELECT MAX(block_height) as max_block FROM history_element');
    return res.rows[0].max_block || 0;
}

// ============================================================
// PASS 1: Direct stablecoin swap prices
// ============================================================
async function pass1_stablecoinSwaps(pool) {
    console.log('\n=== PASS 1: Extracting prices from stablecoin swaps ===');

    const maxBlock = await getMaxBlock(pool);
    console.log(`  Max block: ${maxBlock}`);

    // Temporary table for raw price samples
    await pool.query(`
        CREATE TEMP TABLE IF NOT EXISTS _raw_prices (
            asset_id TEXT NOT NULL,
            hour_bucket INTEGER NOT NULL,
            price_usd DOUBLE PRECISION NOT NULL
        )
    `);
    await pool.query('TRUNCATE _raw_prices');

    const stableList = STABLECOIN_IDS.map((_, i) => `$${i + 3}`).join(',');

    const sql = `
        SELECT
            he.timestamp,
            he.data->>'baseAssetId' AS base_id,
            he.data->>'targetAssetId' AS target_id,
            (he.data->>'baseAssetAmount')::numeric AS base_amount,
            (he.data->>'targetAssetAmount')::numeric AS target_amount
        FROM history_element he
        WHERE he.type = 'CALL'
          AND he.module = 'liquidityProxy'
          AND he.method IN ('swap', 'swapTransfer', 'swapTransferBatch')
          AND (he.execution->>'success')::boolean = true
          AND he.data IS NOT NULL
          AND (
              he.data->>'baseAssetId' IN (${stableList})
              OR he.data->>'targetAssetId' IN (${stableList})
          )
          AND he.block_height >= $1 AND he.block_height < $2
        ORDER BY he.timestamp ASC
    `;

    let totalSamples = 0;
    let totalSwaps = 0;

    for (let start = 0; start <= maxBlock; start += BLOCK_RANGE) {
        const end = start + BLOCK_RANGE;
        const params = [start, end, ...STABLECOIN_IDS];
        const result = await pool.query(sql, params);

        if (result.rows.length === 0) continue;
        totalSwaps += result.rows.length;

        const values = [];
        const insertParams = [];
        let paramIdx = 1;

        for (const row of result.rows) {
            const baseIsStable = STABLECOIN_IDS.includes(row.base_id);
            const targetIsStable = STABLECOIN_IDS.includes(row.target_id);

            if (!baseIsStable && !targetIsStable) continue;

            const baseAmt = parseFloat(row.base_amount);
            const targetAmt = parseFloat(row.target_amount);
            if (!baseAmt || !targetAmt || baseAmt <= 0 || targetAmt <= 0) continue;

            const hourBucket = Math.floor(row.timestamp / 3600) * 3600;

            if (baseIsStable && !targetIsStable) {
                // Base is stablecoin: price of target = base_amount / target_amount
                const price = baseAmt / targetAmt;
                if (price > 0 && price < 1e12 && isFinite(price)) {
                    values.push(`($${paramIdx}, $${paramIdx + 1}, $${paramIdx + 2})`);
                    insertParams.push(row.target_id, hourBucket, price);
                    paramIdx += 3;
                    totalSamples++;
                }
            }

            if (targetIsStable && !baseIsStable) {
                // Target is stablecoin: price of base = target_amount / base_amount
                const price = targetAmt / baseAmt;
                if (price > 0 && price < 1e12 && isFinite(price)) {
                    values.push(`($${paramIdx}, $${paramIdx + 1}, $${paramIdx + 2})`);
                    insertParams.push(row.base_id, hourBucket, price);
                    paramIdx += 3;
                    totalSamples++;
                }
            }

            // If both sides are DAI (shouldn't happen), skip

            // Flush batch
            if (values.length >= BATCH_INSERT_SIZE) {
                await pool.query(
                    `INSERT INTO _raw_prices (asset_id, hour_bucket, price_usd) VALUES ${values.join(',')}`,
                    insertParams
                );
                values.length = 0;
                insertParams.length = 0;
                paramIdx = 1;
            }
        }

        // Flush remaining
        if (values.length > 0) {
            await pool.query(
                `INSERT INTO _raw_prices (asset_id, hour_bucket, price_usd) VALUES ${values.join(',')}`,
                insertParams
            );
        }

        if (start % (BLOCK_RANGE * 10) === 0 || end > maxBlock) {
            const pct = Math.min(100, ((end / maxBlock) * 100)).toFixed(1);
            console.log(`  Block ${start.toLocaleString()}-${end.toLocaleString()} (${pct}%) — ${totalSwaps.toLocaleString()} swaps, ${totalSamples.toLocaleString()} price samples`);
        }
    }

    console.log(`  Pass 1 complete: ${totalSwaps.toLocaleString()} swaps → ${totalSamples.toLocaleString()} price samples`);

    // Aggregate raw samples to hourly medians (robust against outlier swaps)
    console.log('  Aggregating to hourly medians...');
    const aggResult = await pool.query(`
        INSERT INTO sm.price_history (asset_id, hour_bucket, price_usd, sample_count)
        SELECT asset_id, hour_bucket,
               PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY price_usd) AS price_usd,
               COUNT(*) AS sample_count
        FROM _raw_prices
        GROUP BY asset_id, hour_bucket
        ON CONFLICT (asset_id, hour_bucket) DO UPDATE SET
            price_usd = EXCLUDED.price_usd,
            sample_count = EXCLUDED.sample_count
    `);
    console.log(`  Inserted ${aggResult.rowCount.toLocaleString()} hourly price records`);

    // Insert DAI as $1 for all covered hours
    console.log('  Inserting DAI prices ($1)...');
    const hourRange = await pool.query(`
        SELECT MIN(hour_bucket) as min_h, MAX(hour_bucket) as max_h FROM sm.price_history
    `);
    const minH = hourRange.rows[0].min_h;
    const maxH = hourRange.rows[0].max_h;

    await pool.query(`
        INSERT INTO sm.price_history (asset_id, hour_bucket, price_usd, sample_count)
        SELECT $1, h, 1.0, 1
        FROM generate_series($2::int, $3::int, 3600) AS h
        ON CONFLICT (asset_id, hour_bucket) DO NOTHING
    `, [DAI_ID, minH, maxH]);

    await pool.query('DROP TABLE IF EXISTS _raw_prices');
    return { totalSwaps, totalSamples };
}

// ============================================================
// PASS 1.5: Interpolate prices to fill hour gaps (forward-fill)
// ============================================================
async function pass15_interpolate(pool) {
    console.log('\n=== PASS 1.5: Interpolating prices for missing hours ===');

    // Get all assets that have at least some prices
    const assetsRes = await pool.query(`
        SELECT asset_id, COUNT(*) AS cnt,
               MIN(hour_bucket) AS mn, MAX(hour_bucket) AS mx
        FROM sm.price_history
        GROUP BY asset_id
        HAVING COUNT(*) >= 10
        ORDER BY cnt DESC
    `);
    console.log(`  Assets to interpolate: ${assetsRes.rows.length}`);

    let totalInterpolated = 0;
    for (const row of assetsRes.rows) {
        const totalHours = Math.floor((row.mx - row.mn) / 3600) + 1;
        const coverage = parseInt(row.cnt) / totalHours;
        // Skip assets already at >95% coverage
        if (coverage > 0.95) continue;

        const result = await pool.query(`
            WITH all_hours AS (
                SELECT generate_series($2::int, $3::int, 3600) AS h
            ),
            gaps AS (
                SELECT ah.h
                FROM all_hours ah
                LEFT JOIN sm.price_history ph ON ph.asset_id = $1 AND ph.hour_bucket = ah.h
                WHERE ph.hour_bucket IS NULL
            )
            INSERT INTO sm.price_history (asset_id, hour_bucket, price_usd, sample_count)
            SELECT $1, g.h,
                (SELECT price_usd FROM sm.price_history
                 WHERE asset_id = $1 AND hour_bucket < g.h
                 ORDER BY hour_bucket DESC LIMIT 1),
                0
            FROM gaps g
            WHERE (SELECT price_usd FROM sm.price_history
                   WHERE asset_id = $1 AND hour_bucket < g.h
                   ORDER BY hour_bucket DESC LIMIT 1) IS NOT NULL
            ON CONFLICT DO NOTHING
        `, [row.asset_id, row.mn, row.mx]);

        if (result.rowCount > 0) {
            totalInterpolated += result.rowCount;
        }
    }
    console.log(`  Total interpolated hours: ${totalInterpolated.toLocaleString()}`);
}

// ============================================================
// PASS 2: Derive prices via XOR for tokens with missing hours
// ============================================================
async function pass2_xorDerived(pool) {
    console.log('\n=== PASS 2: Deriving prices via XOR (filling gaps for ALL tokens) ===');

    // Build a set of (asset_id, hour_bucket) already covered by Pass 1
    // so we can skip those specific hours (not entire assets)
    console.log('  Loading existing coverage map...');
    const coveredRes = await pool.query('SELECT asset_id, hour_bucket FROM sm.price_history');
    const coveredHours = new Set(coveredRes.rows.map(r => `${r.asset_id}|${r.hour_bucket}`));
    console.log(`  Existing price points: ${coveredHours.size}`);

    // Check if XOR has prices
    const xorCheck = await pool.query('SELECT COUNT(*) as cnt FROM sm.price_history WHERE asset_id = $1', [XOR_ID]);
    if (parseInt(xorCheck.rows[0].cnt) === 0) {
        console.log('  WARNING: XOR has no price data! Cannot derive via XOR.');
        return;
    }

    const maxBlock = await getMaxBlock(pool);

    // Find swaps involving XOR where the OTHER token has no price yet
    await pool.query(`
        CREATE TEMP TABLE IF NOT EXISTS _xor_raw (
            asset_id TEXT NOT NULL,
            hour_bucket INTEGER NOT NULL,
            price_in_xor DOUBLE PRECISION NOT NULL
        )
    `);
    await pool.query('TRUNCATE _xor_raw');

    const sql = `
        SELECT
            he.timestamp,
            he.data->>'baseAssetId' AS base_id,
            he.data->>'targetAssetId' AS target_id,
            (he.data->>'baseAssetAmount')::numeric AS base_amount,
            (he.data->>'targetAssetAmount')::numeric AS target_amount
        FROM history_element he
        WHERE he.type = 'CALL'
          AND he.module = 'liquidityProxy'
          AND he.method IN ('swap', 'swapTransfer', 'swapTransferBatch')
          AND (he.execution->>'success')::boolean = true
          AND he.data IS NOT NULL
          AND (
              he.data->>'baseAssetId' = $3
              OR he.data->>'targetAssetId' = $3
          )
          AND he.block_height >= $1 AND he.block_height < $2
        ORDER BY he.timestamp ASC
    `;

    let totalSamples = 0;

    for (let start = 0; start <= maxBlock; start += BLOCK_RANGE) {
        const end = start + BLOCK_RANGE;
        const result = await pool.query(sql, [start, end, XOR_ID]);
        if (result.rows.length === 0) continue;

        const values = [];
        const insertParams = [];
        let paramIdx = 1;

        for (const row of result.rows) {
            const baseIsXor = row.base_id === XOR_ID;
            const targetIsXor = row.target_id === XOR_ID;
            if (!baseIsXor && !targetIsXor) continue;

            const otherAssetId = baseIsXor ? row.target_id : row.base_id;

            // Skip stablecoins (they're always $1)
            if (STABLECOIN_IDS.includes(otherAssetId)) continue;

            const baseAmt = parseFloat(row.base_amount);
            const targetAmt = parseFloat(row.target_amount);
            if (!baseAmt || !targetAmt || baseAmt <= 0 || targetAmt <= 0) continue;

            const hourBucket = Math.floor(row.timestamp / 3600) * 3600;

            // Skip if this specific (asset, hour) already has a DAI-derived price
            if (coveredHours.has(`${otherAssetId}|${hourBucket}`)) continue;

            // Price in XOR: how many XOR per 1 token?
            let priceInXor;
            if (baseIsXor) {
                // XOR→Token: priceInXor = xor_amount / token_amount
                priceInXor = baseAmt / targetAmt;
            } else {
                // Token→XOR: priceInXor = xor_amount / token_amount
                priceInXor = targetAmt / baseAmt;
            }

            if (priceInXor > 0 && priceInXor < 1e18 && isFinite(priceInXor)) {
                values.push(`($${paramIdx}, $${paramIdx + 1}, $${paramIdx + 2})`);
                insertParams.push(otherAssetId, hourBucket, priceInXor);
                paramIdx += 3;
                totalSamples++;
            }

            if (values.length >= BATCH_INSERT_SIZE) {
                await pool.query(
                    `INSERT INTO _xor_raw (asset_id, hour_bucket, price_in_xor) VALUES ${values.join(',')}`,
                    insertParams
                );
                values.length = 0;
                insertParams.length = 0;
                paramIdx = 1;
            }
        }

        if (values.length > 0) {
            await pool.query(
                `INSERT INTO _xor_raw (asset_id, hour_bucket, price_in_xor) VALUES ${values.join(',')}`,
                insertParams
            );
        }

        if (start % (BLOCK_RANGE * 10) === 0 || end > maxBlock) {
            const pct = Math.min(100, ((end / maxBlock) * 100)).toFixed(1);
            console.log(`  Block ${start.toLocaleString()}-${end.toLocaleString()} (${pct}%) — ${totalSamples.toLocaleString()} XOR-derived samples`);
        }
    }

    console.log(`  Pass 2 raw: ${totalSamples.toLocaleString()} XOR-ratio samples`);

    // Now JOIN with XOR prices to get USD prices
    console.log('  Converting XOR ratios to USD using XOR price history...');
    const deriveResult = await pool.query(`
        INSERT INTO sm.price_history (asset_id, hour_bucket, price_usd, sample_count)
        SELECT
            xr.asset_id,
            xr.hour_bucket,
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY xr.price_in_xor * ph.price_usd) AS price_usd,
            COUNT(*) AS sample_count
        FROM _xor_raw xr
        JOIN sm.price_history ph
            ON ph.asset_id = $1
            AND ph.hour_bucket = xr.hour_bucket
        GROUP BY xr.asset_id, xr.hour_bucket
        ON CONFLICT (asset_id, hour_bucket) DO NOTHING
    `, [XOR_ID]);

    console.log(`  Inserted ${deriveResult.rowCount.toLocaleString()} XOR-derived price records`);

    await pool.query('DROP TABLE IF EXISTS _xor_raw');
}

// ============================================================
// MAIN
// ============================================================
async function main() {
    const pool = new Pool(PG_CONFIG);
    const startTime = Date.now();

    const doReset = process.argv.includes('--reset');
    if (doReset) {
        console.log('Resetting sm.price_history...');
        await pool.query('TRUNCATE sm.price_history');
    }

    // Check current state
    const currentCount = await pool.query('SELECT COUNT(*) as cnt FROM sm.price_history');
    console.log(`Current price_history rows: ${parseInt(currentCount.rows[0].cnt).toLocaleString()}`);

    const pass2Only = process.argv.includes('--pass2-only');

    if (!pass2Only) {
        // Pass 1: Direct stablecoin prices
        await pass1_stablecoinSwaps(pool);

        // Stats after pass 1
        const afterP1 = await pool.query(`
            SELECT COUNT(*) as rows, COUNT(DISTINCT asset_id) as assets
            FROM sm.price_history
        `);
        console.log(`\nAfter Pass 1: ${parseInt(afterP1.rows[0].rows).toLocaleString()} rows, ${afterP1.rows[0].assets} unique assets`);
    } else {
        console.log('\n--pass2-only: Skipping Pass 1 (keeping existing DAI-derived prices)');
        // Always extend DAI to cover the full hour range
        const daiExt = await pool.query(`
            INSERT INTO sm.price_history (asset_id, hour_bucket, price_usd, sample_count)
            SELECT $1, h, 1.0, 1
            FROM generate_series(
                (SELECT MIN(hour_bucket) FROM sm.price_history),
                (SELECT MAX(hour_bucket) FROM sm.price_history),
                3600
            ) AS h
            ON CONFLICT (asset_id, hour_bucket) DO NOTHING
        `, [DAI_ID]);
        if (daiExt.rowCount > 0) console.log(`  Extended DAI by ${daiExt.rowCount} hours`);
    }

    // Pass 1.5: Interpolate prices for missing hours (forward-fill)
    await pass15_interpolate(pool);

    // Pass 2: XOR-derived prices (now with full XOR coverage)
    await pass2_xorDerived(pool);

    // Final stats
    const finalStats = await pool.query(`
        SELECT COUNT(*) as rows, COUNT(DISTINCT asset_id) as assets,
               MIN(hour_bucket) as min_ts, MAX(hour_bucket) as max_ts
        FROM sm.price_history
    `);
    const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
    const stats = finalStats.rows[0];

    console.log('\n=== BACKFILL COMPLETE ===');
    console.log(`  Total rows: ${parseInt(stats.rows).toLocaleString()}`);
    console.log(`  Unique assets: ${stats.assets}`);
    console.log(`  Date range: ${new Date(stats.min_ts * 1000).toISOString().slice(0, 10)} — ${new Date(stats.max_ts * 1000).toISOString().slice(0, 10)}`);
    console.log(`  Time elapsed: ${elapsed}s`);

    await pool.end();
}

main().catch(err => {
    console.error('Fatal:', err);
    process.exit(1);
});
