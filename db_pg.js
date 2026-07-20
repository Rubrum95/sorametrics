'use strict';
// ============================================================
// db_pg.js — PostgreSQL backend for SoraMetrics
// Drop-in replacement for db_better.js (same 54-function API)
// All functions are async; callers must await them.
// ============================================================

const { Pool } = require('pg');

const PG_CONFIG = {
    host: process.env.PG_HOST || 'localhost',
    port: parseInt(process.env.PG_PORT) || 23798,
    database: process.env.PG_DB || 'squid',
    user: process.env.PG_USER || 'postgres',
    password: process.env.PG_PASS || 'squid',
    max: 50,                         // 20 → 50 (was bottleneck during peak load)
    idleTimeoutMillis: 60000,        // 30s → 60s (keep connections warm longer)
    connectionTimeoutMillis: 10000,  // 5s → 10s (more headroom under burst)
    statement_timeout: 30000,
};

let pool = null;

// In-memory logo cache: { symbol: logoUrl }
// Loaded on initDB, ~960 entries, negligible memory
let _logoCache = {};

// In-memory symbol→asset_id cache for price lookups
let _symbolToAssetId = {};

// Reverse map asset_id (lowercase hex) → { symbol, decimals } for event
// enrichment in /block/:n. Populated alongside _symbolToAssetId.
let _assetIdToInfo = {};

// ============================================================
// HELPERS
// ============================================================

function formatTimestamp(ts) {
    const d = new Date(Number(ts));
    const pad = n => String(n).padStart(2, '0');
    return `${pad(d.getDate())}/${pad(d.getMonth() + 1)}/${d.getFullYear()} ${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())}`;
}

function logoFor(symbol) {
    return _logoCache[symbol] || '';
}

// Format USD to 1 decimal place (user preference: no excess decimals)
function fmtUsd(val) {
    const n = parseFloat(val) || 0;
    return parseFloat(n.toFixed(1));
}

function mapTransfers(rows) {
    return rows.map(r => ({
        time: formatTimestamp(r.timestamp),
        block: r.block,
        hash: r.hash || '',
        extrinsic_id: r.extrinsic_id || '',
        from: r.from_addr,
        to: r.to_addr,
        amount: r.amount,
        symbol: r.symbol,
        logo: logoFor(r.symbol),
        usdValue: fmtUsd(r.usd_value),
        assetId: r.asset_id,
    }));
}

function mapSwaps(rows) {
    return rows.map(r => ({
        time: formatTimestamp(r.timestamp),
        block: r.block,
        hash: r.hash || '',
        extrinsic_id: r.extrinsic_id || '',
        wallet: r.wallet,
        in: { symbol: r.in_symbol, amount: r.in_amount, logo: logoFor(r.in_symbol), usd: fmtUsd(r.in_usd) },
        out: { symbol: r.out_symbol, amount: r.out_amount, logo: logoFor(r.out_symbol), usd: fmtUsd(r.out_usd) },
    }));
}

function mapExtrinsics(rows) {
    return rows.map(r => ({
        time: formatTimestamp(r.timestamp),
        block: r.block,
        extrinsic_index: r.extrinsic_index,
        extrinsic_id: `${r.block}-${r.extrinsic_index}`,
        hash: r.hash || '',
        section: r.section,
        method: r.method,
        signer: r.signer,
        success: r.success,
        args_json: r.args_json || '{}',
        error_msg: r.error_msg || '',
        events_json: r.events_json || null,
    }));
}

function parseStoredOrderBookValue(val) {
    if (!val) return val;
    if (typeof val === 'number') return val;
    try { return JSON.parse(val); } catch { return val; }
}

function mapOrderBook(rows) {
    return rows.map(r => ({
        time: formatTimestamp(r.timestamp),
        block: r.block,
        hash: r.hash || '',
        extrinsic_id: r.extrinsic_id || '',
        event_type: r.event_type,
        wallet: r.wallet || '',
        order_id: r.order_id || '',
        base_asset: r.base_asset || '',
        quote_asset: r.quote_asset || '',
        side: r.side || '',
        price: parseStoredOrderBookValue(r.price),
        amount: parseStoredOrderBookValue(r.amount),
        usd_value: fmtUsd(r.usd_value),
    }));
}

// Query helper: MV + live buffer UNION ALL pattern
// Returns rows from MV joined with live table, ordered by timestamp DESC
function mvUnion(mvName, liveName, columns) {
    return `SELECT ${columns} FROM sm.${mvName} UNION ALL SELECT ${columns} FROM sm.${liveName}`;
}

// ============================================================
// INIT
// ============================================================

async function initDB() {
    pool = new Pool(PG_CONFIG);
    pool.on('error', (err) => console.error('[db_pg] Pool error:', err.message));

    // Verify connection + load logo cache
    const client = await pool.connect();
    try {
        const res = await client.query('SELECT COUNT(*) as cnt FROM sm.asset_registry');
        console.log(`[db_pg] Connected to PostgreSQL. Assets: ${res.rows[0].cnt}`);

        // Load logo cache (symbol -> logoUrl)
        const logos = await client.query("SELECT symbol, logo FROM sm.asset_registry WHERE logo IS NOT NULL AND logo != ''");
        _logoCache = {};
        for (const r of logos.rows) _logoCache[r.symbol] = r.logo;
        console.log(`[db_pg] Logo cache loaded: ${Object.keys(_logoCache).length} entries`);

        // Load symbol -> asset_id cache (for price lookups) and the reverse
        // asset_id -> { symbol, decimals } map (used to enrich block events).
        const symbols = await client.query("SELECT symbol, asset_id, decimals FROM sm.asset_registry");
        _symbolToAssetId = {};
        _assetIdToInfo = {};
        for (const r of symbols.rows) {
            _symbolToAssetId[r.symbol] = r.asset_id;
            const idLower = String(r.asset_id || '').toLowerCase();
            if (idLower) _assetIdToInfo[idLower] = { symbol: r.symbol, decimals: Number(r.decimals) || 18 };
        }
        console.log(`[db_pg] Asset registry loaded: ${Object.keys(_symbolToAssetId).length} symbols`);
    } finally {
        client.release();
    }

    // Start MV refresh scheduler (every 5 minutes)
    startRefreshScheduler();
}

// ============================================================
// MV REFRESH SCHEDULER
// ============================================================

// Tiered MV refresh: small MVs every 5 min, medium every 30 min, large only on-demand
// REFRESH CONCURRENTLY needs ~1x MV size in temp space. With limited disk, large MVs are skipped.
const MV_SMALL = [
    'sm.mv_bridges',              // 65 MB
    'sm.mv_liquidity_events',     // 43 MB
    'sm.mv_order_book_events',    // 403 MB
];
const MV_MEDIUM = [
    'sm.mv_transfers',            // 1.1 GB
    'sm.mv_fees',                 // 4.2 GB
];
const MV_LARGE = [
    'sm.mv_swaps',                // 7.2 GB — needs ~8GB free
    'sm.mv_extrinsics',           // 6.8 GB — needs ~7GB free
];

// Map MV name → corresponding live table for truncation
const MV_TO_LIVE = {
    'sm.mv_swaps': 'sm.live_swaps',
    'sm.mv_transfers': 'sm.live_transfers',
    'sm.mv_bridges': 'sm.live_bridges',
    'sm.mv_fees': 'sm.live_fees',
    'sm.mv_liquidity_events': 'sm.live_liquidity_events',
    'sm.mv_order_book_events': 'sm.live_order_book_events',
    'sm.mv_extrinsics': 'sm.live_extrinsics',
};

let refreshing = false;

// Check available disk space inside the PG container (returns GB free)
async function getPgFreeSpaceGB() {
    try {
        const res = await pool.query(
            "SELECT pg_size_pretty(setting::bigint * pg_size_bytes('1 block')) FROM pg_settings WHERE name = 'block_size' LIMIT 1"
        );
        // Approximate: use df on the data dir via a temp table trick — too complex.
        // Instead, use pg_tablespace_size heuristic: if total DB is ~47GB on 96GB disk, estimate.
        // Fallback: just return -1 to indicate unknown.
        return -1;
    } catch { return -1; }
}

async function refreshMVs(mvList, label) {
    const start = Date.now();
    const refreshed = [];
    const client = await pool.connect();
    await client.query('SET statement_timeout = 0');
    try {
        for (const mv of mvList) {
            try {
                await client.query(`REFRESH MATERIALIZED VIEW CONCURRENTLY ${mv}`);
                refreshed.push(mv);
            } catch (err) {
                // If disk full, stop refreshing remaining MVs in this tier
                if (err.message.includes('No space left')) {
                    console.error(`[db_pg] MV refresh ${mv}: disk full, skipping remaining ${label} MVs`);
                    break;
                }
                console.error(`[db_pg] MV refresh ${mv} error: ${err.message}`);
            }
        }
    } finally {
        await client.query(`SET statement_timeout = ${PG_CONFIG.statement_timeout || 30000}`).catch(() => {});
        client.release();
    }
    // Only truncate live tables for successfully refreshed MVs.
    // GUARD: a REFRESH "succeeds" even when subsquid is stalled (it just re-runs
    // the MV query over a frozen history_element). Truncating live_* in that case
    // permanently drops the only copy of recent rows (the WS indexer buffer),
    // since the MV never absorbed them. Only truncate when subsquid has caught up
    // close to the newest row buffered in the live table.
    const SQUID_LAG_TOLERANCE_BLOCKS = 300; // ~30 min of 6s blocks
    let squidHeight = null;
    try {
        const r = await pool.query('SELECT height FROM squid_processor.status LIMIT 1');
        squidHeight = Number(r.rows[0] && r.rows[0].height);
    } catch (e) {
        console.error(`[db_pg] TRUNCATE guard: cannot read squid status (${e.message}); keeping all live_* this cycle`);
    }
    for (const mv of refreshed) {
        const live = MV_TO_LIVE[mv];
        if (!live) continue;
        if (squidHeight == null || !Number.isFinite(squidHeight)) continue; // fail-safe: keep live data
        try {
            const lr = await pool.query(`SELECT COALESCE(MAX(block), 0) AS maxb FROM ${live}`);
            const liveMaxBlock = Number(lr.rows[0] && lr.rows[0].maxb || 0);
            if (liveMaxBlock - squidHeight > SQUID_LAG_TOLERANCE_BLOCKS) {
                console.warn(`[db_pg] SKIP TRUNCATE ${live}: subsquid behind (squid=${squidHeight}, live_max_block=${liveMaxBlock}); keeping ${live} so frontend serves it via UNION`);
                continue;
            }
            if (live === 'sm.live_bridges') {
                // Subsquid only indexes ethBridge.transferToSidechain → only
                // Ethereum reaches mv_bridges. Hashi v2 (Substrate / Parachain
                // / TON) lives exclusively in live_bridges; truncating would
                // permanently drop those rows.
                await pool.query(`DELETE FROM ${live} WHERE network = 'Ethereum'`);
            } else {
                await pool.query(`TRUNCATE ${live}`);
            }
        } catch (e) {
            console.error(`[db_pg] TRUNCATE guard ${live} skipped on error: ${e.message}`);
        }
    }
    if (refreshed.length > 0) {
        const elapsed = ((Date.now() - start) / 1000).toFixed(1);
        console.log(`[db_pg] MV refresh (${label}): ${refreshed.length}/${mvList.length} in ${elapsed}s`);
    }
    return refreshed.length;
}

async function refreshSmallMVs() {
    if (refreshing) return;
    refreshing = true;
    try {
        await refreshMVs(MV_SMALL, 'small');
    } finally { refreshing = false; }
}

let mediumRefreshCount = 0;
async function refreshScheduled() {
    if (refreshing) return;
    refreshing = true;
    try {
        // Small MVs every cycle (5 min)
        await refreshMVs(MV_SMALL, 'small');
        // Medium MVs every 6th cycle (30 min)
        mediumRefreshCount++;
        if (mediumRefreshCount >= 6) {
            mediumRefreshCount = 0;
            await refreshMVs(MV_MEDIUM, 'medium');
        }
        // Large MVs: NOT auto-refreshed (need VPS disk upgrade)
        // Trigger manually: refreshMVs(MV_LARGE, 'large')
    } finally { refreshing = false; }
}

function startRefreshScheduler() {
    setInterval(refreshScheduled, 5 * 60 * 1000);
}

// ============================================================
// INSERT FUNCTIONS (write to sm.live_* buffer tables)
// ============================================================

async function insertTransfer(t) {
    try {
        await pool.query(
            `INSERT INTO sm.live_transfers (timestamp, formatted_time, block, from_addr, to_addr, amount, symbol, logo, usd_value, asset_id, hash, extrinsic_id)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)`,
            [Date.now(), t.time, t.block || 0, t.from, t.to, t.amount, t.symbol, t.logo, parseFloat(t.usdValue) || 0, t.assetId, t.hash || '', t.extrinsic_id || '']
        );
    } catch (err) { console.error('[db_pg] insertTransfer error:', err.message); }
}

async function insertSwap(s) {
    try {
        await pool.query(
            `INSERT INTO sm.live_swaps (timestamp, formatted_time, block, wallet, in_symbol, in_amount, in_logo, in_usd, out_symbol, out_amount, out_logo, out_usd, hash, extrinsic_id)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)`,
            [Date.now(), s.time, s.block, s.wallet, s.in.symbol, s.in.amount, s.in.logo, parseFloat(s.in.usd) || 0, s.out.symbol, s.out.amount, s.out.logo, parseFloat(s.out.usd) || 0, s.hash || '', s.extrinsic_id || '']
        );
    } catch (err) { console.error('[db_pg] insertSwap error:', err.message); }
}

async function insertBridge(b) {
    try {
        await pool.query(
            `INSERT INTO sm.live_bridges (timestamp, block, network, direction, sender, recipient, asset_id, symbol, logo, amount, usd_value, hash, extrinsic_id)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)`,
            [Date.now(), b.block, b.network, b.direction, b.sender, b.recipient, b.assetId || b.asset_id, b.symbol || 'UNK', b.logo || '', b.amount, b.usdValue || b.usd_value || 0, b.hash || '', b.extrinsic_id || '']
        );
    } catch (err) { console.error('[db_pg] insertBridge error:', err.message); }
}

async function insertFee(f) {
    try {
        await pool.query(
            `INSERT INTO sm.live_fees (timestamp, block, type, amount, usd_value, denom_factor)
             VALUES ($1, $2, $3, $4, $5, $6)`,
            [Date.now(), f.block, f.type, f.amount, f.usdValue, f.denomFactor || '1']
        );
    } catch (err) { console.error('[db_pg] insertFee error:', err.message); }
}

async function insertExtrinsic(e) {
    try {
        await pool.query(
            `INSERT INTO sm.live_extrinsics (timestamp, formatted_time, block, extrinsic_index, hash, section, method, signer, success, args_json, error_msg, events_json)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)`,
            [e.timestamp || Date.now(), e.formatted_time || '', e.block || 0, e.extrinsic_index || 0, e.hash || '', e.section || '', e.method || '', e.signer || 'System', e.success ? 1 : 0, e.args_json || '{}', e.error_msg || '', e.events_json || null]
        );
    } catch (err) { console.error('[db_pg] insertExtrinsic error:', err.message); }
}

async function insertLiquidityEvent(event) {
    try {
        await pool.query(
            `INSERT INTO sm.live_liquidity_events (timestamp, block, wallet, pool_base, pool_target, base_amount, target_amount, usd_value, type, hash, extrinsic_id)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)`,
            [Date.now(), event.block || 0, event.wallet, event.poolBase, event.poolTarget, event.baseAmount, event.targetAmount, event.usdValue || 0, event.type, event.hash || '', event.extrinsic_id || '']
        );
    } catch (err) { console.error('[db_pg] insertLiquidityEvent error:', err.message); }
}

async function insertOrderBookEvent(e) {
    try {
        await pool.query(
            `INSERT INTO sm.live_order_book_events (timestamp, formatted_time, block, event_type, wallet, order_id, base_asset, quote_asset, side, price, amount, usd_value, hash, extrinsic_id)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)`,
            [e.timestamp || Date.now(), e.formatted_time || '', e.block || 0, e.event_type || '', e.wallet || '', e.order_id || '', e.base_asset || '', e.quote_asset || '', e.side || '', e.price || '', e.amount || '', e.usd_value || 0, e.hash || '', e.extrinsic_id || '']
        );
    } catch (err) { console.error('[db_pg] insertOrderBookEvent error:', err.message); }
}

async function insertSupplySnapshot(symbol, assetId, totalSupply) {
    try {
        await pool.query(
            `INSERT INTO sm.supply_snapshots (timestamp, symbol, asset_id, total_supply)
             VALUES ($1, $2, $3, $4)`,
            [Date.now(), symbol, assetId, totalSupply]
        );
    } catch (err) { console.error('[db_pg] insertSupplySnapshot error:', err.message); }
}

// ============================================================
// IDENTITY FUNCTIONS
// ============================================================

async function upsertIdentity(address, display, email, web, twitter, discord) {
    await pool.query(
        `INSERT INTO sm.identity_cache (address, display, email, web, twitter, discord, updated_at)
         VALUES ($1, $2, $3, $4, $5, $6, $7)
         ON CONFLICT (address) DO UPDATE SET
            display = EXCLUDED.display, email = EXCLUDED.email, web = EXCLUDED.web,
            twitter = EXCLUDED.twitter, discord = EXCLUDED.discord, updated_at = EXCLUDED.updated_at`,
        [address, display || null, email || null, web || null, twitter || null, discord || null, Date.now()]
    );
}

async function upsertIdentityBatch(identities) {
    const client = await pool.connect();
    try {
        await client.query('BEGIN');
        for (const id of identities) {
            await client.query(
                `INSERT INTO sm.identity_cache (address, display, email, web, twitter, discord, updated_at)
                 VALUES ($1, $2, $3, $4, $5, $6, $7)
                 ON CONFLICT (address) DO UPDATE SET
                    display = EXCLUDED.display, email = EXCLUDED.email, web = EXCLUDED.web,
                    twitter = EXCLUDED.twitter, discord = EXCLUDED.discord, updated_at = EXCLUDED.updated_at`,
                [id.address, id.display || null, id.email || null, id.web || null, id.twitter || null, id.discord || null, Date.now()]
            );
        }
        await client.query('COMMIT');
    } catch (err) {
        await client.query('ROLLBACK');
        throw err;
    } finally {
        client.release();
    }
}

async function getIdentities(addresses) {
    if (!addresses || !addresses.length) return {};
    const res = await pool.query(
        'SELECT address, display, email, web, twitter, discord, updated_at FROM sm.identity_cache WHERE address = ANY($1::text[])',
        [addresses]
    );
    const out = {};
    for (const r of res.rows) out[r.address] = r;
    return out;
}

async function getAllCachedIdentities() {
    const res = await pool.query('SELECT address, display FROM sm.identity_cache WHERE display IS NOT NULL');
    return res.rows;
}

// ============================================================
// FEE UTILITY
// ============================================================

async function fixFeeDenomFactor(correctFactor) {
    const res = await pool.query(
        "UPDATE sm.live_fees SET denom_factor = $1 WHERE denom_factor = '1' OR denom_factor IS NULL",
        [correctFactor]
    );
    return res.rowCount;
}

// ============================================================
// SUPPLY FUNCTIONS
// ============================================================

async function getSupplyHistory(symbol, startTime, assetId, genesisSupply) {
    const rows = [];
    const startTimeSec = Math.floor(startTime / 1000);

    // Non-XOR tokens (VAL, PSWAP, TBCD, KUSD): always use MOF circulating snapshots.
    // On-chain totalIssuance includes locked/vesting tokens and does NOT match circulating supply.
    // XOR: use on-chain data (totalIssuance is reliable after CoinGecko/denomination handling).
    const isXor = symbol === 'XOR';

    if (!isXor) {
        // Non-XOR: MOF circulating snapshots (accurate, available since mid-March 2026)
        const snapRes = await pool.query(
            `SELECT timestamp, total_supply FROM sm.supply_snapshots
             WHERE symbol = $1 AND timestamp >= $2 ORDER BY timestamp ASC`,
            [symbol, startTime]
        );
        for (const r of snapRes.rows) {
            rows.push({ timestamp: Number(r.timestamp), total_supply: r.total_supply });
        }

        // For timeframes that go before MOF data: prepend on-chain totalIssuance
        // adjusted by a constant offset so the transition is seamless.
        // offset = totalIssuance(at junction) - MOF(at junction)
        const mofStart = rows.length > 0 ? rows[0].timestamp : Date.now();
        const needsHistory = startTime < mofStart;

        if (needsHistory) {
            const mofFirstVal = rows.length > 0 ? rows[0].total_supply : null;
            const mofFirstSec = Math.floor(mofStart / 1000);

            if (mofFirstVal) {
                // Gather on-chain data from both sources before MOF period
                const chainRows = [];

                // Source 1: subsquid asset_snapshot (DAY granularity, only if assetId known)
                if (assetId) {
                    const chainRes = await pool.query(
                        `SELECT timestamp, (supply::numeric / 1e18)::float8 as total_supply
                         FROM asset_snapshot
                         WHERE asset_id = $1 AND type = 'DAY' AND timestamp >= $2 AND timestamp < $3
                         ORDER BY timestamp ASC`,
                        [assetId, startTimeSec, mofFirstSec]
                    );
                    for (const r of chainRes.rows) {
                        chainRows.push({ timestamp: r.timestamp * 1000, total_supply: r.total_supply });
                    }
                }

                // Source 2: supply_history backfiller (fills gaps in subsquid)
                const backfillRes = await pool.query(
                    `SELECT timestamp, total_issuance as total_supply
                     FROM sm.supply_history
                     WHERE symbol = $1 AND timestamp >= $2 AND timestamp < $3
                     ORDER BY timestamp ASC`,
                    [symbol, startTimeSec, mofFirstSec]
                );
                for (const r of backfillRes.rows) {
                    chainRows.push({ timestamp: Number(r.timestamp) * 1000, total_supply: r.total_supply });
                }

                // Deduplicate (one per day, keep first), find the latest to compute offset
                chainRows.sort((a, b) => a.timestamp - b.timestamp);
                const daySet = new Set();
                const deduped = [];
                for (const r of chainRows) {
                    const dk = Math.floor(r.timestamp / 86400000);
                    if (!daySet.has(dk)) { daySet.add(dk); deduped.push(r); }
                }

                if (deduped.length > 0) {
                    // Offset between totalIssuance and circulating grows over time
                    // (locked/vesting tokens accumulate). Use linear interpolation:
                    // - At genesis (April 2021): offset ≈ 0 (all tokens circulated)
                    // - At junction (last on-chain before MOF): offset = lastChain - mofFirstVal
                    // Always interpolate relative to genesis, not the query start.
                    const lastChain = deduped[deduped.length - 1].total_supply;
                    const totalOffset = lastChain - mofFirstVal;
                    const genesisTs = (genesisSupply && genesisSupply.timestamp) || deduped[0].timestamp;
                    const junctionTs = deduped[deduped.length - 1].timestamp;
                    const totalSpan = junctionTs - genesisTs;

                    const historical = deduped.map(r => {
                        const progress = totalSpan > 0 ? (r.timestamp - genesisTs) / totalSpan : 1;
                        const interpolatedOffset = totalOffset * progress;
                        return {
                            timestamp: r.timestamp,
                            total_supply: r.total_supply - interpolatedOffset
                        };
                    });
                    rows.unshift(...historical);
                }
            }
        }
    } else {
        const recentCutoff = Date.now() - (2 * 86400000);
        const useOnlyMof = (startTime > recentCutoff);

        if (useOnlyMof) {
            const snapRes = await pool.query(
                `SELECT timestamp, total_supply FROM sm.supply_snapshots
                 WHERE symbol = $1 AND timestamp >= $2 ORDER BY timestamp ASC`,
                [symbol, startTime]
            );
            for (const r of snapRes.rows) {
                rows.push({ timestamp: Number(r.timestamp), total_supply: r.total_supply });
            }
        } else {
            // XOR: on-chain totalIssuance (daily resolution)
            if (assetId) {
                const chainRes = await pool.query(
                    `SELECT timestamp, (supply::numeric / 1e18)::float8 as total_supply
                     FROM asset_snapshot
                     WHERE asset_id = $1 AND type = 'DAY' AND timestamp >= $2
                     ORDER BY timestamp ASC`,
                    [assetId, startTimeSec]
                );
                for (const r of chainRes.rows) {
                    rows.push({ timestamp: r.timestamp * 1000, total_supply: r.total_supply });
                }
            }
            const backfillRes = await pool.query(
                `SELECT timestamp, total_issuance as total_supply
                 FROM sm.supply_history
                 WHERE symbol = $1 AND timestamp >= $2
                 ORDER BY timestamp ASC`,
                [symbol, startTimeSec]
            );
            for (const r of backfillRes.rows) {
                rows.push({ timestamp: Number(r.timestamp) * 1000, total_supply: r.total_supply });
            }
        }
    }

    // Deduplicate: one point per day, keep first
    const seen = new Set();
    const unique = [];
    rows.sort((a, b) => a.timestamp - b.timestamp);
    for (const r of rows) {
        const dayKey = Math.floor(r.timestamp / 86400000);
        if (!seen.has(dayKey)) {
            seen.add(dayKey);
            unique.push(r);
        }
    }

    // Prepend genesis supply as anchor point (XOR only — for non-XOR tokens,
    // genesis is total planned supply, not circulating, so it would distort the chart)
    if (isXor && genesisSupply && genesisSupply.supply && genesisSupply.timestamp) {
        if (startTime === 0 || genesisSupply.timestamp >= startTime) {
            if (unique.length === 0 || genesisSupply.timestamp < unique[0].timestamp) {
                unique.unshift({ timestamp: genesisSupply.timestamp, total_supply: genesisSupply.supply });
            }
        }
    }

    return unique;
}

async function getSupplySnapshotDelta(symbol, startTime) {
    const res = await pool.query(`
        SELECT
            (SELECT total_supply FROM sm.supply_snapshots WHERE symbol = $1 AND timestamp >= $2 ORDER BY timestamp ASC LIMIT 1) as first_supply,
            (SELECT total_supply FROM sm.supply_snapshots WHERE symbol = $1 AND timestamp >= $2 ORDER BY timestamp DESC LIMIT 1) as last_supply
    `, [symbol, startTime]);
    return {
        firstSupply: res.rows[0]?.first_supply || 0,
        lastSupply: res.rows[0]?.last_supply || 0,
    };
}

async function getLatestSupplySnapshot(symbol) {
    const res = await pool.query(
        'SELECT total_supply, timestamp FROM sm.supply_snapshots WHERE symbol = $1 ORDER BY timestamp DESC LIMIT 1',
        [symbol]
    );
    return res.rows[0] || null;
}

async function purgeSupplySnapshotsForSymbol(symbol) {
    const res = await pool.query('DELETE FROM sm.supply_snapshots WHERE symbol = $1', [symbol]);
    return res.rowCount;
}

// ============================================================
// READ FUNCTIONS — Paginated queries use MV + live UNION ALL
// ============================================================

// Column lists for UNION ALL — only columns that exist in BOTH MV and live tables.
// Logos resolved via _logoCache in mappers.
// args_json/events_json resolved on demand in getExtrinsicDetail.
const SWAP_COLS = 'timestamp, block, wallet, in_symbol, in_amount, in_usd, out_symbol, out_amount, out_usd, hash, extrinsic_id';
const TRANSFER_COLS = 'timestamp, block, from_addr, to_addr, amount, symbol, usd_value, asset_id, hash, extrinsic_id';
const BRIDGE_COLS = 'timestamp, block, network, direction, sender, recipient, asset_id, symbol, amount, usd_value, hash, extrinsic_id';
const FEE_COLS = 'timestamp, block, type, amount, usd_value, denom_factor';
const LIQ_COLS = 'timestamp, block, wallet, pool_base, pool_target, base_amount, target_amount, usd_value, type, hash, extrinsic_id';
const OB_COLS = 'timestamp, block, event_type, wallet, order_id, base_asset, quote_asset, side, price, amount, usd_value, hash, extrinsic_id';
const EXT_COLS = 'timestamp, block, extrinsic_index, hash, section, method, signer, success, error_msg';

// Approximate count cache per MV (30s TTL). Avoids COUNT(*) on million-row tables.
const _approxCountCache = {};
const APPROX_COUNT_TTL = 30000;
const MAX_OFFSET = 500000; // Cap OFFSET to prevent deep pagination timeouts

// Reprocessed blocks (WS reconnect / PM2 restart, no ON CONFLICT on live_*) insert byte-identical rows differing only by the Date.now() insert `timestamp`. Collapse those; legit multi-leg rows differ in amount/symbol → kept.
function dedupeLiveRows(rows) {
    const seen = new Set();
    const out = [];
    for (const r of rows) {
        const { timestamp, ...chain } = r;
        const k = JSON.stringify(chain);
        if (seen.has(k)) continue;
        seen.add(k);
        out.push(r);
    }
    return out;
}

// Generic paginated query helper
async function paginatedQuery(mvName, liveName, cols, where, params, orderBy, page, limit, mapFn) {
    const isUnfiltered = !where || where.trim() === '';
    let total;

    if (isUnfiltered) {
        // Use approximate count from pg_class for unfiltered queries (fast)
        const cacheKey = mvName;
        const cached = _approxCountCache[cacheKey];
        if (cached && Date.now() - cached.ts < APPROX_COUNT_TTL) {
            total = cached.count;
        } else {
            const approxRes = await pool.query(
                "SELECT reltuples::bigint AS cnt FROM pg_class WHERE relname = $1",
                [mvName]
            );
            total = parseInt(approxRes.rows[0]?.cnt) || 0;
            _approxCountCache[cacheKey] = { count: total, ts: Date.now() };
        }
    } else {
        // Filtered queries need exact count
        const base = `(SELECT ${cols} FROM sm.${mvName} ${where} UNION ALL SELECT ${cols} FROM sm.${liveName} ${where})`;
        const countSql = `SELECT COUNT(*) as count FROM ${base} AS _u`;
        const countRes = await pool.query(countSql, params);
        total = parseInt(countRes.rows[0].count) || 0;
    }

    // Bidirectional pagination: first N pages forward (DESC), last N pages backward (ASC + reverse)
    // This gives efficient access to BOTH ends of the dataset without deep OFFSET scans
    const maxAccessiblePage = Math.floor(MAX_OFFSET / limit);
    const totalPages = Math.ceil(total / limit);
    const safePage = Math.max(1, Math.min(page, totalPages));

    const distFromStart = safePage - 1;                // pages from the beginning
    const distFromEnd = totalPages - safePage;          // pages from the end
    const useReverse = distFromEnd < distFromStart && distFromEnd < maxAccessiblePage;
    // Flip ORDER BY for reverse: 'timestamp DESC' → 'timestamp ASC'
    const reverseOrder = orderBy.replace(/DESC/gi, '__ASC__').replace(/ASC(?!__)/gi, 'DESC').replace(/__ASC__/g, 'ASC');

    let dataRes;
    if (distFromStart < 1000 / limit) {
        // Early pages: MV + live table, merge in JS (fast, index-friendly)
        const offset = distFromStart * limit;
        const liveRes = await pool.query(
            `SELECT ${cols} FROM sm.${liveName} ${where} ORDER BY ${orderBy}`,
            params
        );
        const liveRows = dedupeLiveRows(liveRes.rows);

        if (offset < liveRows.length) {
            // Page overlaps live data — merge live + MV
            const needed = limit + offset;
            const mvRes = await pool.query(
                `SELECT ${cols} FROM sm.${mvName} ${where} ORDER BY ${orderBy} LIMIT $${params.length + 1} OFFSET $${params.length + 2}`,
                [...params, needed, 0]
            );
            const combined = [...liveRows, ...mvRes.rows].sort((a, b) => Number(b.timestamp) - Number(a.timestamp));
            dataRes = { rows: combined.slice(offset, offset + limit) };
        } else {
            // Page is entirely in MV territory — query MV directly
            const mvOffset = offset - liveRows.length;
            const mvRes = await pool.query(
                `SELECT ${cols} FROM sm.${mvName} ${where} ORDER BY ${orderBy} LIMIT $${params.length + 1} OFFSET $${params.length + 2}`,
                [...params, limit, mvOffset]
            );
            dataRes = mvRes;
        }
    } else if (useReverse) {
        // Near-end pages: query MV in reverse direction with small offset, then reverse results
        const reverseOffset = distFromEnd * limit;
        const dataSql = `SELECT ${cols} FROM sm.${mvName} ${where} ORDER BY ${reverseOrder} LIMIT $${params.length + 1} OFFSET $${params.length + 2}`;
        dataRes = await pool.query(dataSql, [...params, limit, reverseOffset]);
        dataRes.rows.reverse(); // Restore DESC order within the page
    } else {
        // Middle pages: query MV directly with capped offset (may be slow for very deep pages)
        const offset = Math.min(distFromStart * limit, MAX_OFFSET);
        const dataSql = `SELECT ${cols} FROM sm.${mvName} ${where} ORDER BY ${orderBy} LIMIT $${params.length + 1} OFFSET $${params.length + 2}`;
        dataRes = await pool.query(dataSql, [...params, limit, offset]);
    }

    return { data: mapFn ? mapFn(dataRes.rows) : dataRes.rows, total, page: safePage, totalPages };
}

// --- SWAPS ---

async function getSwaps(address, page = 1, limit = 25) {
    return paginatedQuery(
        'mv_swaps', 'live_swaps', SWAP_COLS,
        'WHERE wallet = $1', [address],
        'timestamp DESC', page, limit, mapSwaps
    );
}

async function getLatestSwaps(page = 1, limit = 25, filter = null, timestamp = null) {
    let where = '';
    const params = [];
    const conditions = [];

    if (filter) {
        const f = `%${filter.toUpperCase()}%`;
        params.push(f, f);
        conditions.push(`(UPPER(in_symbol) LIKE $${params.length - 1} OR UPPER(out_symbol) LIKE $${params.length})`);
    }
    if (timestamp) {
        params.push(timestamp);
        conditions.push(`timestamp <= $${params.length}`);
    }
    if (conditions.length) where = 'WHERE ' + conditions.join(' AND ');

    return paginatedQuery('mv_swaps', 'live_swaps', SWAP_COLS, where, params, 'timestamp DESC', page, limit, mapSwaps);
}

// --- TRANSFERS ---

async function getTransfers(address, page = 1, limit = 25) {
    return paginatedQuery(
        'mv_transfers', 'live_transfers', TRANSFER_COLS,
        'WHERE from_addr = $1 OR to_addr = $1', [address],
        'timestamp DESC', page, limit, mapTransfers
    );
}

async function getLatestTransfers(page = 1, limit = 25, filter = null, timestamp = null) {
    let where = '';
    const params = [];
    const conditions = [];

    if (filter) {
        const f = `%${filter.toUpperCase()}%`;
        params.push(f, f, f);
        conditions.push(`(UPPER(symbol) LIKE $${params.length - 2} OR UPPER(from_addr) LIKE $${params.length - 1} OR UPPER(to_addr) LIKE $${params.length})`);
    }
    if (timestamp) {
        params.push(timestamp);
        conditions.push(`timestamp <= $${params.length}`);
    }
    if (conditions.length) where = 'WHERE ' + conditions.join(' AND ');

    return paginatedQuery('mv_transfers', 'live_transfers', TRANSFER_COLS, where, params, 'timestamp DESC', page, limit, mapTransfers);
}

// --- BRIDGES ---

async function getWalletBridges(address, page = 1, limit = 20) {
    const result = await paginatedQuery(
        'mv_bridges', 'live_bridges', BRIDGE_COLS,
        'WHERE sender = $1 OR recipient = $1', [address],
        'timestamp DESC', page, limit, null
    );
    result.data = result.data.map(r => ({ ...r, time: formatTimestamp(r.timestamp), usd_value: fmtUsd(r.usd_value) }));
    return result;
}

async function getLatestBridges(page = 1, limit = 20, filter = null, timestamp = null) {
    let where = '';
    const params = [];
    const conditions = [];

    if (filter) {
        const f = `%${filter.toUpperCase()}%`;
        params.push(f, f, f, f);
        conditions.push(`(UPPER(sender) LIKE $${params.length - 3} OR UPPER(recipient) LIKE $${params.length - 2} OR UPPER(network) LIKE $${params.length - 1} OR UPPER(asset_id) LIKE $${params.length})`);
    }
    if (timestamp) {
        params.push(timestamp);
        conditions.push(`timestamp <= $${params.length}`);
    }
    if (conditions.length) where = 'WHERE ' + conditions.join(' AND ');

    const result = await paginatedQuery('mv_bridges', 'live_bridges', BRIDGE_COLS, where, params, 'timestamp DESC', page, limit, null);
    result.data = result.data.map(r => ({ ...r, time: formatTimestamp(r.timestamp), usd_value: fmtUsd(r.usd_value) }));
    return result;
}

// --- EXTRINSICS ---

let _extCountCache = { count: 0, ts: 0 };

async function getLatestExtrinsics(page = 1, limit = 25, section = null, timestamp = null, block = null, success = null, method = null) {
    let where = '';
    const params = [];
    const conditions = ["NOT (section = 'timestamp' AND method = 'set')"];

    if (section) { params.push(section); conditions.push(`section = $${params.length}`); }
    if (method) { params.push(`%${method}%`); conditions.push(`method LIKE $${params.length}`); }
    if (timestamp) { params.push(timestamp); conditions.push(`timestamp <= $${params.length}`); }
    if (block !== null && block !== undefined) { params.push(block); conditions.push(`block = $${params.length}`); }
    if (success !== null && success !== undefined) { params.push(success); conditions.push(`success = $${params.length}`); }

    where = 'WHERE ' + conditions.join(' AND ');

    const isUnfiltered = !section && !timestamp && block === null && success === null && !method;
    let total;

    if (isUnfiltered && Date.now() - _extCountCache.ts < 30000) {
        total = _extCountCache.count;
    } else {
        // For unfiltered, use approximate count for speed
        if (isUnfiltered) {
            const approxRes = await pool.query("SELECT reltuples::bigint AS cnt FROM pg_class WHERE relname = 'mv_extrinsics'");
            total = parseInt(approxRes.rows[0]?.cnt) || 0;
            _extCountCache = { count: total, ts: Date.now() };
        } else {
            const countSql = `SELECT COUNT(*) as count FROM (SELECT ${EXT_COLS} FROM sm.mv_extrinsics ${where} UNION ALL SELECT ${EXT_COLS} FROM sm.live_extrinsics ${where}) AS _u`;
            const countRes = await pool.query(countSql, params);
            total = parseInt(countRes.rows[0].count) || 0;
        }
    }

    const maxAccessiblePage = Math.floor(MAX_OFFSET / limit);
    const totalPages = Math.ceil(total / limit);
    const safePage = Math.max(1, Math.min(page, totalPages));
    const distFromStart = safePage - 1;
    const distFromEnd = totalPages - safePage;
    const useReverse = distFromEnd < distFromStart && distFromEnd < maxAccessiblePage;

    let dataRes;
    if (distFromStart < 1000 / limit) {
        // Early pages: MV + live table, merge in JS (fast, index-friendly)
        const offset = distFromStart * limit;
        const liveRes = await pool.query(
            `SELECT ${EXT_COLS} FROM sm.live_extrinsics ${where} ORDER BY timestamp DESC`,
            params
        );
        const liveRows = dedupeLiveRows(liveRes.rows);
        const needed = limit + offset - liveRows.length;

        if (needed > 0) {
            const mvOffset = Math.max(0, offset - liveRows.length);
            const mvRes = await pool.query(
                `SELECT ${EXT_COLS} FROM sm.mv_extrinsics ${where} ORDER BY timestamp DESC LIMIT $${params.length + 1} OFFSET $${params.length + 2}`,
                [...params, needed, mvOffset]
            );
            const combined = [...liveRows, ...mvRes.rows].sort((a, b) => Number(b.timestamp) - Number(a.timestamp));
            dataRes = { rows: combined.slice(offset, offset + limit) };
        } else {
            dataRes = { rows: liveRows.slice(offset, offset + limit) };
        }
    } else if (useReverse) {
        // Near-end pages: query MV in reverse (ASC) with small offset, then flip results
        const reverseOffset = distFromEnd * limit;
        const dataSql = `SELECT ${EXT_COLS} FROM sm.mv_extrinsics ${where} ORDER BY timestamp ASC LIMIT $${params.length + 1} OFFSET $${params.length + 2}`;
        dataRes = await pool.query(dataSql, [...params, limit, reverseOffset]);
        dataRes.rows.reverse();
    } else {
        // Middle pages: query MV directly with capped offset
        const offset = Math.min(distFromStart * limit, MAX_OFFSET);
        const dataSql = `SELECT ${EXT_COLS} FROM sm.mv_extrinsics ${where} ORDER BY timestamp DESC LIMIT $${params.length + 1} OFFSET $${params.length + 2}`;
        dataRes = await pool.query(dataSql, [...params, limit, offset]);
    }

    return { data: mapExtrinsics(dataRes.rows), total, page: safePage, totalPages };
}

let _sectionCache = { data: null, ts: 0 };

async function getExtrinsicSections() {
    if (_sectionCache.data && Date.now() - _sectionCache.ts < 300000) return _sectionCache.data;
    const res = await pool.query(
        `SELECT DISTINCT section FROM (SELECT section FROM sm.mv_extrinsics UNION SELECT section FROM sm.live_extrinsics) AS _u WHERE section != 'timestamp' ORDER BY section ASC`
    );
    const data = res.rows.map(r => r.section);
    _sectionCache = { data, ts: Date.now() };
    return data;
}

async function getExtrinsicsByAddress(address, page = 1, limit = 25) {
    return paginatedQuery(
        'mv_extrinsics', 'live_extrinsics', EXT_COLS,
        'WHERE signer = $1', [address],
        'timestamp DESC', page, limit, mapExtrinsics
    );
}

async function getExtrinsicDetail(block, extrinsicIndex) {
    // First get basic info from MV/live
    const res = await pool.query(
        `SELECT ${EXT_COLS} FROM (SELECT ${EXT_COLS} FROM sm.mv_extrinsics WHERE block = $1 AND extrinsic_index = $2
         UNION ALL SELECT ${EXT_COLS} FROM sm.live_extrinsics WHERE block = $1 AND extrinsic_index = $2) AS _u LIMIT 1`,
        [block, extrinsicIndex]
    );
    if (!res.rows[0]) return null;

    const row = res.rows[0];

    // Resolve args_json from history_element (not stored in MV to save disk)
    const heRes = await pool.query(
        'SELECT COALESCE(data::text, \'{}\'::text) AS args_json FROM history_element WHERE id = $1 LIMIT 1',
        [row.hash]
    );
    row.args_json = heRes.rows[0]?.args_json || '{}';

    // Resolve events_json: try live_extrinsics first (has events from blockchain listener),
    // then try subsquid history_element EVENTs for historical data
    let eventsJson = null;
    const liveEvRes = await pool.query(
        'SELECT events_json FROM sm.live_extrinsics WHERE block = $1 AND extrinsic_index = $2 LIMIT 1',
        [block, extrinsicIndex]
    );
    if (liveEvRes.rows[0]?.events_json) {
        eventsJson = liveEvRes.rows[0].events_json;
    } else {
        // For historical extrinsics: query EVENTs from history_element in same block
        // that reference this extrinsic via id prefix pattern (blockHeight-*)
        try {
            const evRes = await pool.query(
                `SELECT module, method, data
                 FROM history_element
                 WHERE type = 'EVENT'
                   AND block_height = $1
                   AND id LIKE $2
                 ORDER BY id ASC
                 LIMIT 50`,
                [block, row.hash + '%']
            );
            if (evRes.rows.length > 0) {
                const events = evRes.rows
                    .filter(e => !(e.module === 'system' && (e.method === 'ExtrinsicSuccess' || e.method === 'ExtrinsicFailed')))
                    .map(e => ({
                        s: e.module,
                        m: e.method,
                        d: e.data ? (typeof e.data === 'string' ? e.data : JSON.stringify(e.data)) : null
                    }));
                if (events.length > 0) eventsJson = JSON.stringify(events);
            }
        } catch (e) { /* subsquid event lookup failed, leave null */ }
    }
    // Fallback 3: sm.extrinsic_events from Rust backfiller (243M decoded events)
    if (!eventsJson) {
        try {
            const bfRes = await pool.query(
                `SELECT section, method, data FROM sm.extrinsic_events
                 WHERE block_height = $1 AND extrinsic_index = $2
                 ORDER BY event_index ASC LIMIT 100`,
                [block, extrinsicIndex]
            );
            if (bfRes.rows.length > 0) {
                const events = bfRes.rows
                    .filter(e => !(e.section === "System" && (e.method === "ExtrinsicSuccess" || e.method === "ExtrinsicFailed")))
                    .map(e => ({
                        s: e.section,
                        m: e.method,
                        d: e.data ? JSON.stringify(e.data) : null
                    }));
                if (events.length > 0) eventsJson = JSON.stringify(events);
            }
        } catch (e) { /* backfill events lookup failed */ }
    }
    row.events_json = eventsJson;

    return mapExtrinsics([row])[0];
}

// --- ORDER BOOK ---

async function getLatestOrderBookEvents(page = 1, limit = 25, type = null, timestamp = null) {
    let where = '';
    const params = [];
    const conditions = [];

    if (type) { params.push(type); conditions.push(`event_type = $${params.length}`); }
    if (timestamp) { params.push(timestamp); conditions.push(`timestamp <= $${params.length}`); }
    if (conditions.length) where = 'WHERE ' + conditions.join(' AND ');

    return paginatedQuery('mv_order_book_events', 'live_order_book_events', OB_COLS, where, params, 'timestamp DESC', page, limit, mapOrderBook);
}

async function getOrderBookByAddress(address, page = 1, limit = 25) {
    return paginatedQuery(
        'mv_order_book_events', 'live_order_book_events', OB_COLS,
        'WHERE wallet = $1', [address],
        'timestamp DESC', page, limit, mapOrderBook
    );
}

// --- LIQUIDITY ---

async function getLiquidityEvents(page = 1, limit = 25, timestamp = null) {
    let where = '';
    const params = [];
    if (timestamp) { params.push(timestamp); where = `WHERE timestamp <= $1`; }

    const result = await paginatedQuery('mv_liquidity_events', 'live_liquidity_events', LIQ_COLS, where, params, 'timestamp DESC', page, limit, null);
    result.data = result.data.map(r => ({ ...r, usd_value: fmtUsd(r.usd_value) }));
    // Preserve page/totalPages from paginatedQuery so the client can paginate
    // beyond the first chunk (previously only `{data,total}` was returned,
    // which capped the UI at ~1-2 pages).
    return { data: result.data, total: result.total, page: result.page, totalPages: result.totalPages };
}

async function getPoolActivity(base, target, limit = 10) {
    const res = await pool.query(
        `SELECT * FROM (SELECT ${LIQ_COLS} FROM sm.mv_liquidity_events WHERE pool_base = $1 AND pool_target = $2
         UNION ALL SELECT ${LIQ_COLS} FROM sm.live_liquidity_events WHERE pool_base = $1 AND pool_target = $2) AS _u
         ORDER BY timestamp DESC LIMIT $3`,
        [base, target, limit]
    );
    return res.rows.map(r => ({ ...r, time: formatTimestamp(r.timestamp), usd_value: fmtUsd(r.usd_value) }));
}

// ============================================================
// ANALYTICS FUNCTIONS
// ============================================================

// Price functions use sm.price_history (hourly medians) instead of
// per-swap derivation. This avoids denomination/repackaging issues
// where raw swap amounts can be 10^13+ after cumulative 10^38 repackaging.

async function getCandles(symbol, resolution = 60, limit = 1000) {
    const assetId = _symbolToAssetId[symbol];
    if (!assetId) return [];

    const intervalSec = resolution * 60;
    const now = Math.floor(Date.now() / 1000);
    const startBucket = now - (intervalSec * limit);

    const res = await pool.query(
        `SELECT hour_bucket, price_usd FROM sm.price_history
         WHERE asset_id = $1 AND hour_bucket >= $2
         ORDER BY hour_bucket ASC`,
        [assetId, startBucket]
    );
    if (res.rows.length === 0) return [];

    const buckets = {};
    for (const r of res.rows) {
        const price = r.price_usd;
        if (!price || price <= 0) continue;
        const candleBucket = Math.floor(r.hour_bucket / intervalSec) * intervalSec;
        if (!buckets[candleBucket]) {
            buckets[candleBucket] = { time: candleBucket, open: price, high: price, low: price, close: price };
        } else {
            const c = buckets[candleBucket];
            c.high = Math.max(c.high, price);
            c.low = Math.min(c.low, price);
            c.close = price;
        }
    }
    return Object.values(buckets).slice(-limit);
}

async function getPriceChange(symbol, currentPrice, timeframeMs) {
    const assetId = _symbolToAssetId[symbol];
    if (!assetId) return 0;

    const pastBucket = Math.floor((Date.now() - timeframeMs) / 1000 / 3600) * 3600;
    const res = await pool.query(
        `SELECT price_usd FROM sm.price_history
         WHERE asset_id = $1 AND hour_bucket <= $2
         ORDER BY hour_bucket DESC LIMIT 1`,
        [assetId, pastBucket]
    );
    if (!res.rows[0]) return 0;
    const oldPrice = res.rows[0].price_usd;
    if (!oldPrice || oldPrice <= 0) return 0;
    return ((currentPrice - oldPrice) / oldPrice) * 100;
}

async function getSparkline(symbol, timeframeMs) {
    const assetId = _symbolToAssetId[symbol];
    if (!assetId) return [];

    const startBucket = Math.floor((Date.now() - timeframeMs) / 1000 / 3600) * 3600;
    const res = await pool.query(
        `SELECT hour_bucket, price_usd FROM sm.price_history
         WHERE asset_id = $1 AND hour_bucket >= $2
         ORDER BY hour_bucket ASC`,
        [assetId, startBucket]
    );

    const prices = res.rows.filter(r => r.price_usd > 0);
    if (prices.length === 0) return [];
    if (prices.length <= 20) return prices.map(r => ({ value: r.price_usd, time: r.hour_bucket * 1000 }));
    const step = Math.floor(prices.length / 20);
    const sampled = prices.filter((_, i) => i % step === 0).slice(0, 20);
    // Always include the last point
    if (sampled.length > 0 && sampled[sampled.length - 1] !== prices[prices.length - 1]) {
        sampled.push(prices[prices.length - 1]);
    }
    return sampled.map(r => ({ value: r.price_usd, time: r.hour_bucket * 1000 }));
}

async function getTopAccumulators(symbol, timeframeMs) {
    const startTime = Date.now() - timeframeMs;
    const res = await pool.query(
        `SELECT wallet, SUM(out_usd) as total_bought_usd, SUM(CAST(out_amount AS double precision)) as total_bought_amount, COUNT(*) as swap_count, MAX(timestamp) as last_buy
         FROM (SELECT wallet, out_usd, out_amount, timestamp FROM sm.mv_swaps WHERE out_symbol = $1 AND timestamp > $2
               UNION ALL SELECT wallet, out_usd, out_amount, timestamp FROM sm.live_swaps WHERE out_symbol = $1 AND timestamp > $2) AS _u
         GROUP BY wallet ORDER BY total_bought_usd DESC LIMIT 10`,
        [symbol, startTime]
    );
    return res.rows;
}

async function getNetworkStats(timeframeMs) {
    const startTime = Date.now() - timeframeMs;
    const res = await pool.query(
        `SELECT
            COALESCE(SUM(in_usd), 0) as volume,
            COUNT(DISTINCT wallet) as users,
            COUNT(*) as tx_count
         FROM (SELECT in_usd, wallet FROM sm.mv_swaps WHERE timestamp > $1
               UNION ALL SELECT in_usd, wallet FROM sm.live_swaps WHERE timestamp > $1) AS _u`,
        [startTime]
    );
    const r = res.rows[0];
    return { volume: parseFloat(r.volume) || 0, users: parseInt(r.users) || 0, txCount: parseInt(r.tx_count) || 0 };
}

async function getMarketTrends(timeframeMs) {
    const startTime = Date.now() - timeframeMs;
    const res = await pool.query(
        `SELECT symbol, SUM(vol) as volume FROM (
            SELECT in_symbol as symbol, SUM(in_usd) as vol FROM sm.mv_swaps WHERE timestamp > $1 GROUP BY in_symbol
            UNION ALL SELECT out_symbol, SUM(out_usd) FROM sm.mv_swaps WHERE timestamp > $1 GROUP BY out_symbol
            UNION ALL SELECT in_symbol, SUM(in_usd) FROM sm.live_swaps WHERE timestamp > $1 GROUP BY in_symbol
            UNION ALL SELECT out_symbol, SUM(out_usd) FROM sm.live_swaps WHERE timestamp > $1 GROUP BY out_symbol
         ) AS _u GROUP BY symbol ORDER BY volume DESC LIMIT 5`,
        [startTime]
    );
    return res.rows;
}

let _totalStatsCache = { data: null, ts: 0 };

async function getTotalStats() {
    // Cache for 30s — approximate counts via pg_class
    if (_totalStatsCache.data && Date.now() - _totalStatsCache.ts < 30000) return _totalStatsCache.data;
    const res = await pool.query(
        `SELECT
            (SELECT reltuples::bigint FROM pg_class WHERE relname = 'mv_swaps') as swaps,
            (SELECT reltuples::bigint FROM pg_class WHERE relname = 'mv_transfers') as transfers`
    );
    const r = res.rows[0];
    const data = {
        swaps: parseInt(r.swaps) || 0,
        transfers: parseInt(r.transfers) || 0,
    };
    _totalStatsCache = { data, ts: Date.now() };
    return data;
}

let _filteredStatsCache = { data: null, ts: 0, key: '' };

async function getFilteredStats(startTime) {
    // Cache for 30s per startTime
    const cacheKey = String(startTime);
    if (_filteredStatsCache.data && _filteredStatsCache.key === cacheKey && Date.now() - _filteredStatsCache.ts < 30000) {
        return _filteredStatsCache.data;
    }
    const res = await pool.query(
        `SELECT
            (SELECT COUNT(*) FROM sm.mv_swaps WHERE timestamp >= $1) + (SELECT COUNT(*) FROM sm.live_swaps WHERE timestamp >= $1) as swaps,
            (SELECT COUNT(*) FROM sm.mv_transfers WHERE timestamp >= $1) + (SELECT COUNT(*) FROM sm.live_transfers WHERE timestamp >= $1) as transfers,
            (SELECT COUNT(*) FROM sm.mv_bridges WHERE timestamp >= $1) + (SELECT COUNT(*) FROM sm.live_bridges WHERE timestamp >= $1) as bridges`,
        [startTime]
    );
    const r = res.rows[0];
    const data = { swaps: parseInt(r.swaps) || 0, transfers: parseInt(r.transfers) || 0, bridges: parseInt(r.bridges) || 0 };
    _filteredStatsCache = { data, ts: Date.now(), key: cacheKey };
    return data;
}

// Fee stats: MV amounts are already in XOR (network_fee/1e18), no denom normalization needed.
// USD values are correct (amount * price_at_hour) for most records.
// Fee stats: MV amounts are already in XOR (network_fee/1e18), no denom normalization needed.
// Filters: (1) Cap per-row USD at $10K and amount at 100 XOR to exclude denomination-boundary
// outlier records. Subsquid network_fee values span 6 repackaging eras — some records have
// amounts in old denomination (huge) paired with tiny prices. The ratio is sometimes correct
// (giving reasonable USD) but the individual amounts/prices are misleading.
// (2) Only include rows where usd_value > 0 (no price data = unreliable amount).
const FEE_USD_CAP = 10000;
const FEE_AMOUNT_CAP = 100; // Max reasonable fee per transaction in XOR

async function getFeeStats(startTime, _currentDenomFactor) {
    const res = await pool.query(
        `SELECT type,
            SUM(CASE WHEN usd_value > 0 AND usd_value <= $2 AND amount <= $3 THEN amount ELSE 0 END) as total_xor,
            SUM(CASE WHEN usd_value > 0 AND usd_value <= $2 AND amount <= $3 THEN usd_value ELSE 0 END) as total_usd
         FROM (SELECT type, amount, usd_value FROM sm.mv_fees WHERE timestamp >= $1
               UNION ALL SELECT type, amount, usd_value FROM sm.live_fees WHERE timestamp >= $1) AS _u
         GROUP BY type`,
        [startTime, FEE_USD_CAP, FEE_AMOUNT_CAP]
    );
    return res.rows;
}

async function getFeeStatsMainOnly(startTime, _currentDenomFactor) {
    const res = await pool.query(
        `SELECT type,
            SUM(amount) as total_xor,
            SUM(usd_value) as total_usd
         FROM sm.live_fees WHERE timestamp >= $1
         GROUP BY type`,
        [startTime]
    );
    return res.rows;
}

async function getFeeTrend(startTime, interval) {
    let fmt;
    if (interval === 'hour') fmt = 'YYYY-MM-DD HH24:00:00';
    else fmt = 'YYYY-MM-DD';

    const res = await pool.query(
        `SELECT TO_CHAR(TO_TIMESTAMP(timestamp / 1000.0), $2) as bucket,
                SUM(CASE WHEN usd_value > 0 AND usd_value <= $3 AND amount <= $4 THEN usd_value ELSE 0 END) as total_usd
         FROM (SELECT timestamp, usd_value, amount FROM sm.mv_fees WHERE timestamp >= $1
               UNION ALL SELECT timestamp, usd_value, amount FROM sm.live_fees WHERE timestamp >= $1) AS _u
         GROUP BY bucket ORDER BY bucket ASC`,
        [startTime, fmt, FEE_USD_CAP, FEE_AMOUNT_CAP]
    );
    return res.rows;
}

// ============================================================
// VOLUME & TREND FUNCTIONS
// ============================================================

async function getLpVolume(msWindow) {
    const startTime = Date.now() - msWindow;
    const res = await pool.query(
        `SELECT COALESCE(SUM(CASE WHEN type = 'Deposit' THEN usd_value ELSE -usd_value END), 0) as total
         FROM (SELECT usd_value, type FROM sm.mv_liquidity_events WHERE timestamp >= $1
               UNION ALL SELECT usd_value, type FROM sm.live_liquidity_events WHERE timestamp >= $1) AS _u`,
        [startTime]
    );
    return parseFloat(res.rows[0].total) || 0;
}

async function getTransferVolume(msWindow) {
    const startTime = Date.now() - msWindow;
    const res = await pool.query(
        `SELECT COALESCE(SUM(usd_value), 0) as total
         FROM (SELECT usd_value FROM sm.mv_transfers WHERE timestamp >= $1
               UNION ALL SELECT usd_value FROM sm.live_transfers WHERE timestamp >= $1) AS _u`,
        [startTime]
    );
    return parseFloat(res.rows[0].total) || 0;
}

async function getSwapVolumeUsd(startTime) {
    const res = await pool.query(
        `SELECT COALESCE(SUM(in_usd), 0) as total
         FROM (SELECT in_usd FROM sm.mv_swaps WHERE timestamp >= $1
               UNION ALL SELECT in_usd FROM sm.live_swaps WHERE timestamp >= $1) AS _u`,
        [startTime]
    );
    return parseFloat(res.rows[0].total) || 0;
}

async function getNetworkTrend(startTime, interval) {
    let fmt;
    if (interval === 'hour') fmt = 'YYYY-MM-DD HH24:00:00';
    else fmt = 'YYYY-MM-DD';

    const [swaps, transfers, lp, accounts] = await Promise.all([
        pool.query(
            `SELECT TO_CHAR(TO_TIMESTAMP(timestamp / 1000.0), $2) as bucket, SUM(in_usd) as val
             FROM (SELECT timestamp, in_usd FROM sm.mv_swaps WHERE timestamp >= $1
                   UNION ALL SELECT timestamp, in_usd FROM sm.live_swaps WHERE timestamp >= $1) AS _u
             GROUP BY bucket ORDER BY bucket`,
            [startTime, fmt]),
        pool.query(
            `SELECT TO_CHAR(TO_TIMESTAMP(timestamp / 1000.0), $2) as bucket, SUM(usd_value) as val
             FROM (SELECT timestamp, usd_value FROM sm.mv_transfers WHERE timestamp >= $1
                   UNION ALL SELECT timestamp, usd_value FROM sm.live_transfers WHERE timestamp >= $1) AS _u
             GROUP BY bucket ORDER BY bucket`,
            [startTime, fmt]),
        pool.query(
            `SELECT TO_CHAR(TO_TIMESTAMP(timestamp / 1000.0), $2) as bucket, SUM(CASE WHEN type = 'Deposit' THEN usd_value ELSE -usd_value END) as val
             FROM (SELECT timestamp, usd_value, type FROM sm.mv_liquidity_events WHERE timestamp >= $1
                   UNION ALL SELECT timestamp, usd_value, type FROM sm.live_liquidity_events WHERE timestamp >= $1) AS _u
             GROUP BY bucket ORDER BY bucket`,
            [startTime, fmt]),
        pool.query(
            `SELECT TO_CHAR(TO_TIMESTAMP(timestamp / 1000.0), $2) as bucket, COUNT(DISTINCT wallet) as val
             FROM (SELECT timestamp, wallet FROM sm.mv_swaps WHERE timestamp >= $1
                   UNION ALL SELECT timestamp, wallet FROM sm.live_swaps WHERE timestamp >= $1) AS _u
             GROUP BY bucket ORDER BY bucket`,
            [startTime, fmt]),
    ]);

    return {
        swaps: swaps.rows,
        transfers: transfers.rows,
        lp: lp.rows,
        accounts: accounts.rows,
    };
}

async function getTopTokens(startTime) {
    const res = await pool.query(
        `SELECT symbol, SUM(vol) as volume FROM (
            SELECT in_symbol as symbol, SUM(in_usd) as vol FROM sm.mv_swaps WHERE timestamp >= $1 GROUP BY in_symbol
            UNION ALL SELECT out_symbol, SUM(out_usd) FROM sm.mv_swaps WHERE timestamp >= $1 GROUP BY out_symbol
            UNION ALL SELECT in_symbol, SUM(in_usd) FROM sm.live_swaps WHERE timestamp >= $1 GROUP BY in_symbol
            UNION ALL SELECT out_symbol, SUM(out_usd) FROM sm.live_swaps WHERE timestamp >= $1 GROUP BY out_symbol
         ) AS _u GROUP BY symbol ORDER BY volume DESC LIMIT 5`,
        [startTime]
    );
    // Resolve logos from cache
    return res.rows.map(r => ({ ...r, logo: logoFor(r.symbol) }));
}

async function getStablecoinStats(startTime) {
    const stables = ['KUSD', 'XSTUSD', 'TBCD'];
    const result = {};
    for (const sym of stables) {
        const res = await pool.query(
            `SELECT
                COALESCE((SELECT SUM(CASE WHEN in_symbol = $1 THEN in_usd ELSE 0 END + CASE WHEN out_symbol = $1 THEN out_usd ELSE 0 END)
                    FROM (SELECT in_symbol, in_usd, out_symbol, out_usd FROM sm.mv_swaps WHERE (in_symbol = $1 OR out_symbol = $1) AND timestamp >= $2
                          UNION ALL SELECT in_symbol, in_usd, out_symbol, out_usd FROM sm.live_swaps WHERE (in_symbol = $1 OR out_symbol = $1) AND timestamp >= $2) AS s), 0) as swap_vol,
                COALESCE((SELECT SUM(usd_value)
                    FROM (SELECT usd_value FROM sm.mv_transfers WHERE symbol = $1 AND timestamp >= $2
                          UNION ALL SELECT usd_value FROM sm.live_transfers WHERE symbol = $1 AND timestamp >= $2) AS t), 0) as transfer_vol`,
            [sym, startTime]
        );
        const r = res.rows[0];
        result[sym] = { symbol: sym, swapVolume: parseFloat(r.swap_vol) || 0, transferVolume: parseFloat(r.transfer_vol) || 0 };
    }
    return result;
}

// ============================================================
// COMPLEX FUNCTIONS
// ============================================================

async function getBurnStats(symbol, startTime) {
    // Fetch only first and last rows instead of all rows
    const [firstRes, lastRes] = await Promise.all([
        pool.query(
            'SELECT timestamp, total_supply FROM sm.supply_snapshots WHERE symbol = $1 AND timestamp >= $2 ORDER BY timestamp ASC LIMIT 1',
            [symbol, startTime]
        ),
        pool.query(
            'SELECT timestamp, total_supply FROM sm.supply_snapshots WHERE symbol = $1 AND timestamp >= $2 ORDER BY timestamp DESC LIMIT 1',
            [symbol, startTime]
        ),
    ]);
    if (!firstRes.rows[0] || !lastRes.rows[0] || firstRes.rows[0].timestamp === lastRes.rows[0].timestamp) {
        return { totalBurned: 0, startSupply: 0, endSupply: 0 };
    }
    const first = firstRes.rows[0];
    const last = lastRes.rows[0];
    return {
        totalBurned: first.total_supply - last.total_supply,
        startSupply: first.total_supply,
        endSupply: last.total_supply,
        startTime: first.timestamp,
        endTime: last.timestamp,
    };
}

async function getBurnStatsFromChain(assetId, startTimeSec) {
    // Query burn/mint totals + supply delta from subsquid asset_snapshot
    const res = await pool.query(`
        SELECT (COALESCE(SUM(burn), 0) / 1e18)::float8 as total_burn,
               (COALESCE(SUM(mint), 0) / 1e18)::float8 as total_mint
        FROM asset_snapshot
        WHERE asset_id = $1 AND type = 'DAY' AND timestamp >= $2
    `, [assetId, startTimeSec]);
    const burn = parseFloat(res.rows[0].total_burn) || 0;
    const mint = parseFloat(res.rows[0].total_mint) || 0;

    // Also compute supply delta (first - last) in the period.
    // Positive = supply decreased (net deflation), negative = supply increased.
    const rangeRes = await pool.query(`
        SELECT
            (SELECT (supply::numeric / 1e18)::float8 FROM asset_snapshot WHERE asset_id = $1 AND type = 'DAY' AND timestamp >= $2 ORDER BY timestamp ASC LIMIT 1) as first_supply,
            (SELECT (supply::numeric / 1e18)::float8 FROM asset_snapshot WHERE asset_id = $1 AND type = 'DAY' ORDER BY timestamp DESC LIMIT 1) as last_supply
    `, [assetId, startTimeSec]);
    const firstSupply = rangeRes.rows[0]?.first_supply || 0;
    const lastSupply = rangeRes.rows[0]?.last_supply || 0;
    const supplyDelta = firstSupply - lastSupply; // positive = deflation

    return { totalBurned: burn - mint, totalBurn: burn, totalMint: mint, supplyDelta, firstSupply, lastSupply };
}

async function lookupExtrinsicUsdValue(extrinsicId, tokenPrices) {
    // extrinsicId = "block-index" (e.g. "25000000-3")
    // Live tables use this format; MVs use subsquid he.id (e.g. "0025000000-000002-3abc")
    // Resolve the subsquid hash so we can search both
    const [blockStr, indexStr] = extrinsicId.split('-');
    const block = parseInt(blockStr);
    const idx = parseInt(indexStr);

    let heId = null;
    if (!isNaN(block) && !isNaN(idx)) {
        const hashRes = await pool.query(
            `SELECT hash FROM (
                SELECT hash FROM sm.mv_extrinsics WHERE block = $1 AND extrinsic_index = $2
                UNION ALL SELECT hash FROM sm.live_extrinsics WHERE block = $1 AND extrinsic_index = $2
            ) AS _u LIMIT 1`,
            [block, idx]
        );
        if (hashRes.rows[0]) heId = hashRes.rows[0].hash;
    }

    // Build list of IDs to search (live format + subsquid hash)
    const ids = [extrinsicId];
    if (heId && heId !== extrinsicId) ids.push(heId);

    // Search transfers
    let res = await pool.query(
        `SELECT usd_value, amount, symbol FROM (
            SELECT usd_value, amount, symbol, extrinsic_id FROM sm.mv_transfers WHERE extrinsic_id = ANY($1)
            UNION ALL SELECT usd_value, amount, symbol, extrinsic_id FROM sm.live_transfers WHERE extrinsic_id = ANY($1)
        ) AS _u LIMIT 1`, [ids]
    );
    if (res.rows[0]) {
        const usd = fmtUsd(res.rows[0].usd_value);
        if (usd > 0) return { usd_value: usd, source: 'transfer' };
        // Fallback: compute from current price if price_history was empty
        if (tokenPrices && res.rows[0].symbol && res.rows[0].amount) {
            const fallback = computeFallbackUsd(res.rows[0].amount, res.rows[0].symbol, tokenPrices);
            if (fallback > 0) return { usd_value: fmtUsd(fallback), source: 'transfer' };
        }
    }

    // Search swaps
    res = await pool.query(
        `SELECT in_usd, out_usd, in_amount, out_amount, in_symbol, out_symbol FROM (
            SELECT in_usd, out_usd, in_amount, out_amount, in_symbol, out_symbol, extrinsic_id FROM sm.mv_swaps WHERE extrinsic_id = ANY($1)
            UNION ALL SELECT in_usd, out_usd, in_amount, out_amount, in_symbol, out_symbol, extrinsic_id FROM sm.live_swaps WHERE extrinsic_id = ANY($1)
        ) AS _u LIMIT 1`, [ids]
    );
    if (res.rows[0]) {
        const usd = fmtUsd(res.rows[0].in_usd || res.rows[0].out_usd);
        if (usd > 0) return { usd_value: usd, source: 'swap' };
        if (tokenPrices) {
            const fallback = computeFallbackUsd(res.rows[0].in_amount, res.rows[0].in_symbol, tokenPrices)
                          || computeFallbackUsd(res.rows[0].out_amount, res.rows[0].out_symbol, tokenPrices);
            if (fallback > 0) return { usd_value: fmtUsd(fallback), source: 'swap' };
        }
    }

    // Search bridges
    res = await pool.query(
        `SELECT usd_value, amount, symbol FROM (
            SELECT usd_value, amount, symbol, extrinsic_id FROM sm.mv_bridges WHERE extrinsic_id = ANY($1)
            UNION ALL SELECT usd_value, amount, symbol, extrinsic_id FROM sm.live_bridges WHERE extrinsic_id = ANY($1)
        ) AS _u LIMIT 1`, [ids]
    );
    if (res.rows[0]) {
        const usd = fmtUsd(res.rows[0].usd_value);
        if (usd > 0) return { usd_value: usd, source: 'bridge' };
        if (tokenPrices && res.rows[0].symbol && res.rows[0].amount) {
            const fallback = computeFallbackUsd(res.rows[0].amount, res.rows[0].symbol, tokenPrices);
            if (fallback > 0) return { usd_value: fmtUsd(fallback), source: 'bridge' };
        }
    }

    // Search liquidity
    res = await pool.query(
        `SELECT usd_value FROM (
            SELECT usd_value, extrinsic_id FROM sm.mv_liquidity_events WHERE extrinsic_id = ANY($1)
            UNION ALL SELECT usd_value, extrinsic_id FROM sm.live_liquidity_events WHERE extrinsic_id = ANY($1)
        ) AS _u LIMIT 1`, [ids]
    );
    if (res.rows[0]) {
        const usd = fmtUsd(res.rows[0].usd_value);
        if (usd > 0) return { usd_value: usd, source: 'liquidity' };
    }

    return null;
}

// Fallback: approximate USD from amount * current price when price_history is empty
function computeFallbackUsd(amount, symbol, tokenPrices) {
    if (!amount || !symbol || !tokenPrices) return 0;
    const price = tokenPrices[symbol] || tokenPrices[symbol.toUpperCase()] || 0;
    if (price <= 0) return 0;
    return parseFloat(amount) * price;
}

async function globalSearch(query) {
    if (!query) return { type: null };
    query = query.trim();

    // Extrinsic ID: "block-index"
    if (/^\d+-\d+$/.test(query)) {
        const [blockStr, indexStr] = query.split('-');
        const detail = await getExtrinsicDetail(parseInt(blockStr), parseInt(indexStr));
        if (detail) return { type: 'extrinsic', data: detail };
        return { type: 'hash_not_found' };
    }

    // Block number
    if (/^\d+$/.test(query)) return { type: 'block', data: { block: parseInt(query) } };

    // TX hash
    if (/^0x[a-fA-F0-9]{64}$/.test(query)) {
        const res = await pool.query(
            `SELECT ${EXT_COLS} FROM (SELECT ${EXT_COLS} FROM sm.mv_extrinsics WHERE hash = $1
             UNION ALL SELECT ${EXT_COLS} FROM sm.live_extrinsics WHERE hash = $1) AS _u LIMIT 1`,
            [query]
        );
        if (res.rows[0]) {
            const row = res.rows[0];
            // Resolve args_json for search results
            const heRes = await pool.query("SELECT COALESCE(data::text, '{}'::text) AS args_json FROM history_element WHERE id = $1 LIMIT 1", [query]);
            row.args_json = heRes.rows[0]?.args_json || '{}';
            row.events_json = null;
            return { type: 'extrinsic', data: mapExtrinsics([row])[0] };
        }
        return { type: 'hash_not_found' };
    }

    // Wallet address
    if (/^cn[a-zA-Z0-9]{46,}$/.test(query)) return { type: 'wallet', data: { address: query } };

    // Token symbol/name search via asset_registry cache
    const qUpper = query.toUpperCase();
    const qLower = query.toLowerCase();
    const matches = [];
    for (const [symbol, assetId] of Object.entries(_symbolToAssetId)) {
        if (symbol.toUpperCase().includes(qUpper)) {
            matches.push({ symbol, assetId });
        }
    }
    if (matches.length === 1) return { type: 'token', data: matches[0] };
    if (matches.length > 1) return { type: 'tokens', data: matches.slice(0, 10) };

    return { type: null };
}

async function getWalletInfo(address) {
    const [extStats, modules, governance, swapStats, topTokens, uniqueTokens, topContacts, transferStats, lpSummary, bridgeSummary] = await Promise.all([
        // Q1: Extrinsic stats
        pool.query(
            `SELECT MIN(timestamp) as first_tx, MAX(timestamp) as last_tx, COUNT(*) as tx_count,
                    SUM(success) as success_count, COUNT(DISTINCT CAST(timestamp / 86400000 AS INTEGER)) as days_active
             FROM (SELECT timestamp, success FROM sm.mv_extrinsics WHERE signer = $1
                   UNION ALL SELECT timestamp, success FROM sm.live_extrinsics WHERE signer = $1) AS _u`,
            [address]),
        // Q2: Modules
        pool.query(
            `SELECT section, COUNT(*) as count FROM (SELECT section FROM sm.mv_extrinsics WHERE signer = $1
                    UNION ALL SELECT section FROM sm.live_extrinsics WHERE signer = $1) AS _u
             GROUP BY section ORDER BY count DESC LIMIT 10`,
            [address]),
        // Q3: Governance
        pool.query(
            `SELECT COUNT(*) as cnt FROM (SELECT section FROM sm.mv_extrinsics WHERE signer = $1 AND section IN ('democracy','council','electionsPhragmen','technicalCommittee')
                    UNION ALL SELECT section FROM sm.live_extrinsics WHERE signer = $1 AND section IN ('democracy','council','electionsPhragmen','technicalCommittee')) AS _u`,
            [address]),
        // Q4: Swap stats
        pool.query(
            `SELECT COUNT(*) as swap_count, COALESCE(AVG(in_usd), 0) as avg_usd, COALESCE(MAX(in_usd), 0) as max_usd, COALESCE(SUM(in_usd), 0) as total_vol
             FROM (SELECT in_usd FROM sm.mv_swaps WHERE wallet = $1 UNION ALL SELECT in_usd FROM sm.live_swaps WHERE wallet = $1) AS _u`,
            [address]),
        // Q5: Top tokens
        pool.query(
            `SELECT symbol, SUM(total_usd) as total_usd, COUNT(*) as trades FROM (
                SELECT in_symbol as symbol, in_usd as total_usd FROM sm.mv_swaps WHERE wallet = $1
                UNION ALL SELECT out_symbol, out_usd FROM sm.mv_swaps WHERE wallet = $1
                UNION ALL SELECT in_symbol, in_usd FROM sm.live_swaps WHERE wallet = $1
                UNION ALL SELECT out_symbol, out_usd FROM sm.live_swaps WHERE wallet = $1
             ) AS _u GROUP BY symbol ORDER BY total_usd DESC LIMIT 10`,
            [address]),
        // Q6: Unique tokens
        pool.query(
            `SELECT COUNT(DISTINCT symbol) as cnt FROM (
                SELECT in_symbol as symbol FROM sm.mv_swaps WHERE wallet = $1
                UNION SELECT out_symbol FROM sm.mv_swaps WHERE wallet = $1
                UNION SELECT in_symbol FROM sm.live_swaps WHERE wallet = $1
                UNION SELECT out_symbol FROM sm.live_swaps WHERE wallet = $1
             ) AS _u`,
            [address]),
        // Q7: Top contacts
        pool.query(
            `SELECT counterparty, COUNT(*) as tx_count, SUM(usd_value) as total_usd FROM (
                SELECT to_addr as counterparty, usd_value FROM sm.mv_transfers WHERE from_addr = $1
                UNION ALL SELECT from_addr, usd_value FROM sm.mv_transfers WHERE to_addr = $1
                UNION ALL SELECT to_addr, usd_value FROM sm.live_transfers WHERE from_addr = $1
                UNION ALL SELECT from_addr, usd_value FROM sm.live_transfers WHERE to_addr = $1
             ) AS _u GROUP BY counterparty ORDER BY total_usd DESC LIMIT 10`,
            [address]),
        // Q8: Transfer stats (in/out)
        pool.query(
            `SELECT
                COALESCE((SELECT COUNT(*) FROM sm.mv_transfers WHERE from_addr = $1), 0) + COALESCE((SELECT COUNT(*) FROM sm.live_transfers WHERE from_addr = $1), 0) as out_count,
                COALESCE((SELECT SUM(usd_value) FROM sm.mv_transfers WHERE from_addr = $1), 0) + COALESCE((SELECT SUM(usd_value) FROM sm.live_transfers WHERE from_addr = $1), 0) as out_usd,
                COALESCE((SELECT COUNT(*) FROM sm.mv_transfers WHERE to_addr = $1), 0) + COALESCE((SELECT COUNT(*) FROM sm.live_transfers WHERE to_addr = $1), 0) as in_count,
                COALESCE((SELECT SUM(usd_value) FROM sm.mv_transfers WHERE to_addr = $1), 0) + COALESCE((SELECT SUM(usd_value) FROM sm.live_transfers WHERE to_addr = $1), 0) as in_usd,
                (SELECT MIN(timestamp) FROM (SELECT timestamp FROM sm.mv_transfers WHERE from_addr = $1 OR to_addr = $1 UNION ALL SELECT timestamp FROM sm.live_transfers WHERE from_addr = $1 OR to_addr = $1) AS _t) as first_transfer_ts,
                (SELECT MAX(timestamp) FROM (SELECT timestamp FROM sm.mv_transfers WHERE from_addr = $1 OR to_addr = $1 UNION ALL SELECT timestamp FROM sm.live_transfers WHERE from_addr = $1 OR to_addr = $1) AS _t) as last_transfer_ts`,
            [address]),
        // Q9: LP summary
        pool.query(
            `SELECT
                COALESCE(SUM(CASE WHEN type = 'Deposit' THEN 1 ELSE 0 END), 0) as deposits,
                COALESCE(SUM(CASE WHEN type = 'Withdraw' THEN 1 ELSE 0 END), 0) as withdrawals,
                COALESCE(SUM(CASE WHEN type = 'Deposit' THEN usd_value ELSE 0 END), 0) as deposited_usd,
                COALESCE(SUM(CASE WHEN type = 'Withdraw' THEN usd_value ELSE 0 END), 0) as withdrawn_usd,
                COUNT(DISTINCT pool_base || '-' || pool_target) as unique_pools
             FROM (SELECT type, usd_value, pool_base, pool_target FROM sm.mv_liquidity_events WHERE wallet = $1
                   UNION ALL SELECT type, usd_value, pool_base, pool_target FROM sm.live_liquidity_events WHERE wallet = $1) AS _u`,
            [address]),
        // Q10: Bridge summary
        pool.query(
            `SELECT
                COALESCE(SUM(CASE WHEN direction = 'Incoming' THEN 1 ELSE 0 END), 0) as incoming_count,
                COALESCE(SUM(CASE WHEN direction = 'Outgoing' THEN 1 ELSE 0 END), 0) as outgoing_count,
                COALESCE(SUM(CASE WHEN direction = 'Incoming' THEN usd_value ELSE 0 END), 0) as incoming_usd,
                COALESCE(SUM(CASE WHEN direction = 'Outgoing' THEN usd_value ELSE 0 END), 0) as outgoing_usd,
                COUNT(DISTINCT network) as unique_networks
             FROM (SELECT direction, usd_value, network FROM sm.mv_bridges WHERE sender = $1 OR recipient = $1
                   UNION ALL SELECT direction, usd_value, network FROM sm.live_bridges WHERE sender = $1 OR recipient = $1) AS _u`,
            [address]),
    ]);

    const e = extStats.rows[0] || {};
    const s = swapStats.rows[0] || {};
    const t = transferStats.rows[0] || {};
    const l = lpSummary.rows[0] || {};
    const b = bridgeSummary.rows[0] || {};

    return {
        firstTx: e.first_tx || t.first_transfer_ts || null,
        lastTx:  e.last_tx  || t.last_transfer_ts  || null,
        txCount: parseInt(e.tx_count) || 0,
        successCount: parseInt(e.success_count) || 0, daysActive: parseInt(e.days_active) || 0,
        modules: modules.rows,
        governanceTx: parseInt(governance.rows[0]?.cnt) || 0,
        swapCount: parseInt(s.swap_count) || 0, swapAvgUsd: parseFloat(s.avg_usd) || 0,
        swapMaxUsd: parseFloat(s.max_usd) || 0, swapTotalVolume: parseFloat(s.total_vol) || 0,
        topTokens: topTokens.rows,
        uniqueTokens: parseInt(uniqueTokens.rows[0]?.cnt) || 0,
        topContacts: topContacts.rows,
        transfersOut: { count: parseInt(t.out_count) || 0, usd: parseFloat(t.out_usd) || 0 },
        transfersIn: { count: parseInt(t.in_count) || 0, usd: parseFloat(t.in_usd) || 0 },
        lpDeposits: parseInt(l.deposits) || 0, lpWithdrawals: parseInt(l.withdrawals) || 0,
        lpDepositedUsd: parseFloat(l.deposited_usd) || 0, lpWithdrawnUsd: parseFloat(l.withdrawn_usd) || 0,
        lpUniquePools: parseInt(l.unique_pools) || 0,
        bridgeIncoming: { count: parseInt(b.incoming_count) || 0, usd: parseFloat(b.incoming_usd) || 0 },
        bridgeOutgoing: { count: parseInt(b.outgoing_count) || 0, usd: parseFloat(b.outgoing_usd) || 0 },
        bridgeUniqueNetworks: parseInt(b.unique_networks) || 0,
    };
}

async function getExportData({ wallets, types, startTs, endTs, limit = 50000 }) {
    const result = {};

    for (const type of types) {
        let cols, mvName, liveName, walletCols;
        switch (type) {
            case 'swaps':
                cols = 'timestamp, block, wallet, in_symbol, in_amount, in_usd, out_symbol, out_amount, out_usd, hash, extrinsic_id';
                mvName = 'mv_swaps'; liveName = 'live_swaps'; walletCols = ['wallet'];
                break;
            case 'transfers':
                cols = 'timestamp, block, from_addr, to_addr, amount, symbol, usd_value, hash, extrinsic_id';
                mvName = 'mv_transfers'; liveName = 'live_transfers'; walletCols = ['from_addr', 'to_addr'];
                break;
            case 'bridges':
                cols = 'timestamp, block, network, direction, sender, recipient, symbol, amount, usd_value, hash, extrinsic_id';
                mvName = 'mv_bridges'; liveName = 'live_bridges'; walletCols = ['sender', 'recipient'];
                break;
            case 'liquidity':
                cols = 'timestamp, block, wallet, pool_base, pool_target, base_amount, target_amount, usd_value, type, hash, extrinsic_id';
                mvName = 'mv_liquidity_events'; liveName = 'live_liquidity_events'; walletCols = ['wallet'];
                break;
            case 'orderbook':
                cols = 'timestamp, block, event_type, wallet, base_asset, quote_asset, side, price, amount, usd_value, hash, extrinsic_id';
                mvName = 'mv_order_book_events'; liveName = 'live_order_book_events'; walletCols = ['wallet'];
                break;
            case 'extrinsics':
                cols = 'timestamp, block, extrinsic_index, hash, section, method, signer, success';
                mvName = 'mv_extrinsics'; liveName = 'live_extrinsics'; walletCols = ['signer'];
                break;
            default: continue;
        }

        const walletFilter = walletCols.length === 1
            ? `${walletCols[0]} = ANY($1::text[])`
            : walletCols.map(c => `${c} = ANY($1::text[])`).join(' OR ');

        const sql = `SELECT ${cols} FROM (
            SELECT ${cols} FROM sm.${mvName} WHERE (${walletFilter}) AND timestamp >= $2 AND timestamp <= $3
            UNION ALL
            SELECT ${cols} FROM sm.${liveName} WHERE (${walletFilter}) AND timestamp >= $2 AND timestamp <= $3
        ) AS _u ORDER BY timestamp DESC LIMIT $4`;

        const res = await pool.query(sql, [wallets, startTs, endTs, limit]);
        result[type] = res.rows;
    }

    return result;
}

// ============================================================
// LIVE PRICE HISTORY UPDATER
// Keeps sm.price_history current using DEX prices from index.js
// Called periodically with the current tokenPrices map
// ============================================================

async function updatePriceHistory(tokenPrices) {
    if (!tokenPrices || Object.keys(tokenPrices).length === 0) return;
    const hourBucket = Math.floor(Date.now() / 1000 / 3600) * 3600;
    let inserted = 0;

    for (const [symbol, price] of Object.entries(tokenPrices)) {
        if (!price || price <= 0) continue;
        const assetId = _symbolToAssetId[symbol];
        if (!assetId) continue;

        try {
            await pool.query(
                `INSERT INTO sm.price_history (asset_id, hour_bucket, price_usd, sample_count)
                 VALUES ($1, $2, $3, 1)
                 ON CONFLICT (asset_id, hour_bucket) DO UPDATE SET
                    price_usd = (sm.price_history.price_usd * sm.price_history.sample_count + $3) / (sm.price_history.sample_count + 1),
                    sample_count = sm.price_history.sample_count + 1`,
                [assetId, hourBucket, price]
            );
            inserted++;
        } catch (err) {
            // Silently skip individual insert failures
        }
    }
    if (inserted > 0) console.log(`[db_pg] Price history updated: ${inserted} assets for bucket ${new Date(hourBucket * 1000).toISOString()}`);
}

// ============================================================
// EXPORTS
// ============================================================

// ============================================================
// POLKAMARKT (prediction market pallet — available in runtime ≥ 4.8.x)
// ============================================================
//
// Pallet surface (extracted from pallets/polkamarkt/src/lib.rs on github):
//   Storage: Markets, Conditions, MarketPools, MarketVolume, MarketPositions,
//            MarketResolution, OpengovConditions, MarketCreatorFees, …
//   Events:  MarketCreated, TradeExecuted, MarketLocked, MarketResolved,
//            MarketCancelled, MarketClaimed, CreatorFeesClaimed, XorBuybackSwept
//   Calls:   create_market, buy, sell, resolve_market, cancel_market, claim_market
//
// Schema here is a local mirror populated by the live indexer in index.js
// (backend consumes `api.events.polkamarkt.*`). Endpoints query these
// tables; if empty, UI shows "coming soon" via the feature-detection
// endpoint /polkamarkt/state.

async function initPolkamarktSchema() {
    const client = await pool.connect();
    try {
        // Markets replica — kept in sync by MarketCreated / Locked / Resolved /
        // Cancelled events. Question / oracle / resolution_source are cached
        // here from Conditions[condId] the first time we see the market so
        // endpoints don't need a second RPC round-trip.
        await client.query(`
            CREATE TABLE IF NOT EXISTS sm.polkamarkt_markets (
                market_id           BIGINT PRIMARY KEY,
                condition_id        BIGINT NOT NULL,
                creator             TEXT NOT NULL,
                close_block         INTEGER NOT NULL,
                collateral_asset    TEXT NOT NULL,
                seed_liquidity      NUMERIC(60) NOT NULL DEFAULT 0,
                status              TEXT NOT NULL,               -- 'Open' | 'Locked' | 'Resolved' | 'Cancelled'
                resolution          TEXT,                          -- 'Yes' | 'No' | NULL
                question            TEXT,
                oracle              TEXT,
                resolution_source   TEXT,
                opengov_network     TEXT,
                opengov_parachain   INTEGER,
                opengov_track       INTEGER,
                opengov_referendum  INTEGER,
                created_at_block    INTEGER NOT NULL,
                created_at_ts       BIGINT  NOT NULL,
                resolved_at_block   INTEGER,
                resolved_at_ts      BIGINT
            )
        `);
        await client.query(`CREATE INDEX IF NOT EXISTS idx_pm_markets_status ON sm.polkamarkt_markets(status)`);
        await client.query(`CREATE INDEX IF NOT EXISTS idx_pm_markets_creator ON sm.polkamarkt_markets(creator)`);
        // 4.8.8 (spec 130) rewrite: every market carries a settlement mechanism
        // (MigratedLegacy | DynamicPariMutuel). Added idempotently on existing tables.
        await client.query(`ALTER TABLE sm.polkamarkt_markets ADD COLUMN IF NOT EXISTS mechanism TEXT`);

        // Every TradeExecuted event (buy / sell, yes / no). We do NOT
        // aggregate ahead of time — the prob-over-time sparkline is
        // computed on-demand from this table (cheap while dataset is
        // small; we can bucketize later if volume grows).
        await client.query(`
            CREATE TABLE IF NOT EXISTS sm.polkamarkt_trades (
                id          BIGSERIAL PRIMARY KEY,
                market_id   BIGINT NOT NULL,
                trader      TEXT NOT NULL,
                side        TEXT NOT NULL,           -- 'Buy' | 'Sell'
                outcome     TEXT NOT NULL,           -- 'Yes' | 'No'
                collateral  NUMERIC(60) NOT NULL,
                shares      NUMERIC(60) NOT NULL,
                fee         NUMERIC(60) NOT NULL,
                block       INTEGER NOT NULL,
                ts          BIGINT  NOT NULL,
                hash        TEXT
            )
        `);
        await client.query(`CREATE INDEX IF NOT EXISTS idx_pm_trades_market_ts ON sm.polkamarkt_trades(market_id, ts)`);
        await client.query(`CREATE INDEX IF NOT EXISTS idx_pm_trades_trader ON sm.polkamarkt_trades(trader)`);

        // Claims — trader payouts after resolution, and creator fee/liquidity claims.
        await client.query(`
            CREATE TABLE IF NOT EXISTS sm.polkamarkt_claims (
                id         BIGSERIAL PRIMARY KEY,
                market_id  BIGINT NOT NULL,
                account    TEXT   NOT NULL,
                kind       TEXT   NOT NULL,         -- 'payout' | 'creator_fees' | 'creator_liquidity' | 'bond_released'
                amount     NUMERIC(60) NOT NULL,
                block      INTEGER NOT NULL,
                ts         BIGINT  NOT NULL
            )
        `);
        await client.query(`CREATE INDEX IF NOT EXISTS idx_pm_claims_account ON sm.polkamarkt_claims(account)`);

        await client.query(`
            CREATE TABLE IF NOT EXISTS sm.polkamarkt_buybacks (
                id            BIGSERIAL PRIMARY KEY,
                block         INTEGER NOT NULL,
                ts            BIGINT  NOT NULL,
                hash          TEXT,
                kusd_spent    NUMERIC(60) NOT NULL,
                xor_burned    NUMERIC(60) NOT NULL
            )
        `);
        await client.query(`CREATE INDEX IF NOT EXISTS idx_pm_buybacks_ts ON sm.polkamarkt_buybacks(ts DESC)`);

        // DPM residual + legacy-migration residual burns (4.8.8). Distinct from
        // the XOR buyback sweep: these are collateral burned/routed by the new
        // pari-mutuel settlement, recorded so burn metrics stay complete.
        await client.query(`
            CREATE TABLE IF NOT EXISTS sm.polkamarkt_burns (
                id         BIGSERIAL PRIMARY KEY,
                block      INTEGER NOT NULL,
                ts         BIGINT  NOT NULL,
                hash       TEXT,
                market_id  BIGINT,
                kind       TEXT NOT NULL,          -- 'dpm_residual' | 'legacy_migration_residual'
                amount     NUMERIC(60) NOT NULL
            )
        `);
        await client.query(`CREATE INDEX IF NOT EXISTS idx_pm_burns_ts ON sm.polkamarkt_burns(ts DESC)`);

        console.log('[db_pg] polkamarkt schema ready');
    } catch (e) {
        console.error('[db_pg] polkamarkt schema init error:', e.message);
    } finally {
        client.release();
    }
}

async function initNewsSchema() {
    const client = await pool.connect();
    try {
        // Migration: an earlier session created sm.radio_episodes. Rename to
        // news_episodes if found so existing data is preserved.
        await client.query(`
            DO $$
            BEGIN
                IF EXISTS (SELECT 1 FROM information_schema.tables
                           WHERE table_schema='sm' AND table_name='radio_episodes')
                   AND NOT EXISTS (SELECT 1 FROM information_schema.tables
                                   WHERE table_schema='sm' AND table_name='news_episodes')
                THEN
                    EXECUTE 'ALTER TABLE sm.radio_episodes RENAME TO news_episodes';
                    EXECUTE 'ALTER INDEX IF EXISTS sm.idx_radio_published RENAME TO idx_news_published';
                END IF;
            END $$;
        `);
        await client.query(`
            CREATE TABLE IF NOT EXISTS sm.news_episodes (
                slug          TEXT PRIMARY KEY,
                published_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                title_es      TEXT NOT NULL,
                title_en      TEXT NOT NULL,
                summary_es    TEXT,
                summary_en    TEXT,
                cover_path    TEXT NOT NULL,
                audio_path_es TEXT NOT NULL,
                audio_path_en TEXT NOT NULL,
                video_path_es TEXT,
                video_path_en TEXT,
                duration_s    INTEGER,
                source_url    TEXT,
                tags          TEXT[]
            )
        `);
        await client.query(`CREATE INDEX IF NOT EXISTS idx_news_published ON sm.news_episodes(published_at DESC)`);
        console.log('[db_pg] news schema ready');
    } catch (e) {
        console.error('[db_pg] news schema init error:', e.message);
    } finally {
        client.release();
    }
}

async function newsInsertEpisode(ep) {
    await pool.query(
        `INSERT INTO sm.news_episodes
         (slug, title_es, title_en, summary_es, summary_en,
          cover_path, audio_path_es, audio_path_en, video_path_es, video_path_en,
          duration_s, source_url, tags)
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)
         ON CONFLICT (slug) DO UPDATE SET
            title_es=EXCLUDED.title_es, title_en=EXCLUDED.title_en,
            summary_es=EXCLUDED.summary_es, summary_en=EXCLUDED.summary_en,
            cover_path=EXCLUDED.cover_path,
            audio_path_es=EXCLUDED.audio_path_es, audio_path_en=EXCLUDED.audio_path_en,
            video_path_es=EXCLUDED.video_path_es, video_path_en=EXCLUDED.video_path_en,
            duration_s=EXCLUDED.duration_s, source_url=EXCLUDED.source_url, tags=EXCLUDED.tags`,
        [ep.slug, ep.titleEs, ep.titleEn, ep.summaryEs, ep.summaryEn,
         ep.coverPath, ep.audioPathEs, ep.audioPathEn, ep.videoPathEs, ep.videoPathEn,
         ep.durationS, ep.sourceUrl, ep.tags || []]
    );
}

async function newsListEpisodes({ limit = 50, offset = 0 } = {}) {
    const r = await pool.query(
        `SELECT slug, published_at, title_es, title_en, summary_es, summary_en,
                cover_path, audio_path_es, audio_path_en, video_path_es, video_path_en,
                duration_s, source_url, tags
         FROM sm.news_episodes
         ORDER BY published_at DESC
         LIMIT $1 OFFSET $2`,
        [limit, offset]
    );
    return r.rows;
}

async function pmInsertMarket(m) {
    await pool.query(
        `INSERT INTO sm.polkamarkt_markets
         (market_id, condition_id, creator, close_block, collateral_asset, seed_liquidity,
          status, question, oracle, resolution_source,
          opengov_network, opengov_parachain, opengov_track, opengov_referendum,
          created_at_block, created_at_ts, mechanism)
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17)
         ON CONFLICT (market_id) DO NOTHING`,
        [m.marketId, m.conditionId, m.creator, m.closeBlock, m.collateralAsset, m.seedLiquidity,
         m.status || 'Open', m.question, m.oracle, m.resolutionSource,
         m.opengovNetwork, m.opengovParachain, m.opengovTrack, m.opengovReferendum,
         m.block, m.ts, m.mechanism || null]
    );
}

async function pmUpdateMarketStatus(marketId, status, resolution, block, ts) {
    await pool.query(
        `UPDATE sm.polkamarkt_markets
         SET status = $2, resolution = $3,
             resolved_at_block = CASE WHEN $2 IN ('Resolved','Cancelled') THEN $4 ELSE resolved_at_block END,
             resolved_at_ts    = CASE WHEN $2 IN ('Resolved','Cancelled') THEN $5 ELSE resolved_at_ts END
         WHERE market_id = $1`,
        [marketId, status, resolution || null, block, ts]
    );
}

// Reconcile a market's lifecycle fields from on-chain truth. Unlike
// pmUpdateMarketStatus (event-driven, stamps resolved_at from the event block),
// this is called by the periodic chain reconcile and only fills status /
// resolution / mechanism — it never invents a resolved_at it doesn't know.
// Returns rowCount > 0 only when a provided value actually differed.
async function pmReconcileMarketStatus(marketId, { status = null, resolution = null, mechanism = null } = {}) {
    // Cast params to ::text — when a value is null Postgres can't infer the
    // parameter type from COALESCE/IS DISTINCT context ("could not determine
    // data type of parameter"). node --check won't catch this (SQL in a string).
    const r = await pool.query(
        `UPDATE sm.polkamarkt_markets
         SET status     = COALESCE($2::text, status),
             resolution = COALESCE($3::text, resolution),
             mechanism  = COALESCE($4::text, mechanism)
         WHERE market_id = $1
           AND ( ($2::text IS NOT NULL AND status     IS DISTINCT FROM $2::text)
              OR ($3::text IS NOT NULL AND resolution IS DISTINCT FROM $3::text)
              OR ($4::text IS NOT NULL AND mechanism  IS DISTINCT FROM $4::text) )`,
        [marketId, status, resolution, mechanism]
    );
    return r.rowCount;
}

async function pmInsertBurn(b) {
    await pool.query(
        `INSERT INTO sm.polkamarkt_burns (block, ts, hash, market_id, kind, amount)
         VALUES ($1,$2,$3,$4,$5,$6)`,
        [b.block, b.ts, b.hash || null, b.marketId ?? null, b.kind, b.amount]
    );
}

async function pmGetBurnStats() {
    const r = await pool.query(
        `SELECT COUNT(*) AS cnt, COALESCE(SUM(amount), 0)::text AS total FROM sm.polkamarkt_burns`
    );
    const row = r.rows[0] || {};
    return { count: Number(row.cnt) || 0, total: String(row.total || '0') };
}

async function pmInsertTrade(t) {
    await pool.query(
        `INSERT INTO sm.polkamarkt_trades
         (market_id, trader, side, outcome, collateral, shares, fee, block, ts, hash)
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)`,
        [t.marketId, t.trader, t.side, t.outcome, t.collateral, t.shares, t.fee, t.block, t.ts, t.hash || null]
    );
}

async function pmInsertClaim(c) {
    await pool.query(
        `INSERT INTO sm.polkamarkt_claims (market_id, account, kind, amount, block, ts)
         VALUES ($1,$2,$3,$4,$5,$6)`,
        [c.marketId, c.account, c.kind, c.amount, c.block, c.ts]
    );
}

async function pmInsertBuyback(b) {
    await pool.query(
        `INSERT INTO sm.polkamarkt_buybacks (block, ts, hash, kusd_spent, xor_burned)
         VALUES ($1,$2,$3,$4,$5)`,
        [b.block, b.ts, b.hash, b.kusdSpent, b.xorBurned]
    );
}

async function pmGetBuybackStats() {
    const r = await pool.query(`
        SELECT
          COUNT(*)                              AS sweep_count,
          COALESCE(SUM(kusd_spent), 0)::text    AS kusd_spent_total,
          COALESCE(SUM(xor_burned), 0)::text    AS xor_burned_total,
          MAX(ts)                               AS last_ts,
          MAX(block)                            AS last_block
        FROM sm.polkamarkt_buybacks
    `);
    const row = r.rows[0] || {};
    return {
        sweepCount:     Number(row.sweep_count) || 0,
        kusdSpentTotal: String(row.kusd_spent_total || '0'),
        xorBurnedTotal: String(row.xor_burned_total || '0'),
        lastTs:         row.last_ts != null ? Number(row.last_ts) : null,
        lastBlock:      row.last_block != null ? Number(row.last_block) : null,
    };
}

async function pmListBuybacks({ limit = 20, offset = 0 } = {}) {
    const r = await pool.query(
        `SELECT block, ts, hash, kusd_spent::text AS kusd_spent, xor_burned::text AS xor_burned
         FROM sm.polkamarkt_buybacks
         ORDER BY ts DESC
         LIMIT $1 OFFSET $2`,
        [limit, offset]
    );
    return r.rows.map(row => ({
        block:      Number(row.block),
        ts:         Number(row.ts),
        hash:       row.hash,
        kusdSpent:  row.kusd_spent,
        xorBurned:  row.xor_burned,
    }));
}

// Totals for KPI header in the Prediction Markets section.
async function pmGetTotals() {
    const r = await pool.query(`
        SELECT
          COUNT(*)                                              AS markets_total,
          COUNT(*) FILTER (WHERE status = 'Open')               AS markets_active,
          COUNT(*) FILTER (WHERE status = 'Resolved')           AS markets_resolved,
          (SELECT COALESCE(SUM(collateral), 0)::text FROM sm.polkamarkt_trades) AS volume_total
        FROM sm.polkamarkt_markets
    `);
    // Distinct collateral assets across all markets — lets the UI label the
    // cumulative volume with a token symbol only when every market shares one
    // collateral (summing raw amounts across different tokens is meaningless).
    const cRes = await pool.query(`SELECT DISTINCT collateral_asset FROM sm.polkamarkt_markets WHERE collateral_asset LIKE '0x%'`);
    return {
        markets: Number(r.rows[0].markets_total) || 0,
        active: Number(r.rows[0].markets_active) || 0,
        resolved: Number(r.rows[0].markets_resolved) || 0,
        // volume kept as a raw (1e18) string — the UI scales + labels it.
        volume: String(r.rows[0].volume_total || '0'),
        collaterals: cRes.rows.map(x => x.collateral_asset),
    };
}

// Paginated markets list for the main Markets tab.
async function pmGetMarkets({ page = 1, limit = 25, status = null } = {}) {
    const params = [];
    const where = [];
    if (status && status !== 'all') { params.push(status); where.push(`status = $${params.length}`); }
    const whereSql = where.length ? 'WHERE ' + where.join(' AND ') : '';
    params.push(limit, (page - 1) * limit);
    // volume = SUM(collateral) of all trades. coll_yes/coll_no = net collateral
    // (money, Buy adds / Sell subtracts) wagered on each outcome — this drives
    // the list's bar. We use MONEY, not share counts: in an outcome-token AMM the
    // share count is inverse to price (a cheaper outcome yields more shares per
    // unit collateral), so a share-split bar would be directionally misleading.
    // The collateral split is purely factual ("how much is on YES vs NO"). We
    // also keep net yes_shares/no_shares for the drill's share readout.
    const data = await pool.query(
        `SELECT m.*, v.volume, v.yes_shares, v.no_shares, v.coll_yes, v.coll_no
         FROM sm.polkamarkt_markets m
         LEFT JOIN LATERAL (
             SELECT
               COALESCE(SUM(collateral), 0) AS volume,
               COALESCE(SUM(CASE WHEN side = 'Buy'  AND outcome = 'Yes' THEN shares ELSE 0 END)
                      - SUM(CASE WHEN side = 'Sell' AND outcome = 'Yes' THEN shares ELSE 0 END), 0) AS yes_shares,
               COALESCE(SUM(CASE WHEN side = 'Buy'  AND outcome = 'No'  THEN shares ELSE 0 END)
                      - SUM(CASE WHEN side = 'Sell' AND outcome = 'No'  THEN shares ELSE 0 END), 0) AS no_shares,
               COALESCE(SUM(CASE WHEN outcome = 'Yes' THEN (CASE WHEN side = 'Sell' THEN -collateral ELSE collateral END) ELSE 0 END), 0) AS coll_yes,
               COALESCE(SUM(CASE WHEN outcome = 'No'  THEN (CASE WHEN side = 'Sell' THEN -collateral ELSE collateral END) ELSE 0 END), 0) AS coll_no
             FROM sm.polkamarkt_trades t WHERE t.market_id = m.market_id
         ) v ON true
         ${whereSql}
         ORDER BY m.market_id DESC
         LIMIT $${params.length - 1} OFFSET $${params.length}`,
        params
    );
    const countParams = status && status !== 'all' ? [status] : [];
    const countRes = await pool.query(
        `SELECT COUNT(*) AS cnt FROM sm.polkamarkt_markets ${whereSql}`,
        countParams
    );
    const total = Number(countRes.rows[0].cnt) || 0;
    return {
        data: data.rows,
        total,
        page,
        totalPages: Math.max(1, Math.ceil(total / limit)),
    };
}

// Full detail of a market: metadata + recent trades + top positions +
// probability history buckets.
async function pmGetMarketDetail(marketId) {
    const mRes = await pool.query(`SELECT * FROM sm.polkamarkt_markets WHERE market_id = $1`, [marketId]);
    if (mRes.rows.length === 0) return null;
    const market = mRes.rows[0];

    // Market-level aggregates so the drill's bar/readout match the list (the
    // market row itself carries no trade totals). volume + net shares + net
    // collateral per outcome — same definitions as pmGetMarkets.
    const aggRes = await pool.query(
        `SELECT
           COALESCE(SUM(collateral), 0)::text AS volume,
           COALESCE(SUM(CASE WHEN side='Buy' AND outcome='Yes' THEN shares ELSE 0 END)
                  - SUM(CASE WHEN side='Sell' AND outcome='Yes' THEN shares ELSE 0 END), 0)::text AS yes_shares,
           COALESCE(SUM(CASE WHEN side='Buy' AND outcome='No'  THEN shares ELSE 0 END)
                  - SUM(CASE WHEN side='Sell' AND outcome='No'  THEN shares ELSE 0 END), 0)::text AS no_shares,
           COALESCE(SUM(CASE WHEN outcome='Yes' THEN (CASE WHEN side='Sell' THEN -collateral ELSE collateral END) ELSE 0 END), 0)::text AS coll_yes,
           COALESCE(SUM(CASE WHEN outcome='No'  THEN (CASE WHEN side='Sell' THEN -collateral ELSE collateral END) ELSE 0 END), 0)::text AS coll_no
         FROM sm.polkamarkt_trades WHERE market_id = $1`,
        [marketId]
    );
    Object.assign(market, aggRes.rows[0] || {});

    const tradesRes = await pool.query(
        `SELECT * FROM sm.polkamarkt_trades WHERE market_id = $1 ORDER BY ts DESC LIMIT 20`,
        [marketId]
    );

    // Top positions: net collateral per trader (positive = long yes+no).
    const topRes = await pool.query(
        `SELECT
            trader,
            SUM(CASE WHEN side = 'Buy'  AND outcome = 'Yes' THEN shares      ELSE 0 END)
          - SUM(CASE WHEN side = 'Sell' AND outcome = 'Yes' THEN shares      ELSE 0 END) AS yes_shares,
            SUM(CASE WHEN side = 'Buy'  AND outcome = 'No'  THEN shares      ELSE 0 END)
          - SUM(CASE WHEN side = 'Sell' AND outcome = 'No'  THEN shares      ELSE 0 END) AS no_shares,
            SUM(CASE WHEN side = 'Buy'  THEN  collateral ELSE -collateral END) AS net_collateral
         FROM sm.polkamarkt_trades
         WHERE market_id = $1
         GROUP BY trader
         ORDER BY ABS(SUM(CASE WHEN side = 'Buy' THEN collateral ELSE -collateral END)) DESC
         LIMIT 10`,
        [marketId]
    );

    // Probability history — hourly buckets, prob_yes derived from cumulative
    // shares. Cheap enough while data is small.
    const probRes = await pool.query(
        `SELECT
            date_trunc('hour', to_timestamp(ts / 1000)) AS bucket,
            SUM(CASE WHEN outcome = 'Yes' AND side = 'Buy'  THEN  shares ELSE 0 END) -
            SUM(CASE WHEN outcome = 'Yes' AND side = 'Sell' THEN  shares ELSE 0 END) AS yes_delta,
            SUM(CASE WHEN outcome = 'No'  AND side = 'Buy'  THEN  shares ELSE 0 END) -
            SUM(CASE WHEN outcome = 'No'  AND side = 'Sell' THEN  shares ELSE 0 END) AS no_delta
         FROM sm.polkamarkt_trades
         WHERE market_id = $1
         GROUP BY bucket
         ORDER BY bucket`,
        [marketId]
    );

    return {
        market,
        recentTrades: tradesRes.rows,
        topPositions: topRes.rows,
        probHistory: probRes.rows,
    };
}

// User-scoped positions for Portfolio / wallet-drill tables.
async function pmGetUserPositions(addr) {
    const r = await pool.query(
        `SELECT
            t.market_id,
            m.question,
            m.status,
            m.resolution,
            SUM(CASE WHEN t.side = 'Buy'  AND t.outcome = 'Yes' THEN t.shares     ELSE 0 END) -
            SUM(CASE WHEN t.side = 'Sell' AND t.outcome = 'Yes' THEN t.shares     ELSE 0 END) AS yes_shares,
            SUM(CASE WHEN t.side = 'Buy'  AND t.outcome = 'No'  THEN t.shares     ELSE 0 END) -
            SUM(CASE WHEN t.side = 'Sell' AND t.outcome = 'No'  THEN t.shares     ELSE 0 END) AS no_shares,
            SUM(CASE WHEN t.side = 'Buy'  THEN  t.collateral ELSE -t.collateral END)         AS net_collateral
         FROM sm.polkamarkt_trades t
         JOIN sm.polkamarkt_markets m ON m.market_id = t.market_id
         WHERE t.trader = $1
         GROUP BY t.market_id, m.question, m.status, m.resolution
         HAVING
           SUM(CASE WHEN t.side = 'Buy'  AND t.outcome = 'Yes' THEN t.shares ELSE 0 END) -
           SUM(CASE WHEN t.side = 'Sell' AND t.outcome = 'Yes' THEN t.shares ELSE 0 END) > 0
         OR
           SUM(CASE WHEN t.side = 'Buy'  AND t.outcome = 'No'  THEN t.shares ELSE 0 END) -
           SUM(CASE WHEN t.side = 'Sell' AND t.outcome = 'No'  THEN t.shares ELSE 0 END) > 0
         ORDER BY t.market_id DESC`,
        [addr]
    );
    return r.rows;
}

// ─────────────────────────────────────────────────────────────────────
// Fee-burns live indexer helpers (added 2026-04-25, schema fee-burns rewrite)
// ─────────────────────────────────────────────────────────────────────

async function initFeeBurnsLiveSchema() {
    await pool.query(`
        CREATE TABLE IF NOT EXISTS sm.fee_burns_live (
            block_height        BIGINT PRIMARY KEY,
            ts                  BIGINT NOT NULL,
            fees_paid_xor       NUMERIC(40, 18) NOT NULL DEFAULT 0,
            ref_paid_xor        NUMERIC(40, 18) NOT NULL DEFAULT 0,
            ref_redirected_xor  NUMERIC(40, 18) NOT NULL DEFAULT 0,
            remint_xor_burned   NUMERIC(40, 18) NOT NULL DEFAULT 0,
            remint_val_burned   NUMERIC(40, 18) NOT NULL DEFAULT 0,
            remint_kusd_burned  NUMERIC(40, 18) NOT NULL DEFAULT 0,
            remint_tbcd_burned  NUMERIC(40, 18) NOT NULL DEFAULT 0
        )
    `);
    await pool.query(`CREATE INDEX IF NOT EXISTS idx_fbl_ts ON sm.fee_burns_live (ts)`);
}

async function insertFeeBurnRow(row) {
    await pool.query(
        `INSERT INTO sm.fee_burns_live
            (block_height, ts, fees_paid_xor, ref_paid_xor, ref_redirected_xor,
             remint_xor_burned, remint_val_burned, remint_kusd_burned, remint_tbcd_burned)
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
         ON CONFLICT (block_height) DO UPDATE SET
             fees_paid_xor       = EXCLUDED.fees_paid_xor,
             ref_paid_xor        = EXCLUDED.ref_paid_xor,
             ref_redirected_xor  = EXCLUDED.ref_redirected_xor,
             remint_xor_burned   = EXCLUDED.remint_xor_burned,
             remint_val_burned   = EXCLUDED.remint_val_burned,
             remint_kusd_burned  = EXCLUDED.remint_kusd_burned,
             remint_tbcd_burned  = EXCLUDED.remint_tbcd_burned`,
        [row.block_height, row.ts,
         row.fees_paid_xor || 0, row.ref_paid_xor || 0, row.ref_redirected_xor || 0,
         row.remint_xor_burned || 0, row.remint_val_burned || 0,
         row.remint_kusd_burned || 0, row.remint_tbcd_burned || 0]
    );
}

async function getFeeBurnsWindow(seconds) {
    const since = seconds === 0 ? 0 : Date.now() - seconds * 1000;
    const r = await pool.query(
        `SELECT
            COALESCE(SUM(fees_paid_xor),       0)::float8 AS fees_paid_xor,
            COALESCE(SUM(ref_paid_xor),        0)::float8 AS ref_paid_xor,
            COALESCE(SUM(ref_redirected_xor),  0)::float8 AS ref_redirected_xor,
            COALESCE(SUM(remint_xor_burned),   0)::float8 AS remint_xor_burned,
            COALESCE(SUM(remint_val_burned),   0)::float8 AS remint_val_burned,
            COALESCE(SUM(remint_kusd_burned),  0)::float8 AS remint_kusd_burned,
            COALESCE(SUM(remint_tbcd_burned),  0)::float8 AS remint_tbcd_burned,
            COUNT(*)                                       AS rows,
            COALESCE(MIN(ts), 0)                           AS min_ts,
            COALESCE(MAX(ts), 0)                           AS max_ts
         FROM sm.fee_burns_live
         WHERE ts >= $1`,
        [since]
    );
    return r.rows[0] || {};
}

// Real burn time-series, grouped by day, for the cumulative burn chart.
// Returns one row per day with per-token burn sums (real on-chain, from the
// indexer). Empty until the indexer has enough days of data.
async function getFeeBurnsSeries(days) {
    const since = Date.now() - days * 86400000;
    const r = await pool.query(
        `SELECT
            (ts / 86400000)::bigint                        AS day_bucket,
            COALESCE(SUM(remint_xor_burned),  0)::float8   AS xor,
            COALESCE(SUM(remint_val_burned),  0)::float8   AS val,
            COALESCE(SUM(remint_kusd_burned), 0)::float8   AS kusd,
            COALESCE(SUM(remint_tbcd_burned), 0)::float8   AS tbcd
         FROM sm.fee_burns_live
         WHERE ts >= $1
         GROUP BY day_bucket
         ORDER BY day_bucket ASC`,
        [since]
    );
    return r.rows.map(row => ({
        ts: Number(row.day_bucket) * 86400000,
        xor: Number(row.xor) || 0,
        val: Number(row.val) || 0,
        kusd: Number(row.kusd) || 0,
        tbcd: Number(row.tbcd) || 0,
    }));
}

// Exposed for /block/:n event enrichment (asset_id hex -> { symbol, decimals }).
function getAssetIdToInfo() { return _assetIdToInfo; }

// VAL staking rewards (4.8.6+) — `xorFee.ValStakingRewardPaid` event indexer.
// Idempotent schema + insert; silent until the event starts firing post-enactment.
async function initValStakingRewardsSchema() {
    await pool.query(`
        CREATE TABLE IF NOT EXISTS sm.val_staking_rewards (
            id BIGSERIAL PRIMARY KEY,
            era INTEGER NOT NULL,
            page INTEGER NOT NULL,
            validator_stash TEXT NOT NULL,
            destination TEXT NOT NULL,
            amount NUMERIC(78, 0) NOT NULL,
            block_num BIGINT NOT NULL,
            block_hash TEXT,
            ts TIMESTAMPTZ NOT NULL DEFAULT now(),
            CONSTRAINT val_staking_rewards_uniq UNIQUE (era, page, validator_stash, destination, block_num)
        );
        CREATE INDEX IF NOT EXISTS val_staking_rewards_validator_era_idx
            ON sm.val_staking_rewards (validator_stash, era);
        CREATE INDEX IF NOT EXISTS val_staking_rewards_destination_ts_idx
            ON sm.val_staking_rewards (destination, ts DESC);
        CREATE INDEX IF NOT EXISTS val_staking_rewards_ts_idx
            ON sm.val_staking_rewards (ts DESC);
    `);
}

async function insertValStakingReward({ era, page, validator_stash, destination, amount, block_num, block_hash, ts }) {
    await pool.query(`
        INSERT INTO sm.val_staking_rewards
            (era, page, validator_stash, destination, amount, block_num, block_hash, ts)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
        ON CONFLICT (era, page, validator_stash, destination, block_num) DO NOTHING
    `, [era, page, validator_stash, destination, amount, block_num, block_hash, ts]);
}

// Read-side aggregates on real indexed events. Returns numeric strings to keep
// 18-decimal precision (caller divides by 1e18 when displaying as VAL).
async function getValStakingNetworkTotals() {
    const buckets = [
        { key: 'all',   sql: '' },
        { key: 'h6',    sql: "WHERE ts > now() - interval '6 hours'" },
        { key: 'h12',   sql: "WHERE ts > now() - interval '12 hours'" },
        { key: 'h24',   sql: "WHERE ts > now() - interval '24 hours'" },
        { key: 'd3',    sql: "WHERE ts > now() - interval '3 days'" },
        { key: 'd6',    sql: "WHERE ts > now() - interval '6 days'" },
        { key: 'd30',   sql: "WHERE ts > now() - interval '30 days'" },
        { key: 'd90',   sql: "WHERE ts > now() - interval '90 days'" },
        { key: 'd180',  sql: "WHERE ts > now() - interval '180 days'" },
        { key: 'd365',  sql: "WHERE ts > now() - interval '365 days'" },
    ];
    const result = {};
    for (const b of buckets) {
        const r = await pool.query(`
            SELECT
                COALESCE(SUM(amount), 0)::text AS total_amount,
                COUNT(*)::int AS payout_count,
                COUNT(DISTINCT validator_stash)::int AS validator_count,
                COUNT(DISTINCT destination)::int AS destination_count
            FROM sm.val_staking_rewards ${b.sql}
        `);
        result[b.key] = r.rows[0];
    }
    return result;
}

async function getValStakingPerValidator() {
    const r = await pool.query(`
        SELECT
            validator_stash,
            COALESCE(SUM(amount), 0)::text AS total_amount,
            COUNT(*)::int AS payout_count,
            COUNT(DISTINCT era)::int AS era_count,
            MAX(era)::int AS last_era,
            MAX(ts) AS last_ts
        FROM sm.val_staking_rewards
        GROUP BY validator_stash
        ORDER BY SUM(amount) DESC
    `);
    return r.rows;
}

async function getValStakingTopDestinations(limit = 10) {
    const r = await pool.query(`
        SELECT
            destination,
            COALESCE(SUM(amount), 0)::text AS total_amount,
            COUNT(*)::int AS payout_count,
            COUNT(DISTINCT validator_stash)::int AS validator_count
        FROM sm.val_staking_rewards
        GROUP BY destination
        ORDER BY SUM(amount) DESC
        LIMIT $1
    `, [limit]);
    return r.rows;
}

async function getValStakingForDestination(destination) {
    const r = await pool.query(`
        SELECT
            era,
            page,
            validator_stash,
            amount::text AS amount,
            ts,
            block_num
        FROM sm.val_staking_rewards
        WHERE destination = $1
        ORDER BY era DESC, page DESC
    `, [destination]);
    return r.rows;
}

// (validator_stash, era) pairs already paid out, as recorded by the indexer the moment the
// payout lands on-chain. Used to mark eras claimed in /staking/rewards WITHOUT waiting for the
// local node's staking.claimedRewards to catch up (it lags behind chain head). Returns a Set of
// `${validator_stash}:${era}` keys, matching the claimedByValidatorEra map keying.
async function getClaimedValStakingPairs() {
    const r = await pool.query(`
        SELECT DISTINCT validator_stash, era
        FROM sm.val_staking_rewards
    `);
    return new Set(r.rows.map(row => `${row.validator_stash}:${row.era}`));
}

// Historical VAL→XOR rate range (min/max) per window, from sm.price_history hourly medians.
// rate = VAL_price_usd / XOR_price_usd per hour (both priced in DAI by the price indexer).
// hour_bucket is a UNIX-seconds bucket. Returns { h24:{min,max}, d7:{min,max}, d30:{min,max} }
// with nulls when a window has no data. Real on-chain-derived data only — no estimates.
const XOR_ASSET_ID = '0x0200000000000000000000000000000000000000000000000000000000000000';
const VAL_ASSET_ID = '0x0200040000000000000000000000000000000000000000000000000000000000';
async function getValXorRateWindows() {
    const r = await pool.query(`
        WITH x AS (SELECT hour_bucket h, price_usd xp FROM sm.price_history WHERE asset_id = $1 AND price_usd > 0),
             v AS (SELECT hour_bucket h, price_usd vp FROM sm.price_history WHERE asset_id = $2 AND price_usd > 0),
             r AS (SELECT x.h, v.vp / x.xp AS ratio FROM x JOIN v ON v.h = x.h),
             n AS (SELECT EXTRACT(EPOCH FROM NOW())::bigint nh)
        SELECT
            MIN(ratio) FILTER (WHERE h >= nh - 86400)   AS min_h24,
            MAX(ratio) FILTER (WHERE h >= nh - 86400)   AS max_h24,
            MIN(ratio) FILTER (WHERE h >= nh - 604800)  AS min_d7,
            MAX(ratio) FILTER (WHERE h >= nh - 604800)  AS max_d7,
            MIN(ratio) FILTER (WHERE h >= nh - 2592000) AS min_d30,
            MAX(ratio) FILTER (WHERE h >= nh - 2592000) AS max_d30
        FROM r, n
    `, [XOR_ASSET_ID, VAL_ASSET_ID]);
    const row = r.rows[0] || {};
    const num = (x) => (x == null ? null : Number(x));
    return {
        h24: { min: num(row.min_h24), max: num(row.max_h24) },
        d7:  { min: num(row.min_d7),  max: num(row.max_d7)  },
        d30: { min: num(row.min_d30), max: num(row.max_d30) },
    };
}

// Aligned hourly price series for N tokens over a window, from sm.price_history (price_usd in DAI;
// hour_bucket is UNIX seconds). Downsampled in SQL to ~150-270 points per token so the payload
// stays small even for 'all' (≈5 years). Real on-chain-derived prices only — no estimates.
// Returns { window, bucketSec, series: { assetId: [{ t, p }] } } (t = bucket-start UNIX seconds).
const PRICE_SERIES_WINDOWS = {
    '7d':   { backSec: 604800,    bucketSec: 3600   },  // 1h  → ~168 pts
    '30d':  { backSec: 2592000,   bucketSec: 14400  },  // 4h  → ~180 pts
    '90d':  { backSec: 7776000,   bucketSec: 43200  },  // 12h → ~180 pts
    '365d': { backSec: 31536000,  bucketSec: 172800 },  // 2d  → ~182 pts
    'all':  { backSec: null,      bucketSec: 604800 },  // 1wk → ~270 pts (since 2021)
};
async function getPriceSeries(assetIds, windowKey = '30d') {
    const ids = (Array.isArray(assetIds) ? assetIds : [assetIds]).filter(Boolean).slice(0, 4);
    if (ids.length === 0) return { window: windowKey, bucketSec: 0, series: {} };
    const w = PRICE_SERIES_WINDOWS[windowKey] || PRICE_SERIES_WINDOWS['30d'];
    const fromSec = w.backSec == null ? 0 : Math.floor(Date.now() / 1000) - w.backSec;
    const r = await pool.query(`
        SELECT asset_id,
               (hour_bucket / $2)::bigint * $2 AS t,
               AVG(price_usd) AS p
        FROM sm.price_history
        WHERE asset_id = ANY($1) AND price_usd > 0 AND hour_bucket >= $3
        GROUP BY asset_id, t
        ORDER BY t ASC
    `, [ids, w.bucketSec, fromSec]);
    const series = {};
    for (const id of ids) series[id] = [];
    for (const row of r.rows) {
        (series[row.asset_id] || (series[row.asset_id] = [])).push({ t: Number(row.t), p: Number(row.p) });
    }
    return { window: windowKey, bucketSec: w.bucketSec, series };
}

// Real network-wide extrinsic stats for a rolling window (default 24h).
// Returns { total, success, failed, successRate, topPallet, topPalletCount }
// over mv_extrinsics ∪ live_extrinsics — NOT the page the UI happens to show.
let _extStats24Cache = { data: null, ts: 0 };
async function getExtrinsicStats24h(windowMs = 86400000) {
    if (_extStats24Cache.data && Date.now() - _extStats24Cache.ts < 30000) return _extStats24Cache.data;
    const startTime = Date.now() - windowMs;
    const cond = "timestamp >= $1 AND NOT (section = 'timestamp' AND method = 'set')";
    const totalsRes = await pool.query(
        `SELECT
            COUNT(*) AS total,
            COUNT(*) FILTER (WHERE success = 1) AS success
         FROM (SELECT timestamp, section, method, success FROM sm.mv_extrinsics WHERE ${cond}
               UNION ALL SELECT timestamp, section, method, success FROM sm.live_extrinsics WHERE ${cond}) AS _u`,
        [startTime]
    );
    const palletRes = await pool.query(
        `SELECT section, COUNT(*) AS cnt
         FROM (SELECT timestamp, section, method FROM sm.mv_extrinsics WHERE ${cond}
               UNION ALL SELECT timestamp, section, method FROM sm.live_extrinsics WHERE ${cond}) AS _u
         GROUP BY section ORDER BY cnt DESC LIMIT 1`,
        [startTime]
    );
    const total = parseInt(totalsRes.rows[0]?.total) || 0;
    const success = parseInt(totalsRes.rows[0]?.success) || 0;
    const data = {
        total,
        success,
        failed: total - success,
        successRate: total ? +(success / total * 100).toFixed(1) : 0,
        topPallet: palletRes.rows[0]?.section || null,
        topPalletCount: parseInt(palletRes.rows[0]?.cnt) || 0,
        windowMs,
    };
    _extStats24Cache = { data, ts: Date.now() };
    return data;
}

// Real per-block fees for a set of blocks. The runtime emits one
// transactionPayment.TransactionFeePaid per signed extrinsic; the live indexer
// stores it in live_fees keyed by block (+type), and mv_fees holds the historical
// part. live_fees stays within ~100 blocks of the chain head, so recent rows DO
// have a fee — unlike events_json, which is null for recent blocks. ~97% of
// blocks carry exactly one fee row, so for those the block fee IS that signed
// extrinsic's fee. Returns { [block]: { totalXor, totalUsd, rows } }. amount
// capped like getFeeStats to drop denomination-boundary outliers.
async function getFeesByBlocks(blocks) {
    const list = (Array.isArray(blocks) ? blocks : [])
        .map(b => parseInt(b)).filter(Number.isFinite).slice(0, 100);
    if (list.length === 0) return {};
    const res = await pool.query(
        `SELECT block, amount, usd_value FROM (
            SELECT block, amount, usd_value FROM sm.mv_fees WHERE block = ANY($1)
            UNION ALL SELECT block, amount, usd_value FROM sm.live_fees WHERE block = ANY($1)
         ) AS _u WHERE amount > 0 AND amount <= $2`,
        [list, FEE_AMOUNT_CAP]
    );
    const out = {};
    for (const r of res.rows) {
        const b = Number(r.block);
        if (!out[b]) out[b] = { totalXor: 0, totalUsd: 0, rows: 0 };
        out[b].totalXor += Number(r.amount) || 0;
        out[b].totalUsd += Number(r.usd_value) || 0;
        out[b].rows += 1;
    }
    return out;
}

module.exports = {
    getExtrinsicStats24h,
    getFeesByBlocks,
    initDB,
    getAssetIdToInfo,
    initFeeBurnsLiveSchema, insertFeeBurnRow, getFeeBurnsWindow, getFeeBurnsSeries,
    initPolkamarktSchema,
    pmInsertMarket, pmUpdateMarketStatus, pmReconcileMarketStatus,
    pmInsertTrade, pmInsertClaim,
    pmInsertBuyback, pmGetBuybackStats, pmListBuybacks,
    pmInsertBurn, pmGetBurnStats,
    pmGetTotals, pmGetMarkets, pmGetMarketDetail, pmGetUserPositions,
    initNewsSchema, newsInsertEpisode, newsListEpisodes,
    initValStakingRewardsSchema, insertValStakingReward,
    getValStakingNetworkTotals, getValStakingPerValidator,
    getValStakingTopDestinations, getValStakingForDestination,
    getClaimedValStakingPairs,
    getValXorRateWindows,
    getPriceSeries,
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
    getExtrinsicDetail,
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
    getBurnStatsFromChain,
    getSupplySnapshotDelta,
    purgeSupplySnapshotsForSymbol,
    lookupExtrinsicUsdValue,
    globalSearch,
    getSwapVolumeUsd,
    getWalletInfo,
    getExportData,
    updatePriceHistory,
};
