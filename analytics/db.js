'use strict';
// analytics/db.js — Site meta-analytics storage (visits, sections, sessions,
// interactions). Isolated module with its own pg Pool to the same PG, so it
// never edits or competes with the hot db_pg.js. Writes to sm.site_* tables.
//
// Design constraints:
//  - PII: never store a raw IP. `visitor` = sha256(ip + ua + day + rotating salt).
//  - Disk (VPS ~78%): raw rows kept RAW_RETENTION_DAYS (30); daily rollup in
//    sm.site_daily keeps the 1-year window without millions of raw rows.
//  - Throughput: /hit enqueues in memory; a batch INSERT flushes every ~5s or
//    500 events, so a beacon never awaits the DB.

const { Pool } = require('pg');
const crypto = require('crypto');

const PG_CONFIG = {
    host: process.env.PG_HOST || 'localhost',
    port: parseInt(process.env.PG_PORT, 10) || 23798,
    database: process.env.PG_DB || 'squid',
    user: process.env.PG_USER || 'postgres',
    password: process.env.PG_PASS || 'squid',
    max: parseInt(process.env.ANALYTICS_PG_MAX, 10) || 4,
};

const RAW_RETENTION_DAYS = parseInt(process.env.ANALYTICS_RAW_RETENTION_DAYS, 10) || 30;
const FLUSH_INTERVAL_MS = parseInt(process.env.ANALYTICS_FLUSH_MS, 10) || 5000;
const FLUSH_MAX = parseInt(process.env.ANALYTICS_FLUSH_MAX, 10) || 500;
const BUFFER_HARD_CAP = parseInt(process.env.ANALYTICS_BUFFER_CAP, 10) || 50000;

let pool = null;
function getPool() {
    if (!pool) pool = new Pool(PG_CONFIG);
    return pool;
}

async function initSchema() {
    await getPool().query(`
        CREATE TABLE IF NOT EXISTS sm.site_events (
            id          BIGSERIAL PRIMARY KEY,
            ts          TIMESTAMPTZ NOT NULL DEFAULT now(),
            type        TEXT NOT NULL,
            section     TEXT,
            visitor     TEXT,
            session_id  TEXT,
            path        TEXT,
            referrer    TEXT,
            country     TEXT,
            device      TEXT,
            duration_ms BIGINT,
            meta        JSONB
        );
        CREATE INDEX IF NOT EXISTS idx_site_events_ts      ON sm.site_events (ts DESC);
        CREATE INDEX IF NOT EXISTS idx_site_events_type_ts ON sm.site_events (type, ts DESC);
        CREATE INDEX IF NOT EXISTS idx_site_events_section ON sm.site_events (section, ts DESC) WHERE section IS NOT NULL;
        CREATE INDEX IF NOT EXISTS idx_site_events_visitor ON sm.site_events (visitor, ts DESC) WHERE visitor IS NOT NULL;

        CREATE TABLE IF NOT EXISTS sm.site_daily (
            day            DATE NOT NULL,
            section        TEXT NOT NULL DEFAULT '',
            pageviews      BIGINT NOT NULL DEFAULT 0,
            section_views  BIGINT NOT NULL DEFAULT 0,
            sessions       BIGINT NOT NULL DEFAULT 0,
            uniques        BIGINT NOT NULL DEFAULT 0,
            avg_session_ms BIGINT NOT NULL DEFAULT 0,
            PRIMARY KEY (day, section)
        );
    `);
}

// Anonymous visitor id: salt rotates daily, so a hash can't be linked across
// days or reversed to an IP. Same (ip,ua) within a day collapses to one id.
function visitorHash(ip, ua) {
    const day = new Date().toISOString().slice(0, 10);
    const salt = process.env.ANALYTICS_SALT || 'sorametrics-default-salt';
    return crypto.createHash('sha256')
        .update(`${ip || ''}|${ua || ''}|${day}|${salt}`)
        .digest('hex')
        .slice(0, 32);
}

// --- Batched ingestion: enqueue in memory, flush periodically ---
let buffer = [];
let flushTimer = null;

function enqueue(event) {
    if (buffer.length >= BUFFER_HARD_CAP) return; // memory guard against beacon floods
    buffer.push(event);
    if (buffer.length >= FLUSH_MAX) flush().catch(() => {});
}

async function flush() {
    if (buffer.length === 0) return { inserted: 0 };
    const batch = buffer;
    buffer = [];
    const rows = [];
    const params = [];
    let i = 1;
    for (const e of batch) {
        rows.push(`(to_timestamp($${i++}/1000.0), $${i++}, $${i++}, $${i++}, $${i++}, $${i++}, $${i++}, $${i++}, $${i++}, $${i++}, $${i++})`);
        params.push(
            e.ts || Date.now(),
            e.type,
            e.section || null,
            e.visitor || null,
            e.session_id || null,
            e.path || null,
            e.referrer || null,
            e.country || null,
            e.device || null,
            e.duration_ms != null ? e.duration_ms : null,
            e.meta ? JSON.stringify(e.meta) : null
        );
    }
    try {
        await getPool().query(
            `INSERT INTO sm.site_events
             (ts, type, section, visitor, session_id, path, referrer, country, device, duration_ms, meta)
             VALUES ${rows.join(',')}`,
            params
        );
        return { inserted: batch.length };
    } catch (err) {
        console.error('[analytics] flush failed, dropping', batch.length, 'events:', err.message);
        return { inserted: 0, error: err.message };
    }
}

function startFlushLoop() {
    if (flushTimer) return;
    flushTimer = setInterval(() => { flush().catch(() => {}); }, FLUSH_INTERVAL_MS);
    if (flushTimer.unref) flushTimer.unref();
}

// Roll complete past days into sm.site_daily (idempotent), then prune raw rows
// beyond retention. Site-wide row uses section=''; per-section rows use the
// section name. Run daily.
async function rollupAndPrune() {
    const p = getPool();
    await p.query(`
        INSERT INTO sm.site_daily (day, section, pageviews, section_views, sessions, uniques, avg_session_ms)
        SELECT
            (ts AT TIME ZONE 'UTC')::date,
            '',
            COUNT(*) FILTER (WHERE type='pageview'),
            COUNT(*) FILTER (WHERE type='section'),
            COUNT(DISTINCT session_id),
            COUNT(DISTINCT visitor),
            COALESCE(AVG(duration_ms) FILTER (WHERE type='session_end'), 0)::bigint
        FROM sm.site_events
        WHERE ts < date_trunc('day', now())
        GROUP BY 1
        ON CONFLICT (day, section) DO UPDATE SET
            pageviews=EXCLUDED.pageviews, section_views=EXCLUDED.section_views,
            sessions=EXCLUDED.sessions, uniques=EXCLUDED.uniques, avg_session_ms=EXCLUDED.avg_session_ms
    `);
    await p.query(`
        INSERT INTO sm.site_daily (day, section, section_views, uniques)
        SELECT (ts AT TIME ZONE 'UTC')::date, section, COUNT(*), COUNT(DISTINCT visitor)
        FROM sm.site_events
        WHERE ts < date_trunc('day', now()) AND type='section' AND section IS NOT NULL
        GROUP BY 1, 2
        ON CONFLICT (day, section) DO UPDATE SET
            section_views=EXCLUDED.section_views, uniques=EXCLUDED.uniques
    `);
    const del = await p.query(
        `DELETE FROM sm.site_events WHERE ts < now() - ($1 || ' days')::interval`,
        [String(RAW_RETENTION_DAYS)]
    );
    return { pruned: del.rowCount || 0 };
}

// Dashboard payload. 24h/7d/30d come from raw (retention covers them); 1y
// comes from the daily rollup + today's raw. uniques for 1y is daily-distinct
// summed (approximate — true period-unique needs HLL, F3). Cached by the route.
async function getSiteStats() {
    const p = getPool();
    const win = { '24h': '24 hours', '7d': '7 days', '30d': '30 days' };

    const [w24, w7, w30, year, today, secs, avg] = await Promise.all([
        p.query(`SELECT COUNT(*) FILTER (WHERE type='pageview') v, COUNT(DISTINCT visitor) u, COUNT(DISTINCT session_id) s FROM sm.site_events WHERE ts >= now() - $1::interval`, [win['24h']]),
        p.query(`SELECT COUNT(*) FILTER (WHERE type='pageview') v, COUNT(DISTINCT visitor) u, COUNT(DISTINCT session_id) s FROM sm.site_events WHERE ts >= now() - $1::interval`, [win['7d']]),
        p.query(`SELECT COUNT(*) FILTER (WHERE type='pageview') v, COUNT(DISTINCT visitor) u, COUNT(DISTINCT session_id) s FROM sm.site_events WHERE ts >= now() - $1::interval`, [win['30d']]),
        p.query(`SELECT COALESCE(SUM(pageviews),0) v, COALESCE(SUM(uniques),0) u, COALESCE(SUM(sessions),0) s FROM sm.site_daily WHERE section='' AND day >= current_date - 365 AND day < current_date`),
        p.query(`SELECT COUNT(*) FILTER (WHERE type='pageview') v, COUNT(DISTINCT visitor) u, COUNT(DISTINCT session_id) s FROM sm.site_events WHERE ts >= date_trunc('day', now())`),
        p.query(`SELECT section, COUNT(*) views, COUNT(DISTINCT visitor) uniques FROM sm.site_events WHERE type='section' AND section IS NOT NULL AND ts >= now() - interval '30 days' GROUP BY section ORDER BY views DESC LIMIT 30`),
        p.query(`SELECT COALESCE(AVG(duration_ms),0)::bigint a FROM sm.site_events WHERE type='session_end' AND ts >= now() - interval '30 days'`),
    ]);
    const n = (x) => Number(x || 0);

    // F2 breakdowns (30d window from raw): searches, interactions, tech, geo, referrers.
    const [searches, inters, devices, countries, referrers, errs, wallets] = await Promise.all([
        p.query(`SELECT meta->>'q' q, COUNT(*) c FROM sm.site_events WHERE type='search' AND meta->>'q' IS NOT NULL AND ts >= now() - interval '30 days' GROUP BY 1 ORDER BY 2 DESC LIMIT 15`),
        p.query(`SELECT meta->>'name' name, COUNT(*) c FROM sm.site_events WHERE type='interaction' AND meta->>'name' IS NOT NULL AND ts >= now() - interval '30 days' GROUP BY 1 ORDER BY 2 DESC LIMIT 15`),
        p.query(`SELECT COALESCE(device,'unknown') d, COUNT(*) c FROM sm.site_events WHERE type='pageview' AND ts >= now() - interval '30 days' GROUP BY 1 ORDER BY 2 DESC`),
        p.query(`SELECT COALESCE(country,'??') k, COUNT(*) c FROM sm.site_events WHERE type='pageview' AND ts >= now() - interval '30 days' GROUP BY 1 ORDER BY 2 DESC LIMIT 15`),
        p.query(`SELECT COALESCE(NULLIF(referrer,''),'direct') r, COUNT(*) c FROM sm.site_events WHERE type='pageview' AND ts >= now() - interval '30 days' GROUP BY 1 ORDER BY 2 DESC LIMIT 12`),
        p.query(`SELECT COUNT(*) c FROM sm.site_events WHERE type='error' AND ts >= now() - interval '7 days'`),
        p.query(`SELECT meta->>'addr' addr, COUNT(*) c FROM sm.site_events WHERE type='interaction' AND meta->>'name'='wallet_view' AND meta->>'addr' IS NOT NULL AND ts >= now() - interval '30 days' GROUP BY 1 ORDER BY 2 DESC LIMIT 12`),
    ]);

    return {
        visits:   { '24h': n(w24.rows[0].v), '7d': n(w7.rows[0].v), '30d': n(w30.rows[0].v), '1y': n(year.rows[0].v) + n(today.rows[0].v) },
        uniques:  { '24h': n(w24.rows[0].u), '7d': n(w7.rows[0].u), '30d': n(w30.rows[0].u), '1y': n(year.rows[0].u) + n(today.rows[0].u) },
        sessions: { '24h': n(w24.rows[0].s), '7d': n(w7.rows[0].s), '30d': n(w30.rows[0].s), '1y': n(year.rows[0].s) + n(today.rows[0].s) },
        sections: secs.rows.map(r => ({ section: r.section, views: n(r.views), uniques: n(r.uniques) })),
        avgSessionMs: n(avg.rows[0].a),
        searches:     searches.rows.map(r => ({ q: r.q, count: n(r.c) })),
        interactions: inters.rows.map(r => ({ name: r.name, count: n(r.c) })),
        devices:      devices.rows.map(r => ({ device: r.d, count: n(r.c) })),
        countries:    countries.rows.map(r => ({ country: r.k, count: n(r.c) })),
        referrers:    referrers.rows.map(r => ({ ref: r.r, count: n(r.c) })),
        topWallets:   wallets.rows.map(r => ({ addr: r.addr, count: n(r.c) })),
        errors7d:     n(errs.rows[0].c),
        generatedAt:  Date.now(),
    };
}

// XOR native asset id (SORA v2) — for the traffic↔price correlation.
const XOR_ASSET = '0x0200000000000000000000000000000000000000000000000000000000000000';

// F3 advanced analytics (30d window). Separate endpoint so the main /stats
// payload stays light. Navigation flow, hour/day heatmap, bounce/depth, and
// site traffic vs XOR price per day.
async function getSiteAdvanced() {
    const p = getPool();
    const n = (x) => Number(x || 0);

    const [flow, heat, eng, priceTraffic, vitals] = await Promise.all([
        // Section→section transitions (consecutive section-views per session).
        p.query(`
            SELECT prev || ' → ' || section AS transition, COUNT(*) c
            FROM (SELECT section, LAG(section) OVER (PARTITION BY session_id ORDER BY ts) prev
                  FROM sm.site_events
                  WHERE type='section' AND section IS NOT NULL AND session_id IS NOT NULL
                    AND ts >= now() - interval '30 days') x
            WHERE prev IS NOT NULL AND prev <> section
            GROUP BY 1 ORDER BY 2 DESC LIMIT 15`),
        // Activity heatmap by day-of-week (0=Sun) × hour-of-day (UTC).
        p.query(`
            SELECT EXTRACT(DOW FROM ts)::int dow, EXTRACT(HOUR FROM ts)::int hr, COUNT(*) c
            FROM sm.site_events
            WHERE type IN ('pageview','section') AND ts >= now() - interval '30 days'
            GROUP BY 1, 2`),
        // Bounce rate (sessions with ≤1 section view) + avg depth (sections/session).
        p.query(`
            SELECT COALESCE(AVG(cnt), 0)::numeric(10,2) depth,
                   COALESCE(COUNT(*) FILTER (WHERE cnt <= 1)::float8 / NULLIF(COUNT(*), 0), 0) bounce,
                   COUNT(*) sessions
            FROM (SELECT session_id, COUNT(*) FILTER (WHERE type='section') cnt
                  FROM sm.site_events
                  WHERE session_id IS NOT NULL AND ts >= now() - interval '30 days'
                  GROUP BY session_id) s`),
        // Daily site visits vs XOR price (last 30d) for the correlation chart.
        p.query(`
            SELECT g::date dt, COALESCE(v.visits,0) visits, x.price
            FROM generate_series(date_trunc('day', now()) - interval '29 days', date_trunc('day', now()), interval '1 day') g
            LEFT JOIN (SELECT ts::date vd, COUNT(*) FILTER (WHERE type='pageview') visits
                       FROM sm.site_events WHERE ts >= now() - interval '30 days' GROUP BY 1) v ON v.vd = g::date
            LEFT JOIN (SELECT to_timestamp(hour_bucket)::date xd, AVG(price_usd) price
                       FROM sm.price_history
                       WHERE asset_id = $1 AND hour_bucket >= EXTRACT(EPOCH FROM now() - interval '30 days')
                       GROUP BY 1) x ON x.xd = g::date
            ORDER BY dt`, [XOR_ASSET]),
        // Web Vitals medians (from 'vitals' beacons): TTFB / FCP / LCP.
        p.query(`
            SELECT
              PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY (meta->>'ttfb')::float8) ttfb,
              PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY (meta->>'fcp')::float8)  fcp,
              PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY (meta->>'lcp')::float8)  lcp,
              COUNT(*) n
            FROM sm.site_events
            WHERE type='vitals' AND ts >= now() - interval '7 days' AND meta ? 'lcp'`),
    ]);

    return {
        navFlow: flow.rows.map(r => ({ transition: r.transition, count: n(r.c) })),
        heatmap: heat.rows.map(r => ({ dow: n(r.dow), hr: n(r.hr), count: n(r.c) })),
        bounce: Number(eng.rows[0].bounce || 0),
        depth: Number(eng.rows[0].depth || 0),
        engagedSessions: n(eng.rows[0].sessions),
        priceTraffic: priceTraffic.rows.map(r => ({ day: r.dt, visits: n(r.visits), price: r.price != null ? Number(r.price) : null })),
        vitals: { ttfb: vitals.rows[0].ttfb != null ? Math.round(vitals.rows[0].ttfb) : null, fcp: vitals.rows[0].fcp != null ? Math.round(vitals.rows[0].fcp) : null, lcp: vitals.rows[0].lcp != null ? Math.round(vitals.rows[0].lcp) : null, samples: n(vitals.rows[0].n) },
        generatedAt: Date.now(),
    };
}

module.exports = {
    getPool, initSchema, visitorHash,
    enqueue, flush, startFlushLoop, rollupAndPrune, getSiteStats, getSiteAdvanced,
    RAW_RETENTION_DAYS,
};
