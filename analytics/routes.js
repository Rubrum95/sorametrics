'use strict';
// analytics/routes.js — Express router for site meta-analytics.
//   POST /hit   : non-blocking beacon ingestion (responds 204, enqueues in RAM)
//   GET  /stats : cached dashboard payload (visits/uniques/sessions/sections + online)
// Mounted in index.js with: app.use('/analytics', require('./analytics/routes')).
// → endpoints become POST /analytics/hit and GET /analytics/stats.
// Relies on the global express.json() parser already in index.js, so the
// client must send the beacon as an application/json Blob.

const express = require('express');
const db = require('./db');
const presence = require('./presence');

const router = express.Router();

const STATS_TTL_MS = 60000;
let statsCache = { at: 0, payload: null };
let advCache = { at: 0, payload: null };

const ALLOWED_TYPES = new Set(['pageview', 'section', 'search', 'interaction', 'error', 'vitals', 'heartbeat']);
const MAX_META_BYTES = 2048;

// Real client IP behind Cloudflare. NEVER stored — only fed to visitorHash.
function clientIp(req) {
    return req.headers['cf-connecting-ip']
        || (req.headers['x-forwarded-for'] || '').split(',')[0].trim()
        || (req.socket && req.socket.remoteAddress)
        || '';
}

// Coarse device/browser/os from UA — aggregate only, no fingerprinting.
function parseUA(ua) {
    ua = ua || '';
    const tablet = /iPad|Tablet|PlayBook|Silk|Android(?!.*Mobile)/i.test(ua);
    const mobile = /Mobi|iPhone|iPod|Android.*Mobile|Windows Phone|webOS|BlackBerry/i.test(ua);
    const device = tablet ? 'tablet' : mobile ? 'mobile' : 'desktop';
    let browser = 'other';
    if (/Edg\//.test(ua)) browser = 'Edge';
    else if (/OPR\/|Opera/.test(ua)) browser = 'Opera';
    else if (/Chrome\//.test(ua)) browser = 'Chrome';
    else if (/Firefox\//.test(ua)) browser = 'Firefox';
    else if (/Safari\//.test(ua)) browser = 'Safari';
    let os = 'other';
    if (/Windows/.test(ua)) os = 'Windows';
    else if (/Mac OS X|Macintosh/.test(ua)) os = 'macOS';
    else if (/Android/.test(ua)) os = 'Android';
    else if (/iPhone|iPad|iOS/.test(ua)) os = 'iOS';
    else if (/Linux/.test(ua)) os = 'Linux';
    return { device, browser, os };
}

// Referrer → bare hostname. Internal (self) navigation collapses to direct
// (null), so the SPA's own URLs don't pollute the referrers panel.
function refHost(ref) {
    if (!ref || typeof ref !== 'string') return null;
    try {
        const h = new URL(ref).hostname.replace(/^www\./, '');
        if (!h || h.endsWith('sorametrics.org')) return null;
        return h.slice(0, 120);
    } catch (_) { return null; }
}

router.post('/hit', (req, res) => {
    // Respond first — never await the DB on the request path.
    res.status(204).end();
    try {
        const b = req.body || {};
        const type = String(b.type || '');
        if (!ALLOWED_TYPES.has(type)) return;

        const ua = req.headers['user-agent'] || '';
        const sid = typeof b.sid === 'string' ? b.sid.slice(0, 64) : null;
        const visitor = db.visitorHash(clientIp(req), ua);

        // Any beacon with a session id refreshes presence (online + session span).
        if (sid) presence.touch(sid, visitor);

        // Heartbeats only drive presence — never persisted (would be ~1 row/30s).
        if (type === 'heartbeat') return;

        let meta = b.meta;
        if (meta && JSON.stringify(meta).length > MAX_META_BYTES) meta = { truncated: true };

        const { device, browser, os } = parseUA(ua);
        // Enrich pageviews (no client meta) with browser/os for tech breakdown.
        if (type === 'pageview') meta = Object.assign({ browser, os }, meta || {});

        db.enqueue({
            type,
            section:    typeof b.section === 'string' ? b.section.slice(0, 64) : null,
            visitor,
            session_id: sid,
            path:       typeof b.path === 'string' ? b.path.slice(0, 256) : null,
            referrer:   refHost(b.ref),
            country:    req.headers['cf-ipcountry'] || null,
            device,
            meta:       meta || null,
        });
    } catch (_) { /* tracking must never break a request */ }
});

// online is O(1) and always fresh; the heavier aggregates are cached TTL 60s.
router.get('/stats', async (req, res) => {
    try {
        const now = Date.now();
        if (!statsCache.payload || now - statsCache.at >= STATS_TTL_MS) {
            statsCache = { at: now, payload: await db.getSiteStats() };
        }
        res.json({ ...statsCache.payload, online: presence.getOnline(), peak: presence.getPeak(), cached: now - statsCache.at > 0 });
    } catch (e) {
        res.status(500).json({ error: e.message });
    }
});

// F3 advanced analytics (navigation flow, heatmap, bounce/depth, price↔traffic,
// web vitals). Heavier query set, cached TTL 60s.
router.get('/advanced', async (req, res) => {
    try {
        const now = Date.now();
        if (!advCache.payload || now - advCache.at >= STATS_TTL_MS) {
            advCache = { at: now, payload: await db.getSiteAdvanced() };
        }
        res.json({ ...advCache.payload, cached: now - advCache.at > 0 });
    } catch (e) {
        res.status(500).json({ error: e.message });
    }
});

module.exports = router;
