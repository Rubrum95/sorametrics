const express = require('express');
const http = require('http');
const fs = require('fs');
const { Server } = require('socket.io');
const cors = require('cors');
const https = require('https');
const BigNumber = require('bignumber.js');
const { initApi, getActiveEndpoint } = require('./blockchain');
const { initDB, getAssetIdToInfo, insertTransfer, getTransfers, getLatestTransfers, insertSwap, getSwaps, getLatestSwaps, getCandles, getPriceChange, getSparkline, getTotalStats, insertBridge, getFilteredStats, insertFee, fixFeeDenomFactor, getFeeStats, getFeeStatsMainOnly, getFeeTrend, getWalletBridges, getLatestBridges, getLpVolume, insertLiquidityEvent, getTransferVolume, getPoolActivity, getNetworkTrend, getTopAccumulators, getNetworkStats, getMarketTrends, getTopTokens, getStablecoinStats, getLiquidityEvents, insertExtrinsic, getLatestExtrinsics, getExtrinsicSections, getExtrinsicsByAddress, getExtrinsicDetail, insertOrderBookEvent, getLatestOrderBookEvents, getOrderBookByAddress, upsertIdentityBatch, getIdentities, getAllCachedIdentities, insertSupplySnapshot, getSupplyHistory, getLatestSupplySnapshot, getBurnStats, getBurnStatsFromChain, getSupplySnapshotDelta, purgeSupplySnapshotsForSymbol, lookupExtrinsicUsdValue, globalSearch, getSwapVolumeUsd, getWalletInfo, getExportData, updatePriceHistory, initFeeBurnsLiveSchema, insertFeeBurnRow, getFeeBurnsWindow, getFeeBurnsSeries, initPolkamarktSchema, pmInsertMarket, pmUpdateMarketStatus, pmReconcileMarketStatus, pmInsertTrade, pmInsertClaim, pmInsertBuyback, pmGetBuybackStats, pmListBuybacks, pmInsertBurn, pmGetBurnStats, pmGetTotals, pmGetMarkets, pmGetMarketDetail, pmGetUserPositions, initNewsSchema, newsInsertEpisode, newsListEpisodes, initValStakingRewardsSchema, insertValStakingReward, getValStakingNetworkTotals, getValStakingPerValidator, getValStakingTopDestinations, getValStakingForDestination, getClaimedValStakingPairs, getValXorRateWindows, getPriceSeries, getExtrinsicStats24h, getFeesByBlocks } = require('./db_pg');
const { startFeeBurnsIndexer } = require("./fee_burns_indexer");
const { Pool: _PgPool } = require("pg");
const { ApiPromise: _ApiPromise, WsProvider: _WsProvider } = require("@polkadot/api");
const { options: _soraOptions } = require("@sora-substrate/api");
const { blake2AsHex } = require("@polkadot/util-crypto");
// ... (imports)




const { WS_ENDPOINT, WHITELIST_URL, PORT, CORS_ORIGINS } = require('./config');
// eth_helper.js - DESACTIVADO temporalmente por memory leak
// const { resolveEthSender } = require('./eth_helper');
function resolveEthSender() { return Promise.resolve(null); }

// Helper: queries con timeout (5 segundos) para evitar memory leak
function withTimeout(promise, ms = 5000) {
    let timeoutId;
    const timeoutPromise = new Promise((_, reject) => {
        timeoutId = setTimeout(() => reject(new Error('Query timeout')), ms);
    });
    
    return Promise.race([promise, timeoutPromise]).finally(() => {
        clearTimeout(timeoutId);
    });
}

// Serialize extrinsic events to condensed JSON for storage
function serializeEvents(extrinsicEvents, maxSize = 8192) {
    try {
        const events = [];
        for (const record of extrinsicEvents) {
            try {
                const { event } = record;
                // Skip system success/failed — already tracked via success field
                if (event.section === 'system' && (event.method === 'ExtrinsicSuccess' || event.method === 'ExtrinsicFailed')) continue;
                events.push({
                    s: event.section,
                    m: event.method,
                    d: event.data ? event.data.toHuman() : null
                });
            } catch (e) { /* skip malformed event */ }
        }
        if (events.length === 0) return '[]';
        const json = JSON.stringify(events);
        if (json.length <= maxSize) return json;
        // Fallback: strip data to fit
        const slim = events.map(e => ({ s: e.s, m: e.m }));
        const slimJson = JSON.stringify(slim);
        return slimJson.length <= maxSize ? slimJson : null;
    } catch (e) {
        return null;
    }
}

const app = express();

// --- SECURITY HEADERS ---
const helmet = require('helmet');
// v6 frontend loads React + ReactDOM + Babel standalone + Socket.IO from
// CDNs (unpkg, cdn.socket.io, cloudflareinsights), plus Google Fonts for
// Inter/JetBrains Mono. Whitelist those explicitly so the CSP doesn't
// block them. Without this entire dashboard renders blank (React never
// boots, stylesheet MIME is rejected).
app.use(helmet({
    contentSecurityPolicy: {
        directives: {
            defaultSrc: ["'self'"],
            scriptSrc: [
                "'self'", "'unsafe-inline'",
                "https://cdnjs.cloudflare.com", "https://cdn.jsdelivr.net",
                "https://www.googletagmanager.com", "https://www.google-analytics.com", "https://googletagmanager.com",
                "https://unpkg.com", "https://cdn.socket.io",
                "https://static.cloudflareinsights.com",
            ],
            scriptSrcElem: [
                "'self'", "'unsafe-inline'",
                "https://cdnjs.cloudflare.com", "https://cdn.jsdelivr.net",
                "https://www.googletagmanager.com", "https://www.google-analytics.com", "https://googletagmanager.com",
                "https://unpkg.com", "https://cdn.socket.io",
                "https://static.cloudflareinsights.com",
            ],
            styleSrc: ["'self'", "'unsafe-inline'", "https://fonts.googleapis.com"],
            styleSrcElem: ["'self'", "'unsafe-inline'", "https://fonts.googleapis.com"],
            imgSrc: ["'self'", "data:", "https://raw.githubusercontent.com", "https://avatars.githubusercontent.com", "https://www.googletagmanager.com", "https://www.google-analytics.com"],
            // Include CDN origins so source-map fetches (unpkg, cdn.socket.io,
            // cdn.jsdelivr.net) don't spam the console with CSP errors.
            connectSrc: [
                "'self'", "wss:", "ws:",
                "https://www.google-analytics.com", "https://analytics.google.com", "https://www.googletagmanager.com",
                "https://unpkg.com", "https://cdn.socket.io", "https://cdn.jsdelivr.net",
            ],
            fontSrc: ["'self'", "https://fonts.gstatic.com", "data:"],
            scriptSrcAttr: ["'unsafe-inline'"],
            upgradeInsecureRequests: null,
        }
    },
    hsts: false,
}));

// --- COMPRESSION ---
const compression = require('compression');
app.use(compression());

// --- CORS: Restringir orígenes (permite mismo origen + dev localhost) ---
const ALLOWED_ORIGINS = CORS_ORIGINS.split(',').filter(Boolean);
if (ALLOWED_ORIGINS.length === 0) console.warn('⚠️ CORS_ORIGINS not set — allowing all origins (dev mode)');
app.use(cors({
    origin: (origin, cb) => {
        // Mismo origen (sin header Origin) o orígenes permitidos
        if (!origin || ALLOWED_ORIGINS.length === 0 || ALLOWED_ORIGINS.includes(origin)) return cb(null, true);
        cb(null, false);
    }
}));
app.use(express.json({ limit: '1mb' }));

// --- STATIC FILES: Solo servir archivos frontend, no código backend ---
// v6 frontend adds /styles.css and /js/*.jsx files on top of the legacy
// /script.js. Without them whitelisted express falls through to the 404
// handler and the browser gets text/html for an asset it expected as CSS
// or JS, tanking the whole render.
const path = require('path');
// Note: '/' is INTENTIONALLY NOT in this set. Express.static would map '/' →
// /index.html (default index lookup) and shadow our `app.get('/')` route that
// serves landing.html. Same reason '/sorav2' and '/minamoto' aren't here —
// each goes through its own app.get(...) handler below.
const ALLOWED_STATIC = new Set([
    '/index.html', '/script.js', '/sw.js', '/manifest.json', '/favicon.svg', '/header-banner.jpg',
    '/styles.css',
    // Network landing + Minamoto SPA shell (served as files when requested by name,
    // but the clean routes /, /sorav2, /minamoto are handled by app.get below).
    '/landing.html', '/minamoto.html',
]);
app.use((req, res, next) => {
    // Must accept HEAD as well as GET — Cloudflare issues HEAD requests to
    // check cache freshness before GET, and if HEAD 404s CF caches the
    // 404 and serves it for subsequent GETs. That's what dropped the
    // stylesheet and the JSX modules ("MIME text/html" on the browser).
    if (req.method !== 'GET' && req.method !== 'HEAD') return next();
    const p = req.path;
    const isStatic = ALLOWED_STATIC.has(p)
        || (p.startsWith('/music/') && !p.includes('..'))
        || (p.startsWith('/news/media/') && !p.includes('..'))
        // v6 JSX modules (e.g. /js/intelligence.jsx). Tight regex to prevent
        // traversal and only allow leaf .jsx files under /js/.
        || /^\/js\/[a-zA-Z0-9_.-]+\.jsx$/.test(p)
        // Minamoto JSX modules (/js/minamoto/<name>.jsx). Same anti-traversal regex.
        || /^\/js\/minamoto\/[a-zA-Z0-9_.-]+\.jsx$/.test(p);
    if (isStatic) return express.static(__dirname)(req, res, next);
    next();
});

// --- SITE ANALYTICS (meta-analytics: visits/sections/sessions, sm.site_*) ---
try {
    app.use('/analytics', require('./analytics/routes'));
    console.log('✅ /analytics router mounted');
} catch (e) {
    console.error('analytics router mount failed:', e.message);
}

// --- MINAMOTO API ROUTER (mn schema, isolated from sm.* / SORA v2) ---
try {
    const _mnRouter = require('./minamoto/routes');
    app.use('/api/minamoto', _mnRouter);
    // Alias so the v2 SPA can hit the SAME data without crossing namespaces in
    // its head. Both networks share the migration registry — the data is
    // identical, only the UI orientation differs.
    app.use('/api/sorav2/xor-migration', _mnRouter);
    console.log('✅ /api/minamoto router mounted');
} catch (e) {
    console.error('⚠️ Minamoto router failed to mount:', e.message);
}

// --- MUSIC PLAYLIST ENDPOINT ---
app.get('/music/list', (req, res) => {
    const musicDir = path.join(__dirname, 'music');
    try {
        // Canonical metadata (title/cover/artist/duration per audio file)
        // shipped alongside the audio as manifest.json. Optional: files absent
        // from it fall back to a filename-derived title and a stem-matched
        // cover, so dropping a bare .mp3 in still works.
        let manifest = {};
        try { manifest = JSON.parse(fs.readFileSync(path.join(musicDir, 'manifest.json'), 'utf8')); } catch (_) {}
        const all = fs.readdirSync(musicDir);
        const covers = new Set(all.filter(f => /\.(webp|png|jpe?g)$/i.test(f)));
        const stemCover = (stem) => ['.webp', '.png', '.jpg', '.jpeg']
            .map(e => stem + e).find(c => covers.has(c)) || null;
        const playlist = all
            .filter(f => f.toLowerCase().endsWith('.mp3'))
            .sort()
            .map(f => {
                const meta = manifest[f] || {};
                const stem = f.replace(/\.mp3$/i, '');
                // Fallback title: strip a leading "NN_" track-number prefix.
                const fallbackTitle = stem.replace(/^\d{2}_/, '').replace(/_/g, ' ').trim();
                const cover = (meta.cover && covers.has(meta.cover)) ? meta.cover : stemCover(stem);
                return {
                    title: meta.title || fallbackTitle,
                    artist: meta.artist || 'SoraMetrics Radio',
                    src: '/music/' + encodeURIComponent(f),
                    cover: cover ? '/music/' + encodeURIComponent(cover) : null,
                    dur: Number.isFinite(meta.dur) ? meta.dur : null,
                };
            });
        res.json(playlist);
    } catch (e) {
        res.json([]);
    }
});

// --- RATE LIMITER simple (sin dependencias) ---
const rateLimitMap = new Map();
function rateLimit(maxReqs = 30, windowMs = 60000) {
    return (req, res, next) => {
        const ip = req.ip || req.connection.remoteAddress;
        const route = req.route ? req.route.path : req.path;
        const key = `${ip}:${route}`;
        const now = Date.now();
        let entry = rateLimitMap.get(key);
        if (!entry || now - entry.start > windowMs) {
            entry = { start: now, count: 1 };
            rateLimitMap.set(key, entry);
        } else {
            entry.count++;
        }
        if (entry.count > maxReqs) {
            return res.status(429).json({ error: 'Too many requests' });
        }
        next();
    };
}
// Limpiar entradas antiguas cada 5 minutos
setInterval(() => {
    const now = Date.now();
    for (const [key, entry] of rateLimitMap) {
        if (now - entry.start > 120000) rateLimitMap.delete(key);
    }
}, 60000);

// --- INPUT VALIDATION ---
const VALID_SS58 = /^[1-9A-HJ-NP-Za-km-z]{46,50}$/;
const VALID_ASSET_ID = /^0x[0-9a-fA-F]{64}$/;
const VALID_SYMBOL = /^[A-Za-z0-9]{1,12}$/;

function validateAddress(req, res, next) {
    if (!VALID_SS58.test(req.params.address)) return res.status(400).json({ error: 'Invalid address format' });
    next();
}
function validateAssetId(req, res, next) {
    if (!VALID_ASSET_ID.test(req.params.assetId)) return res.status(400).json({ error: 'Invalid asset ID format' });
    next();
}
function validateSymbol(req, res, next) {
    if (!VALID_SYMBOL.test(req.params.symbol)) return res.status(400).json({ error: 'Invalid symbol format' });
    next();
}

// --- TIMEFRAME MAP compartido (evita duplicación) ---
const TIMEFRAME_MS = {
    '1h': 3600000, '4h': 14400000, '24h': 86400000, '1d': 86400000,
    '7d': 604800000, '30d': 2592000000, '1m': 2592000000, '1y': 31536000000, 'all': 0
};

// Helper: queries con timeout para evitar memory leak
// Forzar carga de favicon
app.get('/favicon.svg', rateLimit(60, 60000), (req, res) => res.sendFile(__dirname + '/favicon.svg'));

// --- VERSION CHECK ENDPOINT (FUERZA ACTUALIZACION EN iOS PWA) ---
const SERVER_VERSION = 'v4.0';
app.get('/api/version', rateLimit(30, 60000), (req, res) => {
    res.setHeader('Cache-Control', 'no-store, no-cache, must-revalidate');
    res.json({ version: SERVER_VERSION });
});

// --- HEALTH ENDPOINT ---
app.get('/health', rateLimit(30, 60000), (req, res) => {
    res.json({
        status: 'ok',
        uptime: Math.floor(process.uptime()),
        wsConnected: !!(api && api.isConnected),
        timestamp: Date.now()
    });
});

// --- RPC SOURCE ENDPOINT ---
// Tells the frontend which WS endpoint is currently servicing the indexer.
// The pill in the sidebar reads this to show "Sorametrics node" when we're
// on the local container or "Fallback: <host>" when WsProvider has rotated
// to a public node. Loopback (127.0.0.1) is treated as the local case.
app.get('/health/rpc-source', rateLimit(60, 60000), (req, res) => {
    const { WS_ENDPOINTS } = require('./config');
    const active = getActiveEndpoint();
    let label = 'unknown';
    let isPrimary = false;
    let isLocal = false;
    if (active) {
        isPrimary = WS_ENDPOINTS && active === WS_ENDPOINTS[0];
        isLocal = /^ws:\/\/(127\.0\.0\.1|localhost)/.test(active);
        if (isLocal) label = 'sorametrics';
        else {
            // Strip protocol + path for a friendly "host" label.
            const host = active.replace(/^wss?:\/\//, '').replace(/\/.*$/, '');
            label = host;
        }
    }
    res.json({
        active,                      // full URL, e.g. "ws://127.0.0.1:9944"
        label,                        // friendly: "sorametrics" | "ws.mof.sora.org" | …
        isPrimary,                    // true when we're on endpoint[0]
        isLocal,                      // true when host is loopback
        connected: !!(api && api.isConnected),
        endpoints: WS_ENDPOINTS,
    });
});

// --- IMAGE PROXY CON RATE LIMITING (SISTEMA ANTI-CRASH) ---
const imageCache = new Map();
const downloadQueue = [];
let activeDownloads = 0;
const MAX_CONCURRENT_DOWNLOADS = 2; // Límite ULTRA estricto de 2 sockets (Para máxima seguridad)
const PLACEHOLDER_GIF = Buffer.from('R0lGODlhAQABAIAAAAAAAP///ywAAAAAAQABAAACAUwAOw==', 'base64');
function sendPlaceholder(res) {
    if (res.headersSent) return;
    res.setHeader('Content-Type', 'image/gif');
    res.setHeader('Cache-Control', 'public, max-age=86400');
    res.send(PLACEHOLDER_GIF);
}

function processQueue() {
    if (downloadQueue.length === 0 || activeDownloads >= MAX_CONCURRENT_DOWNLOADS) return;

    const item = downloadQueue.shift();
    const { targetUrl, res } = item;

    activeDownloads++;

    const client = targetUrl.startsWith('https') ? require('https') : require('http');

    const request = client.get(targetUrl, { timeout: 5000 }, (proxyRes) => {
        if (proxyRes.statusCode !== 200) {
            proxyRes.resume();
            finishDownload();
            if (!res.headersSent) sendPlaceholder(res);
            return;
        }

        const chunks = [];
        proxyRes.on('data', c => chunks.push(c));
        proxyRes.on('end', () => {
            const buffer = Buffer.concat(chunks);
            const contentType = proxyRes.headers['content-type'] || 'image/png';

            // LRU: eliminar las 500 entradas más antiguas en vez de borrar todo
            if (imageCache.size > 2000) {
                const keys = [...imageCache.keys()].slice(0, 500);
                keys.forEach(k => imageCache.delete(k));
            }
            imageCache.set(targetUrl, { buffer, contentType });

            if (!res.headersSent) {
                res.setHeader('Content-Type', contentType);
                res.setHeader('Cache-Control', 'public, max-age=86400');
                res.send(buffer);
            }
            finishDownload();
        });
    });

    request.on('error', (e) => {
        if (!res.headersSent) sendPlaceholder(res);
        finishDownload();
    });

    request.on('timeout', () => {
        request.destroy();
        if (!res.headersSent) sendPlaceholder(res);
        finishDownload();
    });
}

function finishDownload() {
    activeDownloads--;
    if (activeDownloads < 0) activeDownloads = 0;
    processQueue();
}

app.get('/proxy-image', rateLimit(30, 60000), (req, res) => {
    const targetUrl = req.query.url;
    if (!targetUrl) return res.status(400).send('No URL');

    let u;
    try { u = new URL(targetUrl); } catch (e) { return res.status(400).send('Bad URL'); }

    // Solo http/https
    if (!['http:', 'https:'].includes(u.protocol)) return res.status(400).send('Bad protocol');

    // Bloqueo anti-SSRF (localhost, redes privadas, IPv6, brackets)
    const host = (u.hostname || '').toLowerCase();
    const isPrivate =
        host === 'localhost' ||
        host.endsWith('.local') ||
        host === '0.0.0.0' ||
        host.startsWith('127.') ||
        host.startsWith('10.') ||
        host.startsWith('192.168.') ||
        /^172\.(1[6-9]|2\d|3[0-1])\./.test(host) ||
        host === '169.254.169.254' ||
        host.startsWith('[') ||          // IPv6 brackets
        host === '::1' ||                // IPv6 loopback
        host.startsWith('::ffff:') ||    // IPv6-mapped IPv4
        host.startsWith('fd') ||         // IPv6 private
        host.startsWith('fc') ||         // IPv6 private
        host.startsWith('fe80');         // IPv6 link-local

    if (isPrivate) return sendPlaceholder(res);

    // Solo permitir dominios de logos conocidos (sora-xor, github)
    const allowedHosts = ['raw.githubusercontent.com', 'github.com', 'avatars.githubusercontent.com'];
    if (!allowedHosts.some(h => host === h || host.endsWith('.' + h))) {
        return sendPlaceholder(res);
    }

    const normalized = u.href;

    if (imageCache.has(normalized)) {
        const cached = imageCache.get(normalized);
        res.setHeader('Content-Type', cached.contentType);
        res.setHeader('Cache-Control', 'public, max-age=86400');
        return res.send(cached.buffer);
    }

    // Si hay demasiadas imágenes pendientes, respondemos placeholder para no "tumbar" el servidor/navegador
    if (downloadQueue.length > 500) return sendPlaceholder(res);

    downloadQueue.push({ targetUrl: normalized, res });
    processQueue();
});


const server = http.createServer(app);
const io = new Server(server);

// --- CACHÉ INTELIGENTE ---
const CACHE_TTL = 30 * 1000; // 30s para tokens
let globalTokenCache = { timestamp: 0, data: null };

let api = null;
let ASSETS = [];
let tokenPrices = {};
let holdersCache = {};

// Live pipeline state — populated by storage subscribers + 30s ring buffer.
// Endpoint /staking/rewards/live reads from this in-memory state (zero node load).
const pipelineState = {
    xorToVal: '0',
    xorToBuyBack: '0',
    valStakingEraReward: null,   // { era, value } or null pre-4.8.6
    valBucketPrevEra: null,      // { era, value } — previous era's final bucket (context)
    unassignedValStakingReward: '0',
    activeEra: null,
    bestBlock: null,
    lastUpdate: 0,
    history: [],                  // [{ts, xorToVal, valStakingEraReward}]  capped at 60 samples (30 min @ 30s)
};
let pipelineUnsubs = [];
let pipelineSnapshotTimer = null;
let pipelineEraSubKey = null;     // remember which era we subscribed to, to re-sub on era change

async function startPipelineSubscriber() {
    if (!api?.query?.xorFee) {
        console.warn('[pipeline] xorFee pallet not present — subscriber skipped');
        return;
    }
    // Clean previous subscriptions in case of reconnect.
    pipelineUnsubs.forEach(u => { try { u(); } catch {} });
    pipelineUnsubs = [];
    if (pipelineSnapshotTimer) { clearInterval(pipelineSnapshotTimer); pipelineSnapshotTimer = null; }

    // 1. xorToVal — always present.
    try {
        const u = await api.query.xorFee.xorToVal((v) => {
            pipelineState.xorToVal = v.toString();
            pipelineState.lastUpdate = Date.now();
        });
        pipelineUnsubs.push(u);
    } catch (e) { console.error('[pipeline] xorToVal sub failed:', e.message); }

    // 2. xorToBuyBack — present in 4.8.x runtimes.
    try {
        const u = await api.query.xorFee.xorToBuyBack((v) => {
            pipelineState.xorToBuyBack = v.toString();
            pipelineState.lastUpdate = Date.now();
        });
        pipelineUnsubs.push(u);
    } catch (e) { console.error('[pipeline] xorToBuyBack sub failed:', e.message); }

    // 3. valStakingEraReward[activeEra] — only post-4.8.6.
    if (api.query.xorFee.valStakingEraReward) {
        await subscribeValBucketForCurrentEra();
    }

    // 4. unassignedValStakingReward — only post-4.8.6.
    if (api.query.xorFee.unassignedValStakingReward) {
        try {
            const u = await api.query.xorFee.unassignedValStakingReward((v) => {
                pipelineState.unassignedValStakingReward = v.toString();
                pipelineState.lastUpdate = Date.now();
            });
            pipelineUnsubs.push(u);
        } catch (e) { console.error('[pipeline] unassignedValStakingReward sub failed:', e.message); }
    }

    // 5. New-heads watcher — keep activeEra+bestBlock fresh, re-subscribe bucket when era changes.
    try {
        const u = await api.rpc.chain.subscribeNewHeads(async (header) => {
            pipelineState.bestBlock = header.number.toNumber();
            try {
                const ae = (await api.query.staking.activeEra()).unwrap().index.toNumber();
                if (ae !== pipelineState.activeEra) {
                    pipelineState.activeEra = ae;
                    if (api.query.xorFee.valStakingEraReward) await subscribeValBucketForCurrentEra();
                }
            } catch {}
        });
        pipelineUnsubs.push(u);
    } catch (e) { console.error('[pipeline] newHeads sub failed:', e.message); }

    // 6. Ring buffer snapshot every 30s (60 samples = 30 min).
    // Each sample is tagged with its era so the bucket sparkline can filter to the
    // active era only (mixing eras would draw an artificial drop at era rollover).
    pipelineSnapshotTimer = setInterval(() => {
        pipelineState.history.push({
            ts: Date.now(),
            xorToVal: pipelineState.xorToVal,
            valStakingEraReward: pipelineState.valStakingEraReward?.value || null,
            era: pipelineState.valStakingEraReward?.era ?? null,
        });
        if (pipelineState.history.length > 60) pipelineState.history.shift();
    }, 30_000);

    console.log('[pipeline] live subscribers active');
}

async function subscribeValBucketForCurrentEra() {
    try {
        const ae = (await api.query.staking.activeEra()).unwrap().index.toNumber();
        if (pipelineEraSubKey === ae) return; // already subscribed
        // Tear down previous era sub if any (it's the last element of pipelineUnsubs we tagged)
        // (Simpler approach: keep separate handle.)
        if (pipelineState._eraUnsub) { try { pipelineState._eraUnsub(); } catch {} }
        const u = await api.query.xorFee.valStakingEraReward(ae, (v) => {
            pipelineState.valStakingEraReward = { era: ae, value: v.toString() };
            pipelineState.lastUpdate = Date.now();
        });
        pipelineState._eraUnsub = u;
        pipelineEraSubKey = ae;
        // Reset sparkline history on era rollover so the chart only shows the
        // active era (no artificial 356→0 drop when the era changes).
        pipelineState.history = [];
        // Read the previous era's final bucket once (it's historical, no longer grows).
        if (ae > 0) {
            try {
                const prev = await api.query.xorFee.valStakingEraReward(ae - 1);
                pipelineState.valBucketPrevEra = { era: ae - 1, value: prev.toString() };
            } catch { pipelineState.valBucketPrevEra = null; }
        }
    } catch (e) { console.error('[pipeline] era bucket sub failed:', e.message); }
}
let currentDenomFactor = '1'; // XOR denomination factor (cumulative, queried on-chain)
const CACHE_DURATION = 5 * 60 * 1000;

// Caché para endpoints
// swapsCacheMap declared near the endpoint (per-key cache)
let transfersCache = { data: null, timestamp: 0 };
let tokensCache = { data: null, timestamp: 0 };
let poolsCache = { data: null, timestamp: 0 };
let providersCache = { data: null, timestamp: 0 };
let activityCache = { data: null, timestamp: 0 };

// LP & Staking caches to avoid scanning 432 pools on every request
let poolPropertiesCache = { data: null, timestamp: 0 }; // Global pool properties (same for all wallets)
const POOL_PROPS_TTL = 30 * 60 * 1000; // 30 min — pool list rarely changes (only via governance)
let liquidityCache = {}; // Per-address LP results: { address: { data, timestamp } }
let stakingCache = {};   // Per-address staking results: { address: { data, timestamp } }
let walletInfoCache = {}; // Per-address wallet info: { address: { data, timestamp } }
const LP_STAKING_TTL = 15 * 60 * 1000; // 15 min — LP positions only change when the user transacts

const SWAPS_TTL = 24 * 1000;    // 24s
const TRANSFERS_TTL = 60 * 1000; // 60s
const TOKENS_TTL = 30 * 1000;   // 30s

// Global staking section caches
let validatorsGlobalCache = { data: null, ts: 0 };
const VALIDATORS_TTL = 2 * 60 * 1000; // 2 min (validators change per-era only)
let networkStakingCache = { data: null, ts: 0 };
const NETWORK_STAKING_TTL = 30 * 1000; // 30s
const POOLS_TTL = 60 * 1000;    // 60s
const PROVIDERS_TTL = 90 * 1000; // 90s
const ACTIVITY_TTL = 90 * 1000;  // 90s



// --- PRICE CALCULATION (RESERVE BASED - Same as backfiller for consistency) ---
const XOR_ID = '0x0200000000000000000000000000000000000000000000000000000000000000';
const XSTUS_ID = '0x0200080000000000000000000000000000000000000000000000000000000000'; // XSTUSD
const DAI_ID = '0x0200060000000000000000000000000000000000000000000000000000000000';
const XST_ID = '0x0200090000000000000000000000000000000000000000000000000000000000'; // XST

// --- BURN TRACKER TOKEN CONFIG ---
const BURN_TOKENS = {
    XOR:   { symbol: 'XOR',   assetId: XOR_ID, decimals: 18 },
    VAL:   { symbol: 'VAL',   assetId: null, decimals: 18 },
    PSWAP: { symbol: 'PSWAP', assetId: null, decimals: 18 },
    TBCD:  { symbol: 'TBCD',  assetId: '0x02000a0000000000000000000000000000000000000000000000000000000000', decimals: 18 },
    KUSD:  { symbol: 'KUSD',  assetId: XSTUS_ID, decimals: 18 }
};

// Genesis circulating supply at SORA v2 mainnet launch (April 26, 2021).
// Used to compute "Total Burned" = genesisSupply - currentCirculating.
// VAL: 33.9M crowdloan + 66.1M market maker = ~100M initial distribution.
// PSWAP: 10B total supply minted at genesis.
// TBCD/KUSD: synthetic tokens (bonding curve / stablecoin), no fixed genesis.
const GENESIS_SUPPLY = {
    VAL:   { supply: 100_000_000,      timestamp: 1619395200000 }, // April 26, 2021
    PSWAP: { supply: 10_000_000_000,   timestamp: 1619395200000 },
};

function resolveBurnTokenIds() {
    for (const key of Object.keys(BURN_TOKENS)) {
        if (!BURN_TOKENS[key].assetId) {
            const found = ASSETS.find(a => a.symbol === key);
            if (found) {
                BURN_TOKENS[key].assetId = found.assetId;
                console.log(`🔥 Resolved ${key} assetId: ${found.assetId.substring(0, 10)}...`);
            }
        }
    }
}

let supplyCache = {};
const SUPPLY_CACHE_TTL = 60000; // 60s

// MOF (Ministry of Finance) API — canonical source for SORA circulating supply
// Used by llblab/sora-qty and the official SORA team.
// totalIssuance from chain is wrong for XOR/TBCD due to denomination event + TBC reserves.
const MOF_URLS = ['https://mof.sora.org', 'https://mof2.sora.org', 'https://mof3.sora.org'];
// MOF uses lowercase symbol; KUSD in our config maps to XSTUSD on MOF
const MOF_SYMBOL_MAP = { XOR: 'xor', VAL: 'val', PSWAP: 'pswap', TBCD: 'tbcd', KUSD: 'xstusd' };

// Dedicated MOF fetch — no cache, no on-chain fallback. Returns null on failure.
async function fetchMofSupply(symbol) {
    const mofSym = MOF_SYMBOL_MAP[symbol] || symbol.toLowerCase();
    for (const baseUrl of MOF_URLS) {
        try {
            const controller = new AbortController();
            const timeout = setTimeout(() => controller.abort(), 10000);
            const res = await fetch(`${baseUrl}/qty/${mofSym}`, { signal: controller.signal });
            clearTimeout(timeout);
            if (res.ok) {
                const text = await res.text();
                const val = parseFloat(text);
                if (!isNaN(val) && val > 0 && (symbol !== 'XOR' || val < 1e9)) return val;
            }
        } catch (e) { /* try next mirror */ }
    }
    return null;
}

async function getTokenTotalSupply(symbol) {
    const now = Date.now();
    if (supplyCache[symbol] && (now - supplyCache[symbol].ts < SUPPLY_CACHE_TTL)) {
        return supplyCache[symbol].value;
    }

    const token = BURN_TOKENS[symbol];
    if (!token) return null;

    try {
        let totalSupply = null;

        // XOR: 1 XOR = 1 XOR. Real on-chain total issuance (balances.totalIssuance / 1e18),
        // which matches the MOF figure. No CoinGecko, no denomination unpacking.
        if (symbol === 'XOR' && api && api.query.balances?.totalIssuance) {
            try {
                const issuance = await withTimeout(api.query.balances.totalIssuance());
                totalSupply = new BigNumber(issuance.toString().replace(/,/g, '')).div('1e18').toNumber();
            } catch (e) { /* on-chain query failed */ }
        }

        // For non-XOR (or XOR if CoinGecko failed): use MOF API
        if (!totalSupply) {
            totalSupply = await fetchMofSupply(symbol);
        }

        // Fallback for non-XOR: use latest MOF snapshot from DB (not on-chain totalIssuance
        // which includes locked/vesting and doesn't match circulating supply)
        if (!totalSupply && symbol !== 'XOR') {
            try {
                const snap = await getLatestSupplySnapshot(symbol);
                if (snap && snap.total_supply) totalSupply = snap.total_supply;
            } catch (e2) { /* DB fallback failed */ }
        }

        // Fallback for XOR: on-chain totalIssuance
        if (!totalSupply && symbol === 'XOR' && api && token.assetId) {
            try {
                const issuance = await withTimeout(api.query.balances.totalIssuance());
                const raw = issuance.toString().replace(/,/g, '');
                totalSupply = new BigNumber(raw).div(new BigNumber(10).pow(token.decimals)).toNumber();
            } catch (e2) { /* chain fallback failed */ }
        }

        if (totalSupply) {
            supplyCache[symbol] = { value: totalSupply, ts: now };
        }
        return totalSupply;
    } catch (e) {
        console.error(`Supply query error for ${symbol}:`, e.message);
        return supplyCache[symbol]?.value || null;
    }
}


// Query VAL remint percentage from on-chain timeSinceGenesis.
// SORA staking remints a declining percentage (90%→35% over 5 years) of burned VAL as staking rewards.
// Formula: remint% = max(35, 90 - 55 * elapsed_secs / 157680000)
// Source: sora-xor/substrate frame/staking/src/sora.rs — ValRewardCurve::current_reward_coefficient
let valRemintCache = { value: null, ts: 0 };
const VAL_REMINT_TTL = 3600000; // 1 hour

async function getValRemintPercentage() {
    if (valRemintCache.value !== null && Date.now() - valRemintCache.ts < VAL_REMINT_TTL) {
        return valRemintCache.value;
    }
    try {
        if (!api || !api.query.staking || !api.query.staking.timeSinceGenesis) {
            // Fallback: calculate from known genesis date (~April 2021)
            const genesisApprox = new Date('2021-04-20T00:00:00Z').getTime();
            const elapsedMs = Date.now() - genesisApprox;
            const elapsedSecs = Math.floor(elapsedMs / 1000);
            const fiveYears = 5 * 365 * 24 * 60 * 60;
            if (elapsedSecs >= fiveYears) return 35;
            const pct = 90 - (55 * elapsedSecs / fiveYears);
            valRemintCache = { value: Math.max(35, parseFloat(pct.toFixed(2))), ts: Date.now() };
            return valRemintCache.value;
        }
        const tsg = await withTimeout(api.query.staking.timeSinceGenesis());
        const elapsedSecs = BigInt(tsg.secs.toString());
        const FIVE_YEARS = BigInt(5 * 365 * 24 * 60 * 60); // 157680000
        let pct;
        if (elapsedSecs >= FIVE_YEARS) {
            pct = 35;
        } else {
            const num = 90n * (FIVE_YEARS - elapsedSecs) + 35n * elapsedSecs;
            pct = Number(num * 10000n / (100n * FIVE_YEARS)) / 100;
        }
        valRemintCache = { value: parseFloat(pct.toFixed(2)), ts: Date.now() };
        return valRemintCache.value;
    } catch (e) {
        console.error('VAL remint query error:', e.message);
        // Fallback estimate
        return 35.5;
    }
}

// --- XOR MARKET DATA (CoinGecko + Etherscan for Ethereum supply) ---
// XOR has undergone 6 denomination events (cumulative factor ~1e38).
// On-chain pool price ($5) × MOF supply (1e18) gives absurd market cap.
// Use CoinGecko for realistic XOR market data.
let xorMarketCache = { data: null, ts: 0 };
const XOR_MARKET_TTL = 300000; // 5 minutes

// Update cached denomination factor from on-chain (called at startup + every 1h)
async function updateDenomFactor() {
    try {
        if (api && api.query.denomination && api.query.denomination.denominator) {
            const d = await withTimeout(api.query.denomination.denominator());
            const val = d.toString().replace(/,/g, '');
            if (val && val !== '0') {
                currentDenomFactor = val;
                console.log(`📦 Denomination factor updated: 10^${val.length - 1}`);
            }
        }
    } catch (e) { /* denomination query failed, keep previous value */ }
}

const XOR_ETH_CONTRACT = '0x40fd72257597aa14c7231a7b1aaa29fce868f677';

async function fetchXorMarketData() {
    const now = Date.now();
    if (xorMarketCache.data && (now - xorMarketCache.ts < XOR_MARKET_TTL)) {
        return xorMarketCache.data;
    }
    const result = { cgPrice: null, cgMarketCap: null, cgSupply: null, ethSupply: null };

    // 1. CoinGecko — use full coins endpoint (returns price even for very low values)
    try {
        const controller = new AbortController();
        const timeout = setTimeout(() => controller.abort(), 10000);
        const res = await fetch(
            'https://api.coingecko.com/api/v3/coins/sora?localization=false&tickers=false&market_data=true&community_data=false&developer_data=false',
            { signal: controller.signal }
        );
        clearTimeout(timeout);
        if (res.ok) {
            const json = await res.json();
            const md = json.market_data;
            if (md) {
                result.cgPrice = (md.current_price?.usd != null) ? md.current_price.usd : null;
                result.cgMarketCap = md.market_cap?.usd || null;
                result.cgSupply = (md.circulating_supply != null) ? md.circulating_supply : null;
            }
        }
    } catch (e) { console.log('CoinGecko XOR fetch failed:', e.message); }

    // Fallback: simple price endpoint if full endpoint failed
    if (!result.cgMarketCap) {
        try {
            const controller = new AbortController();
            const timeout = setTimeout(() => controller.abort(), 8000);
            const res = await fetch(
                'https://api.coingecko.com/api/v3/simple/price?ids=sora&vs_currencies=usd&include_market_cap=true',
                { signal: controller.signal }
            );
            clearTimeout(timeout);
            if (res.ok) {
                const json = await res.json();
                if (json.sora) {
                    result.cgPrice = result.cgPrice || json.sora.usd || null;
                    result.cgMarketCap = json.sora.usd_market_cap || null;
                }
            }
        } catch (e) { /* fallback also unavailable */ }
    }

    // 2. Ethereum RPC — Query ERC-20 totalSupply() directly via public RPC
    // totalSupply() function selector = 0x18160ddd
    const ETH_RPCS = ['https://rpc.ankr.com/eth', 'https://eth.llamarpc.com', 'https://cloudflare-eth.com'];
    for (const rpcUrl of ETH_RPCS) {
        if (result.ethSupply) break;
        try {
            const controller = new AbortController();
            const timeout = setTimeout(() => controller.abort(), 8000);
            const res = await fetch(rpcUrl, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    jsonrpc: '2.0', method: 'eth_call',
                    params: [{ to: XOR_ETH_CONTRACT, data: '0x18160ddd' }, 'latest'],
                    id: 1
                }),
                signal: controller.signal
            });
            clearTimeout(timeout);
            if (res.ok) {
                const json = await res.json();
                if (json.result && json.result !== '0x') {
                    const raw = new BigNumber(json.result, 16);
                    if (!raw.isNaN() && raw.gt(0)) {
                        result.ethSupply = raw.div('1e18').toNumber();
                    }
                }
            }
        } catch (e) { /* try next RPC */ }
    }

    // 3. Get on-chain denomination factor
    try {
        if (api) {
            const denom = await withTimeout(api.query.denomination.denominator());
            result.denominationFactor = denom.toString().replace(/,/g, '');
        }
    } catch (e) { /* denomination query failed */ }

    xorMarketCache = { data: result, ts: now };
    return result;
}

// --- BATCHING PARA WEBSOCKET (ANTI-SATURACIÓN) ---

const BATCH_INTERVAL_MS = 5000; // 5 segundos para blindar la red
const MAX_EVENTS_PER_BATCH = 300; // Límite más bajo para evitar payloads gigantes
let pendingTransfers = [];
let pendingSwaps = [];
let pendingExtrinsicsBatch = [];
let pendingOrderBook = [];
let lastBatchTime = Date.now();
let sessionStats = { extrinsics: 0, bridges: 0, block: 0 };

// --- IDENTITY CACHE (IN-MEMORY) ---
const identityMemCache = new Map();
const IDENTITY_MEM_TTL = 3600000;   // 1h
const IDENTITY_DB_TTL = 86400000;   // 24h
const IDENTITY_RESOLVE_QUEUE = new Set();
let identityResolveTimer = null;

function parseIdentityFull(identityOpt) {
    const result = { display: null, email: null, web: null, twitter: null, discord: null };
    if (!identityOpt || identityOpt.isNone) return result;
    try {
        const identity = identityOpt.unwrap();
        const reg = Array.isArray(identity) ? identity[0] : identity;
        const info = reg.info;
        if (!info) return result;
        const extract = (field) => {
            if (!field) return null;
            if (field.isRaw) return Buffer.from(field.asRaw.toU8a(true)).toString('utf8');
            if (field.isNone) return null;
            const hex = field.toHex ? field.toHex() : null;
            return (hex && hex !== '0x') ? Buffer.from(hex.slice(2), 'hex').toString('utf8') : null;
        };
        result.display = extract(info.display);
        result.email = extract(info.email);
        result.web = extract(info.web);
        result.twitter = extract(info.twitter);
        result.discord = extract(info.discord);
    } catch (e) { /* silent */ }
    return result;
}

async function resolveIdentitiesBatch(addresses) {
    if (!api || !api.isConnected || !api.query.identity) return;
    const now = Date.now();
    const toResolve = addresses.filter(addr => {
        const cached = identityMemCache.get(addr);
        return !cached || (now - cached.ts > IDENTITY_MEM_TTL);
    });
    if (toResolve.length === 0) return;

    // Check DB for still-valid entries
    const dbResults = await getIdentities(toResolve);
    const needRpc = [];
    for (const addr of toResolve) {
        const dbRow = dbResults[addr];
        if (dbRow && (now - dbRow.updated_at < IDENTITY_DB_TTL)) {
            identityMemCache.set(addr, { display: dbRow.display, email: dbRow.email, web: dbRow.web, twitter: dbRow.twitter, discord: dbRow.discord, ts: now });
        } else {
            needRpc.push(addr);
        }
    }
    if (needRpc.length === 0) return;

    // Batch RPC in chunks of 50
    const CHUNK = 50;
    for (let i = 0; i < needRpc.length; i += CHUNK) {
        const chunk = needRpc.slice(i, i + CHUNK);
        try {
            const results = await api.query.identity.identityOf.multi(chunk);
            const dbBatch = [];
            for (let j = 0; j < chunk.length; j++) {
                const parsed = parseIdentityFull(results[j]);
                identityMemCache.set(chunk[j], { ...parsed, ts: Date.now() });
                dbBatch.push({ address: chunk[j], ...parsed });
            }
            await upsertIdentityBatch(dbBatch);
        } catch (e) {
            console.error('Identity batch resolve error:', e.message);
        }
    }
    // LRU eviction
    if (identityMemCache.size > 5000) {
        const keys = [...identityMemCache.keys()].slice(0, 1000);
        keys.forEach(k => identityMemCache.delete(k));
    }
}

function queueIdentityResolve(addresses) {
    for (const addr of addresses) {
        if (addr && addr.length > 40 && !addr.startsWith('0x')) {
            IDENTITY_RESOLVE_QUEUE.add(addr);
        }
    }
    if (identityResolveTimer) return;
    identityResolveTimer = setTimeout(async () => {
        const batch = [...IDENTITY_RESOLVE_QUEUE];
        IDENTITY_RESOLVE_QUEUE.clear();
        identityResolveTimer = null;
        if (batch.length > 0) await resolveIdentitiesBatch(batch);
    }, 500);
}

async function loadOfficialWhitelist() {
    try {
        console.log('📥 Descargando lista oficial de activos...');
        const response = await fetch(WHITELIST_URL);
        const data = await response.json();
        ASSETS = data.map(item => ({
            symbol: item.symbol, name: item.name, decimals: item.decimals, assetId: item.address, logo: item.icon
        }));

        // Ensure Stablecoins exist (Hardcoded fallback coverage)
        const ESSENTIALS = [
            { symbol: 'TBCD', decimals: 18, assetId: '0x02000a0000000000000000000000000000000000000000000000000000000000', name: 'TBarton Currency Dollar', logo: 'https://raw.githubusercontent.com/sora-xor/polkaswap-token-logos/master/tokens/0x02000a0000000000000000000000000000000000000000000000000000000000.svg' },
            { symbol: 'XSTUSD', decimals: 18, assetId: '0x0200080000000000000000000000000000000000000000000000000000000000', name: 'SORA Synthetic USD', logo: 'https://raw.githubusercontent.com/sora-xor/polkaswap-token-logos/master/tokens/0x0200080000000000000000000000000000000000000000000000000000000000.svg' },
            { symbol: 'KUSD', decimals: 18, assetId: '0x0081a26ba6cc31c4664c1f964a25b3af61f4c78496464522409f53e601556272', name: 'Kama USD', logo: 'https://raw.githubusercontent.com/sora-xor/polkaswap-token-logos/master/tokens/0x0081a26ba6cc31c4664c1f964a25b3af61f4c78496464522409f53e601556272.svg' }
        ];

        ESSENTIALS.forEach(e => {
            if (!ASSETS.find(a => a.symbol === e.symbol)) {
                ASSETS.push({ ...e, logo: '' });
                console.log(`⚠️ Adding missing essential asset: ${e.symbol}`);
            }
        });

        console.log(`✅ Whitelist cargada: ${ASSETS.length} activos.`);
    } catch (error) {
        console.error('❌ Error cargando whitelist, usando backup.');
        ASSETS = [
            { symbol: 'XOR', decimals: 18, assetId: '0x0200000000000000000000000000000000000000000000000000000000000000', logo: '' },
            { symbol: 'TBCD', decimals: 18, assetId: '0x0200090000000000000000000000000000000000000000000000000000000000', logo: '' },
            { symbol: 'XSTUSD', decimals: 18, assetId: '0x0200080000000000000000000000000000000000000000000000000000000000', logo: '' },
            { symbol: 'KUSD', decimals: 18, assetId: '0x0081a26ba6cc31c4664c1f964a25b3af61f4c78496464522409f53e601556272', logo: '' }
        ];
    }
}

function getAssetInfo(rawId) {
    if (!rawId) return null;
    let str = rawId.toString();
    if (str.startsWith('{')) { try { str = JSON.parse(str).code || str; } catch (e) { } }
    if (rawId.toJSON) { const j = rawId.toJSON(); if (j && j.code) str = j.code; }
    const found = ASSETS.find(a => a.assetId.toLowerCase() === str.toLowerCase());
    return found || { symbol: 'UNK', name: '?', decimals: 18, logo: '', assetId: str };
}

// --- PRICE CALCULATION (RESERVE BASED - Same as backfiller for consistency) ---


async function getXorPriceInDai() {
    try {
        const reserves = await withTimeout(api.query.poolXYK.reserves(XOR_ID, DAI_ID));
        if (!reserves || reserves.length < 2) return 0;

        // Use toString() for u128 precision (toJSON() loses precision for values > 2^53)
        const xorRes = new BigNumber(reserves[0].toString());
        const daiRes = new BigNumber(reserves[1].toString());

        if (xorRes.isZero()) return 0;
        return daiRes.div(xorRes).toNumber();
    } catch (e) {
        console.error('❌ Error fetching XOR/DAI reserves:', e.message);
        return 0;
    }
}

// Minimum 0.1 XOR in pool reserves to consider price reliable
const MIN_XOR_RESERVES = new BigNumber('1e17');

async function getTokenPriceInXor(assetId, tokenDecimals) {
    try {
        const reserves = await withTimeout(api.query.poolXYK.reserves(XOR_ID, assetId));
        if (!reserves || reserves.length < 2) return 0;

        // Use toString() for u128 precision (toJSON() loses precision for values > 2^53)
        const xorRes = new BigNumber(reserves[0].toString());
        const tokenRes = new BigNumber(reserves[1].toString());

        if (tokenRes.isZero()) return 0;
        if (xorRes.lt(MIN_XOR_RESERVES)) return 0; // Low liquidity — unreliable price

        // Spot Price = XOR_Reserves / Token_Reserves (normalized)
        const xorNormal = xorRes.div('1e18');
        const tokenNormal = tokenRes.div(new BigNumber(10).pow(tokenDecimals));

        return xorNormal.div(tokenNormal).toNumber();
    } catch (e) { return 0; }
}


// Token price in DAI via liquidityProxy.quote(). This matches what
// Polkaswap itself displays to users (same primitive). With an
// infinitesimal input (0.000001 × 10^decimals) the quote approaches mid
// price and the result is almost independent of pool depth, which is
// exactly what we want for a dashboard.
//
// Why quote() instead of reserves ratio:
//   · Multi-hop routing — if TOKEN/DAI pool doesn't exist, SORA finds
//     the best path automatically (TOKEN → XOR → DAI, or via XSTUSD).
//   · Multi-DEX — picks best between XYK / TBC / XST / OB.
//   · Honest — pool fees (0.3%) are already baked into the output.
//   · Consistent — same call Polkaswap uses, same numbers the user sees.
//
// Cache is keyed by assetId + decimals, 60s TTL (same as legacy impl).
async function getPriceInDai(assetId, decimals) {
    try {
        const cacheKey = `${assetId}_${decimals}`;
        if (!getPriceInDai.cache) getPriceInDai.cache = {};
        const cached = getPriceInDai.cache[cacheKey];
        if (cached && (Date.now() - cached.ts < 60000)) {
            return cached.price;
        }

        if (assetId === DAI_ID) {
            getPriceInDai.cache[cacheKey] = { price: 1, ts: Date.now() };
            return 1;
        }
        if (!api || !api.rpc?.liquidityProxy?.quote) return 0;

        // Input amount = 0.000001 × 10^decimals (infinitesimal vs. any pool
        // size). Raw value is 10^(decimals - 6). BigInt keeps precision for
        // the 18-decimal assets SORA uses.
        const decInt = Number(decimals) || 18;
        const rawIn = (10n ** BigInt(Math.max(decInt - 6, 0))).toString();

        // DEX 0 + filter 'Disabled' + empty selected sources = "try every
        // DEX / every pair, pick the best path". That's what the Polkaswap
        // UI passes.
        const quoted = await withTimeout(
            api.rpc.liquidityProxy.quote(0, assetId, DAI_ID, rawIn, 'WithDesiredInput', [], 'Disabled')
        );
        const j = quoted.toJSON();
        const amountOutRaw = j?.amount;
        if (amountOutRaw == null) {
            getPriceInDai.cache[cacheKey] = { price: 0, ts: Date.now() };
            return 0;
        }

        // DAI is 18 decimals. price_USD = (out_DAI_raw / 1e18) / 0.000001.
        // Equivalently: price_USD = out_DAI_raw / 10^(18 - 6) = out / 1e12.
        // We use BigNumber for safety on exotic decimals.
        const outBN = new BigNumber(String(amountOutRaw));
        const inputReadable = new BigNumber('0.000001');
        const outputReadable = outBN.div(new BigNumber(10).pow(18));
        const price = outputReadable.div(inputReadable).toNumber();

        const safePrice = Number.isFinite(price) && price > 0 ? price : 0;
        getPriceInDai.cache[cacheKey] = { price: safePrice, ts: Date.now() };
        return safePrice;

    } catch (e) {
        // A quote failure typically means no path exists (new / illiquid
        // token). Cache 0 briefly so we don't hammer the node with retries.
        if (!getPriceInDai.cache) getPriceInDai.cache = {};
        getPriceInDai.cache[`${assetId}_${decimals}`] = { price: 0, ts: Date.now() };
        return 0;
    }
}


async function updateKeyPrices() {
    if (!api) return;
    // Clear internal cache so all tokens get fresh on-chain prices
    getPriceInDai.cache = {};
    const POPULAR = ['XOR', 'VAL', 'PSWAP', 'ETH', 'DAI', 'TBCD', 'KUSD', 'DEO', 'KEN', 'KGOLD', 'KXOR', 'VXOR', 'XSTUSD', 'XST', 'KARMA', 'CERES'];
    for (const sym of POPULAR) {
        const asset = ASSETS.find(a => a.symbol === sym);
        if (asset) {
            tokenPrices[sym] = await getPriceInDai(asset.assetId, asset.decimals);
        }
    }
    console.log('💰 Precios actualizados (' + Object.keys(tokenPrices).length + ' tokens).');

    // Update price_history table for sparklines (runs incrementally, ~16 upserts)
    updatePriceHistory(tokenPrices).catch(err => console.error('[price_history] Update error:', err.message));
}

// Get price for any token - fetches on-demand if not cached
async function getOrFetchPrice(symbol, assetId, decimals) {
    // Return cached price if available
    if (tokenPrices[symbol] !== undefined && tokenPrices[symbol] > 0) {
        return tokenPrices[symbol];
    }

    // Fetch price on-demand
    if (assetId && api) {
        try {
            const price = await getPriceInDai(assetId, decimals || 18);
            if (price > 0) {
                tokenPrices[symbol] = price;
                console.log(`💵 Precio obtenido para ${symbol}.`);
            }
            return price;
        } catch (e) {
            return 0;
        }
    }
    return 0;
}

// --- RUTAS ---
app.get('/tokens', rateLimit(30, 60000), async (req, res) => {
    if (!api) return res.status(503).json({ error: 'Iniciando...' });

    const page = parseInt(req.query.page) || 1;
    const limit = Math.min(parseInt(req.query.limit) || 20, 100);
    const search = (req.query.search || '').toLowerCase();
    const timeframe = req.query.timeframe || '24h';
    const includeSparkline = req.query.sparkline !== 'false'; // Default TRUE
    const onlySparklines = req.query.onlySparklines === 'true'; // Default FALSE

    const isDefaultView = page === 1 && !search && !req.query.symbols && limit === 20 && includeSparkline && !onlySparklines;
    const now = Date.now();

    // Cache for default view
    if (isDefaultView && globalTokenCache.data && (now - globalTokenCache.timestamp < CACHE_TTL)) {
        const cached = JSON.parse(JSON.stringify(globalTokenCache.data));
        cached.data = cached.data.map(t => ({
            ...t,
            price: tokenPrices[t.symbol] || t.price || 0
        }));
        return res.json(cached);
    }

    let filtered = ASSETS;
    if (req.query.symbols) {
        const symbols = req.query.symbols.split(',').map(s => s.trim());
        filtered = ASSETS.filter(a => symbols.includes(a.symbol));
    } else if (search) {
        filtered = ASSETS.filter(a => a.symbol.toLowerCase().includes(search) || a.name.toLowerCase().includes(search) || a.assetId.toLowerCase().includes(search));
    }

    // Sort by Custom Priority
    // 1. Fixed Top: XOR, TBCD, VAL, PSWAP, KUSD
    // 2. Ecosystem (from POPULAR list): Alphabetical
    // 3. Others: Alphabetical

    const FIXED_TOP = ['XOR', 'TBCD', 'VAL', 'PSWAP', 'KUSD'];
    // POPULAR is defined in updateKeyPrices scope, let's redefine explicitly for sorting or use a shared constant if possible.
    // For safety and strict adherence to user request, defining the Ecosystem group here.
    const ECOSYSTEM = ['ETH', 'DAI', 'DEO', 'KEN', 'KGOLD', 'KXOR', 'VXOR', 'XSTUSD', 'XST', 'KARMA', 'CERES'];

    filtered.sort((a, b) => {
        const symA = a.symbol;
        const symB = b.symbol;

        // Group 1: Fixed Top
        const idxA = FIXED_TOP.indexOf(symA);
        const idxB = FIXED_TOP.indexOf(symB);
        if (idxA !== -1 && idxB !== -1) return idxA - idxB;
        if (idxA !== -1) return -1;
        if (idxB !== -1) return 1;

        // Group 2: Ecosystem
        const isEcoA = ECOSYSTEM.includes(symA);
        const isEcoB = ECOSYSTEM.includes(symB);
        if (isEcoA && isEcoB) return symA.localeCompare(symB);
        if (isEcoA) return -1;
        if (isEcoB) return 1;

        // Group 3: All Others (Alphabetical)
        return symA.localeCompare(symB);
    });

    const total = filtered.length;
    const start = (page - 1) * limit;
    const paginated = (limit === 0) ? filtered : filtered.slice(start, start + limit);

    // Fetch/refresh prices for paginated tokens (getPriceInDai has 60s per-key cache)
    if (!onlySparklines) {
        await Promise.all(paginated.map(async (asset) => {
            tokenPrices[asset.symbol] = await getPriceInDai(asset.assetId, asset.decimals);
        }));
    }

    const tfMs = TIMEFRAME_MS[timeframe] || 86400000;

    const enriched = await Promise.all(paginated.map(async a => {
        // If onlySparklines, we skip price change and just get spark
        if (onlySparklines) {
            try {
                const sparkline = await getSparkline(a.symbol, tfMs);
                return { symbol: a.symbol, sparkline };
            } catch (e) { return { symbol: a.symbol, sparkline: [] }; }
        }

        const price = tokenPrices[a.symbol] || 0;
        try {
            let change = 0;
            let sparkline = [];

            const promises = [];
            promises.push(getPriceChange(a.symbol, price, tfMs).then(c => change = c));

            if (includeSparkline) {
                promises.push(getSparkline(a.symbol, tfMs).then(s => sparkline = s));
            }

            await Promise.all(promises);
            return { ...a, price, change24h: change, sparkline };
        } catch (err) {
            console.error(`DB Error for ${a.symbol}:`, err);
            return { ...a, price, change24h: 0, sparkline: [] };
        }
    }));

    const result = { data: enriched, total, page, totalPages: Math.ceil(total / limit) };

    if (isDefaultView) {
        globalTokenCache = { timestamp: now, data: result };
    }

    res.json(result);
});

app.get('/pools', rateLimit(20, 60000), async (req, res) => {
    if (!api) return res.json({ data: [], total: 0 });
    const page = parseInt(req.query.page) || 1;
    const limit = Math.min(parseInt(req.query.limit) || 10, 100);
    const now = Date.now();
    const cacheKey = `pools_${page}_${limit}_${req.query.base}`;

    // Rebuild cache if expired
    if (!poolsCache.data || !poolsCache.timestamp || (now - poolsCache.timestamp >= POOLS_TTL)) {
        try {
            const entries = await withTimeout(api.query.poolXYK.reserves.entries(), 30000);
            let pools = [];

            for (const [key, value] of entries) {
                const args = key.args;
                let baseId = args[0].toHuman();
                let targetId = args[1].toHuman();
                if (typeof baseId === 'object' && baseId.code) baseId = baseId.code;
                if (typeof targetId === 'object' && targetId.code) targetId = targetId.code;

                const reserves = value.toHuman();
                const baseToken = ASSETS.find(a => a.assetId === baseId) || { symbol: '?', name: 'Unknown', assetId: baseId, decimals: 18, logo: '' };
                const targetToken = ASSETS.find(a => a.assetId === targetId) || { symbol: '?', name: 'Unknown', assetId: targetId, decimals: 18, logo: '' };

                if (baseToken.symbol !== '?' && targetToken.symbol !== '?') {
                    pools.push({
                        base: baseToken,
                        target: targetToken,
                        reserves: { base: reserves[0], target: reserves[1] },
                        basePrice: tokenPrices[baseToken.symbol] || 0,
                        targetPrice: tokenPrices[targetToken.symbol] || 0
                    });
                }
            }

            // Sort by TVL (USD value) so meaningful pools appear first
            pools.sort((a, b) => {
                const rawBaseA = parseFloat(String(a.reserves.base || '0').replace(/,/g, '')) / Math.pow(10, a.base.decimals);
                const rawTargetA = parseFloat(String(a.reserves.target || '0').replace(/,/g, '')) / Math.pow(10, a.target.decimals);
                const tvlA = (rawBaseA * (a.basePrice || 0)) + (rawTargetA * (a.targetPrice || 0));
                const rawBaseB = parseFloat(String(b.reserves.base || '0').replace(/,/g, '')) / Math.pow(10, b.base.decimals);
                const rawTargetB = parseFloat(String(b.reserves.target || '0').replace(/,/g, '')) / Math.pow(10, b.target.decimals);
                const tvlB = (rawBaseB * (b.basePrice || 0)) + (rawTargetB * (b.targetPrice || 0));
                return tvlB - tvlA;
            });

            poolsCache = { data: pools, timestamp: now };
        } catch (e) {
            console.error(e);
            return res.status(500).json({ error: 'Internal server error' });
        }
    }

    try {
        // Apply base filter on cached data
        let filtered = poolsCache.data;
        const baseParam = req.query.base;
        if (baseParam && baseParam !== 'all') {
            filtered = filtered.filter(p => p.base.symbol === baseParam);
        }

        const total = filtered.length;
        const totalPages = Math.ceil(total / limit);
        const startIndex = (page - 1) * limit;
        const paginatedPools = filtered.slice(startIndex, startIndex + limit);

        res.json({ data: paginatedPools, total, page, totalPages });
    } catch (e) {
        console.error(e);
        res.status(500).json({ error: 'Internal server error' });
    }
});

app.get('/pool/providers', rateLimit(10, 60000), async (req, res) => {
    const now = Date.now();
    const { base, target } = req.query;
    const cacheKey = `${base}_${target}`;
    
    // Check cache (90s)
    if (providersCache.data && providersCache.timestamp && (now - providersCache.timestamp < PROVIDERS_TTL)) {
        const cached = providersCache.data[cacheKey];
        if (cached) return res.json(cached);
    }
    
    try {
        if (!base || !target) return res.status(400).json({ error: 'Missing base or target' });
        if (!api) return res.json([]);

        console.log(`🔍 Pool Providers Query: base=${base.substring(0, 10)}..., target=${target.substring(0, 10)}...`);

        // Step 1: Find the Pool Account from properties storage
        let poolAccount = null;

        const props = await withTimeout(api.query.poolXYK.properties(base, target));
        if (props && !props.isEmpty) {
            const propsHuman = props.toHuman();
            console.log('   Pool properties:', JSON.stringify(propsHuman).substring(0, 100));
            if (Array.isArray(propsHuman) && propsHuman.length > 0) {
                poolAccount = propsHuman[0];
            } else if (propsHuman && typeof propsHuman === 'object') {
                poolAccount = propsHuman.accountId || propsHuman.account || propsHuman[0];
            }
        }

        // Try reverse pair
        if (!poolAccount) {
            const propsReverse = await withTimeout(api.query.poolXYK.properties(target, base));
            if (propsReverse && !propsReverse.isEmpty) {
                const propsHuman = propsReverse.toHuman();
                console.log('   Pool properties (reversed):', JSON.stringify(propsHuman).substring(0, 100));
                if (Array.isArray(propsHuman) && propsHuman.length > 0) {
                    poolAccount = propsHuman[0];
                }
            }
        }

        if (!poolAccount) {
            console.log('   ❌ Could not find pool account');
            return res.json([]);
        }

        console.log(`   ✅ Pool account: ${String(poolAccount).substring(0, 20)}...`);

        // Step 2: Query poolProviders for this specific pool account
        const providerEntries = await withTimeout(api.query.poolXYK.poolProviders.entries(poolAccount));
        console.log(`   Provider entries: ${providerEntries.length}`);

        const providers = providerEntries.map(([key, value]) => {
            const userAccount = key.args[1].toString();
            const balance = new BigNumber(value.toString()).div('1e18').toNumber();
            return { address: userAccount, balance };
        }).sort((a, b) => b.balance - a.balance);

        console.log(`✅ Found ${providers.length} providers`);
        
        // Save to cache
        if (!providersCache.data) providersCache.data = {};
        providersCache.data[cacheKey] = providers;
        providersCache.timestamp = now;
        
        res.json(providers);
    } catch (e) {
        console.error(e);
        res.status(500).json({ error: 'Internal server error' });
    }
});

// New Endpoint for Network Trend Chart
app.get('/stats/network/trend', rateLimit(15, 60000), async (req, res) => {
    try {
        const { timeframe } = req.query;
        let startTime = Date.now() - (24 * 60 * 60 * 1000); // Default 24h
        let interval = 'hour';

        if (timeframe === '7d' || timeframe === '30d') {
            interval = 'day';
            const days = timeframe === '7d' ? 7 : 30;
            startTime = Date.now() - (days * 24 * 60 * 60 * 1000);
        } else if (timeframe === '1h') {
            startTime = Date.now() - (60 * 60 * 1000);
        } else if (timeframe === '4h') {
            startTime = Date.now() - (4 * 60 * 60 * 1000);
        }

        const data = await getNetworkTrend(startTime, interval);
        res.json(data);
    } catch (e) {
        console.error('Error stats/network/trend:', e);
        res.status(500).json({ error: 'Internal server error' });
    }
});

// Stablecoin Monitor Endpoint
app.get('/stats/stablecoins', rateLimit(20, 60000), async (req, res) => {
    try {
        const { timeframe } = req.query;
        let startTime = Date.now() - (24 * 60 * 60 * 1000); // Default 24h
        let chartInterval = '24h'; // For sparkline

        if (timeframe === '7d') { startTime = Date.now() - (7 * 24 * 60 * 60 * 1000); chartInterval = '7d'; }
        if (timeframe === '30d') { startTime = Date.now() - (30 * 24 * 60 * 60 * 1000); chartInterval = '1m'; } // Use 1m (month) logic for 30d if available or just mapping
        if (timeframe === '1h') { startTime = Date.now() - (60 * 60 * 1000); chartInterval = '1h'; }
        if (timeframe === '4h') { startTime = Date.now() - (4 * 60 * 60 * 1000); chartInterval = '4h'; }

        // Map timeframe for sparkline function if needed, or pass ms. 
        // getSparkline takes (symbol, msWindow)
        const msWindow = TIMEFRAME_MS[timeframe] || 86400000;

        // 1. Get Volumes
        const volStats = await getStablecoinStats(startTime);

        // 2. Get Prices & Sparklines
        const TOKENS = ['KUSD', 'XSTUSD', 'TBCD'];
        const results = [];

        for (const sym of TOKENS) {
            // Ensure price is fresh-ish
            const asset = ASSETS.find(a => a.symbol === sym);
            let price = tokenPrices[sym] || 0;

            // If price is missing or 0, try fetch
            if (asset && (!price || price === 0)) {
                price = await getOrFetchPrice(sym, asset.assetId, asset.decimals);
            }

            // Get sparkline
            const sparkline = await getSparkline(sym, msWindow);

            results.push({
                symbol: sym,
                price: price,
                logo: asset ? asset.logo : '',
                swapVolume: volStats[sym] ? volStats[sym].swapVolume : 0,
                transferVolume: volStats[sym] ? volStats[sym].transferVolume : 0,
                sparkline: sparkline
            });
        }

        res.json(results);
    } catch (e) {
        console.error('Error stats/stablecoins:', e);
        res.status(500).json({ error: 'Internal server error' });
    }
});

// Trending Tokens Endpoint (For Donut Chart)
app.get('/stats/trending-tokens', rateLimit(20, 60000), async (req, res) => {
    try {
        const { timeframe } = req.query;
        let startTime = Date.now() - (24 * 60 * 60 * 1000); // Default 24h

        if (timeframe === '7d') startTime = Date.now() - (7 * 24 * 60 * 60 * 1000);
        if (timeframe === '30d') startTime = Date.now() - (30 * 24 * 60 * 60 * 1000);
        if (timeframe === '1h') startTime = Date.now() - (60 * 60 * 1000);
        if (timeframe === '4h') startTime = Date.now() - (4 * 60 * 60 * 1000);
        if (timeframe === 'all') startTime = 0;

        // Ensure getTopTokens is imported
        const data = await getTopTokens(startTime);
        res.json(data);
    } catch (e) {
        console.error('Error stats/trending-tokens:', e);
        res.status(500).json({ error: 'Internal server error' });
    }
});

app.get('/pool/activity', rateLimit(20, 60000), async (req, res) => {
    const now = Date.now();
    const { base, target } = req.query;
    const cacheKey = `${base}_${target}`;
    
    // Check cache (90s)
    if (activityCache.data && activityCache.timestamp && (now - activityCache.timestamp < ACTIVITY_TTL)) {
        const cached = activityCache.data[cacheKey];
        if (cached) return res.json(cached);
    }
    
    try {
        if (!base || !target) return res.status(400).json({ error: 'Missing base or target' });

        const limit = Math.min(parseInt(req.query.limit) || 50, 200);
        const activity = await getPoolActivity(base, target, limit);
        
        // Save to cache
        if (!activityCache.data) activityCache.data = {};
        activityCache.data[cacheKey] = activity || [];
        activityCache.timestamp = now;
        
        res.json(activity || []);
    } catch (e) {
        console.error(e);
        res.status(500).json({ error: 'Internal server error' });
    }
});

app.get('/holders/:assetId', validateAssetId, rateLimit(15, 60000), async (req, res) => {
    if (!api) return res.status(500).json({ error: 'API no lista' });

    const assetId = req.params.assetId;
    const page = parseInt(req.query.page) || 1;
    const limit = 25;
    const startIndex = (page - 1) * limit;
    const endIndex = page * limit;

    try {
        let fullList = [];
        const now = Date.now();

        if (holdersCache[assetId] && (now - holdersCache[assetId].timestamp < CACHE_DURATION)) {
            fullList = holdersCache[assetId].list;
        } else {
            console.log(`🔍 Escaneando holders para ${assetId}...`);
            const assetInfo = getAssetInfo(assetId);
            const decimals = assetInfo ? assetInfo.decimals : 18;
            const XOR_ID = '0x0200000000000000000000000000000000000000000000000000000000000000';

            if (assetId === XOR_ID) {
                const allEntries = await withTimeout(api.query.system.account.entries(), 60000);
                for (const [key, value] of allEntries) {
                    const data = value.toJSON();
                    const free = (data.data && data.data.free) ? data.data.free.toString() : '0';
                    const amountBn = new BigNumber(free).div('1e18');
                    if (amountBn.gt(1)) {
                        fullList.push({ address: key.args[0].toString(), balance: amountBn.toNumber(), balanceStr: amountBn.toFormat(2) });
                    }
                }
            } else {
                const allEntries = await withTimeout(api.query.tokens.accounts.entries(), 60000);
                for (const [key, value] of allEntries) {
                    const keyArgs = key.args;
                    let currentAssetId = keyArgs[1].toString();
                    if (currentAssetId.startsWith('{')) { try { currentAssetId = JSON.parse(currentAssetId).code; } catch (e) { } }

                    if (currentAssetId === assetId) {
                        const data = value.toJSON();
                        const free = data.free ? data.free.toString() : '0';
                        const amountBn = new BigNumber(free).div(new BigNumber(10).pow(decimals));

                        if (amountBn.gt(0.1)) {
                            fullList.push({ address: keyArgs[0].toString(), balance: amountBn.toNumber(), balanceStr: amountBn.toFormat(2) });
                        }
                    }
                }
            }

            fullList.sort((a, b) => b.balance - a.balance);
            holdersCache[assetId] = { timestamp: now, list: fullList };
        }

        const paginatedItems = fullList.slice(startIndex, endIndex);
        res.json({ page: page, totalHolders: fullList.length, totalPages: Math.ceil(fullList.length / limit), data: paginatedItems });
    } catch (e) {
        console.error(e);
        res.status(500).json({ error: 'Internal server error' });
    }
});

app.get("/wallet/liquidity/:address", validateAddress, rateLimit(300, 60000), async (req, res) => {
    if (!api) return res.status(500).json({ error: 'API not ready' });
    const address = req.params.address;

    // Per-address cache check (3 min TTL)
    const cached = liquidityCache[address];
    if (cached && Date.now() - cached.timestamp < LP_STAKING_TTL) {
        return res.json(cached.data);
    }

    try {
        console.log(`Liquidity Check Start: ${address}`);

        // Global pool properties cache (same for all wallets, 5 min TTL)
        let allProps;
        if (poolPropertiesCache.data && Date.now() - poolPropertiesCache.timestamp < POOL_PROPS_TTL) {
            allProps = poolPropertiesCache.data;
            console.log(`Pool properties from cache: ${allProps.length}`);
        } else {
            allProps = await withTimeout(api.query.poolXYK.properties.entries(), 30000);
            poolPropertiesCache = { data: allProps, timestamp: Date.now() };
            console.log(`Total pools to scan (fresh): ${allProps.length}`);
        }

        // OPTIMIZATION: Use larger chunks + multi query
        const CHUNK_SIZE = 100;
        const poolsData = [];

        // Helper to extract pool account
        const getPoolAccount = (props) => {
            if (Array.isArray(props)) return props[0];
            else if (props && typeof props === 'object') return props.accountId || props.account;
            return null;
        };

        for (let i = 0; i < allProps.length; i += CHUNK_SIZE) {
            const chunk = allProps.slice(i, i + CHUNK_SIZE);
            const chunkArgs = [];
            const validPoolsInChunk = [];

            // Prepare args for multi query
            for (const [key, val] of chunk) {
                const props = val.toJSON();
                const account = getPoolAccount(props);
                if (account) {
                    chunkArgs.push([account, address]);
                    validPoolsInChunk.push({ key, account });
                }
            }

            if (chunkArgs.length === 0) continue;

            // Fetch ALL balances in one go
            const balances = await withTimeout(api.query.poolXYK.poolProviders.multi(chunkArgs));

            // Process results (only non-zero)
            const activePools = [];
            for (let j = 0; j < balances.length; j++) {
                const balFn = balances[j].toString();
                if (balFn !== '0') {
                    activePools.push({
                        ...validPoolsInChunk[j],
                        balance: new BigNumber(balFn)
                    });
                }
            }

            // Fetch details for active pools ONLY (parallel)
            await Promise.all(activePools.map(async (pool) => {
                try {
                    const poolAccount = pool.account;
                    const userBalance = pool.balance;

                    const totalIssuanceCodec = await withTimeout(api.query.poolXYK.totalIssuances(poolAccount));
                    const totalIssuance = new BigNumber(totalIssuanceCodec.toString());
                    if (totalIssuance.isZero()) return;

                    const share = userBalance.div(totalIssuance);
                    const args = pool.key.args;
                    let baseId = args[0].toHuman();
                    let targetId = args[1].toHuman();
                    if (typeof baseId === 'object' && baseId.code) baseId = baseId.code;
                    if (typeof targetId === 'object' && targetId.code) targetId = targetId.code;

                    const resCodec = await withTimeout(api.query.poolXYK.reserves(baseId, targetId));

                    const baseRes = new BigNumber(resCodec[0].toString());
                    const targetRes = new BigNumber(resCodec[1].toString());

                    const baseToken = ASSETS.find(a => a.assetId === baseId) || { symbol: '?', decimals: 18, logo: '' };
                    const targetToken = ASSETS.find(a => a.assetId === targetId) || { symbol: '?', decimals: 18, logo: '' };

                    if (baseToken.symbol === '?' || targetToken.symbol === '?') return;

                    const userBase = baseRes.times(share).div(new BigNumber(10).pow(baseToken.decimals));
                    const userTarget = targetRes.times(share).div(new BigNumber(10).pow(targetToken.decimals));

                    const valBase = userBase.times(tokenPrices[baseToken.symbol] || 0);
                    const valTarget = userTarget.times(tokenPrices[targetToken.symbol] || 0);
                    const totalValue = valBase.plus(valTarget).toNumber();

                    if (isNaN(totalValue) || totalValue < 0.10) return;

                    poolsData.push({
                        base: baseToken,
                        target: targetToken,
                        amountBase: userBase.toNumber(),
                        amountTarget: userTarget.toNumber(),
                        value: totalValue,
                        share: share.toNumber()
                    });
                } catch (e) { console.error("Error processing pool details:", e); }
            }));
        }

        const result = poolsData.sort((a, b) => b.value - a.value);
        console.log(`Liquidity Scan Finished. Found ${result.length} records. (cached for ${LP_STAKING_TTL / 1000}s)`);
        // Cache per-address result
        liquidityCache[address] = { data: result, timestamp: Date.now() };
        res.json(result);
    } catch (e) {
        console.error("Error fetching wallet liquidity:", e);
        res.status(500).json({ error: 'Internal server error' });
    }
});

// Native staking info for a wallet
app.get("/wallet/staking/:address", validateAddress, rateLimit(300, 60000), async (req, res) => {
    if (!api) return res.json({ staked: 0, unbonding: 0, rewards: 0, usdValue: 0 });
    const address = req.params.address;

    // Per-address cache check (3 min TTL)
    const cached = stakingCache[address];
    if (cached && Date.now() - cached.timestamp < LP_STAKING_TTL) {
        return res.json(cached.data);
    }

    try {
        // Check if address is a stash (bonded to a controller)
        const bonded = await withTimeout(api.query.staking.bonded(address));
        const controller = bonded.isSome ? bonded.unwrap().toString() : null;

        // Check staking ledger (try address as controller, or use the controller from bonded)
        let ledger = null;
        try {
            const ledgerResult = await withTimeout(api.query.staking.ledger(address));
            if (ledgerResult.isSome) ledger = ledgerResult.unwrap();
        } catch (e) {}

        if (!ledger && controller) {
            try {
                const ledgerResult = await withTimeout(api.query.staking.ledger(controller));
                if (ledgerResult.isSome) ledger = ledgerResult.unwrap();
            } catch (e) {}
        }

        if (!ledger) {
            return res.json({ staked: 0, unbonding: 0, rewards: 0, usdValue: 0, validators: [] });
        }

        const decimals = 18;
        const active = new BigNumber(ledger.active.toString()).div(new BigNumber(10).pow(decimals));
        const total = new BigNumber(ledger.total.toString()).div(new BigNumber(10).pow(decimals));
        const unbonding = total.minus(active);

        // Get nominator info (which validators)
        let validators = [];
        try {
            const nominators = await withTimeout(api.query.staking.nominators(address));
            if (nominators.isSome) {
                const targets = nominators.unwrap().targets;
                validators = targets.map(v => v.toString());
            }
        } catch (e) {}

        const xorPrice = tokenPrices['XOR'] || 0;
        const stakedNum = active.toNumber();
        const unbondingNum = unbonding.toNumber();

        const result = {
            staked: stakedNum,
            unbonding: unbondingNum,
            rewards: 0, // Phase 2: calculate pending rewards
            usdValue: (stakedNum + unbondingNum) * xorPrice,
            stakedUsd: stakedNum * xorPrice,
            unbondingUsd: unbondingNum * xorPrice,
            validators
        };
        // Cache per-address result
        stakingCache[address] = { data: result, timestamp: Date.now() };
        res.json(result);
    } catch (e) {
        console.error("Error fetching staking info:", e.message);
        res.json({ staked: 0, unbonding: 0, rewards: 0, usdValue: 0, validators: [] });
    }
});

// Wallet Info (aggregate stats)
app.get("/wallet/info/:address", validateAddress, rateLimit(300, 60000), async (req, res) => {
    const address = req.params.address;
    const cached = walletInfoCache[address];
    if (cached && Date.now() - cached.timestamp < LP_STAKING_TTL) {
        return res.json(cached.data);
    }
    try {
        const info = await getWalletInfo(address);
        if (!info) return res.json({ error: 'No data' });

        // Compute Whale Score server-side
        const volumeScore = Math.min(40, Math.round((info.swapTotalVolume / 500000) * 40));
        const freqScore = Math.min(30, Math.round((info.txCount / 5000) * 30));
        const diversityRaw = (info.uniqueTokens || 0) + (info.lpUniquePools || 0) + (info.bridgeUniqueNetworks || 0);
        const divScore = Math.min(30, Math.round((diversityRaw / 30) * 30));
        const whaleScore = volumeScore + freqScore + divScore;
        let whaleTier = 'Shrimp';
        if (whaleScore > 90) whaleTier = 'Megawhale';
        else if (whaleScore > 75) whaleTier = 'Whale';
        else if (whaleScore > 50) whaleTier = 'Dolphin';
        else if (whaleScore > 25) whaleTier = 'Fish';

        const result = {
            ...info,
            whaleScore, whaleTier,
            whaleBreakdown: { volume: volumeScore, frequency: freqScore, diversity: divScore }
        };
        walletInfoCache[address] = { data: result, timestamp: Date.now() };
        res.json(result);
    } catch (e) {
        console.error('Error /wallet/info:', e.message);
        res.status(500).json({ error: 'Internal server error' });
    }
});

// Identity lookup for a single address
app.get('/identity/:address', validateAddress, rateLimit(60, 60000), async (req, res) => {
    const address = req.params.address;
    try {
        // Check cache first
        let cached = identityMemCache.get(address);
        if (cached && (Date.now() - cached.ts < IDENTITY_MEM_TTL)) {
            return res.json({ display: cached.display, email: cached.email, web: cached.web, twitter: cached.twitter, discord: cached.discord });
        }
        // Resolve via batch (handles DB + RPC fallback)
        await resolveIdentitiesBatch([address]);
        cached = identityMemCache.get(address);
        if (cached && cached.display) {
            return res.json({ display: cached.display, email: cached.email, web: cached.web, twitter: cached.twitter, discord: cached.discord });
        }
        res.json({ display: null });
    } catch (e) {
        res.json({ display: null });
    }
});

// Currency rates proxy (cached 1h)
let eurRateCache = { rate: 0.92, ts: 0 };
app.get('/currency-rates', rateLimit(10, 60000), (req, res) => {
    const now = Date.now();
    if (now - eurRateCache.ts < 3600000 && eurRateCache.rate !== 0.92) {
        return res.json({ EUR: eurRateCache.rate });
    }
    https.get('https://open.er-api.com/v6/latest/USD', (resp) => {
        let body = '';
        resp.on('data', chunk => body += chunk);
        resp.on('end', () => {
            try {
                const data = JSON.parse(body);
                if (data.rates?.EUR) {
                    eurRateCache = { rate: data.rates.EUR, ts: now };
                    res.json({ EUR: data.rates.EUR });
                } else { res.json({ EUR: eurRateCache.rate }); }
            } catch (e) { res.json({ EUR: eurRateCache.rate }); }
        });
    }).on('error', () => res.json({ EUR: eurRateCache.rate }));
});

app.get("/balance/:address", validateAddress, rateLimit(300, 60000), async (req, res) => {
    if (!api) return res.json([]);
    const address = req.params.address;
    const balances = [];
    try {
        const { data: { free: xorFree } } = await withTimeout(api.query.system.account(address));
        const xorAmt = new BigNumber(xorFree.toString()).div('1e18');
        if (xorAmt.gt(0)) {
            const xorDef = ASSETS.find(a => a.symbol === 'XOR');
            balances.push({
                symbol: 'XOR', logo: xorDef ? xorDef.logo : '',
                amount: xorAmt.toFixed(4),
                usdValue: xorAmt.times(tokenPrices['XOR'] || 0).toFixed(2)
            });
        }
        const entries = await withTimeout(api.query.tokens.accounts.entries(address));
        for (const [key, value] of entries) {
            const assetId = key.args[1].toString();
            const data = value.toJSON();
            const assetInfo = getAssetInfo(assetId);
            const decimals = assetInfo ? assetInfo.decimals : 18;
            const amount = new BigNumber(data.free).div(new BigNumber(10).pow(decimals));

            if (amount.gt(0.0001)) {
                let price = tokenPrices[assetInfo?.symbol] || 0;
                balances.push({
                    symbol: assetInfo ? assetInfo.symbol : 'UNK',
                    logo: assetInfo ? assetInfo.logo : '',
                    amount: amount.toFixed(4),
                    usdValue: amount.times(price).toFixed(2)
                });
            }
        }
        balances.sort((a, b) => parseFloat(b.usdValue) - parseFloat(a.usdValue));
        res.json(balances);
    } catch (e) { res.json([]); }
});

async function getAddressBalances(address) {
    const balances = [];
    try {
        const { data: { free: xorFree } } = await withTimeout(api.query.system.account(address));
        const xorAmt = new BigNumber(xorFree.toString()).div('1e18');
        if (xorAmt.gt(0)) {
            const xorDef = ASSETS.find(a => a.symbol === 'XOR');
            balances.push({
                symbol: 'XOR', logo: xorDef ? xorDef.logo : '',
                amount: xorAmt.toFixed(4),
                usdValue: xorAmt.times(tokenPrices['XOR'] || 0).toFixed(2),
                assetId: '0x0200000000000000000000000000000000000000000000000000000000000000'
            });
        }
        const entries = await withTimeout(api.query.tokens.accounts.entries(address));
        for (const [key, value] of entries) {
            const assetId = key.args[1].toString();
            const data = value.toJSON();
            const assetInfo = getAssetInfo(assetId);
            const decimals = assetInfo ? assetInfo.decimals : 18;
            const amount = new BigNumber(data.free).div(new BigNumber(10).pow(decimals));


            if (amount.gt(0.0001)) {
                const sym = assetInfo?.symbol;
                let price = tokenPrices[sym] || 0;

                // --- DEBUG XST ---
                if (sym === 'XST') {
                    // Check if price is suspiciously low (discrepancy investigation)
                    if (price < 0.1) {
                        // Fallback: If price is low, maybe the key lookup failed?
                        // Try explicit fetch or check 'XST' key
                        if (tokenPrices['XST'] > 0.1) price = tokenPrices['XST'];
                    }

                    console.log(`💰 BALANCE XST: ${amount.toFixed(4)} @ $${price}`);
                }
                // ----------------

                balances.push({
                    symbol: sym || 'UNK',
                    logo: assetInfo ? assetInfo.logo : '',
                    amount: amount.toFixed(4),
                    usdValue: amount.times(price).toFixed(2),
                    assetId: assetId
                });
            }

        }
        balances.sort((a, b) => parseFloat(b.usdValue) - parseFloat(a.usdValue));
        return balances;
    } catch (e) { return []; }
}

app.post('/balances', rateLimit(20, 60000), async (req, res) => {
    if (!api) return res.json({ result: [] });
    const { addresses } = req.body;
    if (!addresses || !Array.isArray(addresses)) return res.json({ result: [] });
    if (addresses.length > 100) return res.status(400).json({ error: 'Max 100 addresses per request' });
    if (addresses.some(a => !VALID_SS58.test(a))) return res.status(400).json({ error: 'Invalid address format in list' });
    const results = [];
    const CHUNK_SIZE = 20;
    for (let i = 0; i < addresses.length; i += CHUNK_SIZE) {
        const chunk = addresses.slice(i, i + CHUNK_SIZE);
        const chunkResults = await Promise.all(chunk.map(async (addr) => {
            const bal = await getAddressBalances(addr);
            const totalUsd = bal.reduce((acc, t) => acc + parseFloat(t.usdValue || 0), 0);
            return { address: addr, tokens: bal, totalUsd: totalUsd };
        }));
        results.push(...chunkResults);
    }
    res.json({ result: results });
});

app.get('/history/global/transfers', rateLimit(30, 60000), async (req, res) => {
    const now = Date.now();
    const page = parseInt(req.query.page) || 1;
    const limit = Math.min(parseInt(req.query.limit) || 25, 100);

    // Check cache (60s) — only for default page 1 with no filters
    if (page === 1 && limit === 25 && !req.query.filter && !req.query.timestamp
        && transfersCache.data && now - transfersCache.timestamp < TRANSFERS_TTL) {
        return res.json(transfersCache.data);
    }

    try {
        const data = await getLatestTransfers(page, limit, req.query.filter, req.query.timestamp);
        if (page === 1 && limit === 25 && !req.query.filter && !req.query.timestamp) {
            transfersCache = { data, timestamp: now };
        }
        res.json(data);
    } catch (e) { res.json({ data: [], total: 0 }); }
});

const swapsCacheMap = new Map();
app.get('/history/global/swaps', rateLimit(30, 60000), async (req, res) => {
    const now = Date.now();
    const page = parseInt(req.query.page) || 1;
    const limit = Math.min(parseInt(req.query.limit) || 25, 100);
    const filter = req.query.token || req.query.filter || '';
    const ts = req.query.timestamp || '';
    const cacheKey = `${page}_${limit}_${filter}_${ts}`;

    // Check per-key cache (24s)
    const cached = swapsCacheMap.get(cacheKey);
    if (cached && now - cached.timestamp < SWAPS_TTL) {
        return res.json(cached.data);
    }

    try {
        const data = await getLatestSwaps(page, limit, filter || null, req.query.timestamp || null);
        swapsCacheMap.set(cacheKey, { data, timestamp: now });
        // Evict old entries
        if (swapsCacheMap.size > 50) {
            const oldest = swapsCacheMap.keys().next().value;
            swapsCacheMap.delete(oldest);
        }
        res.json(data);
    } catch (e) { res.json({ data: [], total: 0 }); }
});

app.get('/history/transfers/:address', validateAddress, rateLimit(30, 60000), async (req, res) => {
    try {
        const page = parseInt(req.query.page) || 1;
        const limit = Math.min(parseInt(req.query.limit) || 20, 100);
        const data = await getTransfers(req.params.address, page, limit);
        res.json(data);
    } catch (e) {
        console.error(e);
        res.status(500).json({ error: 'Internal server error' });
    }
});

app.get('/history/bridges/:address', validateAddress, rateLimit(30, 60000), async (req, res) => {
    try {
        const page = parseInt(req.query.page) || 1;
        const limit = Math.min(parseInt(req.query.limit) || 20, 100);
        const result = await getWalletBridges(req.params.address, page, limit);

        // Enrich with Asset Info (Symbol, Logo)
        result.data = result.data.map(tx => {
            const asset = getAssetInfo(tx.asset_id);
            return {
                ...tx,
                symbol: asset ? asset.symbol : 'UNK',
                logo: asset ? asset.logo : ''
            };
        });

        res.json(result);
    } catch (e) {
        console.error(e);
        res.status(500).json({ error: 'Internal server error' });
    }
});

app.get('/history/global/bridges', rateLimit(30, 60000), async (req, res) => {
    try {
        const page = parseInt(req.query.page) || 1;
        const limit = Math.min(parseInt(req.query.limit) || 20, 100);

        const result = await getLatestBridges(page, limit, req.query.filter, req.query.timestamp);

        // Enrich with Asset Info (Symbol, Logo)
        result.data = result.data.map(tx => {
            const asset = getAssetInfo(tx.asset_id);
            return {
                ...tx,
                symbol: asset ? asset.symbol : 'UNK',
                logo: asset ? asset.logo : ''
            };
        });

        res.json(result);
    } catch (e) {
        console.error(e);
        res.status(500).json({ error: 'Internal server error' });
    }
});

// --- ORDER BOOK ENDPOINTS ---
app.get('/history/global/orderbook', rateLimit(30, 60000), async (req, res) => {
    try {
        const page = parseInt(req.query.page) || 1;
        const limit = Math.min(parseInt(req.query.limit) || 25, 100);
        const type = req.query.type || null;
        const timestamp = req.query.timestamp ? parseInt(req.query.timestamp) : null;
        const result = await getLatestOrderBookEvents(page, limit, type, timestamp);
        res.json(result);
    } catch (e) {
        console.error('Error /history/global/orderbook:', e);
        res.status(500).json({ error: 'Internal server error' });
    }
});

app.get('/history/orderbook/:address', validateAddress, rateLimit(30, 60000), async (req, res) => {
    try {
        const page = parseInt(req.query.page) || 1;
        const limit = Math.min(parseInt(req.query.limit) || 25, 100);
        const result = await getOrderBookByAddress(req.params.address, page, limit);
        res.json(result);
    } catch (e) {
        console.error('Error /history/orderbook/:address:', e);
        res.status(500).json({ error: 'Internal server error' });
    }
});

// --- EXTRINSICS ENDPOINTS ---
// Caché por params (clave única por combinación). 10s TTL — extrinsics
// nuevos llegan a la live_extrinsics constantemente, pero 10s de stale es
// imperceptible y reduce drásticamente la carga DB cuando varios componentes
// consultan a la vez (network pulse + extrinsics tab + drill panels).
const _extrCache = new Map();
const EXTR_TTL_MS = 10_000;

app.get('/history/global/extrinsics', rateLimit(60, 60000), async (req, res) => {
    try {
        const page = parseInt(req.query.page) || 1;
        const limit = Math.min(parseInt(req.query.limit) || 25, 100);
        const section = req.query.section || null;
        const method = req.query.method || null;
        const timestamp = req.query.timestamp ? parseInt(req.query.timestamp) : null;
        const block = req.query.block ? parseInt(req.query.block) : null;
        const success = req.query.success !== undefined ? parseInt(req.query.success) : null;

        const key = `${page}|${limit}|${section}|${method}|${timestamp}|${block}|${success}`;
        const cached = _extrCache.get(key);
        if (cached && Date.now() - cached.ts < EXTR_TTL_MS) {
            return res.json(cached.data);
        }
        const result = await getLatestExtrinsics(page, limit, section, timestamp, block, success, method);
        _extrCache.set(key, { ts: Date.now(), data: result });
        // Cap cache size — most users hit a few combinations, but no need to
        // hold thousands of stale entries.
        if (_extrCache.size > 200) {
            const firstKey = _extrCache.keys().next().value;
            if (firstKey) _extrCache.delete(firstKey);
        }
        res.json(result);
    } catch (e) {
        console.error('Error /history/global/extrinsics:', e);
        res.status(500).json({ error: 'Internal server error' });
    }
});

app.get('/history/extrinsic-sections', rateLimit(10, 60000), async (req, res) => {
    try {
        res.json(await getExtrinsicSections());
    } catch (e) {
        res.status(500).json({ error: 'Internal server error' });
    }
});

// Real network-wide extrinsic KPIs over a rolling 24h window (count, success
// rate, top pallet) — feeds the Extrinsics page header so it stops showing the
// current page size as if it were the 24h total.
app.get('/stats/extrinsics-24h', rateLimit(30, 60000), async (req, res) => {
    try {
        res.json(await getExtrinsicStats24h());
    } catch (e) {
        console.error('Error /stats/extrinsics-24h:', e.message);
        res.status(500).json({ error: 'Internal server error' });
    }
});

// Real per-block fees for ?blocks=a,b,c. The extrinsics listing has no fee
// column, but live_fees/mv_fees carry the real TransactionFeePaid per block —
// so the table can show the actual fee for the (single) signed extrinsic in
// each block. Returns { [block]: { totalXor, totalUsd, rows } }.
app.get('/history/extrinsic-fees', rateLimit(60, 60000), async (req, res) => {
    try {
        const blocks = String(req.query.blocks || '').split(',')
            .map(s => parseInt(s.trim())).filter(Number.isFinite).slice(0, 100);
        if (blocks.length === 0) return res.json({});
        res.json(await getFeesByBlocks(blocks));
    } catch (e) {
        console.error('Error /history/extrinsic-fees:', e.message);
        res.status(500).json({ error: 'Internal server error' });
    }
});

// USD value lookup for an extrinsic (cross-table search)
app.get('/lookup/usd-value/:extrinsicId', rateLimit(30, 60000), async (req, res) => {
    const exId = req.params.extrinsicId;
    if (!/^\d+-\d+$/.test(exId)) return res.status(400).json({ error: 'Invalid extrinsic ID' });
    try {
        const result = await lookupExtrinsicUsdValue(exId, tokenPrices);
        res.json(result || { usd_value: null });
    } catch (e) {
        res.status(500).json({ error: 'Internal server error' });
    }
});

// Global search endpoint (wallet, hash, extrinsic_id, block)
app.get('/search', rateLimit(20, 60000), async (req, res) => {
    const q = (req.query.q || '').trim();
    if (!q || q.length < 3 || q.length > 128) return res.status(400).json({ error: 'Invalid query' });
    try {
        res.json(await globalSearch(q));
    } catch (e) {
        res.status(500).json({ error: 'Internal server error' });
    }
});

app.get('/history/extrinsics/:address', validateAddress, rateLimit(30, 60000), async (req, res) => {
    try {
        const page = parseInt(req.query.page) || 1;
        const limit = Math.min(parseInt(req.query.limit) || 25, 100);
        const result = await getExtrinsicsByAddress(req.params.address, page, limit);
        res.json(result);
    } catch (e) {
        res.status(500).json({ error: 'Internal server error' });
    }
});

// --- EXTRINSIC DETAIL ENDPOINT (single record with events) ---
app.get('/history/extrinsic/:block/:index', rateLimit(30, 60000), async (req, res) => {
    const block = parseInt(req.params.block);
    const index = parseInt(req.params.index);
    if (isNaN(block) || isNaN(index) || block < 0 || index < 0) {
        return res.status(400).json({ error: 'Invalid block or index' });
    }
    try {
        const result = await getExtrinsicDetail(block, index);
        if (!result) return res.status(404).json({ error: 'Not found' });
        res.json(result);
    } catch (e) {
        console.error('Error /history/extrinsic/:block/:index:', e.message);
        res.status(500).json({ error: 'Internal server error' });
    }
});

// --- IDENTITY RESOLUTION ENDPOINT ---
app.post('/api/identities', rateLimit(30, 60000), async (req, res) => {
    try {
        const { addresses } = req.body;
        if (!addresses || !Array.isArray(addresses) || addresses.length === 0) {
            return res.status(400).json({ error: 'addresses array required' });
        }
        const capped = addresses.filter(a => typeof a === 'string' && a.length > 40 && !a.startsWith('0x')).slice(0, 200);
        if (capped.length === 0) return res.json({});

        const now = Date.now();
        const result = {};
        const toResolve = [];

        for (const addr of capped) {
            const cached = identityMemCache.get(addr);
            if (cached && (now - cached.ts < IDENTITY_MEM_TTL)) {
                if (cached.display) result[addr] = { display: cached.display };
            } else {
                toResolve.push(addr);
            }
        }

        if (toResolve.length > 0) {
            await resolveIdentitiesBatch(toResolve);
            for (const addr of toResolve) {
                const cached = identityMemCache.get(addr);
                if (cached && cached.display) result[addr] = { display: cached.display };
            }
        }

        res.json(result);
    } catch (e) {
        console.error('Error /api/identities:', e.message);
        res.status(500).json({ error: 'Internal server error' });
    }
});

app.get('/history/global/liquidity', rateLimit(30, 60000), async (req, res) => {
    try {
        const page = parseInt(req.query.page) || 1;
        const limit = Math.min(parseInt(req.query.limit) || 20, 100);

        const result = await getLiquidityEvents(page, limit, req.query.timestamp);

        // Enrich with logos
        result.data = result.data.map(ev => {
            const baseAsset = ASSETS.find(a => a.symbol === ev.pool_base);
            const targetAsset = ASSETS.find(a => a.symbol === ev.pool_target);
            return {
                ...ev,
                base_logo: baseAsset ? baseAsset.logo : '',
                target_logo: targetAsset ? targetAsset.logo : ''
            };
        });

        res.json(result);
    } catch (e) {
        console.error(e);
        res.status(500).json({ error: 'Internal server error' });
    }
});

// REMOVED: Duplicate /pool/activity and /pool/providers routes (already defined above)

app.get('/history/swaps/:address', validateAddress, rateLimit(30, 60000), async (req, res) => {
    try {
        res.json(await getSwaps(req.params.address, parseInt(req.query.page) || 1));
    } catch (e) {
        console.error('Error getting swaps for ' + req.params.address, e);
        res.status(500).json({ error: 'Internal server error' });
    }
});
app.get('/chart/:symbol', validateSymbol, rateLimit(30, 60000), async (req, res) => res.json(await getCandles(req.params.symbol, req.query.res || 60)));

// --- CSV EXPORT ENDPOINT ---

const VALID_CSV_TYPES = ['swaps', 'transfers', 'bridges', 'liquidity', 'orderbook', 'extrinsics'];
const VALID_CSV_FORMATS = ['sorametrics', 'koinly', 'cointracking', 'cointracker'];

function csvEsc(v) {
    if (v == null || v === '') return '';
    const s = String(v);
    return (s.includes(',') || s.includes('"') || s.includes('\n')) ? '"' + s.replace(/"/g, '""') + '"' : s;
}

function fmtDateISO(ts) { return ts ? new Date(Number(ts)).toISOString().replace('T', ' ').replace(/\.\d+Z$/, '') : ''; }
function fmtDateUS(ts) {
    if (!ts) return '';
    const d = new Date(Number(ts));
    const mm = String(d.getUTCMonth() + 1).padStart(2, '0');
    const dd = String(d.getUTCDate()).padStart(2, '0');
    const hh = String(d.getUTCHours()).padStart(2, '0');
    const mi = String(d.getUTCMinutes()).padStart(2, '0');
    const ss = String(d.getUTCSeconds()).padStart(2, '0');
    return `${mm}/${dd}/${d.getUTCFullYear()} ${hh}:${mi}:${ss}`;
}

// Determine transfer/bridge direction relative to selected wallets
function getTxDirection(row, walletSet, fromCol, toCol) {
    const fromMatch = walletSet.has(row[fromCol]);
    const toMatch = walletSet.has(row[toCol]);
    if (fromMatch && toMatch) return 'internal';
    if (fromMatch) return 'out';
    return 'in';
}

// Get the wallet address from a row for grouping
function getRowWallet(type, row) {
    if (type === 'swaps' || type === 'liquidity' || type === 'orderbook') return row.wallet;
    if (type === 'transfers') return row.from_addr || row.to_addr;
    if (type === 'bridges') return row.sender || row.recipient;
    if (type === 'extrinsics') return row.signer;
    return '';
}

// SoraMetrics section headers per type
const SM_SECTIONS = {
    swaps: { title: 'SWAPS', header: 'Date,Block,Wallet,In_Token,In_Amount,In_USD,Out_Token,Out_Amount,Out_USD,Hash,Extrinsic_ID',
        row: (r) => [fmtDateISO(r.timestamp), r.block, csvEsc(r.wallet), csvEsc(r.in_symbol), r.in_amount, r.in_usd || '', csvEsc(r.out_symbol), r.out_amount, r.out_usd || '', csvEsc(r.hash), csvEsc(r.extrinsic_id)].join(',') },
    transfers: { title: 'TRANSFERS', header: 'Date,Block,From,To,Token,Amount,USD_Value,Hash,Extrinsic_ID',
        row: (r) => [fmtDateISO(r.timestamp), r.block, csvEsc(r.from_addr), csvEsc(r.to_addr), csvEsc(r.symbol), r.amount, r.usd_value || '', csvEsc(r.hash), csvEsc(r.extrinsic_id)].join(',') },
    bridges: { title: 'BRIDGES', header: 'Date,Block,Network,Direction,From,To,Token,Amount,USD_Value,Hash,Extrinsic_ID',
        row: (r) => [fmtDateISO(r.timestamp), r.block, csvEsc(r.network), csvEsc(r.direction), csvEsc(r.sender), csvEsc(r.recipient), csvEsc(r.symbol), r.amount, r.usd_value || '', csvEsc(r.hash), csvEsc(r.extrinsic_id)].join(',') },
    liquidity: { title: 'LIQUIDITY', header: 'Date,Block,Wallet,Pool_Base,Pool_Target,Base_Amount,Target_Amount,USD_Value,Action,Hash,Extrinsic_ID',
        row: (r) => [fmtDateISO(r.timestamp), r.block, csvEsc(r.wallet), csvEsc(r.pool_base), csvEsc(r.pool_target), r.base_amount, r.target_amount, r.usd_value || '', csvEsc(r.type), csvEsc(r.hash), csvEsc(r.extrinsic_id)].join(',') },
    orderbook: { title: 'ORDER BOOK', header: 'Date,Block,Wallet,Event,Base_Asset,Quote_Asset,Side,Price,Amount,USD_Value,Hash,Extrinsic_ID',
        row: (r) => [fmtDateISO(r.timestamp), r.block, csvEsc(r.wallet), csvEsc(r.event_type), csvEsc(r.base_asset), csvEsc(r.quote_asset), csvEsc(r.side), r.price, r.amount, r.usd_value || '', csvEsc(r.hash), csvEsc(r.extrinsic_id)].join(',') },
    extrinsics: { title: 'EXTRINSICS', header: 'Date,Block,Extrinsic_ID,Signer,Pallet,Method,Result,Hash',
        row: (r) => [fmtDateISO(r.timestamp), r.block, `${r.block}-${r.extrinsic_index}`, csvEsc(r.signer), csvEsc(r.section), csvEsc(r.method), r.success ? 'Success' : 'Failed', csvEsc(r.hash)].join(',') }
};

// --- FORMAT: SoraMetrics (by wallet, detailed) ---
function formatSorametrics(data, types, wallets, walletNames) {
    const lines = [];
    for (let i = 0; i < wallets.length; i++) {
        const addr = wallets[i];
        const name = walletNames[i] || '';
        const label = name ? `${addr.slice(0, 8)}...${addr.slice(-6)} (${name})` : `${addr.slice(0, 8)}...${addr.slice(-6)}`;
        lines.push(`=== WALLET: ${label} ===`);
        const walletSet = new Set([addr]);
        for (const type of types) {
            const allRows = data[type] || [];
            const sec = SM_SECTIONS[type];
            if (!sec) continue;
            // Filter rows belonging to this wallet
            const rows = allRows.filter(r => {
                if (type === 'transfers') return r.from_addr === addr || r.to_addr === addr;
                if (type === 'bridges') return r.sender === addr || r.recipient === addr;
                return getRowWallet(type, r) === addr;
            });
            if (rows.length === 0) continue;
            lines.push(`--- ${sec.title} (${rows.length}) ---`);
            lines.push(sec.header);
            for (const row of rows) lines.push(sec.row(row));
            lines.push('');
        }
        lines.push('');
    }
    return lines.join('\n');
}

// --- FORMAT: Koinly ---
function formatKoinly(data, types, wallets) {
    const walletSet = new Set(wallets);
    const header = 'Date,Sent Amount,Sent Currency,Received Amount,Received Currency,Fee Amount,Fee Currency,Net Worth Amount,Net Worth Currency,TxHash,Description';
    const rows = [header];
    const kRow = (date, sentAmt, sentCur, rcvAmt, rcvCur, nwAmt, nwCur, hash, desc) =>
        [date, sentAmt, csvEsc(sentCur), rcvAmt, csvEsc(rcvCur), '', '', nwAmt, csvEsc(nwCur), csvEsc(hash), csvEsc(desc)].join(',');

    if (types.includes('swaps')) {
        for (const r of (data.swaps || [])) {
            rows.push(kRow(fmtDateISO(r.timestamp), r.out_amount, r.out_symbol, r.in_amount, r.in_symbol,
                r.in_usd || '', 'USD', r.hash, `Swap ${r.out_symbol} -> ${r.in_symbol}`));
        }
    }
    if (types.includes('transfers')) {
        for (const r of (data.transfers || [])) {
            const dir = getTxDirection(r, walletSet, 'from_addr', 'to_addr');
            if (dir === 'out') rows.push(kRow(fmtDateISO(r.timestamp), r.amount, r.symbol, '', '', r.usd_value || '', 'USD', r.hash, 'Transfer out'));
            else if (dir === 'in') rows.push(kRow(fmtDateISO(r.timestamp), '', '', r.amount, r.symbol, r.usd_value || '', 'USD', r.hash, 'Transfer in'));
            else rows.push(kRow(fmtDateISO(r.timestamp), r.amount, r.symbol, '', '', r.usd_value || '', 'USD', r.hash, 'Internal transfer'));
        }
    }
    if (types.includes('bridges')) {
        for (const r of (data.bridges || [])) {
            const dir = getTxDirection(r, walletSet, 'sender', 'recipient');
            if (dir === 'out') rows.push(kRow(fmtDateISO(r.timestamp), r.amount, r.symbol, '', '', r.usd_value || '', 'USD', r.hash, `Bridge out (${r.network})`));
            else if (dir === 'in') rows.push(kRow(fmtDateISO(r.timestamp), '', '', r.amount, r.symbol, r.usd_value || '', 'USD', r.hash, `Bridge in (${r.network})`));
            else rows.push(kRow(fmtDateISO(r.timestamp), r.amount, r.symbol, '', '', r.usd_value || '', 'USD', r.hash, `Bridge internal (${r.network})`));
        }
    }
    if (types.includes('liquidity')) {
        for (const r of (data.liquidity || [])) {
            const isAdd = (r.type || '').toLowerCase().includes('add') || (r.type || '').toLowerCase().includes('deposit');
            const pool = `${r.pool_base}/${r.pool_target}`;
            if (isAdd) {
                if (r.base_amount) rows.push(kRow(fmtDateISO(r.timestamp), r.base_amount, r.pool_base, '', '', '', '', r.hash, `Provide Liquidity ${pool}`));
                if (r.target_amount) rows.push(kRow(fmtDateISO(r.timestamp), r.target_amount, r.pool_target, '', '', '', '', r.hash, `Provide Liquidity ${pool}`));
            } else {
                if (r.base_amount) rows.push(kRow(fmtDateISO(r.timestamp), '', '', r.base_amount, r.pool_base, '', '', r.hash, `Remove Liquidity ${pool}`));
                if (r.target_amount) rows.push(kRow(fmtDateISO(r.timestamp), '', '', r.target_amount, r.pool_target, '', '', r.hash, `Remove Liquidity ${pool}`));
            }
        }
    }
    if (types.includes('orderbook')) {
        for (const r of (data.orderbook || [])) {
            const isBuy = (r.side || '').toLowerCase() === 'buy';
            const quoteAmount = (parseFloat(r.price) || 0) * (parseFloat(r.amount) || 0);
            if (isBuy) rows.push(kRow(fmtDateISO(r.timestamp), quoteAmount || '', r.quote_asset, r.amount, r.base_asset, r.usd_value || '', 'USD', r.hash, `Order Book ${r.event_type}`));
            else rows.push(kRow(fmtDateISO(r.timestamp), r.amount, r.base_asset, quoteAmount || '', r.quote_asset, r.usd_value || '', 'USD', r.hash, `Order Book ${r.event_type}`));
        }
    }
    return rows.join('\n');
}

// --- FORMAT: CoinTracking ---
function formatCoinTracking(data, types, wallets) {
    const walletSet = new Set(wallets);
    const header = '"Type","Buy","Cur.","Sell","Cur.","Fee","Cur.","Exchange","Group","Comment","Date","Tx-ID"';
    const rows = [header];
    const ctRow = (type, buyAmt, buyCur, sellAmt, sellCur, comment, date, txId) =>
        [csvEsc(type), buyAmt, csvEsc(buyCur), sellAmt, csvEsc(sellCur), '', '', '"SORA DEX"', '', csvEsc(comment), csvEsc(date), csvEsc(txId)].join(',');

    if (types.includes('swaps')) {
        for (const r of (data.swaps || [])) {
            rows.push(ctRow('Trade', r.in_amount, r.in_symbol, r.out_amount, r.out_symbol, `Swap on SORA`, fmtDateISO(r.timestamp), r.hash));
        }
    }
    if (types.includes('transfers')) {
        for (const r of (data.transfers || [])) {
            const dir = getTxDirection(r, walletSet, 'from_addr', 'to_addr');
            if (dir === 'out') rows.push(ctRow('Withdrawal', '', '', r.amount, r.symbol, 'Transfer out', fmtDateISO(r.timestamp), r.hash));
            else if (dir === 'in') rows.push(ctRow('Deposit', r.amount, r.symbol, '', '', 'Transfer in', fmtDateISO(r.timestamp), r.hash));
            else rows.push(ctRow('Withdrawal', '', '', r.amount, r.symbol, 'Internal transfer', fmtDateISO(r.timestamp), r.hash));
        }
    }
    if (types.includes('bridges')) {
        for (const r of (data.bridges || [])) {
            const dir = getTxDirection(r, walletSet, 'sender', 'recipient');
            if (dir === 'out') rows.push(ctRow('Withdrawal', '', '', r.amount, r.symbol, `Bridge to ${r.network}`, fmtDateISO(r.timestamp), r.hash));
            else if (dir === 'in') rows.push(ctRow('Deposit', r.amount, r.symbol, '', '', `Bridge from ${r.network}`, fmtDateISO(r.timestamp), r.hash));
            else rows.push(ctRow('Withdrawal', '', '', r.amount, r.symbol, `Bridge internal ${r.network}`, fmtDateISO(r.timestamp), r.hash));
        }
    }
    if (types.includes('liquidity')) {
        for (const r of (data.liquidity || [])) {
            const isAdd = (r.type || '').toLowerCase().includes('add') || (r.type || '').toLowerCase().includes('deposit');
            const pool = `${r.pool_base}/${r.pool_target}`;
            if (isAdd) {
                if (r.base_amount) rows.push(ctRow('Provide Liquidity', '', '', r.base_amount, r.pool_base, `Add LP ${pool}`, fmtDateISO(r.timestamp), r.hash));
                if (r.target_amount) rows.push(ctRow('Provide Liquidity', '', '', r.target_amount, r.pool_target, `Add LP ${pool}`, fmtDateISO(r.timestamp), r.hash));
            } else {
                if (r.base_amount) rows.push(ctRow('Remove Liquidity', r.base_amount, r.pool_base, '', '', `Remove LP ${pool}`, fmtDateISO(r.timestamp), r.hash));
                if (r.target_amount) rows.push(ctRow('Remove Liquidity', r.target_amount, r.pool_target, '', '', `Remove LP ${pool}`, fmtDateISO(r.timestamp), r.hash));
            }
        }
    }
    if (types.includes('orderbook')) {
        for (const r of (data.orderbook || [])) {
            const isBuy = (r.side || '').toLowerCase() === 'buy';
            const quoteAmount = (parseFloat(r.price) || 0) * (parseFloat(r.amount) || 0);
            if (isBuy) rows.push(ctRow('Trade', r.amount, r.base_asset, quoteAmount || '', r.quote_asset, `Order Book ${r.event_type}`, fmtDateISO(r.timestamp), r.hash));
            else rows.push(ctRow('Trade', quoteAmount || '', r.quote_asset, r.amount, r.base_asset, `Order Book ${r.event_type}`, fmtDateISO(r.timestamp), r.hash));
        }
    }
    return rows.join('\n');
}

// --- FORMAT: CoinTracker ---
function formatCoinTracker(data, types, wallets) {
    const walletSet = new Set(wallets);
    const header = 'Date,Received Quantity,Received Currency,Sent Quantity,Sent Currency,Fee Amount,Fee Currency,Tag';
    const rows = [header];
    const ckRow = (date, rcvAmt, rcvCur, sentAmt, sentCur, tag) =>
        [date, rcvAmt, csvEsc(rcvCur), sentAmt, csvEsc(sentCur), '', '', csvEsc(tag)].join(',');

    if (types.includes('swaps')) {
        for (const r of (data.swaps || [])) {
            rows.push(ckRow(fmtDateUS(r.timestamp), r.in_amount, r.in_symbol, r.out_amount, r.out_symbol, ''));
        }
    }
    if (types.includes('transfers')) {
        for (const r of (data.transfers || [])) {
            const dir = getTxDirection(r, walletSet, 'from_addr', 'to_addr');
            if (dir === 'out') rows.push(ckRow(fmtDateUS(r.timestamp), '', '', r.amount, r.symbol, ''));
            else if (dir === 'in') rows.push(ckRow(fmtDateUS(r.timestamp), r.amount, r.symbol, '', '', ''));
            else rows.push(ckRow(fmtDateUS(r.timestamp), '', '', r.amount, r.symbol, ''));
        }
    }
    if (types.includes('bridges')) {
        for (const r of (data.bridges || [])) {
            const dir = getTxDirection(r, walletSet, 'sender', 'recipient');
            if (dir === 'out') rows.push(ckRow(fmtDateUS(r.timestamp), '', '', r.amount, r.symbol, ''));
            else if (dir === 'in') rows.push(ckRow(fmtDateUS(r.timestamp), r.amount, r.symbol, '', '', ''));
            else rows.push(ckRow(fmtDateUS(r.timestamp), '', '', r.amount, r.symbol, ''));
        }
    }
    if (types.includes('liquidity')) {
        for (const r of (data.liquidity || [])) {
            const isAdd = (r.type || '').toLowerCase().includes('add') || (r.type || '').toLowerCase().includes('deposit');
            if (isAdd) {
                if (r.base_amount) rows.push(ckRow(fmtDateUS(r.timestamp), '', '', r.base_amount, r.pool_base, ''));
                if (r.target_amount) rows.push(ckRow(fmtDateUS(r.timestamp), '', '', r.target_amount, r.pool_target, ''));
            } else {
                if (r.base_amount) rows.push(ckRow(fmtDateUS(r.timestamp), r.base_amount, r.pool_base, '', '', ''));
                if (r.target_amount) rows.push(ckRow(fmtDateUS(r.timestamp), r.target_amount, r.pool_target, '', '', ''));
            }
        }
    }
    if (types.includes('orderbook')) {
        for (const r of (data.orderbook || [])) {
            const isBuy = (r.side || '').toLowerCase() === 'buy';
            const quoteAmount = (parseFloat(r.price) || 0) * (parseFloat(r.amount) || 0);
            if (isBuy) rows.push(ckRow(fmtDateUS(r.timestamp), r.amount, r.base_asset, quoteAmount || '', r.quote_asset, ''));
            else rows.push(ckRow(fmtDateUS(r.timestamp), quoteAmount || '', r.quote_asset, r.amount, r.base_asset, ''));
        }
    }
    return rows.join('\n');
}

const CSV_FORMATTERS = { sorametrics: formatSorametrics, koinly: formatKoinly, cointracking: formatCoinTracking, cointracker: formatCoinTracker };
const CSV_FILENAMES = { sorametrics: 'sorametrics_export', koinly: 'koinly_import', cointracking: 'cointracking_import', cointracker: 'cointracker_import' };

app.get('/export/csv', rateLimit(10, 60000), async (req, res) => {
    try {
        const wallets = (req.query.wallets || '').split(',').filter(w => VALID_SS58.test(w));
        const types = (req.query.types || '').split(',').filter(t => VALID_CSV_TYPES.includes(t));
        const format = VALID_CSV_FORMATS.includes(req.query.format) ? req.query.format : 'sorametrics';
        const walletNames = (req.query.walletNames || '').split(',');
        const startTs = parseInt(req.query.start) || 0;
        const endTs = parseInt(req.query.end) || Date.now();

        if (wallets.length === 0) return res.status(400).json({ error: 'No valid wallets' });
        if (wallets.length > 50) return res.status(400).json({ error: 'Max 50 wallets' });
        if (types.length === 0) return res.status(400).json({ error: 'No valid types' });
        if (startTs >= endTs) return res.status(400).json({ error: 'Invalid date range' });

        // Tax formats ignore extrinsics (no financial value)
        const effectiveTypes = format === 'sorametrics' ? types : types.filter(t => t !== 'extrinsics');
        const data = await getExportData({ wallets, types: effectiveTypes, startTs, endTs, limit: 50000 });
        const formatter = CSV_FORMATTERS[format];
        const csv = formatter(data, effectiveTypes, wallets, walletNames);
        const dateStr = new Date().toISOString().slice(0, 10);
        const filename = `${CSV_FILENAMES[format]}_${dateStr}.csv`;

        res.setHeader('Content-Type', 'text/csv; charset=utf-8');
        res.setHeader('Content-Disposition', `attachment; filename="${filename}"`);
        res.send('\uFEFF' + csv);
    } catch (e) {
        console.error('CSV export error:', e.message);
        res.status(500).json({ error: 'Export failed' });
    }
});

// --- SORA INTELLIGENCE ENDPOINTS ---

app.get('/stats/accumulation', rateLimit(15, 60000), async (req, res) => {
    const symbol = req.query.symbol || 'XOR';
    const timeframe = req.query.timeframe || '24h'; // 1h, 4h, 24h, 7d, 30d

    const ms = TIMEFRAME_MS[timeframe] || 86400000;

    try {
        const data = await getTopAccumulators(symbol, ms);
        res.json({ symbol, timeframe, data });
    } catch (e) {
        res.status(500).json({ error: 'Internal server error' });
    }
});

app.get('/stats/network', rateLimit(20, 60000), async (req, res) => {
    try {

        // Snapshot de 24h
        const stats24h = await getNetworkStats(86400000);
        // Snapshot de 7d
        const stats7d = await getNetworkStats(604800000);

        // TPS (Transacciones en las últimas 24h / segundos en un día) -> Promedio burdo
        // Para TPS Real, deberíamos tomar los últimos X bloques, pero esto sirve de media diaria.
        const tps = stats24h.txCount / 86400;

        res.json({
            stats24h,
            stats7d,
            tps: tps.toFixed(2)
        });
    } catch (e) {
        res.status(500).json({ error: 'Internal server error' });
    }
});

// Cache + parallel fetch — overview was 10s cold (4 sequential DB queries
// without cache). Each timeframe gets its own bucket. 30s TTL is fine since
// the dashboard is meant to be a snapshot, not real-time.
const _overviewCache = new Map();
const OVERVIEW_TTL_MS = 30_000;

app.get('/stats/overview', rateLimit(60, 60000), async (req, res) => {
    try {
        const timeframe = req.query.timeframe || '1d';
        const ms = TIMEFRAME_MS[timeframe] || 86400000;

        // Cache key per timeframe so /stats/overview?timeframe=1d and ?timeframe=7d
        // dont share entries.
        const cached = _overviewCache.get(timeframe);
        if (cached && Date.now() - cached.ts < OVERVIEW_TTL_MS) {
            return res.json(cached.data);
        }

        const kusdPeg = tokenPrices['KUSD'] || 0;
        const xstusdPeg = tokenPrices['XSTUSD'] || 0;
        const tbcdPeg = tokenPrices['TBCD'] || 0;

        // Parallelise the 4 DB queries (was 4× sequential, ~10s cold).
        const [netStats, lpVolume, transferVolume, trends] = await Promise.all([
            getNetworkStats(ms).catch(() => ({})),
            getLpVolume(ms).catch(() => 0),
            getTransferVolume(ms).catch(() => 0),
            getMarketTrends(ms).catch(() => ({})),
        ]);

        const data = {
            pegs: { KUSD: kusdPeg, XSTUSD: xstusdPeg, TBCD: tbcdPeg },
            network: { ...netStats, lpVolume, transferVolume },
            trends,
        };
        _overviewCache.set(timeframe, { ts: Date.now(), data });
        res.json(data);
    } catch (e) {
        console.error('Error /stats/overview:', e.message);
        res.status(500).json({ error: 'Internal server error' });
    }
});

app.get('/stats/header', rateLimit(30, 60000), async (req, res) => {
    try {
        const timeframe = req.query.timeframe || '1d';
        const ms = TIMEFRAME_MS[timeframe];
        const startTime = (ms === undefined || ms === 0) ? 0 : (Date.now() - ms);

        const stats = await getFilteredStats(startTime);

        res.json({
            block: sessionStats.block,
            swaps: stats.swaps,
            transfers: stats.transfers,
            bridges: stats.bridges
        });
    } catch (e) {
        res.status(500).json({ error: 'Internal server error' });
    }
});

// Live fee config read from chain storage. Cached 60s so every page load
// doesn't hammer the WS node. Exposes the two multipliers (congestion +
// governance-set), the current remint accumulators, RemintPeriod, and real
// fee-details samples per extrinsic class. SORA fees are flat per class
// (transfer = swap = 0.1 XOR currently, bridge = 1 XOR) — NOT linear in
// weight, confirmed by live probe. The governance multiplier changes via
// runtime upgrade / council motion so the widget can show it explicitly.
//
// Runtime 4.8.3 (spec 124+) introduced a non-zero TransactionByteFee
// (0.0000001 XOR/byte). It's a `pub const` in common/src/weights.rs, NOT
// exposed via api.consts.transactionPayment, so we hardcode it gated by
// specVersion. queryFeeDetails decomposes the partial fee into base /
// length / adjustedWeight so the frontend can show what each component
// contributes (the length component is new in 4.8.3, was zero before).
let _feeConfigCache = { ts: 0, data: null };
const FEE_CONFIG_TTL = 15_000;
// Hardcoded — runtime constants in common/src/weights.rs not exposed
// via metadata. Bumped automatically by gating on specVersion.
const TRANSACTION_BYTE_FEE_BY_SPEC = (sv) => sv >= 124 ? 1e-7 : 0;
async function computeFeeConfig() {
    const api = await initApi();
    const XOR = '0x0200000000000000000000000000000000000000000000000000000000000000';
    const VAL = '0x0200040000000000000000000000000000000000000000000000000000000000';
    const dummy = 'cnVS46aLyfRHTossU1ZEXaw6Eok1Lk9NeMdhJsSNzp7ywJLEq';
    const hexToXor = (hex) => Number(BigInt(String(hex))) / 1e18;
    const big = (v) => Number(BigInt(v.toString())) / 1e18;

    const [nfm, xfm, remintPeriod, xorToVal, xorToBuyBack, head] = await Promise.all([
        api.query.transactionPayment.nextFeeMultiplier(),
        api.query.xorFee.multiplier(),
        api.query.xorFee.remintPeriod(),
        api.query.xorFee.xorToVal(),
        api.query.xorFee.xorToBuyBack(),
        api.rpc.chain.getHeader(),
    ]);
    const specVersion = api?.runtimeVersion?.specVersion?.toNumber?.() ?? 0;
    const transactionByteFee = TRANSACTION_BYTE_FEE_BY_SPEC(specVersion);

    // We can't decompose the inclusion fee client-side: SORA routes most
    // calls through XorFee::CustomFees, which bypasses the standard
    // TransactionPayment path. queryFeeDetails returns inclusionFee=null
    // for custom-fee calls. So we report the total (paymentInfo) plus
    // the byte size, and let the UI show the TransactionByteFee constant
    // separately. The standard path applies len × TransactionByteFee but
    // ONLY for non-custom extrinsics (governance, staking, etc.).
    const sample = async (buildTx) => {
        try {
            const tx = buildTx();
            const callHex = tx.toHex();
            const len = (callHex.length - 2) / 2; // bytes
            const info = await tx.paymentInfo(dummy);
            const ij = info.toJSON();
            return {
                fee: hexToXor(ij.partialFee),
                weightRefTime: Number(ij.weight.refTime) || 0,
                class: ij.class,
                lenBytes: len,
                hypotheticalLenFee: len * transactionByteFee, // what the standard path WOULD add
            };
        } catch (e) { return null; }
    };
    const samples = {};
    samples.transfer = await sample(() => api.tx.assets.transfer(XOR, dummy, '1000000000000000000'));
    if (api.tx.liquidityProxy?.swap) {
        samples.swap = await sample(() => api.tx.liquidityProxy.swap(
            0, XOR, VAL,
            { WithDesiredInput: { desired_amount_in: '1000000000000000000', min_amount_out: '0' } },
            ['XYKPool'], 'Disabled'
        ));
    }
    if (api.tx.ethBridge?.transferToSidechain) {
        samples.bridge = await sample(() => api.tx.ethBridge.transferToSidechain(
            XOR, '0x0000000000000000000000000000000000000000',
            '1000000000000000000', 0
        ));
    }

    return {
        specVersion,
        nextFeeMultiplier: big(nfm),
        xorFeeMultiplier: big(xfm),
        xorToValXor: big(xorToVal),
        xorToBuyBackXor: big(xorToBuyBack),
        remintPeriodBlocks: Number(remintPeriod.toString()) || 0,
        transactionByteFee,
        samples,
        asOfBlock: Number(head.number.toString()) || 0,
        asOfTs: Date.now(),
    };
}

app.get('/stats/fee-config', rateLimit(30, 60000), async (req, res) => {
    try {
        const now = Date.now();
        if (!_feeConfigCache.data || now - _feeConfigCache.ts > FEE_CONFIG_TTL) {
            _feeConfigCache = { ts: now, data: await computeFeeConfig() };
        }
        res.json(_feeConfigCache.data);
    } catch (e) {
        console.error('Error /stats/fee-config:', e.message);
        res.status(500).json({ error: 'Internal server error' });
    }
});

// /stats/fee-burns-live?window=<1h|4h|6h|24h|7d>
//
// Real burn-from-fees stats, counting from when the indexer started
// (fee_burns_indexer.js subscribes to finalized heads). Aggregates over
// sm.fee_burns_live, which only has rows for blocks with actual fee
// activity, so this is cheap. 15s cache.
//
// Returns:
//   fees.totalXor              total XOR paid in fees during the window
//   referrer.paidXor           sum of ReferrerRewarded amounts
//   referrer.redirectedToKusdXor  amount that fell through (no referrer)
//                                 and was redirected to the KUSD bucket
//   burns.{xor,val,kusd}       amounts burned, in each token natives
//   weights                    runtime weights used for the period
//
// "live" mode is handled fully on the frontend from /stats/fee-config
// accumulators — no backend roundtrip for that case.
const _feeBurnsLiveCache = {};
const FEE_BURNS_LIVE_TTL = 15_000;
const FEE_BURNS_WINDOWS = {
    "1h":   3600,
    "4h":  14400,
    "6h":  21600,
    "24h": 86400,
    "7d": 604800,
    "30d": 2592000,
};

app.get("/stats/fee-burns-live", rateLimit(60, 60000), async (req, res) => {
    const window = String(req.query.window || "24h");
    const seconds = FEE_BURNS_WINDOWS[window];
    if (!seconds) {
        return res.status(400).json({ error: "Invalid window. Use 1h, 4h, 6h, 24h, 7d." });
    }
    try {
        const now = Date.now();
        const cached = _feeBurnsLiveCache[window];
        if (cached && now - cached.ts < FEE_BURNS_LIVE_TTL) {
            return res.json(cached.data);
        }
        const sums = await getFeeBurnsWindow(seconds);
        // Runtime weights — 4.7.x: 10/20/50/20=100; 4.8.2-4.8.4: 10/20/50/5=85; 4.8.6: 10/35/40/0=85.
        const sv = api?.runtimeVersion?.specVersion?.toNumber?.() ?? 0;
        const weights = sv >= 128
            ? { ref: 10, xor: 35, val: 40, kusd: 0,  total: 85 }
            : sv >= 120
            ? { ref: 10, xor: 20, val: 50, kusd: 5,  total: 85 }
            : { ref: 10, xor: 20, val: 50, kusd: 20, total: 100 };
        const data = {
            window,
            startTime: now - seconds * 1000,
            endTime: now,
            seconds,
            weights,
            fees: {
                totalXor: Number(sums.fees_paid_xor) || 0,
            },
            referrer: {
                paidXor:               Number(sums.ref_paid_xor)        || 0,
                redirectedToKusdXor:   Number(sums.ref_redirected_xor)  || 0,
            },
            burns: {
                xor:  Number(sums.remint_xor_burned)  || 0,
                val:  Number(sums.remint_val_burned)  || 0,
                kusd: Number(sums.remint_kusd_burned) || 0,
            },
            blocks:  Number(sums.rows)   || 0,
            firstTs: Number(sums.min_ts) || 0,
            lastTs:  Number(sums.max_ts) || 0,
        };
        _feeBurnsLiveCache[window] = { ts: now, data };
        res.json(data);
    } catch (e) {
        console.error("[fee-burns-live]", e.message);
        res.status(500).json({ error: "Internal server error" });
    }
});


// ════════════════════════════════════════════════════════════════════
// RESTORED BLOCK — endpoints + helpers lost between 22-abr and 25-abr 2026
// Source: index.js.bak.pre-v2-static.20260422073652 (verified working)
// Restored 2026-04-25 by reinjection (no DB changes, no behavioral diffs)
// ════════════════════════════════════════════════════════════════════


// --- ARCHIVE_WS_ENDPOINT + getArchiveApi ---
const ARCHIVE_WS_ENDPOINT = process.env.ARCHIVE_WS_ENDPOINT || 'wss://mof2.sora.org';
let _archiveApi = null;
async function getArchiveApi() {
    if (_archiveApi && _archiveApi.isConnected) return _archiveApi;
    const provider = new _WsProvider(ARCHIVE_WS_ENDPOINT, 2500);
    _archiveApi = await _ApiPromise.create(_soraOptions({ provider }));
    await _archiveApi.isReady;
    return _archiveApi;
}


// --- TECH_ACCOUNTS_TTL + buildTechAccountsMap + /api/tech-accounts ---
let _techAccountsCache = { ts: 0, data: null };
const TECH_ACCOUNTS_TTL = 6 * 60 * 60 * 1000; // 6h

async function buildTechAccountsMap() {
    if (!api || !api.isConnected) throw new Error("api_not_ready");
    const rows = await api.query.technical.techAccounts.entries();
    const out = {};
    for (const [key, val] of rows) {
        const addr = key.args[0].toString();
        const h = val.toHuman();
        const outerKey = Object.keys(h || {})[0];
        let label = "Technical";
        try {
            const v = h[outerKey];
            if (outerKey === "Generic" && Array.isArray(v)) {
                // ["pallet-name","sub-key"] → "pallet-name/sub-key"
                label = v.filter(x => typeof x === "string").join("/") || outerKey;
            } else if (outerKey === "Pure" && Array.isArray(v) && typeof v[1] === "object") {
                const inner = Object.keys(v[1])[0];
                label = inner || outerKey;
            } else if (outerKey === "Identifier") {
                label = (Array.isArray(v) ? v.join("/") : String(v)) || outerKey;
            } else {
                label = outerKey;
            }
        } catch (_) {}
        out[addr] = label;
    }
    return out;
}

app.get("/api/tech-accounts", rateLimit(30, 60000), async (req, res) => {
    try {
        const now = Date.now();
        if (!_techAccountsCache.data || (now - _techAccountsCache.ts) > TECH_ACCOUNTS_TTL) {
            const data = await buildTechAccountsMap();
            _techAccountsCache = { ts: now, data };
        }
        res.set("Cache-Control", "public, max-age=3600"); // client hint: 1h
        res.json(_techAccountsCache.data);
    } catch (e) {
        console.error("Error /api/tech-accounts:", e.message);
        res.status(500).json({ error: "tech_accounts_unavailable" });
    }
});


// --- /mof/qty/:symbol ---
app.get('/mof/qty/:symbol', rateLimit(60, 60000), async (req, res) => {
    try {
        const sym = String(req.params.symbol || '').toLowerCase().replace(/[^a-z0-9_\-]/g, '');
        if (!sym) return res.status(400).send('bad_symbol');
        const controller = new AbortController();
        const timeout = setTimeout(() => controller.abort(), 5000);
        const upstream = await fetch('https://mof.sora.org/qty/' + encodeURIComponent(sym), { signal: controller.signal });
        clearTimeout(timeout);
        if (!upstream.ok) return res.status(upstream.status).send(await upstream.text().catch(() => ''));
        res.set('Content-Type', 'text/plain; charset=utf-8');
        res.set('Cache-Control', 'public, max-age=300');
        res.send((await upstream.text()).trim());
    } catch (e) {
        res.status(502).send('mof_unavailable');
    }
});


// --- governance/preimage/* + scheduler/agenda + /block/:n + getPreimageDb ---
// SORA's preimage pallet exposes TWO storage maps depending on pallet version:
//   - statusFor          (v1 API, legacy)        body fields: deposit, count, len
//   - requestStatusFor   (v2 API, current)       body fields: maybeTicket, count, maybeLen
// Bytes presence is authoritative ONLY via preimageFor((hash, len)) — the
// status maps carry metadata, not the actual call payload. Reading just
// statusFor like the old code did caused false "missing preimage" alerts for
// every v2-pallet preimage on chain (e.g. runtime 4.8.6 enactment 2026-05-28).
async function readPreimageState(hash, knownLen) {
    if (!api || !api.query.preimage) return null;
    let kind = null;
    let body = {};
    if (api.query.preimage.requestStatusFor) {
        try {
            const st = await withTimeout(api.query.preimage.requestStatusFor(hash), 5000);
            if (st && !st.isNone) {
                const j = st.unwrap().toJSON();
                if (j && typeof j === 'object') {
                    kind = Object.keys(j)[0];
                    body = j[kind] && typeof j[kind] === 'object' ? j[kind] : {};
                }
            }
        } catch {}
    }
    if (!kind && api.query.preimage.statusFor) {
        try {
            const st = await withTimeout(api.query.preimage.statusFor(hash), 5000);
            if (st && !st.isNone) {
                const j = st.unwrap().toJSON();
                if (j && typeof j === 'object') {
                    kind = Object.keys(j)[0];
                    body = j[kind] && typeof j[kind] === 'object' ? j[kind] : {};
                }
            }
        } catch {}
    }
    const len = body.len ?? body.maybeLen ?? knownLen ?? null;
    let bytesAvailable = false;
    if (len != null && api.query.preimage.preimageFor) {
        try {
            const pf = await withTimeout(api.query.preimage.preimageFor([hash, len]), 5000);
            bytesAvailable = !!(pf && pf.isSome);
        } catch {}
    }
    const ticket = body.deposit || body.ticket || body.maybeTicket || null;
    return {
        status: kind,
        len,
        count: body.count ?? null,
        depositor: Array.isArray(ticket) ? ticket[0] : null,
        deposit: Array.isArray(ticket) && ticket[1] !== undefined ? ticket[1] : null,
        bytesAvailable
    };
}

app.get("/governance/preimages", rateLimit(10, 60000), async (req, res) => {
    try {
        if (!api) return res.json({ error: "API not connected" });
        const hasOld = !!(api.query.preimage && api.query.preimage.statusFor);
        const hasNew = !!(api.query.preimage && api.query.preimage.requestStatusFor);
        if (!hasOld && !hasNew) {
            return res.json({ preimages: [], identities: {} });
        }
        const seen = new Map();
        async function enumerate(storage) {
            const entries = await withTimeout(storage.entries());
            for (const [key, statusOpt] of entries) {
                if (!statusOpt || statusOpt.isNone) continue;
                const hash = key.args[0].toHex();
                if (seen.has(hash)) continue;
                const raw = statusOpt.unwrap().toJSON();
                const kind = raw && typeof raw === 'object' ? Object.keys(raw)[0] : null;
                const body = kind && raw[kind] && typeof raw[kind] === 'object' ? raw[kind] : {};
                seen.set(hash, { kind, body });
            }
        }
        if (hasNew) await enumerate(api.query.preimage.requestStatusFor);
        if (hasOld) await enumerate(api.query.preimage.statusFor);
        const preimages = [];
        const addresses = new Set();
        for (const [hash, { kind, body }] of seen) {
            const statusName = kind ? (kind[0].toUpperCase() + kind.slice(1)) : 'Unknown';
            const ticket = body.deposit || body.ticket || body.maybeTicket || null;
            const depositor = Array.isArray(ticket) ? ticket[0] : null;
            const deposit = Array.isArray(ticket) && ticket[1] !== undefined ? ticket[1] : null;
            if (depositor) addresses.add(depositor);
            preimages.push({
                hash,
                status: statusName,
                len: body.len ?? body.maybeLen ?? null,
                count: body.count ?? null,
                depositor,
                deposit: deposit !== null ? String(deposit) : null
            });
        }
        // Enrich each preimage with first-seen block + timestamp from the
        // dedicated preimage indexer SQLite. This powers the "Subida" column
        // and chronological sorting (most recent on top).
        try {
            const db = getPreimageDb();
            if (db) {
                const stmt = db.prepare(
                    "SELECT block_height AS block, timestamp FROM preimage_events " +
                    "WHERE hash = ? AND method = 'Noted' ORDER BY block_height ASC LIMIT 1"
                );
                for (const p of preimages) {
                    try {
                        const row = stmt.get(p.hash);
                        if (row) {
                            p.firstSeenBlock = row.block;
                            p.firstSeenTimestamp = row.timestamp;
                        } else {
                            p.firstSeenBlock = null;
                            p.firstSeenTimestamp = null;
                        }
                    } catch { /* best effort */ }
                }
            }
        } catch { /* indexer not ready, skip enrichment */ }

        // Sort: entries with a known first-seen timestamp first (newest on top),
        // then everything else by status (Requested / Unrequested / Unknown).
        preimages.sort((a, b) => {
            const at = a.firstSeenTimestamp || 0;
            const bt = b.firstSeenTimestamp || 0;
            if (at !== bt) return bt - at;
            const rank = s => s === 'Requested' ? 0 : s === 'Unrequested' ? 1 : 2;
            return rank(a.status) - rank(b.status);
        });
        const identities = await attachIdentities([...addresses]);
        res.json({ preimages, identities });
    } catch (e) {
        res.status(500).json({ error: e.message });
    }
});

// Scheduler agenda — lists all scheduled calls in upcoming blocks, flagging
// entries whose referenced preimage bytes are NOT stored on-chain. Critical
// for catching runtime upgrades that will silently fail.
app.get("/governance/scheduler/agenda", rateLimit(10, 60000), async (req, res) => {
    try {
        if (!api) return res.status(503).json({ error: "API not connected" });
        if (!api.query.scheduler || !api.query.scheduler.agenda) {
            return res.json({ entries: [], tip: null });
        }
        const head = await withTimeout(api.rpc.chain.getHeader(), 10000);
        const tip = head.number.toNumber();
        const agendaEntries = await withTimeout(api.query.scheduler.agenda.entries(), 30000);

        const missingHashes = new Set();
        const rawList = [];
        for (const [key, value] of agendaEntries) {
            const blockAt = key.args[0].toNumber();
            if (blockAt < tip) continue; // past
            const scheduled = value.toJSON() || [];
            for (let i = 0; i < scheduled.length; i++) {
                const s = scheduled[i];
                if (!s) continue;
                let lookupHash = null;
                let lookupLen = null;
                let inlineCall = null;
                const call = s.call;
                if (call && typeof call === 'object') {
                    const lookup = call.lookup || call.Lookup;
                    if (lookup) {
                        lookupHash = (lookup.hash_ || lookup.hash || '').toLowerCase();
                        lookupLen = lookup.len ?? null;
                    }
                    const inline = call.inline || call.Inline;
                    if (inline) {
                        inlineCall = inline;
                    }
                }
                rawList.push({
                    block: blockAt,
                    slot: i,
                    maybeId: s.maybeId || null,
                    priority: s.priority ?? null,
                    origin: s.origin || null,
                    lookupHash,
                    lookupLen,
                    inlineCall,
                    maybePeriodic: s.maybePeriodic || null
                });
                if (lookupHash) missingHashes.add(lookupHash);
            }
        }

        // Check preimage availability for all referenced lookup hashes.
        // Uses readPreimageState which queries both pallet APIs (v1 statusFor +
        // v2 requestStatusFor) and validates bytes presence with preimageFor.
        // Passing lookupLen lets preimageFor work even if metadata is missing.
        const lenByHash = {};
        for (const r of rawList) {
            if (r.lookupHash && r.lookupLen != null) lenByHash[r.lookupHash] = r.lookupLen;
        }
        const hashStatus = {};
        for (const h of missingHashes) {
            try {
                const state = await readPreimageState(h, lenByHash[h]);
                hashStatus[h] = state || { status: 'none', bytesAvailable: false };
            } catch { hashStatus[h] = { status: 'error', bytesAvailable: false }; }
        }

        // Decode inline calls where possible
        const entries = rawList.map(r => {
            let decoded = null;
            if (r.inlineCall) {
                try { decoded = decodeProposal(api.createType('Call', r.inlineCall)); } catch {}
            }
            const blocksRemaining = Math.max(0, r.block - tip);
            const secondsRemaining = blocksRemaining * 6;
            const preimage = r.lookupHash ? hashStatus[r.lookupHash] : null;
            const alert = r.lookupHash && preimage && !preimage.bytesAvailable;
            return {
                block: r.block,
                blocksRemaining,
                secondsRemaining,
                slot: r.slot,
                maybeId: r.maybeId,
                priority: r.priority,
                origin: r.origin,
                lookupHash: r.lookupHash,
                lookupLen: r.lookupLen,
                inlineDecoded: decoded,
                preimage,
                alert: !!alert
            };
        }).sort((a, b) => a.block - b.block);

        res.json({ tip, entries });
    } catch (e) {
        res.status(500).json({ error: e.message });
    }
});

// Block explorer — returns header + extrinsics + events + logs for a given
// block number. Used by the "Block #N" modal (Subscan-style view). Reads
// directly from the archive node, no indexer needed.
app.get("/block/:n", rateLimit(600, 60000), async (req, res) => {
    try {
        const n = parseInt(req.params.n);
        if (!Number.isFinite(n) || n < 0) return res.status(400).json({ error: 'Invalid block number' });
        const archive = await withTimeout(getArchiveApi(), 30000);
        const blockHash = await withTimeout(archive.rpc.chain.getBlockHash(n), 15000);
        const [signedBlock, events, runtimeVersion] = await Promise.all([
            withTimeout(archive.rpc.chain.getBlock(blockHash), 15000),
            withTimeout(archive.query.system.events.at(blockHash), 15000),
            withTimeout(archive.rpc.state.getRuntimeVersion(blockHash), 10000).catch(() => null)
        ]);
        let timestamp = null;
        try {
            const tsRaw = await withTimeout(archive.query.timestamp.now.at(blockHash), 8000);
            timestamp = typeof tsRaw.toNumber === 'function' ? tsRaw.toNumber() : Number(tsRaw.toString());
        } catch {}

        const header = signedBlock.block.header;
        const extrinsics = signedBlock.block.extrinsics;

        // Map events per extrinsic (phase)
        const extrinsicEvents = new Array(extrinsics.length).fill(null).map(() => []);
        const inherentEvents = [];

        // ── Enrichment helpers ──────────────────────────────────────────
        // Decode a Substrate DispatchError shape into { section, name, docs }.
        // Accepts:
        //   { module: { index, error } }   — toJSON convention (lower-case)
        //   { Module: { index, error } }   — enum-style
        //   { BadOrigin: null }, "CannotLookup"  — simple variants
        const decodeDispatchError = (err) => {
            try {
                if (!err) return null;
                if (typeof err === 'string') return { section: null, name: err, docs: '' };
                if (typeof err !== 'object') return null;
                const mod = err.module || err.Module;
                if (mod && mod.index != null && mod.error != null) {
                    const meta = archive.registry.findMetaError({
                        index: archive.registry.createType('u8', mod.index),
                        error: archive.registry.createType('Bytes', mod.error).toU8a(true)
                    });
                    return {
                        section: meta.section,
                        name: meta.name,
                        docs: (meta.docs || []).map(d => String(d).trim()).filter(Boolean).join(' ')
                    };
                }
                // Token / Arithmetic / Other simple-enum errors:
                //   { Token: 'BelowMinimum' }, { Arithmetic: 'Overflow' }, etc.
                const k = Object.keys(err).find(x => x !== 'module' && x !== 'Module');
                if (k) {
                    const sub = err[k];
                    return { section: null, name: typeof sub === 'string' ? `${k}.${sub}` : k, docs: '' };
                }
            } catch (_) {}
            return null;
        };

        // Walk any value tree and return the first DispatchError-like found.
        // Handles Result<_, DispatchError> shapes ({ Err: <...> }) and direct
        // DispatchError shapes nested anywhere — covers BatchInterrupted,
        // ItemFailed, ProxyExecuted, MultisigExecuted, scheduler.Dispatched, etc.
        const findEmbeddedError = (obj) => {
            if (!obj || typeof obj !== 'object') return null;
            if ('Err' in obj) {
                const dec = decodeDispatchError(obj.Err);
                if (dec) return dec;
            }
            if (obj.module || obj.Module) {
                const dec = decodeDispatchError(obj);
                if (dec) return dec;
            }
            for (const v of Array.isArray(obj) ? obj : Object.values(obj)) {
                const sub = findEmbeddedError(v);
                if (sub) return sub;
            }
            return null;
        };

        // Format a u128 (or u64/u32) raw value with the given decimals.
        // Accepts hex string ("0x…"), number, or BigInt-coercible string.
        const formatTokenAmount = (raw, decimals) => {
            if (raw == null) return null;
            let bn;
            try {
                if (typeof raw === 'string') bn = BigInt(raw.startsWith('0x') ? raw : raw);
                else bn = BigInt(raw);
            } catch { return null; }
            if (bn === 0n) return '0';
            const neg = bn < 0n; const abs = neg ? -bn : bn;
            const div = 10n ** BigInt(decimals);
            const intPart = (abs / div).toString();
            const fracDigits = (abs % div).toString().padStart(decimals, '0');
            const fracTrimmed = fracDigits.replace(/0+$/, '').slice(0, 6);
            const grouped = intPart.replace(/\B(?=(\d{3})+(?!\d))/g, ',');
            return (neg ? '-' : '') + grouped + (fracTrimmed ? '.' + fracTrimmed : '');
        };

        // Sections whose Balance fields are unambiguously XOR (the native
        // balances pallet on SORA tracks XOR; everything else is multi-asset
        // and the symbol must be inferred from a sibling AssetId field).
        const XOR_NATIVE_SECTIONS = new Set([
            'balances', 'xorFee', 'transactionPayment', 'staking', 'session'
        ]);

        // Per-event enrichment: walks all fields once, then resolves Balance
        // tickers using sibling AssetId fields (or falls back to XOR for
        // native sections). Returns array of decoded field descriptors.
        const enrichEventFields = (record) => {
            const sec = String(record.event.section || '');
            const fields = record.event.meta?.fields || [];
            const data = record.event.data;
            const registry = getAssetIdToInfo();

            // SORA AssetId32 toJSON()s as { code: "0x…" } (a wrapped 32-byte
            // hex), but other multi-asset enums (T::CurrencyId in `tokens.*`)
            // serialize as plain hex strings or as enum-shaped objects like
            // { Token: 'XOR' }. Normalize all of those to a registry key.
            const normalizeAssetIdJson = (v) => {
                if (v == null) return null;
                if (typeof v === 'string') return v.toLowerCase();
                if (typeof v === 'object') {
                    if (typeof v.code === 'string') return v.code.toLowerCase();
                    // CurrencyId enum: { Token: 'XOR' } — caller must resolve
                    // by symbol; we can't without a symbol→info map handy, so
                    // we return the inner string for caller to handle.
                    const k = Object.keys(v)[0];
                    if (k && typeof v[k] === 'string') return v[k];
                }
                return null;
            };

            // Helper: resolve a registry entry from an AssetId/CurrencyId
            // value (handles { code: '0x…' }, plain hex string, or enum like
            // { Token: 'XOR' }).
            const resolveAssetInfo = (rawJson) => {
                const idKey = normalizeAssetIdJson(rawJson);
                if (!idKey) return null;
                let info = registry[idKey];
                if (!info) {
                    const upper = idKey.toUpperCase();
                    for (const [, v] of Object.entries(registry)) {
                        if (v.symbol && v.symbol.toUpperCase() === upper) { info = v; break; }
                    }
                }
                return info || null;
            };

            // Pass 1: collect every AssetId/CurrencyId resolved value, in
            // order. Events like liquidityProxy.Exchange have 2 assets + 2
            // balances and we want to pair them positionally.
            const assetSequence = [];      // ordered list of resolved {symbol, decimals}
            for (let i = 0; i < data.length; i++) {
                const tn = (fields[i]?.typeName?.toString() || '').toLowerCase();
                if (/(asset.?id|technicalassetid|currency.?id)/.test(tn)) {
                    let info = null;
                    try { info = resolveAssetInfo(data[i].toJSON()); } catch {}
                    assetSequence.push(info);
                }
            }
            // Default ticker = first resolved asset (for events with 1 asset
            // and 1 balance). For multi-balance events we pair positionally:
            // the k-th Balance uses the k-th AssetId (capped at the last).
            const defaultAsset = assetSequence.find(a => a) || null;
            let siblingTicker = defaultAsset?.symbol || null;
            let siblingDecimals = defaultAsset?.decimals || 18;
            const isXorNative = XOR_NATIVE_SECTIONS.has(sec);

            const decoded = [];
            let balanceSlot = 0;  // counter for positional Balance↔AssetId pairing
            for (let i = 0; i < data.length; i++) {
                const field = fields[i];
                const typeName = (field?.typeName?.toString() || '').toLowerCase();
                const fieldName = field?.name?.toString() || null;
                let raw;
                try { raw = data[i].toJSON(); } catch { raw = null; }
                let human = null;

                if (/balance|^u128$/.test(typeName) && raw != null) {
                    let ticker, decimals;
                    if (isXorNative) {
                        ticker = 'XOR'; decimals = 18;
                    } else {
                        // Pair the k-th Balance with the k-th AssetId (or
                        // fall back to the default if we ran out).
                        const paired = assetSequence[balanceSlot] || defaultAsset;
                        ticker = paired?.symbol || siblingTicker || null;
                        decimals = paired?.decimals || siblingDecimals;
                        balanceSlot++;
                    }
                    const formatted = formatTokenAmount(raw, decimals);
                    if (formatted != null) human = ticker ? `${formatted} ${ticker}` : formatted;
                } else if (/(asset.?id|technicalassetid|currency.?id)/.test(typeName) && raw) {
                    const info = resolveAssetInfo(raw);
                    if (info) human = info.symbol;
                } else if (typeName.includes('weight') && raw && typeof raw === 'object' && raw.refTime != null) {
                    const ms = Number(raw.refTime) / 1e9;
                    human = `${ms.toFixed(2)}ms · ${raw.proofSize}b`;
                } else if (typeName.includes('dispatcherror') || typeName.includes('dispatchresult')) {
                    const dec = findEmbeddedError(raw);
                    if (dec) human = `${dec.section ? dec.section + '.' : ''}${dec.name}${dec.docs ? ' — ' + dec.docs : ''}`;
                }

                decoded.push({
                    name: fieldName,
                    type: field?.type?.toString() || null,
                    typeName: field?.typeName?.toString() || null,
                    value: raw,
                    human
                });
            }
            return decoded;
        };

        events.forEach((record, idx) => {
            const phase = record.phase;
            const dataJson = record.event.data.toJSON();

            // Per-field decoded view (amounts, asset IDs, weights, etc.).
            // Resolves Balance tickers using sibling AssetId field when present.
            let decoded = [];
            try { decoded = enrichEventFields(record); } catch (_) {}

            // Embedded error (covers any event whose data tree contains a
            // DispatchError — ExtrinsicFailed, BatchInterrupted, ProxyExecuted,
            // MultisigExecuted, scheduler.Dispatched, etc.).
            const embeddedError = findEmbeddedError(dataJson);

            const evOut = {
                index: idx,
                section: record.event.section,
                method: record.event.method,
                data: dataJson,
                decoded
            };
            if (embeddedError) evOut.decodedError = embeddedError;

            if (phase && phase.isApplyExtrinsic) {
                const ei = phase.asApplyExtrinsic.toNumber();
                if (extrinsicEvents[ei]) extrinsicEvents[ei].push(evOut); else inherentEvents.push(evOut);
            } else {
                inherentEvents.push(evOut);
            }
        });

        const extList = extrinsics.map((ext, i) => {
            const evs = extrinsicEvents[i] || [];
            const successEv = evs.find(e => e.section === 'system' && e.method === 'ExtrinsicSuccess');
            const failEv = evs.find(e => e.section === 'system' && e.method === 'ExtrinsicFailed');
            let args = null;
            try { args = ext.method.args.map(a => a.toJSON()); } catch {}
            return {
                index: i,
                hash: ext.hash.toHex(),
                signer: ext.isSigned ? ext.signer.toString() : null,
                section: ext.method.section,
                method: String(ext.method.method),
                args,
                success: successEv ? true : (failEv ? false : null),
                events: evs
            };
        });

        const logs = (header.digest && header.digest.logs ? header.digest.logs : []).map(l => {
            try { return l.toJSON(); } catch { return String(l); }
        });

        res.json({
            number: n,
            hash: blockHash.toHex(),
            parentHash: header.parentHash.toHex(),
            stateRoot: header.stateRoot.toHex(),
            extrinsicsRoot: header.extrinsicsRoot.toHex(),
            timestamp,
            specVersion: runtimeVersion ? runtimeVersion.specVersion.toNumber() : null,
            specName: runtimeVersion ? runtimeVersion.specName.toString() : null,
            extrinsics: extList,
            inherentEvents,
            logs,
            totalExtrinsics: extList.length,
            totalEvents: events.length,
            source: ARCHIVE_WS_ENDPOINT
        });
    } catch (e) {
        res.status(500).json({ error: e.message });
    }
});

// Fast preimage events lookup — reads from the dedicated SQLite indexer
// maintained by preimage_indexer.js. Returns sub-100ms for any hash in the
// indexer's coverage window (default: last 60 days).
const _preimageDbPath = require('path').join(__dirname, 'preimage_index.db');
let _preimageDb = null;
function getPreimageDb() {
    if (_preimageDb) return _preimageDb;
    try {
        const BetterSqlite3 = require('better-sqlite3');
        _preimageDb = new BetterSqlite3(_preimageDbPath, { readonly: true, fileMustExist: true });
        _preimageDb.pragma('journal_mode = WAL');
    } catch (e) {
        _preimageDb = null;
    }
    return _preimageDb;
}

app.get("/governance/preimage/:hash/events-fast", rateLimit(1200, 60000), async (req, res) => {
    try {
        const hash = String(req.params.hash || '').toLowerCase();
        if (!/^0x[0-9a-f]{64}$/.test(hash)) return res.status(400).json({ error: 'Invalid hash' });
        const db = getPreimageDb();
        if (!db) {
            return res.status(503).json({
                error: 'Preimage indexer not ready yet. The dedicated indexer DB has not been created. It will populate in the background.',
                indexerReady: false
            });
        }
        const from = parseInt(req.query.from);
        const to = parseInt(req.query.to);
        let q = 'SELECT block_height AS block, event_index, timestamp, method, hash, data, reason, reason_detail FROM preimage_events WHERE hash = ?';
        const params = [hash];
        if (Number.isFinite(from)) { q += ' AND block_height >= ?'; params.push(from); }
        if (Number.isFinite(to)) { q += ' AND block_height <= ?'; params.push(to); }
        q += ' ORDER BY block_height ASC, event_index ASC';
        const rows = db.prepare(q).all(...params);

        const state = {};
        try {
            const sr = db.prepare('SELECT key, value FROM indexer_state').all();
            for (const r of sr) state[r.key] = r.value;
        } catch {}

        const events = rows.map(r => ({
            block: r.block,
            event: 'preimage.' + r.method,
            hash: r.hash,
            timestamp: r.timestamp,
            data: r.data ? (() => { try { return JSON.parse(r.data); } catch { return r.data; } })() : null,
            reason: r.reason || null,
            reason_detail: r.reason_detail || null
        }));
        res.json({
            hash,
            count: events.length,
            events,
            indexer: {
                liveLastBlock: state.live_last_block ? parseInt(state.live_last_block) : null,
                backfillCursor: state.backfill_cursor ? parseInt(state.backfill_cursor) : null,
                backfillComplete: !!state.backfill_complete_at
            }
        });
    } catch (e) {
        res.status(500).json({ error: e.message });
    }
});

// Scan preimage pallet events for a given hash in [from, to] block range.
// Uses the archive node (mof2) to access historical state. Parallel workers.
// Returns a timeline of Preimage.Noted / Requested / Cleared / Unnoted events.
const _preimageEventsCache = new Map();
app.get("/governance/preimage/:hash/events", rateLimit(3, 60000), async (req, res) => {
    try {
        if (!api) return res.status(503).json({ error: "API not connected" });
        const targetHash = String(req.params.hash || '').toLowerCase();
        if (!/^0x[0-9a-f]{64}$/.test(targetHash)) return res.status(400).json({ error: 'Invalid hash' });

        const head = await withTimeout(api.rpc.chain.getHeader(), 10000);
        const tip = head.number.toNumber();
        const MAX_RANGE = 800; // bounded to fit under Cloudflare 100s timeout
        const reqFrom = parseInt(req.query.from);
        const reqTo = parseInt(req.query.to);
        const to = Number.isFinite(reqTo) ? Math.min(reqTo, tip) : tip;
        const from = Number.isFinite(reqFrom) ? Math.max(0, reqFrom) : Math.max(0, to - MAX_RANGE + 1);
        if (to < from) return res.status(400).json({ error: 'to < from' });
        if ((to - from + 1) > MAX_RANGE) {
            return res.status(400).json({
                error: `range too large (${to - from + 1}), max=${MAX_RANGE}. Narrow to/from.`,
                maxRange: MAX_RANGE
            });
        }

        const cacheKey = targetHash + ':' + from + ':' + to;
        if (_preimageEventsCache.has(cacheKey)) {
            return res.json(_preimageEventsCache.get(cacheKey));
        }

        const archive = await withTimeout(getArchiveApi(), 30000);
        const CONCURRENCY = 30;
        let next = to;
        let scanned = 0;
        const hits = [];
        async function proc(n) {
            try {
                const h = await withTimeout(archive.rpc.chain.getBlockHash(n), 15000);
                const events = await withTimeout(archive.query.system.events.at(h), 15000);
                let hitThisBlock = false;
                for (const r of events) {
                    const ev = r.event;
                    if (ev.section !== 'preimage') continue;
                    const hashField = ev.data[0]?.toHex?.() || String(ev.data[0]);
                    if (hashField.toLowerCase() !== targetHash) continue;
                    hitThisBlock = true;
                    hits.push({
                        block: n,
                        event: ev.section + '.' + ev.method,
                        data: ev.data.toJSON()
                    });
                }
                if (hitThisBlock) {
                    try {
                        const ts = await withTimeout(archive.query.timestamp.now.at(h), 8000);
                        const tsMs = typeof ts.toNumber === 'function' ? ts.toNumber() : Number(ts.toString());
                        for (const hit of hits) if (hit.block === n && !hit.timestamp) hit.timestamp = tsMs;
                    } catch {}
                }
            } catch { /* skip */ }
            scanned++;
        }
        const workers = [];
        for (let i = 0; i < CONCURRENCY; i++) {
            workers.push((async () => { while (next >= from) { const b = next--; await proc(b); } })());
        }
        await Promise.all(workers);

        hits.sort((a, b) => a.block - b.block);
        const payload = {
            hash: targetHash,
            from,
            to,
            tip,
            scanned,
            count: hits.length,
            events: hits,
            archive: ARCHIVE_WS_ENDPOINT
        };
        _preimageEventsCache.set(cacheKey, payload);
        res.json(payload);
    } catch (e) {
        res.status(500).json({ error: e.message });
    }
});

// Find which referendums reference a given preimage hash (ongoing + finished).
// Scans democracy.referendumInfoOf for the last N referendums and extracts
// the preimage hash each references. Returns any that match the query hash.
app.get("/governance/preimage/:hash/referendums", rateLimit(10, 60000), async (req, res) => {
    try {
        if (!api) return res.status(503).json({ error: "API not connected" });
        const targetHash = String(req.params.hash || '').toLowerCase();
        if (!/^0x[0-9a-f]{64}$/.test(targetHash)) return res.status(400).json({ error: 'Invalid hash' });
        const limit = Math.min(parseInt(req.query.limit) || 50, 200);

        const count = (await withTimeout(api.query.democracy.referendumCount())).toNumber();
        const from = Math.max(0, count - limit);
        const matches = [];
        for (let i = count - 1; i >= from; i--) {
            try {
                const info = await withTimeout(api.query.democracy.referendumInfoOf(i));
                if (!info || info.isNone) continue;
                const data = info.toJSON();
                const status = data ? Object.keys(data)[0] : 'unknown';
                const detail = data ? data[status] : {};
                let refHash = null;
                const prop = detail && detail.proposal;
                if (prop && typeof prop === 'object') {
                    const lookup = prop.lookup || prop.Lookup;
                    if (lookup) refHash = (lookup.hash_ || lookup.hash || '').toLowerCase();
                } else if (typeof prop === 'string') {
                    refHash = prop.toLowerCase();
                }
                if (refHash === targetHash) {
                    matches.push({ id: i, status, detail });
                }
            } catch { /* skip */ }
        }
        res.json({ hash: targetHash, scanned: count - from, fromId: from, toId: count - 1, matches });
    } catch (e) {
        res.status(500).json({ error: e.message });
    }
});

// Decode a single preimage by hash + len
app.get("/governance/preimage/:hash", rateLimit(600, 60000), async (req, res) => {
    try {
        if (!api) return res.json({ error: "API not connected" });
        const hash = String(req.params.hash || '');
        const len = parseInt(req.query.len) || 0;
        if (!/^0x[0-9a-f]{64}$/i.test(hash)) return res.status(400).json({ error: 'Invalid hash' });
        const decoded = await resolvePreimage(hash, len);
        res.json({ hash, len, decoded });
    } catch (e) {
        res.status(500).json({ error: e.message });
    }
});

// Recover preimage bytes by hash from recent indexed blocks.
// Useful when a preimage has been unnoted/cleared but was uploaded within the
// subsquid rolling window (~30 days). Scans live_extrinsics for notePreimage
// blocks, then RPC-fetches each block to derive blake2_256(bytes) and match.
const _preimageRecoverCache = new Map();
app.get("/governance/preimage/recover/:hash", rateLimit(10, 60000), async (req, res) => {
    try {
        if (!api) return res.status(503).json({ error: "API not connected" });
        const targetHash = String(req.params.hash || '').toLowerCase();
        if (!/^0x[0-9a-f]{64}$/.test(targetHash)) return res.status(400).json({ error: 'Invalid hash' });

        if (_preimageRecoverCache.has(targetHash)) {
            return res.json(_preimageRecoverCache.get(targetHash));
        }

        const pg = new _PgPool({
            host: process.env.PG_HOST || 'localhost',
            port: parseInt(process.env.PG_PORT) || 23798,
            database: process.env.PG_DB || 'squid',
            user: process.env.PG_USER || 'postgres',
            password: process.env.PG_PASS || 'squid',
            max: 2,
            connectionTimeoutMillis: 5000
        });
        let rows = [];
        try {
            const r = await pg.query(
                "SELECT DISTINCT block, formatted_time FROM sm.live_extrinsics " +
                "WHERE section = 'preimage' AND method IN ('notePreimage','notePreimageOperational') " +
                "ORDER BY block DESC"
            );
            rows = r.rows;
        } finally {
            pg.end().catch(() => {});
        }

        const archive = await withTimeout(getArchiveApi(), 30000);
        for (const row of rows) {
            try {
                const blockHash = await withTimeout(archive.rpc.chain.getBlockHash(row.block), 20000);
                const signedBlock = await withTimeout(archive.rpc.chain.getBlock(blockHash), 20000);
                for (const ext of signedBlock.block.extrinsics) {
                    const section = ext.method.section;
                    const method = ext.method.method;
                    if (section !== 'preimage' || !String(method).toLowerCase().startsWith('notepreimage')) continue;
                    const bytes = ext.method.args[0].toHex();
                    if (blake2AsHex(bytes, 256).toLowerCase() !== targetHash) continue;
                    let decoded = null;
                    try { decoded = decodeProposal(archive.createType('Call', bytes)); } catch {}
                    const payload = {
                        found: true,
                        hash: targetHash,
                        bytes,
                        bytesLen: (bytes.length - 2) / 2,
                        decoded,
                        block: row.block,
                        timestamp: row.formatted_time,
                        signer: ext.signer.toString(),
                        extrinsicMethod: section + '.' + method,
                        source: 'subsquid index + archive RPC (' + ARCHIVE_WS_ENDPOINT + ')'
                    };
                    _preimageRecoverCache.set(targetHash, payload);
                    return res.json(payload);
                }
            } catch { /* skip block and continue */ }
        }

        res.json({
            found: false,
            hash: targetHash,
            scannedBlocks: rows.length,
            message: 'Not found in indexed notePreimage extrinsics (rolling ~30 days). Older preimages require an archive-node scan.'
        });
    } catch (e) {
        res.status(500).json({ error: e.message });
    }
});

// List all notePreimage extrinsics indexed in subsquid (rolling ~30d),
// with their derived blake2_256 hash and decoded call. Useful for auditing
// "what was uploaded recently" without knowing the hash in advance.
app.get("/governance/preimages/indexed", rateLimit(5, 60000), async (req, res) => {
    try {
        if (!api) return res.status(503).json({ error: "API not connected" });
        const pg = new _PgPool({
            host: process.env.PG_HOST || 'localhost',
            port: parseInt(process.env.PG_PORT) || 23798,
            database: process.env.PG_DB || 'squid',
            user: process.env.PG_USER || 'postgres',
            password: process.env.PG_PASS || 'squid',
            max: 2,
            connectionTimeoutMillis: 5000
        });
        let rows = [];
        try {
            const r = await pg.query(
                "SELECT DISTINCT block, formatted_time FROM sm.live_extrinsics " +
                "WHERE section = 'preimage' AND method IN ('notePreimage','notePreimageOperational') " +
                "ORDER BY block DESC"
            );
            rows = r.rows;
        } finally {
            pg.end().catch(() => {});
        }
        const archive = await withTimeout(getArchiveApi(), 30000);
        const notes = [];
        for (const row of rows) {
            try {
                const blockHash = await withTimeout(archive.rpc.chain.getBlockHash(row.block), 20000);
                const signedBlock = await withTimeout(archive.rpc.chain.getBlock(blockHash), 20000);
                for (const ext of signedBlock.block.extrinsics) {
                    if (ext.method.section !== 'preimage') continue;
                    if (!String(ext.method.method).toLowerCase().startsWith('notepreimage')) continue;
                    const bytes = ext.method.args[0].toHex();
                    const hash = blake2AsHex(bytes, 256);
                    let decoded = null;
                    try { decoded = decodeProposal(archive.createType('Call', bytes)); } catch {}
                    let stillOnChain = false;
                    try {
                        const state = await readPreimageState(hash, (bytes.length - 2) / 2);
                        stillOnChain = !!(state && state.status);
                    } catch {}
                    notes.push({
                        block: row.block,
                        timestamp: row.formatted_time,
                        extrinsicMethod: ext.method.section + '.' + ext.method.method,
                        signer: ext.signer.toString(),
                        hash,
                        bytes,
                        bytesLen: (bytes.length - 2) / 2,
                        decoded,
                        stillOnChain
                    });
                }
            } catch (e) {
                notes.push({ block: row.block, error: e.message });
            }
        }
        res.json({ count: notes.length, notes });
    } catch (e) {
        res.status(500).json({ error: e.message });
    }
});


// --- decode-pretty IIFE ---
// ==================== DECODE PRETTY (runtime-upgrade enrichment) ====================
// GET /governance/preimage/:hash/decode-pretty[?len=N]
// Returns an enriched decode for runtime-upgrade preimages (system.setCode /
// setCodeWithoutChecks): decompresses the zstd-wrapped WASM, pulls specVersion
// from the custom section, and checks blake2_256 integrity. For non-upgrade
// calls returns the generic decoded args. Cached 1h by hash.
// Inserted automatically — uses globals already in index.js (fs, blake2AsHex, api, rateLimit).
{
  const { execFileSync } = require('child_process');
  const osModule = require('os');
  const pathModule = require('path');

  const SUBSTRATE_ZSTD_MAGIC = '52bc537646db8e05';
  const WASM_MAGIC = '0061736d01000000';
  const prettyCache = new Map();
  const PRETTY_CACHE_MAX = 32;
  const PRETTY_CACHE_TTL_MS = 60 * 60 * 1000;

  function prettyCacheGet(hash) {
    const entry = prettyCache.get(hash);
    if (!entry) return null;
    if (Date.now() - entry.ts > PRETTY_CACHE_TTL_MS) { prettyCache.delete(hash); return null; }
    return entry.value;
  }
  function prettyCacheSet(hash, value) {
    if (prettyCache.size >= PRETTY_CACHE_MAX) {
      const firstKey = prettyCache.keys().next().value;
      if (firstKey !== undefined) prettyCache.delete(firstKey);
    }
    prettyCache.set(hash, { ts: Date.now(), value });
  }

  function readCompact(buf, offset) {
    const first = buf[offset];
    const mode = first & 0x03;
    if (mode === 0) return [first >> 2, 1];
    if (mode === 1) return [((first | (buf[offset + 1] << 8)) >> 2), 2];
    if (mode === 2) {
      const v = first | (buf[offset + 1] << 8) | (buf[offset + 2] << 16) | (buf[offset + 3] << 24);
      return [v >>> 2, 4];
    }
    return [null, 1];
  }

  function extractRuntimeVersion(wasm) {
    const NAME = Buffer.from('runtime_version', 'utf8');
    let off = wasm.indexOf(NAME);
    while (off >= 0) {
      if (off >= 1 && wasm[off - 1] === NAME.length) {
        let cursor = off + NAME.length;
        const [specNameLen, n1] = readCompact(wasm, cursor);
        if (specNameLen == null) { off = wasm.indexOf(NAME, off + 1); continue; }
        cursor += n1;
        const specName = wasm.slice(cursor, cursor + specNameLen).toString('utf8');
        cursor += specNameLen;
        const [implNameLen, n2] = readCompact(wasm, cursor);
        if (implNameLen == null) { off = wasm.indexOf(NAME, off + 1); continue; }
        cursor += n2;
        const implName = wasm.slice(cursor, cursor + implNameLen).toString('utf8');
        cursor += implNameLen;
        const authoringVersion = wasm.readUInt32LE(cursor); cursor += 4;
        const specVersion      = wasm.readUInt32LE(cursor); cursor += 4;
        const implVersion      = wasm.readUInt32LE(cursor); cursor += 4;
        if (specName === 'sora-substrate' && specVersion > 0 && specVersion < 100000) {
          return { specName, implName, authoringVersion, specVersion, implVersion };
        }
      }
      off = wasm.indexOf(NAME, off + 1);
    }
    return null;
  }

  function decompressZstd(compressedBytes) {
    const tmpIn  = pathModule.join(osModule.tmpdir(), `sm-preimage-${Date.now()}-${process.pid}.zst`);
    const tmpOut = tmpIn.replace(/\.zst$/, '.wasm');
    try {
      fs.writeFileSync(tmpIn, compressedBytes);
      execFileSync('zstd', ['-d', '-f', '-q', tmpIn, '-o', tmpOut]);
      return fs.readFileSync(tmpOut);
    } finally {
      try { fs.unlinkSync(tmpIn); } catch (_) {}
      try { fs.unlinkSync(tmpOut); } catch (_) {}
    }
  }

  app.get('/governance/preimage/:hash/decode-pretty', rateLimit(60, 60000), async (req, res) => {
    try {
      if (!api) return res.status(503).json({ error: 'api_not_connected' });
      const hash = String(req.params.hash || '').toLowerCase();
      if (!/^0x[0-9a-f]{64}$/.test(hash)) return res.status(400).json({ error: 'bad_hash' });
      const len = parseInt(req.query.len, 10) || 0;

      const cached = prettyCacheGet(hash);
      if (cached) { res.set('X-Cache', 'HIT'); return res.json(cached); }

      let rawHex = null;
      if (api.query.preimage && api.query.preimage.preimageFor) {
        const p = await api.query.preimage.preimageFor([hash, len]);
        if (p && !p.isNone) rawHex = p.unwrap().toHex();
      }
      if (!rawHex && api.query.democracy && api.query.democracy.preimages) {
        const prop = await api.query.democracy.preimages(hash);
        if (prop && !prop.isNone) {
          const available = prop.unwrap();
          if (available.isAvailable) rawHex = available.asAvailable.data.toHex();
        }
      }
      if (!rawHex) return res.status(404).json({ error: 'preimage_not_found', hash });

      const derived = blake2AsHex(rawHex, 256);
      const integrity = derived.toLowerCase() === hash ? 'match' : 'mismatch';

      const call = api.createType('Call', rawHex);
      const section = call.section;
      const method  = call.method;
      const isRuntimeUpgrade = section === 'system' && (method === 'setCode' || method === 'setCodeWithoutChecks');

      if (!isRuntimeUpgrade) {
        const payload = {
          hash, kind: 'generic', section, method,
          args: call.toHuman().args || {}, integrity,
        };
        prettyCacheSet(hash, payload);
        return res.json(payload);
      }

      const codeBytes = Buffer.from(call.args[0].toU8a(true));
      const head8 = codeBytes.slice(0, 8).toString('hex');
      if (head8 !== SUBSTRATE_ZSTD_MAGIC) {
        const payload = {
          hash, kind: 'runtime_upgrade', section, method,
          compressedBytes: codeBytes.length,
          wasmMagicOk: head8 === WASM_MAGIC,
          integrity, note: 'not_zstd_wrapped',
        };
        prettyCacheSet(hash, payload);
        return res.json(payload);
      }

      const zstdPayload = codeBytes.slice(8);
      const wasm = decompressZstd(zstdPayload);
      const wasmMagicOk = wasm.slice(0, 8).toString('hex') === WASM_MAGIC;
      const target = extractRuntimeVersion(wasm);

      const head = await api.rpc.state.getRuntimeVersion();
      const current = {
        specName:         head.specName.toString(),
        specVersion:      head.specVersion.toNumber(),
        implVersion:      head.implVersion.toNumber(),
        authoringVersion: head.authoringVersion.toNumber(),
      };

      const payload = {
        hash, kind: 'runtime_upgrade', section, method,
        current, target,
        compressedBytes:   codeBytes.length,
        decompressedBytes: wasm.length,
        wasmMagicOk, integrity,
      };
      prettyCacheSet(hash, payload);
      res.set('X-Cache', 'MISS');
      return res.json(payload);
    } catch (e) {
      console.error('[decode-pretty] error:', (e && e.stack) || e);
      return res.status(500).json({ error: 'decode_failed', message: String((e && e.message) || e) });
    }
  });
}
// ==================== END DECODE PRETTY ====================

// ============================================================
// POLKAMARKT endpoints — prediction markets (runtime ≥ 4.8.x)
// ============================================================
//
// Every endpoint does feature-detection first:
//   · `api.query.polkamarkt` absent → pallet not in current runtime, we
//     return { available: false, reason } so the frontend can show the
//     "Coming soon" card instead of rendering empty tables or errors.
//   · Present → query the local mirror populated by the live indexer.
//
// The mirror (sm.polkamarkt_markets / _trades / _claims) stays empty until
// the pallet emits events, so these endpoints are safe to expose now.

function pmAvailable() {
    return !!(api && api.query && api.query.polkamarkt);
}
function pmUnavailable() {
    return {
        available: false,
        reason: 'Polkamarkt pallet not active in current runtime (needs SORA runtime ≥ 4.8.x via on-chain governance)',
    };
}

// --- spec-130 live market pricing (DynamicPariMutuel) ----------------------
// The 4.8.8 rewrite exposes polkamarktAPI.marketState(id), returning the implied
// probability + marginal prices per market. We surface it so the UI shows the
// REAL market probability instead of the collateral split (which reads ~100%
// when one side is thin). Per-market cache, short TTL — markets move per trade.
const PM_STATE_TTL = 20 * 1000;
const pmStateCache = new Map(); // id -> { ts, state }

// Parse a polkadot-js toJSON numeric (small → JS number, big → "0x…" hex string)
// into a BigInt. Used for on-chain Balance/share fields.
function pmToBig(x) {
    if (x == null) return 0n;
    if (typeof x === 'number') return BigInt(Math.trunc(x));
    try { return BigInt(String(x)); } catch { return 0n; }
}

function pmRuntimeApiReady() {
    return !!(api && api.call && api.call.polkamarktAPI && api.call.polkamarktAPI.marketState);
}

async function pmGetLiveStates(ids) {
    const now = Date.now();
    const out = {};
    const stale = [];
    for (const id of ids) {
        const c = pmStateCache.get(id);
        if (c && now - c.ts < PM_STATE_TTL) out[id] = c.state;
        else stale.push(id);
    }
    if (stale.length && pmRuntimeApiReady()) {
        await Promise.all(stale.map(async (id) => {
            try {
                const r = await api.call.polkamarktAPI.marketState(id);
                const j = r && r.toJSON ? r.toJSON() : null;
                if (j) {
                    const state = {
                        mechanism: j.mechanism != null ? String(j.mechanism) : null,
                        impliedYesBps: Number(j.impliedYesProbabilityBps) || 0,
                        impliedNoBps: Number(j.impliedNoProbabilityBps) || 0,
                        marginalYesBps: Number(j.marginalYesPriceBps) || 0,
                        marginalNoBps: Number(j.marginalNoPriceBps) || 0,
                        dpmCollateral: pmToBig(j.dpmCollateral).toString(),
                        virtualDepth: pmToBig(j.virtualDepth).toString(),
                    };
                    pmStateCache.set(id, { ts: now, state });
                    out[id] = state;
                }
            } catch (e) { /* leave this market without live state */ }
        }));
    }
    return out;
}

// Enrich a market detail with per-trader P&L, creator commission, and liquidity
// providers — all read from the spec-130 on-chain storage. Mutates `detail`.
//   · Open/Locked: positions from chain (marketPositions); value = shares marked
//     at the implied probability (a share redeems 1 KUSD iff its outcome wins).
//   · Resolved:   chain zeroes positions, so use our event-sourced trades; value
//     = winning shares × 1 KUSD (payout).
//   · Cancelled:  collateral refunded → value = paid, pnl = 0.
async function pmEnrichMarketDetail(marketId, detail, liveState) {
    if (!detail || !detail.market) return;
    const q = api.query.polkamarkt;
    const market = detail.market;
    const status = String(market.status || '');
    const resolution = market.resolution;
    const iy = BigInt(liveState ? liveState.impliedYesBps : 0);
    const ino = BigInt(liveState ? liveState.impliedNoBps : 0);

    // creator commission accrued (trade fees routed to the market creator)
    try {
        const fees = (await q.marketCreatorFees(marketId)).toString();
        detail.creator = { address: market.creator, feesRaw: pmToBig(fees).toString(), feeBps: 50 };
    } catch (e) { console.error('[pm] creator fees read:', e.message); }

    // liquidity providers + pool size
    try {
        let providers = [];
        if (q.liquidityPositions) {
            const lps = await q.liquidityPositions.entries(marketId);
            providers = lps.map(([k, v]) => {
                const j = v.toJSON ? v.toJSON() : {};
                return {
                    account: k.args[1].toString(),
                    shares: pmToBig(j.shares ?? j.lpShares).toString(),
                    contributed: pmToBig(j.collateralContributed ?? j.contributed).toString(),
                };
            });
        }
        let totals = null;
        if (q.liquidityPositionTotals) {
            const t = (await q.liquidityPositionTotals(marketId)).toJSON() || {};
            totals = {
                totalShares: pmToBig(t.totalShares).toString(),
                totalContributed: pmToBig(t.totalCollateralContributed).toString(),
            };
        }
        detail.liquidity = {
            providers, totals,
            dpmCollateral: liveState ? liveState.dpmCollateral : null,
            seed: market.seed_liquidity != null ? String(market.seed_liquidity) : '0',
        };
    } catch (e) { console.error('[pm] liquidity read:', e.message); }

    // per-trader positions + P&L
    try {
        let rows = [];
        if (status === 'Open' || status === 'Locked') {
            const entries = await q.marketPositions.entries(marketId);
            for (const [k, v] of entries) {
                const j = v.toJSON ? v.toJSON() : {};
                const yes = pmToBig(j.yesShares), no = pmToBig(j.noShares), paid = pmToBig(j.netCollateralPaid);
                const value = (yes * iy + no * ino) / 10000n; // mark at implied probability
                rows.push({ trader: k.args[1].toString(), yes_shares: yes.toString(), no_shares: no.toString(),
                            paid: paid.toString(), value: value.toString(), pnl: (value - paid).toString(), basis: 'mark' });
            }
        } else {
            // Resolved/Cancelled: chain positions are zeroed → use our trades history.
            for (const p of (detail.topPositions || [])) {
                const yes = pmToBig(p.yes_shares), no = pmToBig(p.no_shares), paid = pmToBig(p.net_collateral);
                let value, basis;
                if (status === 'Resolved') {
                    value = resolution === 'Yes' ? yes : (resolution === 'No' ? no : 0n); basis = 'settled';
                } else { value = paid; basis = 'refunded'; } // Cancelled → refund
                rows.push({ trader: p.trader, yes_shares: yes.toString(), no_shares: no.toString(),
                            paid: paid.toString(), value: value.toString(), pnl: (value - paid).toString(), basis });
            }
        }
        rows.sort((a, b) => (BigInt(a.paid) < BigInt(b.paid) ? 1 : -1));
        detail.positions = rows.slice(0, 12);
    } catch (e) { console.error('[pm] positions/pnl read:', e.message); }
}

// Reconcile market lifecycle (status / resolution / mechanism) from on-chain
// truth. The 4.8.8 migration changed statuses via LegacyMarketMigrated and the
// weekly governance batch resolves markets without always firing the events our
// live indexer keys on — so we periodically read the canonical state and fix any
// drift. Cheap: a handful of markets.
async function pmReconcileMarketsFromChain() {
    if (!pmAvailable()) return;
    try {
        const entries = await api.query.polkamarkt.markets.entries();
        let changed = 0;
        for (const [key, val] of entries) {
            const id = Number(key.args[0].toString());
            const m = val && val.toJSON ? val.toJSON() : null;
            if (!m) continue;
            const status = m.status != null ? String(m.status) : null;
            const mechanism = m.mechanism != null ? String(m.mechanism) : null;
            let resolution = null;
            if (status === 'Resolved' && api.query.polkamarkt.marketResolution) {
                try {
                    const rOpt = await api.query.polkamarkt.marketResolution(id);
                    const rj = rOpt && rOpt.toJSON ? rOpt.toJSON() : null;
                    if (rj != null) resolution = String(rj);
                } catch (_) {}
            }
            const n = await pmReconcileMarketStatus(id, { status, resolution, mechanism });
            if (n > 0) changed += 1;
        }
        if (changed > 0) console.log(`🔧 Polkamarkt reconcile: ${changed} market(s) synced from chain`);
    } catch (e) {
        console.error('[pm] reconcile error:', e.message);
    }
}

app.get('/polkamarkt/state', rateLimit(30, 60000), async (req, res) => {
    try {
        if (!pmAvailable()) return res.json(pmUnavailable());
        const totals = await pmGetTotals();
        res.json({ available: true, totals });
    } catch (e) {
        console.error('Error /polkamarkt/state:', e.message);
        res.status(500).json({ error: 'Internal server error' });
    }
});

app.get('/polkamarkt/markets', rateLimit(30, 60000), async (req, res) => {
    try {
        if (!pmAvailable()) return res.json({ ...pmUnavailable(), data: [], total: 0, page: 1, totalPages: 1 });
        const page = parseInt(req.query.page) || 1;
        const limit = Math.min(parseInt(req.query.limit) || 25, 100);
        const status = req.query.status || null;
        const result = await pmGetMarkets({ page, limit, status });
        // Enrich each row with live DPM state so the bar shows the real implied
        // probability, not the collateral split. Best-effort: serve without it
        // if the runtime API call fails.
        try {
            const ids = result.data.map(m => Number(m.market_id));
            const states = await pmGetLiveStates(ids);
            for (const m of result.data) {
                const s = states[Number(m.market_id)];
                if (!s) continue;
                m.implied_yes_bps = s.impliedYesBps;
                m.implied_no_bps = s.impliedNoBps;
                m.marginal_yes_bps = s.marginalYesBps;
                m.marginal_no_bps = s.marginalNoBps;
                if (!m.mechanism && s.mechanism) m.mechanism = s.mechanism;
            }
        } catch (_) { /* no live pricing this round */ }
        res.json(result);
    } catch (e) {
        console.error('Error /polkamarkt/markets:', e.message);
        res.status(500).json({ error: 'Internal server error' });
    }
});

app.get('/polkamarkt/market/:id', rateLimit(30, 60000), async (req, res) => {
    const id = parseInt(req.params.id);
    if (!Number.isFinite(id) || id < 0) return res.status(400).json({ error: 'Invalid market id' });
    try {
        if (!pmAvailable()) return res.json({ ...pmUnavailable(), data: null });
        const detail = await pmGetMarketDetail(id);
        if (!detail) return res.status(404).json({ error: 'Market not found' });
        try {
            const s = (await pmGetLiveStates([id]))[id];
            if (s && detail.market) {
                detail.market.implied_yes_bps = s.impliedYesBps;
                detail.market.implied_no_bps = s.impliedNoBps;
                detail.market.marginal_yes_bps = s.marginalYesBps;
                detail.market.marginal_no_bps = s.marginalNoBps;
                if (!detail.market.mechanism && s.mechanism) detail.market.mechanism = s.mechanism;
            }
            await pmEnrichMarketDetail(id, detail, s);
        } catch (e) { console.error('[pm] market detail enrich:', e.message); }
        res.json({ available: true, ...detail });
    } catch (e) {
        console.error('Error /polkamarkt/market/:id:', e.message);
        res.status(500).json({ error: 'Internal server error' });
    }
});

app.get('/polkamarkt/positions/:addr', rateLimit(30, 60000), async (req, res) => {
    const addr = req.params.addr;
    if (!/^cn[A-Za-z0-9]{46,50}$/.test(addr)) return res.status(400).json({ error: 'Invalid SORA address' });
    try {
        if (!pmAvailable()) return res.json({ ...pmUnavailable(), positions: [] });
        const positions = await pmGetUserPositions(addr);
        res.json({ available: true, positions });
    } catch (e) {
        console.error('Error /polkamarkt/positions:', e.message);
        res.status(500).json({ error: 'Internal server error' });
    }
});

app.get('/news/episodes', rateLimit(60, 60000), async (req, res) => {
    try {
        const limit = Math.max(1, Math.min(100, parseInt(req.query.limit, 10) || 50));
        const offset = Math.max(0, parseInt(req.query.offset, 10) || 0);
        const rows = await newsListEpisodes({ limit, offset });
        res.json({ episodes: rows, total: rows.length });
    } catch (e) {
        console.error('Error /news/episodes:', e.message);
        res.status(500).json({ error: 'Internal server error' });
    }
});

app.get('/polkamarkt/buybacks', rateLimit(30, 60000), async (req, res) => {
    try {
        if (!pmAvailable()) return res.json({ ...pmUnavailable(), pending: '0', stats: null, history: [] });
        const limit = Math.max(1, Math.min(50, parseInt(req.query.limit, 10) || 10));

        let pendingKusd = '0';
        try {
            const v = await api.query.polkamarkt.pendingXorBuybackCollateral();
            pendingKusd = v.toString();
        } catch (e) {
            console.warn('[/polkamarkt/buybacks] pendingXorBuybackCollateral read failed:', e.message);
        }

        const [stats, history, burns] = await Promise.all([
            pmGetBuybackStats(),
            pmListBuybacks({ limit }),
            pmGetBurnStats(),
        ]);

        res.json({ available: true, pendingKusd, stats, history, burns });
    } catch (e) {
        console.error('Error /polkamarkt/buybacks:', e.message);
        res.status(500).json({ error: 'Internal server error' });
    }
});

app.get('/stats/fees', rateLimit(20, 60000), async (req, res) => {
    try {
        const timeframe = req.query.timeframe || '1d';
        const msMap = {
            '1h': 3600000, '4h': 14400000, '1d': 86400000, '24h': 86400000,
            '7d': 604800000, '30d': 2592000000, '1m': 2592000000, '1y': 31536000000, 'all': 0
        };
        const ms = msMap[timeframe];
        const startTime = (ms === undefined || ms === 0) ? 0 : (Date.now() - ms);

        const stats = await getFeeStats(startTime, currentDenomFactor);
        res.json(stats);
    } catch (e) {
        res.status(500).json({ error: 'Internal server error' });
    }
});

app.get('/stats/fees/trend', rateLimit(20, 60000), async (req, res) => {
    try {
        const timeframe = req.query.timeframe || '1d';
        const ms = TIMEFRAME_MS[timeframe];
        const startTime = (ms === undefined || ms === 0) ? 0 : (Date.now() - ms);

        let interval = 'hour';
        if (timeframe === '7d' || timeframe === '1m' || timeframe === '1y' || timeframe === 'all') {
            interval = 'day';
        }

        const stats = await getFeeTrend(startTime, interval);
        res.json(stats);
    } catch (e) {
        res.status(500).json({ error: 'Internal server error' });
    }
});


// --- BURN TRACKER ENDPOINTS ---

const VALID_BURN_SYMBOL = /^(XOR|VAL|PSWAP|TBCD|KUSD)$/i;

// Real burn time-series for the cumulative chart (per-day, from the indexer).
// Returns cumulative burned amount of the requested token. Empty array until
// the indexer accumulates enough days. No synthetic data.
app.get('/burns/series/:symbol', rateLimit(30, 60000), async (req, res) => {
    const symbol = req.params.symbol.toUpperCase();
    if (!VALID_BURN_SYMBOL.test(symbol)) return res.status(400).json({ error: 'Invalid symbol' });
    const days = Math.min(parseInt(req.query.days) || 30, 90);
    try {
        const key = { XOR: 'xor', VAL: 'val', KUSD: 'kusd', TBCD: 'tbcd' }[symbol];
        // PSWAP burns via a different pallet (pswap-distribution), not indexed here.
        if (!key) return res.json({ symbol, points: [], note: 'not tracked by fee-burns indexer' });
        const daily = await getFeeBurnsSeries(days);
        let cum = 0;
        const points = daily.map(d => { cum += d[key] || 0; return { ts: d.ts, daily: d[key] || 0, cumulative: cum }; });
        res.json({ symbol, days, points });
    } catch (e) {
        console.error('[burns/series]', e.message);
        res.status(500).json({ error: 'Internal server error' });
    }
});

app.get('/burns/supply/:symbol', rateLimit(20, 60000), async (req, res) => {
    const symbol = req.params.symbol.toUpperCase();
    if (!VALID_BURN_SYMBOL.test(symbol)) return res.status(400).json({ error: 'Invalid symbol' });

    try {
        const supply = await getTokenTotalSupply(symbol);
        const price = tokenPrices[symbol] || 0;
        // 1 XOR = 1 XOR — supply is the real on-chain total issuance (matches MOF).
        // No CoinGecko, no denomination unpacking.
        const response = { symbol, totalSupply: supply, price, marketCap: (supply || 0) * price };

        res.json(response);
    } catch (e) {
        res.status(500).json({ error: 'Internal server error' });
    }
});

app.get('/burns/supply-history/:symbol', rateLimit(15, 60000), async (req, res) => {
    const symbol = req.params.symbol.toUpperCase();
    if (!VALID_BURN_SYMBOL.test(symbol)) return res.status(400).json({ error: 'Invalid symbol' });

    const timeframe = req.query.timeframe || '7d';
    const msMap = { '4h': 14400000, '1d': 86400000, '7d': 604800000, '1m': 2592000000, '1y': 31536000000, 'all': 0 };
    const ms = msMap[timeframe];
    if (ms === undefined) return res.status(400).json({ error: 'Invalid timeframe' });
    const startTime = ms === 0 ? 0 : (Date.now() - ms);

    try {
        const token = BURN_TOKENS[symbol];
        const genesis = GENESIS_SUPPLY[symbol] || null;
        const data = await getSupplyHistory(symbol, startTime, token?.assetId, genesis);
        res.json(data);
    } catch (e) {
        res.status(500).json({ error: 'Internal server error' });
    }
});

app.get('/burns/stats/:symbol', rateLimit(20, 60000), async (req, res) => {
    const symbol = req.params.symbol.toUpperCase();
    if (!VALID_BURN_SYMBOL.test(symbol)) return res.status(400).json({ error: 'Invalid symbol' });

    const timeframes = { '24h': 86400000, '7d': 604800000, '30d': 2592000000, 'all': 0 };

    try {
        const stats = {};
        const token = BURN_TOKENS[symbol];
        for (const [tf, ms] of Object.entries(timeframes)) {
            const startTime = ms === 0 ? 0 : Date.now() - ms;
            let supplyBurn;

            if (symbol === 'XOR') {
                // XOR: supply snapshot delta is unreliable at 10^18 scale (float64 precision loss).
                // Calculate fee-based burns instead: 20% of all XOR fees are burned directly.
                supplyBurn = await getBurnStats(symbol, startTime);
                const feeData = await getFeeStats(startTime, currentDenomFactor);
                let totalFees = 0;
                let totalFeesUsd = 0;
                feeData.forEach(r => {
                    totalFees += parseFloat(r.total_xor) || 0;
                    totalFeesUsd += parseFloat(r.total_usd) || 0;
                });
                const feeBurn = totalFees * 0.20;
                const feeBurnUsd = totalFeesUsd * 0.20;
                supplyBurn.feeBased = feeBurn;
                supplyBurn.feeBasedUsd = feeBurnUsd;
                if (feeBurn > 0) {
                    supplyBurn.totalBurned = feeBurn;
                    supplyBurn.totalBurnedUsd = feeBurnUsd;
                }
            } else if (token && token.assetId) {
                // Non-XOR: compute burns from circulating supply changes.
                // "all" timeframe: genesisSupply - currentCirculating (e.g. 100M - 56M = 44M).
                // Short timeframes: MOF snapshot delta (first - last in period).
                const price = tokenPrices[symbol] || 0;
                const currentSupplyNow = await getTokenTotalSupply(symbol);
                const genesis = GENESIS_SUPPLY[symbol];

                if (tf === 'all' && genesis && currentSupplyNow) {
                    // Total burned since genesis = initial distribution - current circulating
                    const totalBurned = genesis.supply - currentSupplyNow;
                    supplyBurn = {
                        totalBurned: totalBurned,
                        totalBurnedUsd: totalBurned * price,
                        totalBurn: totalBurned,
                        genesisSupply: genesis.supply,
                    };
                } else {
                    // Short timeframes: delta from MOF circulating snapshots
                    const delta = await getSupplySnapshotDelta(symbol, startTime);
                    const burned = delta.firstSupply - delta.lastSupply; // positive = supply decreased
                    supplyBurn = {
                        totalBurned: burned > 0 ? burned : 0,
                        totalBurnedUsd: (burned > 0 ? burned : 0) * price,
                        totalBurn: burned > 0 ? burned : 0,
                        firstSupply: delta.firstSupply,
                        lastSupply: delta.lastSupply,
                    };
                }
            } else {
                supplyBurn = await getBurnStats(symbol, startTime);
            }

            stats[tf] = supplyBurn;
        }
        const currentSupply = await getTokenTotalSupply(symbol);
        res.json({ symbol, currentSupply, stats, denomFactor: currentDenomFactor });
    } catch (e) {
        res.status(500).json({ error: 'Internal server error' });
    }
});

app.get('/burns/fee-flow', rateLimit(20, 60000), async (req, res) => {
    try {
        const startTime = Date.now() - 86400000;
        const feeStats = await getFeeStats(startTime, currentDenomFactor);

        let totalXor = 0;
        feeStats.forEach(row => { totalXor += parseFloat(row.total_xor) || 0; });

        const sv = api?.runtimeVersion?.specVersion?.toNumber?.() ?? 0;

        // Fee distribution model. Real runtime constants:
        //   4.8.6 (spec>=128): ref 10 / xor 35 / val 40 / kusd 0  (total 85)
        //                      RemintXorBurnPercent 40%, RemintKusdBuyBackPercent 0%, VAL_BURN_PERCENT 10%
        //   <=4.8.4: ref 10 / xor 20 / val 50 / kusd 5            (total 85)
        let distribution, weights;
        if (sv >= 128) {
            const W = { ref: 10, xor: 35, val: 40, kusd: 0, total: 85 };
            const REMINT_XOR_BURN = 0.40;   // RemintXorBurnPercent — 40% of the VAL bucket is burnt as XOR
            const VAL_BURN_PCT = 0.10;       // VAL_BURN_PERCENT — 10% of swapped VAL burnt, 90% to stakers
            const referrer      = totalXor * (W.ref / W.total);            // to referrer, or burnt as XOR if none
            const xorBurnDirect = totalXor * (W.xor / W.total);            // burnt as XOR directly on each fee
            const xorToValBucket = totalXor * (W.val / W.total);           // queued for VAL buy-back at remint
            const xorBurnRemint = xorToValBucket * REMINT_XOR_BURN;        // 40% of bucket burnt as XOR at remint
            const xorToVal      = xorToValBucket * (1 - REMINT_XOR_BURN);  // 60% swapped to VAL
            const valStaking    = xorToVal * (1 - VAL_BURN_PCT);           // 90% of VAL → stakers (NOT burnt)
            const valBurn       = xorToVal * VAL_BURN_PCT;                 // 10% of VAL burnt
            distribution = {
                xorBurn:    xorBurnDirect + xorBurnRemint,   // total XOR burnt (direct + remint)
                valStaking,                                   // XOR-equiv routed to staking rewards
                valBurn,                                      // XOR-equiv of VAL burnt (10%)
                referrer,                                     // to referrer (or burnt if no referrer)
            };
            weights = W;
        } else {
            // Legacy <=4.8.4 model (KUSD buy-back still active)
            const kusdWeight = 0.05;
            const valSlice = totalXor * 0.50;
            const xorBurnExtra = valSlice * 0.01;
            const valSliceAfterXorBurn = valSlice - xorBurnExtra;
            distribution = {
                xorBurn:     totalXor * 0.20 + xorBurnExtra,
                valBurn:     valSliceAfterXorBurn * 0.61,
                kusdBuyback: valSliceAfterXorBurn * 0.39 + totalXor * kusdWeight,
                referrer:    totalXor * 0.10,
            };
            weights = { ref: 10, xor: 20, val: 50, kusd: 5, total: 85 };
        }

        const flow = {
            specVersion: sv,
            totalXorFees: totalXor,
            distribution,
            weights,
            supplies: {}
        };

        for (const sym of Object.keys(BURN_TOKENS)) {
            flow.supplies[sym] = await getTokenTotalSupply(sym);
        }

        res.json(flow);
    } catch (e) {
        console.error('Fee flow error:', e.message);
        res.status(500).json({ error: 'Internal server error' });
    }
});


// Shared scan logic for /burns/holders. Called both synchronously on cold
// hit and asynchronously by stale-while-revalidate. Updates holdersCache.
async function refreshHoldersInBackground(symbol, token) {
    try {
        let fullList = [];
        if (symbol === 'XOR') {
            const allEntries = await withTimeout(api.query.system.account.entries(), 60000);
            for (const [key, value] of allEntries) {
                const data = value.toJSON();
                const free = (data.data && data.data.free) ? data.data.free.toString() : '0';
                const amountBn = new BigNumber(free).div('1e18');
                if (amountBn.gt(1)) {
                    fullList.push({ address: key.args[0].toString(), balance: amountBn.toNumber(), balanceStr: amountBn.toFormat(2) });
                }
            }
        } else {
            const allEntries = await withTimeout(api.query.tokens.accounts.entries(), 60000);
            for (const [key, value] of allEntries) {
                let currentAssetId = key.args[1].toString();
                if (currentAssetId.startsWith('{')) { try { currentAssetId = JSON.parse(currentAssetId).code; } catch (e) { } }
                if (currentAssetId === token.assetId) {
                    const data = value.toJSON();
                    const free = data.free ? data.free.toString() : '0';
                    const amountBn = new BigNumber(free).div(new BigNumber(10).pow(token.decimals));
                    if (amountBn.gt(0.1)) {
                        fullList.push({ address: key.args[0].toString(), balance: amountBn.toNumber(), balanceStr: amountBn.toFormat(2) });
                    }
                }
            }
        }
        fullList.sort((a, b) => b.balance - a.balance);
        holdersCache[token.assetId] = { timestamp: Date.now(), list: fullList };
        console.log(`✅ Refreshed holders for ${symbol}: ${fullList.length} holders`);
    } catch (e) {
        console.error(`❌ refreshHoldersInBackground(${symbol}):`, e.message);
    }
}

app.get("/burns/holders/:symbol", rateLimit(120, 60000), async (req, res) => {
    const symbol = req.params.symbol.toUpperCase();
    if (!VALID_BURN_SYMBOL.test(symbol)) return res.status(400).json({ error: 'Invalid symbol' });

    const token = BURN_TOKENS[symbol];
    if (!token || !token.assetId) return res.status(400).json({ error: 'Asset ID not resolved' });

    const page = parseInt(req.query.page) || 1;
    const limit = 15;
    const startIndex = (page - 1) * limit;

    try {
        let fullList = [];
        const now = Date.now();
        const cached = holdersCache[token.assetId];
        const isFresh = cached && (now - cached.timestamp < CACHE_DURATION);
        const hasStale = cached && cached.list && cached.list.length > 0;

        // Stale-while-revalidate: if we have ANY cached list (even old), serve
        // it instantly and refresh in the background. This avoids the 17-24s
        // freeze every time the cache expires (typical XOR holders scan).
        if (isFresh) {
            fullList = cached.list;
        } else if (hasStale) {
            fullList = cached.list;
            // Trigger background refresh — fire-and-forget, no await.
            if (!holdersCache[token.assetId]._refreshing) {
                holdersCache[token.assetId]._refreshing = true;
                refreshHoldersInBackground(symbol, token).finally(() => {
                    if (holdersCache[token.assetId]) holdersCache[token.assetId]._refreshing = false;
                });
            }
        } else {
            // No cache at all — must do the slow scan synchronously this once.
            console.log(`🔍 Scanning holders for ${symbol}... (cold)`);
            if (symbol === 'XOR') {
                const allEntries = await withTimeout(api.query.system.account.entries(), 60000);
                for (const [key, value] of allEntries) {
                    const data = value.toJSON();
                    const free = (data.data && data.data.free) ? data.data.free.toString() : '0';
                    const amountBn = new BigNumber(free).div('1e18');
                    if (amountBn.gt(1)) {
                        fullList.push({ address: key.args[0].toString(), balance: amountBn.toNumber(), balanceStr: amountBn.toFormat(2) });
                    }
                }
            } else {
                const allEntries = await withTimeout(api.query.tokens.accounts.entries(), 60000);
                for (const [key, value] of allEntries) {
                    let currentAssetId = key.args[1].toString();
                    if (currentAssetId.startsWith('{')) { try { currentAssetId = JSON.parse(currentAssetId).code; } catch (e) { } }
                    if (currentAssetId === token.assetId) {
                        const data = value.toJSON();
                        const free = data.free ? data.free.toString() : '0';
                        const amountBn = new BigNumber(free).div(new BigNumber(10).pow(token.decimals));
                        if (amountBn.gt(0.1)) {
                            fullList.push({ address: key.args[0].toString(), balance: amountBn.toNumber(), balanceStr: amountBn.toFormat(2) });
                        }
                    }
                }
            }
            fullList.sort((a, b) => b.balance - a.balance);
            holdersCache[token.assetId] = { timestamp: now, list: fullList };
        }

        // Get total supply for % calculation
        const totalSupply = await getTokenTotalSupply(symbol);
        const paginatedItems = fullList.slice(startIndex, startIndex + limit);

        // Resolve on-chain identities for paginated holders
        const holderAddresses = paginatedItems.map(h => h.address).filter(a => a && a.length > 40);
        if (holderAddresses.length > 0) {
            try {
                await resolveIdentitiesBatch(holderAddresses);
            } catch (e) { console.error('Burn holders identity resolve error:', e.message); }
        }
        // Attach display names from identity cache
        const enrichedItems = paginatedItems.map(h => {
            const identity = identityMemCache.get(h.address);
            return { ...h, name: identity?.display || null };
        });

        res.json({
            page, totalHolders: fullList.length, totalPages: Math.ceil(fullList.length / limit),
            totalSupply: totalSupply || 0,
            data: enrichedItems
        });
    } catch (e) {
        console.error('Burn holders error:', e.message);
        res.status(500).json({ error: 'Internal server error' });
    }
});

// --- NETWORK ROUTING ---
// /         → landing page (choose network)
// /sorav2   → existing SORA v2 dashboard (was previously the only entry)
// /minamoto → SORA Nexus Minamoto dashboard (Iroha 3)
app.get('/',         rateLimit(60, 60000), (req, res) => res.sendFile(__dirname + '/landing.html'));
app.get('/sorav2',   rateLimit(60, 60000), (req, res) => res.sendFile(__dirname + '/index.html'));
app.get('/minamoto', rateLimit(60, 60000), (req, res) => res.sendFile(__dirname + '/minamoto.html'));

async function startApp() {
    console.log('🛡️ Iniciando servidor con Alta Estabilidad (Proxy Limitado + Batching 3s)...');
    await initDB();
    // Polkamarkt tables are idempotent; safe to run every boot. If the pallet
    // activates later via runtime upgrade the indexer picks up from there.
    await initPolkamarktSchema().catch(e => console.error('[pm] schema init failed:', e.message));
    await initFeeBurnsLiveSchema().catch(e => console.error('[fee-burns] schema init failed:', e.message));
    await initNewsSchema().catch(e => console.error('[news] schema init failed:', e.message));
    await initValStakingRewardsSchema().catch(e => console.error('[val-staking-rewards] schema init failed:', e.message));
    try {
        const _saDb = require('./analytics/db');
        const _saPresence = require('./analytics/presence');
        await _saDb.initSchema();
        _saDb.startFlushLoop();
        _saPresence.startSweepLoop();
        setInterval(() => _saDb.rollupAndPrune().catch(() => {}), 6 * 3600 * 1000);
        console.log('✅ site analytics schema + loops started');
    } catch (e) { console.error('[analytics] init failed:', e.message); }
    await loadOfficialWhitelist();
    resolveBurnTokenIds();
    api = await initApi();
    startFeeBurnsIndexer(api, { insertFeeBurnRow });
    startPipelineSubscriber().catch(e => console.error('[pipeline] subscriber failed:', e.message));

    // Polkamarkt: sync market lifecycle (status/resolution/mechanism) from chain
    // on boot, then periodically. Catches statuses the 4.8.8 migration set
    // without firing the events our live indexer keys on.
    setTimeout(() => pmReconcileMarketsFromChain().catch(() => {}), 8000);
    setInterval(() => pmReconcileMarketsFromChain().catch(() => {}), 5 * 60 * 1000);

    // Initialize denomination factor from on-chain + fix legacy fees with missing denom_factor
    await updateDenomFactor();
    if (currentDenomFactor !== '1') {
        const fixed = await fixFeeDenomFactor(currentDenomFactor);
        if (fixed > 0) console.log(`✅ Fixed ${fixed} fees with denom_factor=10^${currentDenomFactor.length - 1}`);
    }
    setInterval(updateDenomFactor, 3600000); // refresh every 1h

    setInterval(updateKeyPrices, 60000);
    updateKeyPrices();

    // Supply snapshot job - every 30 minutes (Burn Tracker)
    // Uses MOF API circulating supply so chart matches the "Supply actual" card value.
    // Burn stats come from asset_snapshot burn/mint columns, NOT from supply deltas.
    async function takeSupplySnapshots() {
        console.log('📸 Taking supply snapshots...');
        for (const sym of Object.keys(BURN_TOKENS)) {
            try {
                // Use dedicated MOF fetch for snapshots — never on-chain fallback
                // to avoid writing totalIssuance (includes locked/vesting) instead of circulating
                const supply = await fetchMofSupply(sym);
                if (supply !== null && supply > 0) {
                    await insertSupplySnapshot(sym, BURN_TOKENS[sym].assetId, supply);
                    console.log(`  ✅ ${sym}: ${supply.toLocaleString()}`);
                } else {
                    console.log(`  ⚠️ ${sym}: skipped (MOF unavailable)`);
                }
            } catch (e) {
                console.error(`  ❌ ${sym} snapshot error:`, e.message);
            }
        }
    }
    // Pre-warm supply cache from DB so first requests don't return null.
    // XOR is skipped — its supply now comes from on-chain (balances.totalIssuance,
    // 1 XOR = 1 XOR) on demand, not from stale CoinGecko-derived snapshots.
    for (const sym of Object.keys(BURN_TOKENS)) {
        if (sym === 'XOR') continue;
        try {
            const snap = await getLatestSupplySnapshot(sym);
            if (snap && snap.total_supply) {
                supplyCache[sym] = { value: snap.total_supply, ts: Date.now() };
                console.log(`  🔄 ${sym} cache warmed: ${snap.total_supply.toLocaleString()}`);
            }
        } catch (e) { /* DB not ready yet */ }
    }

    setTimeout(takeSupplySnapshots, 15000);
    setInterval(takeSupplySnapshots, 30 * 60 * 1000); // cada 30 minutos

    server.listen(PORT, '0.0.0.0', () => console.log(`🚀 Server on port ${PORT} `));

    // Pre-warm holders cache for the heavy tokens. Each scan is 17-24s
    // synchronously — without this, the first user clicking "Holders" tab
    // pays the latency. Spread out 5s apart so we dont hammer the RPC.
    setTimeout(() => {
        const PRE_WARM_TOKENS = ['XOR', 'VAL', 'KUSD', 'PSWAP', 'TBCD'];
        PRE_WARM_TOKENS.forEach((sym, i) => {
            setTimeout(() => {
                const token = BURN_TOKENS[sym];
                if (token && token.assetId && typeof refreshHoldersInBackground === 'function') {
                    console.log(`🔥 Pre-warming holders cache for ${sym}...`);
                    refreshHoldersInBackground(sym, token).catch(e => console.warn(`Pre-warm ${sym} failed:`, e.message));
                }
            }, i * 5000);
        });
    }, 30000);

    // Pre-load identity cache from DB
    try {
        const cached = await getAllCachedIdentities();
        for (const row of cached) {
            identityMemCache.set(row.address, { display: row.display, ts: Date.now() });
        }
        console.log(`🪪 Loaded ${cached.length} cached identities from DB.`);
    } catch (e) {
        console.error('Failed to pre-load identities:', e.message);
    }

    // Warm cache al inicio (datos pre-cargados para primer usuario)
    setTimeout(async () => {
        console.log('🔥 Warm cache inicializando...');
        try {
            // Pre-cargar swaps (page 1, sin filtro)
            const swapsData = await getLatestSwaps(1, 25);
            swapsCacheMap.set('1__', { data: swapsData, timestamp: Date.now() });
            console.log('✅ Cache swaps pre-cargado');
            
            // Pre-cargar transfers
            transfersCache = { data: await getLatestTransfers(1, 25), timestamp: Date.now() };
            console.log('✅ Cache transfers pre-cargado');
            
            // Pre-cargar tokens (usa cache interno)
            // Los tokens ya tienen su propio globalTokenCache
            console.log('✅ Cache tokens listo');
            
            // Pre-cargar pools
            if (api) {
                const entries = await withTimeout(api.query.poolXYK.reserves.entries(), 30000);
                let pools = [];
                for (const [key, value] of entries) {
                    const args = key.args;
                    let baseId = args[0].toHuman();
                    let targetId = args[1].toHuman();
                    if (typeof baseId === 'object' && baseId.code) baseId = baseId.code;
                    if (typeof targetId === 'object' && targetId.code) targetId = targetId.code;
                    const reserves = value.toHuman();
                    const baseToken = ASSETS.find(a => a.assetId === baseId) || { symbol: '?', name: 'Unknown', assetId: baseId, decimals: 18, logo: '' };
                    const targetToken = ASSETS.find(a => a.assetId === targetId) || { symbol: '?', name: 'Unknown', assetId: targetId, decimals: 18, logo: '' };
                    if (baseToken.symbol !== '?' && targetToken.symbol !== '?') {
                        pools.push({ base: baseToken, target: targetToken, reserves: { base: reserves[0], target: reserves[1] }, basePrice: tokenPrices[baseToken.symbol] || 0, targetPrice: tokenPrices[targetToken.symbol] || 0 });
                    }
                }
                pools.sort((a, b) => {
                    const aRes = parseFloat(String(a.reserves.base || '0').replace(/,/g, ''));
                    const bRes = parseFloat(String(b.reserves.base || '0').replace(/,/g, ''));
                    return bRes - aRes;
                });
                poolsCache = { data: pools, timestamp: Date.now() };
                console.log(`✅ Cache pools pre-cargado (${pools.length} pools)`);
            }
            
            console.log('🔥 Warm cache completado!');
        } catch (e) {
            console.error('⚠️ Error en warm cache:', e.message);
        }
    }, 5000); // Espera 5s a que todo esté listo

    setInterval(() => {
        const now = Date.now();
        if (pendingTransfers.length > 0) {
            const batch = pendingTransfers.splice(0, MAX_EVENTS_PER_BATCH);
            io.emit('transfers-batch', batch);
            console.log(`📤 Sent ${batch.length} transfers in batch`);
        }
        if (pendingSwaps.length > 0) {
            const batch = pendingSwaps.splice(0, MAX_EVENTS_PER_BATCH);
            io.emit('swaps-batch', batch);
            console.log(`📤 Sent ${batch.length} swaps in batch`);
        }
        if (pendingExtrinsicsBatch.length > 0) {
            const batch = pendingExtrinsicsBatch.splice(0, MAX_EVENTS_PER_BATCH);
            io.emit('extrinsics-batch', batch);
        }
        if (pendingOrderBook.length > 0) {
            const batch = pendingOrderBook.splice(0, MAX_EVENTS_PER_BATCH);
            io.emit('orderbook-batch', batch);
            console.log(`📤 Sent ${batch.length} orderbook events in batch`);
        }
        lastBatchTime = now;
    }, BATCH_INTERVAL_MS);

    // --- Block info tracking for frontend indicator ---
    let _lastBlockTimestamp = null;
    const _blockTimes = []; // rolling window of last 10 block intervals
    const BLOCK_TIMES_WINDOW = 10;

    api.rpc.chain.subscribeNewHeads(async (header) => {
        const blockNumber = header.number.toNumber();
        const blockHash = await api.rpc.chain.getBlockHash(blockNumber);

        // Track avg block time
        const now = Date.now();
        if (_lastBlockTimestamp) {
            _blockTimes.push(now - _lastBlockTimestamp);
            if (_blockTimes.length > BLOCK_TIMES_WINDOW) _blockTimes.shift();
        }
        _lastBlockTimestamp = now;

        // Fetch both Block (for extrinsics) and Events
        const [signedBlock, allEvents] = await Promise.all([
            api.rpc.chain.getBlock(blockHash),
            api.query.system.events.at(blockHash)
        ]);

        // Push to recent blocks buffer for staking section
        try {
            let blockAuthor = null;
            try {
                const derivedH = await api.derive.chain.getHeader(blockHash);
                blockAuthor = derivedH.author ? derivedH.author.toString() : null;
            } catch (e) { /* ignore */ }
            let authorName = null;
            if (blockAuthor) {
                const ids = await attachIdentities([blockAuthor]);
                authorName = ids[blockAuthor] || null;
            }
            recentBlocksBuffer.unshift({
                number: blockNumber,
                hash: blockHash.toHex(),
                validator: blockAuthor,
                validatorName: authorName,
                extrinsics: signedBlock.block.extrinsics.length,
                age: 0,
                timestamp: Date.now()
            });
            if (recentBlocksBuffer.length > RECENT_BLOCKS_MAX) recentBlocksBuffer.pop();
            // Update ages
            const nowMs = Date.now();
            recentBlocksBuffer.forEach(b => { if (b.timestamp) b.age = Math.floor((nowMs - b.timestamp) / 1000); });
        } catch (e) { /* ignore recent blocks push error */ }

        // DEBUG: Ver qué tipos de eventos llegan
        const eventSections = [...new Set(allEvents.map(r => `${r.event.section}.${r.event.method}`))];
        if (allEvents.length > 1) {
            console.log(`📦 Block ${blockNumber}: ${allEvents.length} events [${eventSections.join(', ')}]`);
        } else {
            console.log(`📦 Block ${blockNumber}: ${allEvents.length} events, ${signedBlock.block.extrinsics.length} txs.`);
        }

        // --- EXTRINSIC PROCESSING (Targeted for Bridge) ---
        signedBlock.block.extrinsics.forEach((ex, index) => {
            try {
                const { method: { section, method, args }, signer } = ex.toHuman();

                if (section === 'ethBridge' && method === 'transferToSidechain') {
                    const extrinsicEvents = allEvents.filter(({ phase }) =>
                        phase.isApplyExtrinsic && phase.asApplyExtrinsic.eq(index)
                    );
                    const isSuccess = extrinsicEvents.some(({ event }) =>
                        api.events.system.ExtrinsicSuccess.is(event)
                    );

                    if (isSuccess) {
                        console.log(`🌉 EthBridge Outgoing Detected via Extrinsic!`);

                        let assetId = args.asset_id || args[0];
                        let recipient = args.to || args[1];
                        let amount = args.amount || args[2];

                        if (typeof assetId === 'object' && assetId?.code) assetId = assetId.code;
                        if (typeof assetId === 'object' && assetId?.assetId) assetId = assetId.assetId;

                        const assetInfo = getAssetInfo(assetId);
                        const decimals = assetInfo ? assetInfo.decimals : 18;
                        const symbol = assetInfo ? assetInfo.symbol : 'UNK';

                        const rawAmount = typeof amount === 'string' ? amount.replace(/,/g, '') : amount;
                        const amountBn = new BigNumber(rawAmount).div(new BigNumber(10).pow(decimals));

                        (async () => {
                            const price = await getOrFetchPrice(symbol, assetId, decimals);
                            const usdValue = amountBn.times(price).toNumber();

                            if (amountBn.gt(0)) {
                                insertBridge({
                                    block: blockNumber,
                                    network: 'Ethereum',
                                    direction: 'Outgoing',
                                    sender: signer ? signer.toString() : 'Unknown',
                                    recipient: recipient ? recipient.toString() : 'Unknown',
                                    assetId: assetId ? assetId.toString() : '',
                                    amount: amountBn.toFixed(4),
                                    usdValue,
                                    hash: ex.hash.toHex(),
                                    extrinsic_id: `${blockNumber}-${index}`
                                });
                                console.log(`✅ Bridge Outgoing Saved: ${amountBn.toFixed(2)} ${symbol} -> ${recipient}`);
                            }
                        })();
                    }
                }
            } catch (e) { console.error("Error processing extrinsic:", e); }
        });

        // --- HASHI v2 BRIDGES (event-driven, multi-chain) ---
        // sorametrics historically only indexed EthBridge (Hashi v1). SORA
        // runtime also exposes three Hashi v2 bridges that emit Burned/Minted
        // directly, each with its own address shape. Shapes probed live:
        //   substrateBridgeApp.Burned(netId, assetId, sender:AccountId, recipient:GenericAccount, amount)
        //   substrateBridgeApp.Minted(netId, assetId, sender:GenericAccount, recipient:AccountId, amount)
        //   parachainBridgeApp.Burned(netId, assetId, AccountId, ParachainAccountId, amount)
        //   parachainBridgeApp.Minted(netId, assetId, Option<ParachainAccountId>, AccountId, amount)
        //   jettonApp.Burned(assetId, AccountId, TonAddress, amount)           // SORA → TON
        //   jettonApp.Minted(assetId, TonAddress, AccountId, amount)           // TON → SORA
        // For JettonApp the network is always "TON"; the other two use the
        // SubNetworkId enum ("Kusama" / "Polkadot" / "Liberland" / …).
        // User-facing labels prepend the family ("Parachain: Kusama") so the
        // widget can distinguish XCM-parachain from substrate-relay routes.
        const decodeV2Bridge = (ev, index) => {
            const { section, method, data } = ev.event;
            const raw = data.map(d => d.toString());
            let networkFamily, networkName, netIdIdx, assetIdx, senderIdx, recipientIdx, amountIdx, direction;
            if (section === 'jettonApp') {
                networkFamily = 'TON';
                networkName = 'TON';
                assetIdx = 0; amountIdx = 3;
                if (method === 'Burned')  { senderIdx = 1; recipientIdx = 2; direction = 'Outgoing'; }
                if (method === 'Minted')  { senderIdx = 1; recipientIdx = 2; direction = 'Incoming'; }
                // jetton has no network_id in the tuple; netIdIdx stays undefined.
            } else if (section === 'substrateBridgeApp' || section === 'parachainBridgeApp') {
                networkFamily = section === 'parachainBridgeApp' ? 'Parachain' : 'Substrate';
                netIdIdx = 0; assetIdx = 1; amountIdx = 4;
                if (method === 'Burned')  { senderIdx = 2; recipientIdx = 3; direction = 'Outgoing'; }
                if (method === 'Minted')  { senderIdx = 2; recipientIdx = 3; direction = 'Incoming'; }
                // netIdIdx resolves to e.g. "Kusama", "Polkadot", "Liberland".
                const netName = String(raw[0] || '').trim();
                networkName = netName ? (networkFamily + ': ' + netName) : networkFamily;
            } else {
                return null;
            }
            if (!direction) return null; // non-Burned/Minted event we don't track
            return {
                assetId: raw[assetIdx],
                sender: raw[senderIdx] || 'Unknown',
                recipient: raw[recipientIdx] || 'Unknown',
                amountRaw: raw[amountIdx] || '0',
                network: networkName,
                direction,
                section, method,
            };
        };

        const v2BridgeEvents = allEvents.filter(({ event }) =>
            (event.section === 'substrateBridgeApp' || event.section === 'parachainBridgeApp' || event.section === 'jettonApp')
            && (event.method === 'Burned' || event.method === 'Minted')
        );
        for (const ev of v2BridgeEvents) {
            const dec = decodeV2Bridge(ev);
            if (!dec) continue;
            const assetInfo = getAssetInfo(dec.assetId);
            const decimals = assetInfo ? assetInfo.decimals : 18;
            const symbol = assetInfo ? assetInfo.symbol : 'UNK';
            const amountBn = new BigNumber(String(dec.amountRaw).replace(/,/g, '')).div(new BigNumber(10).pow(decimals));
            const extrinsicIdx = ev.phase.isApplyExtrinsic ? ev.phase.asApplyExtrinsic.toNumber() : null;
            const extHash = extrinsicIdx != null && signedBlock.block.extrinsics[extrinsicIdx]
                ? signedBlock.block.extrinsics[extrinsicIdx].hash.toHex()
                : null;
            (async () => {
                try {
                    const price = await getOrFetchPrice(symbol, dec.assetId, decimals);
                    const usdValue = amountBn.times(price).toNumber();
                    if (amountBn.gt(0)) {
                        insertBridge({
                            block: blockNumber,
                            network: dec.network,
                            direction: dec.direction,
                            sender: dec.sender,
                            recipient: dec.recipient,
                            assetId: dec.assetId,
                            amount: amountBn.toFixed(6),
                            usdValue,
                            hash: extHash || ('v2:' + blockNumber + ':' + (extrinsicIdx ?? '?')),
                            extrinsic_id: extHash || `${blockNumber}-${extrinsicIdx ?? '?'}`,
                        });
                        console.log(`🌉 ${dec.section}.${dec.method} (${dec.network}) ${dec.direction} ${amountBn.toFixed(2)} ${symbol}`);
                    }
                } catch (e) { console.error('v2 bridge insert error:', e.message); }
            })();
        }

        const pmEvents = allEvents.filter(({ event }) => event.section === 'polkamarkt');
        if (pmEvents.length > 0) {
            // Resolve ts once for the whole block — cheaper than per event.
            const blockTs = Date.now(); // live indexer: now ≈ block ts
            (async () => {
                for (const rec of pmEvents) {
                    const { section, method, data } = rec.event;
                    const raw = data.map(d => d.toString());
                    const extrinsicIdx = rec.phase.isApplyExtrinsic ? rec.phase.asApplyExtrinsic.toNumber() : null;
                    const extHash = extrinsicIdx != null && signedBlock.block.extrinsics[extrinsicIdx]
                        ? signedBlock.block.extrinsics[extrinsicIdx].hash.toHex()
                        : null;
                    try {
                        if (method === 'MarketCreated') {
                            // Hydrate market metadata from chain storage (question, oracle,
                            // source, mechanism). Requires api.query.polkamarkt.* — present,
                            // since the pallet just emitted the event we're handling.
                            const marketId = Number(raw[0]);
                            const seedLiquidity = raw[1];
                            let question = null, oracle = null, resolutionSource = null;
                            let creator = 'Unknown', closeBlock = 0, collateralAsset = '', conditionId = 0;
                            let mechanism = null, status = 'Open';
                            try {
                                const mOpt = await api.query.polkamarkt.markets(marketId);
                                if (mOpt && mOpt.isSome !== false) {
                                    const m = mOpt.toJSON ? mOpt.toJSON() : mOpt;
                                    creator = String(m.creator || 'Unknown');
                                    conditionId = Number(m.conditionId ?? m.condition_id ?? 0);
                                    closeBlock = Number(m.closeBlock ?? m.close_block ?? 0);
                                    // 4.8.8: MigratedLegacy | DynamicPariMutuel.
                                    mechanism = m.mechanism != null ? String(m.mechanism) : null;
                                    if (m.status != null) status = String(m.status);
                                    // collateralAsset is an AssetId32 — toJSON gives { code: '0x…' },
                                    // not a string. String() on the object yields "[object Object]";
                                    // extract the inner code (same shape as tokens.Transfer assetId).
                                    const _ca = m.collateralAsset ?? m.collateral_asset;
                                    collateralAsset = (_ca && typeof _ca === 'object')
                                        ? String(_ca.code ?? _ca.address ?? _ca.assetId ?? '')
                                        : String(_ca ?? '');
                                }
                                const cOpt = await api.query.polkamarkt.conditions(conditionId);
                                if (cOpt) {
                                    const c = cOpt.toJSON ? cOpt.toJSON() : cOpt;
                                    const hex2str = (h) => {
                                        try { return Buffer.from(String(h || '').replace(/^0x/, ''), 'hex').toString('utf8'); }
                                        catch { return ''; }
                                    };
                                    question = hex2str(c?.question) || null;
                                    oracle = hex2str(c?.oracle) || null;
                                    resolutionSource = hex2str(c?.resolutionSource ?? c?.resolution_source) || null;
                                }
                            } catch (_) { /* best-effort hydration */ }
                            await pmInsertMarket({
                                marketId, conditionId, creator, closeBlock, collateralAsset,
                                seedLiquidity, status,
                                question, oracle, resolutionSource, mechanism,
                                block: blockNumber, ts: blockTs,
                            });
                            console.log(`🎲 Polkamarkt Market #${marketId} (${mechanism || '?'}) created by ${creator.slice(0, 10)}…`);
                        } else if (method === 'TradeExecuted') {
                            await pmInsertTrade({
                                marketId: Number(raw[0]),
                                trader: raw[1],
                                side: raw[2],       // 'Buy' | 'Sell'
                                outcome: raw[3],    // 'Yes' | 'No'
                                collateral: raw[4],
                                shares: raw[5],
                                fee: raw[6],
                                block: blockNumber,
                                ts: blockTs,
                                hash: extHash,
                            });
                        } else if (method === 'MarketLocked') {
                            await pmUpdateMarketStatus(Number(raw[0]), 'Locked', null, blockNumber, blockTs);
                        } else if (method === 'MarketResolved') {
                            await pmUpdateMarketStatus(Number(raw[0]), 'Resolved', raw[1], blockNumber, blockTs);
                        } else if (method === 'MarketCancelled') {
                            await pmUpdateMarketStatus(Number(raw[0]), 'Cancelled', null, blockNumber, blockTs);
                        } else if (method === 'MarketClaimed') {
                            await pmInsertClaim({
                                marketId: Number(raw[0]),
                                account: raw[1],
                                kind: 'payout',
                                amount: raw[2],
                                block: blockNumber, ts: blockTs,
                            });
                        } else if (method === 'CreatorFeesClaimed') {
                            await pmInsertClaim({
                                marketId: Number(raw[0]),
                                account: raw[1],
                                kind: 'creator_fees',
                                amount: raw[2],
                                block: blockNumber, ts: blockTs,
                            });
                        } else if (method === 'LegacyMarketMigrated') {
                            // 4.8.8 migration stamped a final status on each legacy market
                            // (Resolved / Cancelled / …). This is how #0/#1 changed state
                            // WITHOUT a MarketResolved/Cancelled event — handle it explicitly.
                            // resolution (Yes/No) is filled by the periodic chain reconcile.
                            await pmReconcileMarketStatus(Number(raw[0]), {
                                status: String(raw[1]),
                                mechanism: 'MigratedLegacy',
                            });
                            console.log(`🔁 Polkamarkt LegacyMarketMigrated #${raw[0]} → ${raw[1]}`);
                        } else if (method === 'MarketEmergencyCancelled') {
                            await pmUpdateMarketStatus(Number(raw[0]), 'Cancelled', null, blockNumber, blockTs);
                        } else if (method === 'DpmResidualBurned') {
                            await pmInsertBurn({
                                block: blockNumber, ts: blockTs, hash: extHash,
                                marketId: Number(raw[0]), kind: 'dpm_residual', amount: raw[1],
                            });
                            console.log(`🔥 Polkamarkt DpmResidualBurned #${raw[0]} · ${raw[1]}`);
                        } else if (method === 'LegacyMigrationResidualRouted') {
                            await pmInsertBurn({
                                block: blockNumber, ts: blockTs, hash: extHash,
                                marketId: null, kind: 'legacy_migration_residual', amount: raw[0],
                            });
                        } else if (method === 'XorBuybackSwept') {
                            await pmInsertBuyback({
                                block: blockNumber,
                                ts: blockTs,
                                hash: extHash,
                                kusdSpent: raw[0],
                                xorBurned: raw[1],
                            });
                            console.log(`🔥 Polkamarkt XorBuybackSwept · KUSD=${raw[0]} XOR_burned=${raw[1]}`);
                        }
                    } catch (e) {
                        console.error(`polkamarkt.${method} handler error:`, e.message);
                    }
                }
            })();
        }

        // VAL staking rewards (4.8.6+ feature). Silently no-op while the event
        // does not exist in the runtime metadata; starts indexing the instant
        // runtime upgrade introduces `xorFee.ValStakingRewardPaid`.
        const vsrEvents = allEvents.filter(({ event }) =>
            event.section === 'xorFee' && event.method === 'ValStakingRewardPaid'
        );
        if (vsrEvents.length > 0) {
            const blockTs = new Date();
            const blockHashHex = blockHash.toHex();
            (async () => {
                for (const rec of vsrEvents) {
                    try {
                        const d = rec.event.data;
                        // Event signature: (stash, dest, era, page, amount)
                        const stash = d[0].toString();
                        const dest = d[1].toString();
                        const era = d[2].toNumber();
                        const page = d[3].toNumber();
                        const amount = d[4].toString();
                        await insertValStakingReward({
                            era, page,
                            validator_stash: stash,
                            destination: dest,
                            amount,
                            block_num: blockNumber,
                            block_hash: blockHashHex,
                            ts: blockTs,
                        });
                        console.log(`💎 ValStakingRewardPaid era=${era} page=${page} stash=${stash.slice(0,12)} dest=${dest.slice(0,12)} amount=${amount}`);
                    } catch (e) {
                        console.error('ValStakingRewardPaid insert error:', e.message);
                    }
                }
            })();
        }

        const swapEvents = allEvents.filter(({ event }) =>
            event.section === 'liquidityProxy' && event.method === 'Exchange'
        );

        // DEBUG: Investigar estructura de technical.SwapSuccess (swaps de bots/arbi)
        const technicalSwaps = allEvents.filter(({ event }) =>
            event.section === 'technical' && event.method === 'SwapSuccess'
        );

        if (technicalSwaps.length > 0) {
            console.log(`🤖 Detectados ${technicalSwaps.length} technical.SwapSuccess! Analizando estructura:`);
            technicalSwaps.forEach(({ event }) => {
                console.log('DATA:', JSON.stringify(event.data));
            });
        }

        // DEBUG: Log cuando hay swaps detectados
        if (swapEvents.length > 0) {
            console.log(`🔄 ${swapEvents.length} swaps detectados en bloque ${blockNumber}`);
        }

        const transferEvents = allEvents.filter(({ event }) =>
            (event.section === 'balances' && event.method === 'Transfer') ||
            (event.section === 'tokens' && event.method === 'Transfer')
        );

        const limitedTransfers = transferEvents;
        const limitedSwaps = swapEvents;

        // --- STATS LOGIC ---
        // Detect and Persist Bridge Events
        const bridgeEvents = allEvents.filter(({ event }) =>
            event.section.toLowerCase().includes('bridge')
        );

        (async () => {
            for (const record of bridgeEvents) {
                try {
                    const { event, phase } = record;

                    // NEW: Avoid Double Counting (Skip events from transferToSidechain extrinsics)
                    if (phase.isApplyExtrinsic) {
                        const idx = phase.asApplyExtrinsic.toNumber();
                        const ex = signedBlock.block.extrinsics[idx];
                        // Safety check if extrinsic exists
                        if (ex) {
                            const human = ex.toHuman();
                            if (human && human.method) {
                                const { section, method } = human.method;
                                if (section === 'ethBridge' && method === 'transferToSidechain') {
                                    console.log(`⏩ Skipping event ${event.method} (Handled by Extrinsic Logic)`);
                                    continue;
                                }
                            }
                        }
                    }

                    let sender = '', recipient = '', amount = '0', assetId = '', direction = 'Unknown', network = 'Ethereum';

                    const section = event.section.toLowerCase();
                    const method = event.method;
                    console.log(`🌉 Bridge event detected: ${section}.${method} `, event.data.toHuman());

                    // Determine network from section
                    if (section.includes('eth')) network = 'Ethereum';
                    else if (section.includes('sub') || section.includes('parachain')) network = 'Polkadot/Kusama';
                    else if (section.includes('ton')) network = 'TON';
                    else if (section.includes('proxy')) network = 'Multi-Network';
                    else network = 'Unknown';

                    if (method === 'TransferToSidechain') {
                        // Outgoing to Ethereum: [AccountId, H160, Balance, AssetId]
                        const data = event.data;
                        direction = 'Outgoing';
                        sender = data[0].toString();
                        recipient = data[1].toString(); // Ethereum address
                        amount = data[2].toString();
                        assetId = data[3]?.toString() || '';

                    } else if (method === 'RequestRegistered' || method === 'RequestStatusUpdate') {
                        // Incoming from Ethereum: [H256] or [H256, Status]
                        direction = 'Incoming';
                        const hash = event.data[0].toString();
                        console.log(`🔎 fetching bridge request for ${method}: ${hash} `);
                        try {
                            // Check available storage methods for debugging
                            if (api.query.ethBridge) {
                                // console.log('🔍 Debug ethBridge keys:', Object.keys(api.query.ethBridge));
                            }
                            if (api.query.bridgeProxy) {
                                console.log('🔍 Debug bridgeProxy keys:', Object.keys(api.query.bridgeProxy));
                            }

                            // Fetch request details from storage
                            let r = null;
                            if (api.query.ethBridge && api.query.ethBridge.requests) {
                                try {
                                    const req = await withTimeout(api.query.ethBridge.requests(0, hash));
                                    if (req.isSome) {
                                        r = req.unwrap().toJSON();
                                    } else {
                                        // Try bridgeProxy if ethBridge empty?
                                        // Some logic suggests bridgeProxy might wrap it.
                                    }
                                } catch (e) { console.error('EthBridge query failed', e); }
                            }

                            if (r) {
                                console.log('🌉 Incoming Request Data:', JSON.stringify(r));
                            } else {
                                console.log('⚠️ No request data found in ethBridge for hash:', hash);
                            }

                            // ---------------------------------------------------------

                            if (r) {
                                // Handle different request types (Transfer, AddAsset, etc.)
                                // Common structure: { Transfer: [ assetId, to, amount ] } or similar
                                // Adaptive parsing: look for amount/asset fields
                                const transferData = r.Transfer || r.transfer || r;

                                // Handle nested arrays commonly found in SORA structures
                                const extract = (obj) => {
                                    if (Array.isArray(obj)) {
                                        return {
                                            asset: obj[0],
                                            recipient: obj[1],
                                            amount: obj[2]
                                        };
                                    }
                                    return {
                                        asset: obj.asset_id || obj.assetId || obj.currency_id,
                                        recipient: obj.to || obj.recipient,
                                        amount: obj.amount || obj.balance
                                    };
                                };

                                const extracted = extract(transferData);

                                if (extracted.amount && extracted.asset) {
                                    assetId = extracted.asset;
                                    recipient = extracted.recipient;
                                    amount = extracted.amount;
                                    if (!sender) sender = 'Ethereum';
                                }
                            }
                        } catch (e) { console.error('Error fetching bridge request:', e); }

                    } else if (method === 'IncomingRequestFinalized') {
                        // INCOMING BRIDGE FINALIZED - This is the key event!
                        direction = 'Incoming';
                        const hash = event.data[0]?.toString();
                        console.log(`🌉 IncomingRequestFinalized detected! Hash: ${hash?.substring(0, 18)}...`);

                        // ---------------------------------------------------------
                        // NEW: Resolve Ethereum Sender via RequestRegistered Event
                        // ---------------------------------------------------------
                        let ethSender = null;
                        if (phase.isApplyExtrinsic) {
                            const exIndex = phase.asApplyExtrinsic.toNumber();
                            // Find RequestRegistered in same extrinsic
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

                        // Try to fetch request details
                        if (api.query.ethBridge && api.query.ethBridge.requests) {
                            try {
                                const req = await withTimeout(api.query.ethBridge.requests(0, hash));
                                const json = req.toJSON();
                                console.log(`   Request data:`, JSON.stringify(json).substring(0, 200));

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
                                    recipient = transferData.to;
                                    assetId = transferData.assetId?.code || transferData.assetId;
                                    amount = transferData.amount;
                                    sender = ethSender || transferData.from || 'Ethereum'; // Actual ETH address
                                    console.log(`   ✅ Parsed: to=${recipient?.substring(0, 15)}... asset=${assetId?.substring(0, 15)}... amount=${amount}`);
                                } else {
                                    console.log(`   ⚠️ Could not parse transfer data`);
                                    continue;
                                }
                            } catch (e) {
                                console.error(`   Error fetching request:`, e.message);
                                continue;
                            }
                        }

                    } else if (method === 'CurrencyDepositedFromSidechain' || method === 'SidechainCurrencyWithdrawn') {
                        // Legacy fallback (these events may not exist in newer SORA)
                        direction = method.includes('Deposit') ? 'Incoming' : 'Outgoing';
                        const data = event.data;
                        assetId = data[0]?.toString();
                        recipient = data[1]?.toString();
                        amount = data[2]?.toString();
                        if (!sender) sender = 'External';

                    } else if (method === 'ApprovesCollected' || method === 'ApprovalsCollected') {
                        continue;
                    } else {
                        // Generic Fallback
                        direction = 'Incoming';
                        const d = event.data.toHuman();
                        if (Array.isArray(d)) {
                            // Try to find large numbers or addresses
                            // ... omitted complex heuristics to avoid noise
                        }
                    }

                    // Standardize data
                    if (!amount || amount === '0') continue;

                    const assetInfo = getAssetInfo(assetId);
                    const decimals = assetInfo?.decimals || 18;
                    const symbol = assetInfo?.symbol || 'UNK';

                    let amountNum = 0;
                    try {
                        amountNum = new BigNumber(amount).div(new BigNumber(10).pow(decimals)).toNumber();
                    } catch (e) { }

                    // Calculate USD using on-demand price
                    const price = await getOrFetchPrice(symbol, assetId, decimals);
                    const usdValue = amountNum * price;

                    if (amountNum > 0) {
                        insertBridge({
                            block: blockNumber,
                            network,
                            direction,
                            sender: sender || 'Unknown',
                            recipient: recipient || 'Unknown',
                            assetId: assetId || '',
                            symbol: symbol,
                            logo: assetInfo?.logo || '',
                            amount: amountNum.toFixed(4),
                            usdValue,
                            hash: (phase && phase.isApplyExtrinsic) ? signedBlock.block.extrinsics[phase.asApplyExtrinsic].hash.toHex() : '',
                            extrinsic_id: (phase && phase.isApplyExtrinsic) ? `${blockNumber}-${phase.asApplyExtrinsic.toString()}` : ''
                        });
                        console.log(`🌉 Bridge stored: ${direction} ${amountNum.toFixed(4)} ${symbol} ($${usdValue.toFixed(2)})`);
                    }

                } catch (e) {
                    console.error('Error processing bridge event:', e);
                }
            }
        })();

        // --- LIQUIDITY EVENT TRACKING (via extrinsics, not events) ---

        (async () => {
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

                            // Fallback for JSON strings
                            if (currencyId.startsWith('{') && currencyId.includes('code')) {
                                try {
                                    const p = JSON.parse(currencyId);
                                    if (p.code) currencyId = p.code;
                                } catch (e) { }
                            }

                            const amount = data[3].toString();

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

                        // Get prices
                        const basePrice = await getOrFetchPrice(baseInfo?.symbol, baseAssetId, baseDecimals);
                        const targetPrice = await getOrFetchPrice(targetInfo?.symbol, targetAssetId, targetDecimals);

                        const usdValue = (baseAmountNum * basePrice) + (targetAmountNum * targetPrice);

                        insertLiquidityEvent({
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

                        console.log(`💧 LP ${type.toUpperCase()}: ${baseInfo?.symbol || 'UNK'}/${targetInfo?.symbol || 'UNK'} = $${usdValue.toFixed(2)}`);

                    } catch (e) {
                        console.error('Error parsing liquidity extrinsic:', e);
                    }
                }
            }
        })();

        // --- ORDER BOOK EVENT DETECTION ---
        // Helper: parse FixedU128/Balance from SORA order book events
        // toJSON() returns {"inner":"0xHEX","isDivisible":true} — extract hex, convert to decimal, divide by 10^18
        function parseOrderBookValue(val) {
            if (!val) return '';
            if (typeof val === 'string') return val.replace(/,/g, '');
            if (typeof val === 'number') return String(val);
            if (typeof val === 'object' && val.inner) {
                try { return new BigNumber(val.inner).div('1e18').toFixed(6); } catch(e) {}
            }
            const s = String(val);
            if (/^[\d,.]+$/.test(s)) return s.replace(/,/g, '');
            return '';
        }
        const orderBookEvents = allEvents.filter(({ event }) =>
            event.section === 'orderBook'
        );
        if (orderBookEvents.length > 0) {
            console.log(`📋 ${orderBookEvents.length} orderBook events in block ${blockNumber}`);
            for (const { event, phase } of orderBookEvents) {
                try {
                    const m = event.method;
                    const d = event.data;
                    const dJson = d.toJSON ? d.toJSON() : d; // for structured fields like OrderBookId
                    const formattedTime = new Date().toLocaleString('es-ES');
                    let eventType = '', wallet = '', orderId = '', baseAsset = '', quoteAsset = '', side = '', price = '', amount = '';

                    // OrderBookId is always first field: { dexId, base, quote }
                    const obId = dJson[0];
                    if (obId && typeof obId === 'object') {
                        const bInfo = getAssetInfo(obId.base);
                        const qInfo = getAssetInfo(obId.quote);
                        baseAsset = bInfo ? bInfo.symbol : (obId.base || '');
                        quoteAsset = qInfo ? qInfo.symbol : (obId.quote || '');
                    }

                    if (m === 'LimitOrderPlaced') {
                        eventType = 'placed';
                        orderId = d[1].toString();
                        wallet = d[2].toString();
                        const sideJson = dJson[3];
                        side = sideJson === 'Buy' ? 'buy' : 'sell';
                        price = parseOrderBookValue(dJson[4]);
                        amount = parseOrderBookValue(dJson[5]);
                    } else if (m === 'LimitOrderCanceled') {
                        eventType = 'canceled';
                        orderId = d[1].toString();
                        wallet = d[2].toString();
                    } else if (m === 'LimitOrderExecuted') {
                        eventType = 'executed';
                        orderId = d[1].toString();
                        wallet = d[2].toString();
                        const sideJson = dJson[3];
                        side = sideJson === 'Buy' ? 'buy' : 'sell';
                        price = parseOrderBookValue(dJson[4]);
                        amount = parseOrderBookValue(dJson[5]);
                    } else if (m === 'LimitOrderFilled') {
                        eventType = 'filled';
                        orderId = d[1].toString();
                        wallet = d[2].toString();
                    } else if (m === 'MarketOrderExecuted') {
                        eventType = 'market';
                        wallet = d[1].toString();
                        const sideJson = dJson[2];
                        side = sideJson === 'Buy' ? 'buy' : 'sell';
                        amount = parseOrderBookValue(dJson[3]);
                        price = parseOrderBookValue(dJson[4]);
                    } else {
                        continue; // skip unknown events
                    }

                    // Normalize price/amount (remove commas from on-chain strings)
                    price = price.replace(/,/g, '');
                    amount = amount.replace(/,/g, '');

                    const hash = (phase && phase.isApplyExtrinsic) ? signedBlock.block.extrinsics[phase.asApplyExtrinsic].hash.toHex() : '';
                    const extrinsicId = (phase && phase.isApplyExtrinsic) ? `${blockNumber}-${phase.asApplyExtrinsic.toString()}` : '';

                    const obData = {
                        timestamp: Date.now(), formatted_time: formattedTime,
                        block: blockNumber, event_type: eventType,
                        wallet, order_id: orderId,
                        base_asset: baseAsset, quote_asset: quoteAsset,
                        side, price, amount, usd_value: 0,
                        hash, extrinsic_id: extrinsicId
                    };

                    insertOrderBookEvent(obData);
                    pendingOrderBook.push({
                        time: formattedTime, block: blockNumber,
                        event_type: eventType, wallet, order_id: orderId,
                        base_asset: baseAsset, quote_asset: quoteAsset,
                        side, price, amount, usd_value: '0.00',
                        hash, extrinsic_id: extrinsicId
                    });

                    console.log(`📋 OrderBook ${eventType}: ${wallet.substring(0, 8)}... ${side} ${baseAsset}/${quoteAsset}`);
                } catch (e) {
                    console.error('Error parsing orderBook event:', e.message);
                }
            }
        }

        // --- RAW EXTRINSICS LOGGING ---
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

                const formattedTime = new Date().toLocaleString('es-ES');
                const exData = {
                    timestamp: Date.now(), formatted_time: formattedTime,
                    block: blockNumber, extrinsic_index: i,
                    hash: ex.hash.toHex(), section, method,
                    signer: typeof signer === 'string' ? signer : 'System',
                    success: isSuccess, args_json: argsJson,
                    error_msg: errorMsg,
                    events_json: serializeEvents(extrinsicEvents)
                };
                insertExtrinsic(exData);
                pendingExtrinsicsBatch.push({
                    time: formattedTime, block: blockNumber, extrinsic_index: i,
                    extrinsic_id: `${blockNumber}-${i}`,
                    hash: ex.hash.toHex(), section, method,
                    signer: exData.signer, success: isSuccess,
                    error_msg: errorMsg
                });
            } catch (e) { /* skip */ }
        }

        // --- PROACTIVE IDENTITY RESOLUTION ---
        const blockAddresses = new Set();
        signedBlock.block.extrinsics.forEach(ex => {
            try {
                const decoded = ex.toHuman();
                const s = decoded?.signer?.Id || (typeof decoded?.signer === 'string' ? decoded.signer : null);
                if (s && s !== 'System' && s.length > 40) blockAddresses.add(s);
            } catch (e) { /* skip */ }
        });
        if (blockAddresses.size > 0) queueIdentityResolve([...blockAddresses]);

        // Update Session Block & Notify Frontend
        sessionStats.block = blockNumber;

        // --- FEES TRACKING ---
        const extrinsicsEvents = {};
        allEvents.forEach((record) => {
            const { event, phase } = record;
            if (phase.isApplyExtrinsic) {
                const idx = phase.asApplyExtrinsic.toNumber();
                if (!extrinsicsEvents[idx]) extrinsicsEvents[idx] = [];
                extrinsicsEvents[idx].push(event);
            }
        });

        for (const idx in extrinsicsEvents) {
            const events = extrinsicsEvents[idx];
            let type = 'Other';
            let feeFn = null;

            // 1. Determine Type
            const hasSwap = events.some(e => e.section === 'liquidityProxy' && e.method === 'Exchange');
            const hasTransfer = events.some(e => (e.section === 'assets' || e.section === 'balances') && e.method.includes('Transfer'));
            const hasBridge = events.some(e => e.section === 'ethBridge' || e.section === 'bridge' || e.section === 'multisig');

            if (hasSwap) type = 'Swap';
            else if (hasBridge) type = 'Bridge';
            else if (hasTransfer) type = 'Transfer';

            // 2. Find Fee
            const feeEvent = events.find(e => e.section === 'transactionPayment' && e.method === 'TransactionFeePaid');
            if (feeEvent) {
                try {
                    // data: [who, actual_fee, tip]
                    const actualFee = feeEvent.data[1].toString();
                    const feeVal = new BigNumber(actualFee).div(1e18);

                    const xorPrice = tokenPrices['XOR'] || 0;
                    const usdValue = feeVal.times(xorPrice).toNumber();

                    insertFee({
                        block: blockNumber,
                        type,
                        amount: feeVal.toNumber(),
                        usdValue,
                        denomFactor: currentDenomFactor
                    });
                } catch (e) { console.error('Fee parsing error', e); }
            }
        }

        // Emit block info for frontend indicator
        let finalizedNum = null;
        try {
            const fHash = await api.rpc.chain.getFinalizedHead();
            const fHeader = await api.rpc.chain.getHeader(fHash);
            finalizedNum = fHeader.number.toNumber();
        } catch (e) { /* ignore */ }
        const avgBlockTime = _blockTimes.length > 0
            ? (_blockTimes.reduce((a, b) => a + b, 0) / _blockTimes.length / 1000).toFixed(3)
            : null;
        io.emit('new-block-stats', { block: blockNumber, finalized: finalizedNum, avgTime: avgBlockTime });


        // Process transfers with on-demand pricing
        (async () => {
            for (const { event, phase } of limitedTransfers) {
                const d = event.data;
                let from, to, amountStr, assetId;
                if (event.section === 'balances') {
                    from = d[0].toString(); to = d[1].toString(); amountStr = d[2].toString();
                    assetId = '0x0200000000000000000000000000000000000000000000000000000000000000';
                } else {
                    // tokens.Transfer: [assetId, from, to, amount]
                    const assetRaw = d[0];
                    const assetJson = assetRaw.toJSON ? assetRaw.toJSON() : assetRaw;
                    assetId = (typeof assetJson === 'object' && assetJson.code) ? assetJson.code : assetRaw.toString();
                    from = d[1].toString(); to = d[2].toString(); amountStr = d[3].toString();
                }

                if (from.startsWith('cnTQ') || to.startsWith('cnTQ')) continue;

                const assetInfo = getAssetInfo(assetId);
                const decimals = assetInfo ? assetInfo.decimals : 18;
                const symbol = assetInfo ? assetInfo.symbol : 'UNK';
                const amountBn = new BigNumber(amountStr.toString()).div(new BigNumber(10).pow(decimals));

                // Get price on-demand if not cached
                const price = await getOrFetchPrice(symbol, assetId, decimals);
                const usdValue = amountBn.times(price).toNumber();

                if (usdValue >= 0) {
                    const now = new Date();
                    const formattedTime = `${now.getDate().toString().padStart(2, '0')}/${(now.getMonth() + 1).toString().padStart(2, '0')}/${now.getFullYear()} ${now.getHours().toString().padStart(2, '0')}:${now.getMinutes().toString().padStart(2, '0')}:${now.getSeconds().toString().padStart(2, '0')}`;
                    const transferData = {
                        time: formattedTime,
                        from, to, amount: amountBn.toFormat(4),
                        symbol: assetInfo ? assetInfo.symbol : 'UNK',
                        logo: assetInfo ? assetInfo.logo : '',
                        usdValue: usdValue.toFixed(2),
                        assetId,
                        block: blockNumber,
                        hash: (phase && phase.isApplyExtrinsic) ? signedBlock.block.extrinsics[phase.asApplyExtrinsic].hash.toHex() : '',
                        extrinsic_id: (phase && phase.isApplyExtrinsic) ? `${blockNumber}-${phase.asApplyExtrinsic.toString()}` : ''
                    };

                    insertTransfer(transferData);
                    pendingTransfers.push(transferData);
                }
            }
        })();

        // Process Standard Swaps (liquidityProxy.Exchange)
        const processedPhases = new Set();

        await (async () => {
            for (const { event, phase } of limitedSwaps) {
                // Process standard liquidityProxy.Exchange
                if (event.method === 'Exchange') {
                    const d = event.data;
                    const wallet = d[0].toString();
                    const aIn = getAssetInfo(d[2]);
                    const aOut = getAssetInfo(d[3]);

                    const vIn = new BigNumber(d[4].toString()).div(new BigNumber(10).pow(aIn.decimals || 18));
                    const vOut = new BigNumber(d[5].toString()).div(new BigNumber(10).pow(aOut.decimals || 18));

                    const pIn = await getOrFetchPrice(aIn.symbol, aIn.assetId, aIn.decimals);
                    const pOut = await getOrFetchPrice(aOut.symbol, aOut.assetId, aOut.decimals);

                    const now = new Date();
                    const formattedTime = `${now.getDate().toString().padStart(2, '0')}/${(now.getMonth() + 1).toString().padStart(2, '0')}/${now.getFullYear()} ${now.getHours().toString().padStart(2, '0')}:${now.getMinutes().toString().padStart(2, '0')}:${now.getSeconds().toString().padStart(2, '0')}`;
                    const swapData = {
                        block: blockNumber, wallet: wallet, time: formattedTime,
                        in: { symbol: aIn.symbol, logo: aIn.logo, amount: vIn.toFixed(4), usd: vIn.times(pIn).toFixed(2) },
                        out: { symbol: aOut.symbol, logo: aOut.logo, amount: vOut.toFixed(4), usd: vOut.times(pOut).toFixed(2) },
                        hash: (phase && phase.isApplyExtrinsic) ? signedBlock.block.extrinsics[phase.asApplyExtrinsic].hash.toHex() : '',
                        extrinsic_id: (phase && phase.isApplyExtrinsic) ? `${blockNumber}-${phase.asApplyExtrinsic.toString()}` : ''
                    };

                    insertSwap(swapData);
                    pendingSwaps.push(swapData);
                    processedPhases.add(phase.toString());
                }
            }

        })();
    });
}


// ========== GOVERNANCE ENDPOINTS ==========

function formatChainAmount(raw, decimals = 18) {
    if (!raw && raw !== 0) return '0';
    const str = String(raw).replace(/,/g, '');
    try {
        return new BigNumber(str).dividedBy(new BigNumber(10).pow(decimals)).toFixed(4);
    } catch { return str; }
}

function blocksToTime(blocks) {
    const seconds = blocks * 6;
    if (seconds < 3600) return `${Math.round(seconds / 60)}m`;
    if (seconds < 86400) return `${(seconds / 3600).toFixed(1)}h`;
    return `${(seconds / 86400).toFixed(1)}d`;
}

function decodeProposal(prop) {
    if (!prop) return null;
    try {
        let call;
        if (prop.toHex) {
            // Already a polkadot.js type - re-decode from hex to ensure clean Call
            call = api.createType('Call', prop.toHex());
        } else if (typeof prop === 'string') {
            call = api.createType('Call', prop);
        } else {
            return { section: '?', method: '?', args: {}, description: 'Unknown format', remark: null, innerCalls: [] };
        }

        const human = call.toHuman ? call.toHuman() : {};
        const section = human.section || call.section || '?';
        const method = human.method || (typeof call.method === 'string' ? call.method : '?');
        const args = human.args || {};

        let description = `${section}.${method}`;
        let remark = null;
        let innerCalls = [];

        if (section === 'utility' && ['batchAll', 'batch', 'forceBatch'].includes(method)) {
            const callsArg = call.args[0];
            if (callsArg && callsArg.length) {
                for (const inner of callsArg) {
                    const decoded = decodeProposal(inner);
                    if (decoded && decoded.section === 'system' && decoded.method === 'remark') {
                        try {
                            const raw = inner.args[0];
                            remark = raw.toUtf8 ? raw.toUtf8() : (raw.toHuman ? raw.toHuman() : String(raw));
                        } catch { remark = decoded.args?.remark || ''; }
                    } else if (decoded) {
                        innerCalls.push(decoded);
                    }
                }
                description = remark || `Batch of ${callsArg.length} calls`;
            }
        }

        return { section, method: String(method), args, description, remark, innerCalls };
    } catch (e) {
        return { section: '?', method: '?', args: {}, description: 'Error decoding: ' + e.message, remark: null, innerCalls: [] };
    }
}

async function resolvePreimage(hash, len) {
    try {
        if (api.query.preimage && api.query.preimage.preimageFor) {
            try {
                const preimage = await withTimeout(api.query.preimage.preimageFor([hash, parseInt(len) || 0]));
                if (preimage && !preimage.isNone) {
                    const bytes = preimage.unwrap();
                    return decodeProposal(api.createType('Call', bytes.toHex ? bytes.toHex() : bytes));
                }
            } catch {}
        }
        if (api.query.democracy && api.query.democracy.preimages) {
            try {
                const prop = await withTimeout(api.query.democracy.preimages(hash));
                if (prop && !prop.isNone) {
                    const available = prop.unwrap();
                    if (available.isAvailable) {
                        return decodeProposal(api.createType('Call', available.asAvailable.data));
                    }
                }
            } catch {}
        }
    } catch {}
    return null;
}

async function attachIdentities(addresses) {
    if (!addresses || addresses.length === 0) return {};
    await resolveIdentitiesBatch(addresses.filter(a => typeof a === 'string' && a.length > 40));
    const result = {};
    for (const addr of addresses) {
        const cached = identityMemCache.get(addr);
        if (cached && cached.display) result[addr] = cached.display;
    }
    return result;
}

async function fetchCollectiveMotions(collective) {
    const query = api.query[collective];
    if (!query) return { motions: [], identities: {}, currentBlock: 0 };
    const [hashes, proposalCount, header] = await Promise.all([
        withTimeout(query.proposals()),
        withTimeout(query.proposalCount()),
        withTimeout(api.rpc.chain.getHeader())
    ]);
    const currentBlock = header.number.toNumber();
    const motions = [];
    const allAddresses = new Set();
    for (const hash of hashes) {
        try {
            const [prop, votes] = await Promise.all([
                withTimeout(query.proposalOf(hash)),
                withTimeout(query.voting(hash))
            ]);
            const decoded = prop ? decodeProposal(prop) : null;
            const v = votes ? votes.toJSON() : null;
            // Try to resolve preimage for Lookup proposals
            let resolvedProposal = null;
            if (decoded && decoded.args) {
                const pArg = decoded.args.proposal || decoded.args.proposal_hash;
                if (pArg && typeof pArg === 'object') {
                    const lookup = pArg.Lookup || pArg.lookup;
                    if (lookup) {
                        resolvedProposal = await resolvePreimage(lookup.hash_ || lookup.hash, lookup.len);
                    }
                }
            }
            if (v?.ayes) v.ayes.forEach(a => allAddresses.add(a));
            if (v?.nays) v.nays.forEach(a => allAddresses.add(a));
            let blocksRemaining = 0;
            if (v?.end) blocksRemaining = Math.max(0, v.end - currentBlock);
            motions.push({
                hash: hash.toHex(),
                index: v?.index ?? null,
                decoded,
                resolvedProposal,
                voting: v,
                blocksRemaining,
                timeRemaining: blocksToTime(blocksRemaining)
            });
        } catch (e) { }
    }
    const identities = await attachIdentities([...allAddresses]);
    return { motions, identities, currentBlock };
}

// Council members + prime + stake + identities
app.get("/governance/council", rateLimit(15, 60000), async (req, res) => {
    try {
        if (!api) return res.json({ error: "API not connected" });
        const [councilMembers, prime, electedMembers] = await Promise.all([
            withTimeout(api.query.council.members()),
            withTimeout(api.query.council.prime()),
            withTimeout(api.query.electionsPhragmen.members())
        ]);
        const elected = electedMembers.toJSON() || [];
        const stakeMap = {};
        for (const e of elected) {
            if (Array.isArray(e)) stakeMap[e[0]] = formatChainAmount(e[1]);
            else if (e.who) stakeMap[e.who] = formatChainAmount(e.stake);
        }
        const addresses = councilMembers.toJSON() || [];
        const identities = await attachIdentities(addresses);
        const members = addresses.map(addr => ({
            address: addr,
            identity: identities[addr] || null,
            stake: stakeMap[addr] || '0',
            isPrime: prime && prime.toJSON() === addr
        }));
        res.json({ members, prime: prime ? prime.toJSON() : null, identities });
    } catch (e) { res.json({ error: e.message }); }
});

// Elections: elected, candidates, runners-up, timing
app.get("/governance/elections", rateLimit(15, 60000), async (req, res) => {
    try {
        if (!api) return res.json({ error: "API not connected" });
        const [members, candidates, runnersUp, rounds, header] = await Promise.all([
            withTimeout(api.query.electionsPhragmen.members()),
            withTimeout(api.query.electionsPhragmen.candidates()),
            withTimeout(api.query.electionsPhragmen.runnersUp()),
            withTimeout(api.query.electionsPhragmen.electionRounds()),
            withTimeout(api.rpc.chain.getHeader())
        ]);
        const currentBlock = header.number.toNumber();
        let termDuration = 0, desiredMembers = 0, candidacyBond = '0';
        try { termDuration = api.consts.electionsPhragmen.termDuration.toNumber(); } catch {}
        try { desiredMembers = api.consts.electionsPhragmen.desiredMembers.toNumber(); } catch {}
        try { candidacyBond = formatChainAmount(api.consts.electionsPhragmen.candidacyBond.toString()); } catch {}

        let blocksUntilElection = 0;
        if (termDuration > 0) {
            blocksUntilElection = termDuration - (currentBlock % termDuration);
        }

        const mapSeat = (arr) => (arr.toJSON() || []).map(e => {
            if (Array.isArray(e)) return { address: e[0], stake: formatChainAmount(e[1]) };
            return { address: e.who || e[0], stake: formatChainAmount(e.stake || e[1]) };
        });
        const mapCandidate = (arr) => (arr.toJSON() || []).map(e => {
            if (Array.isArray(e)) return { address: e[0], deposit: formatChainAmount(e[1]) };
            return { address: e.who || e[0], deposit: formatChainAmount(e.deposit || e[1]) };
        });

        const electedList = mapSeat(members);
        const candidatesList = mapCandidate(candidates);
        const runnersUpList = mapSeat(runnersUp);
        const allAddresses = [...electedList, ...candidatesList, ...runnersUpList].map(i => i.address);
        const identities = await attachIdentities(allAddresses);

        res.json({
            elected: electedList,
            candidates: candidatesList,
            runnersUp: runnersUpList,
            electionRounds: rounds.toNumber(),
            currentBlock,
            termDuration,
            desiredMembers,
            candidacyBond,
            blocksUntilElection,
            timeUntilElection: blocksToTime(blocksUntilElection),
            identities
        });
    } catch (e) { res.json({ error: e.message }); }
});

// Motions: council + technical committee proposals with decoded call data
app.get("/governance/motions", rateLimit(10, 60000), async (req, res) => {
    try {
        if (!api) return res.json({ error: "API not connected" });
        const [councilResult, techResult] = await Promise.all([
            fetchCollectiveMotions('council'),
            fetchCollectiveMotions('technicalCommittee')
        ]);
        const identities = { ...councilResult.identities, ...techResult.identities };
        res.json({
            council: councilResult.motions,
            technicalCommittee: techResult.motions,
            identities,
            currentBlock: councilResult.currentBlock
        });
    } catch (e) { res.json({ error: e.message }); }
});

// Democracy: referendums + public proposals
app.get("/governance/democracy", rateLimit(10, 60000), async (req, res) => {
    try {
        if (!api) return res.json({ error: "API not connected" });
        const [refCount, lowestUnbaked, publicProps, header] = await Promise.all([
            withTimeout(api.query.democracy.referendumCount()),
            withTimeout(api.query.democracy.lowestUnbaked()),
            withTimeout(api.query.democracy.publicProps()),
            withTimeout(api.rpc.chain.getHeader())
        ]);
        const currentBlock = header.number.toNumber();
        const count = refCount.toNumber();
        const lowest = lowestUnbaked.toNumber();

        let votingPeriod = 0, enactmentPeriod = 0, launchPeriod = 0;
        try { votingPeriod = api.consts.democracy.votingPeriod.toNumber(); } catch {}
        try { enactmentPeriod = api.consts.democracy.enactmentPeriod.toNumber(); } catch {}
        try { launchPeriod = api.consts.democracy.launchPeriod.toNumber(); } catch {}

        const referendums = [];
        for (let i = lowest; i < count; i++) {
            try {
                const info = await withTimeout(api.query.democracy.referendumInfoOf(i));
                if (!info || info.isNone) continue;
                const data = info.toJSON();
                const status = data ? Object.keys(data)[0] : 'unknown';
                const detail = data ? data[status] : {};
                let decoded = null;
                if (status === 'ongoing' && detail.proposal) {
                    try {
                        const lookup = detail.proposal.lookup || detail.proposal.Lookup;
                        if (lookup) {
                            decoded = await resolvePreimage(lookup.hash_ || lookup.hash, lookup.len);
                        } else if (typeof detail.proposal === 'string') {
                            decoded = await resolvePreimage(detail.proposal);
                        }
                    } catch {}
                }
                let blocksRemaining = 0;
                if (status === 'ongoing' && detail.end) {
                    blocksRemaining = Math.max(0, detail.end - currentBlock);
                }
                referendums.push({
                    id: i, status, detail, decoded,
                    blocksRemaining, timeRemaining: blocksToTime(blocksRemaining)
                });
            } catch {}
        }

        const proposals = (publicProps.toJSON() || []).map(p => ({
            index: p[0], hash: p[1], proposer: p[2]
        }));

        res.json({
            referendums, proposals, currentBlock,
            totalReferendums: count, votingPeriod, enactmentPeriod, launchPeriod
        });
    } catch (e) { res.json({ error: e.message }); }
});

// Technical committee members + identities
app.get("/governance/technical-committee", rateLimit(15, 60000), async (req, res) => {
    try {
        if (!api) return res.json({ error: "API not connected" });
        const [members, prime] = await Promise.all([
            withTimeout(api.query.technicalCommittee.members()),
            withTimeout(api.query.technicalCommittee.prime())
        ]);
        const addresses = members.toJSON() || [];
        const identities = await attachIdentities(addresses);
        res.json({
            members: addresses.map(addr => ({
                address: addr,
                identity: identities[addr] || null,
                isPrime: prime && prime.toJSON() === addr
            })),
            prime: prime ? prime.toJSON() : null,
            identities
        });
    } catch (e) { res.json({ error: e.message }); }
});

// Voting info for an address
app.get("/governance/votes/:address", validateAddress, rateLimit(15, 60000), async (req, res) => {
    try {
        if (!api) return res.json({ error: "API not connected" });
        const { address } = req.params;
        const voting = await withTimeout(api.query.democracy.votingOf(address));
        res.json({ address, voting: voting ? voting.toJSON() : null });
    } catch (e) { res.json({ error: e.message }); }
});

// ==================== STAKING SECTION ====================

app.get("/staking/validators", rateLimit(10, 60000), async (req, res) => {
    try {
        if (!api) return res.json({ error: "API not connected" });
        const now = Date.now();
        if (validatorsGlobalCache.data && now - validatorsGlobalCache.ts < VALIDATORS_TTL) {
            return res.json(validatorsGlobalCache.data);
        }

        const activeEraOpt = await withTimeout(api.query.staking.activeEra(), 10000);
        const activeEra = activeEraOpt.unwrap();
        const eraIndex = activeEra.index.toNumber();

        // Calculate era duration in ms for accurate payout time display
        // SORA: 6 sessions/era × 600 blocks/session × 6s/block = 21600s = 6h per era
        let eraDurationMs = 6 * 60 * 60 * 1000; // default 6 hours
        try {
            const sessionsPerEra = api.consts.staking.sessionsPerEra.toNumber();
            const epochDuration = api.consts.babe.epochDuration.toNumber();
            const blockTime = api.consts.babe.expectedBlockTime.toNumber();
            eraDurationMs = sessionsPerEra * epochDuration * blockTime;
        } catch (e) { /* use default */ }

        const sessionValidators = await withTimeout(api.query.session.validators(), 10000);
        const validatorAddresses = sessionValidators.toJSON();

        const prefs = await withTimeout(api.query.staking.validators.multi(validatorAddresses), 15000);

        const eraStakerKeys = validatorAddresses.map(addr => [eraIndex, addr]);
        // erasStakers is deprecated (post-paging pallet_staking). Use overview + clipped fallback.
        const [overviews, clippedFallback] = await Promise.all([
            withTimeout(api.query.staking.erasStakersOverview.multi(eraStakerKeys), 30000),
            withTimeout(api.query.staking.erasStakersClipped.multi(eraStakerKeys), 30000)
        ]);

        const identities = await attachIdentities(validatorAddresses);

        // Payout info: query bonded controllers then ledgers for claimedRewards
        let payoutMap = {};
        try {
            const bonded = await withTimeout(api.query.staking.bonded.multi(validatorAddresses), 15000);
            const controllerAddrs = bonded.map((b, i) => b.isSome ? b.unwrap().toString() : validatorAddresses[i]);
            const ledgers = await withTimeout(api.query.staking.ledger.multi(controllerAddrs), 15000);
            ledgers.forEach((ledger, i) => {
                if (ledger.isSome) {
                    const l = ledger.unwrap().toJSON();
                    const claimed = l.claimedRewards || l.legacyClaimedRewards || [];
                    if (claimed.length > 0) {
                        const lastPayoutEra = Math.max(...claimed);
                        const erasSince = eraIndex - lastPayoutEra;
                        // Convert eras to actual days using era duration
                        const daysSincePayout = (erasSince * eraDurationMs) / (24 * 60 * 60 * 1000);
                        payoutMap[validatorAddresses[i]] = Math.round(daysSincePayout * 10) / 10; // 1 decimal
                    }
                }
            });
        } catch (e) { console.warn("Payout query failed (non-critical):", e.message); }

        const xorPrice = tokenPrices['XOR'] || 0;
        const validators = validatorAddresses.map((addr, i) => {
            const pref = prefs[i].toJSON();

            let totalStakeRaw = '0';
            let ownStakeRaw = '0';
            let othersCount = 0;
            if (overviews[i].isSome) {
                const ov = overviews[i].unwrap();
                totalStakeRaw = ov.total.toString();
                ownStakeRaw = ov.own.toString();
                othersCount = ov.nominatorCount.toNumber();
            } else {
                const c = clippedFallback[i].toJSON();
                totalStakeRaw = c.total || '0';
                ownStakeRaw = c.own || '0';
                othersCount = c.others ? c.others.length : 0;
            }

            const commissionPerbill = pref.commission || 0;
            const commissionPercent = (commissionPerbill / 1_000_000_000 * 100).toFixed(2);

            const totalStake = new BigNumber(String(totalStakeRaw).replace(/,/g, '')).div('1e18');
            const ownStake = new BigNumber(String(ownStakeRaw).replace(/,/g, '')).div('1e18');
            const otherStake = totalStake.minus(ownStake);

            const erasSincePayout = payoutMap[addr] !== undefined ? payoutMap[addr] : null;

            return {
                address: addr,
                identity: identities[addr] || null,
                commission: parseFloat(commissionPercent),
                totalStake: totalStake.toNumber(),
                ownStake: ownStake.toNumber(),
                otherStake: otherStake.toNumber(),
                nominatorsCount: othersCount,
                isBlocked: !!pref.blocked,
                erasSincePayout
            };
        });

        const result = { era: eraIndex, validatorCount: validators.length, validators, xorPrice };
        validatorsGlobalCache = { data: result, ts: Date.now() };
        res.json(result);
    } catch (e) {
        console.error("Error /staking/validators:", e.message);
        res.json({ error: e.message });
    }
});

app.get("/staking/network", rateLimit(15, 60000), async (req, res) => {
    try {
        if (!api) return res.json({ error: "API not connected" });
        const now = Date.now();
        if (networkStakingCache.data && now - networkStakingCache.ts < NETWORK_STAKING_TTL) {
            return res.json(networkStakingCache.data);
        }

        const [activeEraOpt, currentEraOpt, sessionIndex, bestHeader, finalizedHash,
               sessionValidators, validatorCountTarget, intentCounter,
               minNomBondRaw, minValBondRaw] = await Promise.all([
            withTimeout(api.query.staking.activeEra()),
            withTimeout(api.query.staking.currentEra()),
            withTimeout(api.query.session.currentIndex()),
            withTimeout(api.rpc.chain.getHeader()),
            withTimeout(api.rpc.chain.getFinalizedHead()),
            withTimeout(api.query.session.validators()),
            withTimeout(api.query.staking.validatorCount()),
            withTimeout(api.query.staking.counterForValidators()),
            withTimeout(api.query.staking.minNominatorBond()),
            withTimeout(api.query.staking.minValidatorBond()),
        ]);

        const activeEra = activeEraOpt.unwrap();
        const eraIndex = activeEra.index.toNumber();
        const eraStart = activeEra.start.isSome ? activeEra.start.unwrap().toNumber() : null;
        const currentEra = currentEraOpt.isSome ? currentEraOpt.unwrap().toNumber() : eraIndex;
        const currentSession = sessionIndex.toNumber();
        const bestBlock = bestHeader.number.toNumber();

        const finalizedHeader = await withTimeout(api.rpc.chain.getHeader(finalizedHash));
        const finalizedBlock = finalizedHeader.number.toNumber();

        const sessionsPerEra = api.consts.staking.sessionsPerEra?.toNumber?.() || 6;
        const epochDurationBlocks = api.consts.babe.epochDuration?.toNumber?.() || 600;
        const expectedBlockTime = api.consts.babe.expectedBlockTime?.toNumber?.() || 6000;
        const bondingDurationEras = api.consts.staking.bondingDuration?.toNumber?.() || 28;

        const eraSeconds = (sessionsPerEra * epochDurationBlocks * expectedBlockTime) / 1000;
        const epochSeconds = (epochDurationBlocks * expectedBlockTime) / 1000;
        const unbondingDays = (bondingDurationEras * eraSeconds) / 86400;

        let sessionProgress = 0, eraProgress = 0, epochProgress = 0;
        try {
            const eraSessionStart = await withTimeout(api.query.staking.erasStartSessionIndex(eraIndex));
            const eraStartSession = eraSessionStart.isSome ? eraSessionStart.unwrap().toNumber() : 0;
            sessionProgress = currentSession - eraStartSession;
            eraProgress = parseFloat((sessionProgress / sessionsPerEra * 100).toFixed(1));
            const blocksIntoSession = bestBlock % epochDurationBlocks;
            epochProgress = parseFloat((blocksIntoSession / epochDurationBlocks * 100).toFixed(1));
        } catch (e) { /* ignore */ }

        let totalIssuance = null, totalStaked = null, stakingRatio = null;
        try {
            const [issuanceRaw, erasTotalStakeRaw] = await Promise.all([
                withTimeout(api.query.balances.totalIssuance()),
                withTimeout(api.query.staking.erasTotalStake(eraIndex))
            ]);
            // Keep as string to avoid Number precision loss above 2^53
            totalIssuance = new BigNumber(issuanceRaw.toString()).div('1e18').toFixed(2);
            totalStaked = new BigNumber(erasTotalStakeRaw.toString()).div('1e18').toFixed(2);
            if (parseFloat(totalIssuance) > 0) {
                stakingRatio = ((parseFloat(totalStaked) / parseFloat(totalIssuance)) * 100).toFixed(4);
            }
        } catch (e) { /* ignore */ }

        // Find last era with non-zero validator reward (scan back up to historyDepth)
        let lastRewardEra = null, lastRewardAmount = null;
        try {
            const historyDepth = api.consts.staking.historyDepth?.toNumber?.() ?? 84;
            const erasToScan = [];
            for (let e = eraIndex - 1; e >= Math.max(0, eraIndex - historyDepth); e--) erasToScan.push(e);
            const rewards = await withTimeout(api.query.staking.erasValidatorReward.multi(erasToScan), 30000);
            for (let i = 0; i < erasToScan.length; i++) {
                if (rewards[i].isSome) {
                    const v = rewards[i].unwrap();
                    if (!v.isZero()) {
                        lastRewardEra = erasToScan[i];
                        lastRewardAmount = new BigNumber(v.toString()).div('1e18').toFixed(2);
                        break;
                    }
                }
            }
        } catch (e) { /* ignore */ }

        const xorPrice = tokenPrices['XOR'] || 0;
        const activeValidatorsCount = sessionValidators.length;
        const targetCount = validatorCountTarget.toNumber();
        const intentCount = intentCounter.toNumber();
        const waitingValidators = Math.max(0, intentCount - activeValidatorsCount);

        const eraStartedAgo = eraStart ? `${Math.floor((Date.now() - eraStart) / 60000)} min ago` : '';
        const epochDurationLabel = epochSeconds >= 3600
            ? `${(epochSeconds / 3600).toFixed(1)}h`
            : `${(epochSeconds / 60).toFixed(0)}min`;

        const result = {
            // Legacy fields (kept for backwards compat with other consumers)
            activeEra: eraIndex, currentEra, eraStart,
            sessionIndex: currentSession, sessionsPerEra, sessionProgress, eraProgress,
            expectedBlockTime, bestBlock, finalizedBlock,
            totalIssuance, totalStaked, stakingRatio,
            validatorCount: activeValidatorsCount, avgBlockTime: expectedBlockTime / 1000,

            // Fields the Staking Network Info UI consumes
            era: eraIndex,
            totalStake: totalStaked,
            totalStakeUsd: totalStaked && xorPrice ? (parseFloat(totalStaked) * xorPrice).toFixed(2) : null,
            epochProgress: epochProgress + '%',
            epochsPerEra: sessionsPerEra,
            epochDuration: epochDurationLabel,
            activeValidators: activeValidatorsCount,
            waitingValidators,
            validatorTarget: targetCount,
            minNominatorBond: new BigNumber(minNomBondRaw.toString()).div('1e18').toFixed(4),
            minValidatorBond: new BigNumber(minValBondRaw.toString()).div('1e18').toFixed(4),
            lastRewardEra,
            lastRewardAmount,
            idealStakeRate: null, // SORA EraPayout=() → no standard inflation curve
            currentInflation: 0,  // EraPayout=() → 0 XOR minted per era
            unbondingDays: parseFloat(unbondingDays.toFixed(1)),
            unbondingEras: bondingDurationEras,
            eraStartedAgo,
        };

        networkStakingCache = { data: result, ts: Date.now() };
        res.json(result);
    } catch (e) {
        console.error("Error /staking/network:", e.message);
        res.json({ error: e.message });
    }
});

// Live pipeline endpoint — serves from in-memory state populated by storage subscribers.
// Zero load on the node per request. Frontend polls every 30s.
// Token price comparison tool: aligned historical price series for up to 4 tokens.
// ?assets=<assetId>,<assetId>  &window=7d|30d|90d|365d|all   (assetIds are 0x… hex)
// Real data from sm.price_history only. Cached 5 min (history changes slowly).
const _priceSeriesCache = new Map(); // key = `${assets}|${window}` -> { data, ts }
app.get('/tools/price-series', rateLimit(60, 60000), async (req, res) => {
    try {
        const assets = String(req.query.assets || '').split(',').map(s => s.trim()).filter(s => /^0x[0-9a-fA-F]{64}$/.test(s)).slice(0, 4);
        const window = ['7d', '30d', '90d', '365d', 'all'].includes(req.query.window) ? req.query.window : '30d';
        if (assets.length === 0) return res.status(400).json({ error: 'no valid asset ids (expect 0x… 64-hex, comma-separated)' });
        const key = `${assets.join(',')}|${window}`;
        const hit = _priceSeriesCache.get(key);
        if (hit && Date.now() - hit.ts < 300000) return res.json(hit.data);
        const data = await getPriceSeries(assets, window);
        _priceSeriesCache.set(key, { data, ts: Date.now() });
        res.json(data);
    } catch (e) {
        console.error('Error /tools/price-series:', e.message);
        res.status(500).json({ error: e.message });
    }
});

app.get('/staking/rewards/live', rateLimit(120, 60000), (req, res) => {
    const remintPeriod = api?.query?.xorFee?.remintPeriod ? 100 : 100; // const default; could read on boot
    const blocksSinceLastRemint = null; // could derive from event subscriber; left null for now
    res.json({
        xorToVal: pipelineState.xorToVal,
        xorToBuyBack: pipelineState.xorToBuyBack,
        valStakingEraReward: pipelineState.valStakingEraReward,
        valBucketPrevEra: pipelineState.valBucketPrevEra,
        unassignedValStakingReward: pipelineState.unassignedValStakingReward,
        activeEra: pipelineState.activeEra,
        bestBlock: pipelineState.bestBlock,
        lastUpdate: pipelineState.lastUpdate,
        remintPeriod,
        blocksSinceLastRemint,
        history: pipelineState.history,
        xorPrice: tokenPrices['XOR'] || 0,
        valPrice: tokenPrices['VAL'] || 0,
    });
});

// Real-data only. Reads on-chain state + indexed VAL payouts from sm.val_staking_rewards.
// Returns 0/null when no data exists. No estimations, no projections.
let rewardsCache = { data: null, ts: 0 };
const REWARDS_TTL = 60_000; // 1 min

app.get("/staking/rewards", rateLimit(15, 60000), async (req, res) => {
    try {
        if (!api) return res.json({ error: "API not connected" });
        const now = Date.now();
        if (rewardsCache.data && now - rewardsCache.ts < REWARDS_TTL) {
            return res.json(rewardsCache.data);
        }

        const activeEra = (await withTimeout(api.query.staking.activeEra())).unwrap();
        const eraIndex = activeEra.index.toNumber();
        const historyDepth = api.consts.staking.historyDepth?.toNumber?.() ?? 84;
        const sessionsPerEra = api.consts.staking.sessionsPerEra?.toNumber?.() ?? 6;
        const epochDurationBlocks = api.consts.babe.epochDuration?.toNumber?.() ?? 600;
        const blockTimeMs = api.consts.babe.expectedBlockTime?.toNumber?.() ?? 6000;
        const eraSeconds = (sessionsPerEra * epochDurationBlocks * blockTimeMs) / 1000;

        // 1. Network totals from indexed events (real, may be all zero pre-enactment).
        const networkTotals = await getValStakingNetworkTotals().catch(e => {
            console.warn('[rewards] networkTotals failed:', e.message);
            return {};
        });

        // 2. Per-validator from indexed events (real).
        const indexedPerValidator = await getValStakingPerValidator().catch(() => []);
        const indexedMap = new Map(indexedPerValidator.map(r => [r.validator_stash, r]));

        // 3. Active validator set + their on-chain context (real).
        const validators = (await withTimeout(api.query.session.validators())).map(v => v.toString());
        const eraStakerKeys = validators.map(addr => [eraIndex, addr]);
        const [overviews, clippedFallback, prefsAll, bondedList] = await Promise.all([
            withTimeout(api.query.staking.erasStakersOverview.multi(eraStakerKeys), 30000),
            withTimeout(api.query.staking.erasStakersClipped.multi(eraStakerKeys), 30000),
            withTimeout(api.query.staking.validators.multi(validators), 15000),
            withTimeout(api.query.staking.bonded.multi(validators), 15000),
        ]);
        const controllers = bondedList.map((b, i) => b.isSome ? b.unwrap().toString() : validators[i]);
        const ledgers = await withTimeout(api.query.staking.ledger.multi(controllers), 15000);

        // 4. Reward points across the FULL claimable window (HistoryDepth, ~84 eras = 21 days),
        // so outstanding/pending covers ALL claimable VAL, not just the last 30.
        const recentEras = [];
        for (let e = eraIndex - 1; e >= Math.max(0, eraIndex - historyDepth); e--) recentEras.push(e);
        const rewardPointsByEra = await withTimeout(
            api.query.staking.erasRewardPoints.multi(recentEras),
            45000
        );

        // 4b. Network totals — REAL on-chain only. No CoinGecko/MOF.
        // balances.totalIssuance / 10^18 = the real supply post-denomination (matches MOF).
        // For SORA v2 this returns ~9.99×10^17 XOR — large because the treasury holds
        // ~99.99% of supply in legitimate post-denomination units (verified with user).
        let xorSupplyOnchain = null;
        let totalStakedRaw = null;
        try {
            const [issuanceRaw, stakedRaw] = await Promise.all([
                withTimeout(api.query.balances.totalIssuance(), 10000),
                withTimeout(api.query.staking.erasTotalStake(eraIndex), 10000),
            ]);
            xorSupplyOnchain = new BigNumber(issuanceRaw.toString()).div(new BigNumber(10).pow(18)).toNumber();
            totalStakedRaw = stakedRaw.toString();
        } catch (e) { console.warn('[rewards] on-chain network totals failed:', e.message); }

        // 5. Pending VAL bucket per era (only exists post-4.8.6). null arrays otherwise.
        let valBucketCurrent = null;
        let valBucketUnassigned = null;
        let bucketByEra = null; // Map<era, BigInt> for outstanding calc, only when storage exists
        const erasWithBucket = []; // eras that actually carry VAL (post-4.8.6) — the only claimable ones
        if (api.query.xorFee?.valStakingEraReward) {
            try {
                const b = await api.query.xorFee.valStakingEraReward(eraIndex);
                valBucketCurrent = b.toString();
            } catch {}
            // Pull buckets for the full HistoryDepth window; keep only eras with VAL.
            try {
                const buckets = await withTimeout(api.query.xorFee.valStakingEraReward.multi(recentEras), 45000);
                bucketByEra = new Map();
                recentEras.forEach((e, i) => {
                    const raw = buckets[i].toString();
                    if (raw !== '0') { bucketByEra.set(e, BigInt(raw)); erasWithBucket.push(e); }
                });
            } catch {}
        }
        if (api.query.xorFee?.unassignedValStakingReward) {
            try {
                const u = await api.query.xorFee.unassignedValStakingReward();
                valBucketUnassigned = u.toString();
            } catch {}
        }

        // Per-validator claimed pages — only for eras that carry VAL (the only ones that
        // can yield a payout). Avoids querying claimedRewards for all 84×N eras.
        let claimedByValidatorEra = new Map(); // key = `${validator}:${era}` -> Set<page>
        try {
            const claimKeys = [];
            for (const e of erasWithBucket) {
                for (const v of validators) claimKeys.push([e, v]);
            }
            // batching: 200 at a time
            const CHUNK = 200;
            for (let i = 0; i < claimKeys.length; i += CHUNK) {
                const slice = claimKeys.slice(i, i + CHUNK);
                const rows = await withTimeout(api.query.staking.claimedRewards.multi(slice), 30000);
                rows.forEach((row, j) => {
                    const [era, val] = slice[j];
                    const pages = row.toJSON();
                    if (Array.isArray(pages) && pages.length > 0) {
                        claimedByValidatorEra.set(`${val}:${era}`, new Set(pages));
                    }
                });
            }
        } catch (e) {
            console.warn('[rewards] claimedRewards bulk failed:', e.message);
        }

        // Also fold in eras the INDEXER already saw paid (sm.val_staking_rewards). The local node's
        // staking.claimedRewards lags behind chain head, so a just-claimed era can still look pending
        // for a while; the indexer records the payout the moment it lands. Union of both = no stale
        // "pending" after a claim, and no false "claimed" (indexer only has real ValStakingRewardPaid).
        try {
            const indexedClaimed = await getClaimedValStakingPairs();
            for (const key of indexedClaimed) {
                if (!claimedByValidatorEra.has(key)) {
                    const [val, era] = key.split(':');
                    claimedByValidatorEra.set(`${val}:${era}`, new Set([0]));
                }
            }
        } catch (e) {
            console.warn('[rewards] indexed claimed merge failed:', e.message);
        }

        // Per-(validator,era) exposure (own/total) for the eras that carry VAL, so ownOutstanding
        // uses THAT era's own/total — not the current era's (the stake can change era to era).
        // One .multi batch keyed `${val}:${era}`. Commission goes in full (single-page validators),
        // so we don't need the paged exposure here. Verified to the wei vs on-chain payout (era 7211).
        const exposureByValidatorEra = new Map(); // -> { own, total } as BigInt
        try {
            const expKeys = [];
            for (const e of erasWithBucket) {
                for (const v of validators) expKeys.push([e, v]);
            }
            const CHUNK = 200;
            for (let i = 0; i < expKeys.length; i += CHUNK) {
                const slice = expKeys.slice(i, i + CHUNK);
                const ovs = await withTimeout(api.query.staking.erasStakersOverview.multi(slice), 30000);
                slice.forEach(([era, val], j) => {
                    if (!ovs[j].isSome) return;
                    const o = ovs[j].unwrap();
                    exposureByValidatorEra.set(`${val}:${era}`, {
                        own: BigInt(o.own.toString()),
                        total: BigInt(o.total.toString()),
                    });
                });
            }
        } catch (e) {
            console.warn('[rewards] erasStakersOverview bulk failed:', e.message);
        }

        const xorPrice = tokenPrices['XOR'] || 0;
        const valPrice = tokenPrices['VAL'] || 0;

        // Direct VAL→XOR exchange rate from the DEX (how many XOR for 1 VAL) — the
        // honest ratio for "is claiming worth it" since fee is XOR and reward is VAL.
        // No USD detour. One quote per request (cached 60s by the endpoint cache).
        let valToXorRate = null;
        try {
            if (api.rpc?.liquidityProxy?.quote) {
                const VAL_ID = '0x0200040000000000000000000000000000000000000000000000000000000000';
                const rawIn = (10n ** 12n).toString(); // 0.000001 VAL (18 dec)
                const q = await withTimeout(api.rpc.liquidityProxy.quote(0, VAL_ID, XOR_ID, rawIn, 'WithDesiredInput', [], 'Disabled'));
                const out = q.toJSON()?.amount;
                if (out != null) valToXorRate = Number(BigInt(out)) / 1e12; // (out/1e18)/0.000001
            }
        } catch (e) { /* DEX quote unavailable → null */ }

        // Pre-compute total reward points per era for outstanding/yield calculations.
        const totalPointsByEra = new Map();
        recentEras.forEach((era, k) => {
            totalPointsByEra.set(era, rewardPointsByEra[k].total.toNumber());
        });

        // Per-validator rows, combining real on-chain + indexed real payouts.
        const perValidator = validators.map((addr, i) => {
            // Exposure (current era)
            let own = '0', total = '0', nominators = 0;
            if (overviews[i].isSome) {
                const ov = overviews[i].unwrap();
                own = ov.own.toString();
                total = ov.total.toString();
                nominators = ov.nominatorCount.toNumber();
            } else {
                const c = clippedFallback[i].toJSON();
                own = c.own || '0';
                total = c.total || '0';
                nominators = c.others ? c.others.length : 0;
            }
            // Commission (current prefs)
            const prefs = prefsAll[i].toJSON();
            const commissionPerbill = prefs.commission || 0;
            const commission = commissionPerbill / 1_000_000_000;
            // Sum reward points over last 30 eras (real)
            let totalPts = 0, erasProduced = 0;
            const accId = api.registry.createType('AccountId', addr);
            for (let k = 0; k < rewardPointsByEra.length; k++) {
                const rp = rewardPointsByEra[k];
                rp.individual.forEach((value, key) => {
                    if (key.toString() === addr) {
                        const pts = value.toNumber();
                        if (pts > 0) { totalPts += pts; erasProduced++; }
                    }
                });
            }
            const avgPtsPerEra = erasProduced > 0 ? Math.round(totalPts / erasProduced) : 0;
            // Last claim from ledger.legacyClaimedRewards (on-chain).
            let lastClaimEra = null;
            const l = ledgers[i];
            if (l && l.isSome) {
                const claimed = l.unwrap().legacyClaimedRewards.map(e => e.toNumber());
                if (claimed.length > 0) lastClaimEra = Math.max(...claimed);
            }
            // Indexed real payouts for this validator (post-enactment data).
            const ix = indexedMap.get(addr) || { total_amount: '0', payout_count: 0, era_count: 0, last_era: null, last_ts: null };
            // If indexed shows a more recent era than legacy, prefer it.
            const lastEraFinal = ix.last_era != null && (lastClaimEra == null || ix.last_era > lastClaimEra)
                ? ix.last_era : lastClaimEra;
            const lastTsFinal = ix.last_ts
                ? new Date(ix.last_ts).toISOString()
                : (lastEraFinal != null ? new Date(Date.now() - (eraIndex - lastEraFinal) * eraSeconds * 1000).toISOString() : null);

            // VAL outstanding (real): sum over recent eras where validator earned points
            // AND has unclaimed pages AND bucket exists. Pre-4.8.6: bucketByEra=null → 0.
            //   valOutstandingRaw         = total payout pending (validator + nominators)
            //   ownOutstandingRaw         = only what the VALIDATOR collects (commission + own-exposure share)
            //   pendingErasCount          = number of unclaimed eras → each needs 1 payout_stakers call
            // Each payout_stakers costs ~0.01 XOR (measured on-chain), so reclaiming N eras
            // costs N × 0.01 XOR. That's the "cost rises with pending eras" the user cares about.
            let valOutstandingRaw = 0n;
            let ownOutstandingRaw = 0n;
            let pendingErasCount = 0;
            if (bucketByEra) {
                const commPerbill = BigInt(Math.round(commission * 1_000_000_000));
                for (let k = 0; k < recentEras.length; k++) {
                    const era = recentEras[k];
                    const bucket = bucketByEra.get(era);
                    if (!bucket) continue;
                    const totalPts = totalPointsByEra.get(era) || 0;
                    if (totalPts === 0) continue;
                    let myPts = 0;
                    rewardPointsByEra[k].individual.forEach((value, key) => {
                        if (key.toString() === addr) myPts = value.toNumber();
                    });
                    if (myPts === 0) continue;
                    // Has this era been claimed already?
                    const claimedSet = claimedByValidatorEra.get(`${addr}:${era}`);
                    if (claimedSet && claimedSet.size > 0) continue; // simplification: any page claimed = era done
                    // validator total payout for the era (pre-commission split)
                    const t = (bucket * BigInt(myPts)) / BigInt(totalPts);
                    valOutstandingRaw += t;
                    pendingErasCount++;
                    // What the validator's own stash keeps — matches the runtime
                    // (xor_fee_impls.rs:pay_val_staking_reward) for single-page validators (all of
                    // SORA, ≤256 nominators): commission in full + own-exposure share of the rest.
                    //   commission = comm·t
                    //   staking    = own/total × (t − comm·t)
                    // The runtime scales commission by page_total()/total, but page_total() INCLUDES
                    // own, so for 1 page it equals total → factor 1 → full commission. VERIFIED to
                    // the wei against the real on-chain ValStakingRewardPaid (era 7211: 14.592738…266
                    // == formula, diff 0). Uses each era's own/total (not the current era's).
                    const exp = exposureByValidatorEra.get(`${addr}:${era}`);
                    const eOwn = exp ? exp.own : BigInt(own);
                    const eTotal = exp ? exp.total : BigInt(total);
                    const commT = t * commPerbill / 1_000_000_000n;
                    const leftover = t - commT;
                    if (eTotal > 0n) {
                        ownOutstandingRaw += commT + (leftover * eOwn / eTotal);
                    }
                }
            }

            // Nominator yield rate per XOR. Computed on the MOST RECENT era that actually
            // carries VAL (erasWithBucket[0]) — NOT the in-progress era, which is empty most
            // of the time (filled in bursts at remint) and not in recentEras anyway.
            // Formula: (1 - c) * T / total_stake, where T = (myPts / totalPts) * bucket.
            let yieldRateNominatorPerXorPerEra = null;
            if (erasWithBucket.length > 0 && BigInt(total) > 0n) {
                const yieldEra = erasWithBucket[0]; // recentEras is desc → first with bucket = most recent
                const yIdx = recentEras.indexOf(yieldEra);
                const totalPtsY = totalPointsByEra.get(yieldEra) || 0;
                if (yIdx >= 0 && totalPtsY > 0) {
                    let myPtsY = 0;
                    rewardPointsByEra[yIdx].individual.forEach((value, key) => {
                        if (key.toString() === addr) myPtsY = value.toNumber();
                    });
                    const bucketY = bucketByEra.get(yieldEra);
                    if (bucketY && myPtsY > 0) {
                        const tRaw = (bucketY * BigInt(myPtsY)) / BigInt(totalPtsY);
                        const commPerbill = BigInt(Math.round(commission * 1_000_000_000));
                        const leftoverRaw = tRaw - (tRaw * commPerbill / 1_000_000_000n);
                        yieldRateNominatorPerXorPerEra = (leftoverRaw * 1_000_000_000_000n / BigInt(total)).toString();
                    }
                }
            }

            return {
                address: addr,
                commission,
                own, total, nominators,
                avgRewardPointsPerEra: avgPtsPerEra,
                erasProducedRecent: erasProduced,
                lastClaimEra: lastEraFinal,
                lastClaimTs: lastTsFinal,
                indexedTotalValReceived: ix.total_amount,    // raw 18-dec string
                indexedPayoutCount: ix.payout_count,
                indexedErasCovered: ix.era_count,
                valOutstanding: valOutstandingRaw.toString(),       // raw 18-dec total pending (validator + noms)
                ownOutstanding: ownOutstandingRaw.toString(),       // raw 18-dec — only what the validator keeps
                pendingErasCount,                                    // unclaimed eras → payout_stakers calls needed
                yieldRateNominatorPerXorPerEra,                      // string * 1e12 ratio, or null
            };
        });

        const result = {
            era: eraIndex,
            historyDepth,
            eraSeconds,
            xorPrice,
            valPrice,
            valToXorRate,                                  // XOR per 1 VAL, direct from DEX (null if unavailable)
            valToXorRateWindows: await getValXorRateWindows().catch(() => null), // {h24,d7,d30}:{min,max} from price_history
            valBucketCurrentEra: valBucketCurrent,        // null pre-4.8.6
            valBucketUnassigned: valBucketUnassigned,      // null pre-4.8.6
            valBurnPercent: 0.10,                          // 4.8.6: 10% of bucket is burnt-and-not-redistributed
            xorTotalSupply: xorSupplyOnchain,              // SORA v2 native XOR supply, on-chain only (balances.totalIssuance / 10^28)
            totalStaked: totalStakedRaw,                    // raw 18-dec string (needs /1e18)
            networkTotals,                                  // {all,h6,h12,h24,d3,d6,d30,d90,d180,d365}
            topDestinations: await getValStakingTopDestinations(10).catch(() => []),
            validators: perValidator,
        };

        rewardsCache = { data: result, ts: Date.now() };
        res.json(result);
    } catch (e) {
        console.error('Error /staking/rewards:', e.message);
        res.status(500).json({ error: e.message });
    }
});

// Recent blocks ring buffer — populated by subscribeNewHeads, served via API
const recentBlocksBuffer = [];
const RECENT_BLOCKS_MAX = 20;
let _sessionValidatorsCache = null;

app.get("/staking/recent-blocks", rateLimit(30, 60000), async (req, res) => {
    try {
        if (!api) return res.json({ error: "API not connected" });
        if (recentBlocksBuffer.length > 0) {
            return res.json({ blocks: recentBlocksBuffer });
        }
        // Cold start: fetch last 15 blocks
        if (!_sessionValidatorsCache) {
            _sessionValidatorsCache = (await withTimeout(api.query.session.validators())).toJSON();
        }
        const header = await withTimeout(api.rpc.chain.getHeader());
        const best = header.number.toNumber();
        const blocks = [];
        const now = Date.now();
        for (let i = 0; i < 15; i++) {
            try {
                const num = best - i;
                const hash = await api.rpc.chain.getBlockHash(num);
                const [blk, derivedHeader] = await Promise.all([
                    api.rpc.chain.getBlock(hash),
                    api.derive.chain.getHeader(hash)
                ]);
                const author = derivedHeader.author ? derivedHeader.author.toString() : null;
                blocks.push({
                    number: num,
                    hash: hash.toHex(),
                    validator: author,
                    validatorName: null,
                    extrinsics: blk.block.extrinsics.length,
                    age: i * 6
                });
            } catch (e) { /* skip */ }
        }
        // Resolve identities for validators
        const addrs = [...new Set(blocks.map(b => b.validator).filter(Boolean))];
        if (addrs.length > 0) {
            const ids = await attachIdentities(addrs);
            blocks.forEach(b => { if (b.validator && ids[b.validator]) b.validatorName = ids[b.validator]; });
        }
        blocks.forEach(b => recentBlocksBuffer.push(b));
        res.json({ blocks });
    } catch (e) {
        console.error("Error /staking/recent-blocks:", e.message);
        res.json({ error: e.message });
    }
});

startApp();

// --- GRACEFUL SHUTDOWN ---
function gracefulShutdown(signal) {
    console.log(`\n🛑 ${signal} received. Shutting down gracefully...`);
    server.close(() => {
        console.log('✅ HTTP server closed.');
        process.exit(0);
    });
    // Forzar cierre si tarda más de 10s
    setTimeout(() => {
        console.error('⚠️ Forced shutdown after timeout.');
        process.exit(1);
    }, 10000);
}
process.on('SIGTERM', () => gracefulShutdown('SIGTERM'));
process.on('SIGINT', () => gracefulShutdown('SIGINT'));
