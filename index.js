const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const cors = require('cors');
const https = require('https');
const BigNumber = require('bignumber.js');
const { initApi } = require('./blockchain');
const { initDB, insertTransfer, getTransfers, getLatestTransfers, insertSwap, getSwaps, getLatestSwaps, getCandles, getPriceChange, getSparkline, getTotalStats, insertBridge, getFilteredStats, insertFee, getFeeStats, getFeeTrend, getWalletBridges, getLatestBridges, getLpVolume, insertLiquidityEvent, getTransferVolume, getPoolActivity, getNetworkTrend } = require('./db');
// ... (imports)




const { WS_ENDPOINT, WHITELIST_URL } = require('./config');
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

const app = express();
app.use(cors());
app.use(express.json());
app.use(express.static(__dirname));

// Helper: queries con timeout para evitar memory leak
// Forzar carga de favicon
app.get('/favicon.svg', (req, res) => res.sendFile(__dirname + '/favicon.svg'));

// --- VERSION CHECK ENDPOINT (FUERZA ACTUALIZACION EN iOS PWA) ---
const SERVER_VERSION = 'v4.0';
app.get('/api/version', (req, res) => {
    res.setHeader('Cache-Control', 'no-store, no-cache, must-revalidate');
    res.json({ version: SERVER_VERSION });
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

            if (imageCache.size > 2000) imageCache.clear();
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

app.get('/proxy-image', (req, res) => {
    const targetUrl = req.query.url;
    if (!targetUrl) return res.status(400).send('No URL');

    let u;
    try { u = new URL(targetUrl); } catch (e) { return res.status(400).send('Bad URL'); }

    // Solo http/https
    if (!['http:', 'https:'].includes(u.protocol)) return res.status(400).send('Bad protocol');

    // Bloqueo básico anti-SSRF (localhost / redes privadas)
    const host = (u.hostname || '').toLowerCase();
    const isPrivate =
        host === 'localhost' ||
        host.endsWith('.local') ||
        host === '0.0.0.0' ||
        host.startsWith('127.') ||
        host.startsWith('10.') ||
        host.startsWith('192.168.') ||
        /^172\.(1[6-9]|2\d|3[0-1])\./.test(host) ||
        host === '169.254.169.254';

    if (isPrivate) return sendPlaceholder(res);

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
const CACHE_DURATION = 5 * 60 * 1000;

// Caché para endpoints
let swapsCache = { data: null, timestamp: 0 };
let transfersCache = { data: null, timestamp: 0 };
let tokensCache = { data: null, timestamp: 0 };
let poolsCache = { data: null, timestamp: 0 };
let providersCache = { data: null, timestamp: 0 };
let activityCache = { data: null, timestamp: 0 };

const SWAPS_TTL = 24 * 1000;    // 24s
const TRANSFERS_TTL = 60 * 1000; // 60s
const TOKENS_TTL = 30 * 1000;   // 30s
const POOLS_TTL = 60 * 1000;    // 60s
const PROVIDERS_TTL = 90 * 1000; // 90s
const ACTIVITY_TTL = 90 * 1000;  // 90s



// --- PRICE CALCULATION (RESERVE BASED - Same as backfiller for consistency) ---
const XOR_ID = '0x0200000000000000000000000000000000000000000000000000000000000000';
const XSTUS_ID = '0x0200080000000000000000000000000000000000000000000000000000000000'; // XSTUSD
const DAI_ID = '0x0200060000000000000000000000000000000000000000000000000000000000';
const XST_ID = '0x0200090000000000000000000000000000000000000000000000000000000000'; // XST

// --- BATCHING PARA WEBSOCKET (ANTI-SATURACIÓN) ---

const BATCH_INTERVAL_MS = 5000; // 5 segundos para blindar la red
const MAX_EVENTS_PER_BATCH = 300; // Límite más bajo para evitar payloads gigantes
let pendingTransfers = [];
let pendingSwaps = [];
let lastBatchTime = Date.now();
let sessionStats = { extrinsics: 0, bridges: 0, block: 0 };

async function loadOfficialWhitelist() {
    try {
        console.log('📥 Descargando lista oficial de activos...');
        const fetch = (...args) => import('node-fetch').then(({ default: fetch }) => fetch(...args));
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
        const jsonData = reserves.toJSON();
        if (!jsonData || jsonData.length < 2) return 0;

        // Reserves: [XOR, DAI] - sorted by AssetID
        const xorRes = new BigNumber(jsonData[0]);
        const daiRes = new BigNumber(jsonData[1]);

        if (xorRes.isZero()) return 0;
        return daiRes.div(xorRes).toNumber();
    } catch (e) {
        console.error('❌ Error fetching XOR/DAI reserves:', e.message);
        return 0;
    }
}

async function getTokenPriceInXor(assetId, tokenDecimals) {
    try {
        const reserves = await withTimeout(api.query.poolXYK.reserves(XOR_ID, assetId));
        const jsonData = reserves.toJSON();
        if (!jsonData || jsonData.length < 2) return 0;

        const xorRes = new BigNumber(jsonData[0]);
        const tokenRes = new BigNumber(jsonData[1]);

        if (tokenRes.isZero()) return 0;

        // Spot Price = XOR_Reserves / Token_Reserves (normalized)
        const xorNormal = xorRes.div('1e18');
        const tokenNormal = tokenRes.div(new BigNumber(10).pow(tokenDecimals));

        return xorNormal.div(tokenNormal).toNumber();
    } catch (e) { return 0; }
}


async function getPriceInDai(assetId, decimals) {
    try {
        // CHECK CACHE FIRST - evitar queries innecesarias
        const cacheKey = `${assetId}_${decimals}`;
        if (getPriceInDai.cache && getPriceInDai.cache[cacheKey] && (Date.now() - getPriceInDai.cacheTime < 60000)) {
            return getPriceInDai.cache[cacheKey];
        }
        
        if (assetId === DAI_ID) return 1;
        if (!api) return 0;
        
        // Init cache if not exists
        if (!getPriceInDai.cache) {
            getPriceInDai.cache = {};
            getPriceInDai.cacheTime = 0;
        }

        // --- SPECIAL CASE: XST ---
        // The XOR-XST pool is deprecated or unbalanced. Use XST-XSTUSD pool instead.
        if (assetId === XST_ID) {
            // 1. Get XSTUSD Price in DAI
            const xstusdPrice = await getPriceInDai(XSTUS_ID, 18);
            if (xstusdPrice === 0) {
                // Fallback? If XSTUSD is 0 (due to XOR=0), we can't calculate.
                // console.log('XSTUSD Price is 0, defaulting XST to 0');
                return 0;
            }

            // 2. Get XST Price relative to XSTUSD (XST-XSTUSD pool)
            // Pair order: XSTUSD (0x020008...) < XST (0x020009...)
            const reserves = await withTimeout(api.query.poolXYK.reserves(XSTUS_ID, XST_ID));
            const jsonData = reserves.toJSON();

            if (jsonData && jsonData.length >= 2) {
                const baseRes = new BigNumber(jsonData[0]); // XSTUSD
                const targetRes = new BigNumber(jsonData[1]); // XST
                if (!targetRes.isZero()) {
                    const baseNormal = baseRes.div('1e18');
                    const targetNormal = targetRes.div('1e18'); // Both 18 dec
                    const priceInXstUsd = baseNormal.div(targetNormal).toNumber();
                    
                    const finalPrice = priceInXstUsd * xstusdPrice;
                    getPriceInDai.cache[cacheKey] = finalPrice;
                    getPriceInDai.cacheTime = Date.now();
                    
                    return finalPrice;
                }
            }
        }
        // -------------------------

        // 1. Get XOR Price in DAI (Anchor)
        const xorPrice = await getXorPriceInDai();
        if (assetId === XOR_ID) {
            getPriceInDai.cache[cacheKey] = xorPrice;
            getPriceInDai.cacheTime = Date.now();
            return xorPrice;
        }
        if (xorPrice === 0) return 0;

        // 2. Get Token Price relative to XOR
        const tokenPriceInXor = await getTokenPriceInXor(assetId, decimals);
        const finalPrice = tokenPriceInXor * xorPrice;
        
        // SAVE TO CACHE
        getPriceInDai.cache[cacheKey] = finalPrice;
        getPriceInDai.cacheTime = Date.now();
        
        return finalPrice;

    } catch (e) {
        console.error(`CRITICAL ERROR in getPriceInDai for ${assetId}:`, e);
        return 0;
    }
}


async function updateKeyPrices() {
    if (!api) return;
    const POPULAR = ['XOR', 'VAL', 'PSWAP', 'ETH', 'DAI', 'TBCD', 'KUSD', 'DEO', 'KEN', 'KGOLD', 'KXOR', 'VXOR', 'XSTUSD', 'XST', 'KARMA', 'CERES'];
    for (const sym of POPULAR) {
        const asset = ASSETS.find(a => a.symbol === sym);
        if (asset) {
            tokenPrices[sym] = await getPriceInDai(asset.assetId, asset.decimals);
        }
    }
    console.log('💰 Precios actualizados (Populares). XOR=$' + (tokenPrices['XOR'] || 0).toFixed(4));
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
                console.log(`💵 Precio obtenido para ${symbol}: $${price.toFixed(4)}`);
            }
            return price;
        } catch (e) {
            return 0;
        }
    }
    return 0;
}

// --- RUTAS ---
app.get('/tokens', async (req, res) => {
    if (!api) return res.status(503).json({ error: 'Iniciando...' });

    const page = parseInt(req.query.page) || 1;
    const limit = parseInt(req.query.limit) || 20;
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

    // Optimized: Only fetch prices if we need full data
    if (!onlySparklines) {
        await Promise.all(paginated.map(async (asset) => {
            if (!tokenPrices[asset.symbol]) {
                tokenPrices[asset.symbol] = await getPriceInDai(asset.assetId, asset.decimals);
            }
        }));
    }

    const timeframeMap = { '1h': 3600000, '4h': 14400000, '24h': 86400000, '1d': 86400000, '7d': 604800000, '30d': 2592000000, '1m': 2592000000, '1y': 31536000000 };
    const tfMs = timeframeMap[timeframe] || 86400000;

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

app.get('/pools', async (req, res) => {
    if (!api) return res.json({ data: [], total: 0 });
    const page = parseInt(req.query.page) || 1;
    const limit = parseInt(req.query.limit) || 10;
    const now = Date.now();
    const cacheKey = `pools_${page}_${limit}_${req.query.base}`;

    // Check cache (60s)
    if (poolsCache.data && poolsCache.timestamp && (now - poolsCache.timestamp < POOLS_TTL)) {
        const cached = JSON.parse(JSON.stringify(poolsCache.data));
        const startIndex = (page - 1) * limit;
        return res.json({ data: cached.slice(startIndex, startIndex + limit), total: cached.length, page, totalPages: Math.ceil(cached.length / limit) });
    }

    try {
        const entries = await withTimeout(api.query.poolXYK.reserves.entries());
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

        const baseParam = req.query.base;
        if (baseParam && baseParam !== 'all') {
            pools = pools.filter(p => p.base.symbol === baseParam);
        }

        pools.sort((a, b) => {
            const aRes = parseFloat(String(a.reserves.base || '0').replace(/,/g, ''));
            const bRes = parseFloat(String(b.reserves.base || '0').replace(/,/g, ''));
            return bRes - aRes;
        });

        const total = pools.length;
        const totalPages = Math.ceil(total / limit);
        const startIndex = (page - 1) * limit;
        const paginatedPools = pools.slice(startIndex, startIndex + limit);
        
        // Guardar en caché
        poolsCache = { data: pools, timestamp: now };
        
        res.json({ data: paginatedPools, total, page, totalPages });
    } catch (e) {
        console.error(e);
        res.status(500).json({ error: e.message });
    }
});

app.get('/pool/providers', async (req, res) => {
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
        res.status(500).json({ error: e.message });
    }
});

// New Endpoint for Network Trend Chart
app.get('/stats/network/trend', async (req, res) => {
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
        res.status(500).json({ error: e.message });
    }
});

// Stablecoin Monitor Endpoint
app.get('/stats/stablecoins', async (req, res) => {
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
        const timeframeMap = { '1h': 3600000, '4h': 14400000, '24h': 86400000, '7d': 604800000, '30d': 2592000000 };
        const msWindow = timeframeMap[timeframe] || 86400000;

        // 1. Get Volumes
        const volStats = await require('./db').getStablecoinStats(startTime);

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
            const sparkline = await require('./db').getSparkline(sym, msWindow);

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
        res.status(500).json({ error: e.message });
    }
});

// Trending Tokens Endpoint (For Donut Chart)
app.get('/stats/trending-tokens', async (req, res) => {
    try {
        const { timeframe } = req.query;
        let startTime = Date.now() - (24 * 60 * 60 * 1000); // Default 24h

        if (timeframe === '7d') startTime = Date.now() - (7 * 24 * 60 * 60 * 1000);
        if (timeframe === '30d') startTime = Date.now() - (30 * 24 * 60 * 60 * 1000);
        if (timeframe === '1h') startTime = Date.now() - (60 * 60 * 1000);
        if (timeframe === '4h') startTime = Date.now() - (4 * 60 * 60 * 1000);
        if (timeframe === 'all') startTime = 0;

        // Ensure getTopTokens is imported
        const data = await require('./db').getTopTokens(startTime);
        res.json(data);
    } catch (e) {
        console.error('Error stats/trending-tokens:', e);
        res.status(500).json({ error: e.message });
    }
});

app.get('/pool/activity', async (req, res) => {
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

        const limit = parseInt(req.query.limit) || 50;
        const activity = await getPoolActivity(base, target, limit);
        
        // Save to cache
        if (!activityCache.data) activityCache.data = {};
        activityCache.data[cacheKey] = activity || [];
        activityCache.timestamp = now;
        
        res.json(activity || []);
    } catch (e) {
        console.error(e);
        res.status(500).json({ error: e.message });
    }
});

app.get('/holders/:assetId', async (req, res) => {
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
                const allEntries = await withTimeout(api.query.system.account.entries());
                for (const [key, value] of allEntries) {
                    const data = value.toJSON();
                    const free = (data.data && data.data.free) ? data.data.free.toString() : '0';
                    const amountBn = new BigNumber(free).div('1e18');
                    if (amountBn.gt(1)) {
                        fullList.push({ address: key.args[0].toString(), balance: amountBn.toNumber(), balanceStr: amountBn.toFormat(2) });
                    }
                }
            } else {
                const allEntries = await withTimeout(api.query.tokens.accounts.entries());
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
        res.status(500).json({ error: e.message });
    }
});

app.get('/wallet/liquidity/:address', async (req, res) => {
    if (!api) return res.status(500).json({ error: 'API not ready' });
    const address = req.params.address;
    try {
        console.log(`Liquidity Check Start: ${address}`);
        const allProps = await withTimeout(api.query.poolXYK.properties.entries());
        console.log(`Total pools to scan: ${allProps.length}`);

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
                    const reserves = resCodec.toJSON();

                    const baseRes = new BigNumber(String(reserves[0]).replace(/,/g, ''));
                    const targetRes = new BigNumber(String(reserves[1]).replace(/,/g, ''));

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

        console.log(`Liquidity Scan Finished. Found ${poolsData.length} records.`);
        res.json(poolsData.sort((a, b) => b.value - a.value));
    } catch (e) {
        console.error("Error fetching wallet liquidity:", e);
        res.status(500).json({ error: e.message });
    }
});

app.get('/balance/:address', async (req, res) => {
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

                    lastXstDebugLog = {
                        msg: `💰 BALANCE XST`,
                        amount: amount.toFixed(4),
                        price: price,
                        val: amount.times(price).toFixed(2),
                        sym: sym,
                        assetId: assetId
                    };
                    console.log(JSON.stringify(lastXstDebugLog));
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

app.post('/balances', async (req, res) => {
    if (!api) return res.json({ result: [] });
    const { addresses } = req.body;
    if (!addresses || !Array.isArray(addresses)) return res.json({ result: [] });
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

app.get('/history/global/transfers', async (req, res) => {
    const now = Date.now();
    
    // Check cache (60s)
    if (transfersCache.data && now - transfersCache.timestamp < TRANSFERS_TTL) {
        return res.json(transfersCache.data);
    }
    
    try { 
        const data = await getLatestTransfers(req.query.page || 1, 25, req.query.filter, req.query.timestamp);
        transfersCache = { data, timestamp: now };
        res.json(data); 
    } catch (e) { res.json({ data: [], total: 0 }); }
});

app.get('/history/global/swaps', async (req, res) => {
    const now = Date.now();
    const cacheKey = `swaps_${req.query.page}_${req.query.token}_${req.query.filter}`;
    
    // Check cache (24s)
    if (swapsCache.data && now - swapsCache.timestamp < SWAPS_TTL) {
        return res.json(swapsCache.data);
    }
    
    try { 
        const data = await getLatestSwaps(req.query.page || 1, 25, req.query.token || req.query.filter, req.query.timestamp);
        swapsCache = { data, timestamp: now };
        res.json(data); 
    } catch (e) { res.json({ data: [], total: 0 }); }
});

app.get('/history/transfers/:address', async (req, res) => {
    try {
        const page = parseInt(req.query.page) || 1;
        const limit = parseInt(req.query.limit) || 20;
        const data = await getTransfers(req.params.address, page, limit);
        res.json(data);
    } catch (e) {
        console.error(e);
        res.status(500).json({ error: e.message });
    }
});

app.get('/history/bridges/:address', async (req, res) => {
    try {
        const page = parseInt(req.query.page) || 1;
        const limit = parseInt(req.query.limit) || 20;
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
        res.status(500).json({ error: e.message });
    }
});

app.get('/history/global/bridges', async (req, res) => {
    try {
        const { getLatestBridges } = require('./db');
        const page = parseInt(req.query.page) || 1;
        const limit = parseInt(req.query.limit) || 20;

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
        res.status(500).json({ error: e.message });
    }
});

app.get('/history/global/liquidity', async (req, res) => {
    try {
        const { getLiquidityEvents } = require('./db');
        const page = parseInt(req.query.page) || 1;
        const limit = parseInt(req.query.limit) || 20;

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
        res.status(500).json({ error: e.message });
    }
});

app.get('/pool/activity', async (req, res) => {
    try {
        const { getPoolActivity } = require('./db');
        const base = req.query.base;
        const target = req.query.target;
        if (!base || !target) return res.json([]);

        const data = await getPoolActivity(base, target);
        res.json(data);
    } catch (e) {
        console.error(e);
        res.status(500).json({ error: e.message });
    }
});

app.get('/pool/providers', async (req, res) => {
    // Placeholder - we don't have provider tracking yet
    res.json([]);
});

app.get('/history/swaps/:address', async (req, res) => {
    try {
        res.json(await getSwaps(req.params.address, parseInt(req.query.page) || 1));
    } catch (e) {
        console.error('Error getting swaps for ' + req.params.address, e);
        res.status(500).json({ error: e.message });
    }
});
app.get('/chart/:symbol', async (req, res) => res.json(await getCandles(req.params.symbol, req.query.res || 60)));

// --- SORA INTELLIGENCE ENDPOINTS ---

app.get('/stats/accumulation', async (req, res) => {
    const symbol = req.query.symbol || 'XOR';
    const timeframe = req.query.timeframe || '24h'; // 1h, 4h, 24h, 7d, 30d

    const msMap = {
        '1h': 3600000,
        '4h': 14400000,
        '24h': 86400000,
        '1d': 86400000,
        '7d': 604800000,
        '30d': 2592000000,
        '1m': 2592000000,
        '1y': 31536000000
    };
    const ms = msMap[timeframe] || 86400000;

    try {
        const { getTopAccumulators } = require('./db');
        const data = await getTopAccumulators(symbol, ms);
        res.json({ symbol, timeframe, data });
    } catch (e) {
        res.status(500).json({ error: e.message });
    }
});

app.get('/stats/network', async (req, res) => {
    try {
        const { getNetworkStats } = require('./db');
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
        res.status(500).json({ error: e.message });
    }
});

app.get('/stats/overview', async (req, res) => {
    // Un endpoint agregado para el Dashboard
    try {
        const { getNetworkStats, getMarketTrends, getLpVolume } = require('./db');

        // Parse timeframe from query
        const timeframe = req.query.timeframe || '1d';
        const msMap = {
            '1h': 3600000, '4h': 14400000, '1d': 86400000, '24h': 86400000,
            '7d': 604800000, '30d': 2592000000, '1m': 2592000000, '1y': 31536000000, 'all': 0
        };
        const ms = msMap[timeframe] || 86400000;

        // 1. Stablecoin Pegs (Live from tokenPrices)
        const kusdPeg = tokenPrices['KUSD'] || 0;
        const xstusdPeg = tokenPrices['XSTUSD'] || 0;
        const tbcdPeg = tokenPrices['TBCD'] || 0;

        // 2. Network Stats (timeframe-based)
        const netStats = await getNetworkStats(ms);

        // 3. LP Volume (timeframe-based)
        let lpVolume = 0;
        try {
            lpVolume = await getLpVolume(ms);
        } catch (e) { /* silently ignore if function not exists */ }

        // 4. Transfer Volume (timeframe-based)
        let transferVolume = 0;
        try {
            const { getTransferVolume } = require('./db');
            transferVolume = await getTransferVolume(ms);
        } catch (e) { /* silently ignore if function not exists */ }

        // 5. Trends (timeframe-based)
        const trends = await getMarketTrends(ms);

        res.json({
            pegs: {
                KUSD: kusdPeg,
                XSTUSD: xstusdPeg,
                TBCD: tbcdPeg
            },
            network: {
                ...netStats,
                lpVolume: lpVolume,
                transferVolume: transferVolume
            },
            trends: trends
        });
    } catch (e) {
        res.status(500).json({ error: e.message });
    }
});

app.get('/stats/header', async (req, res) => {
    try {
        const timeframe = req.query.timeframe || '1d';
        const msMap = {
            '1h': 3600000, '4h': 14400000, '1d': 86400000, '24h': 86400000,
            '7d': 604800000, '30d': 2592000000, '1m': 2592000000, '1y': 31536000000, 'all': 0
        };

        const ms = msMap[timeframe];
        const startTime = (ms === undefined || ms === 0) ? 0 : (Date.now() - ms);

        const { getFilteredStats } = require('./db');
        const stats = await getFilteredStats(startTime);

        res.json({
            block: sessionStats.block,
            swaps: stats.swaps,
            transfers: stats.transfers,
            bridges: stats.bridges
        });
    } catch (e) {
        res.status(500).json({ error: e.message });
    }
});

app.get('/stats/fees', async (req, res) => {
    try {
        const timeframe = req.query.timeframe || '1d';
        const msMap = {
            '1h': 3600000, '4h': 14400000, '1d': 86400000, '24h': 86400000,
            '7d': 604800000, '30d': 2592000000, '1m': 2592000000, '1y': 31536000000, 'all': 0
        };
        const ms = msMap[timeframe];
        const startTime = (ms === undefined || ms === 0) ? 0 : (Date.now() - ms);

        const stats = await getFeeStats(startTime);
        res.json(stats);
    } catch (e) {
        res.status(500).json({ error: e.message });
    }
});

app.get('/stats/fees/trend', async (req, res) => {
    try {
        const timeframe = req.query.timeframe || '1d';
        const msMap = {
            '1h': 3600000, '4h': 14400000, '1d': 86400000, '24h': 86400000,
            '7d': 604800000, '30d': 2592000000, '1m': 2592000000, '1y': 31536000000, 'all': 0
        };

        const ms = msMap[timeframe];
        const startTime = (ms === undefined || ms === 0) ? 0 : (Date.now() - ms);

        let interval = 'hour';
        if (timeframe === '7d' || timeframe === '1m' || timeframe === '1y' || timeframe === 'all') {
            interval = 'day';
        }

        const stats = await getFeeTrend(startTime, interval);
        res.json(stats);
    } catch (e) {
        res.status(500).json({ error: e.message });
    }
});


// --- DEBUG STATE ---
let lastXstDebugLog = null;

app.get('/debug/xst', (req, res) => {
    const xstAssets = ASSETS.filter(a => a.symbol === 'XST');
    const xstPrice = tokenPrices['XST'];
    const xstUsdPrice = tokenPrices['XSTUSD']; // Add this
    const mXorPrice = tokenPrices['XOR']; // Add this

    res.json({
        xstAssets,
        xstPrice,
        xstUsdPrice,
        mXorPrice,
        allTokenPricesKeys: Object.keys(tokenPrices).filter(k => k.includes('XST')),
        lastLog: lastXstDebugLog
    });
});


app.get('/', (req, res) => res.sendFile(__dirname + '/index.html'));

async function startApp() {
    console.log('🛡️ Iniciando servidor con Alta Estabilidad (Proxy Limitado + Batching 3s)...');
    await initDB();
    await loadOfficialWhitelist();
    api = await initApi();

    setInterval(updateKeyPrices, 60000);
    updateKeyPrices();

    const PORT = process.env.PORT || 3000;
    server.listen(PORT, '0.0.0.0', () => console.log(`🚀 Server on port ${PORT} `));

    // Warm cache al inicio (datos pre-cargados para primer usuario)
    setTimeout(async () => {
        console.log('🔥 Warm cache inicializando...');
        try {
            // Pre-cargar swaps
            swapsCache = { data: await getLatestSwaps(1, 25), timestamp: Date.now() };
            console.log('✅ Cache swaps pre-cargado');
            
            // Pre-cargar transfers
            transfersCache = { data: await getLatestTransfers(1, 25), timestamp: Date.now() };
            console.log('✅ Cache transfers pre-cargado');
            
            // Pre-cargar tokens (usa cache interno)
            // Los tokens ya tienen su propio globalTokenCache
            console.log('✅ Cache tokens listo');
            
            // Pre-cargar pools
            if (api) {
                const entries = await withTimeout(api.query.poolXYK.reserves.entries());
                poolsCache = { data: entries.length, timestamp: Date.now() };
                console.log('✅ Cache pools pre-cargado');
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
        lastBatchTime = now;
    }, BATCH_INTERVAL_MS);

    api.rpc.chain.subscribeNewHeads(async (header) => {
        const blockNumber = header.number.toNumber();
        const blockHash = await api.rpc.chain.getBlockHash(blockNumber);

        // Fetch both Block (for extrinsics) and Events
        const [signedBlock, allEvents] = await Promise.all([
            api.rpc.chain.getBlock(blockHash),
            api.query.system.events.at(blockHash)
        ]);

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
        const { insertLiquidityEvent } = require('./db');

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
                        usdValue
                    });
                } catch (e) { console.error('Fee parsing error', e); }
            }
        }

        // Emit trigger for frontend to re-fetch stats with current filter
        io.emit('new-block-stats', { block: blockNumber });


        // Process transfers with on-demand pricing
        (async () => {
            for (const { event, phase } of limitedTransfers) {
                const data = event.data.toJSON();
                let from, to, amountStr, assetId;
                if (event.section === 'balances') {
                    from = data[0]; to = data[1]; amountStr = data[2];
                    assetId = '0x0200000000000000000000000000000000000000000000000000000000000000';
                } else {
                    assetId = data[0];
                    if (typeof assetId === 'object' && assetId.code) assetId = assetId.code;
                    from = data[1]; to = data[2]; amountStr = data[3];
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


// ========== DEMOCRACY ENDPOINTS ==========

// Get referendum info
app.get("/democracy/referendums", async (req, res) => {
    try {
        if (!api) return res.json({ error: "API not connected" });
        
        const referendumCount = await withTimeout(api.query.democracy.referendumCount());
        const refCount = referendumCount.toNumber();
        
        const referendums = [];
        for (let i = 0; i < refCount; i++) {
            try {
                const info = await withTimeout(api.query.democracy.referendumInfoOf(i));
                if (info && info.toJSON()) {
                    const data = info.toJSON();
                    referendums.push({
                        id: i,
                        status: data ? Object.keys(data)[0] : "unknown",
                        data: data
                    });
                }
            } catch (e) { }
        }
        
        res.json({ count: refCount, referendums });
    } catch (e) {
        res.json({ error: e.message });
    }
});

// Get public proposals
app.get("/democracy/proposals", async (req, res) => {
    try {
        if (!api) return res.json({ error: "API not connected" });
        
        const propCount = await withTimeout(api.query.democracy.publicPropCount());
        const proposals = await withTimeout(api.query.democracy.publicProps());
        
        res.json({ count: propCount.toNumber(), proposals: proposals.toJSON() });
    } catch (e) {
        res.json({ error: e.message });
    }
});

// Get council members
app.get("/council/members", async (req, res) => {
    try {
        if (!api) return res.json({ error: "API not connected" });
        
        const members = await withTimeout(api.query.council.members());
        const prime = await withTimeout(api.query.council.prime());
        
        res.json({
            members: members.toJSON(),
            prime: prime ? prime.toJSON() : null
        });
    } catch (e) {
        res.json({ error: e.message });
    }
});

// Get voting info for address
app.get("/democracy/votes/:address", async (req, res) => {
    try {
        if (!api) return res.json({ error: "API not connected" });
        
        const { address } = req.params;
        const voting = await withTimeout(api.query.democracy.votingOf(address));
        
        res.json({ address, voting: voting ? voting.toJSON() : null });
    } catch (e) {
        res.json({ error: e.message });
    }
});

// Get council proposals
app.get("/council/proposals", async (req, res) => {
    try {
        if (!api) return res.json({ error: "API not connected" });
        
        const proposals = await withTimeout(api.query.council.proposals());
        const proposalCount = await withTimeout(api.query.council.proposalCount());
        
        const proposalList = [];
        for (const hash of proposals) {
            try {
                const prop = await withTimeout(api.query.council.proposalOf(hash));
                proposalList.push({ hash: hash.toHex(), proposal: prop ? prop.toJSON() : null });
            } catch (e) { }
        }
        
        res.json({ count: proposalCount.toNumber(), proposals: proposalList });
    } catch (e) {
        res.json({ error: e.message });
    }
});

startApp();