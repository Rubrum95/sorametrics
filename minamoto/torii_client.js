'use strict';
// ============================================================
// minamoto/torii_client.js — HTTP client for Minamoto Torii
// Features: timeout, exponential-backoff retry, in-memory cache,
// strict error reporting. Uses Node's built-in fetch (Node >= 18).
// ============================================================

const cfg = require('./config');

const _cache = new Map();

function _now() { return Date.now(); }

function _sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

function _cacheKey(url) { return url; }

function _readCache(url) {
    const e = _cache.get(_cacheKey(url));
    if (!e) return null;
    if (_now() - e.ts > cfg.HTTP_CACHE_TTL_MS) {
        _cache.delete(_cacheKey(url));
        return null;
    }
    return e.value;
}

function _writeCache(url, value) {
    _cache.set(_cacheKey(url), { ts: _now(), value });
    // Soft cap to avoid unbounded growth
    if (_cache.size > 256) {
        const oldest = _cache.keys().next().value;
        _cache.delete(oldest);
    }
}

async function _fetchOnce(url, accept) {
    const ctrl = new AbortController();
    const timer = setTimeout(() => ctrl.abort(), cfg.HTTP_TIMEOUT_MS);
    try {
        const res = await fetch(url, {
            method: 'GET',
            headers: accept ? { Accept: accept } : {},
            signal: ctrl.signal,
        });
        const text = await res.text();
        return { ok: res.ok, status: res.status, text, contentType: res.headers.get('content-type') || '' };
    } finally {
        clearTimeout(timer);
    }
}

async function _fetchWithRetry(url, accept) {
    let lastErr = null;
    for (let attempt = 0; attempt <= cfg.HTTP_RETRY_MAX; attempt++) {
        try {
            const r = await _fetchOnce(url, accept);
            if (r.ok) return r;
            // 5xx: retry. 4xx: bubble up immediately, no point retrying.
            if (r.status >= 500 && attempt < cfg.HTTP_RETRY_MAX) {
                lastErr = new Error(`Torii ${r.status} on ${url}`);
                await _sleep(cfg.HTTP_RETRY_DELAY_MS * Math.pow(2, attempt));
                continue;
            }
            const err = new Error(`Torii ${r.status} on ${url}: ${r.text.slice(0, 200)}`);
            err.status = r.status;
            err.body = r.text;
            throw err;
        } catch (e) {
            lastErr = e;
            if (e.name === 'AbortError' && attempt < cfg.HTTP_RETRY_MAX) {
                await _sleep(cfg.HTTP_RETRY_DELAY_MS * Math.pow(2, attempt));
                continue;
            }
            if (attempt < cfg.HTTP_RETRY_MAX) {
                await _sleep(cfg.HTTP_RETRY_DELAY_MS * Math.pow(2, attempt));
                continue;
            }
            throw e;
        }
    }
    throw lastErr || new Error(`Torii request failed: ${url}`);
}

async function getJson(path, { useCache = true } = {}) {
    const url = cfg.TORII_BASE + path;
    if (useCache) {
        const cached = _readCache(url);
        if (cached) return cached;
    }
    const r = await _fetchWithRetry(url, 'application/json');
    let parsed;
    try {
        parsed = JSON.parse(r.text);
    } catch (e) {
        const err = new Error(`Invalid JSON from ${url}: ${e.message}`);
        err.body = r.text.slice(0, 500);
        throw err;
    }
    if (useCache) _writeCache(url, parsed);
    return parsed;
}

async function getText(path, { useCache = true } = {}) {
    const url = cfg.TORII_BASE + path;
    if (useCache) {
        const cached = _readCache(url);
        if (cached) return cached;
    }
    const r = await _fetchWithRetry(url, 'text/plain');
    if (useCache) _writeCache(url, r.text);
    return r.text;
}

// ------------------------------------------------------------
// Convenience wrappers for the endpoints we actually use
// ------------------------------------------------------------

// ------------------------------------------------------------
// Torii API generation. The `optimizations` branch of hyperledger-iroha
// (the only live one since 2026-05) replaced the explorer's page/per_page
// pagination with cursor/limit, dropped /peers, /v1/sumeragi/telemetry,
// /v1/sumeragi/collectors and /v1/gov/council/current, and requires an
// account signature on /v1/explorer/metrics. Its /status carries
// `build.git_commit_sha`; the build Minamoto ran until 2026-06 did not.
// The generation is read from /status on every network_state poll so a
// node upgrade is picked up without a restart.
// ------------------------------------------------------------

let _generation = null;

async function apiGeneration({ refresh = false } = {}) {
    if (_generation && !refresh) return _generation;
    const s = await getJson('/status', { useCache: false });
    const build = (s && s.build) || {};
    _generation = {
        kind: build.git_commit_sha ? 'cursor' : 'page',
        version: build.version || null,
        git_commit_sha: build.git_commit_sha || null,
    };
    return _generation;
}

function cachedGeneration() { return _generation; }

function _cursorQs(cursor, limit, extra = {}) {
    const p = new URLSearchParams();
    p.set('limit', String(Math.max(1, Math.min(100, limit | 0))));
    if (cursor) p.set('cursor', cursor);
    for (const [k, v] of Object.entries(extra)) if (v != null && v !== '') p.set(k, String(v));
    return p.toString();
}

// Cursor-generation explorer reads. Pages carry `pagination.next_cursor`
// / `pagination.has_more`; chain feeds also `snapshot_height`.
async function getExplorerBlocksCursor(cursor, limit = 100)       { return getJson(`/v1/explorer/blocks?${_cursorQs(cursor, limit)}`, { useCache: false }); }
async function getExplorerTransactionsCursor(cursor, limit = 100) { return getJson(`/v1/explorer/transactions?${_cursorQs(cursor, limit)}`, { useCache: false }); }
async function getExplorerInstructionsCursor(cursor, limit = 100) { return getJson(`/v1/explorer/instructions?${_cursorQs(cursor, limit)}`, { useCache: false }); }
async function getExplorerAccountsCursor(cursor, limit = 100)     { return getJson(`/v1/explorer/accounts?${_cursorQs(cursor, limit)}`, { useCache: false }); }
async function getExplorerDomainsCursor(cursor, limit = 100)      { return getJson(`/v1/explorer/domains?${_cursorQs(cursor, limit)}`, { useCache: false }); }
async function getExplorerAssetsCursor(cursor, limit = 100)       { return getJson(`/v1/explorer/assets?${_cursorQs(cursor, limit)}`, { useCache: false }); }
async function getExplorerNftsCursor(cursor, limit = 50)          { return getJson(`/v1/explorer/nfts?${_cursorQs(cursor, limit)}`); }
async function getExplorerRwasCursor(cursor, limit = 50)          { return getJson(`/v1/explorer/rwas?${_cursorQs(cursor, limit)}`); }
// Application list: `{ items, total, has_more, count_mode }`.
async function getAssetDefinitionsPage(limit = 100, offset = 0) {
    return getJson(`/v1/assets/definitions?limit=${limit | 0}&offset=${offset | 0}&count_mode=exact`, { useCache: false });
}

async function getHealth()             { return getText('/health'); }
async function getStatus()              { return getJson('/status'); }
async function getPeers()               { return getJson('/peers'); }
async function getExplorerMetrics()     { return getJson('/v1/explorer/metrics'); }
async function getExplorerBlocks(page = 1, perPage = 10)        { return getJson(`/v1/explorer/blocks?page=${page}&per_page=${perPage}`); }
async function getExplorerTransactions(page = 1, perPage = 20)  { return getJson(`/v1/explorer/transactions?page=${page}&per_page=${perPage}`); }
async function getExplorerAccounts(page = 1, perPage = 50)      { return getJson(`/v1/explorer/accounts?page=${page}&per_page=${perPage}`); }
async function getExplorerDomains(page = 1, perPage = 50)       { return getJson(`/v1/explorer/domains?page=${page}&per_page=${perPage}`); }
async function getExplorerAssets(page = 1, perPage = 50)        { return getJson(`/v1/explorer/assets?page=${page}&per_page=${perPage}`); }
async function getAssetDefinitions()    { return getJson('/v1/assets/definitions'); }
async function getAssetDefinition(id)   { return getJson(`/v1/assets/definitions/${encodeURIComponent(id)}`); }
async function getBlockByHeight(h)      { return getJson(`/v1/explorer/blocks/${h}`); }
async function getBlockByHash(h)        { return getJson(`/v1/explorer/blocks/${h}`); }
async function getTransactionByHash(h)  { return getJson(`/v1/explorer/transactions/${h}`); }
async function getExplorerInstructions(page = 1, perPage = 50) {
    return getJson(`/v1/explorer/instructions?page=${page}&per_page=${perPage}`);
}
async function getPeersInfo()      { return getJson('/v1/telemetry/peers-info'); }
async function getPropagation()    { return getJson('/v1/telemetry/propagation'); }
async function getSumeragiTel()    { return getJson('/v1/sumeragi/telemetry'); }
async function getGovCouncil()     { return getJson('/v1/gov/council/current'); }
async function getGovUnlocks()     { return getJson('/v1/gov/unlocks/stats'); }
async function getKaigiRelays()    { return getJson('/v1/kaigi/relays'); }
async function getExplorerNfts(page = 1, perPage = 50)  { return getJson(`/v1/explorer/nfts?page=${page}&per_page=${perPage}`); }
async function getExplorerRwas(page = 1, perPage = 50)  { return getJson(`/v1/explorer/rwas?page=${page}&per_page=${perPage}`); }
async function getAccountAssets(accountId)      { return getJson(`/v1/accounts/${encodeURIComponent(accountId)}/assets`); }
async function getAccountTransactions(accountId) { return getJson(`/v1/accounts/${encodeURIComponent(accountId)}/transactions`); }
async function getAccountPermissions(accountId)  { return getJson(`/v1/accounts/${encodeURIComponent(accountId)}/permissions`); }
async function getPrometheus()          { return getText('/metrics'); }
async function getSumeragiStatus()      { return getJson('/v1/sumeragi/status'); }
async function getSumeragiCollectors()  { return getJson('/v1/sumeragi/collectors'); }

module.exports = {
    getJson,
    getText,
    apiGeneration,
    cachedGeneration,
    getExplorerBlocksCursor,
    getExplorerTransactionsCursor,
    getExplorerInstructionsCursor,
    getExplorerAccountsCursor,
    getExplorerDomainsCursor,
    getExplorerAssetsCursor,
    getExplorerNftsCursor,
    getExplorerRwasCursor,
    getAssetDefinitionsPage,
    getHealth,
    getStatus,
    getPeers,
    getExplorerMetrics,
    getExplorerBlocks,
    getExplorerTransactions,
    getExplorerAccounts,
    getExplorerDomains,
    getExplorerAssets,
    getAssetDefinitions,
    getAssetDefinition,
    getBlockByHeight,
    getBlockByHash,
    getTransactionByHash,
    getExplorerInstructions,
    getPeersInfo,
    getPropagation,
    getSumeragiTel,
    getGovCouncil,
    getGovUnlocks,
    getKaigiRelays,
    getExplorerNfts,
    getExplorerRwas,
    getAccountAssets,
    getAccountTransactions,
    getAccountPermissions,
    getPrometheus,
    getSumeragiStatus,
    getSumeragiCollectors,
};
