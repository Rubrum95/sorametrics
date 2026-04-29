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
