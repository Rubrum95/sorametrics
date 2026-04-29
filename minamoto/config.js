'use strict';
// ============================================================
// minamoto/config.js — runtime configuration for Minamoto module
// All values overridable via env vars. No secrets here.
// ============================================================

const TORII_BASE = (process.env.MINAMOTO_TORII || 'https://minamoto.sora.org').replace(/\/+$/, '');

const HTTP_TIMEOUT_MS    = parseInt(process.env.MINAMOTO_HTTP_TIMEOUT_MS, 10)    || 10000;
const HTTP_RETRY_MAX     = parseInt(process.env.MINAMOTO_HTTP_RETRY_MAX, 10)     || 2;
const HTTP_RETRY_DELAY_MS = parseInt(process.env.MINAMOTO_HTTP_RETRY_DELAY_MS, 10) || 500;
const HTTP_CACHE_TTL_MS  = parseInt(process.env.MINAMOTO_HTTP_CACHE_TTL_MS, 10)  || 5000;

// Indexer poll intervals (ms)
const POLL_NETWORK_STATE_MS = parseInt(process.env.MINAMOTO_POLL_NETWORK_MS, 10) || 10_000;
const POLL_BLOCKS_MS        = parseInt(process.env.MINAMOTO_POLL_BLOCKS_MS, 10)  || 30_000;
const POLL_TX_MS            = parseInt(process.env.MINAMOTO_POLL_TX_MS, 10)      || 30_000;
const POLL_DOMAINS_MS       = parseInt(process.env.MINAMOTO_POLL_DOMAINS_MS, 10) || 300_000;
const POLL_ACCOUNTS_MS      = parseInt(process.env.MINAMOTO_POLL_ACCOUNTS_MS, 10) || 300_000;
const POLL_ASSETS_MS        = parseInt(process.env.MINAMOTO_POLL_ASSETS_MS, 10)  || 300_000;
const POLL_PEERS_MS         = parseInt(process.env.MINAMOTO_POLL_PEERS_MS, 10)   || 60_000;
const POLL_PROMETHEUS_MS    = parseInt(process.env.MINAMOTO_POLL_PROM_MS, 10)    || 60_000;

// Retention
const METRICS_RETENTION_DAYS  = parseInt(process.env.MINAMOTO_METRICS_RETENTION_DAYS, 10)  || 30;
const METRICS_CLEANUP_PERIOD_MS = parseInt(process.env.MINAMOTO_METRICS_CLEANUP_MS, 10)   || 3_600_000; // 1h

// Pagination caps for /api/minamoto endpoints (defensive)
const MAX_PAGE_SIZE = 100;

module.exports = {
    TORII_BASE,
    HTTP_TIMEOUT_MS,
    HTTP_RETRY_MAX,
    HTTP_RETRY_DELAY_MS,
    HTTP_CACHE_TTL_MS,
    POLL_NETWORK_STATE_MS,
    POLL_BLOCKS_MS,
    POLL_TX_MS,
    POLL_DOMAINS_MS,
    POLL_ACCOUNTS_MS,
    POLL_ASSETS_MS,
    POLL_PEERS_MS,
    POLL_PROMETHEUS_MS,
    METRICS_RETENTION_DAYS,
    METRICS_CLEANUP_PERIOD_MS,
    MAX_PAGE_SIZE,
};
