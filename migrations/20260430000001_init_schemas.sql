-- ============================================================
-- SoraMetrics v33 — initial schema layout
--
-- Creates the four canonical schemas:
--   sm        — SORA v2 (Substrate) indexed state
--   mn        — Minamoto / Iroha 3 indexed state
--   ts        — TimescaleDB hypertables (price, metrics, OHLCV)
--   analytics — Read-only views joining sm / mn for cross-chain analytics
--
-- Idempotent: safe to re-run.
-- ============================================================

CREATE SCHEMA IF NOT EXISTS sm;
COMMENT ON SCHEMA sm IS
  'SORA v2 (Substrate) indexed state. Isolated from mn.';

CREATE SCHEMA IF NOT EXISTS mn;
COMMENT ON SCHEMA mn IS
  'Minamoto / Iroha 3 indexed state. Isolated from sm.';

CREATE SCHEMA IF NOT EXISTS ts;
COMMENT ON SCHEMA ts IS
  'TimescaleDB hypertables: price_history, metrics_snapshots, candlesticks.';

CREATE SCHEMA IF NOT EXISTS analytics;
COMMENT ON SCHEMA analytics IS
  'Read-only views joining sm and mn. NEVER written directly by ingest workers.';

-- TimescaleDB extension is required for ts.* hypertables.
-- The extension lives in the public schema and is global to the database.
CREATE EXTENSION IF NOT EXISTS timescaledb;
