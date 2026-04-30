-- ============================================================
-- SoraMetrics v33 — sm.indexer_state
--
-- Tracks per-job cursor for the substrate-ingest worker. Each
-- logical indexer (live_swaps, live_transfers, live_bridges,
-- backfill_swaps, …) owns one row.
--
-- Idempotent: `CREATE TABLE IF NOT EXISTS`.
-- ============================================================

CREATE TABLE IF NOT EXISTS sm.indexer_state (
    job_name             TEXT        PRIMARY KEY,
    last_processed_block BIGINT      NOT NULL,
    last_finalized_block BIGINT,
    last_updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    status               TEXT        NOT NULL DEFAULT 'idle',
    error_message        TEXT
);

COMMENT ON TABLE sm.indexer_state IS
    'Per-job cursor for substrate-ingest. Idempotent UPSERT on job_name.';

COMMENT ON COLUMN sm.indexer_state.job_name IS
    'Stable identifier, e.g. "live_swaps", "backfill_swaps_2024-01".';
COMMENT ON COLUMN sm.indexer_state.last_processed_block IS
    'Highest block height successfully processed by this job. Used for resume.';
COMMENT ON COLUMN sm.indexer_state.last_finalized_block IS
    'Last finalized block observed (separate from processed). Optional.';
COMMENT ON COLUMN sm.indexer_state.status IS
    'Free-form: idle | running | error | paused. Source-of-truth for health.';
