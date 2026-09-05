-- ============================================================
-- SoraMetrics v33 — 0008: row provenance (origin)
--
-- Reconciliation of the legacy ETL must be EXACT (equal counts + equal
-- checksums). Without provenance, live-ingested rows sharing a block
-- bucket with ETL rows force a lax "target may have more rows" rule —
-- which was shown (2026-08-10, corruption test) to mask real
-- corruption. `origin` removes the ambiguity:
--
--   'live'   — written by the ingest subscriber / backfill decoders
--   'legacy' — written by `ops migrate-legacy`
--
-- The live write path relies on the DEFAULT; only the ETL sets it
-- explicitly. Also useful during Fase 5 parallel-run validation
-- (query exactly which rows came from where).
-- ============================================================

ALTER TABLE sm.swaps         ADD COLUMN IF NOT EXISTS origin TEXT NOT NULL DEFAULT 'live';
ALTER TABLE sm.transfers     ADD COLUMN IF NOT EXISTS origin TEXT NOT NULL DEFAULT 'live';
ALTER TABLE sm.bridges       ADD COLUMN IF NOT EXISTS origin TEXT NOT NULL DEFAULT 'live';
ALTER TABLE ts.price_history ADD COLUMN IF NOT EXISTS origin TEXT NOT NULL DEFAULT 'live';

ALTER TABLE sm.swaps         ADD CONSTRAINT swaps_origin_check         CHECK (origin IN ('live','legacy')) NOT VALID;
ALTER TABLE sm.transfers     ADD CONSTRAINT transfers_origin_check     CHECK (origin IN ('live','legacy')) NOT VALID;
ALTER TABLE sm.bridges       ADD CONSTRAINT bridges_origin_check       CHECK (origin IN ('live','legacy')) NOT VALID;
ALTER TABLE ts.price_history ADD CONSTRAINT price_history_origin_check CHECK (origin IN ('live','legacy')) NOT VALID;

COMMENT ON COLUMN sm.swaps.origin IS
    'Row provenance: live (ingest decoders) or legacy (ops migrate-legacy). Reconciliation compares legacy rows exactly.';
