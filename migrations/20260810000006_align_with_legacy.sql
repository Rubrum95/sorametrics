-- ============================================================
-- SoraMetrics v33 — 0006: align sm.live_* with the legacy schema
--
-- Two changes per event table (swaps / transfers / bridges / fee_burns):
--
-- 1. extrinsic_id INTEGER → TEXT.
--    Live ingest keeps writing the in-block extrinsic index (as text);
--    the legacy ETL (Bloque 2) writes the legacy identifier, which is
--    already text ("<block>-<index>" in the API surface). TEXT is the
--    common denominator; the composite PK (block_height, extrinsic_id,
--    event_id) is unaffected.
--
-- 2. ADD COLUMN hash TEXT (nullable).
--    The extrinsic hash ("0x…", 32 bytes hex). Live ingest populates it
--    from the block body for ApplyExtrinsic events; NULL for events
--    emitted outside an extrinsic (Initialization/Finalization) and for
--    legacy rows that never recorded it.
--
-- The `extrinsic_id >= 0` CHECKs are integer-only; drop them before the
-- type change. Idempotent: IF EXISTS / IF NOT EXISTS everywhere, and
-- ALTER TYPE from TEXT to TEXT is a no-op on re-run.
-- ============================================================

ALTER TABLE sm.live_swaps     DROP CONSTRAINT IF EXISTS live_swaps_extrinsic_id_check;
ALTER TABLE sm.live_transfers DROP CONSTRAINT IF EXISTS live_transfers_extrinsic_id_check;
ALTER TABLE sm.live_bridges   DROP CONSTRAINT IF EXISTS live_bridges_extrinsic_id_check;
ALTER TABLE sm.live_fee_burns DROP CONSTRAINT IF EXISTS live_fee_burns_extrinsic_id_check;

ALTER TABLE sm.live_swaps     ALTER COLUMN extrinsic_id TYPE TEXT USING extrinsic_id::text;
ALTER TABLE sm.live_transfers ALTER COLUMN extrinsic_id TYPE TEXT USING extrinsic_id::text;
ALTER TABLE sm.live_bridges   ALTER COLUMN extrinsic_id TYPE TEXT USING extrinsic_id::text;
ALTER TABLE sm.live_fee_burns ALTER COLUMN extrinsic_id TYPE TEXT USING extrinsic_id::text;

ALTER TABLE sm.live_swaps     ADD COLUMN IF NOT EXISTS hash TEXT;
ALTER TABLE sm.live_transfers ADD COLUMN IF NOT EXISTS hash TEXT;
ALTER TABLE sm.live_bridges   ADD COLUMN IF NOT EXISTS hash TEXT;
ALTER TABLE sm.live_fee_burns ADD COLUMN IF NOT EXISTS hash TEXT;

COMMENT ON COLUMN sm.live_swaps.hash     IS 'Extrinsic hash (0x-hex). NULL for non-extrinsic events and legacy rows without it.';
COMMENT ON COLUMN sm.live_transfers.hash IS 'Extrinsic hash (0x-hex). NULL for non-extrinsic events and legacy rows without it.';
COMMENT ON COLUMN sm.live_bridges.hash   IS 'Extrinsic hash (0x-hex). NULL for non-extrinsic events and legacy rows without it.';
COMMENT ON COLUMN sm.live_fee_burns.hash IS 'Extrinsic hash (0x-hex). NULL for non-extrinsic events and legacy rows without it.';
