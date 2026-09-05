-- ============================================================
-- 0011: sm.fees becomes a live table too.
--
-- Until now sm.fees only received legacy mv_fees rows (ETL). The live
-- ingest now decodes TransactionPayment::TransactionFeePaid per
-- extrinsic (the Node's live_fees mechanism) into the same table:
-- legacy_id = "<block>-<extrinsic index>" for live rows, origin = 'live'
-- (legacy rows keep the default 'legacy'), same provenance rule as
-- swaps/transfers/bridges (0008).
-- ============================================================
ALTER TABLE sm.fees ADD COLUMN IF NOT EXISTS origin TEXT NOT NULL DEFAULT 'legacy';
ALTER TABLE sm.fees DROP CONSTRAINT IF EXISTS fees_origin_check;
ALTER TABLE sm.fees ADD CONSTRAINT fees_origin_check CHECK (origin IN ('live', 'legacy'));
CREATE INDEX IF NOT EXISTS fees_type_ts_idx ON sm.fees (fee_type, block_timestamp DESC);
