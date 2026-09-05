-- ============================================================
-- SoraMetrics v33 — 0007: final table names + legacy-continuity tables
--
-- 1. Rename sm.live_* → final names. After the legacy ETL these tables
--    hold the FULL canonical history (11M swaps, not a 30-day rolling
--    window), so the "live_" prefix inherited from the Node legacy is
--    wrong. Renaming now (dev only, no deployment, smoke data) is one
--    ALTER; renaming after loading 11M rows would ripple everywhere.
--    `live_fee_burns` also becomes `fee_events`: XorFee::FeeWithdrawn
--    is a fee EVENT, not a burn — what actually burns is decided by the
--    burn-split weights + probabilistic remint downstream.
--
-- 2. sm.bridges gains the two columns the legacy carries and the live
--    decoder will populate later: usd_value (computed at index time —
--    the bridge net-flow mechanism) and counterparty (the cross-chain
--    side: sidechain address on Outgoing, absent on some Incoming).
--
-- 3. New legacy-continuity tables (ETL targets, shapes mirror the
--    legacy sources verbatim — mechanism parity):
--    - sm.fees              ← legacy sm.mv_fees (per-call network fee)
--    - sm.fee_burns_aggregate ← legacy sm.fee_burns_live (per-block agg)
--    - ts.price_history     ← legacy sm.price_history (hourly buckets,
--      hour_bucket stays UNIX SECONDS — the documented legacy grain)
--
-- 4. sm.etl_state: keyset cursor per ETL table (resумable batches).
--
-- Idempotent: renames guarded by to_regclass checks; the rest uses
-- IF NOT EXISTS.
-- ============================================================

-- 1. Renames (tables + their indexes, guarded for re-run safety).
DO $$ BEGIN
    IF to_regclass('sm.live_swaps') IS NOT NULL THEN
        ALTER TABLE sm.live_swaps RENAME TO swaps;
        ALTER INDEX sm.live_swaps_caller_block_idx RENAME TO swaps_caller_block_idx;
        ALTER INDEX sm.live_swaps_block_ts_idx RENAME TO swaps_block_ts_idx;
    END IF;
    IF to_regclass('sm.live_transfers') IS NOT NULL THEN
        ALTER TABLE sm.live_transfers RENAME TO transfers;
        ALTER INDEX sm.live_transfers_from_block_idx RENAME TO transfers_from_block_idx;
        ALTER INDEX sm.live_transfers_to_block_idx RENAME TO transfers_to_block_idx;
        ALTER INDEX sm.live_transfers_block_ts_idx RENAME TO transfers_block_ts_idx;
    END IF;
    IF to_regclass('sm.live_bridges') IS NOT NULL THEN
        ALTER TABLE sm.live_bridges RENAME TO bridges;
        ALTER INDEX sm.live_bridges_caller_block_idx RENAME TO bridges_caller_block_idx;
        ALTER INDEX sm.live_bridges_block_ts_idx RENAME TO bridges_block_ts_idx;
        ALTER INDEX sm.live_bridges_network_block_idx RENAME TO bridges_network_block_idx;
    END IF;
    IF to_regclass('sm.live_fee_burns') IS NOT NULL THEN
        ALTER TABLE sm.live_fee_burns RENAME TO fee_events;
    END IF;
END $$;

-- 2. Bridge columns the legacy carries.
ALTER TABLE sm.bridges ADD COLUMN IF NOT EXISTS usd_value NUMERIC(38, 6);
ALTER TABLE sm.bridges ADD COLUMN IF NOT EXISTS counterparty TEXT;

COMMENT ON COLUMN sm.bridges.usd_value IS
    'USD value at index time (bridge net-flow mechanism). NULL until priced.';
COMMENT ON COLUMN sm.bridges.counterparty IS
    'Cross-chain side address (e.g. sidechain recipient on Outgoing). NULL when unknown.';

-- 3a. Per-call network fees (legacy sm.mv_fees continuity).
--     amount is XOR (already /1e18 in the legacy MV), NOT planck.
CREATE TABLE IF NOT EXISTS sm.fees (
    legacy_id        TEXT            PRIMARY KEY,
    block_height     BIGINT          NOT NULL,
    block_timestamp  TIMESTAMPTZ     NOT NULL,
    fee_type         TEXT            NOT NULL,
    amount_xor       NUMERIC(38, 18) NOT NULL,
    usd_value        NUMERIC(38, 6),
    inserted_at      TIMESTAMPTZ     NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS fees_block_idx ON sm.fees (block_height DESC);
CREATE INDEX IF NOT EXISTS fees_ts_idx ON sm.fees (block_timestamp DESC);
COMMENT ON TABLE sm.fees IS
    'Per-call network fees, ETL from legacy sm.mv_fees. amount_xor is human XOR (legacy /1e18).';

-- 3b. Per-block burn aggregates (legacy sm.fee_burns_live, verbatim).
CREATE TABLE IF NOT EXISTS sm.fee_burns_aggregate (
    block_height        BIGINT          PRIMARY KEY,
    ts                  BIGINT          NOT NULL,
    fees_paid_xor       NUMERIC(40, 18) NOT NULL DEFAULT 0,
    ref_paid_xor        NUMERIC(40, 18) NOT NULL DEFAULT 0,
    ref_redirected_xor  NUMERIC(40, 18) NOT NULL DEFAULT 0,
    remint_xor_burned   NUMERIC(40, 18) NOT NULL DEFAULT 0,
    remint_val_burned   NUMERIC(40, 18) NOT NULL DEFAULT 0,
    remint_kusd_burned  NUMERIC(40, 18) NOT NULL DEFAULT 0,
    remint_tbcd_burned  NUMERIC(40, 18) NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS fee_burns_aggregate_ts_idx ON sm.fee_burns_aggregate (ts);
COMMENT ON TABLE sm.fee_burns_aggregate IS
    'Per-block fee/burn aggregates, ETL from legacy sm.fee_burns_live (schema verbatim).';

-- 3c. Hourly price buckets (legacy sm.price_history) as a Timescale
--     hypertable partitioned on the integer hour_bucket (unix SECONDS —
--     legacy convention, documented pitfall: never treat as hours).
CREATE TABLE IF NOT EXISTS ts.price_history (
    asset_id     TEXT             NOT NULL,
    hour_bucket  BIGINT           NOT NULL,
    price_usd    DOUBLE PRECISION NOT NULL,
    sample_count INTEGER          NOT NULL DEFAULT 0,
    PRIMARY KEY (asset_id, hour_bucket)
);
COMMENT ON TABLE ts.price_history IS
    'Hourly price buckets (median USD via DAI). hour_bucket = unix seconds. ETL from legacy sm.price_history.';

-- Hypertable: 30-day chunks (2592000 s). migrate_data covers re-runs
-- where plain rows already landed before conversion.
SELECT create_hypertable(
    'ts.price_history', 'hour_bucket',
    chunk_time_interval => 2592000,
    if_not_exists => TRUE,
    migrate_data => TRUE
);

-- 4. ETL keyset cursors.
CREATE TABLE IF NOT EXISTS sm.etl_state (
    table_name   TEXT        PRIMARY KEY,
    last_cursor  TEXT,
    rows_copied  BIGINT      NOT NULL DEFAULT 0,
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
COMMENT ON TABLE sm.etl_state IS
    'Resumable keyset cursor per migrate-legacy table. last_cursor format is table-specific.';
