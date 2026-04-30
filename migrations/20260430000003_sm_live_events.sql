-- ============================================================
-- SoraMetrics v33 — sm.live_swaps / sm.live_transfers / sm.live_bridges
--
-- These tables are written by `sorametrics-ingest --source=substrate`
-- (live subscription + backfill use the same UPSERT path).
--
-- Idempotency key: (block_height, extrinsic_id, event_id). Re-processing
-- a block is a no-op for already-seen (extrinsic, event) tuples.
--
-- amount_raw stores the on-chain integer value as NUMERIC(78,0). Iroha-3
-- uses 78 digits; SORA v2 uses up to 39 (pre/post denomination). 78 is
-- a safe shared upper bound. amount_human is the same value divided by
-- 10^decimals (looked up from sm.asset_registry, populated later).
--
-- usd_value is best-effort. Computed lazily from sm.price_history on
-- first read, cached. NULL until the price-history row exists.
--
-- Indexes are sized for the dominant query patterns:
--   - SELECT … ORDER BY block_height DESC LIMIT N         (recent feeds)
--   - SELECT … WHERE caller = $1 ORDER BY block_height    (per-wallet)
--   - SELECT … WHERE timestamp >= $1 AND timestamp < $2   (range queries)
--
-- Idempotent: safe to re-run (`IF NOT EXISTS` everywhere).
-- ============================================================

-- -----------------------------------------------------------
-- sm.live_swaps  — liquidityProxy.Exchange events
-- -----------------------------------------------------------
CREATE TABLE IF NOT EXISTS sm.live_swaps (
    block_height     BIGINT          NOT NULL,
    extrinsic_id     INTEGER         NOT NULL,
    event_id         INTEGER         NOT NULL,
    block_timestamp  TIMESTAMPTZ     NOT NULL,
    caller           TEXT            NOT NULL,
    input_asset_id   TEXT            NOT NULL,
    input_amount     NUMERIC(78, 0)  NOT NULL,
    output_asset_id  TEXT            NOT NULL,
    output_amount    NUMERIC(78, 0)  NOT NULL,
    usd_value        NUMERIC(38, 6),
    inserted_at      TIMESTAMPTZ     NOT NULL DEFAULT NOW(),

    PRIMARY KEY (block_height, extrinsic_id, event_id),
    CHECK (input_amount  >= 0),
    CHECK (output_amount >= 0),
    CHECK (extrinsic_id  >= 0),
    CHECK (event_id      >= 0)
);

CREATE INDEX IF NOT EXISTS live_swaps_caller_block_idx
    ON sm.live_swaps (caller, block_height DESC);

CREATE INDEX IF NOT EXISTS live_swaps_block_ts_idx
    ON sm.live_swaps (block_timestamp DESC);

COMMENT ON TABLE sm.live_swaps IS
    'liquidityProxy.Exchange events. UPSERT key = (block_height, extrinsic_id, event_id).';

-- -----------------------------------------------------------
-- sm.live_transfers — assets.Transfer events
-- -----------------------------------------------------------
CREATE TABLE IF NOT EXISTS sm.live_transfers (
    block_height     BIGINT          NOT NULL,
    extrinsic_id     INTEGER         NOT NULL,
    event_id         INTEGER         NOT NULL,
    block_timestamp  TIMESTAMPTZ     NOT NULL,
    from_address     TEXT            NOT NULL,
    to_address       TEXT            NOT NULL,
    asset_id         TEXT            NOT NULL,
    amount           NUMERIC(78, 0)  NOT NULL,
    usd_value        NUMERIC(38, 6),
    inserted_at      TIMESTAMPTZ     NOT NULL DEFAULT NOW(),

    PRIMARY KEY (block_height, extrinsic_id, event_id),
    CHECK (amount       >= 0),
    CHECK (extrinsic_id >= 0),
    CHECK (event_id     >= 0)
);

CREATE INDEX IF NOT EXISTS live_transfers_from_block_idx
    ON sm.live_transfers (from_address, block_height DESC);

CREATE INDEX IF NOT EXISTS live_transfers_to_block_idx
    ON sm.live_transfers (to_address, block_height DESC);

CREATE INDEX IF NOT EXISTS live_transfers_block_ts_idx
    ON sm.live_transfers (block_timestamp DESC);

COMMENT ON TABLE sm.live_transfers IS
    'assets.Transfer events. UPSERT key = (block_height, extrinsic_id, event_id).';

-- -----------------------------------------------------------
-- sm.live_bridges  — Hashi v2 bridge events
-- -----------------------------------------------------------
-- CREATE TYPE has no IF NOT EXISTS in PG14; wrap in a DO block.
DO $$ BEGIN
    CREATE TYPE sm.bridge_direction AS ENUM ('in', 'out');
EXCEPTION
    WHEN duplicate_object THEN NULL;
END $$;

CREATE TABLE IF NOT EXISTS sm.live_bridges (
    block_height     BIGINT                 NOT NULL,
    extrinsic_id     INTEGER                NOT NULL,
    event_id         INTEGER                NOT NULL,
    block_timestamp  TIMESTAMPTZ            NOT NULL,
    direction        sm.bridge_direction    NOT NULL,
    network          TEXT                   NOT NULL,
    caller           TEXT                   NOT NULL,
    asset_id         TEXT                   NOT NULL,
    amount           NUMERIC(78, 0)         NOT NULL,
    inserted_at      TIMESTAMPTZ            NOT NULL DEFAULT NOW(),

    PRIMARY KEY (block_height, extrinsic_id, event_id),
    CHECK (amount       >= 0),
    CHECK (extrinsic_id >= 0),
    CHECK (event_id     >= 0)
);

CREATE INDEX IF NOT EXISTS live_bridges_caller_block_idx
    ON sm.live_bridges (caller, block_height DESC);

CREATE INDEX IF NOT EXISTS live_bridges_block_ts_idx
    ON sm.live_bridges (block_timestamp DESC);

CREATE INDEX IF NOT EXISTS live_bridges_network_block_idx
    ON sm.live_bridges (network, block_height DESC);

COMMENT ON TABLE sm.live_bridges IS
    'Hashi v2 bridge events (substrate / parachain / TON). UPSERT key = (block_height, extrinsic_id, event_id).';
