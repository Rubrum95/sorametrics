-- ============================================================
-- 0012: pool liquidity events (poolXYK deposit / withdraw).
--
-- Node mechanism (live): a successful `poolXYK.depositLiquidity` /
-- `withdrawLiquidity` extrinsic → one row; amounts from the
-- `tokens.Transfer` / `balances.Transfer` events of that extrinsic.
-- Legacy: sm.mv_liquidity_events (subsquid CALL rows, symbols + human
-- amounts) — ETL'd into the same table with origin='legacy'.
--
-- kind is the lowercase live convention ('deposit' | 'withdraw'); the
-- MV's 'Deposit'/'Withdraw' is normalised on ETL (the frontend compares
-- case-insensitively; the Node's lpVolume SUM compared 'Deposit' only,
-- which silently counted live deposits as withdrawals).
-- ============================================================
CREATE TABLE IF NOT EXISTS sm.liquidity_events (
    block_height     BIGINT          NOT NULL,
    extrinsic_id     TEXT            NOT NULL,
    event_id         INTEGER         NOT NULL DEFAULT 0,
    block_timestamp  TIMESTAMPTZ     NOT NULL,
    caller           TEXT            NOT NULL,
    base_asset_id    TEXT            NOT NULL,
    target_asset_id  TEXT            NOT NULL,
    base_amount      NUMERIC(78, 0)  NOT NULL,
    target_amount    NUMERIC(78, 0)  NOT NULL,
    usd_value        NUMERIC(38, 6),
    kind             TEXT            NOT NULL CHECK (kind IN ('deposit', 'withdraw')),
    hash             TEXT,
    origin           TEXT            NOT NULL DEFAULT 'live' CHECK (origin IN ('live', 'legacy')),
    PRIMARY KEY (block_height, extrinsic_id, event_id)
);
CREATE INDEX IF NOT EXISTS liquidity_events_block_event_idx
    ON sm.liquidity_events (block_height DESC, event_id DESC);
CREATE INDEX IF NOT EXISTS liquidity_events_ts_idx
    ON sm.liquidity_events (block_timestamp DESC);
CREATE INDEX IF NOT EXISTS liquidity_events_pool_idx
    ON sm.liquidity_events (base_asset_id, target_asset_id, block_height DESC);
CREATE INDEX IF NOT EXISTS liquidity_events_caller_idx
    ON sm.liquidity_events (caller, block_height DESC);
COMMENT ON TABLE sm.liquidity_events IS
    'poolXYK deposit/withdraw per extrinsic. Amounts are raw planck; usd_value = base×price + target×price at index time.';
