-- ============================================================
-- 0014: extrinsics (the Node's live_extrinsics / mv_extrinsics).
--
-- One row per extrinsic in the block (signed or not; listings exclude
-- timestamp.set like the Node). section/method use the polkadot-js
-- camelCase names the frontend expects ("liquidityProxy" / "swap").
-- args / events are the decoded call args and the phase's events in a
-- toHuman-like JSON (numbers as grouped strings, accounts as SS58,
-- byte arrays as 0x hex); events = [{s, m, d}] without
-- ExtrinsicSuccess/Failed. Legacy MV rows (ETL) carry a SYNTHETIC
-- extrinsic_index (ROW_NUMBER per block) and no args/events — that is
-- the legacy contract for that era.
-- ============================================================
CREATE TABLE IF NOT EXISTS sm.extrinsics (
    block_height     BIGINT       NOT NULL,
    extrinsic_index  INTEGER      NOT NULL,
    block_timestamp  TIMESTAMPTZ  NOT NULL,
    hash             TEXT         NOT NULL,
    section          TEXT         NOT NULL,
    method           TEXT         NOT NULL,
    signer           TEXT         NOT NULL,
    success          BOOLEAN      NOT NULL,
    error_msg        TEXT         NOT NULL DEFAULT '',
    args             JSONB,
    events           JSONB,
    origin           TEXT         NOT NULL DEFAULT 'live' CHECK (origin IN ('live', 'legacy')),
    PRIMARY KEY (block_height, extrinsic_index)
);
CREATE INDEX IF NOT EXISTS extrinsics_ts_idx      ON sm.extrinsics (block_timestamp DESC);
CREATE INDEX IF NOT EXISTS extrinsics_signer_idx  ON sm.extrinsics (signer, block_height DESC);
CREATE INDEX IF NOT EXISTS extrinsics_section_idx ON sm.extrinsics (section, block_height DESC);
CREATE INDEX IF NOT EXISTS extrinsics_hash_idx    ON sm.extrinsics (hash);
CREATE INDEX IF NOT EXISTS extrinsics_block_idx   ON sm.extrinsics (block_height DESC, extrinsic_index DESC);
COMMENT ON TABLE sm.extrinsics IS
    'Every extrinsic per block with toHuman-style args/events JSON. Legacy rows: synthetic index, no args/events.';
