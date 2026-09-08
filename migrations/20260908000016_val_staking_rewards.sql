-- ============================================================
-- 0016: VAL staking payouts (the Node's sm.val_staking_rewards).
--
-- One row per `xorFee.ValStakingRewardPaid(stash, dest, era, page,
-- amount)` event. Same natural key as the Node's live table so the
-- ETL copies it verbatim; `block_timestamp` is the block time (the
-- Node stamped its `ts` at insert).
-- ============================================================
CREATE TABLE IF NOT EXISTS sm.val_staking_rewards (
    era              INTEGER         NOT NULL,
    page             INTEGER         NOT NULL,
    validator_stash  TEXT            NOT NULL,
    destination      TEXT            NOT NULL,
    amount           NUMERIC(78, 0)  NOT NULL,
    block_height     BIGINT          NOT NULL,
    block_hash       TEXT,
    block_timestamp  TIMESTAMPTZ     NOT NULL,
    origin           TEXT            NOT NULL DEFAULT 'live' CHECK (origin IN ('live', 'legacy')),
    PRIMARY KEY (era, page, validator_stash, destination, block_height)
);
CREATE INDEX IF NOT EXISTS val_staking_rewards_validator_era_idx
    ON sm.val_staking_rewards (validator_stash, era);
CREATE INDEX IF NOT EXISTS val_staking_rewards_destination_ts_idx
    ON sm.val_staking_rewards (destination, block_timestamp DESC);
CREATE INDEX IF NOT EXISTS val_staking_rewards_ts_idx
    ON sm.val_staking_rewards (block_timestamp DESC);
COMMENT ON TABLE sm.val_staking_rewards IS
    'xorFee.ValStakingRewardPaid per (era, page, stash, destination, block). amount is raw VAL planck.';
