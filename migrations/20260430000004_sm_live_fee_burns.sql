-- ============================================================
-- SoraMetrics v33 — sm.live_fee_burns
--
-- Captures `XorFee::FeeWithdrawn` (network fee burns) and
-- `XorFee::ReferrerRewarded` (the referrer share of fees) into a
-- single table, discriminated by `kind`.
--
-- Why one table: both events share the same `(block_height,
-- extrinsic_id, event_id)` PK semantics, the same UPSERT path, and
-- almost all downstream queries want to see them together (fee flow
-- analytics). Keeping them in one table avoids UNION ALL in every
-- read.
--
-- Idempotent: safe to re-run.
-- ============================================================

DO $$ BEGIN
    CREATE TYPE sm.fee_burn_kind AS ENUM ('fee_withdrawn', 'referrer_rewarded');
EXCEPTION
    WHEN duplicate_object THEN NULL;
END $$;

CREATE TABLE IF NOT EXISTS sm.live_fee_burns (
    block_height     BIGINT             NOT NULL,
    extrinsic_id     INTEGER            NOT NULL,
    event_id         INTEGER            NOT NULL,
    block_timestamp  TIMESTAMPTZ        NOT NULL,
    kind             sm.fee_burn_kind   NOT NULL,
    -- For `fee_withdrawn`: the account whose fee was burned (the payer).
    -- For `referrer_rewarded`: the referee (user who triggered the fee).
    payer            TEXT               NOT NULL,
    -- Only set when `kind = referrer_rewarded`; NULL for `fee_withdrawn`.
    referrer         TEXT,
    amount           NUMERIC(78, 0)     NOT NULL,
    inserted_at      TIMESTAMPTZ        NOT NULL DEFAULT NOW(),

    PRIMARY KEY (block_height, extrinsic_id, event_id),
    CHECK (amount       >= 0),
    CHECK (extrinsic_id >= 0),
    CHECK (event_id     >= 0),
    -- Enforce the discriminator–payload invariant.
    CHECK (
        (kind = 'fee_withdrawn'    AND referrer IS NULL) OR
        (kind = 'referrer_rewarded' AND referrer IS NOT NULL)
    )
);

CREATE INDEX IF NOT EXISTS live_fee_burns_block_ts_idx
    ON sm.live_fee_burns (block_timestamp DESC);

CREATE INDEX IF NOT EXISTS live_fee_burns_payer_block_idx
    ON sm.live_fee_burns (payer, block_height DESC);

CREATE INDEX IF NOT EXISTS live_fee_burns_kind_block_idx
    ON sm.live_fee_burns (kind, block_height DESC);

COMMENT ON TABLE sm.live_fee_burns IS
    'XorFee::FeeWithdrawn + ::ReferrerRewarded events. Discriminated by `kind`.';
