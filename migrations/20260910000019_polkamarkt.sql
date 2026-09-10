-- ============================================================
-- 0019: Polkamarkt (the Node's sm.polkamarkt_* tables, `initPolkamarktSchema`).
--
-- Live mechanism: every `polkamarkt` event → MarketCreated inserts the
-- market hydrated from `Markets` / `Conditions` storage at the block;
-- TradeExecuted / MarketClaimed / CreatorFeesClaimed / DpmResidualBurned /
-- LegacyMigrationResidualRouted / XorBuybackSwept append rows; the
-- lifecycle events update status / resolution. A periodic reconcile
-- reads `Markets` (+ `MarketResolution`) and fixes drift (the weekly
-- governance batch resolves markets without firing our events).
--
-- Column names and the ms `ts` are the Node's; the append tables add
-- `(block_height, event_id)` as the idempotency key of live rows and
-- `legacy_id` for the rows the ETL copies from the Node's serial ids.
-- ============================================================
CREATE TABLE IF NOT EXISTS sm.polkamarkt_markets (
    market_id           BIGINT       PRIMARY KEY,
    condition_id        BIGINT       NOT NULL,
    creator             TEXT         NOT NULL,
    close_block         INTEGER      NOT NULL,
    collateral_asset    TEXT         NOT NULL,
    seed_liquidity      NUMERIC(60)  NOT NULL DEFAULT 0,
    status              TEXT         NOT NULL,
    resolution          TEXT,
    question            TEXT,
    oracle              TEXT,
    resolution_source   TEXT,
    opengov_network     TEXT,
    opengov_parachain   INTEGER,
    opengov_track       INTEGER,
    opengov_referendum  INTEGER,
    created_at_block    INTEGER      NOT NULL,
    created_at_ts       BIGINT       NOT NULL,
    resolved_at_block   INTEGER,
    resolved_at_ts      BIGINT,
    mechanism           TEXT,
    origin              TEXT         NOT NULL DEFAULT 'live' CHECK (origin IN ('live', 'legacy'))
);
CREATE INDEX IF NOT EXISTS idx_pm_markets_status  ON sm.polkamarkt_markets (status);
CREATE INDEX IF NOT EXISTS idx_pm_markets_creator ON sm.polkamarkt_markets (creator);

CREATE TABLE IF NOT EXISTS sm.polkamarkt_trades (
    id          BIGSERIAL    PRIMARY KEY,
    market_id   BIGINT       NOT NULL,
    trader      TEXT         NOT NULL,
    side        TEXT         NOT NULL,
    outcome     TEXT         NOT NULL,
    collateral  NUMERIC(60)  NOT NULL,
    shares      NUMERIC(60)  NOT NULL,
    fee         NUMERIC(60)  NOT NULL,
    block       INTEGER      NOT NULL,
    ts          BIGINT       NOT NULL,
    hash        TEXT,
    event_id    INTEGER,
    legacy_id   BIGINT       UNIQUE,
    origin      TEXT         NOT NULL DEFAULT 'live' CHECK (origin IN ('live', 'legacy'))
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_pm_trades_event ON sm.polkamarkt_trades (block, event_id) WHERE event_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_pm_trades_market_ts ON sm.polkamarkt_trades (market_id, ts);
CREATE INDEX IF NOT EXISTS idx_pm_trades_trader    ON sm.polkamarkt_trades (trader);

CREATE TABLE IF NOT EXISTS sm.polkamarkt_claims (
    id         BIGSERIAL    PRIMARY KEY,
    market_id  BIGINT       NOT NULL,
    account    TEXT         NOT NULL,
    kind       TEXT         NOT NULL,
    amount     NUMERIC(60)  NOT NULL,
    block      INTEGER      NOT NULL,
    ts         BIGINT       NOT NULL,
    event_id   INTEGER,
    legacy_id  BIGINT       UNIQUE,
    origin     TEXT         NOT NULL DEFAULT 'live' CHECK (origin IN ('live', 'legacy'))
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_pm_claims_event ON sm.polkamarkt_claims (block, event_id) WHERE event_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_pm_claims_account ON sm.polkamarkt_claims (account);

CREATE TABLE IF NOT EXISTS sm.polkamarkt_buybacks (
    id          BIGSERIAL    PRIMARY KEY,
    block       INTEGER      NOT NULL,
    ts          BIGINT       NOT NULL,
    hash        TEXT,
    kusd_spent  NUMERIC(60)  NOT NULL,
    xor_burned  NUMERIC(60)  NOT NULL,
    event_id    INTEGER,
    legacy_id   BIGINT       UNIQUE,
    origin      TEXT         NOT NULL DEFAULT 'live' CHECK (origin IN ('live', 'legacy'))
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_pm_buybacks_event ON sm.polkamarkt_buybacks (block, event_id) WHERE event_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_pm_buybacks_ts ON sm.polkamarkt_buybacks (ts DESC);

CREATE TABLE IF NOT EXISTS sm.polkamarkt_burns (
    id         BIGSERIAL    PRIMARY KEY,
    block      INTEGER      NOT NULL,
    ts         BIGINT       NOT NULL,
    hash       TEXT,
    market_id  BIGINT,
    kind       TEXT         NOT NULL,
    amount     NUMERIC(60)  NOT NULL,
    event_id   INTEGER,
    legacy_id  BIGINT       UNIQUE,
    origin     TEXT         NOT NULL DEFAULT 'live' CHECK (origin IN ('live', 'legacy'))
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_pm_burns_event ON sm.polkamarkt_burns (block, event_id) WHERE event_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_pm_burns_ts ON sm.polkamarkt_burns (ts DESC);
