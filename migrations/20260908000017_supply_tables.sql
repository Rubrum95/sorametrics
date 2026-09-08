-- ============================================================
-- 0017: burn tracker sources.
--
-- sm.supply_snapshots — the Node's 30-minute MOF circulating-supply
-- snapshots (`fetchMofSupply` → `insertSupplySnapshot`), one row per
-- (symbol, ts). Legacy rows come from the Node's table (its `timestamp`
-- in ms), live rows from the ingest supply sampler.
--
-- sm.supply_history — daily on-chain issuance points used by
-- `/burns/supply-history` before the MOF era: the subsquid
-- `asset_snapshot` (type DAY, `supply / 1e18`) and the Node's
-- `sm.supply_history` backfill (`total_issuance`), tagged by source.
-- ============================================================
CREATE TABLE IF NOT EXISTS sm.supply_snapshots (
    symbol        TEXT              NOT NULL,
    ts            TIMESTAMPTZ       NOT NULL,
    asset_id      TEXT,
    total_supply  DOUBLE PRECISION  NOT NULL,
    origin        TEXT              NOT NULL DEFAULT 'live' CHECK (origin IN ('live', 'legacy')),
    PRIMARY KEY (symbol, ts)
);
CREATE INDEX IF NOT EXISTS supply_snapshots_symbol_ts_idx ON sm.supply_snapshots (symbol, ts DESC);

CREATE TABLE IF NOT EXISTS sm.supply_history (
    symbol        TEXT              NOT NULL,
    ts_secs       BIGINT            NOT NULL,
    total_supply  DOUBLE PRECISION  NOT NULL,
    source        TEXT              NOT NULL CHECK (source IN ('asset_snapshot', 'supply_history')),
    PRIMARY KEY (symbol, ts_secs, source)
);
