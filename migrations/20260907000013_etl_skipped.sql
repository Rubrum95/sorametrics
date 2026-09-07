-- 0013: per-table skipped-row counter for ETLs whose reconciliation is
-- a count equation (source = copied + skipped), e.g. liquidity, whose
-- legacy rows are symbol-keyed and may not resolve to an asset id.
CREATE TABLE IF NOT EXISTS sm.etl_skipped (
    table_name  TEXT   PRIMARY KEY,
    skipped     BIGINT NOT NULL DEFAULT 0
);
