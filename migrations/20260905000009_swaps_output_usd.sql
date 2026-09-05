-- ============================================================
-- 0009: output-leg USD value on swaps.
--
-- The legacy contract exposes both legs (`in.usd` from mv_swaps.in_usd,
-- `out.usd` from mv_swaps.out_usd) and the Node live indexer values
-- both at insert time. `usd_value` keeps meaning the INPUT leg (as
-- documented since 0003 / the ETL mapping of in_usd); this adds the
-- output leg alongside. Additive, nullable, idempotent.
-- ============================================================
ALTER TABLE sm.swaps ADD COLUMN IF NOT EXISTS output_usd_value NUMERIC(38, 6);
COMMENT ON COLUMN sm.swaps.usd_value IS
    'USD value of the INPUT leg at index time (legacy in_usd).';
COMMENT ON COLUMN sm.swaps.output_usd_value IS
    'USD value of the OUTPUT leg at index time (legacy out_usd).';
