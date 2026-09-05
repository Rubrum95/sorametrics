-- ============================================================
-- 0010: whitelist flag on the asset registry + latest price per asset.
--
-- 1. sm.asset_registry.whitelisted: the Node's /tokens lists the
--    official sora-xor whitelist (ASSETS), not the whole registry. The
--    legacy registry (962 rows, ETL) also carries every asset ever seen
--    in swaps — those must resolve symbols but must NOT appear in
--    /tokens. Rows loaded by `ops load-asset-registry` are flagged.
--    Existing rows all came from that command → backfilled to true.
--
-- 2. ts.price_latest: the most recent DAI quote per asset. The Node
--    served /tokens from an in-process 60 s quote cache; v33's API is
--    read-only (no RPC), so the ingest sampler persists the last quote
--    here and the API reads it. Hourly buckets stay in price_history.
-- ============================================================
ALTER TABLE sm.asset_registry
    ADD COLUMN IF NOT EXISTS whitelisted BOOLEAN NOT NULL DEFAULT false;
UPDATE sm.asset_registry SET whitelisted = true;
COMMENT ON COLUMN sm.asset_registry.whitelisted IS
    'true = in the official sora-xor whitelist (listed by /tokens); false = registry-only (symbol resolution).';

CREATE TABLE IF NOT EXISTS ts.price_latest (
    asset_id    TEXT             PRIMARY KEY,
    price_usd   DOUBLE PRECISION NOT NULL,
    sampled_at  TIMESTAMPTZ      NOT NULL
);
COMMENT ON TABLE ts.price_latest IS
    'Last liquidityProxy quote (USD via DAI) per asset, written by the ingest price sampler.';
