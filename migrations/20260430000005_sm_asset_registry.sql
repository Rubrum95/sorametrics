-- ============================================================
-- SoraMetrics v33 — sm.asset_registry
--
-- Stable lookup of (asset_id → symbol, name, decimals, logo).
-- Bootstrapped with the 10 SORA "essential" tokens. The full
-- ~962-asset whitelist is loaded by a separate ops command.
--
-- The frontend depends on this for symbol+logo enrichment and for
-- amount-by-decimals normalization in the API response. Without it,
-- swaps/transfers display as raw planck integers.
--
-- Idempotent: safe to re-run.
-- ============================================================

CREATE TABLE IF NOT EXISTS sm.asset_registry (
    asset_id    TEXT        PRIMARY KEY,
    symbol      TEXT        NOT NULL,
    name        TEXT,
    decimals    SMALLINT    NOT NULL DEFAULT 18,
    logo        TEXT,
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CHECK (decimals BETWEEN 0 AND 38),
    CHECK (length(asset_id) = 66)  -- 0x + 64 hex chars
);

CREATE INDEX IF NOT EXISTS asset_registry_symbol_idx
    ON sm.asset_registry (symbol);

COMMENT ON TABLE sm.asset_registry IS
    'Asset metadata used to enrich live_swaps / live_transfers / live_bridges responses.';

-- ------------------------------------------------------------
-- Bootstrap: 10 essential SORA tokens.
-- These are the same hardcoded list used by the Node legacy
-- `scripts/load_asset_registry.js` as a fallback when the GitHub
-- whitelist fetch fails. Logos are intentionally NULL here — they
-- get filled by the ops loader from the whitelist later.
-- ------------------------------------------------------------

INSERT INTO sm.asset_registry (asset_id, symbol, name, decimals) VALUES
    ('0x0200000000000000000000000000000000000000000000000000000000000000', 'XOR',    'SORA',                   18),
    ('0x0200040000000000000000000000000000000000000000000000000000000000', 'VAL',    'SORA Validator Token',   18),
    ('0x0200050000000000000000000000000000000000000000000000000000000000', 'PSWAP',  'Polkaswap',              18),
    ('0x0200060000000000000000000000000000000000000000000000000000000000', 'DAI',    'Dai Stablecoin',         18),
    ('0x0200070000000000000000000000000000000000000000000000000000000000', 'ETH',    'Ether',                  18),
    ('0x0200080000000000000000000000000000000000000000000000000000000000', 'XSTUSD', 'SORA Synthetic USD',     18),
    ('0x0200090000000000000000000000000000000000000000000000000000000000', 'XST',    'SORA Synthetics',        18),
    ('0x02000a0000000000000000000000000000000000000000000000000000000000', 'TBCD',   'TBCD',                   18),
    ('0x02000b0000000000000000000000000000000000000000000000000000000000', 'KEN',    'Kensetsu',               18),
    ('0x02000c0000000000000000000000000000000000000000000000000000000000', 'KUSD',   'Kensetsu USD',           18)
ON CONFLICT (asset_id) DO UPDATE SET
    symbol     = EXCLUDED.symbol,
    name       = EXCLUDED.name,
    decimals   = EXCLUDED.decimals,
    updated_at = NOW();
