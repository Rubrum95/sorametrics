-- Depth check behind every USD valuation. The quoted price is marginal (an
-- infinitesimal input), which is meaningless for assets whose pools hold a
-- few dollars: CERES quoted 90 736 USD while selling one unit returned
-- 0.10 USD. The price sweep therefore also quotes the sale of a small USD
-- notional of the asset (1 USD: liquidity is thin network-wide) and records
-- what the chain would actually pay for it.
CREATE TABLE IF NOT EXISTS sm.asset_liquidity (
    asset_id      TEXT PRIMARY KEY,
    marginal_usd  DOUBLE PRECISION NOT NULL,
    notional_usd  DOUBLE PRECISION NOT NULL,
    returned_usd  DOUBLE PRECISION NOT NULL,
    liquid        BOOLEAN NOT NULL,
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);
