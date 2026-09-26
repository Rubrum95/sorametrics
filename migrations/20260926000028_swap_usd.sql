CREATE OR REPLACE FUNCTION sm.swap_usd(input_usd NUMERIC, output_usd NUMERIC)
RETURNS NUMERIC
LANGUAGE sql IMMUTABLE PARALLEL SAFE
AS $$ SELECT LEAST(NULLIF(input_usd, 0), NULLIF(output_usd, 0)) $$;

COMMENT ON FUNCTION sm.swap_usd(NUMERIC, NUMERIC) IS
    'USD value of a swap: its cheaper priced leg (0 = unknown). A marginal quote of an illiquid token can overvalue one leg by orders of magnitude.';
