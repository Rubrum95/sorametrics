-- ============================================================
-- SoraMetrics Materialized Views
-- Parses history_element.data jsonb into clean columns
-- JOINs with sm.asset_registry (symbols only) and sm.price_history
-- Logos resolved at query time to save ~20GB disk space
-- Timestamps converted to milliseconds (app convention)
-- ============================================================

-- Drop existing views if recreating
DROP MATERIALIZED VIEW IF EXISTS sm.mv_swaps CASCADE;
DROP MATERIALIZED VIEW IF EXISTS sm.mv_transfers CASCADE;
DROP MATERIALIZED VIEW IF EXISTS sm.mv_bridges CASCADE;
DROP MATERIALIZED VIEW IF EXISTS sm.mv_fees CASCADE;
DROP MATERIALIZED VIEW IF EXISTS sm.mv_liquidity_events CASCADE;
DROP MATERIALIZED VIEW IF EXISTS sm.mv_order_book_events CASCADE;
DROP MATERIALIZED VIEW IF EXISTS sm.mv_extrinsics CASCADE;

-- ============================================================
-- 1. SWAPS
-- Source: liquidityProxy.swap/swapTransfer/swapTransferBatch
-- Logos NOT stored — resolved via asset_registry at query time
-- ============================================================
CREATE MATERIALIZED VIEW sm.mv_swaps AS
SELECT
    he.id AS _row_id,
    he.timestamp::bigint * 1000 AS timestamp,
    he.block_height AS block,
    he.address AS wallet,
    he.data->>'baseAssetId' AS in_asset_id,
    COALESCE(ar_in.symbol, '0x' || UPPER(RIGHT(he.data->>'baseAssetId', 4))) AS in_symbol,
    he.data->>'baseAssetAmount' AS in_amount,
    COALESCE(
        (he.data->>'baseAssetAmount')::double precision * ph_in.price_usd,
        0
    ) AS in_usd,
    he.data->>'targetAssetId' AS out_asset_id,
    COALESCE(ar_out.symbol, '0x' || UPPER(RIGHT(he.data->>'targetAssetId', 4))) AS out_symbol,
    he.data->>'targetAssetAmount' AS out_amount,
    COALESCE(
        (he.data->>'targetAssetAmount')::double precision * ph_out.price_usd,
        0
    ) AS out_usd,
    he.id AS hash,
    he.id AS extrinsic_id
FROM history_element he
LEFT JOIN sm.asset_registry ar_in
    ON ar_in.asset_id = he.data->>'baseAssetId'
LEFT JOIN sm.asset_registry ar_out
    ON ar_out.asset_id = he.data->>'targetAssetId'
LEFT JOIN sm.price_history ph_in
    ON ph_in.asset_id = he.data->>'baseAssetId'
    AND ph_in.hour_bucket = (FLOOR(he.timestamp / 3600) * 3600)::int
LEFT JOIN sm.price_history ph_out
    ON ph_out.asset_id = he.data->>'targetAssetId'
    AND ph_out.hour_bucket = (FLOOR(he.timestamp / 3600) * 3600)::int
WHERE he.type = 'CALL'
    AND he.module = 'liquidityProxy'
    AND he.method IN ('swap', 'swapTransfer', 'swapTransferBatch')
    AND (he.execution->>'success')::boolean = true
    AND he.data IS NOT NULL
WITH NO DATA;

-- Unique index required for REFRESH CONCURRENTLY
CREATE UNIQUE INDEX idx_mv_swaps_rowid ON sm.mv_swaps(_row_id);
CREATE INDEX idx_mv_swaps_ts ON sm.mv_swaps(timestamp DESC);
CREATE INDEX idx_mv_swaps_wallet ON sm.mv_swaps(wallet);
CREATE INDEX idx_mv_swaps_block ON sm.mv_swaps(block);
CREATE INDEX idx_mv_swaps_in_sym ON sm.mv_swaps(in_symbol);
CREATE INDEX idx_mv_swaps_out_sym ON sm.mv_swaps(out_symbol);

-- ============================================================
-- 2. TRANSFERS
-- Source: assets.transfer (CALL) + assets.Transfer (EVENT)
-- CALLs have from=address, EVENTs have from in data
-- ============================================================
CREATE MATERIALIZED VIEW sm.mv_transfers AS
SELECT
    he.id AS _row_id,
    he.timestamp::bigint * 1000 AS timestamp,
    he.block_height AS block,
    COALESCE(he.data->>'from', he.address) AS from_addr,
    he.data->>'to' AS to_addr,
    he.data->>'amount' AS amount,
    COALESCE(ar.symbol, '0x' || UPPER(RIGHT(he.data->>'assetId', 4))) AS symbol,
    COALESCE(
        (he.data->>'amount')::double precision * ph.price_usd,
        0
    ) AS usd_value,
    he.data->>'assetId' AS asset_id,
    CASE WHEN he.type = 'CALL' THEN he.id
         ELSE SPLIT_PART(he.id, '-', 1) || '-' || SPLIT_PART(he.id, '-', 2)
    END AS hash,
    he.id AS extrinsic_id
FROM history_element he
LEFT JOIN sm.asset_registry ar
    ON ar.asset_id = he.data->>'assetId'
LEFT JOIN sm.price_history ph
    ON ph.asset_id = he.data->>'assetId'
    AND ph.hour_bucket = (FLOOR(he.timestamp / 3600) * 3600)::int
WHERE he.data IS NOT NULL
    AND he.data->>'amount' IS NOT NULL
    AND (he.data->>'assetId') IS NOT NULL
    AND (
        -- CALL: assets.transfer (successful)
        (he.type = 'CALL' AND he.module = 'assets' AND he.method = 'transfer'
         AND (he.execution->>'success')::boolean = true)
        OR
        -- EVENT: assets.Transfer (native transfers caught as events)
        (he.type = 'EVENT' AND he.module = 'assets' AND he.method = 'Transfer')
    )
WITH NO DATA;

CREATE UNIQUE INDEX idx_mv_transfers_rowid ON sm.mv_transfers(_row_id);
CREATE INDEX idx_mv_transfers_ts ON sm.mv_transfers(timestamp DESC);
CREATE INDEX idx_mv_transfers_from ON sm.mv_transfers(from_addr);
CREATE INDEX idx_mv_transfers_to ON sm.mv_transfers(to_addr);
CREATE INDEX idx_mv_transfers_block ON sm.mv_transfers(block);
CREATE INDEX idx_mv_transfers_symbol ON sm.mv_transfers(symbol);

-- ============================================================
-- 3. BRIDGES
-- Outgoing: ethBridge.transferToSidechain (CALL)
-- Incoming: bridgeMultisig.asMulti (CALL)
-- ============================================================
CREATE MATERIALIZED VIEW sm.mv_bridges AS
SELECT
    he.id AS _row_id,
    he.timestamp::bigint * 1000 AS timestamp,
    he.block_height AS block,
    'Ethereum' AS network,
    CASE
        WHEN he.module = 'ethBridge' AND he.method = 'transferToSidechain' THEN 'Outgoing'
        WHEN he.module = 'bridgeMultisig' THEN 'Incoming'
    END AS direction,
    he.address AS sender,
    CASE
        WHEN he.module = 'ethBridge' THEN he.data->>'sidechainAddress'
        WHEN he.module = 'bridgeMultisig' THEN he.data->>'to'
    END AS recipient,
    he.data->>'assetId' AS asset_id,
    COALESCE(ar.symbol, '0x' || UPPER(RIGHT(he.data->>'assetId', 4))) AS symbol,
    he.data->>'amount' AS amount,
    COALESCE(
        (he.data->>'amount')::double precision * ph.price_usd,
        0
    ) AS usd_value,
    he.id AS hash,
    he.id AS extrinsic_id
FROM history_element he
LEFT JOIN sm.asset_registry ar
    ON ar.asset_id = he.data->>'assetId'
LEFT JOIN sm.price_history ph
    ON ph.asset_id = he.data->>'assetId'
    AND ph.hour_bucket = (FLOOR(he.timestamp / 3600) * 3600)::int
WHERE he.data IS NOT NULL
    AND (he.execution->>'success')::boolean = true
    AND (
        (he.type = 'CALL' AND he.module = 'ethBridge' AND he.method = 'transferToSidechain')
        OR
        (he.type = 'CALL' AND he.module = 'bridgeMultisig' AND he.method IN ('asMulti', 'asMultiThreshold1'))
    )
WITH NO DATA;

CREATE UNIQUE INDEX idx_mv_bridges_rowid ON sm.mv_bridges(_row_id);
CREATE INDEX idx_mv_bridges_ts ON sm.mv_bridges(timestamp DESC);
CREATE INDEX idx_mv_bridges_sender ON sm.mv_bridges(sender);
CREATE INDEX idx_mv_bridges_recipient ON sm.mv_bridges(recipient);
CREATE INDEX idx_mv_bridges_block ON sm.mv_bridges(block);

-- ============================================================
-- 4. FEES
-- Source: All CALLs with network_fee > 0
-- Fee = network_fee (planck) / 1e18 = XOR amount
-- USD = XOR amount * XOR price at hour
-- ============================================================
CREATE MATERIALIZED VIEW sm.mv_fees AS
SELECT
    he.id AS _row_id,
    he.timestamp::bigint * 1000 AS timestamp,
    he.block_height AS block,
    CASE
        WHEN he.module = 'liquidityProxy' THEN 'Swap'
        WHEN he.module = 'ethBridge' THEN 'Bridge'
        WHEN he.module = 'assets' THEN 'Transfer'
        WHEN he.module = 'poolXYK' THEN 'Liquidity'
        WHEN he.module = 'orderBook' THEN 'OrderBook'
        WHEN he.module = 'staking' THEN 'Staking'
        WHEN he.module = 'kensetsu' THEN 'Kensetsu'
        WHEN he.module = 'demeterFarmingPlatform' THEN 'Farming'
        ELSE he.module
    END AS type,
    he.network_fee::double precision / 1e18 AS amount,
    COALESCE(
        (he.network_fee::double precision / 1e18) * ph.price_usd,
        0
    ) AS usd_value,
    '1' AS denom_factor
FROM history_element he
LEFT JOIN sm.price_history ph
    ON ph.asset_id = '0x0200000000000000000000000000000000000000000000000000000000000000'
    AND ph.hour_bucket = (FLOOR(he.timestamp / 3600) * 3600)::int
WHERE he.type = 'CALL'
    AND he.network_fee IS NOT NULL
    AND he.network_fee != '0'
WITH NO DATA;

CREATE UNIQUE INDEX idx_mv_fees_rowid ON sm.mv_fees(_row_id);
CREATE INDEX idx_mv_fees_ts ON sm.mv_fees(timestamp DESC);
CREATE INDEX idx_mv_fees_block ON sm.mv_fees(block);
CREATE INDEX idx_mv_fees_type ON sm.mv_fees(type);

-- ============================================================
-- 5. LIQUIDITY EVENTS
-- Source: poolXYK.depositLiquidity / withdrawLiquidity
-- ============================================================
CREATE MATERIALIZED VIEW sm.mv_liquidity_events AS
SELECT
    he.id AS _row_id,
    he.timestamp::bigint * 1000 AS timestamp,
    he.block_height AS block,
    he.address AS wallet,
    COALESCE(ar_base.symbol, '0x' || UPPER(RIGHT(he.data->>'baseAssetId', 4))) AS pool_base,
    COALESCE(ar_target.symbol, '0x' || UPPER(RIGHT(he.data->>'targetAssetId', 4))) AS pool_target,
    he.data->>'baseAssetAmount' AS base_amount,
    he.data->>'targetAssetAmount' AS target_amount,
    COALESCE(
        (he.data->>'baseAssetAmount')::double precision * ph_base.price_usd,
        0
    ) + COALESCE(
        (he.data->>'targetAssetAmount')::double precision * ph_target.price_usd,
        0
    ) AS usd_value,
    CASE
        WHEN he.method = 'depositLiquidity' THEN 'Deposit'
        WHEN he.method = 'withdrawLiquidity' THEN 'Withdraw'
    END AS type,
    he.id AS hash,
    he.id AS extrinsic_id
FROM history_element he
LEFT JOIN sm.asset_registry ar_base
    ON ar_base.asset_id = he.data->>'baseAssetId'
LEFT JOIN sm.asset_registry ar_target
    ON ar_target.asset_id = he.data->>'targetAssetId'
LEFT JOIN sm.price_history ph_base
    ON ph_base.asset_id = he.data->>'baseAssetId'
    AND ph_base.hour_bucket = (FLOOR(he.timestamp / 3600) * 3600)::int
LEFT JOIN sm.price_history ph_target
    ON ph_target.asset_id = he.data->>'targetAssetId'
    AND ph_target.hour_bucket = (FLOOR(he.timestamp / 3600) * 3600)::int
WHERE he.type = 'CALL'
    AND he.module = 'poolXYK'
    AND he.method IN ('depositLiquidity', 'withdrawLiquidity')
    AND (he.execution->>'success')::boolean = true
    AND he.data IS NOT NULL
WITH NO DATA;

CREATE UNIQUE INDEX idx_mv_liq_rowid ON sm.mv_liquidity_events(_row_id);
CREATE INDEX idx_mv_liq_ts ON sm.mv_liquidity_events(timestamp DESC);
CREATE INDEX idx_mv_liq_wallet ON sm.mv_liquidity_events(wallet);
CREATE INDEX idx_mv_liq_block ON sm.mv_liquidity_events(block);

-- ============================================================
-- 6. ORDER BOOK EVENTS
-- Source: orderBook.placeLimitOrder / cancelLimitOrder / cancelLimitOrdersBatch
-- ============================================================
CREATE MATERIALIZED VIEW sm.mv_order_book_events AS
SELECT
    he.id AS _row_id,
    he.timestamp::bigint * 1000 AS timestamp,
    TO_CHAR(TO_TIMESTAMP(he.timestamp), 'YYYY-MM-DD HH24:MI:SS') AS formatted_time,
    he.block_height AS block,
    CASE
        WHEN he.method = 'placeLimitOrder' THEN 'Place'
        WHEN he.method = 'cancelLimitOrder' THEN 'Cancel'
        WHEN he.method = 'cancelLimitOrdersBatch' THEN 'CancelBatch'
    END AS event_type,
    he.address AS wallet,
    COALESCE(he.data->>'orderId', '')::text AS order_id,
    COALESCE(ar_base.symbol, '0x' || UPPER(RIGHT(he.data->>'baseAssetId', 4))) AS base_asset,
    COALESCE(ar_quote.symbol, '0x' || UPPER(RIGHT(he.data->>'quoteAssetId', 4))) AS quote_asset,
    COALESCE(he.data->>'side', '') AS side,
    COALESCE(he.data->>'price', '0') AS price,
    COALESCE(he.data->>'amount', '0') AS amount,
    COALESCE(
        (he.data->>'amount')::double precision * (he.data->>'price')::double precision
            * ph.price_usd,
        0
    ) AS usd_value,
    he.id AS hash,
    he.id AS extrinsic_id
FROM history_element he
LEFT JOIN sm.asset_registry ar_base
    ON ar_base.asset_id = he.data->>'baseAssetId'
LEFT JOIN sm.asset_registry ar_quote
    ON ar_quote.asset_id = he.data->>'quoteAssetId'
LEFT JOIN sm.price_history ph
    ON ph.asset_id = he.data->>'quoteAssetId'
    AND ph.hour_bucket = (FLOOR(he.timestamp / 3600) * 3600)::int
WHERE he.type = 'CALL'
    AND he.module = 'orderBook'
    AND he.method IN ('placeLimitOrder', 'cancelLimitOrder', 'cancelLimitOrdersBatch')
    AND (he.execution->>'success')::boolean = true
    AND he.data IS NOT NULL
WITH NO DATA;

CREATE UNIQUE INDEX idx_mv_ob_rowid ON sm.mv_order_book_events(_row_id);
CREATE INDEX idx_mv_ob_ts ON sm.mv_order_book_events(timestamp DESC);
CREATE INDEX idx_mv_ob_wallet ON sm.mv_order_book_events(wallet);
CREATE INDEX idx_mv_ob_block ON sm.mv_order_book_events(block);

-- ============================================================
-- 7. EXTRINSICS
-- Source: All CALLs with a signer (address)
-- args_json NOT stored — resolved via history_element at query time
-- events_json NOT stored — always NULL in subsquid data
-- ============================================================
CREATE MATERIALIZED VIEW sm.mv_extrinsics AS
SELECT
    he.id AS _row_id,
    he.timestamp::bigint * 1000 AS timestamp,
    he.block_height AS block,
    ROW_NUMBER() OVER (PARTITION BY he.block_height ORDER BY he.id) AS extrinsic_index,
    he.id AS hash,
    he.module AS section,
    he.method AS method,
    he.address AS signer,
    CASE WHEN (he.execution->>'success')::boolean THEN 1 ELSE 0 END AS success,
    COALESCE(he.execution->>'error', '') AS error_msg
FROM history_element he
WHERE he.type = 'CALL'
    AND he.address IS NOT NULL
WITH NO DATA;

CREATE UNIQUE INDEX idx_mv_ext_rowid ON sm.mv_extrinsics(_row_id);
CREATE INDEX idx_mv_ext_ts ON sm.mv_extrinsics(timestamp DESC);
CREATE INDEX idx_mv_ext_block ON sm.mv_extrinsics(block);
CREATE INDEX idx_mv_ext_signer ON sm.mv_extrinsics(signer);
CREATE INDEX idx_mv_ext_section ON sm.mv_extrinsics(section);
CREATE INDEX idx_mv_ext_method ON sm.mv_extrinsics(method);
CREATE INDEX idx_mv_ext_success ON sm.mv_extrinsics(success);
