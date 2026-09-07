-- ============================================================
-- 0015: order book events (the Node's live_order_book_events /
-- mv_order_book_events).
--
-- Live mechanism: every `orderBook` pallet event of the kinds
-- LimitOrderPlaced / LimitOrderCanceled / LimitOrderExecuted /
-- LimitOrderFilled / MarketOrderExecuted → one row. price / amount are
-- the chain's 18-decimal fixed-point values in human units
-- (`inner / 1e18`); side / price / amount are NULL when the event does
-- not carry them (canceled, filled). usd_value = amount in quote terms
-- × quote-asset price at index time (the mv_order_book_events formula;
-- the Node's live path wrote 0).
--
-- event_type / side use the lowercase live vocabulary the frontend
-- filters on ('placed' | 'canceled' | 'executed' | 'filled' | 'market');
-- the legacy MV's call-based 'Place' / 'Cancel' / 'CancelBatch' and
-- 'Buy' / 'Sell' are normalised on ETL (origin = 'legacy'). Legacy
-- cancel rows carry no pair (the subsquid call data lacked it), hence
-- the nullable asset columns; live rows always have both.
-- ============================================================
CREATE TABLE IF NOT EXISTS sm.order_book_events (
    block_height     BIGINT          NOT NULL,
    extrinsic_id     TEXT            NOT NULL,
    event_id         INTEGER         NOT NULL DEFAULT 0,
    block_timestamp  TIMESTAMPTZ     NOT NULL,
    event_type       TEXT            NOT NULL
        CHECK (event_type IN ('placed', 'canceled', 'executed', 'filled', 'market')),
    wallet           TEXT            NOT NULL,
    order_id         TEXT,
    base_asset_id    TEXT,
    quote_asset_id   TEXT,
    side             TEXT            CHECK (side IN ('buy', 'sell')),
    price            NUMERIC(60, 18),
    amount           NUMERIC(60, 18),
    usd_value        NUMERIC(38, 6),
    hash             TEXT,
    origin           TEXT            NOT NULL DEFAULT 'live' CHECK (origin IN ('live', 'legacy')),
    PRIMARY KEY (block_height, extrinsic_id, event_id)
);
CREATE INDEX IF NOT EXISTS order_book_events_block_event_idx
    ON sm.order_book_events (block_height DESC, event_id DESC);
CREATE INDEX IF NOT EXISTS order_book_events_ts_idx
    ON sm.order_book_events (block_timestamp DESC);
CREATE INDEX IF NOT EXISTS order_book_events_wallet_idx
    ON sm.order_book_events (wallet, block_height DESC);
CREATE INDEX IF NOT EXISTS order_book_events_type_idx
    ON sm.order_book_events (event_type, block_height DESC);
COMMENT ON TABLE sm.order_book_events IS
    'orderBook pallet events (placed/canceled/executed/filled/market). price/amount in human units (inner/1e18); usd_value = quote-terms amount × quote price at index time.';
