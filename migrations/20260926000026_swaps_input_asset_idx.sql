-- no-transaction
CREATE INDEX CONCURRENTLY IF NOT EXISTS swaps_input_asset_block_idx
    ON sm.swaps (input_asset_id, block_height, event_id);
