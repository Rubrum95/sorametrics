-- no-transaction
CREATE INDEX CONCURRENTLY IF NOT EXISTS swaps_output_asset_block_idx
    ON sm.swaps (output_asset_id, block_height, event_id);
