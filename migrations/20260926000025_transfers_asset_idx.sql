-- no-transaction
CREATE INDEX CONCURRENTLY IF NOT EXISTS transfers_asset_block_idx
    ON sm.transfers (asset_id, block_height, event_id);
