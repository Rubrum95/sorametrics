-- no-transaction
CREATE INDEX CONCURRENTLY IF NOT EXISTS transfers_to_block_from_idx
    ON sm.transfers (to_address, block_height DESC) INCLUDE (from_address);
