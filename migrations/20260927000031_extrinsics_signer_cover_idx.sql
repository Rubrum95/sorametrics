-- no-transaction
CREATE INDEX CONCURRENTLY IF NOT EXISTS extrinsics_signer_cover_idx
    ON sm.extrinsics (signer, block_height DESC) INCLUDE (block_timestamp, success, section);
