-- ============================================================
-- 0020: preimage events (the Node's preimage_indexer.js SQLite table).
--
-- One row per `preimage.*` event; `reason` / `reason_detail` explain a
-- Cleared / Unnoted (runtime upgrade in the block, scheduler dispatch,
-- manual unnote). Serves /governance/preimage/:hash/events-fast and the
-- first-seen enrichment of /governance/preimages.
-- ============================================================
CREATE TABLE IF NOT EXISTS sm.preimage_events (
    block_height   BIGINT   NOT NULL,
    event_index    INTEGER  NOT NULL,
    ts             BIGINT,
    section        TEXT     NOT NULL DEFAULT 'preimage',
    method         TEXT     NOT NULL,
    hash           TEXT     NOT NULL,
    data           JSONB,
    reason         TEXT,
    reason_detail  TEXT,
    PRIMARY KEY (block_height, event_index)
);
CREATE INDEX IF NOT EXISTS idx_pe_hash ON sm.preimage_events (hash);
CREATE INDEX IF NOT EXISTS idx_pe_ts   ON sm.preimage_events (ts);
