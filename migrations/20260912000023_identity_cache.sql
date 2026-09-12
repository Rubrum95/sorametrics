-- ============================================================
-- 0023: on-chain identity cache (the Node's sm.identity_cache).
--
-- One row per resolved account; `updated_at` is epoch milliseconds
-- (the Node stored Date.now()). Read before the chain (24 h TTL),
-- written after every chain lookup, loaded into memory at boot.
-- ============================================================
CREATE TABLE IF NOT EXISTS sm.identity_cache (
    address    TEXT PRIMARY KEY,
    display    TEXT,
    email      TEXT,
    web        TEXT,
    twitter    TEXT,
    discord    TEXT,
    updated_at BIGINT NOT NULL
);
