-- ============================================================
-- SoraMetrics — Minamoto (SORA Nexus / Iroha 3) schema
-- All tables under "mn" schema. Strict isolation from "sm" (SORA v2).
-- Idempotent: safe to run multiple times.
-- ============================================================

CREATE SCHEMA IF NOT EXISTS mn;
COMMENT ON SCHEMA mn IS 'SORA Nexus Minamoto mainnet metrics (Iroha 3, Torii REST + Prometheus).';

-- ------------------------------------------------------------
-- Schema versioning (manual migrations bump version)
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS mn.schema_version (
    version     INT         PRIMARY KEY,
    applied_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    description TEXT        NOT NULL
);

-- ------------------------------------------------------------
-- Single-row latest network state (refreshed on every poll)
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS mn.network_state (
    id                     INT         PRIMARY KEY DEFAULT 1 CHECK (id = 1),
    peers                  INT         NOT NULL,
    domains                INT         NOT NULL,
    accounts               INT         NOT NULL,
    assets                 INT         NOT NULL,
    transactions_accepted  BIGINT      NOT NULL,
    transactions_rejected  BIGINT      NOT NULL,
    block_height           BIGINT      NOT NULL,
    finalized_block        BIGINT      NOT NULL,
    avg_commit_time_ms     INT         NOT NULL,
    avg_block_time_ms      BIGINT      NOT NULL,
    last_block_at          TIMESTAMPTZ,
    iroha_version          TEXT,
    updated_at             TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
COMMENT ON TABLE mn.network_state IS 'Latest snapshot from /v1/explorer/metrics + /status. Single row, upsert-only.';

-- ------------------------------------------------------------
-- Blocks
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS mn.blocks (
    height                 BIGINT      PRIMARY KEY,
    hash                   BYTEA       NOT NULL UNIQUE
                                       CHECK (octet_length(hash) = 32),
    prev_hash              BYTEA       CHECK (prev_hash IS NULL OR octet_length(prev_hash) = 32),
    transactions_hash      BYTEA       CHECK (transactions_hash IS NULL OR octet_length(transactions_hash) = 32),
    created_at             TIMESTAMPTZ NOT NULL,
    transactions_committed INT         NOT NULL DEFAULT 0,
    transactions_rejected  INT         NOT NULL DEFAULT 0,
    indexed_at             TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_mn_blocks_created_at  ON mn.blocks (created_at DESC);
CREATE INDEX IF NOT EXISTS idx_mn_blocks_indexed_at  ON mn.blocks (indexed_at);

-- ------------------------------------------------------------
-- Transactions
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS mn.transactions (
    hash             BYTEA       PRIMARY KEY CHECK (octet_length(hash) = 32),
    block_height     BIGINT      NOT NULL REFERENCES mn.blocks(height) ON DELETE CASCADE,
    authority        TEXT        NOT NULL,
    created_at       TIMESTAMPTZ NOT NULL,
    executable_kind  TEXT        NOT NULL,
    status           TEXT        NOT NULL,
    indexed_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_mn_tx_block       ON mn.transactions (block_height);
CREATE INDEX IF NOT EXISTS idx_mn_tx_authority   ON mn.transactions (authority);
CREATE INDEX IF NOT EXISTS idx_mn_tx_created_at  ON mn.transactions (created_at DESC);
CREATE INDEX IF NOT EXISTS idx_mn_tx_rejected
    ON mn.transactions (created_at DESC) WHERE status <> 'Committed';

-- ------------------------------------------------------------
-- Accounts (I105 katakana literal = TEXT, never normalize)
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS mn.accounts (
    id                          TEXT        PRIMARY KEY,
    network_prefix              INT         NOT NULL DEFAULT 753,
    has_primary_alias           BOOLEAN     NOT NULL DEFAULT FALSE,
    primary_alias               TEXT,
    primary_alias_dataspace     TEXT,
    primary_alias_domain        TEXT,
    primary_alias_name          TEXT,
    multisig_quorum             INT,
    multisig_signatories_count  INT,
    metadata                    JSONB       NOT NULL DEFAULT '{}'::jsonb,
    first_seen_at               TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_seen_at                TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_mn_accounts_alias
    ON mn.accounts (primary_alias) WHERE primary_alias IS NOT NULL;

-- ------------------------------------------------------------
-- Asset definitions (catalogue of asset types: XOR, USD, etc.)
-- Lives separately from per-account balances. id is the Iroha 3
-- internal hash; alias (e.g. "xor#universal") is the human handle.
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS mn.asset_definitions (
    id                     TEXT        PRIMARY KEY,
    alias                  TEXT,
    name                   TEXT,
    description            TEXT,
    owned_by               TEXT        NOT NULL,
    mintable               TEXT,
    confidential_mode      TEXT,
    balance_scope_policy   TEXT,
    total_quantity         NUMERIC,
    metadata               JSONB       NOT NULL DEFAULT '{}'::jsonb,
    indexed_at             TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at             TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_mn_asset_def_alias
    ON mn.asset_definitions (alias) WHERE alias IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_mn_asset_def_name
    ON mn.asset_definitions (name) WHERE name IS NOT NULL;

-- ------------------------------------------------------------
-- Assets (one row per (definition_id, account_id))
-- value: arbitrary-precision NUMERIC. Different assets carry different
-- decimal scales (XOR is fractional; cabbage/rose are integers). A fixed
-- scale would silently truncate balances, so we accept what Torii sends.
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS mn.assets (
    definition_id  TEXT        NOT NULL,
    account_id     TEXT        NOT NULL REFERENCES mn.accounts(id) ON DELETE CASCADE,
    value          NUMERIC     NOT NULL DEFAULT 0,
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (definition_id, account_id)
);
CREATE INDEX IF NOT EXISTS idx_mn_assets_account ON mn.assets (account_id);
CREATE INDEX IF NOT EXISTS idx_mn_assets_def     ON mn.assets (definition_id);

-- ------------------------------------------------------------
-- Domains
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS mn.domains (
    id              TEXT        PRIMARY KEY,
    owned_by        TEXT        NOT NULL,
    accounts_count  INT         NOT NULL DEFAULT 0,
    assets_count    INT         NOT NULL DEFAULT 0,
    nfts_count      INT         NOT NULL DEFAULT 0,
    metadata        JSONB       NOT NULL DEFAULT '{}'::jsonb,
    indexed_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ------------------------------------------------------------
-- Peers (network topology, multiaddr is canonical)
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS mn.peers (
    multiaddr      TEXT        PRIMARY KEY,
    public_key     TEXT,
    ip_address     TEXT,
    port           INT,
    first_seen_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_seen_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    is_active      BOOLEAN     NOT NULL DEFAULT TRUE
);
CREATE INDEX IF NOT EXISTS idx_mn_peers_active ON mn.peers (is_active) WHERE is_active;

-- ------------------------------------------------------------
-- Prometheus metric snapshots (rolling time series, 30-day retention)
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS mn.metrics_snapshots (
    id           BIGSERIAL    PRIMARY KEY,
    ts           TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    metric_name  TEXT         NOT NULL,
    labels       JSONB        NOT NULL DEFAULT '{}'::jsonb,
    value        DOUBLE PRECISION NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_mn_metrics_name_ts ON mn.metrics_snapshots (metric_name, ts DESC);
CREATE INDEX IF NOT EXISTS idx_mn_metrics_ts      ON mn.metrics_snapshots (ts);

-- ------------------------------------------------------------
-- Indexer progress / health tracking
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS mn.indexer_state (
    name             TEXT        PRIMARY KEY,
    last_value       JSONB       NOT NULL DEFAULT '{}'::jsonb,
    last_run_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_run_status  TEXT,
    error_count      INT         NOT NULL DEFAULT 0,
    last_error       TEXT
);

-- ------------------------------------------------------------
-- Version row
-- ------------------------------------------------------------
-- ------------------------------------------------------------
-- Migrations applied AFTER initial schema. Each block is idempotent.
-- ------------------------------------------------------------

-- v3: Instructions (ISIs) — Iroha 3's equivalent of Substrate "events".
-- One tx can emit many instructions (Mint, Burn, Transfer, Register, Grant…).
-- Same row = same (transaction_hash, instruction_index). Payload kept as
-- JSONB so we don't lose new ISI variants when Iroha extends them.
CREATE TABLE IF NOT EXISTS mn.instructions (
    transaction_hash  BYTEA       NOT NULL CHECK (octet_length(transaction_hash) = 32),
    instruction_index INT         NOT NULL,
    block_height      BIGINT      NOT NULL,
    authority         TEXT        NOT NULL,
    kind              TEXT        NOT NULL,
    payload           JSONB       NOT NULL DEFAULT '{}'::jsonb,
    transaction_status TEXT       NOT NULL,
    created_at        TIMESTAMPTZ NOT NULL,
    indexed_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (transaction_hash, instruction_index)
);
CREATE INDEX IF NOT EXISTS idx_mn_isi_block      ON mn.instructions (block_height);
CREATE INDEX IF NOT EXISTS idx_mn_isi_kind       ON mn.instructions (kind);
CREATE INDEX IF NOT EXISTS idx_mn_isi_authority  ON mn.instructions (authority);
CREATE INDEX IF NOT EXISTS idx_mn_isi_created_at ON mn.instructions (created_at DESC);

-- v2: assets.value scale fix. Drop the (78,0) fixed scale that truncated
-- decimals on assets like XOR (4.37620 became 4). Switch to unbounded
-- NUMERIC. ALTER preserves data; existing truncated rows must be re-fetched
-- from Torii by the indexer (idempotent upsert overwrites).
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'mn' AND table_name = 'assets'
          AND column_name = 'value' AND numeric_precision = 78 AND numeric_scale = 0
    ) THEN
        ALTER TABLE mn.assets ALTER COLUMN value TYPE NUMERIC;
        -- Existing rows will be refreshed by the indexer's next pass.
    END IF;
END $$;

INSERT INTO mn.schema_version (version, description)
VALUES (1, 'Initial schema: network_state, blocks, transactions, accounts, assets, domains, peers, metrics_snapshots, indexer_state')
ON CONFLICT (version) DO NOTHING;

INSERT INTO mn.schema_version (version, description)
VALUES (2, 'asset_definitions table; assets.value to unbounded NUMERIC (decimals fix)')
ON CONFLICT (version) DO NOTHING;

INSERT INTO mn.schema_version (version, description)
VALUES (3, 'instructions table (ISIs feed) with JSONB payload + kind/block/authority indexes')
ON CONFLICT (version) DO NOTHING;

-- v4: enrich mn.transactions with cross-chain claim metadata. Each XOR claim
-- tx on Minamoto carries metadata pointing back to its SORA v2 burn tx and
-- the recipient. We extract those into typed columns so the migration tracker
-- doesn't need to scan JSON on every query.
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns
                   WHERE table_schema='mn' AND table_name='transactions' AND column_name='sora_v2_claim_tx_hash') THEN
        ALTER TABLE mn.transactions
            ADD COLUMN sora_v2_claim_tx_hash      TEXT,
            ADD COLUMN sora_nexus_claim_recipient TEXT,
            ADD COLUMN fee_sponsor                TEXT;
        CREATE INDEX idx_mn_tx_v2_burn      ON mn.transactions (sora_v2_claim_tx_hash) WHERE sora_v2_claim_tx_hash IS NOT NULL;
        CREATE INDEX idx_mn_tx_claim_recip  ON mn.transactions (sora_nexus_claim_recipient) WHERE sora_nexus_claim_recipient IS NOT NULL;
    END IF;
END $$;

INSERT INTO mn.schema_version (version, description)
VALUES (4, 'transactions enriched with cross-chain claim metadata (v2 burn tx, recipient, fee sponsor)')
ON CONFLICT (version) DO NOTHING;

-- v5: add v2-side resolution. Once we have the burn tx hash from Minamoto we
-- also lift the v2 block + signer (the SS58 wallet that initiated the burn)
-- so both networks can render the migration with the COUNTERPART account
-- linked to its native dashboard.
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns
                   WHERE table_schema='mn' AND table_name='transactions' AND column_name='sora_v2_block') THEN
        ALTER TABLE mn.transactions
            ADD COLUMN sora_v2_block  BIGINT,
            ADD COLUMN sora_v2_signer TEXT;
        CREATE INDEX idx_mn_tx_v2_signer ON mn.transactions (sora_v2_signer) WHERE sora_v2_signer IS NOT NULL;
        CREATE INDEX idx_mn_tx_v2_block  ON mn.transactions (sora_v2_block)  WHERE sora_v2_block  IS NOT NULL;
    END IF;
END $$;

INSERT INTO mn.schema_version (version, description)
VALUES (5, 'transactions enriched with v2 burn-side resolution (block + signer SS58)')
ON CONFLICT (version) DO NOTHING;
