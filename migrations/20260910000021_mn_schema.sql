-- Minamoto (SORA Nexus / Iroha 3) indexed state. Mirrors the Node's
-- `minamoto/schema.sql` (v1..v5) so the ETL copies rows verbatim and the
-- `/api/minamoto/*` contract reads the same columns. Strictly isolated
-- from `sm.*`: no foreign keys across schemas.

CREATE SCHEMA IF NOT EXISTS mn;

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

CREATE TABLE IF NOT EXISTS mn.blocks (
    height                 BIGINT      PRIMARY KEY,
    hash                   BYTEA       NOT NULL UNIQUE CHECK (octet_length(hash) = 32),
    prev_hash              BYTEA       CHECK (prev_hash IS NULL OR octet_length(prev_hash) = 32),
    transactions_hash      BYTEA       CHECK (transactions_hash IS NULL OR octet_length(transactions_hash) = 32),
    created_at             TIMESTAMPTZ NOT NULL,
    transactions_committed INT         NOT NULL DEFAULT 0,
    transactions_rejected  INT         NOT NULL DEFAULT 0,
    indexed_at             TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_mn_blocks_created_at ON mn.blocks (created_at DESC);

CREATE TABLE IF NOT EXISTS mn.transactions (
    hash                        BYTEA       PRIMARY KEY CHECK (octet_length(hash) = 32),
    block_height                BIGINT      NOT NULL REFERENCES mn.blocks(height) ON DELETE CASCADE,
    authority                   TEXT        NOT NULL,
    created_at                  TIMESTAMPTZ NOT NULL,
    executable_kind             TEXT        NOT NULL,
    status                      TEXT        NOT NULL,
    indexed_at                  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    sora_v2_claim_tx_hash       TEXT,
    sora_nexus_claim_recipient  TEXT,
    fee_sponsor                 TEXT,
    sora_v2_block               BIGINT,
    sora_v2_signer              TEXT
);
CREATE INDEX IF NOT EXISTS idx_mn_tx_block      ON mn.transactions (block_height);
CREATE INDEX IF NOT EXISTS idx_mn_tx_authority  ON mn.transactions (authority);
CREATE INDEX IF NOT EXISTS idx_mn_tx_created_at ON mn.transactions (created_at DESC);
CREATE INDEX IF NOT EXISTS idx_mn_tx_v2_burn    ON mn.transactions (sora_v2_claim_tx_hash) WHERE sora_v2_claim_tx_hash IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_mn_tx_claim_recip ON mn.transactions (sora_nexus_claim_recipient) WHERE sora_nexus_claim_recipient IS NOT NULL;

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

CREATE TABLE IF NOT EXISTS mn.assets (
    definition_id  TEXT        NOT NULL,
    account_id     TEXT        NOT NULL REFERENCES mn.accounts(id) ON DELETE CASCADE,
    value          NUMERIC     NOT NULL DEFAULT 0,
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (definition_id, account_id)
);
CREATE INDEX IF NOT EXISTS idx_mn_assets_account ON mn.assets (account_id);
CREATE INDEX IF NOT EXISTS idx_mn_assets_def     ON mn.assets (definition_id);

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

CREATE TABLE IF NOT EXISTS mn.peers (
    multiaddr      TEXT        PRIMARY KEY,
    public_key     TEXT,
    ip_address     TEXT,
    port           INT,
    first_seen_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_seen_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    is_active      BOOLEAN     NOT NULL DEFAULT TRUE
);

CREATE TABLE IF NOT EXISTS mn.metrics_snapshots (
    id           BIGSERIAL        PRIMARY KEY,
    ts           TIMESTAMPTZ      NOT NULL DEFAULT NOW(),
    metric_name  TEXT             NOT NULL,
    labels       JSONB            NOT NULL DEFAULT '{}'::jsonb,
    value        DOUBLE PRECISION NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_mn_metrics_name_ts ON mn.metrics_snapshots (metric_name, ts DESC);
CREATE INDEX IF NOT EXISTS idx_mn_metrics_ts      ON mn.metrics_snapshots (ts);

CREATE TABLE IF NOT EXISTS mn.indexer_state (
    name             TEXT        PRIMARY KEY,
    last_value       JSONB       NOT NULL DEFAULT '{}'::jsonb,
    last_run_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_run_status  TEXT,
    error_count      INT         NOT NULL DEFAULT 0,
    last_error       TEXT
);

CREATE TABLE IF NOT EXISTS mn.instructions (
    transaction_hash   BYTEA       NOT NULL CHECK (octet_length(transaction_hash) = 32),
    instruction_index  INT         NOT NULL,
    block_height       BIGINT      NOT NULL,
    authority          TEXT        NOT NULL,
    kind               TEXT        NOT NULL,
    payload            JSONB       NOT NULL DEFAULT '{}'::jsonb,
    transaction_status TEXT        NOT NULL,
    created_at         TIMESTAMPTZ NOT NULL,
    indexed_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (transaction_hash, instruction_index)
);
CREATE INDEX IF NOT EXISTS idx_mn_isi_block      ON mn.instructions (block_height);
CREATE INDEX IF NOT EXISTS idx_mn_isi_kind       ON mn.instructions (kind);
CREATE INDEX IF NOT EXISTS idx_mn_isi_authority  ON mn.instructions (authority);
CREATE INDEX IF NOT EXISTS idx_mn_isi_created_at ON mn.instructions (created_at DESC);
