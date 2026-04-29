'use strict';
// ============================================================
// minamoto/db.js — PostgreSQL wrapper for the "mn" schema
// Reuses the same Postgres instance as sm.* (sora_subsquid_db)
// but writes ONLY to mn.*. Never touches sm.* or other schemas.
// ============================================================

const { Pool } = require('pg');
const fs = require('fs');
const path = require('path');

const PG_CONFIG = {
    host: process.env.PG_HOST || 'localhost',
    port: parseInt(process.env.PG_PORT, 10) || 23798,
    database: process.env.PG_DB || 'squid',
    user: process.env.PG_USER || 'postgres',
    password: process.env.PG_PASS || 'squid',
    max: parseInt(process.env.MINAMOTO_PG_MAX, 10) || 10,
    idleTimeoutMillis: 60000,
    connectionTimeoutMillis: 10000,
    statement_timeout: 30000,
};

let pool = null;

function getPool() {
    if (pool) return pool;
    pool = new Pool(PG_CONFIG);
    pool.on('error', (err) => {
        console.error('[minamoto.db] PG pool error:', err.message);
    });
    return pool;
}

async function applySchema() {
    const sqlPath = path.join(__dirname, 'schema.sql');
    const sql = fs.readFileSync(sqlPath, 'utf8');
    const client = await getPool().connect();
    try {
        await client.query(sql);
    } finally {
        client.release();
    }
}

async function ping() {
    const r = await getPool().query('SELECT 1 AS ok, current_database() AS db');
    return r.rows[0];
}

// ------------------------------------------------------------
// helpers
// ------------------------------------------------------------

function hexToBytea(hex) {
    if (!hex) return null;
    const clean = hex.startsWith('0x') ? hex.slice(2) : hex;
    if (!/^[0-9a-fA-F]*$/.test(clean) || clean.length % 2 !== 0) {
        throw new Error(`invalid hex: ${hex.slice(0, 16)}...`);
    }
    return Buffer.from(clean, 'hex');
}

function byteaToHex(buf) {
    if (!buf) return null;
    return Buffer.from(buf).toString('hex');
}

// ------------------------------------------------------------
// network_state (upsert single row)
// ------------------------------------------------------------

async function upsertNetworkState(s) {
    const sql = `
        INSERT INTO mn.network_state
            (id, peers, domains, accounts, assets,
             transactions_accepted, transactions_rejected,
             block_height, finalized_block,
             avg_commit_time_ms, avg_block_time_ms,
             last_block_at, iroha_version, updated_at)
        VALUES (1, $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, NOW())
        ON CONFLICT (id) DO UPDATE SET
            peers = EXCLUDED.peers,
            domains = EXCLUDED.domains,
            accounts = EXCLUDED.accounts,
            assets = EXCLUDED.assets,
            transactions_accepted = EXCLUDED.transactions_accepted,
            transactions_rejected = EXCLUDED.transactions_rejected,
            block_height = EXCLUDED.block_height,
            finalized_block = EXCLUDED.finalized_block,
            avg_commit_time_ms = EXCLUDED.avg_commit_time_ms,
            avg_block_time_ms = EXCLUDED.avg_block_time_ms,
            last_block_at = EXCLUDED.last_block_at,
            iroha_version = EXCLUDED.iroha_version,
            updated_at = NOW()
    `;
    await getPool().query(sql, [
        s.peers, s.domains, s.accounts, s.assets,
        s.transactions_accepted, s.transactions_rejected,
        s.block_height, s.finalized_block,
        s.avg_commit_time_ms, s.avg_block_time_ms,
        s.last_block_at, s.iroha_version || null,
    ]);
}

async function getNetworkState() {
    const r = await getPool().query('SELECT * FROM mn.network_state WHERE id = 1');
    return r.rows[0] || null;
}

// ------------------------------------------------------------
// blocks
// ------------------------------------------------------------

async function upsertBlock(b) {
    const sql = `
        INSERT INTO mn.blocks
            (height, hash, prev_hash, transactions_hash, created_at,
             transactions_committed, transactions_rejected)
        VALUES ($1, $2, $3, $4, $5, $6, $7)
        ON CONFLICT (height) DO UPDATE SET
            hash = EXCLUDED.hash,
            prev_hash = EXCLUDED.prev_hash,
            transactions_hash = EXCLUDED.transactions_hash,
            created_at = EXCLUDED.created_at,
            transactions_committed = EXCLUDED.transactions_committed,
            transactions_rejected = EXCLUDED.transactions_rejected
    `;
    await getPool().query(sql, [
        b.height,
        hexToBytea(b.hash),
        hexToBytea(b.prev_hash),
        hexToBytea(b.transactions_hash),
        b.created_at,
        b.transactions_committed | 0,
        b.transactions_rejected | 0,
    ]);
}

async function listBlocks({ page = 1, perPage = 20 } = {}) {
    const offset = (Math.max(1, page) - 1) * perPage;
    const total = (await getPool().query('SELECT COUNT(*)::INT AS c FROM mn.blocks')).rows[0].c;
    const r = await getPool().query(
        `SELECT height, hash, prev_hash, transactions_hash, created_at,
                transactions_committed, transactions_rejected, indexed_at
         FROM mn.blocks
         ORDER BY height DESC
         LIMIT $1 OFFSET $2`,
        [perPage, offset]
    );
    return {
        page, per_page: perPage, total, total_pages: Math.max(1, Math.ceil(total / perPage)),
        items: r.rows.map(row => ({
            height: Number(row.height),
            hash: byteaToHex(row.hash),
            prev_hash: byteaToHex(row.prev_hash),
            transactions_hash: byteaToHex(row.transactions_hash),
            created_at: row.created_at,
            transactions_committed: row.transactions_committed,
            transactions_rejected: row.transactions_rejected,
            indexed_at: row.indexed_at,
        })),
    };
}

// ------------------------------------------------------------
// transactions
// ------------------------------------------------------------

async function upsertTransaction(tx) {
    const sql = `
        INSERT INTO mn.transactions
            (hash, block_height, authority, created_at, executable_kind, status)
        VALUES ($1, $2, $3, $4, $5, $6)
        ON CONFLICT (hash) DO UPDATE SET
            block_height = EXCLUDED.block_height,
            authority = EXCLUDED.authority,
            created_at = EXCLUDED.created_at,
            executable_kind = EXCLUDED.executable_kind,
            status = EXCLUDED.status
    `;
    await getPool().query(sql, [
        hexToBytea(tx.hash),
        tx.block,
        tx.authority,
        tx.created_at,
        tx.executable,
        tx.status,
    ]);
}

async function updateTransactionMetadata(hash, meta) {
    await getPool().query(
        `UPDATE mn.transactions
         SET sora_v2_claim_tx_hash = $2,
             sora_nexus_claim_recipient = $3,
             fee_sponsor = $4
         WHERE hash = $1`,
        [hexToBytea(hash), meta.sora_v2_claim_tx_hash || null, meta.sora_nexus_claim_recipient || null, meta.fee_sponsor || null]
    );
}

// Look up the v2 burn extrinsic in the same Postgres (sm schema indexed by
// the v2 sorametrics indexer). Returns { block, signer } or null. The hash
// stored in metadata has 0x prefix while sm.live_extrinsics uses '0x...'
// strings as well — we accept both.
async function lookupV2BurnExtrinsic(hash) {
    if (!hash) return null;
    const candidates = [hash, hash.startsWith('0x') ? hash.slice(2) : '0x' + hash];
    const r = await getPool().query(
        `SELECT block, signer
         FROM sm.live_extrinsics
         WHERE hash = ANY($1::text[])
         LIMIT 1`,
        [candidates]
    );
    return r.rows[0] || null;
}

async function updateTransactionV2Side(mnHash, v2Block, v2Signer) {
    await getPool().query(
        `UPDATE mn.transactions
         SET sora_v2_block = $2, sora_v2_signer = $3
         WHERE hash = $1`,
        [hexToBytea(mnHash), v2Block != null ? Number(v2Block) : null, v2Signer || null]
    );
}

async function listClaimsMissingV2Resolution(limit = 50) {
    const r = await getPool().query(
        `SELECT hash, sora_v2_claim_tx_hash
         FROM mn.transactions
         WHERE sora_v2_claim_tx_hash IS NOT NULL
           AND sora_v2_signer IS NULL
         ORDER BY created_at DESC
         LIMIT $1`,
        [limit]
    );
    return r.rows.map(row => ({
        mn_hash: byteaToHex(row.hash),
        v2_hash: row.sora_v2_claim_tx_hash,
    }));
}

async function listClaimsToEnrich(limit = 100) {
    // Txs we haven't checked yet (no metadata fields populated). The check is
    // imprecise — a tx with no claim metadata will keep appearing here unless
    // we also mark it "checked". For Minamoto's small volume right now this
    // is fine; tighten later with a dedicated checked_at column if needed.
    const r = await getPool().query(
        `SELECT hash FROM mn.transactions
         WHERE sora_v2_claim_tx_hash IS NULL
           AND status = 'Committed'
         ORDER BY created_at DESC
         LIMIT $1`,
        [limit]
    );
    return r.rows.map(x => byteaToHex(x.hash));
}

// Aggregate wallet stats — equivalent to v2's /wallet/info/:addr but built
// on top of mn.* tables. Covers: first/last tx timestamps, tx counts and
// success rate, days active, top ISI kinds for this authority, domains
// owned, asset definitions registered, and cross-chain claim flag.
async function getWalletInfo(addr) {
    if (!addr) return null;
    const pool = getPool();

    // ---- Outgoing tx stats (this account is the authority) ----
    const txStats = (await pool.query(`
        SELECT
            COUNT(*)::INT AS tx_count,
            SUM(CASE WHEN status = 'Committed' THEN 1 ELSE 0 END)::INT AS tx_committed,
            MIN(created_at) AS first_tx_at,
            MAX(created_at) AS last_tx_at,
            COUNT(DISTINCT DATE(created_at))::INT AS days_active
        FROM mn.transactions
        WHERE authority = $1
    `, [addr])).rows[0];

    // ---- Incoming activity (this account is the destination of a Transfer
    // or Mint ISI) — picks up wallets that only RECEIVE, e.g. cross-chain
    // claim recipients. Two payload shapes:
    //   Transfer.value.destination  = <acc_id>             (no '#')
    //   Mint.value.destination      = <def_id>#<acc_id>    (asset path)
    const incoming = (await pool.query(`
        SELECT
            COUNT(*) FILTER (WHERE kind = 'Transfer')::INT AS in_transfers,
            COUNT(*) FILTER (WHERE kind = 'Mint')::INT     AS in_mints,
            MIN(created_at) AS in_first_at,
            MAX(created_at) AS in_last_at,
            COUNT(DISTINCT DATE(created_at))::INT AS in_days
        FROM mn.instructions
        WHERE (kind = 'Transfer' AND payload->'value'->>'destination' = $1)
           OR (kind = 'Mint'     AND payload->'value'->>'destination' LIKE '%#' || $1)
    `, [addr])).rows[0];

    // ---- Outgoing transfers stats (authority + kind=Transfer) ----
    const outTransfers = (await pool.query(`
        SELECT
            COUNT(*)::INT AS count,
            COALESCE(SUM((payload->'value'->>'object')::NUMERIC), 0)::TEXT AS volume,
            COALESCE(MAX((payload->'value'->>'object')::NUMERIC), 0)::TEXT AS max,
            COALESCE(AVG((payload->'value'->>'object')::NUMERIC), 0)::TEXT AS avg
        FROM mn.instructions
        WHERE authority = $1 AND kind = 'Transfer'
    `, [addr])).rows[0];

    // ---- Top tokens this wallet has interacted with (in + out, per asset
    // definition). For Transfer we read the def from the source path; for
    // Mint we read it from the destination path.
    const topTokens = (await pool.query(`
        WITH involved AS (
            SELECT SPLIT_PART(payload->'value'->>'source', '#', 1)::TEXT AS asset_token,
                   COALESCE((payload->'value'->>'object')::NUMERIC, 0)  AS amount
            FROM mn.instructions
            WHERE kind = 'Transfer'
              AND (authority = $1 OR payload->'value'->>'destination' = $1)
            UNION ALL
            SELECT SPLIT_PART(payload->'value'->>'destination', '#', 1)::TEXT AS asset_token,
                   COALESCE((payload->'value'->>'object')::NUMERIC, 0)        AS amount
            FROM mn.instructions
            WHERE kind = 'Mint' AND payload->'value'->>'destination' LIKE '%#' || $1
        )
        SELECT i.asset_token,
               ad.alias,
               ad.name,
               COUNT(*)::INT       AS trades,
               SUM(i.amount)::TEXT AS volume
        FROM involved i
        LEFT JOIN mn.asset_definitions ad ON ad.id = i.asset_token
        WHERE i.asset_token IS NOT NULL AND i.asset_token <> ''
        GROUP BY i.asset_token, ad.alias, ad.name
        ORDER BY trades DESC, volume DESC NULLS LAST
        LIMIT 10
    `, [addr])).rows;

    // ---- Top ISI kinds emitted by this account ----
    const kinds = (await pool.query(`
        SELECT kind, COUNT(*)::INT AS count
        FROM mn.instructions
        WHERE authority = $1
        GROUP BY kind
        ORDER BY count DESC
        LIMIT 10
    `, [addr])).rows;

    // ---- Domains owned ----
    const domains = (await pool.query(`
        SELECT id, accounts_count, assets_count, nfts_count, updated_at
        FROM mn.domains
        WHERE owned_by = $1
        ORDER BY id
    `, [addr])).rows;

    // ---- Asset definitions created ----
    const assetDefs = (await pool.query(`
        SELECT id, alias, name, total_quantity::text AS total_quantity, confidential_mode
        FROM mn.asset_definitions
        WHERE owned_by = $1
        ORDER BY name NULLS LAST
    `, [addr])).rows;

    // ---- Cross-chain claim recipient status ----
    const cc = (await pool.query(`
        SELECT COUNT(*)::INT AS claims_received,
               COALESCE((
                   SELECT SUM((i.payload->'value'->>'object')::NUMERIC)
                   FROM mn.transactions t
                   JOIN mn.instructions i ON i.transaction_hash = t.hash
                   WHERE t.sora_nexus_claim_recipient = $1 AND i.kind = 'Mint'
               ), 0)::TEXT AS xor_claimed
        FROM mn.transactions
        WHERE sora_nexus_claim_recipient = $1
    `, [addr])).rows[0];

    // ---- Assets currently held (balance > 0) ----
    const assetsHeld = (await pool.query(`
        SELECT COUNT(*)::INT AS c
        FROM mn.assets
        WHERE account_id = $1 AND value > 0
    `, [addr])).rows[0].c;

    // ---- Derived metrics ----
    const txCount      = txStats.tx_count || 0;
    const txCommitted  = txStats.tx_committed || 0;
    const inTransfers  = incoming.in_transfers || 0;
    const inMints      = incoming.in_mints || 0;
    const incomingTotal= inTransfers + inMints;
    const uniqueTokens = topTokens.length;

    // Combined first/last activity across outgoing + incoming
    const dates = [
        txStats.first_tx_at, txStats.last_tx_at,
        incoming.in_first_at, incoming.in_last_at,
    ].filter(Boolean).map(d => +new Date(d));
    const firstAt = dates.length ? new Date(Math.min(...dates)).toISOString() : null;
    const lastAt  = dates.length ? new Date(Math.max(...dates)).toISOString() : null;

    // Total XOR-equivalent volume — pick the row matching xor#universal alias
    // (Iroha 3 has no DEX/USD prices yet, so we denominate the headline
    // metric in XOR; other tokens still appear in the top-tokens table).
    const xorRow = topTokens.find(r => (r.alias === 'xor#universal') || (r.name || '').toLowerCase() === 'xor');
    const xorVolume = xorRow ? xorRow.volume : '0';

    // Whale score — three components 0-100 averaged. Since we only count XOR
    // for volume (no price feed) the bar is conservative; frequency uses all
    // ISI activity (out + in) so receiver-only wallets are not zero;
    // diversity counts unique tokens touched.
    const totalActivity = txCount + incomingTotal;
    const xorVolumeNum  = parseFloat(xorVolume) || 0;
    const clamp = (x) => Math.max(0, Math.min(100, x));
    const volumeScore    = clamp(Math.log10(xorVolumeNum + 1) * 12);
    const frequencyScore = clamp(Math.log10(totalActivity + 1) * 25);
    const diversityScore = clamp(uniqueTokens * 12);
    const whaleTotal     = Math.round((volumeScore + frequencyScore + diversityScore) / 3);
    const whaleClass     = whaleTotal >= 80 ? 'WHALE'
                         : whaleTotal >= 60 ? 'ORCA'
                         : whaleTotal >= 40 ? 'DOLPHIN'
                         : whaleTotal >= 20 ? 'FISH'
                         :                    'SHRIMP';

    return {
        // overall window
        first_at:        firstAt,
        last_at:         lastAt,
        first_tx_at:     txStats.first_tx_at,
        last_tx_at:      txStats.last_tx_at,
        // outgoing
        tx_count:        txCount,
        tx_committed:    txCommitted,
        success_rate:    txCount > 0 ? (txCommitted / txCount) : null,
        days_active:     Math.max(txStats.days_active || 0, incoming.in_days || 0),
        // incoming
        incoming: {
            transfers:   inTransfers,
            mints:       inMints,
            total:       incomingTotal,
            first_at:    incoming.in_first_at,
            last_at:     incoming.in_last_at,
        },
        // movements (TRANSFERS section in v2 layout)
        transfers: {
            out_count:   parseInt(outTransfers.count, 10) || 0,
            in_count:    inTransfers,
            volume_xor:  xorVolume,
            max_xor:     outTransfers.max,
            avg_xor:     outTransfers.avg,
        },
        unique_tokens:   uniqueTokens,
        top_tokens:      topTokens.map(r => ({
            asset_id:    r.asset_token,
            alias:       r.alias,
            name:        r.name,
            trades:      r.trades,
            volume:      r.volume,
        })),
        // existing collections
        assets_held:        assetsHeld,
        instruction_kinds:  kinds,
        domains_owned:      domains,
        asset_defs_created: assetDefs,
        cross_chain: {
            claims_received: cc.claims_received,
            xor_claimed:     cc.xor_claimed,
        },
        whale_score: {
            total:     whaleTotal,
            class:     whaleClass,
            volume:    Math.round(volumeScore),
            frequency: Math.round(frequencyScore),
            diversity: Math.round(diversityScore),
        },
    };
}

// Full XOR mint history — every Mint ISI targeting XOR, categorised by
// source (genesis premint at block 1 vs cross-chain claim with v2 link vs
// other). Returns total + per-source breakdown + chronological timeline so
// the operator can see exactly where every XOR came from and when.
//
// Iroha 3 has no native "supply event" so we reconstruct from instructions:
//   - block_height = 1                                   → genesis_premint
//   - tx.sora_v2_claim_tx_hash IS NOT NULL               → cross_chain_claim
//   - else                                                → other_mint
async function getXorMintHistory() {
    const pool = getPool();
    // Resolve XOR's asset definition id once (avoid hardcoding the hash).
    const xorRow = (await pool.query(`
        SELECT id FROM mn.asset_definitions
        WHERE alias = 'xor#universal' OR LOWER(name) = 'xor'
        LIMIT 1
    `)).rows[0];
    if (!xorRow) {
        return { total_raw: '0', by_source: {}, timeline: [], xor_asset_id: null };
    }
    const xorId = xorRow.id;
    // Category per row, plus the recipient account extracted from the Mint
    // payload's destination (`<asset_def_id>#<account_id>` for Mint).
    const rows = (await pool.query(`
        SELECT
            CASE
                WHEN i.block_height = 1 THEN 'genesis_premint'
                WHEN t.sora_v2_claim_tx_hash IS NOT NULL THEN 'cross_chain_claim'
                ELSE 'other_mint'
            END AS source,
            i.block_height,
            i.created_at,
            (i.payload->'value'->>'object')::TEXT AS amount_raw,
            i.authority AS minter,
            SUBSTRING(i.payload->'value'->>'destination' FROM POSITION('#' IN i.payload->'value'->>'destination') + 1) AS recipient,
            encode(i.transaction_hash, 'hex') AS tx_hash,
            t.sora_v2_claim_tx_hash AS v2_burn_tx,
            t.sora_v2_block AS v2_block,
            t.sora_v2_signer AS v2_signer
        FROM mn.instructions i
        JOIN mn.transactions t ON t.hash = i.transaction_hash
        WHERE i.kind = 'Mint'
          AND i.payload->'value'->>'destination' LIKE $1 || '#%'
        ORDER BY i.created_at ASC, i.block_height ASC
    `, [xorId])).rows;

    // CRITICAL: Iroha 3 stores XOR balances as Numeric rationals **already
    // in user units** (e.g. "5", "0.745", "20") — NOT 1e18-scaled raw like
    // SORA v2 substrate. The bridge converts when claiming. So we sum as
    // decimal Numeric, not BigInt — using string arithmetic via NUMERIC SQL
    // would be safer but JS Number suffices given Minamoto's scale (<100
    // XOR total today). Document this in xor_storage_note for clients.
    const bySource = {};
    let total = 0;
    for (const r of rows) {
        const amt = Number(r.amount_raw || '0');
        total += amt;
        if (!bySource[r.source]) {
            bySource[r.source] = { count: 0, amount: 0, recipients: new Set(), first_at: r.created_at, last_at: r.created_at };
        }
        const s = bySource[r.source];
        s.count += 1;
        s.amount += amt;
        if (r.recipient) s.recipients.add(r.recipient);
        s.last_at = r.created_at; // rows are ASC-sorted; last assignment wins
    }
    const summary = {};
    for (const [k, v] of Object.entries(bySource)) {
        summary[k] = {
            count:      v.count,
            amount:     v.amount,
            recipients: v.recipients.size,
            first_at:   v.first_at,
            last_at:    v.last_at,
        };
    }
    return {
        total_xor:     total,
        total_mints:   rows.length,
        xor_asset_id:  xorId,
        xor_storage_note: 'Amounts are already in XOR units (no 18-decimal scaling). Iroha 3 Numeric is arbitrary-precision rational; the bridge converts v2 raw 1e18 to Minamoto Numeric units when minting.',
        by_source:     summary,
        timeline: rows.map(r => ({
            source:     r.source,
            block:      Number(r.block_height),
            at:         r.created_at,
            amount:     Number(r.amount_raw),
            minter:     r.minter,
            recipient:  r.recipient,
            tx_hash:    r.tx_hash,
            v2_burn_tx: r.v2_burn_tx,
            v2_block:   r.v2_block != null ? Number(r.v2_block) : null,
            v2_signer:  r.v2_signer,
        })),
    };
}

async function listClaims({ page = 1, perPage = 50 } = {}) {
    const offset = (Math.max(1, page) - 1) * perPage;
    const total = (await getPool().query(
        `SELECT COUNT(*)::INT AS c FROM mn.transactions WHERE sora_v2_claim_tx_hash IS NOT NULL`
    )).rows[0].c;
    const r = await getPool().query(
        `SELECT t.hash, t.block_height, t.authority, t.created_at, t.status,
                t.sora_v2_claim_tx_hash, t.sora_nexus_claim_recipient, t.fee_sponsor,
                t.sora_v2_block, t.sora_v2_signer,
                /* Sum the Mint instructions emitted by this tx, treated as the
                   claimed XOR amount. Wallets receive XOR via Mint per the
                   genesis runtime; payload.value.object is a stringified number. */
                COALESCE((
                    SELECT SUM((i.payload->'value'->>'object')::NUMERIC)
                    FROM mn.instructions i
                    WHERE i.transaction_hash = t.hash AND i.kind = 'Mint'
                ), 0) AS claimed_amount
         FROM mn.transactions t
         WHERE t.sora_v2_claim_tx_hash IS NOT NULL
         ORDER BY t.created_at DESC
         LIMIT $1 OFFSET $2`,
        [perPage, offset]
    );
    return {
        page, per_page: perPage, total, total_pages: Math.max(1, Math.ceil(total / perPage)),
        items: r.rows.map(row => ({
            // Minamoto side
            mn_block: Number(row.block_height),
            mn_tx_hash: byteaToHex(row.hash),
            mn_recipient: row.sora_nexus_claim_recipient,
            mn_authority: row.authority,
            // SORA v2 side
            v2_block: row.sora_v2_block != null ? Number(row.sora_v2_block) : null,
            v2_tx_hash: row.sora_v2_claim_tx_hash,
            v2_signer: row.sora_v2_signer,
            // Common
            fee_sponsor: row.fee_sponsor,
            created_at: row.created_at,
            status: row.status,
            claimed_amount: row.claimed_amount != null ? row.claimed_amount.toString() : '0',
        })),
    };
}

async function getCrossChainStats() {
    const r = await getPool().query(`
        SELECT
            (SELECT COUNT(*)::INT FROM mn.transactions WHERE sora_v2_claim_tx_hash IS NOT NULL) AS total_claims,
            (SELECT COUNT(DISTINCT sora_nexus_claim_recipient)::INT FROM mn.transactions WHERE sora_nexus_claim_recipient IS NOT NULL) AS unique_recipients,
            (SELECT COALESCE(SUM((i.payload->'value'->>'object')::NUMERIC), 0)
             FROM mn.transactions t
             JOIN mn.instructions i ON i.transaction_hash = t.hash
             WHERE t.sora_v2_claim_tx_hash IS NOT NULL AND i.kind = 'Mint')::TEXT AS total_xor_claimed,
            (SELECT MIN(created_at) FROM mn.transactions WHERE sora_v2_claim_tx_hash IS NOT NULL) AS first_claim_at,
            (SELECT MAX(created_at) FROM mn.transactions WHERE sora_v2_claim_tx_hash IS NOT NULL) AS last_claim_at
    `);
    return r.rows[0];
}

async function getCrossChainTimeseries(hours = 168) {
    const r = await getPool().query(
        `SELECT date_trunc('hour', t.created_at) AS bucket,
                COUNT(*)::INT AS claims,
                COALESCE(SUM((i.payload->'value'->>'object')::NUMERIC), 0)::TEXT AS xor_claimed
         FROM mn.transactions t
         LEFT JOIN mn.instructions i ON i.transaction_hash = t.hash AND i.kind = 'Mint'
         WHERE t.sora_v2_claim_tx_hash IS NOT NULL
           AND t.created_at > NOW() - ($1 || ' hours')::INTERVAL
         GROUP BY bucket
         ORDER BY bucket ASC`,
        [String(hours)]
    );
    return r.rows.map(row => ({
        bucket: row.bucket,
        claims: row.claims,
        xor_claimed: row.xor_claimed,
    }));
}

async function listTransactions({ page = 1, perPage = 20, status = null, block = null, authority = null } = {}) {
    const offset = (Math.max(1, page) - 1) * perPage;
    const params = [];
    const conds = [];
    if (status)    { params.push(status);    conds.push(`status = $${params.length}`); }
    if (block != null) { params.push(block); conds.push(`block_height = $${params.length}`); }
    if (authority) { params.push(authority); conds.push(`authority = $${params.length}`); }
    const where = conds.length ? `WHERE ${conds.join(' AND ')}` : '';
    const totalSql = `SELECT COUNT(*)::INT AS c FROM mn.transactions ${where}`;
    const total = (await getPool().query(totalSql, params)).rows[0].c;
    params.push(perPage); params.push(offset);
    const r = await getPool().query(
        `SELECT hash, block_height, authority, created_at, executable_kind, status, indexed_at
         FROM mn.transactions
         ${where}
         ORDER BY created_at DESC
         LIMIT $${params.length - 1} OFFSET $${params.length}`,
        params
    );
    return {
        page, per_page: perPage, total, total_pages: Math.max(1, Math.ceil(total / perPage)),
        items: r.rows.map(row => ({
            hash: byteaToHex(row.hash),
            block: Number(row.block_height),
            authority: row.authority,
            created_at: row.created_at,
            executable: row.executable_kind,
            status: row.status,
            indexed_at: row.indexed_at,
        })),
    };
}

// ------------------------------------------------------------
// accounts
// ------------------------------------------------------------

async function upsertAccount(a) {
    const sql = `
        INSERT INTO mn.accounts
            (id, network_prefix, has_primary_alias,
             primary_alias, primary_alias_dataspace, primary_alias_domain, primary_alias_name,
             multisig_quorum, multisig_signatories_count, metadata, last_seen_at)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, NOW())
        ON CONFLICT (id) DO UPDATE SET
            network_prefix = EXCLUDED.network_prefix,
            has_primary_alias = EXCLUDED.has_primary_alias,
            primary_alias = EXCLUDED.primary_alias,
            primary_alias_dataspace = EXCLUDED.primary_alias_dataspace,
            primary_alias_domain = EXCLUDED.primary_alias_domain,
            primary_alias_name = EXCLUDED.primary_alias_name,
            multisig_quorum = EXCLUDED.multisig_quorum,
            multisig_signatories_count = EXCLUDED.multisig_signatories_count,
            metadata = EXCLUDED.metadata,
            last_seen_at = NOW()
    `;
    await getPool().query(sql, [
        a.id,
        a.network_prefix == null ? 753 : a.network_prefix,
        !!a.has_primary_alias,
        a.primary_alias || null,
        a.primary_alias_dataspace || null,
        a.primary_alias_domain || null,
        a.primary_alias_name || null,
        a.multisig_quorum == null ? null : a.multisig_quorum,
        a.multisig_signatories_count == null ? null : a.multisig_signatories_count,
        a.metadata || {},
    ]);
}

async function listAccounts({ page = 1, perPage = 50 } = {}) {
    const offset = (Math.max(1, page) - 1) * perPage;
    const total = (await getPool().query('SELECT COUNT(*)::INT AS c FROM mn.accounts')).rows[0].c;
    const r = await getPool().query(
        `SELECT id, network_prefix, has_primary_alias,
                primary_alias, primary_alias_dataspace, primary_alias_domain, primary_alias_name,
                multisig_quorum, multisig_signatories_count, metadata, first_seen_at, last_seen_at
         FROM mn.accounts
         ORDER BY last_seen_at DESC
         LIMIT $1 OFFSET $2`,
        [perPage, offset]
    );
    return {
        page, per_page: perPage, total, total_pages: Math.max(1, Math.ceil(total / perPage)),
        items: r.rows,
    };
}

// ------------------------------------------------------------
// domains
// ------------------------------------------------------------

async function upsertDomain(d) {
    const sql = `
        INSERT INTO mn.domains
            (id, owned_by, accounts_count, assets_count, nfts_count, metadata, updated_at)
        VALUES ($1, $2, $3, $4, $5, $6, NOW())
        ON CONFLICT (id) DO UPDATE SET
            owned_by = EXCLUDED.owned_by,
            accounts_count = EXCLUDED.accounts_count,
            assets_count = EXCLUDED.assets_count,
            nfts_count = EXCLUDED.nfts_count,
            metadata = EXCLUDED.metadata,
            updated_at = NOW()
    `;
    await getPool().query(sql, [
        d.id,
        d.owned_by,
        d.accounts | 0,
        d.assets | 0,
        d.nfts | 0,
        d.metadata || {},
    ]);
}

async function listDomains() {
    const r = await getPool().query(
        `SELECT id, owned_by, accounts_count, assets_count, nfts_count, metadata, updated_at
         FROM mn.domains ORDER BY id`
    );
    return r.rows;
}

// ------------------------------------------------------------
// assets
// ------------------------------------------------------------

async function upsertAsset(a) {
    // Asset depends on account row. Best-effort: create the account stub if missing.
    await getPool().query(
        `INSERT INTO mn.accounts (id) VALUES ($1) ON CONFLICT (id) DO NOTHING`,
        [a.account_id]
    );
    const sql = `
        INSERT INTO mn.assets (definition_id, account_id, value, updated_at)
        VALUES ($1, $2, $3, NOW())
        ON CONFLICT (definition_id, account_id) DO UPDATE SET
            value = EXCLUDED.value,
            updated_at = NOW()
    `;
    await getPool().query(sql, [a.definition_id, a.account_id, a.value]);
}

async function listAssets({ page = 1, perPage = 50 } = {}) {
    const offset = (Math.max(1, page) - 1) * perPage;
    const total = (await getPool().query('SELECT COUNT(*)::INT AS c FROM mn.assets')).rows[0].c;
    const r = await getPool().query(
        `SELECT a.definition_id, a.account_id, a.value, a.updated_at,
                d.alias, d.name
         FROM mn.assets a
         LEFT JOIN mn.asset_definitions d ON d.id = a.definition_id
         ORDER BY a.updated_at DESC
         LIMIT $1 OFFSET $2`,
        [perPage, offset]
    );
    return {
        page, per_page: perPage, total, total_pages: Math.max(1, Math.ceil(total / perPage)),
        items: r.rows.map(row => ({ ...row, value: row.value.toString() })),
    };
}

// ------------------------------------------------------------
// asset_definitions (catalogue of asset types)
// ------------------------------------------------------------

async function upsertAssetDefinition(d) {
    const sql = `
        INSERT INTO mn.asset_definitions
            (id, alias, name, description, owned_by, mintable,
             confidential_mode, balance_scope_policy, total_quantity, metadata, updated_at)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, NOW())
        ON CONFLICT (id) DO UPDATE SET
            alias = EXCLUDED.alias,
            name = EXCLUDED.name,
            description = EXCLUDED.description,
            owned_by = EXCLUDED.owned_by,
            mintable = EXCLUDED.mintable,
            confidential_mode = EXCLUDED.confidential_mode,
            balance_scope_policy = EXCLUDED.balance_scope_policy,
            total_quantity = EXCLUDED.total_quantity,
            metadata = EXCLUDED.metadata,
            updated_at = NOW()
    `;
    await getPool().query(sql, [
        d.id,
        d.alias || null,
        d.name || null,
        d.description || null,
        d.owned_by,
        d.mintable || null,
        d.confidential_policy && d.confidential_policy.mode ? d.confidential_policy.mode : null,
        d.balance_scope_policy || null,
        d.total_quantity != null ? String(d.total_quantity) : null,
        d.metadata || {},
    ]);
}

async function listAssetDefinitions() {
    const r = await getPool().query(
        `SELECT d.id, d.alias, d.name, d.description, d.owned_by, d.mintable,
                d.confidential_mode, d.balance_scope_policy, d.total_quantity::text AS total_quantity,
                d.metadata, d.updated_at,
                COALESCE((SELECT COUNT(*)::INT FROM mn.assets a WHERE a.definition_id = d.id), 0) AS holders,
                COALESCE((SELECT SUM(a.value) FROM mn.assets a WHERE a.definition_id = d.id), 0)::text AS held_supply
         FROM mn.asset_definitions d
         ORDER BY (CASE WHEN d.alias = 'xor#universal' THEN 0
                        WHEN d.name ILIKE 'xor' THEN 0
                        ELSE 1 END), d.name`
    );
    return r.rows;
}

async function getAssetHolders(needle) {
    // Resolve definition_id from alias OR id OR name (case-insensitive).
    const def = await getPool().query(
        `SELECT id, alias, name, total_quantity::text AS total_quantity,
                confidential_mode, mintable, owned_by
         FROM mn.asset_definitions
         WHERE alias = $1 OR id = $1 OR LOWER(name) = LOWER($1)
         LIMIT 1`, [needle]
    );
    if (def.rows.length === 0) return null;
    const d = def.rows[0];
    const r = await getPool().query(
        `SELECT account_id, value::text AS value, updated_at
         FROM mn.assets
         WHERE definition_id = $1
         ORDER BY value DESC`, [d.id]
    );
    const total = parseFloat(d.total_quantity || '0');
    return {
        definition: d,
        holders: r.rows.map(row => ({
            account_id: row.account_id,
            balance: row.value,
            // pct as 0..100, computed only when total > 0
            pct: total > 0 ? (parseFloat(row.value) / total) * 100 : null,
            updated_at: row.updated_at,
        })),
    };
}

async function getAssetSupply(needle) {
    // Resolves needle as: alias exact, name exact (case-insensitive), or id.
    const r = await getPool().query(
        `SELECT id, alias, name, total_quantity::text AS total_quantity,
                confidential_mode, mintable, owned_by, updated_at,
                COALESCE((SELECT COUNT(*)::INT FROM mn.assets a WHERE a.definition_id = d.id), 0) AS holders,
                COALESCE((SELECT SUM(a.value) FROM mn.assets a WHERE a.definition_id = d.id), 0)::text AS held_supply
         FROM mn.asset_definitions d
         WHERE d.alias = $1 OR d.id = $1 OR LOWER(d.name) = LOWER($1)
         LIMIT 1`,
        [needle]
    );
    return r.rows[0] || null;
}

// ------------------------------------------------------------
// peers
// ------------------------------------------------------------

async function upsertPeer(p) {
    const sql = `
        INSERT INTO mn.peers (multiaddr, public_key, ip_address, port, last_seen_at, is_active)
        VALUES ($1, $2, $3, $4, NOW(), TRUE)
        ON CONFLICT (multiaddr) DO UPDATE SET
            public_key = EXCLUDED.public_key,
            ip_address = EXCLUDED.ip_address,
            port = EXCLUDED.port,
            last_seen_at = NOW(),
            is_active = TRUE
    `;
    await getPool().query(sql, [p.multiaddr, p.public_key || null, p.ip_address || null, p.port || null]);
}

async function listPeers() {
    const r = await getPool().query(
        `SELECT multiaddr, public_key, ip_address, port, first_seen_at, last_seen_at, is_active
         FROM mn.peers ORDER BY is_active DESC, last_seen_at DESC`
    );
    return r.rows;
}

// Mark every peer NOT in `currentMultiaddrs` as inactive. Idempotent. The
// current ones get is_active=TRUE via the upsertPeer that runs alongside.
// Using NOT IN with an empty array is a special case in PG — guard it.
async function deactivateStalePeers(currentMultiaddrs) {
    if (!Array.isArray(currentMultiaddrs) || currentMultiaddrs.length === 0) {
        const r = await getPool().query(`UPDATE mn.peers SET is_active = FALSE WHERE is_active = TRUE`);
        return r.rowCount || 0;
    }
    const r = await getPool().query(
        `UPDATE mn.peers SET is_active = FALSE
         WHERE is_active = TRUE AND multiaddr <> ALL($1::text[])`,
        [currentMultiaddrs]
    );
    return r.rowCount || 0;
}

// ------------------------------------------------------------
// instructions (Iroha 3 ISIs — fine-grained activity feed)
// ------------------------------------------------------------

async function upsertInstruction(i) {
    const sql = `
        INSERT INTO mn.instructions
            (transaction_hash, instruction_index, block_height, authority,
             kind, payload, transaction_status, created_at)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
        ON CONFLICT (transaction_hash, instruction_index) DO UPDATE SET
            block_height = EXCLUDED.block_height,
            authority = EXCLUDED.authority,
            kind = EXCLUDED.kind,
            payload = EXCLUDED.payload,
            transaction_status = EXCLUDED.transaction_status,
            created_at = EXCLUDED.created_at
    `;
    await getPool().query(sql, [
        hexToBytea(i.transaction_hash),
        i.instruction_index | 0,
        i.block,
        i.authority,
        i.kind,
        i.payload || {},
        i.transaction_status,
        i.created_at,
    ]);
}

async function listInstructions({ page = 1, perPage = 50, kind = null, authority = null, block = null, txHash = null } = {}) {
    const offset = (Math.max(1, page) - 1) * perPage;
    const params = [];
    const conds = [];
    if (kind)      { params.push(kind);      conds.push(`kind = $${params.length}`); }
    if (authority) { params.push(authority); conds.push(`authority = $${params.length}`); }
    if (block != null) { params.push(block); conds.push(`block_height = $${params.length}`); }
    if (txHash)    { params.push(hexToBytea(txHash)); conds.push(`transaction_hash = $${params.length}`); }
    const where = conds.length ? `WHERE ${conds.join(' AND ')}` : '';
    const total = (await getPool().query(`SELECT COUNT(*)::INT AS c FROM mn.instructions ${where}`, params)).rows[0].c;
    params.push(perPage); params.push(offset);
    const r = await getPool().query(
        `SELECT transaction_hash, instruction_index, block_height, authority,
                kind, payload, transaction_status, created_at
         FROM mn.instructions ${where}
         ORDER BY created_at DESC, instruction_index ASC
         LIMIT $${params.length - 1} OFFSET $${params.length}`,
        params
    );
    return {
        page, per_page: perPage, total, total_pages: Math.max(1, Math.ceil(total / perPage)),
        items: r.rows.map(row => ({
            transaction_hash: byteaToHex(row.transaction_hash),
            instruction_index: row.instruction_index,
            block: Number(row.block_height),
            authority: row.authority,
            kind: row.kind,
            payload: row.payload,
            transaction_status: row.transaction_status,
            created_at: row.created_at,
        })),
    };
}

async function listInstructionKinds() {
    const r = await getPool().query(
        `SELECT kind, COUNT(*)::INT AS count
         FROM mn.instructions
         GROUP BY kind
         ORDER BY count DESC`
    );
    return r.rows;
}

// ------------------------------------------------------------
// metrics_snapshots (Prometheus time series)
// ------------------------------------------------------------

async function insertMetricsSamples(samples, ts) {
    if (!samples || samples.length === 0) return 0;
    const stamp = ts || new Date();
    // Bulk insert with multi-row VALUES — chunked to avoid hitting parameter limits.
    const CHUNK = 500;
    let total = 0;
    for (let i = 0; i < samples.length; i += CHUNK) {
        const chunk = samples.slice(i, i + CHUNK);
        const vals = [];
        const params = [];
        for (let j = 0; j < chunk.length; j++) {
            const s = chunk[j];
            const off = j * 4;
            vals.push(`($${off + 1}, $${off + 2}, $${off + 3}, $${off + 4})`);
            params.push(stamp, s.name, s.labels || {}, s.value);
        }
        const sql = `INSERT INTO mn.metrics_snapshots (ts, metric_name, labels, value) VALUES ${vals.join(',')}`;
        const r = await getPool().query(sql, params);
        total += r.rowCount || 0;
    }
    return total;
}

async function pruneMetricsSnapshots(retentionDays) {
    const r = await getPool().query(
        `DELETE FROM mn.metrics_snapshots WHERE ts < NOW() - ($1 || ' days')::INTERVAL`,
        [String(retentionDays)]
    );
    return r.rowCount || 0;
}

async function getLatestMetric(name) {
    const r = await getPool().query(
        `SELECT labels, value, ts
         FROM mn.metrics_snapshots
         WHERE metric_name = $1
         ORDER BY ts DESC LIMIT 50`,
        [name]
    );
    return r.rows;
}

async function getMetricSeries(name, hours = 24) {
    const r = await getPool().query(
        `SELECT ts, labels, value
         FROM mn.metrics_snapshots
         WHERE metric_name = $1 AND ts > NOW() - ($2 || ' hours')::INTERVAL
         ORDER BY ts ASC`,
        [name, String(hours)]
    );
    return r.rows;
}

// ------------------------------------------------------------
// indexer_state
// ------------------------------------------------------------

async function recordIndexerRun(name, status, lastValue, error) {
    const sql = `
        INSERT INTO mn.indexer_state (name, last_value, last_run_at, last_run_status, error_count, last_error)
        VALUES ($1, $2, NOW(), $3, $4, $5)
        ON CONFLICT (name) DO UPDATE SET
            last_value = EXCLUDED.last_value,
            last_run_at = NOW(),
            last_run_status = EXCLUDED.last_run_status,
            error_count = CASE WHEN EXCLUDED.last_run_status = 'ok' THEN 0
                               ELSE mn.indexer_state.error_count + 1 END,
            last_error = EXCLUDED.last_error
    `;
    await getPool().query(sql, [name, lastValue || {}, status, status === 'ok' ? 0 : 1, error || null]);
}

async function listIndexerState() {
    const r = await getPool().query(
        `SELECT name, last_run_at, last_run_status, error_count, last_error, last_value
         FROM mn.indexer_state ORDER BY name`
    );
    return r.rows;
}

// ------------------------------------------------------------
// Section-level stats — drive the KPI hero strips that mirror
// SoraMetrics v2's PageHeader + KpiGrid pattern. One query per
// section so each call is fast and cacheable independently.
// ------------------------------------------------------------

async function getAccountsStats() {
    const pool = getPool();
    const r = (await pool.query(`
        SELECT
            COUNT(*)::INT                                                            AS total,
            COUNT(*) FILTER (WHERE multisig_quorum IS NOT NULL)::INT                 AS multisig,
            COUNT(*) FILTER (WHERE has_primary_alias)::INT                           AS aliased,
            COUNT(*) FILTER (WHERE last_seen_at >= NOW() - INTERVAL '24 hours')::INT AS active_24h,
            COUNT(*) FILTER (WHERE first_seen_at >= NOW() - INTERVAL '7 days')::INT  AS new_7d,
            MAX(last_seen_at)                                                        AS last_activity
        FROM mn.accounts
    `)).rows[0];
    return {
        total:         r.total || 0,
        multisig:      r.multisig || 0,
        aliased:       r.aliased || 0,
        active_24h:    r.active_24h || 0,
        new_7d:        r.new_7d || 0,
        last_activity: r.last_activity,
    };
}

async function getFeeSponsorshipStats() {
    const pool = getPool();
    // Iroha 3 supports fee sponsorship — `fee_sponsor` (when not null)
    // names the account that pays the gas/fee for this tx instead of the
    // signer. Iroha 3's tx model does NOT expose the fee amount via the
    // public block payload, so we only surface counts/relationships.
    // Granted permissions: `CanUseFeeSponsor { sponsor: <account> }`.
    const r = (await pool.query(`
        SELECT
            COUNT(*)::INT                                                       AS total_tx,
            COUNT(*) FILTER (WHERE fee_sponsor IS NOT NULL)::INT                AS sponsored,
            COUNT(*) FILTER (WHERE fee_sponsor IS NOT NULL
                             AND created_at >= NOW() - INTERVAL '24 hours')::INT AS sponsored_24h,
            COUNT(DISTINCT fee_sponsor)::INT                                    AS distinct_sponsors,
            COUNT(DISTINCT authority) FILTER (WHERE fee_sponsor IS NOT NULL)::INT AS distinct_sponsored_signers
        FROM mn.transactions
    `)).rows[0];
    const top = (await pool.query(`
        SELECT fee_sponsor AS sponsor, COUNT(*)::INT AS count
        FROM mn.transactions
        WHERE fee_sponsor IS NOT NULL
        GROUP BY fee_sponsor
        ORDER BY count DESC
        LIMIT 10
    `)).rows;
    return {
        total_tx:                  r.total_tx || 0,
        sponsored:                 r.sponsored || 0,
        sponsored_24h:             r.sponsored_24h || 0,
        distinct_sponsors:         r.distinct_sponsors || 0,
        distinct_sponsored_signers: r.distinct_sponsored_signers || 0,
        top_sponsors:              top,
    };
}

async function getTransactionsStats() {
    const pool = getPool();
    const r = (await pool.query(`
        SELECT
            COUNT(*)::INT                                                       AS total,
            SUM(CASE WHEN status = 'Committed' THEN 1 ELSE 0 END)::INT          AS committed,
            COUNT(*) FILTER (WHERE created_at >= NOW() - INTERVAL '24 hours')::INT AS total_24h,
            SUM(CASE WHEN status='Committed' AND created_at >= NOW() - INTERVAL '24 hours' THEN 1 ELSE 0 END)::INT AS committed_24h,
            COUNT(DISTINCT authority)::INT                                       AS unique_signers
        FROM mn.transactions
    `)).rows[0];
    const top = (await pool.query(`
        SELECT authority, COUNT(*)::INT AS c
        FROM mn.transactions
        GROUP BY authority
        ORDER BY c DESC
        LIMIT 1
    `)).rows[0] || null;
    const total      = r.total || 0;
    const committed  = r.committed || 0;
    const total24h   = r.total_24h || 0;
    const committed24 = r.committed_24h || 0;
    return {
        total,
        committed,
        success_rate:    total > 0 ? committed / total : null,
        total_24h:       total24h,
        success_rate_24h:total24h > 0 ? committed24 / total24h : null,
        unique_signers:  r.unique_signers || 0,
        top_authority:   top ? { authority: top.authority, count: top.c } : null,
    };
}

async function getTransfersStats() {
    const pool = getPool();
    // Iroha 3 Transfer ISIs come in three variants: Asset (fungible token),
    // AssetDefinition (re-assigning an asset definition), Domain (renaming
    // ownership). Only the `Asset` variant has a numeric quantity in
    // `value.object`; the others put a hash there. We filter accordingly so
    // SUM(...)::NUMERIC never sees non-numeric input.
    const r = (await pool.query(`
        SELECT
            COUNT(*)::INT                                                       AS total,
            COUNT(*) FILTER (WHERE created_at >= NOW() - INTERVAL '24 hours')::INT AS total_24h,
            COUNT(DISTINCT authority)::INT                                       AS unique_senders,
            COUNT(DISTINCT payload->'value'->>'destination')::INT                AS unique_recipients,
            COALESCE(SUM((payload->'value'->>'object')::NUMERIC) FILTER (WHERE payload->>'variant' = 'Asset'), 0)::TEXT AS volume_total,
            COALESCE(SUM((payload->'value'->>'object')::NUMERIC) FILTER (WHERE payload->>'variant' = 'Asset' AND created_at >= NOW() - INTERVAL '24 hours'), 0)::TEXT AS volume_24h
        FROM mn.instructions
        WHERE kind = 'Transfer'
    `)).rows[0];
    const top = (await pool.query(`
        SELECT SPLIT_PART(payload->'value'->>'source', '#', 1) AS asset_id,
               COUNT(*)::INT AS c,
               COALESCE(SUM((payload->'value'->>'object')::NUMERIC), 0)::TEXT AS volume
        FROM mn.instructions
        WHERE kind = 'Transfer' AND payload->>'variant' = 'Asset'
        GROUP BY asset_id
        ORDER BY c DESC
        LIMIT 1
    `)).rows[0] || null;
    let topAsset = null;
    if (top) {
        const def = (await pool.query(
            `SELECT id, alias, name FROM mn.asset_definitions WHERE id = $1`, [top.asset_id]
        )).rows[0];
        topAsset = {
            asset_id: top.asset_id,
            alias:    def ? def.alias : null,
            name:     def ? def.name  : null,
            count:    top.c,
            volume:   top.volume,
        };
    }
    return {
        total:             r.total || 0,
        total_24h:         r.total_24h || 0,
        unique_senders:    r.unique_senders || 0,
        unique_recipients: r.unique_recipients || 0,
        volume_total:      r.volume_total || '0',
        volume_24h:        r.volume_24h || '0',
        top_asset:         topAsset,
    };
}

async function getDomainsStats() {
    const pool = getPool();
    const r = (await pool.query(`
        SELECT
            COUNT(*)::INT                                  AS total,
            COALESCE(SUM(accounts_count), 0)::INT          AS accounts_total,
            COALESCE(SUM(assets_count), 0)::INT            AS assets_total,
            COALESCE(SUM(nfts_count), 0)::INT              AS nfts_total
        FROM mn.domains
    `)).rows[0];
    const largestByAccounts = (await pool.query(`
        SELECT id, accounts_count FROM mn.domains
        ORDER BY accounts_count DESC NULLS LAST LIMIT 1
    `)).rows[0] || null;
    const largestByAssets = (await pool.query(`
        SELECT id, assets_count FROM mn.domains
        ORDER BY assets_count DESC NULLS LAST LIMIT 1
    `)).rows[0] || null;
    return {
        total:         r.total || 0,
        accounts_total:r.accounts_total || 0,
        assets_total:  r.assets_total || 0,
        nfts_total:    r.nfts_total || 0,
        largest_by_accounts: largestByAccounts,
        largest_by_assets:   largestByAssets,
    };
}

async function getAssetsStats() {
    const pool = getPool();
    const r = (await pool.query(`
        SELECT
            COUNT(*)::INT                                                          AS total,
            COUNT(*) FILTER (WHERE confidential_mode = 'Convertible')::INT         AS zk_convertible,
            COUNT(*) FILTER (WHERE mintable = 'Infinitely')::INT                   AS mintable_inf,
            COUNT(*) FILTER (WHERE mintable = 'Once')::INT                         AS mintable_once
        FROM mn.asset_definitions
    `)).rows[0];
    return {
        total:          r.total || 0,
        zk_convertible: r.zk_convertible || 0,
        mintable_inf:   r.mintable_inf || 0,
        mintable_once:  r.mintable_once || 0,
    };
}

async function getLaneStakingLifecycle() {
    const pool = getPool();
    // RegisterPublicLaneValidator + ActivatePublicLaneValidator ISIs encode
    // their validator pubkey as ASCII-hex inside a Norito blob (see
    // crates/iroha_data_model/src/isi/staking.rs). The pubkey always uses
    // the 'ea0130' algorithm prefix followed by 96 hex chars (ed25519 pubkey
    // total = 102 chars / 51 bytes). We extract with a regex against the
    // hex-encoded payload string instead of decoding Norito client-side.
    const rows = (await pool.query(`
        SELECT
            kind,
            authority,
            payload->'value'->>'encoded' AS encoded,
            transaction_hash,
            block_height,
            transaction_status,
            created_at
        FROM mn.instructions
        WHERE kind IN ('RegisterPublicLaneValidator', 'ActivatePublicLaneValidator')
        ORDER BY created_at ASC
    `)).rows;

    // Extract pubkey from each encoded Norito blob.
    //
    // The blob is hex-encoded BYTES. Inside those bytes, the validator
    // pubkey appears as an ASCII string (102 chars: `ea0130` algorithm
    // prefix + 96 char ed25519 body). So in the outer hex string each
    // pubkey char is 2 hex chars (the byte's hex representation):
    //   'e' = 0x65 → "65", 'a' = 0x61 → "61",
    //   '0' = 0x30 → "30", '1' = 0x31 → "31", '3' = 0x33 → "33".
    // → ASCII "ea0130" appears as `656130313330` in the outer hex.
    // Followed by 96 more pubkey chars = 192 outer hex chars.
    // We match 204 hex chars total (102 ASCII bytes) and ASCII-decode.
    const PK_RE = /(656130313330[0-9a-fA-F]{192})/i;
    const events = rows.map(row => {
        const m = row.encoded ? row.encoded.match(PK_RE) : null;
        const pubkey = m ? Buffer.from(m[1], 'hex').toString('ascii') : null;
        return {
            pubkey,
            kind:      row.kind,
            authority: row.authority,
            tx_hash:   byteaToHex(row.transaction_hash),
            block:     Number(row.block_height),
            status:    row.transaction_status,
            created_at: row.created_at,
        };
    }).filter(e => e.pubkey != null);

    // Group by validator pubkey.
    const byValidator = new Map();
    for (const e of events) {
        if (!byValidator.has(e.pubkey)) {
            byValidator.set(e.pubkey, {
                pubkey:        e.pubkey,
                registered_at: null,
                registered_tx: null,
                registered_block: null,
                activated_at:  null,
                activated_tx:  null,
                activated_block: null,
                status:        'unknown',
                events:        [],
            });
        }
        const v = byValidator.get(e.pubkey);
        v.events.push({ kind: e.kind, tx: e.tx_hash, block: e.block, at: e.created_at, status: e.status });
        if (e.kind === 'RegisterPublicLaneValidator' && e.status === 'Committed' && !v.registered_at) {
            v.registered_at = e.created_at;
            v.registered_tx = e.tx_hash;
            v.registered_block = e.block;
            v.status = 'pending_activation';
        }
        if (e.kind === 'ActivatePublicLaneValidator' && e.status === 'Committed') {
            v.activated_at = e.created_at;
            v.activated_tx = e.tx_hash;
            v.activated_block = e.block;
            v.status = 'active';
        }
    }

    return {
        total_events: events.length,
        validators: [...byValidator.values()].sort((a, b) => {
            const ta = a.registered_at ? +new Date(a.registered_at) : 0;
            const tb = b.registered_at ? +new Date(b.registered_at) : 0;
            return ta - tb;
        }),
    };
}

async function getPermissionsStats() {
    const pool = getPool();
    // Iroha 3 Grant ISI shape (verified in mn.instructions on 2026-04-28):
    //   payload = {
    //     value: { object: { name: "<perm_name>", payload: {...} },
    //              destination: "<account_id>" },
    //     variant: "PermissionToAccount" | "RoleToAccount"
    //   }
    // The 12 distinct permission names observed on Minamoto include:
    //   CanResolveAccountAlias (63), CanManageAccountAlias (63),
    //   CanRegisterTrigger, CanRegisterAccount, CanManageVerifyingKeys,
    //   CanMintAssetWithDefinition, CanUseFeeSponsor, CanRegisterDomain,
    //   CanEnactGovernance, CanSetParameters,
    //   CanPublishSpaceDirectoryManifest, CanManageSoracloud.
    const r = (await pool.query(`
        SELECT
            COUNT(*)::INT                                                       AS total,
            COUNT(*) FILTER (WHERE payload->>'variant' = 'PermissionToAccount')::INT AS perm_to_account,
            COUNT(*) FILTER (WHERE payload->>'variant' = 'RoleToAccount')::INT       AS role_to_account,
            COUNT(DISTINCT payload->'value'->'object'->>'name')::INT            AS distinct_perms,
            COUNT(DISTINCT authority)::INT                                      AS distinct_grantors,
            COUNT(DISTINCT payload->'value'->>'destination')::INT               AS distinct_recipients
        FROM mn.instructions
        WHERE kind = 'Grant'
    `)).rows[0];
    const top = (await pool.query(`
        SELECT
            payload->'value'->'object'->>'name' AS name,
            payload->>'variant'                 AS variant,
            COUNT(*)::INT                       AS count
        FROM mn.instructions
        WHERE kind = 'Grant'
        GROUP BY name, variant
        ORDER BY count DESC
        LIMIT 20
    `)).rows;
    return {
        total:               r.total || 0,
        perm_to_account:     r.perm_to_account || 0,
        role_to_account:     r.role_to_account || 0,
        distinct_perms:      r.distinct_perms || 0,
        distinct_grantors:   r.distinct_grantors || 0,
        distinct_recipients: r.distinct_recipients || 0,
        top_permissions:     top,
    };
}

async function listPermissionGrants({ page = 1, perPage = 50, permName = null, authority = null, destination = null, variant = null } = {}) {
    const pool = getPool();
    const offset = (Math.max(1, page) - 1) * perPage;
    const params = [];
    const conds = [`kind = 'Grant'`];
    if (permName)    { params.push(permName);    conds.push(`payload->'value'->'object'->>'name' = $${params.length}`); }
    if (authority)   { params.push(authority);   conds.push(`authority = $${params.length}`); }
    if (destination) { params.push(destination); conds.push(`payload->'value'->>'destination' = $${params.length}`); }
    if (variant)     { params.push(variant);     conds.push(`payload->>'variant' = $${params.length}`); }
    const where = `WHERE ${conds.join(' AND ')}`;
    const total = (await pool.query(
        `SELECT COUNT(*)::INT AS c FROM mn.instructions ${where}`, params
    )).rows[0].c;
    params.push(perPage); params.push(offset);
    const r = await pool.query(
        `SELECT transaction_hash, instruction_index, block_height, authority,
                payload, transaction_status, created_at
         FROM mn.instructions
         ${where}
         ORDER BY created_at DESC, instruction_index ASC
         LIMIT $${params.length - 1} OFFSET $${params.length}`,
        params
    );
    return {
        page, per_page: perPage, total, total_pages: Math.max(1, Math.ceil(total / perPage)),
        items: r.rows.map(row => ({
            transaction_hash: byteaToHex(row.transaction_hash),
            instruction_index: row.instruction_index,
            block: Number(row.block_height),
            authority: row.authority,
            // Hoist the most useful fields client-side so the table renderer
            // doesn't have to know payload internals:
            permission_name: row.payload && row.payload.value && row.payload.value.object && row.payload.value.object.name,
            permission_args: row.payload && row.payload.value && row.payload.value.object && row.payload.value.object.payload,
            destination:     row.payload && row.payload.value && row.payload.value.destination,
            variant:         row.payload && row.payload.variant,
            transaction_status: row.transaction_status,
            created_at: row.created_at,
        })),
    };
}

async function getBlocksStats() {
    const pool = getPool();
    // mn.blocks.transactions_committed is not backfilled by the indexer (the
    // Torii payload doesn't include it on every block), so we count from
    // mn.transactions directly. Blocks are considered "empty" when no tx
    // has block_height = blocks.height.
    const r = (await pool.query(`
        SELECT
            COUNT(*)::INT                                                          AS total,
            MAX(height)::INT                                                       AS latest_height,
            COUNT(*) FILTER (WHERE created_at >= NOW() - INTERVAL '24 hours')::INT AS blocks_24h
        FROM mn.blocks
    `)).rows[0];
    const txWindow = (await pool.query(`
        SELECT
            COUNT(*)::INT                                                          AS tx_24h,
            COUNT(DISTINCT block_height)::INT                                      AS blocks_with_tx_24h
        FROM mn.transactions
        WHERE created_at >= NOW() - INTERVAL '24 hours'
    `)).rows[0];
    const avgBlock = (await pool.query(`
        WITH last100 AS (
            SELECT created_at FROM mn.blocks ORDER BY height DESC LIMIT 100
        ),
        diffs AS (
            SELECT EXTRACT(EPOCH FROM (LEAD(created_at) OVER (ORDER BY created_at) - created_at)) * 1000 AS ms
            FROM last100
        )
        SELECT AVG(ms)::FLOAT AS avg_ms FROM diffs WHERE ms IS NOT NULL AND ms > 0
    `)).rows[0];
    const blocks24 = r.blocks_24h || 0;
    const blocksWithTx24 = txWindow.blocks_with_tx_24h || 0;
    return {
        total:          r.total || 0,
        latest_height:  r.latest_height,
        blocks_24h:     blocks24,
        tx_24h:         txWindow.tx_24h || 0,
        empty_24h:      Math.max(0, blocks24 - blocksWithTx24),
        avg_block_ms:   avgBlock && avgBlock.avg_ms ? Math.round(avgBlock.avg_ms) : null,
    };
}

module.exports = {
    getPool, applySchema, ping,
    upsertNetworkState, getNetworkState,
    upsertBlock, listBlocks, getBlocksStats,
    upsertTransaction, listTransactions, getWalletInfo, getTransactionsStats, getFeeSponsorshipStats,
    updateTransactionMetadata, listClaimsToEnrich,
    lookupV2BurnExtrinsic, updateTransactionV2Side, listClaimsMissingV2Resolution,
    listClaims, getCrossChainStats, getCrossChainTimeseries, getXorMintHistory,
    upsertAccount, listAccounts, getAccountsStats,
    upsertDomain, listDomains, getDomainsStats,
    upsertAsset, listAssets,
    upsertAssetDefinition, listAssetDefinitions, getAssetSupply, getAssetHolders, getAssetsStats,
    upsertPeer, listPeers, deactivateStalePeers,
    upsertInstruction, listInstructions, listInstructionKinds, getTransfersStats,
    getPermissionsStats, listPermissionGrants, getLaneStakingLifecycle,
    insertMetricsSamples, pruneMetricsSnapshots, getLatestMetric, getMetricSeries,
    recordIndexerRun, listIndexerState,
};
