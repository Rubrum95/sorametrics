'use strict';
// ============================================================
// minamoto/routes.js — Express router mounted at /api/minamoto
// All endpoints read-only. DB-backed where indexer fills the table,
// Torii-backed where data is unbounded or live (raw passthrough).
// ============================================================

const express = require('express');
const cfg = require('./config');
const db = require('./db');
const torii = require('./torii_client');
const prom = require('./prom_parser');

const router = express.Router();

// Tiny per-route rate limiter, scoped to this router (does not interfere
// with the parent app's limiter).
const _bucket = new Map();
function rateLimit(maxReqs, windowMs) {
    return (req, res, next) => {
        const key = (req.ip || 'unknown') + ':' + req.path;
        const now = Date.now();
        const e = _bucket.get(key);
        if (!e || now - e.start > windowMs) {
            _bucket.set(key, { start: now, count: 1 });
            return next();
        }
        e.count++;
        if (e.count > maxReqs) return res.status(429).json({ error: 'too many requests' });
        next();
    };
}

function clampPage(req, defaultSize = 20) {
    const page = Math.max(1, parseInt(req.query.page, 10) || 1);
    const perPage = Math.max(1, Math.min(cfg.MAX_PAGE_SIZE, parseInt(req.query.per_page, 10) || defaultSize));
    return { page, perPage };
}

function asyncHandler(fn) {
    return (req, res) => Promise.resolve(fn(req, res)).catch(err => {
        console.error('[minamoto.api]', req.path, err.message);
        res.status(err.status || 500).json({ error: err.message });
    });
}

// ------------------------------------------------------------
// Health & meta
// ------------------------------------------------------------

router.get('/health', rateLimit(60, 60_000), asyncHandler(async (_req, res) => {
    const ping = await db.ping();
    res.json({ ok: true, db: ping.db, torii: cfg.TORII_BASE });
}));

router.get('/torii/health', rateLimit(60, 60_000), asyncHandler(async (_req, res) => {
    const t = await torii.getHealth();
    res.json({ ok: true, message: t.trim() });
}));

router.get('/status', rateLimit(60, 60_000), asyncHandler(async (_req, res) => {
    const s = await torii.getStatus();
    res.json(s);
}));

// ------------------------------------------------------------
// Network state (DB cache, fast)
// ------------------------------------------------------------

router.get('/network-state', rateLimit(60, 60_000), asyncHandler(async (_req, res) => {
    const state = await db.getNetworkState();
    res.json({ state, source: 'db', updated_at: state ? state.updated_at : null });
}));

router.get('/network-state/live', rateLimit(30, 60_000), asyncHandler(async (_req, res) => {
    const m = await torii.getExplorerMetrics();
    res.json({ state: m, source: 'torii', updated_at: new Date().toISOString() });
}));

// ------------------------------------------------------------
// Blocks
// ------------------------------------------------------------

router.get('/blocks', rateLimit(60, 60_000), asyncHandler(async (req, res) => {
    const { page, perPage } = clampPage(req, 20);
    res.json(await db.listBlocks({ page, perPage }));
}));

router.get('/blocks/stats', rateLimit(60, 60_000), asyncHandler(async (_req, res) => {
    res.json(await db.getBlocksStats());
}));

// ------------------------------------------------------------
// Transactions
// ------------------------------------------------------------

router.get('/transactions', rateLimit(60, 60_000), asyncHandler(async (req, res) => {
    const { page, perPage } = clampPage(req, 20);
    const status = req.query.status ? String(req.query.status) : null;
    const block = req.query.block != null && req.query.block !== '' ? parseInt(req.query.block, 10) : null;
    const authority = req.query.authority ? String(req.query.authority) : null;
    res.json(await db.listTransactions({ page, perPage, status, block, authority }));
}));

router.get('/transactions/stats', rateLimit(60, 60_000), asyncHandler(async (_req, res) => {
    res.json(await db.getTransactionsStats());
}));

router.get('/transactions/fee-sponsorship', rateLimit(60, 60_000), asyncHandler(async (_req, res) => {
    res.json(await db.getFeeSponsorshipStats());
}));

// Aggregate stats for a wallet — drives the rich Info tab in the wallet
// drilldown (mirror of v2's /wallet/info/:addr).
router.get('/wallet/:addr/info', rateLimit(30, 60_000), asyncHandler(async (req, res) => {
    const info = await db.getWalletInfo(req.params.addr);
    if (!info) return res.status(404).json({ error: 'wallet not found' });
    res.json(info);
}));

// ------------------------------------------------------------
// Accounts
// ------------------------------------------------------------

router.get('/accounts', rateLimit(60, 60_000), asyncHandler(async (req, res) => {
    const { page, perPage } = clampPage(req, 50);
    res.json(await db.listAccounts({ page, perPage }));
}));

router.get('/accounts/stats', rateLimit(60, 60_000), asyncHandler(async (_req, res) => {
    res.json(await db.getAccountsStats());
}));

router.get('/accounts/:id/assets', rateLimit(60, 60_000), asyncHandler(async (req, res) => {
    const r = await torii.getAccountAssets(req.params.id);
    res.json(r);
}));

router.get('/accounts/:id/transactions', rateLimit(60, 60_000), asyncHandler(async (req, res) => {
    const r = await torii.getAccountTransactions(req.params.id);
    res.json(r);
}));

router.get('/accounts/:id/permissions', rateLimit(60, 60_000), asyncHandler(async (req, res) => {
    const r = await torii.getAccountPermissions(req.params.id);
    res.json(r);
}));

// ------------------------------------------------------------
// Domains
// ------------------------------------------------------------

router.get('/domains', rateLimit(60, 60_000), asyncHandler(async (_req, res) => {
    res.json({ items: await db.listDomains() });
}));

router.get('/domains/stats', rateLimit(60, 60_000), asyncHandler(async (_req, res) => {
    res.json(await db.getDomainsStats());
}));

// ------------------------------------------------------------
// Assets
// ------------------------------------------------------------

router.get('/assets', rateLimit(60, 60_000), asyncHandler(async (req, res) => {
    const { page, perPage } = clampPage(req, 50);
    res.json(await db.listAssets({ page, perPage }));
}));

// ------------------------------------------------------------
// Asset definitions (XOR, USD, etc. — the catalogue, not balances)
// ------------------------------------------------------------

router.get('/asset-definitions', rateLimit(60, 60_000), asyncHandler(async (_req, res) => {
    res.json({ items: await db.listAssetDefinitions() });
}));

router.get('/asset-definitions/stats', rateLimit(60, 60_000), asyncHandler(async (_req, res) => {
    res.json(await db.getAssetsStats());
}));

router.get('/asset/:idOrAlias', rateLimit(60, 60_000), asyncHandler(async (req, res) => {
    const found = await db.getAssetSupply(req.params.idOrAlias);
    if (!found) return res.status(404).json({ error: 'asset not found' });
    res.json(found);
}));

router.get('/asset/:idOrAlias/holders', rateLimit(60, 60_000), asyncHandler(async (req, res) => {
    const r = await db.getAssetHolders(req.params.idOrAlias);
    if (!r) return res.status(404).json({ error: 'asset not found' });
    res.json(r);
}));

// ------------------------------------------------------------
// Block + transaction detail (proxied to Torii — they have rich
// metadata that we don't store in the indexer, e.g. cross-chain
// claim hashes, signature, full executable payload).
// ------------------------------------------------------------

router.get('/block/:idOrHeight', rateLimit(60, 60_000), asyncHandler(async (req, res) => {
    res.json(await torii.getBlockByHeight(req.params.idOrHeight));
}));

router.get('/tx/:hash', rateLimit(60, 60_000), asyncHandler(async (req, res) => {
    res.json(await torii.getTransactionByHash(req.params.hash));
}));

// ------------------------------------------------------------
// Instructions (ISI feed) — DB-backed (rich filters by kind/authority/block/tx)
// ------------------------------------------------------------

router.get('/instructions', rateLimit(60, 60_000), asyncHandler(async (req, res) => {
    const { page, perPage } = clampPage(req, 50);
    const kind = req.query.kind ? String(req.query.kind) : null;
    const authority = req.query.authority ? String(req.query.authority) : null;
    const block = req.query.block != null && req.query.block !== '' ? parseInt(req.query.block, 10) : null;
    const txHash = req.query.tx ? String(req.query.tx) : null;
    res.json(await db.listInstructions({ page, perPage, kind, authority, block, txHash }));
}));

router.get('/instructions/kinds', rateLimit(60, 60_000), asyncHandler(async (_req, res) => {
    res.json({ items: await db.listInstructionKinds() });
}));

router.get('/transfers/stats', rateLimit(60, 60_000), asyncHandler(async (_req, res) => {
    res.json(await db.getTransfersStats());
}));

// Permissions / Grants — surface the 142+ Grant ISIs that drive Iroha 3's
// permission system. These were previously hidden inside the generic
// instructions list; now there's a dedicated section.
router.get('/permissions/stats', rateLimit(60, 60_000), asyncHandler(async (_req, res) => {
    res.json(await db.getPermissionsStats());
}));

// Public Lane staking lifecycle — derives validator status (pending_activation,
// active) by replaying RegisterPublicLaneValidator + ActivatePublicLaneValidator
// events from mn.instructions. Pubkeys are extracted from the Norito-encoded
// hex blob (regex on the ASCII-hex). When the API exposes structured payloads
// for these ISIs, swap to native parsing.
router.get('/lane-staking/lifecycle', rateLimit(60, 60_000), asyncHandler(async (_req, res) => {
    res.json(await db.getLaneStakingLifecycle());
}));

router.get('/permissions/grants', rateLimit(60, 60_000), asyncHandler(async (req, res) => {
    const { page, perPage } = clampPage(req, 50);
    const permName    = req.query.perm_name   ? String(req.query.perm_name)   : null;
    const authority   = req.query.authority   ? String(req.query.authority)   : null;
    const destination = req.query.destination ? String(req.query.destination) : null;
    const variant     = req.query.variant     ? String(req.query.variant)     : null;
    res.json(await db.listPermissionGrants({ page, perPage, permName, authority, destination, variant }));
}));

// ------------------------------------------------------------
// Telemetry / governance / kaigi (live passthroughs to Torii)
// ------------------------------------------------------------

router.get('/telemetry/peers-info', rateLimit(60, 60_000), asyncHandler(async (_req, res) => {
    res.json(await torii.getPeersInfo());
}));
router.get('/telemetry/propagation', rateLimit(60, 60_000), asyncHandler(async (_req, res) => {
    res.json(await torii.getPropagation());
}));
router.get('/telemetry/sumeragi', rateLimit(60, 60_000), asyncHandler(async (_req, res) => {
    res.json(await torii.getSumeragiTel());
}));

// Aggregate Sumeragi roles & validator-set view. Combines /peers (roster
// in pubkey@host:port form) + /v1/telemetry/peers-info (per-node
// connectivity + connected_peers list) + /v1/sumeragi/telemetry (VRF) +
// /status.sumeragi (leader_index / view / quorum) + Prometheus per-peer
// metrics (post counts, rbc backlog, lane validator counts) into a
// single payload the frontend can render as a roster + roles table.
//
// Note: Iroha 3 does NOT publish the canonical ordering of the validator
// set, so we expose `leader_index` as-is. The frontend can highlight
// "Leader: index N" without committing to a specific pubkey.
router.get('/sumeragi/roles', rateLimit(30, 60_000), asyncHandler(async (_req, res) => {
    const [status, peersList, peersInfo, sumeragiTel, promText, collectorsResp] = await Promise.all([
        torii.getStatus().catch(() => ({})),
        torii.getPeers().catch(() => []),
        torii.getPeersInfo().catch(() => []),
        torii.getSumeragiTel().catch(() => ({})),
        torii.getPrometheus().catch(() => ''),
        torii.getSumeragiCollectors().catch(() => null),
    ]);
    const sum = (status && status.sumeragi) || {};
    const promSamples = prom.parse(promText);

    // Index Prometheus per-peer metrics by pubkey for quick joins.
    const byPeer = new Map(); // pubkey → { post_total, rbc_queue }
    function ensure(pk) {
        if (!byPeer.has(pk)) byPeer.set(pk, { post_total: 0, rbc_queue: 0 });
        return byPeer.get(pk);
    }
    const lanes = []; // { lane, status, count }
    let laneStakeBondedTotal = 0;
    for (const s of promSamples) {
        if (s.name === 'sumeragi_post_to_peer_total' && s.labels && s.labels.peer) {
            ensure(s.labels.peer).post_total = Number(s.value) || 0;
        } else if (s.name === 'sumeragi_bg_post_queue_depth_by_peer' && s.labels && s.labels.peer) {
            ensure(s.labels.peer).rbc_queue = Number(s.value) || 0;
        } else if (s.name === 'nexus_public_lane_validator_total' && s.labels) {
            lanes.push({ lane: s.labels.lane, status: s.labels.status, count: Number(s.value) || 0 });
        } else if (s.name === 'nexus_public_lane_stake_bonded' && s.labels) {
            laneStakeBondedTotal += Number(s.value) || 0;
        }
    }

    // Build the full roster by cross-referencing `connected_peers` lists:
    // in a fully-connected mesh of N validators, each pubkey appears in
    // exactly N-1 nodes' connected_peers (every node except its own
    // owner). The union of all connected_peers therefore yields the full
    // set of N validator pubkeys, and each owner is identified by being
    // the only node whose own list omits its pubkey.
    //
    // We use peers-info (Torii URL) for host:port and /peers (P2P
    // address) only as a fallback for nodes whose Torii lacks a
    // public_key in config (most do, except where the operator opted in).

    const allPubkeysSet = new Set();
    for (const node of (Array.isArray(peersInfo) ? peersInfo : [])) {
        for (const pk of (node.connected_peers || [])) allPubkeysSet.add(pk);
        const ownPk = node.config && node.config.public_key;
        if (ownPk) allPubkeysSet.add(ownPk);
    }

    // Map pubkey → owner host via cross-reference deduction.
    const ownerByPubkey = new Map(); // pubkey → { host, port, torii_url, connected }
    for (const node of (Array.isArray(peersInfo) ? peersInfo : [])) {
        const seenByMe = new Set(node.connected_peers || []);
        const ownPk = node.config && node.config.public_key;
        // Candidate(s): pubkeys present in the union but NOT in my own
        // connected_peers — in a healthy mesh that's exactly 1 (me).
        const candidates = [...allPubkeysSet].filter(pk => !seenByMe.has(pk));
        const myPk = ownPk || (candidates.length === 1 ? candidates[0] : null);
        if (!myPk) continue;
        const u = (() => { try { return new URL(node.url); } catch (_) { return null; } })();
        ownerByPubkey.set(myPk, {
            host: u ? u.hostname : node.url,
            port: u && u.port ? Number(u.port) : null,
            torii_url: node.url,
            connected: !!node.connected,
            telemetry_unsupported: !!node.telemetry_unsupported,
            ownership: ownPk ? 'self_disclosed' : 'deduced',
        });
    }

    // P2P host:port from /peers (kept as a separate enrichment so the
    // operator can see both the Torii URL and the gossip endpoint).
    const p2pByPubkey = new Map(); // pubkey → { host, port }
    for (const entry of (Array.isArray(peersList) ? peersList : [])) {
        const at = String(entry).indexOf('@');
        if (at <= 0) continue;
        const pk = entry.slice(0, at);
        const hostPort = entry.slice(at + 1);
        const colon = hostPort.lastIndexOf(':');
        p2pByPubkey.set(pk, {
            host: colon > 0 ? hostPort.slice(0, colon) : hostPort,
            port: colon > 0 ? Number(hostPort.slice(colon + 1)) : null,
        });
    }

    // Final roster — one row per validator pubkey.
    const roster = [...allPubkeysSet].map(pk => {
        const owner = ownerByPubkey.get(pk) || {};
        const p2p   = p2pByPubkey.get(pk)   || {};
        const m     = byPeer.get(pk)        || {};
        return {
            pubkey:                pk,
            torii_host:            owner.host || null,
            torii_port:            owner.port || null,
            torii_url:             owner.torii_url || null,
            p2p_host:              p2p.host || null,
            p2p_port:              p2p.port || null,
            connected:             owner.connected != null ? owner.connected : null,
            telemetry_unsupported: !!owner.telemetry_unsupported,
            ownership:             owner.ownership || 'unknown',
            post_total:            m.post_total || 0,
            rbc_queue:             m.rbc_queue || 0,
        };
    }).sort((a, b) => a.pubkey.localeCompare(b.pubkey));

    // Lane summary — collapse {lane,status,count} entries into one row
    // per lane with active / pending counts side-by-side.
    const laneMap = {};
    for (const l of lanes) {
        if (!laneMap[l.lane]) laneMap[l.lane] = { lane: l.lane, active: 0, pending: 0 };
        if (l.status === 'active') laneMap[l.lane].active = l.count;
        else if (l.status === 'pending') laneMap[l.lane].pending = l.count;
    }
    const laneSummary = Object.values(laneMap).sort((a, b) => Number(a.lane) - Number(b.lane));

    // Resolve the canonical leader pubkey from the live PRF inputs.
    // Verified algorithm (hyperledger-iroha/iroha · crates/iroha_core/src/sumeragi/network_topology.rs):
    //
    //   leader_index_prf(seed, height, view) =
    //       u64_be(Blake2b512(seed || height.to_be_bytes() || view.to_be_bytes())[..8]) % N
    //
    // applied to the canonical roster sorted by PeerId. PeerId sorts by
    // public_key bytes which, since pubkeys are hex strings of equal
    // length, is equivalent to lexicographic sort of the hex string.
    //
    // Inputs we read from /status.sumeragi:
    //   - prf_epoch_seed: hex32 (zeros until VRF for epoch 0 finalises)
    //   - prf_height:     u64 (next block height being decided)
    //   - prf_view:       u64 (current view counter — bumps on view change)
    //
    // The `leader_index` exposed by Torii (also 0 in the payload above)
    // is the index AFTER the topology has been internally rotated so the
    // leader sits at position 0; it is therefore not directly useful for
    // mapping to a pubkey. We use the PRF formula instead.
    // Leader resolution — empirical method via /v1/sumeragi/collectors.
    //
    // The collectors endpoint exposes the post-rotation peers excluding the
    // leader (the leader sits at post-rotation position 0 by design and is
    // omitted from the collector list). Therefore: the canonical roster
    // member NOT present in collectors IS the current leader. This is
    // ground truth — no chain_id guessing, no PRF assumptions, no manual
    // formula reproduction. The frontend can always trust it.
    //
    // We retain the PRF formula (Blake2b-512 on canonical-sorted roster) as
    // an `expected_via_prf` cross-check; when `prf_epoch_seed` is the all-
    // zero placeholder reported by /status, the actual seed used internally
    // resolves through `latest_epoch_seed_from_world(world, chain_id)` and
    // ends in `chain_epoch_seed(chain_id) = Hash::new(chain_id_bytes)` —
    // which we cannot reproduce because Torii does not expose chain_id. So
    // a PRF mismatch isn't a bug; it just means we can't replay the same
    // pseudorandom source from outside the runtime. The collectors API
    // bypasses that entirely.
    const sortedRoster = [...roster].sort((a, b) =>
        Buffer.from(a.pubkey, 'hex').compare(Buffer.from(b.pubkey, 'hex'))
    );
    let leaderPubkey = null;
    let leaderHost   = null;
    let leaderPort   = null;
    let leaderRosterIdx = null;
    let leaderSource = 'unresolved';

    if (collectorsResp && Array.isArray(collectorsResp.collectors) && roster.length > 0) {
        const collectorPubkeys = new Set(collectorsResp.collectors.map(c => c.peer_id));
        const candidates = sortedRoster.filter(r => !collectorPubkeys.has(r.pubkey));
        if (candidates.length === 1) {
            leaderPubkey    = candidates[0].pubkey;
            leaderHost      = candidates[0].torii_host || candidates[0].p2p_host;
            leaderPort      = candidates[0].torii_port || candidates[0].p2p_port;
            leaderRosterIdx = sortedRoster.findIndex(r => r.pubkey === leaderPubkey);
            leaderSource    = 'sumeragi_collectors_complement';
        }
    }

    // Cross-check via PRF formula (won't match while seed is the placeholder
    // zeros reported by /status; serves as a sanity flag for when on-chain
    // governance sets a non-zero seed).
    let prfExpectedPubkey = null;
    if (sortedRoster.length > 0 && sum.prf_epoch_seed && sum.prf_height != null) {
        try {
            const seedHex = String(sum.prf_epoch_seed).replace(/^0x/, '');
            if (/^[0-9a-fA-F]{64}$/.test(seedHex)) {
                const seed = Buffer.from(seedHex, 'hex');
                const heightBuf = Buffer.alloc(8);
                heightBuf.writeBigUInt64BE(BigInt(sum.prf_height));
                const viewBuf = Buffer.alloc(8);
                viewBuf.writeBigUInt64BE(BigInt(sum.prf_view || 0));
                const digest = require('crypto')
                    .createHash('blake2b512')
                    .update(Buffer.concat([seed, heightBuf, viewBuf]))
                    .digest();
                const first8 = digest.readBigUInt64BE(0);
                const idx = Number(first8 % BigInt(sortedRoster.length));
                prfExpectedPubkey = sortedRoster[idx].pubkey;
            }
        } catch (_) { /* leave null */ }
    }

    res.json({
        mode_tag:                     sum.mode_tag || null,
        leader_index:                 sum.leader_index != null ? sum.leader_index : null,
        view:                         sum.commit_qc_view != null ? sum.commit_qc_view : null,
        commit_qc_height:             sum.commit_qc_height || null,
        validator_set_len:            sum.commit_qc_validator_set_len || roster.length,
        quorum:                       sum.commit_signatures_required || null,
        view_changes_total:           sum.view_change_install_total || 0,
        view_change_suggest_total:    sum.view_change_suggest_total || 0,
        gossip_fallback_total:        sum.gossip_fallback_total || 0,
        epoch_length_blocks:          sum.epoch_length_blocks || null,
        prf_height:                   sum.prf_height || null,
        prf_view:                     sum.prf_view || 0,
        prf_epoch_seed:               sum.prf_epoch_seed || null,
        vrf:                          sumeragiTel.vrf || null,
        availability:                 sumeragiTel.availability || null,
        rbc_pending:                  sumeragiTel.rbc_pending || null,
        roster,
        lanes:                        laneSummary,
        lane_stake_bonded_total:      laneStakeBondedTotal,
        leader_pubkey:                leaderPubkey,
        leader_host:                  leaderHost,
        leader_port:                  leaderPort,
        leader_roster_index:          leaderRosterIdx,
        leader_resolution: leaderPubkey
            ? {
                method: leaderSource,
                source: '/v1/sumeragi/collectors complement',
                description: 'Empirical: the canonical-sorted roster member absent from /v1/sumeragi/collectors IS the current leader (leader sits at post-rotation index 0 by design, excluded from collectors).',
                prf_cross_check: prfExpectedPubkey
                    ? {
                        expected_pubkey: prfExpectedPubkey,
                        matches_empirical: prfExpectedPubkey === leaderPubkey,
                        note: 'PRF formula (Blake2b512 on pubkey-sorted roster) replayed from outside the runtime. Mismatch is expected while /status reports prf_epoch_seed=zeros — the actual seed used internally is chain_epoch_seed(chain_id), which Torii does not expose.',
                        inputs_used: { seed: sum.prf_epoch_seed, height: sum.prf_height, view: sum.prf_view || 0 },
                      }
                    : null,
              }
            : null,
    });
}));
router.get('/gov/council', rateLimit(60, 60_000), asyncHandler(async (_req, res) => {
    res.json(await torii.getGovCouncil());
}));
router.get('/gov/unlocks', rateLimit(60, 60_000), asyncHandler(async (_req, res) => {
    res.json(await torii.getGovUnlocks());
}));
router.get('/kaigi/relays', rateLimit(60, 60_000), asyncHandler(async (_req, res) => {
    res.json(await torii.getKaigiRelays());
}));
router.get('/explorer/nfts', rateLimit(60, 60_000), asyncHandler(async (req, res) => {
    const { page, perPage } = clampPage(req, 50);
    res.json(await torii.getExplorerNfts(page, perPage));
}));
router.get('/explorer/rwas', rateLimit(60, 60_000), asyncHandler(async (req, res) => {
    const { page, perPage } = clampPage(req, 50);
    res.json(await torii.getExplorerRwas(page, perPage));
}));

// ------------------------------------------------------------
// Cross-chain XOR migration (SORA v2 burn → Minamoto claim)
// ------------------------------------------------------------

router.get('/cross-chain/stats', rateLimit(60, 60_000), asyncHandler(async (_req, res) => {
    res.json(await db.getCrossChainStats());
}));

router.get('/cross-chain/timeseries', rateLimit(60, 60_000), asyncHandler(async (req, res) => {
    const hours = Math.max(1, Math.min(720, parseInt(req.query.hours, 10) || 168));
    res.json({ hours, series: await db.getCrossChainTimeseries(hours) });
}));

router.get('/cross-chain/claims', rateLimit(60, 60_000), asyncHandler(async (req, res) => {
    const { page, perPage } = clampPage(req, 50);
    res.json(await db.listClaims({ page, perPage }));
}));

// Full XOR mint history with source attribution (genesis premint vs
// cross-chain claim vs other). Lets the user see exactly when each XOR
// was created and where it came from.
router.get('/cross-chain/mint-history', rateLimit(60, 60_000), asyncHandler(async (_req, res) => {
    res.json(await db.getXorMintHistory());
}));

// ------------------------------------------------------------
// Peers / Validators
// ------------------------------------------------------------

router.get('/peers', rateLimit(60, 60_000), asyncHandler(async (_req, res) => {
    res.json({ items: await db.listPeers() });
}));

// ------------------------------------------------------------
// Prometheus metrics
// ------------------------------------------------------------

router.get('/prometheus/raw', rateLimit(10, 60_000), asyncHandler(async (_req, res) => {
    const text = await torii.getPrometheus();
    res.type('text/plain').send(text);
}));

router.get('/prometheus/parsed', rateLimit(20, 60_000), asyncHandler(async (_req, res) => {
    const text = await torii.getPrometheus();
    const samples = prom.parse(text);
    res.json({ count: samples.length, samples });
}));

router.get('/prometheus/metric/:name', rateLimit(60, 60_000), asyncHandler(async (req, res) => {
    const hours = Math.max(1, Math.min(168, parseInt(req.query.hours, 10) || 24));
    const series = await db.getMetricSeries(req.params.name, hours);
    res.json({ name: req.params.name, hours, series });
}));

// ------------------------------------------------------------
// Indexer state (operator visibility)
// ------------------------------------------------------------

router.get('/indexer/state', rateLimit(30, 60_000), asyncHandler(async (_req, res) => {
    res.json({ items: await db.listIndexerState() });
}));

module.exports = router;
