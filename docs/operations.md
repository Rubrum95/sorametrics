# Operations

## Processes

| Process | Command | Notes |
|---------|---------|-------|
| SORA v2 indexer | `sorametrics-ingest --source substrate` | Applies migrations at start, resumes from `sm.indexer_state.substrate_live`, fills any gap before following the finalized stream. |
| Minamoto indexer | `sorametrics-ingest --source iroha` | Polls Torii; every job run is recorded in `mn.indexer_state` (`/api/minamoto/indexer/state`). |
| API | `sorametrics-api` | Stateless. Socket.IO on `/socket.io/*`. Needs `WS_ENDPOINTS` for chain-backed routes, `MINAMOTO_TORII` for `/api/minamoto/*` passthroughs, `STATIC_DIR` for the SPA files (`landing.html`, `index.html`, `minamoto.html`, `styles.css`, `sw.js`, `manifest.json`, `favicon.svg`, `header-banner.jpg`, `js/*.jsx`, `js/minamoto/*.jsx` — only those are served), `MUSIC_DIR` / `NEWS_DIR` for the media. Site analytics (`/analytics/*`) run in-process: `ANALYTICS_SALT`, `ANALYTICS_RAW_RETENTION_DAYS`. Compression and the security headers (CSP) are set by the API itself, as the Node did. |

Run them as separate supervised processes (PM2 or systemd) with the environment from
`.env.example`. Logs are `tracing` lines on stdout; set `RUST_LOG` per target.

## Build

```bash
SQLX_OFFLINE=true cargo build --release --workspace   # ~2 min from scratch
```

Release binaries: `sorametrics-api` 14.5 MB, `sorametrics-ingest` 10.0 MB, `sorametrics-ops`
9.8 MB (2026-09-11). The substrate ingest holds about 36 MB RSS while streaming (debug build,
10 h soak) and the release API about 29 MB after serving the Minamoto parity set plus the
heavy SORA routes; the architecture budgets are 80 MB for the ingest and 100 MB for the API.

## Supervision

`deploy/ecosystem.v33.config.js` defines the three PM2 processes (`sorametrics-v33-api`,
`sorametrics-v33-ingest-substrate`, `sorametrics-v33-ingest-iroha`) reading `.env` from the
install directory. `deploy/nginx-v33.conf` routes `/v33/*` to the Rust API for the parallel
monitoring phase and documents the final switch.

## Deploy

1. `cargo build --release --workspace` on the host (or ship the three binaries).
2. Put the new binaries next to the running ones; the ingest applies pending migrations itself,
   the API only reads. Migrations are additive, so an older API keeps working during the swap.
3. Restart the ingest first, then the API.
4. Check `GET /health` (`wsConnected: true`), `GET /health/freshness` (every table `healthy`),
   `GET /api/minamoto/indexer/state` (jobs `ok`).

Rollback = start the previous binaries. No migration needs reverting: none removes or renames
columns that older code reads.

## Backfill and gaps

The subscriber fills gaps on its own when it reconnects (it compares the cursor with the finalized
head). For a manual range:

```bash
sorametrics-ops backfill --from 27500000 --to 27510000 --concurrency 8 --rpc wss://mof2.sora.org
```

For the cutover the chain-first era starts where the Node's `mv_extrinsics` ends, **block
25 278 042**, not 25.8 M: the range 25.28 M–25.8 M spans runtime specs 119–129, so run it with
`--era-metadata` (and `--price-rpc` for USD values). That also covers the preimage events the
Node indexed in its SQLite (from block 25 610 792) and every cross-chain burn (from 25 868 450).

Idempotent: rows are UPSERTs keyed by `(block_height, extrinsic_id, event_id)`; the live cursor is
not moved. Throughput is bound by the RPC round trip (about five sequential calls per block) and
scales linearly with `--concurrency` until the node throttles: from a client 0.5 s away from
`mof2.sora.org`, 8 → 3.6 blocks/s, 32 → 15.6, 64 → 30, 128 → 60 (400 blocks, no failures). Against a node on the same host the same
work takes milliseconds per block. Raise `--concurrency` for bulk history only against a node you
operate. One block for diagnosis:

```bash
sorametrics-ops decode-block --height 27572842 --rpc wss://mof2.sora.org
```

Holes inside an already indexed range (a crashed backfill, a reconnect that never filled) are found
and repaired by height, using `sm.extrinsics` as the presence marker (every block carries at least
`timestamp.set`):

```bash
sorametrics-ops gap-fill --from 27500000 --to 27600000 --dry-run   # report count + ranges
sorametrics-ops gap-fill --from 27500000 --to 27600000             # decode the missing heights
```

The pinned runtime metadata covers the current spec only, so a block of an earlier runtime fails
to decode by default. `--era-metadata` (both commands) asks the node for the runtime version of
each block and, for a spec other than the pinned one, decodes it with the metadata the node served
at that block (fetched once per spec, about 5 s on `mof2`). This needs an archive node:

```bash
sorametrics-ops backfill --from 25284607 --to 25284607 --era-metadata --rpc wss://mof2.sora.org
```

Verified: the seven Liberland Hashi bridges of specs 119–129 (blocks 25284607 … 26413657) and a
spec-86 block (16250000: 54 events, 3 swaps, 6 fee burns) decode with the pinned static types.

### USD values of past blocks

An event older than the live window is valued from its hourly bucket in `ts.price_history`
(the Node's history plus what the live sampler records). A block filled outside that history
(a long gap, a backfill of a period the sampler did not cover) has no bucket, so `usd_value`
stays NULL — unless an archive RPC is configured, in which case the asset is quoted at that
block's state and the quote is folded into the hour's bucket:

```bash
PRICE_ARCHIVE_RPC=wss://mof2.sora.org            # ingest (.env): applies to gap fills
sorametrics-ops backfill --from … --to … --price-rpc wss://mof2.sora.org
```

One quote per asset and hour per process, misses cached too. Blocks before 24 943 612 are never
quoted: `Denomination::Denominator` reached its final value there (2026-02-20 21:54:30 UTC) and
earlier quotes are in the previous units, or `null` while the pools were migrated (the Node's own
history has no XOR price before February 2026). Verified on block 27609750 (swap XOR → 0x006a27…):
`usd_value` 0.827127 from an XOR quote of 4.137356 at that block.

## Runtime upgrades

`sorametrics-ops metadata-check` compares the pinned metadata with the node's, pallet by pallet
(hash of each pallet's metadata). Exit 0 when the 29 pinned pallets are identical, 2 when any
drifted or is missing; `--height N` compares against the metadata served at that block. CI runs
it as the non-blocking `metadata-compat` job. On drift: regenerate the metadata (see
`crates/substrate/metadata/README.md`), rebuild, run the decoder tests, and re-check the pallets
that changed against the decoders in `crates/substrate/src/`.

## Loading history from the Node database (ETL)

The legacy source must expose, besides the `sm.mv_*` views and the small tables, the two stores
the Node reads on demand for the extrinsic detail page: `public.history_element` (call args,
15.8 M rows, 14 GB) and `sm.extrinsic_events` (events, 234 M rows, 89 GB, dense from block
~10 M). Without them the copied extrinsics have no args and no events. To move them to another
host without a server-to-server credential and without a temp file on the full legacy disk,
stream them in chunks of 100 000 blocks (400 MB of text, 35 MB gzipped each; the whole events
table is about 9 GB on the wire):

```bash
# from a machine that can ssh to both hosts
for from in $(seq 10000000 100000 25300000); do to=$((from+99999))
  ssh legacy "docker exec sora_subsquid_db psql -U postgres -d squid -Atc \
    \"COPY (SELECT block_height, extrinsic_index, event_index, section, method, data \
           FROM sm.extrinsic_events WHERE block_height BETWEEN $from AND $to) TO STDOUT\" | gzip -1" \
  | ssh new "gunzip | docker exec -i sorametrics-v33-postgres psql -U sorametrics -d legacy_copy -c \
    'COPY sm.extrinsic_events (block_height, extrinsic_index, event_index, section, method, data) FROM STDIN'"
done
```

Each chunk is idempotent to repeat after a failure (truncate that block range first). Then run
the ETL on the new host with `LEGACY_DATABASE_URL` pointing at `legacy_copy`.

```bash
LEGACY_DATABASE_URL=postgres://... sorametrics-ops migrate-legacy \
  --tables asset_registry,swaps,transfers,bridges,fees,fee_burns,price_history,liquidity,extrinsics,order_book,val_staking_rewards,supply_snapshots,supply_history,news_episodes,polkamarkt_markets,polkamarkt_trades,polkamarkt_claims,polkamarkt_buybacks,polkamarkt_burns,site_daily,site_events,mn_blocks,mn_accounts,mn_transactions,mn_instructions,mn_domains,mn_asset_definitions,mn_assets,mn_peers,mn_network_state,mn_indexer_state,mn_metrics_snapshots
```

Read-only on the source. Resumable: the cursor per table lives in `sm.etl_state`. The run fails
if any table does not reconcile (counts and checksums per bucket); skipped source rows (NULL
required fields) are reported, never dropped silently. To restart one table from zero, truncate
it in the target and delete its `sm.etl_state` row.

## Minamoto chain reset

If Minamoto restarts from genesis, the ingest sees a height re-served with a different hash,
truncates `mn.blocks`, `mn.transactions` and `mn.instructions`, and backfills from Torii. World
tables (`mn.accounts`, `mn.domains`, `mn.assets`, `mn.asset_definitions`) are UPSERTed on the next
poll. Nothing to do by hand; the event is logged at `WARN`.

## Which build runs Minamoto

`mn.indexer_state` row `network_state` carries `last_value.git_commit_sha` from `/status`.
Resolve it with `git branch -a --contains <sha>` in a clone of `hyperledger-iroha/iroha`.

## Cutover checks

Before routing production traffic to v33: full ETL, then
`python3 scripts/parity_check.py --strict` against production must report every route in parity
(the only accepted differences are the ones documented per route).
