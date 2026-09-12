# Changelog

All notable changes to SoraMetrics v33. Dates are the day the work landed on the `v33` branch.

## Unreleased

### 2026-09-12
- API: on-chain identities are cached in `sm.identity_cache` as in the Node (memory 1 h → table
  24 h → chain in chunks of 50, written back; loaded into memory at boot); ETL copies the table.
- API: any valid SS58 prefix is accepted in address parameters and re-encoded with the SORA
  prefix (production answers foreign-prefix addresses; v33 returned 400).
- deploy: `health-check-v33.sh` for the three PM2 processes (cron every 15 min).
- API: the SPA files the Node served itself (`/` → `landing.html`, `/sorav2` → `index.html`,
  `/minamoto` → `minamoto.html`, the allow-listed root files, `/js/*.jsx`, `/js/minamoto/*.jsx`)
  are served from `STATIC_DIR` with the Node's allow-list, content types and `Cache-Control`;
  nothing else under the directory is reachable.
- API: site analytics ported from the Node (`POST /analytics/hit`, `GET /analytics/stats`,
  `GET /analytics/advanced`; `sm.site_events` / `sm.site_daily`; presence, 5 s batched flush,
  6-hourly rollup and pruning; visitor id = daily-salted hash, no IP stored). ETL copies both
  tables (`site_daily`, `site_events`) with reconciliation.
- API: `/api/sorav2/xor-migration/*` mounted as the second copy of the Minamoto router, with the
  same rate limits.
- API: response compression (br / gzip / deflate) and the Node's `helmet` headers, CSP byte for
  byte.
- ETL: `migrate-legacy --tables extrinsics` now carries the detail of every legacy extrinsic —
  `args` from `public.history_element` (the row whose id is the tx hash, kept as the exact
  `data::text` the Node serves) and `events` from `sm.extrinsic_events` (first 100 by
  `event_index`, minus `System.ExtrinsicSuccess/Failed`, as `[{s,m,d}]`), resolved exactly as
  the Node's `getExtrinsicDetail`; reconciliation compares per-bucket counts of rows with
  args and with events. Verified field by field against production on six extrinsics from
  2024 to 2026. Deviation: integers above 2^53 keep every digit (the Node rounds them through
  a JS double).

### 2026-09-11
- prices: events outside the live window with no hourly bucket are quoted at their own block
  (`liquidityProxy_quote` with the block hash) when an archive RPC is configured —
  `PRICE_ARCHIVE_RPC` for the ingest, `--price-rpc` / the same variable for `ops backfill`,
  `gap-fill` and `decode-block`. One quote per asset and hour, folded into that hour's bucket;
  never before block 24 943 612 (final XOR denomination). Unset: `usd_value` stays NULL as before.
- ops: `backfill` and `gap-fill` accept `--era-metadata`: a block of an earlier runtime is decoded
  with the metadata the node served at that block (one client per spec, cached; archive node
  required). Recovers the Hashi bridges of specs 119–129 and decodes back to spec 86 (block
  16.25M) with the same static decoders. The storage reads made while decoding (`XorFee`,
  `Polkamarkt`) are no longer rejected when the pallet hash differs from the pinned one.
- ops: `metadata-check [--height N]` compares the pinned runtime metadata with the node's pallet
  by pallet (exit 2 on drift); `make metadata-check`; CI job `metadata-compat` (non-blocking).
- binaries: only the working directory's `.env` is loaded (no parent-directory walk).
- substrate example: `era_decode` (decode a block with the metadata served at that height).
- deploy: PM2 ecosystem and nginx `/v33/` routing for the parallel-monitoring phase; release
  build sizes and memory recorded in the operations guide.
- ingest: lag monitor in the health loop (`SUBSTRATE_LAG_ALERT_BLOCKS`): warns when the live
  cursor falls behind the finalized head and exits for a supervisor restart when it also stops
  moving; per-block `lag_ms` in the logs.
- substrate: decoder tests on SCALE-encoded synthetic events decoded with the pinned metadata
  (swaps, transfers, fees, bridges, order book, VAL rewards) — no network needed.
- ingest: gap fill after a reconnect runs `SUBSTRATE_GAP_CONCURRENCY` blocks in parallel
  (cursor advances per completed chunk); measured 7× faster on a 4.7k-block gap.
- ops: `gap-fill --from --to [--dry-run]` finds heights without indexed extrinsics and decodes
  them through the backfill path.
- Docs: README, CONTRIBUTING, SECURITY, CHANGELOG, operations guide, issue and PR templates.
- API: per-client, per-route rate limiting with the Node's table (60 s fixed window, 429 body).

### 2026-09-10
- API: Socket.IO realtime feed (`new-block-stats`, `swaps-batch`, `transfers-batch`,
  `extrinsics-batch`, `orderbook-batch`) tailing the indexed tables; verified with the
  production client.
- Minamoto (Fase 2): `sorametrics-iroha` crate (Torii client typed against
  `hyperledger-iroha/iroha` `optimizations@cfa5e8ce77`), `mn.*` schema, `ingest --source iroha`
  with chain-reset detection, 50 `/api/minamoto/*` routes, ETL of the 11 `mn_*` tables,
  Torii and legacy-database mocks; parity 37/37 against production.
- Governance, Polkamarkt, explorer, tech-accounts and wallet-info routes; parity script
  (`scripts/parity_check.py`).

### 2026-09-07 / 2026-09-08
- Order book, staking (validators, network, rewards, live sampler), fee burns and MOF supply
  snapshots, CSV export, small routes (lookup, media, proxy-image).

### 2026-09-05
- Liquidity events, extrinsics with `toHuman`/`toJSON` rendering, pool providers, transfers
  reconciliation against production.

### 2026-08-10
- SS58 codec, legacy schema alignment, `sorametrics-ops migrate-legacy` with keyset resume and
  mandatory reconciliation.

### 2026-04-30
- Workspace bootstrap: crates, migrations, docker-compose, CI, substrate subscriber with
  gap-fill, first decoders (swaps, transfers, bridges), price sampler.
