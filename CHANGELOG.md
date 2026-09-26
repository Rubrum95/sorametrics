# Changelog

All notable changes to SoraMetrics v33. Dates are the day the work landed on the `v33` branch.

## Unreleased

### 2026-09-26
- API: a swap is worth its cheaper priced leg (`sm.swap_usd`, 0 = unknown). Swap rows (history,
  Socket.IO, CSV, `/lookup/usd-value`) carry it in both `in.usd` and `out.usd`; network volume,
  trends, trending tokens, top pair, stablecoin swap volume, accumulation and wallet info sum it.
  One swap of 0.5 CERES → 5.17 DAI had been valued at 42 869.94 USD and inflated the 24 h volume
  tenfold.
- API: `/stats/overview.topPair`, the unordered pair with the most swap USD in the window.
- API: the back half of the swaps, transfers, bridges, fee-event and extrinsics listings is read
  from the oldest end when an index gives that order, so the last page costs what the first does
  (it was a 10.8 M-row OFFSET that timed out). With an estimated total, page numbers near the
  middle are approximate; the last page always holds the oldest rows.
- API: `?symbol=` (exact token, canonical asset id) on swaps and transfers, served by new
  `(asset, block_height, event_id)` indexes; `?network=` (exact) on bridges; `?timestamp=` resolves
  to a block bound and an estimated total.
- API: `next_before` carries the extrinsic id as tiebreak (`block-event-extrinsic`); legacy rows of
  one block share `event_id = 0`. Two-part cursors keep their meaning.
- API: DB sessions run with `statement_timeout = 30 s`; the listing routes use a second pool with
  `plan_cache_mode = force_custom_plan` (generic plans ignored the wallet index behind
  `($n IS NULL OR caller = $n)`; forcing it pool-wide made each price-hypertable lookup re-plan).
- API: `/stats/overview.network` gains `transferCount` and `bridgeVolume`; the Transfers and Bridges
  KPI cards read them instead of summing the visible page.
- API: extrinsics `?timestamp=` resolves to a block bound; the unfiltered estimate leaves out the
  `timestamp.set` rows the listing hides. MCP `recent_activity` sends `symbol=` for swaps/transfers.
- API: `/health/freshness` table thresholds sit above the longest quiet gaps seen on mainnet.
- frontend: date filters send `timestamp`, the swaps token dropdown sends `symbol`, swap KPIs read
  the real 24 h fields, and the invented `× 0.997` on the output leg is gone.
- API: deep pages without deep OFFSETs (`routes/deep.rs`). From offset 5 000, the block holding
  the page and the rows before it are found in one index-only statement (one snapshot, so a block
  committed meanwhile cannot make the skip negative), and the page query starts at that block:
  global swaps/transfers, one wallet's swaps/transfers/extrinsics, and wallet sets. A bot wallet's
  page 9 000 of 14 034 answers in ~1.5 s (it timed out).
- API: `?wallets=a,b,…` (≤ 50, deduplicated) on `/history/global/{swaps,transfers,bridges,
  extrinsics}` merges several wallets server-side (per-wallet index scans from the page's start
  block, exact total); it cannot be combined with other filters (400). Entries that are not SORA
  accounts are left out and listed in `invalid_wallets`; `resolved_wallets` maps each entry to
  the canonical address. No keyset cursor in this mode. The Portfolio history tabs page through it.
- API: a wallet's transfers = sent by it, or received from outside the queried set (disjoint, one
  row each), counted index-only on a new `(to_address, block_height) INCLUDE (from_address)` index
  (migration 0030): the 1.1 M-transfer wallet's count went from 9.8 s to 0.1 s and its middle
  pages no longer time out.
- API: a wallet's extrinsics count uses the signer index alone (`timestamp.set` is never signed by
  a wallet): the bot wallet's page 1 went from a 504 to ~0.3 s.
- DB: migration 0029 vacuums `sm.extrinsics`, `sm.swaps` and `sm.transfers` every 10 000 inserts,
  so wallet counts stay index-only (the default 20 % left 154 K pages off the visibility map).
- API: `/polkamarkt/positions/:addr` rows carry `collateral_asset`.
- frontend: any SORA account shown anywhere opens its wallet drawer — names and addresses in
  tables, drills, the live feed, governance, staking, holders, Polkamarkt, LP providers, and every
  `cn…` inside JSON args/events. Copying moved to a ⧉ icon. Foreign-chain accounts stay plain.
- frontend: the expanded extrinsic row shows the real args and events from
  `/history/extrinsic/:block/:index`; it showed hard-coded sample data.
- frontend: wallet drawer histories page (30 per page); Polkamarkt positions show shares and net
  paid in collateral units with the asset symbol (they were raw 1e18 values printed as USD).
- frontend: toasts sit above every modal; Liberland-side accounts inside JSON stay plain text.
- frontend (mobile): the topbar search is an icon button (the fixed "Buscar" label was cut to one
  letter and never translated); table rows no longer overflow their card by 20 px (`width: 100%`
  plus margins on swaps, transfers, bridges and preimages); the stacked-card labels of transfers,
  bridges, order book, pools and preimages use the i18n keys of their table headers.
- frontend: the referendum detail modal (Governance → Democracy → preimages → Ref #) renders in a
  portal; inside the blurred `.card` it was positioned and clipped relative to the card.
- frontend: every `backdrop-filter` also sets `-webkit-backdrop-filter` (drawer and drill backdrops,
  drill header, toasts, Pulse explorer overlay, Studio mini player, network menu): Safari before 18
  only reads the prefixed property.

### 2026-09-13
- deploy kit for Rubrum-01: `docker-compose.prod.yml` (TimescaleDB pg14, loopback, sized for the
  shared host), `env.rubrum.example`, PM2 ecosystem with opposite primary nodes for the ingest
  and the API, the final nginx server block, and `transfer_legacy.sh` — a resumable copy of every
  table the ETL reads (whole small tables, `asset_snapshot` DAY rows, `extrinsic_events` and
  `history_element` CALL rows in gzip-streamed block chunks) into a `legacy_copy` database,
  verified locally end to end with the ETL reconciling every table against it.
- docs: `cutover-runbook.md`, the ordered plan from the Contabo upgrade to the DNS switch.

### 2026-09-12
- API: `/holders` pre-warm — every `HOLDERS_PREWARM_SECS` (240 s, under the 5 min cache) one chain
  walk refreshes the scans of every asset requested in the last 6 h (at most 20), so the request
  path only hits the cache. `0` disables it.
- chain clients: storage maps are walked in pages of 1000 keys (subxt's default of 64 made the
  58k-entry `tokens.accounts` scan behind `/holders` cost ~1 800 round trips instead of ~120).
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
