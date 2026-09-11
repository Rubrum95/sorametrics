# Changelog

All notable changes to SoraMetrics v33. Dates are the day the work landed on the `v33` branch.

## Unreleased

### 2026-09-11
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
