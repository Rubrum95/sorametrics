# Changelog

All notable changes to SoraMetrics v33. Dates are the day the work landed on the `v33` branch.

## Unreleased

### 2026-09-11
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
