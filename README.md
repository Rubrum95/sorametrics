# SoraMetrics

Production analytics dashboard for the SORA ecosystem. Indexes on-chain data in real time, serves it through a REST + WebSocket API, and renders it in a responsive single-page frontend.

**Live:** [sorametrics.org](https://www.sorametrics.org)

Dual-network: tracks both **SORA v2 (Substrate)** mainnet and **Minamoto (Iroha 3 / SORA Nexus)** mainnet from the same backend.

---

## Overview

A user landing on `sorametrics.org` is presented with two entry points:

| Path | Network | Purpose |
|------|---------|---------|
| `/` | — | Landing page with two entry buttons |
| `/sorav2` | SORA v2 (Substrate) | Full SPA: tokens, pools, swaps, transfers, governance, staking, portfolio, intelligence widgets |
| `/minamoto` | Minamoto (Iroha 3) | Iroha 3 explorer: blocks, accounts, ISIs, domains, assets, peers, lanes, governance, permissions, prometheus |

Both SPAs share the same Express server, the same PostgreSQL instance, and the same shell (PWA, i18n, dark/light theme, mobile drawer, deep-linking). They differ only in the data layer (`sm.*` vs `mn.*` schemas).

---

## Architecture

```
┌──────────────────────────────────────────────────────────────────────────────┐
│                              Browser (SPA)                                   │
│   /          → landing.html        (network selector)                        │
│   /sorav2    → index.html          + js/*.jsx              (Substrate UI)   │
│   /minamoto  → minamoto.html       + js/minamoto/*.jsx     (Iroha 3 UI)     │
│   sw.js + manifest.json (PWA, offline-capable)                               │
└──────────────────────────────────────────────────────────────────────────────┘
                          │ REST + Socket.IO
┌──────────────────────────────────────────────────────────────────────────────┐
│                         Node.js / Express server                             │
│                              (index.js, port 3000)                           │
│                                                                              │
│  ┌─── SORA v2 routes ─────────────────────────────┐  ┌─── Minamoto ──────┐  │
│  │ /tokens, /pools, /balance, /history/global/*,  │  │ /api/minamoto/*   │  │
│  │ /governance/*, /burns/*, /stats/*,             │  │ (mounted from     │  │
│  │ /chart/:symbol, /search, /polkamarkt/*,        │  │  minamoto/        │  │
│  │ /stats/fee-config, /stats/fee-burns-live, …    │  │  routes.js)       │  │
│  └────────────────────────────────────────────────┘  └───────────────────┘  │
│                                                                              │
│  ┌─── Adapters ────────────────────────────────────────────────────────────┐│
│  │ blockchain.js       Polkadot/SORA WS client (failover via WS_ENDPOINTS)││
│  │ db_pg.js            PostgreSQL backend (sm.* schema, async)            ││
│  │ db_better.js        Legacy SQLite backend (kept for reference)         ││
│  │ redis.js            Redis cache layer (price/identity TTL)             ││
│  │ eth_helper.js       Ethereum RPC (Hashi v2 bridge events)              ││
│  │ minamoto/torii_client.js   Iroha 3 Torii REST + Prometheus             ││
│  └─────────────────────────────────────────────────────────────────────────┘│
└──────────────────────────────────────────────────────────────────────────────┘
                          │
       ┌──────────────────┼──────────────────────────────────────────┐
       ▼                  ▼                                          ▼
┌─────────────┐    ┌─────────────────────┐                  ┌───────────────────┐
│ SORA Node   │    │ PostgreSQL 14       │                  │ Iroha 3 Torii     │
│ wss://...   │    │ (Docker)            │                  │ minamoto.sora.org │
│ + Hashi ETH │    │ ┌─ sm.*  (SORA v2) │                  │ + /metrics (Prom) │
│             │    │ ├─ mn.*  (Minamoto)│                  │                   │
│             │    │ └─ public.history_ │                  │                   │
│             │    │      element       │                  │                   │
│             │    │      (subsquid)    │                  │                   │
└─────────────┘    └─────────────────────┘                  └───────────────────┘
```

---

## Tech Stack

| Layer | Technology |
|-------|-----------|
| Runtime | Node.js 20+ |
| Web | Express 5, Socket.IO 4 |
| Frontend | Vanilla React + Babel-standalone (in-browser JSX), Chart.js, LightweightCharts |
| Database | PostgreSQL 14 (primary), SQLite (legacy/local fallback) |
| Cache | Redis 7 (ioredis) |
| SORA v2 RPC | `@polkadot/api`, `@sora-substrate/api` (WS, with failover) |
| Iroha 3 | Torii REST + Prometheus scraping |
| Process manager | PM2 |
| Security | Helmet, CORS, rate limiting (60 req/min/IP), input validation |
| PWA | Service Worker, manifest.json, offline cache |
| i18n | `data-i18n` attribute system, 14 languages |

---

## Project Structure

```
sorametrics/
├── index.js                     # Express + Socket.IO server (single entry point)
├── index.html                   # SORA v2 SPA shell
├── minamoto.html                # Minamoto SPA shell
├── landing.html                 # Network selector landing
├── styles.css                   # Shared v6 styles
├── sw.js                        # Service Worker (PWA)
├── manifest.json                # PWA manifest
├── favicon.svg, header-banner.jpg
│
├── blockchain.js                # SORA WS connection (with WS_ENDPOINTS failover)
├── config.js                    # Env-driven config
├── db_pg.js                     # PostgreSQL backend (sm.* schema)
├── db_better.js                 # Legacy SQLite backend (deprecated)
├── redis.js                     # Redis cache helpers
├── eth_helper.js                # Ethereum RPC for Hashi v2 bridge
│
├── ecosystem.config.js          # PM2 process definitions
├── package.json
│
├── js/                          # SORA v2 frontend (React via Babel-standalone)
│   ├── main.jsx                 # Entry point
│   ├── shell.jsx                # App shell (nav, drawer, search)
│   ├── routes.jsx               # Section routing
│   ├── i18n.jsx                 # Translations + helpers
│   ├── common.jsx               # Shared components (KpiGrid, MiniSpark, etc.)
│   ├── pulse.jsx                # Network Pulse (live KPIs)
│   ├── portfolio.jsx            # Multi-wallet portfolio
│   ├── swaps.jsx                # Swaps + transfers + bridges
│   ├── extrinsics.jsx           # Block explorer
│   ├── intelligence.jsx         # Bridge/burn analytics widgets
│   ├── tokens / pools / studio / tools / polkamarkt / xor_migration ...
│   └── minamoto/                # Minamoto frontend (12 modules)
│       ├── shell.jsx, routes.jsx, main.jsx, common.jsx, i18n.jsx
│       ├── overview.jsx
│       ├── blocks.jsx, transactions.jsx, accounts.jsx, domains.jsx, assets.jsx
│       ├── peers.jsx, prometheus.jsx
│       ├── governance.jsx, lanes.jsx, permissions.jsx, instructions.jsx
│       ├── crosschain.jsx, ecosystem.jsx, verbs.jsx, wallet.jsx
│
├── minamoto/                    # Minamoto backend module
│   ├── config.js                # Env-driven (TORII_BASE, intervals, retention)
│   ├── torii_client.js          # HTTP wrapper (retry, timeout, 5s cache)
│   ├── prom_parser.js           # Prometheus text → JSON (462 metrics)
│   ├── db.js                    # pg Pool + parametrized CRUD (mn.* schema)
│   ├── schema.sql               # Idempotent DDL (10 tables)
│   ├── routes.js                # Express router for /api/minamoto/*
│   └── indexer.js               # PM2 worker, 9 parallel polling jobs
│
├── backfiller.js                # SORA v2 historical block indexer
├── backfiller_orderbook.js      # Order book historical indexer
├── events_backfiller.js         # Events backfill helper
├── supply-filler.js             # Token supply snapshots
├── gap_filler_fees.js           # Fill gaps in fee history
├── backfill_fees.js             # Fee history backfill
├── fee_burns_indexer.js         # Live fee burn indexer (sm.fee_burns_live)
├── preimage_indexer.js          # Governance preimage indexer
│
└── scripts/                     # One-off setup / maintenance scripts
    ├── load_asset_registry.js   # Bootstrap sm.asset_registry (~962 assets)
    ├── backfill_price_history.js
    ├── backfill_xor_supply.js
    ├── create_materialized_views.sql
    ├── check_coverage.js
    ├── debug_dai.js, fix_dai.js
└── add_indices.sql              # Index additions for hot queries
```

---

## Database

### PostgreSQL schemas

| Schema | Source | Purpose |
|--------|--------|---------|
| `sm.*` | This codebase + `scripts/` | SORA v2 indexed data: live events, materialized views, asset registry, price history, fee burns, supply snapshots |
| `mn.*` | `minamoto/schema.sql` | Minamoto (Iroha 3) state: blocks, transactions, accounts, domains, assets, peers, prometheus snapshots |
| `public.history_element` | External `sora-subsquid` indexer | Raw SORA v2 events (canonical source). Reused via materialized views. |

Strict schema isolation: `sm.*` and `mn.*` never cross-reference each other. Each network owns its data.

### `sm.*` (SORA v2)

| Table / View | Role |
|--------------|------|
| `sm.asset_registry` | 962 tokens (id, symbol, decimals, logo) bootstrapped from sora-xor whitelist |
| `sm.price_history` | Hourly price buckets in USD (derived from DAI swap ratios) |
| `sm.identity_cache` | Cached on-chain identities |
| `sm.supply_snapshots` | Token supply over time |
| `sm.live_swaps`, `sm.live_transfers`, `sm.live_bridges` | Real-time events from WS subscription |
| `sm.fee_burns_live` | Per-block fee burn events |
| `sm.polkamarkt_markets` / `_trades` / `_claims` | Polkamarkt scaffolding (feature-detected) |
| `mv_swaps`, `mv_transfers`, `mv_bridges`, `mv_fees`, `mv_extrinsics`, `mv_liquidity`, `mv_orderbook` | 7 materialized views (~20 GB) for fast paginated queries |

### `mn.*` (Minamoto)

| Table | Role |
|-------|------|
| `mn.network_state` | Single-row latest network status |
| `mn.blocks` | Block height, hash (BYTEA(32)), timestamp, transaction count |
| `mn.transactions` | Transaction hash, authority, status, block height, fee_sponsor |
| `mn.accounts` | Account ID, signatories, signature check condition |
| `mn.assets`, `mn.asset_definitions` | Asset state (NUMERIC(78,0) for raw values) |
| `mn.domains` | Domain registry (`name.dataspace`) |
| `mn.peers` | Peer multiaddrs from `/peers` |
| `mn.metrics_snapshots` | Prometheus scrape history (rolling 30 days) |
| `mn.indexer_state` | Poll cursor + last-success timestamps per job |
| `mn.schema_version` | Manual migration tracking |

DDL is idempotent — safe to re-run. See [`minamoto/schema.sql`](minamoto/schema.sql).

---

## Getting Started

### Prerequisites

- Node.js 20+
- PostgreSQL 14 (the `sora-subsquid` Docker container is the typical deployment)
- Redis 7
- A reachable SORA v2 WebSocket endpoint (or run a local `sora2/substrate` node)
- Optional: a reachable Minamoto Torii endpoint (`https://minamoto.sora.org` is public)

### Install

```bash
git clone git@github.com:Rubrum95/sorametrics.git
cd sorametrics
npm install
```

### Configure

```bash
cp .env.example .env
# Edit .env: set PG_*, WS_ENDPOINTS, MINAMOTO_TORII, etc.
```

All environment variables are documented in [`.env.example`](.env.example). Key ones:

| Variable | Default | Notes |
|----------|---------|-------|
| `PORT` | 3000 | HTTP server port |
| `WS_ENDPOINTS` | `wss://ws.mof.sora.org,wss://mof2.sora.org` | Comma-separated, failover order |
| `PG_HOST` / `PG_PORT` / `PG_USER` / `PG_PASS` / `PG_DB` | — | PostgreSQL connection |
| `MINAMOTO_TORII` | `https://minamoto.sora.org` | Iroha 3 Torii base URL |
| `ETH_RPC_URL` | — | Required only for Hashi v2 bridge indexer |

### Initialize database

```bash
# Create sm.* schema (run psql or your migration tool)
psql -f scripts/create_materialized_views.sql

# Bootstrap asset registry
node scripts/load_asset_registry.js

# Optional: backfill price history (~246K hourly records)
node scripts/backfill_price_history.js

# Create mn.* schema (Minamoto)
psql -f minamoto/schema.sql
```

### Run

```bash
# Development (single process)
npm start

# Production (PM2)
pm2 start ecosystem.config.js
```

---

## PM2 Processes

| Name | Script | Memory | Role |
|------|--------|--------|------|
| `sorametrics-api` | `index.js` | 512 MB | Express API + WebSocket server (port 3000) |
| `sorametrics-backfill` | `backfiller.js` | 1 GB | SORA v2 historical block indexer |
| `sorametrics-preimage-indexer` | `preimage_indexer.js` | 512 MB | Governance preimage tracker |
| `sorametrics-minamoto-indexer` | `minamoto/indexer.js` | 512 MB | Minamoto Torii poller (9 parallel jobs) |

`fee_burns_indexer.js` is currently triggered manually or via cron — not registered in PM2.

---

## API Reference

### SORA v2 (Substrate)

#### Health & meta
- `GET /health`
- `GET /api/version`
- `GET /health/rpc-source` — current active WS endpoint, primary/secondary status

#### Tokens & pools
- `GET /tokens?page=&limit=` — paginated token list with prices
- `GET /pools?page=&limit=` — pools sorted by TVL
- `GET /pool/providers?base=&target=&page=` — pool LP providers
- `GET /pool/activity?base=&target=` — add/remove liquidity events
- `GET /holders/:assetId?page=` — holder distribution
- `GET /chart/:symbol?resolution=` — OHLCV candlestick data

#### Wallet
- `GET /balance/:address` — token balances
- `POST /balances` — batch balance query
- `GET /wallet/liquidity/:address` — LP positions
- `GET /wallet/staking/:address` — bonded amount, nominations
- `GET /identity/:address` — on-chain identity

#### History
- `GET /history/global/{swaps,transfers,bridges,extrinsics,orderbook,liquidity}`
- `GET /history/{swaps,transfers,bridges,extrinsics,orderbook}/:address`
- `GET /search?q=` — global search by hash, address, or block

#### Statistics
- `GET /stats/{overview,header,network,fees,trending-tokens,stablecoins,accumulation}`
- `GET /stats/network/trend`, `GET /stats/fees/trend`
- `GET /stats/fee-config` — on-chain xorFee weights
- `GET /stats/fee-burns-live` — live fee burn events
- `GET /currency-rates` — EUR/XOR exchange rates

#### Governance
- `GET /governance/{council,elections,motions,democracy,technical-committee}`

#### Burns & supply
- `GET /burns/supply/:symbol`, `/burns/supply-history/:symbol`
- `GET /burns/stats/:symbol`, `/burns/fee-flow`

#### Polkamarkt (feature-detected, awaits runtime ≥ 4.8.x)
- `GET /polkamarkt/{state,markets,market/:id,positions/:addr}`

### Minamoto (Iroha 3) — `/api/minamoto/*`

| Endpoint | Returns |
|----------|---------|
| `GET /api/minamoto/health` | Indexer + Torii reachability |
| `GET /api/minamoto/status` | Live status + last indexed snapshot |
| `GET /api/minamoto/network-state` | Latest single-row state |
| `GET /api/minamoto/blocks?page=&limit=` | Paginated blocks |
| `GET /api/minamoto/transactions?page=&limit=` | Paginated transactions |
| `GET /api/minamoto/accounts` | Account list |
| `GET /api/minamoto/accounts/:id/{assets,permissions,transactions}` | Per-account drill-in |
| `GET /api/minamoto/domains` | Domain registry |
| `GET /api/minamoto/assets` | Asset list with definitions |
| `GET /api/minamoto/peers` | Multiaddr peer list |
| `GET /api/minamoto/permissions/{stats,grants}` | Permission grant stats + paginated grants |
| `GET /api/minamoto/lane-staking/lifecycle` | Public lane staking events |
| `GET /api/minamoto/sumeragi/roles` | Empirical leader + voter roles |
| `GET /api/minamoto/transactions/fee-sponsorship` | Sponsored tx analytics |
| `GET /api/minamoto/prometheus/{metrics,snapshot}` | Prometheus passthrough |
| `GET /api/minamoto/indexer/state` | Per-job last-success cursors |

### Real-time (Socket.IO)

The server emits real-time events for SORA v2: `new_swap`, `new_transfer`, `new_bridge`, `new_block`, `new_extrinsic`. The client connects on the same origin.

---

## Indexer Architecture

### SORA v2

- **Live (push)**: WebSocket subscription in `index.js` writes to `sm.live_*` and broadcasts via Socket.IO.
- **Backfill (pull)**: `backfiller.js` walks block ranges newest→oldest, batch-inserts into materialized views. State persists in `backfill_state.json` (gitignored) so restarts resume.
- **Specialized**: `preimage_indexer.js` tracks governance preimages; `fee_burns_indexer.js` tracks per-block fee burn breakdown.

### Minamoto

`minamoto/indexer.js` runs 9 parallel polling jobs against the Torii REST API. Each job has its own interval (`MINAMOTO_POLL_*_MS` env vars):

| Job | Default interval | Source |
|-----|------------------|--------|
| `network_state` | 10s | `/v1/explorer/metrics` + `/status` |
| `blocks` | 15s | `/v1/explorer/blocks?page=...` |
| `transactions` | 15s | `/v1/explorer/transactions?page=...` |
| `accounts` | 60s | `/v1/explorer/accounts` |
| `domains` | 5min | `/v1/explorer/domains` |
| `assets` | 5min | `/v1/explorer/assets` |
| `peers` | 30s | `/peers` |
| `prometheus` | 15s | `/metrics` (parsed to JSON) |

Each job recovers FK violations silently and bumps `mn.indexer_state` on success.

---

## Security Notes

- All input validated; query params clamped (`limit ≤ 100`, `page ≥ 0`, `offset ≤ 500K`).
- Rate limiting: 60 req/min/IP via Helmet middleware.
- SSRF-protected image proxy for token logos.
- No secrets in code: all credentials via env vars (`.env` is gitignored).
- Music assets in `music/` are deployed separately via SCP (gitignored, large files).

---

## License

MIT
