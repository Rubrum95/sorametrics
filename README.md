# SoraMetrics

Production-grade analytics dashboard for the [SORA Network](https://sora.org). Indexes on-chain data in real time, serves it through a REST + WebSocket API, and renders it in a responsive single-page frontend.

**Live:** [sorametrics.org](https://www.sorametrics.org)

---

## Architecture

```
Browser (SPA)            Server                    Blockchain
┌──────────────┐    ┌──────────────────┐    ┌─────────────────────┐
│ index.html   │◄──►│ index.js         │◄──►│ SORA Substrate Node │
│ script.js    │ WS │ Express + Socket │ WS │ wss://ws.mof.sora   │
│ sw.js (PWA)  │    │ .IO              │    └─────────────────────┘
└──────────────┘    │                  │
                    │ db_better.js     │    ┌─────────────────────┐
                    │ SQLite (WAL)     │    │ backfiller.js       │
                    │  ├ database_30d  │◄───│ backfiller_orderbook│
                    │  └ database      │    │ (PM2 processes)     │
                    └──────────────────┘    └─────────────────────┘
```

| Layer | File(s) | Role |
|-------|---------|------|
| **Frontend** | `index.html`, `script.js`, `sw.js` | SPA with Chart.js, LightweightCharts, Socket.IO client, html2canvas. PWA-capable. |
| **API Server** | `index.js` | Express 5 REST API (52 endpoints) + Socket.IO real-time feed. |
| **Database** | `db_better.js` | `better-sqlite3` with dual-DB strategy: 30-day rolling (`database_30d.db`) + full history (`database.db`). WAL mode, prepared statement cache, 256 MB mmap. |
| **Indexers** | `backfiller.js`, `backfiller_orderbook.js` | Historical block processors. Batch-insert with transactions, resume via `backfill_state.json`. |
| **Blockchain** | `blockchain.js`, `config.js` | `@polkadot/api` + `@sora-substrate/api` connection layer with auto-reconnect. |

---

## Features

### Dashboard Sections

| Section | Description |
|---------|-------------|
| **Portfolio** | Multi-wallet net worth with donut chart, holdings table, LP summary, staking. Multi-currency (USD/EUR/XOR). |
| **Swaps** | Global swap history with filters, token logos, USD values at TX time. |
| **Transfers** | Transfer history across all indexed wallets. |
| **Extrinsics** | Block explorer with section/method filters, success/failed status, detailed args JSON. Search by hash. |
| **Order Book** | Limit order tracking (placed, executed, cancelled) with pair logos. |
| **Pools** | Liquidity pool list sorted by TVL, provider rankings, pool activity. |
| **Governance** | Council, Elections, Motions, Democracy, Technical Committee (5 sub-tabs). |
| **Tokens** | Token list with price, supply, 24h change, sparkline charts. |
| **Holders** | Per-token holder distribution with on-chain identity resolution. |

### Cross-Cutting Features

- **Real-time updates** via Socket.IO (new swaps, transfers, blocks).
- **On-chain identity resolution** with 3-tier cache (memory 1h, DB 24h, RPC fallback).
- **Deep links**: `#tx=HASH`, `#block=NUM`, `#wallet=ADDR`, `#pool=BASE-TARGET`.
- **Share + Screenshot** buttons on all modal views.
- **Candlestick charts** (LightweightCharts) with SMA/EMA overlays, 5m to 1D timeframes.
- **PWA** with Service Worker caching and offline support.
- **Multi-language** (EN/ES) with `data-i18n` attribute system.
- **Dark theme** with CSS custom properties.

---

## Tech Stack

| Category | Technology |
|----------|-----------|
| Runtime | Node.js |
| HTTP | Express 5 |
| Real-time | Socket.IO 4 |
| Database | SQLite via `better-sqlite3` |
| Blockchain | `@polkadot/api`, `@sora-substrate/api` |
| Security | Helmet, CORS, rate limiting, input validation, XSS escaping |
| Compression | gzip (`compression`) |
| Process Manager | PM2 |
| Charts | LightweightCharts, Chart.js |
| Screenshots | html2canvas |

---

## Getting Started

### Prerequisites

- Node.js >= 18
- npm

### Installation

```bash
git clone https://github.com/Rubrum95/sorametrics.git
cd sorametrics
npm install
```

### Configuration

Copy and edit the config file:

```bash
cp config.js config.local.js
```

Key variables in `config.js`:

| Variable | Default | Description |
|----------|---------|-------------|
| `WS_ENDPOINT` | `wss://ws.mof.sora.org` | SORA node WebSocket |
| `WS_ENDPOINT_BACKFILL` | `wss://mof2.sora.org` | Separate node for historical indexing |
| `PORT` | `3000` | HTTP server port |
| `CORS_ORIGINS` | `''` | Comma-separated allowed origins |

### Running

```bash
# Development
npm start

# Production (PM2)
pm2 start ecosystem.config.js
```

This starts two processes:
- `sorametrics` — API server on port 3000
- `sorametrics-backfiller` — Historical block indexer

---

## API Reference

52 endpoints organized by domain:

### Health & Meta
| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/health` | Server health check |
| GET | `/api/version` | Current version |

### Tokens & Pools
| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/tokens?page=&limit=` | Paginated token list with prices |
| GET | `/pools?page=&limit=` | Pool list sorted by TVL |
| GET | `/pool/providers?base=&target=&page=` | Pool liquidity providers |
| GET | `/pool/activity?base=&target=` | Pool add/remove events |
| GET | `/holders/:assetId?page=` | Token holder distribution |
| GET | `/chart/:symbol?resolution=` | OHLCV candlestick data |

### Wallet
| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/balance/:address` | Token balances for address |
| POST | `/balances` | Batch balance query |
| GET | `/wallet/liquidity/:address` | LP positions |
| GET | `/wallet/staking/:address` | Staking info (bonded, nominations) |
| GET | `/identity/:address` | On-chain identity |

### History
| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/history/global/swaps` | Global swap feed |
| GET | `/history/global/transfers` | Global transfer feed |
| GET | `/history/global/bridges` | Global bridge feed |
| GET | `/history/global/extrinsics` | Global extrinsic feed |
| GET | `/history/global/orderbook` | Global order book events |
| GET | `/history/global/liquidity` | Global liquidity events |
| GET | `/history/swaps/:address` | Wallet swap history |
| GET | `/history/transfers/:address` | Wallet transfer history |
| GET | `/history/bridges/:address` | Wallet bridge history |
| GET | `/history/extrinsics/:address` | Wallet extrinsic history |
| GET | `/history/orderbook/:address` | Wallet order book history |
| GET | `/search?q=` | Global search (hash, address, block) |

### Statistics
| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/stats/overview` | Dashboard summary metrics |
| GET | `/stats/header` | Header bar stats |
| GET | `/stats/network` | Network stats (validators, era) |
| GET | `/stats/network/trend` | Network trend data |
| GET | `/stats/fees` | Fee statistics |
| GET | `/stats/fees/trend` | Fee trend over time |
| GET | `/stats/trending-tokens` | Top movers |
| GET | `/stats/stablecoins` | Stablecoin metrics |
| GET | `/stats/accumulation` | Accumulation data |
| GET | `/currency-rates` | EUR/XOR exchange rates |

### Governance
| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/governance/council` | Council members |
| GET | `/governance/elections` | Election candidates |
| GET | `/governance/motions` | Active motions |
| GET | `/governance/democracy` | Referenda |
| GET | `/governance/technical-committee` | TC members |

### Burns & Supply
| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/burns/supply/:symbol` | Current total supply |
| GET | `/burns/supply-history/:symbol` | Historical supply snapshots |
| GET | `/burns/stats/:symbol` | Burn statistics |
| GET | `/burns/fee-flow` | Fee distribution flow |

---

## Database Schema

Dual-database strategy with SQLite:

- **`database_30d.db`** — Rolling 30-day window (fast queries for live dashboard)
- **`database.db`** — Full historical archive (attached as `hist`)

### Tables

| Table | Key Columns | Purpose |
|-------|-------------|---------|
| `transfers` | block, from_address, to_address, amount, symbol, time | Transfer events |
| `swaps` | block, caller, input/output symbol/amount, time | DEX swap events |
| `bridges` | block, caller, symbol, amount, direction, network, time | Bridge events |
| `fees` | block, caller, fee_amount, type, time | Transaction fees |
| `extrinsics` | block, extrinsic_id, hash, section, method, signer, success, time | All extrinsics |
| `liquidity` | block, caller, base/target assets, type, time | LP add/remove |
| `orderbook` | block, caller, event_type, base/target, price, amount, time | Order book events |
| `identities` | address, display, legal, web, twitter, updated_at | Cached on-chain identities |
| `supply_snapshots` | symbol, total_supply, block, timestamp | Periodic supply records |
| `burn_stats` | symbol, period, burned, block_start, block_end | Aggregated burn data |

### Performance Tuning

```sql
PRAGMA journal_mode = WAL;
PRAGMA synchronous = NORMAL;
PRAGMA cache_size = -64000;    -- 64 MB
PRAGMA mmap_size = 268435456;  -- 256 MB
PRAGMA temp_store = MEMORY;
```

---

## Deployment

The app runs on a VPS with PM2:

```bash
# Deploy files
scp index.js script.js index.html sw.js user@server:/app/

# Restart
ssh user@server "pm2 restart sorametrics"
```

### PM2 Ecosystem

```javascript
// ecosystem.config.js
{
  apps: [
    { name: 'sorametrics',            script: 'index.js',      max_memory_restart: '512M' },
    { name: 'sorametrics-backfiller', script: 'backfiller.js',  max_memory_restart: '1G'   }
  ]
}
```

---

## Project Structure

```
sorametrics/
├── index.js                  # Express API server + WebSocket
├── index.html                # SPA frontend (HTML + CSS)
├── script.js                 # Client-side JavaScript
├── sw.js                     # Service Worker (PWA cache)
├── db_better.js              # Database layer (better-sqlite3)
├── backfiller.js             # Historical block indexer
├── backfiller_orderbook.js   # Order book event indexer
├── blockchain.js             # Polkadot/SORA API connection
├── config.js                 # Environment configuration
├── ecosystem.config.js       # PM2 process definitions
├── package.json              # Dependencies
├── manifest.json             # PWA manifest
└── favicon.svg               # App icon
```

---

## License

MIT
