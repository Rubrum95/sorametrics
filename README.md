# SoraMetrics

Production-grade analytics dashboard for the [SORA Network](https://sora.org). Indexes on-chain data via [sora-subsquid](https://github.com/sora-xor/sora-subsquid), serves it through a REST + WebSocket API, and renders it in a responsive single-page frontend.

**Live:** [sorametrics.org](https://www.sorametrics.org)

---

## Architecture

```
                    SORA Blockchain
                    (25M+ blocks)
                         |
          +--------------+--------------+
          |                             |
     Real-time                   Historical data
          |                             |
     index.js                    SQD Network
     (WebSocket)               (pre-indexed data lake)
          |                             |
          |                      sora-subsquid
          |                    (TypeScript processor)
          |                             |
          |                      PostgreSQL 14
          |                        (Docker)
          |                             |
          |                    pg_to_sqlite.js
          |                   (export every 10 min)
          |                             |
          +-------------+---------------+
                        |
                   SQLite x 2
              sorametrics_live.db    <-- real-time data
              sorametrics_history.db <-- full history
                        |
                 Express + Socket.IO
                    (index.js)
                        |
                  sorametrics.org
```

| Layer | File(s) | Role |
|-------|---------|------|
| **Frontend** | `index.html`, `script.js`, `sw.js` | SPA with Chart.js, LightweightCharts, Socket.IO client. PWA-capable. |
| **API Server** | `index.js` | Express REST API (52 endpoints) + Socket.IO real-time feed. |
| **Database** | `db_better.js` | `better-sqlite3` with dual-DB strategy: live (`database_30d.db`) + full history (`database.db`). WAL mode, prepared statements, 256 MB mmap. |
| **Indexer** | `sora-subsquid` | Official SORA indexer (Subsquid SDK). Processes all blocks from SQD Network into PostgreSQL. |
| **Export** | `export/pg_to_sqlite.js` | Transforms PostgreSQL data to SoraMetrics SQLite format. Runs every 10 minutes via PM2 cron. |
| **Blockchain** | `blockchain.js`, `config.js` | `@polkadot/api` + `@sora-substrate/api` connection with auto-reconnect. |

---

## Tech Stack

| Category | Technology |
|----------|-----------|
| Runtime | Node.js 20 |
| HTTP Server | Express 5 |
| Real-time | Socket.IO 4 |
| App Database | SQLite via `better-sqlite3` (2 databases, WAL mode) |
| Indexer Database | PostgreSQL 14 (Docker) |
| Indexer | sora-subsquid (TypeScript, Subsquid SDK, SQD Network) |
| Blockchain SDK | `@polkadot/api`, `@sora-substrate/api` |
| Security | Helmet, CORS, rate limiting, input validation, XSS escaping |
| Compression | gzip (`compression`) |
| Process Manager | PM2 |
| Container | Docker (PostgreSQL) |
| Charts | LightweightCharts, Chart.js |
| Screenshots | html2canvas |
| Server | Linux VPS (Contabo) |

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
- **Dark/Light theme** with CSS custom properties.

---

## Database Architecture

SoraMetrics uses a **dual SQLite + PostgreSQL** architecture:

### SQLite (Application)

| Database | Purpose | Written by |
|----------|---------|------------|
| `database_30d.db` | Live data — real-time events from WebSocket | `index.js` (Express server) |
| `database.db` | Full history — complete blockchain data | `pg_to_sqlite.js` (export script) |

Both databases are queried together via `ATTACH DATABASE`. The `dedup()` function in `db_better.js` handles overlap between live and historical data.

#### SQLite Tables

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

#### SQLite Performance Tuning

```sql
PRAGMA journal_mode = WAL;
PRAGMA synchronous = NORMAL;
PRAGMA cache_size = -64000;    -- 64 MB
PRAGMA mmap_size = 268435456;  -- 256 MB
PRAGMA temp_store = MEMORY;
```

### PostgreSQL (Indexer)

PostgreSQL 14 runs in Docker (`sora_subsquid_db`, port 23798, localhost only) and stores the raw indexed data from sora-subsquid. Key tables:

| Table | Purpose |
|-------|---------|
| `history_element` | All blockchain calls and events with parsed data |
| `asset_snapshot` | Hourly/daily OHLC prices, supply, burn/mint per asset |
| `staking_era`, `staking_validator`, `staking_reward` | Staking data |
| `vault`, `vault_event` | Kensetsu/CDP data |
| `referrer_reward` | Referral rewards |
| `order_book`, `order_book_order` | Order book state |

---

## Getting Started

### Prerequisites

- Node.js >= 18
- Docker (for PostgreSQL)
- npm

### Installation

```bash
git clone https://github.com/Rubrum95/sorametrics.git
cd sorametrics
npm install
cd export && npm install && cd ..
```

### Configuration

Key variables in `config.js`:

| Variable | Default | Description |
|----------|---------|-------------|
| `WS_ENDPOINT` | `wss://ws.mof.sora.org` | SORA node WebSocket |
| `PORT` | `3000` | HTTP server port |
| `CORS_ORIGINS` | `''` | Comma-separated allowed origins |

For the indexer, copy and edit the environment file:

```bash
cp subsquid-config/.env.example subsquid-config/.env
```

### Running

```bash
# 1. Start PostgreSQL
cd sora-subsquid && docker compose -f docker-compose.prod.yml up -d

# 2. Start the indexer (sora-subsquid)
pm2 start lib/processor.js --name sora-subsquid-processor

# 3. Start the app
pm2 start index.js --name sorametrics

# 4. Start periodic export (every 10 minutes)
pm2 start export/pg_to_sqlite.js --name sorametrics-export --cron "*/10 * * * *"
```

### PM2 Processes

| Process | Script | Role |
|---------|--------|------|
| `sorametrics` | `index.js` | API server + WebSocket |
| `sora-subsquid-processor` | `lib/processor.js` | Blockchain indexer |
| `sorametrics-export` | `export/pg_to_sqlite.js` | PG to SQLite export (every 10 min) |

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

## Project Structure

```
sorametrics/
├── index.js                  # Express API server + WebSocket
├── index.html                # SPA frontend (HTML + CSS)
├── script.js                 # Client-side JavaScript
├── sw.js                     # Service Worker (PWA cache)
├── db_better.js              # Database layer (better-sqlite3)
├── blockchain.js             # Polkadot/SORA API connection
├── config.js                 # Environment configuration
├── package.json              # Dependencies
├── manifest.json             # PWA manifest
├── favicon.svg               # App icon
│
├── export/
│   ├── pg_to_sqlite.js       # PostgreSQL -> SQLite transformer
│   └── package.json          # Export dependencies
│
└── subsquid-config/
    ├── docker-compose.prod.yml  # PostgreSQL Docker config
    └── .env.example             # Environment template
```

---

## License

MIT
