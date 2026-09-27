# SoraMetrics v33

[![CI](https://github.com/Rubrum95/sorametrics/actions/workflows/ci.yml/badge.svg?branch=v33)](https://github.com/Rubrum95/sorametrics/actions/workflows/ci.yml)

Indexer, query API and web app behind [sorametrics.org](https://sorametrics.org), for the SORA v2
network (Substrate) and the Minamoto mainnet (Iroha 3 / SORA Nexus). Written in Rust, it has served
production since 2026-09-20, when it replaced the Node.js monolith behind the **same HTTP and
Socket.IO contract**.

## Why a rewrite

The Node service coupled the API, the WebSocket feed, the live indexer, bridge listeners and cache
writes in one process. A slow query stalled the event loop and delayed live events, a bug in any
handler took everything down, and gaps after reconnects were filled by hand. v33 splits ingestion
from serving, types every on-chain payload from the runtime metadata, checks every SQL statement at
compile time, and makes backfills idempotent and gap detection automatic.

## Layout

| Path | Binary | Role |
|------|--------|------|
| `crates/core` | — | Domain types (SORA v2 events, Minamoto rows, SS58, time). |
| `crates/db` | — | PostgreSQL + TimescaleDB layer: migrations, typed `sqlx::query!` helpers for `sm.*` (SORA v2), `mn.*` (Minamoto) and `ts.*` (time series). |
| `crates/telemetry` | — | `tracing` setup. |
| `crates/substrate` | — | subxt codegen from the pinned SORA runtime metadata, event decoders, the per-block processor, historical price quotes. |
| `crates/iroha` | — | Torii REST client, response DTOs and Prometheus text parser for Minamoto. |
| `crates/ingest` | `sorametrics-ingest` | `--source substrate`: finalized-block subscriber with gap-fill, price and supply samplers. `--source iroha`: Torii poller with chain-reset detection. |
| `crates/api` | `sorametrics-api` | axum query API (148 routes), Socket.IO realtime feed, MCP server for agents, per-route rate limiting; also serves the frontend. |
| `crates/ops` | `sorametrics-ops` | `decode-block`, `backfill` and `gap-fill` (optionally with per-era metadata and historical prices), `migrate`, `metadata-check`, `load-asset-registry`, `migrate-legacy` (ETL from the Node's database with mandatory reconciliation). |
| `frontend/` | — | The web app: static HTML + React JSX compiled in the browser, no build step. |

Schemas are isolated: `sm.*` never references `mn.*` and vice versa. Migrations live in
`migrations/` and are applied by the ingest at start-up (or with `make migrate`).

## Quick start

Requirements: Rust 1.83+ (CI uses 1.94.0), Docker (the project uses Colima on macOS), `sqlx-cli`.

```bash
cp .env.example .env
make dev-up            # PostgreSQL 14 + TimescaleDB and Redis 7
make migrate           # applies migrations/ to DATABASE_URL
cargo test --workspace # compile-time SQL checks run against the migrated database
```

Run the services against SORA mainnet:

```bash
cargo run -p sorametrics-ingest -- --source substrate   # needs WS_ENDPOINTS
cargo run -p sorametrics-api                             # API_BIND, default 127.0.0.1:3001
cargo run -p sorametrics-ingest -- --source iroha        # needs MINAMOTO_TORII
```

The first substrate run resumes from the `substrate_live` cursor in `sm.indexer_state`; set it
close to the head before the first start in a fresh database, otherwise the gap-fill walks the
whole distance to the chain head.

Every variable is documented in [`.env.example`](.env.example).

## Frontend

`frontend/` is served by the API from `STATIC_DIR`. It has dark, light and automatic themes and
14 languages. To work on it without a local backend:

```bash
node scripts/dev_preview.js   # files from disk, API calls proxied to sorametrics.org, on :8811
```

`scripts/deploy_frontend.sh` publishes it and stamps the stylesheet URL with its content hash so
cached copies are replaced immediately.

## For agents

- MCP server (read-only, 22 tools, 4 prompts): `https://sorametrics.org/mcp`
  ```bash
  claude mcp add --transport http sorametrics https://sorametrics.org/mcp
  ```
- OpenAPI description: [`/openapi.json`](https://sorametrics.org/openapi.json)
- What the data means and its caveats: [`/llms.txt`](https://sorametrics.org/llms.txt)

## Working on SQL

All queries are `sqlx::query!` and are checked against the schema captured in `.sqlx/`. After any
migration or query change:

```bash
DATABASE_URL=postgres://... cargo sqlx prepare --workspace
```

CI builds with `SQLX_OFFLINE=true`, so the cache must be committed. The CI workflow runs rustfmt,
clippy with warnings denied, the tests against PostgreSQL + TimescaleDB and Redis services, and a
non-blocking check that the pinned runtime metadata still matches the chain (`make metadata-check`).

## Contract and data

Routes keep the Node's JSON shapes, including its quirks (BIGINT columns rendered as strings,
`toFixed(4)` amounts, `d/M/yyyy, HH:mm:ss` times in the server's zone). `scripts/parity_check.py`
compares an instance with another route by route. Where the Node's numbers were wrong, v33
deliberately differs; each such change is recorded in [`CHANGELOG.md`](CHANGELOG.md) (for example
a swap is valued at its cheaper priced leg, and the average block time is measured on chain
instead of the runtime's 6 s target).

## Minamoto without a live Torii

`minamoto.sora.org` can be unreachable for long periods. Two helpers keep the Minamoto path
testable:

- `scripts/minamoto_mock_from_prod.py` rebuilds the legacy `mn.*` schema in a mock database from
  the JSON the production API serves, as the source for `sorametrics-ops migrate-legacy`.
- `scripts/torii_mock.py` serves the current Torii contract (cursor pagination,
  `/status.build.git_commit_sha`) from those fixtures; `--legacy` serves the page/per_page
  generation.

## Documentation

- [`docs/operations.md`](docs/operations.md): deploy, backfill, gap recovery, ETL and rollback.
- [`docs/cutover-runbook.md`](docs/cutover-runbook.md): how production moved from the Node to v33.
- [`docs/design/palette-nexus.md`](docs/design/palette-nexus.md): the colour tokens of both themes.
- [`CHANGELOG.md`](CHANGELOG.md): what changed and why.

## License

MIT — see [LICENSE](LICENSE).
