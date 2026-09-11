# SoraMetrics v33

Rust rewrite of the [SoraMetrics](https://sorametrics.org) backend: an indexer and query API for the
SORA v2 network (Substrate) and the Minamoto mainnet (Iroha 3 / SORA Nexus). It replaces a Node.js
monolith while serving the **same HTTP and Socket.IO contract**, so the existing frontend runs on
it unchanged.

## Why a rewrite

The production Node service couples the API, the WebSocket feed, the live indexer, bridge
listeners and cache writes in one process. A slow query stalls the event loop and delays live
events; a bug in any handler takes everything down; gaps after reconnects were filled by hand.
v33 splits ingestion from serving, types every on-chain payload from the runtime metadata, checks
every SQL statement at compile time, and makes backfills idempotent and gap detection automatic.

## Layout

| Crate | Binary | Role |
|-------|--------|------|
| `crates/core` | — | Domain types (SORA v2 events, Minamoto rows, SS58, time). |
| `crates/db` | — | PostgreSQL + TimescaleDB layer: migrations, typed `sqlx::query!` helpers for `sm.*` (SORA v2), `mn.*` (Minamoto) and `ts.*` (time series). |
| `crates/telemetry` | — | `tracing` setup. |
| `crates/substrate` | — | subxt codegen from the pinned SORA runtime metadata, event decoders, the per-block processor. |
| `crates/iroha` | — | Torii REST client, response DTOs and Prometheus text parser for Minamoto. |
| `crates/ingest` | `sorametrics-ingest` | `--source substrate`: finalized-block subscriber with gap-fill, price and supply samplers. `--source iroha`: Torii poller with chain-reset detection. |
| `crates/api` | `sorametrics-api` | axum query API (138 routes), Socket.IO realtime feed, per-route rate limiting. |
| `crates/ops` | `sorametrics-ops` | `decode-block`, `backfill`, `load-asset-registry`, `migrate-legacy` (ETL from the Node's database with mandatory reconciliation). |

Schemas are isolated: `sm.*` never references `mn.*` and vice versa. Migrations live in
`migrations/` and are applied by the ingest at start-up (or with `make migrate`).

## Quick start

Requirements: Rust 1.83+, Docker (the project uses Colima on macOS), `sqlx-cli`.

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

## Working on SQL

All queries are `sqlx::query!` and are checked against the schema captured in `.sqlx/`. After any
migration or query change:

```bash
DATABASE_URL=postgres://... cargo sqlx prepare --workspace
```

CI builds with `SQLX_OFFLINE=true`, so the cache must be committed.

## Contract parity

The frontend is not rewritten, so every route reproduces the Node's JSON byte for byte, including
its quirks (BIGINT columns rendered as strings, `toFixed(4)` amounts, `d/M/yyyy, HH:mm:ss` times in
the server's zone). `scripts/parity_check.py` compares a local instance with production route by
route; run it before every commit that touches a route:

```bash
python3 scripts/parity_check.py                 # all 119 routes
python3 scripts/parity_check.py --only /history # substring filter
python3 scripts/parity_check.py --strict        # pre-cutover: empty vs populated lists fail
```

Deliberate deviations are listed per route in `CLAUDE.md` (private project notes) and in the
module docs of each route file.

## Minamoto without a live Torii

`minamoto.sora.org` can be unreachable for long periods. Two helpers keep the Minamoto path
testable:

- `scripts/minamoto_mock_from_prod.py` rebuilds the legacy `mn.*` schema in a mock database from
  the JSON the production API serves, as the source for `sorametrics-ops migrate-legacy`.
- `scripts/torii_mock.py` serves the current Torii contract (cursor pagination,
  `/status.build.git_commit_sha`) from those fixtures; `--legacy` serves the page/per_page
  generation.

## Operations

Deploy, backfill, gap recovery, ETL and rollback are described in [`docs/operations.md`](docs/operations.md).

## License

MIT — see [LICENSE](LICENSE).
