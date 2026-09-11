# Operations

## Processes

| Process | Command | Notes |
|---------|---------|-------|
| SORA v2 indexer | `sorametrics-ingest --source substrate` | Applies migrations at start, resumes from `sm.indexer_state.substrate_live`, fills any gap before following the finalized stream. |
| Minamoto indexer | `sorametrics-ingest --source iroha` | Polls Torii; every job run is recorded in `mn.indexer_state` (`/api/minamoto/indexer/state`). |
| API | `sorametrics-api` | Stateless. Socket.IO on `/socket.io/*`. Needs `WS_ENDPOINTS` for chain-backed routes and `MINAMOTO_TORII` for `/api/minamoto/*` passthroughs. |

Run them as separate supervised processes (PM2 or systemd) with the environment from
`.env.example`. Logs are `tracing` lines on stdout; set `RUST_LOG` per target.

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

The pinned runtime metadata covers the current spec only; blocks of an earlier runtime fail with
`Not enough data to fill buffer` and are served from the ETL'd history instead.

## Loading history from the Node database (ETL)

```bash
LEGACY_DATABASE_URL=postgres://... sorametrics-ops migrate-legacy \
  --tables asset_registry,swaps,transfers,bridges,fees,fee_burns,price_history,liquidity,extrinsics,order_book,val_staking_rewards,supply_snapshots,supply_history,news_episodes,polkamarkt_markets,polkamarkt_trades,polkamarkt_claims,polkamarkt_buybacks,polkamarkt_burns,mn_blocks,mn_accounts,mn_transactions,mn_instructions,mn_domains,mn_asset_definitions,mn_assets,mn_peers,mn_network_state,mn_indexer_state,mn_metrics_snapshots
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
