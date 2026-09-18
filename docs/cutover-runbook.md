# Cutover runbook — sorametrics.org from the Node (VPS `sorametrics`) to v33 on Rubrum-01

Every step is idempotent or has a rollback. Steps marked **(user)** need the owner's
credentials or a decision and are not run by the assistant. Nothing here touches the Node
until the last section.

## 0. Preconditions
- Rubrum-01 upgraded to Cloud VPS 12 (12 vCPU / 48 GB / 400 GB). Check: `nproc`, `free -h`.
- Source tree at `/root/sorametrics-v33` (rsynced from the `v33` branch), frontend at
  `/root/sorametrics-static`, media at `/root/sorametrics-media/{music,news}` (done 2026-09-12).
- `deploy/.env` with `POSTGRES_PASSWORD`, `/root/sorametrics-v33/.env` from
  `deploy/env.rubrum.example` (both `chmod 600`) **(user supplies the two secrets)**.

## 1. Build (Rubrum, after the upgrade — never on 5.5 GB)
Ship the tree with `rsync -a --exclude target --exclude .git` (or `COPYFILE_DISABLE=1 tar` on
macOS): a plain macOS `tar` adds `._*` AppleDouble files and `sqlx::migrate!` refuses
`migrations/._2026….sql` at compile time (`find . -name '._*' -delete` fixes a tree already sent).
```bash
cd /root/sorametrics-v33 && SQLX_OFFLINE=true nice -n 10 cargo build --release --workspace -j 6
```
Done 2026-09-17 on the upgraded host (8 cores / 29 GB): 3 min 09 s, validator unaffected.
Binaries: `target/release/sorametrics-{api,ingest,ops}`. glibc 2.34 (AlmaLinux 9.4): do not
copy binaries built on Ubuntu.

## 2. Database
```bash
docker compose -f deploy/docker-compose.prod.yml up -d
./target/release/sorametrics-ops migrate
```
Do NOT start the live ingest on an empty cursor: it would try to gap-fill from block 0. Seed
`sm.indexer_state.substrate_live` with the height where the backfill ends (step 4) first.

## 3. History (one-off)
From the Mac (SSH to both hosts, Touch ID once each):
```bash
SRC_SQL="ssh sorametrics docker exec -i sora_subsquid_db psql -U postgres -d squid" \
DST_SQL="ssh sora-node docker exec -i sorametrics-v33-postgres psql -U sorametrics" \
deploy/transfer_legacy.sh
```
Resumable; ~9 GB on the wire for `sm.extrinsic_events`, ~2 GB for the rest. Then, on Rubrum:
```bash
./target/release/sorametrics-ops migrate-legacy --tables <ALL_TABLES from docs/operations.md> \
  --live-from 25278042 --live-from-ts 1789632873
```
`--live-from` is the first block of the chain-first era (step 4): legacy `mv_transfers`,
`mv_bridges`, `mv_fees`, `mv_liquidity_events`, `fee_burns_live` and `val_staking_rewards` run
past it, and those rows already exist from the chain (same natural keys). `--live-from-ts` is
the unix second the v33 samplers started on the host (first `sm.supply_snapshots` live row):
legacy supply/price samples from then on are v33's. Inside the bound, `price_history` is
upserted legacy-first (production's hourly points win over the backfill's per-block quotes).
Changing either bound after a run needs that table's `sm.etl_state` row reset. Reconciliation
must print `reconciliation OK for all migrated tables`; otherwise stop.
Leave `polkamarkt_trades,polkamarkt_claims,polkamarkt_buybacks,polkamarkt_burns` OUT of the
table list on Rubrum: Polkamarkt started at block 26.3 M, inside the chain-first era the backfill
already covers (verified equal to production market by market); those copies conflict on
`legacy_id` only and would duplicate the decoder's rows. `polkamarkt_markets` stays in: it fills
the static fields of a market whose earlier-era storage the decoder could not read (market 0).

## 4. Chain-first era
```bash
./target/release/sorametrics-ops backfill --from 25278042 --to <head> \
  --era-metadata --price-rpc wss://mof2.sora.org --concurrency 64 --rpc ws://127.0.0.1:9944
```
Bodies come from the local node; events/state of specs 119–129 need the archive metadata
(`--era-metadata` asks the node at each block; local nodes are pruned, so this range uses
`--rpc wss://mof2.sora.org` if the local node answers "State already discarded").

## 5. Processes
```bash
pm2 start deploy/ecosystem.v33.config.js && pm2 save
crontab -e   # */15 * * * * /root/sorametrics-v33/deploy/health-check-v33.sh >> /var/log/health-check-v33.log 2>&1
```
Checks: `curl -s localhost:3311/health`, `/health/freshness` (all tables healthy after the
gap fill), `/api/minamoto/indexer/state`.

## 6. Parity against production (Rubrum still unreachable from the public name)
```bash
python3 scripts/parity_check.py --v33 http://127.0.0.1:3311 --strict
```
Every route in parity except the documented deviations (integers > 2^53 in legacy `d`,
`/balance` ordering for assets the Node cannot price, block time vs insert time).

## 7. TLS and DNS **(user)**
- Certificates: either `rsync -a /etc/letsencrypt/` from the old VPS to Rubrum (private keys:
  the owner runs it), or issue new ones once DNS points at Rubrum.
- `nginx`: `deploy/nginx-sorametrics.conf` → `/etc/nginx/conf.d/`, `nginx -t`, reload;
  firewalld already allows http/https.
- Cloudflare: `sorametrics.org` and `www` A records → `31.220.78.37` (proxied). TTL is
  Cloudflare's; the switch is immediate. Rollback = point the records back.

## 8. Watch (1–2 weeks, Node still running on the old VPS)
`/health/freshness`, the health-check log, validator block production (`docker logs sora`),
`access.log` error rates. The Node keeps its own DB; nothing is shared.

## 9. Decommission **(user)**
Move the rest (opos-v3, oposbombero landing, IPFS seeder, validator — see
`project_sorametrics_vps_consolidation`), then cancel the `sorametrics` VPS. Drop `legacy_copy`
on Rubrum afterwards.
