# V6 Build Log — Living Document

**Purpose:** single source of truth for the local v6 adaptation. Each phase has a PLAN (written upfront) and a RESULT (written when the phase closes). I consult this file between phases to avoid losing context.

**Branch:** `v6-ui` (same branch as the prototype snapshot).
**Working dir:** `/tmp/sorametrics-v6-work/sorametrics/`
**Target local folder:** `v6-front/` (new — the working v6 codebase)
**Local dev server:** `v6-server.js` on port 3001 (serves `v6-front/` statics + proxies `/api/*`, `/history/*`, `/stats/*`, `/burns/*`, `/balance/*`, `/balances`, `/tokens`, `/pools`, `/holders/*`, `/identity/*`, `/currency-rates`, `/search`, `/chart/*`, `/governance/*`, `/staking/*`, `/wallet/*`, `/export/*`, `/music/*`, `/proxy-image`, `/lookup/*`, `/socket.io/*` to prod on `https://sorametrics.org`)

**Why proxy to sorametrics.org instead of running backend locally:** The backend needs Postgres `squid` + Redis + local SORA node on port 9944 (Docker on the VPS). Running it on a Mac is non-trivial (chain sync would take days). The prod VPS already serves all endpoints. Proxying is fastest + safest for UI iteration.

## Source inventory recap

- **Prototype (B6 snapshot)**: `v6-prototype-snapshot/b6-snapshot/` — 29 files, React+Babel in-browser, 14 langs, 13 sections + Network Pulse, drill panel, music player, wallet modals, Cmd+K, CSV, backup/restore.
- **Production front**: `script.js` (8067 lines) + `index.html` (242KB inline CSS) + `sw.js` + `manifest.json` + `music/` folder (empty stub, real audio lives on VPS).
- **Production backend**: `index.js` Express 5 + socket.io 4, 52 REST endpoints, 5 WS events (`new-block-stats`, `transfers-batch`, `swaps-batch`, `extrinsics-batch`, `orderbook-batch`), Postgres `sm` schema with 7 materialized views, Redis 6 cache keys, no auth, rate-limit in-memory per ip:route.

## Phase tracker

| Phase | Goal | Status | Commit |
|-------|------|--------|--------|
| 0 | Scaffold `v6-front/` + `v6-server.js` proxy + verify render | **DONE** | 350f78a |
| 1 | Wire Network Pulse to real socket.io (5 events) | **DONE** | f7034ee |
| 2 | Wire Burn Tracker to `/burns/*` + burn animation on new-block-stats | **DONE** | 3bebba7 |
| 3 | Wire Balance/Portfolio + Wallet Details 8 sub-tabs | **DONE (core)** | d0e0f3b |
| 4 | Wire Swaps / Transfers / Extrinsics / Bridges / OrderBook + CSV tax formats | **DONE** | 34a07ea |
| 5 | Wire Tokens / Pools / Staking / Governance / Intelligence | **DONE** | a1b83c6 |
| 6 | Wire Global Search `/search` + identity resolution `/identity/:addr` | **DONE (search)** | a87c363 |
| 7 | Peg monitor widget + deep-link URL routing | **DONE** | _pending_ |
| 8 | Build Batch 7 features locally: peg alerts, theme toggle, time-range, screenshot/share | PLANNED | — |
| 9 | Cutover prep: PWA, esbuild bundle, `/beta` subpath deploy | PLANNED | — |

---

## Ground rules (followed every phase)

- Keep all prototype features working while swapping mock → real. No regressions.
- Every phase closes with: commit + push + update this log with "Delta" summary.
- No speculation — if an endpoint is missing or behaves unexpectedly, document in Blockers.
- Never log secrets, never amend commits, never force-push.
- Consult this file at phase start; write result at phase end.

## Phase 0 — Scaffold

### Plan
1. Copy `v6-prototype-snapshot/b6-snapshot/` → `v6-front/`.
2. Rename `SoraMetrics Prototype.html` → `index.html` (standard entrypoint).
3. Write `v6-server.js` — Express with:
   - Static serve of `v6-front/`
   - Proxy to `https://sorametrics.org` for every API path + socket.io.
4. Write `v6-front/package.json` with start script.
5. Run `node v6-server.js` and open `http://localhost:3001`. Verify: prototype renders, no CORS errors, sidebar navigates.
6. Commit + push: `feat(v6): scaffold local v6-front + proxy server`.

### Result (Phase 0 closed)
- Copied `v6-prototype-snapshot/b6-snapshot/` → `v6-front/` (29 files, 940 KB).
- Renamed entry: `SoraMetrics Prototype.html` → `v6-front/index.html`.
- Wrote `v6-front/package.json` (`"type": "module"`, express 5 + http-proxy-middleware 3).
- Wrote `v6-front/v6-server.js`: root-mounted `createProxyMiddleware({ pathFilter, ws: true })` — prefix-mount was stripping the path (GET /tokens became GET / upstream and returned SPA HTML).
- Port bumped 3001 → **3005** (3001 was busy with another Vite process).
- **Proxy live**: `GET /api/version` → `{"version":"v4.0"}`, `GET /tokens` → real token list, `GET /stats/header` → `{"block":25754434, "swaps":146, ...}`. Confirmed reaching prod at `https://sorametrics.org`.
- **Prototype render bug fixed**: Claude Design's B6 export shipped with two `const t = useT()` calls placed INSIDE `useEffect` bodies (same class as the Pagination bug it auto-fixed). Location: `js/extrinsics.jsx:151` and `js/burns.jsx:20`. Moved the extrinsics one to the top of the component and deleted the burns one (dead, unused).
- Verified in Chrome devtools at `http://localhost:3005/`: Extrinsics section renders fully with Spanish labels (Rastreador de Quemas, Pulso de la red, Extrínsecos, etc.), LangPicker ES pill visible top-right, all 4 KPI cards + filter row + 15-row table + pagination. No console errors (only harmless Babel-in-browser eval notice).

### File changes
- new: `v6-front/` (29 prototype files), `v6-front/package.json`, `v6-front/v6-server.js`, `v6-front/node_modules/` (not committed)
- modified: `v6-front/js/extrinsics.jsx` (L133-153), `v6-front/js/burns.jsx` (L20-21)

### How to run
```bash
cd /tmp/sorametrics-v6-work/sorametrics/v6-front
export PATH="/opt/homebrew/bin:$PATH"
node v6-server.js              # http://localhost:3005
TARGET=https://sorametrics.org PORT=3005 node v6-server.js  # custom target
```

---

## Phase 1 — Network Pulse

### Plan
1. Open `v6-front/js/pulse.jsx`. Find the mocked event generator (probably `setInterval` loop).
2. Replace with `io('/')` (will be proxied to prod socket.io via `v6-server.js`).
3. Subscribe to 5 events: `new-block-stats` (→ update BLOCK chip + trigger feed prepend as "block" event), `transfers-batch`, `swaps-batch`, `extrinsics-batch`, `orderbook-batch`.
4. Map each batch to the prototype's feed item shape. Preserve filter chips (All / Swap / Transfer / Block / Order / Burn) and the 40-event ring buffer.
5. Verify: real wallet addresses + real block numbers start appearing. Connected/Pause toggle works.
6. Commit + push.

### Result (Phase 1 closed)
- Loaded `socket.io-client@4.7.5` via CDN in `index.html`.
- `v6-server.js` already proxies `/socket.io/*` with `ws: true` + an explicit `server.on('upgrade')` handler so WebSocket upgrades bypass `express.static`.
- Wrote `getPulseSocket()` singleton in `pulse.jsx` — one shared socket for the whole section, reconnects automatically.
- Subscribed to all 5 prod events: `new-block-stats`, `swaps-batch`, `transfers-batch`, `extrinsics-batch`, `orderbook-batch`.
- **Deltas discovered vs inventory assumption**:
  1. `time` field comes as Spanish locale string `"18/4/2026, 16:54:12"`, NOT ISO. Wrote `parseTime()` with ISO-first + locale-regex fallback.
  2. `new-block-stats.finalized` is the **last finalized block NUMBER** (integer), not a boolean. The event fires on every new head (~every 6 s) — without dedup we'd get one row per second in the feed. Added `lastFinalizedRef` to only push when the finalized number advances.
  3. Extrinsics are ~99% `timestamp::set` (per-block housekeeping) during idle hours. Added `EXTRINSIC_NOISE` set filter: `timestamp::set`, `imOnline::heartbeat`, `parachainSystem::setValidationData` dropped from the feed (drill + table pages can still show them).
- **Pause button** wired: `setPaused` toggles, the `push()` function respects `pausedRef`.
- **Connection status chip** turns green "conectado" + displays current block `· #25.754.523` live.
- Verified in browser: feed shows `Block #25.754.520 finalized`, `avg time 5.98s`, relative times (`now`, `6s`, `12s`, `18s`) correctly updating. Feed hovers at 40-item max ring buffer.

### Not wired yet (deferred to later phases)
- KPI cards (14,208 / $4.27M / 2,810 / 6.01s) still mocked — they'll wire to `/stats/overview` or `/stats/network/trend` in Phase 5 when we touch Intelligence.
- "Trending Tokens · 24h" sidebar card + "Network Health" still mocked — same deferral.
- Drill open from feed item uses a hand-built object with mocked detail fields — will get real data from `/history/extrinsic/:block/:idx` in Phase 4.

### File changes
- modified: `v6-front/index.html` (add `socket.io@4.7.5` CDN script)
- modified: `v6-front/js/pulse.jsx` (full rewrite of PulseSection + 5 mapper functions + `parseTime` + `EXTRINSIC_NOISE` + socket singleton)

---

## Phase 2 — Burn Tracker

### Plan
1. Open `v6-front/js/burns.jsx`. Swap mocks for:
   - `GET /burns/stats/:sym` → total burned, 24h, 7d
   - `GET /burns/supply/:sym` → current circulating
   - `GET /burns/supply-history/:sym` → time-series for chart
   - `GET /burns/holders/:sym` → top-10 holders
   - `GET /burns/fee-flow` → fee pipeline diagram
2. Replace whatever chart lib the prototype uses with `lightweight-charts@4.0.1` (already loaded in prod; will add via CDN).
3. Hook `new-block-stats` socket.io event → trigger burn animation on new block.
4. Commit + push.

### Result (Phase 2 closed)
- Added `fetchJson()` helper (null on error, never throws into render).
- Fixed missing `const t = useT()` at top of `BurnSection`.
- Wired 3 real endpoints per-token + global: `/burns/stats/:sym`, `/burns/fee-flow`, `/burns/holders/:sym`. Fee-flow refreshes every 30 s; stats + holders re-fetch on token switch.
- **Hero counter**: starts from `stats.7d.totalBurned` (real 7d cumulative), ticks +0.0001 on each `new-block-stats` event (cheap optimistic), reconciles via stats re-fetch throttled to once per 30 s.
- **24h / 7d / 30d deltas**: real `totalBurned` values from the stats envelope.
- **Current Supply**: real `currentSupply` (77.6K XOR verified).
- **Price**: derived as `usd24 / d24` from the 24h totals (prod doesn't expose a dedicated price field here — this is the cheapest consistent proxy).
- **Market Cap**: `currentSupply × price` live.
- **Holders**: real total count from `holdersData.totalHolders`, top 6 addresses rendered with `balance / totalSupply × 100` as the bar percentage.
- Verified in browser at `/#burns`: hero 54.57 XOR = $516.74 (real 7d total), supply 77.6K, 206 holders, meta rows show $29.92 24h USD. Holders list shows real chain addresses (cnRus2…, cnRwt3…, cnVhh2…).

### Not wired yet (deferred)
- `BurnChart` still uses the prototype's seeded-random sparkline — real `/burns/supply-history/:sym` returns raw 18-decimal values on a varying schedule; a dedicated chart lib swap (lightweight-charts) is cleaner to do in Phase 7 when we build the Chart modal.
- `Furnace` ember animation keeps its own interval; a live-synced spark on new-block-stats is cosmetic and deferred.

### File changes
- modified: `v6-front/js/burns.jsx` — `fetchJson` helper, `useT` at top, 3 real endpoints wired, `new-block-stats` hook for optimistic counter tick.

---

## Phase 3 — Balance / Portfolio + Wallet Details

### Plan
1. Open `v6-front/js/routes.jsx` BalanceSection.
2. Replace mocked wallets with localStorage `sora_wallets` (import format from prod).
3. For each wallet: `POST /balances` batch → render asset bars.
4. Port `sora_theme`, `sora_portfolio_currency`, `sora_hide_low_balances`, `sora_hide_balances`, `sora_group_wallets` from prod into Tweaks panel + Balance tabs.
5. Add Wallet Details modal missing 7 sub-tabs: swaps (`/wallet/.../wallet/:addr`... actually per-addr endpoints: `/history/swaps/:addr`, `/history/transfers/:addr`, `/history/bridges/:addr`, `/history/extrinsics/:addr`, `/wallet/liquidity/:addr`, `/wallet/staking/:addr`, `/wallet/info/:addr`).
6. Wire Add Wallet 3 tabs to either add to `sora_wallets` localStorage (watch-only) or reject seed/priv (since we don't auth).
7. Commit + push.

### Result (to fill)
_(updated when phase closes)_

---

## Phase 4 — Swaps / Transfers / Extrinsics / Bridges / OrderBook + CSV

### Plan
1. For each section, replace mocked rows with paginated fetch:
   - `GET /history/global/swaps?page=&limit=&sort=&from=&to=`
   - `GET /history/global/transfers?...`
   - `GET /history/global/extrinsics?...`
   - `GET /history/global/bridges?...`
   - `GET /history/global/orderbook?...`
2. Keep drill panel SWAP/TRANSFER/EXTRINSIC/BRIDGE/ORDER bodies — just feed real data.
3. Add Extrinsic Detail deep-link: drill EXTRINSIC body fetches `/history/extrinsic/:block/:idx` for full decoded args.
4. Replace generic CSV helper with format selector modal (sorametrics / koinly / cointracking / cointracker) calling `/export/csv?format=...`.
5. Commit + push.

### Result (to fill)
_(updated when phase closes)_

---

## Phase 5 — Tokens / Pools / Staking / Governance / Intelligence

### Plan
- Tokens: `/tokens` for grid, `/chart/:symbol?res=1h|1d|7d` for sparklines, `/holders/:assetId` for distribution bar.
- Pools: `/pools` for list, `/pool/providers?pool=` + `/pool/activity?pool=` for inline-expand.
- Staking: `/staking/validators` + `/staking/network` + `/staking/recent-blocks`.
- Governance: 5 sub-tabs hit `/governance/{council,elections,motions,democracy,technical-committee}`.
- Intelligence: `/stats/accumulation`, `/stats/trending-tokens`, `/stats/stablecoins`, `/stats/fees`, `/stats/fees/trend`, `/stats/header`, `/stats/network/trend`.
- Commit + push.

### Result (to fill)
_(updated when phase closes)_

---

## Phase 6 — Global Search + Identity

### Plan
1. Cmd+K palette: replace mocked 50-item index with `GET /search?q=...`. Keep recents-in-localStorage.
2. On drill panel open for a wallet → `GET /identity/:addr` and display alias if available.
3. Commit + push.

### Result (to fill)
_(updated when phase closes)_

---

## Phase 7 — Missing prod modals

### Plan
1. Peg monitor modal: Chart.js line chart KUSD/XSTUSD/TBCD vs $1.00, depeg badge at >2%. Data from `/stats/stablecoins` + `/burns/supply/*`.
2. Pool Details modal: full page from `/pool/providers` + `/pool/activity`.
3. Chart modal: per-symbol big price chart from `/chart/:symbol`.
4. Deep-link URLs: `?tab=X&wallet=Y&tx=Z` routing.
5. Commit + push.

### Result (to fill)
_(updated when phase closes)_

---

## Phase 8 — Batch 7 features (local, no Claude Design)

### Plan
1. Peg alerts: threshold input in Tweaks panel, check `/stats/stablecoins` every 60s, toast + badge when triggered. Persist thresholds in localStorage.
2. Theme toggle: dark/light/auto in Tweaks, `data-theme` attribute, localStorage.
3. Time-range global selector (1h/24h/7d/30d/90d/all) in Tweaks → applies to all KPIs + charts.
4. Screenshot/share: every KPI card + table row gets a share icon, `html2canvas` → PNG or `?state=base64` deep-link.
5. Commit + push.

### Result (to fill)
_(updated when phase closes)_

---

## Phase 9 — Cutover prep

### Plan
1. Pre-compile Babel: build step with `esbuild` → `v6-front/dist/bundle.js`. HTML loads pre-built.
2. PWA: adapt prod `sw.js`, bump cache name to `sorametrics-v24`, new version gate.
3. Manifest: same as prod but `start_url` is `/beta/`.
4. Deploy: `scp -r v6-front/ sorametrics:/var/www/sorametrics-beta/`, nginx alias `/beta → /var/www/sorametrics-beta`.
5. Smoke test shadow traffic against prod data.
6. Final commit + push.

### Result (to fill)
_(updated when phase closes)_

---

## Blockers log

_(any blocker encountered gets logged here; resolve or defer with a note)_

## Open questions for user

_(when user decision is needed mid-build)_

## File changes index

_(track which prototype files were modified in each phase, helps future sessions)_
