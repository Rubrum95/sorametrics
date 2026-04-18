# SoraMetrics v6 — Gap Analysis: Claude Design Prototype vs Production

**Date:** 2026-04-18
**Prototype:** B6 snapshot in `v6-prototype-snapshot/b6-snapshot/` (React+Babel, 29 files, 895KB)
**Production:** `main` branch (vanilla JS `script.js` 8067 lines + `index.js` Express 4312 lines + Postgres schema `sm` + Redis + Socket.IO)

## Verdict

The prototype is **UX-complete** for 80% of production's surface, but **missing 40%+ of production's data features**. The right move is **phased adaptation in-place**: keep the prototype's visual shell, wire each section one-by-one to the real `/api/*` endpoints and `socket.io` events, and add the handful of missing production features the prototype doesn't model yet.

## What the prototype ALREADY covers (parity with prod)

| Production tab | Prototype section | Status |
|---|---|---|
| Portfolio (`#balance`) | Balance (3 sub-tabs: Overview / Mis Wallets / Vigiladas) | UX match, data mocked |
| Swaps (`#swaps`) | Swaps | UX match (adds drill panel), data mocked |
| Transfers (`#transfers`) | Transfers | UX match, data mocked |
| Tokens (`#tokens`) | Tokens (card grid + sparklines + favorites) | UX match, data mocked |
| Liquidity (`#liquidity`) | Pools (inline-expand → top-10 LPs) | UX match, data mocked |
| Bridges (`#bridges`) | Bridges | UX match, data mocked |
| Order Book (`#orderbook`) | Order Book (bid/ask + spread + recent fills) | UX match, data mocked |
| Governance (`#governance`) | Governance (5 Spanish sub-tabs) | UX match, data mocked |
| Extrinsics (`#extrinsics`) | Extrinsics (12 pallets, inline-expand) | UX match + richer drill |
| Burns (`#burns`) | Burn Tracker | UX match, data mocked |
| Staking (`#staking`) | Staking (Validators + Network Info) | UX match, data mocked |
| Intelligence (`#section-intelligence`) | Intelligence (severity-ribboned cards) | UX match, data mocked |
| — | Network Pulse | **New, not in prod** |

## What the prototype IMPROVES over prod

- **Drill-down side panel** with 11 type-specific bodies (SWAP, TRANSFER, BLOCK, ORDER, BURN, EXTRINSIC, LP, HOLDER, VALIDATOR, BRIDGE, FEED) — prod has modals, prototype has a single unified drill
- **Global Search ⌘K** with fuzzy match + type filter tabs + recent queries — prod has plain `#globalSearchBar`
- **14 languages** (es/en/fr/de/it/pt/ru/zh/ja/ko/ar-RTL/he-RTL/ur-RTL/hi) vs prod's **6** (es/en/pt/it/ru/zh) — plus prod's dropdown offers jp/tr/he/ur but without dictionaries (dead options)
- **RTL layout** with sidebar flip + drill mirror + `.num { direction: ltr }` — prod has no RTL
- **Tweaks panel** with visible-tabs (max 5), theme, density, section selector, plus backup/restore — prod has tab-pick but no unified settings panel
- **Toast stack** (color-coded ok/err/info) — prod uses inline banners
- **Wallet modals** (Import seed / Private key / Watch-only) + Wallet Details — prod has `#addWalletModal` + `#walletDetailsModal` with 8 sub-tabs. **Prod's is richer** (8 sub-tabs); prototype's needs a second pass to reach parity.
- **Music player** UX + ambient sakura stars — prod has same music player wired to real `/music/list` + `<audio>` element; prototype's is simulated

## What the prototype is MISSING (must add)

### A. Features present in prod, absent in prototype
1. **Stablecoin peg monitor** with Chart.js line + $1.00 reference + "Depegged" badge at >2% deviation (KUSD / XSTUSD / TBCD). → **Already in planned Batch 7** (Peg alerts).
2. **Burns Tracker real-time**: supply history chart, fee-flow diagram with **burn animation on new blocks**, per-token holders (XOR / VXOR / KUSD / XSTUSD). → Prototype has a Burn Tracker page but shape/animation is mocked.
3. **Whale accumulation leaderboard** (`/stats/accumulation`) — prototype has Intelligence cards but no dedicated accumulators list.
4. **Network fees by pallet** (`/stats/fees` + `/stats/fees/trend`) — prototype has Extrinsics KPIs, but not the pallet-trend breakdown.
5. **Wallet Details 8 sub-tabs**: prod has `wview-{assets, swaps, transfers, bridges, liquidity, staking, info, extrinsics}`. Prototype has Wallet Details with rename + copy + asset bars + delete only. Missing 7 sub-tabs.
6. **Pool Details modal** with provider + activity sub-tabs (`/pool/providers`, `/pool/activity`). Prototype has inline-expand only.
7. **Extrinsic Detail modal** (`#extrinsicDetailModal`) with deep link by block/idx. Prototype has inline-expand in Extrinsics table + drill panel, but no shareable URL.
8. **Block modal** (`#blockModal`) — prototype uses drill panel as replacement, but prod's block detail is a separate modal, deeper.
9. **Chart modal** (`#chartModal`) for per-symbol price chart with `/chart/:symbol?res=`. Prototype has sparklines in Tokens but no big modal chart.
10. **Holder modal** (`#holderModal`) — prototype has drill panel HOLDER body, prod has full modal with historical portfolio.
11. **Per-wallet CSV export tax formats** — prod exposes `sorametrics | koinly | cointracking | cointracker` format variants via `/export/csv?format=...`. Prototype has a generic CSV exporter (no tax-format variants).
12. **Portfolio advanced settings**: multi-currency (USD / EUR / XOR via `/currency-rates`), hide-balances toggle, hide-low-balances, group-wallets mode, paginated holdings. Prototype has basic Portfolio only.
13. **Multi-format CSV from Wallet Details** — prod's `#csvExportModal` offers wallet / type / date-range selectors. Prototype has plain "Export CSV" in toolbar.
14. **Search backend** — prod has `/search` returning wallet / tx-hash / block / extrinsic-id / token matches. Prototype ⌘K uses a mocked 50-item index.
15. **Identity resolution** — prod has `/identity/:addr` + `POST /api/identities` + 1h-TTL in-memory cache. Prototype just truncates addresses.

### B. Features planned in Batch 7 (not yet built)
- **Peg alerts** (automatic thresholds on KUSD/TBCD)
- **Theme toggle** (dark / light / auto, persist)
- **Time-range universal selector** (1h / 24h / 7d / 30d / 90d / all)
- **Screenshot/share per card** (html2canvas + deep-link URL with state)

All four exist in prod or partially — peg has visual depeg badge, theme is `data-theme="dark"` with `sora_theme` localStorage, time-range is per-chart (e.g. `/chart/:symbol?res=`, Network trend `/stats/network/trend?range=`), screenshot is done via `html2canvas@1.4.1` already loaded.

### C. Production limits that block cutover
1. **Swaps MV (7.2 GB) is never auto-refreshed** — live data gets stuck in `live_swaps`. A cutover must either expand disk or partition rolling (open ticket in backend before cutover).
2. **No per-client WebSocket rooms / auth** — prototype's peg alerts and wallet-specific notifications need `io.on('connection')` with rooms. Prod has broadcast-only socket.io.
3. **Per-user persistence absent** — no `users`, no `saved_portfolios`, no `alert_subscriptions` tables. Prototype's Backup/Restore handles this client-side but any multi-device-sync needs backend.
4. **No validator uptime table** — prototype would need a new aggregation job on `validator_blocks`.
5. **ETH bridge sender resolution disabled** (memory leak) — Bridges section's "From" column can't show ETH origin wallets.
6. **No API versioning** — no `/v1` prefix. Coexistence strategy: new UI calls same endpoints, or add `/v2/*` aliases.
7. **CSV export hard-capped at 50 000 rows** — UX should warn about this ceiling.
8. **Redis under-used** — `/tokens`, `/pools`, `/chart/:symbol`, `/history/global/*` use in-memory maps. Add Redis caching before scaling.

## Adaptation plan (local, phased)

**Goal**: fork the prototype into `/tmp/sorametrics-v6-work/sorametrics/v6-front/` as a new local working folder, keep prod untouched, iterate section-by-section.

### Phase 0 — Setup (30 min)
1. Copy `v6-prototype-snapshot/b6-snapshot/` → `v6-front/`
2. Rename `SoraMetrics Prototype.html` → `index.html`
3. Write a minimal `v6-server.js` (new Express app on port 3001) that proxies to the real backend at port 3000 — so the new front can fetch `/tokens` etc. without CORS drama
4. Verify render at `http://localhost:3001` matches the prototype snapshot
5. Commit: `feat(v6): scaffold local v6 front, proxy to prod backend`

### Phase 1 — Network Pulse wiring (2-3 h)
1. Replace prototype's mocked feed with real `socket.io` connection to prod backend: `io('http://localhost:3000')`.
2. Handle 5 events: `new-block-stats`, `transfers-batch`, `swaps-batch`, `extrinsics-batch`, `orderbook-batch`.
3. Preserve prototype's 40-event ring + filter chips (All / Swap / Transfer / Block / Order / Burn).
4. Add 6th event type "Burn" — synthesize client-side from `transactionPayment.TransactionFeePaid` (already in prod stream as fee events) OR add a new `burns-batch` WS event on the backend.
5. Commit: `feat(v6 pulse): wire real socket.io events into prototype pulse feed`

### Phase 2 — Burn Tracker + real-time animation (2 h)
1. Wire `/burns/stats/:sym`, `/burns/supply/:sym`, `/burns/supply-history/:sym`, `/burns/holders/:sym`, `/burns/fee-flow`.
2. Port prod's burn animation to the prototype's Burn Tracker page — trigger on new `new-block-stats` event.
3. Chart.js is already loaded in prototype — use `lightweight-charts` via CDN for supply history.
4. Commit: `feat(v6 burns): wire supply charts + burn animation to prod endpoints`

### Phase 3 — Balance / Portfolio (3-4 h)
1. Wire `GET /balance/:addr`, `POST /balances` (batch), `/currency-rates`, `/identity/:addr`.
2. Port prod's multi-currency (USD/EUR/XOR), hide-balances toggle, hide-low-balances, group-wallets settings into prototype Tweaks panel.
3. Wire Wallet Details modal: add the missing 7 sub-tabs (swaps, transfers, bridges, liquidity, staking, info, extrinsics) using existing `/wallet/*/:addr` + `/history/*/:addr` endpoints.
4. Wire Add Wallet → prototype's 3 tabs (Seed / Private key / Watch) already match prod's flow.
5. Commit: `feat(v6 balance): wire real wallet data + 8-subtab details modal`

### Phase 4 — Swaps / Transfers / Extrinsics / Bridges (4-5 h)
1. Each page wires to `/history/global/{swaps,transfers,extrinsics,bridges,orderbook,liquidity}` with pagination.
2. Drill panel already supports type-specific bodies — just swap mocked data for fetched rows.
3. CSV export — change prototype's generic CSV helper to call `/export/csv?format=sorametrics|koinly|cointracking|cointracker` (new UI: format selector modal).
4. Add Extrinsic Detail deep-link (`/history/extrinsic/:block/:idx`) + update drill panel EXTRINSIC body to fetch richer data.
5. Commit: `feat(v6 history): wire 5 real-time tables + CSV tax-format variants`

### Phase 5 — Tokens / Pools / Staking / Governance / Intelligence (3-4 h)
1. Tokens: `/tokens` for cards, `/chart/:symbol?res=` for big sparklines, `/holders/:assetId` for holder distribution.
2. Pools: `/pools` for list, `/pool/providers` + `/pool/activity` for inline-expand.
3. Staking: `/staking/validators` + `/staking/network` + `/staking/recent-blocks`.
4. Governance: 5 endpoints (council / elections / motions / democracy / technical-committee) for the 5 Spanish sub-tabs.
5. Intelligence: `/stats/accumulation` + `/stats/trending-tokens` + `/stats/stablecoins` + `/stats/fees` + `/stats/fees/trend` + `/stats/header` + `/stats/network/trend`.
6. Commit: `feat(v6 network): wire tokens/pools/staking/gov/intelligence to real data`

### Phase 6 — Global Search + Identities (1-2 h)
1. Replace prototype's mocked 50-item index with `/search?q=...` backend call.
2. Wire `/identity/:addr` to resolve the prototype's truncated addresses (fetch identity on drill open).
3. Commit: `feat(v6 search): wire real /search + identity resolution`

### Phase 7 — Missing prod features the prototype dropped (2 h)
1. Stablecoin peg monitor modal — Chart.js line + $1.00 reference, depeg badge — using `/stats/stablecoins` + `/burns/supply/*`.
2. Pool Details + Holder Detail + Chart modals — port from prod or build on top of drill panel.
3. Deep-link URLs for sections + entities (prod's `?tab=xxx&wallet=yyy` pattern).
4. Commit: `feat(v6 parity): pool details / peg monitor / deep-link URLs`

### Phase 8 — Batch 7 features (when Claude Design limit resets)
Either (a) fire Batch 7 in Claude Design to get the prototype version + port it, or (b) build them directly in the local v6-front now:
- Peg alerts (threshold inputs + localStorage + toast when triggered)
- Theme toggle (dark/light/auto) — port `sora_theme` from prod
- Time-range universal selector (1h/24h/7d/30d/90d/all) — port prod's per-chart pattern to all KPIs
- Screenshot/share per card — `html2canvas` already loaded

### Phase 9 — Cutover readiness
1. PWA manifest + service worker (copy prod's `sw.js`, bump to `sorametrics-v24`)
2. Production build (if Babel in-browser is too slow, swap to pre-compiled bundle via esbuild)
3. Verify Tailwind-free (both prod and prototype use hand-written CSS — good)
4. Deploy to `/beta` subpath on sorametrics.org VPS, run shadow traffic
5. DNS flip when metrics match prod

## Concrete next session (after your reply)

If you say "vamos con fase 0+1", I'll:
1. Set up `v6-front/` locally + new `v6-server.js` proxy on port 3001
2. Start prod backend locally (if it can run on your Mac) OR tunnel SSH to `sorametrics` VPS port 3000
3. Wire the first real data into the prototype (Network Pulse)
4. Hand back a working screenshot at `http://localhost:3001` with real live swaps flowing

Otherwise, if you prefer a different section first (e.g. "quiero ver Portfolio con datos reales"), I can reorder.
