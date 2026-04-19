# v6 — Session handoff (2026-04-19)

Living document for the **next session** to pick up exactly where this one ended. Read this first.

## TL;DR

- Branch: **`v6-ui`** on `github.com/Rubrum95/sorametrics`.
- Working dir: `/tmp/sorametrics-v6-work/sorametrics/`.
- v6 prototype lives in `v6-front/`. Local dev server: `node v6-server.js` → **http://localhost:3005** (proxies `/api/*` + `/socket.io` + everything else to `https://sorametrics.org`).
- **12 passes committed**, 18 of 20 catalogued gaps closed. Prototype is production-close but not cutover-ready.

## What's running right now (this laptop)

```bash
cd /tmp/sorametrics-v6-work/sorametrics/v6-front
export PATH="/opt/homebrew/bin:$PATH"
node v6-server.js   # port 3005 by default
```

If the server is dead when you return:
```bash
pkill -f 'node v6-server.js' 2>/dev/null
cd /tmp/sorametrics-v6-work/sorametrics/v6-front && node v6-server.js &
```

Verify with `curl -s http://localhost:3005/api/version` → `{"version":"v4.0"}`.

## Commit timeline (newest first)

| Pass | SHA | Scope |
|------|-----|-------|
| 10 | `c11d7ce` | Wallet modal 540→820 + real token logos in swaps/transfers/bridges/wallet-history |
| 9  | `f2cb2b9` | Fix Portfolio crash (useT) + WalletDetailsProvider app-wide + click-wallet-opens-modal from swap/transfer rows |
| 8  | `2081b41` | Holders wired to `/holders/:assetId` · Tokens Mcap→Volume 24h + real sparklines · Balance allocation from real wallet tokens · server error-handler guards WS upgrade errors |
| 7  | `0d17b4b` | Music `/music/list` real tracks + `<audio>` playback · Pulse time-range pills (24H/7D toggle) |
| 6  | `37ca067` | ↗ label restored (was 🔍 Más Info) · Transfers drill real hash · G7 time-range pills in Balance + Burns |
| 5b | `1224901` | Peg history Chart.js line in Intelligence |
| 5a | `da647ca` | Drill parity with prod "Detalles del Extrinsic" (Extrinsic ID/Hash/Firmante/Valor USD lookup/Arguments/Events raw toggle) |
| 4  | `ec54141` | Wallet Details 8 sub-tabs · Extrinsic detail drill · CSV tax formats modal · Governance 5 sub-tabs |
| 3  | `04a9b3f` | Pulse KPIs + Trending + Network Health wired to real stats |
| 2  | `ff0fe47` | Pools DEX filter pills (XOR/XST/KUSD/VXOR) · Reservas column · Providers/Activity modals · server-side pagination |
| 1  | `f7034ee` | Pulse wired to real prod socket.io (5 events) |
| 0  | `350f78a` | v6-front scaffold + proxy server |

Pre-audit docs (before Pass 2):
- `fd8cd55` feature audit + porting plan
- `e77fab7` GAP_ANALYSIS.md
- `831f031` B6 Claude Design snapshot

## Sections status

| Section | Live data? | Open issues |
|---------|------------|-------------|
| Pulse (Network) | ✅ 5 socket.io events + KPIs + Trending + Health + Time-range | — |
| Burn Tracker | ✅ `/burns/stats`, `/burns/fee-flow`, `/burns/holders` + tf selector (24h/7d/30d/All) | BurnChart sparkline still seeded (no real history endpoint) |
| Intelligence | ✅ `/stats/accumulation`+`/trending`+`/stablecoins` + **peg history Chart.js** + **Peg Monitor widget** | insight cards are derived client-side, not from prod-baked endpoints |
| Swaps | ✅ `/history/global/swaps` + DEX filter + **real token logos** + click-wallet opens modal | — |
| Extrinsics | ✅ `/history/global/extrinsics` + real drill with `/history/extrinsic/:block/:idx` + `/lookup/usd-value/:id` | — |
| Transfers | ✅ `/history/global/transfers` + **logos** + click-wallet | — |
| Bridges | ✅ `/history/global/bridges` + logo on asset | ETH bridge sender resolution disabled at backend (memory leak) |
| Order Book | 🟡 fills real from `/history/global/orderbook`, **bids/asks still mocked** | prod has no live book snapshot endpoint |
| Pools | ✅ `/pools?base=` with 4 DEX pills + Providers + Activity modals (real /pool/providers + /pool/activity) + 217 pools paginated | Volume24h/APR/Providers count not exposed by `/pools` — deferred |
| Tokens | ✅ `/tokens` with real price + change24h + **real sparklines** | Total Mcap replaced with Volume 24h (prod doesn't expose mcap here) |
| Holders | ✅ `/holders/:assetId` with XOR/VAL/PSWAP/KUSD pills · 206 holders · server-paginated 9 pages | — |
| Staking | ✅ `/staking/validators` + `/staking/network` — 24 real validators | no validator uptime heatmap (needs new backend agg) |
| Governance | ✅ 5 sub-tabs wired (Consejo/Elecciones/Mociones/Democracia/Comité Técnico) | — |
| Balance/Saldo | ✅ real net worth + real allocation from wallet tokens + time-range pills | — |
| Cartera (Portfolio) | ✅ USD/EUR/XOR + 4 wallet cards + allocation + holdings (fixed Pass 9) | uses static PF_HOLDINGS not real /balance aggregate — TODO |
| Wallet Details modal | ✅ 8 sub-tabs (Assets/Swaps/Transfers/Bridges/Liquidity/Staking/Extrinsics/Info) all fetch per-addr endpoints · opens from any row · 820px wide | Liquidity/Staking/Info tabs show raw JSON — pretty rendering pending |
| Cmd+K palette | ✅ `/search?q=` real hits | — |
| Music player | ✅ `/music/list` 10 real tracks + real `<audio>` playback | — |

## Known limitations / still open

**Hard blockers (would need backend work):**
- **Swaps MV never auto-refreshes** in prod (7.2 GB, disk capacity). Live data stuck in `live_swaps` table. Not our code's fault — backend limitation.
- **No live orderbook snapshot endpoint** — bids/asks in OrderBook section stay mocked.
- **No validator uptime history** — `/staking/validators` only exposes current era. Would need a new aggregation table on the backend.
- **ETH bridge sender resolution disabled** (memory leak in `index.js:17-19`). Bridges "From" column can't resolve ETH→SS58 mapping.
- **CSV export capped at 50 000 rows** in prod (`index.js:2335`).

**Cosmetic / polish (low priority):**
- Favicon manifest points to `/beta/favicon.svg`; in local dev at port 3005 there's a 400 warning (harmless).
- Light mode drift — CSS variables mostly tested in dark only.
- BurnChart uses seeded random sparkline (real `/burns/supply-history` is 18-decimal raw + varying schedule — needs lightweight-charts work).

## Key architectural decisions (read before touching)

1. **Router** is `?tab=X` in the URL + `window.__SM_NAV__(section)` global setter. Sidebar + deep-links both flow through it.
2. **WalletDetailsProvider** wraps the app (`main.jsx`), exposes `window.openWalletDetails(addr, alias?)` → any row/button can pop the modal. Modal fetches `/balance/:addr` on the fly when the addr isn't in the local wallet store.
3. **`useHistory(endpoint, opts)`** (in `common.jsx`) is the generic fetch hook for any `/history/global/*`. Handles pagination params, polling, parseHistTime for prod's Spanish date strings.
4. **TimeRangePills / useTimeRange / TIME_RANGES** in `routes.jsx`, exposed globally for any section to use. Backing store: `localStorage.sm.timeRange`.
5. **Token logos**: both `TokenLogo` (swaps) and `TokenBadge` (routes) accept optional `logo` prop. Pass the base64/URL from prod; fall back to gradient-initial placeholder automatically.
6. **Proxy**: `v6-server.js` uses `createProxyMiddleware({ pathFilter })` — NOT prefix-mount (prefix-mount strips the path and returns SPA HTML). Error handler guards WS upgrade errors where the 2nd arg is a `net.Socket` instead of HTTP `res`.
7. **Real addresses in seed**: `INITIAL_WALLETS` in `features.jsx` uses 4 real SS58s (Polkaswap Treasury, XOR Whale, DEX Maker, Active Trader) so the prototype shows live data out of the box.

## Files most modified

- `v6-front/js/routes.jsx` (huge — Transfers, Bridges, OrderBook, Pools, Tokens, Holders, Staking, Governance, Balance, Intelligence, PegHistoryChart, Pool modals)
- `v6-front/js/features.jsx` (WalletProvider, WalletDetailsProvider, WalletDetailsModal, ToastProvider, GlobalSearchProvider, CSV tax modal, openWalletDetails helper)
- `v6-front/js/drill_music.jsx` (DrillPanel 11 type bodies + ExtrinsicDetail with Extrinsic ID / Hash / Firmante / Valor USD / args / events · MusicPlayer with /music/list)
- `v6-front/js/pulse.jsx` (socket.io 5 events + real stats KPIs + Trending + Network Health + Time-range pills)
- `v6-front/js/swaps.jsx` (TokenLogo with logo prop + wallet click + drill)
- `v6-front/js/burns.jsx` (real /burns/* + tf selector wired)
- `v6-front/js/extrinsics.jsx` (Extrinsics section with drill pass-through)
- `v6-front/js/portfolio.jsx` (useT fix)
- `v6-front/js/tweaks.jsx` (theme dropdown, peg alert threshold slider)
- `v6-front/js/main.jsx` (App + deep-link URL routing + peg-watcher + SW register + WalletDetailsProvider wrap)
- `v6-front/js/common.jsx` (fmt, useHistory, parseHistTime)
- `v6-front/js/i18n.jsx` (14-lang dictionary, LangProvider, useT, LangPicker)
- `v6-front/v6-server.js` (Express + http-proxy-middleware with path filter + WS upgrade)
- `v6-front/index.html` (entry + React/Babel/socket.io/Chart.js CDN)
- `v6-front/sw.js`, `manifest.json`, `DEPLOY.md` (PWA + /beta subpath deploy)

## Docs in `v6-prototype-snapshot/`

- `V6_BUILD_LOG.md` — original 9-phase plan (phases 0-9)
- `GAP_ANALYSIS.md` — prod vs prototype gap analysis (20 gaps catalogued)
- `V6_GAPS.md` — pass-by-pass status of the 20 gaps
- `STATUS.md` — Claude Design B6 snapshot status (pre-audit)
- `FEATURE_AUDIT.md` + `PORTING_PLAN.md` — early feature audit docs
- **`SESSION_HANDOFF.md`** ← this doc

## Suggested next moves (pick ANY)

1. **Pretty-print Liquidity/Staking/Info tabs** inside Wallet Details modal (currently show raw JSON).
2. **Port PF_HOLDINGS** (PortfolioSection) to real `/balance/:addr` aggregate across user's wallets (same treatment as BalanceSection).
3. **BurnChart** → swap seeded sparkline for real `/burns/supply-history/:sym` using lightweight-charts.
4. **Time-range propagation** to Intelligence/Tokens/Holders KPIs.
5. **Deploy trial** to `sorametrics.org/beta/` via rsync + nginx per `v6-front/DEPLOY.md`.
6. **Claude Design Batch 7** (peg alerts/theme/time-range/screenshot) — prototype was rate-limited at pass 6 in Claude Design; can resume when limit resets (likely sábado 8:00 from that session).

---

## PROMPT FOR NEXT SESSION

Copy-paste this in a new Claude Code session to resume:

> Trabajemos en sora/sora-aitai**no wait** — **sorametrics v6**. Branch `v6-ui` del repo local `/tmp/sorametrics-v6-work/sorametrics/`. Lee primero `v6-prototype-snapshot/SESSION_HANDOFF.md` — tiene la timeline de 12 commits, estado de cada sección, arquitectura clave y candidatos para el próximo pass. El dev server corre en `node v6-front/v6-server.js` puerto 3005 proxeando a sorametrics.org. Último commit fue `c11d7ce` (Pass 10, wallet modal 540→820 + real token logos). Sigue cerrando gaps según el documento — empieza verificando que el server sigue vivo con `curl -s http://localhost:3005/api/version` y luego decide qué gap atacar de la lista "Suggested next moves" del handoff.

Shortcut: just say **"continuamos con sorametrics v6, lee SESSION_HANDOFF.md"** and I'll read the doc + pick up.
