# v6 Gaps — Pass 2 (audit prod vs v6-front, 2026-04-19)

Audit performed by navigating `https://sorametrics.org/` tab-by-tab and comparing live feature surface to `v6-front/`. Focused on **missing controls + non-functional buttons + stale data**, not cosmetic.

## HIGH priority (touched in Pass 2)

### G1 — Pools: DEX filter pills (user-reported)
Prod has **5 pills above the pool list**: `Todo` (default) + 4 token logos with `title` = `"XOR (DEX 0)"`, `"XST (DEX 1)"`, `"KUSD (DEX 2)"`, `"VXOR (DEX 3)"`. Clicking a pill calls `/pools?base=XOR|XST|KUSD|VXOR`. SORA's AMM exposes 4 base-asset DEXes. Our v6 has no filter at all.

**Fix**: Add pill row in `PoolsSection`, wire to `useState` + pass `?base=` via useHistory endpoint rebuild on change.

### G2 — Pools: show raw Reservas + per-pool Total USD (green), not TVL/Vol/APR/Providers
Prod row layout:
- Token-pair double-logo + `KUSD-PSWAP` label
- `4120.29 KUSD` + `19,422,019.1 PSWAP` (raw reserves, both assets)
- `Total: $8102.49` (sum in green)
- `Providers` button → modal
- `Activity` button → modal

Our v6 row: TVL/24H Volume/APR/Providers columns — Volume/APR/Providers all 0 because `/pools` doesn't expose them. Prod doesn't pretend to show them either — it shows reserves + TVL.

**Fix**: Rewrite Pools row to match prod columns.

### G3 — Pools: pagination "Página 1 de 22" (22 pages, 10 per page)
Prod paginates server-side via `?page=N&limit=10`. Our v6 fetches all with `pageSize=0` and client-paginates the 10 we get. Prod returns ~220 pools total.

**Fix**: Request pages from server, show real page count.

### G4 — Pool Providers modal (Providers button → LP list)
Prod hits `/pool/providers?base=X&target=Y` returning LP addresses + stake + share%. Our v6 has no modal.

**Fix**: Add `PoolProvidersModal` component, wire button.

### G5 — Pool Activity modal (Activity button → event log)
Prod hits `/pool/activity?base=X&target=Y` returning swap/add/remove events on that pool. Our v6 has no modal.

**Fix**: Add `PoolActivityModal` component, wire button.

### G6 — Row action: "🔍 Más Info" button per row (Swaps/Transfers/Extrinsics/Bridges)
Prod has a dedicated magnifier button per row. We have a `↗` drill-open button but it opens a generic drill with partial data. Prod's Más Info opens a much richer per-row detail view using `/history/extrinsic/:block/:idx` or `/lookup/usd-value/:id`.

**Fix**: Label our drill button consistently + pull richer data on open.

## MEDIUM priority (Pass 3 candidate)

### G7 — Time-range selector `4H / 1D / 7D / 1M / 1Y`
Prod exposes a universal time-range pill row in Balance (visible in screenshot). Selecting one filters all charts + KPIs in that view. Our v6 has no time-range control — sparklines + KPIs use fixed data.

### G8 — Wallet Details modal sub-tabs (8 of 8)
Prod has `#walletDetailsModal` with 8 sub-tabs: Assets / Swaps / Transfers / Bridges / Liquidity / Staking / Info / Extrinsics. Our v6 modal only has Assets tab.

### G9 — Extrinsic Detail modal with decoded JSON args
Prod has `#extrinsicDetailModal` (fetch `/history/extrinsic/:block/:idx`) with expandable args_json, events, fee breakdown. Our drill panel EXTRINSIC body shows limited data.

### G10 — CSV tax formats (sorametrics / koinly / cointracking / cointracker)
Prod exposes `/export/csv?format=...` with 4 formats. Our v6 has a generic format only.

### G11 — Stablecoin peg chart (Chart.js line with $1 reference)
Prod renders a full peg history chart in Intelligence using `/burns/supply/:sym` + historical prices. Our v6 has the peg bar widget but no chart.

### G12 — Governance Elections / Democracy / Tech-committee sub-tabs (fully wired)
Our v6 has `council` + `motions` wired. Elections/Democracy/Tech committee still use mock data.

### G13 — Pulse KPI cards (Swaps 24h / Volume 24h / Active Wallets / Avg Block Time)
Our v6 KPIs are still mocked with `14,208 / $4.27M / 2,810 / 6.01s`. Prod pulls from `/stats/header` + `/stats/network/trend` + `/stats/overview`.

### G14 — Trending Tokens sidebar card in Pulse
Our v6 has hardcoded `[XOR, VAL, PSWAP, TBCD, KUSD, ETH]`. Prod pulls from `/stats/trending-tokens`.

### G15 — Network Health card in Pulse
Validators online / Peers / Era Progress / Finality Lag / TPS — all hardcoded. Prod pulls from `/staking/network`.

## LOW priority (polish)

### G16 — Favicon prototype uses rebranded SVG, prod has Sora logo
### G17 — Music player panel uses sakura stars + mock tracks — prod fetches `/music/list`
### G18 — Backup JSON schema may not match prod's for interop
### G19 — No service-worker in dev — only registers in prod; fine as designed
### G20 — Theme palette colors may drift between light/dark modes; light mode mostly untested

## Endpoint cheatsheet

| Prod endpoint | Our v6 usage |
|---|---|
| `/pools?base=X&page=N&limit=10` | Partially wired, no `base` param, no pagination |
| `/pool/providers?base=X&target=Y` | Not wired |
| `/pool/activity?base=X&target=Y` | Not wired |
| `/history/extrinsic/:block/:idx` | Not wired (needed for Extrinsic Detail) |
| `/lookup/usd-value/:extrinsicId` | Not wired (needed for Swaps Más Info) |
| `/stats/header` + `/stats/network/trend` + `/stats/overview` | Not wired (Pulse KPIs) |
| `/wallet/liquidity/:addr` + `/wallet/staking/:addr` + `/wallet/info/:addr` | Not wired (Wallet Details sub-tabs) |
| `/history/{swaps,transfers,bridges,orderbook,extrinsics}/:addr` | Not wired (per-wallet history) |
| `/export/csv?format=...` | Wired with generic CSV only |

## Pass 2 fix order

1. ✅ G1: Pools DEX filter pills + `?base=` wiring
2. ✅ G2: Pools row layout (Reservas + Total USD, no APR/Providers count)
3. ✅ G3: Pools server-side pagination
4. ✅ G4: Pool Providers modal
5. ✅ G5: Pool Activity modal
6. 🔜 G6–G15: Pass 3
