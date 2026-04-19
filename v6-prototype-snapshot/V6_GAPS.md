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

## Pass 2 fix order (committed `ff0fe47`)

1. ✅ G1: Pools DEX filter pills + `?base=` wiring
2. ✅ G2: Pools row layout (Reservas + Total USD, no APR/Providers count)
3. ✅ G3: Pools server-side pagination
4. ✅ G4: Pool Providers modal
5. ✅ G5: Pool Activity modal

## Pass 3 fixes (pending commit)

6. ✅ G13: Pulse KPI cards wired to `/stats/header` (229 swaps · $2.69K vol) + `/stats/network` (19 active wallets) + `/staking/network` (6.00s block time)
7. ✅ G14: Trending Tokens card wired to `/stats/trending-tokens` (DAI, XOR, VAL, PSWAP, KUSD by real 24h volume)
8. ✅ G15: Network Health card wired to `/staking/network` (24 validadores · Era #7052 · 50% progress · finality lag · TPS)

## Pass 4 fixes (`ec54141`)

- ✅ G8: Wallet Details **8 sub-tabs** (Assets/Swaps/Transfers/Bridges/Liquidity/Staking/Extrinsics/Info)
- ✅ G9: Extrinsic drill body with `/history/extrinsic/:block/:idx`
- ✅ G10: CSV modal with SoraMetrics / Koinly / CoinTracking / CoinTracker formats
- ✅ G12: Governance 5 sub-tabs all real

## Pass 5 fixes (`da647ca` + peg chart commit)

- ✅ G6: **🔍 Más Info** button per row in Extrinsics + Swaps, passing block+idx+extrinsic_id so drill fetch fires with full context
- ✅ **Drill parity with prod "Detalles del Extrinsic" modal**: Extrinsic ID + Hash (full, not truncated) + Block + Pallet + Firmante + Resultado + Hora + **Valor USD (al momento de TX)** via `/lookup/usd-value/:id` + Arguments JSON (pretty-printed + copy) + Events · count with "raw JSON" toggle
- ✅ G11: **Peg history Chart.js line** — KUSD / XSTUSD / TBCD plotted. Client-side rolling ring of /stats/stablecoins snapshots stored in localStorage (`sm.pegHistory`, capped at 120 samples).

## Pass 6 fixes (pending commit)

- ✅ User feedback: reverted **🔍 Más Info** label → **↗** (original prototype aesthetic preferred)
- ✅ Transfers drill: dropped synthesized `hash: '0x' + Math.random()...'` — now passes the real `r.hash` from prod
- ✅ G7 **Time-range pills**:
  - BalanceSection Overview now renders the 5 pills (4H/1D/7D/1M/1Y), persisted in `localStorage.sm.timeRange`
  - Net worth computation switched from fake `.value` sum to real sum of `(w.tokens.usdValue)` from prod
  - Burns Tracker `24h/7d/30d/All` segmented buttons now functional: click 24h → 5.60 XOR burned (real), 7d → 56.83, 30d → 209.19
  - Shared `TIME_RANGES` constant + `useTimeRange` + `TimeRangePills` component (reusable in future sections)

## Pass 7 fixes (pending commit)

- ✅ **G17 Music player real tracks**: `/music/list` fetched, fallback playlist only used if endpoint fails. Real `<audio>` element plays the returned mp3s. First track "20-Twenty-What · SoraMetrics Radio · 6:24" loads with metadata-driven duration.
- ✅ **G7 Pulse time-range extension**: TimeRangePills added to Pulse PageHeader. KPIs (SWAPS / VOLUME / ACTIVE WALLETS) flip between `stats24h` and `stats7d` from `/stats/network` on range change. Labels update to `· 24H` or `· 7D`. Verified: 24H=259 swaps/$2.91K, 7D=2255 swaps/$31.29K.
- ✅ Shared `TimeRangePills` + `useTimeRange` exposed globally so future sections can reuse.

## Still open (Pass 8 candidates)

- Propagate time-range to other sections (Intelligence, Tokens, Holders) + `/stats/network/trend?range=` once prod supports it
- G16: favicon doesn't round-trip scope=/beta/ properly in dev
- G18: backup JSON schema interop with prod
- G20: light mode drift testing
