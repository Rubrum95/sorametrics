# SoraMetrics — Feature Audit (2026-04-18)

> Inventory of production features in `main` that must survive the v6 UI rewrite.
> This document is the contract: when porting from the Claude Design prototype
> into this codebase, every item below must end up working (or explicitly
> marked "deferred to v6.1" in the PORTING_PLAN).

Audited commit baseline: last `main` at v6-ui branch creation.

---

## 1. Top-level sections (sidebar navigation)

| Section | data-section | Present in main | Present in v6 prototype |
|---------|--------------|-----------------|-------------------------|
| Swaps | `swaps` | ✓ | ✓ (Live Swaps table added in pass 2) |
| Transfers | `transfers` | ✓ | ⏳ scaffold pending |
| Extrinsics | — (Wallet wtab-extrinsics) | ✓ | ⏳ pending (critical: raw tx view) |
| Bridges | — (Wallet wtab-bridges) | ✓ | ⏳ pending |
| Order Book | — (Wallet wtab + section) | ✓ | ⏳ pending |
| Pools / Liquidity | `liquidity` | ✓ | ⏳ pending |
| Tokens | `tokens` | ✓ | partial — "Tokens" in sidebar |
| Holders | — | ✓ | partial — top-holders card in Burn |
| Staking (Validators · Network Info) | stakingtab-* | ✓ | partial — sidebar link only |
| Governance (Consejo · Elecciones · Mociones · Democracia · Tech Committee) | govtab-* | ✓ | partial — sidebar link only |
| Burns | `burns` | ✓ | ✓ (Burn Tracker with furnace) |
| Portfolio (Overview · My Wallets · Watched) | btab-* | ✓ | ✓ (simplified) |
| Intelligence | `section-intelligence` | ✓ | — |
| Network Pulse (aggregated live feed) | — | new in v6 | ✓ |

## 2. Governance sub-tabs
- Consejo (`govtab-council`) — members + motions
- Elecciones (`govtab-elections`)
- Mociones (`govtab-motions`)
- Democracia (`govtab-democracy`) — proposals + referendums
- Comité Técnico (`govtab-techcommittee`) — members + motions

## 3. Staking sub-tabs
- Validators (`stakingtab-validators`) — active validator list with metrics
- Network Info (`stakingtab-network`) — chain-wide stats (total stake, era progress, etc.)

## 4. Portfolio sub-tabs
- Overview (`btab-overview`) — aggregate net worth across tracked wallets
- My Wallets (`btab-mywallets`) — user-added wallets with balances
- Vigiladas / Watched (`btab-watched`) — read-only observed wallets

## 5. Wallet-detail tabs (inside `walletDetailsModal`)
- Assets (`wtab-assets`)
- Swaps (`wtab-swaps`)
- Transfers (`wtab-transfers`)
- Bridges (`wtab-bridges`)
- Liquidity (`wtab-liquidity`)
- Staking (`wtab-staking`)
- Info (`wtab-info`)
- Extrinsics (`wtab-extrinsics`)

## 6. Modals (21 in main)
```
addWalletModal         — single + bulk add-wallet form
backupModal            — export/import user settings JSON
blockModal             — block detail view (hash, author, extrinsics, events)
chartModal             — chart fullscreen + export
csvExportModal         — date range + wallet filter + type + format
extrinsicDetailModal   — decoded JSON + events + fee split
holderModal            — top-holder detail + portfolio
poolDetailsModal       — pool providers + activity
txModal                — transaction detail (generic)
walletDetailsModal     — wallet deep view with 8 sub-tabs
langDropdown           — language selector (14 languages)
```
Plus internal action containers: blockModalActions, extrinsicModalActions,
poolModalActions, txModalActions, walletModalActions, holderModalActions,
chartModalActions.

## 7. i18n

**Default language**: Spanish (es). UI strings are in Spanish by default
("Consejo", "Elecciones", "Mociones", "Mis Wallets", "Vigiladas",
"Copia de Seguridad", "Bloque Actual", etc.).

**Supported languages**: es, en, ru, ja, zh, ko, de, fr, it, pt, ar, he,
ur, vi (14 locales).

**RTL**: he (Hebrew) and ur (Urdu) render in RTL. The CSV export modal has
explicit RTL adjustments (see `index.html:2938-2941`).

**Implementation**: i18n keys are declared inline via `data-i18n="key"`
attributes. Translations table lives in `script.js::TRANSLATIONS`. There are
~130 distinct i18n keys.

## 8. Music player (signature UX)

- Trigger: floating button bottom-left, pulses when playing (`.music-btn.playing`
  + `@keyframes musicPulse 2s ease-in-out infinite`).
- Panel: draggable, visible toggle via `.music-player-panel.visible`.
- Contents:
  - `#musicTrackTitle`, `#musicTrackArtist`
  - `#musicPlayBtn` (play/pause, paused state has distinct icon padding)
  - Previous / Next controls (`.music-nav-controls`)
  - `#musicWaveform` + `#musicCurrentTime` + `#musicDuration`
  - `#musicVolume` slider
  - `#musicPlaylist` — expandable via `.music-playlist-toggle`
  - Close button `.music-close-btn`
  - Background: `musicStarTwinkle 3s ease-in-out infinite` ambient stars
  - Pseudo-elements `::before` and `::after` for layered gradients

## 9. External libraries

| Lib | Purpose | Version seen |
|-----|---------|--------------|
| chart.js | All chart rendering | 4.4.1 |
| bignumber.js | Big-integer arithmetic for balances | 9.1.1 |
| html2canvas | Screenshot / card-to-PNG export | 1.4.1 |
| socket.io client | Real-time feed from backend | bundled `/socket.io/socket.io.js` |

## 10. CSV Export workflow
- Trigger: icon in header → opens `csvExportModal`.
- Fields: date-from, date-to, wallet filter (None / Mine / Watched),
  type checkboxes (Swaps, Transfers, Extrinsics, Bridges, Orders, Staking),
  format (CSV / JSON), Download button.
- Loc keys: `csv_export_title`, `csv_date_from`, `csv_date_to`,
  `csv_filter_none`, `csv_filter_mine`, `csv_filter_watched`,
  `csv_select_types`, `csv_select_wallets`, `csv_format`,
  `csv_format_hint_sm`, `csv_download`.

## 11. Backup / Restore
- Trigger: header icon (titulo ES "Copia de Seguridad").
- Actions: download JSON backup of user settings / watchlist / aliases / favorites;
  restore from a previously downloaded JSON.
- Loc keys: `backup_title`, `backup_desc`, `btn_download_backup`, `btn_restore_backup`.

## 12. Wallet management
- Add wallet modal (`addWalletModal`) with two modes:
  - Single (`#mode-single`, default): address + alias
  - Bulk (`#mode-bulk`): multi-line paste, max 50 addresses, import result
    alert shows `added` / `errors`
- Aliases / naming (`name_alias`)
- Favorites (`star` toggle per token/wallet)
- Two separate lists: My Wallets vs Watched

## 13. Dynamic tabs bar
- `#dynamicTabsContainer` — user-configurable pinned sections above the main
  content area.
- Max 5 simultaneous. Attempting to add a 6th triggers the loc-keyed alert
  `max_sections_alert` ("Máximo 5 secciones permitidas. Desactiva una
  primero.").

## 14. Global search (present in code, may not be mounted)
- `#globalSearchBar` with Escape + Enter key handlers.
- Target scope: wallets, tx hashes, blocks, extrinsics, tokens.

## 15. Theme switching
- `[data-theme="dark"]` selector-based overrides in CSS. `data-theme`
  attribute on the `<html>` or `<body>` toggles.
- `<meta name="theme-color" content="#9B1B30">` — browser chrome color.

## 16. Peg alerts (stablecoin monitor)
- `.peg-alert` component. Loc key `stablecoin_monitor`. Intended to surface
  when KUSD or TBCD deviates from $1 by > 2%.

## 17. Pagination (per-table)
Every data table has its own pagination controls with First/Prev/Next/Last
buttons (IDs like `btnSwapFirst`, `btnTransferNext`, `btnExtrinsicLast`,
`btnBridgePrev`, `btnPoolNext`, `btnOrderbookLast`, `btnTokenPrev`, etc.).
`page_x_of_y` is the standard loc key.

## 18. Scripts / tooling (non-frontend)
`scripts/` directory holds operational tooling (not part of the served UI):
- `backfill_price_history.js`
- `backfill_xor_supply.js`
- `check_coverage.js`
- `create_materialized_views.sql`
- `debug_dai.js`, `fix_dai.js`
- `load_asset_registry.js`

These are backend-ops scripts and stay outside the v6 UI migration scope.

## 19. Brand identity tokens

| Token | Value |
|-------|-------|
| Primary red | `#9B1B30` (meta theme-color) |
| Gradient stops | `#9B1B30 → #7B2D5B → #7B5B90 → #8B80B5 → #C8A0B8 → #E0C8D5` |
| Fonts | Inter (UI), JetBrains Mono (numerics) |
| Cards | 16px radius, 3px gradient top accent, subtle shadows |
| Background | Sakura petal animation (site-wide) |

## 20. Missing-in-prototype summary (what porting must preserve)

From the production app, the v6 Claude Design prototype currently omits:
1. Extrinsics section (raw tx view with decoded JSON)
2. Bridges section
3. Order Book section
4. Holders section (top-holder drill-down)
5. Staking sub-tabs (Validators + Network Info)
6. Governance sub-tabs (Consejo, Elecciones, Mociones, Democracia, Tech)
7. Intelligence section
8. Music player (critical UX)
9. i18n with 14 locales + Spanish default + RTL
10. CSV export modal
11. Backup / restore JSON
12. Wallet management (add single/bulk, My / Watched, aliases, favorites)
13. Wallet detail modal with 8 sub-tabs
14. Global search with ⌘K
15. Dynamic tabs bar (user-pinned max 5)
16. Peg alert banner
17. Per-card screenshot / share
18. Theme light/dark toggle exposed to user
19. Universal time-range selector (24h / 7d / 30d / All / custom)
20. All 21 modals in specific detail

The PORTING_PLAN.md documents how and when each of these lands in v6.
