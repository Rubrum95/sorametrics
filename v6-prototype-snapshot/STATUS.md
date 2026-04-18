# SoraMetrics v6 — Claude Design Prototype Snapshot

**Snapshot date:** 2026-04-18 16:29 (batch B6 shipped, B7 blocked on Claude Design usage limit reset — sáb 8:00).

**Prototype URL:** https://claude.ai/design/p/c806f352-b1da-42e8-98ff-1913b7607edd

## What's in this folder

- `sorametrics-prototype-b6-20260418-162912.zip` — raw zip downloaded via Claude Design's **Export → Download project as .zip**. Canonical backup.
- `b6-snapshot/` — unpacked copy of the zip. 29 files totaling ~940KB:
  - `SoraMetrics Prototype.html` — React bootstrap shell.
  - `styles.css` — 81KB, all section + drill + music + i18n + modal chrome.
  - `js/i18n.jsx` — **78KB**, dictionary of ~170 keys × 14 languages + LangProvider + useT + LangPicker + Intl locale routing.
  - `js/routes.jsx` — **67KB**, Transfers / Bridges / OrderBook / Pools / Tokens / Holders / Staking / Governance / Balance / Intelligence sections.
  - `js/features.jsx` — **30KB**, ToastProvider + GlobalSearchProvider (⌘K) + AddWalletModal + WalletDetailsModal + ConfirmModal + toCsv helper + BackupRestore.
  - `js/drill_music.jsx` — **31KB**, DrillPanel (11 type-specific bodies) + MusicPlayer (draggable, waveform, playlist, sakura stars).
  - `js/swaps.jsx`, `extrinsics.jsx`, `burns.jsx`, `pulse.jsx`, `portfolio.jsx`, `common.jsx`, `shell.jsx`, `main.jsx`, `tweaks.jsx`.
  - `scraps/` — verification screenshots Claude Design produced while building (ES/EN/JA/AR Staking renders, governance validation).

## Features shipped (B1–B6)

### B1 — Visual identity
- Burgundy/plum palette softened (gradient `#9B1B30 → #7B2D5B → #7B5B90 → #8B80B5 → #C8A0B8 → #E0C8D5`).
- Logo + theme color preserved from production sorametrics.

### B2 — Extrinsics section
- 4 KPIs (24h count, success rate, avg fee XOR+USD, top pallet), 12 pallet dropdown, success/failed toggle, datetime-local picker.
- 15 mock rows, ~15% failures with decoded reasons. Inline-expand: decoded JSON args + events-emitted chips grouped by pallet + fee breakdown.

### B3 — 10 sections + router
- Transfers, Bridges, Order Book (bid/ask + spread + recent fills), Pools (inline-expand → top-10 LPs), Tokens (grid with sparklines + favorites), Holders (top 20), Staking (Validators + Network Info sub-tabs, 20 validators), Governance (5 Spanish sub-tabs: Consejo / Elecciones / Mociones / Democracia / Comité Técnico), Balance (3 sub-tabs, Overview + Mis Wallets + Vigiladas), Intelligence (severity-ribboned cards).
- Sidebar grouped Featured / Network / My. Tweaks panel SECTION selector now scrollable across all sections. Router falls through to Burns only for unknown ids.

### B4 — Drill panel + Music player
- Right-anchored 440px slide-in panel (180ms ease-out) with 11 type-specific bodies (SWAP, TRANSFER, BLOCK, ORDER, BURN, EXTRINSIC, LP, HOLDER, VALIDATOR, BRIDGE, FEED). Copy-to-clipboard on I105s + hashes. Timestamps relative + UTC + local.
- Floating music button bottom-left, draggable 320×230 panel (grows to 320×390 with playlist). 6 mocked tracks with waveform seek, elapsed tick, auto-advance. 5 drifting sakura stars. Layered plum→burgundy gradient.
- Verifier self-fixed 2 layout bugs (music button covering Portfolio, council name wrap).

### B5 — i18n full (14 languages + RTL)
- Dictionary of ~170 keys in: es (DEFAULT), en, fr, de, it, pt, ru, zh, ja, ko, ar (RTL), he (RTL), ur (RTL), hi.
- LangPicker burgundy pill top-right, flag + 2-letter code, popover with native names.
- RTL: `document.dir="rtl"` + `body.rtl`, sidebar flips right, drill panel slides from left, music player mirrored. Numbers stay LTR via `.num { direction: ltr; unicode-bidi: embed }`.
- Governance sub-tabs (Consejo/Elecciones/Mociones/Democracia/Comité Técnico) + Balance sub-tabs (Mis Wallets/Vigiladas) stay in Spanish for all 14 langs (proper names).
- Verified ES/EN/JA/AR on Staking. Rest of sections have framework in place + dictionary keys defined but some labels still hardcoded (documented below).
- Pagination hook bug (`t('pag.*')` calls without `useT()`) was auto-detected via console logs and fixed.

### B6 — Global Search + Wallet modals + CSV + Backup
- **⌘K / Ctrl-K global palette**: fuzzy match across 50-item mock index (wallets, tx hashes, blocks, extrinsics, tokens, pools, validators), keyboard nav, type-filter tabs, recent-queries persisted in `localStorage`. Also fires when clicking top-bar search pill.
- **Add Wallet modal** (3 tabs): Importar seed (12/24 grid + paste), Clave privada (warning banner), Solo watch (I105 + alias). Toast on add.
- **Wallet Details modal**: rename alias, copy address, asset breakdown bars, delete with inline confirm sub-modal.
- **CSV export**: wired on Extrinsics, Swaps, Transfers, Bridges, Order Book fills, Pools, Tokens, Holders, Staking, Governance, Portfolio. Filename pattern `sorametrics_{section}_{YYYYMMDD_HHMMSS}.csv`.
- **Toast stack**: top-right, color-coded ok/err/info.
- **Backup / Restore**: Tweaks panel footer buttons. Roundtrips tweaks, lang, wallets, watchlist, favorites, recent searches into a `sorametrics_backup_{date}.json`.

## Where we stopped (B7 pending)

**Claude Design usage limit hit at 2026-04-18 ~16:25. Resets "sáb 8:00" (next Saturday morning).**

### B7 planned prompt (NOT yet sent)
- **Peg alerts** — automatic thresholds on KUSD/TBCD peg deviation, toast + badge on sidebar when deviation > 1%.
- **Theme toggle** — dark (current) / light / auto, in Tweaks panel, persists.
- **Time-range universal selector** — 1h / 24h / 7d / 30d / 90d / all — applies to every section's KPIs + time-series.
- **Screenshot/share per card** — each KPI card + table row exposes a share button: screenshot the element (html2canvas) or copy deep-link URL with state.

### Known i18n gaps (framework ready, just wiring remaining)
- Swaps: Input/Output/Action headers, row-level "swap" verb.
- Extrinsics: Pallet::Method header, Success Rate/Top Pallet KPI labels, row Failed banner.
- Transfers/Bridges/Holders: KPI label strings (Transfers·24h, Volume·24h, etc.) beyond the ones translated.
- Order Book: Bids/Asks/Spread/Recent Fills chrome.
- Pools: "All Pools" card title, inline LP table headers.
- Tokens: card labels (Price/24h/Market Cap/Supply), KPI labels.
- Staking: card titles outside the already-done KPI/columns.
- Governance: sub-tab bodies (motions, members, votes count labels) — sub-tab **names** correctly stay Spanish.
- Balance: Overview body labels, Mis Wallets/Vigiladas table content (tab names correctly stay Spanish).
- Intelligence: severity labels on insight cards, body copy.
- Drill panel: Field labels throughout (Relative/UTC/Local, Pool/Route/Slippage, Pair/Size/Price, Validator/Finality).
- Music player: all UI chrome.

All of the above already have dictionary keys defined — it's purely a wiring task for a future pass.

## Next actions (when ready to resume)

1. **Wait for Claude Design usage limit reset** (sáb 8:00 next Saturday).
2. **Fire B7** (peg alerts + theme toggle + time-range + screenshot/share) in one batch.
3. **Optionally fire B8** (finish i18n wiring for the gaps listed above — all keys already defined).
4. **Export final zip** the same way (`Export → Download project as .zip` → drop in this folder with a new timestamped filename).
5. **Start Phase 1 of PORTING_PLAN.md**: wire WebSocket `wss://mof2.sora.org` to the new Pulse component. The prototype's Pulse currently uses mocked events — plug in the real stream keeping the new visual shell.
6. **Phases 2–7**: burns materialized views → portfolio → swaps/transfers/extrinsics/bridges → pools/staking/governance/intelligence/i18n/music/search.
7. **Phase 8 cutover** via `/beta` subpath. Rollback by flipping DNS back to `main`.

## Texture of the prototype (useful facts for the next session)

- **React bootstrap**: Babel in-browser transpilation per file. Scripts load in a specific order: `common.jsx` → `i18n.jsx` → `drill_music.jsx` → `features.jsx` → `shell.jsx` → `tweaks.jsx` → `burns.jsx` + `pulse.jsx` + `swaps.jsx` + `extrinsics.jsx` + `portfolio.jsx` → `routes.jsx` → `main.jsx`. `main.jsx` wraps the app in LangProvider > GlobalSearchProvider > ToastProvider > DrillProvider.
- **Globals via window**: bare identifiers like `useSearch`, `useWallets`, `useT`, `useDrill`, `AddWalletModal`, `WalletDetailsModal`, `ExportCsvButton`, `__CURRENT_LANG__`, `__SM_DRILL__`, `window.exportCsv` resolve at render time through the window proxy.
- **Path quirk**: Claude Design accidentally created a stray empty `SoraMetrics.html` file during B5 phase 3. The real file is `SoraMetrics Prototype.html`. Check the tab URL is `?file=SoraMetrics+Prototype.html`, not `SoraMetrics.html`.
- **Prompt delivery bug**: the Claude Design chat has 2 textareas. The visible one is `querySelectorAll('textarea')[1]` (placeholder "Describe what you want to create…"). `[0]` is a hidden comment textarea. Fill `[1]` with the native `HTMLTextAreaElement.prototype.value` setter + `input` event dispatch, not `[0]`.
- **React-controlled input fix**: `setter.call(ta, text); ta.dispatchEvent(new Event('input', { bubbles: true }));` works. Direct `ta.value = text` does NOT trigger React state update.
