# SoraMetrics v6 — Porting Plan

> Target branch: `v6-ui`
> Source of truth for features: `docs/FEATURE_AUDIT.md`
> Origin of new visual shell: Claude Design prototype
> ([project c806f352-b1da-42e8-98ff-1913b7607edd](https://claude.ai/design/p/c806f352-b1da-42e8-98ff-1913b7607edd))

## Strategy — Hybrid A, phase-by-phase

We keep production (`main`) running at `sorametrics.org` untouched.
`v6-ui` branch carries the new visual shell (HTML/CSS/mock-data JS) from
Claude Design, which we incrementally rewire to the existing backend
(WebSocket to `wss://mof2.sora.org`, backfillers, Postgres, Redis).

**No big-bang rewrite.** Each section is ported under a feature flag so
partial progress can be deployed to a `/beta` subpath while the rest of the
site keeps serving from `main`.

---

## Phase 0 — Import (blocking all others)

Inputs: finalized Claude Design prototype (v2+) with all features from
FEATURE_AUDIT §20 scaffolded.

Tasks:
- [ ] Export Claude Design output (HTML + CSS + JS + any assets).
- [ ] Drop into this branch under `v6/` folder (keep it parallel to
      root index.html so existing site is unaffected).
- [ ] Add `/beta` serving route that maps to `v6/index.html`.
- [ ] Document mock-data shape per section in
      `v6/mock/DATA_SHAPES.md` so later phases know what to wire.

Verifier: open `/beta` in a staging deploy and confirm visual parity with
Claude Design's final canvas.

## Phase 1 — Network Pulse (live feed)

The simplest to wire because data already arrives via WebSocket (`wss://
mof2.sora.org`). No SQL, no backfiller dependency.

Tasks:
- [ ] Lift the WebSocket subscription from `main/script.js` into
      `v6/lib/sora-socket.js`.
- [ ] For each event type (swap, transfer, block, order, burn), map the
      raw socket payload to the mock-shape the prototype already consumes.
- [ ] Wire `v6/sections/pulse.js` to the live stream. Replace the
      generator with the socket subscription.
- [ ] Verify drill-down side panels populate from the real payload
      (wallet addresses, timestamp, fee breakdown, extrinsic hash).

Verifier: `/beta/pulse` shows the same events as production home for 10+
consecutive minutes, with drill-down populated from the real payload.

## Phase 2 — Burns Tracker

Data source: Postgres materialized views (`create_materialized_views.sql`)
+ `backfill_xor_supply.js` for supply history.

Tasks:
- [ ] Endpoint `/api/v6/burns/summary` returning `{ totalBurned,
      by_token: {...}, by_fee_type: {...}, history: [...] }`.
- [ ] Wire the Furnace viz in `v6/sections/burns.js`.
- [ ] Wire the BURN RATE cumulative chart (chart.js `line/area/bars`).
- [ ] Wire the "Top Holders · XOR" table to live data.

Verifier: numbers match production Burns page within acceptable rounding
(recently-indexed blocks may lag by <30s).

## Phase 3 — Portfolio (3 sub-tabs)

Data source: browser localStorage (user wallets) + backend queries per
wallet.

Tasks:
- [ ] Port wallet-add modal (single + bulk modes).
- [ ] Port watched vs mine separation.
- [ ] Wire Overview tab aggregation.
- [ ] Wire wallet-details modal with 8 sub-tabs (Assets / Swaps /
      Transfers / Bridges / Liquidity / Staking / Info / Extrinsics).
- [ ] Favorites toggle persistence.

Verifier: adding / removing / renaming wallets mirrors production behaviour.

## Phase 4 — Swaps / Transfers / Extrinsics / Bridges (table sections)

Share a common pattern: paginated table + filters + drill-down modal.

Tasks:
- [ ] Common table component with pagination (First/Prev/Next/Last).
- [ ] Filter row (token pair, direction, date range, wallet).
- [ ] CSV export integration (`csvExportModal`).
- [ ] Per-row drill-down opens the appropriate detail modal.

Verifier: each page shows the same records as production with matching
sort order and pagination.

## Phase 5 — Pools / Liquidity, Order Book, Tokens, Holders

Lower priority, same patterns. Port one per week once phase 4 is stable.

## Phase 6 — Staking, Governance, Intelligence

- Staking: Validators table + Network Info stat cards.
- Governance: 5 sub-tabs (Consejo, Elecciones, Mociones, Democracia, Tech).
- Intelligence: whatever production currently has (check `section-intelligence`).

## Phase 7 — Cross-cutting features

- [ ] **i18n**: port `TRANSLATIONS` from main `script.js`. Default ES.
      RTL support (HE, UR) via `html[lang]` selectors on the v6 CSS.
- [ ] **Music player**: port the draggable panel, waveform, playlist
      toggle, ambient star particles. Keep it visually consistent with
      Claude Design's soften-the-palette outcome.
- [ ] **Global search (⌘K)**: overlay with grouped results (wallets, tx,
      blocks, extrinsics, tokens).
- [ ] **Dynamic tabs bar**: `#dynamicTabsContainer` with max-5 alert.
- [ ] **Theme toggle**: expose sun/moon icon; `data-theme` attribute.
- [ ] **Peg alerts**: top banner dismissible, surfaces when KUSD or TBCD
      deviates > 2%.
- [ ] **Per-card screenshot/share**: html2canvas pattern + shareable URL.
- [ ] **Universal time-range selectors**: 24h / 7d / 30d / All / custom.

## Phase 8 — Cutover

- [ ] Feature-complete parity gate: FEATURE_AUDIT §20 all at `[x]`.
- [ ] A/B test on `/beta` subroute for 2 weeks.
- [ ] Flip DNS / serving root.
- [ ] Archive `main` to `v5-legacy` branch.

---

## Rollback strategy

Every phase lands behind the `?v=6` query flag before merging. Worst case:
flip DNS back to `main`, which has been untouched. No data migration means
no DB risk.

## Out of scope

- Backend changes (backfiller, db_pg, redis, ETH helpers) — stays untouched.
- `scripts/` operational tooling — stays untouched.
- Contract changes — N/A (sorametrics is view-only, no on-chain writes).

## Dependencies

- Claude Design final export (Phase 0 blocker).
- No changes to `wss://mof2.sora.org` WebSocket format.
- No schema changes in Postgres during the porting window (would break the
  backfillers too, so unlikely).
