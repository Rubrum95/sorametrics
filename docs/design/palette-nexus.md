# SoraMetrics — "Nexus" palette (dark + light), inspired by sora.org

Owner request (translated): "The colour palette feels a bit aggressive. Could we take inspiration from sora.org —
in both dark mode and light mode?" He keeps the Liquid Glass look at MAXIMUM (see the `glass layer` and
`glass level` blocks at the end of styles.css; `--glass: 1`). Changes must be clearly visible, not timid.

## Verified sora.org facts (read from its CSS on 2026-09-27)
- Dark (`:root[data-theme="dark"]`): page #101824, surfaces #192534, text #e0eaf6, secondary text #b1c2d5 /
  #9fb2c7, glass rgba(16,24,36,.72), border rgba(224,234,246,.12), turquoise glow #b7effc (rgba .18–.35),
  amber #ffecbf, accent coral #ff5946, accent-2 teal #51e2cd, gold #ffd372, pink rgba(255,115,190,.6),
  deep shadows rgba(2,5,10,.1–.3).
- Light (`:root`): body #edf1f5 with warm paper gradient #f8f6f1 → #f3efe7 → #f7f4ec, ink #1c2130 / #243343,
  secondary #4f6075 / #627087, cool greys #e9edf4 #dfe5ef #cfd6e5, accent coral #ff4f3c / softer #d55e49,
  teal #46d6c3 / #238b7d, pastels turquoise #a8e2f2, magenta #e8c7f2, amber #f3e0b5, sand #d2c7b7,
  glass white .72, border rgba(28,33,48,.12), soft blue-grey control shadow `4px 5px 14px #8095aa30,
  -3px -3px 10px #ffffffb3, inset 1px 1px 0 #ffffffd9`.
- Background: radial pastel glows (turquoise at 20% 20%, lilac/navy at 70% 15%, amber at 50% 60%).
- Font: Sora (SoraMetrics already uses Sora). Headline gradient peach → lavender → blue → pink.
- Coral is an ACCENT (logo, small labels, one CTA), never large red areas. That calmness is the point.

## Token contract (fixed names; dark = :root default, light = :root[data-theme="light"])
| token | dark | light |
|---|---|---|
| --bg-0 / --bg-1 / --bg-2 / --bg-3 / --bg-4 | #0c131e / #101824 / #142031 / #192534 / #22334a | #edf1f5 / #f4f6f9 / #ffffff / #eef2f7 / #e1e8f0 |
| --bg-card | #142031 | #ffffff |
| --fg-0 / --fg-1 / --fg-2 / --fg-3 | #f2f6fb / #e0eaf6 / #b1c2d5 / #8b9db2 | #0f1623 / #1c2130 / #46556b / #627087 |
| --ov-rgb (overlay base for `rgb(var(--ov-rgb) / A)`) | 224 234 246 | 28 33 48 |
| --shade-rgb (shadow base) | 2 5 10 | 72 96 124 |
| --border / --border-strong | rgb(var(--ov-rgb) / .10) / .18 | .12 / .20 |
| --accent / --accent-text / --accent-rgb | #ff5946 / #ff7a66 / 255 89 70 | #d55e49 / #b8452f / 213 94 73 |
| --accent-2 (teal) | #51e2cd | #238b7d |
| --turquoise / --lilac / --amber | #b7effc / #c9b3f2 / #ffd372 | #5fb8cf / #8a6cc2 / #a4751f |
| --ok / --err / --warn / --info / --pink | #34d399 / #f87171 / #fbbf24 / #7cc4fa / #f28dc4 | #0e8f63 / #cc3d3d / #b7791f / #2f6fb3 / #b8508a |
| --grad-brand | linear-gradient(110deg,#f6b5a3,#c7a6f0 45%,#9fbde8 70%,#e7a1c9) | linear-gradient(110deg,#c65a44,#7d5bc0 45%,#4b73b0 70%,#b8508a) |
| --grad-accent | linear-gradient(135deg,#ff7a5c,#ff5946,#e0452f) | linear-gradient(135deg,#e27560,#d55e49,#bf4a36) |
| --grad-avatar | linear-gradient(135deg,#2c4a6e,#4b3f7a) | linear-gradient(135deg,#cfdced,#ddd2f0) |
Old names (--brand-*, --grad-ember, --grad-plum, --card, --card-2, --border-color…) alias the new tokens.

## Rules
- White-alpha overlays → `rgb(var(--ov-rgb) / A)`; black shadows → `rgb(var(--shade-rgb) / A)`.
- Status colours → --ok/--err/--warn/--info/--pink; ember/burgundy → --accent / rgb(var(--accent-rgb) / A);
  amethyst/plum → --lilac; slate greys → --fg-2/--fg-3; white text → --fg-0 (on accent fills: --on-accent).
- Token / chain / pallet identity colours are data: keep them but legible on light surfaces.
- SVG presentation attributes don't take var() reliably → style props. Canvas / Chart.js read tokens with
  getComputedStyle at draw time and redraw on the `sm-theme` window event.
- Theme: localStorage 'sm.theme' = auto|dark|light (default auto), early inline script in index.html <head>,
  topbar toggle cycles auto → light → dark, one-row topbar at 390/834/1024/1440.
- Glass: dark = navy tinted glass; light = white frosted glass; keep --glass and all fallbacks in both themes.
