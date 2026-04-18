# v6 Deploy Cheat Sheet

## Local dev
```bash
cd v6-front
npm install
node v6-server.js               # http://localhost:3005  (proxies → sorametrics.org)
# or point at a different backend:
TARGET=https://staging.sorametrics.org PORT=3010 node v6-server.js
```

## Production deploy to `/beta` subpath on the sorametrics VPS

### 1. SCP the files
The VPS is already configured with SSH access as `sorametrics` (Touch ID).
```bash
cd v6-front
rsync -avz --delete \
  --exclude node_modules --exclude v6-server.js --exclude package-lock.json \
  ./ sorametrics:/var/www/sorametrics-beta/
```

What gets copied:
- `index.html`, `styles.css`, `favicon.svg`, `manifest.json`, `sw.js`
- `js/*.jsx` (13 files)

Total uncompressed: ~940 KB. Transfer completes in seconds over broadband.

### 2. Nginx alias for `/beta`
Edit `/etc/nginx/sites-available/sorametrics` on the VPS, add inside the `server { ... }` block that already serves `sorametrics.org`:

```nginx
location /beta/ {
    alias /var/www/sorametrics-beta/;
    try_files $uri $uri/ /beta/index.html;
    add_header Cache-Control "no-cache, no-store, must-revalidate" always;
}

# SW must be served from the exact root it scopes — Service-Worker-Allowed header.
location = /beta/sw.js {
    alias /var/www/sorametrics-beta/sw.js;
    add_header Service-Worker-Allowed "/beta/";
    add_header Cache-Control "no-cache" always;
}
```

Reload: `sudo nginx -t && sudo systemctl reload nginx`.

### 3. Backend routing
The v6 front needs the same endpoints prod already serves at `sorametrics.org`. The `/beta` subpath can reach them via the same origin — no CORS changes needed because `/history`, `/tokens`, `/stats`, `/socket.io`, etc. live on port 3000 and nginx already proxies root paths there. No backend changes required.

### 4. Smoke test
```bash
curl -I https://sorametrics.org/beta/              # 200 text/html
curl -s https://sorametrics.org/beta/manifest.json # JSON
curl -s https://sorametrics.org/tokens | head      # real token list
```

Open `https://sorametrics.org/beta/` in a browser:
- Sidebar renders in Spanish (default) with 15 sections.
- Network Pulse streams real blocks via socket.io.
- Burn Tracker shows real XOR burn data.
- Cmd+K palette searches via `/search?q=` against prod.
- Peg Monitor card shows real stablecoin prices.

### 5. Rollback
If v6 breaks something, remove the nginx `location /beta/` blocks and reload. `sorametrics.org` root keeps serving the vanilla JS front from `main` branch — `/beta` is additive-only.

## Optional: pre-compile JSX with esbuild (cutover hardening)

In-browser Babel works but adds a ~250ms transpile delay per script on cold cache. For a production cutover you'd want:

```bash
# one-off, not committed
npm install --save-dev esbuild
npx esbuild js/*.jsx --bundle --format=esm --minify --outdir=dist
# then in index.html replace <script type="text/babel" ...> with:
#   <script type="module" src="dist/main.js"></script>
# and drop the @babel/standalone CDN line.
```

Alternative: keep Babel CDN during the beta phase, bundle only if traffic demands it.

## File inventory

| File                    | Size   | Purpose                                 |
|-------------------------|--------|-----------------------------------------|
| index.html              | 2.5 KB | React bootstrap + 14 script tags        |
| styles.css              | 81 KB  | Full stylesheet (CSS variables + RTL)   |
| sw.js                   | 2.5 KB | Service worker (cache-first + NO for API)|
| manifest.json           | 0.4 KB | PWA manifest scoped to /beta/           |
| favicon.svg             | 0.7 KB | Logo                                    |
| js/common.jsx           | 11 KB  | fmt, seededRand, useHistory, parseHistTime |
| js/i18n.jsx             | 78 KB  | 14-lang dictionary, LangProvider, useT  |
| js/shell.jsx            | 4.6 KB | Sidebar + Topbar                        |
| js/burns.jsx            | 12 KB  | Burn Tracker wired to /burns/*          |
| js/pulse.jsx            | 14 KB  | Network Pulse wired to socket.io        |
| js/portfolio.jsx        | 10 KB  | Portfolio (legacy, overlaps Balance)    |
| js/swaps.jsx            | 12 KB  | Swaps wired to /history/global/swaps    |
| js/extrinsics.jsx       | 20 KB  | Extrinsics wired to /history/global/extrinsics |
| js/routes.jsx           | 71 KB  | Transfers/Bridges/OrderBook/Pools/Tokens/Holders/Staking/Gov/Balance/Intel |
| js/drill_music.jsx      | 31 KB  | DrillPanel + MusicPlayer                |
| js/features.jsx         | 30 KB  | ToastProvider, WalletProvider, CmdK, modals, CSV, Backup/Restore |
| js/tweaks.jsx           | 4 KB   | Tweaks panel (theme, peg threshold, …) |
| js/main.jsx             | 4.5 KB | App + deep-link router + peg-watcher   |

Total: **~386 KB** of JSX + CSS (pre-compile would minify to ~120 KB).

## Known limits (see GAP_ANALYSIS.md)

- Swap/Extrinsic row fees come as 0 (prod `/history/*` doesn't expose per-row fee).
- Tokens sparklines stay seeded (real `/chart/:symbol` needs a chart lib swap).
- OrderBook bids/asks stay mocked (no prod snapshot endpoint).
- Wallet Details shows 1 tab (assets). The other 7 prod sub-tabs need per-addr endpoints wiring — deferred.
- CSV tax-format variants (koinly/cointracking/cointracker) deferred — prod does expose `/export/csv?format=...` so wiring is 30 lines.
- Time-range universal selector (1h/24h/7d/30d) + screenshot/share per card deferred to a v6.1 pass.
