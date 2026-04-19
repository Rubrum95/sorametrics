// v6-server.js — local dev server for sorametrics v6 front.
// Serves v6-front/ statics on http://localhost:3001 and proxies every real API
// path + socket.io to https://sorametrics.org (production backend).
//
// Why proxy: running the prod backend locally requires Postgres sm schema +
// Redis + a synced SORA node on port 9944. Out of scope for UI iteration.
// The proxy lets us eat real data + real WS events while keeping CORS clean.

import express from 'express';
import { createProxyMiddleware } from 'http-proxy-middleware';
import { fileURLToPath } from 'node:url';
import { dirname, join } from 'node:path';
import http from 'node:http';

const __dirname = dirname(fileURLToPath(import.meta.url));
const PORT = Number(process.env.PORT) || 3005;
const TARGET = process.env.TARGET || 'https://sorametrics.org';

const app = express();

// Paths that must go to the production backend — anything the front consumes.
// Order matters: longer prefixes first to avoid accidental shadowing.
const PROXIED_PATHS = [
  '/api',
  '/tokens',
  '/balance',
  '/balances',
  '/holders',
  '/identity',
  '/currency-rates',
  '/search',
  '/lookup',
  '/proxy-image',
  '/pools',
  '/pool',
  '/chart',
  '/export',
  '/music',
  '/wallet',
  '/history',
  '/staking',
  '/governance',
  '/burns',
  '/stats',
  '/socket.io',
];

// Use pathFilter on a root-mounted proxy instead of prefix-mounting per path.
// Prefix-mounting (app.use('/tokens', proxy)) strips the prefix before proxying,
// which turns GET /tokens into GET / at the upstream — wrong path, HTML back.
const isProxiedPath = (pathname) =>
  PROXIED_PATHS.some((p) => pathname === p || pathname.startsWith(p + '/'));

const proxy = createProxyMiddleware({
  target: TARGET,
  changeOrigin: true,
  ws: true,
  pathFilter: (pathname) => isProxiedPath(pathname),
  logger: console,
  on: {
    proxyReq: (proxyReq, req) => {
      if (req.url.startsWith('/socket.io')) {
        console.log(`[v6-server] ws-proxy → ${req.url}`);
      }
    },
    error: (err, req, res) => {
      console.error(`[v6-server] proxy error on ${req?.url}:`, err.message);
      // WS upgrade failures hand us a raw `net.Socket` here, not an HTTP
      // response. writeHead/end don't exist on sockets, so guard the write
      // and just destroy the socket instead. For normal HTTP requests we
      // still return a JSON 502.
      if (res && typeof res.writeHead === 'function') {
        if (!res.headersSent) {
          res.writeHead(502, { 'Content-Type': 'application/json' });
          res.end(JSON.stringify({ error: 'upstream_unavailable', path: req?.url }));
        }
      } else if (res && typeof res.destroy === 'function') {
        try { res.destroy(); } catch {}
      }
    },
  },
});

app.use(proxy);

// Static files — v6-front/ itself. Must come after proxies so matching API
// paths aren't served as 404s from disk.
app.use(express.static(__dirname, {
  // Babel+React can be chatty; let the browser cache aggressively during dev.
  maxAge: 0,
  setHeaders: (res, path) => {
    if (path.endsWith('.jsx') || path.endsWith('.js')) {
      res.setHeader('Cache-Control', 'no-cache');
    }
  },
}));

// SPA fallback: anything that didn't match static or proxy → index.html.
// Guard: don't fallback for known proxied prefixes (already handled).
app.use((req, res, next) => {
  if (PROXIED_PATHS.some((p) => req.path.startsWith(p))) {
    return next();
  }
  res.sendFile(join(__dirname, 'index.html'));
});

// Create server + attach upgrade handler so socket.io WebSocket upgrades
// make it to the proxy (express.static doesn't handle 'upgrade' events).
const server = http.createServer(app);
server.on('upgrade', (req, socket, head) => {
  if (req.url && req.url.startsWith('/socket.io')) {
    proxy.upgrade(req, socket, head);
  } else {
    socket.destroy();
  }
});

server.listen(PORT, () => {
  console.log(`[v6-server] v6-front running at http://localhost:${PORT}`);
  console.log(`[v6-server] proxying API + socket.io → ${TARGET}`);
});
