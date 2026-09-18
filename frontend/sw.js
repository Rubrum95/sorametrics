// Service worker for SoraMetrics v6 — PRODUCTION (root path).
// v25 bumps the cache name so the first install after the v1→v2 switchover
// wipes every v1 cache entry and refetches the bundle fresh. On every
// subsequent navigation network-first wins for HTML/JSX/CSS so users see
// new deploys without a hard reload.

const CACHE_NAME = 'sorametrics-v77-prod';
const ASSETS = [
    '/',
    '/index.html',
    '/styles.css',
    '/manifest.json',
    '/favicon.svg',
    '/js/common.jsx',
    '/js/tokenRegistry.jsx',
    '/js/identityRegistry.jsx',
    '/js/i18n.jsx',
    '/js/shell.jsx',
    '/js/burns.jsx',
    '/js/pulse.jsx',
    '/js/portfolio.jsx',
    '/js/swaps.jsx',
    '/js/extrinsics.jsx',
    '/js/routes.jsx',
    '/js/drill_music.jsx',
    '/js/studio.jsx',
    '/js/intelligence.jsx',
    '/js/features.jsx',
    '/js/tweaks.jsx',
    '/js/main.jsx',
];

self.addEventListener('install', (e) => {
    self.skipWaiting();
    e.waitUntil(caches.open(CACHE_NAME).then(c => c.addAll(ASSETS).catch(() => {})));
});

self.addEventListener('activate', (e) => {
    e.waitUntil(
        caches.keys().then(keys => Promise.all(
            keys.filter(k => k !== CACHE_NAME).map(k => caches.delete(k))
        ))
    );
    self.clients.claim();
});

self.addEventListener('fetch', (e) => {
    const url = e.request.url;
    // Network First for HTML + JSX + CSS — always try latest, fall back to cache.
    if (e.request.method === 'GET' && (
        url.includes('/index.html') ||
        url.endsWith(self.location.origin + '/') ||
        url.endsWith('.jsx') ||
        url.endsWith('.js') ||
        url.endsWith('.css')
    )) {
        e.respondWith(
            fetch(e.request)
                .then(res => {
                    const clone = res.clone();
                    caches.open(CACHE_NAME).then(c => c.put(e.request, clone)).catch(() => {});
                    return res;
                })
                .catch(() => caches.match(e.request))
        );
        return;
    }
    // Backend API calls (/governance, /tokens, /balance, /history, …) and
    // socket.io traffic must NEVER be cached by the SW — they're live data.
    // Let the browser hit the network directly.
});
