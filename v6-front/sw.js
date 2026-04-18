// Service worker for v6 prototype under /beta subpath.
// Bumped to v24 (prod is v23) so the first /beta install wipes old caches
// and fetches the v6 bundle fresh.
const CACHE_NAME = 'sorametrics-v24-beta';
const ASSETS = [
    '/beta/',
    '/beta/index.html',
    '/beta/styles.css',
    '/beta/manifest.json',
    '/beta/favicon.svg',
    '/beta/js/common.jsx',
    '/beta/js/i18n.jsx',
    '/beta/js/shell.jsx',
    '/beta/js/burns.jsx',
    '/beta/js/pulse.jsx',
    '/beta/js/portfolio.jsx',
    '/beta/js/swaps.jsx',
    '/beta/js/extrinsics.jsx',
    '/beta/js/routes.jsx',
    '/beta/js/drill_music.jsx',
    '/beta/js/features.jsx',
    '/beta/js/tweaks.jsx',
    '/beta/js/main.jsx',
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
        url.endsWith('/beta/') ||
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

    // Network Only for API + WebSocket — never cache live data.
    if (
        url.includes('/history/') || url.includes('/socket.io') ||
        url.includes('/stats/') || url.includes('/balance') ||
        url.includes('/balances') || url.includes('/search') ||
        url.includes('/identity/') || url.includes('/burns/') ||
        url.includes('/staking/') || url.includes('/governance/') ||
        url.includes('/pools') || url.includes('/pool/') ||
        url.includes('/chart/') || url.includes('/tokens') ||
        url.includes('/wallet/') || url.includes('/currency-rates') ||
        url.includes('/holders/') || url.includes('/api/') ||
        url.includes('/export/') || url.includes('/music/')
    ) {
        return;
    }

    // Cache First for fonts + logos + anything else static.
    e.respondWith(caches.match(e.request).then(r => r || fetch(e.request)));
});
