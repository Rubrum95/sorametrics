const CACHE_NAME = 'sorametrics-v4';
const ASSETS = [
    '/',
    '/index.html',
    '/script.js',
    '/manifest.json',
    '/favicon.svg'
];

self.addEventListener('install', (e) => {
    self.skipWaiting(); // Forzar activación inmediata del nuevo SW
    e.waitUntil(
        caches.open(CACHE_NAME).then((cache) => cache.addAll(ASSETS))
    );
});

self.addEventListener('activate', (e) => {
    e.waitUntil(
        caches.keys().then((keys) => {
            return Promise.all(
                keys.map((key) => {
                    if (key !== CACHE_NAME) return caches.delete(key);
                })
            );
        })
    );
    self.clients.claim(); // Tomar control de clientes inmediatamente
});

self.addEventListener('fetch', (e) => {
    // Network First para HTML y JS (Asegura tener la última versión si hay internet)
    if (e.request.method === 'GET' && (e.request.url.includes('/index.html') || e.request.url.includes('/script.js') || e.request.url.endsWith('/'))) {
        e.respondWith(
            fetch(e.request)
                .then(res => {
                    const clone = res.clone();
                    caches.open(CACHE_NAME).then(cache => cache.put(e.request, clone));
                    return res;
                })
                .catch(() => caches.match(e.request))
        );
        return;
    }

    // Network Only para API/Sockets
    if (e.request.url.includes('/history') || e.request.url.includes('/socket.io')) {
        return;
    }

    // Cache First para imágenes y otros estáticos (Mejor rendimiento)
    e.respondWith(
        caches.match(e.request).then((response) => response || fetch(e.request))
    );
});
