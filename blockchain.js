const { ApiPromise, WsProvider } = require('@polkadot/api');
const { options } = require('@sora-substrate/api');
const WebSocket = require('ws');
const { WS_ENDPOINTS } = require('./config');

let api = null;
let provider = null;

// Track which endpoint of the array we're currently using. WsProvider
// internally rotates `index = (index + 1) % endpoints.length` on every
// failed connection, so we trail it via the `connected` event payload
// (the WsProvider exposes `endpoint` after connect).
let _activeEndpoint = null;
function getActiveEndpoint() { return _activeEndpoint; }

// Probe a WS endpoint cheaply: open a socket, wait for `open` (≤3s
// timeout), close it. Returns true if reachable.
function probeWs(url, timeoutMs = 3000) {
    return new Promise(resolve => {
        let done = false;
        let ws = null;
        const finish = (ok) => { if (!done) { done = true; resolve(ok); try { ws && ws.close(); } catch {} } };
        try {
            ws = new WebSocket(url);
            const tm = setTimeout(() => finish(false), timeoutMs);
            ws.on('open',  () => { clearTimeout(tm); finish(true); });
            ws.on('error', () => { clearTimeout(tm); finish(false); });
        } catch (e) { finish(false); }
    });
}

// When WsProvider has fallen back to a non-primary endpoint and the primary
// recovers, we restart the process so the new WsProvider starts again from
// index[0]. PM2 (`unless-stopped`) brings us back in ~15s. There is no
// supported public API to rewind WsProvider's internal index, hence the
// process-level restart. Healthcheck cadence: every 2 minutes.
function startPrimaryHealthcheck() {
    if (!Array.isArray(WS_ENDPOINTS) || WS_ENDPOINTS.length < 2) return; // nothing to fall back to
    const PRIMARY = WS_ENDPOINTS[0];
    setInterval(async () => {
        try {
            if (_activeEndpoint === PRIMARY) return; // already on primary
            const ok = await probeWs(PRIMARY);
            if (ok) {
                console.warn(`[rpc] primary recovered (${PRIMARY}), currently on ${_activeEndpoint}. Restarting to prefer primary.`);
                process.exit(0); // PM2 restarts us; new WsProvider starts at endpoints[0]
            }
        } catch (e) {
            // Probe errors are non-fatal — we just stay on the current endpoint.
        }
    }, 2 * 60 * 1000);
}

async function initApi() {
    if (api && api.isConnected) return api;

    console.log(`Connecting to RPC pool: ${WS_ENDPOINTS.join(' → ')}`);

    // WsProvider accepts string | string[]. With an array it rotates to the
    // next endpoint when the active socket drops, giving us free failover.
    // Re-connect interval 2.5s (kept from the previous single-endpoint impl).
    provider = new WsProvider(WS_ENDPOINTS, 2500);

    provider.on('connected', () => {
        // `provider.endpoint` is the URL of the currently-active socket.
        _activeEndpoint = provider.endpoint || WS_ENDPOINTS[0];
        console.log(`WS connected → ${_activeEndpoint}`);
    });
    provider.on('disconnected', () => {
        console.warn(`WS disconnected from ${_activeEndpoint}. WsProvider will rotate.`);
    });
    provider.on('error', (err) => console.error('WS error:', err.message));

    api = await ApiPromise.create(options({ provider }));
    await api.isReady;

    api.on('disconnected', () => console.warn('API disconnected.'));
    api.on('connected', () => console.log('API reconnected.'));
    api.on('error', (err) => console.error('API error:', err.message));

    console.log(`Blockchain API ready (active endpoint: ${_activeEndpoint || '?'}).`);
    startPrimaryHealthcheck();
    return api;
}

module.exports = { initApi, getActiveEndpoint };
