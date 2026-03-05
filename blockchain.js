const { ApiPromise, WsProvider } = require('@polkadot/api');
const { options } = require('@sora-substrate/api');
const { WS_ENDPOINT } = require('./config');

let api = null;
let provider = null;

async function initApi() {
    if (api && api.isConnected) return api;

    console.log(`Connecting to ${WS_ENDPOINT}...`);

    provider = new WsProvider(WS_ENDPOINT, 2500); // auto-reconnect every 2.5s

    provider.on('connected', () => console.log('WS connected.'));
    provider.on('disconnected', () => console.warn('WS disconnected. Auto-reconnecting...'));
    provider.on('error', (err) => console.error('WS error:', err.message));

    api = await ApiPromise.create(options({ provider }));
    await api.isReady;

    api.on('disconnected', () => console.warn('API disconnected.'));
    api.on('connected', () => console.log('API reconnected.'));
    api.on('error', (err) => console.error('API error:', err.message));

    console.log('Blockchain API ready.');
    return api;
}

module.exports = { initApi };
