// blockchain.js
const { ApiPromise, WsProvider } = require('@polkadot/api');
const { options } = require('@sora-substrate/api');
const { WS_ENDPOINT } = require('./config');

let api = null;

async function initApi() {
    if (api) return api;
    console.log(`🔌 Conectando a ${WS_ENDPOINT}...`);
    const provider = new WsProvider(WS_ENDPOINT);
    api = await ApiPromise.create(options({ provider }));
    await api.isReady;
    console.log('✅ Conexión establecida.');
    return api;
}

module.exports = { initApi };
