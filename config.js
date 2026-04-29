require('dotenv').config();

// WS_ENDPOINTS: ordered list of nodes to use, primary first. The
// @polkadot/api WsProvider takes an array and fails over to the next
// entry when the active one disconnects. We layer a periodic healthcheck
// on top (see blockchain.js) so we eventually return to the primary.
//
// Override via WS_ENDPOINTS env var as a comma-separated list, e.g.
//   WS_ENDPOINTS=ws://127.0.0.1:9944,wss://ws.mof.sora.org,wss://mof2.sora.org
// Legacy single-endpoint WS_ENDPOINT is still honoured if present.
const parseEndpoints = () => {
    if (process.env.WS_ENDPOINTS) {
        return process.env.WS_ENDPOINTS.split(',').map(s => s.trim()).filter(Boolean);
    }
    if (process.env.WS_ENDPOINT) return [process.env.WS_ENDPOINT];
    return ['wss://ws.mof.sora.org', 'wss://mof2.sora.org'];
};
const WS_ENDPOINTS = parseEndpoints();

module.exports = {
    WS_ENDPOINTS,
    WS_ENDPOINT: WS_ENDPOINTS[0],                                                 // back-compat for code reading the singular form
    WS_ENDPOINT_BACKFILL: process.env.WS_ENDPOINT_BACKFILL || WS_ENDPOINTS[1] || WS_ENDPOINTS[0],
    WHITELIST_URL: process.env.WHITELIST_URL || 'https://raw.githubusercontent.com/sora-xor/polkaswap-token-whitelist-config/master/whitelist.json',
    ETH_RPC_URL: process.env.ETH_RPC_URL || '',
    PORT: parseInt(process.env.PORT, 10) || 3000,
    CORS_ORIGINS: process.env.CORS_ORIGINS || ''
};
