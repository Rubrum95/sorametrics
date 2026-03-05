require('dotenv').config();

module.exports = {
    WS_ENDPOINT: process.env.WS_ENDPOINT || 'wss://ws.mof.sora.org',
    WS_ENDPOINT_BACKFILL: process.env.WS_ENDPOINT_BACKFILL || 'wss://mof2.sora.org',
    WHITELIST_URL: process.env.WHITELIST_URL || 'https://raw.githubusercontent.com/sora-xor/polkaswap-token-whitelist-config/master/whitelist.json',
    ETH_RPC_URL: process.env.ETH_RPC_URL || '',
    PORT: parseInt(process.env.PORT, 10) || 3000,
    CORS_ORIGINS: process.env.CORS_ORIGINS || ''
};
