const https = require('https');
const { ETH_RPC_URL } = require('./config');

function resolveEthSender(txHash) {
    return new Promise((resolve, reject) => {
        if (!ETH_RPC_URL) {
            console.warn('⚠️ No ETH_RPC_URL configured. Skipping sender resolution.');
            return resolve(null);
        }

        const url = new URL(ETH_RPC_URL);
        const data = JSON.stringify({
            jsonrpc: "2.0",
            method: "eth_getTransactionByHash",
            params: [txHash],
            id: 1
        });

        const options = {
            hostname: url.hostname,
            path: url.pathname + url.search,
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                'Content-Length': data.length
            }
        };

        const req = https.request(options, (res) => {
            let body = '';
            res.on('data', (chunk) => body += chunk);
            res.on('end', () => {
                try {
                    const response = JSON.parse(body);
                    if (response.result && response.result.from) {
                        resolve(response.result.from);
                    } else {
                        resolve(null);
                    }
                } catch (e) {
                    console.error('Error parsing Infura response:', e);
                    resolve(null);
                }
            });
        });

        req.on('error', (e) => {
            console.error('Error requesting Infura:', e);
            resolve(null);
        });

        req.write(data);
        req.end();
    });
}

module.exports = { resolveEthSender };
