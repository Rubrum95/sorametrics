# SoraMetrics

SORA Network Analytics Dashboard - Real-time metrics for the SORA ecosystem.

## Features

- 📊 **Real-time Analytics** - Live swaps, transfers, and bridge transactions
- 💰 **Token Prices** - Market data with price changes and volume
- 🏊 **Liquidity Pools** - TVL, volume, providers, and activity
- 👛 **Wallet Analysis** - Complete wallet history and balances
- 📱 **PWA Support** - Install as a native app

## Quick Start

```bash
# Install dependencies
npm install

# Start the server
npm start

# Or with PM2
pm2 start index.js --name sorametrics
```

## Configuration

Edit `config.js` to configure:
- WebSocket endpoint (WS_ENDPOINT)
- Whitelist URL

## Tech Stack

- Node.js + Express
- SQLite (better-sqlite3)
- Polkadot.js API
- Socket.IO (real-time)

## License

MIT
