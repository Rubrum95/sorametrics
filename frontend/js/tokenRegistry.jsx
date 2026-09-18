/* global */
// Token registry — partitions the SORA token universe into two groups:
//
//   · Native  → emitted/managed by SORA runtime (XOR, VAL, PSWAP, KUSD, the
//               Kensetsu CDP family K*, XST stack, partner tokens like CERES,
//               DEO, APOLLO, etc.). Market cap / supply / holders come from
//               the VPS (/burns/supply/:sym + /holders/:assetId + /tokens price).
//
//   · External → assets bridged into SORA from another chain (ETH, DAI, USDT,
//                AAVE, COMP, DOT, KSM, …). Market cap / supply come from
//                CoinGecko's free public API with 24-hour client-side caching.
//
// The classifier is intentionally conservative: everything explicitly listed
// as "external" goes to CoinGecko; everything else is treated as SORA-native.
// That matches the user's mental model — "anything that starts with K (except
// KSM) is Kensetsu → SORA-native".

// Explicit external-chain assets present on SORA as bridged representations.
// Anything outside this set is assumed to be a SORA-native asset.
const EXTERNAL_SYMBOLS = new Set([
  // Stablecoins & majors
  'ETH','WETH','DAI','USDT','USDC','BUSD','WBTC','HBTC','CBTC','CETH','CUSDT','CUNI','CDAI','FRAX','FEI','HUSD',
  // Polkadot/Kusama ecosystem
  'DOT','KSM','ASTR','ACA',
  // DeFi / Ethereum-chain ERC-20
  'AAVE','UNI','COMP','CRV','SNX','MKR','YFI','BAL','BAND','LINK','SUSHI','1INCH','BAT','KNC','REN','ZRX',
  'BNT','GRT','ENJ','FTT','MANA','MATIC','CHZ','CRO','ALPHA','ALCX','ANKR','ANT','APE','AUDIO','AXS','AVAX',
  'BADGER','BICO','BOND','BONDLY','BTM','BTMX','BZRX','CAPS','CEL','CELR','CREAM','CRE','CRU','CVC','DIA',
  'DODO','DENT','ELF','ERN','FARM','FET','FIS','FOTO','FRONT','FX','FUN','GLM','GNO','GNY','GT','HOT',
  'IDEX','JST','LRC','MLN','MPH','NEXO','NMR','OMG','OXT','PAX','PERP','QNT','RAD','REP','RGT','RUNE',
  'SAI','SRM','STAKE','STORJ','SUPER','TEL','TRIBE','TRU','TWT','UMA','UNI','USDP','WAVES','WOO',
  'XRP','ACH','ADX','AKRO','ALEPH','AMP','AUCTION','DNT','EXRD','FLX','FUN','GAS','IOTX','JASMY',
  'LQTY','NKN','OCEAN','PAXG','POWR','QKC','RARI','REQ','SKL','SPELL','STG','SUSHI','TLM','TRB','UMA','WNXM',
]);

// Additional aliases that are also external but might come through on SORA as
// different spellings.
const EXTERNAL_ALIASES = new Set(['WETH','WBNB','WBTC','HBTC']);

// Anything in here is force-classified as native even if it accidentally shows
// up near external patterns (e.g. tokens starting with K that aren't Kensetsu).
const FORCE_NATIVE = new Set([
  'XOR','VAL','PSWAP','KUSD','XST','XSTUSD','TBCD','VXOR','DEO','CERES','APOLLO','KARMA','KEN','HMX',
]);

function isSoraNative(symbol) {
  if (!symbol) return false;
  const s = String(symbol).toUpperCase();
  if (FORCE_NATIVE.has(s)) return true;
  if (EXTERNAL_SYMBOLS.has(s) || EXTERNAL_ALIASES.has(s)) return false;
  // Kensetsu CDP tokens — "K*" family is SORA-native by convention. KSM is
  // already in EXTERNAL_SYMBOLS so it won't hit this rule.
  if (s.startsWith('K') && s.length >= 3) return true;
  // XST* family — SORA synthetic tokens.
  if (s.startsWith('XST')) return true;
  // Default: treat unknown symbols as native (they're probably SORA-registered
  // assets we haven't catalogued). If that turns out wrong we add them to the
  // EXTERNAL_SYMBOLS set.
  return true;
}

// Symbol → CoinGecko ID for the external tokens we care about. Missing entries
// fall back to "unknown" and the market cap / supply cells render '—'.
const COINGECKO_IDS = {
  // Majors
  ETH: 'ethereum',          WETH: 'weth',
  DAI: 'dai',               USDT: 'tether',
  USDC: 'usd-coin',         BUSD: 'binance-usd',
  WBTC: 'wrapped-bitcoin',  HBTC: 'huobi-btc',
  FRAX: 'frax',             FEI: 'fei-usd',
  HUSD: 'husd',             PAX:  'paxos-standard',
  USDP: 'paxos-standard',   SAI:  'sai',
  // Polkadot / Kusama
  DOT:  'polkadot',         KSM:  'kusama',
  ASTR: 'astar',            ACA:  'acala',
  // DeFi governance
  AAVE: 'aave',             UNI:  'uniswap',
  COMP: 'compound-governance-token',
  CRV:  'curve-dao-token',  SNX:  'havven',
  MKR:  'maker',            YFI:  'yearn-finance',
  BAL:  'balancer',         BAND: 'band-protocol',
  LINK: 'chainlink',        SUSHI:'sushi',
  '1INCH':'1inch',          BAT:  'basic-attention-token',
  KNC:  'kyber-network-crystal', REN:  'republic-protocol',
  ZRX:  'ox',               BNT:  'bancor',
  GRT:  'the-graph',        ENJ:  'enjincoin',
  FTT:  'ftx-token',        CHZ:  'chiliz',
  CRO:  'crypto-com-chain',
  ANKR: 'ankr',             ANT:  'aragon',
  AUDIO:'audius',           AXS:  'axie-infinity',
  AVAX: 'avalanche-2',
  FET:  'fetch-ai',         FTM:  'fantom',
  GLM:  'golem',            GNO:  'gnosis',
  OMG:  'omisego',          XRP:  'ripple',
  CEL:  'celsius-degree-token',
  // Long tail (added after audit)
  ADX:  'adex',             ADX1: 'adex',
  AKRO: 'akropolis',
  ALCX: 'alchemix',
  ALEPH:'aleph',
  ALPHA:'alpha-finance',
  AMP:  'amp-token',
  ANC:  'anchor-protocol',
  AUCTION:'auction',
  BADGER:'badger-dao',
  BNB:  'binancecoin',
  BCSI: 'basecoin-ai',
  BONDLY:'bondly',
  BTM:  'bytom',
  BTMX: 'bmax',
  BZRX: 'bzx-protocol',
  CAPS: 'ternoa',
  CDAI: 'cdai',
  CELR: 'celer-network',
  CETH: 'compound-ether',
  CGT:  'cache-gold',
  CRU:  'crust-network',
  CUNI: 'compound-uniswap',
  CUSDT:'compound-usdt',
  CVC:  'civic',
  DENT: 'dent',
  DIA:  'dia-data',
  DODO: 'dodo',
  ELF:  'aelf',
  ERN:  'ethernity-chain',
  FARM: 'harvest-finance',
  FIS:  'stafi',
  FRONT:'frontier-token',
  FX:   'fx-coin',
  FUN:  'funfair',
  HOT:  'holotoken',
  IDEX: 'aurora-dao',
  TEL:  'telcoin',
  RARI: 'rarible',
  REP:  'augur',
  STORJ:'storj',
  SUPER:'superfarm',
  TRB:  'tellor',
  UMA:  'uma',
  WAVES:'waves',
  WNXM: 'wrapped-nxm',
  WOO:  'woo-network',
  XMR:  'monero',
  XLM:  'stellar',
  LTC:  'litecoin',
  BCH:  'bitcoin-cash',
  DOGE: 'dogecoin',
  SHIB: 'shiba-inu',
  // Second wave — audit 2026-04-21. Every ID verified against CoinGecko
  // /coins/list; 3 symbols still have no CG mapping (CBTC/CRE/FOTO) and
  // intentionally render empty mcap/supply instead of fake data.
  ACH:   'alchemy-pay',
  APE:   'apecoin',
  BICO:  'biconomy',
  BOND:  'barnbridge',                    // not "barnbridge-governance-token"
  CREAM: 'cream-2',
  DNT:   'district0x',
  EXRD:  'e-radix',
  FLX:   'reflexer-ungovernance-token',
  GAS:   'gas',
  GNY:   'gny',
  GT:    'gatechain-token',
  IOTX:  'iotex',
  JASMY: 'jasmycoin',
  JST:   'just',
  LQTY:  'liquity',
  LRC:   'loopring',
  MANA:  'decentraland',
  MATIC: 'matic-network',
  MLN:   'melon',
  MPH:   '88mph',
  NEXO:  'nexo',
  NKN:   'nkn',
  NMR:   'numeraire',
  OCEAN: 'ocean-protocol',
  OXT:   'orchid-protocol',
  PAXG:  'pax-gold',
  PERP:  'perpetual-protocol',
  POWR:  'power-ledger',
  QKC:   'quark-chain',
  QNT:   'quant-network',
  RAD:   'radicle',
  REQ:   'request-network',
  RGT:   'rari-governance-token',
  RUNE:  'thorchain',
  SKL:   'skale',
  SPELL: 'spell-token',
  SRM:   'serum',
  STAKE: 'xdai-stake',
  STG:   'stargate-finance',
  TLM:   'alien-worlds',
  TRIBE: 'tribe-2',
  TRU:   'truefi',
  TWT:   'trust-wallet-token',
};

function coingeckoId(symbol) {
  if (!symbol) return null;
  return COINGECKO_IDS[String(symbol).toUpperCase()] || null;
}

// ---- CoinGecko client with a 24h localStorage cache ----------------------
// We use /coins/markets instead of /simple/price because only /markets exposes
// circulating_supply + total_supply. Results are cached per-symbol with a 24h
// TTL so typical sessions hit localStorage, not the network.
const COINGECKO_MARKETS = 'https://api.coingecko.com/api/v3/coins/markets';
// v2: invalidate caches from before the 2026-04-21 audit that added 43 new
// CG mappings (MATIC/MANA/NEXO/RUNE/…). Bumping forces a refetch so clients
// don't show blank mcap/supply for tokens that now have valid IDs.
const CG_CACHE_KEY = 'sm.coingeckoCache.v2';
const CG_TTL_MS = 24 * 60 * 60 * 1000; // 24h

function _cgReadCache() {
  try { return JSON.parse(localStorage.getItem(CG_CACHE_KEY) || '{}'); } catch { return {}; }
}
function _cgWriteCache(obj) {
  try { localStorage.setItem(CG_CACHE_KEY, JSON.stringify(obj)); } catch {}
}

// Batch fetch CoinGecko markets data. Only pulls symbols missing from cache
// or older than 24h. Returns { [symbol]: { price, marketCap, totalSupply, … } }.
async function fetchExternalMarketData(symbols) {
  const cache = _cgReadCache();
  const now = Date.now();
  const idsNeeded = [];
  const idToSym = {};
  for (const sym of symbols) {
    const id = coingeckoId(sym);
    if (!id) continue;
    const hit = cache[sym];
    if (hit && (now - hit.ts) < CG_TTL_MS) continue;
    idsNeeded.push(id);
    idToSym[id] = sym;
  }
  if (idsNeeded.length > 0) {
    // CoinGecko caps /coins/markets at 250 ids per page; chunk for safety.
    const CHUNK = 100;
    for (let i = 0; i < idsNeeded.length; i += CHUNK) {
      const slice = idsNeeded.slice(i, i + CHUNK);
      try {
        const q = new URLSearchParams({
          vs_currency: 'usd',
          ids: slice.join(','),
          per_page: String(slice.length),
          page: '1',
          sparkline: 'false',
          price_change_percentage: '24h',
        });
        const r = await fetch(COINGECKO_MARKETS + '?' + q.toString());
        if (!r.ok) continue;
        const arr = await r.json();
        if (!Array.isArray(arr)) continue;
        for (const row of arr) {
          const sym = idToSym[row.id];
          if (!sym) continue;
          cache[sym] = {
            ts: now,
            data: {
              price: Number(row.current_price) || 0,
              marketCap: Number(row.market_cap) || 0,
              totalSupply: Number(row.total_supply) || 0,
              circulatingSupply: Number(row.circulating_supply) || 0,
              volume24h: Number(row.total_volume) || 0,
              change24h: Number(row.price_change_percentage_24h) || 0,
              logo: row.image,
            },
          };
        }
        _cgWriteCache(cache);
      } catch (_) { /* keep stale cache on network error */ }
    }
  }
  const out = {};
  for (const sym of symbols) {
    const hit = cache[sym];
    if (hit?.data) out[sym] = hit.data;
  }
  return out;
}

// ---- Native token market data cache --------------------------------------
// Two-layer cache:
//   · in-memory (this tab) — no TTL, overwritten on fresh fetch.
//   · localStorage (cross-tab / cross-reload) — 24h TTL. The user sees data
//     immediately on page load; fresh fetches happen in background and update
//     state as results arrive.
const NATIVE_TTL_MS = 24 * 60 * 60 * 1000;           // 24h
// v3: invalidate caches populated by the previous XOR fallback logic that
// preferred /burns/supply over MOF. MOF is now the authoritative source for
// every native token.
const NATIVE_LS_KEY = 'sm.nativeMarketCache.v3';
const _nativeMem = new Map();

function _loadNativeLs() {
  try { return JSON.parse(localStorage.getItem(NATIVE_LS_KEY) || '{}'); } catch { return {}; }
}
function _saveNativeLs(obj) {
  try { localStorage.setItem(NATIVE_LS_KEY, JSON.stringify(obj)); } catch {}
}
// Hydrate memory cache from localStorage at boot so the first render has data.
(function _hydrateNativeCache() {
  const ls = _loadNativeLs();
  const now = Date.now();
  for (const [sym, entry] of Object.entries(ls)) {
    if (entry && (now - entry.ts) < NATIVE_TTL_MS) _nativeMem.set(sym, entry);
  }
})();

// Authoritative supply for SORA-native tokens: the MOF (Master of Funds)
// service exposes raw on-chain totals at https://mof.sora.org/qty/<sym>
// returning plain-text human-readable numbers (e.g. "56225280.2967..." VAL).
// MOF is the source of truth for every native token — including XOR. The
// large XOR number reflects real holdings (it counts a very large whale as
// well as every other account) and that's the correct on-chain total.
function _fetchMofSupply(symbol) {
  // mof.sora.org has no CORS headers, so we go through our server's MOF proxy
  // (same-origin /mof/qty/:sym). Locally v6-server.js serves it; in prod the
  // VPS has the equivalent route.
  const url = '/mof/qty/' + encodeURIComponent(String(symbol).toLowerCase());
  return fetch(url)
    .then(r => r.ok ? r.text() : null)
    .then(txt => {
      if (!txt) return null;
      const n = Number(String(txt).trim());
      return Number.isFinite(n) ? n : null;
    })
    .catch(() => null);
}

// Primary fetch — supply + price + mcap only. Fast because MOF is parallel
// and /burns/supply is a single small VPS query.
async function _fetchNativeDirect(symbol, assetId) {
  const [burns, mofSupply] = await Promise.all([
    fetch('/burns/supply/' + encodeURIComponent(symbol))
      .then(r => r.ok ? r.json() : null).catch(() => null),
    _fetchMofSupply(symbol),
  ]);
  const price = Number(burns?.price) || 0;
  // MOF is the canonical supply source for every native token. /burns/supply
  // is only a fallback when MOF is unreachable.
  const totalSupply = mofSupply != null ? mofSupply : (Number(burns?.totalSupply) || 0);
  // Market cap: always prefer the backend-provided value (CoinGecko-derived,
  // reflects circulating mcap). Only compute price × supply when burns has
  // nothing AND the supply is a reasonable magnitude — avoids absurd mcaps
  // on tokens where on-chain total ≫ circulating (e.g. XOR with a large
  // whale that inflates total but isn't part of circulating mcap).
  const marketCap = Number(burns?.marketCap)
    || (price > 0 && totalSupply > 0 && totalSupply < 1e12 ? price * totalSupply : 0);
  return { price, marketCap, totalSupply, holders: 0 };
}

// Secondary fetch — holders count only. Goes through the shared serial queue
// so we don't storm /holders. Returns null if it can't be fetched.
async function fetchNativeHolders(assetId) {
  if (!assetId || !window.getHoldersCached) return null;
  try {
    const { data } = await window.getHoldersCached(assetId, 1, 1);
    return Number(data?.totalHolders) || 0;
  } catch { return null; }
}

async function fetchNativeMarketData(symbol, assetId) {
  const hit = _nativeMem.get(symbol);
  const now = Date.now();
  if (hit && (now - hit.ts) < NATIVE_TTL_MS) return hit.data;
  // No serial chain anymore: MOF is external and /burns/supply is small. The
  // /holders call goes through getHoldersCached which has its own serial queue.
  const data = await _fetchNativeDirect(symbol, assetId);
  const entry = { ts: Date.now(), data };
  _nativeMem.set(symbol, entry);
  const ls = _loadNativeLs();
  ls[symbol] = entry;
  _saveNativeLs(ls);
  return data;
}

// Synchronous cache peek — lets the Tokens page paint cached values on first
// render before async re-validation fires.
function readNativeMarketCache(symbol) {
  const hit = _nativeMem.get(symbol);
  return hit ? hit.data : null;
}

Object.assign(window, {
  isSoraNative, coingeckoId,
  fetchExternalMarketData, fetchNativeMarketData, readNativeMarketCache,
  fetchNativeHolders,
  EXTERNAL_SYMBOLS, FORCE_NATIVE, COINGECKO_IDS,
});
