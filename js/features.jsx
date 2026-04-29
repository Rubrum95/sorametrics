/* global React, fmt, FAKE_ADDRS, IDENTITIES, TOKENS, useT, I */
/* =========================================================================
   FEATURES: toasts, global search (Cmd+K), wallet modals, CSV export,
             backup/restore.
   ========================================================================= */
const { useState, useEffect, useRef, useMemo, useCallback, createContext, useContext } = React;

/* ---------- Toast bus ---------- */
const ToastCtx = createContext({ push: () => {} });
function useToast() { return useContext(ToastCtx); }

function ToastProvider({ children }) {
  const [toasts, setToasts] = useState([]);
  const push = useCallback((msg, kind) => {
    const id = Math.random().toString(36).slice(2);
    setToasts(ts => [...ts, { id, msg, kind: kind || 'ok' }]);
    setTimeout(() => setToasts(ts => ts.filter(t => t.id !== id)), 2600);
  }, []);
  // Listen for peg-alert CustomEvents dispatched by the main.jsx peg-watcher.
  useEffect(() => {
    const onAlert = (ev) => {
      const { symbol, price, dev } = ev.detail || {};
      push('⚠ ' + symbol + ' depeg ' + (price >= 1 ? '+' : '-') + Math.abs(dev).toFixed(2) + '% · $' + Number(price).toFixed(4), 'err');
    };
    window.addEventListener('peg-alert', onAlert);
    return () => window.removeEventListener('peg-alert', onAlert);
  }, [push]);
  return (
    <ToastCtx.Provider value={{ push }}>
      {children}
      <div className="toast-stack">
        {toasts.map(t => (
          <div key={t.id} className={'toast toast-' + t.kind}>{t.msg}</div>
        ))}
      </div>
    </ToastCtx.Provider>
  );
}

/* ---------- Wallet store (shared, persisted) ---------- */
const WalletCtx = createContext(null);
function useWallets() { return useContext(WalletCtx); }

// Global helper: open the WalletDetailsModal anchored anywhere — swap row,
// transfer row, pool provider row, holder row, drill panel. Works because
// WalletDetailsProvider (rendered once at app root in main.jsx) sets the
// singleton setter on window.__SM_WALLET_DETAILS__ on mount.
//
// Buffer pattern: when this is called BEFORE the provider has mounted —
// e.g. from a section's mount-time useEffect, since React fires child
// effects before parent effects on initial mount — we stash the wallet on
// `window.__SM_WALLET_PENDING__`. The provider drains the buffer when its
// own useEffect runs. Without this, the deep-link from `/sorav2?tab=balance
// &address=<SS58>` (used by /minamoto's XOR migration table) was lost on
// page load: BalanceSection.useEffect ran first and silently no-op'd.
function openWalletDetails(addr, alias) {
  if (!addr) return false;
  const wallet = {
    addr,
    alias: alias || (addr.length > 14 ? addr.slice(0, 8) + '…' + addr.slice(-4) : addr),
    id: 'external-' + addr,
    kind: 'watch',
  };
  try {
    if (typeof window.__SM_WALLET_DETAILS__ === 'function') {
      window.__SM_WALLET_DETAILS__(wallet);
      return true;
    }
    // Provider not yet mounted — buffer for the provider's mount effect.
    window.__SM_WALLET_PENDING__ = wallet;
  } catch {}
  return false;
}

// Wraps the app. Renders a singleton WalletDetailsModal that any section
// can open via openWalletDetails(addr). If the addr isn't in the wallet
// store, we fetch /balance/:addr on-the-fly so the Assets tab has data.
function WalletDetailsProvider({ children }) {
  const [external, setExternal] = useState(null);
  const [externalTokens, setExternalTokens] = useState([]);
  const store = useContext(WalletCtx);

  useEffect(() => {
    window.__SM_WALLET_DETAILS__ = (w) => setExternal(w);
    // Drain any wallet that openWalletDetails buffered before we mounted.
    // Happens on deep-link entry where a child section's mount effect calls
    // openWalletDetails before this provider's effect has run.
    if (window.__SM_WALLET_PENDING__) {
      setExternal(window.__SM_WALLET_PENDING__);
      delete window.__SM_WALLET_PENDING__;
    }
    return () => { if (window.__SM_WALLET_DETAILS__) delete window.__SM_WALLET_DETAILS__; };
  }, []);

  // When opening an address that's NOT in the user's wallet store, fetch
  // its balance directly from prod so Assets tab renders the real tokens.
  useEffect(() => {
    if (!external?.addr) { setExternalTokens([]); return; }
    const known = store?.wallets?.find(w => w.addr === external.addr) || store?.watched?.find(w => w.addr === external.addr);
    if (known && Array.isArray(known.tokens) && known.tokens.length > 0) {
      setExternalTokens(known.tokens);
      return;
    }
    let cancelled = false;
    fetch('/balance/' + encodeURIComponent(external.addr))
      .then(r => r.ok ? r.json() : [])
      .then(j => { if (!cancelled) setExternalTokens(Array.isArray(j) ? j : []); })
      .catch(() => {});
    return () => { cancelled = true; };
  }, [external?.addr, store?.wallets]);

  const wallet = external ? {
    ...external,
    tokens: externalTokens.length > 0
      ? externalTokens
      : (store?.wallets?.find(w => w.addr === external.addr)?.tokens || []),
  } : null;

  return (
    <>
      {children}
      <WalletDetailsModal wallet={wallet} open={!!wallet} onClose={() => setExternal(null)}/>
    </>
  );
}

// Real SS58 addresses that DO have on-chain balances on prod sorametrics.org.
// Used as seed data so new visitors immediately see live numbers without
// having to paste addresses themselves.
// Verified SS58 addresses from prod's /holders/XOR top list (2026-04-20).
// Must have a valid SORA checksum — the node's runtime decode rejects bad ones.
const INITIAL_WALLETS = [
  { id:'w1', alias: 'Polkaswap Treasury', addr: 'cnRwt3q7DkvJqr3YkuN7dFibTx6yu8rqDDYKmBp4Sko5TW2Dd', value: 0, live: false, kind:'watch' },
  { id:'w2', alias: 'XOR Whale',          addr: 'cnVhh27kkYkfJ1mH4jyPWWV6Tq4vfWjrC7Avnf3Bq1ZawcTgQ', value: 0, live: false, kind:'watch' },
  { id:'w3', alias: 'DEX Maker',          addr: 'cnSpcE5QSjKf9x2yJgv3h5orz2oGrmWzUJHxMBtJvZu3V3orN', value: 0, live: false, kind:'watch' },
  { id:'w4', alias: 'Active Trader',      addr: 'cnRWUatULzf8AVfKVexXMKJx9bT9UeTEM4wmWG7B7g1cxVjXy', value: 0, live: false, kind:'watch' },
];
// Verified SS58 holders from prod's /holders/XOR top list.
const INITIAL_WATCHED = [
  { id:'v1', alias: 'XOR Holder #1', addr: 'cnV2jLWvHYE54sbh34Xvbmt2UcHs3KcahNYN5sbP3W7nDYNrv', value: 0 },
  { id:'v2', alias: 'XOR Holder #2', addr: 'cnWX6Y7dT9FEY8DX9BcxQJyCRaHwYZYRoW2ENcwLgfoHhz8aP', value: 0 },
  { id:'v3', alias: 'XOR Holder #3', addr: 'cnSGSCSMgDFRH6zKvYA75CAyruJhBokQ4csX9uA2rZ6Rq4NN4', value: 0 },
];

function loadLS(k, fallback) {
  try { const s = localStorage.getItem(k); if (s) return JSON.parse(s); } catch (_) {}
  return fallback;
}
function saveLS(k, v) { try { localStorage.setItem(k, JSON.stringify(v)); } catch (_) {} }

// -------------------------------------------------------------------------
// v1 → v2 wallet migration
// v1 stored wallets under `sora_wallets` with shape { address, name, type }.
// v2 uses `sm.wallets` + `sm.watched` with shape { id, alias, addr, kind }.
// On first visit after upgrade we read v1's key, split entries into the two
// v2 arrays, and save. Ran ONCE per browser — guarded by a sentinel flag.
// Notes:
//   · v1 `type` values observed: 'seed' | 'key' | 'watch'. Since v2 is read
//     only by design (no custody), every v1 entry lands as a watch-only
//     wallet in v2. The user's own seed-imported wallets in v1 still map to
//     v2's `wallets[]` (with kind:'watch') so they show up in Portfolio —
//     they just can't sign anything here, which is correct.
//   · The v1 localStorage key is NOT deleted. If the user reverts to v1
//     everything still works. v2's migration is purely additive.
// -------------------------------------------------------------------------
const V1_MIGRATION_KEY = 'sm.v1Migrated';

function migrateV1WalletsOnce() {
  if (typeof localStorage === 'undefined') return { migrated: 0 };
  if (localStorage.getItem(V1_MIGRATION_KEY) === '1') return { migrated: 0 };
  let raw;
  try { raw = localStorage.getItem('sora_wallets'); }
  catch { return { migrated: 0 }; }
  if (!raw) {
    try { localStorage.setItem(V1_MIGRATION_KEY, '1'); } catch {}
    return { migrated: 0 };
  }
  let v1list;
  try { v1list = JSON.parse(raw); }
  catch { v1list = null; }
  if (!Array.isArray(v1list) || v1list.length === 0) {
    try { localStorage.setItem(V1_MIGRATION_KEY, '1'); } catch {}
    return { migrated: 0 };
  }
  const wallets = [];
  const watched = [];
  for (const w of v1list) {
    const addr = w?.address || w?.addr;
    if (!addr) continue;
    const alias = w?.name || w?.alias || (addr.slice(0, 6) + '…' + addr.slice(-4));
    const entry = { id: 'm' + Date.now() + Math.random().toString(36).slice(2, 6), alias, addr, value: 0, live: false };
    if ((w?.type || w?.kind) === 'watch') {
      watched.push(entry);
    } else {
      // seed / key / unknown → wallet slot with kind:'watch' (read-only v2)
      wallets.push({ ...entry, kind: 'watch' });
    }
  }
  // Merge with any v2 data that might already be there (rare — but be safe).
  const mergeByAddr = (existing, incoming) => {
    const out = [...existing];
    const seen = new Set(existing.map(x => x.addr));
    for (const x of incoming) if (!seen.has(x.addr)) { out.push(x); seen.add(x.addr); }
    return out;
  };
  try {
    const existingW = JSON.parse(localStorage.getItem('sm.wallets') || '[]');
    const existingV = JSON.parse(localStorage.getItem('sm.watched') || '[]');
    const finalW = mergeByAddr(Array.isArray(existingW) ? existingW : [], wallets);
    const finalV = mergeByAddr(Array.isArray(existingV) ? existingV : [], watched);
    localStorage.setItem('sm.wallets', JSON.stringify(finalW));
    localStorage.setItem('sm.watched', JSON.stringify(finalV));
    // Prevent the auto-seeding logic from overwriting the migrated data on
    // the same load: those sentinels are what distinguish "first visit" from
    // "user already had data".
    localStorage.setItem('sm.wallets.seeded', '1');
    localStorage.setItem('sm.watched.seeded', '1');
    localStorage.setItem(V1_MIGRATION_KEY, '1');
    // Also migrate language + favorites if present.
    const v1lang = localStorage.getItem('sora_lang');
    if (v1lang && !localStorage.getItem('sorametrics.lang')) {
      localStorage.setItem('sorametrics.lang', v1lang);
    }
    const v1favs = localStorage.getItem('sora_favorites');
    if (v1favs && !localStorage.getItem('sm.favTokens')) {
      try {
        const parsed = JSON.parse(v1favs);
        if (Array.isArray(parsed)) localStorage.setItem('sm.favTokens', JSON.stringify(parsed));
      } catch {}
    }
    return { migrated: wallets.length + watched.length };
  } catch (e) {
    return { migrated: 0, error: String(e) };
  }
}
// Run synchronously at module load so WalletProvider's useState initialiser
// sees the migrated data in localStorage already.
migrateV1WalletsOnce();

// -------------------------------------------------------------------------
// v1 backup-file parser (restore compatibility).
// v1 saves { wallets: stringified-JSON-array, favorites, lang, timestamp }.
// v2 saves { version:1+, settings, favorites:{tokens,wallets}, watchlist, wallets, recentSearches }.
// parseV1Backup() returns null if the file isn't v1, or the mapped v2-shape
// payload if it is. `restoreBackup()` calls it first to widen compatibility.
// -------------------------------------------------------------------------
function parseV1Backup(data) {
  if (!data || typeof data !== 'object') return null;
  // v1 key: `wallets` is a stringified array. v2 key: it's an array. Split
  // on that.
  if (typeof data.wallets !== 'string') return null;
  let v1list;
  try { v1list = JSON.parse(data.wallets); } catch { return null; }
  if (!Array.isArray(v1list)) return null;
  const wallets = [];
  const watched = [];
  for (const w of v1list) {
    const addr = w?.address || w?.addr;
    if (!addr) continue;
    const alias = w?.name || w?.alias || (addr.slice(0, 6) + '…' + addr.slice(-4));
    const entry = { id: 'r' + Date.now() + Math.random().toString(36).slice(2, 6), alias, addr, value: 0, live: false };
    if ((w?.type || w?.kind) === 'watch') watched.push(entry);
    else wallets.push({ ...entry, kind: 'watch' });
  }
  let favTokens = [];
  if (typeof data.favorites === 'string') {
    try { const p = JSON.parse(data.favorites); if (Array.isArray(p)) favTokens = p; } catch {}
  }
  // Return in the v2 shape that restoreBackup consumes.
  return {
    wallets,
    watchlist: watched,
    favorites: { tokens: favTokens, wallets: [] },
    settings: { lang: data.lang || null },
    _legacy: 'v1',
  };
}

// Cache of real balance data per address, keyed by SS58. Prod's
// GET /balance/:addr returns the full token list with usdValue, while
// POST /balances returns a stub with empty tokens — so we do N concurrent
// GETs for our 4-10 wallets rather than a batch POST.
const _balanceCache = new Map();
async function fetchBalances(addresses) {
  if (!addresses.length) return {};
  const pairs = await Promise.all(addresses.map(async (addr) => {
    try {
      const r = await fetch('/balance/' + encodeURIComponent(addr));
      if (!r.ok) return [addr, []];
      const j = await r.json();
      return [addr, Array.isArray(j) ? j : []];
    } catch { return [addr, []]; }
  }));
  return Object.fromEntries(pairs);
}

function WalletProvider({ children }) {
  // Distinguish "first visit" (no key in LS at all) from "user cleared the list"
  // (key exists, value is []). Without the sentinel, loadLS returns INITIAL_WALLETS
  // both times — the user can't actually remove the seeded defaults.
  const [wallets, setWallets] = useState(() => {
    const seenKey = 'sm.wallets.seeded';
    const alreadySeeded = typeof localStorage !== 'undefined' && localStorage.getItem(seenKey) === '1';
    if (alreadySeeded) return loadLS('sm.wallets', []); // honour user's deletions
    try { localStorage.setItem(seenKey, '1'); } catch {}
    return loadLS('sm.wallets', INITIAL_WALLETS);
  });
  const [watched, setWatched] = useState(() => {
    const seenKey = 'sm.watched.seeded';
    const alreadySeeded = typeof localStorage !== 'undefined' && localStorage.getItem(seenKey) === '1';
    if (alreadySeeded) return loadLS('sm.watched', []);
    try { localStorage.setItem(seenKey, '1'); } catch {}
    return loadLS('sm.watched', INITIAL_WATCHED);
  });
  const [balances, setBalances] = useState(() => ({})); // addr → tokens[]
  useEffect(() => saveLS('sm.wallets', wallets), [wallets]);
  useEffect(() => saveLS('sm.watched', watched), [watched]);

  // Pull real balances from prod for all addresses (wallets + watched).
  // Refreshes every 60s + on mount / wallet-list change.
  useEffect(() => {
    const addrs = [...new Set([...wallets, ...watched].map(w => w.addr).filter(Boolean))];
    let cancelled = false;
    const pull = async () => {
      const next = await fetchBalances(addrs);
      if (cancelled) return;
      // merge into cache so tokens persist across re-renders when addrs unchanged.
      for (const [a, t] of Object.entries(next)) _balanceCache.set(a, t);
      setBalances({ ..._balanceCache.toJSON ? _balanceCache : Object.fromEntries(_balanceCache) });
    };
    pull();
    const id = setInterval(pull, 60_000);
    return () => { cancelled = true; clearInterval(id); };
  // We don't want to re-fetch on every wallets change — only when addresses change.
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [wallets.map(w => w.addr).join(','), watched.map(w => w.addr).join(',')]);

  // Enrich wallet/watched with tokens + a naive total (sum of token amounts — unitless;
  // real USD needs price lookup which we'll add in Phase 5 via /currency-rates + /tokens).
  const walletsWithData = wallets.map(w => ({ ...w, tokens: balances[w.addr] || [] }));
  const watchedWithData = watched.map(w => ({ ...w, tokens: balances[w.addr] || [] }));

  // Move helpers — relocate a wallet between the two lists preserving alias
  // and address. Used by the WalletDetails modal when the user wants to
  // reclassify a wallet (e.g. a restored address that should actually be
  // their own, or a wallet they no longer control).
  const moveToWatched = (id) => {
    let moved = null;
    setWallets(ws => {
      moved = ws.find(w => w.id === id) || null;
      return ws.filter(w => w.id !== id);
    });
    // setState is async — defer the watched push to next tick so we read the
    // latest list. React batches; we use the functional form to be safe.
    setTimeout(() => {
      if (!moved) return;
      setWatched(ws => [...ws, { id:'v'+Date.now(), alias: moved.alias, addr: moved.addr, value: 0 }]);
    }, 0);
  };
  const moveToWallets = (id) => {
    let moved = null;
    setWatched(ws => {
      moved = ws.find(w => w.id === id) || null;
      return ws.filter(w => w.id !== id);
    });
    setTimeout(() => {
      if (!moved) return;
      setWallets(ws => [...ws, { id:'w'+Date.now(), alias: moved.alias, addr: moved.addr, value: 0, live: false, kind: 'watch' }]);
    }, 0);
  };
  const api = {
    wallets: walletsWithData, watched: watchedWithData, setWallets, setWatched,
    addWallet: (w) => setWallets(ws => [...ws, { id:'w'+Date.now(), value: 0, live: false, ...w }]),
    addWatched: (w) => setWatched(ws => [...ws, { id:'v'+Date.now(), value: 0, ...w }]),
    removeWallet:  (id) => setWallets(ws => ws.filter(w => w.id !== id)),
    removeWatched: (id) => setWatched(ws => ws.filter(w => w.id !== id)),
    renameWallet:  (id, alias) => setWallets(ws => ws.map(w => w.id === id ? { ...w, alias } : w)),
    renameWatched: (id, alias) => setWatched(ws => ws.map(w => w.id === id ? { ...w, alias } : w)),
    moveToWatched, moveToWallets,
  };
  return <WalletCtx.Provider value={api}>{children}</WalletCtx.Provider>;
}

/* =========================================================================
   MODAL shell
   ========================================================================= */
function Modal({ open, onClose, children, width = 520, label }) {
  useEffect(() => {
    if (!open) return;
    const onKey = (e) => { if (e.key === 'Escape') onClose(); };
    window.addEventListener('keydown', onKey);
    return () => window.removeEventListener('keydown', onKey);
  }, [open, onClose]);
  if (!open) return null;
  return (
    <div className="sm-modal-backdrop" onClick={onClose} role="dialog" aria-label={label || ''}>
      <div className="sm-modal" style={{ width }} onClick={e => e.stopPropagation()}>
        {children}
      </div>
    </div>
  );
}

/* =========================================================================
   GLOBAL SEARCH PALETTE
   ========================================================================= */
// Local index — ONLY real data the app already knows about: user's wallets +
// registered tokens. Everything else (blocks, hashes, extrinsics, pools,
// validators) is resolved server-side via /search?q= so we never ship mocked
// identifiers or fabricated stats like "1240 calls 24h" or fake TVL.
function buildSearchIndex() {
  const out = [];
  const wallets = loadLS('sm.wallets', INITIAL_WALLETS);
  const watched = loadLS('sm.watched', INITIAL_WATCHED);
  wallets.forEach(w => out.push({
    type:'wallet', primary: w.alias,
    secondary: fmt.addr(w.addr, 10, 8) + ' · ' + (w.kind || 'owned'),
    raw: w.addr, section:'balance',
  }));
  watched.forEach(w => out.push({
    type:'wallet', primary: w.alias + '  (watch)',
    secondary: fmt.addr(w.addr, 10, 8) + ' · watched',
    raw: w.addr, section:'balance',
  }));
  // Known token symbols — points the user at Tokens or Holders. Descriptions
  // are intentionally generic to avoid claiming stats we haven't fetched.
  Object.keys(TOKENS || {}).forEach(k => {
    out.push({ type:'token', primary: k, secondary: 'token · open in Tokens', raw: k, section:'tokens' });
  });
  return out;
}

const TYPE_LABELS = {
  all: { en:'All',        es:'Todos',     fr:'Tout',     de:'Alle',      ja:'すべて' },
  wallet:{ en:'Wallets', es:'Carteras',   fr:'Wallets',  de:'Wallets',   ja:'ウォレット' },
  tx:    { en:'Tx',      es:'Tx',         fr:'Tx',       de:'Tx',        ja:'Tx' },
  block: { en:'Blocks',  es:'Bloques',    fr:'Blocs',    de:'Blöcke',    ja:'ブロック' },
  extrinsic:{ en:'Extrinsics', es:'Extrinsics', fr:'Extrinsics', de:'Extrinsics', ja:'Extrinsics' },
  token: { en:'Tokens',  es:'Tokens',     fr:'Tokens',   de:'Tokens',    ja:'トークン' },
  pool:  { en:'Pools',   es:'Pools',      fr:'Pools',    de:'Pools',     ja:'プール' },
  validator:{ en:'Validators', es:'Validadores', fr:'Validateurs', de:'Validatoren', ja:'バリデータ' },
};
function typeLbl(t, lang) {
  const e = TYPE_LABELS[t]; if (!e) return t;
  return e[lang] || e.en;
}
const TYPE_ICON = {
  wallet:'wallet', tx:'swap', block:'ext', extrinsic:'ext',
  token:'tokens', pool:'pools', validator:'stake',
};

/* fuzzy score: substring first, then char-sequence */
function fuzzy(q, text) {
  if (!q) return 1;
  const t = text.toLowerCase(); const s = q.toLowerCase();
  if (t.includes(s)) return 10 - (t.indexOf(s) / Math.max(t.length, 1));
  let ti = 0, hits = 0;
  for (let qi = 0; qi < s.length; qi++) {
    while (ti < t.length && t[ti] !== s[qi]) ti++;
    if (ti >= t.length) return 0;
    hits++; ti++;
  }
  return hits / s.length;
}

const SearchCtx = createContext({ open: () => {} });
function useSearch() { return useContext(SearchCtx); }

function GlobalSearchProvider({ children }) {
  const [open, setOpen] = useState(false);
  const api = { open: () => setOpen(true), close: () => setOpen(false) };
  useEffect(() => {
    const onKey = (e) => {
      const meta = e.metaKey || e.ctrlKey;
      if (meta && (e.key === 'k' || e.key === 'K')) {
        e.preventDefault();
        setOpen(o => !o);
      }
    };
    window.addEventListener('keydown', onKey);
    return () => window.removeEventListener('keydown', onKey);
  }, []);
  return (
    <SearchCtx.Provider value={api}>
      {children}
      <CommandPalette open={open} onClose={() => setOpen(false)}/>
    </SearchCtx.Provider>
  );
}

function CommandPalette({ open, onClose }) {
  const t = useT ? useT() : ((k) => k);
  const lang = (typeof window !== 'undefined' && window.__CURRENT_LANG__) || 'es';
  const [q, setQ] = useState('');
  const [type, setType] = useState('all');
  const [cursor, setCursor] = useState(0);
  const [recents, setRecents] = useState(() => loadLS('sm.searchRecent', []));
  const inputRef = useRef(null);
  const toast = useToast();

  const index = useMemo(() => buildSearchIndex(), [open]);

  // Real prod search: GET /search?q=... returns {type, data} with types
  // "tokens" / "wallet" / "block" / "tx" / "extrinsic". We debounce the query
  // and merge the prod hit into the local index so the palette shows both.
  const [realHits, setRealHits] = useState([]);
  useEffect(() => {
    if (!q.trim() || q.length < 2) { setRealHits([]); return; }
    const h = setTimeout(async () => {
      try {
        const r = await fetch('/search?q=' + encodeURIComponent(q.trim()));
        if (!r.ok) return;
        const j = await r.json();
        const hits = [];
        if (j.type === 'tokens' && Array.isArray(j.data)) {
          j.data.forEach(tk => hits.push({
            type: 'token', primary: tk.symbol, secondary: 'asset ' + (tk.assetId ? tk.assetId.slice(0, 10) + '…' : ''),
            raw: tk.symbol, section: 'tokens', real: true,
          }));
        } else if (j.type === 'wallet' && j.data) {
          hits.push({
            type: 'wallet', primary: j.data.address, secondary: 'on-chain · live balance',
            raw: j.data.address, section: 'balance', real: true,
          });
        } else if (j.type === 'block' && j.data) {
          hits.push({
            type: 'block', primary: 'Block #' + Number(j.data.block).toLocaleString(),
            secondary: 'finalized',
            raw: String(j.data.block), block: Number(j.data.block),
            section: 'extrinsics', real: true,
          });
        } else if ((j.type === 'tx' || j.type === 'extrinsic') && j.data) {
          hits.push({
            type: j.type, primary: j.data.hash || j.data.extrinsic_id || q,
            secondary: j.type + ' · block ' + (j.data.block || '?'),
            raw: j.data.hash || j.data.extrinsic_id,
            // Preserve the block so navigation can server-filter Extrinsics
            // by block=<n>; the user sees the exact extrinsic in context.
            block: j.data.block != null ? Number(j.data.block) : null,
            section: 'extrinsics', real: true,
          });
        }
        setRealHits(hits);
      } catch { setRealHits([]); }
    }, 200);
    return () => clearTimeout(h);
  }, [q]);

  const results = useMemo(() => {
    let base = type === 'all' ? index : index.filter(r => r.type === type);
    if (!q.trim()) return base.slice(0, 40);
    const fuzzyMatches = base
      .map(r => ({ r, s: Math.max(fuzzy(q, r.primary), fuzzy(q, r.secondary) * 0.6, fuzzy(q, r.raw) * 0.8) }))
      .filter(x => x.s > 0.2)
      .sort((a,b) => b.s - a.s)
      .slice(0, 40)
      .map(x => x.r);
    // Prepend real prod hits so they rank above mocks.
    return [...realHits, ...fuzzyMatches];
  }, [q, type, index, realHits]);

  // grouped for display
  const grouped = useMemo(() => {
    const g = {};
    results.forEach(r => { (g[r.type] = g[r.type] || []).push(r); });
    return g;
  }, [results]);
  const flat = useMemo(() => Object.values(grouped).flat(), [grouped]);

  useEffect(() => { if (open) setTimeout(() => inputRef.current?.focus(), 40); }, [open]);
  useEffect(() => { setCursor(0); }, [q, type]);

  // Type-specific routing — every branch uses the REAL identifier the user
  // searched for. No FAKE_ADDRS / mocked blocks (that's what produced the
  // phantom #21418802 earlier).
  //   wallet   → open the live wallet-details modal (loads /balance/:addr).
  //   block    → stash the number in window.__SM_SEARCH_BLOCK__ and navigate
  //              to Extrinsics (the section reads it on mount to filter).
  //   tx/ext   → stash the hash / extrinsic id under __SM_SEARCH_HASH__ and
  //              navigate to Extrinsics.
  //   token    → navigate to the Tokens section (which handles symbol in URL).
  const commit = useCallback((r) => {
    if (!r) return;
    const next = [{ q: r.primary, ts: Date.now() }, ...recents.filter(x => x.q !== r.primary)].slice(0, 6);
    setRecents(next); saveLS('sm.searchRecent', next);
    try {
      if (r.type === 'wallet' && r.raw) {
        if (typeof window.openWalletDetails === 'function') {
          window.openWalletDetails(r.raw, r.primary && r.primary !== r.raw ? r.primary : undefined);
        } else {
          window.__SM_NAV__?.('balance');
        }
      } else if (r.type === 'block' && r.raw) {
        // Open the Block Explorer modal in Network Pulse instead of filtering
        // Extrinsics — a block may have only events / system calls and no
        // user-signed extrinsics, in which case the Extrinsics page would
        // (correctly) show "no extrinsics match your filters". The Block
        // Explorer always renders the block range, so the user gets context.
        const _n = Number(r.block ?? r.raw);
        if (Number.isFinite(_n) && _n > 0) {
          window.__SM_NAV__?.('pulse');
          setTimeout(() => {
            window.dispatchEvent(new CustomEvent('sm:openBlockExplorer', { detail: { block: _n } }));
          }, 30);
        }
      } else if ((r.type === 'tx' || r.type === 'extrinsic') && (r.block || r.raw)) {
        // Prefer filtering by block (backend supports it); keep the hash so
        // the Extrinsics view can highlight or pre-expand the matching row.
        if (r.block) window.__SM_SEARCH_BLOCK__ = String(r.block);
        window.__SM_SEARCH_HASH__ = String(r.raw || '');
        window.__SM_NAV__?.('extrinsics');
      } else if (r.section) {
        window.__SM_NAV__?.(r.section);
      }
    } catch (_) { /* noop — navigation is best-effort */ }
    onClose();
    toast.push(r.primary + ' →', 'ok');
  }, [recents, onClose, toast]);

  const onKey = (e) => {
    if (e.key === 'ArrowDown') { e.preventDefault(); setCursor(c => Math.min(flat.length - 1, c + 1)); }
    else if (e.key === 'ArrowUp') { e.preventDefault(); setCursor(c => Math.max(0, c - 1)); }
    else if (e.key === 'Enter') {
      e.preventDefault();
      // If the user typed a pure number and there is no real backend hit yet
      // (or the cursor result is not a block), jump straight to Block Explorer.
      // Lets users type "25852498" + Enter without waiting for /search?q= to
      // return — the explorer shows the range regardless of whether the block
      // had user-signed extrinsics.
      const trimmed = String(q || '').trim();
      if (/^\d+$/.test(trimmed)) {
        const n = Number(trimmed);
        if (Number.isFinite(n) && n > 0) {
          window.__SM_NAV__?.('pulse');
          setTimeout(() => {
            window.dispatchEvent(new CustomEvent('sm:openBlockExplorer', { detail: { block: n } }));
          }, 30);
          onClose();
          return;
        }
      }
      commit(flat[cursor], e.metaKey || e.ctrlKey);
    }
  };

  if (!open) return null;
  const typeList = ['all','wallet','tx','block','extrinsic','token','pool','validator'];

  return (
    <div className="sm-modal-backdrop palette-backdrop" onClick={onClose}>
      <div className="sm-palette" onClick={e => e.stopPropagation()} onKeyDown={onKey}>
        <div className="palette-inputwrap">
          <I.search style={{width:16, height:16, opacity:0.7}}/>
          <input
            ref={inputRef}
            className="palette-input"
            value={q}
            placeholder={lang === 'es' ? 'Buscar por dirección, hash, bloque…' : (lang === 'ja' ? 'アドレス、ハッシュ、ブロックで検索…' : 'Search address, hash, block…')}
            onChange={e => setQ(e.target.value)}
          />
          <kbd className="palette-kbd">ESC</kbd>
        </div>

        <div className="palette-tabs">
          {typeList.map(tk => (
            <button key={tk}
              className={'palette-tab' + (type === tk ? ' active' : '')}
              onClick={() => setType(tk)}>
              {typeLbl(tk, lang)}
            </button>
          ))}
        </div>

        <div className="palette-results">
          {!q.trim() && recents.length > 0 && (
            <div className="palette-group">
              <div className="palette-grouptitle">{lang === 'es' ? 'Recientes' : 'Recent'}</div>
              {recents.map((r, i) => (
                <div key={i} className="palette-row palette-recent" onClick={() => setQ(r.q)}>
                  <I.search style={{width:14,height:14, opacity:0.5}}/>
                  <span className="palette-primary">{r.q}</span>
                  <span className="palette-secondary muted">{fmt.ago(r.ts)} ago</span>
                </div>
              ))}
            </div>
          )}

          {flat.length === 0 && (
            <div className="palette-empty">
              <div style={{fontSize: 32, opacity:0.3, marginBottom: 8}}>⌕</div>
              <div className="muted">{lang === 'es' ? 'Buscar por dirección, hash, bloque…' : 'No matches'}</div>
            </div>
          )}

          {Object.entries(grouped).map(([tk, rows]) => {
            let offset = 0;
            for (const [tk2, rs2] of Object.entries(grouped)) {
              if (tk2 === tk) break;
              offset += rs2.length;
            }
            return (
              <div key={tk} className="palette-group">
                <div className="palette-grouptitle">{typeLbl(tk, lang)} · {rows.length}</div>
                {rows.map((r, i) => {
                  const idx = offset + i;
                  const Icon = I[TYPE_ICON[r.type] || 'search'];
                  return (
                    <div key={idx}
                         className={'palette-row' + (idx === cursor ? ' active' : '')}
                         onMouseEnter={() => setCursor(idx)}
                         onClick={(e) => commit(r, e.metaKey || e.ctrlKey)}>
                      {Icon && <Icon style={{width:14,height:14, opacity:0.7}}/>}
                      <span className="palette-primary">{r.primary}</span>
                      <span className="palette-secondary muted">{r.secondary}</span>
                    </div>
                  );
                })}
              </div>
            );
          })}
        </div>

        <div className="palette-foot">
          <span><kbd>↑↓</kbd> {lang === 'es' ? 'navegar' : 'navigate'}</span>
          <span><kbd>⏎</kbd> {lang === 'es' ? 'abrir' : 'open'}</span>
          <span><kbd>⌘⏎</kbd> {lang === 'es' ? 'detalles' : 'drill'}</span>
          <span><kbd>ESC</kbd> {lang === 'es' ? 'cerrar' : 'close'}</span>
        </div>
      </div>
    </div>
  );
}

/* =========================================================================
   WALLET: Add / Details / Remove
   ========================================================================= */
// Add-wallet modal — public address only, no secrets.
// SECURITY: an explorer/analytics app never needs a seed phrase or private key
// to show balances and activity. Those live exclusively in the user's own
// wallet software (Polkaswap, Fearless, Talisman, …). Asking for them here
// would be an immediate red flag that phishing sites would learn to mimic.
// The previous implementation had "Import seed" and "Private key" tabs that,
// on top of being dangerous, didn't even derive the real address — they
// assigned a random FAKE_ADDRS entry. Nuked entirely.
function AddWalletModal({ open, onClose }) {
  const t = useT();
  const [alias, setAlias] = useState('');
  const [addr, setAddr] = useState('');
  const wallets = useWallets();
  const toast = useToast();

  useEffect(() => { if (open) { setAlias(''); setAddr(''); } }, [open]);

  // SORA SS58 addresses start with "cn" and are 47-49 chars. We validate the
  // shape client-side and surface a clear error instead of silently failing.
  const trimmedAddr = addr.trim();
  const addrShapeOk = /^cn[1-9A-HJ-NP-Za-km-z]{45,49}$/.test(trimmedAddr);

  const submit = (dest) => {
    if (!addrShapeOk) {
      toast.push(t('wallet.addressInvalid'), 'err');
      return;
    }
    const payload = { alias: alias.trim() || (trimmedAddr.slice(0, 8) + '…' + trimmedAddr.slice(-4)), addr: trimmedAddr };
    if (dest === 'wallets') {
      wallets.addWallet({ ...payload, kind: 'watch' });
      toast.push(t('wallet.addedToMyWallets'), 'ok');
    } else {
      wallets.addWatched(payload);
      toast.push(t('wallet.addedToFollowing'), 'ok');
    }
    onClose();
  };

  return (
    <Modal open={open} onClose={onClose} width={560} label={t('wallet.modalTitle')}>
      <div className="sm-modal-head">
        <h3>{t('wallet.modalTitle')}</h3>
        <button className="sm-modal-x" onClick={onClose}>×</button>
      </div>

      <div className="sm-modal-body">
        <div className="sm-banner info" style={{marginBottom:12}}>
          {t('wallet.securityBanner')}
        </div>

        <div className="sm-field">
          <label>{t('wallet.aliasOptional')}</label>
          <input className="sm-input" value={alias} onChange={e => setAlias(e.target.value)}
                 placeholder={t('wallet.aliasPlaceholder')}/>
        </div>

        <div className="sm-field">
          <label>{t('wallet.publicAddress')}</label>
          <input className="sm-input" value={addr} onChange={e => setAddr(e.target.value)}
                 placeholder={t('wallet.addressPlaceholder')}
                 spellCheck={false} autoComplete="off"/>
          {addr.length > 0 && !addrShapeOk && (
            <div className="sm-banner warn" style={{marginTop:6}}>
              {t('wallet.addressInvalidHint')}
            </div>
          )}
        </div>
      </div>

      <div className="sm-modal-foot" style={{gap:8, flexWrap:'wrap'}}>
        <button className="btn" onClick={onClose}>{t('wallet.cancel')}</button>
        <div style={{flex:1}}/>
        <button className="btn" onClick={() => submit('watched')} disabled={!addrShapeOk}
                title={t('wallet.followTip')}>
          👁 {t('wallet.follow')}
        </button>
        <button className="btn primary" onClick={() => submit('wallets')} disabled={!addrShapeOk}
                title={t('wallet.myWalletsTip')}>
          ＋ {t('wallet.myWallets')}
        </button>
      </div>
    </Modal>
  );
}

// Tiny helper that renders a round token logo from a prod-provided logo
// string (base64 or data URL). Falls back to the symbol's first letter when
// no logo is present.
function TinyTokLogo({ sym, logo, size = 18 }) {
  // Fall back to the global TOKEN_LOGOS cache when the row didn't carry a logo
  // (true for /history/transfers and several other endpoints).
  const src = logo || (sym && window.TOKEN_LOGOS && window.TOKEN_LOGOS[sym]) || null;
  if (src) {
    return <img src={src} alt={sym || ''}
      style={{width: size, height: size, borderRadius: '50%', flexShrink: 0, objectFit: 'cover', background:'rgba(255,255,255,0.04)'}}
      onError={e => { e.currentTarget.style.display = 'none'; }}/>;
  }
  return (
    <span style={{
      width: size, height: size, borderRadius: '50%',
      background: 'linear-gradient(135deg,#7B5B90,#4A3566)',
      display:'inline-flex', alignItems:'center', justifyContent:'center',
      fontSize: Math.round(size * 0.5), fontWeight: 700, color:'#fff', flexShrink: 0,
    }}>{sym ? sym[0] : '?'}</span>
  );
}

// Renders the 4 history sub-tabs inside WalletDetailsModal. Each row shape
// differs between endpoints; we render prod-style cells with real token
// logos + a consistent "block + time" left column.
function WalletHistoryTable({ kind, rows }) {
  if (rows === null) return <div className="muted">Cargando {kind}…</div>;
  if (!rows || rows.length === 0) return <div className="muted tiny">Sin {kind} recientes para esta cartera.</div>;
  return (
    <table className="lp-table">
      <thead>
        <tr>
          <th style={{width: 150}}>Hora / Bloque</th>
          <th>Detalle</th>
        </tr>
      </thead>
      <tbody>
        {rows.slice(0, 30).map((r, i) => {
          const block = r.block || (r.extrinsic_id ? String(r.extrinsic_id).split('-')[0] : '');
          const timeStr = r.time || r.timestamp || '';
          // Detail cell composition — includes token logos like prod.
          let detail;
          if (kind === 'swaps') {
            detail = (
              <div style={{display:'flex', alignItems:'center', gap: 6, flexWrap:'wrap'}}>
                <TinyTokLogo sym={r.in?.symbol} logo={r.in?.logo}/>
                <span className="num">{fmt.num(Number(r.in?.amount || 0), 2)}</span>
                <span style={{fontWeight: 700}}>{r.in?.symbol}</span>
                <span style={{color:'var(--fg-3)'}}>→</span>
                <TinyTokLogo sym={r.out?.symbol} logo={r.out?.logo}/>
                <span className="num">{fmt.num(Number(r.out?.amount || 0), 2)}</span>
                <span style={{fontWeight: 700}}>{r.out?.symbol}</span>
                {r.in?.usd && <span className="muted tiny" style={{marginLeft:'auto'}}>${Number(r.in.usd).toFixed(2)}</span>}
              </div>
            );
          } else if (kind === 'transfers') {
            detail = (
              <div style={{display:'flex', alignItems:'center', gap: 6, flexWrap:'wrap'}}>
                <TinyTokLogo sym={r.symbol} logo={r.logo}/>
                <span className="num">{fmt.num(Number(r.amount || 0), 2)}</span>
                <span style={{fontWeight: 700}}>{r.symbol}</span>
                <span className="muted tiny">{fmt.addr(r.from, 6, 4)} → {fmt.addr(r.to, 6, 4)}</span>
                {r.usdValue && <span className="muted tiny" style={{marginLeft:'auto'}}>${Number(r.usdValue).toFixed(2)}</span>}
              </div>
            );
          } else if (kind === 'bridges') {
            detail = (
              <div style={{display:'flex', alignItems:'center', gap: 6, flexWrap:'wrap'}}>
                <TinyTokLogo sym={r.symbol} logo={r.logo}/>
                <span style={{fontWeight:700}}>{r.direction}</span>
                <span className="num">{fmt.num(Number(r.amount || 0), 2)}</span>
                <span style={{fontWeight: 700}}>{r.symbol}</span>
                <span className="muted tiny">via {r.network || '—'}</span>
              </div>
            );
          } else if (kind === 'extrinsics') {
            detail = (
              <div style={{display:'flex', alignItems:'center', gap: 8}}>
                <span style={{fontFamily:'JetBrains Mono', fontSize: 12}}>
                  <span style={{color:'#EC4899'}}>{r.section}</span>
                  <span style={{color:'var(--fg-3)'}}>::</span>
                  <span>{r.method}</span>
                </span>
                {r.success === 1 || r.success === true
                  ? <span className="tag ok tiny">✓</span>
                  : <span className="tag err tiny">✗</span>}
              </div>
            );
          }
          return (
            <tr key={(r.hash || '') + i}>
              <td style={{whiteSpace:'nowrap'}}>
                <div className="num tiny" style={{fontWeight: 600}}>#{block}</div>
                <div className="muted tiny">{timeStr}</div>
              </td>
              <td>{detail}</td>
            </tr>
          );
        })}
      </tbody>
    </table>
  );
}

// Per-wallet history fetch — each sub-tab triggers one GET.
function useWalletHistory(endpoint, addr, active) {
  const [rows, setRows] = useState(null);
  useEffect(() => {
    if (!active || !addr) return;
    let cancelled = false;
    setRows(null);
    fetch(endpoint + '/' + encodeURIComponent(addr) + '?limit=30&page=1')
      .then(r => r.ok ? r.json() : null)
      .then(j => { if (!cancelled) setRows(j?.data || j?.result || (Array.isArray(j) ? j : [])); })
      .catch(() => { if (!cancelled) setRows([]); });
    return () => { cancelled = true; };
  }, [endpoint, addr, active]);
  return rows;
}

// Width bumped from 540 → 820 so all 8 sub-tabs fit. See WalletDetailsModal <Modal width>.
const WALLET_MODAL_WIDTH = 820;

// Prod's /wallet/liquidity/:addr returns either a bare array or { positions: [...] }.
function liquidityPositions(liquidity) {
  if (!liquidity) return null;
  if (Array.isArray(liquidity)) return liquidity;
  return liquidity.positions || [];
}

function LiquidityPane({ liquidity }) {
  const positions = liquidityPositions(liquidity);
  return (
    <div className="sm-field">
      <label>Posiciones de liquidez</label>
      {positions === null ? <div className="muted">Cargando…</div> :
       positions.length === 0 ? <div className="muted tiny">Sin posiciones de liquidez.</div> :
        <table className="lp-table">
          <thead><tr><th>Pool</th><th style={{textAlign:'right'}}>Cantidad</th><th style={{textAlign:'right'}}>Share</th><th style={{textAlign:'right'}}>Valor</th></tr></thead>
          <tbody>
            {positions.slice(0, 30).map((p, i) => {
              const baseSym = p.base?.symbol || (typeof p.base === 'string' ? p.base : '');
              const targetSym = p.target?.symbol || (typeof p.target === 'string' ? p.target : '');
              const amtBase = Number(p.amountBase || p.amount_base || 0);
              const amtTarget = Number(p.amountTarget || p.amount_target || 0);
              return (
                <tr key={i}>
                  <td>
                    <div style={{display:'flex', alignItems:'center', gap:6}}>
                      <TinyTokLogo sym={baseSym} logo={p.base?.logo}/>
                      <TinyTokLogo sym={targetSym} logo={p.target?.logo}/>
                      <span style={{fontWeight:700}}>{baseSym}/{targetSym}</span>
                    </div>
                  </td>
                  <td style={{textAlign:'right'}} className="num tiny">
                    <div>{fmt.num(amtBase, 3)} {baseSym}</div>
                    <div className="muted">{fmt.num(amtTarget, 3)} {targetSym}</div>
                  </td>
                  <td style={{textAlign:'right'}} className="num">{((Number(p.share) || 0) * 100).toFixed(2)}%</td>
                  <td style={{textAlign:'right'}} className="num">{fmt.usd(Number(p.usdValue || p.value) || 0)}</td>
                </tr>
              );
            })}
          </tbody>
        </table>
      }
    </div>
  );
}

// Small reusable stat cell for the modal stat-grids. Compact variant of .stat-card.
function MiniStat({ label, value, hint }) {
  return (
    <div className="sm-mini-stat">
      <div className="sm-mini-stat-label">{label}</div>
      <div className="sm-mini-stat-value">{value}</div>
      {hint != null && <div className="sm-mini-stat-hint">{hint}</div>}
    </div>
  );
}

function StakingPane({ staking }) {
  if (!staking) return <div className="sm-field"><label>Staking</label><div className="muted">Cargando…</div></div>;
  const staked    = Number(staking.staked) || 0;
  const unbonding = Number(staking.unbonding) || 0;
  const rewards   = Number(staking.rewards) || 0;
  const usdValue  = Number(staking.usdValue) || 0;
  const validators = Array.isArray(staking.validators) ? staking.validators : [];
  const isEmpty = staked === 0 && unbonding === 0 && rewards === 0 && validators.length === 0;
  return (
    <div className="sm-field">
      <label>Staking</label>
      {isEmpty ? <div className="muted tiny">Esta cuenta no tiene posiciones de staking activas.</div> : (<>
        <div className="sm-mini-grid">
          <MiniStat label="Staked"    value={fmt.num(staked, 2)}/>
          <MiniStat label="Unbonding" value={fmt.num(unbonding, 2)}/>
          <MiniStat label="Rewards"   value={fmt.num(rewards, 4)}/>
          <MiniStat label="Valor USD" value={fmt.usd(usdValue)}/>
        </div>
        {validators.length > 0 && (
          <div style={{marginTop: 12}}>
            <div className="sm-subhead">Validadores ({validators.length})</div>
            <table className="lp-table">
              <thead><tr><th>Validador</th><th style={{textAlign:'right'}}>Stake</th><th style={{textAlign:'right'}}>Estado</th></tr></thead>
              <tbody>
                {validators.slice(0, 50).map((v, i) => {
                  const vAddr = v.address || v.stash || v.validator || '';
                  const vStake = Number(v.stake ?? v.bonded ?? v.amount ?? 0);
                  const vStatus = v.status || (v.active ? 'Activo' : v.waiting ? 'Esperando' : '—');
                  return (
                    <tr key={i}>
                      <td>
                        {vAddr
                          ? <a className="link-mono" onClick={(e) => { e.preventDefault(); window.openWalletDetails && window.openWalletDetails(vAddr); }}>{fmt.addr(vAddr)}</a>
                          : '—'}
                      </td>
                      <td style={{textAlign:'right'}} className="num">{vStake > 0 ? fmt.num(vStake, 2) : '—'}</td>
                      <td style={{textAlign:'right'}} className="tiny muted">{vStatus}</td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        )}
      </>)}
    </div>
  );
}

function InfoPane({ info }) {
  if (!info) return <div className="sm-field"><label>Información on-chain</label><div className="muted">Cargando…</div></div>;
  const txCount = Number(info.txCount) || 0;
  const tInCount  = Number(info.transfersIn?.count)  || 0;
  const tOutCount = Number(info.transfersOut?.count) || 0;
  const bInCount  = Number(info.bridgeIncoming?.count) || 0;
  const bOutCount = Number(info.bridgeOutgoing?.count) || 0;
  const swapCount = Number(info.swapCount) || 0;
  const hasAnyActivity = txCount > 0 || info.firstTx || tInCount > 0 || tOutCount > 0 || bInCount > 0 || bOutCount > 0 || swapCount > 0;
  if (!hasAnyActivity) {
    return <div className="sm-field"><label>Información on-chain</label><div className="muted tiny">Sin actividad on-chain registrada para esta cuenta.</div></div>;
  }
  const firstTxMs = info.firstTx ? Number(info.firstTx) : null;
  const lastTxMs  = info.lastTx  ? Number(info.lastTx)  : null;
  const dateShort = (ms) => ms ? new Date(ms).toLocaleDateString('es-ES', { year:'numeric', month:'short', day:'2-digit' }) : '—';
  const successCount = Number(info.successCount) || 0;
  const successRate = txCount > 0 ? (successCount / txCount) * 100 : 0;
  const whaleScore = Number(info.whaleScore) || 0;
  const whaleBreak = info.whaleBreakdown || {};
  const modules    = Array.isArray(info.modules)     ? info.modules     : [];
  const topTokens  = Array.isArray(info.topTokens)   ? info.topTokens   : [];
  const topContacts = Array.isArray(info.topContacts) ? info.topContacts : [];
  const tIn  = info.transfersIn  || { count:0, usd:0 };
  const tOut = info.transfersOut || { count:0, usd:0 };
  const bIn  = info.bridgeIncoming || { count:0, usd:0 };
  const bOut = info.bridgeOutgoing || { count:0, usd:0 };
  const clickWallet = (addr) => { if (addr && window.openWalletDetails) window.openWalletDetails(addr); };
  return (
    <>
      <div className="sm-field">
        <label>Actividad</label>
        <div className="sm-mini-grid">
          <MiniStat label="Primera tx"    value={dateShort(firstTxMs)}/>
          <MiniStat label="Última tx"     value={dateShort(lastTxMs)}/>
          <MiniStat label="Total txs"     value={fmt.int(txCount)} hint={`${successRate.toFixed(1)}% éxito`}/>
          <MiniStat label="Días activos"  value={fmt.int(Number(info.daysActive) || 0)}/>
          <MiniStat label="Tokens únicos" value={fmt.int(Number(info.uniqueTokens) || 0)}/>
          <MiniStat label="Gobernanza"    value={fmt.int(Number(info.governanceTx) || 0)}/>
        </div>
      </div>

      <div className="sm-field">
        <label>Whale score · {info.whaleTier || '—'}</label>
        <div className="sm-whale-row">
          <div className="sm-whale-score">{whaleScore}</div>
          <div className="sm-whale-bars">
            <WhaleBar label="Volumen"    pct={Number(whaleBreak.volume)    || 0}/>
            <WhaleBar label="Frecuencia" pct={Number(whaleBreak.frequency) || 0}/>
            <WhaleBar label="Diversidad" pct={Number(whaleBreak.diversity) || 0}/>
          </div>
        </div>
      </div>

      {(Number(info.swapCount) || 0) > 0 && (
        <div className="sm-field">
          <label>Swaps</label>
          <div className="sm-mini-grid">
            <MiniStat label="Nº swaps"     value={fmt.int(Number(info.swapCount) || 0)}/>
            <MiniStat label="Volumen"      value={fmt.usd(Number(info.swapTotalVolume) || 0)}/>
            <MiniStat label="Promedio"     value={fmt.usd(Number(info.swapAvgUsd)      || 0)}/>
            <MiniStat label="Máximo"       value={fmt.usd(Number(info.swapMaxUsd)      || 0)}/>
          </div>
        </div>
      )}

      {topTokens.length > 0 && (
        <div className="sm-field">
          <label>Top tokens</label>
          <table className="lp-table">
            <thead><tr><th>Token</th><th style={{textAlign:'right'}}>Trades</th><th style={{textAlign:'right'}}>Volumen USD</th></tr></thead>
            <tbody>
              {topTokens.slice(0, 10).map((t, i) => (
                <tr key={i}>
                  <td>{t.symbol}</td>
                  <td style={{textAlign:'right'}} className="num">{fmt.int(Number(t.trades) || 0)}</td>
                  <td style={{textAlign:'right'}} className="num">{fmt.usd(Number(t.total_usd) || 0)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}

      {topContacts.length > 0 && (
        <div className="sm-field">
          <label>Top contactos</label>
          <table className="lp-table">
            <thead><tr><th>Dirección</th><th style={{textAlign:'right'}}>Txs</th><th style={{textAlign:'right'}}>Volumen USD</th></tr></thead>
            <tbody>
              {topContacts.slice(0, 10).map((c, i) => (
                <tr key={i}>
                  <td>
                    <a className="link-mono" onClick={(e) => { e.preventDefault(); clickWallet(c.counterparty); }}>
                      {fmt.addr(c.counterparty)}
                    </a>
                  </td>
                  <td style={{textAlign:'right'}} className="num">{fmt.int(Number(c.tx_count) || 0)}</td>
                  <td style={{textAlign:'right'}} className="num">{fmt.usd(Number(c.total_usd) || 0)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}

      <div className="sm-field">
        <label>Flujos</label>
        <div className="sm-mini-grid">
          <MiniStat label="Transfers in"  value={fmt.int(Number(tIn.count)  || 0)} hint={fmt.usd(Number(tIn.usd)  || 0)}/>
          <MiniStat label="Transfers out" value={fmt.int(Number(tOut.count) || 0)} hint={fmt.usd(Number(tOut.usd) || 0)}/>
          <MiniStat label="Bridges in"    value={fmt.int(Number(bIn.count)  || 0)} hint={fmt.usd(Number(bIn.usd)  || 0)}/>
          <MiniStat label="Bridges out"   value={fmt.int(Number(bOut.count) || 0)} hint={fmt.usd(Number(bOut.usd) || 0)}/>
          <MiniStat label="LP depósitos"  value={fmt.int(Number(info.lpDeposits)    || 0)} hint={fmt.usd(Number(info.lpDepositedUsd) || 0)}/>
          <MiniStat label="LP retiros"    value={fmt.int(Number(info.lpWithdrawals) || 0)} hint={fmt.usd(Number(info.lpWithdrawnUsd) || 0)}/>
        </div>
      </div>

      {modules.length > 0 && (
        <div className="sm-field">
          <label>Módulos usados</label>
          <div className="sm-chip-row">
            {modules.map((m, i) => (
              <span key={i} className="sm-chip">
                <span className="sm-chip-label">{m.section}</span>
                <span className="sm-chip-count">{fmt.int(Number(m.count) || 0)}</span>
              </span>
            ))}
          </div>
        </div>
      )}
    </>
  );
}

function WhaleBar({ label, pct }) {
  const clamped = Math.max(0, Math.min(100, Number(pct) || 0));
  return (
    <div className="sm-whale-bar">
      <div className="sm-whale-bar-head">
        <span className="tiny muted">{label}</span>
        <span className="tiny num">{clamped.toFixed(0)}</span>
      </div>
      <div className="sm-whale-bar-track"><div className="sm-whale-bar-fill" style={{width: clamped + '%'}}/></div>
    </div>
  );
}

function WalletDetailsModal({ wallet, open, onClose, onRemove }) {
  const t = useT();
  const [alias, setAlias] = useState('');
  const [confirmRm, setConfirmRm] = useState(false);
  const [copied, setCopied] = useState(false);
  const [subtab, setSubtab] = useState('assets');
  const [identity, setIdentity] = useState(null); // { display, email, twitter, web, discord }
  const wallets = useWallets();
  const toast = useToast();

  useEffect(() => { if (wallet) setAlias(wallet.alias); }, [wallet?.alias]);
  useEffect(() => { setConfirmRm(false); setSubtab('assets'); setIdentity(null); }, [wallet?.addr, open]);

  // Fetch on-chain identity (pallet Identity) so the modal shows Subscan-style
  // display name + verification links, matching v1 behaviour.
  useEffect(() => {
    if (!open || !wallet?.addr) return;
    let cancelled = false;
    fetch('/identity/' + encodeURIComponent(wallet.addr))
      .then(r => r.ok ? r.json() : null)
      .then(j => { if (!cancelled) setIdentity(j); })
      .catch(() => {});
    return () => { cancelled = true; };
  }, [wallet?.addr, open]);

  // All hooks must run every render (Rules of Hooks), even when wallet is null.
  // Pass a safe empty addr so the sub-tab hooks don't fetch without a target.
  const addr = wallet?.addr || '';
  const swaps = useWalletHistory('/history/swaps', addr, subtab === 'swaps' && !!wallet);
  const transfers = useWalletHistory('/history/transfers', addr, subtab === 'transfers' && !!wallet);
  const bridges = useWalletHistory('/history/bridges', addr, subtab === 'bridges' && !!wallet);
  const extrinsics = useWalletHistory('/history/extrinsics', addr, subtab === 'extrinsics' && !!wallet);
  // Liquidity + staking + info are single-shot GETs without pagination.
  const [liquidity, setLiquidity] = useState(null);
  const [staking, setStaking] = useState(null);
  const [info, setInfo] = useState(null);
  useEffect(() => {
    if (!addr) return;
    let c = false;
    if (subtab === 'liquidity' && !liquidity) {
      fetch('/wallet/liquidity/' + encodeURIComponent(addr)).then(r => r.json()).then(j => { if (!c) setLiquidity(j); }).catch(() => {});
    }
    if (subtab === 'staking' && !staking) {
      fetch('/wallet/staking/' + encodeURIComponent(addr)).then(r => r.json()).then(j => { if (!c) setStaking(j); }).catch(() => {});
    }
    if (subtab === 'info' && !info) {
      fetch('/wallet/info/' + encodeURIComponent(addr)).then(r => r.json()).then(j => { if (!c) setInfo(j); }).catch(() => {});
    }
    return () => { c = true; };
  }, [subtab, addr, liquidity, staking, info]);
  // Reset the single-shot caches when opening a different wallet.
  useEffect(() => { setLiquidity(null); setStaking(null); setInfo(null); }, [addr]);

  if (!wallet) return null;

  // Real token breakdown from prod GET /balance/:addr. Shape per token:
  // { symbol, logo, amount, usdValue }. We weight by usdValue where available,
  // falling back to amount (unitless) for tokens without price data.
  const TOKEN_COLOR = { XOR:'#E5243B', VAL:'#F5B041', PSWAP:'#EC4899', ETH:'#8B7FD9', KUSD:'#60A5FA', TBCD:'#10B981', DAI:'#FDE68A' };
  const rawTokens = wallet.tokens || [];
  const numericTokens = rawTokens
    .map(t => ({
      sym: t.symbol,
      amount: Number(t.amount) || 0,
      usdValue: Number(t.usdValue) || 0,
      logo: t.logo,
    }))
    .filter(t => t.amount > 0)
    .sort((a, b) => b.usdValue - a.usdValue);
  const totalUsd = numericTokens.reduce((s, t) => s + t.usdValue, 0);
  // Include logo from /balance so the breakdown can render real token icons
  // instead of plain color dots. We also keep every token — the previous 6-row
  // cap hid long tails (e.g. wallets with VXOR/HMX/DAI/… outside top 6).
  const breakdown = numericTokens.map(t => ({
    sym: t.sym,
    pct: totalUsd > 0 ? t.usdValue / totalUsd : 0,
    color: TOKEN_COLOR[t.sym] || '#94A3B8',
    amt: t.amount,
    usd: t.usdValue,
    logo: t.logo,
  }));
  // Fallback placeholder when prod returns empty tokens (e.g. unused address).
  if (!breakdown.length) breakdown.push({ sym: '—', pct: 1, color: '#4A3566', amt: 0, usd: 0, logo: null });

  // Is this wallet already in the user's persistent store?
  const storedHere = !!(
    wallets?.wallets?.find(w => w.addr === wallet.addr) ||
    wallets?.watched?.find(w => w.addr === wallet.addr)
  );

  const copyAddr = async () => {
    try {
      if (navigator.clipboard?.writeText) {
        await navigator.clipboard.writeText(wallet.addr);
      } else {
        // Fallback when clipboard API is unavailable (insecure context, etc.)
        const ta = document.createElement('textarea');
        ta.value = wallet.addr;
        ta.style.position = 'fixed'; ta.style.opacity = '0';
        document.body.appendChild(ta);
        ta.select();
        document.execCommand('copy');
        ta.remove();
      }
      setCopied(true);
      setTimeout(() => setCopied(false), 1400);
      toast?.push?.('Dirección copiada', 'ok');
    } catch (e) {
      toast?.push?.('No se pudo copiar', 'err');
    }
  };
  // openWalletDetails(addr, alias) synthesises a wallet with id 'external-<addr>'
  // for any click from Portfolio / Holders / drill rows. That synthetic id will
  // never match the real store ids ('w1', 'v2', …), so renameWallet / removeWallet
  // called with wallet.id were silently no-ops. We always look up the real
  // entry by address first. `storedHere` already matches by address, so by the
  // time we get here we know it exists in either wallets or watched.
  const storedWallet  = wallets?.wallets?.find(w  => w.addr === wallet.addr) || null;
  const storedWatched = wallets?.watched?.find(w => w.addr === wallet.addr) || null;

  // Save semantics:
  //   · already in wallets  → rename (alias edit)
  //   · already in watched  → rename (alias edit)
  //   · not stored yet      → caller passes the destination ('wallets' | 'watched')
  const saveRename = (dest) => {
    const newAlias = alias.trim();
    if (!newAlias) return;
    if (storedWallet) {
      if (newAlias === storedWallet.alias) return;
      wallets.renameWallet(storedWallet.id, newAlias);
      toast.push(t('wallet.aliasUpdated'), 'ok');
    } else if (storedWatched) {
      if (newAlias === storedWatched.alias) return;
      wallets.renameWatched(storedWatched.id, newAlias);
      toast.push(t('wallet.aliasUpdated'), 'ok');
    } else if (dest === 'wallets') {
      wallets.addWallet({ alias: newAlias, addr: wallet.addr, kind: 'watch' });
      toast.push(t('wallet.addedToMyWallets'), 'ok');
    } else {
      wallets.addWatched({ alias: newAlias, addr: wallet.addr });
      toast.push(t('wallet.addedToFollowing'), 'ok');
    }
  };
  const doRemove = () => {
    if (storedWallet) {
      wallets.removeWallet(storedWallet.id);
      toast.push(t('wallet.removed'), 'ok');
    } else if (storedWatched) {
      wallets.removeWatched(storedWatched.id);
      toast.push(t('wallet.removedFromFollowing'), 'ok');
    }
    onClose();
  };

  // Move between Mis Wallets ↔ Seguidas. Only active when the wallet is
  // stored in one of the two lists; external/drill-only wallets have nothing
  // to move yet. We close the modal so the header label refreshes next open.
  const doMoveToWatched = () => {
    if (!storedWallet || !wallets.moveToWatched) return;
    wallets.moveToWatched(storedWallet.id);
    toast.push(t('wallet.addedToFollowing'), 'ok');
    setTimeout(() => onClose(), 300);
  };
  const doMoveToWallets = () => {
    if (!storedWatched || !wallets.moveToWallets) return;
    wallets.moveToWallets(storedWatched.id);
    toast.push(t('wallet.addedToMyWallets'), 'ok');
    setTimeout(() => onClose(), 300);
  };

  return (
    <Modal open={open} onClose={onClose} width={WALLET_MODAL_WIDTH} label={wallet.alias}>
      <div className="sm-modal-head">
        <div style={{display:'flex', alignItems:'center', gap:12, flex:1, minWidth:0}}>
          <div className="sm-avatar" style={{background:'linear-gradient(135deg,#9B1B30,#4A3566)'}}>{wallet.alias[0]}</div>
          <div style={{minWidth:0, flex:1}}>
            <h3 style={{margin:0, display:'flex', alignItems:'center', gap:6, flexWrap:'wrap'}}>
              {identity?.display || wallet.alias}
              {identity?.display && <span className="tag ok" style={{fontSize:10}}>on-chain</span>}
            </h3>
            <div className="muted tiny" style={{display:'flex', alignItems:'center', gap:8, flexWrap:'wrap', marginTop:2}}>
              <span className="num" style={{overflowWrap:'anywhere'}}>{fmt.addr(wallet.addr, 8, 6)}</span>
              <button className="btn" style={{padding:'2px 8px', fontSize:11}} onClick={copyAddr} title="Copiar dirección">
                {copied ? '✓ Copiado' : '⎘ Copiar'}
              </button>
              {identity?.display && identity.display !== wallet.alias && <span>· alias: {wallet.alias}</span>}
            </div>
            {(identity?.twitter || identity?.web || identity?.email || identity?.discord) && (
              <div className="muted tiny" style={{display:'flex', gap:10, marginTop:4}}>
                {identity.twitter && <span>𝕏 {identity.twitter}</span>}
                {identity.web && <span>🌐 {identity.web}</span>}
                {identity.email && <span>✉ {identity.email}</span>}
                {identity.discord && <span>💬 {identity.discord}</span>}
              </div>
            )}
          </div>
        </div>
        <button className="sm-modal-x" onClick={onClose}>×</button>
      </div>

      <div className="sm-modal-tabs" style={{overflowX:'auto', flexWrap:'nowrap', whiteSpace:'nowrap', scrollbarWidth:'thin'}}>
        {[
          ['assets',     'Assets'],
          ['swaps',      'Swaps'],
          ['transfers',  'Transfers'],
          ['bridges',    'Bridges'],
          ['liquidity',  'Liquidity'],
          ['staking',    'Staking'],
          ['predict',    'Predictions'],
          ['extrinsics', 'Extrinsics'],
          ['info',       'Info'],
        ].map(([id, lbl]) => (
          <button key={id}
                  className={'sm-modal-tab' + (subtab===id?' active':'')}
                  style={{padding:'8px 12px', fontSize: 12, flexShrink: 0}}
                  onClick={() => setSubtab(id)}>{lbl}</button>
        ))}
      </div>

      <div className="sm-modal-body" style={{overflowY: 'auto', flex: 1, minHeight: 0}}>
        {subtab === 'assets' && (<>
          <div className="sm-field">
            <label>
              {storedWallet  ? (t('wallet.alias') + ' · ' + t('wallet.inMyWallets'))
                : storedWatched ? (t('wallet.alias') + ' · ' + t('wallet.inFollowing'))
                : t('wallet.savePrompt')}
            </label>
            <div style={{display:'flex', gap:8, flexWrap:'wrap'}}>
              <input className="sm-input" value={alias} onChange={e => setAlias(e.target.value)} placeholder="Alias" style={{flex:'1 1 220px'}}/>
              {storedHere ? (
                /* Already stored — single rename button. Which list it lives
                   in is shown in the label above, so the user always knows. */
                <button
                  className="btn primary"
                  onClick={() => saveRename()}
                  disabled={!alias.trim() || alias === (storedWallet?.alias || storedWatched?.alias)}>
                  Guardar
                </button>
              ) : (
                /* Not stored yet — offer both destinations explicitly. */
                <>
                  <button
                    className="btn primary"
                    onClick={() => saveRename('wallets')}
                    disabled={!alias.trim()}
                    title="Añadir a Mis Wallets (carteras propias)">
                    ＋ Mis Wallets
                  </button>
                  <button
                    className="btn"
                    onClick={() => saveRename('watched')}
                    disabled={!alias.trim()}
                    title="Añadir a Seguidas (carteras a vigilar)">
                    👁 Seguir
                  </button>
                </>
              )}
            </div>
            {!storedHere && (
              <div className="muted tiny" style={{marginTop:6, lineHeight:1.4}}>
                <strong>Mis Wallets</strong> = tus carteras propias ·
                <strong> Seguidas</strong> = otras carteras que quieres vigilar.
              </div>
            )}
          </div>

          <div className="sm-field">
            <label>Dirección</label>
            <div className="sm-addr-row">
              <span className="num tiny" style={{flex:1, overflowWrap:'anywhere'}}>{wallet.addr}</span>
              <button className="btn" onClick={copyAddr}>{copied ? '✓ Copiado' : 'Copiar'}</button>
            </div>
          </div>

          <div className="sm-field">
            <label>Desglose por activo · ${totalUsd.toLocaleString(undefined,{maximumFractionDigits:2})} · {numericTokens.length} tokens</label>
            <div className="sm-breakdown" style={{maxHeight: 380, overflowY:'auto'}}>
              {breakdown.map((b, i) => (
                <div key={b.sym + '_' + i} className="sm-breakdown-row">
                  <TinyTokLogo sym={b.sym} logo={b.logo} size={20}/>
                  <span className="sm-bd-sym" style={{fontWeight:700}}>{b.sym}</span>
                  <div className="sm-bd-bar"><div style={{width:(b.pct*100)+'%', background:b.color}}/></div>
                  <span className="num tiny" style={{minWidth:110, textAlign:'right'}}>{b.amt > 0 ? fmt.num(b.amt, b.amt >= 1000 ? 0 : b.amt >= 1 ? 2 : 6) : '—'}</span>
                  <span className="num tiny muted" style={{minWidth:48, textAlign:'right'}}>{(b.pct*100).toFixed(1)}%</span>
                  <span className="num" style={{fontWeight:600, minWidth:90, textAlign:'right'}}>
                    {b.usd > 0 ? '$' + b.usd.toLocaleString(undefined,{maximumFractionDigits:2}) : '—'}
                  </span>
                </div>
              ))}
            </div>
            {rawTokens.length === 0 && (
              <div className="muted tiny" style={{marginTop:8}}>Cargando balances desde sorametrics.org…</div>
            )}
          </div>
        </>)}

        {/* History sub-tabs: 4 variants share the same row shape — compact table */}
        {['swaps','transfers','bridges','extrinsics'].includes(subtab) && (
          <WalletHistoryTable
            kind={subtab}
            rows={subtab==='swaps'?swaps:subtab==='transfers'?transfers:subtab==='bridges'?bridges:extrinsics}
          />
        )}

        {subtab === 'liquidity' && <LiquidityPane liquidity={liquidity}/>}

        {subtab === 'staking' && <StakingPane staking={staking}/>}

        {/* Polkamarkt positions for this wallet. Self-hides when the pallet
            is not active on-chain or the wallet has no positions. */}
        {subtab === 'predict' && (
          window.PolkamarktPositions
            ? <window.PolkamarktPositions addr={addr}/>
            : <div className="muted tiny" style={{padding: 20, textAlign: 'center'}}>Prediction Markets module not loaded.</div>
        )}

        {subtab === 'info' && <InfoPane info={info}/>}
      </div>

      <div className="sm-modal-foot">
        {!confirmRm ? (
          <>
            <button className="btn danger" onClick={() => setConfirmRm(true)}>{t('wallet.remove', 'Eliminar')}</button>
            {/* Move button — only when the wallet is currently stored in one
                list or the other. Label tells the user the DESTINATION, not
                the current location, so it's unambiguous ("move to …"). */}
            {storedWallet && (
              <button className="btn" onClick={doMoveToWatched}
                title={t('wallet.moveToFollowingTip', 'Mover esta wallet a Seguidas (read-only observada)')}
                style={{marginLeft:8}}>
                → 👁 {t('wallet.follow')}
              </button>
            )}
            {storedWatched && (
              <button className="btn" onClick={doMoveToWallets}
                title={t('wallet.moveToMyWalletsTip', 'Mover esta wallet a Mis Wallets (carteras propias)')}
                style={{marginLeft:8}}>
                → ＋ {t('wallet.myWallets')}
              </button>
            )}
            <div style={{flex:1}}/>
            <button className="btn primary" onClick={onClose}>{t('common.close', 'Cerrar')}</button>
          </>
        ) : (
          <div className="sm-confirm-row">
            <span>{t('wallet.removeConfirm', '¿Eliminar')} "{wallet.alias}"? {t('wallet.removeWarn', 'No se puede deshacer.')}</span>
            <button className="btn" onClick={() => setConfirmRm(false)}>{t('wallet.cancel')}</button>
            <button className="btn danger" onClick={doRemove}>{t('wallet.removeYes', 'Sí, eliminar')}</button>
          </div>
        )}
      </div>
    </Modal>
  );
}

/* =========================================================================
   CSV EXPORT helper
   ========================================================================= */
function csvEscape(v) {
  if (v === null || v === undefined) return '';
  const s = String(v);
  if (/[",\n\r]/.test(s)) return '"' + s.replace(/"/g, '""') + '"';
  return s;
}
function timestampSlug() {
  const d = new Date();
  const p = (n) => String(n).padStart(2,'0');
  return d.getFullYear() + p(d.getMonth()+1) + p(d.getDate()) + '_' + p(d.getHours()) + p(d.getMinutes()) + p(d.getSeconds());
}
function exportCsv(section, headers, rows) {
  const lines = [headers.map(csvEscape).join(',')];
  rows.forEach(r => {
    lines.push(headers.map(h => csvEscape(typeof r === 'object' ? r[h] : r)).join(','));
  });
  const blob = new Blob([lines.join('\n')], { type: 'text/csv;charset=utf-8' });
  const url = URL.createObjectURL(blob);
  const a = document.createElement('a');
  a.href = url;
  a.download = 'sorametrics_' + section + '_' + timestampSlug() + '.csv';
  document.body.appendChild(a); a.click(); a.remove();
  setTimeout(() => URL.revokeObjectURL(url), 1000);
}

/* A button that attaches onClick exporting rows the section provides.
   Props: section, headers, rows (array of plain objects keyed by header),
          label (optional override) */
// CSV tax formats exposed by prod /export/csv?format=<name>&address=<addr>.
// The dropdown lets the user pick among the 4 tax tools prod supports.
// "sorametrics" is the local format built from visible rows (no server roundtrip).
const CSV_FORMATS = [
  { id: 'sorametrics',   label: 'SoraMetrics (local)' },
  { id: 'koinly',        label: 'Koinly' },
  { id: 'cointracking',  label: 'CoinTracking' },
  { id: 'cointracker',   label: 'CoinTracker' },
];
function ExportCsvButton({ section, headers, rows, label, className }) {
  const t = useT ? useT() : ((k) => k);
  const toast = useToast();
  const [open, setOpen] = useState(false);
  const [busy, setBusy] = useState(false);
  const [address, setAddress] = useState('');

  const clickLocal = () => {
    try {
      exportCsv(section, headers, rows);
      toast.push((label || (t('btn.exportCsv') || 'CSV')) + ' · ' + (rows?.length || 0), 'ok');
      setOpen(false);
    } catch { toast.push('Error CSV', 'err'); }
  };
  const fetchProdFormat = async (fmt) => {
    if (!address) { toast.push('Introduce una dirección SS58', 'err'); return; }
    setBusy(true);
    try {
      const url = '/export/csv?format=' + encodeURIComponent(fmt) + '&address=' + encodeURIComponent(address);
      const r = await fetch(url);
      if (!r.ok) throw new Error('HTTP ' + r.status);
      const blob = await r.blob();
      const a = document.createElement('a');
      a.href = URL.createObjectURL(blob);
      a.download = 'sorametrics_' + fmt + '_' + (address.slice(0, 8)) + '_' + timestampSlug() + '.csv';
      a.click();
      URL.revokeObjectURL(a.href);
      toast.push('CSV ' + fmt + ' descargado', 'ok');
      setOpen(false);
    } catch (e) {
      toast.push('Error: ' + (e.message || 'export'), 'err');
    } finally {
      setBusy(false);
    }
  };

  return (
    <>
      <button className={'btn ' + (className || '')} onClick={() => setOpen(true)}>
        {label || t('btn.exportCsv') || 'Export CSV'}
      </button>
      {open && (
        <div className="sm-modal-backdrop" onClick={() => setOpen(false)}>
          <div className="sm-modal" style={{width: 480}} onClick={e => e.stopPropagation()}>
            <div className="sm-modal-head">
              <h3 style={{margin:0}}>Exportar CSV — {section}</h3>
              <button className="sm-modal-x" onClick={() => setOpen(false)}>×</button>
            </div>
            <div className="sm-modal-body">
              <div className="sm-field">
                <label>Formato</label>
                <div className="tweaks-opts" style={{flexWrap:'wrap', gap:6}}>
                  <button className="tweaks-opt active" onClick={clickLocal} disabled={busy}>
                    SoraMetrics (local · {rows?.length || 0} filas visibles)
                  </button>
                </div>
              </div>
              <div className="sm-field">
                <label>O exportar historial completo para una dirección (tax tools)</label>
                <input className="sm-input" placeholder="cnR… dirección SS58"
                       value={address} onChange={e => setAddress(e.target.value)}/>
                <div className="tweaks-opts" style={{flexWrap:'wrap', gap:6, marginTop:8}}>
                  {CSV_FORMATS.filter(f => f.id !== 'sorametrics').map(f => (
                    <button key={f.id} className="tweaks-opt"
                            onClick={() => fetchProdFormat(f.id)} disabled={busy || !address}>
                      {f.label}
                    </button>
                  ))}
                </div>
                <div className="muted tiny" style={{marginTop:6}}>
                  Descarga de prod /export/csv?format=… · limit 50.000 filas.
                </div>
              </div>
            </div>
          </div>
        </div>
      )}
    </>
  );
}

/* =========================================================================
   BACKUP / RESTORE
   ========================================================================= */
function downloadBackup(tweaks) {
  const payload = {
    version: 1,
    createdAt: new Date().toISOString(),
    settings: {
      tweaks,
      lang: loadLS('sorametrics.lang', 'es'),
      density: tweaks?.density,
      accent: tweaks?.accent,
      motion: tweaks?.motion,
      section: tweaks?.section,
    },
    favorites: {
      tokens: loadLS('sm.favTokens', []),
      wallets: loadLS('sm.favWallets', []),
    },
    watchlist: loadLS('sm.watched', INITIAL_WATCHED).map(w => ({ alias:w.alias, addr:w.addr })),
    wallets: loadLS('sm.wallets', INITIAL_WALLETS).map(w => ({ id:w.id, alias:w.alias, addr:w.addr, kind:w.kind, value:w.value })),
    recentSearches: loadLS('sm.searchRecent', []),
  };
  const blob = new Blob([JSON.stringify(payload, null, 2)], { type: 'application/json' });
  const url = URL.createObjectURL(blob);
  const a = document.createElement('a');
  const d = new Date(); const p = (n) => String(n).padStart(2,'0');
  const stamp = d.getFullYear() + p(d.getMonth()+1) + p(d.getDate());
  a.href = url; a.download = 'sorametrics_backup_' + stamp + '.json';
  document.body.appendChild(a); a.click(); a.remove();
  setTimeout(() => URL.revokeObjectURL(url), 1000);
}

// Plain-text address-list parser. Accepts files with one SORA SS58 address
// per line (whitespace trimmed, blank lines + # comments ignored). This is
// the format users commonly export from scripts or maintain by hand — not
// JSON, but still a perfectly valid import source. Each address becomes a
// watched wallet with alias = shortened address.
function parseAddressListBackup(text) {
  if (typeof text !== 'string') return null;
  const SS58 = /^cn[1-9A-HJ-NP-Za-km-z]{45,49}$/;
  const addrs = [];
  for (const rawLine of text.split(/\r?\n/)) {
    const line = rawLine.trim();
    if (!line || line.startsWith('#') || line.startsWith('//')) continue;
    // Be lenient: if the line has "address alias" or "address,alias", take
    // the first token as address and the rest as alias.
    const m = line.match(/^(cn[1-9A-HJ-NP-Za-km-z]{45,49})(?:[\s,;:\-|]+(.*))?$/);
    if (!m) continue;
    const addr = m[1];
    const alias = (m[2] || '').trim() || (addr.slice(0, 6) + '…' + addr.slice(-4));
    addrs.push({ addr, alias });
  }
  if (addrs.length === 0) return null;
  // All address-list entries land in watchlist — the user hasn't told us
  // they're "their own" so we default to the safer "following" slot. They
  // can move any to My Wallets afterwards from the wallet detail modal.
  return {
    wallets: [],
    watchlist: addrs.map((a, i) => ({
      id: 'r' + Date.now() + '_' + i,
      alias: a.alias,
      addr: a.addr,
      value: 0,
    })),
    _plainTextList: true,
  };
}

function restoreBackup(file, setTweak) {
  return new Promise((resolve, reject) => {
    const fr = new FileReader();
    fr.onload = () => {
      const raw = fr.result;
      try {
        // Three-way detection:
        //   1. JSON v1 (sora_wallets as stringified array)
        //   2. JSON v2 (our native shape)
        //   3. Plain-text address list (one SS58 per line)
        // JSON path is tried first; if the file isn't valid JSON, fall
        // through to the text-list parser before giving up.
        let data;
        try {
          const parsed = JSON.parse(raw);
          data = parseV1Backup(parsed) || parsed;
        } catch {
          data = parseAddressListBackup(raw);
          if (!data) { reject(new Error('unrecognised file format')); return; }
        }
        if (data.settings?.lang) {
          saveLS('sorametrics.lang', data.settings.lang);
          // sorametrics.lang is stored as raw string in loadLS — keep as string
          try { localStorage.setItem('sorametrics.lang', data.settings.lang); } catch {}
        }
        if (data.settings?.tweaks && setTweak) {
          Object.entries(data.settings.tweaks).forEach(([k,v]) => setTweak(k, v));
        }
        if (data.wallets) saveLS('sm.wallets', data.wallets);
        if (data.watchlist) saveLS('sm.watched', data.watchlist.map((w,i) => ({ id:'v'+i, value:0, ...w })));
        if (data.favorites?.tokens) saveLS('sm.favTokens', data.favorites.tokens);
        if (data.favorites?.wallets) saveLS('sm.favWallets', data.favorites.wallets);
        if (data.recentSearches) saveLS('sm.searchRecent', data.recentSearches);
        // Important: mark as seeded so WalletProvider doesn't overlay the
        // INITIAL_WALLETS defaults on top of the restored list on next render.
        try {
          localStorage.setItem('sm.wallets.seeded', '1');
          localStorage.setItem('sm.watched.seeded', '1');
        } catch {}
        resolve(data);
      } catch (e) { reject(e); }
    };
    fr.onerror = reject;
    fr.readAsText(file);
  });
}

function BackupRestore({ tweaks, setTweak }) {
  const toast = useToast();
  const fileRef = useRef(null);
  const doBackup = () => {
    downloadBackup(tweaks);
    toast.push('Backup descargado', 'ok');
  };
  const onPick = async (e) => {
    const f = e.target.files?.[0];
    if (!f) return;
    try {
      await restoreBackup(f, setTweak);
      toast.push('Restauración completada', 'ok');
      setTimeout(() => window.location.reload(), 800);
    } catch (err) {
      toast.push('Archivo inválido', 'err');
    }
    e.target.value = '';
  };
  return (
    <div className="tweaks-group" style={{borderTop:'1px solid var(--border)', paddingTop: 14, marginTop: 8}}>
      <label>Backup / Restore</label>
      <div className="tweaks-opts">
        <button className="tweaks-opt" onClick={doBackup}>↓ Backup</button>
        <button className="tweaks-opt" onClick={() => fileRef.current?.click()}>↑ Restore</button>
        <input type="file" ref={fileRef} accept="application/json" style={{display:'none'}} onChange={onPick}/>
      </div>
    </div>
  );
}

Object.assign(window, {
  ToastProvider, useToast,
  WalletProvider, useWallets,
  GlobalSearchProvider, useSearch,
  AddWalletModal, WalletDetailsModal, WalletDetailsProvider,
  // Global wallet-details opener — any section (swaps/transfers/holders/drill)
  // calls window.openWalletDetails(addr, alias?) to pop the modal.
  openWalletDetails,
  ExportCsvButton, exportCsv,
  BackupRestore,
  // Exposed so Portfolio can render dedicated backup/restore buttons without
  // depending on the TweaksPanel being open.
  downloadBackup, restoreBackup,
  Modal,
});
