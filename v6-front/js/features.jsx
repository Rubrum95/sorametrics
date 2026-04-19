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

// Real SS58 addresses that DO have on-chain balances on prod sorametrics.org.
// Used as seed data so new visitors immediately see live numbers without
// having to paste addresses themselves.
const INITIAL_WALLETS = [
  { id:'w1', alias: 'Polkaswap Treasury', addr: 'cnRwt3q7DkvJqr3YkuN7dFibTx6yu8rqDDYKmBp4Sko5TW2Dd', value: 0, live: false, kind:'watch' },
  { id:'w2', alias: 'XOR Whale',          addr: 'cnVhh27kkYkfJ1mH4jyPWWV6Tq2jp4dSU1wBDwZ5efRCTgq11', value: 0, live: false, kind:'watch' },
  { id:'w3', alias: 'DEX Maker',          addr: 'cnSpcE5u2H8QqcppzUhM7rbMbshvcZBSKVPE6ANZASL3orN1V', value: 0, live: false, kind:'watch' },
  { id:'w4', alias: 'Active Trader',      addr: 'cnRWUaRNHRbcg6ZnjkF7z17tRiz1oVQXk6GzT4PvCrxVjXyXB', value: 0, live: false, kind:'watch' },
];
const INITIAL_WATCHED = [
  { id:'v1', alias: 'Whale.sora',  addr: FAKE_ADDRS[6], value: 482300 },
  { id:'v2', alias: 'Sakura Node', addr: FAKE_ADDRS[7], value: 128600 },
  { id:'v3', alias: 'Cerberus',    addr: FAKE_ADDRS[4], value: 96400  },
];

function loadLS(k, fallback) {
  try { const s = localStorage.getItem(k); if (s) return JSON.parse(s); } catch (_) {}
  return fallback;
}
function saveLS(k, v) { try { localStorage.setItem(k, JSON.stringify(v)); } catch (_) {} }

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
  const [wallets, setWallets] = useState(() => loadLS('sm.wallets', INITIAL_WALLETS));
  const [watched, setWatched] = useState(() => loadLS('sm.watched', INITIAL_WATCHED));
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

  const api = {
    wallets: walletsWithData, watched: watchedWithData, setWallets, setWatched,
    addWallet: (w) => setWallets(ws => [...ws, { id:'w'+Date.now(), value: 0, live: false, ...w }]),
    addWatched: (w) => setWatched(ws => [...ws, { id:'v'+Date.now(), value: 0, ...w }]),
    removeWallet: (id) => setWallets(ws => ws.filter(w => w.id !== id)),
    renameWallet: (id, alias) => setWallets(ws => ws.map(w => w.id === id ? { ...w, alias } : w)),
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
const SEARCH_BLOCKS = [
  21418802, 21418795, 21418400, 21418202, 21417950, 21417800,
];

function buildSearchIndex() {
  const out = [];
  // Wallets (user + watched) — use current localStorage state if present
  const wallets = loadLS('sm.wallets', INITIAL_WALLETS);
  const watched = loadLS('sm.watched', INITIAL_WATCHED);
  wallets.forEach(w => out.push({
    type:'wallet', primary: w.alias, secondary: fmt.addr(w.addr, 10, 8) + ' · $' + w.value.toLocaleString(),
    raw: w.addr, section:'balance',
  }));
  watched.forEach(w => out.push({
    type:'wallet', primary: w.alias + '  (watch)', secondary: fmt.addr(w.addr, 10, 8) + ' · $' + w.value.toLocaleString(),
    raw: w.addr, section:'balance',
  }));
  FAKE_ADDRS.forEach(a => {
    if (IDENTITIES[a]) out.push({ type:'wallet', primary: IDENTITIES[a], secondary: fmt.addr(a,10,8), raw:a, section:'holders' });
    else out.push({ type:'wallet', primary: fmt.addr(a,8,8), secondary: 'unnamed account', raw:a, section:'holders' });
  });
  // Tx hashes
  const hashes = [
    '0x2fd4e1c67a2d28fced849ee1bb76e7391b93eb12',
    '0x3f7c2e4a1d9b8c5e6f0a2b3c4d5e6f7890123abc',
    '0x8a1b2c3d4e5f6070809a0b1c2d3e4f5060708090',
    '0xd41d8cd98f00b204e9800998ecf8427ebfef1234',
    '0x9f86d081884c7d659a2feaa0c55ad015a3bf4f1b',
    '0x1234567890abcdef1234567890abcdef12345678',
    '0xabcdef0123456789abcdef0123456789abcdef01',
    '0x5f1aec6e0cbe2a8bfa6c2f0d5a9b3e7c4d8e1f0a',
  ];
  hashes.forEach(h => out.push({
    type:'tx', primary: fmt.addr(h, 10, 8), secondary: 'Signed · block #' + (21418000 + Math.floor(Math.random()*900)),
    raw: h, section:'swaps',
  }));
  // Blocks
  SEARCH_BLOCKS.forEach(b => out.push({
    type:'block', primary: 'Block #' + b.toLocaleString(),
    secondary: (40 + (b % 40)) + ' extrinsics · ' + ((b % 12) + 1) + 's ago',
    raw: String(b), section:'extrinsics',
  }));
  // Extrinsics
  ['balances.transfer','assets.mint','demeterFarming.deposit','pswap.swap','staking.bond',
   'referrals.setReferrer','liquidityProxy.swap','council.vote','bridgeProxy.addPeer'
  ].forEach(m => out.push({ type:'extrinsic', primary: m, secondary: 'pallet::method · 1,240 calls 24h', raw:m, section:'extrinsics' }));
  // Tokens
  Object.keys(TOKENS || {XOR:1,VAL:1,PSWAP:1,KUSD:1,ETH:1,XST:1,KEN:1,TBCD:1,XSTUSD:1}).forEach(k => {
    out.push({ type:'token', primary: k, secondary: 'asset · ' + k + '/USD market', raw: k, section:'tokens' });
  });
  // Pools
  ['XOR/VAL','XOR/KUSD','PSWAP/XOR','KUSD/PSWAP','ETH/XOR','VAL/KUSD','KEN/KUSD','XST/XOR'].forEach(p => {
    out.push({ type:'pool', primary: p, secondary: 'pool · TVL $' + (120 + Math.floor(Math.random()*800)) + 'K', raw: p, section:'pools' });
  });
  // Validators
  ['Sakura Node','Cerberus','Kusari','Moonflower','PolkaLab','Aurora','Fujiwara','Akira'].forEach(v => {
    out.push({ type:'validator', primary: v, secondary: 'validator · commission ' + ((v.length % 8) + 2) + '%', raw: v, section:'staking' });
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
            raw: String(j.data.block), section: 'extrinsics', real: true,
          });
        } else if ((j.type === 'tx' || j.type === 'extrinsic') && j.data) {
          hits.push({
            type: j.type, primary: j.data.hash || j.data.extrinsic_id || q,
            secondary: j.type + ' · block ' + (j.data.block || '?'),
            raw: j.data.hash || j.data.extrinsic_id, section: 'extrinsics', real: true,
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

  const commit = useCallback((r, metaClick) => {
    if (!r) return;
    const next = [{ q: r.primary, ts: Date.now() }, ...recents.filter(x => x.q !== r.primary)].slice(0, 6);
    setRecents(next); saveLS('sm.searchRecent', next);
    if (metaClick) {
      // open drill via window bus
      try { window.__SM_DRILL__?.open({ type: r.type === 'tx' ? 'swap' : r.type, title: r.primary, ts: Date.now(), hash: r.raw, block: 21418802, caller: FAKE_ADDRS[0], inSym:'XOR', outSym:'VAL', inAmt: 12, outAmt: 1.8, pair: r.primary, size: 100, price: 0.42, sym:'XOR', amt:12, usd: 124, from: FAKE_ADDRS[0], to: FAKE_ADDRS[1] }); } catch(_){}
    } else {
      try { window.__SM_NAV__?.(r.section); } catch(_){}
    }
    onClose();
    toast.push(r.primary + ' →', 'ok');
  }, [recents, onClose, toast]);

  const onKey = (e) => {
    if (e.key === 'ArrowDown') { e.preventDefault(); setCursor(c => Math.min(flat.length - 1, c + 1)); }
    else if (e.key === 'ArrowUp') { e.preventDefault(); setCursor(c => Math.max(0, c - 1)); }
    else if (e.key === 'Enter') { e.preventDefault(); commit(flat[cursor], e.metaKey || e.ctrlKey); }
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
function AddWalletModal({ open, onClose }) {
  const [tab, setTab] = useState('seed');
  const [alias, setAlias] = useState('');
  const [words, setWords] = useState(Array(12).fill(''));
  const [pk, setPk] = useState('');
  const [watchAddr, setWatchAddr] = useState('');
  const [len, setLen] = useState(12);
  const wallets = useWallets();
  const toast = useToast();

  useEffect(() => { if (open) { setAlias(''); setWords(Array(len).fill('')); setPk(''); setWatchAddr(''); } }, [open, len]);

  const handlePaste = (e) => {
    const text = (e.clipboardData || window.clipboardData)?.getData('text') || '';
    const parts = text.trim().split(/\s+/);
    if (parts.length >= 12) {
      e.preventDefault();
      const n = parts.length >= 24 ? 24 : 12;
      setLen(n);
      setWords(parts.slice(0, n).concat(Array(Math.max(0, n - parts.length)).fill('')));
    }
  };

  const seedValid = words.filter(w => w.trim().length >= 3).length === len;
  const pkValid = pk.trim().length >= 32 && /^0x[a-f0-9]+$/i.test(pk.trim());
  const watchValid = watchAddr.trim().length >= 30;

  const submit = () => {
    let ok = false;
    if (tab === 'seed' && seedValid) {
      wallets.addWallet({ alias: alias || 'Nueva wallet', addr: FAKE_ADDRS[Math.floor(Math.random()*FAKE_ADDRS.length)], kind:'seed', value: Math.floor(Math.random()*12000 + 500) });
      ok = true;
    } else if (tab === 'key' && pkValid) {
      wallets.addWallet({ alias: alias || 'Wallet PK', addr: FAKE_ADDRS[Math.floor(Math.random()*FAKE_ADDRS.length)], kind:'key', value: Math.floor(Math.random()*12000 + 500) });
      ok = true;
    } else if (tab === 'watch' && watchValid) {
      wallets.addWatched({ alias: alias || 'Observada', addr: watchAddr.trim(), value: Math.floor(Math.random()*200000 + 2000) });
      ok = true;
    }
    if (ok) {
      toast.push('Cartera añadida', 'ok');
      onClose();
    } else {
      toast.push('Datos incompletos', 'err');
    }
  };

  return (
    <Modal open={open} onClose={onClose} width={560} label="Añadir cartera">
      <div className="sm-modal-head">
        <h3>Añadir Cartera</h3>
        <button className="sm-modal-x" onClick={onClose}>×</button>
      </div>

      <div className="sm-modal-tabs">
        <button className={'sm-modal-tab' + (tab==='seed'?' active':'')} onClick={() => setTab('seed')}>Importar seed</button>
        <button className={'sm-modal-tab' + (tab==='key'?' active':'')} onClick={() => setTab('key')}>Clave privada</button>
        <button className={'sm-modal-tab' + (tab==='watch'?' active':'')} onClick={() => setTab('watch')}>Solo watch</button>
      </div>

      <div className="sm-modal-body">
        <div className="sm-field">
          <label>Alias</label>
          <input className="sm-input" value={alias} onChange={e => setAlias(e.target.value)} placeholder="Ej: Trading, Savings…"/>
        </div>

        {tab === 'seed' && (
          <>
            <div className="sm-field-row">
              <label>Longitud</label>
              <div className="sm-mini-toggle">
                <button className={len===12?'active':''} onClick={() => setLen(12)}>12 palabras</button>
                <button className={len===24?'active':''} onClick={() => setLen(24)}>24 palabras</button>
              </div>
            </div>
            <div className={'sm-seed-grid' + (len===24?' seed-24':'')}>
              {words.map((w, i) => (
                <div key={i} className="sm-seed-cell">
                  <span className="sm-seed-num">{i+1}</span>
                  <input className="sm-seed-input" value={w}
                         onPaste={i === 0 ? handlePaste : undefined}
                         onChange={e => setWords(words.map((x,j) => j===i ? e.target.value : x))}/>
                </div>
              ))}
            </div>
            {!seedValid && words.some(w => w.length > 0) && (
              <div className="sm-banner warn">Introduce las {len} palabras para continuar.</div>
            )}
            <div className="sm-banner info">Pega tu seed en la primera casilla para rellenar automáticamente.</div>
          </>
        )}

        {tab === 'key' && (
          <>
            <div className="sm-field">
              <label>Clave privada (hex)</label>
              <textarea className="sm-input sm-textarea" rows={4} value={pk}
                        onChange={e => setPk(e.target.value)}
                        placeholder="0x…"/>
            </div>
            <div className="sm-banner err">
              <b>⚠ Nunca compartas tu clave privada.</b> Esta demo no envía nada a ningún servidor; aun así, en producción evita pegar claves en dispositivos de terceros.
            </div>
          </>
        )}

        {tab === 'watch' && (
          <>
            <div className="sm-field">
              <label>Dirección SORA</label>
              <input className="sm-input" value={watchAddr} onChange={e => setWatchAddr(e.target.value)} placeholder="cnV…"/>
            </div>
            <div className="sm-banner info">Las carteras vigiladas son de solo lectura — verás saldo y actividad sin poder firmar.</div>
          </>
        )}
      </div>

      <div className="sm-modal-foot">
        <button className="btn" onClick={onClose}>Cancelar</button>
        <button className="btn primary" onClick={submit}>Añadir</button>
      </div>
    </Modal>
  );
}

// Renders the 4 history sub-tabs inside WalletDetailsModal. Each row shape
// differs between endpoints; we map defensively to a common {time, block,
// primary, secondary} tuple.
function WalletHistoryTable({ kind, rows }) {
  if (rows === null) return <div className="muted">Cargando {kind}…</div>;
  if (!rows || rows.length === 0) return <div className="muted tiny">Sin {kind} recientes para esta cartera.</div>;
  return (
    <table className="lp-table">
      <thead><tr><th>Hora</th><th>Bloque</th><th>Detalle</th></tr></thead>
      <tbody>
        {rows.slice(0, 30).map((r, i) => {
          let primary = '';
          if (kind === 'swaps') primary = (Number(r.in?.amount || 0).toFixed(2)) + ' ' + (r.in?.symbol || '') + ' → ' + (Number(r.out?.amount || 0).toFixed(2)) + ' ' + (r.out?.symbol || '');
          else if (kind === 'transfers') primary = (Number(r.amount || 0).toFixed(2)) + ' ' + (r.symbol || '') + ' · ' + fmt.addr(r.from, 6, 4) + ' → ' + fmt.addr(r.to, 6, 4);
          else if (kind === 'bridges') primary = (r.direction || '') + ' ' + (Number(r.amount || 0).toFixed(2)) + ' ' + (r.symbol || '') + ' · ' + (r.network || '');
          else if (kind === 'extrinsics') primary = (r.section || '') + '::' + (r.method || '') + (r.success === 1 ? ' ✓' : ' ✗');
          return (
            <tr key={(r.hash || '') + i}>
              <td className="tiny">{r.time || r.timestamp || '—'}</td>
              <td className="num tiny">#{r.block}</td>
              <td className="tiny">{primary}</td>
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

function WalletDetailsModal({ wallet, open, onClose, onRemove }) {
  const [alias, setAlias] = useState('');
  const [confirmRm, setConfirmRm] = useState(false);
  const [copied, setCopied] = useState(false);
  const [subtab, setSubtab] = useState('assets');
  const wallets = useWallets();
  const toast = useToast();

  useEffect(() => { if (wallet) setAlias(wallet.alias); setConfirmRm(false); setSubtab('assets'); }, [wallet, open]);

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
  const breakdown = numericTokens.slice(0, 6).map(t => ({
    sym: t.sym,
    pct: totalUsd > 0 ? t.usdValue / totalUsd : 0,
    color: TOKEN_COLOR[t.sym] || '#94A3B8',
    amt: t.amount,
    usd: t.usdValue,
  }));
  // Fallback placeholder when prod returns empty tokens (e.g. unused address).
  if (!breakdown.length) breakdown.push({ sym: '—', pct: 1, color: '#4A3566', amt: 0, usd: 0 });

  const copyAddr = async () => {
    try { await navigator.clipboard.writeText(wallet.addr); setCopied(true); setTimeout(() => setCopied(false), 1400); } catch(_){}
  };
  const saveRename = () => {
    if (!alias.trim() || alias === wallet.alias) return;
    wallets.renameWallet(wallet.id, alias.trim());
    toast.push('Alias actualizado', 'ok');
  };
  const doRemove = () => {
    wallets.removeWallet(wallet.id);
    toast.push('Cartera eliminada', 'ok');
    onClose();
  };

  return (
    <Modal open={open} onClose={onClose} width={540} label={wallet.alias}>
      <div className="sm-modal-head">
        <div style={{display:'flex', alignItems:'center', gap:12}}>
          <div className="sm-avatar" style={{background:'linear-gradient(135deg,#9B1B30,#4A3566)'}}>{wallet.alias[0]}</div>
          <div>
            <h3 style={{margin:0}}>{wallet.alias}</h3>
            <div className="muted tiny">{wallet.kind === 'watch' ? 'Solo lectura' : (wallet.kind === 'key' ? 'Clave privada' : 'Seed')}</div>
          </div>
        </div>
        <button className="sm-modal-x" onClick={onClose}>×</button>
      </div>

      <div className="sm-modal-tabs" style={{overflowX:'auto'}}>
        {[
          ['assets',     'Assets'],
          ['swaps',      'Swaps'],
          ['transfers',  'Transfers'],
          ['bridges',    'Bridges'],
          ['liquidity',  'Liquidity'],
          ['staking',    'Staking'],
          ['extrinsics', 'Extrinsics'],
          ['info',       'Info'],
        ].map(([id, lbl]) => (
          <button key={id} className={'sm-modal-tab' + (subtab===id?' active':'')} onClick={() => setSubtab(id)}>{lbl}</button>
        ))}
      </div>

      <div className="sm-modal-body">
        {subtab === 'assets' && (<>
          <div className="sm-field">
            <label>Alias</label>
            <div style={{display:'flex', gap:8}}>
              <input className="sm-input" value={alias} onChange={e => setAlias(e.target.value)}/>
              <button className="btn" onClick={saveRename} disabled={!alias.trim() || alias === wallet.alias}>Guardar</button>
            </div>
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
            <div className="sm-breakdown">
              {breakdown.map(b => (
                <div key={b.sym} className="sm-breakdown-row">
                  <span className="sm-bd-dot" style={{background:b.color}}/>
                  <span className="sm-bd-sym">{b.sym}</span>
                  <div className="sm-bd-bar"><div style={{width:(b.pct*100)+'%', background:b.color}}/></div>
                  <span className="num tiny muted">{(b.pct*100).toFixed(1)}%</span>
                  <span className="num" style={{fontWeight:600, minWidth:90, textAlign:'right'}}>
                    {b.usd > 0 ? '$' + b.usd.toLocaleString(undefined,{maximumFractionDigits:2}) : (b.amt > 0 ? fmt.num(b.amt, 2) : '—')}
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

        {subtab === 'liquidity' && (
          <div className="sm-field">
            <label>Posiciones de liquidez</label>
            {!liquidity ? <div className="muted">Cargando…</div> :
             (liquidity.positions || []).length === 0 ? <div className="muted tiny">Sin posiciones de liquidez.</div> :
              <table className="lp-table">
                <thead><tr><th>Pool</th><th style={{textAlign:'right'}}>Share</th><th style={{textAlign:'right'}}>Valor</th></tr></thead>
                <tbody>
                  {(liquidity.positions || []).slice(0, 30).map((p, i) => (
                    <tr key={i}>
                      <td>{p.base?.symbol || p.base}/{p.target?.symbol || p.target}</td>
                      <td style={{textAlign:'right'}} className="num">{((Number(p.share) || 0) * 100).toFixed(2)}%</td>
                      <td style={{textAlign:'right'}} className="num">{fmt.usd(Number(p.usdValue || p.value) || 0)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            }
          </div>
        )}

        {subtab === 'staking' && (
          <div className="sm-field">
            <label>Staking</label>
            {!staking ? <div className="muted">Cargando…</div> :
              <pre className="ext-args" style={{maxHeight: 300, overflow:'auto'}}>{JSON.stringify(staking, null, 2)}</pre>
            }
          </div>
        )}

        {subtab === 'info' && (
          <div className="sm-field">
            <label>Información on-chain</label>
            {!info ? <div className="muted">Cargando…</div> :
              <pre className="ext-args" style={{maxHeight: 300, overflow:'auto'}}>{JSON.stringify(info, null, 2)}</pre>
            }
          </div>
        )}
      </div>

      <div className="sm-modal-foot">
        {!confirmRm ? (
          <>
            <button className="btn danger" onClick={() => setConfirmRm(true)}>Eliminar</button>
            <div style={{flex:1}}/>
            <button className="btn primary" onClick={onClose}>Cerrar</button>
          </>
        ) : (
          <div className="sm-confirm-row">
            <span>¿Eliminar “{wallet.alias}”? No se puede deshacer.</span>
            <button className="btn" onClick={() => setConfirmRm(false)}>Cancelar</button>
            <button className="btn danger" onClick={doRemove}>Sí, eliminar</button>
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

function restoreBackup(file, setTweak) {
  return new Promise((resolve, reject) => {
    const fr = new FileReader();
    fr.onload = () => {
      try {
        const data = JSON.parse(fr.result);
        if (data.settings?.lang) saveLS('sorametrics.lang', data.settings.lang);
        if (data.settings?.tweaks && setTweak) {
          Object.entries(data.settings.tweaks).forEach(([k,v]) => setTweak(k, v));
        }
        if (data.wallets) saveLS('sm.wallets', data.wallets);
        if (data.watchlist) saveLS('sm.watched', data.watchlist.map((w,i) => ({ id:'v'+i, value:0, ...w })));
        if (data.favorites?.tokens) saveLS('sm.favTokens', data.favorites.tokens);
        if (data.favorites?.wallets) saveLS('sm.favWallets', data.favorites.wallets);
        if (data.recentSearches) saveLS('sm.searchRecent', data.recentSearches);
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
  AddWalletModal, WalletDetailsModal,
  ExportCsvButton, exportCsv,
  BackupRestore,
  Modal,
});
