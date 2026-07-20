/* global React, fmt */
// Identity registry — resolves SORA on-chain display names via POST
// /api/identities (batch), caches for 24h in localStorage.
//
// Design:
//   · Components call `identityName(addr)` synchronously to get a cached hit
//     or null. If null they can call `requestIdentity(addr)` to enqueue a
//     batch fetch. The batcher dispatches once per 200ms to combine N calls.
//   · Results persist in localStorage so identity stays available on reload.
//   · A negative-cache slot (`display === null`) prevents re-requesting
//     addresses that genuinely have no identity set.
const { useState, useEffect } = React;

const IDENT_LS_KEY = 'sm.identityCache.v1';
const IDENT_TTL_MS = 24 * 60 * 60 * 1000; // 24h
const BATCH_DELAY_MS = 200;
const BATCH_MAX = 100; // cap per request; server also caps at 200.

const _identMem = new Map();    // addr -> { ts, display } (display may be null = no identity)
const _subscribers = new Set(); // () => void — called on cache updates
let _pendingQueue = new Set();  // addresses queued for the next batch
let _batchTimer = null;
let _inflight = null;           // Promise of current batch

// User-defined aliases — pulled from localStorage keys `sm.wallets` and
// `sm.watched` (managed by features.jsx). Higher priority than on-chain
// identity so the user's own label always wins. Hydrated synchronously at
// boot, re-hydrated on `storage` events (other tabs) and on explicit
// `refreshAliasMap()` calls (same tab — features.jsx invokes it after
// wallet mutations so tables update without a reload).
const _aliasMap = new Map();    // addr → alias string

function _loadAliasMap() {
  _aliasMap.clear();
  for (const key of ['sm.wallets', 'sm.watched']) {
    try {
      const raw = localStorage.getItem(key);
      if (!raw) continue;
      const arr = JSON.parse(raw);
      if (!Array.isArray(arr)) continue;
      for (const w of arr) {
        if (w && w.addr && w.alias) _aliasMap.set(w.addr, w.alias);
      }
    } catch {}
  }
}
function refreshAliasMap() {
  _loadAliasMap();
  _notify();
}
_loadAliasMap();
if (typeof window !== 'undefined') {
  window.addEventListener('storage', (ev) => {
    if (ev.key === 'sm.wallets' || ev.key === 'sm.watched') refreshAliasMap();
  });
}

// Technical-accounts registry — {ss58: "pallet/sub"} labels pulled from
// /api/tech-accounts (on-chain technical.techAccounts map). Populated at boot,
// stored in localStorage for fast re-hydration, refreshed every 6h.
const TECH_LS_KEY = 'sm.techAccounts.v1';
const TECH_TTL_MS = 6 * 60 * 60 * 1000; // 6h
let _techMap = {};              // ss58 → label

function _loadTechLs() {
  try {
    const raw = JSON.parse(localStorage.getItem(TECH_LS_KEY) || 'null');
    if (raw && raw.ts && raw.data && (Date.now() - raw.ts) < TECH_TTL_MS) return raw;
  } catch {}
  return null;
}
function _saveTechLs(data) {
  try { localStorage.setItem(TECH_LS_KEY, JSON.stringify({ ts: Date.now(), data })); } catch {}
}
// Hydrate on boot + refresh in background.
(function _initTechMap() {
  const cached = _loadTechLs();
  if (cached) _techMap = cached.data;
  fetch('/api/tech-accounts')
    .then(r => r.ok ? r.json() : null)
    .then(j => {
      if (!j || typeof j !== 'object') return;
      _techMap = j;
      _saveTechLs(j);
      _notify();
    })
    .catch(() => {});
})();

function _loadLs() {
  try { return JSON.parse(localStorage.getItem(IDENT_LS_KEY) || '{}'); } catch { return {}; }
}
function _saveLs(obj) {
  try { localStorage.setItem(IDENT_LS_KEY, JSON.stringify(obj)); } catch {}
}
// Hydrate memory from localStorage.
(function _hydrate() {
  const ls = _loadLs();
  const now = Date.now();
  for (const [addr, entry] of Object.entries(ls)) {
    if (entry && (now - entry.ts) < IDENT_TTL_MS) _identMem.set(addr, entry);
  }
})();

function _notify() { _subscribers.forEach(fn => { try { fn(); } catch {} }); }

function _shouldSkip(addr) {
  // Skip resolution for non-SS58 entries — EVM addresses, empty strings, etc.
  if (typeof addr !== 'string') return true;
  if (addr.length < 40) return true;
  if (addr.startsWith('0x')) return true;
  return false;
}

async function _flushBatch() {
  _batchTimer = null;
  if (_pendingQueue.size === 0) return;
  const batch = [..._pendingQueue].slice(0, BATCH_MAX);
  _pendingQueue = new Set([..._pendingQueue].slice(BATCH_MAX));
  _inflight = (async () => {
    try {
      const r = await fetch('/api/identities', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ addresses: batch }),
      });
      if (r.ok) {
        const j = await r.json();
        const now = Date.now();
        const ls = _loadLs();
        for (const addr of batch) {
          const display = j[addr]?.display || null;
          const entry = { ts: now, display };
          _identMem.set(addr, entry);
          ls[addr] = entry;
        }
        _saveLs(ls);
        _notify();
      }
    } catch (_) { /* swallow — we'll retry next trigger */ }
    _inflight = null;
    // If more addresses piled up while we were fetching, kick another batch.
    if (_pendingQueue.size > 0) _scheduleBatch();
  })();
}

function _scheduleBatch() {
  if (_batchTimer || _inflight) return;
  _batchTimer = setTimeout(_flushBatch, BATCH_DELAY_MS);
}

// Public: request an identity resolution. Safe to call repeatedly.
function requestIdentity(addr) {
  if (_shouldSkip(addr)) return;
  const hit = _identMem.get(addr);
  if (hit && (Date.now() - hit.ts) < IDENT_TTL_MS) return;
  _pendingQueue.add(addr);
  _scheduleBatch();
}

// Public: get a cached display name synchronously. Returns null if unknown.
// Priority: user-defined alias > on-chain identity > technical-accounts.
function identityName(addr) {
  if (_shouldSkip(addr)) return null;
  const alias = _aliasMap.get(addr);
  if (alias) return alias;
  const hit = _identMem.get(addr);
  if (hit && (Date.now() - hit.ts) < IDENT_TTL_MS && hit.display) return hit.display;
  // Fallback: technical-accounts registry (from /api/tech-accounts).
  // No per-entry TTL — the map itself is refreshed in background every 6h.
  const tech = _techMap[addr];
  return tech || null;
}

// Public: which source did the name come from? UI uses this to render a tag.
// Returns 'alias' (user-saved), 'chain' (pallet Identity), 'tech' (system
// account), or null.
function identitySource(addr) {
  if (_shouldSkip(addr)) return null;
  if (_aliasMap.has(addr)) return 'alias';
  const hit = _identMem.get(addr);
  if (hit && (Date.now() - hit.ts) < IDENT_TTL_MS && hit.display) return 'chain';
  if (_techMap[addr]) return 'tech';
  return null;
}

// Subscribe to cache updates (used by useIdentity).
function subscribeIdentity(fn) {
  _subscribers.add(fn);
  return () => _subscribers.delete(fn);
}

// Hook: returns the display name (or null), triggers a fetch when missing.
function useIdentity(addr) {
  const [, force] = useState(0);
  useEffect(() => {
    if (!addr || _shouldSkip(addr)) return;
    requestIdentity(addr);
    const unsub = subscribeIdentity(() => force(x => x + 1));
    return unsub;
  }, [addr]);
  return identityName(addr);
}

// Hook variant of identitySource that re-renders on cache updates.
function useIdentitySource(addr) {
  useIdentity(addr); // triggers fetch + subscribe
  return identitySource(addr);
}

// Copies an SS58 address to the clipboard and shows a brief "Copiado"
// flash on the triggering element. Used by the two-zone click pattern in
// AddrStack/AddrOrName: clicking the name copies, clicking the truncated
// address opens the wallet drill.
function _copyAddr(addr, ev) {
  if (ev) ev.stopPropagation();
  if (!addr) return;
  // Capture the element synchronously — React's SyntheticEvent recycles
  // currentTarget after the handler returns, so reading it from inside the
  // later `done()` closure would yield null.
  const el = ev && ev.currentTarget;
  const done = () => {
    if (!el) return;
    if (el.getAttribute('data-copy-flash') === '1') return; // already flashing
    el.setAttribute('data-copy-flash', '1');
    const orig = el.textContent;
    el.textContent = '✓ Copiado';
    setTimeout(() => {
      el.textContent = orig;
      el.removeAttribute('data-copy-flash');
    }, 1100);
  };
  if (navigator.clipboard && navigator.clipboard.writeText) {
    navigator.clipboard.writeText(addr).then(done, done);
  } else {
    try {
      const ta = document.createElement('textarea');
      ta.value = addr; ta.style.position = 'fixed'; ta.style.opacity = '0';
      document.body.appendChild(ta); ta.select();
      document.execCommand('copy');
      document.body.removeChild(ta);
      done();
    } catch {}
  }
}

// Small inline tag rendered next to a resolved name to disambiguate the
// source: '★' alias (user-saved), 'on-chain' (pallet Identity), 'sys'
// (technical account). Returns null for unknown source.
function SourceTag({ source }) {
  if (!source) return null;
  const map = {
    alias: { label: '★',        cls: 'ident-tag-alias', title: 'Alias guardado por ti' },
    chain: { label: 'on-chain', cls: 'ident-tag-chain', title: 'Identidad en cadena' },
    tech:  { label: 'sys',      cls: 'ident-tag-tech',  title: 'Cuenta técnica' },
  };
  const t = map[source]; if (!t) return null;
  return <span className={'ident-tag ' + t.cls} title={t.title}>{t.label}</span>;
}

// Component: renders display name if known, else short address. Always shows a
// tooltip with the full address so the user can verify on hover.
// Two-zone click pattern when both name and address are visible:
//   · Click on the NAME → copy address to clipboard.
//   · Click on the TRUNCATED ADDRESS → opens wallet drill (via onClick).
// When only the address is shown (no name resolved), click falls back to the
// drill — copying without a label would be surprising.
function AddrOrName({ addr, prefix = 5, suffix = 4, bold = false, short = true, onClick, className, style }) {
  const name = useIdentity(addr);
  const source = useIdentitySource(addr);
  if (!addr) return <span className="muted tiny">—</span>;
  const baseStyle = { fontWeight: bold ? 700 : undefined, ...style };
  if (!name) {
    return (
      <span
        className={className || ''}
        style={{ cursor: onClick ? 'pointer' : undefined, ...baseStyle }}
        title={addr}
        onClick={onClick}>
        {fmt.addr(addr, prefix, suffix)}
      </span>
    );
  }
  return (
    <span className={(className || '') + ' ident-hit'} style={{display:'inline-flex', alignItems:'center', gap:6, ...baseStyle}}>
      <span
        style={{cursor:'pointer'}}
        title={'Copiar dirección · ' + addr}
        onClick={(ev) => _copyAddr(addr, ev)}>
        {name}
      </span>
      <SourceTag source={source}/>
      <span
        className="muted tiny num"
        style={{cursor: onClick ? 'pointer' : undefined, textDecoration:'underline dotted', textUnderlineOffset: 2}}
        title={onClick ? 'Abrir wallet · ' + addr : addr}
        onClick={onClick}>
        {fmt.addr(addr, prefix, suffix)}
      </span>
    </span>
  );
}

// Component: two-line stack for table cells — identity name on top (when set)
// above the short address. Two-zone click:
//   · Name line → copies the full SS58 address to the clipboard.
//   · Address line → opens the wallet drill (or invokes a custom `onClick`).
// Used for from/to columns in Transfers/Bridges/OrderBook where the row style
// has no avatar dot.
function AddrStack({ addr, onClick, prefix = 5, suffix = 4, title = 'Abrir wallet' }) {
  const name = useIdentity(addr);
  const source = useIdentitySource(addr);
  if (!addr) return <span className="muted tiny">—</span>;
  const openWallet = onClick || ((ev) => { ev.stopPropagation(); window.openWalletDetails?.(addr, name || null); });
  return (
    <div style={{minWidth: 0}}>
      {name && (
        <div style={{display:'flex', alignItems:'center', gap:4}}>
          <span
            style={{fontSize:11, fontWeight:700, color:'var(--fg-0)', whiteSpace:'nowrap', overflow:'hidden', textOverflow:'ellipsis', maxWidth: 180, cursor:'pointer'}}
            title={'Copiar dirección · ' + addr}
            onClick={(ev) => _copyAddr(addr, ev)}>
            {name}
          </span>
          <SourceTag source={source}/>
        </div>
      )}
      <div
        className="muted tiny num clickable"
        style={{textDecoration:'underline dotted', textUnderlineOffset: 2, whiteSpace:'nowrap', cursor:'pointer'}}
        title={title + ' · ' + addr}
        onClick={openWallet}>
        {fmt.addr(addr, prefix, suffix)}
      </div>
    </div>
  );
}

// Component: renders display name if known, else em-dash. Intended for dedicated
// "Identity" columns next to an existing address column — we don't want to
// duplicate the truncated address here.
function IdentityCell({ addr, className, style }) {
  const name = useIdentity(addr);
  if (!addr || !name) return <span className="muted tiny">—</span>;
  return (
    <span
      className={(className || '') + ' ident-hit'}
      style={style}
      title={addr + ' · ' + name}>
      {name}
    </span>
  );
}

Object.assign(window, {
  identityName, identitySource, requestIdentity, subscribeIdentity, useIdentity,
  useIdentitySource, refreshAliasMap, AddrOrName, IdentityCell, AddrStack, SourceTag,
});
