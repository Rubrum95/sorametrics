/* global React, ReactDOM */
const { useState, useEffect, useRef, useMemo, useCallback } = React;

// -------- Parse the Spanish-locale date strings prod emits on every history row:
//   "18/04/2026 17:06:24" or "18/4/2026, 17:06:24"
// Falls back to Date.now() when unparseable.
function parseHistTime(t) {
  if (!t) return Date.now();
  if (typeof t === 'number') return t;
  // Some prod endpoints (/history/global/liquidity among them) return the
  // timestamp as a NUMERIC STRING in milliseconds (e.g. "1776831492000").
  // Date.parse doesn't accept those — it expects ISO or RFC strings — so
  // we'd fall through to Date.now() and every row would display "now".
  // Detect the pure-digit case first and coerce to Number.
  const s = String(t).trim();
  if (/^\d+$/.test(s)) {
    const n = Number(s);
    // Seconds vs milliseconds heuristic: anything below 1e12 is clearly in
    // seconds (dates before the year 33658 in ms). Multiply up to ms.
    return n < 1e12 ? n * 1000 : n;
  }
  const iso = Date.parse(s);
  if (!Number.isNaN(iso)) return iso;
  const re = /^(\d{1,2})\/(\d{1,2})\/(\d{4})(?:[,\s]\s*(\d{1,2}):(\d{2})(?::(\d{2}))?)?$/;
  const m = s.match(re);
  if (m) {
    const [, d, mo, y, hh = '0', mm = '0', ss = '0'] = m;
    const dt = new Date(Number(y), Number(mo) - 1, Number(d), Number(hh), Number(mm), Number(ss));
    if (!Number.isNaN(dt.getTime())) return dt.getTime();
  }
  return Date.now();
}

// Generic history fetcher — backs any section that hits /history/global/*.
// Returns { items, loading, error, refresh, totalPages }.
// Polls every 30s for freshness (cheap since prod uses MV + live buffer).
function useHistory(endpoint, { pageSize = 20, page = 1, pollMs = 30_000 } = {}) {
  const [items, setItems] = useState([]);
  const [total, setTotal] = useState(null);          // backend-reported total record count
  const [totalPages, setTotalPages] = useState(null); // backend-reported total pages
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [nonce, setNonce] = useState(0);
  useEffect(() => {
    let cancelled = false;
    const url = endpoint + (endpoint.includes('?') ? '&' : '?') + 'limit=' + pageSize + '&page=' + page;
    const pull = async () => {
      try {
        const r = await fetch(url);
        if (!r.ok) throw new Error('HTTP ' + r.status);
        const j = await r.json();
        if (cancelled) return;
        // Server response shapes vary: some return bare arrays, others return
        // { data, total, totalPages, page }. Normalise both so the consumer
        // sees real server-side pagination when available. Previously the
        // total/totalPages were thrown away, forcing every section to
        // compute pagination locally against `items.length` (= 20) → the UI
        // would only show 1-5 pages regardless of how many the backend had.
        if (Array.isArray(j)) {
          setItems(j);
          setTotal(null); setTotalPages(null);
        } else {
          setItems(j.data || j.result || []);
          const backendTotal = Number(j.total);
          const backendTotalPages = Number(j.totalPages);
          setTotal(Number.isFinite(backendTotal) ? backendTotal : null);
          setTotalPages(Number.isFinite(backendTotalPages) ? backendTotalPages : null);
        }
        setError(null);
      } catch (e) {
        if (!cancelled) setError(e.message || 'fetch error');
      } finally {
        if (!cancelled) setLoading(false);
      }
    };
    pull();
    const id = pollMs > 0 ? setInterval(pull, pollMs) : null;
    return () => { cancelled = true; if (id) clearInterval(id); };
  }, [endpoint, pageSize, page, nonce]);
  const refresh = useCallback(() => setNonce(n => n + 1), []);
  return { items, total, totalPages, loading, error, refresh };
}

// --- small helpers ---
const fmt = {
  num(n, d = 2) {
    if (n == null || isNaN(n)) return '—';
    if (Math.abs(n) >= 1e9) return (n / 1e9).toFixed(d) + 'B';
    if (Math.abs(n) >= 1e6) return (n / 1e6).toFixed(d) + 'M';
    if (Math.abs(n) >= 1e3) return (n / 1e3).toFixed(d) + 'K';
    return n.toFixed(d);
  },
  int(n) {
    return Math.round(n).toLocaleString('en-US');
  },
  usd(n, d = 2) {
    if (n == null || isNaN(n)) return '$0';
    const sign = n < 0 ? '-' : '';
    n = Math.abs(n);
    if (n >= 1e9) return sign + '$' + (n / 1e9).toFixed(d) + 'B';
    if (n >= 1e6) return sign + '$' + (n / 1e6).toFixed(d) + 'M';
    if (n >= 1e3) return sign + '$' + (n / 1e3).toFixed(d) + 'K';
    return sign + '$' + n.toFixed(d);
  },
  addr(a, left = 6, right = 4) {
    if (!a) return '';
    if (a.length <= left + right + 2) return a;
    return a.slice(0, left) + '…' + a.slice(-right);
  },
  ago(ts) {
    const s = Math.floor((Date.now() - ts) / 1000);
    if (s < 5)  return 'now';
    if (s < 60) return s + 's';
    if (s < 3600) return Math.floor(s/60) + 'm';
    if (s < 86400) return Math.floor(s/3600) + 'h';
    return Math.floor(s/86400) + 'd';
  },
  // Absolute timestamp — "2026-04-21 18:42:09". Use for tooltips on top of
  // fmt.ago so the user can see the exact moment on hover without changing
  // the at-a-glance "5m" / "2h" layout everywhere.
  fullDate(ts) {
    if (!ts) return '';
    const d = new Date(ts);
    if (isNaN(d.getTime())) return '';
    const pad = (n) => String(n).padStart(2, '0');
    return d.getFullYear() + '-' + pad(d.getMonth() + 1) + '-' + pad(d.getDate())
      + ' ' + pad(d.getHours()) + ':' + pad(d.getMinutes()) + ':' + pad(d.getSeconds());
  },
  // Combined: "5m · 2026-04-21 18:42:09". For inline use when there's space.
  agoWithDate(ts) {
    const a = this.ago(ts); const d = this.fullDate(ts);
    return d ? `${a} · ${d}` : a;
  }
};

// deterministic-ish random
function seededRand(seed) {
  let s = seed;
  return () => {
    s = (s * 9301 + 49297) % 233280;
    return s / 233280;
  };
}

// generic sparkline path
function sparkPath(values, w, h, pad = 2) {
  if (!values || values.length < 2) return '';
  const min = Math.min(...values);
  const max = Math.max(...values);
  const span = Math.max(1e-9, max - min);
  return values.map((v, i) => {
    const x = pad + (i / (values.length - 1)) * (w - pad * 2);
    const y = h - pad - ((v - min) / span) * (h - pad * 2);
    return (i === 0 ? 'M' : 'L') + x.toFixed(1) + ',' + y.toFixed(1);
  }).join(' ');
}

// area path (returns both line and area)
function areaPath(values, w, h, pad = 4) {
  if (!values || values.length < 2) return { line: '', area: '' };
  const min = Math.min(...values);
  const max = Math.max(...values);
  const span = Math.max(1e-9, max - min);
  const pts = values.map((v, i) => {
    const x = pad + (i / (values.length - 1)) * (w - pad * 2);
    const y = h - pad - ((v - min) / span) * (h - pad * 2);
    return [x, y];
  });
  const line = pts.map((p, i) => (i === 0 ? 'M' : 'L') + p[0].toFixed(1) + ',' + p[1].toFixed(1)).join(' ');
  const area = line + ` L${pts[pts.length-1][0].toFixed(1)},${h - pad} L${pts[0][0].toFixed(1)},${h - pad} Z`;
  return { line, area };
}

// Token palette for burn + portfolio
const TOKENS = {
  XOR:   { color: '#E5243B', dark: '#7B1D24', glow: 'rgba(229,36,59,0.4)',  name: 'XOR',  grad: 'linear-gradient(135deg, #FF4E3C, #E5243B, #B91C30)' },
  VAL:   { color: '#F5B041', dark: '#8B6428', glow: 'rgba(245,176,65,0.4)', name: 'VAL',  grad: 'linear-gradient(135deg, #FFD166, #F5B041, #D4902E)' },
  PSWAP: { color: '#EC4899', dark: '#831843', glow: 'rgba(236,72,153,0.4)', name: 'PSWAP',grad: 'linear-gradient(135deg, #F9A8D4, #EC4899, #BE185D)' },
  TBCD:  { color: '#10B981', dark: '#064E3B', glow: 'rgba(16,185,129,0.4)', name: 'TBCD', grad: 'linear-gradient(135deg, #34D399, #10B981, #047857)' },
  KUSD:  { color: '#60A5FA', dark: '#1E3A8A', glow: 'rgba(96,165,250,0.4)', name: 'KUSD', grad: 'linear-gradient(135deg, #93C5FD, #60A5FA, #2563EB)' },
  ETH:   { color: '#8B7FD9', dark: '#3B3A6B', glow: 'rgba(139,127,217,0.4)',name: 'ETH',  grad: 'linear-gradient(135deg, #A6A1E3, #8B7FD9, #6258B8)' },
  DAI:   { color: '#FBB040', dark: '#7C5A20', glow: 'rgba(251,176,64,0.4)', name: 'DAI',  grad: 'linear-gradient(135deg, #FCD34D, #FBB040, #D97706)' },
};

// Fake SORA-ish addresses (random but consistent in shape)
const FAKE_ADDRS = [
  'cnV0Qxz5s7K9m4nG2vCZbGq3nKrPfYk6B7LwXy3dW1nT',
  'cnVkY8p4c9hG2mWqXbKnRs4TfVn3hLqY2JzB5DmEkPiN',
  'cnVpN2LmS4qX9ZkR7WbC3tFpGhYn5Q8MdVwXjBkLzT6R',
  'cnVqP5MrT3nC8kYb9WqVzXhF4GjL2NsDpVwBkYnZmQ1T',
  'cnVtR9KcW2nM4LpYbXqSfGh5JkT6VdZn3Q8PwBmLzRyK',
  'cnVjH1BnKqS6LcYpXrWfGhF3ZkT9MdVn2Q5WbLmBzKyN',
  'cnVmD3FpLnQ8WcYbXqRsGhJ4YnT7VdZn5Q2WbKmCzRyP',
  'cnVbC4LqTrN9MpYbXnWsFgJ2ZkR6VdXn5Q8WbMmDzSyL',
];

const IDENTITIES = {
  [FAKE_ADDRS[0]]: 'Polkaswap',
  [FAKE_ADDRS[1]]: 'XOR Treasury',
  [FAKE_ADDRS[2]]: 'Bridge Reserve',
  [FAKE_ADDRS[3]]: 'DAO Multisig',
  [FAKE_ADDRS[4]]: 'Cerberus',
  [FAKE_ADDRS[5]]: 'Kusari',
  [FAKE_ADDRS[6]]: 'Whale.sora',
  [FAKE_ADDRS[7]]: 'Sakura Node',
};

// ---- Icons (inline SVG)
const I = {
  burn: (p) => <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" {...p}><path d="M8.5 14.5A2.5 2.5 0 0 0 11 12c0-1.5-.5-3-2-4.5-1.5-1.5-4-3-4-5 0 0 1 2 2 2s2-2 2-2c0 0 3 3 3 6 0 3-3 3-3 6a2.5 2.5 0 0 0 2.5 2.5zM13 15.5l.5-1c.5-1 2-1 3 0 1.5 1.5 1 3.5-1 3.5-1.5 0-2.5-1-2.5-2.5z"/><path d="M12 22c5.5 0 8-3.5 8-8 0-3-2-5-3-6-.5 1-1 2-2 2.5"/></svg>,
  pulse: (p) => <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" {...p}><path d="M3 12h3l3-9 4 18 3-9h5"/></svg>,
  wallet: (p) => <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" {...p}><rect x="3" y="6" width="18" height="14" rx="3"/><path d="M3 10h18M16 14h2"/></svg>,
  tokens: (p) => <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" {...p}><circle cx="9" cy="9" r="6"/><circle cx="15" cy="15" r="6"/></svg>,
  swap: (p) => <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" {...p}><path d="M7 7h13l-3-3M17 17H4l3 3"/></svg>,
  ext: (p) => <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" {...p}><path d="M4 6h16M4 12h16M4 18h10"/><circle cx="18" cy="18" r="2"/></svg>,
  pools: (p) => <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" {...p}><path d="M3 10c3-3 6-3 9 0s6 3 9 0M3 16c3-3 6-3 9 0s6 3 9 0"/></svg>,
  gov: (p) => <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" {...p}><path d="M3 21h18M5 21V10l7-5 7 5v11M9 21V14h6v7"/></svg>,
  stake: (p) => <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" {...p}><path d="M12 2l10 6-10 6L2 8l10-6zM2 12l10 6 10-6M2 16l10 6 10-6"/></svg>,
  search: (p) => <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" {...p}><circle cx="11" cy="11" r="7"/><path d="m20 20-3.5-3.5"/></svg>,
  sliders: (p) => <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" {...p}><path d="M4 6h10M18 6h2M4 12h4M12 12h8M4 18h12M20 18h0"/><circle cx="16" cy="6" r="2"/><circle cx="10" cy="12" r="2"/><circle cx="18" cy="18" r="2"/></svg>,
  send: (p) => <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" {...p}><path d="M22 2 11 13M22 2l-7 20-4-9-9-4z"/></svg>,
  bridge: (p) => <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" {...p}><path d="M3 10l3-3 3 3M15 14l3 3 3-3M3 10v4M21 14v-4M9 7h6M9 17h6"/></svg>,
  book: (p) => <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" {...p}><path d="M4 4h7v16H4zM13 4h7v16h-7zM4 10h7M13 14h7"/></svg>,
  users: (p) => <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" {...p}><circle cx="9" cy="8" r="4"/><path d="M1 21v-2a6 6 0 0 1 12 0v2M17 11a4 4 0 1 0 0-8M23 21v-2a6 6 0 0 0-4-5.6"/></svg>,
  coins: (p) => <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" {...p}><ellipse cx="12" cy="6" rx="8" ry="3"/><path d="M4 6v6c0 1.7 3.6 3 8 3s8-1.3 8-3V6M4 12v6c0 1.7 3.6 3 8 3s8-1.3 8-3v-6"/></svg>,
  bolt: (p) => <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" {...p}><path d="M13 2 3 14h8l-1 8 10-12h-8l1-8z"/></svg>,
  music: (p) => <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" {...p}><path d="M9 18V5l12-2v13"/><circle cx="6" cy="18" r="3"/><circle cx="18" cy="16" r="3"/></svg>,
  up: (p) => <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round" {...p}><path d="m7 15 5-5 5 5"/></svg>,
  down: (p) => <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round" {...p}><path d="m7 9 5 5 5-5"/></svg>,
};

// Petal background
function Petals({ count = 16 }) {
  const rand = useMemo(() => seededRand(42), []);
  const petals = useMemo(() => Array.from({ length: count }, (_, i) => ({
    left: (i / count) * 100 + rand() * (100/count),
    delay: rand() * 20,
    duration: 18 + rand() * 22,
    size: 6 + rand() * 10,
    opacity: 0.3 + rand() * 0.6,
  })), [count]);
  return (
    <div className="bg-petals" aria-hidden="true">
      {petals.map((p, i) => (
        <div key={i} className="petal" style={{
          left: p.left + '%',
          width: p.size + 'px',
          height: p.size + 'px',
          animationDelay: p.delay + 's',
          animationDuration: p.duration + 's',
          opacity: p.opacity,
        }}/>
      ))}
    </div>
  );
}

// Global symbol→logo cache populated from /tokens once at boot. Endpoints like
// /history/global/swaps don't always echo the logo field, so every section can
// fall back to this map before painting a placeholder gradient.
const TOKEN_LOGOS = {};
let _tokenLogosPromise = null;
function loadTokenLogos() {
  if (_tokenLogosPromise) return _tokenLogosPromise;
  _tokenLogosPromise = fetch('/tokens')
    .then(r => r.ok ? r.json() : null)
    .then(j => {
      const arr = Array.isArray(j) ? j : (j?.tokens || j?.data || []);
      for (const t of arr) {
        if (t?.symbol && t?.logo && !TOKEN_LOGOS[t.symbol]) {
          TOKEN_LOGOS[t.symbol] = t.logo;
        }
      }
    })
    .catch(() => {});
  return _tokenLogosPromise;
}
// Fire-and-forget at module load — front boots from index.html anyway so /tokens
// will already be warm by the time the first render asks for a logo.
loadTokenLogos();

// ---- Holders fetch with TTL cache (memory + localStorage) -----------------
// Why cache: /holders/:asset hits PG with a window function on a 200K+ row
// table. Each call costs ~800ms-2s. Result barely changes within 5 min so we
// keep a stale-while-revalidate window.
// Why localStorage: the memory Map alone resets on every F5. Persisting keeps
// the cache warm across reloads — opening Holders a second time in the same
// session is instant.
// NO background pre-warming: we only fetch the token the user is actively
// viewing. The previous approach walked all known assets serially on mount
// which wasted bandwidth and made the VPS work on tokens nobody looked at.
const HOLDERS_TTL_MS = 5 * 60 * 1000;
const HOLDERS_LS_KEY = 'sm.holdersCache.v1';
const _holdersMem = new Map();  // key "<asset>|<page>|<limit>" -> { ts, data }

function _holdersKey(asset, page, limit) { return asset + '|' + page + '|' + limit; }

// Hydrate from localStorage at module load. Drops stale entries immediately
// so we don't serve data older than TTL after a long session pause.
(function _hydrateHoldersCache() {
  try {
    const raw = localStorage.getItem(HOLDERS_LS_KEY);
    if (!raw) return;
    const obj = JSON.parse(raw);
    const now = Date.now();
    for (const [k, entry] of Object.entries(obj || {})) {
      if (entry && entry.ts && (now - entry.ts) < HOLDERS_TTL_MS) {
        _holdersMem.set(k, entry);
      }
    }
  } catch {}
})();

function _persistHoldersCache() {
  try {
    const out = {};
    for (const [k, v] of _holdersMem.entries()) out[k] = v;
    localStorage.setItem(HOLDERS_LS_KEY, JSON.stringify(out));
  } catch {}
}

function _fetchHoldersDirect(asset, page, limit) {
  const url = '/holders/' + encodeURIComponent(asset) + '?page=' + page + '&limit=' + limit;
  return fetch(url).then(r => r.ok ? r.json() : null).catch(() => null);
}

// In-flight map: dedupe concurrent requests for the same (asset, page, limit).
// Without this, an interactive user clicking pagination + the background
// refresher + the section mount effect could all race into the same URL.
const _holdersInflight = new Map();

// Public API: returns cached data immediately if fresh. Otherwise fires the
// request directly (no global serial chain) while deduping concurrent calls
// for the same key. The previous implementation queued EVERY call behind a
// single promise chain, so the first load of the Holders section had to wait
// for the chain to drain — noticeable on first visit. Users click one tab at
// a time; the right unit of serialisation is per-key, not global.
function getHoldersCached(asset, page = 1, limit = 25) {
  const key = _holdersKey(asset, page, limit);
  const hit = _holdersMem.get(key);
  const now = Date.now();
  if (hit && (now - hit.ts) < HOLDERS_TTL_MS) {
    return Promise.resolve({ data: hit.data, cached: true, ts: hit.ts });
  }
  const pending = _holdersInflight.get(key);
  if (pending) return pending;
  const p = _fetchHoldersDirect(asset, page, limit).then(data => {
    if (data) {
      _holdersMem.set(key, { ts: Date.now(), data });
      _persistHoldersCache();
    }
    _holdersInflight.delete(key);
    return { data, cached: false, ts: Date.now() };
  }, (err) => { _holdersInflight.delete(key); throw err; });
  _holdersInflight.set(key, p);
  return p;
}

// Background refresher: walks a list of assets in order, refreshing each
// entry's page 1 if the cache is stale. Designed to run on an interval.
function _refreshHoldersInSeries(assetIds, page = 1, limit = 25) {
  return assetIds.reduce((chain, asset) => chain.then(async () => {
    // Skip refetch when a fresh entry already exists.
    const key = _holdersKey(asset, page, limit);
    const hit = _holdersMem.get(key);
    if (hit && (Date.now() - hit.ts) < HOLDERS_TTL_MS) return;
    const data = await _fetchHoldersDirect(asset, page, limit);
    if (data) _holdersMem.set(key, { ts: Date.now(), data });
    // Small gap between requests so we don't back-to-back the DB.
    await new Promise(r => setTimeout(r, 250));
  }), Promise.resolve());
}

// Called once by the Holders section with the known asset IDs. Returns a
// disposer that cancels the interval when the section unmounts.
function startHoldersBackgroundRefresh(assetIds) {
  if (!Array.isArray(assetIds) || assetIds.length === 0) return () => {};
  let cancelled = false;
  const loop = async () => {
    if (cancelled) return;
    try { await _refreshHoldersInSeries(assetIds); } catch {}
  };
  loop(); // kick off immediately
  const id = setInterval(loop, HOLDERS_TTL_MS);  // then every 5 min
  return () => { cancelled = true; clearInterval(id); };
}

Object.assign(window, {
  fmt, seededRand, sparkPath, areaPath, TOKENS, TOKEN_LOGOS, loadTokenLogos,
  FAKE_ADDRS, IDENTITIES, I, Petals, useHistory, parseHistTime,
  getHoldersCached, startHoldersBackgroundRefresh, HOLDERS_TTL_MS,
});
