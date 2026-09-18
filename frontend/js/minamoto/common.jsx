/* global window, fetch */
// ============================================================
// js/minamoto/common.jsx
// Shared helpers + the api() fetch wrapper for /api/minamoto.
// Everything attaches to window.MN_* to keep modules decoupled.
// ============================================================

window.MN = window.MN || {};

(function (MN) {
  const cfg = window.__MN_CONFIG__ || { apiBase: '/api/minamoto' };

  // -----------------------------------------------------------
  // API fetch wrapper
  // -----------------------------------------------------------
  async function api(path, opts = {}) {
    const url = cfg.apiBase + path;
    const res = await fetch(url, {
      method: 'GET',
      cache: 'no-store',
      ...opts,
    });
    if (!res.ok) {
      const text = await res.text().catch(() => '');
      const err = new Error(`API ${res.status} ${path}: ${text.slice(0, 160)}`);
      err.status = res.status;
      throw err;
    }
    const ct = res.headers.get('content-type') || '';
    if (ct.includes('application/json')) return res.json();
    return res.text();
  }

  // -----------------------------------------------------------
  // Formatters
  // -----------------------------------------------------------
  function fmtInt(n) {
    const x = Number(n);
    if (!Number.isFinite(x)) return '—';
    return x.toLocaleString('en-US');
  }

  function fmtCompact(n) {
    const x = Number(n);
    if (!Number.isFinite(x)) return '—';
    if (x >= 1_000_000_000) return (x / 1_000_000_000).toFixed(2) + 'B';
    if (x >= 1_000_000) return (x / 1_000_000).toFixed(2) + 'M';
    if (x >= 1_000) return (x / 1_000).toFixed(2) + 'k';
    return String(x);
  }

  // -----------------------------------------------------------
  // Decimal scaling for raw on-chain quantities.
  //
  // Iroha 3 stores asset balances as `Numeric` (arbitrary-scale rational),
  // and `spec.scale` in `/v1/assets/definitions` is null on Minamoto today —
  // there is NO globally-enforced decimal count. However, assets bridged
  // from SORA v2 (notably XOR) keep their v2 raw scaling (18 decimals).
  // Without indexing scale per asset, we use a small hard-coded registry
  // for well-known assets + return null for unknowns so the frontend can
  // label the value as "raw" rather than mis-scale it.
  //
  // When Iroha 3 starts populating `spec.scale`, swap this for an indexed
  // column in `mn.asset_definitions`.
  // -----------------------------------------------------------
  // Iroha 3 / Minamoto storage convention (verified 2026-04-29):
  // ─────────────────────────────────────────────────────────────
  // Iroha 3 uses arbitrary-precision Numeric and stores ASSET BALANCES
  // ALREADY IN USER UNITS (e.g. "24.7664" = 24.7664 XOR), not 1e18-scaled
  // raw like SORA v2 substrate. Confirmed empirically:
  //   - Torii /v1/accounts/.../assets returns quantity "24.7664" directly
  //   - mn.instructions Mint payloads carry amounts like "5", "0.745", "20"
  //   - Wallet sums (5 genesis + 20 bridge − 0.23 fees) match the live balance
  // The bridge from v2 → Minamoto converts v2's raw 1e18 to Minamoto's
  // human-readable Numeric at mint time — we never see 1e18 amounts on this
  // chain.
  //
  // Therefore: NO decimal scaling is applied to any Minamoto asset. We
  // return null (= no scaling) for everything. If a future asset registers
  // with `spec.scale != null` and we see 1e18-style raw amounts, swap to
  // an asset_definitions.scale lookup.
  function assetDecimals(_asset) {
    return null;
  }

  // Scale a raw integer-string by `decimals` and render compact.
  // `decimals = null` falls back to compact-raw (with no division), so the
  // caller can decide separately whether to label "raw" in the UI.
  function fmtTokenAmount(rawAmount, decimals) {
    if (rawAmount == null || rawAmount === '') return '—';
    const s = String(rawAmount);
    if (decimals == null) return fmtCompact(s);
    if (!/^[0-9]+$/.test(s)) return s.slice(0, 12);
    if (s.length <= decimals) {
      const padded = s.padStart(decimals + 1, '0');
      const whole  = padded.slice(0, padded.length - decimals);
      const frac   = padded.slice(padded.length - decimals).replace(/0+$/, '').slice(0, 4);
      return whole + (frac ? '.' + frac : '');
    }
    const whole = s.slice(0, s.length - decimals);
    const fracRaw = s.slice(s.length - decimals);
    const fracTrim = fracRaw.replace(/0+$/, '').slice(0, 4);
    const wholeNum = Number(whole);
    if (!Number.isFinite(wholeNum)) {
      // Whole part too large for Number — fall back to grouped string.
      return whole + (fracTrim ? '.' + fracTrim : '');
    }
    if (wholeNum >= 1_000_000) return (wholeNum / 1_000_000).toFixed(2) + 'M' + (fracTrim ? '' : '');
    if (wholeNum >= 1_000)     return (wholeNum / 1_000).toFixed(2) + 'k';
    return wholeNum.toLocaleString('en-US') + (fracTrim ? '.' + fracTrim : '');
  }

  function fmtMs(ms) {
    const x = Number(ms);
    if (!Number.isFinite(x)) return '—';
    if (x < 1000) return x.toFixed(0) + ' ms';
    if (x < 60_000) return (x / 1000).toFixed(2) + ' s';
    if (x < 3_600_000) return (x / 60_000).toFixed(1) + ' min';
    return (x / 3_600_000).toFixed(2) + ' h';
  }

  function fmtRelative(iso) {
    if (!iso) return '—';
    const t = typeof iso === 'string' ? Date.parse(iso) : Number(iso);
    if (!Number.isFinite(t)) return '—';
    const sec = Math.max(0, Math.round((Date.now() - t) / 1000));
    if (sec < 60) return sec + 's ago';
    if (sec < 3600) return Math.round(sec / 60) + 'min ago';
    if (sec < 86_400) return Math.round(sec / 3600) + 'h ago';
    return Math.round(sec / 86_400) + 'd ago';
  }

  function shortHash(hex, head = 6, tail = 4) {
    if (!hex) return '';
    const s = String(hex);
    if (s.length <= head + tail + 1) return s;
    return s.slice(0, head) + '…' + s.slice(-tail);
  }

  // Standard head + tail abbreviation for I105 account ids. The katakana
  // discriminant character (U+FF65–U+FF9F) renders correctly thanks to the
  // Noto Sans JP fallback loaded in minamoto.html, so we keep the canonical
  // prefix visible — `sorauﾛ1Pﾇ…TCW2PV` — instead of hiding it.
  function shortAccount(id, head = 8, tail = 6) {
    if (!id) return '';
    const s = String(id);
    if (s.length <= head + tail + 1) return s;
    return s.slice(0, head) + '…' + s.slice(-tail);
  }

  function copyToClipboard(text) {
    if (!text) return Promise.resolve(false);
    if (navigator.clipboard && navigator.clipboard.writeText) {
      return navigator.clipboard.writeText(String(text)).then(() => true, () => false);
    }
    // Fallback: textarea trick
    try {
      const ta = document.createElement('textarea');
      ta.value = String(text);
      ta.style.position = 'fixed'; ta.style.opacity = '0';
      document.body.appendChild(ta); ta.select();
      const ok = document.execCommand('copy');
      document.body.removeChild(ta);
      return Promise.resolve(ok);
    } catch (_) { return Promise.resolve(false); }
  }

  // -----------------------------------------------------------
  // useInterval — runs `cb` every `delay` ms, mounted-only
  // -----------------------------------------------------------
  function useInterval(cb, delay) {
    const ref = React.useRef(cb);
    React.useEffect(() => { ref.current = cb; }, [cb]);
    React.useEffect(() => {
      if (delay == null || delay <= 0) return;
      const id = setInterval(() => ref.current(), delay);
      return () => clearInterval(id);
    }, [delay]);
  }

  // -----------------------------------------------------------
  // useFetch — { data, error, loading, reload } for a path
  // -----------------------------------------------------------
  function useFetch(path, intervalMs = 0) {
    const [state, setState] = React.useState({ data: null, error: null, loading: true });
    const reload = React.useCallback(() => {
      setState(s => ({ ...s, loading: true }));
      api(path)
        .then(data => setState({ data, error: null, loading: false }))
        .catch(error => setState(s => ({ data: s.data, error, loading: false })));
    }, [path]);
    React.useEffect(() => { reload(); }, [reload]);
    useInterval(reload, intervalMs);
    return { ...state, reload };
  }

  // -----------------------------------------------------------
  // Token logo lookup. Iroha 3 lets asset definitions carry a `logo`
  // field but it's null in genesis. We bridge to the v2 polkaswap-
  // token-logos repo for assets that exist on both networks (XOR is
  // the obvious one — same brand, same canonical mark). Falls back
  // to a colored chip when no logo is known.
  // -----------------------------------------------------------
  // Inline data URIs from the v2 SoraMetrics /tokens endpoint. github raw
  // URLs return 404; v2 already encodes the official Soramitsu mark inline,
  // so we copy that and avoid one network hop per cell.
  const XOR_LOGO_DATA_URI = "data:image/svg+xml;charset=utf8,%3Csvg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 22 22' %3E%3Cpath fill='%23E3232C' d='M22,11c0,6.1-4.9,11-11,11S0,17.1,0,11S4.9,0,11,0S22,4.9,22,11z'/%3E%3Cpath fill='%23FFFFFF' d='M5.8,20.7c1.7-2.6,3.5-5.2,5.3-7.8l5.2,7.8c0.3-0.1,0.5-0.3,0.8-0.5s0.5-0.3,0.7-0.5 c-1.9-2.9-3.9-5.8-5.8-8.7h5.8V9.2H12V7.3h5.8V5.5H4.3v1.8h5.8v1.9H4.3V11h5.8l-5.8,8.7C4.5,19.9,4.7,20,5,20.2 C5.3,20.4,5.5,20.6,5.8,20.7z'/%3E%3C/svg%3E";
  const KNOWN_BY_ALIAS = {
    'xor#universal': XOR_LOGO_DATA_URI,
  };
  const KNOWN_BY_NAME_LOWER = {
    'xor': XOR_LOGO_DATA_URI,
  };
  function tokenLogo(asset) {
    if (!asset) return null;
    const a = (asset.alias || asset.asset_alias || '').toLowerCase();
    if (KNOWN_BY_ALIAS[a]) return KNOWN_BY_ALIAS[a];
    const n = (asset.name || asset.asset_name || '').toLowerCase();
    if (KNOWN_BY_NAME_LOWER[n]) return KNOWN_BY_NAME_LOWER[n];
    return null;
  }

  // Tiny presentational component: <MN.TokenIcon asset={...} size={28}/>
  // Renders the logo when known, otherwise a colored chip with the first
  // letters of the asset name.
  function TokenIcon({ asset, size = 28 }) {
    const url = tokenLogo(asset);
    const base = {
      width: size, height: size, borderRadius: '50%',
      flexShrink: 0, display: 'inline-block', verticalAlign: 'middle',
    };
    if (url) return React.createElement('img', { src: url, alt: asset.name || asset.asset_name || '', style: { ...base, objectFit: 'cover' } });
    const label = String(asset.name || asset.asset_name || asset.alias || '?').slice(0, 3).toUpperCase();
    const isXor = (asset.alias || asset.asset_alias) === 'xor#universal' || (asset.name || asset.asset_name || '').toLowerCase() === 'xor';
    return React.createElement('span', {
      style: {
        ...base,
        background: isXor ? 'linear-gradient(135deg,#7B2D5B,#7B5B90)' : 'var(--bg-3)',
        display: 'grid', placeItems: 'center',
        color: 'white', fontWeight: 800, fontSize: Math.round(size * 0.36),
      }
    }, label);
  }

  MN.api = api;
  MN.fmt = { int: fmtInt, compact: fmtCompact, ms: fmtMs, relative: fmtRelative, tokenAmount: fmtTokenAmount };
  MN.assetDecimals = assetDecimals;
  MN.shortHash = shortHash;
  MN.shortAccount = shortAccount;
  MN.copy = copyToClipboard;
  MN.useInterval = useInterval;
  MN.useFetch = useFetch;
  MN.cfg = cfg;
  MN.tokenLogo = tokenLogo;
  MN.TokenIcon = TokenIcon;
})(window.MN);
