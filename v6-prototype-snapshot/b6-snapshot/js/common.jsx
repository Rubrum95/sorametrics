/* global React, ReactDOM */
const { useState, useEffect, useRef, useMemo, useCallback } = React;

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

Object.assign(window, { fmt, seededRand, sparkPath, areaPath, TOKENS, FAKE_ADDRS, IDENTITIES, I, Petals });
