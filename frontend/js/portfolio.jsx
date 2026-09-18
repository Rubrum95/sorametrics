/* global React, fmt, TOKENS, sparkPath, useT, ExportCsvButton, AddWalletModal, TokenLogo, useWallets, useToast */
const { useState, useEffect, useMemo, useRef } = React;

// Wallet backup / restore buttons for Portfolio. Writes to localStorage via
// the globally-exposed downloadBackup / restoreBackup helpers (features.jsx).
// Restore auto-detects v1-format backups and plain-text address lists, so
// users coming from sorametrics.org v1 (or with a simple .txt of addresses)
// can drop their file and keep their wallets.
function WalletBackupControls() {
  const t = (typeof window.useT === 'function' ? window.useT() : (k) => k);
  const toast = (typeof window.useToast === 'function' ? window.useToast() : null);
  const fileRef = useRef(null);
  const onBackup = () => {
    if (typeof window.downloadBackup !== 'function') return;
    window.downloadBackup({});
    toast?.push?.(t('portfolio.backupOk'), 'ok');
  };
  const onRestorePick = async (e) => {
    const f = e.target.files?.[0];
    if (!f) return;
    if (!confirm(t('portfolio.restoreConfirm'))) { e.target.value = ''; return; }
    try {
      await window.restoreBackup(f);
      toast?.push?.(t('portfolio.restoreOk'), 'ok');
      setTimeout(() => window.location.reload(), 600);
    } catch {
      toast?.push?.(t('portfolio.restoreErr'), 'err');
    }
    e.target.value = '';
  };
  return (
    <>
      <button className="btn" onClick={onBackup} title={t('portfolio.backupTip')}>
        ↓ {t('portfolio.backup')}
      </button>
      <button className="btn" onClick={() => fileRef.current?.click()} title={t('portfolio.restoreTip')}>
        ↑ {t('portfolio.restore')}
      </button>
      {/* Accept JSON and plain-text lists — the restoreBackup parser detects
          both automatically. */}
      <input type="file" ref={fileRef} accept=".json,.txt,application/json,text/plain"
             style={{display:'none'}} onChange={onRestorePick}/>
    </>
  );
}

// --- Visual: donut chart for token allocation -------------------------------
function Donut({ slices }) {
  const R = 78, r = 54, cx = 90, cy = 90;
  let acc = 0;
  const total = slices.reduce((s, x) => s + x.value, 0) || 1;
  return (
    <svg className="donut-svg" viewBox="0 0 180 180">
      <circle cx={cx} cy={cy} r={R} fill="none" stroke="rgba(255,255,255,0.05)" strokeWidth={R - r}/>
      {slices.map((s, i) => {
        const frac = s.value / total;
        if (!Number.isFinite(frac) || frac <= 0) return null;
        const startA = acc * 2 * Math.PI - Math.PI/2;
        acc += frac;
        const endA = acc * 2 * Math.PI - Math.PI/2;
        const large = frac > 0.5 ? 1 : 0;
        const x1 = cx + R * Math.cos(startA), y1 = cy + R * Math.sin(startA);
        const x2 = cx + R * Math.cos(endA),   y2 = cy + R * Math.sin(endA);
        const x3 = cx + r * Math.cos(endA),   y3 = cy + r * Math.sin(endA);
        const x4 = cx + r * Math.cos(startA), y4 = cy + r * Math.sin(startA);
        const d = `M ${x1} ${y1} A ${R} ${R} 0 ${large} 1 ${x2} ${y2} L ${x3} ${y3} A ${r} ${r} 0 ${large} 0 ${x4} ${y4} Z`;
        return <path key={i} d={d} fill={s.color} opacity="0.92" stroke="#111" strokeWidth="0.6"/>;
      })}
      <text x={cx} y={cy-4} textAnchor="middle" fill="#e5e7eb" fontSize="10" fontFamily="Inter" letterSpacing="1.5" fontWeight="700">NET WORTH</text>
      <text x={cx} y={cy+14} textAnchor="middle" fill="#fff" fontSize="15" fontFamily="JetBrains Mono" fontWeight="800">
        {fmt.usd(total)}
      </text>
    </svg>
  );
}

// Mini sparkline driven by the /tokens endpoint's `sparkline` field when present.
function MiniSpark({ data, color, w = 64, h = 24 }) {
  if (!Array.isArray(data) || data.length < 2) {
    return <span className="muted tiny">—</span>;
  }
  return (
    <svg viewBox={`0 0 ${w} ${h}`} width={w} height={h}>
      <path d={sparkPath(data, w, h, 2)} stroke={color} strokeWidth="1.4" fill="none"/>
    </svg>
  );
}

/* =========================================================================
   PortfolioSection — now driven entirely by real data:
   · Wallets from WalletProvider (user's added accounts)
   · Per-wallet balances from /balance/:addr
   · Per-wallet LP from /wallet/liquidity + staking from /wallet/staking
   · Price/24h/sparkline from /tokens
   · FX rates from /currency-rates
   ========================================================================= */
function PortfolioSection({ tweaks }) {
  const t = useT();
  const store = useWallets();
  const wallets = store?.wallets || [];
  const [cur, setCur] = useState(() => {
    try { return localStorage.getItem('sm.currency') || 'USD'; } catch { return 'USD'; }
  });
  useEffect(() => { try { localStorage.setItem('sm.currency', cur); } catch {} }, [cur]);

  // FX rates from /currency-rates (same source as Balance).
  const [fxRates, setFxRates] = useState({ USD: 1, EUR: 0.92, XOR: 1 });
  useEffect(() => {
    fetch('/currency-rates').then(r => r.ok ? r.json() : null).then(j => {
      if (j && Number(j.EUR)) setFxRates(prev => ({ ...prev, EUR: Number(j.EUR) }));
    }).catch(() => {});
  }, []);
  // Derive XOR rate from any wallet's XOR holding (usd/amt). Same shortcut
  // used in BalanceSection — avoids an extra "XOR price" endpoint.
  useEffect(() => {
    for (const w of wallets) {
      const xor = (w.tokens || []).find(tk => tk.symbol === 'XOR' && Number(tk.amount) > 0);
      if (xor) {
        const rate = Number(xor.usdValue) / Number(xor.amount);
        if (Number.isFinite(rate) && rate > 0) {
          setFxRates(prev => prev.XOR === 1/rate ? prev : ({ ...prev, XOR: 1/rate }));
          break;
        }
      }
    }
  }, [wallets]);

  // Price / 24h / sparkline map from /tokens — keyed by symbol.
  const [tokenMeta, setTokenMeta] = useState({});
  useEffect(() => {
    let cancelled = false;
    fetch('/tokens?timeframe=24h&sparkline=1&limit=100')
      .then(r => r.ok ? r.json() : null)
      .then(j => {
        if (cancelled) return;
        const arr = Array.isArray(j) ? j : (j?.tokens || j?.data || []);
        const map = {};
        for (const tk of arr) {
          if (!tk?.symbol) continue;
          map[tk.symbol] = {
            price: Number(tk.price) || 0,
            change: Number(tk.change24h ?? tk.change ?? tk.delta24h) || 0,
            sparkline: Array.isArray(tk.sparkline) ? tk.sparkline : null,
            logo: tk.logo,
            name: tk.name,
          };
        }
        setTokenMeta(map);
      })
      .catch(() => {});
    return () => { cancelled = true; };
  }, []);

  // LP + staking per-wallet aggregate (same pattern used in Balance).
  const [lpSummary, setLpSummary] = useState({
    lpUsd: 0, stakingUsd: 0, positions: [], stakingByWallet: {}, loading: false,
  });
  useEffect(() => {
    if (wallets.length === 0) {
      setLpSummary({ lpUsd: 0, stakingUsd: 0, positions: [], stakingByWallet: {}, loading: false });
      return;
    }
    let cancelled = false;
    setLpSummary(s => ({ ...s, loading: true }));
    (async () => {
      let lpUsd = 0, stakingUsd = 0;
      const positions = [];
      const stakingByWallet = {};
      await Promise.all(wallets.map(async w => {
        try {
          const [lp, st] = await Promise.all([
            fetch('/wallet/liquidity/' + encodeURIComponent(w.addr)).then(r => r.ok ? r.json() : null).catch(() => null),
            fetch('/wallet/staking/' + encodeURIComponent(w.addr)).then(r => r.ok ? r.json() : null).catch(() => null),
          ]);
          // Prod /wallet/liquidity returns either {pools:[…]} or a flat array
          // of {base,target,amountBase,amountTarget,value,share,…}. Handle both.
          const pools = Array.isArray(lp) ? lp : (lp?.pools || []);
          for (const p of pools) {
            const usd = Number(p.usdValue || p.value) || 0;
            lpUsd += usd;
            if (usd > 0) {
              positions.push({
                wallet: w.alias,
                pair: (p.base?.symbol || p.baseSymbol || '?') + '/' + (p.target?.symbol || p.targetSymbol || '?'),
                base: p.base?.symbol || p.baseSymbol,
                target: p.target?.symbol || p.targetSymbol,
                amountBase: Number(p.amountBase || 0),
                amountTarget: Number(p.amountTarget || 0),
                usd,
                share: Number(p.share) || 0,
              });
            }
          }
          const stUsd = Number(st?.totalStakedUsd) || 0;
          stakingUsd += stUsd;
          if (stUsd > 0 || (Array.isArray(st?.nominations) && st.nominations.length > 0)) {
            stakingByWallet[w.addr] = {
              wallet: w.alias,
              usd: stUsd,
              validators: Array.isArray(st?.nominations) ? st.nominations.length : 0,
              rewards: Number(st?.rewards) || 0,
            };
          }
        } catch {}
      }));
      if (!cancelled) setLpSummary({ lpUsd, stakingUsd, positions: positions.sort((a,b) => b.usd - a.usd), stakingByWallet, loading: false });
    })();
    return () => { cancelled = true; };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [wallets.map(w => w.addr).join(',')]);

  // Aggregate holdings across the user's wallets, keyed by symbol.
  const holdings = useMemo(() => {
    const bySym = {};
    wallets.forEach(w => (w.tokens || []).forEach(tk => {
      const sym = tk.symbol;
      if (!sym) return;
      if (!bySym[sym]) bySym[sym] = { sym, amount: 0, usdValue: 0, logo: tk.logo };
      bySym[sym].amount += Number(tk.amount) || 0;
      bySym[sym].usdValue += Number(tk.usdValue) || 0;
    }));
    // Enrich with price/sparkline/change from /tokens.
    return Object.values(bySym).map(h => {
      const meta = tokenMeta[h.sym] || {};
      const price = meta.price || (h.amount > 0 ? h.usdValue / h.amount : 0);
      return { ...h, price, change: meta.change || 0, sparkline: meta.sparkline, name: meta.name };
    }).sort((a, b) => b.usdValue - a.usdValue);
  }, [wallets, tokenMeta]);

  const tokensNet = holdings.reduce((s, h) => s + h.usdValue, 0);
  const net = tokensNet + (lpSummary.lpUsd || 0) + (lpSummary.stakingUsd || 0);

  // Currency converter respecting the selected display currency.
  const fmtCur = (usd) => {
    const rate = fxRates[cur] || 1;
    const val = (Number(usd) || 0) * rate;
    if (cur === 'USD') return '$' + val.toLocaleString(undefined, { maximumFractionDigits: 2 });
    if (cur === 'EUR') return '€' + val.toLocaleString(undefined, { maximumFractionDigits: 2 });
    return val.toLocaleString(undefined, { maximumFractionDigits: 4 }) + ' XOR';
  };

  // Per-wallet totals (tokens + LP share by wallet + staking by wallet).
  const walletTotals = useMemo(() => {
    return wallets.map(w => {
      const tokUsd = (w.tokens || []).reduce((s, tk) => s + (Number(tk.usdValue) || 0), 0);
      const lpUsd = lpSummary.positions
        .filter(p => p.wallet === w.alias)
        .reduce((s, p) => s + p.usd, 0);
      const stUsd = lpSummary.stakingByWallet[w.addr]?.usd || 0;
      return { ...w, tokUsd, lpUsd, stUsd, total: tokUsd + lpUsd + stUsd };
    }).sort((a, b) => b.total - a.total);
  }, [wallets, lpSummary]);

  const palette = ['#E5243B','#F5B041','#EC4899','#8B7FD9','#60A5FA','#10B981','#FDE68A','#7B5B90','#94A3B8'];
  const slices = holdings.slice(0, 8).map((h, i) => ({ value: h.usdValue, color: palette[i] || '#94A3B8' }));
  const rest = holdings.slice(8).reduce((s, h) => s + h.usdValue, 0);
  if (rest > 0) slices.push({ value: rest, color: '#4A3566' });

  const [addOpen, setAddOpen] = useState(false);
  // Collapse the per-wallet cards by default, regardless of how many the user
  // has. The hero card stays compact; if they want to see the list they tap
  // the header. Their choice is persisted to localStorage so subsequent loads
  // respect it, but the first-ever visit is always collapsed.
  const [walletsExpanded, setWalletsExpanded] = useState(() => {
    try {
      const stored = localStorage.getItem('sm.portfolio.walletsExpanded');
      if (stored !== null) return stored === '1';
    } catch {}
    return false; // always collapsed on first visit
  });
  useEffect(() => {
    try { localStorage.setItem('sm.portfolio.walletsExpanded', walletsExpanded ? '1' : '0'); } catch {}
  }, [walletsExpanded]);

  // Match v1's "Ocultar saldos bajos (≤$0.05)" toggle. Tokens with 0 or dust
  // values hidden from the Holdings table when active. Net worth / allocation
  // donut stay unchanged — this is purely a visual filter for the table, so
  // totals remain truthful.
  const LOW_BALANCE_THRESHOLD_USD = 0.05;
  const [hideLow, setHideLow] = useState(() => {
    try { return localStorage.getItem('sm.hideLowBalances') === '1'; } catch { return false; }
  });
  useEffect(() => {
    try { localStorage.setItem('sm.hideLowBalances', hideLow ? '1' : '0'); } catch {}
  }, [hideLow]);
  const visibleHoldings = useMemo(
    () => hideLow ? holdings.filter(h => (h.usdValue || 0) > LOW_BALANCE_THRESHOLD_USD) : holdings,
    [holdings, hideLow]
  );

  if (wallets.length === 0) {
    return (
      <div>
        <PageHeader title={t('portfolio.title')} sub={t('portfolio.sub')}>
          <WalletBackupControls/>
          <button className="btn primary" onClick={() => setAddOpen(true)}>{t('portfolio.addWallet')}</button>
        </PageHeader>
        <div className="card" style={{padding:40, textAlign:'center', marginTop:20}}>
          <div style={{fontSize:16, fontWeight:600, marginBottom:8}}>{t('portfolio.noWalletsYet')}</div>
          <div className="muted tiny" style={{marginBottom:20}}>
            {t('portfolio.emptyHint')}
          </div>
          <div style={{display:'flex', gap:8, justifyContent:'center', flexWrap:'wrap'}}>
            <button className="btn primary" onClick={() => setAddOpen(true)}>{t('portfolio.addWallet')}</button>
            <WalletBackupControls/>
          </div>
        </div>
        {addOpen && <AddWalletModal open={addOpen} onClose={() => setAddOpen(false)}/>}
      </div>
    );
  }

  return (
    <div>
      <PageHeader title={t('portfolio.title')} sub={t('portfolio.sub')}>
        <WalletBackupControls/>
        <button className="btn" onClick={() => setAddOpen(true)}>{t('portfolio.addWallet')}</button>
        <ExportCsvButton section="portfolio"
          headers={['Alias','Address','TokensUsd','LpUsd','StakingUsd','TotalUsd']}
          rows={walletTotals.map(w => ({
            Alias: w.alias, Address: w.addr,
            TokensUsd: w.tokUsd.toFixed(2), LpUsd: w.lpUsd.toFixed(2),
            StakingUsd: w.stUsd.toFixed(2), TotalUsd: w.total.toFixed(2),
          }))}
          className="primary"/>
      </PageHeader>

      <div className="pf-hero">
        {/* Net worth + wallet cards */}
        <div className="card pf-worth-card">
          <div className="pf-worth-label">Total Net Worth</div>
          <div className="pf-worth-value num">
            <span className="cur">{cur === 'USD' ? '$' : cur === 'EUR' ? '€' : ''}</span>
            {fmtCur(net).replace(/^[$€]/, '').replace(/\s*XOR$/, '')}
            {cur === 'XOR' && <span className="cur" style={{fontSize:'0.55em'}}> XOR</span>}
          </div>
          <div className="pf-worth-delta">
            <span className="stat-sub">
              {wallets.length} wallet{wallets.length === 1 ? '' : 's'} · {holdings.length} tokens
              {(lpSummary.lpUsd + lpSummary.stakingUsd) > 0 && (
                <> · tokens {fmtCur(tokensNet)} + LP {fmtCur(lpSummary.lpUsd)} + staking {fmtCur(lpSummary.stakingUsd)}</>
              )}
            </span>
          </div>

          <div className="pf-curs">
            {['USD', 'EUR', 'XOR'].map(c => (
              <button key={c}
                className={'pf-cur-btn' + (c === cur ? ' active' : '')}
                onClick={() => setCur(c)}>{c}</button>
            ))}
          </div>

          {/* Toggle header + collapsible wallets grid. Button always present
              (even with a small list) so the user can hide the grid anytime;
              default is collapsed when > 4 wallets to keep the hero card
              readable. Clicking the whole row toggles. */}
          {walletTotals.length > 0 && (
            <div
              className="pf-wallets-toggle"
              onClick={() => setWalletsExpanded(v => !v)}
              style={{
                display:'flex', alignItems:'center', justifyContent:'space-between',
                padding:'10px 4px', marginTop:14, marginBottom: walletsExpanded ? 8 : 0,
                borderTop:'1px solid var(--border)',
                cursor:'pointer', userSelect:'none',
              }}
              title={walletsExpanded ? (t('portfolio.hideWallets', 'Ocultar') + ' (' + walletTotals.length + ')') : (t('portfolio.showWallets', 'Mostrar') + ' (' + walletTotals.length + ')')}
            >
              <div className="muted tiny" style={{textTransform:'uppercase', letterSpacing:'0.08em', fontWeight:600}}>
                {walletsExpanded
                  ? t('portfolio.hideWallets', 'Ocultar wallets')
                  : t('portfolio.showWallets', 'Mostrar wallets')}
                <span style={{marginLeft:8, opacity:0.7}}>· {walletTotals.length}</span>
              </div>
              <span style={{fontSize:18, lineHeight:1, color:'var(--fg-1)', transition:'transform 180ms', transform: walletsExpanded ? 'rotate(180deg)' : 'none', fontWeight:700}}>▾</span>
            </div>
          )}

          {walletsExpanded && (
            <div className="pf-wallets">
              {walletTotals.map(w => {
                const pct = net > 0 ? (w.total / net) * 100 : 0;
                return (
                  <div
                    key={w.id || w.addr}
                    className="pf-wallet-card clickable"
                    style={{cursor:'pointer'}}
                    onClick={() => window.openWalletDetails?.(w.addr, w.alias)}
                    title="Abrir detalle de la wallet">
                    <div className="pf-wallet-head">
                      <div className="pf-wallet-av">{w.alias ? w.alias[0] : '?'}</div>
                      <div style={{flex:1, minWidth:0}}>
                        <div className="pf-wallet-name">{w.alias}</div>
                        <div className="pf-wallet-addr">{fmt.addr(w.addr, 5, 4)}</div>
                      </div>
                    </div>
                    <div className="pf-wallet-value num">{fmtCur(w.total)}</div>
                    <div className="stat-sub" style={{fontSize:11}}>
                      {pct.toFixed(1)}% of total
                      {w.lpUsd > 0 && <> · LP {fmtCur(w.lpUsd)}</>}
                      {w.stUsd > 0 && <> · stake {fmtCur(w.stUsd)}</>}
                    </div>
                  </div>
                );
              })}
            </div>
          )}
        </div>

        {/* Donut allocation */}
        <div className="card">
          <div className="card-header">
            <div className="card-title"><span className="dot"/> Allocation</div>
            <span className="tag">{holdings.length} tokens</span>
          </div>
          <div className="card-body">
            <div className="pf-donut-wrap">
              <Donut slices={slices}/>
              <div className="donut-legend">
                {holdings.slice(0, 8).map((h, i) => (
                  <div key={h.sym} className="lg-row" style={{ ['--c']: palette[i] || '#94A3B8' }}>
                    <span className="lg-dot" style={{background: palette[i] || '#94A3B8'}}/>
                    <span className="lg-sym">{h.sym}</span>
                    <span className="lg-val">{fmtCur(h.usdValue)}</span>
                    <span className="lg-pct">{tokensNet > 0 ? (h.usdValue / tokensNet * 100).toFixed(1) : '0'}%</span>
                  </div>
                ))}
                {rest > 0 && (
                  <div className="lg-row" style={{ ['--c']: '#4A3566' }}>
                    <span className="lg-dot" style={{background:'#4A3566'}}/>
                    <span className="lg-sym">Others</span>
                    <span className="lg-val">{fmtCur(rest)}</span>
                    <span className="lg-pct">{tokensNet > 0 ? (rest / tokensNet * 100).toFixed(1) : '0'}%</span>
                  </div>
                )}
              </div>
            </div>
          </div>
        </div>
      </div>

      {/* Holdings table */}
      <div className="card" style={{marginTop: 18}}>
        <div className="card-header" style={{gap:10, flexWrap:'wrap'}}>
          <div className="card-title"><span className="dot"/> Holdings · Live</div>
          <div className="row" style={{gap:10, flexWrap:'wrap'}}>
            {/* Match v1 "Ocultar saldos bajos (≤$0.05)" — hides tokens with
                value at or below the threshold. Total net worth / allocation
                stay untouched; this is only a visual filter for the table. */}
            <label
              style={{display:'inline-flex', alignItems:'center', gap:6, fontSize:12, color:'var(--fg-2)', cursor:'pointer', userSelect:'none'}}
              title={t('portfolio.hideLowTip', 'Oculta tokens con valor <= $0.05 de la tabla. No afecta al total ni al gráfico.')}>
              <input type="checkbox" checked={hideLow} onChange={e => setHideLow(e.target.checked)}
                     style={{cursor:'pointer', accentColor:'var(--accent, #E5243B)'}}/>
              {t('portfolio.hideLow', 'Ocultar saldos bajos')} <span className="muted tiny">(≤$0.05)</span>
            </label>
            <span className="tag ok"><span className="live-dot" style={{width:5,height:5}}/> on-chain</span>
            <span className="tag">
              {visibleHoldings.length}
              {hideLow && visibleHoldings.length !== holdings.length ? ' / ' + holdings.length : ''} tokens
            </span>
          </div>
        </div>
        <div className="card-body" style={{padding: 0}}>
          <table className="holdings-table">
            <thead>
              <tr>
                <th style={{paddingLeft: 20}}>Token</th>
                <th className="num" style={{textAlign:'right'}}>Amount</th>
                <th className="num" style={{textAlign:'right'}}>Price</th>
                <th className="num" style={{textAlign:'right'}}>24h</th>
                <th style={{textAlign:'right'}}>Chart</th>
                <th className="num" style={{textAlign:'right', paddingRight: 20}}>Value</th>
              </tr>
            </thead>
            <tbody>
              {visibleHoldings.length === 0 && hideLow && holdings.length > 0 && (
                <tr><td colSpan={6} style={{padding:24, textAlign:'center', color:'var(--fg-2)'}}>
                  {t('portfolio.allLowHidden', 'Todos los saldos están por debajo del umbral. Desactiva el filtro para verlos.')}
                </td></tr>
              )}
              {visibleHoldings.map(h => {
                const tk = TOKENS[h.sym] || {};
                const change = Number(h.change) || 0;
                return (
                  <tr key={h.sym}>
                    <td style={{paddingLeft: 20}}>
                      <div className="token-cell">
                        <TokenLogo sym={h.sym} logo={h.logo} size={28}/>
                        <div>
                          <div style={{fontWeight: 700, color: 'var(--fg-0)'}}>{h.sym}</div>
                          <div className="muted tiny">{h.name || tk.name || h.sym}</div>
                        </div>
                      </div>
                    </td>
                    <td className="num" style={{textAlign:'right'}}>{fmt.num(h.amount, h.amount > 1000 ? 0 : 4)}</td>
                    <td className="num" style={{textAlign:'right'}}>{h.price > 0 ? ('$' + (h.price < 1 ? h.price.toFixed(6) : h.price.toFixed(2))) : '—'}</td>
                    <td className="num" style={{textAlign:'right'}}>
                      <span style={{color: change >= 0 ? '#6EE7B7' : '#FCA5A5', fontWeight: 700}}>
                        {change >= 0 ? '+' : ''}{change.toFixed(2)}%
                      </span>
                    </td>
                    <td style={{textAlign:'right'}}>
                      <MiniSpark data={h.sparkline} color={change >= 0 ? '#10B981' : '#EF4444'}/>
                    </td>
                    <td className="num" style={{textAlign:'right', paddingRight: 20, fontWeight: 700, color: 'var(--fg-0)'}}>
                      {fmtCur(h.usdValue)}
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      </div>

      {/* LP & Staking */}
      <div className="card" style={{marginTop: 18}}>
        <div className="card-header">
          <div className="card-title"><span className="dot"/> LP & Staking</div>
          <span className="tag">
            LP {fmtCur(lpSummary.lpUsd)} · Staking {fmtCur(lpSummary.stakingUsd)}
          </span>
        </div>
        <div className="card-body" style={{padding: 0}}>
          {lpSummary.loading && <div className="muted tiny" style={{padding:16, textAlign:'center'}}>Cargando posiciones…</div>}
          {!lpSummary.loading && lpSummary.positions.length === 0 && Object.keys(lpSummary.stakingByWallet).length === 0 && (
            <div className="muted tiny" style={{padding:20, textAlign:'center'}}>No hay posiciones de LP ni staking en tus wallets.</div>
          )}
          {lpSummary.positions.length > 0 && (
            <div style={{padding:'4px 0 12px'}}>
              <div className="muted tiny" style={{padding:'10px 20px 6px', letterSpacing:'0.12em', textTransform:'uppercase', fontWeight:700}}>Liquidity Positions</div>
              <table className="holdings-table">
                <thead>
                  <tr>
                    <th style={{paddingLeft:20}}>Wallet</th>
                    <th>Pool</th>
                    <th className="num" style={{textAlign:'right'}}>Amount</th>
                    <th className="num" style={{textAlign:'right'}}>Share</th>
                    <th className="num" style={{textAlign:'right', paddingRight:20}}>Value</th>
                  </tr>
                </thead>
                <tbody>
                  {lpSummary.positions.map((p, i) => (
                    <tr key={i}>
                      <td style={{paddingLeft:20}} className="muted tiny">{p.wallet}</td>
                      <td>
                        <div style={{display:'flex', alignItems:'center', gap:6}}>
                          <TokenLogo sym={p.base} size={18}/>
                          <TokenLogo sym={p.target} size={18}/>
                          <span style={{fontWeight:700}}>{p.pair}</span>
                        </div>
                      </td>
                      <td className="num tiny" style={{textAlign:'right'}}>
                        <div>{fmt.num(p.amountBase, 2)} {p.base}</div>
                        <div className="muted">{fmt.num(p.amountTarget, 2)} {p.target}</div>
                      </td>
                      <td className="num" style={{textAlign:'right'}}>{(p.share * 100).toFixed(2)}%</td>
                      <td className="num" style={{textAlign:'right', paddingRight:20, fontWeight:700}}>{fmtCur(p.usd)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
          {Object.keys(lpSummary.stakingByWallet).length > 0 && (
            <div style={{padding:'4px 0 14px'}}>
              <div className="muted tiny" style={{padding:'10px 20px 6px', letterSpacing:'0.12em', textTransform:'uppercase', fontWeight:700}}>Staking</div>
              <table className="holdings-table">
                <thead>
                  <tr>
                    <th style={{paddingLeft:20}}>Wallet</th>
                    <th className="num" style={{textAlign:'right'}}>Validators</th>
                    <th className="num" style={{textAlign:'right'}}>Rewards</th>
                    <th className="num" style={{textAlign:'right', paddingRight:20}}>Staked</th>
                  </tr>
                </thead>
                <tbody>
                  {Object.entries(lpSummary.stakingByWallet).map(([addr, s]) => (
                    <tr key={addr}>
                      <td style={{paddingLeft:20}}>{s.wallet}</td>
                      <td className="num" style={{textAlign:'right'}}>{s.validators}</td>
                      <td className="num" style={{textAlign:'right'}}>{s.rewards > 0 ? fmt.num(s.rewards, 4) + ' XOR' : '—'}</td>
                      <td className="num" style={{textAlign:'right', paddingRight:20, fontWeight:700}}>{fmtCur(s.usd)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </div>
      </div>

      {/* Prediction market positions per wallet. The PolkamarktPositions
          component is feature-detected: it renders nothing if the pallet
          isn't active on-chain or the wallet has no positions, so this
          block is invisible until Polkamarkt goes live. */}
      {wallets.length > 0 && wallets.map(w => (
        window.PolkamarktPositions
          ? <window.PolkamarktPositions key={'pm-' + w.addr} addr={w.addr} title={'Polkamarkt · ' + (w.alias || fmt.addr(w.addr, 6, 4))}/>
          : null
      ))}

      {addOpen && <AddWalletModal open={addOpen} onClose={() => setAddOpen(false)}/>}
    </div>
  );
}

Object.assign(window, { PortfolioSection });
