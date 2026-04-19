/* global React, TOKENS, fmt, FAKE_ADDRS, IDENTITIES, areaPath, sparkPath, I, useT, getPulseSocket */
const { useState, useEffect, useRef, useMemo } = React;

// Fetch helper — always returns parsed JSON or null (never throws into render).
async function fetchJson(url) {
  try {
    const r = await fetch(url);
    if (!r.ok) return null;
    return await r.json();
  } catch {
    return null;
  }
}

function Furnace({ token, liveSpeed, motion }) {
  const ref = useRef(null);
  useEffect(() => {
    if (motion === 'none' || !ref.current) return;
    const interval = setInterval(() => {
      const el = document.createElement('div');
      el.className = 'ember';
      const left = 50 + (Math.random() - 0.5) * 24;
      const dx = (Math.random() - 0.5) * 120;
      const dur = 1.6 + Math.random() * 1.8;
      el.style.left = left + '%';
      el.style.setProperty('--dx', dx + 'px');
      el.style.setProperty('--dur', dur + 's');
      ref.current && ref.current.appendChild(el);
      setTimeout(() => el.remove(), dur * 1000);
    }, 260 / (liveSpeed || 1));
    return () => clearInterval(interval);
  }, [liveSpeed, motion]);

  const tk = TOKENS[token];
  return (
    <div className="furnace-wrap" ref={ref} style={{ ['--tok-color']: tk.color, ['--tok-glow']: tk.glow }}>
      <svg className="fee-flow-svg" viewBox="0 0 600 280" preserveAspectRatio="xMidYMid meet">
        <defs>
          <linearGradient id="flowGrad" x1="0" y1="0" x2="1" y2="0">
            <stop offset="0%" stopColor={tk.color} stopOpacity="0.1"/>
            <stop offset="100%" stopColor={tk.color} stopOpacity="0.9"/>
          </linearGradient>
          <radialGradient id="coreGrad">
            <stop offset="0%" stopColor="#FFD166"/>
            <stop offset="40%" stopColor={tk.color}/>
            <stop offset="100%" stopColor={tk.dark} stopOpacity="0"/>
          </radialGradient>
        </defs>
        {/* Source nodes (left) */}
        {[
          { y: 40, label: 'Swap Fees',     val: '1.2M XOR' },
          { y: 100, label: 'Transfer Fees', val: '340K XOR' },
          { y: 160, label: 'Extrinsic Fees', val: '820K XOR' },
          { y: 220, label: 'Bridge Fees',   val: '95K XOR' },
        ].map((n, i) => (
          <g key={i} className="flow-node">
            <rect x="10" y={n.y-18} width="150" height="36" rx="8" fill="rgba(255,255,255,0.04)" stroke="rgba(255,255,255,0.1)"/>
            <text x="22" y={n.y - 3} className="flow-label">{n.label}</text>
            <text x="22" y={n.y + 12} className="flow-sub">{n.val}</text>
            <path d={`M160 ${n.y} C 240 ${n.y}, 260 140, 330 140`}
              stroke="url(#flowGrad)" strokeWidth="1.6" fill="none"
              strokeDasharray="4 4">
              <animate attributeName="stroke-dashoffset" from="0" to="-16" dur={(1.2/liveSpeed)+'s'} repeatCount="indefinite"/>
            </path>
          </g>
        ))}

        {/* Furnace core */}
        <circle cx="400" cy="140" r="60" fill="url(#coreGrad)">
          <animate attributeName="r" values="58;66;58" dur="2.4s" repeatCount="indefinite"/>
        </circle>
        <circle cx="400" cy="140" r="30" fill="#FFD166" opacity="0.8">
          <animate attributeName="opacity" values="0.6;1;0.6" dur="1.2s" repeatCount="indefinite"/>
        </circle>
        <text x="400" y="144" textAnchor="middle" fill="white" fontWeight="900" fontSize="11" style={{letterSpacing: '0.15em'}}>FURNACE</text>

        {/* Output labels */}
        {[
          { y: 70, label: 'XOR Burn', val: '70%' },
          { y: 210, label: 'Referrer', val: '30%' },
        ].map((n, i) => (
          <g key={i}>
            <path d={`M460 140 C 520 140, 520 ${n.y}, 560 ${n.y}`}
              stroke={tk.color} strokeWidth="1.6" fill="none" opacity="0.7"
              strokeDasharray="4 4">
              <animate attributeName="stroke-dashoffset" from="0" to="-16" dur={(1.2/liveSpeed)+'s'} repeatCount="indefinite"/>
            </path>
            <text x="555" y={n.y - 3} textAnchor="end" className="flow-label">{n.label}</text>
            <text x="555" y={n.y + 12} textAnchor="end" className="flow-sub">{n.val}</text>
          </g>
        ))}
      </svg>
    </div>
  );
}

function BurnChart({ token, type, motion }) {
  const tk = TOKENS[token];
  // Cumulative-ish burn curve
  const data = useMemo(() => {
    const rand = seededRand(token.charCodeAt(0) * 17);
    const pts = [];
    let v = 1000;
    for (let i = 0; i < 60; i++) {
      v += rand() * 80 + 10;
      pts.push(v);
    }
    return pts;
  }, [token]);

  const W = 560, H = 200, pad = 8;
  const { line, area } = areaPath(data, W, H, pad);

  return (
    <div className="chart-wrap" style={{ ['--tok-color']: tk.color, ['--tok-glow']: tk.glow }}>
      <svg className="chart-svg" viewBox={`0 0 ${W} ${H}`} preserveAspectRatio="none">
        <defs>
          <linearGradient id="burnArea" x1="0" y1="0" x2="0" y2="1">
            <stop offset="0%" stopColor={tk.color} stopOpacity="0.45"/>
            <stop offset="100%" stopColor={tk.color} stopOpacity="0"/>
          </linearGradient>
        </defs>
        <g className="chart-grid">
          {[0.25, 0.5, 0.75].map((p, i) => (
            <line key={i} x1="0" x2={W} y1={H * p} y2={H * p}/>
          ))}
        </g>
        <g className="chart-area">
          {(type === 'area' || type === 'line') && <path className="area" d={area}/>}
          {(type === 'area' || type === 'line') && <path className="line" d={line}/>}
          {type === 'bars' && data.map((v, i) => {
            const x = pad + (i / (data.length - 1)) * (W - pad * 2);
            const h = ((v - data[0]) / (data[data.length-1] - data[0])) * (H - pad * 2);
            return <rect key={i} x={x-2} y={H - pad - h} width="4" height={h} fill={tk.color} opacity="0.8"/>;
          })}
        </g>
      </svg>
    </div>
  );
}

function BurnSection({ tweaks }) {
  const t = useT();
  const [token, setToken] = useState('XOR');
  const [tf, setTf] = useState('7d');   // G7: timeframe selector 24h / 7d / 30d / All
  const tk = TOKENS[token];

  // Real burn data from prod — stats (24h/7d/30d totals), fee-flow (distribution), holders (top 10).
  const [stats, setStats] = useState(null);
  const [feeFlow, setFeeFlow] = useState(null);
  const [holdersData, setHoldersData] = useState(null);
  // Burn counter is driven by the selected timeframe (24h / 7d / 30d) from
  // stats.stats[tf].totalBurned; "All" pipes in stats.stats['30d'] since prod
  // doesn't expose an all-time burn total at this endpoint.
  const [heroVal, setHeroVal] = useState(0);

  // Fetch /burns/* whenever the selected token changes.
  useEffect(() => {
    let cancelled = false;
    setStats(null); setHoldersData(null);
    fetchJson('/burns/stats/' + token).then(j => { if (!cancelled) setStats(j); });
    fetchJson('/burns/holders/' + token).then(j => { if (!cancelled) setHoldersData(j); });
    return () => { cancelled = true; };
  }, [token]);

  // Fee-flow is global across all burn tokens — one fetch + periodic refresh.
  useEffect(() => {
    let cancelled = false;
    const pull = () => fetchJson('/burns/fee-flow').then(j => { if (!cancelled) setFeeFlow(j); });
    pull();
    const id = setInterval(pull, 30_000);
    return () => { cancelled = true; clearInterval(id); };
  }, []);

  // Seed heroVal from the stats for the selected timeframe.
  useEffect(() => {
    if (!stats || !stats.stats) return;
    // "All" → use the longest available window (30d) as a proxy.
    const key = tf === 'All' ? '30d' : tf;
    if (stats.stats[key]) {
      setHeroVal(stats.stats[key].totalBurned || 0);
    }
  }, [stats, tf]);

  // On each new block we assume a sliver of burn happened (fees). This keeps the
  // counter visibly moving; the true amount gets reconciled when /burns/stats
  // refreshes on the next fetch. Also triggers the Furnace ember animation via
  // the Furnace component's own interval — here we just re-fetch stats when a
  // finalized block arrives and the user is ON the Burns page.
  useEffect(() => {
    const sock = getPulseSocket && getPulseSocket();
    if (!sock) return;
    let lastFetch = Date.now();
    const onBlock = (b) => {
      // Cheap optimistic tick — $0.00001 XOR per block is a rounding-error proxy.
      setHeroVal(v => v + 0.0001);
      // Throttled real refresh: at most once per 30s.
      const now = Date.now();
      if (now - lastFetch > 30_000) {
        lastFetch = now;
        fetchJson('/burns/stats/' + token).then(j => { if (j) setStats(j); });
      }
    };
    sock.on('new-block-stats', onBlock);
    return () => sock.off('new-block-stats', onBlock);
  }, [token]);

  // Real top holders from /burns/holders/:sym — top 6.
  const holders = useMemo(() => {
    if (!holdersData || !Array.isArray(holdersData.data)) return [];
    const total = holdersData.totalSupply || 1;
    return holdersData.data.slice(0, 6).map((h, i) => ({
      addr: h.address,
      name: h.name || IDENTITIES[h.address] || null,
      pct: +((h.balance / total) * 100).toFixed(2),
      amt: h.balance,
    }));
  }, [holdersData]);

  // Derived: current supply + 24h/7d/30d deltas.
  const currentSupply = (stats && stats.currentSupply) || 0;
  const d24 = (stats && stats.stats && stats.stats['24h'] && stats.stats['24h'].totalBurned) || 0;
  const d7 = (stats && stats.stats && stats.stats['7d'] && stats.stats['7d'].totalBurned) || 0;
  const d30 = (stats && stats.stats && stats.stats['30d'] && stats.stats['30d'].totalBurned) || 0;
  const usd24 = (stats && stats.stats && stats.stats['24h'] && stats.stats['24h'].totalBurnedUsd) || 0;
  const price = d24 > 0 ? usd24 / d24 : 0;

  return (
    <div style={{ ['--tok-color']: tk.color, ['--tok-glow']: tk.glow, ['--tok-dark']: tk.dark, ['--tok-grad']: tk.grad }}>
      <PageHeader title={t('burn.title')} sub={t('burn.sub')}>
        <div className="segmented">
          {['24h', '7d', '30d', 'All'].map(t => (
            <button key={t} className={tf === t ? 'active' : ''} onClick={() => setTf(t)}>{t}</button>
          ))}
        </div>
        <button className="btn">Share</button>
        <button className="btn primary">Screenshot</button>
      </PageHeader>

      <div className="burn-layout">
        {/* Hero + furnace */}
        <div className="card burn-hero">
          <div className="burn-title-row">
            <div className="card-title"><span className="dot"/> Total {token} Burned</div>
            <div className="burn-token-selector">
              {Object.keys(TOKENS).slice(0, 5).map(t => {
                const T = TOKENS[t];
                const active = t === token;
                return (
                  <button key={t}
                    className={'burn-token-btn' + (active ? ' active' : '')}
                    style={{ ['--tok-grad']: T.grad, ['--tok-glow']: T.glow, ['--tok-color']: T.color }}
                    onClick={() => setToken(t)}>
                    <span className="burn-token-dot" style={{background: T.color}}/>
                    {t}
                  </button>
                );
              })}
            </div>
          </div>

          <div className="burn-hero-value num">{fmt.num(heroVal, 2)}</div>
          <div className="burn-hero-unit">{token} · {price > 0 ? fmt.usd(heroVal * price) : '—'} @ current price</div>

          <div className="burn-hero-meta">
            <div className="mi"><span>24h</span> <strong>+{fmt.num(d24, 2)}</strong></div>
            <div className="mi"><span>7d</span>  <strong>+{fmt.num(d7, 2)}</strong></div>
            <div className="mi"><span>30d</span> <strong>+{fmt.num(d30, 2)}</strong></div>
            <div className="mi"><span>24h usd</span> <strong style={{color: '#10B981'}}>{fmt.usd(usd24)}</strong></div>
          </div>

          <Furnace token={token} liveSpeed={tweaks.liveSpeed} motion={tweaks.motion}/>

          <div className="burn-stats">
            <div className="bstat"><div className="l">Current Supply</div><div className="v">{fmt.num(currentSupply, 1)}</div><div className="d">{token}</div></div>
            <div className="bstat"><div className="l">Market Cap</div><div className="v">{price > 0 ? fmt.usd(currentSupply * price) : '—'}</div><div className="d">live</div></div>
            <div className="bstat"><div className="l">Price</div><div className="v">{price > 0 ? '$' + price.toFixed(price < 1 ? 4 : 2) : '—'}</div><div className="d">from 24h burn</div></div>
            <div className="bstat"><div className="l">Holders</div><div className="v">{holdersData ? holdersData.totalHolders.toLocaleString() : '—'}</div><div className="d">{holdersData ? 'total' : 'loading…'}</div></div>
          </div>
        </div>

        {/* Sidebar: chart + holders */}
        <div style={{display:'grid', gap: 18, alignContent: 'start'}}>
          <div className="card">
            <div className="card-header">
              <div className="card-title"><span className="dot"/> {t('burn.burnRateCum')}</div>
              <div className="segmented" style={{transform:'scale(0.9)', transformOrigin:'right'}}>
                <button className={tweaks.chartType === 'area' ? 'active' : ''}>Area</button>
                <button className={tweaks.chartType === 'line' ? 'active' : ''}>Line</button>
                <button className={tweaks.chartType === 'bars' ? 'active' : ''}>Bars</button>
              </div>
            </div>
            <div className="card-body">
              <BurnChart token={token} type={tweaks.chartType} motion={tweaks.motion}/>
            </div>
          </div>

          <div className="card">
            <div className="card-header">
              <div className="card-title"><span className="dot"/> {t('burn.topHolders')} · {token}</div>
              <span className="tag accent">LIVE</span>
            </div>
            <div className="card-body">
              {holders.map((h, i) => (
                <div className="holder-row" key={h.addr}>
                  <div className="holder-rank">{i+1}</div>
                  <div className="holder-addr">{h.name
                    ? <><span className="ident">{h.name}</span> · {fmt.addr(h.addr, 4, 3)}</>
                    : fmt.addr(h.addr, 6, 4)
                  }</div>
                  <div className="holder-bar"><div className="fill" style={{width: (h.pct * 4) + '%'}}/></div>
                  <div className="holder-pct">{h.pct}%</div>
                </div>
              ))}
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}

Object.assign(window, { BurnSection });
