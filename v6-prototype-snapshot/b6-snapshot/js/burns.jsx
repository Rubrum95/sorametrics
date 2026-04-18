/* global React, TOKENS, fmt, FAKE_ADDRS, IDENTITIES, areaPath, sparkPath, I, useT */
const { useState, useEffect, useRef, useMemo } = React;

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
    const t = useT();
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
  const [token, setToken] = useState('XOR');
  const tk = TOKENS[token];
  const [heroVal, setHeroVal] = useState(5418291);

  useEffect(() => {
    const id = setInterval(() => {
      setHeroVal(v => v + Math.random() * 12 * (tweaks.liveSpeed || 1));
    }, 900);
    return () => clearInterval(id);
  }, [tweaks.liveSpeed]);

  // switch token resets base
  useEffect(() => {
    const base = { XOR: 5418291, VAL: 1204802, PSWAP: 872310, TBCD: 52483, KUSD: 109742 };
    setHeroVal(base[token] || 1000000);
  }, [token]);

  const holders = useMemo(() => {
    const rand = seededRand(token.charCodeAt(0) * 31);
    return FAKE_ADDRS.map((a, i) => ({
      addr: a,
      name: IDENTITIES[a] || null,
      pct: +(25 - i * 2.5 + rand() * 1.5).toFixed(2),
      amt: (5_000_000 / (i + 1)) * (0.4 + rand() * 0.8),
    })).slice(0, 6);
  }, [token]);

  return (
    <div style={{ ['--tok-color']: tk.color, ['--tok-glow']: tk.glow, ['--tok-dark']: tk.dark, ['--tok-grad']: tk.grad }}>
      <PageHeader title={t('burn.title')} sub={t('burn.sub')}>
        <div className="segmented">
          {['24h', '7d', '30d', 'All'].map(tf => (
            <button key={tf} className={tf === '7d' ? 'active' : ''}>{tf}</button>
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

          <div className="burn-hero-value num">{fmt.int(heroVal)}</div>
          <div className="burn-hero-unit">{token} · {fmt.usd(heroVal * (token === 'XOR' ? 0.072 : 1.08))} @ current price</div>

          <div className="burn-hero-meta">
            <div className="mi"><span>24h</span> <strong>+{fmt.num(heroVal * 0.0021)}</strong></div>
            <div className="mi"><span>7d</span>  <strong>+{fmt.num(heroVal * 0.014)}</strong></div>
            <div className="mi"><span>30d</span> <strong>+{fmt.num(heroVal * 0.042)}</strong></div>
            <div className="mi"><span>vs mint</span> <strong style={{color: '#10B981'}}>deflationary 0.84%/yr</strong></div>
          </div>

          <Furnace token={token} liveSpeed={tweaks.liveSpeed} motion={tweaks.motion}/>

          <div className="burn-stats">
            <div className="bstat"><div className="l">Current Supply</div><div className="v">{fmt.num(350e6 - heroVal, 1)}</div><div className="d">-0.02% / day</div></div>
            <div className="bstat"><div className="l">Market Cap</div><div className="v">{fmt.usd(heroVal * 72)}</div><div className="d">+3.2%</div></div>
            <div className="bstat"><div className="l">Price</div><div className="v">$0.072</div><div className="d neg">-1.4%</div></div>
            <div className="bstat"><div className="l">Holders</div><div className="v">18,432</div><div className="d">+12 today</div></div>
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
