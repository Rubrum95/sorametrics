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

// Animated Sankey-style fee-flow diagram (4.8.6 model). Each fee splits into
// flows whose band thickness is proportional to its real share; particles ride
// each band via SVG animateMotion. Pure SVG — no DOM-manipulated embers.
function Furnace({ token, liveSpeed, motion, feeFlow }) {
  const speed = Math.max(0.4, Number(liveSpeed) || 1);
  const animate = motion !== 'none';

  // Real distribution from /burns/fee-flow (4.8.6 keys). Fall back to the
  // theoretical 4.8.6 split (no-referrer case) until live data lands.
  const dist = feeFlow?.distribution || {};
  const hasLive = ['xorBurn', 'valStaking', 'valBurn', 'referrer'].some(k => Number(dist[k]) > 0);
  const flowDefs = [
    { key: 'xorBurn',    label: 'XOR Burn',       color: '#E5243B', kind: 'burn' },
    { key: 'valStaking', label: '→ VAL · Staking', color: '#7DD3FC', kind: 'node' },
    { key: 'referrer',   label: 'Referrer',        color: '#8B7FD9', kind: 'node' },
    { key: 'valBurn',    label: 'VAL Burn',         color: '#F5B041', kind: 'burn' },
  ];
  const fallback = { xorBurn: 60.0, valStaking: 25.4, referrer: 11.8, valBurn: 2.8 };
  const flows = flowDefs
    .map(f => ({ ...f, v: hasLive ? (Number(dist[f.key]) || 0) : fallback[f.key] }))
    .filter(f => f.v > 0);
  const total = flows.reduce((s, f) => s + f.v, 0) || 1;

  const totalXor = Number(feeFlow?.totalXorFees);
  const totalLabel = Number.isFinite(totalXor) && totalXor > 0
    ? (totalXor < 1 ? totalXor.toFixed(4) : totalXor.toFixed(2)) + ' XOR'
    : 'per 100 XOR';

  // Layout: source (left) → split node (center) → N destinations (right).
  const SPLIT_X = 250, SPLIT_Y = 140;
  const DEST_X = 470;
  const top = 44, bottom = 236;
  const gap = flows.length > 1 ? (bottom - top) / (flows.length - 1) : 0;

  return (
    <div className="furnace-wrap" style={{ ['--tok-color']: TOKENS[token]?.color }}>
      <svg className="fee-flow-svg" viewBox="0 0 600 280" preserveAspectRatio="xMidYMid meet">
        <defs>
          <linearGradient id="ff-in" x1="0" y1="0" x2="1" y2="0">
            <stop offset="0%" stopColor="#E5243B" stopOpacity="0.15"/>
            <stop offset="100%" stopColor="#E5243B" stopOpacity="0.85"/>
          </linearGradient>
          <radialGradient id="ff-split">
            <stop offset="0%" stopColor="#FFD166"/>
            <stop offset="55%" stopColor="#E5243B"/>
            <stop offset="100%" stopColor="#8B0000" stopOpacity="0"/>
          </radialGradient>
          {flows.map(f => (
            <linearGradient key={f.key} id={`ff-${f.key}`} x1="0" y1="0" x2="1" y2="0">
              <stop offset="0%" stopColor={f.color} stopOpacity="0.55"/>
              <stop offset="100%" stopColor={f.color} stopOpacity={f.kind === 'burn' ? '0.05' : '0.85'}/>
            </linearGradient>
          ))}
        </defs>

        {/* Source node */}
        <rect x="14" y={SPLIT_Y - 24} width="150" height="48" rx="10" fill="rgba(255,255,255,0.035)" stroke="rgba(255,255,255,0.12)"/>
        <text x="28" y={SPLIT_Y - 4} className="flow-label">XOR fees in</text>
        <text x="28" y={SPLIT_Y + 15} className="flow-sub">{totalLabel}</text>

        {/* Inflow band + particles */}
        <path id="ff-inflow" d={`M164 ${SPLIT_Y} L ${SPLIT_X} ${SPLIT_Y}`} stroke="url(#ff-in)" strokeWidth="10" fill="none" strokeLinecap="round"/>
        {animate && [0, 1, 2].map(i => (
          <circle key={i} r="3.2" fill="#FF6B5A">
            <animateMotion dur={`${1.6 / speed}s`} begin={`${i * 0.5}s`} repeatCount="indefinite"
              path={`M164 ${SPLIT_Y} L ${SPLIT_X} ${SPLIT_Y}`}/>
          </circle>
        ))}

        {/* Split core */}
        <circle cx={SPLIT_X} cy={SPLIT_Y} r="22" fill="url(#ff-split)">
          {animate && <animate attributeName="r" values="20;25;20" dur={`${2.4 / speed}s`} repeatCount="indefinite"/>}
        </circle>
        <circle cx={SPLIT_X} cy={SPLIT_Y} r="9" fill="#FFE08A">
          {animate && <animate attributeName="opacity" values="0.65;1;0.65" dur={`${1.3 / speed}s`} repeatCount="indefinite"/>}
        </circle>

        {/* Output bands — thickness ∝ share, particles ride each band */}
        {flows.map((f, i) => {
          const pct = (f.v / total) * 100;
          const destY = flows.length === 1 ? SPLIT_Y : top + i * gap;
          const sw = Math.max(2.5, Math.min(34, (pct / 100) * 80));
          const d = `M${SPLIT_X} ${SPLIT_Y} C ${SPLIT_X + 90} ${SPLIT_Y}, ${DEST_X - 90} ${destY}, ${DEST_X} ${destY}`;
          const pdur = (1.4 + (1 - pct / 100) * 1.8) / speed; // higher share → faster particles
          const nParticles = pct > 30 ? 4 : pct > 10 ? 2 : 1;
          return (
            <g key={f.key}>
              <path d={d} stroke={`url(#ff-${f.key})`} strokeWidth={sw} fill="none" strokeLinecap="round" opacity="0.9"/>
              {animate && Array.from({ length: nParticles }).map((_, p) => (
                <circle key={p} r={f.kind === 'burn' ? 2.6 : 3} fill={f.color}>
                  <animateMotion dur={`${pdur}s`} begin={`${p * (pdur / nParticles)}s`} repeatCount="indefinite" path={d}/>
                  {f.kind === 'burn' && <animate attributeName="opacity" values="1;1;0" dur={`${pdur}s`} begin={`${p * (pdur / nParticles)}s`} repeatCount="indefinite"/>}
                </circle>
              ))}
              {/* Destination marker: ring for staking/referrer, spark for burn */}
              {f.kind === 'node'
                ? <circle cx={DEST_X} cy={destY} r="5" fill="none" stroke={f.color} strokeWidth="2"/>
                : <circle cx={DEST_X} cy={destY} r="4" fill={f.color}>{animate && <animate attributeName="opacity" values="1;0.3;1" dur={`${1.1 / speed}s`} repeatCount="indefinite"/>}</circle>}
              <text x={DEST_X + 14} y={destY - 2} className="flow-label">{f.label}</text>
              <text x={DEST_X + 14} y={destY + 14} className="flow-sub" style={{ fill: f.color }}>{pct.toFixed(1)}%</text>
            </g>
          );
        })}
      </svg>
    </div>
  );
}

function BurnChart({ token, type, series }) {
  const tk = TOKENS[token];
  // Real cumulative burn curve from the indexer (/burns/series). No synthetic data.
  const data = useMemo(() => (series || []).map(p => p.cumulative), [series]);

  const W = 560, H = 200, pad = 8;
  if (series === null) {
    return <div className="chart-wrap" style={{display:'flex',alignItems:'center',justifyContent:'center',height:200,color:'var(--fg-2)',fontSize:13}}>Cargando…</div>;
  }
  if (data.length < 2) {
    return <div className="chart-wrap" style={{display:'flex',alignItems:'center',justifyContent:'center',height:200,color:'var(--fg-2)',fontSize:13,textAlign:'center',padding:'0 24px'}}>
      Esperando histórico del indexer · {token} burns reales por día (se llena con el tiempo).
    </div>;
  }
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
  const [chartType, setChartType] = useState(tweaks.chartType || 'area');
  const [holdersPage, setHoldersPage] = useState(1);
  const [copied, setCopied] = useState(false);
  const tk = TOKENS[token];

  // Real burn data from prod — stats (24h/7d/30d totals), fee-flow (distribution), holders (top 10).
  const [stats, setStats] = useState(null);
  const [feeFlow, setFeeFlow] = useState(null);
  const [holdersData, setHoldersData] = useState(null);
  const [burnSeries, setBurnSeries] = useState(null);
  const [refWindow, setRefWindow] = useState('24h');
  const [refData, setRefData] = useState(null);
  const [priceData, setPriceData] = useState(null);

  const shareLink = async () => {
    try {
      await navigator.clipboard.writeText(window.location.href + '#burns/' + token);
      setCopied(true);
      setTimeout(() => setCopied(false), 1600);
    } catch {}
  };
  const screenshot = () => { try { window.print(); } catch {} };
  // Burn counter is driven by the selected timeframe (24h / 7d / 30d) from
  // stats.stats[tf].totalBurned; "All" pipes in stats.stats['30d'] since prod
  // doesn't expose an all-time burn total at this endpoint.
  const [heroVal, setHeroVal] = useState(0);

  // Fetch /burns/* whenever the selected token changes.
  useEffect(() => {
    let cancelled = false;
    setStats(null); setHoldersData(null); setBurnSeries(null); setPriceData(null);
    fetchJson('/burns/stats/' + token).then(j => { if (!cancelled) setStats(j); });
    fetchJson('/burns/holders/' + token).then(j => { if (!cancelled) setHoldersData(j); });
    fetchJson('/burns/series/' + token + '?days=30').then(j => { if (!cancelled) setBurnSeries(j?.points || []); });
    fetchJson('/burns/supply/' + token).then(j => { if (!cancelled) setPriceData(j); });
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

  // Referral activity (XOR only) — paid to referrers vs redirected-to-burn, per timeframe.
  useEffect(() => {
    if (token !== 'XOR') return;
    let cancelled = false;
    setRefData(null);
    const pull = () => fetchJson('/stats/fee-burns-live?window=' + refWindow).then(j => { if (!cancelled) setRefData(j); });
    pull();
    const id = setInterval(pull, 30_000);
    return () => { cancelled = true; clearInterval(id); };
  }, [token, refWindow]);

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

  // Real top holders from /burns/holders/:sym — paginated (10 per page).
  const HOLDERS_PER_PAGE = 10;
  const allHolders = useMemo(() => {
    if (!holdersData || !Array.isArray(holdersData.data)) return [];
    const total = holdersData.totalSupply || 1;
    return holdersData.data.map((h, i) => ({
      addr: h.address,
      name: h.name || IDENTITIES[h.address] || null,
      pct: +((h.balance / total) * 100).toFixed(2),
      amt: h.balance,
      rank: i + 1,
    }));
  }, [holdersData]);
  const holdersTotalPages = Math.max(1, Math.ceil(allHolders.length / HOLDERS_PER_PAGE));
  const holders = allHolders.slice((holdersPage - 1) * HOLDERS_PER_PAGE, holdersPage * HOLDERS_PER_PAGE);
  useEffect(() => { setHoldersPage(1); }, [token]);

  // Derived: current supply + 24h/7d/30d deltas.
  const currentSupply = (stats && stats.currentSupply) || 0;
  const d24 = (stats && stats.stats && stats.stats['24h'] && stats.stats['24h'].totalBurned) || 0;
  const d7 = (stats && stats.stats && stats.stats['7d'] && stats.stats['7d'].totalBurned) || 0;
  const d30 = (stats && stats.stats && stats.stats['30d'] && stats.stats['30d'].totalBurned) || 0;
  const usd24 = (stats && stats.stats && stats.stats['24h'] && stats.stats['24h'].totalBurnedUsd) || 0;
  // Real market price (same source as Tokens section: tokenPrices via /burns/supply).
  // Fallback to the burn-implied price only if the real one is unavailable.
  const price = (priceData && priceData.price > 0) ? priceData.price : (d24 > 0 ? usd24 / d24 : 0);

  return (
    <div style={{ ['--tok-color']: tk.color, ['--tok-glow']: tk.glow, ['--tok-dark']: tk.dark, ['--tok-grad']: tk.grad }}>
      <PageHeader title={t('burn.title')} sub={t('burn.sub')}>
        <div className="segmented">
          {['24h', '7d', '30d', 'All'].map(r => (
            <button key={r} className={tf === r ? 'active' : ''} onClick={() => setTf(r)}>{r}</button>
          ))}
        </div>
        <button className="btn" onClick={shareLink} title="Copiar link">{copied ? '✓ Copiado' : 'Share'}</button>
        <button className="btn primary" onClick={screenshot} title="Imprimir / exportar PDF">Screenshot</button>
      </PageHeader>

      <div className="burn-layout">
        {/* Hero + furnace */}
        <div className="card burn-hero">
          <div className="burn-title-row">
            <div className="card-title"><span className="dot"/> Total {token} Burned</div>
            <div className="burn-token-selector">
              {/* 4.8.6: KUSD & TBCD no longer burn via fees (kusd weight = 0, TBCD out of the fee model). */}
              {['XOR', 'VAL', 'PSWAP'].map(t => {
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

          <Furnace token={token} liveSpeed={tweaks.liveSpeed} motion={tweaks.motion} feeFlow={feeFlow}/>

          <div className="burn-stats">
            <div className="bstat"><div className="l">Current Supply</div><div className="v">{fmt.num(currentSupply, 1)}</div><div className="d">{token}</div></div>
            <div className="bstat"><div className="l">Market Cap</div><div className="v">{price > 0 ? fmt.usd(currentSupply * price) : '—'}</div><div className="d">live</div></div>
            <div className="bstat"><div className="l">Price</div><div className="v">{price > 0 ? '$' + price.toFixed(price < 1 ? 4 : 2) : '—'}</div><div className="d">from 24h burn</div></div>
            <div className="bstat"><div className="l">Holders</div><div className="v">{holdersData ? holdersData.totalHolders.toLocaleString() : '—'}</div><div className="d">{holdersData ? 'total' : 'loading…'}</div></div>
          </div>

          {/* Referral activity (XOR only) — real per-timeframe split: paid to referrers
              vs redirected to XOR burn when no referrer. 4.8.6: redirect burns XOR (not KUSD). */}
          {token === 'XOR' && (
            <div className="card">
              <div className="card-header">
                <div className="card-title"><span className="dot"/> {t('burn.referral.title')}</div>
                <div className="segmented" style={{transform:'scale(0.9)', transformOrigin:'right'}}>
                  {['6h','24h','7d','30d'].map(w => (
                    <button key={w} className={refWindow === w ? 'active' : ''} onClick={() => setRefWindow(w)}>{w}</button>
                  ))}
                </div>
              </div>
              <div className="card-body">
                {refData && refData.referrer ? (() => {
                  const paid = Number(refData.referrer.paidXor) || 0;
                  const redirected = Number(refData.referrer.redirectedToKusdXor) || 0;
                  const totalRef = paid + redirected;
                  const paidPct = totalRef > 0 ? (paid / totalRef) * 100 : 0;
                  return (
                    <>
                      <div style={{display:'grid', gridTemplateColumns:'1fr 1fr', gap:16, marginBottom:12}}>
                        <div>
                          <div className="muted tiny" style={{textTransform:'uppercase', letterSpacing:'.05em', marginBottom:3}}>{t('burn.referral.paid')}</div>
                          <div className="num" style={{fontSize:20, fontWeight:700, color:'#8B7FD9'}}>{fmt.num(paid, 4)} <span style={{fontSize:11, color:'var(--fg-2)'}}>XOR</span></div>
                          <div className="muted tiny" style={{fontSize:10}}>{t('burn.referral.paidSub')}</div>
                        </div>
                        <div>
                          <div className="muted tiny" style={{textTransform:'uppercase', letterSpacing:'.05em', marginBottom:3}}>{t('burn.referral.redirected')}</div>
                          <div className="num" style={{fontSize:20, fontWeight:700, color:'#E5243B'}}>{fmt.num(redirected, 4)} <span style={{fontSize:11, color:'var(--fg-2)'}}>XOR</span></div>
                          <div className="muted tiny" style={{fontSize:10}}>{t('burn.referral.redirectedSub')}</div>
                        </div>
                      </div>
                      {totalRef > 0 && (
                        <div style={{height:6, borderRadius:3, overflow:'hidden', display:'flex', background:'rgba(255,255,255,0.06)'}}>
                          <div style={{width: paidPct + '%', background:'#8B7FD9'}}/>
                          <div style={{width: (100 - paidPct) + '%', background:'#E5243B'}}/>
                        </div>
                      )}
                      <div className="muted tiny" style={{marginTop:8, fontSize:10}}>{t('burn.referral.note')}</div>
                    </>
                  );
                })() : <div className="muted tiny" style={{padding:'8px 0'}}>Cargando…</div>}
              </div>
            </div>
          )}
        </div>

        {/* Sidebar: chart + holders */}
        <div style={{display:'grid', gap: 18, alignContent: 'start'}}>
          <div className="card">
            <div className="card-header">
              <div className="card-title"><span className="dot"/> {t('burn.burnRateCum')}</div>
              <div className="segmented" style={{transform:'scale(0.9)', transformOrigin:'right'}}>
                {['area', 'line', 'bars'].map(ty => (
                  <button key={ty} className={chartType === ty ? 'active' : ''} onClick={() => setChartType(ty)}>
                    {ty[0].toUpperCase() + ty.slice(1)}
                  </button>
                ))}
              </div>
            </div>
            <div className="card-body">
              <BurnChart token={token} type={chartType} series={burnSeries}/>
            </div>
          </div>

          <div className="card">
            <div className="card-header">
              <div className="card-title"><span className="dot"/> {t('burn.topHolders')} · {token}</div>
              <span className="tag accent">LIVE</span>
            </div>
            <div className="card-body">
              {holders.length === 0 && (
                <div className="muted tiny" style={{padding:'8px 0'}}>Cargando holders...</div>
              )}
              {holders.map((h) => (
                <div className="holder-row" key={h.addr}>
                  <div className="holder-rank">{h.rank}</div>
                  <div className="holder-addr">{h.name
                    ? <><span className="ident">{h.name}</span> · {fmt.addr(h.addr, 4, 3)}</>
                    : fmt.addr(h.addr, 6, 4)
                  }</div>
                  <div className="holder-bar"><div className="fill" style={{width: Math.min(100, h.pct * 4) + '%'}}/></div>
                  <div className="holder-pct">{h.pct}%</div>
                </div>
              ))}
              {allHolders.length > HOLDERS_PER_PAGE && (
                <div style={{display:'flex', justifyContent:'space-between', alignItems:'center', marginTop:10, paddingTop:8, borderTop:'1px solid var(--border-color)', fontSize:12}}>
                  <button className="btn" disabled={holdersPage === 1} onClick={() => setHoldersPage(p => Math.max(1, p - 1))} style={{padding:'4px 10px'}}>← Prev</button>
                  <span className="muted">Página {holdersPage} / {holdersTotalPages}</span>
                  <button className="btn" disabled={holdersPage === holdersTotalPages} onClick={() => setHoldersPage(p => Math.min(holdersTotalPages, p + 1))} style={{padding:'4px 10px'}}>Next →</button>
                </div>
              )}
            </div>
          </div>

          {/* Fee flow distribution — equivalent to v1's animated SVG "fee flow" card. */}
          {feeFlow && (
            <div className="card">
              <div className="card-header">
                <div className="card-title"><span className="dot"/> Fee Flow · distribución</div>
                <span className="tag">on-chain</span>
              </div>
              <div className="card-body">
                {(() => {
                  const dist = feeFlow?.distribution || {};
                  const entries = Object.entries(dist)
                    .filter(([, v]) => typeof v === 'number' && v > 0)
                    .sort((a, b) => b[1] - a[1]);
                  const total = entries.reduce((s, [, v]) => s + v, 0) || 1;
                  const palette = { xorBurn:'#E5243B', valStaking:'#7DD3FC', valBurn:'#F5B041', referrer:'#8B7FD9', kusdBuyback:'#60A5FA', unallocated:'#6B7280' };
                  const label   = { xorBurn:'XOR burn', valStaking:'→ VAL · staking', valBurn:'VAL burn', referrer:'Referrer', kusdBuyback:'KUSD buy-back', unallocated:'Unallocated' };
                  if (entries.length === 0) return <div className="muted tiny">Sin datos de fee flow.</div>;
                  return entries.map(([k, v]) => {
                    const pct = (v / total) * 100;
                    const color = palette[k] || '#94A3B8';
                    const name = label[k] || k;
                    return (
                      <div key={k} className="holder-row">
                        <div style={{width:12, height:12, borderRadius:3, background:color, flexShrink:0}}/>
                        <div className="holder-addr"><span className="ident">{name}</span></div>
                        <div className="holder-bar"><div className="fill" style={{width: pct + '%', background: color}}/></div>
                        <div className="holder-pct">{pct.toFixed(1)}%</div>
                      </div>
                    );
                  });
                })()}
              </div>
            </div>
          )}
        </div>
      </div>
    </div>
  );
}

Object.assign(window, { BurnSection });
