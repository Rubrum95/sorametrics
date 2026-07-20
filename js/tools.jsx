/* global React, useT, fmt, PageHeader */
// Tools section — a home for utilities that don't fit in the "network
// explorer" buckets. First tool: Block ↔ Time Predictor. More to follow.
//
// Philosophy: everything that can be computed client-side stays
// client-side (no new endpoint, no rate limits). We just need the current
// chain tip + block time, which /staking/network already exposes.
const { useState, useEffect, useMemo } = React;

// --- Shared: current chain tip + block time, refreshed every 30s ---
function useChainTip() {
  const [tip, setTip] = useState(null);
  useEffect(() => {
    let cancelled = false;
    const pull = () => fetch('/staking/network')
      .then(r => r.ok ? r.json() : null)
      .then(j => {
        if (cancelled || !j) return;
        setTip({
          block: Number(j.bestBlock) || 0,
          blockTimeMs: Number(j.expectedBlockTime) || 6000,
          ts: Date.now(),                  // when the backend response landed
        });
      })
      .catch(() => {});
    pull();
    const id = setInterval(pull, 30_000);
    return () => { cancelled = true; clearInterval(id); };
  }, []);
  return tip;
}

// Human-friendly "in 2d 3h 15m" / "hace 5h 20m" from a signed ms delta.
// We lean on fmt.ago when delta is past, build our own for future.
function formatDelta(deltaMs, t) {
  const abs = Math.abs(deltaMs);
  const sec = Math.floor(abs / 1000);
  const min = Math.floor(sec / 60);
  const hr  = Math.floor(min / 60);
  const day = Math.floor(hr  / 24);
  const parts = [];
  if (day > 0) parts.push(day + (t('tools.unit.d', 'd')));
  if (hr  > 0) parts.push((hr  % 24) + (t('tools.unit.h', 'h')));
  if (min > 0) parts.push((min % 60) + (t('tools.unit.m', 'm')));
  if (day === 0 && hr === 0 && min < 3) parts.push((sec % 60) + (t('tools.unit.s', 's')));
  const body = parts.slice(0, 2).join(' ') || '0s';
  return deltaMs >= 0
    ? t('tools.in', 'in') + ' ' + body
    : body + ' ' + t('tools.ago', 'ago');
}

// Local ISO-ish for <input type="datetime-local"> needs 'YYYY-MM-DDTHH:mm'.
function toLocalInput(d) {
  const pad = n => (n < 10 ? '0' : '') + n;
  return d.getFullYear() + '-' + pad(d.getMonth() + 1) + '-' + pad(d.getDate())
       + 'T' + pad(d.getHours()) + ':' + pad(d.getMinutes());
}

function PredictionBlockCard() {
  const t = useT();
  const tip = useChainTip();
  const [mode, setMode] = useState('block2time'); // 'block2time' | 'time2block'
  const [blockInput, setBlockInput] = useState('');
  const [timeInput, setTimeInput] = useState(() => toLocalInput(new Date(Date.now() + 7 * 864e5))); // default +7d

  // Seed block input once the chain tip arrives (default: tip + 1h worth of blocks).
  useEffect(() => {
    if (tip && !blockInput) {
      const blocksIn1h = Math.floor(3_600_000 / tip.blockTimeMs);
      setBlockInput(String(tip.block + blocksIn1h));
    }
  }, [tip, blockInput]);

  // Compute the conversion whenever inputs or tip change.
  const result = useMemo(() => {
    if (!tip) return null;
    const bt = tip.blockTimeMs;
    if (mode === 'block2time') {
      const target = parseInt(blockInput, 10);
      if (!Number.isFinite(target) || target < 0) return { error: t('tools.invalidBlock', 'Introduce un número de bloque válido.') };
      const blocksDelta = target - tip.block;
      const msDelta = blocksDelta * bt;
      const targetTs = tip.ts + msDelta;
      const date = new Date(targetTs);
      return {
        block: target,
        date,
        blocksDelta,
        msDelta,
        deltaLabel: formatDelta(msDelta, t),
      };
    } else {
      const ms = Date.parse(timeInput);
      if (!Number.isFinite(ms)) return { error: t('tools.invalidTime', 'Introduce una fecha/hora válida.') };
      const msDelta = ms - tip.ts;
      const blocksDelta = Math.round(msDelta / bt);
      const block = tip.block + blocksDelta;
      return {
        block,
        date: new Date(ms),
        blocksDelta,
        msDelta,
        deltaLabel: formatDelta(msDelta, t),
      };
    }
  }, [mode, blockInput, timeInput, tip, t]);

  // 15-minute helpers for the time picker.
  const bumpTime = (minutes) => {
    const ms = Date.parse(timeInput) + minutes * 60_000;
    if (Number.isFinite(ms)) setTimeInput(toLocalInput(new Date(ms)));
  };
  const bumpBlock = (n) => {
    const v = parseInt(blockInput, 10);
    if (Number.isFinite(v)) setBlockInput(String(Math.max(0, v + n)));
  };

  return (
    <div className="card" style={{padding: 0}}>
      <div className="card-header">
        <div className="card-title"><span className="dot"/> {t('tools.predict.title', 'Block ↔ Time Predictor')}</div>
        <span className="tag">{tip ? '#' + tip.block.toLocaleString() : '…'}</span>
      </div>

      <div style={{padding: '16px 20px'}}>
        {/* Chain tip reference */}
        {tip && (
          <div style={{
            display:'grid', gridTemplateColumns:'1fr 1fr 1fr', gap:10, marginBottom:16,
            padding:'8px 10px', background:'rgba(96,165,250,0.06)',
            border:'1px solid rgba(96,165,250,0.15)', borderRadius:8,
          }}>
            <div>
              <div className="muted tiny">{t('tools.currentBlock', 'Bloque actual')}</div>
              <div className="num" style={{fontSize: 16, fontWeight: 700, color:'#60A5FA'}}>#{tip.block.toLocaleString()}</div>
            </div>
            <div>
              <div className="muted tiny">{t('tools.blockTime', 'Block time')}</div>
              <div className="num" style={{fontSize: 16, fontWeight: 700}}>{(tip.blockTimeMs / 1000).toFixed(1)}s</div>
            </div>
            <div>
              <div className="muted tiny">{t('tools.now', 'Ahora')}</div>
              <div className="num" style={{fontSize: 12}}>{new Date(tip.ts).toLocaleString()}</div>
            </div>
          </div>
        )}

        {/* Mode toggle — segmented control that actually looks tappable.
            The default `status-opt` class has near-invisible borders when
            inactive; here we roll a small inline style so both states are
            obviously buttons. */}
        <div style={{
          display:'inline-flex', gap:0, marginBottom:16,
          padding:3, borderRadius:10, background:'rgba(255,255,255,0.04)',
          border:'1px solid var(--border-color)',
        }}>
          {[
            { id: 'block2time', label: t('tools.mode.b2t', 'Bloque → Fecha') },
            { id: 'time2block', label: t('tools.mode.t2b', 'Fecha → Bloque') },
          ].map(opt => {
            const isActive = mode === opt.id;
            return (
              <button
                key={opt.id}
                onClick={() => setMode(opt.id)}
                style={{
                  padding: '8px 16px',
                  fontSize: 13,
                  fontWeight: isActive ? 700 : 500,
                  border: 'none',
                  borderRadius: 7,
                  cursor: 'pointer',
                  color: isActive ? 'var(--fg-0)' : 'var(--fg-2)',
                  background: isActive ? 'linear-gradient(135deg, #9B1B30, #7B5B90)' : 'transparent',
                  transition: 'all 0.15s ease',
                  whiteSpace: 'nowrap',
                }}>
                {opt.label}
              </button>
            );
          })}
        </div>

        {/* Input */}
        {mode === 'block2time' ? (
          <div style={{marginBottom:18}}>
            <label className="muted tiny" style={{display:'block', marginBottom:6}}>
              {t('tools.inputBlock', 'Número de bloque objetivo')}
            </label>
            <div style={{display:'flex', gap:6, alignItems:'center'}}>
              <input
                type="number"
                value={blockInput}
                onChange={e => setBlockInput(e.target.value)}
                style={{
                  flex:1, padding:'10px 12px', fontSize:16, fontWeight:700,
                  fontFamily:'JetBrains Mono, monospace',
                  border:'1px solid var(--border-color)', borderRadius:8,
                  background:'var(--bg-card)', color:'var(--fg-0)',
                  outline:'none',
                }}/>
              <button className="btn" onClick={() => bumpBlock(-600)}  title="-1h (−600 blocks)">-1h</button>
              <button className="btn" onClick={() => bumpBlock(-100)}  title="-10min">-10m</button>
              <button className="btn" onClick={() => bumpBlock( 100)}  title="+10min">+10m</button>
              <button className="btn" onClick={() => bumpBlock( 600)}  title="+1h (+600 blocks)">+1h</button>
            </div>
          </div>
        ) : (
          <div style={{marginBottom:18}}>
            <label className="muted tiny" style={{display:'block', marginBottom:6}}>
              {t('tools.inputTime', 'Fecha/hora objetivo')}
            </label>
            <div style={{display:'flex', gap:6, alignItems:'center'}}>
              <input
                type="datetime-local"
                value={timeInput}
                onChange={e => setTimeInput(e.target.value)}
                style={{
                  flex:1, padding:'10px 12px', fontSize:15, fontWeight:700,
                  fontFamily:'JetBrains Mono, monospace',
                  border:'1px solid var(--border-color)', borderRadius:8,
                  background:'var(--bg-card)', color:'var(--fg-0)',
                  outline:'none',
                }}/>
              <button className="btn" onClick={() => bumpTime(-60)} title="-1h">-1h</button>
              <button className="btn" onClick={() => bumpTime(-10)} title="-10min">-10m</button>
              <button className="btn" onClick={() => bumpTime( 10)} title="+10min">+10m</button>
              <button className="btn" onClick={() => bumpTime( 60)} title="+1h">+1h</button>
            </div>
          </div>
        )}

        {/* Output */}
        {result?.error && <div className="muted tiny" style={{color:'#F59E0B'}}>{result.error}</div>}
        {result && !result.error && (
          <div style={{
            padding:'14px 16px', borderRadius:10,
            background: result.msDelta >= 0 ? 'rgba(16,185,129,0.08)' : 'rgba(245,158,11,0.08)',
            border: '1px solid ' + (result.msDelta >= 0 ? 'rgba(16,185,129,0.25)' : 'rgba(245,158,11,0.25)'),
          }}>
            <div className="muted tiny" style={{marginBottom:8}}>{t('tools.result', 'Resultado')}</div>
            {mode === 'block2time' ? (
              <>
                <div style={{fontSize:22, fontWeight:700, marginBottom:4}}>
                  {result.date.toLocaleString()}
                </div>
                <div className="muted" style={{fontSize:13}}>
                  <strong>{result.deltaLabel}</strong>
                  {' · '}{Math.abs(result.blocksDelta).toLocaleString()} {t('tools.blocks', 'bloques')}
                  {' '}{result.blocksDelta >= 0 ? t('tools.forward', 'hacia adelante') : t('tools.back', 'hacia atrás')}
                </div>
              </>
            ) : (
              <>
                <div className="num" style={{fontSize:22, fontWeight:700, marginBottom:4, color:'#60A5FA'}}>
                  #{result.block.toLocaleString()}
                </div>
                <div className="muted" style={{fontSize:13}}>
                  <strong>{result.deltaLabel}</strong>
                  {' · '}{Math.abs(result.blocksDelta).toLocaleString()} {t('tools.blocks', 'bloques')}
                  {' '}{result.blocksDelta >= 0 ? t('tools.forward', 'desde ahora') : t('tools.back', 'atrás')}
                </div>
              </>
            )}
          </div>
        )}

        {/* Caveat */}
        <div className="muted tiny" style={{marginTop:14, fontSize:10, lineHeight:1.5, opacity:0.6}}>
          {t('tools.caveat', 'Estimación basada en block time de 6s. La producción real de bloques puede variar ±5% por congestión o problemas de validadores.')}
        </div>
      </div>
    </div>
  );
}

// --- Token price comparison: ratio A/B over time, real history from price_history ---
// Tokens with full hourly history (asset_ids verified on-chain + price_history + /tokens).
// The 13 comparable tokens. `color` drives the logo fallback + accents (only
// 7 live in the global TOKENS map, so we carry brand colors for all 13 here).
const COMPARE_TOKENS = [
  { sym: 'XOR',    color: '#E5243B', id: '0x0200000000000000000000000000000000000000000000000000000000000000' },
  { sym: 'VAL',    color: '#F5B041', id: '0x0200040000000000000000000000000000000000000000000000000000000000' },
  { sym: 'PSWAP',  color: '#EC4899', id: '0x0200050000000000000000000000000000000000000000000000000000000000' },
  { sym: 'ETH',    color: '#8B7FD9', id: '0x0200070000000000000000000000000000000000000000000000000000000000' },
  { sym: 'XSTUSD', color: '#34D399', id: '0x0200080000000000000000000000000000000000000000000000000000000000' },
  { sym: 'XST',    color: '#A78BFA', id: '0x0200090000000000000000000000000000000000000000000000000000000000' },
  { sym: 'TBCD',   color: '#10B981', id: '0x02000a0000000000000000000000000000000000000000000000000000000000' },
  { sym: 'KEN',    color: '#22D3EE', id: '0x02000b0000000000000000000000000000000000000000000000000000000000' },
  { sym: 'KUSD',   color: '#60A5FA', id: '0x02000c0000000000000000000000000000000000000000000000000000000000' },
  { sym: 'KGOLD',  color: '#FBBF24', id: '0x02000d0000000000000000000000000000000000000000000000000000000000' },
  { sym: 'KXOR',   color: '#F97316', id: '0x02000e0000000000000000000000000000000000000000000000000000000000' },
  { sym: 'DEO',    color: '#2DD4BF', id: '0x00f2f4fda40a4bf1fc3769d156fa695532eec31e265d75068524462c0b80f674' },
  { sym: 'VXOR',   color: '#FB7185', id: '0x006a271832f44c93bd8692584d85415f0f3dccef9748fecd129442c8edcb4361' },
];
const COMPARE_WINDOWS = [['7d','7d'], ['30d','30d'], ['90d','90d'], ['1y','365d'], ['all','all']];
const cmpTok = sym => COMPARE_TOKENS.find(x => x.sym === sym) || {};

// Adaptive ratio format (no scientific notation) — decimals scale to magnitude.
function fmtRatio(x) {
  if (x == null || !Number.isFinite(x)) return '—';
  const a = Math.abs(x);
  const d = a >= 1000 ? 2 : a >= 1 ? 4 : a >= 0.01 ? 5 : a >= 0.0001 ? 7 : 9;
  return x.toLocaleString('en-US', { minimumFractionDigits: d, maximumFractionDigits: d });
}
// Adaptive USD price format.
function fmtUsdPrice(x) {
  if (x == null || !Number.isFinite(x)) return '—';
  const a = Math.abs(x);
  const d = a >= 100 ? 2 : a >= 1 ? 4 : a >= 0.01 ? 5 : 7;
  return '$' + x.toLocaleString('en-US', { minimumFractionDigits: d, maximumFractionDigits: d });
}

// Round token logo: real artwork from window.TOKEN_LOGOS (loaded from /tokens),
// with a colored-initial circle underneath as the 404 / missing fallback.
function CmpLogo({ sym, size = 20 }) {
  const src = (typeof window !== 'undefined' && window.TOKEN_LOGOS) ? window.TOKEN_LOGOS[sym] : null;
  const color = cmpTok(sym).color || '#64748B';
  return (
    <span style={{position:'relative', width:size, height:size, flexShrink:0, display:'inline-block', verticalAlign:'middle'}}>
      <span style={{position:'absolute', inset:0, borderRadius:'50%', display:'flex', alignItems:'center', justifyContent:'center',
                    fontSize:Math.round(size*0.46), fontWeight:800, color:'#fff', letterSpacing:-0.5,
                    background:`linear-gradient(135deg, ${color}, ${color}88)`}}>{sym ? sym[0] : '?'}</span>
      {src && <img src={src} alt={sym} onError={e => { e.currentTarget.style.display = 'none'; }}
                   style={{position:'absolute', inset:0, width:size, height:size, borderRadius:'50%', objectFit:'cover', background:'var(--bg-card)'}}/>}
    </span>
  );
}

// Catmull-Rom → cubic-bezier smoothing. The curve passes through every point,
// so the hover dot (placed at a raw data point) sits exactly on the line.
function smoothLine(pts, tension = 0.18) {
  if (pts.length < 2) return '';
  if (pts.length === 2) return `M${pts[0][0].toFixed(1)},${pts[0][1].toFixed(1)} L${pts[1][0].toFixed(1)},${pts[1][1].toFixed(1)}`;
  let d = `M${pts[0][0].toFixed(1)},${pts[0][1].toFixed(1)}`;
  for (let i = 0; i < pts.length - 1; i++) {
    const p0 = pts[i ? i - 1 : 0], p1 = pts[i], p2 = pts[i + 1], p3 = pts[i + 2 < pts.length ? i + 2 : pts.length - 1];
    const c1x = p1[0] + (p2[0] - p0[0]) * tension, c1y = p1[1] + (p2[1] - p0[1]) * tension;
    const c2x = p2[0] - (p3[0] - p1[0]) * tension, c2y = p2[1] - (p3[1] - p1[1]) * tension;
    d += ` C${c1x.toFixed(1)},${c1y.toFixed(1)} ${c2x.toFixed(1)},${c2y.toFixed(1)} ${p2[0].toFixed(1)},${p2[1].toFixed(1)}`;
  }
  return d;
}

function TokenCompareCard() {
  const t = useT();
  const [a, setA] = useState('VAL');
  const [b, setB] = useState('XOR');
  const [win, setWin] = useState('30d');
  const [data, setData] = useState(null);   // { aSeries:[{t,p}], bSeries:[{t,p}] }
  const [loading, setLoading] = useState(false);
  const [hover, setHover] = useState(null);  // index into ratio[]
  const gid = 'cmp' + a + b;                 // stable, unique gradient id per pair

  const idA = cmpTok(a).id, idB = cmpTok(b).id;
  const colA = cmpTok(a).color || '#10B981', colB = cmpTok(b).color || '#60A5FA';
  const apiWin = COMPARE_WINDOWS.find(([lbl]) => lbl === win)?.[1] || '30d';

  useEffect(() => {
    if (!idA || !idB || idA === idB) { setData(null); return; }
    let cancelled = false;
    setLoading(true);
    fetch(`/tools/price-series?assets=${idA},${idB}&window=${apiWin}`)
      .then(r => r.ok ? r.json() : null)
      .then(j => {
        if (cancelled || !j?.series) { if (!cancelled) { setData(null); setLoading(false); } return; }
        setData({ aSeries: j.series[idA] || [], bSeries: j.series[idB] || [] });
        setLoading(false);
      })
      .catch(() => { if (!cancelled) { setData(null); setLoading(false); } });
    return () => { cancelled = true; };
  }, [idA, idB, apiWin]);

  // Ratio series A/B aligned on shared timestamps, carrying both raw USD prices
  // so the hover readout can show "1 A = x B · A $p · B $q".
  const ratio = useMemo(() => {
    if (!data) return [];
    const bMap = new Map(data.bSeries.map(pt => [pt.t, pt.p]));
    const out = [];
    for (const pt of data.aSeries) {
      const bp = bMap.get(pt.t);
      if (bp != null && bp > 0 && pt.p > 0) out.push({ t: pt.t, r: pt.p / bp, pa: pt.p, pb: bp });
    }
    return out;
  }, [data]);

  const stats = useMemo(() => {
    if (ratio.length === 0) return null;
    let min = Infinity, max = -Infinity, iMin = 0, iMax = 0;
    ratio.forEach((p, i) => { if (p.r < min) { min = p.r; iMin = i; } if (p.r > max) { max = p.r; iMax = i; } });
    const first = ratio[0].r, last = ratio[ratio.length - 1].r;
    return { min, max, iMin, iMax, first, last, changePct: first > 0 ? (last - first) / first * 100 : 0 };
  }, [ratio]);

  // Geometry. Higher viewBox resolution + right margin for the price axis and
  // bottom margin for the time axis. The line uses non-scaling-stroke so it
  // renders at a fixed pixel width however wide the card gets — crisp, not fat.
  const W = 1000, H = 300, PADL = 6, PADR = 74, PADT = 16, PADB = 28;
  const plotR = W - PADR, plotB = H - PADB;
  const xAt = i => PADL + (i / Math.max(1, ratio.length - 1)) * (plotR - PADL);
  const yAt = r => { const span = (stats.max - stats.min) || Math.abs(stats.max) || 1; return PADT + (1 - (r - stats.min) / span) * (plotB - PADT); };

  const paths = useMemo(() => {
    if (ratio.length < 2 || !stats) return null;
    const pts = ratio.map((p, i) => [xAt(i), yAt(p.r)]);
    const line = smoothLine(pts, 0.1);   // gentle smoothing — hugs the data, minimal overshoot
    const area = line + ` L${pts[pts.length - 1][0].toFixed(1)},${plotB} L${pts[0][0].toFixed(1)},${plotB} Z`;
    return { line, area };
  }, [ratio, stats]);

  const hp = hover != null && ratio[hover] ? ratio[hover] : null;
  const up = stats && stats.changePct >= 0;
  const lineColor = up ? '#34D399' : '#F87171';
  const curRatio = hp ? hp.r : (stats ? stats.last : null);

  const TokenSelect = ({ value, onChange, exclude, accent }) => (
    <div style={{position:'relative', display:'inline-flex', alignItems:'center'}}>
      <span style={{position:'absolute', left:9, pointerEvents:'none', display:'inline-flex'}}><CmpLogo sym={value} size={20}/></span>
      <select value={value} onChange={e => onChange(e.target.value)} style={{
        padding:'8px 12px 8px 36px', fontSize:14, fontWeight:800, fontFamily:'JetBrains Mono, monospace',
        border:`1px solid ${accent}55`, borderRadius:10, background:'var(--bg-card)',
        color:'var(--fg-0)', outline:'none', cursor:'pointer', appearance:'none', boxShadow:`0 0 0 0 ${accent}`,
      }}>
        {COMPARE_TOKENS.filter(tk => tk.sym !== exclude).map(tk => (
          <option key={tk.sym} value={tk.sym}>{tk.sym}</option>
        ))}
      </select>
    </div>
  );

  return (
    <div className="card" style={{padding: 0, overflow:'hidden'}}>
      <div className="card-header">
        <div className="card-title"><span className="dot"/> {t('tools.compare.title', 'Comparador de precios')}</div>
        {stats && <span className="tag">{ratio.length} pts · {win}</span>}
      </div>
      <div style={{padding:'16px 20px'}}>
        {/* Token selectors + swap + window pills */}
        <div style={{display:'flex', alignItems:'center', gap:10, flexWrap:'wrap', marginBottom:16}}>
          <TokenSelect value={a} onChange={setA} exclude={b} accent={colA}/>
          <button onClick={() => { setA(b); setB(a); }} title={t('tools.compare.swap', 'Invertir')} style={{
            border:'1px solid var(--border-color)', borderRadius:9, padding:'8px 11px', cursor:'pointer',
            background:'rgba(255,255,255,0.04)', color:'var(--fg-1)', fontSize:15, lineHeight:1, transition:'transform .15s',
          }} onMouseEnter={e=>e.currentTarget.style.transform='rotate(180deg)'} onMouseLeave={e=>e.currentTarget.style.transform=''}>⇄</button>
          <TokenSelect value={b} onChange={setB} exclude={a} accent={colB}/>
          <div style={{flex:1}}/>
          <div style={{display:'inline-flex', gap:2, background:'rgba(255,255,255,0.04)', borderRadius:9, padding:3}}>
            {COMPARE_WINDOWS.map(([lbl]) => (
              <button key={lbl} onClick={() => setWin(lbl)} style={{
                padding:'5px 11px', fontSize:11, fontWeight: win===lbl?800:600, border:'none', borderRadius:6,
                cursor:'pointer', color: win===lbl?'#0b0b10':'var(--fg-2)', transition:'all .15s',
                background: win===lbl?lineColor:'transparent',
              }}>{lbl}</button>
            ))}
          </div>
        </div>

        {/* Hero readout: big ratio + change chip + dual conversion + live USD */}
        {stats && (
          <div style={{display:'flex', alignItems:'flex-end', gap:14, flexWrap:'wrap', marginBottom:14}}>
            <div>
              <div className="muted tiny" style={{display:'flex', alignItems:'center', gap:5, marginBottom:3}}>
                <CmpLogo sym={a} size={15}/> 1 {a} {t('tools.compare.equals','=')}
              </div>
              <div style={{display:'flex', alignItems:'baseline', gap:8}}>
                <span className="num" style={{fontSize:30, fontWeight:800, color: lineColor, lineHeight:1}}>{fmtRatio(curRatio)}</span>
                <span style={{display:'inline-flex', alignItems:'center', gap:4, fontWeight:800, fontSize:15, color:'var(--fg-1)'}}><CmpLogo sym={b} size={16}/> {b}</span>
              </div>
              <div className="muted tiny" style={{marginTop:5}}>{t('tools.compare.inverse','inverso')}: 1 {b} = <span className="num" style={{fontWeight:700, color:'var(--fg-1)'}}>{fmtRatio(curRatio ? 1/curRatio : null)}</span> {a}</div>
            </div>
            <div style={{flex:1}}/>
            {!hp && (
              <span style={{
                display:'inline-flex', alignItems:'center', gap:4, padding:'5px 11px', borderRadius:20, fontWeight:800, fontSize:13,
                color: up ? '#34D399' : '#F87171', background: up ? 'rgba(52,211,153,0.13)' : 'rgba(248,113,113,0.13)',
              }}>{up ? '▲' : '▼'} {Math.abs(stats.changePct).toFixed(2)}% <span style={{fontWeight:500, opacity:0.7}}>· {win}</span></span>
            )}
            {hp && <div className="muted tiny" style={{textAlign:'right'}}>{new Date(hp.t * 1000).toLocaleDateString(undefined,{day:'2-digit',month:'short',year:'2-digit'})}<br/>{new Date(hp.t * 1000).toLocaleTimeString(undefined,{hour:'2-digit',minute:'2-digit'})}</div>}
          </div>
        )}

        {/* Live USD prices of each token */}
        {stats && (
          <div style={{display:'flex', gap:18, marginBottom:14, flexWrap:'wrap'}}>
            <div style={{display:'flex', alignItems:'center', gap:7}}>
              <CmpLogo sym={a} size={22}/>
              <div><div className="muted tiny">{a} {t('tools.compare.usd','USD')}</div><div className="num" style={{fontWeight:700, fontSize:14}}>{fmtUsdPrice(hp ? hp.pa : ratio[ratio.length-1]?.pa)}</div></div>
            </div>
            <div style={{display:'flex', alignItems:'center', gap:7}}>
              <CmpLogo sym={b} size={22}/>
              <div><div className="muted tiny">{b} {t('tools.compare.usd','USD')}</div><div className="num" style={{fontWeight:700, fontSize:14}}>{fmtUsdPrice(hp ? hp.pb : ratio[ratio.length-1]?.pb)}</div></div>
            </div>
          </div>
        )}

        {/* Chart */}
        <div style={{position:'relative', width:'100%'}}>
          {loading && <div className="muted tiny" style={{padding:'80px 0', textAlign:'center'}}>{t('tools.compare.loading', 'Cargando histórico…')}</div>}
          {!loading && idA === idB && <div className="muted tiny" style={{padding:'80px 0', textAlign:'center', color:'#F59E0B'}}>{t('tools.compare.same', 'Elige dos tokens distintos.')}</div>}
          {!loading && idA !== idB && ratio.length < 2 && <div className="muted tiny" style={{padding:'80px 0', textAlign:'center'}}>{t('tools.compare.nodata', 'Sin histórico solapado para este par.')}</div>}
          {!loading && ratio.length >= 2 && stats && paths && (
            <svg viewBox={`0 0 ${W} ${H}`} style={{width:'100%', height:'auto', display:'block', cursor:'crosshair'}}
                 onMouseMove={e => {
                   const rect = e.currentTarget.getBoundingClientRect();
                   const fx = (e.clientX - rect.left) / rect.width;
                   setHover(Math.max(0, Math.min(ratio.length - 1, Math.round(fx * (ratio.length - 1)))));
                 }}
                 onMouseLeave={() => setHover(null)}>
              <defs>
                <linearGradient id={gid} x1="0" y1="0" x2="0" y2="1">
                  <stop offset="0%" stopColor={lineColor} stopOpacity="0.26"/>
                  <stop offset="60%" stopColor={lineColor} stopOpacity="0.06"/>
                  <stop offset="100%" stopColor={lineColor} stopOpacity="0"/>
                </linearGradient>
                <filter id={gid+'g'} x="-5%" y="-40%" width="110%" height="180%">
                  <feGaussianBlur stdDeviation="2.2"/>
                </filter>
              </defs>
              {/* price axis — 5 ticks min→max, value labels in the right margin */}
              {Array.from({length:5}, (_,i) => {
                const val = stats.min + (stats.max - stats.min) * (i/4), y = yAt(val), edge = i===0 || i===4;
                return <g key={'p'+i}>
                  <line x1={PADL} x2={plotR} y1={y} y2={y} stroke="var(--fg-1)" strokeOpacity={edge?0.13:0.045} strokeWidth="1" strokeDasharray={edge?'none':'2 6'} vectorEffect="non-scaling-stroke"/>
                  <text x={plotR+6} y={y+3} fontSize="10" fill="var(--fg-2)" opacity="0.65" className="num">{fmtRatio(val)}</text>
                </g>;
              })}
              {/* time axis — 5 date ticks along the bottom */}
              {Array.from({length:5}, (_,i) => {
                const idx = Math.round((i/4) * (ratio.length-1)), x = xAt(idx), dt = new Date(ratio[idx].t*1000);
                const lbl = (win==='1y' || win==='all') ? dt.toLocaleDateString(undefined,{month:'short',year:'2-digit'}) : dt.toLocaleDateString(undefined,{day:'2-digit',month:'short'});
                return <text key={'t'+i} x={x} y={H-8} fontSize="10" fill="var(--fg-2)" opacity="0.6" textAnchor={i===0?'start':i===4?'end':'middle'}>{lbl}</text>;
              })}
              {/* area + soft glow underlay (scales) + crisp thin line on top */}
              <path d={paths.area} fill={`url(#${gid})`}/>
              <path d={paths.line} fill="none" stroke={lineColor} strokeWidth="2.2" strokeLinecap="round" strokeLinejoin="round" filter={`url(#${gid}g)`} opacity="0.45"/>
              <path d={paths.line} fill="none" stroke={lineColor} strokeWidth="1.4" strokeLinecap="round" strokeLinejoin="round" vectorEffect="non-scaling-stroke"/>
              {/* min & max dots */}
              <circle cx={xAt(stats.iMax)} cy={yAt(stats.max)} r="2.6" fill={lineColor} stroke="var(--bg-card)" strokeWidth="1" vectorEffect="non-scaling-stroke"/>
              <circle cx={xAt(stats.iMin)} cy={yAt(stats.min)} r="2.6" fill={lineColor} stroke="var(--bg-card)" strokeWidth="1" vectorEffect="non-scaling-stroke"/>
              {/* hover crosshair (both axes) + value pill on the price axis */}
              {hp && (() => {
                const x = xAt(hover), y = yAt(hp.r);
                return <g>
                  <line x1={x} y1={PADT} x2={x} y2={plotB} stroke="var(--fg-1)" strokeWidth="1" strokeDasharray="3 3" strokeOpacity="0.4" vectorEffect="non-scaling-stroke"/>
                  <line x1={PADL} y1={y} x2={plotR} y2={y} stroke={lineColor} strokeWidth="1" strokeDasharray="3 3" strokeOpacity="0.45" vectorEffect="non-scaling-stroke"/>
                  <circle cx={x} cy={y} r="6" fill={lineColor} opacity="0.16"/>
                  <circle cx={x} cy={y} r="3" fill={lineColor} stroke="var(--bg-card)" strokeWidth="1.5" vectorEffect="non-scaling-stroke"/>
                  <rect x={plotR+2} y={y-8} width={PADR-4} height="16" rx="3" fill={lineColor}/>
                  <text x={plotR+PADR/2} y={y+3.5} fontSize="9.5" fill="#0b0b10" fontWeight="700" textAnchor="middle" className="num">{fmtRatio(hp.r)}</text>
                </g>;
              })()}
            </svg>
          )}
        </div>

        <div className="muted tiny" style={{marginTop:14, fontSize:10, lineHeight:1.5, opacity:0.55}}>
          {t('tools.compare.caveat', 'Ratio = precio {a} ÷ precio {b} por hora, del histórico real de precios (price_history). Sin estimaciones.').replace('{a}', a).replace('{b}', b)}
        </div>
      </div>
    </div>
  );
}

function ToolsSection() {
  const t = useT();
  return (
    <div>
      <PageHeader title={t('tools.title', 'Tools')}/>
      <div style={{display:'grid', gap:16}}>
        <TokenCompareCard/>
        <PredictionBlockCard/>
        {/* More tool cards will go here as they get added. */}
      </div>
    </div>
  );
}

Object.assign(window, { ToolsSection, PredictionBlockCard });
