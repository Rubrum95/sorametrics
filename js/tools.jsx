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

function ToolsSection() {
  const t = useT();
  return (
    <div>
      <PageHeader title={t('tools.title', 'Tools')}/>
      <div style={{display:'grid', gap:16}}>
        <PredictionBlockCard/>
        {/* More tool cards will go here as they get added. */}
      </div>
    </div>
  );
}

Object.assign(window, { ToolsSection, PredictionBlockCard });
