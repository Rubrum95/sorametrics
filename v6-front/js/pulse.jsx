/* global React, fmt, FAKE_ADDRS, IDENTITIES, TOKENS, sparkPath, I, useDrill, useT */
const { useState, useEffect, useRef, useMemo } = React;

function Sparkline({ data, w = 70, h = 28, color = '#E5243B' }) {
  const path = sparkPath(data, w, h, 2);
  const last = data[data.length-1];
  const first = data[0];
  const up = last >= first;
  const t = useT();
 return (
    <svg viewBox={`0 0 ${w} ${h}`} width={w} height={h} className="sparkline">
      <path d={path} stroke={up ? '#10B981' : '#EF4444'} strokeWidth="1.5" fill="none"/>
    </svg>
  );
}

function PulseStat({ label, value, sub, delta, deltaPositive, spark, color }) {
  return (
    <div className="pulse-stat">
      <div className="stat">
        <span className="stat-label">{label}</span>
        <span className="stat-value num">{value}</span>
        <div className="row">
          {delta && (
            <span className={'stat-delta ' + (deltaPositive ? 'up' : 'down')}>
              {deltaPositive ? '▲' : '▼'} {delta}
            </span>
          )}
          {sub && <span className="stat-sub">{sub}</span>}
        </div>
      </div>
      {spark && <Sparkline data={spark} color={color}/>}
    </div>
  );
}

const KINDS = [
  { id: 'swap',     label: 'Swap' },
  { id: 'transfer', label: 'Transfer' },
  { id: 'block',    label: 'Block' },
  { id: 'order',    label: 'Order' },
  { id: 'burn',     label: 'Burn' },
];

function generateEvent(id, seedRand) {
  const kinds = ['swap', 'transfer', 'block', 'order', 'burn'];
  const k = kinds[Math.floor(seedRand() * kinds.length)];
  const from = FAKE_ADDRS[Math.floor(seedRand() * FAKE_ADDRS.length)];
  const to = FAKE_ADDRS[Math.floor(seedRand() * FAKE_ADDRS.length)];
  const tokenKeys = Object.keys(TOKENS);
  const tA = tokenKeys[Math.floor(seedRand() * tokenKeys.length)];
  let tB = tokenKeys[Math.floor(seedRand() * tokenKeys.length)];
  if (tB === tA) tB = 'KUSD';
  const amt = seedRand() * 10000 + 1;
  const ts = Date.now();

  if (k === 'swap') return {
    id, kind: k, ts,
    line1: <>Swap <b>{fmt.num(amt,2)} {tA}</b> → <b>{fmt.num(amt * (seedRand() + 0.5), 2)} {tB}</b></>,
    line2: `${fmt.addr(from)} · fee 0.3% · ${fmt.usd(amt * 0.5)}`,
  };
  if (k === 'transfer') return {
    id, kind: k, ts,
    line1: <>Transfer <b>{fmt.num(amt,2)} {tA}</b></>,
    line2: `${fmt.addr(from)} → ${fmt.addr(to)}`,
  };
  if (k === 'block') return {
    id, kind: k, ts,
    line1: <>Block <b>#{Math.floor(21_400_000 + seedRand() * 1000).toLocaleString()}</b> finalized</>,
    line2: `${Math.floor(seedRand()*80)} extrinsics · validator ${IDENTITIES[from] || fmt.addr(from)}`,
  };
  if (k === 'order') return {
    id, kind: k, ts,
    line1: <><b>{seedRand() > 0.5 ? 'BUY' : 'SELL'}</b> order <b>{fmt.num(amt,0)} {tA}</b> @ ${(seedRand()*0.5+0.05).toFixed(4)}</>,
    line2: `${tA}/${tB} · ${fmt.addr(from)}`,
  };
  return {
    id, kind: k, ts,
    line1: <><b>{fmt.num(amt * 0.01, 2)} XOR</b> burned · network fees</>,
    line2: `from block reward distribution`,
  };
}

function PulseSection({ tweaks }) {
  const { open } = useDrill();
  const [filter, setFilter] = useState('all');
  const [events, setEvents] = useState(() => {
    const rand = seededRand(7);
    return Array.from({length: 14}, (_, i) => {
      const e = generateEvent('seed-' + i, rand);
      e.ts = Date.now() - (14 - i) * 3000;
      return e;
    }).reverse();
  });
  const idRef = useRef(100);
  const [tick, setTick] = useState(0);

  // push new events
  useEffect(() => {
    const rand = seededRand(Date.now());
    const id = setInterval(() => {
      const ev = generateEvent('e-' + (++idRef.current), Math.random);
      setEvents(prev => [ev, ...prev].slice(0, 40));
    }, 1400 / (tweaks.liveSpeed || 1));
    return () => clearInterval(id);
  }, [tweaks.liveSpeed]);

  // re-render clock for "ago"
  useEffect(() => {
    const id = setInterval(() => setTick(t => t + 1), 1000);
    return () => clearInterval(id);
  }, []);

  const counts = useMemo(() => {
    const c = { all: events.length };
    events.forEach(e => { c[e.kind] = (c[e.kind] || 0) + 1; });
    return c;
  }, [events]);

  const filtered = filter === 'all' ? events : events.filter(e => e.kind === filter);

  const sparkA = useMemo(() => Array.from({length: 30}, (_, i) => 50 + Math.sin(i/3) * 20 + Math.random()*10), []);
  const sparkB = useMemo(() => Array.from({length: 30}, (_, i) => 100 + Math.cos(i/4) * 30 + Math.random()*15), []);
  const sparkC = useMemo(() => Array.from({length: 30}, (_, i) => 30 + i * 1.2 + Math.random()*8), []);
  const sparkD = useMemo(() => Array.from({length: 30}, (_, i) => 80 - i * 0.6 + Math.sin(i/2)*6), []);

  return (
    <div>
      <PageHeader title={t('pulse.title')} sub={t('pulse.sub')}>
        <span className="tag ok"><span className="live-dot" style={{width:5,height:5}}/> {t('common.connected')}</span>
        <button className="btn">{t('common.pause')}</button>
        <button className="btn primary">{t('btn.fullExplorer')}</button>
      </PageHeader>

      <div className="pulse-grid">
        <PulseStat label={t('pulse.kpi.swaps24')} value="14,208" delta="8.4%" deltaPositive={true} sub="vs 7d avg" spark={sparkA}/>
        <PulseStat label={t('pulse.kpi.volume')} value="$4.27M" delta="12.1%" deltaPositive={true} sub="last 24h" spark={sparkB}/>
        <PulseStat label={t('pulse.kpi.wallets')} value="2,810" delta="3.2%" deltaPositive={true} sub="unique signers" spark={sparkC}/>
        <PulseStat label={t('pulse.kpi.block')} value="6.01s" delta="0.4%" deltaPositive={false} sub="finality 12s" spark={sparkD}/>
      </div>

      <div className="pulse-layout">
        <div className="card">
          <div className="card-header">
            <div className="card-title"><span className="dot"/> Live feed</div>
            <div className="row">
              <span className="tag">{events.length} events</span>
            </div>
          </div>
          <div className="filter-row">
            <div className={'filter-chip' + (filter === 'all' ? ' active' : '')}
                 onClick={() => setFilter('all')}>All <span className="n">{counts.all || 0}</span></div>
            {KINDS.map(k => (
              <div key={k.id}
                   className={'filter-chip' + (filter === k.id ? ' active' : '')}
                   onClick={() => setFilter(k.id)}>
                {k.label} <span className="n">{counts[k.id] || 0}</span>
              </div>
            ))}
          </div>
          <div className="feed">
            {filtered.map(e => (
              <div className="feed-item clickable" key={e.id}
                   onClick={() => open({
                     type: e.kind, title: e.kind.toUpperCase() + ' · ' + fmt.ago(e.ts) + ' ago',
                     ts: e.ts, hash: '0x' + Math.random().toString(16).slice(2, 18),
                     block: 21418802 + (e.id.toString().length), caller: FAKE_ADDRS[0],
                     inSym:'XOR', outSym:'VAL', inAmt: 12.4, outAmt: 1.8,
                     sym: 'XOR', amt: 12.4, usd: 124, from: FAKE_ADDRS[0], to: FAKE_ADDRS[1],
                     num: 21418802, validator: 'Sakura Node', extrinsics: 48, weight: 42,
                     side: 'buy', pair: 'KUSD/XOR', size: 1500, price: 0.42, filled: 62,
                   })}>
                <span className={'feed-kind ' + e.kind}>{e.kind}</span>
                <div className="feed-body">
                  <div className="line1">{e.line1}</div>
                  <div className="line2">{e.line2}</div>
                </div>
                <div className="feed-time">{fmt.ago(e.ts)}</div>
              </div>
            ))}
          </div>
        </div>

        <div style={{display:'grid', gap: 18, alignContent: 'start'}}>
          <div className="card">
            <div className="card-header">
              <div className="card-title"><span className="dot"/> {t('pulse.trending')}</div>
              <span className="tag">Top 6</span>
            </div>
            <div className="card-body">
              {['XOR', 'VAL', 'PSWAP', 'TBCD', 'KUSD', 'ETH'].map((sym, i) => {
                const tk = TOKENS[sym];
                const change = [3.4, 8.2, -1.8, 0.3, 0.01, 5.6][i];
                return (
                  <div key={sym} className="holder-row" style={{ gridTemplateColumns: '32px 1fr 70px 70px 56px' }}>
                    <div className="token-logo" style={{ background: tk.grad, width: 24, height: 24, fontSize: 10 }}>{sym[0]}</div>
                    <div className="holder-addr"><span className="ident">{sym}</span> <span className="muted tiny">· {tk.name}</span></div>
                    <svg className="mini-spark" viewBox="0 0 64 24" width="64" height="24">
                      <path d={sparkPath(Array.from({length:20}, (_, j) => 10 + Math.sin(j/2 + i) * 4 + Math.random()*3), 64, 24, 2)}
                            stroke={change >= 0 ? '#10B981' : '#EF4444'} strokeWidth="1.3" fill="none"/>
                    </svg>
                    <div className="holder-pct" style={{ color: change >= 0 ? '#6EE7B7' : '#FCA5A5' }}>
                      {change >= 0 ? '+' : ''}{change.toFixed(2)}%
                    </div>
                    <div className="holder-pct num">${(0.002 + i*0.015).toFixed(4)}</div>
                  </div>
                );
              })}
            </div>
          </div>

          <div className="card">
            <div className="card-header">
              <div className="card-title"><span className="dot"/> {t('pulse.health')}</div>
            </div>
            <div className="card-body" style={{display:'grid', gap:10}}>
              {[
                { l: 'Validators Online', v: '187 / 192', ok: true },
                { l: 'Peers',              v: '42',        ok: true },
                { l: 'Era Progress',       v: '62%',       ok: true, bar: 62 },
                { l: 'Finality Lag',       v: '2 blocks',  ok: true },
                { l: 'TPS (est.)',         v: '23.4',      ok: true },
              ].map((r, i) => (
                <div key={i} style={{display:'flex', alignItems:'center', gap: 10, fontSize: 13}}>
                  <span style={{flex: 1, color: 'var(--fg-2)'}}>{r.l}</span>
                  {r.bar && (
                    <div className="holder-bar" style={{ width: 80 }}>
                      <div className="fill" style={{ width: r.bar + '%' }}/>
                    </div>
                  )}
                  <span className="num" style={{ color: r.ok ? '#6EE7B7' : '#FCA5A5', fontWeight: 700 }}>{r.v}</span>
                </div>
              ))}
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}

Object.assign(window, { PulseSection });
