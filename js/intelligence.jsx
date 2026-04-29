/* global React, useT, fmt, PageHeader, TokenLogo, TinyTokLogo, IDENTITIES, useDrill */
// Intelligence — real-data insight dashboard. Each widget is self-contained:
// it fetches its own inputs, computes a severity (ok / warn / alert), and
// renders a card with the signal and the raw numbers behind it.
//
// Tiers:
//   1. Deterministic alerts (peg, concentration, whales, bridge flow)
//   2. Algorithmic signals (divergence, fee/TPS spikes, validator health)
//   3. Extras (governance pulse, new listings, cross-DEX arbitrage)
//
// No synthetic placeholders; if an endpoint fails the widget surfaces "—".
const { useState, useEffect, useMemo, useRef } = React;

// ---------- shared primitives ----------
function Severity({ level }) {
  const map = { ok:'#10B981', warn:'#F59E0B', alert:'#EF4444', none:'#6B7280' };
  return <span style={{display:'inline-block', width:8, height:8, borderRadius:'50%', background: map[level] || map.none}}/>;
}

function WidgetCard({ title, severity = 'none', tag, children }) {
  const borderColor = { ok:'rgba(16,185,129,0.25)', warn:'rgba(245,158,11,0.3)', alert:'rgba(239,68,68,0.35)', none:'rgba(255,255,255,0.08)' };
  return (
    <div className="card" style={{borderColor: borderColor[severity]}}>
      <div className="card-header">
        <div className="card-title" style={{display:'flex', alignItems:'center', gap:8}}>
          <Severity level={severity}/>
          <span>{title}</span>
        </div>
        {tag && <span className="tag">{tag}</span>}
      </div>
      <div className="card-body">{children}</div>
    </div>
  );
}

// ---------- TIER 1 ----------

// Peg Monitor — keeps only stables with a $1 target (KUSD, XSTUSD, DAI).
// TBCD is excluded because it's a TBC dollar whose target is its supply-based
// formula price, not $1, so treating it as depegged at $4k is misleading.
function PegMonitor() {
  const STABLE_TARGETS = { KUSD: 1, XSTUSD: 1, DAI: 1 };
  const [data, setData] = useState(null);
  useEffect(() => {
    let cancelled = false;
    const pull = () => fetch('/stats/stablecoins').then(r => r.ok ? r.json() : null).then(j => {
      if (cancelled) return;
      const arr = Array.isArray(j) ? j : [];
      setData(arr.filter(s => STABLE_TARGETS[s.symbol] != null));
    }).catch(() => {});
    pull();
    const id = setInterval(pull, 60_000);
    return () => { cancelled = true; clearInterval(id); };
  }, []);

  const worst = (data || []).reduce((max, s) => {
    const dev = Math.abs((Number(s.price) || 1) - STABLE_TARGETS[s.symbol]) / STABLE_TARGETS[s.symbol];
    return dev > (max?.dev || 0) ? { symbol: s.symbol, dev, price: s.price } : max;
  }, null);
  const severity = !worst ? 'none' : worst.dev > 0.02 ? 'alert' : worst.dev > 0.005 ? 'warn' : 'ok';

  return (
    <WidgetCard
      title="Peg Monitor"
      severity={severity}
      tag={worst ? (worst.dev > 0.02 ? worst.symbol + ' DEPEG ' + (worst.dev * 100).toFixed(1) + '%' : 'within range') : '…'}>
      {!data && <div className="muted tiny">Cargando…</div>}
      {data && data.length === 0 && <div className="muted tiny">Sin stablecoins monitorizados.</div>}
      {data && data.length > 0 && (
        <div style={{display:'grid', gap:10}}>
          {data.map(s => {
            const target = STABLE_TARGETS[s.symbol];
            const price = Number(s.price) || 0;
            const devPct = ((price - target) / target) * 100;
            const abs = Math.abs(devPct);
            const rowSev = abs > 2 ? 'alert' : abs > 0.5 ? 'warn' : 'ok';
            return (
              <div key={s.symbol} style={{display:'grid', gridTemplateColumns:'14px 80px 1fr 90px 120px', alignItems:'center', gap:10}}>
                <Severity level={rowSev}/>
                <div style={{fontWeight:700}}>{s.symbol}</div>
                <div style={{position:'relative', height:6, background:'rgba(255,255,255,0.06)', borderRadius:3}}>
                  <div style={{position:'absolute', left:'50%', top:-3, width:1, height:12, background:'rgba(255,255,255,0.3)'}}/>
                  <div style={{
                    position:'absolute',
                    left: devPct >= 0 ? '50%' : (50 - Math.min(abs, 10) * 5) + '%',
                    width: Math.min(abs, 10) * 5 + '%',
                    top: 0, bottom: 0,
                    background: rowSev === 'alert' ? '#EF4444' : rowSev === 'warn' ? '#F59E0B' : '#10B981',
                    borderRadius: 3,
                  }}/>
                </div>
                <div className="num" style={{textAlign:'right', fontWeight:600}}>${price.toFixed(4)}</div>
                <div style={{textAlign:'right'}}>
                  <span className={'tag ' + (rowSev === 'alert' ? 'err' : rowSev === 'ok' ? 'ok' : '')}>
                    {devPct >= 0 ? '+' : ''}{devPct.toFixed(2)}%
                  </span>
                </div>
              </div>
            );
          })}
        </div>
      )}
    </WidgetCard>
  );
}

// Concentration Risk — top 10 share + Gini for the 4 main SORA tokens.
// Uses the Holders cache so we don't re-hit the VPS per intel refresh.
function ConcentrationRisk() {
  const ASSETS = [
    { sym:'XOR',   id:'0x0200000000000000000000000000000000000000000000000000000000000000' },
    { sym:'VAL',   id:'0x0200040000000000000000000000000000000000000000000000000000000000' },
    { sym:'PSWAP', id:'0x0200050000000000000000000000000000000000000000000000000000000000' },
    { sym:'KUSD',  id:'0x02000c0000000000000000000000000000000000000000000000000000000000' },
  ];
  const [rows, setRows] = useState(null);
  useEffect(() => {
    let cancelled = false;
    (async () => {
      const getCached = window.getHoldersCached;
      if (!getCached) { setRows([]); return; }
      // Serial fetch — rides the same queue as the Holders section, so parallel
      // calls from both pages collapse onto a single in-flight request per asset.
      const out = [];
      for (const a of ASSETS) {
        if (cancelled) return;
        const { data } = await getCached(a.id, 1, 100);
        if (!data || !Array.isArray(data.data)) { out.push({ ...a, err: true }); continue; }
        const balances = data.data.map(r => Number(r.balance) || 0).filter(b => b > 0);
        const total = balances.reduce((s, b) => s + b, 0) || 1;
        const top10 = balances.slice(0, 10).reduce((s, b) => s + b, 0);
        const top10Pct = (top10 / total) * 100;
        // Gini coefficient on the page-1 sample. Not the true global Gini but a
        // solid proxy since page 1 is the top-N holders by balance.
        const sorted = [...balances].sort((x, y) => x - y);
        let num = 0;
        for (let i = 0; i < sorted.length; i++) {
          num += (2 * (i + 1) - sorted.length - 1) * sorted[i];
        }
        const gini = num / (sorted.length * sorted.reduce((s, b) => s + b, 0) || 1);
        out.push({ ...a, top10Pct, gini: Math.abs(gini), totalHolders: data.totalHolders || data.total || balances.length });
      }
      if (!cancelled) setRows(out);
    })();
    return () => { cancelled = true; };
  }, []);

  const worst = (rows || []).reduce((max, r) => !r.err && r.top10Pct > (max?.top10Pct || 0) ? r : max, null);
  const severity = !worst ? 'none' : worst.top10Pct > 80 ? 'alert' : worst.top10Pct > 60 ? 'warn' : 'ok';

  return (
    <WidgetCard title="Concentration Risk" severity={severity} tag={worst ? worst.sym + ' top10: ' + worst.top10Pct.toFixed(0) + '%' : '…'}>
      {!rows && <div className="muted tiny">Calculando…</div>}
      {rows && rows.map(r => {
        if (r.err) return <div key={r.sym} className="muted tiny">{r.sym}: no disponible</div>;
        const rowSev = r.top10Pct > 80 ? 'alert' : r.top10Pct > 60 ? 'warn' : 'ok';
        return (
          <div key={r.sym} style={{display:'grid', gridTemplateColumns:'14px 60px 1fr 80px 80px', alignItems:'center', gap:10, padding:'6px 0'}}>
            <Severity level={rowSev}/>
            <div style={{fontWeight:700}}>{r.sym}</div>
            <div style={{position:'relative', height:6, background:'rgba(255,255,255,0.06)', borderRadius:3}}>
              <div style={{
                position:'absolute', left:0, top:0, bottom:0,
                width: r.top10Pct + '%',
                background: rowSev === 'alert' ? '#EF4444' : rowSev === 'warn' ? '#F59E0B' : '#10B981',
                borderRadius: 3,
              }}/>
            </div>
            <div className="num tiny" style={{textAlign:'right'}}>Gini {r.gini.toFixed(2)}</div>
            <div className="num" style={{textAlign:'right', fontWeight:600}}>{r.top10Pct.toFixed(1)}%</div>
          </div>
        );
      })}
      {rows && <div className="muted tiny" style={{marginTop:8}}>Top 10 wallets control X% del supply on-chain. Umbral alert ≥ 80%.</div>}
    </WidgetCard>
  );
}

// Whale Activity — flag txs above a threshold (default $10K) in last 24h.
// Bridge Net Flow Bar — replaces the old WhaleActivity widget.
// Horizontal red-green bar (like PegMonitor's deviation bar) whose center
// moves left when net flow is OUT of SORA and right when net flow is IN.
// Below the bar we surface the top token by inflow and the top token by
// outflow for the selected window.
//
// Why bar and not net number alone: the user wants to eyeball "is SORA
// bleeding liquidity or accumulating it?" without reading digits. A single
// red-green bar with a marker is the same visual grammar used for peg
// deviation, so it reads instantly.
//
// Source: /history/global/bridges, paginated DESC until we cross the
// timeframe cutoff (or hit a safety cap). For "all" we just walk the full
// paginated dataset up to the hard cap (20 pages × 100 = 2000 bridges).
function WhaleActivity() {
  const tt = useT();
  const [tf, setTf] = useState('1d');
  const [agg, setAgg] = useState(null);
  const [loading, setLoading] = useState(false);

  const WINDOWS = {
    '4h':  4 * 3_600_000,
    '1d':  86_400_000,
    '7d':  604_800_000,
    '1m':  2_592_000_000,
    '1y':  31_536_000_000,
    'all': Number.POSITIVE_INFINITY,
  };
  const MAX_PAGES = 20; // hard cap on /history/global/bridges walk

  useEffect(() => {
    let cancelled = false;
    setLoading(true);
    (async () => {
      const windowMs = WINDOWS[tf];
      const cutoff = windowMs === Number.POSITIVE_INFINITY ? 0 : Date.now() - windowMs;
      let inUsd = 0, outUsd = 0;
      const perSymIn = {};   // sym → total USD inflow
      const perSymOut = {};  // sym → total USD outflow
      let totalTxs = 0;

      for (let page = 1; page <= MAX_PAGES; page++) {
        const j = await fetch('/history/global/bridges?limit=100&page=' + page)
          .then(r => r.ok ? r.json() : null).catch(() => null);
        if (cancelled) return;
        const list = Array.isArray(j) ? j : (j?.data || []);
        if (list.length === 0) break;
        let reachedCutoff = false;
        for (const b of list) {
          const ts = Number(b.timestamp);
          if (!Number.isFinite(ts)) continue;
          if (ts < cutoff) { reachedCutoff = true; break; }
          const usd = Number(b.usd_value) || 0;
          if (usd <= 0) continue;
          const sym = b.symbol || '?';
          const isIn = String(b.direction || '').trim().toLowerCase() === 'incoming';
          if (isIn) {
            inUsd += usd;
            perSymIn[sym] = (perSymIn[sym] || 0) + usd;
          } else {
            outUsd += usd;
            perSymOut[sym] = (perSymOut[sym] || 0) + usd;
          }
          totalTxs++;
        }
        if (reachedCutoff) break;
        if (list.length < 100) break;
      }
      if (cancelled) return;

      const topIn  = Object.entries(perSymIn).sort((a, b) => b[1] - a[1])[0] || null;
      const topOut = Object.entries(perSymOut).sort((a, b) => b[1] - a[1])[0] || null;
      setAgg({
        inUsd, outUsd,
        net: inUsd - outUsd,
        totalTxs,
        topIn:  topIn  ? { sym: topIn[0],  usd: topIn[1] }  : null,
        topOut: topOut ? { sym: topOut[0], usd: topOut[1] } : null,
      });
      setLoading(false);
    })();
    return () => { cancelled = true; };
  }, [tf]);

  // Bar position: mapping net to [0,100]%.
  //   · 50% = balanced (in == out or nothing happened)
  //   · 100% = 100% inflow / 0% outflow
  //   · 0%   = 100% outflow / 0% inflow
  // Total = in + out. Share of in = in / total. Bar fill = share_of_in × 100.
  const total = agg ? (agg.inUsd + agg.outUsd) : 0;
  const markerPct = total > 0 ? (agg.inUsd / total) * 100 : 50;
  const netColor = agg && agg.net >= 0 ? '#10B981' : '#EF4444';
  const tag = agg
    ? ((agg.net >= 0 ? '+' : '−') + '$' + Math.abs(agg.net).toLocaleString(undefined, { maximumFractionDigits: 0 }))
    : '…';

  const tfLabels = {
    '4h': '4h', '1d': '1d', '7d': '7d', '1m': '1m', '1y': '1y',
    'all': tt('intel.bflow.tfAll', 'All'),
  };

  return (
    <WidgetCard title={tt('intel.bflow.title', 'Bridge net flow')} severity="ok" tag={tag}>
      {/* Timeframe selector */}
      <div style={{display:'flex', gap:4, marginBottom:12, flexWrap:'wrap'}}>
        {['4h', '1d', '7d', '1m', '1y', 'all'].map(k => (
          <button
            key={k}
            onClick={() => setTf(k)}
            className={'status-opt' + (tf === k ? ' active' : '')}
            style={{padding:'3px 10px', fontSize:11}}>
            {tfLabels[k]}
          </button>
        ))}
      </div>

      {(loading || !agg) && <div className="muted tiny">{tt('common.loading', 'Loading…')}</div>}
      {agg && !loading && total === 0 && (
        <div className="muted tiny">{tt('intel.bflow.noFlow', 'No bridge flow in this window.')}</div>
      )}
      {agg && !loading && total > 0 && (
        <>
          {/* Horizontal red-green bar with marker. Same visual language as
              PegMonitor: left half red (outflow), right half green (inflow),
              dark marker line at the current ratio. */}
          <div style={{position:'relative', height:10, borderRadius:5, overflow:'hidden', marginBottom:6, background:'linear-gradient(90deg,#EF4444 0%,#EF4444 50%,#10B981 50%,#10B981 100%)'}}>
            <div style={{
              position:'absolute', top:-2, bottom:-2,
              left: `calc(${markerPct}% - 2px)`,
              width: 4, background:'var(--fg-0)',
              borderRadius: 2, boxShadow:'0 0 4px rgba(0,0,0,0.6)',
            }}/>
          </div>
          <div style={{display:'flex', justifyContent:'space-between', fontSize:11, marginBottom:14}}>
            <span className="muted tiny">
              ↑ {tt('intel.bflow.out', 'Out')} · ${agg.outUsd.toLocaleString(undefined, {maximumFractionDigits:0})}
            </span>
            <span style={{fontWeight:700, color:netColor, fontSize:13}}>
              {tt('intel.bflow.net', 'Net')} {agg.net >= 0 ? '+' : '−'}${Math.abs(agg.net).toLocaleString(undefined, {maximumFractionDigits:0})}
            </span>
            <span className="muted tiny">
              ${agg.inUsd.toLocaleString(undefined, {maximumFractionDigits:0})} · ↓ {tt('intel.bflow.in', 'In')}
            </span>
          </div>

          {/* Top inflow / outflow tokens */}
          <div style={{display:'grid', gridTemplateColumns:'1fr 1fr', gap:10, padding:'8px 10px', background:'rgba(255,255,255,0.03)', borderRadius:8}}>
            <div>
              <div className="muted tiny">{tt('intel.bflow.topIn', 'Top inflow')}</div>
              {agg.topIn ? (
                <div style={{display:'flex', alignItems:'center', gap:6, marginTop:3}}>
                  <TinyTokLogo sym={agg.topIn.sym}/>
                  <span style={{fontWeight:700}}>{agg.topIn.sym}</span>
                  <span className="num tiny" style={{color:'#10B981', marginLeft:'auto'}}>
                    ${agg.topIn.usd.toLocaleString(undefined, {maximumFractionDigits:0})}
                  </span>
                </div>
              ) : <div className="muted tiny">—</div>}
            </div>
            <div>
              <div className="muted tiny">{tt('intel.bflow.topOut', 'Top outflow')}</div>
              {agg.topOut ? (
                <div style={{display:'flex', alignItems:'center', gap:6, marginTop:3}}>
                  <TinyTokLogo sym={agg.topOut.sym}/>
                  <span style={{fontWeight:700}}>{agg.topOut.sym}</span>
                  <span className="num tiny" style={{color:'#EF4444', marginLeft:'auto'}}>
                    ${agg.topOut.usd.toLocaleString(undefined, {maximumFractionDigits:0})}
                  </span>
                </div>
              ) : <div className="muted tiny">—</div>}
            </div>
          </div>
          <div className="muted tiny" style={{textAlign:'center', marginTop:8, fontSize:10, opacity:0.55}}>
            {agg.totalTxs} {tt('intel.bflow.txs', 'bridge tx')}
          </div>
        </>
      )}
    </WidgetCard>
  );
}

// Bridge Large Tx — individual bridge transactions above a USD threshold
// (default $3K) within a selectable window (24h / 7d / 30d / 1y).
// Replaces the net-flow-per-network widget, which hid the interesting
// signal (who bridged what) behind aggregates.
//
// Backend caps /history/global/bridges at 100 rows per page but exposes
// `total` + `totalPages`, so we walk pages DESC until we cross the cutoff
// timestamp or hit a safety cap. In practice 24h / 7d are one page; 30d
// and 1y will paginate a few times on high-activity windows.
function BridgeNetFlow() {
  const tt = useT();
  const { open: openDrill } = useDrill();
  const [tf, setTf] = useState('24h');
  const [rows, setRows] = useState(null);
  const [loading, setLoading] = useState(false);

  const MIN_USD = 3000;
  const WINDOWS = { '24h': 86_400_000, '7d': 604_800_000, '30d': 2_592_000_000, '1y': 31_536_000_000 };
  const MAX_PAGES = 20; // hard cap: 20 pages × 100 rows = 2000 bridges max per view

  useEffect(() => {
    let cancelled = false;
    setLoading(true);
    (async () => {
      const windowMs = WINDOWS[tf] || WINDOWS['24h'];
      const cutoff = Date.now() - windowMs;
      const out = [];
      for (let page = 1; page <= MAX_PAGES; page++) {
        const j = await fetch('/history/global/bridges?limit=100&page=' + page)
          .then(r => r.ok ? r.json() : null).catch(() => null);
        if (cancelled) return;
        const arr = Array.isArray(j) ? j : (j?.data || []);
        if (arr.length === 0) break;
        let reachedCutoff = false;
        for (const b of arr) {
          const ts = Number(b.timestamp);
          if (!Number.isFinite(ts)) continue;
          if (ts < cutoff) { reachedCutoff = true; break; }
          const usd = Number(b.usd_value) || 0;
          if (usd >= MIN_USD) {
            const isIn = String(b.direction || '').trim().toLowerCase() === 'incoming';
            // Shape matches BridgesSection in routes.jsx so the drill shows
            // the same detail view (counterparty resolution, Etherscan link,
            // copy buttons) as the main Bridges table.
            out.push({
              id: 'bwidget-' + (b.hash || (b.block + ':' + i)),
              ts, usd,
              sym: b.symbol || '',
              logo: b.logo,
              amt: Number(b.amount) || 0,
              dir: isIn ? 'in' : 'out',
              from: isIn ? (b.network || 'Ethereum') : 'SORA',
              to:   isIn ? 'SORA' : (b.network || 'Ethereum'),
              network: b.network || 'Unknown',
              status: b.status || 'done',
              hash: b.hash,
              block: b.block,          // drill uses this to resolve ETH origin
              sender: b.sender,
              recipient: b.recipient,
              settle: 0,
            });
          }
        }
        if (reachedCutoff) break;
        // stop if we got fewer rows than limit (last page)
        if (arr.length < 100) break;
      }
      if (cancelled) return;
      out.sort((a, b) => b.usd - a.usd); // biggest first
      setRows(out);
      setLoading(false);
    })();
    return () => { cancelled = true; };
  }, [tf]);

  const totalUsd = (rows || []).reduce((s, r) => s + r.usd, 0);
  const severity = 'ok'; // informational list, no alerting

  const tfLabels = {
    '24h': tt('intel.bridges.tf24h', '24h'),
    '7d':  tt('intel.bridges.tf7d', '7d'),
    '30d': tt('intel.bridges.tf30d', '30d'),
    '1y':  tt('intel.bridges.tf1y', '1y'),
  };

  return (
    <WidgetCard
      title={tt('intel.bridges.title', 'Bridge moves · ≥$3K')}
      severity={severity}
      tag={rows ? (rows.length + ' · $' + (totalUsd / 1000).toFixed(0) + 'K') : '…'}
    >
      {/* Timeframe selector. Active tab styled to match the rest of the app. */}
      <div style={{display:'flex', gap:4, marginBottom:10}}>
        {['24h', '7d', '30d', '1y'].map(k => (
          <button
            key={k}
            onClick={() => setTf(k)}
            className={'status-opt' + (tf === k ? ' active' : '')}
            style={{padding:'3px 10px', fontSize:11}}>
            {tfLabels[k]}
          </button>
        ))}
      </div>

      {(loading || !rows) && <div className="muted tiny">{tt('common.loading', 'Loading…')}</div>}
      {rows && !loading && rows.length === 0 && (
        <div className="muted tiny">
          {tt('intel.bridges.none', 'No bridge moves ≥ $3K in the selected window.')}
        </div>
      )}
      {rows && rows.length > 0 && (
        <div style={{display:'grid', gap:5, maxHeight: 320, overflowY:'auto'}}>
          {rows.slice(0, 40).map((r, i) => (
            <div
              key={r.id}
              onClick={() => openDrill({
                ...r,
                type: 'bridge',
                title: r.from + ' → ' + r.to,
              })}
              className="clickable"
              style={{
                display:'grid',
                gridTemplateColumns:'60px 28px 1fr 110px',
                gap:8,
                alignItems:'center',
                fontSize:12,
                padding:'6px 8px',
                background: i % 2 === 0 ? 'rgba(255,255,255,0.02)' : 'transparent',
                borderRadius: 4,
                cursor: 'pointer',
                transition: 'background 0.12s ease',
              }}
              title={tt('intel.bridges.openDrill', 'Click to open details')}
              onMouseEnter={e => e.currentTarget.style.background = 'rgba(155,27,48,0.10)'}
              onMouseLeave={e => e.currentTarget.style.background = i % 2 === 0 ? 'rgba(255,255,255,0.02)' : 'transparent'}>
              <div className="muted tiny" title={fmt.fullDate(r.ts)}>{fmt.ago(r.ts)}</div>
              <span style={{
                fontWeight:700, fontSize:14, textAlign:'center',
                color: r.dir === 'in' ? '#10B981' : '#EF4444',
              }}>
                {r.dir === 'in' ? '↓' : '↑'}
              </span>
              <div style={{display:'flex', alignItems:'center', gap:6, minWidth:0, overflow:'hidden'}}>
                <TinyTokLogo sym={r.sym}/>
                <span style={{fontWeight:700}}>{fmt.num(r.amt, 3)}</span>
                <span className="muted tiny">{r.sym}</span>
                <span className="muted tiny">·</span>
                <span className="muted tiny">{r.network}</span>
              </div>
              <div className="num" style={{
                textAlign:'right', fontWeight:700,
                color: r.usd > 100_000 ? '#EF4444' : r.usd > 20_000 ? '#F59E0B' : 'var(--fg-0)',
              }}>
                ${r.usd.toLocaleString(undefined, {maximumFractionDigits:0})}
              </div>
            </div>
          ))}
          {rows.length > 40 && (
            <div className="muted tiny" style={{textAlign:'center', marginTop:6, opacity:0.6}}>
              {tt('intel.bridges.more', '+') + (rows.length - 40) + ' ' + tt('intel.bridges.moreSfx', 'more')}
            </div>
          )}
        </div>
      )}
    </WidgetCard>
  );
}

// ---------- TIER 2 ----------

// Live network fees — real current fee per extrinsic class (transfer /
// swap / bridge) + the two multipliers that compose the total cost.
// Replaces the older "Volume / Price Divergence" widget, which flagged
// every illiquid SORA token as "suspicious" just because its volume was
// $0 — noise, not signal.
//
// How SORA fees actually work (verified by probing the live node):
//   · Each extrinsic class has a FLAT base fee in XOR. Transfer = Swap =
//     0.10 XOR currently, Bridge = 1.00 XOR. The bridge is ~10× pricier
//     but NOT because of weight (liquidityProxy.swap has 18× more refTime
//     than assets.transfer yet both cost 0.10 XOR). Fees are flat per
//     class, not linear in weight — that was the user's point.
//   · Two storage multipliers scale the base:
//       - xorFee.Multiplier (governance-set, ~142.86× today). Changes
//         only via referendum / council motion.
//       - transactionPayment.nextFeeMultiplier (chain-dynamic, 1.00×
//         when no congestion). Auto-adjusted every block from targeted
//         block fullness.
//   · XOR paid gets accumulated in xorFee.XorToVal + xorFee.XorToBuyBack
//     (reset every RemintPeriod = 100 blocks, ~10 min). When the period
//     fires the runtime burns a portion and distributes the rest to
//     validators / referral / treasury. Exact split lives in the runtime
//     code and can be changed by governance. We no longer hardcode "20%".
//   · Endpoint /stats/fee-config reads all of this live and caches 60s.
// Shared row-renderer for the burn-from-fees breakdown. Used by both
// the live (in-flight cycle) and historic (real burn-event sums) modes
// of the FeeWeekly widget so the visual remains identical regardless
// of where the numbers come from.
//
// rows:    [{ sym, pct, native, unit }]
// refRow:  { paidXor, redirectedXor, isLive }
//   paidXor       — XOR really paid out as referrer reward (no burn)
//   redirectedXor — XOR redirected to the KUSD bucket (no-referrer fallback)
//   isLive        — true → only show estimated bucket portion (no event data)
function BurnRows({ rows, refRow, sourceLabel, totalUsd, tt }) {
  const colorOf = sym =>
    sym === 'XOR'  ? '#E3232C' :
    sym === 'VAL'  ? '#FBC02D' :
    sym === 'KUSD' ? '#FFA726' :
                     '#9C27B0';
  const fmt = (n, unit) => {
    if (!Number.isFinite(n) || n <= 0) return '— ' + unit;
    if (n < 0.0001) return n.toExponential(2) + ' ' + unit;
    if (n >= 1000)  return n.toFixed(0) + ' ' + unit;
    if (n >= 1)     return n.toFixed(4) + ' ' + unit;
    return n.toFixed(6) + ' ' + unit;
  };
  return (
    <>
      {rows.map(row => (
        <div key={row.sym} style={{display:'grid', gridTemplateColumns:'24px 60px 1fr 75px 110px', gap:8, alignItems:'center', padding:'4px 0'}}>
          <TinyTokLogo sym={row.sym}/>
          <span style={{fontWeight:700, fontSize:12}}>{row.sym}</span>
          <div style={{position:'relative', height:5, background:'rgba(255,255,255,0.06)', borderRadius:3, overflow:'hidden'}}>
            <div style={{
              position:'absolute', inset:0,
              width: Math.min(100, row.pct) + '%',
              background: colorOf(row.sym),
              borderRadius: 3,
            }}/>
          </div>
          <span className="num" style={{textAlign:'right', fontWeight:700, fontSize:12}}>
            {row.pct.toFixed(2)}%
          </span>
          <span className="num tiny" style={{textAlign:'right', fontWeight:600, fontSize:11, opacity:0.9}}>
            {fmt(row.native, row.unit)}
          </span>
        </div>
      ))}
      {/* Referrer row — split into paid + redirected when we have real data */}
      <div style={{padding:'6px 0 0 0', marginTop:4, borderTop:'1px solid rgba(255,255,255,0.06)', opacity:0.85}}>
        <div style={{display:'grid', gridTemplateColumns:'24px 60px 1fr 75px 110px', gap:8, alignItems:'center', padding:'2px 0'}}>
          <span style={{fontSize:14, textAlign:'center'}}>👥</span>
          <span style={{fontWeight:700, fontSize:12}}>Referrer</span>
          <div className="muted tiny" style={{fontStyle:'italic'}}>
            {refRow.isLive
              ? tt('intel.fees.refLiveBucket', 'reserved 11.76% (split paid/redirected unknown until remint)')
              : tt('intel.fees.refSplit', 'paid to referrers · redirected to KUSD when no-referrer')}
          </div>
          <span className="num" style={{textAlign:'right', fontWeight:700, fontSize:12, opacity:0.6}}>—</span>
          <span className="num tiny" style={{textAlign:'right', fontWeight:600, fontSize:11, opacity:0.85}}>
            {refRow.isLive
              ? (refRow.redirectedXor || 0).toFixed(4) + ' XOR'
              : ((refRow.paidXor || 0) + (refRow.redirectedXor || 0)).toFixed(4) + ' XOR'}
          </span>
        </div>
        {/* Detailed split — only when we have real (non-live) data */}
        {!refRow.isLive && (
          <div style={{display:'grid', gridTemplateColumns:'24px 60px 1fr 75px 110px', gap:8, alignItems:'center', padding:'2px 0', fontSize:10, opacity:0.7}}>
            <span></span>
            <span></span>
            <span style={{paddingLeft:4}}>↳ {tt('intel.fees.refPaid', 'paid to referrer')} <span style={{opacity:0.7, fontStyle:'italic'}}>· {tt('intel.fees.notBurned', 'not burned')}</span></span>
            <span></span>
            <span className="num tiny" style={{textAlign:'right'}}>{(refRow.paidXor || 0).toFixed(4)} XOR</span>
          </div>
        )}
        {!refRow.isLive && (
          <div style={{display:'grid', gridTemplateColumns:'24px 60px 1fr 75px 110px', gap:8, alignItems:'center', padding:'2px 0', fontSize:10, opacity:0.7}}>
            <span></span>
            <span></span>
            <span style={{paddingLeft:4}}>↳ {tt('intel.fees.refRedirected', 'redirected to KUSD bucket')} <span style={{opacity:0.7, fontStyle:'italic'}}>· {tt('intel.fees.willBurn', 'will be burned')}</span></span>
            <span></span>
            <span className="num tiny" style={{textAlign:'right'}}>{(refRow.redirectedXor || 0).toFixed(4)} XOR</span>
          </div>
        )}
      </div>
      {/* Source label + total USD */}
      <div style={{display:'flex', justifyContent:'space-between', alignItems:'center', marginTop:8, paddingTop:6, borderTop:'1px solid rgba(255,255,255,0.06)', fontSize:10}}>
        <span className="muted tiny" style={{opacity:0.7}}>{sourceLabel}</span>
        {totalUsd > 0 && (
          <span className="num" style={{fontWeight:700, color:'#F59E0B'}}>
            ≈ ${totalUsd.toFixed(2)} {tt('intel.fees.burnedTotal', 'burned')}
          </span>
        )}
      </div>
    </>
  );
}
function FeeWeekly() {
  const tt = useT();
  const [cfg, setCfg] = useState(null);
  // Prices for all tokens we display in the burn breakdown so we can
  // convert the backend's XOR-equivalent burn amounts into each token's
  // native units (e.g. 0.5 XOR worth of VAL → 50 VAL given current price).
  const [prices, setPrices] = useState({ xor: 0, val: 0, kusd: 0, tbcd: 0 });
  const [weekly, setWeekly] = useState(null); // optional: 7d vs prev 7d trend
  // Burn breakdown selector + fetched data. 'live' uses on-chain
  // accumulators (current cycle); other values hit /stats/fee-burns
  // which sums real events (FeeWithdrawn + ReferrerRewarded).
  const [burnTf, setBurnTf] = useState('1h');
  const [burnData, setBurnData] = useState(null);
  const [burnLoading, setBurnLoading] = useState(false);
  // Tick every second so the "next buy-burn" countdown actually counts
  // down between backend cache refreshes. Cheap — only re-renders this
  // widget. Stops itself on unmount.
  const [, setTick] = useState(0);
  useEffect(() => {
    const id = setInterval(() => setTick(t => t + 1), 1000);
    return () => clearInterval(id);
  }, []);

  // Pull config/overview/trend. Memoised so we can call it from two places
  // (the regular 60s interval AND a one-shot trigger when the buy-burn
  // cycle just fired — see useEffect below).
  const pullAll = React.useCallback(async () => {
    const [feeCfg, overview, trend] = await Promise.all([
      fetch('/stats/fee-config').then(r => r.ok ? r.json() : null).catch(() => null),
      fetch('/stats/overview').then(r => r.ok ? r.json() : null).catch(() => null),
      fetch('/stats/fees/trend?timeframe=30d').then(r => r.ok ? r.json() : null).catch(() => null),
    ]);
    if (feeCfg) setCfg(feeCfg);
    // Pull prices for the 4 tokens we render in the burn breakdown.
    // limit=50 is generous so VAL/KUSD/TBCD all show up in the response;
    // backend returns them sorted by something (volume?) — we filter by
    // symbol regardless of order.
    fetch('/tokens?timeframe=24h&limit=50').then(r => r.ok ? r.json() : null).then(j => {
      const arr = Array.isArray(j?.data) ? j.data : Array.isArray(j) ? j : [];
      const pick = (sym) => Number(arr.find(tok => tok.symbol === sym)?.price) || 0;
      setPrices({
        xor:  pick('XOR'),
        val:  pick('VAL'),
        kusd: pick('KUSD'),
        tbcd: pick('TBCD'),
      });
    }).catch(() => {});
    const buckets = Array.isArray(trend) ? trend : [];
    const now = Date.now();
    const DAY = 24 * 60 * 60 * 1000;
    let last7 = 0, prev7 = 0;
    for (const b of buckets) {
      const ts = Date.parse(String(b.bucket || '').replace(' ', 'T') + 'Z');
      if (!Number.isFinite(ts)) continue;
      const age = now - ts;
      const usd = Number(b.total_usd) || 0;
      if (age <= 7 * DAY) last7 += usd;
      else if (age <= 14 * DAY) prev7 += usd;
    }
    setWeekly({ last7, prev7, ratio: prev7 > 0 ? last7 / prev7 : null });
  }, []);

  useEffect(() => {
    pullAll();
    const id = setInterval(pullAll, 60_000);
    return () => { clearInterval(id); };
  }, [pullAll]);

  // Fetch real burn breakdown from backend when the user picks a non-'live'
  // timeframe. 'live' uses on-chain accumulators directly (cycle-current),
  // no backend roundtrip needed. Each timeframe is cached server-side 30s,
  // so we re-fetch every 60s for visible refresh.
  useEffect(() => {

    let cancelled = false;
    setBurnLoading(true);
    const pull = () => fetch('/stats/fee-burns-live?window=' + encodeURIComponent(burnTf))
      .then(r => r.ok ? r.json() : null)
      .then(j => { if (!cancelled && j && !j.error) { setBurnData(j); setBurnLoading(false); } })
      .catch(() => { if (!cancelled) setBurnLoading(false); });
    pull();
    const id = setInterval(pull, 60_000);
    return () => { cancelled = true; clearInterval(id); };
  }, [burnTf]);

  // When the live countdown crosses a buy-burn boundary (i.e., we go from
  // "few blocks left" to "fresh full cycle"), the on-chain buckets just
  // flushed. Trigger an extra fetch so the UI shows 0/0 right away
  // instead of waiting up to 15s for the next interval poll.
  const lastBlocksLeftRef = React.useRef(null);
  useEffect(() => {
    if (!cfg) return;
    const period = Number(cfg.remintPeriodBlocks) || 100;
    const liveBlock = cfg.asOfBlock + Math.floor((Date.now() - cfg.asOfTs) / 6000);
    const blocksLeft = period - (liveBlock % period);
    const prev = lastBlocksLeftRef.current;
    lastBlocksLeftRef.current = blocksLeft;
    // Cross detection: previous tick said ≤2 blocks left, now we're back
    // near the top of the cycle (>period-3). The boundary just fired.
    if (prev != null && prev <= 2 && blocksLeft >= period - 3) {
      // Brief delay so the indexer has a chance to read the new state.
      setTimeout(() => { pullAll(); }, 1500);
    }
  });

  const severity = 'ok'; // informational, never alerts
  const tag = cfg
    ? (tt('intel.fees.block', 'block') + ' #' + Number(cfg.asOfBlock).toLocaleString())
    : '…';
  const xorPrice = prices.xor || 0;
  const classes = cfg ? [
    { id: 'transfer', label: tt('intel.fees.classTransfer', 'Transfer'), fee: cfg.samples?.transfer?.fee },
    { id: 'swap',     label: tt('intel.fees.classSwap', 'Swap'),         fee: cfg.samples?.swap?.fee },
    { id: 'bridge',   label: tt('intel.fees.classBridge', 'Bridge'),     fee: cfg.samples?.bridge?.fee },
  ] : [];

  // Acumuladores: 0 = ya se distribuyó en el último remint. Lo decimos
  // explícitamente para que "0 XOR" no genere la duda "¿y el remint qué?".
  const accumEmpty = cfg && cfg.xorToValXor === 0 && cfg.xorToBuyBackXor === 0;

  return (
    <WidgetCard title={tt('intel.fees.title', 'Network fees · live')} severity={severity} tag={tag}>
      {!cfg && <div className="muted tiny">{tt('common.loading', 'Loading…')}</div>}
      {cfg && (
        <>
          {/* Flat-per-class fees. Swap == Transfer despite 18× weight diff. */}
          <div style={{display:'grid', gridTemplateColumns:'repeat(3, 1fr)', gap:10, marginBottom:12}}>
            {classes.map(c => (
              <div key={c.id}>
                <div className="muted tiny" style={{textTransform:'uppercase', letterSpacing:'0.06em'}}>{c.label}</div>
                <div className="num" style={{fontSize:20, fontWeight:700}}>
                  {c.fee != null ? c.fee.toFixed(4) : '—'} <span style={{fontSize:12, opacity:0.7}}>XOR</span>
                </div>
                <div className="muted tiny num">
                  {c.fee != null && xorPrice > 0 ? '≈ $' + (c.fee * xorPrice).toFixed(2) : '—'}
                </div>
              </div>
            ))}
          </div>

          {/* Two composing multipliers. Sublabels removed by user request;
              the numeric value + top label is enough once you know the
              model (see chat explanation). */}
          <div style={{display:'grid', gridTemplateColumns:'1fr 1fr', gap:10, padding:'10px', background:'rgba(96,165,250,0.06)', border:'1px solid rgba(96,165,250,0.15)', borderRadius:8, marginBottom:10}}>
            <div>
              <div className="muted tiny">{tt('intel.fees.multGov', 'Governance multiplier')}</div>
              <div className="num" style={{fontSize:17, fontWeight:700, color:'#60A5FA'}}>
                {cfg.xorFeeMultiplier.toFixed(2)}×
              </div>
            </div>
            <div>
              <div className="muted tiny">{tt('intel.fees.multCong', 'Congestion multiplier')}</div>
              <div className="num" style={{fontSize:17, fontWeight:700, color: cfg.nextFeeMultiplier > 1.1 ? '#F59E0B' : '#10B981'}}>
                {cfg.nextFeeMultiplier.toFixed(3)}×
              </div>
            </div>
          </div>

          {/* Buy-burn accumulators (live from xor-fee storage).
              The two on-chain buckets that fill between random_remint
              cycles. Countdown removed because the cycle is probabilistic
              (T::Randomness inside on_initialize). */}
          <div style={{display:'grid', gridTemplateColumns:'1fr 1fr', gap:8, fontSize:11, marginBottom:14}}>
            <div>
              <div className="muted tiny">{tt('intel.fees.accumVal', 'Queued → VAL buy-burn')}</div>
              <div className="num">{cfg.xorToValXor.toFixed(4)} XOR</div>
            </div>
            <div>
              <div className="muted tiny">{tt('intel.fees.accumPswap', 'Queued → KUSD buy-burn')}</div>
              <div className="num">{cfg.xorToBuyBackXor.toFixed(4)} XOR</div>
            </div>
          </div>

          {/* Burn from network fees — counts from when the live indexer
              started (process restart resets the count). All amounts are
              REAL on-chain values (never extrapolations except for 'live'
              which shows the in-flight cycle's accumulators). */}
          <div style={{padding:'10px 12px', background:'rgba(155,27,48,0.06)', border:'1px solid rgba(155,27,48,0.2)', borderRadius:8, marginBottom:10}}>
            <div style={{display:'flex', justifyContent:'space-between', alignItems:'center', marginBottom:8, gap:8, flexWrap:'wrap'}}>
              <div className="muted tiny" style={{textTransform:'uppercase', letterSpacing:'0.06em'}}>
                {tt('intel.fees.burnFromFees', 'Burn from network fees')}
              </div>
              {/* Timeframe selector — Live shows in-flight cycle from on-chain
                  accumulators; everything else aggregates real burn events
                  recorded by the indexer since process start. */}
              <div style={{display:'inline-flex', gap:0, padding:2, borderRadius:6, background:'rgba(255,255,255,0.04)', border:'1px solid rgba(255,255,255,0.08)'}}>
                {[
                  ['1h',   '1h'],
                  ['4h',   '4h'],
                  ['6h',   '6h'],
                  ['24h',  '1d'],
                  ['7d',   '7d'],
                ].map(([k, label]) => (
                  <button key={k}
                    onClick={() => setBurnTf(k)}
                    style={{
                      padding: '3px 8px',
                      fontSize: 10,
                      fontWeight: burnTf === k ? 700 : 500,
                      border: 'none',
                      borderRadius: 4,
                      cursor: 'pointer',
                      color: burnTf === k ? 'var(--fg-0)' : 'var(--fg-2)',
                      background: burnTf === k ? 'rgba(155,27,48,0.4)' : 'transparent',
                      transition: 'all 0.12s ease',
                    }}>
                    {label}
                  </button>
                ))}
              </div>
            </div>

            {/* Header */}
            <div style={{display:'grid', gridTemplateColumns:'24px 60px 1fr 75px 110px', gap:8, alignItems:'center', padding:'2px 0', fontSize:9, opacity:0.55, textTransform:'uppercase', letterSpacing:'0.05em'}}>
              <span></span>
              <span></span>
              <span></span>
              <span style={{textAlign:'right'}}>{tt('intel.fees.shareCol', 'Share')}</span>
              <span style={{textAlign:'right'}}>{tt('intel.fees.burnedCol', 'Burned')}</span>
            </div>

            {(() => {
              // ──────────────────────────────────────────────────────
              // LIVE — extrapolation from current on-chain accumulators.
              // We can show the IN-FLIGHT cycle (what will be burned at
              // the next remint, IF nothing else is added until then).
              // ──────────────────────────────────────────────────────
              // ──────────────────────────────────────────────────────
              // HISTORIC — sum of real burn events since the indexer
              // started counting (process boot or service restart).
              // ──────────────────────────────────────────────────────
              if (!burnData) {
                return (
                  <div className="muted tiny" style={{padding:'18px 0', textAlign:'center'}}>
                    {burnLoading
                      ? tt('common.loading', 'Loading…')
                      : tt('intel.fees.noData', 'No data yet · counter starts at next on-chain remint')}
                  </div>
                );
              }
              const xorPrice  = prices.xor  || 0;
              const valPrice  = prices.val  || 0;
              const kusdPrice = prices.kusd || 1;
              const tbcdPrice = prices.tbcd || 0;
              // burnData.burns are in NATIVE units (already converted by
              // the indexer from on-chain assets.Burn events).
              const xorAmt  = Number(burnData.burns?.xor)  || 0;
              const valAmt  = Number(burnData.burns?.val)  || 0;
              const kusdAmt = Number(burnData.burns?.kusd) || 0;
              const tbcdAmt = Number(burnData.burns?.tbcd) || 0;
              // Total USD of all burns combined.
              const totalUsd =
                xorAmt  * xorPrice  +
                valAmt  * valPrice  +
                kusdAmt * kusdPrice +
                tbcdAmt * tbcdPrice;
              const pctOf = (usd) => totalUsd > 0 ? (usd / totalUsd) * 100 : 0;

              const rows = [
                { sym:'XOR',  pct:pctOf(xorAmt  * xorPrice),  native:xorAmt,  unit:'XOR'  },
                { sym:'VAL',  pct:pctOf(valAmt  * valPrice),  native:valAmt,  unit:'VAL'  },
                { sym:'KUSD', pct:pctOf(kusdAmt * kusdPrice), native:kusdAmt, unit:'KUSD' },
                { sym:'TBCD', pct:pctOf(tbcdAmt * tbcdPrice), native:tbcdAmt, unit:'TBCD' },
              ];

              const totalFees = Number(burnData.fees?.totalXor) || 0;
              const refPaid = Number(burnData.referrer?.paidXor) || 0;
              const refRedirected = Number(burnData.referrer?.redirectedToKusdXor) || 0;
              const blocks = Number(burnData.blocks) || 0;
              const sourceLabel = totalFees > 0
                ? `${burnTf} · ${totalFees.toFixed(4)} XOR ${tt('intel.fees.totalPaid', 'total fees paid')} · ${blocks} ${tt('intel.fees.blocks', 'blocks')}`
                : `${burnTf} · ${tt('intel.fees.noFees', 'no fees recorded yet in this window')}`;

              return (
                <BurnRows
                  rows={rows}
                  refRow={{ paidXor: refPaid, redirectedXor: refRedirected, isLive: false }}
                  sourceLabel={sourceLabel}
                  totalUsd={totalUsd}
                  tt={tt}
                />
              );
            })()}
          </div>

          {weekly && weekly.ratio != null && (
            <div style={{display:'flex', alignItems:'center', justifyContent:'space-between', padding:'6px 10px', background:'rgba(255,255,255,0.03)', borderRadius:6, fontSize:12}}>
              <span className="muted tiny">{tt('intel.fees.weekly', 'Fee volume · 7d vs prev')}</span>
              <span className="num" style={{fontWeight:700, color:'#60A5FA'}}>
                {weekly.ratio >= 1 ? '↑' : '↓'} {Math.abs((weekly.ratio - 1) * 100).toFixed(0)}% · ${Math.round(weekly.last7).toLocaleString()}
              </span>
            </div>
          )}
        </>
      )}
    </WidgetCard>
  );
}

// Fee / TPS anomalies — compares 24h tx volume against the 7-day daily average,
// and surfaces the fee distribution by category from /stats/fees (which does
// NOT expose per-bucket totals, so we show it as a static breakdown, not a
// ratio). The previous implementation tried to read `stats24h.fees` — a
// field /stats/network never publishes — so the "Avg fee/tx" line was
// always `0.0000 XOR`. Fixed by removing that false comparison and sourcing
// fees from the dedicated /stats/fees endpoint.
function FeeTpsAnomalies() {
  const [net, setNet] = useState(null);
  const [fees, setFees] = useState(null);
  useEffect(() => {
    let cancelled = false;
    Promise.all([
      fetch('/stats/network').then(r => r.ok ? r.json() : null).catch(() => null),
      fetch('/stats/fees').then(r => r.ok ? r.json() : null).catch(() => null),
    ]).then(([n, f]) => {
      if (cancelled) return;
      setNet(n);
      setFees(Array.isArray(f) ? f : []);
    });
    return () => { cancelled = true; };
  }, []);

  const stats24 = net?.stats24h || null;
  const stats7 = net?.stats7d || null;
  const tx24 = Number(stats24?.txCount) || 0;
  const tx7 = Number(stats7?.txCount) || 0;
  const vol24 = Number(stats24?.volume) || 0;
  const vol7 = Number(stats7?.volume) || 0;
  const tpsRatio = tx7 > 0 ? tx24 / (tx7 / 7) : 1;
  const volRatio = vol7 > 0 ? vol24 / (vol7 / 7) : 1;

  const totalFeeUsd = (fees || []).reduce((s, f) => s + (Number(f.total_usd) || 0), 0);
  const totalFeeXor = (fees || []).reduce((s, f) => s + (Number(f.total_xor) || 0), 0);

  const severity = !net ? 'none' :
    (tpsRatio > 1.5 || tpsRatio < 0.5 || volRatio > 1.5 || volRatio < 0.5) ? 'warn' : 'ok';

  const row = (label, value, base, ratio, unit) => (
    <div style={{display:'grid', gridTemplateColumns:'1fr auto auto auto', gap:12, alignItems:'center', padding:'6px 0'}}>
      <div style={{fontWeight:600}}>{label}</div>
      <div className="num tiny">{value}{unit || ''}</div>
      <div className="muted tiny">baseline {base}{unit || ''}</div>
      <div className="num" style={{fontWeight:700, color: Math.abs(ratio - 1) > 0.5 ? '#F59E0B' : '#10B981'}}>×{ratio.toFixed(2)}</div>
    </div>
  );

  return (
    <WidgetCard title="Fee / TPS Anomalies · 24h vs 7d" severity={severity} tag={net ? 'live' : '…'}>
      {!net && <div className="muted tiny">Cargando…</div>}
      {net && (
        <>
          {row('Tx/day', tx24.toLocaleString(), Math.round(tx7 / 7).toLocaleString(), tpsRatio, '')}
          {row('Volume/day', '$' + vol24.toFixed(0), '$' + Math.round(vol7 / 7).toLocaleString(), volRatio, '')}
          {fees && fees.length > 0 && (
            <div style={{marginTop:10, paddingTop:10, borderTop:'1px solid rgba(255,255,255,0.06)'}}>
              <div className="muted tiny" style={{marginBottom:6}}>
                Fee split · {totalFeeXor.toFixed(2)} XOR · ${totalFeeUsd.toFixed(0)}
              </div>
              <div style={{display:'grid', gap:4}}>
                {fees.map(f => {
                  const pct = totalFeeUsd > 0 ? (Number(f.total_usd) || 0) / totalFeeUsd * 100 : 0;
                  return (
                    <div key={f.type} style={{display:'grid', gridTemplateColumns:'90px 1fr 60px', gap:8, alignItems:'center', fontSize:11}}>
                      <span>{f.type}</span>
                      <div style={{position:'relative', height:4, background:'rgba(255,255,255,0.06)', borderRadius:2, overflow:'hidden'}}>
                        <div style={{position:'absolute', inset:0, width: pct + '%', background:'linear-gradient(90deg,#60A5FA,#EC4899)', borderRadius:2}}/>
                      </div>
                      <span className="num tiny muted" style={{textAlign:'right'}}>{pct.toFixed(1)}%</span>
                    </div>
                  );
                })}
              </div>
            </div>
          )}
        </>
      )}
    </WidgetCard>
  );
}

// Validator Health — validators that produced blocks in the *last* era but
// none in the current window. Uses /staking/recent-blocks (1 per head slot).
function ValidatorHealth() {
  const [snapshot, setSnapshot] = useState(null);
  useEffect(() => {
    let cancelled = false;
    fetch('/staking/recent-blocks?limit=200').then(r => r.ok ? r.json() : null).then(j => {
      if (cancelled) return;
      const arr = Array.isArray(j) ? j : (j?.blocks || []);
      const now = Date.now();
      const HOUR = 60 * 60 * 1000;
      const recentActive = new Set();  // validators in last 1h
      const olderActive = new Set();   // validators in 1h-6h ago
      arr.forEach(b => {
        const ts = Number(b.timestamp) || 0;
        const age = now - ts;
        if (age <= HOUR) recentActive.add(b.validator);
        else if (age <= 6 * HOUR) olderActive.add(b.validator);
      });
      const silent = [...olderActive].filter(v => !recentActive.has(v));
      const nameOf = (addr) => {
        const blk = arr.find(b => b.validator === addr);
        return blk?.validatorName || fmt.addr(addr, 6, 4);
      };
      setSnapshot({
        activeRecent: recentActive.size,
        activeOlder: olderActive.size,
        silent: silent.map(v => ({ addr: v, name: nameOf(v) })),
      });
    }).catch(() => setSnapshot({ activeRecent: 0, activeOlder: 0, silent: [] }));
    return () => { cancelled = true; };
  }, []);

  const severity = !snapshot ? 'none' : snapshot.silent.length > 2 ? 'warn' : 'ok';

  return (
    <WidgetCard title="Validator Health" severity={severity} tag={snapshot ? snapshot.activeRecent + ' active · ' + snapshot.silent.length + ' silent' : '…'}>
      {!snapshot && <div className="muted tiny">Cargando…</div>}
      {snapshot && snapshot.silent.length === 0 && <div className="muted tiny">Todos los validadores activos producen bloques.</div>}
      {snapshot && snapshot.silent.length > 0 && (
        <>
          <div className="muted tiny" style={{marginBottom:6}}>Produjeron bloques 1-6h atrás pero no en la última hora:</div>
          {snapshot.silent.map(v => (
            <div key={v.addr} style={{display:'flex', alignItems:'center', gap:8, padding:'4px 0', fontSize:12}}>
              <Severity level="warn"/>
              <span style={{fontWeight:700}}>{v.name}</span>
              <span className="muted tiny num">{fmt.addr(v.addr, 5, 4)}</span>
            </div>
          ))}
        </>
      )}
    </WidgetCard>
  );
}

// ---------- TIER 3 ----------

// Governance Pulse — referendums + scheduler + preimages snapshot.
function GovernancePulse() {
  const [data, setData] = useState(null);
  useEffect(() => {
    let cancelled = false;
    Promise.all([
      fetch('/governance/democracy').then(r => r.ok ? r.json() : null).catch(() => null),
      fetch('/governance/scheduler/agenda').then(r => r.ok ? r.json() : null).catch(() => null),
      fetch('/governance/preimages').then(r => r.ok ? r.json() : null).catch(() => null),
    ]).then(([dem, sch, pre]) => {
      if (cancelled) return;
      // Backend field is `referendums` (plural with 's'), not `referenda`.
      // The old accessor never matched → always read 0 even when live
      // referendums existed. Also surface `totalReferendums` (historical
      // count, currently ~781) as a secondary stat.
      setData({
        referendums: Array.isArray(dem?.referendums) ? dem.referendums.length : 0,
        totalReferendums: Number(dem?.totalReferendums) || 0,
        proposals: Array.isArray(dem?.proposals) ? dem.proposals.length : 0,
        scheduledCalls: Array.isArray(sch?.entries) ? sch.entries.length
          : Array.isArray(sch?.agenda) ? sch.agenda.length
          : (Array.isArray(sch) ? sch.length : 0),
        preimages: Array.isArray(pre?.preimages) ? pre.preimages.length : 0,
      });
    });
    return () => { cancelled = true; };
  }, []);

  const severity = !data ? 'none' : data.scheduledCalls > 0 ? 'warn' : 'ok';

  return (
    <WidgetCard title="Governance Pulse" severity={severity} tag={data ? (data.scheduledCalls + ' scheduled') : '…'}>
      {!data && <div className="muted tiny">Cargando…</div>}
      {data && (
        <div style={{display:'grid', gridTemplateColumns:'repeat(4, 1fr)', gap:10, textAlign:'center'}}>
          {[
            // Active referendums. Sub-line shows the historical count so an
            // empty "0" is not misread as "the chain has zero governance".
            { label: data.totalReferendums ? 'Active · ' + data.totalReferendums + ' all-time' : 'Referendums', value: data.referendums },
            { label:'Proposals',   value: data.proposals },
            { label:'Scheduled',   value: data.scheduledCalls, alert: data.scheduledCalls > 0 },
            { label:'Preimages',   value: data.preimages },
          ].map(s => (
            <div key={s.label} style={{padding:'8px 6px', background:'rgba(255,255,255,0.03)', borderRadius:8, border: s.alert ? '1px solid rgba(245,158,11,0.3)' : '1px solid rgba(255,255,255,0.04)'}}>
              <div className="num" style={{fontSize:22, fontWeight:800, color: s.alert ? '#F59E0B' : 'var(--fg-0)'}}>{s.value}</div>
              <div className="muted tiny" style={{marginTop:2}}>{s.label}</div>
            </div>
          ))}
        </div>
      )}
    </WidgetCard>
  );
}

// New Listings — tokens that weren't in the previous /tokens snapshot we saw
// (stored in localStorage as a symbol set, rolled over every 24h).
function NewListings() {
  const STORAGE_KEY = 'sm.intel.knownTokens';
  const [result, setResult] = useState(null);
  useEffect(() => {
    let cancelled = false;
    fetch('/tokens?limit=500').then(r => r.ok ? r.json() : null).then(j => {
      if (cancelled) return;
      const arr = Array.isArray(j) ? j : (j?.tokens || []);
      const now = Date.now();
      let prev = null;
      try { prev = JSON.parse(localStorage.getItem(STORAGE_KEY) || 'null'); } catch {}
      const current = arr.map(t => t.symbol).filter(Boolean);
      // Reset the baseline every 24h; new tokens between snapshots = listings.
      let listings = [];
      if (prev && (now - prev.ts) < 24 * 60 * 60 * 1000 && Array.isArray(prev.tokens)) {
        const prevSet = new Set(prev.tokens);
        listings = current.filter(s => !prevSet.has(s));
      }
      localStorage.setItem(STORAGE_KEY, JSON.stringify({ ts: prev?.ts || now, tokens: prev?.tokens || current }));
      // Details for listings from the full token map.
      setResult(listings.map(s => arr.find(t => t.symbol === s)).filter(Boolean));
    });
    return () => { cancelled = true; };
  }, []);

  const severity = !result ? 'none' : result.length > 0 ? 'warn' : 'ok';

  return (
    <WidgetCard title="New Listings · 24h" severity={severity} tag={result ? result.length + ' new' : '…'}>
      {!result && <div className="muted tiny">Cargando…</div>}
      {result && result.length === 0 && <div className="muted tiny">Sin nuevos tokens en la última ventana de 24h.</div>}
      {result && result.length > 0 && result.slice(0, 8).map(tk => (
        <div key={tk.symbol} style={{display:'flex', alignItems:'center', gap:10, padding:'6px 0', fontSize:13}}>
          <TokenLogo sym={tk.symbol} logo={tk.logo} size={24}/>
          <div style={{flex:1, minWidth:0}}>
            <div style={{fontWeight:700}}>{tk.symbol}</div>
            <div className="muted tiny">{tk.name}</div>
          </div>
          <div className="num tiny">${Number(tk.price) > 0 ? Number(tk.price).toFixed(4) : '—'}</div>
        </div>
      ))}
    </WidgetCard>
  );
}

// Cross-Pair Arbitrage — same base token priced across different quote pairs.
// /pools does NOT expose a dex_id field (only base/basePrice/reserves/target/
// targetPrice), so the previous "Cross-DEX" claim was unverifiable — every
// pool was tagged 'DEX undefined'. The real arbitrage signal we CAN compute
// from this data is: the same base token (e.g. XOR) trades at multiple
// implied USD prices across its pairs (XOR/DAI, XOR/KUSD, XOR/ETH) → a route
// exists that closes the gap. We surface the best pair-pair divergence per
// base token, and label the rows with the pair (not a fake DEX).
function CrossDexArb() {
  const [spread, setSpread] = useState(null);
  useEffect(() => {
    let cancelled = false;
    (async () => {
      const pages = await Promise.all([1, 2, 3].map(p =>
        fetch('/pools?page=' + p + '&limit=25').then(r => r.ok ? r.json() : null).catch(() => null)
      ));
      if (cancelled) return;
      const all = pages.flatMap(p => p?.data || []);
      const bySym = {};
      all.forEach(p => {
        const sym = p.base?.symbol;
        const price = Number(p.basePrice);
        if (!sym || !Number.isFinite(price) || price <= 0) return;
        if (!bySym[sym]) bySym[sym] = [];
        bySym[sym].push({ target: p.target?.symbol || '?', price });
      });
      const out = [];
      Object.entries(bySym).forEach(([sym, rows]) => {
        if (rows.length < 2) return;
        const max = rows.reduce((m, r) => r.price > m.price ? r : m);
        const min = rows.reduce((m, r) => r.price < m.price ? r : m);
        if (min.target === max.target) return; // same pair — nothing to arb
        const spreadPct = ((max.price - min.price) / min.price) * 100;
        if (spreadPct > 0.5) out.push({ sym, spreadPct, low: min, high: max });
      });
      out.sort((a, b) => b.spreadPct - a.spreadPct);
      setSpread(out.slice(0, 6));
    })();
    return () => { cancelled = true; };
  }, []);

  const best = (spread || [])[0];
  const severity = !spread ? 'none' : best?.spreadPct > 2 ? 'warn' : 'ok';

  return (
    <WidgetCard title="Cross-Pair Arbitrage" severity={severity} tag={best ? best.sym + ' spread ' + best.spreadPct.toFixed(2) + '%' : '…'}>
      {!spread && <div className="muted tiny">Analizando pools…</div>}
      {spread && spread.length === 0 && <div className="muted tiny">Sin spreads &gt; 0.5% entre pares.</div>}
      {spread && spread.map(s => (
        <div key={s.sym} style={{display:'grid', gridTemplateColumns:'60px 1fr 1fr 80px', gap:10, alignItems:'center', padding:'6px 0', fontSize:12}}>
          <span style={{fontWeight:700}}>{s.sym}</span>
          <span className="muted tiny">{s.sym}/{s.low.target}: ${s.low.price.toPrecision(4)}</span>
          <span className="muted tiny">{s.sym}/{s.high.target}: ${s.high.price.toPrecision(4)}</span>
          <span className="num" style={{textAlign:'right', fontWeight:700, color: s.spreadPct > 2 ? '#F59E0B' : 'var(--fg-0)'}}>+{s.spreadPct.toFixed(2)}%</span>
        </div>
      ))}
    </WidgetCard>
  );
}

// ---------- MAIN ----------
function IntelligenceSection() {
  const t = useT();
  return (
    <div>
      <PageHeader title={t('intel.title')} sub={t('intel.sub')}>
        <span className="tag ok"><span className="live-dot" style={{width:5,height:5}}/> on-chain signals</span>
      </PageHeader>

      <div style={{marginBottom:12}}>
        <h3 style={{margin:'8px 0', fontSize:13, color:'var(--fg-2)', letterSpacing:'0.12em', textTransform:'uppercase'}}>Tier 1 · Alerts</h3>
        <div style={{display:'grid', gridTemplateColumns:'repeat(auto-fit, minmax(380px, 1fr))', gap:14}}>
          <PegMonitor/>
          <WhaleActivity/>
          <BridgeNetFlow/>
        </div>
      </div>

      <div style={{marginBottom:12, marginTop:22}}>
        <h3 style={{margin:'8px 0', fontSize:13, color:'var(--fg-2)', letterSpacing:'0.12em', textTransform:'uppercase'}}>Tier 2 · Algorithmic Signals</h3>
        <div style={{display:'grid', gridTemplateColumns:'repeat(auto-fit, minmax(380px, 1fr))', gap:14}}>
          <FeeWeekly/>
          <FeeTpsAnomalies/>
          <ValidatorHealth/>
        </div>
      </div>

      <div style={{marginTop:22}}>
        <h3 style={{margin:'8px 0', fontSize:13, color:'var(--fg-2)', letterSpacing:'0.12em', textTransform:'uppercase'}}>Tier 3 · Extras</h3>
        <div style={{display:'grid', gridTemplateColumns:'repeat(auto-fit, minmax(380px, 1fr))', gap:14}}>
          <GovernancePulse/>
          <NewListings/>
          <CrossDexArb/>
        </div>
      </div>
    </div>
  );
}

Object.assign(window, { IntelligenceSection });
