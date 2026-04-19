/* global React, fmt, TOKENS, FAKE_ADDRS, IDENTITIES, I, sparkPath */
const { useState, useEffect, useRef, useMemo, createContext, useContext } = React;

/* =========================================================================
   DrillContext — global open(row) API
   ========================================================================= */
const DrillContext = createContext({ open: () => {}, close: () => {} });

function useDrill() { return useContext(DrillContext); }

function DrillProvider({ children }) {
  const [row, setRow] = useState(null);
  const open = (r) => setRow(r);
  const close = () => setRow(null);

  useEffect(() => {
    window.__SM_DRILL__ = { open, close };
    const onKey = (e) => { if (e.key === 'Escape') close(); };
    window.addEventListener('keydown', onKey);
    return () => window.removeEventListener('keydown', onKey);
  }, []);

  return (
    <DrillContext.Provider value={{ open, close }}>
      {children}
      {row && <DrillPanel row={row} onClose={close}/>}
    </DrillContext.Provider>
  );
}

/* =========================================================================
   DrillPanel
   ========================================================================= */

const TYPE_META = {
  swap:     { badge: 'SWAP',       color: '#EC4899' },
  transfer: { badge: 'TRANSFER',   color: '#60A5FA' },
  block:    { badge: 'BLOCK',      color: '#9B1B30' },
  order:    { badge: 'ORDER',      color: '#F5B041' },
  burn:     { badge: 'BURN',       color: '#E5243B' },
  extrinsic:{ badge: 'EXTRINSIC',  color: '#8B5CF6' },
  lp:       { badge: 'LP STAKE',   color: '#10B981' },
  holder:   { badge: 'HOLDER',     color: '#FBB040' },
  validator:{ badge: 'VALIDATOR',  color: '#7B5B90' },
  bridge:   { badge: 'BRIDGE',     color: '#06B6D4' },
  feed:     { badge: 'EVENT',      color: '#9B1B30' },
};

function Copy({ text, short = false }) {
  const [copied, setCopied] = useState(false);
  const onClick = (e) => {
    e.stopPropagation();
    navigator.clipboard?.writeText(text);
    setCopied(true);
    setTimeout(() => setCopied(false), 1400);
  };
  return (
    <button className="drill-copy" onClick={onClick} title={copied ? 'Copied!' : 'Copy'}>
      {copied ? '✓' : (short ? '⎘' : '⎘ copy')}
    </button>
  );
}

function Field({ label, children, mono }) {
  return (
    <div className="drill-field">
      <div className="drill-field-label">{label}</div>
      <div className={'drill-field-val' + (mono ? ' mono' : '')}>{children}</div>
    </div>
  );
}

function Addr({ addr }) {
  return (
    <div className="drill-addr">
      <div style={{width:24, height:24, borderRadius:'50%', background:'linear-gradient(135deg,#7B5B90,#4A3566)', flexShrink:0}}/>
      <div style={{flex:1, minWidth:0}}>
        {IDENTITIES[addr] && <div style={{fontSize:12, fontWeight:700, color:'var(--fg-0)'}}>{IDENTITIES[addr]}</div>}
        <code className="mono tiny" style={{color:'var(--fg-2)'}}>{fmt.addr(addr, 10, 8)}</code>
      </div>
      <Copy text={addr} short/>
    </div>
  );
}

function TimeLine({ ts }) {
  const d = new Date(ts);
  return (
    <>
      <Field label="Relative">{fmt.ago(ts)} ago</Field>
      <Field label="UTC" mono>{d.toISOString().replace('T', ' ').slice(0,19)}Z</Field>
      <Field label="Local" mono>{d.toLocaleString()}</Field>
    </>
  );
}

function DrillPanel({ row, onClose }) {
  const meta = TYPE_META[row.type] || TYPE_META.feed;
  const [visible, setVisible] = useState(false);
  useEffect(() => { requestAnimationFrame(() => setVisible(true)); }, []);

  return (
    <div className={'drill-backdrop' + (visible ? ' visible' : '')} onClick={onClose}>
      <aside className={'drill-panel' + (visible ? ' visible' : '')}
             onClick={(e) => e.stopPropagation()}>
        <div className="drill-head">
          <span className="drill-badge" style={{['--bc']: meta.color}}>{meta.badge}</span>
          <div className="drill-head-title">{row.title || 'Detail'}</div>
          {row.hash && <Copy text={row.hash} short/>}
          <button className="drill-close" onClick={onClose}>×</button>
        </div>
        <div className="drill-body">
          <DrillBody row={row}/>
        </div>
      </aside>
    </div>
  );
}

function DrillBody({ row }) {
  switch (row.type) {
    case 'swap':     return <SwapDetail r={row}/>;
    case 'transfer': return <TransferDetail r={row}/>;
    case 'block':    return <BlockDetail r={row}/>;
    case 'order':    return <OrderDetail r={row}/>;
    case 'burn':     return <BurnDetail r={row}/>;
    case 'extrinsic':return <ExtrinsicDetail r={row}/>;
    case 'lp':       return <LpDetail r={row}/>;
    case 'holder':   return <HolderDetail r={row}/>;
    case 'validator':return <ValidatorDetail r={row}/>;
    case 'bridge':   return <BridgeDetail r={row}/>;
    default:         return <DefaultDetail r={row}/>;
  }
}

function SwapDetail({ r }) {
  return (
    <>
      <div className="drill-section">
        <div className="drill-sec-title">Pair</div>
        <div className="drill-swap-pair">
          <div className="drill-side">
            <div className="drill-tok-badge" style={{background: TOKENS[r.inSym]?.grad}}>{r.inSym[0]}</div>
            <div>
              <div className="drill-side-label">IN</div>
              <div className="drill-side-amt num">{fmt.num(r.inAmt, 3)} {r.inSym}</div>
              <div className="drill-side-usd">≈ ${fmt.num(r.inUsd || r.inAmt * 0.07, 2)}</div>
            </div>
          </div>
          <div className="drill-arr">→</div>
          <div className="drill-side">
            <div className="drill-tok-badge" style={{background: TOKENS[r.outSym]?.grad}}>{r.outSym[0]}</div>
            <div>
              <div className="drill-side-label">OUT</div>
              <div className="drill-side-amt num">{fmt.num(r.outAmt, 3)} {r.outSym}</div>
              <div className="drill-side-usd">≈ ${fmt.num(r.outUsd || r.outAmt * 0.07, 2)}</div>
            </div>
          </div>
        </div>
      </div>

      <div className="drill-section">
        <div className="drill-sec-title">Route</div>
        <Field label="Pool">{r.pool || 'XYK Pool'}</Field>
        <Field label="Route" mono>{r.route || `${r.inSym} → ${r.outSym}`}</Field>
        <Field label="Slippage actual">{(r.slippage || 0.12).toFixed(2)}%</Field>
      </div>

      <div className="drill-section">
        <div className="drill-sec-title">Fee Breakdown</div>
        <div className="drill-fee">
          <div><span>LP fee · 0.3%</span><span className="num">{((r.fee || 0.02) * 0.78).toFixed(4)} XOR</span></div>
          <div><span>Treasury</span><span className="num">{((r.fee || 0.02) * 0.1).toFixed(4)} XOR</span></div>
          <div><span>Burn</span><span className="num">{((r.fee || 0.02) * 0.12).toFixed(4)} XOR</span></div>
          <div className="total"><span>Total</span><span className="num">{(r.fee || 0.02).toFixed(4)} XOR</span></div>
        </div>
      </div>

      <div className="drill-section">
        <div className="drill-sec-title">Caller</div>
        <Addr addr={r.caller || FAKE_ADDRS[0]}/>
      </div>

      <div className="drill-section">
        <div className="drill-sec-title">Chain</div>
        <Field label="Block" mono>#{(r.block || 21418802).toLocaleString()}</Field>
        <Field label="Extrinsic" mono><span style={{flex:1, minWidth:0, overflow:'hidden', textOverflow:'ellipsis'}}>{r.hash || '0xabc123…'}</span><Copy text={r.hash || ''} short/></Field>
        <Field label="Status"><span className="br-status done">✓ Success</span></Field>
        <TimeLine ts={r.ts || Date.now()}/>
      </div>
    </>
  );
}

function TransferDetail({ r }) {
  return (
    <>
      <div className="drill-section">
        <div className="drill-sec-title">Asset</div>
        <div className="drill-swap-pair" style={{justifyContent:'flex-start'}}>
          <div className="drill-tok-badge" style={{background: TOKENS[r.sym]?.grad}}>{r.sym[0]}</div>
          <div>
            <div className="drill-side-amt num">{fmt.num(r.amt, 3)} {r.sym}</div>
            <div className="drill-side-usd">≈ ${fmt.num(r.usd, 2)}</div>
          </div>
        </div>
      </div>
      <div className="drill-section">
        <div className="drill-sec-title">From</div>
        <Addr addr={r.from}/>
      </div>
      <div className="drill-section">
        <div className="drill-sec-title">To</div>
        <Addr addr={r.to}/>
      </div>
      <div className="drill-section">
        <div className="drill-sec-title">Chain</div>
        <Field label="Fee" mono>{(r.fee || 0.008).toFixed(4)} XOR</Field>
        {r.memo && r.memo !== '—' && <Field label="Memo">{r.memo}</Field>}
        <Field label="Block" mono>#{(r.block || 21418802).toLocaleString()}</Field>
        <Field label="Extrinsic" mono><span style={{flex:1, minWidth:0, overflow:'hidden', textOverflow:'ellipsis'}}>{r.hash || '0x…'}</span><Copy text={r.hash || ''} short/></Field>
        <TimeLine ts={r.ts}/>
      </div>
    </>
  );
}

function BlockDetail({ r }) {
  return (
    <>
      <div className="drill-section">
        <div className="drill-sec-title">Block</div>
        <Field label="Number" mono>#{(r.block || r.num).toLocaleString()}</Field>
        <Field label="Finality"><span className="br-status done">✓ Finalized</span></Field>
        <Field label="Validator">{r.validator || 'Sakura Node'}</Field>
        <Field label="Extrinsics"><span className="num">{r.extrinsics || 48}</span></Field>
        <Field label="Weight used">{((r.weight || 42.1)).toFixed(1)}%</Field>
      </div>
      <div className="drill-section">
        <div className="drill-sec-title">Hashes</div>
        <Field label="Parent" mono>0x3a9f…b7e2<Copy text="0x3a9f…b7e2" short/></Field>
        <Field label="State root" mono>0x82c4…f103<Copy text="0x82c4…f103" short/></Field>
        <TimeLine ts={r.ts || Date.now()}/>
      </div>
    </>
  );
}

function OrderDetail({ r }) {
  const pct = r.filled || 62;
  return (
    <>
      <div className="drill-section">
        <div className="drill-sec-title">Order</div>
        <Field label="Side"><span className={'fill-side ' + (r.side || 'buy')}>{(r.side || 'buy').toUpperCase()}</span></Field>
        <Field label="Pair">{r.pair || 'KUSD/XOR'}</Field>
        <Field label="Size" mono>{(r.size || 1500).toFixed(2)} {(r.pair||'KUSD/XOR').split('/')[0]}</Field>
        <Field label="Price" mono>{(r.price || 0.42).toFixed(4)}</Field>
      </div>
      <div className="drill-section">
        <div className="drill-sec-title">Fill Status</div>
        <div className="drill-fill-bar"><div className="drill-fill-fill" style={{width: pct + '%'}}/></div>
        <Field label="Filled">{pct}%</Field>
        <Field label="Remaining" mono>{((r.size || 1500) * (1 - pct/100)).toFixed(2)}</Field>
        <Field label="Lifespan">Placed {fmt.ago(r.ts || Date.now())} ago · cancels at block #{((r.block||21418802)+1200).toLocaleString()}</Field>
      </div>
      <div className="drill-section">
        <div className="drill-sec-title">Caller</div>
        <Addr addr={r.caller || FAKE_ADDRS[0]}/>
      </div>
    </>
  );
}

function BurnDetail({ r }) {
  return (
    <>
      <div className="drill-section">
        <div className="drill-sec-title">Burn</div>
        <div style={{fontSize: 36, fontWeight: 800, color: '#E5243B', fontFamily:'JetBrains Mono', letterSpacing: '-0.02em'}}>
          {fmt.num(r.amt || 420, 2)} XOR
        </div>
        <div style={{color:'var(--fg-2)', fontSize: 13, marginTop: 4}}>≈ ${fmt.num((r.amt || 420) * 0.072, 2)}</div>
      </div>
      <div className="drill-section">
        <div className="drill-sec-title">Source</div>
        <Field label="Fee type">{r.feeType || 'Swap fee · 0.3%'}</Field>
        <Field label="Pool">{r.pool || 'XOR/VAL'}</Field>
        <Field label="Triggering extrinsic" mono><span style={{flex:1, overflow:'hidden', textOverflow:'ellipsis'}}>{r.hash || '0x…'}</span><Copy text={r.hash || ''} short/></Field>
        <Field label="Block" mono>#{(r.block || 21418802).toLocaleString()}</Field>
        <TimeLine ts={r.ts || Date.now()}/>
      </div>
    </>
  );
}

function ExtrinsicDetail({ r }) {
  const [argsOpen, setArgsOpen] = useState(true);
  // G9: pull rich detail from prod /history/extrinsic/:block/:idx whenever
  // the drill opens for a row that has a real block + idx. Fills args_json +
  // events_json + success + signer + error_msg.
  const [live, setLive] = useState(null);
  useEffect(() => {
    const block = r.block || (r.extrinsic_id ? String(r.extrinsic_id).split('-')[0] : null);
    const idx = r.idx != null ? r.idx : (r.extrinsic_id ? String(r.extrinsic_id).split('-')[1] : null);
    if (!block || idx == null) return;
    let cancelled = false;
    fetch('/history/extrinsic/' + encodeURIComponent(block) + '/' + encodeURIComponent(idx))
      .then(res => res.ok ? res.json() : null)
      .then(j => { if (!cancelled && j) setLive(j); })
      .catch(() => {});
    return () => { cancelled = true; };
  }, [r.block, r.idx, r.extrinsic_id]);
  // Prefer live args_json / events_json when present.
  const argsJson = live?.args_json || r.argsJson;
  const eventsJsonRaw = live?.events_json || r.eventsJson;
  const decodedEvents = useMemo(() => {
    if (!eventsJsonRaw) return null;
    try { return typeof eventsJsonRaw === 'string' ? JSON.parse(eventsJsonRaw) : eventsJsonRaw; }
    catch { return null; }
  }, [eventsJsonRaw]);
  const events = decodedEvents && Array.isArray(decodedEvents)
    ? decodedEvents.map(e => ({ pallet: e.s || e.section, name: e.m || e.method, data: e.d || e.data, color: '#EC4899' }))
    : (r.events || [
        {pallet: 'system', name: 'ExtrinsicSuccess', color: '#10B981'},
        {pallet: r.pallet || 'liquidityProxy', name: 'Exchange', color: '#EC4899'},
        {pallet: 'transactionPayment', name: 'TransactionFeePaid', color: '#10B981'},
      ]);
  return (
    <>
      <div className="drill-section">
        <div className="drill-sec-title">Call</div>
        <div style={{fontFamily:'JetBrains Mono', fontSize: 15, fontWeight: 700}}>
          <span style={{color:'#EC4899'}}>{r.pallet || 'liquidityProxy'}</span>
          <span style={{color:'var(--fg-3)'}}> :: </span>
          <span style={{color:'var(--fg-0)'}}>{r.method || 'swap'}</span>
        </div>
        <Field label="Status">
          {r.ok !== false
            ? <span className="br-status done">✓ Success</span>
            : <span className="br-status failed">✗ {r.failReason || 'Failed'}</span>}
        </Field>
      </div>

      <div className="drill-section">
        <div className="drill-sec-title" style={{cursor:'pointer'}} onClick={() => setArgsOpen(o => !o)}>
          Decoded Args <span style={{color:'var(--fg-3)', marginLeft: 6}}>{argsOpen ? '▾' : '▸'}</span>
        </div>
        {argsOpen && (
          <pre className="ext-args" style={{marginTop: 8}}>{
            argsJson
              ? (typeof argsJson === 'string'
                  ? (() => { try { return JSON.stringify(JSON.parse(argsJson), null, 2); } catch { return argsJson; } })()
                  : JSON.stringify(argsJson, null, 2))
              : (r.args || '{ // loading from /history/extrinsic/' + (r.block || '?') + '/' + (r.idx ?? '?') + ' … }')
          }</pre>
        )}
        {live?.signer && (
          <div className="muted tiny" style={{marginTop: 6}}>signer: <span className="num">{fmt.addr(live.signer, 8, 6)}</span></div>
        )}
        {live?.error_msg && (
          <div style={{color:'#FCA5A5', marginTop: 6, fontSize: 12}}>error: {live.error_msg}</div>
        )}
      </div>

      <div className="drill-section">
        <div className="drill-sec-title">Events · {events.length}</div>
        <div className="ext-events">
          {events.map((ev, i) => (
            <div key={i} className="ext-event-chip" style={{['--ec']: ev.color}}>
              <span className="ec-dot"/>
              <span className="ec-pallet">{ev.pallet}</span>
              <span className="ec-sep">·</span>
              <span className="ec-name">{ev.name}</span>
            </div>
          ))}
        </div>
      </div>

      <div className="drill-section">
        <div className="drill-sec-title">Fee Breakdown</div>
        <div className="drill-fee">
          <div><span>Gas</span><span className="num">{((r.fee||0.02)*0.78).toFixed(4)} XOR</span></div>
          <div><span>Tip</span><span className="num">{((r.fee||0.02)*0.03).toFixed(4)} XOR</span></div>
          <div><span>Treasury</span><span className="num">{((r.fee||0.02)*0.12).toFixed(4)} XOR</span></div>
          <div><span>Reserved</span><span className="num">{((r.fee||0.02)*0.07).toFixed(4)} XOR</span></div>
          <div className="total"><span>Total</span><span className="num">{(r.fee||0.02).toFixed(4)} XOR</span></div>
        </div>
      </div>

      <div className="drill-section">
        <div className="drill-sec-title">Caller</div>
        <Addr addr={r.caller || FAKE_ADDRS[0]}/>
      </div>

      <div className="drill-section">
        <div className="drill-sec-title">Chain</div>
        <Field label="Block" mono>#{(r.block || 21418802).toLocaleString()}</Field>
        <Field label="Hash" mono><span style={{flex:1, overflow:'hidden', textOverflow:'ellipsis'}}>{r.hash}</span><Copy text={r.hash} short/></Field>
        <TimeLine ts={r.ts || Date.now()}/>
      </div>
    </>
  );
}

function LpDetail({ r }) {
  return (
    <>
      <div className="drill-section">
        <div className="drill-sec-title">Liquidity Position</div>
        <Field label="Pool">{r.pool || 'XOR/VAL'}</Field>
        <Field label="Stake" mono>{fmt.usd(r.stake || 14200)}</Field>
        <Field label="Pool share">{(r.share || 4.2).toFixed(2)}%</Field>
        <Field label="First deposit" mono>{r.since || '2024-08-12'}</Field>
        <Field label="Rewards earned" mono>{(r.rewards || 128).toFixed(2)} PSWAP</Field>
      </div>
      <div className="drill-section">
        <div className="drill-sec-title">Provider</div>
        <Addr addr={r.addr || FAKE_ADDRS[0]}/>
      </div>
      <div className="drill-section">
        <button className="btn" disabled title="not in prototype" style={{width:'100%', opacity: 0.55, cursor:'not-allowed'}}>
          Claim rewards
        </button>
      </div>
    </>
  );
}

function HolderDetail({ r }) {
  const breakdown = [
    {sym:'XOR', v: (r.value || 24000) * 0.42},
    {sym:'VAL', v: (r.value || 24000) * 0.18},
    {sym:'PSWAP', v: (r.value || 24000) * 0.12},
    {sym:'ETH', v: (r.value || 24000) * 0.16},
    {sym:'KUSD', v: (r.value || 24000) * 0.08},
    {sym:'Other', v: (r.value || 24000) * 0.04},
  ];
  const chartData = Array.from({length: 30}, (_, i) => 70 + Math.sin(i/3)*14 + Math.cos(i/5)*8 + i*0.4);
  return (
    <>
      <div className="drill-section">
        <Addr addr={r.addr}/>
        <div className="drill-hero-val num" style={{marginTop: 14}}>{fmt.usd(r.value || 24000)}</div>
        <div className="drill-hero-sub">Total portfolio value</div>
      </div>

      <div className="drill-section">
        <div className="drill-sec-title">Asset Breakdown</div>
        <div style={{display:'flex', gap: 18, alignItems:'center'}}>
          <svg viewBox="0 0 80 80" width="80" height="80">
            {(() => {
              let acc = 0;
              const tot = breakdown.reduce((s,b) => s + b.v, 0);
              return breakdown.map((b, i) => {
                const frac = b.v / tot;
                const sA = acc * 2 * Math.PI - Math.PI/2;
                acc += frac;
                const eA = acc * 2 * Math.PI - Math.PI/2;
                const large = frac > 0.5 ? 1 : 0;
                const R = 34, r_ = 22, cx = 40, cy = 40;
                const x1 = cx + R*Math.cos(sA), y1 = cy + R*Math.sin(sA);
                const x2 = cx + R*Math.cos(eA), y2 = cy + R*Math.sin(eA);
                const x3 = cx + r_*Math.cos(eA), y3 = cy + r_*Math.sin(eA);
                const x4 = cx + r_*Math.cos(sA), y4 = cy + r_*Math.sin(sA);
                const d = `M ${x1} ${y1} A ${R} ${R} 0 ${large} 1 ${x2} ${y2} L ${x3} ${y3} A ${r_} ${r_} 0 ${large} 0 ${x4} ${y4} Z`;
                return <path key={i} d={d} fill={TOKENS[b.sym]?.color || '#64748B'}/>;
              });
            })()}
          </svg>
          <div style={{flex:1}}>
            {breakdown.map((b, i) => (
              <div key={i} style={{display:'flex', alignItems:'center', gap:8, fontSize:12, padding:'3px 0'}}>
                <span style={{width:8, height:8, borderRadius:'50%', background: TOKENS[b.sym]?.color || '#64748B'}}/>
                <span style={{flex:1, fontWeight: 700}}>{b.sym}</span>
                <span className="num">{fmt.usd(b.v)}</span>
              </div>
            ))}
          </div>
        </div>
      </div>

      <div className="drill-section">
        <div className="drill-sec-title">Portfolio · 30d</div>
        <svg viewBox="0 0 300 70" width="100%" height="70" style={{display:'block'}}>
          <path d={sparkPath(chartData, 300, 70, 4)} stroke="#9B1B30" strokeWidth="1.6" fill="none"/>
        </svg>
      </div>

      <div className="drill-section">
        <Field label="Tokens held"><span className="num">{r.tokens || 6}</span></Field>
        <Field label="Last activity">{r.lastActivity || '3h ago'}</Field>
      </div>
    </>
  );
}

function ValidatorDetail({ r }) {
  const recent = Array.from({length: 8}, (_, i) => 21418802 - i*7 - Math.floor(Math.random()*3));
  return (
    <>
      <div className="drill-section">
        <div className="drill-sec-title">Validator</div>
        <div style={{display:'flex', alignItems:'center', gap: 12}}>
          <div style={{width: 36, height: 36, borderRadius: 8, background:'linear-gradient(135deg,#9B1B30,#4A3566)', flexShrink: 0}}/>
          <div>
            <div style={{fontWeight: 800, fontSize: 16}}>{r.name || 'Sakura Node'}</div>
            <div className="muted tiny">Rank #{r.rank || 1}</div>
          </div>
        </div>
      </div>
      <div className="drill-section">
        <div className="drill-sec-title">Stake</div>
        <Field label="Total" mono>{fmt.num(r.total || 420000, 0)} XOR</Field>
        <Field label="Own" mono>{fmt.num(r.own || 12400, 0)} XOR</Field>
        <Field label="Nominators" mono>{r.nominators || 142} · {fmt.num((r.total||420000) - (r.own||12400), 0)} XOR</Field>
        <div className="drill-fill-bar" style={{marginTop: 6}}>
          <div className="drill-fill-fill" style={{width: ((r.own||12400)/(r.total||420000)*100) + '%', background:'linear-gradient(90deg,#9B1B30,#7B5B90)'}}/>
        </div>
      </div>
      <div className="drill-section">
        <div className="drill-sec-title">Performance</div>
        <Field label="Commission" mono>{(r.commission || 4.2).toFixed(2)}%</Field>
        <Field label="Era points" mono>{(r.points || 8420).toLocaleString()}</Field>
        <Field label="Status">
          <span className={'val-status ' + (r.status || 'active')}>
            {(r.status || 'active') === 'active' ? '● Active' : (r.status === 'waiting' ? '◌ Waiting' : '⚠ Oversubscribed')}
          </span>
        </Field>
      </div>
      <div className="drill-section">
        <div className="drill-sec-title">Recent blocks produced</div>
        <div style={{display:'flex', flexWrap:'wrap', gap: 6}}>
          {recent.map((b, i) => (
            <code key={i} className="mono tiny" style={{padding:'3px 7px', background:'rgba(155,27,48,0.08)', border:'1px solid rgba(155,27,48,0.25)', borderRadius: 4, color: 'var(--accent)'}}>
              #{b.toLocaleString()}
            </code>
          ))}
        </div>
      </div>
    </>
  );
}

function BridgeDetail({ r }) {
  return (
    <>
      <div className="drill-section">
        <div className="drill-sec-title">Bridge Transfer</div>
        <div className="chain-route" style={{fontSize: 14}}>
          <span className={'chain-tag c-' + r.from.toLowerCase()}>{r.from}</span>
          <span className="route-arr">→</span>
          <span className={'chain-tag c-' + r.to.toLowerCase()}>{r.to}</span>
        </div>
        <div style={{marginTop: 14, fontSize: 28, fontWeight: 800, color:'var(--fg-0)', fontFamily:'JetBrains Mono'}}>{fmt.num(r.amt, 2)} {r.sym}</div>
      </div>
      <div className="drill-section">
        <div className="drill-sec-title">Status</div>
        <Field label="Current">
          <span className={'br-status ' + r.status}>
            {r.status === 'done' ? '✓ Done' : r.status === 'pending' ? '⏳ Pending' : '✗ Failed'}
          </span>
        </Field>
        <Field label="Settlement">{(r.settle || 7.4).toFixed(1)} min</Field>
        <Field label="Tx" mono><span style={{flex:1, overflow:'hidden', textOverflow:'ellipsis'}}>{r.hash}</span><Copy text={r.hash} short/></Field>
        <TimeLine ts={r.ts}/>
      </div>
    </>
  );
}

function DefaultDetail({ r }) {
  return (
    <>
      <div className="drill-section">
        <div className="drill-sec-title">Event</div>
        <div style={{fontSize: 14, color:'var(--fg-0)', fontWeight:600}}>{r.title || r.label || 'Chain event'}</div>
        {r.body && <div style={{fontSize: 13, color:'var(--fg-2)', marginTop: 8}}>{r.body}</div>}
        {r.ts && <TimeLine ts={r.ts}/>}
      </div>
    </>
  );
}

/* =========================================================================
   MUSIC PLAYER
   ========================================================================= */

const TRACKS = [
  { title: 'Sakura no Yume',     artist: 'Yumiko Tanaka',  dur: 214 },
  { title: 'Midnight Tokyo',     artist: 'Kanade',         dur: 186 },
  { title: 'Lo-fi XOR',          artist: 'Sora Collective',dur: 248 },
  { title: 'Blockchain Bloom',   artist: 'ambient.wav',    dur: 302 },
  { title: 'Validator Dreams',   artist: 'Kusari',         dur: 224 },
  { title: 'Bridge Lullaby',     artist: 'Cerberus',       dur: 278 },
];

function fmtTime(s) {
  s = Math.max(0, Math.floor(s));
  return Math.floor(s/60) + ':' + String(s%60).padStart(2, '0');
}

function MusicPlayer() {
  const [open, setOpen] = useState(false);
  const [playing, setPlaying] = useState(false);
  const [trackIdx, setTrackIdx] = useState(0);
  const [elapsed, setElapsed] = useState(0);
  const [volume, setVolume] = useState(0.7);
  const [listOpen, setListOpen] = useState(false);
  const [pos, setPos] = useState({ x: 28, y: window.innerHeight - 260 });
  const [drag, setDrag] = useState(null);

  const track = TRACKS[trackIdx];

  // tick
  useEffect(() => {
    if (!playing) return;
    const id = setInterval(() => {
      setElapsed(e => {
        if (e + 1 >= track.dur) {
          setTrackIdx(i => (i + 1) % TRACKS.length);
          return 0;
        }
        return e + 1;
      });
    }, 1000);
    return () => clearInterval(id);
  }, [playing, track.dur]);

  // reset on track change
  useEffect(() => { setElapsed(0); }, [trackIdx]);

  // drag
  useEffect(() => {
    if (!drag) return;
    const onMove = (e) => {
      setPos({
        x: Math.max(8, Math.min(window.innerWidth - 340, e.clientX - drag.dx)),
        y: Math.max(8, Math.min(window.innerHeight - 60, e.clientY - drag.dy)),
      });
    };
    const onUp = () => setDrag(null);
    window.addEventListener('mousemove', onMove);
    window.addEventListener('mouseup', onUp);
    return () => {
      window.removeEventListener('mousemove', onMove);
      window.removeEventListener('mouseup', onUp);
    };
  }, [drag]);

  const waveform = useMemo(() => {
    return Array.from({length: 60}, (_, i) =>
      0.2 + 0.35 * Math.abs(Math.sin(i * 0.6 + trackIdx)) + 0.25 * Math.abs(Math.cos(i * 0.3 + trackIdx * 1.7))
    );
  }, [trackIdx]);

  const progress = elapsed / track.dur;

  const onSeek = (e) => {
    const rect = e.currentTarget.getBoundingClientRect();
    const p = (e.clientX - rect.left) / rect.width;
    setElapsed(Math.floor(p * track.dur));
  };

  const next = () => setTrackIdx(i => (i + 1) % TRACKS.length);
  const prev = () => setTrackIdx(i => (i - 1 + TRACKS.length) % TRACKS.length);

  const stars = [0,1,2,3,4];

  return (
    <>
      <button
        className={'music-btn' + (playing ? ' playing' : '')}
        onClick={() => setOpen(o => !o)}
        title="Music player"
        aria-label="Music player"
      >
        <svg viewBox="0 0 24 24" width="20" height="20" fill="none" stroke="white" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
          <path d="M9 18V5l12-2v13"/>
          <circle cx="6" cy="18" r="3"/>
          <circle cx="18" cy="16" r="3"/>
        </svg>
      </button>

      {open && (
        <div className={'music-panel' + (listOpen ? ' tall' : '')}
             style={{left: pos.x, top: pos.y}}>
          <div className="music-head"
               onMouseDown={(e) => setDrag({dx: e.clientX - pos.x, dy: e.clientY - pos.y})}>
            <div style={{flex:1, minWidth: 0}}>
              <div className="music-title">{track.title}</div>
              <div className="music-artist">{track.artist}</div>
            </div>
            <button className="music-close" onClick={() => setOpen(false)}>×</button>
          </div>

          <div className="music-wave" onClick={onSeek}>
            <svg viewBox="0 0 260 32" width="100%" height="32" preserveAspectRatio="none">
              {waveform.map((h, i) => {
                const x = (i / waveform.length) * 260;
                const bh = h * 28;
                const played = (i / waveform.length) <= progress;
                return (
                  <rect key={i} x={x} y={16 - bh/2} width={260/waveform.length * 0.7} height={bh}
                        fill={played ? '#9B1B30' : 'rgba(255,255,255,0.18)'} rx="1"/>
                );
              })}
            </svg>
          </div>

          <div className="music-time">
            <span className="num">{fmtTime(elapsed)}</span>
            <span className="num muted">{fmtTime(track.dur)}</span>
          </div>

          <div className="music-controls">
            <button className="music-skip" onClick={prev} title="Previous">
              <svg viewBox="0 0 24 24" width="16" height="16" fill="currentColor"><path d="M6 4h2v16H6zM20 4v16L8 12z"/></svg>
            </button>
            <button className="music-play" onClick={() => setPlaying(p => !p)} title={playing ? 'Pause' : 'Play'}>
              {playing
                ? <svg viewBox="0 0 24 24" width="20" height="20" fill="currentColor"><rect x="6" y="5" width="4" height="14"/><rect x="14" y="5" width="4" height="14"/></svg>
                : <svg viewBox="0 0 24 24" width="20" height="20" fill="currentColor"><path d="M8 5v14l11-7z"/></svg>}
            </button>
            <button className="music-skip" onClick={next} title="Next">
              <svg viewBox="0 0 24 24" width="16" height="16" fill="currentColor"><path d="M16 4h2v16h-2zM4 4l12 8-12 8z"/></svg>
            </button>
          </div>

          <div className="music-vol">
            <svg viewBox="0 0 24 24" width="13" height="13" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
              <polygon points="11 5 6 9 2 9 2 15 6 15 11 19 11 5"/>
              <path d="M15.54 8.46a5 5 0 0 1 0 7.07"/>
            </svg>
            <input type="range" min="0" max="1" step="0.01" value={volume} onChange={e => setVolume(+e.target.value)}/>
            <button className="music-list-toggle" onClick={() => setListOpen(o => !o)} title="Playlist">
              <svg viewBox="0 0 24 24" width="14" height="14" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round"><path d={listOpen ? 'm6 15 6-6 6 6' : 'm6 9 6 6 6-6'}/></svg>
            </button>
          </div>

          {listOpen && (
            <div className="music-list">
              {TRACKS.map((t, i) => (
                <div key={i} className={'music-list-row' + (i === trackIdx ? ' active' : '')}
                     onClick={() => setTrackIdx(i)}>
                  <span className="music-list-num">
                    {i === trackIdx && playing ? '▶' : (i === trackIdx ? '•' : String(i+1).padStart(2,'0'))}
                  </span>
                  <div style={{flex:1, minWidth: 0}}>
                    <div className="music-list-title">{t.title}</div>
                    <div className="music-list-artist">{t.artist}</div>
                  </div>
                  <span className="num muted tiny">{fmtTime(t.dur)}</span>
                </div>
              ))}
            </div>
          )}

          {/* ambient stars */}
          {stars.map(i => (
            <span key={i} className="music-star" style={{
              left: (i * 19 + 12) + '%',
              top: (15 + (i % 3) * 18) + '%',
              animationDelay: (i * 0.6) + 's',
            }}/>
          ))}
        </div>
      )}
    </>
  );
}

Object.assign(window, { DrillProvider, useDrill, MusicPlayer });
