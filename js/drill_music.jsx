/* global React, fmt, TOKENS, FAKE_ADDRS, IDENTITIES, I, sparkPath, TokenLogo */
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
  // All fields come from /history/global/swaps → {in, out, wallet, block, hash, time}.
  // Route/pool/slippage/fee-breakdown aren't exposed by the endpoint, so we
  // don't synthesize them anymore — we show only what's real.
  const inAmt = Number(r.inAmt) || 0;
  const outAmt = Number(r.outAmt) || 0;
  const inUsd = Number(r.inUsd) || Number(r.usd) || 0;
  const outUsd = Number(r.outUsd) || 0;
  return (
    <>
      <div className="drill-section">
        <div className="drill-sec-title">Pair</div>
        <div className="drill-swap-pair">
          <div className="drill-side">
            <TokenLogo sym={r.inSym} size={40}/>
            <div>
              <div className="drill-side-label">IN</div>
              <div className="drill-side-amt num">{fmt.num(inAmt, 3)} {r.inSym}</div>
              {inUsd > 0 && <div className="drill-side-usd">≈ ${fmt.num(inUsd, 2)}</div>}
            </div>
          </div>
          <div className="drill-arr">→</div>
          <div className="drill-side">
            <TokenLogo sym={r.outSym} size={40}/>
            <div>
              <div className="drill-side-label">OUT</div>
              <div className="drill-side-amt num">{fmt.num(outAmt, 3)} {r.outSym}</div>
              {outUsd > 0 && <div className="drill-side-usd">≈ ${fmt.num(outUsd, 2)}</div>}
            </div>
          </div>
        </div>
      </div>

      {r.caller && (
        <div className="drill-section">
          <div className="drill-sec-title">Caller</div>
          <Addr addr={r.caller}/>
        </div>
      )}

      <div className="drill-section">
        <div className="drill-sec-title">Chain</div>
        {r.block && <Field label="Block" mono>#{Number(r.block).toLocaleString()}</Field>}
        {r.hash && (
          <Field label="Extrinsic" mono>
            <span style={{flex:1, minWidth:0, overflow:'hidden', textOverflow:'ellipsis'}}>{r.hash}</span>
            <Copy text={r.hash} short/>
          </Field>
        )}
        {r.ts && <TimeLine ts={r.ts}/>}
      </div>
    </>
  );
}

function TransferDetail({ r }) {
  // /history/global/transfers returns {symbol, amount, usdValue, from, to,
  // block, hash, logo, time}. No `fee` field is exposed — remove the fake
  // 0.008 XOR default instead of misleading the user.
  const amt = Number(r.amt ?? r.amount) || 0;
  const usd = Number(r.usd ?? r.usdValue) || 0;
  const sym = r.sym || r.symbol;
  return (
    <>
      <div className="drill-section">
        <div className="drill-sec-title">Asset</div>
        <div className="drill-swap-pair" style={{justifyContent:'flex-start'}}>
          <TokenLogo sym={sym} logo={r.logo} size={40}/>
          <div>
            <div className="drill-side-amt num">{fmt.num(amt, 3)} {sym}</div>
            {usd > 0 && <div className="drill-side-usd">≈ ${fmt.num(usd, 2)}</div>}
          </div>
        </div>
      </div>
      {r.from && (
        <div className="drill-section">
          <div className="drill-sec-title">From</div>
          <Addr addr={r.from}/>
        </div>
      )}
      {r.to && (
        <div className="drill-section">
          <div className="drill-sec-title">To</div>
          <Addr addr={r.to}/>
        </div>
      )}
      <div className="drill-section">
        <div className="drill-sec-title">Chain</div>
        {r.memo && r.memo !== '—' && <Field label="Memo">{r.memo}</Field>}
        {r.block && <Field label="Block" mono>#{Number(r.block).toLocaleString()}</Field>}
        {r.hash && (
          <Field label="Extrinsic" mono>
            <span style={{flex:1, minWidth:0, overflow:'hidden', textOverflow:'ellipsis'}}>{r.hash}</span>
            <Copy text={r.hash} short/>
          </Field>
        )}
        {r.ts && <TimeLine ts={r.ts}/>}
      </div>
    </>
  );
}

function BlockDetail({ r }) {
  const num = r.block || r.num;
  // Pull real data from /block/:n — includes hash, parentHash, stateRoot, all
  // extrinsics (with success, section, method, events, args) and block logs.
  const [data, setData] = useState(null);
  const [expandExt, setExpandExt] = useState(false);
  const [openExt, setOpenExt] = useState(null); // index of extrinsic whose events are shown
  useEffect(() => {
    if (!num) return;
    let cancelled = false;
    fetch('/block/' + encodeURIComponent(num))
      .then(res => res.ok ? res.json() : null)
      .then(j => { if (!cancelled) setData(j); })
      .catch(() => {});
    return () => { cancelled = true; };
  }, [num]);

  const exts = Array.isArray(data?.extrinsics) ? data.extrinsics : [];
  const inherent = Array.isArray(data?.inherentEvents) ? data.inherentEvents : [];
  const totalEvents = Number(data?.totalEvents) || (exts.reduce((s, e) => s + (Array.isArray(e.events) ? e.events.length : 0), 0) + inherent.length);
  const okCount = exts.filter(e => e.success).length;
  const shortHash = (h) => h ? h.slice(0, 10) + '…' + h.slice(-6) : '—';
  const ts = data?.timestamp ? Number(data.timestamp) : (r.ts || Date.now());
  const visibleExts = expandExt ? exts : exts.slice(0, 8);

  const toggleExt = (idx) => setOpenExt(v => v === idx ? null : idx);

  return (
    <>
      <div className="drill-section">
        <div className="drill-sec-title">Block</div>
        <Field label="Number" mono>#{Number(num).toLocaleString()}</Field>
        <Field label="Finality"><span className="br-status done">✓ Finalized</span></Field>
        <Field label="Extrinsics"><span className="num">{data ? exts.length : '…'}</span>{exts.length > 0 && <span className="muted tiny" style={{marginLeft:8}}>{okCount} ok · {exts.length - okCount} fail</span>}</Field>
        <Field label="Events"><span className="num">{data ? totalEvents : '…'}</span>{inherent.length > 0 && <span className="muted tiny" style={{marginLeft:8}}>{inherent.length} inherent</span>}</Field>
        <Field label="Spec">{data ? (data.specName + ' · v' + data.specVersion) : '…'}</Field>
      </div>
      <div className="drill-section">
        <div className="drill-sec-title">Hashes</div>
        <Field label="Hash" mono>{shortHash(data?.hash)}{data?.hash && <Copy text={data.hash} short/>}</Field>
        <Field label="Parent" mono>{shortHash(data?.parentHash)}{data?.parentHash && <Copy text={data.parentHash} short/>}</Field>
        <Field label="State root" mono>{shortHash(data?.stateRoot)}{data?.stateRoot && <Copy text={data.stateRoot} short/>}</Field>
        <Field label="Extrinsics root" mono>{shortHash(data?.extrinsicsRoot)}{data?.extrinsicsRoot && <Copy text={data.extrinsicsRoot} short/>}</Field>
        <TimeLine ts={ts}/>
      </div>
      {exts.length > 0 && (
        <div className="drill-section">
          <div className="drill-sec-title">Extrinsics ({exts.length}) <span className="muted tiny" style={{fontWeight:400}}>· click para ver eventos</span></div>
          <table className="lp-table">
            <thead><tr><th style={{width:28}}></th><th>#</th><th>Call</th><th>Signer</th><th style={{textAlign:'right'}}>Events</th><th style={{textAlign:'center'}}>OK</th></tr></thead>
            <tbody>
              {visibleExts.map((e) => {
                const open = openExt === e.index;
                const evs = Array.isArray(e.events) ? e.events : [];
                return (
                  <React.Fragment key={e.index}>
                    <tr onClick={() => toggleExt(e.index)} style={{cursor:'pointer'}}>
                      <td style={{textAlign:'center', color:'var(--fg-3)'}}>{open ? '▼' : '▶'}</td>
                      <td className="num tiny">{e.index}</td>
                      <td style={{fontFamily:'JetBrains Mono', fontSize:11}}>
                        <span style={{color:'#EC4899'}}>{e.section}</span>
                        <span style={{color:'var(--fg-3)'}}>::</span>
                        <span>{e.method}</span>
                      </td>
                      <td className="muted tiny">{e.signer ? fmt.addr(e.signer, 5, 4) : '—'}</td>
                      <td className="num tiny" style={{textAlign:'right'}}>{evs.length}</td>
                      <td style={{textAlign:'center'}}>{e.success ? '✓' : '✗'}</td>
                    </tr>
                    {open && (
                      <tr>
                        <td colSpan={6} style={{padding:'6px 12px', background:'rgba(0,0,0,0.25)'}}>
                          {Array.isArray(e.args) && e.args.length > 0 && (
                            <>
                              <div className="muted tiny" style={{margin:'4px 0 2px', fontWeight:700}}>Args</div>
                              <pre style={{margin:0, padding:8, background:'rgba(0,0,0,0.4)', borderRadius:6, overflow:'auto', maxHeight:180, fontSize:11, fontFamily:'JetBrains Mono'}}>{JSON.stringify(e.args, null, 2)}</pre>
                            </>
                          )}
                          <div className="muted tiny" style={{margin:'8px 0 2px', fontWeight:700}}>Events ({evs.length})</div>
                          {evs.length === 0 ? (
                            <div className="muted tiny">Sin eventos.</div>
                          ) : (
                            <pre style={{margin:0, padding:8, background:'rgba(0,0,0,0.4)', borderRadius:6, overflow:'auto', maxHeight:260, fontSize:11, fontFamily:'JetBrains Mono'}}>{JSON.stringify(evs, null, 2)}</pre>
                          )}
                        </td>
                      </tr>
                    )}
                  </React.Fragment>
                );
              })}
            </tbody>
          </table>
          {exts.length > 8 && (
            <button className="btn" style={{marginTop:8}} onClick={() => setExpandExt(v => !v)}>
              {expandExt ? '↑ Ver menos' : '↓ Ver todos (' + exts.length + ')'}
            </button>
          )}
        </div>
      )}
      {inherent.length > 0 && (
        <div className="drill-section">
          <div className="drill-sec-title">Inherent events ({inherent.length})</div>
          <pre style={{margin:0, padding:8, background:'rgba(0,0,0,0.4)', borderRadius:6, overflow:'auto', maxHeight:240, fontSize:11, fontFamily:'JetBrains Mono'}}>{JSON.stringify(inherent, null, 2)}</pre>
        </div>
      )}
      {data && exts.length === 0 && inherent.length === 0 && (
        <div className="drill-section">
          <div className="muted tiny">Sin extrinsics en este bloque.</div>
        </div>
      )}
    </>
  );
}

function OrderDetail({ r }) {
  // Render only the fields we actually have from /history/global/orderbook
  // rather than fabricating placeholder fill%, remaining, lifespan, etc.
  const pair = (r.base_asset && r.quote_asset) ? (r.base_asset + '/' + r.quote_asset) : (r.pair || '—');
  const side = (r.side || '').toLowerCase();
  const price = Number(r.price) || 0;
  const amount = Number(r.amount) || Number(r.size) || 0;
  const usd = Number(r.usd_value) || Number(r.usd) || 0;
  const caller = r.wallet || r.caller;
  return (
    <>
      <div className="drill-section">
        <div className="drill-sec-title">Order</div>
        <Field label="Event"><span className="tag">{r.event_type || '—'}</span></Field>
        <Field label="Side">
          {side
            ? <span className={'fill-side ' + side}>{side.toUpperCase()}</span>
            : <span className="muted tiny">—</span>}
        </Field>
        <Field label="Pair">{pair}</Field>
        <Field label="Price" mono>{price > 0 ? price.toFixed(6) : '—'}</Field>
        <Field label="Amount" mono>{amount > 0 ? fmt.num(amount, 2) + ' ' + (r.base_asset || pair.split('/')[0] || '') : '—'}</Field>
        {usd > 0 && <Field label="USD">${usd.toFixed(2)}</Field>}
        {r.order_id && <Field label="Order ID" mono>{r.order_id}</Field>}
      </div>
      <div className="drill-section">
        <div className="drill-sec-title">Caller</div>
        {caller
          ? <Addr addr={caller}/>
          : <div className="muted tiny">No disponible</div>}
      </div>
    </>
  );
}

function BurnDetail({ r }) {
  // Only render data that was actually passed in. No synthetic fee-type /
  // pool / $USD inference — the Burn event in prod feeds doesn't expose it,
  // so making it up would mislead the user.
  const amt = Number(r.amt ?? r.amount) || 0;
  const usd = Number(r.usd) || 0;
  const sym = r.sym || r.symbol || 'XOR';
  return (
    <>
      <div className="drill-section">
        <div className="drill-sec-title">Burn</div>
        <div style={{fontSize: 36, fontWeight: 800, color: '#E5243B', fontFamily:'JetBrains Mono', letterSpacing: '-0.02em'}}>
          {amt > 0 ? fmt.num(amt, 2) + ' ' + sym : '—'}
        </div>
        {usd > 0 && <div style={{color:'var(--fg-2)', fontSize: 13, marginTop: 4}}>≈ ${fmt.num(usd, 2)}</div>}
      </div>
      {(r.hash || r.block) && (
        <div className="drill-section">
          <div className="drill-sec-title">On-chain</div>
          {r.hash && (
            <Field label="Extrinsic" mono>
              <span style={{flex:1, overflow:'hidden', textOverflow:'ellipsis'}}>{r.hash}</span>
              <Copy text={r.hash} short/>
            </Field>
          )}
          {r.block && <Field label="Block" mono>#{Number(r.block).toLocaleString()}</Field>}
          {r.ts && <TimeLine ts={r.ts}/>}
        </div>
      )}
    </>
  );
}

function ExtrinsicDetail({ r }) {
  const [argsOpen, setArgsOpen] = useState(true);
  const [eventsOpen, setEventsOpen] = useState(true);
  const [showRawEvents, setShowRawEvents] = useState(false);
  const [copied, setCopied] = useState('');
  // G9/Pass5: pull rich detail from /history/extrinsic/:block/:idx AND
  // /lookup/usd-value/:id so the drill matches prod's "Detalles del Extrinsic"
  // modal field-for-field (Extrinsic ID, Hash, Block, Pallet, Firmante,
  // Resultado, Hora, Valor USD, full Arguments JSON, full Events JSON).
  const [live, setLive] = useState(null);
  const [usd, setUsd] = useState(null);
  const block = r.block || (r.extrinsic_id ? String(r.extrinsic_id).split('-')[0] : null);
  const idx = r.idx != null ? r.idx : (r.extrinsic_id ? String(r.extrinsic_id).split('-')[1] : null);
  const extrinsicId = block && idx != null ? (block + '-' + idx) : (r.extrinsic_id || null);

  useEffect(() => {
    if (!block || idx == null) return;
    let cancelled = false;
    fetch('/history/extrinsic/' + encodeURIComponent(block) + '/' + encodeURIComponent(idx))
      .then(res => res.ok ? res.json() : null)
      .then(j => { if (!cancelled && j) setLive(j); })
      .catch(() => {});
    // Prod exposes USD value at TX time via /lookup/usd-value/:extrinsic_id.
    if (extrinsicId) {
      fetch('/lookup/usd-value/' + encodeURIComponent(extrinsicId))
        .then(res => res.ok ? res.json() : null)
        .then(j => { if (!cancelled && j && j.usd_value != null) setUsd(j); })
        .catch(() => {});
    }
    return () => { cancelled = true; };
  }, [block, idx, extrinsicId]);

  const argsJson = live?.args_json || r.argsJson;
  const eventsJsonRaw = live?.events_json || r.eventsJson;
  const decodedEvents = useMemo(() => {
    if (!eventsJsonRaw) return null;
    try { return typeof eventsJsonRaw === 'string' ? JSON.parse(eventsJsonRaw) : eventsJsonRaw; }
    catch { return null; }
  }, [eventsJsonRaw]);
  const events = decodedEvents && Array.isArray(decodedEvents)
    ? decodedEvents.map(e => ({ pallet: e.s || e.section, name: e.m || e.method, data: e.d || e.data, color: '#EC4899' }))
    : (r.events || []);
  const hash = live?.hash || r.hash;
  const signer = live?.signer || r.caller || r.signer;
  const section = live?.section || r.pallet || r.section;
  const method = live?.method || r.method;
  const timeStr = live?.time || (r.ts ? new Date(r.ts).toLocaleString('es-ES') : null);
  const success = live?.success === 1 || live?.success === true || (live == null && r.ok !== false);

  const copy = async (text, label) => {
    try {
      await navigator.clipboard.writeText(text);
      setCopied(label);
      setTimeout(() => setCopied(''), 1200);
    } catch {}
  };

  const prettyArgs = (() => {
    if (!argsJson) return '// cargando args_json desde prod…';
    if (typeof argsJson !== 'string') return JSON.stringify(argsJson, null, 2);
    try { return JSON.stringify(JSON.parse(argsJson), null, 2); }
    catch { return argsJson; }
  })();

  return (
    <>
      {/* Header field set — mirrors prod's "Detalles del Extrinsic" card */}
      <div className="drill-section">
        <div className="drill-sec-title">Detalles del Extrinsic</div>
        <Field label="Extrinsic ID">
          <span className="num" style={{fontWeight: 600}}>{extrinsicId || '—'}</span>
          {extrinsicId && (
            <button className="btn tiny" style={{marginLeft:8, fontSize:11}} onClick={() => copy(extrinsicId, 'id')}>
              {copied === 'id' ? '✓' : '⎘'}
            </button>
          )}
        </Field>
        <Field label="Hash">
          <span className="num tiny" style={{overflowWrap:'anywhere'}}>{hash || '—'}</span>
          {hash && (
            <button className="btn tiny" style={{marginLeft:8, fontSize:11}} onClick={() => copy(hash, 'hash')}>
              {copied === 'hash' ? '✓' : '⎘'}
            </button>
          )}
        </Field>
        <Field label="Block">
          <span className="num" style={{fontWeight: 600}}>#{block ? Number(block).toLocaleString('es-ES') : '—'}</span>
        </Field>
        <Field label="Pallet">
          <span style={{fontFamily:'JetBrains Mono', fontSize: 14, fontWeight: 700}}>
            <span style={{color:'#EC4899'}}>{section || '—'}</span>
            <span style={{color:'var(--fg-3)'}}> :: </span>
            <span style={{color:'var(--fg-0)'}}>{method || '—'}</span>
          </span>
        </Field>
        <Field label="Firmante">
          {signer ? <Addr addr={signer}/> : <span className="muted">—</span>}
        </Field>
        <Field label="Resultado">
          {success
            ? <span className="br-status done">✓ Success</span>
            : <span className="br-status failed">✗ {live?.error_msg || r.failReason || 'Failed'}</span>}
        </Field>
        <Field label="Hora">
          <span className="num tiny">{timeStr || '—'}</span>
        </Field>
        <Field label="Valor USD (al momento de TX)">
          <span className="num" style={{fontWeight:700, color:'#6EE7B7'}}>
            {usd != null ? '$' + Number(usd.usd_value).toLocaleString('es-ES', {minimumFractionDigits: 2, maximumFractionDigits: 2}) : (extrinsicId ? 'cargando…' : '—')}
          </span>
          {usd?.source && <span className="muted tiny" style={{marginLeft:6}}>({usd.source})</span>}
        </Field>
      </div>

      <div className="drill-section">
        <div className="drill-sec-title" style={{display:'flex', alignItems:'center', gap:8, cursor:'pointer'}} onClick={() => setArgsOpen(o => !o)}>
          <span>Arguments (JSON)</span>
          <span style={{color:'var(--fg-3)'}}>{argsOpen ? '▾' : '▸'}</span>
          {argsJson && (
            <button className="btn tiny" style={{marginLeft:'auto', fontSize:11}} onClick={(e) => { e.stopPropagation(); copy(prettyArgs, 'args'); }}>
              {copied === 'args' ? '✓ copiado' : 'copiar'}
            </button>
          )}
        </div>
        {argsOpen && (
          <pre className="ext-args" style={{marginTop: 8, maxHeight: 320, overflow:'auto'}}>{prettyArgs}</pre>
        )}
      </div>

      <div className="drill-section">
        <div className="drill-sec-title" style={{display:'flex', alignItems:'center', gap:8, cursor:'pointer'}} onClick={() => setEventsOpen(o => !o)}>
          <span>Events · {events.length}</span>
          <span style={{color:'var(--fg-3)'}}>{eventsOpen ? '▾' : '▸'}</span>
          {decodedEvents && (
            <button className="btn tiny" style={{marginLeft:'auto', fontSize:11}}
                    onClick={(e) => { e.stopPropagation(); setShowRawEvents(v => !v); }}>
              {showRawEvents ? 'chips' : 'raw JSON'}
            </button>
          )}
        </div>
        {eventsOpen && !showRawEvents && (
          <div className="ext-events" style={{marginTop: 8}}>
            {events.map((ev, i) => (
              <div key={i} className="ext-event-chip" style={{['--ec']: ev.color}}>
                <span className="ec-dot"/>
                <span className="ec-pallet">{ev.pallet}</span>
                <span className="ec-sep">·</span>
                <span className="ec-name">{ev.name}</span>
              </div>
            ))}
          </div>
        )}
        {eventsOpen && showRawEvents && decodedEvents && (
          <pre className="ext-args" style={{marginTop: 8, maxHeight: 380, overflow:'auto'}}>{JSON.stringify(decodedEvents, null, 2)}</pre>
        )}
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
  // Pull the real on-chain holdings for this address. /balance/:addr returns
  // an array of { symbol, amount, logo, usdValue } rows; /wallet/info/:addr
  // returns activity metadata.
  const [tokens, setTokens] = useState(null); // null = loading, [] = empty
  const [info, setInfo] = useState(null);
  useEffect(() => {
    if (!r.addr) { setTokens([]); return; }
    let cancelled = false;
    fetch('/balance/' + encodeURIComponent(r.addr))
      .then(res => res.ok ? res.json() : [])
      .then(j => { if (!cancelled) setTokens(Array.isArray(j) ? j : []); })
      .catch(() => { if (!cancelled) setTokens([]); });
    fetch('/wallet/info/' + encodeURIComponent(r.addr))
      .then(res => res.ok ? res.json() : null)
      .then(j => { if (!cancelled) setInfo(j); })
      .catch(() => {});
    return () => { cancelled = true; };
  }, [r.addr]);

  const rows = (tokens || [])
    .map(t => ({
      sym: t.symbol,
      amount: Number(t.amount) || 0,
      usd: Number(t.usdValue) || 0,
      logo: t.logo,
    }))
    .sort((a, b) => b.usd - a.usd);
  const totalUsd = rows.reduce((s, t) => s + t.usd, 0);

  // Format "last activity" from wallet/info when we have a timestamp; otherwise
  // leave a dash so the user can tell data is missing instead of guessing.
  const lastTs = info?.lastActivity || info?.lastActiveTs || info?.lastSeen || null;
  const lastActivity = lastTs
    ? fmt.ago
      ? fmt.ago(Number(lastTs) * (Number(lastTs) > 1e12 ? 1 : 1000))
      : new Date(Number(lastTs) * (Number(lastTs) > 1e12 ? 1 : 1000)).toLocaleString()
    : '—';

  return (
    <>
      <div className="drill-section">
        <Addr addr={r.addr}/>
        <div className="drill-hero-val num" style={{marginTop: 14}}>
          {tokens === null ? '…' : fmt.usd(totalUsd)}
        </div>
        <div className="drill-hero-sub">Total portfolio value</div>
      </div>

      <div className="drill-section">
        <div className="drill-sec-title">Asset Breakdown</div>
        {tokens === null && <div className="muted tiny">Cargando tokens…</div>}
        {tokens && rows.length === 0 && <div className="muted tiny">Sin balances on-chain.</div>}
        {rows.length > 0 && (
          <div style={{display:'flex', gap: 18, alignItems:'flex-start'}}>
            <svg viewBox="0 0 80 80" width="80" height="80" style={{flexShrink:0}}>
              {(() => {
                let acc = 0;
                const tot = totalUsd || 1;
                return rows.map((b, i) => {
                  const frac = b.usd / tot;
                  if (!Number.isFinite(frac) || frac <= 0) return null;
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
                  return <path key={b.sym + i} d={d} fill={TOKENS[b.sym]?.color || '#64748B'}/>;
                });
              })()}
            </svg>
            <div style={{flex:1, minWidth:0}}>
              {rows.map((b, i) => (
                <div key={b.sym + i} style={{display:'flex', alignItems:'center', gap:8, fontSize:12, padding:'4px 0'}}>
                  <TokenLogo sym={b.sym} logo={b.logo} size={18}/>
                  <span style={{fontWeight: 700, minWidth: 54}}>{b.sym}</span>
                  <span className="num" style={{flex:1}}>{fmt.num(b.amount, b.amount >= 1000 ? 0 : b.amount >= 1 ? 2 : 6)}</span>
                  <span className="num muted">{fmt.usd(b.usd)}</span>
                </div>
              ))}
            </div>
          </div>
        )}
      </div>

      <div className="drill-section">
        <Field label="Tokens held"><span className="num">{tokens === null ? '…' : rows.length}</span></Field>
        <Field label="Last activity">{lastActivity}</Field>
      </div>
    </>
  );
}

function ValidatorDetail({ r }) {
  // Pull real recent blocks produced by this validator via /staking/recent-blocks
  // filtered by validator address; everything else comes from the row data that
  // was already fetched from /staking/validators.
  const [recent, setRecent] = useState(null); // null = loading, [] = none
  useEffect(() => {
    if (!r.address) { setRecent([]); return; }
    let cancelled = false;
    fetch('/staking/recent-blocks?limit=50')
      .then(res => res.ok ? res.json() : null)
      .then(j => {
        if (cancelled) return;
        const arr = Array.isArray(j) ? j : (j?.blocks || j?.data || []);
        setRecent(arr.filter(b => b.validator === r.address).slice(0, 12));
      })
      .catch(() => { if (!cancelled) setRecent([]); });
    return () => { cancelled = true; };
  }, [r.address]);

  const total = Number(r.total) || 0;
  const own = Number(r.own) || 0;
  const other = Number(r.other) || (total - own);
  const noms = Number(r.nominators) || 0;
  const commission = Number(r.commission);
  const points = Number(r.points) || 0;

  return (
    <>
      <div className="drill-section">
        <div className="drill-sec-title">Validator</div>
        <div style={{display:'flex', alignItems:'center', gap: 12}}>
          <div style={{width: 36, height: 36, borderRadius: 8, background:'linear-gradient(135deg,#9B1B30,#4A3566)', flexShrink: 0}}/>
          <div>
            <div style={{fontWeight: 800, fontSize: 16}}>{r.name || (r.address ? fmt.addr(r.address, 8, 6) : '—')}</div>
            {r.rank && <div className="muted tiny">Rank #{r.rank}</div>}
          </div>
        </div>
        {r.address && (
          <div style={{marginTop:10}}>
            <Addr addr={r.address}/>
          </div>
        )}
      </div>
      {total > 0 && (
        <div className="drill-section">
          <div className="drill-sec-title">Stake</div>
          <Field label="Total" mono>{fmt.num(total, 0)} XOR</Field>
          <Field label="Own" mono>{fmt.num(own, 0)} XOR</Field>
          <Field label="Nominators" mono>{noms} · {fmt.num(other, 0)} XOR</Field>
          {total > 0 && (
            <div className="drill-fill-bar" style={{marginTop: 6}}>
              <div className="drill-fill-fill" style={{width: (own / total * 100) + '%', background:'linear-gradient(90deg,#9B1B30,#7B5B90)'}}/>
            </div>
          )}
        </div>
      )}
      <div className="drill-section">
        <div className="drill-sec-title">Performance</div>
        {Number.isFinite(commission) && <Field label="Commission" mono>{commission.toFixed(2)}%</Field>}
        {points > 0 && <Field label="Era points" mono>{points.toLocaleString()}</Field>}
        <Field label="Status">
          {r.status
            ? <span className={'val-status ' + r.status}>
                {r.status === 'active' ? '● Active' : r.status === 'waiting' ? '◌ Waiting' : r.status === 'blocked' ? '⛔ Blocked' : r.status}
              </span>
            : <span className="muted tiny">—</span>}
        </Field>
      </div>
      <div className="drill-section">
        <div className="drill-sec-title">Recent blocks produced</div>
        {recent === null && <div className="muted tiny">Cargando…</div>}
        {recent && recent.length === 0 && <div className="muted tiny">Sin bloques recientes en los últimos 50.</div>}
        {recent && recent.length > 0 && (
          <div style={{display:'flex', flexWrap:'wrap', gap: 6}}>
            {recent.map(b => (
              <code key={b.hash || b.number} className="mono tiny" style={{padding:'3px 7px', background:'rgba(155,27,48,0.08)', border:'1px solid rgba(155,27,48,0.25)', borderRadius: 4, color: 'var(--accent)'}}>
                #{Number(b.number).toLocaleString()}
              </code>
            ))}
          </div>
        )}
      </div>
    </>
  );
}

function BridgeDetail({ r }) {
  // sender / recipient come from /history/global/bridges:
  //   direction = "Outgoing" → SORA sender, Ethereum recipient (0x…)
  //   direction = "Incoming" → SORA hot wallet as "sender", SORA recipient; the
  //                            real Ethereum origin has to be resolved from the
  //                            ethBridge.RequestRegistered event logs.
  const sender = r.sender;
  const recipient = r.recipient;
  const isOut = r.dir === 'out' || /out/i.test(r.dir || '');

  // Incoming-only: resolve the real Ethereum origin wallet.
  //   1. /block/:n → find the extrinsic by hash
  //   2. pick event ethBridge.RequestRegistered → data[0] is the ETH tx hash
  //      (only for native ETH deposits; for internal SORA bridge events this
  //      hash is an internal ID and eth_getTransactionByHash returns null)
  //   3. public Ethereum RPC → result.from = signing EOA
  // v1 used to do this server-side in eth_helper.js but it's disabled for
  // memory reasons, so we run the lookup on-demand in the drill.
  const [ethOrigin, setEthOrigin] = useState({ status: 'idle', from: null, txHash: null });
  useEffect(() => {
    if (isOut || !r.block || !r.hash) return;
    let cancelled = false;
    setEthOrigin({ status: 'loading', from: null, txHash: null });
    (async () => {
      try {
        const block = await fetch('/block/' + encodeURIComponent(r.block)).then(res => res.ok ? res.json() : null);
        const ext = (block?.extrinsics || []).find(e => e.hash === r.hash);
        const reqReg = (ext?.events || []).find(e => e.section === 'ethBridge' && e.method === 'RequestRegistered');
        const ethHash = reqReg?.data?.[0];
        if (!ethHash || !/^0x[0-9a-fA-F]{64}$/.test(ethHash)) {
          if (!cancelled) setEthOrigin({ status: 'no_hash', from: null, txHash: ethHash || null });
          return;
        }
        // Try a couple of public RPCs for reliability (one may rate-limit).
        const RPCS = ['https://eth.llamarpc.com', 'https://cloudflare-eth.com', 'https://rpc.ankr.com/eth'];
        let from = null;
        for (const rpc of RPCS) {
          try {
            const j = await fetch(rpc, {
              method: 'POST',
              headers: {'Content-Type':'application/json'},
              body: JSON.stringify({ jsonrpc:'2.0', method:'eth_getTransactionByHash', params:[ethHash], id:1 })
            }).then(res => res.json());
            if (j?.result?.from) { from = j.result.from; break; }
          } catch {}
        }
        if (cancelled) return;
        setEthOrigin({ status: from ? 'found' : 'internal', from, txHash: ethHash });
      } catch {
        if (!cancelled) setEthOrigin({ status: 'error', from: null, txHash: null });
      }
    })();
    return () => { cancelled = true; };
  }, [isOut, r.block, r.hash]);
  // Pick the right Ethereum-side explorer per network (bridge supports ETH, Polygon, Base, BSC).
  const net = (isOut ? r.to : r.from) || '';
  const ETH_EXPLORERS = {
    Ethereum: 'https://etherscan.io/address/',
    Polygon:  'https://polygonscan.com/address/',
    Base:     'https://basescan.org/address/',
    BSC:      'https://bscscan.com/address/',
  };
  const ETH_TX = {
    Ethereum: 'https://etherscan.io/tx/',
    Polygon:  'https://polygonscan.com/tx/',
    Base:     'https://basescan.org/tx/',
    BSC:      'https://bscscan.com/tx/',
  };
  const explorerBase = ETH_EXPLORERS[net] || 'https://etherscan.io/address/';
  const txExplorerBase = ETH_TX[net] || 'https://etherscan.io/tx/';

  return (
    <>
      <div className="drill-section">
        <div className="drill-sec-title">Bridge Transfer</div>
        <div className="chain-route" style={{fontSize: 14}}>
          <span className={'chain-tag c-' + (r.from || '').toLowerCase()}>{r.from}</span>
          <span className="route-arr">→</span>
          <span className={'chain-tag c-' + (r.to || '').toLowerCase()}>{r.to}</span>
        </div>
        <div style={{marginTop: 14, fontSize: 28, fontWeight: 800, color:'var(--fg-0)', fontFamily:'JetBrains Mono'}}>{fmt.num(r.amt, 2)} {r.sym}</div>
        {r.usd > 0 && <div className="muted tiny" style={{marginTop:4}}>≈ ${fmt.num(r.usd, 2)}</div>}
      </div>

      <div className="drill-section">
        <div className="drill-sec-title">Wallets</div>
        {isOut ? (
          <>
            <Field label="From · SORA">
              {sender ? (
                <div style={{display:'flex', alignItems:'center', gap:6, flex:1, minWidth:0}}>
                  <span className="num tiny" style={{overflowWrap:'anywhere'}}>{sender}</span>
                  <Copy text={sender} short/>
                  <button className="btn" style={{padding:'2px 8px'}} onClick={() => window.openWalletDetails?.(sender, IDENTITIES[sender])} title="Abrir wallet SORA">↗</button>
                </div>
              ) : <span className="muted tiny">—</span>}
            </Field>
            <Field label={'To · ' + net}>
              {recipient ? (
                <div style={{display:'flex', alignItems:'center', gap:6, flex:1, minWidth:0}}>
                  <span className="num tiny" style={{overflowWrap:'anywhere'}}>{recipient}</span>
                  <Copy text={recipient} short/>
                  <a className="btn" style={{padding:'2px 8px'}} href={explorerBase + recipient} target="_blank" rel="noopener noreferrer" title={'Ver en ' + net + ' explorer'}>↗ {net}</a>
                </div>
              ) : <span className="muted tiny">—</span>}
            </Field>
          </>
        ) : (
          <>
            <Field label={'From · ' + net}>
              {ethOrigin.status === 'loading' && <span className="muted tiny">resolviendo desde RPC…</span>}
              {ethOrigin.status === 'found' && ethOrigin.from && (
                <div style={{display:'flex', alignItems:'center', gap:6, flex:1, minWidth:0}}>
                  <span className="num tiny" style={{overflowWrap:'anywhere'}}>{ethOrigin.from}</span>
                  <Copy text={ethOrigin.from} short/>
                  <a className="btn" style={{padding:'2px 8px'}} href={explorerBase + ethOrigin.from} target="_blank" rel="noopener noreferrer" title={'Ver en ' + net + ' explorer'}>↗ {net}</a>
                </div>
              )}
              {ethOrigin.status === 'internal' && (
                <span className="muted tiny" title={ethOrigin.txHash}>Bridge-internal ID · no es un tx {net} directo</span>
              )}
              {(ethOrigin.status === 'no_hash' || ethOrigin.status === 'error') && (
                <span className="muted tiny">No disponible</span>
              )}
            </Field>
            <Field label="To · SORA">
              {recipient ? (
                <div style={{display:'flex', alignItems:'center', gap:6, flex:1, minWidth:0}}>
                  <span className="num tiny" style={{overflowWrap:'anywhere'}}>{recipient}</span>
                  <Copy text={recipient} short/>
                  <button className="btn" style={{padding:'2px 8px'}} onClick={() => window.openWalletDetails?.(recipient, IDENTITIES[recipient])} title="Abrir wallet SORA">↗</button>
                </div>
              ) : <span className="muted tiny">—</span>}
            </Field>
            {/* SORA hot wallet that records the incoming: shown as reference. */}
            {sender && (
              <Field label="Relayer SORA">
                <div style={{display:'flex', alignItems:'center', gap:6, flex:1, minWidth:0}}>
                  <span className="num tiny" style={{overflowWrap:'anywhere', opacity:0.8}}>{sender}</span>
                  <Copy text={sender} short/>
                </div>
              </Field>
            )}
          </>
        )}
      </div>

      <div className="drill-section">
        <div className="drill-sec-title">Status</div>
        <Field label="Current">
          <span className={'br-status ' + r.status}>
            {r.status === 'done' ? '✓ Done' : r.status === 'pending' ? '⏳ Pending' : '✗ Failed'}
          </span>
        </Field>
        <Field label="Tx" mono>
          <span style={{flex:1, overflow:'hidden', textOverflow:'ellipsis'}}>{r.hash}</span>
          {r.hash && <Copy text={r.hash} short/>}
        </Field>
        {/* Outgoing: the SORA extrinsic hash is also the tx on the counter-chain
            (bridgeProxy.burn emits an L1 tx whose hash matches). Link it. */}
        {isOut && r.hash && /^0x[0-9a-f]{64}$/i.test(r.hash) && ETH_TX[net] && (
          <Field label={'Ver en ' + net}>
            <a className="btn" style={{padding:'2px 8px'}} href={txExplorerBase + r.hash} target="_blank" rel="noopener noreferrer">↗ {net}scan</a>
          </Field>
        )}
        {/* Incoming: expose the ETH request hash pulled from ethBridge.RequestRegistered. */}
        {!isOut && ethOrigin.txHash && (
          <Field label={net + ' tx'} mono>
            <span style={{flex:1, overflow:'hidden', textOverflow:'ellipsis'}}>{ethOrigin.txHash.slice(0, 12)}…{ethOrigin.txHash.slice(-8)}</span>
            <Copy text={ethOrigin.txHash} short/>
            {ethOrigin.status === 'found' && ETH_TX[net] && (
              <a className="btn" style={{padding:'2px 8px'}} href={txExplorerBase + ethOrigin.txHash} target="_blank" rel="noopener noreferrer">↗ {net}scan</a>
            )}
          </Field>
        )}
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

// Fallback playlist when prod /music/list is unreachable. Replaced at runtime
// by the real response (see fetchTracks below).
const FALLBACK_TRACKS = [
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
  // G17: pull the real playlist from prod /music/list — JSON array of
  // { title, artist, src }. src is a relative URL like /music/Foo.mp3 which
  // gets served directly by the proxy (same-origin) so <audio> can load it.
  const [tracks, setTracks] = useState(FALLBACK_TRACKS);
  const audioRef = useRef(null);
  useEffect(() => {
    let cancelled = false;
    fetch('/music/list').then(r => r.ok ? r.json() : null).then(j => {
      if (cancelled || !Array.isArray(j) || j.length === 0) return;
      // Normalise — prod responses don't carry a dur field. We'll rely on the
      // audio element's metadata to fill in once loaded (see onLoadedMetadata).
      setTracks(j.map(t => ({ title: t.title, artist: t.artist, src: t.src, dur: 180 })));
    }).catch(() => {});
    return () => { cancelled = true; };
  }, []);

  const track = tracks[trackIdx] || tracks[0];

  // Real audio playback: when `playing` flips on + we have a src, trigger the
  // HTMLAudioElement. When a track changes, set src + (optionally) play.
  useEffect(() => {
    const el = audioRef.current;
    if (!el) return;
    if (track?.src && el.src !== track.src) {
      el.src = track.src;
      el.load();
      if (playing) el.play().catch(() => {});
    }
    el.volume = volume;
    if (playing) { el.play().catch(() => {}); }
    else el.pause();
  }, [track?.src, playing, volume]);

  // Tick: drive `elapsed` from the real audio currentTime when src is loaded,
  // fall back to +1s interval simulation when no src (fallback playlist).
  useEffect(() => {
    if (!playing) return;
    const el = audioRef.current;
    if (el && el.src) {
      const onTime = () => setElapsed(Math.floor(el.currentTime));
      const onEnd = () => setTrackIdx(i => (i + 1) % tracks.length);
      el.addEventListener('timeupdate', onTime);
      el.addEventListener('ended', onEnd);
      return () => {
        el.removeEventListener('timeupdate', onTime);
        el.removeEventListener('ended', onEnd);
      };
    }
    // Fallback: simulated ticker (no real audio)
    const id = setInterval(() => {
      setElapsed(e => {
        if (e + 1 >= (track?.dur || 180)) {
          setTrackIdx(i => (i + 1) % tracks.length);
          return 0;
        }
        return e + 1;
      });
    }, 1000);
    return () => clearInterval(id);
  }, [playing, track?.dur, tracks.length]);

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

  const next = () => setTrackIdx(i => (i + 1) % tracks.length);
  const prev = () => setTrackIdx(i => (i - 1 + tracks.length) % tracks.length);

  const stars = [0,1,2,3,4];

  return (
    <>
      {/* Real audio element — hidden, driven by the UI controls above. Only
          renders a src when /music/list returned real tracks; otherwise the
          fallback simulated ticker handles progress. */}
      <audio ref={audioRef} preload="metadata" style={{display:'none'}}
             onLoadedMetadata={(e) => {
               // Update track.dur with real file duration once metadata arrives.
               const d = Math.floor(e.currentTarget.duration);
               if (Number.isFinite(d) && d > 0) {
                 setTracks(ts => ts.map((t, i) => i === trackIdx ? { ...t, dur: d } : t));
               }
             }}/>
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
              {tracks.map((t, i) => (
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
