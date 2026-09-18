/* global React, fmt, FAKE_ADDRS, IDENTITIES, TOKENS, sparkPath, I, useDrill, useT, io, TimeRangePills, useTimeRange */
const { useState, useEffect, useRef, useMemo } = React;

// Shared socket.io connection. Created lazily the first time a component asks for it.
// Proxied to https://sorametrics.org via v6-server.js — same-origin from the browser.
let __pulseSocket = null;
function getPulseSocket() {
  if (__pulseSocket) return __pulseSocket;
  if (typeof io !== 'function') return null;
  __pulseSocket = io('/', {
    transports: ['websocket', 'polling'],
    reconnection: true,
    reconnectionDelay: 1500,
  });
  return __pulseSocket;
}

// Parse timestamps that come from prod as "D/M/YYYY, HH:MM:SS" (Spanish locale).
// Fall back to Date.now() when unparseable.
function parseTime(t) {
  if (!t) return Date.now();
  if (typeof t === 'number') return t;
  // Try ISO first (plain Date.parse works for ISO 8601).
  const iso = Date.parse(t);
  if (!Number.isNaN(iso)) return iso;
  // Spanish locale format: "18/4/2026, 16:54:12"
  const re = /^(\d{1,2})\/(\d{1,2})\/(\d{4})(?:,\s*(\d{1,2}):(\d{2})(?::(\d{2}))?)?$/;
  const m = String(t).trim().match(re);
  if (m) {
    const [, d, mo, y, hh = '0', mm = '0', ss = '0'] = m;
    const dt = new Date(Number(y), Number(mo) - 1, Number(d), Number(hh), Number(mm), Number(ss));
    if (!Number.isNaN(dt.getTime())) return dt.getTime();
  }
  return Date.now();
}

// Noise filter — skip per-block housekeeping extrinsics. They're 99% of the
// feed otherwise, and the prod UI hides them in Pulse too.
const EXTRINSIC_NOISE = new Set(['timestamp::set', 'imOnline::heartbeat', 'parachainSystem::setValidationData']);

// Map real prod backend batches → feed items that match the prototype's shape.
// Returning null skips the event (e.g. transfer from == to, or noise we don't want).
function makeFeedItemFromSwap(s, idx) {
  // swaps-batch row shape: { time, block, wallet, in:{amount,symbol,logo,usd}, out:{amount,symbol,logo}, hash, extrinsic_id }
  if (!s || !s.in || !s.out) return null;
  const amtIn = Number(s.in.amount || 0);
  const amtOut = Number(s.out.amount || 0);
  const usd = Number(s.in.usd || 0);
  return {
    id: 'wsS-' + (s.hash || (s.block + ':' + idx)),
    kind: 'swap',
    ts: parseTime(s.time),
    raw: s,
    line1: React.createElement(React.Fragment, null,
      'Swap ',
      React.createElement('b', null, fmt.num(amtIn, 2) + ' ' + (s.in.symbol || '?')),
      ' → ',
      React.createElement('b', null, fmt.num(amtOut, 2) + ' ' + (s.out.symbol || '?')),
    ),
    line2: fmt.addr(s.wallet || '') + ' · fee 0.3% · ' + (usd ? fmt.usd(usd) : ''),
  };
}
function makeFeedItemFromTransfer(t, idx) {
  // transfers-batch row shape: { time, block, from, to, amount, symbol, usdValue, logo, hash, extrinsic_id }
  if (!t) return null;
  const amt = Number(t.amount || 0);
  return {
    id: 'wsT-' + (t.hash || (t.block + ':' + idx)),
    kind: 'transfer',
    ts: parseTime(t.time),
    raw: t,
    line1: React.createElement(React.Fragment, null,
      'Transfer ',
      React.createElement('b', null, fmt.num(amt, 2) + ' ' + (t.symbol || '?')),
    ),
    line2: fmt.addr(t.from || '') + ' → ' + fmt.addr(t.to || ''),
  };
}
function makeFeedItemFromExtrinsic(x, idx) {
  // extrinsics-batch row shape: { time, block, extrinsic_index, extrinsic_id, section, method, signer, success, error_msg }
  if (!x) return null;
  const tag = (x.section || '?') + '::' + (x.method || '?');
  // Filter out per-block housekeeping extrinsics — they'd drown the feed.
  if (EXTRINSIC_NOISE.has(tag)) return null;
  const isOrder = x.section === 'orderBook' || x.section === 'orderbook';
  return {
    id: 'wsX-' + (x.extrinsic_id || (x.block + ':' + (x.extrinsic_index || idx))),
    kind: isOrder ? 'order' : 'block',
    ts: parseTime(x.time),
    raw: x,
    line1: React.createElement(React.Fragment, null,
      React.createElement('b', null, tag),
      ' ',
      x.success === false ? React.createElement('span', { className: 'tag err' }, 'failed') : null,
    ),
    line2: fmt.addr(x.signer || '') + ' · block ' + x.block,
  };
}
function makeFeedItemFromOrder(o, idx) {
  // orderbook-batch row shape: { time, block, event_type, base_asset, quote_asset, side, price, amount, wallet, hash, extrinsic_id }
  if (!o) return null;
  const side = (o.side || '').toUpperCase();
  return {
    id: 'wsO-' + (o.hash || (o.block + ':' + idx)),
    kind: 'order',
    ts: parseTime(o.time),
    raw: o,
    line1: React.createElement(React.Fragment, null,
      React.createElement('b', null, side),
      ' order ',
      React.createElement('b', null, fmt.num(Number(o.amount || 0), 0) + ' ' + (o.base_asset || '')),
      ' @ $',
      Number(o.price || 0).toFixed(4),
    ),
    line2: (o.base_asset || '') + '/' + (o.quote_asset || '') + ' · ' + fmt.addr(o.wallet || ''),
  };
}
// Prod emits new-block-stats for EVERY new head. `finalized` is the most
// recent finalized block NUMBER (not a boolean). To avoid one "Block #...
// finalized" row per second, we only push when the finalized number actually
// advances — reducing the feed to ~1 row per ~20 s of real finalization.
function makeFeedItemFromBlock(b, prevFinalized) {
  if (!b || !b.block) return null;
  const finalizedNum = Number(b.finalized);
  if (!finalizedNum) return null;
  if (prevFinalized && finalizedNum <= prevFinalized) return null;
  return {
    id: 'wsB-' + finalizedNum,
    kind: 'block',
    ts: Date.now(),
    raw: b,
    line1: React.createElement(React.Fragment, null,
      'Block ',
      React.createElement('b', null, '#' + finalizedNum.toLocaleString()),
      ' finalized',
    ),
    line2: 'avg time ' + (b.avgTime ? Number(b.avgTime).toFixed(2) + 's' : '—'),
  };
}

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

// Explorador Completo — paginated block list with deep-link to BlockDetail.
// Pulls /staking/recent-blocks?limit=N&before=<n> (prod supports both). Each
// row opens the drill so the user can inspect extrinsics + events.
function FullExplorerModal({ open, onClose, initialBlock }) {
  const t = useT();
  const { open: openDrill } = useDrill();
  const LIMIT = 25;
  const [anchor, setAnchor] = useState(null);       // highest block on current page
  const [blocks, setBlocks] = useState([]);
  const [loading, setLoading] = useState(false);
  const [goto, setGoto] = useState('');

  // Whenever the modal opens, anchor either at initialBlock (when a search
  // came in from the header input) or at the latest finalized head. anchor
  // is "highest block on current page exclusive", so initialBlock+1 makes
  // the page render initialBlock down through initialBlock-24.
  useEffect(() => {
    if (!open) return;
    if (initialBlock && Number.isFinite(Number(initialBlock))) {
      setAnchor(Number(initialBlock) + 1);
    } else {
      setAnchor(null);
    }
    setBlocks([]);
    setGoto('');
  }, [open, initialBlock]);

  // Fetch whenever the anchor changes (or modal first opens without an anchor).
  // Two strategies: the "latest" page uses /staking/recent-blocks (which gives
  // validator names); paging backwards uses parallel /block/:n calls since the
  // recent-blocks endpoint ignores ?before= in current prod.
  useEffect(() => {
    if (!open) return;
    let cancelled = false;
    setLoading(true);

    if (!anchor) {
      // Head view — use recent-blocks for the validator names.
      fetch('/staking/recent-blocks?limit=' + LIMIT)
        .then(r => r.ok ? r.json() : null)
        .then(j => {
          if (cancelled) return;
          const arr = Array.isArray(j) ? j : (j?.blocks || j?.data || []);
          const seen = new Set();
          const unique = arr.filter(b => {
            const n = Number(b.number);
            if (!Number.isFinite(n) || seen.has(n)) return false;
            seen.add(n);
            return true;
          });
          setBlocks(unique);
          setLoading(false);
        })
        .catch(() => { if (!cancelled) setLoading(false); });
    } else {
      // Arbitrary range — fetch /block/:n for n = anchor-1 … anchor-LIMIT in
      // parallel. /block/:n does not carry a validator field; we display "—".
      const top = Number(anchor) - 1;
      const nums = Array.from({ length: LIMIT }, (_, i) => top - i).filter(n => n > 0);
      Promise.all(nums.map(n =>
        fetch('/block/' + n)
          .then(r => r.ok ? r.json() : null)
          .then(j => j ? ({
            number: j.number,
            hash: j.hash,
            timestamp: j.timestamp,
            extrinsics: j.totalExtrinsics,
            validator: null,
            validatorName: null,
          }) : null)
          .catch(() => null)
      ))
        .then(results => {
          if (cancelled) return;
          setBlocks(results.filter(Boolean));
          setLoading(false);
        });
    }
    return () => { cancelled = true; };
  }, [open, anchor]);

  if (!open) return null;

  const prev = () => {
    if (blocks.length === 0) return;
    const oldest = blocks[blocks.length - 1].number;
    setAnchor(Number(oldest));
  };
  const next = () => {
    if (blocks.length === 0) return;
    const newest = Number(blocks[0].number);
    setAnchor(newest + LIMIT + 1);
  };
  const resetToHead = () => setAnchor(null);
  const submitGoto = (e) => {
    e?.preventDefault?.();
    const n = parseInt(String(goto).replace(/[^\d]/g, ''), 10);
    if (Number.isFinite(n) && n > 0) setAnchor(n + 1); // +1 so "before=n+1" includes n
  };

  return (
    <div onClick={onClose} style={{position:'fixed', inset:0, background:'rgba(0,0,0,0.72)', backdropFilter:'blur(6px)', zIndex:9000, display:'flex', alignItems:'center', justifyContent:'center', padding:20}}>
      <div onClick={e => e.stopPropagation()} style={{background:'var(--bg-card)', color:'var(--fg-0)', borderRadius:14, maxWidth:920, width:'100%', maxHeight:'88vh', overflow:'auto', border:'1px solid var(--border-strong)'}}>
        <div style={{display:'flex', alignItems:'center', justifyContent:'space-between', padding:'18px 22px', borderBottom:'1px solid var(--border)'}}>
          <div>
            <h3 style={{margin:0, fontSize:17}}>{t('explorer.title', 'Explorador de bloques')}</h3>
            <div className="muted tiny" style={{marginTop:2}}>{t('explorer.sub', 'Pagina hacia atrás o salta a cualquier bloque')}</div>
          </div>
          <button className="btn" onClick={onClose}>{t('common.close', 'Cerrar')}</button>
        </div>
        <div style={{padding:'12px 22px', display:'flex', gap:10, flexWrap:'wrap', alignItems:'center', borderBottom:'1px solid var(--border)'}}>
          <form onSubmit={submitGoto} style={{display:'flex', gap:6}}>
            <input
              className="sm-input"
              placeholder={t('explorer.gotoPlaceholder', 'Ir a bloque #…')}
              value={goto}
              onChange={e => setGoto(e.target.value)}
              style={{width:160, padding:'6px 10px', fontSize:13}}/>
            <button type="submit" className="btn" style={{padding:'6px 12px'}}>{t('explorer.go', 'Ir')}</button>
          </form>
          <div style={{flex:1}}/>
          <button className="btn" onClick={resetToHead} disabled={!anchor}>⟳ {t('explorer.latest', 'Último')}</button>
          <button className="btn" onClick={next} disabled={!anchor || loading}>← {t('explorer.newer', 'Más reciente')}</button>
          <button className="btn" onClick={prev} disabled={loading || blocks.length === 0}>{t('explorer.older', 'Más antiguo')} →</button>
        </div>
        <div style={{padding:'0 22px 16px'}}>
          <table className="lp-table" style={{width:'100%', marginTop:12}}>
            <thead>
              <tr>
                <th style={{textAlign:'left'}}>{t('explorer.col.block', 'Bloque')}</th>
                <th style={{textAlign:'left'}}>{t('explorer.col.time', 'Hora')}</th>
                <th style={{textAlign:'left'}}>{t('explorer.col.validator', 'Validator')}</th>
                <th style={{textAlign:'right'}}>{t('explorer.col.extrinsics', 'Extrinsics')}</th>
                <th style={{textAlign:'left'}}>{t('explorer.col.hash', 'Hash')}</th>
              </tr>
            </thead>
            <tbody>
              {loading && blocks.length === 0 && (
                <tr><td colSpan={5} style={{padding:32, textAlign:'center', color:'var(--fg-2)'}}>{t('explorer.loading', 'Cargando…')}</td></tr>
              )}
              {!loading && blocks.length === 0 && (
                <tr><td colSpan={5} style={{padding:32, textAlign:'center', color:'var(--fg-2)'}}>{t('explorer.empty', 'Sin bloques en este rango.')}</td></tr>
              )}
              {blocks.map(b => {
                const ts = Number(b.timestamp) || 0;
                return (
                  <tr key={b.number + '-' + (b.hash || '')}
                      className="clickable"
                      style={{cursor:'pointer'}}
                      onClick={() => openDrill({ type:'block', title:'BLOCK #' + b.number, block: Number(b.number), num: Number(b.number), ts })}>
                    <td className="num" style={{fontWeight:700}}>#{Number(b.number).toLocaleString()}</td>
                    <td className="muted tiny">
                      <div>{fmt.ago(ts)}</div>
                      <div style={{opacity:0.7}}>{new Date(ts).toLocaleString()}</div>
                    </td>
                    <td className="tiny">{b.validatorName || (b.validator ? fmt.addr(b.validator, 5, 4) : '—')}</td>
                    <td className="num tiny" style={{textAlign:'right'}}>{b.extrinsics ?? '—'}</td>
                    <td className="num tiny" style={{opacity:0.8}}>{b.hash ? b.hash.slice(0,10) + '…' + b.hash.slice(-6) : '—'}</td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  );
}

function PulseSection({ tweaks }) {
  const t = useT();
  const { open } = useDrill();
  const [filter, setFilter] = useState('all');
  const [events, setEvents] = useState([]);
  const [paused, setPaused] = useState(false);
  const [explorerOpen, setExplorerOpen] = useState(false);
  // Block search input — feeds initialBlock to FullExplorerModal so the
  // user can jump straight to a specific block (or open the explorer
  // anchored at the current head when the input is empty).
  const [blockSearch, setBlockSearch] = useState("");
  const [blockSearchInitial, setBlockSearchInitial] = useState(null);
  const [blockSearchErr, setBlockSearchErr] = useState(false);

  // External jump-to-block: the topbar block-search dispatches this event
  // after navTo("pulse") so the modal opens with the requested block.
  useEffect(() => {
    const handler = (e) => {
      const n = Number(e?.detail?.block);
      if (!Number.isFinite(n) || n <= 0) return;
      setBlockSearchInitial(n);
      setExplorerOpen(true);
    };
    window.addEventListener("sm:openBlockExplorer", handler);
    return () => window.removeEventListener("sm:openBlockExplorer", handler);
  }, []);
  const [connected, setConnected] = useState(false);
  const [currentBlock, setCurrentBlock] = useState(null);
  const [tick, setTick] = useState(0);
  const pausedRef = useRef(paused);
  pausedRef.current = paused;
  // Track last finalized block number so block events aren't duplicated.
  const lastFinalizedRef = useRef(0);
  // Real stats from prod — replaces hardcoded KPI + Trending + Health values.
  const [statsHeader, setStatsHeader] = useState(null);     // { block, swaps, transfers, bridges }
  const [statsOverview, setStatsOverview] = useState(null); // { pegs, network:{volume, users, txCount, ...}, trends }
  const [statsNetwork, setStatsNetwork] = useState(null);   // { stats24h, stats7d, tps }
  const [stakingNet, setStakingNet] = useState(null);       // { validatorCount, eraProgress, avgBlockTime, bestBlock, ... }
  const [trending, setTrending] = useState([]);             // [{ symbol, volume, logo }]
  const [range, setRange] = useTimeRange('24h');            // G7: shared pill — prod only exposes 24h + 7d here

  // Fetch all 5 stats endpoints + refresh every 30s.
  useEffect(() => {
    let cancelled = false;
    const pull = async () => {
      try {
        const [h, o, n, sk, tr] = await Promise.all([
          fetch('/stats/header').then(r => r.json()).catch(() => null),
          fetch('/stats/overview').then(r => r.json()).catch(() => null),
          fetch('/stats/network').then(r => r.json()).catch(() => null),
          fetch('/staking/network').then(r => r.json()).catch(() => null),
          fetch('/stats/trending-tokens').then(r => r.json()).catch(() => []),
        ]);
        if (cancelled) return;
        setStatsHeader(h); setStatsOverview(o); setStatsNetwork(n);
        setStakingNet(sk); setTrending(Array.isArray(tr) ? tr : []);
      } catch {}
    };
    pull();
    const id = setInterval(pull, 30_000);
    return () => { cancelled = true; clearInterval(id); };
  }, []);

  // Wire socket.io to the real prod backend.
  // Events emitted by index.js: new-block-stats, transfers-batch,
  // swaps-batch, extrinsics-batch, orderbook-batch.
  useEffect(() => {
    const sock = getPulseSocket();
    if (!sock) {
      // socket.io library didn't load (offline?) — keep feed empty.
      return;
    }
    const push = (item) => {
      if (!item || pausedRef.current) return;
      setEvents(prev => {
        // Dedup by id + cap ring buffer at 40 items.
        if (prev.some(p => p.id === item.id)) return prev;
        return [item, ...prev].slice(0, 40);
      });
    };
    const onConnect = () => setConnected(true);
    const onDisconnect = () => setConnected(false);
    const onBlock = (b) => {
      if (b && b.block) setCurrentBlock(b.block);
      const item = makeFeedItemFromBlock(b, lastFinalizedRef.current);
      if (item) {
        lastFinalizedRef.current = Number(b.finalized);
        push(item);
      }
    };
    const onSwaps = (batch) => Array.isArray(batch) && batch.forEach((s, i) => push(makeFeedItemFromSwap(s, i)));
    const onTransfers = (batch) => Array.isArray(batch) && batch.forEach((r, i) => push(makeFeedItemFromTransfer(r, i)));
    const onExtrinsics = (batch) => Array.isArray(batch) && batch.forEach((x, i) => push(makeFeedItemFromExtrinsic(x, i)));
    const onOrders = (batch) => Array.isArray(batch) && batch.forEach((o, i) => push(makeFeedItemFromOrder(o, i)));

    sock.on('connect', onConnect);
    sock.on('disconnect', onDisconnect);
    sock.on('new-block-stats', onBlock);
    sock.on('swaps-batch', onSwaps);
    sock.on('transfers-batch', onTransfers);
    sock.on('extrinsics-batch', onExtrinsics);
    sock.on('orderbook-batch', onOrders);
    // Adopt current connection state in case we mounted after the connect event.
    if (sock.connected) setConnected(true);

    return () => {
      sock.off('connect', onConnect);
      sock.off('disconnect', onDisconnect);
      sock.off('new-block-stats', onBlock);
      sock.off('swaps-batch', onSwaps);
      sock.off('transfers-batch', onTransfers);
      sock.off('extrinsics-batch', onExtrinsics);
      sock.off('orderbook-batch', onOrders);
    };
  }, []);

  // re-render clock for "ago"
  useEffect(() => {
    const id = setInterval(() => setTick(tk => tk + 1), 1000);
    return () => clearInterval(id);
  }, []);

  const counts = useMemo(() => {
    const c = { all: events.length };
    events.forEach(e => { c[e.kind] = (c[e.kind] || 0) + 1; });
    return c;
  }, [events]);

  const filtered = filter === 'all' ? events : events.filter(e => e.kind === filter);

  // Sparklines removed — we don't have a real time-series endpoint for
  // swaps/volume/wallets/block-time; drawing them from Math.sin + random
  // would just be decorative fiction. If /stats/network later exposes a
  // rolling series per bucket we can plug it in here.

  return (
    <div>
      <PageHeader title={t('pulse.title')} sub={t('pulse.sub')}>
        <TimeRangePills value={range} onChange={setRange}/>
        <span className={'tag ' + (connected ? 'ok' : 'err')}>
          <span className="live-dot" style={{width:5,height:5}}/>
          {' '}
          {connected ? t('common.connected') : 'disconnected'}
          {currentBlock ? ' · #' + Number(currentBlock).toLocaleString() : ''}
        </span>
        <button className="btn" onClick={() => setPaused(p => !p)}>
          {paused ? t('common.resume') : t('common.pause')}
        </button>
        <button className="btn" onClick={() => { setBlockSearchInitial(null); setExplorerOpen(true); }}>{t('btn.fullExplorer', 'Full Explorer')}</button>
      </PageHeader>
      <FullExplorerModal open={explorerOpen} onClose={() => setExplorerOpen(false)} initialBlock={blockSearchInitial || currentBlock}/>

      <div className="pulse-grid">
        {(() => {
          // Pulse KPIs switch between stats24h / stats7d when the range pill
          // flips. Longer ranges (1M/1Y) fall back to 7d — prod doesn't expose
          // those windows on /stats/network.
          const use7d = range === '7d' || range === '1m' || range === '1y';
          const bucket = use7d ? statsNetwork?.stats7d : statsNetwork?.stats24h;
          const label24or7 = use7d ? '7D' : '24H';
          return (
            <>
              <PulseStat
                label={'SWAPS · ' + label24or7}
                value={bucket ? Number(bucket.txCount || 0).toLocaleString() : '—'}
                sub={statsNetwork ? 'vs ' + Number((use7d ? statsNetwork.stats24h : statsNetwork.stats7d)?.txCount || 0).toLocaleString() + ' ' + (use7d ? '24H' : '7D') : 'loading…'}/>
              <PulseStat
                label={'VOLUME · ' + label24or7}
                value={bucket ? fmt.usd(bucket.volume || 0) : '—'}
                sub={statsNetwork ? (use7d ? '24H: ' : '7D: ') + fmt.usd((use7d ? statsNetwork.stats24h : statsNetwork.stats7d)?.volume || 0) : '—'}/>
              <PulseStat
                label={'ACTIVE WALLETS · ' + label24or7}
                value={bucket ? Number(bucket.users || 0).toLocaleString() : '—'}
                sub={statsNetwork ? Number((use7d ? statsNetwork.stats24h : statsNetwork.stats7d)?.users || 0).toLocaleString() + ' ' + (use7d ? '24H' : '7D') : 'unique signers'}/>
              <PulseStat
                label={t('pulse.kpi.block')}
                value={stakingNet ? (Number(stakingNet.avgBlockTime || 0)).toFixed(2) + 's' : '—'}
                sub={stakingNet ? 'best #' + Number(stakingNet.bestBlock || 0).toLocaleString() : 'finality'}/>
            </>
          );
        })()}
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
                   onClick={() => {
                     // For block rows we fetch the real block (num from id "wsB-<n>");
                     // BlockDetail then calls /block/:n. For other kinds we still
                     // synthesize a payload — those drills are not yet wired to real
                     // endpoints and would look empty otherwise.
                     const rawBlock = e.raw?.finalized || (e.id?.startsWith('wsB-') ? Number(e.id.slice(4)) : null);
                     if (e.kind === 'block' && rawBlock) {
                       open({ type: 'block', title: 'BLOCK · ' + fmt.ago(e.ts) + ' ago', ts: e.ts, block: rawBlock, num: rawBlock });
                       return;
                     }
                     open({
                       type: e.kind, title: e.kind.toUpperCase() + ' · ' + fmt.ago(e.ts) + ' ago',
                       ts: e.ts, hash: '0x' + Math.random().toString(16).slice(2, 18),
                       block: 21418802 + (e.id.toString().length), caller: FAKE_ADDRS[0],
                       inSym:'XOR', outSym:'VAL', inAmt: 12.4, outAmt: 1.8,
                       sym: 'XOR', amt: 12.4, usd: 124, from: FAKE_ADDRS[0], to: FAKE_ADDRS[1],
                       side: 'buy', pair: 'KUSD/XOR', size: 1500, price: 0.42, filled: 62,
                     });
                   }}>
                <span className={'feed-kind ' + e.kind}>{e.kind}</span>
                <div className="feed-body">
                  <div className="line1">{e.line1}</div>
                  <div className="line2">{e.line2}</div>
                </div>
                <div className="feed-time" title={fmt.fullDate(e.ts)}>{fmt.ago(e.ts)}</div>
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
              {trending.length === 0 ? (
                <div className="muted tiny" style={{padding: 12}}>Cargando…</div>
              ) : trending.slice(0, 6).map((tk, i) => {
                const sym = tk.symbol;
                const vol = Number(tk.volume) || 0;
                const fallback = TOKENS[sym] || { grad: 'linear-gradient(135deg,#7B5B90,#4A3566)', name: sym };
                return (
                  <div key={sym + i} className="holder-row" style={{ gridTemplateColumns: '32px 1fr 80px 80px' }}>
                    <TokenLogo sym={sym} logo={tk.logo} size={24}/>
                    <div className="holder-addr"><span className="ident">{sym}</span> <span className="muted tiny">· {fallback.name}</span></div>
                    <div className="holder-pct num" style={{ textAlign:'right', fontWeight: 600 }}>
                      {fmt.usd(vol)}
                    </div>
                    <div className="muted tiny" style={{ textAlign:'right' }}>24h vol</div>
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
              {(() => {
                // Derived from /staking/network + /stats/network
                const vCount = Number(stakingNet?.validatorCount || 0);
                const eraProgress = Number(stakingNet?.eraProgress || 0);
                const finalityLag = stakingNet ? Number(stakingNet.bestBlock || 0) - Number(stakingNet.finalizedBlock || 0) : 0;
                const tps = statsNetwork?.tps || (stakingNet ? (Number(statsNetwork?.stats24h?.txCount || 0) / 86400).toFixed(3) : '—');
                const rows = [
                  { l: 'Validadores', v: stakingNet ? vCount + ' activos' : '—', ok: vCount > 0 },
                  { l: 'Era',          v: stakingNet ? '#' + stakingNet.activeEra : '—', ok: !!stakingNet },
                  { l: 'Era Progress', v: stakingNet ? eraProgress.toFixed(0) + '%' : '—', ok: !!stakingNet, bar: eraProgress },
                  { l: 'Finality Lag', v: stakingNet ? finalityLag + ' blocks' : '—', ok: finalityLag <= 2 },
                  { l: 'TPS',          v: tps, ok: true },
                ];
                return rows.map((r, i) => (
                  <div key={i} style={{display:'flex', alignItems:'center', gap: 10, fontSize: 13}}>
                    <span style={{flex: 1, color: 'var(--fg-2)'}}>{r.l}</span>
                    {r.bar != null && (
                      <div className="holder-bar" style={{ width: 80 }}>
                        <div className="fill" style={{ width: r.bar + '%' }}/>
                      </div>
                    )}
                    <span className="num" style={{ color: r.ok ? '#6EE7B7' : '#FCA5A5', fontWeight: 700 }}>{r.v}</span>
                  </div>
                ));
              })()}
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}

Object.assign(window, { PulseSection });
