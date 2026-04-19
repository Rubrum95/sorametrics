/* global React, fmt, TOKENS, FAKE_ADDRS, IDENTITIES, seededRand, sparkPath, areaPath, I, useDrill, useT, useWallets, AddWalletModal, WalletDetailsModal, ExportCsvButton, exportCsv, useToast */
const { useState, useMemo, useEffect } = React;

/* =========================================================================
   Shared: KpiGrid, MiniSpark, TokenBadge, TokenPair
   ========================================================================= */

function KpiGrid({ items }) {
  return (
    <div className="swaps-stats-grid">
      {items.map((k, i) => (
        <div className="stat-card" key={i}>
          <span className="stat-label">{k.label}</span>
          <span className="stat-value num" style={k.valStyle || {}}>
            {k.value}
            {k.unit && <span style={{fontSize: 15, color:'var(--fg-2)', marginLeft: 6}}>{k.unit}</span>}
          </span>
          {k.delta && <span className={'stat-delta ' + (k.deltaDir || 'up')}>{k.delta}</span>}
          {k.sub && <span className="stat-sub">{k.sub}</span>}
        </div>
      ))}
    </div>
  );
}

function MiniSpark({ data, color = '#9B1B30', w = 72, h = 26 }) {
  if (!data) return null;
  return (
    <svg viewBox={`0 0 ${w} ${h}`} width={w} height={h}>
      <path d={sparkPath(data, w, h, 2)} stroke={color} strokeWidth="1.4" fill="none"/>
    </svg>
  );
}

function TokenBadge({ sym, size = 22 }) {
  const t = TOKENS[sym] || {};
  return (
    <span className="token-dot"
      style={{width: size, height: size, background: t.grad || 'linear-gradient(135deg,#64748B,#334155)'}}>
      {sym[0]}
    </span>
  );
}

function TokenPair({ a, b }) {
  return (
    <span className="tok-pair">
      <TokenBadge sym={a} size={22}/>
      <TokenBadge sym={b} size={22}/>
      <span className="tok-pair-label">{a} · {b}</span>
    </span>
  );
}

function Pagination({ page, setPage, total, pageSize }) {
  const t = useT();
  const totalPages = Math.max(1, Math.ceil(total / pageSize));
  const cur = Math.min(page, totalPages);
  return (
    <div className="swaps-pag">
      <button className="btn" disabled={cur === 1} onClick={() => setPage(1)}>{t('pag.first')}</button>
      <button className="btn" disabled={cur === 1} onClick={() => setPage(p => Math.max(1, p-1))}>{t('pag.prev')}</button>
      <span className="pag-indicator">{t('pag.pageOf')} {cur} {t('pag.of')} {totalPages}</span>
      <button className="btn" disabled={cur === totalPages} onClick={() => setPage(p => Math.min(totalPages, p+1))}>{t('pag.next')}</button>
      <button className="btn" disabled={cur === totalPages} onClick={() => setPage(totalPages)}>{t('pag.last')}</button>
    </div>
  );
}

function Tabs({ tabs, current, onChange }) {
  return (
    <div className="route-tabs">
      {tabs.map(t => (
        <button key={t.id}
          className={'route-tab' + (current === t.id ? ' active' : '')}
          onClick={() => onChange(t.id)}>
          {t.label}
          {t.count != null && <span className="route-tab-count">{t.count}</span>}
        </button>
      ))}
    </div>
  );
}

/* =========================================================================
   TRANSFERS
   ========================================================================= */

function TransfersSection({ tweaks }) {
  const t = useT();
  const { open } = useDrill();
  const [page, setPage] = useState(1);
  const [assetFilter, setAssetFilter] = useState(null);

  // Real transfers from prod /history/global/transfers. Shape mapping:
  //   prod { time, block, hash, extrinsic_id, from, to, amount, symbol, logo, usdValue }
  //   prototype { id, ts, block, sym, from, to, amt, usd, fee, memo }
  const { items: rawTransfers } = useHistory('/history/global/transfers', { pageSize: 50, page: 1, pollMs: 20_000 });
  const rows = useMemo(() => {
    if (!rawTransfers || rawTransfers.length === 0) return [];
    return rawTransfers.map((r, i) => ({
      id: 't-' + (r.hash || (r.block + ':' + i)),
      ts: parseHistTime(r.time),
      block: r.block,
      hash: r.hash,
      sym: r.symbol,
      from: r.from,
      to: r.to,
      amt: Number(r.amount) || 0,
      usd: Number(r.usdValue) || 0,
      fee: 0, // prod /transfers doesn't expose per-row fee; deferred
      memo: '',
    }));
  }, [rawTransfers]);

  const filtered = assetFilter ? rows.filter(r => r.sym === assetFilter) : rows;
  const pageSize = tweaks.density === 'compact' ? 12 : tweaks.density === 'spacious' ? 6 : 10;
  const visible = filtered.slice((page-1) * pageSize, page * pageSize);
  const total = rows.length;

  return (
    <div>
      <PageHeader title={t('transfers.title')} sub={t('transfers.sub')}>
        <span className="tag ok"><span className="live-dot" style={{width:5,height:5}}/> {t('btn.streaming')}</span>
        <ExportCsvButton section="transfers"
          headers={['Time','Block','Asset','From','To','Amount','USD','Fee','Memo']}
          rows={filtered.map(r => ({
            Time: new Date(r.ts).toISOString(),
            Block: r.block,
            Asset: r.sym,
            From: r.from,
            To: r.to,
            Amount: r.amt,
            USD: r.usd.toFixed(2),
            Fee: r.fee,
            Memo: r.memo,
          }))}/>
      </PageHeader>

      <KpiGrid items={[
        { label: t('nav.transfers') + ' · 24h', value: (total*412).toLocaleString(), delta: '▲ 4.8%', deltaDir: 'up' },
        { label: t('col.volume') + ' · 24h',    value: '$14.82', unit: 'M', sub: 'across all assets' },
        { label: 'Top Sender',      value: 'Polkaswap', valStyle:{fontSize: 20}, sub: '1,240 transfers' },
        { label: 'Counterparties',  value: '2,108',     sub: 'unique addresses' },
      ]}/>

      <div className="card" style={{marginTop: 18}}>
        <div className="swaps-filter-bar">
          <div className="asset-chips">
            <button className={'asset-chip' + (!assetFilter ? ' active' : '')} onClick={() => setAssetFilter(null)}>{t('chip.all')}</button>
            {Object.keys(TOKENS).map(s => (
              <button key={s} className={'asset-chip' + (assetFilter === s ? ' active' : '')}
                onClick={() => setAssetFilter(s)}>
                <TokenBadge sym={s} size={16}/> {s}
              </button>
            ))}
          </div>
          <div className="swaps-filter-spacer"/>
          <span className="tag">{filtered.length} transfers</span>
        </div>

        <div className="swaps-table-wrap">
          <table className="swaps-table">
            <thead>
              <tr>
                <th style={{paddingLeft: 20}}>Time</th>
                <th>Block</th>
                <th>Asset</th>
                <th>From</th>
                <th>To</th>
                <th style={{textAlign:'right'}}>Amount</th>
                <th style={{textAlign:'right'}}>Fee</th>
                <th style={{paddingRight: 20}}>Memo</th>
              </tr>
            </thead>
            <tbody>
              {visible.map(r => (
                <tr key={r.id} className="swap-row clickable" onClick={() => open({...r, type:'transfer', title:`${r.sym} transfer · ${fmt.num(r.amt,3)} ${r.sym}`, hash:'0x' + Math.random().toString(16).slice(2,18)})}>
                  <td style={{paddingLeft: 20}}>
                    <div style={{fontSize:12, fontWeight:700}}>{fmt.ago(r.ts)}</div>
                    <div className="muted tiny">{new Date(r.ts).toLocaleTimeString('en-US', {hour:'2-digit', minute:'2-digit'})}</div>
                  </td>
                  <td><a className="block-link num" href="#" onClick={(e) => e.stopPropagation()}>#{r.block.toLocaleString()}</a></td>
                  <td><div style={{display:'flex', alignItems:'center', gap:8}}><TokenBadge sym={r.sym}/><span style={{fontWeight:700}}>{r.sym}</span></div></td>
                  <td>
                    {IDENTITIES[r.from] && <div style={{fontSize:11, fontWeight:700}}>{IDENTITIES[r.from]}</div>}
                    <div className="muted tiny num">{fmt.addr(r.from, 5, 4)}</div>
                  </td>
                  <td>
                    {IDENTITIES[r.to] && <div style={{fontSize:11, fontWeight:700}}>{IDENTITIES[r.to]}</div>}
                    <div className="muted tiny num">{fmt.addr(r.to, 5, 4)}</div>
                  </td>
                  <td style={{textAlign:'right'}}>
                    <div className="num" style={{fontWeight:700}}>{fmt.num(r.amt, 3)} {r.sym}</div>
                    <div className="muted tiny num">${fmt.num(r.usd, 2)}</div>
                  </td>
                  <td style={{textAlign:'right'}}>
                    <div className="num tiny">{r.fee.toFixed(4)} XOR</div>
                  </td>
                  <td style={{paddingRight: 20}}>
                    <span className="memo-chip">{r.memo}</span>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>

        <Pagination page={page} setPage={setPage} total={filtered.length} pageSize={pageSize}/>
      </div>
    </div>
  );
}

/* =========================================================================
   BRIDGES
   ========================================================================= */

function BridgesSection({ tweaks }) {
  const t = useT();
  const { open } = useDrill();
  const [page, setPage] = useState(1);
  const [statusF, setStatusF] = useState('all');

  // Real bridges from prod /history/global/bridges. Shape:
  //   { timestamp, block, network, direction ("Incoming"/"Outgoing"), sender,
  //     recipient, asset_id, symbol, amount, usd_value, hash, time, logo }
  const { items: rawBridges } = useHistory('/history/global/bridges', { pageSize: 50, page: 1, pollMs: 30_000 });
  const rows = useMemo(() => {
    if (!rawBridges || rawBridges.length === 0) return [];
    return rawBridges.map((r, i) => {
      const isIn = (r.direction || '').toLowerCase().startsWith('in');
      return {
        id: 'b-' + (r.hash || (r.block + ':' + i)),
        ts: r.time ? parseHistTime(r.time) : Number(r.timestamp) || Date.now(),
        sym: r.symbol,
        from: isIn ? (r.network || 'Ethereum') : 'SORA',
        to:   isIn ? 'SORA' : (r.network || 'Ethereum'),
        dir: isIn ? 'in' : 'out',
        amt: Number(r.amount) || 0,
        usd: Number(r.usd_value) || 0,
        status: r.status || 'done',
        hash: r.hash,
        settle: 0, // prod doesn't expose settlement time; deferred
        sender: r.sender,
        recipient: r.recipient,
      };
    });
  }, [rawBridges]);

  const filtered = statusF === 'all' ? rows : rows.filter(r => r.status === statusF);
  const pageSize = tweaks.density === 'compact' ? 12 : tweaks.density === 'spacious' ? 6 : 10;
  const visible = filtered.slice((page-1) * pageSize, page * pageSize);

  const pending = rows.filter(r => r.status === 'pending').length;

  return (
    <div>
      <PageHeader title={t('bridges.title')} sub={t('bridges.sub')}>
        <span className="tag ok"><span className="live-dot" style={{width:5,height:5}}/> {t('btn.streaming')}</span>
        <ExportCsvButton section="bridges"
          headers={['Time','From','To','Asset','Amount','Status','Settlement','Hash']}
          rows={filtered.map(r => ({
            Time: new Date(r.ts).toISOString(),
            From: r.from, To: r.to,
            Asset: r.sym, Amount: r.amt,
            Status: r.status, Settlement: r.settle + ' min', Hash: r.hash,
          }))}/>
      </PageHeader>

      <KpiGrid items={[
        { label:'Bridge Vol · 24h', value:'$6.42', unit:'M', delta:'▲ 11%', deltaDir:'up' },
        { label:'Assets Bridged',   value:'14',    sub:'unique assets' },
        { label:'Pending Now',      value: String(pending*8), valStyle:{color:'#F5B041'}, sub:'awaiting confirmations' },
        { label:'Avg Settlement',   value:'7.4', unit:'min', sub:'finality median' },
      ]}/>

      <div className="card" style={{marginTop: 18}}>
        <div className="swaps-filter-bar">
          <div className="status-toggle">
            {[
              {id:'all', label:'All'},
              {id:'done', label:'✓ Done'},
              {id:'pending', label:'⏳ Pending'},
              {id:'failed', label:'✗ Failed'},
            ].map(o => (
              <button key={o.id} className={'status-opt' + (statusF === o.id ? ' active' : '') + ' ' + o.id}
                onClick={() => { setStatusF(o.id); setPage(1); }}>{o.label}</button>
            ))}
          </div>
          <div className="swaps-filter-spacer"/>
          <span className="tag">{filtered.length} bridges</span>
        </div>

        <div className="swaps-table-wrap">
          <table className="swaps-table">
            <thead>
              <tr>
                <th style={{paddingLeft: 20}}>Time</th>
                <th>Dir</th>
                <th>Asset</th>
                <th>Route</th>
                <th style={{textAlign:'right'}}>Amount</th>
                <th style={{textAlign:'center'}}>Status</th>
                <th style={{paddingRight:20}}>Tx</th>
              </tr>
            </thead>
            <tbody>
              {visible.map(r => (
                <tr key={r.id} className="swap-row clickable" onClick={() => open({...r, type:'bridge', title:`${r.from} → ${r.to}`})}>
                  <td style={{paddingLeft: 20}}>
                    <div style={{fontSize:12, fontWeight:700}}>{fmt.ago(r.ts)}</div>
                    <div className="muted tiny">{new Date(r.ts).toLocaleTimeString()}</div>
                  </td>
                  <td>
                    <span className={'bridge-dir ' + r.dir}>
                      {r.dir === 'in' ? '↓ IN' : '↑ OUT'}
                    </span>
                  </td>
                  <td><div style={{display:'flex', alignItems:'center', gap:8}}><TokenBadge sym={r.sym}/><span style={{fontWeight:700}}>{r.sym}</span></div></td>
                  <td>
                    <div className="chain-route">
                      <span className={'chain-tag c-' + r.from.toLowerCase()}>{r.from}</span>
                      <span className="route-arr">→</span>
                      <span className={'chain-tag c-' + r.to.toLowerCase()}>{r.to}</span>
                    </div>
                  </td>
                  <td style={{textAlign:'right'}}>
                    <div className="num" style={{fontWeight:700}}>{fmt.num(r.amt, 2)} {r.sym}</div>
                  </td>
                  <td style={{textAlign:'center'}}>
                    <span className={'br-status ' + r.status}>
                      {r.status === 'done' ? '✓ Done' : r.status === 'pending' ? '⏳ Pending' : '✗ Failed'}
                    </span>
                  </td>
                  <td style={{paddingRight:20}}>
                    <code className="num tiny" style={{color:'var(--fg-2)'}}>{r.hash.slice(0,10)}…</code>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>

        <Pagination page={page} setPage={setPage} total={filtered.length} pageSize={pageSize}/>
      </div>
    </div>
  );
}

/* =========================================================================
   ORDER BOOK
   ========================================================================= */

function OrderBookSection({ tweaks }) {
  const t = useT();
  const { open } = useDrill();
  const PAIRS = ['KUSD/XOR', 'XOR/DAI', 'VAL/XOR', 'ETH/KUSD', 'TBCD/XOR'];
  const [pair, setPair] = useState('KUSD/XOR');
  const [pairOpen, setPairOpen] = useState(false);

  const [base, quote] = pair.split('/');

  // Prod /history/global/orderbook returns order events (Place, Fill, Cancel).
  // We surface recent Fill events as the "fills" table. Bids/asks/mid/spread
  // would need a live book snapshot (prod doesn't expose it yet — deferred).
  const { items: rawOrders } = useHistory('/history/global/orderbook', { pageSize: 40, page: 1, pollMs: 20_000 });
  const { bids, asks, mid, spread, fills } = useMemo(() => {
    const rnd = seededRand(73 + pair.charCodeAt(0));
    const basePrice = 0.42 + rnd() * 0.3;
    // Bids/Asks stay mocked — no public snapshot endpoint available.
    const bids = Array.from({length: 12}, (_, i) => {
      const px = basePrice * (1 - (i+1) * 0.002 - rnd()*0.001);
      return { price: px, amount: +(rnd() * 400 + 40).toFixed(2), depth: +(rnd() * 12000 + 400).toFixed(0) };
    });
    const asks = Array.from({length: 12}, (_, i) => {
      const px = basePrice * (1 + (i+1) * 0.002 + rnd()*0.001);
      return { price: px, amount: +(rnd() * 400 + 40).toFixed(2), depth: +(rnd() * 12000 + 400).toFixed(0) };
    });
    const mid = basePrice;
    const spread = ((asks[0].price - bids[0].price) / mid) * 10000;

    // Real fills (Place/Fill events) from prod. Only keep pairs that match selected.
    const filtered = (rawOrders || []).filter(o => {
      if (!o.base_asset || !o.quote_asset) return false;
      const p = o.base_asset + '/' + o.quote_asset;
      return p === pair || pair === 'KUSD/XOR'; // default shows everything if KUSD/XOR (common pair)
    });
    const fills = filtered.slice(0, 10).map(o => ({
      ts: parseHistTime(o.time),
      side: (o.side || '').toLowerCase(),
      price: Number(o.price) || 0,
      amount: Number(o.amount) || 0,
      wallet: o.wallet,
      hash: o.hash,
      eventType: o.event_type,
    }));
    // Fallback to mocked fills if prod returned nothing for this pair.
    const finalFills = fills.length > 0 ? fills : Array.from({length: 10}, (_, i) => ({
      ts: Date.now() - i * 5000,
      side: rnd() > 0.5 ? 'buy' : 'sell',
      price: basePrice * (1 + (rnd() - 0.5) * 0.003),
      amount: +(rnd() * 180 + 10).toFixed(2),
    }));
    return { bids, asks, mid, spread, fills: finalFills };
  }, [pair, rawOrders]);

  const maxBidAmt = Math.max(...bids.map(b => b.amount));
  const maxAskAmt = Math.max(...asks.map(a => a.amount));

  return (
    <div>
      <PageHeader title={t('orderbook.title')} sub={t('orderbook.sub')}>
        <div className="swap-dropdown-wrap">
          <button className="swap-dropdown-btn has-filter" onClick={() => setPairOpen(o => !o)}>
            <TokenBadge sym={base} size={18}/>
            <TokenBadge sym={quote} size={18}/>
            <span>{pair}</span>
            <svg width="10" height="10" viewBox="0 0 10 10" fill="none" stroke="currentColor" strokeWidth="1.5"><path d="m2 4 3 3 3-3"/></svg>
          </button>
          {pairOpen && (
            <div className="swap-dropdown-content" style={{right: 0, left: 'auto'}}>
              {PAIRS.map(p => (
                <div key={p} className={'swap-dd-item' + (p === pair ? ' active' : '')}
                  onClick={() => { setPair(p); setPairOpen(false); }}>
                  <TokenBadge sym={p.split('/')[0]} size={16}/>
                  <TokenBadge sym={p.split('/')[1]} size={16}/>
                  <span>{p}</span>
                </div>
              ))}
            </div>
          )}
        </div>
        <ExportCsvButton section={'orderbook_' + pair.replace('/','_')}
          headers={['Time','Side','Price','Amount','Pair']}
          rows={fills.map(f => ({
            Time: new Date(f.ts).toISOString(),
            Side: f.side, Price: f.price.toFixed(6), Amount: f.amount, Pair: pair,
          }))}/>
      </PageHeader>

      <KpiGrid items={[
        { label:'Bid Depth',  value: fmt.usd(bids.reduce((s,b) => s + b.depth, 0)), sub: 'cumulative bids' },
        { label:'Ask Depth',  value: fmt.usd(asks.reduce((s,a) => s + a.depth, 0)), sub: 'cumulative asks' },
        { label:'Spread',     value: spread.toFixed(1), unit: 'bps', valStyle:{color:'#F5B041'} },
        { label:'Last Trade', value: mid.toFixed(4), unit: quote, sub: fills[0].side === 'buy' ? '↑ buy' : '↓ sell' },
      ]}/>

      <div className="ob-grid">
        <div className="card ob-side">
          <div className="card-header"><div className="card-title" style={{color:'#6EE7B7'}}><span className="dot" style={{background:'#10B981'}}/> BIDS</div>
            <span className="tag">{bids.length} levels</span>
          </div>
          <div className="ob-rows bids">
            <div className="ob-head"><span>Price ({quote})</span><span style={{textAlign:'right'}}>Amount ({base})</span><span style={{textAlign:'right'}}>Total</span></div>
            {bids.map((b, i) => (
              <div key={i} className="ob-row" style={{['--w']: (b.amount / maxBidAmt * 100) + '%'}}>
                <span className="num bid-px">{b.price.toFixed(4)}</span>
                <span className="num" style={{textAlign:'right'}}>{b.amount.toFixed(2)}</span>
                <span className="num tiny muted" style={{textAlign:'right'}}>{fmt.usd(b.depth)}</span>
              </div>
            ))}
          </div>
        </div>

        <div className="ob-mid">
          <div className="ob-mid-label">SPREAD</div>
          <div className="ob-mid-val num">{mid.toFixed(4)}</div>
          <div className="ob-mid-sub">{spread.toFixed(1)} bps · {(((asks[0].price - bids[0].price) * 10000)).toFixed(1)} ticks</div>
        </div>

        <div className="card ob-side">
          <div className="card-header"><div className="card-title" style={{color:'#FCA5A5'}}><span className="dot" style={{background:'#EF4444'}}/> ASKS</div>
            <span className="tag">{asks.length} levels</span>
          </div>
          <div className="ob-rows asks">
            <div className="ob-head"><span>Price ({quote})</span><span style={{textAlign:'right'}}>Amount ({base})</span><span style={{textAlign:'right'}}>Total</span></div>
            {asks.map((a, i) => (
              <div key={i} className="ob-row ask" style={{['--w']: (a.amount / maxAskAmt * 100) + '%'}}>
                <span className="num ask-px">{a.price.toFixed(4)}</span>
                <span className="num" style={{textAlign:'right'}}>{a.amount.toFixed(2)}</span>
                <span className="num tiny muted" style={{textAlign:'right'}}>{fmt.usd(a.depth)}</span>
              </div>
            ))}
          </div>
        </div>
      </div>

      <div className="card" style={{marginTop: 18}}>
        <div className="card-header">
          <div className="card-title"><span className="dot"/> Recent Fills · {pair}</div>
          <span className="tag ok"><span className="live-dot" style={{width:5,height:5}}/> live</span>
        </div>
        <div className="swaps-table-wrap">
          <table className="swaps-table">
            <thead>
              <tr>
                <th style={{paddingLeft:20}}>Time</th>
                <th>Side</th>
                <th style={{textAlign:'right'}}>Price</th>
                <th style={{textAlign:'right'}}>Amount</th>
                <th style={{textAlign:'right', paddingRight:20}}>Total</th>
              </tr>
            </thead>
            <tbody>
              {fills.map((f, i) => (
                <tr key={i} className="clickable" onClick={() => open({type:'order', title:`${f.side.toUpperCase()} · ${pair}`, side:f.side, pair, size:f.amount, price:f.price, filled:100, ts:f.ts, caller: FAKE_ADDRS[i % FAKE_ADDRS.length]})}>
                  <td style={{paddingLeft:20}}><span className="muted tiny">{fmt.ago(f.ts)}</span></td>
                  <td><span className={'fill-side ' + f.side}>{f.side === 'buy' ? '▲ BUY' : '▼ SELL'}</span></td>
                  <td style={{textAlign:'right'}} className="num">{f.price.toFixed(4)}</td>
                  <td style={{textAlign:'right'}} className="num">{f.amount.toFixed(2)}</td>
                  <td style={{textAlign:'right', paddingRight:20}} className="num">{(f.price * f.amount).toFixed(2)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  );
}

/* =========================================================================
   POOLS / LIQUIDITY
   ========================================================================= */

// Matches prod's 4 DEX bases on SORA.
const DEX_BASES = [
  { base: 'XOR',  dex: 0, color: '#E5243B' },
  { base: 'XST',  dex: 1, color: '#F5B041' },
  { base: 'KUSD', dex: 2, color: '#60A5FA' },
  { base: 'VXOR', dex: 3, color: '#7B5B90' },
];

function PoolsSection({ tweaks }) {
  const t = useT();
  const { open: openDrill } = useDrill();
  const [page, setPage] = useState(1);
  const [baseFilter, setBaseFilter] = useState('all');     // 'all' | 'XOR' | 'XST' | 'KUSD' | 'VXOR'
  const [pools, setPools] = useState([]);
  const [totalPages, setTotalPages] = useState(1);
  const [total, setTotal] = useState(0);
  const [providersModal, setProvidersModal] = useState(null);  // { base, target }
  const [activityModal, setActivityModal] = useState(null);    // { base, target }

  // Fetch server-paginated pools. Prod paginates at 10 per page across ~22 pages.
  useEffect(() => {
    let cancelled = false;
    const params = new URLSearchParams({ page: String(page), limit: '10' });
    if (baseFilter !== 'all') params.set('base', baseFilter);
    const pull = async () => {
      try {
        const r = await fetch('/pools?' + params.toString());
        if (!r.ok) return;
        const j = await r.json();
        if (cancelled) return;
        const parseBig = (s, dec) => {
          const raw = Number(String(s || '0').replace(/,/g, ''));
          return raw / Math.pow(10, dec || 18);
        };
        const rows = (j.data || []).map((p, i) => {
          const baseReserve = parseBig(p.reserves?.base, p.base?.decimals);
          const targetReserve = parseBig(p.reserves?.target, p.target?.decimals);
          const bp = Number(p.basePrice) || 0;
          const tp = Number(p.targetPrice) || 0;
          const totalUsd = (baseReserve * bp) + (targetReserve * tp);
          return {
            id: (p.base?.symbol || '?') + '-' + (p.target?.symbol || '?') + '-' + i,
            base: p.base,
            target: p.target,
            baseReserve, targetReserve,
            basePrice: bp, targetPrice: tp,
            totalUsd,
          };
        });
        setPools(rows);
        setTotalPages(Number(j.totalPages) || 1);
        setTotal(Number(j.total) || rows.length);
      } catch {}
    };
    pull();
    const id = setInterval(pull, 60_000);
    return () => { cancelled = true; clearInterval(id); };
  }, [page, baseFilter]);

  // Reset to page 1 whenever the DEX filter changes.
  useEffect(() => { setPage(1); }, [baseFilter]);

  const totalTvl = pools.reduce((s, p) => s + p.totalUsd, 0);

  return (
    <div>
      <PageHeader title={t('pools.title')} sub={t('pools.sub')}>
        <ExportCsvButton section="pools"
          headers={['Pair','BaseReserve','TargetReserve','TotalUsd']}
          rows={pools.map(p => ({
            Pair: (p.base?.symbol || '') + '/' + (p.target?.symbol || ''),
            BaseReserve: p.baseReserve.toFixed(2),
            TargetReserve: p.targetReserve.toFixed(2),
            TotalUsd: p.totalUsd.toFixed(2),
          }))}/>
        <button className="btn primary">{t('btn.provideLiquidity')}</button>
      </PageHeader>

      <KpiGrid items={[
        { label:'Total TVL (page)', value: fmt.usd(totalTvl), sub:'sum of visible pools' },
        { label:'Total Pools',      value: String(total), sub:'across all 4 DEX' },
        { label:'Top Pool',         value: pools[0] ? (pools[0].base.symbol + '/' + pools[0].target.symbol) : '—', valStyle:{fontSize: 20}, sub: pools[0] ? fmt.usd(pools[0].totalUsd) : '' },
        { label:'DEX Filter',       value: baseFilter === 'all' ? 'Todo' : baseFilter, sub: baseFilter === 'all' ? 'all 4 DEX' : 'DEX ' + (DEX_BASES.find(d => d.base === baseFilter)?.dex ?? '?') },
      ]}/>

      {/* DEX filter pills — Todo + 4 base-asset pills (XOR/XST/KUSD/VXOR). */}
      <div className="filter-row" style={{marginTop: 18, marginBottom: 12}}>
        <div
          className={'filter-chip' + (baseFilter === 'all' ? ' active' : '')}
          onClick={() => setBaseFilter('all')}
          title="Todos los DEX"
          style={{cursor:'pointer'}}>
          Todo
        </div>
        {DEX_BASES.map(d => (
          <div
            key={d.base}
            className={'filter-chip' + (baseFilter === d.base ? ' active' : '')}
            onClick={() => setBaseFilter(d.base)}
            title={d.base + ' (DEX ' + d.dex + ')'}
            style={{cursor:'pointer', display:'flex', alignItems:'center', gap: 6}}>
            <span style={{display:'inline-block', width:14, height:14, borderRadius:'50%', background: d.color}}/>
            <span style={{fontWeight: 600}}>{d.base}</span>
            <span className="muted tiny">DEX {d.dex}</span>
          </div>
        ))}
      </div>

      <div className="card">
        <div className="card-header">
          <div className="card-title"><span className="dot"/> {baseFilter === 'all' ? 'Todos los pools' : baseFilter + ' / … (DEX ' + (DEX_BASES.find(d => d.base === baseFilter)?.dex ?? '?') + ')'}</div>
          <span className="tag">{total} pools · página {page} de {totalPages}</span>
        </div>
        <div className="swaps-table-wrap">
          <table className="swaps-table">
            <thead>
              <tr>
                <th style={{paddingLeft: 20}}>Par</th>
                <th style={{textAlign:'right'}}>Reservas</th>
                <th style={{textAlign:'right'}}>Total</th>
                <th style={{textAlign:'center'}}>Providers</th>
                <th style={{textAlign:'center', paddingRight: 20}}>Activity</th>
              </tr>
            </thead>
            <tbody>
              {pools.map(p => (
                <tr key={p.id} className="ext-row">
                  <td style={{paddingLeft: 20}}>
                    <TokenPair a={p.base?.symbol} b={p.target?.symbol}/>
                  </td>
                  <td style={{textAlign:'right'}} className="num">
                    <div style={{lineHeight:1.3}}>
                      <div>{fmt.num(p.baseReserve, 2)} <b>{p.base?.symbol}</b></div>
                      <div>{fmt.num(p.targetReserve, 2)} <b>{p.target?.symbol}</b></div>
                    </div>
                  </td>
                  <td style={{textAlign:'right', fontWeight: 700, color: '#6EE7B7'}} className="num">
                    {fmt.usd(p.totalUsd)}
                  </td>
                  <td style={{textAlign:'center'}}>
                    <button className="btn" onClick={() => setProvidersModal({ base: p.base, target: p.target })}>Providers</button>
                  </td>
                  <td style={{textAlign:'center', paddingRight: 20}}>
                    <button className="btn" onClick={() => setActivityModal({ base: p.base, target: p.target })}>Activity</button>
                  </td>
                </tr>
              ))}
              {pools.length === 0 && (
                <tr><td colSpan="5" style={{padding:32, textAlign:'center', color:'var(--fg-2)'}}>
                  Cargando pools…
                </td></tr>
              )}
            </tbody>
          </table>
        </div>
        <Pagination page={page} setPage={setPage} total={total} pageSize={10}/>
      </div>

      {/* Pool Providers modal — GET /pool/providers?base=&target= */}
      {providersModal && (
        <PoolProvidersModal
          base={providersModal.base}
          target={providersModal.target}
          onClose={() => setProvidersModal(null)}
        />
      )}
      {/* Pool Activity modal — GET /pool/activity?base=&target= */}
      {activityModal && (
        <PoolActivityModal
          base={activityModal.base}
          target={activityModal.target}
          onClose={() => setActivityModal(null)}
        />
      )}
    </div>
  );
}

// --- Pool Providers modal ---
// Prod endpoint: /pool/providers?base=<id>&target=<id>
// Response shape: { providers: [{ address, balance, share }], totalProviders, ... }
function PoolProvidersModal({ base, target, onClose }) {
  const [data, setData] = useState(null);
  useEffect(() => {
    let cancelled = false;
    fetch('/pool/providers?base=' + encodeURIComponent(base.assetId) + '&target=' + encodeURIComponent(target.assetId))
      .then(r => r.json()).then(j => { if (!cancelled) setData(j); }).catch(() => {});
    return () => { cancelled = true; };
  }, [base.assetId, target.assetId]);
  // /pool/providers returns a bare array [{ address, balance }].
  const providers = Array.isArray(data) ? data : ((data && (data.providers || data.data)) || []);
  const totalBalance = providers.reduce((s, p) => s + (Number(p.balance) || 0), 0) || 1;
  return (
    <div className="sm-modal-backdrop" onClick={onClose}>
      <div className="sm-modal" style={{width: 620}} onClick={e => e.stopPropagation()}>
        <div className="sm-modal-head">
          <h3 style={{margin:0}}>Providers · {base.symbol}/{target.symbol}</h3>
          <button className="sm-modal-x" onClick={onClose}>×</button>
        </div>
        <div className="sm-modal-body">
          {!data ? <div className="muted">Cargando…</div> :
            providers.length === 0 ? <div className="muted">Sin proveedores retornados por prod.</div> :
            <table className="lp-table">
              <thead><tr><th>#</th><th>Proveedor</th><th style={{textAlign:'right'}}>Balance</th><th style={{textAlign:'right'}}>Share</th></tr></thead>
              <tbody>
                {providers.slice(0, 50).map((p, i) => {
                  const bal = Number(p.balance) || 0;
                  const share = bal / totalBalance * 100;
                  return (
                  <tr key={(p.address || '') + i}>
                    <td className="num">{i + 1}</td>
                    <td><span className="num tiny">{fmt.addr(p.address || p.wallet, 8, 6)}</span></td>
                    <td style={{textAlign:'right'}} className="num">{fmt.num(bal, 4)}</td>
                    <td style={{textAlign:'right', color:'#FBB040', fontWeight: 700}} className="num">{share.toFixed(2)}%</td>
                  </tr>
                  );
                })}
              </tbody>
            </table>
          }
        </div>
      </div>
    </div>
  );
}

// --- Pool Activity modal ---
// Prod endpoint: /pool/activity?base=<id>&target=<id>
// Response shape: { activities: [{ time, type, amount, wallet, hash }] }
function PoolActivityModal({ base, target, onClose }) {
  const [data, setData] = useState(null);
  useEffect(() => {
    let cancelled = false;
    fetch('/pool/activity?base=' + encodeURIComponent(base.assetId) + '&target=' + encodeURIComponent(target.assetId))
      .then(r => r.json()).then(j => { if (!cancelled) setData(j); }).catch(() => {});
    return () => { cancelled = true; };
  }, [base.assetId, target.assetId]);
  const acts = (data && (data.activities || data.data || data)) || [];
  const list = Array.isArray(acts) ? acts : [];
  return (
    <div className="sm-modal-backdrop" onClick={onClose}>
      <div className="sm-modal" style={{width: 720}} onClick={e => e.stopPropagation()}>
        <div className="sm-modal-head">
          <h3 style={{margin:0}}>Activity · {base.symbol}/{target.symbol}</h3>
          <button className="sm-modal-x" onClick={onClose}>×</button>
        </div>
        <div className="sm-modal-body" style={{maxHeight: 520, overflow:'auto'}}>
          {!data ? <div className="muted">Cargando…</div> :
            list.length === 0 ? <div className="muted">Sin actividad reciente en este pool.</div> :
            <table className="lp-table">
              <thead><tr><th>Tiempo</th><th>Tipo</th><th style={{textAlign:'right'}}>Monto</th><th>Wallet</th></tr></thead>
              <tbody>
                {list.slice(0, 80).map((a, i) => (
                  <tr key={(a.hash || '') + i}>
                    <td className="tiny">{a.time || fmt.ago(Number(a.timestamp) || Date.now())}</td>
                    <td><span className="tag">{a.type || a.event_type || 'swap'}</span></td>
                    <td style={{textAlign:'right'}} className="num">{fmt.num(Number(a.amount || 0), 2)}</td>
                    <td><span className="num tiny">{fmt.addr(a.wallet || a.account, 6, 4)}</span></td>
                  </tr>
                ))}
              </tbody>
            </table>
          }
        </div>
      </div>
    </div>
  );
}

/* =========================================================================
   TOKENS
   ========================================================================= */

function TokensSection({ tweaks }) {
  const t = useT();
  const [fav, setFav] = useState(() => new Set(['XOR','VAL']));
  const [filter, setFilter] = useState('all');

  // Real token registry from prod /tokens. Shape: { data: [{ symbol, name,
  //   decimals, assetId, logo, price, totalSupply, marketCap, change24h, ... }] }
  // Prod doesn't expose per-token 24h history at this endpoint — sparklines
  // stay seeded until Phase 7 wires /chart/:symbol?res=.
  const { items: rawTokens } = useHistory('/tokens', { pageSize: 0, page: 1, pollMs: 60_000 });
  const tokens = useMemo(() => {
    if (!rawTokens || rawTokens.length === 0) return [];
    const rnd = seededRand(75);
    return rawTokens.slice(0, 20).map((rt, i) => {
      const price = Number(rt.price) || 0;
      const supply = Number(rt.totalSupply) || 0;
      const mcap = Number(rt.marketCap) || (price * supply);
      const change = Number(rt.change24h) || (rnd() - 0.4) * 20;
      const spark = Array.from({length: 30}, (_, j) =>
        50 + Math.sin(j/3 + i*1.3)*14 + (change > 0 ? j*0.3 : -j*0.3) + rnd()*3
      );
      return {
        sym: rt.symbol,
        name: rt.name || (rt.symbol + ' Token'),
        price, supply, mcap, change, spark,
        logo: rt.logo,
      };
    });
  }, [rawTokens]);

  const visible = filter === 'fav' ? tokens.filter(t => fav.has(t.sym)) : tokens;
  const gainer = [...tokens].sort((a,b) => b.change - a.change)[0] || { sym: '—', change: 0 };
  const loser  = [...tokens].sort((a,b) => a.change - b.change)[0] || { sym: '—', change: 0 };
  const totalMcap = tokens.reduce((s,t) => s + t.mcap, 0);

  const toggleFav = (sym) => {
    setFav(prev => {
      const n = new Set(prev);
      if (n.has(sym)) n.delete(sym); else n.add(sym);
      return n;
    });
  };

  return (
    <div>
      <PageHeader title={t('tokens.title')} sub={t('tokens.sub')}>
        <ExportCsvButton section="tokens"
          headers={['Symbol','Price','Change24h','MarketCap','Supply']}
          rows={visible.map(r => ({
            Symbol: r.sym, Price: r.price, Change24h: r.change.toFixed(2)+'%',
            MarketCap: r.mcap.toFixed(0), Supply: r.supply,
          }))}/>
        <div className="status-toggle">
          <button className={'status-opt' + (filter==='all' ? ' active' : '')} onClick={() => setFilter('all')}>{t('chip.all')}</button>
          <button className={'status-opt' + (filter==='fav' ? ' active' : '')} onClick={() => setFilter('fav')}>★ {t('chip.favorites')}</button>
        </div>
      </PageHeader>

      <KpiGrid items={[
        { label:'Total Tokens',   value: String(tokens.length), sub:'registered' },
        { label:'Total Mcap',     value: fmt.usd(totalMcap), delta:'▲ 2.1%', deltaDir:'up' },
        { label:'Top Gainer',     value: gainer.sym, valStyle:{fontSize: 22, color: '#6EE7B7'}, sub: '+' + gainer.change.toFixed(1) + '% · 24h' },
        { label:'Top Loser',      value: loser.sym, valStyle:{fontSize: 22, color: '#FCA5A5'}, sub: loser.change.toFixed(1) + '% · 24h' },
      ]}/>

      <div className="token-grid">
        {visible.map(t => {
          const tk = TOKENS[t.sym] || { grad: 'linear-gradient(135deg, #7B5B90, #4A3566)' };
          return (
            <div key={t.sym} className="token-card">
              <div className="token-card-head">
                <div className="token-logo big" style={{ background: tk.grad }}>{t.sym[0]}</div>
                <div style={{flex:1, minWidth:0}}>
                  <div className="token-card-sym">{t.sym}</div>
                  <div className="muted tiny">{t.name}</div>
                </div>
                <button className={'fav-btn' + (fav.has(t.sym) ? ' on' : '')} onClick={() => toggleFav(t.sym)}>★</button>
              </div>
              <div className="token-card-price num">
                ${t.price < 1 ? t.price.toFixed(4) : t.price.toFixed(2)}
              </div>
              <div className={'token-card-delta ' + (t.change >= 0 ? 'up' : 'down')}>
                {t.change >= 0 ? '▲' : '▼'} {Math.abs(t.change).toFixed(2)}% · 24h
              </div>
              <div style={{margin: '10px 0'}}>
                <svg viewBox="0 0 120 36" width="100%" height="36">
                  <path d={sparkPath(t.spark, 120, 36, 2)} stroke={t.change >= 0 ? '#10B981' : '#EF4444'} strokeWidth="1.6" fill="none"/>
                </svg>
              </div>
              <div className="token-card-foot">
                <div><span className="muted tiny">Mcap</span><div className="num small">{fmt.usd(t.mcap, 1)}</div></div>
                <div><span className="muted tiny">Supply</span><div className="num small">{fmt.num(t.supply, 1)}</div></div>
              </div>
            </div>
          );
        })}
      </div>
    </div>
  );
}

/* =========================================================================
   HOLDERS
   ========================================================================= */

function HoldersSection({ tweaks }) {
  const t = useT();
  const { open } = useDrill();
  const [page, setPage] = useState(1);

  const holders = useMemo(() => {
    const rnd = seededRand(76);
    const top5 = ['Polkaswap', 'XOR Treasury', 'Bridge Reserve', 'DAO Multisig', 'Cerberus'];
    return Array.from({length: 20}, (_, i) => {
      const val = (22e6 * Math.pow(0.72, i)) + rnd() * 50000;
      const label = i < 5 ? top5[i] : (i === 5 ? 'Kusari' : null);
      return {
        rank: i + 1,
        name: label,
        addr: FAKE_ADDRS[i % FAKE_ADDRS.length],
        value: val,
        tokens: Math.floor(rnd() * 10 + 2),
        lastActivity: fmt.ago(Date.now() - (rnd() * 86400000 * 3)),
      };
    });
  }, []);

  const pageSize = tweaks.density === 'compact' ? 15 : tweaks.density === 'spacious' ? 8 : 12;
  const visible = holders.slice((page-1) * pageSize, page * pageSize);

  const top10Share = holders.slice(0,10).reduce((s,h) => s + h.value, 0) /
                     holders.reduce((s,h) => s + h.value, 0) * 100;

  return (
    <div>
      <PageHeader title={t('holders.title')} sub={t('holders.sub')}>
        <ExportCsvButton section="holders"
          headers={['Rank','Name','Address','Value','Tokens']}
          rows={holders.map(r => ({
            Rank: r.rank, Name: r.name || '', Address: r.addr,
            Value: r.value.toFixed(2), Tokens: r.tokens,
          }))}/>
        <span className="tag ok"><span className="live-dot" style={{width:5,height:5}}/> snapshot · now</span>
      </PageHeader>

      <KpiGrid items={[
        { label:'Unique Holders',  value:'18,420', delta:'▲ 142 · 24h', deltaDir:'up' },
        { label:'Top 10 Share',    value: top10Share.toFixed(1), unit:'%', sub: 'of chain value' },
        { label:'Whales (>$1M)',   value: '42', sub:'addresses' },
        { label:'Median Balance',  value:'$142', sub:'across all holders' },
      ]}/>

      <div className="card" style={{marginTop: 18}}>
        <div className="card-header">
          <div className="card-title"><span className="dot"/> {t('burn.topHolders')}</div>
          <span className="tag">{holders.length} ranked</span>
        </div>
        <div className="swaps-table-wrap">
          <table className="swaps-table">
            <thead>
              <tr>
                <th style={{paddingLeft: 20, width: 56}}>#</th>
                <th>{t('col.account')}</th>
                <th style={{textAlign:'right'}}>{t('col.value')}</th>
                <th style={{textAlign:'right'}}>{t('col.tokens')}</th>
                <th style={{paddingRight: 20}}>{t('col.lastActivity')}</th>
              </tr>
            </thead>
            <tbody>
              {visible.map(h => (
                <tr key={h.rank} className="swap-row clickable" onClick={() => open({type:'holder', title:`#${h.rank} · ${h.name || fmt.addr(h.addr,6,4)}`, ...h})}>
                  <td style={{paddingLeft: 20}}>
                    <span className={'rank-chip ' + (h.rank <= 3 ? 'top3' : '')}>{h.rank}</span>
                  </td>
                  <td>
                    <div style={{display:'flex', alignItems:'center', gap: 10}}>
                      <div style={{width:28, height:28, borderRadius:'50%', background: h.rank <= 5 ? 'linear-gradient(135deg,#9B1B30,#4A3566)' : 'linear-gradient(135deg,#7B5B90,#4A3566)', flexShrink: 0}}/>
                      <div>
                        {h.name && <div style={{fontSize:13, fontWeight:700, color:'var(--fg-0)'}}>{h.name}</div>}
                        <div className="muted tiny num">{fmt.addr(h.addr, 6, 4)}</div>
                      </div>
                    </div>
                  </td>
                  <td style={{textAlign:'right'}} className="num">
                    <span style={{fontWeight:700, color:'var(--fg-0)'}}>{fmt.usd(h.value)}</span>
                  </td>
                  <td style={{textAlign:'right'}} className="num">{h.tokens}</td>
                  <td style={{paddingRight: 20}} className="muted tiny">{h.lastActivity}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
        <Pagination page={page} setPage={setPage} total={holders.length} pageSize={pageSize}/>
      </div>
    </div>
  );
}

/* =========================================================================
   STAKING (tabs: Validators / Network Info)
   ========================================================================= */

function StakingSection({ tweaks }) {
  const t = useT();
  const { open } = useDrill();
  const [tab, setTab] = useState('validators');
  const [page, setPage] = useState(1);

  // Real validators from prod /staking/validators. Endpoint returns an OBJECT
  // (not array-of-rows) so we fetch directly instead of via useHistory.
  // Shape: { era, validatorCount, validators: [{ address, identity, commission,
  //   totalStake, ownStake, otherStake, nominatorsCount, isBlocked, erasSincePayout }] }
  const [rawValidators, setRawValidators] = useState([]);
  useEffect(() => {
    let cancelled = false;
    const pull = async () => {
      try {
        const r = await fetch('/staking/validators');
        if (!r.ok) return;
        const j = await r.json();
        if (!cancelled) setRawValidators(j.validators || []);
      } catch {}
    };
    pull();
    const id = setInterval(pull, 60_000);
    return () => { cancelled = true; clearInterval(id); };
  }, []);
  const validators = useMemo(() => {
    return rawValidators.map((v, i) => ({
      rank: i + 1,
      name: v.identity || (v.address ? v.address.slice(0, 8) + '…' + v.address.slice(-6) : 'Unknown'),
      address: v.address,
      total: Number(v.totalStake) || 0,
      own: Number(v.ownStake) || 0,
      nominators: Number(v.nominatorsCount) || 0,
      commission: Number(v.commission) || 0,
      points: Math.round(Number(v.erasSincePayout) * 1000) || 0,
      status: v.isBlocked ? 'blocked' : 'active',
    }));
  }, [rawValidators]);

  const pageSize = tweaks.density === 'compact' ? 15 : 12;
  const visible = validators.slice((page-1) * pageSize, page * pageSize);

  return (
    <div>
      <PageHeader title={t('staking.title')} sub={t('staking.sub')}>
        <ExportCsvButton section="staking"
          headers={['Rank','Name','Stake','Commission','Nominators','Points']}
          rows={validators.map((v, i) => ({
            Rank: i+1, Name: v.name, Stake: v.total.toFixed(0),
            Commission: v.commission+'%', Nominators: v.noms, Points: v.points,
          }))}/>
      </PageHeader>

      <Tabs tabs={[
        { id: 'validators', label: t('staking.tab.validators'), count: validators.length },
        { id: 'network', label: t('staking.tab.network') },
      ]} current={tab} onChange={setTab}/>

      {tab === 'validators' && (
        <>
          <KpiGrid items={[
            { label: t('staking.kpi.activeValidators'), value:'14 / 20', sub: t('staking.kpi.targetWaiting') },
            { label: t('staking.kpi.totalStaked'),      value:'$3.42', unit:'M', delta:'▲ 0.8%', deltaDir:'up' },
            { label: t('staking.kpi.avgCommission'),    value:'4.2', unit:'%', sub: t('staking.kpi.activeSet') },
            { label: t('staking.kpi.nextEraIn'),        value:'2h 14m', sub: t('staking.kpi.era') + ' 2408' },
          ]}/>

          <div className="card" style={{marginTop: 18}}>
            <div className="card-header">
              <div className="card-title"><span className="dot"/> {t('staking.tab.validators')}</div>
            </div>
            <div className="swaps-table-wrap">
              <table className="swaps-table">
                <thead>
                  <tr>
                    <th style={{paddingLeft:20, width:48}}>#</th>
                    <th>{t('staking.col.validator')}</th>
                    <th style={{textAlign:'right'}}>{t('staking.col.totalStake')}</th>
                    <th style={{textAlign:'right'}}>{t('staking.col.own')}</th>
                    <th style={{textAlign:'right'}}>{t('staking.col.noms')}</th>
                    <th style={{textAlign:'right'}}>{t('staking.col.commission')}</th>
                    <th style={{textAlign:'right'}}>{t('staking.col.eraPts')}</th>
                    <th style={{paddingRight:20}}>{t('staking.col.status')}</th>
                  </tr>
                </thead>
                <tbody>
                  {visible.map(v => (
                    <tr key={v.rank} className="swap-row clickable" onClick={() => open({type:'validator', title:v.name, ...v})}>
                      <td style={{paddingLeft: 20}}><span className={'rank-chip ' + (v.rank <= 3 ? 'top3' : '')}>{v.rank}</span></td>
                      <td>
                        <div style={{display:'flex', alignItems:'center', gap: 10}}>
                          <div style={{width:26, height:26, borderRadius:6, background:'linear-gradient(135deg,#9B1B30,#4A3566)'}}/>
                          <span style={{fontWeight:700, color:'var(--fg-0)'}}>{v.name}</span>
                        </div>
                      </td>
                      <td style={{textAlign:'right'}} className="num"><strong>{fmt.num(v.total, 0)} XOR</strong></td>
                      <td style={{textAlign:'right'}} className="num">{fmt.num(v.own, 0)}</td>
                      <td style={{textAlign:'right'}} className="num">{v.nominators}</td>
                      <td style={{textAlign:'right'}} className="num"><span style={{color: v.commission > 5 ? '#F5B041' : '#6EE7B7', fontWeight:700}}>{v.commission}%</span></td>
                      <td style={{textAlign:'right'}} className="num">{v.points.toLocaleString()}</td>
                      <td style={{paddingRight:20}}>
                        <span className={'val-status ' + v.status}>
                          {v.status === 'active' ? '● ' + t('status.active') : v.status === 'waiting' ? '◌ ' + t('status.waiting') : '⚠ ' + t('status.oversub')}
                        </span>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
            <Pagination page={page} setPage={setPage} total={validators.length} pageSize={pageSize}/>
          </div>
        </>
      )}

      {tab === 'network' && (
        <div className="stake-network-grid">
          {[
            ['Total Stake', '612,480 XOR', '$44,098'],
            ['Active Era', '2,408', 'era started 3h 46m ago'],
            ['Epoch Progress', '14 / 6', 'epochs this era · ~4h each'],
            ['Validators Target', '20 active', '6 waiting in queue'],
            ['Min Nominator Bond', '1.00 XOR', 'hard floor'],
            ['Last Reward Era', '2,407', '4,284 XOR distributed'],
            ['Ideal Stake Rate', '50%', 'annual inflation model'],
            ['Current Inflation', '3.84%', 'this era annualised'],
            ['Unbonding Period', '7 days', 'withdrawal lock'],
          ].map((row, i) => (
            <div key={i} className="stake-stat-card">
              <div className="stat-label">{row[0]}</div>
              <div className="stat-value num" style={{fontSize: 26}}>{row[1]}</div>
              <div className="stat-sub">{row[2]}</div>
            </div>
          ))}
        </div>
      )}
    </div>
  );
}

/* =========================================================================
   GOVERNANCE (tabs in Spanish)
   ========================================================================= */

function GovSection({ tweaks }) {
  const t = useT();
  const [tab, setTab] = useState('consejo');

  // Real council + motions + elections + democracy + tech committee from prod.
  // All five endpoints return OBJECTS (not array-of-rows), so direct fetch.
  const [rawCouncil, setRawCouncil] = useState([]);
  const [rawMotions, setRawMotions] = useState([]);
  const [rawElections, setRawElections] = useState(null);    // { elected: [], candidates: [], runnersUp: [] }
  const [rawDemocracy, setRawDemocracy] = useState(null);    // { referendums: [], proposals: [], currentBlock, ... }
  const [rawTech, setRawTech] = useState(null);              // { members: [], prime, identities }
  useEffect(() => {
    let cancelled = false;
    const pullAll = async () => {
      try {
        const [cRes, mRes, eRes, dRes, tRes] = await Promise.all([
          fetch('/governance/council').then(r => r.ok ? r.json() : null).catch(() => null),
          fetch('/governance/motions').then(r => r.ok ? r.json() : null).catch(() => null),
          fetch('/governance/elections').then(r => r.ok ? r.json() : null).catch(() => null),
          fetch('/governance/democracy').then(r => r.ok ? r.json() : null).catch(() => null),
          fetch('/governance/technical-committee').then(r => r.ok ? r.json() : null).catch(() => null),
        ]);
        if (cancelled) return;
        setRawCouncil(cRes?.members || []);
        setRawMotions(mRes?.council || []);
        setRawElections(eRes);
        setRawDemocracy(dRes);
        setRawTech(tRes);
      } catch {}
    };
    pullAll();
    const id = setInterval(pullAll, 120_000);
    return () => { cancelled = true; clearInterval(id); };
  }, []);

  const council = useMemo(() => {
    if (rawCouncil.length === 0) return []; // hide mocks until real loads
    return rawCouncil.map((m, i) => ({
      name: m.identity || ('Seat ' + (i + 1)),
      addr: m.address,
      joined: 0, // prod doesn't expose join era
      votes: 0,  // prod /council doesn't expose vote counts — deferred
      stake: Number(m.stake) || 0,
      isPrime: !!m.isPrime,
    }));
  }, [rawCouncil]);

  // Real elections from /governance/elections.
  // Shape: { elected:[{address,stake}], candidates?, runnersUp? }
  const elections = useMemo(() => {
    const src = rawElections || {};
    const elected = Array.isArray(src.elected) ? src.elected : [];
    return {
      seats: elected.length || 13,
      filled: elected.length,
      candidates: elected.map(e => ({
        name: e.identity || (e.address ? e.address.slice(0, 8) + '…' + e.address.slice(-6) : 'Unknown'),
        addr: e.address,
        votes: Math.round(Number(e.stake) || 0),
        bond: 0,
      })),
      runnersUp: (Array.isArray(src.runnersUp) ? src.runnersUp : []).map(e => ({
        name: e.identity || (e.address ? e.address.slice(0, 8) + '…' + e.address.slice(-6) : 'Unknown'),
        votes: Math.round(Number(e.stake || e.votes) || 0),
      })),
    };
  }, [rawElections]);

  // Real motions from prod /governance/motions → council[].
  // Shape: { hash, index, decoded:{section,method,args,description}, remark, ... }
  const motions = useMemo(() => {
    if (rawMotions.length === 0) {
      // Show empty-state instead of mocks — prod has no active motions sometimes.
      return [];
    }
    return rawMotions.slice(0, 8).map((m, i) => ({
      id: m.index != null ? m.index : i,
      title: (m.decoded && m.decoded.description) || (m.decoded && (m.decoded.section + '::' + m.decoded.method)) || 'Unknown motion',
      proposer: '—', // not exposed by /motions endpoint
      threshold: '—',
      votes: { aye: 0, nay: 0 },
      deadline: 'pending',
      status: 'open',
      hash: m.hash,
    }));
  }, [rawMotions]);

  // Real democracy from /governance/democracy.
  // Shape: { referendums:[{id,status,detail:{end,tally:{ayes,nays,turnout}},timeRemaining,...}],
  //          proposals:[], currentBlock, totalReferendums, ... }
  const democracy = useMemo(() => {
    const src = rawDemocracy || {};
    const refs = (src.referendums || []).map(rf => {
      const tally = rf.detail?.tally || {};
      const ayes = Number(tally.ayes) || 0;
      const nays = Number(tally.nays) || 0;
      const total = ayes + nays || 1;
      return {
        id: rf.id,
        title: rf.decoded?.description || ('Referendum #' + rf.id),
        aye: Math.round((ayes / total) * 100),
        nay: Math.round((nays / total) * 100),
        ends: rf.timeRemaining || '—',
        turnout: Number(rf.detail?.tally?.turnout || 0) / 1e18,
        status: rf.status,
      };
    });
    const props = (src.proposals || []).map(p => ({
      id: 'P-' + (p.index != null ? p.index : p.id || '?'),
      title: p.decoded?.description || 'Public proposal',
      seconds: p.seconds?.length || 0,
      deposit: Number(p.deposit) || 0,
    }));
    return { referendums: refs, proposals: props };
  }, [rawDemocracy]);

  // Real tech committee from /governance/technical-committee.
  // Shape: { members:[{address,identity,isPrime}], prime, identities }
  const tech = useMemo(() => ({
    members: ((rawTech && rawTech.members) || []).map(m => ({
      name: m.identity || (m.address ? m.address.slice(0, 8) + '…' + m.address.slice(-6) : 'Unknown'),
      addr: m.address,
      isPrime: !!m.isPrime,
    })),
    // Tech-committee motions not exposed by /governance/technical-committee.
    // When /governance/motions returns tech-specific entries we could split
    // them from council motions; for now this stays empty + empty-state UI.
    motions: [],
  }), [rawTech]);

  return (
    <div>
      <PageHeader title={t('gov.title')} sub={t('gov.sub')}>
        <ExportCsvButton section="governance"
          headers={['Kind','ID','Title','Aye','Nay','Ends','Turnout']}
          rows={[
            ...((democracy && democracy.referendums) || []).map(r => ({
              Kind:'referendum', ID:r.id, Title:r.title,
              Aye:r.aye+'%', Nay:r.nay+'%', Ends:r.ends, Turnout:r.turnout+'%',
            })),
            ...((democracy && democracy.proposals) || []).map(p => ({
              Kind:'proposal', ID:p.id || '', Title:p.title,
              Aye:'', Nay:'', Ends:p.ends || '', Turnout:(p.endorsements || 0)+' endorsements',
            })),
            ...((tech && tech.motions) || []).map(m => ({
              Kind:'motion', ID:m.id || '', Title:m.title,
              Aye:m.aye ? m.aye+'' : '', Nay:m.nay ? m.nay+'' : '',
              Ends:m.status || '', Turnout:'',
            })),
          ]}/>
      </PageHeader>

      <Tabs tabs={[
        { id:'consejo', label:'Consejo' },
        { id:'elecciones', label:'Elecciones' },
        { id:'mociones', label:'Mociones' },
        { id:'democracia', label:'Democracia' },
        { id:'tecnico', label:'Comité Técnico' },
      ]} current={tab} onChange={setTab}/>

      {tab === 'consejo' && (
        <div className="card" style={{marginTop: 18}}>
          <div className="card-header">
            <div className="card-title"><span className="dot"/> Miembros del Consejo · {council.length}</div>
            <span className="tag">7 asientos ocupados</span>
          </div>
          <table className="swaps-table">
            <thead><tr>
              <th style={{paddingLeft:20}}>Miembro</th>
              <th style={{textAlign:'right'}}>Se unió (era)</th>
              <th style={{textAlign:'right', paddingRight:20}}>Votos emitidos</th>
            </tr></thead>
            <tbody>
              {council.map((c, i) => (
                <tr key={i}>
                  <td style={{paddingLeft:20}}>
                    <div style={{display:'flex', alignItems:'center', gap:10}}>
                      <div style={{width:28, height:28, borderRadius:'50%', background:'linear-gradient(135deg,#9B1B30,#4A3566)', flexShrink:0}}/>
                      <div style={{minWidth:0}}>
                        <div style={{fontWeight:700, whiteSpace:'nowrap'}}>{c.name}</div>
                        <div className="muted tiny num" style={{whiteSpace:'nowrap'}}>{fmt.addr(c.addr, 6, 4)}</div>
                      </div>
                    </div>
                  </td>
                  <td style={{textAlign:'right'}} className="num">{c.joined}</td>
                  <td style={{textAlign:'right', paddingRight:20}} className="num">{c.votes}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}

      {tab === 'elecciones' && (
        <div className="gov-elections-grid">
          <div className="card">
            <div className="card-header"><div className="card-title"><span className="dot"/> Candidatos actuales</div><span className="tag">{elections.candidates.length}</span></div>
            <div style={{padding: 12}}>
              {elections.candidates.map((c, i) => (
                <div key={i} className="elec-row">
                  <div style={{width:24, height:24, borderRadius:6, background:'linear-gradient(135deg,#9B1B30,#4A3566)'}}/>
                  <div style={{flex:1, minWidth:0}}>
                    <div style={{fontWeight:700, fontSize:13}}>{c.name}</div>
                    <div className="muted tiny">Fianza · {c.bond} XOR</div>
                  </div>
                  <div className="elec-bar"><div className="elec-bar-fill" style={{width: (c.votes / 48200 * 100) + '%'}}/></div>
                  <div className="num" style={{fontWeight:700, minWidth: 70, textAlign:'right'}}>{fmt.num(c.votes, 1)}</div>
                </div>
              ))}
            </div>
          </div>
          <div className="card">
            <div className="card-header"><div className="card-title"><span className="dot"/> Suplentes</div></div>
            <div style={{padding: 12}}>
              {elections.runnersUp.map((r, i) => (
                <div key={i} className="elec-row">
                  <div style={{width: 24, height: 24, borderRadius: 6, background:'linear-gradient(135deg,#7B5B90,#4A3566)'}}/>
                  <div style={{flex:1, fontWeight:700, fontSize: 13}}>{r.name}</div>
                  <div className="num muted">{fmt.num(r.votes, 1)}</div>
                </div>
              ))}
              <div style={{marginTop: 16, padding: 12, background:'rgba(255,255,255,0.02)', borderRadius:8, fontSize: 12, color:'var(--fg-2)'}}>
                <strong>{elections.filled} / {elections.seats}</strong> asientos cubiertos · siguiente votación en <strong>3 eras</strong>.
              </div>
            </div>
          </div>
        </div>
      )}

      {tab === 'mociones' && (
        <div className="card" style={{marginTop: 18}}>
          <div className="card-header"><div className="card-title"><span className="dot"/> Mociones activas y recientes</div></div>
          <div className="motions-list">
            {motions.map(m => (
              <div key={m.id} className="motion-card">
                <div style={{display:'flex', alignItems:'center', gap:12, marginBottom: 8}}>
                  <span className="motion-id">#{m.id}</span>
                  <span className={'br-status ' + (m.status === 'open' ? 'pending' : m.status === 'passed' ? 'done' : 'failed')}>
                    {m.status === 'open' ? '⏳ Abierta' : m.status === 'passed' ? '✓ Aprobada' : '✗ Rechazada'}
                  </span>
                  <span className="muted tiny" style={{marginLeft:'auto'}}>Umbral · {m.threshold} · {m.deadline}</span>
                </div>
                <div style={{fontSize:15, fontWeight:700, color:'var(--fg-0)', marginBottom: 10}}>{m.title}</div>
                <div className="muted tiny">Propuesta por <strong>{m.proposer}</strong></div>
                <div className="vote-bar" style={{marginTop: 12}}>
                  <div className="vote-aye" style={{flex: m.votes.aye}}>✓ {m.votes.aye} AYE</div>
                  <div className="vote-nay" style={{flex: m.votes.nay || 0.3}}>✗ {m.votes.nay} NAY</div>
                </div>
              </div>
            ))}
          </div>
        </div>
      )}

      {tab === 'democracia' && (
        <div style={{marginTop: 18}}>
          <div className="card">
            <div className="card-header"><div className="card-title"><span className="dot"/> Referéndums en curso</div><span className="tag">{democracy.referendums.length}</span></div>
            <div className="motions-list">
              {democracy.referendums.map(r => (
                <div key={r.id} className="motion-card">
                  <div style={{display:'flex', alignItems:'center', gap:12, marginBottom: 8}}>
                    <span className="motion-id">#{r.id}</span>
                    <span className="tag">Termina en {r.ends}</span>
                    <span className="muted tiny" style={{marginLeft:'auto'}}>Participación · {r.turnout}%</span>
                  </div>
                  <div style={{fontSize:15, fontWeight:700, marginBottom: 10}}>{r.title}</div>
                  <div className="vote-bar">
                    <div className="vote-aye" style={{flex: r.aye}}>✓ {r.aye}% AYE</div>
                    <div className="vote-nay" style={{flex: r.nay}}>✗ {r.nay}% NAY</div>
                  </div>
                </div>
              ))}
            </div>
          </div>

          <div className="card" style={{marginTop: 18}}>
            <div className="card-header"><div className="card-title"><span className="dot"/> Propuestas públicas</div></div>
            <table className="swaps-table">
              <thead><tr>
                <th style={{paddingLeft:20}}>ID</th>
                <th>Título</th>
                <th style={{textAlign:'right'}}>Respaldos</th>
                <th style={{textAlign:'right', paddingRight:20}}>Depósito</th>
              </tr></thead>
              <tbody>
                {democracy.proposals.map(p => (
                  <tr key={p.id}>
                    <td style={{paddingLeft:20}} className="num" style={{fontWeight:700, color:'var(--accent)'}}>{p.id}</td>
                    <td>{p.title}</td>
                    <td style={{textAlign:'right'}} className="num">{p.seconds}</td>
                    <td style={{textAlign:'right', paddingRight:20}} className="num">{p.deposit} XOR</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      )}

      {tab === 'tecnico' && (
        <div className="gov-elections-grid" style={{marginTop: 18}}>
          <div className="card">
            <div className="card-header"><div className="card-title"><span className="dot"/> Miembros del Comité Técnico</div></div>
            <table className="swaps-table">
              <tbody>
                {tech.members.map((m, i) => (
                  <tr key={i}>
                    <td style={{paddingLeft:20}}>
                      <div style={{display:'flex', alignItems:'center', gap:10, padding:'4px 0'}}>
                        <div style={{width:28, height:28, borderRadius:'50%', background:'linear-gradient(135deg,#7B5B90,#4A3566)'}}/>
                        <div>
                          <div style={{fontWeight:700}}>{m.name}</div>
                          <div className="muted tiny num">{fmt.addr(m.addr, 6, 4)}</div>
                        </div>
                      </div>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
          <div className="card">
            <div className="card-header"><div className="card-title"><span className="dot"/> Mociones técnicas</div></div>
            <div className="motions-list">
              {tech.motions.map(m => (
                <div key={m.id} className="motion-card">
                  <div style={{display:'flex', alignItems:'center', gap:12, marginBottom: 8}}>
                    <span className="motion-id">#{m.id}</span>
                    <span className={'br-status ' + (m.status === 'open' ? 'pending' : 'done')}>
                      {m.status === 'open' ? '⏳ Abierta' : '✓ Aprobada'}
                    </span>
                    <span className="muted tiny" style={{marginLeft:'auto'}}>Umbral · {m.threshold}</span>
                  </div>
                  <div style={{fontWeight: 700}}>{m.title}</div>
                  <div className="vote-bar" style={{marginTop: 10}}>
                    <div className="vote-aye" style={{flex: m.aye}}>✓ {m.aye}</div>
                    <div className="vote-nay" style={{flex: m.nay || 0.3}}>✗ {m.nay}</div>
                  </div>
                </div>
              ))}
            </div>
          </div>
        </div>
      )}
    </div>
  );
}

/* =========================================================================
   BALANCE (Overview / Mis Wallets / Vigiladas — Spanish)
   ========================================================================= */

function BalanceSection({ tweaks }) {
  const t = useT();
  const [tab, setTab] = useState('overview');
  const [addOpen, setAddOpen] = useState(false);
  const [detailWallet, setDetailWallet] = useState(null);
  const store = useWallets();
  const wallets = store.wallets;
  const watched = store.watched;

  const net = wallets.reduce((s,w) => s + w.value, 0);

  return (
    <div>
      <PageHeader title={t('balance.title')} sub={t('balance.sub')}>
        <button className="btn primary" onClick={() => setAddOpen(true)}>+ Añadir Wallet</button>
      </PageHeader>

      <Tabs tabs={[
        { id:'overview', label:'Overview' },
        { id:'mis', label:'Mis Wallets', count: wallets.length },
        { id:'vig', label:'Vigiladas', count: watched.length },
      ]} current={tab} onChange={setTab}/>

      {tab === 'overview' && (
        <div>
          <div className="card" style={{padding: 32, marginTop: 18, textAlign:'center'}}>
            <div className="stat-label">Patrimonio Neto Total</div>
            <div className="num" style={{fontSize: 56, fontWeight: 800, margin:'14px 0', color:'var(--fg-0)'}}>${net.toLocaleString()}</div>
            <div className="stat-delta up" style={{fontSize: 14}}>▲ $1,240 · 2.4% · 24h</div>
          </div>
          <div className="balance-alloc-grid">
            {['XOR 42%', 'VAL 18%', 'PSWAP 12%', 'ETH 10%', 'Stables 12%', 'Otros 6%'].map((s, i) => (
              <div key={i} className="alloc-card">
                <div className="alloc-bar" style={{background: ['#E5243B','#F5B041','#EC4899','#8B7FD9','#60A5FA','#94A3B8'][i]}}/>
                <div style={{fontWeight: 700}}>{s.split(' ')[0]}</div>
                <div className="num" style={{color:'var(--accent)', fontWeight: 700}}>{s.split(' ')[1]}</div>
              </div>
            ))}
          </div>
        </div>
      )}

      {tab === 'mis' && (
        <div className="card" style={{marginTop: 18}}>
          <div className="card-header"><div className="card-title"><span className="dot"/> Mis Wallets</div></div>
          <div className="wallet-list">
            {wallets.map((w, i) => {
              const toks = (w.tokens || []).filter(t => Number(t.amount) > 0);
              const totalUsd = toks.reduce((s, t) => s + (Number(t.usdValue) || 0), 0);
              return (
                <div key={w.id || i} className="wallet-list-card clickable" onClick={() => setDetailWallet(w)}>
                  <div style={{width: 36, height: 36, borderRadius: 8, background:'linear-gradient(135deg,#9B1B30,#4A3566)', display:'grid', placeItems:'center', fontWeight:800}}>{w.alias[0]}</div>
                  <div style={{flex:1, minWidth: 0}}>
                    <div style={{fontWeight: 700}}>{w.alias}</div>
                    <div className="muted tiny num">{fmt.addr(w.addr, 8, 6)}</div>
                  </div>
                  <div style={{textAlign:'right'}}>
                    <div className="num" style={{fontWeight:700, fontSize:15}}>{totalUsd > 0 ? '$' + totalUsd.toLocaleString(undefined,{maximumFractionDigits:2}) : '—'}</div>
                    <span className={'tag ' + (toks.length > 0 ? 'ok' : '')} style={{fontSize:10}}>
                      {toks.length > 0 ? <><span className="live-dot" style={{width:5,height:5}}/> {toks.length} tokens</> : 'sin saldo'}
                    </span>
                  </div>
                </div>
              );
            })}
          </div>
        </div>
      )}

      {tab === 'vig' && (
        <div className="card" style={{marginTop: 18}}>
          <div className="card-header">
            <div className="card-title"><span className="dot"/> Wallets Vigiladas</div>
            <span className="tag">solo lectura</span>
          </div>
          <div className="wallet-list">
            {watched.map((w, i) => (
              <div key={w.id || i} className="wallet-list-card">
                <div style={{width: 36, height: 36, borderRadius: 8, background:'linear-gradient(135deg,#7B5B90,#4A3566)', display:'grid', placeItems:'center', fontWeight:800}}>👁</div>
                <div style={{flex:1, minWidth: 0}}>
                  <div style={{fontWeight: 700}}>{w.alias}</div>
                  <div className="muted tiny num">{fmt.addr(w.addr, 8, 6)}</div>
                </div>
                <div style={{textAlign:'right'}}>
                  <div className="num" style={{fontWeight:700}}>${w.value.toLocaleString()}</div>
                </div>
              </div>
            ))}
          </div>
        </div>
      )}

      <AddWalletModal open={addOpen} onClose={() => setAddOpen(false)}/>
      <WalletDetailsModal wallet={detailWallet} open={!!detailWallet} onClose={() => setDetailWallet(null)}/>
    </div>
  );
}

/* =========================================================================
   INTELLIGENCE
   ========================================================================= */

// --- Peg history Chart.js line ---
// Prod exposes /stats/stablecoins only as a snapshot. To give users a rolling
// history without touching the backend, we persist up to the last 120 samples
// per symbol in localStorage ('sm.pegHistory') and plot them with Chart.js.
// Each sample = { t: ts, KUSD: px, XSTUSD: px, TBCD: px }.
function PegHistoryChart({ stables }) {
  const canvasRef = useRef(null);
  const chartRef = useRef(null);
  const [history, setHistory] = useState(() => {
    try { const raw = localStorage.getItem('sm.pegHistory'); if (raw) return JSON.parse(raw).slice(-120); } catch {}
    return [];
  });

  // Append each new stables snapshot to history + persist.
  useEffect(() => {
    if (!stables || !stables.length) return;
    const sample = { t: Date.now() };
    stables.forEach(sc => { sample[sc.symbol] = Number(sc.price) || 0; });
    setHistory(h => {
      const next = [...h, sample].slice(-120);
      try { localStorage.setItem('sm.pegHistory', JSON.stringify(next)); } catch {}
      return next;
    });
  }, [stables]);

  // Redraw Chart.js on every history update.
  useEffect(() => {
    if (!canvasRef.current || !window.Chart || history.length === 0) return;
    if (chartRef.current) { chartRef.current.destroy(); chartRef.current = null; }
    const labels = history.map(p => new Date(p.t).toLocaleTimeString('es-ES', {hour:'2-digit', minute:'2-digit'}));
    const syms = ['KUSD', 'XSTUSD', 'TBCD'];
    const colors = { KUSD: '#60A5FA', XSTUSD: '#F5B041', TBCD: '#10B981' };
    const datasets = syms.map(sym => ({
      label: sym,
      data: history.map(p => p[sym] || null),
      borderColor: colors[sym],
      backgroundColor: colors[sym] + '22',
      fill: false,
      tension: 0.25,
      pointRadius: 0,
      spanGaps: true,
    }));
    chartRef.current = new window.Chart(canvasRef.current.getContext('2d'), {
      type: 'line',
      data: { labels, datasets },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        plugins: {
          legend: { labels: { color: '#C8A0B8' } },
          tooltip: { mode: 'index', intersect: false },
        },
        scales: {
          x: { ticks: { color: '#94A3B8', maxTicksLimit: 6 }, grid: { color: 'rgba(255,255,255,0.04)' } },
          y: {
            ticks: { color: '#94A3B8', callback: v => '$' + Number(v).toFixed(2) },
            grid: { color: 'rgba(255,255,255,0.04)' },
          },
        },
      },
    });
    return () => { if (chartRef.current) { chartRef.current.destroy(); chartRef.current = null; } };
  }, [history]);

  return (
    <div className="card" style={{marginBottom: 18}}>
      <div className="card-header">
        <div className="card-title"><span className="dot"/> Peg history · KUSD / XSTUSD / TBCD</div>
        <span className="tag">{history.length} snapshots · ref $1.00</span>
      </div>
      <div className="card-body" style={{height: 220}}>
        {history.length < 2
          ? <div className="muted tiny" style={{padding: 20, textAlign:'center'}}>Recogiendo datos… el primer punto se guarda ahora mismo.</div>
          : <canvas ref={canvasRef}/>}
      </div>
    </div>
  );
}

function IntelligenceSection({ tweaks }) {
  const t = useT();
  // Pull real signals from prod /stats/* endpoints and turn them into
  // insight cards. This is a thin synthesis layer — prod doesn't publish
  // pre-baked insights, so we derive them here.
  const [accum, setAccum] = useState([]);
  const [trending, setTrending] = useState([]);
  const [stables, setStables] = useState([]);
  useEffect(() => {
    let cancelled = false;
    const pull = async () => {
      try {
        const [a, t, s] = await Promise.all([
          fetch('/stats/accumulation').then(r => r.json()).catch(() => null),
          fetch('/stats/trending-tokens').then(r => r.json()).catch(() => null),
          fetch('/stats/stablecoins').then(r => r.json()).catch(() => null),
        ]);
        if (cancelled) return;
        setAccum(a && Array.isArray(a.data) ? a.data : (Array.isArray(a) ? a : []));
        setTrending(Array.isArray(t) ? t : []);
        setStables(Array.isArray(s) ? s : []);
      } catch {}
    };
    pull();
    const id = setInterval(pull, 60_000);
    return () => { cancelled = true; clearInterval(id); };
  }, []);

  const insights = useMemo(() => {
    const out = [];
    // Top accumulator → whale signal
    if (accum[0]) {
      const a = accum[0];
      out.push({
        type: 'whale',
        tag: 'Whale accumulation',
        severity: 'high',
        title: 'Whale accumulation · ' + (a.wallet?.slice(0, 8) + '…' + a.wallet?.slice(-6)),
        body: 'Bought ' + Number(a.total_bought_amount || 0).toFixed(2) + ' ' + (a.symbol || 'XOR') + ' worth $' + Number(a.total_bought_usd || 0).toFixed(0) + ' · ' + a.swap_count + ' swaps 24h',
      });
    }
    // Top trending token
    if (trending[0]) {
      const tk = trending[0];
      out.push({
        type: 'volume',
        tag: 'Volume spike',
        severity: 'high',
        title: tk.symbol + ' leads 24h volume',
        body: '$' + Number(tk.volume || 0).toFixed(0) + ' traded · top of trending list',
      });
    }
    // Stablecoin peg health — flag depegs
    stables.forEach(sc => {
      const price = Number(sc.price) || 0;
      const dev = Math.abs(price - 1);
      if (dev > 0.01) {
        out.push({
          type: 'peg',
          tag: 'Peg alert',
          severity: dev > 0.03 ? 'high' : 'mid',
          title: sc.symbol + ' deviates ' + (dev * 100).toFixed(2) + '% from $1',
          body: 'Current price $' + price.toFixed(4) + ' · ' + (price > 1 ? 'premium' : 'discount'),
        });
      }
    });
    // Remaining trending → low-severity "watch" entries
    trending.slice(1, 6).forEach(tk => {
      out.push({
        type: 'pool',
        tag: 'Trending',
        severity: 'low',
        title: tk.symbol + ' on the rise',
        body: '$' + Number(tk.volume || 0).toFixed(0) + ' 24h volume',
      });
    });
    return out;
  }, [accum, trending, stables]);

  return (
    <div>
      <PageHeader title={t('intel.title')} sub={t('intel.sub')}>
        <span className="tag ok"><span className="live-dot" style={{width:5,height:5}}/> engine active</span>
      </PageHeader>

      <KpiGrid items={[
        { label:'Insights · 24h', value: '42', delta:'▲ 8 since last refresh', deltaDir:'up' },
        { label:'Active Alerts',  value: String(insights.filter(i => i.severity === 'high').length), valStyle:{color:'#F5B041'}, sub:'high-severity open' },
        { label:'Watchlist Hits', value: '7', sub:'tracked addresses' },
        { label:'Open Anomalies', value: String(insights.filter(i => i.type === 'peg').length), sub:'last 4h' },
      ]}/>

      {/* Stablecoin peg history line — Chart.js. Client-side rolling ring of
          /stats/stablecoins snapshots stored in localStorage so the curve
          survives reloads. Prod has no historical stablecoin price endpoint,
          so we build one here at 30s cadence. */}
      {stables.length > 0 && <PegHistoryChart stables={stables}/>}

      {/* Stablecoin Peg Monitor — ports prod's visual depeg badge.
          Each row shows price vs $1 reference; bar fills red when |dev| > 2%. */}
      {stables.length > 0 && (
        <div className="card" style={{marginBottom: 18}}>
          <div className="card-header">
            <div className="card-title"><span className="dot"/> Peg Monitor</div>
            <span className="tag">ref $1.00</span>
          </div>
          <div className="card-body" style={{display:'grid', gap: 12}}>
            {stables.map((sc) => {
              const price = Number(sc.price) || 0;
              const devPct = (price - 1) * 100;
              const absDev = Math.abs(devPct);
              const depegged = absDev > 2;
              return (
                <div key={sc.symbol} style={{display:'grid', gridTemplateColumns:'80px 1fr 90px 110px', alignItems:'center', gap: 12}}>
                  <div style={{fontWeight: 700}}>{sc.symbol}</div>
                  <div style={{position:'relative', height: 6, background:'rgba(255,255,255,0.06)', borderRadius: 3}}>
                    <div style={{position:'absolute', left:'50%', top: -3, width: 1, height: 12, background:'rgba(255,255,255,0.3)'}}/>
                    <div style={{
                      position:'absolute',
                      left: devPct >= 0 ? '50%' : (50 - Math.min(absDev, 10) * 5) + '%',
                      width: Math.min(absDev, 10) * 5 + '%',
                      top: 0, bottom: 0,
                      background: depegged ? '#EF4444' : '#10B981',
                      borderRadius: 3,
                    }}/>
                  </div>
                  <div className="num" style={{textAlign:'right', fontWeight: 600}}>${price.toFixed(4)}</div>
                  <div style={{textAlign:'right'}}>
                    {depegged ? (
                      <span className="tag err">DEPEG {devPct >= 0 ? '+' : ''}{devPct.toFixed(2)}%</span>
                    ) : (
                      <span className="tag ok">peg ±{absDev.toFixed(2)}%</span>
                    )}
                  </div>
                </div>
              );
            })}
          </div>
        </div>
      )}

      <div className="insights-grid">
        {insights.map((it, i) => (
          <div key={i} className={'insight-card sev-' + it.severity}>
            <div className="insight-head">
              <span className={'insight-sev sev-' + it.severity}/>
              <span className="insight-tag">{it.tag}</span>
              <span className="muted tiny" style={{marginLeft:'auto'}}>{i * 14 + 3}m ago</span>
            </div>
            <div className="insight-title">{it.title}</div>
            <div className="insight-body">{it.body}</div>
            <div className="insight-foot">
              <button className="btn" style={{fontSize: 11, padding:'5px 10px'}}>View detail</button>
              <button className="btn" style={{fontSize: 11, padding:'5px 10px'}}>Dismiss</button>
            </div>
          </div>
        ))}
      </div>
    </div>
  );
}

Object.assign(window, {
  TransfersSection, BridgesSection, OrderBookSection, PoolsSection,
  TokensSection, HoldersSection, StakingSection, GovSection,
  BalanceSection, IntelligenceSection,
});
