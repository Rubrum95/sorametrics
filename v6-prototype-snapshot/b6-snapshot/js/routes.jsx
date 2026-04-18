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

  const rows = useMemo(() => {
    const rnd = seededRand(71);
    const syms = Object.keys(TOKENS);
    const memos = ['—', 'payroll', 'LP reward', 'donation', 'gas refund', 'escrow', 'airdrop', 'bounty'];
    const now = Date.now();
    return Array.from({length: 15}, (_, i) => {
      const sym = syms[Math.floor(rnd() * syms.length)];
      const price = TOKENS[sym]?.sym === 'KUSD' ? 1 : (rnd() * 4 + 0.1);
      const amt = +(rnd() * 2000 + 2).toFixed(3);
      return {
        id: i,
        ts: now - i * 9000,
        block: 21_418_800 + Math.floor(rnd()*2000),
        sym,
        from: FAKE_ADDRS[Math.floor(rnd() * FAKE_ADDRS.length)],
        to:   FAKE_ADDRS[Math.floor(rnd() * FAKE_ADDRS.length)],
        amt,
        usd: amt * price,
        fee: +(rnd()*0.03 + 0.003).toFixed(4),
        memo: memos[Math.floor(rnd() * memos.length)],
      };
    });
  }, []);

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

  const rows = useMemo(() => {
    const rnd = seededRand(72);
    const syms = ['XOR','VAL','PSWAP','ETH','DAI','KUSD'];
    const chains = [
      {from:'Ethereum', to:'SORA', dir:'in'},
      {from:'SORA',     to:'Ethereum', dir:'out'},
      {from:'Kusama',   to:'SORA', dir:'in'},
      {from:'SORA',     to:'Polkadot', dir:'out'},
      {from:'Liberland',to:'SORA', dir:'in'},
    ];
    const now = Date.now();
    return Array.from({length: 15}, (_, i) => {
      const c = chains[Math.floor(rnd() * chains.length)];
      const sym = syms[Math.floor(rnd() * syms.length)];
      const amt = +(rnd() * 800 + 5).toFixed(2);
      const rStatus = rnd();
      const status = rStatus < 0.7 ? 'done' : rStatus < 0.9 ? 'pending' : 'failed';
      return {
        id: i, ts: now - i*12000, sym, ...c, amt, status,
        hash: '0x' + Math.random().toString(16).slice(2, 18),
        settle: (+(rnd()*18 + 1).toFixed(1)),
      };
    });
  }, []);

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

  const { bids, asks, mid, spread, fills } = useMemo(() => {
    const rnd = seededRand(73 + pair.charCodeAt(0));
    const basePrice = 0.42 + rnd() * 0.3;
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

    const fills = Array.from({length: 10}, (_, i) => ({
      ts: Date.now() - i * 5000,
      side: rnd() > 0.5 ? 'buy' : 'sell',
      price: basePrice * (1 + (rnd() - 0.5) * 0.003),
      amount: +(rnd() * 180 + 10).toFixed(2),
    }));
    return { bids, asks, mid, spread, fills };
  }, [pair]);

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

function PoolsSection({ tweaks }) {
  const t = useT();
  const { open: openDrill } = useDrill();
  const [page, setPage] = useState(1);
  const [expanded, setExpanded] = useState(null);

  const pools = useMemo(() => {
    const rnd = seededRand(74);
    const pairs = [
      ['XOR','VAL'], ['XOR','PSWAP'], ['KUSD','XOR'], ['XOR','ETH'],
      ['XOR','DAI'], ['PSWAP','KUSD'], ['VAL','KUSD'], ['TBCD','XOR'],
      ['ETH','DAI'], ['PSWAP','VAL'], ['ETH','KUSD'], ['VAL','TBCD'],
    ];
    return pairs.map((p, i) => {
      const tvl = (rnd() * 8e6 + 1.2e5) * (i === 0 ? 3 : 1);
      const vol = tvl * (0.05 + rnd() * 0.3);
      return {
        id: i, a: p[0], b: p[1],
        tvl, vol, apr: +(rnd() * 42 + 2).toFixed(2),
        providers: Math.floor(rnd() * 380 + 14),
      };
    }).sort((a, b) => b.tvl - a.tvl);
  }, []);

  const totalTvl = pools.reduce((s,p) => s + p.tvl, 0);
  const totalFees = pools.reduce((s,p) => s + p.vol * 0.003, 0);
  const pageSize = tweaks.density === 'compact' ? 12 : tweaks.density === 'spacious' ? 6 : 10;
  const visible = pools.slice((page-1) * pageSize, page * pageSize);

  return (
    <div>
      <PageHeader title={t('pools.title')} sub={t('pools.sub')}>
        <ExportCsvButton section="pools"
          headers={['Pair','TVL','Volume24h','APR','Providers']}
          rows={pools.map(p => ({
            Pair: p.a + '/' + p.b,
            TVL: p.tvl.toFixed(2), Volume24h: p.vol.toFixed(2),
            APR: p.apr + '%', Providers: p.providers,
          }))}/>
        <button className="btn primary">{t('btn.provideLiquidity')}</button>
      </PageHeader>

      <KpiGrid items={[
        { label:'Total TVL',       value: fmt.usd(totalTvl), delta:'▲ 3.2%', deltaDir:'up' },
        { label:'Total Pools',     value: String(pools.length), sub:'active AMM pools' },
        { label:'Top Pool',        value: pools[0].a + '/' + pools[0].b, valStyle:{fontSize: 20}, sub: fmt.usd(pools[0].tvl) + ' TVL' },
        { label:'24h Fees Earned', value: fmt.usd(totalFees), sub:'0.3% swap fee' },
      ]}/>

      <div className="card" style={{marginTop: 18}}>
        <div className="card-header">
          <div className="card-title"><span className="dot"/> All Pools</div>
          <span className="tag">{pools.length} pools</span>
        </div>
        <div className="swaps-table-wrap">
          <table className="swaps-table">
            <thead>
              <tr>
                <th style={{paddingLeft: 20}}>Pool</th>
                <th style={{textAlign:'right'}}>TVL</th>
                <th style={{textAlign:'right'}}>24h Volume</th>
                <th style={{textAlign:'right'}}>APR</th>
                <th style={{textAlign:'right'}}>Providers</th>
                <th style={{width:36, paddingRight:20}}></th>
              </tr>
            </thead>
            <tbody>
              {visible.map(p => (
                <React.Fragment key={p.id}>
                  <tr className={'ext-row' + (expanded === p.id ? ' open' : '')}
                      onClick={() => setExpanded(expanded === p.id ? null : p.id)}>
                    <td style={{paddingLeft: 20}}>
                      <TokenPair a={p.a} b={p.b}/>
                    </td>
                    <td style={{textAlign:'right'}} className="num" ><span style={{fontWeight:700}}>{fmt.usd(p.tvl)}</span></td>
                    <td style={{textAlign:'right'}} className="num">{fmt.usd(p.vol)}</td>
                    <td style={{textAlign:'right'}} className="num">
                      <span style={{color: p.apr > 20 ? '#FBB040' : '#6EE7B7', fontWeight:700}}>{p.apr.toFixed(2)}%</span>
                    </td>
                    <td style={{textAlign:'right'}} className="num">{p.providers}</td>
                    <td style={{paddingRight: 20, textAlign:'center'}}>
                      <span className={'ext-caret' + (expanded === p.id ? ' open' : '')}>▾</span>
                    </td>
                  </tr>
                  {expanded === p.id && (
                    <tr className="ext-detail-row">
                      <td colSpan="6" style={{padding: 0}}>
                        <div className="ext-detail">
                          <div className="ext-detail-label">Top 10 Liquidity Providers</div>
                          <table className="lp-table">
                            <thead>
                              <tr><th>#</th><th>Provider</th><th style={{textAlign:'right'}}>Stake</th><th style={{textAlign:'right'}}>Share</th></tr>
                            </thead>
                            <tbody>
                              {Array.from({length: 10}, (_, i) => {
                                const share = (24 - i*2.1) * (1 + Math.random()*0.2);
                                const addr = FAKE_ADDRS[i % FAKE_ADDRS.length];
                                return (
                                  <tr key={i} className="clickable"
                                      onClick={(e) => { e.stopPropagation(); openDrill({type:'lp', title:`LP · ${p.a}/${p.b}`, pool:`${p.a}/${p.b}`, stake: p.tvl * share / 100, share, addr, rewards: share * 12, since:'2024-08-12'}); }}>
                                    <td className="num">{i+1}</td>
                                    <td>
                                      <div style={{display:'flex', alignItems:'center', gap: 8}}>
                                        <div style={{width:20, height:20, borderRadius:'50%', background:'linear-gradient(135deg,#7B5B90,#4A3566)'}}/>
                                        {IDENTITIES[addr] && <span style={{fontSize:11, fontWeight:700}}>{IDENTITIES[addr]}</span>}
                                        <span className="muted tiny num">{fmt.addr(addr, 5, 4)}</span>
                                      </div>
                                    </td>
                                    <td style={{textAlign:'right'}} className="num">{fmt.usd(p.tvl * share / 100)}</td>
                                    <td style={{textAlign:'right', color:'#FBB040', fontWeight: 700}} className="num">{share.toFixed(2)}%</td>
                                  </tr>
                                );
                              })}
                            </tbody>
                          </table>
                        </div>
                      </td>
                    </tr>
                  )}
                </React.Fragment>
              ))}
            </tbody>
          </table>
        </div>
        <Pagination page={page} setPage={setPage} total={pools.length} pageSize={pageSize}/>
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

  const tokens = useMemo(() => {
    const rnd = seededRand(75);
    const base = [
      { sym:'XOR', price:0.072, supply: 38.2e6, mcap: 38.2e6*0.072 },
      { sym:'VAL', price:0.094, supply: 100e6, mcap: 100e6*0.094 },
      { sym:'PSWAP', price:0.0031, supply: 600e6, mcap: 600e6*0.0031 },
      { sym:'TBCD', price:1.002, supply: 2.4e6, mcap: 2.4e6*1.002 },
      { sym:'KUSD', price:0.998, supply: 14.2e6, mcap: 14.2e6*0.998 },
      { sym:'ETH',  price:3240, supply: 420, mcap: 420*3240 },
      { sym:'DAI',  price:1.0, supply: 1.4e6, mcap: 1.4e6 },
      { sym:'KXOR', price:0.072, supply: 8.1e5, mcap: 8.1e5*0.072 },
      { sym:'KEN',  price:0.21,  supply: 3.2e6, mcap: 3.2e6*0.21 },
      { sym:'KARMA',price:0.0048,supply: 280e6, mcap: 280e6*0.0048 },
      { sym:'HMX',  price:0.018, supply: 42e6, mcap: 42e6*0.018 },
      { sym:'AXOR', price:0.068, supply: 620000, mcap: 620000*0.068 },
      { sym:'XSTUSD', price:0.999, supply: 2.1e6, mcap: 2.1e6 },
      { sym:'XST',  price:0.24, supply: 1.8e6, mcap: 1.8e6*0.24 },
      { sym:'DEO',  price:0.016, supply: 16e6, mcap: 16e6*0.016 },
    ];
    return base.map((t, i) => {
      const change = (rnd() - 0.4) * 20;
      const spark = Array.from({length: 30}, (_, j) =>
        50 + Math.sin(j/3 + i*1.3)*14 + (change > 0 ? j*0.3 : -j*0.3) + rnd()*3
      );
      return { ...t, change, spark, name: t.sym + ' Token' };
    });
  }, []);

  const visible = filter === 'fav' ? tokens.filter(t => fav.has(t.sym)) : tokens;
  const gainer = [...tokens].sort((a,b) => b.change - a.change)[0];
  const loser  = [...tokens].sort((a,b) => a.change - b.change)[0];
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

  const validators = useMemo(() => {
    const rnd = seededRand(77);
    const names = ['Sakura Node','Kusari-01','Cerberus','Moonflower','Akira Validators','Sora.keepers',
      'PolkaLab','Nebula Stake','RedPetal','Yama','Hokkaido Node','Aurora','Kitsune','Ronin-Staking',
      'Sakurajima','Fujiwara','Shinobi','Kirin Validator','Midori','Hanabi'];
    return names.map((n, i) => {
      const total = (rnd() * 4e5 + 5e4);
      const own = total * (rnd()*0.12 + 0.01);
      const stat = rnd();
      const status = i < 14 ? 'active' : stat > 0.5 ? 'waiting' : 'oversubscribed';
      return {
        rank: i+1, name: n, total, own,
        nominators: Math.floor(rnd() * 220 + 14),
        commission: +(rnd() * 10 + 1).toFixed(2),
        points: Math.floor(rnd() * 9800 + 200),
        status,
      };
    });
  }, []);

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

  const council = useMemo(() => ([
    { name: 'Sakura Node', addr: FAKE_ADDRS[0], joined: 2401, votes: 142 },
    { name: 'Cerberus',     addr: FAKE_ADDRS[4], joined: 2388, votes: 201 },
    { name: 'Kusari',       addr: FAKE_ADDRS[5], joined: 2362, votes: 188 },
    { name: 'Moonflower',   addr: FAKE_ADDRS[6], joined: 2402, votes: 92 },
    { name: 'PolkaLab',     addr: FAKE_ADDRS[2], joined: 2375, votes: 156 },
    { name: 'Aurora',       addr: FAKE_ADDRS[7], joined: 2394, votes: 128 },
    { name: 'Fujiwara',     addr: FAKE_ADDRS[1], joined: 2410, votes: 62 },
  ]), []);

  const elections = useMemo(() => ({
    seats: 13, filled: 7,
    candidates: [
      { name:'Hokkaido Node', votes: 48200, bond: 120 },
      { name:'Yama',          votes: 41000, bond: 100 },
      { name:'RedPetal',      votes: 39500, bond: 80 },
      { name:'Sakurajima',    votes: 31200, bond: 80 },
      { name:'Akira',         votes: 24800, bond: 60 },
      { name:'Midori',        votes: 18400, bond: 60 },
    ],
    runnersUp: [
      { name:'Shinobi', votes: 14200 },
      { name:'Hanabi',  votes: 11900 },
      { name:'Ronin',    votes: 9820 },
    ],
  }), []);

  const motions = [
    { id: 42, title:'Elevar el umbral del consejo a 5/7', proposer:'Cerberus', threshold:'5/7', votes:{aye: 4, nay: 1}, deadline: '2d 8h', status: 'open' },
    { id: 41, title:'Financiar auditoría trimestral (12k XOR)', proposer:'Kusari', threshold:'4/7', votes:{aye: 3, nay: 0}, deadline: '5d 2h', status: 'open' },
    { id: 40, title:'Aprobar nueva passphrase del bridge', proposer:'Sakura Node', threshold:'5/7', votes:{aye: 5, nay: 2}, deadline: 'finalizado', status: 'passed' },
    { id: 39, title:'Rechazar puente con cadena X', proposer:'PolkaLab', threshold:'4/7', votes:{aye: 2, nay: 4}, deadline: 'finalizado', status: 'rejected' },
  ];

  const democracy = {
    referendums: [
      { id: 42, title: 'Aumentar límite de gas en 30%',       aye: 68, nay: 32, ends: '6h 12m', turnout: 18.4 },
      { id: 41, title: 'Añadir soporte a nuevo puente EVM',    aye: 74, nay: 26, ends: '1d 4h',  turnout: 14.8 },
      { id: 40, title: 'Reducir comisión por defecto a 0.25%', aye: 42, nay: 58, ends: '2d 18h', turnout: 22.1 },
    ],
    proposals: [
      { id: 'P-017', title: 'Sprint de marketing asiático',     seconds: 12, deposit: 2400 },
      { id: 'P-016', title: 'Programa de embajadores v2',       seconds: 8,  deposit: 1200 },
      { id: 'P-015', title: 'Tokenomics rework · Q3',           seconds: 22, deposit: 3800 },
    ],
  };

  const tech = {
    members: [
      { name:'Cerberus',   addr: FAKE_ADDRS[4] },
      { name:'Aurora',     addr: FAKE_ADDRS[7] },
      { name:'Kusari',     addr: FAKE_ADDRS[5] },
      { name:'PolkaLab',   addr: FAKE_ADDRS[2] },
      { name:'Sakura Node',addr: FAKE_ADDRS[0] },
    ],
    motions: [
      { id: 18, title:'Hotfix: reentrancy guard en liquidityProxy::swap', threshold:'3/5', aye:3, nay:0, status:'passed' },
      { id: 17, title:'Enable fast-track on referendum #42', threshold:'4/5', aye:2, nay:1, status:'open' },
    ],
  };

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
            {wallets.map((w, i) => (
              <div key={w.id || i} className="wallet-list-card clickable" onClick={() => setDetailWallet(w)}>
                <div style={{width: 36, height: 36, borderRadius: 8, background:'linear-gradient(135deg,#9B1B30,#4A3566)', display:'grid', placeItems:'center', fontWeight:800}}>{w.alias[0]}</div>
                <div style={{flex:1, minWidth: 0}}>
                  <div style={{fontWeight: 700}}>{w.alias}</div>
                  <div className="muted tiny num">{fmt.addr(w.addr, 8, 6)}</div>
                </div>
                <div style={{textAlign:'right'}}>
                  <div className="num" style={{fontWeight:700, fontSize:15}}>${w.value.toLocaleString()}</div>
                  {w.live && <span className="tag ok" style={{fontSize:10}}><span className="live-dot" style={{width:5,height:5}}/> live</span>}
                </div>
              </div>
            ))}
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

function IntelligenceSection({ tweaks }) {
  const t = useT();
  const insights = [
    { type:'volume',  title:'Unusual volume on PSWAP/KUSD pool', body:'+340% vs 24h avg · 18 large swaps clustered within 42 min.', tag:'Volume anomaly', severity: 'high' },
    { type:'whale',   title:'Whale accumulation · cnVmD3…zRyP',  body:'Moved 120,400 XOR out of Polkaswap into cold wallet in 4h.', tag:'Whale alert', severity: 'high' },
    { type:'gov',     title:'Referendum #42 closes in 6h',        body:'Currently 68% AYE · 18.4% turnout · likely to pass.', tag:'Governance', severity: 'mid' },
    { type:'bridge',  title:'Bridge settlement slowing',          body:'ETH → SORA avg now 11.2 min (was 7.4 min 24h ago).', tag:'Bridge', severity: 'mid' },
    { type:'staking', title:'Validator Akira entering oversub.',  body:'Commission jumped to 8% · 12 noms moved to Cerberus.', tag:'Staking', severity: 'low' },
    { type:'burn',    title:'Burn rate up 14% this week',         body:'Swap volume on PSWAP pairs driving fee burn surge.', tag:'Burn', severity: 'low' },
    { type:'pool',    title:'New pool listed: KEN/KUSD',          body:'TVL crossed $200K within 8 blocks of creation.', tag:'Pool', severity: 'low' },
    { type:'holder',  title:'Top-20 holder rotation',             body:'3 new addresses entered top 20 in last 72h.', tag:'Holders', severity: 'low' },
  ];

  return (
    <div>
      <PageHeader title={t('intel.title')} sub={t('intel.sub')}>
        <span className="tag ok"><span className="live-dot" style={{width:5,height:5}}/> engine active</span>
      </PageHeader>

      <KpiGrid items={[
        { label:'Insights · 24h', value: '42', delta:'▲ 8 since last refresh', deltaDir:'up' },
        { label:'Active Alerts',  value: '3', valStyle:{color:'#F5B041'}, sub:'high-severity open' },
        { label:'Watchlist Hits', value: '7', sub:'tracked addresses' },
        { label:'Open Anomalies', value: '2', sub:'last 4h' },
      ]}/>

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
