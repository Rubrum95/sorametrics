/* global React, fmt, TOKENS, FAKE_ADDRS, IDENTITIES, seededRand, sparkPath, areaPath, I, useDrill, useT, useWallets, AddWalletModal, WalletDetailsModal, ExportCsvButton, exportCsv, useToast */
const { useState, useMemo, useEffect } = React;

/* =========================================================================
   Shared: KpiGrid, MiniSpark, TokenBadge, TokenPair
   ========================================================================= */

// Shared compact KPI card row used in every section (Swaps/Transfers/…).
// Accepts an optional inline icon so titles like "Top Pair: DAI/XOR" or
// "Top Gainer: XOR" render the token logos alongside the number instead
// of just text. Three ways to request an icon (first one wins):
//   · k.icon    — any JSX node to render inline (e.g. <I.bolt/>).
//   · k.logoSym — single symbol, renders <TokenBadge sym={logoSym} size={18}/>.
//   · k.pair    — { a, b } for two-token pairs, renders two overlapping badges.
function KpiGrid({ items }) {
  return (
    <div className="swaps-stats-grid">
      {items.map((k, i) => {
        let icon = null;
        if (k.icon) icon = k.icon;
        else if (k.pair && k.pair.a && k.pair.b) {
          icon = (
            <span style={{display:'inline-flex', marginRight:2}}>
              <TokenBadge sym={k.pair.a} size={16}/>
              <TokenBadge sym={k.pair.b} size={16} style={{marginLeft:-5}}/>
            </span>
          );
        } else if (k.logoSym) {
          icon = <TokenBadge sym={k.logoSym} logo={k.logo} size={18}/>;
        }
        return (
          <div className="stat-card" key={i}>
            <span className="stat-label">{k.label}</span>
            <span className={'stat-value num' + (icon ? ' stat-value-row' : '')} style={k.valStyle || {}}>
              {icon}
              <span>{k.value}
                {k.unit && <span style={{fontSize: 12, color:'var(--fg-2)', marginLeft: 5}}>{k.unit}</span>}
              </span>
            </span>
            {k.delta && <span className={'stat-delta ' + (k.deltaDir || 'up')}>{k.delta}</span>}
            {k.sub && <span className="stat-sub">{k.sub}</span>}
          </div>
        );
      })}
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

// Renders a round token badge. When `logo` URL/base64 is given (from
// /history/global/transfers, /tokens, /balance), renders the real image;
// otherwise falls back to the gradient initial letter.
function TokenBadge({ sym, logo, size = 22 }) {
  // Prefer an inline logo; fall back to the global TOKEN_LOGOS cache populated
  // from /tokens so every callsite renders real artwork instead of an initial.
  const src = logo || (sym && window.TOKEN_LOGOS && window.TOKEN_LOGOS[sym]) || null;
  if (src) {
    return (
      <img src={src} alt={sym || ''}
        style={{
          width: size, height: size, borderRadius: '50%',
          flexShrink: 0, objectFit: 'cover',
          background: 'rgba(255,255,255,0.04)',
        }}
        onError={(e) => { e.currentTarget.style.display = 'none'; }}
      />
    );
  }
  const t = TOKENS[sym] || {};
  return (
    <span className="token-dot"
      style={{width: size, height: size, background: t.grad || 'linear-gradient(135deg,#64748B,#334155)'}}>
      {sym ? sym[0] : '?'}
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
  const [dateFilter, setDateFilter] = useState('');
  const [networkOverview, setNetworkOverview] = useState(null);

  // Real network stats for the KPI strip (replaces fake numbers).
  useEffect(() => {
    let cancelled = false;
    const pull = () => fetch('/stats/overview').then(r => r.ok ? r.json() : null).then(j => {
      if (!cancelled) setNetworkOverview(j);
    }).catch(() => {});
    pull();
    const id = setInterval(pull, 60_000);
    return () => { cancelled = true; clearInterval(id); };
  }, []);

  // Endpoint with optional date cutoff and server-side asset filter.
  const tEndpoint = useMemo(() => {
    const q = new URLSearchParams();
    if (dateFilter) {
      const ts = new Date(dateFilter).getTime();
      if (Number.isFinite(ts)) q.set('before', String(ts));
    }
    if (assetFilter) q.set('symbol', assetFilter);
    return '/history/global/transfers' + (q.toString() ? '?' + q.toString() : '');
  }, [dateFilter, assetFilter]);

  // Real transfers from prod /history/global/transfers. Shape mapping:
  //   prod { time, block, hash, extrinsic_id, from, to, amount, symbol, logo, usdValue }
  // Pagination is server-side — backend exposes total/totalPages. assetFilter
  // already hits the server via the endpoint memo above, so local filtering
  // below can be removed too.
  const pageSize = tweaks.density === 'compact' ? 12 : tweaks.density === 'spacious' ? 6 : 10;
  const { items: rawTransfers, total: backendTotal, totalPages: backendTotalPages, refresh, loading } = useHistory(tEndpoint, { pageSize, page, pollMs: 20_000 });
  const rows = useMemo(() => {
    if (!rawTransfers || rawTransfers.length === 0) return [];
    return rawTransfers.map((r, i) => ({
      id: 't-' + (r.hash || (r.block + ':' + i)),
      ts: parseHistTime(r.time),
      block: r.block,
      hash: r.hash,
      sym: r.symbol,
      logo: r.logo,  // real token logo from prod /history/transfers
      from: r.from,
      to: r.to,
      amt: Number(r.amount) || 0,
      usd: Number(r.usdValue) || 0,
      fee: 0,
      memo: '',
    }));
  }, [rawTransfers]);

  const visible = rows;
  const total = backendTotal ?? rows.length;

  return (
    <div>
      <PageHeader title={t('transfers.title')} sub={t('transfers.sub')}>
        <span className="tag ok"><span className="live-dot" style={{width:5,height:5}}/> {t('btn.streaming')}</span>
        <ExportCsvButton section="transfers"
          headers={['Time','Block','Asset','From','To','Amount','USD','Fee','Memo']}
          rows={rows.map(r => ({
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

      {(() => {
        // Real KPIs derived from the current dataset + /stats/overview.
        const volUsd = rows.reduce((s, r) => s + (r.usd || 0), 0);
        const unique = new Set();
        const sendCount = {};
        rows.forEach(r => {
          if (r.from) { unique.add(r.from); sendCount[r.from] = (sendCount[r.from] || 0) + 1; }
          if (r.to) unique.add(r.to);
        });
        const topSenderEntry = Object.entries(sendCount).sort((a, b) => b[1] - a[1])[0];
        // Prefer on-chain identity; falls back to short addr until the batch resolves.
        if (topSenderEntry) window.requestIdentity?.(topSenderEntry[0]);
        const topSender = topSenderEntry
          ? (window.identityName?.(topSenderEntry[0]) || fmt.addr(topSenderEntry[0], 5, 4))
          : '—';
        const topSenderCount = topSenderEntry ? topSenderEntry[1] : 0;
        const netVol = Number(networkOverview?.network?.transfersVolume) || volUsd;
        const netCount = Number(networkOverview?.network?.transfersCount) || rows.length;
        const netUnique = Number(networkOverview?.network?.uniqueAddresses) || unique.size;
        return (
          <KpiGrid items={[
            { label: t('nav.transfers') + ' · 24h', value: netCount.toLocaleString(), sub: 'last 24h' },
            { label: t('col.volume') + ' · 24h',    value: fmt.usd(netVol), sub: 'across all assets' },
            { label: 'Top Sender',      value: topSender, valStyle:{fontSize: 18}, sub: topSenderCount ? (topSenderCount + ' transfers') : '—' },
            { label: 'Counterparties',  value: netUnique.toLocaleString(), sub: 'unique addresses' },
          ]}/>
        );
      })()}

      <div className="card" style={{marginTop: 18}}>
        <div className="swaps-filter-bar">
          <div className="asset-chips">
            <button className={'asset-chip' + (!assetFilter ? ' active' : '')} onClick={() => { setAssetFilter(null); setPage(1); }}>{t('chip.all')}</button>
            {Object.keys(TOKENS).map(s => (
              <button key={s} className={'asset-chip' + (assetFilter === s ? ' active' : '')}
                onClick={() => { setAssetFilter(s); setPage(1); }}>
                <TokenBadge sym={s} size={16}/> {s}
              </button>
            ))}
          </div>
          <input
            type="datetime-local"
            className="swap-date-input"
            value={dateFilter}
            onChange={e => { setDateFilter(e.target.value); setPage(1); }}
            title="Filtrar transfers anteriores a esta fecha/hora"
            style={{padding:'6px 10px', border:'1px solid var(--border-color)', borderRadius:8, background:'var(--bg-card)', color:'var(--fg-0)', fontSize:13}}/>
          {dateFilter && (
            <button
              className="btn"
              onClick={() => { setDateFilter(''); setPage(1); }}
              title="Limpiar filtro de fecha"
              style={{padding:'4px 10px'}}>✕</button>
          )}
          <div className="swaps-filter-spacer"/>
          <span className="tag">{(total ?? rows.length).toLocaleString()} transfers{loading ? ' · cargando' : ''}</span>
          <button className="btn" onClick={refresh} disabled={loading} title="Actualizar">↻ Refresh</button>
        </div>

        <div className="swaps-table-wrap responsive-table">
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
                <tr key={r.id} className="swap-row clickable" onClick={() => open({
                  ...r,
                  type:'transfer',
                  title:`${r.sym} transfer · ${fmt.num(r.amt,3)} ${r.sym}`,
                  // Real hash from prod history — keep, don't fabricate.
                  hash: r.hash,
                })}>
                  <td data-label="Time" style={{paddingLeft: 20}} title={fmt.fullDate(r.ts)}>
                    <div style={{fontSize:12, fontWeight:700}}>{fmt.ago(r.ts)}</div>
                    <div className="muted tiny">{fmt.fullDate(r.ts)}</div>
                  </td>
                  <td data-label="Block"><a className="block-link num" href="#" onClick={(e) => e.stopPropagation()}>#{r.block.toLocaleString()}</a></td>
                  <td data-label="Asset"><div style={{display:'flex', alignItems:'center', gap:8}}><TokenBadge sym={r.sym} logo={r.logo}/><span style={{fontWeight:700}}>{r.sym}</span></div></td>
                  <td data-label="From"><AddrStack addr={r.from}/></td>
                  <td data-label="To"><AddrStack addr={r.to}/></td>
                  <td data-label="Amount" style={{textAlign:'right'}}>
                    <div className="num" style={{fontWeight:700}}>{fmt.num(r.amt, 3)} {r.sym}</div>
                    <div className="muted tiny num">${fmt.num(r.usd, 2)}</div>
                  </td>
                  <td data-label="Fee" style={{textAlign:'right'}}>
                    <div className="num tiny">{r.fee.toFixed(4)} XOR</div>
                  </td>
                  <td data-label="Memo" style={{paddingRight: 20}}>
                    <span className="memo-chip">{r.memo}</span>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>

        <Pagination page={page} setPage={setPage} total={total} pageSize={pageSize}/>
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
  const [dateFilter, setDateFilter] = useState('');
  const [networkF, setNetworkF] = useState('all');
  const [networkOverview, setNetworkOverview] = useState(null);

  useEffect(() => {
    let cancelled = false;
    const pull = () => fetch('/stats/overview').then(r => r.ok ? r.json() : null).then(j => {
      if (!cancelled) setNetworkOverview(j);
    }).catch(() => {});
    pull();
    const id = setInterval(pull, 60_000);
    return () => { cancelled = true; clearInterval(id); };
  }, []);

  const bridgesEndpoint = useMemo(() => {
    const q = new URLSearchParams();
    if (dateFilter) {
      const ts = new Date(dateFilter).getTime();
      if (Number.isFinite(ts)) q.set('before', String(ts));
    }
    if (networkF !== 'all') q.set('network', networkF);
    return '/history/global/bridges' + (q.toString() ? '?' + q.toString() : '');
  }, [dateFilter, networkF]);

  // Real bridges from prod /history/global/bridges. Shape:
  //   { timestamp, block, network, direction ("Incoming"/"Outgoing"), sender,
  //     recipient, asset_id, symbol, amount, usd_value, hash, time, logo }
  // Server-side pagination — status/date/network filters already ride on the
  // endpoint URL via the memo above, so the backend returns exactly the page
  // the user is looking at.
  const pageSize = tweaks.density === 'compact' ? 12 : tweaks.density === 'spacious' ? 6 : 10;
  const { items: rawBridges, total: backendTotal, totalPages: backendTotalPages, refresh, loading } = useHistory(bridgesEndpoint, { pageSize, page, pollMs: 30_000 });
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
        logo: r.logo,
        settle: 0,
        sender: r.sender,
        recipient: r.recipient,
        block: r.block,         // needed by drill to look up /block/:n for ETH-origin resolution
        network: r.network,
      };
    });
  }, [rawBridges]);

  // Status filter is client-side within the current page only (backend
  // doesn't take a `status=` param here). Keeps "pending" count relative
  // to what the user is seeing — good enough for an at-a-glance indicator.
  const visible = statusF === 'all' ? rows : rows.filter(r => r.status === statusF);
  const total = backendTotal ?? rows.length;
  const pending = rows.filter(r => r.status === 'pending').length;

  return (
    <div>
      <PageHeader title={t('bridges.title')} sub={t('bridges.sub')}>
        <span className="tag ok"><span className="live-dot" style={{width:5,height:5}}/> {t('btn.streaming')}</span>
        <ExportCsvButton section="bridges"
          headers={['Time','From','To','Asset','By','Amount','Status','Settlement','Hash']}
          rows={rows.map(r => ({
            Time: new Date(r.ts).toISOString(),
            From: r.from, To: r.to,
            Asset: r.sym,
            By: r.dir === 'in' ? (r.recipient || '') : (r.sender || ''),
            Amount: r.amt,
            Status: r.status, Settlement: r.settle + ' min', Hash: r.hash,
          }))}/>
      </PageHeader>

      {(() => {
        // Real KPIs derived from dataset + /stats/overview when exposed.
        const volUsd = rows.reduce((s, r) => s + (r.usd || 0), 0);
        const uniqueAssets = new Set(rows.map(r => r.sym).filter(Boolean)).size;
        const networks = new Set(rows.map(r => r.from === 'SORA' ? r.to : r.from).filter(n => n && n !== 'SORA'));
        const netVol = Number(networkOverview?.network?.bridgesVolume) || volUsd;
        const netAssets = Number(networkOverview?.network?.bridgesAssets) || uniqueAssets;
        return (
          <KpiGrid items={[
            { label:'Bridge Vol · 24h', value: fmt.usd(netVol), sub: 'across ' + networks.size + ' networks' },
            { label:'Assets Bridged',   value: String(netAssets), sub: 'unique assets' },
            { label:'Pending Now',      value: String(pending), valStyle:{color:'#F5B041'}, sub: 'awaiting confirmations' },
            { label:'Networks',         value: networks.size > 0 ? [...networks].slice(0,3).join(', ') : '—', valStyle:{fontSize: 18}, sub: 'active counterparties' },
          ]}/>
        );
      })()}

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
          {(() => {
            // Hybrid: known supported networks (always shown) + any dynamic ones from the data.
            // Backend handler already supports all four families — the user sees the full
            // catalogue and knows the system is wired even if a network has no traffic yet.
            const KNOWN = [
              'Ethereum',
              'Substrate: Liberland',
              'Substrate: Kusama',
              'Substrate: Polkadot',
              'Parachain: Kusama',
              'Parachain: Polkadot',
              'TON',
            ];
            const dynamic = [...new Set(rows.map(r => r.from === 'SORA' ? r.to : r.from).filter(n => n && n !== 'SORA'))];
            const allNets = [...new Set([...KNOWN, ...dynamic])];
            // Count occurrences for the small badge
            const counts = {};
            for (const r of rows) {
              const n = r.from === 'SORA' ? r.to : r.from;
              if (n && n !== 'SORA') counts[n] = (counts[n] || 0) + 1;
            }
            return (
              <select
                value={networkF}
                onChange={e => { setNetworkF(e.target.value); setPage(1); }}
                title="Filtrar por red"
                style={{padding:'6px 10px', border:'1px solid var(--border-color)', borderRadius:8, background:'var(--bg-card)', color:'var(--fg-0)', fontSize:13, cursor:'pointer'}}>
                <option value="all">Todas las redes</option>
                {allNets.map(n => (
                  <option key={n} value={n}>{n}{counts[n] ? '' : ' (0)'}</option>
                ))}
              </select>
            );
          })()}
          <input
            type="datetime-local"
            className="swap-date-input"
            value={dateFilter}
            onChange={e => { setDateFilter(e.target.value); setPage(1); }}
            title="Filtrar bridges anteriores a esta fecha/hora"
            style={{padding:'6px 10px', border:'1px solid var(--border-color)', borderRadius:8, background:'var(--bg-card)', color:'var(--fg-0)', fontSize:13}}/>
          {dateFilter && (
            <button className="btn" onClick={() => { setDateFilter(''); setPage(1); }} style={{padding:'4px 10px'}} title="Limpiar">✕</button>
          )}
          <div className="swaps-filter-spacer"/>
          <span className="tag">{(total ?? rows.length).toLocaleString()} bridges{loading ? ' · cargando' : ''}</span>
          <button className="btn" onClick={refresh} disabled={loading} title="Actualizar">↻ Refresh</button>
        </div>

        <div className="swaps-table-wrap responsive-table">
          <table className="swaps-table">
            <thead>
              <tr>
                <th style={{paddingLeft: 20}}>Time</th>
                <th>Dir</th>
                <th>Asset</th>
                <th>Route</th>
                <th>By</th>
                <th style={{textAlign:'right'}}>Amount</th>
                <th style={{textAlign:'center'}}>Status</th>
                <th style={{paddingRight:20}}>Tx</th>
              </tr>
            </thead>
            <tbody>
              {visible.map(r => (
                <tr key={r.id} className="swap-row clickable" onClick={() => open({...r, type:'bridge', title:`${r.from} → ${r.to}`})}>
                  <td data-label="Time" style={{paddingLeft: 20}} title={fmt.fullDate(r.ts)}>
                    <div style={{fontSize:12, fontWeight:700}}>{fmt.ago(r.ts)}</div>
                    <div className="muted tiny">{fmt.fullDate(r.ts)}</div>
                  </td>
                  <td data-label="Dir">
                    <span className={'bridge-dir ' + r.dir}>
                      {r.dir === 'in' ? '↓ IN' : '↑ OUT'}
                    </span>
                  </td>
                  <td data-label="Asset"><div style={{display:'flex', alignItems:'center', gap:8}}><TokenBadge sym={r.sym} logo={r.logo}/><span style={{fontWeight:700}}>{r.sym}</span></div></td>
                  <td data-label="Route">
                    <div className="chain-route">
                      <span className={'chain-tag c-' + r.from.toLowerCase()}>{r.from}</span>
                      <span className="route-arr">→</span>
                      <span className={'chain-tag c-' + r.to.toLowerCase()}>{r.to}</span>
                    </div>
                  </td>
                  <td data-label="By">
                    {/* SORA-side wallet of the bridger. IN: the SORA recipient
                        (who is receiving the bridged tokens on SORA).
                        OUT: the SORA sender (who is pushing tokens out).
                        Click = open WalletDetails modal (same UX as Holders /
                        Portfolio). stopPropagation so we don't also trigger
                        the row drill for the bridge itself. */}
                    {(() => {
                      const soraAddr = r.dir === 'in' ? r.recipient : r.sender;
                      if (!soraAddr) return <span className="muted tiny">—</span>;
                      return (
                        <span className="num tiny"
                          title={soraAddr + ' · clic para abrir detalle'}
                          onClick={(e) => {
                            e.stopPropagation();
                            window.openWalletDetails?.(soraAddr);
                          }}
                          style={{cursor:'pointer', color:'var(--accent)', textDecoration:'underline dotted', textUnderlineOffset:3}}>
                          {fmt.addr(soraAddr, 6, 4)}
                        </span>
                      );
                    })()}
                  </td>
                  <td data-label="Amount" style={{textAlign:'right'}}>
                    <div className="num" style={{fontWeight:700}}>{fmt.num(r.amt, 2)} {r.sym}</div>
                  </td>
                  <td data-label="Status" style={{textAlign:'center'}}>
                    <span className={'br-status ' + r.status}>
                      {r.status === 'done' ? '✓ Done' : r.status === 'pending' ? '⏳ Pending' : '✗ Failed'}
                    </span>
                  </td>
                  <td data-label="Tx" style={{paddingRight:20}}>
                    <code className="num tiny" style={{color:'var(--fg-2)'}}>{r.hash.slice(0,10)}…</code>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>

        <Pagination page={page} setPage={setPage} total={total} pageSize={pageSize}/>
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
  const [typeFilter, setTypeFilter] = useState('');
  const [dateFilter, setDateFilter] = useState('');

  const [base, quote] = pair.split('/');

  // Prod /history/global/orderbook returns order events (Place, Fill, Cancel).
  // Endpoint accepts ?type= and ?before= for filtering (matches v1 contract).
  const obEndpoint = useMemo(() => {
    const q = new URLSearchParams();
    if (typeFilter) q.set('type', typeFilter);
    if (dateFilter) {
      const ts = new Date(dateFilter).getTime();
      if (Number.isFinite(ts)) q.set('before', String(ts));
    }
    return '/history/global/orderbook' + (q.toString() ? '?' + q.toString() : '');
  }, [typeFilter, dateFilter]);
  const { items: rawOrders, refresh, loading } = useHistory(obEndpoint, { pageSize: 40, page: 1, pollMs: 20_000 });
  const { fills, mid, spread } = useMemo(() => {
    // We used to synthesise bids/asks with seededRand because SORA has no
    // public orderbook-snapshot endpoint. That was dishonest (the numbers did
    // not reflect reality). Removed. Recent Fills uses /history/global/orderbook
    // which IS real. Mid/spread are derived from the last buy + last sell fill
    // when both exist, otherwise shown as —.
    const filtered = (rawOrders || []).filter(o => {
      if (!o.base_asset || !o.quote_asset) return false;
      const p = o.base_asset + '/' + o.quote_asset;
      return p === pair;
    });
    const fillsArr = filtered.slice(0, 20).map(o => ({
      ts: parseHistTime(o.time),
      side: (o.side || '').toLowerCase(),
      price: Number(o.price) || 0,
      amount: Number(o.amount) || 0,
      wallet: o.wallet,
      hash: o.hash,
      eventType: o.event_type,
    }));
    const lastBuy  = fillsArr.find(f => f.side === 'buy'  && f.price > 0);
    const lastSell = fillsArr.find(f => f.side === 'sell' && f.price > 0);
    const midPx  = lastBuy && lastSell ? (lastBuy.price + lastSell.price) / 2
                 : lastBuy ? lastBuy.price
                 : lastSell ? lastSell.price : null;
    const spreadBps = lastBuy && lastSell && midPx
      ? Math.abs((lastSell.price - lastBuy.price) / midPx) * 10000 : null;
    return { fills: fillsArr, mid: midPx, spread: spreadBps };
  }, [pair, rawOrders]);

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
        { label:'Fills (recent)', value: String(fills.length), sub: 'from /history/global/orderbook' },
        { label:'Spread',         value: spread != null ? spread.toFixed(1) : '—', unit: spread != null ? 'bps' : '', valStyle:{color:'#F5B041'} },
        { label:'Mid price',      value: mid != null ? mid.toFixed(6) : '—', unit: mid != null ? quote : '', sub: 'avg(last buy, last sell)' },
        { label:'Last Fill',      value: fills[0] ? fills[0].side.toUpperCase() : '—', valStyle:{color: fills[0]?.side === 'buy' ? '#10B981' : '#EF4444'}, sub: fills[0] ? fmt.ago(fills[0].ts) : '' },
      ]}/>

      {/* Honest disclosure: SORA has no public orderbook-snapshot endpoint, so
          we don't fabricate a bids/asks ladder. Only real recent fills below. */}
      <div className="card" style={{marginTop: 14, padding: '14px 16px', display:'flex', gap:14, alignItems:'center', flexWrap:'wrap'}}>
        <div style={{fontSize:13, color:'var(--fg-2)', lineHeight:1.5}}>
          <span style={{fontWeight:700, color:'var(--fg-0)'}}>ℹ️ {t('orderbook.noSnapshot.title', 'No live depth snapshot')}</span>
          <span style={{marginLeft:8}}>
            {t('orderbook.noSnapshot.body', 'SORA does not expose a public bids/asks snapshot. Only recent fills (Place / Fill / Cancel events) are shown below.')}
          </span>
        </div>
      </div>

      <div className="card" style={{marginTop: 18}}>
        <div className="card-header">
          <div className="card-title"><span className="dot"/> Recent Fills · {pair}</div>
          <span className="tag ok"><span className="live-dot" style={{width:5,height:5}}/> live</span>
        </div>
        <div className="swaps-table-wrap responsive-table">
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
                  <td data-label="Time" style={{paddingLeft:20}}><span className="muted tiny" title={fmt.fullDate(f.ts)}>{fmt.ago(f.ts)}</span></td>
                  <td data-label="Side"><span className={'fill-side ' + f.side}>{f.side === 'buy' ? '▲ BUY' : '▼ SELL'}</span></td>
                  <td data-label="Price" style={{textAlign:'right'}} className="num">{f.price.toFixed(4)}</td>
                  <td data-label="Amount" style={{textAlign:'right'}} className="num">{f.amount.toFixed(2)}</td>
                  <td data-label="Total" style={{textAlign:'right', paddingRight:20}} className="num">{(f.price * f.amount).toFixed(2)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>

      {/* Global order book events table — all pairs, all event types.
          Matches v1's standalone Orderbook tab (Placed/Canceled/Executed/Filled/Market). */}
      <div className="card" style={{marginTop: 18}}>
        <div className="card-header">
          <div className="card-title"><span className="dot"/> Todos los eventos</div>
          <div style={{display:'flex', gap:8, alignItems:'center', flexWrap:'wrap'}}>
            <select
              value={typeFilter}
              onChange={e => setTypeFilter(e.target.value)}
              title="Tipo de evento"
              style={{padding:'6px 10px', border:'1px solid var(--border-color)', borderRadius:8, background:'var(--bg-card)', color:'var(--fg-0)', fontSize:13, cursor:'pointer'}}>
              <option value="">All Types</option>
              <option value="placed">Placed</option>
              <option value="canceled">Canceled</option>
              <option value="executed">Executed</option>
              <option value="filled">Filled</option>
              <option value="market">Market</option>
            </select>
            <input
              type="datetime-local"
              className="swap-date-input"
              value={dateFilter}
              onChange={e => setDateFilter(e.target.value)}
              title="Filtrar eventos anteriores a esta fecha/hora"
              style={{padding:'6px 10px', border:'1px solid var(--border-color)', borderRadius:8, background:'var(--bg-card)', color:'var(--fg-0)', fontSize:13}}/>
            {dateFilter && (
              <button className="btn" onClick={() => setDateFilter('')} style={{padding:'4px 10px'}} title="Limpiar">✕</button>
            )}
            <span className="tag">{(rawOrders || []).length} eventos{loading ? ' · cargando' : ''}</span>
            <button className="btn" onClick={refresh} disabled={loading} title="Actualizar">↻ Refresh</button>
          </div>
        </div>
        <div className="swaps-table-wrap">
          <table className="swaps-table">
            <thead>
              <tr>
                <th style={{paddingLeft:20}}>Hora</th>
                <th>Bloque</th>
                <th>Tipo</th>
                <th>Par</th>
                <th>Lado</th>
                <th style={{textAlign:'right'}}>Precio</th>
                <th style={{textAlign:'right'}}>Cantidad</th>
                <th style={{paddingRight:20}}>Wallet</th>
              </tr>
            </thead>
            <tbody>
              {(rawOrders || []).length === 0 && (
                <tr><td colSpan={8} style={{padding:32, textAlign:'center', color:'var(--fg-2)'}}>
                  {loading ? 'Cargando eventos...' : 'Sin eventos para este filtro'}
                </td></tr>
              )}
              {(rawOrders || []).slice(0, 50).map((o, i) => {
                const ts = parseHistTime(o.time);
                const baseSym = o.base_asset || '?';
                const quoteSym = o.quote_asset || '?';
                const side = (o.side || '').toLowerCase();
                return (
                  <tr key={'ob-' + (o.hash || (o.block + ':' + i))}>
                    <td style={{paddingLeft:20}} title={fmt.fullDate(ts)}>
                      <div style={{fontSize:12, fontWeight:700}}>{fmt.ago(ts)}</div>
                      <div className="muted tiny">{fmt.fullDate(ts)}</div>
                    </td>
                    <td><a className="block-link num" href="#" onClick={e => e.preventDefault()}>#{(o.block || 0).toLocaleString()}</a></td>
                    <td><span className="tag">{o.event_type || '—'}</span></td>
                    <td><TokenPair a={baseSym} b={quoteSym}/></td>
                    <td><span className={'fill-side ' + side}>{side === 'buy' ? '▲ BUY' : side === 'sell' ? '▼ SELL' : '—'}</span></td>
                    <td style={{textAlign:'right'}} className="num">{Number(o.price || 0).toFixed(6)}</td>
                    <td style={{textAlign:'right'}} className="num">{Number(o.amount || 0).toFixed(4)}</td>
                    <td style={{paddingRight:20}}>
                      <AddrStack addr={o.wallet}/>
                    </td>
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

/* =========================================================================
   POOLS / LIQUIDITY
   ========================================================================= */

// Matches prod's 4 DEX bases on SORA. Verified 2026-04-21 against
// /pools?base=<symbol>: the DEX 1 base is XSTUSD (synthetic USD), NOT XST.
// XST appears only as a TARGET in XSTUSD/XST pools. Sending base=XST to the
// backend returns an empty result, which is why the DEX-1 filter looked broken.
const DEX_BASES = [
  { base: 'XOR',    dex: 0, color: '#E5243B' },
  { base: 'XSTUSD', dex: 1, color: '#F5B041' },
  { base: 'KUSD',   dex: 2, color: '#60A5FA' },
  { base: 'VXOR',   dex: 3, color: '#7B5B90' },
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
        { label:'Top Pool',         value: pools[0] ? (pools[0].base.symbol + '/' + pools[0].target.symbol) : '—',
          pair: pools[0] ? { a: pools[0].base.symbol, b: pools[0].target.symbol } : null,
          sub: pools[0] ? fmt.usd(pools[0].totalUsd) : '' },
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
            <TokenBadge sym={d.base} size={16}/>
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
        <div className="swaps-table-wrap responsive-table">
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
                  <td data-label="Par" style={{paddingLeft: 20}}>
                    <TokenPair a={p.base?.symbol} b={p.target?.symbol}/>
                  </td>
                  <td data-label="Reservas" style={{textAlign:'right'}} className="num">
                    <div style={{lineHeight:1.3}}>
                      <div>{fmt.num(p.baseReserve, 2)} <b>{p.base?.symbol}</b></div>
                      <div>{fmt.num(p.targetReserve, 2)} <b>{p.target?.symbol}</b></div>
                    </div>
                  </td>
                  <td data-label="Total" style={{textAlign:'right', fontWeight: 700, color: '#6EE7B7'}} className="num">
                    {fmt.usd(p.totalUsd)}
                  </td>
                  <td data-label="Providers" style={{textAlign:'center'}}>
                    <button className="btn" onClick={() => setProvidersModal({ base: p.base, target: p.target })}>Providers</button>
                  </td>
                  <td data-label="Activity" style={{textAlign:'center', paddingRight: 20}}>
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
        {/* Use backend-reported total (217 across 22 pages) — pools.length
            is just this page's 10 rows, which made Pagination compute
            totalPages=1 and disable every button. */}
        <Pagination page={page} setPage={setPage} total={total || pools.length} pageSize={10}/>
      </div>

      {/* Global Liquidity Activity — live events from /history/global/liquidity */}
      <GlobalLiquidityActivity/>

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

// Global liquidity activity — aggregated Add/Remove events across all pools.
// Equivalent to v1's second "Actividad" card under the Liquidez tab.
function GlobalLiquidityActivity() {
  const t = useT();
  // Server-side pagination. Backend reads `timestamp=<ms>` (NOT `before=`)
  // as the ceiling for the listing and supports `page` + `limit`.
  // getLiquidityEvents() returns {data,total,page,totalPages} — useHistory
  // surfaces total/totalPages so the pager can walk the full dataset.
  const PAGE_SIZE = 20;
  const [page, setPage] = useState(1);
  const [dateFilter, setDateFilter] = useState('');

  const endpoint = useMemo(() => {
    const q = new URLSearchParams();
    if (dateFilter) {
      const ts = new Date(dateFilter).getTime();
      if (Number.isFinite(ts)) q.set('timestamp', String(ts));
    }
    return '/history/global/liquidity?' + q.toString();
  }, [dateFilter]);

  const {
    items: rawEvents,
    total: backendTotal,
    totalPages: backendTotalPages,
    refresh,
    loading,
  } = useHistory(endpoint, { pageSize: PAGE_SIZE, page, pollMs: 30_000 });

  // Prod shape: { timestamp, block, wallet, pool_base, pool_target,
  //   base_amount, target_amount, usd_value, type, hash, base_logo, target_logo }
  const rows = useMemo(() => (rawEvents || []).map((e, i) => ({
    id: 'liq-' + (e.hash || (e.block + ':' + i)),
    ts: parseHistTime(e.time || e.timestamp),
    block: e.block,
    hash: e.hash,
    type: e.type || e.action || (Number(e.base_amount) > 0 ? 'Add' : 'Remove'),
    baseAmount: Number(e.base_amount ?? e.baseAmount ?? e.base?.amount) || 0,
    targetAmount: Number(e.target_amount ?? e.targetAmount ?? e.target?.amount) || 0,
    baseSymbol: e.pool_base || e.baseSymbol || e.base?.symbol || '',
    targetSymbol: e.pool_target || e.targetSymbol || e.target?.symbol || '',
    baseLogo: e.base_logo || e.base?.logo,
    targetLogo: e.target_logo || e.target?.logo,
    usd: Number(e.usd_value) || 0,
    wallet: e.wallet || e.account,
  })), [rawEvents]);

  // Server paginates — trust backendTotalPages when present, fall back to a
  // local computation only when the backend omits it (legacy responses).
  const total = backendTotal ?? rows.length;
  const totalPages = backendTotalPages && backendTotalPages > 0
    ? backendTotalPages
    : Math.max(1, Math.ceil(total / PAGE_SIZE));
  const pageRows = rows; // backend already sliced for this page
  useEffect(() => {
    if (page > totalPages) setPage(1);
  }, [totalPages, page]);
  const goOlder = () => setPage(p => Math.min(totalPages, p + 1));
  const goNewer = () => setPage(p => Math.max(1, p - 1));

  return (
    <div className="card" style={{marginTop: 20}}>
      <div className="card-header">
        <div className="card-title"><span className="dot"/> {t('pool.activity.globalTitle', 'Actividad global de liquidez')}</div>
        <div style={{display:'flex', gap:8, alignItems:'center', flexWrap:'wrap'}}>
          <input
            type="datetime-local"
            className="swap-date-input"
            value={dateFilter}
            onChange={e => { setDateFilter(e.target.value); setPage(1); }}
            title={t('pool.activity.dateFilterTip', 'Filtrar eventos anteriores a esta fecha/hora')}
            style={{padding:'6px 10px', border:'1px solid var(--border-color)', borderRadius:8, background:'var(--bg-card)', color:'var(--fg-0)', fontSize:13}}/>
          {dateFilter && (
            <button className="btn" onClick={() => { setDateFilter(''); setPage(1); }} style={{padding:'4px 10px'}} title={t('common.clear', 'Limpiar')}>✕</button>
          )}
          <span className="tag">{(total ?? rows.length).toLocaleString()} {t('pool.activity.events', 'eventos')}{loading ? ' · ' + t('common.loadingShort', 'cargando') : ''}</span>
          <button className="btn" onClick={refresh} disabled={loading} title={t('common.refresh', 'Actualizar')}>↻ {t('common.refresh', 'Refresh')}</button>
        </div>
      </div>
      <div className="swaps-table-wrap">
        <table className="swaps-table">
          <thead>
            <tr>
              <th style={{paddingLeft:20}}>{t('pool.col.time', 'Hora')}</th>
              <th>{t('pool.col.block', 'Bloque')}</th>
              <th>{t('pool.col.pair', 'Par')}</th>
              <th>{t('pool.col.type', 'Tipo')}</th>
              <th style={{textAlign:'right'}}>{t('pool.col.amount', 'Cantidad')}</th>
              <th style={{paddingRight:20}}>{t('col.account', 'Wallet')}</th>
            </tr>
          </thead>
          <tbody>
            {pageRows.length === 0 && (
              <tr><td colSpan={6} style={{padding:32, textAlign:'center', color:'var(--fg-2)'}}>
                {loading ? t('pool.activity.loading', 'Cargando actividad...') : t('pool.activity.noneRecent', 'Sin actividad reciente')}
              </td></tr>
            )}
            {pageRows.map(r => (
              <tr key={r.id}>
                <td style={{paddingLeft:20}} title={fmt.fullDate(r.ts)}>
                  {/* Full date as primary (YYYY-MM-DD HH:MM:SS) because the
                      user wants to see the actual moment, not just "3d".
                      "ago" is kept below as a small hint. */}
                  <div style={{fontSize:12, fontWeight:700, color:'var(--fg-0)'}}>{fmt.fullDate(r.ts)}</div>
                  <div className="muted" style={{fontSize:11}}>{fmt.ago(r.ts)}</div>
                </td>
                <td><a className="block-link num" href="#" onClick={e => e.preventDefault()}>#{(r.block || 0).toLocaleString()}</a></td>
                <td><TokenPair a={r.baseSymbol} b={r.targetSymbol}/></td>
                <td>
                  <span className={'tag ' + (String(r.type).toLowerCase().startsWith('add') || String(r.type).toLowerCase() === 'deposit' ? 'ok' : '')}>
                    {String(r.type).toLowerCase().startsWith('add') || String(r.type).toLowerCase() === 'deposit' ? '+ ' + t('pool.activity.add', 'Add') : '− ' + t('pool.activity.remove', 'Remove')}
                  </span>
                </td>
                <td style={{textAlign:'right'}}>
                  <div className="num" style={{fontWeight:700}}>{fmt.num(r.baseAmount, 3)} {r.baseSymbol}</div>
                  <div className="muted tiny num">{fmt.num(r.targetAmount, 3)} {r.targetSymbol}</div>
                </td>
                <td
                  style={{paddingRight:20, cursor: r.wallet ? 'pointer' : 'default'}}
                  title={r.wallet ? t('wallet.openTip', 'Abrir wallet') : ''}
                  onClick={() => { if (r.wallet) window.openWalletDetails?.(r.wallet, IDENTITIES[r.wallet]); }}>
                  {IDENTITIES[r.wallet] && <div style={{fontSize:11, fontWeight:700}}>{IDENTITIES[r.wallet]}</div>}
                  <div className="muted tiny num" style={{textDecoration: r.wallet ? 'underline dotted' : 'none'}}>{r.wallet ? fmt.addr(r.wallet, 5, 4) : '—'}</div>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      {/* Client-side pagination over the 100-record window the backend returns.
          Hidden entirely when there's only one page. */}
      {totalPages > 1 && (
        <div style={{display:'flex', justifyContent:'space-between', alignItems:'center', padding:'12px 20px', gap:10, flexWrap:'wrap', borderTop:'1px solid var(--border)'}}>
          <button className="btn" onClick={goNewer} disabled={page <= 1 || loading}>← {t('explorer.newer', 'Más reciente')}</button>
          <span className="muted tiny">
            {t('pool.activity.pageN', 'Página {n}').replace('{n}', String(page))} / {totalPages}
            <span style={{marginLeft:8, opacity:0.6}}>· {(total ?? rows.length).toLocaleString()} {t('pool.activity.events', 'eventos')}</span>
          </span>
          <button className="btn" onClick={goOlder} disabled={page >= totalPages || loading}>{t('explorer.older', 'Más antiguo')} →</button>
        </div>
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
  const t = useT();
  const [data, setData] = useState(null);
  useEffect(() => {
    let cancelled = false;
    // Backend quirk: /pool/activity expects SYMBOLS (base=XOR&target=VAL),
    // while /pool/providers expects ASSET IDS (base=0x0200…&target=0x0200…).
    // Passing assetId to /pool/activity returns [] silently — which is why
    // the modal was always blank. Use the symbol here to match what the
    // endpoint actually indexes.
    fetch('/pool/activity?base=' + encodeURIComponent(base.symbol) + '&target=' + encodeURIComponent(target.symbol))
      .then(r => r.json()).then(j => { if (!cancelled) setData(j); }).catch(() => {});
    return () => { cancelled = true; };
  }, [base.symbol, target.symbol]);
  const acts = (data && (data.activities || data.data || data)) || [];
  const list = Array.isArray(acts) ? acts : [];
  return (
    <div className="sm-modal-backdrop" onClick={onClose}>
      <div className="sm-modal" style={{width: 720}} onClick={e => e.stopPropagation()}>
        <div className="sm-modal-head">
          <h3 style={{margin:0}}>{t('pool.activity.title', 'Activity')} · {base.symbol}/{target.symbol}</h3>
          <button className="sm-modal-x" onClick={onClose}>×</button>
        </div>
        <div className="sm-modal-body" style={{maxHeight: 520, overflow:'auto'}}>
          {!data ? <div className="muted">{t('common.loading', 'Cargando…')}</div> :
            list.length === 0 ? <div className="muted">{t('pool.activity.empty', 'Sin actividad reciente en este pool.')}</div> :
            <table className="lp-table">
              <thead><tr>
                <th>{t('pool.col.time', 'Tiempo')}</th>
                <th>{t('pool.col.type', 'Tipo')}</th>
                <th style={{textAlign:'right'}}>{t('pool.col.amount', 'Monto')}</th>
                <th>{t('col.account', 'Wallet')}</th>
              </tr></thead>
              <tbody>
                {list.slice(0, 80).map((a, i) => {
                  const ts = parseHistTime(a.time || a.timestamp);
                  return (
                  <tr key={(a.hash || '') + i}>
                    <td className="tiny" title={fmt.fullDate(ts)}>{fmt.ago(ts)}</td>
                    <td><span className="tag">{a.type || a.event_type || 'swap'}</span></td>
                    <td style={{textAlign:'right'}} className="num">{fmt.num(Number(a.amount || a.base_amount || 0), 4)}</td>
                    <td>
                      {(() => {
                        const w = a.wallet || a.account;
                        if (!w) return <span className="muted">—</span>;
                        return (
                          <span className="num tiny"
                            style={{cursor:'pointer', color:'var(--accent)', textDecoration:'underline dotted'}}
                            onClick={() => window.openWalletDetails?.(w)}>{fmt.addr(w, 6, 4)}</span>
                        );
                      })()}
                    </td>
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

/* =========================================================================
   TOKENS
   ========================================================================= */

// Crypto-standard price renderer — matches v1 (/script.js · formatPrice):
//   · num >= 0.001 → toLocaleString with 2–6 decimals (trailing zeros trimmed)
//   · 0 < num < 0.001 → subscript notation "$0.0ₙXXXX" where n = leading zeros
//                       count and XXXX = first 4 significant digits.
//   · num === 0 or NaN → "$0.00"
// Use <FormattedPrice value={price}/> anywhere a USD amount is rendered.
function FormattedPrice({ value }) {
  const num = parseFloat(value);
  if (!Number.isFinite(num) || num === 0) {
    return <span><span className="currency-symbol">$</span>0.00</span>;
  }
  if (Math.abs(num) >= 0.001) {
    const formatted = num.toLocaleString('en-US', {
      minimumFractionDigits: 2,
      maximumFractionDigits: 6,
    });
    return <span><span className="currency-symbol">$</span>{formatted}</span>;
  }
  // Sub-0.001 path: extract leading-zero count, show first 4 significant digits
  // inline. `toFixed(30)` gives us enough precision to work with; the regex
  // captures "0.0…" where the trailing 0s are the ones we compress into the
  // subscript counter.
  const str = num.toFixed(30);
  const match = str.match(/^0\.0+/);
  if (!match) {
    return <span><span className="currency-symbol">$</span>{num.toFixed(6)}</span>;
  }
  const zeros = match[0].length - 2; // subtract the "0." prefix
  const remaining = str.substring(match[0].length).substring(0, 4);
  return (
    <span>
      <span className="currency-symbol">$</span>
      0.0<sub style={{color:'var(--fg-3)'}}>{zeros}</sub>{remaining}
    </span>
  );
}

function TokensSection({ tweaks }) {
  const t = useT();
  const [fav, setFav] = useState(() => {
    try { const raw = localStorage.getItem('sm.favTokens'); if (raw) return new Set(JSON.parse(raw)); } catch {}
    return new Set(['XOR','VAL']);
  });
  const [filter, setFilter] = useState('all');
  const [search, setSearch] = useState('');
  const [searchDebounced, setSearchDebounced] = useState('');
  const [timeframe, setTimeframe] = useState(() => {
    try { return localStorage.getItem('sm.tokensTf') || '24h'; } catch { return '24h'; }
  });
  const [page, setPage] = useState(1);
  const [chartToken, setChartToken] = useState(null);

  useEffect(() => { try { localStorage.setItem('sm.favTokens', JSON.stringify([...fav])); } catch {} }, [fav]);
  useEffect(() => { try { localStorage.setItem('sm.tokensTf', timeframe); } catch {} }, [timeframe]);

  // Debounce search (~400ms, like v1's 800ms but snappier).
  useEffect(() => {
    const id = setTimeout(() => { setSearchDebounced(search); setPage(1); }, 400);
    return () => clearTimeout(id);
  }, [search]);

  // Build endpoint with server-side filters matching v1's /tokens contract.
  const endpoint = useMemo(() => {
    const q = new URLSearchParams();
    q.set('timeframe', timeframe);
    q.set('sparkline', 'true');
    if (searchDebounced) q.set('search', searchDebounced);
    if (filter === 'fav' && fav.size) q.set('symbols', [...fav].join(','));
    return '/tokens?' + q.toString();
  }, [timeframe, searchDebounced, filter, fav]);

  const { items: rawTokens, total: backendTotal, totalPages: backendTotalPages, refresh, loading } = useHistory(endpoint, { pageSize: 20, page, pollMs: 60_000 });
  // /stats/overview.network.volume gives us real 24h network volume so the
  // KPI card isn't a stale $0. Prod doesn't expose aggregate market cap here.
  const [networkOverview, setNetworkOverview] = useState(null);
  useEffect(() => {
    let cancelled = false;
    const pull = () => fetch('/stats/overview').then(r => r.ok ? r.json() : null)
      .then(j => { if (!cancelled) setNetworkOverview(j); }).catch(() => {});
    pull();
    const id = setInterval(pull, 60_000);
    return () => { cancelled = true; clearInterval(id); };
  }, []);

  // Market-data enrichment: MCap / Supply / Holders per token.
  //   · SORA-native → MOF (supply) + /burns/supply (price/CG mcap fallback)
  //                   + /holders (via shared serial queue).
  //   · External    → CoinGecko free API, batched + cached 24h in localStorage.
  // Both caches live in localStorage with 24h TTL, so on second load the page
  // paints instantly. Re-validation happens in parallel in the background.
  const [extraMarket, setExtraMarket] = useState(() => {
    // Prime from cache so the first render isn't a wall of "—".
    const primed = {};
    try {
      // External: read CoinGecko localStorage cache directly.
      const cg = JSON.parse(localStorage.getItem('sm.coingeckoCache.v1') || '{}');
      for (const [sym, v] of Object.entries(cg)) if (v?.data) primed[sym] = { ...v.data, source: 'coingecko' };
      // Native: readNativeMarketCache (gets hydrated from localStorage at boot).
      const native = JSON.parse(localStorage.getItem('sm.nativeMarketCache.v2') || '{}');
      for (const [sym, v] of Object.entries(native)) if (v?.data) primed[sym] = { ...v.data, source: 'native' };
    } catch {}
    return primed;
  });
  useEffect(() => {
    if (!rawTokens || rawTokens.length === 0) return;
    let cancelled = false;
    (async () => {
      const externalSyms = [];
      const nativeList = [];
      for (const rt of rawTokens) {
        const sym = rt.symbol;
        if (!sym) continue;
        if (window.isSoraNative?.(sym)) nativeList.push({ sym, assetId: rt.assetId });
        else externalSyms.push(sym);
      }
      // External batch — single CoinGecko request for all externals at once.
      if (externalSyms.length > 0) {
        window.fetchExternalMarketData(externalSyms).then(res => {
          if (cancelled) return;
          setExtraMarket(prev => {
            const next = { ...prev };
            for (const [sym, data] of Object.entries(res)) next[sym] = { ...data, source: 'coingecko' };
            return next;
          });
        });
      }
      // Native — two passes:
      //   1) Fast: MOF supply + /burns/supply (mcap/price). Parallel fan-out,
      //      no serial queue. UI lights up almost immediately.
      //   2) Slow: /holders counts via the shared serial queue. Merges in as
      //      each one arrives so the "Holders" cell fills progressively.
      nativeList.forEach(({ sym, assetId }) => {
        window.fetchNativeMarketData(sym, assetId).then(data => {
          if (cancelled) return;
          setExtraMarket(prev => ({ ...prev, [sym]: { ...(prev[sym] || {}), ...data, source: 'native' } }));
        }).catch(() => {});
      });
      // Pass 2: holders counts (background).
      nativeList.forEach(({ sym, assetId }) => {
        window.fetchNativeHolders?.(assetId).then(h => {
          if (cancelled || h == null) return;
          setExtraMarket(prev => ({ ...prev, [sym]: { ...(prev[sym] || {}), holders: h, source: prev[sym]?.source || 'native' } }));
        }).catch(() => {});
      });
    })();
    return () => { cancelled = true; };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [rawTokens ? rawTokens.map(t => t.symbol).join(',') : '']);
  const tokens = useMemo(() => {
    if (!rawTokens || rawTokens.length === 0) return [];
    return rawTokens.map((rt) => {
      const sym = rt.symbol;
      const extra = extraMarket[sym] || {};
      // Prefer extra (on-chain / CoinGecko) over /tokens shallow fields because
      // Price ALWAYS comes from the backend (Polkaswap liquidityProxy.quote,
      // i.e. on-chain). Never from extra.price (CoinGecko) — user policy:
      // CG is only a source for supply / marketCap / holders. Using the CG
      // price here meant ETH showed $2303 (global CEX VWAP) instead of
      // $1933 (actual SORA pool cotization). Now we consume `rt.price`
      // directly and `extra` only contributes supply/mcap/holders below.
      const price = Number(rt.price) || 0;
      // Backend returns the timeframe-adjusted percentage in `change24h`
      // regardless of the requested timeframe (see /tokens?timeframe=7d
      // returning 7d % in the same field). Trust it when finite, even if it
      // is 0 — only fall back to CoinGecko's 24h cache when the backend
      // didn't publish a number at all, and only when the user IS viewing
      // the 24h timeframe, to avoid mixing stale 24h data into a 7d view.
      const rtRaw = rt.change24h ?? rt.change ?? rt.priceChange;
      const rtChange = rtRaw != null && Number.isFinite(Number(rtRaw)) ? Number(rtRaw) : null;
      const change = rtChange != null
        ? rtChange
        : (timeframe === '24h' ? (Number(extra.change24h) || 0) : 0);
      const realSpark = Array.isArray(rt.sparkline)
        ? rt.sparkline.map(p => Number(p.value) || 0).filter(v => v > 0)
        : [];
      return {
        sym,
        name: rt.name || (sym + ' Token'),
        price,
        supply: Number(extra.totalSupply) || Number(rt.totalSupply) || 0,
        mcap: Number(extra.marketCap) || Number(rt.marketCap) || 0,
        change,
        spark: realSpark.length >= 2 ? realSpark : [price, price],
        logo: rt.logo,
        assetId: rt.assetId,
        holders: (extra.holders != null && extra.holders > 0) ? extra.holders : (Number(rt.holders) || null),
        native: window.isSoraNative?.(sym) ?? true,
        source: extra.source || null,
      };
    });
  }, [rawTokens, timeframe, extraMarket]);

  const visible = tokens;
  const gainer = [...tokens].sort((a,b) => b.change - a.change)[0] || { sym: '—', change: 0 };
  const loser  = [...tokens].sort((a,b) => a.change - b.change)[0] || { sym: '—', change: 0 };
  // Real 24h network trading volume (from /stats/overview) replaces fake mcap sum.
  const vol24h = Number(networkOverview?.network?.volume) || 0;

  const toggleFav = (sym) => {
    setFav(prev => {
      const n = new Set(prev);
      if (n.has(sym)) n.delete(sym); else n.add(sym);
      return n;
    });
  };

  const tfLabel = { '1h':'1h', '4h':'4h', '24h':'24h', '7d':'7d' }[timeframe] || '24h';

  return (
    <div>
      <PageHeader title={t('tokens.title')} sub={t('tokens.sub')}>
        <button
          className="icon-btn"
          onClick={refresh}
          disabled={loading}
          title="Actualizar"
          style={{background:'none', border:'1px solid var(--border-color)', borderRadius:8, color:'var(--fg-0)', padding:'6px 10px', cursor:'pointer', fontSize:14, opacity: loading ? 0.5 : 1}}>
          {loading ? '⌛' : '⟳'}
        </button>
        <ExportCsvButton section="tokens"
          headers={['Symbol','Price','Change','MarketCap','Supply','Holders']}
          rows={visible.map(r => ({
            Symbol: r.sym, Price: r.price, Change: r.change.toFixed(2)+'%',
            MarketCap: r.mcap.toFixed(0), Supply: r.supply, Holders: r.holders ?? '',
          }))}/>
        <div className="status-toggle">
          <button className={'status-opt' + (filter==='all' ? ' active' : '')} onClick={() => { setFilter('all'); setPage(1); }}>{t('chip.all')}</button>
          <button className={'status-opt' + (filter==='fav' ? ' active' : '')} onClick={() => { setFilter('fav'); setPage(1); }}>★ {t('chip.favorites')}</button>
        </div>
      </PageHeader>

      <div className="filter-row" style={{display:'flex', gap:12, alignItems:'center', flexWrap:'wrap', margin:'10px 0 14px'}}>
        <input
          type="text"
          placeholder="Buscar por nombre, símbolo o ID..."
          value={search}
          onChange={e => setSearch(e.target.value)}
          style={{flex:1, minWidth:220, padding:'8px 12px', borderRadius:8, border:'1px solid var(--border-color)', background:'var(--bg-card)', color:'var(--fg-0)', fontSize:13, outline:'none'}}/>
        <select
          value={timeframe}
          onChange={e => { setTimeframe(e.target.value); setPage(1); }}
          style={{padding:'8px 10px', borderRadius:8, border:'1px solid var(--border-color)', background:'var(--bg-card)', color:'var(--fg-0)', fontSize:13, cursor:'pointer'}}
          title="Timeframe para el % de cambio">
          <option value="1h">1h %</option>
          <option value="4h">4h %</option>
          <option value="24h">24h %</option>
          <option value="7d">7d %</option>
        </select>
      </div>

      <KpiGrid items={[
        { label:'Total Tokens',   value: String(tokens.length), sub:'registered' },
        { label:'Volume · 24H',   value: fmt.usd(vol24h), sub: 'network-wide' },
        { label:'Top Gainer',     value: gainer.sym, logoSym: gainer.sym !== '—' ? gainer.sym : null, valStyle:{color: '#6EE7B7'}, sub: '+' + gainer.change.toFixed(1) + '% · ' + tfLabel },
        { label:'Top Loser',      value: loser.sym,  logoSym: loser.sym  !== '—' ? loser.sym  : null, valStyle:{color: '#FCA5A5'}, sub: loser.change.toFixed(1) + '% · ' + tfLabel },
      ]}/>

      {visible.length === 0 && !loading && (
        <div className="card" style={{padding:24, textAlign:'center', color:'var(--fg-2)'}}>
          {searchDebounced ? `No se encontraron tokens para "${searchDebounced}".` : 'No hay tokens disponibles.'}
        </div>
      )}

      <div className="token-grid">
        {visible.map(tk => {
          const tkCfg = TOKENS[tk.sym] || { grad: 'linear-gradient(135deg, #7B5B90, #4A3566)' };
          return (
            <div
              key={tk.sym}
              className="token-card clickable"
              onClick={() => setChartToken(tk)}
              style={{cursor:'pointer'}}
              title="Ver gráfico histórico">
              <div className="token-card-head">
                <TokenBadge sym={tk.sym} logo={tk.logo} size={36}/>
                <div style={{flex:1, minWidth:0}}>
                  <div className="token-card-sym">{tk.sym}</div>
                  <div className="muted tiny">{tk.name}</div>
                </div>
                <button
                  className={'fav-btn' + (fav.has(tk.sym) ? ' on' : '')}
                  onClick={e => { e.stopPropagation(); toggleFav(tk.sym); }}>★</button>
              </div>
              <div className="token-card-price num">
                <FormattedPrice value={tk.price}/>
              </div>
              <div className={'token-card-delta ' + (tk.change >= 0 ? 'up' : 'down')}>
                {tk.change >= 0 ? '▲' : '▼'} {Math.abs(tk.change).toFixed(2)}% · {tfLabel}
              </div>
              <div style={{margin: '10px 0'}}>
                <svg viewBox="0 0 120 36" width="100%" height="36">
                  <path d={sparkPath(tk.spark, 120, 36, 2)} stroke={tk.change >= 0 ? '#10B981' : '#EF4444'} strokeWidth="1.6" fill="none"/>
                </svg>
              </div>
              <div className="token-card-foot">
                <div><span className="muted tiny">Mcap</span><div className="num small">{tk.mcap > 0 ? fmt.usd(tk.mcap, 1) : '—'}</div></div>
                <div><span className="muted tiny">Supply</span><div className="num small">{tk.supply > 0 ? fmt.num(tk.supply, 1) : '—'}</div></div>
                <div>
                  <span className="muted tiny">Holders</span>
                  <div className="num small">
                    {tk.holders != null && tk.holders > 0 ? (
                      <span
                        style={{cursor:'pointer', textDecoration:'underline dotted'}}
                        onClick={e => { e.stopPropagation(); window.location.hash = '#holders/' + encodeURIComponent(tk.sym); }}
                        title="Ver holders">
                        {fmt.num(tk.holders, 0)}
                      </span>
                    ) : '—'}
                  </div>
                </div>
              </div>
              {/* Source chip removed — irrelevant to the user. */}
            </div>
          );
        })}
      </div>

      {/* Pagination: backend returns {total, totalPages} in the /tokens
          response (verified: XOR search over 277 tokens yields multiple
          pages). Trust backendTotal so First / Last navigate the whole
          catalogue, not just what's currently on screen. */}
      <div style={{marginTop:20, display:'flex', alignItems:'center', justifyContent:'space-between', flexWrap:'wrap', gap:10}}>
        <span className="muted tiny">
          {(backendTotal ?? visible.length).toLocaleString()} tokens
          {loading ? ' · ' + t('common.loadingShort', 'cargando') : ''}
        </span>
        <Pagination page={page} setPage={setPage} total={backendTotal ?? visible.length} pageSize={20}/>
      </div>

      {chartToken && <TokenChartModal token={chartToken} onClose={() => setChartToken(null)}/>}
    </div>
  );
}

// Modal with full Chart.js line chart for a single token.
// Uses /chart/:symbol?res=... to fetch candles.
function TokenChartModal({ token, onClose }) {
  const canvasRef = React.useRef(null);
  const chartRef = React.useRef(null);
  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(true);
  const [res, setRes] = useState('1h');

  // Backend expects resolution in minutes (integer). Map UI labels → minutes.
  const RES_MIN = { '5m': 5, '15m': 15, '1h': 60, '4h': 240, '1d': 1440 };

  useEffect(() => {
    let cancelled = false;
    setLoading(true);
    const resMin = RES_MIN[res] || 60;
    fetch('/chart/' + encodeURIComponent(token.sym) + '?res=' + resMin)
      .then(r => r.ok ? r.json() : null)
      .then(j => { if (!cancelled) { setData(j); setLoading(false); } })
      .catch(() => { if (!cancelled) setLoading(false); });
    return () => { cancelled = true; };
  }, [token.sym, res]);

  useEffect(() => {
    if (!canvasRef.current || !window.Chart) return;
    if (chartRef.current) { chartRef.current.destroy(); chartRef.current = null; }
    // Prod /chart/:symbol returns a raw array of OHLC rows, not {candles:[…]}.
    const candles = Array.isArray(data) ? data : ((data && (data.candles || data.data)) || []);
    if (!candles.length) return;
    // `time` is a unix seconds value — multiply by 1000 for Date.
    const toMs = (t) => {
      const n = Number(t);
      if (!Number.isFinite(n)) return Date.parse(t);
      return n < 1e12 ? n * 1000 : n;
    };
    const labels = candles.map(c => new Date(toMs(c.time || c.t || c.timestamp)).toLocaleString());
    const values = candles.map(c => Number(c.close ?? c.value ?? c.price) || 0);
    chartRef.current = new window.Chart(canvasRef.current.getContext('2d'), {
      type: 'line',
      data: { labels, datasets: [{
        label: token.sym, data: values,
        borderColor: '#9B1B30', backgroundColor: 'rgba(155,27,48,0.1)',
        fill: true, tension: 0.25, pointRadius: 0, borderWidth: 2,
      }] },
      options: {
        responsive: true, maintainAspectRatio: false,
        plugins: { legend: { display: false } },
        scales: { x: { ticks: { maxTicksLimit: 8 } } },
      },
    });
    return () => { if (chartRef.current) { chartRef.current.destroy(); chartRef.current = null; } };
  }, [data, token.sym]);

  return (
    <div
      onClick={onClose}
      style={{position:'fixed', inset:0, background:'rgba(0,0,0,0.6)', zIndex:10000, display:'flex', alignItems:'center', justifyContent:'center', padding:20}}>
      <div
        onClick={e => e.stopPropagation()}
        style={{background:'#121218', color:'var(--fg-0)', borderRadius:12, maxWidth:900, width:'100%', maxHeight:'90vh', overflow:'auto', padding:22, border:'1px solid var(--border)'}}>
        <div style={{display:'flex', justifyContent:'space-between', alignItems:'center', marginBottom:14, gap:10, flexWrap:'wrap'}}>
          <div>
            <h3 style={{margin:0, fontSize:18}}>{token.sym} · <span className="muted">{token.name}</span></h3>
            <div className="muted tiny num" style={{marginTop:4}}>${token.price < 1 ? token.price.toFixed(6) : token.price.toFixed(2)}</div>
          </div>
          <div style={{display:'flex', gap:6, alignItems:'center', flexWrap:'wrap'}}>
            {['5m','15m','1h','4h','1d'].map(r => (
              <button
                key={r}
                onClick={() => setRes(r)}
                className={'pill' + (res === r ? ' active' : '')}
                style={{padding:'4px 10px', border:'1px solid var(--border-color)', borderRadius:6, background: res === r ? 'var(--accent)' : 'transparent', color: res === r ? '#fff' : 'var(--fg-0)', cursor:'pointer', fontSize:12}}>
                {r}
              </button>
            ))}
            <button
              onClick={onClose}
              style={{padding:'4px 12px', border:'1px solid var(--border-color)', borderRadius:6, background:'transparent', color:'var(--fg-0)', cursor:'pointer'}}>Cerrar</button>
          </div>
        </div>
        <div style={{height:420, position:'relative'}}>
          {loading && <div style={{position:'absolute', inset:0, display:'flex', alignItems:'center', justifyContent:'center', color:'var(--fg-2)'}}>Cargando gráfico...</div>}
          <canvas ref={canvasRef}/>
        </div>
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
  const [asset, setAsset] = useState('XOR');
  const [raw, setRaw] = useState(null);   // { page, totalHolders, totalPages, data: [{ address, balance, balanceStr }] }

  // Each SORA asset has a well-known assetId. The first 4 always render as
  // pills (the most-viewed tokens); the rest live in a dropdown so the list
  // doesn't wrap across two rows. Asset IDs resolved 2026-04-21 from prod's
  // /tokens endpoint — do NOT hand-edit without verifying there.
  const ASSETS = [
    // Always-visible pills
    { sym:'XOR',    id:'0x0200000000000000000000000000000000000000000000000000000000000000', primary: true },
    { sym:'VAL',    id:'0x0200040000000000000000000000000000000000000000000000000000000000', primary: true },
    { sym:'PSWAP',  id:'0x0200050000000000000000000000000000000000000000000000000000000000', primary: true },
    { sym:'KUSD',   id:'0x02000c0000000000000000000000000000000000000000000000000000000000', primary: true },
    // Dropdown extras
    { sym:'TBCD',   id:'0x02000a0000000000000000000000000000000000000000000000000000000000' },
    { sym:'DAI',    id:'0x0200060000000000000000000000000000000000000000000000000000000000' },
    { sym:'ETH',    id:'0x0200070000000000000000000000000000000000000000000000000000000000' },
    { sym:'KEN',    id:'0x02000b0000000000000000000000000000000000000000000000000000000000' },
    { sym:'KARMA',  id:'0x02000f0000000000000000000000000000000000000000000000000000000000' },
    { sym:'KGOLD',  id:'0x02000d0000000000000000000000000000000000000000000000000000000000' },
    { sym:'VXOR',   id:'0x006a271832f44c93bd8692584d85415f0f3dccef9748fecd129442c8edcb4361' },
    { sym:'KXOR',   id:'0x02000e0000000000000000000000000000000000000000000000000000000000' },
    { sym:'XST',    id:'0x0200090000000000000000000000000000000000000000000000000000000000' },
    { sym:'XSTUSD', id:'0x0200080000000000000000000000000000000000000000000000000000000000' },
    { sym:'APOLLO', id:'0x00efe45135018136733be626b380a87ae663ccf6784a25fe9d9d2be64acecb9d' },
  ];
  const PRIMARY_ASSETS = ASSETS.filter(a => a.primary);
  const EXTRA_ASSETS = ASSETS.filter(a => !a.primary);
  const assetId = ASSETS.find(a => a.sym === asset)?.id || ASSETS[0].id;
  const [extraOpen, setExtraOpen] = useState(false);

  // Uses a shared in-memory cache (TTL 5 min) + a serial fetch chain so we
  // never hit the VPS with parallel /holders calls. See startHoldersBackgroundRefresh
  // which walks the known assets in series every 5 minutes.
  useEffect(() => {
    let cancelled = false;
    window.getHoldersCached?.(assetId, page, 25).then(({ data }) => {
      if (!cancelled && data) setRaw(data);
    }).catch(() => {});
    return () => { cancelled = true; };
  }, [assetId, page]);

  // No background pre-warm: we only fetch the token the user actually clicks.
  // The cache (in common.jsx) is now persistent via localStorage with a 5-min
  // TTL, so second visits in the same session are instant even without a
  // pre-warmer. Pre-warming 15 tokens on every mount was burning bandwidth
  // on assets nobody looked at.

  useEffect(() => { setPage(1); }, [asset]);

  const holders = useMemo(() => {
    if (!raw || !raw.data) return [];
    const offset = ((raw.page || 1) - 1) * 25;
    return raw.data.map((h, i) => ({
      rank: offset + i + 1,
      name: h.name || null,
      addr: h.address,
      value: Number(h.balance) || 0,
      balanceStr: h.balanceStr,
      tokens: 0, // prod /holders only returns per-asset balance; use Tokens tab for multi-asset count
      lastActivity: null,
    }));
  }, [raw]);

  const totalHolders = raw?.totalHolders || 0;
  const totalPages = raw?.totalPages || 1;

  // Top 10 share — computed from this page's top 10 only (page 1 = real top 10)
  const top10Share = raw && page === 1 && holders.length >= 10
    ? (holders.slice(0, 10).reduce((s,h) => s + h.value, 0) / holders.reduce((s,h) => s + h.value, 0) * 100)
    : null;

  return (
    <div>
      <PageHeader title={t('holders.title')} sub={t('holders.sub')}>
        <ExportCsvButton section="holders"
          headers={['Rank','Address','Balance']}
          rows={holders.map(r => ({
            Rank: r.rank, Address: r.addr, Balance: r.balanceStr || String(r.value),
          }))}/>
        <span className="tag ok"><span className="live-dot" style={{width:5,height:5}}/> snapshot · now</span>
      </PageHeader>

      {/* Asset selector: always-visible pills for the 4 main tokens + a
          "More ▾" dropdown for everything else. The dropdown chip shows which
          extra token is currently active (if any), so the user can always see
          what they picked without reopening the menu. */}
      <div className="filter-row" style={{marginTop: 12, marginBottom: 12, position:'relative'}}>
        {PRIMARY_ASSETS.map(a => (
          <div key={a.sym}
               className={'filter-chip' + (asset === a.sym ? ' active' : '')}
               onClick={() => setAsset(a.sym)}
               style={{cursor:'pointer', fontWeight: 600, display:'flex', alignItems:'center', gap:6}}>
            <TokenBadge sym={a.sym} size={18}/>
            <span>{a.sym}</span>
          </div>
        ))}
        {(() => {
          const activeExtra = EXTRA_ASSETS.find(a => a.sym === asset);
          return (
            <div style={{position:'relative'}}>
              <div
                className={'filter-chip' + (activeExtra ? ' active' : '')}
                onClick={() => setExtraOpen(o => !o)}
                style={{cursor:'pointer', fontWeight:600, display:'flex', alignItems:'center', gap:6}}
                title="Más tokens">
                {activeExtra ? <TokenBadge sym={activeExtra.sym} size={18}/> : null}
                <span>{activeExtra ? activeExtra.sym : 'Más'}</span>
                <svg width="10" height="10" viewBox="0 0 10 10" fill="none" stroke="currentColor" strokeWidth="1.5">
                  <path d="m2 4 3 3 3-3"/>
                </svg>
              </div>
              {extraOpen && (
                <div style={{
                  position:'absolute', top:'calc(100% + 6px)', left:0, zIndex:50,
                  background:'var(--bg-card)', border:'1px solid var(--border-color)',
                  borderRadius:10, padding:6, minWidth:170,
                  boxShadow:'0 8px 24px rgba(0,0,0,0.45)',
                  display:'grid', gridTemplateColumns:'1fr 1fr', gap:4
                }}>
                  {EXTRA_ASSETS.map(a => (
                    <div key={a.sym}
                         onClick={() => { setAsset(a.sym); setExtraOpen(false); }}
                         className={asset === a.sym ? 'active' : ''}
                         style={{
                           cursor:'pointer', padding:'6px 10px', borderRadius:6,
                           display:'flex', alignItems:'center', gap:6, fontSize:13, fontWeight:600,
                           background: asset === a.sym ? 'var(--accent-soft, rgba(16,185,129,0.15))' : 'transparent'
                         }}
                         onMouseEnter={e => { if (asset !== a.sym) e.currentTarget.style.background = 'rgba(255,255,255,0.05)'; }}
                         onMouseLeave={e => { if (asset !== a.sym) e.currentTarget.style.background = 'transparent'; }}>
                      <TokenBadge sym={a.sym} size={16}/>
                      <span>{a.sym}</span>
                    </div>
                  ))}
                </div>
              )}
            </div>
          );
        })()}
      </div>

      <KpiGrid items={[
        { label: 'Total Holders', value: totalHolders ? totalHolders.toLocaleString() : '—', sub: 'for ' + asset },
        { label: 'Top 10 Share',  value: top10Share != null ? top10Share.toFixed(1) : '—', unit: top10Share != null ? '%' : '', sub: 'of page total' },
        { label: 'Pages',         value: String(totalPages), sub: '25 per page' },
        { label: 'Current Page',  value: '#' + page + ' of ' + totalPages, sub: 'paginated' },
      ]}/>

      <div className="card" style={{marginTop: 18}}>
        <div className="card-header">
          <div className="card-title" style={{display:'flex', alignItems:'center', gap:8}}>
            <span className="dot"/>
            Top holders ·
            <TokenBadge sym={asset} size={20}/>
            <span>{asset}</span>
          </div>
          <span className="tag">{totalHolders} ranked total</span>
        </div>
        <div className="swaps-table-wrap responsive-table">
          <table className="swaps-table">
            <thead>
              <tr>
                <th style={{paddingLeft: 20, width: 56}}>#</th>
                <th>{t('col.account')}</th>
                <th>On-chain Identity</th>
                <th style={{textAlign:'right', paddingRight: 20}}>Balance ({asset})</th>
              </tr>
            </thead>
            <tbody>
              {holders.length === 0 && (
                <tr><td colSpan="4" style={{padding:32, textAlign:'center', color:'var(--fg-2)'}}>Cargando holders desde prod…</td></tr>
              )}
              {holders.map(h => (
                <tr key={h.rank} className="swap-row clickable"
                    onClick={() => window.openWalletDetails?.(h.addr, window.identityName?.(h.addr) || null)}>
                  <td data-label="#" style={{paddingLeft: 20}}>
                    <span className={'rank-chip ' + (h.rank <= 3 ? 'top3' : '')}>{h.rank}</span>
                  </td>
                  <td data-label={t('col.account')}>
                    <div style={{display:'flex', alignItems:'center', gap: 10, minWidth:0}}>
                      <div style={{width:28, height:28, borderRadius:'50%', background: h.rank <= 5 ? 'linear-gradient(135deg,#9B1B30,#4A3566)' : 'linear-gradient(135deg,#7B5B90,#4A3566)', flexShrink: 0}}/>
                      <div className="num tiny" style={{overflow:'hidden', textOverflow:'ellipsis', wordBreak:'break-all'}}>{fmt.addr(h.addr, 8, 6)}</div>
                    </div>
                  </td>
                  <td data-label="Identity">
                    <IdentityCell addr={h.addr}/>
                  </td>
                  <td data-label={'Balance ' + asset} style={{textAlign:'right', paddingRight: 20}} className="num">
                    <span style={{fontWeight:700, color:'var(--fg-0)'}}>{h.balanceStr || fmt.num(h.value, 2)}</span>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
        <Pagination page={page} setPage={setPage} total={totalHolders} pageSize={25}/>
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
  const [search, setSearch] = useState('');
  const [sortKey, setSortKey] = useState('rank');
  const [sortDir, setSortDir] = useState('asc');

  // Real validators from prod /staking/validators.
  const [rawValidators, setRawValidators] = useState([]);
  const [stakingMeta, setStakingMeta] = useState(null); // { era, validatorCount }
  const [networkStats, setNetworkStats] = useState(null);
  const [recentBlocks, setRecentBlocks] = useState([]);

  useEffect(() => {
    let cancelled = false;
    const pull = async () => {
      try {
        const r = await fetch('/staking/validators');
        if (!r.ok) return;
        const j = await r.json();
        if (cancelled) return;
        setRawValidators(j.validators || []);
        setStakingMeta({ era: j.era, validatorCount: j.validatorCount, maxValidators: j.maxValidators });
      } catch {}
    };
    pull();
    const id = setInterval(pull, 60_000);
    return () => { cancelled = true; clearInterval(id); };
  }, []);

  // Network stats + recent blocks (for Network tab).
  useEffect(() => {
    if (tab !== 'network') return;
    let cancelled = false;
    const pullNet = () => fetch('/staking/network').then(r => r.ok ? r.json() : null).then(j => {
      if (!cancelled) setNetworkStats(j);
    }).catch(() => {});
    const pullBlocks = () => fetch('/staking/recent-blocks?limit=20').then(r => r.ok ? r.json() : null).then(j => {
      if (!cancelled) setRecentBlocks(Array.isArray(j) ? j : (j?.blocks || j?.data || []));
    }).catch(() => {});
    pullNet(); pullBlocks();
    const id = setInterval(() => { pullNet(); pullBlocks(); }, 15_000);
    return () => { cancelled = true; clearInterval(id); };
  }, [tab]);

  const validators = useMemo(() => {
    return rawValidators.map((v, i) => ({
      rank: i + 1,
      name: v.identity || (v.address ? v.address.slice(0, 8) + '…' + v.address.slice(-6) : 'Unknown'),
      address: v.address,
      total: Number(v.totalStake) || 0,
      own: Number(v.ownStake) || 0,
      other: Number(v.otherStake) || 0,
      nominators: Number(v.nominatorsCount) || 0,
      commission: Number(v.commission) || 0,
      points: Math.round(Number(v.erasSincePayout) * 1000) || 0,
      erasSincePayout: Number(v.erasSincePayout) || 0,
      status: v.isBlocked ? 'blocked' : 'active',
    }));
  }, [rawValidators]);

  const filtered = useMemo(() => {
    const q = search.trim().toLowerCase();
    let list = !q ? validators : validators.filter(v =>
      v.name.toLowerCase().includes(q) || (v.address || '').toLowerCase().includes(q)
    );
    if (sortKey !== 'rank') {
      list = [...list].sort((a, b) => {
        const va = a[sortKey], vb = b[sortKey];
        if (va === vb) return 0;
        const cmp = (typeof va === 'string') ? va.localeCompare(vb) : (va - vb);
        return sortDir === 'asc' ? cmp : -cmp;
      });
    } else if (sortDir === 'desc') {
      list = [...list].reverse();
    }
    return list;
  }, [validators, search, sortKey, sortDir]);

  const pageSize = tweaks.density === 'compact' ? 15 : 12;
  const visible = filtered.slice((page-1) * pageSize, page * pageSize);

  const toggleSort = (key) => {
    if (sortKey === key) setSortDir(d => d === 'asc' ? 'desc' : 'asc');
    else { setSortKey(key); setSortDir('desc'); }
    setPage(1);
  };

  // Real KPIs from the validators set.
  const totalStake = validators.reduce((s, v) => s + v.total, 0);
  const avgCommission = validators.length ? (validators.reduce((s, v) => s + v.commission, 0) / validators.length) : 0;
  const activeCount = validators.filter(v => v.status === 'active').length;
  const maxV = stakingMeta?.maxValidators;

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
            { label: t('staking.kpi.activeValidators'), value: activeCount + (maxV ? ' / ' + maxV : ''), sub: t('staking.kpi.activeSet') },
            { label: t('staking.kpi.totalStaked'),      value: fmt.num(totalStake, 0) + ' XOR', sub: 'across active set' },
            { label: t('staking.kpi.avgCommission'),    value: avgCommission.toFixed(2), unit:'%', sub: 'mean of active set' },
            { label: t('staking.kpi.era'),              value: stakingMeta?.era != null ? String(stakingMeta.era) : '—', sub: 'current era' },
          ]}/>

          <div className="card" style={{marginTop: 18}}>
            <div className="card-header">
              <div className="card-title"><span className="dot"/> {t('staking.tab.validators')}</div>
              <div style={{display:'flex', gap:8, alignItems:'center'}}>
                <input
                  type="text"
                  value={search}
                  onChange={e => { setSearch(e.target.value); setPage(1); }}
                  placeholder="Buscar validator..."
                  style={{padding:'6px 10px', border:'1px solid var(--border-color)', borderRadius:8, background:'var(--bg-card)', color:'var(--fg-0)', fontSize:13, width:200, outline:'none'}}/>
                <span className="tag">{filtered.length} validators</span>
              </div>
            </div>
            <div className="swaps-table-wrap">
              <table className="swaps-table">
                <thead>
                  <tr>
                    {[
                      { k: 'rank',       label: '#',                        w: 48,  align: 'left',  pad: 20 },
                      { k: 'name',       label: t('staking.col.validator'), align: 'left' },
                      { k: 'total',      label: t('staking.col.totalStake'), align: 'right' },
                      { k: 'own',        label: t('staking.col.own'),        align: 'right' },
                      { k: 'nominators', label: t('staking.col.noms'),       align: 'right' },
                      { k: 'commission', label: t('staking.col.commission'), align: 'right' },
                      { k: 'points',     label: t('staking.col.eraPts'),     align: 'right' },
                      { k: 'status',     label: t('staking.col.status'),     align: 'left', pad: 20 },
                    ].map(h => (
                      <th
                        key={h.k}
                        onClick={() => toggleSort(h.k)}
                        style={{
                          paddingLeft: h.pad || undefined,
                          paddingRight: (h.align === 'left' && h.pad) ? 0 : undefined,
                          textAlign: h.align,
                          width: h.w,
                          cursor: 'pointer',
                          userSelect: 'none',
                        }}
                        title="Click para ordenar">
                        {h.label}
                        {sortKey === h.k && <span style={{marginLeft: 4, opacity: 0.7}}>{sortDir === 'asc' ? '▲' : '▼'}</span>}
                      </th>
                    ))}
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
                  {visible.length === 0 && (
                    <tr><td colSpan={8} style={{padding:32, textAlign:'center', color:'var(--fg-2)'}}>
                      {search ? `Sin resultados para "${search}".` : 'Cargando validators...'}
                    </td></tr>
                  )}
                </tbody>
              </table>
            </div>
            <Pagination page={page} setPage={setPage} total={filtered.length} pageSize={pageSize}/>
          </div>
        </>
      )}

      {tab === 'network' && (
        <>
          {(() => {
            const ns = networkStats || {};
            const rows = [
              ['Total Stake', ns.totalStake != null ? fmt.num(Number(ns.totalStake), 0) + ' XOR' : '—', ns.totalStakeUsd ? fmt.usd(Number(ns.totalStakeUsd)) : ''],
              ['Active Era', ns.era != null ? String(ns.era) : '—', ns.eraStartedAgo || ''],
              ['Epoch Progress', ns.epochProgress != null ? ns.epochProgress : '—', ns.epochsPerEra ? (ns.epochsPerEra + ' epochs · ~' + (ns.epochDuration || '?') + ' each') : ''],
              ['Validators Target', ns.activeValidators != null ? (ns.activeValidators + ' active') : '—', ns.waitingValidators != null ? (ns.waitingValidators + ' waiting in queue') : ''],
              ['Min Nominator Bond', ns.minNominatorBond != null ? (fmt.num(Number(ns.minNominatorBond), 2) + ' XOR') : '—', 'hard floor'],
              ['Last Reward Era', ns.lastRewardEra != null ? String(ns.lastRewardEra) : '—', ns.lastRewardAmount != null ? (fmt.num(Number(ns.lastRewardAmount), 0) + ' XOR distributed') : ''],
              ['Ideal Stake Rate', ns.idealStakeRate != null ? (ns.idealStakeRate + '%') : '—', 'annual inflation model'],
              ['Current Inflation', ns.currentInflation != null ? (ns.currentInflation + '%') : '—', 'this era annualised'],
              ['Unbonding Period', ns.unbondingDays != null ? (ns.unbondingDays + ' days') : '—', 'withdrawal lock'],
            ];
            return (
              <div className="stake-network-grid">
                {rows.map((row, i) => (
                  <div key={i} className="stake-stat-card">
                    <div className="stat-label">{row[0]}</div>
                    <div className="stat-value num" style={{fontSize: 26}}>{row[1]}</div>
                    <div className="stat-sub">{row[2]}</div>
                  </div>
                ))}
              </div>
            );
          })()}

          <div className="card" style={{marginTop: 18}}>
            <div className="card-header">
              <div className="card-title"><span className="dot"/> Recent blocks <span className="tag ok" style={{marginLeft:8}}><span className="live-dot" style={{width:5,height:5}}/> live</span></div>
              <span className="tag">{recentBlocks.length} últimos</span>
            </div>
            <div className="swaps-table-wrap">
              <table className="swaps-table">
                <thead>
                  <tr>
                    <th style={{paddingLeft:20}}>Bloque</th>
                    <th>Hash</th>
                    <th>Validator</th>
                    <th>Hora</th>
                    <th style={{paddingRight:20, textAlign:'right'}}>Txs</th>
                  </tr>
                </thead>
                <tbody>
                  {recentBlocks.length === 0 && (
                    <tr><td colSpan={5} style={{padding:32, textAlign:'center', color:'var(--fg-2)'}}>Esperando bloques...</td></tr>
                  )}
                  {recentBlocks.map(b => {
                    const bn = b.number || b.block || b.height;
                    const hash = b.hash || '';
                    const validator = b.validator || b.author || '';
                    const ts = b.timestamp || b.time;
                    const txs = b.txCount ?? b.extrinsicsCount ?? b.txs ?? 0;
                    return (
                      <tr key={String(bn) + (hash || '')}>
                        <td style={{paddingLeft:20}}><a className="block-link num" href="#" onClick={e => e.preventDefault()}>#{Number(bn || 0).toLocaleString()}</a></td>
                        <td><span className="num tiny muted">{hash ? (hash.slice(0, 10) + '…' + hash.slice(-6)) : '—'}</span></td>
                        <td>{validator ? (IDENTITIES[validator] || fmt.addr(validator, 5, 4)) : '—'}</td>
                        <td><span className="muted tiny" title={ts ? fmt.fullDate(Number(ts)) : ''}>{ts ? fmt.ago(Number(ts)) : '—'}</span></td>
                        <td style={{paddingRight:20, textAlign:'right'}} className="num">{txs}</td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            </div>
          </div>
        </>
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
  // Shape: { hash, index, decoded, voting:{threshold,ayes:[addr],nays:[addr],end},
  //          blocksRemaining, timeRemaining }
  // The old code ignored `voting` entirely and displayed 0/0 for every row
  // even when aye/nay addresses were clearly populated (e.g. motion #947
  // had 3 ayes, 0 nays, threshold 5 — app still showed "0 AYE 0 NAY").
  const motions = useMemo(() => {
    if (rawMotions.length === 0) {
      // Show empty-state instead of mocks — prod has no active motions sometimes.
      return [];
    }
    return rawMotions.slice(0, 8).map((m, i) => {
      const v = m.voting || {};
      const ayeCount = Array.isArray(v.ayes) ? v.ayes.length : 0;
      const nayCount = Array.isArray(v.nays) ? v.nays.length : 0;
      const threshold = Number(v.threshold) > 0 ? Number(v.threshold) : null;
      const blocksLeft = Number(m.blocksRemaining);
      // A motion that has met threshold OR whose voting window expired is
      // effectively closed, even if the node hasn't pruned it yet.
      const expired = Number.isFinite(blocksLeft) && blocksLeft <= 0;
      const passed = threshold != null && ayeCount >= threshold;
      const rejected = threshold != null && nayCount >= threshold;
      const status = passed ? 'passed' : rejected ? 'failed' : expired ? 'failed' : 'open';
      return {
        id: m.index != null ? m.index : i,
        title: (m.decoded && m.decoded.description) || (m.decoded && (m.decoded.section + '::' + m.decoded.method)) || 'Unknown motion',
        proposer: '—', // still not exposed by /motions
        threshold: threshold != null ? (ayeCount + '/' + threshold) : '—',
        votes: { aye: ayeCount, nay: nayCount },
        deadline: m.timeRemaining || (v.end ? ('block #' + v.end) : 'pending'),
        status,
        hash: m.hash,
      };
    });
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
        { id:'consejo',    label: t('gov.tab.council',    'Consejo') },
        { id:'elecciones', label: t('gov.tab.elections',  'Elecciones') },
        { id:'mociones',   label: t('gov.tab.motions',    'Mociones') },
        { id:'democracia', label: t('gov.tab.democracy',  'Democracia') },
        { id:'tecnico',    label: t('gov.tab.technical',  'Comité Técnico') },
      ]} current={tab} onChange={setTab}/>

      {tab === 'consejo' && (
        <div className="card" style={{marginTop: 18}}>
          <div className="card-header">
            <div className="card-title"><span className="dot"/> {t('gov.council.members', 'Miembros del Consejo')} · {council.length}</div>
            <span className="tag">{t('gov.council.seatsFilled', '{n} asientos ocupados').replace('{n}', String(council.length))}</span>
          </div>
          <table className="swaps-table">
            <thead><tr>
              <th style={{paddingLeft:20}}>{t('gov.col.member')}</th>
              <th style={{textAlign:'right'}}>{t('gov.col.joined', 'Se unió (era)')}</th>
              <th style={{textAlign:'right', paddingRight:20}}>{t('gov.col.votesCast', 'Votos emitidos')}</th>
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
            <div className="card-header"><div className="card-title"><span className="dot"/> {t('gov.elections.candidates', 'Candidatos actuales')}</div><span className="tag">{elections.candidates.length}</span></div>
            <div style={{padding: 12}}>
              {elections.candidates.map((c, i) => (
                <div key={i} className="elec-row">
                  <div style={{width:24, height:24, borderRadius:6, background:'linear-gradient(135deg,#9B1B30,#4A3566)'}}/>
                  <div style={{flex:1, minWidth:0}}>
                    <div style={{fontWeight:700, fontSize:13}}>{c.name}</div>
                    <div className="muted tiny">{t('gov.elections.bond', 'Fianza')} · {c.bond} XOR</div>
                  </div>
                  <div className="elec-bar"><div className="elec-bar-fill" style={{width: (c.votes / 48200 * 100) + '%'}}/></div>
                  <div className="num" style={{fontWeight:700, minWidth: 70, textAlign:'right'}}>{fmt.num(c.votes, 1)}</div>
                </div>
              ))}
            </div>
          </div>
          <div className="card">
            <div className="card-header"><div className="card-title"><span className="dot"/> {t('gov.elections.runnersUp', 'Suplentes')}</div></div>
            <div style={{padding: 12}}>
              {elections.runnersUp.map((r, i) => (
                <div key={i} className="elec-row">
                  <div style={{width: 24, height: 24, borderRadius: 6, background:'linear-gradient(135deg,#7B5B90,#4A3566)'}}/>
                  <div style={{flex:1, fontWeight:700, fontSize: 13}}>{r.name}</div>
                  <div className="num muted">{fmt.num(r.votes, 1)}</div>
                </div>
              ))}
              <div style={{marginTop: 16, padding: 12, background:'rgba(255,255,255,0.02)', borderRadius:8, fontSize: 12, color:'var(--fg-2)'}}>
                <strong>{elections.filled} / {elections.seats}</strong> {t('gov.elections.seatsCovered', 'asientos cubiertos · siguiente votación en')} <strong>3 {t('gov.elections.eras', 'eras')}</strong>.
              </div>
            </div>
          </div>
        </div>
      )}

      {tab === 'mociones' && (
        <div className="card" style={{marginTop: 18}}>
          <div className="card-header"><div className="card-title"><span className="dot"/> {t('gov.motions.title', 'Mociones activas y recientes')}</div></div>
          <div className="motions-list">
            {motions.map(m => (
              <div key={m.id} className="motion-card">
                <div style={{display:'flex', alignItems:'center', gap:12, marginBottom: 8}}>
                  <span className="motion-id">#{m.id}</span>
                  <span className={'br-status ' + (m.status === 'open' ? 'pending' : m.status === 'passed' ? 'done' : 'failed')}>
                    {m.status === 'open' ? ('⏳ ' + t('gov.status.open', 'Abierta')) : m.status === 'passed' ? ('✓ ' + t('gov.status.passed', 'Aprobada')) : ('✗ ' + t('gov.status.rejected', 'Rechazada'))}
                  </span>
                  <span className="muted tiny" style={{marginLeft:'auto'}}>{t('gov.col.threshold')} · {m.threshold} · {m.deadline}</span>
                </div>
                <div style={{fontSize:15, fontWeight:700, color:'var(--fg-0)', marginBottom: 10}}>{m.title}</div>
                <div className="muted tiny">{t('gov.motions.proposedBy', 'Propuesta por')} <strong>{m.proposer}</strong></div>
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
            <div className="card-header"><div className="card-title"><span className="dot"/> {t('gov.democracy.referendums', 'Referéndums en curso')}</div><span className="tag">{democracy.referendums.length}</span></div>
            <div className="motions-list">
              {democracy.referendums.length === 0 && (
                <div className="muted tiny" style={{padding:'12px 16px'}}>{t('gov.democracy.noReferendums', 'No hay referéndums activos.')}</div>
              )}
              {democracy.referendums.map(r => (
                <div key={r.id} className="motion-card">
                  <div style={{display:'flex', alignItems:'center', gap:12, marginBottom: 8}}>
                    <span className="motion-id">#{r.id}</span>
                    <span className="tag">{t('gov.democracy.endsIn', 'Termina en')} {r.ends}</span>
                    <span className="muted tiny" style={{marginLeft:'auto'}}>{t('gov.democracy.turnout', 'Participación')} · {r.turnout}%</span>
                  </div>
                  <div style={{fontSize:15, fontWeight:700, marginBottom: 10}}>{r.title}</div>
                  <div className="vote-bar">
                    <div className="vote-aye" style={{flex: r.aye || 0.5}}>✓ {r.aye}% AYE</div>
                    <div className="vote-nay" style={{flex: r.nay || 0.5}}>✗ {r.nay}% NAY</div>
                  </div>
                </div>
              ))}
            </div>
          </div>

          <div className="card" style={{marginTop: 18}}>
            <div className="card-header"><div className="card-title"><span className="dot"/> {t('gov.democracy.publicProposals', 'Propuestas públicas')}</div></div>
            <table className="swaps-table">
              <thead><tr>
                <th style={{paddingLeft:20}}>ID</th>
                <th>{t('gov.col.title', 'Título')}</th>
                <th style={{textAlign:'right'}}>{t('gov.col.seconds', 'Respaldos')}</th>
                <th style={{textAlign:'right', paddingRight:20}}>{t('gov.col.deposit', 'Depósito')}</th>
              </tr></thead>
              <tbody>
                {democracy.proposals.length === 0 && (
                  <tr><td colSpan={4} className="muted tiny" style={{padding:14, textAlign:'center'}}>{t('gov.democracy.noProposals', 'Sin propuestas públicas activas.')}</td></tr>
                )}
                {democracy.proposals.map(p => (
                  <tr key={p.id}>
                    <td style={{paddingLeft:20, fontWeight:700, color:'var(--accent)'}} className="num">{p.id}</td>
                    <td>{p.title}</td>
                    <td style={{textAlign:'right'}} className="num">{p.seconds}</td>
                    <td style={{textAlign:'right', paddingRight:20}} className="num">{p.deposit} XOR</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>

          {/* Preimages — replicates v1's modern preimages explorer (Decode + History modals). */}
          <PreimagesPanel/>

          {/* Scheduler agenda — replicates v1's monitor with red alerts when bytes are missing. */}
          <SchedulerAgendaPanel/>
        </div>
      )}

      {tab === 'tecnico' && (
        <div className="gov-elections-grid" style={{marginTop: 18}}>
          <div className="card">
            <div className="card-header"><div className="card-title"><span className="dot"/> {t('gov.tech.members', 'Miembros del Comité Técnico')}</div></div>
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
            <div className="card-header"><div className="card-title"><span className="dot"/> {t('gov.tech.motions', 'Mociones técnicas')}</div></div>
            <div className="motions-list">
              {tech.motions.map(m => (
                <div key={m.id} className="motion-card">
                  <div style={{display:'flex', alignItems:'center', gap:12, marginBottom: 8}}>
                    <span className="motion-id">#{m.id}</span>
                    <span className={'br-status ' + (m.status === 'open' ? 'pending' : 'done')}>
                      {m.status === 'open' ? ('⏳ ' + t('gov.status.open', 'Abierta')) : ('✓ ' + t('gov.status.passed', 'Aprobada'))}
                    </span>
                    <span className="muted tiny" style={{marginLeft:'auto'}}>{t('gov.col.threshold')} · {m.threshold}</span>
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
   PREIMAGES PANEL — modern explorer (Decode + History modals)
   ========================================================================= */

// Lists every preimage currently in storage (Requested + Unrequested).
// Each row exposes Decode (call decode) + History (events timeline via
// dedicated indexer + RPC fallback). Same UX we shipped on v1 today.
function PreimagesPanel() {
  const t = useT();
  const [data, setData] = useState({ preimages: [], identities: {} });
  // Pagination — keeps the panel a fixed height so the next-up Scheduler card
  // stays visible without long scroll. 10 rows per page is enough to scan
  // recently-noted preimages without overwhelming the layout.
  const PAGE_SIZE = 10;
  const [page, setPage] = useState(0);
  const [loading, setLoading] = useState(true);
  const [filter, setFilter] = useState('');
  const [decodeModal, setDecodeModal] = useState(null); // { hash, len }
  const [historyModal, setHistoryModal] = useState(null); // { hash }
  const [refModal, setRefModal] = useState(null); // { id }
  // Lazy-filled enrichment maps (call decoded, linked referendum, first-seen block).
  const [decodes, setDecodes] = useState({}); // hash -> { section, method }
  const [links, setLinks] = useState({});     // hash -> { refId } | 'none'
  const [firstSeen, setFirstSeen] = useState({}); // hash -> { block, timestamp }

  useEffect(() => {
    let cancelled = false;
    const pull = () => fetch('/governance/preimages').then(r => r.ok ? r.json() : null)
      .then(j => { if (!cancelled && j) { setData(j); setLoading(false); } })
      .catch(() => { if (!cancelled) setLoading(false); });
    pull();
    const id = setInterval(pull, 60_000);
    return () => { cancelled = true; clearInterval(id); };
  }, []);

  const items = useMemo(() => {
    const f = filter.trim().toLowerCase();
    const filtered = (data.preimages || []).filter(p => !f
      || p.hash.toLowerCase().includes(f)
      || (p.depositor || '').toLowerCase().includes(f)
    );
    // Most recent first — by firstSeen.timestamp when known, otherwise by block.
    // Unknown timestamps settle at the end so the newest items we've resolved
    // bubble up as the background enrichment fills the map in.
    return [...filtered].sort((a, b) => {
      const ta = firstSeen[a.hash]?.timestamp ? Number(firstSeen[a.hash].timestamp) : 0;
      const tb = firstSeen[b.hash]?.timestamp ? Number(firstSeen[b.hash].timestamp) : 0;
      if (ta !== tb) return tb - ta;
      const ba = firstSeen[a.hash]?.block || 0;
      const bb = firstSeen[b.hash]?.block || 0;
      return bb - ba;
    });
  }, [data.preimages, filter, firstSeen]);

  // Reset page on filter change. Also clamp page if items shrink below current page.
  useEffect(() => { setPage(0); }, [filter]);
  const pageCount = Math.max(1, Math.ceil(items.length / PAGE_SIZE));
  const safePage = Math.min(page, pageCount - 1);
  const pagedItems = items.slice(safePage * PAGE_SIZE, (safePage + 1) * PAGE_SIZE);

  // Enrich visible rows: fetch decode + linked-referendum + first-seen in parallel.
  // Fires once per hash per session; cheap fetches cached in state.
  useEffect(() => {
    let cancelled = false;
    const pending = items.filter(p => decodes[p.hash] === undefined).slice(0, 8);
    pending.forEach(p => {
      fetch('/governance/preimage/' + encodeURIComponent(p.hash) + '?len=' + encodeURIComponent(p.len || 0))
        .then(r => r.ok ? r.json() : null)
        .then(j => { if (!cancelled) setDecodes(d => ({ ...d, [p.hash]: j?.decoded ? { section: j.decoded.section, method: j.decoded.method } : null })); })
        .catch(() => { if (!cancelled) setDecodes(d => ({ ...d, [p.hash]: null })); });
    });
    const pendingLinks = items.filter(p => links[p.hash] === undefined).slice(0, 8);
    pendingLinks.forEach(p => {
      fetch('/governance/preimage/' + encodeURIComponent(p.hash) + '/referendums?limit=100')
        .then(r => r.ok ? r.json() : null)
        .then(j => {
          if (cancelled) return;
          const match = j?.matches?.[0];
          setLinks(l => ({ ...l, [p.hash]: match ? { refId: match.id, status: match.status } : 'none' }));
        })
        .catch(() => { if (!cancelled) setLinks(l => ({ ...l, [p.hash]: 'none' })); });
    });
    // Bigger batch for first-seen — needed so the chronological sort has enough
    // data to actually order the visible rows, not just the first 8.
    const pendingFirst = items.filter(p => firstSeen[p.hash] === undefined).slice(0, 40);
    pendingFirst.forEach(p => {
      fetch('/governance/preimage/' + encodeURIComponent(p.hash) + '/events-fast')
        .then(r => r.ok ? r.json() : null)
        .then(j => {
          if (cancelled) return;
          const first = (j?.events || [])[0];
          setFirstSeen(fs => ({ ...fs, [p.hash]: first ? { block: first.block, timestamp: first.timestamp } : null }));
        })
        .catch(() => { if (!cancelled) setFirstSeen(fs => ({ ...fs, [p.hash]: null })); });
    });
    return () => { cancelled = true; };
  }, [items]);

  const fmtDeposit = (raw) => {
    if (!raw) return '-';
    try { return (Number(BigInt(raw)) / 1e18).toFixed(4); } catch { return String(raw); }
  };

  return (
    <div className="card" style={{marginTop: 18}}>
      <div className="card-header">
        <div className="card-title"><span className="dot"/> {t('gov.preimages.title', 'Preimágenes')}</div>
        <span className="tag">{items.length} / {data.preimages?.length || 0}</span>
      </div>
      <div style={{padding: '0 14px 14px'}}>
        <input
          type="text"
          placeholder={t('gov.preimages.filterPlaceholder', 'Filtrar por hash o depositante...')}
          value={filter}
          onChange={e => setFilter(e.target.value)}
          style={{width:'100%', padding:'8px 10px', border:'1px solid var(--border-color)', borderRadius:8, background:'var(--bg-card)', color:'var(--fg-0)', fontSize:13, outline:'none'}}/>
      </div>
      <div className="swaps-table-wrap responsive-table">
        <table className="swaps-table">
          <thead>
            <tr>
              <th style={{paddingLeft:20}}>Hash</th>
              <th>Action</th>
              <th>Status</th>
              <th style={{textAlign:'right'}}>{t('gov.preimages.size', 'Tamaño')}</th>
              <th>Author</th>
              <th style={{textAlign:'right'}}>{t('gov.preimages.deposit', 'Depósito')}</th>
              <th>{t('gov.preimages.published', 'Publicada')}</th>
              <th>Ref.</th>
              <th style={{paddingRight:20}}></th>
            </tr>
          </thead>
          <tbody>
            {loading && (
              <tr><td colSpan={9} style={{padding:24, textAlign:'center', color:'var(--fg-2)'}}>{t('gov.preimages.loading', 'Cargando preimágenes...')}</td></tr>
            )}
            {!loading && items.length === 0 && (
              <tr><td colSpan={9} style={{padding:24, textAlign:'center', color:'var(--fg-2)'}}>{t('gov.preimages.empty', 'Sin preimágenes que mostrar.')}</td></tr>
            )}
            {pagedItems.map(p => {
              const short = p.hash.slice(0, 10) + '…' + p.hash.slice(-6);
              const depLabel = p.depositor ? (data.identities?.[p.depositor] || fmt.addr(p.depositor, 6, 4)) : '—';
              const statusColor = p.status === 'Requested' ? '#10b981' : 'var(--fg-2)';
              const decoded = decodes[p.hash];
              const link = links[p.hash];
              const first = firstSeen[p.hash];
              return (
                <tr key={p.hash}>
                  <td data-label="Hash" style={{paddingLeft:20, fontFamily:'monospace'}} title={p.hash}>
                    <span style={{cursor:'pointer'}} onClick={() => navigator.clipboard?.writeText(p.hash)}>{short}</span>
                  </td>
                  <td data-label="Action">
                    {decoded === undefined && <span className="muted tiny">…</span>}
                    {decoded === null && <span className="muted tiny">—</span>}
                    {decoded && <span style={{fontWeight:600}}>{decoded.section}.<span style={{color:'var(--accent)'}}>{decoded.method}</span></span>}
                  </td>
                  <td data-label="Status"><span style={{color: statusColor, fontWeight:600}}>{p.status}</span></td>
                  <td data-label="Tamaño" style={{textAlign:'right'}} className="num">{p.len ?? '—'}</td>
                  <td data-label="Author" title={p.depositor || ''}>{depLabel}</td>
                  <td data-label="Depósito" style={{textAlign:'right'}} className="num">{fmtDeposit(p.deposit)}</td>
                  <td data-label="Publicada">
                    {first === undefined && <span className="muted tiny">…</span>}
                    {first === null && <span className="muted tiny">—</span>}
                    {first && (
                      <div className="muted tiny" style={{lineHeight:1.35}}>
                        {first.timestamp && (
                          <div title={fmt.fullDate(first.timestamp)}>
                            {fmt.fullDate(first.timestamp)}
                          </div>
                        )}
                        <div style={{opacity:0.7}}>#{first.block}{first.timestamp && <> · {fmt.ago(first.timestamp)}</>}</div>
                      </div>
                    )}
                  </td>
                  <td data-label="Ref.">
                    {link === undefined && <span className="muted tiny">…</span>}
                    {link === 'none' && <span className="muted tiny">—</span>}
                    {link && link !== 'none' && (
                      <span
                        style={{cursor:'pointer', color:'var(--accent)', textDecoration:'underline dotted'}}
                        onClick={() => setRefModal({ id: link.refId })}
                        title="Abrir detalle del referendum">
                        Ref #{link.refId}
                      </span>
                    )}
                  </td>
                  <td style={{paddingRight:20, whiteSpace:'nowrap'}}>
                    <button className="btn" style={{padding:'4px 10px', marginRight:4}} onClick={(e) => { e.stopPropagation(); console.log('[Preimages] open decode modal', p.hash, p.len); setDecodeModal({ hash: p.hash, len: p.len || 0 }); }}>Decode</button>
                    <button className="btn" style={{padding:'4px 10px'}} onClick={(e) => { e.stopPropagation(); console.log('[Preimages] open history modal', p.hash); setHistoryModal({ hash: p.hash }); }}>{t('gov.preimages.history', 'Historial')}</button>
                  </td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
      {pageCount > 1 && (
        <div style={{display:"flex", alignItems:"center", justifyContent:"center", gap:8, padding:"10px 14px 14px"}}>
          <button className="btn" disabled={safePage === 0}
            onClick={() => setPage(p => Math.max(0, p - 1))}
            style={{padding:"4px 12px", opacity: safePage === 0 ? 0.4 : 1}}>
            ← {t('common.prev', 'Prev')}
          </button>
          <span className="muted tiny" style={{minWidth:90, textAlign:"center"}}>
            {t('common.pageOf', 'Page {n} of {total}').replace('{n}', String(safePage + 1)).replace('{total}', String(pageCount))}
          </span>
          <button className="btn" disabled={safePage >= pageCount - 1}
            onClick={() => setPage(p => Math.min(pageCount - 1, p + 1))}
            style={{padding:"4px 12px", opacity: safePage >= pageCount - 1 ? 0.4 : 1}}>
            {t('common.next', 'Next')} →
          </button>
        </div>
      )}
      {decodeModal && <PreimageDecodeModal hash={decodeModal.hash} len={decodeModal.len} onClose={() => setDecodeModal(null)}/>}
      {historyModal && <PreimageHistoryModal hash={historyModal.hash} onClose={() => setHistoryModal(null)}/>}
      {refModal && <ReferendumDetailModal refId={refModal.id} onClose={() => setRefModal(null)}/>}
    </div>
  );
}

// Subscan-style referendum detail modal with tabs:
//   · Overview  — proposer, origin, status, voting stats
//   · Timeline  — Submitted / Started / Passed/Rejected / Executed events
//   · Proposal Preimage — created at, author, module, call, deposit, params
function ReferendumDetailModal({ refId, onClose }) {
  const t = useT();
  const [tab, setTab] = useState('overview');
  const [democracy, setDemocracy] = useState(null);
  const [preimage, setPreimage] = useState(null);
  const [preimageHistory, setPreimageHistory] = useState(null);

  useEffect(() => {
    let cancelled = false;
    fetch('/governance/democracy').then(r => r.ok ? r.json() : null).then(j => {
      if (cancelled) return;
      const ref = (j?.referendums || []).find(r => r.id === refId);
      setDemocracy(ref || null);
      // Fetch the proposal preimage decoded + history.
      const prop = ref?.detail?.proposal;
      const lookup = prop && (prop.lookup || prop.Lookup);
      const lookupHash = lookup ? (lookup.hash_ || lookup.hash) : (typeof prop === 'string' ? prop : null);
      const lookupLen = lookup ? (lookup.len || 0) : 0;
      if (lookupHash) {
        fetch('/governance/preimage/' + encodeURIComponent(lookupHash) + '?len=' + lookupLen)
          .then(r => r.ok ? r.json() : null)
          .then(j2 => { if (!cancelled) setPreimage({ hash: lookupHash, len: lookupLen, decoded: j2?.decoded }); })
          .catch(() => {});
        fetch('/governance/preimage/' + encodeURIComponent(lookupHash) + '/events-fast')
          .then(r => r.ok ? r.json() : null)
          .then(j3 => { if (!cancelled) setPreimageHistory(j3); })
          .catch(() => {});
      }
    }).catch(() => {});
    return () => { cancelled = true; };
  }, [refId]);

  const ref = democracy;
  const tally = ref?.detail?.tally || {};
  const ayes = Number(tally.ayes) || 0;
  const nays = Number(tally.nays) || 0;
  const total = ayes + nays || 1;

  return (
    <div onClick={onClose} style={{position:'fixed', inset:0, background:'rgba(0,0,0,0.6)', zIndex:10000, display:'flex', alignItems:'center', justifyContent:'center', padding:20}}>
      <div onClick={e => e.stopPropagation()} style={{background:'var(--bg-card)', color:'var(--fg-0)', borderRadius:12, maxWidth:880, width:'100%', maxHeight:'92vh', overflow:'auto', padding:22, border:'1px solid var(--border-color)'}}>
        <div style={{display:'flex', justifyContent:'space-between', alignItems:'flex-start', marginBottom:14, gap:10, flexWrap:'wrap'}}>
          <div>
            <h3 style={{margin:0, fontSize:18}}>Referendum #{refId}</h3>
            <div className="muted tiny" style={{marginTop:4}}>
              {ref ? (ref.status + (ref.timeRemaining ? ' · Termina en ' + ref.timeRemaining : '')) : 'Cargando...'}
            </div>
          </div>
          <button className="btn" onClick={onClose}>Cerrar</button>
        </div>

        <div style={{display:'flex', gap:4, borderBottom:'1px solid var(--border-color)', marginBottom:14, flexWrap:'wrap'}}>
          {['overview', 'timeline', 'preimage'].map(k => (
            <button
              key={k}
              onClick={() => setTab(k)}
              style={{
                padding:'8px 14px', background:'transparent', border:'none',
                color: tab === k ? 'var(--fg-0)' : 'var(--fg-2)',
                borderBottom: tab === k ? '2px solid var(--accent, #10b981)' : '2px solid transparent',
                cursor:'pointer', fontSize:14,
              }}>
              {k === 'overview' ? 'Overview' : k === 'timeline' ? 'Timeline' : 'Proposal Preimage'}
            </button>
          ))}
        </div>

        {tab === 'overview' && (
          <div>
            {!ref ? <div className="muted" style={{padding:20, textAlign:'center'}}>Cargando...</div> : (
              <div>
                <div style={{background:'rgba(255,255,255,0.03)', border:'1px solid var(--border-color)', borderRadius:8, padding:14, marginBottom:12}}>
                  <div style={{display:'grid', gridTemplateColumns:'max-content 1fr', gap:'6px 16px', fontSize:13}}>
                    <div className="muted">Status</div><div>{ref.status}</div>
                    <div className="muted">Ends in</div><div>{ref.timeRemaining || '—'}</div>
                    <div className="muted">End block</div><div className="num">{ref.detail?.end || '—'}</div>
                    <div className="muted">Threshold</div><div>{ref.detail?.threshold || '—'}</div>
                  </div>
                </div>
                <div style={{padding:14, border:'1px solid var(--border-color)', borderRadius:8}}>
                  <div className="muted tiny" style={{marginBottom:6}}>Voting</div>
                  <div className="vote-bar" style={{height: 28}}>
                    <div className="vote-aye" style={{flex: ayes / total}}>✓ AYE · {fmt.num(ayes / 1e18, 2)}</div>
                    <div className="vote-nay" style={{flex: nays / total}}>✗ NAY · {fmt.num(nays / 1e18, 2)}</div>
                  </div>
                  <div className="muted tiny" style={{marginTop:8}}>
                    Participación · {fmt.num(Number(tally.turnout || 0) / 1e18, 2)}
                  </div>
                </div>
              </div>
            )}
          </div>
        )}

        {tab === 'timeline' && (
          <div>
            {!preimageHistory ? <div className="muted" style={{padding:20, textAlign:'center'}}>{t('preimage.history.loadingTimeline')}</div> : (() => {
              const events = (preimageHistory.events || []).slice().sort((a, b) => a.block - b.block);
              if (events.length === 0) return <div className="muted" style={{padding:20, textAlign:'center'}}>Sin eventos indexados para este referendum.</div>;
              return (
                <div style={{position:'relative', paddingLeft:20}}>
                  <div style={{position:'absolute', top:4, bottom:4, left:6, width:2, background:'var(--border-color)'}}/>
                  {events.map((e, i) => {
                    const color = e.event.endsWith('.Noted') ? '#10b981' : e.event.endsWith('.Requested') ? '#f59e0b' : '#ef4444';
                    return (
                      <div key={i} style={{position:'relative', paddingBottom:14}}>
                        <div style={{position:'absolute', left:-20, top:4, width:14, height:14, borderRadius:'50%', background:color}}/>
                        <div style={{fontWeight:600, color}}>{e.event}</div>
                        <div className="muted tiny" style={{marginTop:2}}>
                          block <span className="num">{e.block}</span>
                          {e.timestamp && <> · {new Date(e.timestamp).toLocaleString()}</>}
                          {e.reason && <> · <span style={{fontStyle:'italic'}}>{e.reason}</span></>}
                        </div>
                      </div>
                    );
                  })}
                </div>
              );
            })()}
          </div>
        )}

        {tab === 'preimage' && (
          <div>
            {!preimage ? <div className="muted" style={{padding:20, textAlign:'center'}}>{t('preimage.decode.decoding')}</div> : (
              <div>
                <div style={{background:'rgba(255,255,255,0.03)', border:'1px solid var(--border-color)', borderRadius:8, padding:14, marginBottom:12}}>
                  <div style={{display:'grid', gridTemplateColumns:'max-content 1fr', gap:'6px 16px', fontSize:13}}>
                    <div className="muted">Hash</div><div className="num tiny" style={{wordBreak:'break-all'}}>{preimage.hash}</div>
                    <div className="muted">Bytes len</div><div>{preimage.len || '—'}</div>
                    {preimage.decoded && <>
                      <div className="muted">Module</div><div>{preimage.decoded.section}</div>
                      <div className="muted">Call</div><div style={{color:'var(--accent)', fontWeight:600}}>{preimage.decoded.method}</div>
                    </>}
                  </div>
                </div>
                {preimage.decoded?.args && (
                  <>
                    <div className="muted tiny" style={{marginBottom:6}}>Parameters</div>
                    <pre style={{margin:0, padding:12, background:'rgba(0,0,0,0.3)', borderRadius:8, overflow:'auto', maxHeight:'50vh', fontSize:12}}>{JSON.stringify(preimage.decoded.args, null, 2)}</pre>
                  </>
                )}
                {!preimage.decoded && (
                  <div className="muted" style={{padding:14, textAlign:'center'}}>No se pudo decodificar (bytes no disponibles en storage).</div>
                )}
              </div>
            )}
          </div>
        )}
      </div>
    </div>
  );
}

// Known SORA asset IDs → symbol + decimals. Keeps the decode modal self-contained
// so we don't have to round-trip to /tokens just to render a recognisable symbol.
// Source: the same list v1 shipped; XSTUSD/KUSD/VAL/PSWAP/XOR are the ones that
// actually appear in preimages today. Unknown IDs fall through to a shortened hex.
const KNOWN_ASSETS = {
  '0x0200000000000000000000000000000000000000000000000000000000000000': { sym: 'XOR',    decimals: 18 },
  '0x0200040000000000000000000000000000000000000000000000000000000000': { sym: 'VAL',    decimals: 18 },
  '0x0200050000000000000000000000000000000000000000000000000000000000': { sym: 'PSWAP',  decimals: 18 },
  '0x0200060000000000000000000000000000000000000000000000000000000000': { sym: 'DAI',    decimals: 18 },
  '0x0200070000000000000000000000000000000000000000000000000000000000': { sym: 'ETH',    decimals: 18 },
  '0x0200080000000000000000000000000000000000000000000000000000000000': { sym: 'XSTUSD', decimals: 18 },
  '0x0200090000000000000000000000000000000000000000000000000000000000': { sym: 'XST',    decimals: 18 },
  '0x02000c0000000000000000000000000000000000000000000000000000000000': { sym: 'KUSD',   decimals: 18 },
  '0x02000d0000000000000000000000000000000000000000000000000000000000': { sym: 'TBCD',   decimals: 18 },
  '0x0200180000000000000000000000000000000000000000000000000000000000': { sym: 'KXOR',   decimals: 18 },
};

function looksLikeAssetId(v) {
  return typeof v === 'string' && /^0x02[0-9a-f]{62}$/i.test(v);
}
function looksLikeSoraAddress(v) {
  // SORA SS58 addresses start with "cn" and are 47-49 chars long.
  return typeof v === 'string' && /^cn[1-9A-HJ-NP-Za-km-z]{45,49}$/.test(v);
}
function looksLikeBalanceField(key) {
  if (!key) return false;
  const k = String(key).toLowerCase();
  return /amount|value|balance|deposit|fee|price|limit|liquidity/.test(k);
}
function formatBalance(raw, decimals = 18) {
  // Balances arrive as strings ("123456789") or already-formatted ("1,234 XOR").
  // Only transform pure numeric strings; leave anything else untouched.
  if (raw == null) return '—';
  const s = String(raw).replace(/,/g, '');
  if (!/^-?\d+$/.test(s)) return String(raw);
  try {
    const big = BigInt(s);
    const divisor = BigInt(10) ** BigInt(decimals);
    const whole = big / divisor;
    const frac = big % divisor;
    const fracStr = frac.toString().padStart(decimals, '0').slice(0, 4).replace(/0+$/, '');
    return fracStr ? `${whole.toString()}.${fracStr}` : whole.toString();
  } catch { return String(raw); }
}

// Shared in-memory cache for /identity/:addr lookups. Survives re-renders of
// the decode modal; cleared on page reload. Avoids re-fetching the same address
// every time a modal opens.
const identityCache = new Map();
function useResolveIdentities(addresses) {
  const [resolved, setResolved] = useState(() => {
    const out = {};
    for (const a of addresses) if (identityCache.has(a)) out[a] = identityCache.get(a);
    return out;
  });
  useEffect(() => {
    let cancelled = false;
    const missing = addresses.filter(a => !identityCache.has(a));
    if (missing.length === 0) return;
    Promise.all(missing.map(addr =>
      fetch('/identity/' + encodeURIComponent(addr))
        .then(r => r.ok ? r.json() : null)
        .then(j => ({ addr, display: j?.display || null }))
        .catch(() => ({ addr, display: null }))
    )).then(results => {
      if (cancelled) return;
      for (const { addr, display } of results) identityCache.set(addr, display);
      setResolved(prev => {
        const next = { ...prev };
        for (const { addr, display } of results) next[addr] = display;
        return next;
      });
    });
    return () => { cancelled = true; };
  }, [addresses.join('|')]);
  return resolved;
}

// Substrate runtime WASM is always zstd-compressed and prefixed with this 8-byte
// magic (hex 52bc537646db8e05). If we see it in args.code of a system.setCode
// call, we know this preimage is a runtime upgrade and we can render it as a
// human card instead of dumping 5.9M chars of hex.
const SUBSTRATE_ZSTD_MAGIC_HEX = '52bc537646db8e05';

function isRuntimeUpgradeCall(decoded) {
  if (!decoded || decoded.section !== 'system') return false;
  if (decoded.method !== 'setCode' && decoded.method !== 'setCodeWithoutChecks') return false;
  const code = decoded.args?.code;
  if (typeof code !== 'string' || code.length < 20) return false;
  const head = code.startsWith('0x') ? code.slice(2, 18) : code.slice(0, 16);
  return head.toLowerCase() === SUBSTRATE_ZSTD_MAGIC_HEX;
}

function hexByteLength(hex) {
  if (typeof hex !== 'string') return 0;
  return Math.floor((hex.startsWith('0x') ? hex.length - 2 : hex.length) / 2);
}

function formatBytes(n) {
  if (!n) return '—';
  if (n < 1024) return n + ' B';
  if (n < 1024 * 1024) return (n / 1024).toFixed(1) + ' KB';
  return (n / (1024 * 1024)).toFixed(2) + ' MB';
}

// Convert a 0x-prefixed hex string to a Uint8Array. Used for the "download raw
// bytes" button so the user gets a .wasm.zst file they can feed to `zstd -d`.
function hexToBytes(hex) {
  const clean = hex.startsWith('0x') ? hex.slice(2) : hex;
  const out = new Uint8Array(clean.length / 2);
  for (let i = 0; i < out.length; i++) out[i] = parseInt(clean.substr(i * 2, 2), 16);
  return out;
}

function triggerDownload(filename, blob) {
  const url = URL.createObjectURL(blob);
  const a = document.createElement('a');
  a.href = url; a.download = filename;
  document.body.appendChild(a); a.click();
  document.body.removeChild(a);
  setTimeout(() => URL.revokeObjectURL(url), 1000);
}

// Friendly, per-call-type render. Covers the runtime-upgrade case explicitly
// (because that's the one that drives a 6 MB hex blob) and falls back to a
// shallow key/value dump with hex fields collapsed for everything else.
function HumanDecodeView({ decoded, pretty }) {
  const t = useT();
  const runtimeUpgrade = isRuntimeUpgradeCall(decoded);

  if (runtimeUpgrade) {
    const codeHex = decoded.args?.code || '';
    const compressed = hexByteLength(codeHex);
    // Backend enrichment (optional). Without it we still show compressed size +
    // zstd magic confirmation; with it we also show specVersion delta.
    const target = pretty?.target;
    const current = pretty?.current;
    const decompressed = pretty?.decompressedBytes;
    const integrityOk = pretty?.integrity === 'match';
    return (
      <div style={{padding:16, background:'rgba(16,185,129,0.06)', border:'1px solid rgba(16,185,129,0.25)', borderRadius:10}}>
        <div style={{fontSize:15, fontWeight:700, marginBottom:10, color:'#10b981'}}>
          {t('preimage.decode.upgradeTitle')}
        </div>
        {current && target ? (
          <div style={{fontSize:14, marginBottom:12}}>
            specVersion <strong style={{color:'var(--fg-2)'}}>{current.specVersion}</strong>
            <span style={{margin:'0 8px', color:'var(--fg-3)'}}>→</span>
            <strong style={{color:'#10b981'}}>{target.specVersion}</strong>
            {target.implVersion !== undefined && <span className="muted tiny" style={{marginLeft:8}}>· impl {target.implVersion}</span>}
          </div>
        ) : (
          <div className="muted tiny" style={{marginBottom:12}}>
            {t('preimage.decode.targetPending')}
          </div>
        )}
        <div style={{display:'grid', gridTemplateColumns:'auto 1fr', gap:'6px 14px', fontSize:13}}>
          <div className="muted">{t('preimage.decode.specName')}</div><div>{target?.specName || current?.specName || 'sora-substrate'}</div>
          <div className="muted">{t('preimage.decode.compressed')}</div><div>{formatBytes(compressed)} <span className="muted tiny">({compressed.toLocaleString()} B)</span></div>
          {decompressed != null && (<>
            <div className="muted">{t('preimage.decode.decompressed')}</div><div>{formatBytes(decompressed)} <span className="muted tiny">({decompressed.toLocaleString()} B)</span></div>
          </>)}
          <div className="muted">{t('preimage.decode.magicZstd')}</div><div style={{color:'#10b981'}}>{t('preimage.decode.magicOk')} <span className="num tiny">52bc537646db8e05</span></div>
          {pretty?.wasmMagicOk != null && (<>
            <div className="muted">{t('preimage.decode.magicWasm')}</div>
            <div style={{color: pretty.wasmMagicOk ? '#10b981' : '#ef4444'}}>
              {pretty.wasmMagicOk
                ? <>{t('preimage.decode.magicOk')} <span className="num tiny">0061736d01000000</span></>
                : t('preimage.decode.magicFail')}
            </div>
          </>)}
          {pretty?.integrity && (<>
            <div className="muted">{t('preimage.decode.integrity')}</div>
            <div style={{color: integrityOk ? '#10b981' : '#ef4444'}}>
              {integrityOk ? t('preimage.decode.integrityMatch') : t('preimage.decode.integrityMismatch')}
            </div>
          </>)}
        </div>
        <div className="muted tiny" style={{marginTop:12, paddingTop:10, borderTop:'1px solid var(--border-color)'}}>
          {t('preimage.decode.rawHelp')}
        </div>
      </div>
    );
  }

  // Generic fallback: key/value grid with:
  //   · asset IDs → symbol + decimals tooltip
  //   · SORA addresses → on-chain identity when available, shortened form otherwise
  //   · balance-like fields → formatted with the relevant token's decimals
  //   · long hex blobs → collapsed with length shown
  return <GenericArgsView decoded={decoded} />;
}

function GenericArgsView({ decoded }) {
  const t = useT();
  const args = decoded.args || {};
  const entries = Object.entries(args);

  // Collect every address that appears in args so we can batch-resolve identities
  // in a single effect pass — addresses may sit at the top level or nested inside
  // one level of object/array (e.g. multisig.as_multi signatories).
  const addresses = useMemo(() => {
    const out = new Set();
    const walk = (v) => {
      if (typeof v === 'string' && looksLikeSoraAddress(v)) out.add(v);
      else if (Array.isArray(v)) v.forEach(walk);
      else if (v && typeof v === 'object') Object.values(v).forEach(walk);
    };
    entries.forEach(([, v]) => walk(v));
    return [...out];
  }, [entries.length, decoded]);
  const identities = useResolveIdentities(addresses);

  if (entries.length === 0) {
    return <div className="muted" style={{padding:14}}>{t('preimage.decode.noArgs')}</div>;
  }

  const renderValue = (key, v, siblings) => {
    // Asset ID → symbol.
    if (looksLikeAssetId(v)) {
      const meta = KNOWN_ASSETS[v.toLowerCase()];
      if (meta) {
        return (
          <span title={v} style={{fontWeight:600}}>
            {meta.sym}
            <span className="muted tiny" style={{marginLeft:6, fontWeight:400}}>· {v.slice(0, 10)}…</span>
          </span>
        );
      }
      return <span className="num tiny" title={v}>{v.slice(0, 10)}…{v.slice(-4)}</span>;
    }
    // SORA address → identity or shortened form.
    if (looksLikeSoraAddress(v)) {
      const display = identities[v];
      return (
        <span title={v}>
          {display
            ? <strong style={{color:'var(--accent)'}}>{display}</strong>
            : <span className="num tiny">{fmt.addr(v, 6, 4)}</span>}
        </span>
      );
    }
    // Long hex blob → collapsed.
    if (typeof v === 'string' && v.startsWith('0x') && v.length > 80) {
      return (
        <span className="num tiny" title={v}>
          {v.slice(0, 18)}…{v.slice(-8)}
          <span className="muted" style={{marginLeft:8}}>({hexByteLength(v).toLocaleString()} B)</span>
        </span>
      );
    }
    // Balance-like field → format with the decimals of the sibling asset_id if
    // we can find one; otherwise default to 18 (XOR decimals, the SORA norm).
    if (looksLikeBalanceField(key) && typeof v === 'string' && /^\d+$/.test(v.replace(/,/g, ''))) {
      const assetIdNeighbour = siblings
        && Object.values(siblings).find(x => looksLikeAssetId(x));
      const meta = assetIdNeighbour ? KNOWN_ASSETS[String(assetIdNeighbour).toLowerCase()] : null;
      const decimals = meta?.decimals ?? 18;
      return (
        <span>
          <strong>{formatBalance(v, decimals)}</strong>
          {meta && <span className="muted tiny" style={{marginLeft:6}}>{meta.sym}</span>}
          <span className="muted tiny" style={{marginLeft:8}} title={`raw: ${v}`}>(raw)</span>
        </span>
      );
    }
    // Nested objects/arrays — render as pretty JSON so batchAll/sub-calls are
    // still readable without falling off the cliff of JSON.stringify on the
    // whole args tree.
    if (v && typeof v === 'object') {
      return <pre style={{margin:0, padding:8, background:'rgba(0,0,0,0.25)', borderRadius:6, fontSize:12, maxHeight:240, overflow:'auto'}}>{JSON.stringify(v, null, 2)}</pre>;
    }
    return <span style={{wordBreak:'break-all'}}>{String(v)}</span>;
  };

  return (
    <div style={{display:'grid', gridTemplateColumns:'auto 1fr', gap:'10px 14px', fontSize:13, padding:12, background:'rgba(0,0,0,0.2)', borderRadius:8}}>
      {entries.map(([k, v]) => (
        <React.Fragment key={k}>
          <div className="muted" style={{fontWeight:600}}>{k}</div>
          <div>{renderValue(k, v, args)}</div>
        </React.Fragment>
      ))}
    </div>
  );
}

function PreimageDecodeModal({ hash, len, onClose }) {
  const t = useT();
  const [data, setData] = useState(null);
  const [pretty, setPretty] = useState(true);   // default human view
  const [prettyData, setPrettyData] = useState(null); // enriched payload from /decode-pretty (optional)
  const [loading, setLoading] = useState(true);
  const [fetchErr, setFetchErr] = useState(null);
  const [nonce, setNonce] = useState(0);
  useEffect(() => {
    let cancelled = false;
    setLoading(true); setFetchErr(null); setPrettyData(null);
    fetch('/governance/preimage/' + encodeURIComponent(hash) + '?len=' + encodeURIComponent(len))
      .then(async r => {
        if (!r.ok) {
          const body = await r.text().catch(() => '');
          throw new Error('HTTP ' + r.status + (body ? ' · ' + body.slice(0, 200) : ''));
        }
        return r.json();
      })
      .then(j => { if (!cancelled) { setData(j); setLoading(false); } })
      .catch(err => { if (!cancelled) { setFetchErr(err.message || 'fetch failed'); setLoading(false); } });
    return () => { cancelled = true; };
  }, [hash, len, nonce]);

  // Optional enrichment: if the backend exposes /decode-pretty, pull extra
  // fields (target specVersion, decompressed size, integrity check). If the
  // endpoint isn't deployed yet it just 404s — the basic human view still works.
  useEffect(() => {
    if (!data?.decoded) return;
    if (!isRuntimeUpgradeCall(data.decoded)) return;
    let cancelled = false;
    fetch('/governance/preimage/' + encodeURIComponent(hash) + '/decode-pretty?len=' + encodeURIComponent(len))
      .then(r => r.ok ? r.json() : null)
      .then(j => { if (!cancelled && j && !j.error) setPrettyData(j); })
      .catch(() => {});
    return () => { cancelled = true; };
  }, [hash, len, data]);

  // For large string args (e.g. system.setCode code: 5.9MB hex), JSON.stringify
  // of the whole args object freezes the browser. Build a truncated view that
  // keeps the structure readable, and expose "Copy full bytes" buttons per field.
  const TRUNC = 400;
  const truncatedArgs = useMemo(() => {
    const args = data?.decoded?.args;
    if (!args || typeof args !== 'object') return args;
    const out = {};
    for (const [k, v] of Object.entries(args)) {
      if (typeof v === 'string' && v.length > TRUNC) {
        out[k] = v.slice(0, TRUNC) + `… [truncated · ${v.length.toLocaleString()} chars]`;
      } else {
        out[k] = v;
      }
    }
    return out;
  }, [data]);

  const largeFields = useMemo(() => {
    const args = data?.decoded?.args;
    if (!args || typeof args !== 'object') return [];
    return Object.entries(args)
      .filter(([, v]) => typeof v === 'string' && v.length > TRUNC)
      .map(([k, v]) => ({ key: k, length: v.length, value: v }));
  }, [data]);

  const copyField = async (field) => {
    try { await navigator.clipboard.writeText(field.value); }
    catch { /* fallback: open a new window with the text */ }
  };

  const is429 = fetchErr && /429|Too many requests/i.test(fetchErr);

  // Portal to body so backdrop-filter / overflow:hidden on ancestor .card
  // cannot clip or re-anchor the position:fixed modal.
  return ReactDOM.createPortal((
    <div onMouseDown={(e) => { if (e.target === e.currentTarget) onClose(); }} style={{position:'fixed', inset:0, background:'rgba(0,0,0,0.6)', zIndex:10000, display:'flex', alignItems:'center', justifyContent:'center', padding:20}}>
      <div onClick={e => e.stopPropagation()} onMouseDown={e => e.stopPropagation()} style={{background:'var(--bg-card)', color:'var(--fg-0)', borderRadius:12, maxWidth:760, width:'100%', maxHeight:'90vh', overflow:'auto', padding:22, border:'1px solid var(--border-color)'}}>
        <div style={{display:'flex', justifyContent:'space-between', alignItems:'flex-start', marginBottom:14, gap:10, flexWrap:'wrap'}}>
          <div>
            <h3 style={{margin:0, fontSize:17}}>{t('preimage.decode.title')}</h3>
            <div className="num tiny muted" style={{marginTop:4, wordBreak:'break-all'}}>{hash}</div>
          </div>
          <button className="btn" onClick={onClose}>{t('common.close', 'Close')}</button>
        </div>
        {loading && <div style={{padding:30, textAlign:'center', color:'var(--fg-2)'}}>{len > 1_000_000 ? t('preimage.decode.decodingLarge') : t('preimage.decode.decoding')}</div>}
        {!loading && fetchErr && (
          <div style={{padding:14, border:'1px solid rgba(239,68,68,0.3)', borderRadius:8, background:'rgba(239,68,68,0.08)'}}>
            <div style={{color:'#ef4444', fontWeight:700, marginBottom:6}}>Error: {fetchErr}</div>
            {is429 && (
              <div style={{color:'var(--fg-2)', fontSize:12, marginBottom:10}}>
                {t('preimage.decode.rateLimitHint')}
              </div>
            )}
            <button className="btn" onClick={() => setNonce(n => n + 1)}>{t('preimage.decode.retry')}</button>
          </div>
        )}
        {!loading && !fetchErr && data?.decoded && (
          <div>
            <div style={{display:'flex', justifyContent:'space-between', alignItems:'center', gap:10, flexWrap:'wrap', marginBottom:8}}>
              <div style={{fontWeight:700, fontSize:18}}>
                <span style={{color:'var(--fg-2)'}}>{data.decoded.section}</span>
                <span style={{color:'var(--fg-3)'}}>.</span>
                <span style={{color:'var(--accent, #10b981)'}}>{data.decoded.method}</span>
              </div>
              <button className="btn" onClick={() => setPretty(p => !p)}
                title={pretty ? t('preimage.decode.viewJsonTip') : t('preimage.decode.viewHumanTip')}
                style={{padding:'4px 10px', fontSize:12}}>
                {pretty ? t('preimage.decode.viewJson') : t('preimage.decode.viewHuman')}
              </button>
            </div>
            {data.decoded.description && <div style={{color:'var(--fg-2)', fontSize:12, marginBottom:12}}>{data.decoded.description}</div>}
            {largeFields.length > 0 && (
              <div style={{marginBottom:10, display:'flex', gap:8, flexWrap:'wrap'}}>
                {largeFields.map(f => (
                  <React.Fragment key={f.key}>
                    <button className="btn" onClick={() => copyField(f)}
                      title={t('preimage.decode.copyFieldTip').replace('{key}', f.key)}
                      style={{padding:'4px 10px', fontSize:12}}>
                      📋 {t('preimage.decode.copyField')} {f.key} ({f.length.toLocaleString()} chars)
                    </button>
                    <button className="btn" onClick={() => {
                      // Raw hex as a .txt file — useful for sharing without a clipboard limit.
                      triggerDownload(`preimage-${hash.slice(2, 10)}-${f.key}.hex.txt`,
                        new Blob([f.value], { type: 'text/plain' }));
                    }} title={t('preimage.decode.downloadHexTip').replace('{key}', f.key)}
                      style={{padding:'4px 10px', fontSize:12}}>
                      💾 {t('preimage.decode.download')} {f.key}.hex
                    </button>
                    {isRuntimeUpgradeCall(data.decoded) && f.key === 'code' && (
                      <button className="btn" onClick={() => {
                        // Actual decoded bytes as .wasm.zst so the recipient can run
                        // `zstd -d` + any WASM tool directly, no hex parsing needed.
                        const bytes = hexToBytes(f.value);
                        // Strip the 8-byte Substrate framing so the output is a plain
                        // zstd file recognised by standard tools.
                        const zst = bytes.slice(8);
                        triggerDownload(`runtime-${hash.slice(2, 10)}.wasm.zst`,
                          new Blob([zst], { type: 'application/zstd' }));
                      }} title={t('preimage.decode.downloadWasmTip')}
                        style={{padding:'4px 10px', fontSize:12}}>
                      💾 {t('preimage.decode.downloadWasm')}
                    </button>
                    )}
                  </React.Fragment>
                ))}
              </div>
            )}

            {pretty ? (
              <HumanDecodeView decoded={data.decoded} pretty={prettyData} />
            ) : (
              <pre style={{margin:0, padding:12, background:'rgba(0,0,0,0.3)', borderRadius:8, overflow:'auto', maxHeight:'60vh', fontSize:12, lineHeight:1.5}}>{JSON.stringify(truncatedArgs, null, 2)}</pre>
            )}
          </div>
        )}
        {!loading && !fetchErr && data && !data.decoded && (
          <div className="muted" style={{padding:14}}>{t('preimage.decode.noDecoded')}</div>
        )}
      </div>
    </div>
  ), document.body);
}

function PreimageHistoryModal({ hash, onClose }) {
  const t = useT();
  const [events, setEvents] = useState([]);
  const [indexer, setIndexer] = useState({});
  const [loading, setLoading] = useState(true);
  const [usedSlow, setUsedSlow] = useState(false);

  useEffect(() => {
    let cancelled = false;
    setLoading(true);
    // Try fast first (indexer-backed).
    fetch('/governance/preimage/' + encodeURIComponent(hash) + '/events-fast')
      .then(r => r.ok ? r.json() : null)
      .then(j => {
        if (cancelled) return;
        if (j && Array.isArray(j.events)) {
          setEvents(j.events.slice().sort((a, b) => a.block - b.block));
          setIndexer(j.indexer || {});
          setLoading(false);
        }
      }).catch(() => { if (!cancelled) setLoading(false); });
    return () => { cancelled = true; };
  }, [hash]);

  const scanEarlier = async () => {
    setUsedSlow(true);
    setLoading(true);
    try {
      const r = await fetch('/governance/preimage/' + encodeURIComponent(hash) + '/events');
      if (r.ok && (r.headers.get('content-type') || '').includes('json')) {
        const j = await r.json();
        if (j && Array.isArray(j.events)) {
          const merged = [...events, ...j.events].sort((a, b) => a.block - b.block)
            .filter((e, i, arr) => i === 0 || arr[i-1].block !== e.block || arr[i-1].event !== e.event);
          setEvents(merged);
        }
      }
    } catch {}
    setLoading(false);
  };

  const firstNoted = events.find(e => e.event === 'preimage.Noted');
  const firstReq = events.find(e => e.event === 'preimage.Requested');
  const lastCleared = [...events].reverse().find(e => e.event === 'preimage.Cleared' || e.event === 'preimage.Unnoted');
  const reasonLabel = {
    runtime_upgrade: t('preimage.history.reasonUpgrade'),
    scheduler_dispatched: t('preimage.history.reasonScheduler'),
    unnote_manual: t('preimage.history.reasonUnnote'),
  };

  // Portal to body so backdrop-filter / overflow:hidden on ancestor .card
  // cannot clip or re-anchor the position:fixed modal.
  return ReactDOM.createPortal((
    <div onMouseDown={(e) => { if (e.target === e.currentTarget) onClose(); }} style={{position:'fixed', inset:0, background:'rgba(0,0,0,0.6)', zIndex:10000, display:'flex', alignItems:'center', justifyContent:'center', padding:20}}>
      <div onClick={e => e.stopPropagation()} onMouseDown={e => e.stopPropagation()} style={{background:'var(--bg-card)', color:'var(--fg-0)', borderRadius:12, maxWidth:880, width:'100%', maxHeight:'90vh', overflow:'auto', padding:22, border:'1px solid var(--border-color)'}}>
        <div style={{display:'flex', justifyContent:'space-between', alignItems:'flex-start', marginBottom:14, gap:10, flexWrap:'wrap'}}>
          <div>
            <h3 style={{margin:0, fontSize:17}}>{t('preimage.history.title')}</h3>
            <div className="num tiny muted" style={{marginTop:4, wordBreak:'break-all'}}>{hash}</div>
          </div>
          <button className="btn" onClick={onClose}>{t('common.close', 'Close')}</button>
        </div>

        <div style={{background:'rgba(255,255,255,0.03)', border:'1px solid var(--border-color)', borderRadius:8, padding:'12px 14px', marginBottom:14}}>
          <div className="muted tiny" style={{textTransform:'uppercase', letterSpacing:'0.05em', marginBottom:6}}>{t('preimage.history.summary')}</div>
          {firstNoted
            ? <div style={{margin:'3px 0'}}><span style={{color:'#10b981'}}>● {t('preimage.history.noted')}</span> · block {firstNoted.block} · {firstNoted.timestamp ? new Date(firstNoted.timestamp).toLocaleString() : '—'}</div>
            : <div className="muted tiny" style={{margin:'3px 0'}}>● {t('preimage.history.noNoted')}</div>}
          {firstReq && <div style={{margin:'3px 0'}}><span style={{color:'#f59e0b'}}>● {t('preimage.history.requested')}</span> · block {firstReq.block} · {firstReq.timestamp ? new Date(firstReq.timestamp).toLocaleString() : '—'}</div>}
          {lastCleared && <div style={{margin:'3px 0'}}><span style={{color:'#ef4444'}}>● {t('preimage.history.cleared')}</span> · block {lastCleared.block} · {lastCleared.timestamp ? new Date(lastCleared.timestamp).toLocaleString() : '—'}</div>}
          {indexer.backfillCursor && !firstNoted && (
            <div className="muted tiny" style={{marginTop:8, paddingTop:8, borderTop:'1px solid var(--border-color)'}}>
              ℹ️ {t('preimage.history.indexerWorking').replace('{cursor}', String(indexer.backfillCursor))}
            </div>
          )}
        </div>

        {events.length === 0
          ? <div className="muted" style={{padding:20, textAlign:'center'}}>{loading ? t('preimage.history.loading') : t('preimage.history.noEvents')}</div>
          : (
            <div style={{display:'flex', flexDirection:'column', gap:8}}>
              {events.map((e, i) => {
                const color = e.event.endsWith('.Noted') ? '#10b981' : e.event.endsWith('.Requested') ? '#f59e0b' : '#ef4444';
                const reason = e.reason ? reasonLabel[e.reason] || e.reason : '';
                return (
                  <div key={i} style={{display:'flex', gap:12, padding:'10px 12px', border:'1px solid var(--border-color)', borderRadius:8, alignItems:'center', flexWrap:'wrap'}}>
                    <div style={{minWidth:140}}>
                      <span style={{color, fontWeight:600}}>{e.event}</span>
                      {reason && <span className="muted tiny" style={{marginLeft:6}}>· {reason}</span>}
                    </div>
                    <div className="muted tiny" style={{flex:1, minWidth:180}}>{e.timestamp ? new Date(e.timestamp).toLocaleString() : '—'}</div>
                    <div className="num muted tiny">block {e.block}</div>
                  </div>
                );
              })}
            </div>
          )}

        <div style={{marginTop:14, display:'flex', justifyContent:'space-between', alignItems:'center', flexWrap:'wrap', gap:10, fontSize:12, color:'var(--fg-2)'}}>
          <div>
            {usedSlow ? (
              t('preimage.history.rpcScanUsed')
            ) : indexer.backfillComplete ? (
              <span style={{color:'#10b981'}}>{t('preimage.history.dbComplete')}</span>
            ) : (
              <>{t('preimage.history.dbSource')} <span className="muted tiny">· {t('preimage.history.dbBackfilling')}</span></>
            )}
          </div>
          {/* Only offer the RPC scan fallback when the indexer hasn't finished
              walking the historical range. Once backfillComplete=true the DB
              is the source of truth, so the button would just add latency
              without new data. */}
          {!usedSlow && !indexer.backfillComplete && (
            <button className="btn" onClick={scanEarlier} disabled={loading}>{t('preimage.history.rpcScanBtn')}</button>
          )}
        </div>
      </div>
    </div>
  ), document.body);
}

/* =========================================================================
   SCHEDULER AGENDA PANEL — alerts when preimage bytes are missing
   ========================================================================= */

function SchedulerAgendaPanel() {
  const t = useT();
  const [data, setData] = useState({ tip: null, entries: [] });
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    let cancelled = false;
    const pull = () => fetch('/governance/scheduler/agenda').then(r => r.ok ? r.json() : null)
      .then(j => { if (!cancelled && j) { setData(j); setLoading(false); } })
      .catch(() => { if (!cancelled) setLoading(false); });
    pull();
    const id = setInterval(pull, 30_000);
    return () => { cancelled = true; clearInterval(id); };
  }, []);

  const fmtCountdown = (sec) => {
    if (!sec || sec <= 0) return 'ahora';
    if (sec < 60) return Math.round(sec) + 's';
    if (sec < 3600) return Math.round(sec/60) + ' min';
    if (sec < 86400) return (sec/3600).toFixed(1) + ' h';
    return (sec/86400).toFixed(1) + ' d';
  };

  const entries = data.entries || [];
  const alerts = entries.filter(e => e.alert).length;

  return (
    <div className="card" style={{marginTop: 18}}>
      <div className="card-header">
        <div className="card-title"><span className="dot"/> {t('gov.scheduler.title', 'Próximas ejecuciones (Scheduler)')}</div>
        <span className="tag">{entries.length} {t('gov.scheduler.scheduled', 'programadas')}{loading ? ' · ' + t('common.loading', 'cargando') : ''}</span>
      </div>
      {alerts > 0 && (
        <div style={{margin:'0 14px 12px', background:'rgba(239,68,68,0.12)', border:'1px solid #ef4444', borderRadius:8, padding:'10px 12px', fontSize:13}}>
          <strong style={{color:'#ef4444'}}>⚠ {alerts} alerta{alerts > 1 ? 's' : ''}:</strong> {t('gov.scheduler.alertWillFail', 'hay ejecuciones programadas que fallarán porque los bytes de la preimagen no están on-chain.')}
        </div>
      )}
      <div className="swaps-table-wrap">
        <table className="swaps-table">
          <thead>
            <tr>
              <th style={{paddingLeft:20}}>{t('gov.scheduler.block', 'Bloque')}</th>
              <th>{t('gov.scheduler.executesAt', 'Ejecuta en')}</th>
              <th>{t('gov.scheduler.call', 'Call')}</th>
              <th>{t('gov.scheduler.preimage', 'Preimage')}</th>
              <th style={{paddingRight:20}}>{t('gov.scheduler.origin', 'Origen')}</th>
            </tr>
          </thead>
          <tbody>
            {entries.length === 0 && (
              <tr><td colSpan={5} style={{padding:24, textAlign:'center', color:'var(--fg-2)'}}>{loading ? t('gov.scheduler.loading', 'Cargando agenda...') : t('gov.scheduler.empty', 'No hay ejecuciones programadas.')}</td></tr>
            )}
            {entries.map((e, i) => {
              const callLabel = e.inlineDecoded
                ? <strong>{e.inlineDecoded.section}.{e.inlineDecoded.method} <span className="muted tiny">(inline)</span></strong>
                : e.lookupHash
                  ? <span className="num tiny">{e.lookupHash.slice(0, 10)}…{e.lookupHash.slice(-6)} <span className="muted">({e.lookupLen ?? '?'} bytes)</span></span>
                  : <span className="muted">?</span>;
              let preimageLabel;
              if (e.lookupHash) {
                if (e.preimage?.bytesAvailable) preimageLabel = <span style={{color:'#10b981'}}>✓ disponible ({e.preimage.len} bytes)</span>;
                else if (e.preimage) preimageLabel = <span><span style={{color:'#ef4444', fontWeight:700}}>✗ FALTA</span> <span className="muted tiny">(status: {e.preimage.status || '-'})</span></span>;
                else preimageLabel = <span className="muted">?</span>;
              } else {
                preimageLabel = <span className="muted">n/a (inline)</span>;
              }
              const originLabel = e.origin && typeof e.origin === 'object'
                ? Object.keys(e.origin)[0] + (e.origin[Object.keys(e.origin)[0]]?.root !== undefined ? '.root' : '')
                : String(e.origin || '-');
              return (
                <tr key={i} style={e.alert ? {background:'rgba(239,68,68,0.08)'} : undefined}>
                  <td style={{paddingLeft:20}} className="num">{e.block}</td>
                  <td>{fmtCountdown(e.secondsRemaining)}</td>
                  <td>{callLabel}</td>
                  <td>{preimageLabel}</td>
                  <td style={{paddingRight:20}} className="muted tiny">{originLabel}</td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
      {data.tip != null && (
        <div className="muted tiny" style={{padding:'8px 14px'}}>{t('gov.scheduler.tipFooter', 'Tip actual: {tip} · {n} ejecuciones programadas').replace('{tip}', String(data.tip)).replace('{n}', String(entries.length))}</div>
      )}
    </div>
  );
}

/* =========================================================================
   BALANCE (Overview / Mis Wallets / Vigiladas — Spanish)
   ========================================================================= */

// G7 — universal time-range pills. Match prod's exact labels: 4H/1D/7D/1M/1Y.
// Selection persists in localStorage so it survives tab switches + reloads.
const TIME_RANGES = [
  { id: '4h', label: '4H', hours: 4 },
  { id: '1d', label: '1D', hours: 24 },
  { id: '7d', label: '7D', hours: 168 },
  { id: '1m', label: '1M', hours: 720 },
  { id: '1y', label: '1Y', hours: 8760 },
];
function useTimeRange(defaultId = '7d') {
  const [id, setId] = useState(() => {
    try { return localStorage.getItem('sm.timeRange') || defaultId; } catch { return defaultId; }
  });
  const set = (next) => {
    setId(next);
    try { localStorage.setItem('sm.timeRange', next); } catch {}
  };
  return [id, set];
}
function TimeRangePills({ value, onChange }) {
  return (
    <div className="filter-row" style={{gap: 4}}>
      {TIME_RANGES.map(r => (
        <div
          key={r.id}
          className={'filter-chip' + (value === r.id ? ' active' : '')}
          onClick={() => onChange(r.id)}
          style={{cursor:'pointer', minWidth: 40, textAlign:'center', fontWeight: 600}}>
          {r.label}
        </div>
      ))}
    </div>
  );
}

function BalanceSection({ tweaks }) {
  const t = useT();
  // Overview was removed — the Portfolio section now owns the aggregated view.
  // Balance is purely per-wallet + activity.
  const [tab, setTab] = useState('mis');
  const [addOpen, setAddOpen] = useState(false);
  const [detailWallet, setDetailWallet] = useState(null);
  const [range, setRange] = useTimeRange('24h');
  const store = useWallets();
  const wallets = store.wallets;
  const watched = store.watched;

  // Deep-link from external URL: /sorav2?tab=balance&address=<SS58>.
  // The XOR cross-chain migration table on /minamoto opens this URL in a new
  // tab when the v2 signer is clicked; we read the param once on mount and
  // pop the wallet-details modal so the user lands on the right wallet.
  useEffect(() => {
    try {
      const addr = new URLSearchParams(window.location.search).get('address');
      if (addr && typeof window.openWalletDetails === 'function') {
        window.openWalletDetails(addr);
      }
    } catch (_) {}
  }, []);

  // --- Privacy toggles (persisted in localStorage) ---
  const [hideBalances, setHideBalances] = useState(() => {
    try { return localStorage.getItem('sm.hideBalances') === '1'; } catch { return false; }
  });
  const [hideLowBalances, setHideLowBalances] = useState(() => {
    try { return localStorage.getItem('sm.hideLowBalances') === '1'; } catch { return false; }
  });
  useEffect(() => { try { localStorage.setItem('sm.hideBalances', hideBalances ? '1' : '0'); } catch {} }, [hideBalances]);
  useEffect(() => { try { localStorage.setItem('sm.hideLowBalances', hideLowBalances ? '1' : '0'); } catch {} }, [hideLowBalances]);

  // --- Currency selector (USD / EUR / XOR), rates from /currency-rates ---
  const [currency, setCurrency] = useState(() => {
    try { return localStorage.getItem('sm.currency') || 'USD'; } catch { return 'USD'; }
  });
  const [fxRates, setFxRates] = useState({ USD: 1, EUR: 0.92, XOR: 1 });
  useEffect(() => { try { localStorage.setItem('sm.currency', currency); } catch {} }, [currency]);
  useEffect(() => {
    fetch('/currency-rates').then(r => r.ok ? r.json() : null).then(j => {
      if (j && Number(j.EUR)) setFxRates(prev => ({ ...prev, EUR: Number(j.EUR) }));
    }).catch(() => {});
  }, []);
  // XOR rate derived from any wallet's XOR holding (usdValue / amount).
  useEffect(() => {
    for (const w of wallets) {
      const xor = (w.tokens || []).find(tk => tk.symbol === 'XOR' && Number(tk.amount) > 0);
      if (xor) {
        const rate = Number(xor.usdValue) / Number(xor.amount);
        if (Number.isFinite(rate) && rate > 0) {
          setFxRates(prev => prev.XOR === 1/rate ? prev : ({ ...prev, XOR: 1/rate }));
          break;
        }
      }
    }
  }, [wallets]);

  // --- LP + Staking aggregated across user's wallets ---
  const [lpSummary, setLpSummary] = useState({ lpUsd: 0, stakingUsd: 0, positions: [], validators: 0, loading: true });
  useEffect(() => {
    if (!wallets.length) { setLpSummary({ lpUsd: 0, stakingUsd: 0, positions: [], validators: 0, loading: false }); return; }
    let cancelled = false;
    setLpSummary(s => ({ ...s, loading: true }));
    (async () => {
      let lpUsd = 0, stakingUsd = 0;
      const positions = [];
      let validators = 0;
      await Promise.all(wallets.map(async w => {
        try {
          const [lp, st] = await Promise.all([
            fetch('/wallet/liquidity/' + encodeURIComponent(w.addr)).then(r => r.ok ? r.json() : null).catch(() => null),
            fetch('/wallet/staking/' + encodeURIComponent(w.addr)).then(r => r.ok ? r.json() : null).catch(() => null),
          ]);
          if (lp && Array.isArray(lp.pools)) {
            for (const p of lp.pools) {
              const usd = Number(p.usdValue) || 0;
              lpUsd += usd;
              if (usd > 0) positions.push({ wallet: w.alias, pair: (p.baseSymbol || '?') + '/' + (p.targetSymbol || '?'), usd });
            }
          }
          if (st) {
            stakingUsd += Number(st.totalStakedUsd) || 0;
            validators += Array.isArray(st.nominations) ? st.nominations.length : 0;
          }
        } catch {}
      }));
      if (!cancelled) setLpSummary({ lpUsd, stakingUsd, positions: positions.sort((a,b) => b.usd - a.usd), validators, loading: false });
    })();
    return () => { cancelled = true; };
  }, [wallets.map(w => w.addr).join(',')]);

  // --- Net worth: tokens + LP + staking ---
  const tokensNet = wallets.reduce((s, w) => s + (w.tokens || []).reduce((a, tk) => a + (Number(tk.usdValue) || 0), 0), 0);
  const net = tokensNet + (lpSummary.lpUsd || 0) + (lpSummary.stakingUsd || 0);

  // Currency formatter. Respects hideBalances.
  const formatMoney = (usd) => {
    if (hideBalances) return '••••••';
    const rate = fxRates[currency] || 1;
    const val = (Number(usd) || 0) * rate;
    if (currency === 'USD') return '$' + val.toLocaleString(undefined, { maximumFractionDigits: 2 });
    if (currency === 'EUR') return '€' + val.toLocaleString(undefined, { maximumFractionDigits: 2 });
    return val.toLocaleString(undefined, { maximumFractionDigits: 4 }) + ' XOR';
  };

  const rangeLabel = TIME_RANGES.find(r => r.id === range)?.label || '24h';

  return (
    <div>
      <PageHeader title={t('balance.title')} sub={t('balance.sub')}>
        <select
          value={currency}
          onChange={e => setCurrency(e.target.value)}
          className="pill"
          style={{padding:'6px 12px', borderRadius:8, background:'var(--bg-card)', color:'var(--fg-0)', border:'1px solid var(--border-color)', fontSize:13, cursor:'pointer'}}
          title="Moneda de visualización">
          <option value="USD">$ USD</option>
          <option value="EUR">€ EUR</option>
          <option value="XOR">✕ XOR</option>
        </select>
        <button
          onClick={() => setHideBalances(v => !v)}
          className="icon-btn"
          title={hideBalances ? 'Mostrar saldos' : 'Ocultar saldos'}
          style={{background:'none', border:'none', color:'var(--fg-0)', fontSize:18, cursor:'pointer', padding:'4px 8px', opacity: hideBalances ? 1 : 0.6}}>
          {hideBalances ? '👁‍🗨' : '👁'}
        </button>
        <button className="btn primary" onClick={() => setAddOpen(true)}>+ Añadir Wallet</button>
      </PageHeader>

      <Tabs tabs={[
        { id:'mis', label:'Mis Wallets', count: wallets.length },
        { id:'vig', label:'Vigiladas', count: watched.length },
        { id:'swaps', label:'Swaps' },
        { id:'transfers', label:'Transfers' },
        { id:'bridges', label:'Bridges' },
        { id:'extrinsics', label:'Extrinsics' },
      ]} current={tab} onChange={setTab}/>

      {['swaps','transfers','bridges','extrinsics'].includes(tab) && (
        <AggregatedHistory kind={tab} wallets={wallets} watched={watched}/>
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
            {watched.map((w, i) => {
              const toks = (w.tokens || []).filter(t => Number(t.amount) > 0);
              const totalUsd = toks.reduce((s, t) => s + (Number(t.usdValue) || 0), 0);
              return (
                <div key={w.id || i}
                     className="wallet-list-card clickable"
                     style={{cursor:'pointer'}}
                     onClick={() => setDetailWallet(w)}
                     title="Abrir detalle de la wallet">
                  <div style={{width: 36, height: 36, borderRadius: 8, background:'linear-gradient(135deg,#7B5B90,#4A3566)', display:'grid', placeItems:'center', fontWeight:800}}>👁</div>
                  <div style={{flex:1, minWidth: 0}}>
                    <div style={{fontWeight: 700}}>{w.alias}</div>
                    <div className="muted tiny num">{fmt.addr(w.addr, 8, 6)}</div>
                  </div>
                  <div style={{textAlign:'right'}}>
                    <div className="num" style={{fontWeight:700, fontSize:15}}>{totalUsd > 0 ? '$' + totalUsd.toLocaleString(undefined, {maximumFractionDigits:2}) : '—'}</div>
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


// Combines /history/<kind>/<addr> for every wallet the user has added, merges
// and sorts by timestamp, then renders with the same WalletHistoryTable used
// inside the per-wallet drill so the visuals stay consistent.
// Combines /history/<kind>/<addr> for every wallet in scope (mine / watched /
// both), merges + sorts by timestamp. Renders its own compact table with a
// dedicated Wallet column so the user can tell which of their accounts
// produced each row — the shared WalletHistoryTable doesn't surface that.
function AggregatedHistory({ kind, wallets, watched }) {
  const t = useT();
  const [scope, setScope] = useState('all');                 // 'all' | 'mis' | 'vig'
  const [walletFilter, setWalletFilter] = useState('all');   // 'all' | <addr>
  const [rows, setRows] = useState(null);                    // null = loading

  const activeSet = useMemo(() => {
    if (scope === 'mis') return wallets || [];
    if (scope === 'vig') return watched || [];
    return [...(wallets || []), ...(watched || [])];
  }, [scope, wallets, watched]);

  useEffect(() => { setWalletFilter('all'); }, [scope]);

  useEffect(() => {
    if (activeSet.length === 0) { setRows([]); return; }
    let cancelled = false;
    setRows(null);
    (async () => {
      const all = await Promise.all(
        activeSet.map(w =>
          fetch('/history/' + kind + '/' + encodeURIComponent(w.addr))
            .then(r => r.ok ? r.json() : null)
            .then(j => {
              const arr = Array.isArray(j) ? j : (j?.data || j?.items || []);
              return arr.map(row => ({
                ...row,
                __walletAlias: w.alias,
                __walletAddr: w.addr,
                __walletKind: (wallets || []).some(x => x.addr === w.addr) ? 'mis' : 'vig',
              }));
            })
            .catch(() => [])
        )
      );
      if (cancelled) return;
      const parseTs = (r) => {
        if (r.timestamp) { const n = Number(r.timestamp); return n < 1e12 ? n * 1000 : n; }
        if (r.time) return parseHistTime(r.time);
        return 0;
      };
      const flat = all.flat().sort((a, b) => parseTs(b) - parseTs(a));
      setRows(flat.slice(0, 200));
    })();
    return () => { cancelled = true; };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [kind, activeSet.map(w => w.addr).join(',')]);

  if (!wallets.length && !watched.length) {
    return (
      <div className="card" style={{marginTop:14, padding:24, textAlign:'center', color:'var(--fg-2)'}}>
        Añade una wallet (mía o vigilada) para ver su actividad aquí.
      </div>
    );
  }

  const filteredRows = walletFilter === 'all'
    ? (rows || [])
    : (rows || []).filter(r => r.__walletAddr === walletFilter);

  // Extract the row's USD value so we can render it in a dedicated column with
  // a fixed width — otherwise USD drifts far-right with flex auto and leaves
  // a visually "empty" band across every row.
  const rowUsd = (r) => {
    if (kind === 'swaps') return Number(r.in?.usd) || 0;
    if (kind === 'transfers') return Number(r.usdValue) || 0;
    if (kind === 'bridges') return Number(r.usd_value || r.usdValue) || 0;
    return 0;
  };

  const renderDetail = (r) => {
    if (kind === 'swaps') {
      return (
        <div style={{display:'flex', alignItems:'center', justifyContent:'center', gap:6, flexWrap:'wrap'}}>
          <TinyTokLogo sym={r.in?.symbol} logo={r.in?.logo}/>
          <span className="num">{fmt.num(Number(r.in?.amount || 0), 2)}</span>
          <span style={{fontWeight:700}}>{r.in?.symbol}</span>
          <span style={{color:'var(--fg-3)'}}>→</span>
          <TinyTokLogo sym={r.out?.symbol} logo={r.out?.logo}/>
          <span className="num">{fmt.num(Number(r.out?.amount || 0), 2)}</span>
          <span style={{fontWeight:700}}>{r.out?.symbol}</span>
        </div>
      );
    }
    if (kind === 'transfers') {
      // Click on from/to opens the respective wallet detail modal. Stop
      // propagation so we don't also trigger the row's wallet-column click.
      const openWallet = (addr) => (ev) => {
        ev.stopPropagation();
        if (addr) window.openWalletDetails?.(addr, window.identityName?.(addr) || null);
      };
      return (
        <div style={{display:'flex', alignItems:'center', justifyContent:'center', gap:6, flexWrap:'wrap'}}>
          <TinyTokLogo sym={r.symbol} logo={r.logo}/>
          <span className="num">{fmt.num(Number(r.amount || 0), 2)}</span>
          <span style={{fontWeight:700}}>{r.symbol}</span>
          <span className="muted tiny" style={{display:'inline-flex', alignItems:'center', gap:4}}>
            <AddrOrName addr={r.from} prefix={6} suffix={4} onClick={openWallet(r.from)} style={{textDecoration:'underline dotted'}}/>
            {' → '}
            <AddrOrName addr={r.to} prefix={6} suffix={4} onClick={openWallet(r.to)} style={{textDecoration:'underline dotted'}}/>
          </span>
        </div>
      );
    }
    if (kind === 'bridges') {
      const openWallet = (addr) => (ev) => {
        ev.stopPropagation();
        // Only open SORA-side drill for SS58 addresses. EVM addresses (0x…) get
        // no drill because /balance/:addr requires SS58.
        if (addr && !/^0x/.test(addr)) window.openWalletDetails?.(addr, window.identityName?.(addr) || null);
      };
      return (
        <div style={{display:'flex', alignItems:'center', justifyContent:'center', gap:6, flexWrap:'wrap'}}>
          <TinyTokLogo sym={r.symbol} logo={r.logo}/>
          <span style={{fontWeight:700}}>{r.direction}</span>
          <span className="num">{fmt.num(Number(r.amount || 0), 2)}</span>
          <span style={{fontWeight:700}}>{r.symbol}</span>
          <span className="muted tiny">via {r.network || '—'}</span>
          {(r.sender || r.recipient) && (
            <span className="muted tiny" style={{display:'inline-flex', alignItems:'center', gap:4}}>
              {r.sender && <AddrOrName addr={r.sender} prefix={6} suffix={4} onClick={openWallet(r.sender)} style={{textDecoration:'underline dotted'}}/>}
              {r.sender && r.recipient && ' → '}
              {r.recipient && <AddrOrName addr={r.recipient} prefix={6} suffix={4} onClick={openWallet(r.recipient)} style={{textDecoration:'underline dotted'}}/>}
            </span>
          )}
        </div>
      );
    }
    return (
      <span style={{fontFamily:'JetBrains Mono', fontSize:12}}>
        <span style={{color:'#EC4899'}}>{r.section}</span>
        <span style={{color:'var(--fg-3)'}}>::</span>
        <span>{r.method}</span>
        {(r.success === 1 || r.success === true)
          ? <span className="tag ok tiny" style={{marginLeft:8}}>✓</span>
          : <span className="tag err tiny" style={{marginLeft:8}}>✗</span>}
      </span>
    );
  };

  const kindTitle = kind.charAt(0).toUpperCase() + kind.slice(1);

  return (
    <div className="card" style={{marginTop:14}}>
      <div className="card-header" style={{flexWrap:'wrap', gap:10}}>
        <div className="card-title"><span className="dot"/> {kindTitle} · actividad agregada</div>
        <div style={{display:'flex', gap:8, alignItems:'center', flexWrap:'wrap'}}>
          <div className="status-toggle">
            {[
              { id:'all', label: t('chip.all'),       n: (wallets?.length || 0) + (watched?.length || 0) },
              { id:'mis', label: t('scope.mine'),     n: wallets?.length || 0 },
              { id:'vig', label: t('scope.watched'),  n: watched?.length || 0 },
            ].map(o => (
              <button key={o.id}
                      className={'status-opt' + (scope === o.id ? ' active' : '')}
                      onClick={() => setScope(o.id)}>
                {o.label} <span className="muted tiny" style={{marginLeft:4}}>{o.n}</span>
              </button>
            ))}
          </div>
          <select
            value={walletFilter}
            onChange={e => setWalletFilter(e.target.value)}
            style={{padding:'6px 10px', border:'1px solid var(--border-color)', borderRadius:8, background:'var(--bg-card)', color:'var(--fg-0)', fontSize:13, cursor:'pointer'}}
            title="Filtrar por wallet concreta">
            <option value="all">Todas las wallets</option>
            {activeSet.map(w => <option key={w.addr} value={w.addr}>{w.alias}</option>)}
          </select>
          <span className="tag">{rows === null ? '…' : filteredRows.length + ' rows'}</span>
        </div>
      </div>
      {/* Full-width table with evenly-distributed columns (percentage widths +
          tableLayout:fixed). Detail content is centered within its column so
          it doesn't cling to the left edge on wide rows. */}
      <div className="swaps-table-wrap" style={{width:'100%'}}>
        <table className="swaps-table" style={{width:'100%', tableLayout:'fixed'}}>
          <colgroup>
            <col style={{width:'16%'}}/>
            <col style={{width:'20%'}}/>
            <col style={{width:'50%'}}/>
            <col style={{width:'14%'}}/>
          </colgroup>
          <thead>
            <tr>
              <th style={{paddingLeft:20, whiteSpace:'nowrap'}}>Hora / Bloque</th>
              <th style={{whiteSpace:'nowrap'}}>Wallet</th>
              <th style={{textAlign:'center'}}>Detalle</th>
              <th style={{textAlign:'right', paddingRight:20, whiteSpace:'nowrap'}}>USD</th>
            </tr>
          </thead>
          <tbody>
            {rows === null && (
              <tr><td colSpan={4} style={{padding:28, textAlign:'center', color:'var(--fg-2)'}}>Cargando actividad…</td></tr>
            )}
            {rows && filteredRows.length === 0 && (
              <tr><td colSpan={4} style={{padding:28, textAlign:'center', color:'var(--fg-2)'}}>Sin {kind} recientes para este filtro.</td></tr>
            )}
            {filteredRows.map((r, i) => {
              const block = r.block || (r.extrinsic_id ? String(r.extrinsic_id).split('-')[0] : '');
              const ts = (() => {
                if (r.timestamp) { const n = Number(r.timestamp); return n < 1e12 ? n * 1000 : n; }
                return parseHistTime(r.time);
              })();
              const usd = rowUsd(r);
              return (
                <tr key={(r.hash || '') + '-' + r.__walletAddr + '-' + i}>
                  <td style={{paddingLeft:20, whiteSpace:'nowrap'}}>
                    <div className="num tiny" style={{fontWeight:700}}>#{block}</div>
                    <div className="muted tiny">{fmt.ago(ts)} · {new Date(ts).toLocaleDateString()}</div>
                  </td>
                  <td
                    className="clickable"
                    style={{cursor:'pointer', overflow:'hidden'}}
                    onClick={() => window.openWalletDetails?.(r.__walletAddr, r.__walletAlias)}
                    title="Open wallet details">
                    <div style={{fontWeight:700, fontSize:12, whiteSpace:'nowrap', overflow:'hidden', textOverflow:'ellipsis'}}>{r.__walletAlias}</div>
                    <div className="muted tiny num" style={{textDecoration:'underline dotted', marginTop:2, whiteSpace:'nowrap'}}>
                      <span style={{marginRight:6, padding:'1px 5px', borderRadius:4, background: r.__walletKind === 'mis' ? 'rgba(16,185,129,0.15)' : 'rgba(96,165,250,0.15)', color: r.__walletKind === 'mis' ? '#10B981' : '#60A5FA', fontSize:10, fontWeight:700}}>
                        {r.__walletKind === 'mis' ? t('badge.mine') : t('badge.watched')}
                      </span>
                      {fmt.addr(r.__walletAddr, 5, 4)}
                    </div>
                  </td>
                  <td>{renderDetail(r)}</td>
                  <td style={{textAlign:'right', paddingRight:20}} className="num">
                    {usd > 0 ? <span style={{fontWeight:600}}>${usd.toFixed(2)}</span> : <span className="muted tiny">—</span>}
                  </td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
    </div>
  );
}

Object.assign(window, {
  TransfersSection, BridgesSection, OrderBookSection, PoolsSection,
  TokensSection, HoldersSection, StakingSection, GovSection,
  BalanceSection,
  // IntelligenceSection lives in js/intelligence.jsx — no re-export here.
  // Shared helpers for other sections (Pulse/Burns/etc) that want the same pills.
  TimeRangePills, useTimeRange, TIME_RANGES,
  // Shared compact KPI card row used from swaps.jsx etc. No import system
  // (in-browser babel), so the global bus is how foreign files see it.
  KpiGrid, TokenBadge,
});
