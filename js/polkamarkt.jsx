/* global React, useT, fmt, PageHeader, KpiGrid, TokenBadge, useDrill, useWallets, io */
// Prediction Markets section — mirrors the `polkamarkt` pallet.
// Until the SORA runtime upgrades to 4.8.x on-chain, the pallet isn't
// active; the backend /polkamarkt/state returns { available:false }. In
// that case we render a "Coming soon" card with the reason so the sidebar
// entry doesn't take the user to an empty page.
const { useState, useEffect, useMemo } = React;

// Thin pill shown when the pallet is not yet on-chain. Visible in the
// sidebar (via main.jsx) so users see the feature exists and is arriving.
// Minimal "Coming soon" card — no technical reason text, per user request.
// The backend's feature-detection details live in /polkamarkt/state for
// anyone poking the API, but the UI stays clean.
const COMING_SOON = () => (
  <div style={{
    margin: '28px auto', maxWidth: 560,
    padding: '36px 24px', textAlign: 'center',
    background: 'rgba(96,165,250,0.05)',
    border: '1px solid rgba(96,165,250,0.2)',
    borderRadius: 14,
  }}>
    <div style={{fontSize: 42, marginBottom: 14, opacity: 0.7}}>🔮</div>
    <div style={{fontSize: 20, fontWeight: 700, marginBottom: 8}}>Polkamarkt</div>
    <div style={{
      fontSize: 14, fontWeight: 600, color: '#60A5FA',
      letterSpacing: '0.15em', textTransform: 'uppercase',
    }}>Soooon</div>
  </div>
);

// Resolve a market's YES probability + the basis used, in priority order:
//   1. Resolved        → the settled outcome (100% to the winner).
//   2. implied_*_bps    → the real market-implied probability from the spec-130
//                         DynamicPariMutuel runtime API (polkamarktAPI.marketState).
//   3. coll_yes/coll_no → fallback collateral split (money on each side) for
//                         markets with no live pricing (legacy, or API miss).
function pmYesProbability(m) {
  if (m.status === 'Resolved' && (m.resolution === 'Yes' || m.resolution === 'No')) {
    return { pct: m.resolution === 'Yes' ? 100 : 0, basis: 'resolved' };
  }
  const iy = Number(m.implied_yes_bps) || 0;
  const ino = Number(m.implied_no_bps) || 0;
  if (iy + ino > 0) return { pct: (iy / (iy + ino)) * 100, basis: 'implied' };
  const cy = Math.max(0, Number(m.coll_yes) || 0);
  const cn = Math.max(0, Number(m.coll_no) || 0);
  if (cy + cn > 0) return { pct: (cy / (cy + cn)) * 100, basis: 'collateral' };
  return { pct: 50, basis: 'none' };
}

// YES/NO bar. Uses the real market-implied probability when the chain exposes it
// (DPM markets), the settled outcome for resolved markets, else the collateral
// split. Green = YES, red = NO.
function ProbBar({ m }) {
  const { pct, basis } = pmYesProbability(m);
  const yesPct = Math.max(0, Math.min(100, pct));
  const title = basis === 'resolved'
    ? `Resolved: ${m.resolution}`
    : `YES ${yesPct.toFixed(1)}% · NO ${(100 - yesPct).toFixed(1)}% (${basis === 'implied' ? 'market-implied' : 'by collateral'})`;
  return (
    <div title={title}
         style={{position:'relative', height:6, borderRadius:3, overflow:'hidden', background:'#EF4444', minWidth: 60}}>
      <div style={{position:'absolute', inset:0, width: yesPct + '%', background:'#10B981'}}/>
    </div>
  );
}

// Status chip with Polkamarkt vocabulary.
function StatusChip({ status, resolution }) {
  const map = {
    Open:      { label: 'Open',           cls: 'ok'  },
    Locked:    { label: '🔒 Locked',       cls: ''    },
    Resolved:  { label: '✓ Resolved',     cls: 'ok'  },
    Cancelled: { label: '✗ Cancelled',    cls: 'err' },
  };
  const m = map[status] || { label: status, cls: '' };
  const extra = status === 'Resolved' && resolution ? ' ' + resolution : '';
  return <span className={'tag ' + m.cls} style={{fontSize:10}}>{m.label + extra}</span>;
}

// Settlement mechanism badge — DPM (DynamicPariMutuel, 4.8.8) vs Legacy
// (MigratedLegacy, pre-rewrite markets carried over by the migration).
function MechBadge({ mechanism }) {
  if (!mechanism) return null;
  const dpm = mechanism === 'DynamicPariMutuel';
  return <span className="tag" style={{fontSize:9, marginLeft:6, opacity:0.75}}
    title={dpm ? 'Dynamic Pari-Mutuel (4.8.8)' : 'Migrated legacy market'}>{dpm ? 'DPM' : 'Legacy'}</span>;
}

function fmtNative(raw, displayDecimals = 4) {
  if (raw == null) return '—';
  const s = String(raw);
  if (s === '0' || s === '') return '0';
  try {
    const big = BigInt(s);
    // 1e18 as a BigInt literal — do NOT use the exponentiation operator here.
    // The in-browser Babel "env" preset rewrites that operator to Math.pow(),
    // which throws on BigInt ("Cannot convert a BigInt value to a number"),
    // sending every large amount to the catch and rendering it as a dash.
    const div = 1000000000000000000n;
    const intPart = big / div;
    const fracPart = big % div;
    if (fracPart === 0n) return intPart.toLocaleString('en-US');
    const fracStr = String(fracPart).padStart(18, '0').slice(0, displayDecimals).replace(/0+$/, '');
    return intPart.toLocaleString('en-US') + (fracStr ? '.' + fracStr : '');
  } catch {
    return '—';
  }
}

// Signed amount for P&L — "+x" green / "-x" red. raw may be a negative string.
function fmtSigned(raw) {
  const s = String(raw == null ? '' : raw);
  if (!s || s === '0') return '0';
  const neg = s.startsWith('-');
  return (neg ? '-' : '+') + fmtNative(neg ? s.slice(1) : s);
}

function usePolkamarktBuybacks() {
  const [data, setData] = useState(null);
  useEffect(() => {
    let cancelled = false;
    const pull = () => fetch('/polkamarkt/buybacks?limit=10')
      .then(r => r.ok ? r.json() : null)
      .then(j => { if (!cancelled && j) setData(j); })
      .catch(() => {});
    pull();
    const id = setInterval(pull, 30_000);
    return () => { cancelled = true; clearInterval(id); };
  }, []);
  return data;
}

// Hook — shared fetch with feature detection. Components call this once and
// get both availability + payload.
function usePolkamarktState() {
  const [state, setState] = useState(null);
  useEffect(() => {
    let cancelled = false;
    const pull = () => fetch('/polkamarkt/state')
      .then(r => r.ok ? r.json() : null)
      .then(j => { if (!cancelled && j) setState(j); })
      .catch(() => {});
    pull();
    const id = setInterval(pull, 60_000);
    return () => { cancelled = true; clearInterval(id); };
  }, []);
  return state;
}

function BuybackCard({ tt }) {
  const data = usePolkamarktBuybacks();
  if (!data) {
    return (
      <div className="card" style={{marginTop: 16}}>
        <div className="card-header"><div className="card-title"><span className="dot"/> {tt('predict.buyback.title', 'XOR buyback pool')}</div></div>
        <div className="card-body muted tiny" style={{padding: 16, textAlign:'center'}}>{tt('common.loading', 'Loading…')}</div>
      </div>
    );
  }
  const stats = data.stats || { sweepCount: 0, kusdSpentTotal: '0', xorBurnedTotal: '0', lastTs: null, lastBlock: null };
  const history = Array.isArray(data.history) ? data.history : [];
  return (
    <div className="card" style={{marginTop: 16}}>
      <div className="card-header">
        <div className="card-title">
          <span className="dot" style={{background:'#F59E0B'}}/> {tt('predict.buyback.title', 'XOR buyback pool')}
        </div>
        <span className="tag" style={{fontSize: 10}}>{tt('predict.buyback.tag', 'permissionless sweep')}</span>
      </div>
      <div className="card-body">
        <KpiGrid items={[
          { label: tt('predict.buyback.pending',    'KUSD pending'),   value: fmtNative(data.pendingKusd) + ' KUSD',   sub: tt('predict.buyback.pendingSub', 'awaiting next sweep') },
          { label: tt('predict.buyback.xorBurned',  'XOR burned'),     value: fmtNative(stats.xorBurnedTotal) + ' XOR', sub: tt('predict.buyback.xorBurnedSub', 'all sweeps cumulative') },
          { label: tt('predict.buyback.kusdSpent',  'KUSD swept'),     value: fmtNative(stats.kusdSpentTotal) + ' KUSD',sub: tt('predict.buyback.kusdSpentSub', 'all sweeps cumulative') },
          { label: tt('predict.buyback.sweeps',     'Sweeps'),         value: stats.sweepCount.toLocaleString(),       sub: stats.lastTs ? fmt.ago(stats.lastTs) + ' ' + tt('predict.buyback.ago', 'ago') : tt('predict.buyback.never', 'never executed') },
        ]}/>
        <div className="muted tiny" style={{marginTop: 10, lineHeight: 1.5}}>
          {tt('predict.buyback.help', 'Trade fees accrue as KUSD here. Anyone can call sweep_xor_buyback_and_burn to drain the pool — the KUSD is swapped to XOR via BuyBackHandler and burned.')}
        </div>
        {history.length > 0 && (
          <div style={{marginTop: 14}}>
            <div className="muted tiny" style={{textTransform:'uppercase', letterSpacing:'0.06em', marginBottom: 6}}>{tt('predict.buyback.recent', 'Recent sweeps')}</div>
            <div className="swaps-table-wrap">
              <table className="swaps-table">
                <thead>
                  <tr>
                    <th style={{paddingLeft: 12}}>{tt('predict.buyback.when', 'When')}</th>
                    <th>{tt('predict.buyback.block', 'Block')}</th>
                    <th style={{textAlign:'right'}}>{tt('predict.buyback.kusdSpent', 'KUSD swept')}</th>
                    <th style={{textAlign:'right', paddingRight: 12}}>{tt('predict.buyback.xorBurned', 'XOR burned')}</th>
                  </tr>
                </thead>
                <tbody>
                  {history.map((h, i) => (
                    <tr key={i}>
                      <td style={{paddingLeft: 12}} className="tiny muted" title={fmt.fullDate(h.ts)}>{fmt.ago(h.ts)}</td>
                      <td className="num tiny muted">#{h.block.toLocaleString()}</td>
                      <td style={{textAlign:'right'}} className="num">{fmtNative(h.kusdSpent)}</td>
                      <td style={{textAlign:'right', paddingRight: 12, color:'#F59E0B', fontWeight:600}} className="num">{fmtNative(h.xorBurned)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>
        )}
      </div>
    </div>
  );
}

function PolkamarktSection() {
  const t = useT();
  const { open: openDrill } = useDrill();
  const state = usePolkamarktState();
  const [statusFilter, setStatusFilter] = useState('all');
  const [page, setPage] = useState(1);
  const [markets, setMarkets] = useState({ data: [], total: 0, totalPages: 1 });
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    if (state && state.available === false) { setLoading(false); return; }
    let cancelled = false;
    setLoading(true);
    const q = new URLSearchParams({ page: String(page), limit: '25' });
    if (statusFilter !== 'all') q.set('status', statusFilter);
    fetch('/polkamarkt/markets?' + q.toString())
      .then(r => r.ok ? r.json() : null)
      .then(j => {
        if (cancelled || !j) return;
        setMarkets({
          data: Array.isArray(j.data) ? j.data : [],
          total: Number(j.total) || 0,
          totalPages: Number(j.totalPages) || 1,
        });
        setLoading(false);
      })
      .catch(() => setLoading(false));
    return () => { cancelled = true; };
  }, [state, statusFilter, page]);

  // While checking feature flag, render a minimal skeleton (not the full
  // "Coming soon" card — avoids flashing it when the pallet IS available).
  if (state === null) {
    return (
      <div>
        <PageHeader title={t('predict.title', 'Polkamarkt')} sub={t('predict.sub', 'Prediction markets on SORA')}/>
        <div className="muted tiny" style={{padding: 24, textAlign: 'center'}}>{t('common.loading', 'Loading…')}</div>
      </div>
    );
  }

  if (state.available === false) {
    return (
      <div>
        <PageHeader title={t('predict.title', 'Polkamarkt')} sub={t('predict.sub', 'Prediction markets on SORA')}/>
        {COMING_SOON()}
      </div>
    );
  }

  const totals = state.totals || { markets: 0, active: 0, resolved: 0, volume: '0', collaterals: [] };
  const tabData = markets.data;

  // Cumulative volume is a raw (1e18) sum of trade collateral. Label it with a
  // token symbol only when every market shares one collateral — summing raw
  // amounts across different tokens would be meaningless, so we drop the symbol.
  const volSym = Array.isArray(totals.collaterals) && totals.collaterals.length === 1
    ? pmAssetSym(totals.collaterals[0]) : '';

  return (
    <div>
      <PageHeader title={t('predict.title', 'Polkamarkt')} sub={t('predict.sub', 'Prediction markets on SORA')}>
        <span className="tag ok" style={{fontSize: 10}}>live</span>
      </PageHeader>

      <KpiGrid items={[
        { label: t('predict.kpi.total', 'Markets'),       value: totals.markets.toLocaleString(),        sub: 'all-time' },
        { label: t('predict.kpi.active', 'Active'),       value: totals.active.toLocaleString(),         sub: 'currently open' },
        { label: t('predict.kpi.volume', 'Trading vol.'), value: fmtNative(totals.volume) + (volSym ? ' ' + volSym : ''), sub: 'cumulative' },
        { label: t('predict.kpi.resolved', 'Resolved'), value: (totals.resolved || 0).toLocaleString(), sub: 'settled' },
      ]}/>

      <BuybackCard tt={t}/>

      <div style={{display:'flex', gap: 8, marginTop: 16, marginBottom: 12, alignItems:'center'}}>
        <span className="muted tiny" style={{textTransform:'uppercase', letterSpacing:'0.06em'}}>{t('predict.tab.markets', 'All Markets')}</span>
        <div style={{flex: 1}}/>
        <div className="status-toggle">
          {['all', 'Open', 'Locked', 'Resolved', 'Cancelled'].map(s => (
            <button key={s} className={'status-opt' + (statusFilter === s ? ' active' : '')}
              onClick={() => { setStatusFilter(s); setPage(1); }}>
              {s === 'all' ? t('chip.all', 'All') : s}
            </button>
          ))}
        </div>
      </div>

      <div className="card" style={{marginTop: 8}}>
        <div className="swaps-table-wrap">
          <table className="swaps-table">
            <thead>
              <tr>
                <th style={{paddingLeft: 16, width: 48}}>#</th>
                <th>{t('predict.col.question', 'Question')}</th>
                <th style={{width: 110}}>{t('predict.col.prob', 'Prob · YES')}</th>
                <th style={{textAlign:'right', width: 90}}>{t('predict.col.volume', 'Volume')}</th>
                <th style={{width: 110}}>{t('predict.col.status', 'Status')}</th>
                <th style={{width: 100}}>{t('predict.col.close', 'Close')}</th>
                <th style={{width: 110}}>{t('predict.col.creator', 'Creator')}</th>
                <th style={{paddingRight: 16, width: 30}}></th>
              </tr>
            </thead>
            <tbody>
              {loading && (
                <tr><td colSpan={8} style={{padding: 24, textAlign: 'center'}} className="muted tiny">
                  {t('common.loading', 'Loading…')}
                </td></tr>
              )}
              {!loading && tabData.length === 0 && (
                <tr><td colSpan={8} style={{padding: 24, textAlign: 'center'}} className="muted tiny">
                  {t('predict.noMarkets', 'No markets have been created yet.')}
                </td></tr>
              )}
              {!loading && tabData.map(m => (
                <tr key={m.market_id} className="swap-row clickable"
                    onClick={() => openDrill({ type: 'polkamarkt', title: '#' + m.market_id + ' · ' + (m.question || 'Untitled'), ...m })}>
                  <td style={{paddingLeft: 16, fontWeight: 700, color: 'var(--accent)'}} className="num">#{m.market_id}</td>
                  <td style={{maxWidth: 400, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap'}}>
                    {m.question || <span className="muted tiny">(condition #{m.condition_id})</span>}
                  </td>
                  <td><ProbBar m={m}/></td>
                  <td style={{textAlign: 'right'}} className="num">{fmtNative(m.volume)} <span className="muted tiny">{pmAssetSym(m.collateral_asset)}</span></td>
                  <td><StatusChip status={m.status} resolution={m.resolution}/><MechBadge mechanism={m.mechanism}/></td>
                  <td className="num tiny muted">#{m.close_block != null ? m.close_block.toLocaleString() : '—'}</td>
                  <td>
                    <span className="num tiny muted">{m.creator ? fmt.addr(m.creator, 5, 4) : '—'}</span>
                  </td>
                  <td style={{paddingRight: 16}}>↗</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  );
}

// ----- Portfolio / Wallet drill: polkamarkt positions table -----
// Rendered from PortfolioSection (shell.jsx → routes.jsx) underneath the
// Staking / LP rows, and also inside the WalletDetailsModal. It pulls
// /polkamarkt/positions/:addr which returns aggregated YES/NO shares + net
// collateral per market for that address.
function PolkamarktPositions({ addr, title }) {
  const t = useT();
  const [data, setData] = useState(null);
  useEffect(() => {
    if (!addr) return;
    let cancelled = false;
    fetch('/polkamarkt/positions/' + encodeURIComponent(addr))
      .then(r => r.ok ? r.json() : null)
      .then(j => { if (!cancelled && j) setData(j); })
      .catch(() => {});
    return () => { cancelled = true; };
  }, [addr]);

  // Hide entirely when pallet not active — no noise in the user's portfolio
  // if prediction markets don't exist on-chain yet.
  if (!data || data.available === false) return null;
  const positions = Array.isArray(data.positions) ? data.positions : [];
  if (positions.length === 0) return null;

  return (
    <div className="card" style={{marginTop: 14}}>
      <div className="card-header">
        <div className="card-title"><span className="dot"/> {title || t('predict.positions.title', 'Prediction Market Positions')}</div>
        <span className="tag">{positions.length}</span>
      </div>
      <div className="swaps-table-wrap">
        <table className="swaps-table">
          <thead>
            <tr>
              <th style={{paddingLeft: 16}}>{t('predict.col.market', 'Market')}</th>
              <th style={{textAlign:'right'}}>{t('predict.col.yes', 'YES shares')}</th>
              <th style={{textAlign:'right'}}>{t('predict.col.no', 'NO shares')}</th>
              <th style={{textAlign:'right'}}>{t('predict.col.netPaid', 'Net paid')}</th>
              <th style={{paddingRight: 16}}>{t('predict.col.status', 'Status')}</th>
            </tr>
          </thead>
          <tbody>
            {positions.map(p => (
              <tr key={p.market_id}>
                <td style={{paddingLeft: 16, maxWidth: 320, overflow:'hidden', textOverflow:'ellipsis', whiteSpace:'nowrap'}}>
                  <span style={{color:'var(--accent)', fontWeight:700}}>#{p.market_id}</span>{' '}
                  <span className="muted tiny">{p.question || '(untitled)'}</span>
                </td>
                <td style={{textAlign:'right'}} className="num">{fmt.num(Number(p.yes_shares) || 0, 2)}</td>
                <td style={{textAlign:'right'}} className="num">{fmt.num(Number(p.no_shares) || 0, 2)}</td>
                <td style={{textAlign:'right'}} className="num">{fmt.usd(Number(p.net_collateral) || 0)}</td>
                <td style={{paddingRight: 16}}><StatusChip status={p.status} resolution={p.resolution}/></td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}

// Native collateral asset → symbol. Markets are collateralised in a native
// SORA asset (XOR in practice); map the common ones, truncate anything else.
const PM_ASSET_SYM = {
  '0x0200000000000000000000000000000000000000000000000000000000000000': 'XOR',
  '0x0200040000000000000000000000000000000000000000000000000000000000': 'VAL',
  '0x0200050000000000000000000000000000000000000000000000000000000000': 'PSWAP',
  '0x0200080000000000000000000000000000000000000000000000000000000000': 'XSTUSD',
  '0x02000c0000000000000000000000000000000000000000000000000000000000': 'KUSD',
};
function pmAssetSym(id) {
  if (!id || typeof id !== 'string') return '—';
  if (PM_ASSET_SYM[id]) return PM_ASSET_SYM[id];
  return id.startsWith('0x') && id.length > 14 ? id.slice(0, 8) + '…' + id.slice(-4) : id;
}

// Drill panel for a single market — opened from the markets table (type
// 'polkamarkt'). Receives the clicked row, then fetches /polkamarkt/market/:id
// for the rich detail (oracle, resolution source, recent trades, top positions,
// OpenGov link). Wired into the shared drill modal via drill_music.jsx.
function PolkamarktDrill({ market }) {
  const t = useT();
  const m0 = market || {};
  const id = m0.market_id;
  const [detail, setDetail] = useState(null);
  useEffect(() => {
    if (id == null) return;
    let cancelled = false;
    fetch('/polkamarkt/market/' + encodeURIComponent(id))
      .then(r => r.ok ? r.json() : null)
      .then(j => { if (!cancelled && j && j.market) setDetail(j); })
      .catch(() => {});
    return () => { cancelled = true; };
  }, [id]);

  const m = (detail && detail.market) || m0;
  const trades = (detail && Array.isArray(detail.recentTrades)) ? detail.recentTrades : [];
  const positions = (detail && Array.isArray(detail.topPositions)) ? detail.topPositions : [];
  // Enriched (spec-130 on-chain): per-trader P&L, creator commission, liquidity.
  const richPositions = (detail && Array.isArray(detail.positions)) ? detail.positions : [];
  const creator = (detail && detail.creator) || null;
  const liquidity = (detail && detail.liquidity) || null;
  // Probability — market-implied (DPM) when the chain exposes it, the settled
  // outcome for resolved markets, else the collateral split. Same basis as the
  // list bar (pmYesProbability).
  const { pct: probYes, basis: probBasis } = pmYesProbability(m);

  const KV = ({ k, children }) => (
    <div className="drill-kv"><span className="drill-k">{k}</span><span className="drill-v">{children}</span></div>
  );

  return (
    <div className="drill-head-wrap">
      <div className="drill-head">
        <div className="drill-title" style={{fontSize: 15, lineHeight: 1.4}}>
          <span style={{color:'var(--accent)', fontWeight:800}}>#{m.market_id}</span> {m.question || ('Condition #' + (m.condition_id ?? '?'))}
        </div>
        <div style={{marginTop: 8}}><StatusChip status={m.status} resolution={m.resolution}/></div>
      </div>

      {/* Outcome probability + basis + share counts below */}
      <div className="drill-section">
        <div style={{display:'flex', justifyContent:'space-between', fontSize:12, fontWeight:700, marginBottom:6}}>
          <span style={{color:'#10B981'}}>{t('predict.yes', 'YES')} {probYes.toFixed(1)}%</span>
          <span style={{color:'#EF4444'}}>{t('predict.no', 'NO')} {(100 - probYes).toFixed(1)}%</span>
        </div>
        <div style={{position:'relative', height:10, borderRadius:5, overflow:'hidden', background:'#EF4444'}}>
          <div style={{position:'absolute', inset:0, width: probYes + '%', background:'#10B981'}}/>
        </div>
        <div className="muted tiny" style={{marginTop:6}}>
          {probBasis === 'resolved'
            ? <span>{t('predict.drill.settled', 'settled outcome')}: <b>{m.resolution}</b></span>
            : probBasis === 'implied'
              ? <span>{t('predict.drill.implied', 'market-implied')} · {t('predict.drill.price', 'price')} {(Number(m.marginal_yes_bps) / 100).toFixed(2)}% / {(Number(m.marginal_no_bps) / 100).toFixed(2)}%</span>
              : <span>{t('predict.drill.byMoney', 'by collateral')}: {fmtNative(m.coll_yes)} / {fmtNative(m.coll_no)} {pmAssetSym(m.collateral_asset)}</span>}
          {' · '}{fmtNative(m.yes_shares)} {t('predict.col.yes', 'YES shares')} · {fmtNative(m.no_shares)} {t('predict.col.no', 'NO shares')}
        </div>
      </div>

      {/* Market metadata */}
      <div className="drill-section">
        <KV k={t('predict.drill.volume', 'Volume')}>{fmtNative(m.volume)} {pmAssetSym(m.collateral_asset)}</KV>
        <KV k={t('predict.drill.collateral', 'Collateral')}>{pmAssetSym(m.collateral_asset)}</KV>
        {m.mechanism && <KV k={t('predict.drill.mechanism', 'Mechanism')}>{m.mechanism === 'DynamicPariMutuel' ? 'Dynamic Pari-Mutuel' : 'Migrated legacy'}</KV>}
        <KV k={t('predict.drill.seed', 'Seed liquidity')}>{fmtNative(m.seed_liquidity)} {pmAssetSym(m.collateral_asset)}</KV>
        <KV k={t('predict.drill.creator', 'Creator')}>{m.creator ? fmt.addr(m.creator, 8, 6) : '—'}</KV>
        <KV k={t('predict.drill.oracle', 'Oracle')}>{m.oracle || '—'}</KV>
        <KV k={t('predict.drill.source', 'Resolution source')}>{m.resolution_source || '—'}</KV>
        <KV k={t('predict.drill.close', 'Close block')}>{m.close_block != null ? '#' + Number(m.close_block).toLocaleString() : '—'}</KV>
        <KV k={t('predict.drill.condition', 'Condition')}>#{m.condition_id ?? '—'}</KV>
        {m.created_at_block != null && <KV k={t('predict.drill.created', 'Created')}>#{Number(m.created_at_block).toLocaleString()}{m.created_at_ts ? ' · ' + fmt.ago(Number(m.created_at_ts)) : ''}</KV>}
      </div>

      {/* Creator earnings */}
      {creator && (
        <div className="drill-section">
          <div className="muted tiny" style={{textTransform:'uppercase', letterSpacing:'0.06em', marginBottom:6}}>{t('predict.drill.creatorTitle', 'Creator')}</div>
          <KV k={t('predict.drill.creatorAddr', 'Address')}>{creator.address ? fmt.addr(creator.address, 8, 6) : '—'}</KV>
          <KV k={t('predict.drill.creatorFees', 'Commission earned')}>{fmtNative(creator.feesRaw)} {pmAssetSym(m.collateral_asset)}</KV>
          <KV k={t('predict.drill.feeRate', 'Trade fee')}>{(Number(creator.feeBps || 0) / 100).toFixed(2)}%</KV>
        </div>
      )}

      {/* Liquidity & pool */}
      {liquidity && (
        <div className="drill-section">
          <div className="muted tiny" style={{textTransform:'uppercase', letterSpacing:'0.06em', marginBottom:6}}>{t('predict.drill.liquidityTitle', 'Liquidity & pool')}</div>
          <KV k={t('predict.drill.pool', 'Pool (DPM collateral)')}>{liquidity.dpmCollateral != null ? fmtNative(liquidity.dpmCollateral) : '—'} {pmAssetSym(m.collateral_asset)}</KV>
          {liquidity.providers && liquidity.providers.length > 0 ? (
            <div style={{marginTop:8}}>
              <div className="muted tiny" style={{marginBottom:4}}>{liquidity.providers.length} {t('predict.drill.lps', 'liquidity providers')}</div>
              <div className="swaps-table-wrap">
                <table className="swaps-table">
                  <thead><tr>
                    <th style={{paddingLeft:8}}>{t('predict.drill.provider', 'Provider')}</th>
                    <th style={{textAlign:'right', paddingRight:8}}>{t('predict.drill.contributed', 'Contributed')}</th>
                  </tr></thead>
                  <tbody>
                    {liquidity.providers.map((lp, i) => (
                      <tr key={i}>
                        <td style={{paddingLeft:8}} className="tiny num muted">{fmt.addr(lp.account, 6, 4)}</td>
                        <td style={{textAlign:'right', paddingRight:8}} className="num">{fmtNative(lp.contributed)} {pmAssetSym(m.collateral_asset)}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </div>
          ) : (
            <div className="muted tiny" style={{marginTop:6}}>{t('predict.drill.noLps', 'No external liquidity providers yet — pool is seed-funded.')}</div>
          )}
        </div>
      )}

      {/* Recent trades */}
      {trades.length > 0 && (
        <div className="drill-section">
          <div className="muted tiny" style={{textTransform:'uppercase', letterSpacing:'0.06em', marginBottom:6}}>{t('predict.drill.trades', 'Recent trades')}</div>
          <div className="swaps-table-wrap">
            <table className="swaps-table">
              <thead><tr>
                <th style={{paddingLeft: 8}}>{t('predict.drill.when', 'When')}</th>
                <th>{t('predict.drill.action', 'Action')}</th>
                <th style={{textAlign:'right'}}>{t('predict.drill.collateralCol', 'Collateral')}</th>
                <th style={{textAlign:'right', paddingRight: 8}}>{t('predict.drill.shares', 'Shares')}</th>
              </tr></thead>
              <tbody>
                {trades.map(tr => (
                  <tr key={tr.id}>
                    <td style={{paddingLeft: 8}} className="tiny muted" title={fmt.fullDate(Number(tr.ts))}>{fmt.ago(Number(tr.ts))}</td>
                    <td className="tiny"><span style={{fontWeight:700, color: tr.side === 'Buy' ? '#10B981' : '#EF4444'}}>{tr.side}</span> <span style={{color: tr.outcome === 'Yes' ? '#10B981' : '#EF4444'}}>{tr.outcome}</span></td>
                    <td style={{textAlign:'right'}} className="num">{fmtNative(tr.collateral)}</td>
                    <td style={{textAlign:'right', paddingRight: 8}} className="num">{fmtNative(tr.shares)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      )}

      {/* Positions & P&L */}
      {(richPositions.length > 0 || positions.length > 0) && (() => {
        const list = richPositions.length ? richPositions : positions.map(p => ({
          trader: p.trader, yes_shares: p.yes_shares, no_shares: p.no_shares,
          paid: p.net_collateral, value: null, pnl: null, basis: 'mark',
        }));
        const basis = list[0] && list[0].basis;
        const basisLabel = basis === 'settled' ? t('predict.drill.pnlSettled', 'realised · settled')
          : basis === 'refunded' ? t('predict.drill.pnlRefunded', 'refunded · cancelled')
          : t('predict.drill.pnlMark', 'unrealised · at market price');
        return (
        <div className="drill-section">
          <div className="muted tiny" style={{textTransform:'uppercase', letterSpacing:'0.06em', marginBottom:6}}>
            {t('predict.drill.positions', 'Positions & P&L')}
            <span style={{textTransform:'none', marginLeft:8, opacity:0.7}}>· {basisLabel}</span>
          </div>
          <div className="swaps-table-wrap">
            <table className="swaps-table">
              <thead><tr>
                <th style={{paddingLeft: 8}}>{t('predict.drill.trader', 'Trader')}</th>
                <th style={{textAlign:'right'}}>{t('predict.col.yes', 'YES shares')}</th>
                <th style={{textAlign:'right'}}>{t('predict.col.no', 'NO shares')}</th>
                <th style={{textAlign:'right'}}>{t('predict.drill.paid', 'Paid')}</th>
                <th style={{textAlign:'right'}}>{t('predict.drill.value', 'Value')}</th>
                <th style={{textAlign:'right', paddingRight: 8}}>{t('predict.drill.pnl', 'P&L')}</th>
              </tr></thead>
              <tbody>
                {list.map((p, i) => {
                  const neg = String(p.pnl || '').startsWith('-');
                  const isCreator = creator && creator.address && p.trader === creator.address;
                  return (
                  <tr key={i}>
                    <td style={{paddingLeft: 8}} className="tiny num muted">{fmt.addr(p.trader, 6, 4)}{isCreator ? <span className="tag" style={{fontSize:9, marginLeft:6}}>creator</span> : null}</td>
                    <td style={{textAlign:'right'}} className="num">{fmtNative(p.yes_shares)}</td>
                    <td style={{textAlign:'right'}} className="num">{fmtNative(p.no_shares)}</td>
                    <td style={{textAlign:'right'}} className="num">{fmtNative(p.paid)}</td>
                    <td style={{textAlign:'right'}} className="num">{p.value != null ? fmtNative(p.value) : '—'}</td>
                    <td style={{textAlign:'right', paddingRight: 8, fontWeight:600, color: p.pnl == null ? 'inherit' : (neg ? '#EF4444' : '#10B981')}} className="num">{p.pnl != null ? fmtSigned(p.pnl) : '—'}</td>
                  </tr>
                )})}
              </tbody>
            </table>
          </div>
          <div className="muted tiny" style={{marginTop:6}}>{t('predict.drill.pnlNote', 'Amounts in')} {pmAssetSym(m.collateral_asset)}.</div>
        </div>
        );
      })()}
    </div>
  );
}

Object.assign(window, { PolkamarktSection, PolkamarktPositions, PolkamarktDrill });
