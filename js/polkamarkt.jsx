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
    <div style={{fontSize: 20, fontWeight: 700, marginBottom: 8}}>Prediction Markets</div>
    <div style={{
      fontSize: 14, fontWeight: 600, color: '#60A5FA',
      letterSpacing: '0.15em', textTransform: 'uppercase',
    }}>Soooon</div>
  </div>
);

// Probability bar from pool reserves / cumulative deltas. prob_yes = no / (yes+no).
// Backing math: in a YES/NO LMSR-like AMM, the implied probability that YES
// wins is proportional to the NO-share reserves (the more NO shares the LP
// has sold, the more the market believes YES is likely).
function ProbBar({ yesShares, noShares }) {
  const y = Number(yesShares) || 0;
  const n = Number(noShares) || 0;
  const total = y + n;
  const probYes = total > 0 ? (n / total) * 100 : 50;
  return (
    <div title={`Yes ${probYes.toFixed(1)}%  · No ${(100 - probYes).toFixed(1)}%`}
         style={{position:'relative', height:6, borderRadius:3, overflow:'hidden', background:'#EF4444', minWidth: 60}}>
      <div style={{position:'absolute', inset:0, width: probYes + '%', background:'#10B981'}}/>
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

function PolkamarktSection() {
  const t = useT();
  const { open: openDrill } = useDrill();
  const state = usePolkamarktState();
  const [tab, setTab] = useState('markets'); // 'markets' | 'opengov'
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
        <PageHeader title={t('predict.title', 'Prediction Markets')}/>
        <div className="muted tiny" style={{padding: 24, textAlign: 'center'}}>{t('common.loading', 'Loading…')}</div>
      </div>
    );
  }

  if (state.available === false) {
    return (
      <div>
        <PageHeader title={t('predict.title', 'Prediction Markets')}/>
        {COMING_SOON()}
      </div>
    );
  }

  const totals = state.totals || { markets: 0, active: 0, volume: 0 };
  const filteredOpengov = markets.data.filter(m => m.opengov_network);
  const tabData = tab === 'opengov' ? filteredOpengov : markets.data;

  return (
    <div>
      <PageHeader title={t('predict.title', 'Prediction Markets')}>
        <span className="tag ok" style={{fontSize: 10}}>live</span>
      </PageHeader>

      <KpiGrid items={[
        { label: t('predict.kpi.total', 'Markets'),       value: totals.markets.toLocaleString(),        sub: 'all-time' },
        { label: t('predict.kpi.active', 'Active'),       value: totals.active.toLocaleString(),         sub: 'currently open' },
        { label: t('predict.kpi.volume', 'Trading vol.'), value: fmt.usd(totals.volume),                 sub: 'cumulative' },
        { label: t('predict.kpi.opengov', 'OpenGov-linked'), value: filteredOpengov.length.toLocaleString(), sub: 'referendum-linked' },
      ]}/>

      <div style={{display:'flex', gap: 8, marginTop: 16, marginBottom: 12}}>
        <button className={'status-opt' + (tab === 'markets' ? ' active' : '')} onClick={() => setTab('markets')}>
          {t('predict.tab.markets', 'All Markets')}
        </button>
        <button className={'status-opt' + (tab === 'opengov' ? ' active' : '')} onClick={() => setTab('opengov')}>
          {t('predict.tab.opengov', 'OpenGov-linked')}
        </button>
        <div style={{flex: 1}}/>
        {tab === 'markets' && (
          <div className="status-toggle">
            {['all', 'Open', 'Locked', 'Resolved', 'Cancelled'].map(s => (
              <button key={s} className={'status-opt' + (statusFilter === s ? ' active' : '')}
                onClick={() => { setStatusFilter(s); setPage(1); }}>
                {s === 'all' ? t('chip.all', 'All') : s}
              </button>
            ))}
          </div>
        )}
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
                  {tab === 'opengov'
                    ? t('predict.noOpengov', 'No referendum-linked markets yet.')
                    : t('predict.noMarkets', 'No markets have been created yet.')}
                </td></tr>
              )}
              {!loading && tabData.map(m => (
                <tr key={m.market_id} className="swap-row clickable"
                    onClick={() => openDrill({ type: 'polkamarkt', title: '#' + m.market_id + ' · ' + (m.question || 'Untitled'), ...m })}>
                  <td style={{paddingLeft: 16, fontWeight: 700, color: 'var(--accent)'}} className="num">#{m.market_id}</td>
                  <td style={{maxWidth: 400, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap'}}>
                    {m.question || <span className="muted tiny">(condition #{m.condition_id})</span>}
                  </td>
                  <td><ProbBar yesShares={m.yes_shares || 0} noShares={m.no_shares || 0}/></td>
                  <td style={{textAlign: 'right'}} className="num">{fmt.usd(Number(m.volume) || 0)}</td>
                  <td><StatusChip status={m.status} resolution={m.resolution}/></td>
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

Object.assign(window, { PolkamarktSection, PolkamarktPositions });
