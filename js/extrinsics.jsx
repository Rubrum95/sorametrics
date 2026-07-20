/* global React, fmt, FAKE_ADDRS, IDENTITIES, seededRand, useDrill, useT, ExportCsvButton */
const { useState, useMemo, useEffect } = React;

const PALLETS = [
  'currencies', 'liquidityProxy', 'orderBook', 'bridgeProxy', 'referrals',
  'staking', 'democracy', 'council', 'technicalCommittee', 'utility',
  'assets', 'vestedRewards',
];

const PALLET_METHODS = {
  currencies: ['transfer', 'transferNativeCurrency'],
  liquidityProxy: ['swap', 'swapTransfer', 'enableLiquiditySource'],
  orderBook: ['placeLimitOrder', 'cancelLimitOrder', 'executeMarketOrder'],
  bridgeProxy: ['transferIn', 'transferOut', 'addAsset'],
  referrals: ['reserve', 'unreserve', 'setReferrer'],
  staking: ['bond', 'unbond', 'nominate', 'chill', 'withdrawUnbonded'],
  democracy: ['propose', 'second', 'vote', 'removeVote'],
  council: ['propose', 'vote', 'close'],
  technicalCommittee: ['propose', 'vote'],
  utility: ['batch', 'batchAll', 'forceBatch'],
  assets: ['transfer', 'register', 'mint', 'burn'],
  vestedRewards: ['claimRewards', 'setAssetPair'],
};

const PALLET_COLORS = {
  currencies: '#60A5FA', liquidityProxy: '#EC4899', orderBook: '#F59E0B',
  bridgeProxy: '#10B981', referrals: '#8B5CF6', staking: '#E5243B',
  democracy: '#FBB040', council: '#A062B0', technicalCommittee: '#7B5B90',
  utility: '#64748B', assets: '#06B6D4', vestedRewards: '#14B8A6',
};

const FAIL_REASONS = [
  'BadOrigin: caller is not permitted to call this dispatchable',
  'InsufficientBalance: account balance too low to cover fee',
  'UnknownAssetId: asset 0x0200… is not registered',
  'SlippageTolerance: price moved beyond the allowed 0.5%',
  'AlreadyExists: referrer was already set for this account',
  'ArithmeticError::Overflow in liquidityProxy::swap',
];

function hash32() {
  const chars = '0123456789abcdef';
  let s = '0x';
  for (let i = 0; i < 64; i++) s += chars[Math.floor(Math.random() * 16)];
  return s;
}

function makeExtrinsic(id, rnd, now) {
  const pallet = PALLETS[Math.floor(rnd() * PALLETS.length)];
  const methods = PALLET_METHODS[pallet];
  const method = methods[Math.floor(rnd() * methods.length)];
  const caller = FAKE_ADDRS[Math.floor(rnd() * FAKE_ADDRS.length)];
  const feeXor = +(rnd() * 0.8 + 0.01).toFixed(4);
  const block = 21_418_000 + Math.floor(rnd() * 5000);
  const idx = Math.floor(rnd() * 80);
  const ok = rnd() > 0.15;
  return {
    id, pallet, method, caller, feeXor, block, idx, ok, ts: now,
    hash: hash32(),
    failReason: ok ? null : FAIL_REASONS[Math.floor(rnd() * FAIL_REASONS.length)],
  };
}

function argsFor(e) {
  const compact = (x) => JSON.stringify(x, null, 2);
  const samples = {
    'liquidityProxy.swap': {
      dex_id: 0,
      input_asset_id: '0x0200000000000000000000000000000000000000000000000000000000000000',
      output_asset_id: '0x0200080000000000000000000000000000000000000000000000000000000000',
      swap_amount: { WithDesiredInput: { desired_amount_in: '12400000000000000000', min_amount_out: '1200000000000000000' }},
      selected_source_types: ['XYKPool', 'MulticollateralBondingCurvePool'],
      filter_mode: 'Disabled',
    },
    'currencies.transfer': {
      dest: e.caller,
      currency_id: '0x0200000000000000000000000000000000000000000000000000000000000000',
      amount: '24800000000000000000',
    },
    'staking.bond': {
      controller: e.caller,
      value: '100000000000000000000',
      payee: 'Stash',
    },
    'orderBook.placeLimitOrder': {
      order_book_id: { dex_id: 0, base: 'XOR', quote: 'VAL' },
      price: '420000000000000000',
      amount: '15000000000000000000',
      side: 'Buy',
      lifespan: null,
    },
    'referrals.reserve': {
      balance: '10000000000000000000',
    },
    'utility.batchAll': {
      calls: '[2 nested calls: currencies.transfer, liquidityProxy.swap]',
    },
    'bridgeProxy.transferIn': {
      network_id: { EvmLegacy: 'Ethereum' },
      asset_id: '0x0200050000000000000000000000000000000000000000000000000000000000',
      recipient: e.caller,
      amount: '50000000000000000000',
    },
    'vestedRewards.claimRewards': {
      reward_reason: 'PresalePSwap',
    },
  };
  const key = e.pallet + '.' + e.method;
  return compact(samples[key] || { /* decoded args */ caller: e.caller, nonce: e.idx });
}

function eventsFor(e) {
  if (!e.ok) return [{ pallet: 'system', name: 'ExtrinsicFailed', color: '#EF4444' }];
  const base = [
    { pallet: 'system', name: 'NewAccount', color: '#64748B' },
    { pallet: e.pallet, name: capFirst(e.method), color: PALLET_COLORS[e.pallet] },
    { pallet: 'transactionPayment', name: 'TransactionFeePaid', color: '#10B981' },
    { pallet: 'system', name: 'ExtrinsicSuccess', color: '#10B981' },
  ];
  if (e.pallet === 'liquidityProxy') {
    base.splice(2, 0,
      { pallet: 'poolXYK', name: 'Exchange', color: '#EC4899' },
      { pallet: 'xorFee', name: 'FeeWithdrawn', color: '#F59E0B' });
  }
  if (e.pallet === 'staking') {
    base.splice(2, 0, { pallet: 'balances', name: 'Reserved', color: '#E5243B' });
  }
  return base;
}

function capFirst(s) { return s[0].toUpperCase() + s.slice(1); }

// Adaptive fee format. SORA's xorless model makes swap fees tiny (~0.00009 XOR),
// so a flat 4-decimal display would round them to "0.0001". Scale decimals to
// magnitude instead — never scientific notation.
function fmtFee(x) {
  if (x == null || !Number.isFinite(x)) return '—';
  const a = Math.abs(x);
  const d = a >= 1 ? 4 : a >= 0.01 ? 5 : a >= 0.0001 ? 6 : 8;
  return x.toLocaleString('en-US', { minimumFractionDigits: d, maximumFractionDigits: d });
}

// Renders the real fee for an expanded row from the shared blockFees entry the
// table already fetched (keyed by block). bf = { totalXor, totalUsd, rows }.
// rows===1 → that fee is exactly this extrinsic's. rows>1 → the block had several
// fee-paying txs, shown as a block total. undefined = loading, falsy = not indexed.
function ExtFee({ bf, signed }) {
  const t = useT();
  if (!signed) return <div className="muted tiny">{t('ext.feeUnsigned', 'Extrinsic no firmado · sin coste')}</div>;
  if (bf === undefined) return <div className="muted tiny">{t('ext.feeLoading', 'Cargando fee…')}</div>;
  if (!bf || !bf.totalXor) return <div className="muted tiny">{t('ext.feeNotIndexed', 'Fee aún no indexada para este bloque.')}</div>;
  const xorPrice = (typeof window !== 'undefined' && window.TOKEN_PRICES && window.TOKEN_PRICES.XOR) || 0;
  const usd = bf.totalUsd > 0 ? bf.totalUsd : (xorPrice ? bf.totalXor * xorPrice : 0);
  return (
    <div className="ext-fee-list">
      <div className="ext-fee-total"><span>{bf.rows > 1 ? t('ext.feeBlockTotal', 'Fee total del bloque') : t('ext.feePaid', 'Fee pagada')}</span><span className="num">{fmtFee(bf.totalXor)} XOR{usd > 0 ? ` · $${usd.toFixed(4)}` : ''}</span></div>
      {bf.rows > 1 && <div className="muted tiny" style={{marginTop:4}}>{t('ext.feeBlockNote', 'Este bloque tuvo {n} transacciones con coste.').replace('{n}', bf.rows)}</div>}
    </div>
  );
}

function ExtrinsicsSection({ tweaks }) {
  const t = useT();
  const { open } = useDrill();
  const [palletFilter, setPalletFilter] = useState(null);
  const [statusFilter, setStatusFilter] = useState('all');
  const [methodSearch, setMethodSearch] = useState('');
  const [methodDebounced, setMethodDebounced] = useState('');
  const [dateFilter, setDateFilter] = useState('');
  // Block filter — populated by the global search palette when the user picks
  // a block/tx/extrinsic result. Read once on mount from window.__SM_SEARCH_*
  // and then cleared so subsequent navigations don't re-apply stale values.
  const [blockFilter, setBlockFilter] = useState('');
  const [hashHighlight, setHashHighlight] = useState('');
  const [page, setPage] = useState(1);
  const [expanded, setExpanded] = useState(null);
  const [palletOpen, setPalletOpen] = useState(false);
  const [palletList, setPalletList] = useState(PALLETS);
  const [ext24h, setExt24h] = useState(null);   // { total, success, failed, successRate, topPallet, topPalletCount }
  const [fees24h, setFees24h] = useState(null);  // [{ type, total_xor, total_usd }]

  // Pick up search-palette handoff. The palette stashes identifiers on window
  // rather than re-rendering through React state; we consume them once.
  useEffect(() => {
    if (window.__SM_SEARCH_BLOCK__) {
      setBlockFilter(String(window.__SM_SEARCH_BLOCK__));
      setPage(1);
      delete window.__SM_SEARCH_BLOCK__;
    }
    if (window.__SM_SEARCH_HASH__) {
      setHashHighlight(String(window.__SM_SEARCH_HASH__));
      delete window.__SM_SEARCH_HASH__;
    }
    // Deep-link via URL: /sorav2?tab=extrinsics&block=<n> or &q=<hash>.
    // Used by the XOR migration table on /minamoto when the user clicks a v2
    // burn-tx hash or v2 block — we land them on the right pre-filtered view.
    try {
      const params = new URLSearchParams(window.location.search);
      const blockParam = params.get('block');
      const qParam = params.get('q');
      if (blockParam) { setBlockFilter(String(blockParam)); setPage(1); }
      if (qParam) {
        setHashHighlight(String(qParam));
        // Resolve the hash → block via /search so the block filter narrows the
        // page to exactly the extrinsic the deep-link is pointing at.
        fetch('/search?q=' + encodeURIComponent(qParam))
          .then(r => r.ok ? r.json() : null)
          .then(j => {
            if (j && j.type === 'extrinsic' && j.data && j.data.block != null) {
              setBlockFilter(String(j.data.block));
              setPage(1);
            }
          })
          .catch(() => {});
      }
    } catch (_) {}
  }, []);

  // Load dynamic pallet list from prod if available.
  useEffect(() => {
    let cancelled = false;
    fetch('/history/extrinsic-sections').then(r => r.ok ? r.json() : null).then(j => {
      if (cancelled) return;
      const arr = Array.isArray(j) ? j : (j?.sections || j?.data || null);
      if (Array.isArray(arr) && arr.length) setPalletList(arr.map(s => typeof s === 'string' ? s : (s.section || s.name)).filter(Boolean));
    }).catch(() => {});
    return () => { cancelled = true; };
  }, []);

  // Real network-wide KPIs: 24h extrinsic count + success + top pallet from the
  // dedicated endpoint, and 24h fee totals from /stats/fees. Both are computed
  // server-side over the full window, not over the page the table happens to show.
  useEffect(() => {
    let cancelled = false;
    const pull = () => {
      fetch('/stats/extrinsics-24h').then(r => r.ok ? r.json() : null).then(j => {
        if (!cancelled && j) setExt24h(j);
      }).catch(() => {});
      fetch('/stats/fees?timeframe=1d').then(r => r.ok ? r.json() : null).then(j => {
        if (!cancelled && Array.isArray(j)) setFees24h(j);
      }).catch(() => {});
    };
    pull();
    const id = setInterval(pull, 60_000);
    return () => { cancelled = true; clearInterval(id); };
  }, []);

  // Debounce method search ~500ms (matches v1).
  useEffect(() => {
    const id = setTimeout(() => { setMethodDebounced(methodSearch); setPage(1); }, 500);
    return () => clearTimeout(id);
  }, [methodSearch]);

  // Build endpoint with server-side filters.
  const extEndpoint = useMemo(() => {
    const q = new URLSearchParams();
    if (palletFilter) q.set('section', palletFilter);
    if (methodDebounced) q.set('method', methodDebounced);
    if (statusFilter === 'success') q.set('success', '1');
    else if (statusFilter === 'failed') q.set('success', '0');
    if (dateFilter) {
      const ts = new Date(dateFilter).getTime();
      if (Number.isFinite(ts)) q.set('timestamp', String(ts));
    }
    if (blockFilter && /^\d+$/.test(blockFilter)) q.set('block', blockFilter);
    return '/history/global/extrinsics' + (q.toString() ? '?' + q.toString() : '');
  }, [palletFilter, methodDebounced, statusFilter, dateFilter, blockFilter]);

  // Real extrinsics from prod /history/global/extrinsics. Shape mapping:
  //   prod { time, block, extrinsic_index, extrinsic_id, hash, section, method,
  //          signer, success (0/1), args_json, error_msg, events_json }
  // Server-side pagination — backend exposes /history/global/extrinsics
  // with total + totalPages (hundreds of thousands of rows). Pass the UI
  // page through so the backend returns the right slice directly.
  const pageSize = tweaks.density === 'compact' ? 12 : tweaks.density === 'spacious' ? 6 : 10;
  const { items: raw, total: backendTotal, totalPages: backendTotalPages, loading: histLoading, refresh } = useHistory(extEndpoint, { pageSize, page, pollMs: 20_000 });
  const items = useMemo(() => {
    if (!raw || raw.length === 0) return [];
    return raw.map((e, i) => ({
      // Include row index in id — prod occasionally returns duplicate
      // extrinsic_ids across the paginated window (MV + live_extrinsics union)
      // and React needs unique keys.
      id: 'x-' + (e.extrinsic_id || (e.block + ':' + e.extrinsic_index)) + '-' + i,
      pallet: e.section,
      method: e.method,
      caller: e.signer,
      block: e.block,
      idx: e.extrinsic_index,
      ok: e.success === 1 || e.success === true,
      ts: parseHistTime(e.time),
      hash: e.hash,
      failReason: e.error_msg || null,
      argsJson: e.args_json,
      eventsJson: e.events_json,
      // The listing has no fee column. Only signed extrinsics pay a fee
      // (unsigned: heartbeats, timestamps, unsigned election submissions cost
      // nothing). The real fee is resolved per block below into blockFees.
      signed: !!e.signer && e.signer !== 'System' && e.signer !== 'Unsigned',
    }));
  }, [raw]);

  // Real per-block fees for the signed rows on screen. live_fees/mv_fees index
  // the runtime's TransactionFeePaid keyed by block, and live_fees stays within
  // ~100 blocks of the head — so recent rows resolve too (events_json is null for
  // recent blocks, so the per-block table is the working source). One batched
  // request for all signed blocks on the page. blockFees[block] = {totalXor,totalUsd,rows};
  // rows===1 → exact fee for that block's single signed extrinsic.
  const [blockFees, setBlockFees] = useState(undefined);
  useEffect(() => {
    const blocks = [...new Set(items.filter(x => x.signed).map(x => x.block))];
    if (blocks.length === 0) { setBlockFees({}); return; }
    let cancelled = false;
    setBlockFees(undefined);  // loading
    fetch('/history/extrinsic-fees?blocks=' + blocks.join(','))
      .then(r => r.ok ? r.json() : null)
      .then(j => { if (!cancelled) setBlockFees(j || {}); })
      .catch(() => { if (!cancelled) setBlockFees({}); });
    return () => { cancelled = true; };
  }, [items]);

  // re-render for "ago"
  const [, setTick] = useState(0);
  useEffect(() => {
    const id = setInterval(() => setTick(tick => tick + 1), 1000);
    return () => clearInterval(id);
  }, []);

  // Server-side filtering: backend returns exactly what we asked for,
  // so `filtered` is just `items`.
  const filtered = items;

  // Backend-driven pagination — `items` already contains only the current
  // page, and the real totalPages comes from the response meta.
  const totalPages = backendTotalPages && backendTotalPages > 0
    ? backendTotalPages
    : Math.max(1, Math.ceil(filtered.length / pageSize));
  const curPage = Math.min(page, totalPages);
  const visible = filtered;

  // KPIs are network-wide over the last 24h (from /stats/extrinsics-24h and
  // /stats/fees), with a graceful fallback to the loaded page only while those
  // endpoints are still loading. Avg fee = total 24h fees ÷ 24h extrinsic count.
  const stats = useMemo(() => {
    const fees24Total = Array.isArray(fees24h)
      ? fees24h.reduce((s, r) => s + (Number(r.total_xor) || 0), 0) : 0;
    const fees24Usd = Array.isArray(fees24h)
      ? fees24h.reduce((s, r) => s + (Number(r.total_usd) || 0), 0) : 0;
    if (ext24h) {
      const avgFee = ext24h.total ? fees24Total / ext24h.total : 0;
      return {
        total: ext24h.total,
        successRate: ext24h.successRate != null ? ext24h.successRate.toFixed(1) : '—',
        failed: ext24h.failed,
        avgFee,
        avgFeeUsd: ext24h.total ? fees24Usd / ext24h.total : 0,
        feesKnown: Array.isArray(fees24h),
        topPallet: ext24h.topPallet || '—',
        topPalletCount: ext24h.topPalletCount || 0,
        windowed: true,
      };
    }
    // Fallback: page-level until the 24h endpoint responds.
    const total = items.length;
    const ok = items.filter(x => x.ok).length;
    const palletCounts = {};
    items.forEach(x => palletCounts[x.pallet] = (palletCounts[x.pallet] || 0) + 1);
    const topPallet = Object.entries(palletCounts).sort((a,b) => b[1] - a[1])[0];
    return {
      total,
      successRate: total ? (ok / total * 100).toFixed(1) : '—',
      failed: total - ok,
      avgFee: 0,
      avgFeeUsd: 0,
      feesKnown: false,
      topPallet: topPallet ? topPallet[0] : '—',
      topPalletCount: topPallet ? topPallet[1] : 0,
      windowed: false,
    };
  }, [items, ext24h, fees24h]);

  const copyTx = (hash) => { navigator.clipboard?.writeText(hash); };

  return (
    <div>
      <PageHeader title={t('extrinsics.title')} sub={t('extrinsics.sub')}>
        <span className="tag ok"><span className="live-dot" style={{width:5,height:5}}/> {t('btn.streaming')}</span>
        <ExportCsvButton section="extrinsics"
          headers={['Time','Block','Index','Hash','Pallet','Method','Caller','Status']}
          rows={filtered.map(r => ({
            Time: new Date(r.ts).toISOString(),
            Block: r.block,
            Index: r.idx,
            Hash: r.hash,
            Pallet: r.pallet,
            Method: r.method,
            Caller: r.caller,
            Status: r.ok ? 'success' : ('failed: ' + (r.failReason || '')),
          }))}/>
      </PageHeader>

      <div className="swaps-stats-grid">
        <div className="stat-card">
          <span className="stat-label">Extrinsics · 24h</span>
          <span className="stat-value num">{stats.total.toLocaleString()}</span>
          <span className="stat-sub">{stats.windowed ? 'network-wide · 24h' : 'loading…'}</span>
        </div>
        <div className="stat-card">
          <span className="stat-label">Success Rate · 24h</span>
          <span className="stat-value num" style={{color: '#6EE7B7'}}>{stats.successRate}%</span>
          <span className="stat-sub">{stats.failed.toLocaleString()} failed · 24h</span>
        </div>
        <div className="stat-card">
          <span className="stat-label">Avg Fee · 24h</span>
          <span className="stat-value num">{stats.feesKnown ? stats.avgFee.toFixed(4) : '—'}<span style={{fontSize: 16, color:'var(--fg-2)', marginLeft: 6}}>XOR</span></span>
          <span className="stat-sub">{stats.feesKnown ? '$' + stats.avgFeeUsd.toFixed(4) + ' · per extrinsic' : 'loading…'}</span>
        </div>
        <div className="stat-card">
          <span className="stat-label">Top Pallet · 24h</span>
          <span className="stat-value" style={{fontSize: 20, color: PALLET_COLORS[stats.topPallet] || 'var(--fg-0)'}}>{stats.topPallet}</span>
          <span className="stat-sub">{stats.topPalletCount.toLocaleString()} calls · 24h</span>
        </div>
      </div>

      <div className="card" style={{marginTop: 18}}>
        <div className="swaps-filter-bar">
          <div className="swap-dropdown-wrap">
            <button className={'swap-dropdown-btn' + (palletFilter ? ' has-filter' : '')}
                    onClick={() => setPalletOpen(o => !o)}>
              <span style={{width: 8, height: 8, borderRadius: '50%',
                background: palletFilter ? PALLET_COLORS[palletFilter] : 'linear-gradient(135deg,#9B1B30,#7B5B90)'}}/>
              <span>{palletFilter || 'All Pallets'}</span>
              <svg width="10" height="10" viewBox="0 0 10 10" fill="none" stroke="currentColor" strokeWidth="1.5"><path d="m2 4 3 3 3-3"/></svg>
            </button>
            {palletOpen && (
              <div className="swap-dropdown-content">
                <div className="swap-dd-item" onClick={() => { setPalletFilter(null); setPalletOpen(false); setPage(1); }}>
                  <span style={{width:8,height:8,borderRadius:'50%',background:'linear-gradient(135deg,#9B1B30,#7B5B90)'}}/>
                  <span>🌟 All Pallets</span>
                </div>
                {palletList.map(p => (
                  <div key={p} className={'swap-dd-item' + (palletFilter === p ? ' active' : '')}
                       onClick={() => { setPalletFilter(p); setPalletOpen(false); setPage(1); }}>
                    <span style={{width:8, height:8, borderRadius:'50%', background: PALLET_COLORS[p] || '#64748B'}}/>
                    <span>{p}</span>
                  </div>
                ))}
              </div>
            )}
          </div>

          <input
            type="text"
            value={methodSearch}
            onChange={e => setMethodSearch(e.target.value)}
            placeholder="Buscar método..."
            title="Filtrar por nombre del método (debounce 500ms)"
            style={{padding:'6px 10px', border:'1px solid var(--border-color)', borderRadius:8, background:'var(--bg-card)', color:'var(--fg-0)', fontSize:13, minWidth: 180, outline:'none'}}/>

          <input
            type="datetime-local"
            value={dateFilter}
            onChange={e => { setDateFilter(e.target.value); setPage(1); }}
            title="Filtrar extrinsics anteriores a esta fecha/hora"
            style={{padding:'6px 10px', border:'1px solid var(--border-color)', borderRadius:8, background:'var(--bg-card)', color:'var(--fg-0)', fontSize:13}}/>
          {dateFilter && (
            <button className="btn" onClick={() => { setDateFilter(''); setPage(1); }} style={{padding:'4px 10px'}} title="Limpiar fecha">✕</button>
          )}

          {blockFilter && (
            <span className="tag" style={{display:'inline-flex', alignItems:'center', gap:6, background:'var(--accent-bg, #9B1B3022)', color:'var(--accent, #F5B041)', borderColor:'var(--accent, #F5B041)'}}>
              Block #{Number(blockFilter).toLocaleString()}
              <button className="btn" onClick={() => { setBlockFilter(''); setHashHighlight(''); setPage(1); }} style={{padding:'0 6px', marginLeft:4}} title="Quitar filtro de bloque">✕</button>
            </span>
          )}

          <button className="btn" onClick={refresh} disabled={histLoading} title="Actualizar" style={{marginLeft:'auto'}}>
            ↻ {histLoading ? 'Cargando…' : 'Refresh'}
          </button>

          <div className="status-toggle">
            {[
              { id: 'all',     label: t('chip.all') },
              { id: 'success', label: '✓ ' + t('status.success') },
              { id: 'failed',  label: '✗ ' + t('status.failed') },
            ].map(o => (
              <button key={o.id}
                className={'status-opt' + (statusFilter === o.id ? ' active' : '') + ' ' + o.id}
                onClick={() => { setStatusFilter(o.id); setPage(1); }}>
                {o.label}
              </button>
            ))}
          </div>

          <input type="datetime-local" className="swap-date-input" defaultValue="2026-04-18T12:00"/>

          <div className="swaps-filter-spacer"/>
          <span className="tag">{filtered.length} extrinsics</span>
        </div>

        <div className="swaps-table-wrap responsive-table">
          <table className="swaps-table extrinsics-table">
            <thead>
              <tr>
                <th style={{paddingLeft: 20}}>{t('col.time')}</th>
                <th>{t('drill.block')}</th>
                <th>{t('col.extrinsic')}</th>
                <th>Pallet :: Method</th>
                <th>{t('col.caller')}</th>
                <th style={{textAlign:'right'}}>{t('col.fee')}</th>
                <th style={{textAlign:'center'}}>{t('col.status')}</th>
                <th style={{width: 36, paddingRight: 20}}></th>
              </tr>
            </thead>
            <tbody>
              {visible.map(e => {
                // hashHighlight comes from /sorav2?tab=extrinsics&q=<hash> deep
                // links (used by the XOR migration table on /minamoto). When
                // the row's hash matches, paint a plum left-border so the user
                // visually finds the targeted extrinsic in a list of siblings.
                const isHighlighted = hashHighlight && e.hash && e.hash.toLowerCase() === hashHighlight.toLowerCase();
                return (
                <React.Fragment key={e.id}>
                  <tr className={'ext-row' + (expanded === e.id ? ' open' : '')}
                      style={isHighlighted ? { background: 'rgba(160,98,176,0.10)', boxShadow: 'inset 3px 0 0 #A062B0' } : null}
                      onClick={() => setExpanded(expanded === e.id ? null : e.id)}>
                    <td data-label={t('col.time')} style={{paddingLeft: 20}} title={fmt.fullDate(e.ts)}>
                      <div style={{fontSize: 12, fontWeight: 700, color: 'var(--fg-0)'}}>{fmt.ago(e.ts)}</div>
                      <div className="muted tiny">{fmt.fullDate(e.ts)}</div>
                    </td>
                    <td data-label={t('drill.block')}>
                      <a className="block-link num" onClick={(ev) => ev.stopPropagation()} href="#">#{e.block.toLocaleString()}-{e.idx}</a>
                    </td>
                    <td data-label={t('col.extrinsic')}>
                      <div className="ext-hash-cell" onClick={(ev) => ev.stopPropagation()}>
                        <code className="num">{e.hash.slice(0, 10)}…{e.hash.slice(-6)}</code>
                        <button className="copy-btn" onClick={() => copyTx(e.hash)} title="Copy hash">⎘</button>
                      </div>
                    </td>
                    <td data-label="Pallet :: Method">
                      <div className="pallet-method-cell">
                        <span className="pallet-badge" style={{['--pc']: PALLET_COLORS[e.pallet]}}>{e.pallet}</span>
                        <span className="pallet-sep">::</span>
                        <span className="method-name">{e.method}</span>
                      </div>
                    </td>
                    <td data-label={t('col.caller')}>
                      <div style={{display:'flex', alignItems:'center', gap:8, minWidth: 0}}>
                        <div style={{width:20, height:20, borderRadius:'50%', background:'linear-gradient(135deg,#7B5B90,#4A3566)', flexShrink: 0}}/>
                        <AddrStack addr={e.caller}/>
                      </div>
                    </td>
                    <td data-label={t('col.fee')} style={{textAlign:'right'}}>
                      {(() => {
                        // Unsigned → free. Signed + block has exactly one fee row →
                        // that IS this extrinsic's fee. Loading → "…". Multiple fees
                        // in the block or not indexed → "—" (drawer shows detail).
                        if (!e.signed) return <div className="muted tiny" title={t('ext.feeUnsigned', 'Extrinsic no firmado · sin coste')}>—</div>;
                        if (blockFees === undefined) return <div className="muted tiny num" style={{opacity:0.5}}>…</div>;
                        const bf = blockFees[e.block];
                        if (bf && bf.rows === 1 && bf.totalXor > 0) {
                          const xp = (window.TOKEN_PRICES && window.TOKEN_PRICES.XOR) || 0;
                          const usd = bf.totalUsd > 0 ? bf.totalUsd : (xp ? bf.totalXor * xp : 0);
                          return <><div className="num" style={{fontSize:12, fontWeight:700, color:'var(--fg-0)'}}>{fmtFee(bf.totalXor)} XOR</div>
                            {usd > 0 && <div className="muted tiny num">${usd.toFixed(4)}</div>}</>;
                        }
                        return <div className="muted tiny" title={bf ? t('ext.feeAmbiguous', 'Varias transacciones con coste en este bloque — despliega para el detalle') : t('ext.feeOnExpand', 'Fee no disponible para este extrinsic')}>—</div>;
                      })()}
                    </td>
                    <td data-label={t('col.status')} style={{textAlign:'center'}}>
                      {e.ok
                        ? <span className="status-pill ok" title="Success">✓</span>
                        : <span className="status-pill err" title="Failed">✗</span>}
                    </td>
                    <td style={{paddingRight: 20, textAlign:'center'}}>
                      <button className="row-action-btn" onClick={(ev) => { ev.stopPropagation(); open({type:'extrinsic', title:`${e.pallet}::${e.method}`, pallet:e.pallet, method:e.method, caller:e.caller, block:e.block, idx:e.idx, extrinsic_id:(e.block + '-' + e.idx), ts:e.ts, hash:e.hash, ok:e.ok, failReason:e.failReason, argsJson: e.argsJson, eventsJson: e.eventsJson, args: argsFor(e), events: eventsFor(e)}); }} title="Más Info">↗</button>
                      <span className={'ext-caret' + (expanded === e.id ? ' open' : '')} style={{marginLeft: 6}}>▾</span>
                    </td>
                  </tr>
                  {expanded === e.id && (
                    <tr className="ext-detail-row">
                      <td colSpan="8" style={{padding: 0}}>
                        <div className="ext-detail">
                          {!e.ok && (
                            <div className="ext-fail-banner">
                              <span style={{fontWeight:700, color:'#FCA5A5'}}>Failed:</span>
                              <span style={{color:'#FCA5A5', marginLeft: 8}}>{e.failReason}</span>
                            </div>
                          )}
                          <div className="ext-detail-grid">
                            <div>
                              <div className="ext-detail-label">Decoded Args</div>
                              <pre className="ext-args">{argsFor(e)}</pre>
                            </div>
                            <div>
                              <div className="ext-detail-label">Events Emitted · {eventsFor(e).length}</div>
                              <div className="ext-events">
                                {eventsFor(e).map((ev, i) => (
                                  <div key={i} className="ext-event-chip" style={{['--ec']: ev.color}}>
                                    <span className="ec-dot"/>
                                    <span className="ec-pallet">{ev.pallet}</span>
                                    <span className="ec-sep">·</span>
                                    <span className="ec-name">{ev.name}</span>
                                  </div>
                                ))}
                              </div>
                              <div className="ext-detail-label" style={{marginTop: 16}}>{t('ext.txFee', 'Fee de la transacción')}</div>
                              <ExtFee bf={blockFees === undefined ? undefined : blockFees[e.block]} signed={e.signed}/>
                            </div>
                          </div>
                        </div>
                      </td>
                    </tr>
                  )}
                </React.Fragment>
                );
              })}
              {visible.length === 0 && (
                <tr><td colSpan="8" style={{padding:40, textAlign:'center', color:'var(--fg-2)'}}>
                  No extrinsics match your filters.
                </td></tr>
              )}
            </tbody>
          </table>
        </div>

        <div className="swaps-pag">
          <button className="btn" disabled={curPage === 1} onClick={() => setPage(1)}>« First</button>
          <button className="btn" disabled={curPage === 1} onClick={() => setPage(p => Math.max(1, p - 1))}>⬅ Prev</button>
          <span className="pag-indicator">Page {curPage} of {totalPages}</span>
          <button className="btn" disabled={curPage === totalPages} onClick={() => setPage(p => Math.min(totalPages, p + 1))}>Next ➡</button>
          <button className="btn" disabled={curPage === totalPages} onClick={() => setPage(totalPages)}>Last »</button>
        </div>
      </div>
    </div>
  );
}

Object.assign(window, { ExtrinsicsSection });
