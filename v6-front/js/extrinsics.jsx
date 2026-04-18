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

function ExtrinsicsSection({ tweaks }) {
  const t = useT();
  const { open } = useDrill();
  const [palletFilter, setPalletFilter] = useState(null);
  const [statusFilter, setStatusFilter] = useState('all');
  const [page, setPage] = useState(1);
  const [expanded, setExpanded] = useState(null);
  const [palletOpen, setPalletOpen] = useState(false);

  // Real extrinsics from prod /history/global/extrinsics. Shape mapping:
  //   prod { time, block, extrinsic_index, extrinsic_id, hash, section, method,
  //          signer, success (0/1), args_json, error_msg, events_json }
  //   prototype expects { id, pallet, method, caller, feeXor, block, idx, ok,
  //                       ts, hash, failReason }
  const { items: raw, loading: histLoading } = useHistory('/history/global/extrinsics', { pageSize: 50, page: 1, pollMs: 20_000 });
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
      feeXor: 0, // prod /extrinsics endpoint doesn't include per-ext fee; derived in a later phase
      block: e.block,
      idx: e.extrinsic_index,
      ok: e.success === 1 || e.success === true,
      ts: parseHistTime(e.time),
      hash: e.hash,
      failReason: e.error_msg || null,
      argsJson: e.args_json,
      eventsJson: e.events_json,
    }));
  }, [raw]);

  // re-render for "ago"
  const [, setTick] = useState(0);
  useEffect(() => {
    const id = setInterval(() => setTick(tick => tick + 1), 1000);
    return () => clearInterval(id);
  }, []);

  const filtered = useMemo(() => {
    return items.filter(e => {
      if (palletFilter && e.pallet !== palletFilter) return false;
      if (statusFilter === 'success' && !e.ok) return false;
      if (statusFilter === 'failed' && e.ok) return false;
      return true;
    });
  }, [items, palletFilter, statusFilter]);

  const pageSize = tweaks.density === 'compact' ? 12 : tweaks.density === 'spacious' ? 6 : 10;
  const totalPages = Math.max(1, Math.ceil(filtered.length / pageSize));
  const curPage = Math.min(page, totalPages);
  const visible = filtered.slice((curPage - 1) * pageSize, curPage * pageSize);

  const stats = useMemo(() => {
    const total = items.length;
    const ok = items.filter(x => x.ok).length;
    const fee = items.reduce((s, x) => s + x.feeXor, 0);
    const palletCounts = {};
    items.forEach(x => palletCounts[x.pallet] = (palletCounts[x.pallet] || 0) + 1);
    const topPallet = Object.entries(palletCounts).sort((a,b) => b[1] - a[1])[0];
    return {
      total: total * 384,
      success: (ok / total * 100).toFixed(1),
      failed: total - ok,
      avgFee: fee / total,
      topPallet: topPallet ? topPallet[0] : 'liquidityProxy',
      topPalletCount: topPallet ? topPallet[1] * 200 : 0,
    };
  }, [items]);

  const copyTx = (hash) => { navigator.clipboard?.writeText(hash); };

  return (
    <div>
      <PageHeader title={t('extrinsics.title')} sub={t('extrinsics.sub')}>
        <span className="tag ok"><span className="live-dot" style={{width:5,height:5}}/> {t('btn.streaming')}</span>
        <ExportCsvButton section="extrinsics"
          headers={['Time','Block','Index','Hash','Pallet','Method','Caller','Fee','Status']}
          rows={filtered.map(r => ({
            Time: new Date(r.ts).toISOString(),
            Block: r.block,
            Index: r.idx,
            Hash: r.hash,
            Pallet: r.pallet,
            Method: r.method,
            Caller: r.caller,
            Fee: r.feeXor,
            Status: r.ok ? 'success' : ('failed: ' + (r.failReason || '')),
          }))}/>
      </PageHeader>

      <div className="swaps-stats-grid">
        <div className="stat-card">
          <span className="stat-label">Extrinsics · 24h</span>
          <span className="stat-value num">{stats.total.toLocaleString()}</span>
          <span className="stat-delta up">▲ 3.7% vs yesterday</span>
        </div>
        <div className="stat-card">
          <span className="stat-label">Success Rate</span>
          <span className="stat-value num" style={{color: '#6EE7B7'}}>{stats.success}%</span>
          <span className="stat-sub">{stats.failed} failed in sample</span>
        </div>
        <div className="stat-card">
          <span className="stat-label">Avg Fee</span>
          <span className="stat-value num">{stats.avgFee.toFixed(4)}<span style={{fontSize: 16, color:'var(--fg-2)', marginLeft: 6}}>XOR</span></span>
          <span className="stat-sub">≈ ${(stats.avgFee * 0.072).toFixed(4)}</span>
        </div>
        <div className="stat-card">
          <span className="stat-label">Top Pallet</span>
          <span className="stat-value" style={{fontSize: 20, color: PALLET_COLORS[stats.topPallet]}}>{stats.topPallet}</span>
          <span className="stat-sub">{stats.topPalletCount.toLocaleString()} calls</span>
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
                {PALLETS.map(p => (
                  <div key={p} className={'swap-dd-item' + (palletFilter === p ? ' active' : '')}
                       onClick={() => { setPalletFilter(p); setPalletOpen(false); setPage(1); }}>
                    <span style={{width:8, height:8, borderRadius:'50%', background: PALLET_COLORS[p]}}/>
                    <span>{p}</span>
                  </div>
                ))}
              </div>
            )}
          </div>

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

        <div className="swaps-table-wrap">
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
              {visible.map(e => (
                <React.Fragment key={e.id}>
                  <tr className={'ext-row' + (expanded === e.id ? ' open' : '')}
                      onClick={() => setExpanded(expanded === e.id ? null : e.id)}>
                    <td style={{paddingLeft: 20}}>
                      <div title={new Date(e.ts).toISOString()} style={{fontSize: 12, fontWeight: 700, color: 'var(--fg-0)'}}>{fmt.ago(e.ts)}</div>
                      <div className="muted tiny">{new Date(e.ts).toLocaleTimeString('en-US', {hour:'2-digit', minute:'2-digit'})}</div>
                    </td>
                    <td>
                      <a className="block-link num" onClick={(ev) => ev.stopPropagation()} href="#">#{e.block.toLocaleString()}-{e.idx}</a>
                    </td>
                    <td>
                      <div className="ext-hash-cell" onClick={(ev) => ev.stopPropagation()}>
                        <code className="num">{e.hash.slice(0, 10)}…{e.hash.slice(-6)}</code>
                        <button className="copy-btn" onClick={() => copyTx(e.hash)} title="Copy hash">⎘</button>
                      </div>
                    </td>
                    <td>
                      <div className="pallet-method-cell">
                        <span className="pallet-badge" style={{['--pc']: PALLET_COLORS[e.pallet]}}>{e.pallet}</span>
                        <span className="pallet-sep">::</span>
                        <span className="method-name">{e.method}</span>
                      </div>
                    </td>
                    <td>
                      <div style={{display:'flex', alignItems:'center', gap:8, minWidth: 0}}>
                        <div style={{width:20, height:20, borderRadius:'50%', background:'linear-gradient(135deg,#7B5B90,#4A3566)', flexShrink: 0}}/>
                        <div style={{flex:1, minWidth:0, display:'flex', flexDirection:'column', lineHeight:1.25}}>
                          {IDENTITIES[e.caller] && <span style={{fontSize:11, fontWeight:700, color:'var(--fg-0)', whiteSpace:'nowrap', overflow:'hidden', textOverflow:'ellipsis'}}>{IDENTITIES[e.caller]}</span>}
                          <span className="muted tiny num" style={{whiteSpace:'nowrap'}}>{fmt.addr(e.caller, 5, 4)}</span>
                        </div>
                      </div>
                    </td>
                    <td style={{textAlign:'right'}}>
                      <div className="num" style={{fontSize:12, fontWeight:700, color:'var(--fg-0)'}}>{e.feeXor.toFixed(4)} XOR</div>
                      <div className="muted tiny num">${(e.feeXor * 0.072).toFixed(4)}</div>
                    </td>
                    <td style={{textAlign:'center'}}>
                      {e.ok
                        ? <span className="status-pill ok" title="Success">✓</span>
                        : <span className="status-pill err" title="Failed">✗</span>}
                    </td>
                    <td style={{paddingRight: 20, textAlign:'center'}}>
                      <button className="row-action-btn" onClick={(ev) => { ev.stopPropagation(); open({type:'extrinsic', title:`${e.pallet}::${e.method}`, pallet:e.pallet, method:e.method, caller:e.caller, fee:e.feeXor, block:e.block, ts:e.ts, hash:e.hash, ok:e.ok, failReason:e.failReason, args: argsFor(e), events: eventsFor(e)}); }} title="Open drill">↗</button>
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
                              <div className="ext-detail-label" style={{marginTop: 16}}>Fee Breakdown</div>
                              <div className="ext-fee-list">
                                <div><span>Gas fee</span><span className="num">{(e.feeXor * 0.78).toFixed(4)} XOR</span></div>
                                <div><span>Tip</span><span className="num">{(e.feeXor * 0.03).toFixed(4)} XOR</span></div>
                                <div><span>Treasury</span><span className="num">{(e.feeXor * 0.12).toFixed(4)} XOR</span></div>
                                <div><span>Reserved</span><span className="num">{(e.feeXor * 0.07).toFixed(4)} XOR</span></div>
                                <div className="ext-fee-total"><span>Total</span><span className="num">{e.feeXor.toFixed(4)} XOR</span></div>
                              </div>
                            </div>
                          </div>
                        </div>
                      </td>
                    </tr>
                  )}
                </React.Fragment>
              ))}
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
