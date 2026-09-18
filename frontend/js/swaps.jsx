/* global React, fmt, TOKENS, FAKE_ADDRS, IDENTITIES, seededRand, useDrill, useT, ExportCsvButton, useIdentity, useIdentitySource, SourceTag, KpiGrid */
const { useState, useEffect, useMemo, useRef } = React;

// Account cell used in the swaps/transfers/bridges tables. Shows the resolved
// name (user alias > on-chain identity > technical account) above the short
// address, with a gradient avatar. Two-zone click pattern:
//   · Name → copies the SS58 address to the clipboard (brief "Copiado" flash).
//   · Address → opens the wallet drill.
// When no name is resolved, the address row stays clickable for the drill.
function AccountCell({ addr, size = 22 }) {
  const name = useIdentity(addr);
  const source = useIdentitySource(addr);
  const openWallet = (ev) => {
    ev.stopPropagation();
    if (addr) window.openWalletDetails?.(addr, name || null);
  };
  const copyAddr = (ev) => {
    ev.stopPropagation();
    if (!addr) return;
    const el = ev.currentTarget;
    const done = () => {
      if (el.getAttribute('data-copy-flash') === '1') return;
      el.setAttribute('data-copy-flash', '1');
      const orig = el.textContent;
      el.textContent = '✓ Copiado';
      setTimeout(() => { el.textContent = orig; el.removeAttribute('data-copy-flash'); }, 1100);
    };
    if (navigator.clipboard?.writeText) navigator.clipboard.writeText(addr).then(done, done);
    else {
      try {
        const ta = document.createElement('textarea');
        ta.value = addr; ta.style.position = 'fixed'; ta.style.opacity = '0';
        document.body.appendChild(ta); ta.select();
        document.execCommand('copy');
        document.body.removeChild(ta);
        done();
      } catch {}
    }
  };
  return (
    <div style={{display:'flex', alignItems:'center', gap:8, minWidth: 0}}
         title={addr ? addr + (name ? ' · ' + name : '') : undefined}>
      <div style={{width:size,height:size,borderRadius:'50%',background:'linear-gradient(135deg,#7B5B90,#4A3566)',flexShrink:0}}/>
      <div style={{flex: 1, minWidth: 0, display: 'flex', flexDirection: 'column', lineHeight: 1.25}}>
        {name && (
          <div style={{display:'flex', alignItems:'center', gap:4}}>
            <span
              style={{fontSize: 12, fontWeight: 700, color:'var(--fg-0)', whiteSpace: 'nowrap', overflow: 'hidden', textOverflow: 'ellipsis', cursor:'pointer'}}
              title={'Copiar dirección · ' + addr}
              onClick={copyAddr}>
              {name}
            </span>
            {window.SourceTag && <window.SourceTag source={source}/>}
          </div>
        )}
        <span
          className="muted tiny num"
          style={{whiteSpace:'nowrap', textDecoration:'underline dotted', textUnderlineOffset: 2, cursor:'pointer'}}
          title={'Abrir wallet · ' + addr}
          onClick={openWallet}>
          {fmt.addr(addr, 5, 4)}
        </span>
      </div>
    </div>
  );
}

const SWAP_TOKENS = ['XOR', 'VAL', 'PSWAP', 'TBCD', 'KUSD', 'ETH', 'DAI'];

function makeSwap(id, rnd, now) {
  const inTok = SWAP_TOKENS[Math.floor(rnd() * SWAP_TOKENS.length)];
  let outTok = SWAP_TOKENS[Math.floor(rnd() * SWAP_TOKENS.length)];
  if (outTok === inTok) outTok = 'KUSD';
  const inAmt = +(rnd() * 5000 + 1).toFixed(2);
  const rate = (rnd() * 2 + 0.1);
  const outAmt = +(inAmt * rate).toFixed(2);
  const priceIn = 0.01 + rnd() * 4;
  const usd = +(inAmt * priceIn).toFixed(2);
  const acc = FAKE_ADDRS[Math.floor(rnd() * FAKE_ADDRS.length)];
  const block = 21_418_000 + Math.floor(rnd() * 5000);
  return {
    id, block, ts: now,
    inTok, outTok, inAmt, outAmt, usd, acc,
    fee: +(usd * 0.003).toFixed(3),
  };
}

// Renders a round token logo. If `logo` URL/base64 is provided (from prod
// /tokens, /history/swaps, /balance), uses it. Otherwise falls back to the
// gradient initial placeholder.
function TokenLogo({ sym, logo, size = 24 }) {
  // Prefer an explicit logo from the fetched row; fall back to the global
  // TOKEN_LOGOS cache (populated from /tokens) so every section renders a real
  // icon instead of a first-letter placeholder when the row omits the field.
  const src = logo || (sym && window.TOKEN_LOGOS && window.TOKEN_LOGOS[sym]) || null;
  if (src) {
    return (
      <img src={src}
           alt={sym || ''}
           style={{
             width: size, height: size, borderRadius: '50%',
             flexShrink: 0, objectFit: 'cover',
             boxShadow: '0 2px 8px rgba(0,0,0,0.25)',
             background: 'rgba(255,255,255,0.04)',
           }}
           onError={(e) => { e.currentTarget.style.display = 'none'; }}
      />
    );
  }
  const tk = TOKENS[sym] || { grad: 'linear-gradient(135deg,#555,#333)' };
  return (
    <div className="swap-tok-logo-round" style={{
      width: size, height: size, borderRadius: '50%',
      background: tk.grad,
      display: 'inline-flex', alignItems: 'center', justifyContent: 'center',
      fontSize: Math.round(size * 0.42), fontWeight: 800, color: '#fff',
      flexShrink: 0,
      boxShadow: '0 2px 8px rgba(0,0,0,0.25)',
    }}>{sym ? sym[0] : '?'}</div>
  );
}

function SwapArrow() {
  return (
    <svg viewBox="0 0 36 24" width="36" height="20" className="swap-arrow-svg"
         fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
      <path d="M4 9h22l-4-4M30 15H8l4 4"/>
    </svg>
  );
}

function SwapsSection({ tweaks }) {
  const t = useT();
  const { open } = useDrill();
  const [filter, setFilter] = useState(null); // token symbol or null
  const [page, setPage] = useState(1);
  const [dropdownOpen, setDropdownOpen] = useState(false);
  const [dropdownSearch, setDropdownSearch] = useState('');
  const [dateFilter, setDateFilter] = useState(''); // ISO local datetime e.g. 2026-04-19T15:00
  const [tokenList, setTokenList] = useState(SWAP_TOKENS); // dynamic, from /tokens
  const [networkOverview, setNetworkOverview] = useState(null);
  const [, setTick] = useState(0);

  // Load full token registry once (dropdown should not be hardcoded).
  useEffect(() => {
    let cancelled = false;
    fetch('/tokens?limit=100&sparkline=false').then(r => r.ok ? r.json() : null).then(j => {
      if (cancelled || !j) return;
      const arr = Array.isArray(j) ? j : (j.data || []);
      const syms = arr.map(t => t.symbol).filter(Boolean);
      if (syms.length) setTokenList(syms);
    }).catch(() => {});
    return () => { cancelled = true; };
  }, []);

  // Network overview: real Volume 24h + unique accounts when prod exposes it.
  useEffect(() => {
    let cancelled = false;
    const pull = () => fetch('/stats/overview').then(r => r.ok ? r.json() : null).then(j => {
      if (!cancelled) setNetworkOverview(j);
    }).catch(() => {});
    pull();
    const id = setInterval(pull, 60_000);
    return () => { cancelled = true; clearInterval(id); };
  }, []);

  // Build endpoint with optional date filter (`before` ms or `date` ISO).
  const swapsEndpoint = useMemo(() => {
    const q = new URLSearchParams();
    if (dateFilter) {
      const ts = new Date(dateFilter).getTime();
      if (Number.isFinite(ts)) q.set('before', String(ts));
    }
    if (filter) q.set('token', filter);
    return '/history/global/swaps' + (q.toString() ? '?' + q.toString() : '');
  }, [dateFilter, filter]);

  // Real swaps from prod /history/global/swaps. Shape mapping:
  //   prod { time, block, hash, extrinsic_id, wallet, in:{symbol,amount,logo,usd}, out:{symbol,amount,logo} }
  // Pagination is now server-side: we pass the UI page straight to the
  // backend (which exposes `total`/`totalPages` — millions of swaps indexed).
  // pageSize depends on the density tweak so mobile/compact views get tighter rows.
  const pageSize = tweaks.density === 'compact' ? 12 : tweaks.density === 'spacious' ? 6 : 8;
  const { items: rawSwaps, total: backendTotal, totalPages: backendTotalPages, refresh, loading } = useHistory(swapsEndpoint, { pageSize, page, pollMs: 20_000 });
  const items = useMemo(() => {
    if (!rawSwaps || rawSwaps.length === 0) return [];
    return rawSwaps.map((s, i) => ({
      id: 's-' + (s.hash ? s.hash + '-' + i : s.block + ':' + i),
      ts: parseHistTime(s.time),
      block: s.block,
      hash: s.hash,
      acc: s.wallet,
      inTok: s.in?.symbol,
      outTok: s.out?.symbol,
      inAmt: Number(s.in?.amount || 0),
      outAmt: Number(s.out?.amount || 0),
      usd: Number(s.in?.usd || 0),
      inLogo: s.in?.logo,     // match prod: render the real token logo (base64/url)
      outLogo: s.out?.logo,
    }));
  }, [rawSwaps]);

  // time ticker
  useEffect(() => {
    const id = setInterval(() => setTick(t => t + 1), 1000);
    return () => clearInterval(id);
  }, []);

  // Token filter goes to the backend (see `q.set('token', filter)` in the
  // endpoint builder above). The server returns only matching swaps already
  // paginated, so we don't need a second local filter/slice pass.
  const visible = items;
  // Fall back to a local count only if the backend doesn't return totalPages
  // (some endpoint variants / errors). Otherwise trust the real number.
  const totalPages = backendTotalPages && backendTotalPages > 0
    ? backendTotalPages
    : Math.max(1, Math.ceil(items.length / pageSize));
  const curPage = Math.min(page, totalPages);

  // Live stats — derived from real items + network overview when available.
  const stats = useMemo(() => {
    const vol = items.reduce((s, x) => s + x.usd, 0);
    // Top pair (real): aggregate by inTok/outTok pair, pick highest USD volume.
    const byPair = {};
    items.forEach(x => {
      const key = (x.inTok || '?') + '/' + (x.outTok || '?');
      byPair[key] = (byPair[key] || 0) + x.usd;
    });
    const topPair = Object.entries(byPair).sort((a, b) => b[1] - a[1])[0];
    // Real unique signers from current dataset.
    const uniqueAccs = new Set(items.map(x => x.acc).filter(Boolean)).size;
    // Prefer network-wide volume from /stats/overview when present.
    const realVol24h = Number(networkOverview?.network?.volume) || vol;
    const realCount24h = Number(networkOverview?.network?.swapsCount) || items.length;
    const realUniqueAccs = Number(networkOverview?.network?.uniqueAccounts) || uniqueAccs;
    return {
      vol: realVol24h,
      count: realCount24h,
      uniqueAccs: realUniqueAccs,
      topPair: topPair ? topPair[0] : '—',
    };
  }, [items, networkOverview]);

  return (
    <div>
      <PageHeader title={t('swaps.title')} sub={t('swaps.sub')}>
        <span className="tag ok"><span className="live-dot" style={{width:5,height:5}}/> {t('btn.streaming')}</span>
        <ExportCsvButton section="swaps"
          headers={['Time','Block','Account','Input','Output','USD','Action']}
          rows={items.map(r => ({
            Time: new Date(r.ts).toISOString(),
            Block: r.block,
            Account: r.acc,
            Input: (r.inAmt || 0) + ' ' + r.inTok,
            Output: (r.outAmt || 0) + ' ' + r.outTok,
            USD: (r.usd || 0).toFixed(2),
            Action: 'swap',
          }))}/>
      </PageHeader>

      {/* Hero stats — via shared KpiGrid so the compact style + logos come
          for free. `topPair` is "DAI/XOR" style; split on / to feed `pair`. */}
      {(() => {
        const [a, b] = (stats.topPair || '').split('/');
        return (
          <KpiGrid items={[
            { label:'Swaps · 24h',     value: stats.count.toLocaleString(),    sub:'last 24h' },
            { label:'Volume · 24h',    value: fmt.usd(stats.vol),              sub:'network-wide' },
            { label:'Unique Accounts', value: stats.uniqueAccs.toLocaleString(), sub:'signers · 24h' },
            { label:'Top Pair',        value: stats.topPair,                    sub:'highest volume', pair: a && b ? { a, b } : null },
          ]}/>
        );
      })()}

      {/* Filter bar */}
      <div className="card" style={{marginTop: 18}}>
        <div className="swaps-filter-bar">
          <div className="swap-dropdown-wrap">
            <button className={'swap-dropdown-btn' + (filter ? ' has-filter' : '')}
                    onClick={() => setDropdownOpen(o => !o)}>
              {filter ? <><TokenLogo sym={filter} size={18}/> <span>Filter: {filter}</span></>
                      : <><span style={{width:18,height:18,borderRadius:'50%',background:'linear-gradient(135deg,#FFD166,#E5243B)',display:'inline-block'}}/> <span>All Tokens</span></>}
              <svg width="10" height="10" viewBox="0 0 10 10" fill="none" stroke="currentColor" strokeWidth="1.5"><path d="m2 4 3 3 3-3"/></svg>
            </button>
            {dropdownOpen && (
              <div className="swap-dropdown-content">
                <input
                  type="text"
                  className="swap-dd-search"
                  autoFocus
                  placeholder="Buscar token..."
                  value={dropdownSearch}
                  onChange={e => setDropdownSearch(e.target.value)}
                  style={{width:'100%', padding:'8px 10px', border:'1px solid var(--border-color)', borderRadius:6, background:'var(--bg-card)', color:'var(--fg-0)', marginBottom:6, fontSize:13, outline:'none'}}/>
                <div className="swap-dd-item" onClick={() => { setFilter(null); setDropdownOpen(false); setDropdownSearch(''); setPage(1); }}>
                  <span style={{width:18,height:18,borderRadius:'50%',background:'linear-gradient(135deg,#FFD166,#E5243B)',display:'inline-block'}}/>
                  <span>🌟 All Tokens</span>
                </div>
                {tokenList
                  .filter(s => !dropdownSearch || s.toLowerCase().includes(dropdownSearch.toLowerCase()) || (TOKENS[s]?.name || '').toLowerCase().includes(dropdownSearch.toLowerCase()))
                  .map(s => (
                  <div key={s} className={'swap-dd-item' + (filter === s ? ' active' : '')}
                       onClick={() => { setFilter(s); setDropdownOpen(false); setDropdownSearch(''); setPage(1); }}>
                    <TokenLogo sym={s} size={18}/>
                    <span>{s}</span>
                    <span className="muted tiny" style={{marginLeft: 'auto'}}>{TOKENS[s]?.name || s}</span>
                  </div>
                ))}
              </div>
            )}
          </div>

          <input
            type="datetime-local"
            className="swap-date-input"
            value={dateFilter}
            onChange={e => { setDateFilter(e.target.value); setPage(1); }}
            title="Filtrar swaps anteriores a esta fecha/hora"/>
          {dateFilter && (
            <button
              className="btn"
              onClick={() => { setDateFilter(''); setPage(1); }}
              title="Limpiar filtro de fecha"
              style={{padding:'4px 10px'}}>✕</button>
          )}

          <div className="swaps-filter-spacer"/>

          <span className="tag">{(backendTotal ?? items.length).toLocaleString()} swaps{loading ? ' · cargando' : ''}</span>
          <button className="btn" onClick={refresh} disabled={loading} title="Actualizar">↻ Refresh</button>
        </div>

        {/* Table (desktop) */}
        <div className="swaps-table-wrap">
          <table className="swaps-table">
            <thead>
              <tr>
                <th style={{paddingLeft: 20}}>{t('col.time')}</th>
                <th>{t('drill.block')}</th>
                <th>Input</th>
                <th style={{textAlign:'center', width: 50}}></th>
                <th>Output</th>
                <th>{t('col.account')}</th>
                <th style={{textAlign:'right', paddingRight: 20}}>Action</th>
              </tr>
            </thead>
            <tbody>
              {visible.map(s => (
                <tr key={s.id} className="swap-row clickable"
                    onClick={() => open({type:'swap', title:`${s.inTok} → ${s.outTok}`, inSym:s.inTok, outSym:s.outTok, inAmt:s.inAmt, outAmt:s.outAmt, inUsd:s.usd, outUsd:s.usd*0.997, fee:s.fee, caller:s.acc, block:s.block, ts:s.ts, hash:'0x' + Math.random().toString(16).slice(2,18), pool:`${s.inTok}/${s.outTok} XYK`})}>
                  <td style={{paddingLeft: 20}} title={fmt.fullDate(s.ts)}>
                    <div style={{fontSize: 12, fontWeight: 700, color: 'var(--fg-0)'}}>{fmt.ago(s.ts)}</div>
                    <div className="muted tiny">{fmt.fullDate(s.ts)}</div>
                  </td>
                  <td>
                    <a className="block-link num" href="#" onClick={(e) => e.stopPropagation()}>#{s.block.toLocaleString()}</a>
                  </td>
                  <td>
                    <div className="swap-tok-cell">
                      <TokenLogo sym={s.inTok} logo={s.inLogo} size={26}/>
                      <div className="swap-tok-vals">
                        <div className="swap-tok-sym">{s.inTok}</div>
                        <div className="swap-tok-amt num">{fmt.num(s.inAmt, 2)}</div>
                        <div className="swap-tok-usd">${fmt.num(s.usd, 2)}</div>
                      </div>
                    </div>
                  </td>
                  <td style={{textAlign:'center', color: 'var(--accent)'}}>
                    <SwapArrow/>
                  </td>
                  <td>
                    <div className="swap-tok-cell">
                      <TokenLogo sym={s.outTok} logo={s.outLogo} size={26}/>
                      <div className="swap-tok-vals">
                        <div className="swap-tok-sym">{s.outTok}</div>
                        <div className="swap-tok-amt num">{fmt.num(s.outAmt, 2)}</div>
                        <div className="swap-tok-usd">${fmt.num(s.usd * 0.997, 2)}</div>
                      </div>
                    </div>
                  </td>
                  <td>
                    <AccountCell addr={s.acc}/>
                  </td>
                  <td style={{textAlign:'right', paddingRight: 20}}>
                    <button className="row-action-btn" title="Más Info"
                      onClick={(ev) => { ev.stopPropagation(); open({
                        type:'swap',
                        title: s.inTok + ' → ' + s.outTok,
                        block: s.block,
                        idx: s.idx,
                        extrinsic_id: s.block && s.idx != null ? (s.block + '-' + s.idx) : null,
                        hash: s.hash,
                        caller: s.acc,
                        inSym: s.inTok, outSym: s.outTok,
                        inAmt: s.inAmt, outAmt: s.outAmt,
                        usd: s.usd,
                        ts: s.ts,
                      }); }}>↗</button>
                  </td>
                </tr>
              ))}
              {visible.length === 0 && (
                <tr><td colSpan="7" style={{padding:40, textAlign:'center', color:'var(--fg-2)'}}>
                  No swaps found{filter ? ` for ${filter}` : ''}.
                </td></tr>
              )}
            </tbody>
          </table>
        </div>

        {/* Pagination */}
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

Object.assign(window, { SwapsSection, TokenLogo });
