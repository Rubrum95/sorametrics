/* global React, fmt, TOKENS, FAKE_ADDRS, IDENTITIES, seededRand, useDrill, useT, ExportCsvButton */
const { useState, useEffect, useMemo, useRef } = React;

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
  if (logo) {
    return (
      <img src={logo}
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
  const [, setTick] = useState(0);

  // Real swaps from prod /history/global/swaps. Shape mapping:
  //   prod { time, block, hash, extrinsic_id, wallet, in:{symbol,amount,logo,usd}, out:{symbol,amount,logo} }
  //   prototype { id, ts, block, hash, acc, inTok, outTok, inAmt, outAmt, usd }
  const { items: rawSwaps } = useHistory('/history/global/swaps', { pageSize: 60, page: 1, pollMs: 20_000 });
  const items = useMemo(() => {
    if (!rawSwaps || rawSwaps.length === 0) return [];
    return rawSwaps.map((s, i) => ({
      id: 's-' + (s.hash || (s.block + ':' + i)),
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

  const filtered = useMemo(() => filter
    ? items.filter(s => s.inTok === filter || s.outTok === filter)
    : items, [items, filter]);

  const pageSize = tweaks.density === 'compact' ? 12 : tweaks.density === 'spacious' ? 6 : 8;
  const totalPages = Math.max(1, Math.ceil(filtered.length / pageSize));
  const curPage = Math.min(page, totalPages);
  const visible = filtered.slice((curPage - 1) * pageSize, curPage * pageSize);

  // Live stats (derived from current items)
  const stats = useMemo(() => {
    const vol = items.reduce((s, x) => s + x.usd, 0);
    const byTok = {};
    items.forEach(x => {
      byTok[x.inTok] = (byTok[x.inTok] || 0) + x.usd;
      byTok[x.outTok] = (byTok[x.outTok] || 0) + x.usd * 0.5;
    });
    const topTok = Object.entries(byTok).sort((a,b) => b[1] - a[1]).slice(0, 3);
    return { vol, count: items.length, topTok };
  }, [items]);

  return (
    <div>
      <PageHeader title={t('swaps.title')} sub={t('swaps.sub')}>
        <span className="tag ok"><span className="live-dot" style={{width:5,height:5}}/> {t('btn.streaming')}</span>
        <ExportCsvButton section="swaps"
          headers={['Time','Block','Account','Input','Output','USD','Action']}
          rows={filtered.map(r => ({
            Time: new Date(r.ts).toISOString(),
            Block: r.block,
            Account: r.acc,
            Input: (r.inAmt || 0) + ' ' + r.inTok,
            Output: (r.outAmt || 0) + ' ' + r.outTok,
            USD: (r.usd || 0).toFixed(2),
            Action: 'swap',
          }))}/>
      </PageHeader>

      {/* Hero stats */}
      <div className="swaps-stats-grid">
        <div className="stat-card">
          <span className="stat-label">Swaps · 24h</span>
          <span className="stat-value num">{stats.count.toLocaleString()}</span>
          <span className="stat-delta up">▲ 8.4% vs yesterday</span>
        </div>
        <div className="stat-card">
          <span className="stat-label">Volume · 24h</span>
          <span className="stat-value num">{fmt.usd(stats.vol * 12)}</span>
          <span className="stat-delta up">▲ 12.1%</span>
        </div>
        <div className="stat-card">
          <span className="stat-label">Unique Accounts</span>
          <span className="stat-value num">1,284</span>
          <span className="stat-sub">signers · 24h</span>
        </div>
        <div className="stat-card">
          <span className="stat-label">Top Pair</span>
          <span className="stat-value" style={{fontSize: 24}}>
            {stats.topTok[0]?.[0] || 'XOR'} / KUSD
          </span>
          <span className="stat-sub">highest volume</span>
        </div>
      </div>

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
                <div className="swap-dd-item" onClick={() => { setFilter(null); setDropdownOpen(false); setPage(1); }}>
                  <span style={{width:18,height:18,borderRadius:'50%',background:'linear-gradient(135deg,#FFD166,#E5243B)',display:'inline-block'}}/>
                  <span>🌟 All</span>
                </div>
                {SWAP_TOKENS.map(s => (
                  <div key={s} className={'swap-dd-item' + (filter === s ? ' active' : '')}
                       onClick={() => { setFilter(s); setDropdownOpen(false); setPage(1); }}>
                    <TokenLogo sym={s} size={18}/>
                    <span>{s}</span>
                    <span className="muted tiny" style={{marginLeft: 'auto'}}>{TOKENS[s]?.name || s}</span>
                  </div>
                ))}
              </div>
            )}
          </div>

          <input type="datetime-local" className="swap-date-input" defaultValue="2026-04-18T12:00"/>

          <div className="swaps-filter-spacer"/>

          <span className="tag">{filtered.length} swaps</span>
          <button className="btn">↻ Refresh</button>
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
                  <td style={{paddingLeft: 20}}>
                    <div style={{fontSize: 12, fontWeight: 700, color: 'var(--fg-0)'}}>{fmt.ago(s.ts)}</div>
                    <div className="muted tiny">{new Date(s.ts).toLocaleTimeString('en-US', {hour:'2-digit', minute:'2-digit', second: '2-digit'})}</div>
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
                    <div className="clickable" style={{display:'flex', alignItems:'center', gap:8, minWidth: 0, cursor:'pointer'}}
                         title="Abrir wallet"
                         onClick={(ev) => { ev.stopPropagation(); if (s.acc) window.openWalletDetails?.(s.acc, IDENTITIES[s.acc]); }}>
                      <div style={{width:22,height:22,borderRadius:'50%',background:'linear-gradient(135deg,#7B5B90,#4A3566)',flexShrink:0}}/>
                      <div style={{flex: 1, minWidth: 0, display: 'flex', flexDirection: 'column', lineHeight: 1.25}}>
                        {IDENTITIES[s.acc] && <span style={{fontSize: 12, fontWeight: 700, color:'var(--fg-0)', whiteSpace: 'nowrap', overflow: 'hidden', textOverflow: 'ellipsis'}}>{IDENTITIES[s.acc]}</span>}
                        <span className="muted tiny num" style={{whiteSpace:'nowrap', textDecoration:'underline dotted', textUnderlineOffset: 2}}>{fmt.addr(s.acc, 5, 4)}</span>
                      </div>
                    </div>
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

Object.assign(window, { SwapsSection });
