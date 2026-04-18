/* global React, fmt, TOKENS, FAKE_ADDRS, IDENTITIES, sparkPath, useT, ExportCsvButton, PF_WALLETS, AddWalletModal */
const { useState, useEffect, useMemo } = React;

const PF_HOLDINGS = [
  { sym: 'XOR',   amt: 18420,    price: 0.072 },
  { sym: 'VAL',   amt: 72100,    price: 0.094 },
  { sym: 'PSWAP', amt: 842000,   price: 0.0031 },
  { sym: 'KUSD',  amt: 3120,     price: 0.998 },
  { sym: 'ETH',   amt: 1.82,     price: 3240 },
  { sym: 'TBCD',  amt: 512,      price: 1.002 },
  { sym: 'DAI',   amt: 1820,     price: 1.0 },
];

const PF_WALLETS = [
  { name: 'Main', addr: FAKE_ADDRS[0], share: 0.48, delta: 4.2 },
  { name: 'Savings', addr: FAKE_ADDRS[1], share: 0.28, delta: 2.1 },
  { name: 'LP Farm', addr: FAKE_ADDRS[2], share: 0.16, delta: -0.8 },
  { name: 'Cold', addr: FAKE_ADDRS[3], share: 0.08, delta: 0.0 },
];

function Donut({ slices }) {
  const R = 78, r = 54, cx = 90, cy = 90;
  let acc = 0;
  const total = slices.reduce((s, x) => s + x.value, 0);
  const t = useT();
 return (
    <svg className="donut-svg" viewBox="0 0 180 180">
      <circle cx={cx} cy={cy} r={R} fill="none" stroke="rgba(255,255,255,0.05)" strokeWidth={R - r}/>
      {slices.map((s, i) => {
        const frac = s.value / total;
        const startA = acc * 2 * Math.PI - Math.PI/2;
        acc += frac;
        const endA = acc * 2 * Math.PI - Math.PI/2;
        const large = frac > 0.5 ? 1 : 0;
        const x1 = cx + R * Math.cos(startA), y1 = cy + R * Math.sin(startA);
        const x2 = cx + R * Math.cos(endA),   y2 = cy + R * Math.sin(endA);
        const x3 = cx + r * Math.cos(endA),   y3 = cy + r * Math.sin(endA);
        const x4 = cx + r * Math.cos(startA), y4 = cy + r * Math.sin(startA);
        const d = `M ${x1} ${y1} A ${R} ${R} 0 ${large} 1 ${x2} ${y2} L ${x3} ${y3} A ${r} ${r} 0 ${large} 0 ${x4} ${y4} Z`;
        return <path key={i} d={d} fill={s.color} opacity="0.92" stroke="#111" strokeWidth="0.6"/>;
      })}
      <text x={cx} y={cy-4} textAnchor="middle" fill="#e5e7eb" fontSize="10" fontFamily="Inter" letterSpacing="1.5" fontWeight="700">NET WORTH</text>
      <text x={cx} y={cy+14} textAnchor="middle" fill="#fff" fontSize="16" fontFamily="JetBrains Mono" fontWeight="800">
        {fmt.usd(total)}
      </text>
    </svg>
  );
}

function MiniSpark({ data, color, w = 64, h = 24 }) {
  return (
    <svg viewBox={`0 0 ${w} ${h}`} width={w} height={h}>
      <path d={sparkPath(data, w, h, 2)} stroke={color} strokeWidth="1.3" fill="none"/>
    </svg>
  );
}

function PortfolioSection({ tweaks }) {
  const [cur, setCur] = useState('USD');
  const [tick, setTick] = useState(0);

  useEffect(() => {
    const id = setInterval(() => setTick(t => t + 1), 1400 / (tweaks.liveSpeed || 1));
    return () => clearInterval(id);
  }, [tweaks.liveSpeed]);

  const rates = { USD: 1, EUR: 0.93, XOR: 13.89 };

  const holdings = useMemo(() => {
    return PF_HOLDINGS.map(h => {
      // gentle live price wiggle
      const wiggle = Math.sin((tick + h.sym.charCodeAt(0)) / 2) * 0.003 + Math.random() * 0.002;
      const p = h.price * (1 + wiggle);
      return { ...h, price: p, value: h.amt * p };
    });
  }, [tick]);

  const total = holdings.reduce((s, h) => s + h.value, 0);

  const slices = holdings.map(h => ({ value: h.value, color: TOKENS[h.sym]?.color || '#888' }));

  const sparks = useMemo(() => {
    const out = {};
    PF_HOLDINGS.forEach((h, i) => {
      out[h.sym] = Array.from({length: 30}, (_, j) =>
        50 + Math.sin(j/3 + i) * 14 + Math.cos(j/5 + i*1.7) * 6 + Math.random() * 3
      );
    });
    return out;
  }, []);

  const fmtCur = (v) => {
    if (cur === 'XOR') return (v * rates.XOR).toLocaleString('en-US', { maximumFractionDigits: 0 }) + ' XOR';
    if (cur === 'EUR') return '€' + fmt.num(v * rates.EUR, 2).replace(/[BMK]/, m => ({B: 'B', M:'M', K:'K'}[m]));
    return fmt.usd(v);
  };

  const [addOpen, setAddOpen] = useState(false);

  return (
    <div>
      <PageHeader title={t('portfolio.title')} sub={t('portfolio.sub')}>
        <button className="btn" onClick={() => setAddOpen(true)}>+ Add Wallet</button>
        <ExportCsvButton section="portfolio"
          headers={['Name','Address','Share','Delta24h']}
          rows={PF_WALLETS.map(w => ({
            Name: w.name, Address: w.addr, Share: (w.share*100).toFixed(2)+'%', Delta24h: w.delta+'%',
          }))}
          className="primary"/>
      </PageHeader>

      <div className="pf-hero">
        {/* Net worth */}
        <div className="card pf-worth-card">
          <div className="pf-worth-label">Total Net Worth</div>
          <div className="pf-worth-value num">
            <span className="cur">$</span>{fmt.int(total)}<span className="cur" style={{fontSize: '0.55em'}}>.{((total % 1) * 100).toFixed(0).padStart(2,'0')}</span>
          </div>
          <div className="pf-worth-delta">
            <span className="stat-delta up">▲ $4,218 · 2.41%</span>
            <span className="stat-sub" style={{marginLeft: 8}}>24h · across 4 wallets</span>
          </div>

          <div className="pf-curs">
            {['USD', 'EUR', 'XOR'].map(c => (
              <button key={c}
                className={'pf-cur-btn' + (c === cur ? ' active' : '')}
                onClick={() => setCur(c)}>{c}</button>
            ))}
          </div>

          <div className="pf-wallets">
            {PF_WALLETS.map((w, i) => (
              <div key={w.name} className="pf-wallet-card">
                <div className="pf-wallet-head">
                  <div className="pf-wallet-av">{w.name[0]}</div>
                  <div style={{flex:1, minWidth:0}}>
                    <div className="pf-wallet-name">{w.name}</div>
                    <div className="pf-wallet-addr">{fmt.addr(w.addr, 5, 4)}</div>
                  </div>
                </div>
                <div className="pf-wallet-value num">{fmtCur(total * w.share)}</div>
                <div className={'pf-wallet-delta ' + (w.delta >= 0 ? 'stat-delta up' : 'stat-delta down')}>
                  {w.delta >= 0 ? '▲' : '▼'} {Math.abs(w.delta).toFixed(1)}% 24h
                </div>
              </div>
            ))}
          </div>
        </div>

        {/* Donut */}
        <div className="card">
          <div className="card-header">
            <div className="card-title"><span className="dot"/> Allocation</div>
            <div className="segmented" style={{transform:'scale(0.9)', transformOrigin:'right'}}>
              <button className="active">By Token</button>
              <button>By Wallet</button>
            </div>
          </div>
          <div className="card-body">
            <div className="pf-donut-wrap">
              <Donut slices={slices}/>
              <div className="donut-legend">
                {holdings.sort((a,b) => b.value - a.value).map(h => (
                  <div key={h.sym} className="lg-row" style={{ ['--c']: TOKENS[h.sym]?.color }}>
                    <span className="lg-dot"/>
                    <span className="lg-sym">{h.sym}</span>
                    <span className="lg-val">{fmtCur(h.value)}</span>
                    <span className="lg-pct">{(h.value / total * 100).toFixed(1)}%</span>
                  </div>
                ))}
              </div>
            </div>
          </div>
        </div>
      </div>

      {/* Holdings table */}
      <div className="card" style={{marginTop: 18}}>
        <div className="card-header">
          <div className="card-title"><span className="dot"/> Holdings · Live</div>
          <div className="row">
            <span className="tag ok"><span className="live-dot" style={{width:5,height:5}}/> price feed</span>
            <span className="tag">{holdings.length} tokens</span>
          </div>
        </div>
        <div className="card-body" style={{padding: 0}}>
          <table className="holdings-table">
            <thead>
              <tr>
                <th style={{paddingLeft: 20}}>Token</th>
                <th className="num" style={{textAlign:'right'}}>Amount</th>
                <th className="num" style={{textAlign:'right'}}>Price</th>
                <th className="num" style={{textAlign:'right'}}>24h</th>
                <th style={{textAlign:'right'}}>Chart</th>
                <th className="num" style={{textAlign:'right', paddingRight: 20}}>Value</th>
              </tr>
            </thead>
            <tbody>
              {holdings.sort((a,b) => b.value - a.value).map((h, i) => {
                const tk = TOKENS[h.sym] || {};
                const sp = sparks[h.sym];
                const change = sp ? ((sp[sp.length-1] - sp[0]) / sp[0] * 100) : 0;
                return (
                  <tr key={h.sym}>
                    <td style={{paddingLeft: 20}}>
                      <div className="token-cell">
                        <div className="token-logo" style={{ background: tk.grad }}>{h.sym[0]}</div>
                        <div>
                          <div style={{fontWeight: 700, color: 'var(--fg-0)'}}>{h.sym}</div>
                          <div className="muted tiny">{tk.name || h.sym}</div>
                        </div>
                      </div>
                    </td>
                    <td className="num" style={{textAlign:'right'}}>{fmt.num(h.amt, h.amt > 1000 ? 0 : 4)}</td>
                    <td className="num" style={{textAlign:'right'}}>${h.price < 1 ? h.price.toFixed(4) : h.price.toFixed(2)}</td>
                    <td className="num" style={{textAlign:'right'}}>
                      <span style={{color: change >= 0 ? '#6EE7B7' : '#FCA5A5', fontWeight: 700}}>
                        {change >= 0 ? '+' : ''}{change.toFixed(2)}%
                      </span>
                    </td>
                    <td style={{textAlign:'right'}}>
                      <MiniSpark data={sp} color={change >= 0 ? '#10B981' : '#EF4444'}/>
                    </td>
                    <td className="num" style={{textAlign:'right', paddingRight: 20, fontWeight: 700, color: 'var(--fg-0)'}}>
                      {fmtCur(h.value)}
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      </div>
      <AddWalletModal open={addOpen} onClose={() => setAddOpen(false)}/>
    </div>
  );
}

Object.assign(window, { PortfolioSection });
