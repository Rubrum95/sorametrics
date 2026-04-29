/* global window, React, Chart */
// ============================================================
// js/minamoto/crosschain.jsx
// XOR migration tracker. Each Minamoto tx that mints XOR carries
// a "sora_v2_claim_tx_hash" pointing back to its burn on SORA v2.
// We aggregate those claims, total amount migrated, unique
// recipients, and a per-hour series. Bidirectional links open
// the v2 burn extrinsic and the recipient wallet.
// ============================================================

window.MN = window.MN || {};

(function (MN) {
  // Iroha 3 / Minamoto stores XOR as Numeric ALREADY in user units (not
  // 1e18-scaled raw like SORA v2). Bridge converts at mint time. So format
  // straight as-is with sane decimal trimming.
  function fmtXor(s) {
    if (s == null || s === '') return '—';
    const n = Number(s);
    if (!Number.isFinite(n)) return s;
    if (n === 0) return '0';
    if (n < 0.001) return n.toFixed(6);
    if (n < 1) return n.toFixed(4);
    if (n < 1000) return n.toFixed(4).replace(/\.?0+$/, '');
    return n.toLocaleString('en-US', { maximumFractionDigits: 4 });
  }

  function Kpi({ label, value, sub }) {
    return (
      <div className="stat-card">
        <span className="stat-label">{label}</span>
        <span className="stat-value num">{value}</span>
        {sub && <span className="stat-sub">{sub}</span>}
      </div>
    );
  }

  function TimeChart({ hours = 168 }) {
    const t = MN.i18n.useT();
    const { data } = MN.useFetch(`/cross-chain/timeseries?hours=${hours}`, 60_000);
    const ref = React.useRef(null);
    const chartRef = React.useRef(null);
    const series = (data && data.series) || [];

    React.useEffect(() => {
      if (!ref.current) return;
      if (series.length === 0) {
        if (chartRef.current) { chartRef.current.destroy(); chartRef.current = null; }
        return;
      }
      const labels = series.map(s => new Date(s.bucket).toLocaleString());
      const claims = series.map(s => s.claims);
      const xor = series.map(s => Number(s.xor_claimed));
      if (chartRef.current) chartRef.current.destroy();
      chartRef.current = new Chart(ref.current.getContext('2d'), {
        type: 'bar',
        data: {
          labels,
          datasets: [
            { type: 'bar', label: 'Claims', data: claims, backgroundColor: 'rgba(160,98,176,0.55)', borderColor: '#A062B0', borderWidth: 1, yAxisID: 'y' },
            { type: 'line', label: 'XOR claimed', data: xor, borderColor: '#10B981', backgroundColor: 'rgba(16,185,129,0.10)', borderWidth: 2, tension: 0.25, pointRadius: 0, yAxisID: 'y1' },
          ],
        },
        options: {
          responsive: true, maintainAspectRatio: false,
          interaction: { mode: 'nearest', intersect: false },
          scales: {
            x: { ticks: { color: '#6b7280', maxRotation: 0, autoSkip: true, maxTicksLimit: 10 }, grid: { display: false } },
            y:  { ticks: { color: '#A062B0' }, grid: { color: 'rgba(255,255,255,0.04)' }, title: { display: true, text: 'Claims', color: '#A062B0' } },
            y1: { position: 'right', ticks: { color: '#10B981' }, grid: { display: false }, title: { display: true, text: 'XOR', color: '#10B981' } },
          },
          plugins: {
            legend: { labels: { color: '#9ca3af', font: { size: 11 } } },
            tooltip: { titleColor: '#fafafa', bodyColor: '#e5e7eb' },
          },
        },
      });
      return () => { if (chartRef.current) { chartRef.current.destroy(); chartRef.current = null; } };
    }, [JSON.stringify(series)]);

    if (series.length === 0) return null;
    return (
      <div style={{ marginTop: 18, padding: '18px 20px', borderRadius: 16, background: 'rgba(20,20,28,0.72)', border: '1px solid rgba(255,255,255,0.07)' }}>
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'baseline', marginBottom: 10 }}>
          <h3 style={{ margin: 0, color: '#fafafa', fontSize: 14, fontWeight: 700 }}>{t('cc.timeChart')}</h3>
          <span style={{ color: '#6b7280', fontSize: 11 }}>last {hours}h</span>
        </div>
        <div style={{ height: 240 }}><canvas ref={ref}></canvas></div>
      </div>
    );
  }

  // Bidirectional claims table. Renders BOTH sides per row — the user is on
  // /minamoto so we orient mn-side as primary (highlighted) and v2-side as
  // counterpart (muted ember). The v2 SPA uses the same component but with
  // perspective='v2' — see SORA v2's xor_migration.jsx.
  function ClaimsTable({ perspective = 'minamoto' }) {
    const t = MN.i18n.useT();
    const [page, setPage] = React.useState(1);
    const perPage = 50;
    const { data, error, loading } = MN.useFetch(`/cross-chain/claims?page=${page}&per_page=${perPage}`, 30_000);
    const items = data && data.items ? data.items : [];

    const isMn = perspective === 'minamoto';

    const cols = [
      { key: 'when', label: t('common.created'), render: r => <span style={{ color: '#9ca3af', fontSize: 12 }}>{MN.fmt.relative(r.created_at)}</span> },
      { key: 'amt', label: 'XOR', render: r => <span className="mono" style={{ color: '#10B981', fontWeight: 700 }}>{fmtXor(r.claimed_amount)}</span> },
      // PRIMARY (current network) BLOCK
      { key: 'this_block', label: isMn ? t('cc.mnBlock') : t('cc.v2Block'), render: r => {
        const b = isMn ? r.mn_block : r.v2_block;
        if (b == null) return <span style={{ color: '#6b7280' }}>—</span>;
        return isMn
          ? <a className="mono" href={'#block/' + b} style={{ color: '#fafafa', textDecoration: 'none', borderBottom: '1px dotted rgba(255,255,255,0.20)' }}>#{MN.fmt.int(b)}</a>
          : <a className="mono" href={'?tab=extrinsics&block=' + b} style={{ color: '#fafafa', textDecoration: 'none', borderBottom: '1px dotted rgba(255,255,255,0.20)' }}>#{MN.fmt.int(b)}</a>;
      } },
      // PRIMARY (current network) TX HASH
      { key: 'this_tx', label: isMn ? t('cc.mnTx') : t('cc.v2Tx'), render: r => {
        const h = isMn ? r.mn_tx_hash : r.v2_tx_hash;
        if (!h) return <span style={{ color: '#6b7280' }}>—</span>;
        return isMn
          ? <a className="mono" href={'#tx/' + h} title={h} style={{ color: '#C8A0B8', textDecoration: 'none', borderBottom: '1px dotted rgba(200,160,184,0.40)' }}>{MN.shortHash(h, 6, 4)}</a>
          : <a className="mono" href={'?tab=extrinsics&q=' + encodeURIComponent(h)} title={h} style={{ color: '#FCD7D5', textDecoration: 'none', borderBottom: '1px dotted rgba(252,215,213,0.40)' }}>{MN.shortHash(h, 8, 4)}</a>;
      } },
      // PRIMARY (current network) ACCOUNT
      { key: 'this_acc', label: isMn ? t('cc.mnRecipient') : t('cc.v2Signer'), render: r => {
        const a = isMn ? r.mn_recipient : r.v2_signer;
        if (!a) return <span style={{ color: '#6b7280' }}>—</span>;
        return isMn
          ? <a className="mono" href={MN.walletHref(a)} title={a} style={{ color: '#C8A0B8', textDecoration: 'none', borderBottom: '1px dotted rgba(200,160,184,0.40)' }}>{MN.shortAccount(a, 8, 6)}</a>
          : <a className="mono" href={'?tab=balance&address=' + encodeURIComponent(a)} title={a} style={{ color: '#FCD7D5', textDecoration: 'none', borderBottom: '1px dotted rgba(252,215,213,0.40)' }}>{a.slice(0, 10)}…{a.slice(-6)}</a>;
      } },
      // OTHER NETWORK BLOCK
      { key: 'other_block', label: isMn ? t('cc.v2Block') : t('cc.mnBlock'), render: r => {
        const b = isMn ? r.v2_block : r.mn_block;
        if (b == null) return <span style={{ color: '#6b7280' }}>—</span>;
        return isMn
          ? <a className="mono" href={'/sorav2?tab=extrinsics&block=' + b} target="_blank" rel="noopener" style={{ color: '#FCD7D5', textDecoration: 'none', borderBottom: '1px dotted rgba(252,215,213,0.40)' }}>#{MN.fmt.int(b)}</a>
          : <a className="mono" href={'/minamoto#block/' + b} target="_blank" rel="noopener" style={{ color: '#C8A0B8', textDecoration: 'none', borderBottom: '1px dotted rgba(200,160,184,0.40)' }}>#{MN.fmt.int(b)}</a>;
      } },
      // OTHER NETWORK TX HASH
      { key: 'other_tx', label: isMn ? t('cc.v2Tx') : t('cc.mnTx'), render: r => {
        const h = isMn ? r.v2_tx_hash : r.mn_tx_hash;
        if (!h) return <span style={{ color: '#6b7280' }}>—</span>;
        return isMn
          ? <a className="mono" href={'/sorav2?tab=extrinsics&q=' + encodeURIComponent(h)} target="_blank" rel="noopener" title={h} style={{ color: '#FCD7D5', textDecoration: 'none', borderBottom: '1px dotted rgba(252,215,213,0.40)' }}>{MN.shortHash(h, 8, 4)}</a>
          : <a className="mono" href={'/minamoto#tx/' + h} target="_blank" rel="noopener" title={h} style={{ color: '#C8A0B8', textDecoration: 'none', borderBottom: '1px dotted rgba(200,160,184,0.40)' }}>{MN.shortHash(h, 6, 4)}</a>;
      } },
      // OTHER NETWORK ACCOUNT
      { key: 'other_acc', label: isMn ? t('cc.v2Signer') : t('cc.mnRecipient'), render: r => {
        const a = isMn ? r.v2_signer : r.mn_recipient;
        if (!a) return <span style={{ color: '#6b7280' }}>—</span>;
        return isMn
          ? <a className="mono" href={'/sorav2?tab=balance&address=' + encodeURIComponent(a)} target="_blank" rel="noopener" title={a} style={{ color: '#FCD7D5', textDecoration: 'none', borderBottom: '1px dotted rgba(252,215,213,0.40)' }}>{a.slice(0, 10)}…{a.slice(-6)}</a>
          : <a className="mono" href={'/minamoto#wallet/' + encodeURIComponent(a)} target="_blank" rel="noopener" title={a} style={{ color: '#C8A0B8', textDecoration: 'none', borderBottom: '1px dotted rgba(200,160,184,0.40)' }}>{MN.shortAccount(a, 8, 6)}</a>;
      } },
    ];
    const rows = items.map(x => ({ ...x, __key: x.mn_tx_hash }));

    return (
      <div style={{ marginTop: 18 }}>
        {error && <div style={{ color: '#EF4444', padding: 12 }}>{error.message}</div>}
        <MN.ui.Table columns={cols} rows={rows} empty={loading ? t('common.loading') : t('cc.noClaims')} />
        <MN.ui.Pager page={page} totalPages={data ? data.total_pages : 1} onChange={setPage} />
      </div>
    );
  }

  // -----------------------------------------------------------
  // MintHistoryPanel — "cuándo se acuñó cada XOR y de dónde".
  // Three sources possible:
  //   - genesis_premint   — minted at block #1 by the genesis admin (no
  //                         v2 burn correspondence; native bootstrap supply)
  //   - cross_chain_claim — bridge mint with sora_v2_claim_tx_hash link
  //   - other_mint        — admin/runtime mint after genesis (rare)
  // The panel shows totals by source + a chronological timeline so the
  // user can answer "where did this XOR come from?" for any wallet.
  // -----------------------------------------------------------
  function SourceBadge({ source }) {
    const cfg = {
      genesis_premint:   { color: '#A062B0', label: 'GÉNESIS' },
      cross_chain_claim: { color: '#10B981', label: 'CROSS-CHAIN' },
      other_mint:        { color: '#F59E0B', label: 'DIRECTO' },
    }[source] || { color: '#9ca3af', label: (source || '?').toUpperCase() };
    return (
      <span style={{
        display: 'inline-block', padding: '2px 8px', borderRadius: 999,
        fontSize: 10, fontWeight: 700, letterSpacing: '0.06em',
        color: cfg.color, background: cfg.color + '14', border: '1px solid ' + cfg.color + '50',
      }}>{cfg.label}</span>
    );
  }

  function MintHistoryPanel() {
    const t = MN.i18n.useT();
    const { data } = MN.useFetch('/cross-chain/mint-history', 60_000);
    if (!data) return null;
    const tl = Array.isArray(data.timeline) ? data.timeline : [];
    const bs = data.by_source || {};
    const genesis = bs.genesis_premint   || { count: 0, amount: 0, recipients: 0 };
    const bridge  = bs.cross_chain_claim || { count: 0, amount: 0, recipients: 0 };
    const other   = bs.other_mint        || { count: 0, amount: 0, recipients: 0 };

    return (
      <div style={{ marginTop: 24 }}>
        <div className="page-header" style={{ marginBottom: 14 }}>
          <div>
            <h2 className="page-title" style={{ fontSize: 18 }}>{t('cc.mint.title')}</h2>
            <div className="page-sub">{t('cc.mint.subtitle')}</div>
          </div>
        </div>

        {/* Source breakdown — 3 cards side-by-side */}
        <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(220px, 1fr))', gap: 12, marginBottom: 14 }}>
          <div className="card" style={{ padding: '14px 16px', borderTop: '2px solid #A062B0' }}>
            <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between' }}>
              <span className="stat-label">{t('cc.mint.genesis')}</span>
              <SourceBadge source="genesis_premint" />
            </div>
            <div className="num" style={{ color: 'var(--fg-0)', fontSize: 22, fontWeight: 700, marginTop: 4 }}>{fmtXor(genesis.amount)} XOR</div>
            <div className="muted tiny" style={{ marginTop: 4 }}>
              {genesis.count} {t('cc.mint.mints')} · {genesis.recipients} {t('cc.mint.recipients')}
            </div>
            <div className="muted tiny" style={{ marginTop: 2 }}>{t('cc.mint.genesisHint')}</div>
          </div>
          <div className="card" style={{ padding: '14px 16px', borderTop: '2px solid #10B981' }}>
            <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between' }}>
              <span className="stat-label">{t('cc.mint.bridge')}</span>
              <SourceBadge source="cross_chain_claim" />
            </div>
            <div className="num" style={{ color: 'var(--fg-0)', fontSize: 22, fontWeight: 700, marginTop: 4 }}>{fmtXor(bridge.amount)} XOR</div>
            <div className="muted tiny" style={{ marginTop: 4 }}>
              {bridge.count} {t('cc.mint.mints')} · {bridge.recipients} {t('cc.mint.recipients')}
            </div>
            <div className="muted tiny" style={{ marginTop: 2 }}>{t('cc.mint.bridgeHint')}</div>
          </div>
          {other.count > 0 && (
            <div className="card" style={{ padding: '14px 16px', borderTop: '2px solid #F59E0B' }}>
              <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between' }}>
                <span className="stat-label">{t('cc.mint.other')}</span>
                <SourceBadge source="other_mint" />
              </div>
              <div className="num" style={{ color: 'var(--fg-0)', fontSize: 22, fontWeight: 700, marginTop: 4 }}>{fmtXor(other.amount)} XOR</div>
              <div className="muted tiny" style={{ marginTop: 4 }}>
                {other.count} {t('cc.mint.mints')} · {other.recipients} {t('cc.mint.recipients')}
              </div>
            </div>
          )}
          <div className="card" style={{ padding: '14px 16px', background: 'rgba(160,98,176,0.08)' }}>
            <span className="stat-label">{t('cc.mint.total')}</span>
            <div className="num" style={{ color: 'var(--accent)', fontSize: 24, fontWeight: 800, marginTop: 4 }}>{fmtXor(data.total_xor)} XOR</div>
            <div className="muted tiny" style={{ marginTop: 4 }}>{data.total_mints} {t('cc.mint.totalMints')}</div>
          </div>
        </div>

        {/* Chronological timeline */}
        <div className="card" style={{ padding: 0, overflow: 'hidden' }}>
          <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'baseline',
                        padding: '14px 18px', borderBottom: '1px solid var(--border)' }}>
            <h3 style={{ margin: 0, color: 'var(--fg-0)', fontSize: 14, fontWeight: 700 }}>{t('cc.mint.timeline')}</h3>
            <span className="muted tiny">{t('cc.mint.chronological')}</span>
          </div>
          <div className="swaps-table-wrap responsive-table">
            <table className="swaps-table">
              <thead>
                <tr>
                  <th>{t('cc.mint.col.when')}</th>
                  <th>{t('cc.mint.col.block')}</th>
                  <th>{t('cc.mint.col.source')}</th>
                  <th style={{ textAlign: 'right' }}>XOR</th>
                  <th>{t('cc.mint.col.recipient')}</th>
                  <th>{t('cc.mint.col.proof')}</th>
                </tr>
              </thead>
              <tbody>
                {tl.length === 0 && (
                  <tr><td colSpan={6} style={{ padding: 24, textAlign: 'center', color: 'var(--fg-3)' }}>{t('common.empty')}</td></tr>
                )}
                {tl.map(e => (
                  <tr key={e.tx_hash}>
                    <td data-label={t('cc.mint.col.when')}>
                      <span className="muted tiny">{MN.fmt.relative(e.at)}</span>
                      <div className="muted tiny" style={{ fontSize: 10, marginTop: 2 }}>{e.at && e.at.replace('T', ' ').slice(0, 19)}</div>
                    </td>
                    <td data-label={t('cc.mint.col.block')}>
                      <a className="num" href={'#block/' + e.block}
                         style={{ color: 'var(--fg-1)', fontSize: 12, textDecoration: 'none' }}>#{MN.fmt.int(e.block)}</a>
                    </td>
                    <td data-label={t('cc.mint.col.source')}>
                      <SourceBadge source={e.source} />
                    </td>
                    <td data-label="XOR" className="num" style={{ textAlign: 'right', color: '#10B981', fontWeight: 700 }}>
                      {fmtXor(e.amount)}
                    </td>
                    <td data-label={t('cc.mint.col.recipient')}>
                      {e.recipient ? (
                        <a className="num" href={MN.walletHref(e.recipient)} title={e.recipient}
                           style={{ color: 'var(--accent)', fontSize: 11, textDecoration: 'none', borderBottom: '1px dotted rgba(255,255,255,0.20)' }}>
                          {MN.shortAccount(e.recipient, 8, 6)}
                        </a>
                      ) : <span className="muted">—</span>}
                    </td>
                    <td data-label={t('cc.mint.col.proof')}>
                      {e.v2_burn_tx ? (
                        <a className="num" href={'/sorav2?tab=extrinsics&q=' + encodeURIComponent(e.v2_burn_tx)}
                           target="_blank" rel="noopener" title={e.v2_burn_tx}
                           style={{ color: '#FCD7D5', fontSize: 11, textDecoration: 'none', borderBottom: '1px dotted rgba(252,215,213,0.40)' }}>
                          v2 burn {MN.shortHash(e.v2_burn_tx, 6, 4)}
                        </a>
                      ) : (
                        <a className="num" href={'#tx/' + e.tx_hash}
                           style={{ color: 'var(--accent)', fontSize: 11, textDecoration: 'none' }}>
                          mn tx {MN.shortHash(e.tx_hash, 6, 4)}
                        </a>
                      )}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
        <div className="muted tiny" style={{ marginTop: 8 }}>{t('cc.mint.note')}</div>
      </div>
    );
  }

  function CrossChain() {
    const t = MN.i18n.useT();
    const stats = MN.useFetch('/cross-chain/stats', 30_000);
    const xor   = MN.useFetch('/asset/xor%23universal', 60_000);
    // First page of claims drives the "largest single claim" + "avg claim"
    // mini-stats. Cheap (50 rows) and refreshed every 60s alongside the
    // main stats card.
    const recent = MN.useFetch('/cross-chain/claims?per_page=50', 60_000);
    const s = stats.data || {};
    const xorSupply = xor.data && xor.data.total_quantity ? xor.data.total_quantity : null;
    const migrated = parseFloat(s.total_xor_claimed || '0');
    const supplyN = parseFloat(xorSupply || '0');
    const otherMint = supplyN > migrated ? (supplyN - migrated) : 0;
    const migratedPct = supplyN > 0 ? (migrated / supplyN) * 100 : 0;

    const claims = (recent.data && recent.data.items) || [];
    const claimAmounts = claims.map(c => parseFloat(c.claimed_amount || '0')).filter(Number.isFinite);
    const largest = claimAmounts.length ? Math.max(...claimAmounts) : 0;
    const avg     = s.total_claims > 0 ? migrated / s.total_claims : 0;

    return (
      <section className="section">
        <div className="page-header">
          <div>
            <h1 className="page-title">{t('cc.title')}</h1>
            <div className="page-sub">{t('cc.subtitle')}</div>
          </div>
        </div>

        <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(180px, 1fr))', gap: 14 }}>
          <Kpi label={t('cc.supplyTotal')}     value={fmtXor(xorSupply)}            sub={t('cc.supplyTotalSub')} />
          <Kpi label={t('cc.totalXor')}        value={fmtXor(s.total_xor_claimed)}  sub={migratedPct ? migratedPct.toFixed(1) + '% ' + t('cc.ofSupply') : t('cc.viaClaim')} />
          <Kpi label={t('cc.otherMint')}       value={fmtXor(otherMint)}            sub={t('cc.otherMintSub')} />
          <Kpi label={t('cc.totalClaims')}     value={MN.fmt.int(s.total_claims)} />
          <Kpi label={t('cc.uniqueRecipients')} value={MN.fmt.int(s.unique_recipients)} />
          <Kpi label={t('cc.lastClaim')}       value={s.last_claim_at ? MN.fmt.relative(s.last_claim_at) : '—'} sub={s.last_claim_at} />
          <Kpi label={t('cc.largestClaim')}    value={fmtXor(largest)}              sub={t('cc.largestClaimSub')} />
          <Kpi label={t('cc.avgClaim')}        value={fmtXor(avg)}                  sub={t('cc.avgClaimSub')} />
        </div>

        <div style={{
          marginTop: 14, padding: '12px 16px', borderRadius: 12,
          background: 'rgba(96,165,250,0.08)', border: '1px solid rgba(96,165,250,0.20)',
          color: '#9ca3af', fontSize: 12, lineHeight: 1.6,
        }}>
          {t('cc.discrepancyNote')}
        </div>

        <TimeChart hours={168} />
        <ClaimsTable />
        <MintHistoryPanel />
      </section>
    );
  }

  MN.CrossChain = CrossChain;
})(window.MN);
