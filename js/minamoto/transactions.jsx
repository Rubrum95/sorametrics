/* global window, React */
// ============================================================
// js/minamoto/transactions.jsx
// Paginated transactions table, with status filter.
// ============================================================

window.MN = window.MN || {};

(function (MN) {
  // v2 .status-pill — ✓ green / ✗ red circle. Same component used inside
  // every Minamoto table that displays tx status, so users see the exact same
  // success/fail glyph as in /sorav2 extrinsics.
  function StatusPill({ status }) {
    return status === 'Committed'
      ? <span className="status-pill ok" title={status}>✓</span>
      : <span className="status-pill err" title={status}>✗</span>;
  }

  // KPI tile reused by every list page — same .stat-card class as v2.
  function Kpi({ label, value, sub, accent }) {
    return (
      <div className="stat-card" style={accent ? { borderTop: '2px solid ' + accent } : null}>
        <span className="stat-label">{label}</span>
        <span className="stat-value num">{value}</span>
        {sub && <span className="stat-sub">{sub}</span>}
      </div>
    );
  }

  function TxList() {
    const t = MN.i18n.useT();
    const [page, setPage] = React.useState(1);
    const [status, setStatus] = React.useState('');
    const perPage = 25;
    const qs = `?page=${page}&per_page=${perPage}` + (status ? `&status=${encodeURIComponent(status)}` : '');
    const { data, error, loading } = MN.useFetch('/transactions' + qs, 30_000);
    const stats = MN.useFetch('/transactions/stats', 60_000);
    const fee   = MN.useFetch('/transactions/fee-sponsorship', 60_000);
    const s = stats.data || {};
    const f = fee.data || {};

    const cols = [
      { key: 'hash', label: t('common.hash'), render: r => <MN.ui.Hash hex={r.hash} href={'#tx/' + r.hash} /> },
      { key: 'block', label: t('common.height'), render: r => (
        <a href={'#block/' + r.block} className="mono" style={{ color: '#fafafa', textDecoration: 'none', borderBottom: '1px dotted rgba(255,255,255,0.20)' }}>#{MN.fmt.int(r.block)}</a>
      ) },
      { key: 'authority', label: t('common.authority'), render: r => (
        <a className="mono" href={MN.walletHref ? MN.walletHref(r.authority) : '#wallet/' + encodeURIComponent(r.authority)}
           style={{ color: '#C8A0B8', textDecoration: 'none', borderBottom: '1px dotted rgba(200,160,184,0.40)' }}
           title={r.authority}>{MN.shortAccount(r.authority)}</a>
      ) },
      { key: 'kind', label: t('common.kind'), render: r => <span style={{ color: '#9ca3af' }}>{r.executable}</span> },
      { key: 'status', label: t('common.status'), render: r => <StatusPill status={r.status} /> },
      { key: 'created_at', label: t('common.created'), render: r => (
        <span style={{ color: '#fafafa' }}>{MN.fmt.relative(r.created_at)}</span>
      ) },
    ];
    const rows = (data && data.items ? data.items : []).map(x => ({ ...x, __key: x.hash }));

    function FilterBtn({ value, label }) {
      const active = status === value;
      return (
        <button onClick={() => { setStatus(value); setPage(1); }}
          style={{
            padding: '6px 12px', borderRadius: 8, fontSize: 12, fontWeight: 600,
            background: active ? 'rgba(160,98,176,0.20)' : 'transparent',
            color: active ? '#fafafa' : '#9ca3af',
            border: '1px solid ' + (active ? 'rgba(160,98,176,0.40)' : 'rgba(255,255,255,0.10)'),
            cursor: 'pointer',
          }}>{label}</button>
      );
    }

    const sr   = s.success_rate     != null ? (s.success_rate     * 100).toFixed(1) + '%' : '—';
    const sr24 = s.success_rate_24h != null ? (s.success_rate_24h * 100).toFixed(1) + '%' : null;

    return (
      <section className="section">
        <div className="page-header">
          <div>
            <h1 className="page-title">{t('transactions.title')}</h1>
            <div className="page-sub">{data ? `${MN.fmt.int(data.total)} total` : t('common.loading')}</div>
          </div>
          <div className="page-actions">
            <FilterBtn value=""          label="All" />
            <FilterBtn value="Committed" label={t('common.committed')} />
            <FilterBtn value="Rejected"  label={t('common.rejected')} />
          </div>
        </div>

        {/* KPI hero — same density as v2 ExtrinsicsSection */}
        <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(180px, 1fr))', gap: 12, marginBottom: 18 }}>
          <Kpi label={t('tx.kpi.total24h')}    value={MN.fmt.int(s.total_24h)} sub={t('tx.kpi.total24hSub')} accent="#A062B0" />
          <Kpi label={t('tx.kpi.successRate')} value={sr}                       sub={sr24 ? sr24 + ' · 24h' : null} />
          <Kpi label={t('tx.kpi.signers')}     value={MN.fmt.int(s.unique_signers)} sub={t('tx.kpi.signersSub')} />
          <Kpi label={t('tx.kpi.topAuthor')}
               value={s.top_authority ? MN.shortAccount(s.top_authority.authority, 6, 4) : '—'}
               sub={s.top_authority ? MN.fmt.int(s.top_authority.count) + ' ' + t('tx.kpi.txs') : null} />
          <Kpi label={t('tx.kpi.total')}       value={MN.fmt.int(s.total)} />
        </div>

        {/* Fee sponsorship panel — Iroha 3 lets one account pay the gas
            for another's tx (`CanUseFeeSponsor` permission). The block
            payload does NOT expose the fee amount so we surface counts
            and the top sponsors, not value flows. */}
        {f.sponsored > 0 && (
          <div className="card" style={{ padding: '14px 18px', marginBottom: 14 }}>
            <div className="stat-label" style={{ marginBottom: 10 }}>
              {t('tx.fee.title')} <span className="muted tiny" style={{ marginLeft: 8, fontWeight: 400, letterSpacing: 0, textTransform: 'none' }}>{t('tx.fee.subtitle')}</span>
            </div>
            <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(170px, 1fr))', gap: 12, marginBottom: 12 }}>
              <Kpi label={t('tx.fee.sponsored')}     value={MN.fmt.int(f.sponsored)}
                   sub={f.total_tx ? ((f.sponsored / f.total_tx) * 100).toFixed(1) + '% ' + t('tx.fee.ofAll') : null} accent="#F59E0B" />
              <Kpi label={t('tx.fee.sponsored24h')}  value={MN.fmt.int(f.sponsored_24h)} sub={t('tx.fee.last24h')} />
              <Kpi label={t('tx.fee.sponsors')}      value={MN.fmt.int(f.distinct_sponsors)} sub={t('tx.fee.distinct')} />
              <Kpi label={t('tx.fee.beneficiaries')} value={MN.fmt.int(f.distinct_sponsored_signers)} sub={t('tx.fee.distinctSigners')} />
            </div>
            {(f.top_sponsors || []).length > 0 && (
              <div>
                <div className="muted tiny" style={{ marginBottom: 6 }}>{t('tx.fee.topSponsors')}</div>
                <div style={{ display: 'flex', flexDirection: 'column', gap: 4 }}>
                  {f.top_sponsors.map(sp => (
                    <a key={sp.sponsor} href={MN.walletHref(sp.sponsor)}
                       style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center',
                                padding: '6px 10px', borderRadius: 6,
                                background: 'rgba(245,158,11,0.06)',
                                border: '1px solid rgba(245,158,11,0.20)',
                                color: 'inherit', textDecoration: 'none', fontSize: 12 }}>
                      <span className="num" style={{ color: 'var(--accent)' }} title={sp.sponsor}>
                        {MN.shortAccount(sp.sponsor, 10, 6)}
                      </span>
                      <span className="num" style={{ color: '#F59E0B', fontWeight: 700 }}>{MN.fmt.int(sp.count)} {t('tx.fee.txs')}</span>
                    </a>
                  ))}
                </div>
              </div>
            )}
          </div>
        )}

        {error && <div style={{ color: 'var(--err)', padding: 12 }}>{error.message}</div>}
        <MN.ui.Table columns={cols} rows={rows} empty={loading ? t('common.loading') : t('common.empty')} />
        <MN.ui.Pager page={page} totalPages={data ? data.total_pages : 1} onChange={setPage} />
      </section>
    );
  }

  function TxDetail({ hash }) {
    const t = MN.i18n.useT();
    const { data, error } = MN.useFetch(`/tx/${encodeURIComponent(hash)}`, 60_000);
    if (error) return <div style={{ color: '#EF4444', padding: 16 }}>{error.message}</div>;
    if (!data) return <div style={{ color: '#9ca3af', padding: 32, textAlign: 'center' }}>{t('common.loading')}</div>;
    const md = data.metadata || {};
    const v2BurnTx = md.sora_v2_claim_tx_hash;
    const claimRecipient = md.sora_nexus_claim_recipient;
    const feeSponsor = md.fee_sponsor;
    return (
      <section className="section">
        <div style={{ marginBottom: 12 }}>
          <a href="#transactions" style={{ color: 'var(--fg-2)', fontSize: 13, textDecoration: 'none' }}>← {t('transactions.title')}</a>
        </div>
        <div className="page-header">
          <div>
            <h1 className="page-title">
              {t('tx.title')}
              {data.executable && <span className="muted" style={{ fontSize: 14, marginLeft: 12, fontWeight: 500 }}>
                {data.executable}{data.executable_payload && data.executable_payload.instruction_count != null ? ` · ${data.executable_payload.instruction_count} ${t('tx.instructions')}` : ''}
              </span>}
            </h1>
            <div className="num page-sub" onClick={() => MN.copy(data.hash)} title={data.hash}
                 style={{ wordBreak: 'break-all', cursor: 'pointer' }}>{data.hash}</div>
          </div>
          <div className="page-actions">
            <StatusPill status={data.status} />
          </div>
        </div>
        <div className="card" style={{ padding: '20px 22px', marginBottom: 18 }}>
          <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(200px, 1fr))', gap: 14 }}>
            <Pair k={t('common.height')} v={'#' + data.block} href={'#block/' + data.block} />
            <Pair k={t('common.created')} v={MN.fmt.relative(data.created_at)} sub={data.created_at} />
            <Pair k={t('common.authority')} v={MN.shortAccount(data.authority)} href={MN.walletHref ? MN.walletHref(data.authority) : '#wallet/' + encodeURIComponent(data.authority)} mono title={data.authority} />
            {feeSponsor && <Pair k={t('tx.feeSponsor')} v={MN.shortAccount(feeSponsor)} href={MN.walletHref ? MN.walletHref(feeSponsor) : '#wallet/' + encodeURIComponent(feeSponsor)} mono title={feeSponsor} />}
          </div>
        </div>

        {(v2BurnTx || claimRecipient) && (
          <div style={{ padding: '18px 20px', borderRadius: 16, marginBottom: 18,
                        background: 'rgba(160,98,176,0.06)', border: '1px solid rgba(160,98,176,0.30)' }}>
            <div style={{ color: '#A062B0', fontSize: 11, fontWeight: 700, letterSpacing: '0.16em', textTransform: 'uppercase', marginBottom: 10 }}>{t('tx.crossChain')}</div>
            <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(280px, 1fr))', gap: 12 }}>
              {v2BurnTx && (
                <div>
                  <div style={{ color: '#9ca3af', fontSize: 11, fontWeight: 600, letterSpacing: '0.08em', textTransform: 'uppercase', marginBottom: 4 }}>{t('tx.v2BurnTx')}</div>
                  <a href={'/sorav2#extrinsics?hash=' + v2BurnTx} className="mono" style={{ color: '#FCD7D5', fontSize: 12, textDecoration: 'none', borderBottom: '1px dotted rgba(252,215,213,0.40)', wordBreak: 'break-all' }}>{v2BurnTx}</a>
                </div>
              )}
              {claimRecipient && (
                <div>
                  <div style={{ color: '#9ca3af', fontSize: 11, fontWeight: 600, letterSpacing: '0.08em', textTransform: 'uppercase', marginBottom: 4 }}>{t('tx.recipient')}</div>
                  <a href={MN.walletHref ? MN.walletHref(claimRecipient) : '#wallet/' + encodeURIComponent(claimRecipient)} className="mono" style={{ color: '#C8A0B8', fontSize: 12, textDecoration: 'none', borderBottom: '1px dotted rgba(200,160,184,0.40)', wordBreak: 'break-all' }}>{claimRecipient}</a>
                </div>
              )}
            </div>
          </div>
        )}

        {Object.keys(md).length > 0 && (
          <div style={{ border: '1px solid rgba(255,255,255,0.07)', borderRadius: 16, background: 'rgba(15,15,20,0.6)', overflow: 'hidden', marginBottom: 18 }}>
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'baseline', padding: '12px 16px', background: 'rgba(255,255,255,0.02)', borderBottom: '1px solid rgba(255,255,255,0.06)' }}>
              <h3 style={{ margin: 0, color: '#fafafa', fontSize: 14, fontWeight: 700 }}>{t('tx.metadata')}</h3>
            </div>
            <pre className="mono" style={{ margin: 0, padding: 14, color: '#C8A0B8', fontSize: 12, overflow: 'auto', maxHeight: 400 }}>{JSON.stringify(md, null, 2)}</pre>
          </div>
        )}

        {/* Instructions emitted by this transaction — equivalent to events
            in v2 sorametrics shown inside each extrinsic. */}
        {MN.ui.InstructionsList && (
          <div style={{ marginBottom: 18 }}>
            <MN.ui.InstructionsList txHash={hash} title={t('tx.instructionsTitle')} />
          </div>
        )}

        {data.signature && (
          <div style={{ border: '1px solid rgba(255,255,255,0.07)', borderRadius: 16, background: 'rgba(15,15,20,0.6)', overflow: 'hidden' }}>
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'baseline', padding: '12px 16px', background: 'rgba(255,255,255,0.02)', borderBottom: '1px solid rgba(255,255,255,0.06)' }}>
              <h3 style={{ margin: 0, color: '#fafafa', fontSize: 14, fontWeight: 700 }}>{t('tx.signature')}</h3>
            </div>
            <div className="mono" style={{ padding: 14, color: '#9ca3af', fontSize: 11, wordBreak: 'break-all' }}>{data.signature}</div>
          </div>
        )}
      </section>
    );
  }

  function Pair({ k, v, href, sub, mono, title }) {
    const valStyle = mono ? { fontFamily: 'JetBrains Mono, monospace', color: '#C8A0B8' } : { color: '#fafafa' };
    return (
      <div>
        <div style={{ color: '#9ca3af', fontSize: 11, fontWeight: 600, letterSpacing: '0.10em', textTransform: 'uppercase', marginBottom: 4 }}>{k}</div>
        {href
          ? <a href={href} title={title} style={{ ...valStyle, fontSize: 13, textDecoration: 'none', borderBottom: '1px dotted rgba(255,255,255,0.20)' }}>{v}</a>
          : <span style={{ ...valStyle, fontSize: 13 }} title={title}>{v}</span>}
        {sub && <div style={{ color: '#6b7280', fontSize: 11, marginTop: 2 }}>{sub}</div>}
      </div>
    );
  }

  function Transactions() {
    const [hash, setHash] = React.useState(() => (window.location.hash || '').replace(/^#/, ''));
    React.useEffect(() => {
      function on() { setHash((window.location.hash || '').replace(/^#/, '')); }
      window.addEventListener('hashchange', on);
      return () => window.removeEventListener('hashchange', on);
    }, []);
    if (hash.startsWith('tx/')) {
      const h = decodeURIComponent(hash.slice('tx/'.length));
      if (h) return <TxDetail hash={h} />;
    }
    return <TxList />;
  }

  MN.Transactions = Transactions;
})(window.MN);
