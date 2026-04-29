/* global window, React */
// ============================================================
// js/minamoto/blocks.jsx
// Paginated blocks table (DB-backed, refreshed every 30 s).
// ============================================================

window.MN = window.MN || {};

(function (MN) {
  // Shared Pager — uses v2's .swaps-pag / .btn / .pag-indicator classes so the
  // pagination on Minamoto reads identically to v2's extrinsics/swaps lists.
  function Pager({ page, totalPages, onChange }) {
    if (totalPages <= 1) return null;
    return (
      <div className="swaps-pag">
        <button className="btn" disabled={page <= 1}
                onClick={() => onChange(Math.max(1, page - 1))}>‹ Prev</button>
        <span className="pag-indicator">{page} / {totalPages}</span>
        <button className="btn" disabled={page >= totalPages}
                onClick={() => onChange(Math.min(totalPages, page + 1))}>Next ›</button>
      </div>
    );
  }

  // Shared Table — wraps the v2 .card + .swaps-table-wrap pattern. All MN
  // sections (blocks, tx, instructions, accounts, claims, peers…) feed into
  // this so they share the same row metrics, hover state, mobile collapse.
  function Table({ columns, rows, empty }) {
    if (!rows || rows.length === 0) {
      return <div className="card" style={{ padding: 32, textAlign: 'center', color: 'var(--fg-3)' }}>{empty || 'No data'}</div>;
    }
    return (
      <div className="card" style={{ padding: 0, overflow: 'hidden' }}>
        <div className="swaps-table-wrap responsive-table">
          <table className="swaps-table">
            <thead>
              <tr>
                {columns.map(c => <th key={c.key}>{c.label}</th>)}
              </tr>
            </thead>
            <tbody>
              {rows.map((r, i) => (
                <tr key={r.__key || i}>
                  {columns.map(c => (
                    <td key={c.key} data-label={typeof c.label === 'string' ? c.label : ''}>
                      {c.render ? c.render(r) : r[c.key]}
                    </td>
                  ))}
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
    );
  }

  function Hash({ hex, head, tail, href }) {
    if (!hex) return <span style={{ color: '#6b7280' }}>—</span>;
    if (href) {
      return (
        <a className="mono" href={href} title={hex}
           style={{ color: '#C8A0B8', textDecoration: 'none', borderBottom: '1px dotted rgba(200,160,184,0.40)' }}>
          {MN.shortHash(hex, head || 8, tail || 6)}
        </a>
      );
    }
    return (
      <span className="mono" style={{ color: '#C8A0B8', cursor: 'pointer' }} title={hex}
            onClick={() => MN.copy(hex)}>
        {MN.shortHash(hex, head || 8, tail || 6)}
      </span>
    );
  }

  function Kpi({ label, value, sub, accent }) {
    return (
      <div className="stat-card" style={accent ? { borderTop: '2px solid ' + accent } : null}>
        <span className="stat-label">{label}</span>
        <span className="stat-value num">{value}</span>
        {sub && <span className="stat-sub">{sub}</span>}
      </div>
    );
  }

  function BlockList() {
    const t = MN.i18n.useT();
    const [page, setPage] = React.useState(1);
    const perPage = 25;
    const { data, error, loading } = MN.useFetch(`/blocks?page=${page}&per_page=${perPage}`, 30_000);
    const stats = MN.useFetch('/blocks/stats', 60_000);
    const s = stats.data || {};

    const cols = [
      { key: 'height', label: t('common.height'), render: r => (
        <a href={'#block/' + r.height} className="num"
           style={{ color: 'var(--fg-0)', fontWeight: 700, textDecoration: 'none' }}>
          #{MN.fmt.int(r.height)}
        </a>
      ) },
      { key: 'hash', label: t('common.hash'), render: r => <Hash hex={r.hash} href={'#block/' + r.height} /> },
      { key: 'created_at', label: t('common.created'), render: r => (
        <span>
          <span style={{ color: 'var(--fg-0)' }}>{MN.fmt.relative(r.created_at)}</span>
          <span className="muted tiny" style={{ marginLeft: 6 }}>{r.created_at}</span>
        </span>
      ) },
      // Tx column — green count for committed, red tag for failed (v2 .tag.err
      // pattern). Replaces the previous "ko" jargon which was made up.
      { key: 'tx', label: 'Tx', render: r => (
        <span>
          <span className="num" style={{ color: '#6EE7B7', fontWeight: 600 }}>{MN.fmt.int(r.transactions_committed)}</span>
          {r.transactions_rejected ? (
            <span className="tag err" style={{ marginLeft: 8 }}>
              +{MN.fmt.int(r.transactions_rejected)} {t('common.failed')}
            </span>
          ) : null}
        </span>
      ) },
    ];
    const rows = (data && data.items ? data.items : []).map(b => ({ ...b, __key: b.height }));

    return (
      <section className="section">
        <div className="page-header">
          <div>
            <h1 className="page-title">{t('blocks.title')}</h1>
            <div className="page-sub">{data ? `${MN.fmt.int(data.total)} total` : t('common.loading')}</div>
          </div>
        </div>

        <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(170px, 1fr))', gap: 12, marginBottom: 18 }}>
          <Kpi label={t('blocks.kpi.height')}     value={s.latest_height != null ? '#' + MN.fmt.int(s.latest_height) : '—'} sub={t('blocks.kpi.heightSub')} accent="#A062B0" />
          <Kpi label={t('blocks.kpi.blocks24h')}  value={MN.fmt.int(s.blocks_24h)}                                          sub={t('blocks.kpi.blocks24hSub')} />
          <Kpi label={t('blocks.kpi.tx24h')}      value={MN.fmt.int(s.tx_24h)}                                              sub={t('blocks.kpi.tx24hSub')} />
          <Kpi label={t('blocks.kpi.empty24h')}   value={MN.fmt.int(s.empty_24h)}                                           sub={s.blocks_24h ? ((s.empty_24h / s.blocks_24h) * 100).toFixed(1) + '%' : null} />
          <Kpi label={t('blocks.kpi.avgBlock')}   value={s.avg_block_ms != null ? MN.fmt.ms(s.avg_block_ms) : '—'}           sub={t('blocks.kpi.avgBlockSub')} />
        </div>

        {error && <div style={{ color: 'var(--err)', padding: 12 }}>{error.message}</div>}
        <Table columns={cols} rows={rows} empty={loading ? t('common.loading') : t('common.empty')} />
        <Pager page={page} totalPages={data ? data.total_pages : 1} onChange={setPage} />
      </section>
    );
  }

  function BlockDetail({ height }) {
    const t = MN.i18n.useT();
    const head = MN.useFetch(`/block/${encodeURIComponent(height)}`, 30_000);
    const txs = MN.useFetch(`/transactions?block=${encodeURIComponent(height)}&per_page=100`, 30_000);
    const b = head.data;
    if (head.error) return <div style={{ color: '#EF4444', padding: 16 }}>{head.error.message}</div>;
    if (!b) return <div style={{ color: '#9ca3af', padding: 32, textAlign: 'center' }}>{t('common.loading')}</div>;

    const txList = txs.data && txs.data.items ? txs.data.items : [];

    return (
      <section className="section">
        <div style={{ marginBottom: 12 }}>
          <a href="#blocks" style={{ color: 'var(--fg-2)', fontSize: 13, textDecoration: 'none' }}>← {t('blocks.title')}</a>
        </div>
        <div className="page-header">
          <div>
            <h1 className="page-title">{t('block.title')} <span className="num">#{MN.fmt.int(b.height)}</span></h1>
            <div className="page-sub">{MN.fmt.relative(b.created_at)} · {b.created_at}</div>
          </div>
        </div>
        <div className="card" style={{ padding: '20px 22px', marginBottom: 18 }}>
          <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(200px, 1fr))', gap: 14 }}>
            <Field k={t('block.hash')} v={b.hash} mono copy />
            <Field k={t('block.prevHash')} v={b.prev_block_hash || b.prev_hash} mono copy
                   linkHref={(b.prev_block_hash || b.prev_hash) && b.height > 1 ? '#block/' + (b.height - 1) : null} />
            <Field k={t('block.txsHash')} v={b.transactions_hash} mono copy />
            <Field k={t('block.txTotal')} v={b.transactions_total != null ? b.transactions_total : (b.transactions_committed != null ? b.transactions_committed : '—')} />
            <Field k={t('block.txRejected')} v={b.transactions_rejected != null ? b.transactions_rejected : '—'} />
          </div>
        </div>
        <div className="card" style={{ padding: 0, overflow: 'hidden' }}>
          <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'baseline', padding: '14px 18px', borderBottom: '1px solid var(--border)' }}>
            <h3 style={{ margin: 0, color: 'var(--fg-0)', fontSize: 14, fontWeight: 700 }}>{t('block.txList')}</h3>
            <span className="tag">{txList.length}</span>
          </div>
          {txList.length === 0 && <div style={{ padding: 24, textAlign: 'center', color: 'var(--fg-3)', fontSize: 12 }}>{t('common.empty')}</div>}
          {txList.length > 0 && (
            <div className="swaps-table-wrap responsive-table">
              <table className="swaps-table">
                <thead><tr>
                  <th>{t('common.hash')}</th>
                  <th>{t('common.authority')}</th>
                  <th>{t('common.kind')}</th>
                  <th style={{ textAlign: 'center' }}>{t('common.status')}</th>
                </tr></thead>
                <tbody>
                  {txList.map(tx => (
                    <tr key={tx.hash}>
                      <td data-label={t('common.hash')}>
                        <a className="num" href={'#tx/' + tx.hash} title={tx.hash}
                           style={{ color: 'var(--accent)', textDecoration: 'none' }}>{MN.shortHash(tx.hash, 8, 6)}</a>
                      </td>
                      <td data-label={t('common.authority')}>
                        <a className="num" href={MN.walletHref ? MN.walletHref(tx.authority) : '#wallet/' + encodeURIComponent(tx.authority)}
                           style={{ color: 'var(--accent)', textDecoration: 'none' }} title={tx.authority}>{MN.shortAccount(tx.authority)}</a>
                      </td>
                      <td data-label={t('common.kind')} className="muted">{tx.executable}</td>
                      <td data-label={t('common.status')} style={{ textAlign: 'center' }}>
                        {tx.status === 'Committed'
                          ? <span className="status-pill ok" title="Committed">✓</span>
                          : <span className="status-pill err" title={tx.status}>✗</span>}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </div>

        {/* Instructions emitted in this block — mirrors the v2 pattern of
            showing events inside each block. Pulls from mn.instructions
            with block_height = X. Empty when the block was a vote/heartbeat
            and emitted no ISIs. */}
        {MN.ui.InstructionsList && (
          <div style={{ marginTop: 18 }}>
            <MN.ui.InstructionsList block={Number(height)} title={t('block.instructionsTitle')} />
          </div>
        )}
      </section>
    );
  }

  function Field({ k, v, mono, copy, linkHref }) {
    const valStyle = { color: '#fafafa', fontSize: 13, fontWeight: 500, wordBreak: 'break-all' };
    if (mono) Object.assign(valStyle, { fontFamily: 'JetBrains Mono, monospace', color: '#e5e7eb' });
    return (
      <div>
        <div style={{ color: '#9ca3af', fontSize: 11, fontWeight: 600, letterSpacing: '0.10em', textTransform: 'uppercase', marginBottom: 4 }}>{k}</div>
        {linkHref
          ? <a href={linkHref} style={{ ...valStyle, textDecoration: 'none', borderBottom: '1px dotted rgba(255,255,255,0.20)' }}>{v}</a>
          : <span style={valStyle} onClick={copy ? () => MN.copy(v) : null}>{v}</span>}
      </div>
    );
  }
  function Blocks() {
    const [hash, setHash] = React.useState(() => (window.location.hash || '').replace(/^#/, ''));
    React.useEffect(() => {
      function on() { setHash((window.location.hash || '').replace(/^#/, '')); }
      window.addEventListener('hashchange', on);
      return () => window.removeEventListener('hashchange', on);
    }, []);
    if (hash.startsWith('block/')) {
      const h = decodeURIComponent(hash.slice('block/'.length));
      if (h) return <BlockDetail height={h} />;
    }
    return <BlockList />;
  }

  MN.Blocks = Blocks;
  // Reusable pieces for other pages
  MN.ui = { Table, Pager, Hash };
})(window.MN);
