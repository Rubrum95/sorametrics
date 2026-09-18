/* global window, React */
// ============================================================
// js/minamoto/instructions.jsx
// Iroha 3 ISI feed: every fine-grained action on-chain (Mint,
// Burn, Transfer, Register, Grant, SetKeyValue…). One transaction
// can emit many instructions. This is the closest equivalent to
// the v2 "Events" / "Extrinsics" tab.
// ============================================================

window.MN = window.MN || {};

(function (MN) {
  // -----------------------------------------------------------
  // Kind colour palette — keeps the table scannable.
  // -----------------------------------------------------------
  const KIND_COLORS = {
    Mint:           { bg: 'rgba(16,185,129,0.14)', fg: '#10B981', border: 'rgba(16,185,129,0.30)' },
    Burn:           { bg: 'rgba(239,68,68,0.14)',  fg: '#EF4444', border: 'rgba(239,68,68,0.30)' },
    Transfer:       { bg: 'rgba(96,165,250,0.14)', fg: '#60A5FA', border: 'rgba(96,165,250,0.30)' },
    Register:       { bg: 'rgba(160,98,176,0.14)', fg: '#A062B0', border: 'rgba(160,98,176,0.30)' },
    Unregister:     { bg: 'rgba(251,113,133,0.14)', fg: '#FB7185', border: 'rgba(251,113,133,0.30)' },
    Grant:          { bg: 'rgba(245,158,11,0.14)', fg: '#F59E0B', border: 'rgba(245,158,11,0.30)' },
    Revoke:         { bg: 'rgba(217,70,140,0.14)', fg: '#D9468C', border: 'rgba(217,70,140,0.30)' },
    SetKeyValue:    { bg: 'rgba(139,92,246,0.14)', fg: '#8B5CF6', border: 'rgba(139,92,246,0.30)' },
    RemoveKeyValue: { bg: 'rgba(124,58,237,0.10)', fg: '#A78BFA', border: 'rgba(124,58,237,0.30)' },
    Upgrade:        { bg: 'rgba(34,197,94,0.10)',  fg: '#22C55E', border: 'rgba(34,197,94,0.30)' },
    Custom:         { bg: 'rgba(255,255,255,0.04)', fg: '#9CA3AF', border: 'rgba(255,255,255,0.10)' },
    ExecuteTrigger: { bg: 'rgba(244,114,182,0.14)', fg: '#F472B6', border: 'rgba(244,114,182,0.30)' },
  };

  function KindBadge({ kind }) {
    const c = KIND_COLORS[kind] || KIND_COLORS.Custom;
    return (
      <span style={{
        display: 'inline-block', padding: '2px 8px', borderRadius: 999,
        fontSize: 11, fontWeight: 700, letterSpacing: '0.04em',
        background: c.bg, color: c.fg, border: '1px solid ' + c.border,
      }}>{kind}</span>
    );
  }

  // -----------------------------------------------------------
  // Render the most-useful one-liner from the payload, per kind.
  // For unknown shapes we degrade gracefully to a JSON snippet.
  // -----------------------------------------------------------
  function PayloadLine({ kind, payload }) {
    if (!payload || typeof payload !== 'object') return <span style={{ color: '#6b7280' }}>—</span>;
    const v = payload.value || payload;

    // Mint / Burn: { destination: "asset#account", object: "qty" }
    // Transfer:    { source, destination, object: "qty" }
    if (kind === 'Mint' || kind === 'Burn') {
      const dest = v.destination || '';
      const obj  = v.object;
      const variant = payload.variant || v.variant;
      const [defId, accId] = String(dest).split('#');
      return (
        <span>
          <span style={{ color: '#fafafa', fontWeight: 600 }}>{obj != null ? obj : '?'}</span>
          {variant === 'Asset' && defId && (
            <>
              <span style={{ color: '#9ca3af' }}> {variant?.toLowerCase() || ''} </span>
              <a href={'#asset/' + encodeURIComponent(defId)} className="mono"
                 style={{ color: '#C8A0B8', textDecoration: 'none', borderBottom: '1px dotted rgba(200,160,184,0.40)' }}>
                {MN.shortHash(defId, 6, 4)}
              </a>
            </>
          )}
          {accId && (
            <>
              <span style={{ color: '#9ca3af' }}> → </span>
              <a className="mono"
                 href={MN.walletHref ? MN.walletHref(accId) : '#wallet/' + encodeURIComponent(accId)}
                 style={{ color: '#C8A0B8', textDecoration: 'none', borderBottom: '1px dotted rgba(200,160,184,0.40)' }}
                 title={accId}>
                {MN.shortAccount(accId, 8, 6)}
              </a>
            </>
          )}
        </span>
      );
    }

    if (kind === 'Transfer') {
      const src = v.source;
      const dest = v.destination;
      const obj = v.object;
      return (
        <span>
          <span style={{ color: '#fafafa', fontWeight: 600 }}>{obj != null ? obj : '?'}</span>
          {src && <>
            <span style={{ color: '#9ca3af' }}> from </span>
            <span className="mono" style={{ color: '#C8A0B8' }}>{MN.shortAccount(String(src).split('#').pop(), 8, 6)}</span>
          </>}
          {dest && <>
            <span style={{ color: '#9ca3af' }}> → </span>
            <span className="mono" style={{ color: '#C8A0B8' }}>{MN.shortAccount(String(dest).split('#').pop(), 8, 6)}</span>
          </>}
        </span>
      );
    }

    if (kind === 'Register' || kind === 'Unregister') {
      // payload variant: Account / AssetDefinition / Domain / Trigger / etc.
      const variant = payload.variant || v.variant;
      const target = v.id || v.account || v.asset_definition || v.name || JSON.stringify(v).slice(0, 80);
      return (
        <span>
          <span style={{ color: '#9ca3af' }}>{variant || ''} </span>
          <span className="mono" style={{ color: '#fafafa' }}>{typeof target === 'string' ? MN.shortHash(target, 14, 6) : '...'}</span>
        </span>
      );
    }

    if (kind === 'Grant' || kind === 'Revoke') {
      const perm = v.permission || v.object;
      const dest = v.destination;
      return (
        <span>
          <span style={{ color: '#fafafa', fontWeight: 600 }}>{(perm && perm.name) || (typeof perm === 'string' ? perm : 'permission')}</span>
          {dest && <>
            <span style={{ color: '#9ca3af' }}> → </span>
            <span className="mono" style={{ color: '#C8A0B8' }}>{MN.shortAccount(String(dest), 8, 6)}</span>
          </>}
        </span>
      );
    }

    if (kind === 'SetKeyValue' || kind === 'RemoveKeyValue') {
      const key = v.key;
      const target = v.object || v.target || v.id;
      return (
        <span>
          <span className="mono" style={{ color: '#A78BFA' }}>{key || '—'}</span>
          {target && <span style={{ color: '#6b7280' }}> on {MN.shortHash(String(target), 8, 4)}</span>}
        </span>
      );
    }

    // Fallback: short JSON of the value
    const blob = JSON.stringify(v);
    return <span className="mono" style={{ color: '#9ca3af', fontSize: 11 }}>{blob.length > 90 ? blob.slice(0, 90) + '…' : blob}</span>;
  }

  // -----------------------------------------------------------
  // Kind filter row
  // -----------------------------------------------------------
  function KindFilter({ value, onChange }) {
    const { data } = MN.useFetch('/instructions/kinds', 60_000);
    const items = data && data.items ? data.items : [];
    return (
      <div style={{ display: 'flex', gap: 6, flexWrap: 'wrap' }}>
        <button onClick={() => onChange('')} style={pillStyle(!value)}>All</button>
        {items.map(k => (
          <button key={k.kind} onClick={() => onChange(k.kind)} style={pillStyle(value === k.kind)}>
            <KindBadge kind={k.kind} />
            <span style={{ color: '#6b7280', fontSize: 10, marginLeft: 6 }}>{k.count}</span>
          </button>
        ))}
      </div>
    );
  }
  function pillStyle(active) {
    return {
      display: 'inline-flex', alignItems: 'center', gap: 4,
      padding: '4px 10px', borderRadius: 999, fontSize: 12, fontWeight: 600,
      background: active ? 'rgba(160,98,176,0.20)' : 'transparent',
      color: active ? '#fafafa' : '#9ca3af',
      border: '1px solid ' + (active ? 'rgba(160,98,176,0.40)' : 'rgba(255,255,255,0.10)'),
      cursor: 'pointer',
    };
  }

  function Instructions() {
    const t = MN.i18n.useT();
    const [page, setPage] = React.useState(1);
    const [kind, setKind] = React.useState('');
    const perPage = 50;
    const qs = `?page=${page}&per_page=${perPage}` + (kind ? `&kind=${encodeURIComponent(kind)}` : '');
    const { data, error, loading } = MN.useFetch('/instructions' + qs, 30_000);

    const cols = [
      { key: 'kind', label: 'Kind', render: r => <KindBadge kind={r.kind} /> },
      { key: 'detail', label: 'Detail', render: r => <PayloadLine kind={r.kind} payload={r.payload} /> },
      { key: 'authority', label: t('common.authority'), render: r => (
        <a className="mono" href={MN.walletHref ? MN.walletHref(r.authority) : '#wallet/' + encodeURIComponent(r.authority)}
           style={{ color: '#C8A0B8', textDecoration: 'none', borderBottom: '1px dotted rgba(200,160,184,0.40)' }}
           title={r.authority}>{MN.shortAccount(r.authority, 8, 5)}</a>
      ) },
      { key: 'block', label: '#', render: r => (
        <a href={'#block/' + r.block} className="mono" style={{ color: '#fafafa', textDecoration: 'none', borderBottom: '1px dotted rgba(255,255,255,0.20)' }}>#{MN.fmt.int(r.block)}</a>
      ) },
      { key: 'tx', label: 'Tx', render: r => (
        <a className="mono" href={'#tx/' + r.transaction_hash}
           style={{ color: '#C8A0B8', textDecoration: 'none', borderBottom: '1px dotted rgba(200,160,184,0.40)' }}
           title={r.transaction_hash}>{MN.shortHash(r.transaction_hash, 6, 4)}</a>
      ) },
      { key: 'when', label: t('common.created'), render: r => (
        <span style={{ color: '#9ca3af', fontSize: 12 }}>{MN.fmt.relative(r.created_at)}</span>
      ) },
    ];
    const rows = (data && data.items ? data.items : []).map(x => ({ ...x, __key: x.transaction_hash + ':' + x.instruction_index }));

    return (
      <section className="section">
        <div className="page-header">
          <div>
            <h1 className="page-title">{t('instructions.title')}</h1>
            <div className="page-sub">{t('instructions.subtitle')} {data && `· ${MN.fmt.int(data.total)} total`}</div>
          </div>
        </div>
        <div style={{ marginBottom: 12 }}>
          <KindFilter value={kind} onChange={k => { setKind(k); setPage(1); }} />
        </div>
        {error && <div style={{ color: 'var(--err)', padding: 12 }}>{error.message}</div>}
        <MN.ui.Table columns={cols} rows={rows} empty={loading ? t('common.loading') : t('common.empty')} />
        <MN.ui.Pager page={page} totalPages={data ? data.total_pages : 1} onChange={setPage} />
      </section>
    );
  }

  // Reusable inline list — used by BlockDetail and TxDetail to embed
  // the ISIs emitted in their scope (mirrors the v2 pattern of showing
  // events inside each block / extrinsic).
  function InlineList({ block, txHash, limit = 200, title }) {
    const t = MN.i18n.useT();
    const params = new URLSearchParams();
    if (block != null) params.set('block', String(block));
    if (txHash) params.set('tx', txHash);
    params.set('per_page', String(limit));
    const { data, error, loading } = MN.useFetch('/instructions?' + params.toString(), 30_000);
    const items = data && data.items ? data.items : [];

    return (
      <div style={{ border: '1px solid rgba(255,255,255,0.07)', borderRadius: 16, background: 'rgba(15,15,20,0.6)', overflow: 'hidden' }}>
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'baseline',
                      padding: '12px 16px', background: 'rgba(255,255,255,0.02)',
                      borderBottom: '1px solid rgba(255,255,255,0.06)' }}>
          <h3 style={{ margin: 0, color: '#fafafa', fontSize: 14, fontWeight: 700 }}>{title || t('instructions.title')}</h3>
          <span style={{ color: '#6b7280', fontSize: 11 }}>{items.length}</span>
        </div>
        {error && <div style={{ padding: 14, color: '#EF4444', fontSize: 12 }}>{error.message}</div>}
        {!error && loading && items.length === 0 && (
          <div style={{ padding: 18, textAlign: 'center', color: '#6b7280', fontSize: 12 }}>{t('common.loading')}</div>
        )}
        {!loading && items.length === 0 && !error && (
          <div style={{ padding: 18, textAlign: 'center', color: '#6b7280', fontSize: 12 }}>{t('common.empty')}</div>
        )}
        {items.length > 0 && (
          <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: 13 }}>
            <thead>
              <tr>
                <th style={th()}>Kind</th>
                <th style={th()}>Detail</th>
                <th style={th()}>{t('common.authority')}</th>
                {block == null && <th style={th()}>#</th>}
                {!txHash && <th style={th()}>Tx</th>}
              </tr>
            </thead>
            <tbody>
              {items.map((r, i) => (
                <tr key={r.transaction_hash + ':' + r.instruction_index} style={{ borderBottom: '1px solid rgba(255,255,255,0.04)' }}>
                  <td style={{ padding: '8px 14px' }}><KindBadge kind={r.kind} /></td>
                  <td style={{ padding: '8px 14px' }}><PayloadLine kind={r.kind} payload={r.payload} /></td>
                  <td style={{ padding: '8px 14px' }}>
                    <a className="mono" href={MN.walletHref ? MN.walletHref(r.authority) : '#wallet/' + encodeURIComponent(r.authority)}
                       style={{ color: '#C8A0B8', textDecoration: 'none', borderBottom: '1px dotted rgba(200,160,184,0.40)' }}
                       title={r.authority}>{MN.shortAccount(r.authority, 8, 5)}</a>
                  </td>
                  {block == null && (
                    <td style={{ padding: '8px 14px' }}>
                      <a href={'#block/' + r.block} className="mono" style={{ color: '#fafafa', textDecoration: 'none', borderBottom: '1px dotted rgba(255,255,255,0.20)' }}>#{MN.fmt.int(r.block)}</a>
                    </td>
                  )}
                  {!txHash && (
                    <td style={{ padding: '8px 14px' }}>
                      <a className="mono" href={'#tx/' + r.transaction_hash}
                         style={{ color: '#C8A0B8', textDecoration: 'none', borderBottom: '1px dotted rgba(200,160,184,0.40)' }}
                         title={r.transaction_hash}>{MN.shortHash(r.transaction_hash, 6, 4)}</a>
                    </td>
                  )}
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </div>
    );
  }
  function th() {
    return { textAlign: 'left', padding: '10px 14px', color: '#9ca3af',
             fontSize: 11, fontWeight: 600, letterSpacing: '0.08em', textTransform: 'uppercase',
             borderBottom: '1px solid rgba(255,255,255,0.07)', background: 'rgba(255,255,255,0.02)' };
  }

  MN.Instructions = Instructions;
  MN.ui = MN.ui || {};
  MN.ui.InstructionsList = InlineList;
  MN.ui.KindBadge = KindBadge;
  MN.ui.PayloadLine = PayloadLine;
})(window.MN);
