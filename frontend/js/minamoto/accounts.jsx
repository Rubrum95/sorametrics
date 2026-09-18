/* global window, React */
// ============================================================
// js/minamoto/accounts.jsx
// Accounts directory — KPIs hero strip + filter bar + enriched
// table. Mirrors the v2 HoldersSection density (KpiGrid + token
// chips + rank-styled rows + identity column) adapted to the
// I105 account world: instead of per-token rank we show alias /
// multisig / activity recency, since Minamoto accounts span many
// tokens at once and there is no single "balance" column.
// ============================================================

window.MN = window.MN || {};

(function (MN) {
  // -----------------------------------------------------------
  // KPI hero — same .stat-card shape as v2 PageHeader's KpiGrid
  // -----------------------------------------------------------
  function Kpi({ label, value, sub, accent }) {
    return (
      <div className="stat-card" style={accent ? { borderTop: '2px solid ' + accent } : null}>
        <span className="stat-label">{label}</span>
        <span className="stat-value num">{value}</span>
        {sub && <span className="stat-sub">{sub}</span>}
      </div>
    );
  }

  // -----------------------------------------------------------
  // Filter chip — mirrors v2 .filter-chip behaviour (radio-style
  // active state, monochrome accent border).
  // -----------------------------------------------------------
  function FilterChip({ active, onClick, children }) {
    return (
      <button onClick={onClick}
        style={{
          padding: '6px 12px', borderRadius: 999, fontSize: 12, fontWeight: 600,
          background: active ? 'rgba(160,98,176,0.20)' : 'rgba(255,255,255,0.03)',
          color:      active ? 'var(--fg-0)' : 'var(--fg-2)',
          border: '1px solid ' + (active ? 'rgba(160,98,176,0.40)' : 'var(--border)'),
          cursor: 'pointer',
        }}>{children}</button>
    );
  }

  function Avatar({ id, isMultisig }) {
    // Deterministic gradient from the last 2 hex chars of the id, so each
    // wallet keeps the same colour across pages without us storing colours.
    const seed = (id || 'x').slice(-2);
    const hue = (parseInt(seed, 36) || 0) % 360;
    const grad = isMultisig
      ? 'linear-gradient(135deg,#7B5B90,#A062B0)'
      : `linear-gradient(135deg, hsl(${hue},55%,40%), hsl(${(hue+40)%360},55%,30%))`;
    return (
      <div style={{
        width: 28, height: 28, borderRadius: '50%',
        background: grad, flexShrink: 0,
        boxShadow: '0 4px 12px -6px rgba(0,0,0,0.4)',
      }} />
    );
  }

  function Accounts() {
    const t = MN.i18n.useT();
    const [page, setPage] = React.useState(1);
    const [filter, setFilter] = React.useState('all');
    const [search, setSearch] = React.useState('');
    const perPage = 50;

    const stats = MN.useFetch('/accounts/stats', 60_000);
    const list  = MN.useFetch(`/accounts?page=${page}&per_page=${perPage}`, 60_000);
    const s = stats.data || {};
    const items = (list.data && list.data.items) || [];

    // Client-side filter (search + multisig). Server-side filtering would
    // require new query params; for the volumes Minamoto has today the
    // client filter is plenty fast.
    const filtered = items.filter(a => {
      if (filter === 'multisig' && !a.multisig_quorum) return false;
      if (filter === 'aliased'  && !a.has_primary_alias) return false;
      if (filter === 'active'   && !(a.last_seen_at && (Date.now() - Date.parse(a.last_seen_at)) < 24*3600_000)) return false;
      if (search) {
        const q = search.toLowerCase();
        const hay = (a.id + ' ' + (a.primary_alias_name || '') + ' ' + (a.primary_alias_domain || '')).toLowerCase();
        if (!hay.includes(q)) return false;
      }
      return true;
    });

    const totalPages = list.data ? list.data.total_pages : 1;

    return (
      <section className="section">
        <div className="page-header">
          <div>
            <h1 className="page-title">{t('accounts.title')}</h1>
            <div className="page-sub">
              {list.data ? `${MN.fmt.int(list.data.total)} total · ${t('common.page')} ${page} / ${totalPages}` : t('common.loading')}
            </div>
          </div>
        </div>

        {/* KPI hero — 5 stats */}
        <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(180px, 1fr))', gap: 12, marginBottom: 18 }}>
          <Kpi label={t('accounts.kpi.total')}     value={MN.fmt.int(s.total)}     sub={t('accounts.kpi.totalSub')} accent="#A062B0" />
          <Kpi label={t('accounts.kpi.multisig')}  value={MN.fmt.int(s.multisig)}  sub={s.total ? ((s.multisig / s.total) * 100).toFixed(1) + '% ' + t('accounts.kpi.ofAll') : null} />
          <Kpi label={t('accounts.kpi.aliased')}   value={MN.fmt.int(s.aliased)}   sub={s.total ? ((s.aliased  / s.total) * 100).toFixed(1) + '% ' + t('accounts.kpi.ofAll') : null} />
          <Kpi label={t('accounts.kpi.active24h')} value={MN.fmt.int(s.active_24h)} sub={t('accounts.kpi.active24hSub')} />
          <Kpi label={t('accounts.kpi.new7d')}     value={MN.fmt.int(s.new_7d)}    sub={s.last_activity ? t('accounts.kpi.lastSub') + ' ' + MN.fmt.relative(s.last_activity) : null} />
        </div>

        {/* Filter bar — search + segmented toggle */}
        <div className="card" style={{ padding: '12px 14px', marginBottom: 14, display: 'flex', gap: 10, alignItems: 'center', flexWrap: 'wrap' }}>
          <input value={search} onChange={e => setSearch(e.target.value)}
            placeholder={t('accounts.searchPlaceholder')}
            style={{
              flex: '1 1 240px', padding: '8px 12px', borderRadius: 8, fontSize: 13,
              background: 'rgba(255,255,255,0.04)', color: 'var(--fg-0)',
              border: '1px solid var(--border)', outline: 'none',
              fontFamily: 'JetBrains Mono, monospace',
            }} />
          <div style={{ display: 'flex', gap: 6, flexWrap: 'wrap' }}>
            <FilterChip active={filter === 'all'}      onClick={() => setFilter('all')}>{t('accounts.filter.all')}</FilterChip>
            <FilterChip active={filter === 'multisig'} onClick={() => setFilter('multisig')}>{t('accounts.filter.multisig')}</FilterChip>
            <FilterChip active={filter === 'aliased'}  onClick={() => setFilter('aliased')}>{t('accounts.filter.aliased')}</FilterChip>
            <FilterChip active={filter === 'active'}   onClick={() => setFilter('active')}>{t('accounts.filter.active24h')}</FilterChip>
          </div>
          <span className="muted tiny" style={{ marginLeft: 'auto' }}>
            {filtered.length} {t('common.shown')} / {items.length} {t('common.onPage')}
          </span>
        </div>

        {list.error && <div style={{ color: 'var(--err)', padding: 12 }}>{list.error.message}</div>}

        {/* Enriched accounts table */}
        <div className="card" style={{ padding: 0, overflow: 'hidden' }}>
          <div className="swaps-table-wrap responsive-table">
            <table className="swaps-table">
              <thead>
                <tr>
                  <th style={{ width: 40 }}>#</th>
                  <th>{t('accounts.col.account')}</th>
                  <th>{t('accounts.col.alias')}</th>
                  <th>{t('accounts.col.multisig')}</th>
                  <th>{t('accounts.col.firstSeen')}</th>
                  <th>{t('accounts.col.lastSeen')}</th>
                </tr>
              </thead>
              <tbody>
                {filtered.length === 0 && (
                  <tr><td colSpan={6} style={{ padding: 32, textAlign: 'center', color: 'var(--fg-3)' }}>
                    {list.loading ? t('common.loading') : t('common.empty')}
                  </td></tr>
                )}
                {filtered.map((a, i) => {
                  const rank = (page - 1) * perPage + i + 1;
                  const isMultisig = !!a.multisig_quorum;
                  const isActive = a.last_seen_at && (Date.now() - Date.parse(a.last_seen_at)) < 24*3600_000;
                  return (
                    <tr key={a.id} style={{ cursor: 'pointer' }}
                        onClick={() => { window.location.hash = 'wallet/' + encodeURIComponent(a.id); }}>
                      <td data-label="#">
                        <span className="num muted" style={{ fontSize: 12 }}>{rank}</span>
                      </td>
                      <td data-label={t('accounts.col.account')}>
                        <div style={{ display: 'flex', alignItems: 'center', gap: 10, minWidth: 0 }}>
                          <Avatar id={a.id} isMultisig={isMultisig} />
                          <div style={{ minWidth: 0 }}>
                            <div className="num" style={{ color: 'var(--accent)', fontSize: 12, fontWeight: 600 }} title={a.id}>
                              {MN.shortAccount(a.id, 10, 6)}
                            </div>
                            {isActive && (
                              <span style={{
                                display: 'inline-block', marginTop: 2, padding: '1px 6px', borderRadius: 999,
                                fontSize: 9, fontWeight: 700, letterSpacing: '0.06em',
                                color: '#10B981', background: 'rgba(16,185,129,0.12)',
                                border: '1px solid rgba(16,185,129,0.30)',
                              }}>● {t('accounts.activeNow')}</span>
                            )}
                          </div>
                        </div>
                      </td>
                      <td data-label={t('accounts.col.alias')}>
                        {a.has_primary_alias ? (
                          <span style={{ color: 'var(--fg-0)' }}>
                            {a.primary_alias_name || a.primary_alias}
                            {a.primary_alias_domain && <span className="muted">@{a.primary_alias_domain}</span>}
                          </span>
                        ) : <span className="muted">—</span>}
                      </td>
                      <td data-label={t('accounts.col.multisig')}>
                        {isMultisig ? (
                          <span style={{
                            display: 'inline-flex', alignItems: 'center', gap: 4,
                            padding: '2px 8px', borderRadius: 999,
                            fontSize: 11, fontWeight: 700,
                            color: '#A062B0', background: 'rgba(160,98,176,0.10)',
                            border: '1px solid rgba(160,98,176,0.30)',
                          }}>
                            {a.multisig_quorum}/{a.multisig_signatories_count}
                          </span>
                        ) : <span className="muted">—</span>}
                      </td>
                      <td data-label={t('accounts.col.firstSeen')}>
                        <span className="muted tiny">{a.first_seen_at ? MN.fmt.relative(a.first_seen_at) : '—'}</span>
                      </td>
                      <td data-label={t('accounts.col.lastSeen')}>
                        <span style={{ color: isActive ? 'var(--fg-0)' : 'var(--fg-2)' }}>
                          {a.last_seen_at ? MN.fmt.relative(a.last_seen_at) : '—'}
                        </span>
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        </div>
        <MN.ui.Pager page={page} totalPages={totalPages} onChange={setPage} />
      </section>
    );
  }

  MN.Accounts = Accounts;
})(window.MN);
