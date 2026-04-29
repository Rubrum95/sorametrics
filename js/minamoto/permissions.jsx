/* global window, React */
// ============================================================
// js/minamoto/permissions.jsx
// Permissions / Grants browser. Iroha 3 expresses access control
// through Grant ISIs (variants PermissionToAccount, RoleToAccount).
// On Minamoto these are by far the most-active ISI kind (142+
// grants vs 50 Registers, 19 Mints, 8 Transfers) yet were buried
// inside the generic Instructions list. This section surfaces them
// as a first-class browser with KPI hero + filter chips per
// permission name + paginated table.
// ============================================================

window.MN = window.MN || {};

(function (MN) {
  function Kpi({ label, value, sub, accent }) {
    return (
      <div className="stat-card" style={accent ? { borderTop: '2px solid ' + accent } : null}>
        <span className="stat-label">{label}</span>
        <span className="stat-value num">{value}</span>
        {sub && <span className="stat-sub">{sub}</span>}
      </div>
    );
  }

  // Map common permission names to a stable colour. Falls back to neutral
  // for unknown perms so new permission types still render cleanly when
  // future Iroha 3 releases add them.
  const PERM_COLORS = {
    CanManageAccountAlias:           '#A062B0',
    CanResolveAccountAlias:          '#C8A0B8',
    CanRegisterTrigger:              '#F472B6',
    CanRegisterAccount:              '#60A5FA',
    CanRegisterDomain:               '#3B82F6',
    CanMintAssetWithDefinition:      '#10B981',
    CanUseFeeSponsor:                '#F59E0B',
    CanManageVerifyingKeys:          '#8B5CF6',
    CanEnactGovernance:              '#FB7185',
    CanSetParameters:                '#EF4444',
    CanPublishSpaceDirectoryManifest:'#22C55E',
    CanManageSoracloud:              '#06B6D4',
  };
  function permColor(name) { return PERM_COLORS[name] || '#9ca3af'; }

  function PermBadge({ name }) {
    const c = permColor(name);
    return (
      <span style={{
        display: 'inline-block', padding: '2px 8px', borderRadius: 999,
        fontSize: 11, fontWeight: 700, letterSpacing: '0.02em',
        color: c, background: c + '14', border: '1px solid ' + c + '40',
      }}>{name}</span>
    );
  }

  function PermFilterChips({ stats, value, onChange }) {
    const items = (stats && stats.top_permissions) || [];
    return (
      <div style={{ display: 'flex', flexWrap: 'wrap', gap: 6 }}>
        <button onClick={() => onChange('')} style={chipStyle(!value)}>All</button>
        {items.map(p => (
          <button key={p.name} onClick={() => onChange(p.name)} style={chipStyle(value === p.name)}>
            <PermBadge name={p.name} />
            <span style={{ color: 'var(--fg-3)', fontSize: 10, marginLeft: 6 }}>{p.count}</span>
          </button>
        ))}
      </div>
    );
  }

  function chipStyle(active) {
    return {
      display: 'inline-flex', alignItems: 'center', gap: 4,
      padding: '4px 10px', borderRadius: 999, fontSize: 12, fontWeight: 600,
      background: active ? 'rgba(160,98,176,0.20)' : 'transparent',
      color:      active ? 'var(--fg-0)' : 'var(--fg-2)',
      border: '1px solid ' + (active ? 'rgba(160,98,176,0.40)' : 'var(--border)'),
      cursor: 'pointer',
    };
  }

  // Render permission_args as a compact, scannable summary. Different
  // permissions carry different arg shapes:
  //   CanUseFeeSponsor             → { sponsor }
  //   CanMintAssetWithDefinition   → { asset_definition }
  //   CanManageAccountAlias        → { dataspace, account_alias_pattern... }
  // For unknown shapes we degrade to a short JSON snippet.
  function PermArgs({ args }) {
    if (!args || typeof args !== 'object') return <span className="muted">—</span>;
    const sponsor = args.sponsor;
    const def     = args.asset_definition;
    const acc     = args.account || args.account_id;
    const dom     = args.domain  || args.domain_id;
    if (sponsor) {
      return (
        <a className="num" href={MN.walletHref(sponsor)} title={sponsor}
           style={{ color: 'var(--accent)', fontSize: 11, textDecoration: 'none', borderBottom: '1px dotted rgba(255,255,255,0.20)' }}>
          sponsor: {MN.shortAccount(sponsor, 6, 4)}
        </a>
      );
    }
    if (def) {
      return (
        <a className="num" href={'#asset/' + encodeURIComponent(def)} title={def}
           style={{ color: 'var(--accent)', fontSize: 11, textDecoration: 'none', borderBottom: '1px dotted rgba(255,255,255,0.20)' }}>
          asset: {MN.shortHash(def, 6, 4)}
        </a>
      );
    }
    if (acc) return <span className="num muted" style={{ fontSize: 11 }}>{MN.shortAccount(String(acc), 8, 5)}</span>;
    if (dom) return <span className="num muted" style={{ fontSize: 11 }}>{String(dom)}</span>;
    const blob = JSON.stringify(args);
    return <span className="num muted" style={{ fontSize: 11 }}>{blob.length > 50 ? blob.slice(0, 50) + '…' : blob}</span>;
  }

  function Permissions() {
    const t = MN.i18n.useT();
    const [page, setPage] = React.useState(1);
    const [permFilter, setPermFilter] = React.useState('');
    const [searchAuthor, setSearchAuthor] = React.useState('');
    const [searchDest, setSearchDest] = React.useState('');
    const perPage = 50;

    const stats = MN.useFetch('/permissions/stats', 60_000);
    const qs = `?page=${page}&per_page=${perPage}` +
               (permFilter ? `&perm_name=${encodeURIComponent(permFilter)}` : '') +
               (searchAuthor ? `&authority=${encodeURIComponent(searchAuthor)}` : '') +
               (searchDest ? `&destination=${encodeURIComponent(searchDest)}` : '');
    const list = MN.useFetch('/permissions/grants' + qs, 30_000);

    const s = stats.data || {};
    const items = (list.data && list.data.items) || [];

    return (
      <section className="section">
        <div className="page-header">
          <div>
            <h1 className="page-title">{t('permissions.title')}</h1>
            <div className="page-sub">{t('permissions.subtitle')}</div>
          </div>
        </div>

        {/* KPI hero — 5 stats */}
        <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(180px, 1fr))', gap: 12, marginBottom: 14 }}>
          <Kpi label={t('permissions.kpi.totalGrants')}        value={MN.fmt.int(s.total)}             accent="#A062B0" />
          <Kpi label={t('permissions.kpi.distinctPerms')}      value={MN.fmt.int(s.distinct_perms)}    sub={t('permissions.kpi.distinctPermsSub')} />
          <Kpi label={t('permissions.kpi.grantors')}           value={MN.fmt.int(s.distinct_grantors)} sub={t('permissions.kpi.grantorsSub')} />
          <Kpi label={t('permissions.kpi.recipients')}         value={MN.fmt.int(s.distinct_recipients)} sub={t('permissions.kpi.recipientsSub')} />
          <Kpi label={t('permissions.kpi.roles')}              value={MN.fmt.int(s.role_to_account)}    sub={t('permissions.kpi.rolesSub')} />
        </div>

        {/* Filter bar: permission chips + search inputs */}
        <div className="card" style={{ padding: '14px 16px', marginBottom: 14 }}>
          <div style={{ marginBottom: 10 }}>
            <PermFilterChips stats={s} value={permFilter} onChange={p => { setPermFilter(p); setPage(1); }} />
          </div>
          <div style={{ display: 'flex', gap: 10, flexWrap: 'wrap' }}>
            <input value={searchAuthor} onChange={e => { setSearchAuthor(e.target.value); setPage(1); }}
              placeholder={t('permissions.search.authority')}
              style={{ flex: '1 1 240px', padding: '8px 12px', borderRadius: 8, fontSize: 13,
                       background: 'rgba(255,255,255,0.04)', color: 'var(--fg-0)',
                       border: '1px solid var(--border)', outline: 'none',
                       fontFamily: 'JetBrains Mono, monospace' }} />
            <input value={searchDest} onChange={e => { setSearchDest(e.target.value); setPage(1); }}
              placeholder={t('permissions.search.destination')}
              style={{ flex: '1 1 240px', padding: '8px 12px', borderRadius: 8, fontSize: 13,
                       background: 'rgba(255,255,255,0.04)', color: 'var(--fg-0)',
                       border: '1px solid var(--border)', outline: 'none',
                       fontFamily: 'JetBrains Mono, monospace' }} />
            <span className="muted tiny" style={{ alignSelf: 'center', marginLeft: 'auto' }}>
              {list.data ? `${MN.fmt.int(list.data.total)} ${t('permissions.matching')}` : ''}
            </span>
          </div>
        </div>

        {list.error && <div style={{ color: 'var(--err)', padding: 12 }}>{list.error.message}</div>}

        {/* Grants table */}
        <div className="card" style={{ padding: 0, overflow: 'hidden' }}>
          <div className="swaps-table-wrap responsive-table">
            <table className="swaps-table">
              <thead>
                <tr>
                  <th style={{ width: 130 }}>{t('common.created')}</th>
                  <th>{t('permissions.col.permission')}</th>
                  <th>{t('permissions.col.args')}</th>
                  <th>{t('permissions.col.grantor')}</th>
                  <th>{t('permissions.col.grantee')}</th>
                  <th style={{ width: 80 }}>{t('common.height')}</th>
                  <th style={{ width: 90 }}>Tx</th>
                </tr>
              </thead>
              <tbody>
                {items.length === 0 && (
                  <tr><td colSpan={7} style={{ padding: 32, textAlign: 'center', color: 'var(--fg-3)' }}>
                    {list.loading ? t('common.loading') : t('common.empty')}
                  </td></tr>
                )}
                {items.map((g, i) => (
                  <tr key={g.transaction_hash + ':' + g.instruction_index}>
                    <td data-label={t('common.created')}>
                      <span className="muted tiny">{MN.fmt.relative(g.created_at)}</span>
                    </td>
                    <td data-label={t('permissions.col.permission')}>
                      <PermBadge name={g.permission_name} />
                    </td>
                    <td data-label={t('permissions.col.args')}>
                      <PermArgs args={g.permission_args} />
                    </td>
                    <td data-label={t('permissions.col.grantor')}>
                      <a className="num" href={MN.walletHref(g.authority)} title={g.authority}
                         style={{ color: 'var(--fg-0)', fontSize: 11, textDecoration: 'none', borderBottom: '1px dotted rgba(255,255,255,0.20)' }}>
                        {MN.shortAccount(g.authority, 8, 5)}
                      </a>
                    </td>
                    <td data-label={t('permissions.col.grantee')}>
                      <a className="num" href={MN.walletHref(g.destination)} title={g.destination}
                         style={{ color: 'var(--accent)', fontSize: 11, textDecoration: 'none', borderBottom: '1px dotted rgba(200,160,184,0.40)' }}>
                        {MN.shortAccount(g.destination, 8, 5)}
                      </a>
                    </td>
                    <td data-label={t('common.height')}>
                      <a className="num" href={'#block/' + g.block} style={{ color: 'var(--fg-1)', fontSize: 11, textDecoration: 'none' }}>#{MN.fmt.int(g.block)}</a>
                    </td>
                    <td data-label="Tx">
                      <a className="num" href={'#tx/' + g.transaction_hash} title={g.transaction_hash}
                         style={{ color: 'var(--accent)', fontSize: 11, textDecoration: 'none' }}>{MN.shortHash(g.transaction_hash, 6, 4)}</a>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
        <MN.ui.Pager page={page} totalPages={list.data ? list.data.total_pages : 1} onChange={setPage} />
      </section>
    );
  }

  MN.Permissions = Permissions;
})(window.MN);
