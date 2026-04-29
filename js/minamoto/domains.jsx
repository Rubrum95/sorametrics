/* global window, React */
// ============================================================
// js/minamoto/domains.jsx
// Domains directory — KPIs hero + dense per-domain cards. Iroha
// 3 has no v2 equivalent (substrate has no namespacing) so the
// shape mirrors v2 TokensSection density (KpiGrid + grid of
// rich cards) rather than HoldersSection (rank table). Each
// card carries owner avatar + 3 mini-stats (accounts/assets/
// nfts) + accent gradient.
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

  function DomainCard({ d }) {
    const t = MN.i18n.useT();
    const isUniversal = d.id === 'universal' || d.id.endsWith('.universal');
    const accent = isUniversal ? '#A062B0' : '#6b7280';
    return (
      <div style={{
        position: 'relative', padding: '18px 20px', borderRadius: 14,
        background: 'rgba(20,20,28,0.72)', border: '1px solid rgba(255,255,255,0.07)',
        backdropFilter: 'blur(14px)',
      }}>
        <div style={{ position: 'absolute', top: 0, left: 0, right: 0, height: 2, background: accent, opacity: 0.6 }} />
        <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: 12 }}>
          <div style={{ display: 'flex', alignItems: 'center', gap: 10, minWidth: 0 }}>
            <span style={{
              width: 32, height: 32, borderRadius: 8,
              background: `linear-gradient(135deg, ${accent}, ${accent}88)`,
              display: 'grid', placeItems: 'center',
              color: 'white', fontWeight: 800, fontSize: 14, flexShrink: 0,
            }}>{d.id.slice(0, 1).toUpperCase()}</span>
            <div style={{ minWidth: 0 }}>
              <div style={{ color: 'var(--fg-0)', fontSize: 14, fontWeight: 700, letterSpacing: '-0.01em' }}>{d.id}</div>
              <a className="num" href={MN.walletHref ? MN.walletHref(d.owned_by) : '#wallet/' + encodeURIComponent(d.owned_by)}
                 title={d.owned_by}
                 style={{ color: 'var(--accent)', fontSize: 11, textDecoration: 'none', borderBottom: '1px dotted rgba(200,160,184,0.30)' }}>
                {MN.shortAccount(d.owned_by, 8, 5)}
              </a>
            </div>
          </div>
        </div>
        <div style={{ display: 'grid', gridTemplateColumns: 'repeat(3, 1fr)', gap: 10, paddingTop: 10, borderTop: '1px solid var(--border)' }}>
          <div>
            <div className="stat-label" style={{ fontSize: 10 }}>{t('domains.accounts')}</div>
            <div className="num" style={{ color: 'var(--fg-0)', fontSize: 16, fontWeight: 700, marginTop: 2 }}>{MN.fmt.int(d.accounts_count)}</div>
          </div>
          <div>
            <div className="stat-label" style={{ fontSize: 10 }}>{t('domains.assets')}</div>
            <div className="num" style={{ color: 'var(--fg-0)', fontSize: 16, fontWeight: 700, marginTop: 2 }}>{MN.fmt.int(d.assets_count)}</div>
          </div>
          <div>
            <div className="stat-label" style={{ fontSize: 10 }}>NFTs</div>
            <div className="num" style={{ color: 'var(--fg-0)', fontSize: 16, fontWeight: 700, marginTop: 2 }}>{MN.fmt.int(d.nfts_count)}</div>
          </div>
        </div>
        <div className="muted tiny" style={{ marginTop: 8 }}>{t('domains.updated')} {MN.fmt.relative(d.updated_at)}</div>
      </div>
    );
  }

  function Domains() {
    const t = MN.i18n.useT();
    const list  = MN.useFetch('/domains', 60_000);
    const stats = MN.useFetch('/domains/stats', 60_000);
    const items = (list.data && list.data.items) || [];
    const s = stats.data || {};

    return (
      <section className="section">
        <div className="page-header">
          <div>
            <h1 className="page-title">{t('domains.title')}</h1>
            <div className="page-sub">{list.data ? `${MN.fmt.int(items.length)} total` : t('common.loading')}</div>
          </div>
        </div>

        <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(180px, 1fr))', gap: 12, marginBottom: 18 }}>
          <Kpi label={t('domains.kpi.total')}    value={MN.fmt.int(s.total)}          accent="#A062B0" />
          <Kpi label={t('domains.kpi.accounts')} value={MN.fmt.int(s.accounts_total)} sub={t('domains.kpi.accountsSub')} />
          <Kpi label={t('domains.kpi.assets')}   value={MN.fmt.int(s.assets_total)}   sub={t('domains.kpi.assetsSub')} />
          <Kpi label={t('domains.kpi.nfts')}     value={MN.fmt.int(s.nfts_total)}     sub={t('domains.kpi.nftsSub')} />
          <Kpi label={t('domains.kpi.largestAcc')}
               value={s.largest_by_accounts ? s.largest_by_accounts.id : '—'}
               sub={s.largest_by_accounts ? MN.fmt.int(s.largest_by_accounts.accounts_count) + ' ' + t('domains.accounts').toLowerCase() : null} />
          <Kpi label={t('domains.kpi.largestAst')}
               value={s.largest_by_assets ? s.largest_by_assets.id : '—'}
               sub={s.largest_by_assets ? MN.fmt.int(s.largest_by_assets.assets_count) + ' ' + t('domains.assets').toLowerCase() : null} />
        </div>

        {list.error && <div style={{ color: 'var(--err)', padding: 12 }}>{list.error.message}</div>}
        <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(280px, 1fr))', gap: 12 }}>
          {items.map(d => <DomainCard key={d.id} d={d} />)}
        </div>
        {list.loading && items.length === 0 && (
          <div style={{ padding: 32, textAlign: 'center', color: 'var(--fg-3)' }}>{t('common.loading')}</div>
        )}
      </section>
    );
  }

  MN.Domains = Domains;
})(window.MN);
