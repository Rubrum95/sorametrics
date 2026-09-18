/* global window, React */
// ============================================================
// js/minamoto/assets.jsx
// Asset definitions catalogue. Highlights XOR (the asset that
// migrates in from SORA v2 via burn-and-claim per Minamoto launch).
// ============================================================

window.MN = window.MN || {};

(function (MN) {
  // Asset quantities arrive as raw on-chain integers. For well-known
  // bridged assets (XOR from SORA v2 → 18 decimals) we apply the
  // canonical scaling; for unknown assets we group thousands but keep
  // the raw value (Iroha 3 does not enforce a global decimal count).
  function fmtAmount(v, asset) {
    if (v == null || v === '') return '—';
    const decimals = asset ? MN.assetDecimals(asset) : null;
    if (decimals != null) return MN.fmt.tokenAmount(v, decimals);
    const s = String(v);
    if (s.includes('.')) {
      return s.replace(/(\.\d*?)0+$/, '$1').replace(/\.$/, '');
    }
    const n = Number(s);
    if (Number.isFinite(n) && n < 1e15) return n.toLocaleString('en-US');
    return s;
  }

  function ConfidentialBadge({ mode }) {
    if (!mode) return null;
    const isZk = mode === 'Convertible';
    return (
      <span style={{
        fontSize: 10.5, fontWeight: 700, letterSpacing: '0.10em', textTransform: 'uppercase',
        padding: '3px 8px', borderRadius: 999,
        color: isZk ? '#A062B0' : '#9ca3af',
        border: '1px solid ' + (isZk ? 'rgba(160,98,176,0.40)' : 'rgba(255,255,255,0.10)'),
        background: isZk ? 'rgba(160,98,176,0.10)' : 'rgba(255,255,255,0.02)',
      }}>
        {isZk ? 'ZK Convertible' : mode}
      </span>
    );
  }

  function MintableBadge({ mintable }) {
    if (!mintable) return null;
    const inf = mintable === 'Infinitely';
    return (
      <span style={{
        fontSize: 10.5, fontWeight: 700, letterSpacing: '0.10em', textTransform: 'uppercase',
        padding: '3px 8px', borderRadius: 999,
        color: inf ? '#F59E0B' : '#10B981',
        border: '1px solid ' + (inf ? 'rgba(245,158,11,0.30)' : 'rgba(16,185,129,0.30)'),
        background: 'rgba(255,255,255,0.02)',
      }}>{mintable}</span>
    );
  }

  function AssetCard({ d, accent }) {
    const t = MN.i18n.useT();
    const isXor = d.alias === 'xor#universal' || (d.name || '').toLowerCase() === 'xor';
    const titleColor = isXor ? '#fafafa' : '#e5e7eb';
    const href = '#asset/' + encodeURIComponent(d.alias || d.name || d.id);
    return (
      <a href={href} style={{
        display: 'block', textDecoration: 'none', color: 'inherit',
        position: 'relative', padding: '22px 24px', borderRadius: 16,
        background: 'rgba(20,20,28,0.72)', border: '1px solid ' + (accent || 'rgba(255,255,255,0.07)'),
        backdropFilter: 'blur(14px)', boxShadow: '0 1px 0 rgba(255,255,255,0.04) inset',
        overflow: 'hidden', cursor: 'pointer',
        transition: 'transform 200ms ease, border-color 200ms ease',
      }}
        onMouseEnter={e => { e.currentTarget.style.borderColor = '#A062B0'; }}
        onMouseLeave={e => { e.currentTarget.style.borderColor = accent || 'rgba(255,255,255,0.07)'; }}>
        {accent && <div style={{ position: 'absolute', top: 0, left: 0, right: 0, height: 3, background: accent }} />}
        <div style={{ display: 'flex', alignItems: 'flex-start', justifyContent: 'space-between', gap: 12, marginBottom: 12 }}>
          <div style={{ display: 'flex', alignItems: 'center', gap: 12, minWidth: 0 }}>
            <MN.TokenIcon asset={d} size={36} />
            <div style={{ minWidth: 0 }}>
              <div style={{ fontSize: 22, fontWeight: 800, letterSpacing: '-0.02em', color: titleColor }}>
                {d.name || d.alias || MN.shortHash(d.id, 8, 6)}
              </div>
              {d.alias && <div className="mono" style={{ fontSize: 12, color: '#9ca3af', marginTop: 2 }}>{d.alias}</div>}
            </div>
          </div>
          <div style={{ display: 'flex', flexDirection: 'column', gap: 4, alignItems: 'flex-end' }}>
            <ConfidentialBadge mode={d.confidential_mode} />
            <MintableBadge mintable={d.mintable} />
          </div>
        </div>
        <div style={{ display: 'grid', gridTemplateColumns: 'repeat(3, 1fr)', gap: 14, marginTop: 14, paddingTop: 14, borderTop: '1px solid rgba(255,255,255,0.06)' }}>
          <div>
            <div style={{ color: '#9ca3af', fontSize: 11, fontWeight: 600, letterSpacing: '0.10em', textTransform: 'uppercase' }}>Total supply</div>
            <div className="mono" style={{ color: '#fafafa', fontSize: 20, fontWeight: 700, marginTop: 4 }}
                 title={d.total_quantity + ' raw'}>{fmtAmount(d.total_quantity, d)}</div>
          </div>
          <div>
            <div style={{ color: '#9ca3af', fontSize: 11, fontWeight: 600, letterSpacing: '0.10em', textTransform: 'uppercase' }}>Held / Holders</div>
            <div className="mono" style={{ color: '#e5e7eb', fontSize: 16, fontWeight: 600, marginTop: 4 }}>
              {fmtAmount(d.held_supply, d)} <span style={{ color: '#6b7280', fontSize: 12 }}>/ {d.holders}</span>
            </div>
          </div>
          <div>
            <div style={{ color: '#9ca3af', fontSize: 11, fontWeight: 600, letterSpacing: '0.10em', textTransform: 'uppercase' }}>{t('assets.createdBy')}</div>
            <span className="mono" title={d.owned_by}
                  style={{ display: 'inline-block', color: '#C8A0B8', fontSize: 12, marginTop: 4 }}>
              {MN.shortAccount(d.owned_by, 8, 6)}
            </span>
          </div>
        </div>
        <div className="mono" style={{ marginTop: 12, color: '#6b7280', fontSize: 11 }} title={d.id}>
          id: {MN.shortHash(d.id, 10, 6)}
        </div>
      </a>
    );
  }

  // -----------------------------------------------------------
  // Asset list (root view)
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

  function AssetList() {
    const t = MN.i18n.useT();
    const { data, error, loading } = MN.useFetch('/asset-definitions', 60_000);
    const stats = MN.useFetch('/asset-definitions/stats', 60_000);
    const items = data && data.items ? data.items : [];
    const s = stats.data || {};
    const [filter, setFilter] = React.useState('all');
    const [search, setSearch] = React.useState('');

    const filtered = items.filter(d => {
      if (filter === 'zk'        && d.confidential_mode !== 'Convertible') return false;
      if (filter === 'mintable'  && d.mintable !== 'Infinitely') return false;
      if (filter === 'fixed'     && d.mintable !== 'Once') return false;
      if (search) {
        const q = search.toLowerCase();
        const hay = (d.name + ' ' + (d.alias || '') + ' ' + d.id).toLowerCase();
        if (!hay.includes(q)) return false;
      }
      return true;
    });

    // Aggregate total holders across all assets (sum of d.holders) — useful
    // headline for the network's "ownership breadth" before we have a
    // dedicated unique-holders metric.
    const totalHolders = items.reduce((acc, d) => acc + (d.holders || 0), 0);

    return (
      <section className="section">
        <div className="page-header">
          <div>
            <h1 className="page-title">Assets</h1>
            <div className="page-sub">{data ? `${items.length} ${t('assets.subtitle')}` : t('common.loading')}</div>
          </div>
        </div>

        <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(180px, 1fr))', gap: 12, marginBottom: 14 }}>
          <Kpi label={t('assets.kpi.total')}    value={MN.fmt.int(s.total)}          accent="#A062B0" />
          <Kpi label={t('assets.kpi.zk')}       value={MN.fmt.int(s.zk_convertible)} sub={s.total ? ((s.zk_convertible / s.total) * 100).toFixed(1) + '%' : null} />
          <Kpi label={t('assets.kpi.mintInf')}  value={MN.fmt.int(s.mintable_inf)}   sub={t('assets.kpi.mintInfSub')} />
          <Kpi label={t('assets.kpi.mintOnce')} value={MN.fmt.int(s.mintable_once)}  sub={t('assets.kpi.mintOnceSub')} />
          <Kpi label={t('assets.kpi.holders')}  value={MN.fmt.int(totalHolders)}     sub={t('assets.kpi.holdersSub')} />
        </div>

        <div className="card" style={{ padding: '12px 14px', marginBottom: 14, display: 'flex', gap: 10, alignItems: 'center', flexWrap: 'wrap' }}>
          <input value={search} onChange={e => setSearch(e.target.value)}
            placeholder={t('assets.searchPlaceholder')}
            style={{
              flex: '1 1 200px', padding: '8px 12px', borderRadius: 8, fontSize: 13,
              background: 'rgba(255,255,255,0.04)', color: 'var(--fg-0)',
              border: '1px solid var(--border)', outline: 'none',
            }} />
          <div style={{ display: 'flex', gap: 6, flexWrap: 'wrap' }}>
            <FilterChip active={filter === 'all'}      onClick={() => setFilter('all')}>{t('common.all')}</FilterChip>
            <FilterChip active={filter === 'zk'}       onClick={() => setFilter('zk')}>ZK</FilterChip>
            <FilterChip active={filter === 'mintable'} onClick={() => setFilter('mintable')}>{t('assets.filter.mintable')}</FilterChip>
            <FilterChip active={filter === 'fixed'}    onClick={() => setFilter('fixed')}>{t('assets.filter.fixed')}</FilterChip>
          </div>
          <span className="muted tiny" style={{ marginLeft: 'auto' }}>{filtered.length} / {items.length}</span>
        </div>

        {error && <div style={{ color: 'var(--err)', padding: 12 }}>{error.message}</div>}
        <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(340px, 1fr))', gap: 14 }}>
          {filtered.map(d => {
            const isXor = d.alias === 'xor#universal' || (d.name || '').toLowerCase() === 'xor';
            return <AssetCard key={d.id} d={d} accent={isXor ? '#A062B0' : null} />;
          })}
        </div>
        {loading && items.length === 0 && (
          <div style={{ padding: 32, textAlign: 'center', color: '#9ca3af' }}>{t('common.loading')}</div>
        )}
      </section>
    );
  }

  // -----------------------------------------------------------
  // Asset detail (holders)
  // -----------------------------------------------------------
  function AssetDetail({ idOrAlias }) {
    const t = MN.i18n.useT();
    const enc = encodeURIComponent(idOrAlias);
    const { data, error, loading } = MN.useFetch(`/asset/${enc}/holders`, 30_000);
    const def = data && data.definition;
    const holders = data && data.holders ? data.holders : [];

    if (error) return <div style={{ color: '#EF4444', padding: 16 }}>{error.message}</div>;
    if (!def && loading) return <div style={{ color: '#9ca3af', padding: 32, textAlign: 'center' }}>{t('common.loading')}</div>;
    if (!def) return <div style={{ color: '#9ca3af', padding: 32, textAlign: 'center' }}>{t('common.empty')}</div>;

    const isXor = def.alias === 'xor#universal' || (def.name || '').toLowerCase() === 'xor';

    return (
      <section>
        <div style={{ display: 'flex', alignItems: 'center', gap: 12, marginBottom: 14 }}>
          <a href="#assets" style={{ color: '#9ca3af', fontSize: 13, textDecoration: 'none' }}>← Assets</a>
        </div>
        <header style={{
          padding: '20px 22px', borderRadius: 16, marginBottom: 18,
          background: isXor ? 'linear-gradient(180deg, rgba(160,98,176,0.10), rgba(20,20,28,0.72) 60%)' : 'rgba(20,20,28,0.72)',
          border: '1px solid ' + (isXor ? 'rgba(160,98,176,0.30)' : 'rgba(255,255,255,0.07)'),
        }}>
          <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', flexWrap: 'wrap', gap: 16 }}>
            <div style={{ display: 'flex', alignItems: 'center', gap: 14, minWidth: 0 }}>
              <MN.TokenIcon asset={def} size={56} />
              <div style={{ minWidth: 0 }}>
              <div style={{ display: 'flex', alignItems: 'baseline', gap: 10, flexWrap: 'wrap' }}>
                <h2 style={{ margin: 0, color: '#fafafa', fontSize: 28, fontWeight: 800, letterSpacing: '-0.02em' }}>{def.name || MN.shortHash(def.id, 8, 4)}</h2>
                {def.alias && <span className="mono" style={{ color: '#9ca3af', fontSize: 13 }}>{def.alias}</span>}
                {def.confidential_mode === 'Convertible' && (
                  <span style={{ fontSize: 10, fontWeight: 700, letterSpacing: '0.12em', textTransform: 'uppercase',
                                 padding: '3px 8px', borderRadius: 999, color: '#A062B0',
                                 border: '1px solid rgba(160,98,176,0.40)' }}
                        title="Convertible: this asset can be moved between transparent and ZK-shielded states using Halo2 proofs">ZK Convertible</span>
                )}
                {def.mintable && (
                  <span style={{ fontSize: 10, fontWeight: 700, letterSpacing: '0.12em', textTransform: 'uppercase',
                                 padding: '3px 8px', borderRadius: 999, color: '#F59E0B',
                                 border: '1px solid rgba(245,158,11,0.30)' }}>{def.mintable}</span>
                )}
              </div>
              <div className="mono" style={{ color: '#6b7280', fontSize: 11, marginTop: 6 }} title={def.id}>id: {def.id}</div>
              </div>
            </div>
          </div>
          <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(150px, 1fr))', gap: 14, marginTop: 16, paddingTop: 14, borderTop: '1px solid rgba(255,255,255,0.06)' }}>
            <Stat k={t('asset.totalSupply')} v={def.total_quantity} />
            <Stat k={t('asset.holders')}     v={holders.length} />
            <Stat k={t('assets.createdBy')}  v={MN.shortAccount(def.owned_by, 8, 6)} link={MN.walletHref ? MN.walletHref(def.owned_by) : null} />
          </div>
        </header>

        <div style={{ border: '1px solid rgba(255,255,255,0.07)', borderRadius: 16, background: 'rgba(15,15,20,0.6)', overflow: 'hidden' }}>
          <div style={sectionHeader()}>
            <h3 style={{ margin: 0, color: '#fafafa', fontSize: 14, fontWeight: 700 }}>{t('asset.holdersTitle')}</h3>
            <span style={{ color: '#6b7280', fontSize: 11 }}>{holders.length}</span>
          </div>
          {holders.length === 0 && <div style={{ padding: 24, textAlign: 'center', color: '#6b7280', fontSize: 12 }}>{t('common.empty')}</div>}
          {holders.length > 0 && (
            <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: 13 }}>
              <thead>
                <tr>
                  <th style={th()}>{t('common.address')}</th>
                  <th style={{ ...th(), textAlign: 'right' }}>{t('asset.balance')}</th>
                  <th style={{ ...th(), textAlign: 'right' }}>%</th>
                  <th style={{ ...th(), textAlign: 'right' }}>{t('common.last_seen')}</th>
                </tr>
              </thead>
              <tbody>
                {holders.map((h, i) => (
                  <tr key={h.account_id} style={{ borderBottom: '1px solid rgba(255,255,255,0.04)' }}>
                    <td style={{ padding: '10px 14px' }}>
                      <a className="mono" href={MN.walletHref ? MN.walletHref(h.account_id) : '#wallet/' + encodeURIComponent(h.account_id)}
                         style={{ color: '#C8A0B8', textDecoration: 'none', borderBottom: '1px dotted rgba(200,160,184,0.40)' }}
                         title={h.account_id}>{MN.shortAccount(h.account_id, 12, 8)}</a>
                    </td>
                    <td className="mono" style={{ padding: '10px 14px', textAlign: 'right', color: '#fafafa', fontWeight: 600 }}>{h.balance}</td>
                    <td className="mono" style={{ padding: '10px 14px', textAlign: 'right', color: '#9ca3af' }}>{h.pct != null ? h.pct.toFixed(2) + '%' : '—'}</td>
                    <td style={{ padding: '10px 14px', textAlign: 'right', color: '#6b7280', fontSize: 11 }}>{MN.fmt.relative(h.updated_at)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          )}
        </div>
      </section>
    );
  }

  function Stat({ k, v, link }) {
    return (
      <div>
        <div style={{ color: '#9ca3af', fontSize: 11, fontWeight: 600, letterSpacing: '0.10em', textTransform: 'uppercase' }}>{k}</div>
        {link
          ? <a className="mono" href={link} style={{ color: '#C8A0B8', fontSize: 16, fontWeight: 700, textDecoration: 'none', borderBottom: '1px dotted rgba(200,160,184,0.40)' }}>{v}</a>
          : <div className="mono" style={{ color: '#fafafa', fontSize: 18, fontWeight: 700, marginTop: 4 }}>{v}</div>}
      </div>
    );
  }
  function sectionHeader() {
    return { display: 'flex', justifyContent: 'space-between', alignItems: 'baseline',
             padding: '12px 16px', background: 'rgba(255,255,255,0.02)',
             borderBottom: '1px solid rgba(255,255,255,0.06)' };
  }
  function th() {
    return { textAlign: 'left', padding: '10px 14px', color: '#9ca3af',
             fontSize: 11, fontWeight: 600, letterSpacing: '0.08em', textTransform: 'uppercase',
             borderBottom: '1px solid rgba(255,255,255,0.07)', background: 'rgba(255,255,255,0.02)' };
  }

  // -----------------------------------------------------------
  // Top-level dispatch: list ↔ detail by URL hash
  // -----------------------------------------------------------
  function Assets() {
    const [hash, setHash] = React.useState(() => (window.location.hash || '').replace(/^#/, ''));
    React.useEffect(() => {
      function on() { setHash((window.location.hash || '').replace(/^#/, '')); }
      window.addEventListener('hashchange', on);
      return () => window.removeEventListener('hashchange', on);
    }, []);
    if (hash.startsWith('asset/')) {
      const id = decodeURIComponent(hash.slice('asset/'.length));
      if (id) return <AssetDetail idOrAlias={id} />;
    }
    return <AssetList />;
  }

  MN.Assets = Assets;
})(window.MN);
