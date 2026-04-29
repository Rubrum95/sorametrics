/* global window, React */
// ============================================================
// js/minamoto/ecosystem.jsx
// NFTs / RWAs / Kaigi relays. All three live endpoints exist on
// Torii but return empty today (network is fresh). The pages are
// pre-wired so that when the first record lands it shows up
// without further deploy.
// ============================================================

window.MN = window.MN || {};

(function (MN) {
  function EmptyState({ icon, label, hint }) {
    return (
      <div style={{
        padding: '40px 24px', textAlign: 'center',
        background: 'rgba(20,20,28,0.50)', border: '1px dashed rgba(255,255,255,0.10)',
        borderRadius: 14,
      }}>
        <div style={{ fontSize: 38, color: '#374151', marginBottom: 12 }}>{icon}</div>
        <div style={{ color: '#fafafa', fontSize: 15, fontWeight: 600, marginBottom: 6 }}>{label}</div>
        <div style={{ color: '#6b7280', fontSize: 12, lineHeight: 1.6, maxWidth: 460, margin: '0 auto' }}>{hint}</div>
      </div>
    );
  }

  // -----------------------------------------------------------
  // NFTs
  // -----------------------------------------------------------
  // Torii response shape mirrors /v1/explorer/assets but for NFTs.
  // We render the same compact list when data appears.
  function Nfts() {
    const t = MN.i18n.useT();
    // Proxy via the same Torii /v1/explorer/nfts using the api wrapper
    const { data, error, loading } = MN.useFetch('/explorer/nfts', 60_000);
    const items = data && data.items ? data.items : [];
    return (
      <section className="section">
        <div className="page-header">
          <div>
            <h1 className="page-title">{t('nft.title')}</h1>
            <div className="page-sub">{t('nft.subtitle')}</div>
          </div>
        </div>
        {error && <div style={{ color: '#EF4444', padding: 12 }}>{error.message}</div>}
        {!loading && items.length === 0 && <EmptyState icon="◇" label={t('nft.emptyLabel')} hint={t('nft.emptyHint')} />}
        {items.length > 0 && (
          <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(280px, 1fr))', gap: 12 }}>
            {items.map((n, i) => (
              <div key={i} style={{ padding: '14px 16px', borderRadius: 12, background: 'rgba(20,20,28,0.72)', border: '1px solid rgba(255,255,255,0.07)' }}>
                <div className="mono" style={{ color: '#C8A0B8', fontSize: 12 }}>{JSON.stringify(n).slice(0, 200)}</div>
              </div>
            ))}
          </div>
        )}
      </section>
    );
  }

  // -----------------------------------------------------------
  // RWAs (Real-World Assets)
  // -----------------------------------------------------------
  function Rwas() {
    const t = MN.i18n.useT();
    const { data, error, loading } = MN.useFetch('/explorer/rwas', 60_000);
    const items = data && data.items ? data.items : [];
    return (
      <section className="section">
        <div className="page-header">
          <div>
            <h1 className="page-title">{t('rwa.title')}</h1>
            <div className="page-sub">{t('rwa.subtitle')}</div>
          </div>
        </div>
        {error && <div style={{ color: '#EF4444', padding: 12 }}>{error.message}</div>}
        {!loading && items.length === 0 && <EmptyState icon="🏛" label={t('rwa.emptyLabel')} hint={t('rwa.emptyHint')} />}
        {items.length > 0 && (
          <pre className="mono" style={{ color: '#C8A0B8', fontSize: 11, padding: 12, borderRadius: 12, background: 'rgba(20,20,28,0.72)', border: '1px solid rgba(255,255,255,0.07)' }}>{JSON.stringify(items, null, 2)}</pre>
        )}
      </section>
    );
  }

  // -----------------------------------------------------------
  // Kaigi cross-chain relays
  // -----------------------------------------------------------
  function Kaigi() {
    const t = MN.i18n.useT();
    const { data, error, loading } = MN.useFetch('/kaigi/relays', 60_000);
    const items = data && data.items ? data.items : [];
    return (
      <section className="section">
        <div className="page-header">
          <div>
            <h1 className="page-title">{t('kaigi.title')}</h1>
            <div className="page-sub">{t('kaigi.subtitle')}</div>
          </div>
        </div>
        {error && <div style={{ color: '#EF4444', padding: 12 }}>{error.message}</div>}
        {!loading && items.length === 0 && <EmptyState icon="⇄" label={t('kaigi.emptyLabel')} hint={t('kaigi.emptyHint')} />}
        {items.length > 0 && (
          <pre className="mono" style={{ color: '#C8A0B8', fontSize: 11, padding: 12, borderRadius: 12, background: 'rgba(20,20,28,0.72)', border: '1px solid rgba(255,255,255,0.07)' }}>{JSON.stringify(items, null, 2)}</pre>
        )}
      </section>
    );
  }

  MN.Nfts = Nfts;
  MN.Rwas = Rwas;
  MN.Kaigi = Kaigi;
})(window.MN);
