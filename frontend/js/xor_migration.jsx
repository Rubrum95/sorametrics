/* global React, useT */
// ============================================================
// js/xor_migration.jsx (SORA v2 dashboard)
// Mirror image of /minamoto's cross-chain section, oriented from
// SORA v2's side. Each row is one verified XOR migration:
//   v2 burn block + tx + signer  →  Minamoto claim block + tx + recipient
// Powered by /api/sorav2/xor-migration/cross-chain/* (alias of the
// same Minamoto router — the registry of migrations is shared).
// ============================================================

const { useState, useEffect, useMemo } = React;

(function () {
  function fmtXor(s) {
    if (s == null) return '—';
    const n = Number(s);
    if (!Number.isFinite(n)) return s;
    if (n === 0) return '0';
    if (n < 0.001) return n.toFixed(6);
    if (n < 1) return n.toFixed(4);
    if (n < 1000) return n.toFixed(2);
    return n.toLocaleString('en-US', { maximumFractionDigits: 2 });
  }
  function shortHash(h, head = 8, tail = 6) {
    if (!h) return '';
    return h.length > head + tail + 1 ? h.slice(0, head) + '…' + h.slice(-tail) : h;
  }
  function shortAddr(a, head = 10, tail = 6) {
    if (!a) return '';
    return a.length > head + tail + 1 ? a.slice(0, head) + '…' + a.slice(-tail) : a;
  }
  function relative(iso) {
    if (!iso) return '—';
    const t = typeof iso === 'string' ? Date.parse(iso) : Number(iso);
    if (!Number.isFinite(t)) return '—';
    const sec = Math.max(0, Math.round((Date.now() - t) / 1000));
    if (sec < 60) return sec + 's ago';
    if (sec < 3600) return Math.round(sec / 60) + 'min ago';
    if (sec < 86400) return Math.round(sec / 3600) + 'h ago';
    return Math.round(sec / 86400) + 'd ago';
  }

  function useFetch(path, intervalMs = 30000) {
    const [state, setState] = useState({ data: null, error: null, loading: true });
    useEffect(() => {
      let cancelled = false;
      function load() {
        fetch(path, { cache: 'no-store' })
          .then(r => r.ok ? r.json() : Promise.reject(new Error('HTTP ' + r.status)))
          .then(data => { if (!cancelled) setState({ data, error: null, loading: false }); })
          .catch(error => { if (!cancelled) setState(s => ({ data: s.data, error, loading: false })); });
      }
      load();
      const id = intervalMs ? setInterval(load, intervalMs) : null;
      return () => { cancelled = true; if (id) clearInterval(id); };
    }, [path, intervalMs]);
    return state;
  }

  function Kpi({ label, value, sub }) {
    return (
      <div className="card" style={{ position: 'relative', padding: '20px 22px' }}>
        <div style={{ position: 'absolute', top: 0, left: 0, right: 0, height: 2, background: '#9B1B30', opacity: 0.55 }} />
        <div style={{ color: 'var(--fg-2)', fontSize: 11, fontWeight: 600, letterSpacing: '0.10em', textTransform: 'uppercase' }}>{label}</div>
        <div className="num" style={{ color: 'var(--fg-0)', fontSize: 26, fontWeight: 800, marginTop: 8, letterSpacing: '-0.02em' }}>{value}</div>
        {sub && <div style={{ color: 'var(--fg-3)', fontSize: 12, marginTop: 4 }}>{sub}</div>}
      </div>
    );
  }

  function XorMigrationSection() {
    const t = (typeof useT === 'function') ? useT() : ((k) => k);
    const stats = useFetch('/api/sorav2/xor-migration/cross-chain/stats', 30_000);
    const list  = useFetch('/api/sorav2/xor-migration/cross-chain/claims?per_page=50', 30_000);
    const s = stats.data || {};
    const items = (list.data && list.data.items) || [];

    return (
      <div className="section">
        <div className="page-header">
          <div>
            <h1 className="page-title">{t('xormig.title') || 'XOR cross-chain migration'}</h1>
            <div className="page-sub">{t('xormig.subtitle') || 'XOR burned on SORA v2 with eligibility from block #25,867,650 is reclaimable on Minamoto. Each verified migration carries the v2 burn-tx hash on-chain in Minamoto so we can stitch both sides into one timeline.'}</div>
          </div>
        </div>

        <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(180px, 1fr))', gap: 14, marginTop: 16 }}>
          <Kpi label={t('xormig.totalClaims') || 'Migrations'}     value={(s.total_claims != null ? s.total_claims : 0).toLocaleString()} />
          <Kpi label={t('xormig.totalXor')    || 'XOR migrated'}    value={fmtXor(s.total_xor_claimed)}   sub="leaves v2 supply" />
          <Kpi label={t('xormig.uniqueWallets') || 'Unique senders'} value={(s.unique_recipients != null ? s.unique_recipients : 0).toLocaleString()} />
          <Kpi label={t('xormig.lastClaim')   || 'Last migration'}  value={s.last_claim_at ? relative(s.last_claim_at) : '—'} sub={s.last_claim_at} />
        </div>

        <div className="card" style={{ marginTop: 18, overflow: 'auto' }}>
          {list.error && <div style={{ color: 'var(--err)', padding: 12 }}>{list.error.message}</div>}
          <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: 13 }}>
            <thead>
              <tr style={{ background: 'rgba(255,255,255,0.02)', borderBottom: '1px solid var(--border)' }}>
                <th style={th()}>{t('xormig.when') || 'When'}</th>
                <th style={th()}>XOR</th>
                <th style={th()}>{t('xormig.v2Block')   || 'v2 block'}</th>
                <th style={th()}>{t('xormig.v2BurnTx') || 'v2 burn tx'}</th>
                <th style={th()}>{t('xormig.v2Signer') || 'v2 signer'}</th>
                <th style={{ ...th(), borderLeft: '1px solid var(--border)' }}>{t('xormig.mnBlock')     || 'MN block'}</th>
                <th style={th()}>{t('xormig.mnClaimTx') || 'MN claim tx'}</th>
                <th style={th()}>{t('xormig.mnRecipient') || 'MN recipient'}</th>
              </tr>
            </thead>
            <tbody>
              {items.length === 0 && (
                <tr><td colSpan={8} style={{ padding: 32, textAlign: 'center', color: 'var(--fg-3)' }}>
                  {list.loading ? (t('common.loading') || 'Loading…') : (t('xormig.noClaims') || 'No verified migrations yet — they appear here once a Minamoto claim references its v2 burn-tx hash on-chain.')}
                </td></tr>
              )}
              {items.map(r => (
                <tr key={r.mn_tx_hash} style={{ borderBottom: '1px solid var(--border)' }}>
                  <td style={td()}><span style={{ color: 'var(--fg-2)', fontSize: 12 }}>{relative(r.created_at)}</span></td>
                  <td style={td()}><span className="num" style={{ color: '#10B981', fontWeight: 700 }}>{fmtXor(r.claimed_amount)}</span></td>
                  {/* v2 side (this network) — primary highlighted */}
                  <td style={td()}>
                    {r.v2_block != null
                      ? <a href={'?tab=extrinsics&block=' + r.v2_block} className="num" style={linkPrimary()}>#{r.v2_block.toLocaleString()}</a>
                      : <span style={{ color: 'var(--fg-3)' }}>—</span>}
                  </td>
                  <td style={td()}>
                    <a href={'?tab=extrinsics&q=' + encodeURIComponent(r.v2_tx_hash)} className="num" title={r.v2_tx_hash} style={linkPrimary()}>{shortHash(r.v2_tx_hash)}</a>
                  </td>
                  <td style={td()}>
                    {r.v2_signer
                      ? <a href={'?tab=balance&address=' + encodeURIComponent(r.v2_signer)} className="num" title={r.v2_signer} style={linkPrimary()}>{shortAddr(r.v2_signer)}</a>
                      : <span style={{ color: 'var(--fg-3)' }}>—</span>}
                  </td>
                  {/* Minamoto side (other network) — links open the Minamoto SPA */}
                  <td style={{ ...td(), borderLeft: '1px solid var(--border)' }}>
                    {r.mn_block != null
                      ? <a href={'/minamoto#block/' + r.mn_block} target="_blank" rel="noopener" className="num" style={linkOther()}>#{r.mn_block.toLocaleString()}</a>
                      : <span style={{ color: 'var(--fg-3)' }}>—</span>}
                  </td>
                  <td style={td()}>
                    <a href={'/minamoto#tx/' + r.mn_tx_hash} target="_blank" rel="noopener" className="num" title={r.mn_tx_hash} style={linkOther()}>{shortHash(r.mn_tx_hash)}</a>
                  </td>
                  <td style={td()}>
                    {r.mn_recipient
                      ? <a href={'/minamoto#wallet/' + encodeURIComponent(r.mn_recipient)} target="_blank" rel="noopener" className="num" title={r.mn_recipient} style={linkOther()}>{shortAddr(r.mn_recipient)}</a>
                      : <span style={{ color: 'var(--fg-3)' }}>—</span>}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
    );
  }

  function th() {
    return { textAlign: 'left', padding: '10px 14px', color: 'var(--fg-2)',
             fontSize: 11, fontWeight: 600, letterSpacing: '0.08em', textTransform: 'uppercase' };
  }
  function td() {
    return { padding: '10px 14px', verticalAlign: 'top' };
  }
  function linkPrimary() {
    return { color: 'var(--fg-0)', textDecoration: 'none', borderBottom: '1px dotted rgba(255,255,255,0.20)' };
  }
  function linkOther() {
    // Plum/amethyst hint that the destination is the OTHER network (Minamoto).
    return { color: '#C8A0B8', textDecoration: 'none', borderBottom: '1px dotted rgba(200,160,184,0.40)' };
  }

  window.XorMigrationSection = XorMigrationSection;
})();
