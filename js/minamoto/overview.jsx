/* global window, React */
// ============================================================
// js/minamoto/overview.jsx
// "Pulse" — replicates the v2 Pulse layout (4 stat-cards + live feed
// + trending + network health) so users feel at home across networks.
// Data wired to the equivalent Minamoto endpoints.
// ============================================================

window.MN = window.MN || {};

(function (MN) {
  // -----------------------------------------------------------
  // KPI card — uses v2's .stat-card class.
  // -----------------------------------------------------------
  function Kpi({ label, value, sub }) {
    return (
      <div className="stat-card">
        <span className="stat-label">{label}</span>
        <span className="stat-value num">{value}</span>
        {sub && <span className="stat-sub">{sub}</span>}
      </div>
    );
  }

  // -----------------------------------------------------------
  // LIVE FEED — recent blocks. The closest Iroha 3 equivalent of
  // v2's swaps/transfers/burns feed. Keeps the same visual rhythm.
  // -----------------------------------------------------------
  const FEED_FILTERS = ['all', 'block', 'mint', 'burn', 'transfer', 'register'];

  function LiveFeed() {
    const t = MN.i18n.useT();
    const [filter, setFilter] = React.useState('all');

    // Pull recent blocks + recent instructions and merge into one stream
    // sorted DESC by created_at. Refreshes every 10 s.
    const blocks = MN.useFetch('/blocks?per_page=20', 10_000);
    const isi    = MN.useFetch('/instructions?per_page=40', 10_000);

    const events = React.useMemo(() => {
      const out = [];
      if (filter === 'all' || filter === 'block') {
        for (const b of (blocks.data && blocks.data.items) || []) {
          out.push({
            kind: 'block',
            t: Date.parse(b.created_at) || 0,
            payload: b,
          });
        }
      }
      if (filter === 'all' || ['mint','burn','transfer','register'].includes(filter)) {
        for (const i of (isi.data && isi.data.items) || []) {
          if (filter !== 'all' && i.kind.toLowerCase() !== filter) continue;
          out.push({
            kind: 'isi',
            t: Date.parse(i.created_at) || 0,
            payload: i,
          });
        }
      }
      return out.sort((a, b) => b.t - a.t).slice(0, 30);
    }, [blocks.data, isi.data, filter]);

    const counts = React.useMemo(() => {
      const c = { all: 0, block: 0, mint: 0, burn: 0, transfer: 0, register: 0 };
      for (const b of (blocks.data && blocks.data.items) || []) c.block++;
      for (const i of (isi.data && isi.data.items) || []) {
        const k = i.kind.toLowerCase();
        if (k in c) c[k]++;
      }
      c.all = c.block + c.mint + c.burn + c.transfer + c.register;
      return c;
    }, [blocks.data, isi.data]);

    return (
      <div className="card" style={{ padding: 0, overflow: 'hidden' }}>
        <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', padding: '16px 18px 12px', borderBottom: '1px solid var(--border)' }}>
          <div style={{ display: 'flex', alignItems: 'center', gap: 10 }}>
            <span style={{ width: 8, height: 8, borderRadius: '50%', background: '#10B981', boxShadow: '0 0 8px rgba(16,185,129,0.6)' }} />
            <span style={{ color: 'var(--fg-2)', fontSize: 11, fontWeight: 700, letterSpacing: '0.16em', textTransform: 'uppercase' }}>{t('pulse.liveFeed')}</span>
          </div>
          <span className="tag">{events.length} {t('pulse.events')}</span>
        </div>
        <div style={{ display: 'flex', gap: 6, padding: '12px 18px', flexWrap: 'wrap', borderBottom: '1px solid var(--border)' }}>
          {FEED_FILTERS.map(f => (
            <button key={f} onClick={() => setFilter(f)}
              className={'btn' + (filter === f ? ' active' : '')}
              style={{
                padding: '4px 10px', fontSize: 12,
                background: filter === f ? 'rgba(160,98,176,0.20)' : 'rgba(255,255,255,0.03)',
                color: filter === f ? 'var(--fg-0)' : 'var(--fg-2)',
                border: '1px solid ' + (filter === f ? 'rgba(160,98,176,0.40)' : 'var(--border)'),
              }}>
              {f === 'all' ? 'All' : f.charAt(0).toUpperCase() + f.slice(1)} <span style={{ color: 'var(--fg-3)', marginLeft: 4 }}>{counts[f]}</span>
            </button>
          ))}
        </div>
        <div style={{ maxHeight: 540, overflowY: 'auto' }}>
          {events.length === 0 && (
            <div style={{ padding: 24, textAlign: 'center', color: 'var(--fg-3)', fontSize: 12 }}>{t('common.empty')}</div>
          )}
          {events.map((e, i) => <FeedRow key={i} ev={e} />)}
        </div>
      </div>
    );
  }

  function FeedRow({ ev }) {
    if (ev.kind === 'block') {
      const b = ev.payload;
      return (
        <a href={'#block/' + b.height}
           style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between',
                    padding: '12px 18px', borderBottom: '1px solid var(--border)',
                    textDecoration: 'none', color: 'inherit', transition: 'background 200ms ease' }}
           onMouseEnter={e => e.currentTarget.style.background = 'rgba(255,255,255,0.02)'}
           onMouseLeave={e => e.currentTarget.style.background = 'transparent'}>
          <div style={{ display: 'flex', alignItems: 'center', gap: 12 }}>
            <span className="tag" style={{ background: 'rgba(160,98,176,0.14)', color: '#C8A0B8', borderColor: 'rgba(160,98,176,0.30)' }}>BLOCK</span>
            <div>
              <div className="num" style={{ color: 'var(--fg-0)', fontWeight: 600 }}>
                Block <span style={{ color: 'var(--accent)' }}>#{MN.fmt.int(b.height)}</span>
                {b.transactions_committed > 0 && <span className="muted" style={{ marginLeft: 8, fontWeight: 400 }}>· {b.transactions_committed} tx</span>}
              </div>
              <div className="muted tiny num" style={{ marginTop: 2 }}>{MN.shortHash(b.hash, 8, 6)}</div>
            </div>
          </div>
          <div className="muted tiny">{MN.fmt.relative(b.created_at)}</div>
        </a>
      );
    }
    // ISI feed row
    const i = ev.payload;
    const kindColor = {
      Mint: '#6EE7B7', Burn: '#FCA5A5', Transfer: '#93C5FD',
      Register: '#C8A0B8', Grant: '#FCD34D',
    }[i.kind] || 'var(--fg-2)';
    return (
      <a href={'#tx/' + i.transaction_hash}
         style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between',
                  padding: '12px 18px', borderBottom: '1px solid var(--border)',
                  textDecoration: 'none', color: 'inherit', transition: 'background 200ms ease' }}
         onMouseEnter={e => e.currentTarget.style.background = 'rgba(255,255,255,0.02)'}
         onMouseLeave={e => e.currentTarget.style.background = 'transparent'}>
        <div style={{ display: 'flex', alignItems: 'center', gap: 12 }}>
          <span className="tag" style={{ color: kindColor, borderColor: kindColor + '55', background: kindColor + '14' }}>{i.kind.toUpperCase()}</span>
          <div>
            <div style={{ color: 'var(--fg-0)' }}>
              {MN.ui.PayloadLine ? <MN.ui.PayloadLine kind={i.kind} payload={i.payload} /> : i.kind}
            </div>
            <div className="muted tiny num" style={{ marginTop: 2 }}>
              #{MN.fmt.int(i.block)} · {MN.shortAccount(i.authority, 6, 4)}
            </div>
          </div>
        </div>
        <div className="muted tiny">{MN.fmt.relative(i.created_at)}</div>
      </a>
    );
  }

  // -----------------------------------------------------------
  // ASSETS PANEL — equivalent of v2's "Tokens en tendencia".
  // Lists the asset definitions with supply + holders count.
  // -----------------------------------------------------------
  function AssetsPanel() {
    const t = MN.i18n.useT();
    const { data } = MN.useFetch('/asset-definitions', 60_000);
    const items = (data && data.items) || [];

    return (
      <div className="card" style={{ padding: 0, overflow: 'hidden' }}>
        <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', padding: '16px 18px 12px', borderBottom: '1px solid var(--border)' }}>
          <span style={{ color: 'var(--fg-2)', fontSize: 11, fontWeight: 700, letterSpacing: '0.16em', textTransform: 'uppercase' }}>{t('pulse.assets')}</span>
          <span className="tag">{items.length}</span>
        </div>
        {items.length === 0 && (
          <div style={{ padding: 18, textAlign: 'center', color: 'var(--fg-3)', fontSize: 12 }}>{t('common.loading')}</div>
        )}
        {items.map(d => {
          const isXor = d.alias === 'xor#universal' || (d.name || '').toLowerCase() === 'xor';
          return (
            <a key={d.id} href={'#asset/' + encodeURIComponent(d.alias || d.name || d.id)}
               style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between',
                        padding: '14px 18px', borderBottom: '1px solid var(--border)',
                        textDecoration: 'none', color: 'inherit', transition: 'background 200ms ease' }}
               onMouseEnter={e => e.currentTarget.style.background = 'rgba(255,255,255,0.02)'}
               onMouseLeave={e => e.currentTarget.style.background = 'transparent'}>
              <div style={{ display: 'flex', alignItems: 'center', gap: 10, minWidth: 0 }}>
                <MN.TokenIcon asset={d} size={30} />
                <div style={{ minWidth: 0 }}>
                  <div style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
                    <span style={{ color: 'var(--fg-0)', fontSize: 13, fontWeight: 600 }}>{d.name || MN.shortHash(d.id, 8, 4)}</span>
                    {d.confidential_mode === 'Convertible' && (
                      <span className="tag accent" style={{ fontSize: 9, padding: '1px 6px' }} title="Asset supports ZK shielded transfers (convertible between transparent and confidential states via Halo2)">ZK</span>
                    )}
                  </div>
                  {d.alias && <div className="muted tiny num" style={{ marginTop: 2 }}>{d.alias}</div>}
                </div>
              </div>
              <div style={{ textAlign: 'right' }}>
                <div className="num" style={{ color: 'var(--fg-0)', fontSize: 13, fontWeight: 600 }}>{fmtAmount(d.total_quantity)}</div>
                <div className="muted tiny">{d.holders} {d.holders === 1 ? 'holder' : 'holders'}</div>
              </div>
            </a>
          );
        })}
      </div>
    );
  }

  function fmtAmount(v) {
    if (v == null || v === '') return '—';
    const n = Number(v);
    if (!Number.isFinite(n)) return v;
    if (n >= 1_000_000) return (n / 1_000_000).toFixed(2) + 'M';
    if (n >= 1_000) return (n / 1_000).toFixed(2) + 'k';
    if (n < 1) return n.toFixed(4);
    return n.toLocaleString('en-US', { maximumFractionDigits: 4 });
  }

  // -----------------------------------------------------------
  // NETWORK HEALTH — mirrors v2's "Salud de la Red" panel.
  // -----------------------------------------------------------
  function HealthPanel() {
    const t = MN.i18n.useT();
    const live = MN.useFetch('/network-state/live', 10_000);
    const status = MN.useFetch('/status', 30_000);
    const peersInfo = MN.useFetch('/telemetry/peers-info', 30_000);

    const m = (live.data && live.data.state) || {};
    const s = status.data || {};
    const sumeragi = s.sumeragi || {};
    const nodes = Array.isArray(peersInfo.data) ? peersInfo.data : [];
    const onlineNodes = nodes.filter(n => n.connected).length;

    return (
      <div className="card" style={{ padding: 0, overflow: 'hidden' }}>
        <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', padding: '16px 18px 12px', borderBottom: '1px solid var(--border)' }}>
          <div style={{ display: 'flex', alignItems: 'center', gap: 10 }}>
            <span style={{ width: 8, height: 8, borderRadius: '50%', background: '#A062B0', boxShadow: '0 0 8px rgba(160,98,176,0.6)' }} />
            <span style={{ color: 'var(--fg-2)', fontSize: 11, fontWeight: 700, letterSpacing: '0.16em', textTransform: 'uppercase' }}>{t('pulse.health')}</span>
          </div>
        </div>
        <div style={{ padding: '8px 18px 14px' }}>
          <HealthRow k={t('pulse.validators')} v={onlineNodes > 0 ? onlineNodes + ' ' + t('pulse.active') : (sumeragi.commit_qc_validator_set_len || '—')} />
          <HealthRow k={t('pulse.iroha')}     v={s.build && s.build.version ? s.build.version : '—'} />
          <HealthRow k={t('pulse.blockHeight')} v={m.block != null ? '#' + MN.fmt.int(m.block) : '—'} />
          <HealthRow k={t('pulse.finalized')}   v={m.finalized_block != null ? '#' + MN.fmt.int(m.finalized_block) : '—'} />
          <HealthRow k={t('pulse.avgBlock')}    v={m.avg_block_time ? MN.fmt.ms(m.avg_block_time.ms) : '—'} />
          <HealthRow k={t('pulse.avgCommit')}   v={m.avg_commit_time ? MN.fmt.ms(m.avg_commit_time.ms) : '—'} />
          <HealthRow k={t('pulse.queueSize')}   v={s.queue_size != null ? s.queue_size : '—'} />
          <HealthRow k={t('pulse.viewChanges')} v={s.view_changes != null ? s.view_changes : '—'} />
        </div>
      </div>
    );
  }

  function HealthRow({ k, v }) {
    return (
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'baseline', padding: '7px 0', borderBottom: '1px solid var(--border)' }}>
        <span style={{ color: 'var(--fg-2)', fontSize: 12 }}>{k}</span>
        <span className="num" style={{ color: 'var(--fg-0)', fontSize: 13, fontWeight: 600 }}>{v}</span>
      </div>
    );
  }

  // -----------------------------------------------------------
  // The actual Overview = Pulse top-level component.
  // -----------------------------------------------------------
  function Overview() {
    const t = MN.i18n.useT();
    const live = MN.useFetch('/network-state/live', 10_000);
    const dbState = MN.useFetch('/network-state', 10_000);
    const ccStats = MN.useFetch('/cross-chain/stats', 30_000);
    const xor = MN.useFetch('/asset/xor%23universal', 60_000);

    const s = (live.data && live.data.state) || (dbState.data && dbState.data.state) || {};
    const cc = ccStats.data || {};
    const xorSupply = (xor.data && xor.data.total_quantity) || null;

    const blockHeight = s.block != null ? s.block : (s.block_height != null ? s.block_height : null);
    const peers = s.peers != null ? s.peers : null;
    const accounts = s.accounts != null ? s.accounts : null;
    const blockMs = (s.avg_block_time && s.avg_block_time.ms) || s.avg_block_time_ms || null;

    return (
      <section className="section">
        <div className="page-header">
          <div>
            <h1 className="page-title">{t('pulse.title')}</h1>
            <div className="page-sub">{t('pulse.subtitle')}</div>
          </div>
          <div className="page-actions">
            <span className="tag" style={{ background: 'rgba(16,185,129,0.12)', color: '#10B981', borderColor: 'rgba(16,185,129,0.30)' }}>
              <span style={{ width: 6, height: 6, borderRadius: '50%', background: 'currentColor' }} />
              {t('pulse.connected')} · #{MN.fmt.int(blockHeight)}
            </span>
          </div>
        </div>

        {/* Top KPI row */}
        <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(180px, 1fr))', gap: 12, marginBottom: 18 }}>
          <Kpi label={t('pulse.kpi.height')}    value={'#' + MN.fmt.int(blockHeight)}    sub={s.finalized_block != null ? 'finalized #' + s.finalized_block : ''} />
          <Kpi label={t('pulse.kpi.peers')}     value={MN.fmt.int(peers)}                sub={t('pulse.kpi.peersSub')} />
          <Kpi label={t('pulse.kpi.accounts')}  value={MN.fmt.int(accounts)} />
          <Kpi label={t('pulse.kpi.xorSupply')} value={fmtAmount(xorSupply)}             sub={'XOR'} />
          <Kpi label={t('pulse.kpi.migrated')}  value={fmtAmount(cc.total_xor_claimed)}  sub={cc.total_claims != null ? cc.total_claims + ' ' + t('cc.totalClaims').toLowerCase() : ''} />
          <Kpi label={t('pulse.kpi.blockTime')} value={MN.fmt.ms(blockMs)}                sub={s.last_block_at ? MN.fmt.relative(s.last_block_at) : ''} />
        </div>

        {/* Two-col layout: live feed left, assets+health stacked right.
            The CSS class `mn-pulse-grid` collapses to a single column on
            mobile (≤640px) — see minamoto.html <style>. Without that, the
            right column gets squeezed to ~33vw which makes the Assets
            list and Network Health rows wrap onto multiple lines. */}
        <div className="mn-pulse-grid" style={{ display: 'grid', gridTemplateColumns: '2fr 1fr', gap: 18 }}>
          <LiveFeed />
          <div style={{ display: 'flex', flexDirection: 'column', gap: 18 }}>
            <AssetsPanel />
            <HealthPanel />
          </div>
        </div>
      </section>
    );
  }

  MN.Overview = Overview;
})(window.MN);
