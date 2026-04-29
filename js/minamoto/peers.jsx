/* global window, React */
// ============================================================
// js/minamoto/peers.jsx
// Network topology view powered by /v1/telemetry/peers-info.
// Each peer reports its own connected_peers list, which is how
// /v1/explorer/metrics arrives at "peers: 4" (4 nodes total)
// while a single node's /peers endpoint returns 3 (the OTHER
// three from its perspective). We render that explicitly.
// ============================================================

window.MN = window.MN || {};

(function (MN) {
  // -----------------------------------------------------------
  // Pretty-print a public key short form (last 6 chars carry
  // enough entropy to differentiate the 4 nodes for humans).
  // -----------------------------------------------------------
  function pkShort(pk) {
    if (!pk) return '—';
    const s = String(pk);
    return s.length > 14 ? s.slice(0, 8) + '…' + s.slice(-6) : s;
  }
  function pkTail(pk) {
    if (!pk) return '?';
    return String(pk).slice(-6);
  }

  function parseHostPort(url) {
    try {
      const u = new URL(url);
      return { host: u.hostname, port: parseInt(u.port, 10) || null };
    } catch (_) {
      return { host: url, port: null };
    }
  }

  // -----------------------------------------------------------
  // Small KV row used inside per-node config card
  // -----------------------------------------------------------
  function Row({ k, v }) {
    return (
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'baseline', padding: '4px 0', borderBottom: '1px solid rgba(255,255,255,0.04)' }}>
        <span style={{ color: '#9ca3af', fontSize: 11 }}>{k}</span>
        <span className="mono" style={{ color: '#fafafa', fontSize: 12 }}>{v}</span>
      </div>
    );
  }

  // -----------------------------------------------------------
  // Per-node card showing endpoint, connection state, config and
  // who it has connected (with my-pubkey-tail vs others).
  // -----------------------------------------------------------
  function NodeCard({ node, allNodes }) {
    const { host, port } = parseHostPort(node.url);
    const myPk = node.config && node.config.public_key ? node.config.public_key : null;
    const cfg = node.config || {};
    const peerKeys = node.connected_peers || [];

    return (
      <div style={{
        position: 'relative', padding: '18px 20px', borderRadius: 14,
        background: 'rgba(20,20,28,0.72)', border: '1px solid ' + (node.connected ? 'rgba(16,185,129,0.30)' : 'rgba(255,255,255,0.07)'),
        backdropFilter: 'blur(14px)',
      }}>
        <div style={{ display: 'flex', alignItems: 'baseline', justifyContent: 'space-between', marginBottom: 10 }}>
          <div>
            <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
              <span style={{
                width: 8, height: 8, borderRadius: '50%',
                background: node.connected ? '#10B981' : '#6b7280',
                boxShadow: node.connected ? '0 0 8px rgba(16,185,129,0.50)' : 'none',
              }} />
              <span style={{ color: '#fafafa', fontSize: 14, fontWeight: 700 }}>
                {host}:{port}
              </span>
            </div>
            {myPk && (
              <span className="mono" style={{ color: '#A062B0', fontSize: 11, marginTop: 4, display: 'inline-block' }}
                    title={myPk}>{pkShort(myPk)}</span>
            )}
          </div>
          <span style={{
            fontSize: 10, fontWeight: 700, letterSpacing: '0.10em', textTransform: 'uppercase',
            padding: '3px 8px', borderRadius: 999,
            color: node.connected ? '#10B981' : '#6b7280',
            border: '1px solid ' + (node.connected ? 'rgba(16,185,129,0.30)' : 'rgba(255,255,255,0.10)'),
          }}>{node.connected ? 'Online' : 'Offline'}</span>
        </div>

        {Object.keys(cfg).length > 0 && (
          <div style={{ marginTop: 10, paddingTop: 8, borderTop: '1px solid rgba(255,255,255,0.06)' }}>
            <div style={{ color: '#6b7280', fontSize: 10, fontWeight: 700, letterSpacing: '0.10em', textTransform: 'uppercase', marginBottom: 4 }}>Config</div>
            {cfg.queue_capacity != null && <Row k="Queue capacity" v={cfg.queue_capacity.toLocaleString()} />}
            {cfg.network_block_gossip_size != null && <Row k="Block gossip size" v={cfg.network_block_gossip_size} />}
            {cfg.network_block_gossip_period && cfg.network_block_gossip_period.ms != null && <Row k="Block gossip period" v={cfg.network_block_gossip_period.ms + ' ms'} />}
            {cfg.network_tx_gossip_size != null && <Row k="Tx gossip size" v={cfg.network_tx_gossip_size} />}
            {cfg.network_tx_gossip_period && cfg.network_tx_gossip_period.ms != null && <Row k="Tx gossip period" v={cfg.network_tx_gossip_period.ms + ' ms'} />}
            {node.telemetry_unsupported && <div style={{ color: '#F59E0B', fontSize: 11, marginTop: 4 }}>⚠ telemetry unsupported on this node</div>}
          </div>
        )}

        <div style={{ marginTop: 10, paddingTop: 8, borderTop: '1px solid rgba(255,255,255,0.06)' }}>
          <div style={{ color: '#6b7280', fontSize: 10, fontWeight: 700, letterSpacing: '0.10em', textTransform: 'uppercase', marginBottom: 6 }}>
            Sees {peerKeys.length} peer{peerKeys.length === 1 ? '' : 's'}
          </div>
          <div style={{ display: 'flex', flexWrap: 'wrap', gap: 6 }}>
            {peerKeys.map(pk => {
              const tail = pkTail(pk);
              const otherNode = allNodes.find(n => n.config && n.config.public_key === pk);
              const label = otherNode ? parseHostPort(otherNode.url).port : tail;
              return (
                <span key={pk} title={pk}
                  style={{
                    fontSize: 11, fontWeight: 600, padding: '3px 8px', borderRadius: 6,
                    background: 'rgba(160,98,176,0.12)', color: '#C8A0B8',
                    border: '1px solid rgba(160,98,176,0.24)', fontFamily: 'JetBrains Mono, monospace',
                  }}>
                  {otherNode ? `:${label}` : `…${tail}`}
                </span>
              );
            })}
            {peerKeys.length === 0 && <span style={{ color: '#6b7280', fontSize: 11 }}>none</span>}
          </div>
        </div>
      </div>
    );
  }

  // -----------------------------------------------------------
  // Block propagation chart — how fast a new block reaches every
  // peer. spread_ms = time between first sighting and last
  // sighting across reporters. <100ms is healthy; multi-second
  // spreads point to gossip pressure or stalled peers.
  // -----------------------------------------------------------
  function PropagationChart() {
    const t = MN.i18n.useT();
    const { data, error } = MN.useFetch('/telemetry/propagation', 15_000);
    const items = Array.isArray(data) ? [...data].sort((a, b) => a.block - b.block) : [];
    const ref = React.useRef(null);
    const chartRef = React.useRef(null);

    React.useEffect(() => {
      if (!ref.current || items.length === 0) {
        if (chartRef.current) { chartRef.current.destroy(); chartRef.current = null; }
        return;
      }
      const labels = items.map(i => '#' + i.block);
      const values = items.map(i => i.spread_ms);
      if (chartRef.current) chartRef.current.destroy();
      chartRef.current = new Chart(ref.current.getContext('2d'), {
        type: 'bar',
        data: {
          labels,
          datasets: [{
            label: 'spread_ms',
            data: values,
            backgroundColor: values.map(v => v < 1000 ? 'rgba(16,185,129,0.55)' : v < 5000 ? 'rgba(245,158,11,0.55)' : 'rgba(239,68,68,0.55)'),
            borderColor: values.map(v => v < 1000 ? '#10B981' : v < 5000 ? '#F59E0B' : '#EF4444'),
            borderWidth: 1,
          }],
        },
        options: {
          responsive: true, maintainAspectRatio: false,
          scales: {
            x: { ticks: { color: '#6b7280', maxRotation: 0, autoSkip: true, maxTicksLimit: 12 }, grid: { display: false } },
            y: { ticks: { color: '#6b7280', callback: v => v < 1000 ? v + ' ms' : (v / 1000).toFixed(1) + ' s' }, grid: { color: 'rgba(255,255,255,0.04)' } },
          },
          plugins: {
            legend: { display: false },
            tooltip: {
              callbacks: {
                label: (ctx) => {
                  const i = items[ctx.dataIndex];
                  return `${ctx.parsed.y} ms · ${i.peers_reported} peers reported`;
                }
              }
            },
          },
        },
      });
      return () => { if (chartRef.current) { chartRef.current.destroy(); chartRef.current = null; } };
    }, [JSON.stringify(items)]);

    if (error) return null;
    if (items.length === 0) return null;

    const avg = items.reduce((s, i) => s + i.spread_ms, 0) / items.length;
    const max = items.reduce((m, i) => Math.max(m, i.spread_ms), 0);

    return (
      <div style={{
        marginTop: 18, padding: '18px 20px', borderRadius: 14,
        background: 'rgba(20,20,28,0.72)', border: '1px solid rgba(255,255,255,0.07)',
      }}>
        <div style={{ display: 'flex', alignItems: 'baseline', justifyContent: 'space-between', marginBottom: 10 }}>
          <h3 style={{ margin: 0, color: '#fafafa', fontSize: 14, fontWeight: 700 }}>{t('peers.propagation')}</h3>
          <span style={{ color: '#9ca3af', fontSize: 11 }}>
            {items.length} {t('peers.blocks')} · {t('peers.avg')} {avg < 1000 ? avg.toFixed(0) + ' ms' : (avg / 1000).toFixed(1) + ' s'} · {t('peers.max')} {max < 1000 ? max + ' ms' : (max / 1000).toFixed(1) + ' s'}
          </span>
        </div>
        <div style={{ height: 200 }}><canvas ref={ref}></canvas></div>
        <div style={{ marginTop: 8, color: '#6b7280', fontSize: 11, lineHeight: 1.5 }}>
          {t('peers.propagationNote')}
        </div>
      </div>
    );
  }

  // -----------------------------------------------------------
  // Full Peers section
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


  function Peers() {
    const t = MN.i18n.useT();
    const { data, error, loading } = MN.useFetch('/telemetry/peers-info', 30_000);
    const prop = MN.useFetch('/telemetry/propagation', 15_000);
    const nodes = Array.isArray(data) ? data : [];
    const onlineCount = nodes.filter(n => n.connected).length;
    const propItems = Array.isArray(prop.data) ? prop.data : [];
    const avgSpread = propItems.length ? propItems.reduce((s, i) => s + i.spread_ms, 0) / propItems.length : null;
    const maxSpread = propItems.length ? propItems.reduce((m, i) => Math.max(m, i.spread_ms), 0) : null;

    return (
      <section className="section">
        <div className="page-header">
          <div>
            <h1 className="page-title">{t('peers.title')}</h1>
            <div className="page-sub">
              {nodes.length > 0
                ? `${nodes.length} ${t('peers.nodes')} · ${onlineCount} ${t('peers.online')}`
                : (loading ? t('common.loading') : t('common.empty'))}
            </div>
          </div>
        </div>

        {nodes.length > 0 && (
          <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(180px, 1fr))', gap: 12, marginBottom: 18 }}>
            <Kpi label={t('peers.kpi.total')}     value={MN.fmt.int(nodes.length)} accent="#A062B0" />
            <Kpi label={t('peers.kpi.online')}    value={MN.fmt.int(onlineCount)}
                 sub={nodes.length ? ((onlineCount / nodes.length) * 100).toFixed(0) + '%' : null} />
            <Kpi label={t('peers.kpi.offline')}   value={MN.fmt.int(nodes.length - onlineCount)} />
            <Kpi label={t('peers.kpi.avgSpread')} value={avgSpread != null ? MN.fmt.ms(avgSpread) : '—'}
                 sub={propItems.length ? propItems.length + ' ' + t('peers.blocks') : null} />
            <Kpi label={t('peers.kpi.maxSpread')} value={maxSpread != null ? MN.fmt.ms(maxSpread) : '—'}
                 sub={t('peers.kpi.maxSpreadSub')} />
          </div>
        )}

        {error && <div style={{ color: 'var(--err)', padding: 12 }}>{error.message}</div>}

        {nodes.length > 0 && (
          <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(330px, 1fr))', gap: 14 }}>
            {nodes.map((n, i) => <NodeCard key={i} node={n} allNodes={nodes} />)}
          </div>
        )}

        <PropagationChart />

        <p style={{ marginTop: 14, color: '#6b7280', fontSize: 11, lineHeight: 1.6 }}>
          {t('peers.note')}
        </p>
      </section>
    );
  }

  MN.Peers = Peers;
})(window.MN);
