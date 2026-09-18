/* global window, React, Chart */
// ============================================================
// js/minamoto/prometheus.jsx
// Live Prometheus snapshot table + per-metric time-series chart.
// ============================================================

window.MN = window.MN || {};

(function (MN) {
  // -----------------------------------------------------------
  // Per-metric time-series chart (uses Chart.js loaded in HTML)
  // -----------------------------------------------------------
  function MetricChart({ name }) {
    const ref = React.useRef(null);
    const chartRef = React.useRef(null);
    const { data, error } = MN.useFetch(`/prometheus/metric/${encodeURIComponent(name)}?hours=24`, 60_000);

    React.useEffect(() => {
      if (!ref.current) return;
      if (!data || !data.series || data.series.length === 0) {
        if (chartRef.current) { chartRef.current.destroy(); chartRef.current = null; }
        return;
      }
      // Group by labels-stringified key so multi-label metrics show multiple lines
      const groups = new Map();
      for (const s of data.series) {
        const k = JSON.stringify(s.labels || {});
        if (!groups.has(k)) groups.set(k, []);
        groups.get(k).push({ x: new Date(s.ts).getTime(), y: Number(s.value) });
      }
      const palette = ['#A062B0', '#E5243B', '#10B981', '#60A5FA', '#F59E0B', '#7B5B90', '#C8A0B8', '#EC4899'];
      const datasets = [...groups.entries()].slice(0, 8).map(([k, points], i) => ({
        label: k === '{}' ? name : k,
        data: points.sort((a, b) => a.x - b.x),
        borderColor: palette[i % palette.length],
        backgroundColor: palette[i % palette.length] + '22',
        tension: 0.25,
        pointRadius: 0,
        borderWidth: 2,
      }));
      if (chartRef.current) chartRef.current.destroy();
      chartRef.current = new Chart(ref.current.getContext('2d'), {
        type: 'line',
        data: { datasets },
        options: {
          responsive: true, maintainAspectRatio: false,
          interaction: { mode: 'nearest', intersect: false },
          scales: {
            x: { type: 'linear', ticks: { color: '#6b7280', callback: (v) => new Date(v).toLocaleTimeString() }, grid: { color: 'rgba(255,255,255,0.04)' } },
            y: { ticks: { color: '#6b7280' }, grid: { color: 'rgba(255,255,255,0.04)' } },
          },
          plugins: {
            legend: { labels: { color: '#9ca3af', font: { size: 10 } } },
            tooltip: { titleColor: '#fafafa', bodyColor: '#e5e7eb' },
          },
        },
      });
      return () => { if (chartRef.current) { chartRef.current.destroy(); chartRef.current = null; } };
    }, [data, name]);

    if (error) return <div style={{ color: '#EF4444', padding: 12 }}>{error.message}</div>;
    if (!data) return <div style={{ color: '#9ca3af', padding: 12 }}>{MN.i18n.t('common.loading')}</div>;
    if (data.series.length === 0) {
      return <div style={{ color: '#6b7280', padding: 12, textAlign: 'center' }}>No samples yet — the indexer hasn't recorded this metric in the last 24 h.</div>;
    }
    return <div style={{ height: 280 }}><canvas ref={ref}></canvas></div>;
  }

  // -----------------------------------------------------------
  // Snapshot table (live /metrics → parsed → searchable)
  // -----------------------------------------------------------
  function Prometheus() {
    const t = MN.i18n.useT();
    const { data, error, loading } = MN.useFetch('/prometheus/parsed', 30_000);
    const [filter, setFilter] = React.useState('');
    const [picked, setPicked] = React.useState(null);

    const grouped = React.useMemo(() => {
      const out = new Map();
      const samples = data && data.samples ? data.samples : [];
      for (const s of samples) {
        if (!out.has(s.name)) out.set(s.name, []);
        out.get(s.name).push(s);
      }
      return out;
    }, [data]);

    const names = React.useMemo(() => {
      const all = [...grouped.keys()].sort();
      const f = filter.trim().toLowerCase();
      return f ? all.filter(n => n.toLowerCase().includes(f)) : all;
    }, [grouped, filter]);

    return (
      <section className="section">
        <div className="page-header">
          <div>
            <h1 className="page-title">{t('prometheus.title')}</h1>
            <div className="page-sub">{t('prometheus.subtitle')} {data && `· ${MN.fmt.int(data.count)} samples · ${MN.fmt.int(grouped.size)} metrics`}</div>
          </div>
        </div>

        {error && <div style={{ color: 'var(--err)', padding: 12 }}>{error.message}</div>}

        <div style={{ display: 'grid', gridTemplateColumns: '320px 1fr', gap: 18, alignItems: 'start' }}>
          <div style={{ border: '1px solid rgba(255,255,255,0.07)', borderRadius: 14, background: 'rgba(15,15,20,0.6)', overflow: 'hidden', maxHeight: 600 }}>
            <input
              type="search" value={filter} onChange={e => setFilter(e.target.value)}
              placeholder="Search metric…"
              style={{
                width: '100%', boxSizing: 'border-box', padding: '12px 14px',
                background: 'rgba(255,255,255,0.03)', color: '#fafafa', fontSize: 13,
                border: 'none', borderBottom: '1px solid rgba(255,255,255,0.07)', outline: 'none',
              }} />
            <div style={{ overflowY: 'auto', maxHeight: 540 }}>
              {loading && !data && <div style={{ color: '#9ca3af', padding: 12 }}>{t('common.loading')}</div>}
              {names.map(n => {
                const rows = grouped.get(n) || [];
                const active = picked === n;
                return (
                  <button key={n} onClick={() => setPicked(n)}
                    style={{
                      display: 'flex', justifyContent: 'space-between', alignItems: 'baseline',
                      width: '100%', textAlign: 'left', padding: '8px 12px', borderRadius: 0,
                      background: active ? 'rgba(160,98,176,0.18)' : 'transparent',
                      color: active ? '#fafafa' : '#e5e7eb',
                      border: 'none', borderLeft: '2px solid ' + (active ? '#A062B0' : 'transparent'),
                      cursor: 'pointer', fontSize: 12, fontFamily: 'JetBrains Mono, monospace',
                    }}>
                    <span>{n}</span>
                    <span style={{ color: '#6b7280', fontSize: 11 }}>{rows.length}</span>
                  </button>
                );
              })}
            </div>
          </div>

          <div style={{ border: '1px solid rgba(255,255,255,0.07)', borderRadius: 14, background: 'rgba(15,15,20,0.6)', padding: 18, minHeight: 320 }}>
            {!picked && <div style={{ color: '#6b7280', textAlign: 'center', padding: 32 }}>← Pick a metric on the left</div>}
            {picked && (
              <>
                <div style={{ display: 'flex', alignItems: 'baseline', justifyContent: 'space-between', marginBottom: 12 }}>
                  <h3 className="mono" style={{ margin: 0, color: '#fafafa', fontSize: 16 }}>{picked}</h3>
                  <span style={{ color: '#9ca3af', fontSize: 12 }}>{(grouped.get(picked) || []).length} active series</span>
                </div>
                <MetricChart name={picked} />
                <div style={{ marginTop: 16, maxHeight: 220, overflowY: 'auto' }}>
                  <table style={{ width: '100%', fontSize: 12, borderCollapse: 'collapse' }}>
                    <thead>
                      <tr style={{ color: '#9ca3af', textAlign: 'left' }}>
                        <th style={{ padding: '6px 8px', borderBottom: '1px solid rgba(255,255,255,0.07)' }}>Labels</th>
                        <th style={{ padding: '6px 8px', borderBottom: '1px solid rgba(255,255,255,0.07)', textAlign: 'right' }}>{t('common.value')}</th>
                      </tr>
                    </thead>
                    <tbody>
                      {(grouped.get(picked) || []).map((s, i) => (
                        <tr key={i} style={{ borderBottom: '1px solid rgba(255,255,255,0.04)' }}>
                          <td className="mono" style={{ padding: '6px 8px', color: '#C8A0B8' }}>
                            {Object.keys(s.labels).length === 0 ? '—' : Object.entries(s.labels).map(([k, v]) => k + '="' + v + '"').join(', ')}
                          </td>
                          <td className="mono" style={{ padding: '6px 8px', color: '#fafafa', textAlign: 'right' }}>{s.value.toLocaleString()}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </>
            )}
          </div>
        </div>
      </section>
    );
  }

  MN.Prometheus = Prometheus;
})(window.MN);
