/* global React, useT, PageHeader */
// Metrics section — site self-analytics (its own sidebar entry, below Tools).
// Reads /analytics/stats (cached server-side 60s): online-now, visits/uniques
// per window, avg session time, most-visited sections. F2/F3 panels (search
// terms, interactions, heatmap, navigation flow…) will be added as more cards
// inside MetricsSection. window.MetricsSection is wired in main.jsx.
const { useState, useEffect } = React;

function smFmtNum(n) {
  n = Number(n) || 0;
  if (n >= 1e6) return (n / 1e6).toFixed(1) + 'M';
  if (n >= 1e3) return (n / 1e3).toFixed(1) + 'k';
  return String(n);
}

function smFmtDuration(ms) {
  if (!ms || ms < 1000) return '0s';
  const s = Math.floor(ms / 1000);
  const m = Math.floor(s / 60);
  const h = Math.floor(m / 60);
  if (h > 0) return h + 'h ' + (m % 60) + 'm';
  if (m > 0) return m + 'm ' + (s % 60) + 's';
  return s + 's';
}

const SM_KPI_BOX = { padding: '8px 10px', background: 'rgba(96,165,250,0.06)', border: '1px solid rgba(96,165,250,0.15)', borderRadius: 8 };
const SM_KPI_NUM = { fontSize: 18, fontWeight: 700 };

function SiteMetricsCard() {
  const t = useT();
  const [data, setData] = useState(null);
  const [err, setErr] = useState(false);
  const [win, setWin] = useState('30d');

  useEffect(() => {
    let cancelled = false;
    const pull = () => fetch('/analytics/stats')
      .then(r => (r.ok ? r.json() : null))
      .then(j => { if (cancelled) return; if (j) { setData(j); setErr(false); } else setErr(true); })
      .catch(() => { if (!cancelled) setErr(true); });
    pull();
    const id = setInterval(pull, 30000);
    return () => { cancelled = true; clearInterval(id); };
  }, []);

  const windows = ['24h', '7d', '30d', '1y'];
  const visits = (data && data.visits) || {};
  const uniques = (data && data.uniques) || {};
  const sessions = (data && data.sessions) || {};
  const sections = (data && data.sections) || [];
  const maxViews = sections.reduce((m, s) => Math.max(m, s.views || 0), 0) || 1;

  return (
    <div className="card" style={{ padding: 0 }}>
      <div className="card-header">
        <div className="card-title"><span className="dot" /> {t('site.title', 'SoraMetrics Metrics')}</div>
        <span className="tag" style={{ display: 'inline-flex', alignItems: 'center', gap: 6 }}>
          <span style={{ width: 7, height: 7, borderRadius: '50%', background: data ? '#34D399' : '#888', display: 'inline-block' }} />
          {data ? (data.online + ' ' + t('site.online', 'online')) : '…'}
        </span>
      </div>

      <div style={{ padding: '16px 20px' }}>
        {err && !data && (
          <div className="muted" style={{ fontSize: 13 }}>{t('site.unavailable', 'Site analytics not available yet.')}</div>
        )}

        <div style={{ display: 'inline-flex', padding: 3, borderRadius: 10, background: 'rgba(255,255,255,0.04)', border: '1px solid var(--border-color)', marginBottom: 16 }}>
          {windows.map(w => {
            const active = win === w;
            return (
              <button key={w} onClick={() => setWin(w)} style={{
                padding: '6px 14px', fontSize: 12, fontWeight: active ? 700 : 500, border: 'none', borderRadius: 7,
                cursor: 'pointer', color: active ? 'var(--fg-0)' : 'var(--fg-2)',
                background: active ? 'linear-gradient(135deg, #9B1B30, #7B5B90)' : 'transparent', transition: 'all .15s',
              }}>{w}</button>
            );
          })}
        </div>

        <div style={{ display: 'grid', gridTemplateColumns: 'repeat(3, 1fr)', gap: 10, marginBottom: 16 }}>
          <div style={SM_KPI_BOX}>
            <div className="muted tiny">{t('site.visits', 'Visits')}</div>
            <div className="num" style={SM_KPI_NUM}>{data ? smFmtNum(visits[win]) : '…'}</div>
          </div>
          <div style={SM_KPI_BOX}>
            <div className="muted tiny">{t('site.uniques', 'Unique visitors')}</div>
            <div className="num" style={SM_KPI_NUM}>{data ? smFmtNum(uniques[win]) : '…'}</div>
          </div>
          <div style={SM_KPI_BOX}>
            <div className="muted tiny">{t('site.avgSession', 'Avg. session')}</div>
            <div className="num" style={SM_KPI_NUM}>{data ? smFmtDuration(data.avgSessionMs) : '…'}</div>
          </div>
        </div>

        <div className="muted tiny" style={{ marginBottom: 14 }}>
          {t('site.onlineNow', 'Online now')}: <b style={{ color: '#34D399' }}>{data ? data.online : '…'}</b>
          {data && data.peak ? ' · ' + t('site.peak', 'peak') + ' ' + data.peak : ''}
          {' · '}{t('site.sessions', 'sessions')} ({win}): {data ? smFmtNum(sessions[win]) : '…'}
        </div>

        <div className="muted tiny" style={{ marginBottom: 8, textTransform: 'uppercase', letterSpacing: 0.5 }}>
          {t('site.topSections', 'Most-visited sections')} <span style={{ opacity: 0.6 }}>(30d)</span>
        </div>
        <div style={{ display: 'grid', gap: 6 }}>
          {sections.length === 0 && <div className="muted tiny">{t('site.noData', 'No data yet.')}</div>}
          {sections.slice(0, 12).map(s => (
            <div key={s.section} style={{ display: 'grid', gridTemplateColumns: '120px 1fr 56px', alignItems: 'center', gap: 10 }}>
              <div style={{ fontSize: 12, fontWeight: 600, textTransform: 'capitalize', whiteSpace: 'nowrap', overflow: 'hidden', textOverflow: 'ellipsis' }}>{s.section}</div>
              <div style={{ height: 8, borderRadius: 4, background: 'rgba(255,255,255,0.05)', overflow: 'hidden' }}>
                <div style={{ height: '100%', width: Math.max(2, (s.views / maxViews) * 100) + '%', background: 'linear-gradient(90deg, #9B1B30, #7B5B90)', borderRadius: 4 }} />
              </div>
              <div className="num" style={{ fontSize: 12, textAlign: 'right' }}>{smFmtNum(s.views)}</div>
            </div>
          ))}
        </div>
      </div>
    </div>
  );
}

function SmBarList({ rows, labelKey, valueKey, fmt }) {
  const list = rows || [];
  const max = list.reduce((a, r) => Math.max(a, r[valueKey] || 0), 0) || 1;
  return (
    <div style={{ display: 'grid', gap: 5 }}>
      {list.length === 0 && <div className="muted tiny">—</div>}
      {list.map((r, i) => (
        <div key={i} style={{ display: 'grid', gridTemplateColumns: '1fr 44px', alignItems: 'center', gap: 8 }}>
          <div style={{ position: 'relative', height: 18, borderRadius: 4, background: 'rgba(255,255,255,0.04)', overflow: 'hidden' }}>
            <div style={{ position: 'absolute', top: 0, left: 0, bottom: 0, width: Math.max(2, ((r[valueKey] || 0) / max) * 100) + '%', background: 'rgba(155,27,48,0.35)' }} />
            <span style={{ position: 'relative', fontSize: 11, lineHeight: '18px', paddingLeft: 6, whiteSpace: 'nowrap', overflow: 'hidden', textOverflow: 'ellipsis', display: 'block' }}>{fmt ? fmt(r[labelKey]) : (r[labelKey] || '—')}</span>
          </div>
          <div className="num" style={{ fontSize: 11, textAlign: 'right' }}>{smFmtNum(r[valueKey])}</div>
        </div>
      ))}
    </div>
  );
}

const SM_PANEL_TITLE = { marginBottom: 8, textTransform: 'uppercase', letterSpacing: 0.5 };

function SiteInteractionsCard() {
  const t = useT();
  const [data, setData] = useState(null);
  useEffect(() => {
    let cancelled = false;
    const pull = () => fetch('/analytics/stats').then(r => (r.ok ? r.json() : null))
      .then(j => { if (!cancelled && j) setData(j); }).catch(() => {});
    pull();
    const id = setInterval(pull, 30000);
    return () => { cancelled = true; clearInterval(id); };
  }, []);
  if (!data) return null;
  return (
    <div className="card" style={{ padding: 0 }}>
      <div className="card-header">
        <div className="card-title"><span className="dot" /> {t('site.interactions.title', 'Interactions & audience')}</div>
        {data.errors7d ? <span className="tag" style={{ color: '#F87171' }}>{data.errors7d} {t('site.errors', 'JS errors 7d')}</span> : null}
      </div>
      <div style={{ padding: '16px 20px', display: 'grid', gridTemplateColumns: 'repeat(2, 1fr)', gap: 18 }}>
        <div>
          <div className="muted tiny" style={SM_PANEL_TITLE}>{t('site.topSearches', 'Top searches')}</div>
          <SmBarList rows={data.searches} labelKey="q" valueKey="count" />
        </div>
        <div>
          <div className="muted tiny" style={SM_PANEL_TITLE}>{t('site.topInteractions', 'Top interactions')}</div>
          <SmBarList rows={data.interactions} labelKey="name" valueKey="count" />
        </div>
        <div>
          <div className="muted tiny" style={SM_PANEL_TITLE}>{t('site.countries', 'Countries')}</div>
          <SmBarList rows={data.countries} labelKey="country" valueKey="count" />
        </div>
        <div>
          <div className="muted tiny" style={SM_PANEL_TITLE}>{t('site.devices', 'Devices')}</div>
          <SmBarList rows={data.devices} labelKey="device" valueKey="count" />
        </div>
        <div>
          <div className="muted tiny" style={SM_PANEL_TITLE}>{t('site.referrers', 'Referrers')}</div>
          <SmBarList rows={data.referrers} labelKey="ref" valueKey="count" />
        </div>
      </div>
    </div>
  );
}

function SiteAdvancedCard() {
  const t = useT();
  const [d, setD] = useState(null);
  useEffect(() => {
    let c = false;
    const pull = () => fetch('/analytics/advanced').then(r => (r.ok ? r.json() : null)).then(j => { if (!c && j) setD(j); }).catch(() => {});
    pull();
    const id = setInterval(pull, 60000);
    return () => { c = true; clearInterval(id); };
  }, []);
  if (!d) return null;
  const days = ['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat'];
  const heat = {};
  let maxHeat = 1;
  (d.heatmap || []).forEach(x => { heat[x.dow + '_' + x.hr] = x.count; if (x.count > maxHeat) maxHeat = x.count; });
  const v = d.vitals || {};
  const ms = x => (x != null ? x + 'ms' : '—');
  return (
    <div className="card" style={{ padding: 0 }}>
      <div className="card-header">
        <div className="card-title"><span className="dot" /> {t('site.advanced.title', 'Engagement & performance')}</div>
        {v.samples ? <span className="tag">{v.samples} {t('site.vitalsSamples', 'vitals')}</span> : null}
      </div>
      <div style={{ padding: '16px 20px' }}>
        <div style={{ display: 'grid', gridTemplateColumns: 'repeat(5, 1fr)', gap: 8, marginBottom: 18 }}>
          <div style={SM_KPI_BOX}><div className="muted tiny">{t('site.bounce', 'Bounce')}</div><div className="num" style={SM_KPI_NUM}>{Math.round((d.bounce || 0) * 100)}%</div></div>
          <div style={SM_KPI_BOX}><div className="muted tiny">{t('site.depth', 'Depth')}</div><div className="num" style={SM_KPI_NUM}>{(d.depth || 0).toFixed(1)}</div></div>
          <div style={SM_KPI_BOX}><div className="muted tiny">TTFB</div><div className="num" style={SM_KPI_NUM}>{ms(v.ttfb)}</div></div>
          <div style={SM_KPI_BOX}><div className="muted tiny">FCP</div><div className="num" style={SM_KPI_NUM}>{ms(v.fcp)}</div></div>
          <div style={SM_KPI_BOX}><div className="muted tiny">LCP</div><div className="num" style={SM_KPI_NUM}>{ms(v.lcp)}</div></div>
        </div>

        <div className="muted tiny" style={SM_PANEL_TITLE}>{t('site.navFlow', 'Navigation flow')} <span style={{ opacity: 0.6 }}>(30d)</span></div>
        <div style={{ marginBottom: 18 }}><SmBarList rows={d.navFlow} labelKey="transition" valueKey="count" /></div>

        <div className="muted tiny" style={SM_PANEL_TITLE}>{t('site.heatmap', 'Activity by hour (UTC)')}</div>
        <div style={{ overflowX: 'auto' }}>
          <div style={{ display: 'grid', gridTemplateColumns: '28px repeat(24, 1fr)', gap: 2, minWidth: 560 }}>
            <div />
            {Array.from({ length: 24 }, (_, h) => <div key={'h' + h} className="muted" style={{ fontSize: 8, textAlign: 'center' }}>{h % 6 === 0 ? h : ''}</div>)}
            {days.map((dn, dow) => (
              <React.Fragment key={dow}>
                <div className="muted" style={{ fontSize: 9, lineHeight: '14px' }}>{dn}</div>
                {Array.from({ length: 24 }, (_, h) => {
                  const c = heat[dow + '_' + h] || 0;
                  const o = c / maxHeat;
                  return <div key={dow + '_' + h} title={dn + ' ' + h + 'h: ' + c} style={{ height: 14, borderRadius: 2, background: o > 0 ? 'rgba(155,27,48,' + (0.15 + o * 0.85).toFixed(2) + ')' : 'rgba(255,255,255,0.03)' }} />;
                })}
              </React.Fragment>
            ))}
          </div>
        </div>
      </div>
    </div>
  );
}

function MetricsSection() {
  const t = useT();
  return (
    <div>
      <PageHeader title={t('site.title', 'SoraMetrics Metrics')} />
      <div style={{ display: 'grid', gap: 16 }}>
        <SiteMetricsCard />
        <SiteInteractionsCard />
        <SiteAdvancedCard />
      </div>
    </div>
  );
}

Object.assign(window, { SiteMetricsCard, SiteInteractionsCard, SiteAdvancedCard, MetricsSection });
