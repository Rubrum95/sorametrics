/* global React, useT, LangPicker, useSearch */
const { useState, useEffect, useMemo } = React;

// --- RPC source pill ---
// Polls /health/rpc-source every 30s. Shows:
//   ● Sorametrics node · connected   (we're on the local container — green)
//   ● Fallback: <host> · connected   (WsProvider rotated to a public node — amber)
//   ● Disconnected                   (no WS active — red)
function useRpcSource() {
  const [src, setSrc] = useState(null);
  useEffect(() => {
    let cancelled = false;
    const pull = () => fetch('/health/rpc-source')
      .then(r => r.ok ? r.json() : null)
      .then(j => { if (!cancelled && j) setSrc(j); })
      .catch(() => {});
    pull();
    const id = setInterval(pull, 30_000);
    return () => { cancelled = true; clearInterval(id); };
  }, []);
  return src;
}

// i18n-keyed nav definition. Labels are resolved at render via t().
const NAV_GROUPS = [
  {
    titleKey: 'nav.featured',
    items: [
      // LIVE is the only meaningful badge — it reflects the socket.io stream.
      // Other placeholders ('3', '4W', '5T') were static lies (portfolio had
      // N wallets != 4), so they got removed. If a future badge comes back it
      // should be dynamic from the real store.
      { id: 'pulse',     key: 'nav.pulse',       icon: 'pulse',  count: 'LIVE', countKey: 'common.live' },
      { id: 'intel',     key: 'nav.intelligence',icon: 'bolt' },
      { id: 'studio',    key: 'nav.studio',      icon: 'music' },
    ],
  },
  {
    titleKey: 'nav.my',
    items: [
      { id: 'portfolio', key: 'nav.portfolio', icon: 'wallet' },
      { id: 'balance',   key: 'nav.balance',   icon: 'coins' },
    ],
  },
  {
    titleKey: 'nav.network',
    items: [
      { id: 'swaps',      key: 'nav.swaps',      icon: 'swap' },
      { id: 'extrinsics', key: 'nav.extrinsics', icon: 'ext' },
      { id: 'transfers',  key: 'nav.transfers',  icon: 'send' },
      { id: 'bridges',    key: 'nav.bridges',    icon: 'bridge' },
      { id: 'orderbook',  key: 'nav.orderBook',  icon: 'book' },
      { id: 'pools',      key: 'nav.pools',      icon: 'pools' },
      { id: 'tokens',     key: 'nav.tokens',     icon: 'tokens' },
      { id: 'holders',    key: 'nav.holders',    icon: 'users' },
      { id: 'staking',    key: 'nav.staking',    icon: 'stake' },
      { id: 'gov',        key: 'nav.governance', icon: 'gov' },
      { id: 'polkamarkt', key: 'nav.predict',    icon: 'pools' },
      { id: 'burns',      key: 'nav.burnTracker',icon: 'burn' },
      { id: 'xormig',     key: 'nav.xorMigration', icon: 'bridge' },
    ],
  },
  {
    // "Tools" category kept last in the sidebar per UX feedback — it's a
    // utility drawer, not a primary navigation group.
    titleKey: 'nav.toolsGroup',
    items: [
      { id: 'tools', key: 'nav.tools', icon: 'bolt' },
    ],
  },
];

function Sidebar({ section, setSection }) {
  const t = useT();
  // When the user picks a section while the mobile drawer is open, close the
  // drawer so the content becomes visible. Desktop/tablet keep the sidebar
  // always on — the body class is only set in mobile mode anyway.
  const pickSection = (id) => {
    setSection(id);
    document.body.classList.remove('drawer-open');
  };
  return (
    <>
      {/* Backdrop: tappable overlay that closes the drawer. Only renders in
          mobile drawer-open state via CSS. Kept outside the <aside> so clicks
          on the sidebar don't bubble into it. */}
      <div className="drawer-backdrop"
           onClick={() => document.body.classList.remove('drawer-open')}/>
      <aside className="sidebar">
        <div className="brand">
          {/* Official SORA/XOR mark — same asset served as favicon.svg and used
              by v1's header. Replaced the placeholder hexagon so v6 matches
              the brand identity users already recognise from v1. */}
          <img className="brand-logo" src="/favicon.svg" alt="SoraMetrics" width="28" height="28"/>
          <div>
            <div className="brand-name"><span className="brand-sora">Sora</span><span className="brand-metrics">Metrics</span></div>
          </div>
        </div>

        <div className="nav">
          {NAV_GROUPS.map(g => (
            <React.Fragment key={g.titleKey}>
              <div className="nav-section-title">{t(g.titleKey)}</div>
              {g.items.map(i => {
                const Icon = I[i.icon];
                const countLabel = i.countKey ? t(i.countKey) : i.count;
                return (
                  <div key={i.id}
                       className={'nav-item' + (section === i.id ? ' active' : '')}
                       onClick={() => pickSection(i.id)}>
                    {Icon ? <Icon className="nav-icon"/> : <span className="nav-icon"/>}
                    <span className="nav-label">{t(i.key)}</span>
                    {countLabel && <span className="count">{countLabel}</span>}
                  </div>
                );
              })}
            </React.Fragment>
          ))}
        </div>

        <div className="sidebar-footer">
          <RpcSourcePill/>
        </div>
      </aside>
    </>
  );
}

// Compact pill rendered in the sidebar footer. Three visual states:
//   · Local node up:        green dot · "Sorametrics node · connected"
//   · On a public fallback: amber dot · "Fallback: <host>"
//   · Disconnected:         red dot   · "Disconnected"
// Tooltip exposes the full active URL for the operator.
function RpcSourcePill() {
  const t = useT();
  const src = useRpcSource();
  if (!src) {
    // Unknown state during the first ~1s — show neutral.
    return <div className="live-pill"><span className="live-dot"/> SORA · …</div>;
  }
  if (!src.connected) {
    return (
      <div className="live-pill" style={{color: '#FCA5A5'}} title="WS disconnected">
        <span className="live-dot" style={{background:'#EF4444'}}/> Disconnected
      </div>
    );
  }
  if (src.isLocal || src.isPrimary) {
    return (
      <div className="live-pill" title={src.active}>
        <span className="live-dot"/> Sorametrics node · {t('common.connected')}
      </div>
    );
  }
  // Fallback: rotated to a public node by WsProvider after the primary went down.
  return (
    <div className="live-pill" style={{color:'#FBBF24'}} title={src.active}>
      <span className="live-dot" style={{background:'#F59E0B'}}/>
      Fallback · {src.label}
    </div>
  );
}

function Topbar({ block }) {

  const t = useT();
  const search = useSearch();
  // Pull real era + epoch from /staking/network (refreshed every 30s). Prod
  // exposes activeEra / sessionProgress / sessionsPerEra — sessions per era
  // are SORA's "epochs". We display "<era> · <session_in_era>/<sessions_per_era>".
  const [staking, setStaking] = useState(null);
  useEffect(() => {
    let cancelled = false;
    const pull = () => fetch('/staking/network')
      .then(r => r.ok ? r.json() : null)
      .then(j => { if (!cancelled) setStaking(j); })
      .catch(() => {});
    pull();
    const id = setInterval(pull, 30_000);
    return () => { cancelled = true; clearInterval(id); };
  }, []);

  const era = staking?.activeEra ?? staking?.currentEra;
  const sessionInEra = Number.isFinite(Number(staking?.sessionProgress))
    ? Number(staking.sessionProgress)
    : null;
  const sessionsPerEra = Number(staking?.sessionsPerEra) || null;
  const eraLabel = era != null
    ? (sessionsPerEra && sessionInEra != null
        ? era + ' · ' + sessionInEra + '/' + sessionsPerEra
        : String(era))
    : '—';

  // Toggle the mobile nav drawer by flipping a class on <body>. CSS then
  // switches the sidebar from bottom-bar to full-height drawer mode. Keeps
  // the change local to this component — no extra provider or prop drilling.
  const toggleDrawer = () => {
    document.body.classList.toggle('drawer-open');
  };

  return (
    <div className="topbar">
      {/* Hamburger — shown only on mobile via CSS. Tapping opens the sidebar
          as a full-height drawer so the user can reach ALL 11+ sections, not
          just the 4-5 that fit horizontally in the bottom bar. */}
      <button
        className="mobile-hamburger"
        onClick={toggleDrawer}
        aria-label="Open navigation menu"
        title="Menú">
        <span/><span/><span/>
      </button>
      <div className="search" onClick={() => search.open()} role="button" tabIndex={0}
           onKeyDown={(e) => { if (e.key === 'Enter' || e.key === ' ') search.open(); }}>
        <I.search style={{width:14,height:14}}/>
        {t('common.search')}
        <kbd>⌘K</kbd>
      </div>
      <div className="block-chip hide-mobile">
        <span className="label">{t('topbar.block')}</span>
        <span className="val">#{block.toLocaleString()}</span>
      </div>
      <div className="block-chip hide-mobile" title={staking ? 'Era progress ' + (staking.eraProgress || 0) + '%' : ''}>
        <span className="label">{t('topbar.eraEpoch')}</span>
        <span className="val">{eraLabel}</span>
      </div>
      <LangPicker/>
      <NetworkSwitcher/>
    </div>
  );
}

// ---------------------------------------------------------------
// NetworkSwitcher — pill in the top-right that mirrors the one on
// /minamoto, so the user can hop to the other SORA network or back
// to the landing without leaving keyboard / pointer focus on the page.
// Self-contained (no external state) — closes on outside click.
// ---------------------------------------------------------------
function NetworkSwitcher() {
  const [open, setOpen] = React.useState(false);
  const ref = React.useRef(null);
  React.useEffect(() => {
    function onDoc(e) { if (ref.current && !ref.current.contains(e.target)) setOpen(false); }
    document.addEventListener('mousedown', onDoc);
    return () => document.removeEventListener('mousedown', onDoc);
  }, []);
  const itemStyle = { display:'flex', alignItems:'center', gap:10, padding:'8px 10px',
                      borderRadius:8, color:'#fafafa', textDecoration:'none', fontSize:13, cursor:'pointer' };
  const chipStyle = (grad) => ({ width:22, height:22, borderRadius:6, display:'grid', placeItems:'center',
                                 color:'white', fontWeight:900, fontSize:11, background:grad });
  return (
    <div ref={ref} style={{ position:'relative' }}>
      <button onClick={() => setOpen(o => !o)} aria-haspopup="true" aria-expanded={open}
        style={{
          display:'inline-flex', alignItems:'center', gap:8,
          padding:'6px 12px 6px 8px', borderRadius:999,
          background:'rgba(229,36,59,0.16)', border:'1px solid rgba(229,36,59,0.34)',
          color:'#FCD7D5', fontSize:12, fontWeight:600, letterSpacing:'0.04em',
          cursor:'pointer',
        }}>
        <span style={chipStyle('linear-gradient(135deg,#FF4E3C,#E5243B,#9B1B30)')}>v2</span>
        <span>SORA v2</span>
        <span style={{ opacity:0.6 }}>▾</span>
      </button>
      {open && (
        <div role="menu" style={{
          position:'absolute', top:'calc(100% + 8px)', right:0, minWidth:200,
          background:'rgba(20,20,28,0.96)', border:'1px solid rgba(255,255,255,0.10)',
          borderRadius:12, padding:8, boxShadow:'0 30px 60px -24px rgba(0,0,0,0.7)',
          zIndex:50, backdropFilter:'blur(20px)',
        }}>
          <div style={{ ...itemStyle, opacity:0.7, cursor:'default' }} aria-current="page">
            <span style={chipStyle('linear-gradient(135deg,#FF4E3C,#E5243B,#9B1B30)')}>v2</span>
            <span>SORA v2</span>
            <span style={{ marginLeft:'auto', color:'#10B981' }}>•</span>
          </div>
          <a href="/minamoto" style={itemStyle}>
            <span style={chipStyle('linear-gradient(135deg,#7B2D5B,#7B5B90,#C8A0B8)')}>源</span>
            <span>Minamoto</span>
          </a>
          <a href="/" style={itemStyle}>
            <span style={chipStyle('#262634')}>↩</span>
            <span>Networks</span>
          </a>
        </div>
      )}
    </div>
  );
}

function PageHeader({ title, sub, children }) {
  return (
    <div className="page-header">
      <div>
        <h1 className="page-title">{title}</h1>
        {sub && <div className="page-sub">{sub}</div>}
      </div>
      <div className="page-actions">{children}</div>
    </div>
  );
}

Object.assign(window, { Sidebar, Topbar, PageHeader, NAV_GROUPS });
