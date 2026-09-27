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
      { id: 'news',      key: 'nav.news',        icon: 'pulse' },
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
      { id: 'agents', key: 'nav.agents', icon: 'code' },
      { id: 'metrics', key: 'nav.metrics', icon: 'pulse' },
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
  const navRef = React.useRef(null);
  React.useEffect(() => {
    const el = navRef.current && navRef.current.querySelector('.nav-item.active');
    if (el) el.scrollIntoView({ block: 'nearest' });
  }, [section]);
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
            <div className="brand-name"><span className="brand-sora">Sora</span><span className="brand-metrics">{t('nav.metrics', 'Metrics')}</span></div>
          </div>
        </div>

        <div className="nav" ref={navRef}>
          {NAV_GROUPS.map(g => (
            <React.Fragment key={g.titleKey}>
              <div className="nav-section-title">{t(g.titleKey)}</div>
              {g.items.map(i => {
                const Icon = I[i.icon];
                const countLabel = i.countKey ? t(i.countKey) : i.count;
                return (
                  <a key={i.id}
                     href={'?tab=' + encodeURIComponent(i.id)}
                     className={'nav-item' + (section === i.id ? ' active' : '')}
                     aria-current={section === i.id ? 'page' : undefined}
                     onClick={e => {
                       // Modified clicks keep the browser's "open in new tab".
                       if (e.metaKey || e.ctrlKey || e.shiftKey || e.altKey || e.button !== 0) return;
                       e.preventDefault();
                       pickSection(i.id);
                     }}>
                    {Icon ? <Icon className="nav-icon"/> : <span className="nav-icon"/>}
                    <span className="nav-label">{t(i.key)}</span>
                    {countLabel && <span className="count">{countLabel}</span>}
                  </a>
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
      <div className="live-pill" style={{color: 'var(--err)', background: 'rgb(var(--err-rgb) / .12)', borderColor: 'rgb(var(--err-rgb) / .3)'}} title={t('s.wsDisconnected', 'WS disconnected')}>
        <span className="live-dot" style={{background:'var(--err)', boxShadow:'0 0 8px var(--err)'}}/> {t('s.disconnected', 'Disconnected')}
      </div>
    );
  }
  if (src.isLocal || src.isPrimary) {
    return (
      <div className="live-pill" title={src.active}>
        <span className="live-dot"/> {t('s.sorametricsNode', 'Sorametrics node ·')} {t('common.connected')}
      </div>
    );
  }
  // Fallback: rotated to a public node by WsProvider after the primary went down.
  return (
    <div className="live-pill" style={{color:'var(--warn)', background: 'rgb(var(--warn-rgb) / .12)', borderColor: 'rgb(var(--warn-rgb) / .3)'}} title={src.active}>
      <span className="live-dot" style={{background:'var(--warn)', boxShadow:'0 0 8px var(--warn)'}}/>
      {t('s.fallback', 'Fallback ·')} {src.label}
    </div>
  );
}

const THEME_NEXT = { auto: 'light', light: 'dark', dark: 'auto' };

function ThemeIcon({ mode }) {
  const common = { viewBox: '0 0 24 24', fill: 'none', stroke: 'currentColor', strokeWidth: 2, strokeLinecap: 'round', strokeLinejoin: 'round', 'aria-hidden': true };
  if (mode === 'light') {
    return (
      <svg {...common}>
        <circle cx="12" cy="12" r="4"/>
        <path d="M12 2.5v2M12 19.5v2M4.6 4.6l1.4 1.4M18 18l1.4 1.4M2.5 12h2M19.5 12h2M4.6 19.4L6 18M18 6l1.4-1.4"/>
      </svg>
    );
  }
  if (mode === 'dark') {
    return (
      <svg {...common}>
        <path d="M20.5 14.2A8.5 8.5 0 1 1 9.8 3.5a6.8 6.8 0 0 0 10.7 10.7z"/>
      </svg>
    );
  }
  return (
    <svg {...common}>
      <circle cx="12" cy="12" r="8.5"/>
      <path d="M12 3.5a8.5 8.5 0 0 1 0 17z" fill="currentColor" stroke="none"/>
    </svg>
  );
}

function ThemeToggle({ mode, onChange }) {
  const t = useT();
  const labels = { auto: t('theme.auto'), light: t('theme.light'), dark: t('theme.dark') };
  const current = THEME_NEXT[mode] ? mode : 'auto';
  const next = THEME_NEXT[current];
  const shown = current === 'auto' ? labels.auto + ' (' + t('theme.autoHint') + ')' : labels[current];
  const tip = t('theme.tip').replace('{mode}', shown).replace('{next}', labels[next]);
  return (
    <button type="button" className="theme-toggle" onClick={() => onChange(next)} title={tip} aria-label={tip}>
      <ThemeIcon mode={current}/>
      <span className="theme-label">{labels[current]}</span>
    </button>
  );
}

function Topbar({ block, themeMode, onThemeMode }) {

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
        aria-label={t('s.openNavigationMenu', 'Open navigation menu')}
        title={t('s.menu', 'Menú')}>
        <span/><span/><span/>
      </button>
      <div className="search" onClick={() => search.open()} role="button" tabIndex={0}
           aria-label={t('common.search')}
           onKeyDown={(e) => { if (e.key === 'Enter' || e.key === ' ') search.open(); }}>
        <I.search style={{width:14,height:14}}/>
        <span className="search-label">{t('common.search')}</span>
        <kbd>⌘K</kbd>
      </div>
      <div className="block-chip hide-mobile">
        <span className="label">{t('topbar.block')}</span>
        <span className="val">#{block.toLocaleString()}</span>
      </div>
      <div className="block-chip hide-mobile" title={staking ? t('s.eraProgress', 'Era progress') + ' ' + (staking.eraProgress || 0) + '%' : ''}>
        <span className="label">{t('topbar.eraEpoch')}</span>
        <span className="val">{eraLabel}</span>
      </div>
      <a className="block-chip agents-chip" href="?tab=agents" title={t('agents.chip.tip', 'Ask SoraMetrics from Claude, ChatGPT or your code')}
         onClick={(e) => { if (window.__SM_NAV__) { e.preventDefault(); window.__SM_NAV__('agents'); } }}>
        <I.code style={{width:13,height:13}}/>
        <span className="val hide-mobile">{t('agents.chip', 'Agents')}</span>
      </a>
      <LangPicker/>
      {onThemeMode && <ThemeToggle mode={themeMode} onChange={onThemeMode}/>}
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
  const t = useT();
  const [open, setOpen] = React.useState(false);
  const ref = React.useRef(null);
  React.useEffect(() => {
    function onDoc(e) { if (ref.current && !ref.current.contains(e.target)) setOpen(false); }
    document.addEventListener('mousedown', onDoc);
    return () => document.removeEventListener('mousedown', onDoc);
  }, []);
  const itemStyle = { display:'flex', alignItems:'center', gap:10, padding:'8px 10px',
                      borderRadius:8, color:'var(--fg-0)', textDecoration:'none', fontSize:13, cursor:'pointer' };
  const chipStyle = (grad, fg = 'var(--on-accent)') => ({ width:22, height:22, borderRadius:6, display:'grid', placeItems:'center',
                                 color:fg, fontWeight:900, fontSize:11, background:grad });
  return (
    <div ref={ref} style={{ position:'relative' }}>
      <button className="net-switch" onClick={() => setOpen(o => !o)} aria-haspopup="true" aria-expanded={open}
        style={{
          display:'inline-flex', alignItems:'center', gap:8,
          padding:'6px 12px 6px 8px', borderRadius:999,
          color:'var(--fg-0)', fontSize:12, fontWeight:600, letterSpacing:'0.04em',
          cursor:'pointer',
        }}>
        <span style={chipStyle('var(--grad-cta)')}>v2</span>
        <span className="net-label">SORA v2</span>
        <span style={{ opacity:0.6 }}>▾</span>
      </button>
      {open && (
        <div role="menu" style={{
          position:'absolute', top:'calc(100% + 8px)', right:0, minWidth:200,
          background:'rgb(var(--glass-rgb) / .94)', border:'1px solid var(--border)',
          borderRadius:12, padding:8, boxShadow:'0 30px 60px -24px rgb(var(--shade-rgb) / calc(.7 * var(--shade-k)))',
          zIndex:50, backdropFilter:'blur(20px)', WebkitBackdropFilter:'blur(20px)',
        }}>
          <div style={{ ...itemStyle, opacity:0.7, cursor:'default' }} aria-current="page">
            <span style={chipStyle('var(--grad-cta)')}>v2</span>
            <span>SORA v2</span>
            <span style={{ marginLeft:'auto', color:'var(--ok)' }}>•</span>
          </div>
          <a href="/minamoto" style={itemStyle}>
            <span style={chipStyle('var(--grad-avatar)', 'var(--fg-0)')}>源</span>
            <span>Minamoto</span>
          </a>
          <a href="/" style={itemStyle}>
            <span style={chipStyle('var(--bg-4)', 'var(--fg-0)')}>↩</span>
            <span>{t('s.networks', 'Networks')}</span>
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
