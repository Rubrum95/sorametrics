/* global window, React */
// ============================================================
// js/minamoto/shell.jsx
// Top-level layout: sidebar + main column. Reuses /styles.css
// classes from the SORA v2 SPA so the visual feels consistent.
// ============================================================

window.MN = window.MN || {};

(function (MN) {
  // Mirrors v2's NAV_GROUPS structure. v2 surfaces VERBS as top-level
  // sidebar items (swaps, transfers, bridges, burns, …) — events live
  // inside extrinsic detail. Minamoto follows the same pattern: verb-
  // specific sections (Transfers, Mints, Burns) at the top level, with
  // raw Instructions moved to Tools as a power-user view.
  const NAV_GROUPS = [
    { titleKey: 'nav.featured', items: [
      { id: 'pulse',        icon: '◉', key: 'nav.pulse' },
      { id: 'crosschain',   icon: '⇌', key: 'nav.crosschain' },
    ]},
    { titleKey: 'nav.my', items: [
      { id: 'wallet',       icon: '◔', key: 'nav.wallet' },
    ]},
    { titleKey: 'nav.network', items: [
      { id: 'transactions', icon: '↹', key: 'nav.transactions' },  // v2: Extrinsics
      { id: 'transfers',    icon: '↗', key: 'nav.transfers' },     // v2: Transfers
      { id: 'assets',       icon: '◆', key: 'nav.assets' },        // v2: Tokens
      { id: 'accounts',     icon: '◇', key: 'nav.accounts' },      // v2: Holders (per token, but listed globally here)
      { id: 'blocks',       icon: '⛓', key: 'nav.blocks' },        // v2 has no Blocks tab (visible in Pulse drilldown)
      { id: 'domains',      icon: '⬡', key: 'nav.domains' },       // Iroha 3 specific
      { id: 'peers',        icon: '◈', key: 'nav.peers' },         // v2: Staking validator topology
      { id: 'governance',   icon: '⚖', key: 'nav.governance' },    // v2: Governance
      { id: 'nfts',         icon: '◇', key: 'nav.nfts' },
      { id: 'rwas',         icon: '⏣', key: 'nav.rwas' },
      { id: 'kaigi',        icon: '⇄', key: 'nav.kaigi' },         // v2: Bridges
    ]},
    { titleKey: 'nav.toolsGroup', items: [
      { id: 'lanes',        icon: '≣', key: 'nav.lanes' },          // Iroha 3 internal lanes
      { id: 'permissions',  icon: '⚷', key: 'nav.permissions' },    // Grant ISIs (most-active kind on Minamoto)
      { id: 'instructions', icon: '✦', key: 'nav.instructions' },   // "All instructions" (= v2 has no events tab)
      { id: 'prometheus',   icon: '∿', key: 'nav.prometheus' },
    ]},
  ];

  function NetworkSwitcher({ t }) {
    const [open, setOpen] = React.useState(false);
    const ref = React.useRef(null);
    React.useEffect(() => {
      function onDoc(e) { if (ref.current && !ref.current.contains(e.target)) setOpen(false); }
      document.addEventListener('mousedown', onDoc);
      return () => document.removeEventListener('mousedown', onDoc);
    }, []);
    return (
      <div ref={ref} style={{ position: 'relative' }}>
        <button
          onClick={() => setOpen(o => !o)}
          aria-haspopup="true" aria-expanded={open}
          style={{
            display: 'inline-flex', alignItems: 'center', gap: 8,
            padding: '6px 12px 6px 8px', borderRadius: 999,
            background: 'rgba(123,91,144,0.20)', border: '1px solid rgba(160,98,176,0.34)',
            color: '#E0C8D5', fontSize: 12, fontWeight: 600, letterSpacing: '0.04em',
            cursor: 'pointer',
          }}>
          <span style={{
            width: 18, height: 18, borderRadius: '50%',
            background: 'linear-gradient(135deg,#7B2D5B,#7B5B90,#C8A0B8)',
            display: 'grid', placeItems: 'center', color: 'white',
            fontWeight: 900, fontSize: 10, letterSpacing: 0,
          }}>源</span>
          <span>{t('app.network.minamoto')}</span>
          <span style={{ opacity: 0.6 }}>▾</span>
        </button>
        {open && (
          <div role="menu" style={{
            position: 'absolute', top: 'calc(100% + 8px)', right: 0, minWidth: 200,
            background: 'rgba(20,20,28,0.96)', border: '1px solid rgba(255,255,255,0.10)',
            borderRadius: 12, padding: 8, boxShadow: '0 30px 60px -24px rgba(0,0,0,0.7)',
            zIndex: 50, backdropFilter: 'blur(20px)',
          }}>
            <a href="/sorav2" style={netItemStyle()}>
              <span style={{ ...iconChip(), background: 'linear-gradient(135deg,#FF4E3C,#E5243B,#9B1B30)' }}>v2</span>
              <span>{t('app.network.sorav2')}</span>
            </a>
            <div style={{ ...netItemStyle(), opacity: 0.7, cursor: 'default' }} aria-current="page">
              <span style={{ ...iconChip(), background: 'linear-gradient(135deg,#7B2D5B,#7B5B90,#C8A0B8)' }}>源</span>
              <span>{t('app.network.minamoto')}</span>
              <span style={{ marginLeft: 'auto', color: '#10B981' }}>•</span>
            </div>
            <a href="/" style={netItemStyle()}>
              <span style={{ ...iconChip(), background: '#262634' }}>↩</span>
              <span>{t('app.back')}</span>
            </a>
          </div>
        )}
      </div>
    );
  }

  function netItemStyle() {
    return {
      display: 'flex', alignItems: 'center', gap: 10, padding: '8px 10px',
      borderRadius: 8, color: '#fafafa', textDecoration: 'none', fontSize: 13,
      cursor: 'pointer',
    };
  }
  function iconChip() {
    return {
      width: 22, height: 22, borderRadius: 6,
      display: 'grid', placeItems: 'center', color: 'white',
      fontWeight: 900, fontSize: 11,
    };
  }

  function LangPicker() {
    const [, force] = React.useState(0);
    const cur = MN.i18n.lang();
    return (
      <div style={{ display: 'inline-flex', gap: 4 }}>
        {MN.i18n.available.map(l => (
          <button key={l} onClick={() => { MN.i18n.setLang(l); force(x => x + 1); }}
            style={{
              padding: '4px 8px', borderRadius: 6, fontSize: 11, fontWeight: 700, letterSpacing: '0.06em',
              background: l === cur ? 'rgba(160,98,176,0.30)' : 'transparent',
              color: l === cur ? '#fafafa' : '#9ca3af',
              border: '1px solid ' + (l === cur ? 'rgba(160,98,176,0.50)' : 'rgba(255,255,255,0.10)'),
              cursor: 'pointer', textTransform: 'uppercase',
            }}>{l}</button>
        ))}
      </div>
    );
  }

  function Sidebar({ section, onSelect }) {
    const t = MN.i18n.useT();
    return (
      <aside className="sidebar">
        <div className="brand">
          {/* Same brand block as the SORA v2 sidebar (same favicon + .brand-sora /
              .brand-metrics spans for the Sora-red + Metrics-white split). One
              identity across both networks; the network is signalled by tagline + accent. */}
          <img className="brand-logo" src="/favicon.svg" alt="SoraMetrics" width="28" height="28" />
          <div>
            <div className="brand-name">
              <span className="brand-sora">Sora</span><span className="brand-metrics">Metrics</span>
            </div>
            <div style={{ fontSize: 11, color: '#9ca3af', marginTop: 2 }}>{t('app.tagline')}</div>
          </div>
        </div>
        <nav className="nav">
          {NAV_GROUPS.map(g => (
            <React.Fragment key={g.titleKey}>
              <div style={{
                fontSize: 10, fontWeight: 700, letterSpacing: '0.18em', textTransform: 'uppercase',
                color: '#6b7280', padding: '14px 12px 6px',
              }}>{t(g.titleKey)}</div>
              {g.items.map(n => {
                const active = n.id === section;
                return (
                  <a key={n.id} href={'#' + n.id}
                     onClick={e => { e.preventDefault(); onSelect(n.id); }}
                     style={{
                       display: 'flex', alignItems: 'center', gap: 12,
                       padding: '10px 12px', borderRadius: 10, marginBottom: 2,
                       color: active ? '#fafafa' : '#9ca3af',
                       background: active ? 'rgba(160,98,176,0.16)' : 'transparent',
                       borderLeft: active ? '2px solid #A062B0' : '2px solid transparent',
                       fontSize: 13, fontWeight: active ? 600 : 500, textDecoration: 'none',
                       transition: 'background 200ms ease, color 200ms ease',
                     }}>
                    <span style={{
                      width: 22, height: 22, display: 'grid', placeItems: 'center',
                      fontSize: 14, color: active ? '#C8A0B8' : '#6b7280',
                    }}>{n.icon}</span>
                    <span>{t(n.key)}</span>
                  </a>
                );
              })}
            </React.Fragment>
          ))}
        </nav>
        <div className="sidebar-footer" style={{ paddingTop: 12, borderTop: '1px solid rgba(255,255,255,0.07)' }}>
          <a href="/" style={{ ...netItemStyle(), color: '#9ca3af', fontSize: 12 }}>
            <span style={{ ...iconChip(), background: '#262634', fontSize: 12 }}>↩</span>
            <span>{t('app.back')}</span>
          </a>
        </div>
      </aside>
    );
  }

  // Mobile hamburger toggle. Mirrors v2's pattern (see styles.css `.mobile-
  // hamburger`, `.drawer-backdrop`, `body.drawer-open .sidebar`). The CSS
  // classes already exist because Minamoto loads /styles.css; we just need
  // to render the button + backdrop and toggle the body class. Without
  // this, on ≤768px the sidebar is `display: none` per v2 CSS — leaving
  // mobile users with no navigation at all.
  function MobileHamburger() {
    const onClick = () => {
      document.body.classList.toggle('drawer-open');
    };
    return (
      <button className="mobile-hamburger" onClick={onClick} aria-label="Menu">
        <span /><span /><span />
      </button>
    );
  }

  function DrawerBackdrop() {
    const onClick = () => {
      document.body.classList.remove('drawer-open');
    };
    // Tap-anywhere-to-close. The CSS makes this overlay only visible while
    // body.drawer-open is set, but we render it always so React keeps the
    // listener attached.
    return <div className="drawer-backdrop" onClick={onClick} />;
  }

  function Topbar({ section }) {
    const t = MN.i18n.useT();
    return (
      <header className="mn-topbar" style={{
        display: 'flex', alignItems: 'center', justifyContent: 'space-between',
        padding: '18px 28px', borderBottom: '1px solid rgba(255,255,255,0.07)',
        background: 'linear-gradient(180deg, rgba(123,91,144,0.06), transparent)',
        gap: 8,
      }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: 12, minWidth: 0, flex: 1 }}>
          <MobileHamburger />
          {/* "MINAMOTO /" prefix is redundant on mobile because the network
              switcher pill on the right already shows the active network.
              Hide it on ≤640px via the .hide-mobile-tight helper to keep
              the section title readable. */}
          <div className="hide-mobile-tight" style={{ fontSize: 13, color: '#9ca3af', textTransform: 'uppercase', letterSpacing: '0.16em' }}>
            {t('app.network.minamoto')}
          </div>
          <span className="hide-mobile-tight" style={{ color: '#374151' }}>/</span>
          <div style={{ fontSize: 14, fontWeight: 700, color: '#fafafa', whiteSpace: 'nowrap', overflow: 'hidden', textOverflow: 'ellipsis', minWidth: 0 }}>{t('nav.' + section)}</div>
        </div>
        <div style={{ display: 'flex', alignItems: 'center', gap: 8, flexShrink: 0 }}>
          <LangPicker />
          <NetworkSwitcher t={t} />
        </div>
      </header>
    );
  }

  function Shell({ section, onSection, children }) {
    // Close the drawer whenever the user picks a section — otherwise the
    // drawer stays open after navigation on mobile. We wrap onSection so all
    // sidebar items pass through this.
    const handleSection = React.useCallback((s) => {
      document.body.classList.remove('drawer-open');
      onSection(s);
    }, [onSection]);
    return (
      <div className="app">
        <Sidebar section={section} onSelect={handleSection} />
        <DrawerBackdrop />
        <div className="main">
          <Topbar section={section} />
          <div style={{ padding: '24px 28px 48px' }}>{children}</div>
        </div>
      </div>
    );
  }

  MN.Shell = Shell;
})(window.MN);
