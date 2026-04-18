/* global React, useT, LangPicker, useSearch */
const { useState, useEffect } = React;

// i18n-keyed nav definition. Labels are resolved at render via t().
const NAV_GROUPS = [
  {
    titleKey: 'nav.featured',
    items: [
      { id: 'burns',     key: 'nav.burnTracker', icon: 'burn',   count: '5T' },
      { id: 'pulse',     key: 'nav.pulse',       icon: 'pulse',  count: 'LIVE', countKey: 'common.live' },
      { id: 'intel',     key: 'nav.intelligence',icon: 'bolt',   count: '3' },
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
    ],
  },
  {
    titleKey: 'nav.my',
    items: [
      { id: 'portfolio', key: 'nav.portfolio', icon: 'wallet', count: '4W' },
      { id: 'balance',   key: 'nav.balance',   icon: 'coins' },
    ],
  },
];

function Sidebar({ section, setSection }) {
  const t = useT();
  return (
    <aside className="sidebar">
      <div className="brand">
        <svg className="brand-logo" viewBox="0 0 32 32" width="28" height="28" aria-hidden="true">
          <defs>
            <radialGradient id="hexGrad" cx="50%" cy="42%" r="60%">
              <stop offset="0%"  stopColor="#9B1B30"/>
              <stop offset="100%" stopColor="#7B2D5B"/>
            </radialGradient>
          </defs>
          <polygon points="16,2 28.124,9 28.124,23 16,30 3.876,23 3.876,9" fill="url(#hexGrad)"/>
          <polygon points="16,5.2 25.35,10.6 25.35,21.4 16,26.8 6.65,21.4 6.65,10.6"
                   fill="none" stroke="#9B1B30" strokeWidth="1.5" opacity="0.85"/>
        </svg>
        <div>
          <div className="brand-name"><span className="brand-sora">Sora</span><span className="brand-metrics">Metrics</span></div>
          <div className="brand-sub">v5.0 · Prototype</div>
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
                     onClick={() => setSection(i.id)}>
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
        <div className="live-pill"><span className="live-dot"/> ws.mof.sora · {t('common.connected')}</div>
      </div>
    </aside>
  );
}

function Topbar({ block }) {
  const t = useT();
  const search = useSearch();
  return (
    <div className="topbar">
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
      <div className="block-chip hide-mobile">
        <span className="label">{t('topbar.eraEpoch')}</span>
        <span className="val">2408 · 14/6</span>
      </div>
      <LangPicker/>
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
