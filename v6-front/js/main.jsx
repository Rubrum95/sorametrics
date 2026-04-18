/* global React, ReactDOM, Sidebar, Topbar, BurnSection, PulseSection, PortfolioSection, SwapsSection, ExtrinsicsSection, TransfersSection, BridgesSection, OrderBookSection, PoolsSection, TokensSection, HoldersSection, StakingSection, GovSection, BalanceSection, IntelligenceSection, TweaksPanel, Petals, DrillProvider, MusicPlayer, LangProvider, ToastProvider, WalletProvider, GlobalSearchProvider */
const { useState, useEffect, useCallback } = React;

const SECTION_COMPONENTS = {
  burns: 'BurnSection',
  pulse: 'PulseSection',
  portfolio: 'PortfolioSection',
  swaps: 'SwapsSection',
  extrinsics: 'ExtrinsicsSection',
  transfers: 'TransfersSection',
  bridges: 'BridgesSection',
  orderbook: 'OrderBookSection',
  pools: 'PoolsSection',
  tokens: 'TokensSection',
  holders: 'HoldersSection',
  staking: 'StakingSection',
  gov: 'GovSection',
  balance: 'BalanceSection',
  intel: 'IntelligenceSection',
};

function App() {
  const [tweaks, setTweaks] = useState(() => ({ ...(window.__TWEAKS__ || {}) }));
  // Deep-link section: read ?tab=X from the URL so shareable links land on
  // the right tab without manual sidebar clicks.
  const [section, setSection] = useState(() => {
    try {
      const params = new URLSearchParams(window.location.search);
      const tab = params.get('tab');
      if (tab && SECTION_COMPONENTS[tab]) return tab;
    } catch (_) {}
    return tweaks.section || 'burns';
  });
  const [block, setBlock] = useState(21_418_802);
  const [editOpen, setEditOpen] = useState(false);

  // Keep the URL in sync with the section without reloading the page.
  useEffect(() => {
    try {
      const url = new URL(window.location.href);
      if (url.searchParams.get('tab') !== section) {
        url.searchParams.set('tab', section);
        window.history.replaceState({}, '', url.toString());
      }
    } catch (_) {}
  }, [section]);

  useEffect(() => {
    document.documentElement.setAttribute('data-density', tweaks.density);
    document.documentElement.setAttribute('data-motion',  tweaks.motion);
    document.documentElement.setAttribute('data-accent',  tweaks.accent);
  }, [tweaks.density, tweaks.motion, tweaks.accent]);

  useEffect(() => {
    const id = setInterval(() => setBlock(b => b + 1), 6000 / (tweaks.liveSpeed || 1));
    return () => clearInterval(id);
  }, [tweaks.liveSpeed]);

  const setTweak = useCallback((k, v) => {
    setTweaks(prev => {
      const next = { ...prev, [k]: v };
      try {
        window.parent.postMessage({ type: '__edit_mode_set_keys', edits: { [k]: v } }, '*');
      } catch (_) {}
      if (k === 'section') setSection(v);
      return next;
    });
  }, []);

  useEffect(() => {
    const handler = (e) => {
      const m = e.data;
      if (!m || typeof m !== 'object') return;
      if (m.type === '__activate_edit_mode') setEditOpen(true);
      else if (m.type === '__deactivate_edit_mode') setEditOpen(false);
    };
    window.addEventListener('message', handler);
    try { window.parent.postMessage({ type: '__edit_mode_available' }, '*'); } catch (_) {}
    return () => window.removeEventListener('message', handler);
  }, []);

  const CompName = SECTION_COMPONENTS[section] || 'BurnSection';
  const Comp = window[CompName] || window.BurnSection;
  const content = <Comp tweaks={tweaks}/>;

  // expose a nav handle so the search palette can change section from anywhere
  useEffect(() => {
    window.__SM_NAV__ = (s) => { setSection(s); setTweak('section', s); };
  }, [setTweak]);

  return (
    <LangProvider>
    <ToastProvider>
    <WalletProvider>
    <DrillProvider>
    <GlobalSearchProvider>
      <div className="app">
        <Petals count={tweaks.motion === 'none' ? 0 : (tweaks.motion === 'subtle' ? 8 : 16)}/>
        <Sidebar section={section} setSection={(s) => { setSection(s); setTweak('section', s); }}/>
        <main className="main">
          <Topbar block={block}/>
          {content}
        </main>
        <TweaksPanel tweaks={tweaks} setTweak={setTweak} open={editOpen} onClose={() => setEditOpen(false)}/>
        <MusicPlayer/>
      </div>
    </GlobalSearchProvider>
    </DrillProvider>
    </WalletProvider>
    </ToastProvider>
    </LangProvider>
  );
}

ReactDOM.createRoot(document.getElementById('root')).render(<App/>);
