/* global React, ReactDOM, Sidebar, Topbar, BurnSection, PulseSection, PortfolioSection, SwapsSection, ExtrinsicsSection, TransfersSection, BridgesSection, OrderBookSection, PoolsSection, TokensSection, HoldersSection, StakingSection, GovSection, BalanceSection, IntelligenceSection, PolkamarktSection, ToolsSection, MusicStudioSection, XorMigrationSection, StudioProvider, TweaksPanel, Petals, DrillProvider, LangProvider, ToastProvider, WalletProvider, WalletDetailsProvider, GlobalSearchProvider */
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
  polkamarkt: 'PolkamarktSection',
  tools: 'ToolsSection',
  studio: 'MusicStudioSection',
  xormig: 'XorMigrationSection',
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
  // Topbar block number comes from the live tip via /staking/recent-blocks.
  // Previously this was a hardcoded 21_418_802 + a fake setInterval that just
  // did `block + 1` every 6s — which drifted several million blocks behind
  // the real chain. Now we fetch the tip at mount and then every 6s.
  const [block, setBlock] = useState(0);
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

  // Theme toggle — dark | light | auto. "auto" follows prefers-color-scheme.
  useEffect(() => {
    const apply = () => {
      let theme = tweaks.theme || 'dark';
      if (theme === 'auto') {
        theme = window.matchMedia && window.matchMedia('(prefers-color-scheme: light)').matches ? 'light' : 'dark';
      }
      document.documentElement.setAttribute('data-theme', theme);
    };
    apply();
    if (tweaks.theme === 'auto' && window.matchMedia) {
      const mq = window.matchMedia('(prefers-color-scheme: light)');
      mq.addEventListener?.('change', apply);
      return () => mq.removeEventListener?.('change', apply);
    }
  }, [tweaks.theme]);

  // Peg-watcher — poll /stats/stablecoins every 60s and fire a window-level
  // CustomEvent("peg-alert") when any stablecoin deviates beyond the
  // user-set threshold. Consumers (toast stack) listen.
  // Disabled by default — opt-in via Tweaks panel (tweaks.pegAlerts = true).
  useEffect(() => {
    if (!tweaks.pegAlerts) return;
    let cancelled = false;
    const lastFired = {}; // sym → ts of last toast (debounce)
    const check = async () => {
      try {
        const r = await fetch('/stats/stablecoins');
        if (!r.ok) return;
        const stables = await r.json();
        if (cancelled || !Array.isArray(stables)) return;
        const threshold = Number(tweaks.pegThreshold) || 2;
        for (const sc of stables) {
          const dev = Math.abs((Number(sc.price) || 0) - 1) * 100;
          if (dev > threshold) {
            const now = Date.now();
            if (!lastFired[sc.symbol] || now - lastFired[sc.symbol] > 5 * 60 * 1000) {
              lastFired[sc.symbol] = now;
              window.dispatchEvent(new CustomEvent('peg-alert', { detail: {
                symbol: sc.symbol, price: sc.price, dev,
              }}));
            }
          }
        }
      } catch {}
    };
    check();
    const id = setInterval(check, 60_000);
    return () => { cancelled = true; clearInterval(id); };
  }, [tweaks.pegThreshold, tweaks.pegAlerts]);

  useEffect(() => {
    let cancelled = false;
    const pull = () => fetch('/staking/recent-blocks?limit=1')
      .then(r => r.ok ? r.json() : null)
      .then(j => {
        if (cancelled) return;
        const tip = Array.isArray(j?.blocks) ? j.blocks[0] : Array.isArray(j) ? j[0] : j?.data?.[0];
        const n = tip && Number(tip.number);
        if (Number.isFinite(n) && n > 0) setBlock(n);
      })
      .catch(() => {});
    pull();
    // SORA block time is 6s — poll that cadence. liveSpeed > 1 just shortens
    // the visual refresh; the chain itself keeps 6s regardless.
    const id = setInterval(pull, Math.max(2000, 6000 / (tweaks.liveSpeed || 1)));
    return () => { cancelled = true; clearInterval(id); };
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

  const navTo = (s) => { setSection(s); setTweak('section', s); };

  return (
    <LangProvider>
    <ToastProvider>
    <WalletProvider>
    <WalletDetailsProvider>
    <DrillProvider>
    <GlobalSearchProvider>
    {/* StudioProvider hosts the persistent <audio> so playback survives
        section changes and can show a mini-player when the user leaves. */}
    <StudioProvider section={section} setSection={navTo}>
      <div className="app">
        <Petals count={tweaks.motion === 'none' ? 0 : (tweaks.motion === 'subtle' ? 8 : 16)}/>
        <Sidebar section={section} setSection={navTo}/>
        <main className="main">
          <Topbar block={block}/>
          {content}
        </main>
        <TweaksPanel tweaks={tweaks} setTweak={setTweak} open={editOpen} onClose={() => setEditOpen(false)}/>
      </div>
    </StudioProvider>
    </GlobalSearchProvider>
    </DrillProvider>
    </WalletDetailsProvider>
    </WalletProvider>
    </ToastProvider>
    </LangProvider>
  );
}

ReactDOM.createRoot(document.getElementById('root')).render(<App/>);

// Register the service worker once the app is rendered. Only in production
// (non-localhost) — the SW caches JSX which would hurt live iteration.
if ('serviceWorker' in navigator && !location.hostname.includes('localhost')) {
  window.addEventListener('load', () => {
    navigator.serviceWorker.register('sw.js').catch(() => {});
  });
}
