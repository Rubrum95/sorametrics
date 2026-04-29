/* global window, React */
// ============================================================
// js/minamoto/routes.jsx
// Maps section id → component. Persists current section in
// the URL hash so a refresh keeps the user where they were.
// ============================================================

window.MN = window.MN || {};

(function (MN) {
  const VALID = new Set(['pulse', 'blocks', 'transactions', 'instructions', 'transfers', 'accounts', 'domains', 'assets', 'peers', 'lanes', 'governance', 'nfts', 'rwas', 'kaigi', 'crosschain', 'wallet', 'prometheus', 'permissions']);

  function readHash() {
    const h = (window.location.hash || '').replace(/^#/, '');
    // Backwards-compat: legacy #overview links keep working as pulse.
    if (h === 'overview') return 'pulse';
    // Sub-page deep links: id read directly from the hash by each section's
    // own router (wallet.jsx, blocks.jsx, transactions.jsx, assets.jsx).
    if (h.startsWith('wallet')) return 'wallet';
    if (h.startsWith('block/')) return 'blocks';
    if (h.startsWith('tx/'))    return 'transactions';
    if (h.startsWith('asset/')) return 'assets';
    return VALID.has(h) ? h : 'pulse';
  }

  function Router() {
    const [section, setSection] = React.useState(readHash);

    React.useEffect(() => {
      function onHash() { setSection(readHash()); }
      window.addEventListener('hashchange', onHash);
      return () => window.removeEventListener('hashchange', onHash);
    }, []);

    const onSection = React.useCallback((s) => {
      if (!VALID.has(s)) return;
      window.location.hash = s;
      setSection(s);
    }, []);

    let body;
    switch (section) {
      case 'blocks':       body = <MN.Blocks />; break;
      case 'transactions': body = <MN.Transactions />; break;
      case 'instructions': body = <MN.Instructions />; break;
      case 'transfers':    body = <MN.Transfers />; break;
      case 'accounts':     body = <MN.Accounts />; break;
      case 'domains':      body = <MN.Domains />; break;
      case 'assets':       body = <MN.Assets />; break;
      case 'peers':        body = <MN.Peers />; break;
      case 'lanes':        body = <MN.Lanes />; break;
      case 'governance':   body = <MN.Governance />; break;
      case 'nfts':         body = <MN.Nfts />; break;
      case 'rwas':         body = <MN.Rwas />; break;
      case 'kaigi':        body = <MN.Kaigi />; break;
      case 'crosschain':   body = <MN.CrossChain />; break;
      case 'wallet':       body = <MN.Wallet />; break;
      case 'prometheus':   body = <MN.Prometheus />; break;
      case 'permissions':  body = <MN.Permissions />; break;
      case 'pulse':
      default:             body = <MN.Overview />; break;
    }

    return <MN.Shell section={section} onSection={onSection}>{body}</MN.Shell>;
  }

  MN.Router = Router;
})(window.MN);
