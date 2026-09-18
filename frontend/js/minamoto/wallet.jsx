/* global window, React */
// ============================================================
// js/minamoto/wallet.jsx
// Wallet drilldown: I105 account → balances, permissions, tx
// history, multisig info. Hash-driven (#wallet/<id>) so any
// account link in another section can deep-link here.
// ============================================================

window.MN = window.MN || {};

(function (MN) {
  // -----------------------------------------------------------
  // Asset-definitions cache: hash → { name, alias, confidential_mode }
  // Loaded once, refreshed every 5 min. Used to render balances
  // with human names instead of opaque ids.
  // -----------------------------------------------------------
  const _defCache = { ts: 0, byId: {} };
  async function getAssetDefs() {
    const fresh = (Date.now() - _defCache.ts) < 5 * 60_000;
    if (fresh && Object.keys(_defCache.byId).length) return _defCache.byId;
    const r = await MN.api('/asset-definitions');
    const map = {};
    for (const d of (r.items || [])) map[d.id] = d;
    _defCache.byId = map;
    _defCache.ts = Date.now();
    return map;
  }

  // -----------------------------------------------------------
  // Read account id from hash. URL shape: #wallet/<id>
  // The id may contain ":" / "@" / katakana — we only split on the FIRST "/"
  // so the id keeps its raw form.
  // -----------------------------------------------------------
  function readWalletId() {
    const h = (window.location.hash || '').replace(/^#/, '');
    if (!h.startsWith('wallet')) return null;
    const slash = h.indexOf('/');
    if (slash === -1) return '';
    return decodeURIComponent(h.slice(slash + 1));
  }

  // -----------------------------------------------------------
  // Portfolio storage — mirrors v2's `sm.wallets` pattern but for
  // Minamoto. Each entry: { id, alias, addr, addedAt }.
  // Persisted in localStorage so the user's named wallets survive
  // refreshes / browser sessions. No private keys are ever stored.
  // -----------------------------------------------------------
  const WALLETS_KEY = 'mn.wallets';

  function loadWallets() {
    try {
      const raw = localStorage.getItem(WALLETS_KEY);
      if (!raw) return [];
      const arr = JSON.parse(raw);
      return Array.isArray(arr) ? arr : [];
    } catch (_) { return []; }
  }
  function saveWallets(list) {
    try { localStorage.setItem(WALLETS_KEY, JSON.stringify(list)); } catch (_) {}
  }
  function addWallet(addr, alias) {
    const list = loadWallets();
    if (list.some(w => w.addr === addr)) return list;
    const id = (typeof crypto !== 'undefined' && crypto.randomUUID) ? crypto.randomUUID() : 'w_' + Date.now();
    const next = [...list, { id, alias: alias || '', addr, addedAt: new Date().toISOString() }];
    saveWallets(next);
    return next;
  }
  function removeWallet(addr) {
    const next = loadWallets().filter(w => w.addr !== addr);
    saveWallets(next);
    return next;
  }
  function renameWallet(addr, alias) {
    const next = loadWallets().map(w => w.addr === addr ? { ...w, alias } : w);
    saveWallets(next);
    return next;
  }

  // -----------------------------------------------------------
  // Per-wallet card showing alias, address, balances and a quick
  // open-drilldown action. Data fetched per card; cheap because
  // Minamoto returns small payloads.
  // -----------------------------------------------------------
  function PortfolioCard({ w, defs, onRemove, onRename }) {
    const t = MN.i18n.useT();
    const enc = encodeURIComponent(w.addr);
    const balances = MN.useFetch(`/accounts/${enc}/assets`, 60_000);
    const items = balances.data && balances.data.items ? balances.data.items : [];
    const [editing, setEditing] = React.useState(false);
    const [aliasDraft, setAliasDraft] = React.useState(w.alias || '');

    // Torii returns { asset, asset_alias, asset_name, quantity }; older
    // proxies returned { definition_id, value }. Accept both.
    const xorBal = items.find(b => {
      const alias = b.asset_alias || (defs[b.asset || b.definition_id] || {}).alias;
      const name  = b.asset_name  || (defs[b.asset || b.definition_id] || {}).name;
      return alias === 'xor#universal' || (name || '').toLowerCase() === 'xor';
    });
    const xorQty = xorBal ? (xorBal.quantity != null ? xorBal.quantity : xorBal.value) : null;

    function saveAlias() {
      onRename(w.addr, aliasDraft.trim());
      setEditing(false);
    }

    return (
      <div style={{
        position: 'relative', padding: '18px 20px', borderRadius: 14,
        background: 'rgba(20,20,28,0.72)', border: '1px solid rgba(255,255,255,0.07)',
        backdropFilter: 'blur(14px)',
      }}>
        <div style={{ display: 'flex', alignItems: 'baseline', justifyContent: 'space-between', gap: 8, marginBottom: 8 }}>
          <div style={{ minWidth: 0, flex: 1 }}>
            {editing ? (
              <div style={{ display: 'flex', gap: 6 }}>
                <input value={aliasDraft} onChange={e => setAliasDraft(e.target.value)}
                  onKeyDown={e => { if (e.key === 'Enter') saveAlias(); if (e.key === 'Escape') setEditing(false); }}
                  autoFocus
                  style={{ flex: 1, padding: '4px 8px', fontSize: 14, fontWeight: 700,
                           background: 'rgba(255,255,255,0.04)', color: '#fafafa',
                           border: '1px solid rgba(160,98,176,0.40)', borderRadius: 6, outline: 'none' }} />
                <button onClick={saveAlias} style={pillBtnStyle()}>✓</button>
                <button onClick={() => setEditing(false)} style={pillBtnStyle()}>✕</button>
              </div>
            ) : (
              <div onClick={() => { setAliasDraft(w.alias || ''); setEditing(true); }}
                   style={{ color: '#fafafa', fontSize: 14, fontWeight: 700, cursor: 'pointer', letterSpacing: '-0.01em' }}
                   title={t('wallet.renameHint')}>
                {w.alias || <span style={{ color: '#6b7280', fontStyle: 'italic' }}>{t('wallet.unnamed')}</span>}
              </div>
            )}
            <a href={MN.walletHref(w.addr)} className="mono" title={w.addr}
               style={{ display: 'inline-block', marginTop: 4, color: '#C8A0B8', fontSize: 11, textDecoration: 'none', borderBottom: '1px dotted rgba(200,160,184,0.40)' }}>
              {MN.shortAccount(w.addr, 12, 8)}
            </a>
          </div>
          <button onClick={() => { if (confirm(t('wallet.confirmRemove'))) onRemove(w.addr); }}
                  style={pillBtnStyle()}
                  title={t('wallet.remove')}>✕</button>
        </div>

        {/* Asset summary */}
        <div style={{ marginTop: 10, paddingTop: 10, borderTop: '1px solid rgba(255,255,255,0.06)' }}>
          {balances.loading && items.length === 0 && <div style={{ color: '#6b7280', fontSize: 11 }}>{t('common.loading')}</div>}
          {!balances.loading && items.length === 0 && <div style={{ color: '#6b7280', fontSize: 11 }}>{t('wallet.noAssets')}</div>}
          {items.length > 0 && (
            <div style={{ display: 'flex', flexDirection: 'column', gap: 6 }}>
              {xorBal && (
                <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'baseline' }}>
                  <span style={{ color: '#A062B0', fontSize: 11, fontWeight: 700 }}>XOR</span>
                  <span className="num" style={{ color: 'var(--fg-0)', fontSize: 14, fontWeight: 700 }}>{xorQty}</span>
                </div>
              )}
              <div style={{ color: '#9ca3af', fontSize: 11 }}>
                {items.length} asset{items.length === 1 ? '' : 's'} held
              </div>
            </div>
          )}
        </div>

        <a href={MN.walletHref(w.addr)} style={{
          display: 'block', textAlign: 'center', marginTop: 12,
          padding: '8px 12px', borderRadius: 8, fontSize: 12, fontWeight: 600,
          background: 'rgba(160,98,176,0.16)', color: '#E0C8D5',
          border: '1px solid rgba(160,98,176,0.30)', textDecoration: 'none',
        }}>{t('wallet.openDetail')} →</a>
      </div>
    );
  }

  // -----------------------------------------------------------
  // Portfolio top-level (when no #wallet/<id> selected)
  // -----------------------------------------------------------
  function Portfolio({ onPick }) {
    const t = MN.i18n.useT();
    const [wallets, setWallets] = React.useState(loadWallets);
    const [defs, setDefs] = React.useState({});
    React.useEffect(() => { getAssetDefs().then(setDefs).catch(() => {}); }, []);

    // Add-wallet form
    const [addAddr, setAddAddr] = React.useState('');
    const [addAlias, setAddAlias] = React.useState('');

    function submitAdd() {
      const a = addAddr.trim();
      if (!a) return;
      setWallets(addWallet(a, addAlias.trim()));
      setAddAddr(''); setAddAlias('');
    }
    function onRemove(addr) { setWallets(removeWallet(addr)); }
    function onRename(addr, alias) { setWallets(renameWallet(addr, alias)); }

    // Backup / restore
    function exportJson() {
      const blob = new Blob([JSON.stringify({ network: 'minamoto', exported_at: new Date().toISOString(), wallets }, null, 2)], { type: 'application/json' });
      const url = URL.createObjectURL(blob);
      const a = document.createElement('a');
      a.href = url; a.download = `mn-wallets-${Date.now()}.json`;
      document.body.appendChild(a); a.click();
      document.body.removeChild(a); URL.revokeObjectURL(url);
    }
    function importJson(ev) {
      const f = ev.target.files && ev.target.files[0];
      if (!f) return;
      const r = new FileReader();
      r.onload = () => {
        try {
          const parsed = JSON.parse(r.result);
          const incoming = Array.isArray(parsed) ? parsed : (parsed.wallets || []);
          let cur = loadWallets();
          for (const w of incoming) {
            if (w && w.addr && !cur.some(x => x.addr === w.addr)) {
              cur = [...cur, { id: w.id || ('w_' + Date.now() + '_' + Math.random()), alias: w.alias || '', addr: w.addr, addedAt: w.addedAt || new Date().toISOString() }];
            }
          }
          saveWallets(cur);
          setWallets(cur);
        } catch (e) {
          alert('Invalid JSON: ' + e.message);
        }
      };
      r.readAsText(f);
      ev.target.value = '';
    }

    return (
      <section className="section">
        <div className="page-header">
          <div>
            <h1 className="page-title">{t('wallet.portfolio')}</h1>
            <div className="page-sub">{t('wallet.portfolioSub')}</div>
          </div>
        </div>

        {/* Add wallet form */}
        <div style={{
          padding: '16px 18px', borderRadius: 14, marginBottom: 18,
          background: 'rgba(20,20,28,0.72)', border: '1px solid rgba(255,255,255,0.07)',
        }}>
          <div style={{ display: 'flex', gap: 8, flexWrap: 'wrap' }}>
            <input
              value={addAddr} onChange={e => setAddAddr(e.target.value)}
              onKeyDown={e => { if (e.key === 'Enter') submitAdd(); }}
              placeholder="sora… (paste account ID)"
              spellCheck={false}
              style={{
                flex: '2 1 320px', padding: '10px 12px', borderRadius: 10,
                background: 'rgba(255,255,255,0.04)', color: '#fafafa', fontSize: 13,
                fontFamily: 'JetBrains Mono, monospace',
                border: '1px solid rgba(255,255,255,0.10)', outline: 'none',
              }} />
            <input
              value={addAlias} onChange={e => setAddAlias(e.target.value)}
              onKeyDown={e => { if (e.key === 'Enter') submitAdd(); }}
              placeholder={t('wallet.aliasPlaceholder')}
              style={{
                flex: '1 1 180px', padding: '10px 12px', borderRadius: 10,
                background: 'rgba(255,255,255,0.04)', color: '#fafafa', fontSize: 13,
                border: '1px solid rgba(255,255,255,0.10)', outline: 'none',
              }} />
            <button onClick={submitAdd}
              style={{
                padding: '10px 18px', borderRadius: 10, fontSize: 13, fontWeight: 700,
                background: 'linear-gradient(135deg,#7B2D5B,#7B5B90,#C8A0B8)',
                color: '#fff', border: 'none', cursor: 'pointer',
              }}>{t('wallet.addWallet')}</button>
          </div>
          <div style={{ display: 'flex', gap: 12, marginTop: 12, fontSize: 11, color: '#9ca3af' }}>
            <button onClick={exportJson} style={pillBtnStyle()}>{t('wallet.export')}</button>
            <label style={{ ...pillBtnStyle(), display: 'inline-flex', alignItems: 'center' }}>
              {t('wallet.import')}
              <input type="file" accept="application/json" onChange={importJson} style={{ display: 'none' }} />
            </label>
            <span style={{ marginLeft: 'auto', alignSelf: 'center' }}>
              {wallets.length} {t('wallet.savedCount')}
            </span>
          </div>
        </div>

        {/* Saved wallets */}
        {wallets.length === 0 ? (
          <div style={{
            padding: '40px 20px', textAlign: 'center',
            background: 'rgba(20,20,28,0.50)', border: '1px dashed rgba(255,255,255,0.10)',
            borderRadius: 14,
          }}>
            <div style={{ fontSize: 38, color: '#374151', marginBottom: 12 }}>◔</div>
            <div style={{ color: '#fafafa', fontSize: 14, fontWeight: 600, marginBottom: 6 }}>{t('wallet.emptyLabel')}</div>
            <div style={{ color: '#6b7280', fontSize: 12 }}>{t('wallet.emptyHint')}</div>
          </div>
        ) : (
          <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(320px, 1fr))', gap: 14 }}>
            {wallets.map(w => (
              <PortfolioCard key={w.addr} w={w} defs={defs} onRemove={onRemove} onRename={onRename} />
            ))}
          </div>
        )}

        {/* Recent network accounts as a quick-add helper */}
        <RecentNetworkAccounts onPick={onPick} savedAddrs={new Set(wallets.map(w => w.addr))} />
      </section>
    );
  }

  function RecentNetworkAccounts({ onPick, savedAddrs }) {
    const t = MN.i18n.useT();
    const { data } = MN.useFetch('/accounts?per_page=20', 60_000);
    const recent = (data && data.items ? data.items : []).filter(a => !savedAddrs.has(a.id)).slice(0, 8);
    if (recent.length === 0) return null;
    return (
      <div style={{ marginTop: 26 }}>
        <div style={{ color: '#6b7280', fontSize: 11, fontWeight: 700, letterSpacing: '0.10em', textTransform: 'uppercase', marginBottom: 8 }}>
          {t('wallet.recentNetwork')}
        </div>
        <div style={{ display: 'flex', flexDirection: 'column', gap: 4 }}>
          {recent.map(a => (
            <a key={a.id} href={MN.walletHref(a.id)}
               style={{
                 display: 'flex', justifyContent: 'space-between', alignItems: 'center',
                 padding: '8px 12px', borderRadius: 10, background: 'rgba(255,255,255,0.02)',
                 border: '1px solid rgba(255,255,255,0.06)',
                 color: '#e5e7eb', fontSize: 12, textDecoration: 'none',
               }}>
              <span className="mono" style={{ color: '#C8A0B8' }}>{MN.shortAccount(a.id, 14, 8)}</span>
              <span style={{ color: '#6b7280', fontSize: 11 }}>{MN.fmt.relative(a.last_seen_at)}</span>
            </a>
          ))}
        </div>
      </div>
    );
  }

  // -----------------------------------------------------------
  // Drilldown: balances / permissions / transactions
  // -----------------------------------------------------------
  function Drilldown({ id }) {
    const t = MN.i18n.useT();
    const [defs, setDefs] = React.useState({});
    React.useEffect(() => { getAssetDefs().then(setDefs).catch(() => {}); }, []);

    // Saved-state for the "save to portfolio" button. Re-checks on
    // mount so navigating to an already-saved wallet shows ✓ Saved.
    const [saved, setSaved] = React.useState(() => loadWallets().some(w => w.addr === id));

    const enc = encodeURIComponent(id);
    const acctList = MN.useFetch('/accounts?per_page=100', 0); // loaded once for metadata
    const acct = (acctList.data && acctList.data.items)
      ? acctList.data.items.find(a => a.id === id)
      : null;
    const balances = MN.useFetch(`/accounts/${enc}/assets`, 30_000);
    const perms    = MN.useFetch(`/accounts/${enc}/permissions`, 60_000);
    const txs      = MN.useFetch(`/accounts/${enc}/transactions`, 30_000);

    const balItems = balances.data && balances.data.items ? balances.data.items : [];
    const permItems = perms.data && perms.data.items ? perms.data.items : [];
    const txItems = txs.data && txs.data.items ? txs.data.items : [];

    function copyId() { MN.copy(id); }

    // Torii returns balances as { account_id, asset, asset_alias, asset_name,
    // quantity }. Older endpoints (and our other proxies) used { definition_id,
    // value, id }. Normalize at the boundary so the rendering below stays
    // simple regardless of which shape arrives.
    function normBalance(b) {
      const defId = b.asset || b.definition_id || (b.id || '').split('#')[0];
      const qty   = b.quantity != null ? b.quantity : b.value;
      const alias = b.asset_alias || (defs[defId] && defs[defId].alias) || null;
      const name  = b.asset_name  || (defs[defId] && defs[defId].name)  || null;
      const conf  = (defs[defId] && defs[defId].confidential_mode) || null;
      return { defId, qty, alias, name, conf };
    }
    const isXorAlias = (alias, name) => alias === 'xor#universal' || (name || '').toLowerCase() === 'xor';

    // Sort balances: XOR first, then by quantity desc.
    const sortedBalances = [...balItems].sort((a, b) => {
      const na = normBalance(a), nb = normBalance(b);
      const xa = isXorAlias(na.alias, na.name) ? 1 : 0;
      const xb = isXorAlias(nb.alias, nb.name) ? 1 : 0;
      if (xa !== xb) return xb - xa;
      const va = parseFloat(na.qty); const vb = parseFloat(nb.qty);
      return (Number.isFinite(vb) ? vb : 0) - (Number.isFinite(va) ? va : 0);
    });

    // Group permissions by name
    const permsByName = permItems.reduce((acc, p) => {
      if (!acc[p.name]) acc[p.name] = [];
      acc[p.name].push(p.payload || {});
      return acc;
    }, {});

    // Tabs — 1:1 visual map of the v2 WalletDetailsModal (avatar + alias +
    // short addr header → tab bar → per-tab content). Iroha 3 has no Swaps /
    // Bridges / Liquidity / Predictions yet, so we only show the tabs that
    // map to existing data; the rest will appear automatically once their
    // ISI kinds materialise on-chain.
    const TABS = [
      { id: 'assets',   key: 'wallet.tab.assets'   },
      { id: 'transfers',key: 'wallet.tab.transfers'},
      { id: 'extr',     key: 'wallet.tab.extrinsics'},
      { id: 'perms',    key: 'wallet.tab.permissions'},
      { id: 'info',     key: 'wallet.tab.info'     },
    ];
    const [tab, setTab] = React.useState('assets');

    const savedWallet = loadWallets().find(w => w.addr === id);
    const initialAlias = savedWallet ? savedWallet.alias : '';
    const [aliasDraft, setAliasDraft] = React.useState(initialAlias);
    const [aliasSavedTick, setAliasSavedTick] = React.useState(0);

    function commitAlias() {
      if (savedWallet) {
        renameWallet(id, aliasDraft.trim());
      } else {
        addWallet(id, aliasDraft.trim());
        setSaved(true);
      }
      setAliasSavedTick(x => x + 1);
    }
    function unsaveWallet() {
      removeWallet(id);
      setSaved(false);
      setAliasDraft('');
    }

    // Total qty as a sum of asset shares (Minamoto has no USD price feed yet,
    // so the % bar normalises by SUM of holdings — purely relative weight,
    // not a USD denomination like v2). When prices become available we swap
    // this for USD-weighted shares.
    const sortedNorm = sortedBalances.map(normBalance);
    const sumQty = sortedNorm.reduce((s, n) => s + (parseFloat(n.qty) || 0), 0);

    const avatarLetter = (initialAlias || (acct && acct.primary_alias_name) || 'W').slice(0, 1).toUpperCase();

    return (
      <section className="section">
        {/* Top card: avatar + alias + short address + back button */}
        <div className="card" style={{ padding: '20px 22px', marginBottom: 18 }}>
          <div style={{ display: 'flex', alignItems: 'center', gap: 14, flexWrap: 'wrap' }}>
            <div style={{
              width: 44, height: 44, borderRadius: 12,
              background: 'linear-gradient(135deg,#7B2D5B,#7B5B90)',
              display: 'grid', placeItems: 'center',
              color: 'white', fontWeight: 800, fontSize: 18, flexShrink: 0,
              boxShadow: '0 8px 20px -8px rgba(160,98,176,0.50)',
            }}>{avatarLetter}</div>
            <div style={{ minWidth: 0, flex: 1 }}>
              <h1 className="page-title" style={{ fontSize: 22, marginBottom: 2 }}>
                {initialAlias || (acct && acct.primary_alias_name) || t('wallet.unnamed')}
              </h1>
              <div style={{ display: 'flex', alignItems: 'center', gap: 8, flexWrap: 'wrap' }}>
                <span className="num muted" style={{ fontSize: 12 }} title={id}>{MN.shortAccount(id, 10, 6)}</span>
                <button className="btn" onClick={copyId} style={{ padding: '2px 10px', fontSize: 11 }}>⎘ {t('common.copy')}</button>
              </div>
            </div>
            <div style={{ display: 'flex', gap: 8, alignItems: 'center' }}>
              <button className="btn" onClick={() => { window.location.hash = 'wallet'; }}>← {t('wallet.changeWallet')}</button>
            </div>
          </div>

          {/* Tab bar */}
          <div style={{ display: 'flex', gap: 4, marginTop: 18, borderBottom: '1px solid var(--border)', flexWrap: 'wrap' }}>
            {TABS.map(tb => {
              const active = tab === tb.id;
              return (
                <button key={tb.id} onClick={() => setTab(tb.id)}
                  style={{
                    padding: '10px 14px', fontSize: 13, fontWeight: active ? 700 : 500,
                    background: 'transparent',
                    color: active ? 'var(--fg-0)' : 'var(--fg-2)',
                    border: 'none',
                    borderBottom: '2px solid ' + (active ? 'var(--accent)' : 'transparent'),
                    cursor: 'pointer', marginBottom: -1,
                  }}>
                  {t(tb.key)}
                </button>
              );
            })}
          </div>
        </div>

        {/* Tab content */}
        {tab === 'assets' && (
          <>
            {/* ALIAS · EN MIS WALLETS */}
            <div className="card" style={{ padding: '16px 18px', marginBottom: 14 }}>
              <div className="stat-label" style={{ marginBottom: 8 }}>{t('wallet.aliasLabel')}</div>
              <div style={{ display: 'flex', gap: 8 }}>
                <input value={aliasDraft} onChange={e => setAliasDraft(e.target.value)}
                  onKeyDown={e => { if (e.key === 'Enter') commitAlias(); }}
                  placeholder={t('wallet.aliasPlaceholder')}
                  style={{ flex: 1, padding: '8px 12px', borderRadius: 8, fontSize: 13,
                           background: 'rgba(255,255,255,0.04)', color: 'var(--fg-0)',
                           border: '1px solid var(--border)', outline: 'none' }} />
                <button className="btn" onClick={commitAlias}
                  style={{ background: 'var(--accent)', color: 'white', borderColor: 'var(--accent)', fontWeight: 700 }}>
                  {aliasSavedTick > 0 ? '✓ ' : ''}{t('wallet.save')}
                </button>
              </div>
            </div>

            {/* DIRECCIÓN */}
            <div className="card" style={{ padding: '16px 18px', marginBottom: 14 }}>
              <div className="stat-label" style={{ marginBottom: 8 }}>{t('wallet.address')}</div>
              <div style={{ display: 'flex', gap: 8, alignItems: 'center' }}>
                <span className="num" style={{ flex: 1, color: 'var(--fg-0)', fontSize: 13, wordBreak: 'break-all' }}>{id}</span>
                <button className="btn" onClick={copyId}>⎘ {t('common.copy')}</button>
              </div>
            </div>

            {/* DESGLOSE POR ACTIVO */}
            <div className="card" style={{ padding: 0, overflow: 'hidden' }}>
              <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'baseline',
                            padding: '14px 18px', borderBottom: '1px solid var(--border)' }}>
                <h3 style={{ margin: 0, color: 'var(--fg-0)', fontSize: 14, fontWeight: 700 }}>
                  {t('wallet.breakdown')} <span className="muted" style={{ fontSize: 12, marginLeft: 6 }}>· {balItems.length} {t('wallet.tokens')}</span>
                </h3>
              </div>
              {balances.error && <div style={{ padding: 14, color: 'var(--err)', fontSize: 12 }}>{balances.error.message}</div>}
              {balances.loading && balItems.length === 0 && <div style={{ padding: 24, textAlign: 'center', color: 'var(--fg-3)', fontSize: 12 }}>{t('common.loading')}</div>}
              {!balances.loading && balItems.length === 0 && <div style={{ padding: 24, textAlign: 'center', color: 'var(--fg-3)', fontSize: 12 }}>{t('wallet.noAssets')}</div>}
              {sortedNorm.map(n => {
                const xor = isXorAlias(n.alias, n.name);
                const qtyN = parseFloat(n.qty) || 0;
                const pct = sumQty > 0 ? (qtyN / sumQty) * 100 : 0;
                const proxy = { alias: n.alias, name: n.name, asset: n.defId };
                return (
                  <a key={n.defId} className="mn-row-asset" href={'#asset/' + encodeURIComponent(n.alias || n.name || n.defId)}
                     style={{ display: 'grid', gridTemplateColumns: '40px 110px 1fr 120px 60px',
                              alignItems: 'center', gap: 12,
                              padding: '12px 18px', borderBottom: '1px solid var(--border)',
                              textDecoration: 'none', color: 'inherit',
                              transition: 'background 200ms ease' }}
                     onMouseEnter={e => e.currentTarget.style.background = 'rgba(255,255,255,0.02)'}
                     onMouseLeave={e => e.currentTarget.style.background = 'transparent'}>
                    <MN.TokenIcon asset={proxy} size={28} />
                    <div style={{ minWidth: 0 }}>
                      <div style={{ color: xor ? 'var(--fg-0)' : 'var(--fg-1)', fontSize: 13, fontWeight: 700 }}>
                        {n.name || MN.shortHash(n.defId, 6, 4)}
                      </div>
                      {n.alias && <div className="num muted" style={{ fontSize: 10 }}>{n.alias}</div>}
                    </div>
                    <div style={{ height: 6, borderRadius: 999, background: 'rgba(255,255,255,0.06)', overflow: 'hidden' }}>
                      <div style={{
                        width: pct.toFixed(2) + '%', height: '100%',
                        background: xor ? 'linear-gradient(90deg,#FF4E3C,#E5243B)' : 'linear-gradient(90deg,#7B5B90,#A062B0)',
                      }}/>
                    </div>
                    <div className="num" style={{ color: 'var(--fg-0)', fontSize: 13, fontWeight: 700, textAlign: 'right' }}>{n.qty}</div>
                    <div className="num muted" style={{ fontSize: 11, textAlign: 'right' }}>{pct.toFixed(1)}%</div>
                  </a>
                );
              })}
            </div>
          </>
        )}

        {tab === 'transfers' && <WalletTransfers id={id} defs={defs} />}
        {tab === 'extr'      && <WalletExtrinsics id={id} />}

        {tab === 'perms' && (
          <WalletPermsPane id={id} permItems={permItems} permsByName={permsByName} loading={perms.loading} />
        )}

        {tab === 'info' && (
          <WalletInfoPane id={id} acct={acct} savedWallet={savedWallet} />
        )}

        {/* Bottom action row — mirrors v2's Eliminar / Seguir / Cerrar */}
        <div style={{ display: 'flex', gap: 8, marginTop: 18, justifyContent: 'space-between', flexWrap: 'wrap' }}>
          <div style={{ display: 'flex', gap: 8 }}>
            {saved && <button className="btn" onClick={unsaveWallet} style={{ color: '#FCA5A5', borderColor: 'rgba(239,68,68,0.30)' }}>{t('wallet.remove')}</button>}
            {!saved && <button className="btn" onClick={commitAlias}>★ {t('wallet.savePortfolio')}</button>}
          </div>
          <button className="btn" onClick={() => { window.location.hash = 'wallet'; }}>{t('common.close')}</button>
        </div>
      </section>
    );
  }

  function InfoRow({ k, v, sub, mono }) {
    return (
      <div>
        <div className="stat-label">{k}</div>
        <div className={mono ? 'num' : ''} style={{ color: 'var(--fg-0)', fontSize: 14, fontWeight: 600, marginTop: 4, wordBreak: 'break-all' }}>{v || '—'}</div>
        {sub && <div className="muted tiny" style={{ marginTop: 2 }}>{sub}</div>}
      </div>
    );
  }

  // Per-wallet Transfers tab — filters mn.instructions for kind=Transfer
  // where this wallet is sender or receiver. The list reuses the same row
  // pattern as the global Transfers section.
  function WalletTransfers({ id, defs }) {
    const t = MN.i18n.useT();
    const out = MN.useFetch(`/instructions?kind=Transfer&authority=${encodeURIComponent(id)}&per_page=100`, 30_000);
    const items = (out.data && out.data.items) || [];
    return (
      <div className="card" style={{ padding: 0, overflow: 'hidden' }}>
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'baseline',
                      padding: '14px 18px', borderBottom: '1px solid var(--border)' }}>
          <h3 style={{ margin: 0, color: 'var(--fg-0)', fontSize: 14, fontWeight: 700 }}>{t('wallet.tab.transfers')}</h3>
          <span className="tag">{items.length}</span>
        </div>
        {out.loading && items.length === 0 && <div style={{ padding: 24, textAlign: 'center', color: 'var(--fg-3)', fontSize: 12 }}>{t('common.loading')}</div>}
        {!out.loading && items.length === 0 && <div style={{ padding: 24, textAlign: 'center', color: 'var(--fg-3)', fontSize: 12 }}>{t('common.empty')}</div>}
        {items.map((i, idx) => {
          const v = i.payload && i.payload.value ? i.payload.value : {};
          const dest = String(v.destination || '');
          const src  = String(v.source || '');
          const defId = (src.split('#')[0]) || null;
          const def = defs[defId] || {};
          return (
            <a key={idx} className="mn-row-transfer" href={'#tx/' + i.transaction_hash}
               style={{ display: 'grid', gridTemplateColumns: '120px 1fr 100px 110px 60px',
                        alignItems: 'center', gap: 12, padding: '12px 18px',
                        borderBottom: '1px solid var(--border)',
                        textDecoration: 'none', color: 'inherit' }}>
              <span className="muted tiny">{MN.fmt.relative(i.created_at)}</span>
              <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
                <MN.TokenIcon asset={{ alias: def.alias, name: def.name, asset: defId }} size={22} />
                <span style={{ color: 'var(--fg-0)', fontSize: 13 }}>{def.name || MN.shortHash(defId, 6, 4)}</span>
              </div>
              <span className="num" style={{ color: 'var(--fg-0)', fontWeight: 600 }}>{v.object || '—'}</span>
              <span className="num muted" style={{ fontSize: 11 }} title={dest}>→ {MN.shortAccount(dest.split('#').pop() || dest, 6, 4)}</span>
              <span className="num" style={{ color: 'var(--accent)', fontSize: 11 }}>{MN.shortHash(i.transaction_hash, 5, 3)}</span>
            </a>
          );
        })}
      </div>
    );
  }

  function WalletExtrinsics({ id }) {
    const t = MN.i18n.useT();
    // Use mn.transactions (full Transactions table) filtered by authority.
    const out = MN.useFetch(`/transactions?authority=${encodeURIComponent(id)}&per_page=100`, 30_000);
    const items = (out.data && out.data.items) || [];
    return (
      <div className="card" style={{ padding: 0, overflow: 'hidden' }}>
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'baseline',
                      padding: '14px 18px', borderBottom: '1px solid var(--border)' }}>
          <h3 style={{ margin: 0, color: 'var(--fg-0)', fontSize: 14, fontWeight: 700 }}>{t('wallet.tab.extrinsics')}</h3>
          <span className="tag">{items.length}</span>
        </div>
        {out.loading && items.length === 0 && <div style={{ padding: 24, textAlign: 'center', color: 'var(--fg-3)', fontSize: 12 }}>{t('common.loading')}</div>}
        {!out.loading && items.length === 0 && <div style={{ padding: 24, textAlign: 'center', color: 'var(--fg-3)', fontSize: 12 }}>{t('common.empty')}</div>}
        {items.map((tx, idx) => (
          <a key={idx} className="mn-row-extrinsic" href={'#tx/' + tx.hash}
             style={{ display: 'grid', gridTemplateColumns: '120px 1fr 80px 100px 40px',
                      alignItems: 'center', gap: 12, padding: '12px 18px',
                      borderBottom: '1px solid var(--border)',
                      textDecoration: 'none', color: 'inherit' }}>
            <span className="muted tiny">{MN.fmt.relative(tx.created_at)}</span>
            <span className="num" style={{ color: 'var(--accent)', fontSize: 12 }}>{MN.shortHash(tx.hash, 8, 6)}</span>
            <span className="num muted" style={{ fontSize: 11 }}>#{tx.block}</span>
            <span className="muted tiny">{tx.executable}</span>
            {tx.status === 'Committed'
              ? <span className="status-pill ok">✓</span>
              : <span className="status-pill err">✗</span>}
          </a>
        ))}
      </div>
    );
  }

  // -----------------------------------------------------------
  // WalletPermsPane — Permissions tab content. Combines two sources:
  //   1. /accounts/{id}/permissions — LIVE permissions held now (Torii
  //      passthrough). Each entry: { name, payload }.
  //   2. /permissions/grants?destination={id} — historical Grant ISIs
  //      received by this wallet (with grantor + tx + block).
  // The UI groups by permission name so the operator sees BOTH the
  // current snapshot ("you hold X permission") AND the audit trail
  // ("granted by Y at block Z").
  // -----------------------------------------------------------
  const PERM_COLORS = {
    CanManageAccountAlias:           '#A062B0',
    CanResolveAccountAlias:          '#C8A0B8',
    CanRegisterTrigger:              '#F472B6',
    CanRegisterAccount:              '#60A5FA',
    CanRegisterDomain:               '#3B82F6',
    CanMintAssetWithDefinition:      '#10B981',
    CanUseFeeSponsor:                '#F59E0B',
    CanManageVerifyingKeys:          '#8B5CF6',
    CanEnactGovernance:              '#FB7185',
    CanSetParameters:                '#EF4444',
    CanPublishSpaceDirectoryManifest:'#22C55E',
    CanManageSoracloud:              '#06B6D4',
  };
  function permColor(name) { return PERM_COLORS[name] || '#9ca3af'; }

  function PermBadge({ name }) {
    const c = permColor(name);
    return (
      <span style={{
        display: 'inline-block', padding: '2px 8px', borderRadius: 999,
        fontSize: 11, fontWeight: 700, letterSpacing: '0.02em',
        color: c, background: c + '14', border: '1px solid ' + c + '40',
      }}>{name}</span>
    );
  }

  function permPayloadSummary(payload) {
    if (!payload || typeof payload !== 'object') return null;
    const sponsor = payload.sponsor;
    const def     = payload.asset_definition;
    if (sponsor) return { kind: 'sponsor', value: sponsor, label: 'sponsor: ' };
    if (def)     return { kind: 'asset',   value: def,     label: 'asset: ' };
    const blob = JSON.stringify(payload);
    return { kind: 'json', value: blob.length > 60 ? blob.slice(0, 60) + '…' : blob };
  }

  function WalletPermsPane({ id, permItems, permsByName, loading }) {
    const t = MN.i18n.useT();
    const enc = encodeURIComponent(id);
    const grants = MN.useFetch(`/permissions/grants?destination=${enc}&per_page=100`, 60_000);
    const grantItems = (grants.data && grants.data.items) || [];
    // Index grant history by permission name so we can show the audit trail
    // alongside the live permissions.
    const grantsByName = {};
    for (const g of grantItems) {
      const k = g.permission_name || 'unknown';
      if (!grantsByName[k]) grantsByName[k] = [];
      grantsByName[k].push(g);
    }
    const liveNames = Object.keys(permsByName);
    const grantOnlyNames = Object.keys(grantsByName).filter(n => !permsByName[n]);
    const hasGrants = grantItems.length > 0;

    return (
      <div style={{ display: 'flex', flexDirection: 'column', gap: 14 }}>
        {/* Header strip */}
        <div className="card" style={{ padding: '14px 18px', display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(150px, 1fr))', gap: 14 }}>
          <div>
            <div className="stat-label">{t('wallet.perms.live')}</div>
            <div className="num" style={{ color: 'var(--fg-0)', fontSize: 18, fontWeight: 700, marginTop: 4 }}>{permItems.length}</div>
            <div className="muted tiny" style={{ marginTop: 2 }}>{t('wallet.perms.liveSub')}</div>
          </div>
          <div>
            <div className="stat-label">{t('wallet.perms.granted')}</div>
            <div className="num" style={{ color: 'var(--fg-0)', fontSize: 18, fontWeight: 700, marginTop: 4 }}>{grantItems.length}</div>
            <div className="muted tiny" style={{ marginTop: 2 }}>{t('wallet.perms.grantedSub')}</div>
          </div>
          <div>
            <div className="stat-label">{t('wallet.perms.uniqueTypes')}</div>
            <div className="num" style={{ color: 'var(--fg-0)', fontSize: 18, fontWeight: 700, marginTop: 4 }}>{liveNames.length}</div>
            <div className="muted tiny" style={{ marginTop: 2 }}>{t('wallet.perms.uniqueTypesSub')}</div>
          </div>
        </div>

        {/* Live permissions per name with grant audit trail underneath */}
        <div className="card" style={{ padding: 0, overflow: 'hidden' }}>
          <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'baseline',
                        padding: '14px 18px', borderBottom: '1px solid var(--border)' }}>
            <h3 style={{ margin: 0, color: 'var(--fg-0)', fontSize: 14, fontWeight: 700 }}>{t('wallet.perms.activeTitle')}</h3>
            <span className="tag">{permItems.length}</span>
          </div>
          {loading && permItems.length === 0 && (
            <div style={{ padding: 24, textAlign: 'center', color: 'var(--fg-3)', fontSize: 12 }}>{t('common.loading')}</div>
          )}
          {!loading && liveNames.length === 0 && (
            <div style={{ padding: 24, textAlign: 'center', color: 'var(--fg-3)', fontSize: 12 }}>{t('wallet.perms.empty')}</div>
          )}
          {liveNames.map(name => {
            const payloads = permsByName[name];
            const grantHistory = grantsByName[name] || [];
            return (
              <div key={name} style={{ padding: '12px 18px', borderBottom: '1px solid var(--border)' }}>
                <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', flexWrap: 'wrap', gap: 8 }}>
                  <PermBadge name={name} />
                  <span className="muted tiny">
                    {payloads.length} {payloads.length === 1 ? t('wallet.perms.grant') : t('wallet.perms.grants')}
                  </span>
                </div>
                {payloads.length > 0 && (
                  <div style={{ marginTop: 6, display: 'flex', flexWrap: 'wrap', gap: 6 }}>
                    {payloads.slice(0, 6).map((p, i) => {
                      const summary = permPayloadSummary(p);
                      if (!summary) return null;
                      if (summary.kind === 'sponsor') {
                        return (
                          <a key={i} className="num" href={MN.walletHref(summary.value)} title={summary.value}
                             style={{ color: 'var(--accent)', fontSize: 11, textDecoration: 'none', borderBottom: '1px dotted rgba(255,255,255,0.20)' }}>
                            {summary.label}{MN.shortAccount(summary.value, 6, 4)}
                          </a>
                        );
                      }
                      if (summary.kind === 'asset') {
                        return (
                          <a key={i} className="num" href={'#asset/' + encodeURIComponent(summary.value)} title={summary.value}
                             style={{ color: 'var(--accent)', fontSize: 11, textDecoration: 'none', borderBottom: '1px dotted rgba(255,255,255,0.20)' }}>
                            {summary.label}{MN.shortHash(summary.value, 6, 4)}
                          </a>
                        );
                      }
                      return <span key={i} className="num muted" style={{ fontSize: 11 }}>{summary.value}</span>;
                    })}
                  </div>
                )}
                {grantHistory.length > 0 && (
                  <div className="muted tiny" style={{ marginTop: 6, paddingTop: 6, borderTop: '1px dashed var(--border)' }}>
                    {t('wallet.perms.grantedBy')}{' '}
                    {grantHistory.slice(0, 3).map((g, i) => (
                      <span key={i}>
                        {i > 0 && ', '}
                        <a className="num" href={MN.walletHref(g.authority)} title={g.authority}
                           style={{ color: 'var(--fg-1)', fontSize: 11 }}>
                          {MN.shortAccount(g.authority, 6, 4)}
                        </a>
                        {' '}
                        <a href={'#tx/' + g.transaction_hash}
                           style={{ color: 'var(--accent)', fontSize: 11, textDecoration: 'none' }}>
                          @#{g.block}
                        </a>
                      </span>
                    ))}
                    {grantHistory.length > 3 && <span> · +{grantHistory.length - 3} {t('common.more')}</span>}
                  </div>
                )}
              </div>
            );
          })}
        </div>

        {/* Permissions seen in grants but NOT currently held — typically
            indicates a Revoke happened; surface to operators for audit. */}
        {grantOnlyNames.length > 0 && (
          <div className="card" style={{ padding: 0, overflow: 'hidden' }}>
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'baseline',
                          padding: '14px 18px', borderBottom: '1px solid var(--border)' }}>
              <h3 style={{ margin: 0, color: 'var(--fg-2)', fontSize: 13, fontWeight: 700 }}>{t('wallet.perms.historyTitle')}</h3>
              <span className="tag">{grantOnlyNames.length}</span>
            </div>
            <div className="muted tiny" style={{ padding: '8px 18px 4px' }}>{t('wallet.perms.historyHint')}</div>
            {grantOnlyNames.map(name => (
              <div key={name} style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center',
                                        padding: '10px 18px', borderBottom: '1px solid var(--border)' }}>
                <PermBadge name={name} />
                <span className="muted tiny">{grantsByName[name].length} {t('wallet.perms.grants')}</span>
              </div>
            ))}
          </div>
        )}
      </div>
    );
  }

  // -----------------------------------------------------------
  // WalletInfoPane — mirrors v2's InfoPane:
  //   - ACTIVITY mini-grid (first/last tx, total txs + success rate, days
  //     active, tokens held)
  //   - IDENTITY (primary alias, multisig, first/last seen on-chain)
  //   - TOP INSTRUCTION KINDS (per-authority aggregate, top 10)
  //   - DOMAINS OWNED + ASSET DEFINITIONS CREATED (governance/builder roles)
  //   - CROSS-CHAIN BRIDGE (claims received + XOR migrated from v2)
  //   - LOCAL PORTFOLIO (saved alias, internal id)
  // Each section only renders when it has data, so retail wallets stay
  // compact while builder/multisig wallets get the full picture.
  // -----------------------------------------------------------
  function WalletInfoPane({ id, acct, savedWallet }) {
    const t = MN.i18n.useT();
    const enc = encodeURIComponent(id);
    const info = MN.useFetch(`/wallet/${enc}/info`, 60_000);
    const data = info.data || {};

    const successPct = data.success_rate != null ? (data.success_rate * 100).toFixed(1) + '%' : null;
    const kinds = data.instruction_kinds || [];
    const domains = data.domains_owned || [];
    const assetDefs = data.asset_defs_created || [];
    const cc = data.cross_chain || { claims_received: 0, xor_claimed: '0' };
    const incoming = data.incoming || { transfers: 0, mints: 0, total: 0 };
    const transfers = data.transfers || { out_count: 0, in_count: 0, volume_xor: '0', max_xor: '0', avg_xor: '0' };
    const topTokens = data.top_tokens || [];
    const whale = data.whale_score || { total: 0, class: 'SHRIMP', volume: 0, frequency: 0, diversity: 0 };
    const hasActivity = (data.tx_count || 0) > 0 || (incoming.total || 0) > 0;
    const hasMovements = (transfers.out_count || 0) > 0 || (transfers.in_count || 0) > 0 || (incoming.mints || 0) > 0;
    const hasCrossChain = (cc.claims_received || 0) > 0;

    return (
      <div style={{ display: 'flex', flexDirection: 'column', gap: 14 }}>
        {/* ACTIVITY */}
        <div className="card" style={{ padding: '16px 18px' }}>
          <div className="stat-label" style={{ marginBottom: 12 }}>{t('wallet.info.activity')}</div>
          {info.loading && !info.data && <div className="muted tiny">{t('common.loading')}</div>}
          {!info.loading && !hasActivity && <div className="muted tiny">{t('wallet.info.noActivity')}</div>}
          {hasActivity && (
            <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(160px, 1fr))', gap: 14 }}>
              <MiniStat label={t('wallet.info.firstTx')}
                value={data.first_at ? MN.fmt.relative(data.first_at) : '—'}
                sub={data.first_at} />
              <MiniStat label={t('wallet.info.lastTx')}
                value={data.last_at ? MN.fmt.relative(data.last_at) : '—'}
                sub={data.last_at} />
              <MiniStat label={t('wallet.info.totalTxs')}
                value={MN.fmt.int(data.tx_count)}
                sub={successPct ? `${successPct} ${t('wallet.info.successRate')}` : null} />
              <MiniStat label={t('wallet.info.daysActive')} value={MN.fmt.int(data.days_active || 0)} />
              <MiniStat label={t('wallet.info.assetsHeld')} value={MN.fmt.int(data.assets_held || 0)} />
              <MiniStat label={t('wallet.info.received')}
                value={MN.fmt.int(incoming.total)}
                sub={incoming.mints ? `${incoming.mints} mint${incoming.mints === 1 ? '' : 's'}` : null} />
            </div>
          )}
        </div>

        {/* WHALE SCORE — only when there's any on-chain footprint */}
        {hasActivity && (
          <div className="card" style={{ padding: '16px 18px' }}>
            <div className="stat-label" style={{ marginBottom: 14 }}>
              {t('wallet.info.whaleScore')} · <span style={{ color: 'var(--accent)', fontWeight: 700, letterSpacing: '0.06em' }}>{whale.class}</span>
            </div>
            <div style={{ display: 'grid', gridTemplateColumns: '120px 1fr', gap: 22, alignItems: 'center' }}>
              <div style={{
                fontSize: 56, fontWeight: 800,
                color: 'var(--accent)', lineHeight: 1,
                fontFamily: 'JetBrains Mono, monospace', textAlign: 'center',
              }}>
                {whale.total}
              </div>
              <div style={{ display: 'flex', flexDirection: 'column', gap: 10 }}>
                <ScoreBar label={t('wallet.info.whaleVolume')} value={whale.volume} />
                <ScoreBar label={t('wallet.info.whaleFrequency')} value={whale.frequency} />
                <ScoreBar label={t('wallet.info.whaleDiversity')} value={whale.diversity} />
              </div>
            </div>
          </div>
        )}

        {/* MOVEMENTS — equivalent to v2's SWAPS mini-grid */}
        {hasActivity && (
          <div className="card" style={{ padding: '16px 18px' }}>
            <div className="stat-label" style={{ marginBottom: 12 }}>{t('wallet.info.movements')}</div>
            {!hasMovements ? (
              <div className="muted tiny">{t('wallet.info.noMovements')}</div>
            ) : (
              <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(160px, 1fr))', gap: 14 }}>
                <MiniStat label={t('wallet.info.sent')} value={MN.fmt.int(transfers.out_count)} />
                <MiniStat label={t('wallet.info.received')} value={MN.fmt.int(transfers.in_count + (incoming.mints || 0))}
                  sub={incoming.mints ? `+${incoming.mints} mint${incoming.mints === 1 ? '' : 's'}` : null} />
                <MiniStat label={t('wallet.info.volumeXor')} value={MN.fmt.compact(transfers.volume_xor)}
                  sub={transfers.volume_xor !== '0' ? transfers.volume_xor : null} />
                <MiniStat label={t('wallet.info.avgXor')} value={transfers.avg_xor !== '0' ? Number(transfers.avg_xor).toFixed(4) : '—'} />
                <MiniStat label={t('wallet.info.maxXor')} value={transfers.max_xor !== '0' ? transfers.max_xor : '—'} />
              </div>
            )}
          </div>
        )}

        {/* TOP TOKENS — table with logo + interactions + volume.
            "Interactions" counts Transfer ISIs (in/out) + Mint receipts.
            Iroha 3 has no DEX, so this is NOT swap count. Volume is raw
            on-chain units (no decimal scaling — XOR uses 18 decimals so
            "1B" raw ≈ 1 XOR; full decimal-aware formatting requires
            indexing asset_definition.decimals which is a future TODO). */}
        {hasActivity && (
          <div className="card" style={{ padding: 0, overflow: 'hidden' }}>
            <div style={{ padding: '14px 18px', borderBottom: '1px solid var(--border)' }}>
              <div className="stat-label">{t('wallet.info.topTokens')}</div>
              <div className="muted tiny" style={{ marginTop: 4 }}>{t('wallet.info.topTokensHint')}</div>
            </div>
            {topTokens.length === 0 ? (
              <div className="muted tiny" style={{ padding: '16px 18px' }}>{t('wallet.info.noTopTokens')}</div>
            ) : (
              <>
                <div style={{ display: 'grid', gridTemplateColumns: '40px 1fr 100px 140px',
                              gap: 12, padding: '10px 18px',
                              borderBottom: '1px solid var(--border)',
                              fontSize: 11, fontWeight: 700,
                              color: 'var(--fg-3)', letterSpacing: '0.08em', textTransform: 'uppercase' }}>
                  <span></span>
                  <span>{t('wallet.info.token')}</span>
                  <span style={{ textAlign: 'right' }}>{t('wallet.info.trades')}</span>
                  <span style={{ textAlign: 'right' }}>{t('wallet.info.volume')}</span>
                </div>
                {topTokens.map(tk => (
                  <a key={tk.asset_id} className="mn-row-top-token" href={'#asset/' + encodeURIComponent(tk.alias || tk.asset_id)}
                     style={{ display: 'grid', gridTemplateColumns: '40px 1fr 100px 140px',
                              gap: 12, padding: '12px 18px', alignItems: 'center',
                              borderBottom: '1px solid var(--border)',
                              textDecoration: 'none', color: 'inherit' }}>
                    <MN.TokenIcon asset={{ alias: tk.alias, name: tk.name, asset: tk.asset_id }} size={28} />
                    <div style={{ minWidth: 0 }}>
                      <div style={{ color: 'var(--fg-0)', fontSize: 13, fontWeight: 700 }}>
                        {tk.name || MN.shortHash(tk.asset_id, 6, 4)}
                      </div>
                      {tk.alias && <div className="num muted" style={{ fontSize: 10 }}>{tk.alias}</div>}
                    </div>
                    <span className="num" style={{ color: 'var(--fg-0)', fontSize: 13, fontWeight: 700, textAlign: 'right' }}>
                      {MN.fmt.int(tk.trades)}
                    </span>
                    <span className="num" style={{ color: 'var(--fg-1)', fontSize: 13, textAlign: 'right' }}
                          title={tk.volume + ' raw on-chain units'}>
                      {MN.fmt.tokenAmount(tk.volume, MN.assetDecimals(tk))}
                    </span>
                  </a>
                ))}
              </>
            )}
          </div>
        )}

        {/* IDENTITY */}
        {acct && (acct.has_primary_alias || acct.multisig_quorum || acct.first_seen_at || acct.last_seen_at) && (
          <div className="card" style={{ padding: '16px 18px' }}>
            <div className="stat-label" style={{ marginBottom: 12 }}>{t('wallet.info.identity')}</div>
            <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(220px, 1fr))', gap: 18 }}>
              {acct.has_primary_alias && (
                <InfoRow k={t('wallet.info.alias')} v={acct.primary_alias_name || acct.primary_alias} />
              )}
              {acct.multisig_quorum && (
                <InfoRow k={t('wallet.info.multisig')}
                  v={acct.multisig_quorum + '/' + acct.multisig_signatories_count} mono />
              )}
              {acct.first_seen_at && (
                <InfoRow k={t('wallet.info.firstSeen')}
                  v={MN.fmt.relative(acct.first_seen_at)} sub={acct.first_seen_at} />
              )}
              {acct.last_seen_at && (
                <InfoRow k={t('wallet.info.lastSeen')}
                  v={MN.fmt.relative(acct.last_seen_at)} sub={acct.last_seen_at} />
              )}
            </div>
          </div>
        )}

        {/* TOP INSTRUCTION KINDS */}
        {hasActivity && (
          <div className="card" style={{ padding: 0, overflow: 'hidden' }}>
            <div style={{ padding: '14px 18px', borderBottom: '1px solid var(--border)' }}>
              <div className="stat-label">{t('wallet.info.topKinds')}</div>
              <div className="muted tiny" style={{ marginTop: 4 }}>{t('wallet.info.topKindsHint')}</div>
            </div>
            {kinds.length === 0 ? (
              <div className="muted tiny" style={{ padding: '16px 18px' }}>{t('wallet.info.noKinds')}</div>
            ) : (
              kinds.map(k => (
                <a key={k.kind} href={`#instructions?kind=${encodeURIComponent(k.kind)}&authority=${enc}`}
                   style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center',
                            padding: '10px 18px', borderBottom: '1px solid var(--border)',
                            textDecoration: 'none', color: 'inherit' }}>
                  <span className="num" style={{ color: '#A062B0', fontSize: 13, fontWeight: 600 }}>{k.kind}</span>
                  <span className="num" style={{ color: 'var(--fg-0)', fontSize: 13, fontWeight: 700 }}>
                    {MN.fmt.int(k.count)}
                  </span>
                </a>
              ))
            )}
          </div>
        )}

        {/* DOMAINS OWNED */}
        {domains.length > 0 && (
          <div className="card" style={{ padding: 0, overflow: 'hidden' }}>
            <div style={{ padding: '14px 18px', borderBottom: '1px solid var(--border)' }}>
              <div className="stat-label">{t('wallet.info.domainsOwned')} <span className="tag" style={{ marginLeft: 6 }}>{domains.length}</span></div>
              <div className="muted tiny" style={{ marginTop: 4 }}>{t('wallet.info.domainsHint')}</div>
            </div>
            {domains.map(d => (
              <div key={d.id} style={{ display: 'grid', gridTemplateColumns: '1fr 80px 80px 80px',
                                        gap: 10, padding: '10px 18px',
                                        borderBottom: '1px solid var(--border)' }}>
                <span className="num" style={{ color: 'var(--fg-0)', fontSize: 13, fontWeight: 600 }}>{d.id}</span>
                <span className="num muted" style={{ fontSize: 11, textAlign: 'right' }}>{MN.fmt.int(d.accounts_count)} acc</span>
                <span className="num muted" style={{ fontSize: 11, textAlign: 'right' }}>{MN.fmt.int(d.assets_count)} ast</span>
                <span className="num muted" style={{ fontSize: 11, textAlign: 'right' }}>{MN.fmt.int(d.nfts_count)} nft</span>
              </div>
            ))}
          </div>
        )}

        {/* ASSET DEFINITIONS CREATED */}
        {assetDefs.length > 0 && (
          <div className="card" style={{ padding: 0, overflow: 'hidden' }}>
            <div style={{ padding: '14px 18px', borderBottom: '1px solid var(--border)' }}>
              <div className="stat-label">{t('wallet.info.assetsCreated')} <span className="tag" style={{ marginLeft: 6 }}>{assetDefs.length}</span></div>
              <div className="muted tiny" style={{ marginTop: 4 }}>{t('wallet.info.assetsCreatedHint')}</div>
            </div>
            {assetDefs.map(a => (
              <a key={a.id} href={'#asset/' + encodeURIComponent(a.alias || a.id)}
                 style={{ display: 'grid', gridTemplateColumns: '40px 1fr 140px 70px',
                          alignItems: 'center', gap: 12, padding: '12px 18px',
                          borderBottom: '1px solid var(--border)',
                          textDecoration: 'none', color: 'inherit' }}>
                <MN.TokenIcon asset={{ alias: a.alias, name: a.name, asset: a.id }} size={28} />
                <div style={{ minWidth: 0 }}>
                  <div style={{ color: 'var(--fg-0)', fontSize: 13, fontWeight: 700 }}>
                    {a.name || MN.shortHash(a.id, 6, 4)}
                  </div>
                  {a.alias && <div className="num muted" style={{ fontSize: 10 }}>{a.alias}</div>}
                </div>
                <span className="num" style={{ color: 'var(--fg-0)', fontSize: 12, textAlign: 'right' }}>{a.total_quantity}</span>
                <span className="num muted" style={{ fontSize: 11, textAlign: 'right' }}>{a.confidential_mode || '—'}</span>
              </a>
            ))}
          </div>
        )}

        {/* CROSS-CHAIN BRIDGE */}
        <div className="card" style={{ padding: '16px 18px' }}>
          <div className="stat-label" style={{ marginBottom: 4 }}>{t('wallet.info.crossChain')}</div>
          <div className="muted tiny" style={{ marginBottom: 12 }}>{t('wallet.info.crossChainHint')}</div>
          {!hasCrossChain ? (
            <div className="muted tiny">{t('wallet.info.noCrossChain')}</div>
          ) : (
            <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(180px, 1fr))', gap: 14 }}>
              <MiniStat label={t('wallet.info.claimsReceived')} value={MN.fmt.int(cc.claims_received)} />
              <MiniStat label={t('wallet.info.xorClaimed')} value={cc.xor_claimed || '0'} />
            </div>
          )}
        </div>

        {/* LOCAL PORTFOLIO (browser-side only) */}
        {savedWallet && (
          <div className="card" style={{ padding: '16px 18px' }}>
            <div className="stat-label" style={{ marginBottom: 12 }}>{t('wallet.info.localAlias')}</div>
            <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(220px, 1fr))', gap: 18 }}>
              <InfoRow k={t('wallet.savedAlias')} v={savedWallet.alias || '—'} />
              <InfoRow k={t('wallet.localId')} v={savedWallet.id} mono />
            </div>
          </div>
        )}
      </div>
    );
  }

  function MiniStat({ label, value, sub }) {
    return (
      <div>
        <div className="stat-label">{label}</div>
        <div className="num" style={{ color: 'var(--fg-0)', fontSize: 18, fontWeight: 700, marginTop: 4 }}>{value}</div>
        {sub && <div className="muted tiny" style={{ marginTop: 2 }}>{sub}</div>}
      </div>
    );
  }

  // Whale-score progress bar — label on the left, gradient track in the middle,
  // numeric score on the right. Shape mirrors v2 exactly so the two networks
  // feel like the same product.
  function ScoreBar({ label, value }) {
    const pct = Math.max(0, Math.min(100, Number(value) || 0));
    return (
      <div style={{ display: 'grid', gridTemplateColumns: '110px 1fr 40px', alignItems: 'center', gap: 12 }}>
        <span className="muted tiny" style={{ letterSpacing: '0.04em' }}>{label}</span>
        <div style={{ height: 6, borderRadius: 999, background: 'rgba(255,255,255,0.06)', overflow: 'hidden' }}>
          <div style={{
            width: pct + '%', height: '100%',
            background: 'linear-gradient(90deg,#7B2D5B,#A062B0,#C8A0B8)',
          }}/>
        </div>
        <span className="num" style={{ color: 'var(--fg-1)', fontSize: 12, textAlign: 'right' }}>{Math.round(pct)}</span>
      </div>
    );
  }

  function pillBtnStyle() {
    return {
      padding: '4px 10px', borderRadius: 999, fontSize: 11, fontWeight: 600,
      background: 'rgba(255,255,255,0.04)', color: '#e5e7eb',
      border: '1px solid rgba(255,255,255,0.10)', cursor: 'pointer',
    };
  }
  function sectionHeaderStyle() {
    return {
      display: 'flex', justifyContent: 'space-between', alignItems: 'baseline',
      padding: '12px 16px', background: 'rgba(255,255,255,0.02)',
      borderBottom: '1px solid rgba(255,255,255,0.06)',
    };
  }
  function muteStyle()  { return { padding: 24, textAlign: 'center', color: '#6b7280', fontSize: 12 }; }
  function errStyle()   { return { padding: 14, color: '#EF4444', fontSize: 12 }; }

  // -----------------------------------------------------------
  // Top-level: Portfolio when no id in hash, Drilldown when one is.
  // -----------------------------------------------------------
  function Wallet() {
    const [id, setId] = React.useState(readWalletId);
    React.useEffect(() => {
      function onHash() { setId(readWalletId()); }
      window.addEventListener('hashchange', onHash);
      return () => window.removeEventListener('hashchange', onHash);
    }, []);

    const onPick = (val) => {
      window.location.hash = 'wallet/' + encodeURIComponent(val);
    };

    if (!id) return <Portfolio onPick={onPick} />;
    return <Drilldown id={id} />;
  }

  MN.Wallet = Wallet;
  MN.walletHref = (id) => '#wallet/' + encodeURIComponent(id);
})(window.MN);
