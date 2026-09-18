/* global window, React */
// ============================================================
// js/minamoto/verbs.jsx
// First-class "verb" sections: Transfers, Mints, Burns. Each is a
// pre-filtered Instructions list with a column layout tailored to
// that specific kind — same conceptual pattern as v2 sorametrics
// surfacing Swaps / Transfers / Bridges as top-level sidebar items
// instead of a single generic "Events" tab.
//
// Backend: same /api/minamoto/instructions?kind=<X> endpoint that
// powers the all-actions view; here we just shape the UI.
// ============================================================

window.MN = window.MN || {};

(function (MN) {
  // -----------------------------------------------------------
  // Asset cache: hash → { name, alias, confidential_mode } so the
  // table can render the asset's name + logo instead of the opaque
  // `6TEAJqbb…` definition_id buried in payload.value.object.
  // -----------------------------------------------------------
  function useAssetDefs() {
    const [defs, setDefs] = React.useState({});
    React.useEffect(() => {
      let cancelled = false;
      MN.api('/asset-definitions').then(r => {
        if (cancelled) return;
        const m = {};
        for (const d of (r.items || [])) m[d.id] = d;
        setDefs(m);
      }).catch(() => {});
      return () => { cancelled = true; };
    }, []);
    return defs;
  }

  // Extract { defId, accId } from an Iroha 3 path.
  // - "<defId>#<accId>"  → both halves (asset path)
  // - "<accId>"          → bare account id (no asset, e.g. Transfer's
  //                        destination field which is already an account)
  // The account-only form starts with the network prefix "sorau" /
  // "testu" — we use that as a tie-breaker when there's no '#'.
  function splitAssetPath(s) {
    if (!s) return { defId: null, accId: null };
    const str = String(s);
    const idx = str.indexOf('#');
    if (idx !== -1) return { defId: str.slice(0, idx), accId: str.slice(idx + 1) };
    // No '#' — Iroha 3 account ids start with the network prefix.
    if (/^(sorau|testu|soraヾ|soraｴ|sora[uW])/.test(str) || str.length > 40) {
      return { defId: null, accId: str };
    }
    return { defId: str, accId: null };
  }

  // -----------------------------------------------------------
  // Shared verb table. Column set chosen per `kind` so each section
  // reads as a domain-specific list, not a generic ISI dump.
  // -----------------------------------------------------------
  function Kpi({ label, value, sub, accent }) {
    return (
      <div className="stat-card" style={accent ? { borderTop: '2px solid ' + accent } : null}>
        <span className="stat-label">{label}</span>
        <span className="stat-value num">{value}</span>
        {sub && <span className="stat-sub">{sub}</span>}
      </div>
    );
  }

  // Format raw token quantities from Iroha 3 instructions. Most tokens use
  // 18 decimals (XOR-style). We render compact (1.2M, 3.4k, etc.) so the
  // KPI cards stay the same width regardless of token magnitude.
  function fmtTokenQty(raw, decimals = 18) {
    if (raw == null || raw === '' || raw === '0') return '0';
    const s = String(raw);
    if (!/^[0-9]+$/.test(s)) return s.slice(0, 12);
    if (s.length <= decimals) {
      const padded = s.padStart(decimals + 1, '0');
      const whole  = padded.slice(0, padded.length - decimals);
      const frac   = padded.slice(padded.length - decimals).replace(/0+$/, '').slice(0, 4);
      return whole + (frac ? '.' + frac : '');
    }
    const whole = s.slice(0, s.length - decimals);
    const n = Number(whole);
    if (!Number.isFinite(n)) return whole;
    if (n >= 1_000_000) return (n / 1_000_000).toFixed(2) + 'M';
    if (n >= 1_000)     return (n / 1_000).toFixed(2) + 'k';
    return n.toLocaleString('en-US');
  }

  function VerbTable({ kind, defs }) {
    const t = MN.i18n.useT();
    const [page, setPage] = React.useState(1);
    const perPage = 50;
    const { data, error, loading } = MN.useFetch(
      `/instructions?page=${page}&per_page=${perPage}&kind=${encodeURIComponent(kind)}`,
      30_000
    );
    // Stats endpoint exists for Transfer; for Mint/Burn we derive lightweight
    // stats from the current page (still informative on a young chain).
    const xferStats = MN.useFetch(kind === 'Transfer' ? '/transfers/stats' : null, 60_000);
    const xs = (kind === 'Transfer' && xferStats.data) ? xferStats.data : null;

    function assetCell(defId) {
      const d = defs[defId] || {};
      const proxy = { alias: d.alias, name: d.name, asset: defId };
      return (
        <a href={'#asset/' + encodeURIComponent(d.alias || d.name || defId)}
           style={{ display: 'inline-flex', alignItems: 'center', gap: 8, textDecoration: 'none', color: 'inherit' }}>
          {MN.TokenIcon ? <MN.TokenIcon asset={proxy} size={22} /> : null}
          <span style={{ color: 'var(--fg-0)', fontWeight: 600, fontSize: 13 }}>{d.name || MN.shortHash(defId, 6, 4)}</span>
          {d.alias && <span className="num muted" style={{ fontSize: 11 }}>{d.alias}</span>}
        </a>
      );
    }

    function accountCell(accId) {
      if (!accId) return <span className="muted">—</span>;
      return (
        <a className="num" href={MN.walletHref ? MN.walletHref(accId) : '#wallet/' + encodeURIComponent(accId)}
           title={accId}
           style={{ color: 'var(--accent)', textDecoration: 'none', borderBottom: '1px dotted rgba(255,255,255,0.20)' }}>
          {MN.shortAccount(accId, 8, 6)}
        </a>
      );
    }

    // Build per-kind column set.
    let cols;
    if (kind === 'Transfer') {
      cols = [
        { key: 'when',   label: t('common.created'),   render: r => <span className="muted">{MN.fmt.relative(r.created_at)}</span> },
        { key: 'from',   label: t('verb.from'),        render: r => accountCell(splitAssetPath(r.payload?.value?.source).accId) },
        { key: 'to',     label: t('verb.to'),          render: r => accountCell(splitAssetPath(r.payload?.value?.destination).accId) },
        { key: 'asset',  label: t('verb.asset'),       render: r => {
          const path = splitAssetPath(r.payload?.value?.source || r.payload?.value?.destination);
          return assetCell(path.defId);
        } },
        { key: 'qty',    label: t('verb.amount'),      render: r => <span className="num" style={{ color: 'var(--fg-0)', fontWeight: 600 }}>{r.payload?.value?.object || '—'}</span> },
        { key: 'tx',     label: t('common.hash'),      render: r => (
          <a className="num" href={'#tx/' + r.transaction_hash} title={r.transaction_hash}
             style={{ color: 'var(--accent)', textDecoration: 'none' }}>{MN.shortHash(r.transaction_hash, 6, 4)}</a>
        ) },
        { key: 'block',  label: '#',                   render: r => (
          <a className="num" href={'#block/' + r.block} style={{ color: 'var(--fg-0)', textDecoration: 'none' }}>#{MN.fmt.int(r.block)}</a>
        ) },
      ];
    } else if (kind === 'Mint') {
      cols = [
        { key: 'when',   label: t('common.created'),   render: r => <span className="muted">{MN.fmt.relative(r.created_at)}</span> },
        { key: 'to',     label: t('verb.recipient'),   render: r => accountCell(splitAssetPath(r.payload?.value?.destination).accId) },
        { key: 'asset',  label: t('verb.asset'),       render: r => assetCell(splitAssetPath(r.payload?.value?.destination).defId) },
        { key: 'qty',   label: t('verb.amount'),       render: r => <span className="num" style={{ color: '#6EE7B7', fontWeight: 700 }}>+{r.payload?.value?.object || '—'}</span> },
        { key: 'by',     label: t('verb.minter'),      render: r => accountCell(r.authority) },
        { key: 'tx',     label: t('common.hash'),      render: r => (
          <a className="num" href={'#tx/' + r.transaction_hash} title={r.transaction_hash}
             style={{ color: 'var(--accent)', textDecoration: 'none' }}>{MN.shortHash(r.transaction_hash, 6, 4)}</a>
        ) },
        { key: 'block',  label: '#',                   render: r => (
          <a className="num" href={'#block/' + r.block} style={{ color: 'var(--fg-0)', textDecoration: 'none' }}>#{MN.fmt.int(r.block)}</a>
        ) },
      ];
    } else if (kind === 'Burn') {
      cols = [
        { key: 'when',   label: t('common.created'),   render: r => <span className="muted">{MN.fmt.relative(r.created_at)}</span> },
        { key: 'from',   label: t('verb.from'),        render: r => accountCell(splitAssetPath(r.payload?.value?.destination).accId) },
        { key: 'asset',  label: t('verb.asset'),       render: r => assetCell(splitAssetPath(r.payload?.value?.destination).defId) },
        { key: 'qty',   label: t('verb.amount'),       render: r => <span className="num" style={{ color: '#FCA5A5', fontWeight: 700 }}>-{r.payload?.value?.object || '—'}</span> },
        { key: 'by',     label: t('verb.burner'),      render: r => accountCell(r.authority) },
        { key: 'tx',     label: t('common.hash'),      render: r => (
          <a className="num" href={'#tx/' + r.transaction_hash} title={r.transaction_hash}
             style={{ color: 'var(--accent)', textDecoration: 'none' }}>{MN.shortHash(r.transaction_hash, 6, 4)}</a>
        ) },
        { key: 'block',  label: '#',                   render: r => (
          <a className="num" href={'#block/' + r.block} style={{ color: 'var(--fg-0)', textDecoration: 'none' }}>#{MN.fmt.int(r.block)}</a>
        ) },
      ];
    }

    const rows = (data && data.items ? data.items : []).map(x => ({ ...x, __key: x.transaction_hash + ':' + x.instruction_index }));
    const total = data ? data.total : 0;

    const titles = {
      Transfer: { t: t('verb.transfersTitle'), s: t('verb.transfersSub') },
      Mint:     { t: t('verb.mintsTitle'),     s: t('verb.mintsSub') },
      Burn:     { t: t('verb.burnsTitle'),     s: t('verb.burnsSub') },
    };
    const meta = titles[kind] || { t: kind, s: '' };

    // Per-page derived stats (used for Mint/Burn since we don't have a
    // dedicated stats endpoint for them yet).
    const pageItems = (data && data.items) || [];
    const pageVolume = pageItems.reduce((acc, x) => {
      const v = x.payload && x.payload.value && x.payload.value.object;
      const variant = x.payload && (x.payload.variant || (x.payload.value && x.payload.value.variant));
      if (variant !== 'Asset') return acc;
      try { return acc + BigInt(String(v || '0')); } catch (_) { return acc; }
    }, 0n);
    const pageUniqueAuthors = new Set(pageItems.map(x => x.authority)).size;

    return (
      <section className="section">
        <div className="page-header">
          <div>
            <h1 className="page-title">{meta.t}</h1>
            <div className="page-sub">{meta.s} · {data ? `${MN.fmt.int(total)} total` : t('common.loading')}</div>
          </div>
        </div>

        {/* KPI hero — uses /transfers/stats for Transfer kind, falls back to
            page-derived stats for Mint/Burn (less accurate but informative). */}
        {kind === 'Transfer' && xs ? (
          <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(180px, 1fr))', gap: 12, marginBottom: 18 }}>
            <Kpi label={t('verb.kpi.total24h')}     value={MN.fmt.int(xs.total_24h)}                 sub={t('verb.kpi.totalSub')} accent="#60A5FA" />
            <Kpi label={t('verb.kpi.volume24h')}
               value={MN.fmt.tokenAmount(xs.volume_24h, MN.assetDecimals(xs.top_asset || { alias: 'xor#universal' }))}
               sub={(xs.top_asset && xs.top_asset.name) || 'XOR'} />
            <Kpi label={t('verb.kpi.uniqueSenders')} value={MN.fmt.int(xs.unique_senders)}            sub={t('verb.kpi.unique')} />
            <Kpi label={t('verb.kpi.topAsset')}
                 value={xs.top_asset ? (xs.top_asset.name || MN.shortHash(xs.top_asset.asset_id, 6, 3)) : '—'}
                 sub={xs.top_asset ? MN.fmt.int(xs.top_asset.count) + ' ' + t('verb.kpi.transfers') : null} />
            <Kpi label={t('verb.kpi.total')}        value={MN.fmt.int(xs.total)} />
          </div>
        ) : (
          <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(180px, 1fr))', gap: 12, marginBottom: 18 }}>
            <Kpi label={t('verb.kpi.total')}         value={MN.fmt.int(total)}            sub={kind === 'Mint' ? t('verb.kpi.totalMintSub') : t('verb.kpi.totalBurnSub')}
                 accent={kind === 'Mint' ? '#10B981' : '#EF4444'} />
            <Kpi label={t('verb.kpi.pageVolume')}    value={fmtTokenQty(pageVolume.toString())} sub={t('verb.kpi.thisPage')} />
            <Kpi label={t('verb.kpi.pageActors')}    value={MN.fmt.int(pageUniqueAuthors)} sub={t('verb.kpi.uniqueOnPage')} />
            <Kpi label={t('verb.kpi.pageRows')}      value={MN.fmt.int(pageItems.length)}  sub={t('verb.kpi.thisPage')} />
          </div>
        )}

        {error && <div style={{ color: 'var(--err)', padding: 12 }}>{error.message}</div>}
        <MN.ui.Table columns={cols} rows={rows} empty={loading ? t('common.loading') : t('common.empty')} />
        <MN.ui.Pager page={page} totalPages={data ? data.total_pages : 1} onChange={setPage} />
      </section>
    );
  }

  function Transfers() { return <VerbTable kind="Transfer" defs={useAssetDefs()} />; }
  function Mints()     { return <VerbTable kind="Mint"     defs={useAssetDefs()} />; }
  function Burns()     { return <VerbTable kind="Burn"     defs={useAssetDefs()} />; }

  MN.Transfers = Transfers;
  MN.Mints     = Mints;
  MN.Burns     = Burns;
})(window.MN);
