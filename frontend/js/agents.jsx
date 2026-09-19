/* global React, useT, PageHeader */
// "API · MCP" section: how to plug an AI agent (or plain code) into
// SoraMetrics. The tool and prompt lists are read from the MCP endpoint
// itself, so the page cannot drift from what the server offers.
const { useState: useStateAg, useEffect: useEffectAg } = React;

function mcpCall(method, params) {
  return fetch('/mcp', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ jsonrpc: '2.0', id: 1, method, params: params || {} }),
  }).then(r => r.ok ? r.json() : null).then(j => (j && j.result) || null).catch(() => null);
}

function AgCopyRow({ label, value, href, mono = true }) {
  const t = useT();
  const [copied, setCopied] = useStateAg(false);
  const copy = () => {
    if (!navigator.clipboard) return;
    navigator.clipboard.writeText(value).then(() => { setCopied(true); setTimeout(() => setCopied(false), 1500); });
  };
  return (
    <div style={{display:'grid', gridTemplateColumns:'minmax(110px, 170px) 1fr auto', gap:12, alignItems:'center', padding:'10px 0', borderTop:'1px solid var(--border)'}}>
      <span className="muted tiny" style={{textTransform:'uppercase', letterSpacing:'0.06em'}}>{label}</span>
      {href
        ? <a href={href} target="_blank" rel="noopener" className={mono ? 'mono' : ''} style={{color:'var(--fg-0)', overflowWrap:'anywhere'}}>{value}</a>
        : <code className="mono" style={{color:'var(--fg-0)', overflowWrap:'anywhere', whiteSpace:'pre-wrap'}}>{value}</code>}
      <button className="btn ghost" onClick={copy}>{copied ? t('tools.agents.copied', 'Copied') : t('tools.agents.copy', 'Copy')}</button>
    </div>
  );
}

function AgCard({ title, children }) {
  return (
    <div className="card" style={{padding: 0}}>
      <div className="card-header"><div className="card-title"><span className="dot"/> {title}</div></div>
      <div style={{padding:'4px 20px 16px'}}>{children}</div>
    </div>
  );
}

function AgentsSection() {
  const t = useT();
  const origin = window.location.origin;
  const mcpUrl = origin + '/mcp';
  const [tools, setTools] = useStateAg(null);
  const [prompts, setPrompts] = useStateAg(null);
  useEffectAg(() => {
    let cancelled = false;
    mcpCall('tools/list').then(r => { if (!cancelled) setTools((r && r.tools) || []); });
    mcpCall('prompts/list').then(r => { if (!cancelled) setPrompts((r && r.prompts) || []); });
    return () => { cancelled = true; };
  }, []);

  const jsonConfig = JSON.stringify({ mcpServers: { sorametrics: { type: 'http', url: mcpUrl } } }, null, 2);
  const curl = "curl -s " + origin + "/tokens?search=xor&sparkline=false";
  const examples = [
    t('agents.ex1', 'What does wallet cn… hold, and what would it really sell for?'),
    t('agents.ex2', 'Is the quoted price of VAL realizable? Show liquidity, holders and 30-day price.'),
    t('agents.ex3', 'Which validators are in the active set but producing no blocks?'),
    t('agents.ex4', 'What is the council voting on right now?'),
    t('agents.ex5', 'How much XOR was burned in the last 7 days, and how?'),
  ];

  return (
    <div>
      <PageHeader title={t('agents.title', 'API · MCP')} sub={t('agents.sub', 'Connect agents and code to SoraMetrics')}/>
      <div style={{display:'grid', gap:16}}>
        <AgCard title={t('agents.what.title', 'What this is')}>
          <p className="muted" style={{margin:'12px 0', maxWidth:820, lineHeight:1.6}}>
            {t('agents.what.body', 'SoraMetrics can be queried by agents (Claude, Cursor and any client that speaks the Model Context Protocol) and by your own code. It is read-only, free and needs no account or API key. Every answer comes with its caveats, such as marginal prices and tokens without liquidity, so an agent does not draw false conclusions.')}
          </p>
        </AgCard>

        <AgCard title={t('agents.connect.title', 'Connect an agent (MCP)')}>
          <AgCopyRow label={t('tools.agents.mcp', 'MCP server')} value={mcpUrl}/>
          <AgCopyRow label="Claude Code" value={'claude mcp add --transport http sorametrics ' + mcpUrl}/>
          <AgCopyRow label={t('agents.connect.json', 'JSON config')} value={jsonConfig}/>
          <p className="muted tiny" style={{margin:'12px 0 0', maxWidth:820, lineHeight:1.6}}>
            {t('agents.connect.note', 'In Claude (web or desktop): Settings → Connectors → Add custom connector, and paste the MCP server URL. The JSON block is the usual format for Cursor, VS Code and other clients. No authentication.')}
          </p>
        </AgCard>

        <AgCard title={t('agents.ask.title', 'Things you can ask once connected')}>
          <ul style={{margin:'12px 0 0', paddingLeft:18, lineHeight:1.9, color:'var(--fg-1)'}}>
            {examples.map((e, i) => <li key={i}>{e}</li>)}
          </ul>
          {prompts && prompts.length > 0 && (
            <div style={{marginTop:14}}>
              <div className="muted tiny" style={{textTransform:'uppercase', letterSpacing:'0.06em', marginBottom:6}}>{t('agents.prompts', 'Ready-made prompts')}</div>
              <div style={{display:'flex', flexWrap:'wrap', gap:8}}>
                {prompts.map(p => <span key={p.name} className="tag" title={p.description}>{p.title || p.name}</span>)}
              </div>
            </div>
          )}
        </AgCard>

        <AgCard title={(t('agents.tools.title', 'Tools') + (tools ? ' · ' + tools.length : ''))}>
          {tools === null && <p className="muted" style={{margin:'12px 0'}}>…</p>}
          {tools && tools.length === 0 && <p className="muted" style={{margin:'12px 0'}}>—</p>}
          {tools && tools.length > 0 && (
            <div style={{display:'grid', gridTemplateColumns:'repeat(auto-fill, minmax(280px, 1fr))', gap:'4px 24px', marginTop:8}}>
              {tools.map(tool => (
                <div key={tool.name} style={{padding:'10px 0', borderTop:'1px solid var(--border)'}}>
                  <div style={{fontWeight:700, color:'var(--fg-0)'}}>{tool.title || tool.name}</div>
                  <code className="mono tiny muted">{tool.name}</code>
                  <div className="muted tiny" style={{marginTop:4, lineHeight:1.5}}>{tool.description}</div>
                </div>
              ))}
            </div>
          )}
        </AgCard>

        <AgCard title={t('agents.rest.title', 'REST API for developers')}>
          <AgCopyRow label="OpenAPI 3.1" value={origin + '/openapi.json'} href="/openapi.json"/>
          <AgCopyRow label="llms.txt" value={origin + '/llms.txt'} href="/llms.txt"/>
          <AgCopyRow label={t('agents.rest.example', 'Example')} value={curl}/>
          <p className="muted tiny" style={{margin:'12px 0 0', maxWidth:820, lineHeight:1.6}}>
            {t('agents.rest.note', 'Plain JSON over HTTPS. Limits are per IP and per route (10 to 300 requests per minute); a 429 means wait for the next minute. llms.txt explains what the figures mean.')}
          </p>
        </AgCard>
      </div>
    </div>
  );
}

Object.assign(window, { AgentsSection });
