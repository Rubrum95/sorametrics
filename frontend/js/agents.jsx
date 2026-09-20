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
  const [client, setClient] = useStateAg('claude');
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
      <PageHeader title={t('agents.title', 'Agents · API')} sub={t('agents.sub', 'Ask SoraMetrics from Claude, ChatGPT or your own code')}/>
      <div style={{display:'grid', gap:16}}>
        <AgCard title={t('agents.what.title', 'What this is')}>
          <p className="muted" style={{margin:'12px 0', maxWidth:820, lineHeight:1.6}}>
            {t('agents.what.body', 'You can plug SoraMetrics into your assistant and simply ask it things: what a wallet holds, whether a token can really be sold at its price, which validators are not producing blocks, what is being voted. The assistant reads the same on-chain data as this site. It is free, read-only and needs no account or key.')}
          </p>
        </AgCard>

        <AgCard title={t('agents.quick.title', 'Connect it in 3 steps')}>
          <div style={{display:'flex', gap:8, flexWrap:'wrap', margin:'12px 0 4px'}}>
            {[['claude', 'Claude'], ['chatgpt', 'ChatGPT'], ['code', t('agents.quick.code', 'Claude Code · Cursor · VS Code')]].map(([id, label]) => (
              <button key={id} className={'btn ' + (client === id ? 'primary' : 'ghost')} onClick={() => setClient(id)}>{label}</button>
            ))}
          </div>
          {client === 'claude' && (
            <ol className="agents-steps">
              <li>{t('agents.claude.1', 'In Claude (web or desktop) open Customize → Connectors, press "+" and choose "Add custom connector".')}</li>
              <li>{t('agents.claude.2', 'Paste the address below and press Add. No login or key is needed.')}</li>
              <li>{t('agents.claude.3', 'Start a chat and ask, for example: "Using SoraMetrics, what does wallet cn… hold?" Works on every plan, including Free (one custom connector).')}</li>
            </ol>
          )}
          {client === 'chatgpt' && (
            <ol className="agents-steps">
              <li>{t('agents.gpt.1', 'In ChatGPT on the web open Settings and turn on Developer mode (under Apps or Connectors → Advanced, depending on your version). It needs a paid plan: Plus, Pro, Business, Enterprise or Edu.')}</li>
              <li>{t('agents.gpt.2', 'Add a custom connector, paste the address below and choose "no authentication".')}</li>
              <li>{t('agents.gpt.3', 'In a new chat press "+" → More → Developer mode, select SoraMetrics and ask your question.')}</li>
            </ol>
          )}
          {client === 'code' && (
            <ol className="agents-steps">
              <li>{t('agents.code.1', 'Claude Code: run the command below in your terminal.')}</li>
              <li>{t('agents.code.2', 'Cursor, VS Code and other clients: add the JSON block below to their MCP configuration.')}</li>
              <li>{t('agents.code.3', 'Restart the client: the SoraMetrics tools appear on their own.')}</li>
            </ol>
          )}
          <div style={{marginTop:14}}>
            <AgCopyRow label={t('tools.agents.mcp', 'MCP server')} value={mcpUrl}/>
            {client === 'code' && <AgCopyRow label="Claude Code" value={'claude mcp add --transport http sorametrics ' + mcpUrl}/>}
            {client === 'code' && <AgCopyRow label={t('agents.connect.json', 'JSON config')} value={jsonConfig}/>}
          </div>
          <p className="muted tiny" style={{margin:'12px 0 0', maxWidth:820, lineHeight:1.6}}>
            {t('agents.quick.note', 'Menus change between versions of each assistant; what matters is adding a custom connector (MCP) with that address. If your assistant cannot reach the internet from its code sandbox, use its connector or browsing feature instead.')}
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
