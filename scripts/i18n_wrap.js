// usage: node i18n_ast.js <nm_root> scan <out.json> files...   |   apply <map.json> files...
const path = require('path'), fs = require('fs');
const root = process.argv[2], mode = process.argv[3], arg = process.argv[4], files = process.argv.slice(5);
const parser = require(path.join(root, 'node_modules/@babel/parser'));
const traverse = require(path.join(root, 'node_modules/@babel/traverse')).default;
const ATTRS = new Set(['title','placeholder','label','sub','hint','aria-label','alt','empty','desc','caption','tooltip','text','heading','eyebrow','subtitle','emptyText','cta']);
const PROPS = new Set(['label','title','sub','hint','desc','text','placeholder','tooltip','caption','empty','heading','subtitle','short','long']);
const LETTERS = /[A-Za-zÀ-ÿ]{3,}/;
const IDLIKE = /^[a-z0-9_.\-:/#]+$/;
const BRAND = /^(?:[\s\d\W]|XOR|VAL|PSWAP|TBCD|KUSD|XSTUSD|XST|DAI|ETH|USD|EUR|SORA|API|MCP|REST|CSV|JSON|TVL|APY|APR|DEX|ID|TX|URL|GitHub|Telegram|Polkaswap|SoraMetrics|Minamoto|Taira|Iroha|Torii|Polkamarkt|Hashi|Kensetsu|Demeter|Ceres|Apollo|Wallet|Hash|Swaps|Bridges|Extrinsics|Transfers)+$/;
function ok(s) { s = s.trim(); return LETTERS.test(s) && !IDLIKE.test(s) && !BRAND.test(s); }
function fnName(p) {
  let f = p.getFunctionParent(), name = null;
  while (f) { const n = f.node.id?.name || (f.parentPath.isVariableDeclarator() && f.parentPath.node.id.name) || null; if (n) name = n; f = f.getFunctionParent(); }
  return name;
}
function collect(file) {
  const src = fs.readFileSync(file, 'utf8');
  const ast = parser.parse(src, { sourceType: 'script', plugins: ['jsx'] });
  const hits = [];
  const tState = (p) => {
    const b = p.scope.getBinding('t');
    if (!b) return 'none';
    if (b.path.isVariableDeclarator()) { const init = b.path.node.init ? src.slice(b.path.node.init.start, b.path.node.init.end) : ''; return /useT\(|useLang\(|useContext\(/.test(init) ? 'ok' : 'shadow'; }
    if (b.kind === 'param' && b.path.parentPath && b.path.isObjectPattern()) return 'ok';
    return 'shadow';
  };
  const compBody = (p) => {
    let f = p.getFunctionParent();
    while (f) {
      const n = f.node.id?.name || (f.parentPath.isVariableDeclarator() && f.parentPath.node.id.name) || '';
      if (/^[A-Z]/.test(n) && f.node.body.type === 'BlockStatement') return f.node.body.start + 1;
      f = f.getFunctionParent();
    }
    return null;
  };
  const push = (p, node, kind, text, start, end) => { const ts = tState(p); hits.push({ file, line: node.loc.start.line, kind, text, start, end, hasT: ts === 'ok', tState: ts, insertAt: ts === 'none' ? compBody(p) : null, fn: fnName(p) }); };
  traverse(ast, {
    JSXText(p) {
      const raw = p.node.value; if (!ok(raw)) return;
      const lead = raw.match(/^\s*/)[0].length, trail = raw.match(/\s*$/)[0].length;
      const core = raw.slice(lead, raw.length - trail);
      push(p, p.node, 'jsxtext', core.replace(/\s*\n\s*/g, ' '), p.node.start + lead, p.node.end - trail);
    },
    StringLiteral(p) {
      const v = p.node.value; if (!ok(v)) return;
      const par = p.parentPath;
      if (par.isJSXAttribute()) { const n = par.node.name.name; if (ATTRS.has(n)) push(p, p.node, 'attr', v, p.node.start, p.node.end); return; }
      if (par.isObjectProperty() && par.node.value === p.node) { const k = par.node.key.name || par.node.key.value; if (PROPS.has(k)) push(p, p.node, 'prop', v, p.node.start, p.node.end); return; }
      let q = par; // literals that render as JSX children: {cond ? 'A' : 'B'}, {x || 'A'}
      while (q && (q.isConditionalExpression() || q.isLogicalExpression())) q = q.parentPath;
      if (q && q.isJSXExpressionContainer() && q.parentPath.isJSXElement()) {
        if (par.isConditionalExpression() && par.node.test === p.node) return;
        push(p, p.node, 'expr', v, p.node.start, p.node.end);
      }
    },
  });
  return { src, hits };
}
if (mode === 'scan') {
  const all = []; for (const f of files) all.push(...collect(f).hits.map(({ src, ...h }) => h));
  fs.writeFileSync(arg, JSON.stringify(all, null, 0));
  const by = {}; for (const h of all) { by[h.file] = by[h.file] || [0, 0]; by[h.file][0]++; if (!h.hasT) by[h.file][1]++; }
  for (const f in by) console.log(String(by[f][0]).padStart(4), 'noT=' + String(by[f][1]).padStart(3), path.basename(f));
  console.log(all.length, 'hits,', new Set(all.map(h => h.text)).size, 'unique');
} else if (mode === 'apply') {
  const map = JSON.parse(fs.readFileSync(arg, 'utf8')); // text -> key
  let done = 0, skipped = [];
  for (const f of files) {
    let { src, hits } = collect(f);
    const edits = [], inserts = new Set();
    for (const h of hits) {
      const key = map[h.text]; if (!key) continue;
      if (!h.hasT && h.insertAt == null) { skipped.push(`${path.basename(f)}:${h.line} [${h.fn}] (${h.tState}) ${h.text}`); continue; }
      if (!h.hasT) inserts.add(h.insertAt);
      const lit = "'" + h.text.replace(/\\/g, '\\\\').replace(/'/g, "\\'") + "'";
      const c = `t('${key}', ${lit})`;
      edits.push({ start: h.start, end: h.end, rep: (h.kind === 'jsxtext' || h.kind === 'attr') ? `{${c}}` : c }); done++;
    }
    for (const pos of inserts) edits.push({ start: pos, end: pos, rep: '\n  const t = useT();' });
    edits.sort((a, b) => b.start - a.start);
    for (const e of edits) src = src.slice(0, e.start) + e.rep + src.slice(e.end);
    fs.writeFileSync(f, src);
  }
  console.log('wrapped', done, '| skipped (no t in scope):', skipped.length); skipped.forEach(s => console.log('  ', s));
}
