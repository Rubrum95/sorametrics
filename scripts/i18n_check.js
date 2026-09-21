// usage: node scripts/i18n_check.js <dir with node_modules/@babel> <dict.json> frontend/js/*.jsx
// Fails loudly on t() calls without a real translator binding (missing useT, shadowed `t`, TDZ) and on keys absent from the dictionary.
const path=require('path'),fs=require('fs'); const root=process.argv[2];
const parser=require(path.join(root,'node_modules/@babel/parser')); const traverse=require(path.join(root,'node_modules/@babel/traverse')).default;
const DICT=JSON.parse(fs.readFileSync(process.argv[3],'utf8')); let problems=0, calls=0, missing=new Set();
for(const f of process.argv.slice(4)){ const src=fs.readFileSync(f,'utf8'); const ast=parser.parse(src,{sourceType:'script',plugins:['jsx']});
 traverse(ast,{CallExpression(p){ const c=p.node.callee; if(c.type!=='Identifier'||c.name!=='t')return; calls++;
  const a=p.node.arguments[0]; if(a&&a.type==='StringLiteral'&&!DICT[a.value]&&p.node.arguments.length<2) missing.add(path.basename(f)+':'+a.value);
  const b=p.scope.getBinding('t'); const where=`${path.basename(f)}:${p.node.loc.start.line}`;
  if(!b){problems++;console.log('NO BINDING',where);return;}
  if(b.path.isVariableDeclarator()){ const init=b.path.node.init?src.slice(b.path.node.init.start,b.path.node.init.end):''; if(!/useT\(|useLang\(|useContext\(/.test(init)){problems++;console.log('SHADOW',where,init.slice(0,40));return;}
    const sameFn=b.scope.getFunctionParent()===p.scope.getFunctionParent()||b.scope===p.scope.getFunctionParent()?.scope; 
    if(b.path.node.start>p.node.start && b.scope.block===p.getFunctionParent()?.node){problems++;console.log('TDZ',where);} }
  else if(!(b.kind==='param')){problems++;console.log('ODD',where,b.kind);} }});}
console.log('t() calls:',calls,'| problems:',problems,'| keys used without fallback and missing in dict:',missing.size); [...missing].slice(0,20).forEach(m=>console.log('  ',m));
