// Static frontend from disk + everything else proxied to production API (read-only GET/POST passthrough).
const http = require('http'), https = require('https'), fs = require('fs'), path = require('path');
const ROOT = require('path').join(__dirname, '..', 'frontend');
const MIME = { '.html':'text/html', '.js':'text/javascript', '.jsx':'text/babel', '.css':'text/css', '.svg':'image/svg+xml', '.json':'application/json', '.jpg':'image/jpeg', '.png':'image/png' };
http.createServer((req, res) => {
  const u = new URL(req.url, 'http://x'); let p = decodeURIComponent(u.pathname);
  if (p === '/' || p === '/sorav2') p = '/index.html';
  const file = path.join(ROOT, p);
  if (file.startsWith(ROOT) && fs.existsSync(file) && fs.statSync(file).isFile()) {
    res.writeHead(200, { 'content-type': MIME[path.extname(file)] || 'application/octet-stream', 'cache-control': 'no-store' });
    return fs.createReadStream(file).pipe(res);
  }
  const up = https.request({ host: 'sorametrics.org', path: req.url, method: req.method, headers: { ...req.headers, host: 'sorametrics.org' } }, r => { res.writeHead(r.statusCode, r.headers); r.pipe(res); });
  up.on('error', e => { res.writeHead(502); res.end(String(e)); }); req.pipe(up);
}).listen(8811, () => console.log('v33 preview on 8811'));
