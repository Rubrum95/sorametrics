// js/analytics.jsx — SoraMetrics site-analytics beacon client.
// Plain JS (no JSX/React) but named .jsx and loaded as text/babel, because the
// static middleware only serves /js/*.jsx. Loaded before main.jsx. Everything
// is fire-and-forget via navigator.sendBeacon, so it never blocks render,
// navigation, or unload. Initialised in requestIdle to stay off the critical
// render path.
(function () {
  'use strict';
  var ENDPOINT = '/analytics/hit';
  var HEARTBEAT_MS = 30000;

  // Ephemeral per-tab session id (cleared when the tab closes).
  function sessionId() {
    try {
      var k = '__sm_sid__';
      var v = sessionStorage.getItem(k);
      if (!v) {
        v = Date.now().toString(36) + Math.random().toString(36).slice(2, 10);
        sessionStorage.setItem(k, v);
      }
      return v;
    } catch (_) { return null; }
  }
  var SID = sessionId();

  function send(payload) {
    try {
      payload.sid = SID;
      var body = JSON.stringify(payload);
      if (navigator.sendBeacon) {
        navigator.sendBeacon(ENDPOINT, new Blob([body], { type: 'application/json' }));
      } else {
        fetch(ENDPOINT, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: body, keepalive: true }).catch(function () {});
      }
    } catch (_) { /* tracking must never throw into the app */ }
  }

  var track = {
    pageview: function () { send({ type: 'pageview', path: location.pathname + location.search, ref: document.referrer || null }); },
    section: function (id) { if (id) send({ type: 'section', section: String(id) }); },
    interaction: function (name, meta) { var m = meta || {}; m.name = name; send({ type: 'interaction', meta: m }); },
    search: function (q) { if (q) send({ type: 'search', meta: { q: String(q).slice(0, 80) } }); },
    event: function (type, meta) { send({ type: type, meta: meta || null }); },
  };
  window.__SM_TRACK__ = track;

  // Capture client JS errors (capped per page-load) to surface real prod bugs.
  var errCount = 0;
  function reportError(msg, src, line) {
    if (errCount >= 10) return;
    errCount++;
    send({ type: 'error', meta: { msg: String(msg || '').slice(0, 200), src: String(src || '').slice(0, 120), line: line || 0 } });
  }
  window.addEventListener('error', function (e) { reportError(e.message, e.filename, e.lineno); });
  window.addEventListener('unhandledrejection', function (e) { reportError((e.reason && (e.reason.message || e.reason)) || 'unhandledrejection', 'promise', 0); });

  // Heartbeat only while the tab is visible — no waste in backgrounded tabs.
  function beat() { if (document.visibilityState === 'visible') send({ type: 'heartbeat' }); }

  // Web Vitals: TTFB + FCP from timing, LCP via observer. One beacon when the
  // page settles (first hide or after 8s) so LCP has stabilised.
  var lcp = null, vitalsSent = false;
  try {
    if ('PerformanceObserver' in window) {
      var po = new PerformanceObserver(function (list) { var es = list.getEntries(); if (es.length) lcp = es[es.length - 1].startTime; });
      po.observe({ type: 'largest-contentful-paint', buffered: true });
    }
  } catch (_) {}
  function flushVitals() {
    if (vitalsSent) return; vitalsSent = true;
    try {
      var nav = performance.getEntriesByType('navigation')[0];
      var fcpE = performance.getEntriesByType('paint').filter(function (p) { return p.name === 'first-contentful-paint'; })[0];
      var m = {
        ttfb: nav ? Math.round(nav.responseStart) : null,
        fcp: fcpE ? Math.round(fcpE.startTime) : null,
        lcp: lcp != null ? Math.round(lcp) : null,
      };
      if (m.ttfb != null || m.fcp != null || m.lcp != null) send({ type: 'vitals', meta: m });
    } catch (_) {}
  }

  function start() {
    track.pageview();
    beat();
    setInterval(beat, HEARTBEAT_MS);
    document.addEventListener('visibilitychange', function () {
      if (document.visibilityState === 'hidden') flushVitals();
      else beat();
    });
    setTimeout(flushVitals, 8000);
    // PWA usage signal (installed app launches in standalone display mode).
    try {
      if ((window.matchMedia && window.matchMedia('(display-mode: standalone)').matches) || navigator.standalone) {
        track.interaction('pwa_standalone', {});
      }
    } catch (_) {}
  }

  if ('requestIdleCallback' in window) requestIdleCallback(start, { timeout: 3000 });
  else if (document.readyState === 'complete') setTimeout(start, 0);
  else window.addEventListener('load', function () { setTimeout(start, 0); });
})();
