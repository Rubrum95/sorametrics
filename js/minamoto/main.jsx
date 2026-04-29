/* global window, React, ReactDOM */
// ============================================================
// js/minamoto/main.jsx
// Boot. Mounts MN.Router into <div id="root">.
// All other modules attach to window.MN. This must load LAST.
// ============================================================

(function () {
  const mount = document.getElementById('root');
  if (!mount) {
    console.error('[minamoto] #root not found');
    return;
  }
  if (!window.MN || !window.MN.Router) {
    console.error('[minamoto] MN.Router missing — module load order broken');
    const err = document.createElement('div');
    err.style.cssText = 'padding:32px;color:#EF4444';
    err.textContent = 'Failed to boot: missing modules. Check the network tab.';
    mount.replaceChildren(err);
    return;
  }
  const root = ReactDOM.createRoot(mount);
  root.render(<window.MN.Router />);
})();
