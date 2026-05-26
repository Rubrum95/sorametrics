(function () {
  const btn = document.getElementById('nav-hamburger');
  if (!btn) return;

  btn.addEventListener('click', () => {
    const open = document.body.classList.toggle('nav-open');
    btn.setAttribute('aria-expanded', String(open));
  });

  document.querySelectorAll('.nav__link').forEach((a) => {
    a.addEventListener('click', () => {
      document.body.classList.remove('nav-open');
      btn.setAttribute('aria-expanded', 'false');
    });
  });
})();
