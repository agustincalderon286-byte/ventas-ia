(() => {
  const name = 'tec_coach_local_lock';
  const locked = () => document.cookie.split(';').some(value => value.trim() === name + '=1');
  const privatePage = () => Boolean(document.querySelector('[data-coach-app-page]'));
  function leave() {
    if (privatePage()) {
      document.querySelector('[data-coach-app-page]').hidden = true;
      window.location.replace('/coach/login/');
    }
  }
  function lock() {
    document.cookie = name + '=1; Path=/; Max-Age=31536000; SameSite=Lax' +
      (location.protocol === 'https:' ? '; Secure' : '');
    if (!locked()) throw Error('Activa las cookies para bloquear este dispositivo.');
    try { localStorage.setItem(name, crypto.randomUUID()); } catch {}
  }
  async function revoke() {
    if (!locked() || !navigator.onLine) return;
    try {
      await fetch('/api/coach/session/revoke-local', {method:'POST', credentials:'same-origin',
        headers:{'Content-Type':'application/json'}, body:'{}', keepalive:true});
    } catch { /* The lock persists and the next online visit retries. */ }
  }
  window.TecSessionLock = {locked, lock, revoke};
  window.addEventListener('storage', event => { if (event.key === name) leave(); });
  document.addEventListener('visibilitychange', () => { if (!document.hidden && locked()) leave(); });
  window.addEventListener('online', revoke);
  window.addEventListener('pageshow', event => {
    if (locked()) { leave(); void revoke(); }
    else if (event.persisted && privatePage()) location.reload();
  });
  if (locked()) { leave(); void revoke(); }
})();
