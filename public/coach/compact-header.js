(() => {
  'use strict';
  const root = document.documentElement;
  const header = document.querySelector('[data-visit-header]');
  function shouldStartAtFirstStep() {
    const navigation = window.performance?.getEntriesByType?.('navigation')?.[0];
    return root.dataset.coachPortalMode !== 'telemarketing' && !window.location.hash && navigation?.type !== 'back_forward';
  }
  function visitAnchor() {
    if (root.dataset.coachPortalMode === 'telemarketing') return null;
    const id = window.location.hash.slice(1);
    return /^(paso-[a-z0-9-]+|prueba-agua)$/.test(id) ? document.getElementById(id) : null;
  }
  function initialWorkspace(saved, defaultTab) {
    return shouldStartAtFirstStep() || visitAnchor() ? defaultTab : saved || defaultTab;
  }
  function measure() {
    const height = header?.getBoundingClientRect().height || 0;
    if (height) root.style.setProperty('--tec-visit-header-height', `${Math.ceil(height) + 12}px`);
  }
  function ready() {
    measure();
    const frame = window.requestAnimationFrame || (callback => setTimeout(callback, 0));
    const anchor = visitAnchor();
    if (anchor || shouldStartAtFirstStep()) frame(() => frame(() => {
      if (anchor) anchor.scrollIntoView({block:'start',behavior:'instant'});
      else window.scrollTo({top:0,left:0,behavior:'instant'});
    }));
  }
  const menu = document.querySelector('[data-visit-account-menu]');
  document.addEventListener('click', event => { if (menu?.open && !menu.contains(event.target)) menu.open = false; });
  document.addEventListener('keydown', event => {
    if (event.key === 'Escape' && menu?.open) { menu.open = false; menu.querySelector('summary').focus({preventScroll:true}); }
  });
  if (header && typeof ResizeObserver !== 'undefined') new ResizeObserver(measure).observe(header);
  window.addEventListener('resize', measure);
  window.TecVisitHeader = Object.freeze({shouldStartAtFirstStep, initialWorkspace, ready});
})();
