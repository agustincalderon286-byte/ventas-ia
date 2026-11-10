(() => {
  'use strict';
  const tabs = [...document.querySelectorAll('.library-tabs [role="tab"]')];
  const books = document.querySelector('#libros');
  if (!tabs.length || !books) return;
  const audioSections = [...document.querySelectorAll('main > section')].filter(section => section !== books);
  const switcher = document.querySelector('.library-tabs');
  function activate(hash, scroll = false) {
    const reading = hash === '#libros';
    books.hidden = !reading;
    audioSections.forEach(section => { section.hidden = reading; });
    document.body.dataset.libraryView = reading ? 'books' : 'audio';
    for (const tab of tabs) {
      const active = tab.hash === (reading ? '#libros' : '#catalogo');
      tab.setAttribute('aria-selected', String(active));
      tab.tabIndex = active ? 0 : -1;
    }
    for (const link of document.querySelectorAll('.top nav a,.phone-nav a')) {
      if (link.hash === '#libros' || link.hash === '#catalogo') {
        if (link.hash === (reading ? '#libros' : '#catalogo')) link.setAttribute('aria-current', 'page');
        else link.removeAttribute('aria-current');
      }
    }
    if (scroll) switcher.scrollIntoView({ block: 'start', behavior: 'instant' });
  }
  function navigate(hash) {
    if (location.hash !== hash) history.pushState(null, '', hash);
    activate(hash, true);
  }
  document.addEventListener('click', event => {
    const link = event.target.closest('a[href="#catalogo"],a[href="#libros"]');
    if (!link || event.defaultPrevented || event.button || event.metaKey || event.ctrlKey || event.shiftKey || event.altKey) return;
    event.preventDefault();
    navigate(link.hash);
  });
  tabs.forEach((tab, index) => tab.addEventListener('keydown', event => {
    let next;
    if (event.key === 'ArrowRight') next = (index + 1) % tabs.length;
    else if (event.key === 'ArrowLeft') next = (index + tabs.length - 1) % tabs.length;
    else if (event.key === 'Home') next = 0;
    else if (event.key === 'End') next = tabs.length - 1;
    else return;
    event.preventDefault();
    navigate(tabs[next].hash);
    tabs[next].focus({ preventScroll: true });
  }));
  function followLocation() {
    activate(location.hash);
    const target = document.getElementById(location.hash.slice(1));
    if (target) target.scrollIntoView({ block: 'start', behavior: 'instant' });
  }
  window.addEventListener('hashchange', followLocation);
  window.addEventListener('popstate', followLocation);
  activate(location.hash);
  if (location.hash === '#libros') switcher.scrollIntoView({ block: 'start', behavior: 'instant' });
})();
