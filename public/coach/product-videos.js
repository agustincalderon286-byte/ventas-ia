(() => {
  'use strict';
  const catalog = globalThis.TecProductVideoCatalog;
  if (!catalog) return;
  let dialog, heading, subtitle, search, category, kind, grid, count, player, status, retry;
  let activeVideo = null, available = [], opener = null, previousOverflow = '', origin = '';
  let viewerActive = false;
  const node = (tag, className, text) => {
    const result = document.createElement(tag);
    if (className) result.className = className;
    if (text) result.textContent = text;
    return result;
  };
  function stopPlayback() {
    // Removing the frame also stops sound, including when closing with Escape.
    player.replaceChildren();
    player.hidden = true;
  }
  function finishClose() {
    // A delayed close event from the previous opening must not affect a new one.
    if (!viewerActive || dialog.open) return;
    viewerActive = false;
    stopPlayback(); activeVideo = null;
    document.body.style.overflow = previousOverflow;
    if (opener?.isConnected) opener.focus({ preventScroll: true });
    opener = null;
  }
  function closeDialog() {
    if (!dialog.open) return;
    stopPlayback();
    dialog.close();
    finishClose();
  }
  function play(video) {
    activeVideo = video;
    for (const button of grid.querySelectorAll('[data-tec-video-id]')) {
      button.setAttribute('aria-pressed', String(button.dataset.tecVideoId === video.id));
    }
    stopPlayback();
    retry.hidden = false;
    if (navigator.onLine === false) {
      status.textContent = 'Necesitas internet para ver los videos. Puedes cerrar y seguir usando el Coach sin perder tus datos.';
      return;
    }
    const source = catalog.embedUrl(video.id);
    if (!source) { status.textContent = 'Este video no está disponible.'; return; }
    const frame = node('iframe');
    frame.title = video.title;
    frame.src = source;
    frame.allow = 'encrypted-media; picture-in-picture; fullscreen';
    frame.allowFullscreen = true;
    frame.referrerPolicy = 'strict-origin-when-cross-origin';
    player.append(frame);
    player.hidden = false;
    status.textContent = `${video.title}. Pulsa reproducir. Si no carga, comprueba tu conexión y toca Reintentar video.`;
    dialog.querySelector('[data-tec-video-body]').scrollTop = 0;
  }
  function renderList() {
    const query = catalog.normalize(search.value);
    const selected = available.filter(video => (!query || catalog.normalize(
      `${video.product} ${video.title} ${video.category} ${video.category === 'Licuadoras' ? 'licuadora batidora' : ''}`
    ).includes(query)) && (!category.value || video.category === category.value) && (!kind.value || video.type === kind.value));
    count.textContent = `${selected.length} ${selected.length === 1 ? 'video disponible' : 'videos disponibles'}`;
    grid.replaceChildren();
    if (!selected.length) grid.append(node('p', 'tec-video-empty', 'No hay videos con ese filtro. Prueba otro nombre.'));
    for (const video of selected) {
      const card = node('article', 'tec-video-card');
      const button = node('button', 'tec-video-poster'); button.type = 'button';
      button.dataset.tecVideoId = video.id;
      button.setAttribute('aria-label', `Ver ${video.title}`);
      button.setAttribute('aria-pressed', String(activeVideo?.id === video.id));
      if (navigator.onLine !== false && catalog.thumbnailUrl(video.id)) {
        const image = node('img'); image.src = catalog.thumbnailUrl(video.id);
        image.alt = ''; image.loading = 'lazy'; image.width = 480; image.height = 360;
        image.addEventListener('error', () => { image.hidden = true; });
        button.append(image);
      }
      const icon = node('span', 'tec-video-play', '▶'); icon.setAttribute('aria-hidden', 'true'); button.append(icon);
      if (video.duration) button.append(node('span', 'tec-video-duration', video.duration));
      button.addEventListener('click', () => play(video));
      card.append(button, node('p', 'tec-video-type', video.type), node('h3', '', video.product), node('p', 'tec-video-title', video.title));
      grid.append(card);
    }
  }
  function createDialog() {
    dialog = node('dialog', 'tec-video-dialog');
    dialog.setAttribute('aria-labelledby', 'tec-video-heading');
    const header = node('header', 'tec-video-header');
    const titles = node('div'); heading = node('h2'); heading.id = 'tec-video-heading';
    subtitle = node('p'); titles.append(heading, subtitle);
    const close = node('button', 'tec-video-close', '✕ Cerrar y volver'); close.type = 'button'; close.autofocus = true;
    close.setAttribute('data-tec-video-close', ''); close.addEventListener('click', closeDialog);
    header.append(titles, close);
    const body = node('div', 'tec-video-body'); body.setAttribute('data-tec-video-body', '');
    const note = node('p', 'tec-video-note', 'Videos oficiales de Royal Prestige · Requieren internet. Confirma que el modelo mostrado coincida con tu producto.');
    player = node('div', 'tec-video-player'); player.hidden = true;
    status = node('p', 'tec-video-status'); status.setAttribute('role', 'status');
    retry = node('button', 'nav-button', 'Reintentar video'); retry.type = 'button'; retry.hidden = true;
    retry.addEventListener('click', () => { if (activeVideo) play(activeVideo); });
    const filters = node('div', 'tec-video-filters');
    search = node('input'); search.type = 'search'; search.placeholder = 'Buscar producto o video'; search.setAttribute('aria-label', 'Buscar videos de productos');
    category = node('select'); category.setAttribute('aria-label', 'Categoría de videos');
    category.add(new Option('Todas las categorías', ''));
    for (const label of [...new Set(catalog.videos.map(video => video.category))]) category.add(new Option(label, label));
    kind = node('select'); kind.setAttribute('aria-label', 'Tipo de video'); kind.add(new Option('Todos los tipos', ''));
    for (const label of ['Presentación', 'Uso y cuidados']) kind.add(new Option(label, label));
    search.addEventListener('input', renderList); category.addEventListener('change', renderList); kind.addEventListener('change', renderList);
    filters.append(search, category, kind);
    count = node('p', 'tec-video-count'); count.setAttribute('role', 'status');
    grid = node('div', 'tec-video-grid');
    body.append(note, player, status, retry, filters, count, grid); dialog.append(header, body); document.body.append(dialog);
    dialog.addEventListener('close', finishClose);
    dialog.addEventListener('cancel', event => { event.preventDefault(); closeDialog(); });
    window.addEventListener('coach:workspace-change', closeDialog);
    window.addEventListener('pagehide', closeDialog);
    window.addEventListener('offline', () => {
      if (!dialog.open) return;
      stopPlayback();
      status.textContent = 'Se perdió la conexión. Los datos de tu encuesta y calculadora se conservan. Reintenta el video cuando vuelva internet.';
    });
    window.addEventListener('online', () => {
      if (dialog.open && !player.querySelector('iframe')) {
        status.textContent = 'La conexión regresó. Elige un video o toca Reintentar video.';
        renderList();
      }
    });
  }
  function open({ product = null, button = null, returnTo = 'los catálogos' } = {}) {
    const videos = product ? catalog.forProduct(product) : catalog.videos;
    if (!videos.length) return;
    if (!dialog) createDialog();
    if (dialog.open) return;
    available = [...videos].sort((a, b) => (a.type === 'Presentación' ? 0 : 1) - (b.type === 'Presentación' ? 0 : 1));
    opener = button; origin = returnTo;
    heading.textContent = product ? `Videos · ${product.nombre_producto || product.name || 'Producto'}` : 'Videos de productos';
    subtitle.textContent = `Al cerrar vuelves a ${origin}.`;
    dialog.querySelector('[data-tec-video-close]').setAttribute('aria-label', `Cerrar videos y volver a ${origin}`);
    search.value = ''; category.value = ''; kind.value = '';
    activeVideo = null; retry.hidden = true; stopPlayback();
    status.textContent = navigator.onLine === false ? 'Necesitas internet para reproducir los videos. Puedes seguir usando el Coach sin perder tus datos.' : 'Elige un video para verlo aquí.';
    renderList(); previousOverflow = document.body.style.overflow;
    dialog.showModal(); document.body.style.overflow = 'hidden';
    viewerActive = true;
    if (product) play(available[0]);
    dialog.querySelector('[data-tec-video-body]').scrollTop = 0;
  }
  function mount(container, product, { origin = 'la calculadora' } = {}) {
    if (!container) return;
    container.replaceChildren();
    const videos = catalog.forProduct(product);
    container.hidden = !videos.length;
    if (!videos.length) return;
    const button = node('button', 'secondary-button tec-product-video-button', `▶ Ver videos (${videos.length})`);
    button.type = 'button'; button.setAttribute('aria-haspopup', 'dialog');
    button.setAttribute('aria-label', `Ver videos de ${product.nombre_producto || product.name || 'este producto'}`);
    button.addEventListener('click', event => { event.preventDefault(); open({ product, button, returnTo: origin }); });
    container.append(button);
  }
  document.addEventListener('click', event => {
    const button = event.target.closest?.('[data-product-video-library]');
    if (button) { event.preventDefault(); open({ button }); }
  });
  window.TecProductVideos = Object.freeze({ mount });
})();
