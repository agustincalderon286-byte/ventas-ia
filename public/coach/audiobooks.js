(() => {
  'use strict';
  const root = document.querySelector('[data-audiobooks]');
  if (!root) return;
  const $ = selector => root.querySelector(selector);
  const audio = $('[data-ab-audio]');
  const reader = new TecBookReader.Reader($('[data-ab-reader]'), audio);
  const state = { loaded: false, busy: false, book: null, page: 1, generation: 0, saveAt: 0, saveQueue: Promise.resolve() };
  const time = seconds => { const n = Math.max(0, Math.floor(Number(seconds) || 0)); return `${Math.floor(n / 60)}:${String(n % 60).padStart(2, '0')}`; };
  const el = (tag, text, className) => { const node = document.createElement(tag); if (text !== undefined) node.textContent = text; if (className) node.className = className; return node; };
  const status = (selector, message) => { $(selector).textContent = message; };
  async function api(url, options = {}) {
    const response = await fetch(url, { credentials: 'same-origin', cache: 'no-store', ...options,
      headers: { ...(options.body ? { 'Content-Type': 'application/json' } : {}), ...options.headers } });
    const data = await response.json().catch(() => ({}));
    if (!response.ok) {
      if ([401, 403].includes(response.status)) { audio.pause(); audio.removeAttribute('src'); audio.load(); }
      throw new Error(data.error || (response.status === 401 ? 'Tu sesión terminó. Vuelve a entrar a TEC.' : 'No pude completar la solicitud. Inténtalo de nuevo.'));
    }
    return data;
  }
  async function loadCatalog() {
    if (state.loaded || state.busy) return;
    state.busy = true;
    status('[data-ab-catalog-status]', 'Cargando tu biblioteca…');
    try {
      const { books } = await api('/api/coach/audiobooks');
      const list = $('[data-ab-catalog]'); list.replaceChildren();
      for (const book of books) {
        const card = el('article', undefined, 'ab-book-card');
        const image = el('img'); image.src = book.cover; image.alt = `Portada de ${book.title}`; image.width = 180; image.height = 240;
        const body = el('div', undefined, 'ab-card-body');
        body.append(el('span', book.categories.join(' · '), 'ab-eyebrow'), el('h3', book.title), el('p', book.author, 'ab-author'), el('p', book.description), el('p', `${time(book.duration)} · ${book.language} · Libro íntegro`, 'ab-meta'));
        const button = el('button', book.available ? 'Escuchar y leer' : 'Próximamente', 'primary-button'); button.type = 'button'; button.disabled = !book.available;
        button.addEventListener('click', () => openBook(book.id));
        body.append(button); card.append(image, body); list.append(card);
      }
      status('[data-ab-catalog-status]', books.length ? `${books.length} ${books.length === 1 ? 'audiolibro disponible' : 'audiolibros en tu biblioteca'}` : 'Pronto encontrarás nuevos audiolibros aquí.');
      $('[data-ab-retry]').hidden = true; state.loaded = true;
    } catch (error) { status('[data-ab-catalog-status]', error.message); $('[data-ab-retry]').hidden = false; }
    finally { state.busy = false; }
  }
  function saveProgress() {
    if (!state.book || !audio.getAttribute('src') || !Number.isFinite(audio.currentTime)) return Promise.resolve();
    const id = state.book.id, body = JSON.stringify({ position: Math.min(audio.currentTime, state.book.duration), speed: audio.playbackRate });
    state.saveAt = Date.now();
    state.saveQueue = state.saveQueue.catch(() => {}).then(() => api(`/api/coach/audiobooks/${id}/progress`, { method: 'PUT', body, keepalive: true }))
      .then(() => { if (state.book?.id === id) status('[data-ab-save-status]', 'Tu avance se guardó en tu cuenta.'); })
      .catch(() => { if (state.book?.id === id) status('[data-ab-save-status]', 'No pude guardar tu avance. Revisa tu conexión.'); });
    return state.saveQueue;
  }
  async function openBook(id) {
    audio.pause(); await saveProgress(); reader.clear();
    const generation = ++state.generation;
    status('[data-ab-catalog-status]', 'Abriendo audiolibro…');
    try {
      const data = await api(`/api/coach/audiobooks/${encodeURIComponent(id)}`);
      if (generation !== state.generation) return;
      const book = data.book; state.book = book; state.page = 1;
      $('[data-ab-library]').hidden = true; $('[data-ab-detail]').hidden = false;
      $('[data-ab-title]').textContent = book.title; $('[data-ab-title]').focus({ preventScroll: true });
      $('[data-ab-author]').textContent = book.author;
      $('[data-ab-cover]').src = book.cover; $('[data-ab-cover]').alt = `Portada de ${book.title}`;
      $('[data-ab-description]').textContent = book.description;
      $('[data-ab-meta]').textContent = `${time(book.duration)} · ${book.language} · ${book.chapters.length} partes · Edición íntegra`;
      $('[data-ab-voice]').textContent = book.narrator || 'Producción TEC';
      $('[data-ab-credits]').textContent = book.credits;
      $('[data-ab-speed]').value = String(data.progress.speed);
      status('[data-ab-player-status]', 'Listo para escuchar.'); status('[data-ab-save-status]', data.progress.position > 0 ? `Continuarás desde ${time(data.progress.position)}.` : 'Tu avance se guarda mientras escuchas.');
      audio.removeAttribute('src'); audio.load();
      audio.onloadedmetadata = () => {
        if (generation !== state.generation) return;
        audio.currentTime = Math.min(data.progress.position, Math.max(0, book.duration - 0.1));
        audio.playbackRate = data.progress.speed;
      };
      audio.src = book.audioUrl; audio.load();
      const chapters = $('[data-ab-chapters]'); chapters.replaceChildren();
      book.chapters.forEach((chapter, index) => {
        const item = el('li'); const button = el('button', undefined, 'ab-chapter'); button.type = 'button'; button.dataset.chapter = index;
        button.append(el('span', chapter.title), el('span', time(chapter.start), 'ab-chapter-time'));
        button.addEventListener('click', () => { audio.currentTime = chapter.start; audio.play().catch(() => status('[data-ab-player-status]', 'Toca reproducir para escuchar.')); });
        item.append(button); chapters.append(item);
      });
      loadText(book, generation); updateRating(data.rating); await loadReviews();
      if ('mediaSession' in navigator && 'MediaMetadata' in window) {
        navigator.mediaSession.metadata = new MediaMetadata({ title: book.title, artist: book.author, album: 'Audiolibros TEC', artwork: [{ src: new URL(book.cover, location.href).href, type: 'image/svg+xml', sizes: '600x800' }] });
        for (const [action, handler] of Object.entries({ play: () => audio.play().catch(() => {}), pause: () => audio.pause(), seekbackward: () => seek(-15), seekforward: () => seek(15), seekto: detail => { audio.currentTime = detail.seekTime; } })) {
          try { navigator.mediaSession.setActionHandler(action, handler); } catch {}
        }
      }
    } catch (error) { status('[data-ab-catalog-status]', error.message); status('[data-ab-player-status]', error.message); }
  }
  async function loadText(book, generation) {
    reader.clear();
    if (!book.transcriptUrl) return;
    $('[data-ab-reader]').hidden = false;
    status('[data-ab-read-status]', 'Abriendo el libro…'); $('[data-ab-read-retry]').hidden = true;
    try {
      const text = await api(book.transcriptUrl);
      if (generation !== state.generation || state.book?.id !== book.id) return;
      reader.set(text); status('[data-ab-read-status]', 'Toca un pasaje para escucharlo.');
    } catch (error) {
      if (generation !== state.generation) return;
      status('[data-ab-read-status]', 'No se pudo abrir el texto. Revisa tu conexión.'); $('[data-ab-read-retry]').hidden = false;
    }
  }
  $('[data-ab-read-retry]').addEventListener('click', () => { if (state.book) loadText(state.book, state.generation); });
  function updateRating(rating) {
    $('[data-ab-rating]').textContent = rating.count ? `★ ${rating.average.toFixed(1)} de 5 · ${rating.count} ${rating.count === 1 ? 'reseña' : 'reseñas'}` : 'Sé la primera persona en calificar este audiolibro.';
  }
  async function loadReviews() {
    const id = state.book?.id; if (!id) return;
    status('[data-ab-review-status]', 'Cargando reseñas…');
    try {
      const data = await api(`/api/coach/audiobooks/${id}/reviews?page=${state.page}`);
      if (state.book?.id !== id) return;
      updateRating(data.rating);
      const list = $('[data-ab-reviews]'); list.replaceChildren();
      for (const review of data.reviews) {
        const row = el('article', undefined, 'ab-review');
        row.append(el('h4', `${review.author}${review.mine ? ' · Tú' : ''}`), el('p', `${'★'.repeat(review.rating)}${'☆'.repeat(5 - review.rating)}`, 'ab-stars'));
        if (review.text) row.append(el('p', review.text, 'ab-review-text'));
        row.append(el('small', new Date(review.updatedAt).toLocaleDateString('es-MX', { year: 'numeric', month: 'short', day: 'numeric' })));
        list.append(row);
      }
      const form = $('[data-ab-review-form]'); form.reset();
      if (data.mine) { form.elements.rating.value = String(data.mine.rating); form.elements.text.value = data.mine.text; }
      $('[data-ab-delete-review]').hidden = !data.mine;
      $('[data-ab-save-review]').textContent = data.mine ? 'Actualizar mi reseña' : 'Publicar mi reseña';
      $('[data-ab-prev-reviews]').hidden = state.page <= 1; $('[data-ab-next-reviews]').hidden = !data.hasMore;
      status('[data-ab-review-status]', data.reviews.length ? `Página ${state.page}` : 'Todavía no hay reseñas. Comparte tu opinión.');
    } catch (error) { status('[data-ab-review-status]', error.message); }
  }
  function seek(seconds) { if (state.book) audio.currentTime = Math.max(0, Math.min(state.book.duration, audio.currentTime + seconds)); }
  $('[data-ab-review-form]').addEventListener('submit', async event => {
    event.preventDefault(); const form = event.currentTarget;
    if (!form.reportValidity() || !state.book) return;
    const submit = $('[data-ab-save-review]'); submit.disabled = true;
    status('[data-ab-form-status]', 'Guardando tu reseña…');
    try {
      await api(`/api/coach/audiobooks/${state.book.id}/review`, { method: 'PUT', body: JSON.stringify({ rating: Number(form.elements.rating.value), text: form.elements.text.value }) });
      state.page = 1; await loadReviews(); status('[data-ab-form-status]', 'Tu reseña se publicó. Puedes editarla o eliminarla cuando quieras.');
    } catch (error) { status('[data-ab-form-status]', error.message); }
    finally { submit.disabled = false; }
  });
  $('[data-ab-delete-review]').addEventListener('click', async () => {
    if (!state.book || !window.confirm('¿Eliminar tu reseña de este audiolibro?')) return;
    const button = $('[data-ab-delete-review]'); button.disabled = true;
    try { await api(`/api/coach/audiobooks/${state.book.id}/review`, { method: 'DELETE' }); state.page = 1; await loadReviews(); status('[data-ab-form-status]', 'Tu reseña se eliminó.'); }
    catch (error) { status('[data-ab-form-status]', error.message); }
    finally { button.disabled = false; }
  });
  $('[data-ab-prev-reviews]').addEventListener('click', () => { state.page--; loadReviews(); });
  $('[data-ab-next-reviews]').addEventListener('click', () => { state.page++; loadReviews(); });
  $('[data-ab-back]').addEventListener('click', () => { audio.pause(); $('[data-ab-detail]').hidden = true; $('[data-ab-library]').hidden = false; status('[data-ab-catalog-status]', 'Tu biblioteca'); });
  $('[data-ab-retry]').addEventListener('click', loadCatalog);
  $('[data-ab-backward]').addEventListener('click', () => seek(-15)); $('[data-ab-forward]').addEventListener('click', () => seek(15));
  $('[data-ab-speed]').addEventListener('change', event => { audio.playbackRate = Number(event.target.value); saveProgress(); });
  audio.addEventListener('timeupdate', () => {
    if (!state.book) return;
    const chapters = state.book.chapters; let active = 0;
    for (let i = 0; i < chapters.length; i++) if (audio.currentTime >= chapters[i].start) active = i;
    root.querySelectorAll('[data-chapter]').forEach((button, index) => { if (index === active) button.setAttribute('aria-current', 'true'); else button.removeAttribute('aria-current'); });
    $('[data-ab-now-chapter]').textContent = chapters[active].title;
    if (!audio.paused && Date.now() - state.saveAt > 15000) saveProgress();
  });
  audio.addEventListener('play', () => status('[data-ab-player-status]', 'Reproduciendo'));
  audio.addEventListener('pause', () => { status('[data-ab-player-status]', 'En pausa'); saveProgress(); });
  audio.addEventListener('ended', () => { status('[data-ab-player-status]', 'Libro terminado'); saveProgress(); });
  audio.addEventListener('error', () => { if (audio.getAttribute('src')) status('[data-ab-player-status]', 'No pude cargar el audio. Revisa tu conexión y vuelve a abrir el libro.'); });
  window.addEventListener('pagehide', saveProgress);
  document.addEventListener('visibilitychange', () => { if (document.hidden) saveProgress(); });
  document.querySelectorAll('[data-coach-logout], [data-coach-return-session]').forEach(button => button.addEventListener('click', () => { audio.pause(); state.book = null; state.generation++; audio.removeAttribute('src'); audio.load(); reader.clear(); }));
  window.addEventListener('coach:workspace-change', event => { if (event.detail?.workspace === 'audiolibros') loadCatalog(); });
  window.addEventListener('hashchange', () => { if (location.hash === '#audiolibros') document.querySelector('[data-coach-workspace-tab="audiolibros"]')?.click(); });
  if (!root.hidden) loadCatalog();
})();
