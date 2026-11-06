(() => {
  'use strict';
  const $ = selector => document.querySelector(selector);
  const audio = $('#audio');
  const player = $('#player');
  const accountDialog = $('#account');
  const apiRoot = '/api/audiolibros';
  let account = {};
  let books = [];
  let selected = null;
  let generation = 0;
  let saveQueue = Promise.resolve();
  let lastSave = 0;
  let timerHandle;
  let deadline = 0;
  let stopAt = 0;
  let signInToken = new URLSearchParams(location.hash.slice(1)).get('entrar');
  const reader = new TecBookReader.Reader($('[data-ab-reader]'), audio, position => seek(position, true));

  function element(tag, text, className) {
    const node = document.createElement(tag);
    if (text !== undefined) node.textContent = text;
    if (className) node.className = className;
    return node;
  }

  function button(text, action, style = 'outline') {
    const node = element('button', text, 'button ' + style);
    node.type = 'button';
    node.addEventListener('click', action);
    return node;
  }

  function time(seconds) {
    const whole = Math.max(0, Math.floor(Number(seconds) || 0));
    const [hours, minutes, remainder] = [Math.floor(whole / 3600), Math.floor(whole / 60) % 60, whole % 60];
    return [hours || null, hours ? String(minutes).padStart(2, '0') : minutes, String(remainder).padStart(2, '0')]
      .filter(part => part !== null).join(':');
  }

  async function api(path, method = 'GET', body) {
    const response = await fetch(apiRoot + path, {
      method, cache: 'no-store', credentials: 'same-origin', keepalive: method === 'PUT',
      ...(body ? { body: JSON.stringify(body), headers: { 'Content-Type': 'application/json' } } : {})
    });
    const data = await response.json().catch(() => ({}));
    if (!response.ok) throw new Error(data.error || 'No se pudo completar la solicitud. Inténtalo de nuevo.');
    return data;
  }

  function renderBooks() {
    const cards = books.map(book => {
      const card = element('article', undefined, 'book');
      const image = element('img');
      Object.assign(image, { src: book.cover, alt: '', width: 600, height: 800 });
      const copy = element('div');
      copy.append(
        element('p', book.categories.join(' · '), 'eyebrow'), element('h3', book.title),
        element('p', book.author, 'author'), element('p', book.description),
        element('p', book.language + ' · ' + time(book.duration))
      );
      const actions = element('div', undefined, 'actions');
      if (account.member) actions.append(button('Escuchar y leer', () => openBook(book, false), 'primary'));
      if (book.previewUrl) actions.append(button('▶ Muestra · ' + time(book.previewDuration), () => openBook(book, true)));
      copy.append(actions);
      card.append(image, copy);
      return card;
    });
    $('#books').replaceChildren(...cards);
  }

  async function refreshAccount() {
    account = await api('/cuenta');
    $('#account-button').textContent = account.authenticated ? 'Mi cuenta' : 'Ingresar';
    $('#subscribe').textContent = account.member ? 'Ir al catálogo' : 'Suscribirme';
    $('#billing-status').textContent = account.billingReady && account.signInReady ? '' : 'La activación de nuevas suscripciones está pendiente.';
    renderBooks();
  }

  function showAccount() {
    const confirming = Boolean(signInToken);
    $('#sign-in').hidden = confirming || Boolean(account.authenticated);
    $('#account-actions').hidden = confirming || !account.authenticated;
    $('#portal').hidden = !account.customer;
    $('#confirm').hidden = !confirming;
    $('#account-title').textContent = confirming ? 'Confirma tu acceso' : account.authenticated ? 'Mi cuenta' : 'Ingresa con tu correo';
    $('#account-status').textContent = '';
    if (!accountDialog.open) accountDialog.showModal();
  }

  function busy(control, status, action) {
    return async event => {
      event?.preventDefault();
      if (control.disabled) return;
      control.disabled = true;
      $(status).textContent = '';
      try { await action(); }
      catch (error) { $(status).textContent = error.message; }
      finally { control.disabled = false; }
    };
  }

  $('#account-button').addEventListener('click', showAccount);
  $('#sign-in').addEventListener('submit', busy($('#sign-in button'), '#account-status', async () => {
    await api('/entrar', 'POST', { email: $('#email').value });
    $('#account-status').textContent = 'Revisa tu correo. El enlace caduca en 15 minutos.';
  }));
  $('#confirm').addEventListener('click', busy($('#confirm'), '#account-status', async () => {
    try { await api('/confirmar', 'POST', { token: signInToken }); }
    catch (error) { signInToken = null; showAccount(); throw error; }
    signInToken = null;
    await refreshAccount();
    showAccount();
    $('#account-status').textContent = account.member ? 'Tu membresía está activa.' : 'Ya ingresaste a tu biblioteca.';
  }));
  $('#portal').addEventListener('click', busy($('#portal'), '#account-status', async () => {
    location.assign((await api('/portal', 'POST')).url);
  }));
  $('#logout').addEventListener('click', busy($('#logout'), '#account-status', async () => {
    await api('/salir', 'POST');
    accountDialog.close();
    await refreshAccount();
  }));
  $('#subscribe').addEventListener('click', busy($('#subscribe'), '#billing-status', async () => {
    if (account.member) { location.hash = 'catalogo'; return; }
    if (!account.billingReady || !account.signInReady) {
      await refreshAccount();
      if (!account.billingReady || !account.signInReady) return;
    }
    if (!account.authenticated) {
      showAccount();
      $('#account-status').textContent = 'Ingresa con tu correo para activar tu membresía.';
      return;
    }
    location.assign((await api('/checkout', 'POST')).url);
  }));

  function saveProgress() {
    if (!selected?.full || audio.readyState === 0) return;
    const { id, duration } = selected;
    const body = { position: Math.min(audio.currentTime, duration), speed: audio.playbackRate };
    lastSave = Date.now();
    saveQueue = saveQueue.catch(() => {}).then(() => api('/libros/' + id + '/progreso', 'PUT', body));
    saveQueue.catch(error => { if (selected?.id === id) $('#player-status').textContent = error.message; });
  }

  function seek(position, play = false) {
    if (!selected || audio.readyState === 0) return;
    audio.currentTime = Math.max(0, Math.min(selected.duration, position));
    if (play) audio.play().catch(() => { $('#player-status').textContent = 'Toca reproducir para escuchar.'; });
  }

  function resetTimer(message = '') {
    clearTimeout(timerHandle);
    deadline = 0;
    stopAt = 0;
    $('#timer').value = '0';
    $('#timer-status').textContent = message;
  }

  function checkTimer() {
    if ((deadline && Date.now() >= deadline) || (stopAt && audio.currentTime >= stopAt)) {
      audio.pause();
      resetTimer('El temporizador pausó el audiolibro.');
    }
  }

  function updateChapter() {
    if (!selected?.chapters?.length) return;
    const index = selected.chapters.findLastIndex(chapter => audio.currentTime >= chapter.start);
    const active = Math.max(0, index);
    $('#current-chapter').textContent = selected.chapters[active].title;
    for (const [i, chapter] of Array.from($('#chapters').children).entries()) {
      if (active === i) chapter.firstChild.setAttribute('aria-current', 'true');
      else chapter.firstChild.removeAttribute('aria-current');
    }
  }

  async function renderReader(book, request) {
    $('#reader-panel').hidden = !book.transcriptUrl;
    if (!book.transcriptUrl) return;
    $('#reader-status').textContent = 'Abriendo el texto…';
    try {
      const text = await api(book.transcriptUrl.slice(apiRoot.length));
      if (request !== generation) return;
      reader.set(text);
      $('#reader-status').textContent = 'Toca un pasaje para escuchar desde ahí.';
    } catch (error) {
      if (request === generation) $('#reader-status').textContent = error.message;
    }
  }

  function renderMarks() {
    $('#marks').replaceChildren(...selected.marks.map(position => {
      const row = element('li');
      const remove = button('Quitar', () => updateMarks(selected.marks.filter(mark => mark !== position)));
      remove.setAttribute('aria-label', 'Quitar marcador de ' + time(position));
      row.append(button(time(position), () => seek(position, true)), remove);
      return row;
    }));
  }

  async function updateMarks(marks) {
    const book = selected;
    if (!book?.full || book.savingMarks) return;
    book.savingMarks = true;
    try {
      const data = await api('/libros/' + book.id + '/marcadores', 'PUT', { marks });
      if (selected !== book) return;
      selected.marks = data.marks;
      renderMarks();
      $('#player-status').textContent = 'Marcadores guardados.';
    } catch (error) {
      if (selected === book) $('#player-status').textContent = error.message;
    } finally { book.savingMarks = false; }
  }

  function setMediaSession(book) {
    if (!('mediaSession' in navigator) || !('MediaMetadata' in window)) return;
    navigator.mediaSession.metadata = new MediaMetadata({
      title: book.title, artist: book.author, album: 'Audiolibros TEC',
      artwork: [{ src: new URL(book.cover, location.href).href, type: 'image/svg+xml' }]
    });
    const actions = {
      play: () => audio.play().catch(() => {}), pause: () => audio.pause(),
      seekbackward: detail => seek(audio.currentTime - (detail.seekOffset || 15)),
      seekforward: detail => seek(audio.currentTime + (detail.seekOffset || 15)),
      seekto: detail => seek(detail.seekTime)
    };
    for (const [action, handler] of Object.entries(actions)) {
      try { navigator.mediaSession.setActionHandler(action, handler); } catch {}
    }
  }

  async function openBook(book, preview) {
    const request = ++generation;
    const full = !preview;
    $('#player-controls').hidden = true;
    $('#player-status').textContent = full ? 'Cargando audiolibro…' : '';
    $('#book-mode').textContent = full ? 'Edición íntegra' : 'Muestra gratuita · 10 %';
    $('#book-title').textContent = book.title;
    $('#book-author').textContent = book.author;
    $('#book-cover').src = book.cover;
    if (!player.open) player.showModal();
    try {
      const data = full
        ? await api('/libros/' + encodeURIComponent(book.id))
        : { book: { ...book, duration: book.previewDuration, audioUrl: book.previewUrl }, progress: { position: 0, speed: 1, marks: [] } };
      if (request !== generation) return;
      selected = { ...data.book, full, marks: data.progress.marks };
      lastSave = Date.now();
      $('#speed').value = String(data.progress.speed).replace(/^0\./, '.');
      $('#timer option[value="chapter"]').disabled = !full;
      $('#preview-upsell').hidden = full || account.member;
      $('#chapter-list').hidden = !full;
      $('#bookmark-list').hidden = !full;
      $('#chapters').replaceChildren(...(selected.chapters || []).map(chapter => {
        const row = element('li');
        row.append(button(chapter.title + ' · ' + time(chapter.start), () => seek(chapter.start, true)));
        return row;
      }));
      $('#current-chapter').textContent = full ? '' : 'Duración de la muestra · ' + time(book.previewDuration);
      $('#player-status').textContent = data.progress.position ? 'Continúa en ' + time(data.progress.position) + '.' : '';
      $('#player-controls').hidden = false;
      audio.onloadedmetadata = () => {
        if (request !== generation) return;
        audio.playbackRate = Number($('#speed').value) || 1;
        if (data.progress.position) seek(Math.min(data.progress.position, Math.max(0, audio.duration - .1)));
        updateChapter();
      };
      audio.src = selected.audioUrl;
      setMediaSession(book);
      if (full) { renderMarks(); renderReader(selected, request); }
    } catch (error) {
      if (request === generation) $('#player-status').textContent = error.message;
    }
  }

  player.addEventListener('close', () => {
    saveProgress();
    ++generation;
    selected = null;
    audio.pause();
    audio.removeAttribute('src');
    audio.load();
    reader.clear();
    $('#reader-panel').hidden = true;
    for (const panel of player.querySelectorAll('details')) panel.open = false;
    resetTimer();
    if ('mediaSession' in navigator) navigator.mediaSession.metadata = null;
  });
  for (const control of document.querySelectorAll('[data-close]')) {
    control.addEventListener('click', () => control.closest('dialog').close());
  }
  $('#back').addEventListener('click', () => seek(audio.currentTime - 15));
  $('#forward').addEventListener('click', () => seek(audio.currentTime + 15));
  $('#speed').addEventListener('change', () => { audio.playbackRate = Number($('#speed').value); saveProgress(); });
  $('#timer').addEventListener('change', event => {
    const value = event.target.value;
    resetTimer();
    event.target.value = value;
    if (value === 'chapter') {
      stopAt = selected?.chapters?.find(chapter => chapter.start > audio.currentTime)?.start || selected.duration;
      $('#timer-status').textContent = 'Pausa al terminar el capítulo.';
    } else if (Number(value)) {
      deadline = Date.now() + Number(value) * 60_000;
      timerHandle = setTimeout(checkTimer, Number(value) * 60_000);
      $('#timer-status').textContent = 'Pausa programada en ' + value + ' minutos.';
    }
  });
  $('#add-mark').addEventListener('click', () => {
    if (audio.readyState > 0) updateMarks([...new Set([...selected.marks, Math.floor(audio.currentTime)])]);
  });
  $('#unlock').addEventListener('click', () => { player.close(); location.hash = 'membresia'; });
  audio.addEventListener('timeupdate', () => {
    checkTimer();
    updateChapter();
    if (!audio.paused && Date.now() - lastSave > 15000) saveProgress();
  });
  audio.addEventListener('pause', saveProgress);
  audio.addEventListener('ended', () => {
    if (!selected) return;
    $('#player-status').textContent = selected.full ? 'Llegaste al final del audiolibro.' : 'Llegaste al final de la muestra.';
    resetTimer();
    saveProgress();
  });
  audio.addEventListener('error', () => {
    if (selected) $('#player-status').textContent = 'No se pudo cargar el audio. Vuelve a abrir el libro para intentarlo.';
  });
  window.addEventListener('pagehide', saveProgress);
  document.addEventListener('visibilitychange', () => { checkTimer(); if (document.hidden) saveProgress(); });

  if (signInToken) {
    history.replaceState(null, '', location.pathname + location.search);
    showAccount();
  }
  api('/catalogo').then(data => {
    books = data.books;
    $('#catalog-status').textContent = '';
    renderBooks();
  }).catch(error => { $('#catalog-status').textContent = error.message; });

  const checkoutReturn = new URLSearchParams(location.search).get('membresia') === 'confirmada';
  refreshAccount().then(async () => {
    if (!checkoutReturn) return;
    history.replaceState(null, '', location.pathname + '#membresia');
    $('#membresia').scrollIntoView();
    if (!account.member) {
      $('#billing-status').textContent = 'Confirmando tu acceso con Stripe…';
      await new Promise(resolve => setTimeout(resolve, 1500));
      await refreshAccount();
    }
    $('#billing-status').textContent = account.member ? 'Membresía activa. Ya puedes escuchar el catálogo completo.' : 'Stripe está confirmando el pago. Vuelve a cargar la página en unos minutos.';
  }).catch(error => { $('#billing-status').textContent = error.message; });
})();
