(() => {
  'use strict';
  const $ = selector => document.querySelector(selector);
  const audio = $('#audio');
  const player = $('#player');
  const delivery = new TecAudioDelivery.Controller(audio, { onFailure: text => { if (selected) $('#player-status').textContent = text; } });
  const seekBar = $('#seek-bar');
  const search = $('#catalog-search');
  const miniPlayer = $('#mini-player');
  const accountDialog = $('#account');
  const apiRoot = '/api/audiolibros';
  const testEntry = new URLSearchParams(location.search).get('acceso') === 'prueba';
  let account = {};
  let books = [];
  let selected = null;
  let generation = 0;
  let saveQueue = Promise.resolve();
  let lastSave = 0;
  let timerHandle;
  let deadline = 0;
  let stopAt = 0;
  let scrubbing = false;
  const accountParams = new URLSearchParams(location.hash.slice(1));
  let credentialToken = accountParams.get('cuenta');
  const legacyEmailLink = accountParams.has('entrar');
  const checkoutSessionId = new URLSearchParams(location.search).get('sesion');
  let credentialPurpose = '', accountView = 'login';
  const readingRequest = new URLSearchParams(location.search).get('lectura');
  const validReadingId = value => typeof value === 'string' && /^[a-z0-9-]{1,80}$/.test(value);
  if (validReadingId(readingRequest)) { try { localStorage.setItem('tec.reading.return.v1', JSON.stringify({ id: readingRequest, at: Date.now() })); } catch {} }
  function resumeReading() {
    if (!account.member) return false;
    let pending = validReadingId(readingRequest) ? { id: readingRequest, at: Date.now() } : null;
    if (!pending) { try { pending = JSON.parse(localStorage.getItem('tec.reading.return.v1')); } catch {} }
    if (!validReadingId(pending?.id) || !Number.isFinite(pending.at) || Date.now() - pending.at > 3600000 || pending.at > Date.now()) return false;
    try { localStorage.removeItem('tec.reading.return.v1'); } catch {}
    location.assign('/audiolibros/leer/' + pending.id + '/'); return true;
  }
  const reader = new TecBookReader.Reader($('[data-ab-reader]'), audio, position => seek(position, true));
  const sharing = new TecAudioSharing.Controller({ audio, reader, context: () => selected });
  $('#share-open').addEventListener('click', () => sharing.open());
  $('#reader-share').addEventListener('click', () => sharing.open());

  function achievementsContext() {
    const original = selected && books.find(book => book.id === selected.id);
    document.dispatchEvent(new CustomEvent('tec-audio-account-context', { detail: {
      account, catalog: books, book: original ? { id: original.id, duration: original.duration, previewDuration: original.previewDuration, full: selected.full } : null
    } }));
  }
  document.addEventListener('tec-audio-achievements-ready', achievementsContext);

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
    if (!books.length) return;
    const searchable = value => value.normalize('NFD').replace(/\p{Diacritic}/gu, '').toLowerCase();
    const query = searchable(search.value.trim());
    const matches = books.filter(book => searchable(book.title + ' ' + book.author).includes(query));
    const invited = document.querySelector('[data-librivox-pilot]');
    const invitedMatches = Boolean(invited && searchable(invited.dataset.search || '').includes(query));
    if (invited) invited.hidden = !invitedMatches;
    const cards = matches.map(book => {
      const card = element('article', undefined, 'book');
      const image = element('img');
      Object.assign(image, { src: book.cover, alt: 'Portada de ' + book.title, width: 600, height: 800, loading: 'lazy', decoding: 'async' });
      const copy = element('div');
      const detailUrl = '/audiolibros/libros/' + encodeURIComponent(book.id) + '/';
      const titleLink = element('a', book.title);
      titleLink.href = detailUrl;
      const heading = element('h3');
      heading.append(titleLink);
      copy.append(
        element('p', book.categories.join(' · '), 'eyebrow'), heading,
        element('p', book.author, 'author'), element('p', book.description, 'book-description'),
        element('p', book.language + ' · ' + time(book.duration), 'book-duration')
      );
      const actions = element('div', undefined, 'actions');
      if (account.member) actions.append(button('Escuchar y leer', () => openBook(book, false), 'primary'));
      if (book.previewUrl) actions.append(button('▶ Muestra · ' + time(book.previewDuration), () => openBook(book, true)));
      const details = element('a', 'Ver libro', 'book-link');
      details.href = detailUrl;
      actions.append(details);
      copy.append(actions);
      card.append(image, copy);
      return card;
    });
    $('#books').replaceChildren(...cards);
    $('#catalog-status').textContent = matches.length || invitedMatches ? '' : 'No encontramos títulos para esta búsqueda.';
    achievementsContext();
  }

  async function refreshAccount() {
    account = await api('/cuenta');
    $('#account-button').textContent = account.authenticated ? 'Mi cuenta' : 'Ingresar';
    $('#subscribe').textContent = account.member ? 'Ir al catálogo' : 'Suscribirme';
    $('#billing-status').textContent = account.billingReady && account.signInReady ? '' : 'La activación de nuevas suscripciones está pendiente.';
    renderBooks();
  }

  function showAccount() {
    const credentials = Boolean(credentialToken);
    const paid = accountView === 'paid';
    const authenticated = Boolean(account.authenticated) && !credentials && !paid && accountView !== 'recovery';
    $('#sign-in').hidden = credentials || authenticated || paid || testEntry || accountView === 'recovery';
    $('#paid-confirmation').hidden = !paid || credentials;
    $('#recovery').hidden = credentials || authenticated || accountView !== 'recovery';
    $('#credentials').hidden = !credentials || !credentialPurpose || credentialPurpose === 'checkout';
    $('#continue-membership').hidden = !credentials || credentialPurpose !== 'checkout';
    $('#test-access').hidden = credentials || authenticated || paid || !testEntry || accountView === 'recovery';
    $('#account-actions').hidden = credentials || !authenticated;
    $('#portal').hidden = !account.customer || account.testAccount;
    $('#account-title').textContent = credentials ? (credentialPurpose === 'checkout' ? 'Continúa tu membresía' : credentialPurpose === 'reset' ? 'Elige una nueva contraseña' : 'Confirma tu correo y crea tu cuenta')
      : paid ? 'Pago confirmado' : authenticated ? 'Mi cuenta' : accountView === 'recovery' ? 'Recupera tu acceso' : testEntry ? 'Acceso de prueba de TEC' : 'Ingresa a tu cuenta';
    $('#account-badges').hidden = !authenticated;
    $('#account-status').textContent = account.testAccount && !credentials ? 'Hola, ' + account.displayName + '. Acceso gratuito de prueba activo. Tu actividad queda fuera de las estadísticas comerciales.' : '';
    if (!accountDialog.open) accountDialog.showModal();
  }
  function showRecovery() {
    credentialToken = null; credentialPurpose = ''; accountView = 'recovery';
    $('#new-password').value = ''; $('#new-password-confirmation').value = '';
    showAccount();
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

  $('#account-button').addEventListener('click', () => { accountView = 'login'; showAccount(); });
  $('#account-badges')?.addEventListener('click', () => accountDialog.close());
  $('#phone-account').addEventListener('click', () => { accountView = 'login'; showAccount(); });
  search.addEventListener('input', renderBooks);
  $('#sign-in').addEventListener('submit', busy($('#sign-in button'), '#account-status', async () => {
    try { await api('/entrar', 'POST', { identifier: $('#login-identifier').value, password: $('#login-password').value }); }
    finally { $('#login-password').value = ''; }
    await refreshAccount();
    if (resumeReading()) return;
    showAccount();
    $('#account-status').textContent = account.member ? 'Tu membresía está activa.' : 'Ingresaste a tu cuenta. Puedes gestionar o renovar tu membresía.';
  }));
  $('#forgot-password').addEventListener('click', () => {
    if ($('#login-identifier').value.includes('@')) $('#recovery-email').value = $('#login-identifier').value;
    showRecovery();
  });
  $('#resend-activation').addEventListener('click', showRecovery);
  document.querySelectorAll('[data-account-login]').forEach(control => control.addEventListener('click', () => {
    accountView = 'login'; showAccount();
  }));
  $('#recovery').addEventListener('submit', busy($('#recovery button'), '#account-status', async () => {
    await api('/recuperar', 'POST', { email: $('#recovery-email').value });
    $('#account-status').textContent = 'Si el correo corresponde a una membresía, recibirás un enlace de TEC. Revisa también Spam y espera un minuto antes de pedir otro. Por seguridad, esta pantalla no confirma si existe una cuenta.';
  }));
  $('#credentials').addEventListener('submit', busy($('#credentials-submit'), '#account-status', async () => {
    const result = await api('/credenciales', 'POST', { token: credentialToken, username: $('#credentials-username').value,
      password: $('#new-password').value, passwordConfirmation: $('#new-password-confirmation').value });
    credentialToken = null; credentialPurpose = ''; accountView = 'login';
    $('#new-password').value = ''; $('#new-password-confirmation').value = '';
    $('#login-identifier').value = result.username;
    await refreshAccount();
    if (result.authenticated && resumeReading()) return;
    showAccount();
    $('#account-status').textContent = result.purpose === 'activation'
      ? 'Correo confirmado y cuenta creada. Tu membresía está lista.'
      : 'Contraseña actualizada. Ingresa con tu usuario o correo y la nueva contraseña.';
  }));
  $('#test-sign-in').addEventListener('submit', busy($('#test-sign-in button'), '#account-status', async () => {
    await api('/pruebas/entrar', 'POST', { username: $('#test-username').value, password: $('#test-password').value });
    $('#test-password').value = '';
    document.dispatchEvent(new CustomEvent('tec-audio-test-account'));
    await refreshAccount();
    if (resumeReading()) return;
    showAccount();
  }));
  $('#account-subscribe').addEventListener('click', () => accountDialog.close());
  $('#portal').addEventListener('click', busy($('#portal'), '#account-status', async () => {
    location.assign((await api('/portal', 'POST')).url);
  }));
  $('#logout').addEventListener('click', busy($('#logout'), '#account-status', async () => {
    stopPlayback();
    await saveQueue.catch(() => {});
    await api('/salir', 'POST');
    try { localStorage.setItem('tec.reading.signed-out', String(Date.now())); } catch {}
    accountDialog.close();
    await refreshAccount();
  }));
  async function startCheckout(email, ownershipToken) {
    let analytics;
    try { analytics = await window.TecAudiobookAnalytics?.checkoutContext?.(); } catch {}
    const checkout = await api('/checkout', 'POST', { ...(email ? { email } : {}), ...(ownershipToken ? { ownershipToken } : {}), ...(analytics ? { analytics } : {}) });
    window.TecAudiobookAnalytics?.checkoutStarted?.();
    location.assign(checkout.url);
  }
  $('#subscribe').addEventListener('click', busy($('#subscribe'), '#billing-status', async () => {
    if (account.member) { location.hash = 'catalogo'; return; }
    if (!account.billingReady || !account.signInReady) {
      await refreshAccount();
      if (!account.billingReady || !account.signInReady) return;
    }
    if (account.authenticated) { await startCheckout(); return; }
    $('#checkout-status').textContent = '';
    $('#checkout-dialog').showModal();
  }));
  $('#checkout-form').addEventListener('submit', busy($('#checkout-form button'), '#checkout-status', async () => {
    await startCheckout($('#checkout-email').value);
  }));
  $('#checkout-login').addEventListener('click', () => {
    $('#checkout-dialog').close();
    $('#login-identifier').value = $('#checkout-email').value;
    showAccount();
  });
  $('#continue-membership-submit').addEventListener('click', busy($('#continue-membership-submit'), '#account-status', async () => {
    await startCheckout(undefined, credentialToken);
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
    delivery.seek(Math.max(0, Math.min(Number(seekBar.max), position)), play);
    updatePlayback();
  }

  function startPlayback() { if (selected) delivery.play(); }

  function togglePlayback() {
    if (audio.paused) startPlayback();
    else audio.pause();
  }

  function updatePlayback() {
    const playing = Boolean(selected) && !audio.paused && !audio.ended;
    const label = playing ? 'Pausar' : 'Reproducir';
    for (const control of [$('#play'), $('#mini-play')]) {
      if (control.getAttribute('aria-label') === label) continue;
      control.setAttribute('aria-label', label);
      control.textContent = playing ? '❚❚' : '▶';
    }
    if (!selected) return;
    const duration = Number(seekBar.max);
    const elapsed = scrubbing ? Number(seekBar.value) : Math.min(duration, audio.currentTime);
    if (!scrubbing) seekBar.value = String(elapsed);
    seekBar.style.setProperty('--seek-progress', (duration > 0 ? elapsed / duration * 100 : 0) + '%');
    seekBar.setAttribute('aria-valuetext', time(elapsed) + ' de ' + time(duration));
    $('#elapsed').textContent = time(elapsed);
    $('#duration').textContent = time(duration);
    $('#mini-status').textContent = (selected.full ? 'Audiolibro' : 'Muestra') + ' · ' + time(audio.currentTime) + ' / ' + time(duration);
  }

  function resetTimer(message = '') {
    clearTimeout(timerHandle);
    deadline = 0;
    stopAt = 0;
    $('#timer').value = '0';
    $('#timer-status').textContent = message;
    $('#timer-badge').hidden = true;
    $('#timer-shortcut').removeAttribute('data-active');
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
    const panel = $('#reader-panel');
    panel.hidden = !book.transcriptUrl;
    $('#reader-shortcut').hidden = !book.transcriptUrl;
    if (!book.transcriptUrl) return;
    panel.open = false;
    $('#reader-status').textContent = 'Abriendo el texto…';
    try {
      const text = await api(book.transcriptUrl.slice(apiRoot.length));
      if (request !== generation) return;
      reader.set(text);
      $('#reader-status').textContent = (book.full ? '' : 'Texto de la muestra gratuita. ') + 'Toca un pasaje para escuchar desde ahí.';
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
    const artwork = book.mediaArtwork
      ? { src: book.mediaArtwork, type: 'image/png', sizes: '512x512' }
      : { src: book.cover };
    navigator.mediaSession.metadata = new MediaMetadata({
      title: book.title, artist: book.author, album: 'Audiolibros TEC',
      artwork: [{ ...artwork, src: new URL(artwork.src, location.href).href }]
    });
    const actions = {
      play: () => delivery.play(), pause: () => delivery.pause(),
      seekbackward: detail => seek(audio.currentTime - (detail.seekOffset || 15)),
      seekforward: detail => seek(audio.currentTime + (detail.seekOffset || 15)),
      seekto: detail => seek(detail.seekTime)
    };
    for (const [action, handler] of Object.entries(actions)) {
      try { navigator.mediaSession.setActionHandler(action, handler); } catch {}
    }
  }

  async function openBook(book, preview, startAt = null) {
    const full = !preview;
    if (selected?.id === book.id && selected.full === full && !audio.error) {
      expandPlayer();
      if (Number.isFinite(startAt)) seek(startAt);
      return;
    }
    stopPlayback();
    const request = generation;
    $('#player-controls').hidden = true;
    $('#player-status').textContent = full ? 'Cargando audiolibro…' : '';
    $('#book-mode').textContent = full ? (book.releaseLabel || 'Edición íntegra') : 'Muestra gratuita · 10 %';
    $('#book-title').textContent = book.title;
    $('#reader-view-title').textContent = book.title;
    $('#book-author').textContent = book.author;
    $('#book-cover').src = book.cover;
    expandPlayer();
    $('.player-body').scrollTop = 0;
    try {
      const data = full
        ? await api('/libros/' + encodeURIComponent(book.id))
        : { book: { ...book, duration: book.previewDuration, audioUrl: book.previewUrl, transcriptUrl: book.previewTranscriptUrl }, progress: { position: 0, speed: 1, marks: [] } };
      if (request !== generation) return;
      selected = { ...data.book, full, marks: data.progress.marks };
      lastSave = Date.now();
      $('#mini-cover').src = book.cover;
      $('#mini-title').textContent = book.title;
      $('#mini-open').setAttribute('aria-label', 'Abrir reproductor de ' + book.title);
      seekBar.max = String(selected.duration);
      seekBar.value = '0';
      $('#speed').value = String(data.progress.speed).replace(/^0\./, '.');
      $('#timer option[value="chapter"]').disabled = !full;
      $('#preview-upsell').hidden = full || account.member;
      $('#chapter-list').hidden = !full;
      $('#chapter-shortcut').hidden = !full;
      $('#bookmark-list').hidden = !full;
      $('#chapters').replaceChildren(...(selected.chapters || []).map(chapter => {
        const row = element('li');
        row.append(button(chapter.title + ' · ' + time(chapter.start), () => {
          seek(chapter.start, true);
          closeToolPanels();
          $('#chapter-shortcut').focus({ preventScroll: true });
        }));
        return row;
      }));
      $('#current-chapter').textContent = full ? '' : 'Duración de la muestra · ' + time(book.previewDuration);
      $('#player-status').textContent = data.progress.position ? 'Continúa en ' + time(data.progress.position) + '.' : '';
      $('#player-controls').hidden = false;
      const position = Number.isFinite(startAt) ? startAt : data.progress.position;
      await delivery.load(selected.audioUrl, { position, rate: Number($('#speed').value) || 1, onReady: () => {
        if (request !== generation) return;
        seekBar.max = String(Number.isFinite(audio.duration) ? Math.min(selected.duration, audio.duration) : selected.duration);
        seekBar.disabled = false;
        updateChapter(); updatePlayback();
      } });
      if (request !== generation) return;
      achievementsContext();
      updatePlayback();
      setMediaSession(book);
      if (full) renderMarks();
      renderReader(selected, request);
    } catch (error) {
      if (request === generation) $('#player-status').textContent = error.message;
    }
  }

  function expandPlayer() {
    miniPlayer.hidden = true;
    if (!player.open) player.showModal();
  }

  function setReadingFullscreen(enabled) {
    const text = $('[data-ab-reader-text]');
    const scrollable = text.scrollHeight - text.clientHeight;
    const progress = scrollable > 0 ? text.scrollTop / scrollable : 0;
    player.classList.toggle('reading-fullscreen', enabled);
    if (!enabled) $('#reader-panel').open = false;
    $('#reader-view-heading').hidden = !enabled;
    $('#reader-fullscreen').textContent = enabled ? 'Reducir lectura' : 'Pantalla completa';
    $('#reader-fullscreen').setAttribute('aria-pressed', String(enabled));
    if (enabled) {
      $('#reader-panel').open = true;
      reader.fontSize = Math.max(22, reader.fontSize);
      reader.resize(0);
    }
    text.scrollTop = progress * Math.max(0, text.scrollHeight - text.clientHeight);
    $('.player-body').scrollTop = 0;
    reader.update(true);
  }

  function toggleReadingFullscreen() {
    const enabled = !player.classList.contains('reading-fullscreen');
    setReadingFullscreen(enabled);
    (enabled ? $('[data-ab-reader-text]') : $('#reader-shortcut')).focus({ preventScroll: true });
  }

  function stopPlayback() {
    saveProgress();
    setReadingFullscreen(false);
    ++generation;
    selected = null;
    achievementsContext();
    scrubbing = false;
    seekBar.disabled = true;
    delivery.stop();
    miniPlayer.hidden = true;
    reader.clear();
    $('#reader-panel').hidden = true;
    $('#reader-shortcut').hidden = true;
    for (const panel of player.querySelectorAll('details')) panel.open = false;
    closeToolPanels();
    resetTimer();
    if ('mediaSession' in navigator) navigator.mediaSession.metadata = null;
  }

  player.addEventListener('close', () => {
    if (player.open) return;
    setReadingFullscreen(false);
    if (!selected) { ++generation; return; }
    saveProgress();
    miniPlayer.hidden = false;
    updatePlayback();
  });
  for (const control of document.querySelectorAll('[data-close]')) {
    control.addEventListener('click', () => control.closest('dialog').close());
  }
  $('#player-stop').addEventListener('click', () => { stopPlayback(); player.close(); });
  $('#mini-stop').addEventListener('click', stopPlayback);
  $('#mini-open').addEventListener('click', expandPlayer);
  $('#mini-play').addEventListener('click', togglePlayback);
  $('#play').addEventListener('click', togglePlayback);
  seekBar.addEventListener('input', () => { scrubbing = true; updatePlayback(); });
  seekBar.addEventListener('change', () => { scrubbing = false; seek(Number(seekBar.value)); saveProgress(); });
  for (const event of ['blur', 'pointercancel']) {
    seekBar.addEventListener(event, () => { scrubbing = false; updatePlayback(); });
  }
  function closeToolPanels() {
    for (const id of ['timer-settings', 'playback-settings']) $('#' + id).hidden = true;
    $('#chapter-list').open = false;
    for (const id of ['timer-shortcut', 'settings-shortcut', 'chapter-shortcut']) $('#' + id).setAttribute('aria-expanded', 'false');
  }
  function toggleTool(id, control) {
    const panel = $('#' + id);
    const expanded = panel.tagName === 'DETAILS' ? panel.open : !panel.hidden;
    closeToolPanels();
    if (expanded) return;
    if (panel.tagName === 'DETAILS') panel.open = true;
    else panel.hidden = false;
    control.setAttribute('aria-expanded', 'true');
    panel.scrollIntoView({ block: 'nearest', behavior: 'instant' });
  }
  for (const [control, panel] of [['chapter-shortcut', 'chapter-list'], ['timer-shortcut', 'timer-settings'], ['settings-shortcut', 'playback-settings']]) {
    $('#' + control).addEventListener('click', event => toggleTool(panel, event.currentTarget));
  }
  $('#chapter-list').addEventListener('toggle', event => {
    $('#chapter-shortcut').setAttribute('aria-expanded', String(event.currentTarget.open));
  });
  for (const control of document.querySelectorAll('[data-tool-close]')) {
    control.addEventListener('click', () => {
      const id = control.closest('.player-option-panel').id;
      closeToolPanels();
      $(id === 'timer-settings' ? '#timer-shortcut' : '#settings-shortcut').focus({ preventScroll: true });
    });
  }
  $('#reader-shortcut').addEventListener('click', () => {
    closeToolPanels();
    toggleReadingFullscreen();
  });
  $('#reader-panel').addEventListener('toggle', event => {
    $('#reader-shortcut').setAttribute('aria-expanded', String(event.currentTarget.open));
  });
  $('#reader-fullscreen').addEventListener('click', toggleReadingFullscreen);
  $('#reader-return').addEventListener('click', toggleReadingFullscreen);
  player.addEventListener('cancel', event => {
    if (!player.classList.contains('reading-fullscreen')) return;
    event.preventDefault();
    toggleReadingFullscreen();
  });
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
    if (value !== '0') {
      $('#timer-badge').textContent = value === 'chapter' ? 'Capítulo' : value + ' min';
      $('#timer-badge').hidden = false;
      $('#timer-shortcut').dataset.active = 'true';
    }
    closeToolPanels();
    $('#timer-shortcut').focus({ preventScroll: true });
  });
  $('#add-mark').addEventListener('click', () => {
    if (audio.readyState > 0) updateMarks([...new Set([...selected.marks, Math.floor(audio.currentTime)])]);
  });
  $('#unlock').addEventListener('click', () => { player.close(); location.hash = 'membresia'; });
  audio.addEventListener('timeupdate', () => {
    checkTimer();
    updateChapter();
    updatePlayback();
    if (!audio.paused && Date.now() - lastSave > 15000) saveProgress();
  });
  audio.addEventListener('play', () => {
    if (selected && !selected.full) window.TecAudiobookAnalytics?.samplePlayed(selected.id);
  });
  audio.addEventListener('pause', saveProgress);
  for (const event of ['play', 'pause', 'ended', 'seeked']) audio.addEventListener(event, updatePlayback);
  audio.addEventListener('ended', () => {
    if (!selected) return;
    $('#player-status').textContent = selected.full ? 'Llegaste al final del audiolibro.' : 'Llegaste al final de la muestra.';
    resetTimer();
    saveProgress();
  });

  window.addEventListener('pagehide', saveProgress);
  document.addEventListener('visibilitychange', () => { checkTimer(); if (document.hidden) saveProgress(); });

  if (credentialToken || legacyEmailLink) {
    history.replaceState(null, '', location.pathname + location.search);
    if (legacyEmailLink) {
      showRecovery();
      $('#account-status').textContent = 'Ahora ingresas con usuario y contraseña. Si ya pagaste, pide un enlace para configurar tu cuenta.';
    } else {
      showAccount();
      $('#account-status').textContent = 'Comprobando tu enlace…';
      api('/credenciales/estado', 'POST', { token: credentialToken }).then(info => {
        credentialPurpose = info.purpose;
        const activation = info.purpose === 'activation';
        $('#credentials-username-field').hidden = !activation;
        $('#credentials-username').required = activation;
        $('#credentials-username').disabled = !activation;
        $('#credentials-explanation').textContent = activation
          ? 'Tu pago ya fue confirmado. Al completar este formulario confirmas tu correo y creas tu cuenta.'
          : 'Elige una contraseña nueva. Después deberás ingresar otra vez.';
        $('#credentials-submit').textContent = activation ? 'Confirmar correo y crear mi cuenta' : 'Guardar nueva contraseña';
        showAccount();
      }).catch(error => { showRecovery(); $('#account-status').textContent = error.message; });
    }
  }
  const catalogReady = api('/catalogo').then(data => {
    books = data.books;
    $('#catalog-status').textContent = '';
    search.closest('label').hidden = !books.length;
    renderBooks();
  }).catch(error => { $('#catalog-status').textContent = error.message; });

  const checkoutReturn = new URLSearchParams(location.search).get('membresia') === 'confirmada';
  const accountReady = refreshAccount().then(async () => {
    if (validReadingId(readingRequest)) {
      if (resumeReading()) return;
      if (!account.authenticated && location.hash !== '#membresia') showAccount();
    }
    if (testEntry) { showAccount(); $('#test-access').open = !account.authenticated; }
    if (!checkoutReturn) return;
    history.replaceState(null, '', location.pathname + '#membresia');
    $('#membresia').scrollIntoView();
    $('#billing-status').textContent = 'Confirmando tu pago con Stripe…';
    const confirmation = await api('/alta/estado', 'POST', { sessionId: checkoutSessionId });
    await refreshAccount();
    if (account.member) {
      $('#billing-status').textContent = 'Membresía activa. Ya puedes escuchar el catálogo completo.';
      resumeReading();
    } else if (confirmation.paid) {
      credentialToken = null; credentialPurpose = ''; accountView = 'paid'; showAccount();
      $('#billing-status').textContent = 'Pago confirmado. Termina de crear tu cuenta desde el correo de Audiolibros TEC.';
    } else {
      $('#billing-status').textContent = 'Stripe todavía no confirma el pago. No se habilitó una cuenta. Revisa tu correo o contacta a TEC.';
    }
  }).catch(error => { $('#billing-status').textContent = error.message; });
  Promise.all([catalogReady, accountReady]).then(() => {
    const params = new URLSearchParams(location.search);
    const book = books.find(item => item.id === params.get('libro'));
    if (!book) return;
    const requested = Number(params.get('t') || 0);
    const position = Number.isFinite(requested) && requested >= 0 && requested < (account.member ? book.duration : book.previewDuration) ? requested : 0;
    openBook(book, !account.member, position);
  });
})();
