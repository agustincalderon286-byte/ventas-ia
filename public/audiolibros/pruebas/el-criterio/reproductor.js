(() => {
  'use strict';
  const $ = id => document.getElementById(id);
  const audio = $('audio'), play = $('play'), status = $('status');
  const storageKey = 'tec.librivox.el-criterio.progress.v1';
  let book, index = 0, ready = false, position = 0, rate = 1, loading = false, lastSave = 0, loaded = false;
  const format = seconds => {
    const n = Math.max(0, Math.floor(Number(seconds) || 0));
    return Math.floor(n / 60) + ':' + String(n % 60).padStart(2, '0');
  };
  function readSaved() { try { return JSON.parse(localStorage.getItem(storageKey)); } catch { return null; } }
  function save() {
    if (!book || !ready || loading || !Number.isFinite(audio.currentTime)) return;
    position = Math.min(book.chapters[index].duration, Math.max(0, audio.currentTime));
    try { localStorage.setItem(storageKey, JSON.stringify({ index, position, rate, at: Date.now() })); } catch {}
    lastSave = Date.now();
  }
  function render() {
    const chapter = book.chapters[index];
    $('current-track').textContent = chapter.title;
    $('current-reader').textContent = 'Narración: ' + chapter.reader;
    $('previous').disabled = index === 0;
    $('next').disabled = index === book.chapters.length - 1;
    $('backward').disabled = $('forward').disabled = !ready;
    document.querySelectorAll('[data-track]').forEach(node => {
      if (Number(node.dataset.track) === index) node.setAttribute('aria-current', 'true');
      else node.removeAttribute('aria-current');
    });
    if ('mediaSession' in navigator && 'MediaMetadata' in window) {
      navigator.mediaSession.metadata = new MediaMetadata({ title: chapter.title, artist: 'Jaime Balmes · LibriVox', album: 'El criterio · Audiolibros TEC', artwork: [{ src: new URL('./portada.jpg', location.href).href, sizes: '1000x1000', type: 'image/jpeg' }] });
    }
  }
  async function resume() {
    try { await audio.play(); }
    catch { if (!audio.error) status.textContent = 'Toca Reproducir para continuar.'; }
  }
  function selectTrack(next, { start = 0, autoplay = true } = {}) {
    if (!book || !Number.isInteger(next) || next < 0 || next >= book.chapters.length) return;
    save(); loading = true; ready = false; audio.pause();
    index = next; position = Math.max(0, Math.min(Number(start) || 0, book.chapters[index].duration));
    loaded = true; status.textContent = 'Conectando con Internet Archive…';
    render();
    audio.src = book.chapters[index].url;
    audio.load();
    if (autoplay) resume();
  }
  function seek(delta) {
    if (!ready) return;
    const end = Number.isFinite(audio.duration) ? audio.duration : book.chapters[index].duration;
    audio.currentTime = Math.max(0, Math.min(end, audio.currentTime + delta)); save();
  }
  audio.addEventListener('loadedmetadata', () => {
    if (!book || audio.currentSrc !== book.chapters[index].url) return;
    const end = Number.isFinite(audio.duration) ? audio.duration : book.chapters[index].duration;
    audio.currentTime = Math.min(position, Math.max(0, end - .05));
    audio.playbackRate = rate;
    loading = false; ready = true;
    status.textContent = 'Pista ' + (index + 1) + ' de ' + book.chapters.length + ' · Audio de LibriVox';
    render(); save();
  });
  audio.addEventListener('play', () => { play.textContent = 'Ⅱ Pausar'; play.setAttribute('aria-label', 'Pausar El criterio'); });
  audio.addEventListener('pause', () => { play.textContent = loaded ? '▶ Continuar' : '▶ Reproducir'; play.setAttribute('aria-label', 'Reproducir El criterio'); save(); });
  audio.addEventListener('timeupdate', () => { if (Date.now() - lastSave > 5000) save(); });
  audio.addEventListener('seeked', save);
  audio.addEventListener('ended', () => {
    save();
    if (index < book.chapters.length - 1) selectTrack(index + 1);
    else status.textContent = 'Terminaste El criterio. Puedes elegir cualquier capítulo para volver a escucharlo.';
  });
  audio.addEventListener('error', () => {
    loading = false; ready = false; render();
    status.textContent = navigator.onLine === false ? 'Estás sin conexión. Este audio necesita internet; tu avance guardado se conserva.' : 'Internet Archive no pudo cargar esta pista. Toca Reproducir para reintentar o usa el reproductor original de abajo.';
    play.textContent = '↻ Reintentar';
  });
  play.addEventListener('click', () => {
    if (!book) return;
    if (!loaded || audio.error) selectTrack(index, { start: position });
    else if (audio.paused) resume();
    else audio.pause();
  });
  $('previous').addEventListener('click', () => selectTrack(index - 1));
  $('next').addEventListener('click', () => selectTrack(index + 1));
  $('backward').addEventListener('click', () => seek(-15));
  $('forward').addEventListener('click', () => seek(15));
  $('speed').addEventListener('change', () => { rate = Number($('speed').value); audio.defaultPlaybackRate = rate; audio.playbackRate = rate; save(); });
  window.addEventListener('pagehide', save);
  document.addEventListener('visibilitychange', () => { if (document.hidden) save(); });
  $('embed-open').addEventListener('click', () => {
    save(); audio.pause();
    if ($('embed').firstChild) return;
    const frame = document.createElement('iframe');
    frame.src = 'https://archive.org/embed/criterio_2408_librivox';
    frame.title = 'Reproductor original de El criterio en Internet Archive'; frame.allow = 'autoplay';
    frame.referrerPolicy = 'no-referrer';
    $('embed').append(frame); $('embed-open').disabled = true;
  });
  fetch('./libro.json', { credentials: 'omit' }).then(response => {
    if (!response.ok) throw new Error('manifest'); return response.json();
  }).then(data => {
    if (data.id !== 'el-criterio-librivox' || data.chapters?.length !== 27 || data.chapters.some((chapter, i) => chapter.url !== 'https://archive.org/download/criterio_2408_librivox/criterio_' + String(i + 1).padStart(2, '0') + '_balmes_128kb.mp3' || !Number.isFinite(chapter.duration) || chapter.duration <= 0)) throw new Error('manifest');
    book = data;
    const saved = readSaved();
    if (Number.isInteger(saved?.index) && saved.index >= 0 && saved.index < book.chapters.length && Number.isFinite(saved.position) && saved.position >= 0) {
      index = saved.index; position = Math.min(saved.position, book.chapters[index].duration);
      if ([.75, 1, 1.25, 1.5, 1.75, 2].includes(saved.rate)) rate = saved.rate;
    }
    $('speed').value = String(rate);
    book.chapters.forEach((chapter, i) => {
      const row = document.createElement('li'), button = document.createElement('button');
      button.type = 'button'; button.dataset.track = String(i);
      for (const [value, cls] of [[String(i + 1).padStart(2, '0'), 'track-number'], [chapter.title, 'track-title'], [format(chapter.duration), 'track-duration']]) {
        const part = document.createElement('span'); part.className = cls; part.textContent = value; button.append(part);
      }
      button.addEventListener('click', () => selectTrack(i)); row.append(button); $('chapters').append(row);
    });
    play.disabled = false; render();
    play.textContent = position > 0 ? '▶ Continuar desde ' + format(position) : '▶ Reproducir';
    status.textContent = position > 0 ? 'Tu avance está guardado en la pista ' + (index + 1) + '.' : 'Elige Reproducir para empezar.';
    if ('mediaSession' in navigator) {
      for (const [action, fn] of Object.entries({ play: () => loaded ? resume() : selectTrack(index, { start: position }), pause: () => audio.pause(), previoustrack: () => selectTrack(index - 1), nexttrack: () => selectTrack(index + 1), seekbackward: event => seek(-(event.seekOffset || 15)), seekforward: event => seek(event.seekOffset || 15) })) {
        try { navigator.mediaSession.setActionHandler(action, fn); } catch {}
      }
    }
  }).catch(() => { status.textContent = 'No se pudieron preparar los capítulos. Recarga la página o usa el reproductor original de abajo.'; });
})();
