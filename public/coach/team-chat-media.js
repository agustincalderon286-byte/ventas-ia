(() => {
  'use strict';
  const MAX = 5 * 1024 * 1024;
  const icon = (button, value, label) => { button.textContent = value; button.setAttribute('aria-label', label); button.title = label; };
  const node = (tag, className) => { const el = document.createElement(tag); el.className = className; return el; };
  function database(mode, key, value) {
    return new Promise((resolve, reject) => {
      const open = indexedDB.open('tec-team-chat-drafts', 1);
      open.onupgradeneeded = () => open.result.createObjectStore('pending');
      open.onerror = () => reject(new Error('No pude guardar el archivo en este navegador.'));
      open.onsuccess = () => {
        const db = open.result; let result;
        const tx = db.transaction('pending', mode === 'get' ? 'readonly' : 'readwrite');
        const store = tx.objectStore('pending');
        const request = mode === 'get' ? store.get(key) : mode === 'delete' ? store.delete(key) : store.put(value,key);
        request.onsuccess = () => { result = request.result; };
        tx.oncomplete = () => { db.close(); resolve(result); };
        tx.onabort = tx.onerror = () => { db.close(); reject(new Error('No pude guardar el archivo en este navegador.')); };
      };
    });
  }
  const toBase64 = blob => new Promise((resolve,reject) => { const reader = new FileReader(); reader.onload = () => resolve(String(reader.result).split(',')[1]); reader.onerror = reject; reader.readAsDataURL(blob); });
  function init({ form, actions, send, input, notify, changed, submit }) {
    const attach = node('button','tec-team-chat__attach'); attach.type = 'button'; icon(attach,'＋','Adjuntar foto, GIF o audio');
    const picker = node('input',''); picker.type = 'file'; picker.accept = 'image/jpeg,image/png,image/webp,image/gif,audio/*'; picker.hidden = true;
    const mic = node('button','tec-team-chat__mic'); mic.type = 'button'; icon(mic,'🎙','Mantén presionado para grabar. Suelta para enviar; desliza a la izquierda para borrar.'); mic.setAttribute('aria-pressed','false');
    const preview = node('div','tec-team-chat__preview'); preview.hidden = true;
    const activity = node('span','tec-team-chat__recording'); activity.hidden = true; activity.setAttribute('role','status');
    actions.append(attach,mic,picker,activity); form.append(preview);
    let removeButton = null;
    let selected = null, previewUrl = '', locked = true, destroyed = false, recorder = null, stream = null, gesture = null, timer = null, seconds = 0, preparing = false, waiting = false;
    const urls = new Set(), players = new Set(), displays = new Map(), removers = [];
    const listen = (target,type,fn) => { target.addEventListener(type,fn); removers.push(() => target.removeEventListener(type,fn)); };
    const stopTracks = () => { stream?.getTracks().forEach(track => track.stop()); stream = null; };
    const clearClock = () => { clearInterval(timer); timer = null; };
    function clear() { selected = null; if (previewUrl) URL.revokeObjectURL(previewUrl); previewUrl = ''; preview.replaceChildren(); removeButton = null; preview.hidden = true; picker.value = ''; changed(); }
    function sync(disabled) {
      locked = disabled;
      if (locked && gesture) cancelRecording();
      if (removeButton) removeButton.disabled = locked || preparing || Boolean(gesture);
      attach.disabled = locked || preparing || Boolean(gesture);
      mic.disabled = (locked || preparing) && !gesture;
      mic.hidden = Boolean(input.value.trim() || selected) && !gesture;
      mic.setAttribute('aria-pressed', gesture ? 'true' : 'false');
      mic.classList.toggle('is-recording',Boolean(gesture));
      mic.classList.toggle('is-cancelling',Boolean(gesture?.cancelled));
      send.hidden = !input.value.trim() && !selected;
      activity.hidden = !gesture;
      activity.textContent = gesture?.cancelled ? '🗑 Suelta para borrar' : `${seconds}s · ${waiting ? 'Suelta para enviar' : '← Desliza para borrar'}`;
      input.disabled = input.disabled || preparing || Boolean(gesture);
    }
    async function choose(file) {
      if (destroyed || locked || preparing) return;
      if (!file || !file.size || file.size > MAX || (file.type === 'image/gif' && file.size > 2 * 1024 * 1024)) { notify('Usa una foto o audio de hasta 5 MB; GIF de hasta 2 MB.',true); return; }
      preparing = true; changed();
      try {
        const base64 = await toBase64(file);
        if (destroyed || locked) return;
        clear(); selected = { base64, mime: file.type };
        previewUrl = URL.createObjectURL(file); preview.hidden = false;
        const media = node(file.type.startsWith('audio/') || file.type.startsWith('video/') ? 'audio' : 'img','tec-team-chat__preview-media');
        media.src = previewUrl;
        if (media.tagName === 'AUDIO') { media.controls = true; media.preload = 'metadata'; } else media.alt = 'Archivo listo para enviar';
        const remove = node('button',''); remove.type = 'button'; icon(remove,'×','Quitar archivo'); removeButton = remove; listen(remove,'click',() => { if (!locked && !preparing) clear(); });
        preview.append(media,remove); notify('Archivo listo. Puedes añadir un texto y enviarlo.');
      } catch { notify('No pude abrir el archivo. Vuelve a elegirlo.',true); }
      finally { preparing = false; changed(); }
    }
    function cancelRecording() {
      gesture = null; waiting = false; clearClock();
      if (recorder?.state === 'recording') recorder.stop();
      stopTracks(); seconds = 0; changed();
    }
    function end(cancel = false) {
      if (!gesture) return;
      gesture.released = true; gesture.cancelled ||= cancel;
      if (gesture.cancelled) { cancelRecording(); notify('Grabación borrada.'); return; }
      if (waiting) { const blob = gesture.blob; gesture = null; waiting = false; changed(); void sendRecording(blob); return; }
      if (!recorder) { cancelRecording(); notify('Mantén presionado mientras se prepara el micrófono.'); return; }
      if (recorder.state === 'recording') recorder.stop();
    }
    async function sendRecording(blob) {
      if (!blob || destroyed) return;
      await choose(blob);
      if (selected && !destroyed) await submit();
    }
    async function start(event) {
      if (locked || destroyed || preparing || gesture || event.button > 0 || event.isPrimary === false) return;
      event.preventDefault();
      gesture = { pointerId: event.pointerId, startX: event.clientX, cancelled: false, released: false };
      const currentGesture = gesture; seconds = 0; changed();
      try { mic.setPointerCapture(event.pointerId); } catch {}
      if (!navigator.mediaDevices?.getUserMedia || !window.MediaRecorder) { cancelRecording(); notify('Este navegador no permite grabar. Puedes escribir o adjuntar un audio.',true); return; }
      try {
        const captured = await navigator.mediaDevices.getUserMedia({ audio: { echoCancellation: true, noiseSuppression: true }, video: false });
        if (destroyed || gesture !== currentGesture || currentGesture.released) { captured.getTracks().forEach(track => track.stop()); return; }
        stream = captured;
        const mime = ['audio/webm;codecs=opus','audio/mp4','audio/ogg;codecs=opus'].find(type => MediaRecorder.isTypeSupported(type));
        const recording = new MediaRecorder(stream, mime ? { mimeType: mime } : {}); recorder = recording;
        const chunks = []; let bytes = 0; const started = Date.now();
        recording.ondataavailable = event => { if (gesture !== currentGesture) return; if (event.data.size) { chunks.push(event.data); bytes += event.data.size; if (bytes > MAX) { cancelRecording(); notify('El audio supera 5 MB. Graba uno más corto.',true); } } };
        recording.onerror = () => { if (gesture !== currentGesture) return; cancelRecording(); notify('Se interrumpió la grabación. Vuelve a intentar.',true); };
        recording.onstop = () => {
          captured.getTracks().forEach(track => track.stop());
          if (recorder !== recording) return;
          clearClock(); if (stream === captured) stream = null; recorder = null;
          if (destroyed || gesture !== currentGesture || currentGesture.cancelled) return;
          if (Date.now() - started < 350 || bytes < 64) { cancelRecording(); notify('Mantén presionado un poco más para grabar.'); return; }
          const blob = new Blob(chunks,{ type: recording.mimeType || mime || 'audio/webm' });
          if (currentGesture.released) { gesture = null; changed(); void sendRecording(blob); }
          else { currentGesture.blob = blob; waiting = true; changed(); }
        };
        recording.start(250);
        timer = setInterval(() => { seconds = Math.floor((Date.now() - started) / 1000); if (seconds >= 89 && recording.state === 'recording') recording.stop(); changed(); },250);
      } catch { if (gesture === currentGesture) { cancelRecording(); notify('Permite el micrófono en el navegador para grabar.',true); } }
    }
    listen(attach,'click',() => picker.click()); listen(picker,'change',() => void choose(picker.files?.[0]));
    listen(mic,'pointerdown',event => void start(event));
    listen(mic,'pointermove',event => { if (gesture?.pointerId === event.pointerId) { gesture.cancelled = event.clientX - gesture.startX <= -64; changed(); } });
    listen(mic,'pointerup',event => { if (gesture?.pointerId === event.pointerId) { event.preventDefault(); end(); } });
    listen(mic,'pointercancel',() => end(true)); listen(mic,'lostpointercapture',() => { if (gesture && !gesture.released) end(true); });
    listen(mic,'contextmenu',event => event.preventDefault());
    // Space/Enter hold provides the same interaction without a pointer.
    listen(mic,'keydown',event => { if ([' ','Enter'].includes(event.key) && !event.repeat) void start({ preventDefault: () => event.preventDefault(), pointerId: 'keyboard', clientX: 0 }); });
    listen(mic,'keyup',event => { if ([' ','Enter'].includes(event.key) && gesture?.pointerId === 'keyboard') { event.preventDefault(); end(); } });
    listen(document,'keydown',event => { if (event.key === 'Escape' && gesture) end(true); });
    function pause() { players.forEach(player => player.pause()); if (gesture) end(true); }
    listen(document,'visibilitychange',() => { if (document.hidden) pause(); }); listen(window,'pagehide',pause);
    return {
      selected: () => selected, busy: () => preparing || Boolean(gesture), sync, clear, pause,
      persist: (key,value) => database('put',key,value), restore: key => database('get',key), forget: key => database('delete',key),
      render(row,message) {
        const attachment = message.attachment;
        if (!attachment) return;
        let display = displays.get(message.id);
        if (!display) {
          const wrapper = node('div','tec-team-chat__media');
          // Only server-owned same-origin media paths are eligible, never arbitrary chat URLs.
          const url = new URL(attachment.url,location.origin);
          if (url.origin !== location.origin || !/^\/api\/coach\/team-chat\/media\/[a-f0-9]{24}$/.test(url.pathname)) return;
          const media = node(attachment.kind === 'audio' ? 'audio' : 'img','tec-team-chat__media-content');
          media.src = url.href;
          if (attachment.kind === 'audio') { media.controls = true; media.preload = 'none'; players.add(media); listen(media,'play',() => players.forEach(other => { if (other !== media) other.pause(); })); }
          else { if (attachment.width > 0 && attachment.height > 0) { media.width = attachment.width; media.height = attachment.height; } media.alt = attachment.kind === 'gif' ? 'GIF compartido por ' + message.senderName : 'Foto compartida por ' + message.senderName; media.loading = 'lazy'; media.decoding = 'async';
            const link = node('a',''); link.href = url.href; link.target = '_blank'; link.rel = 'noopener'; link.append(media); wrapper.append(link); }
          if (attachment.kind === 'audio') wrapper.append(media);
          const transcript = node('details','tec-team-chat__transcript'); const summary = node('summary',''); summary.textContent = 'Transcripción automática'; const text = node('p',''); transcript.append(summary,text); wrapper.append(transcript);
          display = { wrapper, transcript, text }; displays.set(message.id,display);
        }
        display.transcript.hidden = !attachment.transcript;
        display.text.textContent = attachment.transcript || '';
        if (display.wrapper.parentElement !== row) row.append(display.wrapper);
      },
      destroy() { destroyed = true; cancelRecording(); players.forEach(player => { player.pause(); player.removeAttribute('src'); player.load(); }); if (previewUrl) URL.revokeObjectURL(previewUrl); urls.forEach(url => URL.revokeObjectURL(url)); removers.forEach(remove => remove()); displays.clear(); }
    };
  }
  window.TecTeamChatMedia = { init };
})();
