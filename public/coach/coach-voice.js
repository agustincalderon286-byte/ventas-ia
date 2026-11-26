(() => {
  'use strict';
  // Un mensaje de voz dura hasta 3 minutos. Un servicio de voz propio (el Chef público) conserva
  // su tope anterior de 90 segundos, que es el que valida su servidor, salvo que pida otro.
  const MAX_BYTES = 8 * 1024 * 1024, MAX_SECONDS = 180, CUSTOM_API_SECONDS = 90, MIN_MS = 500;
  const formatClock = value => `${Math.floor(value / 60)}:${String(Math.floor(value % 60)).padStart(2, '0')}`;
  function microphoneSettings() {
    const ua = navigator.userAgent || '';
    if (/\bTEC-iOS\//i.test(ua) || window.webkit?.messageHandlers?.tecCapture) {
      return 'Abre Configuración → Privacidad y seguridad → Micrófono y activa TEC Coach.';
    }
    const ios = /iPhone|iPad|iPod/.test(ua) || (/Macintosh/.test(ua) && navigator.maxTouchPoints > 1);
    if (ios && !navigator.standalone && /Version\/.*Safari\//.test(ua) && !/CriOS|FxiOS|EdgiOS|OPiOS/.test(ua)) {
      return 'En Safari, abre el menú de página de TEC → Ajustes del sitio web → Micrófono → Permitir.';
    }
    return 'Activa el acceso al micrófono para TEC en los permisos del sitio.';
  }
  function recordingMime(Recorder = window.MediaRecorder) {
    return ['audio/webm;codecs=opus','audio/mp4','audio/webm'].find(type => Recorder?.isTypeSupported?.(type)) || '';
  }
  // Sin una tasa fija cada navegador elige la suya (hasta unos 256 kbps). Para voz basta Opus a 32 kbps
  // o AAC a 48 kbps: tres minutos pesan cerca de 1 MB y caben de sobra en el tope de subida.
  function recordingOptions(mime = recordingMime()) {
    const options = { audioBitsPerSecond: /webm|opus/.test(mime) ? 32000 : 48000 };
    if (mime) options.mimeType = mime;
    return options;
  }
  async function readSpeech(response, onChunk) {
    if(!response.headers.get('Content-Type')?.includes('application/x-ndjson'))return response.blob();
    const reader=response.body.getReader(), decoder=new TextDecoder(), chunks=[];
    let pending='', complete=false, length=0;
    const line=value=>{
      if(!value.trim())return;
      const event=JSON.parse(value);
      if(event.type==='error')throw new Error(event.error || 'No se completó la voz.');
      if(event.type==='done')complete=true;
      if(event.type==='audio'){
        const raw=atob(event.data), bytes=Uint8Array.from(raw,char=>char.charCodeAt(0));
        length+=bytes.length;if(length>25*1024*1024)throw new Error('El audio es demasiado grande.');
        chunks.push(bytes);onChunk?.(bytes);
      }
    };
    try {
      while(true){const {done,value}=await reader.read();if(done)break;pending+=decoder.decode(value,{stream:true});let end;while((end=pending.indexOf('\n'))>=0){line(pending.slice(0,end));pending=pending.slice(end+1);}}
      pending+=decoder.decode();if(pending.trim())line(pending);
      if(!complete || !length || length%2)throw new Error('La voz se interrumpió. Puedes reintentar el audio.');
    } finally {reader.cancel().catch(()=>{});}
    return pcmWave(chunks, length);
  }
  function pcmWave(chunks, length) {
    const header=new ArrayBuffer(44), view=new DataView(header), write=(at,text)=>[...text].forEach((char,i)=>view.setUint8(at+i,char.charCodeAt(0)));
    write(0,'RIFF');view.setUint32(4,length+36,true);write(8,'WAVE');write(12,'fmt ');view.setUint32(16,16,true);view.setUint16(20,1,true);view.setUint16(22,1,true);view.setUint32(24,24000,true);view.setUint32(28,48000,true);view.setUint16(32,2,true);view.setUint16(34,16,true);write(36,'data');view.setUint32(40,length,true);
    return new Blob([header,...chunks],{type:'audio/wav'});
  }
  function speechParts(text) {
    if (text.length <= 3000) return [text];
    // Word boundaries, with enough room for two UTF-16 units per code point.
    return text.match(/[\s\S]{1,2999}(?:\s|$)|[\s\S]{1,2999}/gu);
  }
  // Keep a short lead over the speaker clock so normal packet jitter is inaudible.
  function createSpeechBuffer(context, onEnded) {
    const rate = 24000;
    let pending = [], size = 0, next = 0, last = null, finished = false;
    function schedule(final = false) {
      const count = size - size % 2;
      if (!count || (!final && next <= context.currentTime && count < rate * 2 * 0.45)) return false;
      const bytes = new Uint8Array(size);
      let offset = 0;
      for (const part of pending) { bytes.set(part, offset); offset += part.length; }
      pending = size === count ? [] : [bytes.slice(count)];
      size -= count;
      const samples = count / 2;
      const buffer = context.createBuffer(1, samples, rate);
      const output = buffer.getChannelData(0);
      const view = new DataView(bytes.buffer);
      for (let i = 0; i < samples; i++) output[i] = view.getInt16(i * 2, true) / 32768;
      const source = context.createBufferSource();
      source.buffer = buffer;
      source.connect(context.destination);
      if (last) last.onended = null;
      last = source;
      source.onended = () => { if (last === source) { last = null; if (finished) onEnded(); } };
      const at = next > context.currentTime ? next : context.currentTime + 0.04;
      source.start(at);
      next = at + buffer.duration;
      return true;
    }
    return {
      append(bytes) { pending.push(bytes); size += bytes.length; return schedule(); },
      finish() { finished = true; schedule(true); if (!last) onEnded(); },
      get playing() { return last !== null; }
    };
  }

  function init({ userId, forms = [], sendMessage, voiceApi, speaker = 'Coach',
    maxBytes = MAX_BYTES, maxSeconds, toolbarSelector = '', hasExtraInput = () => false, onBusyChange = () => {} }) {
    if ((!userId && !voiceApi) || !forms.length) return null;
    const limit = Math.max(1, Math.floor(Number(maxSeconds)) || (voiceApi ? CUSTOM_API_SECONDS : MAX_SECONDS));
    const limitText = limit % 60 ? `${limit} segundos` : limit === 60 ? '1 minuto' : `${limit / 60} minutos`;
    let phase = 'idle', chatBusy = false, generation = 0, recorder = null, stream = null, timer = null;
    let draft = null, draftText = '', draftUrl = '', draftDuration = 0, previewUrl = '', active = null, seconds = 0;
    // finishAction: qué hacer con lo grabado cuando el grabador se detiene ('send' lo envía, 'keep' lo deja pendiente).
    // pendingSend: hay un audio guardado que se enviará solo en cuanto la pantalla sea visible y el chat esté libre.
    let startedAt = 0, endedAt = 0, finishAction = 'send', pendingSend = false;
    const controls = [], players = new Set(), urls = new Set();
    let micPermission, micRequest;
    // The browser owns this permission. Reading its live state never opens a prompt.
    navigator.permissions?.query?.({name:'microphone'}).then(status => { micPermission = status; }, () => {});
    const RECORD_LABEL = 'Grabar mensaje de voz';
    const IDLE_HINT = `Toca el micrófono y habla hasta ${limitText}.`;
    const RECORDING_HINT = 'Toca ■ otra vez para enviar. «Cancelar» borra el audio.';
    // Mientras se graba una demostración el micrófono es de la demo: al Coach solo se le escribe.
    const DEMO_HINT = 'Estás grabando la demo. Escríbele a Jorge; el audio vuelve cuando termines.';
    const DEMO_LABEL = 'Mientras grabas la demo, escríbele a Jorge';
    let demoRecording = document.documentElement?.dataset?.tecDemoRecording === 'true';
    const MIC_ICON = '<svg viewBox="0 0 24 24" aria-hidden="true"><g class="mic"><rect x="9" y="3" width="6" height="12" rx="3"/><path d="M6 10v2a6 6 0 0 0 12 0v-2M12 18v3m-4 0h8"/></g><rect class="stop" x="7" y="7" width="10" height="10" rx="2"/></svg>';
    const BIN_ICON = '<svg viewBox="0 0 24 24" aria-hidden="true"><path d="M4 7h16M9 7V4h6v3M6 7l1 14h10l1-14M10 10v7m4-7v7"/></svg>';
    let liveContext = null, liveFinished = null, playbackEpoch = 0;
    // En iPhone, mientras el micrófono captura, el sistema pone el audio en modo de llamada, que suena más bajo.
    // Al cerrar el micrófono se pide el modo de reproducción para la respuesta hablada y, cuando termina,
    // se vuelve al automático para no estorbar a otras grabaciones de la página.
    let sessionTimer = null;
    function audioSession(type) {
      const session = navigator.audioSession;
      if (!session) return;
      window.clearTimeout(sessionTimer);
      sessionTimer = null;
      try { if (session.type !== type) session.type = type; } catch {}
      if (type === 'playback') sessionTimer = window.setTimeout(() => audioSession('auto'), 120000);
    }
    function stopLive() {
      playbackEpoch++;
      liveFinished?.();
      const previous = liveContext;
      liveContext = null;
      previous?.close().catch(() => {});
      audioSession('auto');
    }
    // La respuesta se reproduce en un contexto de audio creado después de cerrar el micrófono: el que estuvo
    // abierto durante la grabación conserva el volumen bajo del modo de llamada. Si el nuevo no arranca,
    // se conserva el anterior, que ya estaba desbloqueado por el gesto.
    function refreshAudio() {
      const Context = window.AudioContext || window.webkitAudioContext;
      const previous = liveContext;
      if (!Context || !previous || liveFinished) return;
      let fresh;
      try { fresh = new Context(); } catch { return; }
      const settle = () => {
        if (fresh.state === 'running' && liveContext === previous && !liveFinished) {
          liveContext = fresh;
          previous.close().catch(() => {});
        } else fresh.close().catch(() => {});
      };
      Promise.resolve(fresh.resume()).then(settle, settle);
    }
    function unlockAudio(){
      if(!liveContext){try{const Context=window.AudioContext || window.webkitAudioContext;if(Context)liveContext=new Context();}catch{}}
      liveContext?.resume().catch(()=>{});
    }
    const makeUrl = blob => { const url = URL.createObjectURL(blob); urls.add(url); return url; };
    const release = url => { if(url) { URL.revokeObjectURL(url); urls.delete(url); } };
    const pausePlayback = (except, preserveContext = false) => {
      if (!preserveContext) stopLive();
      players.forEach(audio => { if(audio !== except) audio.pause(); });
    };
    let stopMeter = () => {};
    function monitorVolume(item, captured) {
      if (!liveContext?.createMediaStreamSource || !window.requestAnimationFrame) return;
      let frame, source, analyser;
      stopMeter = () => {
        window.cancelAnimationFrame(frame);
        source?.disconnect();
        analyser?.disconnect();
        item.meter.style.removeProperty('--voice-level');
        item.meter.hidden = true;
        stopMeter = () => {};
      };
      try {
        source = liveContext.createMediaStreamSource(captured);
        analyser = liveContext.createAnalyser();
        analyser.fftSize = 512;
        source.connect(analyser);
        const data = new Float32Array(analyser.fftSize);
        let peak = 0;
        item.meter.hidden = false;
        function update() {
          analyser.getFloatTimeDomainData(data);
          let energy = 0;
          for (const sample of data) energy += sample * sample;
          peak = Math.max(peak * 0.82, Math.min(1, Math.sqrt(energy / data.length) * 6));
          item.meter.style.setProperty('--voice-level', String(peak));
          frame = window.requestAnimationFrame(update);
        }
        update();
      } catch { stopMeter(); }
    }
    // Al hablar de uno a tres minutos sin tocar la pantalla, el teléfono la apagaría y cortaría la grabación.
    // Donde el navegador lo permite, se le pide mantenerla encendida solo mientras graba.
    let wakeLock = null;
    function keepAwake(stillRecording) {
      navigator.wakeLock?.request?.('screen').then(lock => {
        if (stillRecording()) wakeLock = lock; else lock.release().catch(() => {});
      }, () => {});
    }
    const stopTracks = () => {
      stopMeter();
      wakeLock?.release().catch(() => {});
      wakeLock = null;
      if (!stream) return;
      stream.getTracks().forEach(track => track.stop());
      stream = null;
      audioSession('playback');
    };
    const clearTimer = () => { window.clearInterval(timer); timer = null; };
    const notify = text => { if(active) active.status.textContent = text; };
    const capturing = () => ['requesting','recording','finishing'].includes(phase);
    function render() {
      const locked = chatBusy || phase !== 'idle', left = limit - seconds;
      for(const item of controls) {
        const current = item === active, recording = current && capturing();
        const hasText = Boolean(item.form.querySelector('textarea')?.value.trim()) || hasExtraInput(item.form);
        // Mientras graba, el botón nunca se bloquea: siempre se puede detener y conservar el audio.
        item.record.disabled = !recording && (chatBusy || demoRecording || phase !== 'idle' || Boolean(draft));
        item.record.setAttribute('aria-pressed', recording ? 'true' : 'false');
        item.record.title = recording ? 'Detener y enviar' : demoRecording ? DEMO_LABEL : RECORD_LABEL;
        item.record.classList.toggle('is-recording',recording);
        item.form.classList.toggle('is-voice-recording',recording);
        item.tools.hidden = (!recording && (hasText || Boolean(draft)) && phase === 'idle') || (Boolean(draft) && phase === 'sending');
        item.activity.hidden = !recording;
        item.activity.classList.toggle('is-ending',recording && phase === 'recording' && left <= 30);
        item.status.classList.toggle('sr-only',recording);
        item.elapsed.textContent = formatClock(seconds);
        item.elapsed.hidden = phase === 'requesting';
        item.total.hidden = phase !== 'recording' || left <= 30;
        item.note.textContent = phase === 'requesting' ? 'Preparando micrófono…' : phase === 'finishing' ? 'Guardando…' : left <= 30 ? `Quedan ${left} s` : '';
        item.hint.textContent = recording ? RECORDING_HINT : demoRecording ? DEMO_HINT : IDLE_HINT;
        item.hint.hidden = hasText || (!recording && (phase !== 'idle' || Boolean(draft)));
        item.preview.hidden = !draft || !current || phase !== 'idle';
        item.send.disabled = locked || !draft; item.cancel.disabled = locked;
        item.form.querySelectorAll('textarea, button[type="submit"]').forEach(node => {
          node.disabled = locked;
          if(node.type === 'submit')node.hidden = !hasText || recording || (Boolean(draft) && !recording);
        });
      }
      onBusyChange(capturing() || phase === 'sending' || Boolean(draft));
    }
    function clearDraft() { draft = null; draftText = ''; draftUrl = ''; draftDuration = 0; pendingSend = false; release(previewUrl); previewUrl = ''; controls.forEach(item => item.player.setSource('')); }
    function setDraft(blob, duration) {
      clearDraft();
      // Último recurso: la tasa fija y el corte por tamaño de start() evitan llegar aquí.
      if(blob.size > maxBytes) { notify(`El audio supera ${maxBytes / 1024 / 1024} MB. Graba uno más corto.`); render(); return false; }
      draft = blob; draftDuration = duration; previewUrl = makeUrl(blob); active.player.setSource(previewUrl, duration);return true;
    }
    function stopRecording(action) {
      finishAction = action;clearTimer();endedAt ||= Date.now();
      if(recorder?.state === 'recording') {phase='finishing';recorder.stop();}
      stopTracks();
    }
    // Único camino que borra lo grabado: «Cancelar», Escape dentro del compositor, la papelera del audio pendiente o reset().
    function cancel(message = phase === 'requesting' ? 'Grabación cancelada.' : 'Audio borrado.') {
      generation++;stopRecording('keep');recorder=null;clearDraft();phase='idle';seconds=0;notify(message);render();
      audioSession('auto');
    }
    // Perder el foco (blur), un toque interrumpido o cambiar de ventana no tocan la grabación: sigue corriendo.
    // Si la página se oculta o se cierra el chat, el navegador puede cortar el micrófono en cualquier momento,
    // así que se detiene el grabador de inmediato y lo grabado queda guardado en memoria.
    function suspendCapture(action) {
      if(phase === 'requesting') cancel('Toca el micrófono para grabar.');
      else if(phase === 'recording') { stopRecording(action); render(); }
    }
    // Con la página oculta el navegador puede congelar la subida y la respuesta hablada no puede sonar; con el
    // chat ocupado el envío se rechazaría. Lo más simple y seguro: el audio espera guardado y sale solo al volver.
    function deliver() {
      if(document.hidden || chatBusy) {
        pendingSend = true;
        notify(document.hidden ? 'Guardé tu audio. Lo envío en cuanto vuelvas a esta pantalla.' : 'Guardé tu audio. Lo envío en cuanto el Coach termine.');
        render();return;
      }
      send();
    }
    function flushPending() { if(pendingSend && draft && phase === 'idle' && !chatBusy && !document.hidden) send(); }
    function toggleRecording(item) {
      if(phase === 'idle') { if(!chatBusy && !draft && !demoRecording) start(item); return; }
      // Un doble toque accidental no corta la grabación recién iniciada.
      if(phase !== 'recording' || item !== active || Date.now() - startedAt < MIN_MS) return;
      unlockAudio();stopRecording('send');refreshAudio();render();
    }
    async function start(item) {
      if(chatBusy || demoRecording || phase !== 'idle' || draft)return;
      active=item;
      if(!navigator.mediaDevices?.getUserMedia || !window.MediaRecorder) {notify('La grabación no está disponible. Puedes escribir tu pregunta.');return;}
      if(micPermission?.state === 'denied') {notify('El micrófono está bloqueado. ' + microphoneSettings());return;}
      if(micRequest) {notify('Preparando micrófono…');return;}
      pausePlayback();unlockAudio();seconds=0;
      const token=++generation;phase='requesting';notify('Preparando micrófono…');render();
      try {
        audioSession('play-and-record');
        micRequest=navigator.mediaDevices.getUserMedia({audio:{echoCancellation:true,noiseSuppression:true},video:false});
        const captured=await micRequest;
        // Se canceló o se ocultó la página mientras el navegador pedía el micrófono: todavía no hay nada grabado.
        if(token!==generation || document.hidden){captured.getTracks().forEach(track=>track.stop());audioSession('auto');if(token===generation){phase='idle';notify('Toca el micrófono para grabar.');render();}return;}
        stream=captured;const mime=recordingMime(),chunks=[];let bytes=0,warned=0,recording;
        try { recording=new MediaRecorder(captured,recordingOptions(mime)); }
        catch { recording=new MediaRecorder(captured,mime?{mimeType:mime}:{}); }
        recorder=recording;startedAt=Date.now();endedAt=0;finishAction='send';
        recording.addEventListener('dataavailable',event=>{
          if(!event.data.size || token!==generation)return;
          chunks.push(event.data);bytes+=event.data.size;
          // Nunca se descarta por tamaño: antes de rebasar el tope de subida se detiene y se envía lo grabado.
          if(bytes>maxBytes*0.9 && phase==='recording'){notify('Tu audio llegó al tamaño máximo. Lo envío ahora.');stopRecording('send');refreshAudio();render();}
        });
        // También llega aquí si el sistema corta el micrófono (una llamada, por ejemplo): se conserva lo grabado.
        recording.addEventListener('stop',()=>{
          if(token!==generation)return;clearTimer();stopTracks();recorder=null;phase='idle';
          const length=(endedAt || Date.now())-startedAt,blob=new Blob(chunks,{type:recording.mimeType || mime || 'audio/webm'});
          if(length<MIN_MS || blob.size<64){seconds=0;notify('No alcancé a grabar nada. Toca el micrófono para intentarlo otra vez.');render();return;}
          if(!setDraft(blob, Math.min(limit, length / 1000)))return;
          if(finishAction==='send')deliver();
          else {notify('Guardé tu audio. Puedes enviarlo o borrarlo.');render();}
        });
        recording.addEventListener('error',()=>{
          if(token!==generation)return;
          if(recording.state==='recording')stopRecording('keep');else finishAction='keep';
        });
        recording.start(500);monitorVolume(item, captured);phase='recording';
        keepAwake(()=>token===generation && phase==='recording');
        notify(`Grabando. Toca el micrófono otra vez para enviar o «Cancelar» para borrar. Máximo ${limitText}.`);render();
        timer=window.setInterval(()=>{
          const now=Math.min(limit,Math.floor((Date.now()-startedAt)/1000));
          if(now===seconds)return;
          seconds=now;const left=limit-seconds;
          // Al llegar al máximo se detiene y se envía solo; nunca se descarta.
          if(left<=0){notify(`Llegaste a ${limitText}. Envío tu audio.`);stopRecording('send');refreshAudio();}
          else if(left<=10 && warned<2){warned=2;notify('Quedan 10 segundos.');}
          else if(left<=30 && warned<1){warned=1;notify('Quedan 30 segundos.');}
          render();
        },250);
      } catch(error) {
        if(token!==generation)return;stopTracks();audioSession('auto');recorder=null;phase='idle';
        notify(error.name==='NotAllowedError' ? 'El micrófono está bloqueado. ' + microphoneSettings() : 'No pude abrir el micrófono. Revisa que esté disponible.');render();
      } finally {
        micRequest=null;
      }
    }
    async function request(url, options = {}) {
      const controller = new AbortController(), timeout = window.setTimeout(()=>controller.abort(),120000);
      try {
        const response = await fetch(url,{credentials:'same-origin',...options,signal:controller.signal});
        if(!response.ok) { const data = await response.json().catch(()=>({})); throw new Error(data.error || 'No pude completar el audio. Vuelve a intentarlo.'); }
        return response;
      } catch(error) { if(error.name === 'AbortError')throw new Error('El audio tardó demasiado. Puedes reintentarlo.'); throw error; }
      finally {window.clearTimeout(timeout);}
    }
    async function send() {
      if(!draft || phase !== 'idle' || chatBusy)return;
      pendingSend = false;
      // Keep the AudioContext unlocked by the microphone gesture while awaiting the reply.
      pausePlayback(null, true);unlockAudio();
      const audio = draft; phase='sending'; notify('Transcribiendo tu audio…'); render();
      try {
        if (!draftText) {
          const headers = {'Content-Type':audio.type || 'audio/webm'};
          if (userId) headers['X-TEC-Account-ID'] = String(userId);
          const response = await request(voiceApi?.transcription || '/api/coach/voice/transcribe',{method:'POST',headers,body:audio});
          const data = await response.json();
          draftText = data.text?.trim();
          if (!draftText) throw new Error('No escuché una pregunta clara. Tu audio sigue aquí.');
        }
        notify('');
        draftUrl ||= makeUrl(audio);
        const result = await sendMessage(draftText,{audioUrl:draftUrl, audioDuration:draftDuration});
        if(result?.ok) { clearDraft(); notify(''); }
        else notify('No se completó la respuesta. Conservé tu audio para que puedas reintentar.');
      } catch(error) { notify(error.message || 'No pude enviar el audio. Tu grabación sigue aquí.'); }
      finally {phase='idle';render();}
    }
    const button = (text,handler) => { const node=document.createElement('button');node.type='button';node.textContent=text;if(handler)node.addEventListener('click',handler);return node; };
    function createPlayer(title, playLabel, isCurrent = () => true) {
      const card = document.createElement('div');
      card.className = 'coach-voice-card';
      const audio = document.createElement('audio');
      audio.hidden = true;
      audio.preload = 'metadata';
      const label = document.createElement('span');
      label.className = 'coach-voice-card-label';
      label.textContent = title;
      const time = document.createElement('span');
      time.className = 'coach-voice-card-time';
      const track = document.createElement('span');
      track.className = 'coach-voice-card-track';
      track.setAttribute('aria-hidden', 'true');
      const fill = document.createElement('span');
      track.append(fill);
      let streamToggle = null, streamPlaying = false, recordedDuration = 0;
      const play = button('', () => {
        if (!isCurrent()) return;
        if (streamToggle) return streamToggle();
        if (!audio.paused) audio.pause();
        else audio.play().catch(() => { time.textContent = 'Reintentar'; });
      });
      play.className = 'coach-voice-play';
      play.innerHTML = '<svg viewBox="0 0 24 24" aria-hidden="true"><path class="play" d="M9 5.5v13l10-6.5z"/><path class="pause" d="M8 6h3v12H8zm6 0h3v12h-3z"/></svg>';
      card.append(play, label, time, track, audio);
      function update() {
        const playing = streamToggle ? streamPlaying : !audio.paused && !audio.ended;
        const duration = Number.isFinite(audio.duration) ? audio.duration : recordedDuration;
        const knownDuration = Number.isFinite(duration) && duration > 0;
        const elapsed = audio.currentTime || 0;
        play.setAttribute('aria-label', playing ? 'Pausar ' + title.toLowerCase() : playLabel);
        play.disabled = !streamToggle && !audio.getAttribute('src');
        card.classList.toggle('is-playing', playing);
        card.classList.toggle('is-streaming', Boolean(streamToggle));
        time.textContent = streamToggle ? (playing ? 'En curso' : 'En pausa')
          : knownDuration ? formatClock(playing ? elapsed : duration) : (elapsed ? formatClock(elapsed) : '');
        fill.setAttribute('style', `width:${knownDuration ? Math.min(100, elapsed / duration * 100) : 0}%`);
      }
      for (const event of ['loadedmetadata', 'durationchange', 'play', 'pause', 'ended', 'timeupdate', 'emptied']) {
        audio.addEventListener(event, update);
      }
      audio.addEventListener('play', () => pausePlayback(audio));
      players.add(audio);
      update();
      return { card, audio,
        setSource(url, duration = 0) {
          recordedDuration = duration;
          if (url) audio.setAttribute('src', url); else audio.removeAttribute('src');
          update();
        },
        setStream(toggle, playing = false) { streamToggle = toggle; streamPlaying = playing; update(); }
      };
    }

    for(const form of forms) {
      form.classList.add('coach-voice-composer');
      const tools=document.createElement('div');tools.className='coach-voice-tools';
      const item={form,tools};
      const toolbar = (toolbarSelector && form.querySelector(toolbarSelector)) || form;
      // Con el campo de texto como hijo directo, la barra de grabación ocupa su misma celda y el compositor no cambia de alto.
      if(toolbar === form) form.classList.add('coach-voice-inline');
      item.record=button('');item.record.innerHTML=MIC_ICON;item.record.className='coach-voice-hold';
      item.record.setAttribute('aria-label',RECORD_LABEL);
      item.record.addEventListener('contextmenu',event=>event.preventDefault());
      // Un toque empieza y otro toque envía. `click` cubre por igual dedo, ratón, Espacio/Enter y lectores de pantalla.
      item.record.addEventListener('click',event=>{event.preventDefault();toggleRecording(item);});
      item.status=document.createElement('p');item.status.className='coach-voice-status';item.status.setAttribute('role','status');item.status.setAttribute('aria-live','polite');
      item.hint=document.createElement('small');item.hint.className='coach-voice-hint';item.hint.textContent=IDLE_HINT;
      // Barra de grabación: vive dentro del compositor, en flujo normal, para no tapar el chat ni lo que esté encima.
      item.activity=document.createElement('div');item.activity.className='coach-voice-activity';item.activity.hidden=true;
      const meter=document.createElement('span');meter.className='coach-voice-meter';item.meter=meter;meter.hidden=true;meter.setAttribute('aria-hidden','true');meter.innerHTML='<i></i><i></i><i></i><i></i><i></i>';
      // El reloj cambia cada segundo; los avisos para lectores de pantalla salen por el estado (aria-live), no por aquí.
      item.elapsed=document.createElement('span');item.elapsed.className='coach-voice-elapsed';item.elapsed.setAttribute('aria-hidden','true');
      item.total=document.createElement('span');item.total.className='coach-voice-limit';item.total.setAttribute('aria-hidden','true');item.total.textContent='/ '+formatClock(limit);
      item.note=document.createElement('span');item.note.className='coach-voice-note';item.note.setAttribute('aria-hidden','true');
      item.discard=button('Cancelar',()=>{cancel();item.record.focus();});item.discard.className='coach-voice-discard';
      item.discard.setAttribute('aria-label','Cancelar y borrar la grabación');
      item.activity.append(meter,item.elapsed,item.total,item.note,item.discard);
      item.preview=document.createElement('div');item.preview.className='coach-voice-preview';item.preview.hidden=true;
      item.player = createPlayer('Audio pendiente', 'Escuchar el audio pendiente');
      item.send=button('Enviar audio',send);item.cancel=button('',()=>cancel());item.cancel.innerHTML=BIN_ICON;item.cancel.setAttribute('aria-label','Borrar audio pendiente');
      item.preview.append(item.player.card,item.send,item.cancel);tools.append(item.record);
      toolbar.append(tools);
      form.append(item.hint,item.activity,item.preview,item.status);controls.push(item);
      form.querySelector('textarea')?.addEventListener('input',render);
      // Cerrar el chat a media grabación no la borra: queda como audio pendiente para enviarlo o borrarlo al volver.
      form.closest('dialog')?.addEventListener('close',()=>{pausePlayback();suspendCapture('keep');});
    }
    window.addEventListener('tec:coach-session-change', () => pausePlayback());
    async function speak(text, handles, reference, { isCurrent = () => true, verbatim = false } = {}) {
      if (!isCurrent()) return false;
      const panels = (handles || []).map(({card, target}) => {
        const panel = document.createElement('div');
        panel.className = 'coach-voice-reply';
        const label = document.createElement('small');
        label.setAttribute('role','status');
        const player = createPlayer('Voz del ' + speaker, 'Reproducir la respuesta del ' + speaker, isCurrent);
        const retry = button('Reintentar audio', () => {
          if (isCurrent()) { unlockAudio(); generate(); }
        });
        player.card.hidden = retry.hidden = true;
        panel.append(label, player.card, retry);
        card.classList.add('coach-message-with-voice');
        card.append(panel);
        if (target) target.scrollTop = target.scrollHeight;
        return { panel, label, player, retry, target };
      });
      let context, paused = false, loading = false, playing = false;
      function status(value) {
        panels.forEach(p => { p.label.textContent = value; p.label.hidden = !value; });
      }
      function showPlayer({ player, target }) {
        const atBottom = target && target.scrollHeight - target.scrollTop - target.clientHeight < 24;
        player.card.hidden = false;
        if (atBottom) target.scrollTop = target.scrollHeight;
      }
      function complete() {
        playing = paused = false;
        panels.forEach(p => p.player.setStream(null));
        status('');
        if (liveFinished === complete) liveFinished = null;
        audioSession('auto');
      }
      async function toggle() {
        if (!playing || context !== liveContext) return;
        try {
          if (paused) await context.resume(); else await context.suspend();
          if (context !== liveContext || !playing) return;
          paused = context.state === 'suspended';
          panels.forEach(p => p.player.setStream(toggle, !paused));
        } catch { pausePlayback(); }
      }
      async function generate() {
        if (loading || !isCurrent()) return false;
        loading = true;
        pausePlayback(null, true);
        if (liveFinished) stopLive();
        context = liveContext;
        const attempt = playbackEpoch;
        const canPlay = () => attempt === playbackEpoch && isCurrent() && !document.hidden &&
          context === liveContext && panels.some(p => p.panel.getClientRects().length);
        let skipped = false;
        const queue = context ? createSpeechBuffer(context, complete) : null;
        panels.forEach(p => { p.retry.hidden = true; p.player.card.hidden = true; });
        status('Preparando la voz del ' + speaker + '…');
        try {
          function enqueue(bytes) {
            if (!queue || skipped) return;
            if (!canPlay() || (context.state !== 'running' && !paused)) { skipped = true; return; }
            if (queue.append(bytes) && !playing) {
              playing = true;
              liveFinished = complete;
              status('');
              panels.forEach(p => { p.player.setStream(toggle, true); showPlayer(p); });
            }
          }
          // Bound synthesis requests while retaining one continuous player for the full reply.
          const parts = voiceApi ? [text] : speechParts(text);
          const waves = [];
          for (const part of parts) {
            if (!isCurrent()) return false;
            const payload = voiceApi ? reference : {accountId:String(userId), text:part, verbatim};
            const response = await request(voiceApi?.speech || '/api/coach/voice/speech', {
              method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify({...payload, stream:true})
            });
            const wave = await readSpeech(response, enqueue);
            if (wave.size <= 44) throw new Error('No se completó el audio.');
            waves.push(wave);
          }
          if (!isCurrent()) return false;
          const pcm = waves.map(wave => wave.slice(44));
          const blob = waves.length === 1 ? waves[0] : pcmWave(pcm, pcm.reduce((n, part) => n + part.size, 0));
          if (playing && (!canPlay() || skipped)) stopLive();
          const url = makeUrl(blob);
          panels.forEach(p => { p.player.setSource(url); showPlayer(p); });
          if (playing) queue.finish();
          else {
            complete();
            if (canPlay()) {
              const visible = panels.find(p => p.panel.getClientRects().length);
              await visible?.player.audio.play().catch(() => status('Toca reproducir para escuchar al ' + speaker + '.'));
            }
          }
          return true;
        } catch (error) {
          if (!isCurrent()) return false;
          if (context === liveContext && attempt === playbackEpoch) stopLive();
          status(error.message || 'Tu respuesta escrita está lista. Puedes reintentar el audio.');
          panels.forEach(p => { p.player.card.hidden = true; p.retry.hidden = false; });
          return false;
        } finally { loading = false; }
      }
      return generate();
    }
    document.addEventListener('visibilitychange',()=>{
      if(document.hidden){suspendCapture('send');pausePlayback();}
      else flushPending();
    });
    // Escape equivale a «Cancelar» solo con el foco dentro del compositor, para no borrar un audio largo por accidente.
    document.addEventListener('keydown',event=>{
      if(event.key==='Escape' && capturing() && active?.form.contains(event.target)){event.preventDefault();cancel();active.record.focus();}
    });
    window.addEventListener('pagehide',event=>{
      pausePlayback();
      // Si el navegador guarda la página para volver atrás, el audio sigue en memoria y se envía al regresar.
      if(event.persisted){suspendCapture('send');return;}
      generation++;stopRecording('keep');urls.forEach(release);
    });
    // La demo empezó o terminó. Si había un audio a medias se detiene y queda guardado: no se pierde ni estorba a la demo.
    window.addEventListener('tec:demo-recording',event=>{
      demoRecording = event.detail?.recording === true;
      if(demoRecording && capturing()) suspendCapture('keep');
      render();
    });
    render();
    // El envío pendiente sale en una microtarea para que quien liberó el chat termine antes de volver a ocuparlo.
    return { setChatBusy(value){chatBusy=Boolean(value);render();if(!chatBusy && pendingSend)Promise.resolve().then(flushPending);}, pausePlayback, speak,
      reset() {
        pausePlayback();
        cancel('');
        urls.forEach(release);
        players.clear();
        controls.forEach(item => players.add(item.player.audio));
      },
      attachInput(handles,url,duration){
        for (const {card, target} of handles) {
          const player = createPlayer('Tu audio', 'Escuchar mi pregunta de audio');
          player.setSource(url,duration);
          card.classList.add('coach-message-with-voice');
          card.append(player.card);
          if (target) target.scrollTop = target.scrollHeight;
        }
      } };
  }
  window.TecCoachVoice=Object.freeze({init,recordingMime,recordingOptions,formatClock,readSpeech,createSpeechBuffer,microphoneSettings});
})();
