(() => {
  'use strict';
  const MAX_BYTES = 8 * 1024 * 1024, MAX_SECONDS = 90;
  function recordingMime(Recorder = window.MediaRecorder) {
    return ['audio/webm;codecs=opus','audio/mp4','audio/webm'].find(type => Recorder?.isTypeSupported?.(type)) || '';
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
    const header=new ArrayBuffer(44), view=new DataView(header), write=(at,text)=>[...text].forEach((char,i)=>view.setUint8(at+i,char.charCodeAt(0)));
    write(0,'RIFF');view.setUint32(4,length+36,true);write(8,'WAVE');write(12,'fmt ');view.setUint32(16,16,true);view.setUint16(20,1,true);view.setUint16(22,1,true);view.setUint32(24,24000,true);view.setUint32(28,48000,true);view.setUint16(32,2,true);view.setUint16(34,16,true);write(36,'data');view.setUint32(40,length,true);
    return new Blob([header,...chunks],{type:'audio/wav'});
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

  function init({ userId, forms = [], sendMessage }) {
    if (!userId || !forms.length) return null;
    let phase = 'idle', chatBusy = false, generation = 0, recorder = null, stream = null, timer = null;
    let draft = null, draftDuration = 0, previewUrl = '', active = null, gesture = null, seconds = 0, finishAction = 'hold';
    const controls = [], players = new Set(), urls = new Set();
    const HOLD_LABEL = 'Mantén presionado para grabar. Suelta para enviar. Desliza a la izquierda para borrar.';
    const MIC_ICON = '<svg viewBox="0 0 24 24" aria-hidden="true"><rect x="9" y="3" width="6" height="12" rx="3"/><path d="M6 10v2a6 6 0 0 0 12 0v-2M12 18v3m-4 0h8"/></svg>';
    const BIN_ICON = '<svg viewBox="0 0 24 24" aria-hidden="true"><path d="M4 7h16M9 7V4h6v3M6 7l1 14h10l1-14M10 10v7m4-7v7"/></svg>';
    let liveContext = null, liveFinished = null, playbackEpoch = 0;
    function stopLive() {
      playbackEpoch++;
      liveFinished?.();
      const previous = liveContext;
      liveContext = null;
      previous?.close().catch(() => {});
    }
    function unlockAudio(){
      if(!liveContext){try{const Context=window.AudioContext || window.webkitAudioContext;if(Context)liveContext=new Context();}catch{}}
      liveContext?.resume().catch(()=>{});
    }
    const makeUrl = blob => { const url = URL.createObjectURL(blob); urls.add(url); return url; };
    const release = url => { if(url) { URL.revokeObjectURL(url); urls.delete(url); } };
    const pausePlayback = (except, preserveContext = false) => {if(!preserveContext)stopLive();players.forEach(audio => { if(audio !== except) audio.pause(); });};
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
    const stopTracks = () => { stopMeter(); stream?.getTracks().forEach(track => track.stop()); stream = null; };
    const clearTimer = () => { window.clearInterval(timer); timer = null; };
    const notify = text => { if(active) active.status.textContent = text; };
    const capturing = () => ['requesting','recording','finishing','ready'].includes(phase);
    function render() {
      const locked = chatBusy || phase !== 'idle';
      for(const item of controls) {
        const current = item === active, recording = current && capturing(), cancelled = recording && gesture?.cancelled;
        const hasText = Boolean(item.form.querySelector('textarea')?.value.trim());
        item.record.disabled = chatBusy || (!recording && phase !== 'idle') || (Boolean(draft) && !recording);
        item.record.setAttribute('aria-label', recording ? 'Grabando. Suelta para enviar; desliza a la izquierda o pulsa Escape para borrar.' : HOLD_LABEL);
        item.record.setAttribute('aria-pressed', recording ? 'true' : 'false');
        item.record.classList.toggle('is-recording',recording);
        item.record.classList.toggle('is-cancelling',Boolean(cancelled));
        item.tools.hidden = (!recording && (hasText || Boolean(draft)) && phase === 'idle') || (Boolean(draft) && phase === 'sending');
        item.activity.hidden = !recording;
        item.status.classList.toggle('sr-only',recording);
        item.activity.classList.toggle('is-cancelling',Boolean(cancelled));
        item.elapsed.textContent = `${Math.floor(seconds/60)}:${String(seconds%60).padStart(2,'0')}`;
        item.slide.textContent = cancelled ? 'Suelta para borrar' : phase === 'requesting' ? 'Preparando micrófono…' : phase === 'ready' ? 'Suelta para enviar' : '← Desliza para borrar';
        item.hint.hidden = hasText || phase !== 'idle' || (Boolean(draft) && !recording);
        item.preview.hidden = !draft || !current || phase !== 'idle';
        item.send.disabled = locked || !draft; item.cancel.disabled = locked;
        item.form.querySelectorAll('textarea, button[type="submit"]').forEach(node => {
          node.disabled = locked;
          if(node.type === 'submit')node.hidden = !hasText || recording || (Boolean(draft) && !recording);
        });
      }
    }
    function clearDraft() { draft = null; draftDuration = 0; release(previewUrl); previewUrl = ''; controls.forEach(item => item.player.setSource('')); }
    function setDraft(blob, duration) {
      clearDraft();
      if(blob.size < 64 || blob.size > MAX_BYTES) { notify(blob.size > MAX_BYTES ? 'El audio supera 8 MB. Graba uno más corto.' : 'Mantén presionado un poco más para grabar.'); render(); return false; }
      draft = blob; draftDuration = duration; previewUrl = makeUrl(blob); active.player.setSource(previewUrl, duration);return true;
    }
    function stopRecording(action = 'hold') {
      finishAction = action;clearTimer();
      if(recorder?.state === 'recording') {phase='finishing';recorder.stop();}
      stopTracks();
    }
    function resetGesture() {
      const previous=gesture;gesture=null;
      if(previous?.pointerId != null){try{previous.item.record.releasePointerCapture(previous.pointerId);}catch{}}
    }
    function cancel(message = 'Audio borrado.') {
      generation++; resetGesture();clearTimer();stopRecording();recorder=null;clearDraft();phase='idle';seconds=0;notify(message);render();
    }
    function finish() {
      if(!gesture)return;
      const cancelled=gesture.cancelled;resetGesture();
      if(cancelled){cancel();return;}
      if(phase==='requesting'){cancel('Si el navegador pide permiso, acéptalo y mantén presionado otra vez para grabar.');return;}
      unlockAudio();
      if(phase==='recording'){stopRecording('send');render();}
      else if(phase==='finishing')finishAction='send';
      else if(phase==='ready'){phase='idle';send();}
    }
    async function start(item) {
      if(chatBusy || phase !== 'idle' || draft)return;
      active=item;pausePlayback();unlockAudio();seconds=0;
      if(!navigator.mediaDevices?.getUserMedia || !window.MediaRecorder) {resetGesture();notify('Este navegador no permite grabar. Puedes escribir tu pregunta.');return;}
      const token=++generation;phase='requesting';notify('Permite el micrófono y mantén presionado para grabar.');render();
      try {
        const captured=await navigator.mediaDevices.getUserMedia({audio:{echoCancellation:true,noiseSuppression:true},video:false});
        if(token!==generation || document.hidden || !gesture){captured.getTracks().forEach(track=>track.stop());if(token===generation){phase='idle';render();}return;}
        stream=captured;const mime=recordingMime(),chunks=[];
        const recording=new MediaRecorder(captured,mime?{mimeType:mime}:{});recorder=recording;const started=Date.now();
        recording.addEventListener('dataavailable',event=>{if(event.data.size && token===generation)chunks.push(event.data);});
        recording.addEventListener('stop',()=>{
          if(token!==generation)return;clearTimer();stopTracks();recorder=null;
          if(Date.now()-started<350){phase='idle';resetGesture();notify('Mantén presionado para grabar tu pregunta.');render();return;}
          const blob=new Blob(chunks,{type:recording.mimeType || mime || 'audio/webm'});
          phase='idle';if(!setDraft(blob, Math.min(MAX_SECONDS, (Date.now() - started) / 1000))){resetGesture();render();return;}
          if(finishAction==='send'){send();}
          else {phase='ready';notify('Llegaste a 90 segundos. Suelta para enviar o desliza para borrar.');render();}
        });
        recording.addEventListener('error',()=>{if(token===generation)cancel('No pude completar la grabación. Vuelve a intentarlo.');});
        recording.start(500);monitorVolume(item, captured);phase='recording';notify('Grabando. Suelta para enviar o desliza a la izquierda para borrar.');render();
        timer=window.setInterval(()=>{seconds++;render();if(seconds>=MAX_SECONDS)stopRecording();},1000);
      } catch(error) {
        if(token!==generation)return;stopTracks();resetGesture();phase='idle';
        notify(error.name==='NotAllowedError' ? 'El micrófono está bloqueado. Permítelo en el navegador para grabar.' : 'No pude abrir el micrófono. Revisa que esté disponible.');render();
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
      // Keep the AudioContext unlocked by the microphone gesture while awaiting the reply.
      pausePlayback(null, true);unlockAudio();
      const audio = draft; phase='sending'; notify('Transcribiendo tu audio…'); render();
      try {
        const response = await request('/api/coach/voice/transcribe',{method:'POST',headers:{'Content-Type':audio.type || 'audio/webm','X-TEC-Account-ID':String(userId)},body:audio});
        const data = await response.json(); if(!data.text?.trim()) throw new Error('No escuché una pregunta clara. Tu audio sigue aquí.');
        notify('');
        const result = await sendMessage(data.text,{audioUrl:makeUrl(audio), audioDuration:draftDuration});
        if(result?.ok) { clearDraft(); notify(''); }
        else notify('No se completó la respuesta. Conservé tu audio para que puedas reintentar.');
      } catch(error) { notify(error.message || 'No pude enviar el audio. Tu grabación sigue aquí.'); }
      finally {phase='idle';render();}
    }
    const button = (text,handler) => { const node=document.createElement('button');node.type='button';node.textContent=text;if(handler)node.addEventListener('click',handler);return node; };
    function createPlayer(title, playLabel) {
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
        if (streamToggle) return streamToggle();
        if (!audio.paused) audio.pause();
        else audio.play().catch(() => { time.textContent = 'Reintentar'; });
      });
      play.className = 'coach-voice-play';
      play.innerHTML = '<svg viewBox="0 0 24 24" aria-hidden="true"><path class="play" d="M9 5.5v13l10-6.5z"/><path class="pause" d="M8 6h3v12H8zm6 0h3v12h-3z"/></svg>';
      card.append(play, label, time, track, audio);
      const formatTime = value => `${Math.floor(value / 60)}:${String(Math.floor(value % 60)).padStart(2, '0')}`;
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
          : knownDuration ? formatTime(playing ? elapsed : duration) : (elapsed ? formatTime(elapsed) : '');
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
      item.record=button('');item.record.innerHTML=MIC_ICON;item.record.className='coach-voice-hold';item.record.title=HOLD_LABEL;
      item.record.addEventListener('contextmenu',event=>event.preventDefault());
      item.record.addEventListener('pointerdown',event=>{
        if(event.button!==0 || event.isPrimary===false || gesture || chatBusy || phase!=='idle' || draft)return;
        event.preventDefault();gesture={item,pointerId:event.pointerId,startX:event.clientX,cancelled:false};
        try{item.record.setPointerCapture(event.pointerId);}catch{}start(item);
      });
      item.record.addEventListener('pointermove',event=>{
        if(gesture?.item!==item || gesture.pointerId!==event.pointerId)return;
        gesture.cancelled=event.clientX-gesture.startX<=-64;render();
      });
      item.record.addEventListener('pointerup',event=>{if(gesture?.item===item && gesture.pointerId===event.pointerId){event.preventDefault();finish();}});
      for(const type of ['pointercancel','lostpointercapture'])item.record.addEventListener(type,event=>{if(gesture?.item===item && gesture.pointerId===event.pointerId)cancel();});
      item.record.addEventListener('keydown',event=>{
        if(event.key==='Escape' && capturing()){event.preventDefault();cancel();return;}
        if(![' ','Enter'].includes(event.key))return;event.preventDefault();
        if(!event.repeat && !gesture && phase==='idle' && !chatBusy && !draft){gesture={item,key:event.key,cancelled:false};start(item);}
      });
      item.record.addEventListener('keyup',event=>{if(gesture?.item===item && gesture.key===event.key){event.preventDefault();finish();}});
      // Assistive technologies may activate a button without pointer or key events.
      item.record.addEventListener('click',event=>{
        event.preventDefault();if(event.detail!==0)return;
        if(gesture?.assistive && gesture.item===item)finish();
        else if(!gesture && phase==='idle' && !chatBusy && !draft){gesture={item,assistive:true,cancelled:false};start(item);}
      });
      item.status=document.createElement('p');item.status.className='coach-voice-status';item.status.setAttribute('role','status');item.status.setAttribute('aria-live','polite');
      item.hint=document.createElement('small');item.hint.className='coach-voice-hint';item.hint.textContent='Mantén el micrófono para hablar';
      item.activity=document.createElement('div');item.activity.className='coach-voice-activity';item.activity.hidden=true;item.activity.setAttribute('aria-hidden','true');
      const bin=document.createElement('span');bin.className='coach-voice-bin';bin.innerHTML=BIN_ICON;
      const meter=document.createElement('span');meter.className='coach-voice-meter';item.meter=meter;meter.hidden=true;meter.innerHTML='<i></i><i></i><i></i><i></i><i></i>';
      item.elapsed=document.createElement('span');item.elapsed.className='coach-voice-elapsed';
      item.slide=document.createElement('span');item.slide.className='coach-voice-slide';item.activity.append(bin,meter,item.elapsed,item.slide);
      item.preview=document.createElement('div');item.preview.className='coach-voice-preview';item.preview.hidden=true;
      item.player = createPlayer('Audio pendiente', 'Escuchar el audio pendiente');
      item.send=button('Reintentar audio',send);item.cancel=button('',()=>cancel());item.cancel.innerHTML=BIN_ICON;item.cancel.setAttribute('aria-label','Borrar audio pendiente');
      item.preview.append(item.player.card,item.send,item.cancel);tools.append(item.record);form.append(tools,item.hint,item.activity,item.preview,item.status);controls.push(item);
      form.querySelector('textarea')?.addEventListener('input',render);
      form.closest('dialog')?.addEventListener('close',()=>{pausePlayback();if(capturing())cancel();});
    }
    async function speak(text, handles) {
      const panels = (handles || []).map(({card, target}) => {
        const panel = document.createElement('div');
        panel.className = 'coach-voice-reply';
        const label = document.createElement('small');
        label.setAttribute('role','status');
        const player = createPlayer('Voz del Coach', 'Reproducir la respuesta del Coach');
        const retry = button('Reintentar audio', () => {
          unlockAudio();
          generate();
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
        if (loading) return;
        loading = true;
        pausePlayback(null, true);
        if (liveFinished) stopLive();
        context = liveContext;
        const attempt = playbackEpoch;
        const canPlay = () => attempt === playbackEpoch && !document.hidden &&
          context === liveContext && panels.some(p => p.panel.getClientRects().length);
        let skipped = false;
        const queue = context ? createSpeechBuffer(context, complete) : null;
        panels.forEach(p => { p.retry.hidden = true; p.player.card.hidden = true; });
        status('Preparando la voz del Coach…');
        try {
          const response = await request('/api/coach/voice/speech', {method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({accountId:String(userId),text,stream:true})});
          const blob = await readSpeech(response, bytes => {
            if (!queue || skipped) return;
            if (!canPlay() || (context.state !== 'running' && !paused)) { skipped = true; return; }
            if (queue.append(bytes) && !playing) {
              playing = true;
              liveFinished = complete;
              status('');
              panels.forEach(p => { p.player.setStream(toggle, true); showPlayer(p); });
            }
          });
          if (blob.size <= 44) throw new Error('No se completó el audio.');
          if (playing && (!canPlay() || skipped)) stopLive();
          const url = makeUrl(blob);
          panels.forEach(p => { p.player.setSource(url); showPlayer(p); });
          if (playing) queue.finish();
          else {
            complete();
            if (canPlay()) {
              const visible = panels.find(p => p.panel.getClientRects().length);
              await visible?.player.audio.play().catch(() => status('Toca reproducir para escuchar al Coach.'));
            }
          }
          return true;
        } catch (error) {
          if (context === liveContext && attempt === playbackEpoch) stopLive();
          status(error.message || 'Tu respuesta escrita está lista. Puedes reintentar el audio.');
          panels.forEach(p => { p.player.card.hidden = true; p.retry.hidden = false; });
          return false;
        } finally { loading = false; }
      }
      return generate();
    }
    document.addEventListener('visibilitychange',()=>{if(document.hidden){if(capturing())cancel();pausePlayback();}});
    window.addEventListener('blur',()=>{if(capturing())cancel();});
    document.addEventListener('keydown',event=>{if(event.key==='Escape' && capturing())cancel();});
    window.addEventListener('pagehide',event=>{generation++;resetGesture();clearTimer();stopRecording();pausePlayback();if(!event.persisted)urls.forEach(release);});
    render();
    return { setChatBusy(value){chatBusy=Boolean(value);render();}, pausePlayback, speak,
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
  window.TecCoachVoice=Object.freeze({init,recordingMime,readSpeech,createSpeechBuffer});
})();
