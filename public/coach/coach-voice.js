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
  function init({ userId, forms = [], sendMessage }) {
    if (!userId || !forms.length) return null;
    let phase = 'idle', chatBusy = false, generation = 0, recorder = null, stream = null, timer = null;
    let draft = null, previewUrl = '', active = null;
    const controls = [], players = new Set(), urls = new Set();
    let liveContext = null;const liveStopButtons=new Set();
    const stopLive = () => {liveStopButtons.forEach(button=>{button.hidden=true;});const previous=liveContext;liveContext=null;previous?.close().catch(()=>{});};
    function unlockAudio(){
      if(!liveContext){try{const Context=window.AudioContext || window.webkitAudioContext;if(Context)liveContext=new Context();}catch{}}
      liveContext?.resume().catch(()=>{});
    }
    const makeUrl = blob => { const url = URL.createObjectURL(blob); urls.add(url); return url; };
    const release = url => { if(url) { URL.revokeObjectURL(url); urls.delete(url); } };
    const pausePlayback = (except, preserveContext = false) => {if(!preserveContext)stopLive();players.forEach(audio => { if(audio !== except) audio.pause(); });};
    const stopTracks = () => { stream?.getTracks().forEach(track => track.stop()); stream = null; };
    const clearTimer = () => { window.clearInterval(timer); timer = null; };
    const notify = text => { if(active) active.status.textContent = text; };
    function render() {
      const locked = chatBusy || phase !== 'idle';
      for(const item of controls) {
        item.record.disabled = locked && !(phase === 'recording' && item === active);
        item.record.textContent = phase === 'recording' && item === active ? 'Terminar grabación' : 'Grabar audio';
        item.record.setAttribute('aria-pressed',phase === 'recording' && item === active ? 'true' : 'false');
        item.upload.disabled = locked; item.send.disabled = locked || !draft; item.cancel.disabled = chatBusy || phase === 'sending';
        item.preview.hidden = !draft || item !== active;
        item.cancel.hidden = item !== active || (!draft && phase !== 'recording' && phase !== 'requesting');
        item.form.querySelectorAll('textarea, button[type="submit"]').forEach(node => { node.disabled = locked; });
      }
    }
    function clearDraft() { draft = null; release(previewUrl); previewUrl = ''; controls.forEach(item => {item.audio.removeAttribute('src');}); }
    function setDraft(blob) {
      clearDraft();
      if(blob.size < 64 || blob.size > MAX_BYTES) { notify(blob.size > MAX_BYTES ? 'El audio supera 8 MB. Usa una grabación más corta.' : 'No se grabó suficiente audio. Vuelve a intentarlo.'); render(); return; }
      draft = blob; previewUrl = makeUrl(blob); active.audio.src = previewUrl;
      notify('Audio listo. Puedes escucharlo antes de enviarlo.'); render();
    }
    function stopRecording() {
      clearTimer();
      if(recorder?.state === 'recording') recorder.stop();
      stopTracks();
    }
    function cancel() {
      generation++; clearTimer(); stopRecording(); recorder = null; clearDraft(); phase = 'idle'; notify('Audio descartado.'); render();
    }
    async function start(item) {
      if(chatBusy || phase !== 'idle') return;
      active = item; pausePlayback();
      if(!navigator.mediaDevices?.getUserMedia || !window.MediaRecorder) { notify('Este navegador no permite grabar aquí. Puedes adjuntar un audio.'); return; }
      const token = ++generation; phase = 'requesting'; notify('Permite usar el micrófono para grabar.'); render();
      try {
        const captured = await navigator.mediaDevices.getUserMedia({audio:{echoCancellation:true,noiseSuppression:true},video:false});
        if(token !== generation || document.hidden) { captured.getTracks().forEach(track => track.stop()); if(token === generation){phase='idle';render();} return; }
        stream = captured; const mime = recordingMime(); const chunks = [];
        const recording = new MediaRecorder(captured,mime ? {mimeType:mime} : {}); recorder = recording;
        recording.addEventListener('dataavailable',event => { if(event.data.size && token === generation) chunks.push(event.data); });
        recording.addEventListener('stop',() => {
          if(token !== generation) return; clearTimer(); stopTracks(); recorder = null; phase = 'idle';
          setDraft(new Blob(chunks,{type:recording.mimeType || mime || 'audio/webm'}));
        });
        recording.addEventListener('error',() => { if(token === generation) { cancel(); notify('No pude completar la grabación. Puedes adjuntar un audio.'); } });
        recording.start(500); phase = 'recording'; let seconds = 0; notify('Grabando… 0:00'); render();
        timer = window.setInterval(()=>{seconds++;notify(`Grabando… ${Math.floor(seconds/60)}:${String(seconds%60).padStart(2,'0')}`);if(seconds >= MAX_SECONDS)stopRecording();},1000);
      } catch(error) {
        if(token !== generation)return; stopTracks(); phase='idle';
        notify(error.name === 'NotAllowedError' ? 'El micrófono está bloqueado. Permítelo en el navegador o adjunta un audio.' : 'No pude abrir el micrófono. Revisa que esté disponible o adjunta un audio.'); render();
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
      pausePlayback();unlockAudio();
      const audio = draft; phase='sending'; notify('Transcribiendo tu audio…'); render();
      try {
        const response = await request('/api/coach/voice/transcribe',{method:'POST',headers:{'Content-Type':audio.type || 'audio/webm','X-TEC-Account-ID':String(userId)},body:audio});
        const data = await response.json(); if(!data.text?.trim()) throw new Error('No escuché una pregunta clara. Tu audio sigue aquí.');
        notify('Tu pregunta está lista. El Coach está respondiendo…');
        const result = await sendMessage(data.text,{audioUrl:makeUrl(audio)});
        if(result?.ok) { clearDraft(); notify('Respuesta lista.'); }
        else notify('No se completó la respuesta. Conservé tu audio para que puedas reintentar.');
      } catch(error) { notify(error.message || 'No pude enviar el audio. Tu grabación sigue aquí.'); }
      finally {phase='idle';render();}
    }
    const button = (text,handler) => { const node=document.createElement('button');node.type='button';node.textContent=text;node.addEventListener('click',handler);return node; };
    for(const form of forms) {
      const tools=document.createElement('div');tools.className='coach-voice-tools';
      const item={form};
      item.record=button('Grabar audio',()=>phase === 'recording' && active === item ? stopRecording() : start(item));
      item.upload=button('Adjuntar audio',()=>item.file.click());
      item.file=document.createElement('input');item.file.type='file';item.file.accept='audio/*,video/webm';item.file.hidden=true;
      item.file.addEventListener('change',()=>{const file=item.file.files?.[0];item.file.value='';if(file && !chatBusy && phase === 'idle'){active=item;setDraft(file);}});
      item.status=document.createElement('p');item.status.className='coach-voice-status';item.status.setAttribute('role','status');item.status.setAttribute('aria-live','polite');
      item.preview=document.createElement('div');item.preview.className='coach-voice-preview';item.preview.hidden=true;
      item.audio=document.createElement('audio');item.audio.controls=true;item.audio.preload='metadata';item.audio.setAttribute('aria-label','Escuchar mi audio antes de enviarlo');
      item.audio.addEventListener('play',()=>pausePlayback(item.audio));players.add(item.audio);
      item.send=button('Enviar audio',send); item.cancel=button('Descartar audio',cancel);
      item.preview.append(item.audio,item.send);tools.append(item.record,item.upload,item.file,item.cancel,item.preview,item.status);
      const note=document.createElement('small');note.className='coach-voice-note';note.textContent='Texto → texto · Audio → voz de IA. Audios de hasta 90 s.';tools.append(note);form.append(tools);controls.push(item);
      form.closest('dialog')?.addEventListener('close',()=>{pausePlayback();if(phase==='recording')stopRecording();else if(phase==='requesting')cancel();});
    }
    function attachAudio(handles,url,label) {
      const audios=[];
      for(const {card} of handles || []) {
        const player=document.createElement('audio');player.controls=true;player.preload='metadata';player.src=url;player.setAttribute('aria-label',label);player.className='coach-voice-player';
        player.addEventListener('play',()=>pausePlayback(player));players.add(player);card.append(player);audios.push(player);
      }
      return audios;
    }
    async function speak(text,handles) {
      const panels=(handles || []).map(({card})=>{
        const panel=document.createElement('div');panel.className='coach-voice-reply';
        const label=document.createElement('small');label.textContent='Preparando la voz de IA…';
        const retry=button('Reintentar audio',()=>generate());retry.hidden=true;const stop=button('Detener voz',()=>{pausePlayback();label.textContent='Voz generada por IA';});stop.hidden=true;liveStopButtons.add(stop);panel.append(label,stop,retry);card.append(panel);return {panel,label,retry,stop};
      });
      async function generate() {
        unlockAudio();const context=liveContext;let playhead=0, streamed=false;
        panels.forEach(p=>{p.retry.hidden=true;p.label.textContent='Preparando la voz de IA…';});
        try {
          const response=await request('/api/coach/voice/speech',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({accountId:String(userId),text,stream:true})});
          const blob=await readSpeech(response,bytes=>{
            if(document.hidden || !context || context!==liveContext || context.state!=='running' || !panels.some(p=>p.panel.getClientRects().length))return;
            const samples=Math.floor(bytes.length/2);if(!samples)return;
            const buffer=context.createBuffer(1,samples,24000), channel=buffer.getChannelData(0), data=new DataView(bytes.buffer,bytes.byteOffset,bytes.byteLength);
            for(let i=0;i<samples;i++)channel[i]=data.getInt16(i*2,true)/32768;
            const source=context.createBufferSource();source.buffer=buffer;source.connect(context.destination);
            playhead=Math.max(context.currentTime+.06,playhead);source.start(playhead);playhead+=buffer.duration;streamed=true;
            panels.forEach(p=>{p.label.textContent='Voz de IA · reproduciendo…';p.stop.hidden=false;});
          });if(blob.size < 44)throw new Error('No se completó el audio.');
          const url=makeUrl(blob), audios=attachAudio(handles,url,'Escuchar respuesta del Coach');
          panels.forEach(p=>{p.label.textContent=streamed && context===liveContext ? 'Voz de IA · reproduciendo…' : 'Voz generada por IA';p.retry.hidden=true;});
          if(streamed)window.setTimeout(()=>panels.forEach(p=>{p.stop.hidden=true;p.label.textContent='Voz generada por IA';}),Math.max(0,(playhead-(context?.currentTime || 0))*1000));
          const player=document.hidden ? null : audios.find(audio=>audio.getClientRects().length);
          if(player && !streamed)await player.play().catch(()=>panels.forEach(p=>{p.label.textContent='Voz de IA lista. Pulsa reproducir para escuchar.';}));
          return true;
        } catch(error) {
          stopLive();
          panels.forEach(p=>{p.label.textContent=error.message || 'La respuesta escrita está lista. Reintenta el audio.';p.retry.hidden=false;});return false;
        }
      }
      return generate();
    }
    document.addEventListener('visibilitychange',()=>{if(document.hidden){if(phase==='recording')stopRecording();else if(phase==='requesting')cancel();pausePlayback();}});
    window.addEventListener('pagehide',event=>{generation++;clearTimer();stopRecording();pausePlayback();if(!event.persisted)urls.forEach(release);});
    render();
    return { setChatBusy(value){chatBusy=Boolean(value);render();}, pausePlayback, speak,
      attachInput(handles,url){attachAudio(handles,url,'Escuchar mi pregunta de audio');} };
  }
  window.TecCoachVoice=Object.freeze({init,recordingMime,readSpeech});
})();
