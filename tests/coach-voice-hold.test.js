import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import vm from 'node:vm';
const source=fs.readFileSync(new URL('../public/coach/coach-voice.js',import.meta.url),'utf8');
const turn=async()=>{for(let i=0;i<50;i++)await Promise.resolve();};
class Element {
 constructor(tag='div'){this.tagName=tag;this.children=[];this.events=new Map();this.attributes={};this.value='';this.hidden=false;this.paused=true;this.textContent='';this.classes=new Set();this.classList={add:name=>this.classes.add(name),toggle:(name,on)=>on?this.classes.add(name):this.classes.delete(name)};}
 append(...nodes){this.children.push(...nodes);}
 addEventListener(type,fn){if(!this.events.has(type))this.events.set(type,[]);this.events.get(type).push(fn);}
 fire(type,event={}){for(const fn of this.events.get(type)||[])fn({preventDefault(){},...event});}
 setAttribute(k,v){this.attributes[k]=v;}
 getAttribute(k){return this.attributes[k]??null;}
 removeAttribute(k){delete this.attributes[k];}
 getClientRects(){return this.hidden?[]:[{}];}
 querySelector(selector){return this.querySelectorAll(selector)[0];}
 querySelectorAll(selector){return this.children.flatMap(node=>[...(selector.includes(node.tagName) && (node.tagName!=='button'||node.type==='submit')?[node]:[]),...node.querySelectorAll(selector)]);}
 closest(){return this.dialog;}
 contains(node){return node===this || this.children.some(child=>child.contains(node));}
 focus(){this.focused=(this.focused||0)+1;}
 pause(){}
}
function fixture({deferred=false,fail=false,denied=false,speech=false,replyText='Respuesta del Coach',permissionState,userAgent,audioSession,singleContext=false,rejectBitrate=false,wakeLock,init={}}={}){
 let now=0,captureResolve,trackStops=0,urlCount=0,micRequests=0;const calls=[],sent=[],intervals=new Map(),contexts=[],sources=[],recorders=[];
 const form=new Element('form'),textarea=new Element('textarea'),submit=new Element('button'),dialog=new Element('dialog');submit.type='submit';form.dialog=dialog;form.append(textarea,submit);
 const document=new Element('document');document.createElement=tag=>new Element(tag);document.hidden=false;
 const window=new Element('window');Object.assign(window,{setInterval(fn){intervals.set(1,fn);return 1;},clearInterval(id){intervals.delete(id);},setTimeout(){return 2;},clearTimeout(){}});
 const track={stop(){trackStops++;}},stream={getTracks:()=>[track]};
 const permission = {state:permissionState};
 const navigator={userAgent,...(audioSession && {audioSession}),...(wakeLock && {wakeLock}),permissions:permissionState ? {query:async()=>permission} : undefined,mediaDevices:{getUserMedia(){
   micRequests++;
   if(denied)return Promise.reject(Object.assign(new Error('denied'),{name:'NotAllowedError'}));
   if(deferred)return new Promise(resolve=>{captureResolve=resolve;});
   return Promise.resolve(stream);
 }}};

 class Recorder extends Element {
  static isTypeSupported(){return true;}
  constructor(_stream,options={}){super();if(rejectBitrate && options.audioBitsPerSecond)throw new Error('bitrate');this.options=options;this.state='inactive';this.mimeType='audio/webm';recorders.push(this);}
  start(){this.state='recording';}
  emit(size){this.fire('dataavailable',{data:new Blob(['x'.repeat(size)],{type:this.mimeType})});}
  stop(){this.state='inactive';queueMicrotask(()=>{this.emit(200);this.fire('stop');});}
 }
 class Clock extends Date {static now(){return now;}}
 window.MediaRecorder=Recorder;
 if(speech)window.AudioContext=class {
  constructor(){if(singleContext && contexts.length)throw new Error('sin otro contexto');this.state='suspended';this.currentTime=0;contexts.push(this);}
  resume(){if(gestureRunning)this.state='running';return Promise.resolve();}
  close(){this.state='closed';return Promise.resolve();}
  createBuffer(channels, samples, rate){return {duration:samples/rate,getChannelData:()=>new Float32Array(samples)};}
  createBufferSource(){return {connect(){},start(){sources.push(this);}};}
 };
 let gestureRunning=false, current=true;
 const fetch=async(url,options)=>{
   calls.push({url,options});
   if(fail)throw new Error('Sin conexión');
   if(url.endsWith('/speech')){
     const data=Buffer.alloc(24000,4).toString('base64');
     return new Response(JSON.stringify({type:'audio',data})+'\n'+JSON.stringify({type:'done'})+'\n',{headers:{'Content-Type':'application/x-ndjson'}});
   }
   return new Response(JSON.stringify({text:'Pregunta grabada'}));
 };
 vm.runInNewContext(source,{window,document,navigator,MediaRecorder:Recorder,Blob,DataView,ArrayBuffer,Uint8Array,TextDecoder,atob,AbortController,Date:Clock,fetch,URL:{createObjectURL:()=>`blob:${++urlCount}`,revokeObjectURL(){}},queueMicrotask});
 const question=new Element('article'), transcript=new Element('p'), answer=new Element('article');question.append(transcript);
 const controller=window.TecCoachVoice.init({userId:'alice',forms:[form],sendMessage:async(text,options)=>{sent.push({text,options});
   if(speech){
     transcript.textContent=text;
     controller.pausePlayback(null,true);
     controller.attachInput([{card:question,paragraph:transcript}],options.audioUrl);
     await controller.speak(replyText,[{card:answer}],undefined,{verbatim:true,isCurrent:()=>current});
   }
   return{ok:true};},...init});
 const all=node=>[node,...node.children.flatMap(all)],find=name=>all(form).find(n=>n.className===name);
 const record=find('coach-voice-hold');
 return{switchChat(){current=false;window.fire('tec:coach-session-change');},question,transcript,answer,contexts,sources,recorders,form,textarea,submit,dialog,document,window,calls,sent,record,find,controller,api:window.TecCoachVoice,
  advance(ms){now+=ms;},get stopped(){return trackStops;},get micRequests(){return micRequests;},permission,resolveCapture(){captureResolve(stream);},recover(){fail=false;},tick(){intervals.get(1)?.();},
  // Un toque completo en el micrófono: el navegador lo entrega como `click`, igual que Espacio, Enter o un lector de pantalla.
  tap(detail=1){gestureRunning=true;record.fire('click',{detail});gestureRunning=false;},
  discard(){find('coach-voice-discard').fire('click');},
  hide(){document.hidden=true;document.fire('visibilitychange');},show(){document.hidden=false;document.fire('visibilitychange');},
  get status(){return find('coach-voice-status').textContent;},get pressed(){return record.attributes['aria-pressed'];}};
}
test('one microphone replaces the empty text send button; typing switches to text send',()=>{
 const f=fixture();assert.equal(f.submit.hidden,true);assert.equal(f.find('coach-voice-tools').hidden,false);assert.equal(f.form.children.filter(n=>n.tagName==='input').length,0);
 f.textarea.value='Pregunta escrita';f.textarea.fire('input');assert.equal(f.submit.hidden,false);assert.equal(f.find('coach-voice-tools').hidden,true);
 f.textarea.value='';f.textarea.fire('input');assert.equal(f.submit.hidden,true);assert.equal(f.find('coach-voice-tools').hidden,false);
});
test('un toque graba con reloj a la vista y sin mantener presionado; el segundo toque envía una sola vez',async()=>{
 const f=fixture();assert.equal(f.pressed,'false');assert.equal(f.record.attributes['aria-label'],'Grabar mensaje de voz');
 assert.equal(f.find('coach-voice-status').attributes['aria-live'],'polite');
 f.tap();await turn();
 assert.equal(f.pressed,'true');assert.equal(f.find('coach-voice-activity').hidden,false);assert.equal(f.form.classes.has('is-voice-recording'),true);
 assert.equal(f.textarea.disabled,true);assert.equal(f.record.disabled,false);
 assert.match(f.status,/Grabando.*otra vez para enviar.*Cancelar.*3 minutos/);
 assert.match(f.find('coach-voice-hint').textContent,/otra vez para enviar/);assert.equal(f.find('coach-voice-hint').hidden,false);
 assert.equal(f.find('coach-voice-limit').textContent,'/ 3:00');
 f.advance(75000);f.tick();assert.equal(f.find('coach-voice-elapsed').textContent,'1:15');assert.equal(f.calls.length,0);
 f.tap();await turn();
 assert.equal(f.calls.length,1);assert.equal(f.sent.length,1);assert.equal(f.sent[0].options.audioDuration,75);
 assert.equal(f.calls[0].url,'/api/coach/voice/transcribe');assert.equal(f.calls[0].options.headers['X-TEC-Account-ID'],'alice');
 assert.equal(f.stopped,1);assert.equal(f.pressed,'false');assert.equal(f.textarea.disabled,false);
 assert.equal(f.find('coach-voice-activity').hidden,true);assert.equal(f.form.classes.has('is-voice-recording'),false);assert.equal(f.find('coach-voice-preview').hidden,true);
});
test('«Cancelar» borra la grabación sin transcribir ni escribir en el chat y devuelve el foco al micrófono',async()=>{
 const f=fixture();f.tap();await turn();f.advance(40000);
 assert.equal(f.find('coach-voice-discard').textContent,'Cancelar');
 f.discard();await turn();
 assert.equal(f.calls.length,0);assert.equal(f.sent.length,0);assert.equal(f.stopped,1);assert.equal(f.pressed,'false');
 assert.equal(f.status,'Audio borrado.');assert.equal(f.find('coach-voice-preview').hidden,true);assert.equal(f.record.focused,1);
 f.tap();await turn();f.advance(900);f.tap();await turn();assert.equal(f.sent.length,1,'the next recording works');
});
test('un doble toque accidental no corta la grabación recién iniciada',async()=>{
 const f=fixture();f.tap();await turn();f.advance(120);f.tap();await turn();
 assert.equal(f.pressed,'true');assert.equal(f.calls.length,0);assert.equal(f.stopped,0);
 f.advance(1000);f.tap();await turn();assert.equal(f.sent.length,1);
});
test('cancelar antes de que el navegador entregue el micrófono descarta la captura tardía',async()=>{
 const f=fixture({deferred:true});f.tap();f.tap();assert.equal(f.micRequests,1);assert.equal(f.pressed,'true');
 f.discard();assert.equal(f.status,'Grabación cancelada.');
 f.resolveCapture();await turn();assert.equal(f.stopped,1);assert.equal(f.calls.length,0);assert.equal(f.pressed,'false');assert.equal(f.textarea.disabled,false);
 const hidden=fixture({deferred:true});hidden.tap();hidden.hide();hidden.resolveCapture();await turn();
 assert.equal(hidden.stopped,1);assert.equal(hidden.recorders.length,0);assert.equal(hidden.pressed,'false');
});
test('permission denial displays an actionable error and frees the composer',async()=>{
 const f=fixture({denied:true});f.tap();await turn();assert.equal(f.calls.length,0);assert.match(f.status,/bloqueado/);assert.equal(f.textarea.disabled,false);assert.equal(f.pressed,'false');
});
test('a slow browser prompt is never duplicated and a granted site records after a fresh load',async()=>{
 const first=fixture({deferred:true,permissionState:'prompt',userAgent:'iPhone'});
 await turn();assert.equal(first.micRequests,0);
 first.tap();first.window.fire('blur');assert.equal(first.pressed,'true','losing focus to the permission prompt changes nothing');
 first.discard();await turn();
 assert.doesNotMatch(first.status,/Safari|Configuración|Permitir/);
 first.tap();await turn();assert.equal(first.micRequests,1);
 first.permission.state='granted';first.resolveCapture();await turn();assert.equal(first.stopped,1);assert.equal(first.calls.length,0);
 const reopened=fixture({permissionState:'granted',userAgent:'iPhone'});
 await turn();assert.equal(reopened.micRequests,0);
 reopened.tap();await turn();reopened.advance(700);reopened.tap();await turn();
 assert.equal(reopened.micRequests,1);assert.equal(reopened.sent.length,1);assert.equal(reopened.stopped,1);
});
test('a saved denial shows how to change the site setting and a later grant uses the mic',async()=>{
 const f=fixture({permissionState:'denied',userAgent:'Mozilla/5.0 (iPhone) AppleWebKit/605.1.15 Version/27.0 Mobile Safari/604.1'});
 await turn();f.tap();await turn();assert.equal(f.micRequests,0);
 assert.match(f.status,/Safari.*Micrófono.*Permitir/);
 f.permission.state='granted';f.tap();await turn();f.advance(700);f.tap();await turn();
 assert.equal(f.micRequests,1);assert.equal(f.sent.length,1);assert.equal(f.stopped,1);
});
test('la app solo muestra cómo habilitar su micrófono tras una denegación', async()=>{
 const app=fixture({denied:true,userAgent:'Mozilla/5.0 (iPhone) AppleWebKit/605.1.15 TEC-iOS/0.7.1'});
 assert.equal(app.find('coach-voice-hint').textContent,'Toca el micrófono y habla hasta 3 minutos.');
 assert.equal(app.status,'');
 app.tap();await turn();
 const notice=app.status;
 assert.match(notice,/Configuración.*Micrófono.*TEC Coach/);
 assert.doesNotMatch(notice,/Safari|Azure/);
 const chrome=fixture({permissionState:'denied',userAgent:'Mozilla/5.0 (iPhone) CriOS/155.0 Mobile Safari/604.1'});
 await turn();chrome.tap();await turn();
 assert.doesNotMatch(chrome.status,/Safari|TEC Coach/);
});
test('perder el foco, un toque interrumpido o soltar el puntero no detienen ni borran la grabación',async()=>{
 const f=fixture();f.tap();await turn();f.advance(30000);
 f.window.fire('blur');f.record.fire('pointercancel',{pointerId:1});f.record.fire('lostpointercapture',{pointerId:1});f.record.fire('pointerup',{pointerId:1});await turn();
 assert.equal(f.pressed,'true');assert.equal(f.stopped,0);assert.equal(f.calls.length,0);
 f.advance(30000);f.tap();await turn();
 assert.equal(f.sent.length,1);assert.equal(f.sent[0].options.audioDuration,60);
});
test('si la página se oculta se cierra el micrófono, el audio queda guardado y se envía al volver',async()=>{
 const f=fixture();f.tap();await turn();f.advance(20000);f.hide();await turn();
 assert.equal(f.stopped,1,'the microphone is released as soon as the page is hidden');
 assert.equal(f.calls.length,0);assert.equal(f.sent.length,0);assert.match(f.status,/Guardé tu audio.*vuelvas/);
 assert.equal(f.find('coach-voice-preview').hidden,false,'the kept audio can also be sent or deleted by hand');
 f.advance(600000);f.window.fire('blur');f.window.fire('pagehide',{persisted:true});await turn();assert.equal(f.calls.length,0);
 f.show();await turn();
 assert.equal(f.calls.length,1);assert.equal(f.sent.length,1);
 assert.equal(f.sent[0].options.audioDuration,20,'time spent away is not counted as recording');
 assert.equal(f.find('coach-voice-preview').hidden,true);
 f.hide();f.show();await turn();assert.equal(f.calls.length,1,'it is sent only once');
});
test('cerrar el chat a media grabación la deja como audio pendiente para enviarla o borrarla',async()=>{
 for(const erase of [false,true]){
  const f=fixture();f.tap();await turn();f.advance(15000);f.dialog.fire('close');await turn();
  assert.equal(f.stopped,1);assert.equal(f.calls.length,0);assert.equal(f.pressed,'false');
  assert.match(f.status,/Guardé tu audio/);assert.equal(f.find('coach-voice-preview').hidden,false);
  const [,send,bin]=f.find('coach-voice-preview').children;assert.equal(send.textContent,'Enviar audio');
  (erase?bin:send).fire('click');await turn();
  assert.equal(f.sent.length,erase?0:1);assert.equal(f.find('coach-voice-preview').hidden,true);
 }
});
test('con teclado o lector de pantalla el mismo botón inicia y envía; Escape solo borra dentro del compositor',async()=>{
 const f=fixture();f.tap(0);await turn();assert.equal(f.pressed,'true');f.advance(1000);f.tap(0);await turn();assert.equal(f.sent.length,1);
 const g=fixture();g.tap(0);await turn();g.advance(1000);
 g.document.fire('keydown',{key:'Escape',target:g.document});await turn();assert.equal(g.pressed,'true','Escape elsewhere on the page keeps the recording');
 g.document.fire('keydown',{key:'Escape',target:g.record});await turn();
 assert.equal(g.pressed,'false');assert.equal(g.calls.length,0);assert.equal(g.status,'Audio borrado.');
});
test('a los 180 segundos se detiene y se envía solo; nunca se descarta',async()=>{
 const f=fixture();f.tap();await turn();
 for(let i=1;i<=180;i++){
  f.advance(1000);f.tick();
  if(i===89)assert.equal(f.pressed,'true','the old 90-second cap is gone');
  if(i===150){assert.equal(f.status,'Quedan 30 segundos.');assert.equal(f.find('coach-voice-note').textContent,'Quedan 30 s');assert.equal(f.find('coach-voice-elapsed').textContent,'2:30');}
  if(i===170)assert.equal(f.status,'Quedan 10 segundos.');
  if(i<180)assert.equal(f.calls.length,0);
 }
 await turn();
 assert.equal(f.stopped,1);assert.equal(f.calls.length,1);assert.equal(f.sent.length,1);assert.equal(f.sent[0].options.audioDuration,180);assert.equal(f.pressed,'false');
});
test('un servicio de voz propio conserva 90 segundos salvo que pida otro máximo',async()=>{
 const voiceApi={transcription:'/api/chef/public/voice/transcribe',speech:'/api/chef/public/voice/speech'};
 const chef=fixture({init:{userId:undefined,voiceApi}});
 assert.match(chef.find('coach-voice-hint').textContent,/hasta 90 segundos/);
 chef.tap();await turn();for(let i=0;i<90;i++){chef.advance(1000);chef.tick();}await turn();
 assert.equal(chef.sent.length,1);assert.equal(chef.sent[0].options.audioDuration,90);assert.equal(chef.calls[0].url,voiceApi.transcription);
 const short=fixture({init:{maxSeconds:60}});
 assert.match(short.find('coach-voice-hint').textContent,/hasta 1 minuto/);
 short.tap();await turn();for(let i=0;i<60;i++){short.advance(1000);short.tick();}await turn();assert.equal(short.sent.length,1);
});
test('tres minutos caben en la subida: la grabación pide una tasa de voz y se corta antes de rebasar el tope',async()=>{
 const f=fixture();f.tap();await turn();
 assert.deepEqual({...f.recorders[0].options},{audioBitsPerSecond:32000,mimeType:'audio/webm;codecs=opus'});
 for(const mime of ['audio/webm;codecs=opus','audio/webm','audio/mp4','']){
  const options=f.api.recordingOptions(mime);
  assert.ok(options.audioBitsPerSecond*180/8<2*1024*1024,mime+' stays far below 8 MB and the 5 MB of the Chef');
  assert.equal(options.mimeType,mime||undefined);
 }
 assert.equal(f.api.recordingOptions('audio/mp4').audioBitsPerSecond,48000);
 assert.equal(f.api.formatClock(0),'0:00');assert.equal(f.api.formatClock(180),'3:00');assert.equal(f.api.formatClock(65.9),'1:05');
 const strict=fixture({rejectBitrate:true});strict.tap();await turn();
 assert.equal(strict.pressed,'true','a browser that rejects the bitrate still records');assert.equal(strict.recorders[0].options.audioBitsPerSecond,undefined);
 const small=fixture({init:{maxBytes:10000}});small.tap();await turn();small.advance(5000);small.recorders[0].emit(8000);await turn();
 assert.equal(small.pressed,'true');small.recorders[0].emit(1100);await turn();
 assert.equal(small.sent.length,1,'reaching the size limit sends what was recorded');assert.equal(small.stopped,1);
});
test('si el chat se ocupa mientras se graba, el audio se puede detener y sale cuando el Coach termina',async()=>{
 const f=fixture();f.tap();await turn();f.advance(8000);f.controller.setChatBusy(true);
 assert.equal(f.record.disabled,false,'the stop button stays usable');
 f.tap();await turn();assert.equal(f.calls.length,0);assert.match(f.status,/Guardé tu audio.*Coach termine/);
 f.controller.setChatBusy(false);await turn();assert.equal(f.calls.length,1);assert.equal(f.sent.length,1);
});
test('si el sistema corta el micrófono o el grabador falla, lo grabado se conserva',async()=>{
 const cut=fixture();cut.tap();await turn();cut.advance(12000);cut.recorders[0].stop();await turn();
 assert.equal(cut.sent.length,1);assert.equal(cut.stopped,1);
 const broken=fixture();broken.tap();await turn();broken.advance(12000);broken.recorders[0].fire('error');await turn();
 assert.equal(broken.calls.length,0);assert.equal(broken.find('coach-voice-preview').hidden,false);assert.match(broken.status,/Guardé tu audio/);
});
test('la pantalla se mantiene encendida solo mientras se graba',async()=>{
 const seen=[],wakeLock={request:async type=>{seen.push(type);return{release:async()=>{seen.push('release');}};}};
 const f=fixture({wakeLock});await turn();assert.deepEqual(seen,[]);
 f.tap();await turn();assert.deepEqual(seen,['screen']);
 f.advance(60000);f.tap();await turn();assert.deepEqual(seen,['screen','release']);assert.equal(f.sent.length,1);
 const late={request:()=>new Promise(resolve=>{late.grant=()=>resolve({release:async()=>{late.released=true;}});})};
 const g=fixture({wakeLock:late});g.tap();await turn();g.advance(2000);g.discard();late.grant();await turn();
 assert.equal(late.released,true,'a lock granted after the recording ended is returned at once');
});
test('failed transcription retains the recording; only explicit retry resends and success removes recovery buttons',async()=>{
 const f=fixture({fail:true});f.tap();await turn();f.advance(1000);f.tap();await turn();assert.equal(f.calls.length,1);assert.equal(f.find('coach-voice-preview').hidden,false);assert.equal(f.sent.length,0);
 f.tap();await turn();f.advance(1000);f.tap();await turn();assert.equal(f.calls.length,1);assert.equal(f.micRequests,1);
 f.recover();const retry=f.find('coach-voice-preview').children[1];retry.fire('click');await turn();assert.equal(f.calls.length,2);assert.equal(f.sent.length,1);assert.equal(f.find('coach-voice-preview').hidden,true);
});

test('audio chat keeps the transcription visible and starts playback with the microphone gesture',async()=>{
 const f=fixture({speech:true});
 f.tap();await turn();f.advance(1250);f.tap();await turn();
 // La respuesta suena en un contexto creado al soltar, ya con el micrófono cerrado; el de la grabación se cierra.
 assert.equal(f.contexts.length,2,'the reply plays in a context created after the microphone closed');
 assert.equal(f.contexts[0].state,'closed');
 assert.equal(f.contexts[1].state,'running','the release gesture unlocks the new context');
 assert.equal(f.question.children[0],f.transcript);
 assert.equal(f.transcript.textContent,'Pregunta grabada');
 assert.equal(f.question.children[1].className,'coach-voice-card');
 assert.equal(f.answer.children[0].children[1].className,'coach-voice-card');
 assert.equal(f.answer.children[0].children[1].hidden,false);
 assert.equal(f.sources.length,1,'the first audio must play without a second gesture');
 f.sources[0].onended();
 assert.equal(f.answer.children[0].children[1].classes.has('is-playing'),false);
 assert.equal(f.answer.children[0].children[1].children[0].disabled,false,'the complete answer is ready to replay');
});

test('la nota larga se reproduce completa por el servicio del Coach usando la cuenta activa y sin otro toque', async()=>{
 const replyText=('Para: ensayo_tec@example.invalid. Asunto: Prueba #12. '.repeat(250))+'Los horarios son las 12 y las 13.';
 const f=fixture({speech:true,replyText});
 f.tap();await turn();f.advance(1250);f.tap();
 for(let i=0;i<12;i++)await turn();
 const requests=f.calls.filter(call=>call.url.endsWith('/speech')).map(call=>JSON.parse(call.options.body));
 assert.ok(requests.length>1);
 assert.ok(requests.every(body=>body.verbatim && body.stream && body.accountId==='alice' && body.text.length<=12000));
 assert.equal(requests.map(body=>body.text).join(''),replyText);
 assert.equal(f.sources.length,requests.length);
 const player=f.answer.children[0].children[1];
 assert.equal(player.classes.has('is-playing'),true);
 f.switchChat();
 assert.deepEqual(f.contexts.map(context=>context.state),['closed','closed']);
 assert.equal(player.classes.has('is-playing'),false);
 player.children[0].fire('click');
 assert.equal(f.sources.length,requests.length);
 assert.equal(await f.controller.speak('Respuesta antigua',[{card:f.answer}],undefined,{isCurrent:()=>false}),false);
 assert.equal(f.calls.length,requests.length+1);
});

test('si el contexto nuevo no puede crearse, la respuesta suena en el que desbloqueó el micrófono',async()=>{
 const f=fixture({speech:true,singleContext:true});
 f.tap();await turn();f.advance(1250);f.tap();await turn();
 assert.equal(f.contexts.length,1);
 assert.equal(f.contexts[0].state,'running');
 assert.equal(f.sources.length,1,'the first audio must play without a second gesture');
});

test('en iPhone el audio pasa a modo de reproducción al cerrar el micrófono y vuelve al automático al terminar',async()=>{
 const session={type:'auto'}, seen=[];
 const audioSession={get type(){return session.type;},set type(value){session.type=value;seen.push(value);}};
 const f=fixture({speech:true,audioSession});
 f.tap();await turn();
 assert.equal(session.type,'play-and-record');
 f.advance(1250);f.tap();await turn();
 assert.equal(session.type,'playback','the spoken reply must not play in call mode');
 assert.equal(f.sources.length,1);
 f.sources[0].onended();
 assert.equal(session.type,'auto','other recorders on the page get the default mode back');
 assert.deepEqual(seen,['play-and-record','playback','auto']);
 // Cancelar la grabación o negar el micrófono no deja el modo cambiado.
 const cancelled=fixture({audioSession});
 cancelled.tap();await turn();cancelled.advance(1200);cancelled.discard();await turn();
 assert.equal(session.type,'auto');
 const denied=fixture({denied:true,audioSession});
 denied.tap();await turn();
 assert.equal(session.type,'auto');
});
