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
 pause(){}
 setPointerCapture(id){this.capture=id;}
 releasePointerCapture(id){this.capture=null;this.fire('lostpointercapture',{pointerId:id});}
}
function fixture({deferred=false,fail=false,denied=false,speech=false,replyText='Respuesta del Coach',permissionState,userAgent,audioSession,singleContext=false}={}){
 let now=0,captureResolve,trackStops=0,urlCount=0,micRequests=0;const calls=[],sent=[],intervals=new Map(),contexts=[],sources=[];
 const form=new Element('form'),textarea=new Element('textarea'),submit=new Element('button'),dialog=new Element('dialog');submit.type='submit';form.dialog=dialog;form.append(textarea,submit);
 const document=new Element('document');document.createElement=tag=>new Element(tag);document.hidden=false;
 const window=new Element('window');Object.assign(window,{setInterval(fn){intervals.set(1,fn);return 1;},clearInterval(id){intervals.delete(id);},setTimeout(){return 2;},clearTimeout(){}});
 const track={stop(){trackStops++;}},stream={getTracks:()=>[track]};
 const permission = {state:permissionState};
 const navigator={userAgent,...(audioSession && {audioSession}),permissions:permissionState ? {query:async()=>permission} : undefined,mediaDevices:{getUserMedia(){
   micRequests++;
   if(denied)return Promise.reject(Object.assign(new Error('denied'),{name:'NotAllowedError'}));
   if(deferred)return new Promise(resolve=>{captureResolve=resolve;});
   return Promise.resolve(stream);
 }}};

 class Recorder extends Element {
  static isTypeSupported(){return true;}
  constructor(){super();this.state='inactive';this.mimeType='audio/webm';}
  start(){this.state='recording';}
  stop(){this.state='inactive';queueMicrotask(()=>{this.fire('dataavailable',{data:new Blob(['x'.repeat(200)],{type:this.mimeType})});this.fire('stop');});}
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
   return{ok:true};}});
 const all=node=>[node,...node.children.flatMap(all)],find=name=>all(form).find(n=>n.className===name);
 const record=find('coach-voice-hold');
 return{switchChat(){current=false;window.fire('tec:coach-session-change');},question,transcript,answer,contexts,sources,form,textarea,submit,dialog,document,window,calls,sent,record,find,controller,advance(ms){now+=ms;},get stopped(){return trackStops;},get micRequests(){return micRequests;},permission,resolveCapture(){captureResolve(stream);},recover(){fail=false;},tick(){intervals.get(1)?.();},down(){gestureRunning=true;record.fire('pointerdown',{button:0,isPrimary:true,pointerId:1,clientX:300});gestureRunning=false;},move(x){record.fire('pointermove',{pointerId:1,clientX:x});},up(){gestureRunning=true;record.fire('pointerup',{pointerId:1});gestureRunning=false;}};
}
test('one microphone replaces the empty text send button; typing switches to text send',()=>{
 const f=fixture();assert.equal(f.submit.hidden,true);assert.equal(f.find('coach-voice-tools').hidden,false);assert.equal(f.form.children.filter(n=>n.tagName==='input').length,0);
 f.textarea.value='Pregunta escrita';f.textarea.fire('input');assert.equal(f.submit.hidden,false);assert.equal(f.find('coach-voice-tools').hidden,true);
 f.textarea.value='';f.textarea.fire('input');assert.equal(f.submit.hidden,true);assert.equal(f.find('coach-voice-tools').hidden,false);
});
test('holding records visibly; release sends once with no confirmation and unlocks text',async()=>{
 const f=fixture();f.down();await turn();assert.equal(f.record.attributes['aria-pressed'],'true');assert.equal(f.find('coach-voice-activity').hidden,false);assert.equal(f.textarea.disabled,true);
 f.advance(1500);f.up();await turn();assert.equal(f.calls.length,1);assert.equal(f.sent.length,1);assert.equal(f.calls[0].options.headers['X-TEC-Account-ID'],'alice');assert.equal(f.stopped,1);assert.equal(f.textarea.disabled,false);assert.equal(f.find('coach-voice-preview').hidden,true);
 f.up();assert.equal(f.calls.length,1);
});
test('left drag marks the trash target; releasing deletes without transcription or chat',async()=>{
 const f=fixture();f.down();await turn();f.advance(1500);f.move(220);assert.equal(f.find('coach-voice-slide').textContent,'Suelta para borrar');f.up();await turn();assert.equal(f.calls.length,0);assert.equal(f.sent.length,0);assert.equal(f.stopped,1);assert.equal(f.record.attributes['aria-pressed'],'false');
});
test('dragging back from trash restores send; short taps never send accidental audio',async()=>{
 const f=fixture();f.down();await turn();f.move(220);f.move(285);assert.match(f.find('coach-voice-slide').textContent,/Desliza/);f.advance(1000);f.up();await turn();assert.equal(f.sent.length,1);
 const g=fixture();g.down();await turn();g.advance(100);g.up();await turn();assert.equal(g.calls.length,0);assert.match(g.find('coach-voice-status').textContent,/Mantén/);
});
test('release before permission resolves rejects a late stream and never records or sends',async()=>{
 const f=fixture({deferred:true});f.down();f.up();f.resolveCapture();await turn();assert.equal(f.stopped,1);assert.equal(f.calls.length,0);assert.equal(f.record.attributes['aria-pressed'],'false');assert.equal(f.textarea.disabled,false);
});
test('permission denial resets the gesture and displays an actionable error',async()=>{
 const f=fixture({denied:true});f.down();await turn();assert.equal(f.calls.length,0);assert.match(f.find('coach-voice-status').textContent,/bloqueado/);assert.equal(f.textarea.disabled,false);
});
test('a slow browser prompt is never duplicated and a granted site records after a fresh load',async()=>{
 const first=fixture({deferred:true,permissionState:'prompt',userAgent:'iPhone'});
 await turn();assert.equal(first.micRequests,0);
 first.down();first.window.fire('blur');first.up();await turn();
 assert.match(first.find('coach-voice-status').textContent,/mantener presionado/i);
 assert.doesNotMatch(first.find('coach-voice-status').textContent,/Safari|Configuración|Permitir/);
 first.down();await turn();assert.equal(first.micRequests,1);
 first.permission.state='granted';first.resolveCapture();await turn();assert.equal(first.stopped,1);assert.equal(first.calls.length,0);
 const reopened=fixture({permissionState:'granted',userAgent:'iPhone'});
 await turn();assert.equal(reopened.micRequests,0);
 reopened.down();await turn();reopened.advance(700);reopened.up();await turn();
 assert.equal(reopened.micRequests,1);assert.equal(reopened.sent.length,1);assert.equal(reopened.stopped,1);
});
test('a saved denial shows how to change the site setting and a later grant uses the mic',async()=>{
 const f=fixture({permissionState:'denied',userAgent:'Mozilla/5.0 (iPhone) AppleWebKit/605.1.15 Version/27.0 Mobile Safari/604.1'});
 await turn();f.down();await turn();assert.equal(f.micRequests,0);
 assert.match(f.find('coach-voice-status').textContent,/Safari.*Micrófono.*Permitir/);
 f.permission.state='granted';f.down();await turn();f.advance(700);f.up();await turn();
 assert.equal(f.micRequests,1);assert.equal(f.sent.length,1);assert.equal(f.stopped,1);
});
test('la app solo muestra cómo habilitar su micrófono tras una denegación', async()=>{
 const app=fixture({denied:true,userAgent:'Mozilla/5.0 (iPhone) AppleWebKit/605.1.15 TEC-iOS/0.7.1'});
 assert.equal(app.find('coach-voice-hint').textContent,'Mantén presionado para hablar. Suelta para enviar.');
 assert.equal(app.find('coach-voice-status').textContent,'');
 app.down();await turn();
 const notice=app.find('coach-voice-status').textContent;
 assert.match(notice,/Configuración.*Micrófono.*TEC Coach/);
 assert.doesNotMatch(notice,/Safari|Azure/);
 const chrome=fixture({permissionState:'denied',userAgent:'Mozilla/5.0 (iPhone) CriOS/155.0 Mobile Safari/604.1'});
 await turn();chrome.down();await turn();
 assert.doesNotMatch(chrome.find('coach-voice-status').textContent,/Safari|TEC Coach/);
});
test('pointer cancellation, loss of capture, dialog close, blur and hiding cancel without sending',async()=>{
 for(const cancel of [f=>f.record.fire('pointercancel',{pointerId:1}),f=>f.record.fire('lostpointercapture',{pointerId:1}),f=>f.dialog.fire('close'),f=>f.window.fire('blur'),f=>{f.document.hidden=true;f.document.fire('visibilitychange');}]){
  const f=fixture();f.down();await turn();f.advance(1200);cancel(f);await turn();assert.equal(f.calls.length,0);assert.equal(f.stopped,1);assert.equal(f.record.attributes['aria-pressed'],'false');
 }
});
test('keyboard hold and release sends; Escape discards without an accidental keyup send',async()=>{
 const f=fixture();f.record.fire('keydown',{key:' ',repeat:false});await turn();f.advance(1000);f.record.fire('keyup',{key:' '});await turn();assert.equal(f.sent.length,1);
 const g=fixture();g.record.fire('keydown',{key:'Enter',repeat:false});await turn();g.advance(1000);g.document.fire('keydown',{key:'Escape'});g.record.fire('keyup',{key:'Enter'});await turn();assert.equal(g.calls.length,0);
});
test('90-second cap stops tracks and waits for release; the pending hold can still be discarded',async()=>{
 for(const discard of [false,true]){const f=fixture();f.down();await turn();for(let i=0;i<90;i++){f.advance(1000);f.tick();}await turn();assert.equal(f.stopped,1);assert.equal(f.calls.length,0);assert.equal(f.record.disabled,false);assert.equal(f.find('coach-voice-slide').textContent,'Suelta para enviar');if(discard)f.move(200);f.up();await turn();assert.equal(f.sent.length,discard?0:1);}
});
test('failed transcription retains the recording; only explicit retry resends and success removes recovery buttons',async()=>{
 const f=fixture({fail:true});f.down();await turn();f.advance(1000);f.up();await turn();assert.equal(f.calls.length,1);assert.equal(f.find('coach-voice-preview').hidden,false);assert.equal(f.sent.length,0);
 f.down();f.up();await turn();assert.equal(f.calls.length,1);f.recover();const retry=f.find('coach-voice-preview').children[1];retry.fire('click');await turn();assert.equal(f.calls.length,2);assert.equal(f.sent.length,1);assert.equal(f.find('coach-voice-preview').hidden,true);
});

test('audio chat keeps the transcription visible and starts playback with the microphone gesture',async()=>{
 const f=fixture({speech:true});
 f.down();await turn();f.advance(1250);f.up();await turn();
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
 f.down();await turn();f.advance(1250);f.up();
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
 f.down();await turn();f.advance(1250);f.up();await turn();
 assert.equal(f.contexts.length,1);
 assert.equal(f.contexts[0].state,'running');
 assert.equal(f.sources.length,1,'the first audio must play without a second gesture');
});

test('en iPhone el audio pasa a modo de reproducción al soltar el micrófono y vuelve al automático al terminar',async()=>{
 const session={type:'auto'}, seen=[];
 const audioSession={get type(){return session.type;},set type(value){session.type=value;seen.push(value);}};
 const f=fixture({speech:true,audioSession});
 f.down();await turn();
 assert.equal(session.type,'play-and-record');
 f.advance(1250);f.up();await turn();
 assert.equal(session.type,'playback','the spoken reply must not play in call mode');
 assert.equal(f.sources.length,1);
 f.sources[0].onended();
 assert.equal(session.type,'auto','other recorders on the page get the default mode back');
 assert.deepEqual(seen,['play-and-record','playback','auto']);
 // Cancelar la grabación o negar el micrófono no deja el modo cambiado.
 const cancelled=fixture({audioSession});
 cancelled.down();await turn();cancelled.advance(1200);cancelled.move(200);cancelled.up();await turn();
 assert.equal(session.type,'auto');
 const denied=fixture({denied:true,audioSession});
 denied.down();await turn();
 assert.equal(session.type,'auto');
});
