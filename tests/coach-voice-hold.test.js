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
function fixture({deferred=false,fail=false,denied=false,speech=false,permissionState,userAgent}={}){
 let now=0,captureResolve,trackStops=0,urlCount=0,micRequests=0;const calls=[],sent=[],intervals=new Map(),contexts=[],sources=[];
 const form=new Element('form'),textarea=new Element('textarea'),submit=new Element('button'),dialog=new Element('dialog');submit.type='submit';form.dialog=dialog;form.append(textarea,submit);
 const document=new Element('document');document.createElement=tag=>new Element(tag);document.hidden=false;
 const window=new Element('window');Object.assign(window,{setInterval(fn){intervals.set(1,fn);return 1;},clearInterval(id){intervals.delete(id);},setTimeout(){return 2;},clearTimeout(){}});
 const track={stop(){trackStops++;}},stream={getTracks:()=>[track]};
 const permission = {state:permissionState};
 const navigator={userAgent,permissions:permissionState ? {query:async()=>permission} : undefined,mediaDevices:{getUserMedia(){
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
  constructor(){this.state='suspended';this.currentTime=0;contexts.push(this);}
  resume(){if(gestureRunning)this.state='running';return Promise.resolve();}
  close(){this.state='closed';return Promise.resolve();}
  createBuffer(channels, samples, rate){return {duration:samples/rate,getChannelData:()=>new Float32Array(samples)};}
  createBufferSource(){return {connect(){},start(){sources.push(this);}};}
 };
 let gestureRunning=false;
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
     await controller.speak('Respuesta del Coach',[{card:answer}]);
   }
   return{ok:true};}});
 const all=node=>[node,...node.children.flatMap(all)],find=name=>all(form).find(n=>n.className===name);
 const record=find('coach-voice-hold');
 return{question,transcript,answer,contexts,sources,form,textarea,submit,dialog,document,window,calls,sent,record,find,controller,advance(ms){now+=ms;},get stopped(){return trackStops;},get micRequests(){return micRequests;},permission,resolveCapture(){captureResolve(stream);},recover(){fail=false;},tick(){intervals.get(1)?.();},down(){gestureRunning=true;record.fire('pointerdown',{button:0,isPrimary:true,pointerId:1,clientX:300});gestureRunning=false;},move(x){record.fire('pointermove',{pointerId:1,clientX:x});},up(){gestureRunning=true;record.fire('pointerup',{pointerId:1});gestureRunning=false;}};
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
 assert.match(first.find('coach-voice-status').textContent,/Ajustes del sitio web.*Permitir/);
 first.down();await turn();assert.equal(first.micRequests,1);
 first.permission.state='granted';first.resolveCapture();await turn();assert.equal(first.stopped,1);assert.equal(first.calls.length,0);
 const reopened=fixture({permissionState:'granted',userAgent:'iPhone'});
 await turn();assert.equal(reopened.micRequests,0);
 reopened.down();await turn();reopened.advance(700);reopened.up();await turn();
 assert.equal(reopened.micRequests,1);assert.equal(reopened.sent.length,1);assert.equal(reopened.stopped,1);
});
test('a saved denial shows how to change the site setting and a later grant uses the mic',async()=>{
 const f=fixture({permissionState:'denied',userAgent:'iPhone'});
 await turn();f.down();await turn();assert.equal(f.micRequests,0);
 assert.match(f.find('coach-voice-status').textContent,/Safari.*Micrófono.*Permitir/);
 f.permission.state='granted';f.down();await turn();f.advance(700);f.up();await turn();
 assert.equal(f.micRequests,1);assert.equal(f.sent.length,1);assert.equal(f.stopped,1);
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
 assert.equal(f.contexts.length,1,'sending must retain the context unlocked by the microphone');
 assert.equal(f.contexts[0].state,'running');
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

function privateSpeechFixture(voices) {
  const f=fixture(), utterances=[];
  const synth=new Element('speech');
  synth.getVoices=()=>voices;
  synth.speak=u=>{utterances.push(u);u.onstart?.();};
  synth.cancel=()=>{synth.cancels=(synth.cancels || 0)+1;};
  synth.removeEventListener=(type,fn)=>synth.events.set(type,(synth.events.get(type)||[]).filter(x=>x!==fn));
  f.window.speechSynthesis=synth;
  f.window.SpeechSynthesisUtterance=class {constructor(text){this.text=text;}};
  let current=true;
  const read=(text,autoplay=true)=>f.controller.readPrivateReply(text,[{card:f.answer}],{autoplay,isCurrent:()=>current});
  const toggle=()=>f.answer.children.at(-1).children[0];
  const status=()=>f.answer.children.at(-1).children[1].textContent;
  return {...f,read,toggle,status,utterances,synth,switchChat(){current=false;f.window.fire('tec:coach-session-change');}};
}

test('lee todo el correo y ambos horarios con la voz española del dispositivo sin red',()=>{
  const local={lang:'es-MX',localService:true}, remote={lang:'es-US',default:true,localService:false};
  const f=privateSpeechFixture([remote,local]);
  const text='Para: ensayo@example.invalid. Asunto: Seguimiento. '+('Este es el siguiente párrafo del correo completo. '.repeat(18))+'12:00 o 12:45 p. m.';
  f.read(text);
  assert.equal(f.toggle().textContent,'Detener lectura');
  for(let i=0;i<f.utterances.length;i++) f.utterances[i].onend();
  assert.ok(f.utterances.length>3);
  assert.ok(f.utterances.every(u=>u.voice===local));
  assert.equal(f.utterances.map(u=>u.text).join(' '),text.replace(/\s+/g,' ').trim());
  assert.equal(f.toggle().textContent,'Escuchar respuesta completa');assert.equal(f.calls.length,0);
});

test('un mensaje escrito espera un toque, cancelar o cambiar de sesión impide continuar o reproducir detalles anteriores',()=>{
  const f=privateSpeechFixture([{lang:'es-ES',localService:true}]);
  f.read('Una conversación privada que se puede detener. '.repeat(20),false);
  assert.equal(f.utterances.length,0);f.toggle().fire('click');assert.equal(f.utterances.length,1);
  const previous=f.utterances[0];f.toggle().fire('click');previous.onend();assert.equal(f.utterances.length,1);
  f.toggle().fire('click');assert.equal(f.utterances.length,2);
  f.switchChat();f.utterances[1].onend();f.toggle().fire('click');assert.equal(f.utterances.length,2);
  assert.equal(f.synth.cancels,2);assert.equal(f.calls.length,0);
});

test('espera las voces locales y permite reintentar si el navegador bloquea el audio',()=>{
  const voices=[{lang:'es-US',localService:false}],f=privateSpeechFixture(voices);
  f.read('Este texto solo se escucha en el dispositivo.');
  assert.equal(f.utterances.length,0);
  f.synth.fire('voiceschanged');
  assert.match(f.status(),/activa una voz en español/);assert.equal(f.calls.length,0);
  f.toggle().fire('click');
  voices.push({lang:'es-US',localService:true});f.synth.fire('voiceschanged');
  assert.equal(f.utterances.length,1);
  f.utterances[0].onerror();assert.match(f.status(),/Toca Escuchar/);
  f.toggle().fire('click');assert.equal(f.utterances.length,2);
  f.document.hidden=true;f.document.fire('visibilitychange');f.utterances[1].onend();
  assert.equal(f.utterances.length,2);assert.equal(f.calls.length,0);
});
