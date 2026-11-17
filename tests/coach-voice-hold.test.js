import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import vm from 'node:vm';
const source=fs.readFileSync(new URL('../public/coach/coach-voice.js',import.meta.url),'utf8');
const turn=async()=>{for(let i=0;i<50;i++)await Promise.resolve();};
class Element {
 constructor(tag='div'){this.tagName=tag;this.children=[];this.events=new Map();this.attributes={};this.value='';this.hidden=false;this.textContent='';this.classes=new Set();this.classList={add:name=>this.classes.add(name),toggle:(name,on)=>on?this.classes.add(name):this.classes.delete(name)};}
 append(...nodes){this.children.push(...nodes);}
 addEventListener(type,fn){if(!this.events.has(type))this.events.set(type,[]);this.events.get(type).push(fn);}
 fire(type,event={}){for(const fn of this.events.get(type)||[])fn({preventDefault(){},...event});}
 setAttribute(k,v){this.attributes[k]=v;}
 removeAttribute(k){delete this.attributes[k];}
 querySelector(selector){return this.querySelectorAll(selector)[0];}
 querySelectorAll(selector){return this.children.flatMap(node=>[...(selector.includes(node.tagName) && (node.tagName!=='button'||node.type==='submit')?[node]:[]),...node.querySelectorAll(selector)]);}
 closest(){return this.dialog;}
 pause(){}
 setPointerCapture(id){this.capture=id;}
 releasePointerCapture(id){this.capture=null;this.fire('lostpointercapture',{pointerId:id});}
}
function fixture({deferred=false,fail=false,denied=false}={}){
 let now=0,captureResolve,trackStops=0,urlCount=0;const calls=[],sent=[],intervals=new Map();
 const form=new Element('form'),textarea=new Element('textarea'),submit=new Element('button'),dialog=new Element('dialog');submit.type='submit';form.dialog=dialog;form.append(textarea,submit);
 const document=new Element('document');document.createElement=tag=>new Element(tag);document.hidden=false;
 const window=new Element('window');Object.assign(window,{setInterval(fn){intervals.set(1,fn);return 1;},clearInterval(id){intervals.delete(id);},setTimeout(){return 2;},clearTimeout(){}});
 const track={stop(){trackStops++;}},stream={getTracks:()=>[track]};
 const navigator={mediaDevices:{getUserMedia:()=>denied?Promise.reject(Object.assign(new Error('denied'),{name:'NotAllowedError'})):deferred?new Promise(resolve=>{captureResolve=resolve;}):Promise.resolve(stream)}};
 class Recorder extends Element {
  static isTypeSupported(){return true;}
  constructor(){super();this.state='inactive';this.mimeType='audio/webm';}
  start(){this.state='recording';}
  stop(){this.state='inactive';queueMicrotask(()=>{this.fire('dataavailable',{data:new Blob(['x'.repeat(200)],{type:this.mimeType})});this.fire('stop');});}
 }
 class Clock extends Date {static now(){return now;}}
 window.MediaRecorder=Recorder;
 const fetch=async(url,options)=>{calls.push({url,options});if(fail)throw new Error('Sin conexión');return new Response(JSON.stringify({text:'Pregunta grabada'}));};
 vm.runInNewContext(source,{window,document,navigator,MediaRecorder:Recorder,Blob,DataView,ArrayBuffer,Uint8Array,TextDecoder,atob,AbortController,Date:Clock,fetch,URL:{createObjectURL:()=>`blob:${++urlCount}`,revokeObjectURL(){}},queueMicrotask});
 const controller=window.TecCoachVoice.init({userId:'alice',forms:[form],sendMessage:async(text,options)=>{sent.push({text,options});return{ok:true};}});
 const all=node=>[node,...node.children.flatMap(all)],find=name=>all(form).find(n=>n.className===name);
 const record=find('coach-voice-hold');
 return{form,textarea,submit,dialog,document,window,calls,sent,record,find,controller,advance(ms){now+=ms;},get stopped(){return trackStops;},resolveCapture(){captureResolve(stream);},recover(){fail=false;},tick(){intervals.get(1)?.();},down(){record.fire('pointerdown',{button:0,isPrimary:true,pointerId:1,clientX:300});},move(x){record.fire('pointermove',{pointerId:1,clientX:x});},up(){record.fire('pointerup',{pointerId:1});}};
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
