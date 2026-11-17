import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import vm from 'node:vm';
const source=fs.readFileSync(new URL('../public/coach/team-chat-media.js',import.meta.url),'utf8');
const tick=async()=>{for(let n=0;n<50;n++)await Promise.resolve();};
class Element {
  constructor(tag='div'){this.tagName=tag.toUpperCase();this.children=[];this.events=new Map();this.value='';this.hidden=false;this.className='';this.attributes={};this.classList={toggle(){}};}
  append(...nodes){nodes.forEach(n=>{n.parentElement=this;this.children.push(n);});} replaceChildren(...nodes){this.children=[];this.append(...nodes);}
  setAttribute(k,v){this.attributes[k]=v;} removeAttribute(k){delete this.attributes[k];} setPointerCapture(){} pause(){this.paused=true;} load(){}
  addEventListener(k,fn){if(!this.events.has(k))this.events.set(k,new Set());this.events.get(k).add(fn);} removeEventListener(k,fn){this.events.get(k)?.delete(fn);}
  fire(k,e={}){for(const fn of this.events.get(k)||[])fn({preventDefault(){},pointerId:1,clientX:200,button:0,...e});}
}
function fixture({permission}={}){
  let at=1000, stopped=0, submitted=0,media,requested=0; const notices=[],timers=new Map();
  const document=new Element();document.createElement=t=>new Element(t);document.hidden=false;
  const window=new Element();
  class Recorder{static isTypeSupported(type){return type.startsWith('audio/webm');}constructor(stream,opts){this.mimeType=opts?.mimeType;this.state='inactive';}start(){this.state='recording';}stop(){if(this.state!=='recording')return;this.state='inactive';queueMicrotask(()=>{this.ondataavailable({data:new Blob([new Uint8Array(100)],{type:this.mimeType})});this.onstop();});}}
  class Reader{readAsDataURL(blob){blob.arrayBuffer().then(data=>{this.result='data:'+blob.type+';base64,'+Buffer.from(data).toString('base64');this.onload();});}}
  class Clock extends Date{static now(){return at;}}
  class Url extends URL{static createObjectURL(){return 'blob:fixture';}static revokeObjectURL(){}}
  window.MediaRecorder=Recorder;
  const stream={getTracks:()=>[{stop(){stopped++;}}]};
  const navigator={mediaDevices:{async getUserMedia(){requested++;return permission?await permission:stream;}}};
  vm.runInNewContext(source,{window,document,navigator,MediaRecorder:Recorder,Blob,FileReader:Reader,URL:Url,Date:Clock,location:{origin:'https://totalelitecoaching.com'},setInterval(fn){const key=timers.size+1;timers.set(key,fn);return key;},clearInterval(key){timers.delete(key);},console});
  const form=new Element('form'),actions=new Element(),send=new Element('button'),input=new Element('textarea');
  media=window.TecTeamChatMedia.init({form,actions,send,input,notify:(...args)=>notices.push(args),changed(){input.disabled=false;media?.sync(false);},submit:async()=>{submitted++;}});media.sync(false);
  const mic=actions.children.find(n=>n.className==='tec-team-chat__mic');
  return {media,mic,document,window,form,actions,input,send,notices,stream,timers,advance(ms){at+=ms;},get stopped(){return stopped;},get submitted(){return submitted;},get requested(){return requested;}};
}

test('hold records, release sends once, and normal text keeps a visible Send button',async()=>{
  const f=fixture();assert.equal(f.send.hidden,true);f.input.value='Hola';f.media.sync(false);assert.equal(f.send.hidden,false);assert.equal(f.mic.hidden,true);f.input.value='';f.media.sync(false);
  f.mic.fire('pointerdown');await tick();assert.equal(f.requested,1);assert.equal(f.mic.attributes['aria-pressed'],'true');f.advance(1200);f.mic.fire('pointerup');await tick();
  assert.equal(f.submitted,1);assert.equal(f.media.selected().mime,'audio/webm;codecs=opus');assert.ok(f.stopped>0);assert.equal(f.mic.attributes['aria-pressed'],'false');f.media.destroy();
});
test('dragging left discards audio without sending it',async()=>{
  const f=fixture();f.mic.fire('pointerdown');await tick();f.advance(1000);f.mic.fire('pointermove',{clientX:100});f.mic.fire('pointerup');await tick();assert.equal(f.submitted,0);assert.equal(f.media.selected(),null);assert.ok(f.stopped>0);f.media.destroy();
});
test('release while permission is pending never starts a hidden recording',async()=>{
  let resolve;const permission=new Promise(r=>{resolve=r;});const f=fixture({permission});f.mic.fire('pointerdown');f.mic.fire('pointerup');resolve(f.stream);await tick();assert.equal(f.submitted,0);assert.ok(f.stopped>0);assert.equal(f.media.busy(),false);f.media.destroy();
});
test('backgrounding or destroying cancels capture and releases the microphone',async()=>{
  const f=fixture();f.mic.fire('pointerdown');await tick();f.advance(1000);f.document.hidden=true;f.document.fire('visibilitychange');await tick();assert.equal(f.submitted,0);assert.ok(f.stopped>0);assert.equal(f.media.busy(),false);f.media.destroy();
});
test('duration cap waits for release rather than unexpectedly sending, and keyboard hold works',async()=>{
  const f=fixture();f.mic.fire('keydown',{key:' ',repeat:false});await tick();f.advance(89000);for(const fn of [...f.timers.values()])fn();await tick();assert.equal(f.submitted,0);assert.equal(f.media.busy(),true);f.mic.fire('keyup',{key:' '});await tick();assert.equal(f.submitted,1);assert.equal(f.media.busy(),false);f.media.destroy();
});
test('very short taps cannot send blank voice notes',async()=>{
  const f=fixture();f.mic.fire('pointerdown');await tick();f.advance(100);f.mic.fire('pointerup');await tick();assert.equal(f.submitted,0);assert.equal(f.media.selected(),null);f.media.destroy();
});
test('received audio never autoplays and updating a transcript retains the same player',()=>{
  const f=fixture(),row=new Element(),message={id:'000000000000000000000001',senderName:'Eric',attachment:{kind:'audio',url:'/api/coach/team-chat/media/000000000000000000000001?roomId=test',transcript:''}};
  f.media.render(row,message);const wrapper=row.children[0],player=wrapper.children[0];assert.equal(player.preload,'none');assert.equal(player.autoplay,undefined);
  message.attachment.transcript='Nos vemos a las seis';f.media.render(row,message);assert.equal(row.children.length,1);assert.equal(wrapper.children[0],player);assert.equal(wrapper.children[1].hidden,false);f.media.destroy();
});
