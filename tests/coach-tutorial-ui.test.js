import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import vm from 'node:vm';
const sourceCode=fs.readFileSync(new URL('../public/coach/coach-tutorial.js',import.meta.url),'utf8');
class Node {
 constructor(){this.events=new Map();this.dataset={};this.attrs={};this.nodes=new Map();this.hidden=false;this.open=false;this.pauses=0;this.loads=0;this.textContent='';this.focused=false;this.value='';}
 addEventListener(type,fn){if(!this.events.has(type))this.events.set(type,[]);this.events.get(type).push(fn);}
 fire(type){for(const fn of this.events.get(type)||[])fn({});}
 querySelector(key){return this.nodes.get(key);}
 querySelectorAll(key){return this.nodes.get(key)||[];}
 setAttribute(k,v){this.attrs[k]=v;}
 getAttribute(k){return this.attrs[k]||null;}
 removeAttribute(k){delete this.attrs[k];}
 focus(){this.focused=true;}
 showModal(){this.open=true;}
 close(){this.open=false;this.fire('close');}
 click(){this.fire('click');}
 pause(){this.pauses++;}
 load(){this.loads++;}
 closest(){return this.menu;}
}
function fixture(){
 const root=new Node(),dialog=new Node(),opener=new Node(),close=new Node(),video=new Node(),source=new Node(),search=new Node(),result=new Node(),error=new Node(),menu=new Node(),summary=new Node(),otherAudio=new Node(),window=new Node();
 const items=['¿Cómo cancelo un audio? Desliza a la izquierda.','¿Cómo regreso al orden estándar? Restablecer y Guardar.','¿Quién puede ver mis mensajes? Mi equipo.'].map(text=>{const n=new Node();n.textContent=text;return n;});
 root.nodes.set('[data-coach-tutorial]',dialog);root.nodes.set('[data-open-coach-tutorial]',[opener]);root.nodes.set('audio,video',[video,otherAudio]);
 for(const [key,node] of [['[data-close-coach-tutorial]',close],['video',video],['[data-tutorial-video-src]',source],['[data-tutorial-search]',search],['[data-tutorial-search-result]',result],['[data-tutorial-faq]',items],['[data-tutorial-video-error]',error]])dialog.nodes.set(key,node);
 opener.menu=menu;menu.open=true;menu.nodes.set('summary',summary);source.dataset.tutorialVideoSrc='/api/coach/tutorial/media/recorrido-3min.mp4';dialog.hidden=true;error.hidden=true;
 video.nodes.set('[data-tutorial-video-src]',source);video.parentElement=dialog;
 const mobile=['iphone','android'].map(name=>{const v=new Node(),src=new Node(),section=new Node(),err=new Node();v.parentElement=section;v.menu=section;v.nodes.set('[data-tutorial-video-src]',src);section.nodes.set('[data-tutorial-video-error]',err);src.dataset.tutorialVideoSrc=`/api/coach/tutorial/media/instalar-${name}.mp4`;err.hidden=true;return{video:v,source:src,section,error:err};});
 const chapterStatus=new Node(),chapters=[56.5,127.5].map((seconds,i)=>{const n=new Node();n.dataset.tutorialChapter=String(seconds);n.dataset.chapterLabel=i?'Audiolibros':'Calculadora';return n;});
 dialog.nodes.set('[data-tutorial-chapter]',chapters);dialog.nodes.set('[data-tutorial-chapter-status]',chapterStatus);video.readyState=0;video.duration=NaN;video.currentTime=0;
 dialog.nodes.set('video',[video,...mobile.map(p=>p.video)]);root.nodes.set('audio,video',[video,otherAudio,...mobile.map(p=>p.video)]);
 vm.runInNewContext(sourceCode,{document:root,window});return{root,dialog,opener,close,video,source,search,result,error,menu,summary,items,otherAudio,window,mobile,chapters,chapterStatus};
}
test('opening is read-only, lazy-loads the video without playing it, and closes the account menu',()=>{
 const f=fixture();assert.equal(f.video.loads,0);f.opener.click();assert.equal(f.dialog.open,true);assert.equal(f.dialog.hidden,false);assert.equal(f.menu.open,false);assert.equal(f.video.loads,1);assert.equal(f.source.getAttribute('src'),f.source.dataset.tutorialVideoSrc);assert.equal(f.close.focused,true);
});
test('close or Escape pauses video and restores focus to the visible account summary',()=>{
 const f=fixture();f.opener.click();f.close.click();assert.equal(f.dialog.hidden,true);assert.equal(f.video.pauses,1);assert.equal(f.summary.focused,true);f.opener.click();assert.equal(f.video.loads,1);f.dialog.close();assert.equal(f.video.pauses,2);
});
test('help search matches accented words and answer text; no results and clearing restore sensible content',()=>{
 const f=fixture();f.search.value='estandar';f.search.fire('input');assert.deepEqual(f.items.map(n=>n.hidden),[true,false,true]);assert.equal(f.items[1].open,true);assert.match(f.result.textContent,/1 respuesta/);
 f.search.value='<script>';f.search.fire('input');assert.ok(f.items.every(n=>n.hidden));assert.match(f.result.textContent,/No hay coincidencias/);
 f.search.value='';f.search.fire('input');assert.ok(f.items.every(n=>!n.hidden&&!n.open));assert.equal(f.result.textContent,'');
});
test('failed video gives a useful fallback and retries on next open; recovery clears the notice',()=>{
 const f=fixture();f.opener.click();f.video.error={code:4};f.source.fire('error');assert.equal(f.error.hidden,false);f.close.click();f.opener.click();assert.equal(f.video.loads,2);f.video.error=null;f.video.fire('loadeddata');assert.equal(f.error.hidden,true);
});
test('video playback pauses other audio and stops when the app becomes hidden',()=>{
 const f=fixture();f.opener.click();f.video.fire('play');assert.equal(f.otherAudio.pauses,1);assert.equal(f.video.pauses,0);f.root.hidden=true;f.root.fire('visibilitychange');assert.equal(f.video.pauses,1);f.window.fire('pagehide');assert.equal(f.video.pauses,2);
});

test('a pending browser play event cannot restart the video after closing the tutorial',()=>{const f=fixture();f.opener.click();f.close.click();const count=f.video.pauses;f.video.fire('play');assert.equal(f.video.pauses,count+1);assert.equal(f.otherAudio.pauses,0);});

test('phone videos load only when expanded, remain exclusive, pause when folded or closed, and retry per player',()=>{
 const f=fixture(),[iphone,android]=f.mobile;
 f.opener.click();assert.equal(iphone.video.loads,0);assert.equal(android.video.loads,0);
 iphone.section.open=true;iphone.section.fire('toggle');assert.equal(iphone.video.loads,1);assert.equal(android.video.loads,0);
 iphone.video.fire('play');assert.equal(f.video.pauses,1);assert.equal(android.video.pauses,1);
 android.section.open=true;android.section.fire('toggle');android.video.fire('play');assert.equal(iphone.video.pauses,1);
 android.section.open=false;android.section.fire('toggle');assert.equal(android.video.pauses,2);
 iphone.video.error={code:4};iphone.video.fire('error');assert.equal(iphone.error.hidden,false);assert.equal(f.error.hidden,true);
 iphone.section.open=false;iphone.section.fire('toggle');iphone.section.open=true;iphone.section.fire('toggle');assert.equal(iphone.video.loads,2);
 iphone.video.error=null;iphone.video.fire('loadeddata');assert.equal(iphone.error.hidden,true);
 f.close.click();assert.equal(iphone.video.pauses,3);assert.equal(android.video.pauses,3);
 const n=iphone.video.pauses;iphone.video.fire('play');assert.equal(iphone.video.pauses,n+1);
});

test('chapters wait for metadata, use the latest choice and seek without auto-playing',()=>{
 const f=fixture();f.opener.click();f.chapters[0].click();assert.equal(f.video.currentTime,0);assert.match(f.chapterStatus.textContent,/Preparando/);
 f.chapters[1].click();f.video.duration=180;f.video.readyState=1;f.video.fire('loadedmetadata');assert.equal(f.video.currentTime,127.5);assert.equal(f.chapters[1].getAttribute('aria-current'),'true');assert.equal(f.chapters[0].getAttribute('aria-current'),null);assert.match(f.chapterStatus.textContent,/Audiolibros.*reproducir/);assert.ok(f.mobile.every(p=>p.video.pauses===2));assert.equal(f.otherAudio.pauses,0);
 f.video.duration=120;f.chapters[1].click();assert.equal(f.video.currentTime,119.95);
});
test('closing cancels pending chapter seeks so late metadata cannot move a hidden video',()=>{
 const f=fixture();f.opener.click();f.chapters[0].click();f.close.click();f.video.readyState=1;f.video.duration=180;f.video.fire('loadedmetadata');assert.equal(f.video.currentTime,0);assert.equal(f.chapterStatus.textContent,'');
 f.chapters[1].click();assert.equal(f.video.currentTime,0);
});
