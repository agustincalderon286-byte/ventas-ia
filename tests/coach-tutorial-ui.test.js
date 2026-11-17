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
 vm.runInNewContext(sourceCode,{document:root,window});return{root,dialog,opener,close,video,source,search,result,error,menu,summary,items,otherAudio,window};
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
