import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import vm from 'node:vm';
const source=fs.readFileSync(new URL('../public/coach/coach-onboarding.js',import.meta.url),'utf8');
class Node {
 constructor(){this.events=new Map();this.nodes=new Map();this.hidden=false;this.open=false;this.attrs={};this.textContent='';this.focused=false;this.paused=true;this.pauses=0;}
 addEventListener(type,fn){if(!this.events.has(type))this.events.set(type,[]);this.events.get(type).push(fn);}
 removeEventListener(type,fn){this.events.set(type,(this.events.get(type)||[]).filter(f=>f!==fn));}
 fire(type){const event={defaultPrevented:false,preventDefault(){this.defaultPrevented=true;}};for(const fn of this.events.get(type)||[])fn(event);return event;}
 querySelector(key){return this.nodes.get(key)||null;}
 querySelectorAll(key){return this.nodes.get(key)||[];}
 showModal(){this.open=true;}
 close(){this.open=false;this.fire('close');}
 setAttribute(k,v){this.attrs[k]=v;}
 removeAttribute(k){delete this.attrs[k];}
 click(){this.fire('click');}
 focus(){this.focused=true;}
 closest(){return this.menu||null;}
 pause(){this.pauses++;this.paused=true;}
}
function fixture({seen=new Map(),storageBroken=false,editing=false,hiddenApp=false,playing=false}={}){
 const root=new Node(),app=new Node(),dialog=new Node(),opener=new Node(),menu=new Node(),summary=new Node(),tutorial=new Node(),media=new Node(),window=new Node();
 app.hidden=hiddenApp;dialog.hidden=true;opener.hidden=true;opener.menu=menu;menu.open=true;menu.nodes.set('summary',summary);root.activeElement={matches:()=>editing};media.paused=!playing;
 root.nodes.set('[data-coach-app-page]',app);root.nodes.set('[data-coach-onboarding]',dialog);root.nodes.set('[data-open-coach-onboarding]',[opener]);root.nodes.set('[data-open-coach-tutorial]',tutorial);root.nodes.set('[data-visit-account-menu] > summary',summary);root.nodes.set('audio,video',[media]);
 const keys=['next','back','skip','tutorial','title','copy','progress','route','hint'];const controls={};for(const name of keys){controls[name]=new Node();dialog.nodes.set(`[data-onboarding-${name}]`,controls[name]);}
 const store={getItem(key){if(storageBroken)throw new Error('blocked');return seen.get(key)||null;},setItem(key,value){if(storageBroken)throw new Error('blocked');seen.set(key,value);}};
 window.localStorage=store;vm.runInNewContext(source,{window,document:root});return{root,app,dialog,opener,summary,controls,seen,window,media,tutorial,api:window.TecCoachOnboarding,store};
}
test('a first signed-in visit opens three informational steps and writes only on explicit completion',()=>{
 const f=fixture();f.api.init({user:{id:'account-A'}});assert.equal(f.dialog.open,true);assert.equal(f.seen.size,0);assert.match(f.controls.title.textContent,/Cierre/);assert.equal(f.controls.back.hidden,true);
 f.controls.next.click();assert.match(f.controls.progress.textContent,/2 de 3/);f.controls.next.click();assert.equal(f.controls.tutorial.hidden,false);assert.equal(f.seen.size,0);f.controls.next.click();assert.equal(f.dialog.open,false);assert.equal(f.dialog.hidden,true);assert.equal(f.seen.size,1);assert.match([...f.seen.values()][0],/completed/);assert.equal(f.summary.focused,true);
 const again=fixture({seen:f.seen});again.api.init({user:{id:'account-A'}});assert.equal(again.dialog.open,false);again.opener.click();assert.equal(again.dialog.open,true);assert.match(again.controls.progress.textContent,/1 de 3/);
});
test('seen preferences belong to the exact account and switching removes old handlers',()=>{
 const f=fixture();f.api.init({user:{id:'account-A'}});f.api.init({user:{id:'account-B'}});assert.equal(f.dialog.open,true);f.controls.skip.click();assert.equal(f.seen.size,1);assert.ok([...f.seen.keys()][0].endsWith('account-B'));assert.match([...f.seen.values()][0],/skipped/);
 f.api.init({user:{id:'account-A'}});assert.equal(f.dialog.open,true);assert.equal(f.seen.size,1);
 f.api.init({user:null});assert.equal(f.dialog.open,false);assert.equal(f.opener.hidden,true);f.controls.skip.click();assert.equal(f.seen.size,1);
});
test('Escape is a dismissal rather than a stored completion; storage failures never block the guide',()=>{
 const f=fixture();f.api.init({user:{id:'A'}});assert.equal(f.dialog.fire('cancel').defaultPrevented,true);assert.equal(f.dialog.open,false);assert.equal(f.seen.size,0);
 const blocked=fixture({storageBroken:true});blocked.api.init({user:{id:'A'}});blocked.controls.next.click();blocked.controls.next.click();assert.doesNotThrow(()=>blocked.controls.next.click());assert.equal(blocked.dialog.open,false);
});
test('the guide does not run on login, before account visibility, while editing, during playback or over another dialog',()=>{
 const anon=fixture();assert.equal(anon.api.init(),null);assert.equal(anon.dialog.open,false);
 const loading=fixture({hiddenApp:true});assert.equal(loading.api.init({user:{id:'A'}}),null);
 for(const setup of [{editing:true},{playing:true}]){const f=fixture(setup);f.api.init({user:{id:'A'}});assert.equal(f.dialog.open,false);assert.equal(f.seen.size,0);f.opener.click();assert.equal(f.dialog.open,true);assert.equal(f.media.pauses,1);}
 const modal=fixture();modal.root.nodes.set('dialog[open]',new Node());modal.api.init({user:{id:'A'}});assert.equal(modal.dialog.open,false);assert.equal(modal.seen.size,0);
});
test('finishing into the tutorial closes first, avoids stealing focus, and opens only the help dialog',()=>{
 const f=fixture();let opened=0;f.tutorial.addEventListener('click',()=>{assert.equal(f.dialog.open,false);opened++;});f.api.init({user:{id:'A'}});f.controls.next.click();f.controls.next.click();f.controls.tutorial.click();assert.equal(opened,1);assert.equal(f.summary.focused,false);assert.equal(f.seen.size,1);
});
