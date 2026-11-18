import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import vm from 'node:vm';
import sharp from 'sharp';
const source=fs.readFileSync(new URL('../public/coach/coach-install.js',import.meta.url),'utf8');
class Node{
 constructor(tag=''){this.tag=tag;this.nodes=new Map();this.events=new Map();this.attrs={};this.dataset={};this.children=[];this.hidden=false;this.open=false;this.textContent='';this.disabled=false;}
 set innerHTML(value){this.html=value;if(this.tag==='dialog')for(const key of ['close','native','status','description','instructions','steps'])this.nodes.set(`[data-install-${key}]`,new Node(key));}
 querySelector(k){return this.nodes.get(k)||null;}querySelectorAll(k){return this.nodes.get(k)||[];}
 append(node){this.children.push(node);}setAttribute(k,v){this.attrs[k]=v;if(k==='open')this.open=true;}removeAttribute(k){delete this.attrs[k];if(k==='open')this.open=false;}
 addEventListener(type,fn){const list=this.events.get(type)||[];list.push(fn);this.events.set(type,list);}
 async fire(type,arg={}){await Promise.all((this.events.get(type)||[]).map(fn=>fn(arg)));}
 focus(){this.focused=true;}closest(){return this.menu||null;}showModal(){this.open=true;}close(){this.open=false;return this.fire('close');}
}
function fixture({ios=false,ipad=false,standalone=false,navigatorStandalone=false,openers=true,userAgent}={}){
 const root=new Node(),opener=new Node(),env=new Node(),mode=new Node();root.body=new Node();root.nodes.set('[data-open-tec-install]',openers?[opener]:[]);
 root.createElement=tag=>new Node(tag);root.querySelector=key=>key==='[data-tec-install-dialog]'?root.body.children.find(n=>n.tag==='dialog')||null:root.nodes.get(key)||null;
 mode.matches=standalone;env.matchMedia=()=>mode;env.navigator={userAgent:userAgent||(ios?'iPhone Safari':'Chrome Android'),platform:ipad?'MacIntel':'Linux',maxTouchPoints:ipad?5:0,standalone:navigatorStandalone};
 vm.runInNewContext(source,{document:root,window:env});const dialog=root.body.children[0];
 let prompts=0;
 function offer({outcome='accepted',throws=false,wait=null}={}){const event={preventDefault(){this.prevented=true;},async prompt(){prompts++;if(throws)throw new Error('browser unavailable');},userChoice:wait||Promise.resolve({outcome})};return{event,send:()=>env.fire('beforeinstallprompt',event)};}
 return{root,opener,env,mode,dialog,install:dialog?.querySelector('[data-install-native]'),status:dialog?.querySelector('[data-install-status]'),description:dialog?.querySelector('[data-install-description]'),steps:dialog?.querySelector('[data-install-steps]'),instructions:dialog?.querySelector('[data-install-instructions]'),close:dialog?.querySelector('[data-install-close]'),offer,get prompts(){return prompts;}};
}
test('the native browser prompt only runs after the install button gesture, not initialization, readiness or opening help',async()=>{
 const f=fixture();assert.equal(f.prompts,0);assert.equal(f.dialog.hidden,true);const offer=f.offer();await offer.send();assert.equal(offer.event.prevented,true);assert.equal(f.prompts,0);await f.opener.fire('click');assert.equal(f.dialog.open,true);assert.equal(f.prompts,0);assert.equal(f.install.hidden,false);await f.install.fire('click');assert.equal(f.prompts,1);
});
test('accepted and appinstalled are requests, and do not claim that the page opened standalone',async()=>{
 const f=fixture();await f.opener.fire('click');await f.offer().send();await f.install.fire('click');assert.match(f.status.textContent,/solicit|solicitud/i);assert.doesNotMatch(f.description.textContent,/Ya abriste TEC como app independiente/);assert.equal(f.mode.matches,false);
 await f.env.fire('appinstalled');assert.match(f.status.textContent,/solicitud.*aceptada/i);assert.doesNotMatch(f.status.textContent,/está abierta en modo app|confirmó la instalación/i);assert.doesNotMatch(f.description.textContent,/Ya abriste TEC como app independiente/);assert.equal(f.install.hidden,true);
});
test('dismissing the browser prompt keeps manual help and allows a fresh offer to be tried',async()=>{
 const f=fixture();await f.opener.fire('click');await f.offer({outcome:'dismissed'}).send();await f.install.fire('click');assert.match(f.status.textContent,/No se instaló/);assert.equal(f.install.hidden,true);assert.equal(f.install.disabled,false);await f.close.fire('click');assert.equal(f.dialog.hidden,true);assert.equal(f.opener.focused,true);await f.opener.fire('click');assert.equal(f.steps.open,true);await f.offer().send();assert.equal(f.install.hidden,false);await f.install.fire('click');assert.equal(f.prompts,2);
});
test('a rejected prompt gives a recoverable browser-menu fallback and does not consume future offers',async()=>{
 const f=fixture();await f.opener.fire('click');await f.offer({throws:true}).send();await f.install.fire('click');assert.match(f.status.textContent,/No se pudo abrir/);assert.equal(f.install.disabled,false);await f.offer().send();await f.install.fire('click');assert.equal(f.prompts,2);assert.match(f.status.textContent,/solicit|solicitud/i);
});
test('a pending browser prompt cannot be triggered twice or be overwritten by its late choice after appinstalled',async()=>{
 let finish;const f=fixture(),wait=new Promise(resolve=>{finish=resolve;});await f.opener.fire('click');await f.offer({wait}).send();const pending=f.install.fire('click');await Promise.resolve();assert.equal(f.install.disabled,true);await f.install.fire('click');assert.equal(f.prompts,1);await f.env.fire('appinstalled');finish({outcome:'dismissed'});await pending;assert.match(f.status.textContent,/solicitud.*aceptada/i);assert.equal(f.install.hidden,true);assert.equal(f.install.disabled,false);
});
test('actual standalone display mode suppresses install and reports the observed app mode',async()=>{
 const f=fixture({standalone:true});await f.opener.fire('click');assert.equal(f.install.hidden,true);assert.match(f.description.textContent,/Ya abriste TEC como app independiente/);assert.match(f.status.textContent,/está abierta en modo app/);assert.equal(f.steps.open,false);await f.offer().send();await f.install.fire('click');assert.equal(f.prompts,0);
 const browser=fixture();await browser.opener.fire('click');browser.mode.matches=true;await browser.mode.fire('change');assert.match(browser.status.textContent,/está abierta en modo app/);assert.equal(browser.install.hidden,true);
});
test('iPhone and touch-enabled iPad get Safari instructions; iOS standalone is recognized without matchMedia',async()=>{
 for(const config of [{ios:true},{ipad:true}]){const f=fixture(config);await f.opener.fire('click');assert.match(f.description.textContent,/Compartir de Safari/);assert.match(f.instructions.children.map(n=>n.textContent).join(' '),/Safari.*Compartir.*Añadir a pantalla de inicio.*Abrir como app web/);assert.equal(f.steps.open,true);assert.equal(f.install.hidden,true);assert.equal(f.prompts,0);}
 const app=fixture({ios:true,navigatorStandalone:true});await app.opener.fire('click');assert.match(app.status.textContent,/está abierta en modo app/);assert.equal(app.install.hidden,true);
});
test('initialization creates one dialog and does nothing on pages without an opener',()=>{
 const f=fixture();f.env.TecCoachInstall.init(f.root,f.env);assert.equal(f.root.body.children.length,1);assert.equal(fixture({openers:false}).root.body.children.length,0);
});
test('the manifest preserves app identity, launch scope and real image assets matching their declared dimensions',async()=>{
 const root=new URL('../public/',import.meta.url),manifest=JSON.parse(fs.readFileSync(new URL('coach/site.webmanifest',root),'utf8'));
 assert.equal(manifest.id,'/coach/');assert.equal(manifest.start_url,'/coach/app/');assert.equal(manifest.scope,'/coach/');assert.equal(manifest.display,'standalone');assert.notEqual(manifest.prefer_related_applications,true);assert.ok(manifest.name||manifest.short_name);assert.ok(manifest.icons.some(i=>i.sizes==='192x192'));assert.ok(manifest.icons.some(i=>i.sizes==='512x512'));
 assert.ok(manifest.screenshots.some(i=>i.form_factor==='narrow'));assert.ok(manifest.screenshots.some(i=>i.form_factor==='wide'));
 for(const asset of [...manifest.icons,...manifest.screenshots]){
  const url=new URL(asset.src,'https://totalelitecoaching.com');assert.equal(url.origin,'https://totalelitecoaching.com');assert.ok(url.pathname.startsWith('/coach/'));const file=new URL(url.pathname.slice(1),root);const data=fs.readFileSync(file);assert.ok(data.length>100);const info=await sharp(data).metadata();assert.equal(`${info.width}x${info.height}`,asset.sizes);assert.equal(`image/${info.format==='jpg'?'jpeg':info.format}`,asset.type);
 }
});

test('desktop installation describes the current computer rather than phone-only steps',async()=>{const f=fixture({userAgent:'Chrome Mac'});await f.opener.fire('click');const text=f.instructions.children.map(n=>n.textContent).join(' ');assert.match(text,/Chrome o Edge.*barra de direcciones.*computadora/);assert.doesNotMatch(text,/tres puntos|pantalla de inicio/);});
