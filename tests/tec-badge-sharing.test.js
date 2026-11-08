import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs/promises';
import crypto from 'node:crypto';
import express from 'express';
import sharp from 'sharp';
import {BADGES} from '../public/audiolibros/logros-core.js';
import {BADGE_VISUALS,badgeSvg} from '../public/audiolibros/insignias-art.js';
import {BADGE_CARD_FORMATS,badgeShareInfo,badgeShareText,nativeBadgeShare} from '../public/audiolibros/insignias-share-core.js';
import {BadgeSharingController} from '../public/audiolibros/insignias-compartir.js';
import {badgeSharePage,registerBadgeSharePages} from '../src/tec-badge-sharing.js';
const origin='https://totalelitecoaching.com';
const asset=id=>new URL('../public/audiolibros/insignias/v2/'+id,import.meta.url);

test('all 25 badges have distinct original vector symbols and earned/locked visual states',()=>{
  assert.deepEqual(Object.keys(BADGE_VISUALS).sort(),BADGES.map(b=>b.id).sort());
  assert.equal(new Set(Object.values(BADGE_VISUALS).map(a=>a.icon)).size,25);
  const hashes=new Set();
  for(const b of BADGES){const svg=badgeSvg(b.id);hashes.add(crypto.createHash('sha256').update(svg).digest('hex'));assert.doesNotMatch(svg.replace('xmlns="http://www.w3.org/2000/svg"',''),/<script|onload|https?:|<image|Audible/);assert.notEqual(svg,badgeSvg(b.id,false));}
  assert.equal(hashes.size,25);assert.throws(()=>badgeSvg('unknown'));
});

test('75 pre-rendered PNG cards have the exact share dimensions and distinct badge artwork',async()=>{
  const hashes=new Set();let bytes=0;
  for(const b of BADGES)for(const [format,size] of Object.entries(BADGE_CARD_FORMATS)){
    const png=await fs.readFile(asset(b.id+'-'+format+'.png')),meta=await sharp(png).metadata();
    assert.equal(meta.format,'png');assert.equal(meta.width,size.width);assert.equal(meta.height,size.height);assert.ok(png.length>10000&&png.length<2_000_000);
    hashes.add(crypto.createHash('sha256').update(png).digest('hex'));bytes+=png.length;
  }
  assert.equal(hashes.size,75);assert.ok(bytes<18_000_000);
});

test('share links use only an allowlisted badge and origin, without private progress or arbitrary input',()=>{
  for(const b of BADGES){const info=badgeShareInfo(b.id);assert.equal(info.url,origin+'/audiolibros/logros/'+b.id+'/');assert.match(info.image,/\/insignias\/v2\/[a-z0-9-]+-square\.png$/);assert.match(badgeShareText(info),/primer 10 %/);assert.doesNotMatch(info.url,/email|userId|token|position|\?/);}
  for(const [id,format] of [['<script>','square'],['tiempo-60','huge']])assert.throws(()=>badgeShareInfo(id,format));
  assert.throws(()=>badgeShareInfo('tiempo-60','square','javascript:alert(1)'));
});

test('native sharing chooses files when supported and preserves direct user activation',async()=>{
  const file=new File(['png'],'badge.png',{type:'image/png'}),info=badgeShareInfo('tiempo-60');let called=false,payload;
  const task=nativeBadgeShare({canShare:()=>true,share:async data=>{called=true;payload=data;}},info,file);
  assert.equal(called,true);assert.equal(await task,'shared-file');assert.deepEqual(payload.files,[file]);assert.match(payload.text,/https:\/\/totalelitecoaching.com\/audiolibros\/logros\/tiempo-60\//);
  assert.equal(payload.email,undefined);assert.equal(payload.userId,undefined);
});

test('unsupported devices, cancellation, share restrictions and link-only sharing fail safely',async()=>{
  const info=badgeShareInfo('tiempo-60'),file=new File(['png'],'badge.png',{type:'image/png'});let payload;
  assert.equal(await nativeBadgeShare({},info,file),'unsupported');
  assert.equal(await nativeBadgeShare({canShare:()=>false,share:async data=>{payload=data;}},info,file),'shared-link');assert.equal(payload.url,info.url);assert.equal(payload.files,undefined);assert.ok(!payload.text.includes(info.url));
  assert.equal(await nativeBadgeShare({canShare:()=>true,share:async()=>{throw Object.assign(new Error('cancel'),{name:'AbortError'});}},info,file),'cancelled');
  assert.equal(await nativeBadgeShare({canShare:()=>{throw new Error('blocked');},share:async()=>{}},info,file),'failed');
});

test('public badge pages are cookie-independent previews and never expose account history or bypass book access',async t=>{
  const app=express();registerBadgeSharePages(app,{publicOrigin:origin});const server=app.listen(0,'127.0.0.1');await new Promise(resolve=>server.once('listening',resolve));t.after(()=>new Promise(resolve=>server.close(resolve)));
  const base='http://127.0.0.1:'+server.address().port;
  const response=await fetch(base+'/audiolibros/logros/tiempo-60/');assert.equal(response.status,200);assert.match(response.headers.get('cache-control'),/public/);
  const html=await response.text();assert.match(html,/<meta property="og:image"[^>]+tiempo-60-preview\.png/);assert.match(html,/content="1200"/);assert.match(html,/content="630"/);assert.match(html,/noindex,follow/);
  assert.doesNotMatch(html,/analytics\.js|googletagmanager|tec_audio_session|stripeCustomer|position|<script/);
  assert.equal(await(await fetch(base+'/audiolibros/logros/tiempo-60/?email=SECRET',{headers:{Cookie:'tec_audio_session=PRIVATE'}})).text(),html);
  const redirect=await fetch(base+'/audiolibros/logros/tiempo-60',{redirect:'manual'});assert.equal(redirect.status,301);assert.equal(redirect.headers.get('location'),'/audiolibros/logros/tiempo-60/');
  assert.equal((await fetch(base+'/audiolibros/logros/not-a-badge/')).status,404);
  for(const b of BADGES)assert.equal((await fetch(base+'/audiolibros/logros/'+b.id+'/')).status,200);
  const png=await fetch(base+'/audiolibros/insignias/v2/tiempo-60-preview.png');assert.equal(png.headers.get('content-type'),'image/png');assert.match(png.headers.get('cache-control'),/immutable/);
  const titlePage=badgeSharePage('titulo-como-un-hombre-piensa',origin);assert.match(titlePage,/\/audiolibros\/\?libro=como-un-hombre-piensa/);
});

function fixture({eligible=true,nav={},fetchImpl}={}){
  const elements=new Map();let focus=false,downloadClicks=0;
  const element=selector=>{if(!elements.has(selector))elements.set(selector,{value:selector==='[data-badge-share-format]'?'square':'',hidden:false,disabled:false,handlers:{},addEventListener(event,fn){this.handlers[event]=fn;},removeAttribute(key){delete this[key];},focus(){focus=true;},select(){focus=true;},click(){downloadClicks++;}});return elements.get(selector);};
  const dialog={open:false,handlers:{},querySelector:element,querySelectorAll(){return['native','instagram','tiktok','mensajes'].map(id=>element('[data-badge-share-'+id+']'));},addEventListener(event,fn){this.handlers[event]=fn;},showModal(){this.open=true;},close(){this.open=false;this.handlers.close?.();}};
  let allowed=eligible;const controller=new BadgeSharingController({dialog,eligible:()=>allowed,nav,fetchImpl});
  return{controller,dialog,element,allow:v=>{allowed=v;},focus:()=>focus,downloads:()=>downloadClicks};
}

test('only earned badges open the sharing dialog and the prepared file matches the chosen format',async()=>{
  const png=await fs.readFile(asset('tiempo-60-square.png'));let fetches=0;
  const f=fixture({eligible:false,fetchImpl:async()=>{fetches++;return{ok:true,blob:async()=>new Blob([png],{type:'image/png'})};}});
  await f.controller.open('tiempo-60');assert.equal(f.dialog.open,false);assert.equal(fetches,0);
  f.allow(true);await f.controller.open('tiempo-60');assert.equal(f.dialog.open,true);assert.match(f.controller.file.name,/tiempo-60-square\.png$/);assert.equal(f.element('[data-badge-share-native]').disabled,false);
  assert.match(f.element('[data-badge-share-facebook]').href,/facebook\.com\/sharer\/sharer.php\?u=https%3A/);assert.match(f.element('[data-badge-share-sms]').href,/^sms:/);
  f.allow(false);f.controller.eligibilityChanged();assert.equal(f.dialog.open,false);assert.equal(f.controller.file,null);
});

test('changing formats or closing while a request is pending cannot expose a stale image',async()=>{
  const png=await fs.readFile(asset('tiempo-60-square.png'));let resolveFirst;
  const f=fixture({fetchImpl:async path=>path.includes('-square')?new Promise(resolve=>{resolveFirst=()=>resolve({ok:true,blob:async()=>new Blob([png],{type:'image/png'})});}):{ok:true,blob:async()=>new Blob([png],{type:'image/png'})}});
  const first=f.controller.open('tiempo-60');f.element('[data-badge-share-format]').value='story';await f.controller.prepare();resolveFirst();await first;
  assert.match(f.controller.file.name,/-story\.png$/);f.dialog.close();assert.equal(f.controller.objectURL,null);assert.equal(f.controller.file,null);
});

test('offline card failures still leave the public link available without changing playback',async()=>{
  const f=fixture({fetchImpl:async()=>{throw new Error('offline');}});await f.controller.open('tiempo-60');
  assert.equal(f.controller.file,null);assert.match(f.element('[data-badge-share-link]').value,/\/logros\/tiempo-60\//);assert.match(f.element('[data-badge-share-status]').textContent,/copiar el enlace/);
});

test('Instagram, TikTok and Messages have a download/copy fallback and tolerate blocked clipboard',async()=>{
  const png=await fs.readFile(asset('tiempo-60-square.png'));
  for(const platform of ['instagram','tiktok','mensajes']){
    const f=fixture({fetchImpl:async()=>({ok:true,blob:async()=>new Blob([png],{type:'image/png'})})});await f.controller.open('tiempo-60');await f.controller.platform(platform);
    assert.equal(f.downloads(),1);assert.equal(f.focus(),true);assert.match(f.element('[data-badge-share-status]').textContent,/Descarga preparada/);f.dialog.close();
  }
});

test('the default browser fetch keeps its native receiver rather than binding to the controller',async()=>{
  const png=await fs.readFile(asset('tiempo-60-square.png')),original=globalThis.fetch;
  globalThis.fetch=function(){assert.ok(this===undefined||this===globalThis);return Promise.resolve({ok:true,blob:async()=>new Blob([png],{type:'image/png'})});};
  try{const f=fixture();await f.controller.open('tiempo-60');assert.ok(f.controller.file);f.dialog.close();}
  finally{globalThis.fetch=original;}
});
