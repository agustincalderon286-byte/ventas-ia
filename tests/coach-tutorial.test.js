import test from 'node:test';
import assert from 'node:assert/strict';
import express from 'express';
import fs from 'node:fs';
import path from 'node:path';
import {fileURLToPath} from 'node:url';
import {registerCoachTutorial,coachTutorialAssets} from '../src/coach-tutorial.js';
const root=fileURLToPath(new URL('../private/coach-tutorial/',import.meta.url));
async function fixture(t){
 const app=express();registerCoachTutorial(app,{requireActive:async(req,res)=>{if(req.get('X-Test-Active')!=='yes'){res.status(401).json({error:'Acceso requerido'});return null;}return{user:{_id:'test-seat'}};}});
 const server=await new Promise(resolve=>{const s=app.listen(0,'127.0.0.1',()=>resolve(s));});t.after(()=>{server.closeAllConnections();server.close();});
 return (asset,headers={})=>fetch(`http://127.0.0.1:${server.address().port}/api/coach/tutorial/media/${asset}`,{headers});
}
test('tutorial video and screenshots require active access, including byte-range requests',async t=>{
 const get=await fixture(t);
 for(const asset of ['recorrido-3min.mp4','instalar-iphone.mp4','instalar-android.mp4','instalar-iphone.jpg','instalar-android.jpg','voz.jpg']){const r=await get(asset,{Range:'bytes=0-15'});assert.equal(r.status,401);assert.equal(r.headers.get('cache-control'),'private, no-store');assert.match(r.headers.get('vary'),/Cookie/);}
});
test('authenticated members receive seekable video ranges and private images',async t=>{
 const get=await fixture(t);let r=await get('recorrido-3min.mp4',{'X-Test-Active':'yes',Range:'bytes=0-15'});assert.equal(r.status,206);assert.equal(r.headers.get('content-type'),'video/mp4');assert.match(r.headers.get('content-range'),/^bytes 0-15\//);assert.deepEqual(Buffer.from(await r.arrayBuffer()),fs.readFileSync(path.join(root,'recorrido-3min.mp4')).subarray(0,16));assert.equal(r.headers.get('cache-control'),'private, no-store');
 r=await get('voz.jpg',{'X-Test-Active':'yes'});assert.equal(r.status,200);assert.equal(r.headers.get('content-type'),'image/jpeg');assert.equal(r.headers.get('x-robots-tag'),'noindex, nofollow');
});
test('only explicitly listed tutorial media can be read',async t=>{
 const get=await fixture(t);for(const asset of ['server.js','coach-app.html','..%2Fcoach-app.html','%2e%2e%2fserver.js']){const r=await get(asset,{'X-Test-Active':'yes'});assert.equal(r.status,404);}
});
test('every tutorial image and video reference maps to a private existing asset; service worker excludes private media',()=>{
 const html=fs.readFileSync(new URL('../private/coach-app.html',import.meta.url),'utf8');
 const references=[...html.matchAll(/\/api\/coach\/tutorial\/media\/([a-z0-9.-]+)/g)].map(match=>match[1]);
 assert.ok(references.length>=12);for(const name of references){assert.ok(coachTutorialAssets.includes(name),name);assert.ok(fs.statSync(path.join(root,name)).size>1000,name);}
 assert.equal((html.match(/data-open-coach-tutorial/g)||[]).length,1);assert.match(html,/<details class="tec-account-menu"[\s\S]*data-open-coach-tutorial/);
 assert.match(html,/preload="none"/);assert.doesNotMatch(html.match(/<video controls[\s\S]*?<\/video>/)[0],/autoplay/);
 const sw=fs.readFileSync(new URL('../public/coach/sw.js',import.meta.url),'utf8');assert.match(sw,/if \(url.pathname.startsWith\("\/api\/coach\/tutorial\/media\/"\)\) return;/);
});

test('both phone tutorials support authenticated seeking without exposing media to anonymous viewers',async t=>{
 const get=await fixture(t);
 for(const name of ['instalar-iphone.mp4','instalar-android.mp4']){const r=await get(name,{'X-Test-Active':'yes',Range:'bytes=128-255'});assert.equal(r.status,206);assert.equal(r.headers.get('content-type'),'video/mp4');assert.deepEqual(Buffer.from(await r.arrayBuffer()),fs.readFileSync(path.join(root,name)).subarray(128,256));assert.equal(r.headers.get('cache-control'),'private, no-store');}
});
