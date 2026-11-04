import test from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import vm from 'node:vm';
import express from 'express';
import { EVENTS, safeEvent, safePage } from '../public/coach/analytics-events.js';
import { sanitizeUsageBatch, usageOperations, registerTecUsage } from '../src/tec-usage.js';
const source = readFileSync(new URL('../public/coach/analytics.js', import.meta.url), 'utf8');
function browser({ choice = '', online = true, busy = false, gpc = false, native = false, brokenStorage = false, observer = false } = {}) {
  const handlers = {}, docHandlers = {}, timers = new Map(), scripts = [], writes = [], requests = [], store = new Map([['tec.analytics.choice.v1', choice]]), cookies = [];
  let timerId = 0, observeChange;
  let observedOptions;
  const elements = new Map();
  function node(tag) { return { tag, hidden: false, children: [], dataset: {}, setAttribute(){}, append(child){this.children.push(child); if(tag==='head')scripts.push(child);}, addEventListener(type, cb){this[type]=cb;}, querySelector(selector){if(!elements.has(selector))elements.set(selector,node(selector));return elements.get(selector);}, matches(){return false;} }; }
  const doc = { head: node('head'), body: node('body'), createElement: node, getElementById: id => doc.body.children.find(x=>x.id===id), querySelector: selector=>selector==='[data-demo-recorder]'?{dataset:{captureBusy:String(busy)}}:null, addEventListener(type, cb){docHandlers[type]=cb;} };
  Object.defineProperty(doc,'cookie',{set(v){cookies.push(v);}});
  const context = { document:doc, location:{pathname:'/coach/app/',href:'https://totalelitecoaching.com/coach/app/?email=secret@example.test#4415-PRIVATE-ST'}, matchMedia:()=>({matches:false}), navigator:{onLine:online,globalPrivacyControl:gpc}, localStorage:{getItem:k=>{if(brokenStorage)throw Error();return store.get(k);},setItem:(k,v)=>{if(brokenStorage)throw Error();store.set(k,v);writes.push(k);}}, safeEvent, safePage, EVENTS, MEASUREMENT_ID:'G-DZCX3RMST7', AREAS:['inicio','login','planes','cierre','actividad','mapa','equipo','biblioteca','privacidad'], setTimeout(cb){timers.set(++timerId,cb);return timerId;}, clearTimeout(id){timers.delete(id);}, fetch:async(url,opt)=>{requests.push({url,...opt});return {ok:true};}, AbortController, AbortSignal, Date };
  if (observer) context.MutationObserver = class { constructor(cb){observeChange=cb;} observe(_target, options){observedOptions=options;} };
  context.window = { addEventListener(type,cb){handlers[type]=cb;}, ...(native?{TecNativeOffline:{}}:{}) };
  vm.runInNewContext(source.replace(/^import .*;\n/,''),context);
  return {context, scripts, doc, handlers, docHandlers, elements, requests, store, writes, cookies,
    async flush(){const list=[...timers.values()];timers.clear();for(const fn of list)await fn();},
    click(selector){const t={disabled:false, matches:s=>s.split(', ').includes(selector)};docHandlers.click({target:{closest:()=>t}});},
    accept(){elements.get('[data-analytics-accept]').click();}, reject(){elements.get('[data-analytics-reject]').click();}, busy(v){busy=v;observeChange?.();}, observed:()=>observedOptions };
}
test('explicit vocabulary rejects every arbitrary value and nested payload',()=>{
  for (const name of ['customer@example.test','4415 Harrison St',null,{},'recording.mp4']) assert.equal(safeEvent(name,'mapa','web'),null);
  assert.equal(safeEvent('route_open','123 Private Street','web'),null);
  assert.equal(sanitizeUsageBatch({events:[{name:'route_open',area:'mapa',surface:'web',address:'private'}]}),null);
  assert.equal(sanitizeUsageBatch({events:[],email:'private'}),null);
  assert.equal(sanitizeUsageBatch({events:Array(9).fill({name:'page_view',area:'cierre',surface:'web'})}),null);
});
test('no script or telemetry before consent; rejecting never touches drafts or sessions',async()=>{
  const f=browser();assert.equal(f.scripts.length,0);f.click('[data-order-calc-toggle]');await f.flush();assert.equal(f.requests.length,0);
  f.reject();assert.equal(f.store.get('tec.analytics.choice.v1'),'rejected');assert.deepEqual(f.writes,['tec.analytics.choice.v1']);assert.equal(f.scripts.length,0);assert.ok(f.cookies.every(c=>c.startsWith('_ga')));
});
test('acceptance sends only fixed metadata, never real query/hash/referrer or DOM contents',async()=>{
  const f=browser();f.accept();assert.equal(f.scripts.length,1);f.scripts[0].onload();f.click('[data-property-walk]');f.click('[data-order-calc-toggle]');await f.flush();
  const payload=JSON.stringify(f.requests)+JSON.stringify(f.context.window.dataLayer.map(a=>Array.from(a)));
  assert.doesNotMatch(payload,/secret@|4415|PRIVATE|email=|href|user_id|user_properties/);
  const config=f.context.window.dataLayer.find(a=>a[0]==='config')[2];assert.equal(config.send_page_view,false);assert.equal(config.allow_google_signals,false);assert.equal(config.page_referrer,'');
  const names=JSON.parse(f.requests[0].body).events.map(x=>x.name);assert.deepEqual(names,['page_view','route_open','calculator_open']);
  assert.equal(f.requests[0].cache,'no-store');
});
test('offline does not load Google, enqueue retries or touch operational storage',async()=>{
  const f=browser({choice:'accepted',online:false});f.click('[data-property-walk]');await f.flush();assert.equal(f.scripts.length,0);assert.equal(f.requests.length,0);assert.equal(f.writes.length,0);
  f.context.navigator.onLine=true;f.handlers.online();assert.equal(f.scripts.length,1);
  f.context.navigator.onLine=false;f.handlers.offline();await f.flush();assert.equal(f.requests.length,0);
});
test('a recording suppresses telemetry without altering media or GPS APIs',async()=>{
  const f=browser({choice:'accepted',busy:true});assert.equal(f.scripts.length,0);f.click('[data-order-calc-toggle]');await f.flush();assert.equal(f.requests.length,0);
  assert.doesNotMatch(source,/getUserMedia|MediaRecorder|watchPosition|indexedDB|serviceWorker|\.value\b|textContent\.trim|document\.referrer|location\.(href|search|hash)/);
});
test('queued analytics is discarded when recording begins, not sent after it',async()=>{
  const f=browser({choice:'accepted'});f.click('[data-property-walk]');f.busy(true);await f.flush();assert.equal(f.requests.length,0);f.busy(false);await f.flush();assert.equal(f.requests.length,0);
});
test('GPC and native apps do not load any tag',async()=>{
  for(const options of [{gpc:true},{native:true}]){const f=browser({choice:'accepted',...options});assert.equal(f.scripts.length,0);await f.flush();assert.equal(f.requests.length,0);}
});
test('storage errors do not throw; no consent still means no Google',()=>{const f=browser({brokenStorage:true});assert.equal(f.scripts.length,0);f.reject();});
test('Google failure stays bounded while first-party counts remain optional',async()=>{
  const f=browser();f.accept();f.scripts[0].onerror();for(let n=0;n<100;n++)f.click('[data-property-walk]');await f.flush();assert.equal(f.context.window.dataLayer.length,0);assert.ok(JSON.parse(f.requests[0].body).events.length<=8);
});
test('consent can be withdrawn and accepted again without injecting a second tag',async()=>{
  const f=browser();f.accept();f.scripts[0].onload();f.reject();f.click('[data-property-walk]');await f.flush();assert.equal(f.requests.length,0);assert.equal(f.context.window['ga-disable-G-DZCX3RMST7'],true);f.accept();f.click('[data-property-walk]');await f.flush();assert.equal(f.scripts.length,1);assert.equal(f.requests.length,1);
});
test('daily aggregates contain no identifier, event content or exact event timestamp',()=>{
  const event={name:'route_open',area:'mapa',surface:'pwa'};const ops=usageOperations([event,event],new Date('2026-11-04T12:34:56Z'));
  assert.equal(ops.length,1);assert.equal(ops[0].updateOne.update.$inc.count,2);assert.deepEqual(Object.keys(ops[0].updateOne.update.$setOnInsert).sort(),['area','day','expiresAt','name','surface']);assert.equal(ops[0].updateOne.update.$setOnInsert.expiresAt.toISOString(),'2027-02-02T00:00:00.000Z');
});
test('sanitized pages are constants even with malformed areas',()=>{assert.equal(safePage('private@example.test'),'https://totalelitecoaching.com/coach/app/');assert.equal(safePage('planes'),'https://totalelitecoaching.com/coach/planes/');});
async function apiFixture(t, admin=true) {
  const app=express(), saved=[];app.use(express.json());registerTecUsage(app,{model:{bulkWrite:async rows=>saved.push(rows),aggregate:async()=>[{_id:'route_open',count:4}]},requireAdmin:async(req,res)=>{if(!admin){res.sendStatus(403);return null;}return {user:{}};}});
  const server=app.listen(0,'127.0.0.1');await new Promise(resolve=>server.once('listening',resolve));t.after(()=>new Promise(resolve=>server.close(resolve)));return {base:`http://127.0.0.1:${server.address().port}`,saved};
}
const headers={'Content-Type':'application/json',Origin:'https://totalelitecoaching.com','Sec-Fetch-Site':'same-origin','X-TEC-Usage':'v1'};
const body=JSON.stringify({events:[{name:'route_open',area:'mapa',surface:'web'}]});
test('collector blocks foreign origins and extra personal fields without writing',async t=>{
  const f=await apiFixture(t);for(const opt of [{headers:{...headers,Origin:'https://evil.test'},body},{headers,body:JSON.stringify({events:[{name:'route_open',area:'mapa',surface:'web',email:'person@test'}]})}]){const r=await fetch(f.base+'/api/coach/usage-events',{method:'POST',...opt});assert.ok([400,403].includes(r.status));}assert.equal(f.saved.length,0);
});
test('collector accepts safe counters only; reports require administrator',async t=>{
  const f=await apiFixture(t,false);const r=await fetch(f.base+'/api/coach/usage-events',{method:'POST',headers,body});assert.equal(r.status,204);assert.equal(f.saved.length,1);const report=await fetch(f.base+'/api/control/tec/usage');assert.equal(report.status,403);
});
test('Torre report is explicitly separate from Google and clamps invalid periods',async t=>{
  const f=await apiFixture(t);const r=await fetch(f.base+'/api/control/tec/usage?days=999');const data=await r.json();assert.equal(data.source,'tec_aggregate_counters');assert.equal(data.days,30);assert.deepEqual(data.events,[{name:'route_open',count:4}]);assert.equal(r.headers.get('cache-control'),'private, no-store');
});
test('PWA caches optional code but does not force activation or cache event posts',()=>{
  const worker=readFileSync(new URL('../public/coach/sw.js',import.meta.url),'utf8');assert.match(worker,/analytics-events\.js\?v=20261104-analytics1/);assert.doesNotMatch(worker,/self\.skipWaiting\(/);assert.match(worker,/if \(request.method !== "GET"\) \{\s*return;/);
});

test('recording observer preserves consent UI, pauses a loaded tag and re-enables it afterwards',async()=>{
 const f=browser({observer:true});assert.ok(f.elements.has('[data-analytics-accept]'));f.accept();f.scripts[0].onload();
 assert.deepEqual(Array.from(f.observed().attributeFilter),['data-capture-busy']);f.busy(true);assert.equal(f.context.window['ga-disable-G-DZCX3RMST7'],true);await f.flush();assert.equal(f.requests.length,0);
 f.busy(false);assert.equal(f.context.window['ga-disable-G-DZCX3RMST7'],false);f.click('[data-property-walk]');await f.flush();assert.equal(f.requests.length,1);
});
test('unavailable analytics network is dropped without retrying or an unhandled rejection',async()=>{
 const f=browser({choice:'accepted'});let calls=0;f.context.fetch=async()=>{calls++;throw Error('offline');};await f.flush();await f.flush();assert.equal(calls,1);assert.equal(f.writes.length,0);
});
