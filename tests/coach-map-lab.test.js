import test from 'node:test';
import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import {freshLabFix,labNavigationFix,createLabNavigator,labDistance,labTime} from '../public/coach/property-lab-guidance.js';
import {createPropertyRouteStore,completeRouteStop,nextRouteStop} from '../public/coach/property-route-state.js';
import {propertyVisitKey} from '../public/coach/property-visit-helpers.js';
const time=Date.parse('2026-10-29T12:00:00Z');
const from={lat:41.87,lon:-87.63},to={lat:41.88,lon:-87.63};
const plan={coordinates:[[-87.63,41.87],[-87.63,41.88]],distance:1112,duration:800,steps:[{along:0,text:'Comienza el recorrido.'},{along:700,text:'Gira a la derecha por TEST ST.'},{along:1112,text:'Llegas al destino.'}]};
const point=(lat=41.872)=>({lat,lon:-87.63,accuracy:8,timestamp:time});
const house=i=>({pin:String(i).padStart(14,'0'),address:`${i} TEST ST`,city:'CHICAGO',state:'IL',zipCode:'60601',formattedAddress:`${i} TEST ST, CHICAGO, IL`,lat:41.88+i/100000,lon:-87.63,ownerNames:[`PROPIETARIO ${i}`,`COPROPIETARIO ${i}`],ownerChecked:true});
const route=()=>({anchor:house(0),stops:Array.from({length:50},(_,i)=>({property:house(i+1),completedAt:i<3?'2026-10-28T12:00:00.000Z':null})),travelMode:'walking'});
function fixture(requestImpl=async()=>structuredClone(plan)){
  let current=route(),clock=time,handler,errorHandler,watchCount=0,clearCount=0,speaks=[],calls=[],capture=false,visible=true;
  const geo={watchPosition(onFix,onError){handler=onFix;errorHandler=onError;return ++watchCount;},clearWatch(){clearCount++;}};
  const nav=createLabNavigator({getRoute:()=>current,request:async(path,options)=>{calls.push({path,options});return requestImpl(path,options);},geolocation:geo,now:()=>clock,onSpeak:v=>speaks.push(v),captureBusy:()=>capture,canSpeak:()=>visible});
  return {nav,calls,speaks,get route(){return current;},set route(v){current=v;},setClock:v=>clock=v,setCapture:v=>capture=v,setVisible:v=>visible=v,get watches(){return watchCount;},get cleared(){return clearCount;},fix:(p=point())=>handler({coords:{latitude:p.lat,longitude:p.lon,accuracy:p.accuracy,heading:p.heading,speed:p.speed},timestamp:p.timestamp}),error:v=>errorHandler(v),oldFix:()=>handler};
}
const settle=()=>new Promise(resolve=>setImmediate(resolve));
test('LAB rejects missing, stale, future, inaccurate and non-finite GPS fixes',()=>{
  assert.ok(freshLabFix(point(),time));
  for(const bad of [{...point(),timestamp:time-15001},{...point(),timestamp:time+2001},{...point(),accuracy:NaN},{...point(),accuracy:41},{...point(),lat:NaN},{...point(),timestamp:undefined}])assert.equal(freshLabFix(bad,time),false);
});
test('LAB distinguishes next-turn distance, total remaining and estimated time; snaps precise fixes',()=>{
  const fix=labNavigationFix(point(),plan,to,time);assert.equal(fix.phase,'guiding');assert.equal(fix.arrow,'↱');assert.ok(fix.nextMeters>470&&fix.nextMeters<490);assert.ok(fix.remaining>880);assert.ok(fix.seconds>600);assert.ok(Number.isFinite(fix.position.bearing));assert.equal(fix.completedAt,undefined);
  assert.equal(labDistance(1540),'1.5 km');assert.equal(labDistance(26),'25 m');assert.equal(labTime(121),'3 min');
});
test('LAB arrival and off-route detection never mark a house visited',()=>{
  assert.equal(labNavigationFix({...point(),...to},plan,to,time).phase,'arrived');
  assert.equal(labNavigationFix({...point(),lon:-87.64},plan,to,time).phase,'offroute');
  assert.equal(labNavigationFix({...point(),accuracy:90},plan,to,time).phase,'waiting');
});
test('opening controller does not start GPS or write routes; start sends only coordinates and mode',async()=>{
  const f=fixture();assert.equal(f.watches,0);f.nav.update();assert.equal(f.watches,0);f.nav.start();assert.equal(f.watches,1);f.fix();await settle();
  assert.equal(f.calls.length,1);assert.equal(f.calls[0].path,'/api/coach/property-navigation');assert.deepEqual(Object.keys(f.calls[0].options.body).sort(),['from','mode','to']);assert.doesNotMatch(JSON.stringify(f.calls),/PROPIETARIO|TEST ST|completedAt/);assert.equal(f.route.stops.length,50);assert.equal(f.route.stops.filter(s=>s.completedAt).length,3);
});
test('late route result after stop cannot restart guidance or speech',async()=>{
  let resolve;const f=fixture(()=>new Promise(r=>resolve=r));f.nav.start();f.fix();f.nav.stop();resolve(plan);await settle();assert.equal(f.nav.get().active,false);assert.equal(f.nav.get().plan,null);assert.equal(f.speaks.length,0);assert.equal(f.cleared,1);
});
test('switching houses rejects old HTTP and old GPS callbacks',async()=>{
  const responses=[];const f=fixture(()=>new Promise(r=>responses.push(r)));f.nav.start();f.fix();const old=f.oldFix();
  f.route=completeRouteStop(f.route,propertyVisitKey(nextRouteStop(f.route).property),'2026-10-29T12:00:00Z');f.nav.update();
  assert.equal(f.watches,2);old({coords:{latitude:0,longitude:0,accuracy:5},timestamp:time});assert.notEqual(f.nav.get().position.lat,0);
  responses[0](plan);await settle();assert.equal(f.nav.get().plan,null);responses[1](plan);await settle();assert.ok(f.nav.get().plan);assert.equal(f.route.stops.filter(s=>s.completedAt).length,4);
});
test('off-route recalculation requires repeated precise fixes and respects cooldown',async()=>{
  const f=fixture();f.nav.start();f.fix();await settle();f.setClock(time+16000);const off={...point(),lon:-87.64,timestamp:time+16000};f.fix(off);assert.equal(f.calls.length,1);f.fix(off);await settle();assert.equal(f.calls.length,2);f.fix(off);f.fix(off);await settle();assert.equal(f.calls.length,2);
});
test('bad route response and service errors remain retryable without discarding 50 houses',async()=>{
  let count=0;const f=fixture(async()=>{if(++count===1)return {};return plan;});f.nav.start();f.fix();await settle();assert.equal(f.nav.get().phase,'error');assert.equal(f.route.stops.length,50);f.setClock(time+11000);f.fix({...point(),timestamp:time+11000});await settle();assert.equal(f.nav.get().phase,'guiding');
});
test('no repeat speech for duplicate fixes; muted/hidden state does not advance speech cue',async()=>{
  const f=fixture();f.nav.start();f.setVisible(false);f.fix();await settle();assert.equal(f.speaks.length,0);f.setVisible(true);f.fix();assert.equal(f.speaks.length,1);f.fix();assert.equal(f.speaks.length,1);
});
test('recording conflict and denied GPS stop watches without changing the route',async()=>{
  const f=fixture();f.setCapture(true);f.nav.start();assert.equal(f.watches,0);f.setCapture(false);f.nav.start();f.error({code:1});assert.equal(f.nav.get().active,false);assert.equal(f.cleared,1);
  f.nav.start();f.setCapture(true);f.nav.tick();assert.equal(f.nav.get().active,false);assert.equal(f.cleared,2);assert.equal(f.route.stops.filter(s=>s.completedAt).length,3);
});
test('stale GPS watchdog changes guidance to waiting without inventing movement',async()=>{
  const f=fixture();f.nav.start();f.fix();await settle();const position=f.nav.get().position;f.setClock(time+20000);f.nav.tick();assert.equal(f.nav.get().phase,'waiting');assert.deepEqual(f.nav.get().position,position);
});
test('shared offline store reopens all 50 owners and exact progress after a LAB completion',async()=>{
  const disk=new Map(),queue=[];let id=0;
  const offline={read:async key=>structuredClone(disk.get(key)),write:async(key,value)=>disk.set(key,structuredClone(value)),pending:async()=>structuredClone(queue),flush:async()=>{}};
  const request=async(_path,options)=>{const requestId=`lab-request-${++id}`;queue.push({id:requestId,kind:options.offlineKind,ownerEmail:'lab@example.test',body:structuredClone(options.body)});return {requestId};};
  const make=()=>createPropertyRouteStore({accountId:'lab',email:'lab@example.test',request,offline,online:()=>false,now:()=>time});
  let store=make();await store.ready;await store.change(()=>route());
  await store.change(value=>completeRouteStop(value,propertyVisitKey(nextRouteStop(value).property),'2026-10-29T12:00:00.000Z'));
  store=make();await store.ready;assert.equal(store.get().stops.length,50);assert.equal(store.get().stops.filter(s=>s.completedAt).length,4);assert.equal(nextRouteStop(store.get()).property.address,'5 TEST ST');assert.deepEqual(store.get().stops[49].property.ownerNames,['PROPIETARIO 50','COPROPIETARIO 50']);
});
test('LAB is opt-in and lazy, uses existing callbacks, and does not force service worker takeover',()=>{
  const read=p=>readFileSync(new URL(p,import.meta.url),'utf8'),html=read('../private/coach-app.html'),map=read('../public/coach/property-map.js'),worker=read('../public/coach/sw.js'),lab=read('../public/coach/property-lab.js');
  assert.match(html,/data-property-walk/);assert.match(html,/data-property-lab/);assert.match(map,/await import\('\.\/property-lab/);assert.match(map,/createPropertyLab\(\{ \.\.\.routeTools/);assert.doesNotMatch(lab,/localStorage|indexedDB|TecNativeRoute|property-route-v/);assert.doesNotMatch(worker,/self\.skipWaiting\(/);assert.doesNotMatch(worker,/"\/coach\/vendor\/maplibre/);
});
