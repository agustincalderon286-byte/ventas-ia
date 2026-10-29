import test from 'node:test';
import assert from 'node:assert/strict';
import { createPropertyRouteService, validatePropertyRoute, registerCoachPropertyRoute } from '../src/coach-property-route.js';
import { emptyPropertyRoute, toggleRouteStop, completeRouteStop, moveRouteStop, nextRouteStop, orderRouteByProximity, createPropertyRouteStore, latestPropertyRoute } from '../public/coach/property-route-state.js';
import { propertyVisitKey } from '../public/coach/property-visit-helpers.js';
const now = () => Date.parse('2026-10-28T12:00:00Z');
const house = i => ({ pin: String(i + 1).padStart(14, '0'), address: `${100 + i} TEST ST`, city: 'HILLSIDE', state:'IL', zipCode:'60162', lat:41.87 + i * .0001, lon:-87.9 });
const route = (count = 3) => ({ ...emptyPropertyRoute(house(0)), stops:Array.from({length:count}, (_,i)=>({property:house(i + 1), completedAt:null})), clientUpdatedAt:'2026-10-28T11:00:00.000Z' });
function memoryModel() {
 const rows = new Map();
 const matches = (row, filter) => row && (row.clientUpdatedAt < filter.$or[0].clientUpdatedAt.$lt || (row.clientUpdatedAt === filter.$or[1].clientUpdatedAt && row.requestId < filter.$or[1].requestId.$lt));
 return { rows, init:async()=>{}, findOne:({ownerUserId})=>({lean:async()=>structuredClone(rows.get(ownerUserId)||null)}), findOneAndUpdate:(filter,update,options)=>({lean:async()=>{
  const row=rows.get(filter.ownerUserId);
  if (matches(row,filter) || (!row && options.upsert)) { rows.set(filter.ownerUserId,structuredClone(update.$set)); return structuredClone(update.$set); }
  if(options.upsert) throw Object.assign(new Error('duplicate'),{code:11000});
  return null;
 }}) };
}
function fixture() { const model=memoryModel(); return {model,service:createPropertyRouteService({model,now,toOwnerId:String})}; }
test('up to 50 unique houses; remove, undo, progress and clear do not retain stale destinations',()=>{
 let r=emptyPropertyRoute(house(0));
 for(let i=1;i<=50;i++) r=toggleRouteStop(r,house(i),house(0));
 assert.equal(r.stops.length,50); assert.throws(()=>toggleRouteStop(r,house(51)),/50/);
 r=completeRouteStop(r,propertyVisitKey(house(1)),new Date(now()).toISOString());
 assert.equal(nextRouteStop(r).property.address,house(2).address);
 r=completeRouteStop(r,propertyVisitKey(house(1)),null); assert.equal(nextRouteStop(r).property.address,house(1).address);
 r=toggleRouteStop(r,{...house(1),pin:'99999999999999'}); assert.equal(r.stops.length,49);
 assert.equal(nextRouteStop(emptyPropertyRoute()),null);
});
test('proximity uses coordinates, keeps completed progress and supports manual changes',()=>{
 let r=route(); r.stops=[{property:house(30),completedAt:null},{property:house(2),completedAt:null},{property:house(1),completedAt:null}];
 r=orderRouteByProximity(r); assert.deepEqual(r.stops.map(s=>s.property.address),[house(1),house(2),house(30)].map(p=>p.address));
 r=completeRouteStop(r,propertyVisitKey(house(1)),new Date(now()).toISOString());
 r=moveRouteStop(r,propertyVisitKey(house(30)),-1); assert.equal(nextRouteStop(r).property.address,house(30).address);
 r=orderRouteByProximity(r); assert.equal(r.stops[0].completedAt,new Date(now()).toISOString()); assert.equal(nextRouteStop(r).property.address,house(2).address);
});
test('server validates limit, geography, duplicates, clock, mode, and strips extra personal fields',()=>{
 assert.equal(validatePropertyRoute(route(50),now).stops.length,50);
 assert.throws(()=>validatePropertyRoute(route(51),now),{status:400});
 const r=route(); r.stops[1].property={...r.stops[0].property,pin:'99999999999999'};
 assert.throws(()=>validatePropertyRoute(r,now),{status:400});
 assert.throws(()=>validatePropertyRoute({...route(),travelMode:'flying'},now),{status:400});
 assert.throws(()=>validatePropertyRoute({...route(),clientUpdatedAt:'2040-01-01'},now),{status:400});
 assert.throws(()=>validatePropertyRoute({...route(),anchor:null},now),{status:400});
 const unsafe=route(); unsafe.stops[0].property.lat=null; assert.throws(()=>validatePropertyRoute(unsafe,now),{status:400});
 const extra=route(); extra.stops[0].property.ownerName='Not retained'; assert.equal(validatePropertyRoute(extra,now).stops[0].property.ownerName,undefined);
});
test('server keeps one private route per account and old retries cannot erase newer progress or cleared routes',async()=>{
 const {model,service}=fixture(); const old=route();
 await service.save('alice',old,'route-request-001');
 assert.equal(await service.get('bob'),null);
 const recent={...completeRouteStop(old,propertyVisitKey(house(1)),new Date(now()).toISOString()),clientUpdatedAt:'2026-10-28T12:00:00.000Z'};
 await service.save('alice',recent,'route-request-002');
 assert.equal((await service.save('alice',old,'route-request-001')).stops[0].completedAt,recent.stops[0].completedAt);
 await service.save('bob',old,'route-request-001'); assert.equal(model.rows.size,2);
 const cleared={...emptyPropertyRoute(),clientUpdatedAt:'2026-10-28T12:00:01.000Z'};
 await service.save('alice',cleared,'route-request-003');
 assert.equal((await service.save('alice',recent,'route-request-002')).stops.length,0);
});
test('a newer first save survives an initial concurrent upsert collision',async()=>{
 const {model,service}=fixture(); const update=model.findOneAndUpdate;
 let collide=true;
 model.findOneAndUpdate=(filter,body,options)=>{
  if(collide){ collide=false; model.rows.set('alice',{...route(),ownerUserId:'alice',requestId:'older-route'}); return {lean:async()=>{throw Object.assign(new Error('race'),{code:11000});}};}
  return update(filter,body,options);
 };
 const result=await service.save('alice',{...route(4),clientUpdatedAt:'2026-10-28T12:00:00.000Z'},'newer-route');
 assert.equal(result.stops.length,4);
});
test('route endpoints require active authentication, verify owner, and use no-store',async()=>{
 const handlers=new Map();let auth=false,writes=0;
 registerCoachPropertyRoute({get:(p,f)=>handlers.set('GET',f),post:(p,f)=>handlers.set('POST',f)}, {requireCoachActivo:async(_req,res)=>auth?{user:{_id:'alice'}}:(res.status(401).json({error:'login'}),null),service:{save:async()=>{writes++;return route()},get:async()=>route()}});
 async function call(method,accountId){const res={code:200,set(k,v){this[k]=v;return this},status(c){this.code=c;return this},json(v){this.body=v;return this}};await handlers.get(method)({method,body:{accountId,route:route()},headers:{'x-tec-request-id':'route-request-001'}},res);return res;}
 assert.equal((await call('POST','alice')).code,401); auth=true;
 assert.equal((await call('POST','bob')).code,403); assert.equal(writes,0);
 assert.equal((await call('POST','alice')).code,200); assert.equal(writes,1);
 assert.equal((await call('GET')).body.route.stops.length,3); assert.equal((await call('GET'))['Cache-Control'],'private, no-store');
});
function localFixture(){
 const {service}=fixture(), disk=new Map(), queue=[];let connected=false,failQueue=false,failCache=false,seq=0;
 const offline={read:async key=>structuredClone(disk.get(key)||[]),write:async(key,value)=>{if(failCache)throw Error('quota');disk.set(key,structuredClone(value))},pending:async()=>structuredClone(queue),flush:async()=>{if(!connected)return;while(queue.length){const item=queue[0];await service.save(item.body.accountId,item.body.route,item.id);queue.shift()}}};
 function store(accountId='alice',email='alice@example.test'){
  const request=async(url,options={})=>{if(options.queueFirst){if(failQueue)throw Error('quota');const id=`route-request-${String(++seq).padStart(5,'0')}`;queue.push({id,kind:options.offlineKind,ownerEmail:email,body:structuredClone(options.body)});return{requestId:id,offlineQueued:true}};if(!connected)throw Error('offline');return{route:await service.get(accountId)}};
  return createPropertyRouteStore({accountId,email,request,offline,online:()=>connected,now});
 }
 return{disk,queue,service,store,connect:()=>{connected=true},failQueue:()=>{failQueue=true},failCache:()=>{failCache=true}};
}
test('offline 50-stop progress survives reopening, syncs once connected, and restores on another device',async()=>{
 const f=localFixture();let s=f.store();await s.ready;
 for(let i=1;i<=50;i++)await s.change(r=>toggleRouteStop(r,house(i),house(0)));
 await s.change(r=>completeRouteStop(r,propertyVisitKey(house(1)),new Date(now()).toISOString()));
 s=f.store();await s.ready;assert.equal(s.get().stops.length,50);assert.equal(nextRouteStop(s.get()).property.address,house(2).address);assert.equal(s.get().pending,true);
 const other=f.store('bob','bob@example.test');await other.ready;assert.equal(other.get().stops.length,0);
 f.connect();await s.sync();assert.equal(f.queue.length,0);assert.equal(s.get().pending,false);
 f.disk.clear();s=f.store();await s.ready;await s.refresh();assert.equal(s.get().stops.length,50);assert.ok(s.get().stops[0].completedAt);
});
test('failed durable save does not change visible progress; failed cache recovers from committed queue',async()=>{
 const f=localFixture();let s=f.store();await s.ready;f.failCache();await s.change(r=>toggleRouteStop(r,house(1),house(0)));
 assert.equal(s.get().stops.length,1);s=f.store();await s.ready;assert.equal(s.get().stops.length,1);
 f.failQueue();await assert.rejects(s.change(r=>completeRouteStop(r,propertyVisitKey(house(1)),new Date(now()).toISOString())),/quota/);
 assert.equal(s.get().stops[0].completedAt,null);
});
test('pending snapshots are account scoped and late remote responses never reset local progress',()=>{
 const current={...route(4),clientUpdatedAt:'2026-10-28T12:00:00.000Z',requestId:'latest-route',pending:true};
 const foreign={kind:'property-route',id:'foreign-route',ownerEmail:'bob@example.test',body:{accountId:'bob',route:{...route(10),clientUpdatedAt:'2026-10-28T12:00:01.000Z'}}};
 assert.equal(latestPropertyRoute(current,route(),[foreign],'alice','alice@example.test').stops.length,4);
 assert.equal(latestPropertyRoute(current,{...current,pending:undefined},[],'alice','alice@example.test').pending,false);
});

test('removing the final house releases the old starting point before searching another zone',()=>{
 let r=toggleRouteStop(emptyPropertyRoute(),house(1),house(0));
 r=toggleRouteStop(r,house(1)); assert.equal(r.stops.length,0); assert.equal(r.anchor,null);
 r=toggleRouteStop(r,house(40),house(30)); assert.equal(r.anchor.address,house(30).address);
 const legacyEmpty=emptyPropertyRoute(house(0));
 assert.equal(toggleRouteStop(legacyEmpty,house(40),house(30)).anchor.address,house(30).address);
});
