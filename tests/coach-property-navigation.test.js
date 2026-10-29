import test from 'node:test';
import assert from 'node:assert/strict';
import {createStreetRouter,registerStreetRoutes} from '../src/coach-street-route.js';
import {createCoachPropertyService} from '../src/coach-properties.js';
import {mergeRouteOwners,routeProperty} from '../public/coach/property-route-state.js';
import {navigationFix,projectRoute} from '../public/coach/route-guidance.js';
const from={lat:41.65,lon:-87.67},to={lat:41.651,lon:-87.67};
const sample={code:'Ok',routes:[{distance:111,duration:80,geometry:{coordinates:[[-87.67,41.65],[-87.67,41.651]]},legs:[{steps:[{name:'TEST ST',maneuver:{type:'depart',location:[-87.67,41.65]}},{maneuver:{type:'arrive',location:[-87.67,41.651]}}]}]}]};
test('route provider receives coordinates only, caches and globally throttles uncached requests',async()=>{
 let calls=0;const router=createStreetRouter({fetchImpl:async(url,options)=>{calls++;assert.match(url,/routed-foot\/route/);assert.ok(options.headers['User-Agent'].includes('totalelitecoaching.com'));assert.ok(!url.includes('OWNER'));return{ok:true,json:async()=>sample};},now:()=>10000});
 const result=await router.route({from,to,mode:'walking',ownerName:'OWNER'});assert.equal(result.steps.length,2);assert.match(result.steps[1].text,/destino/);
 await router.route({from,to,mode:'walking'});assert.equal(calls,1);
 await assert.rejects(router.route({from,to:{...to,lat:41.652},mode:'walking'}),{status:429});
});
test('invalid geography, bad mode, upstream failure and empty routes fail explicitly',async()=>{
 const router=createStreetRouter({fetchImpl:async()=>({ok:true,json:async()=>({code:'NoRoute'})})});
 for(const input of [{from:{lat:null,lon:-87.67},to,mode:'walking'},{from,to,mode:'flying'},{from:{lat:10,lon:10},to,mode:'walking'}])await assert.rejects(router.route(input),{status:400});
 await assert.rejects(router.route({from,to,mode:'walking'}),{status:503});
});
test('street routes require an active signed-in account',async()=>{
 let handler,calls=0;registerStreetRoutes({post:(_p,h)=>handler=h},{requireCoachActivo:async()=>null,service:{route:()=>{calls++;}}});
 await handler({body:{from,to,mode:'walking'}},{set(){}});assert.equal(calls,0);
});
test('owner restoration asks latest year for each PIN and returns empty missing owners',async()=>{
 let calls=0;const service=createCoachPropertyService({fetchImpl:async url=>{calls++;const u=new URL(url);assert.ok(u.searchParams.get('$where').includes('00000000000001'));return{ok:true,json:async()=>calls===1?[{pin:'00000000000001',year:'2025'},{pin:'00000000000002',year:'2024'}]:[{pin:'00000000000001',owner_address_name:'PUBLIC OWNER'},{pin:'00000000000002',owner_address_name:'UNKNOWN'}]};}});
 const value=await service.owners({pins:'00000000000001,00000000000002'});assert.deepEqual(value.owners[0].ownerNames,['PUBLIC OWNER']);assert.deepEqual(value.owners[1].ownerNames,[]);
 await service.owners({pins:'00000000000001,00000000000002'});assert.equal(calls,2);
 await assert.rejects(service.owners({pins:"bad' OR 1=1"}),{status:400});
 await assert.rejects(service.owners({pins:Array.from({length:51},(_,i)=>String(i).padStart(14,'0')).join(',')}),{status:400});
});
test('owners merge preserves latest progress, notes, order and removal; survives route snapshot',()=>{
 const property={pin:'1',address:'1 TEST ST',lat:41.6,lon:-87.6},r={anchor:null,stops:[{property,completedAt:'2026-10-28T00:00:00Z'}]};
 const merged=mergeRouteOwners(r,[{pin:'1',ownerNames:['PUBLIC OWNER']},{pin:'removed',ownerNames:['REMOVED']}]);assert.equal(merged.stops.length,1);assert.equal(merged.stops[0].completedAt,r.stops[0].completedAt);assert.equal(routeProperty(merged.stops[0].property).ownerName,'PUBLIC OWNER');assert.equal(merged.stops[0].property.ownerChecked,true);
});
test('GPS rejects poor accuracy, detects off-route, follows streets and never completes visits',()=>{
 const plan={coordinates:sample.routes[0].geometry.coordinates,steps:[{along:111,text:'Llegas al destino.'}]};
 assert.equal(navigationFix({...from,accuracy:100},plan,to).state,'imprecise');
 assert.equal(navigationFix({lat:41.65,lon:-87.68,accuracy:5},plan,to).state,'offroute');
 const fix=navigationFix({lat:41.6505,lon:-87.67,accuracy:8},plan,to);assert.equal(fix.state,'guiding');assert.ok(fix.remaining>40&&fix.remaining<70);assert.equal(fix.completedAt,undefined);
 assert.equal(navigationFix({...to,accuracy:5},plan,to).state,'arrived');
 assert.ok(projectRoute({lat:41.6505,lon:-87.67},plan.coordinates).distance<1);
});
