import {createPropertyLab} from '/coach/property-lab.js?v=20261029-lab1';
import {createPropertyWalk} from '/coach/property-walk.js?v=20261029-lab1';
import {createPropertyRouteStore,completeRouteStop,nextRouteStop} from '/coach/property-route-state.js?v=20261028-walk2';
import {propertyVisitKey} from '/coach/property-visit-helpers.js';
const prefix='tec-maplab-fixture-only:',seed=()=>({anchor:{lat:41.8781,lon:-87.6298,address:'Origen de prueba'},travelMode:'walking',stops:Array.from({length:50},(_,i)=>({property:{pin:String(i+1).padStart(14,'0'),address:`Casa de prueba ${i+1}`,formattedAddress:`Casa de prueba ${i+1} · Chicago`,city:'CHICAGO',state:'IL',zipCode:'60601',lat:41.8818+Math.floor(i/10)*.0005,lon:-87.6232+(i%10)*.00045,ownerNames:[`PROPIETARIO DE PRUEBA ${i+1}`],ownerChecked:true},completedAt:i<3?'2026-10-28T12:00:00.000Z':null}))});
const offline={read:async key=>JSON.parse(localStorage.getItem(prefix+key)||'null'),write:async(key,value)=>localStorage.setItem(prefix+key,JSON.stringify(value)),pending:async()=>JSON.parse(localStorage.getItem(prefix+'queue')||'[]'),flush:async()=>{}};
let lab,walk,store,location={lat:41.8781,lon:-87.6298},geometry=[],step=0,watchId=0,timers=new Map();
const request=async(path,options)=>{
 if(path==='/api/coach/property-route'){
  const requestId='lab-demo-'+crypto.randomUUID(),queue=await offline.pending();queue.push({id:requestId,kind:'property-route',ownerEmail:'demo@example.test',body:options.body});localStorage.setItem(prefix+'queue',JSON.stringify(queue.slice(-5)));return{requestId};
 }
 const response=await fetch('/api/coach/property-navigation',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(options.body)}),plan=await response.json();
 if(!response.ok)throw Error(plan.error);geometry=plan.coordinates;step=0;return plan;
};
const geolocation={watchPosition(ok){
 const id=++watchId,send=()=>{if(geometry.length){const p=geometry[Math.min(step,geometry.length-1)];location={lat:p[1],lon:p[0]};step=Math.min(geometry.length-1,step+1);}ok({coords:{latitude:location.lat,longitude:location.lon,accuracy:7,heading:null,speed:.8},timestamp:Date.now()});};
 setTimeout(()=>{if(timers.has(id))send();},50);timers.set(id,setInterval(send,1300));return id;
},clearWatch(id){clearInterval(timers.get(id));timers.delete(id);}};
function update(){if(!store)return;const r=store.get(),done=r.stops.filter(s=>s.completedAt).length;document.querySelector('#result').textContent=`${r.stops.length} casas · ${done} visitadas · siguiente: ${nextRouteStop(r)?.property.address||'ninguna'}`;lab?.update();walk?.update();}
store=createPropertyRouteStore({accountId:'lab-demo',email:'demo@example.test',request,offline,online:()=>false,onChange:update});
await store.ready;if(!store.get().stops.length)await store.change(seed);
const tools={getRoute:()=>store.get(),request,complete:async stop=>{await store.change(r=>completeRouteStop(r,propertyVisitKey(stop.property),new Date().toISOString()));return true;},followup:async(property,status)=>{
 if(!status){document.querySelector('#status').textContent='Las notas reales se abren en la ficha existente de TEC. Esta demo solo prueba el mapa y el avance.';return true;}
 await store.change(r=>completeRouteStop(r,propertyVisitKey(property),new Date().toISOString()));return true;
}};
function loadLeaflet(){if(window.L)return Promise.resolve(window.L);return new Promise((resolve,reject)=>{const css=document.createElement('link');css.rel='stylesheet';css.href='/coach/vendor/leaflet/leaflet.css';document.head.append(css);const script=document.createElement('script');script.src='/coach/vendor/leaflet/leaflet.js';script.onload=()=>resolve(window.L);script.onerror=reject;document.head.append(script);});}
walk=createPropertyWalk({...tools,loadLeaflet});lab=createPropertyLab({...tools,geolocation,demo:true,onFallback:()=>walk.open()});
document.querySelector('#lab').addEventListener('click',async()=>{await walk.close();lab.open();});document.querySelector('#old').addEventListener('click',()=>{lab.close();walk.open();});
document.querySelector('#reset').addEventListener('click',async()=>{lab.close();await walk.close();await store.change(seed);geometry=[];location={lat:41.8781,lon:-87.6298};update();});update();
