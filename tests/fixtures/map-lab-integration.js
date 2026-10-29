import '/coach/property-map.js?v=20261029-lab1';
const fixture={anchor:{lat:41.8781,lon:-87.6298,address:'Origen de prueba'},travelMode:'walking',clientUpdatedAt:'2026-10-29T12:00:00.000Z',requestId:'fixture-integration-001',stops:Array.from({length:50},(_,i)=>({property:{pin:String(i+1).padStart(14,'0'),address:`${i+1} DEMO ST`,formattedAddress:`${i+1} DEMO ST, CHICAGO, IL`,city:'CHICAGO',state:'IL',zipCode:'60601',lat:41.8818+Math.floor(i/10)*.0005,lon:-87.6232+(i%10)*.00045,ownerNames:[`PROPIETARIO FICTICIO ${i+1}`],ownerChecked:true},completedAt:i<3?'2026-10-28T12:00:00.000Z':null}))};
const prefix='tec-maplab-integration-only:',queue=JSON.parse(localStorage.getItem(prefix+'queue')||'[]');
const offline={read:async key=>JSON.parse(localStorage.getItem(prefix+key)||'null')||(key.startsWith('property-route-v1:')?structuredClone(fixture):[]),write:async(key,value)=>localStorage.setItem(prefix+key,JSON.stringify(value)),pending:async()=>structuredClone(queue),flush:async()=>{}};
const request=async(path,options)=>{
 if(options?.offlineQueue){const requestId='demo-ui-'+crypto.randomUUID();queue.push({id:requestId,kind:options.offlineKind,ownerEmail:'integration@example.test',body:structuredClone(options.body)});localStorage.setItem(prefix+'queue',JSON.stringify(queue));return{requestId};}
 if(path==='/api/coach/property-route')return{route:structuredClone(fixture)};
 if(path.includes('property-visits'))return{visits:[]};
 throw Error('La integración de prueba no permite esta consulta.');
};
window.initTecPropertyMap({request,user:{id:'lab-ui',email:'integration@example.test'},offline});
