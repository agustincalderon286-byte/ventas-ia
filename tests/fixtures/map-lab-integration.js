import '/coach/property-map.js?v=20261101-recorrido1';
const fixture={anchor:{lat:41.8781,lon:-87.6298,address:'Origen de prueba'},travelMode:'walking',clientUpdatedAt:'2026-10-29T12:00:00.000Z',requestId:'fixture-integration-001',stops:Array.from({length:50},(_,i)=>({property:{pin:String(i+1).padStart(14,'0'),address:`${i+1} DEMO ST`,formattedAddress:`${i+1} DEMO ST, CHICAGO, IL`,city:'CHICAGO',state:'IL',zipCode:'60601',lat:41.8818+Math.floor(i/10)*.0005,lon:-87.6232+(i%10)*.00045,ownerNames:[`PROPIETARIO FICTICIO ${i+1}`],ownerChecked:true},completedAt:i<3?'2026-10-28T12:00:00.000Z':null}))};
const prefix='tec-maplab-integration-only:',queue=JSON.parse(localStorage.getItem(prefix+'queue')||'[]');
const offline={viewer:async()=>({user:{id:'lab-ui',email:'integration@example.test'}}),read:async key=>JSON.parse(localStorage.getItem(prefix+key)||'null')||(key.startsWith('property-route-v1:')?structuredClone(fixture):[]),write:async(key,value)=>localStorage.setItem(prefix+key,JSON.stringify(value)),pending:async()=>structuredClone(queue),flush:async()=>{}};
const request=async(path,options)=>{
 if(options?.offlineQueue){const requestId='demo-ui-'+crypto.randomUUID();queue.push({id:requestId,kind:options.offlineKind,ownerEmail:'integration@example.test',body:structuredClone(options.body)});localStorage.setItem(prefix+'queue',JSON.stringify(queue));return{requestId};}
 if(path==='/api/coach/property-navigation'){const response=await fetch(path,{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(options.body)});const value=await response.json();if(!response.ok)throw Object.assign(Error(value.error),{status:response.status});return value;}
 if(path==='/api/coach/property-route')return{route:structuredClone(fixture)};
 if(path.includes('property-visits'))return{visits:[]};
 throw Error('La integración de prueba no permite esta consulta.');
};
const gpsTimers=new Map();let gpsId=0;Object.defineProperty(navigator,'geolocation',{configurable:true,value:{watchPosition(ok){const id=++gpsId;const send=()=>ok({coords:{latitude:41.8781,longitude:-87.6298,accuracy:7,heading:0,speed:0},timestamp:Date.now()});gpsTimers.set(id,setInterval(send,1500));setTimeout(send,50);return id;},clearWatch(id){clearInterval(gpsTimers.get(id));gpsTimers.delete(id);}}});
window.initTecPropertyMap({request,user:{id:'lab-ui',email:'integration@example.test'},offline});
