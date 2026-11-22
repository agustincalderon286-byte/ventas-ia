import test from 'node:test';
import assert from 'node:assert/strict';
import {createCoachRouteAgent} from '../src/coach-route-agent.js';
import {createTrafficRoutes} from '../src/coach-route-google.js';
import {createRouteAllowance} from '../src/coach-route-allowance.js';
import {calendarGoogleClient} from '../src/coach-calendar-google.js';

const now=Date.parse('2031-06-12T16:00:00Z'); // 11:00 Chicago
const event={id:'demo',title:'Demo de cocina',location:'100 Demo Avenue, Cicero, IL',start:'2031-06-12T22:00:00Z'};
const sheetAddress='200 Pine St', sheetCity='Oak Park, IL';
const key=Buffer.alloc(32,1);
function fixture({enabled=true,blocks=[],events=[event],rows,traffic,owner='agustin',extra={}}={}) {
  let auth={user:{_id:owner,email:owner+'@example.invalid',name:owner},session:{accessMode:'direct',tokenHash:owner+'-token'}};
  const calls=[], routes={enabled,async minutes(from,to,at){calls.push({from,to,at});return traffic?traffic(from,to,at):15;}};
  const sheets={async access(user){assert.equal(user,owner);return {token:'sheet-'+owner,grant:'sheet-grant'};},async current(user,grant){return user===owner && grant==='sheet-grant';}};
  const sheetsGoogle={async readFollowups(token,book,options){assert.equal(token,'sheet-'+owner);assert.equal(options.forRoute,true);return {rows:rows || []};}};
  const calendar={async getConnection(user){assert.equal(user,owner);return {grant:owner+'-grant',calendarId:owner+'-calendar',availabilityEnabled:true};},
    async availability({owner:user,grant,start,end}){assert.equal(user,owner);assert.equal(grant,owner+'-grant');assert.ok(Date.parse(start)<Date.parse(end));return blocks;},
    async routeAppointments({owner:user,grant}){assert.equal(user,owner);assert.equal(grant,owner+'-grant');return events;}};
  const route=createCoachRouteAgent({config:{key,planPilot:['agustin@example.invalid','rigoberto@example.invalid'],sheetsPilot:['*']},
    calendar,sheets,sheetsGoogle,readSession:async()=>auth,routes,now:()=>now,switched:req=>req.switched,...extra});
  const send=(text,pending,options={})=>route({req:{body:{coachTimeZone:'America/Chicago',accountId:owner,coachRoutePending:pending,
    coachQuestionFocus:pending?'route':'',...options.body},switched:options.switched},auth,sessionId:options.sessionId || 'chat1',text});
  return {send,calls,expire(){auth={};}};
}
test('la pregunta real enlaza la demo y la dirección de la hoja del vendedor y calcula ambos tramos con la visita',async()=>{
 for(const owner of ['agustin','rigoberto']){
  const f=fixture({owner,rows:[{fullName:'Eva López',seller:owner,address:sheetAddress,city:sheetCity},
    {fullName:'Rosa López',seller:'otro vendedor',address:'900 Private Ave',city:'otro lugar'}]});
  let r=await f.send('¿Me da chance de pasar con la señora López antes de la de las cinco?');
  assert.match(r.respuesta,/dirección vas a salir/);
  r=await f.send('Salgo de 300 First Avenue, Chicago, IL a las 3 pm',r.coachRoutePending);
  assert.match(r.respuesta,/Cuántos minutos/);
  r=await f.send('30 minutos',r.coachRoutePending);
  assert.match(r.respuesta,/Sí cabe la visita/);assert.match(r.respuesta,/3:00/);assert.match(r.respuesta,/4:00/);
  assert.match(r.respuesta,/dirección de tu hoja/);assert.match(r.respuesta,/tráfico de Google Maps/);
  assert.equal(f.calls.length,2);assert.equal(f.calls[0].to,sheetAddress+', '+sheetCity);
  assert.equal(f.calls[1].at-f.calls[0].at,45*60000);
  const map=new URL(r.coachHelpActions[0].url);assert.equal(map.searchParams.get('waypoints'),sheetAddress+', '+sheetCity);
  assert.ok(!r.respuesta.includes('Private'));assert.equal(r.coachRoutePending,'');
 }
});
test('mantiene proyectos y borradores ajenos a la ruta; el contexto está vinculado a vendedor y conversación',async()=>{
 const a=fixture(),b=fixture({owner:'rigoberto'});
 const r=await a.send('¿A qué hora salgo para llegar a mi demo?');
 assert.ok(r.coachRoutePending);
 assert.match((await b.send('300 Pine Ave, Chicago',r.coachRoutePending)).respuesta,/venció/);
 assert.match((await a.send('300 Pine Ave, Chicago',r.coachRoutePending,{sessionId:'otro'})).respuesta,/venció/);
 assert.match((await a.send('Calcula la ruta a mi demo',null,{switched:true})).respuesta,/propia cuenta/);
 assert.match((await a.send('Calcula la ruta a mi demo',null,{body:{accountId:'otro'}})).respuesta,/propia cuenta/);
 assert.equal(await a.send('Revisa mis correos',r.coachRoutePending),null);
 assert.equal(await a.send('Prepara un correo que diga a qué hora salgo para mi demo'),null);
});
test('sin clave funciona el enlace y el cálculo con minutos indicados, sin atribuirlos a tráfico verificado',async()=>{
 const f=fixture({enabled:false});
 let r=await f.send('¿Me da tiempo de visitar a un cliente antes de mi demo? Origen: 200 First St, Chicago; parada: 250 Main St, Berwyn; durante 30 minutos.');
 assert.match(r.respuesta,/todavía no está activo/);assert.ok(r.coachRoutePending);assert.equal(r.coachHelpActions[0].type,'routeMap');
 r=await f.send('12 y 18 minutos',r.coachRoutePending);
 assert.match(r.respuesta,/Sí cabe/);assert.match(r.respuesta,/minutos que me indicaste/);
 assert.doesNotMatch(r.respuesta,/con tráfico de Google/);assert.equal(f.calls.length,0);
});
test('un bloqueo entre la salida fija y la demo evita prometer una llegada o consultar Maps',async()=>{
 const f=fixture({blocks:[{start:'2031-06-12T20:30:00Z',end:'2031-06-12T21:00:00Z'}]});
 const r=await f.send('¿Llego a mi demo si salgo de 30 Main St, Cicero a las 3 pm? Calcula la ruta.');
 assert.match(r.respuesta,/otro compromiso/);assert.equal(f.calls.length,0);
});
test('tiempos que exceden la ventana, ubicaciones ausentes y varias demos requieren una respuesta honesta',async()=>{
 const f=fixture({traffic:()=>95});
 const r=await f.send('¿Me da chance de pasar antes de la demo? Salgo de 100 Oak St, Berwyn a las 3 pm; parada: 300 Rose St, Cicero; durante 15 minutos.');
 assert.match(r.respuesta,/no cabe/);
 const p=fixture({events:[]});let primary=await p.send('Calcula la ruta para mi demo a las 5 pm. Salgo de 110 Lake St, Chicago.');
 assert.match(primary.respuesta,/dirección de la demo/);
 primary=await p.send('Joliet',primary.coachRoutePending);assert.match(primary.respuesta,/dirección de la demo/);
 const two=fixture({events:[event,{...event,id:'otra',title:'Visita',start:'2031-06-12T20:00:00Z'}]});
 let list=await two.send('Calcula la ruta para mi demo. Origen: 110 Lake St, Chicago.');
 assert.match(list.respuesta,/cuál cita/);list=await two.send('segunda',list.coachRoutePending);assert.match(list.respuesta,/Visita/);
});
test('solicita solo ubicaciones, hora y duración al proveedor con despliegue limitado a Routes',async()=>{
 const calls=[];
 const config={mapsRoutesKey:'test-server-key'}, allowance={configured:()=>true,reserve:async()=>true};
 const client=createTrafficRoutes(config,{allowance,now:()=>now,fetcher:async(url,init)=>{
  calls.push({url,init});return new Response(JSON.stringify({routes:[{duration:'601.5s'}]}));
 }});
 assert.equal(await client.minutes('origen','destino',now+60000),11);
 assert.equal(calls[0].url,'https://routes.googleapis.com/directions/v2:computeRoutes');
 const body=JSON.parse(calls[0].init.body);
 assert.deepEqual(body,{origin:{address:'origen'},destination:{address:'destino'},travelMode:'DRIVE',routingPreference:'TRAFFIC_AWARE',departureTime:new Date(now+60000).toISOString()});
 assert.match(calls[0].init.headers['X-Goog-FieldMask'],/^routes\.duration,fallbackInfo,/);
 const partial=createTrafficRoutes(config,{allowance,fetcher:async()=>Response.json({routes:[{duration:'120s'}],geocodingResults:{origin:{partialMatch:true}}})});
 await assert.rejects(partial.minutes('a','b',now),e=>e.partialAddress);
 const bad=createTrafficRoutes(config,{allowance,fetcher:async()=>new Response(JSON.stringify({routes:[{duration:'120s'}],fallbackInfo:{reason:'SERVER_ERROR'}}))});
 await assert.rejects(bad.minutes('a','b',now));
});
test('las ubicaciones provienen exclusivamente de la agenda TEC vinculada y de eventos con hora',async()=>{
 const id='abcdefghijk@group.calendar.google.com',calls=[];
 const client=calendarGoogleClient({origin:'https://example.invalid'},async(url,init)=>{
  calls.push({url,init});return new Response(JSON.stringify({items:[{id:'uno',summary:'Demo',location:'1 Lake St, Chicago',start:{dateTime:event.start}},
   {id:'dos',start:{date:'2031-06-12'}},{id:'tres',status:'cancelled',start:{dateTime:event.start}}]}));
 });
 const window={start:new Date(now).toISOString(),end:event.start};
 const items=await client.routeAppointments('solo-esa-cuenta',id,window);
 assert.equal(items.length,1);assert.equal(items[0].location,'1 Lake St, Chicago');
 const query=new URL(calls[0].url);assert.equal(decodeURIComponent(query.pathname),'/calendar/v3/calendars/'+id+'/events');
 assert.equal(calls[0].init.headers.Authorization,'Bearer solo-esa-cuenta');
 assert.doesNotMatch(query.searchParams.get('fields'),/description|attendees/);
 await assert.rejects(client.routeAppointments('t','primary',window));assert.equal(calls.length,1);
});

test('solo se envían peticiones previamente reservadas para ese mes del Pacífico, incluso entre vendedores, errores y nuevas instancias',async()=>{
 const at=Date.parse('2031-07-01T06:30Z'); // still June 30 in Los Angeles
 const config={mapsRoutesKey:'restricted-test',mapsFreeMonth:'2031-06',mapsFreeRequests:'2'};
 const counts=new Map();let calls=0;
 const db={async updateOne(query,update,opts){
   assert.equal(query._id,'compute-routes-pro:2031-06');
   assert.deepEqual(update,{$inc:{requests:1}});assert.deepEqual(opts.writeConcern,{w:'majority'});
   const n=counts.get(query._id) || 0;if (n>=query.requests.$lt) throw Error('duplicate');
   counts.set(query._id,n+1);return {acknowledged:true,upsertedCount:n===0?1:0,modifiedCount:n>0?1:0};
 }};
 const client=(cfg=config,time=at,store=db)=>createTrafficRoutes(cfg,{
   now:()=>time,allowance:createRouteAllowance(cfg,{store,now:()=>time}),fetcher:async()=>{
     calls++;throw Error('lost response');
   }
 });
 for(const cfg of [{mapsRoutesKey:'restricted-test'},{...config,mapsFreeMonth:'2031-05'},{...config,mapsFreeRequests:'5001'},
   {...config,mapsFreeRequests:'0'},{...config,mapsFreeRequests:'2.5'}]) {
   assert.equal(client(cfg).enabled,false);await assert.rejects(client(cfg).minutes('a','b',at),e=>e.freeAllowance);
 }
 assert.equal(calls,0);assert.equal(client().enabled,true);
 await Promise.all(Array.from({length:8},()=>client().minutes('a','b',at).catch(()=>{})));
 assert.equal(calls,2);assert.equal(counts.values().next().value,2);
 assert.equal(client(config,Date.parse('2031-07-01T06:59:30Z')).enabled,false);
 assert.equal(client(config,Date.parse('2031-07-01T07:01:00Z')).enabled,false);
 await assert.rejects(client(config,at,{updateOne:async()=>{throw Error('database offline');}}).minutes('a','b',at),e=>e.freeAllowance);
 assert.equal(calls,2);
});

test('si empeora el tráfico de salida, ajusta la salida con una estimación nueva cerca de la demo',async()=>{
 const f=fixture({traffic:(_a,_b,ms)=> new Date(ms).getUTCHours()>=21 ? 28 : 10});
 const r=await f.send('¿A qué hora salgo para mi demo? Origen: 30 Main St, Cicero.');
 assert.match(r.respuesta,/4:31/);assert.match(r.respuesta,/4:59/);
 assert.equal(f.calls.length,3);
});

test('si la dirección en la hoja deja de pertenecerle al vendedor, la siguiente respuesta pide verificarla',async()=>{
 const rows=[{fullName:'Clara López',seller:'agustin',address:'300 Some St',city:'Cicero, IL'}],f=fixture({rows});
 let r=await f.send('¿Puedo pasar con la señora López antes de mi demo? Origen: 30 Main St, Cicero.');
 assert.match(r.respuesta,/Cuántos minutos/);
 rows[0].seller='otro';
 r=await f.send('25 minutos',r.coachRoutePending);
 assert.match(r.respuesta,/qué dirección/);
 assert.ok(!JSON.stringify(r).includes('300 Some St'));
 assert.equal(f.calls.length,0);
});
