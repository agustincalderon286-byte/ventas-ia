import test from 'node:test';
import assert from 'node:assert/strict';
import {randomBytes} from 'node:crypto';
import {createCoachCalendarConnections} from '../src/coach-calendar-connections.js';
import {sealToken} from '../src/coach-hycite-gmail.js';
import {calendarGoogleClient} from '../src/coach-calendar-google.js';
import {readCoachAgenda} from '../src/coach-agenda.js';

const id='abcdefgh1234@group.calendar.google.com';
const range={start:'2026-11-23T06:00:00Z',end:'2026-11-24T06:00:00Z',zone:'America/Chicago'};
const ok=value=>({status:'fulfilled',value});
const denied={status:'rejected',reason:Object.assign(Error(),{reauthorize:true})};
const result=data=>({ok:true,status:200,json:async()=>data});
const at=Date.parse('2026-11-22T15:00:00Z');
const allDay={id:'whole',start:{date:'2026-11-21'},end:{date:'2026-11-24'},summary:'Feria regional'};
const timed={id:'timed',start:{dateTime:'2026-11-23T20:00:00-06:00'},end:{dateTime:'2026-11-23T21:00:00-06:00'},summary:'Demo',transparency:'transparent'};

function fixture(overrides={}) {
  const calls=[];
  const calendar={
    getConnection:async owner=>owner==='a' || owner==='b'?{grant:'grant-'+owner,calendarId:id}:null,
    readAgenda:async arg=>{
      calls.push(arg);
      return {events:ok([{title:'Demo · '+arg.owner,start:timed.start.dateTime,end:timed.end.dateTime,allDay:false}]),
        busy:ok([]),availabilityEnabled:true,...overrides};
    }
  };
  const ask=(text,extra={})=>readCoachAgenda({calendar,owner:'a',zone:range.zone,text,now:()=>at,current:async()=>true,...extra});
  return {calls,ask};
}

test('lee cada página del calendario TEC, conserva los eventos de día completo y expande recurrentes',async()=>{
  const queryPages=[];
  const client=calendarGoogleClient({origin:'https://test.invalid'},async (url,init)=>{
    const dest=new URL(url);
    assert.equal(init.method,'GET');assert.ok(dest.pathname.endsWith('/'+encodeURIComponent(id)+'/events'));
    const q=dest.searchParams;
    assert.equal(q.get('singleEvents'),'true');assert.equal(q.get('timeZone'),range.zone);assert.equal(q.get('timeMin'),range.start);
    assert.equal(q.get('maxAttendees'),'1');assert.match(q.get('fields'),/attendees\(self,responseStatus\)/);
    assert.doesNotMatch(q.get('fields'),/email|description|location/);
    queryPages.push(q.get('pageToken'));
    return result(q.has('pageToken')?{items:[timed,{id:'old',status:'cancelled'}]}:{items:[allDay],nextPageToken:'opaque'});
  });
  const rows=await client.agendaEvents('secret',id,range);
  assert.deepEqual(queryPages,[null,'opaque']);
  assert.deepEqual(rows,[{title:'Feria regional',start:'2026-11-21',end:'2026-11-24',allDay:true,available:false},
    {title:'Demo',start:timed.start.dateTime,end:timed.end.dateTime,allDay:false,available:true}]);
});

test('rechaza una agenda incompleta sin presentar un día vacío',async()=>{
  for (const page of [{items:[{...timed,start:{dateTime:'no hay fecha'}}]},{items:[{...allDay,end:{date:'2026-02-31'}}]},
    {nextPageToken:'more'},{items:[],nextPageToken:'repetido'}]) {
    const client=calendarGoogleClient({origin:'https://test.invalid'},async()=>result(page));
    await assert.rejects(client.agendaEvents('token',id,range),e=>[422,503].includes(e.status));
  }
  const client=calendarGoogleClient({origin:'https://test.invalid'},()=>assert.fail('Ruta inesperada'));
  await assert.rejects(client.agendaEvents('token','primary',range));
});

test('los bloques del principal se consultan sin mezclar Seguimientos TEC',async()=>{
  const mailbox='persona@example.test';
  const client=calendarGoogleClient({origin:'https://test.invalid'},async (url,init)=>{
    assert.equal(new URL(url).pathname,'/calendar/v3/freeBusy');
    assert.deepEqual(JSON.parse(init.body).items,[{id:'primary'}]);
    return result({timeMin:range.start,timeMax:range.end,calendars:{[mailbox]:{busy:[]}}});
  });
  assert.deepEqual(await client.primaryBusy('token',range,mailbox),[]);
});

test('frase del vendedor consulta dos fuentes por cuenta y usa la fecha en su zona',async()=>{
  const {calls,ask}=fixture();
  for (const owner of ['a','b']) {
    const answer=await ask('Revisa si tengo citas para mañana en mi calendario de Google.',{owner});
    assert.match(answer,new RegExp('Demo · '+owner));assert.match(answer,/Calendario principal: sin bloques/);
  }
  for(const [i,owner] of ['a','b'].entries()) {
    assert.deepEqual(calls[i],{owner,grant:'grant-'+owner,zone:range.zone,start:new Date(range.start).toISOString(),
      end:new Date(range.end).toISOString(),includeEvents:true,includePrimary:true});
  }
  await ask('¿Qué tengo mañana?',{now:Date.parse('2026-10-31T13:00:00Z')});
  const dst=calls.at(-1);assert.equal(Date.parse(dst.end)-Date.parse(dst.start),25*3600000);
});

test('limita las fuentes, maneja permisos parciales y suprime resultados de una sesión que cambió',async()=>{
  const {ask,calls}=fixture({busy:denied});
  const mixed=await ask('¿Qué tengo mañana?');
  assert.match(mixed,/Demo · a/);assert.match(mixed,/renovar el permiso para tu calendario principal/);
  assert.doesNotMatch(mixed,/sin bloques/);
  const personal=await ask('¿Qué tengo mañana en mi calendario principal?');
  assert.equal(calls.at(-1).includeEvents,false);assert.doesNotMatch(personal,/Demo · a/);
  await ask('Revisa mañana sin consultar mi calendario principal');
  assert.equal(calls.at(-1).includePrimary,false);assert.equal(calls.at(-1).includeEvents,true);
  const onlyTec=await ask('Revisa Seguimientos TEC mañana');
  assert.equal(calls.at(-1).includePrimary,false);assert.doesNotMatch(onlyTec,/bloques|renovar/);
  const noPerm=fixture({availabilityEnabled:false,busy:ok(null)});
  assert.match(await noPerm.ask('¿Qué tengo mañana?'),/activa «Horarios libres»/);
  const missing=await ask('¿Qué tengo mañana?',{owner:'sin-cuenta'});assert.match(missing,/Conecta tu Google Calendar/);
  let checks=0;assert.match(await ask('¿Qué tengo mañana?',{current:async()=>++checks===1}),/sesión cambió/);
  assert.ok(checks===2);
});

test('hora puntual, intervalo y restricciones ambiguas no se convierten en una consulta más amplia',async()=>{
  const {ask,calls}=fixture();
  assert.match(await ask('¿Estoy libre mañana a las cuatro?'),/mañana o de la tarde/);
  assert.match(await ask('¿Estoy libre mañana después de las 3?'),/horas/);
  assert.match(await ask('¿Qué tengo de lunes a martes?'),/día concreto/);
  assert.equal(calls.length,0);
  await ask('¿Estoy libre mañana a las cuatro de la tarde?');
  assert.equal(calls.at(-1).start,'2026-11-23T22:00:00.000Z');assert.equal(calls.at(-1).end,'2026-11-23T22:01:00.000Z');
  await ask('¿Qué tengo mañana de 14:00 a 18:00?');
  assert.equal(calls.at(-1).start,'2026-11-23T20:00:00.000Z');assert.equal(calls.at(-1).end,'2026-11-24T00:00:00.000Z');
});

test('el servicio comprueba la autorización vigente antes y después de las lecturas de Google',async()=>{
  const key=randomBytes(32), owner='owner-a', grant='grant-a';
  const doc={_id:owner,grant,calendarId:id,mailbox:'a@example.test',availabilityEnabled:false,
    encryptedToken:sealToken('refresh-for-a',key,'calendar-refresh:'+owner)};
  let active=true;
  const store={
    findOne:filter=>({lean:async()=>active && filter._id===owner && filter.grant===grant ? {...doc} : null}),
    exists:async filter=>active && filter._id===owner && filter.grant===grant && filter.calendarId===id &&
      (!filter.availabilityEnabled || doc.availabilityEnabled)
  };
  const calls=[];
  const google={
    refresh:async token=>{assert.equal(token,'refresh-for-a');return {access_token:'only-a'};},
    agendaEvents:async(token,calendarId)=>{
      assert.equal(token,'only-a');assert.equal(calendarId,id);calls.push('events');return [];
    },
    primaryBusy:async(token,_range,mailbox)=>{
      assert.equal(token,'only-a');assert.equal(mailbox,doc.mailbox);calls.push('primary');
      throw Object.assign(Error(),{reauthorize:true});
    },
  };
  const {service}=createCoachCalendarConnections({config:{key,calendarReady:true},store,google});
  const ask=(extra={})=>service.readAgenda({owner,grant,includeEvents:true,includePrimary:true,...range,...extra});
  assert.equal((await ask()).availabilityEnabled,false);assert.deepEqual(calls,['events']);
  await assert.rejects(ask({owner:'owner-b'}));assert.deepEqual(calls,['events']);
  doc.availabilityEnabled=true;
  const partial=await ask();assert.deepEqual(partial.events.value,[]);assert.ok(partial.busy.reason.reauthorize);
  assert.deepEqual(calls,['events','events','primary']);
  google.agendaEvents=async()=>{active=false;return ['datos del permiso antiguo'];};
  await assert.rejects(ask(),/conexión cambió/);
});

test('expresa la hora solicitada, diferencia un instante de una hora y no inventa una demo del principal',async()=>{
  const day='2026-11-23T';
  const events=[
    {title:'Feria',allDay:true,start:'2026-11-22',end:'2026-11-25'},
    {title:'Demo rechazada',start:day+'13:00:00Z',end:day+'13:30:00Z',attendance:'declined'},
    {title:'Hacer llamadas',start:day+'14:00:00Z',end:day+'15:00:00Z'},
    {title:'Demostración a confirmar',start:day+'19:00:00Z',end:day+'20:00:00Z',attendance:'needsAction'},
    {title:'Demo cocina',start:day+'21:00:00Z',end:day+'22:00:00Z'}
  ];
  const {ask,calls}=fixture({events:ok(events),busy:ok([{start:day+'15:00:00Z',end:day+'16:00:00Z'}])});
  const first=await ask('¿A qué hora es mi primera demo mañana?');
  assert.match(first,/primera demo con hora: 1:00 p.m./);assert.match(first,/invitación pendiente/);
  assert.match(first,/principal solo puedo ver disponibilidad/);assert.doesNotMatch(first,/Hacer llamadas|Demo rechazada|Demo cocina/);
  const friday=await ask('¿Y el viernes?');assert.match(friday,/27 nov 2026/);
  const whole=await ask('Revisa mis citas mañana');assert.match(whole,/todo el día 22 nov al 24 nov/);
  assert.doesNotMatch(whole,/Demo rechazada/);
  assert.match(await ask('¿Qué invitaciones rechazadas tengo mañana?'),/Demo rechazada/);
  const points=['mañana a las 4 pm','mañana cuatro de la tarde','mañana a las cuatro de la tarde'];
  for (const phrasing of points) {
    await ask(phrasing);assert.equal(calls.at(-1).start,day+'22:00:00.000Z');assert.equal(calls.at(-1).end,day+'22:01:00.000Z');
  }
  const hour=await ask('¿Tengo una hora libre mañana a las cuatro de la tarde?');
  assert.match(hour,/de 4:00 p.m. a 5:00 p.m./);
  assert.equal(calls.at(-1).end,day+'23:00:00.000Z');
});

test('una consulta de disponibilidad requiere el resultado de cada calendario y cubre toda la duración solicitada',async()=>{
  const everyoneFree=fixture({events:ok([]),busy:ok([])});
  assert.match(await everyoneFree.ask('¿Tengo libre una hora mañana a las 4 pm?'),/disponibilidad durante ese intervalo completo/);
  assert.equal(everyoneFree.calls.at(-1).end,'2026-11-23T23:00:00.000Z');
  assert.match(await everyoneFree.ask('¿Tengo libre una hora mañana?'),/qué hora/);
  assert.equal(everyoneFree.calls.length,1);
  const partial=fixture({events:ok([]),busy:denied});
  assert.doesNotMatch(await partial.ask('¿Estoy libre mañana a las 4 pm?'),/tienes disponibilidad/);
});
