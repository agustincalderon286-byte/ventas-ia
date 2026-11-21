import assert from 'node:assert/strict';
import test from 'node:test';
import { createHash } from 'node:crypto';
import { gmailClient, gmailScope, gmailSendScope } from '../src/coach-hycite-gmail.js';
import { availabilityScope, calendarGoogleClient, calendarScope, isSecondaryCalendarId } from '../src/coach-calendar-google.js';

const calendarId = 'abcdef0123456789@group.calendar.google.com', otherCalendar = 'feeeddcc00112233@group.calendar.google.com';
const event = { owner:'seller1', eventId:'c123456789abcdef', calendarId, operationId:'test-confirm-1',
  mode:'create', summary:'Llamar a María', start:'2026-12-01T20:00:00.000Z', end:'2026-12-01T20:30:00.000Z',
  timeZone:'America/Chicago', reminderMinutes:30 };
const config = {origin:'https://example.com',clientId:'test-id',secret:'test-secret'};
const json = (status, data) => ({ status, ok:status < 300,json:async()=>data });

test('Calendar solicita únicamente identidad y acceso a los calendarios creados por TEC con PKCE', () => {
  const url = new URL(calendarGoogleClient(config).authorize('cal.123','challenge','seller@example.com'));
  assert.deepEqual(new Set(url.searchParams.get('scope').split(' ')),new Set(['openid','email',calendarScope]));
  const upgraded = new URL(calendarGoogleClient(config).authorize('cal.456','challenge','seller@example.com',true));
  assert.deepEqual(upgraded.searchParams.get('scope').split(' '),['openid','email',calendarScope,availabilityScope]);
  assert.equal(url.searchParams.get('code_challenge_method'),'S256');
  const granted = gmailClient({...config,calendarReady:true}).authorize('gmail-state','challenge',{send:true,mailbox:'seller@example.com'});
  const upgrade = new URL(granted).searchParams;
  assert.equal(upgrade.get('include_granted_scopes'),'false');
  assert.deepEqual(new Set(upgrade.get('scope').split(' ')),new Set([gmailScope,gmailSendScope]));
  assert.equal(url.searchParams.get('redirect_uri'),config.origin + '/api/coach/conexiones/gmail/callback');
  for (const id of ['primary','a@example.com','../private','example.com@attacker.test']) assert.equal(isSecondaryCalendarId(id),false);
});

test('disponibilidad lee solo los dos calendarios del vendedor y falla ante respuestas parciales',async()=>{
  const start='2035-12-01T19:00:00Z',end='2035-12-01T21:00:00Z',mailbox='seller@example.test';
  let data={timeMin:start,timeMax:end,calendars:{[mailbox]:{busy:[{start:'2035-12-01T19:20:00Z',end:'2035-12-01T19:50:00Z',summary:'privado'}]},[calendarId]:{busy:[]}}};
  const google=calendarGoogleClient(config,async(input,init)=>{
    assert.equal(input,'https://www.googleapis.com/calendar/v3/freeBusy');
    assert.deepEqual(JSON.parse(init.body),{timeMin:start,timeMax:end,items:[{id:'primary'},{id:calendarId}]});
    assert.equal(init.headers.Authorization,'Bearer token');return json(200,data);
  });
  const query=()=>google.availability('token',calendarId,{start,end},mailbox);
  assert.deepEqual(await query(),[{start:'2035-12-01T19:20:00.000Z',end:'2035-12-01T19:50:00.000Z'}]);
  const valid={busy:[]};
  for(const calendars of [{primary:valid},{[calendarId]:valid},{primary:valid,[calendarId]:{busy:[],errors:[{reason:'notFound'}]}},
    {primary:{busy:[{start:'bad',end}]},[calendarId]:valid},{primary:{},[calendarId]:valid}]) {
    data={timeMin:start,timeMax:end,calendars};await assert.rejects(query(),e=>e.status===503);
  }
  data={timeMin:end,timeMax:end,calendars:{primary:valid,[calendarId]:valid}};
  await assert.rejects(query(),e=>e.status===503);
  data={timeMin:start,timeMax:end,calendars:{primary:{errors:[{reason:'forbidden'}]},[calendarId]:valid}};
  await assert.rejects(query(),e=>e.status===403 && e.reauthorize);
});

test('recomprueba el horario al confirmar y recupera sin insertar dos veces tras perder la respuesta',async()=>{
  const candidate={...event,start:'2090-12-01T20:00:00Z',end:'2090-12-01T20:45:00Z',checkAvailability:true};
  let busy=true, row, loseResponse=true;
  const calls=[];
  const google=calendarGoogleClient(config,async(input,init)=>{
    if(input.endsWith('/freeBusy')) {
      calls.push('busy');const {timeMin,timeMax}=JSON.parse(init.body);
      return json(200,{timeMin,timeMax,calendars:{primary:{busy:busy?[{start:timeMin,end:timeMax}]:[]},[calendarId]:{busy:[]}}});
    }
    if(init.method==='GET'){calls.push('read');return row?json(200,row):json(404,{});}
    assert.equal(init.method,'POST');calls.push('insert');
    row={...JSON.parse(init.body),etag:'v1'};
    if(loseResponse) {loseResponse=false;throw Error('sin respuesta');}
    return json(200,row);
  });
  await assert.rejects(google.saveEvent('token',calendarId,candidate),e=>e.status===409&&e.slotBusy);
  busy=false;
  await assert.rejects(google.saveEvent('token',calendarId,{...candidate,start:'2001-01-01T12:00:00Z',end:'2001-01-01T13:00:00Z'}),e=>e.slotBusy);
  await assert.rejects(google.saveEvent('token',calendarId,{...candidate,retryGoogle:true}),/sin respuesta/);
  busy=true;
  assert.equal((await google.saveEvent('token',calendarId,{...candidate,retryGoogle:true})).id,candidate.eventId);
  assert.deepEqual(calls,['busy','busy','read','busy','insert','read']);
});
test('eventos personales son idempotentes, se mueven en el mismo calendario y no agregan asistentes',async()=>{
  const records = new Map(), writes=[];
  const google = calendarGoogleClient(config,async(input,init)=>{
    const url = new URL(input), body=init.body ? JSON.parse(init.body) : null;
    assert.equal(init.headers.Authorization,'Bearer seller-token');
    const id = decodeURIComponent(url.pathname.split('/')[4]);
    assert.ok([calendarId,otherCalendar].includes(id));
    const key=id+'/'+event.eventId;
    if (init.method === 'POST') {
      assert.equal(url.searchParams.get('sendUpdates'),'none');
      if(records.has(key))return json(409,{});
      const row={...body,etag:'first',htmlLink:'https://calendar.google.com/calendar/event?eid=x'};
      records.set(key,row); writes.push(body); return json(200,row);
    }
    if(init.method === 'PATCH') {
      const prev=records.get(key);
      assert.equal(init.headers['If-Match'],prev.etag);
      const row={...prev,...body,etag:'second'};
      records.set(key,row); writes.push(body); return json(200,row);
    }
    return records.has(key) ? json(200,records.get(key)) : json(404,{});
  });
  await google.saveEvent('seller-token',calendarId,event);
  await google.saveEvent('seller-token',calendarId,event);
  const moved={...event,mode:'move',operationId:'move-2',previousOperationId:event.operationId,previousEtag:'first',start:'2026-12-02T20:00:00Z',end:'2026-12-02T20:30:00Z'};
  const result=await google.saveEvent('seller-token',calendarId,moved);
  await google.saveEvent('seller-token',calendarId,moved);
  assert.equal(result.id,event.eventId);
  assert.equal(writes.length,2);
  assert.equal(writes[0].attendees,undefined);
  assert.deepEqual(writes[0].reminders.overrides,[{method:'popup',minutes:30}]);
  assert.equal(writes[1].start.timeZone,'America/Chicago');
  await assert.rejects(google.saveEvent('seller-token',calendarId,{...moved,operationId:'outdated'}),/cambió/);
  await assert.rejects(google.saveEvent('seller-token',calendarId,{...moved,owner:'another',operationId:'tampered'}),/cambió/);
  const manual = records.get(calendarId+'/'+event.eventId);
  manual.etag='manual-edit';manual.start={dateTime:'2026-12-10T16:00:00Z'};
  await assert.rejects(google.saveEvent('seller-token',calendarId,{...moved,operationId:'move-3',previousOperationId:'move-2',previousEtag:'second'}),err=>err.status===409);
  await assert.rejects(google.saveEvent('seller-token',calendarId,moved),err=>err.status===409);
  assert.equal(manual.start.dateTime,'2026-12-10T16:00:00Z');
  assert.equal(writes.length,2);
  records.get(calendarId+'/'+event.eventId).attendees=[{email:'existing@example.com'}];
  await assert.rejects(google.saveEvent('seller-token',calendarId,{...moved,operationId:'move-3',previousOperationId:'move-2',previousEtag:'manual-edit'}),/cambió/);
  await assert.rejects(google.saveEvent('seller-token',otherCalendar,{...moved,calendarId:otherCalendar}));
  assert.equal(writes.length,2);
});
test('findEvent solo lee y devuelve el evento escrito por esa misma operación',async()=>{
  const methods=[];let row=null;
  const google=calendarGoogleClient(config,async(input,init)=>{
    methods.push(init.method);assert.equal(init.body,undefined);
    assert.ok(input.includes('/events/'+event.eventId+'?'));
    return row?json(200,row):json(404,{});
  });
  assert.equal(await google.findEvent('token',calendarId,event),null);
  const tecEvent=createHash('sha256').update([event.owner,calendarId,event.eventId].join(':')).digest('hex');
  row={id:event.eventId,etag:'v1',htmlLink:'https://calendar.google.com/calendar/event?eid=x',status:'confirmed',summary:event.summary,
    start:{dateTime:event.start},end:{dateTime:event.end},reminders:{useDefault:false,overrides:[{method:'popup',minutes:30}]},
    extendedProperties:{private:{tecEvent,tecOperation:event.operationId}}};
  assert.deepEqual(await google.findEvent('token',calendarId,event),{id:event.eventId,htmlLink:row.htmlLink,etag:'v1',operationId:event.operationId});
  assert.equal(await google.findEvent('token',calendarId,{...event,operationId:'another-op'}),null);
  assert.equal(await google.findEvent('token',calendarId,{...event,owner:'another'}),null);
  row.status='cancelled';
  assert.equal(await google.findEvent('token',calendarId,event),null);
  row.status='confirmed';row.start={dateTime:'2026-12-01T21:00:00.000Z'};
  await assert.rejects(google.findEvent('token',calendarId,event),e=>e.status===409);
  assert.ok(methods.every(method=>method==='GET'));
});

test('rechaza campos o fechas inválidos antes de acceder al proveedor',async()=>{
  const google=calendarGoogleClient(config,async()=>assert.fail('No debe llegar a Google'));
  for (const bad of [{operationId:'a\nb'}, {reminderMinutes:50000}, {start:'2026-12-01'}, {eventId:'../otro'}, {timeZone:'Fake/Zone'}]) {
    await assert.rejects(google.saveEvent('token',calendarId,{...event,...bad}));
  }
});
