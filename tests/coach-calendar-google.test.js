import assert from 'node:assert/strict';
import test from 'node:test';
import { gmailClient, gmailScope, gmailSendScope } from '../src/coach-hycite-gmail.js';
import { calendarGoogleClient, calendarScope, isSecondaryCalendarId } from '../src/coach-calendar-google.js';

const calendarId = 'abcdef0123456789@group.calendar.google.com', otherCalendar = 'feeeddcc00112233@group.calendar.google.com';
const event = { owner:'seller1', eventId:'c123456789abcdef', calendarId, operationId:'test-confirm-1',
  mode:'create', summary:'Llamar a María', start:'2026-12-01T20:00:00.000Z', end:'2026-12-01T20:30:00.000Z',
  timeZone:'America/Chicago', reminderMinutes:30 };
const config = {origin:'https://example.com',clientId:'test-id',secret:'test-secret'};
const json = (status, data) => ({ status, ok:status < 300,json:async()=>data });

test('Calendar solicita únicamente identidad y acceso a los calendarios creados por TEC con PKCE', () => {
  const url = new URL(calendarGoogleClient(config).authorize('cal.123','challenge','seller@example.com'));
  assert.deepEqual(new Set(url.searchParams.get('scope').split(' ')),new Set(['openid','email',calendarScope]));
  assert.equal(url.searchParams.get('code_challenge_method'),'S256');
  const granted = gmailClient({...config,calendarReady:true}).authorize('gmail-state','challenge',{send:true,mailbox:'seller@example.com'});
  const upgrade = new URL(granted).searchParams;
  assert.equal(upgrade.get('include_granted_scopes'),'false');
  assert.deepEqual(new Set(upgrade.get('scope').split(' ')),new Set([gmailScope,gmailSendScope]));
  assert.equal(url.searchParams.get('redirect_uri'),config.origin + '/api/coach/conexiones/gmail/callback');
  for (const id of ['primary','a@example.com','../private','example.com@attacker.test']) assert.equal(isSecondaryCalendarId(id),false);
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
test('rechaza campos o fechas inválidos antes de acceder al proveedor',async()=>{
  const google=calendarGoogleClient(config,async()=>assert.fail('No debe llegar a Google'));
  for (const bad of [{operationId:'a\nb'}, {reminderMinutes:50000}, {start:'2026-12-01'}, {eventId:'../otro'}, {timeZone:'Fake/Zone'}]) {
    await assert.rejects(google.saveEvent('token',calendarId,{...event,...bad}));
  }
});
