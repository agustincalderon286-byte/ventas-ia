import test from 'node:test';
import assert from 'node:assert/strict';
import {randomBytes} from 'node:crypto';
import {createCoachCalendarAgent} from '../src/coach-calendar-agent.js';
import {availableSlots,readSlotHours,slotBounds} from '../src/coach-calendar-slots.js';

import {offerBody,offerChoice,offerKeepsSlots,slotLabel} from '../src/coach-offer-text.js';

const now=Date.parse('2026-11-20T17:00:00Z');
const iso=time=>'2026-11-21T'+time+':00.000Z';
const block=(a,b)=>({start:iso(a),end:iso(b)});
function fixture({busy=[],enabled=true,clock=()=>now}={}) {
  const queries=[],drafts=[],confirmed=[];
  const agent=createCoachCalendarAgent({
    config:{key:randomBytes(32),pilot:['a@example.test','r@example.test'],calendarReady:true},
    now:clock,switched:()=>false,findLeads:async()=>[],
    calendar:{
      getConnection:async owner=>({grant:owner+'-grant',calendarId:owner+'-cal',mailbox:owner+'@example.test',availabilityEnabled:enabled}),
      availability:async q=>{ queries.push(q); return busy; }
    },
    followups:{
      prepare:async(binding,proposal)=>{drafts.push({binding,proposal});return {id:'0123456789AB'};},
      confirm:async(_,id)=>{confirmed.push(id);return {status:'slotBusy'};}
    }
  });
  const send=(text,pending='',owner='a',zone='America/Chicago')=>agent({text,sessionId:'chat',
    req:{body:{accountId:owner,coachCalendarPending:pending,coachTimeZone:zone}},
    auth:{user:{_id:owner,email:owner+'@example.test'},session:{tokenHash:'session-'+owner}}});
  return {send,queries,drafts,confirmed};
}

test('consulta los dos horarios, elegir crea solo la vista previa, se confirma por separado',async()=>{
  const f=fixture({busy:[block('15:00','15:25'),block('16:30','17:00')]});
  const result=await f.send('Búscame dos espacios de 45 minutos mañana para una demo');
  assert.deepEqual(f.queries,[{owner:'a',grant:'a-grant',start:iso('15:00'),end:'2026-11-22T01:00:00.000Z'}]);
  assert.match(result.respuesta,/09:30–10:15/);
  assert.match(result.respuesta,/11:00–11:45/);
  assert.equal(result.coachHelpActions.length,2);
  assert.equal(f.drafts.length,0);
  const first=result.coachHelpActions[0];
  assert.equal(first.type,'calendarSlot');assert.equal(first.choice,1);
  const chosen=await f.send('Elegir horario 2 '+first.searchId,result.coachCalendarPending);
  assert.match(chosen.respuesta,/Demo · personal/);
  assert.deepEqual(chosen.coachHelpActions.map(a=>a.type),['calendarConfirm','calendarCancel']);
  assert.equal(f.drafts[0].proposal.start,iso('17:00'));
  assert.equal(f.drafts[0].proposal.availabilitySearchId,first.searchId);
  assert.equal(f.drafts[0].proposal.checkAvailability,true);
  assert.equal(f.confirmed.length,0);
  assert.match((await f.send('Confirmar cita 0123456789AB')).respuesta,/ya no está disponible/);
});

test('la lista se liga a cuenta y búsqueda; los botones antiguos no eligen en las listas nuevas',async()=>{
  const f=fixture(),a=await f.send('¿Qué horarios libres tengo mañana por la tarde?');
  assert.match(a.respuesta,/12:00–19:00/);
  const b=await f.send('horarios libres mañana');
  const stale=await f.send('Elegir horario 1 '+a.coachHelpActions[0].searchId,b.coachCalendarPending);
  assert.match(stale.respuesta,/otra búsqueda/);
  assert.equal(stale.coachCalendarPending,b.coachCalendarPending);
  await f.send('la segunda',b.coachCalendarPending,'r');
  assert.equal(f.drafts.length,0);
  await f.send('la segunda',b.coachCalendarPending);
  assert.equal(f.drafts.length,1);
  const own=await f.send('horarios disponibles mañana por la tarde para la demo con Omar','','r');
  await f.send('1',own.coachCalendarPending,'r');
  assert.equal(f.drafts[1].binding.owner,'r');assert.equal(f.drafts[1].proposal.name,'Omar');
});

test('solo propone opciones que caben, pide permiso a cuentas antiguas y aclara restricciones',async()=>{
  const f=fixture();
  const one=await f.send('Busca dos opciones para una demo de 45 minutos mañana de 14:00 a 15:00');
  assert.equal(one.coachHelpActions.length,1);
  assert.match(one.respuesta,/Encontré una opción/);
  assert.match((await f.send('2',one.coachCalendarPending)).respuesta,/Elige «1»/);
  const none=await f.send('Horarios libres mañana por la tarde para una demo de 8 horas');
  assert.deepEqual(none.coachHelpActions,[]);
  assert.match(none.respuesta,/No encontré espacio/);
  const initial=await f.send('Dame horarios libres después de las 3 para una demo mañana');
  assert.match(initial.respuesta,/Entre qué horas/);
  const next=await f.send('de 14:00 a 16:00',initial.coachCalendarPending);
  assert.match(next.respuesta,/14:00–16:00/);
  const g=fixture({enabled:false});
  assert.match((await g.send('Busca horarios libres mañana')).respuesta,/permiso de disponibilidad/);
  assert.equal(g.queries.length,0);
  const emptyFilter=await f.send('Busca horarios libres');
  assert.match(emptyFilter.respuesta,/qué día/);
  assert.match((await f.send('mañana por la mañana',emptyFilter.coachCalendarPending)).respuesta,/09:00–12:00/);
});

test('combina bloques desordenados, respeta límites, zona, cambio horario y el futuro',()=>{
  const all=block('15:00','18:00'), gaps=availableSlots(all,[block('15:33','16:00'),block('15:05','15:55')],45,Date.parse(iso('15:00')));
  assert.deepEqual(gaps,[block('16:00','16:45'),block('17:00','17:45')]);
  assert.deepEqual(availableSlots(all,[all],10,now),[]);
  assert.deepEqual(availableSlots(all,[],45,Date.parse(iso('17:15'))),[]);
  assert.throws(()=>availableSlots(all,[{start:'invalid',end:all.end}],10,now));
  assert.equal(slotBounds({timeZone:'America/Chicago',day:Date.UTC(2027,2,14),slotHours:[120,240]}).start,undefined);
  assert.equal(slotBounds({timeZone:'America/Chicago',day:Date.UTC(2027,2,14)}).start,'2027-03-14T14:00:00.000Z');
  assert.deepEqual(readSlotHours('entre las 2 pm y las 5 pm'),{hours:[840,1020]});
});


test('prefiere cuartos de hora separados sin perder dos huecos ajustados',()=>{
  assert.deepEqual(availableSlots(block('15:03','16:33'),[],45,now),
    [block('15:03','15:48'),block('15:48','16:33')]);
  assert.deepEqual(availableSlots(block('15:03','17:15'),[],35,now),
    [block('15:15','15:50'),block('16:15','16:50')]);
});

test('la edición del correo conserva el orden de las dos opciones y su zona',()=>{
  const slots=[block('15:00','15:45'),block('17:00','17:45')];
  const timeZone='America/Chicago';
  const offer={name:'Ana',timeZone,labels:slots.map(slot=>slotLabel(slot.start,timeZone))};
  const body=offerBody(offer);
  assert.ok(offerKeepsSlots(offer,body+'\nGracias, Agustín.'));
  assert.equal(offerKeepsSlots(offer,body.replace(timeZone,'America/New_York')),false);
  assert.equal(offerKeepsSlots(offer,body.replace('1. '+offer.labels[0],'2. '+offer.labels[0])),false);
  assert.equal(offerKeepsSlots(offer,body+'\nMejor ven el miércoles.'),false);
  assert.equal(offerChoice('La primera no.',slots,timeZone).index,undefined);
});
