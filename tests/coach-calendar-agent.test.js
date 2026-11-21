import test from 'node:test';
import assert from 'node:assert/strict';
import {randomBytes} from 'node:crypto';
import {calendarIntent,readFollowup,followupTime} from '../src/coach-followup-input.js';
import {createCoachCalendarAgent} from '../src/coach-calendar-agent.js';

const date=Date.parse('2026-11-20T17:00:00Z');
const contact='0123456789abcdef01234567';
const lead={id:contact,name:'María López',nextActionAt:'2026-11-23T01:00:00.000Z'};
function fixture({leads=[lead],events=[],connected=true,switched=false}={}) {
  const proposals=[],searches=[],finds=[];
  const agent=createCoachCalendarAgent({config:{key:randomBytes(32),pilot:['a@example.test'],calendarReady:true},switched:()=>switched,
    calendar:{getConnection:async()=>connected?{calendarId:'cid',mailbox:'a@example.test',grant:'local-grant'}:null},
    findLeads:async q=>{finds.push(q);return leads},syncLead:()=>assert.fail('must confirm first'),now:()=>date,
    followups:{prepare:async (scope,value,target)=>{proposals.push({scope,value,target});return {id:'0123456789AB'};},
      search:async (scope,params)=>{searches.push(params);return events;}}});
  const send=(text,body={})=>agent({auth:{user:{_id:'alice',email:'a@example.test'},session:{tokenHash:'private'}},sessionId:'chat-1',text,
      req:{body:{accountId:'alice',coachTimeZone:'America/Chicago',...body}}});
  return {send,proposals,finds,searches};
}

test('el vendedor aclara el meridiano y recibe una vista previa de día local, prospecto y recordatorio',async()=>{
  const {send,proposals}=fixture();
  const first=await send('Agéndame una llamada con María el jueves a las seis y recuérdamelo media hora antes');
  assert.match(first.respuesta,/mañana o de la tarde/);assert.equal(proposals.length,0);
  const second=await send('de la tarde',{coachCalendarPending:first.coachCalendarPending});
  assert.match(second.respuesta,/María López/);assert.match(second.respuesta,/jueves/);
  assert.match(second.respuesta,/30 min antes/);assert.match(second.respuesta,/Fecha anterior/);
  assert.deepEqual(second.coachHelpActions.map(a=>a.type),['calendarConfirm','calendarCancel']);
  assert.equal(proposals[0].value.start,'2026-11-27T00:00:00.000Z');
  assert.equal(proposals[0].value.expectedStart,lead.nextActionAt);
});

test('la ficha abierta sirve solo si no se indicó otro nombre; selección desconocida no se apropia',async()=>{
  const {send,finds}=fixture();
  await send('Agéndame un seguimiento mañana a las 4 pm',{activeCrmRecordId:contact});
  assert.deepEqual(finds[0],{owner:'alice',id:contact});
  await send('Agéndame un seguimiento con María mañana a las 4 pm',{activeCrmRecordId:contact});
  assert.deepEqual(finds[1],{owner:'alice',name:'María'});
  const missing=fixture({leads:[]});
  assert.match((await missing.send('Agéndame un seguimiento mañana a las 4 pm',{activeCrmRecordId:contact})).respuesta,/Con quién/);
});

test('dos prospectos piden nombre inequívoco o solo calendario, nunca se escoge el primero',async()=>{
  const {send,proposals}=fixture({leads:[lead,{...lead,name:'María Cruz'}]});
  const choice=await send('Agenda una llamada con María el lunes a las 8 am');
  assert.match(choice.respuesta,/varios prospectos/);assert.equal(proposals.length,0);
  const manual=await send('solo en mi calendario',{coachCalendarPending:choice.coachCalendarPending});
  assert.match(manual.respuesta,/solo en tu calendario/);assert.equal(proposals.length,1);assert.equal(proposals[0].value.leadId,'');
  assert.equal(proposals[0].value.name,'María');
});

test('mover una llamada conserva título, identificador, duración, aviso y compara el seguimiento previo del CRM',async()=>{
  const target={eventId:'private-google-id',name:'María López',leadId:contact,summary:'Llamada · María López',operationId:'prior',
    timeZone:'America/Chicago',start:'2026-11-23T21:00:00.000Z',end:'2026-11-23T21:45:00.000Z',action:'llamar',reminderMinutes:25};
  const f=fixture({events:[target]});
  const moved=await f.send('Mueve la cita con María al jueves');
  assert.match(moved.respuesta,/Mover/);assert.equal(f.searches[0].name,'María');assert.equal(f.proposals[0].target,target);
  assert.equal(f.proposals[0].value.start,'2026-11-26T21:00:00.000Z');assert.equal(f.proposals[0].value.end,'2026-11-26T21:45:00.000Z');
  assert.equal(f.proposals[0].value.expectedStart,target.start);assert.equal(f.proposals[0].value.previousOperationId,'prior');
});

test('filtros de sesión y zona, días locales y horas de transición DST',async()=>{
  assert.equal(calendarIntent('Responde al segundo correo. Dile que podemos mover la cita al jueves'),null);
  assert.equal(await fixture().send('Mejor el viernes',{coachMailPending:'correo-pendiente'}),null);
  assert.match((await fixture({switched:true}).send('Revisa mis seguimientos')).respuesta,/propia cuenta/);
  assert.match((await fixture({connected:false}).send('Agenda una llamada con María mañana')).respuesta,/Conecta Google Calendar/);
  for(const text of ['2027-03-14 a las 2:30 am','2027-11-07 a las 1:30 am']){
    const {state}=readFollowup(text,{}, {timeZone:'America/Chicago',now:date});
    assert.match(followupTime(state,date).question,/cambio de horario/);
  }
  const {state}=readFollowup('mañana a las 18:00',{}, {timeZone:'Europe/Madrid',now:date});
  assert.equal(followupTime(state,date).start,'2026-11-21T17:00:00.000Z');
});

test('las instrucciones para corregir un borrador no interceptan el chat de correo',async()=>{
  const f=fixture();
  for(const text of ['Agrega al final: gracias por la demo','Cambia la palabra cita por llamada','Cambia jueves por viernes en la cita','Mejor el viernes','Responde al segundo: agenda una cita','Corrige el segundo correo']) {
    assert.equal(await f.send(text,{coachMailPending:'gmail-context'}),null,text);
  }
  assert.match((await f.send('Agenda una llamada con María mañana a las 6pm',{coachMailPending:'gmail-context'})).respuesta,/Agendar/);
  assert.match((await f.send('Mueve mi cita',{coachMailPending:'gmail-context'})).respuesta,/Con quién/);
});

test('listado de hoy consulta únicamente el rango civil del vendedor y aclara una ventana no compatible',async()=>{
  const f=fixture();
  const reply=await f.send('Muestra mis citas de hoy');
  assert.match(reply.respuesta,/período que pediste/);
  assert.deepEqual(f.searches[0],{limit:6,since:Date.parse('2026-11-20T06:00:00Z'),before:Date.parse('2026-11-21T06:00:00Z')});
  assert.match((await f.send('Muestra mis citas del año pasado')).respuesta,/Puedes pedirme/);
  assert.equal(f.searches.length,1);
});

test('horario con puntos, rechazo de horas fuera de rango y cita movida en Google',async()=>{
  const {state}=readFollowup('Agéndame una cita mañana a las 8 p.m.',{}, {timeZone:'America/Chicago',now:date});
  assert.equal(followupTime(state,date).start,'2026-11-22T02:00:00.000Z');
  assert.match(readFollowup('mañana a las 25:30',{}, {timeZone:'Europe/Madrid',now:date}).question,/hora/);
  const target={eventId:'existing',name:'María López',summary:'Llamada · María López',etag:'"123"',operationId:'previous',
    timeZone:'America/Chicago',start:'2026-11-23T21:00:00.000Z',end:'2026-11-23T21:45:00.000Z',action:'llamar',reminderMinutes:25};
  const f=fixture({events:[target]});
  await f.send('Mueve mi cita con María al jueves a las 5 pm');
  assert.equal(f.proposals[0].value.previousEtag,'"123"');
});
