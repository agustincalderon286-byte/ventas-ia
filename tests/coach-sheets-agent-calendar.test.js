import test from 'node:test';
import assert from 'node:assert/strict';
import {randomBytes} from 'node:crypto';
import {createSheetsChatAgent} from '../src/coach-sheets-agent.js';
import {createCoachCalendarAgent} from '../src/coach-calendar-agent.js';
import {calendarGoogleClient} from '../src/coach-calendar-google.js';

const listText = 'Revisa mis leads y dime a quién le toca seguimiento hoy';
const chooseText = 'Busca dos horarios para el lead 1 mañana de 10:00 a 16:00';
const actors = {a:'Agustín Prueba',r:'Rigoberto Prueba'};
const email = id => id+'@example.test';
const calendarId = id => 'calendario-'+id+'@group.calendar.google.com';

function fixture(t) {
  t.mock.timers.enable({apis:['Date'],now:Date.parse('2026-11-21T17:00:00Z')});
  const config = {key:randomBytes(32),sheetsPilot:Object.keys(actors).map(email),pilot:Object.keys(actors).map(email),calendarReady:true};
  const grants = {}, sessions = {}, books = {}, availability = [], writes = [], busy = new Set(), drafts = new Map();
  for (const [id,name] of Object.entries(actors)) {
    grants[id]='sheet-grant-'+id;
    sessions[id]={user:{_id:id,name,email:email(id)},session:{tokenHash:'sesion-'+id,accessMode:'direct'}};
    const own = {kind:'tickets',sheetId:0,sheetTitle:'BOLETAS NUEVAS',row:4,fullName:'Contacto '+id,
      host:'',seller:name,followupDate:'2026-11-21',status:'',result:'',history:'Llamar'};
    const referral = {kind:'referrals',sheetTitle:'4 en 14',sheetId:3,host:'Anfitrión '+id};
    if (id==='r') Object.assign(own,referral);
    books[id]=[own,
      {...own,...(id==='a' ? referral : {kind:'tickets',sheetId:0,sheetTitle:'BOLETAS NUEVAS',host:''}),row:8,fullName:'Referencia '+id,
        history:'Volver a llamar',followupDate:'',seller:''},
      {...own,row:12,fullName:'Persona de otro vendedor',seller:'Otro vendedor'},
      {...own,row:13,fullName:'No contactar',history:'No volver a llamar'}];
  }
  const remote = calendarGoogleClient(config,async(url,init)=>{
    const owner = Object.keys(actors).find(id=>init.headers.Authorization==='Bearer calendario-token-'+id);
    assert.ok(owner,'Calendar needs the token belonging to this seller');
    const body=JSON.parse(init.body);
    if (url.endsWith('/freeBusy')) {
      assert.deepEqual(body.items,[{id:'primary'},{id:calendarId(owner)}]);
      availability.push({owner,range:[body.timeMin,body.timeMax]});
      const blocks=busy.has(owner)?[{start:body.timeMin,end:body.timeMax}]:[];
      return Response.json({timeMin:body.timeMin,timeMax:body.timeMax,calendars:{primary:{busy:blocks},[calendarId(owner)]:{busy:[]}}});
    }
    assert.equal(init.method,'POST');
    assert.equal(new URL(url).pathname.split('/').slice(-2).map(decodeURIComponent).join('/'),calendarId(owner)+'/events');
    assert.equal(new URL(url).searchParams.get('sendUpdates'),'none');
    assert.equal(body.attendees,undefined);
    writes.push({owner,body});
    return Response.json({id:body.id,etag:'"saved"',htmlLink:'https://calendar.google.com/calendar/event?eid=fixture'});
  });
  const calendar=createCoachCalendarAgent({config,switched:()=>false,findLeads:()=>assert.fail('a Sheets selection must not select a CRM namesake'),
    syncLead:()=>assert.fail('Google Sheets contacts have no bound CRM record'),calendar:{
      getConnection:async id=>({grant:'calendar-grant-'+id,calendarId:calendarId(id),mailbox:email(id),availabilityEnabled:true}),
      availability:q=>remote.availability('calendario-token-'+q.owner,calendarId(q.owner),q,email(q.owner)),
      saveEvent:event=>remote.saveEvent('calendario-token-'+event.owner,calendarId(event.owner),{...event,mailbox:email(event.owner)})
    },followups:{
      async prepare(binding,proposal) {
        const id=randomBytes(6).toString('hex').toUpperCase();
        drafts.set(id,{binding,proposal});return {id};
      },
      async confirm(binding,id,{saveEvent}) {
        const draft=drafts.get(id);
        if (!draft || JSON.stringify(draft.binding)!==JSON.stringify(binding)) return {status:'unavailable'};
        try {
          const receipt=await saveEvent({...draft.proposal,owner:binding.owner,grant:binding.grant,eventId:id.toLowerCase().repeat(2),operationId:id.toLowerCase()});
          return {...draft.proposal,...receipt,status:'saved',reference:id};
        } catch(error) {if (error.slotBusy) return {status:'slotBusy'};throw error;}
      }
    }
  });
  const effects={afterRead:null};
  const sheet=createSheetsChatAgent({config,calendar,switched:req=>Boolean(req.body.switched),readSession:async req=>sessions[req.body.accountId],
    sheets:{
      access:async id=>({token:'hoja-token-'+id,grant:grants[id],spreadsheetId:'libro-'+id,ticketsTitle:'BOLETAS NUEVAS',referralsTitle:'4 en 14'}),
      current:async(id,grant)=>grants[id]===grant
    },
    google:{readFollowups:async(token,book)=>{
      const owner=Object.keys(actors).find(id=>token==='hoja-token-'+id);
      assert.equal(book.spreadsheetId,'libro-'+owner);
      if(effects.afterRead) effects.afterRead();
      return {locale:'en_US',rows:structuredClone(books[owner])};
    }}
  });
  const send = async (owner,text,body={},sessionId='chat personal')=>{
    const args={text,sessionId,auth:structuredClone(sessions[owner]),req:{body:{accountId:owner,coachTimeZone:'America/Chicago',
      activeCrmRecordId:'0123456789abcdef01234567',...body}}};
    return await sheet(args) ?? await calendar(args);
  };
  async function preview(owner, list) {
    const slots=await send(owner,chooseText,{coachSheetsPending:list.coachSheetsPending});
    assert.deepEqual(slots.coachHelpActions.map(a=>a.type),['calendarSlot','calendarSlot'],slots.respuesta);
    const selected=slots.coachHelpActions[0];
    const draft=await send(owner,'Elegir horario 1 '+selected.searchId,{coachCalendarPending:slots.coachCalendarPending});
    assert.deepEqual(draft.coachHelpActions.map(a=>a.type),['calendarConfirm','calendarCancel'],draft.respuesta);
    assert.match(draft.respuesta,new RegExp('Contacto '+owner));
    assert.ok(draft.respuesta.includes(email(owner)));
    return draft.coachHelpActions[0].id;
  }
  return {send,preview,books,sessions,grants,availability,writes,busy,drafts,effects};
}

test('two independent Coach chats read their tabs, select the due lead and create one personal event each only after confirmation',async t=>{
  const f=fixture(t);
  const [a,r]=await Promise.all(Object.keys(actors).map(id=>f.send(id,listText)));
  for (const [owner,reply,other] of [['a',a,'r'],['r',r,'a']]) {
    assert.ok(reply.coachSheetsAgent);
    assert.ok(reply.respuesta.includes('Para hoy (1)'));
    assert.ok(reply.respuesta.includes('Pendientes sin fecha (1)'));
    assert.ok(reply.respuesta.includes('1. Contacto '+owner));
    assert.ok(reply.respuesta.split('\n').find(line=>line.includes('1. Contacto '+owner)).includes(owner==='a'?'BOLETAS NUEVAS':'4 en 14'));
    assert.ok(reply.respuesta.includes('Referencia '+owner));
    assert.ok(reply.respuesta.includes('Anfitrión '+owner));
    assert.ok(!reply.respuesta.includes('Contacto '+other));
    assert.doesNotMatch(reply.respuesta,/Persona de otro vendedor|No contactar/);
  }
  const [draftA,draftR]=await Promise.all([f.preview('a',a),f.preview('r',r)]);
  assert.equal(f.drafts.size,2);
  assert.equal(f.writes.length,0);
  await f.send('r','Confirmar cita '+draftA);
  assert.equal(f.writes.length,0);
  const receipts=await Promise.all([f.send('a','Confirmar cita '+draftA),f.send('r','Confirmar cita '+draftR)]);
  for (const [index,owner] of ['a','r'].entries()) {
    assert.match(receipts[index].respuesta,/Google Calendar confirmó/);
    assert.equal(f.availability.filter(q=>q.owner===owner).length,2);
    const [event]=f.writes.filter(w=>w.owner===owner);
    assert.ok(event.body.summary.includes('Contacto '+owner));
    assert.equal(event.body.start.timeZone,'America/Chicago');
    assert.equal(event.body.start.dateTime,'2026-11-22T16:00:00.000Z');
  }
  assert.equal(f.writes.length,2);
});

test('another account, conversation, login, grant, changed sheet row, or expiration cannot select an old contact',async t=>{
  const f=fixture(t);
  const list=await f.send('a',listText);
  const pick={coachSheetsPending:list.coachSheetsPending};
  for (const response of [
    await f.send('r',chooseText,pick),
    await f.send('a',chooseText,pick,'otra conversación'),
    await f.send('a',chooseText,{...pick,accountId:'r'}),
    await f.send('a',chooseText,{...pick,switched:true})
  ]) assert.deepEqual(response.coachHelpActions,[]);
  f.sessions.a.session.tokenHash='otra sesión';
  assert.deepEqual((await f.send('a',chooseText,pick)).coachHelpActions,[]);
  f.sessions.a.session.tokenHash='sesion-a';
  f.grants.a='otra conexión';
  assert.deepEqual((await f.send('a',chooseText,pick)).coachHelpActions,[]);
  f.grants.a='sheet-grant-a';
  f.books.a[0].history='El contacto cambió';
  assert.match((await f.send('a',chooseText,pick)).respuesta,/fila|actualizad/i);
  f.books.a[0].history='Llamar';
  t.mock.timers.tick(16*60000);
  assert.deepEqual((await f.send('a',chooseText,pick)).coachHelpActions,[]);
  assert.equal(f.availability.length,0);
  assert.equal(f.drafts.size,0);
  assert.equal(f.writes.length,0);
});

test('session or consent changing in flight hides the results; a newly occupied slot prevents the event insertion',async t=>{
  const f=fixture(t);
  f.effects.afterRead=()=>{f.grants.a='renovado';};
  assert.doesNotMatch((await f.send('a',listText)).respuesta,/Contacto a|Referencia a/);
  f.effects.afterRead=()=>{f.sessions.a.session.tokenHash='otra sesión';};
  assert.doesNotMatch((await f.send('a',listText)).respuesta,/Contacto a|Referencia a/);
  f.effects.afterRead=null;
  const list=await f.send('r',listText);
  const id=await f.preview('r',list);
  f.busy.add('r');
  assert.match((await f.send('r','Confirmar cita '+id)).respuesta,/ya no está disponible/);
  assert.equal(f.writes.length,0);
});

test('las instrucciones del chat permiten recorrer las tres secciones sin interceptar Calendar o un correo', async () => {
  const {sheetsChatIntent}=await import('../src/coach-sheets-agent.js');
  for (const [text,only,page] of [
    [listText,'',1],['Revisa mis leads de hoy','',1],['Pendientes sin fecha página 2','undated',2],
    ['Leads vencidos página 3','overdue',3],['Leads de seguimiento de hoy página 2','today',2]
  ]) assert.deepEqual(sheetsChatIntent(text),{kind:'list',only,page});
  assert.equal(sheetsChatIntent('Muestra mis citas de Calendar hoy'),null);
  assert.equal(sheetsChatIntent('Responde al correo sobre los leads pendientes de hoy'),null);
  assert.equal(sheetsChatIntent('Busca dos horarios para el lead n.º 2 mañana').index,1);
});
