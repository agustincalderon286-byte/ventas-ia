import assert from 'node:assert/strict';
import {randomBytes} from 'node:crypto';
import test from 'node:test';
import {connectionConfig, registerCoachConnections} from '../src/coach-connections.js';
import {sealToken} from '../src/coach-hycite-gmail.js';
import {interpretPilotPlan} from '../src/coach-plan-language.js';

const sellers={agustin:'agustincalderon286@gmail.com',rigo:'calderonrigoberto51@gmail.com'};
const appointmentRequest='Revisa si tengo citas para mañana en mi calendario de Google.';

function fixture() {
  const config={...connectionConfig({COACH_CONNECTIONS_PILOT_EMAILS:Object.values(sellers).join(',')}),
    key:randomBytes(32),ready:true,sheetsReady:true,calendarReady:true};
  const sessions=new Map(), calendars=new Map(), mailboxes=new Map();
  const calls={provider:[],model:[],writes:[],ownership:[]};
  const forbid=name=>async()=>{calls.writes.push(name);throw Error('La lectura intentó '+name);};
  for (const [id,email] of Object.entries(sellers)) {
    mailboxes.set(id,{_id:id,mailbox:email,grant:'g-'+id,sendEnabled:true,encryptedToken:sealToken(id,config.key,id)});
    calendars.set(id,{_id:id,mailbox:email,grant:'c-'+id,availabilityEnabled:true,
      calendarId:id.repeat(4)+'@group.calendar.google.com',encryptedToken:sealToken(id,config.key,'calendar-refresh:'+id)});
  }
  const lean=data=>({lean:async()=>structuredClone(data)});
  const readStore=(source,docs)=>({
    findById(id){calls.ownership.push([source,id]);return lean(docs.get(id));},
    findOne(filter){const doc=docs.get(filter._id);return lean(doc?.grant===filter.grant?doc:null);},
    exists(filter){const doc=docs.get(filter._id);return !!doc && doc.grant===filter.grant && doc.calendarId===filter.calendarId;},
    updateOne:forbid(source+' update'),insertOne:forbid(source+' insert')
  });
  const plans={
    load:async scope=>structuredClone(sessions.get(scope)),
    async save(scope,previous,data){
      if (sessions.get(scope)?.revision!==previous) return null;
      const revision=randomBytes(12).toString('hex');
      sessions.set(scope,{revision,data:structuredClone(data)});
      return revision;
    }
  };
  const calendarGoogle={
    refresh:async owner=>({access_token:owner}),
    async agendaEvents(owner,id,when){
      assert.equal(id,calendars.get(owner).calendarId);calls.provider.push(['agenda',owner,when]);
      return [{title:'Demo de prueba '+owner,start:when.start,end:when.end,available:false}];
    },
    async primaryBusy(owner,when,mailbox){
      assert.equal(mailbox,sellers[owner]);calls.provider.push(['primary',owner,when]);return [];
    },
    saveEvent:forbid('calendar.saveEvent')
  };
  const gmail={
    refresh:async owner=>({access_token:owner}),
    async inbox(owner){calls.provider.push(['inbox',owner]);return [{id:'test-'+owner,subject:'Aviso de prueba '+owner,
      from:'demo@example.invalid',date:new Date().toISOString()}];},
    send:forbid('gmail.send')
  };
  const boundAuth=(login,mode)=>({user:{_id:login,email:sellers[login]},session:{tokenHash:'login-'+login,accessMode:mode}});
  let decision='agenda';
  const pilotInterpret=(text,opts)=>interpretPilotPlan(text,{...opts,apiKey:'test',fetchImpl:async(_url,init)=>{
    const body=JSON.parse(init.body);const masked=JSON.parse(body.input);calls.model.push({model:body.model,...masked});
    // Simulate only the provider's choice. Schema validation, fallbacks, binding, dispatch and result delivery are production code.
    const [name,args]=decision==='inbox'?['leer_bandeja',{}]:decision==='unsupported'?['derivar',{destino:'fuera_de_alcance'}]:
      ['consultar_agenda',{schedule:masked.message.includes('viernes')?'el viernes':'mañana'}];
    return {ok:true,json:async()=>({status:'completed',output:[{type:'function_call',name,arguments:JSON.stringify(args)}]})};
  }});
  const run=registerCoachConnections({get(){},post(){}},{
    config,switched:req=>req.switched,requireActive:req=>req.auth,readSheetsSession:async req=>req.auth,
    store:readStore('gmail',mailboxes),sheetsStore:readStore('sheets',new Map()),calendarStore:readStore('calendar',calendars),
    calendarGoogle,gmail,offers:null,followups:{prepare:forbid('preparar cita'),confirm:forbid('confirmar cita'),
      search:async()=>{calls.provider.push(['legacy-calendar']);return [];}},
    drafts:{prepare:forbid('preparar correo'),confirm:forbid('confirmar correo')},
    planDependencies:{plans,pilotInterpret}
  });
  const ask=(owner,text,body={},mode='direct',switched=false)=>{
    const auth=boundAuth(owner,mode);const req={auth,switched,body:{accountId:owner,coachPlanVersion:1,
      coachMailVersion:1,coachCalendarVersion:1,coachTimeZone:'America/Chicago',responseMode:'text',...body}};
    return run({req,auth,sessionId:'piloto-compartido',text});
  };
  return {ask,calls,calendars,mailboxes,decide:value=>{decision=value;}};
}

test('el chat ejecuta la agenda de cada participante y conserva el contexto por cuenta para «¿y el viernes?»',async()=>{
  const f=fixture();
  for (const owner of Object.keys(sellers)) {
    for(const mode of ['text','audio']){
      // Also cover the original false refusal: an unsupported classification must still reach Calendar.
      f.decide(mode==='audio'?'unsupported':'agenda');
      const first=await f.ask(owner,appointmentRequest,{responseMode:mode});
      assert.match(first.respuesta,new RegExp('Demo de prueba '+owner));assert.match(first.coachPlanPending,/^[0-9a-f]{24}$/);
      assert.equal(!!first.coachPrivateVoice,mode==='audio');assert.deepEqual(first.coachHelpActions,[]);
      assert.deepEqual(f.calls.provider.slice(-2).map(x=>x.slice(0,2)),[['agenda',owner],['primary',owner]]);
      const context={responseMode:mode,coachQuestionFocus:'plan',coachPlanPending:first.coachPlanPending};
      const next=await f.ask(owner,'¿y el viernes?',context);
      assert.match(next.respuesta,new RegExp('Demo de prueba '+owner));assert.deepEqual(next.coachHelpActions,[]);
      assert.equal(f.calls.model.at(-1).context.lastRead,'agenda');
      const range=f.calls.provider.at(-1)[2];
      assert.equal(new Intl.DateTimeFormat('en-US',{timeZone:range.zone,weekday:'short'}).format(new Date(range.start)),'Fri');
      const other=owner==='rigo'?'agustin':'rigo';
      const before=f.calls.provider.length;
      const foreign=await f.ask(other,'¿y el viernes?',{...context,coachPlanPending:next.coachPlanPending});
      assert.match(foreign.respuesta,/cambió|caducó/);assert.equal(f.calls.provider.length,before);
    }
  }
  assert.ok(f.calls.model.every(call=>call.model==='gpt-6.2-sol'));
  assert.deepEqual(f.calls.writes,[]);
});

test('una conexión ausente no impide usar la otra, con información y estado propios',async()=>{
  const f=fixture();
  f.calendars.delete('rigo');
  f.mailboxes.delete('agustin');
  const missingCal=await f.ask('rigo',appointmentRequest,{responseMode:'audio'});
  assert.match(missingCal.respuesta,/Conecta tu Google Calendar/);assert.deepEqual(f.calls.provider,[]);
  const rigoStatus=f.calls.model.at(-1).context.connections;
  assert.equal(rigoStatus.calendar,'missing');assert.equal(rigoStatus.gmail,'connected');
  f.decide('inbox');
  const own=await f.ask('rigo','Revisa mis correos',{responseMode:'audio'});
  assert.match(own.respuesta,/Aviso de prueba rigo/);assert.equal(own.coachPrivateVoice,true);
  const missingMail=await f.ask('agustin','Revisa mis correos');
  assert.match(missingMail.respuesta,/Conecta tu propia cuenta de Gmail/);
  const agustinStatus=f.calls.model.at(-1).context.connections;
  assert.equal(agustinStatus.gmail,'missing');assert.equal(agustinStatus.calendar,'connected');
  f.decide('agenda');
  assert.match((await f.ask('agustin',appointmentRequest)).respuesta,/Demo de prueba agustin/);
  assert.deepEqual(f.calls.provider.map(v=>v.slice(0,2)),[['inbox','rigo'],['agenda','agustin'],['primary','agustin']]);
  assert.deepEqual(f.calls.writes,[]);
});

test('una cuenta ajena o una visita de equipo no puede iniciar la lectura ni recibir el audio privado',async()=>{
  const f=fixture();
  for (const [body,mode,switched] of [[{accountId:'rigo'},'direct',false],[{},'team_visit',true],[{},'team_visit',false]]){
    const answer=await f.ask('agustin',appointmentRequest,{...body,responseMode:'audio'},mode,switched);
    assert.doesNotMatch(answer?.respuesta || '',/Demo de prueba/);assert.notEqual(answer?.coachPrivateVoice,true);
  }
  assert.deepEqual(f.calls.model,[]);assert.deepEqual(f.calls.ownership,[]);
  assert.deepEqual(f.calls.provider,[]);assert.deepEqual(f.calls.writes,[]);
});

test('una selección errónea del modelo respeta «no revises mi calendario»',async()=>{
  const f=fixture();
  for(const owner of Object.keys(sellers)){
    // Fixture selects the Agenda tool regardless of the request. Consent is enforced by TEC.
    const reply=await f.ask(owner,'No revises mi calendario, solo quería conocer cómo funciona.',{responseMode:'audio'});
    assert.doesNotMatch(reply?.respuesta || '',/Demo de prueba/);
  }
  assert.deepEqual(f.calls.provider,[]);assert.deepEqual(f.calls.writes,[]);
});
