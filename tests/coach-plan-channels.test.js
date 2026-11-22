import test from 'node:test';
import assert from 'node:assert/strict';
import {randomBytes} from 'node:crypto';
import {sealToken} from '../src/coach-hycite-gmail.js';
import {createPlanChannels} from '../src/coach-plan-channels.js';
import {createConversationPlan} from '../src/coach-conversation-plan.js';

const fixed=Date.parse('2026-11-21T18:00:00Z');
function fixture({busy=[],outcome='saved',outbound='sent',message='Gracias por conversar conmigo.',editResult={body:'¿Guardaste el folleto?'}}={}) {
  const actor={owner:'vendedor-a',email:'a@example.test',session:'directa-a',zone:'America/Chicago',current:async()=>true};
  const config={key:randomBytes(32),pilot:[actor.email]};
  const cal={grant:'acceso-cal-a',calendarId:'tec-a',mailbox:actor.email,availabilityEnabled:true};
  const doc={grant:'acceso-mail-a',mailbox:actor.email,sendEnabled:true,encryptedToken:sealToken('solo-fixture',config.key,actor.owner)};
  const log={reads:[],proposals:[],writes:[],preparedMail:[],delivered:[],compose:[],rewrite:[]};
  const channels=createPlanChannels({config,now:()=>fixed,calendar:{
    getConnection:async owner=>{assert.equal(owner,actor.owner);return cal;},
    availability:async query=>{log.reads.push(query);return busy;},
    saveEvent:async event=>{log.writes.push(event);return {status:'saved'};}
  },followups:{
    prepare:async(binding,data)=>{log.proposals.push({binding,data});return {id:'CAL-1'};},
    confirm:async (binding,id,{saveEvent})=>{
      assert.equal(id,'CAL-1');assert.equal(binding.owner,actor.owner);
      if(outcome==='saved') await saveEvent(log.proposals.at(-1).data);
      return {status:outcome,...log.proposals.at(-1).data};
    }
  },store:{findById(owner){assert.equal(owner,actor.owner);return {lean:async()=>doc};}},gmail:{
    refresh:async token=>{assert.equal(token,'solo-fixture');return {access_token:'memoria'};},
    send:async(token,data)=>{assert.equal(token,'memoria');log.delivered.push(data);return {id:'test'};}
  },drafts:{
    prepare:async data=>{log.preparedMail.push(data);return {id:'MAIL-1'};},
    confirm:async({owner,session,grant,id,deliver})=>{
      assert.deepEqual({owner,session,grant,id},{owner:actor.owner,session:actor.session,grant:doc.grant,id:'MAIL-1'});
      if(outbound==='sent') await deliver(log.preparedMail.at(-1));
      return {status:outbound};
    }
  },compose:async instruction=>{
    log.compose.push(instruction);
    return {to:'cliente@example.test',subject:'Seguimiento',body:message};
  },rewrite:async (body,change)=>{
    log.rewrite.push({body,change});
    return editResult;
  }});
  return {actor,channels,doc,cal,log};
}

test('piloto: referidos, dos horarios y borrador antes de elegir; confirmar solo la cita conserva el correo',async()=>{
  const {actor,channels,log}=fixture();
  const email='agustincalderon286@gmail.com';
  const auth={user:{_id:actor.owner,email},session:{accessMode:'direct',tokenHash:'fixture'}};
  const req={body:{accountId:actor.owner,coachPlanVersion:1,coachTimeZone:actor.zone}};
  const intents=[
    {kind:'plan',operations:['list','appointment','email'],bucket:'today',leadKind:'referrals',schedule:'mañana',slots:true,mailInstruction:'déjame listo el correo'},
    {kind:'revise',operations:[],ordinal:2}
  ];
  let state,reads=0;
  const runner=createConversationPlan({config:{planPilot:[email],sheetsPilot:[email]},now:()=>fixed,readConnections:async()=>({}),
    readSession:async()=>auth,switched:()=>false,pilotInterpret:async()=>intents.shift(),
    plans:{load:async()=>structuredClone(state),save:async(_,old,data)=>{
      assert.equal(old,state?.revision);const revision=randomBytes(12).toString('hex');state={data:structuredClone(data),revision};return revision;
    }},
    sheets:{access:async()=>({grant:'mi-hoja',token:'ficticio'})},sheetTools:{
      search:async(_ctx,q)=>{assert.equal(q.kind,'referrals');reads++;return {total:1,contacts:[{fullName:'Cliente Prueba',key:'1',identityKey:'1',sheetTitle:'4 en 14',row:2,email:'cliente@example.test',reason:'Toca hoy.'}]};},
      revalidate:async()=>true
    },channels:{
      appointment:(_actor,...args)=>channels.appointment(actor,...args),
      email:(_actor,...args)=>channels.email(actor,...args),
      confirmAppointment:(_actor,...args)=>channels.confirmAppointment(actor,...args)
    }});
  const ask=async text=>{
    const result=await runner({req,auth,sessionId:'chat',text});
    req.body.coachPlanPending=result.coachPlanPending;
    return result;
  };
  const first=await ask('Checa quién me falta del cuatro en catorce, búscame dos huecos para mañana y déjame listo el correo');
  assert.equal(reads,1);assert.match(first.respuesta,/Toca hoy/);
  const offered=first.coachHelpActions.filter(btn=>btn.op==='slot');
  assert.equal(offered.length,2);assert.equal(log.proposals.length,0);assert.equal(log.preparedMail.length,1);
  for(const option of offered) assert.ok(log.preparedMail[0].body.includes(option.label));
  assert.ok(!first.coachHelpActions.some(btn=>btn.op==='apply'));
  const second=await ask('Mejor el segundo');
  const confirm=second.coachHelpActions.find(btn=>btn.op==='apply' && btn.arg==='appointment');
  assert.ok(confirm);assert.equal(log.preparedMail.length,2);
  assert.ok(log.preparedMail[1].body.includes(offered[1].label));assert.ok(!log.preparedMail[1].body.includes(offered[0].label));
  assert.equal(log.writes.length,0);
  await ask('TECPLAN '+confirm.revision+' apply appointment');
  assert.equal(log.writes.length,1);assert.equal(log.delivered.length,0);
});

test('después de las cuatro de la tarde busca dos horarios dentro de ese viernes sin preparar ni crear evento',async()=>{
  const {actor,channels,log}=fixture();
  const availability=await channels.appointment(actor,{},{
    name:'Cliente Prueba',schedule:'el viernes después de las cuatro de la tarde',slots:true
  });
  assert.equal(availability.options.length,2);
  assert.equal(availability.whenText,undefined);
  assert.equal(availability.proposal,undefined);
  assert.equal(log.reads[0].start,'2026-11-27T22:01:00.000Z');
  assert.equal(log.reads[0].end,'2026-11-28T06:00:00.000Z');
  assert.equal(log.reads[0].owner,actor.owner);
  assert.equal(log.proposals.length,0);
  assert.equal(log.writes.length,0);
  const picked=availability.options[1];
  const draft=await channels.appointment(actor,availability.state,{name:'Cliente Prueba',schedule:'',slot:picked,slots:true});
  assert.match(draft.whenText,/viernes.*noviembre.*2026.*America\/Chicago/);
  assert.ok(draft.preview.includes(draft.whenText));
  assert.equal(log.proposals[0].data.checkAvailability,true);
  assert.equal(log.proposals[0].data.start,picked.start);
  assert.equal(log.writes.length,0);
  const receipt=await channels.confirmAppointment(actor,draft.proposal);
  assert.equal(receipt.status,'saved');
  assert.equal(log.writes.length,1);
});

test('aclara una hora ambigua de un rango abierto y permite acotarla sin repetir el día',async()=>{
  const {actor,channels,log}=fixture();
  const first=await channels.appointment(actor,{},{name:'Prueba',schedule:'el viernes después de las cuatro',slots:true});
  assert.match(first.question,/mañana o de la tarde/);
  assert.equal(log.reads.length,0);
  const second=await channels.appointment(actor,first.state,{name:'Prueba',schedule:'de la tarde',slots:false});
  assert.equal(second.options.length,2);
  assert.equal(log.reads[0].start,'2026-11-27T22:01:00.000Z');
  await channels.appointment(actor,first.state,{name:'Prueba',schedule:'mañana',slots:false});
  assert.equal(log.reads[1].start,'2026-11-27T10:01:00.000Z');
  await channels.appointment(actor,{},{name:'Prueba',schedule:'el viernes a partir de las 4pm',slots:true});
  assert.equal(log.reads[2].start,'2026-11-27T22:00:00.000Z');
  assert.equal(log.writes.length,0);
});

test('una revisión de la hora conserva el viernes y un conflicto de horario nunca afirma confirmación',async()=>{
  const {actor,channels,log}=fixture({outcome:'slotBusy'});
  const first=await channels.appointment(actor,{},{name:'Prueba',schedule:'viernes a las cuatro de la tarde'});
  const second=await channels.appointment(actor,first.state,{name:'Prueba',schedule:'mejor a las cinco de la tarde'});
  assert.notEqual(first.whenText,second.whenText);
  assert.equal(log.proposals[1].data.start,'2026-11-27T23:00:00.000Z');
  const r=await channels.confirmAppointment(actor,second.proposal);
  assert.equal(r.status,'blocked');
  assert.match(r.message,/ocupó/);
  assert.equal(log.writes.length,0);
});

test('una solicitud de confirmación de cita produce correo neutral con el horario que vio el vendedor',async()=>{
  const {actor,channels,log}=fixture();
  const result=await channels.appointment(actor,{},{name:'Cliente',schedule:'viernes a las cinco de la tarde'});
  const mail=await channels.email(actor,{to:'cliente@example.test',instruction:'para confirmar la visita',whenText:result.whenText});
  assert.equal(log.compose.length,0);
  assert.ok(mail.preview.includes('Horario propuesto: '+result.whenText));
  assert.ok(mail.preview.includes(log.preparedMail[0].body));
  assert.doesNotMatch(log.preparedMail[0].body,/ya (está|quedó)|confirmada/i);
  assert.deepEqual(log.delivered,[]);
  assert.deepEqual(log.writes,[]);
  const sent=await channels.confirmEmail(actor,mail.proposal);
  assert.equal(sent.status,'saved');assert.equal(log.delivered.length,1);
  assert.equal(log.delivered[0].body,log.preparedMail[0].body);
});

test('no se envían datos privados del horario al redactor y un borrador espera permiso o falla de forma explícita',async()=>{
  const {actor,channels,log,doc}=fixture({outbound:'unknown'});
  const whenText='viernes 27 de noviembre de 2026, 5:00 p.m. (America/Chicago)';
  const proposed=await channels.email(actor,{to:'cliente@example.test',instruction:'pregúntale si guardó el folleto',whenText});
  assert.equal(log.compose.length,1);
  assert.ok(!log.compose[0].includes(whenText));
  assert.ok(log.preparedMail[0].body.includes(whenText));
  assert.ok(proposed.preview.includes(log.preparedMail[0].body));
  const result=await channels.confirmEmail(actor,proposed.proposal);
  assert.equal(result.status,'retry');assert.doesNotMatch(result.message,/Gmail confirmó el envío/);
  doc.grant='otro';
  await assert.rejects(channels.confirmEmail(actor,proposed.proposal),/permiso/);
  assert.equal(log.delivered.length,0);
});

test('origen del horario, sesión del vendedor y acceso de Calendar se verifican antes del efecto',async()=>{
  const {actor,channels,log,cal}=fixture({message:'Ya quedó confirmada la cita en tu calendario.'});
  const d=await channels.appointment(actor,{},{name:'Prueba',schedule:'el viernes a las seis de la tarde'});
  const m=await channels.email(actor,{to:'cliente@example.test',instruction:'añade que reserve el folleto',whenText:d.whenText});
  assert.match(m.question,/texto|horario/); assert.ok(!m.proposal);
  actor.current=async()=>false;
  await assert.rejects(channels.confirmAppointment(actor,d.proposal),/sesión/);
  cal.grant='otro grant';
  await assert.rejects(channels.confirmAppointment(actor,d.proposal),/cambió/);
  await assert.rejects(channels.appointment({...actor,email:'otro@example.test'},{},{name:'X',schedule:'viernes'}),/piloto/);
  assert.equal(log.writes.length,0);assert.equal(log.preparedMail.length,0);
});

test('hazlo más corto conserva asunto y cambia la hora al final sin enviar fechas de Calendar al redactor',async()=>{
  const {actor,channels,log}=fixture();
  const to='cliente@example.test';
  const whenOne='viernes de prueba, 5:00 p.m. (America/Chicago)';
  const whenTwo='viernes de prueba, 6:00 p.m. (America/Chicago)';
  const first=await channels.email(actor,{to,instruction:'pregunta si todavía tiene el folleto',whenText:whenOne});
  const second=await channels.email(actor,{to,priorDraft:first.baseDraft,editInstruction:'hazlo más corto',whenText:whenTwo});
  assert.equal(log.compose.length,1);
  assert.equal(log.rewrite.length,1);
  assert.equal(log.rewrite[0].body,first.baseDraft.body);
  assert.ok(log.rewrite[0].change.startsWith('hazlo más corto'));
  assert.doesNotMatch(JSON.stringify(log.rewrite),/America\/Chicago/);
  assert.equal(first.baseDraft.subject,second.baseDraft.subject);
  assert.equal(second.baseDraft.body,'¿Guardaste el folleto?');
  assert.ok(!second.preview.includes(whenOne));
  assert.equal(second.preview.split(whenTwo).length,2);
  assert.ok(second.preview.includes(log.preparedMail[1].body));
  const next=await channels.email(actor,{to,priorDraft:second.baseDraft,whenText:whenOne});
  assert.equal(log.rewrite.length,1);
  assert.ok(next.preview.includes(whenOne));
  assert.ok(!next.preview.includes(whenTwo));
  assert.equal(log.delivered.length,0);
});

test('una corrección ambigua o una hora inventada pide aclaración sin preparar un envío',async()=>{
  const {actor,channels,log}=fixture({editResult:{question:'¿Qué parte quieres cambiar?'},message:'Te veo a las dos para la demo.'});
  const to='cliente@example.test';
  const edit=await channels.email(actor,{to,priorDraft:{subject:'Demo',body:'Hola'},editInstruction:'cambia eso'});
  assert.match(edit.question,/parte/);
  const invented=await channels.email(actor,{to,instruction:'menciona el folleto',whenText:'viernes a las cuatro'});
  assert.match(invented.question,/horario propuesto/);
  assert.equal(log.preparedMail.length,0);
  assert.equal(log.delivered.length,0);
});
