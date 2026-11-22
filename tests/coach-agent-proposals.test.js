// Fase 2 del asistente con ciclo: propuestas que el vendedor confirma con su botón.
// Todo es ficticio: teléfonos 555-01xx y dominios example.invalid.
import test from 'node:test';
import assert from 'node:assert/strict';
import {createAgentTools, agentTools} from '../src/coach-agent-tools.js';
import {createSellerAgent, sellerInstructions} from '../src/coach-agent-seller.js';
import {createConversationPlan} from '../src/coach-conversation-plan.js';
import {createPlanChannels} from '../src/coach-plan-channels.js';
import {createPlanStore} from '../src/coach-plan-store.js';
import fs from 'node:fs';
import vm from 'node:vm';

const ZONE='America/Chicago', NOW=Date.parse('2026-11-23T15:00:00Z');
const owner='000000000000000000000411';
const auth={user:{_id:owner,email:'A@example.invalid',name:'Agustín',accountType:'owner'},session:{tokenHash:'th',accessMode:'direct'}};
const people={
  'id-pedro':{key:'k-pedro',identityKey:'id-pedro',fullName:'Pedro Ficticio',host:'Rosa Prueba',kind:'referrals',row:7,sheetTitle:'4 en 14',phone:'+13125550101',email:'pedro@example.invalid',noCalls:false},
  'id-luis':{key:'k-luis',identityKey:'id-luis',fullName:'Luis Ejemplo',host:'Rosa Prueba',kind:'referrals',row:8,sheetTitle:'4 en 14',noCalls:true}
};

function world({busy=[]}={}) {
  const calls=[];
  const note=(name,args)=>{calls.push([name,args]);};
  const calendar={getConnection:async()=>({grant:'cg',calendarId:'cal',mailbox:'a@example.invalid',availabilityEnabled:true}),
    availability:async range=>{note('availability',range);return busy;},saveEvent:async event=>{note('saveEvent',event);return {id:'ev'};}};
  const followups={prepare:async(binding,draft)=>{note('followupPrepare',draft);return {id:'cita1'};},
    confirm:async(binding,id)=>{note('followupConfirm',id);return {status:'saved',summary:'Cita · Pedro Ficticio',start:'2026-11-27T00:00:00.000Z',timeZone:ZONE};}};
  const drafts={prepare:async draft=>{note('draftPrepare',draft);return {id:'correo1'};}};
  const store={findById:()=>({lean:async()=>({grant:'mg',mailbox:'a@example.invalid',sendEnabled:true})})};
  const config={pilot:['a@example.invalid'],sheetsPilot:['a@example.invalid'],planPilot:['a@example.invalid'],calendarReady:true,ready:true,sheetsReady:true,contactsReady:true,key:Buffer.alloc(32,1)};
  const channels=createPlanChannels({config,calendar,followups,store,gmail:{},drafts,now:()=>NOW,
    compose:async instruction=>{note('compose',instruction);return {to:/a (\S+@\S+) para/.exec(instruction)[1],subject:'Seguimiento',body:'Hola, gracias por recibirnos.'};}});
  const sheets={access:async()=>({token:'t',grant:'sg',spreadsheetId:'s',ticketsSheetId:1,referralsSheetId:2}),current:async()=>true};
  const sheetTools={
    search:async()=>({total:2,contacts:[{...people['id-pedro'],phone:undefined,rawPhone:'(312) 555-0101',rawEmail:'pedro@example.invalid',address:'5 Av. Ficticia, Chicago',bucket:'overdue',date:'2026-11-20',reason:'Seguimiento vencido'},
      {...people['id-luis'],rawPhone:'',rawEmail:'',address:''}]}),
    card:async(ctx,id)=>{note('card',id);if (!people[id]) throw Object.assign(Error('La fila cambió.'),{status:409});return people[id];},
    prepareNote:async(ctx,args)=>{note('prepareNote',args);return {preview:'Pedro Ficticio — 4 en 14, fila 7. Agregar al historial: «2026-11-23: '+args.note+'».',proposal:{binding:'b',id:'id-pedro',entry:args.note,followupDate:args.followupDate}};},
    confirmNote:async(ctx,proposal)=>{note('confirmNote',proposal);return {status:'saved',message:'Google Sheets confirmó el cambio de Pedro Ficticio en 4 en 14, fila 7.'};},
    prepareAdd:async(ctx,args)=>{note('prepareAdd',args);return {preview:'Agregar '+args.fullName+' · '+args.phone+' a 4 en 14 con anfitrión '+args.hostName+'.',proposal:{binding:'b',previewId:'p1'}};},
    revalidate:async()=>true
  };
  const deps={config,calendar,store,gmail:{},syncSales:async()=>null,sheets,sheetTools,contacts:{},readConnections:async()=>({}),channels};
  const actor={owner,session:'s',email:'A@example.invalid',name:'Agustín',zone:ZONE,current:async()=>true,now:()=>NOW};
  return {calls,deps,actor,channels,sheets,sheetTools};
}
const find={nombre:'Pedro',anfitrion:'',notas:'',seguimiento:'',tipo:'',pagina:1};

test('el catálogo declara las propuestas con esquema estricto y las instrucciones prohíben darlas por hechas',()=>{
  for (const name of ['preparar_nota','preparar_cita','preparar_alta','preparar_mensaje']) {
    const tool=agentTools.find(item=>item.name===name);
    assert.equal(tool.parameters.additionalProperties,false);
    assert.deepEqual(tool.parameters.required,Object.keys(tool.parameters.properties));
  }
  const text=sellerInstructions({name:'Agustín',zone:ZONE,now:NOW});
  assert.match(text,/Tú nunca guardas, reservas ni envías/);
  assert.match(text,/preparar_nota/);
  assert.doesNotMatch(text,/solo lees/);
  // Los correos siguen en el flujo existente: su redactor recibe la dirección del destinatario.
  assert.equal(agentTools.some(item=>item.name==='preparar_correo'),false);
  assert.match(agentTools.find(item=>item.name==='pasar_al_coach').description,/un correo/);
});

test('la nota y la cita de una persona quedan en una propuesta con la forma del plan de conversación, sin escribir nada',async()=>{
  const {deps,actor,calls}=world();
  const refs={}, turn={};
  const execute=createAgentTools(deps)(actor,refs,turn);
  const [pedro,luis]=(await execute('buscar_referidos',find)).fichas;
  assert.match(pedro.ref,/^f[a-f0-9]{5}$/);
  assert.notEqual(pedro.ref,luis.ref);
  assert.equal(refs[pedro.ref],'id-pedro');
  // La referencia identifica a la persona: es la misma en otra búsqueda.
  assert.equal((await execute('buscar_referidos',find)).fichas[0].ref,pedro.ref);

  const noted=await execute('preparar_nota',{ref:pedro.ref,nota:' No contestó, volver a marcar ',proximoSeguimiento:'2026-11-26'});
  assert.equal(noted.estado,'preparado');
  assert.deepEqual(calls.find(([name])=>name==='prepareNote')[1],{key:'k-pedro',note:' No contestó, volver a marcar ',followupDate:'2026-11-26',today:'2026-11-23'});
  const booked=await execute('preparar_cita',{ref:pedro.ref,nombre:'',dia:'2026-11-26',hora:'18:00',minutos:60,tipo:'demo'});
  assert.equal(booked.estado,'preparado');
  const draft=calls.find(([name])=>name==='followupPrepare')[1];
  assert.equal(draft.start,'2026-11-27T00:00:00.000Z');
  assert.equal(draft.end,'2026-11-27T01:00:00.000Z');
  assert.equal(draft.summary,'Demo · Pedro Ficticio');
  assert.equal(draft.action,'demo');

  const s=turn.plan.draft;
  assert.equal(turn.plan.changed,true);
  assert.deepEqual(s.ops,['note','appointment']);
  assert.equal(s.steps.note.status,'ready');
  assert.deepEqual(s.steps.appointment.proposal,{id:'cita1',grant:'cg',calendarId:'cal'});
  assert.deepEqual(s.chosen,{key:'k-pedro',identityKey:'id-pedro',fullName:'Pedro Ficticio',host:'Rosa Prueba',kind:'referrals',row:7,sheetTitle:'4 en 14',email:'pedro@example.invalid'});
  assert.equal(s.sheetGrant,'sg');
  assert.equal(s.fields.noteSchedule,'2026-11-26');
  assert.match(s.agentLoop.note,/Nota para Pedro Ficticio: «No contestó, volver a marcar» · próximo seguimiento 2026-11-26/);
  assert.match(s.agentLoop.appointment,/demo con Pedro Ficticio el 2026-11-26 a las 18:00, 60 min/);
  // Ni el teléfono ni las claves internas vuelven al modelo, y nada se confirmó.
  assert.doesNotMatch(JSON.stringify([noted,booked,s.agentLoop]),/555|k-pedro|id-pedro|cita1/);
  assert.equal(calls.some(([name])=>['confirmNote','saveEvent','followupConfirm'].includes(name)),false);

  // Otra persona en el mismo turno, un alta mezclada y referencias inventadas se rechazan.
  assert.match((await execute('preparar_nota',{ref:luis.ref,nota:'x',proximoSeguimiento:''})).nota,/una sola persona/);
  assert.match((await execute('preparar_alta',{nombre:'Nora Prueba',telefono:'312-555-0144',tipo:'referido',anfitrion:'Rosa Prueba'})).nota,/su propia propuesta/);
  for (const ref of ['f00000','r1','__proto__','']) assert.equal((await execute('preparar_nota',{ref,nota:'x',proximoSeguimiento:''})).estado,'peticion_invalida');
  assert.equal((await execute('preparar_nota',{ref:pedro.ref,nota:'x',proximoSeguimiento:'26/11/2026'})).estado,'peticion_invalida');
  assert.deepEqual(s.ops,['note','appointment']);
});

test('un horario ocupado, una cuenta sin Calendar y una cita sin ficha se resuelven sin inventar',async()=>{
  const busy=world({busy:[{start:'2026-11-27T00:00:00.000Z',end:'2026-11-27T01:00:00.000Z'}]});
  const turn={};
  const taken=await createAgentTools(busy.deps)(busy.actor,{},turn)('preparar_cita',{ref:'',nombre:'Nora Prueba',dia:'2026-11-26',hora:'18:00',minutos:30,tipo:'cita'});
  assert.equal(taken.estado,'no_preparado');
  assert.match(taken.motivo,/ya no está libre/);
  assert.equal(turn.plan?.changed || false,false);

  const free=world();
  const state={};
  const execute=createAgentTools(free.deps)(free.actor,{},state);
  const made=await execute('preparar_cita',{ref:'',nombre:'Nora Prueba',dia:'2026-11-27',hora:'09:30',minutos:15,tipo:'llamada'});
  assert.equal(made.estado,'preparado');
  assert.equal(free.calls.find(([name])=>name==='followupPrepare')[1].summary,'Llamada · Nora Prueba');
  assert.equal(state.plan.draft.chosen,undefined);
  assert.equal((await execute('preparar_cita',{ref:'',nombre:'<script>',dia:'2026-11-27',hora:'09:30',minutos:15,tipo:'cita'})).estado,'peticion_invalida');
  assert.equal((await execute('preparar_cita',{ref:'',nombre:'Nora',dia:'2026-11-27',hora:'9:30',minutos:15,tipo:'cita'})).estado,'peticion_invalida');

  const outsider=world();
  outsider.deps.calendar.getConnection=async()=>null;
  const missing=await createAgentTools(outsider.deps)(outsider.actor,{},{})('preparar_cita',{ref:'',nombre:'Nora',dia:'2026-11-27',hora:'09:30',minutos:15,tipo:'cita'});
  assert.equal(missing.estado,'sin_conexion');
  assert.match(missing.nota,/Mis conexiones/);
});

test('el alta y el mensaje de texto se preparan con los datos reales fuera del alcance del modelo',async()=>{
  const {deps,actor,calls}=world();
  const refs={}, turn={};
  const execute=createAgentTools(deps)(actor,refs,turn);

  assert.equal((await execute('preparar_alta',{nombre:'Nora Prueba',telefono:'312-555-0144',tipo:'referido',anfitrion:''})).estado,'peticion_invalida');
  const added=await execute('preparar_alta',{nombre:' Nora Prueba ',telefono:'312-555-0144',tipo:'referido',anfitrion:'Rosa Prueba'});
  assert.equal(added.estado,'preparado');
  assert.deepEqual(calls.find(([name])=>name==='prepareAdd')[1],{kind:'referrals',fullName:'Nora Prueba',phone:'312-555-0144',hostName:'Rosa Prueba'});
  assert.deepEqual(turn.plan.draft.ops,['add']);
  assert.deepEqual(turn.plan.draft.fields,{contact:'Nora Prueba',phone:'312-555-0144',leadKind:'referrals',host:'Rosa Prueba'});

  const [pedro,luis]=(await execute('buscar_referidos',find)).fichas;
  const sms=await execute('preparar_mensaje',{ref:pedro.ref,texto:'Hola Pedro, soy Agustín.  ¿Te marco el jueves?'});
  assert.equal(sms.estado,'preparado');
  assert.deepEqual(turn.messages,[{to:'+13125550101',name:'Pedro Ficticio',text:'Hola Pedro, soy Agustín. ¿Te marco el jueves?'}]);
  assert.doesNotMatch(JSON.stringify(sms),/555/);
  assert.equal((await execute('preparar_mensaje',{ref:luis.ref,texto:'Hola Luis'})).estado,'no_permitido');
  assert.equal(turn.messages.length,1);
});

// --- Asistente y botón de confirmar ------------------------------------------
function planCollection() {
  const docs=new Map();
  return {docs,findById:id=>({lean:async()=>docs.get(id) || null}),
    create:async doc=>{ if (docs.has(doc._id)) throw Object.assign(Error('dup'),{code:11000}); docs.set(doc._id,{...doc}); },
    updateOne:async({_id,revision},{$set})=>{ const doc=docs.get(_id); if (!doc || doc.revision!==revision) return {matchedCount:0}; docs.set(_id,{...doc,...$set}); return {matchedCount:1}; }};
}
function desk({script,prepare=()=>{}}) {
  const w=world();
  prepare(w.deps);
  const plans=createPlanStore({key:w.deps.config.key,store:planCollection(),now:()=>NOW});
  const memory=createPlanStore({key:w.deps.config.key,store:planCollection(),now:()=>NOW});
  const inputs=[];
  const usage={inputTokens:10,cachedInputTokens:0,outputTokens:5,requests:1};
  const run=async options=>{
    inputs.push(options.input);
    const trace=[];
    const call=async(name,args)=>{const result=await options.execute(name,args);trace.push({name,estado:result.estado || 'ok'});return result;};
    const text=await script[Math.min(inputs.length,script.length)-1](call,options);
    return {text,trace,steps:1,usage};
  };
  const agent=createSellerAgent({config:{loopPilot:['a@example.invalid'],agentModel:'gpt-6.2-sol'},bindTools:createAgentTools(w.deps),memory,plans,
    mailContext:async()=>'',inboxToken:()=>'ficha',budget:{allow:async()=>true,charge:async()=>0},consent:{accepted:async()=>new Date(NOW)},
    readSession:async()=>auth,switched:()=>false,run,now:()=>NOW,log:()=>{}});
  const confirm=createConversationPlan({config:w.deps.config,plans,sheets:w.sheets,sheetTools:w.sheetTools,channels:w.channels,
    readSession:async()=>auth,switched:()=>false,now:()=>NOW});
  const request=(text,extra={})=>({req:{body:{coachPlanVersion:1,accountId:owner,coachTimeZone:ZONE,...extra}},auth,sessionId:'conv',text});
  return {...w,inputs,ask:(text,extra)=>agent(request(text,extra)),press:(text,extra)=>confirm(request(text,extra))};
}
const prepareNote=async call=>{
  const {fichas}=await call('buscar_referidos',find);
  await call('preparar_nota',{ref:fichas[0].ref,nota:'No contestó',proximoSeguimiento:'2026-11-26'});
  return 'Te dejé lista la nota de Pedro con seguimiento el jueves.';
};

test('el vendedor recibe la vista previa con su botón y solo ese botón guarda en la hoja',async()=>{
  const {ask,press,calls}=desk({script:[prepareNote]});
  const reply=await ask('pedro no contesto, le marco el jueves');
  assert.equal(reply.coachAgentLoop,true);
  assert.equal(reply.coachPlanAgent,true);
  assert.match(reply.coachPlanPending,/^[a-f0-9]{24}$/);
  assert.match(reply.respuesta,/^Te dejé lista la nota de Pedro/);
  assert.match(reply.respuesta,/Prospecto de la hoja: Pedro Ficticio \(4 en 14, fila 7\)\./);
  assert.match(reply.respuesta,/Nota en Sheets: Pedro Ficticio — 4 en 14, fila 7\. Agregar al historial: «2026-11-23: No contestó»\./);
  assert.match(reply.respuesta,/Nada se ha guardado ni enviado todavía/);
  assert.match(reply.respuesta,/Fuentes consultadas: Google Sheets\.$/);
  assert.deepEqual(reply.coachHelpActions.map(action=>[action.type,action.op,action.arg,action.label]),
    [['planAction','apply','note','Guardar nota'],['planAction','close','0','Cerrar propuesta']]);
  assert.equal(calls.some(([name])=>name==='confirmNote'),false);

  // El botón llega al plan de conversación, que guarda con la propuesta preparada por el asistente.
  const saved=await press('TECPLAN '+reply.coachPlanPending+' apply note',{coachPlanPending:reply.coachPlanPending});
  assert.match(saved.respuesta,/Resultado de tu confirmación/);
  assert.match(saved.respuesta,/✓ Google Sheets confirmó el cambio de Pedro Ficticio/);
  assert.deepEqual(calls.filter(([name])=>name==='confirmNote').map(([,proposal])=>proposal.entry),['No contestó']);
  // Un botón de una versión anterior ya no guarda nada.
  assert.match((await press('TECPLAN '+reply.coachPlanPending+' apply note',{coachPlanPending:reply.coachPlanPending})).respuesta,/cambió o caducó/);
  assert.equal(calls.filter(([name])=>name==='confirmNote').length,1);
});

test('una propuesta abierta sigue disponible tras una lectura y se corrige por conversación sin perder lo demás',async()=>{
  const {ask,press,inputs,calls}=desk({script:[prepareNote,
    async call=>{await call('estado_conexiones',{});return 'Tu hoja está conectada.';},
    async call=>{
      const {fichas}=await call('buscar_referidos',find);
      await call('preparar_cita',{ref:fichas[0].ref,nombre:'',dia:'2026-11-26',hora:'17:00',minutos:30,tipo:'llamada'});
      return 'Agregué la llamada del jueves a las 5.';
    }]});
  const first=await ask('pedro no contesto, le marco el jueves');
  const token=first.coachPlanPending;

  const read=await ask('mi hoja esta conectada?',{coachPlanPending:token});
  assert.equal(read.coachPlanPending,token);
  assert.equal('coachPlanAgent' in read,false);
  const note=inputs[1].find(item=>item.role==='developer');
  assert.match(note.content,/Propuesta abierta que el vendedor todavía no confirma: Nota para Pedro Ficticio: «No contestó» · próximo seguimiento 2026-11-26/);
  assert.doesNotMatch(JSON.stringify(inputs),/555-0101|k-pedro|id-pedro/);

  const revised=await ask('agendame tambien la llamada a las 5',{coachPlanPending:token});
  assert.notEqual(revised.coachPlanPending,token);
  assert.deepEqual(revised.coachHelpActions.filter(action=>action.op==='apply').map(action=>action.arg),['note','appointment']);
  assert.match(revised.respuesta,/Cita personal: Llamada · Pedro Ficticio/);
  assert.match((await press('TECPLAN '+token+' apply note',{coachPlanPending:token})).respuesta,/cambió o caducó/);
  const booked=await press('TECPLAN '+revised.coachPlanPending+' apply appointment',{coachPlanPending:revised.coachPlanPending});
  assert.match(booked.respuesta,/✓ Google Calendar confirmó Cita · Pedro Ficticio/);
  assert.equal(calls.filter(([name])=>name==='followupConfirm').length,1);
  assert.equal(calls.some(([name])=>name==='confirmNote'),false);
});

test('si el modelo da la propuesta por guardada se le corrige, y el mensaje de texto llega con su botón',async()=>{
  const corrected=desk({script:[
    async call=>{await prepareNote(call);return 'Listo, ya guardé la nota de Pedro.';},
    async()=>'La nota de Pedro quedó lista para que la confirmes.']});
  const fixed=await corrected.ask('pedro no contesto');
  assert.match(fixed.respuesta,/^La nota de Pedro quedó lista para que la confirmes\./);
  assert.match(corrected.inputs[1].at(-1).content,/tú no puedes hacerlo/);
  assert.equal(fixed.coachHelpActions[0].arg,'note');

  const stubborn=desk({script:[async call=>{await prepareNote(call);return 'Ya quedó guardada la nota.';}]});
  const neutral=await stubborn.ask('pedro no contesto');
  assert.match(neutral.respuesta,/^Te dejé lista la propuesta\./);
  assert.doesNotMatch(neutral.respuesta,/quedó guardada/);
  assert.equal(neutral.coachHelpActions[0].arg,'note');

  // Sin ninguna herramienta, afirmar que algo quedó agendado no se entrega: el mensaje sigue por el flujo existente.
  const invented=desk({script:[async()=>'Ya agendé tu cita con Pedro para el jueves.']});
  assert.equal(await invented.ask('agenda a pedro el jueves'),null);

  const texting=desk({script:[async call=>{
    const {fichas}=await call('buscar_referidos',find);
    await call('preparar_mensaje',{ref:fichas[0].ref,texto:'Hola Pedro, soy Agustín de Royal Prestige. ¿Te marco el jueves?'});
    return 'Te dejé el mensaje para Pedro.';
  }]});
  const sms=await texting.ask('mandale un mensaje a pedro que le marco el jueves');
  assert.deepEqual(sms.coachHelpActions,[{type:'smsOpen',to:'+13125550101',text:'Hola Pedro, soy Agustín de Royal Prestige. ¿Te marco el jueves?',label:'Abrir Mensajes · Pedro Ficticio'}]);
  assert.match(sms.respuesta,/Mensaje de texto para Pedro Ficticio:\n«Hola Pedro, soy Agustín de Royal Prestige\. ¿Te marco el jueves\?»/);
  assert.equal('coachPlanPending' in sms,false);
});

test('mientras se confirma, o si una confirmación quedó dudosa, el asistente se aparta',async()=>{
  const record={revision:'a'.repeat(24),data:null};
  const agent=createSellerAgent({config:{loopPilot:['a@example.invalid'],agentModel:'m'},bindTools:()=>async()=>({estado:'ok'}),
    memory:{load:async()=>null,save:async()=>'r'},plans:{load:async()=>record,save:async()=>'b'.repeat(24)},mailContext:async()=>'',inboxToken:()=>'',
    budget:{allow:async()=>true,charge:async()=>0},consent:{accepted:async()=>new Date(NOW)},readSession:async()=>auth,switched:()=>false,
    run:async()=>({text:'hola',trace:[],steps:1,usage:{inputTokens:1,cachedInputTokens:0,outputTokens:1}}),now:()=>NOW,log:()=>{}});
  const send=()=>agent({req:{body:{coachPlanVersion:1,accountId:owner,coachTimeZone:ZONE,coachPlanPending:record.revision}},auth,sessionId:'conv',text:'hola'});
  // Una propuesta del flujo existente (sin la marca del asistente) sigue perteneciendo a ese flujo.
  record.data={ops:['note'],steps:{note:{status:'ready'}}};
  assert.equal(await send(),null);
  record.data={ops:['note'],steps:{note:{status:'ready'}},agentLoop:{note:'Nota'},running:{at:NOW}};
  assert.equal(await send(),null);
  record.data={ops:['email'],steps:{email:{status:'retry'}},agentLoop:{email:'Correo'}};
  assert.equal(await send(),null);
  // La propia, sin confirmar, sigue disponible tras una respuesta cualquiera.
  record.data={ops:['note'],steps:{note:{status:'ready'}},agentLoop:{note:'Nota'}};
  assert.equal((await send()).coachPlanPending,record.revision);
  // Ya confirmada, no hay nada que mantener abierto.
  record.data={ops:['note'],steps:{note:{status:'saved'}},agentLoop:{note:'Nota'}};
  assert.equal('coachPlanPending' in await send(),false);
});

test('el chat convierte el mensaje preparado en un enlace que abre Mensajes con el número y el texto',()=>{
  const app=fs.readFileSync(new URL('../public/coach/app.js',import.meta.url),'utf8');
  const element=tag=>({tag,children:[],dataset:{},querySelector:()=>null,get childElementCount() { return this.children.length; },appendChild(item) { this.children.push(item); }});
  const render=vm.runInNewContext(app.slice(app.indexOf('function renderCoachMessageActions'),app.indexOf('function addCoachMessage('))+';renderCoachMessageActions',{document:{createElement:element}});
  const card=element('article');
  render(card,[
    {type:'smsOpen',to:'+13125550101',text:'Hola Pedro & familia, ¿te marco el jueves?',label:'Abrir Mensajes · Pedro Ficticio'},
    {type:'smsOpen',to:'3125550101',text:'Hola'},
    {type:'smsOpen',to:'+13125550101;extra',text:'Hola'},
    {type:'smsOpen',to:'+13125550101',text:'   '},
    {type:'smsOpen',to:'+13125550101',text:'x'.repeat(1001)}]);
  const links=card.children[0].children;
  assert.equal(links.length,1);
  assert.equal(links[0].tag,'a');
  assert.equal(links[0].href,'sms:+13125550101?&body=Hola%20Pedro%20%26%20familia%2C%20%C2%BFte%20marco%20el%20jueves%3F');
  assert.equal(links[0].textContent,'Abrir Mensajes · Pedro Ficticio');
});

test('el chat conserva la ficha de la propuesta tras una lectura del asistente y la retira cuando ya no hay nada pendiente',()=>{
  const app=fs.readFileSync(new URL('../public/coach/app.js',import.meta.url),'utf8');
  const contexts=vm.runInNewContext(app.slice(app.indexOf('function createCoachAgentContexts'),app.indexOf('// Disables offers that a newer reply'))+';createCoachAgentContexts')();
  const send=()=>{const body={accountId:'a',sessionId:'s1',pregunta:'hola'};contexts.attach(body);return body;};
  const loop={coachMailAgent:true,coachAgentLoop:true};
  // Propuesta nueva: su ficha viaja en los mensajes siguientes y los botones de la anterior quedan sin efecto.
  let body=send();
  const made=contexts.record(body,{...loop,coachPlanAgent:true,coachPlanPending:'a'.repeat(24)},[{type:'planAction'}]);
  assert.equal(made.channel,'plan');
  assert.equal(made.contextChanged,true);
  body=send();
  assert.equal(body.coachPlanPending,'a'.repeat(24));
  assert.equal(body.coachQuestionFocus,'plan');
  // Una lectura que devuelve la misma ficha la conserva; también conserva la del correo.
  const read=contexts.record(body,{...loop,coachPlanPending:'a'.repeat(24)},[]);
  assert.equal(read.retiredPlan,false);
  assert.equal(send().coachPlanPending,'a'.repeat(24));
  // Sin la ficha en la respuesta, la propuesta se retira.
  assert.equal(contexts.record(send(),{...loop},[]).retiredPlan,true);
  assert.equal('coachPlanPending' in send(),false);
});

test('el teléfono y la dirección de una ficha llegan al chat como enlaces, con un botón para llamar a la persona',async()=>{
  const saved=[];
  const {ask,inputs}=desk({script:[async call=>{
    const {fichas}=await call('buscar_referidos',find);
    saved.push(fichas[0]);
    return 'Pedro Ficticio vive en '+fichas[0].direccion+' y su teléfono es '+fichas[0].telefono+'. Luis pide no ser llamado.';
  },async()=>'Su teléfono es '+saved[0].telefono+'.']});
  const reply=await ask('dame el telefono y la direccion de pedro');
  assert.match(reply.respuesta,/^Pedro Ficticio vive en 5 Av\. Ficticia, Chicago y su teléfono es \(312\) 555-0101\./);
  assert.deepEqual(reply.coachLinks,[{kind:'map',text:'5 Av. Ficticia, Chicago'},{kind:'tel',text:'(312) 555-0101',to:'+13125550101'}]);
  assert.deepEqual(reply.coachHelpActions,[{type:'callOpen',to:'+13125550101',label:'Llamar a Pedro Ficticio'}]);
  // En el turno siguiente el marcador recordado conserva el nombre para el botón, y el modelo sigue sin ver el dato.
  const later=await ask('repiteme su telefono');
  assert.deepEqual(later.coachHelpActions,[{type:'callOpen',to:'+13125550101',label:'Llamar a Pedro Ficticio'}]);
  assert.doesNotMatch(JSON.stringify(inputs),/555-0101|Av\. Ficticia/);
});

test('el chat vuelve presionables los teléfonos y las direcciones sin interpretar el texto como HTML',()=>{
  const app=fs.readFileSync(new URL('../public/coach/app.js',import.meta.url),'utf8');
  const node=tag=>({tag,children:[],set textContent(value) { this.text=value; this.children=[]; },get textContent() { return this.text; },appendChild(item) { this.children.push(item); }});
  const document={createElement:node,createTextNode:text=>({tag:'#text',text})};
  const render=vm.runInNewContext(app.slice(app.indexOf('const coachPhonePattern'),app.indexOf('function addCoachMessage('))+';renderCoachMessageText',{document});
  const shape=paragraph=>paragraph.children.map(child=>child.tag==='a' ? [child.text,child.href,child.target || ''] : child.text);

  const p=node('p');
  render(p,'Lidia vive en 12 Calle Ficticia, Blue Island. Tel: 3125550131 o (312) 555-0132. Pedido 4455667788, <b>hoy</b>.',
    [{kind:'map',text:'12 Calle Ficticia, Blue Island'},{kind:'tel',text:'3125550131',to:'+13125550131'},
      {kind:'tel',text:'Tel',to:'javascript:alert(1)'},{kind:'map',text:'<b>'},{kind:'web',text:'Lidia',href:'https://example.invalid'}]);
  assert.deepEqual(shape(p),['Lidia vive en ',
    ['12 Calle Ficticia, Blue Island','https://www.google.com/maps/search/?api=1&query=12%20Calle%20Ficticia%2C%20Blue%20Island','_blank'],
    '. Tel: ',['3125550131','tel:+13125550131',''],' o ',['(312) 555-0132','tel:+13125550132',''],'. Pedido 4455667788, <b>hoy</b>.']);

  // Formatos comunes de teléfono; fechas, pedidos y números sueltos no se tocan.
  const formats=node('p');
  render(formats,'312-555-0101, 312.555.0102, +1 312 555 0103, 1-312-555-0104 y +523312345678; el 2026-11-23 a las 18:00, orden 12345678901.');
  assert.deepEqual(formats.children.filter(child=>child.tag==='a').map(child=>child.href),
    ['tel:+13125550101','tel:+13125550102','tel:+13125550103','tel:+13125550104','tel:+523312345678']);
  const plain=node('p');
  render(plain,'Mañana tienes una demo a las 4.');
  assert.equal(plain.text,'Mañana tienes una demo a las 4.');
  assert.equal(plain.children.length,0);

  const card={children:[],dataset:{},querySelector:()=>null,get childElementCount() { return this.children.length; },appendChild(item) { this.children.push(item); }};
  const element=tag=>({tag,children:[],dataset:{},get childElementCount() { return this.children.length; },appendChild(item) { this.children.push(item); }});
  const actions=vm.runInNewContext(app.slice(app.indexOf('function renderCoachMessageActions'),app.indexOf('function createCoachAgentContexts'))+';renderCoachMessageActions',{document:{createElement:element}});
  actions(card,[{type:'callOpen',to:'+13125550101',label:'Llamar a Pedro Ficticio'},{type:'callOpen',to:'312-555-0101'}]);
  assert.deepEqual(card.children[0].children.map(link=>[link.href,link.textContent]),[['tel:+13125550101','Llamar a Pedro Ficticio']]);
});

test('los leads por distancia se ordenan por minutos de manejo reales y el tiempo llega al vendedor, no al modelo',async()=>{
  const {deps,actor}=world();
  const fichas=[
    {identityKey:'id-a',fullName:'Ana Lejos',host:'Rosa',kind:'referrals',sheetTitle:'4 en 14',row:4,address:'900 Calle Larga, Chicago',rawPhone:'312-555-0111',rawEmail:'',noCalls:false,zoneIndex:1},
    {identityKey:'id-b',fullName:'Beto Cerca',host:'Rosa',kind:'referrals',sheetTitle:'4 en 14',row:5,address:'12 Calle Corta, Blue Island',rawPhone:'312-555-0112',rawEmail:'',noCalls:false,zoneIndex:0},
    {identityKey:'id-c',fullName:'Caro Sin Calle',host:'Rosa',kind:'referrals',sheetTitle:'4 en 14',row:6,address:'Blue Island',rawPhone:'',rawEmail:'',noCalls:false,zoneIndex:0},
    {identityKey:'id-d',fullName:'Dora Dudosa',host:'Rosa',kind:'referrals',sheetTitle:'4 en 14',row:7,address:'1 Calle Rara, Harvey',rawPhone:'',rawEmail:'',noCalls:false,zoneIndex:1}];
  const asked=[];
  deps.sheetTools.byZone=async(ctx,args)=>{asked.push(['byZone',args]);return {total:9,perZone:[2,7],contacts:fichas};};
  const minutes={'900 Calle Larga, Chicago':34,'12 Calle Corta, Blue Island':6};
  deps.routes={enabled:true,minutes:async(origin,destination,departure)=>{
    asked.push(['minutes',origin,destination,departure]);
    if (!minutes[destination]) throw Object.assign(Error('Ubicación por verificar.'),{partialAddress:true});
    return minutes[destination];
  }};
  const turn={};
  const execute=createAgentTools(deps)(actor,{},turn);
  const near=await execute('leads_por_distancia',{origen:' Blue Island, IL ',zonas:['Blue Island','Chicago'],tipo:'referidos',maximo:5});
  assert.deepEqual(asked[0],['byZone',{zones:['Blue Island','Chicago'],today:'2026-11-23',kind:'referrals',limit:8}]);
  // La ficha sin calle no se mide; la que Google no reconoce queda sin medir.
  assert.deepEqual(asked.slice(1).map(call=>call.slice(1,3)),[['Blue Island, IL','900 Calle Larga, Chicago'],['Blue Island, IL','12 Calle Corta, Blue Island'],['Blue Island, IL','1 Calle Rara, Harvey']]);
  assert.equal(asked[1][3],NOW);
  assert.deepEqual(near.fichas.map(ficha=>ficha.nombre),['Beto Cerca','Ana Lejos']);
  assert.deepEqual([near.medidas,near.sinMedir,near.fichasEnEsasZonas],[2,2,9]);
  assert.match(near.fichas[0].manejo,/^\[\[min:[a-f0-9]{6}\]\]$/);
  assert.equal(turn.secrets[near.fichas[0].manejo.slice(2,-2)],'6 min en auto');
  assert.equal(turn.secrets[near.fichas[1].manejo.slice(2,-2)],'34 min en auto');
  assert.doesNotMatch(JSON.stringify(near),/555|Calle|6 min|34 min/);

  // Sin Google Maps activo, con el cupo agotado o sin direcciones reconocibles, el asistente recurre a la cercanía por ciudad.
  deps.routes={enabled:false,minutes:async()=>{throw Error('no debe llamarse');}};
  const off=await createAgentTools(deps)(actor,{},{})('leads_por_distancia',{origen:'Blue Island, IL',zonas:['Blue Island'],tipo:'',maximo:5});
  assert.equal(off.estado,'no_disponible');
  assert.match(off.nota,/leads_por_zona/);
  deps.routes={enabled:true,minutes:async()=>{throw Object.assign(Error('Cupo'),{freeAllowance:true});}};
  assert.match((await createAgentTools(deps)(actor,{},{})('leads_por_distancia',{origen:'Blue Island, IL',zonas:['Blue Island'],tipo:'',maximo:5})).nota,/cupo gratuito/);
  deps.routes={enabled:true,minutes:async()=>{throw Object.assign(Error('Ubicación'),{partialAddress:true});}};
  assert.match((await createAgentTools(deps)(actor,{},{})('leads_por_distancia',{origen:'por aquí',zonas:['Blue Island'],tipo:'',maximo:5})).nota,/cruce de calles/);
  assert.equal((await execute('leads_por_distancia',{origen:'',zonas:['Blue Island'],tipo:'',maximo:5})).estado,'peticion_invalida');
  assert.equal((await execute('leads_por_distancia',{origen:'Blue Island, IL',zonas:[],tipo:'',maximo:5})).estado,'peticion_invalida');
});

test('el vendedor ve los minutos de manejo junto a cada lead y la fuente Google Maps',async()=>{
  const d=desk({prepare:deps=>{
    deps.sheetTools.byZone=async()=>({total:1,perZone:[1],contacts:[{identityKey:'id-b',fullName:'Beto Cerca',host:'Rosa',kind:'referrals',sheetTitle:'4 en 14',row:5,
      address:'12 Calle Corta, Blue Island',rawPhone:'312-555-0112',rawEmail:'',noCalls:false,zoneIndex:0}]});
    deps.routes={enabled:true,minutes:async()=>6};
  },script:[async call=>{
    const {fichas}=await call('leads_por_distancia',{origen:'Blue Island, IL',zonas:['Blue Island'],tipo:'',maximo:5});
    return 'El más cercano es '+fichas[0].nombre+', a '+fichas[0].manejo+'. Su teléfono: '+fichas[0].telefono+'.';
  }]});
  const reply=await d.ask('estoy en blue island, dame el lead mas cercano');
  assert.match(reply.respuesta,/^El más cercano es Beto Cerca, a 6 min en auto\. Su teléfono: 312-555-0112\./);
  assert.match(reply.respuesta,/Fuentes consultadas: Google Maps\.$/);
  assert.deepEqual(reply.coachHelpActions,[{type:'callOpen',to:'+13125550112',label:'Llamar a Beto Cerca'}]);
  const text=sellerInstructions({name:'Agustín',zone:ZONE,now:NOW});
  assert.match(text,/leads_por_distancia/);
});
