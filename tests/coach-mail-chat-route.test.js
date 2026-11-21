import assert from 'node:assert/strict';
import { randomBytes } from 'node:crypto';
import { readFileSync } from 'node:fs';
import test from 'node:test';
import vm from 'node:vm';
import { connectionConfig, registerCoachConnections } from '../src/coach-connections.js';
import { sealToken } from '../src/coach-hycite-gmail.js';
import { scopeCoachChat } from '../src/tec-security.js';

const server = readFileSync(new URL('../server.js', import.meta.url), 'utf8');
function section(start, end) {
  const first = server.indexOf(start), last = server.indexOf(end, first + 1);
  assert.ok(first >= 0 && last > first, 'Se espera encontrar la ruta o función de producción ' + start);
  return server.slice(first, last);
}

const chat = section('app.post("/chat",', '// Central Express 4 async error boundary');
const mode = section('function normalizarModoChat(', 'function limpiarUrlExterna(');
const streaming = section('function prepararRespuestaStreamingChat(', 'async function consumirStreamingOpenAiChat(');
const user = { _id:'test-seller', email:'vendedor@example.invalid' };
const base = { accountId:user._id, sessionId:'session-mail-123', mode:'coach', pregunta:'Revisa mis correos' };

function harness(extraConnections = {}) {
  const key = randomBytes(32);
  const config = { ...connectionConfig({COACH_CONNECTIONS_PILOT_EMAILS:user.email}), key, ready:true, calendarReady:true };
  const doc = { _id:user._id, mailbox:'piloto@example.invalid', grant:'test-grant',
    encryptedToken:sealToken('personal-refresh-token',key,user._id) };
  const recorded = { db:[], gmail:[], usage:[], model:[], scopes:[] };
  const requireCoachActivo = async (req,res) => {
    if (req.anonymous) {
      res.status(401).json({ error:'Sesión requerida' });
      return null;
    }
    return { user:{...user,email:req.loginEmail || user.email},session:{tokenHash:'dummy-session-hash'} };
  };
  let route;
  const app = {
    get(){},
    post(path, handler) { if (path === '/chat') route=handler; }
  };
  const agent = registerCoachConnections(app,{
    config, requireActive:requireCoachActivo, switched:req => req.switched,
    store:{findById(id) { recorded.db.push(id); return {lean:async()=>String(id)===doc._id?doc:null}; }},
    gmail:{
      refresh:async token=>{ assert.equal(token,'personal-refresh-token');return {access_token:'seller-access'}; },
      inbox:async(token,opts)=>{
        recorded.gmail.push({token,opts});
        return [{id:'abcd1234',subject:'Asunto confidencial del piloto',from:'cliente@example.invalid',
          snippet:'Texto privado de prueba',date:'2026-11-20T12:34:56Z'}];
      }
    },
    ...(typeof extraConnections === 'function' ? extraConnections({key,doc,recorded}) : extraConnections)
  });
  // This marker stops the normal model path at its first call. Anything else unexpected is a test failure.
  const normalChat = new Error('Entered normal chat');
  const inert = () => null;
  const context = {
    app, requireCoachActivo, scopeCoachChat,
    limpiarCoachHealthSurveyActiveContext:inert, limpiarCoachProgram414ActiveContext:inert,
    limpiarCoachDecisionContext:inert, limpiarCoachOrderCalcContext:inert, limpiarCoachDemoOutcomeContext:inert,
    asegurarSesionRedisCargada:inert, obtenerCoachContextoSesion:inert, validarLimiteUsoCoach:async()=>({allowed:true}),
    validarLimiteUsoChef:async()=>({allowed:true}), marcarSesionActiva:inert, limpiarMemoriaSesiones:inert,
    runCoachConnectedAgent:args=>{recorded.scopes.push(args.sessionId);return agent(args);},
    guardarMensajeRaw:async row=>recorded.usage.push(row),
    construirContextoModoPrompt:mode=>{recorded.model.push(mode); throw normalChat;},
    console:{error:err=>{ if (err!==normalChat) throw err; }}
  };
  vm.runInNewContext(mode + streaming + chat, context);
  async function request(body, extras = {}) {
    const res = {
      headers:{}, chunks:[], statusCode:200,
      setHeader(k,v){this.headers[k.toLowerCase()]=v;},
      status(n){this.statusCode=n;return this;}, json(value){this.data=JSON.parse(JSON.stringify(value));},
      write(chunk){this.chunks.push(chunk);}, end(){this.writableEnded=true;}
    };
    await route({...extras,body},res);
    return res;
  }
  return {recorded,request};
}

for (const stream of [false,true]) {
  test('el chat consulta Gmail del vendedor en ' + (stream?'NDJSON':'JSON') + ' sin enviar el correo al modelo', async () => {
    const {request,recorded}=harness();
    const result=await request({...base,stream,pregunta:'Revisa mis correos sin leer'});
    assert.equal(result.statusCode,200);
    let reply=result.data;
    if (stream) {
      assert.match(result.headers['content-type'],/application\/x-ndjson/);
      assert.equal(result.writableEnded,true);
      const events=result.chunks.map(chunk=>JSON.parse(chunk));
      assert.deepEqual(events.map(e=>e.type),['delta','done']);
      reply=events[1].payload;
      assert.equal(events[0].content,reply.respuesta);
    } else {
      assert.deepEqual(result.chunks,[]);
    }
    assert.equal(reply.coachMailAgent,true);
    assert.match(reply.respuesta,/Asunto confidencial del piloto/);
    assert.deepEqual(reply.coachHelpActions,[{type:'mailSource',mailbox:'piloto@example.invalid',id:'abcd1234',label:'Abrir correo 1'}]);
    assert.deepEqual(recorded.gmail,[{token:'seller-access',opts:{unread:true}}]);
    assert.deepEqual(recorded.scopes,[scopeCoachChat(user._id,base.sessionId)]);
    assert.deepEqual(recorded.model,[]);
    assert.equal(recorded.usage.length,1);
    assert.doesNotMatch(JSON.stringify(recorded.usage),/sin leer|Asunto confidencial|Texto privado|cliente@example|seller-access|personal-refresh/);
    assert.deepEqual([...new Set(recorded.db)],[user._id]);
  });
}

test('el chat cambia de la bandeja a Calendar, muestra la propuesta y conserva la privacidad en streaming',async()=>{
  let previews=0;
  const {request,recorded}=harness({
    calendarStore:{findById(owner){assert.equal(owner,user._id);return {lean:async()=>({grant:'calGrant',calendarId:'personal',mailbox:user.email})};}},
    followups:{prepare:async(binding,event)=>{assert.equal(binding.owner,user._id);assert.equal(event.leadId,'mycrm');previews++;return {id:'A0B1C2D3E4F5'};}},
    findLeads:async({owner,name})=>{assert.equal(owner,user._id);assert.equal(name,'María');return [{id:'mycrm',name:'María',nextActionAt:null}];}
  });
  const inbox=await request(base);
  const preview=await request({...base,stream:true,coachTimeZone:'America/Chicago',coachCalendarVersion:1,
    coachMailPending:inbox.data.coachMailPending,pregunta:'Agenda una llamada con María mañana a las 4 pm'});
  const result=preview.chunks.map(line=>JSON.parse(line)).at(-1).payload;
  assert.equal(result.coachCalendarAgent,true);assert.equal(previews,1);
  assert.match(result.respuesta,/María/);
  assert.equal(result.coachHelpActions[0].type,'calendarConfirm');
  assert.equal(recorded.model.length,0);assert.equal(recorded.usage.length,2);
  assert.doesNotMatch(JSON.stringify(recorded.usage),/María|personal-refresh|calGrant/);
});

test('el agente exige sesión propia, participación en el piloto y cuenta activa en la pestaña', async () => {
  const {request,recorded}=harness();
  assert.equal((await request(base,{anonymous:true})).statusCode,401);
  assert.equal((await request({...base,accountId:'stale-account'})).statusCode,403);
  assert.deepEqual(recorded.scopes,[]);
  for (const [body,extras,reason] of [
    [{...base,accountId:undefined},{},/propia cuenta/],
    [base,{switched:true},/propia cuenta/],
    [base,{loginEmail:'outside@example.invalid'},/piloto/]
  ]) {
    const {data}=await request(body,extras);
    assert.equal(data.coachMailAgent,true);
    assert.match(data.respuesta,reason);
  }
  assert.deepEqual(recorded.db,[]); assert.deepEqual(recorded.gmail,[]); assert.deepEqual(recorded.model,[]);
});

test('una búsqueda de horarios desde el chat dirige al vendedor al permiso opcional sin consultar Gmail ni al modelo',async()=>{
  const {request,recorded}=harness({
    calendarStore:{findById(id){assert.equal(id,user._id);return {lean:async()=>({calendarId:'tec',grant:'own',mailbox:user.email})};}}
  });
  const result=await request({...base,pregunta:'Búscame dos espacios de 45 minutos mañana para una demo',coachTimeZone:'America/Chicago'});
  assert.equal(result.data.coachCalendarAgent,true);
  assert.match(result.data.respuesta,/disponibilidad/);
  assert.equal(result.data.coachHelpActions[0].type,'connections');
  assert.deepEqual(recorded.model,[]);
  assert.deepEqual(recorded.gmail,[]);
});

test('el Chef público omite el agente privado y una pregunta normal del Coach sigue a su ruta habitual', async () => {
  const {request,recorded}=harness();
  await request({...base,mode:'chef'}, {anonymous:true});
  assert.deepEqual(recorded.scopes,[]);
  await request({...base,pregunta:'¿Cómo explico la garantía?'});
  assert.deepEqual(recorded.model,['chef','coach']);
  assert.deepEqual(recorded.db,[]); assert.deepEqual(recorded.gmail,[]);
  assert.deepEqual(recorded.usage,[]);
});

test('un WhatsApp desde /chat no consulta el Gmail previo ni publica el destino o texto en historial',async()=>{
  let prepared;
  const {recorded,request}=harness({
    waProfile:{load:async owner=>{assert.equal(owner,user._id);return '+13125550109';}},
    waDrafts:{prepare:async fields=>{prepared=fields;return {...fields,id:'A0B1C2D3E4F5',status:'pending'};}}
  });
  const inbox=await request(base);
  const body={...base,pregunta:'Prepara un WhatsApp para +13125550101. Mensaje: Puedo pasar el jueves.',coachMailPending:inbox.data.coachMailPending,stream:true};
  const response=await request(body);
  const answer=response.chunks.map(line=>JSON.parse(line)).at(-1).payload;
  assert.equal(answer.coachWhatsAppAgent,true);
  assert.equal(prepared.owner,user._id);
  assert.equal(prepared.to,'+13125550101');
  assert.equal(prepared.senderPhone,'+13125550109');
  assert.match(answer.respuesta,/número de referencia: \+13125550109/i);
  assert.match(answer.respuesta,/Puedo pasar el jueves/);
  assert.ok(answer.coachHelpActions.some(a=>a.type==='waPersonalConfirm'));
  assert.ok(answer.coachHelpActions.every(a=>a.type!=='waOpen'));
  assert.equal(recorded.gmail.length,1);
  assert.equal(recorded.model.length,0);
  assert.doesNotMatch(JSON.stringify(recorded.usage),/3125550101|3125550109|pasar el jueves/);
});

test('el chat resuelve un contacto, corrige el WhatsApp y permite cambiar de canal',async()=>{
  const contacts=['Rosa Luna','Rosa Vega'];
  const lookups=[];
  let draft;
  const {request,recorded}=harness({
    findLeads:async query=>{
      lookups.push(query);
      return contacts.filter(name=>name.includes(query.name)).map(name=>({name,phone:name==='Rosa Vega'?'+13125550102':'+13125550101'}));
    },
    waDrafts:{
      prepare:async fields=>(draft={...fields,id:'A0B1C2D3E4F5',status:'pending'}),
      view:async()=>draft,
      revise:async({body})=>(draft={...draft,body})
    }
  });
  const turn=async(pregunta,previous)=> (await request({...base,pregunta,coachWhatsAppPending:previous?.coachWhatsAppPending})).data;
  const choices=await turn('Prepara un WhatsApp para Rosa. Mensaje: ¿Nos vemos mañana?');
  assert.match(choices.respuesta,/Rosa Luna.*Rosa Vega/);
  assert.equal(draft,undefined);
  const preview=await turn('Rosa Vega',choices);
  assert.equal(preview.coachWhatsAppAgent,true);
  assert.equal(draft.to,'+13125550102');
  assert.deepEqual(lookups, ['Rosa','Rosa Vega'].map(name=>({owner:user._id,name,includePhone:true})));
  const revision=await turn('Mensaje: ¿Te viene bien el viernes?',preview);
  assert.equal(draft.body,'¿Te viene bien el viernes?');
  assert.equal(revision.coachWhatsAppAgent,true);
  assert.ok(revision.coachHelpActions.every(a=>a.type!=='waOpen'));
  const agenda=await turn('Agenda una llamada con Rosa mañana',revision);
  assert.equal(agenda.coachCalendarAgent,true);
  const inbox=await turn('Revisa mis correos',revision);
  assert.equal(inbox.coachMailAgent,true);
  assert.equal(inbox.coachWhatsAppAgent,undefined);
  assert.equal(recorded.gmail.length,1);
  assert.doesNotMatch(JSON.stringify(recorded.usage),/Rosa|3125550102|el viernes/);
});

test('el chat conserva la bandeja y corrige la respuesta al hilo después de consultar horarios',async()=>{
  let draft, writes=0;
  const {request,recorded}=harness(({key,doc})=>{
    doc.sendEnabled=true;
    const cal={_id:user._id,mailbox:user.email,grant:'cal-grant',calendarId:'cal-demo',availabilityEnabled:true,
      encryptedToken:sealToken('refresh-calendar',key,'calendar-refresh:'+user._id)};
    const row=()=>({lean:async()=>cal});
    return {
      calendarStore:{findById:row,findOne:row,exists:async()=>true},
      calendarGoogle:{refresh:async()=>({access_token:'calendar-access'}),availability:async()=>[]},
      gmail:{
        refresh:async()=>({access_token:'mail-access'}),
        inbox:async()=>[{id:'a12',from:'cliente@example.invalid',date:'2026-01-01T12:00:00Z',subject:'Demo'}],
        replyTarget:async(_,id)=>{assert.equal(id,'a12');return {to:'cliente@example.invalid',subject:'Demo',
          thread:{threadId:'hilo-a',parent:'<msg@example.invalid>',references:['<msg@example.invalid>']}};},
        send:async()=>{writes++;return {id:'unexpected'};}
      },
      drafts:{
        prepare:async input=>(draft={...input,id:'ABCDEF123456',status:'pending'}),
        view:async()=>draft,
        revise:async({changes})=>(draft={...draft,...changes,id:'123456ABCDEF'})
      }
    };
  });
  const client=readFileSync(new URL('../public/coach/app.js',import.meta.url),'utf8');
  const contexts=vm.runInNewContext(client.slice(client.indexOf('function createCoachAgentContexts()'),client.indexOf('function retireCoachOffers('))+';createCoachAgentContexts()');
  async function ask(pregunta) {
    const body={...base,pregunta,coachTimeZone:'America/Chicago'};
    contexts.attach(body);
    const result=(await request(body)).data;
    contexts.record(body,result,result.coachHelpActions);
    return result;
  }
  await ask('Revisa mis correos');
  assert.ok((await ask('Busca dos horarios libres mañana para la demo')).coachCalendarPending);
  assert.match((await ask('Responde al primero')).respuesta,/qué quieres decirle/i);
  assert.match((await ask('Mensaje: Nos vemos el jueves.')).respuesta,/cliente@example.invalid/);
  assert.ok((await ask('Busca dos horarios libres el viernes')).coachCalendarPending);
  const edited=await ask('cambia jueves por lunes');
  assert.match(edited.respuesta,/Nos vemos el lunes/);
  assert.equal(edited.coachHelpActions.find(a=>a.type==='mailConfirm').id,'123456ABCDEF');
  assert.equal(draft.thread.threadId,'hilo-a');
  assert.equal(writes,0);
  assert.equal(recorded.model.length,0);
});

test('desde /chat el borrador ofrece exactamente los horarios que el vendedor vio en Calendar',async()=>{
  let offered,occupied=false;
  const {request,recorded}=harness(({key,doc})=>{
    doc.sendEnabled=true; doc.mailbox=user.email;
    const cal={_id:user._id,mailbox:user.email,grant:'cal-a',calendarId:'cal-demo',availabilityEnabled:true,
      encryptedToken:sealToken('calendar',key,'calendar-refresh:'+user._id)};
    const row=()=>({lean:async()=>cal});
    return {
      calendarStore:{findById:row,findOne:row,exists:async()=>true},
      calendarGoogle:{refresh:async()=>({access_token:'calendar'}),availability:async()=>occupied?[offered.offer.slots[0]]:[]},
      gmail:{
        refresh:async()=>({access_token:'mail'}),
        inbox:async()=>[{id:'abc',from:'ana@example.test',date:'2026-01-01T12:00:00Z',subject:'Demo'}],
        replyTarget:async()=>({to:'ana@example.test',subject:'Demo',thread:{threadId:'def'}}),
        thread:async()=>[{id:'abc',from:'ana@example.test',sent:false,replyTo:'',snippet:'Me gustaría una demo.'}]
      },
      offers:{newId:()=> '1'.repeat(32),isId:()=>false,isCode:()=>false},findLeads:async()=>[],
      drafts:{prepare:async input=>(offered={...input,id:'ABCDEF123456'})}
    };
  });
  const inbox=(await request({...base})).data;
  const slots=(await request({...base,pregunta:'Busca dos horarios libres mañana por la tarde',coachTimeZone:'America/Chicago',
    coachMailPending:inbox.coachMailPending})).data;
  const body={...base,pregunta:'Responde al primero con esos dos horarios',coachTimeZone:'America/Chicago',
    coachMailPending:inbox.coachMailPending,coachCalendarPending:slots.coachCalendarPending,coachQuestionFocus:'calendar'};
  const draft=(await request(body)).data;
  assert.equal(draft.coachOfferAgent,true,draft.respuesta);
  assert.equal(offered.to,'ana@example.test');
  assert.equal(offered.thread.threadId,'def');
  const clock=new Intl.DateTimeFormat('es-US',{timeZone:'America/Chicago',hour:'2-digit',minute:'2-digit',hourCycle:'h23'});
  assert.deepEqual(offered.offer.slots.map(s=>'Elegir '+clock.format(new Date(s.start))),slots.coachHelpActions.map(a=>a.label));
  assert.equal(draft.coachHelpActions[0].type,'mailConfirm');
  occupied=true;
  assert.match((await request(body)).data.respuesta,/se ocupó/);
  assert.equal(recorded.model.length,0);
});
