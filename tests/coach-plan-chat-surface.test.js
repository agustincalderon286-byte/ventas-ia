import test from 'node:test';
import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import {randomBytes} from 'node:crypto';
import vm from 'node:vm';
import {registerCoachConnections,connectionConfig} from '../src/coach-connections.js';
import {scopeCoachChat} from '../src/tec-security.js';

const file=name=>readFileSync(new URL('../'+name,import.meta.url),'utf8');
const server=file('server.js'), frontend=file('public/coach/app.js');
function part(source,start,end) {
  const a=source.indexOf(start), b=source.indexOf(end,a+start.length);
  assert.ok(a>=0 && b>a);
  return source.slice(a,b);
}
const participants=['agustin','rigoberto'];
const authFor=(id)=>({user:{_id:id,email:id+'@example.invalid',name:id},session:{accessMode:'direct',tokenHash:'login-'+id}});
const mixed='Mira la ficha de Julia, pon que le marqué y prepárame un correo para mandarle el catálogo';
const literal='Mensaje: mejor dile que ya tengo el catálogo';
const selective='Solo guarda la nota, el correo déjalo para después';
const group='Quién tengo pendiente sin fecha';
const fixtureIntents=new Map([
  [mixed,{kind:'plan',operations:['note','email'],contact:'Julia',note:'le marqué',mailInstruction:'mandarle el catálogo',sendEmail:true}],
  [literal,{kind:'revise',operations:[],mailInstruction:'mejor dile que ya tengo el catálogo'}],
  [selective,{kind:'select',operations:[],select:['note']}],
  [group,{kind:'plan',operations:['list'],bucket:'undated'}]
]);
function chat() {
  const records=new Map(), calls={interpret:[],writes:[],audit:[],legacyMail:[]};
  const readSession=req=>authFor(req.login);
  const plans={
    load:async scope=>structuredClone(records.get(scope)),
    save:async(scope,previous,data)=>{
      if(records.get(scope)?.revision!==previous) return null;
      const revision=randomBytes(12).toString('hex');
      records.set(scope,{revision,data:structuredClone(data)});
      return revision;
    }
  };
  const sheetTools={
    search:async(ctx,{bucket,offset})=>{
      const size=bucket?19:1;
      return {total:size,contacts:Array.from({length:size},(_,i)=>({
        fullName:bucket?ctx.owner+' persona '+(i+1):'Julia',sheetTitle:'BOLETAS NUEVAS',row:i+2,
        email:'julia@example.invalid',identityKey:ctx.owner+'-id',key:ctx.owner+'-row'
      })).slice(offset,offset+12)};
    },
    prepareNote:async(ctx,{note,key})=>{assert.equal(key,ctx.owner+'-row');return {preview:note,proposal:{note}};},
    revalidate:async(ctx,key)=>assert.equal(key,ctx.owner+'-id'),
    confirmNote:async(ctx,{note})=>{calls.writes.push(['note',ctx.owner,note]);return {status:'saved',message:'Nota guardada'};}
  };
  const channels={
    email:async(actor,{instruction})=>({preview:instruction,proposal:{instruction,owner:actor.owner}}),
    confirmEmail:async(actor,proposal)=>{calls.writes.push(['email',actor.owner,proposal]);return {status:'saved',message:'Correo enviado'};}
  };
  const emails=participants.map(n=>n+'@example.invalid').join(',');
  const config={...connectionConfig({COACH_CONNECTIONS_PILOT_EMAILS:emails,COACH_PLAN_PILOT_EMAILS:emails}),ready:true,key:randomBytes(32)};
  let route;
  const app={get(){},post(path,fn){if(path==='/chat')route=fn;}};
  const runCoachConnectedAgent=registerCoachConnections(app,{
    config,store:{findById(id){calls.legacyMail.push(id);return {lean:async()=>null};}},
    requireActive:readSession,switched:req=>!!req.switched,readSheetsSession:readSession,
    planDependencies:{plans,channels,sheetTools,sheets:{
      access:async owner=>({grant:'grant-'+owner,token:'token-'+owner}),current:async(owner,grant)=>grant==='grant-'+owner
    },interpret:async text=>{calls.interpret.push(text);return fixtureIntents.get(text) || {kind:'clarify',question:'¿Qué propuesta preparo?'};}}
  });
  const empty=()=>null;
  const context={app,scopeCoachChat,runCoachConnectedAgent,requireCoachActivo:readSession,
    limpiarCoachHealthSurveyActiveContext:empty,limpiarCoachProgram414ActiveContext:empty,limpiarCoachDecisionContext:empty,
    limpiarCoachOrderCalcContext:empty,limpiarCoachDemoOutcomeContext:empty,asegurarSesionRedisCargada:empty,obtenerCoachContextoSesion:empty,
    validarLimiteUsoCoach:()=>({allowed:true}),marcarSesionActiva:empty,limpiarMemoriaSesiones:empty,
    guardarMensajeRaw:async item=>calls.audit.push(item),console:{error:e=>{throw e;}}};
  vm.runInNewContext(part(server,'function normalizarModoChat(','function limpiarUrlExterna(')+
    part(server,'function prepararRespuestaStreamingChat(','async function consumirStreamingOpenAiChat(')+
    part(server,'app.post("/chat",','// Central Express 4 async error boundary'),context);
  async function send(login,pregunta,overrides={},extra={}) {
    const body={accountId:login,sessionId:'chat-piloto-123',pregunta,mode:'coach',coachPlanVersion:1,coachMailVersion:1,
      coachCalendarVersion:1,coachTimeZone:'America/Chicago',...overrides};
    const res={headers:{},chunks:[],statusCode:200,status(n){this.statusCode=n;return this;},json(value){this.body=value;},
      setHeader(k,v){this.headers[k]=v;},write(s){this.chunks.push(JSON.parse(s));},end(){}};
    await route({login,body,...extra},res);
    return {status:res.statusCode,data:res.body || res.chunks.at(-1)?.payload,chunks:res.chunks};
  }
  return {send,calls};
}

function ui(){
  const createElement=tag=>({tag,dataset:{},children:[],querySelector:()=>null,
    get childElementCount(){return this.children.length;},appendChild(e){this.children.push(e);}});
  const state={document:{createElement},me:{user:{id:'agustin'}},session:'chat-piloto-123',sent:[]};
  state.getCoachChatSessionId=()=>state.session;
  state.setCoachChatStatusText=t=>{state.status=t;};
  state.sendCoachMessage=(command,options)=>state.sent.push({command,label:options?.displayText});
  state.chatMessages={addEventListener(name,fn){state[name]=fn;}};
  state.floatingChatMessages=null;
  const helpers=part(frontend,'function renderCoachMessageActions','function addCoachMessage(');
  const handlers=part(frontend.slice(frontend.indexOf('  coachVoice = window.TecCoachVoice?.init')),
    '  [chatMessages, floatingChatMessages].filter(Boolean).forEach(root => {','  chatInput?.addEventListener');
  const browser=vm.runInNewContext(helpers+handlers+';({renderCoachMessageActions,contexts:createCoachAgentContexts(),retireCoachOffers})',state);
  function controls(actions){
    const card=createElement('article');
    browser.renderCoachMessageActions(card,actions.map(a=>({...a,accountId:state.me.user.id,sessionId:state.session})));
    return card.children[0]?.children || [];
  }
  function click(button){
    state.click({target:{closest(selector){
      if(selector==='[data-coach-offer-stale]' && button.dataset.coachOfferStale) return button;
      if(selector==='[data-coach-plan-action]' && button.dataset.coachPlanAction) return button;
      return null;
    }}});
  }
  return {...browser,state,click,controls};
}

test('la lectura privada queda disponible solo en la sesión personal del piloto',async()=>{
  const {send,calls}=chat();
  for (const owner of participants) {
    const voice=await send(owner,mixed,{responseMode:'audio',stream:true});
    assert.equal(voice.data.coachPrivateVoice,true);
    const typed=await send(owner,mixed,{responseMode:'text'});
    assert.notEqual(typed.data.coachPrivateVoice,true);
    const switched=await send(owner,mixed,{responseMode:'audio',coachPlanPending:'123'},{switched:true});
    assert.notEqual(switched.data?.coachPrivateVoice,true);
  }
  assert.deepEqual(calls.writes,[]);
});

test('del chat normal al botón, cada cuenta puede elegir solo la nota y revisar el correo sin enviarlo',async()=>{
  const {send,calls}=chat(), b=ui();
  for(const owner of participants){
    b.state.me.user.id=owner;
    const pending={coachQuestionFocus:'mail',coachMailPending:'pregunta-anterior'};
    const first=await send(owner,mixed,{...pending,stream:true});
    assert.deepEqual(first.chunks.map(c=>c.type),['delta','done']);
    assert.equal(first.data.coachPlanAgent,true);
    assert.match(first.data.respuesta,/Julia/);
    const original={...pending,coachPlanPending:first.data.coachPlanPending,coachQuestionFocus:'plan'};
    const corrected=await send(owner,literal,{...original,coachDraftFocus:'mail'});
    assert.match(corrected.data.respuesta,/ya tengo el catálogo/);
    assert.deepEqual(calls.legacyMail,[]);
    const selected=await send(owner,selective,{...original,coachPlanPending:corrected.data.coachPlanPending});
    const controls=b.controls(selected.data.coachHelpActions), note=controls.find(c=>c.textContent==='Guardar nota');
    assert.ok(note);
    assert.ok(controls.every(c=>!c.dataset.coachPlanAction.endsWith('apply email')));
    const revision=selected.data.coachPlanPending;
    const confirmation={coachPlanPending:revision,coachQuestionFocus:'plan'};
    const other=participants.find(x=>x!==owner);
    const clicksBefore=b.state.sent.length;
    b.state.me.user.id=other;b.click(note);
    assert.match(b.state.status,/cuenta/);assert.equal(b.state.sent.length,clicksBefore);
    b.state.me.user.id=owner;
    const before=calls.writes.length;
    b.click(note);
    const action=b.state.sent.at(-1);
    assert.equal(action.label,'Guardar nota');
    for(const [who,body] of [[other,confirmation],[owner,{...confirmation,sessionId:'otro-chat-123'}]]) {
      assert.match((await send(who,action.command,body)).data.respuesta,/caducó|cambió/);
    }
    assert.equal((await send(other,action.command,{...confirmation,accountId:owner})).status,403);
    assert.match((await send(owner,'TECPLAN '+revision+' apply email',confirmation)).data.respuesta,/caducó|cambió/);
    assert.equal(calls.writes.length,before);
    assert.match((await send(owner,action.command,confirmation)).data.respuesta,/Nota guardada/);
    assert.match((await send(owner,action.command,confirmation)).data.respuesta,/caducó|cambió/);
    assert.deepEqual(calls.writes.slice(before),[['note',owner,'le marqué']]);
  }
  assert.doesNotMatch(JSON.stringify(calls.audit),/Julia|catálogo|le marqué|TECPLAN/);
});

test('la página siguiente y los controles se vinculan a su conversación y revisión',async()=>{
  const {send}=chat(),b=ui(), owner='agustin';
  const first=(await send(owner,group)).data;
  const source={accountId:owner,sessionId:b.state.session};
  b.contexts.attach(source);
  b.contexts.record(source,first,first.coachHelpActions);
  const page=b.controls(first.coachHelpActions).find(c=>c.dataset.coachPlanAction.endsWith('page undated2'));
  assert.ok(page);
  const continuation={...source};
  b.contexts.attach(continuation);
  assert.equal(continuation.coachPlanPending,first.coachPlanPending);
  b.click(page);
  const next=(await send(owner,b.state.sent[0].command,continuation)).data;
  assert.match(next.respuesta,/agustin persona 11/);
  assert.doesNotMatch(next.respuesta,/rigoberto persona/);
  const outcome=b.contexts.record(continuation,next,next.coachHelpActions);
  const root={querySelectorAll:()=>[page]};
  b.retireCoachOffers([root],[],outcome);
  assert.equal(page.disabled,true);b.click(page);
  assert.equal(b.state.sent.length,1);
  const newer=b.controls(next.coachHelpActions)[0];
  const switchChannel={...source};
  b.contexts.attach(switchChannel);
  const mail=b.contexts.record(switchChannel,{coachMailAgent:true,coachMailPending:'correo'},[]);
  b.retireCoachOffers([{querySelectorAll:()=>[newer]}],[],mail);
  assert.equal(newer.disabled,true);
  const other={accountId:'rigoberto',sessionId:source.sessionId};
  assert.ok(b.contexts.attach(other));
  assert.equal(other.coachPlanPending,undefined);assert.equal(other.coachMailPending,undefined);
  assert.equal(b.contexts.record(continuation,next,next.coachHelpActions),null);
});

test('un comando literal de Gmail o una continuación ambigua sigue en su canal anterior',async()=>{
  const {send,calls}=chat(), pending={coachQuestionFocus:'mail',coachDraftFocus:'mail',coachMailPending:'pregunta-anterior'};
  const direct=await send('agustin','Mensaje: agenda una cita y envía el correo',pending);
  assert.equal(direct.data.coachMailAgent,true);assert.notEqual(direct.data.coachPlanAgent,true);
  assert.deepEqual(calls.interpret,[]);
  const follow=await send('agustin','a las cuatro',pending);
  assert.notEqual(follow.data.coachPlanAgent,true);
  assert.deepEqual(calls.interpret,['a las cuatro']);assert.deepEqual(calls.writes,[]);
});

test('si un vendedor cambia de conversación durante la consulta, la respuesta tardía queda fuera del nuevo chat',async()=>{
  const updates=[],offered=[];
  const empty=()=>null, body={user:{id:'agustin'}};
  let session='original', finish, handlers;
  const context={me:body,window:{sessionStorage:{getItem:()=>''}},COACH_WORKSPACE_TAB_KEY:'tec',
    getCoachChatSessionId:()=>session,getCoachDemoStageMeta:()=>({id:'cierre'}),getCoachDemoStageId:empty,
    getCoachVisitorId:empty,getCoachDemoEvents:()=>[],getActiveCoachHealthSurveyContext:empty,getActiveCoachProgram414Context:empty,
    getActiveCoachOrderCalcContext:empty,getActiveCoachDecisionContext:empty,getActiveCoachDemoOutcomeContext:empty,getActiveCoachCrmContext:empty,
    coachChatBusy:false,coachVoice:null,chatMessages:null,floatingChatMessages:null,floatingChatInput:null,chatInput:null,
    floatingCoachMode:'idle',clearCoachChatInputs:empty,toggleCoachChatBusy:empty,setCoachChatStatusText:empty,
    addCoachMessage:()=>[{card:{}}],updateCoachMessage:(_handles,txt,opts)=>{updates.push(txt);if(opts?.actions)offered.push(...opts.actions);},
    streamCoachChatReply:(_request,callbacks)=>{handlers=callbacks;return new Promise(resolve=>{finish=resolve;});}
  };
  const source=part(frontend,'function renderCoachMessageActions','function addCoachMessage(')+
    part(frontend,'  const sendCoachMessage = async','  coachVoice = window.TecCoachVoice?.init');
  const send=vm.runInNewContext(source+';sendCoachMessage',context);
  const running=send('Consulta mis leads');
  session='otra';body.user.id='rigoberto';
  handlers.onDelta('Nombre confidencial de Agustín');
  finish({coachMailAgent:true,coachPlanAgent:true,coachPlanPending:'a'.repeat(24),respuesta:'Nombre confidencial de Agustín',
    coachHelpActions:[{type:'planAction',revision:'a'.repeat(24),op:'apply',arg:'note',label:'Guardar nota'}]});
  assert.equal((await running).ok,false);
  assert.doesNotMatch(updates.join(),/Agustín/);assert.deepEqual(offered,[]);
});
