import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import vm from 'node:vm';

const app = fs.readFileSync(new URL('../public/coach/app.js',import.meta.url),'utf8');

function fakeElement(tag) {
  return { tag, children: [], dataset: {}, querySelector:()=>null,
    get childElementCount() { return this.children.length; }, appendChild(item) { this.children.push(item); } };
}

test('las acciones de correo unen botones al código que se vio y usan enlaces de destino fijo',()=>{
  const document={createElement:fakeElement};
  const render=vm.runInNewContext(app.slice(app.indexOf('function renderCoachMessageActions'),app.indexOf('function addCoachMessage('))+';renderCoachMessageActions',{document});
  const card=fakeElement('article');
  render(card,[
    {type:'workspace',workspace:'cierre',label:'Cerrar'},
    {type:'mailConfirm',id:'aa11bb22cc33',label:'Cambiar esto'},
    {type:'mailCancel',id:'aa11bb22cc33'},
    {type:'connections',href:'https://example.invalid/'},
    {type:'mailSource',id:'ab1234',mailbox:'a+tec@example.test',label:'Abrir aviso',href:'https://example.invalid/'},
    {type:'mailSource',id:'javascript:bad',mailbox:'foo@example.test'},
    {type:'mailConfirm',id:'aa11bb22cc33 extra'}
  ]);
  const controls=card.children[0].children;
  assert.equal(controls.length,5);
  assert.equal(controls[0].dataset.coachChatActionWorkspace,'cierre');
  assert.equal(controls[1].dataset.coachMailCommand,'Confirmar envío AA11BB22CC33');
  assert.equal(controls[2].dataset.coachMailCommand,'Cancelar borrador AA11BB22CC33');
  assert.equal(controls[3].href,'/mis-conexiones/');
  assert.equal(controls[4].href,'https://mail.google.com/mail/?authuser=a%2Btec%40example.test#all/ab1234');
  assert.equal(controls[3].target,'_blank');assert.equal(controls[4].rel,'noopener noreferrer');
  const register=app.slice(app.indexOf('  [chatMessages, floatingChatMessages].filter(Boolean).forEach(root => {',app.indexOf('  coachVoice = window.TecCoachVoice?.init')),app.indexOf('  chatInput?.addEventListener',app.indexOf('  coachVoice = window.TecCoachVoice?.init')));
  let events={},commands=[];
  vm.runInNewContext(register,{
    chatMessages:{addEventListener:(name,fn)=>{events[name]=fn;}}, floatingChatMessages:null,
    sendCoachMessage:txt=>commands.push(txt),setCoachWorkspaceTab() {}
  });
  events.click({target:{closest:name=>name==='[data-coach-mail-command]'?controls[1]:null}});
  assert.deepEqual(commands,['Confirmar envío AA11BB22CC33']);
});

function chatFixture(responses) {
  let currentSession='s1';
  const requests=[],spoken=[],updates=[];
  const context={chatMessages:null,me:{user:{id:'a'}},coachChatBusy:false,
    window:{sessionStorage:{getItem:()=>'cierre'}}, COACH_WORKSPACE_TAB_KEY:'workspace',
    getCoachChatSessionId:()=>currentSession,getCoachVisitorId:()=>'',getCoachDemoStageMeta:()=>({id:'a'}),
    getCoachDemoStageId:()=>'',getCoachDemoEvents:()=>[],getActiveCoachHealthSurveyContext:()=>null,
    getActiveCoachProgram414Context:()=>null,getActiveCoachOrderCalcContext:()=>null,getActiveCoachDecisionContext:()=>null,getActiveCoachDemoOutcomeContext:()=>null,
    clearCoachChatInputs(){},toggleCoachChatBusy:value=>{context.coachChatBusy=value;},setCoachChatStatusText(){}, coachVoice:{pausePlayback(){},speak:t=>spoken.push(t)},
    addCoachMessage:()=>[{}],updateCoachMessage:(_handle,txt,opts)=>updates.push({txt,opts}),
    renderCoachRepLeadSummary(){},renderActiveLeadContext(){},floatingCoachMode:'idle',floatingChatInput:null,chatInput:null,
    streamCoachChatReply:async payload=>{requests.push(payload);return responses.shift();}
  };
  const source=app.slice(app.indexOf('  const sendCoachMessage = async'),app.indexOf('  coachVoice = window.TecCoachVoice?.init'));
  const send=vm.runInNewContext(source+';sendCoachMessage',context);
  return {send,requests,updates,spoken,context,setSession:value=>{currentSession=value;}};
}

test('el token pasa una vez a la misma conversación; el resultado privado excluye TTS y las demás respuestas conservan voz',async()=>{
  const agent={respuesta:'Pedido #1035',coachMailAgent:true,coachMailPending:'opaque',coachHelpActions:[{type:'mailConfirm',id:'111111111111'}]};
  const f=chatFixture([agent,{respuesta:'Ya se envió',coachMailAgent:true},{respuesta:'Hola'}]);
  for (const text of ['Leer','Confirmar envío 111111111111','Hola']) assert.equal((await f.send(text,{replyMode:'audio'})).ok,true);
  assert.deepEqual(f.spoken,['Hola']);assert.equal(f.context.coachChatBusy,false);
  assert.deepEqual(f.requests.map(p=>p.coachMailPending),[undefined,'opaque',undefined]);
  assert.equal(f.updates[0].opts.actions[0].id,'111111111111');
});

test('un cambio de conversación o cuenta descarta el token privado anterior',async()=>{
  const prepared={respuesta:'Dirección?',coachMailAgent:true,coachMailPending:'opaque'};
  const f=chatFixture([prepared,prepared,{respuesta:'Otro chat'}]);
  await f.send('correo');
  f.setSession('s2');await f.send('hola');
  f.context.me.user.id='b';await f.send('hola');
  assert.ok(f.requests.every(p=>!p.coachMailPending));
});

test('el chat admite la respuesta JSON del agente incluso cuando fetch expone ReadableStream',async()=>{
  const stream=vm.runInNewContext(app.slice(app.indexOf('  const streamCoachChatReply = async'),app.indexOf('  const sendCoachMessage = async'))+';streamCoachChatReply',{
    COACH_CHAT_API_URL:'/chat',fetch:async()=>Response.json({coachMailAgent:true,respuesta:'lista'})
  });
  assert.equal((await stream({})).respuesta,'lista');
});
