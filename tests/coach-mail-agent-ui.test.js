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
    {type:'mailConfirm',id:'aa11bb22cc33 extra'},
    {type:'calendarConfirm',id:'aabbcc123456'},
    {type:'calendarCancel',id:'aabbcc123456'},
    {type:'calendarEvent',url:'https://www.google.com/calendar/event?eid=def456'},
    {type:'calendarEvent',url:'https://calendar.google.com.example.test/calendar/event?eid=def456'},
    {type:'waPersonalConfirm',id:'aa11bb22cc33'},
    {type:'waCancel',id:'aa11bb22cc33'},
    {type:'waOpen',accountId:'a',sessionId:'s1',to:'+13125550123',text:'Hola María & José',url:'https://example.invalid'},
    {type:'waOpen',accountId:'a',sessionId:'s1',to:'https://example.invalid',text:'Hola'},
    {type:'waOpen',to:'+13125550123',text:'Hola'},
    {type:'waConfirm',id:'aa11bb22cc33'},
    {type:'calendarSlot',choice:2,searchId:'f'.repeat(32)},
    {type:'calendarSlot',choice:3,searchId:'f'.repeat(32)},
    {type:'calendarSlot',choice:1,searchId:'otra búsqueda'}
  ]);
  const controls=card.children[0].children;
  assert.equal(controls.length,12);
  assert.equal(controls[0].dataset.coachChatActionWorkspace,'cierre');
  assert.equal(controls[1].dataset.coachMailCommand,'Confirmar envío AA11BB22CC33');
  assert.equal(controls[2].dataset.coachMailCommand,'Cancelar borrador AA11BB22CC33');
  assert.equal(controls[3].href,'/mis-conexiones/');
  assert.equal(controls[4].href,'https://mail.google.com/mail/?authuser=a%2Btec%40example.test#all/ab1234');
  assert.equal(controls[5].dataset.coachMailCommand,'Confirmar cita AABBCC123456');
  assert.equal(controls[6].dataset.coachMailCommand,'Descartar cita AABBCC123456');
  assert.equal(controls[7].href,'https://www.google.com/calendar/event?eid=def456');
  assert.equal(controls[8].dataset.coachMailCommand,'Confirmar borrador WhatsApp AA11BB22CC33');
  assert.equal(controls[9].dataset.coachMailCommand,'Descartar WhatsApp AA11BB22CC33');
  assert.equal(controls[10].href,'https://wa.me/13125550123?text=Hola%20Mar%C3%ADa%20%26%20Jos%C3%A9');
  assert.equal(controls[11].textContent,'Elegir horario 2');
  assert.equal(controls[11].dataset.coachCalendarChoice,'Elegir horario 2 '+'f'.repeat(32));
  assert.equal(controls[3].target,'_blank');assert.equal(controls[4].rel,'noopener noreferrer');
  const register=app.slice(app.indexOf('  [chatMessages, floatingChatMessages].filter(Boolean).forEach(root => {',app.indexOf('  coachVoice = window.TecCoachVoice?.init')),app.indexOf('  chatInput?.addEventListener',app.indexOf('  coachVoice = window.TecCoachVoice?.init')));
  let events={},commands=[];
  vm.runInNewContext(register,{
    chatMessages:{addEventListener:(name,fn)=>{events[name]=fn;}}, floatingChatMessages:null,
    sendCoachMessage:txt=>commands.push(txt),setCoachWorkspaceTab() {},me:{user:{id:'b'}},getCoachChatSessionId:()=>'s1',setCoachChatStatusText(){}
  });
  events.click({target:{closest:name=>name==='[data-coach-mail-command]'?controls[1]:null}});
  assert.deepEqual(commands,['Confirmar envío AA11BB22CC33']);
  let blocked=false;
  events.click({target:{closest:name=>name==='[data-coach-whatsapp-account]'?controls[10]:null},preventDefault(){blocked=true;}});
  assert.equal(blocked,true);
});

function domElement(tag) {
  const element={ tag, children: [], dataset: {}, attributes: {}, querySelector:()=>null,
    get childElementCount() { return this.children.length; },
    appendChild(item) { this.children.push(item); },
    setAttribute(name,value) { this.attributes[name]=value; },
    contains(item) { return item===this || this.children.some(child=>child.contains?.(item)); },
    querySelectorAll(selector) {
      const found=[];
      const walk=node=>node.children?.forEach(child=>{
        if (selector==='[data-coach-offer-channel]' && child.dataset?.coachOfferChannel) found.push(child);
        walk(child);
      });
      walk(this);
      return found;
    } };
  return element;
}

function chatFixture(responses, extra = {}) {
  let currentSession='s1';
  const requests=[],spoken=[],updates=[];
  const context={chatMessages:null,floatingChatMessages:null,document:{createElement:domElement},me:{user:{id:'a'}},coachChatBusy:false,
    window:{sessionStorage:{getItem:()=>'cierre'}}, COACH_WORKSPACE_TAB_KEY:'workspace',
    getCoachChatSessionId:()=>currentSession,getCoachVisitorId:()=>'',getCoachDemoStageMeta:()=>({id:'a'}),
    getCoachDemoStageId:()=>'',getCoachDemoEvents:()=>[],getActiveCoachHealthSurveyContext:()=>null,
    getActiveCoachProgram414Context:()=>null,getActiveCoachOrderCalcContext:()=>null,getActiveCoachDecisionContext:()=>null,getActiveCoachDemoOutcomeContext:()=>null,getActiveCoachCrmContext:()=>null,
    clearCoachChatInputs(){},toggleCoachChatBusy:value=>{context.coachChatBusy=value;},setCoachChatStatusText(){}, coachVoice:{pausePlayback(){},speak:t=>spoken.push(t)},
    addCoachMessage:()=>[{}],updateCoachMessage:(_handle,txt,opts)=>updates.push({txt,opts}),
    renderCoachRepLeadSummary(){},renderActiveLeadContext(){},floatingCoachMode:'idle',floatingChatInput:null,chatInput:null,
    streamCoachChatReply:async payload=>{requests.push({...payload});return responses.shift();},
    ...extra
  };
  const helpers=app.slice(app.indexOf('function renderCoachMessageActions'),app.indexOf('function addCoachMessage('));
  const source=app.slice(app.indexOf('  const sendCoachMessage = async'),app.indexOf('  coachVoice = window.TecCoachVoice?.init'));
  const send=vm.runInNewContext(helpers+source+';sendCoachMessage',context);
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

test('una precisión de cita conserva su propia conversación sin borrar el contexto de Gmail',async()=>{
  const f=chatFixture([
    {coachMailAgent:true,respuesta:'Borrador',coachMailPending:'correo'},
    {coachMailAgent:true,coachCalendarAgent:true,respuesta:'¿A qué hora?',coachCalendarPending:'cita'},
    {coachMailAgent:true,coachCalendarAgent:true,respuesta:'Listo'},
    {respuesta:'Hola'}
  ]);
  for (const text of ['Revisa Gmail','Agéndame una llamada','A las 3 pm','Hola']) await f.send(text,{replyMode:'audio'});
  assert.deepEqual(f.requests.map(p=>p.coachCalendarPending),[undefined,undefined,'cita',undefined]);
  assert.deepEqual(f.requests.map(p=>p.coachMailPending),[undefined,'correo','correo','correo']);
  assert.ok(f.requests.every(p=>p.coachTimeZone && p.coachCalendarVersion===1));
  assert.deepEqual(f.spoken,['Hola']);
});

test('bandeja → horarios → «Responde al primero» envía la lista de Gmail y conserva la de Calendar',async()=>{
  const f=chatFixture([
    {coachMailAgent:true,respuesta:'1. Pedido',coachMailPending:'bandeja'},
    {coachMailAgent:true,coachCalendarAgent:true,respuesta:'1. 16:00',coachCalendarPending:'horarios',
      coachHelpActions:[{type:'calendarSlot',choice:1,searchId:'a'.repeat(32)}]},
    {coachMailAgent:true,respuesta:'Respuesta en el mismo hilo',coachMailPending:'vista',
      coachHelpActions:[{type:'mailConfirm',id:'aa11bb22cc33'},{type:'mailCancel',id:'aa11bb22cc33'}]},
    {coachMailAgent:true,coachCalendarAgent:true,respuesta:'Agendar demo',coachHelpActions:[{type:'calendarConfirm',id:'cc11dd22ee33'}]}
  ]);
  for (const text of ['Revisa mis correos','Busca dos horarios libres para la demo','Responde al primero','1']) await f.send(text);
  assert.deepEqual(f.requests.map(p=>p.coachMailPending),[undefined,'bandeja','bandeja','vista']);
  assert.deepEqual(f.requests.map(p=>p.coachCalendarPending),[undefined,undefined,'horarios','horarios']);
  assert.deepEqual(f.requests.map(p=>p.coachWhatsAppPending),[undefined,undefined,undefined,undefined]);
});

test('una corrección tras un turno de Calendar indica el borrador visto; una pregunta de Calendar conserva el borrador',async()=>{
  const preview={coachMailAgent:true,respuesta:'Borrador',coachMailPending:'vista-1',coachHelpActions:[{type:'mailConfirm',id:'aa11bb22cc33'}]};
  const f=chatFixture([
    preview,
    {coachMailAgent:true,coachCalendarAgent:true,respuesta:'Tus seguimientos'},
    {coachMailAgent:true,respuesta:'Borrador corregido',coachMailPending:'vista-2',coachHelpActions:[{type:'mailConfirm',id:'bb22cc33dd44'}]},
    {coachMailAgent:true,coachCalendarAgent:true,respuesta:'¿Con quién?',coachCalendarPending:'cita'},
    {coachMailAgent:true,coachCalendarAgent:true,respuesta:'Agendar',coachHelpActions:[{type:'calendarConfirm',id:'cc11dd22ee33'}]}
  ]);
  for (const text of ['Prepara un correo','Muestra mis seguimientos','cambia jueves por viernes','Agenda una llamada','María']) await f.send(text);
  assert.deepEqual(f.requests.map(p=>p.coachDraftFocus),[undefined,'mail','mail','mail','mail']);
  assert.deepEqual(f.requests.map(p=>p.coachMailPending),[undefined,'vista-1','vista-1','vista-2','vista-2']);
  assert.equal(f.requests[4].coachCalendarPending,'cita');
});

test('WhatsApp personal y Gmail conservan su contexto al alternar y el foco sigue al último borrador',async()=>{
  const f=chatFixture([
    {coachMailAgent:true,respuesta:'Borrador',coachMailPending:'correo',coachHelpActions:[{type:'mailConfirm',id:'aa11bb22cc33'}]},
    {coachMailAgent:true,coachWhatsAppAgent:true,respuesta:'Borrador de WhatsApp',coachWhatsAppPending:'wa',coachHelpActions:[{type:'waPersonalConfirm',id:'dd11ee22ff33'}]},
    {coachMailAgent:true,coachWhatsAppAgent:true,respuesta:'Abre tu WhatsApp',coachHelpActions:[{type:'waOpen',to:'+13125550123',text:'Hola'}]},
    {coachMailAgent:true,respuesta:'Borrador corregido',coachMailPending:'correo-2',coachHelpActions:[{type:'mailConfirm',id:'bb22cc33dd44'}]}
  ]);
  for (const text of ['Prepara un correo','Prepara un WhatsApp','Confirmar borrador WhatsApp DD11EE22FF33','cambia jueves por viernes']) await f.send(text);
  assert.deepEqual(f.requests.map(p=>p.coachMailPending),[undefined,'correo','correo','correo']);
  assert.deepEqual(f.requests.map(p=>p.coachWhatsAppPending),[undefined,undefined,'wa',undefined]);
  assert.deepEqual(f.requests.map(p=>p.coachDraftFocus),[undefined,'mail','whatsapp','mail']);
  assert.equal(f.updates[2].opts.actions[0].accountId,'a');
});

test('solo se desactivan los botones reemplazados del mismo canal; los vigentes de otro canal siguen activos',async()=>{
  const root=domElement('section');
  const extra={chatMessages:root,
    addCoachMessage:(_container,_role,text)=>{const card=domElement('article');card.text=text;root.appendChild(card);return [{card,paragraph:{},target:{}}];},
    updateCoachMessage:(handles,txt,opts)=>{handles.forEach(h=>{h.paragraph.textContent=txt;extra.renderCoachMessageActions(h.card,opts?.actions||[]);});}};
  const f=chatFixture([
    {coachMailAgent:true,respuesta:'Borrador',coachMailPending:'v1',coachHelpActions:[{type:'mailConfirm',id:'aa11bb22cc33'},{type:'mailCancel',id:'aa11bb22cc33'}]},
    {coachMailAgent:true,coachCalendarAgent:true,respuesta:'Horarios',coachCalendarPending:'h1',coachHelpActions:[{type:'calendarSlot',choice:1,searchId:'b'.repeat(32)},{type:'calendarSlot',choice:2,searchId:'b'.repeat(32)}]},
    {coachMailAgent:true,respuesta:'Corregido',coachMailPending:'v2',coachHelpActions:[{type:'mailConfirm',id:'bb22cc33dd44'}]},
    {coachMailAgent:true,coachCalendarAgent:true,respuesta:'Tus seguimientos'},
    {coachMailAgent:true,respuesta:'Gmail confirmó el envío.'}
  ],extra);
  extra.renderCoachMessageActions=vm.runInNewContext(app.slice(app.indexOf('function renderCoachMessageActions'),app.indexOf('function addCoachMessage('))+';renderCoachMessageActions',{document:{createElement:domElement}});
  const offers=()=>root.querySelectorAll('[data-coach-offer-channel]');
  const live=()=>offers().filter(e=>!e.disabled).map(e=>e.dataset.coachMailCommand||e.dataset.coachCalendarChoice);
  await f.send('Prepara un correo');
  await f.send('Busca dos horarios libres para la demo');
  assert.deepEqual(live(),['Confirmar envío AA11BB22CC33','Cancelar borrador AA11BB22CC33','Elegir horario 1 '+'b'.repeat(32),'Elegir horario 2 '+'b'.repeat(32)]);
  await f.send('cambia jueves por viernes');
  assert.deepEqual(live(),['Elegir horario 1 '+'b'.repeat(32),'Elegir horario 2 '+'b'.repeat(32),'Confirmar envío BB22CC33DD44']);
  const old=offers()[0];
  assert.equal(old.dataset.coachOfferStale,'1');assert.equal(old.attributes['aria-disabled'],'true');
  // Una consulta de Calendar sin lista nueva anula los horarios (dependían de su contexto), no el borrador.
  await f.send('Muestra mis seguimientos');
  assert.deepEqual(live(),['Confirmar envío BB22CC33DD44']);
  await f.send('Confirmar envío BB22CC33DD44');
  assert.deepEqual(live(),[]);
});

test('al cambiar de conversación se descartan los contextos y se desactivan todos los botones anteriores',async()=>{
  const root=domElement('section');
  const extra={chatMessages:root,
    addCoachMessage:()=>{const card=domElement('article');root.appendChild(card);return [{card,paragraph:{},target:{}}];},
    updateCoachMessage:(handles,_txt,opts)=>handles.forEach(h=>extra.render(h.card,opts?.actions||[]))};
  extra.render=vm.runInNewContext(app.slice(app.indexOf('function renderCoachMessageActions'),app.indexOf('function addCoachMessage('))+';renderCoachMessageActions',{document:{createElement:domElement}});
  const f=chatFixture([
    {coachMailAgent:true,respuesta:'Borrador',coachMailPending:'v1',coachHelpActions:[{type:'mailConfirm',id:'aa11bb22cc33'}]},
    {coachMailAgent:true,coachWhatsAppAgent:true,respuesta:'WA',coachWhatsAppPending:'w1',coachHelpActions:[{type:'waPersonalConfirm',id:'dd11ee22ff33'}]},
    {respuesta:'Hola'},{respuesta:'Hola'}
  ],extra);
  await f.send('Prepara un correo');await f.send('Prepara un WhatsApp');
  f.setSession('s2');await f.send('Hola');
  f.setSession('s1');await f.send('Hola');
  assert.ok(f.requests.slice(2).every(p=>!p.coachMailPending && !p.coachWhatsAppPending && !p.coachDraftFocus));
  assert.ok(root.querySelectorAll('[data-coach-offer-channel]').every(e=>e.disabled===true));
});

test('WhatsApp presenta el enlace solo en la respuesta confirmada y asocia el borrador al chat propio',async()=>{
  const f=chatFixture([
    {coachMailAgent:true,coachWhatsAppAgent:true,respuesta:'Revisa el texto',coachWhatsAppPending:'draft-token',coachHelpActions:[{type:'waPersonalConfirm',id:'aa11bb22cc33'}]},
    {coachMailAgent:true,coachWhatsAppAgent:true,respuesta:'Toca Enviar en WhatsApp',coachHelpActions:[{type:'waOpen',to:'+13125550123',text:'Hola'}]},
    {respuesta:'Listo'}
  ]);
  await f.send('Prepara un WhatsApp',{replyMode:'audio'});
  await f.send('Confirmar borrador WhatsApp AA11BB22CC33',{replyMode:'audio'});
  f.setSession('s2');await f.send('Hola');
  assert.deepEqual(f.requests.map(p=>p.coachWhatsAppPending),[undefined,'draft-token',undefined]);
  assert.equal(f.updates[0].opts.actions[0].type,'waPersonalConfirm');
  assert.equal(f.updates[1].opts.actions[0].accountId,'a');
  assert.equal(f.updates[1].opts.actions[0].sessionId,'s1');
  assert.deepEqual(f.spoken,[]);
});

test('el chat admite la respuesta JSON del agente incluso cuando fetch expone ReadableStream',async()=>{
  const stream=vm.runInNewContext(app.slice(app.indexOf('  const streamCoachChatReply = async'),app.indexOf('  const sendCoachMessage = async'))+';streamCoachChatReply',{
    COACH_CHAT_API_URL:'/chat',fetch:async()=>Response.json({coachMailAgent:true,respuesta:'lista'})
  });
  assert.equal((await stream({})).respuesta,'lista');
});
