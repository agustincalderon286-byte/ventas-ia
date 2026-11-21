import test from 'node:test';
import assert from 'node:assert/strict';
import {createHash,randomBytes} from 'node:crypto';
import {createCoachWaAgent} from '../src/coach-wa-chat.js';
import {createWaDrafts} from '../src/coach-wa-drafts.js';
import {createWaCloud,waCloudConfig} from '../src/coach-wa-cloud.js';
import {connectionConfig,registerCoachConnections} from '../src/coach-connections.js';

// Datos ficticios: dominio .test, números 555-01xx y un token de prueba sin valor real.
const env={COACH_WA_CLOUD_SELLER_EMAIL:'A@example.test',COACH_WA_CLOUD_PHONE:'+13125550177',
  COACH_WA_CLOUD_PHONE_NUMBER_ID:'1000000000001',COACH_WA_CLOUD_ACCESS_TOKEN:'test-token-'+'x'.repeat(24)};

function graph({display='+1 312-555-0177',post}={}) {
  const calls=[];
  const fetch=async(url,init={})=>{
    calls.push({url,init,body:init.body?JSON.parse(init.body):null});
    if (!init.method) return Response.json({display_phone_number:display,verified_name:'TEC Prueba',id:'1000000000001'});
    const r=post?await post():{status:200,body:{messaging_product:'whatsapp',messages:[{id:'wamid.TEST'+calls.length,message_status:'accepted'}]}};
    return Response.json(r.body,{status:r.status});
  };
  return {fetch,calls,posts:()=>calls.filter(c=>c.init.method==='POST'),gets:()=>calls.filter(c=>!c.init.method)};
}

function memoryStore() {
  const docs=new Map();
  const match=(d,q)=>Object.entries(q).every(([k,v])=>v?.$gt?d[k]>v.$gt:v&&typeof v==='object'&&'$ne' in v?d[k]!==v.$ne:d[k]===v);
  const find=q=>[...docs.values()].find(d=>match(d,q));
  const apply=(d,u)=>{Object.assign(d,u.$set);for (const k of Object.keys(u.$unset||{})) delete d[k];};
  return {docs,
    create:async d=>{docs.set(d._id,structuredClone(d));},
    findOne:q=>({lean:async()=>structuredClone(find(q)||null)}),
    updateOne:async(q,u)=>{const d=find(q);if (!d) return {matchedCount:0};apply(d,u);return {matchedCount:1};},
    // Atómico como en Mongo: la coincidencia y la reclamación ocurren en la misma llamada.
    findOneAndUpdate(q,u){const d=find(q);const before=d?structuredClone(d):null;if (d) apply(d,u);return {lean:async()=>before};}
  };
}

function fixture({cloudEnv=env,display,post,pilot=['b@example.test']}={}) {
  let time=Date.UTC(2026,10,20,17);
  const now=()=>time,key=randomBytes(32),store=memoryStore(),g=graph({display,post});
  const cloud=createWaCloud(waCloudConfig(cloudEnv),{fetch:g.fetch});
  const waDrafts=createWaDrafts({key,store,now});
  // Ventana de 24 h abierta de prueba: el runtime la trata como cerrada si falta el webhook.
  const agent=createCoachWaAgent({config:{key,pilot},switched:()=>false,waDrafts,now,cloud,isWindowOpen:async()=>true,
    rewrite:async()=>({body:'¿Te va bien el viernes?'}),findLeads:async()=>[]});
  const send=(text,{owner='a',login='active',sessionId='chat-a',version=2,...body}={})=>agent({
    req:{body:{accountId:owner,coachWhatsAppVersion:version,...body}},
    auth:{user:{_id:owner,email:owner+'@example.test'},session:{tokenHash:login}},sessionId,text});
  return {send,g,store,waDrafts,cloud,advance:ms=>{time+=ms;},key,now};
}
const sendCmd=id=>'Enviar WhatsApp Business '+id;

test('la configuración exige un email, un número y un ID exactos y nunca el número global de Twilio',()=>{
  const ok=waCloudConfig(env);
  assert.equal(ok.ready,true);assert.equal(ok.email,'a@example.test');
  assert.doesNotMatch(JSON.stringify(ok),/test-token/);
  for (const bad of [{COACH_WA_CLOUD_SELLER_EMAIL:'*'},{COACH_WA_CLOUD_SELLER_EMAIL:'a@example.test,b@example.test'},
    {COACH_WA_CLOUD_SELLER_EMAIL:'*@example.test'},{COACH_WA_CLOUD_PHONE:'3125550177'},{COACH_WA_CLOUD_PHONE:'+1312555*'},
    {COACH_WA_CLOUD_PHONE_NUMBER_ID:'*'},{COACH_WA_CLOUD_ACCESS_TOKEN:''},{COACH_WA_CLOUD_GRAPH_VERSION:'latest'},
    {TWILIO_WHATSAPP_FROM:'whatsapp:+13125550177'},{TWILIO_SMS_FROM:'+13125550177'}]) {
    const config=waCloudConfig({...env,...bad});
    assert.equal(config.ready,false,JSON.stringify(bad));assert.equal(config.grant,'');assert.equal(config.token,'');
  }
  assert.ok(waCloudConfig({...env,TWILIO_WHATSAPP_FROM:'whatsapp:+12605550100'}).ready);
  assert.notEqual(waCloudConfig({...env,COACH_WA_CLOUD_ACCESS_TOKEN:'other-token-'+'y'.repeat(24)}).grant,ok.grant);
});

test('Graph: verifica display_phone_number y envía texto libre con timeout y errores claros',async()=>{
  const g=graph();
  const cloud=createWaCloud(waCloudConfig(env),{fetch:g.fetch});
  assert.deepEqual(await cloud.verify(),{ok:true,phone:'+13125550177',name:'TEC Prueba'});
  assert.match(g.calls[0].url,/^https:\/\/graph\.facebook\.com\/v27\.0\/1000000000001\?fields=display_phone_number/);
  assert.equal(g.calls[0].init.headers.Authorization,'Bearer test-token-'+'x'.repeat(24));
  assert.ok(g.calls[0].init.signal instanceof AbortSignal);
  const receipt=await cloud.send({to:'+525555010101',body:'Hola'});
  assert.equal(receipt.id,'wamid.TEST2');
  const post=g.posts()[0];
  assert.match(post.url,/\/v27\.0\/1000000000001\/messages$/);
  assert.deepEqual(post.body,{messaging_product:'whatsapp',recipient_type:'individual',to:'525555010101',type:'text',text:{preview_url:false,body:'Hola'}});
  assert.ok(post.init.signal instanceof AbortSignal);assert.equal(post.init.redirect,'error');
  assert.equal((await createWaCloud(waCloudConfig(env),{fetch:graph({display:'+1 312 555 0199'}).fetch}).verify()).reason,'mismatch');
  assert.equal((await createWaCloud(waCloudConfig(env),{fetch:async()=>{throw Error('offline');}}).verify()).reason,'unreachable');
  assert.equal((await createWaCloud(waCloudConfig({}),{fetch:g.fetch}).verify()).reason,'notConfigured');
  const rejected=createWaCloud(waCloudConfig(env),{fetch:graph({post:()=>({status:400,body:{error:{code:131047,message:'Re-engagement'}}})}).fetch});
  await assert.rejects(()=>rejected.send({to:'+525555010101',body:'Hola'}),e=>e.providerRejected===true&&e.reason==='template');
  const down=createWaCloud(waCloudConfig(env),{fetch:graph({post:()=>({status:503,body:{}})}).fetch});
  await assert.rejects(()=>down.send({to:'+525555010101',body:'Hola'}),e=>!e.providerRejected&&!e.beforeSend);
});

test('solo el vendedor configurado envía; confirmar, «sí» y códigos viejos nunca envían; la corrección exige el código nuevo',async()=>{
  const f=fixture();
  const draft=await f.send('Prepara un WhatsApp para +52 55 5501 0101. Mensaje: ¿Te va bien el jueves?',{to:'+19999999999',accessToken:'client'});
  assert.match(draft.respuesta,/De: tu línea \+13125550177/);
  assert.match(draft.respuesta,/Para: \+525555010101/);
  assert.deepEqual(draft.coachHelpActions.map(a=>a.type),['waCloudSend','waCancel']);
  const first=draft.coachHelpActions[0];
  assert.equal(first.to,'+525555010101');assert.equal(first.text,'¿Te va bien el jueves?');
  assert.doesNotMatch(JSON.stringify([...f.store.docs.values()]),/525555010101|jueves/);
  // El comando personal y las respuestas libres no se convierten en envío.
  const personal=await f.send('Confirmar borrador WhatsApp '+first.id);
  assert.ok(!personal.coachHelpActions.some(a=>a.type==='waOpen'));assert.match(personal.respuesta,/no envía nada/);
  const yes=await f.send('Sí',{coachWhatsAppPending:draft.coachWhatsAppPending});
  assert.deepEqual(yes.coachHelpActions.map(a=>a.type),['waCloudSend','waCancel']);
  assert.equal(yes.coachHelpActions[0].id,first.id);
  assert.equal(f.g.posts().length,0);
  // Corregir emite un código nuevo; el anterior queda inválido.
  const edited=await f.send('Mejor el viernes',{coachWhatsAppPending:draft.coachWhatsAppPending});
  const current=edited.coachHelpActions[0];
  assert.notEqual(current.id,first.id);assert.equal(current.text,'¿Te va bien el viernes?');
  assert.match((await f.send(sendCmd(first.id))).respuesta,/caducó, cambió/);
  assert.equal(f.g.posts().length,0);
  assert.match((await f.send(sendCmd(current.id),{version:1})).respuesta,/Actualiza el Coach/);
  assert.match((await f.send(sendCmd(current.id),{responseMode:'audio'})).respuesta,/No se envió nada/);
  assert.equal(f.g.posts().length,0);
  const sent=await f.send(sendCmd(current.id),{to:'+19999999999',from:'+19999999998',token:'client'});
  assert.match(sent.respuesta,/Meta aceptó tu WhatsApp para \+525555010101/);
  assert.match(sent.respuesta,/ID de Meta: wamid\.TEST\d+/);assert.match(sent.respuesta,/no confirma la entrega/);
  assert.deepEqual(sent.coachHelpActions,[{type:'waCloudStatus',state:'accepted',label:'Aceptado por Meta'}]);
  assert.equal(f.g.posts().length,1);
  assert.equal(f.g.posts()[0].body.to,'525555010101');assert.equal(f.g.posts()[0].body.text.body,'¿Te va bien el viernes?');
  assert.equal(f.g.gets().length,1,'verifica la línea antes del envío');
  // Repetir el comando informa el resultado sin otra llamada.
  assert.match((await f.send(sendCmd(current.id))).respuesta,/Meta aceptó/);
  assert.equal(f.g.posts().length,1);
});

test('doble clic hace una sola llamada y un resultado indeterminado no se repite',async()=>{
  const f=fixture();
  const d=(await f.send('Prepara un WhatsApp para +525555010101. Mensaje: Hola')).coachHelpActions[0];
  const results=await Promise.all([f.send(sendCmd(d.id)),f.send(sendCmd(d.id))]);
  assert.equal(f.g.posts().length,1);
  assert.ok(results.some(r=>/Meta aceptó/.test(r.respuesta)));
  for (const r of results) assert.match(r.respuesta,/Meta aceptó|ya se está enviando/);

  const timeout=fixture({post:async()=>{throw Object.assign(Error('timeout'),{name:'TimeoutError'});}});
  const t=(await timeout.send('Prepara un WhatsApp para +525555010101. Mensaje: Hola')).coachHelpActions[0];
  assert.match((await timeout.send(sendCmd(t.id))).respuesta,/No pude confirmar si Meta aceptó.*no lo reintentará/s);
  assert.match((await timeout.send(sendCmd(t.id))).respuesta,/No pude confirmar/);
  assert.equal(timeout.g.posts().length,1);

  // Si el proceso queda a mitad de la llamada, pasado el plazo el estado es indeterminado, no reintentable.
  const s=fixture();
  const p=(await s.send('Prepara un WhatsApp para +525555010101. Mensaje: Hola')).coachHelpActions[0];
  let release;
  const binding={owner:'a',channel:'cloud',grant:s.cloud.config.grant};
  const ownSession=createHash('sha256').update('active:chat-a').digest('hex');
  const hanging=s.waDrafts.send({...binding,session:ownSession,id:p.id,deliver:()=>new Promise(r=>{release=r;})});
  await new Promise(r=>setImmediate(r));
  assert.match((await s.send(sendCmd(p.id))).respuesta,/ya se está enviando/);
  s.advance(2*60000);
  assert.match((await s.send(sendCmd(p.id))).respuesta,/No pude confirmar/);
  release({id:'wamid.LATE'});await hanging;
  assert.equal(s.g.posts().length,0);
});

test('Meta rechaza por ventana de 24 horas o línea no verificada: mensaje claro y sin envío',async()=>{
  const f=fixture({post:()=>({status:400,body:{error:{code:131047}}})});
  const d=(await f.send('Prepara un WhatsApp para +525555010101. Mensaje: Hola')).coachHelpActions[0];
  const r=await f.send(sendCmd(d.id));
  assert.match(r.respuesta,/plantilla aprobada.*24 horas.*no se envió/s);
  assert.equal(r.coachHelpActions[0].state,'failed');
  await f.send(sendCmd(d.id));assert.equal(f.g.posts().length,1);

  const wrong=fixture({display:'+1 312 555 0199'});
  const w=(await wrong.send('Prepara un WhatsApp para +525555010101. Mensaje: Hola')).coachHelpActions[0];
  assert.match((await wrong.send(sendCmd(w.id))).respuesta,/número distinto.*No se envió nada/s);
  assert.equal(wrong.g.posts().length,0);
});

test('separación: otros vendedores siguen en su WhatsApp manual y nadie usa el borrador o la línea de otro',async()=>{
  const f=fixture();
  const own=(await f.send('Prepara un WhatsApp para +525555010101. Mensaje: Privado')).coachHelpActions[0];
  const other=await f.send('Prepara un WhatsApp para +525555010102. Mensaje: Hola',{owner:'b'});
  assert.deepEqual(other.coachHelpActions.map(a=>a.type),['waPersonalConfirm','waCancel']);
  assert.doesNotMatch(other.respuesta,/Business|3125550177/);
  for (const opt of [{owner:'b'},{login:'other'},{sessionId:'other'},{owner:'z'}]) await f.send(sendCmd(own.id),opt);
  assert.match((await f.send(sendCmd(other.coachHelpActions[0].id),{owner:'b'})).respuesta,/no tiene una línea/);
  assert.match((await f.send(sendCmd(other.coachHelpActions[0].id))).respuesta,/caducó, cambió/);
  assert.ok((await f.send('Confirmar borrador WhatsApp '+other.coachHelpActions[0].id,{owner:'b'})).coachHelpActions[0].type==='waOpen');
  assert.equal(f.g.posts().length,0);
  // Si el dueño cambia la línea o el token, los borradores anteriores ya no valen.
  const rotated=createCoachWaAgent({config:{key:f.key,pilot:[]},switched:()=>false,now:f.now,
    waDrafts:createWaDrafts({key:f.key,store:f.store,now:f.now}),
    cloud:createWaCloud(waCloudConfig({...env,COACH_WA_CLOUD_ACCESS_TOKEN:'other-token-'+'y'.repeat(24)}),{fetch:f.g.fetch})});
  const r=await rotated({req:{body:{accountId:'a',coachWhatsAppVersion:2}},auth:{user:{_id:'a',email:'a@example.test'},session:{tokenHash:'active'}},sessionId:'chat-a',text:sendCmd(own.id)});
  assert.match(r.respuesta,/caducó, cambió/);
  assert.equal(f.g.posts().length,0);
});

test('la UI pide confirmar destinatario y texto, envía el comando propio una sola vez y muestra el estado de Meta',async()=>{
  const fs=await import('node:fs'),vm=await import('node:vm');
  const app=fs.readFileSync(new URL('../public/coach/app.js',import.meta.url),'utf8');
  assert.match(app,/coachWhatsAppVersion: 2,/);
  const el=tag=>({tag,children:[],dataset:{},querySelector:()=>null,get childElementCount(){return this.children.length;},appendChild(i){this.children.push(i);}});
  const render=vm.runInNewContext(app.slice(app.indexOf('function renderCoachMessageActions'),app.indexOf('function addCoachMessage('))+';renderCoachMessageActions',{document:{createElement:el}});
  const card=el('article');
  render(card,[{type:'waCloudSend',id:'aa11bb22cc33',to:'+525555010101',text:'Hola & adiós'},{type:'waCloudSend',id:'bad',to:'+525555010101',text:'x'},
    {type:'waCloudSend',id:'aa11bb22cc33',to:'javascript:x',text:'x'},{type:'waCloudStatus',state:'accepted'},{type:'waCloudStatus',state:'delivered'}]);
  const [button,status]=card.children[0].children;
  assert.equal(card.children[0].children.length,2);
  assert.equal(button.dataset.coachWaCloudSend,'Enviar WhatsApp Business AA11BB22CC33');
  assert.equal(button.textContent,'Enviar por WhatsApp Business');
  assert.equal(status.textContent,'Aceptado por Meta');
  const start=app.indexOf('  coachVoice = window.TecCoachVoice?.init');
  const register=app.slice(app.indexOf('  [chatMessages, floatingChatMessages].filter(Boolean).forEach(root => {',start),app.indexOf('  chatInput?.addEventListener',start));
  const events={},commands=[],prompts=[];let accept=false;
  vm.runInNewContext(register,{chatMessages:{addEventListener:(n,fn)=>{events[n]=fn;}},floatingChatMessages:null,
    window:{confirm:t=>{prompts.push(t);return accept;}},sendCoachMessage:t=>commands.push(t),setCoachWorkspaceTab(){},
    me:{user:{id:'a'}},getCoachChatSessionId:()=>'s1',setCoachChatStatusText(){}});
  const click=()=>events.click({target:{closest:s=>s==='[data-coach-wa-cloud-send]'?button:null}});
  click();assert.deepEqual(commands,[]);
  assert.match(prompts[0],/Para: \+525555010101\n\nHola & adiós/);
  accept=true;click();click();
  assert.deepEqual(commands,['Enviar WhatsApp Business AA11BB22CC33']);assert.equal(button.disabled,true);
});

test('Mis conexiones muestra el estado real de la línea solo al vendedor configurado',async()=>{
  const routes=new Map();
  const app={get:(p,h)=>routes.set(p,h),post:(p,h)=>routes.set(p,h)};
  const config=connectionConfig({COACH_GMAIL_ENCRYPTION_KEY:randomBytes(32).toString('base64'),COACH_CONNECTIONS_PILOT_EMAILS:'b@example.test'});
  let display='+1 312-555-0177';
  const g=graph();
  const fetch=async(url,init)=>init?.method?g.fetch(url,init):Response.json({display_phone_number:display,verified_name:'TEC Prueba'});
  registerCoachConnections(app,{config,switched:()=>false,waCloud:createWaCloud(waCloudConfig(env),{fetch}),
    waProfile:{load:async()=> '',save:async()=>true,remove:async()=>{}},
    requireActive:async req=>({user:{_id:req.id,email:req.id+'@example.test'},session:{tokenHash:'s-'+req.id}}),
    store:{findById:()=>({lean:async()=>null})}});
  async function open(id) {
    const res={set(){return this;},vary(){return this;},status(c){this.code=c;return this;},send(b){this.body=b;},redirect(){}};
    await routes.get('/mis-conexiones/')({id,path:'/mis-conexiones/',method:'GET',query:{},body:{},get:()=>null},res);
    return res.body;
  }
  const a=await open('a');
  assert.match(a,/<h2>WhatsApp Business<\/h2>/);assert.match(a,/Conectada · Meta confirma el número <strong>\+13125550177/);
  assert.doesNotMatch(a,/test-token|1000000000001/);
  display='+1 312 555 0199';
  assert.match(await open('a'),/Sin conexión verificada.*número distinto/s);
  const b=await open('b');
  assert.doesNotMatch(b,/WhatsApp Business<\/h2>|3125550177/);assert.match(b,/<h2>WhatsApp<\/h2>/);
  assert.equal(g.posts().length,0);
});
