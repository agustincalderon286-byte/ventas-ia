import test from 'node:test';
import assert from 'node:assert/strict';
import {randomBytes} from 'node:crypto';
import fs from 'node:fs';
import {createCoachWaAgent} from '../src/coach-wa-chat.js';
import {createWaDrafts} from '../src/coach-wa-drafts.js';
import {createWaCloud,waCloudConfig} from '../src/coach-wa-cloud.js';
import {createWaCloudTemplate} from '../src/coach-wa-cloud-template.js';
import {coachMetaContactKey,createCoachMetaWebhook} from '../src/coach-meta-webhook.js';
import {connectionConfig,registerCoachConnections} from '../src/coach-connections.js';

// Integración ventana de 24 h (webhook) / plantilla verificada / revisión cifrada.
// Solo dobles de prueba: dominio .test, números 555-01xx, token y plantilla ficticios.
const TOKEN='test-token-'+'w'.repeat(24);
const PHONE_ID='1000000000001';
const env={COACH_WA_CLOUD_SELLER_EMAIL:'a@example.test',COACH_WA_CLOUD_PHONE:'+13125550177',
  COACH_WA_CLOUD_PHONE_NUMBER_ID:PHONE_ID,COACH_WA_CLOUD_ACCESS_TOKEN:TOKEN};
const TO='+525555010101';
const BODY='Hola, soy tu asesor de TEC.\nQueda pendiente tu seguimiento. ¿Te puedo escribir por aquí?';
const row=(extra={})=>({name:'seguimiento_tec',language:'es_MX',status:'APPROVED',category:'UTILITY',components:[{type:'BODY',text:BODY}],...extra});
const TPL={wabaId:'2000000000002',templateName:'seguimiento_tec',language:'es_MX',env:{}};
const sendCmd=id=>'Enviar WhatsApp Business '+id;

function graph({rows=[row()],post}={}) {
  const calls=[];
  const state={rows};
  const fetch=async(url,init={})=>{
    const body=init.body?JSON.parse(init.body):null;
    calls.push({url,method:init.method||'GET',body});
    if (url.includes('/phone_numbers')) return Response.json({data:[{id:PHONE_ID,display_phone_number:'+1 312-555-0177'}]});
    if (url.includes('/message_templates')) return Response.json({data:state.rows});
    if (url.endsWith('/messages')) {
      const r=post?await post(body):{status:200,body:{messages:[{id:'wamid.T'+calls.length,message_status:'accepted'}]}};
      return Response.json(r.body,{status:r.status});
    }
    return Response.json({display_phone_number:'+1 312-555-0177',verified_name:'TEC Prueba'});
  };
  return {fetch,calls,state,posts:()=>calls.filter(c=>c.method==='POST')};
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
    findOneAndUpdate(q,u){const d=find(q);const before=d?structuredClone(d):null;if (d) apply(d,u);return {lean:async()=>before};}
  };
}

// open: función de ventana; null = sin consulta inyectada (valor por defecto del runtime).
// templates: 'ready' | 'missing' (sin WABA/nombre/idioma) | 'none' (sin módulo).
function fixture({open=()=>true,templates='ready',rows,post,cloudEnv=env,key=randomBytes(32),store=memoryStore()}={}) {
  let time=Date.UTC(2026,10,20,17);
  const now=()=>time,g=graph({rows,post}),windowCalls=[];
  const config=waCloudConfig(cloudEnv);
  const cloud=createWaCloud(config,{fetch:g.fetch});
  const tpl=templates==='none'?null:createWaCloudTemplate(config,{fetch:g.fetch,...(templates==='ready'?TPL:{env:{}})});
  const isWindowOpen=open===null?null:async to=>{windowCalls.push(to);return open(to);};
  const waDrafts=createWaDrafts({key,store,now});
  const agent=createCoachWaAgent({config:{key,pilot:['b@example.test']},switched:()=>false,waDrafts,now,cloud,isWindowOpen,templates:tpl,
    rewrite:async()=>({body:'¿Te va bien el viernes?'}),findLeads:async()=>[]});
  const replies=[];
  const send=async(text,{owner='a',login='active',sessionId='chat-a',version=2,...body}={})=>{
    const r=await agent({req:{body:{accountId:owner,coachWhatsAppVersion:version,...body}},
      auth:{user:{_id:owner,email:owner+'@example.test'},session:{tokenHash:login}},sessionId,text});
    replies.push(r);return r;
  };
  return {send,g,store,waDrafts,windowCalls,replies,key,now,config,advance:ms=>{time+=ms;}};
}
const compose=f=>f.send('Prepara un WhatsApp para +52 55 5501 0101. Mensaje: Te mando el precio especial');

test('sin consulta de ventana (predeterminado) no hay texto libre; sin plantilla explica qué falta y no da botón',async()=>{
  const f=fixture({open:null,templates:'missing'});
  const r=await compose(f);
  assert.deepEqual(r.coachHelpActions.map(a=>a.type),['connections']);
  assert.match(r.respuesta,/últimas 24 horas/);
  assert.match(r.respuesta,/WABA/);assert.match(r.respuesta,/nombre exacto de una plantilla/);assert.match(r.respuesta,/idioma de esa plantilla/);
  assert.match(r.respuesta,/no preparó ningún envío/);
  assert.equal(r.coachWhatsAppPending,'');
  assert.equal(f.store.docs.size,0);assert.equal(f.g.calls.length,0);

  // Sin módulo de plantillas, ventana que falla o plantilla no aprobada: tampoco hay botón.
  const none=await compose(fixture({open:()=>false,templates:'none'}));
  assert.match(none.respuesta,/TEC todavía no configuró una plantilla aprobada/);
  assert.ok(!none.coachHelpActions.some(a=>a.type==='waCloudSend'));
  const failing=fixture({open:()=>{throw Error('caída');},templates:'missing'});
  assert.ok(!(await compose(failing)).coachHelpActions.some(a=>a.type==='waCloudSend'));
  const paused=fixture({open:()=>false,rows:[row({status:'PAUSED'})]});
  const p=await compose(paused);
  assert.match(p.respuesta,/no está aprobada por Meta/);
  assert.ok(!p.coachHelpActions.some(a=>a.type==='waCloudSend'));
  assert.equal(paused.g.posts().length,0);assert.equal(paused.store.docs.size,0);
});

test('ventana abierta: texto libre revisado; se revalida justo antes del API y si se cerró no se envía ni se convierte en plantilla',async()=>{
  let isOpen=true;
  const f=fixture({open:()=>isOpen});
  const d=await compose(f);
  const [btn]=d.coachHelpActions;
  assert.deepEqual(d.coachHelpActions.map(a=>a.type),['waCloudSend','waCancel']);
  assert.equal(btn.text,'Te mando el precio especial');
  assert.match(d.respuesta,/justo antes de enviarlo/);
  assert.deepEqual(f.windowCalls,[TO]);
  isOpen=false;
  const r=await f.send(sendCmd(btn.id));
  assert.match(r.respuesta,/24 horas.*no envió tu texto ni lo cambió por una plantilla.*no se envió/s);
  assert.deepEqual(r.coachHelpActions,[{type:'waCloudStatus',state:'failed',label:'No enviado'}]);
  assert.deepEqual(f.windowCalls,[TO,TO],'revalida en el envío');
  assert.equal(f.g.posts().length,0);
  assert.ok(!f.g.calls.some(c=>c.url.includes('message_templates')),'no busca plantilla tras confirmar');
  isOpen=true;
  assert.match((await f.send(sendCmd(btn.id))).respuesta,/no se envió/);
  assert.equal(f.g.posts().length,0,'la revisión fallida no se reintenta');

  const ok=fixture({open:()=>true});
  const sent=await ok.send(sendCmd((await compose(ok)).coachHelpActions[0].id));
  assert.equal(ok.g.posts().length,1);
  assert.equal(ok.g.posts()[0].body.type,'text');assert.equal(ok.g.posts()[0].body.text.body,'Te mando el precio especial');
  assert.deepEqual(sent.coachHelpActions,[{type:'waCloudStatus',state:'accepted',label:'Aceptado por Meta'}]);
});

test('ventana cerrada con plantilla verificada: texto fijo exacto, aviso explícito, sin edición y envío de plantilla una sola vez',async()=>{
  const f=fixture({open:()=>false});
  const d=await compose(f);
  const [btn]=d.coachHelpActions;
  assert.deepEqual(d.coachHelpActions.map(a=>a.type),['waCloudSend','waCancel']);
  assert.equal(btn.text,BODY);assert.equal(btn.to,TO);
  assert.ok(d.respuesta.includes(BODY));
  assert.match(d.respuesta,/Plantilla en Meta: seguimiento_tec · idioma es_MX/);
  assert.match(d.respuesta,/texto fijo/);assert.match(d.respuesta,/distinto/);
  assert.match(d.respuesta,/aceptó recibir mensajes/);assert.match(d.respuesta,/corresponde a la gestión que te pidió/);
  assert.doesNotMatch(d.respuesta,/precio especial/);
  // Nombre, idioma, texto y destino quedan solo dentro del sobre cifrado de la revisión.
  const stored=JSON.stringify([...f.store.docs.values()]);
  for (const plain of ['525555010101','precio especial','seguimiento_tec','es_MX','asesor de TEC']) assert.ok(!stored.includes(plain),plain);

  const edit=await f.send('Mejor el viernes',{coachWhatsAppPending:d.coachWhatsAppPending});
  assert.deepEqual(edit.coachHelpActions,[]);assert.match(edit.respuesta,/texto es fijo/);
  const replace=await f.send('Mensaje: Otro texto libre',{coachWhatsAppPending:d.coachWhatsAppPending});
  assert.ok(!replace.coachHelpActions.some(a=>a.type==='waCloudSend'));
  const ownSession=(await import('node:crypto')).createHash('sha256').update('active:chat-a').digest('hex');
  await assert.rejects(f.waDrafts.revise({owner:'a',session:ownSession,channel:'cloud',grant:f.config.grant,id:btn.id,body:'Otro'}),e=>e.fixedTemplate===true);
  const again=await f.send('Sí',{coachWhatsAppPending:d.coachWhatsAppPending});
  assert.equal(again.coachHelpActions[0].id,btn.id);assert.equal(again.coachHelpActions[0].text,BODY);
  assert.equal(f.g.posts().length,0);

  const sent=await f.send(sendCmd(btn.id));
  assert.equal(f.g.posts().length,1);
  assert.deepEqual(f.g.posts()[0].body,{messaging_product:'whatsapp',recipient_type:'individual',to:'525555010101',type:'template',
    template:{name:'seguimiento_tec',language:{code:'es_MX'}}});
  assert.match(sent.respuesta,/Meta aceptó la plantilla aprobada para \+525555010101/);
  assert.match(sent.respuesta,/no confirma la entrega ni la lectura/);
  assert.deepEqual(sent.coachHelpActions,[{type:'waCloudStatus',state:'accepted',label:'Aceptado por Meta'}]);
  assert.doesNotMatch(JSON.stringify(sent),/delivered|entregado/);
  assert.match((await f.send(sendCmd(btn.id))).respuesta,/Meta aceptó la plantilla/);
  assert.equal(f.g.posts().length,1);
});

test('una ventana que se abre después no convierte la plantilla confirmada en texto libre',async()=>{
  let isOpen=false;
  const f=fixture({open:()=>isOpen});
  const d=await compose(f);
  isOpen=true;
  await f.send(sendCmd(d.coachHelpActions[0].id));
  assert.equal(f.g.posts().length,1);
  assert.equal(f.g.posts()[0].body.type,'template');assert.equal(f.g.posts()[0].body.text,undefined);
  assert.deepEqual(f.windowCalls,[TO],'el envío de plantilla no consulta la ventana');
});

test('plantilla cambiada en Meta o resultado dudoso: no envía, no reintenta y no recomienda otro envío',async()=>{
  const f=fixture({open:()=>false});
  const d=await compose(f);
  f.g.state.rows=[row({components:[{type:'BODY',text:BODY+' Nuevo.'}]})];
  const changed=await f.send(sendCmd(d.coachHelpActions[0].id));
  assert.match(changed.respuesta,/La plantilla cambió.*no se envió/s);
  assert.equal(f.g.posts().length,0);

  const slow=fixture({open:()=>false,post:async()=>{throw Object.assign(Error('timeout'),{name:'TimeoutError'});}});
  const s=await compose(slow);
  const unknown=await slow.send(sendCmd(s.coachHelpActions[0].id));
  assert.match(unknown.respuesta,/No pude confirmar si Meta aceptó.*no lo reintentará ni lo enviará de otra forma/s);
  assert.doesNotMatch(unknown.respuesta,/prepár|vuelve a (?:enviar|intentar)|intenta de nuevo/i);
  assert.deepEqual(unknown.coachHelpActions,[{type:'waCloudStatus',state:'unknown',label:'Resultado sin confirmar'}]);
  assert.match((await slow.send(sendCmd(s.coachHelpActions[0].id))).respuesta,/No pude confirmar/);
  assert.equal(slow.g.posts().length,1);
});

test('el grant sigue aislado al único vendedor, a su sesión y a la versión de Graph; sin secretos en respuestas',async()=>{
  const key=randomBytes(32),store=memoryStore();
  const f=fixture({open:()=>false,key,store});
  const d=await compose(f);
  const id=d.coachHelpActions[0].id;
  assert.match((await f.send(sendCmd(id),{owner:'b'})).respuesta,/no tiene una línea/);
  assert.match((await f.send(sendCmd(id),{sessionId:'otra'})).respuesta,/caducó, cambió/);
  assert.match((await f.send(sendCmd(id),{login:'otra'})).respuesta,/caducó, cambió/);
  const other=fixture({open:()=>false,key,store,cloudEnv:{...env,COACH_WA_CLOUD_GRAPH_VERSION:'v26.0'}});
  assert.notEqual(other.config.grant,f.config.grant);
  assert.match((await other.send(sendCmd(id))).respuesta,/caducó, cambió/);
  const old=await f.send('Prepara un WhatsApp para +525555010102. Mensaje: Hola',{version:1});
  assert.match(old.respuesta,/Actualiza el Coach/);assert.deepEqual(old.coachHelpActions,[]);
  assert.equal(f.g.posts().length+other.g.posts().length,0);
  const all=JSON.stringify([...f.replies,...other.replies]);
  assert.ok(!all.includes(TOKEN));assert.ok(!all.includes(PHONE_ID));
});

test('registerCoachConnections inyecta ventana y plantillas; sin ventana cierra por defecto; server.js pasa la consulta del webhook',async()=>{
  const config=connectionConfig({COACH_GMAIL_ENCRYPTION_KEY:randomBytes(32).toString('base64'),COACH_CONNECTIONS_PILOT_EMAILS:'b@example.test'});
  const g=graph();
  const cloudConfig=waCloudConfig(env);
  const build=extra=>registerCoachConnections({get(){},post(){}},{config,switched:()=>false,
    requireActive:async()=>null,store:{findById:()=>({lean:async()=>null})},
    waCloud:createWaCloud(cloudConfig,{fetch:g.fetch}),waDrafts:createWaDrafts({key:config.key,store:memoryStore()}),
    waTemplates:createWaCloudTemplate(cloudConfig,{fetch:g.fetch,env:{}}),...extra});
  const ask=agent=>agent({req:{body:{accountId:'a',coachWhatsAppVersion:2}},auth:{user:{_id:'a',email:'a@example.test'},session:{tokenHash:'s'}},
    sessionId:'chat',text:'Prepara un WhatsApp para +525555010101. Mensaje: Hola'});
  const closed=await ask(build({}));
  assert.match(closed.respuesta,/no preparó ningún envío/);assert.ok(!closed.coachHelpActions.some(a=>a.type==='waCloudSend'));
  const seen=[];
  const open=await ask(build({waWindowOpen:async to=>{seen.push(to);return true;}}));
  assert.deepEqual(open.coachHelpActions.map(a=>a.type),['waCloudSend','waCancel']);
  assert.deepEqual(seen,[TO]);
  assert.equal(g.posts().length,0);

  const source=fs.readFileSync(new URL('../server.js',import.meta.url),'utf8');
  const hookAt=source.indexOf('const coachMetaWebhook = createCoachMetaWebhook(');
  const passAt=source.indexOf('waWindowOpen: coachMetaWebhook.isWindowOpen');
  const registerAt=source.indexOf('registerCoachConnections(app, {');
  assert.ok(hookAt>0 && registerAt>hookAt && passAt>registerAt);
});

test('una sola variable para la línea en webhook y Cloud, v27.0 por defecto y en el ejemplo',async()=>{
  const {COACH_WA_CLOUD_GRAPH_VERSION:_,...noVersion}=env;
  assert.equal(waCloudConfig(noVersion).version,'v27.0');
  const SECRET='test-app-secret';
  const key=coachMetaContactKey({appSecret:SECRET,phoneId:PHONE_ID,waId:TO});
  const at=new Date(Date.UTC(2026,10,20,16));
  const store={findOne:q=>({lean:async()=>q._id===key&&q.expiresAt.$gt<new Date(at.getTime()+86400000)?{lastInboundAt:at}:null})};
  const now=()=>Date.UTC(2026,10,20,17);
  const hook=createCoachMetaWebhook({store,now,env:{COACH_WA_META_APP_SECRET:SECRET,COACH_WA_CLOUD_PHONE_NUMBER_ID:PHONE_ID}});
  assert.equal(await hook.isWindowOpen(TO),true);
  const legacy=createCoachMetaWebhook({store,now,env:{COACH_WA_META_APP_SECRET:SECRET,COACH_WA_META_PHONE_ID:PHONE_ID}});
  assert.equal(await legacy.isWindowOpen(TO),false,'la variable antigua ya no configura la línea');
  assert.doesNotMatch(fs.readFileSync(new URL('../src/coach-meta-webhook.js',import.meta.url),'utf8'),/COACH_WA_META_PHONE_ID/);
  const example=fs.readFileSync(new URL('../.env.example',import.meta.url),'utf8');
  for (const line of ['COACH_WA_CLOUD_PHONE_NUMBER_ID=','COACH_WA_META_APP_SECRET=','COACH_WA_WEBHOOK_VERIFY_TOKEN=','COACH_WA_CLOUD_GRAPH_VERSION=v27.0',
    'COACH_WA_CLOUD_WABA_ID=','COACH_WA_CLOUD_TEMPLATE_NAME=','COACH_WA_CLOUD_TEMPLATE_LANGUAGE=']) assert.ok(example.includes('\n'+line),line);
  assert.doesNotMatch(example,/COACH_WA_META_PHONE_ID/);
});

test('Mis conexiones mantiene el flujo manual mientras el alta Business está incompleta y explica la plantilla',async()=>{
  const routes=new Map();
  const config=connectionConfig({COACH_GMAIL_ENCRYPTION_KEY:randomBytes(32).toString('base64'),COACH_CONNECTIONS_PILOT_EMAILS:'a@example.test'});
  const register=(cloudEnv,waTemplates)=>registerCoachConnections({get:(p,h)=>routes.set(p,h),post:(p,h)=>routes.set(p,h)},{config,switched:()=>false,
    waCloud:createWaCloud(waCloudConfig(cloudEnv),{fetch:graph().fetch}),waTemplates,
    waProfile:{load:async()=>'',save:async()=>true,remove:async()=>{}},
    requireActive:async()=>({user:{_id:'a',email:'a@example.test'},session:{tokenHash:'s-a'}}),
    store:{findById:()=>({lean:async()=>null})}});
  async function open() {
    const res={set(){return this;},vary(){return this;},status(c){this.code=c;return this;},send(b){this.body=b;},redirect(){}};
    await routes.get('/mis-conexiones/')({path:'/mis-conexiones/',method:'GET',query:{},body:{},get:()=>null},res);
    return res.body;
  }
  register({...env,COACH_WA_CLOUD_ACCESS_TOKEN:''});
  const pending=await open();
  assert.match(pending,/<h2>WhatsApp<\/h2>/);assert.doesNotMatch(pending,/WhatsApp Business<\/h2>/);
  assert.match(pending,/TEC todavía no completó la conexión de tu línea de WhatsApp Business/);
  assert.match(pending,/Mi número de WhatsApp \(con prefijo de país\)/);

  const config2=waCloudConfig(env);
  register(env,createWaCloudTemplate(config2,{fetch:graph().fetch,...TPL}));
  const ready=await open();
  assert.match(ready,/<h2>WhatsApp Business<\/h2>/);
  assert.match(ready,/plantilla de texto fijo aprobada por Meta/);assert.doesNotMatch(ready,/aún no envía/);
  register(env,createWaCloudTemplate(config2,{fetch:graph().fetch,env:{}}));
  assert.match(await open(),/todavía no configuró una plantilla aprobada/);
  for (const page of [pending,ready]) assert.ok(!page.includes(TOKEN) && !page.includes(PHONE_ID) && !page.includes('2000000000002'));
});
