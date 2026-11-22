import test from 'node:test';
import assert from 'node:assert/strict';
import vm from 'node:vm';
import {readFileSync} from 'node:fs';
import {randomBytes} from 'node:crypto';
import {registerCoachConnections,connectionConfig} from '../src/coach-connections.js';
import {createCoachSheetTools} from '../src/coach-sheets-tools.js';
import {sealToken} from '../src/coach-hycite-gmail.js';
import {scopeCoachChat} from '../src/tec-security.js';

const read=path=>readFileSync(new URL('../'+path,import.meta.url),'utf8');
const server=read('server.js'),web=read('public/coach/app.js');
const owners=['agustin','rigoberto'],email=id=>id+'@example.invalid';
const queue='Dame el siguiente pendiente',sale='De las ventas que Hy Cite me confirmó esta semana, ¿a quién le falta demo de uso?',
  person='Guarda a Elena en mis contactos con el teléfono (312) 555-0137.',trip='¿A qué hora salgo para la demo de las cinco?';
const phone=owner=>owner==='agustin'?'3125550111':'3125550122';
const now=Date.parse('2031-06-12T16:00:00Z'),end='2031-06-12T22:00:00Z';
function code(source,start,end) {
  const i=source.indexOf(start),j=source.indexOf(end,i+start.length);
  assert.ok(i>=0 && j>i,'No se encontró la superficie '+start);
  return source.slice(i,j);
}
function memoryPlans() {
  const records=new Map();
  return {load:async k=>structuredClone(records.get(k)),async save(k,prior,data){
    if (records.get(k)?.revision!==prior) return null;
    const revision=randomBytes(12).toString('hex');records.set(k,{revision,data:structuredClone(data)});return revision;
  }};
}
function fixture() {
  const routes=new Map(), audit=[], writes=[], providerCalls=[],requests=[];
  const key=randomBytes(32),pilots=owners.map(email).join(',');
  const config={...connectionConfig({COACH_GMAIL_CLIENT_ID:'fake-id',COACH_GMAIL_CLIENT_SECRET:'test-only',
    COACH_GMAIL_ENCRYPTION_KEY:key.toString('base64'),COACH_CONNECTIONS_PILOT_EMAILS:pilots,COACH_PLAN_PILOT_EMAILS:pilots,COACH_SHEETS_PILOT_EMAILS:pilots})};
  const active=req=>({user:{_id:req.login,name:req.login,email:email(req.login)},session:{accessMode:'direct',tokenHash:'login-'+req.login}});
  const app={get:(p,h)=>routes.set('GET '+p,h),post:(p,h)=>routes.set('POST '+p,h)};
  const storeFor=value=>({findById(owner){return {lean:async()=>value(owner)};}});
  const personalStore=storeFor(owner=>({mailbox:email(owner),grant:'gmail-'+owner,encryptedToken:sealToken(owner,key,owner)}));
  const sheets={access:async owner=>({token:owner,grant:'sheets-'+owner,spreadsheetId:'test-sheet-'+owner,ticketsSheetId:0,referralsSheetId:3}),
    current:async(owner,g)=>g==='sheets-'+owner};
  const sheetsGoogle={async readFollowups(owner){providerCalls.push(['sheet',owner]);return {locale:'en_US',rows:[{row:2,sheetId:0,kind:'tickets',
    sheetTitle:'BOLETAS NUEVAS',fullName:'Cliente de '+owner,seller:owner,sellerColumn:true,email:'',phone:phone(owner),host:'',
    status:'Pendiente',result:'',history:'Llamar',followupDate:'2031-06-12',editable:{}}]};}};
  const contacts={profile:async owner=>({grant:'contacts-'+owner}),search:async(owner,grant)=>{assert.equal(grant,'contacts-'+owner);return [];},
    create:async(owner,grant,values)=>{assert.equal(grant,'contacts-'+owner);writes.push([owner,values.name]);return {resourceName:'people/test'};}};
  const calendar={getConnection:async owner=>({grant:'calendar-'+owner,calendarId:'calendar-'+owner,availabilityEnabled:true}),
    routeAppointments:async({owner,grant})=>{assert.equal(grant,'calendar-'+owner);return [{id:owner,title:'Demo de '+owner,start:end,location:'110 Main St, Chicago, IL'}];},
    availability:async({owner,grant})=>{assert.equal(grant,'calendar-'+owner);return [];}};
  const runCoachConnectedAgent=registerCoachConnections(app,{config,store:personalStore,
    requireActive:active,switched:req=>!!req.switched,readSheetsSession:active,waProfile:{load:async()=>null},
    calendarStore:storeFor(()=>null),sheetsStore:storeFor(()=>null),
    contactsStore:storeFor(owner=>owner==='agustin'?{mailbox:email(owner),grant:'g',encryptedToken:'private-token'}:null),
    gmail:{refresh:async t=>({access_token:t}),notices:async(owner,opts)=>{assert.equal(opts.forPostSale,true);providerCalls.push(['mail',owner]);
      return {confirmed:[{confirmed:true,order:'T-'+owner,date:'2031-06-12T15:00:00Z',customer:{phone:phone(owner)}}]};}},
    routeDependencies:{calendar,routes:{enabled:false},now:()=>now},
    contactsDependencies:{plans:memoryPlans(),contacts,infer:async()=>({name:'Elena',host:'',sheet:false,readOnly:false})},
    planDependencies:{plans:memoryPlans(),sheets,sheetTools:createCoachSheetTools({google:sheetsGoogle}),now:()=>now,
      interpret:async t=>t===queue?{kind:'plan',operations:['call']}:t===sale?{kind:'plan',operations:['postSale'],salesPeriod:'week'}:{kind:'none'}}});
  const empty=()=>null;
  const hooks={app,scopeCoachChat,runCoachConnectedAgent,requireCoachActivo:active,limpiarCoachHealthSurveyActiveContext:empty,
    limpiarCoachProgram414ActiveContext:empty,limpiarCoachDecisionContext:empty,limpiarCoachOrderCalcContext:empty,limpiarCoachDemoOutcomeContext:empty,limpiarCoachDemoEventPrompt:empty,
    asegurarSesionRedisCargada:empty,obtenerCoachContextoSesion:empty,validarLimiteUsoCoach:()=>({allowed:true}),marcarSesionActiva:empty,
    limpiarMemoriaSesiones:empty,guardarMensajeRaw:async entry=>audit.push(entry),console:{error:e=>{throw e;}}};
  vm.runInNewContext(code(server,'function normalizarModoChat(','function limpiarUrlExterna(')+
    code(server,'function prepararRespuestaStreamingChat(','async function consumirStreamingOpenAiChat(')+
    code(server,'app.post("/chat",','// Central Express 4 async error boundary'),hooks);
  async function serve(method,path,login,body={},extra={}) {
    const res={statusCode:200,headers:{},chunks:[],status(n){this.statusCode=n;return this;},json(v){this.body=v;},send(v){this.body=v;},
      set(){return this;},vary(){return this;},setHeader(k,v){this.headers[k]=v;},write(s){this.chunks.push(JSON.parse(s));},end(){}};
    await routes.get(method+' '+path)({login,body,path,method,query:{},...extra},res);
    return {data:res.body ?? res.chunks.at(-1)?.payload,status:res.statusCode};
  }
  async function chat(login,body) {
    requests.push({...body});
    return (await serve('POST','/chat',login,{accountId:login,sessionId:'chat-piloto-nuevo',mode:'coach',
      coachMailVersion:1,coachPlanVersion:1,coachCalendarVersion:1,coachTimeZone:'America/Chicago',...body})).data;
  }
  return {serve,chat,requests,writes,providerCalls,audit};
}
const property=selector=>/^\[data-([\w-]+)\]$/.exec(selector)?.[1].replace(/-([a-z])/g,(_m,c)=>c.toUpperCase());
class Element {
  constructor(tag){this.tag=tag;this.dataset={};this.children=[];}
  appendChild(el){el.parent=this;this.children.push(el);}
  remove(){this.parent.children=this.parent.children.filter(c=>c!==this);}
  get childElementCount(){return this.children.length;}
  contains(el){return el===this || this.children.some(c=>c.contains(el));}
  matches(selector){
    if (selector.startsWith('a[')) return this.tag==='a' && this.matches(selector.slice(1));
    const p=property(selector);return p?Object.hasOwn(this.dataset,p):selector==='.'+this.className;
  }
  querySelectorAll(selector){return this.children.flatMap(c=>[...(c.matches(selector)?[c]:[]),...c.querySelectorAll(selector)]);}
  querySelector(s){return this.querySelectorAll(s)[0];}
  closest(s){return this.matches(s)?this:this.parent?.closest(s);}
  setAttribute(k,v){this[k]=v;}
  removeAttribute(k){delete this[k];}
  addEventListener(k,f){this['on'+k]=f;}
}
function browser(f) {
  const feed=new Element('main'), msg=[],buttons=()=>msg.at(-1)?.card.children[0]?.children || [];
  const nothing=()=>null;
  let done=Promise.resolve(),finish;
  const ctx={URL,document:{createElement:tag=>new Element(tag)},me:{user:{id:'agustin'}},session:'chat-piloto-nuevo',
    window:{sessionStorage:{getItem:()=>null}},Intl:{DateTimeFormat:()=>({resolvedOptions:()=>({timeZone:'America/Chicago'})})},
    chatMessages:feed,floatingChatMessages:null,coachChatBusy:false,coachVoice:null,chatInput:null,floatingChatInput:null,floatingCoachMode:'idle',
    COACH_WORKSPACE_TAB_KEY:'workspace',getCoachChatSessionId:()=>ctx.session,getCoachVisitorId:()=>'',getCoachDemoStageMeta:()=>({id:'inicio'}),
    getCoachDemoStageId:()=>'',getCoachDemoEvents:()=>[],getActiveCoachHealthSurveyContext:nothing,getActiveCoachProgram414Context:nothing,
    getActiveCoachOrderCalcContext:nothing,getActiveCoachDecisionContext:nothing,getActiveCoachDemoOutcomeContext:nothing,getActiveCoachCrmContext:nothing,
    clearCoachChatInputs(){},setCoachChatStatusText:t=>{ctx.status=t;},toggleCoachChatBusy:value=>{
      ctx.coachChatBusy=value;if(value)done=new Promise(resolve=>{finish=resolve;});else finish?.();},
    addCoachMessage(_root,role,txt){const card=new Element('article');feed.appendChild(card);msg.push({card,role,txt});return [{card}];},
    updateCoachMessage(handles,txt,opts){msg.at(-1).txt=txt;if (opts) ui.renderCoachMessageActions(handles[0].card,opts.actions);},
    streamCoachChatReply:body=>f.chat(ctx.me.user.id,{...body,stream:true}),renderActiveLeadContext(){},renderCoachRepLeadSummary(){}};
  const handlers=code(web.slice(web.indexOf('  coachVoice = window.TecCoachVoice?.init')),'  [chatMessages, floatingChatMessages].filter(Boolean).forEach(root => {','  chatInput?.addEventListener');
  const ui=vm.runInNewContext(code(web,'function renderCoachMessageActions','function addCoachMessage(')+
    code(web,'  const sendCoachMessage = async','  coachVoice = window.TecCoachVoice?.init')+handlers+';({sendCoachMessage,renderCoachMessageActions})',ctx);
  async function click(el,type='click'){
    let prevented=false;feed['on'+type]({target:el,preventDefault(){prevented=true;}});await done;return prevented;
  }
  const say=async t=>{await ui.sendCoachMessage(t);return msg.at(-1).txt;};
  return {...ui,ctx,say,click,buttons};
}

test('del chat a Llamar, rutas y Contactos: la interfaz permite solo la acción de la cuenta y conversación vigentes',async()=>{
  const f=fixture(),b=browser(f);
  assert.match(await b.say(queue),/Cliente de agustin/);
  const tel=b.buttons().find(n=>n.href?.startsWith('tel:'));
  assert.equal(tel.href,'tel:+1'+phone('agustin'));assert.equal(await b.click(tel),false);assert.equal(await b.click(tel,'auxclick'),false);assert.deepEqual(f.writes,[]);
  b.ctx.me.user.id='rigoberto';assert.equal(await b.click(tel),true);assert.equal(await b.click(tel,'auxclick'),true);
  assert.match(await b.say(trip),/dirección vas a salir/);
  assert.equal(tel.href,undefined,'Al retirar una acción de otra cuenta debe perder su href.');
  assert.match(await b.say('Salgo de 30 Lake St, Chicago, IL'),/cupo gratuito/);
  const map=b.buttons().find(n=>n.tag==='a');const link=new URL(map.href);
  assert.equal(link.origin,'https://www.google.com');assert.equal(link.pathname,'/maps/dir/');assert.equal(link.searchParams.get('api'),'1');
  assert.match(map.rel,/noreferrer/);assert.equal(await b.click(map),false);
  b.ctx.session='otro-chat-piloto';assert.equal(await b.click(map),true);assert.equal(await b.click(map,'auxclick'),true);
  assert.match(await b.say(person),/Elena/);
  const save=b.buttons().find(n=>n.dataset.coachContactsAction?.includes('create'));
  assert.equal(f.writes.length,0);
  b.ctx.me.user.id='agustin';assert.equal(await b.click(save),true);assert.equal(f.writes.length,0);
  b.ctx.me.user.id='rigoberto';assert.equal(await b.click(save),true);assert.deepEqual(f.writes,[['rigoberto','Elena']]);
  const stored=JSON.stringify(f.audit);assert.ok(!stored.includes('Elena') && !stored.includes('Lake St') && !stored.includes(phone('agustin')));
});
test('un objetivo nuevo de llamadas cede el foco desde la propuesta abierta de Contactos',async()=>{
 const f=fixture(),b=browser(f);
 await b.say('Guarda a Elena en mis contactos.');
 assert.match(await b.say(queue),/Cliente de agustin/);assert.ok(b.buttons().some(a=>a.href?.startsWith('tel:')));
 assert.deepEqual(f.writes,[]);
});
test('una solicitud de postventa toma el foco de una ruta abierta y solo cruza correo y hoja propios',async()=>{
 const f=fixture();
 for (const owner of owners) {
   const b=browser(f);b.ctx.me.user.id=owner;await b.say(trip);
   assert.match(await b.say(sale),/1 ventas? confirmadas?/);
   assert.ok(f.providerCalls.some(([type,id])=>type==='mail' && id===owner));
   assert.match(JSON.stringify(b.buttons().map(n=>n.textContent)),new RegExp('Cliente de '+owner));
 }
 assert.deepEqual(f.writes,[]);
});
test('Mis conexiones muestra el estado y formulario personal de Contactos y protege al alternar cuentas',async()=>{
 const f=fixture();
 for(const who of owners) {
   const {data,status}=await f.serve('GET','/mis-conexiones/',who);
   assert.equal(status,200);assert.match(data,/Contactos de Google/);
   assert.ok(data.includes(email(who)));assert.ok(!data.includes(email(owners.find(o=>o!==who))));
   const action=who==='agustin'?'disconnect':'start';
   assert.ok(data.includes('action="/api/coach/conexiones/contactos/'+action+'"'));
   assert.match(data,/name="csrf" value="[A-Za-z0-9_-]+"/);
 }
 assert.equal((await f.serve('GET','/mis-conexiones/','agustin',{}, {switched:true})).status,403);
});

test('el render descarta acciones sin cuenta o con enlaces y formatos distintos de los aprobados',()=>{
 const b=browser(fixture()),card=new Element('article'),scope={accountId:'agustin',sessionId:'chat-piloto-nuevo'};
 b.renderCoachMessageActions(card,[
   {...scope,type:'planCall',phone:'312-555-0100'},{type:'planCall',phone:'+13125550100'},
   {...scope,type:'routeMap',url:'https://example.invalid/maps/dir/?api=1'},
   {...scope,type:'routeMap',url:'https://www.google.com/maps/dir/?api=2'},
   {...scope,type:'contactsAction',revision:'a'.repeat(24),index:1,op:'delete',label:'Otro'},
   {type:'contactsAction',revision:'a'.repeat(24),index:0,op:'create',label:'Crear'}
 ]);
 assert.equal(card.children.length,0);
});
