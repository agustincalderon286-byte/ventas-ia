// Revisión de conversaciones por el administrador. Todo es ficticio: 555-01xx y example.invalid.
import test from 'node:test';
import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import {createAssistantLog, createChatReview, registerChatReview, scrubContact, CHAT_RETENTION_DAYS, ASSISTANT_RETENTION_DAYS} from '../src/coach-chat-review.js';
import {createSellerAgent} from '../src/coach-agent-seller.js';
import {CONSENT_VERSION} from '../src/coach-agent-consent.js';

const day=86400000, NOW=Date.parse('2026-11-23T15:00:00Z');
const ana='0000000000000000000000a1', beto='0000000000000000000000b2';
const key=Buffer.alloc(32,7);

// Colección en memoria con lo que usa el módulo: find/sort/limit/lean, distinct, create, updateOne, updateMany, deleteOne y deleteMany.
function collection(rows=[]) {
  const test=(value,rule)=>{
    if (rule===null || typeof rule!=='object' || rule instanceof Date) return Array.isArray(value) ? value.includes(rule) : String(value)===String(rule);
    return Object.entries(rule).every(([op,want])=>({$in:()=>want.map(String).includes(String(value)),$nin:()=>!want.map(v=>v===null?'null':String(v)).includes(value==null?'null':String(value)),
      $ne:()=>value!==want,$gte:()=>new Date(value)>=new Date(want),$lte:()=>new Date(value)<=new Date(want),$lt:()=>new Date(value)<new Date(want)})[op]());
  };
  const match=filter=>row=>Object.entries(filter).every(([field,rule])=>test(row[field],rule));
  const query=found=>({sort(order){const [[field,dir]]=Object.entries(order);found.sort((a,b)=>(new Date(a[field])-new Date(b[field]))*dir);return this;},
    limit(n){found=found.slice(0,n);return this;},lean:async()=>structuredClone(found)});
  return {rows,
    find:(filter={})=>query(rows.filter(match(filter))),
    distinct:async(field,filter)=>[...new Set(rows.filter(match(filter)).flatMap(row=>row[field]))],
    create:async doc=>{rows.push(structuredClone(doc));},
    updateOne:async(filter,{$set},options={})=>{const row=rows.find(match(filter));if (row) Object.assign(row,$set);else if (options.upsert) rows.push({...filter,...$set});},
    updateMany:async(filter,{$set})=>{rows.filter(match(filter)).forEach(row=>Object.assign(row,$set));},
    deleteOne:async filter=>{const index=rows.findIndex(match(filter));if (index>=0) rows.splice(index,1);},
    deleteMany:async filter=>{const keep=rows.filter(row=>!match(filter)(row));const deletedCount=rows.length-keep.length;rows.splice(0,rows.length,...keep);return {deletedCount};}};
}
let serial=0;
const message=(seller,session,role,content,minutesAgo,intent='coach_chat')=>({_id:(++serial).toString(16).padStart(24,'0'),sessionId:session,role,content,intent,
  detectedTopics:role==='user' ? ['coach_user:'+seller] : [],createdAt:new Date(NOW-minutesAgo*60000)});

function setup() {
  const Message=collection([
    message(ana,'s-ana','user','¿Cómo respondo si dicen que está caro?',60),
    message(ana,'s-ana','assistant','Pregunta qué parte del precio le preocupa.',59),
    message(ana,'s-ana','user','¿Y si pide descuento?',50),
    message(ana,'s-ana','assistant','Habla del valor antes del precio.',49),
    message(ana,'s-ana','user','[Acción en las conexiones del vendedor]',40),
    message(beto,'s-beto','user','¿Cuánto cuesta la olla de 6 litros?',30,'coach_help'),
    message(beto,'s-beto','assistant','Consulta la lista de precios vigente.',29,'coach_help'),
    message(beto,'s-beto','user','Pregunta sin respuesta',20),
    message(ana,'s-viejo','user','Pregunta de hace siete meses',210*24*60),
    {...message(ana,'s-chef','user','Mensaje de otro producto',300*24*60,'chef_lead')}]);
  const Users=collection([{_id:ana,name:'Ana Ficticia',email:'ana@example.invalid'},{_id:beto,name:'Beto Prueba',email:'beto@example.invalid'}]);
  const Profiles=collection([{userId:ana,lastQuestion:'vieja',lastCoachReply:'vieja',recentSessionsSummary:[{summary:'x'}],lastInteractionAt:new Date(NOW-200*day)},
    {userId:beto,lastQuestion:'reciente',lastCoachReply:'r',recentSessionsSummary:[{summary:'y'}],lastInteractionAt:new Date(NOW-day)}]);
  const turns=collection(), flags=collection();
  const assistant=createAssistantLog({key,store:turns,now:()=>NOW});
  const review=createChatReview({Message,Users,Profiles,assistant,flags,now:()=>NOW});
  return {Message,Users,Profiles,turns,flags,assistant,review};
}

test('los datos de contacto se quitan del texto que se conserva del asistente',()=>{
  assert.equal(scrubContact('Llama a Pedro al [[tel:ab12cd]] o al (312) 555-0101; vive en [[dir:0f0f0f]] a [[min:123abc]]. Correo pedro@example.invalid o [[correo:aaaaaa]].'),
    'Llama a Pedro al [teléfono] o al [teléfono]; vive en [dirección] a [minutos]. Correo [correo] o [correo].');
  assert.equal(scrubContact('da de alta a Nora 3125550144 y +523312345678, cita el 2026-11-26 a las 18:00'),'da de alta a Nora [teléfono] y [teléfono], cita el 2026-11-26 a las 18:00');
});

test('el registro del asistente se guarda cifrado, sin datos de contacto y caduca a los 30 días',async()=>{
  const {assistant,turns}=setup();
  const id=await assistant.record({owner:ana,question:'dame el telefono de Pedro, el mio es 312-555-0199',answer:'El teléfono de Pedro Ficticio es [[tel:ab12cd]].',
    tools:[{name:'buscar_referidos',estado:'ok'}]});
  const [row]=turns.rows;
  assert.equal(row.owner,ana);
  assert.equal(row.expires.getTime(),NOW+ASSISTANT_RETENTION_DAYS*day);
  assert.doesNotMatch(JSON.stringify(row),/Pedro|telefono|555/);
  const [turn]=await assistant.list({owner:ana,from:new Date(NOW-day),to:new Date(NOW),limit:10});
  assert.deepEqual({id:turn.id,question:turn.question,answer:turn.answer,tools:turn.tools},
    {id,question:'dame el telefono de Pedro, el mio es [teléfono]',answer:'El teléfono de Pedro Ficticio es [teléfono].',tools:['buscar_referidos:ok']});
  // Con otra llave no se puede leer.
  assert.deepEqual(await createAssistantLog({key:Buffer.alloc(32,9),store:turns,now:()=>NOW}).list({from:new Date(0),to:new Date(NOW),limit:10}),[]);
});

test('la revisión empareja cada pregunta con su respuesta, por vendedor, con búsqueda y origen',async()=>{
  const {review,assistant}=setup();
  await assistant.record({owner:ana,question:'que tengo mañana',answer:'Tienes una demo a las 4.',tools:[{name:'consultar_agenda',estado:'ok'}]});
  const all=await review.list();
  assert.deepEqual(all.entries.map(entry=>[entry.source,entry.sellerName,entry.question,entry.answer]),[
    ['asistente','Ana Ficticia','que tengo mañana','Tienes una demo a las 4.'],
    ['coach','Beto Prueba','Pregunta sin respuesta',''],
    ['coach','Beto Prueba','¿Cuánto cuesta la olla de 6 litros?','Consulta la lista de precios vigente.'],
    ['coach','Ana Ficticia','¿Y si pide descuento?','Habla del valor antes del precio.'],
    ['coach','Ana Ficticia','¿Cómo respondo si dicen que está caro?','Pregunta qué parte del precio le preocupa.']]);
  assert.equal(all.total,5);
  assert.deepEqual(all.entries[0].tools,['consultar_agenda:ok']);
  assert.equal(all.entries[0].sellerEmail,'ana@example.invalid');
  assert.deepEqual((await review.list({seller:beto})).entries.map(entry=>entry.sellerName),['Beto Prueba','Beto Prueba']);
  assert.deepEqual((await review.list({search:'DESCUENTO'})).entries.map(entry=>entry.question),['¿Y si pide descuento?']);
  assert.deepEqual((await review.list({search:'lista de precios'})).entries.map(entry=>entry.question),['¿Cuánto cuesta la olla de 6 litros?']);
  assert.equal((await review.list({source:'asistente'})).total,1);
  assert.equal((await review.list({source:'coach'})).total,4);
  assert.equal((await review.list({seller:'no-es-id'})).total,0);
  // El periodo por defecto son 30 días; con fechas se llega a lo anterior.
  assert.equal((await review.list({from:'2026-04-01',to:'2026-05-01'})).entries[0].question,'Pregunta de hace siete meses');
  assert.deepEqual((await review.sellers()).map(person=>person.name),['Ana Ficticia','Beto Prueba']);
});

test('una respuesta se marca como mala con una nota, se lista aparte y la marca caduca con la conversación',async()=>{
  const {review,flags,assistant}=setup();
  const [turnEntry,,price]=(await (async()=>{await assistant.record({owner:ana,question:'q',answer:'a'});return review.list();})()).entries;
  assert.equal(await review.flag({id:price.id,note:'  Debió dar el precio\n exacto ',by:ana}),true);
  assert.equal(await review.flag({id:turnEntry.id,note:'',by:ana}),true);
  assert.equal(await review.flag({id:'c:'+'f'.repeat(24),note:'x',by:ana}),false);
  assert.equal(await review.flag({id:'otra cosa',note:'x',by:ana}),false);
  const priceFlag=flags.rows.find(row=>row._id===price.id);
  assert.equal(priceFlag.note,'Debió dar el precio exacto');
  assert.equal(priceFlag.expires.getTime(),Date.parse(price.at)+CHAT_RETENTION_DAYS*day);
  assert.equal(flags.rows.find(row=>row._id===turnEntry.id).expires.getTime(),NOW+ASSISTANT_RETENTION_DAYS*day);
  const marked=await review.list({flagged:true});
  assert.deepEqual(marked.entries.map(entry=>[entry.source,entry.flag.note]).sort(),[['asistente',''],['coach','Debió dar el precio exacto']]);
  assert.equal((await review.list()).entries.find(entry=>entry.id===price.id).flag.note,'Debió dar el precio exacto');
  await review.unflag(price.id);
  assert.equal((await review.list({flagged:true})).total,1);
});

test('a los 6 meses se borran las conversaciones del Coach y el texto del perfil; una solicitud borra todo lo de un vendedor',async()=>{
  const {review,Message,Profiles,assistant,turns}=setup();
  assert.equal(await review.purge(),1);
  assert.equal(Message.rows.some(row=>row.content==='Pregunta de hace siete meses'),false);
  assert.equal(Message.rows.some(row=>row.intent==='chef_lead'),true,'otros productos no se tocan');
  assert.deepEqual(Profiles.rows.map(row=>[row.lastQuestion,row.recentSessionsSummary.length]),[['',0],['reciente',1]]);
  await assistant.record({owner:beto,question:'q',answer:'a'});
  await assistant.record({owner:ana,question:'q',answer:'a'});
  assert.equal(await review.eraseSeller(beto),4);
  assert.equal(Message.rows.some(row=>row.detectedTopics.includes('coach_user:'+beto)),false);
  assert.deepEqual(turns.rows.map(row=>row.owner),[ana]);
  assert.equal(Profiles.rows[1].lastQuestion,'');
  assert.equal(await review.eraseSeller('x'),0);
});

test('solo el administrador entra a las rutas, y marcar exige una petición del propio sitio',async()=>{
  const {review}=setup();
  const routes=new Map();
  const app={get:(path,handler)=>routes.set('GET '+path,handler),post:(path,handler)=>routes.set('POST '+path,handler)};
  registerChatReview(app,{review,sameOrigin:req=>req.site==='same-origin',
    requireAdmin:async(req,res)=>{ if (req.admin) return {user:{_id:ana}}; res.status(403).json({error:'Solo administradores.'}); return null; }});
  const call=async(route,req)=>{
    const res={code:200,headers:{},set(name,value){this.headers[name]=value;return this;},status(code){this.code=code;return this;},json(body){this.body=body;return this;}};
    await routes.get(route)(req,res);
    return res;
  };
  const denied=await call('GET /api/control/coach-chats',{query:{}});
  assert.equal(denied.code,403);
  assert.equal(JSON.stringify(denied.body).includes('caro'),false);
  assert.equal((await call('POST /api/control/coach-chats/marca',{body:{id:'c:'+'1'.padStart(24,'0')},site:'same-origin'})).code,403);

  const listed=await call('GET /api/control/coach-chats',{admin:true,query:{vendedor:ana,buscar:'caro',pagina:'1'}});
  assert.equal(listed.headers['Cache-Control'],'private, no-store');
  assert.deepEqual(listed.body.entries.map(entry=>entry.question),['¿Cómo respondo si dicen que está caro?']);
  assert.deepEqual(listed.body.retention,{coachDays:180,assistantDays:30});
  assert.equal(listed.body.sellers.length,2);
  const id=listed.body.entries[0].id;
  assert.equal((await call('POST /api/control/coach-chats/marca',{admin:true,body:{id,nota:'Muy general'},site:'cross-site'})).code,403);
  assert.equal((await call('POST /api/control/coach-chats/marca',{admin:true,body:{id,nota:'Muy general'},site:'same-origin'})).body.ok,true);
  assert.equal((await call('GET /api/control/coach-chats',{admin:true,query:{marcadas:'1'}})).body.entries[0].flag.note,'Muy general');
  assert.equal((await call('POST /api/control/coach-chats/marca',{admin:true,body:{id,quitar:true},site:'same-origin'})).body.ok,true);
  assert.equal((await call('POST /api/control/coach-chats/marca',{admin:true,body:{id:'c:'+'e'.repeat(24)},site:'same-origin'})).code,404);
});

test('el asistente deja para la revisión su texto con marcadores, nunca el teléfono real que vio el vendedor',async()=>{
  const turns=collection();
  const auth={user:{_id:ana,email:'A@example.invalid',name:'Ana',accountType:'owner'},session:{tokenHash:'th',accessMode:'direct'}};
  const make=reviewLog=>createSellerAgent({config:{loopPilot:['a@example.invalid'],agentModel:'m'},
    bindTools:(actor,refs,turn)=>async()=>{turn.secrets={'tel:ab12cd':'(312) 555-0101'};return {estado:'ok'};},
    memory:{load:async()=>null,save:async()=>'r'},plans:{load:async()=>null,save:async()=>null},mailContext:async()=>'',inboxToken:()=>'',
    budget:{allow:async()=>true,charge:async()=>0},consent:{accepted:async()=>new Date(NOW)},reviewLog,readSession:async()=>auth,switched:()=>false,
    run:async options=>{await options.execute('buscar_referidos',{});return {text:'El teléfono de Pedro Ficticio es [[tel:ab12cd]].',trace:[{name:'buscar_referidos',estado:'ok'}],steps:1,usage:{inputTokens:1,cachedInputTokens:0,outputTokens:1}};},
    now:()=>NOW,log:()=>{}});
  const ask=agent=>agent({req:{body:{coachPlanVersion:1,accountId:ana,coachTimeZone:'America/Chicago'}},auth,sessionId:'conv',text:'dame el telefono de pedro'});
  const log=createAssistantLog({key,store:turns,now:()=>NOW});
  const reply=await ask(make(log));
  assert.match(reply.respuesta,/\(312\) 555-0101/);
  const [turn]=await log.list({owner:ana,from:new Date(NOW-day),to:new Date(NOW+day),limit:5});
  assert.deepEqual([turn.question,turn.answer,turn.tools],['dame el telefono de pedro','El teléfono de Pedro Ficticio es [teléfono].',['buscar_referidos:ok']]);
  // Un fallo al guardar el registro no le quita la respuesta al vendedor.
  assert.match((await ask(make({record:async()=>{throw Error('sin base');}}))).respuesta,/555-0101/);
});

test('el permiso y la política de privacidad dicen lo que el código conserva',()=>{
  assert.equal(CONSENT_VERSION,3);
  const privacy=readFileSync(new URL('../public/coach/privacidad/index.html',import.meta.url),'utf8').replace(/\s+/g,' ');
  assert.match(privacy,/TEC conserva las preguntas que haces al Coach y sus respuestas durante 6 meses\. El administrador de TEC puede leerlas/);
  assert.match(privacy,/ningún otro vendedor puede verlas/);
  assert.match(privacy,/durante 30 días, tu pregunta y la respuesta del asistente sin los teléfonos/);
  const page=readFileSync(new URL('../src/coach-connections-page.js',import.meta.url),'utf8');
  assert.match(page,/conserva cifradas durante 30 días tu pregunta y la respuesta del asistente/);
  assert.match(page,/El administrador de TEC puede leerlas/);
  assert.equal(CHAT_RETENTION_DAYS,180);
  assert.equal(ASSISTANT_RETENTION_DAYS,30);
});

test('el servidor exige ser administrador de la Torre y estar en la lista de revisores, y programa el borrado',()=>{
  const server=readFileSync(new URL('../server.js',import.meta.url),'utf8');
  assert.match(server,/const coachChatReviewers = parseCoachEmailList\(process\.env\.COACH_CHAT_REVIEW_EMAILS \|\| ""\);/);
  assert.match(server,/const puedeRevisarConversaciones = userDoc => usuarioPuedeVerTorreControl\(userDoc\) && coachChatReviewers\.includes\(normalizarEmail\(userDoc\.email\)\);/);
  const routes=server.slice(server.indexOf('registerChatReview(app, {'),server.indexOf('app.get(["/control", "/control/"'));
  assert.match(routes,/const auth = await requireControlTowerAccess\(req, res\);\s+if \(!auth\) return null;\s+if \(!puedeRevisarConversaciones\(auth\.user\)\)/);
  assert.match(routes,/if \(!puedeRevisarConversaciones\(auth\.user\)\) return res\.redirect\("\/control\/"\);/);
  assert.match(routes,/needsVerification\(auth\)\) return res\.redirect/);
  assert.match(routes,/sendFile\(path\.join\(PRIVATE_DIR, "tec-chat-review\.html"\)\)/);
  assert.match(server,/void purgeCoachChats\(\);\s+const chatPurgeTimer = setInterval\(purgeCoachChats, 24 \* 60 \* 60 \* 1000\);/);
  // La pantalla escribe el texto de las conversaciones sin interpretarlo como HTML.
  const page=readFileSync(new URL('../public/control/chats.js',import.meta.url),'utf8');
  assert.doesNotMatch(page,/innerHTML|insertAdjacentHTML|document\.write/);
  assert.match(readFileSync(new URL('../private/tec-chat-review.html',import.meta.url),'utf8'),/src="\/control\/chats\.js/);
});
