import test from 'node:test';
import assert from 'node:assert/strict';
import { randomBytes } from 'node:crypto';
import { createCoachMailAgent } from '../src/coach-mail-agent.js';
import { sealToken } from '../src/coach-hycite-gmail.js';
import { createMailDrafts } from '../src/coach-mail-drafts.js';

function memoryStore() {
  const docs = new Map();
  function find(filter) {
    return [...docs.values()].find(doc => Object.entries(filter).every(([key, value]) =>
      value?.$gt ? doc[key] > value.$gt : doc[key] === value));
  }
  function modify(filter, update) {
    const doc = find(filter);
    if (!doc) return null;
    const original = structuredClone(doc);
    Object.assign(doc, update.$set);
    for (const key of Object.keys(update.$unset || {})) delete doc[key];
    return original;
  }
  const asQuery = value => ({ lean: async () => structuredClone(value) });
  return {
    docs,
    create: async doc => { docs.set(doc._id, structuredClone(doc)); },
    findOne: filter => asQuery(find(filter) || null),
    updateOne: async (filter, update) => ({ matchedCount: modify(filter, update) ? 1 : 0 }),
    findOneAndUpdate: (filter, update, options) => {
      assert.equal(options.new, false);
      return asQuery(modify(filter, update));
    },
    deleteMany: async filter => { while (find(filter)) docs.delete(find(filter)._id); }
  };
}

const alice = { owner: 'seller-a', session: 'session-a', grant: 'grant-a' };
const example = { to: 'cliente@example.test', subject: 'Visita del martes', body: '¿Te viene bien a las cinco?' };
function fixture() {
  const store = memoryStore(), key = randomBytes(32);
  let time = Date.UTC(2026, 0, 1);
  const drafts = createMailDrafts({ store, key, now: () => time });
  return { drafts, store, advance: ms => { time += ms; } };
}

test('el borrador se cifra, muestra lo que enviará y prevalece la copia guardada al confirmar', async () => {
  const { drafts, store } = fixture();
  const preview = await drafts.prepare({ ...alice, ...example });
  assert.match(preview.id, /^[0-9A-F]{12}$/);
  assert.equal(preview.subject, example.subject);
  assert.equal(JSON.stringify([...store.docs.values()]).includes(example.body), false);
  assert.deepEqual(await drafts.view({ ...alice, id: preview.id }), { status: 'pending', ...preview });
  const confirm = { ...alice, id: preview.id.toLowerCase(), to: 'different@example.test' };
  const result = await drafts.confirm({ ...confirm, deliver: async actual => {
    assert.deepEqual(actual, example);
    return { id: 'gmail-message-1' };
  } });
  assert.deepEqual(result, { status:'sent', messageId:'gmail-message-1' });
  assert.ok(!store.docs.get(preview.id).content);
  assert.deepEqual(await drafts.confirm({ ...confirm, deliver: () => assert.fail('duplicado') }), result);
  assert.deepEqual(await drafts.cancel(confirm), result);
});

test('cuenta, sesión y conexión deben coincidir y los borradores caducan', async () => {
  const { drafts, advance } = fixture();
  const {id}=await drafts.prepare({ ...alice,...example });
  for (const mismatch of [{owner:'seller-b'}, {session:'session-b'}, {grant:'reconnected'}]) {
    const p={...alice, ...mismatch, id};
    assert.equal((await drafts.view(p)).status,'unavailable');
    assert.equal((await drafts.confirm({...p, deliver:()=>assert.fail('acceso indebido')})).status,'unavailable');
    assert.equal((await drafts.cancel(p)).status,'unavailable');
  }
  assert.equal((await drafts.view({...alice,id:{ $ne:null }})).status,'unavailable');
  advance(30*60000);
  assert.equal((await drafts.view({...alice,id})).status,'unavailable');
  assert.equal((await drafts.confirm({...alice,id,deliver:()=>assert.fail('caducado')})).status,'unavailable');
});

test('confirmaciones simultáneas o cancelar durante el envío producen una sola entrega', async () => {
  const { drafts } = fixture();
  const {id}=await drafts.prepare({ ...alice,...example });
  const params={...alice,id};
  let release;
  const receipt = new Promise(resolve=>{ release=resolve; });
  let calls=0;
  const deliver = async () => {calls++; return receipt;};
  const first=drafts.confirm({...params,deliver});
  const others=await Promise.all([drafts.confirm({...params,deliver}),drafts.cancel(params)]);
  assert.deepEqual(others.map(x=>x.status),['sending','sending']);
  release({id:'gmail-1'});
  assert.equal((await first).status,'sent');
  assert.equal(calls,1);
});

test('pérdida de la respuesta de Gmail requiere revisión y no vuelve a enviar', async () => {
  const {drafts}=fixture();
  const {id}=await drafts.prepare({...alice,...example});
  const p={...alice,id};
  assert.equal((await drafts.confirm({...p,deliver:async()=>{ throw new Error('timeout'); }})).status,'unknown');
  assert.equal((await drafts.confirm({...p,deliver:()=>assert.fail('reintento de envío')})).status,'unknown');
});

test('cancelación y desconexión eliminan el borrador listo para enviar', async () => {
  const {drafts,store}=fixture();
  const {id}=await drafts.prepare({...alice,...example});
  assert.equal((await drafts.cancel({...alice,id})).status,'cancelled');
  assert.ok(!store.docs.get(id).content);
  assert.equal((await drafts.confirm({...alice,id,deliver:()=>assert.fail('cancelado')})).status,'cancelled');
  const second=await drafts.prepare({...alice,...example});
  await drafts.purge(alice);
  assert.equal((await drafts.view({...alice,id:second.id})).status,'unavailable');
});

test('fallos antes del envío o rechazos confirmados de Gmail se distinguen de los tiempos de espera',async()=>{
  const {drafts}=fixture();
  for(const flag of ['beforeSend','providerRejected']){
    const {id}=await drafts.prepare({...alice,...example});
    const result=await drafts.confirm({...alice,id,deliver:async()=>{throw Object.assign(new Error('test'),{[flag]:true});}});
    assert.equal(result.status,'failed');
    assert.equal((await drafts.confirm({...alice,id,deliver:()=>assert.fail('duplicado')})).status,'failed');
  }
});

test('no se abre un borrador con encabezados inyectados, múltiples destinatarios ni cuerpo vacío',async()=>{
  const {drafts,store}=fixture();
  for (const bad of [{subject:'interés\r\nCc: otro@example.test'}, {to:'a@example.test,b@example.test'}, {to:'a@example.test\n'}, {body:' \n '}]){
    await assert.rejects(drafts.prepare({...alice,...example,...bad}));
  }
  assert.equal(store.docs.size,0);
});

test('si se pierde la escritura del recibo después de la aceptación de Gmail, el mismo código nunca duplica el envío',async()=>{
  const {drafts,store,advance}=fixture();
  const {id}=await drafts.prepare({...alice,...example});
  const p={...alice,id};
  store.updateOne=async()=>{throw new Error('temporary database failure');};
  assert.equal((await drafts.confirm({...p,deliver:async()=>({id:'google-id'})})).status,'sent');
  advance(3*60000);
  assert.equal((await drafts.confirm({...p,deliver:()=>assert.fail('envío duplicado')})).status,'unknown');
});

test('una corrección invalida el código anterior y conserva las cabeceras al enviar la versión revisada', async()=>{
  const {drafts} = fixture();
  const thread = {threadId:'afe12', parent:'<parent@example.test>',references:['<parent@example.test>']};
  const first = await drafts.prepare({...alice,...example,thread});
  await assert.rejects(drafts.revise({...alice,id:first.id,changes:{subject:''}}));
  const edited = await drafts.revise({...alice,id:first.id,changes:{body:'Nos vemos el viernes.'}});
  assert.equal(edited.status,'pending'); assert.notEqual(edited.id,first.id);
  const forbidden = () => assert.fail('Nunca se puede enviar una versión desactualizada.');
  assert.equal((await drafts.confirm({...alice,id:first.id,deliver:forbidden})).status,'unavailable');
  assert.equal((await drafts.cancel({...alice,id:first.id})).status,'unavailable');
  assert.equal((await drafts.view({...alice,owner:'otro',id:edited.id})).status,'unavailable');
  const sent = [];
  const send = id => drafts.confirm({...alice,id,deliver:async m=>{sent.push(m);return {id:'message'};}});
  await Promise.all([send(edited.id),send(edited.id)]);
  assert.deepEqual(sent,[{...example,body:'Nos vemos el viernes.',thread}]);
});

test('dos correcciones simultáneas no dejan dos versiones enviables, incluso en borradores del piloto anterior',async()=>{
  const {drafts,store} = fixture();
  const first = await drafts.prepare({...alice,...example});
  delete store.docs.get(first.id).revisionCode;
  const revisions = await Promise.all(['A','B'].map(body=>drafts.revise({...alice,id:first.id,changes:{body}})));
  assert.deepEqual(revisions.map(r=>r.status).sort(),['pending','unavailable']);
  const current = revisions.find(r=>r.id);
  const deliveries = [];
  const deliver = async m=>{deliveries.push(m.body);return {id:'new'};};
  await Promise.all([drafts.confirm({...alice,id:first.id,deliver}),drafts.confirm({...alice,id:current.id,deliver})]);
  assert.deepEqual(deliveries,[current.body]);
});

test('desde el chat: segundo correo, texto en otro turno, corrección, revisión rechazada y un envío en el hilo elegido',async()=>{
  const {drafts} = fixture(), key = randomBytes(32), sends = [], ids = [], model = [];
  const thread={threadId:'02bf',parent:'<segundo@cliente.test>',references:['<segundo@cliente.test>']};
  const account = id => ({_id:id,mailbox:id+'@tec.test',grant:id+'-grant',sendEnabled:true,encryptedToken:sealToken(id+'-token',key,id)});
  const apps = {a:account('a'),b:account('b')};
  const agent=createCoachMailAgent({config:{ready:true,pilot:['*'],key}, drafts, switched:()=>false,
    store:{findById:id=>({lean:async()=>apps[id]})},syncSales:async()=>({confirmed:[],review:[],limited:false}),
    gmail:{refresh:async t=>({access_token:t}),
      inbox:async()=>['0fa','02b'].map(id=>({id,subject:'Seguimiento',from:'cliente',date:new Date().toISOString()})),
      replyTarget:async(token,id)=>{ids.push([token,id]);return {to:'contacto@cliente.test',subject:'Seguimiento',thread};},
      send:async(token,msg)=>{sends.push({token,msg});return {id:'enviado'};}
    },rewrite:async(body,text)=>{model.push({body,text});return {body:'Hola, '+body};}
  });
  const call=(text,token='',owner='a',sessionId='chat')=>agent({req:{body:{accountId:owner,coachMailPending:token}},
    auth:{user:{_id:owner,email:owner+'@tec.test'},session:{tokenHash:'login-'+owner}},sessionId,text});
  const list = await call('Revisa mis correos');
  for (const [id,chat] of [['b','chat'],['a','otro']]) {
    const wrong = await call('Responde al segundo',list.coachMailPending,id,chat);
    assert.match(wrong.respuesta,/Primero pídeme/);
  }
  const question=await call('Responde al segundo',list.coachMailPending);
  assert.match(question.respuesta,/Qué quieres decirle/);
  const draft=await call('Mensaje: Puedo pasar el jueves.',question.coachMailPending);
  assert.deepEqual(ids,[['a-token','02b']]);assert.match(draft.respuesta,/Respuesta en el mismo hilo/);
  const edit = await call('Mejor el viernes',draft.coachMailPending);
  const code = r => r.coachHelpActions.find(a=>a.type==='mailConfirm').id;
  assert.match(edit.respuesta,/Puedo pasar el viernes/);assert.notEqual(code(draft),code(edit));
  const polished = await call('Hazlo más amable',edit.coachMailPending);
  assert.deepEqual(model,[{body:'Puedo pasar el viernes.',text:'Hazlo más amable'}]);
  assert.deepEqual(sends,[]);
  await call('Confirmar envío '+code(edit));
  assert.deepEqual(sends,[]);
  assert.equal(await call('¿Cómo hago una demo?',polished.coachMailPending),null);
  await Promise.all([call('Confirmar envío '+code(polished)),call('Confirmar envío '+code(polished))]);
  assert.equal(sends.length,1);assert.equal(sends[0].token,'a-token');
  assert.deepEqual(sends[0].msg,{from:'a@tec.test',to:'contacto@cliente.test',subject:'Seguimiento',body:'Hola, Puedo pasar el viernes.',thread});
  const report = await call('Mándame por correo mi resumen de ventas de Hy Cite');
  const fallback = await call('Hazlo más amable',report.coachMailPending);
  assert.match(fallback.respuesta,/Mensaje:/);assert.equal(model.length,1);
});
