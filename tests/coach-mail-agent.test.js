import test from 'node:test';
import assert from 'node:assert/strict';
import { randomBytes } from 'node:crypto';
import { createCoachMailAgent } from '../src/coach-mail-agent.js';
import { sealToken } from '../src/coach-hycite-gmail.js';

function fixture() {
  const key = randomBytes(32);
  const config = { ready:true, pilot:['a@test.invalid','b@test.invalid'], key };
  const seller = _id => ({_id,mailbox:_id+'@test.invalid',grant:_id+'-grant',sendEnabled:false,encryptedToken:sealToken('token-'+_id,key,_id)});
  const docs = {a:seller('a'),b:seller('b')}, events = [], byCode = new Map();
  const store = { findById: id=>({ lean:async()=>structuredClone(docs[id] || null) }) };
  const gmail = {
    refresh:async t=>({access_token:t}),
    inbox:async t=>[{id:'a001',subject:'Pedido',from:'Prueba',date:new Date().toISOString(),snippet:t}],
    send:async(t,m)=>{events.push({t,m});return {id:'a002'};}
  };
  const order = {order:'AB-1000',date:new Date().toISOString(),id:'a003'};
  const syncSales=async(id,grant)=>({confirmed:[order],review:[],limited:true});
  const code='123456ABCDEF';
  const drafts={
    prepare:async d=>{byCode.set(code,d);return {...d,id:code};},
    confirm:async d=>{
      const original=byCode.get(d.id);
      if (!original || original.session!==d.session || original.grant!==d.grant || original.owner!==d.owner) return {status:'unavailable'};
      const {id}=await d.deliver(original);
      return {status:'sent',messageId:id};
    },
    cancel:async()=>({status:'cancelled'})
  };
  const agent=createCoachMailAgent({config,store,gmail,drafts,syncSales,switched:req=>req.switched});
  const call=(text,{id='a',sessionId='conversation-1',pending,switched=false,tabId=id}={})=>agent({
    req:{switched,body:{accountId:tabId,coachMailPending:pending}},auth:{user:{_id:id,email:id+'@test.invalid'},session:{tokenHash:'auth-'+id}},sessionId,text
  });
  return {call,events,docs,code,byCode};
}

test('el chat consulta datos en vivo en el buzón propio y distingue el conteo parcial de ventas',async()=>{
  const {call}=fixture();
  const own=await call('revisa mis correos');
  assert.match(own.respuesta,/token-a/);assert.doesNotMatch(own.respuesta,/token-b/);
  assert.equal(own.coachHelpActions[0].mailbox,'a@test.invalid');
  const sales=await call('Revisa mis ventas de Hy Cite');
  assert.match(sales.respuesta,/1 ventas? confirmadas?/);assert.match(sales.respuesta,/AB-1000/);assert.match(sales.respuesta,/parcial/);
  const other=await call('revisa mis correos',{id:'b'});assert.match(other.respuesta,/token-b/);
  assert.equal(await call('¿Cómo presento una sartén?'),null);
});
test('la conversación retoma el texto faltante, genera un borrador y solo envía tras el permiso y la confirmación exacta',async()=>{
  const {call,docs,events,code}=fixture();
  const q=await call('Prepara un correo. Asunto: Visita. Mensaje: ¿Nos vemos mañana?');
  assert.ok(q.coachMailPending);assert.doesNotMatch(q.coachMailPending,/vemos/);
  const draft=await call('cliente@example.com',{pending:q.coachMailPending});
  assert.match(draft.respuesta,/cliente@example.com/);assert.match(draft.respuesta,/¿Nos vemos mañana\?/);
  assert.equal(draft.coachHelpActions.find(a=>a.type==='mailConfirm').id,code);
  assert.match((await call('Confirmar envío '+code)).respuesta,/autoriza/);assert.equal(events.length,0);
  docs.a.sendEnabled=true;
  assert.match((await call('Confirmar envío '+code,{sessionId:'otra-conversacion'})).respuesta,/caducó/);
  assert.equal(events.length,0);
  const sent=await call('Confirmar envío '+code);
  assert.match(sent.respuesta,/Gmail confirmó/);assert.equal(events.length,1);
  assert.equal(events[0].m.from,'a@test.invalid');assert.equal(events[0].t,'token-a');
});
test('deniega sesión cambiada y descarta seguimiento cifrado de otra cuenta, sesión o conexión',async()=>{
  const {call,docs}=fixture();
  for(const options of [{switched:true},{tabId:'b'}]) assert.match((await call('revisa mis correos',options)).respuesta,/propia cuenta/);
  assert.match((await call('revisa mis correos',{id:'c'})).respuesta,/piloto/);
  const {coachMailPending:pending}=await call('Prepara un correo asunto: Visita mensaje: Nos vemos mañana.');
  for(const options of [{id:'b'},{sessionId:'otra'},{pending:'alterado'}]) assert.match((await call('cliente@example.com',{pending,...options})).respuesta,/caducó/);
  docs.a.grant='nueva';assert.match((await call('cliente@example.com',{pending})).respuesta,/caducó/);
});
test('el agente enlaza la consulta de Hy Cite con un borrador real a mi correo y espera aprobación',async()=>{
  const {call,events,code}=fixture();
  const report=await call('Mándame por correo mi resumen de ventas de Hy Cite');
  assert.match(report.respuesta,/Borrador desde a@test.invalid/);
  assert.match(report.respuesta,/Para: a@test.invalid/);
  assert.match(report.respuesta,/Ventas confirmadas: 1/);
  assert.match(report.respuesta,/conteo es parcial/i);
  assert.doesNotMatch(report.respuesta,/AB-1000/);
  assert.equal(report.coachHelpActions.find(a=>a.type==='mailConfirm').id,code);
  assert.equal(events.length,0);
});
test('el resumen compuesto pide un destinatario y vuelve a consultar antes de mostrar el borrador',async()=>{
  const {call,events}=fixture();
  const question=await call('Revisa mis ventas de Hy Cite y prepara un correo con el resumen');
  assert.ok(question.coachMailPending);
  const report=await call('vendedor@example.com',{pending:question.coachMailPending});
  assert.match(report.respuesta,/Para: vendedor@example.com/);
  assert.match(report.respuesta,/Ventas confirmadas: 1/);
  assert.equal(events.length,0);
});
test('destino explícito y período calendario funcionan juntos en el informe compuesto',async()=>{
  const {call,events}=fixture();
  const report=await call('Revisa mis ventas de Hy Cite de esta semana y manda un correo a vendedor@example.com con el resumen');
  assert.match(report.respuesta,/Para: vendedor@example.com/);
  assert.match(report.respuesta,/esta semana/);
  assert.match(report.respuesta,/Ventas confirmadas: 1/);
  assert.equal(events.length,0);
});
