import test from 'node:test';
import assert from 'node:assert/strict';
import {randomBytes} from 'node:crypto';
import {createCoachWaAgent} from '../src/coach-wa-chat.js';
import {createWaDrafts} from '../src/coach-wa-drafts.js';
import {waRequestParts,whatsAppIntent} from '../src/coach-wa-chat-input.js';

function fixture({contacts=[],switched=false,model,getSenderPhone}={}) {
  const docs=new Map(), matches=[];
  function find(q) {
    return [...docs.values()].find(d=>Object.entries(q).every(([k,v])=>v?.$gt?d[k]>v.$gt:d[k]===v));
  }
  let time=Date.UTC(2026,10,20,17);
  const now=()=>time,key=randomBytes(32);
  const store={
    create:async d=>{docs.set(d._id,structuredClone(d));},
    findOne:q=>({lean:async()=>structuredClone(find(q) || null)}),
    updateOne:async(q,u)=>{
      const d=find(q);
      if (!d) return {matchedCount:0};
      Object.assign(d,u.$set);
      for (const k of Object.keys(u.$unset||{})) delete d[k];
      return {matchedCount:1};
    }
  };
  const waDrafts=createWaDrafts({key,store,now});
  const agent=createCoachWaAgent({getSenderPhone,config:{key,pilot:['a@example.test','b@example.test']},switched:()=>switched,waDrafts,now,
    findLeads:async q=>{matches.push(q);return contacts.filter(c=>c.owner===q.owner&&(q.id?c.id===q.id:c.name.startsWith(q.name)));},
    ...(model?{compose:model}:{})
  });
  async function send(text,{owner='a',login='active',sessionId='chat-a',accountId=owner,...body}={}) {
    return agent({req:{body:{accountId,coachWhatsAppVersion:1,...body}},auth:{user:{_id:owner,email:owner+'@example.test'},session:{tokenHash:login}},sessionId,text});
  }
  return {send,docs,matches,advance:ms=>{time+=ms;},waDrafts};
}

test('WhatsApp personal permite preparar, corregir y confirmar solo la versión exacta para abrirla en la app',async()=>{
  const f=fixture();
  const original=await f.send('Prepara un WhatsApp de seguimiento para +1 (312) 555-0100. Mensaje: ¿Te va bien el jueves?');
  const code=original.coachHelpActions[0].id;
  assert.deepEqual(original.coachHelpActions.map(a=>a.type),['waPersonalConfirm','waCancel']);
  assert.match(original.respuesta,/cuenta de WhatsApp activa/);
  assert.match(original.respuesta,/\+13125550100/);
  assert.doesNotMatch(JSON.stringify([...f.docs.values()]),/5550100|jueves/);
  const edited=await f.send('Mejor el viernes',{coachWhatsAppPending:original.coachWhatsAppPending});
  const current=edited.coachHelpActions[0].id;
  assert.notEqual(current,code);assert.match(edited.respuesta,/viernes/);
  const command='Confirmar borrador WhatsApp ';
  assert.deepEqual((await f.send(command+code)).coachHelpActions,[]);
  const normal=await f.send('Sí',{coachWhatsAppPending:edited.coachWhatsAppPending});
  assert.ok(!normal.coachHelpActions.some(a=>a.type==='waOpen'));
  const copy=[{type:'waOpen',to:'+13125550100',text:'¿Te va bien el viernes?',label:'Abrir en mi WhatsApp'}];
  const [first,second]=await Promise.all([f.send(command+current),f.send(command+current)]);
  for (const result of [first,second]) {assert.deepEqual(result.coachHelpActions,copy);assert.match(result.respuesta,/pulsa Enviar allí/);}
});
test('otro vendedor, chat, sesión, pestaña o autorización cambiada no heredan el borrador personal',async()=>{
  const f=fixture();
  const d=await f.send('Prepara un WhatsApp a +521234567890: texto privado');
  const cmd='Confirmar borrador WhatsApp '+d.coachHelpActions[0].id;
  for(const opt of [{owner:'b'},{login:'other'},{sessionId:'other'},{accountId:'b'},{owner:'z'}]) {
    assert.deepEqual((await f.send(cmd,opt)).coachHelpActions,[]);
  }
  const expiredToken=await f.send('Mejor el lunes',{owner:'b',coachWhatsAppPending:d.coachWhatsAppPending});
  assert.match(expiredToken.respuesta,/caduc/i);
  assert.match((await fixture({switched:true}).send('Prepara un WhatsApp')).respuesta,/propia cuenta/);
  const cancelled=await f.send('Descartar WhatsApp '+d.coachHelpActions[0].id);
  assert.match(cancelled.respuesta,/Descarté/);assert.deepEqual((await f.send(cmd)).coachHelpActions,[]);
  const another=await f.send('Prepara un WhatsApp para +521234567890: otro');
  f.advance(30*60000);
  assert.deepEqual((await f.send('Confirmar borrador WhatsApp '+another.coachHelpActions[0].id)).coachHelpActions,[]);
});
test('el Coach busca el contacto propio, aclara duplicados, y solicita el prefijo si falta',async()=>{
  const f=fixture({contacts:[{owner:'a',name:'María Norte',phone:'312 555 0100'},{owner:'a',name:'María Sur',phone:'+1 312 555 0102'},{owner:'b',name:'María Externa',phone:'+1 900 000 0001'}]});
  const request=await f.send('Prepara un mensaje de seguimiento para María. Mensaje: ¿Hablamos mañana?');
  assert.match(request.respuesta,/María Norte/);assert.doesNotMatch(request.respuesta,/Externa/);
  assert.ok(f.matches.every(q=>q.owner==='a' && q.includePhone));
  const next=await f.send('María Norte',{coachWhatsAppPending:request.coachWhatsAppPending});
  assert.match(next.respuesta,/prefijo/);assert.deepEqual(next.coachHelpActions,[]);
  const message=await f.send('+52 33 1010 0100',{coachWhatsAppPending:next.coachWhatsAppPending});
  assert.match(message.respuesta,/María Norte · \+523310100100/);assert.match(message.respuesta,/¿Hablamos mañana\?/);
});
test('el cuerpo no determina el destinatario y las correcciones de otro canal permanecen en su canal',async()=>{
  const f=fixture();
  const parts=waRequestParts('Prepara un WhatsApp para María. Mensaje: Escríbele a Juan al +1 555 555 0199');
  assert.equal(parts.to,'');assert.equal(parts.name,'María');assert.match(parts.body,/Juan/);
  for(const s of ['Responde al segundo: confírmale por WhatsApp','Prepara un correo con asunto WhatsApp']) assert.equal(whatsAppIntent(s),null);
  for(const s of ['Agrega al final: te mando WhatsApp','Cambia la palabra cita por WhatsApp']) assert.equal(await f.send(s,{coachMailPending:'mail'}),null);
  const multiple=await f.send('Prepara un WhatsApp para +1 312 555 0100 y +52 33 5555 0101: Hola');
  assert.deepEqual(multiple.coachHelpActions,[]);assert.match(multiple.respuesta,/un solo destinatario/);
  const missing=await f.send('Prepárame un WhatsApp para 3125550100: Hola');assert.deepEqual(missing.coachHelpActions,[]);
});

test('el número de referencia se cifra, queda separado del destino y nunca aparece en el enlace de abrir',async()=>{
  const owners=[],sender='+13125550103';
  const f=fixture({getSenderPhone:async owner=>{owners.push(owner);return sender;}});
  const draft=await f.send('Prepara un WhatsApp para +521234567890. Mensaje: Pasaré el jueves.');
  assert.match(draft.respuesta,/Tu número de referencia: \+13125550103/);
  assert.match(draft.respuesta,/Para: \+521234567890/);
  assert.doesNotMatch(JSON.stringify([...f.docs.values()]),/13125550103|521234567890/);
  const edited=await f.send('Mejor el viernes',{coachWhatsAppPending:draft.coachWhatsAppPending});
  assert.match(edited.respuesta,/Tu número de referencia: \+13125550103/);
  const ready=await f.send('Confirmar borrador WhatsApp '+edited.coachHelpActions[0].id);
  assert.equal(ready.coachHelpActions[0].to,'+521234567890');
  assert.doesNotMatch(JSON.stringify(ready),/13125550103/);
  assert.deepEqual(owners,['a']);
  const noTarget=await f.send('Prepara un WhatsApp');
  assert.deepEqual(noTarget.coachHelpActions,[]);assert.doesNotMatch(noTarget.respuesta,/13125550103/);
});
