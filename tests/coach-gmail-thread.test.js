import test from 'node:test';
import assert from 'node:assert/strict';
import { gmailClient } from '../src/coach-hycite-gmail.js';
import { replyEnvelope } from '../src/coach-gmail-thread.js';
import { classifyMailIntent } from '../src/coach-mail-intent.js';
import { correction, replyText } from '../src/coach-mail-followup.js';

const parent = '<padre@cliente.test>', origin = '<origen@cliente.test>';
const headers = {
  From:'"Ana, R." <ana@cliente.test>', 'Reply-To':'"Atención, Norte" <servicio@cliente.test>',
  Subject:'Visita del jueves', 'Message-ID':parent, References:origin
};
const sample = (h=headers) => ({id:'abc',threadId:'def',payload:{headers:Object.entries(h).map(([name,value])=>({name,value}))}});
test('responde con destinatario, asunto, referencias e identificador del hilo en la petición real de Gmail',async t=>{
  const client = gmailClient({origin:'https://example.test'});
  let posts=0;
  t.mock.method(globalThis,'fetch',async (url, options)=>{
    assert.equal(options.headers.Authorization,'Bearer propio');
    if (options.method !== 'POST') {
      const u=new URL(url);
      assert.ok(u.pathname.endsWith('/messages/abc'));
      assert.equal(u.searchParams.get('format'),'metadata');
      return Response.json(sample());
    }
    posts++;
    const request = JSON.parse(options.body), raw = Buffer.from(request.raw,'base64url').toString();
    assert.equal(request.threadId,'def'); assert.match(raw,/To: servicio@cliente\.test\r\n/);
    assert.match(raw,/In-Reply-To: <padre@cliente.test>\r\n/);
    assert.match(raw,/References: <origen@cliente.test>\r\n <padre@cliente.test>\r\n/);
    assert.equal(Buffer.from(raw.match(/Subject: =\?UTF-8\?B\?([^?]+)/)[1],'base64').toString(),headers.Subject);
    return Response.json({id:'enviado',threadId:'def'});
  });
  const target=await client.replyTarget('propio','abc');
  assert.equal(target.to,'servicio@cliente.test');
  await client.send('propio',{...target,from:'yo@tec.test',body:'Gracias.'});
  assert.equal(posts,1);
});

test('no construye una respuesta con varios destinatarios, metadatos ambiguos o encabezados inyectados',()=>{
  for (const changes of [
    {'Reply-To':'A <a@example.test>, B <b@example.test>'},
    {'Reply-To':'ana@example.test\r\nBcc: otro@example.test'},
    {'Reply-To':'','From':'ana@example.test'},
    {'Message-ID':'<id@example.test>\r\nBcc: otro@example.test'},
    {Subject:'encabezado\r\nCc: otro@example.test'}
  ]) assert.throws(()=>replyEnvelope(sample({...headers,...changes})));
  const noReplyTo={...headers};delete noReplyTo['Reply-To'];
  assert.equal(replyEnvelope(sample(noReplyTo)).to,'ana@cliente.test');
  assert.equal(replyEnvelope(sample({...headers,Subject:''})).subject,'');
  const repeated=sample();repeated.payload.headers.push({name:'from',value:'a@b.test'});
  delete repeated.payload.headers[1];repeated.payload.headers=repeated.payload.headers.filter(Boolean);
  assert.throws(()=>replyEnvelope(repeated));
});

test('selecciona un solo correo por posición y conserva texto exacto, correcciones y asunto',()=>{
  assert.deepEqual(classifyMailIntent('Responde al segundo. Dile que puedo pasar el jueves'), {kind:'reply',index:2,latest:false,instruction:'Dile que puedo pasar el jueves'});
  assert.equal(classifyMailIntent('¿Puedes responder al correo número 3?')?.index,3);
  assert.equal(classifyMailIntent('No respondas al segundo correo')?.kind,'help');
  assert.equal(classifyMailIntent('Contesta al primero y al segundo').unsupported,true);
  assert.equal(replyText('Mensaje: iPhone confirmado.'),'iPhone confirmado.');
  assert.equal(replyText('Dile que puedo pasar el jueves.'),'Puedo pasar el jueves.');
  assert.deepEqual(correction({body:'Hola. Puedo pasar el jueves.'},'Mejor el viernes'),{body:'Hola. Puedo pasar el viernes.'});
  assert.match(correction({body:'El jueves o el lunes'},'Mejor el viernes').question,/frase/);
  assert.match(correction({thread:{},body:'Hola'},'Asunto: otro').question,/hilo/);
});

test('lee solo el fragmento de la última respuesta si Gmail omite el texto en metadata',async t=>{
  const client=gmailClient({origin:'https://example.test'}),requests=[];
  const message={...sample(),internalDate:'100',labelIds:['INBOX']};
  t.mock.method(globalThis,'fetch',async url=>{
    const u=new URL(url);requests.push(u);
    if (u.pathname.endsWith('/threads/def')) return Response.json({id:'def',messages:[message]});
    assert.equal(u.searchParams.get('fields'),'id,threadId,labelIds,snippet');
    return Response.json({id:'abc',threadId:'def',labelIds:['INBOX'],snippet:'La segunda, gracias.'});
  });
  assert.equal((await client.thread('test','def'))[0].snippet,'La segunda, gracias.');
  assert.deepEqual(requests.map(u=>u.searchParams.get('format')),['metadata','full']);
  message.labelIds=['TRASH'];
  assert.equal(await client.thread('test','def'),null);
  assert.equal(requests.length,3);
});
