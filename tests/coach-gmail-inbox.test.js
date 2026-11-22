import test from 'node:test';
import assert from 'node:assert/strict';
import { gmailClient } from '../src/coach-hycite-gmail.js';
import { pilotInboxFilters, mailReadClarification } from '../src/coach-mail-intent.js';
import { connectionsPage } from '../src/coach-connections-page.js';

const gmail = gmailClient({ origin:'https://coach.example' });

test('el asunto dictado o entre comillas se conserva aparte de remitente y fechas', () => {
  const filter = pilotInboxFilters('Revisa en Gmail los correos de Hay Cite con el asunto «Demo de hoy» de los últimos siete días.');
  assert.deepEqual(filter.options, {subject:'Demo de hoy',hyCite:true});
  assert.doesNotMatch(filter.request,/Demo de hoy/);
  assert.equal(mailReadClarification(filter.request,'inbox'),'');
  assert.deepEqual(pilotInboxFilters('¿Hay un correo con asunto Reunión semanal?').options,{subject:'Reunión semanal'});
  assert.deepEqual(pilotInboxFilters('Busca el asunto «Noticias de Hy Cite».').options,{subject:'Noticias de Hy Cite'});
  assert.ok(pilotInboxFilters('Busca por asunto').question);
  assert.ok(pilotInboxFilters('Busca por asunto "hola" con asunto "otro"').request.includes('con asunto'));
});

test('busca por asunto y por los dominios de Hy Cite sin traer mensajes de remitentes parecidos', async t => {
  const client = gmailClient({domains:['hycite.com']});
  const fetch = t.mock.method(globalThis,'fetch',async address=>{
    const url = new URL(address);
    if (url.pathname.endsWith('/messages')) {
      assert.equal(url.searchParams.get('q'),'subject:"Demo de hoy" {from:(@hycite.com)}');
      assert.equal(url.searchParams.get('labelIds'),'INBOX');
      return Response.json({messages:[{id:'a'},{id:'b'}]});
    }
    const id = url.pathname.split('/').pop();
    return Response.json({id, internalDate:'1000',snippet:'',payload:{headers:[
      {name:'From',value:id === 'a' ? 'Empresa <avisos@hycite.com>' : 'avisos@hycite.com.example'},
      {name:'Subject',value:'Demo de hoy'}
    ]}});
  });
  await assert.rejects(client.inbox('token',{subject:'abc" z'}),TypeError);
  assert.equal(fetch.mock.callCount(),0);
  assert.deepEqual((await client.inbox('token',{hyCite:true,subject:'Demo de hoy'})).map(m=>m.id),['a']);
});

test('lista únicamente cinco mensajes, selecciona encabezados y entrega extractos acotados en orden reciente', async t => {
  const seen = [];
  t.mock.method(globalThis, 'fetch', async (address, opts) => {
    assert.equal(opts.headers.Authorization, 'Bearer token-de-prueba');
    const url = new URL(address), query = url.searchParams;
    assert.equal(url.host, 'gmail.googleapis.com');
    assert.equal(opts.body, undefined);
    if (url.pathname.endsWith('/messages')) {
      assert.deepEqual(query.getAll('labelIds'), ['INBOX','UNREAD']);
      assert.equal(query.get('q'), 'from:"team+sales@example.com" newer_than:7d');
      assert.equal(query.get('maxResults'), '5'); assert.equal(query.get('fields'), 'messages(id)');
      return Response.json({ messages:Array.from({length:6},(_,i)=>({id:String(i+1)})) });
    }
    const id = url.pathname.split('/').pop();
    assert.ok(Number(id) <= 5);
    if (query.get('format') === 'full') {
      assert.equal(id,'1'); assert.equal(query.get('fields'),'snippet');
      return Response.json({snippet:'  Texto de\r\n  muestra. '});
    }
    assert.equal(query.get('format'),'metadata');
    assert.equal(query.get('fields'),'id,internalDate,snippet,payload/headers');
    assert.deepEqual(query.getAll('metadataHeaders'),['From','Subject']);
    seen.push(id);
    if (id === '2') return Response.json({}, {status:404});
    return Response.json({id,internalDate:String(Number(id)*1000),
      ...(id === '1'?{}:{snippet:'A'.repeat(400)}),payload:{headers:[
        {name:'From',value:'Acme\r\n <ventas@example.org>'},
        {name:'Subject',value:' Confirmación\n del pedido  '},
        {name:'Delivered-To',value:'contenido privado ignorado'}
      ]}});
  });
  const rows = await gmail.inbox('token-de-prueba', {from:'team+sales@example.com',unread:true,days:7});
  assert.equal(seen.length,5);
  assert.deepEqual(rows.map(row=>row.id), ['5','4','3','1']);
  assert.deepEqual(rows.at(-1),{id:'1',from:'Acme <ventas@example.org>',subject:'Confirmación del pedido',
    date:'1970-01-01T00:00:01.000Z',snippet:'Texto de muestra.'});
  assert.equal(rows[0].snippet.length,240);
});

test('valida los filtros antes de consultar y maneja una bandeja vacía', async t => {
  const fetch = t.mock.method(globalThis,'fetch',async address=>{
    const query = new URL(address).searchParams;
    assert.deepEqual(query.getAll('labelIds'),['INBOX']);assert.equal(query.has('q'),false);
    return Response.json({});
  });
  for (const options of [{from:'foo OR bar'},{unread:'false'},{days:-1},{days:700},{days:0.5}]) {
    await assert.rejects(gmail.inbox('token-de-prueba',options),TypeError);
  }
  assert.equal(fetch.mock.callCount(),0);
  assert.deepEqual(await gmail.inbox('token-de-prueba'),[]);
});

test('un permiso revocado se distingue de una bandeja vacía', async t => {
  t.mock.method(globalThis,'fetch',async()=> Response.json({}, {status:401}));
  await assert.rejects(gmail.inbox('token-de-prueba'),error => error.reauthorize === true);
});

test('Mis conexiones explica cómo leer correos, consultar ventas y solicitar envío en el chat', () => {
  const options={doc:{mailbox:'seller@example.com'},pilot:true,ready:true,csrf:'fixture'};
  const html = connectionsPage(options);
  assert.match(html,/Revisa mis correos/);
  assert.match(html,/Revisa mis ventas de Hy Cite/);
  assert.match(html,/Autorizar envío en Google/);
  assert.doesNotMatch(connectionsPage({...options,doc:{...options.doc,sendEnabled:true}}),/Autorizar envío en Google/);
});
