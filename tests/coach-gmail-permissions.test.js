import test from 'node:test';
import assert from 'node:assert/strict';
import { createHash, randomBytes } from 'node:crypto';
import { connectionConfig, registerCoachConnections } from '../src/coach-connections.js';
import { gmailClient, gmailScope, gmailSendScope, isGmailAddress, openToken, sealToken } from '../src/coach-hycite-gmail.js';

test('solo la ampliación de permiso solicita enviar y dirige al buzón ya vinculado', () => {
  const gmail = gmailClient({ origin:'https://example.com', clientId:'test' });
  const read = new URL(gmail.authorize('s','challenge')).searchParams;
  assert.equal(read.get('scope'), gmailScope);
  const upgrade = new URL(gmail.authorize('s','challenge',{send:true,mailbox:'seller@example.com'})).searchParams;
  assert.deepEqual(upgrade.get('scope').split(' '), [gmailScope,gmailSendScope]);
  assert.equal(upgrade.get('include_granted_scopes'), 'true');
  assert.equal(upgrade.get('login_hint'), 'seller@example.com');
  assert.equal(upgrade.get('code_challenge_method'), 'S256');
  assert.equal(upgrade.get('redirect_uri'), read.get('redirect_uri'));
});

test('el envío MIME conserva tildes, emoji y saltos de línea y acepta un solo destinatario', async t => {
  const gmail = gmailClient({origin:'https://example.com'});
  const subject = 'Confirmación de la demostración 📆 '.repeat(4).trim();
  const body = 'Hola, María.\n\n¿Te viene bien el jueves? ' + 'Sí. '.repeat(100);
  const message = {from:'coach@example.com',to:'maria+ventas@example.org',subject,body};
  const fetch = t.mock.method(globalThis,'fetch',async (url, req) => {
    assert.equal(url, 'https://gmail.googleapis.com/gmail/v1/users/me/messages/send');
    assert.equal(req.method, 'POST');
    assert.equal(req.headers.Authorization, 'Bearer prueba');
    const wire = Buffer.from(JSON.parse(req.body).raw,'base64url').toString();
    const [headers, content] = wire.split('\r\n\r\n');
    assert.match(headers,/\r\nFrom: coach@example\.com\r\nTo: maria\+ventas@example\.org\r\n/);
    const words = [...headers.matchAll(/=\?UTF-8\?B\?([^?]+)\?=/g)];
    assert.ok(words.length > 1);
    assert.ok(words.every(([word])=>word.length <= 75));
    assert.equal(words.map(([,x])=>Buffer.from(x,'base64').toString()).join(''),subject);
    assert.ok(content.trimEnd().split('\r\n').every(line=>line.length <= 76));
    assert.equal(Buffer.from(content,'base64').toString(), body.replaceAll('\n','\r\n'));
    return Response.json({id:'message-id',threadId:'thread-id'});
  });
  assert.deepEqual(await gmail.send('prueba',message),{id:'message-id'});
  for (const to of ['two@example.com,other@example.com','a@b.com\nBcc: c@d.com','not-an-address',
    'name..other@example.com','who@bad-.example','Él <mail@example.com>']) {
    assert.equal(isGmailAddress(to),false);
    await assert.rejects(gmail.send('prueba',{...message,to}),/dirección/);
  }
  await assert.rejects(gmail.send('prueba',{...message,subject:'Asunto\r\nBcc: x@evil.org'}),/asunto/);
  assert.equal(fetch.mock.callCount(),1);
});

test('una respuesta sin id o con timeout no se trata como envío exitoso', async t => {
  const message = {from:'a@example.com',to:'b@example.com',subject:'Demostración',body:'Viernes'};
  const send = gmailClient({origin:'https://example.com'}).send;
  const mocked = t.mock.method(globalThis,'fetch',async()=>Response.json({}));
  await assert.rejects(send('token',message),/comprobante/);
  mocked.mock.mockImplementation(async()=>Response.json({}, {status:408}));
  await assert.rejects(send('token',message),err=>err.providerRejected===false);
  mocked.mock.mockImplementation(async()=>Response.json({}, {status:403}));
  await assert.rejects(send('token',message),err=>err.providerRejected===true);
});

test('cancelar, denegar o escoger otro buzón conserva credencial, ventas y grant; un callback solo se consume una vez', async () => {
  const key = randomBytes(32);
  const owner = 'owner';
  const originalToken = sealToken('read-token',key,owner);
  const doc = { _id:owner, grant:'fixed-grant', mailbox:'seller@example.com', encryptedToken:originalToken, snapshot:{confirmed:['42']} };
  const routes = {};
  function modify(where,update) {
    if (where._id !== doc._id) return null;
    for (const [k,v] of Object.entries(where)) {
      if (v?.$gt !== undefined) { if (!(doc[k] > v.$gt)) return null; }
      else if (v?.$exists !== undefined) { if ((k in doc)!==v.$exists) return null; }
      else if (doc[k] !== v) return null;
    }
    const previous = structuredClone(doc);
    Object.assign(doc,update.$set);
    for (const field of Object.keys(update.$unset || {})) delete doc[field];
    return previous;
  }
  const store = {
    findOneAndUpdate(where,update,{new:after}) { return { lean: async () => {
      const prev = modify(where,update); return after && prev ? structuredClone(doc) : prev;
    } }; },
    updateOne: async (where,update)=>({ matchedCount:modify(where,update)?1:0 })
  };
  const app = Object.fromEntries(['get','post'].map(m=>[m,(path,handler)=>{ routes[m+' '+path]=handler; }]));
  const config = {...connectionConfig({COACH_CONNECTIONS_PILOT_EMAILS:'coach@example.com'}),ready:true,key};
  let entered, otherMailbox=false, sendAllowed=true;
  const gmail = { authorize(state) { entered=state;return 'https://accounts.google.com'; },
    exchange: async()=>({refresh_token:'both-token',access_token:'access',scope:[gmailScope,sendAllowed?gmailSendScope:''].join(' ')}),
    profile: async()=>({emailAddress:otherMailbox?'other@example.org':doc.mailbox}) };
  registerCoachConnections(app,{ config,store,gmail,switched:()=>false,requireActive:async()=>({
    user:{_id:owner,email:'coach@example.com'}, session:{tokenHash:'test-session'}
  }) });
  const csrf = createHash('sha256').update('coach-connections:test-session').digest('base64url');
  async function run(method,endpoint, query={}) {
    const path = '/api/coach/conexiones/gmail/' + endpoint;
    let outcome;
    const res={ set(){return this}, vary(){return this}, redirect(code,url){outcome=url;} };
    await routes[method+' '+path]({method:method.toUpperCase(),path,query,body:{csrf}},res);return outcome;
  }
  const start = () => run('post','enable-send');
  const callback = params => run('get','callback',{state:entered,...params});
  await start();
  assert.match(await callback({error:'access_denied'}),/denied/);
  assert.match(await callback({code:'retry'}),/expired/);
  sendAllowed = false;
  await start(); assert.match(await callback({code:'without-send'}),/denied/);
  sendAllowed=true; otherMailbox=true;
  await start(); assert.match(await callback({code:'wrong'}),/wrongMailbox/);
  assert.equal(doc.encryptedToken,originalToken);
  assert.equal(doc.grant,'fixed-grant');
  assert.equal(doc.mailbox,'seller@example.com');
  assert.equal(doc.sendEnabled,undefined);
  await start();const stale=entered;await start();
  assert.match(await run('get','callback',{code:'stale',state:stale}),/expired/);
  otherMailbox=false;
  assert.match(await callback({code:'valid'}),/sendEnabled/);
  assert.match(await callback({code:'replay'}),/expired/);
  assert.equal(doc.sendEnabled,true);
  assert.equal(doc.grant,'fixed-grant');
  assert.deepEqual(doc.snapshot,{confirmed:['42']});
  assert.equal(openToken(doc.encryptedToken,key,owner),'both-token');
});
