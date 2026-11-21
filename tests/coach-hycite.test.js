import test from 'node:test';
import assert from 'node:assert/strict';
import { randomBytes } from 'node:crypto';
import { summarizeMessages, openToken, sealToken, gmailScope } from '../src/coach-hycite-gmail.js';
import { registerCoachConnections, connectionConfig } from '../src/coach-connections.js';

function notice(subject, time, snippet = '', sender = 'orders@hycite.com', auth = 'dmarc=pass header.from=hycite.com') {
  const headers = Object.entries({ From: sender, Subject: subject, 'Authentication-Results': 'mx.google.com; ' + auth });
  return { id: String(time), internalDate: String(time), snippet, payload: { headers: headers.map(([name,value]) => ({name,value})) } };
}
test('cuenta una orden una vez, contrasta cambios, autenticación y avisos ambiguos', () => {
  const x = summarizeMessages([
    notice('Venta confirmada #1234',100), notice('Venta confirmada #1234',200),
    notice('Orden confirmada #8765',300), notice('Order #8765 cancelled',400),
    notice('Orden recibida',500), notice('Venta confirmada',600,'Número de orden: AB123'),
    notice('Venta confirmada',610,'Número de orden: AB124. Pendiente de aprobación'),
    notice('Venta confirmada #111',700,'','otro@example.com'),
    notice('Venta confirmada #222',800,'','orders@hycite.com','dmarc=fail header.from=hycite.com')
  ], ['hycite.com']);
  assert.deepEqual(x.confirmed.map(r => r.order), ['AB123','1234']);
  assert.deepEqual(x.review.map(r => r.id), ['610','500','400']);
});
test('el permiso cifrado está vinculado a la cuenta y detecta modificaciones', () => {
  const key = randomBytes(32), token = sealToken('test-only',key,'seller1');
  assert.equal(openToken(token,key,'seller1'),'test-only');
  assert.throws(() => openToken(token,key,'seller2'));
  assert.throws(() => openToken(token + 'zz',key,'seller1'));
});

function mockStore() {
  const docs = new Map();
  const matches = (d,f) => d && Object.entries(f).every(([k,v]) => {
    if (typeof v === 'object' && '$exists' in v) return (k in d) === v.$exists;
    if (typeof v === 'object' && '$gt' in v) return d[k] > v.$gt;
    return d[k] === v;
  });
  const change = (d,u) => { Object.assign(d,u.$set); for (const k in u.$unset) delete d[k]; };
  const lean = value => ({lean:async () => structuredClone(value)});
  return {
    findById: id => lean(docs.get(id)),
    async updateOne(f,u,opts) {
      let d = docs.get(f._id);
      if (!matches(d,f)) {
        if (!opts?.upsert) return {matchedCount:0};
        if (d) throw Object.assign(new Error(),{code:11000});
        d={_id:f._id}; docs.set(f._id,d);
      }
      change(d,u); return {matchedCount:1};
    },
    findOneAndUpdate(f,u,opts) {
      const d = docs.get(f._id);
      if (!matches(d,f)) return lean(null);
      const old = structuredClone(d); change(d,u); return lean(opts.new ? d : old);
    },
    findOneAndDelete(f) {const d=docs.get(f._id); docs.delete(f._id); return lean(d);}
  };
}

test('dos vendedores conectan Gmail, consultan órdenes y desconectan de forma independiente',async () => {
  const routes = new Map(), store=mockStore(), revoked=[];
  const app=Object.fromEntries(['get','post'].map(method=>[method,(p,h)=>routes.set(method+' '+p,h)]));
  const config = {...connectionConfig({COACH_CONNECTIONS_PILOT_EMAILS:'a@test.invalid,b@test.invalid'}), ready:true, key:randomBytes(32)};
  const gmail = {
    authorize: state=>'https://accounts.google.com/?state='+state,
    exchange: async code=>({scope:gmailScope,access_token:code,refresh_token:code}),
    profile: async code=>({emailAddress:code+'@test.invalid'}),
    refresh: async code=>({access_token:code}),
    notices: async code=>summarizeMessages([notice('Venta confirmada #'+(code==='a'?1111:2222),100)],['hycite.com']),
    revoke: async code=>{revoked.push(code);return true;}
  };
  registerCoachConnections(app,{store,gmail,config,drafts:{purge:async()=>{}},switched:r=>r.switched,requireActive:async req=>({user:{_id:req.account,email:req.account+'@test.invalid'},session:{tokenHash:'session-'+req.account}})});
  const page='/mis-conexiones/', api='/api/coach/conexiones/gmail/';
  async function run(method,path,account,parts={}) {
    const r={set(){return this},vary(){return this},status(code){this.statusCode=code;return this},send(body){this.body=body},redirect(code,to){this.to=to;this.statusCode=code}};
    await routes.get(method+' '+path)({path,method:method.toUpperCase(),account,query:{},...parts},r); return r;
  }
  const csrf = {};
  for (const id of ['a','b']) {
    const html = (await run('get',page,id)).body;
    csrf[id]=html.match(/name="csrf" value="([^"]+)"/)[1];
    const start=await run('post',api+'start',id,{body:{csrf:csrf[id]}});
    const state=new URL(start.to).searchParams.get('state');
    const query={state,code:id};
    assert.match((await run('get',api+'callback',id==='a'?'b':'a',{query})).to,/expired/);
    assert.match((await run('get',api+'callback',id,{query})).to,/connected/);
    assert.match((await run('get',api+'callback',id,{query})).to,/expired/);
    assert.match((await run('post',api+'sync',id,{body:{csrf:csrf[id]}})).to,/updated/);
  }
  assert.equal((await run('post',api+'sync','a',{body:{csrf:csrf.b}})).statusCode,403);
  assert.equal((await run('get',page,'a',{switched:true})).statusCode,403);
  assert.equal((await run('post',api+'start','z',{body:{}})).statusCode,403);
  const a=(await run('get',page,'a')).body, b=(await run('get',page,'b')).body;
  assert.match(a,/Orden 1111/); assert.doesNotMatch(a,/Orden 2222/);
  assert.match(b,/Orden 2222/); assert.doesNotMatch(b,/Orden 1111/);
  assert.match((await run('post',api+'disconnect','a',{body:{csrf:csrf.a}})).to,/disconnected/);
  assert.deepEqual(revoked,['a']);
  assert.doesNotMatch((await run('get',page,'a')).body,/Orden 1111/);
  assert.match((await run('get',page,'b')).body,/Orden 2222/);
});
