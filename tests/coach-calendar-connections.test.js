import test from 'node:test';
import assert from 'node:assert/strict';
import {createHash, randomBytes} from 'node:crypto';
import {createCoachCalendarConnections} from '../src/coach-calendar-connections.js';
import {registerCoachConnections} from '../src/coach-connections.js';
import {calendarScope,calendarDescription} from '../src/coach-calendar-google.js';

function harness() {
  const docs = new Map(), calls=[], calendars=new Map(), routes=new Map();
  const match = (row,filter) => row && Object.entries(filter).every(([key,value])=>{
    if (value?.$exists != null) return (key in row) === value.$exists;
    if (value?.$gt) return row[key] > value.$gt;
    return value instanceof Date ? row[key]?.getTime() === value.getTime() : row[key] === value;
  });
  const change=(row,updates)=>{
    Object.assign(row,updates.$set);
    for(const key of Object.keys(updates.$unset||{})) delete row[key];
  };
  const lean=value=>({lean:async()=>structuredClone(value)});
  const store={
    findById:id=>lean(docs.get(id)),
    findOne:f=>lean(match(docs.get(f._id),f)?docs.get(f._id):null),
    exists:async f=>Boolean(match(docs.get(f._id),f)),
    async updateOne(f,u,options={}) {
      let row=docs.get(f._id);
      if(!row && options.upsert) {row={_id:f._id};docs.set(f._id,row);}
      if(!match(row,f)) return {matchedCount:0};
      change(row,u);return {matchedCount:1};
    },
    findOneAndUpdate(f,u,opts) {
      const row=docs.get(f._id);
      if(!match(row,f))return lean(null);
      const before=structuredClone(row);change(row,u);return lean(opts.new?row:before);
    }
  };
  const config={calendarReady:true,key:randomBytes(32)};
  const google={
    authorize:(state,challenge,email)=>({state,challenge,email}),
    exchange:async(code,verifier)=>{
      calls.push({name:'exchange',code,verifier});
      return {access_token:code,refresh_token:code,scope:code==='denied'?'openid':calendarScope};
    },
    profile:async code=>({sub:'uid-'+code,email:code+'@example.com',email_verified:true}),
    refresh:async key=>({access_token:key}),
    async createCalendar(code,marker) {
      const id=code.repeat(12)+'@group.calendar.google.com';
      calendars.set(id,{id,description:calendarDescription(marker)});
      calls.push({name:'createCalendar',code});
      return {id};
    },
    async getCalendar(_token,id) {
      if (!calendars.has(id)) throw Object.assign(new Error('No existe'),{status:404});
      return calendars.get(id);
    },
    async saveEvent(token,id,event) {calls.push({name:'event',token,id,event});return {id:event.eventId};}
  };
  const integration=createCoachCalendarConnections({config,store,google,purge:async v=>calls.push({name:'purge',...v})});
  const back=(_res,result)=>result;
  integration.register({post:(path,h)=>routes.set(path.split('/').at(-1),h)},h=>h,back);
  const seller=(id,session=id)=>({id,session,email:id+'@example.com'});
  const post=(route,actor,body={})=>routes.get(route)({body},{redirect:(_code,destination)=>destination},actor);
  const callback=(actor,state,code)=>integration.callback({query:{state,code}},{},actor,back);
  return {docs,calls,calendars,google,seller,post,callback,store,service:integration.service};
}

test('Calendar vincula estado PKCE, vendedor, Google verificado y sesión; una reproducción no crea otro calendario',async()=>{
  const h=harness(),a=h.seller('a'),b=h.seller('b');
  const {state,challenge,email}=await h.post('start',a);
  assert.match(state,/^cal\./);assert.equal(email,a.email);
  assert.equal(await h.callback(b,state,'a'),'expired');
  assert.equal(await h.callback({...a,session:'other'},state,'a'),'expired');
  assert.equal(await h.callback(a,state,'a'),'calendarConnected');
  const exchange=h.calls.find(c=>c.name==='exchange');
  assert.equal(createHash('sha256').update(exchange.verifier).digest('base64url'),challenge);
  assert.equal(await h.callback(a,state,'a'),'expired');
  const stateB=(await h.post('start',b)).state;
  assert.equal(await h.callback(b,stateB,'a'),'calendarWrongAccount');
  assert.equal(await h.service.getConnection('b'),null);
  assert.equal(await h.callback(b,(await h.post('start',b)).state,'denied'),'denied');
  assert.equal(await h.callback(b,(await h.post('start',b)).state,'b'),'calendarConnected');
  const aCal=await h.service.getConnection('a'),bCal=await h.service.getConnection('b');
  assert.notEqual(aCal.calendarId,bCal.calendarId);assert.notEqual(aCal.grant,bCal.grant);
  assert.notEqual(h.docs.get('a').encryptedToken,'a');
  await assert.rejects(h.service.saveEvent({owner:'b',grant:aCal.grant,eventId:'1234'}));
  await h.service.saveEvent({owner:'a',grant:aCal.grant,eventId:'1234',calendarId:bCal.calendarId});
  const saved=h.calls.at(-1);assert.equal(saved.token,'a');assert.equal(saved.id,aCal.calendarId);assert.equal(saved.event.calendarId,aCal.calendarId);
  assert.equal(await h.post('disconnect',a),'calendarDisconnected');
  assert.equal(await h.service.getConnection('a'),null);assert.deepEqual(await h.service.getConnection('b'),bCal);
  await assert.rejects(h.service.saveEvent({owner:'a',grant:aCal.grant}));
  assert.equal(h.docs.get('a').encryptedToken,undefined);
  assert.deepEqual(h.calls.at(-1),{name:'purge',owner:'a',grant:aCal.grant});
  assert.equal(await h.callback(a,(await h.post('start',a)).state,'a'),'calendarConnected');
  const again=await h.service.getConnection('a');assert.notEqual(again.grant,aCal.grant);assert.equal(again.calendarId,aCal.calendarId);
  assert.equal(h.calls.filter(c=>c.name==='createCalendar').length,2);
});

test('si Google crea el calendario y se pierde la respuesta, se recupera comprobando su referencia personal',async()=>{
  const h=harness(),a=h.seller('a'),b=h.seller('b');
  await h.callback(b,(await h.post('start',b)).state,'b');
  const create=h.google.createCalendar;
  h.google.createCalendar=async(...args)=>{await create(...args);throw new Error('Perdida de conexión');};
  await assert.rejects(h.callback(a,(await h.post('start',a)).state,'a'));
  assert.equal(h.docs.get('a').provisionState,'uncertain');
  assert.equal(await h.post('prepare',a,{}),'calendarPending');
  assert.equal(await h.post('prepare',a,{calendarId:h.docs.get('b').calendarId}),'calendarWrongId');
  assert.equal(await h.post('prepare',a,{calendarId:'a'.repeat(12)+'@group.calendar.google.com'}),'calendarConnected');
  assert.equal(h.calls.filter(c=>c.name==='createCalendar').length,2);
});

test('Mis conexiones y callback compartido conservan las barreras de sesión, piloto y CSRF para Calendar',async()=>{
  const h=harness(), routes=new Map(), gmailQueries=[];
  const project={key:randomBytes(32),ready:true,calendarReady:true,pilot:['a@example.com','b@example.com']};
  const app=Object.fromEntries(['get','post'].map(method=>[method,(path,fn)=>routes.set(method+' '+path,fn)]));
  const mailStore={
    findById(id) {gmailQueries.push(id);return {lean:async()=>({_id:id,mailbox:id+'@example.com'})};},
    findOneAndDelete() {return {lean:async()=>({encryptedToken:'read','grant':'mail-grant'})};}
  };
  registerCoachConnections(app,{config:project,switched:req=>req.switched,requireActive:async req=>({
    user:{_id:req.owner,email:req.owner+'@example.com'},session:{tokenHash:'session-'+req.owner}
  }),store:mailStore,gmail:{revoke:async()=>assert.fail('No revocar el otro conector')},drafts:{purge:async()=>{}},
  calendarStore:h.store,calendarGoogle:h.google});
  async function run(method,path,owner,parts={}) {
    const res={set(){return this;},vary(){return this;},status(n){this.code=n;return this;},send(body){this.body=body;},
      redirect(code,to){this.code=code;this.to=to;}};
    await routes.get(method+' '+path)({path,owner,method:method.toUpperCase(),query:{},...parts},res);
    return res;
  }
  const page='/mis-conexiones/',cal='/api/coach/conexiones/calendar/',mail='/api/coach/conexiones/gmail/';
  const panel=await run('get',page,'a');
  assert.match(panel.body,/Conectar mi Google Calendar/);assert.match(panel.body,/a@example.com/);
  const csrf=panel.body.match(/name="csrf" value="([^"]+)"/)[1];
  assert.equal((await run('post',cal+'start','b',{body:{csrf}})).code,403);
  assert.equal((await run('post',cal+'start','z',{body:{csrf}})).code,403);
  assert.equal((await run('post',cal+'start','a',{body:{csrf},switched:true})).code,403);
  const state=(await run('post',cal+'start','a',{body:{csrf}})).to.state;
  const query={state,code:'a'};
  assert.match((await run('get',mail+'callback','b',{query})).to,/expired/);
  assert.match((await run('get',mail+'callback','a',{query})).to,/calendarConnected/);
  assert.match((await run('get',page,'a')).body,/Desconectar Calendar/);
  assert.doesNotMatch((await run('get',page,'b')).body,/Desconectar Calendar/);
  assert.match((await run('post',mail+'disconnect','a',{body:{csrf}})).to,/gmailDisconnectedLocal/);
  assert.match((await run('get',page,'a')).body,/Desconectar Calendar/);
});

test('volver a autorizar recupera un calendario borrado por el vendedor y descarta propuestas antiguas',async()=>{
  const h=harness(), a=h.seller('a');
  await h.callback(a,(await h.post('start',a)).state,'a');
  const old=await h.service.getConnection('a');
  h.calendars.delete(old.calendarId);
  h.google.createCalendar=async()=>({id:'replacement0001@group.calendar.google.com'});
  assert.equal(await h.callback(a,(await h.post('start',a)).state,'a'),'calendarConnected');
  const fresh=await h.service.getConnection('a');
  assert.notEqual(fresh.calendarId,old.calendarId);
  assert.notEqual(fresh.grant,old.grant);
  await assert.rejects(h.service.saveEvent({owner:'a',grant:old.grant}));
});
