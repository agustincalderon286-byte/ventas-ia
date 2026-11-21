import test from 'node:test';
import assert from 'node:assert/strict';
import {createHash, randomBytes} from 'node:crypto';
import {createCoachCalendarConnections} from '../src/coach-calendar-connections.js';
import {registerCoachConnections} from '../src/coach-connections.js';
import {availabilityScope,calendarScope,calendarDescription} from '../src/coach-calendar-google.js';

function harness() {
  const docs = new Map(), calls=[], calendars=new Map(), routes=new Map();
  const match = (row,filter) => row && Object.entries(filter).every(([key,value])=>{
    if (key === '$or') return value.some(condition => match(row,condition));
    if (value?.$exists != null) return (key in row) === value.$exists;
    if (value?.$gt) return row[key] > value.$gt;
    if (value?.$lte) return row[key] <= value.$lte;
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
    authorize:(state,challenge,email,withAvailability)=>({state,challenge,email,withAvailability}),
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

test('el permiso de horarios libres pertenece al vendedor y requiere consentimiento explícito y persistente',async()=>{
  const h=harness(),a=h.seller('a'),b=h.seller('b');
  const exchange=h.google.exchange;
  h.google.exchange=async(...args)=>({...await exchange(...args),scope:calendarScope+' '+availabilityScope});
  const first=await h.post('start',a);
  assert.equal(first.withAvailability,false);await h.callback(a,first.state,'a');
  const as=await h.service.getConnection('a');
  assert.equal(as.availabilityEnabled,false);
  const upgraded=await h.post('start',b,{availability:'yes'});
  assert.equal(upgraded.withAvailability,true);assert.equal(await h.callback(b,upgraded.state,'b'),'availabilityConnected');
  const bs=await h.service.getConnection('b');
  assert.equal(bs.availabilityEnabled,true);
  const input={start:'2035-12-01T18:00:00Z',end:'2035-12-01T22:00:00Z'};
  h.google.availability=async(token,id,period,mailbox)=>{assert.equal(token,'b');assert.equal(id,bs.calendarId);assert.deepEqual(period,input);assert.equal(mailbox,b.email);return [];};
  await assert.rejects(h.service.availability({owner:a.id,grant:as.grant,...input}),e=>e.reauthorize);
  await assert.rejects(h.service.availability({owner:a.id,grant:bs.grant,...input}));
  assert.deepEqual(await h.service.availability({owner:b.id,grant:bs.grant,calendarId:as.calendarId,mailbox:a.email,...input}),[]);
  await assert.rejects(h.service.saveEvent({owner:a.id,grant:as.grant,checkAvailability:true}),e=>e.reauthorize);
  h.google.exchange=async(...args)=>({...await exchange(...args),scope:calendarScope+' '+availabilityScope,refresh_token:undefined});
  assert.equal(await h.callback(a,(await h.post('start',a,{availability:'yes'})).state,'a'),'availabilityMissing');
  h.google.refresh=async key=>({access_token:key,scope:calendarScope+' '+availabilityScope});
  assert.equal(await h.callback(a,(await h.post('start',a,{availability:'yes'})).state,'a'),'availabilityConnected');
  h.google.exchange=exchange;
  assert.equal(await h.callback(a,(await h.post('start',a,{availability:'yes'})).state,'a'),'availabilityMissing');
  assert.equal((await h.service.getConnection('a')).availabilityEnabled,false);
  assert.equal((await h.service.getConnection('b')).availabilityEnabled,true);
});

test('dos propuestas distintas para el mismo horario se comprueban por turnos, independientemente para cada vendedor',async()=>{
  const h=harness();
  const exchange=h.google.exchange;
  h.google.exchange=async(...args)=>({...await exchange(...args),scope:calendarScope+' '+availabilityScope});
  async function proposal(owner,eventId) {
    const actor=h.seller(owner);
    await h.callback(actor,(await h.post('start',actor,{availability:'yes'})).state,owner);
    return {owner,grant:(await h.service.getConnection(owner)).grant,eventId,checkAvailability:true,
      start:'2090-12-01T18:00:00Z',end:'2090-12-01T19:00:00Z'};
  }
  const first=await proposal('a','first'), alternative={...first,eventId:'alternative'}, other=await proposal('b','other');
  const inGoogle=Promise.withResolvers(), finish=Promise.withResolvers(), taken=new Set(), checks=[];
  h.google.saveEvent=async(_token,calendarId,event)=>{
    checks.push(event.eventId);
    if (event.eventId==='first') {inGoogle.resolve();await finish.promise;}
    const key=calendarId+'/'+event.start;
    if(taken.has(key)) throw Object.assign(new Error('Se ocupó el horario'),{slotBusy:true,status:409});
    taken.add(key);return {id:event.eventId};
  };
  h.docs.get('a').bookingLease='expired';
  h.docs.get('a').bookingLeaseUntil=new Date(0);
  const pending=h.service.saveEvent(first);
  await inGoogle.promise;
  await assert.rejects(h.service.saveEvent(alternative),e=>e.status===503);
  assert.deepEqual(await h.service.saveEvent(other),{id:'other'});
  finish.resolve();assert.deepEqual(await pending,{id:'first'});
  assert.equal(h.docs.get('a').bookingLease,undefined);
  await assert.rejects(h.service.saveEvent(alternative),e=>e.slotBusy&&e.status===409);
  assert.deepEqual(checks,['first','other','alternative']);
  assert.equal(h.docs.get('a').bookingLease,undefined);
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
  assert.match(panel.body,/name="availability" value="yes"/);assert.match(panel.body,/Activar horarios libres/);
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
