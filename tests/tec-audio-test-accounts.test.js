import assert from 'node:assert/strict';
import test from 'node:test';
import express from 'express';
import { Readable } from 'node:stream';
import { registerAudioMembership } from '../src/tec-audio-membership.js';
import { createTestPassword, hasTestAudioAccess, verifyTestPassword } from '../src/tec-audio-test-accounts.js';

const origin = 'https://totalelitecoaching.com';
const password = 'test-password-strong-2026';
const book = {id:'test-book',title:'Test',duration:100,chapters:[],audio:{bytes:16}};

test('test access requires all operator-provisioned flags and a salted valid password', async()=>{
  const user={isTest:true,analyticsExcluded:true,testAccessEnabled:true,accessType:'test',credentialVersion:'version',...await createTestPassword(password)};
  assert.ok(await verifyTestPassword(password,user));
  assert.equal(await verifyTestPassword(password+'wrong',user),false);
  assert.equal(await verifyTestPassword(password,null),false);
  for(const key of ['isTest','analyticsExcluded','testAccessEnabled','accessType','credentialVersion']) {
    const copy={...user};delete copy[key];assert.equal(hasTestAudioAccess(copy),false);
  }
});

test('four separate test users get free audio, isolated progress, no billing, revocation and login protections', async t=>{
  const accounts=new Map(),sessions=new Map(),progress=new Map();let stripeCalls=0;
  for(const username of ['agustin','rigoberto','enrique','debbie']) accounts.set('test:'+username,{_id:'test:'+username,username,displayName:username,
    isTest:true,analyticsExcluded:true,testAccessEnabled:true,accessType:'test',credentialVersion:'v1',...await createTestPassword(password)});
  const lean=value=>({lean:async()=>value});
  const app=express();app.use(express.json());
  registerAudioMembership(app,{
    catalog:[book], getTranscript:()=>({cues:[{start:0,end:100,text:'Complete'}],trailingSeparator:''}),publicOrigin:origin,
    accounts:{findById:id=>lean(accounts.get(id))},
    sessions:{create:async row=>sessions.set(row._id,row),findOne:({_id,expiresAt})=>lean(sessions.get(_id)?.expiresAt>expiresAt.$gt?sessions.get(_id):null),deleteOne:async({_id})=>sessions.delete(_id)},
    listening:{findById:id=>lean(progress.get(id)),updateOne:async({_id},{$set})=>progress.set(_id,{...progress.get(_id),...$set})},
    media:{findOne:()=>lean({fileId:'verified'})},
    getBucket:()=>({find:()=>({limit:()=>({toArray:async()=>[{_id:'verified',length:16}]})}),openDownloadStream:(_id,{start,end})=>Readable.from(Buffer.alloc(end-start,65))}),
    stripe:{subscriptions:{async *list(){stripeCalls++;}},prices:{retrieve:()=>{stripeCalls++;throw Error();}},billingPortal:{sessions:{create:()=>{stripeCalls++;throw Error();}}}},priceId:'price',portalId:'portal'
  });
  const server=await new Promise(resolve=>{const s=app.listen(0,'127.0.0.1',()=>resolve(s));});
  t.after(()=>{server.closeAllConnections();server.close();});
  const base='http://127.0.0.1:'+server.address().port+'/api/audiolibros';
  const request=(p,method='GET',body,cookie='',extra={})=>fetch(base+p,{method,headers:{Origin:origin,'Content-Type':'application/json',Cookie:cookie,...extra},...(body?{body:JSON.stringify(body)}:{})});
  assert.equal((await request('/pruebas/entrar','POST',{username:'agustin',password},'',{Origin:'https://evil.test'})).status,403);
  assert.equal((await request('/pruebas/entrar','POST',{username:'agustin',password:'badpassword-long'})).status,401);
  const cookies=[];
  for(const username of ['Agustin','Rigoberto','Enrique','Debbie']) {
    const login=await request('/pruebas/entrar','POST',{username,password,isTest:false});
    assert.equal(login.status,200);
    const set=login.headers.getSetCookie();
    assert.ok(set.some(c=>c.startsWith('tec_audio_test=1;')&&c.includes('Path=/audiolibros')));
    const cookie=set.find(c=>c.startsWith('tec_audio_session=')).split(';')[0];cookies.push(cookie);
    assert.match(set.find(c=>c.startsWith('tec_audio_session=')),/HttpOnly/);
    const identity=await request('/cuenta','GET',null,cookie).then(r=>r.json());
    assert.ok(identity.member&&identity.testAccount&&identity.analyticsExcluded);assert.equal(identity.customer,false);
    assert.ok(!JSON.stringify(identity).includes('passwordHash'));
    assert.equal((await request('/libros/test-book/texto','GET',null,cookie)).status,200);
    const audio=await request('/libros/test-book/audio','GET',null,cookie,{Range:'bytes=4-7'});
    assert.equal(audio.status,206);assert.equal(await audio.text(),'AAAA');
    assert.equal((await request('/checkout','POST',{},cookie)).status,403);
    assert.equal((await request('/portal','POST',{},cookie)).status,403);
  }
  assert.equal(stripeCalls,0);assert.equal(new Set(cookies).size,4);
  assert.equal((await request('/libros/test-book/audio','GET',null,'tec_audio_test=1')).status,401);
  await request('/libros/test-book/progreso','PUT',{position:42,speed:1},cookies[0]);
  const first=await request('/libros/test-book','GET',null,cookies[0]).then(r=>r.json());
  const second=await request('/libros/test-book','GET',null,cookies[1]).then(r=>r.json());
  assert.equal(first.progress.position,42);assert.equal(second.progress.position,0);
  accounts.get('test:agustin').testAccessEnabled=false;
  assert.equal((await request('/libros/test-book/audio','GET',null,cookies[0])).status,403);
  accounts.get('test:rigoberto').credentialVersion='rotated';
  assert.equal((await request('/libros/test-book/audio','GET',null,cookies[1])).status,401);
  assert.equal((await request('/salir','POST',{},cookies[2])).status,200);
  assert.equal((await request('/libros/test-book/audio','GET',null,cookies[2])).status,401);
  for(let i=0;i<3;i++) await request('/pruebas/entrar','POST',{username:'unknown',password});
  assert.equal((await request('/pruebas/entrar','POST',{username:'debbie',password})).status,429);
  assert.equal((await request('/cuenta','GET',null,cookies[3]).then(r=>r.json())).member,true);
});
