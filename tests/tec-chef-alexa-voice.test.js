import test from 'node:test';
import assert from 'node:assert/strict';
import express from 'express';
import path from 'node:path';
import {fileURLToPath} from 'node:url';
import {createHash} from 'node:crypto';
import {registerPublicChef,spokenSellerCandidates,spokenVoiceCode} from '../src/tec-chef-public.js';
import {chefPublicMemoryModel as memory} from './helpers/chef-public-memory.js';

// Fictional accounts only. 555 numbers are reserved for fiction.
const root=fileURLToPath(new URL('../',import.meta.url));
const account=(n,name,parent=null,role='seat')=>({_id:String(n).repeat(24),name,parentUserId:parent,accountType:role});
const owner=account(1,'Owner Ejemplo',null,'owner'),eric=account(3,'Eric Ejemplo',owner._id),lorena=account(2,'Lorena Prueba',owner._id),peer=account(4,'Pedro Ficticio',owner._id);
const KEY='clave-de-prueba-local';
const alexaUser=createHash('sha256').update('usuario ficticio').digest('hex');
const otherAlexaUser=createHash('sha256').update('otro usuario ficticio').digest('hex');

async function fixture(t,{key=KEY,limits={}}={}){
  const app=express();app.use(express.json());
  const leads=memory(),synced=[];
  const contexts=new Map([['eric-ejemplo',eric],['lorena-prueba',lorena],['eric-ejemplo-dos',peer],['eric-ejemplo-2',lorena],['chef-1a2b3c4d',peer]]
    .map(([slug,user])=>[slug,{slug,userDoc:user,ownership:{ownerUserId:owner._id,ownerName:owner.name}}]));
  contexts.get('eric-ejemplo').voiceCode='482913';
  registerPublicChef(app,{requireActive:async()=>null,getBySlug:async slug=>contexts.get(slug)||null,getDefault:async()=>null,getOwnContext:async()=>null,getDownline:async()=>[],
    visitors:memory(),conversations:memory(),leads,privateDir:path.join(root,'private'),publicDir:path.join(root,'public'),
    alexaVoiceKey:key,limits,getByVoiceCode:async code=>[...contexts.values()].find(ctx=>ctx.voiceCode===code)||null,completion:async()=>'Respuesta de cocina.',syncCrm:async(user,lead)=>{synced.push({user,lead});}});
  const server=await new Promise(resolve=>{const s=app.listen(0,'127.0.0.1',()=>resolve(s));});
  t.after(()=>{server.closeAllConnections();server.close();});
  const base='http://127.0.0.1:'+server.address().port;
  const post=(url,body,{headerKey=KEY}={})=>fetch(base+url,{method:'POST',headers:{'Content-Type':'application/json',...(headerKey?{'X-TEC-Alexa-Key':headerKey}:{})},body:JSON.stringify(body)});
  const get=url=>fetch(base+url);
  return {post,get,base,leads,contexts,synced};
}
const demo=(overrides={})=>({alexaUser,slug:'eric-ejemplo',requestId:'a'.repeat(40),fullName:'Ana Prueba',phone:'7085550100',city:'Chicago',appointmentPreferredTime:'el sábado en la tarde',consentGiven:true,...overrides});

test('spoken seller identifiers become the slug printed in the QR link',()=>{
  assert.deepEqual(spokenSellerCandidates('mi vendedor es Eric Ejemplo'),['eric-ejemplo']);
  assert.deepEqual(spokenSellerCandidates('https://totalelitecoaching.com/chef/eric-ejemplo/'),['eric-ejemplo']);
  assert.ok(spokenSellerCandidates('eric ejemplo dos').includes('eric-ejemplo-2'));
  assert.ok(spokenSellerCandidates('chef 1 a 2 b 3 c 4 d').includes('chef-1a2b3c4d'));
  assert.ok(spokenSellerCandidates('eric ejemplo a tres efe nueve').includes('eric-ejemplo-a3f9'));
  assert.deepEqual(spokenSellerCandidates('¿?'),[]);
  assert.deepEqual(spokenSellerCandidates({toString:()=>'eric'}),[]);
});

test('voice endpoints stay closed until the shared key is configured, and refuse a wrong key',async t=>{
  const off=await fixture(t,{key:''});
  let r=await off.post('/api/chef/public/alexa/seller',{alexaUser,vendedor:'eric ejemplo'});
  assert.equal(r.status,503);assert.equal((await r.json()).fallback,true);
  assert.equal((await off.post('/api/chef/public/alexa/leads',demo())).status,503);
  const on=await fixture(t);
  for(const headerKey of ['',KEY+'x']){
    assert.equal((await on.post('/api/chef/public/alexa/seller',{alexaUser,vendedor:'eric ejemplo'},{headerKey})).status,401);
    assert.equal((await on.post('/api/chef/public/alexa/leads',demo(),{headerKey})).status,401);
  }
  assert.equal((await on.post('/api/chef/public/alexa/seller',{alexaUser:'amzn1.ask.account.raw',vendedor:'eric ejemplo'})).status,400);
  assert.equal(off.leads.docs.length+on.leads.docs.length,0);
});

test('correct seller: the spoken QR identifier resolves to that seller and the lead is attributed to them',async t=>{
  const f=await fixture(t);
  const found=await f.post('/api/chef/public/alexa/seller',{alexaUser,vendedor:'mi vendedor es Eric Ejemplo'});
  assert.equal(found.status,200);
  const seller=await found.json();
  assert.deepEqual(seller,{slug:'eric-ejemplo',sellerName:'Eric Ejemplo'});
  const r=await f.post('/api/chef/public/alexa/leads',demo({slug:seller.slug,ownerUserId:peer._id,generatedByUserId:peer._id,status:'agendado',appointmentStatus:'confirmada',chefSlug:'lorena-prueba'}));
  assert.equal(r.status,200);
  const data=await r.json();
  assert.equal(data.sellerName,'Eric Ejemplo');assert.equal(data.duplicate,false);assert.equal(data.appointmentStatus,'solicitada');
  assert.equal(f.leads.docs.length,1);
  const saved=f.leads.docs[0];
  assert.equal(saved.generatedByUserId,eric._id);assert.equal(saved.ownerUserId,owner._id);assert.equal(saved.chefSlug,'eric-ejemplo');
  assert.equal(saved.status,'nuevo');assert.equal(saved.appointmentStatus,'solicitada');assert.equal(saved.source,'chef_personal');
  assert.equal(saved.sourceDetail,'TEC Chef · Alexa por voz');assert.ok(saved.tags.includes('alexa_voz'));
  assert.equal(saved.appointmentPreferredTime,'Preferencia: el sábado en la tarde');
  assert.match(saved.notes,/preferencia del cliente; confirma disponibilidad/);
  assert.doesNotMatch(JSON.stringify(saved),new RegExp(alexaUser));
  assert.equal(f.synced.length,1);assert.equal(f.synced[0].user._id,eric._id);
});

test('invalid seller: unknown, ambiguous or deactivated sellers never receive the lead',async t=>{
  const f=await fixture(t);
  for(const vendedor of ['vendedor desconocido','','¿?']){
    const r=await f.post('/api/chef/public/alexa/seller',{alexaUser,vendedor});
    assert.equal(r.status,404);assert.equal((await r.json()).fallback,true);
  }
  // "eric ejemplo dos" can be read as two different active sellers: refuse instead of guessing.
  const ambiguous=await f.post('/api/chef/public/alexa/seller',{alexaUser,vendedor:'eric ejemplo dos'});
  assert.equal(ambiguous.status,409);assert.doesNotMatch(await ambiguous.text(),/Pedro|Lorena/);
  assert.equal((await f.post('/api/chef/public/alexa/leads',demo({slug:'no-existe'}))).status,404);
  assert.equal((await f.post('/api/chef/public/alexa/leads',demo({slug:'Eric Ejemplo'}))).status,400);
  f.contexts.delete('eric-ejemplo');
  const late=await f.post('/api/chef/public/alexa/leads',demo());
  assert.equal(late.status,404);assert.equal((await late.json()).fallback,true);
  assert.equal(f.leads.docs.length,0);
});

test('consent refused or missing stores nothing; invalid phone is reported without saving',async t=>{
  const f=await fixture(t);
  for(const consentGiven of [false,undefined,'true','sí'])assert.equal((await f.post('/api/chef/public/alexa/leads',demo({consentGiven}))).status,400);
  const phone=await f.post('/api/chef/public/alexa/leads',demo({phone:'12345'}));
  assert.equal(phone.status,400);assert.equal((await phone.json()).field,'phone');
  assert.equal((await f.post('/api/chef/public/alexa/leads',demo({city:''}))).status,400);
  assert.equal(f.leads.docs.length,0);
});

test('retries with the same key return the stored request; other data with that key conflicts',async t=>{
  const f=await fixture(t);
  const first=await Promise.all([1,2,3].map(()=>f.post('/api/chef/public/alexa/leads',demo())));
  assert.ok(first.every(r=>r.status===200));
  assert.equal(f.leads.docs.length,1);
  const again=await f.post('/api/chef/public/alexa/leads',demo());
  assert.equal((await again.json()).duplicate,true);
  assert.equal(f.leads.docs.length,1);
  assert.equal((await f.post('/api/chef/public/alexa/leads',demo({phone:'7085550199'}))).status,409);
  // The key is scoped to the Alexa user and seller, so another household is a separate request.
  assert.equal((await f.post('/api/chef/public/alexa/leads',demo({alexaUser:otherAlexaUser}))).status,200);
  assert.equal(f.leads.docs.length,2);
  assert.notEqual(f.leads.docs[0].offlineRequestId,f.leads.docs[1].offlineRequestId);
});

test('voice lookups and new requests are limited per Alexa user without blocking a retry',async t=>{
  const f=await fixture(t,{limits:{alexaSellerLookups:2,alexaLeadsPerUserDay:1}});
  for(let n=0;n<2;n++)assert.equal((await f.post('/api/chef/public/alexa/seller',{alexaUser,vendedor:'nadie'})).status,404);
  assert.equal((await f.post('/api/chef/public/alexa/seller',{alexaUser,vendedor:'eric ejemplo'})).status,429);
  assert.equal((await f.post('/api/chef/public/alexa/leads',demo())).status,200);
  assert.equal((await f.post('/api/chef/public/alexa/leads',demo())).status,200);
  assert.equal((await f.post('/api/chef/public/alexa/leads',demo({requestId:'b'.repeat(40)}))).status,429);
  assert.equal(f.leads.docs.length,1);
});

test('the seller can be named with «se llama» or with the six-digit code shown next to the QR',async t=>{
  assert.deepEqual(spokenSellerCandidates('mi vendedor se llama Eric Ejemplo'),['eric-ejemplo']);
  assert.equal(spokenVoiceCode('el código para alexa es cuatro ocho dos nueve uno tres'),'482913');
  assert.equal(spokenVoiceCode('482 913'),'482913');
  assert.equal(spokenVoiceCode('cuatro ocho dos'),'');
  assert.equal(spokenVoiceCode('eric ejemplo'),'');
  const f=await fixture(t);
  for(const vendedor of ['mi vendedor se llama Eric Ejemplo','cuatro ocho dos nueve uno tres','el código es 482913']){
    const r=await f.post('/api/chef/public/alexa/seller',{alexaUser,vendedor});
    assert.equal(r.status,200,vendedor);assert.deepEqual(await r.json(),{slug:'eric-ejemplo',sellerName:'Eric Ejemplo'});
  }
  assert.equal((await f.post('/api/chef/public/alexa/seller',{alexaUser,vendedor:'cuatro ocho dos nueve uno cuatro'})).status,404);
  f.contexts.delete('eric-ejemplo');
  assert.equal((await f.post('/api/chef/public/alexa/seller',{alexaUser:otherAlexaUser,vendedor:'482913'})).status,404);
});

test('the product said by voice becomes the interest; without it the generic text stays',async t=>{
  const f=await fixture(t);
  assert.equal((await f.post('/api/chef/public/alexa/leads',demo({interest:'las ollas'}))).status,200);
  assert.equal((await f.post('/api/chef/public/alexa/leads',demo({requestId:'b'.repeat(40)}))).status,200);
  assert.deepEqual(f.leads.docs.map(doc=>doc.interest),['las ollas','Demostración solicitada por voz en Alexa']);
});

test('only the authenticated voice route can label a request as made by voice',async t=>{
  const f=await fixture(t);
  const r=await fetch(f.base+'/api/chef/public/leads?chef=eric-ejemplo',
    {method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({...demo(),email:'',interest:'ollas',origin:'alexa-voz',appointmentRequested:true})});
  assert.equal(r.status,400);
  assert.equal(f.leads.docs.length,0);
});

test('the customer page and the seller panel show the seller code only for that seller',async t=>{
  const f=await fixture(t);
  assert.equal((await (await f.get('/api/chef/public/bootstrap?chef=eric-ejemplo')).json()).voiceCode,'482913');
  assert.equal((await (await f.get('/api/chef/public/bootstrap?chef=lorena-prueba')).json()).voiceCode,'');
  assert.equal((await (await f.get('/api/chef/public/bootstrap')).json()).voiceCode,'');
});
