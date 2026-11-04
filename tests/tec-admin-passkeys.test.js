import test from 'node:test';import assert from 'node:assert/strict';import crypto from 'node:crypto';import express from 'express';import {encodeCBOR} from '@levischuck/tiny-cbor';
import {registerAdminPasskeys,adminAssured,ADMIN_ASSURANCE_MS} from '../src/tec-admin-passkeys.js';
const b64=v=>Buffer.from(v).toString('base64url'),hash=v=>crypto.createHash('sha256').update(v).digest();
function authenticator(){
 const {publicKey,privateKey}=crypto.generateKeyPairSync('ec',{namedCurve:'prime256v1'}),jwk=publicKey.export({format:'jwk'}),id=crypto.randomBytes(32),credential=b64(id),rp=hash('totalelitecoaching.com');
 const cose=new Map([[1,2],[3,-7],[-1,1],[-2,new Uint8Array(Buffer.from(jwk.x,'base64url'))],[-3,new Uint8Array(Buffer.from(jwk.y,'base64url'))]]);
 return{credential,register(challenge,{origin='https://totalelitecoaching.com',verified=true}={}){
  const count=Buffer.alloc(4),length=Buffer.alloc(2);length.writeUInt16BE(id.length);const auth=Buffer.concat([rp,Buffer.from([verified?0x45:0x41]),count,Buffer.alloc(16),length,id,Buffer.from(encodeCBOR(cose))]);
  const client=Buffer.from(JSON.stringify({type:'webauthn.create',challenge,origin,crossOrigin:false}));
  const object=new Map([['fmt','none'],['attStmt',new Map()],['authData',new Uint8Array(auth)]]);
  return{id:credential,rawId:credential,type:'public-key',response:{clientDataJSON:b64(client),attestationObject:b64(encodeCBOR(object)),transports:['internal']},clientExtensionResults:{},authenticatorAttachment:'platform'};
 },authenticate(challenge,{origin='https://totalelitecoaching.com',counter=1,verified=true,badSignature=false}={}){
  const count=Buffer.alloc(4);count.writeUInt32BE(counter);const auth=Buffer.concat([rp,Buffer.from([verified?0x05:0x01]),count]);const client=Buffer.from(JSON.stringify({type:'webauthn.get',challenge,origin,crossOrigin:false}));
  const sig=crypto.sign('sha256',Buffer.concat([auth,hash(client)]),privateKey);if(badSignature)sig[5]^=255;
  return{id:credential,rawId:credential,type:'public-key',response:{clientDataJSON:b64(client),authenticatorData:b64(auth),signature:b64(sig),userHandle:null},clientExtensionResults:{},authenticatorAttachment:'platform'};
 }};
}
function model(){const data=[];const matches=(x,q)=>Object.entries(q).every(([k,v])=>v&&typeof v==='object'&&'$gt'in v?new Date(x[k])>v.$gt:x[k]===v);let n=0;
 const query=values=>({select(){return this;},lean:async()=>structuredClone(values)});
 return{data,exists:async q=>data.some(x=>matches(x,q)),find:q=>query(data.filter(x=>matches(x,q))),findOne:q=>query(data.find(x=>matches(x,q))||null),create:async row=>{if(row.credentialId&&data.some(x=>x.credentialId===row.credentialId))throw Error('duplicate');const x={_id:row._id||String(++n),...structuredClone(row)};data.push(x);return x;},deleteMany:async q=>{for(let i=data.length-1;i>=0;i--)if(matches(data[i],q))data.splice(i,1);},findOneAndDelete:async q=>{const i=data.findIndex(x=>matches(x,q));return i<0?null:data.splice(i,1)[0];},deleteOne:async q=>{const i=data.findIndex(x=>matches(x,q));if(i>=0)data.splice(i,1);},updateOne:async(q,update)=>{const x=data.find(x=>matches(x,q));if(x)for(const[k,v]of Object.entries(update.$max||{}))x[k]=Math.max(x[k]||0,v);}};
}
async function fixture(t){
 const keys=model(),challenges=model();let current='A',clock=Date.now();const sessions={A:{tokenHash:'synthetic-A',save:async()=>{}},B:{tokenHash:'synthetic-B',save:async()=>{}}};
 const app=express();app.use(express.json());const service=registerAdminPasskeys(app,{keys,challenges,getAuth:async()=>current==='none'?null:{user:{_id:current,email:current+'@example.test',name:'Fictitious admin',passwordSalt:'synthetic',passwordHash:'synthetic'},session:sessions[current]},isAdmin:u=>u._id!=='B',verifyPassword:async p=>p==='synthetic-password',now:()=>clock});
 const server=app.listen(0,'127.0.0.1');await new Promise(resolve=>server.once('listening',resolve));t.after(()=>new Promise(resolve=>server.close(resolve)));
 async function request(path='',body,method=body?'POST':'GET'){const r=await fetch(`http://127.0.0.1:${server.address().port}/api/coach/admin-security${path}`,{method,headers:{'Content-Type':'application/json'},...(body?{body:JSON.stringify(body)}:{})});return{status:r.status,data:await r.json()};}
 return{keys,challenges,service,request,sessions,setUser:u=>current=u,setClock:t=>clock=t,clock:()=>clock};
}
async function enroll(f,device,extra={}){const options=await f.request('/enroll/options',{password:'synthetic-password'});assert.equal(options.status,200);return f.request('/enroll/verify',{challengeId:options.data.challengeId,response:device.register(options.data.options.challenge,extra),label:'Synthetic test device'});}
test('first enrollment is opt-in, uses a real verified WebAuthn registration and stores only the public key',async t=>{
 const f=await fixture(t),device=authenticator();assert.equal(await f.service.needsVerification({user:{_id:'A'},session:f.sessions.A}),false);const result=await enroll(f,device);assert.equal(result.status,200);assert.equal(f.keys.data.length,1);assert.equal(f.keys.data[0].ownerUserId,'A');assert.equal(adminAssured(f.sessions.A),true);assert.equal('privateKey'in f.keys.data[0],false);assert.equal((await f.request()).data.enrolled,true);
});
test('a new admin session needs step-up; a valid signed response verifies it and cannot be replayed',async t=>{
 const f=await fixture(t),device=authenticator();await enroll(f,device);delete f.sessions.A.adminVerifiedAt;assert.equal(await f.service.needsVerification({user:{_id:'A'},session:f.sessions.A}),true);
 const options=await f.request('/verify/options',{});const body={challengeId:options.data.challengeId,response:device.authenticate(options.data.options.challenge)};
 assert.equal((await f.request('/verify/complete',body)).status,200);assert.equal(f.keys.data[0].counter,1);assert.equal(adminAssured(f.sessions.A),true);assert.equal((await f.request('/verify/complete',body)).status,400);
});
test('wrong password cannot enroll or create a challenge',async t=>{const f=await fixture(t);assert.equal((await f.request('/enroll/options',{password:'incorrect'})).status,403);assert.equal(f.challenges.data.length,0);});
test('registration from a foreign origin or without user verification never enables protection',async t=>{
 for(const extra of [{origin:'https://foreign.example'},{verified:false}]){const f=await fixture(t),device=authenticator();const r=await enroll(f,device,extra);assert.equal(r.status,400);assert.equal(f.keys.data.length,0);}
});
test('a bad signature or missing user verification cannot step up the session',async t=>{
 for(const extra of [{badSignature:true},{verified:false}]){const f=await fixture(t),device=authenticator();await enroll(f,device);delete f.sessions.A.adminVerifiedAt;const options=await f.request('/verify/options',{});const r=await f.request('/verify/complete',{challengeId:options.data.challengeId,response:device.authenticate(options.data.options.challenge,extra)});assert.equal(r.status,400);assert.equal(adminAssured(f.sessions.A),false);}
});
test('unknown users and ordinary representatives cannot read enrollment status',async t=>{const f=await fixture(t);f.setUser('none');assert.equal((await f.request()).status,401);f.setUser('B');assert.equal((await f.request()).status,403);});
test('challenge expires after five minutes and is tied to its session token hash',async t=>{
 for(const alter of [f=>f.setClock(f.clock()+6*60000),f=>{f.sessions.A.tokenHash='another-synthetic-session';}]){const f=await fixture(t),device=authenticator();const options=await f.request('/enroll/options',{password:'synthetic-password'});alter(f);const r=await f.request('/enroll/verify',{challengeId:options.data.challengeId,response:device.register(options.data.options.challenge)});assert.equal(r.status,400);assert.equal(f.keys.data.length,0);}
});
test('enrolled admins cannot add another key from an unverified session using password alone',async t=>{const f=await fixture(t);await enroll(f,authenticator());delete f.sessions.A.adminVerifiedAt;assert.equal((await f.request('/enroll/options',{password:'synthetic-password'})).status,403);assert.equal(f.keys.data.length,1);});
test('deleting a key requires recent verified presence and the account password',async t=>{
 const f=await fixture(t),device=authenticator();await enroll(f,device);assert.equal((await f.request('/keys/'+device.credential,{password:'wrong'},'DELETE')).status,403);f.setClock(f.clock()+6*60000);assert.equal((await f.request('/keys/'+device.credential,{password:'synthetic-password'},'DELETE')).status,403);assert.equal(f.keys.data.length,1);
});
test('admin assurance expires, rejects future timestamps, and does not change basic Coach sessions',()=>{const at=Date.now();assert.equal(adminAssured({adminVerifiedAt:new Date(at)},at),true);assert.equal(adminAssured({adminVerifiedAt:new Date(at-ADMIN_ASSURANCE_MS)},at),false);assert.equal(adminAssured({adminVerifiedAt:new Date(at+1000)},at),false);assert.equal(adminAssured({}),false);});
