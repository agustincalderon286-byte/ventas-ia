import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import express from 'express';
import sharp from 'sharp';
import path from 'node:path';
import {fileURLToPath} from 'node:url';
import {randomUUID} from 'node:crypto';
import mongoose from 'mongoose';
import QRCode from 'qrcode';
import {createPublicChefModels,registerPublicChef} from '../src/tec-chef-public.js';
import {pcmToWav} from '../src/coach-voice.js';
import {downlineMembers} from '../src/tec-team-supervision.js';
import {chefPublicMemoryModel as memory} from './helpers/chef-public-memory.js';
const root=fileURLToPath(new URL('../',import.meta.url));
const account=(n,name,parent=null,role='seat',supervised=false)=>({_id:String(n).repeat(24),name,email:name.toLowerCase()+'@example.test',parentUserId:parent,accountType:role,teamSupervisionEnabled:supervised});
const owner=account(1,'Owner',null,'owner'),lorena=account(2,'Lorena',owner._id,'seat',true),eric=account(3,'Eric',lorena._id),peer=account(4,'Peer',owner._id),outside=account(5,'Outside',null,'owner');
const profile=user=>({slug:user.name.toLowerCase(),userDoc:user,ownership:{ownerUserId:user===outside?outside._id:owner._id,ownerName:user===outside?outside.name:owner.name,ownerEmail:user===outside?outside.email:owner.email}});
async function fixture(t,{limits={},defaultContext=profile(owner),syncFailure=false,voice=null}={}){
 const app=express();app.use(express.json({limit:'15mb'}));
 const users=memory([owner,lorena,eric,peer,outside]),visitors=memory(),conversations=memory(),leads=memory(),contexts=new Map([owner,lorena,eric,peer,outside].map(user=>[user.name.toLowerCase(),profile(user)])),ai=[],synced=[];
 const auth=async(req,res)=>{const user=users.docs.find(u=>u._id===req.get('X-Test-User'));if(!user){res.status(401).json({error:'Acceso requerido'});return null;}return{user};};
 registerPublicChef(app,{requireActive:auth,voice,getBySlug:async slug=>contexts.get(slug)||null,getDefault:async()=>defaultContext,getOwnContext:async user=>contexts.get(user.name.toLowerCase())||null,getDownline:user=>downlineMembers({...user,teamSupervisionEnabled:true},users),hasDownline:async user=>users.docs.some(member=>member.parentUserId===user._id),visitors,conversations,leads,privateDir:path.join(root,'private'),publicDir:path.join(root,'public'),limits,completion:async messages=>{ai.push(messages);return 'Respuesta de cocina.';},syncCrm:async(user,lead)=>{synced.push({user,lead});if(syncFailure)throw Error('crm');}});
 const server=await new Promise(resolve=>{const s=app.listen(0,'127.0.0.1',()=>resolve(s));});t.after(()=>{server.closeAllConnections();server.close();});
 const base='http://127.0.0.1:'+server.address().port;
 const request=(url,{body,cookie,user,headers={}}={})=>fetch(base+url,{redirect:'manual',method:body?'POST':'GET',headers:{...(body?{'Content-Type':'application/json',Origin:'https://totalelitecoaching.com'}:{}),...(cookie?{Cookie:cookie}:{}),...(user?{'X-Test-User':user._id}:{}),...headers},...(body?{body:JSON.stringify(body)}:{})});
 const bootstrap=async(slug='eric')=>{const r=await request('/api/chef/public/bootstrap'+(slug?'?chef='+slug:''));assert.equal(r.status,200);return(r.headers.get('set-cookie')||'').split(';')[0];};
 return{request,bootstrap,leads,conversations,visitors,contexts,ai,synced,users,base};
}
const lead=(requestId=randomUUID())=>({requestId,fullName:'Cliente de prueba',phone:'7085550100',email:'',city:'Hillside, IL',interest:'Power Blender Max',notes:'Pregunta de ejemplo',consentGiven:true});

test('the public page is separate and contains no distributor navigation or private contact list',async t=>{
 const f=await fixture(t),r=await f.request('/chef/eric/');assert.equal(r.status,200);const html=await r.text();assert.match(html,/id="customer-content"/);assert.doesNotMatch(html,/<a[^>]+href=["'][^"']*(?:coach|control)/i);assert.doesNotMatch(html,/id="(?:leads|refresh-leads|admin-name)"|Prueba privada|Mi cuenta|Cerrar sesi[oó]n/);
 for(const asset of ['customer.js','customer.css','robot.webp','tec-chef.css'])assert.equal((await f.request('/chef/public/'+asset)).status,200);
 assert.equal((await f.request('/chef/public/../tec-chef.js')).status,404);
});
test('public bootstrap discloses only the seller display name and creates an opaque HTTP-only visitor session',async t=>{
 const f=await fixture(t),r=await f.request('/api/chef/public/bootstrap?chef=eric'),data=await r.json();assert.equal(data.sellerName,'Eric');assert.equal(data.contactAvailable,true);assert.equal(JSON.stringify(data).includes(eric.email),false);assert.equal(JSON.stringify(data).includes(owner._id),false);assert.ok(!('user'in data));assert.match(r.headers.get('set-cookie'),/HttpOnly/);assert.match(r.headers.get('set-cookie'),/SameSite=Lax/);assert.equal(f.visitors.docs.length,1);assert.ok(!r.headers.get('set-cookie').includes(f.visitors.docs[0]._id));
});
test('each QR fixes the recipient on the server, ignoring forged owner and seller fields',async t=>{
 const f=await fixture(t),cookie=await f.bootstrap(),body={...lead(),ownerUserId:outside._id,generatedByUserId:peer._id,sellerEmail:outside.email,status:'agendado',chefSlug:'outside'};
 const r=await f.request('/api/chef/public/leads?chef=eric',{cookie,body});assert.equal(r.status,200);const data=await r.json();assert.equal(data.sellerName,'Eric');assert.equal(f.leads.docs.length,1);const saved=f.leads.docs[0];assert.equal(saved.ownerUserId,owner._id);assert.equal(saved.generatedByUserId,eric._id);assert.equal(saved.chefSlug,'eric');assert.equal(saved.status,'nuevo');assert.equal(saved.source,'chef_personal');assert.equal(f.synced[0].user._id,eric._id);assert.equal(f.ai.length,0);
});
test('contact intake requires explicit consent and valid contact, and cannot assert a confirmed appointment',async t=>{
 const f=await fixture(t),cookie=await f.bootstrap();for(const body of [{...lead(),consentGiven:false},{...lead(),phone:'123'},{...lead(),appointmentRequested:true}]){assert.equal((await f.request('/api/chef/public/leads?chef=eric',{cookie,body})).status,400);}assert.equal(f.leads.docs.length,0);
 const r=await f.request('/api/chef/public/leads?chef=eric',{cookie,body:{...lead(),appointmentRequested:true,appointmentPreferredTime:'martes 5 pm',appointmentStatus:'confirmada'}});assert.equal(r.status,200);assert.equal((await r.json()).appointmentStatus,'solicitada');assert.equal(f.leads.docs[0].appointmentStatus,'solicitada');
});
test('lost replies and concurrent retries create one lead; changed data with the same key conflicts',async t=>{
 const f=await fixture(t),cookie=await f.bootstrap(),body=lead();const results=await Promise.all([1,2,3].map(()=>f.request('/api/chef/public/leads?chef=eric',{cookie,body})));assert.ok(results.every(r=>r.status===200));assert.equal(f.leads.docs.length,1);
 assert.equal((await f.request('/api/chef/public/leads?chef=eric',{cookie,body:{...body,phone:'7085550199'}})).status,409);assert.equal(f.leads.docs.length,1);
 const otherCookie=await f.bootstrap('peer');assert.equal((await f.request('/api/chef/public/leads?chef=peer',{cookie:otherCookie,body})).status,200);assert.equal(f.leads.docs[1].generatedByUserId,peer._id);assert.notEqual(f.leads.docs[1].offlineRequestId,f.leads.docs[0].offlineRequestId);
});
test('a CRM failure retains the attributed lead instead of losing a customer request',async t=>{
 const f=await fixture(t,{syncFailure:true}),cookie=await f.bootstrap(),r=await f.request('/api/chef/public/leads?chef=eric',{cookie,body:lead()});assert.equal(r.status,200);assert.equal((await r.json()).crmSynced,false);assert.equal(f.leads.docs[0].generatedByUserId,eric._id);
});
test('seller and upline lead reads follow actual ancestry, excluding peers, uplines and unrelated owners',async t=>{
 const f=await fixture(t);
 for(const slug of ['eric','lorena','peer','owner','outside']){const cookie=await f.bootstrap(slug);await f.request('/api/chef/public/leads?chef='+slug,{cookie,body:lead()});}
 const read=async(user,scope='mine')=>{const r=await f.request('/api/chef/workspace?scope='+scope,{user});assert.equal(r.status,200);return(await r.json()).leads.map(l=>l.sellerName).sort();};
 assert.deepEqual(await read(eric),['Eric']);assert.deepEqual(await read(lorena,'team'),['Eric','Lorena']);assert.deepEqual(await read(owner,'team'),['Eric','Lorena','Owner','Peer']);assert.deepEqual(await read(outside,'team'),['Outside']);
 assert.equal((await f.request('/api/chef/workspace?scope=team',{user:eric})).status,403);
 assert.equal((await f.request('/api/chef/workspace?scope=team&seller='+peer._id,{user:lorena})).status,403);
 assert.equal((await f.request('/api/chef/workspace?scope=mine&seller='+eric._id,{user:lorena})).status,403);
});
test('anonymous sessions never grant distributor access and stale-account reads/QRs fail closed',async t=>{
 const f=await fixture(t),cookie=await f.bootstrap();for(const url of ['/coach/chef/','/api/chef/workspace','/api/chef/workspace/qr.svg'])assert.equal((await f.request(url,{cookie})).status,401);
 assert.equal((await f.request('/api/chef/workspace',{user:lorena,headers:{'X-TEC-Account-ID':eric._id}})).status,403);assert.equal((await f.request('/api/chef/workspace/qr.svg?account='+eric._id,{user:lorena})).status,403);
});
test('personal QR encodes the canonical customer URL for the authenticated seller',async t=>{
 const f=await fixture(t);const r=await f.request('/api/chef/workspace/qr.svg?account='+eric._id,{user:eric});assert.equal(r.status,200);assert.match(r.headers.get('content-type'),/image\/svg\+xml/);assert.equal(await r.text(),await QRCode.toString('https://totalelitecoaching.com/chef/eric/',{type:'svg',width:280,margin:2,errorCorrectionLevel:'M'}));assert.equal(r.headers.get('cache-control'),'private, no-store');
 const data=await(await f.request('/api/chef/workspace',{user:eric})).json();assert.equal(data.shareUrl,'https://totalelitecoaching.com/chef/eric/');
});
test('an inactive or unknown personal link never falls back to a different distributor',async t=>{
 const f=await fixture(t);assert.equal((await f.request('/chef/not-found/')).status,404);assert.equal((await f.request('/api/chef/public/bootstrap?chef=missing')).status,404);f.contexts.delete('eric');assert.equal((await f.request('/api/chef/public/bootstrap?chef=eric')).status,404);assert.equal((await f.request('/api/chef/public/bootstrap?chef[]=eric')).status,400);assert.equal(f.leads.docs.length,0);
});
test('conversations require the same visitor and seller, even if session IDs are guessed',async t=>{
 const f=await fixture(t),cookie=await f.bootstrap(),sessionId=randomUUID(),body={pregunta:'Una receta de prueba',sessionId,requestId:randomUUID()};let r=await f.request('/api/chef/public/chat?chef=eric',{cookie,body});assert.equal(r.status,200);assert.equal(f.ai.length,1);
 r=await f.request('/api/chef/public/chat?chef=eric',{cookie,body});assert.equal(r.status,200);assert.equal(f.ai.length,1);
 const otherCookie=await f.bootstrap();for(const [suffix,who]of [['eric',otherCookie],['peer',cookie]]){const response=await f.request('/api/chef/public/conversation?chef='+suffix+'&sessionId='+sessionId,{cookie:who});assert.deepEqual((await response.json()).messages,[]);}
 assert.equal((await f.request('/api/chef/public/conversation?chef=eric&sessionId='+sessionId)).status,401);
});
test('public chat has same-origin protection and bounded daily use without blocking an idempotent retry',async t=>{
 const f=await fixture(t,{limits:{chatsPerVisitorDay:2}}),cookie=await f.bootstrap(),body={pregunta:'¿Cuánto cuesta una olla?',sessionId:randomUUID(),requestId:randomUUID()};
 assert.equal((await f.request('/api/chef/public/chat?chef=eric',{cookie,body,headers:{Origin:'https://evil.example'}})).status,403);
 assert.equal((await f.request('/api/chef/public/chat?chef=eric',{cookie,body,headers:{'Sec-Fetch-Site':'cross-site'}})).status,403);
 assert.equal((await f.request('/api/chef/public/chat?chef=eric',{cookie,body})).status,200);const second={...body,requestId:randomUUID()};assert.equal((await f.request('/api/chef/public/chat?chef=eric',{cookie,body:second})).status,200);
 assert.equal((await f.request('/api/chef/public/chat?chef=eric',{cookie,body:second})).status,200);const blocked=await f.request('/api/chef/public/chat?chef=eric',{cookie,body:{...body,requestId:randomUUID(),sessionId:randomUUID()}});assert.equal(blocked.status,429);assert.ok(blocked.headers.get('retry-after'));assert.equal(f.conversations.docs.length,1);
});
test('generic public Chef works without a configured recipient but cannot silently misassign a request',async t=>{
 const f=await fixture(t,{defaultContext:null}),cookie=await f.bootstrap(''),r=await f.request('/api/chef/public/leads',{cookie,body:lead()});assert.equal(r.status,400);assert.equal(f.leads.docs.length,0);
 const chat=await f.request('/api/chef/public/chat',{cookie,body:{pregunta:'Quiero solicitar una cita',requestId:randomUUID(),sessionId:randomUUID()}});assert.equal(chat.status,200);assert.doesNotMatch((await chat.json()).respuesta,/Agustín/);
});
test('production schemas enforce unique anonymous conversations and expire visitor and message records',()=>{
 const isolated=new mongoose.Mongoose(),models=createPublicChefModels(isolated);assert.ok(models.conversations.schema.indexes().some(([key,opt])=>key.sellerUserId===1&&key.visitorHash===1&&key.sessionId===1&&opt.unique));assert.ok(models.visitors.schema.indexes().some(([key,opt])=>key.expiresAt&&opt.expireAfterSeconds===0));
 const source=fs.readFileSync(path.join(root,'server.js'),'utf8');assert.ok(source.indexOf('registerPublicChef(app,')<source.indexOf('registerTecChef(app,'));assert.match(source,/getBySlug: resolverCoachChefContextPorSlug/);assert.match(source,/downlineMembers\(\{_id:user\._id,teamSupervisionEnabled:true\},CoachUser\)/);
});

test('a seller who is an actual parent can supervise Chef leads without gaining global team-management rights',async t=>{const f=await fixture(t);const cookie=await f.bootstrap();await f.request('/api/chef/public/leads?chef=eric',{cookie,body:lead()});const parent=f.users.docs.find(user=>user._id===lorena._id);parent.teamSupervisionEnabled=false;const r=await f.request('/api/chef/workspace?scope=team',{user:parent});assert.equal(r.status,200);const data=await r.json();assert.equal(data.canViewTeam,true);assert.deepEqual(data.leads.map(row=>row.sellerName),['Eric']);assert.equal(parent.teamSupervisionEnabled,false);});

test('public Chef transmits a resized photo once, retains only text, and accepts a correction',async t=>{
 const f=await fixture(t),cookie=await f.bootstrap(),sessionId=randomUUID();
 const photo=(await sharp({create:{width:1900,height:700,channels:3,background:'#42a55f'}}).withMetadata({orientation:6}).jpeg().toBuffer()).toString('base64');
 const body={photo,sessionId,requestId:randomUUID()},chat='/api/chef/public/chat?chef=eric';
 assert.equal((await f.request(chat,{body})).status,401);
 assert.equal((await f.request(chat,{cookie,body})).status,200);
 const [text,image]=f.ai[0].at(-1).content;
 assert.match(text.text,/platillos/);
 const meta=await sharp(Buffer.from(image.image_url.url.split(',')[1],'base64')).metadata();
 assert.equal(meta.format,'jpeg');assert.equal(meta.height,1440);assert.equal(meta.orientation,undefined);
 assert.equal((await f.request(chat,{cookie,body})).status,200);assert.equal(f.ai.length,1);
 const differentPhoto=(await sharp({create:{width:32,height:32,channels:3,background:'#833'}}).jpeg().toBuffer()).toString('base64');
 assert.equal((await f.request(chat,{cookie,body:{...body,photo:differentPhoto}})).status,409);
 const history=await(await f.request('/api/chef/public/conversation?chef=eric&sessionId='+sessionId,{cookie})).json();
 assert.equal(history.messages[0].hasPhoto,true);assert.equal(history.messages[1].content,'Respuesta de cocina.');
 assert.doesNotMatch(JSON.stringify(f.conversations.docs),/data:image|base64/);
 assert.equal(f.leads.docs.length,0);
 const followUp={pregunta:'Era calabacita, no pepino',sessionId,requestId:randomUUID()};
 assert.equal((await f.request(chat,{cookie,body:followUp})).status,200);
 assert.match(f.ai[1].at(-2).content,/Respuesta de cocina/);
 assert.doesNotMatch(JSON.stringify(f.ai[1]),/data:image/);
});

test('invalid image data is rejected before AI and follows the same origin rules',async t=>{
 const f=await fixture(t),cookie=await f.bootstrap(),chat='/api/chef/public/chat?chef=eric';
 const body={sessionId:randomUUID(),requestId:randomUUID(),pregunta:'Quiero una receta'};
 const svg=Buffer.from('<svg xmlns="http://www.w3.org/2000/svg" width="2" height="2"></svg>').toString('base64');
 for(const photo of [null,'!',svg,'AAAA'.repeat(700000)])assert.equal((await f.request(chat,{cookie,body:{...body,photo}})).status,400);
 assert.equal((await f.request(chat,{cookie,body:{...body,photo:svg},headers:{Origin:'https://otro.example'}})).status,403);
 assert.equal(f.ai.length,0);assert.equal(f.conversations.docs.length,0);
});

test('Chef receives private recordings and speaks only an audio turn from the same visitor and seller',async t=>{
  const input=[],output=[];
  const voice={
    async transcribe(bytes,mime,options){input.push({bytes,mime,options});return '¿Qué cocino con pollo y arroz?';},
    async speak(text,id,{onChunk}){output.push(text);const pcm=Buffer.from([2,0,3,0]);onChunk?.(pcm);return pcmToWav([pcm]);}
  };
  const f=await fixture(t,{voice,limits:{voicePerVisitorDay:2}}),cookie=await f.bootstrap();
  const sessionId=randomUUID(),requestId=randomUUID();
  const clip=pcmToWav([Buffer.alloc(48000)]);
  const transcribe=(cookie,body=clip,origin='https://totalelitecoaching.com')=>fetch(f.base+'/api/chef/public/voice/transcribe?chef=eric',{
    method:'POST',headers:{'Content-Type':'audio/wav',Origin:origin,...(cookie?{Cookie:cookie}:{})},body
  });
  assert.equal((await transcribe(null)).status,401);
  assert.equal((await transcribe(cookie,clip,'https://evil.example')).status,403);
  assert.equal((await transcribe(cookie,pcmToWav([Buffer.alloc(48000*91)]))).status,400);
  const result=await transcribe(cookie);
  assert.equal(result.status,200);
  const {text}=await result.json();
  assert.equal(text,'¿Qué cocino con pollo y arroz?');
  assert.deepEqual(input.map(entry=>entry.mime),['audio/wav']);
  assert.equal((await transcribe(cookie)).status,429);
  const query={pregunta:text,sessionId,requestId,responseMode:'audio'};
  assert.equal((await f.request('/api/chef/public/chat?chef=eric',{cookie,body:query})).status,200);
  assert.equal((await f.request('/api/chef/public/chat?chef=eric',{cookie,body:query})).status,200);
  const speak=(who,slug='eric',id=requestId)=>f.request('/api/chef/public/voice/speech?chef='+slug,
    {cookie:who,body:{sessionId,requestId:id,stream:true,text:'Contenido arbitrario'}});
  assert.equal((await speak(await f.bootstrap())).status,404);
  assert.equal((await speak(cookie,'peer')).status,404);
  const textOnlyId=randomUUID();
  assert.equal((await f.request('/api/chef/public/chat?chef=eric',{cookie,body:{pregunta:'Otra receta',sessionId,requestId:textOnlyId}})).status,200);
  assert.equal((await speak(cookie,'eric',textOnlyId)).status,404);
  const playback=await speak(cookie);
  assert.equal(playback.status,200);
  assert.match(playback.headers.get('content-type'),/application\/x-ndjson/);
  assert.match(await playback.text(),/"type":"done"/);
  assert.deepEqual(output,['Respuesta de cocina.']);
  assert.doesNotMatch(JSON.stringify(f.conversations.docs),/base64|UklGR/);
});
