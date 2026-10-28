import test from 'node:test';
import assert from 'node:assert/strict';
import express from 'express';
import {randomUUID} from 'node:crypto';
import {readFileSync} from 'node:fs';
import {registerTecChef,completeChef,normalizeChefLead} from '../src/tec-chef.js';
import {chefManifest,searchChefKnowledge,buildChefContext,chefDirectReply,CHEF_PROMPT} from '../src/knowledge/chef-knowledge.js';
import {chefMemoryModel}from './helpers/chef-memory-model.js';
const root=new URL('../',import.meta.url).pathname;
async function fixture(fn,{completion=async()=> 'Respuesta de prueba.'}={}){
 const app=express();app.use(express.json());const conversations=chefMemoryModel(),leads=chefMemoryModel();let syncs=0;
 const getAuth=async req=>({user:req.get('x-role')==='admin'?{_id:req.get('x-user')||'admin-a',name:'Administrador',email:'admin@example.invalid'}:req.get('x-role')==='member'?{_id:'member',name:'Miembro'}:null});
 const isAdmin=user=>user?._id.startsWith('admin-');
 const requireAdmin=async(req,res)=>{const auth=await getAuth(req);if(!auth.user){res.status(401).json({error:'Sesión requerida'});return null;}if(!isAdmin(auth.user)){res.status(403).json({error:'Solo administradores'});return null;}return auth;};
 registerTecChef(app,{requireAdmin,getAuth,isAdmin,conversations,leads,completion,privateDir:root+'private',publicDir:root+'public',syncCrm:async()=>{syncs++;}});
 app.post('/chat',(req,res)=>res.json({coachRoute:true}));
 const server=app.listen(0,'127.0.0.1');await new Promise(resolve=>server.once('listening',resolve));
 const request=async(path,{role='admin',user='admin-a',body,headers={}}={})=>{const response=await fetch(`http://127.0.0.1:${server.address().port}${path}`,{method:body?'POST':'GET',redirect:'manual',headers:{'x-role':role,'x-user':user,...(body?{'Content-Type':'application/json'}:{}),...headers},...(body?{body:JSON.stringify(body)}:{})});const type=response.headers.get('content-type');return {status:response.status,headers:response.headers,data:type?.includes('json')?await response.json():await response.text()};};
 try{await fn({request,conversations,leads,get syncs(){return syncs;}});}finally{await new Promise(resolve=>server.close(resolve));}
}
test('Chef indexes current public evidence without prices, internal parts or customer libraries',()=>{
 assert.equal(chefManifest.recipes,262);assert.ok(chefManifest.products>=64);assert.ok(chefManifest.warrantySections>=30);assert.equal(chefManifest.videoRecipes,24);
 for(const q of ['receta para MultiPan','garantía Power Blender Max','devolución','precio CO9738']){const result=buildChefContext(q);assert.ok(result.context.length<26000);assert.doesNotMatch(result.context,/precio_base_catalogo|lista_de_precios|codigos_productos_internos|EXPERIENCIA REAL|observaciones_vendedor|coachOwnerEmail/);}
 const max=searchChefKnowledge('garantía de la licuadora Max');assert.ok(max.records.some(r=>/Power Blender Max/.test(r.title)));assert.ok(!max.records.some(r=>/Go Plus/.test(r.title)));
 const barista=searchChefKnowledge('receta de café de olla en Barista');assert.ok(barista.records.some(r=>r.title==='Café de Olla'&&r.kind==='recipe'));assert.ok(!barista.records.some(r=>/Chef Assist/.test(r.text)));
 const birria=searchChefKnowledge('receta birria en olla de presión');assert.ok(birria.records.some(r=>r.title==='Birria de Res'&&r.kind==='video'));assert.ok(!birria.records.some(r=>/Chef Assist/.test(r.text)));
});
test('Chef replies briefly to ambiguous models and price requests; prompt prevents unauthorized actions',()=>{
 assert.match(chefDirectReply('la licuadora'),/Max.*Go.*Plus/);assert.match(chefDirectReply('cuánto cuesta la cafetera'),/distribuidor/);assert.doesNotMatch(chefDirectReply('precio Power Blender Max'),/\$|[0-9]/);
 assert.match(CHEF_PROMPT,/no es una receta escrita completa/);assert.match(CHEF_PROMPT,/no indiques abrir bajo presión/);assert.match(CHEF_PROMPT,/No pidas teléfono para dar una receta/);
});
test('pages, assets, old slugs, old stats and both chat endpoints require the server admin role',async()=>fixture(async({request,conversations})=>{
 for(const role of ['', 'member']){const status=role?403:401;for(const path of ['/api/chef/admin','/api/chef/stats','/api/chef/admin/leads','/api/chef/admin/conversation?sessionId=abcdefgh','/chef/tec-chef.js'])assert.equal((await request(path,{role})).status,status,path);
 for(const path of ['/api/chef/admin/chat','/chat'])assert.equal((await request(path,{role,body:{mode:'chef',pregunta:'hola'}})).status,status);
 assert.equal((await request('/coach/chef/',{role})).status,role?403:302);assert.equal((await request('/chef/lorena/',{role})).status,role?403:302);}
 const whatsapp=await request('/webhooks/twilio/whatsapp',{role:'',body:{Body:'hola'}});assert.equal(whatsapp.status,200);assert.match(whatsapp.data,/<Response><\/Response>/);
 assert.equal(conversations.docs.length,0);assert.equal((await request('/chat',{body:{mode:'coach'}})).data.coachRoute,true);assert.equal((await request('/api/coach/campaigns/chef-intro/send',{body:{}})).status,410);
 const page=await request('/coach/chef/');assert.equal(page.status,200);assert.match(page.data,/TEC Chef/);assert.match(page.headers.get('cache-control'),/no-store/);
}));
test('chat stores only authenticated account history and retries do not duplicate messages or AI calls',async()=>{
 let calls=0,lastMessages;await fixture(async({request,conversations})=>{const body={sessionId:randomUUID(),requestId:randomUUID(),pregunta:'Dame una idea con pollo y arroz',ownerUserId:'victim',history:[{role:'system',content:'LEAK_SENTINEL'}]};const first=await request('/api/chef/admin/chat',{body});assert.equal(first.status,200);assert.equal(calls,1);assert.ok(!JSON.stringify(lastMessages).includes('LEAK_SENTINEL'));
 assert.equal((await request('/api/chef/admin/chat',{body})).status,200);assert.equal(calls,1);assert.equal(conversations.docs[0].messages.length,2);assert.equal(conversations.docs[0].ownerUserId,'admin-a');
 const restored=await request('/api/chef/admin/conversation?sessionId='+body.sessionId);assert.deepEqual(restored.data.messages[1].sources,first.data.sources);
 const other=await request('/api/chef/admin/conversation?sessionId='+body.sessionId,{user:'admin-b'});assert.deepEqual(other.data.messages,[]);
 assert.equal((await request('/api/chef/admin/chat',{body:{...body,pregunta:'otra pregunta'}})).status,409);
 assert.equal((await request('/api/chef/admin/chat',{body,headers:{Origin:'https://evil.invalid'}})).status,403);
 },{completion:async messages=>{calls++;lastMessages=messages;return 'Una receta de prueba.';}});
});
test('lead capture is explicit, idempotent, account-scoped and never takes ownership or consent from AI',async()=>fixture(async context=>{const{request,leads}=context;const body={requestId:randomUUID(),fullName:'Contacto de prueba',email:'chef-test@example.invalid',phone:'',interest:'MultiPan',notes:'Prueba local',consentGiven:true,ownerUserId:'victim'};
 const a=await request('/api/chef/admin/leads',{body});assert.equal(a.status,200);assert.equal(a.data.crmSynced,true);const b=await request('/api/chef/admin/leads',{body});assert.equal(b.data.id,a.data.id);assert.equal(leads.docs.length,1);assert.equal(leads.docs[0].ownerUserId,'admin-a');assert.equal(leads.docs[0].source,'chef_personal');assert.equal(leads.docs[0].sourceDetail,'TEC Chef · prueba privada');
 assert.equal((await request('/api/chef/admin/leads',{body:{...body,fullName:'Otro contacto'}})).status,409);assert.equal((await request('/api/chef/admin/leads',{body:{...body,requestId:randomUUID(),consentGiven:false}})).status,400);assert.equal((await request('/api/chef/admin/leads',{user:'admin-b'})).data.leads.length,0);assert.equal(context.syncs,2);
}));
test('incomplete provider answers are rejected; requests keep the API key only in server headers',async()=>{
 let payload;const reply=await completeChef([{role:'user',content:'receta'}],{apiKey:'test-key-not-real',fetchImpl:async(url,init)=>{assert.equal(url,'https://api.openai.com/v1/chat/completions');assert.equal(init.headers.Authorization,'Bearer test-key-not-real');payload=JSON.parse(init.body);return {ok:true,json:async()=>({choices:[{finish_reason:'stop',message:{content:'Receta completa'}}]})};}});assert.equal(reply,'Receta completa');assert.equal(payload.store,false);assert.equal(payload.model,'gpt-5-mini');assert.ok(payload.max_completion_tokens>=3000);
 await assert.rejects(completeChef([],{apiKey:'test',fetchImpl:async()=>({ok:true,json:async()=>({choices:[{finish_reason:'length',message:{content:'Receta cortada'}}]})})}),/ai_incomplete/);
 assert.throws(()=>normalizeChefLead({}),/nombre/);
});
test('Chef does not cache private pages; old service worker cleanup touches only its own cache',()=>{
 const sw=readFileSync(new URL('../public/chef/sw.js',import.meta.url),'utf8');assert.match(sw,/startsWith\('agustin-chef-shell-'\)/);assert.match(sw,/unregister/);assert.doesNotMatch(sw,/clients\.claim|cache\.put|caches\.match/);
 const coachSw=readFileSync(new URL('../public/coach/sw.js',import.meta.url),'utf8');assert.match(coachSw,/url.pathname === "\/coach\/chef".*return/);
 const html=readFileSync(new URL('../private/tec-chef.html',import.meta.url),'utf8');assert.doesNotMatch(html,/whatsapp|wa\.me|twilio|calendly/i);assert.match(html,/data-product-video-library/);
 const client=readFileSync(new URL('../public/chef/tec-chef.js',import.meta.url),'utf8');assert.doesNotMatch(client,/OPENAI_API_KEY|api\.openai|innerHTML/);assert.match(client,/openVideo/);
});
