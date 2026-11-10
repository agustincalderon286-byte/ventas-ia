import test from 'node:test';
import assert from 'node:assert/strict';
import vm from 'node:vm';
import {readFileSync} from 'node:fs';
import {randomUUID} from 'node:crypto';

// Execute the production controller against a small DOM surface, without a
// browser, network credentials, real customers or outbound email.
class Element {
 constructor(){this.children=[];this.dataset={};this.handlers={};this.value='';this.checked=false;this.disabled=false;this.hidden=false;this.textContent='';}
 append(...items){this.children.push(...items);}
 replaceChildren(...items){this.children=[...items];}
 addEventListener(event,handler){this.handlers[event]=handler;}
 focus(){}
 async emit(event){return this.handlers[event]?.({preventDefault(){}});}
}
const settle=async()=>{for(let i=0;i<8;i++)await new Promise(resolve=>setImmediate(resolve));};
async function fixture({failFirst=false}={}){
 const nodes=new Map(),requests=[];let attempts=0;
 const get=selector=>{if(!nodes.has(selector))nodes.set(selector,new Element());return nodes.get(selector);};
 const context={document:{querySelector:get,querySelectorAll:()=>[],createElement:()=>new Element()},navigator:{onLine:true},crypto:{randomUUID},AbortSignal,URL,sessionStorage:{getItem:()=>null,setItem(){}},window:{},fetch:async(url,options)=>{
  const body=options.body?JSON.parse(options.body):undefined;requests.push({url,body});
  let data={};let ok=true;
  if(url==='/api/chef/admin')data={user:{id:'synthetic-admin',name:'Prueba'},leadEmailReady:true,manifest:{recipes:262,products:64}};
  else if(url.startsWith('/api/chef/admin/conversation'))data={messages:[]};
  else if(url==='/api/chef/admin/leads'&&!body)data={leads:[]};
  else if(url==='/api/chef/admin/leads'){attempts++;ok=!(failFirst&&attempts===1);data=ok?{ok:true,emailStatus:'sent'}:failFirst==='session'?{error:'Inicia sesión en TEC para continuar.'}:{error:'Error de prueba; conserva tus datos.'};}
  else if(url==='/api/chef/admin/chat')data={respuesta:'Pregunta de prueba',appointmentSuggested:true};
  else throw Error('Unexpected URL '+url);
  return {ok,json:async()=>data};
 }};
 vm.createContext(context);
 for(const file of ['chef-appointment-flow.js','tec-chef.js'])vm.runInContext(readFileSync(new URL('../public/chef/'+file,import.meta.url),'utf8'),context);
 await settle();
 const answer=async value=>{get('#question').value=value;await get('#chat-form').emit('submit');await settle();};
 const complete=async()=>{await get('#appointment-start').emit('click');for(const value of ['Persona sintética','7085550100','Hillside, IL','Power Blender Max','Martes por la tarde'])await answer(value);};
 return {get,requests,answer,complete};
}
test('the actual chat controller keeps contact answers out of AI and sends only after explicit review consent',async()=>{
 const f=await fixture();await f.complete();
 assert.equal(f.get('#appointment-review').hidden,false);
 assert.equal(f.get('#send').disabled,true);
 assert.equal(f.get('#appointment-summary').children.length,10);
 assert.equal(f.requests.filter(r=>r.body).length,0);
 await f.get('#appointment-submit').emit('click');assert.equal(f.requests.filter(r=>r.body).length,0);
 assert.match(f.get('#appointment-status').textContent,/autorización/);
 f.get('#appointment-consent').checked=true;await f.get('#appointment-submit').emit('click');
 const sent=f.requests.filter(r=>r.body);assert.equal(sent.length,1);assert.equal(sent[0].url,'/api/chef/admin/leads');assert.equal(sent[0].body.consentGiven,true);assert.equal(sent[0].body.phone,'7085550100');
 assert.equal(f.get('#appointment-review').hidden,true);assert.equal(f.get('#question').disabled,false);assert.match(f.get('#lead-status').textContent,/enviada a Agustín/);
});
test('an uncertain request retains the review and idempotency key for a retry; cancellation sends nothing else',async()=>{
 const f=await fixture({failFirst:true});await f.complete();f.get('#appointment-consent').checked=true;
 await f.get('#appointment-submit').emit('click');assert.equal(f.get('#appointment-review').hidden,false);assert.match(f.get('#appointment-status').textContent,/Error de prueba/);
 await f.get('#appointment-submit').emit('click');
 const posts=f.requests.filter(r=>r.body);assert.equal(posts.length,2);assert.equal(posts[0].body.requestId,posts[1].body.requestId);
 await f.get('#appointment-start').emit('click');await f.answer('Otro contacto sintético');await f.get('#appointment-cancel').emit('click');
 assert.equal(f.get('#appointment-review').hidden,true);assert.equal(f.requests.filter(r=>r.body).length,2);
 await f.answer('Dame una receta con arroz');const last=f.requests.filter(r=>r.body).at(-1);assert.equal(last.url,'/api/chef/admin/chat');assert.equal(last.body.pregunta,'Dame una receta con arroz');assert.ok(!JSON.stringify(last.body).includes('Otro contacto'));
});
test('session expiry while reviewing a contact preserves the draft and never asks for a passkey',async()=>{
 const f=await fixture({failFirst:'session'});
 await f.complete();f.get('#appointment-consent').checked=true;await f.get('#appointment-submit').emit('click');
 assert.equal(f.get('#appointment-review').hidden,false);assert.match(f.get('#appointment-status').textContent,/Inicia sesión/);
 const original=f.requests.find(r=>r.url==='/api/chef/admin/leads'&&r.body);
 await f.get('#appointment-submit').emit('click');const retries=f.requests.filter(r=>r.url==='/api/chef/admin/leads'&&r.body);
 assert.equal(retries.length,2);assert.equal(retries[1].body.requestId,original.body.requestId);assert.equal(retries[1].body.fullName,'Persona sintética');
 const html=readFileSync(new URL('../private/tec-chef.html',import.meta.url),'utf8');assert.doesNotMatch(html,/coach\/seguridad|Verificar llave/);
});
