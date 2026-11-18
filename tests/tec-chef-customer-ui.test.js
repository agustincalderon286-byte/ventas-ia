import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import vm from 'node:vm';
import {webcrypto} from 'node:crypto';
const code=fs.readFileSync(new URL('../public/chef/customer.js',import.meta.url),'utf8');
class Element{
  constructor(){this.children=[];this.events=new Map();this.dataset={};this.attrs={};this.value='';this.checked=false;this.textContent='';this.open=false;this.hidden=false;}
  append(...nodes){this.children.push(...nodes);}
  replaceChildren(...nodes){this.children=nodes;}
  addEventListener(type,fn){this.events.set(type,fn);}
  async fire(type,extra={}){return this.events.get(type)?.({currentTarget:this,preventDefault(){},...extra});}
  focus(){}
  showModal(){this.open=true;}
  close(){this.open=false;this.fire('close');}
  querySelectorAll(){return Object.values(this.elements||{});}
  reportValidity(){return true;}
  reset(){for(const e of Object.values(this.elements||{})){e.value='';e.checked=false;}}
}
const settle=async()=>{for(let i=0;i<10;i++)await new Promise(resolve=>setImmediate(resolve));};
async function fixture({storage=new Map(),storageBlocked=false,failFirst=false,search=""}={}){
  const nodes=new Map(),requests=[];let attempts=0;
  const get=selector=>{if(!nodes.has(selector))nodes.set(selector,new Element());return nodes.get(selector);};
  const form=get('#contact-form');form.elements={};for(const key of ['fullName','contact','city','interest','notes','appointmentRequested','appointmentPreferredTime','consentGiven'])form.elements[key]=new Element();nodes.set('#request-demo',form.elements.appointmentRequested);
  class Fields{constructor(form){this.data=new Map(Object.entries(form.elements).filter(([name,node])=>!['appointmentRequested','consentGiven'].includes(name)||node.checked).map(([name,node])=>[name,['appointmentRequested','consentGiven'].includes(name)?'on':node.value]));}get(k){return this.data.get(k)||'';}has(k){return this.data.has(k);}}
  const sessionStorage={getItem:key=>{if(storageBlocked)throw Error('blocked');return storage.get(key)||null;},setItem:(key,value)=>{if(storageBlocked)throw Error('blocked');storage.set(key,value);},removeItem:key=>{if(storageBlocked)throw Error('blocked');storage.delete(key);}};
  const context={document:{querySelector:get,querySelectorAll:()=>[],createElement:()=>new Element()},crypto:webcrypto,TextEncoder,Uint8Array,FormData:Fields,sessionStorage,navigator:{onLine:true},location:{pathname:'/chef/eric/',search},URL,URLSearchParams,AbortSignal,fetch:async(url,options)=>{
    const body=options.body?JSON.parse(options.body):undefined;requests.push({url,body});let data={},ok=true;
    if(url.includes('/bootstrap'))data={sellerName:'Eric Bautista',contactAvailable:true};
    else if(url.includes('/conversation'))data={messages:[]};
    else if(url.includes('/chat'))data={respuesta:'Una receta de prueba',appointmentSuggested:false};
    else if(url.includes('/leads')){attempts++;ok=!(failFirst&&attempts===1);data=ok?{ok:true,sellerName:'Eric Bautista'}:{error:'Conexión interrumpida. Reintenta.'};}
    else throw Error('Unexpected request');return{ok,json:async()=>data};
  }};
  vm.runInNewContext(code,context);await settle();
  const fill=()=>{Object.assign(form.elements.fullName,{value:'Cliente sintético'});form.elements.contact.value='7085550100';form.elements.city.value='Hillside, IL';form.elements.interest.value='Power Blender Max';form.elements.consentGiven.checked=true;};
  return{get,requests,storage,form,fill};
}
test('the customer sees only the public Chef API; contact fields are never sent to the AI',async()=>{
  const f=await fixture();assert.ok(f.requests.every(r=>r.url.startsWith('/api/chef/public/')));assert.equal(f.get('#recipient-name').textContent,'Eric Bautista');
  await f.get('#request-information').fire('click');assert.equal(f.get('#contact-dialog').open,true);f.fill();assert.equal(f.requests.filter(r=>r.body).length,0);
  await f.form.fire('submit');await settle();const sent=f.requests.filter(r=>r.body);assert.equal(sent.length,1);assert.match(sent[0].url,/public\/leads\?chef=eric/);assert.equal(sent[0].body.fullName,'Cliente sintético');assert.equal(sent[0].body.consentGiven,true);assert.equal(f.get('#contact-dialog').open,false);assert.equal(f.form.elements.fullName.value,'');
});
test('consent is required even if form submission is invoked directly; closing a draft sends nothing',async()=>{
  const f=await fixture();f.fill();f.form.elements.consentGiven.checked=false;await f.form.fire('submit');await settle();assert.equal(f.requests.filter(r=>r.body).length,0);assert.match(f.get('#contact-status').textContent,/autorización/);await f.get('#request-information').fire('click');await f.get('#contact-close').fire('click');assert.equal(f.requests.filter(r=>r.body).length,0);
});
test('an uncertain send keeps the data and retries the same key, including when browser storage is blocked',async()=>{
  for(const storageBlocked of [false,true]){const f=await fixture({failFirst:true,storageBlocked});f.fill();await f.form.fire('submit');await settle();assert.equal(f.form.elements.fullName.value,'Cliente sintético');assert.match(f.get('#contact-status').textContent,/interrumpida/);await f.form.fire('submit');await settle();const sent=f.requests.filter(r=>r.body);assert.equal(sent.length,2);assert.equal(sent[0].body.requestId,sent[1].body.requestId);assert.equal(f.get('#contact-submit').disabled,false);}
});
test('reload can recover a failed request key without storing personal contact fields',async()=>{
  const store=new Map(),f=await fixture({failFirst:true,storage:store});f.fill();await f.form.fire('submit');await settle();const first=f.requests.find(r=>r.body);const raw=JSON.stringify([...store]);assert.equal(raw.includes('Cliente sintético'),false);assert.equal(raw.includes('7085550100'),false);
  const g=await fixture({storage:store});g.fill();await g.form.fire('submit');await settle();assert.equal(g.requests.find(r=>r.body).body.requestId,first.body.requestId);
});
test('a normal recipe question does not create or transmit a lead',async()=>{
  const f=await fixture();f.get('#question').value='Una receta con arroz';await f.get('#chat-form').fire('submit');await settle();const posts=f.requests.filter(r=>r.body);assert.equal(posts.length,1);assert.match(posts[0].url,/public\/chat/);assert.equal(posts[0].body.pregunta,'Una receta con arroz');assert.equal(posts[0].body.fullName,undefined);
});

test('Alexa opens the selected seller appointment without posting until contact is authorized',async()=>{
  const f=await fixture({search:'?origen=alexa'});
  assert.equal(f.get('#contact-dialog').open,true);
  assert.equal(f.get('#request-demo').checked,true);
  assert.equal(f.get('#recipient-name').textContent,'Eric Bautista');
  assert.equal(f.requests.filter(r=>r.body).length,0);
  f.fill();
  f.get('#request-demo').checked=true;
  f.form.elements.appointmentPreferredTime.value='Viernes por la tarde';
  f.form.elements.consentGiven.checked=false;
  await f.form.fire('submit');
  assert.equal(f.requests.filter(r=>r.body).length,0);
  f.form.elements.consentGiven.checked=true;
  await f.form.fire('submit');
  await settle();
  const sent=f.requests.find(r=>r.body);
  assert.equal(sent.url,'/api/chef/public/leads?chef=eric');
  assert.equal(sent.body.origin,'alexa');
  assert.equal(sent.body.appointmentRequested,true);
});
