import test from 'node:test';
import assert from 'node:assert/strict';
import vm from 'node:vm';
import {readFileSync} from 'node:fs';

const source=readFileSync(new URL('../public/coach/demo-close-chat.js',import.meta.url),'utf8');
function fixture({question='¿Qué acordaron?', extracted=true, current='visit-a'}={}) {
  const messages=[],calls=[],listeners={},saved=new Map();
  const close={visitId:'visit-a',crmId:'',version:0,fields:{},confirmed:{}};
  const element=()=>({children:[],append(...items){this.children.push(...items);},addEventListener(type,fn){this[type]=fn;}});
  const env={window:{addEventListener:(k,fn)=>{listeners[k]=fn;}},document:{createElement:element},sessionStorage:{
    getItem:k=>saved.get(k),setItem:(k,v)=>saved.set(k,v)}};
  vm.runInNewContext(source,env);
  async function request(url,options) {
    calls.push({url,options});
    const questions=question?[{field:'outcome',question}]:[];
    if (url.endsWith('/inbox')) return {items:[{id:'rec-a',close,questions}]};
    if (url.endsWith('/clarify')) return {understood:extracted,changed:{outcome:'venta'},close:{...close,version:1},questions:[]};
    return {close,questions};
  }
  const chat=env.window.TecDemoCloseChat.init({accountId:'seller-a',request,addMessage:text=>{
    const card=element();messages.push({text,card});return [{card}];
  },visitId:()=>current,crmId:()=>'',openChat(){},prepareReminder(){throw Error('Cita creada al contestar');}});
  return {calls,messages,chat,listeners};
}
test('al abrir llegan las dudas de la visita; una aclaración solo prepara borrador, nunca llama guardar ni agenda',async()=>{
  const f=fixture();await new Promise(setImmediate);assert.equal(f.chat.hasPending(),true);
  assert.match(f.messages[0].text,/¿Qué acordaron\?/);
  const answer=await f.chat.answer('Sí vendí');
  assert.match(answer.text,/borrador/);assert.equal(f.chat.hasPending(),false);
  assert.deepEqual(f.calls.map(c=>c.url),['/api/coach/demo-close/inbox','/api/coach/demo-analyses/rec-a/close/clarify']);
  assert.equal(f.calls[1].options.body.visitId,'visit-a');
  await f.chat.notify('rec-a');assert.equal(f.messages.length,1);
});
test('una demo anterior no captura la siguiente nota de voz; una pregunta ajena pasa al chat habitual',async()=>{
  const old=fixture({current:'visit-b'});await new Promise(setImmediate);assert.equal(old.chat.hasPending(),false);
  assert.equal(await old.chat.answer('Muestra mi agenda'),null);
  const f=fixture({extracted:false});await new Promise(setImmediate);assert.equal(await f.chat.answer('Muestra mi agenda'),null);
  assert.equal(f.chat.hasPending(),false);
});
test('se puede dejar pendiente y un cierre sin dudas no exige grabar otro resumen',async()=>{
  const f=fixture();await new Promise(setImmediate);assert.match((await f.chat.answer('ahora no')).text,/pendiente/);
  assert.equal(f.calls.length,1);assert.equal(f.chat.hasPending(),false);
  const ready=fixture({question:''});await new Promise(setImmediate);
  assert.equal(ready.chat.hasPending(),false);assert.match(ready.messages[0].text,/revisar/);
});
