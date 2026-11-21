import test from 'node:test';
import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import vm from 'node:vm';
import {registerCoachSheetsRoutes} from '../src/coach-sheets-routes.js';

const script = readFileSync(new URL('../public/coach/sheets-transfer.js', import.meta.url), 'utf8');
const base = '/api/coach/conexiones/sheets/';
const contacts = [
  {fullName:'Persona A',phone:'2025550101'}, {fullName:'Persona B',phone:'2025550102'},
  {fullName:'Persona C',phone:'2025550103'}, {fullName:'Persona D',phone:'2025550104'}
];
const sheet = {documentType:'tickets', referrals:contacts};
const sheetTitle = 'BOLETAS NUEVAS';
const changedTitle = '4-14 de prueba';
const preview = (id='first', states=['new','duplicate','possible','new']) => ({
  previewId:id,title:'Equipo ficticio',sheetTitle,rows:contacts.map((c,i) => ({
    ...c,index:i,status:states[i],matches:states[i]==='new' ? [] : [
      {fullName:i===1 ? 'Persona B' : 'Persona X',sheetTitle:changedTitle,row:9,reason:i===1?'name_phone':'phone'}
    ]
  }))
});
const reply = (status,data) => ({status,ok:status>=200 && status<300,json:async()=>data});

class Element extends EventTarget {
  constructor(tag) {
    super();
    this.tag = tag;
    this.children = [];
    this.textContent = '';
    this.attributes = new Map();
  }
  setAttribute(name,value) { this.attributes.set(name,value); }
  append(...nodes) { for (const node of nodes) { node.parent=this; this.children.push(node); } }
  replaceChildren(...nodes) { for (const old of this.children) old.parent=null; this.children=[]; this.append(...nodes); }
  remove() {
    if (this.parent) this.parent.children=this.parent.children.filter(x=>x!==this);
    this.parent=null;
  }
  get isConnected() { return this.tag==='body' || Boolean(this.parent?.isConnected); }
  showModal() { this.open=true; }
  close() { this.open=false; this.dispatchEvent(new Event('close')); }
  descendants() { return [this,...this.children.flatMap(c=>c.descendants())]; }
}

function harness(handle) {
  const window=new EventTarget(), document=new EventTarget(), calls=[];
  document.body=new Element('body');
  document.createElement=tag=>new Element(tag);
  let current=true;
  async function fetch(url,opts) {
    const name=new URL(url,'https://tec.example').pathname.slice(base.length);
    const body=opts.body && JSON.parse(opts.body);
    calls.push({name,body,opts,url});
    if (name==='context') return reply(200,{connected:true,csrf:'csrf-example'});
    return handle(name, body, calls);
  }
  vm.runInNewContext(script,{window,document,fetch,AbortController});
  const elements = () => document.body.descendants();
  return {calls, elements, window, open:()=>window.TecSheetsTransfer.open(sheet,'a',()=>current),
    dialog:()=>elements().find(n=>n.tag==='dialog'), checks:()=>elements().filter(n=>n.tag==='input'),
    byText:t=>elements().find(n=>n.textContent===t),status:()=>elements().find(n=>n.attributes.get('role')==='status')?.textContent,
    confirm:()=>elements().find(n=>n.textContent==='Confirmar guardado en Sheets'),
    revoke() {current=false; window.dispatchEvent(new Event('tec:coach-session-change'));}
  };
}

test('review shows a destination and duplicate sources, pauses before every write and keeps the selection on an ambiguous retry', async()=>{
  let attempts=0;
  const f=harness((op,body)=>{
    if(op==='preview') return reply(200,preview());
    attempts++;
    if(attempts===1) return reply(503,{error:'Prueba de respuesta perdida'});
    return reply(200,{previewId:body.previewId,sheetTitle,addedCount:body.selected.length});
  });
  await f.open();
  assert.deepEqual(f.calls.map(r=>r.name),['context','preview']);
  assert.match(f.calls[0].url,/accountId=a$/);
  assert.deepEqual(f.calls[1].body.referrals,contacts);
  assert.equal(f.calls[1].body.kind,'tickets');
  assert.equal(f.calls[1].body.csrf,'csrf-example');
  assert.equal(f.calls[1].opts.headers['X-TEC-Account-ID'],'a');
  assert.ok(f.byText('Equipo ficticio · '+sheetTitle));
  assert.ok(f.byText('Persona X · '+changedTitle+' · fila 9 · mismo teléfono'));
  const c=f.checks();
  assert.deepEqual(c.map(i=>i.checked),[true,false,false,true]);
  assert.deepEqual(c.map(i=>i.disabled),[false,true,false,false]);
  c[2].checked=true; c[2].onchange(); c[3].checked=false; c[3].onchange();
  const button=f.confirm();
  const first=button.onclick(); await button.onclick(); await first;
  assert.equal(attempts,1);
  assert.ok(c.every(input=>input.disabled));
  assert.equal(button.disabled,false);
  await button.onclick();
  assert.deepEqual(f.calls.filter(r=>r.name==='confirm').map(r=>r.body.selected),[[0,2],[0,2]]);
  assert.equal(f.status(),'Guardados en BOLETAS NUEVAS: 2.');
});

test('a changed duplicate review retains exclusions and asks again about any new collision',async()=>{
  let attempts=0;
  const f=harness((op,body)=>{
    if(op==='preview') return reply(200,preview());
    if(!attempts++) return reply(409,{error:'Cambió la hoja',preview:preview('second',['possible','duplicate','new','new'])});
    return reply(200,{previewId:body.previewId,sheetTitle,addedCount:body.selected.length});
  });
  await f.open();
  f.checks()[3].checked=false;
  await f.confirm().onclick();
  assert.equal(f.status(),'Cambió la hoja');
  assert.deepEqual(f.checks().map(i=>i.checked),[false,false,false,false]);
  assert.equal(f.confirm().disabled,true);
  f.checks()[2].checked=true; f.checks()[2].onchange();
  await f.confirm().onclick();
  assert.equal(f.calls.at(-1).body.previewId,'second'); assert.deepEqual(f.calls.at(-1).body.selected,[2]);
});

test('switching accounts dismisses a draft and a late response cannot replace another review',async()=>{
  let pending;
  const f=harness(async op=>{
    if(op==='preview')return reply(200,preview());
    return new Promise(resolve=>{pending=resolve});
  });
  await f.open();
  const flight=f.confirm().onclick(); const previous=f.dialog();
  await f.open();
  assert.equal(previous.isConnected,false);
  assert.equal(f.calls.find(r=>r.name==='confirm').opts.signal.aborted,true);
  pending(reply(200,{previewId:'first',sheetTitle,addedCount:2}));
  await flight;
  assert.ok(f.confirm());
  const stale=f.confirm();f.revoke();await stale.onclick();
  assert.equal(f.dialog(),undefined);
  assert.equal(f.calls.filter(r=>r.name==='confirm').length,1);
});

test('a stale confirmation can be reviewed afresh and an invalid receipt cannot show success',async()=>{
  let attempts=0;
  const f=harness(op=>op==='preview' ? reply(200,preview()) : (
    attempts++===0 ? reply(200,{}) : reply(409,{error:'La vista previa venció.'})
  ));
  await f.open();await f.confirm().onclick();
  assert.match(f.status(),/comprobante/);assert.equal(f.confirm().disabled,false);
  await f.confirm().onclick();assert.equal(f.checks().length,0);assert.ok(f.byText('Preparar otra vista previa'));
});

test('transfer endpoints bind the account and keep refresh credentials and the server access token out of previews',async()=>{
  const handlers=new Map(), actor={id:'seller-a',session:'session-a',sellerName:'Persona A',isSessionCurrent:async()=>true};
  const app={get:(p,h)=>handlers.set(p,h),post:(p,h)=>handlers.set(p,h)};
  let destinationCalls=0, transferCalls=0,ctx;
  const sheets={
    getConnection:async()=>({spreadsheetId:'book',grant:'private-grant'}),
    access:async()=>{destinationCalls++;return {spreadsheetId:'book',token:'access-example',grant:'private-grant'};},
    current:async(owner,grant)=>owner===actor.id && grant==='private-grant'
  };
  registerCoachSheetsRoutes(app,{own:h=>h,sheets,transfer:{
    preview:async(c,b)=>{ctx=c;transferCalls++;return preview();},
    confirm:async()=>{throw Object.assign(new Error('Hoja modificada'),{status:409,preview:preview('updated')});}
  }});
  async function call(op, accountId,header=accountId) {
    const req={method:op==='context'?'GET':'POST',query:{accountId},body:{accountId},get:()=>header};
    const res={code:200,status(s){this.code=s;return this},json(data){this.data=data;return this}};
    await handlers.get(base+op)(req,res,actor);
    return res;
  }
  assert.equal((await call('context','seller-b')).code,403);
  assert.deepEqual((await call('context',actor.id)).data,{connected:true,csrf:actor.session});
  assert.equal((await call('preview',actor.id,'wrong-header')).code,403);
  assert.equal((await call('confirm','seller-b')).code,403);
  assert.equal(destinationCalls,0);
  const result=await call('preview',actor.id);
  assert.equal(transferCalls,1);assert.equal(ctx.access,'access-example');assert.equal(ctx.owner,actor.id);
  assert.deepEqual(ctx.workbook,{spreadsheetId:'book'});
  assert.equal(await ctx.isCurrent(),true);
  actor.isSessionCurrent=async()=>false;assert.equal(await ctx.isCurrent(),false);
  assert.doesNotMatch(JSON.stringify(result.data),/access-example|private-grant/);
  actor.isSessionCurrent=async()=>true;
  const changed=await call('confirm',actor.id);assert.equal(changed.code,409);assert.equal(changed.data.preview.previewId,'updated');
});
