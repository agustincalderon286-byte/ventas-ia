import test from 'node:test';
import assert from 'node:assert/strict';
import {randomBytes} from 'node:crypto';
import {createFollowupStore} from '../src/coach-followup-store.js';

function database() {
  const rows=[];
  const get=(row,key)=>key.split('.').reduce((o,k)=>o?.[k],row);
  function matches(row,q) {
    return Object.entries(q).every(([k,v])=>{
      if (k==='$or') return v.some(a=>matches(row,a));
      const actual=get(row,k);
      if (v?.$in) return v.$in.includes(actual);
      if (v?.$gt) return actual>v.$gt;
      if (v?.$gte) return actual>=v.$gte;
      if (v?.$lte) return actual<=v.$lte;
      if (v && Object.hasOwn(v,'$exists')) return (actual!==undefined)===v.$exists;
      return Array.isArray(actual)?actual.includes(v):actual===v;
    });
  }
  function update(q,changes) {
    const row=rows.find(d=>matches(d,q));
    if (!row) return null;
    const old=structuredClone(row);
    for (const [op,cols] of Object.entries(changes)) for(const [key,value] of Object.entries(cols)) {
      const keys=key.split('.');
      const target=keys.slice(0,-1).reduce((o,k)=>o[k]??={},row), name=keys.at(-1);
      if(op==='$set') target[name]=structuredClone(value);
      if(op==='$unset') delete target[name];
      if(op==='$inc') target[name]+=value;
    }
    return old;
  }
  const result=value=>({lean:async()=>structuredClone(value)});
  return {rows,create:async d=>rows.push(structuredClone(d)),findOne:q=>result(rows.find(d=>matches(d,q))),
    find:q=>({sort:()=>({limit:n=>result(rows.filter(d=>matches(d,q)).sort((a,b)=>a.startsAt-b.startsAt).slice(0,n))})}),
    updateOne:async (q,updateDoc)=>({matchedCount:update(q,updateDoc)?1:0}),
    findOneAndUpdate:(q,updateDoc)=>result(update(q,updateDoc))};
}

const input={summary:'Llamada · María',name:'María',start:'2027-01-09T00:00:00.000Z',end:'2027-01-09T00:30:00.000Z',
  timeZone:'America/Chicago',action:'llamar',mode:'create',leadId:'lead',expectedStart:null};
const binding={owner:'seller',calendarId:'calendar',grant:'grant',session:'session'};
const create=()=>{const raw=database(); let now=Date.UTC(2026,10,21);
  return {raw,repo:createFollowupStore({store:raw,key:randomBytes(32),now:()=>now}),advance:ms=>{now+=ms;}};};

test('las vistas previas están cifradas y ligadas a una cuenta, calendario, sesión y consentimiento',async()=>{
  const {raw,repo,advance}=create();
  const {id}=await repo.prepare(binding,input);
  assert.doesNotMatch(JSON.stringify(raw.rows),/María|Llamada/);
  for (const foreign of [{owner:'other'},{calendarId:'other'},{session:'new'},{grant:'new'}]) {
    const b={...binding,...foreign};
    assert.equal((await repo.confirm(b,id,{})).status,'unavailable');
    assert.equal(await repo.discard(b,id),'unavailable');
  }
  advance(31*60000);
  assert.equal((await repo.confirm(binding,id,{})).status,'unavailable');
});

test('dos confirmaciones crean un único evento y reprogramar conserva el id de Google',async()=>{
  const {repo}=create();
  const {id}=await repo.prepare(binding,input);
  let release,entered;
  const started=new Promise(r=>{entered=r;});
  const wait=new Promise(r=>{release=r;});
  const writes=[]; const crm=[];
  const api={saveEvent:async data=>{writes.push(data);entered();await wait;return {id:data.eventId,htmlLink:'https://calendar.google.com/calendar/event?eid=x'};},
    syncLead:async data=>{crm.push(data);return true;}};
  const first=repo.confirm(binding,id,api);
  await started;
  assert.equal((await repo.confirm(binding,id,api)).status,'working');
  release();
  const receipt=await first;
  assert.equal(receipt.status,'saved'); assert.equal(receipt.crm,'updated');
  assert.deepEqual(await repo.confirm(binding,id,api),receipt); assert.equal(writes.length,1);
  const [existing]=await repo.search(binding,{name:'MARIA'});
  assert.equal(existing.reference,receipt.reference);
  assert.equal((await repo.search({...binding,owner:'other'},{name:'MARIA'})).length,0);
  const proposal={...input,mode:'move',start:'2027-01-09T02:00:00.000Z',end:'2027-01-09T02:30:00.000Z',
    expectedStart:input.start,previousOperationId:existing.operationId};
  const move=await repo.prepare(binding,proposal,existing);
  const newest=await repo.prepare(binding,proposal,existing);
  assert.equal((await repo.confirm(binding,move.id,api)).status,'unavailable');
  assert.equal((await repo.confirm(binding,newest.id,api)).status,'saved');
  assert.equal(writes[0].eventId,writes[1].eventId);
  assert.equal(writes[1].previousOperationId,id.toLowerCase());
  assert.equal(crm[1].expectedStart,input.start);
  assert.equal(await repo.prepare(binding,proposal,existing),null);
});

test('si el CRM falla después de Google, la misma confirmación solo reintenta el CRM',async()=>{
  const {repo}=create();const {id}=await repo.prepare(binding,input);let google=0,crm=0;
  const api={saveEvent:async data=>{google++;return {id:data.eventId};},
    syncLead:async()=>{if (!crm++) throw new Error('CRM caído'); return false;}};
  assert.equal((await repo.confirm(binding,id,api)).status,'crmPending');
  const result=await repo.confirm(binding,id,api);
  assert.equal(result.crm,'conflict');assert.equal(google,1);assert.equal(crm,2);
});

test('Google puede reconciliar respuestas perdidas usando el mismo eventId y operationId',async()=>{
  const {repo}=create();const {id}=await repo.prepare(binding,input);
  const calls=[];
  const api={saveEvent:async data=>{calls.push(data);if(calls.length===1) throw new Error('timeout');return {id:data.eventId};},syncLead:async()=>true};
  assert.equal((await repo.confirm(binding,id,api)).status,'retry');
  assert.equal((await repo.confirm(binding,id,api)).status,'saved');
  assert.deepEqual(calls[0],calls[1]);
});
