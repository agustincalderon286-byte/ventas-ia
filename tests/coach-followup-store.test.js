import test from 'node:test';
import assert from 'node:assert/strict';
import {randomBytes} from 'node:crypto';
import {createFollowupStore,followupLimits,purgeCalendarProposals} from '../src/coach-followup-store.js';

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
  const many=(q,updateDoc)=>{
    const hits=rows.filter(d=>matches(d,q));
    for (const row of hits) update({_id:row._id},updateDoc);
    return {matchedCount:hits.length,modifiedCount:hits.length};
  };
  return {rows,create:async d=>{
    if (rows.some(row=>row._id===d._id)) throw Object.assign(new Error('duplicate'),{code:11000});
    rows.push(structuredClone(d));
  },findOne:q=>result(rows.find(d=>matches(d,q))),
    find:q=>({sort:()=>({limit:n=>result(rows.filter(d=>matches(d,q)).sort((a,b)=>a.startsAt-b.startsAt).slice(0,n))})}),
    updateOne:async (q,updateDoc)=>({matchedCount:update(q,updateDoc)?1:0}),
    updateMany:async (q,updateDoc)=>many(q,updateDoc),
    deleteMany:async q=>{const keep=rows.filter(d=>!matches(d,q));const n=rows.length-keep.length;rows.splice(0,rows.length,...keep);return {deletedCount:n};},
    findOneAndUpdate:(q,updateDoc)=>result(update(q,updateDoc))};
}

const input={summary:'Llamada · María',name:'María',start:'2027-01-09T00:00:00.000Z',end:'2027-01-09T00:30:00.000Z',
  timeZone:'America/Chicago',action:'llamar',mode:'create',leadId:'lead',expectedStart:null};
const binding={owner:'seller',calendarId:'calendar',grant:'grant',session:'session'};
const create=()=>{const raw=database(), key=randomBytes(32); let now=Date.UTC(2026,10,21);
  return {raw,key,clock:()=>now,repo:createFollowupStore({store:raw,key,now:()=>now}),advance:ms=>{now+=ms;}};};

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

test('una búsqueda solo permite confirmar la última opción y conserva su identidad al recuperar una respuesta perdida',async()=>{
  const {repo}=create();
  const options={...input,checkAvailability:true,availabilitySearchId:'f'.repeat(32)};
  const first=await repo.prepare(binding,options);
  const second=await repo.prepare(binding,{...options,start:'2027-01-09T02:00:00.000Z',end:'2027-01-09T02:30:00.000Z'});
  assert.equal((await repo.confirm(binding,first.id,{})).status,'unavailable');
  const calls=[];
  const api={saveEvent:async event=>{
    calls.push(event);
    if (calls.length===1) throw new Error('response lost');
    return {id:event.eventId};
  },syncLead:async()=>true};
  assert.equal((await repo.confirm(binding,second.id,api)).status,'retry');
  assert.equal(await repo.prepare(binding,options),null);
  assert.equal((await repo.confirm(binding,second.id,api)).status,'saved');
  assert.deepEqual(calls.map(event=>event.retryGoogle),[false,true]);
  assert.equal(calls[0].eventId,calls[1].eventId);
  assert.equal(await repo.prepare(binding,options),null);
});

test('un horario que se ocupó vence la propuesta sin registrar un seguimiento en TEC',async()=>{
  const {repo}=create();
  const {id}=await repo.prepare(binding,{...input,checkAvailability:true,availabilitySearchId:'a'.repeat(32)});
  const api={saveEvent:async()=>{throw Object.assign(new Error('ocupado'),{slotBusy:true,status:409});},
    syncLead:async()=>assert.fail('no se debe cambiar el prospecto')};
  assert.equal((await repo.confirm(binding,id,api)).status,'slotBusy');
  assert.equal((await repo.confirm(binding,id,api)).status,'unavailable');
  assert.deepEqual(await repo.search(binding),[]);
});

const {googleMs,crmMs}=followupLimits;

test('tras el plazo de escritura un reintento solo lee Google y caduca sin insertar ni tocar el CRM',async()=>{
  const {raw,repo,advance}=create();
  const {id}=await repo.prepare(binding,input);
  const calls=[];
  const api={saveEvent:async data=>{calls.push(data);if(calls.length===1) throw new Error('timeout');return null;},
    syncLead:async()=>assert.fail('no se debe cambiar el prospecto')};
  assert.equal((await repo.confirm(binding,id,api)).status,'retry');
  advance(googleMs);
  assert.deepEqual(await repo.confirm(binding,id,api),{status:'expired'});
  assert.equal(calls[0].reconcileOnly,undefined);
  assert.equal(calls[1].reconcileOnly,true);
  assert.equal(calls[1].eventId,calls[0].eventId);assert.equal(calls[1].operationId,calls[0].operationId);
  assert.deepEqual(await repo.confirm(binding,id,api),{status:'expired'});
  assert.equal(calls.length,2);
  assert.equal((await repo.confirm({...binding,session:'other'},id,api)).status,'unavailable');
  assert.deepEqual(await repo.search(binding),[]);
  assert.equal(raw.rows[0].pending,undefined);assert.ok(raw.rows[0].removeAfter instanceof Date);
});

test('tras el plazo reconcilia un evento que Google sí creó y completa registro y CRM una sola vez',async()=>{
  const {repo,advance}=create();
  const {id}=await repo.prepare(binding,input);
  const calls=[],crm=[];
  const api={saveEvent:async data=>{calls.push(data);if(calls.length===1) throw new Error('timeout');return {id:data.eventId,etag:'v1',htmlLink:''};},
    syncLead:async data=>{crm.push(data);return true;}};
  assert.equal((await repo.confirm(binding,id,api)).status,'retry');
  advance(googleMs+1);
  const receipt=await repo.confirm(binding,id,api);
  assert.equal(receipt.status,'saved');assert.equal(receipt.crm,'updated');
  assert.equal(calls[1].reconcileOnly,true);
  const [event]=await repo.search(binding,{name:'María'});
  assert.equal(event.reference,receipt.reference);assert.equal(event.etag,'v1');
  assert.deepEqual(await repo.confirm(binding,id,api),receipt);
  assert.equal(calls.length,2);assert.equal(crm.length,1);
});

test('el CRM pendiente caduca: conserva la cita registrada y no actualiza la ficha fuera de plazo',async()=>{
  const {repo,advance}=create();const {id}=await repo.prepare(binding,input);let google=0,crm=0;
  const api={saveEvent:async data=>{google++;return {id:data.eventId};},syncLead:async()=>{crm++;throw new Error('CRM caído');}};
  assert.equal((await repo.confirm(binding,id,api)).status,'crmPending');
  advance(crmMs);
  const receipt=await repo.confirm(binding,id,api);
  assert.equal(receipt.status,'saved');assert.equal(receipt.crm,'expired');
  assert.equal(google,1);assert.equal(crm,1);
  assert.equal((await repo.search(binding)).length,1);
  assert.deepEqual(await repo.confirm(binding,id,api),receipt);assert.equal(crm,1);
});

test('mover una cita interrumpida caduca sin borrar el registro anterior y admite otra propuesta',async()=>{
  const {raw,repo,advance}=create();
  const {id}=await repo.prepare(binding,input);
  let fail=false;const calls=[];
  const api={saveEvent:async data=>{calls.push(data);if(data.reconcileOnly) return null;if(fail) throw new Error('timeout');return {id:data.eventId,etag:'e'+calls.length};},
    syncLead:async()=>true};
  assert.equal((await repo.confirm(binding,id,api)).status,'saved');
  const [existing]=await repo.search(binding,{name:'María'});
  const proposal={...input,mode:'move',start:'2027-01-09T02:00:00.000Z',end:'2027-01-09T02:30:00.000Z',
    expectedStart:input.start,previousOperationId:existing.operationId,previousEtag:existing.etag};
  const move=await repo.prepare(binding,proposal,existing);
  fail=true;
  assert.equal((await repo.confirm(binding,move.id,api)).status,'retry');
  advance(googleMs);
  assert.deepEqual(await repo.confirm(binding,move.id,api),{status:'expired'});
  assert.equal(calls.at(-1).reconcileOnly,true);
  assert.equal(calls.filter(c=>!c.reconcileOnly).length,2);
  const [kept]=await repo.search(binding,{name:'María'});
  assert.equal(kept.start,input.start);assert.equal(kept.version,existing.version);assert.equal(kept.reference,existing.reference);
  assert.equal(raw.rows.length,1);assert.equal(raw.rows[0].removeAfter,undefined);
  assert.ok(await repo.prepare(binding,proposal,existing));
});

test('una escritura en vuelo bloquea otros intentos y, si responde tras caducar, se registra una sola vez',async()=>{
  const {repo,advance}=create();
  const {id}=await repo.prepare(binding,input);
  const inGoogle=Promise.withResolvers(), answer=Promise.withResolvers(), writes=[], reads=[];
  const api={saveEvent:async data=>{
    if (data.reconcileOnly) {reads.push(data);return null;}
    writes.push(data);inGoogle.resolve();await answer.promise;return {id:data.eventId,etag:'late'};
  },syncLead:async()=>assert.fail('fuera de plazo no se actualiza el prospecto')};
  const first=repo.confirm(binding,id,api);
  await inGoogle.promise;
  assert.equal((await repo.confirm(binding,id,api)).status,'working');
  advance(googleMs);
  assert.deepEqual(await repo.confirm(binding,id,api),{status:'expired'});
  answer.resolve();
  const late=await first;
  assert.equal(late.status,'saved');assert.equal(late.crm,'expired');
  assert.deepEqual(await repo.confirm(binding,id,api),late);
  assert.equal(writes.length,1);assert.equal(reads.length,1);
  assert.equal((await repo.search(binding,{name:'María'})).length,1);
  assert.equal((await repo.confirm({...binding,session:'other'},id,api)).status,'unavailable');
});

test('datos anteriores sin confirmedAt ni TTL se limitan desde su preparación',async()=>{
  const {raw,repo,advance,clock}=create();
  const {id}=await repo.prepare(binding,input);
  advance(5*60000);
  const row=raw.rows[0];
  Object.assign(row.pending,{phase:'google',lease:'old',leaseUntil:new Date(clock())});
  delete row.removeAfter;
  assert.equal((await repo.expireInterrupted()).legacy,1);
  assert.ok(row.removeAfter>new Date(clock()));
  advance(googleMs-5*60000);
  const calls=[];
  const api={saveEvent:async data=>{calls.push(data);return null;},syncLead:async()=>assert.fail('sin CRM')};
  assert.deepEqual(await repo.confirm(binding,id,api),{status:'expired'});
  assert.equal(calls.length,1);assert.equal(calls[0].reconcileOnly,true);
});

test('una reserva en proceso desde otra sesión da un aviso propio y la propuesta sigue confirmable',async()=>{
  const {raw,repo}=create();
  const other={...binding,session:'tab-2'};
  const options={...input,checkAvailability:true,availabilitySearchId:'b'.repeat(32)};
  const a=await repo.prepare(binding,options);
  const b=await repo.prepare(other,{...options,availabilitySearchId:'c'.repeat(32)});
  const inGoogle=Promise.withResolvers(), finish=Promise.withResolvers(), taken=new Set(), calls=[];
  let holder=null;
  const api={saveEvent:async data=>{
    calls.push(data);
    if (holder) throw Object.assign(new Error('Hay otra cita en proceso de confirmación.'),{status:503,bookingBusy:true});
    holder=data.eventId;
    try {
      if (calls.length===1) {inGoogle.resolve();await finish.promise;}
      if (taken.has(data.start)) throw Object.assign(new Error('ocupado'),{status:409,slotBusy:true});
      taken.add(data.start);return {id:data.eventId};
    } finally { holder=null; }
  },syncLead:async()=>true};
  const first=repo.confirm(binding,a.id,api);
  await inGoogle.promise;
  assert.deepEqual(await repo.confirm(other,b.id,api),{status:'bookingBusy'});
  const row=raw.rows.find(r=>r.pending?.id===b.id);
  assert.equal(row.pending.phase,'preview');assert.equal(row.pending.lease,undefined);assert.equal(row.pending.confirmedAt,undefined);
  assert.equal(row.removeAfter.getTime(),row.pending.deadline.getTime());
  finish.resolve();assert.equal((await first).status,'saved');
  assert.equal((await repo.confirm(other,a.id,api)).status,'unavailable');
  assert.equal((await repo.confirm(other,b.id,api)).status,'slotBusy');
  assert.deepEqual(calls.map(c=>c.retryGoogle),[false,false,false]);
});

test('al desconectar se acotan las confirmaciones interrumpidas sin borrar citas registradas',async()=>{
  const {raw,repo,key,clock}=create();
  await repo.prepare(binding,input);
  const stuck=await repo.prepare(binding,{...input,start:'2027-01-10T00:00:00.000Z',end:'2027-01-10T00:30:00.000Z'});
  const recorded=await repo.prepare(binding,{...input,start:'2027-01-11T00:00:00.000Z',end:'2027-01-11T00:30:00.000Z'});
  assert.equal((await repo.confirm(binding,stuck.id,{saveEvent:async()=>{throw new Error('timeout');}})).status,'retry');
  assert.equal((await repo.confirm(binding,recorded.id,{saveEvent:async d=>({id:d.eventId}),
    syncLead:async()=>{throw new Error('CRM caído');}})).status,'crmPending');
  const byId=id=>raw.rows.find(r=>r.pending?.id===id);
  const stuckRow=byId(stuck.id), recordedRow=byId(recorded.id);
  delete stuckRow.removeAfter;
  await purgeCalendarProposals({owner:'seller',grant:'grant'},key,{store:raw,now:clock});
  assert.equal(raw.rows.length,2);
  assert.ok(stuckRow.removeAfter instanceof Date);assert.equal(stuckRow.pending.phase,'google');
  assert.equal(recordedRow.pending,undefined);assert.ok(recordedRow.current);
  assert.equal((await repo.search(binding)).length,1);
});

test('el mantenimiento retira confirmaciones de CRM abandonadas conservando el registro',async()=>{
  const {raw,repo,advance}=create();const {id}=await repo.prepare(binding,input);
  await repo.confirm(binding,id,{saveEvent:async d=>({id:d.eventId}),syncLead:async()=>{throw new Error('CRM caído');}});
  assert.equal((await repo.expireInterrupted()).crm,0);
  advance(crmMs);
  assert.equal((await repo.expireInterrupted()).crm,1);
  assert.equal(raw.rows[0].pending,undefined);
  assert.equal((await repo.search(binding)).length,1);
});

test('una demo por Gmail queda en la agenda del vendedor y una relectura no revierte una cita movida',async()=>{
  const {repo}=create();
  const event={...input,eventId:'abcdef0123456789abcdef0123456789',operationId:'offer-test',etag:'orig'};
  const reference=await repo.rememberEvent(binding,event);
  assert.equal(reference,'ABCDEF012345');
  const [first]=await repo.search(binding,{reference});
  assert.equal(first.operationId,'offer-test');
  assert.deepEqual(await repo.search({...binding,owner:'otro'},{reference}),[]);
  const moved={...event,mode:'move',start:'2027-01-10T00:00:00Z',end:'2027-01-10T00:45:00Z'};
  const preview=await repo.prepare(binding,moved,first);
  const saved=await repo.confirm(binding,preview.id,{saveEvent:async()=>({id:event.eventId,etag:'v2'}),syncLead:async()=>true});
  assert.equal(saved.status,'saved');
  await repo.rememberEvent(binding,event);
  const [latest]=await repo.search(binding,{reference});
  assert.equal(latest.start,moved.start);
  assert.equal(latest.etag,'v2');
});
