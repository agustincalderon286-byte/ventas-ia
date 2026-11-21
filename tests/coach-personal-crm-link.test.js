import assert from 'node:assert/strict';
import test from 'node:test';
import { personalCalendarCrm } from '../src/coach-personal-crm-link.js';

const leadId='00000000000000000000000a';
function fixture(options={}) {
  const calls={};
  const chain=value=>({select(){return this;},sort(){return this;},limit(){return this;},lean:async()=>value});
  const crm=personalCalendarCrm({
    users:{findById(id){return chain({_id:id});}},
    viewerQuery:async()=>({ownerUserId:options.team || 'seller'}),
    records:{
      find(filter){calls.search=filter;return chain([{_id:leadId,leadName:'María Cruz',phone:'+13125550123',nextActionAt:null}]);},
      findOneAndUpdate(filter,update){calls.filter=filter;calls.update=update;return chain(options.conflict ? null : {ownerUserId:'seller'});}
    },
    applySources:async(...args)=>{calls.sources=args;}, invalidate:async(...args)=>{calls.invalidate=args;}
  });
  return {crm,calls};
}
test('el CRM busca nombres sin acentos y conserva la pertenencia del vendedor en equipos',async()=>{
  const {crm,calls}=fixture({team:'equipo'});
  assert.deepEqual(await crm.findLeads({owner:'seller',name:'maria'}),[{id:leadId,name:'María Cruz',nextActionAt:null}]);
  assert.ok(calls.search.leadName.test('María Cruz'));
  assert.ok(!calls.search.leadName.test('Mariangel Cruz'));
  assert.equal(calls.search.ownerUserId,'equipo');
  assert.ok(calls.search.$or.every(role=>Object.values(role)[0]==='seller'));
  assert.deepEqual(await crm.findLeads({owner:'seller',name:'.*'}),[]);
  assert.equal((await crm.findLeads({owner:'seller',id:leadId,includePhone:true}))[0].phone,'+13125550123');
  assert.equal(calls.search.ownerUserId,'equipo');
  assert.ok(calls.search.$or.every(role=>Object.values(role)[0]==='seller'));
});
test('la actualización de un prospecto respeta un cambio posterior de la fecha y propaga la cita aplicada',async()=>{
  const detail={owner:'seller',leadId,start:'2026-12-01T21:30:00Z',action:'llamar',expectedStart:null};
  const {crm,calls}=fixture();
  assert.equal(await crm.syncLead(detail),true);
  assert.equal(calls.filter.ownerUserId,'seller');
  assert.deepEqual(calls.filter.nextActionAt.$in,[null,new Date(detail.start)]);
  assert.equal(calls.sources[1].nextAction,'llamar');
  assert.deepEqual(calls.invalidate,[{_id:'seller'},'seller']);
  const changed=fixture({conflict:true});
  assert.equal(await changed.crm.syncLead(detail),false);
  assert.equal(changed.calls.sources,undefined);
});

test('el hilo identifica por dirección completa en el CRM accesible al vendedor',async()=>{
  const {crm,calls}=fixture({team:'equipo'});
  await crm.findLeads({owner:'seller',email:'Ana.M+demo@example.test'});
  assert.ok(calls.search.email.test('ana.m+demo@example.test'));
  assert.ok(!calls.search.email.test('Otro.ana.m+demo@example.test'));
  assert.ok(!calls.search.email.test('AnaXMdemo@example.test'));
  assert.equal(calls.search.ownerUserId,'equipo');
  assert.ok(calls.search.$or.every(role=>Object.values(role)[0]==='seller'));
  await crm.findLeads({owner:'seller',id:leadId,email:'ana@example.test'});
  assert.equal(calls.search._id,leadId);
  assert.ok(calls.search.email.test('ana@example.test'));
  assert.deepEqual(await crm.findLeads({owner:'seller',email:'',name:'María'}),[]);
});
