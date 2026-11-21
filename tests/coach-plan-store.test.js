import test from 'node:test';
import assert from 'node:assert/strict';
import {randomBytes} from 'node:crypto';
import {createPlanStore} from '../src/coach-plan-store.js';

test('las propuestas se cifran por cuenta y versión y expiran sin perder el control de concurrencia',async()=>{
  const records=new Map();
  let now=0;
  const store={
    findById(id){return {lean:async()=>records.get(id)};},
    async create(doc){if(records.has(doc._id)) throw Object.assign(new Error(),{code:11000});records.set(doc._id,doc);},
    async updateOne({_id,revision},{$set}){const doc=records.get(_id);
      if(doc?.revision!==revision) return {matchedCount:0};
      records.set(_id,{...doc,...$set});return {matchedCount:1};}
  };
  const plans=createPlanStore({key:randomBytes(32),now:()=>now,store});
  const data={note:'Texto de prueba sensible'};
  const r=await plans.save('a',undefined,data);
  assert.match(r,/^[a-f0-9]{24}$/);assert.equal(await plans.save('a',undefined,data),null);
  assert.ok(!JSON.stringify(records.get('a')).includes(data.note));
  records.set('otra-cuenta',records.get('a'));await assert.rejects(plans.load('otra-cuenta'));
  assert.deepEqual((await plans.load('a')).data,data);
  now+=20*60000;assert.deepEqual(await plans.load('a'),{revision:r,data:null});
  assert.equal(await plans.save('a','errónea',{}),null);
  const next=await plans.save('a',r,{nuevo:true});assert.notEqual(next,r);
  assert.deepEqual((await plans.load('a')).data,{nuevo:true});
});
