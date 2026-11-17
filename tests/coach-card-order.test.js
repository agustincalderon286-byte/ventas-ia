import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import vm from 'node:vm';
import express from 'express';
import { COACH_FLOW_CARD_IDS, readCoachCardOrder, registerCoachCardOrder } from '../src/coach-card-order.js';
import { createCoachRequestGuard } from '../src/tec-security.js';
const html=fs.readFileSync(new URL('../private/coach-app.html',import.meta.url),'utf8');
const source=fs.readFileSync(new URL('../public/coach/card-order.js',import.meta.url),'utf8');
const context={window:{}};vm.runInNewContext(source,context);
const reconcile=(a,b)=>Array.from(context.window.TecCardOrder.reconcile(a,b));

test('the standard list matches all fourteen Cierre cards and every current tool has a stable unique ID',()=>{
 const cards=[...html.matchAll(/<article\b[^>]*data-coach-card-id="([^"]+)"[^>]*>/g)].map(m=>({id:m[1],order:Number(m[0].match(/--coach-flow-order:\s*(\d+)/)[1])})).sort((a,b)=>a.order-b.order);
 assert.deepEqual(cards.map(c=>c.id),COACH_FLOW_CARD_IDS);assert.equal(new Set(cards.map(c=>c.id)).size,14);
 assert.ok(cards.some(c=>c.id==='prueba-agua'));assert.ok(cards.some(c=>c.id==='resultado-visita'));
});
test('saved order retains valid cards, appends new cards and drops removed or duplicate IDs without mutating the standard order',()=>{
 const defaults=['a','b','new'];const saved=['b','retired','b','a'];
 assert.deepEqual(reconcile(saved,defaults),['b','a','new']);assert.deepEqual(defaults,['a','b','new']);assert.deepEqual(saved,['b','retired','b','a']);
 assert.deepEqual(reconcile([],defaults),defaults);assert.deepEqual(reconcile(null,defaults),defaults);
 assert.deepEqual(readCoachCardOrder(['prueba-agua','invalid','prueba-agua','paso-regalo']),['prueba-agua','paso-regalo']);
});
async function fixture(t){
 const records=new Map([['alice',{coachCardOrder:[]}],['bob',{coachCardOrder:[]}]]);let failed=false;
 const authorize=async(req,res)=>{const id=req.get('X-Fixture-User');if(!records.has(id)){res.status(401).json({error:'Sin sesión'});return null;}return {user:{_id:id,teamOwnerUserId:'shared-team'}};};
 const model={async updateOne(filter,update){if(failed)throw new Error('Database unavailable');const record=records.get(filter._id);if(!record)return{matchedCount:0};record.coachCardOrder=[...update.$set.coachCardOrder];return{matchedCount:1};}};
 const app=express();app.use(express.json());app.use('/api/coach',createCoachRequestGuard({requireActive:authorize}));registerCoachCardOrder(app,{requireActive:authorize,users:model});
 const server=await new Promise(resolve=>{const s=app.listen(0,'127.0.0.1',()=>resolve(s));});t.after(()=>{server.closeAllConnections();server.close();});
 const send=(body,user='alice',origin='https://totalelitecoaching.com')=>fetch(`http://127.0.0.1:${server.address().port}/api/coach/card-order`,{method:'PUT',headers:{'Content-Type':'application/json','X-Fixture-User':user,Origin:origin},body:JSON.stringify(body)});
 return {records,send,fail:()=>{failed=true;}};
}
test('accounts in the same team save only their own order, survive a new read, and can restore the standard order',async t=>{
 const f=await fixture(t),order=['prueba-agua','paso-conexion'];
 let res=await f.send({accountId:'alice',order});assert.equal(res.status,200);assert.deepEqual((await res.json()).order,order);
 assert.equal(res.headers.get('cache-control'),'private, no-store');assert.ok(res.headers.get('vary').includes('Cookie'));
 assert.deepEqual(readCoachCardOrder(f.records.get('alice').coachCardOrder),order);assert.deepEqual(f.records.get('bob').coachCardOrder,[]);
 res=await f.send({accountId:'bob',order:['paso-regalo']},'bob');assert.equal(res.status,200);assert.deepEqual(f.records.get('alice').coachCardOrder,order);
 res=await f.send({accountId:'alice',order:[]});assert.equal(res.status,200);assert.deepEqual(f.records.get('alice').coachCardOrder,[]);
});
test('unsigned requests, account switches and foreign-origin writes cannot alter preferences',async t=>{
 const f=await fixture(t);assert.equal((await f.send({accountId:'alice',order:[]},'unsigned')).status,401);
 assert.equal((await f.send({accountId:'alice',order:['prueba-agua']},'bob')).status,403);
 assert.equal((await f.send({accountId:'alice',order:['prueba-agua']},'alice','https://foreign.example')).status,403);
 assert.deepEqual(f.records.get('alice').coachCardOrder,[]);assert.deepEqual(f.records.get('bob').coachCardOrder,[]);
});
test('malformed or duplicated card IDs are rejected and storage failure never claims success',async t=>{
 const f=await fixture(t);
 for(const order of [null,'prueba-agua',{},['prueba-agua','prueba-agua'],['__proto__'],['paso-conexion',{}],Array(20).fill('paso-regalo')])assert.equal((await f.send({accountId:'alice',order})).status,400);
 f.fail();const response=await f.send({accountId:'alice',order:['prueba-agua']});assert.equal(response.status,503);assert.ok((await response.json()).error.includes('siguen en el editor'));assert.deepEqual(f.records.get('alice').coachCardOrder,[]);
});
