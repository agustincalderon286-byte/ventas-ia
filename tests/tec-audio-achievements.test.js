import test from 'node:test';
import assert from 'node:assert/strict';
import crypto from 'node:crypto';
import express from 'express';
import {BADGES,emptyState,mergeStates,recordRange,rangeSeconds,evaluateBadges,ListeningTracker} from '../public/audiolibros/logros-core.js';
import {AchievementStore} from '../public/audiolibros/logros-store.js';
import {validateAchievementState,mergeSavedAchievements} from '../src/tec-audio-achievements.js';
import {registerAudioMembership} from '../src/tec-audio-membership.js';
import {audiobookCatalog as catalog} from '../src/tec-audiobooks.js';

const book=catalog[0],context={...book,previewDuration:Math.floor(book.duration*100)/1000,full:false};
const stateWith=(id,field,ranges)=>({v:1,books:{[id]:{[field]:ranges}}});
function model(initial=new Map()){
  return {rows:initial,findById(id){const row=initial.get(id);return{lean:async()=>row?structuredClone(row):null};},async updateOne(filter,update,options){
    const current=initial.get(filter._id),matches=current&&(typeof filter.revision==='number'?current.revision===filter.revision:current.revision===undefined);
    if(current&&!matches){if(options.upsert)throw Object.assign(new Error('duplicate'),{code:11000});return{matchedCount:0};}
    initial.set(filter._id,{_id:filter._id,...update.$set});return{matchedCount:current?1:0,upsertedCount:current?0:1};
  }};
}

test('exactly 25 distinct badges start locked and all are attainable with the eight current books',()=>{
  assert.equal(BADGES.length,25);assert.equal(new Set(BADGES.map(b=>b.id)).size,25);assert.equal(evaluateBadges(emptyState(),catalog).earned,0);
  const state=emptyState();for(const b of catalog)recordRange(state,{...b,full:true},0,b.duration,true);
  const view=evaluateBadges(state,catalog);assert.equal(view.earned,25);assert.equal(view.completed,8);assert.equal(view.samples,8);assert.equal(view.topics,4);
});

test('replays, overlapping devices, sample/full overlap and retries do not double count',()=>{
  const a=stateWith(book.id,'sample',[[0,60],[30,90]]),b=stateWith(book.id,'full',[[0,100]]);
  assert.equal(evaluateBadges(mergeStates(a,b,a,b),catalog).seconds,100);
  assert.deepEqual(mergeStates(a,b),mergeStates(b,a));
  assert.equal(evaluateBadges(stateWith(book.id,'sample',[[0,59.99]]),catalog).earned,0);
  assert.equal(evaluateBadges(stateWith(book.id,'sample',[[0,60]]),catalog).earned,1);
});

test('sample end or a cursor at the end never completes a book; full coverage and the ending are required',()=>{
  const sample=stateWith(book.id,'sample',[[0,context.previewDuration]]);
  assert.equal(evaluateBadges(sample,catalog).completed,0);
  assert.equal(evaluateBadges({position:book.duration,books:{}},catalog).completed,0);
  assert.equal(evaluateBadges(stateWith(book.id,'full',[[0,book.duration*.91]]),catalog).completed,0);
  assert.equal(evaluateBadges(stateWith(book.id,'full',[[book.duration-10,book.duration]]),catalog).completed,0);
  assert.equal(evaluateBadges(stateWith(book.id,'full',[[0,book.duration]]),catalog).completed,1);
});

test('tracker counts playback at 1x/2x but excludes seeks, pause, buffering and invalid clock movement',()=>{
  let seconds=0;const tracker=new ListeningTracker((a,b)=>seconds+=b-a);
  const tick=(position,now,playing=true,extra={})=>tracker.tick({position,now,playing,...extra});
  tick(0,0);tick(10,10000);assert.equal(seconds,10);
  tick(1000,11000);assert.equal(seconds,10);
  tick(1000,12000,false);tick(1000,50000,false);tick(1000,51000);tick(1002,53000);assert.equal(seconds,12);
  tick(1002,54000,true,{seeking:true});tick(2000,55000);tick(2000,55000,true,{rate:2});tick(2002,56000,true,{rate:2});
  tick(2004,57000,true,{rate:2});assert.equal(seconds,16);
  tick(2004,58000,false);tick(2004,90000);tick(2004,80000);assert.equal(seconds,16);
});

test('reading credit is limited to played ranges with reading visible, not an open panel alone',()=>{
  const state=emptyState();const tracker=new ListeningTracker((a,b,reading)=>recordRange(state,context,a,b,reading));
  tracker.tick({position:0,now:0,playing:true,reading:true});tracker.tick({position:10,now:10000,playing:true,reading:true});
  tracker.tick({position:20,now:20000,playing:true,reading:false});tracker.tick({position:30,now:30000,playing:true,reading:false});
  const view=evaluateBadges(state,catalog);assert.equal(view.seconds,30);assert.equal(view.reading,10);
});

test('server validates catalog IDs, limits, range bounds and strips unauthorized full-book credit',()=>{
  assert.throws(()=>validateAchievementState(stateWith('unknown','full',[[0,1]]),catalog));
  for(const ranges of [[[0,Infinity]],[[-1,2]],[[10,9]],[[0,context.previewDuration+1]],Array.from({length:4097},()=>[0,1])])assert.throws(()=>validateAchievementState(stateWith(book.id,'sample',ranges),catalog));
  assert.throws(()=>validateAchievementState({v:1,books:{[book.id]:{unlocked:['all']}}},catalog));
  const clean=validateAchievementState({v:1,books:{[book.id]:{sample:[[0,60]],full:[[0,100]],readingFull:[[0,100]]}}},catalog,false);
  assert.equal(rangeSeconds(clean.books[book.id].full),0);assert.equal(rangeSeconds(clean.books[book.id].sample),60);
});

test('atomic merge retains simultaneous device updates and duplicate creation races',async()=>{
  const db=model();await Promise.all([mergeSavedAchievements(db,'a',stateWith(book.id,'sample',[[0,60]])),mergeSavedAchievements(db,'a',stateWith(book.id,'sample',[[60,120]]))]);
  assert.equal(evaluateBadges(db.rows.get('a').state,catalog).seconds,120);
  await mergeSavedAchievements(db,'a',stateWith(book.id,'sample',[[0,60]]));assert.equal(evaluateBadges(db.rows.get('a').state,catalog).seconds,120);
});

async function httpFixture(t,{active=true,db=model()}={}){
  const tokens={a:'A'.repeat(43),b:'B'.repeat(43)};
  const sessions={findOne({_id}){const who=Object.keys(tokens).find(k=>crypto.createHash('sha256').update(tokens[k]).digest('hex')===_id);return{lean:async()=>who?{email:who}:null};}};
  const accounts={findById(id){return{lean:async()=>({_id:id,stripeCustomerId:'cus_'+id})};}};
  const stripe={subscriptions:{async *list(){if(active)yield{status:'active',metadata:{audience:'tec-audiolibros'},items:{data:[{price:{id:'audio'}}]}};}}};
  const app=express();app.use(express.json());registerAudioMembership(app,{accounts,sessions,achievements:db,catalog,stripe,priceId:'audio',publicOrigin:'https://totalelitecoaching.com',secure:false});
  const server=app.listen(0,'127.0.0.1');await new Promise(resolve=>server.once('listening',resolve));t.after(()=>new Promise(resolve=>server.close(resolve)));
  const req=(method,user,body,origin='https://totalelitecoaching.com',path='/logros')=>fetch(`http://127.0.0.1:${server.address().port}/api/audiolibros`+path,{method,headers:{'Content-Type':'application/json',Origin:origin,...(user?{Cookie:'tec_audio_session='+tokens[user]}:{})},...(body?{body:JSON.stringify(body)}:{})});
  return{req,db};
}

test('HTTP achievements require own account, reject cross-site writes and isolate users without exposing email',async t=>{
  const{req}=await httpFixture(t);
  assert.equal((await req('GET',null)).status,401);assert.equal((await req('PUT',null,{state:emptyState()})).status,401);
  assert.equal((await req('PUT','a',{state:emptyState()},'https://other.test')).status,403);
  const data=stateWith(book.id,'sample',[[0,60]]);
  assert.equal((await req('PUT','a',{state:data,userId:'b'})).status,200);
  assert.equal(evaluateBadges((await(await req('GET','a')).json()).state,catalog).seconds,60);
  assert.equal(evaluateBadges((await(await req('GET','b')).json()).state,catalog).seconds,0);
  const account=await(await req('GET','a',null,undefined,'/cuenta')).json();assert.match(account.achievementKey,/^[a-f0-9]{64}$/);assert.equal(account.email,undefined);
});

test('expired membership can view its badges and keep sample achievements without gaining new full-book credit',async t=>{
  const db=model(new Map([['a',{_id:'a',revision:1,state:stateWith(book.id,'full',[[0,100]])}]]));const{req}=await httpFixture(t,{active:false,db});
  const state=mergeStates(stateWith(book.id,'full',[[0,200]]),stateWith(book.id,'sample',[[150,200]]));
  const response=await req('PUT','a',{state});assert.equal(response.status,200);const saved=(await response.json()).state;
  assert.equal(rangeSeconds(saved.books[book.id].full),100);assert.equal(rangeSeconds(saved.books[book.id].sample),50);
});

const storage=()=>{const rows=new Map();return{rows,getItem:key=>rows.get(key)||null,setItem:(key,value)=>rows.set(key,value)};};
test('offline state survives reload, retries are idempotent and guest/account histories remain separate',async()=>{
  const disk=storage();let online=false,cloud=emptyState();const request=async(method,body)=>{if(!online)throw new Error('offline');if(method==='PUT')cloud=mergeStates(cloud,body.state);return{state:cloud};};
  const local=new AchievementStore({storage:disk,request});await local.setIdentity('');local.record(context,0,100,false);local.persist();
  await local.setIdentity('account-a');assert.equal(evaluateBadges(local.state,catalog).seconds,0);local.record(context,100,200,false);await local.sync();
  const restored=new AchievementStore({storage:disk,request});await restored.setIdentity('account-a');assert.equal(evaluateBadges(restored.state,catalog).seconds,100);
  online=true;await restored.sync();await restored.sync();assert.equal(evaluateBadges(cloud,catalog).seconds,100);
  cloud=emptyState();await restored.setIdentity('account-b');assert.equal(evaluateBadges(restored.state,catalog).seconds,0);await restored.setIdentity('');assert.equal(evaluateBadges(restored.state,catalog).seconds,100);
});

test('late cloud response after switching accounts cannot leak the previous account into the new one',async()=>{
  let finish;const disk=storage();const request=async()=>new Promise(resolve=>{finish=resolve;});
  const store=new AchievementStore({storage:disk,request});const pending=store.setIdentity('a');await store.setIdentity('');
  finish({state:stateWith(book.id,'full',[[0,200]])});await pending;assert.equal(evaluateBadges(store.state,catalog).seconds,0);
});

test('a playback update arriving during sync is preserved and sent by the next checkpoint',async()=>{
  let resolvePut,cloud=emptyState();const request=async(method,body)=>method==='GET'?{state:cloud}:new Promise(resolve=>{resolvePut=()=>{cloud=mergeStates(cloud,body.state);resolve({state:cloud});};});
  const store=new AchievementStore({storage:storage(),request});await store.setIdentity('a');store.record(context,0,60,false);
  const pending=store.sync();store.record(context,60,100,false);await Promise.resolve();resolvePut();await pending;
  assert.equal(evaluateBadges(store.state,catalog).seconds,100);assert.equal(store.dirty,true);
  const next=store.sync();await Promise.resolve();resolvePut();await next;assert.equal(evaluateBadges(cloud,catalog).seconds,100);
});

test('blocked device storage does not prevent server persistence',async()=>{
  let cloud=emptyState();const store=new AchievementStore({storage:{getItem(){throw new Error('blocked');},setItem(){throw new Error('blocked');}},request:async(method,body)=>{if(body)cloud=mergeStates(cloud,body.state);return{state:cloud};}});
  await store.setIdentity('a');store.record(context,0,60,false);await store.sync();assert.equal(evaluateBadges(cloud,catalog).seconds,60);
});


test('reconnecting after an empty offline load pulls the existing account badges without new playback',async()=>{
  let online=false;const cloud=stateWith(book.id,'full',[[0,200]]);
  const store=new AchievementStore({storage:storage(),request:async()=>{if(!online)throw new Error('offline');return{state:cloud};}});
  await store.setIdentity('a');assert.equal(evaluateBadges(store.state,catalog).seconds,0);
  online=true;await store.sync();assert.equal(evaluateBadges(store.state,catalog).seconds,200);
});

test('two tabs merge durable ranges without overwriting each other',async()=>{
  const disk=storage(),request=async()=>({state:emptyState()});
  const a=new AchievementStore({storage:disk,request}),b=new AchievementStore({storage:disk,request});
  await a.setIdentity('');await b.setIdentity('');a.record(context,0,60,false);b.record(context,60,100,false);a.persist();b.persist();a.absorbStorage();
  assert.equal(evaluateBadges(a.state,catalog).seconds,100);assert.equal(evaluateBadges(b.state,catalog).seconds,100);
});


test('a queued save for a prior account is cancelled before credentials can change its recipient',async()=>{
  const writes=[];let currentUser='a';const store=new AchievementStore({storage:storage(),request:async(method,body)=>{if(method==='PUT')writes.push({user:currentUser,state:body.state});return{state:emptyState()};}});
  await store.setIdentity('a');store.record(context,0,60,false);const pending=store.sync();currentUser='b';await store.setIdentity('b');await pending;
  assert.equal(writes.length,0);assert.equal(evaluateBadges(store.state,catalog).seconds,0);
});
