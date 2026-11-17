import test from 'node:test';
import assert from 'node:assert/strict';
import express from 'express';
import mongoose from 'mongoose';
import webpush from 'web-push';
import { randomBytes } from 'node:crypto';
import { createCoachTeamPush,createCoachPushModels,loadChatPushKeys,normalizeChatPushSubscription,CHAT_PUSH_SIGNAL } from '../src/coach-team-chat-push.js';
import { teamChatRoomId } from '../src/coach-team-chat.js';
import { createCoachRequestGuard } from '../src/tec-security.js';
const id=n=>n.toString(16).padStart(24,'0');
const at=Date.parse('2026-11-17T22:00:00Z');
const valid=()=>({endpoint:'https://fcm.googleapis.com/fcm/send/'+randomBytes(24).toString('hex'),keys:{p256dh:webpush.generateVAPIDKeys().publicKey,auth:randomBytes(16).toString('base64url')}});
const compare=(a,b)=>a instanceof Date||b instanceof Date?new Date(a).getTime()-new Date(b).getTime():String(a).localeCompare(String(b));
function matches(row,filter){return Object.entries(filter).every(([key,value])=>{
 if(key==='$or')return value.some(f=>matches(row,f));
 const actual=row[key];
 if(value&&typeof value==='object'&&!(value instanceof Date))return Object.entries(value).every(([op,bound])=>op==='$ne'?String(actual)!==String(bound):op==='$in'?bound.some(v=>String(v)===String(actual)):op==='$elemMatch'?Array.isArray(actual)&&actual.some(v=>matches(v,bound)):actual!==undefined&&(op==='$lt'?compare(actual,bound)<0:op==='$lte'?compare(actual,bound)<=0:op==='$gt'?compare(actual,bound)>0:op==='$gte'?compare(actual,bound)>=0:false));
 return String(actual)===String(value);
});}
function collection(rows=[]){let seq=500;const update=(row,value,insert=false)=>{Object.assign(row,insert?value.$setOnInsert||{}:{},value.$set||{});for(const[key,v]of Object.entries(value.$inc||{}))row[key]=(row[key]||0)+v;for(const key of Object.keys(value.$unset||{}))delete row[key];};
 function query(get){let sort=null,limit=Infinity;return {select(){return this;},sort(value){sort=value;return this;},limit(value){limit=value;return this;},async lean(){let result=get();if(Array.isArray(result)){if(sort)result.sort((a,b)=>{for(const[key,dir]of Object.entries(sort)){const d=compare(a[key],b[key]);if(d)return d*dir;}return 0;});result=result.slice(0,limit);}return result===null?null:structuredClone(result);}};}
 const model={rows,find:f=>query(()=>rows.filter(r=>matches(r,f))),findOne:f=>query(()=>rows.find(r=>matches(r,f))||null),findById:key=>query(()=>rows.find(r=>String(r._id)===String(key))||null),findOneAndUpdate(f,u,o={}){return query(()=>{let row=rows.find(r=>matches(r,f));if(!row&&!o.upsert)return null;const inserting=!row;if(!row){row={...f,_id:f._id||id(++seq)};rows.push(row);}update(row,u,inserting);return row;});},async updateOne(f,u,o={}){let row=rows.find(r=>matches(r,f));if(!row&&!o.upsert)return {matchedCount:0};const inserting=!row;if(!row){row={...f,_id:f._id||id(++seq)};rows.push(row);}update(row,u,inserting);return {matchedCount:1};},async updateMany(f,u){for(const row of rows.filter(r=>matches(r,f)))update(row,u);},async bulkWrite(items){for(const item of items)await model.updateOne(item.updateOne.filter,item.updateOne.update,{upsert:true});}};
 return model;
}
async function fixture(t){
 let clock=at,sendError=null,allowed=true;
 const owner=id(1),member=id(2),other=id(3),session=id(12),room=teamChatRoomId(owner),subscription=valid(),keys=webpush.generateVAPIDKeys();
 const devices=collection(),configs=collection([{_id:'team-chat-v1',...keys}]),deliveries=collection(),messages=collection(),readers=collection();
 const models={devices,configs,deliveries},sent=[];
 const users={owner:{_id:owner,team:owner},member:{_id:member,team:owner},other:{_id:other,team:other}};
 const requireActive=async(req,res)=>{const user=users[req.get('X-User')];if(!user){res.status(401).json({error:'login'});return null;}return {user,session:{_id:session,expiresAt:new Date(clock+86400000)}};};
 const service=createCoachTeamPush({models,messages,readers,requireActive,getTeamKey:async(user,target)=>{if(target&&target!==user.team)throw Object.assign(new Error('group'),{status:403});return user.team;},roomIdForKey:teamChatRoomId,canDeliver:async()=>allowed,now:()=>clock,send:async(...args)=>{sent.push(args);if(sendError)throw sendError;return {statusCode:201};}});
 await service.initialize();
 const app=express();app.use(express.json());app.use('/api/coach',createCoachRequestGuard({requireActive}));service.register(app);
 const server=await new Promise(resolve=>{const s=app.listen(0,'127.0.0.1',()=>resolve(s));});t.after(()=>{service.stop();server.closeAllConnections();server.close();});
 async function request(path,{user='member',body,origin='https://totalelitecoaching.com'}={}){const response=await fetch(`http://127.0.0.1:${server.address().port}/api/coach/team-chat/push/`+path,{method:body?'POST':'GET',headers:{'X-User':user,Origin:origin,...(body?{'Content-Type':'application/json'}:{})},...(body?{body:JSON.stringify(body)}:{})});return {status:response.status,headers:response.headers,data:await response.json()};}
 const body=(fields={})=>({accountId:member,roomId:room,subscription,...fields});
 const subscribe=()=>request('subscribe',{body:body()});
 const postMessage=()=>{const row={_id:id(100+messages.rows.length),roomId:room,senderId:owner,body:'SECRET CUSTOMER / VERÓNICA / SALE $2500',createdAt:new Date(clock+1),pushState:'pending'};messages.rows.push(row);clock+=10;return row;};
 return {service,models,messages,readers,sent,subscription,keys,owner,member,other,session,room,request,body,subscribe,postMessage,advance(ms){clock+=ms;},deny(){allowed=false;},error(error){sendError=error;}};
}

test('subscription validation rejects arbitrary destinations, private addresses, broken keys and spoofed provider names',()=>{
 const good=valid();assert.equal(normalizeChatPushSubscription(good).endpoint,good.endpoint);
 for(const endpoint of ['https://127.0.0.1/private','https://evil.example/','https://fcm.googleapis.com.evil.example/x','http://fcm.googleapis.com/x','https://x@fcm.googleapis.com/x','https://fcm.googleapis.com:444/x'])assert.throws(()=>normalizeChatPushSubscription({...good,endpoint}));
 assert.throws(()=>normalizeChatPushSubscription({...good,keys:{...good.keys,p256dh:'A'.repeat(87)}}));
 assert.throws(()=>normalizeChatPushSubscription({...good,keys:{...good.keys,auth:'bad'}}));
});
test('device/config secrets are excluded by default and key initialization survives duplicate bootstrap',async()=>{
 const m=createCoachPushModels(mongoose);assert.equal(m.devices.schema.path('push').options.select,false);assert.equal(m.configs.schema.path('privateKey').options.select,false);
 const keys=webpush.generateVAPIDKeys();let writes=0;
 const configs={findOneAndUpdate(){return {select(){return this;},async lean(){writes++;throw Object.assign(new Error('race'),{code:11000});}};},findById(){return {select(){return this;},lean:async()=>keys};}};
 const restored=await loadChatPushKeys(configs);assert.equal(restored.publicKey,keys.publicKey);assert.equal(writes,1);
});
test('configuration is private, leaks no private VAPID key, and subscription requires the exact account and group',async t=>{
 const f=await fixture(t);const cfg=await f.request('config');assert.equal(cfg.status,200);assert.equal(cfg.data.publicKey,f.keys.publicKey);assert.equal(cfg.data.privateKey,undefined);assert.equal(cfg.headers.get('cache-control'),'private, no-store');
 assert.equal((await f.request('config',{user:'anonymous'})).status,401);
 assert.equal((await f.request('subscribe',{body:f.body({accountId:f.other})})).status,403);
 assert.equal((await f.request('subscribe',{body:f.body({roomId:'forged'})})).status,403);
 assert.equal((await f.request('subscribe',{body:f.body({teamOwnerId:f.other})})).status,403);
 assert.equal((await f.request('subscribe',{body:f.body(),origin:'https://evil.example'})).status,403);
 assert.equal(f.models.devices.rows.length,0);
});
test('a device receives nothing before opt-in; subscribe/status/unsubscribe are scoped to this account/session/group',async t=>{
 const f=await fixture(t);f.postMessage();await f.service.fanout();assert.equal(f.models.deliveries.rows.length,0);assert.equal(f.sent.length,0);
 assert.equal((await f.subscribe()).data.enabled,true);assert.equal((await f.request('status',{body:f.body()})).data.enabled,true);
 assert.equal((await f.request('status',{user:'other',body:f.body({accountId:f.other,roomId:teamChatRoomId(f.other)})})).data.enabled,false);
 assert.equal((await f.request('unsubscribe',{body:f.body()})).data.enabled,false);assert.equal(f.models.devices.rows[0].enabled,false);
});
test('new messages fan out idempotently and provider receives only a generic signal with no internal identifiers or content',async t=>{
 const f=await fixture(t);await f.subscribe();const message=f.postMessage();await f.service.fanout();assert.equal(f.models.deliveries.rows.length,1);message.pushState='pending';delete message.pushFanoutAfter;await f.service.fanout();assert.equal(f.models.deliveries.rows.length,1);
 await f.service.deliver();assert.equal(f.sent.length,1);const [subscription,payload,options]=f.sent[0];assert.equal(subscription.endpoint,f.subscription.endpoint);assert.equal(payload,CHAT_PUSH_SIGNAL);assert.deepEqual(JSON.parse(payload),{type:'tec-chat'});
 for(const value of [f.member,f.owner,f.room,message._id,'VERÓNICA','CUSTOMER','2500'])assert.ok(!payload.includes(value));assert.equal(options.vapidDetails.publicKey,f.keys.publicKey);assert.equal(f.models.deliveries.rows[0].state,'accepted');
 assert.equal(await f.service.deliver(),false);
});
test('the author, already-read messages and revoked membership do not trigger a push',async t=>{
 const f=await fixture(t);await f.subscribe();f.models.devices.rows[0].accountId=f.owner;f.postMessage();await f.service.fanout();assert.equal(f.models.deliveries.rows.length,0);
 f.models.devices.rows[0].accountId=f.member;const message=f.postMessage();await f.service.fanout();f.readers.rows.push({accountId:f.member,roomId:f.room,lastReadMessageId:message._id});await f.service.deliver();assert.equal(f.sent.length,0);
 f.readers.rows.length=0;f.postMessage();await f.service.fanout();f.deny();await f.service.deliver();assert.equal(f.sent.length,0);
});
test('pre-opt-in history, expired messages and changed sessions never notify',async t=>{
 const f=await fixture(t);const historical=f.postMessage();await f.subscribe();await f.service.fanout();assert.equal(f.models.deliveries.rows.length,0);
 f.postMessage();await f.service.fanout();f.models.devices.rows[0].sessionId=id(999);await f.service.deliver();assert.equal(f.sent.length,0);
 historical.pushState='pending';f.advance(300001);await f.service.fanout();assert.equal(historical.pushState,'done');
});
test('provider expiry disables only that subscription; temporary failures retry with the same delivery',async t=>{
 const f=await fixture(t);await f.subscribe();f.postMessage();await f.service.fanout();f.error(Object.assign(new Error('temporary'),{statusCode:503}));await f.service.deliver();assert.equal(f.models.deliveries.rows[0].state,'pending');assert.equal(f.models.deliveries.rows.length,1);
 f.advance(20000);f.error(Object.assign(new Error('expired'),{statusCode:410}));await f.service.deliver();assert.equal(f.models.devices.rows[0].enabled,false);assert.equal(f.models.deliveries.rows[0].state,'skipped');
});
test('a stolen endpoint with different encryption keys cannot take over another device',async t=>{
 const f=await fixture(t);await f.subscribe();const attacker=valid();attacker.endpoint=f.subscription.endpoint;
 const result=await f.request('subscribe',{user:'other',body:f.body({accountId:f.other,roomId:teamChatRoomId(f.other),subscription:attacker})});assert.equal(result.status,409);assert.equal(f.models.devices.rows[0].accountId,f.member);
});
test('a generic wake-up resolves the notification target only inside authenticated TEC and never reveals it to another account',async t=>{
 const f=await fixture(t);await f.subscribe();f.postMessage();await f.service.fanout();await f.service.deliver();
 const inbox=await f.request('inbox',{body:f.body()});assert.equal(inbox.data.notify,true);assert.match(inbox.data.url,/openTeamChat=1&teamChatOwnerId=/);
 const other=await f.request('inbox',{user:'other',body:f.body({accountId:f.other,roomId:teamChatRoomId(f.other)})});assert.equal(other.data.notify,false);assert.equal(other.data.url,undefined);
 await f.request('unsubscribe',{body:f.body()});assert.equal((await f.request('inbox',{body:f.body()})).data.notify,false);
});
