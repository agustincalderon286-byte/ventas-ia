import test from 'node:test';
import assert from 'node:assert/strict';
import express from 'express';
import sharp from 'sharp';
import { resolveTeamChatGroup,listTeamChatGroups,getExplicitChatDelegations } from '../src/coach-team-chat-access.js';
import { registerCoachTeamChat,teamChatRoomId } from '../src/coach-team-chat.js';
const id=n=>n.toString(16).padStart(24,'0');
const rigo={_id:id(1),name:'Rigoberto',accountType:'owner'},david={_id:id(2),name:'David Peñaherra',accountType:'owner',parentUserId:rigo._id},lorena={_id:id(3),name:'Lorena',accountType:'leader',parentUserId:david._id,teamOwnerUserId:david._id},eric={_id:id(4),name:'Eric',accountType:'seat',parentUserId:lorena._id,teamOwnerUserId:david._id},peer={_id:id(5),name:'Otro equipo',accountType:'owner'};
const records=[rigo,david,lorena,eric,peer];
const users={findById(key){return {select(){return this;},lean:async()=>records.find(r=>r._id===String(key))||null};},find(filter){return {select(){return this;},lean:async()=>records.filter(r=>filter.parentUserId.$in.map(String).includes(String(r.parentUserId)))};}};
const getOwnKey=user=>user.teamOwnerUserId||user._id;
const resolve=(user,target)=>resolveTeamChatGroup(user,target,{users,getOwnKey});

test('Rigoberto can select David’s group; it includes Lorena and Eric without merging independent groups',async()=>{
  assert.equal(await resolve(rigo,david._id),david._id);
  assert.equal(await resolve(lorena),david._id);assert.equal(await resolve(eric),david._id);
  const groups=await listTeamChatGroups(rigo,{users,getOwnKey});
  assert.deepEqual(groups.map(g=>g.ownerId),[rigo._id,david._id]);assert.match(groups[1].name,/David Peñaherra/);
  assert.notEqual(groups[0].roomId,groups[1].roomId);
});
test('a descendant cannot select an upline, another branch or a seat as an independent room',async()=>{
  for(const [user,target] of [[rigo,peer._id],[david,rigo._id],[eric,rigo._id],[rigo,lorena._id],[rigo,'malformed']])await assert.rejects(resolve(user,target),e=>e.status===403);
});
test('ancestry cycles and missing ancestors fail closed',async()=>{
  const bad={findById(key){return {select(){return this;},lean:async()=>({_id:key,accountType:'owner',parentUserId:key})};}};
  await assert.rejects(resolveTeamChatGroup(rigo,david._id,{users:bad,getOwnKey}),e=>e.status===403);
});

test('actual group routes let Rigoberto reply and open David’s attachments with his own identity, while blocking peer teams',async t=>{
  const rows=[];let sequence=100;
  const matches=(r,f)=>Object.entries(f).every(([k,v])=>v&&typeof v==='object'&&!Buffer.isBuffer(v)?Object.entries(v).every(([op,n])=>op==='$ne'?String(r[k])!==String(n):op==='$lt'?String(r[k])<String(n):op==='$gt'?String(r[k])>String(n):false):String(r[k])===String(v));
  const messages={findOne(f){return {select(){return this;},lean:async()=>rows.find(r=>matches(r,f))||null};},find(f){let limit=51;return {sort(){return this;},limit(n){limit=n;return this;},lean:async()=>rows.filter(r=>matches(r,f)).sort((a,b)=>b._id.localeCompare(a._id)).slice(0,limit)};},countDocuments:async f=>rows.filter(r=>matches(r,f)).length,create:async value=>{const r={...value,_id:id(++sequence)};rows.push(r);return r;}};
  const readers={findOne(){return {lean:async()=>null};}};
  const app=express();app.use(express.json());
  const requireActive=async(req,res)=>{const user=records.find(u=>u._id===req.get('X-Fixture-Account'));if(!user){res.status(401).json({error:'login'});return null;}return {user};};
  registerCoachTeamChat(app,{requireActive,getTeamKey:resolve,getRooms:user=>listTeamChatGroups(user,{users,getOwnKey}),messages,readers});
  const server=await new Promise(resolve=>{const s=app.listen(0,'127.0.0.1',()=>resolve(s));});t.after(()=>{server.closeAllConnections();server.close();});
  const request=async(path,user=rigo,body)=>{const response=await fetch(`http://127.0.0.1:${server.address().port}${path}`,{method:body?'POST':'GET',headers:{'Content-Type':'application/json','X-Fixture-Account':user._id},...(body?{body:JSON.stringify(body)}:{})});return {status:response.status,data:response.headers.get('content-type')?.includes('json')?await response.json():await response.arrayBuffer()};};
  const roomId=teamChatRoomId(david._id),token='12345678-1234-4123-8123-123456789012';
  const rooms=await request('/api/coach/team-chat/rooms');assert.equal(rooms.data.rooms.length,2);
  const photo=await sharp({create:{width:2,height:2,channels:3,background:'blue'}}).png().toBuffer();
  const sent=await request('/api/coach/team-chat/messages',rigo,{accountId:rigo._id,roomId,teamOwnerId:david._id,clientMessageId:token,body:'Hola, aquí está Rigoberto',media:{mime:'image/png',base64:photo.toString('base64')}});
  assert.equal(sent.status,201);assert.equal(sent.data.message.senderId,rigo._id);assert.equal(sent.data.message.senderName,'Rigoberto');
  assert.equal((await request('/api/coach/team-chat',eric)).data.messages[0].body,'Hola, aquí está Rigoberto');
  const media=sent.data.message.attachment.url;assert.ok(media.includes('teamOwnerId='+david._id));assert.equal((await request(media,rigo)).status,200);
  assert.equal((await request('/api/coach/team-chat',rigo)).data.messages.length,0);
  assert.equal((await request('/api/coach/team-chat?teamOwnerId='+peer._id,rigo)).status,403);
  assert.equal((await request('/api/coach/team-chat/messages',rigo,{accountId:rigo._id,roomId:teamChatRoomId(peer._id),teamOwnerId:peer._id,clientMessageId:token,body:'forged'})).status,403);
  assert.equal(rows.length,1);
});


test('reserved Rigoberto administrator gets the explicit shared root even without an ancestry link; other administrators and names get no grant',async()=>{
 const admin={_id:id(901),name:'Rigoberto',email:'calderonrigoberto51@gmail.com',accountType:'owner'};
 const policy=user=>getExplicitChatDelegations(user,user=>user.email==='calderonrigoberto51@gmail.com');
 const target=policy(admin)[0].ownerId;
 const shared={_id:target,name:'David Peñaherra',accountType:'owner',parentUserId:null};
 const juniorOwner={_id:id(903),name:'Otra cuenta de la rama',accountType:'owner',parentUserId:target};
 const unrelated={_id:id(904),name:'Otro equipo',accountType:'owner'};
 const all=[admin,shared,juniorOwner,unrelated];
 const localUsers={findById(key){return {select(){return this;},lean:async()=>all.find(r=>r._id===String(key))||null};},find(filter){return {select(){return this;},lean:async()=>all.filter(r=>filter.parentUserId.$in.map(String).includes(String(r.parentUserId)))};}};
 const options={users:localUsers,getOwnKey:user=>user._id,getDelegations:policy};
 assert.equal(await resolveTeamChatGroup(admin,undefined,options),target,'shared group is the default');
 assert.equal(await resolveTeamChatGroup(admin,target,options),target);
 assert.equal(await resolveTeamChatGroup(admin,juniorOwner._id,options),juniorOwner._id);
 assert.equal(await resolveTeamChatGroup(admin,admin._id,options),admin._id,'own empty group remains explicitly accessible');
 await assert.rejects(resolveTeamChatGroup(admin,unrelated._id,options),e=>e.status===403);
 const groups=await listTeamChatGroups(admin,options);assert.equal(groups.length,3);assert.equal(groups.find(r=>r.ownerId===target).preferred,true);
 assert.deepEqual(getExplicitChatDelegations({...admin,email:'another@example.com'},()=>true),[]);
 assert.deepEqual(getExplicitChatDelegations(admin,()=>false),[]);
 assert.deepEqual(getExplicitChatDelegations({_id:id(999),name:'Rigoberto'},()=>true),[]);
});

test('a stale explicit delegation never opens a deleted or non-owner group',async()=>{
 const admin={_id:id(901),email:'calderonrigoberto51@gmail.com',accountType:'owner'};
 const policy=user=>getExplicitChatDelegations(user,()=>true),target=policy(admin)[0].ownerId;
 const localUsers={findById(key){return {select(){return this;},lean:async()=>key===target?{_id:target,accountType:'seat'}:null};},find(){return {select(){return this;},lean:async()=>[]};}};
 const options={users:localUsers,getOwnKey:()=>admin._id,getDelegations:policy};
 assert.equal(await resolveTeamChatGroup(admin,undefined,options),admin._id);
 await assert.rejects(resolveTeamChatGroup(admin,target,options),e=>e.status===403);
 assert.equal((await listTeamChatGroups(admin,options)).length,1);
});
