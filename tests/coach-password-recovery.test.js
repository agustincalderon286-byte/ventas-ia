import test from 'node:test';
import assert from 'node:assert/strict';
import crypto from 'node:crypto';
import fs from 'node:fs';
import vm from 'node:vm';
import express from 'express';
import mongoose from 'mongoose';
import {createCoachPasswordRecovery,registerCoachPasswordRecovery,coachSessionCredentialsMatch,coachPreviousSessionsQuery,COACH_RECOVERY_MESSAGE} from '../src/coach-password-recovery.js';
import {createCoachRequestGuard} from '../src/tec-security.js';
const hash=value=>crypto.createHash('sha256').update(value).digest('hex');
function matches(doc,query){return Object.entries(query).every(([key,expected])=>{if(key==='$or')return expected.some(q=>matches(doc,q));const actual=doc[key];if(expected&&typeof expected==='object'&&!(expected instanceof Date)){return Object.entries(expected).every(([op,value])=>op==='$exists'?(actual!==undefined)===value:op==='$gt'?actual>value:op==='$lte'?actual<=value:actual===expected);}return actual===expected;});}
const lean=value=>({select(){return this;},async lean(){return value?structuredClone(value):null;}});
function harness(options={}){
 const records=[{_id:'one',email:'owner@example.test',passwordSalt:'old-salt',passwordHash:'old-hash',accountType:'owner',subscriptionStatus:'active',credentialVersion:''},{_id:'two',email:'other@example.test',passwordSalt:'other-salt',passwordHash:'other-hash',accountType:'seat'},{_id:'phone',username:'8475550123',passwordSalt:'phone-salt',passwordHash:'phone-hash'}],mail=[],deleted=[],warnings=[];let time=1000000;
 const users={findOne(query){return lean(records.find(d=>matches(d,query)));},findOneAndUpdate(query,update){const doc=records.find(d=>matches(d,query));if(doc){Object.assign(doc,update.$set||{});for(const key of Object.keys(update.$unset||{}))delete doc[key];}return lean(doc);},async updateOne(query,update){this.findOneAndUpdate(query,update);}};
 const deps={users,sessions:{async deleteMany(query){deleted.push(query);if(options.deleteFails)throw new Error('database cleanup failed');await options.beforeDelete?.(query,records);if(options.sessionRows)for(let i=options.sessionRows.length-1;i>=0;i--)if(matches(options.sessionRows[i],query))options.sessionRows.splice(i,1);}},createPassword:options.createPassword|| (async password=>({passwordSalt:'new-salt-'+password,passwordHash:hash(password)})),sendMail:options.sendMail|| (async message=>{mail.push(message);}),now:()=>time,logger:{warn:message=>warnings.push(message)}};
 const service=createCoachPasswordRecovery(deps);
 return{service,records,mail,deleted,warnings,deps,advance:ms=>{time+=ms;},token:()=>new URL(mail.find(m=>m.subject.startsWith('Restablece')).text.match(/https:\/\/[^\s]+/)[0]).hash.slice(7)};
}
async function issue(h,email='owner@example.test',ip='request-ip'){const response=h.service.requestRecovery(email,ip);await h.service.drain();return response;}
test('request returns a generic response before DB/mail work, hashes a single-use token, and uses only the stored email',async()=>{
 let release;const gate=new Promise(resolve=>{release=resolve;});const mail=[];const h=harness({sendMail:async m=>{mail.push(m);await gate;}});
 const known=h.service.requestRecovery(' OWNER@example.test ','same-ip');const unknown=h.service.requestRecovery('missing@example.test','same-ip');assert.deepEqual(known,unknown);assert.equal(known.message,COACH_RECOVERY_MESSAGE);
 await new Promise(setImmediate);assert.equal(mail.length,1);assert.deepEqual(mail[0].to,['owner@example.test']);const url=new URL(mail[0].text.match(/https:\/\/[^\s]+/)[0]);const token=url.hash.slice(7);assert.equal(url.origin,'https://totalelitecoaching.com');assert.equal(url.pathname,'/coach/recuperar/');assert.equal(url.search,'');assert.match(token,/^[A-Za-z0-9_-]{43}$/);assert.equal(h.records[0].passwordResetDigest,hash(token));assert.notEqual(h.records[0].passwordResetDigest,token);assert.equal(Number(h.records[0].passwordResetExpiresAt)-1000000,30*60000);release();await h.service.drain();
 assert.throws(()=>h.service.requestRecovery('8475550123','ip'),e=>e.status===400);
 await issue(h,'phone@example.test','ip');assert.equal(mail.length,1);assert.equal(h.records[2].passwordResetDigest,undefined);
});
test('valid token updates only its own credentials, invalidates all sessions, notifies and cannot be reused',async()=>{
 const h=harness();await issue(h);const token=h.token(),other=structuredClone(h.records[1]);
 const result=await h.service.resetPassword({token,password:'correct new password',passwordConfirmation:'correct new password',email:'other@example.test'},'ip');assert.equal(result.ok,true);assert.equal(h.records[0].passwordHash,hash('correct new password'));assert.notEqual(h.records[0].credentialVersion,'');assert.equal(h.records[0].passwordResetDigest,undefined);assert.equal(h.records[0].accountType,'owner');assert.equal(h.records[0].subscriptionStatus,'active');assert.deepEqual(h.records[1],other);assert.deepEqual(h.deleted,[coachPreviousSessionsQuery('one','')]);assert.equal(h.mail.length,2);assert.doesNotMatch(h.mail[1].text,/correct new password/);
 await assert.rejects(h.service.resetPassword({token,password:'another password',passwordConfirmation:'another password'},'ip'),e=>e.status===400);
});
test('cleanup removes only the prior snapshot and fieldless legacy sessions, preserving new and future logins',async()=>{
 for(const futureRotation of [false,true]){
  const rows=[{id:'old',userId:'one',credentialVersion:'previous'},{id:'legacy',userId:'one'},{id:'older',userId:'one',credentialVersion:'older-revoked'},{id:'other-account',userId:'two',credentialVersion:'previous'}];
  const h=harness({sessionRows:rows,beforeDelete:(_query,records)=>{
    rows.push({id:'new-login',userId:'one',credentialVersion:records[0].credentialVersion});
    if(futureRotation){records[0].credentialVersion='future-rotation';rows.push({id:'future-login',userId:'one',credentialVersion:'future-rotation'});}
  }});h.records[0].credentialVersion='previous';await issue(h);await h.service.resetPassword({token:h.token(),password:'new password',passwordConfirmation:'new password'},'ip');
  assert.deepEqual(rows.map(row=>row.id),['older','other-account','new-login',...(futureRotation?['future-login']:[])]);
  assert.equal(coachSessionCredentialsMatch(rows.find(row=>row.id===(futureRotation?'future-login':'new-login')),h.records[0]),true);
  assert.equal(coachSessionCredentialsMatch(rows.find(row=>row.id==='older'),h.records[0]),false);
 }
});
test('leader reset cleanup also preserves a login issued after saving the new password',async()=>{
 const source=fs.readFileSync(new URL('../server.js',import.meta.url),'utf8'),start=source.indexOf('app.post("/api/coach/team/seats/:userId/reset-password"'),end=source.indexOf('\napp.post("/api/coach/team/seats/:userId/switch"',start),userId='671000000000000000000001';
 const rows=[{id:'old',userId,credentialVersion:'previous'},{id:'legacy',userId}];let handler,body;
 const seat={_id:userId,credentialVersion:'previous',email:'seat@example.test',async save(){rows.push({id:'new-login',userId,credentialVersion:this.credentialVersion});},toObject(){return{};}};
 const context={app:{post:(_path,fn)=>{handler=fn;}},requireCoachTeamManager:async()=>({user:{_id:'manager'}}),mongoose,resolverCoachOwnerUserId:()=> 'manager',downlineScope:async()=>({query:{}}),CoachUser:{findOne:async()=>seat},generarCoachSeatPasswordTemporal:()=> 'temporary password',crearPasswordSeguro:async()=>({hash:'new-hash',salt:'new-salt'}),generarTokenSeguro:()=> 'new-version',Date,coachPreviousSessionsQuery,CoachSession:{async deleteMany(query){for(let i=rows.length-1;i>=0;i--)if(matches(rows[i],query))rows.splice(i,1);}},asegurarCoachDistributorProfile:async()=>null,limpiarCoachTeamSeat:()=>({}),responderCoachError:()=>{throw new Error('unexpected route error');},console};
 vm.createContext(context);vm.runInContext(source.slice(start,end),context);await handler({params:{userId}},{json:value=>{body=value;}});
 assert.deepEqual(rows.map(row=>row.id),['new-login']);assert.equal(coachSessionCredentialsMatch(rows[0],seat),true);assert.equal(body.forcedSignOut,true);
});
test('expired, replaced, changed-email, changed-credential, malformed and mismatched requests cannot reset',async()=>{
 for(const mutation of [h=>h.advance(30*60000),h=>{h.records[0].email='changed@example.test';},h=>{h.records[0].passwordSalt='changed-salt';}]){const h=harness();await issue(h);const token=h.token();mutation(h);await assert.rejects(h.service.resetPassword({token,password:'new password',passwordConfirmation:'new password'},'ip'),e=>e.status===400);assert.equal(h.deleted.length,0);}
 const h=harness();await issue(h);const token=h.token();await assert.rejects(h.service.resetPassword({token,password:'short',passwordConfirmation:'short'},'ip'),e=>e.field==='password');await assert.rejects(h.service.resetPassword({token,password:'new password',passwordConfirmation:'different password'},'ip'),e=>e.field==='passwordConfirmation');await assert.rejects(h.service.resetPassword({token:{$ne:null},password:'new password',passwordConfirmation:'new password'},'ip'),e=>e.status===400);assert.equal(h.records[0].passwordHash,'old-hash');
 h.advance(60001);await issue(h);await assert.rejects(h.service.resetPassword({token,password:'new password',passwordConfirmation:'new password'},'ip'),e=>e.status===400);
});
test('concurrent use consumes the token atomically exactly once',async()=>{
 let entered=0,release;const gate=new Promise(resolve=>{release=resolve;});const h=harness({createPassword:async()=>{entered++;if(entered===2)release();await gate;return{passwordSalt:'new-salt',passwordHash:'new-hash'};}});await issue(h);const body={token:h.token(),password:'a new password',passwordConfirmation:'a new password'};const result=await Promise.allSettled([h.service.resetPassword(body,'ip'),h.service.resetPassword(body,'ip')]);assert.equal(result.filter(r=>r.status==='fulfilled').length,1);assert.equal(result.filter(r=>r.status==='rejected'&&r.reason.status===400).length,1);assert.equal(h.deleted.length,1);
});
test('credential version revokes previous and return sessions even when deletion fails; legacy sessions stay compatible',async()=>{
 assert.equal(coachSessionCredentialsMatch({},{}),true);const h=harness({deleteFails:true});await issue(h);await h.service.resetPassword({token:h.token(),password:'new password',passwordConfirmation:'new password'},'ip');assert.equal(coachSessionCredentialsMatch({credentialVersion:''},h.records[0]),false);assert.equal(coachSessionCredentialsMatch({credentialVersion:h.records[0].credentialVersion},h.records[0]),true);assert.equal(h.warnings.length,1);
 const source=fs.readFileSync(new URL('../server.js',import.meta.url),'utf8'),start=source.indexOf('async function obtenerCoachAuthPorToken('),end=source.indexOf('\nasync function crearCoachSesion(',start);const context={hashTokenSeguro:hash,coachSessionCredentialsMatch,Date,CoachSession:{findOne:()=>({populate:async()=>({userId:h.records[0],credentialVersion:'',save:async()=>{throw new Error('must not touch revoked session');}})})}};vm.createContext(context);vm.runInContext(source.slice(start,end),context);const auth=await context.obtenerCoachAuthPorToken('x'.repeat(43));assert.equal(auth.user,null);assert.equal(auth.session,null);
});
test('saving a hydrated legacy login after reset cannot overwrite its credential version or authorize its stale snapshot',async()=>{
 const source=fs.readFileSync(new URL('../server.js',import.meta.url),'utf8');
 const schemaStart=source.indexOf('const coachUserSchema = new mongoose.Schema('),schemaEnd=source.indexOf('\nconst coachDistributorProfileSchema',schemaStart);
 const schema=vm.runInNewContext(source.slice(schemaStart,schemaEnd)+'\ncoachUserSchema;',{mongoose});
 const User=mongoose.models.CoachRecoveryHydrationFixture || mongoose.model('CoachRecoveryHydrationFixture',schema);
 const h=harness();h.records[0]._id='671000000000000000000001';delete h.records[0].credentialVersion;
 // This document was hydrated by login or /me before another request resets the password.
 const legacy=User.hydrate(h.records[0]);assert.equal(legacy.credentialVersion,undefined);
 await issue(h);await h.service.resetPassword({token:h.token(),password:'new password',passwordConfirmation:'new password'},'ip');const rotated=h.records[0].credentialVersion;
 legacy.lastLoginAt=new Date();legacy.updatedAt=new Date();const changes=legacy.getChanges();
 assert.equal(Object.hasOwn(changes.$set||{},'credentialVersion'),false);assert.equal(Object.hasOwn(changes.$setOnInsert||{},'credentialVersion'),false);
 Object.assign(h.records[0],changes.$set);assert.equal(h.records[0].credentialVersion,rotated);
 // Exercise the real issuer with the pre-reset login snapshot, after the reset committed.
 const issuerStart=source.indexOf('async function crearCoachSesion('),issuerEnd=source.indexOf('\nasync function destruirCoachSesion(',issuerStart),created=[];
 const context={Date,COACH_SESSION_DAYS:30,COACH_MAX_ACTIVE_SESSIONS:4,generarTokenSeguro:()=> 'z'.repeat(43),hashTokenSeguro:hash,obtenerCoachIp:()=> '127.0.0.1',setCoachCookie:()=>{},CoachSession:{find:()=>({sort:async()=>[]}),create:async value=>created.push(value)}};
 vm.createContext(context);vm.runInContext(source.slice(issuerStart,issuerEnd),context);await context.crearCoachSesion({headers:{}},{},legacy);
 assert.equal(created[0].credentialVersion,'');assert.equal(coachSessionCredentialsMatch(created[0],h.records[0]),false);
 // The session schema may normalize its own legacy snapshot to ''; it never saves user credentials.
 const sessionStart=source.indexOf('const coachSessionSchema = new mongoose.Schema('),sessionEnd=source.indexOf('\nconst coachLeadInboxSchema',sessionStart);
 const sessionSchema=vm.runInNewContext(source.slice(sessionStart,sessionEnd)+'\ncoachSessionSchema;',{mongoose});
 const Session=mongoose.models.CoachRecoverySessionHydrationFixture || mongoose.model('CoachRecoverySessionHydrationFixture',sessionSchema);
 const oldSession=Session.hydrate({_id:'671000000000000000000002',userId:legacy._id,tokenHash:'token-hash',expiresAt:new Date(Date.now()+60000)});oldSession.lastSeenAt=new Date();
 assert.equal(oldSession.credentialVersion,'');assert.equal(coachSessionCredentialsMatch(oldSession,h.records[0]),false);assert.equal(h.records[0].credentialVersion,rotated);
});
test('mail failures invalidate the unsent grant without changing password and per-address/IP limits bound spam',async()=>{
 const h=harness({sendMail:async()=>{throw new Error('provider failure with secret');}});await issue(h);assert.equal(h.records[0].passwordResetDigest,undefined);assert.equal(h.records[0].passwordHash,'old-hash');assert.doesNotMatch(h.warnings.join(''),/secret/);
 const limited=harness();for(let n=0;n<4;n++){await issue(limited,'owner@example.test','same-ip');limited.advance(60001);}assert.equal(limited.mail.length,3);
 const ipLimited=harness();for(let n=0;n<12;n++)ipLimited.service.requestRecovery(`missing${n}@example.test`,'same-ip');assert.throws(()=>ipLimited.service.requestRecovery('other@example.test','same-ip'),e=>e.status===429);await ipLimited.service.drain();
});
test('HTTP routes enforce JSON, same-origin guard, generic status, no-store and no session creation',async t=>{
 const h=harness(),app=express();let clears=0;app.use(express.json());app.use('/api/coach',createCoachRequestGuard({requireActive:async()=>null}));const service=registerCoachPasswordRecovery(app,{...h.deps,clearCookies:()=>{clears++;}});const server=await new Promise(resolve=>{const s=app.listen(0,'127.0.0.1',()=>resolve(s));});t.after(()=>{server.closeAllConnections();server.close();});const base=`http://127.0.0.1:${server.address().port}`;const post=(route,body,origin='https://totalelitecoaching.com',type='application/json')=>fetch(base+'/api/coach/password/'+route,{method:'POST',headers:{Origin:origin,'Content-Type':type},body:JSON.stringify(body)});
 assert.equal((await post('request',{email:'owner@example.test'},'https://evil.test')).status,403);assert.equal((await post('request',{email:'owner@example.test'},'https://totalelitecoaching.com','text/plain')).status,415);
 const response=await post('request',{email:'owner@example.test'});assert.equal(response.status,202);assert.equal(response.headers.get('cache-control'),'private, no-store');assert.equal((await response.json()).message,COACH_RECOVERY_MESSAGE);await service.drain();const reset=await post('reset',{token:h.token(),password:'new password',passwordConfirmation:'new password'});assert.equal(reset.status,200);assert.equal(reset.headers.get('set-cookie'),null);assert.equal(clears,1);
});
