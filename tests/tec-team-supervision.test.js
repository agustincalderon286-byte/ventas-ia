import test from 'node:test';
import assert from 'node:assert/strict';
import vm from 'node:vm';
import fs from 'node:fs';
import express from 'express';
import mongoose from 'mongoose';
import {canReadDownline,canManageTeamSeats,downlineMembers,downlineScope,analysisViewerIds} from '../src/tec-team-supervision.js';
const source=fs.readFileSync(new URL('../server.js',import.meta.url),'utf8');
const id=n=>String(n).padStart(24,'0');
const users=[
 {_id:id(1),name:'David',accountType:'owner',teamOwnerUserId:id(1),parentUserId:null},
 {_id:id(2),name:'Lorena',accountType:'seat',teamSupervisionEnabled:true,teamOwnerUserId:id(1),parentUserId:id(1)},
 {_id:id(3),name:'Veronica',accountType:'seat',teamOwnerUserId:id(1),parentUserId:id(2)},
 {_id:id(4),name:'Peer branch',accountType:'seat',teamSupervisionEnabled:true,teamOwnerUserId:id(1),parentUserId:id(1)},
 {_id:id(5),name:'Peer seller',accountType:'seat',teamOwnerUserId:id(1),parentUserId:id(4)},
 {_id:id(6),name:'Own distributor',accountType:'owner',teamOwnerUserId:id(6),parentUserId:id(3)},
 {_id:id(7),name:'Junior distributor',accountType:'leader',teamOwnerUserId:id(6),parentUserId:id(6)},
 {_id:id(8),name:'Junior seller',accountType:'seat',teamOwnerUserId:id(6),parentUserId:id(7)},
 {_id:id(9),name:'Other root',accountType:'owner',teamOwnerUserId:id(9),parentUserId:null},
 {_id:id(10),name:'Other seller',accountType:'seat',teamOwnerUserId:id(9),parentUserId:id(9)}
].map(u=>({...u,email:u.name.replaceAll(' ','').toLowerCase()+'@example.invalid',seatStatus:'active'}));
function matches(row,q={}){return Object.entries(q).every(([key,value])=>key==='$and'?value.every(part=>matches(row,part)):key==='$or'?value.some(part=>matches(row,part)):value&&typeof value==='object'&&'$in'in value?value.$in.some(x=>String(x)===String(row[key])):String(row[key])===String(value));}
function model(rows){
 const query=value=>{const result={select(){return result;},sort(){return result;},limit(){return result;},lean:async()=>structuredClone(value),then:(a,b)=>Promise.resolve(structuredClone(value)).then(a,b)};return result;};
 return {rows,find:q=>query(rows.filter(r=>matches(r,q))),findOne:q=>query(rows.find(r=>matches(r,q))||null),countDocuments:async q=>rows.filter(r=>matches(r,q)).length,
  findOneAndDelete:async q=>{const n=rows.findIndex(r=>matches(r,q));return n<0?null:rows.splice(n,1)[0];},
  aggregate:async pipeline=>{const subset=rows.filter(r=>matches(r,pipeline[0].$match));const field=pipeline[1].$group._id.slice(1),counts=new Map();for(const r of subset)counts.set(r[field],(counts.get(r[field])||0)+1);return [...counts].map(([_id,total])=>({_id,total}));}};
}
function functionSource(name){const start=source.indexOf('async function '+name+'(')>=0?source.indexOf('async function '+name+'('):source.indexOf('function '+name+'(');assert.ok(start>=0,name);const re=/\n(?:async )?function \w+\(/g;re.lastIndex=start+15;return source.slice(start,re.exec(source)?.index||source.length);}
test('every level sees descendants only, regardless of billing owner, never siblings, ancestors or peers',async()=>{
 const m=model(structuredClone(users));
 assert.equal(canReadDownline(users[1]),true);assert.equal(canManageTeamSeats(users[1]),false);assert.equal(canReadDownline(users[2]),false);
 assert.deepEqual((await downlineMembers(users[1],m)).map(u=>u._id).sort(),[3,6,7,8].map(id));
 assert.deepEqual((await downlineMembers(users[3],m)).map(u=>u._id),[id(5)]);
 assert.deepEqual((await downlineMembers(users[6],m)).map(u=>u._id),[id(8)]);
 assert.deepEqual((await analysisViewerIds(users[1],m)).sort(),[2,3,6,7,8].map(id));
 assert.deepEqual(await analysisViewerIds(users[2],m),[id(3)]);
 assert.deepEqual((await downlineMembers(users[0],m)).map(u=>u._id).sort(),[2,3,4,5,6,7,8].map(id));
 assert.deepEqual((await downlineMembers(users[8],m)).map(u=>u._id),[id(10)]);
});
test('cycles, oversized networks and malformed adapter results fail closed before returning a scope',async()=>{
 const corrupt=structuredClone(users);corrupt[0].parentUserId=id(2);
 await assert.rejects(downlineScope(users[1],model(corrupt)),/cycle/);
 await assert.rejects(downlineMembers(users[1],model(users),{maxMembers:1}),/limit/);
 await assert.rejects(downlineMembers(users[1],model(users),{maxDepth:1}),/limit/);
 await assert.rejects(downlineMembers(users[1],{find:()=>({select:()=>({lean:async()=>[users[8]]})})}),/invalid/);
});
async function httpFixture(t){
 const app=express();app.use(express.json());
 const u=model(structuredClone(users)),workspace=user=>['seat','leader'].includes(user.accountType)?user.teamOwnerUserId:user._id;
 const records=[3,4,5,6,8,10].map(n=>({_id:id(100+n),ownerUserId:workspace(users[n-1]),generatedByUserId:id(n),fullName:'Synthetic '+n,programType:'4_en_14',privateMarker:n===4?'PEER_SECRET':String(n)}));
 const analyses=[1,2,3,4,5,6,8,10].map(n=>({_id:id(200+n),ownerUserId:id(n),status:'completed',report:{title:'Analysis '+n,privateMarker:n===4?'PEER_SECRET':String(n)},transcript:'Transcript '+n}));
 const requireActive=async(req,res)=>{const user=u.rows.find(x=>x._id===req.get('x-user'));if(!user){res.status(401).json({error:'login'});return null;}return {user};};
 const context={app,console,mongoose,canReadDownline,downlineScope,analysisViewerIds,requireCoachActivo:requireActive,
  esCoachTeamManager:canManageTeamSeats,resolverCoachOwnerUserId:workspace,CoachUser:u,
  CoachDistributorProfile:model([]),CoachLeadInbox:model(structuredClone(records)),CoachProgramSheet:model(structuredClone(records)),
  CoachHealthSurvey:model(structuredClone(records)),CoachRecruitmentApplication:model(structuredClone(records)),CoachDemoAnalysis:model(analyses),
  responderCoachError:(res,status,error)=>res.status(status).json({error}),
  limpiarCoachTeamSeat:(user,profile,counts)=>({...user,id:String(user._id),counts,chef:{privateCode:'not-needed'},contactShare:{code:'not-needed'}}),
  construirCoachTeamSummary:seats=>({totalSeats:seats.length}),limpiarCoachInboxLead:x=>x,limpiarCoachProgramSheet:x=>x,
  limpiarCoachHealthSurvey:x=>x,limpiarCoachRecruitmentApplication:x=>x,
  limpiarCoachDemoAnalysis:(doc,{includeDetail=false,viewerUserId}={})=>({...doc,report:includeDetail?doc.report:{title:doc.report.title},canDelete:String(viewerUserId)===String(doc.ownerUserId)})};
 vm.createContext(context);
 for(const name of ['requireCoachTeamViewer','requireCoachTeamManager','listarCoachDemoAnalysisUserIds'])vm.runInContext(functionSource(name),context);
 const chunks=[
  source.slice(source.indexOf('app.get("/api/coach/team",'),source.indexOf('app.post("/api/coach/team/seats",')),
  source.slice(source.indexOf('app.get("/api/coach/demo-analyses",'),source.indexOf('// Serialize changes to a recording')),
  source.slice(source.indexOf('app.post("/api/coach/team/seats",'),source.indexOf('app.get("/api/coach/territories"')),
  source.slice(source.indexOf('app.delete("/api/coach/demo-analyses/:id"'),source.indexOf('app.get("/api/coach/demo-outcomes"'))
 ];
 for(const chunk of chunks)vm.runInContext(chunk,context);
 const server=app.listen(0,'127.0.0.1');await new Promise(resolve=>server.once('listening',resolve));t.after(()=>new Promise(resolve=>server.close(resolve)));
 const request=async(path,{user=2,method='GET',body}={})=>{const r=await fetch(`http://127.0.0.1:${server.address().port}`+path,{method,headers:{'x-user':id(user),'Content-Type':'application/json'},...(body?{body:JSON.stringify(body)}:{})});return {status:r.status,data:await r.json()};};
 return {request,analyses};
}
test('actual team and analysis handlers isolate the whole downline, including independent distributor workspaces',async t=>{
 const {request}=await httpFixture(t);
 const team=await request('/api/coach/team');assert.equal(team.status,200);assert.deepEqual(team.data.seats.map(u=>u._id).sort(),[3,6,7,8].map(id));assert.equal(team.data.permissions.canManageSeats,false);assert.ok(team.data.seats.every(u=>!u.chef&&!u.contactShare));
 for(const n of [3,6,8]){const r=await request('/api/coach/team/seats/'+id(n)+'/activity');assert.equal(r.status,200);assert.equal(r.data.leads.length,1);assert.equal(r.data.leads[0].generatedByUserId,id(n));assert.equal(r.data.programSheets.length,1);assert.deepEqual(r.data.surveys,[]);assert.deepEqual(r.data.applications,[]);assert.equal(r.data.permissions.canViewForms,false);}
 for(const n of [1,2,4,5,9,10])assert.equal((await request('/api/coach/team/seats/'+id(n)+'/activity')).status,404);
 const list=await request('/api/coach/demo-analyses');assert.equal(list.status,200);assert.ok(!JSON.stringify(list.data).includes('PEER_SECRET'));
 for(const n of [3,6,8]){const r=await request('/api/coach/demo-analyses/'+id(200+n));assert.equal(r.status,200);assert.equal(r.data.analysis.canDelete,false);assert.equal(r.data.analysis.transcript,'Transcript '+n);}
 for(const n of [1,4,5,10])assert.equal((await request('/api/coach/demo-analyses/'+id(200+n))).status,404);
 const peer=await request('/api/coach/team',{user:4});assert.deepEqual(peer.data.seats.map(x=>x._id),[id(5)]);
 const ordinary=await request('/api/coach/team',{user:3});assert.equal(ordinary.status,403);
 const david=await request('/api/coach/team',{user:1});assert.equal(david.data.seats.length,7);
});
test('a read-only upline cannot grant permissions, create accounts, reset passwords, switch sessions or delete a downline report',async t=>{
 const {request,analyses}=await httpFixture(t);
 for(const [method,path]of [['POST','/api/coach/team/seats'],['PATCH','/api/coach/team/seats/'+id(3)],['POST','/api/coach/team/seats/'+id(3)+'/reset-password'],['POST','/api/coach/team/seats/'+id(3)+'/switch']]){
  assert.equal((await request(path,{method,body:{teamSupervisionEnabled:true,accountType:'leader',seatStatus:'active'}})).status,403);
 }
 const r=await request('/api/coach/demo-analyses/'+id(203),{method:'DELETE'});assert.equal(r.data.deleted,false);assert.ok(analyses.some(a=>a._id===id(203)));
 const serverGuard=functionSource('requireControlTowerAccess');assert.match(serverGuard,/usuarioPuedeVerTorreControl/);assert.doesNotMatch(serverGuard,/teamSupervisionEnabled|canReadDownline/);
 const workspace=functionSource('construirCoachWorkspaceQuery');assert.match(workspace,/query.generatedByUserId = userDoc._id/);assert.doesNotMatch(workspace,/teamSupervisionEnabled/);
});
test('the actual navigation opens Equipo for the supervisor while keeping creation, billing, results administration and Torre hidden',()=>{
 const client=fs.readFileSync(new URL('../public/coach/app.js',import.meta.url),'utf8');
 const extract=name=>{const start=client.indexOf('function '+name+'(');assert.ok(start>=0);const re=/\nfunction \w+\(/g;re.lastIndex=start+15;return client.slice(start,re.exec(client)?.index||client.length);};
 const node=(dataset={},attrs=[])=>({dataset,hidden:true,handlers:{},textContent:'Equipo',hasAttribute:a=>attrs.includes(a),classList:{toggle(){}},setAttribute(){},addEventListener(name,handler){this.handlers[name]=handler;}});
 const tabs=['cierre','actividad','mapa','audiolibros','equipo'].map(coachWorkspaceTab=>node({coachWorkspaceTab}));
 const summary=node({coachWorkspaceSection:'equipo'},['data-team-viewer-only']),list=node({coachWorkspaceSection:'equipo'},['data-team-viewer-only']),form=node({coachWorkspaceSection:'equipo'},['data-team-manager-only']),results=node({coachWorkspaceSection:'equipo'},['data-team-manager-only']);
 const billing=node(),tower=node(),chef=node();
 const selectors={'[data-coach-workspace-tab]':tabs,'[data-team-manager-only]':[form,results,billing],'[data-team-viewer-only]':[tabs[4],summary,list],'[data-control-tower-link]':[tower],'[data-tec-chef-admin-link]':[chef],'[data-coach-workspace-section]':[summary,list,form,results]};
 const context={document:{documentElement:{dataset:{}},querySelectorAll:q=>selectors[q]||[],querySelector:()=>null},window:{sessionStorage:{getItem:()=> 'equipo',setItem(){}},location:{hash:''},dispatchEvent(){},addEventListener(){}},getCoachPortalMode:()=> 'coach',COACH_FOCUSED_WORKSPACE_TAB:'cierre',COACH_WORKSPACE_TAB_KEY:'synthetic',CustomEvent:class{}};
 vm.createContext(context);vm.runInContext(extract('syncCoachManagerUi'),context);vm.runInContext(extract('initCoachWorkspaceTabs'),context);
 context.syncCoachManagerUi({accountType:'seat',managesTeam:false,canViewTeam:true,canViewControlTower:false});
 assert.equal(tabs[4].hidden,false);assert.equal(tower.hidden,true);assert.equal(chef.hidden,true);assert.equal(billing.hidden,true);
 context.initCoachWorkspaceTabs();assert.equal(summary.hidden,false);assert.equal(list.hidden,false);assert.equal(form.hidden,true);assert.equal(results.hidden,true);
});
