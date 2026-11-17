import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import vm from 'node:vm';
const controller=fs.readFileSync(new URL('../public/coach/team-chat-push.js',import.meta.url),'utf8');
const worker=fs.readFileSync(new URL('../public/coach/team-chat-push-worker.js',import.meta.url),'utf8');
const turn=async()=>{for(let i=0;i<60;i++)await Promise.resolve();};
class Element{constructor(){this.children=[];this.attributes={};this.events=new Map();this.textContent='';}append(...nodes){this.children.push(...nodes);}setAttribute(k,v){this.attributes[k]=v;}addEventListener(k,fn){this.events.set(k,fn);}removeEventListener(k){this.events.delete(k);}fire(k){this.events.get(k)?.();}}
function client({ios=false,installed=false,permission='default',permissionWait,failSubscribe=false}={}){
 const root=new Element(),document={createElement:()=>new Element()};let permissions=0,subscribes=0,unsubscribes=0,subscription=null;
 const calls=[],key='BAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA';
 const Notification={permission,requestPermission(){permissions++;const value=permissionWait||Promise.resolve('granted');return value.then(p=>{Notification.permission=p;return p;});}};
 const registration={pushManager:{getSubscription:async()=>subscription,subscribe:async options=>{subscribes++;assert.equal(options.userVisibleOnly,true);subscription={options,toJSON:()=>({endpoint:'https://fcm.googleapis.com/x',keys:{auth:'local',p256dh:'local'}}),unsubscribe:async()=>{unsubscribes++;subscription=null;}};return subscription;}}};
 const window={isSecureContext:true,Notification,PushManager:function(){},matchMedia:()=>({matches:installed})};
 const navigator={userAgent:ios?'iPhone':'Android',serviceWorker:{ready:Promise.resolve(registration)}};
 vm.runInNewContext(controller,{window,document,navigator,Notification,Uint8Array,atob,setTimeout:()=>0,Promise,console});
 const request=async(url,options={})=>{calls.push({url,options});if(url.endsWith('/config'))return {accountId:'alice',roomId:'room',publicKey:key};if(url.endsWith('/unsubscribe'))return {enabled:false,anyEnabled:false};if(url.endsWith('/subscribe')){if(failSubscribe)throw new Error('server unavailable');return {enabled:true};}return {enabled:false};};
 const instance=window.TecTeamChatPush.init({root,userId:'alice',request});const wrap=root.children[0],button=wrap.children[0],status=wrap.children[1];
 return {instance,button,status,calls,get permissions(){return permissions;},get subscribes(){return subscribes;},get unsubscribes(){return unsubscribes;}};
}
test('visiting TEC never asks permission or subscribes; only the explicit button can activate alerts',async()=>{
 const f=client();await turn();assert.equal(f.permissions,0);assert.equal(f.subscribes,0);f.button.fire('click');assert.equal(f.permissions,1,'permission request stays directly in the click');await turn();assert.equal(f.subscribes,1);assert.equal(f.button.attributes['aria-pressed'],'true');assert.ok(f.calls.find(c=>c.url.endsWith('/subscribe')));f.instance.destroy();
});
test('iPhone outside Home Screen gives setup steps without requesting permission',async()=>{
 const f=client({ios:true});await turn();f.button.fire('click');await turn();assert.equal(f.permissions,0);assert.equal(f.subscribes,0);assert.match(f.status.textContent,/pantalla de inicio/);f.instance.destroy();
});
test('denied permission or a failed registration cannot claim notifications are active',async()=>{
 const denied=client({permission:'denied'});await turn();denied.button.fire('click');await turn();assert.equal(denied.subscribes,0);assert.equal(denied.button.attributes['aria-pressed'],'false');denied.instance.destroy();
 const failed=client({failSubscribe:true});await turn();failed.button.fire('click');await turn();assert.equal(failed.button.attributes['aria-pressed'],'false');assert.match(failed.status.textContent,/server unavailable/);failed.instance.destroy();
});
test('disabling removes the server enrollment first; destroying during permission does not enroll a stale account',async()=>{
 const f=client();await turn();f.button.fire('click');await turn();f.button.fire('click');await turn();assert.equal(f.unsubscribes,1);assert.equal(f.button.attributes['aria-pressed'],'false');f.instance.destroy();
 let resolve;const wait=new Promise(r=>{resolve=r;});const stale=client({permissionWait:wait});await turn();stale.button.fire('click');stale.instance.destroy();resolve('granted');await turn();assert.equal(stale.subscribes,0);
});
function background({logged=true,notify=true,target='https://totalelitecoaching.com/coach/app/?openTeamChat=1&teamChatOwnerId=000000000000000000000002',existing=false}={}){
 const handlers={},notifications=[],opened=[],navigated=[],fetches=[];let focused=0,closed=0;
 const response=data=>({ok:true,json:async()=>data});
 const self={location:{origin:'https://totalelitecoaching.com'},registration:{pushManager:{getSubscription:async()=>({toJSON:()=>({endpoint:'browser-owned'})})},showNotification:async(...args)=>notifications.push(args)},clients:{matchAll:async()=>existing?[{url:'https://totalelitecoaching.com/coach/app/',navigate:async url=>navigated.push(url),focus:async()=>{focused++;}}]:[],openWindow:async url=>opened.push(url)},addEventListener(type,fn){handlers[type]=fn;}};
 const fetch=async(url,options)=>{fetches.push({url,options});if(url.endsWith('/config'))return logged?response({accountId:'current-user',roomId:'current-room'}):{ok:false};return response({notify,url:target});};
 vm.runInNewContext(worker,{self,fetch,URL,console});
 const push=async(signal={type:'tec-chat'})=>{let work;handlers.push({data:{json:()=>signal},waitUntil:value=>{work=value;}});await work;};
 const click=async()=>{let work;handlers.notificationclick({notification:{data:{type:'tec-chat',url:'https://evil.example'},close(){closed++;}},waitUntil:value=>{work=value;}});await work;};
 return {push,click,notifications,opened,navigated,fetches,get focused(){return focused;},get closed(){return closed;}};
}
test('closed-page push fetches its target only from TEC and displays a generic notification without message data',async()=>{
 const f=background();await f.push();assert.equal(f.notifications.length,1);assert.equal(f.notifications[0][0],'TEC · Chat del equipo');assert.equal(f.notifications[0][1].body,'Tienes un mensaje nuevo en tu equipo.');assert.deepEqual(JSON.parse(JSON.stringify(f.notifications[0][1].data)),{type:'tec-chat'});assert.equal(f.fetches.length,2);for(const call of f.fetches)assert.ok(call.url.startsWith('/api/coach/team-chat/push/'));
});
test('logged-out, revoked and unrelated pushes display no team notification',async()=>{
 for(const options of [{logged:false},{notify:false}]){const f=background(options);await f.push();assert.equal(f.notifications.length,0);}
 const f=background();await f.push({type:'unrelated'});assert.equal(f.fetches.length,0);
});
test('notification click revalidates the current session and opens or focuses only the authorized TEC page',async()=>{
 const f=background();await f.click();assert.match(f.opened[0],/teamChatOwnerId=000000000000000000000002/);assert.equal(f.closed,1);
 const existing=background({existing:true});await existing.click();assert.equal(existing.navigated.length,1);assert.equal(existing.focused,1);assert.equal(existing.opened.length,0);
 for(const target of ['https://evil.example/','javascript:alert(1)','/coach/app/?openTeamChat=1&teamChatOwnerId=bad']){const bad=background({target});await bad.click();assert.equal(bad.opened[0],'https://totalelitecoaching.com/coach/app/?openTeamChat=1');}
 const expired=background({logged:false});await expired.click();assert.equal(expired.opened[0],'https://totalelitecoaching.com/coach/app/?openTeamChat=1');
});
