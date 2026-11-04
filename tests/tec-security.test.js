import test from 'node:test';
import assert from 'node:assert/strict';
import vm from 'node:vm';
import crypto from 'node:crypto';
import {readFileSync} from 'node:fs';
import express from 'express';
import 'express-async-errors';
import {createCoachRequestGuard,scopeCoachChat,securityHeaders,securityErrorHandler} from '../src/tec-security.js';
import {validateDestination,publicAddress,publicLookup,secureWebhookFetch} from '../src/secure-outbound.js';
const source=readFileSync(new URL('../server.js',import.meta.url),'utf8'),client=readFileSync(new URL('../public/coach/app.js',import.meta.url),'utf8');
function fn(text,name){const re=new RegExp('(?:^|\\n)(?:async )?function '+name+'\\('),m=re.exec(text);assert.ok(m,name);const start=m.index+(text[m.index]==='\n'?1:0),end=/\n(?:async )?function \w+\(/g;end.lastIndex=start+10;return text.slice(start,end.exec(text)?.index||text.length);}
async function fixture(t,{authId='A',authenticate,guard=true}={}){
 const app=express();app.use(securityHeaders);app.use(express.json());app.use(express.urlencoded({extended:true}));let writes=0;
 if(guard)app.use(['/api/coach','/api/control','/chat'],createCoachRequestGuard({requireActive:authenticate||(async()=>({user:{_id:authId}}))}));
 app.post('/api/coach/login',(_q,r)=>{writes++;r.json({ok:true});});app.post('/api/coach/signup-checkout',(_q,r)=>{writes++;r.json({ok:true});});app.post('/api/coach/leads',(_q,r)=>{writes++;r.json({ok:true});});app.post('/api/coach/demo-analyses/fixture/chunks',(_q,r)=>{writes++;r.json({ok:true});});
 app.get('/throw',async()=>{throw Error('NEVER-EXPOSE-SECRET');});app.get('/ok',(_q,r)=>r.json({ok:true}));app.use(securityErrorHandler);
 const server=app.listen(0,'127.0.0.1');await new Promise(resolve=>server.once('listening',resolve));t.after(()=>new Promise(resolve=>server.close(resolve)));
 return{base:`http://127.0.0.1:${server.address().port}`,get writes(){return writes;},app};
}
const json={'Content-Type':'application/json',Origin:'https://totalelitecoaching.com','Sec-Fetch-Site':'same-origin'};
const validLogin=JSON.stringify({email:'fictitious@example.test',password:'fictitious-password'});
test('malformed cookies cannot throw, poison prototypes or erase another valid cookie',()=>{
 const context={};vm.createContext(context);vm.runInContext(fn(source,'parsearCookies'),context);
 const parsed=context.parsearCookies('bad=%; safe=ok; __proto__=inert; another=%E0%A4');assert.equal(parsed.safe,'ok');assert.equal(parsed.bad,undefined);assert.equal(Object.getPrototypeOf(parsed),null);
});
test('auth rejects a malformed token before querying MongoDB',async()=>{
 const context={CoachSession:{findOne(){throw Error('must not query');}}};vm.createContext(context);vm.runInContext(fn(source,'obtenerCoachAuthPorToken'),context);
 assert.equal((await context.obtenerCoachAuthPorToken('%bad')).user,null);assert.equal((await context.obtenerCoachAuthPorToken({})).user,null);
});
test('Express 4 catches a rejected async route and remains available after it',async t=>{
 const f=await fixture(t);const error=await fetch(f.base+'/throw');assert.equal(error.status,503);assert.doesNotMatch(await error.text(),/NEVER-EXPOSE|stack/);assert.equal((await fetch(f.base+'/ok')).status,200);
});
test('malformed cookie against the actual private page handler stays alive',async t=>{
 const f=await fixture(t,{guard:false});const marker='app.get(["/coach/app", "/coach/app/"], async (req, res) => {';const start=source.indexOf(marker)+marker.length,end=source.indexOf('\napp.get(["/coach/telemarketing"',start);const body=source.slice(start,end).replace(/\n\}\);\s*$/,'');
 const context={};vm.createContext(context);vm.runInContext(fn(source,'parsearCookies'),context);
 const handler=vm.runInNewContext(`async(req,res)=>{${body}}`,{obtenerCoachAuth:async req=>{context.parsearCookies(req.headers.cookie||'');return{user:null};}});
 // Additional route-specific error handler is needed because fixture's final handler precedes this test route.
 f.app.get('/coach/app/',handler);f.app.use(securityErrorHandler);
 const result=await fetch(f.base+'/coach/app/',{headers:{Cookie:'synthetic=%'},redirect:'manual'});assert.equal(result.status,302);assert.equal((await fetch(f.base+'/ok')).status,200);
});
test('foreign-origin form login is rejected before authentication or session creation',async t=>{
 const f=await fixture(t);const r=await fetch(f.base+'/api/coach/login',{method:'POST',headers:{Origin:'https://untrusted.example','Content-Type':'application/x-www-form-urlencoded'},body:'email=a%40example.test&password=fictitious'});assert.equal(r.status,403);assert.equal(f.writes,0);
});
test('same-origin JSON login and no-origin native JSON clients remain compatible',async t=>{
 const f=await fixture(t);for(const headers of [json,{'Content-Type':'application/json'}]){const r=await fetch(f.base+'/api/coach/login',{method:'POST',headers,body:validLogin});assert.equal(r.status,200);}assert.equal(f.writes,2);
});
test('cross-site metadata, null origins and simple no-origin forms are rejected',async t=>{
 const f=await fixture(t);for(const headers of [{...json,'Sec-Fetch-Site':'cross-site'},{...json,Origin:'null'},{'Content-Type':'application/x-www-form-urlencoded'}]){const r=await fetch(f.base+'/api/coach/login',{method:'POST',headers,body:validLogin});assert.equal(r.status,403);}assert.equal(f.writes,0);
});
test('password length and email validation bound the expensive login path',async t=>{
 const f=await fixture(t);for(const body of [{email:'invalid',password:'valid-password'},{email:'test@example.test',password:'x'.repeat(129)}]){const r=await fetch(f.base+'/api/coach/login',{method:'POST',headers:json,body:JSON.stringify(body)});assert.equal(r.status,400);}assert.equal(f.writes,0);
});
test('audio upload requests retain their native content type and origin support',async t=>{
 const f=await fixture(t);const r=await fetch(f.base+'/api/coach/demo-analyses/fixture/chunks',{method:'POST',headers:{Origin:'https://totalelitecoaching.com','Content-Type':'audio/mp4'},body:new Uint8Array([1,2,3])});assert.equal(r.status,200);
});
test('queued lead A cannot write under account B, even with a valid B cookie',async t=>{
 const f=await fixture(t,{authId:'B'});const r=await fetch(f.base+'/api/coach/leads',{method:'POST',headers:{...json,'X-TEC-Request-ID':'pending-001','X-TEC-Account-ID':'A'},body:JSON.stringify({fullName:'Synthetic A',accountId:'A'})});assert.equal(r.status,403);assert.equal(f.writes,0);
});
test('scope in the JSON body protects existing native pending forms without a new header',async t=>{
 const f=await fixture(t);const r=await fetch(f.base+'/api/coach/leads',{method:'POST',headers:{...json,'X-TEC-Request-ID':'pending-001'},body:JSON.stringify({accountId:'A'})});assert.equal(r.status,200);assert.equal(f.writes,1);
});
test('conflicting account header and body, or a pending request with no scope, fail closed',async t=>{
 const f=await fixture(t,{authId:'B'});for(const headers of [{...json,'X-TEC-Request-ID':'pending-001'},{...json,'X-TEC-Request-ID':'pending-002','X-TEC-Account-ID':'B'}]){const r=await fetch(f.base+'/api/coach/leads',{method:'POST',headers,body:JSON.stringify(headers['X-TEC-Account-ID']?{accountId:'A'}:{})});assert.equal(r.status,403);}assert.equal(f.writes,0);
});
test('signup abuse limit rejects repeated email attempts without executing another signup',async t=>{
 const f=await fixture(t);for(let i=0;i<6;i++){const r=await fetch(f.base+'/api/coach/signup-checkout',{method:'POST',headers:json,body:validLogin});assert.equal(r.status,i<5?200:429);}assert.equal(f.writes,5);
});
test('server chat namespace is deterministic per user and distinct across accounts',()=>{
 assert.equal(scopeCoachChat('A','session-123'),scopeCoachChat('A','session-123'));assert.notEqual(scopeCoachChat('A','session-123'),scopeCoachChat('B','session-123'));assert.notEqual(scopeCoachChat('A','session-123'),scopeCoachChat('A','session-124'));
 const auth=source.indexOf('sessionId = scopeCoachChat(coachAuth.user._id');assert.ok(auth>source.indexOf('app.post("/chat", async'));assert.ok(auth<source.indexOf('await asegurarSesionRedisCargada(sessionId);',source.indexOf('app.post("/chat", async')));
});
test('client rotates the chat on A-to-B or logout while preserving offline storage',async()=>{
 const map=new Map(),removed=[];let n=0;const context={window:{sessionStorage:{getItem:k=>map.get(k),setItem:(k,v)=>map.set(k,v),removeItem:k=>map.delete(k)}},COACH_CHAT_OWNER_KEY:'owner',COACH_CHAT_SESSION_KEY:'chat',TEC_VIEWER_CACHE_KEY:'viewer',buildCoachId:()=>String(++n),tecOfflineDelete:async(...args)=>removed.push(args)};
 vm.createContext(context);vm.runInContext(fn(client,'resetCoachChatForAccount')+fn(client,'getCoachChatSessionId')+fn(client,'clearTecCachedViewer'),context);
 context.resetCoachChatForAccount('A');const first=context.getCoachChatSessionId();context.resetCoachChatForAccount('A');assert.equal(first,context.getCoachChatSessionId());context.resetCoachChatForAccount('B');assert.notEqual(first,context.getCoachChatSessionId());await context.clearTecCachedViewer();assert.equal(map.has('chat'),false);assert.deepEqual(removed,[['cache','viewer']]);
});
test('outbox worker freezes A in its header and retains the item after an account-switch rejection',async()=>{
 const sw=readFileSync(new URL('../public/coach/sw.js',import.meta.url),'utf8'),start=sw.indexOf('async function flushOutboxInServiceWorker()'),end=sw.indexOf('\nself.addEventListener("sync"',start);let deleted=false,sent;
 const context={fetch:async(url,options)=>{if(url==='/api/coach/me')return{ok:true,json:async()=>({user:{email:'a@example.test',id:'A'}})};sent=options;return{ok:false,status:403,json:async()=>({error:'account changed'})};},getAllOutboxItems:async()=>[{id:'pending-001',ownerEmail:'a@example.test',kind:'lead',url:'/api/coach/leads',body:{}}],deleteOutboxItem:async()=>{deleted=true;},recordOfflineFormError:async()=>{},broadcastOfflineStatus:async()=>{}};
 vm.createContext(context);vm.runInContext(sw.slice(start,end),context);await context.flushOutboxInServiceWorker();assert.equal(sent.headers['X-TEC-Account-ID'],'A');assert.equal(deleted,false);
});
test('legacy lead memory is off by default, before any collection lookup',async()=>{
 const context={process:{env:{}},usuarioPuedeVerTorreControl:()=>true,obtenerLeadMemoryCollections:()=>{throw Error('must not read legacy DB');}};vm.createContext(context);vm.runInContext(fn(source,'obtenerMemoriaLeadRelacionada'),context);const result=await context.obtenerMemoriaLeadRelacionada({mode:'coach',coachUser:{_id:'A'}});assert.equal(result.leadContext,null);
});
test('private cache and staged browser policies are present without enforcing CSP over tools',async t=>{
 const f=await fixture(t);const r=await fetch(f.base+'/api/coach/login',{method:'POST',headers:json,body:validLogin});assert.equal(r.headers.get('cache-control'),'private, no-store');assert.ok(r.headers.get('content-security-policy-report-only'));assert.match(r.headers.get('content-security-policy'),/object-src 'none'/);assert.doesNotMatch(r.headers.get('content-security-policy'),/script-src|default-src/);
});
test('password hashing is asynchronous and verifies the existing salt/hash format',async()=>{
 const context={crypto,Buffer};vm.createContext(context);vm.runInContext(fn(source,'crearPasswordSeguro')+fn(source,'verificarPasswordSeguro'),context);const pending=context.crearPasswordSeguro('Fictitious-password-17');assert.equal(typeof pending.then,'function');const value=await pending;assert.equal(await context.verificarPasswordSeguro('Fictitious-password-17',value.salt,value.hash),true);assert.equal(await context.verificarPasswordSeguro('wrong',value.salt,value.hash),false);
});
test('private and special IP ranges and non-HTTPS URLs cannot be destinations',()=>{
 for(const address of ['127.0.0.1','10.1.2.3','172.16.0.1','192.168.1.1','169.254.169.254','100.64.1.1','::1','fc00::1','::ffff:127.0.0.1'])assert.equal(publicAddress(address),false,address);
 for(const url of ['http://public.example','https://127.0.0.1/','https://[::1]/','https://user:password@public.example','https://public.example:8443','https://x.local'])assert.throws(()=>validateDestination(url));
 assert.ok(publicAddress('8.8.8.8'));assert.ok(publicAddress('2606:4700:4700::1111'));assert.equal(validateDestination('https://hooks.example/path').hostname,'hooks.example');
});
test('optional hostname allowlist has domain boundaries',()=>{
 assert.equal(validateDestination('https://hooks.example/a',{hosts:'hooks.example'}).hostname,'hooks.example');assert.throws(()=>validateDestination('https://hooks.example.attacker.test',{hosts:'hooks.example'}));assert.throws(()=>validateDestination('https://evil-example',{hosts:'*.example'}));
});
test('DNS lookup rejects mixed public/private results and rechecks each connection',async()=>{
 let n=0;const lookup=publicLookup((_host,_opts,cb)=>cb(null,[{address:++n===1?'8.8.8.8':'127.0.0.1',family:4}]));
 const run=()=>new Promise((resolve,reject)=>lookup('hooks.example',{},(error,value)=>error?reject(error):resolve(value)));
 assert.equal(await run(),'8.8.8.8');await assert.rejects(run(),/destino/);
 const mixed=publicLookup((_h,_o,cb)=>cb(null,[{address:'8.8.8.8',family:4},{address:'10.0.0.1',family:4}]));await assert.rejects(new Promise((resolve,reject)=>mixed('hooks.example',{all:true},(error,result)=>error?reject(error):resolve(result))));
});
test('redirect to a private host is blocked before a second request or any payload is sent there',async()=>{
 const calls=[];await assert.rejects(secureWebhookFetch('https://hooks.example',{body:'synthetic'},{dispatcher:{},fetchImpl:async(url)=>{calls.push(url);return{status:307,headers:new Headers({location:'https://127.0.0.1/internal'}),body:{cancel:async()=>{}}};}}));assert.equal(calls.length,1);
});
test('normal HTTPS delivery and public 303 redirect preserve fetch semantics without retaining response content',async()=>{
 const calls=[];const result=await secureWebhookFetch('https://hooks.example',{method:'POST',body:'synthetic'},{dispatcher:{},fetchImpl:async(url,options)=>{calls.push({url,options});return calls.length===1?{status:303,headers:new Headers({location:'https://results.example/ok'}),body:{cancel:async()=>{}}}:{status:200,ok:true,body:{cancel:async()=>{}}};}});assert.equal(result.ok,true);assert.equal(calls[1].options.method,'GET');assert.equal(calls[1].options.body,undefined);assert.equal(calls[0].options.redirect,'manual');
});

test('the actual administrator guard requires step-up only after role authorization',async()=>{
 const runSource=fn(source,'requireControlTowerAccess');
 for(const [admin,needs,status]of[[false,false,403],[true,true,403],[true,false,0]]){
  const auth={user:{_id:'A'},session:{}};let called=0,code=0,payload;
  const context={requireCoachUser:async()=>auth,usuarioPuedeVerTorreControl:()=>admin,responderCoachError:(r,c,msg)=>{code=c;},coachAdminPasskeys:{needsVerification:async()=>{called++;return needs;}}};vm.createContext(context);vm.runInContext(runSource,context);
  const result=await context.requireControlTowerAccess({},{status(c){code=c;return this;},json(value){payload=value;}});assert.equal(code,status);assert.equal(called,admin?1:0);if(needs&&admin)assert.equal(payload.mfaRequired,true);if(!status)assert.equal(result,auth);
 }
});

test('public signup cannot create a reserved administrative or trial identity',async()=>{
 const marker='app.post("/api/coach/signup-checkout", async (req, res) => {';const start=source.indexOf(marker)+marker.length,end=source.indexOf('\napp.post("/api/coach/login"',start);const body=source.slice(start,end).replace(/\n\}\);\s*$/,'');
 let databaseReads=0,status=0;const context={normalizarEmail:v=>v,normalizarPlanCoach:()=> 'monthly',coachTieneAccesoDePrueba:()=>false,passwordEsValido:()=>true,stripeListoParaCheckout:()=>true,CONTROL_TOWER_ACCESS_EMAILS:['reserved@example.test'],CoachUser:{findOne:async()=>{databaseReads++;}},responderCoachError:(_r,code)=>status=code};const run=vm.runInNewContext(`async(req,res)=>{${body}}`,context);await run({body:{name:'Synthetic',email:'reserved@example.test',password:'fictitious-password'}},{});assert.equal(status,403);assert.equal(databaseReads,0);
});
