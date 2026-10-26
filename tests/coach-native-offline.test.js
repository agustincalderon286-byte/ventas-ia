import test from 'node:test';
import assert from 'node:assert/strict';
import vm from 'node:vm';
import {readFileSync} from 'node:fs';
const app=readFileSync(new URL('../public/coach/app.js',import.meta.url),'utf8');
const viewer=readFileSync(new URL('../public/coach/document-viewer.js',import.meta.url),'utf8');
const section=(start,end)=>app.slice(app.indexOf(start),app.indexOf(end));
test('native catalog rendering never starts a download; only explicit save prepares catalogs',async()=>{
  const session={pages:[{}],page:1,total:1,url:'/catalog',abort:{signal:{aborted:false}}};
  let renders=0,downloads=0;const button={};
  const context=vm.createContext({active:session,window:{TecNativeOffline:{prepareCatalogs:async()=>{downloads++;}}},
    dialog:{querySelector:()=>button},syncControls(){},status(){},renderImage:async()=>{renders++;return true;}});
  const render=vm.runInContext(`(${viewer.slice(viewer.indexOf('async function render(session)'),viewer.indexOf('async function saveOffline(session)')).trim()})`,context);
  const save=vm.runInContext(`(${viewer.slice(viewer.indexOf('async function saveOffline(session)'),viewer.indexOf('async function open(')).trim()})`,context);
  await render(session);assert.equal(renders,1);assert.equal(downloads,0);
  await save(session);assert.equal(downloads,1);assert.equal(session.saved,true);assert.equal(button.disabled,false);
});
test('native queue commits pending and matching draft deletion in one transaction',async()=>{
  const ops=[];
  const queue=vm.runInNewContext(`(${section('async function queueTecOfflineRequest','async function getTecOfflinePendingCount').trim()})`,{
    getTecCachedViewer:async()=>({user:{id:'a',email:'qa@example.invalid'}}),
    tecNativeStore:async(method,operations)=>{assert.equal(method,'batch');ops.push(...operations);},
    window:{TecNativeOffline:{},dispatchEvent(){}},navigator:{},CustomEvent:class{},
  });
  await queue({id:'stable',kind:'lead',body:{accountId:'a'},draftKey:'draft',formSnapshot:{revision:7}});
  assert.equal(ops.length,2);assert.equal(ops[0].value.id,'stable');assert.equal(ops[1].op,'deleteRevision');assert.equal(ops[1].revision,7);
  await assert.rejects(queue({id:'wrong',body:{accountId:'b'},draftKey:'draft'}),/cuenta cambió/);
  assert.equal(ops.length,2);
});
test('server acknowledgement is atomically archived before the pending entry is removed',async()=>{
  let ops;
  const complete=vm.runInNewContext(`(${section('async function completeTecOfflineForm','async function recordTecOfflineFormError').trim()})`,{
    window:{TecNativeOffline:{}},tecNativeStore:async(method,operations)=>{ops=operations;}
  });
  await complete({id:'same',ownerEmail:'qa@example.invalid',kind:'lead',body:{fullName:'QA'}},{ok:true});
  assert.equal(ops[0].store,'cache');assert.equal(ops[0].value.requestId,'same');assert.equal(ops[1].op,'delete');assert.equal(ops[1].key,'same');
});
test('native recordings retain original audio after confirmed analysis and do not re-upload',async()=>{
  let recording={localId:'one',ownerEmail:'qa@example.invalid',status:'ready',consentConfirmed:true,serverSessionId:'server'},uploads=0;
  const sync=vm.runInNewContext(`(${section('async function syncTecOfflineRecordingUnlocked','async function syncTecOfflineRecordings()').trim()})`,{
    navigator:{onLine:true},window:{TecNativeOffline:{},dispatchEvent(){}},CustomEvent:class{},tecActiveRecordingIds:new Set(),
    tecOfflineGet:async()=>recording,getTecCachedViewer:async()=>({user:{email:'qa@example.invalid'}}),
    verifyTecNativeSyncOwner:async()=>{},getTecAudioTail:async()=>[],
    saveTecOfflineRecording:async value=>(recording=value),getTecOfflineAudioChunks:async()=>[{index:0,blob:{size:100,nativeKey:'bytes'}}],
    uploadTecStoredAudioChunk:async()=>{uploads++;},apiRequest:async()=>({analysis:{id:'server',status:'completed',report:{title:'QA'}}}),
    tecOfflineDelete:async()=>assert.fail('must retain original bytes')
  });
  await sync('one');assert.equal(recording.status,'synced');assert.equal(uploads,1);
  assert.equal(await sync('one'),null);assert.equal(uploads,1);
});
test('wrong live session prevents native sync without touching pending copies',async()=>{
  const verify=vm.runInNewContext(`(${section('async function verifyTecNativeSyncOwner','window.prepareTecNativeOffline').trim()})`,{
    window:{TecNativeOffline:{}},apiRequest:async()=>({authenticated:true,user:{email:'other@example.invalid'}})
  });
  await assert.rejects(verify('qa@example.invalid'),e=>e.status===401);
});

test('lost response after server commit retries the same ID and synchronizes exactly once',async()=>{
  const queued=new Map([['stable',{id:'stable',kind:'lead',ownerEmail:'qa@example.invalid',url:'/api/coach/leads',body:{fullName:'QA'}}],
    ['other',{id:'other',kind:'lead',ownerEmail:'other@example.invalid',body:{fullName:'Other account'}}]]);
  const server=new Map(),sent=[],receipts=[];let first=true;
  const context=vm.createContext({navigator:{onLine:true},window:{TecNativeOffline:{},dispatchEvent(){}},CustomEvent:class{},tecOutboxFlushPromise:null,
    getTecCachedViewer:async()=>({user:{email:'qa@example.invalid'}}),verifyTecNativeSyncOwner:async()=>{},
    tecOfflineGetAll:async()=>[...queued.values()],getTecOfflinePendingCount:async()=>queued.size,
    async fetch(url,config){const id=config.headers['X-TEC-Request-ID'];sent.push(id);if(!server.has(id))server.set(id,{lead:{id:'server-lead'}});
      if(first){first=false;throw new TypeError('Response lost after commit');}return{ok:true,json:async()=>server.get(id)};},
    completeTecOfflineForm:async(item,data)=>{receipts.push(data);queued.delete(item.id);},
    recordTecOfflineFormError:async()=>{},syncTecOfflineRecordings:async()=>({synced:0})
  });
  const flush=vm.runInContext(`(${section('async function flushTecOfflineOutbox','function initTecOfflineStatus').trim()})`,context);
  await flush();assert.equal(queued.has('stable'),true);assert.equal(server.size,1);
  await flush();await flush();assert.deepEqual(sent,['stable','stable']);assert.equal(server.size,1);
  assert.equal(receipts.length,1);assert.equal(queued.has('stable'),false);assert.equal(queued.has('other'),true);
});
