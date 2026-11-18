import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import test from 'node:test';
import vm from 'node:vm';

const source = path => readFileSync(new URL('../' + path, import.meta.url), 'utf8');
const worker = source('public/coach/sw.js');

test('login is POST-only and stays disabled until the submit handler is installed', () => {
  const page = source('public/coach/login/index.html');
  assert.match(page, /<form[^>]*method="post"[^>]*action="\/coach\/login\/"[^>]*>/);
  assert.match(page, /<button[^>]*type="submit"[^>]*disabled/);
  const app = source('public/coach/app.js').split('async function initLoginPage()')[1].split('function formatCoachDemoRecordingTime')[0];
  assert.ok(app.indexOf('event.preventDefault()') < app.indexOf('submit.disabled = false'));
  assert.ok(app.indexOf('submit.disabled = false') < app.indexOf('const me = await'));
});

test('a closed app can resume an interrupted form only for the same account, with one receipt', async () => {
  const queued = new Map([['visit-1',{id:'visit-1', ownerEmail:'rep@example.test', ownerAccountId:'one',
    body:{accountId:'one'},kind:'lead',url:'/api/coach/leads'}]]);
  const receipts = new Set();
  let signedIn = 'one', transport = 'offline';
  const fetch = async (url,options) => {
    if (url === '/api/coach/me') return {ok:true,json:async()=>({user:{id:signedIn,email:'rep@example.test'}})};
    if (transport === 'offline') throw new TypeError('offline');
    // The server accepted this request but the gateway could not deliver its response.
    receipts.add(options.headers['X-TEC-Request-ID']);
    if (transport === 'gateway error') return {ok:false,status:503,json:async()=>({})};
    return {ok:true,json:async()=>({ok:true})};
  };
  function wake() {
    const events = {};
    const context = vm.createContext({self:{addEventListener:(k,fn)=>{events[k]=fn;}},
      fetch, readRows:async()=>[...queued.values()], finish:async item=>queued.delete(item.id)});
    vm.runInContext(worker,context);
    vm.runInContext('getAllOutboxItems=readRows;completeOfflineForm=finish;recordOfflineFormError=async()=>{};broadcastOfflineStatus=async()=>{};',context);
    let result;
    events.sync({tag:'tec-sync-outbox',waitUntil(p){result=p;}});
    return result;
  }
  await assert.rejects(wake(),/offline/);
  signedIn='two';transport='ok';await wake();assert.equal(receipts.size,0);
  signedIn='one';transport='gateway error';await assert.rejects(wake(),/reintentar/);
  assert.equal(queued.size,1);
  transport='ok';await wake();await wake();
  assert.equal(queued.size,0);assert.deepEqual([...receipts],['visit-1']);
});

test('a local logout leaves the private screen while offline and requests revocation upon reconnect', async () => {
  const events = {}, sent = [], page = {hidden:false}, redirects = [];
  const document = {cookie:'',querySelector:()=>page,addEventListener(){}};
  const location = {protocol:'https:',replace:url=>redirects.push(url)};
  const navigator = {onLine:false};
  const window = {location,addEventListener:(type,fn)=>{events[type]=fn;}};
  vm.runInNewContext(source('public/coach/session-lock.js'), {window,document,location,navigator,
    crypto:{randomUUID:()=> 'qa-event'},localStorage:{setItem(){}},fetch:async url=>sent.push(url)});
  window.TecSessionLock.lock();assert.equal(window.TecSessionLock.locked(),true);
  events.pageshow({persisted:true});
  assert.equal(page.hidden,true);assert.deepEqual(redirects,['/coach/login/']);assert.equal(sent.length,0);
  navigator.onLine=true;await events.online();assert.deepEqual(sent,['/api/coach/session/revoke-local']);
});
