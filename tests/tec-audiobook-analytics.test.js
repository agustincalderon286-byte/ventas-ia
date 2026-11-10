import test from 'node:test';
import assert from 'node:assert/strict';
import vm from 'node:vm';
import { readFileSync } from 'node:fs';
import * as events from '../public/audiolibros/analytics-events.js';
import { audiobookCatalog } from '../src/tec-audiobooks.js';
import { audioCollections } from '../src/knowledge/audiobook-editorial.js';
import { audiobookGuides } from '../src/knowledge/audiobook-guides.js';

const source = readFileSync(new URL('../public/audiolibros/analytics.js', import.meta.url), 'utf8').replace(/^import[^\n]+\n/, '');
function browser({ choice = '', online = true, gpc = false, dnt = false, native = false, brokenStorage = false, origin = 'https://totalelitecoaching.com', pathname = '/audiolibros/', search = '', testAccount = false, storedCampaign = '', referrer = 'https://www.google.com/search?q=PRIVATE+PERSON' } = {}) {
  const handlers = {}, docHandlers = {}, scripts = [], writes = [], cookies = [], elements = new Map();
  function node(tag) { return { tag, hidden: false, disabled: false, children: [], setAttribute(){}, prepend(...items){this.children.unshift(...items);}, append(...items){ this.children.push(...items); if(tag === 'head') scripts.push(...items); }, addEventListener(type, fn){this[type] = fn;}, querySelector(selector){if(!elements.has(selector)) elements.set(selector,node(selector));return elements.get(selector);} }; }
  const footer = node('footer');
  const document = { head:node('head'), body:node('body'), referrer, createElement:node, querySelector:s=>s==='footer'?footer:null, addEventListener(type,fn){docHandlers[type]=fn;} };
  Object.defineProperty(document,'cookie',{get(){return (testAccount ? 'tec_audio_test=1; ' : '') + 'tec_audio_ga=1; tec_audio_ga_DZCX3RMST7=2; _ga=COACH; tec_audio_session=LOGIN; draft=SAVED';},set(value){cookies.push(value);}});
  const storage = new Map([['tec.audiobooks.analytics.choice.v2', choice], [events.CAMPAIGN_KEY, storedCampaign]]);
  const requests = [];
  const context = { ...events, document, location:{origin,pathname,search,href:origin+pathname+'?email=PRIVATE@example.test&libro=SECRET#TOKEN'}, navigator:{onLine:online,globalPrivacyControl:gpc,doNotTrack:dnt?'1':undefined}, localStorage:{getItem(k){if(brokenStorage)throw Error('denied');return storage.get(k)||'';},setItem(k,v){if(brokenStorage)throw Error('denied');writes.push([k,v]);storage.set(k,v);},removeItem(k){storage.delete(k);}}, URL, URLSearchParams, Object, Set, Date, setTimeout, clearTimeout,
    fetch: async (url,options) => {requests.push({url,options});return {ok:true};} };
  context.window={...(native?{TecNative:{}}:{}),addEventListener(type,fn){handlers[type]=fn;}};
  vm.runInNewContext(source, context);
  return {context,scripts,writes,cookies,footer,elements,handlers,docHandlers,storage,requests,
    accept(){elements.get('[data-audio-analytics-accept]').click();},reject(){elements.get('[data-audio-analytics-reject]').click();},load(){scripts.at(-1).onload();},
    click(target){docHandlers.click({target:{closest:()=>target}});},events(){return (context.window.dataLayer||[]).map(x=>Array.from(x)).filter(x=>x[0]==='event');},
    sample(id){context.window.TecAudiobookAnalytics.samplePlayed(id);} };
}

test('analytics vocabulary covers the public catalog and collections but rejects unknown or private paths',()=>{
  assert.deepEqual([...events.BOOK_IDS].sort(),audiobookCatalog.map(b=>b.id).sort());
  assert.deepEqual([...events.COLLECTION_IDS].sort(),audioCollections.map(c=>c.id).sort());
  assert.deepEqual([...events.GUIDE_IDS].sort(),audiobookGuides.map(g=>g.id).sort());
  for(const path of ['/coach/app/','/api/audiolibros/cuenta','/audiolibros/privacidad/','/audiolibros/compartir/secret','/audiolibros/libros/email@example.test/']) assert.equal(events.pageContext(path),null);
  const params=events.safeAudioEvent('page_view',{path:'/audiolibros/',type:'PRIVATE@example.test',email:'SECRET'});
  assert.equal(params.page_title,'Audiolibros TEC · biblioteca'); assert.ok(!JSON.stringify(params).includes('PRIVATE'));
  assert.equal(events.safeAudioEvent('audio_sample_play',events.pageContext('/audiolibros/'),'SECRET'),null);
  assert.equal(events.safeAudioEvent('PRIVATE',events.pageContext('/audiolibros/')),null);
  assert.equal(events.safeReferrer('https://www.google.com/search?q=SECRET'),'https://www.google.com/');
  assert.equal(events.safeReferrer('https://private-company.test/notes?email=SECRET'),'');
});

test('no script or event is sent before consent, after rejection, offline, with DNT/GPC or inside native TEC',()=>{
  for(const options of [{},{choice:'rejected'},{choice:'accepted',online:false},{choice:'accepted',gpc:true},{choice:'accepted',dnt:true},{choice:'accepted',native:true},{choice:'accepted',origin:'http://127.0.0.1:4194'}]){
    const f=browser(options); f.sample(events.BOOK_IDS[0]); assert.equal(f.scripts.length,0,JSON.stringify(options)); assert.equal(f.events().length,0);
  }
});

test('accepted analytics has sanitized metadata, isolated 30-day cookies and no automatic page view duplication',()=>{
  const f=browser();f.accept();assert.equal(f.scripts.length,1);assert.equal(f.scripts[0].referrerPolicy,'no-referrer');f.load();
  assert.equal(f.events().length,1);assert.equal(f.events()[0][1],'page_view');
  const commands=f.context.window.dataLayer.map(x=>Array.from(x)), config=commands.find(x=>x[0]==='config')[2];
  assert.equal(config.send_page_view,false);assert.equal(config.cookie_prefix,'tec_audio');assert.equal(config.cookie_path,'/audiolibros');assert.equal(config.cookie_expires,2592000);assert.equal(config.allow_google_signals,false);
  assert.ok(!JSON.stringify(commands).includes('PRIVATE'));assert.ok(!JSON.stringify(commands).includes('SECRET'));
  f.sample('genesis-rv1909'); f.sample('genesis-rv1909'); f.sample('NOT-A-BOOK');
  assert.equal(f.events().filter(e=>e[1]==='audio_sample_play').length,1);
  f.click({id:'subscribe',getAttribute:()=>null});f.click({id:'share-open',getAttribute:()=>null});
  assert.ok(f.events().some(e=>e[1]==='audio_membership_click'));assert.ok(f.events().some(e=>e[1]==='audio_share_open'));
  assert.ok(f.writes.every(([key])=>key==='tec.audiobooks.analytics.choice.v2'));
});

test('revoking consent and offline transitions stop events without changing Coach, login, progress or audio',()=>{
  const f=browser({choice:'accepted'});f.load();const before=f.events().length;
  f.context.navigator.onLine=false;f.handlers.offline();f.sample(events.BOOK_IDS[1]);assert.equal(f.events().length,before);
  f.context.navigator.onLine=true;f.handlers.online();assert.equal(f.events().length,before);
  f.reject();f.sample(events.BOOK_IDS[0]);assert.equal(f.events().length,before);assert.equal(f.context.window['ga-disable-'+events.MEASUREMENT_ID],true);
  assert.ok(f.cookies.length>0);assert.ok(f.cookies.every(c=>c.startsWith('tec_audio_ga')));
  assert.ok(!f.cookies.some(c=>c.startsWith('_ga=')||c.startsWith('tec_audio_session=')||c.startsWith('draft=')));
  assert.ok(f.requests.every(r=>r.url==='/api/audiolibros/estadisticas/consentimiento'));assert.ok(!source.includes('MediaRecorder'));assert.ok(!source.includes('audio.pause('));
});

test('blocked storage, a failed Google download and consent revoked during loading fail safely',()=>{
  const broken=browser({brokenStorage:true});broken.accept();broken.load();assert.equal(broken.events().length,1);
  const error=browser({choice:'accepted'});error.scripts[0].onerror();error.sample(events.BOOK_IDS[0]);assert.equal(error.events().length,0);
  const race=browser();race.accept();race.reject();race.load();assert.equal(race.events().length,0);
});

test('guide analytics accepts only public canonical slugs and respects the same consent rules',()=>{
  assert.equal(events.pageContext('/audiolibros/guias/').type,'guias');
  for(const g of audiobookGuides){
    const pathname='/audiolibros/guias/'+g.id+'/';
    assert.equal(events.pageContext(pathname).type,'guia');
    assert.equal(events.pageContext(pathname+'PRIVATE@example.test'),null);
    const rejected=browser({pathname,choice:'rejected'});assert.equal(rejected.scripts.length,0);
    const accepted=browser({pathname,choice:'accepted'});accepted.load();
    assert.equal(accepted.events().length,1);
    assert.equal(accepted.events()[0][2].page_location,'https://totalelitecoaching.com'+pathname);
    assert.equal(accepted.events()[0][2].page_title,'Audiolibros TEC · guia');
    assert.ok(!JSON.stringify(accepted.events()).includes('PRIVATE'));
  }
  assert.equal(events.pageContext('/audiolibros/guias/not-a-guide/'),null);
});


test('test accounts never load Google or send events, including previously accepted consent',()=>{
  for (const opts of [{testAccount:true},{search:'?acceso=prueba'}]) {
    const f=browser({choice:'accepted',...opts}); f.sample(events.BOOK_IDS[0]);
    assert.equal(f.scripts.length,0); assert.equal(f.events().length,0);
    assert.equal(f.footer.children.length,0);
  }
  const f=browser({choice:'accepted'});f.load();
  f.docHandlers['tec-audio-test-account'](); f.sample(events.BOOK_IDS[0]);
  assert.equal(f.context.window['ga-disable-'+events.MEASUREMENT_ID],true);
  assert.equal(f.events().length,0);
});

test('reviewed ad attribution survives consent without exposing arbitrary URL data',()=>{
  for (const [name, source] of [['audiolibros_chicago_es','meta'],['audiolibros_instagram_chicago_es','instagram']]) {
    const search = `?utm_source=${source}&utm_medium=paid_social&utm_campaign=${name}&utm_content=clasicos_15s&fbclid=PRIVATE&email=PRIVATE@example.test&entrar=SECRET&utm_term=SECRET`;
    const f=browser({search});
    assert.equal(f.scripts.length,0); assert.equal(f.events().length,0);
    f.accept(); f.load();
    const config=f.context.window.dataLayer.map(x=>Array.from(x)).find(x=>x[0]==='config')[2];
    assert.equal(config.campaign_source,source); assert.equal(config.campaign_medium,'paid_social');
    assert.equal(config.campaign_name,name); assert.equal(config.campaign_content,'clasicos_15s');
    assert.equal(config.page_location,'https://totalelitecoaching.com/audiolibros/');
    assert.equal(f.events().length,1); assert.equal(f.events()[0][2].campaign_name,name);
    assert.ok(!JSON.stringify(f.context.window.dataLayer).match(/PRIVATE|SECRET|fbclid|utm_term/));
    f.reject(); const before=f.events().length;f.sample(events.BOOK_IDS[0]);assert.equal(f.events().length,before);
  }
});

test('unrecognized, duplicate or private campaign values are never forwarded',()=>{
  for(const search of ['', '?fbclid=SECRET', '?utm_source=PRIVATE@example.test&utm_medium=paid_social&utm_campaign=audiolibros_chicago_es',
    '?utm_source=meta&utm_medium=SECRET&utm_campaign=audiolibros_chicago_es',
    '?utm_source=meta&utm_medium=paid_social&utm_campaign=SECRET',
    '?utm_source=meta&utm_source=SECRET&utm_medium=paid_social&utm_campaign=audiolibros_chicago_es',
    '?utm_source=meta&utm_medium=paid_social&utm_campaign=toString']) assert.deepEqual(events.safeCampaign(search),{});
  const safe=events.safeCampaign('?utm_source=meta&utm_medium=paid_social&utm_campaign=audiolibros_chicago_es&utm_content=PRIVATE@example.test');
  assert.equal(safe.campaign_name,'audiolibros_chicago_es');assert.equal(safe.campaign_content,undefined);
  for(const opts of [{choice:'rejected'},{choice:'accepted',testAccount:true},{choice:'accepted',gpc:true}]) {
    const f=browser({...opts,search:'?utm_source=meta&utm_medium=paid_social&utm_campaign=audiolibros_chicago_es'});
    assert.equal(f.scripts.length,0);assert.equal(f.events().length,0);
  }
});

test('ChatGPT attribution survives navigation only after consent and expires or disappears on rejection',()=>{
  const f=browser({search:'?utm_source=chatgpt&utm_medium=cpc&utm_campaign=tec_clasicos_es&utm_content=james_allen_muestra&oppref=SECRET'});
  assert.equal(f.storage.get(events.CAMPAIGN_KEY),'');
  f.accept();f.load();
  assert.equal(f.events()[0][2].campaign_source,'chatgpt');
  assert.ok(!JSON.stringify(f.events()).includes('SECRET'));
  const saved=f.storage.get(events.CAMPAIGN_KEY);
  const next=browser({choice:'accepted',storedCampaign:saved,pathname:'/audiolibros/libros/como-un-hombre-piensa/',referrer:'https://totalelitecoaching.com/audiolibros/'});
  next.load();assert.equal(next.events()[0][2].campaign_name,'tec_clasicos_es');
  next.reject();assert.equal(next.storage.has(events.CAMPAIGN_KEY),false);
  assert.ok(next.requests.some(r=>r.options.method==='DELETE'));
  assert.deepEqual(events.savedCampaign(JSON.stringify({at:Date.now()-events.CAMPAIGN_TTL,campaign:analyticsCampaign()})),{});
  assert.deepEqual(events.savedCampaign(JSON.stringify({at:Date.now()+10000,campaign:analyticsCampaign()})),{});
  assert.equal(events.safeReferrer('https://chatgpt.com/c/SECRET?email=PRIVATE'),'https://chatgpt.com/');
  for(const referrer of ['https://www.google.com/search?q=PRIVATE','https://other.test/private']) {
    const external=browser({choice:'accepted',storedCampaign:saved,referrer});external.load();
    assert.equal(external.events()[0][2].campaign_source,undefined);
    assert.equal(external.storage.has(events.CAMPAIGN_KEY),false);
  }
});
function analyticsCampaign(){return {campaign_source:'chatgpt',campaign_medium:'cpc',campaign_name:'tec_clasicos_es'};}

test('checkout context uses Google client/session IDs and becomes unavailable immediately on rejection',async()=>{
  const f=browser({choice:'accepted'});f.load();
  const pending=f.context.window.TecAudiobookAnalytics.checkoutContext();
  for(const args of f.context.window.dataLayer){if(args[0]==='get')args[3](args[2]==='client_id'?'123456.1794310000':'1794310000');}
  const result=await pending;
  assert.equal(result.consentVersion,2);assert.equal(result.clientId,'123456.1794310000');
  f.context.window.TecAudiobookAnalytics.checkoutStarted();
  assert.equal(f.events().filter(e=>e[1]==='begin_checkout').length,1);
  assert.equal(f.events().filter(e=>e[1]==='purchase').length,0);
  f.reject();assert.equal(await f.context.window.TecAudiobookAnalytics.checkoutContext(),null);
});
