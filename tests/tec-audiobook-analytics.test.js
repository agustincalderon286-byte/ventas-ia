import test from 'node:test';
import assert from 'node:assert/strict';
import vm from 'node:vm';
import { readFileSync } from 'node:fs';
import * as events from '../public/audiolibros/analytics-events.js';
import { audiobookCatalog } from '../src/tec-audiobooks.js';
import { audioCollections } from '../src/knowledge/audiobook-editorial.js';

const source = readFileSync(new URL('../public/audiolibros/analytics.js', import.meta.url), 'utf8').replace(/^import[^\n]+\n/, '');
function browser({ choice = '', online = true, gpc = false, dnt = false, native = false, brokenStorage = false, origin = 'https://totalelitecoaching.com', pathname = '/audiolibros/' } = {}) {
  const handlers = {}, docHandlers = {}, scripts = [], writes = [], cookies = [], elements = new Map();
  function node(tag) { return { tag, hidden: false, disabled: false, children: [], setAttribute(){}, append(...items){ this.children.push(...items); if(tag === 'head') scripts.push(...items); }, addEventListener(type, fn){this[type] = fn;}, querySelector(selector){if(!elements.has(selector)) elements.set(selector,node(selector));return elements.get(selector);} }; }
  const footer = node('footer');
  const document = { head:node('head'), body:node('body'), referrer:'https://www.google.com/search?q=PRIVATE+PERSON', createElement:node, querySelector:s=>s==='footer'?footer:null, addEventListener(type,fn){docHandlers[type]=fn;} };
  Object.defineProperty(document,'cookie',{get(){return 'tec_audio_ga=1; tec_audio_ga_DZCX3RMST7=2; _ga=COACH; tec_audio_session=LOGIN; draft=SAVED';},set(value){cookies.push(value);}});
  const context = { ...events, document, location:{origin,pathname,href:origin+pathname+'?email=PRIVATE@example.test&libro=SECRET#TOKEN'}, navigator:{onLine:online,globalPrivacyControl:gpc,doNotTrack:dnt?'1':undefined}, localStorage:{getItem(){if(brokenStorage)throw Error('denied');return choice;},setItem(k,v){if(brokenStorage)throw Error('denied');writes.push([k,v]);}}, URL, Object, Set, Date };
  context.window={...(native?{TecNative:{}}:{}),addEventListener(type,fn){handlers[type]=fn;}};
  vm.runInNewContext(source, context);
  return {context,scripts,writes,cookies,footer,elements,handlers,docHandlers,
    accept(){elements.get('[data-audio-analytics-accept]').click();},reject(){elements.get('[data-audio-analytics-reject]').click();},load(){scripts.at(-1).onload();},
    click(target){docHandlers.click({target:{closest:()=>target}});},events(){return (context.window.dataLayer||[]).map(x=>Array.from(x)).filter(x=>x[0]==='event');},
    sample(id){context.window.TecAudiobookAnalytics.samplePlayed(id);} };
}

test('analytics vocabulary covers the public catalog and collections but rejects unknown or private paths',()=>{
  assert.deepEqual([...events.BOOK_IDS].sort(),audiobookCatalog.map(b=>b.id).sort());
  assert.deepEqual([...events.COLLECTION_IDS].sort(),audioCollections.map(c=>c.id).sort());
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
  assert.ok(f.writes.every(([key])=>key==='tec.audiobooks.analytics.choice.v1'));
});

test('revoking consent and offline transitions stop events without changing Coach, login, progress or audio',()=>{
  const f=browser({choice:'accepted'});f.load();const before=f.events().length;
  f.context.navigator.onLine=false;f.handlers.offline();f.sample(events.BOOK_IDS[1]);assert.equal(f.events().length,before);
  f.context.navigator.onLine=true;f.handlers.online();assert.equal(f.events().length,before);
  f.reject();f.sample(events.BOOK_IDS[0]);assert.equal(f.events().length,before);assert.equal(f.context.window['ga-disable-'+events.MEASUREMENT_ID],true);
  assert.ok(f.cookies.length>0);assert.ok(f.cookies.every(c=>c.startsWith('tec_audio_ga')));
  assert.ok(!f.cookies.some(c=>c.startsWith('_ga=')||c.startsWith('tec_audio_session=')||c.startsWith('draft=')));
  assert.ok(!source.includes('fetch('));assert.ok(!source.includes('MediaRecorder'));assert.ok(!source.includes('audio.pause('));
});

test('blocked storage, a failed Google download and consent revoked during loading fail safely',()=>{
  const broken=browser({brokenStorage:true});broken.accept();broken.load();assert.equal(broken.events().length,1);
  const error=browser({choice:'accepted'});error.scripts[0].onerror();error.sample(events.BOOK_IDS[0]);assert.equal(error.events().length,0);
  const race=browser();race.accept();race.reject();race.load();assert.equal(race.events().length,0);
});
