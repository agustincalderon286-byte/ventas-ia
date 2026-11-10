import test from 'node:test';
import assert from 'node:assert/strict';
import vm from 'node:vm';
import {readFileSync} from 'node:fs';

const navigation=readFileSync(new URL('../public/audiolibros/navegacion.js',import.meta.url),'utf8');
function element(id,hash='') {
  return {id,hash,hidden:false,attrs:{},events:{},setAttribute(k,v){this.attrs[k]=v;},removeAttribute(k){delete this.attrs[k];},
    addEventListener(k,fn){this.events[k]=fn;},scrollIntoView(){this.scrolled=true;},focus(){this.focused=true;}};
}
function environment(hash='') {
  const books=element('libros'),catalog=element('catalogo'),hero=element('hero'),membership=element('membresia');
  const tabs=[element('audiobooks-tab','#catalogo'),element('reading-tab','#libros')],switcher=element('switcher');
  const links=[element('audio-link','#catalogo'),element('books-link','#libros')];
  const handlers={},docHandlers={},location={hash},body={dataset:{}};
  const document={body,
    querySelector(selector){if(selector==='#audio')throw Error('Changing tabs must not touch playback');return selector==='#libros'?books:switcher;},
    querySelectorAll(selector){return selector.includes('[role="tab"]')?tabs:selector==='main > section'?[hero,catalog,books,membership]:links;},
    getElementById(id){return [books,catalog,membership].find(n=>n.id===id);},
    addEventListener(k,fn){docHandlers[k]=fn;}};
  const history={pushState(_state,_title,value){location.hash=value;}};
  vm.runInNewContext(navigation,{document,location,history,window:{addEventListener(k,fn){handlers[k]=fn;}}});
  const click=link=>docHandlers.click({target:{closest:()=>link},button:0,preventDefault(){}});
  return {books,catalog,hero,membership,tabs,links,switcher,handlers,location,body,click};
}

test('library tabs show one format, keep their deep link and never touch audio playback',()=>{
  const e=environment();
  assert.equal(e.books.hidden,true);assert.equal(e.catalog.hidden,false);
  e.click(e.links[1]);
  assert.equal(e.location.hash,'#libros');assert.equal(e.books.hidden,false);
  assert.ok(e.catalog.hidden&&e.hero.hidden&&e.membership.hidden);
  assert.equal(e.tabs[1].attrs['aria-selected'],'true');assert.equal(e.tabs[1].tabIndex,0);
  assert.equal(e.links[1].attrs['aria-current'],'page');
  e.click(e.tabs[0]);
  assert.equal(e.books.hidden,true);assert.equal(e.catalog.hidden,false);
  assert.equal(e.location.hash,'#catalogo');
});

test('direct reading links, keyboard tabs and back/forward reveal their destination',()=>{
  const e=environment('#libros');assert.equal(e.books.hidden,false);assert.ok(e.switcher.scrolled);
  e.tabs[1].events.keydown({key:'ArrowLeft',preventDefault(){}});
  assert.equal(e.catalog.hidden,false);assert.ok(e.tabs[0].focused);
  e.location.hash='#libros';e.handlers.popstate();assert.equal(e.books.hidden,false);
  e.location.hash='#membresia';e.handlers.hashchange();
  assert.equal(e.books.hidden,true);assert.equal(e.membership.hidden,false);assert.ok(e.membership.scrolled);
});

test('sleep timer pauses once at its deadline or chapter boundary and clears its active indicator',()=>{
  const source=readFileSync(new URL('../public/audiolibros/biblioteca.js',import.meta.url),'utf8');
  const timer=source.slice(source.indexOf('  function resetTimer('),source.indexOf('  function updateChapter('));
  const nodes=new Map(['timer','timer-status','timer-badge','timer-shortcut'].map(id=>[id,{value:'15',hidden:false,textContent:'',removeAttribute(k){this.removed=k;}}]));
  let now=1000,pauses=0,canceled=0;
  const ctx=vm.createContext({$:selector=>nodes.get(selector.slice(1)),audio:{currentTime:0,pause(){pauses++;}},
    Date:{now:()=>now},clearTimeout(){canceled++;}});
  vm.runInContext('let deadline=2000,stopAt=0,timerHandle=42;'+timer,ctx);
  vm.runInContext('checkTimer()',ctx);assert.equal(pauses,0);
  now=2000;vm.runInContext('checkTimer();checkTimer()',ctx);
  assert.equal(pauses,1);assert.equal(canceled,1);assert.equal(nodes.get('timer').value,'0');
  assert.ok(nodes.get('timer-badge').hidden);assert.equal(nodes.get('timer-shortcut').removed,'data-active');
  assert.match(nodes.get('timer-status').textContent,/pausó/);
  vm.runInContext('deadline=0;stopAt=120;audio.currentTime=119;checkTimer()',ctx);assert.equal(pauses,1);
  vm.runInContext('audio.currentTime=120;checkTimer()',ctx);assert.equal(pauses,2);
});
