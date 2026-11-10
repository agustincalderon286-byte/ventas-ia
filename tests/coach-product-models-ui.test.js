import test from 'node:test';
import assert from 'node:assert/strict';
import vm from 'node:vm';
import {readFileSync} from 'node:fs';

const source=readFileSync(new URL('../public/coach/product-models.js',import.meta.url),'utf8');
const catalogSource=readFileSync(new URL('../public/coach/product-models-catalog.js',import.meta.url),'utf8');
const tick=()=>new Promise(setImmediate);
function harness({offline=false}={}) {
  const nodes=[],handlers={},timers=new Map();let calls=0,resolveLoad,timerId=0;
  const moduleReady=new Promise(resolve=>{resolveLoad=resolve;});
  function element(tag){
    const el={tagName:tag.toUpperCase(),children:[],attrs:{},events:{},hidden:false,isConnected:true,style:{},
      setAttribute(k,v){this.attrs[k]=v;},removeAttribute(k){delete this.attrs[k];},
      append(...items){this.children.push(...items);},replaceChildren(...items){this.children=items;},
      remove(){this.isConnected=false;},addEventListener(k,fn){(this.events[k]||=[]).push(fn);},
      fire(k,event={}){for(const fn of this.events[k]||[])fn({preventDefault(){},...event});},
      showModal(){this.open=true;},close(){this.open=false;this.fire('close');},focus(){this.focused=true;},jumpCameraToGoal(){this.centered=true;}};
    nodes.push(el);return el;
  }
  const body=element('body');body.style.overflow='scroll';const ctor={};
  const scope={document:{body,createElement:element},navigator:{onLine:!offline},customElements:{get:()=>ctor},
    loadViewerModule(){calls++;return moduleReady;},window:{
      setTimeout(fn){timers.set(++timerId,fn);return timerId;},clearTimeout(id){timers.delete(id);},
      addEventListener(k,fn){handlers[k]=fn;}}};
  vm.runInNewContext(catalogSource,scope);
  // Mock the vendor module transport; exercise the actual mount/dialog lifecycle.
  vm.runInNewContext(source.replace("import('/coach/vendor/model-viewer/4.4.0/model-viewer.min.js')",'loadViewerModule()'),scope);
  const container=element('div');const product={codigo_producto:'CO9560',nombre_producto:'OLLA DE 3 CUARTOS'};
  const mount=p=>scope.window.TecProductModels.mount(container,p);
  const find=cls=>nodes.find(n=>n.className===cls);
  return {nodes,body,ctor,scope,container,mount,product,find,timers,handlers,resolveLoad,get calls(){return calls;}};
}

test('the calculator adds only a button until clicked; clearing selection removes the stale model',()=>{
  const h=harness();h.mount(h.product);assert.equal(h.container.hidden,false);assert.equal(h.calls,0);
  assert.match(h.container.children[0].attrs['aria-label'],/Olla NOVEL 3 cuartos/);
  assert.equal(h.nodes.some(n=>n.tagName==='MODEL-VIEWER'),false);
  h.mount({codigo_producto:'CO9570',nombre_producto:'OLLA DE 6 CUARTOS'});assert.match(h.container.children[0].attrs['aria-label'],/6 cuartos/);
  h.mount(null);assert.equal(h.container.hidden,true);assert.equal(h.container.children.length,0);
});

test('closing before the vendor loads never reopens a viewer or disturbs the calculator',async()=>{
  const h=harness();h.mount(h.product);const opener=h.container.children[0];opener.fire('click');
  assert.equal(h.calls,1);assert.equal(h.body.style.overflow,'hidden');
  h.find('tec-model-close').fire('click');assert.equal(h.body.style.overflow,'scroll');assert.ok(opener.focused);
  h.resolveLoad();await tick();assert.equal(h.nodes.some(n=>n.tagName==='MODEL-VIEWER'),false);
  assert.equal(h.find('tec-model-dialog').open,false);assert.equal(h.timers.size,0);
});

test('a loaded viewer uses private assets and user-initiated AR, retries errors and releases on exit',async()=>{
  const h=harness();h.mount(h.product);h.container.children[0].fire('click');h.resolveLoad();await tick();
  let viewer=h.nodes.filter(n=>n.tagName==='MODEL-VIEWER').at(-1);
  assert.equal(viewer.attrs.src,'/api/coach/product-models/olla-3-cuartos/model.glb');
  assert.equal(viewer.attrs['ar-modes'],'webxr quick-look');assert.equal(viewer.attrs['auto-rotate'],undefined);
  assert.equal(h.ctor.modelCacheSize,1);
  viewer.fire('load');assert.equal(h.timers.size,0);assert.match(h.find('tec-model-status').textContent,/Gira y acerca/);
  viewer.fire('error');assert.equal(h.nodes.find(n=>n.textContent==='Reintentar modelo').hidden,false);
  h.nodes.find(n=>n.textContent==='Reintentar modelo').fire('click');await tick();assert.equal(viewer.isConnected,false);
  viewer=h.nodes.filter(n=>n.tagName==='MODEL-VIEWER').at(-1);
  h.find('tec-model-dialog').fire('cancel');assert.equal(viewer.attrs.src,undefined);assert.equal(viewer.isConnected,false);
  assert.equal(h.body.style.overflow,'scroll');assert.equal(h.timers.size,0);
});

test('offline opening gives a retry and never starts a vendor download or microphone',()=>{
  const h=harness({offline:true});h.mount(h.product);h.container.children[0].fire('click');
  assert.equal(h.calls,0);assert.match(h.find('tec-model-status').textContent,/Necesitas conexión/);
  assert.equal(h.nodes.find(n=>n.textContent==='Reintentar modelo').hidden,false);
  assert.doesNotMatch(source,/getUserMedia|MediaRecorder|\.activateAR\(/);
});
