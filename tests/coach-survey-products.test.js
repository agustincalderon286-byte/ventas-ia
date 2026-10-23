import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import vm from 'node:vm';

const app = fs.readFileSync(new URL('../public/coach/app.js', import.meta.url), 'utf8');
const source = app.slice(app.indexOf('function initSurveyProductPicker()'), app.indexOf('function initOrderCalculator()'));
const products = [10,12].map(size => ({nombre_producto:`SARTEN ${size}`,codigo_producto:`C${size}`,precio_base_catalogo:size*100,imagen_producto:`/images/${size}.webp`}));
const tick = () => new Promise(setImmediate);
function harness(search = async () => products) {
  const nodes=[];
  const node = tag => {
    const n={tag, value:'', textContent:'', hidden:false, attrs:{}, children:[], handlers:{}, classList:{add(){}},
      setAttribute(k,v){this.attrs[k]=v;}, append(...children){this.children.push(...children);},
      replaceChildren(...children){this.children=children;this.textContent='';},
      addEventListener(k,f){(this.handlers[k] ||= []).push(f);},
      dispatchEvent(e){for(const f of this.handlers[e.type]||[]) f(e);},
      fire(type){this.dispatchEvent({type,preventDefault(){}});}};
    nodes.push(n); return n;
  };
  const inputs=[1,2,3].map(()=>{const n=node('input');n.parentElement=node('div');return n;});
  const form=node('form'); form.elements={namedItem:name=>inputs[Number(name.slice(-1))-1]};
  const applied=[], cleared=[];
  const calc={applySurveyProduct:(index,match)=>applied.push({index,match}),clearSurveyProduct:index=>cleared.push(index)};
  let timer=0; const timers=new Map();
  vm.runInNewContext(source+'\ninitSurveyProductPicker();',{
    document:{querySelector:s=>s==='[data-health-survey-form]'?form:calc,createElement:node},
    searchTecProductCatalog:search,Event,clearTimeout:id=>timers.delete(id),setTimeout:fn=>{timers.set(++timer,fn);return timer;}
  });
  const descendants = n => [n,...n.children.flatMap(descendants)];
  return {form,inputs,applied,cleared,
    results:i=>inputs[i].parentElement.children.find(n=>n.className==='order-calc-suggestions'),
    button:i=>inputs[i].parentElement.children.find(n=>n.className?.includes('tec-survey-product-search')),
    text:i=>descendants(inputs[i].parentElement).map(n=>n.textContent).join(' '),
    async type(i,value){inputs[i].value=value;inputs[i].fire('input'); const jobs=[...timers.values()];timers.clear();jobs.forEach(fn=>fn());await tick();},
    descendants
  };
}

test('three survey fields show photo choices without prices and only the clicked exact product is copied', async () => {
  const h=harness();
  for(let i=0;i<3;i++) {
    await h.type(i,'sarten');
    assert.equal(h.applied.length,i);
    assert.equal(h.results(i).hidden,false);
    assert.equal(h.results(i).children.length,2);
    assert.equal(h.descendants(h.results(i)).filter(n=>n.tag==='img').length,2);
    assert.doesNotMatch(h.text(i), /\$|1000|1200|Base /);
    const tapped = h.results(i).children[1];
    h.inputs[i].fire('change'); // Mobile blur occurs before the suggestion click.
    assert.equal(h.results(i).children[1],tapped);
    h.results(i).children[1].fire('click');
    assert.equal(h.applied.at(-1).index,i);
    assert.equal(h.applied.at(-1).match.codigo_producto,'C12');
    assert.equal(h.applied.at(-1).match.imagen_producto,'/images/12.webp');
    assert.equal(h.inputs[i].value,'SARTEN 12');
    assert.equal(h.results(i).hidden,true);
  }
});

test('editing invalidates a selected product; search button and focus reopen the list', async () => {
  const h=harness(); await h.type(0,'sarten'); h.results(0).children[1].fire('click');
  assert.equal(h.form.getSelectedCatalogProducts()[0].codigo_producto,'C12');
  await h.type(0,'olla');
  assert.equal(h.form.getSelectedCatalogProducts()[0],null);
  assert.equal(h.applied.length,1); assert.equal(h.cleared.at(-1),0);
  h.inputs[0].fire('keydown'); h.button(0).fire('click'); await tick();
  assert.equal(h.results(0).hidden,false);
  h.results(0).children[0].fire('click'); h.inputs[0].fire('focus'); await tick();
  assert.equal(h.results(0).hidden,false);
});

test('late responses cannot replace a newer query or selection', async () => {
  let resolveFirst;
  const h=harness(q=>q==='old'?new Promise(resolve=>{resolveFirst=resolve;}):Promise.resolve(products));
  await h.type(0,'old'); await h.type(0,'sarten'); h.results(0).children[1].fire('click');
  resolveFirst([products[0]]); await tick();
  assert.equal(h.inputs[0].value,'SARTEN 12'); assert.equal(h.results(0).hidden,true);
  assert.equal(h.applied.length,1);
});

test('unknown restored text opens choices instead of guessing a product', async () => {
  const h=harness(); h.inputs[0].value='sarten'; h.form.refreshProductPicker(); await tick();
  assert.equal(h.applied.length,0); assert.equal(h.results(0).hidden,false);
  h.inputs[0].value='SARTEN 12'; h.form.refreshProductPicker(); await tick();
  assert.equal(h.applied.at(-1).match.codigo_producto,'C12');
});
