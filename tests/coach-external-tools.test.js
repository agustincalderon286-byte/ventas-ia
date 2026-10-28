import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import vm from 'node:vm';
const source=fs.readFileSync(new URL('../public/coach/external-tools.js',import.meta.url),'utf8');
function harness(navigator={}) {
 const events={},nodes=new Map();let focused=false,pushes=0;
 const node=key=>{if(!nodes.has(key))nodes.set(key,{addEventListener(type,fn){this[type]=fn},removeAttribute(key){delete this[key]}});return nodes.get(key)};
 const dialog={querySelector:node,setAttribute(){},addEventListener(type,fn){this[type]=fn},showModal(){this.open=true},close(){this.open=false}};
 const history={state:{saved:true},pushState(state){this.state=state;pushes++},replaceState(state){this.state=state}};
 const document={body:{style:{cssText:'color:navy'},append(){}},activeElement:{focus(){focused=true}},createElement:()=>dialog,addEventListener(type,fn){events[type]=fn}};
 const opened=[];
 const win={navigator,open(...args){opened.push(args)},scrollY:100,scrollTo(){},addEventListener(type,fn){events[type]=fn}};
 vm.runInNewContext(source,{window:win,document,history,location:{href:'https://tec.example/coach/app/'}});
 return {win,opened,dialog,events,node,history,document,get focused(){return focused},get pushes(){return pushes}};
}
test('Konverta is isolated from TEC data and closing/reopening does not wait on iframe navigation history',async()=>{
 const h=harness(); const {win,dialog,node,history,events,document}=h;
 await win.TecExternalTools.openKonverta();
 assert.equal(node('iframe').src,'https://dash.konverta.app/signin');assert.equal(dialog.open,true);assert.equal(history.state.saved,true);
 assert.match(dialog.innerHTML,/sandbox="allow-forms allow-scripts allow-same-origin/);assert.doesNotMatch(dialog.innerHTML,/allow-top-navigation/);
 node('[data-konverta-close]').click();assert.equal(dialog.open,false);assert.equal(node('iframe').src,undefined);assert.equal(h.focused,true);assert.equal(document.body.style.cssText,'color:navy');
 await win.TecExternalTools.openKonverta();assert.equal(dialog.open,true);assert.equal(h.pushes,1);
 history.state={saved:true};events.popstate();assert.equal(dialog.open,false);
 assert.equal(win.TecExternalTools.openWater('60406&other=1'),false);assert.equal(dialog.open,false);
 await win.TecExternalTools.openWater('60406');assert.equal(node('iframe').src,'https://www.ewg.org/tapwater/search-results.php?zip5=60406');
 node('[data-konverta-close]').click();assert.equal(dialog.open,false);
 await win.TecExternalTools.openWater();assert.equal(node('iframe').src,'https://www.ewg.org/tapwater/');
 assert.doesNotMatch(source.replace(/\/\*[\s\S]*?\*\//g,''),/localStorage|fetch\(|postMessage|apiRequest|password|credentials/);
});
test('phone and iPad taps keep the real Konverta universal link, never a sandboxed login',()=>{
 for(const navigator of [{userAgent:'iPhone'},{userAgent:'Android'},{userAgent:'Macintosh',maxTouchPoints:5}]) {
  const h=harness(navigator);let prevented=false;
  const link={hasAttribute:()=>false};
  h.events.click({target:{closest:()=>link},preventDefault(){prevented=true}});
  assert.equal(prevented,false);assert.equal(h.dialog.open,undefined);assert.equal(h.pushes,0);
  h.win.TecExternalTools.openKonverta();
  assert.deepEqual(h.opened,[['https://dash.konverta.app/signin','_blank','noopener,noreferrer']]);
  assert.equal(h.document.body.style.cssText,'color:navy');
 }
});
test('water button opens the EWG search inside TEC without requiring or copying a ZIP',()=>{
 const h=harness({userAgent:'iPhone'});let prevented=false;
 h.events.click({target:{closest:()=>({hasAttribute:attribute=>attribute==='data-open-water'})},preventDefault(){prevented=true}});
 assert.equal(prevented,true);assert.equal(h.dialog.open,true);
 assert.equal(h.node('iframe').src,'https://www.ewg.org/tapwater/');assert.equal(h.opened.length,0);
});
