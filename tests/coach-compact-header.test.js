import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import vm from 'node:vm';
const source=fs.readFileSync(new URL('../public/coach/compact-header.js',import.meta.url),'utf8');
function fixture({mode='default',hash='',navigation='navigate'}={}){
 const events={},frames=[],scrolls=[],styles={},anchorScrolls=[];
 let focused=false;
 const root={dataset:{coachPortalMode:mode},style:{setProperty:(key,value)=>styles[key]=value}};
 const menu={open:false,contains:target=>target==='inside',querySelector:()=>({focus(){focused=true}})};
 const header={getBoundingClientRect:()=>({height:122.5})};
 const anchor={scrollIntoView:value=>anchorScrolls.push(value)};
 const window={location:{hash},performance:{getEntriesByType:()=>[{type:navigation}]},requestAnimationFrame:fn=>frames.push(fn),scrollTo:value=>scrolls.push(value),addEventListener:(name,fn)=>events[name]=fn};
 const document={documentElement:root,querySelector:selector=>selector==='[data-visit-header]'?header:menu,getElementById:id=>id==='paso-demostracion'?anchor:null,addEventListener:(name,fn)=>events[name]=fn};
 vm.runInNewContext(source,{window,document,setTimeout});
 return {api:window.TecVisitHeader,events,menu,styles,scrolls,anchorScrolls,drain(){while(frames.length)frames.shift()()},get focused(){return focused}};
}
test('fresh entry opens Cierre while browser Back, explicit step links and telemarketing retain their intended context',()=>{
 const fresh=fixture();assert.equal(fresh.api.initialWorkspace('actividad','cierre'),'cierre');assert.equal(fresh.api.shouldStartAtFirstStep(),true);
 const back=fixture({navigation:'back_forward'});assert.equal(back.api.initialWorkspace('actividad','cierre'),'actividad');assert.equal(back.api.shouldStartAtFirstStep(),false);
 const link=fixture({hash:'#paso-demostracion'});assert.equal(link.api.initialWorkspace('actividad','cierre'),'cierre');assert.equal(link.api.shouldStartAtFirstStep(),false);
 const telemarketing=fixture({mode:'telemarketing'});assert.equal(telemarketing.api.initialWorkspace('program414','crm'),'program414');
});
test('ready positions only a fresh entry, preserves Back scroll and gives explicit step links room below the sticky header',()=>{
 const fresh=fixture();fresh.api.ready();fresh.drain();assert.equal(fresh.styles['--tec-visit-header-height'],'135px');assert.equal(fresh.scrolls.length,1);assert.equal(fresh.scrolls[0].top,0);
 const back=fixture({navigation:'back_forward'});back.api.ready();back.drain();assert.equal(back.scrolls.length,0);
 const link=fixture({hash:'#paso-demostracion'});link.api.ready();link.drain();assert.equal(link.scrolls.length,0);assert.equal(link.anchorScrolls.length,1);assert.equal(link.anchorScrolls[0].block,'start');
 const tele=fixture({mode:'telemarketing'});tele.api.ready();tele.drain();assert.equal(tele.scrolls.length,0);
});
test('account menu dismisses on Escape or outside click and restores keyboard focus without moving the page',()=>{
 const h=fixture();h.menu.open=true;h.events.click({target:'inside'});assert.equal(h.menu.open,true);
 h.events.keydown({key:'Escape'});assert.equal(h.menu.open,false);assert.equal(h.focused,true);assert.equal(h.scrolls.length,0);
 h.menu.open=true;h.events.click({target:'outside'});assert.equal(h.menu.open,false);
});
