import test from 'node:test';
import assert from 'node:assert/strict';
import vm from 'node:vm';
import fs from 'node:fs';
const app=fs.readFileSync(new URL('../public/coach/app.js',import.meta.url),'utf8');
const source=app.slice(app.indexOf('function createFloatingCoachSurface('),app.indexOf('async function initCoachAppPage()'));
function fixture({modal=true}={}){
 const events={},viewportEvents={},style=new Map(),restored=[];let shows=0,closes=0,blurs=0;
 const textarea={value:'Pregunta que todavía no envié',scrollTop:0,clientHeight:48,scrollHeight:48,blur(){blurs++;}};
 const history={scrollTop:100,clientHeight:200,scrollHeight:700};
 const panel={hidden:true,open:false,style:{setProperty:(k,v)=>style.set(k,v)},contains:n=>[history,textarea].includes(n),querySelector:()=>textarea};
 if(modal){panel.showModal=()=>{panel.open=true;shows++;};panel.close=()=>{panel.open=false;closes++;};}
 const window={scrollX:0,scrollY:1250,innerHeight:800,visualViewport:{height:800,offsetTop:0,addEventListener:(k,f)=>viewportEvents[k]=f},addEventListener(){},scrollTo:p=>restored.push(p)};
 const document={addEventListener:(k,f)=>events[k]=f};
 const create=vm.runInNewContext(source+';createFloatingCoachSurface',{window,document});
 return{surface:create(panel),panel,window,events,viewportEvents,style,restored,textarea,history,get shows(){return shows},get closes(){return closes},get blurs(){return blurs}};
}
test('chat uses a modal surface and restores the visit after keyboard panning without discarding an unsent question',()=>{
 const f=fixture();f.surface.setOpen(true);f.surface.setOpen(true);assert.equal(f.shows,1);assert.equal(f.panel.hidden,false);
 f.window.scrollY=1800;f.surface.setOpen(false);assert.equal(f.panel.hidden,true);assert.equal(f.panel.open,false);assert.equal(f.closes,1);assert.equal(f.restored[0].top,1250);assert.equal(f.textarea.value,'Pregunta que todavía no envié');f.surface.setOpen(false);assert.equal(f.restored.length,1);
});
test('chat wheel scrolling is allowed in history but cannot escape through its edges or the background',()=>{
 const f=fixture();f.surface.setOpen(true);const target={closest:()=>f.history};let prevented=0;
 const wheel=(target,deltaY)=>f.events.wheel({target,deltaY,cancelable:true,preventDefault(){prevented++}});
 wheel(target,100);assert.equal(prevented,0);f.history.scrollTop=500;wheel(target,100);assert.equal(prevented,1);f.history.scrollTop=0;wheel(target,-100);assert.equal(prevented,2);wheel({closest:()=>null},100);assert.equal(prevented,3);f.surface.setOpen(false);wheel(target,100);assert.equal(prevented,3);
});
test('one-finger gestures stay in overflowing chat content and never chain into the underlying visit',()=>{
 const f=fixture();f.surface.setOpen(true);let prevented=0;
 const start=y=>f.events.touchstart({touches:[{clientY:y}]});
 const move=(y,target)=>f.events.touchmove({touches:[{clientY:y}],target,cancelable:true,preventDefault(){prevented++}});
 start(500);move(400,{closest:()=>f.history});assert.equal(prevented,0);
 f.history.scrollTop=500;move(300,{closest:()=>f.history});assert.equal(prevented,1);
 move(200,{closest:()=>null});assert.equal(prevented,2);
 f.surface.setOpen(false);move(100,{closest:()=>null});assert.equal(prevented,2);
});
test('keyboard viewport changes resize the panel and clamp elastic overscroll offsets',()=>{
 const f=fixture();f.surface.setOpen(true);f.window.visualViewport.height=350;f.window.visualViewport.offsetTop=20;f.viewportEvents.resize();assert.equal(f.style.get('--tec-chat-viewport-height'),'350px');assert.equal(f.style.get('--tec-chat-viewport-top'),'20px');f.window.visualViewport.offsetTop=-30;f.viewportEvents.scroll();assert.equal(f.style.get('--tec-chat-viewport-top'),'0px');
});
test('cached legacy div markup retains opening, closing and scroll restoration without dialog APIs',()=>{
 const f=fixture({modal:false});f.surface.setOpen(true);assert.equal(f.panel.hidden,false);f.surface.setOpen(false);assert.equal(f.panel.hidden,true);assert.equal(f.restored[0].top,1250);
});
