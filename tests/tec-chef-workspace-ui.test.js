import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import vm from 'node:vm';
const source=fs.readFileSync(new URL('../public/coach/chef-workspace.js',import.meta.url),'utf8');
const turn=async()=>{for(let i=0;i<30;i++)await Promise.resolve();};
class Node{
  constructor(tag='div'){this.tagName=tag;this.children=[];this.events=new Map();this.dataset={};this.attrs={};this.hidden=false;this.value='';this.textContent='';}
  append(...nodes){this.children.push(...nodes);}
  replaceChildren(...nodes){this.children=nodes;}
  addEventListener(type,fn){this.events.set(type,fn);}
  async fire(type){return this.events.get(type)?.({});}
  setAttribute(key,value){this.attrs[key]=value;}
  getAttribute(key){return this.attrs[key]??this[key]??null;}
  focus(){}
  select(){}
}
function fixture(){
  const nodes=new Map(),get=key=>{if(!nodes.has(key))nodes.set(key,new Node());return nodes.get(key);};
  const mine=get('[data-chef-scope="mine"]'),team=get('[data-chef-scope="team"]');mine.dataset.chefScope='mine';team.dataset.chefScope='team';
  const document=new Node();document.querySelector=get;document.querySelectorAll=()=>[mine,team];document.createElement=tag=>new Node(tag);document.hidden=false;
  const window=new Node(),calls=[],timers=new Map();let n=0,reloads=0;
  let handler=async()=>({ok:true,data:{user:{id:'eric',name:'Eric'},shareUrl:'https://totalelitecoaching.com/chef/eric/',sharingEnabled:true,canViewTeam:true,leads:[{id:'lead-1',fullName:'<script>cliente</script>',sellerName:'Eric',status:'nuevo',interest:'Producto',phone:'7085550100',createdAt:'2026-11-17T12:00:00Z'}]}});
  vm.runInNewContext(source,{document,window,navigator:{},AbortSignal,Date,Number,encodeURIComponent,location:{reload(){reloads++;}},setTimeout(fn){const id=++n;timers.set(id,fn);return id;},clearTimeout:id=>timers.delete(id),fetch:async(url,options)=>{calls.push({url,options});const result=await handler(url,options);return{ok:result.ok,status:result.status||200,json:async()=>result.data};}});
  return{get,calls,document,timers,setHandler(fn){handler=fn;},get reloads(){return reloads;}};
}
test('the private workspace renders contacts as text and uses the account-bound personal QR',async()=>{
 const f=fixture();await turn();assert.equal(f.get('#chef-share-url').value,'https://totalelitecoaching.com/chef/eric/');assert.equal(f.get('#chef-personal-qr').src,'/api/chef/workspace/qr.svg?account=eric');assert.equal(f.get('#chef-leads').children[0].children[0].children[0].textContent,'<script>cliente</script>');assert.match(f.get('#chef-leads-count').textContent,/1 solicitud/);
});
test('switching to team requests only the authenticated account scope and preserves account binding',async()=>{
 const f=fixture();await turn();await f.get('[data-chef-scope="team"]').fire('click');await turn();assert.equal(f.calls.at(-1).url,'/api/chef/workspace?scope=team');assert.equal(f.calls.at(-1).options.headers['X-TEC-Account-ID'],'eric');assert.equal(f.get('[data-chef-scope="team"]').attrs['aria-selected'],'true');
});
test('account change clears contacts, share URL and QR and waits for a fresh page',async()=>{
 const f=fixture();await turn();f.setHandler(async()=>({ok:false,status:403,data:{error:'La cuenta cambió.'}}));await f.get('#refresh-chef-leads').fire('click');await turn();assert.equal(f.get('#chef-share-url').value,'');assert.equal(f.get('#chef-personal-qr').hidden,true);assert.equal(f.get('#chef-leads').children.length,0);assert.equal(f.get('#chef-leads-count').textContent,'');assert.match(f.get('#workspace-status').textContent,/cuenta cambió/);assert.equal(f.timers.size,0);await f.get('#refresh-chef-leads').fire('click');assert.equal(f.reloads,1);
});
test('a connection failure preserves loaded contacts while backgrounding pauses automatic refresh',async()=>{
 const f=fixture();await turn();const first=f.get('#chef-leads').children[0];f.setHandler(async()=>{throw Error('Sin conexión');});await f.get('#refresh-chef-leads').fire('click');await turn();assert.equal(f.get('#chef-leads').children[0],first);assert.match(f.get('#workspace-status').textContent,/Sin conexión/);assert.equal(f.get('#refresh-chef-leads').disabled,false);f.document.hidden=true;await f.document.fire('visibilitychange');assert.equal(f.timers.size,0);
});
