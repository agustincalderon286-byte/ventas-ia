import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import vm from 'node:vm';
const ux=fs.readFileSync(new URL('../public/coach/signup-ux.js',import.meta.url),'utf8');
const app=fs.readFileSync(new URL('../public/coach/app.js',import.meta.url),'utf8');
const plan=app.slice(app.indexOf('async function initPlanPage()'),app.indexOf('async function initLoginPage()'));
class Node{
 constructor(name=''){this.name=name;this.value='';this.textContent='';this.dataset={};this.attrs={};this.hidden=false;this.disabled=false;this.nodes=new Map();this.events=new Map();this.classes=new Set();this.classList={add:n=>this.classes.add(n),remove:n=>this.classes.delete(n),toggle:(n,on)=>on?this.classes.add(n):this.classes.delete(n)};this.validity={typeMismatch:false};}
 querySelector(k){return this.nodes.get(k)||null;}querySelectorAll(k){return this.nodes.get(k)||[];}
 setAttribute(k,v){this.attrs[k]=v;}getAttribute(k){return this.attrs[k]??null;}removeAttribute(k){delete this.attrs[k];}hasAttribute(k){return k in this.attrs;}
 addEventListener(type,fn,capture=false){const arr=this.events.get(type)||[];arr.push({fn,capture});this.events.set(type,arr);}
 async fire(type){const event={preventDefault(){this.prevented=true;},stopImmediatePropagation(){this.stopped=true;}};const tasks=[];for(const item of [...(this.events.get(type)||[])].sort((a,b)=>Number(b.capture)-Number(a.capture))){if(event.stopped)break;tasks.push(item.fn(event));}await Promise.all(tasks);return event;}
 focus(){this.focused=true;}scrollIntoView(){}
}
async function fixture(request=async()=>({url:'/checkout-test'})){
 const root=new Node(),window={location:{}},form=new Node(),message=new Node(),existing=new Node(),button=new Node(),fields=['name','email','password'].map(n=>new Node(n)),planInput=new Node('plan');planInput.value='monthly';
 const errors=Object.fromEntries(fields.map(n=>[n.name,new Node()]));for(const n of fields)form.nodes.set(`[data-signup-error="${n.name}"]`,errors[n.name]);
 form.nodes.set('input',[planInput,...fields]);form.nodes.set('.form-result',message);form.nodes.set('[data-signup-existing-account]',existing);form.nodes.set('button[type="submit"]',button);button.textContent='Crear cuenta y continuar al mensual';
 root.nodes.set('[data-coach-plan-page]',new Node());root.nodes.set('[data-signup-checkout-form]',form);root.nodes.set('[data-plan-input]',planInput);root.nodes.set('[data-plan-submit-label]',button);
 const cards=['monthly','annual'].map(n=>{const x=new Node();x.dataset.plan=n;return x;});const choices=['monthly','annual'].map(n=>{const x=new Node();x.dataset.planSelect=n;x.textContent=n;return x;});root.nodes.set('[data-plan-card]',cards);root.nodes.set('[data-plan-select]',choices);
 let calls=0;const configs=Object.fromEntries(['monthly','annual'].map(n=>[n,{name:n,copy:n,signupLabel:n,memberCopy:n,memberLabel:n}]));
 const context=vm.createContext({window,document:root,WeakMap,console,fetchViewer:async()=>({authenticated:false,user:null}),updateAuthTargets(){},hasCoachAccess:()=>false,COACH_PLAN_CONFIG:configs,apiRequest:async(...args)=>{calls++;return request(...args);},clearMessage:n=>{n.textContent='';},setMessage:(n,text,state)=>{n.textContent=text;n.dataset.state=state;},setButtonLoading:(n,loading)=>{n.disabled=loading;},FormData:class{constructor(){this.values=new Map([planInput,...fields].map(n=>[n.name,n.value]));}get(k){return this.values.get(k);}}});
 vm.runInContext(ux,context);const controller=window.TecSignupUx.init(form);vm.runInContext(plan,context);await vm.runInContext('initPlanPage()',context);
 return{root,window,context,form,message,existing,button,fields,errors,controller,planInput,cards,choices,get calls(){return calls;},fill(){fields[0].value='Usuario de prueba';fields[1].value='prueba@example.com';fields[2].value='clavePrueba9';}};
}
test('empty and malformed fields stop checkout, explain each field and focus the first error',async()=>{
 const f=await fixture();await f.form.fire('submit');assert.equal(f.calls,0);assert.ok(f.fields[0].focused);assert.ok(f.message.textContent.includes('campos'));for(const n of f.fields){assert.equal(n.getAttribute('aria-invalid'),'true');assert.equal(f.errors[n.name].hidden,false);}
 f.fields[0].value='   ';f.fields[1].value='correo-invalido';f.fields[2].value='corta';await f.form.fire('submit');assert.equal(f.calls,0);assert.match(f.errors.name.textContent,/nombre/);assert.match(f.errors.email.textContent,/correo válido/);assert.match(f.errors.password.textContent,/8 caracteres/);
});
test('correcting a field clears its error without losing the other inputs or chosen plan',async()=>{
 const f=await fixture();await f.choices[1].fire('click');await f.form.fire('submit');f.fields[1].value='prueba@example.com';await f.fields[1].fire('input');assert.equal(f.errors.email.hidden,true);assert.equal(f.fields[1].getAttribute('aria-invalid'),'false');assert.equal(f.planInput.value,'annual');assert.equal(f.errors.password.hidden,false);f.fill();await f.form.fire('submit');assert.equal(f.calls,1);assert.equal(f.window.location.href,'/checkout-test');
});
test('existing email errors return focus and an entry link while retaining the password and plan',async()=>{
 const f=await fixture(async()=>{throw Object.assign(new Error('Ese correo ya tiene cuenta'),{status:409});});f.fill();await f.choices[1].fire('click');await f.form.fire('submit');assert.equal(f.calls,1);assert.match(f.errors.email.textContent,/ya tiene una cuenta/);assert.equal(f.existing.hidden,false);assert.ok(f.fields[1].focused);assert.equal(f.fields[2].value,'clavePrueba9');assert.equal(f.planInput.value,'annual');assert.equal(f.button.disabled,false);assert.equal(f.form.getAttribute('aria-busy'),'false');
});
test('network/server failures explain recovery and unlock retry without leaking backend details',async()=>{
 const f=await fixture(async()=>{throw Object.assign(new Error('SECRET_BACKEND_TRACE'),{status:500});});f.fill();await f.form.fire('submit');assert.equal(f.existing.hidden,false);assert.match(f.message.textContent,/cuenta ya se creó/);assert.doesNotMatch(f.message.textContent,/SECRET/);assert.equal(f.button.disabled,false);assert.ok(f.choices.every(n=>!n.disabled));
});
test('duplicate submits make a single checkout request and temporarily lock plan selection',async()=>{
 let finish;const f=await fixture(()=>new Promise(resolve=>{finish=resolve;}));f.fill();const first=f.form.fire('submit');await Promise.resolve();assert.equal(f.calls,1);assert.equal(f.form.getAttribute('aria-busy'),'true');assert.ok(f.choices.every(n=>n.disabled));await f.form.fire('submit');assert.equal(f.calls,1);finish({url:'/checkout-test'});await first;assert.equal(f.form.getAttribute('aria-busy'),'false');
});
test('before the account controls are ready, form submission cannot navigate with credentials',async()=>{
 const form=new Node(),message=new Node(),root=new Node();form.nodes.set('input',[]);form.nodes.set('.form-result',message);root.nodes.set('[data-signup-checkout-form]',form);vm.runInNewContext(ux,{window:{},document:root});const event=await form.fire('submit');assert.equal(event.prevented,true);assert.equal(event.stopped,true);assert.match(message.textContent,/preparando/);
});
test('password toggle preserves selection, changes its text and re-hides on submit and page restore',async()=>{
 const input=new Node(),button=new Node(),form=new Node(),window=new Node();input.type='password';input.selectionStart=2;input.selectionEnd=5;input.setSelectionRange=(a,b)=>{input.selected=[a,b];};input.form=form;button.setAttribute('aria-controls','password');button.setAttribute('data-password-toggle-text','');const document={querySelectorAll:()=>[button],getElementById:()=>input};vm.runInNewContext(fs.readFileSync(new URL('../public/coach/password-visibility.js',import.meta.url),'utf8'),{document,window});await button.fire('click');assert.equal(input.type,'text');assert.equal(button.textContent,'Ocultar');assert.equal(button.getAttribute('aria-pressed'),'true');assert.deepEqual(input.selected,[2,5]);await form.fire('submit');assert.equal(input.type,'password');assert.equal(button.textContent,'Mostrar');await button.fire('click');await window.fire('pageshow');assert.equal(input.type,'password');
});
