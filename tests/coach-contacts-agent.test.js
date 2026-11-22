import test from 'node:test';
import assert from 'node:assert/strict';
import {randomBytes} from 'node:crypto';
import {createCoachContactsAgent} from '../src/coach-contacts-agent.js';
import {createCoachSheetTools} from '../src/coach-sheets-tools.js';

const emails=['agustin@example.invalid','rigoberto@example.invalid'];
const draft='Guarda a María en mis contactos, su teléfono es (312) 555-0140.';
const referral='Guarda en mis contactos a María, la referencia de Rosa de mi hoja.';
function fixture() {
 const records=new Map(),writes=[],reads=[];
 const people={agustin:[],rigoberto:[]},grants={agustin:'ga',rigoberto:'gr'};
 const leads={agustin:{phone:'(312) 555-0191',seller:'agustin'},rigoberto:{phone:'(312) 555-0192',seller:'rigoberto'}};
 const plans={
   async load(scope){return structuredClone(records.get(scope));},
   async save(scope,prior,data){if (records.get(scope)?.revision!==prior)return null;const revision=randomBytes(12).toString('hex');records.set(scope,{revision,data:structuredClone(data)});return revision;}
 };
 const contacts={
    async profile(owner){return {grant:grants[owner]};},
    async search(owner,grant,person){assert.equal(grant,grants[owner]);reads.push({channel:'contacts',owner,person});return structuredClone(people[owner]);},
    async create(owner,grant,person){assert.equal(grant,grants[owner]);writes.push({owner,op:'create',person});return {resourceName:'people/c1'};},
    async update(owner,grant,expected,person){assert.equal(grant,grants[owner]);writes.push({owner,op:'update',expected,person});return {resourceName:expected.resourceName};}
 };
 const sheets={
    access:async owner=>({grant:'s'+owner,token:'t'+owner,spreadsheetId:'spreadsheet-test',ticketsSheetId:0,referralsSheetId:3}),
    current:async(owner,grant)=>grant==='s'+owner
 };
 const sheetTools=createCoachSheetTools({google:{async readFollowups(access,book,opts){
   assert.equal(opts.forPlan,true);
   const owner=access.slice(1);reads.push({channel:'sheets',owner});
   return {locale:'en_US',rows:Object.entries(leads).map(([person,r],i)=>({kind:'referrals',fullName:'María Prueba',host:'Rosa',
     email:'',phone:r.phone,seller:r.seller,sellerColumn:true,sheetId:3,sheetTitle:'4 en 14',row:4+i,history:'Pendiente',status:'',result:''}))};
 }}});
 const config={contactsReady:true,planPilot:emails,sheetsPilot:emails};
 const run=createCoachContactsAgent({config,contacts,plans,sheets,sheetTools,switched:req=>!!req.switched,readSession:req=>req.auth,
   infer:async({instruction})=>({name:instruction.includes('María')?'María':'',host:instruction.includes('Rosa')?'Rosa':'',sheet:instruction.includes('hoja'),readOnly:false})});
 function ask(owner,text,revision='',opts={}) {
   const auth={user:{_id:owner,email:owner+'@example.invalid',name:owner},session:{tokenHash:opts.login||'login-'+owner,accessMode:'direct'}};
   const req={auth,switched:opts.switched,body:{accountId:opts.accountId||owner,coachQuestionFocus:revision?'contacts':'',coachContactsPending:revision}};
   return run({req,auth,text,sessionId:opts.chat || 'chat-prueba'});
 }
 function confirm(owner,result,op='create',i=0,opts) {
   return ask(owner,'TECPERSON '+result.coachContactsPending+' '+op+' '+i,result.coachContactsPending,opts);
 }
 return {ask,confirm,writes,reads,people,leads,grants,config};
}

test('una propuesta no escribe, ni un sí textual; dos toques simultáneos en el botón crean solo un contacto',async()=>{
 const f=fixture();
 const proposal=await f.ask('agustin',draft);
 assert.match(proposal.respuesta,/María/);assert.deepEqual(f.writes,[]);
 const yes=await f.ask('agustin','sí',proposal.coachContactsPending);assert.match(yes.respuesta,/botón/);assert.deepEqual(f.writes,[]);
 const answers=await Promise.all([f.confirm('agustin',proposal),f.confirm('agustin',proposal)]);
 assert.equal(answers.filter(a=>/Contacto creado/.test(a.respuesta)).length,1);assert.equal(f.writes.length,1);
 assert.deepEqual(f.writes[0],{op:'create',owner:'agustin',person:{name:'María',phone:'(312) 555-0140',email:''}});
});

test('bloquea propuesta de otro vendedor, login y chat, suplantación y cambio del permiso de Google',async()=>{
 const f=fixture();
 const draftA=await f.ask('agustin',draft);
 for (const [owner,opts] of [['rigoberto',{}],['agustin',{chat:'otro-chat'}],['agustin',{login:'otro-login'}],
   ['agustin',{accountId:'rigoberto'}],['agustin',{switched:true}]]) {
   const res=await f.confirm(owner,draftA,'create',0,opts);assert.doesNotMatch(res.respuesta,/Contacto creado/);
 }
 f.grants.agustin='otro-permiso';
 assert.match((await f.confirm('agustin',draftA)).respuesta,/cambió o caducó/);assert.deepEqual(f.writes,[]);
});

test('si aparece un duplicado o cambia el etag tras la vista previa, exige volver a revisar',async()=>{
 const f=fixture();
 let prepared=await f.ask('agustin',draft);
 f.people.agustin.push({resourceName:'people/c12',etag:'v1',name:'María Estudio',phones:['+13125550140'],emails:[]});
 assert.match((await f.confirm('agustin',prepared)).respuesta,/agenda cambió/);assert.deepEqual(f.writes,[]);
 prepared=await f.ask('agustin',draft);
 assert.ok(prepared.coachHelpActions.find(a=>a.op==='update'));
 f.people.agustin[0].etag='v2';
 assert.match((await f.confirm('agustin',prepared,'update',1)).respuesta,/agenda cambió/);assert.deepEqual(f.writes,[]);
 const fresh=await f.ask('agustin',draft);await f.confirm('agustin',fresh,'update',1);
 assert.equal(f.writes[0].expected.etag,'v2');assert.equal(f.writes[0].op,'update');
});

test('el contacto desde Sheets toma el teléfono de la fila del vendedor y revalida una reasignación antes de escribir',async()=>{
 const f=fixture();
 const notices=await Promise.all(['agustin','rigoberto'].map(owner=>f.ask(owner,referral)));
 assert.match(notices[0].respuesta,/María Prueba/);
 assert.match(notices[0].respuesta,/\+13125550191/);assert.match(notices[1].respuesta,/\+13125550192/);
 assert.doesNotMatch(notices[0].respuesta,/0192/);assert.deepEqual(f.writes,[]);
 f.leads.agustin.seller='otro vendedor';
 const stale=await f.confirm('agustin',notices[0]);assert.doesNotMatch(stale.respuesta,/Contacto creado/);assert.deepEqual(f.writes,[]);
 const ok=await f.confirm('rigoberto',notices[1]);
 assert.match(ok.respuesta,/Contacto creado/);assert.equal(f.writes[0].person.phone,'+13125550192');
});
