import test from 'node:test';
import assert from 'node:assert/strict';
import {randomBytes} from 'node:crypto';
import {createCoachSheetTools} from '../src/coach-sheets-tools.js';
import {interpretCoachPlan} from '../src/coach-plan-language.js';
import {createConversationPlan} from '../src/coach-conversation-plan.js';
import {summarizeMessages,sealToken,gmailClient} from '../src/coach-hycite-gmail.js';
import {createPostSaleReader,reviewConfirmedSales,salePeriod} from '../src/coach-postsale.js';

const now=Date.parse('2026-11-21T16:00:00Z'), today='2026-11-21', zone='America/Chicago';
const range={...salePeriod('week',today),today,zone,locale:'en_US'};
const emails=['agustin@example.invalid','rigoberto@example.invalid'];
function row(name,id,history,extra={}) {
  return {kind:'tickets',sheetTitle:'BOLETAS NUEVAS',sheetId:0,row:id,fullName:name,host:'',seller:'Agustin',sellerColumn:true,
    phone:'31255501'+String(id).padStart(2,'0'),email:'',followupDate:'',status:'',result:'',history,
    editable:{history:{column:8,displayed:history,formula:false}},...extra};
}
const hit=(r)=>({id:'id'+r.row,key:'key'+r.row,row:r});
const sale=(order,customer={},date='2026-11-19T17:00:00Z')=>({confirmed:true,order,date,customer});
function email(id,order,snippet) {
  return {id,internalDate:String(now),snippet,payload:{headers:[
    {name:'From',value:'Hy Cite <notices@hycite.com>'},{name:'Authentication-Results',value:'mx.google.com; dmarc=pass header.from=hycite.com'},
    {name:'Subject',value:'Sale confirmed #'+order}
  ]}};
}

test('solo se extraen identificadores etiquetados del comprador en la consulta puntual de postventa',()=>{
 const data=[email('A','AB-12','Customer phone: (312) 555-0199; Customer email: buyer@example.invalid. Seller phone: 7735550111.'),
   email('B','AB-13','Vendedor: seller@example.invalid. Tel: 1234567890'), email('C','AB-14','Correo del cliente: a@example.invalid; Correo del cliente: b@example.invalid')];
 const normal=summarizeMessages(data,['hycite.com']), report=summarizeMessages(data,['hycite.com'],{forPostSale:true});
 assert.ok(normal.confirmed.every(r=>!Object.hasOwn(r,'customer')));
 assert.deepEqual(report.confirmed.map(s=>s.customer),[{phone:'(312) 555-0199',email:'buyer@example.invalid'}, {}, {}]);
});

test('el cliente Gmail consume la lista y los campos reales de mensajes sin obtener el cuerpo del correo',async t=>{
 const paths=[];
 t.mock.method(globalThis,'fetch',async(input,opts)=>{
   const url=new URL(input);paths.push(url.pathname);
   assert.equal(opts.headers.Authorization,'Bearer prueba-token');
   if (url.pathname.endsWith('/messages')) {
     assert.match(url.searchParams.get('q'),/hycite\.com/);
     return Response.json({messages:[{id:'m1',threadId:'h1'}],nextPageToken:'hay-mas'});
   }
   assert.equal(url.searchParams.get('fields'),'id,internalDate,snippet,payload/headers');
   return Response.json(email('m1','AB-14','Teléfono del cliente: (312) 555-0199.'));
 });
 const summary=await gmailClient({domains:['hycite.com']}).notices('prueba-token',{forPostSale:true});
 assert.deepEqual(paths,['/gmail/v1/users/me/messages','/gmail/v1/users/me/messages/m1']);
 assert.equal(summary.limited,true);assert.deepEqual(summary.confirmed[0].customer,{phone:'(312) 555-0199'});
});

test('postventa distingue coincidencias exactas, gestiones fechadas, exclusiones y pedidos sin vínculo',()=>{
 const rows=[
   row('Ana',4,'19/11/26: pedido: X-11. / Viernes. 20/11/26. Le ofrecí una demo de uso.'),
   row('Luz',5,'2026-11-17: ofrecí una demo de uso. / 2026-11-20: no contestó.',{order:'X-12'}),
   row('Beto',6,'2026-11-20: no volver a llamar',{email:'beto@example.invalid'}),
   row('Luis',7,'2026-11-21: llamar mañana',{email:'duplicado@example.invalid'}),
   row('José',8,'2026-11-21: buzón',{email:'duplicado@example.invalid'})
 ];
 const snapshot={limited:true,confirmed:[sale('X-11'),sale('X-12'),sale('X-13',{email:'beto@example.invalid'}),sale('X-14',{email:'duplicado@example.invalid'}),
   sale('X-15',{email:'nadie@example.invalid'}),sale('X-16',{phone:rows[1].phone,email:'duplicado@example.invalid'}),
   sale('X-25',{},'2026-11-16T01:00:00Z'),{...sale('X-17'),confirmed:false}]};
 const result=reviewConfirmedSales(snapshot,rows.map(hit),range);
 assert.equal(result.count,6);assert.equal(result.completed,1);assert.equal(result.excluded,1);
 assert.deepEqual(result.pending.map(x=>x.order),['X-12']);assert.deepEqual(result.ambiguous,['X-14','X-16']);
 assert.deepEqual(result.unmatched,['X-15']);assert.equal(result.limited,true);
 assert.deepEqual(reviewConfirmedSales({confirmed:[sale('X-11',{phone:'3125550122'})]},rows.map(hit),range).ambiguous,['X-11']);
 const buyer=hit(row('Ana',4,'2026-11-21: ofrecí una demo de uso.'));
 const multiple={confirmed:[sale('X-11',{phone:buyer.row.phone}),sale('X-12',{phone:buyer.row.phone})]};
 assert.equal(reviewConfirmedSales(multiple,[buyer],range).completed,0);
 buyer.row.history+=' / 2026-11-21: Pedido: X-12. Le ofrecí una demo de uso.';
 assert.equal(reviewConfirmedSales(multiple,[buyer],range).completed,1);
});

test('el lector usa únicamente la autorización personal y descarta resultados tras desconexión',async()=>{
 const key=randomBytes(32),actor={owner:'agustin',email:emails[0],current:async()=>true};
 let grant='g',switched=false,calls=0;
 const store={findById(owner){assert.equal(owner,'agustin');return {lean:async()=>({mailbox:emails[0],grant,encryptedToken:sealToken('refresh',key,owner)})};}};
 const gmail={refresh:async value=>{assert.equal(value,'refresh');return {access_token:'access'};},
   notices:async(token,opts)=>{calls++;assert.equal(token,'access');assert.deepEqual(opts,{forPostSale:true});if (switched)grant='otra';return {confirmed:[]};}};
 const read=createPostSaleReader({config:{key,ready:true,pilot:emails},store,gmail});
 assert.equal((await read(actor)).grant,'g');
 await assert.rejects(read({...actor,email:'fuera@example.invalid'}),/propia/);
 await assert.rejects(read({...actor,current:async()=>false}),/propia/);
 await assert.rejects(read(actor,{knownGrant:'ajeno'}),/cambió/);
 switched=true;await assert.rejects(read(actor),/Cambió/);assert.equal(calls,2);
});

function fixture() {
 const rows=[row('Ana',4,'2026-11-17: llamar el 2026-11-20'),row('Bea',5,'Llamar el 2026-11-21'),row('Eva',6,'Buzón'),
   row('Sol',7,'Llamar el 2026-11-18',{seller:'Rigoberto'}),row('Excluida',8,'No llamar',{followupDate:today}),
   row('Corta',9,'Llamar el 2026-11-16',{phone:'312-55'}),row('Mañana',10,'Llamar el 2026-11-22')];
 const records=new Map(),updates=[],scans=[];
 const plans={
   async load(s){return structuredClone(records.get(s));},
   async save(s,rev,data){if (rev!==records.get(s)?.revision)return null;const revision=randomBytes(12).toString('hex');records.set(s,{revision,data:structuredClone(data)});return revision;}
 };
 const sheets={access:async owner=>({token:'t'+owner,grant:'g'+owner,spreadsheetId:'test-file-id',ticketsSheetId:0,referralsSheetId:3}),current:async()=>true};
 const google={async readFollowups(token){scans.push(token);return {rows:structuredClone(rows),locale:'en_US'};},
   async replaceFollowupCell(token,book,{row:n,before,after}){const sheet=rows.find(r=>r.row===n);assert.equal(sheet.history,before);
     sheet.history=sheet.editable.history.displayed=after;updates.push(['note',n]);return true;}};
 const channels={appointment:async actor=>({state:{},proposal:{owner:actor.owner},preview:'Llamada del jueves a las 18:30'}),
   confirmAppointment:async(actor,p)=>{assert.equal(p.owner,actor.owner);updates.push(['calendar',actor.owner]);return {status:'saved',message:'Cita confirmada'};}};
 const snapshots=new Map([['Agustin',{grant:'ga',confirmed:[sale('A-100',{phone:rows[0].phone})]}],['Rigoberto',{grant:'gr',confirmed:[sale('R-100',{phone:rows[3].phone})]}]]);
 const readSales=async(actor,{verifyOnly,knownGrant})=>{const snap=snapshots.get(actor.owner);if (knownGrant)assert.equal(knownGrant,snap.grant);return verifyOnly?undefined:snap;};
 const answers=new Map([
   ['Dame el siguiente pendiente',{kind:'plan',operations:['call']}],
   ['Le llamé, recuérdame el jueves a las seis y media de la tarde',{kind:'revise',operations:['note','appointment'],note:'Le llamé',noteSchedule:'el jueves',schedule:'el jueves a las seis y media de la tarde'}],
   ['¿A quién falta ofrecer demo de uso de mis ventas de esta semana?',{kind:'plan',operations:['postSale'],salesPeriod:'week'}]
 ]);
 const run=createConversationPlan({config:{sheetsPilot:emails,planPilot:emails},plans,sheets,sheetTools:createCoachSheetTools({google}),
   channels,switched:req=>!!req.switched,readSession:req=>req.auth,interpret:async t=>answers.get(t),readSales,now:()=>now});
 function ask(owner,text,rev='',body={}) {
   const auth={user:{_id:owner,name:owner,email:owner.toLowerCase()+'@example.invalid'},session:{tokenHash:'login'+owner,accessMode:'direct'}};
   const req={auth,body:{accountId:owner,coachPlanVersion:1,coachPlanPending:rev,coachTimeZone:zone,...body}};
   return run({auth,req,text,sessionId:'personal'});
 }
 return {ask,updates,scans,snapshots};
}

test('cada vendedor recibe su cola ordenada; el resultado dictado prepara la nota y el recordatorio, con confirmaciones separadas',async()=>{
 const f=fixture();
 const a=await f.ask('Agustin','Dame el siguiente pendiente'),b=await f.ask('Rigoberto','Dame el siguiente pendiente');
 assert.match(a.respuesta,/Venció el 2026-11-20/);assert.match(b.respuesta,/Sol/);
 assert.equal(a.coachHelpActions.find(x=>x.type==='planCall').phone,'+13125550104');
 assert.equal(b.coachHelpActions.find(x=>x.type==='planCall').phone,'+13125550107');
 assert.ok(!a.respuesta.includes('Sol'));assert.ok(!b.respuesta.includes('Ana'));
 const draft=await f.ask('Agustin','Le llamé, recuérdame el jueves a las seis y media de la tarde',a.coachPlanPending);
 assert.match(draft.respuesta,/Nota en Sheets/);assert.match(draft.respuesta,/Cita personal/);assert.deepEqual(f.updates,[]);
 const blocked=await f.ask('Agustin','Dame el siguiente pendiente',draft.coachPlanPending);
 assert.match(blocked.respuesta,/cambios por revisar/);assert.deepEqual(f.updates,[]);
 const apply=(rev,k)=>'TECPLAN '+rev+' apply '+k;
 const stolen=await f.ask('Rigoberto',apply(draft.coachPlanPending,'note'),draft.coachPlanPending);assert.match(stolen.respuesta,/caducó/);
 const noted=await f.ask('Agustin',apply(draft.coachPlanPending,'note'),draft.coachPlanPending);assert.deepEqual(f.updates,[['note',4]]);
 const booked=await f.ask('Agustin',apply(noted.coachPlanPending,'appointment'),noted.coachPlanPending);
 assert.deepEqual(f.updates,[['note',4],['calendar','Agustin']]);assert.match(booked.respuesta,/confirmada/);
 const next=await f.ask('Agustin','Dame el siguiente pendiente',booked.coachPlanPending);assert.match(next.respuesta,/Bea/);
 const undated=await f.ask('Agustin','Dame el siguiente pendiente',next.coachPlanPending);assert.match(undated.respuesta,/Pendiente sin fecha/);
 const done=await f.ask('Agustin','Dame el siguiente pendiente',undated.coachPlanPending);assert.match(done.respuesta,/Ya recorriste/);
});
test('selecciona una postventa de la cuenta correcta y rechaza una venta retirada antes de confirmar',async()=>{
 const f=fixture();
 for (const [owner,order,name] of [['Agustin','A-100','Ana'],['Rigoberto','R-100','Sol']]) {
   const report=await f.ask(owner,'¿A quién falta ofrecer demo de uso de mis ventas de esta semana?');
   assert.match(report.respuesta,new RegExp(order));assert.match(report.respuesta,new RegExp(name));
   const selected=await f.ask(owner,'TECPLAN '+report.coachPlanPending+' contact 1',report.coachPlanPending);
   assert.ok(selected.coachHelpActions.find(a=>a.type==='planCall'));
   const draft=await f.ask(owner,'Le llamé, recuérdame el jueves a las seis y media de la tarde',selected.coachPlanPending);
   f.snapshots.get(owner).confirmed=[];
   const stopped=await f.ask(owner,'TECPLAN '+draft.coachPlanPending+' apply appointment',draft.coachPlanPending);
   assert.match(stopped.respuesta,/confirmación o el historial/);assert.deepEqual(f.updates,[]);
 }
});

test('el lenguaje cotidiano para marcar ahorita funciona incluso si el intérprete no está disponible',async()=>{
 const offline=async()=>{throw Error('intérprete no disponible');};
 for (const phrase of ['¿A quién le marco ahorita?','dame el siguiente pendiente','a quién llamo primero por favor']) {
   const intent=await interpretCoachPlan(phrase,{infer:offline});
   assert.deepEqual(intent.operations,['call']);
 }
 for (const other of ['No me digas a quién le marco ahorita','¿A quién le marco ahorita para la postventa de la semana?']) {
   const intent=await interpretCoachPlan(other,{infer:offline});assert.deepEqual(intent.operations,[]);
 }
});
