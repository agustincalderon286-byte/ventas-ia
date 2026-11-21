import test from 'node:test';
import assert from 'node:assert/strict';
import {createConversationPlan} from '../src/coach-conversation-plan.js';
import {readFollowup,followupTime} from '../src/coach-followup-input.js';

const start='La que me refirió Rosa quiere que le marque el viernes. Anótalo y déjame un correo para confirmar la llamada.';
const clarify='A Luisa, y mejor a las cinco de la tarde.';
const choose='Guarda la nota y la cita, pero el correo déjamelo tantito.';
const overdue='Dime a quién se me pasó marcar y prepara un correo para el primero.';
const resolve={
  [start]:{kind:'plan',operations:['list','note','email','appointment'],host:'Rosa',note:'quiere que le marque el viernes',schedule:'el viernes',mailInstruction:'confirmar la llamada'},
  [clarify]:{kind:'revise',operations:[],contact:'Luisa',schedule:'a las cinco de la tarde'},
  [choose]:{kind:'select',select:['note','appointment']},
  'Ahora sí envíalo':{kind:'select',select:['email'],sendEmail:true},
  '¿A quién le toca hoy?':{kind:'plan',operations:['list'],bucket:'today'},
  [overdue]:{kind:'plan',operations:['list','email'],bucket:'overdue',ordinal:1,mailInstruction:'saludarle'},
  'Cámbialo al jueves a las seis de la tarde':{kind:'revise',operations:[],schedule:'jueves a las seis de la tarde'},
  '¿Cómo vendo mejor?':{kind:'none'}
};
function fixture() {
  const docs=new Map(),writes=[], prepared=[],checks=[],grants={a:'sa',r:'sr'};
  const now=Date.parse('2026-11-21T15:00:00Z');
  let seq=0,expired=false,conflict=false,failOnce=false;
  const plans={
    async load(key){const doc=structuredClone(docs.get(key));return expired && doc?{...doc,data:null}:doc;},
    async save(key,prev,data){if(docs.get(key)?.revision!==prev) return null;
      const revision=(++seq).toString(16).padStart(24,'0');docs.set(key,{revision,data:structuredClone(data)});return revision;}
  };
  const contacts=(owner)=>Array.from({length:14},(_,i)=>({fullName:i===0?'Luisa':i===1?'María':'Persona '+i,
    host:'Rosa',key:owner+i,email:'ficticio'+i+'@example.com',identityKey:owner+i,sheetTitle:'4 en 14',row:4+i,reason:'Se venció el seguimiento.'}));
  const runner=createConversationPlan({config:{sheetsPilot:['a@example.com','r@example.com']},plans,now:()=>now,
    readSession:async req=>req.auth,switched:req=>req.switched,interpret:async t=>resolve[t] || {kind:'none'},
    sheets:{access:async owner=>({token:'t',grant:grants[owner],spreadsheetId:'w'+owner}),current:async (owner,g)=>g===grants[owner]},
    sheetTools:{
      search:async (ctx,{name,host,bucket,offset=0})=>{let rows=contacts(ctx.owner);if(bucket==='today') rows=[];if(host) rows=rows.slice(0,2);if(name) rows=rows.filter(x=>x.fullName===name);return {total:rows.length,contacts:rows.slice(offset,offset+12)};},
      revalidate:async(ctx,id)=>{checks.push([ctx.owner,id]);if(conflict) throw Object.assign(Error('Cambió la fila.'),{status:409});},
      prepareNote:async(ctx,data)=>({preview:data.note,proposal:data}),
      confirmNote:async(ctx,data)=>{writes.push(['note',ctx.owner,data.key]);return {status:'saved',message:'Nota guardada.'};}
    },
    channels:{
      appointment:async(actor,state,{name,schedule})=>{const next=readFollowup(schedule,state,{timeZone:actor.zone,now}).state;
        const when=followupTime(next,now);if(when.question)return {state:next,question:when.question};
        prepared.push(['appointment',actor.owner,when.start]);return {state:next,preview:name+' '+when.start,whenText:when.start,proposal:{start:when.start}};},
      email:async(actor,{to,instruction,whenText})=>{prepared.push(['mail',actor.owner,to,whenText]);return {proposal:{to},preview:'Borrador '+to+' '+instruction+' '+(whenText||'')};},
      confirmAppointment:async(actor,p)=>{if(failOnce){failOnce=false;return {status:'retry',message:'Reintenta.'};}writes.push(['appointment',actor.owner,p.start]);return {status:'saved',message:'Cita confirmada.'};},
      confirmEmail:async(actor,p)=>{writes.push(['email',actor.owner,p.to]);return {status:'saved',message:'Enviado.'};}
    }
  });
  async function ask(owner,text,rev='',login='personal',extra={}) {
    const auth={user:{_id:owner,email:owner+'@example.com',name:owner},session:{accessMode:'direct',tokenHash:login}};
    const req={auth,body:{coachPlanVersion:1,coachTimeZone:'America/Chicago',accountId:owner,coachPlanPending:rev}};
    return runner({req,auth,sessionId:'mi-chat',text,...extra});
  }
  const click=(owner,btn,rev=btn.revision,login)=>ask(owner,['TECPLAN',btn.revision,btn.op,btn.arg].join(' '),rev,login);
  return {ask,click,writes,prepared,checks,grants,expire:()=>expired=true,move:()=>conflict=true,fail:()=>failOnce=true};
}
const buttons=(r,op)=>r.coachHelpActions.filter(a=>a.op===op);
const button=(r,op,arg)=>buttons(r,op).find(a=>a.arg===arg);
async function review(f,owner='a'){
  const first=await f.ask(owner,start);assert.equal(buttons(first,'contact').length,2);
  const second=await f.ask(owner,clarify,first.coachPlanPending);
  assert.match(second.respuesta,/2026-11-27T23:00:00/);
  assert.match(second.respuesta,/Borrador ficticio0@example.com/);
  return second;
}
test('cada vendedor desambigua al referido, revisa el horario y confirma solo lo elegido',async()=>{
  const f=fixture();
  for(const owner of ['a','r']){
    const r=await review(f,owner);assert.equal(button(r,'apply','email'),undefined);
    const s=await f.ask(owner,choose,r.coachPlanPending);
    const b=button(s,'apply','note,appointment');assert.ok(b);
    assert.equal(f.writes.filter(w=>w[1]===owner).length,0);
    assert.match((await f.click(owner,{...b,arg:'email'})).respuesta,/cambió/);
    const [x,y]=await Promise.all([f.click(owner,b),f.click(owner,b)]);
    const done=[x,y].find(v=>v.respuesta.includes('Resultado'));
    assert.ok(done);assert.match(done.respuesta,/Borrador/);
    assert.deepEqual(f.writes.filter(w=>w[1]===owner).map(w=>w[0]),['note','appointment']);
    const later=await f.ask(owner,'Ahora sí envíalo',done.coachPlanPending);
    assert.ok(button(later,'apply','email'));assert.equal(f.writes.some(w=>w[0]==='email'),false);
    assert.ok(f.checks.filter(([o])=>o===owner).every(([o,id])=>id.startsWith(o)));
  }
});
test('cambio de conexión, sesión, contacto o revisión impide aplicar una propuesta anterior',async()=>{
  const f=fixture(),r=await review(f),b=button(r,'apply','appointment');
  assert.match((await f.click('r',b)).respuesta,/caducó/);
  assert.match((await f.click('a',b,b.revision,'otro-login')).respuesta,/caducó/);
  const next=await f.ask('a','Cámbialo al jueves a las seis de la tarde',r.coachPlanPending);
  assert.match(next.respuesta,/2026-11-27T00:00:00/);
  assert.match((await f.click('a',b)).respuesta,/caducó/);
  const fresh=button(next,'apply','appointment');f.grants.a='changed';
  assert.match((await f.click('a',fresh)).respuesta,/conexión/);f.grants.a='sa';f.move();
  assert.match((await f.click('a',fresh)).respuesta,/Cambió la fila/);assert.deepEqual(f.writes,[]);
  f.expire();assert.match((await f.click('a',fresh)).respuesta,/caducó/);
});
test('lee hoy, sin fecha y vencidos, navega sin contar boletas ajenas y acepta primero en el mismo turno',async()=>{
  const f=fixture();
  const first=await f.ask('r','¿A quién le toca hoy?');
  assert.match(first.respuesta,/Para hoy \(0\).*Vencidos \(14\).*Pendientes sin fecha \(14\)/s);
  const late=await f.click('r',button(first,'page','overdue1'));
  assert.equal(buttons(late,'contact').length,10);
  const more=await f.click('r',button(late,'page','overdue2'));assert.equal(buttons(more,'contact').length,4);
  assert.match(more.respuesta,/Persona 10/);
  const picked=await f.ask('r',overdue,more.coachPlanPending);
  assert.match(picked.respuesta,/Borrador ficticio0/);assert.deepEqual(f.writes,[]);
});
test('una falla parcial se puede reintentar sin duplicar la nota',async()=>{
  const f=fixture();f.fail();const r=await review(f),s=await f.ask('a',choose,r.coachPlanPending);
  const partial=await f.click('a',button(s,'apply','note,appointment'));assert.match(partial.respuesta,/Reintenta/);
  assert.equal(button(partial,'apply','note'),undefined);
  const done=await f.click('a',button(partial,'apply','appointment'));assert.match(done.respuesta,/Cita confirmada/);
  assert.deepEqual(f.writes.map(w=>w[0]),['note','appointment']);
});
test('una conversación comercial normal pasa al Coach habitual',async()=>{
  const f=fixture();assert.equal(await f.ask('a','¿Cómo vendo mejor?'),null);
});
