import test from 'node:test';
import assert from 'node:assert/strict';
import { createDemoCloseService, demoCloseQuestions, demoCloseText, inDemoClosePilot } from '../src/coach-demo-close.js';

const id = i => i.toString(16).padStart(24, '0');
const visit = 'visit-12345678';
function demo(i, owner, overrides = {}) {
  return {_id:id(i), ownerUserId:owner, closeVisitId:visit, closeCrmId:'', closeVersion:0, status:'completed',
    knownOutcome:'unknown', startedAt:new Date(2026,10,i), transcript:'DATOS PRIVADOS DE PRUEBA', chunks:[{text:'audio privado'}],
    report:{summary:'Resumen de entrenamiento privado',closeFacts:{},overallScore:90}, ...overrides};
}
function readPath(object, path) { return path.split('.').reduce((result, key) => result?.[key], object); }
function writePath(object, path, value) {
  const keys = path.split('.');
  let target = object;
  for (const key of keys.slice(0,-1)) { target[key] ??= {}; target = target[key]; }
  target[keys.at(-1)] = structuredClone(value);
}
function matches(doc, filter) {
  return Object.entries(filter).every(([key, expected]) => {
    const actual = readPath(doc,key);
    if (!expected || typeof expected !== 'object') return actual === expected;
    return Object.entries(expected).every(([op,value]) => {
      if (op === '$type') return typeof actual === value;
      if (op === '$ne') return actual !== value;
      assert.fail('Operador no cubierto por la prueba: '+op);
    });
  });
}
function fixture(rows) {
  const docs = structuredClone(rows), projections = [], writes = [];
  function query(filter, many, change) {
    let keys, sorted, max = Infinity;
    return {
      select(value) { keys = value.split(/\s+/u); projections.push(keys); return this; },
      sort(value) { sorted = value; return this; },
      limit(value) { max = value; return this; },
      async lean() {
        let hits = docs.filter(doc => matches(doc,filter));
        if (sorted?.startedAt === -1) hits.sort((a,b) => b.startedAt - a.startedAt);
        if (change && hits.length) {
          const doc = hits[0];
          for (const [path,value] of Object.entries(change.$set || {})) writePath(doc,path,value);
          for (const [path,value] of Object.entries(change.$inc || {})) writePath(doc,path,readPath(doc,path)+value);
          writes.push(structuredClone(change));
        }
        hits = hits.slice(0,max).map(doc => {
          const record = {_id:doc._id};
          for (const key of keys) {
            const v = readPath(doc,key);
            if (v !== undefined) writePath(record,key,v);
          }
          return record;
        });
        return many ? hits : hits[0] || null;
      }
    };
  }
  return { docs, projections, writes, service:createDemoCloseService({
    find:filter=>query(filter,true), findOne:filter=>query(filter,false), findOneAndUpdate:(filter,change)=>query(filter,false,change)
  }) };
}
const status = code => err => err.status === code;

test('el inbox muestra las tres visitas propias recientes por aclarar y el chat no adivina entre dos grabaciones', async () => {
  const f = fixture([
    demo(1,'A'), demo(2,'A'), demo(3,'A'), demo(4,'A', {closeCrmId:id(52)}), demo(5,'B'),
    demo(6,'A',{closeAnswered:true}),demo(7,'A',{status:'recording'}),demo(8,'A',{closeVisitId:undefined})
  ]);
  assert.deepEqual((await f.service.inbox('A')).map(row=>row.id), [id(4),id(3),id(2)]);
  assert.deepEqual((await f.service.inbox('B')).map(row=>row.id), [id(5)]);
  await assert.rejects(f.service.read('B',id(4)),status(404));
  await assert.rejects(f.service.read('A',id(8)),status(404));
  assert.equal(await f.service.forChat('A',visit,'',''), null);
  assert.equal(await f.service.forChat('A',visit,'',id(5)), null);
  assert.equal(await f.service.forChat('A',visit,id(52),id(2)), null);
  assert.equal((await f.service.forChat('A',visit,id(52),id(4))).visitId,visit);
  for (const keys of f.projections) {
    assert.ok(!keys.some(k=>['report','report.summary','chunks','transcript','ownerEmail','recording','report.strengths'].includes(k)));
  }
});

test('dos confirmaciones de la misma versión producen una sola escritura y un resultado idéntico al reintentar', async () => {
  const f=fixture([demo(1,'A'),demo(2,'B')]);
  const input={visitId:visit,version:0,fields:{outcome:'venta',saleAmount:'1200'}};
  const [first,second]=await Promise.all([f.service.confirm('A',id(1),input),f.service.confirm('A',id(1),input)]);
  assert.deepEqual(first,second);
  assert.equal(first.version,1);
  assert.deepEqual(first.confirmed,input.fields);
  assert.deepEqual(await f.service.confirm('A',id(1),input),first);
  await assert.rejects(f.service.confirm('A',id(1),{...input,fields:{saleAmount:'1100'}}),status(409));
  await assert.rejects(f.service.confirm('B',id(1),input),status(404));
  await assert.rejects(f.service.confirm('A',id(1),{...input,visitId:'visit-87654321'}),status(409));
  assert.equal(f.writes.length,1);
  assert.equal((await f.service.read('B',id(2))).version,0);
  assert.deepEqual(await f.service.inbox('A'),[]);
});

test('un importe cotizado únicamente no registra una venta; corregir venta a seguimiento despeja el total', async () => {
  const f=fixture([demo(1,'A')]);
  const confirm = (version,fields) => f.service.confirm('A',id(1),{visitId:visit,version,fields});
  await assert.rejects(confirm(0,{outcome:'follow_up',saleAmount:'90'}),status(422));
  assert.equal(f.writes.length,0);
  const quote=await confirm(0,{quotedMonthly:'90'});
  assert.equal(quote.confirmed.outcome,undefined);
  await confirm(1,{outcome:'venta',saleAmount:'900'});
  const corrected=await confirm(2,{outcome:'follow_up'});
  assert.equal(corrected.confirmed.saleAmount,undefined);
  assert.equal(corrected.confirmed.quotedMonthly,'90');
});

test('aclarar solo prepara campos, relee la versión y descarta una respuesta tardía tras una corrección', async () => {
  const f=fixture([demo(1,'A',{knownOutcome:'sale',report:{closeFacts:{reason:'Quiere comparar'}}})]);
  const body={visitId:visit,version:0,text:'fueron novecientos'};
  const draft=await f.service.clarify('A',id(1),body,async (text,context)=>{
    assert.equal(text,body.text);
    assert.equal(context.fields.outcome,'venta');
    assert.deepEqual(context.questions.map(q=>q.field),['saleAmount']);
    assert.equal(JSON.stringify(context).includes('privado'),false);
    return {fields:{saleAmount:'900.00'}};
  });
  assert.deepEqual(draft.close.confirmed,{});
  assert.equal(draft.close.answered,false);
  assert.equal(draft.close.fields.saleAmount,'900.00');
  await assert.rejects(f.service.clarify('A',id(1),body,async()=>assert.fail('No se debe consultar al modelo con una versión obsoleta')),status(409));
  let finish, started;
  const pending=new Promise(resolve=>{started=resolve;});
  const delayed=f.service.clarify('A',id(1),{...body,version:1},async()=>{
    started(); return new Promise(resolve=>{finish=resolve;});
  });
  await pending;
  await f.service.confirm('A',id(1),{visitId:visit,version:1,fields:{outcome:'venta',saleAmount:'890'}});
  finish({fields:{saleAmount:'999'}});
  await assert.rejects(delayed,status(409));
  assert.equal((await f.service.read('A',id(1))).confirmed.saleAmount,'890');
  assert.equal(f.writes.length,2);
});

test('conserva una nota comercial opcional como borrador aunque no haya otros campos nuevos', async () => {
  const f=fixture([demo(1,'A')]);
  const note='El monto queda pendiente de confirmar.';
  const result=await f.service.clarify('A',id(1),{visitId:visit,version:0,text:'Mañana confirmo el monto.'},async()=>({fields:{},note}));
  assert.equal(result.understood,true);
  assert.equal(result.close.fields.sellerNote,note);
  assert.deepEqual(result.close.confirmed,{});
  assert.equal(result.close.answered,false);
  assert.equal(f.writes.length,1);
  const next=await f.service.clarify('A',id(1),{visitId:visit,version:1,text:'no sé'},async()=>({fields:{}}));
  assert.equal(next.understood,false);
  assert.equal(f.writes.length,1);
});

test('audio parcial y resultado incierto preguntan sin fabricar venta; las discrepancias generan como máximo dos preguntas', async () => {
  const f=fixture([demo(1,'A',{report:{partial:true,outcomeAssessment:'incierto',closeFacts:{}}}),
    demo(2,'A',{knownOutcome:'no_sale',report:{closeFacts:{outcome:'venta'}}})]);
  const uncertain=await f.service.read('A',id(1));
  assert.deepEqual(uncertain.fields,{});
  assert.equal(uncertain.declaredOutcome,'');
  assert.deepEqual(demoCloseQuestions(uncertain).map(q=>q.field),['outcome']);
  assert.match(demoCloseText(uncertain),/parcial/);
  assert.deepEqual(demoCloseQuestions(await f.service.read('A',id(2))).map(q=>q.field),['outcome','saleAmount']);
});

test('Jorge pregunta primero la cifra que la grabación dejó en duda y la respuesta queda con su producto', async () => {
  const review = [
    { field:'referral', label:'Julia Ríos', state:'guardar' },
    { field:'price', label:'Filtro Premium', state:'preguntar', text:'99 dólares', heard:['799'], reason:'Se oyó voz sin transcribir junto a la cifra.' },
    { field:'note', label:'Julia Ríos', state:'pendiente', text:'renta su casa' }
  ];
  const f=fixture([demo(1,'A',{knownOutcome:'follow_up',autoCloseReceipt:{review,appointment:{state:'saved'}}})]);
  const close=await f.service.read('A',id(1));
  assert.deepEqual(close.doubts,[{ field:'calcPrice', label:'Filtro Premium', text:'99 dólares', heard:['799'] }]);
  const [first]=demoCloseQuestions(close);
  assert.equal(first.field,'calcPrice');
  assert.equal(first.question,'Del Filtro Premium oí «99 dólares», pero pudo quedar cortado (la otra lectura oyó 799). ¿Cuál fue el precio?');
  const answered=await f.service.clarify('A',id(1),{visitId:visit,version:0,text:'799'},async (text,context)=>{
    assert.deepEqual(context.questions.map(q=>q.field),['calcPrice']);
    return {fields:{calcPrice:'799.00'}};
  });
  assert.deepEqual(answered.changed,{calcPrice:'799.00',calcProduct:'Filtro Premium'});
  assert.deepEqual(demoCloseQuestions(answered.close),[]);
  // A doubtful hour asks for the follow-up only while the calendar has nothing saved.
  const hour=fixture([demo(2,'A',{knownOutcome:'follow_up',autoCloseReceipt:{review:[{ field:'followUp', label:'mañana a las 4', state:'preguntar' }]}})]);
  assert.deepEqual(demoCloseQuestions(await hour.service.read('A',id(2))).map(q=>q.question),
    ['No me quedó clara la fecha u hora del seguimiento («mañana a las 4»). ¿Para cuándo quedó?']);
});

test('el piloto exige sesión individual de Agustín o Rigoberto, también ante listas de acceso amplias', () => {
  const who = email=>({user:{email},session:{tokenHash:'test-only',accessMode:'direct'}});
  const a=who('agustincalderon286@gmail.com'), b=who('calderonrigoberto51@gmail.com');
  assert.equal(inDemoClosePilot(a,false),true);
  assert.equal(inDemoClosePilot(b,false),true);
  assert.equal(inDemoClosePilot(a,true),false);
  assert.equal(inDemoClosePilot({...b,session:{tokenHash:'test',accessMode:'delegated'}},false),false);
  assert.equal(inDemoClosePilot(who('otra@example.invalid'),false,['*']),false);
});
