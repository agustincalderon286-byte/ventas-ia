import test from 'node:test';
import assert from 'node:assert/strict';
import {cases} from '../scripts/coach-agent-cases.mjs';
import {checkCases, judge, patternAgents, routerOutcome, runBench} from '../scripts/coach-agent-bench.mjs';

const never=()=>{throw Error('No se debe llamar al modelo');};
const collect=async(args,options={})=>{
  const lines=[];
  const code=await runBench(args,{interpret:never,key:'',emit:line=>lines.push(JSON.parse(line)),...options});
  return {code,lines};
};

test('el conjunto es amplio, sin repetidos y solo con datos ficticios',()=>{
  assert.deepEqual(checkCases(),[]);
  assert.ok(cases.length>=60);
  assert.ok(new Set(cases.map(c=>c.group)).size>=12);
  assert.ok(cases.filter(c=>c.fixture).length>=3);
});

test('el juez exige las lecturas, las propuestas exactas y respeta lo prohibido',()=>{
  const result=(reads=[],proposes=[])=>({reads,proposes});
  assert.equal(judge({reads:[['referrals','calls']]},result(['calls'])),'');
  assert.match(judge({reads:['agenda']},result(['inbox'])),/faltó consultar: agenda/);
  assert.match(judge({proposes:['appointment']},result([],['appointment','email'])),/propuso de más: email/);
  assert.match(judge({proposes:['appointment','email']},result([],['appointment'])),/faltó proponer: email/);
  assert.match(judge({reads:['inbox'],forbid:['email']},result(['inbox'],['email'])),/hizo lo prohibido: email/);
  assert.match(judge({mode:'advice'},result(['agenda'])),/consejo/);
  assert.equal(judge({mode:'advice'},result()),'');
  assert.match(judge({mode:'clarify'},result([],['email'])),/propuso de más/);
});

test('la capa de patrones se mide sin red y un agente que reclama sin resolver corta la cadena',async()=>{
  assert.deepEqual(patternAgents('Agéndame una demo con Laura Ficticia mañana a las 5 de la tarde.').proposes,['appointment']);
  assert.equal(patternAgents('guarda en mi calendario la visita con la familia Ejemplo el sábado 11 am').via,'calendar:help');
  assert.equal(patternAgents('que le digo a un cliente que dice que esta muy caro').via,'chat general');
  const {code,lines}=await collect([]);
  const summary=lines.at(-1);
  assert.equal(code,0);
  assert.equal(summary.total,cases.length);
  assert.equal(lines.length,cases.length+1);
  assert.equal(summary.unsafe,0);
  assert.ok(summary.passed<summary.total);
  assert.equal(Object.values(summary.groups).reduce((sum,group)=>sum+group.total,0),cases.length);
});

test('el selector solo llama al modelo con --run y clave, y sus decisiones se traducen a capacidades',async()=>{
  const preview=await collect(['--system','router','--group','agenda']);
  assert.equal(preview.lines[0].mode,'preview');
  assert.equal(preview.lines[0].calls,cases.filter(c=>c.group==='agenda').length*2);
  await assert.rejects(collect(['--system','router','--run']),/OPENAI_API_KEY/);
  for (const args of [['--system','otro'],['--model','otro'],['--group','otro'],['--case','otro'],['--limit','0'],['--x']]) await assert.rejects(collect(args));

  const calls=[];
  const interpret=async(text,{model,onUsage})=>{
    calls.push(model);
    onUsage({inputTokens:1000,cachedInputTokens:0,cacheWriteTokens:0,longCacheWriteTokens:0,outputTokens:100});
    return {kind:'agenda'};
  };
  const run=await collect(['--system','router','--run','--case','agenda-corta','--model','gpt-6-luna'],{interpret,key:'ficticia'});
  assert.deepEqual(calls,['gpt-6-luna']);
  assert.equal(run.lines[0].passed,true);
  assert.equal(run.lines[0].usd,0.00015);
  assert.equal(run.lines[1].system,'router · gpt-6-luna');

  const plan=(operations,extra={})=>({kind:'plan',operations,slots:false,ordinal:0,...extra});
  assert.deepEqual(routerOutcome(plan(['appointment'],{slots:true}),{}).reads,['slots']);
  assert.deepEqual(routerOutcome(plan(['appointment','email']),{}).proposes,['appointment','email']);
  assert.deepEqual(routerOutcome(plan(['note']),{}),{via:'selector',reads:['referrals'],proposes:['note']});
  assert.deepEqual(routerOutcome({kind:'revise',operations:[],ordinal:2,slots:true},{context:{operations:['appointment'],waiting:'slot'}}).proposes,['appointment']);
  assert.deepEqual(routerOutcome({kind:'select',select:['appointment']},{}).proposes,['appointment']);
  assert.equal(routerOutcome({kind:'unsupported'},{}).via,'selector:sin herramienta');
  assert.match(routerOutcome({kind:'none'},{text:'¿Qué correos nuevos me han entrado?'}).via,/^derivado → mail$/);
});

test('el ciclo se mide con lecturas ficticias y lo que deriva continúa en el selector',async()=>{
  const usage={inputTokens:1000,cachedInputTokens:0,outputTokens:100};
  const seen=[];
  const loop=async options=>{
    const text=options.input.at(-1).content;
    seen.push({text,history:options.input.length-1});
    if (/Agéndame/.test(text)) return {handoff:'gestion',trace:[{name:'pasar_al_coach',estado:'derivado'}],usage};
    const inbox=await options.execute('leer_bandeja',{});
    return {text:'El correo dice: '+inbox.mensajes[0].extracto,trace:[{name:'leer_bandeja',estado:inbox.estado}],usage};
  };
  const interpret=async()=>({kind:'plan',operations:['appointment'],slots:false,ordinal:0});
  const read=await collect(['--system','loop','--run','--case','inyeccion-correo','--model','gpt-6-luna'],{loop,interpret,key:'ficticia'});
  assert.equal(read.lines[0].passed,true);
  assert.deepEqual(read.lines[0].reads,['inbox']);
  assert.match(read.lines[0].answer,/ATENCIÓN ASISTENTE/);
  assert.equal(read.lines[0].usd,0.00015);
  const handed=await collect(['--system','loop','--run','--case','cita-directa','--model','gpt-6-luna'],{loop,interpret,key:'ficticia'});
  assert.equal(handed.lines[0].passed,true);
  assert.equal(handed.lines[0].via,'ciclo → selector');
  assert.deepEqual(handed.lines[0].proposes,['appointment']);
  assert.equal(handed.lines[1].system,'loop · gpt-6-luna');
  const followup=await collect(['--system','loop','--run','--case','agenda-seguimiento','--model','gpt-6-luna'],{loop,interpret,key:'ficticia'});
  assert.equal(seen.at(-1).history,2);
  assert.equal(followup.lines[0].passed,false);
  const down=await collect(['--system','loop','--run','--case','agenda-corta','--model','gpt-6-luna'],{loop:async()=>{throw Error('x');},interpret,key:'ficticia'});
  assert.equal(down.lines[0].via,'ciclo:no disponible');
  assert.equal((await collect(['--system','loop','--group','agenda'])).lines[0].system,'loop');
});
