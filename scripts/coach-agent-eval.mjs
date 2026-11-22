// Opt-in benchmark. Calls the same pilot interpreter as the chat, never its executors.
import {pathToFileURL} from 'node:url';
import {interpretPilotPlan} from '../src/coach-plan-language.js';

const models=['gpt-6-luna','gpt-6.2-sol'];
const rates={'gpt-6-luna':{input:0.10,cached:0.01,output:0.50},'gpt-6.2-sol':{input:2,cached:0.1,output:10}};
const connected={calendar:'connected',availability:'connected',gmail:'connected',sheets:'connected'};
const cases=[
  {id:'agenda-manana',source:'captura de Agustín',text:'Revisa si tengo citas para mañana en mi calendario de Google.',kind:'agenda'},
  {id:'agenda-implicita',text:'¿Qué tengo mañana?',kind:'agenda'},
  {id:'primera-demo',text:'¿A qué hora es mi primera demo mañana?',kind:'agenda'},
  {id:'solo-disponibilidad',text:'No me agendes nada. Solo dime si ando libre mañana de dos a tres.',kind:'agenda'},
  {id:'continuar-agenda',text:'¿Y pasado mañana?',context:{lastRead:'agenda'},kind:'agenda'},
  {id:'lectura-sin-conexion',text:'Checa si el viernes ya tengo algo en mi calendario.',context:{connections:{...connected,calendar:'missing',availability:'missing'}},kind:'agenda'},
  {id:'correo-reciente',text:'¿Qué correos nuevos me han entrado?',kind:'inbox'},
  {id:'correo-de-hycite',text:'Enséñame los correos que llegaron de Hy Cite, quiero leer sus avisos.',kind:'inbox'},
  {id:'ventas-hycite',text:'¿Cuántas ventas de Hy Cite quedaron confirmadas esta semana?',kind:'sales'},
  {id:'ventas-dictadas',text:'Checa si cayó una venta de Hay Cite.',kind:'sales'},
  {id:'referidos',text:'¿A quién de mis cuatro en catorce le toca seguimiento hoy?',kind:'plan',ops:['list'],fields:{leadKind:'referrals',bucket:'today'}},
  {id:'boletas',text:'Saca mis boletas de rifa pendientes sin fecha.',kind:'plan',ops:['list'],fields:{leadKind:'tickets',bucket:'undated'}},
  {id:'anfitrion',text:'Busca en la hoja a quienes me refirió Rosa.',kind:'plan',ops:['list'],fields:{host:'Rosa'}},
  {id:'dos-opciones',text:'Búscame dos huecos para una demo de 45 minutos mañana por la tarde.',kind:'plan',ops:['appointment'],fields:{slots:true}},
  {id:'tres-herramientas',text:'Checa quién me falta del cuatro en catorce, búscame dos huecos para mañana y déjame listo el correo.',kind:'plan',ops:['list','appointment','email'],fields:{slots:true,sendEmail:false,leadKind:'referrals'}},
  {id:'borrador',text:'Prepárame un correo para confirmar la visita. Déjamelo en borrador.',kind:'plan',ops:['email'],fields:{sendEmail:false}},
  {id:'cambiar-horario',text:'Mejor el segundo.',context:{operations:['list','appointment','email'],waiting:'slot',hasSelection:true},kind:'revise',fields:{ordinal:2}},
  {id:'seleccionar-sin-enviar',text:'Solo guarda la cita, el correo déjalo para después.',context:{operations:['appointment','email'],hasReview:true},kind:'select',selected:['appointment'],fields:{sendEmail:false}},
  {id:'ensenar',text:'Explícame cómo trabajar una boleta de rifa con un ejemplo inventado.',kind:'none'},
  {id:'sin-permiso-de-lectura',text:'No leas mi correo. ¿Qué otra cosa me puedes ayudar a hacer?',kinds:['clarify','capabilities']}
];

function configFrom(flags) {
  const config={models,run:false,list:false,limit:cases.length,example:null};
  for (let i=0;i<flags.length;i++) {
    switch (flags[i]) {
      case '--run': config.run=true;break;
      case '--list': config.list=true;break;
      case '--limit': {
        const value=flags[++i];
        if (!/^(?:[1-9]|1[0-9]|20)$/.test(value || '')) throw Error('Usa --limit del 1 al 20.');
        config.limit=Number(value);break;
      }
      case '--model': {
        const id=flags[++i];
        if (!models.includes(id)) throw Error('Modelos admitidos: '+models.join(', '));
        config.models=[id];break;
      }
      case '--case': {
        config.example=flags[++i];
        if (!cases.some(c=>c.id===config.example)) throw Error('Caso desconocido. Usa --list para ver los nombres.');
        break;
      }
      default: throw Error('Opción desconocida: '+flags[i]);
    }
  }
  return config;
}

const sameSet=(left,right)=>left?.length===right.length && right.every(x=>left.includes(x));
function correctlyRouted(scenario,intent) {
  return (scenario.kinds || [scenario.kind]).includes(intent.kind) &&
    (!scenario.ops || sameSet(intent.operations,scenario.ops)) &&
    (!scenario.selected || sameSet(intent.select,scenario.selected)) &&
    Object.entries(scenario.fields || {}).every(([key,value])=>intent[key]===value);
}

function modelCost(model,usage) {
  const price=rates[model];
  const written=usage.cacheWriteTokens, long=usage.longCacheWriteTokens;
  const ordinary=usage.inputTokens-usage.cachedInputTokens-written;
  return (ordinary*price.input + usage.cachedInputTokens*price.cached +
      (written-long)*price.input*1.25 + long*price.input*2.5 + usage.outputTokens*price.output)/1_000_000;
}
const rounded = value => Number(value.toFixed(6));
const percentile=(items,p)=>items.length ? items.toSorted((a,b)=>a-b)[Math.ceil(items.length*p)-1] : null;

export async function evaluatePilot(argv,{interpret=interpretPilotPlan,key=process.env.OPENAI_API_KEY,emit=console.log}={}) {
  const config=configFrom(argv);
  const pool=cases.filter(c=>!config.example || c.id===config.example).slice(0,config.limit);
  const jobs=pool.flatMap(example=>config.models.map(model=>({example,model})));
  if (!config.run || config.list) {
    emit(JSON.stringify({mode:'preview',calls:jobs.length,models:config.models,cases:pool.map(c=>({id:c.id,
      source:c.source || 'frase representativa, sintética',text:c.text}))}));
    return 0;
  }
  if (!key) throw Error('Esta sesión necesita OPENAI_API_KEY para las pruebas reales.');
  const results=[];
  let index=0;
  async function worker() {
    while(index<jobs.length) {
      const {example,model}=jobs[index++];
      let usage;
      const start=performance.now();
      const intent=await interpret(example.text,{apiKey:key,model,context:{connections:connected,...example.context},
        onUsage:metrics=>{usage=metrics;}});
      const passed=correctlyRouted(example,intent);
      const elapsed=Math.round(performance.now()-start);
      const dollars=usage ? modelCost(model,usage) : null;
      const row={case:example.id,model,passed,kind:intent.kind,operations:intent.operations || [],expected:example.kinds || [example.kind],
        ...(example.ops && {expectedOps:example.ops}),...(example.selected && {expectedSelect:example.selected,select:intent.select || []}),
        ...(example.fields && {expectedFields:example.fields,fields:Object.fromEntries(Object.keys(example.fields).map(k=>[k,intent[k]]))}),
        ms:elapsed,usage:usage || null,estimatedUsd:dollars===null ? null : rounded(dollars)};
      results.push(row);
      emit(JSON.stringify(row));
    }
  }
  await Promise.all([worker(),worker()]);
  for (const model of config.models) {
    const rows=results.filter(r=>r.model===model);
    const priced=rows.filter(r=>!!r.usage);
    emit(JSON.stringify({summary:true,model,passed:rows.filter(r=>r.passed).length,total:rows.length,withUsage:priced.length,
      p50Ms:percentile(rows.map(r=>r.ms),.5),p95Ms:percentile(rows.map(r=>r.ms),.95),
      estimatedUsd:priced.length ? rounded(priced.reduce((s,r)=>s+modelCost(model,r.usage),0)) : null}));
  }
  return results.every(r=>r.passed) ? 0 : 1;
}

if (process.argv[1] && import.meta.url===pathToFileURL(process.argv[1]).href) {
  try {process.exitCode=await evaluatePilot(process.argv.slice(2));}
  catch (e) {console.error(e.message);process.exitCode=2;}
}
