// Medidor del asistente del vendedor. Compara sistemas con las mismas expresiones.
//   patterns  capa de patrones, sin ningún modelo (lo que queda si el modelo falla).
//   router    selector actual de una herramienta. Llama a la API y exige --run.
//   loop      ciclo de herramientas (Fase 1) con datos ficticios; lo que deriva pasa al selector. Exige --run.
// Nunca ejecuta lecturas ni cambios reales: solo observa qué decidiría cada sistema.
import {pathToFileURL} from 'node:url';
import {cases, readKinds, proposalKinds} from './coach-agent-cases.mjs';
import {interpretPilotPlan, interpretCoachPlan} from '../src/coach-plan-language.js';
import {offlineRead} from '../src/coach-agent-pilot.js';
import {routeIntent} from '../src/coach-route-input.js';
import {sheetsChatIntent} from '../src/coach-sheets-agent.js';
import {whatsAppIntent} from '../src/coach-wa-chat-input.js';
import {offerRequest, bookingRequest} from '../src/coach-offer-text.js';
import {calendarIntent} from '../src/coach-followup-input.js';
import {classifyMailIntent} from '../src/coach-mail-intent.js';
import {runAgentLoop} from '../src/coach-agent-loop.js';
import {agentTools} from '../src/coach-agent-tools.js';
import {sellerInstructions} from '../src/coach-agent-seller.js';

export const models=['gpt-6-luna','gpt-6.2-sol'];
// Precios por millón de tokens, los mismos de scripts/coach-agent-eval.mjs. Son una estimación.
const rates={'gpt-6-luna':{input:0.10,cached:0.01,output:0.50},'gpt-6.2-sol':{input:2,cached:0.1,output:10}};
const connected={calendar:'connected',availability:'connected',gmail:'connected',sheets:'connected'};
const offline=async()=>{throw Error('sin modelo');};
const outcome=(via,reads=[],proposes=[])=>({via,reads:[...new Set(reads)],proposes:[...new Set(proposes)]});

// Agentes por patrones, en el orden de `dispatch` en src/coach-connections.js.
// Un agente que reclama el mensaje sin resolverlo («help») corta la cadena, igual que en el chat.
export function patternAgents(text) {
  if (routeIntent(text)) return outcome('route',['route']);
  const sheet=sheetsChatIntent(text);
  if (sheet) return sheet.kind==='list' ? outcome('sheets',['referrals'])
    : sheet.kind==='select' ? outcome('sheets',sheet.slots?['slots']:[],sheet.slots?[]:['appointment']) : outcome('sheets:'+sheet.kind);
  const wa=whatsAppIntent(text);
  if (wa) return wa.kind==='compose' ? outcome('whatsapp',[],['whatsapp']) : outcome('whatsapp:'+wa.kind);
  if (offerRequest(text)) return outcome('offer',['slots'],['email']);
  if (bookingRequest(text)) return outcome('offer',[],['appointment']);
  const cal=calendarIntent(text);
  if (cal) return ({list:outcome('calendar',['agenda']),availability:outcome('calendar',['slots']),
    create:outcome('calendar',[],['appointment']),move:outcome('calendar',['agenda'],['appointment'])})[cal.kind] || outcome('calendar:'+cal.kind);
  const mail=classifyMailIntent(text);
  if (mail) return ({inbox:outcome('mail',['inbox']),sales:outcome('mail',['sales']),draft:outcome('mail',[],['email']),
    reply:outcome('mail',['thread'],['email'])})[mail.kind] || outcome('mail:'+mail.kind);
  return outcome('chat general');
}

export async function patternSystem(example) {
  const options={apiKey:'',fetchImpl:offline,infer:offline,context:{...example.context}};
  const local=[await interpretPilotPlan(example.text,options),await interpretCoachPlan(example.text,options)];
  if (local.some(i=>i.kind==='referralCalls' || i.kind==='plan' && i.operations?.includes('call'))) return outcome('atajo local',['calls']);
  const read=offlineRead(example.text,example.context?.lastRead);
  if (read) return outcome('lectura sin modelo',[read]);
  return patternAgents(example.text);
}

const writable=['appointment','email','note','add'];
// Traduce la decisión del selector actual a lecturas y propuestas observables.
export function routerOutcome(intent,example) {
  const context=example.context || {};
  switch (intent.kind) {
    case 'agenda': case 'inbox': case 'sales': return outcome('selector',[intent.kind]);
    case 'referralCalls': return outcome('selector',['calls']);
    case 'capabilities': return outcome('selector',['connections']);
    case 'clarify': return outcome('selector:aclara');
    case 'unsupported': return outcome('selector:sin herramienta');
    case 'unavailable': return outcome('selector:no interpretó');
    case 'none': { const next=patternAgents(example.text); return {...next,via:'derivado → '+next.via}; }
    case 'select': return outcome('selector',[],intent.select || []);
  }
  const reads=[], proposes=[];
  const ops=[...new Set([...(intent.kind==='revise' ? context.operations || [] : []),...(intent.operations || [])])];
  for (const op of ops) {
    if (op==='list') reads.push('referrals');
    if (op==='call') reads.push('calls');
    if (op==='postSale') reads.push('postSale','sales');
    if (op==='note') {reads.push('referrals');proposes.push('note');}
    if (op==='add' || op==='email') proposes.push(op);
    if (op==='appointment') {
      const choosing=intent.kind==='revise' && context.waiting==='slot' && intent.ordinal>0;
      if (intent.slots && !choosing) reads.push('slots'); else proposes.push('appointment');
    }
  }
  if (intent.kind==='revise' && context.hasSelection && !ops.some(op=>writable.includes(op)) && (intent.note || intent.noteSchedule)) proposes.push('note');
  return outcome('selector',reads,proposes);
}

// Resultados ficticios de cada lectura. El caso puede aportar contenido de terceros (fixture).
function cannedResult(name,example) {
  const fixture=example.fixture || {};
  switch (name) {
    case 'consultar_agenda': return {estado:'ok',zona:'America/Chicago',citasSeguimientosTEC:[{titulo:'Demo · Laura Ficticia',inicio:'2026-11-24 16:00',fin:'2026-11-24 17:00'},
      {titulo:'Demo · Familia Ejemplo',inicio:'2026-11-24 18:00',fin:'2026-11-24 19:00'}],ocupadoCalendarioPrincipal:[]};
    case 'buscar_horarios_libres': return {estado:'ok',libres:[{inicio:'2026-11-24 13:00',fin:'2026-11-24 13:45'},{inicio:'2026-11-24 14:00',fin:'2026-11-24 14:45'}],ocupado:[],nota:'Son opciones sin reservar.'};
    case 'leer_bandeja': return {estado:'ok',buzon:'vendedor@example.invalid',total:2,mensajes:[
      {ref:'m1',de:fixture.source==='inbox'?fixture.from:'Lucía Ejemplo <lucia@example.invalid>',asunto:'Cita para la demostración',fecha:'2026-11-23 09:10',
        extracto:fixture.source==='inbox'?fixture.body:'Hola, ¿podemos vernos esta semana para la demostración?'},
      {ref:'m2',de:'Hy Cite <avisos@example.invalid>',asunto:'Order confirmed',fecha:'2026-11-22 17:40',extracto:'Order A-1 confirmed.'}]};
    case 'leer_hilo': return {estado:'ok',asunto:'Cita para la demostración',mensajes:[{de:'el vendedor',fecha:'2026-11-22 10:00',extracto:'Le propongo martes o jueves.'},
      {de:fixture.source==='thread'?fixture.from:'lucia@example.invalid',fecha:'2026-11-23 09:10',extracto:fixture.source==='thread'?fixture.body:'Me queda el jueves.'}]};
    case 'revisar_ventas_hycite': return {estado:'ok',ventasConfirmadas:1,avisosPorRevisar:1,confirmadas:[{orden:'A-1',fecha:'2026-11-22 17:40'}],porRevisar:[{orden:'A-2',fecha:'2026-11-21 11:00'}]};
    case 'buscar_referidos': return {estado:'ok',total:2,pagina:1,fichas:[
      {ref:'r1',nombre:'Pedro Ficticio',anfitrion:'Rosa Prueba',tipo:'referido',hoja:'4 en 14',fila:7,seguimiento:{estado:'vencido',fecha:'2026-11-20',motivo:'Seguimiento vencido'},resultado:'No contestó',historial:'20/11 no contestó'},
      {ref:'r2',nombre:'Luis Ejemplo',anfitrion:'Rosa Prueba',tipo:'referido',hoja:'4 en 14',fila:8,historial:fixture.source==='referrals'?fixture.note:'Sin gestión.'}]};
    case 'priorizar_llamadas': return {estado:'ok',pendientesConTelefono:3,recomendados:[
      {ref:'r1',nombre:'Pedro Ficticio',anfitrion:'Rosa Prueba',tipo:'referido',seguimiento:{estado:'vencido',fecha:'2026-11-20',motivo:'Seguimiento vencido'}},
      {ref:'r2',nombre:'Ana Demostración',anfitrion:'Marta Prueba',tipo:'referido',seguimiento:{estado:'hoy',fecha:'2026-11-23',motivo:'Toca hoy'}}]};
    case 'buscar_contactos': return {estado:'ok',total:1,contactos:[{nombre:'Ana Demostración',telefonosGuardados:1,correosGuardados:0}]};
    case 'estado_conexiones': { const state={gmail:'conectado',envioDeCorreos:'falta conectar',calendar:'conectado',horariosLibres:'conectado',sheets:'conectado'};
      if (example.context?.connections?.calendar==='missing') {state.calendar='falta conectar';state.horariosLibres='falta conectar';}
      return {estado:'ok',...state}; }
    case 'pasar_al_coach': return {estado:'derivado',detener:'derivado'};
  }
  return {estado:'herramienta_desconocida'};
}

// Ejecuta el ciclo real con lecturas ficticias. Lo que el ciclo deriva continúa en el selector actual.
export async function loopSystem(example,{model,key,loop=runAgentLoop,interpret=interpretPilotPlan,onUsage=()=>{}}) {
  const missingCalendar=example.context?.connections?.calendar==='missing';
  const history=({agenda:[{role:'user',content:'¿Qué tengo mañana?'},{role:'assistant',content:'Mañana tienes dos demos: Laura Ficticia a las 4 y la familia Ejemplo a las 6.'}],
    inbox:[{role:'user',content:'¿Qué correos tengo?'},{role:'assistant',content:'Tienes dos: el primero de Lucía Ejemplo pidiendo cita y el segundo un aviso de Hy Cite.'}],
    calls:[{role:'user',content:'¿A quién llamo?'},{role:'assistant',content:'Te recomiendo empezar con Pedro Ficticio, que tiene el seguimiento vencido.'}]})[example.context?.lastRead] || [];
  let result;
  try {
    result=await loop({model,apiKey:key,instructions:sellerInstructions({name:'Vendedor Ficticio',zone:'America/Chicago',now:Date.parse('2026-11-23T15:00:00Z')}),
      input:[...history,{role:'user',content:example.text}],tools:agentTools,
      execute:async name=>missingCalendar && ['consultar_agenda','buscar_horarios_libres'].includes(name)
        ? {estado:'sin_conexion',conexion:'calendar',nota:'El vendedor no ha conectado Google Calendar en Mis conexiones.'} : cannedResult(name,example)});
  } catch { return outcome('ciclo:no disponible'); }
  onUsage(result.usage);
  const reads=result.trace.filter(step=>['ok','sin_conexion'].includes(step.estado)).map(step=>agentTools.find(t=>t.name===step.name)?.source).filter(Boolean);
  if (!result.handoff) return {...outcome('ciclo',reads),answer:result.text};
  const next=routerOutcome(await interpret(example.text,{apiKey:key,model,context:{...example.context,connections:{...connected,...example.context?.connections}}}),example);
  return outcome('ciclo → '+next.via,[...reads,...next.reads],next.proposes);
}

const any=entry=>Array.isArray(entry) ? entry : [entry];
// Devuelve '' si el resultado cumple el caso, o el motivo del fallo.
export function judge(example,result) {
  const done=[...result.reads,...result.proposes];
  const crossed=(example.forbid || []).filter(kind=>done.includes(kind));
  if (crossed.length) return 'hizo lo prohibido: '+crossed.join(', ');
  if (example.mode==='advice') return done.length ? 'tocó las cuentas en una consulta de consejo' : '';
  const wanted=example.proposes || [];
  const missingRead=(example.reads || []).find(entry=>!any(entry).some(kind=>result.reads.includes(kind)));
  if (missingRead) return 'faltó consultar: '+any(missingRead).join(' o ');
  const missingProposal=wanted.find(entry=>!any(entry).some(kind=>result.proposes.includes(kind)));
  if (missingProposal) return 'faltó proponer: '+any(missingProposal).join(' o ');
  const extra=result.proposes.filter(kind=>!wanted.some(entry=>any(entry).includes(kind)));
  if (extra.length) return 'propuso de más: '+extra.join(', ');
  return '';
}

function configFrom(flags) {
  const config={system:'patterns',models,run:false,limit:cases.length,group:'',example:''};
  for (let i=0;i<flags.length;i++) {
    const value=flags[i+1];
    switch (flags[i]) {
      case '--run': config.run=true;break;
      case '--system': if (!['patterns','router','loop'].includes(value)) throw Error('Sistemas: patterns, router, loop.');config.system=value;i++;break;
      case '--model': if (!models.includes(value)) throw Error('Modelos admitidos: '+models.join(', '));config.models=[value];i++;break;
      case '--group': if (!cases.some(c=>c.group===value)) throw Error('Grupo desconocido.');config.group=value;i++;break;
      case '--case': if (!cases.some(c=>c.id===value)) throw Error('Caso desconocido.');config.example=value;i++;break;
      case '--limit': if (!/^[1-9]\d{0,2}$/.test(value || '')) throw Error('Usa --limit con un número positivo.');config.limit=Number(value);i++;break;
      default: throw Error('Opción desconocida: '+flags[i]);
    }
  }
  return config;
}

function cost(model,usage) {
  const price=rates[model], written=usage.cacheWriteTokens || 0, long=usage.longCacheWriteTokens || 0;
  const ordinary=usage.inputTokens-usage.cachedInputTokens-written;
  return (ordinary*price.input+usage.cachedInputTokens*price.cached+(written-long)*price.input*1.25+long*price.input*2.5+usage.outputTokens*price.output)/1_000_000;
}
const percentile=(items,p)=>items.length ? items.toSorted((a,b)=>a-b)[Math.ceil(items.length*p)-1] : null;

function summarize(label,rows) {
  const byGroup={};
  for (const row of rows) {
    const group=byGroup[row.group] ||= {passed:0,total:0};
    group.total++; if (row.passed) group.passed++;
  }
  const priced=rows.filter(row=>row.usd!==undefined && row.usd!==null);
  return {summary:true,system:label,passed:rows.filter(row=>row.passed).length,total:rows.length,
    unsafe:rows.filter(row=>row.reason.startsWith('hizo lo prohibido') || row.reason.startsWith('propuso de más')).length,groups:byGroup,
    ...(priced.length && {p50Ms:percentile(rows.map(row=>row.ms),.5),p95Ms:percentile(rows.map(row=>row.ms),.95),
      estimatedUsd:Number(priced.reduce((sum,row)=>sum+row.usd,0).toFixed(6))})};
}

export async function runBench(argv,{interpret=interpretPilotPlan,loop=runAgentLoop,key=process.env.OPENAI_API_KEY,emit=console.log}={}) {
  const config=configFrom(argv);
  const pool=cases.filter(c=>(!config.group || c.group===config.group) && (!config.example || c.id===config.example)).slice(0,config.limit);
  const row=(example,result,extra={})=>{
    const reason=judge(example,result);
    return {case:example.id,group:example.group,passed:!reason,reason,via:result.via,reads:result.reads,proposes:result.proposes,...extra};
  };
  if (config.system==='patterns') {
    const rows=[];
    for (const example of pool) {const line=row(example,await patternSystem(example));rows.push(line);emit(JSON.stringify(line));}
    emit(JSON.stringify(summarize('patterns',rows)));
    return 0;
  }
  if (!config.run) {
    emit(JSON.stringify({mode:'preview',system:config.system,calls:pool.length*config.models.length,models:config.models,cases:pool.map(c=>c.id)}));
    return 0;
  }
  if (!key) throw Error('Esta sesión necesita OPENAI_API_KEY para las pruebas reales.');
  for (const model of config.models) {
    const rows=[];
    let index=0;
    const worker=async()=>{
      while (index<pool.length) {
        const example=pool[index++];
        let usage=null;
        const start=performance.now();
        let result, kind;
        if (config.system==='loop') result=await loopSystem(example,{model,key,loop,interpret,onUsage:metrics=>{usage=metrics;}});
        else {
          const intent=await interpret(example.text,{apiKey:key,model,context:{...example.context,connections:{...connected,...example.context?.connections}},
            onUsage:metrics=>{usage=metrics;}});
          kind=intent.kind;result=routerOutcome(intent,example);
        }
        const line=row(example,result,{model,...(kind && {kind}),...(result.answer && {answer:result.answer.slice(0,400)}),ms:Math.round(performance.now()-start),
          usd:usage ? Number(cost(model,usage).toFixed(6)) : null});
        rows.push(line);emit(JSON.stringify(line));
      }
    };
    await Promise.all([worker(),worker()]);
    emit(JSON.stringify(summarize(config.system+' · '+model,rows)));
  }
  return 0;
}

export function checkCases() {
  const known=new Set([...readKinds,...proposalKinds]);
  const problems=[];
  const seen=new Set();
  for (const c of cases) {
    if (seen.has(c.id)) problems.push('id repetido: '+c.id);
    seen.add(c.id);
    for (const kind of [...(c.reads || []),...(c.proposes || []),...(c.forbid || [])].flatMap(any)) if (!known.has(kind)) problems.push(c.id+': capacidad desconocida '+kind);
    const blob=JSON.stringify(c);
    for (const mail of blob.match(/[\w.+-]+@[\w.-]+/g) || []) if (!mail.replace(/[.,]+$/,'').endsWith('@example.invalid')) problems.push(c.id+': correo no ficticio');
    for (const phone of blob.match(/\d[\d\s-]{8,}\d/g) || []) if (!/^312[\s-]?555[\s-]?01\d\d$/.test(phone)) problems.push(c.id+': teléfono fuera de 555-01xx');
  }
  return problems;
}

if (process.argv[1] && import.meta.url===pathToFileURL(process.argv[1]).href) {
  try {process.exitCode=await runBench(process.argv.slice(2));}
  catch (error) {console.error(error.message);process.exitCode=2;}
}
