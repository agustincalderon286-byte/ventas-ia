// Ciclo del asistente del vendedor: el modelo pide herramientas, lee sus resultados
// y decide el siguiente paso. Este módulo no conoce Gmail, Calendar ni Sheets: recibe
// un catálogo y un ejecutor. Nunca confirma cambios; las herramientas de este ciclo leen.
const modelIds = new Set(['gpt-6.2-sol','gpt-6-luna']);
const MAX_ARGUMENTS = 4000;
const MAX_RESULT = 6000;
const MAX_CALLS_PER_STEP = 4;

export class AgentUnavailable extends Error {}
const unavailable = message => new AgentUnavailable(message);

function matches(value, rule) {
  if (rule.type==='array') return Array.isArray(value) && value.length<=(rule.maxItems ?? 20) && value.every(item=>matches(item,rule.items));
  if (rule.type==='integer') return Number.isInteger(value) && value>=(rule.minimum ?? -1e9) && value<=(rule.maximum ?? 1e9);
  if (rule.type==='boolean') return typeof value==='boolean';
  return typeof value==='string' && value.length<=(rule.maxLength ?? 400) && (!rule.enum || rule.enum.includes(value));
}

// Solo acepta los argumentos declarados, todos presentes y del tipo esperado.
export function readArguments(tool, raw) {
  if (typeof raw!=='string' || raw.length>MAX_ARGUMENTS) return null;
  let args;
  try { args=JSON.parse(raw); } catch { return null; }
  const props=tool.parameters.properties;
  if (!args || typeof args!=='object' || Array.isArray(args) || Object.keys(args).length!==Object.keys(props).length) return null;
  return Object.entries(props).every(([key,rule])=>Object.hasOwn(args,key) && matches(args[key],rule)) ? args : null;
}

const count = value => Number.isSafeInteger(value) && value>=0 ? value : 0;
function addUsage(total, usage) {
  total.inputTokens+=count(usage?.input_tokens);
  total.cachedInputTokens+=count(usage?.input_tokens_details?.cached_tokens);
  total.cacheWriteTokens+=count(usage?.input_tokens_details?.cache_write_tokens);
  total.longCacheWriteTokens+=count(usage?.input_tokens_details?.cache_write_tokens_details?.ttl_12h_tokens);
  total.outputTokens+=count(usage?.output_tokens);
}

function serialize(result) {
  const text=JSON.stringify(result);
  return text.length<=MAX_RESULT ? text : JSON.stringify({estado:'recortado',nota:'El resultado era demasiado largo. Pide un filtro más preciso.',inicio:text.slice(0,MAX_RESULT-200)});
}

// tools: [{name, description, parameters}]. execute(name,args) devuelve un objeto con `estado`.
// Devuelve {text, trace, steps, usage}. Lanza AgentUnavailable si el modelo no completa el turno.
export async function runAgentLoop({model='gpt-6.2-sol', instructions, input, tools, execute, maxSteps=6, deadlineMs=40000,
  fetchImpl=globalThis.fetch, apiKey=process.env.OPENAI_API_KEY, now=Date.now, onUsage=()=>{}}) {
  if (!modelIds.has(model) || !apiKey) throw unavailable('Modelo no disponible');
  const started=now();
  const items=[...input];
  const trace=[];
  const usage={inputTokens:0,cachedInputTokens:0,cacheWriteTokens:0,longCacheWriteTokens:0,outputTokens:0,requests:0};
  const definitions=tools.map(({name,description,parameters})=>({type:'function',name,description,strict:true,parameters}));
  for (let step=1; step<=maxSteps+1; step++) {
    const remaining=deadlineMs-(now()-started);
    if (remaining<2000) throw unavailable('Se agotó el tiempo del turno');
    const last=step>maxSteps;
    let response;
    try {
      response=await fetchImpl('https://api.openai.com/v1/responses',{
        method:'POST',redirect:'error',signal:AbortSignal.timeout(Math.min(remaining,20000)),
        headers:{Authorization:'Bearer '+apiKey,'Content-Type':'application/json'},
        body:JSON.stringify({model,store:false,reasoning:{effort:'low'},include:['reasoning.encrypted_content'],max_output_tokens:2000,
          instructions,input:items,tools:definitions,tool_choice:last?'none':'auto',parallel_tool_calls:true})
      });
    } catch { throw unavailable('No se pudo consultar el modelo'); }
    if (!response.ok) { await response.body?.cancel?.(); throw unavailable('No se pudo consultar el modelo'); }
    const body=await response.json();
    usage.requests++;
    addUsage(usage,body.usage);
    // Entrega el consumo acumulado incluso si un paso posterior no termina.
    onUsage({...usage});
    if (body.status!=='completed' || !Array.isArray(body.output)) throw unavailable('El modelo no completó la respuesta');
    const calls=body.output.filter(item=>item.type==='function_call');
    if (!calls.length) {
      const text=body.output.filter(item=>item.type==='message').flatMap(item=>item.content || [])
        .filter(part=>part.type==='output_text' && typeof part.text==='string').map(part=>part.text).join('\n').trim();
      if (!text) throw unavailable('El modelo no redactó una respuesta');
      return {text,trace,steps:step,usage};
    }
    if (last || calls.length>MAX_CALLS_PER_STEP) throw unavailable('El modelo pidió demasiadas herramientas');
    items.push(...body.output);
    const results=await Promise.all(calls.map(async call=>{
      const tool=tools.find(item=>item.name===call.name);
      const args=tool && readArguments(tool,call.arguments);
      if (!args || typeof call.call_id!=='string') return {call,result:{estado:'argumentos_invalidos'}};
      let result;
      try { result=await execute(tool.name,args); }
      catch { result={estado:'fallo',nota:'La herramienta no respondió. No afirmes su contenido.'}; }
      return {call,tool,args,result};
    }));
    for (const {call,tool,args,result} of results) {
      trace.push({name:tool?.name || 'desconocida',args:args || null,estado:result?.estado || 'ok'});
      if (result?.detener) return {handoff:result.detener,trace,steps:step,usage};
      items.push({type:'function_call_output',call_id:String(call.call_id),output:serialize(result)});
    }
  }
  throw unavailable('El modelo no terminó el turno');
}
