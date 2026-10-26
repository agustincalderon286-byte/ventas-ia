// Synthetic opt-in checks only: no customer data, chats, orders or memberships written.
import assert from 'node:assert/strict';
import { applyCoachResponseStyle, buildCoachKnowledgeContext, COACH_CONSULTATION_PROMPT, COACH_PROMPT_VERSION, getCoachKnowledgeDirectReply } from '../src/knowledge/coach-knowledge.js';
const cases = [
  {q:'Háblame de la licuadora',maxWords:45,clarify:true},
  {q:'Cuanto cuesta la licuadora',maxWords:45,clarify:true},
  {q:'Quiero la batidora de mano',maxWords:45,clarify:true,contains:/Power Blender Go/i,forbidden:/Blender Max/i},
  {q:'Ahora busco la batidora de mano',history:[{role:'user',content:'precio CO9555'},{role:'assistant',content:'La olla de 2 cuartos cuesta $649 base.'}],maxWords:45,clarify:true,contains:/Power Blender Go/i,forbidden:/Blender Max|CO9555/i},
  {q:'Como se llama la que sirve para moler cafe y triturar semillas',maxWords:45,clarify:true,contains:/Power Blender Max/i},
  {q:'El aparato que corta verduras',maxWords:45,clarify:true,contains:/ensaladas/i,forbidden:/tazon|tazón|CO4900/i},
  {q:'Tienen la licuadira?',maxWords:45,clarify:true,contains:/Blender/i},
  {q:'Sí esa, cuánto cuesta?',history:[{role:'user',content:'Quiero la batidora de mano'},{role:'assistant',content:'¿Te refieres a la Power Blender Go?'}],maxWords:70,contains:/999/,forbidden:/\?|garant[ií]a/i},
  {q:'Busco la goma de la olla de presion',maxWords:60,clarify:true,contains:/modelo|medida|tama[nñ]o|litros/i},
  {q:'Que código tiene el aro de silicona Innove de 20 cm',maxWords:70,contains:/SP0430/,forbidden:/\$/},
  {q:'Que es RP9714 y cuanto cuesta?',maxWords:90,contains:/20\s*cm/i,forbidden:/\$\s*\d|quieres que consulte/i},
  {q:'La Max, cuánto cuesta?',history:[{role:'user',content:'Háblame de la licuadora'},{role:'assistant',content:'¿Te refieres a la Power Blender Max o a la Power Blender Go?'}],maxWords:90,contains:/[12][,.]?\d{3}|\$\s*\d/,forbidden:/garant[ií]a|objecion|siguiente paso/i},
  {q:'Cuanto cuesta CO9555',history:[{role:'assistant',content:'Fuente: lista_de_precios.json.'}],maxWords:70,contains:/649/,forbidden:/garant[ií]a|objecion|siguiente paso/i},
  {q:'Que garantia tiene el sarten Easy Release',maxWords:110,contains:/5 (a[nñ]os|years)/i,forbidden:/siguiente paso|objecion/i},
  {q:'Compara la Power Blender Max con la Power Blender Go',maxWords:200,contains:/Max[\s\S]*Go|Go[\s\S]*Max/i},
  {q:'Dame la receta completa de arroz con pollo, ingredientes y pasos',maxWords:450,contains:/ingredientes|arroz/i}
];
if (!process.argv.includes('--live')) {
  console.log(JSON.stringify({prompt:COACH_PROMPT_VERSION,cases:cases.map(c=>c.q),note:'Use --live for synthetic OpenAI checks.'}));
} else {
  await import('dotenv/config');
  if (!process.env.OPENAI_API_KEY) throw new Error('OPENAI_API_KEY unavailable');
  let failures=0;
  for (const c of cases) {
    const payload=applyCoachResponseStyle({model:'gpt-5-mini',reasoning_effort:'low',max_completion_tokens:2400,messages:[
      {role:'system',content:COACH_CONSULTATION_PROMPT},
      {role:'system',content:buildCoachKnowledgeContext(c.q,{history:c.history||[]})},
      // Regression fixture: demo context must not cause an unrequested sales essay.
      {role:'system',content:'Contexto de la demo: estamos en cierre. Ayuda con objeciones, garantías y siguientes pasos.'},
      ...(c.history||[]), {role:'user',content:c.q}
    ]});
    const start=Date.now();
    const direct=getCoachKnowledgeDirectReply(c.q,{history:c.history||[]});
    let value;
    if(direct) value={choices:[{message:{content:direct},finish_reason:'stop'}]};
    else {
      const response=await fetch('https://api.openai.com/v1/chat/completions',{method:'POST',signal:AbortSignal.timeout(60000),headers:{Authorization:'Bearer '+process.env.OPENAI_API_KEY,'Content-Type':'application/json'},body:JSON.stringify(payload)});
      if(!response.ok) throw new Error('OpenAI test HTTP '+response.status);
      value=await response.json();
    }
    const answer=value.choices?.[0]?.message?.content||'';
    const words=answer.trim().split(/\s+/).length;
    const finish=value.choices?.[0]?.finish_reason;
    let error='';
    try {
      assert.ok(answer); assert.equal(finish,'stop'); assert.ok(words<=c.maxWords,`Too long: ${words}`);
      assert.doesNotMatch(answer,/\.json\b|src\/data\/|lista_de_precios|tec-consultation|[a-f0-9]{64}/i);
      if(c.clarify) { assert.equal((answer.match(/\?/g)||[]).length,1); assert.doesNotMatch(answer,/\$|garant[ií]a|objecion|siguiente paso/i); }
      if(c.contains) assert.match(answer,c.contains);
      if(c.forbidden) assert.doesNotMatch(answer,c.forbidden);
    } catch(e) {error=e.message;failures++;}
    console.log(JSON.stringify({question:c.q,answer,words,ms:Date.now()-start,finish,route:direct?'catalog-clarification':'openai',passed:!error,error}));
  }
  console.log(JSON.stringify({prompt:COACH_PROMPT_VERSION,cases:cases.length,failures}));
  if(failures) process.exitCode=1;
}
