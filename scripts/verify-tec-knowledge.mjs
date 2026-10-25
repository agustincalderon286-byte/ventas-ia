import { performance } from 'node:perf_hooks';
import { buildCoachKnowledgeContext, getCoachKnowledgeManifest, COACH_CONSULTATION_PROMPT } from '../src/knowledge/coach-knowledge.js';
const questions = ['¿Cuántos años de garantía tiene Easy Release?', '¿Qué garantía tiene el aro de silicona de INNOVE?', '¿La olla de presión tiene la misma garantía en todas las piezas?', '¿Cuánto cuesta CO9738?', 'Dame la receta de arroz con pollo con ingredientes y pasos', '¿Puedo devolver un producto que ya usé? ¿Y en Nueva York?', '¿Cómo lleno RoyalOne?'];
const times=[];
for(let i=0;i<500;i++) { const start=performance.now(); buildCoachKnowledgeContext(questions[i%questions.length]); times.push(performance.now()-start); }
times.sort((a,b)=>a-b);
console.log(JSON.stringify({manifest:getCoachKnowledgeManifest(),lookup:{runs:times.length,p50_ms:times[250],p95_ms:times[475],max_ms:times.at(-1)}}));
// Explicit opt-in, synthetic questions only. Does not create chats, leads, orders or memberships.
if (process.argv.includes('--live')) {
  await import('dotenv/config');
  if(!process.env.OPENAI_API_KEY) throw new Error('OPENAI_API_KEY unavailable');
  for(const question of questions) {
    const started=performance.now();
    const response=await fetch('https://api.openai.com/v1/chat/completions',{method:'POST',signal:AbortSignal.timeout(60000),headers:{Authorization:'Bearer '+process.env.OPENAI_API_KEY,'Content-Type':'application/json'},body:JSON.stringify({model:'gpt-5-mini',reasoning_effort:'low',max_completion_tokens:1800,messages:[{role:'system',content:COACH_CONSULTATION_PROMPT},{role:'system',content:buildCoachKnowledgeContext(question)},{role:'user',content:question}]})});
    if(!response.ok) throw new Error('OpenAI test HTTP '+response.status);
    const payload=await response.json();
    console.log(JSON.stringify({question,ms:Math.round(performance.now()-started),finish:payload.choices?.[0]?.finish_reason,answer:payload.choices?.[0]?.message?.content}));
  }
}
