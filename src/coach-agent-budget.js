// Presupuesto mensual del asistente por vendedor, en millonésimas de dólar.
// El cargo se registra al terminar cada turno, así que el exceso máximo es un turno.
import mongoose from 'mongoose';

const schema = new mongoose.Schema({_id:String,usedMicros:Number,turns:Number,updatedAt:Date},{versionKey:false});
const Store = mongoose.models.CoachAgentUsage || mongoose.model('CoachAgentUsage',schema);
// Precios estimados por millón de tokens, los mismos de scripts/coach-agent-eval.mjs.
const rates = {'gpt-6-luna':{input:0.10,cached:0.01,output:0.50},'gpt-6.2-sol':{input:2,cached:0.1,output:10}};

export function turnMicros(model, usage) {
  const price=rates[model];
  if (!price) return 0;
  // Lo que el proveedor guarda en caché se cobra con recargo: 1,25 veces la entrada, o 2,5 si lo conserva 12 horas.
  const written=usage.cacheWriteTokens || 0, long=usage.longCacheWriteTokens || 0;
  const fresh=Math.max(0,usage.inputTokens-usage.cachedInputTokens-written);
  return Math.ceil(fresh*price.input+usage.cachedInputTokens*price.cached+(written-long)*price.input*1.25+long*price.input*2.5+usage.outputTokens*price.output);
}

export function createAgentBudget({limitMicros,store=Store,now=Date.now}) {
  const key=owner=>String(owner)+':'+new Date(now()).toISOString().slice(0,7);
  return {
    async allow(owner) {
      if (!(limitMicros>0)) return false;
      const row=await store.findById(key(owner),'usedMicros').lean();
      return (row?.usedMicros || 0)<limitMicros;
    },
    async charge(owner, model, usage) {
      const micros=turnMicros(model,usage);
      await store.updateOne({_id:key(owner)},{$inc:{usedMicros:micros,turns:1},$set:{updatedAt:new Date(now())}},{upsert:true});
      return micros;
    }
  };
}
