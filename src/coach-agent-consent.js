// Permiso del vendedor para que el asistente consulte sus cuentas y envíe lo leído al modelo.
// Sube CONSENT_VERSION cuando cambie qué datos se comparten: los permisos anteriores dejan de valer.
import mongoose from 'mongoose';

// 2 (22-nov-2026): el modelo recibe también la ciudad o el código postal de las fichas, para ordenar por cercanía.
// 3 (22-nov-2026): TEC conserva 30 días cada pregunta y respuesta del asistente, sin datos de contacto, para que el
//   administrador las revise (coach-chat-review.js). Antes no se conservaba nada más de 20 minutos.
export const CONSENT_VERSION = 3;
const schema = new mongoose.Schema({_id:String,version:Number,acceptedAt:Date},{versionKey:false,writeConcern:{w:'majority'}});
const Store = mongoose.models.CoachAgentConsent || mongoose.model('CoachAgentConsent',schema);

export function createAgentConsent({store=Store,now=Date.now}={}) {
  return {
    // Devuelve la fecha de aceptación vigente, o null.
    async accepted(owner) {
      const doc=await store.findById(String(owner),'version acceptedAt').lean();
      return doc?.version===CONSENT_VERSION && doc.acceptedAt ? new Date(doc.acceptedAt) : null;
    },
    // Para Mis conexiones: `outdated` indica un permiso dado antes de que cambiara lo que se comparte.
    async state(owner) {
      const doc=await store.findById(String(owner),'version acceptedAt').lean();
      const current=doc?.version===CONSENT_VERSION && doc.acceptedAt;
      return {acceptedAt:current ? new Date(doc.acceptedAt) : null,outdated:Boolean(doc && !current)};
    },
    async accept(owner) {
      await store.updateOne({_id:String(owner)},{$set:{version:CONSENT_VERSION,acceptedAt:new Date(now())}},{upsert:true});
    },
    async revoke(owner) { await store.deleteOne({_id:String(owner)}); }
  };
}
