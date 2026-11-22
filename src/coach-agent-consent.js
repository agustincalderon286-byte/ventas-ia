// Permiso del vendedor para que el asistente consulte sus cuentas y envíe lo leído al modelo.
// Sube CONSENT_VERSION cuando cambie qué datos se comparten: los permisos anteriores dejan de valer.
import mongoose from 'mongoose';

export const CONSENT_VERSION = 1;
const schema = new mongoose.Schema({_id:String,version:Number,acceptedAt:Date},{versionKey:false,writeConcern:{w:'majority'}});
const Store = mongoose.models.CoachAgentConsent || mongoose.model('CoachAgentConsent',schema);

export function createAgentConsent({store=Store,now=Date.now}={}) {
  return {
    // Devuelve la fecha de aceptación vigente, o null.
    async accepted(owner) {
      const doc=await store.findById(String(owner),'version acceptedAt').lean();
      return doc?.version===CONSENT_VERSION && doc.acceptedAt ? new Date(doc.acceptedAt) : null;
    },
    async accept(owner) {
      await store.updateOne({_id:String(owner)},{$set:{version:CONSENT_VERSION,acceptedAt:new Date(now())}},{upsert:true});
    },
    async revoke(owner) { await store.deleteOne({_id:String(owner)}); }
  };
}
