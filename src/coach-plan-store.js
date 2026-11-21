import {randomBytes} from 'node:crypto';
import mongoose from 'mongoose';
import {openToken,sealToken} from './coach-hycite-gmail.js';

const schema=new mongoose.Schema({_id:String,revision:String,content:String,expires:{type:Date,expires:0}}, {versionKey:false,writeConcern:{w:'majority'}});
const Store=mongoose.models.CoachConversationPlan || mongoose.model('CoachConversationPlan',schema);
export const planRevision=()=>randomBytes(12).toString('hex');
export function createPlanStore({key,store=Store,now=Date.now}) {
  const aad=(scope,revision)=>'coach-plan:'+scope+':'+revision;
  return {
    async load(scope) {
      const doc=await store.findById(scope).lean();
      if (!doc) return null;
      // Keep the version for CAS even if Mongo has not collected an expired record yet.
      return {revision:doc.revision,data:Number(doc.expires)>now()?JSON.parse(openToken(doc.content,key,aad(scope,doc.revision))):null};
    },
    async save(scope,previous,data) {
      const revision=planRevision();
      const value={revision,content:sealToken(JSON.stringify(data),key,aad(scope,revision)),expires:new Date(now()+20*60000)};
      try {
        if (!previous) await store.create({_id:scope,...value});
        else if (!(await store.updateOne({_id:scope,revision:previous},{$set:value})).matchedCount) return null;
      } catch(error) { if(error.code===11000) return null; throw error; }
      return revision;
    }
  };
}
