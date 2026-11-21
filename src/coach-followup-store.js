import {createHmac, randomBytes} from 'node:crypto';
import mongoose from 'mongoose';
import {sealToken,openToken} from './coach-hycite-gmail.js';

const minute = 60000;
const schema = new mongoose.Schema({
  _id:String, owner:{type:String,required:true}, calendar:{type:String,required:true},
  ref:String, names:[String], startsAt:Date, current:String, version:Number,
  pending:{id:String,session:String,grant:String,phase:String,deadline:Date,lease:String,leaseUntil:Date,content:String},
  last:{id:String,session:String,grant:String,result:String}, removeAfter:{type:Date,expires:0}
}, {versionKey:false,minimize:true,writeConcern:{w:'majority'}});
schema.index({owner:1,calendar:1,names:1,startsAt:1});
schema.index({owner:1,calendar:1,'pending.id':1});
schema.index({owner:1,calendar:1,ref:1});
const Model = mongoose.models.CoachCalendarFollowup || mongoose.model('CoachCalendarFollowup',schema);
const randomId = n => randomBytes(n).toString('hex');
const normalize = name => String(name || '').trim().toLowerCase().normalize('NFD').replace(/\p{M}/gu,'').replace(/\s+/g,' ');
const digest = (key,value) => createHmac('sha256',key).update(String(value)).digest('base64url');

export async function purgeCalendarProposals({owner,grant},key=Buffer.from(process.env.COACH_GMAIL_ENCRYPTION_KEY || '','base64')) {
  const scope = {owner:String(owner),'pending.grant':digest(key,grant)};
  // A Google write may be in flight; keep its operation and receipt recoverable
  // by the worker while the old authorization is invalidated.
  const idle = {...scope,'pending.phase':'preview'};
  await Model.deleteMany({...idle,current:{$exists:false}});
  await Model.updateMany({...idle,current:{$exists:true}},{$unset:{pending:1}});
}

export function createFollowupStore({key,store=Model,now=Date.now}) {
  const scope = b => ({owner:String(b.owner),calendar:digest(key,b.calendarId)});
  const authorization = b => ({session:digest(key,b.session),grant:digest(key,b.grant)});
  const seal = (event,kind,data) => sealToken(JSON.stringify(data),key,event._id+':'+event.owner+':'+event.calendar+':'+kind);
  const open = (event,kind,value) => JSON.parse(openToken(value,key,event._id+':'+event.owner+':'+event.calendar+':'+kind));
  const read = (q) => store.findOne(q).lean();
  const nameKeys = name => {
    const words = normalize(name).split(' ');
    return words.map((_,i)=>digest(key, words.slice(0,i+1).join(' ')));
  };
  const current = row => ({...open(row,'event',row.current),eventId:row._id,reference:row.ref,version:row.version});
  const identity = (b,id,field='pending') => {
    const auth = authorization(b);
    return {...scope(b),[field+'.id']:id,[field+'.grant']:auth.grant,[field+'.session']:auth.session};
  };
  return {
    async search(b,{name,reference,since,before,limit=6}={}) {
      const query = {...scope(b),current:{$exists:true},startsAt:{$gte:new Date(since ?? now()),...(before && {$lt:new Date(before)})}};
      if (reference) { query.ref=reference; delete query.startsAt; }
      else if (name) query.names=digest(key,normalize(name));
      const rows=await store.find(query).sort({startsAt:1}).limit(limit).lean();
      return rows.map(current);
    },
    async prepare(b,proposal,existing) {
      const nowMs=now(), id=randomId(6).toUpperCase();
      const row={...scope(b),_id:existing?.eventId || randomId(16)};
      const deadline=new Date(nowMs+30*minute);
      const pending={...authorization(b),id,phase:'preview',deadline,content:seal(row,id,proposal)};
      if (existing) {
        const query={...row,version:existing.version,$or:[{'pending.phase':'preview'},{'pending.phase':{$exists:false}}]};
        const result=await store.updateOne(query,{$set:{pending}});
        if (!result.matchedCount) return null;
      } else {
        await store.create({...row,ref:row._id.slice(0,12).toUpperCase(),version:0,pending,removeAfter:deadline});
      }
      return {id};
    },
    async discard(b,id) {
      const q={...identity(b,id),'pending.phase':'preview'};
      // The temporary document of a cancelled new event is removed by its TTL.
      const result=await store.updateOne(q,{$unset:{pending:1}});
      return result.matchedCount ? 'discarded' : 'unavailable';
    },
    async confirm(b,id,{saveEvent,syncLead}) {
      const nowMs=now();
      const query=identity(b,id);
      const lease=randomId(12);
      const lock={'pending.lease':lease,'pending.leaseUntil':new Date(nowMs+2*minute)};
      let saved=await store.findOneAndUpdate({...query,'pending.phase':'preview','pending.deadline':{$gt:new Date(nowMs)}},
        {$set:{...lock,'pending.phase':'google'},$unset:{removeAfter:1}},{new:false}).lean();
      if (!saved) {
        saved=await store.findOneAndUpdate({...query,'pending.phase':{$in:['google','crm']},'pending.leaseUntil':{$lte:new Date(nowMs)}},
          {$set:lock},{new:false}).lean();
      }
      if (!saved) {
        const prior=await read(identity(b,id,'last'));
        if (prior) return open(prior,id,prior.last.result);
        const busy=await read(query);
        return {status:busy && ['google','crm'].includes(busy.pending.phase)?'working':'unavailable'};
      }
      const guard={...query,'pending.lease':lease};
      const proposal=open(saved,id,saved.pending.content);
      let receipt=saved.pending.phase==='crm' ? current(saved) : null;
      try {
        if (!receipt) {
          const response=await saveEvent({...proposal,owner:b.owner,grant:b.grant,eventId:saved._id,operationId:id.toLowerCase()});
          if (response?.id !== saved._id) throw new Error('Respuesta incompleta de Calendar');
          receipt={...proposal,operationId:id.toLowerCase(),htmlLink:response.htmlLink || '',etag:response.etag || ''};
          const checkpoint=await store.updateOne(guard,{$set:{
            current:seal(saved,'event',receipt),names:nameKeys(proposal.name),startsAt:new Date(proposal.start),'pending.phase':'crm'
          },$inc:{version:1}});
          if (!checkpoint.matchedCount) return {status:'working'};
        }
        let crm='none';
        if (proposal.leadId) {
          const ok=await syncLead({owner:b.owner,leadId:proposal.leadId,start:proposal.start,action:proposal.action,expectedStart:proposal.expectedStart});
          crm=ok ? 'updated' : 'conflict';
        }
        const result={status:'saved',crm,htmlLink:receipt.htmlLink,summary:proposal.summary,start:proposal.start,timeZone:proposal.timeZone,reference:saved.ref};
        const last={...authorization(b),id,result:seal(saved,id,result)};
        await store.updateOne(guard,{$set:{last},$unset:{pending:1}});
        return result;
      } catch (e) {
        // The same operationId and eventId reconcile a lost Google response on retry.
        const rejected=Boolean(e?.status && [400,401,403,404,409,410,412].includes(e.status));
        if (rejected && !receipt) {
          const changes={$unset:{pending:1}};
          if (!saved.current) changes.$set={removeAfter:new Date(nowMs+30*minute)};
          await store.updateOne(guard,changes);
          return {status:e.reauthorize?'reconnect':'changed'};
        }
        await store.updateOne(guard,{$set:{'pending.leaseUntil':new Date(now())}});
        return {status:receipt?'crmPending':e?.reauthorize?'reconnect':'retry'};
      }
    }
  };
}
