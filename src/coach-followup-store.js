import {createHmac, randomBytes} from 'node:crypto';
import mongoose from 'mongoose';
import {sealToken,openToken} from './coach-hycite-gmail.js';

const minute = 60000;
// previewMs: a preview can start its confirmation. leaseMs: one attempt owns the operation.
// googleMs: after the first confirmation, retries may still write to Google; later they only read.
// crmMs: the lead may still be updated. retainMs: unresolved new events keep a recoverable record.
export const followupLimits = Object.freeze({previewMs:30*minute,leaseMs:2*minute,googleMs:10*minute,crmMs:60*minute,retainMs:24*60*minute});
const schema = new mongoose.Schema({
  _id:String, owner:{type:String,required:true}, calendar:{type:String,required:true},
  ref:String, names:[String], startsAt:Date, current:String, version:Number,
  pending:{id:String,session:String,grant:String,phase:String,deadline:Date,confirmedAt:Date,lease:String,leaseUntil:Date,content:String},
  last:{id:String,session:String,grant:String,result:String}, removeAfter:{type:Date,expires:0}
}, {versionKey:false,minimize:true,writeConcern:{w:'majority'}});
schema.index({owner:1,calendar:1,names:1,startsAt:1});
schema.index({owner:1,calendar:1,'pending.id':1});
schema.index({owner:1,calendar:1,ref:1});
schema.index({'pending.phase':1,'pending.leaseUntil':1});
const Model = mongoose.models.CoachCalendarFollowup || mongoose.model('CoachCalendarFollowup',schema);
const randomId = n => randomBytes(n).toString('hex');
const normalize = name => String(name || '').trim().toLowerCase().normalize('NFD').replace(/\p{M}/gu,'').replace(/\s+/g,' ');
const digest = (key,value) => createHmac('sha256',key).update(String(value)).digest('base64url');
// Records written before confirmedAt existed only carry the preview deadline; the preparation
// time it implies is never later than the real confirmation, so old operations lapse no later.
const confirmedAt = (pending,limits) => pending?.confirmedAt ? new Date(pending.confirmedAt).getTime()
  : pending?.deadline ? new Date(pending.deadline).getTime()-limits.previewMs : 0;
const lapsedSince = (ms,nowMs,limits) => {
  const cut=new Date(nowMs-ms);
  return {$or:[{'pending.confirmedAt':{$lte:cut}},
    {'pending.confirmedAt':{$exists:false},'pending.deadline':{$lte:new Date(nowMs-ms+limits.previewMs)}}]};
};

export async function purgeCalendarProposals({owner,grant},key=Buffer.from(process.env.COACH_GMAIL_ENCRYPTION_KEY || '','base64'),
  {store=Model,now=Date.now,limits=followupLimits}={}) {
  const scope = {owner:String(owner),'pending.grant':digest(key,grant)};
  const idle = {...scope,'pending.phase':'preview'};
  await store.deleteMany({...idle,current:{$exists:false}});
  await store.updateMany({...idle,current:{$exists:true}},{$unset:{pending:1}});
  // The revoked grant can never confirm again. A write in flight keeps its lease and checkpoint;
  // unresolved new events get a bounded TTL and recorded events keep their history.
  const started = {...scope,'pending.phase':{$in:['google','crm']}};
  await store.updateMany({...started,current:{$exists:false},removeAfter:{$exists:false}},
    {$set:{removeAfter:new Date(now()+limits.retainMs)}});
  await store.updateMany({...started,current:{$exists:true},'pending.leaseUntil':{$lte:new Date(now())}},{$unset:{pending:1}});
}

export function createFollowupStore({key,store=Model,now=Date.now,limits=followupLimits}) {
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
  const record = (saved,proposal,receipt) => ({current:seal(saved,'event',receipt),names:nameKeys(proposal.name),startsAt:new Date(proposal.start)});
  return {
    async search(b,{name,reference,since,before,limit=6}={}) {
      const query = {...scope(b),current:{$exists:true},startsAt:{$gte:new Date(since ?? now()),...(before && {$lt:new Date(before)})}};
      if (reference) { query.ref=reference; delete query.startsAt; }
      else if (name) query.names=digest(key,normalize(name));
      const rows=await store.find(query).sort({startsAt:1}).limit(limit).lean();
      return rows.map(current);
    },
    // Index a confirmed Gmail demo for the same search and move commands as any other TEC appointment.
    async rememberEvent(b, receipt) {
      const row={...scope(b),_id:receipt.eventId};
      const ref=receipt.eventId.slice(0,12).toUpperCase();
      try {
        await store.create({...row,ref,version:1,...record(row,receipt,receipt)});
      } catch (error) {
        if (error.code!==11000 || !await read({...row,current:{$exists:true}})) throw error;
      }
      return ref;
    },
    async prepare(b,proposal,existing) {
      const nowMs=now(), id=randomId(6).toUpperCase();
      const searchId=proposal.availabilitySearchId;
      if (proposal.checkAvailability && (!/^[a-f0-9]{32}$/.test(searchId || '') || existing)) return null;
      // All choices from one signed search share one Google event ID.
      const eventId=proposal.checkAvailability
        ? createHmac('sha256',key).update(JSON.stringify([b.owner.toString(),b.calendarId,b.grant,b.session,searchId])).digest('hex').slice(0,32)
        : existing?.eventId || randomId(16);
      const row={...scope(b),_id:eventId};
      const deadline=new Date(nowMs+limits.previewMs);
      const pending={...authorization(b),id,phase:'preview',deadline,content:seal(row,id,proposal)};
      if (existing) {
        const query={...row,version:existing.version,$or:[{'pending.phase':'preview'},{'pending.phase':{$exists:false}}]};
        const result=await store.updateOne(query,{$set:{pending}});
        if (!result.matchedCount) return null;
      } else {
        try {
          await store.create({...row,ref:row._id.slice(0,12).toUpperCase(),version:0,pending,removeAfter:deadline});
        } catch (error) {
          if (error.code!==11000 || !proposal.checkAvailability) throw error;
          // A different choice supersedes the previous preview, up to the instant confirmation starts.
          const editable={...row,current:{$exists:false},'pending.phase':'preview'};
          const changed=await store.updateOne(editable,{$set:{pending,removeAfter:deadline}});
          if (!changed.matchedCount) return null;
        }
      }
      return {id};
    },
    async discard(b,id) {
      const q={...identity(b,id),'pending.phase':'preview'};
      // The temporary document of a cancelled new event is removed by its TTL.
      const result=await store.updateOne(q,{$unset:{pending:1}});
      return result.matchedCount ? 'discarded' : 'unavailable';
    },
    // Maintenance pass for operations nobody retried. It never removes a recorded event.
    async expireInterrupted() {
      const nowMs=now(), idle={'pending.leaseUntil':{$lte:new Date(nowMs)}};
      await store.updateMany({'pending.phase':'preview','pending.deadline':{$lte:new Date(nowMs)},current:{$exists:true}},{$unset:{pending:1}});
      const crm=await store.updateMany({'pending.phase':'crm',...idle,...lapsedSince(limits.crmMs,nowMs,limits)},{$unset:{pending:1}});
      const moves=await store.updateMany({'pending.phase':'google',current:{$exists:true},...idle,
        ...lapsedSince(limits.googleMs+limits.retainMs,nowMs,limits)},{$unset:{pending:1}});
      // Older versions dropped the TTL of new events when their confirmation started.
      const legacy=await store.updateMany({'pending.phase':'google',current:{$exists:false},removeAfter:{$exists:false}},
        {$set:{removeAfter:new Date(nowMs+limits.retainMs)}});
      return {crm:crm.modifiedCount ?? crm.matchedCount,moves:moves.modifiedCount ?? moves.matchedCount,legacy:legacy.modifiedCount ?? legacy.matchedCount};
    },
    async confirm(b,id,{saveEvent,syncLead}) {
      const nowMs=now(), at=new Date(nowMs);
      const query=identity(b,id);
      const lease=randomId(12);
      const lock={'pending.lease':lease,'pending.leaseUntil':new Date(nowMs+limits.leaseMs)};
      const fresh={...query,'pending.phase':'preview','pending.deadline':{$gt:at}};
      const begin={...lock,'pending.phase':'google','pending.confirmedAt':at};
      // A new event keeps a bounded TTL until Google's answer is recorded; a recorded event never expires.
      let saved=await store.findOneAndUpdate({...fresh,current:{$exists:false}},
        {$set:{...begin,removeAfter:new Date(nowMs+limits.googleMs+limits.retainMs)}},{new:false}).lean();
      if (!saved) saved=await store.findOneAndUpdate({...fresh,current:{$exists:true}},{$set:begin},{new:false}).lean();
      if (!saved) {
        saved=await store.findOneAndUpdate({...query,'pending.phase':{$in:['google','crm']},'pending.leaseUntil':{$lte:at}},
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
      const phase=saved.pending.phase, operationId=id.toLowerCase();
      const startedAt=phase==='preview' ? nowMs : confirmedAt(saved.pending,limits);
      const finish=async crm=>{
        const result={status:'saved',crm,htmlLink:receipt.htmlLink,summary:proposal.summary,start:proposal.start,timeZone:proposal.timeZone,reference:saved.ref};
        const last={...authorization(b),id,result:seal(saved,id,result)};
        const closed=await store.updateOne(guard,{$set:{last},$unset:{pending:1}});
        return closed.matchedCount ? result : {status:'working'};
      };
      let receipt=phase==='crm' ? current(saved) : null;
      try {
        if (!receipt) {
          // Past the write window only an existing event can be reconciled; inserting again needs a new preview.
          // Un borrado se puede repetir sin riesgo: solo quita la cita que sigue intacta, o comprueba que ya no está.
          const reconcileOnly=proposal.mode!=='cancel' && now()-startedAt>=limits.googleMs;
          const response=await saveEvent({...proposal,owner:b.owner,grant:b.grant,eventId:saved._id,operationId,
            ...(reconcileOnly ? {reconcileOnly:true} : proposal.checkAvailability && {retryGoogle:phase==='google'})});
          if (reconcileOnly && !response) {
            const result={status:'expired'};
            const changes={$set:{last:{...authorization(b),id,result:seal(saved,id,result)}},$unset:{pending:1}};
            if (!saved.current) changes.$set.removeAfter=new Date(nowMs+limits.previewMs);
            const closed=await store.updateOne(guard,changes);
            return closed.matchedCount ? result : {status:'working'};
          }
          if (response?.id !== saved._id) throw new Error('Respuesta incompleta de Calendar');
          if (proposal.mode==='cancel') {
            // La cita ya no existe: deja de aparecer en las búsquedas y su registro se elimina después.
            const result={status:'cancelled',summary:proposal.summary,start:proposal.start,timeZone:proposal.timeZone,reference:saved.ref};
            const closed=await store.updateOne(guard,{$set:{last:{...authorization(b),id,result:seal(saved,id,result)},
              removeAfter:new Date(nowMs+limits.retainMs)},$unset:{pending:1,current:1,names:1,startsAt:1},$inc:{version:1}});
            return closed.matchedCount ? result : {status:'working'};
          }
          receipt={...proposal,operationId,htmlLink:response.htmlLink || '',etag:response.etag || ''};
          const checkpoint=await store.updateOne(guard,{$set:{...record(saved,proposal,receipt),'pending.phase':'crm'},
            $inc:{version:1},$unset:{removeAfter:1}});
          if (!checkpoint.matchedCount) {
            // The lease lapsed while Google answered and another attempt closed the operation.
            // Google accepted this write, so record it once instead of leaving it outside TEC.
            const result={status:'saved',crm:proposal.leadId?'expired':'none',htmlLink:receipt.htmlLink,summary:proposal.summary,
              start:proposal.start,timeZone:proposal.timeZone,reference:saved.ref};
            const adopted=await store.updateOne({...scope(b),_id:saved._id,version:saved.version,pending:{$exists:false}},
              {$set:{...record(saved,proposal,receipt),last:{...authorization(b),id,result:seal(saved,id,result)}},$inc:{version:1},$unset:{removeAfter:1}});
            return adopted.matchedCount ? result : {status:'working'};
          }
        }
        if (!proposal.leadId) return await finish('none');
        if (now()-startedAt>=limits.crmMs) return await finish('expired');
        const ok=await syncLead({owner:b.owner,leadId:proposal.leadId,start:proposal.start,action:proposal.action,expectedStart:proposal.expectedStart});
        return await finish(ok ? 'updated' : 'conflict');
      } catch (e) {
        if (e?.bookingBusy && !receipt) {
          // Nothing reached Google: a fresh confirmation returns to its preview and keeps its original deadline.
          await store.updateOne(guard,phase==='preview'
            ? {$set:{'pending.phase':'preview',...(!saved.current && {removeAfter:saved.pending.deadline})},
              $unset:{'pending.lease':1,'pending.leaseUntil':1,'pending.confirmedAt':1}}
            : {$set:{'pending.leaseUntil':new Date(now())}});
          return {status:'bookingBusy'};
        }
        // The same operationId and eventId reconcile a lost Google response on retry.
        const rejected=Boolean(e?.status && [400,401,403,404,409,410,412].includes(e.status));
        if (rejected && !receipt) {
          const changes={$unset:{pending:1}};
          if (!saved.current) changes.$set={removeAfter:new Date(nowMs+limits.previewMs)};
          await store.updateOne(guard,changes);
          return {status:e.slotBusy?'slotBusy':e.reauthorize?'reconnect':'changed'};
        }
        await store.updateOne(guard,{$set:{'pending.leaseUntil':new Date(now())}});
        return {status:receipt?'crmPending':e?.reauthorize?'reconnect':'retry'};
      }
    }
  };
}
