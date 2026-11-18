import crypto from 'node:crypto';

export function recoveryJobModel(mongoose) {
  const schema = new mongoose.Schema({
    _id:String, email:String, createdAt:Date, purgeAt:Date, dueAt:Date,
    status:{type:String,enum:['queued','sending','sent','skipped','failed']},
    attempts:{type:Number,default:0}, lease:String, finishedAt:Date
  }, {versionKey:false});
  schema.index({status:1,dueAt:1});
  schema.index({purgeAt:1},{expireAfterSeconds:0});
  return mongoose.models.CoachRecoveryJob || mongoose.model('CoachRecoveryJob',schema);
}

// Only the address and work state are persisted. A fresh worker can resume an expired lease.
export function createRecoveryDelivery({jobs,deliver,now=Date.now,logger=console}) {
  let running=null;
  async function run() {
    for (let i=0;i<16;i++) {
      const time=now(), lease=crypto.randomUUID();
      const job=await jobs.findOneAndUpdate(
        {status:{$in:['queued','sending']},dueAt:{$lte:new Date(time)}},
        {$set:{status:'sending',lease,dueAt:new Date(time+90000)},$inc:{attempts:1}},
        {sort:{dueAt:1},new:true}).lean();
      if (!job) return;
      const owned={_id:job._id,lease};
      let status='failed';
      if (time-new Date(job.createdAt).getTime()<15*60000 && job.attempts<=4) {
        try { status=await deliver(job.email,job) ? 'sent' : 'skipped'; }
        catch {
          if (job.attempts<4) {
            const delay=15000*2**(job.attempts-1);
            await jobs.updateOne(owned,{$set:{status:'queued',dueAt:new Date(now()+delay)},$unset:{lease:1}});
            logger.warn('TEC Coach: entrega de recuperación pendiente de reintento.');
            continue;
          }
        }
      }
      await jobs.updateOne(owned,{$set:{status,finishedAt:new Date(now())},$unset:{email:1,lease:1}});
      if(status==='failed')logger.warn('TEC Coach: entrega de recuperación agotada.');
    }
  }
  function pump() {
    if (!running) running=run().catch(()=>logger.warn('TEC Coach: cola de recuperación temporalmente inaccesible.')).finally(()=>{running=null;});
    return running;
  }
  return {
    async enqueue(email) {
      const time=now();
      const id=crypto.createHash('sha256').update(email+'|'+Math.floor(time/60000)).digest('hex');
      try {
        await jobs.updateOne({_id:id},{$setOnInsert:{email,createdAt:new Date(time),purgeAt:new Date(time+86400000),
          dueAt:new Date(time),status:'queued',attempts:0}},{upsert:true});
      } catch(e) { if(e.code!==11000)throw e; }
      void pump();
    },
    pump,
    busy: () => Boolean(running)
  };
}
