import mongoose from 'mongoose';

// One ledger for the whole TEC deployment, shared by every seller and worker.
// Only the counter is stored. Keys, accounts, addresses and itineraries aren't stored here.
const schema = new mongoose.Schema({_id:String,requests:{type:Number,required:true}},{
  versionKey:false,writeConcern:{w:'majority'}
});
const Store = mongoose.models.CoachRouteAllowance || mongoose.model('CoachRouteAllowance',schema);
const pacific = new Intl.DateTimeFormat('en-US',{year:'numeric',month:'2-digit',timeZone:'America/Los_Angeles'});
function googleMonth(time) {
  const parts=pacific.formatToParts(time);
  return parts.find(p=>p.type==='year').value+'-'+parts.find(p=>p.type==='month').value;
}

export function createRouteAllowance({mapsFreeMonth:month,mapsFreeRequests:raw}, {store=Store,now=Date.now}={}) {
  const amount=/^[1-9]\d{0,3}$/.test(String(raw)) ? Number(raw) : 0;
  // Google aggregates all projects on the billing account. Only a verified,
  // exclusive allocation for this month may be supplied by the operator.
  function configured() {
    const at=now();
    return amount>0 && amount<=5000 && month===googleMonth(at) && month===googleMonth(at+60000);
  }
  return {
    configured,
    async reserve() {
      if (!configured()) return false;
      try {
        const result=await store.updateOne({_id:'compute-routes-pro:'+month,requests:{$lt:amount}},
          {$inc:{requests:1}},{upsert:true,writeConcern:{w:'majority'}});
        // An exhausted counter cannot upsert another row with the same _id.
        // Ambiguous writes and failures consume the attempt conservatively.
        return result.acknowledged===true && (result.modifiedCount===1 || result.upsertedCount===1) && configured();
      } catch {return false;}
    }
  };
}
