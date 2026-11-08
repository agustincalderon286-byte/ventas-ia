import { BOOK_LOGROS, emptyState, mergeStates } from '../public/audiolibros/logros-core.js';

export function validateAchievementState(value,catalog,allowFull=true){
  if(!value||value.v!==1||!value.books||typeof value.books!=='object'||Array.isArray(value.books)||Buffer.byteLength(JSON.stringify(value))>200_000)throw new TypeError('invalid achievement state');
  const out=emptyState(),byId=new Map(catalog.map(b=>[b.id,b]));
  for(const [id,row] of Object.entries(value.books)){
    const book=byId.get(id);
    if(!book||!BOOK_LOGROS.some(b=>b[0]===id)||!row||typeof row!=='object'||Array.isArray(row))throw new TypeError('invalid book');
    const clean={};
    for(const field of Object.keys(row)){
      if(!['sample','full','readingSample','readingFull'].includes(field))throw new TypeError('invalid range type');
      const ranges=row[field],full=field==='full'||field==='readingFull',limit=full?book.duration:Math.floor(book.duration*100)/1000;
      if(!Array.isArray(ranges)||ranges.length>4096||ranges.some(r=>!Array.isArray(r)||r.length!==2||!r.every(Number.isFinite)||r[0]<0||r[1]<=r[0]||r[1]>limit+.01))throw new TypeError('invalid ranges');
      clean[field]=full&&!allowFull?[]:ranges.map(([a,b])=>[a,Math.min(b,limit)]);
    }
    out.books[id]=clean;
  }
  return mergeStates(out);
}

export async function mergeSavedAchievements(model,id,incoming){
  for(let attempt=0;attempt<5;attempt++){
    const current=await model.findById(id).lean();
    const state=mergeStates(current?.state,incoming),revision=(current?.revision||0)+1;
    try{
      const result=await model.updateOne({_id:id,revision:current?.revision??{$exists:false}},{$set:{state,revision}},{upsert:!current});
      if(result.matchedCount||result.upsertedCount)return state;
    }catch(error){if(error.code!==11000)throw error;}
  }
  throw new Error('achievement update conflict');
}

export function registerAudioAchievements({route,account,isMember,model,catalog}){
  route('get','/logros',async(req,res)=>{
    const user=await account(req);if(!user)return res.status(401).json({error:'Ingresa para guardar tus logros en tu cuenta.'});
    const row=await model.findById(user._id).lean();res.json({state:mergeStates(row?.state)});
  });
  route('put','/logros',async(req,res)=>{
    const user=await account(req);if(!user)return res.status(401).json({error:'Ingresa para guardar tus logros en tu cuenta.'});
    let incoming;
    try{incoming=validateAchievementState(req.body?.state,catalog);}catch{return res.status(400).json({error:'No pudimos validar el avance de los logros.'});}
    const hasFull=Object.values(incoming.books).some(b=>b.full?.length||b.readingFull?.length);
    if(hasFull&&!await isMember(user))incoming=validateAchievementState(incoming,catalog,false);
    res.json({state:await mergeSavedAchievements(model,user._id,incoming)});
  });
}
