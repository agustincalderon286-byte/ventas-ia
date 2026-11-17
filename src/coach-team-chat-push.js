import webpush from 'web-push';
import { createHash, createECDH, randomUUID } from 'node:crypto';
const hash=value=>createHash('sha256').update(String(value)).digest('hex');
const invalid=()=>Object.assign(new Error('La suscripción del dispositivo no es válida. Vuelve a activarla.'),{status:400});
export function normalizeChatPushSubscription(input){
  if(!input||typeof input.endpoint!=='string'||input.endpoint.length>2048)throw invalid();
  let url;try{url=new URL(input.endpoint);}catch{throw invalid();}
  const host=url.hostname.toLowerCase();
  const allowed=['fcm.googleapis.com','updates.push.services.mozilla.com','web.push.apple.com','webpush.push.apple.com'].includes(host)||(host.endsWith('.notify.windows.com')&&/^[a-z0-9.-]+$/.test(host));
  if(!allowed||url.protocol!=='https:'||url.port||url.username||url.password||url.hash)throw invalid();
  const auth=input.keys?.auth,p256dh=input.keys?.p256dh;
  if(typeof auth!=='string'||typeof p256dh!=='string'||!/^[A-Za-z0-9_-]{22}={0,2}$/.test(auth)||!/^[A-Za-z0-9_-]{87}={0,2}$/.test(p256dh))throw invalid();
  const point=Buffer.from(p256dh,'base64url');
  if(Buffer.from(auth,'base64url').length!==16||point.length!==65||point[0]!==4)throw invalid();
  try{const e=createECDH('prime256v1');e.generateKeys();e.computeSecret(point);}catch{throw invalid();}
  return {endpoint:url.href,expirationTime:null,keys:{auth,p256dh}};
}
export function createCoachPushModels(mongoose){
  const room=new mongoose.Schema({roomId:String,ownerId:String,enabledAt:Date},{_id:false});
  const device=new mongoose.Schema({endpointHash:{type:String,required:true,unique:true},accountId:{type:mongoose.Schema.Types.ObjectId,required:true},sessionId:{type:mongoose.Schema.Types.ObjectId,required:true},rooms:[room],push:{type:mongoose.Schema.Types.Mixed,required:true,select:false},vapidKey:String,enabled:Boolean,expiresAt:Date,updatedAt:Date});
  device.index({'rooms.roomId':1,enabled:1,_id:1});device.index({expiresAt:1},{expireAfterSeconds:0});
  const config=new mongoose.Schema({_id:String,publicKey:String,privateKey:{type:String,select:false},createdAt:Date});
  const delivery=new mongoose.Schema({_id:String,messageId:mongoose.Schema.Types.ObjectId,subscriptionId:mongoose.Schema.Types.ObjectId,accountId:mongoose.Schema.Types.ObjectId,sessionId:mongoose.Schema.Types.ObjectId,roomId:String,ownerId:String,state:String,attempts:{type:Number,default:0},nextAt:Date,leaseUntil:Date,leaseToken:String,expiresAt:Date});
  delivery.index({state:1,nextAt:1,leaseUntil:1});delivery.index({subscriptionId:1,messageId:-1});delivery.index({expiresAt:1},{expireAfterSeconds:0});
  return {devices:mongoose.models.CoachChatPushDevice||mongoose.model('CoachChatPushDevice',device),configs:mongoose.models.CoachChatPushConfig||mongoose.model('CoachChatPushConfig',config),deliveries:mongoose.models.CoachChatPushDelivery||mongoose.model('CoachChatPushDelivery',delivery)};
}
export async function loadChatPushKeys(configs){
  let value;
  try{value=await configs.findOneAndUpdate({_id:'team-chat-v1'},{$setOnInsert:{...webpush.generateVAPIDKeys(),createdAt:new Date()}},{upsert:true,new:true}).select('+privateKey').lean();}
  catch(error){if(error.code!==11000)throw error;value=await configs.findById('team-chat-v1').select('+privateKey').lean();}
  const curve=createECDH('prime256v1');curve.setPrivateKey(Buffer.from(value?.privateKey||'','base64url'));
  if(curve.getPublicKey().toString('base64url')!==value?.publicKey)throw new Error('Invalid chat push keys');
  return {publicKey:value.publicKey,privateKey:value.privateKey,subject:'mailto:agustin@totalelitecoaching.com'};
}
// The external push provider receives ONLY this generic application signal.
// No account, room, message ID, names, message text or sales data is transmitted.
export const CHAT_PUSH_SIGNAL='{"type":"tec-chat"}';
export function createCoachTeamPush({models,messages,readers,requireActive,getTeamKey,roomIdForKey,canDeliver,now=Date.now,send=(...args)=>webpush.sendNotification(...args)}){
  const {devices,configs,deliveries}=models;
  let keys=null,running=false,stopped=false,timer;
  const rates=new Map();
  async function context(req,res,write=false){
    res.set('Cache-Control','private, no-store').set('X-TEC-Chat-Push-Version','20261117-push1').vary('Cookie');
    const auth=await requireActive(req,res);if(!auth)return null;
    const accountId=String(auth.user._id);
    if((req.get('X-TEC-Account-ID')&&req.get('X-TEC-Account-ID')!==accountId)||(write&&req.body?.accountId!==accountId)){res.status(403).json({error:'La cuenta cambió. Vuelve a abrir TEC.'});return null;}
    const selected=write?req.body?.teamOwnerId:req.query.teamOwnerId;
    const key=await getTeamKey(auth.user,selected);if(typeof key!=='string'||!key.trim())throw Object.assign(new Error('No tienes acceso a este grupo.'),{status:403});
    const roomId=roomIdForKey(key),expected=write?req.body?.roomId:req.query.roomId;
    if((write&&expected!==roomId)||(!write&&expected!==undefined&&expected!==roomId)){res.status(403).json({error:'El equipo cambió. Vuelve a abrir TEC.'});return null;}
    const at=now();for(const[k,v]of rates)if(v.until<=at)rates.delete(k);
    const rate=rates.get(accountId)||{count:0,until:at+60000};
    if((!rates.has(accountId)&&rates.size>=2000)||++rate.count>90){res.status(429).json({error:'Espera un minuto y vuelve a intentar.'});return null;}rates.set(accountId,rate);
    return {auth,accountId,roomId,ownerId:key};
  }
  const unavailable=(res,error)=>res.status([400,403].includes(error?.status)?error.status:503).json({error:[400,403].includes(error?.status)?error.message:'No pude actualizar las notificaciones. Vuelve a intentar.'});
  function register(app){
    app.get('/api/coach/team-chat/push/config',async(req,res)=>{try{const ctx=await context(req,res);if(!ctx)return;if(!keys)return unavailable(res);res.json({ready:true,publicKey:keys.publicKey,accountId:ctx.accountId,roomId:ctx.roomId,ownerId:ctx.ownerId});}catch(e){unavailable(res,e);}});
    for(const action of ['status','subscribe','unsubscribe','inbox'])app.post('/api/coach/team-chat/push/'+action,async(req,res)=>{
      try{
        const ctx=await context(req,res,true);if(!ctx)return;
        const subscription=normalizeChatPushSubscription(req.body?.subscription),endpointHash=hash(subscription.endpoint);
        const current=await devices.findOne({endpointHash}).select('+push').lean();
        const sameKeys=current&&current.push?.keys?.auth===subscription.keys.auth&&current.push?.keys?.p256dh===subscription.keys.p256dh;
        const owned=current&&String(current.accountId)===ctx.accountId;
        const active=owned&&sameKeys&&current.enabled&&String(current.sessionId)===String(ctx.auth.session?._id)&&current.vapidKey===keys?.publicKey;
        const matching=current?.rooms?.find(room=>room.roomId===ctx.roomId);
        if(action==='status')return res.json({enabled:Boolean(active&&matching)});
        if(action==='inbox'){
          if(!active)return res.json({notify:false});
          const candidates=await deliveries.find({subscriptionId:current._id,accountId:ctx.accountId,sessionId:ctx.auth.session._id,state:{$in:['sending','accepted']},expiresAt:{$gt:new Date(now())}}).sort({messageId:-1}).limit(20).lean();
          for(const candidate of candidates){
            const room=current.rooms.find(r=>r.roomId===candidate.roomId);if(!room)continue;
            const message=await messages.findById(candidate.messageId).lean();
            if(!message||new Date(message.createdAt).getTime()<now()-300000||new Date(room.enabledAt)>new Date(message.createdAt))continue;
            let key;try{key=await getTeamKey(ctx.auth.user,room.ownerId);}catch{continue;}
            if(roomIdForKey(key)!==candidate.roomId)continue;
            return res.json({notify:true,url:'/coach/app/?openTeamChat=1&teamChatOwnerId='+encodeURIComponent(room.ownerId)});
          }
          return res.json({notify:false});
        }
        if(action==='unsubscribe'){
          const remaining=owned&&sameKeys?(current.rooms||[]).filter(r=>r.roomId!==ctx.roomId):[];
          if(owned&&sameKeys)await devices.updateOne({_id:current._id,accountId:ctx.accountId,sessionId:ctx.auth.session?._id},{$set:{rooms:remaining,enabled:Boolean(remaining.length),updatedAt:new Date(now())}});
          return res.json({enabled:false,anyEnabled:Boolean(remaining.length)});
        }
        if(!keys||!ctx.auth.session?._id)return unavailable(res);
        if(current&&!sameKeys)return res.status(409).json({error:'Renueva las notificaciones de este navegador y vuelve a intentar.'});
        const rooms=owned&&String(current.sessionId)===String(ctx.auth.session._id)?[...(current.rooms||[])]:[];
        if(!rooms.some(r=>r.roomId===ctx.roomId))rooms.push({roomId:ctx.roomId,ownerId:ctx.ownerId,enabledAt:new Date(now())});
        await devices.updateOne({endpointHash},{$set:{accountId:ctx.accountId,sessionId:ctx.auth.session._id,rooms,push:subscription,vapidKey:keys.publicKey,enabled:true,updatedAt:new Date(now()),expiresAt:ctx.auth.session.expiresAt}},{upsert:true});
        res.json({enabled:true});
      }catch(error){return unavailable(res,error);}
    });
  }
  async function fanout(){
    const at=new Date(now()),cutoff=new Date(now()-300000),token=randomUUID();
    await messages.updateMany({pushState:{$in:['pending','working']},createdAt:{$lt:cutoff}},{$set:{pushState:'done'}});
    const message=await messages.findOneAndUpdate({createdAt:{$gte:cutoff},$or:[{pushState:'pending'},{pushState:'working',pushLeaseUntil:{$lt:at}}]},{$set:{pushState:'working',pushLeaseUntil:new Date(now()+60000),pushLeaseToken:token}},{new:true}).lean();
    if(!message)return false;
    const filter={rooms:{$elemMatch:{roomId:message.roomId,enabledAt:{$lte:message.createdAt}}},accountId:{$ne:message.senderId},enabled:true,expiresAt:{$gt:at}};
    if(message.pushFanoutAfter)filter._id={$gt:message.pushFanoutAfter};
    const targets=await devices.find(filter).sort({_id:1}).limit(100).lean();
    if(targets.length)await deliveries.bulkWrite(targets.map(device=>({updateOne:{filter:{_id:hash(`${message._id}:${device._id}`)},update:{$setOnInsert:{messageId:message._id,subscriptionId:device._id,accountId:device.accountId,sessionId:device.sessionId,roomId:message.roomId,ownerId:device.rooms.find(r=>r.roomId===message.roomId).ownerId,state:'pending',attempts:0,nextAt:at,expiresAt:new Date(now()+86400000)}},upsert:true}})),{ordered:false});
    await messages.updateOne({_id:message._id,pushLeaseToken:token},{$set:{pushState:targets.length===100?'pending':'done',...(targets.length?{pushFanoutAfter:targets.at(-1)._id}:{})},$unset:{pushLeaseUntil:1,pushLeaseToken:1}});
    return true;
  }
  async function deliver(){
    const at=new Date(now()),token=randomUUID();
    const job=await deliveries.findOneAndUpdate({attempts:{$lt:5},$or:[{state:'pending',nextAt:{$lte:at}},{state:'sending',leaseUntil:{$lt:at}}]},{$set:{state:'sending',leaseUntil:new Date(now()+45000),leaseToken:token},$inc:{attempts:1}},{new:true}).lean();
    if(!job)return false;
    const match={_id:job._id,leaseToken:token},finish=state=>deliveries.updateOne(match,{$set:{state},$unset:{leaseToken:1,leaseUntil:1}});
    try{
      const [device,message]=await Promise.all([devices.findById(job.subscriptionId).select('+push').lean(),messages.findById(job.messageId).lean()]);
      const room=device?.rooms?.find(r=>r.roomId===job.roomId);
      if(!device?.enabled||!room||String(device.accountId)!==String(job.accountId)||String(device.sessionId)!==String(job.sessionId)||device.vapidKey!==keys.publicKey||!message||new Date(message.createdAt).getTime()<now()-300000||String(message.senderId)===String(device.accountId)||new Date(room.enabledAt)>new Date(message.createdAt)||!(await canDeliver(device,room.ownerId))){await finish('skipped');return true;}
      if(await readers.findOne({accountId:device.accountId,roomId:job.roomId,lastReadMessageId:{$gte:message._id}}).lean()){await finish('skipped');return true;}
      // Explicit per-device opt-in is required. Provider payload is a fixed generic signal.
      await send(normalizeChatPushSubscription(device.push),CHAT_PUSH_SIGNAL,{vapidDetails:keys,TTL:300,urgency:'high',topic:'tec-chat',timeout:8000});
      await finish('accepted');
    }catch(error){
      if([404,410].includes(error.statusCode)){await devices.updateOne({_id:job.subscriptionId,accountId:job.accountId,sessionId:job.sessionId},{$set:{enabled:false}});await finish('skipped');}
      else if(job.attempts>=5||[400,401,403].includes(error.statusCode)||error.status===400)await finish('failed');
      else await deliveries.updateOne(match,{$set:{state:'pending',nextAt:new Date(now()+Math.min(60000,5000*2**job.attempts))},$unset:{leaseUntil:1,leaseToken:1}});
    }
    return true;
  }
  async function run(){if(running||stopped||!keys)return;running=true;try{
    await deliveries.updateMany({state:'sending',attempts:{$gte:5},leaseUntil:{$lt:new Date(now())}},{$set:{state:'failed'}});
    for(let n=0;n<5&&!stopped;n++)if(!(await fanout()))break;
    for(let n=0;n<20&&!stopped;n++)if(!(await deliver()))break;
  }catch{console.warn('TEC chat push: delivery pending');}finally{running=false;}}
  return {models,register,run,fanout,deliver,async initialize(){keys=await loadChatPushKeys(configs);},start(){stopped=false;void run();timer=setInterval(()=>void run(),5000);timer.unref?.();},stop(){stopped=true;clearInterval(timer);},busy:()=>running};
}
