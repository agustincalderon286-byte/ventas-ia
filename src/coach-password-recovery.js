import crypto from 'node:crypto';
import {createRecoveryDelivery} from './coach-recovery-delivery.js';

const digest = value => crypto.createHash('sha256').update(value).digest('hex');
const fresh = () => crypto.randomBytes(32).toString('base64url');
const emailOf = value => typeof value === 'string' ? value.trim().toLowerCase() : '';
const validEmail = value => typeof value === 'string' && value.length <= 254 && /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(value);
const validToken = value => typeof value === 'string' && /^[A-Za-z0-9_-]{43}$/.test(value);
const error = (status, message, field) => Object.assign(new Error(message), {status,field});
const invalidLink = () => error(400,'Este enlace caducó o ya se usó. Solicita uno nuevo.');
const resetFields = {passwordResetDigest:1,passwordResetExpiresAt:1,passwordResetSalt:1,passwordResetEmail:1};
export const COACH_RECOVERY_MESSAGE = 'Si hay una cuenta asociada a ese correo, recibirás un enlace para restablecer tu contraseña. Revisa también correo no deseado.';
export const coachSessionCredentialsMatch = (session,user) => Boolean(session && user && String(session.credentialVersion || '') === String(user.credentialVersion || ''));
export const coachPreviousSessionsQuery = (userId,credentialVersion) => ({userId,$or:[{credentialVersion:String(credentialVersion || '')},{credentialVersion:{$exists:false}}]});

export function createCoachPasswordRecovery({users,sessions,createPassword,sendMail,jobs,deliverySecret,publicOrigin='https://totalelitecoaching.com',now=()=>Date.now(),logger=console}={}){
  if (jobs && sendMail && !deliverySecret) throw new Error('coach_recovery_missing_delivery_secret');
  const origin=new URL(publicOrigin);
  if(!['https:','http:'].includes(origin.protocol) || origin.username || origin.password || (origin.protocol!=='https:' && !['localhost','127.0.0.1','[::1]'].includes(origin.hostname)))throw new Error('coach_recovery_invalid_origin');
  const landing=new URL('/coach/recuperar/',origin.origin).href;
  const limits=new Map(),queue=[];let active=0;const waiters=[];
  function allow(kind,value,max,windowMs){
    const time=now(),key=kind+':'+digest(String(value||''));let bucket=limits.get(key);
    if(!bucket || bucket.until<=time){bucket={until:time+windowMs,count:0};limits.set(key,bucket);}
    bucket.count++;
    if(limits.size>5000){for(const [key,value]of limits)if(value.until<=time)limits.delete(key);if(limits.size>5000)limits.delete(limits.keys().next().value);}
    return bucket.count<=max;
  }
  const queryOne=(query)=>users.findOne(query).select('+passwordResetDigest +passwordResetExpiresAt +passwordResetRequestedAt +passwordResetSalt +passwordResetEmail').lean();
  async function issue(email,job){
    if(!sendMail)return;
    const user=await queryOne({email});
    // Only the stored email is a recovery channel; a provisioned phone username is not one.
    if(!user || !validEmail(user.email) || emailOf(user.email)!==email || !user.passwordSalt)return;
    const time=job?new Date(job.createdAt).getTime():now();
    const secret=job?crypto.createHmac('sha256',deliverySecret).update('TEC recovery v1/'+job._id+'/'+user.passwordSalt).digest('base64url'):fresh();
    const hash=digest(secret);
    const sameGrant=job && user.passwordResetDigest===hash && +new Date(user.passwordResetRequestedAt)===time;
    if (!sameGrant) {
      const claim=await users.findOneAndUpdate({_id:user._id,email:user.email,passwordSalt:user.passwordSalt,
        $or:[{passwordResetRequestedAt:{$exists:false}},{passwordResetRequestedAt:{$lte:new Date(time-60000)}}]},
        {$set:{passwordResetDigest:hash,passwordResetExpiresAt:new Date(time+30*60000),passwordResetRequestedAt:new Date(time),passwordResetSalt:user.passwordSalt,passwordResetEmail:user.email}}, {new:true}).lean();
      if(!claim)return false;
    }
    try{await sendMail({...(job?{idempotencyKey:'tec-recovery-'+job._id}:{}),to:[user.email],subject:'Restablece tu contraseña de TEC Coach',text:`Recibimos una solicitud para restablecer tu contraseña de TEC Coach.\n\nElige una nueva contraseña aquí:\n\n${landing}#token=${secret}\n\nEste enlace es de un solo uso y caduca en 30 minutos. Si no lo solicitaste, ignora este correo: tu contraseña no cambia hasta completar el formulario.\n\nAyuda: agustin@totalelitecoaching.com`});}
    catch(cause){if(job)throw cause;await users.updateOne({_id:user._id,passwordResetDigest:hash},{$unset:resetFields});logger.warn('TEC Coach: no se pudo enviar un correo de recuperación.');return false;}
    return true;
  }
  const delivery=jobs?createRecoveryDelivery({jobs,deliver:issue,now,logger}):null;
  function pump(){
    while(active<2 && queue.length){const email=queue.shift();active++;Promise.resolve().then(()=>issue(email)).catch(()=>logger.warn('TEC Coach: recuperación temporalmente no disponible.')).finally(()=>{active--;pump();if(!active&&!queue.length)waiters.splice(0).forEach(fn=>fn());});}
  }
  function requestRecovery(rawEmail,ip=''){
    const email=emailOf(rawEmail);
    if(!validEmail(email))throw error(400,'Escribe el correo de tu cuenta.', 'email');
    if(!sendMail || queue.length>=64)throw error(503,'La recuperación está temporalmente ocupada. Inténtalo de nuevo.');
    if(!allow('request-ip',ip,12,15*60000))throw error(429,'Has solicitado varios enlaces. Espera unos minutos antes de volver a intentarlo.');
    // The public response never depends on account existence or SMTP timing.
    if(allow('request-email',email,3,3600000)){
      if(delivery)return delivery.enqueue(email).then(()=>({message:COACH_RECOVERY_MESSAGE}));
      queue.push(email);pump();
    }
    return {message:COACH_RECOVERY_MESSAGE};
  }
  async function resetPassword({token,password,passwordConfirmation}={},ip=''){
    if(!allow('complete-ip',ip,20,15*60000))throw error(429,'Demasiados intentos. Espera unos minutos antes de volver a intentarlo.');
    if(!validToken(token))throw invalidLink();
    if(typeof password!=='string' || password.trim().length<8 || password.length>128)throw error(400,'Usa una contraseña de 8 a 128 caracteres.','password');
    if(password!==passwordConfirmation)throw error(400,'Las contraseñas deben ser iguales.','passwordConfirmation');
    const hash=digest(token),user=await queryOne({passwordResetDigest:hash,passwordResetExpiresAt:{$gt:new Date(now())}});
    if(!user || user.passwordSalt!==user.passwordResetSalt || user.email!==user.passwordResetEmail || !validEmail(user.email))throw invalidLink();
    const credentials=await createPassword(password);
    const updated=await users.findOneAndUpdate({_id:user._id,email:user.passwordResetEmail,passwordSalt:user.passwordResetSalt,passwordResetDigest:hash,passwordResetExpiresAt:{$gt:new Date(now())}},
      {$set:{...credentials,credentialVersion:fresh(),updatedAt:new Date(now())},$unset:resetFields},{new:true}).lean();
    if(!updated)throw invalidLink();
    // Credential version invalidates old sessions immediately, even if deletion fails.
    try{await sessions.deleteMany(coachPreviousSessionsQuery(user._id,user.credentialVersion));}catch{logger.warn('TEC Coach: limpieza de sesiones pendiente; credenciales anteriores revocadas.');}
    if(sendMail){try{await sendMail({to:[updated.email],subject:'Tu contraseña de TEC Coach cambió',text:'Se restableció la contraseña de tu cuenta de TEC Coach. Por seguridad cerramos las sesiones anteriores.\n\nEntra con tu correo o teléfono y la nueva contraseña en https://totalelitecoaching.com/coach/login/\n\nSi no reconoces este cambio, contacta a agustin@totalelitecoaching.com.'});}catch{logger.warn('TEC Coach: no se pudo enviar la notificación del cambio de contraseña.');}}
    return {ok:true,message:'Tu contraseña se actualizó. Entra de nuevo con tu nueva contraseña.'};
  }
  return {requestRecovery,resetPassword,resume:delivery?.pump,busy:()=>delivery?.busy() || active>0,drain:()=>active||queue.length?new Promise(resolve=>waiters.push(resolve)):Promise.resolve()};
}

export function registerCoachPasswordRecovery(app,{clearCookies=()=>{},...options}={}){
  const recovery=createCoachPasswordRecovery(options);
  app.get(['/coach/recuperar','/coach/recuperar/'],(req,res,next)=>{res.set({'Cache-Control':'no-store','Referrer-Policy':'no-referrer','X-Robots-Tag':'noindex, nofollow'});next();});
  const route=(operation,status)=>async(req,res)=>{
    res.set({'Cache-Control':'private, no-store','Referrer-Policy':'no-referrer','X-Robots-Tag':'noindex, nofollow'});
    if(!req.is('application/json'))return res.status(415).json({error:'Abre el formulario de recuperación de TEC.'});
    try{const result=await operation(req.body,req.ip);if(result.ok)clearCookies(req,res);res.status(status).json(result);}
    catch(e){const code=[400,429].includes(e.status)?e.status:503;if(code===429)res.set('Retry-After','900');res.status(code).json({error:code===503?'No pudimos completar la solicitud. Inténtalo de nuevo.':e.message,...(e.field?{field:e.field}:{})});}
  };
  app.post('/api/coach/password/request',route((body,ip)=>recovery.requestRecovery(body?.email,ip),202));
  app.post('/api/coach/password/reset',route((body,ip)=>recovery.resetPassword(body,ip),200));
  return recovery;
}
