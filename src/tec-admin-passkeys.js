import crypto from 'node:crypto';
import {generateRegistrationOptions,verifyRegistrationResponse,generateAuthenticationOptions,verifyAuthenticationResponse} from '@simplewebauthn/server';
export const ADMIN_ASSURANCE_MS=8*60*60*1000;
export const adminAssured=(session,now=Date.now())=>{const delta=now-new Date(session?.adminVerifiedAt||0).getTime();return Number.isFinite(delta)&&delta>=0&&delta<ADMIN_ASSURANCE_MS;};
export function createAdminPasskeyModels(mongoose){
 const key=new mongoose.Schema({ownerUserId:{type:String,index:true,required:true},credentialId:{type:String,unique:true,required:true},publicKey:String,counter:Number,transports:[String],label:String,createdAt:Date});
 const challenge=new mongoose.Schema({_id:String,ownerUserId:String,sessionHash:String,purpose:String,challenge:String,expiresAt:Date});challenge.index({expiresAt:1},{expireAfterSeconds:0});
 return{keys:mongoose.models.TecAdminPasskey||mongoose.model('TecAdminPasskey',key),challenges:mongoose.models.TecAdminChallenge||mongoose.model('TecAdminChallenge',challenge)};
}
export function registerAdminPasskeys(app,{keys,challenges,getAuth,isAdmin,verifyPassword,origin='https://totalelitecoaching.com',now=Date.now,webauthn={generateRegistrationOptions,verifyRegistrationResponse,generateAuthenticationOptions,verifyAuthenticationResponse}}){
 const expectedOrigin=new URL(origin).origin,rpID=new URL(expectedOrigin).hostname,rates=new Map();
 const hasKeys=userId=>keys.exists({ownerUserId:String(userId)});
 const needsVerification=async auth=>Boolean(await hasKeys(auth.user._id))&&!adminAssured(auth.session,now());
 async function authorize(req,res){
  res.set({'Cache-Control':'private, no-store','Vary':'Cookie'});
  if(req.method!=='GET'&&(req.get('Sec-Fetch-Site')==='cross-site'||(req.get('Origin')&&req.get('Origin')!==expectedOrigin))) {res.status(403).json({error:'Abre Seguridad desde el dominio de TEC.'});return null;}
  const auth=await getAuth(req);
  if(!auth?.user){res.status(401).json({error:'Inicia sesión antes de abrir Seguridad.'});return null;}
  if(!isAdmin(auth.user)){res.status(403).json({error:'Esta función es exclusiva de administradores.'});return null;}
  if(!auth.session?.tokenHash){res.status(401).json({error:'Vuelve a iniciar sesión.'});return null;}
  return auth;
 }
 const wrap=handler=>async(req,res)=>{try{const auth=await authorize(req,res);if(!auth)return;if(req.method!=='GET'&&JSON.stringify(req.body||{}).length>65536)return res.status(413).json({error:'La solicitud de verificación es demasiado grande.'});await handler(req,res,auth);}catch(error){console.error('TEC admin verification failed',{name:error.name,code:error.code||''});if(!res.headersSent)res.status(400).json({error:'No se pudo completar la verificación. Inténtalo de nuevo desde TEC.'});}};
 function limit(auth,res){const id=String(auth.user._id),at=now(),entry=rates.get(id);if(entry?.until>at&&entry.count>=12){res.status(429).json({error:'Espera un minuto antes de volver a verificar.'});return false;}for(const[k,v]of rates)if(v.until<=at)rates.delete(k);rates.set(id,{until:entry?.until>at?entry.until:at+60000,count:entry?.until>at?entry.count+1:1});return true;}
 async function issue(auth,purpose,options){
  const id=crypto.randomBytes(24).toString('base64url');await challenges.deleteMany({ownerUserId:String(auth.user._id),sessionHash:auth.session.tokenHash,purpose});
  await challenges.create({_id:id,ownerUserId:String(auth.user._id),sessionHash:auth.session.tokenHash,purpose,challenge:options.challenge,expiresAt:new Date(now()+5*60000)});return{challengeId:id,options};
 }
 async function consume(auth,id,purpose){if(typeof id!=='string'||!/^[A-Za-z0-9_-]{32}$/.test(id))throw Error('bad challenge');const challenge=await challenges.findOneAndDelete({_id:id,ownerUserId:String(auth.user._id),sessionHash:auth.session.tokenHash,purpose,expiresAt:{$gt:new Date(now())}});if(!challenge)throw Error('expired challenge');return challenge;}
 async function mark(auth){auth.session.adminVerifiedAt=new Date(now());await auth.session.save();}
 app.get('/api/coach/admin-security',wrap(async(_req,res,auth)=>{
  const list=await keys.find({ownerUserId:String(auth.user._id)}).select('credentialId label createdAt').lean();res.json({enrolled:list.length>0,verified:adminAssured(auth.session,now()),keys:list.map(k=>({id:k.credentialId,label:k.label,createdAt:k.createdAt})),canonicalUrl:expectedOrigin+'/coach/seguridad/'});
 }));
 app.post('/api/coach/admin-security/enroll/options',wrap(async(req,res,auth)=>{
  if(!limit(auth,res))return;
  if(await needsVerification(auth))return res.status(403).json({error:'Primero verifica una de tus llaves actuales.'});
  if(typeof req.body?.password!=='string'||req.body.password.length>128||!await verifyPassword(req.body.password,auth.user.passwordSalt,auth.user.passwordHash))return res.status(403).json({error:'La contraseña no es correcta.'});
  const list=await keys.find({ownerUserId:String(auth.user._id)}).lean();if(list.length>=5)return res.status(409).json({error:'Ya tienes cinco llaves registradas. Elimina una antes de agregar otra.'});
  const options=await webauthn.generateRegistrationOptions({rpName:'TEC · Administración',rpID,userName:auth.user.email,userDisplayName:auth.user.name||auth.user.email,userID:crypto.createHash('sha256').update(String(auth.user._id)).digest(),attestationType:'none',authenticatorSelection:{residentKey:'preferred',userVerification:'required'},excludeCredentials:list.map(k=>({id:k.credentialId,transports:k.transports})),supportedAlgorithmIDs:[-7,-257]});
  res.json(await issue(auth,'enroll',options));
 }));
 app.post('/api/coach/admin-security/enroll/verify',wrap(async(req,res,auth)=>{
  if(!limit(auth,res))return;
  // If enrollment happened elsewhere since options were issued, require step-up.
  if(await needsVerification(auth))return res.status(403).json({error:'Primero verifica una de tus llaves actuales.'});
  const challenge=await consume(auth,req.body?.challengeId,'enroll');
  const verified=await webauthn.verifyRegistrationResponse({response:req.body?.response,expectedChallenge:challenge.challenge,expectedOrigin,expectedRPID:rpID,requireUserVerification:true,supportedAlgorithmIDs:[-7,-257]});
  if(!verified.verified||!verified.registrationInfo?.credential)throw Error('not verified');
  const credential=verified.registrationInfo.credential;
  await keys.create({ownerUserId:String(auth.user._id),credentialId:credential.id,publicKey:Buffer.from(credential.publicKey).toString('base64'),counter:credential.counter,transports:credential.transports||[],label:String(req.body?.label||'Mi llave de acceso').trim().slice(0,80),createdAt:new Date(now())});
  await mark(auth);res.json({ok:true,enrolled:true,verified:true});
 }));
 app.post('/api/coach/admin-security/verify/options',wrap(async(_req,res,auth)=>{
  if(!limit(auth,res))return;const list=await keys.find({ownerUserId:String(auth.user._id)}).lean();if(!list.length)return res.status(409).json({error:'Registra primero una llave de acceso.'});
  const options=await webauthn.generateAuthenticationOptions({rpID,userVerification:'required',allowCredentials:list.map(k=>({id:k.credentialId,transports:k.transports}))});res.json(await issue(auth,'verify',options));
 }));
 app.post('/api/coach/admin-security/verify/complete',wrap(async(req,res,auth)=>{
  if(!limit(auth,res))return;const challenge=await consume(auth,req.body?.challengeId,'verify');
  if(typeof req.body?.response?.id!=='string'||! /^[A-Za-z0-9_-]{1,1500}$/.test(req.body.response.id))throw Error('bad credential');
  const credential=await keys.findOne({ownerUserId:String(auth.user._id),credentialId:req.body.response.id}).lean();if(!credential)throw Error('unknown key');
  const verification=await webauthn.verifyAuthenticationResponse({response:req.body.response,expectedChallenge:challenge.challenge,expectedOrigin,expectedRPID:rpID,requireUserVerification:true,credential:{id:credential.credentialId,publicKey:new Uint8Array(Buffer.from(credential.publicKey,'base64')),counter:credential.counter,transports:credential.transports}});
  if(!verification.verified)throw Error('not verified');
  await keys.updateOne({_id:credential._id},{$max:{counter:verification.authenticationInfo.newCounter}});await mark(auth);res.json({ok:true,verified:true});
 }));
 app.delete('/api/coach/admin-security/keys/:id',wrap(async(req,res,auth)=>{
  if(!limit(auth,res))return;
  if(!adminAssured(auth.session,now())||now()-new Date(auth.session.adminVerifiedAt).getTime()>5*60000)return res.status(403).json({error:'Verifica tu llave de nuevo antes de eliminarla.'});
  if(typeof req.body?.password!=='string'||req.body.password.length>128||!await verifyPassword(req.body.password,auth.user.passwordSalt,auth.user.passwordHash))return res.status(403).json({error:'La contraseña no es correcta.'});
  await keys.deleteOne({ownerUserId:String(auth.user._id),credentialId:String(req.params.id).slice(0,1500)});res.json({ok:true});
 }));
 return{hasKeys,needsVerification};
}
