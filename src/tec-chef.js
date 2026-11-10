import path from 'node:path';
import crypto from 'node:crypto';
import { CHEF_VERSION, CHEF_PROMPT, chefManifest, chefDirectReply, buildChefContext, chefRequestFingerprint, chefAppointmentIntent } from './knowledge/chef-knowledge.js';
export const CHEF_PRIVATE_PILOT=true;
const validId=value=>typeof value==='string'&&/^[a-zA-Z0-9_-]{8,100}$/.test(value);
const text=(value,max=200)=>typeof value==='string'?value.trim().slice(0,max):'';
const fields=['fullName','phone','email','interest','notes','city','appointmentRequested','appointmentPreferredTime'];
export const CHEF_LEAD_EMAIL='agustincalderon286@gmail.com';
export function chefLeadMessage(lead,reference) {
 return {to:[CHEF_LEAD_EMAIL],subject:lead.appointmentRequested?'[TEC Chef · PRUEBA] Solicitud de cita':'[TEC Chef · PRUEBA] Contacto interesado',
 idempotencyKey:'tec-chef-lead-'+reference,
 text:[
 'Contacto captado en TEC Chef · prueba privada de administración.',
 'Referencia: '+reference,'Nombre: '+lead.fullName,'Teléfono: '+(lead.phone||'No indicado'),
 'Correo: '+(lead.email||'No indicado'),'Ciudad: '+(lead.city||'No indicada'),
 'Producto o interés: '+(lead.interest||'Por confirmar'),
 ...(lead.appointmentRequested?['Horario preferido: '+lead.appointmentPreferredTime,'Estado: SOLICITUD PENDIENTE DE CONFIRMACIÓN. No es una cita reservada.']:[]),
 'Notas: '+(lead.notes||'Sin notas'),
 'El administrador confirmó autorización para guardar y enviar estos datos de contacto.',
 'Disponible en TEC, en los contactos del Chef.'
 ].join('\n')};
}
export async function notifyChefLead({auth,lead,requestId,notifications,sendLeadMail,configured}) {
 if(!configured||!notifications||!sendLeadMail)return 'not_configured';
 const _id=crypto.createHash('sha256').update(String(auth.user._id)+'\n'+requestId).digest('hex');
 try{await notifications.updateOne({_id},{$setOnInsert:{_id,ownerUserId:auth.user._id,leadId:String(lead._id),status:'pending',createdAt:new Date()}},{upsert:true});}catch(error){if(error.code!==11000)throw error;}
 const record=await notifications.findOne({_id}).lean();
 if(!record)return 'pending';
 if(!['pending','failed'].includes(record.status))return record.status;
 const claimed=await notifications.findOneAndUpdate({_id,status:record.status},{$set:{status:'sending',updatedAt:new Date()}},{new:true});
 if(!claimed)return 'pending';
 try {
  const result=await sendLeadMail(chefLeadMessage(lead,_id));
  if(result?.accepted!==true)throw Error('tec_mail_uncertain');
  await notifications.updateOne({_id,status:'sending'},{$set:{status:'sent',sentAt:new Date(),updatedAt:new Date()}});
  return 'sent';
 }catch(error){
  // A provider timeout can happen after acceptance. Never blindly resend it.
  const state=error.message==='tec_mail_configuration_failed'?'failed':'unknown';
  await notifications.updateOne({_id,status:'sending'},{$set:{status:state,errorClass:state==='failed'?'configuration':'delivery_unconfirmed',updatedAt:new Date()}});
  return state;
 }
}
export function normalizeChefLead(body={}) {
 const lead={fullName:text(body.fullName,120),phone:text(body.phone,40).replace(/[^+\d]/g,''),email:text(body.email,160).toLowerCase(),interest:text(body.interest,200),notes:text(body.notes,1000)};
 if(lead.fullName.length<2)throw Error('Escribe el nombre del contacto.');
 if(lead.email&&!/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(lead.email))throw Error('Revisa el correo electrónico.');
 if(lead.phone&&!/^\+?\d{10,15}$/.test(lead.phone))throw Error('Revisa el teléfono.');
 if(!lead.phone&&!lead.email)throw Error('Escribe un teléfono o correo para guardar el contacto.');
 lead.city=text(body.city,120);lead.appointmentRequested=body.appointmentRequested===true;lead.appointmentPreferredTime=text(body.appointmentPreferredTime,160);
 if(lead.appointmentRequested&&(!lead.city||lead.city.length<2||lead.appointmentPreferredTime.length<3||lead.interest.length<2))throw Error('Indica la ciudad, el producto y el día u horario preferido.');
 if(body.consentGiven!==true)throw Error('Confirma que tienes autorización para enviar este contacto a Agustín Calderón.');
 if(!validId(body.requestId))throw Error('No pude identificar este guardado. Recarga la página.');
 return lead;
}
export function createChefModels(mongoose) {
 const schema=new mongoose.Schema({ownerUserId:{type:mongoose.Schema.Types.ObjectId,required:true},sessionId:{type:String,required:true},revision:{type:Number,default:0},messages:[{role:String,content:String,sources:[{title:String,url:String}]}],lastRequestId:String,lastFingerprint:String,lastReply:String,lastSources:[{title:String,url:String}],updatedAt:Date,createdAt:{type:Date,default:Date.now}});
 schema.index({ownerUserId:1,sessionId:1},{unique:true});
 const notificationSchema=new mongoose.Schema({_id:String,ownerUserId:mongoose.Schema.Types.ObjectId,leadId:String,status:String,createdAt:Date,updatedAt:Date,sentAt:Date,errorClass:String});
 return {conversations:mongoose.models.TecChefConversation||mongoose.model('TecChefConversation',schema),notifications:mongoose.models.TecChefLeadNotification||mongoose.model('TecChefLeadNotification',notificationSchema)};
}
export async function completeChef(messages,{apiKey=process.env.OPENAI_API_KEY,fetchImpl=fetch}={}) {
 if(!apiKey)throw Error('ai_unconfigured');
 const response=await fetchImpl('https://api.openai.com/v1/chat/completions',{method:'POST',headers:{'Content-Type':'application/json',Authorization:`Bearer ${apiKey}`},signal:AbortSignal.timeout(45000),body:JSON.stringify({model:'gpt-5-mini',reasoning_effort:'low',verbosity:'low',max_completion_tokens:3500,store:false,messages})});
 if(!response.ok)throw Error('ai_unavailable');
 const result=await response.json(),choice=result.choices?.[0];
 if(choice?.finish_reason!=='stop'||typeof choice.message?.content!=='string'||!choice.message.content.trim())throw Error('ai_incomplete');
 return choice.message.content.trim();
}
export function registerTecChef(app,{requireAdmin,getAuth,isAdmin,privateDir,publicDir,conversations,leads,notifications,sendLeadMail=null,mailConfigured=()=>Boolean(sendLeadMail),syncCrm=async()=>{},completion=completeChef,isChefMode=mode=>String(mode||'').trim().toLowerCase()!=='coach'}) {
 const busy=new Set(),rates=new Map();
 const headers=res=>res.set({'Cache-Control':'private, no-store','X-Robots-Tag':'noindex, nofollow','Vary':'Cookie','X-TEC-Chef-Version':CHEF_VERSION});
 const wrap=handler=>async(req,res,next)=>{try{await handler(req,res,next);}catch(e){console.error('TEC Chef request failed',{name:e.name,code:e.code||''});if(!res.headersSent)res.status(503).json({error:'No pude completar la solicitud. Conserva tus datos e inténtalo de nuevo.'});}};
 async function authorize(req,res){headers(res);return requireAdmin(req,res);}
 function sameOrigin(req,res){
  if(req.get('Sec-Fetch-Site')==='cross-site') {res.status(403).json({error:'Abre el Chef desde TEC.'});return false;}
  const origin=req.get('Origin');
  if(origin){try{if(new URL(origin).host!==req.get('host'))throw Error();}catch{res.status(403).json({error:'Abre el Chef desde TEC.'});return false;}}
  if(!req.is('application/json')){res.status(415).json({error:'Envía la solicitud desde el Chef.'});return false;}
  return true;
 }
 const page=wrap(async(req,res)=>{
  headers(res);const auth=await getAuth(req);
  if(!auth?.user)return res.redirect('/coach/login/');
  if(!isAdmin(auth.user))return res.status(403).type('html').send('<!doctype html><html lang="es"><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Chef privado · TEC</title><body style="font-family:system-ui;max-width:560px;margin:70px auto;padding:24px;color:#102f52"><h1>Chef en prueba privada</h1><p>Por ahora esta herramienta solo está disponible para administradores de TEC.</p><a href="/coach/app/">Volver al Coach</a></body></html>');
  res.sendFile(path.join(privateDir,'tec-chef.html'));
 });
 app.get(['/coach/chef','/coach/chef/'],page);
 // Acknowledge the old Chef WhatsApp webhook without sending a reply or running AI.
 app.post('/webhooks/twilio/whatsapp',(req,res)=>res.type('text/xml').send('<?xml version="1.0" encoding="UTF-8"?><Response></Response>'));
 // Retire only the old Chef worker; it must remain reachable without a login to
 // remove its own obsolete public cache. It never touches TEC's offline caches.
 app.get('/chef/sw.js',(req,res)=>res.set('Cache-Control','no-store').sendFile(path.join(publicDir,'chef','sw.js')));
 app.use('/chef',wrap(async(req,res,next)=>{
  if(/\.(?:js|css|png|svg|jpg|webp)$/i.test(req.path)){if(await authorize(req,res))next();return;}
  // Old public slugs, HTML, WhatsApp landing and manifest all require an admin.
  const auth=await getAuth(req);headers(res);
  if(!auth?.user)return res.redirect('/coach/login/');
  if(!isAdmin(auth.user))return res.status(403).json({error:'El Chef está en prueba privada para administradores.'});
  return res.redirect('/coach/chef/');
 }));
 app.get(['/api/chef/admin','/api/chef/stats'],wrap(async(req,res)=>{
  const auth=await authorize(req,res);if(!auth)return;
  res.json({user:{id:String(auth.user._id),name:auth.user.name||''},manifest:chefManifest,adminOnly:true,leadEmailReady:Boolean(notifications&&mailConfigured()),leadEmail:CHEF_LEAD_EMAIL});
 }));
 app.get('/api/chef/admin/conversation',wrap(async(req,res)=>{
  const auth=await authorize(req,res);if(!auth)return;
  if(!validId(req.query.sessionId))return res.status(400).json({error:'Conversación inválida.'});
  const doc=await conversations.findOne({ownerUserId:auth.user._id,sessionId:req.query.sessionId}).lean();
  res.json({messages:(doc?.messages||[]).map(m=>({role:m.role,content:m.content,sources:(m.sources||[]).map(s=>({title:s.title,url:s.url}))}))});
 }));
 const chat=wrap(async(req,res)=>{
  const auth=await authorize(req,res);if(!auth||!sameOrigin(req,res))return;
  const question=text(req.body.pregunta||req.body.question,3000),sessionId=req.body.sessionId,requestId=req.body.requestId;
  if(!question||!validId(sessionId)||!validId(requestId))return res.status(400).json({error:'Escribe una pregunta y vuelve a enviarla desde el Chef.'});
  const owner=String(auth.user._id);
  if(busy.has(owner))return res.status(409).json({error:'Tienes una consulta en proceso. Espera a que termine.'});
  const now=Date.now(),rate=rates.get(owner);if(rate&&rate.until>now&&rate.count>=30)return res.status(429).json({error:'Espera un minuto antes de continuar con las pruebas.'});
  for(const [id,v]of rates)if(v.until<=now)rates.delete(id);
  rates.set(owner,rate?.until>now?{...rate,count:rate.count+1}:{until:now+60000,count:1});
  busy.add(owner);
  try{
   const scope={ownerUserId:auth.user._id,sessionId};
   await conversations.updateOne(scope,{$setOnInsert:{...scope,revision:0,messages:[],createdAt:new Date()}},{upsert:true});
   const doc=await conversations.findOne(scope).lean();if(!doc)throw Error('conversation_missing');
   const fingerprint=chefRequestFingerprint(question);
   if(doc.lastRequestId===requestId){if(doc.lastFingerprint!==fingerprint)return res.status(409).json({error:'Este envío ya contiene otra pregunta.'});return res.json({respuesta:doc.lastReply,sources:doc.lastSources||[],mode:'chef',sessionId,appointmentSuggested:chefAppointmentIntent(question)});}
   const history=(doc.messages||[]).slice(-8).map(m=>({role:m.role,content:m.content}));
   let reply=chefDirectReply(question,history),sources=[];
   if(!reply){const found=buildChefContext(question,history);sources=found.sources;reply=await completion([{role:'system',content:CHEF_PROMPT},{role:'system',content:found.context},...history,{role:'user',content:question}]);}
   const saved=await conversations.findOneAndUpdate({...scope,revision:doc.revision||0},{$push:{messages:{$each:[{role:'user',content:question},{role:'assistant',content:reply,sources}],$slice:-24}},$inc:{revision:1},$set:{updatedAt:new Date(),lastRequestId:requestId,lastFingerprint:fingerprint,lastReply:reply,lastSources:sources}},{new:true});
   if(!saved)return res.status(409).json({error:'La conversación cambió en otra ventana. Recarga antes de continuar.'});
   res.json({respuesta:reply,sources,mode:'chef',sessionId,appointmentSuggested:chefAppointmentIntent(question)});
  }finally{busy.delete(owner);}
 });
 app.post('/api/chef/admin/chat',chat);
 // No public Chef backdoor through the legacy generic chat endpoint.
 app.post('/chat',(req,res,next)=>isChefMode(req.body?.mode)?chat(req,res,next):next());
 app.get('/api/chef/admin/leads',wrap(async(req,res)=>{
  const auth=await authorize(req,res);if(!auth)return;
  const docs=await leads.find({ownerUserId:auth.user._id,source:'chef_personal',sourceDetail:'TEC Chef · prueba privada'}).select('fullName phone email interest city appointmentRequested appointmentPreferredTime createdAt').sort({createdAt:-1}).limit(20).lean();
  res.json({leads:docs.map(d=>({id:String(d._id),fullName:d.fullName,phone:d.phone||'',email:d.email||'',interest:d.interest||'',city:d.city||'',appointmentRequested:Boolean(d.appointmentRequested),appointmentPreferredTime:d.appointmentPreferredTime||'',createdAt:d.createdAt}))});
 }));
 app.post('/api/chef/admin/leads',wrap(async(req,res)=>{
  const auth=await authorize(req,res);if(!auth||!sameOrigin(req,res))return;
  let clean;try{clean=normalizeChefLead(req.body);}catch(e){return res.status(400).json({error:e.message});}
  const scope={ownerUserId:auth.user._id,offlineRequestId:'tec-chef:'+req.body.requestId};
  const payload={...scope,...clean,ownerName:auth.user.name||'',ownerEmail:auth.user.email||'',generatedByUserId:auth.user._id,generatedByName:auth.user.name||'',generatedByAccountType:auth.user.accountType||'owner',source:'chef_personal',sourceDetail:'TEC Chef · prueba privada',status:'nuevo',consentGiven:true,appointmentStatus:clean.appointmentRequested?'solicitada':'',tags:['TEC Chef','prueba_admin'],createdAt:new Date(),updatedAt:new Date()};
  let doc;
  try{doc=await leads.findOneAndUpdate(scope,{$setOnInsert:payload},{upsert:true,new:true});}catch(e){if(e.code!==11000)throw e;doc=await leads.findOne(scope);}
  if(!doc||fields.some(k=>k==='appointmentRequested'?Boolean(doc[k])!==clean[k]:String(doc[k]||'')!==clean[k]))return res.status(409).json({error:'Este guardado ya corresponde a otro contacto. Revisa la lista antes de volver a enviarlo.'});
  let crmSynced=true;try{await syncCrm(auth.user,doc);}catch{crmSynced=false;}
  const emailStatus=await notifyChefLead({auth,lead:doc,requestId:req.body.requestId,notifications,sendLeadMail,configured:mailConfigured()});
  res.json({ok:true,id:String(doc._id),crmSynced,emailStatus,appointmentStatus:clean.appointmentRequested?'solicitada':null});
 }));
 // Legacy outbound Chef campaigns are not used during this private web pilot.
 app.all(['/api/coach/campaigns/chef-intro','/api/coach/campaigns/chef-intro/send'],wrap(async(req,res)=>{
  if(!await authorize(req,res))return;res.status(410).json({error:'Esta fase del Chef se prueba desde /coach/chef/. Las campañas no están habilitadas.'});
 }));
}
