import crypto from 'node:crypto';
import express from 'express';
import {readFile} from 'node:fs/promises';
import path from 'node:path';
import QRCode from 'qrcode';
import {completeChef,normalizeChefLead} from './tec-chef.js';
import {CHEF_PROMPT,chefManifest,chefDirectReply,buildChefContext,chefRequestFingerprint,chefAppointmentIntent} from './knowledge/chef-knowledge.js';
import {canReadDownline} from './tec-team-supervision.js';
import {decodeChefPhoto,chefPhotoContent,CHEF_PHOTO_PROMPT} from './tec-chef-photo.js';

import {CHAT_MEDIA_MAX_BYTES,decodeChatAudioToPcm} from './coach-team-chat-media.js';
import {pcmToWav,sendVoiceError,speechText,streamVoiceResponse} from './coach-voice.js';

const COOKIE='tec_chef_visitor';
const DAY=86400000;
const validId=value=>typeof value==='string'&&/^[a-zA-Z0-9_-]{8,100}$/.test(value);
const validSlug=value=>typeof value==='string'&&/^[a-z0-9-]{1,48}$/.test(value);
const hash=value=>crypto.createHash('sha256').update(value).digest('hex');
const clean=(value,max)=>typeof value==='string'?value.trim().slice(0,max):'';
const compareFields=['fullName','phone','email','city','interest','notes','appointmentRequested','appointmentPreferredTime'];
const publicName=ctx=>ctx?.userDoc?.name||'Tu distribuidor TEC';
const PUBLIC_PROMPT=CHEF_PROMPT.replace('Esta es una prueba privada de TEC,','Este es el Chef público de TEC,').replace('WhatsApp, SMS y campañas no forman parte de esta prueba.','WhatsApp, SMS y campañas no forman parte de este chat.').replaceAll('«Solicitar cita»','«Solicitar información»');
const publicReply=text=>text.replaceAll('«Solicitar cita»','«Solicitar información»').replace('Agustín confirmará la disponibilidad contigo.','Tu distribuidor confirmará la disponibilidad contigo.').replace('Esta prueba funciona dentro de TEC.','Este Chef funciona en esta página.');

export function createPublicChefModels(mongoose){
  const session=new mongoose.Schema({_id:String,createdAt:Date,expiresAt:Date});
  session.index({expiresAt:1},{expireAfterSeconds:0});
  const conversation=new mongoose.Schema({sellerUserId:{type:mongoose.Schema.Types.ObjectId,default:null},visitorHash:String,sessionId:String,revision:{type:Number,default:0},messages:[{role:String,content:String,hasPhoto:Boolean,voiceRequestId:String,sources:[{title:String,url:String}]}],lastRequestId:String,lastFingerprint:String,lastReply:String,lastSources:[{title:String,url:String}],createdAt:Date,updatedAt:Date});
  conversation.index({sellerUserId:1,visitorHash:1,sessionId:1},{unique:true});
  conversation.index({updatedAt:1},{expireAfterSeconds:30*86400});
  return {visitors:mongoose.models.TecChefPublicVisitor||mongoose.model('TecChefPublicVisitor',session),conversations:mongoose.models.TecChefPublicConversation||mongoose.model('TecChefPublicConversation',conversation)};
}

export function publicChefLeadPayload(ctx,body,visitorHash,now=new Date()){
  if(body?.consentGiven!==true)throw Error('Confirma que autorizas al distribuidor y a su equipo de supervisión a recibir tu solicitud y contactarte.');
  const lead=normalizeChefLead({...body,consentGiven:true});
  const ownership=ctx.ownership;
  if(!ctx.userDoc?._id||!ownership?.ownerUserId)throw Error('Este enlace no tiene un distribuidor disponible.');
  return {
    ownerUserId:ownership.ownerUserId,ownerName:ownership.ownerName||'',ownerEmail:ownership.ownerEmail||'',
    generatedByUserId:ctx.userDoc._id,generatedByName:ctx.userDoc.name||'',generatedByAccountType:ctx.userDoc.accountType||'owner',
    offlineRequestId:'chef-public:'+hash(visitorHash+'\n'+String(ctx.userDoc._id)+'\n'+body.requestId),
    ...lead,chefSlug:ctx.slug,source:'chef_personal',sourceDetail:body.origin==='alexa'?'TEC Chef · Alexa':'TEC Chef · enlace público',
    status:'nuevo',consentGiven:true,appointmentStatus:lead.appointmentRequested?'solicitada':'',tags:['TEC Chef','enlace_personal'],createdAt:now,updatedAt:now
  };
}

export function registerPublicChef(app,{requireActive,getBySlug,getDefault,getOwnContext,getDownline,hasDownline=async()=>false,visitors,conversations,leads,privateDir,publicDir,completion=completeChef,voice=null,syncCrm=async()=>{},publicOrigin='https://totalelitecoaching.com',now=Date.now,limits={}}){
  const origin=new URL(publicOrigin).origin,rates=new Map(),busy=new Set(),activeVoice=new Set();
  let activePhotoRequests=0;
  const headers=res=>res.set({'Cache-Control':'private, no-store','X-Robots-Tag':'noindex, nofollow','Vary':'Cookie','X-TEC-Chef-Public-Version':'20261118-voz1'});
  const wrap=fn=>async(req,res,next)=>{try{headers(res);await fn(req,res,next);}catch(error){console.error('TEC public Chef unavailable',{name:error.name,code:error.code||''});if(!res.headersSent)res.status(503).json({error:'No pude completar la solicitud. Conserva tus datos e inténtalo de nuevo.'});}};
  const consume=(key,max,windowMs,res)=>{
    const at=now();if(rates.size>=2000)for(const [id,value] of rates)if(value.until<=at)rates.delete(id);
    if(!rates.has(key)&&rates.size>=20000){res.status(429).json({error:'El Chef está ocupado. Intenta de nuevo más tarde.'});return false;}
    const old=rates.get(key),value=old?.until>at?old:{count:0,until:at+windowMs};value.count++;rates.set(key,value);
    if(value.count>max){res.set('Retry-After',String(Math.ceil((value.until-at)/1000)));res.status(429).json({error:'Llegaste al límite de consultas por ahora. Intenta de nuevo más tarde.'});return false;}return true;
  };
  const ipKey=req=>hash(String(req.ip||''));
  const sameOrigin=(req,res,requireJson=true)=>{
    if(req.get('Sec-Fetch-Site')==='cross-site'){res.status(403).json({error:'Envía tu solicitud desde esta página del Chef.'});return false;}
    const supplied=req.get('Origin');
    if(supplied){let allowed=false;try{const value=new URL(supplied);allowed=value.origin===origin||value.host===req.get('host');}catch{}if(!allowed){res.status(403).json({error:'Envía tu solicitud desde esta página del Chef.'});return false;}}
    if(requireJson&&!req.is('application/json')){res.status(415).json({error:'Envía tu solicitud desde el formulario del Chef.'});return false;}return true;
  };
  async function context(req,res){
    const slug=req.query.chef;
    if(slug!=null&&!validSlug(slug)){res.status(400).json({error:'El enlace del Chef no es válido.'});return undefined;}
    const ctx=slug?await getBySlug(slug):await getDefault();
    if(slug&&!ctx){res.status(404).json({error:'Este Chef no está disponible. Pide un enlace actualizado a quien te lo compartió.'});return undefined;}
    return ctx||null;
  }
  async function visitor(req,res,create=false){
    const raw=(req.get('Cookie')||'').split(';').map(value=>value.trim()).find(value=>value.startsWith(COOKIE+'='))?.slice(COOKIE.length+1);
    if(raw&&/^[a-f0-9]{64}$/.test(raw)){
      const value=hash(raw),doc=await visitors.findOne({_id:value,expiresAt:{$gt:new Date(now())}}).lean();if(doc)return value;
    }
    if(!create){res.status(401).json({error:'Vuelve a abrir el Chef para continuar. Tu pregunta sigue en esta página.'});return null;}
    if(!consume('session:'+ipKey(req),limits.sessionsPerHour||200,3600000,res))return null;
    const token=crypto.randomBytes(32).toString('hex'),value=hash(token);
    await visitors.create({_id:value,createdAt:new Date(now()),expiresAt:new Date(now()+14*DAY)});
    res.cookie(COOKIE,token,{httpOnly:true,sameSite:'lax',secure:req.secure||req.get('X-Forwarded-Proto')==='https',maxAge:14*DAY,path:'/'});
    return value;
  }
  const assets=new Map([
    ['alexa-demo.mp4','chef/assets/alexa-demo.mp4'],
    ['alexa-demo.jpg','chef/assets/alexa-demo.jpg'],
    ['alexa-demo.es.vtt','chef/assets/alexa-demo.es.vtt'],
    ['customer.js','chef/customer.js'],['customer.css','chef/customer.css'],['robot.webp','chef/assets/tec-chef-robot-v1.webp'],['tec-chef.css','chef/tec-chef.css']
  ]);
  app.get('/chef/public/:asset',(req,res)=>{
    const file=assets.get(req.params.asset);if(!file)return res.status(404).end();
    res.set('Cache-Control','public, max-age=3600').sendFile(path.join(publicDir,file));
  });
  app.get('/chef/alexa/pagina.css',(req,res)=>res.sendFile(path.join(publicDir,'chef/alexa/pagina.css')));
  app.get(/^\/chef\/alexa\/(privacidad|condiciones)\/?$/,(req,res)=>
    res.sendFile(path.join(publicDir,'chef/alexa',req.params[0],'index.html')));

  app.get(['/chef/alexa/cita','/chef/alexa/cita/'],wrap(async(req,res)=>{
    if(req.query.vendedor===undefined)return res.sendFile(path.join(privateDir,'chef-alexa-cita.html'));
    const reference=clean(req.query.vendedor,180).toLowerCase();
    let slug=reference;
    if(/^https:\/\//.test(reference)){
      try{
        const link=new URL(reference);
        slug=link.origin===origin?link.pathname.match(/^\/chef\/([a-z0-9-]{1,48})\/?$/)?.[1]:'';
      }catch{slug='';}
    }
    if(validSlug(slug)&&await getBySlug(slug))return res.redirect(303,'/chef/'+slug+'/?origen=alexa');
    const page=await readFile(path.join(privateDir,'chef-alexa-cita.html'),'utf8');
    res.status(400).type('html').send(page.replace('id="seller-error" hidden','id="seller-error"'));
  }));

  app.get(/^\/chef\/(?:([a-z0-9-]{1,48})\/?)?$/i,wrap(async(req,res)=>{
    const slug=req.params[0];
    if(slug&&!await getBySlug(slug.toLowerCase()))return res.status(404).type('html').send('<!doctype html><html lang="es"><meta name="viewport" content="width=device-width,initial-scale=1"><title>TEC Chef</title><body style="font:18px system-ui;max-width:600px;margin:60px auto;padding:24px"><h1>Este Chef no está disponible</h1><p>Pide un enlace actualizado a quien te lo compartió.</p></body></html>');
    res.sendFile(path.join(privateDir,'chef-customer.html'));
  }));
  app.get('/chef',(req,res)=>res.redirect('/chef/'));
  app.get(['/coach/chef','/coach/chef/'],wrap(async(req,res)=>{if(!await requireActive(req,res))return;res.sendFile(path.join(privateDir,'chef-workspace.html'));}));

  app.get('/api/chef/public/bootstrap',wrap(async(req,res)=>{
    const ctx=await context(req,res);if(ctx===undefined)return;if(!await visitor(req,res,true))return;
    res.json({sellerName:ctx?publicName(ctx):'',contactAvailable:Boolean(ctx),canAttachPhoto:true,canSendVoice:Boolean(voice),manifest:chefManifest});
  }));
  app.get('/api/chef/public/conversation',wrap(async(req,res)=>{
    const ctx=await context(req,res);if(ctx===undefined)return;const visitorHash=await visitor(req,res);if(!visitorHash)return;
    if(!validId(req.query.sessionId))return res.status(400).json({error:'Conversación inválida.'});
    const doc=await conversations.findOne({sellerUserId:ctx?.userDoc?._id||null,visitorHash,sessionId:req.query.sessionId}).lean();
    res.json({messages:(doc?.messages||[]).map(m=>({role:m.role,content:m.content,...(m.hasPhoto?{hasPhoto:true}:{}),sources:(m.sources||[]).map(s=>({title:s.title,url:s.url}))}))});
  }));
  app.post('/api/chef/public/chat',wrap(async(req,res)=>{
    if(!sameOrigin(req,res))return;const ctx=await context(req,res);if(ctx===undefined)return;const visitorHash=await visitor(req,res);if(!visitorHash)return;
    const caption=clean(req.body?.pregunta,3000),sessionId=req.body?.sessionId,requestId=req.body?.requestId;
    const hasPhoto=req.body?.photo!==undefined;
    const voiceReply=Boolean(voice)&&req.body?.responseMode==='audio';
    if((!caption&&!hasPhoto)||!validId(sessionId)||!validId(requestId))return res.status(400).json({error:'Escribe una pregunta o adjunta una foto.'});
    const question=caption||'¿Qué platillos puedo preparar con lo que hay en la foto?';
    if(busy.has(visitorHash))return res.status(409).json({error:'Tienes una consulta en proceso. Espera a que termine.'});
    busy.add(visitorHash);
    let photoSlot=false;
    try{
      let photoBytes;
      if(hasPhoto){
        try{photoBytes=decodeChefPhoto(req.body.photo);}catch(error){return res.status(400).json({error:error.message});}
      }
      const scope={sellerUserId:ctx?.userDoc?._id||null,visitorHash,sessionId};
      let doc=await conversations.findOne(scope).lean();
      const photoHash=photoBytes?hash(photoBytes):'';
      const fingerprint=voiceReply
        ? chefRequestFingerprint(JSON.stringify(['audio',question,photoHash]))
        : photoHash ? chefRequestFingerprint(JSON.stringify([caption,photoHash])) : chefRequestFingerprint(question);
      const repeat=()=>{
        if(doc?.lastRequestId!==requestId)return false;
        if(doc.lastFingerprint!==fingerprint)res.status(409).json({error:'Este envío contiene otra pregunta. Revisa antes de continuar.'});
        else res.json({respuesta:doc.lastReply,sources:doc.lastSources||[],appointmentSuggested:chefAppointmentIntent(question)});
        return true;
      };
      if(repeat())return;
      // Reject excess requests before creating another conversation record.
      if(!consume('chat-minute:'+visitorHash,30,60000,res)||!consume('chat-day:'+visitorHash,limits.chatsPerVisitorDay||60,DAY,res)||!consume('chat-ip:'+ipKey(req),limits.chatsPerIpDay||300,DAY,res))return;
      let image;
      if(photoBytes){
        if(activePhotoRequests>=3){res.set('Retry-After','5');return res.status(429).json({error:'El Chef está revisando otras fotos. Reintenta en unos segundos.'});}
        if(!consume('photo-visitor:'+visitorHash,12,DAY,res)||!consume('photo-ip:'+ipKey(req),80,DAY,res))return;
        activePhotoRequests++;photoSlot=true;
        try{image=await chefPhotoContent(photoBytes);}catch(error){return res.status(400).json({error:error.message});}
      }
      if(!doc){
        try{await conversations.updateOne(scope,{$setOnInsert:{...scope,revision:0,messages:[],createdAt:new Date(now()),updatedAt:new Date(now())}},{upsert:true});}catch(error){if(error.code!==11000)throw error;}
        doc=await conversations.findOne(scope).lean();if(!doc)throw Error('conversation_missing');
        if(repeat())return;
      }
      const history=(doc.messages||[]).slice(-8).map(m=>({role:m.role,content:m.content}));
      let reply=image?'':chefDirectReply(question,history),sources=[];
      if(!reply){
        const found=image&&!caption?null:buildChefContext(question,history);
        const guidance=[{role:'system',content:PUBLIC_PROMPT}];
        if(voiceReply)guidance.push({role:'system',content:'La pregunta llegó por audio. Redacta para escuchar: usa oraciones cortas y las cantidades exactas. Conserva todos los pasos e indicaciones de seguridad que requiera el platillo.'});
        if(image)guidance.push({role:'system',content:CHEF_PHOTO_PROMPT});
        if(found){guidance.push({role:'system',content:found.context});sources=found.sources;}
        const content=image?[{type:'text',text:question},image]:question;
        reply=await completion([...guidance,...history,{role:'user',content}]);
      }
      reply=publicReply(reply);
      const saved=await conversations.findOneAndUpdate({...scope,revision:doc.revision||0},{$push:{messages:{$each:[{role:'user',content:question,...(image?{hasPhoto:true}:{})},{role:'assistant',content:reply,sources,...(voiceReply?{voiceRequestId:requestId}:{})}],$slice:-24}},$inc:{revision:1},$set:{updatedAt:new Date(now()),lastRequestId:requestId,lastFingerprint:fingerprint,lastReply:reply,lastSources:sources}},{new:true});
      if(!saved)return res.status(409).json({error:'La conversación cambió en otra ventana. Vuelve a abrirla antes de continuar.'});
      res.json({respuesta:reply,sources,appointmentSuggested:chefAppointmentIntent(question)});
    }finally{if(photoSlot)activePhotoRequests--;busy.delete(visitorHash);}
  }));
  const requireVoiceVisitor = wrap(async (req, res, next) => {
    if (!sameOrigin(req, res, req.path.endsWith('/speech'))) return;
    if (!voice) return res.status(503).json({ error: 'La voz del Chef no está disponible.' });
    const ctx = await context(req, res);
    if (ctx === undefined) return;
    const visitorHash = await visitor(req, res);
    if (!visitorHash) return;
    req.chefVoiceScope = { sellerUserId: ctx?.userDoc?._id || null, visitorHash };
    next();
  });
  async function withVoiceLimit(req, res, kind, run) {
    const { visitorHash } = req.chefVoiceScope;
    if (activeVoice.size >= 4 || activeVoice.has(visitorHash)) {
      return res.status(429).set('Retry-After','5').json({error:'Hay audios en proceso. Intenta en unos segundos.'});
    }
    if (!consume('voice-'+kind+'-visitor:'+visitorHash,limits.voicePerVisitorDay||20,DAY,res) ||
        !consume('voice-'+kind+'-ip:'+ipKey(req),limits.voicePerIpDay||100,DAY,res)) return;
    activeVoice.add(visitorHash);
    try { await run(); }
    catch (error) {
      if (error.status === 400 && !res.headersSent) {
        res.status(400).json({error:error.message});
      } else {
        console.warn('TEC public Chef voice unavailable',{kind,code:error.code || error.name});
        sendVoiceError(res, kind === 'speech'
          ? 'La respuesta escrita está lista. Puedes reintentar el audio del Chef.'
          : 'No pude transcribir el audio del Chef. Tu grabación sigue aquí.');
      }
    } finally { activeVoice.delete(visitorHash); }
  }
  app.post('/api/chef/public/voice/transcribe',requireVoiceVisitor,
    express.raw({type:['audio/*','video/webm'],limit:CHAT_MEDIA_MAX_BYTES,inflate:false}),wrap(async(req,res)=>{
      if (!Buffer.isBuffer(req.body) || req.body.length < 64) {
        return res.status(400).json({error:'Mantén presionado el micrófono un poco más para grabar.'});
      }
      const mime = (req.get('Content-Type') || '').split(';')[0].toLowerCase();
      await withVoiceLimit(req,res,'transcribe',async()=>{
        const pcm = await decodeChatAudioToPcm(req.body,mime);
        const text = await voice.transcribe(pcmToWav([pcm]),'audio/wav',{
          prompt:'Transcribe fielmente la pregunta de cocina para TEC Chef. Respeta los ingredientes, cantidades y marcas: Royal Prestige, Power Blender, Power Blender Max, Power Blender Go y Deluxe Easy Release. Si no hay voz clara, devuelve un texto vacío.'
        });
        if (!text || text.length > 3000) return res.status(422).json({error:'No entendí una pregunta breve. Puedes volver a grabarla.'});
        res.json({text});
      });
    }));
  app.post('/api/chef/public/voice/speech',requireVoiceVisitor,wrap(async(req,res)=>{
    const {sessionId,requestId}=req.body||{};
    if (!validId(sessionId)||!validId(requestId)) return res.status(400).json({error:'Respuesta de audio inválida.'});
    const scope = {...req.chefVoiceScope,sessionId};
    const conversation = await conversations.findOne(scope).lean();
    const reply = conversation?.messages.find(message=>message.role==='assistant'&&message.voiceRequestId===requestId);
    const text = speechText(reply?.content);
    if (!text || text.length>12000) return res.status(404).json({error:'No encontré esa respuesta en esta conversación.'});
    await withVoiceLimit(req,res,'speech',()=>streamVoiceResponse(req,res,voice,text,scope.visitorHash));
  }));
  app.post('/api/chef/public/leads',wrap(async(req,res)=>{
    if(!sameOrigin(req,res))return;
    if(req.body?.origin==='alexa'&&(!validSlug(req.query.chef)||req.body.appointmentRequested!==true)){
      return res.status(400).json({error:'Elige tu distribuidor para solicitar una demostración desde Alexa.'});
    }
    const ctx=await context(req,res);if(ctx===undefined)return;
    if(!ctx)return res.status(400).json({error:'Abre el enlace del Chef que te compartió tu distribuidor para solicitar información.'});
    const visitorHash=await visitor(req,res);if(!visitorHash)return;
    let payload;try{payload=publicChefLeadPayload(ctx,req.body,visitorHash,new Date(now()));}catch(error){return res.status(400).json({error:error.message});}
    const scope={ownerUserId:payload.ownerUserId,offlineRequestId:payload.offlineRequestId};
    let doc=await leads.findOne(scope).lean();
    if(!doc){
      if(!consume('lead:'+visitorHash,10,900000,res)||!consume('lead-ip:'+ipKey(req),limits.leadsPerIpQuarterHour||30,900000,res))return;
      try{doc=await leads.findOneAndUpdate(scope,{$setOnInsert:payload},{new:true,upsert:true});}catch(error){if(error.code!==11000)throw error;doc=await leads.findOne(scope).lean();}
    }
    if(!doc||String(doc.generatedByUserId)!==String(ctx.userDoc._id)||compareFields.some(key=>key==='appointmentRequested'?Boolean(doc[key])!==payload[key]:String(doc[key]||'')!==String(payload[key]||'')))return res.status(409).json({error:'Este envío ya corresponde a otra solicitud. Revisa tus datos antes de continuar.'});
    let crmSynced=true;try{await syncCrm(ctx.userDoc,doc);}catch{crmSynced=false;}
    res.json({ok:true,id:String(doc._id),sellerName:publicName(ctx),crmSynced,appointmentStatus:payload.appointmentRequested?'solicitada':null});
  }));

  app.get('/api/chef/workspace',wrap(async(req,res)=>{
    const auth=await requireActive(req,res);if(!auth)return;if(req.get('X-TEC-Account-ID')&&req.get('X-TEC-Account-ID')!==String(auth.user._id))return res.status(403).json({error:'La cuenta cambió. Recarga la página para continuar.'});const ctx=await getOwnContext(auth.user);
    const canViewTeam=canReadDownline(auth.user)||await hasDownline(auth.user),team=req.query.scope==='team';
    if(team&&!canViewTeam)return res.status(403).json({error:'Tu cuenta solo puede consultar sus propios contactos.'});
    const members=team?await getDownline(auth.user):[],ids=[auth.user._id,...members.map(m=>m._id)],selected=req.query.seller;
    if(selected!=null&&(typeof selected!=='string'||!ids.some(id=>String(id)===selected)))return res.status(403).json({error:'Ese vendedor no está dentro de tu red autorizada.'});
    const docs=await leads.find({source:'chef_personal',generatedByUserId:{$in:selected?[ids.find(id=>String(id)===selected)]:ids}}).select('fullName phone email city interest notes status appointmentRequested appointmentPreferredTime appointmentStatus generatedByUserId generatedByName createdAt').sort({createdAt:-1,_id:-1}).limit(100).lean();
    res.json({user:{id:String(auth.user._id),name:auth.user.name||''},canViewTeam,scope:team?'team':'mine',shareUrl:ctx?origin+'/chef/'+ctx.slug+'/':'',sharingEnabled:Boolean(ctx),leads:docs.map(doc=>({id:String(doc._id),fullName:doc.fullName,phone:doc.phone||'',email:doc.email||'',city:doc.city||'',interest:doc.interest||'',notes:doc.notes||'',status:doc.status||'nuevo',sellerId:String(doc.generatedByUserId),sellerName:doc.generatedByName||'',appointmentRequested:Boolean(doc.appointmentRequested),appointmentPreferredTime:doc.appointmentPreferredTime||'',appointmentStatus:doc.appointmentStatus||'',createdAt:doc.createdAt}))});
  }));
  app.get('/api/chef/workspace/qr.svg',wrap(async(req,res)=>{
    const auth=await requireActive(req,res);if(!auth)return;if(req.query.account&&req.query.account!==String(auth.user._id))return res.status(403).json({error:'La cuenta cambió. Recarga la página.'});const ctx=await getOwnContext(auth.user);
    if(!ctx)return res.status(404).json({error:'Tu Chef no está disponible para compartir en este momento.'});
    const url=origin+'/chef/'+ctx.slug+'/';res.type('image/svg+xml').send(await QRCode.toString(url,{type:'svg',width:280,margin:2,errorCorrectionLevel:'M'}));
  }));
}
