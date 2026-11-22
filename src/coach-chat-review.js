// Revisión de conversaciones del Coach por el administrador de TEC, para encontrar dónde mejorarlo.
// - Coach normal: sus preguntas y respuestas ya viven en la colección Message; aquí se leen y se borran a los 6 meses.
// - Asistente conectado a las cuentas del vendedor: se guarda cifrado cada intercambio durante 30 días,
//   sin teléfonos, direcciones ni correos, tal como lo aceptó el vendedor en Mis conexiones.
// La política de privacidad y el permiso del asistente describen estos plazos: si cambian aquí, cambian allá.
import mongoose from 'mongoose';
import {randomBytes} from 'node:crypto';
import {openToken, sealToken} from './coach-hycite-gmail.js';
import {markerPattern} from './coach-agent-tools.js';

export const CHAT_RETENTION_DAYS = 180;
export const ASSISTANT_RETENTION_DAYS = 30;
const day = 86400000;
const coachIntents = ['coach_chat','coach_help'];
const placeholder = '[Acción en las conexiones del vendedor]';

const turnSchema = new mongoose.Schema({_id:String,owner:{type:String,index:true},content:String,createdAt:{type:Date,index:true},
  expires:{type:Date,expires:0}},{versionKey:false});
const Turns = mongoose.models.CoachAssistantTurn || mongoose.model('CoachAssistantTurn',turnSchema);
const flagSchema = new mongoose.Schema({_id:String,note:String,by:String,createdAt:Date,expires:{type:Date,expires:0}},{versionKey:false});
const Flags = mongoose.models.CoachReviewFlag || mongoose.model('CoachReviewFlag',flagSchema);

const markerLabels = {tel:'[teléfono]',dir:'[dirección]',correo:'[correo]',min:'[minutos]'};
// Quita los datos de contacto de un texto: los marcadores del asistente y los teléfonos o correos escritos a mano.
export function scrubContact(value) {
  return String(value ?? '').replace(markerPattern,(found,kind)=>markerLabels[kind])
    .replace(/[^\s@<>()[\],;:]+@[^\s@<>()[\],;:]+\.[a-z]{2,}/gi,'[correo]')
    .replace(/(?<![\d+])(?:\+?1[\s.-]?)?\(?\d{3}\)?[\s.-]?\d{3}[\s.-]?\d{4}(?!\d)|\+[1-9]\d{7,14}(?!\d)/g,'[teléfono]');
}

// Registro del asistente conectado. `record` nunca interrumpe la respuesta al vendedor.
export function createAssistantLog({key,store=Turns,now=Date.now}) {
  const aad=id=>'coach-review:'+id;
  const open=doc=>{
    try { return {id:doc._id,owner:doc.owner,at:new Date(doc.createdAt),...JSON.parse(openToken(doc.content,key,aad(doc._id)))}; }
    catch { return null; }
  };
  return {
    async record({owner,question,answer,tools=[]}) {
      const id=randomBytes(12).toString('hex');
      const body={question:scrubContact(question).slice(0,2000),answer:scrubContact(answer).slice(0,6000),
        tools:tools.slice(0,24).map(step=>String(step.name).slice(0,40)+':'+String(step.estado).slice(0,24))};
      await store.create({_id:id,owner:String(owner),content:sealToken(JSON.stringify(body),key,aad(id)),
        createdAt:new Date(now()),expires:new Date(now()+ASSISTANT_RETENTION_DAYS*day)});
      return id;
    },
    async list({owner,from,to,limit}) {
      const docs=await store.find({...(owner && {owner:String(owner)}),createdAt:{$gte:from,$lte:to}}).sort({createdAt:-1}).limit(limit).lean();
      return docs.map(open).filter(Boolean);
    },
    async byIds(ids) {
      return (await store.find({_id:{$in:ids}}).lean()).map(open).filter(Boolean);
    },
    async erase(owner) { return (await store.deleteMany({owner:String(owner)})).deletedCount || 0; }
  };
}

const civil = value => typeof value==='string' && /^\d{4}-\d{2}-\d{2}$/.test(value) && Number.isFinite(Date.parse(value)) ? Date.parse(value) : null;
const fold = value => String(value || '').normalize('NFD').replace(/\p{M}/gu,'').toLowerCase();

// Message: modelo de mensajes del chat. Users: modelo de cuentas del Coach. assistant: createAssistantLog.
// Profiles: perfil del vendedor, que guarda su última pregunta y un resumen de las diez más recientes.
export function createChatReview({Message,Users,Profiles,assistant,flags=Flags,now=Date.now}) {
  const noText={lastQuestion:'',lastCoachReply:'',recentSessionsSummary:[]};
  const sellerOf=row=>(row.detectedTopics || []).map(topic=>/^coach_user:([a-f0-9]{24})$/.exec(topic)?.[1]).find(Boolean) || '';

  // Empareja cada pregunta con la primera respuesta de la misma conversación anterior a la pregunta siguiente.
  async function coachEntries(filter,limit) {
    const questions=await Message.find({intent:{$in:coachIntents},role:'user',content:{$ne:placeholder},...filter}).sort({createdAt:-1}).limit(limit).lean();
    if (!questions.length) return [];
    const sessions=[...new Set(questions.map(row=>row.sessionId).filter(Boolean))];
    const first=questions.at(-1).createdAt, last=new Date(new Date(questions[0].createdAt).getTime()+10*60000);
    const answers=await Message.find({intent:{$in:coachIntents},role:'assistant',sessionId:{$in:sessions},createdAt:{$gte:first,$lte:last}}).sort({createdAt:1}).lean();
    const bySession=new Map();
    for (const row of answers) bySession.set(row.sessionId,[...(bySession.get(row.sessionId) || []),row]);
    const asked=new Map();
    for (const row of questions) asked.set(row.sessionId,[...(asked.get(row.sessionId) || []),new Date(row.createdAt).getTime()].sort((a,b)=>a-b));
    return questions.map(row=>{
      const at=new Date(row.createdAt).getTime();
      const next=asked.get(row.sessionId)?.find(time=>time>at) ?? Infinity;
      const reply=(bySession.get(row.sessionId) || []).find(item=>{const t=new Date(item.createdAt).getTime();return t>=at && t<next && t-at<=10*60000;});
      return {id:'c:'+String(row._id),source:'coach',seller:sellerOf(row),at:new Date(row.createdAt),question:String(row.content || ''),
        answer:reply ? String(reply.content || '') : '',tools:[],expires:new Date(at+CHAT_RETENTION_DAYS*day)};
    });
  }
  const assistantEntry=turn=>({id:'a:'+turn.id,source:'asistente',seller:turn.owner,at:turn.at,question:turn.question,answer:turn.answer,
    tools:turn.tools || [],expires:new Date(turn.at.getTime()+ASSISTANT_RETENTION_DAYS*day)});

  async function decorate(entries) {
    const ids=[...new Set(entries.map(entry=>entry.seller).filter(id=>/^[a-f0-9]{24}$/.test(id)))];
    const [people,marks]=await Promise.all([
      ids.length ? Users.find({_id:{$in:ids}},'name email').lean() : [],
      entries.length ? flags.find({_id:{$in:entries.map(entry=>entry.id)}}).lean() : []]);
    const names=new Map(people.map(person=>[String(person._id),{name:String(person.name || '').trim(),email:String(person.email || '')}]));
    const noted=new Map(marks.map(mark=>[mark._id,mark]));
    return entries.map(({expires,...entry})=>({...entry,at:entry.at.toISOString(),sellerName:names.get(entry.seller)?.name || 'Sin nombre',
      sellerEmail:names.get(entry.seller)?.email || '',...(noted.has(entry.id) && {flag:{note:noted.get(entry.id).note || '',at:new Date(noted.get(entry.id).createdAt).toISOString()}})}));
  }

  return {
    // seller: id de la cuenta o vacío. from/to: AAAA-MM-DD. source: '', 'coach' o 'asistente'. Página de 30.
    async list({seller='',from='',to='',search='',source='',flagged=false,page=1}={}) {
      const size=30, window=600;
      const end=civil(to)!=null ? civil(to)+day-1 : now();
      const start=civil(from) ?? end-30*day;
      if (seller && !/^[a-f0-9]{24}$/.test(seller)) return {entries:[],total:0,page:1,pages:1,limited:false};
      const range={createdAt:{$gte:new Date(start),$lte:new Date(end)}};
      let entries;
      if (flagged) {
        const marked=(await flags.find({}).sort({createdAt:-1}).limit(window).lean()).map(mark=>mark._id);
        const coachIds=marked.filter(id=>id.startsWith('c:')).map(id=>id.slice(2)), turnIds=marked.filter(id=>id.startsWith('a:')).map(id=>id.slice(2));
        const [coach,turns]=await Promise.all([coachIds.length ? coachEntries({_id:{$in:coachIds}},window) : [],turnIds.length ? assistant.byIds(turnIds) : []]);
        entries=[...coach,...turns.map(assistantEntry)].filter(entry=>(!seller || entry.seller===seller) && (!source || entry.source===source));
      } else {
        const [coach,turns]=await Promise.all([
          source==='asistente' ? [] : coachEntries({...range,...(seller && {detectedTopics:'coach_user:'+seller})},window),
          source==='coach' ? [] : assistant.list({owner:seller,from:new Date(start),to:new Date(end),limit:window})]);
        entries=[...coach,...turns.map(assistantEntry)];
      }
      const limited=entries.length>=window;
      const wanted=fold(search).trim();
      if (wanted) entries=entries.filter(entry=>fold(entry.question+'\n'+entry.answer).includes(wanted));
      entries.sort((a,b)=>b.at-a.at);
      const pages=Math.max(1,Math.ceil(entries.length/size)), current=Math.min(Math.max(1,Number(page) || 1),pages);
      return {entries:await decorate(entries.slice((current-1)*size,current*size)),total:entries.length,page:current,pages,limited};
    },

    // Vendedores con conversaciones en los últimos 6 meses, para el filtro de la pantalla.
    async sellers() {
      const since=new Date(now()-CHAT_RETENTION_DAYS*day);
      const [topics,owners]=await Promise.all([
        Message.distinct('detectedTopics',{intent:{$in:coachIntents},role:'user',createdAt:{$gte:since}}),
        assistant.list({from:since,to:new Date(now()),limit:2000})]);
      const ids=[...new Set([...topics.map(topic=>/^coach_user:([a-f0-9]{24})$/.exec(topic)?.[1]),...owners.map(turn=>turn.owner)].filter(id=>/^[a-f0-9]{24}$/.test(id || '')))];
      const people=ids.length ? await Users.find({_id:{$in:ids}},'name email').lean() : [];
      return people.map(person=>({id:String(person._id),name:String(person.name || '').trim() || 'Sin nombre',email:String(person.email || '')}))
        .sort((a,b)=>a.name.localeCompare(b.name,'es'));
    },

    // Marca una respuesta como mala, con una nota para la lista de mejoras. La marca caduca con la conversación.
    async flag({id,note='',by}) {
      const coach=/^c:([a-f0-9]{24})$/.exec(id), turn=/^a:([a-f0-9]{24})$/.exec(id);
      if (!coach && !turn) return false;
      const found=coach ? (await coachEntries({_id:coach[1]},1))[0] : (await assistant.byIds([turn[1]])).map(assistantEntry)[0];
      if (!found) return false;
      await flags.updateOne({_id:id},{$set:{note:String(note).replace(/[\p{Cc}\p{Cf}\s]+/gu,' ').trim().slice(0,500),by:String(by),createdAt:new Date(now()),expires:found.expires}},{upsert:true});
      return true;
    },
    async unflag(id) { await flags.deleteOne({_id:String(id)}); return true; },

    // Borra las conversaciones del Coach normal con más de 6 meses. Las del asistente caducan solas a los 30 días.
    // También vacía la última pregunta y el resumen del perfil de quien lleva 6 meses sin usar el Coach.
    async purge() {
      const cutoff=new Date(now()-CHAT_RETENTION_DAYS*day);
      const result=await Message.deleteMany({intent:{$in:coachIntents},createdAt:{$lt:cutoff}});
      if (Profiles) await Profiles.updateMany({lastInteractionAt:{$lt:cutoff},lastQuestion:{$nin:[null,'']}},{$set:noText});
      return result.deletedCount || 0;
    },

    // Solicitud de eliminación de un vendedor: borra sus conversaciones del Coach y del asistente.
    async eraseSeller(seller) {
      if (!/^[a-f0-9]{24}$/.test(String(seller))) return 0;
      // Las respuestas se localizan por la conversación de sus preguntas: no todas llevan la marca del vendedor.
      const sessions=(await Message.distinct('sessionId',{intent:{$in:coachIntents},detectedTopics:'coach_user:'+seller})).filter(Boolean);
      const replies=sessions.length ? await Message.deleteMany({intent:{$in:coachIntents},role:'assistant',sessionId:{$in:sessions}}) : {deletedCount:0};
      const removed=await Message.deleteMany({intent:{$in:coachIntents},detectedTopics:'coach_user:'+seller});
      if (Profiles) await Profiles.updateOne({userId:seller},{$set:noText});
      return (replies.deletedCount || 0)+(removed.deletedCount || 0)+await assistant.erase(seller);
    }
  };
}

// Rutas del administrador. requireAdmin(req,res) responde por sí mismo cuando no hay acceso y devuelve null.
// sameOrigin(req) rechaza peticiones de otros sitios en las rutas que cambian datos.
export function registerChatReview(app,{review,requireAdmin,sameOrigin=()=>true}) {
  const guard=handler=>async(req,res)=>{
    const auth=await requireAdmin(req,res);
    if (!auth) return;
    res.set('Cache-Control','private, no-store');
    try { await handler(req,res,auth); }
    catch (error) {
      console.error('TEC chat review',{name:error?.name});
      res.status(500).json({error:'No pude cargar las conversaciones.'});
    }
  };
  const text=(value,max)=>typeof value==='string' ? value.slice(0,max) : '';
  app.get('/api/control/coach-chats',guard(async(req,res)=>{
    const q=req.query || {};
    const [list,sellers]=await Promise.all([review.list({seller:text(q.vendedor,24),from:text(q.desde,10),to:text(q.hasta,10),search:text(q.buscar,80),
      source:['coach','asistente'].includes(q.origen) ? q.origen : '',flagged:q.marcadas==='1',page:Number(q.pagina) || 1}),review.sellers()]);
    res.json({...list,sellers,retention:{coachDays:CHAT_RETENTION_DAYS,assistantDays:ASSISTANT_RETENTION_DAYS}});
  }));
  app.post('/api/control/coach-chats/marca',guard(async(req,res,auth)=>{
    if (!sameOrigin(req)) return res.status(403).json({error:'Petición no permitida.'});
    const body=req.body || {}, id=text(body.id,40);
    const done=body.quitar===true ? await review.unflag(id) : await review.flag({id,note:text(body.nota,500),by:auth.user._id});
    if (!done) return res.status(404).json({error:'Esa conversación ya no está disponible.'});
    res.json({ok:true});
  }));
}
