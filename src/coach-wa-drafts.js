import {createHash, randomBytes} from 'node:crypto';
import mongoose from 'mongoose';
import {openToken, sealToken} from './coach-hycite-gmail.js';
import {validTemplateBody} from './coach-wa-cloud-template.js';

const ttl = 30 * 60_000;
const resultTtl = 24 * 60 * 60_000;
const sendingLimit = 2 * 60_000;
const schema = new mongoose.Schema({
  _id:String, code:{type:String,required:true,unique:true}, owner:{type:String,required:true,index:true},
  sessionKey:String, state:String, content:String, expiresAt:{type:Date,expires:0},
  channel:String, grantKey:String, claimedAt:Date, messageId:String, reason:String,
  // Solo para redactar el resultado; lo que se envía sale siempre del sobre cifrado.
  kind:String
}, {versionKey:false,writeConcern:{w:'majority'}});
const Model = mongoose.models.CoachPersonalWaDraft || mongoose.model('CoachPersonalWaDraft',schema);
const code = () => randomBytes(6).toString('hex').toUpperCase();
const unavailable = {status:'unavailable'};
const sha = value => createHash('sha256').update(value).digest('hex');

export function validWaBody(body) {
  return typeof body==='string' && Boolean(body.trim()) && body.length<=4096 &&
    !/[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]/.test(body);
}

// Plantilla verificada en Meta: nombre, idioma y texto exactos tal como se mostraron.
export function validWaTemplate(t) {
  return Boolean(t) && typeof t==='object' && typeof t.name==='string' && /^[a-z0-9_]{1,512}$/.test(t.name) &&
    typeof t.language==='string' && /^[a-z]{2,3}(?:_[A-Z]{2}|_\d{3})?$/.test(t.language) && validTemplateBody(t.body);
}
const isTemplate = message => message?.kind==='template';

// channel 'cloud' = línea de WhatsApp Business del piloto (envío por Meta).
// Cualquier otro valor = borrador personal que solo se abre en la app del vendedor.
export function createWaDrafts({key,store=Model,now=Date.now}) {
  const cloud = input => input.channel==='cloud';
  const line = input => {
    if (typeof input.grant!=='string' || !input.grant) throw Error('Falta la línea de WhatsApp Business.');
    return {channel:'cloud',grantKey:sha('coach-wa-cloud-grant:'+input.grant)};
  };
  const base = ({owner,session}) => ({owner:String(owner),sessionKey:sha(session)});
  const scope = input => ({...base(input),...(cloud(input)?line(input):{channel:{$ne:'cloud'}})});
  const aad = d => d.channel==='cloud' ? JSON.stringify(['cloud-whatsapp',d._id,d.owner,d.sessionKey,d.grantKey])
    : JSON.stringify(['personal-whatsapp',d._id,d.owner,d.sessionKey]);
  const seal = (d,message) => sealToken(JSON.stringify(message),key,aad(d));
  const open = d => JSON.parse(openToken(d.content,key,aad(d)));
  const find = input => typeof input.id==='string' && /^[a-f0-9]{12}$/i.test(input.id)
    ? store.findOne({...scope(input),code:input.id.toUpperCase(),expiresAt:{$gt:new Date(now())}}).lean() : null;
  const awaiting = d => ({_id:d._id,code:d.code,state:'pending',expiresAt:{$gt:new Date(now())}});
  function result(d) {
    if (!d) return unavailable;
    if (d.state==='cancelled') return {status:'cancelled'};
    const kind = d.kind==='template' ? {kind:'template'} : {};
    if (d.state==='sending') return {status:now()-new Date(d.claimedAt).getTime()>=sendingLimit?'unknown':'sending',id:d.code,...kind};
    if (d.state==='unknown') return {status:'unknown',id:d.code,...kind};
    if (d.state==='sent') return {status:'sent',id:d.code,messageId:d.messageId,...kind};
    if (d.state==='failed') return {status:'failed',id:d.code,reason:d.reason,...kind};
    return {status:d.state,id:d.code,...open(d)};
  }
  return {
    async prepare(input) {
      const fixed=isTemplate(input);
      if (!/^\+[1-9]\d{6,14}$/.test(input.to) || (fixed ? !cloud(input) || !validWaTemplate(input.template) : !validWaBody(input.body))) throw Error('Número o mensaje no válido.');
      const d={...base(input),...(cloud(input)?line(input):{}),...(fixed?{kind:'template'}:{}),_id:randomBytes(16).toString('hex'),code:code(),state:'pending',expiresAt:new Date(now()+ttl)};
      // Una plantilla guarda solo su nombre, idioma y texto exactos; nunca un texto libre que pudiera enviarse en su lugar.
      const fields=fixed
        ? {to:input.to,name:String(input.name||'').slice(0,150),kind:'template',
          template:{name:input.template.name,language:input.template.language,body:input.template.body}}
        : {to:input.to,name:String(input.name||'').slice(0,150),body:input.body};
      if (!cloud(input) && typeof input.senderPhone==='string' && /^\+[1-9]\d{6,14}$/.test(input.senderPhone)) fields.senderPhone=input.senderPhone;
      await store.create({...d,content:seal(d,fields)});
      return {status:'pending',id:d.code,...fields};
    },
    async view(input) {return result(await find(input));},
    async revise(input) {
      if (!validWaBody(input.body)) throw Error('Mensaje no válido.');
      const d=await find(input);
      if (d?.state!=='pending') return result(d);
      const current=open(d);
      // El texto de una plantilla aprobada es fijo: no se edita ni se sustituye por texto libre.
      if (isTemplate(current)) throw Object.assign(Error('La plantilla aprobada no se puede editar.'),{fixedTemplate:true});
      const message={...current,body:input.body};
      // Cada corrección emite un código nuevo: el botón o comando anterior deja de valer.
      const next=code();
      const changed=await store.updateOne(awaiting(d),{$set:{code:next,content:seal(d,message),expiresAt:new Date(now()+ttl)}});
      return changed.matchedCount ? {status:'pending',id:next,...message} : result(await find(input));
    },
    async cancel(input) {
      const d=await find(input);
      if (d?.state!=='pending') return result(d);
      const changed=await store.updateOne(awaiting(d),{$set:{state:'cancelled'},$unset:{content:1}});
      return changed.matchedCount ? {status:'cancelled'} : result(await find(input));
    },
    // Solo prepara el enlace personal. Nunca encuentra ni envía un borrador de la línea Business.
    async confirm(input) {
      const personal={...input,channel:undefined};
      const d=await find(personal);
      if (d?.state!=='pending') return result(d);
      // Correction and confirmation contend for the displayed revision. An old
      // button cannot produce a link containing a different revision or number.
      const changed=await store.updateOne(awaiting(d),{$set:{state:'ready'}});
      return changed.matchedCount ? {status:'ready',id:d.code,...open(d)} : result(await find(personal));
    },
    // Envío por la línea Business: una sola reclamación atómica de la revisión mostrada.
    // Un resultado indeterminado queda como 'unknown' y nunca se reintenta.
    async send(input) {
      if (!cloud(input) || typeof input.deliver!=='function') throw Error('Falta la línea de WhatsApp Business.');
      const d=await find(input);
      if (d?.state!=='pending') return result(d);
      const time=now();
      const claimed=await store.findOneAndUpdate(awaiting(d),
        {$set:{state:'sending',claimedAt:new Date(time),expiresAt:new Date(time+resultTtl)}},{new:false}).lean();
      if (!claimed) return result(await find(input));
      let status='failed', messageId, reason='rejected', message;
      try {
        message=open(claimed);
        status='unknown'; reason=undefined;
        // Se entrega exactamente el tipo guardado en la revisión confirmada; nunca se convierte en el otro.
        const receipt=await input.deliver(isTemplate(message)
          ? {to:message.to,kind:'template',template:message.template}
          : {to:message.to,kind:'text',body:message.body});
        if (typeof receipt?.id==='string' && receipt.id) {status='sent';messageId=receipt.id;}
      } catch (error) {
        if (error?.beforeSend===true || error?.providerRejected===true) {status='failed';reason=String(error.reason||'rejected');}
      }
      const $set={state:status,...(messageId?{messageId}:{}),...(reason?{reason}:{})};
      await store.updateOne({_id:claimed._id,state:'sending'},{$set,$unset:{content:1}}).catch(()=>{});
      return {status,id:claimed.code,to:message?.to,name:message?.name,...(isTemplate(message)||claimed.kind==='template'?{kind:'template'}:{}),
        ...(messageId?{messageId}:{}),...(reason?{reason}:{})};
    }
  };
}
