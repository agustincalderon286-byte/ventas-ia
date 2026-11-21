import {createHash} from 'node:crypto';
import {openToken,sealToken} from './coach-hycite-gmail.js';
import {correction,isDraftCorrection} from './coach-mail-followup.js';
import {rewriteSellerBody} from './coach-mail-intent.js';
import {createWaDrafts,validWaBody} from './coach-wa-drafts.js';
import {composeWaBody,foldWa,recipientAnswer,waNumber,waRequestParts,whatsAppIntent} from './coach-wa-chat-input.js';

const help='Pídeme «Prepara un WhatsApp de seguimiento para María». También puedes indicar un solo número con prefijo de país y escribir «Mensaje:» seguido de tu texto. Al confirmar podrás abrir el borrador en tu WhatsApp.';
const answer=(respuesta,actions=[],pending='')=>({mode:'coach',coachMailAgent:true,coachWhatsAppAgent:true,respuesta,coachHelpActions:actions,coachWhatsAppPending:pending});
const stop=/^(?:no gracias|cancelar?|descartar?|ya no|ol[vb][íi]dalo)[.!]?$/i;
const bodyError='El texto de WhatsApp debe tener entre 1 y 4.096 caracteres. Escribe «Mensaje:» y el texto final.';
const otherTask=/^(?:(?:por favor|coach)[, ]+)*(?:(?:re)?agenda(?:me)?|programa(?:me)?|mueve(?:me)?|revisa(?:me)? (?:mis |el )?(?:correos?|ventas|gmail|calendario)|(?:prepara|envia|redacta)(?:me)? (?:un |el )?(?:correo|email)|(?:confirmar|descartar) cita|confirmar envio|cancelar borrador [a-f0-9]{12}|responde al (?:primero|segundo|tercero))\b/;

function receipt(result) {
  if (result.status==='ready') return answer('Borrador confirmado para '+result.to+'. Abre tu WhatsApp, revisa la cuenta activa y pulsa Enviar allí. El Coach no ha enviado el mensaje.',[
    {type:'waOpen',to:result.to,text:result.body,label:'Abrir en mi WhatsApp'}
  ]);
  return answer(result.status==='cancelled'?'Descarté el borrador de WhatsApp.':'Ese borrador de WhatsApp caducó, cambió o pertenece a otra cuenta o conversación. Vuelve a pedirme el mensaje.');
}

export function createCoachWaAgent({config,findLeads,switched,getSenderPhone,waDrafts=createWaDrafts({key:config.key}),now=Date.now,compose=composeWaBody,rewrite=rewriteSellerBody}) {
  return async ({req,auth,sessionId,text})=>{
    const input=String(text||'').trim(), intent=whatsAppIntent(input), pending=req.body?.coachWhatsAppPending;
    if (!pending && (req.body?.coachMailPending || req.body?.coachCalendarPending) && isDraftCorrection(input)) return null;
    if (!intent && (!pending || otherTask.test(foldWa(input)))) return null;
    const actor=auth?.user;
    if (!actor?._id || !auth.session?.tokenHash || req.body?.accountId!==String(actor._id) || switched(req)) return answer('Abre el chat desde tu propia cuenta de Coach para preparar un WhatsApp.');
    if (!config.pilot.includes('*') && !config.pilot.includes(String(actor.email||'').toLowerCase())) return answer('Tu cuenta todavía no participa en el piloto de WhatsApp.');
    if (req.body?.responseMode==='audio' && req.body?.coachWhatsAppVersion!==1) return answer('Actualiza el Coach para usar WhatsApp desde el chat.');
    if (intent?.kind==='help' && !(pending && isDraftCorrection(input))) return answer(help,[{type:'connections',label:'Mis conexiones'}]);
    if (config.key?.length!==32) return answer('El servicio de borradores de WhatsApp todavía no está configurado.');
    const binding={owner:actor._id,session:createHash('sha256').update(auth.session.tokenHash+':'+sessionId).digest('hex')};
    const aad='wa-conversation:'+binding.owner+':'+binding.session;
    const token=state=>sealToken(JSON.stringify({state,until:now()+30*60000}),config.key,aad);
    const ask=(message,state)=>answer(message,[],token(state));
    const preview=draft=>answer('Borrador de WhatsApp\n'+(draft.senderPhone?'Tu número de referencia: '+draft.senderPhone+'\n':'')+
      'El envío se hará desde la cuenta de WhatsApp activa en tu teléfono o WhatsApp Web.\nPara: '+
      (draft.name?draft.name+' · ':'')+draft.to+'\n\n'+draft.body+'\n\nPuedes corregir el texto. Al confirmar obtendrás un enlace para abrir este borrador en WhatsApp. El envío final lo haces tú allí. El borrador dura 30 minutos.\nTambién puedes escribir «Confirmar borrador WhatsApp '+draft.id+'».',
      [{type:'waPersonalConfirm',id:draft.id,label:'Confirmar borrador WhatsApp'},{type:'waCancel',id:draft.id,label:'Descartar WhatsApp'}],token({stage:'preview',id:draft.id}));
    let state;
    if (typeof pending==='string' && pending.length<=60000) {
      try {
        const data=JSON.parse(openToken(pending,config.key,aad));
        if (data.until>now()) state=data.state;
      } catch { /* Other sellers and conversations cannot continue this question. */ }
    }
    if (!intent && !state) return answer('La pregunta de WhatsApp ya caducó. Vuelve a pedirme el borrador.');
    try {
      if (intent?.kind==='confirm') return receipt(await waDrafts.confirm({...binding,id:intent.id}));
      if (intent?.kind==='cancel' || (!intent && stop.test(input))) {
        const id=intent?.id || state?.id;
        return id ? receipt(await waDrafts.cancel({...binding,id})) : answer('Dejé de preparar ese WhatsApp.');
      }
      if (state?.stage==='preview' && isDraftCorrection(input)) {
        const old=await waDrafts.view({...binding,id:state.id});
        if (old.status!=='pending') return receipt(old);
        if (/^(?:para\s*:|(?:cambia|corrige) (?:el )?(?:n[uú]mero|destinatario))/.test(input.toLowerCase())) {
          return ask('Para cambiar el destinatario descarta este borrador y pídeme un WhatsApp nuevo con el número correcto.',state);
        }
        let change=correction({body:old.body},input);
        if (change.freeform) {
          try {change=await rewrite(old.body,input);} catch { /* Keep explicit text replacement available. */ }
        }
        if (!validWaBody(change.body)) return ask(change.question || 'Escribe «Mensaje:» seguido del texto corregido. Para otro destinatario, prepara un WhatsApp nuevo.',state);
        const updated=await waDrafts.revise({...binding,id:state.id,body:change.body});
        return updated.status==='pending'?preview(updated):receipt(updated);
      }
      if (state?.stage==='preview' && !intent) {
        if (/^(?:si|confirmo|confirma(?:r)?(?: borrador)?|enviar?|mandalo|envialo)[. !]*$/i.test(foldWa(input))) {
          const old=await waDrafts.view({...binding,id:state.id});
          return old.status==='pending'?preview(old):receipt(old);
        }
        return null;
      }
      if (intent?.kind==='compose') state=waRequestParts(input);
      else if (state?.stage==='recipient') {
        const next=recipientAnswer(input);
        state={...state,...next,body:next.body || state.body,purpose:next.purpose || state.purpose,
          name:next.name || (next.to && state.keepName ? state.name : '')};
      } else if (state?.stage==='body') {state.body=input.replace(/^(?:mensaje|texto)\s*:\s*/i,'');state.purpose='';}
      if (!state) return answer(help);
      if (state.ambiguous || (!state.to && !state.name && state.mentionsPhone)) {
        return ask('Elige un solo destinatario. Escribe su número completo con «+» y el prefijo del país.',{...state,stage:'recipient',to:'',name:'',ambiguous:false,mentionsPhone:false});
      }
      if (!state.to) {
        const id=req.body?.activeCrmRecordId;
        let rows=[];
        if (findLeads && state.name) rows=await findLeads({owner:binding.owner,name:state.name,includePhone:true});
        else if (findLeads && !state.name && /^[0-9a-f]{24}$/i.test(id||'') && state.stage!=='recipient') rows=await findLeads({owner:binding.owner,id,includePhone:true});
        if (rows.length===1) {
          state.name=rows[0].name;
          state.to=waNumber(rows[0].phone);
        }
        if (!state.to) {
          const question=rows.length>1?'Hay varios prospectos con ese nombre: '+rows.map(r=>r.name).join(', ')+'. Escribe el nombre completo o un solo número con prefijo del país.':
            state.name?'Necesito el número internacional de '+state.name+'. Escríbelo con «+» y el prefijo del país.':'¿Para qué prospecto de tu CRM o número de WhatsApp preparo el mensaje? Incluye el prefijo del país si escribes un número.';
          return ask(question,{...state,stage:'recipient',keepName:rows.length===1});
        }
      }
      const result=await compose(state);
      if (!validWaBody(result.body)) return ask(result.question || bodyError,{...state,stage:'body'});
      let senderPhone='';
      if (getSenderPhone) {
        try {senderPhone=waNumber(await getSenderPhone(binding.owner));} catch { /* The reference is optional. */ }
      }
      return preview(await waDrafts.prepare({...binding,to:state.to,name:state.name,body:result.body,senderPhone}));
    } catch {
      return answer('No pude preparar el borrador de WhatsApp. Puedes volver a intentarlo indicando el número internacional y «Mensaje:» con el texto.');
    }
  };
}
