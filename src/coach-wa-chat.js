import {createHash} from 'node:crypto';
import {openToken,sealToken} from './coach-hycite-gmail.js';
import {correction,isDraftCorrection} from './coach-mail-followup.js';
import {rewriteSellerBody} from './coach-mail-intent.js';
import {lineProblem,sendProblem,waCloudSeller} from './coach-wa-cloud.js';
import {templateProblem} from './coach-wa-cloud-template.js';
import {createWaDrafts,validWaBody} from './coach-wa-drafts.js';
import {composeWaBody,foldWa,recipientAnswer,waNumber,waRequestParts,whatsAppIntent} from './coach-wa-chat-input.js';

const help='Pídeme «Prepara un WhatsApp de seguimiento para María». También puedes indicar un solo número con prefijo de país y escribir «Mensaje:» seguido de tu texto. Al confirmar podrás abrir el borrador en tu WhatsApp.';
const cloudHelp='Pídeme «Prepara un WhatsApp de seguimiento para María» o indica un solo número con prefijo de país y «Mensaje:» con tu texto. Coach lo enviará desde tu línea de WhatsApp Business solo cuando revises el destinatario y el texto y pulses «Enviar por WhatsApp Business». El texto libre solo se envía si esta persona te escribió en las últimas 24 horas; fuera de ese plazo Coach solo puede ofrecerte la plantilla fija aprobada por Meta, si TEC la configuró.';
// Motivos propios de la integración ventana/plantilla; los demás vienen de los módulos de Meta.
const localReasons={
  windowClosed:'Esta persona ya no está dentro de las 24 horas en que Meta admite texto libre (o Coach no tiene registrado su último mensaje), así que Coach no envió tu texto ni lo cambió por una plantilla.',
  kindChanged:'El tipo de mensaje no coincide con la revisión que confirmaste.'
};
const problem=(reason,fixed)=>localReasons[reason] || (fixed?templateProblem(reason):sendProblem(reason));
const missingLabels={cloud:'la conexión completa de tu línea con Meta',wabaId:'el identificador de la cuenta de WhatsApp Business (WABA)',
  templateName:'el nombre exacto de una plantilla de utilidad aprobada por Meta',language:'el idioma de esa plantilla',base:'la dirección segura de Meta'};
function noTemplate(reason,templates) {
  const problems=reason==='notConfigured' && Array.isArray(templates?.settings?.problems) ? templates.settings.problems.filter(p=>missingLabels[p]) : [];
  const missing=problems.length ? 'Falta que TEC configure en el servidor: '+problems.map(p=>missingLabels[p]).join(', ')+'.' : templateProblem(reason || 'notConfigured');
  return answer('Coach no tiene registrado un mensaje de esta persona en las últimas 24 horas, así que Meta no admite texto libre. '+missing+
    ' Coach no preparó ningún envío. Si necesitas escribirle ahora, hazlo desde tu app de WhatsApp Business.',[{type:'connections',label:'Mis conexiones'}]);
}
const answer=(respuesta,actions=[],pending='')=>({mode:'coach',coachMailAgent:true,coachWhatsAppAgent:true,respuesta,coachHelpActions:actions,coachWhatsAppPending:pending});
const stop=/^(?:no gracias|cancelar?|descartar?|ya no|ol[vb][íi]dalo)[.!]?$/i;
const bodyError='El texto de WhatsApp debe tener entre 1 y 4.096 caracteres. Escribe «Mensaje:» y el texto final.';
const otherTask=/^(?:(?:por favor|coach)[, ]+)*(?:(?:re)?agenda(?:me)?|programa(?:me)?|mueve(?:me)?|revisa(?:me)? (?:mis |el )?(?:correos?|ventas|gmail|calendario)|(?:prepara|envia|redacta)(?:me)? (?:un |el )?(?:correo|email)|(?:confirmar|descartar) cita|confirmar envio|cancelar borrador [a-f0-9]{12}|responde al (?:primero|segundo|tercero))\b/;
const expired='Ese borrador de WhatsApp caducó, cambió o pertenece a otra cuenta o conversación. Vuelve a pedirme el mensaje.';

function receipt(result) {
  if (result.status==='ready') return answer('Borrador confirmado para '+result.to+'. Abre tu WhatsApp, revisa la cuenta activa y pulsa Enviar allí. El Coach no ha enviado el mensaje.',[
    {type:'waOpen',to:result.to,text:result.body,label:'Abrir en mi WhatsApp'}
  ]);
  return answer(result.status==='cancelled'?'Descarté el borrador de WhatsApp.':expired);
}

function cloudReceipt(result,line) {
  const to=result.to?' para '+(result.name?result.name+' · ':'')+result.to:'';
  const fixed=result.kind==='template';
  if (result.status==='sent') return answer('Meta aceptó '+(fixed?'la plantilla aprobada':'tu WhatsApp')+to+' desde tu línea de WhatsApp Business '+line+'.\nID de Meta: '+result.messageId+
    '\nEsto confirma que Meta lo recibió para enviarlo; no confirma la entrega ni la lectura. Revisa la conversación en tu app de WhatsApp Business. Coach no lo reenviará.',
    [{type:'waCloudStatus',state:'accepted',label:'Aceptado por Meta'}]);
  if (result.status==='sending') return answer('Este WhatsApp ya se está enviando. Coach no lo repetirá. Espera unos segundos y vuelve a escribir el mismo comando para ver el resultado.',
    [{type:'waCloudStatus',state:'sending',label:'Enviando…'}]);
  // Resultado dudoso: no se sugiere otro envío; solo comprobar en la app.
  if (result.status==='unknown') return answer('No pude confirmar si Meta aceptó este WhatsApp'+to+'. Para evitar un duplicado, Coach no lo reintentará ni lo enviará de otra forma. Comprueba en la conversación de tu app de WhatsApp Business si salió.',
    [{type:'waCloudStatus',state:'unknown',label:'Resultado sin confirmar'}]);
  if (result.status==='failed') return answer(problem(result.reason,fixed)+' El mensaje no se envió.',[{type:'waCloudStatus',state:'failed',label:'No enviado'}]);
  return answer(result.status==='cancelled'?'Descarté el borrador de WhatsApp.':expired);
}

// isWindowOpen: consulta del webhook auténtico de Meta. Si falta o falla, la ventana se considera
// cerrada y nunca se envía texto libre. templates: createWaCloudTemplate (opcional).
export function createCoachWaAgent({config,findLeads,switched,getSenderPhone,cloud=null,isWindowOpen=null,templates=null,waDrafts=createWaDrafts({key:config.key}),now=Date.now,compose=composeWaBody,rewrite=rewriteSellerBody}) {
  const windowOpen=async to=>{
    if (typeof isWindowOpen!=='function') return false;
    try {return (await isWindowOpen(to))===true;} catch {return false;}
  };
  const notSent=reason=>Object.assign(Error('No enviado.'),{beforeSend:true,reason});
  return async ({req,auth,sessionId,text,literalDraftInput=false})=>{
    const input=String(text||'').trim(), intent=literalDraftInput ? null : whatsAppIntent(input), pending=req.body?.coachWhatsAppPending;
    if (!pending && (req.body?.coachMailPending || req.body?.coachCalendarPending) && isDraftCorrection(input)) return null;
    if (!intent && (!pending || otherTask.test(foldWa(input)))) return null;
    const actor=auth?.user;
    if (!actor?._id || !auth.session?.tokenHash || req.body?.accountId!==String(actor._id) || switched(req)) return answer('Abre el chat desde tu propia cuenta de Coach para preparar un WhatsApp.');
    // Solo el email exacto configurado por el dueño de TEC usa la línea Business del piloto.
    const business=waCloudSeller(cloud?.config,actor);
    if (!business && !config.pilot.includes('*') && !config.pilot.includes(String(actor.email||'').toLowerCase())) return answer('Tu cuenta todavía no participa en el piloto de WhatsApp.');
    const version=req.body?.coachWhatsAppVersion;
    if (req.body?.responseMode==='audio' && version!==1 && version!==2) return answer('Actualiza el Coach para usar WhatsApp desde el chat.');
    if (intent?.kind==='help' && !(pending && isDraftCorrection(input))) return answer(business?cloudHelp:help,[{type:'connections',label:'Mis conexiones'}]);
    // La revisión de la línea Business (botón con destinatario y texto exactos) exige el cliente actual.
    if (business && version!==2 && intent?.kind!=='send') return answer('Actualiza el Coach para usar tu línea de WhatsApp Business. No se preparó ni se envió nada.');
    if (config.key?.length!==32) return answer('El servicio de borradores de WhatsApp todavía no está configurado.');
    const binding={owner:actor._id,session:createHash('sha256').update(auth.session.tokenHash+':'+sessionId).digest('hex'),
      ...(business?{channel:'cloud',grant:cloud.config.grant}:{})};
    const aad='wa-conversation:'+binding.owner+':'+binding.session+(business?':cloud':'');
    const token=state=>sealToken(JSON.stringify({state,until:now()+30*60000}),config.key,aad);
    const ask=(message,state)=>answer(message,[],token(state));
    const line=business?cloud.config.phone:'';
    const fixedPreview=draft=>answer('Plantilla aprobada de WhatsApp Business (texto fijo)\nDe: tu línea '+line+' (WhatsApp Business conectada con Meta)\nPara: '+
      (draft.name?draft.name+' · ':'')+draft.to+'\nPlantilla en Meta: '+draft.template.name+' · idioma '+draft.template.language+'\n\n'+draft.template.body+
      '\n\nAtención: Coach no tiene registrado un mensaje de esta persona en las últimas 24 horas, así que Meta no admite texto libre. Este es el texto fijo aprobado por Meta, distinto de cualquier mensaje que hayas pedido, y no se puede editar. '+
      'Al pulsar «Enviar por WhatsApp Business» o escribir «Enviar WhatsApp Business '+draft.id+'» confirmas que esta persona aceptó recibir mensajes de tu línea y que este texto corresponde a la gestión que te pidió. Si no es así, descártalo. El borrador dura 30 minutos.',
      [{type:'waCloudSend',id:draft.id,to:draft.to,text:draft.template.body,label:'Enviar por WhatsApp Business'},{type:'waCancel',id:draft.id,label:'Descartar WhatsApp'}],token({stage:'preview',id:draft.id}));
    const preview=draft=>business ? (draft.kind==='template' ? fixedPreview(draft) : answer('Borrador de WhatsApp Business\nDe: tu línea '+line+' (WhatsApp Business conectada con Meta)\nPara: '+
      (draft.name?draft.name+' · ':'')+draft.to+'\n\n'+draft.body+'\n\nRevisa el destinatario y el texto. Coach lo enviará solo cuando pulses «Enviar por WhatsApp Business» o escribas «Enviar WhatsApp Business '+draft.id+
      '». Si corriges el texto recibirás un código nuevo y el anterior dejará de valer. Meta solo admite texto libre dentro de las 24 horas desde el último mensaje de esta persona; Coach lo comprueba justo antes de enviarlo y, si ese plazo ya terminó, no lo enviará. El borrador dura 30 minutos.',
      [{type:'waCloudSend',id:draft.id,to:draft.to,text:draft.body,label:'Enviar por WhatsApp Business'},{type:'waCancel',id:draft.id,label:'Descartar WhatsApp'}],token({stage:'preview',id:draft.id})))
      : answer('Borrador de WhatsApp\n'+(draft.senderPhone?'Tu número de referencia: '+draft.senderPhone+'\n':'')+
      'El envío se hará desde la cuenta de WhatsApp activa en tu teléfono o WhatsApp Web.\nPara: '+
      (draft.name?draft.name+' · ':'')+draft.to+'\n\n'+draft.body+'\n\nPuedes corregir el texto. Al confirmar obtendrás un enlace para abrir este borrador en WhatsApp. El envío final lo haces tú allí. El borrador dura 30 minutos.\nTambién puedes escribir «Confirmar borrador WhatsApp '+draft.id+'».',
      [{type:'waPersonalConfirm',id:draft.id,label:'Confirmar borrador WhatsApp'},{type:'waCancel',id:draft.id,label:'Descartar WhatsApp'}],token({stage:'preview',id:draft.id}));
    const shown=result=>business?cloudReceipt(result,line):receipt(result);
    let state;
    if (typeof pending==='string' && pending.length<=60000) {
      try {
        const data=JSON.parse(openToken(pending,config.key,aad));
        if (data.until>now()) state=data.state;
      } catch { /* Other sellers and conversations cannot continue this question. */ }
    }
    if (!intent && !state) return answer('La pregunta de WhatsApp ya caducó. Vuelve a pedirme el borrador.');
    try {
      if (intent?.kind==='send') {
        if (!business) return answer('Tu cuenta no tiene una línea de WhatsApp Business conectada en Coach. Coach no envió nada; prepara el borrador y ábrelo en tu WhatsApp.');
        if (version!==2) return answer('Actualiza el Coach para enviar por WhatsApp Business. No se envió nada.');
        // Por voz no hay diálogo de confirmación de destinatario y texto: solo se envía desde el botón o texto escrito.
        if (req.body?.responseMode==='audio') return answer('Por seguridad, el envío por WhatsApp Business se confirma con el botón o escribiendo el comando. No se envió nada.');
        const current=await waDrafts.view({...binding,id:intent.id});
        if (current.status!=='pending') return shown(current);
        // Meta debe confirmar que el ID configurado sigue siendo exactamente esta línea.
        const verified=await cloud.verify();
        if (!verified.ok) return answer(lineProblem(verified.reason)+' No se envió nada; el borrador sigue disponible.',
          [{type:'waCloudStatus',state:'failed',label:'No enviado'}],token({stage:'preview',id:current.id}));
        const kind=current.kind==='template'?'template':'text';
        return shown(await waDrafts.send({...binding,id:intent.id,deliver:async message=>{
          // El tipo queda fijado por la revisión confirmada: nunca se cambia texto libre por plantilla ni al revés.
          if ((message.kind==='template'?'template':'text')!==kind) throw notSent('kindChanged');
          if (kind==='template') {
            if (!templates) throw notSent('notConfigured');
            // templates.send revalida en Meta línea, aprobación y texto exacto; sin reintentos.
            return templates.send({to:message.to,template:message.template});
          }
          // Revalida la ventana auténtica justo antes de llamar a Meta.
          if (!(await windowOpen(message.to))) throw notSent('windowClosed');
          return cloud.send({to:message.to,body:message.body});
        }}));
      }
      if (intent?.kind==='confirm') {
        if (business) return answer('En tu cuenta los WhatsApp se envían desde tu línea de WhatsApp Business. «Confirmar borrador» no envía nada: revisa el destinatario y el texto y pulsa «Enviar por WhatsApp Business».');
        return receipt(await waDrafts.confirm({...binding,id:intent.id}));
      }
      if (intent?.kind==='cancel' || (!intent && stop.test(input))) {
        const id=intent?.id || state?.id;
        return id ? shown(await waDrafts.cancel({...binding,id})) : answer('Dejé de preparar ese WhatsApp.');
      }
      if (state?.stage==='preview' && isDraftCorrection(input)) {
        const old=await waDrafts.view({...binding,id:state.id});
        if (old.status!=='pending') return shown(old);
        if (old.kind==='template') return ask('Esta es la plantilla aprobada por Meta: su texto es fijo y Coach no puede cambiarlo ni sustituirlo por otro. Envíala tal cual con el botón o descártala. Podrás enviar texto libre cuando esta persona te escriba.',state);
        if (/^(?:para\s*:|(?:cambia|corrige) (?:el )?(?:n[uú]mero|destinatario))/.test(input.toLowerCase())) {
          return ask('Para cambiar el destinatario descarta este borrador y pídeme un WhatsApp nuevo con el número correcto.',state);
        }
        let change=correction({body:old.body},input);
        if (change.freeform) {
          try {change=await rewrite(old.body,input);} catch { /* Keep explicit text replacement available. */ }
        }
        if (!validWaBody(change.body)) return ask(change.question || 'Escribe «Mensaje:» seguido del texto corregido. Para otro destinatario, prepara un WhatsApp nuevo.',state);
        const updated=await waDrafts.revise({...binding,id:state.id,body:change.body});
        return updated.status==='pending'?preview(updated):shown(updated);
      }
      if (state?.stage==='preview' && !intent) {
        // «Sí» o «envíalo» nunca envían: vuelven a mostrar la revisión vigente con su código.
        if (/^(?:si|confirmo|confirma(?:r)?(?: borrador)?|enviar?|mandalo|envialo)[. !]*$/i.test(foldWa(input))) {
          const old=await waDrafts.view({...binding,id:state.id});
          return old.status==='pending'?preview(old):shown(old);
        }
        return null;
      }
      if (intent?.kind==='compose') state=waRequestParts(input);
      else if (state?.stage==='recipient') {
        const next=recipientAnswer(input);
        state={...state,...next,body:next.body || state.body,purpose:next.purpose || state.purpose,
          name:next.name || (next.to && state.keepName ? state.name : '')};
      } else if (state?.stage==='body') {state.body=input.replace(/^(?:mensaje|texto)\s*:\s*/i,'');state.purpose='';}
      if (!state) return answer(business?cloudHelp:help);
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
      if (business && !(await windowOpen(state.to))) {
        // Ventana cerrada o sin registro auténtico: solo la plantilla verificada ahora en Meta, o nada.
        let found;
        try {found=templates?await templates.inspect():{ok:false,reason:'notConfigured'};} catch {found={ok:false,reason:'unreachable'};}
        if (!found?.ok) return noTemplate(found?.reason,templates);
        return preview(await waDrafts.prepare({...binding,to:state.to,name:state.name,kind:'template',template:found.template}));
      }
      const result=await compose(state);
      if (!validWaBody(result.body)) return ask(result.question || bodyError,{...state,stage:'body'});
      let senderPhone='';
      if (getSenderPhone && !business) {
        try {senderPhone=waNumber(await getSenderPhone(binding.owner));} catch { /* The reference is optional. */ }
      }
      return preview(await waDrafts.prepare({...binding,to:state.to,name:state.name,body:result.body,senderPhone}));
    } catch {
      return answer(intent?.kind==='send'?'No pude completar la solicitud de envío. Coach no la repetirá automáticamente; comprueba en tu app de WhatsApp Business si el mensaje salió.':
        'No pude preparar el borrador de WhatsApp. Puedes volver a intentarlo indicando el número internacional y «Mensaje:» con el texto.');
    }
  };
}
