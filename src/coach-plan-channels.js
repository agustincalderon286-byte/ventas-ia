import {randomBytes} from 'node:crypto';
import {readFollowup, followupTime} from './coach-followup-input.js';
import {availableSlots, readSlotHours, slotBounds} from './coach-calendar-slots.js';
import {composeMailDraft, rewriteSellerBody} from './coach-mail-intent.js';
import {isGmailAddress, openToken} from './coach-hycite-gmail.js';

const fmt=(date,zone)=>new Intl.DateTimeFormat('es-US',{dateStyle:'full',timeStyle:'short',timeZone:zone}).format(new Date(date));
const fold=value=>String(value||'').normalize('NFD').replace(/\p{M}/gu,'').toLowerCase().trim();
const problem=message=>Object.assign(new Error(message),{publicMessage:message});

// A lower bound explicitly given by the seller extends to the end of that civil day.
// Leave other ranges to the same parser used by the established Calendar agent.
function planSlotHours(schedule,state,now) {
  const text=fold(schedule);
  const meridian=/^(?:(?:por|de|en) la )?(am|pm|manana|tarde|noche)[.!]?$/.exec(text);
  let term, clock;
  if (state.openBoundary && meridian) {
    term=state.openBoundary.term;
    clock={hour:state.openBoundary.hour%12+(/^(pm|tarde|noche)$/.test(meridian[1])?12:0),minute:state.openBoundary.minute};
  } else {
    const bounds=[...text.matchAll(/\b(despues de|a partir de|antes de|desde|hasta|entre)\b/g)];
    if (bounds.length!==1 || !['despues de','a partir de'].includes(bounds[0][1])) return readSlotHours(schedule,state.slotHours);
    term=bounds[0][1];
    const start=text.slice(bounds[0].index+term.length).trim().replace(/^(?:las?|el)\s+/,'');
    if (!/^(?:\d{1,2}(?=[:\s.,!?]|[ap]\.?m\b|$)|(?:cero|un[ao]?|dos|tres|cuatro|cinco|seis|siete|ocho|nueve|diez|once|doce|mediodia|medianoche)\b)/i.test(start) ||
      /\b(?:excepto|salvo|menos|pero|o a las?|o las?)\b/.test(start)) return readSlotHours(schedule,state.slotHours);
    const parsed=readFollowup('a las '+start,{kind:'create'},{timeZone:state.timeZone,now});
    if (!parsed.state.clock || parsed.question) return {question:parsed.question || '¿A partir de qué hora te sirve?'};
    clock=parsed.state.clock;
    if (clock.ambiguous) {
      state.openBoundary={term,hour:clock.ambiguous,minute:clock.minute};
      return {question:'¿Después de las '+clock.ambiguous+' de la mañana o de la tarde?'};
    }
  }
  const from=clock.hour*60+clock.minute+(term==='despues de'?1:0);
  if (from>=1440) return {question:'Ese día termina a esa hora. ¿Qué otro día quieres revisar?'};
  return {hours:[from,1440]};
}

function aboutAppointmentOnly(instruction) {
  const text=fold(instruction).replace(/[^\p{L}\s]/gu,' ').replace(/\s+/g,' ').trim();
  return /^(?:(?:para|de|que|solo|un correo para|el correo para) )?(?:confirm(?:ar|arle|ando)|propon(?:er|erle|iendo)|acordar|coordinar|recordar(?:le)?) (?:el |la |nuestra |nuestro |esa |ese |su )?(?:cita|reunion|demo|visita|llamada|horario|hora)(?: (?:de la |de nuestra )(?:cita|reunion|demo|visita|llamada))?$/.test(text);
}

export function createPlanChannels({config,calendar,followups,store,gmail,drafts,compose=composeMailDraft,rewrite=rewriteSellerBody,now=Date.now}) {
  function personalPilot(actor) {
    if (!config.pilot?.some(x=>x==='*' || x===String(actor.email).toLowerCase())) throw problem('Esta cuenta aún no participa en el piloto de Gmail y Calendar.');
  }
  const mailDoc=owner=>store.findById(owner,'mailbox encryptedToken grant sendEnabled').lean();
  return {
    async appointment(actor, state, {name,schedule,slots,slot}) {
      personalPilot(actor);
      const cal=await calendar.getConnection(actor.owner);
      if (!cal?.grant || !cal.availabilityEnabled) throw problem('Conecta Calendar y activa los horarios libres en Mis conexiones.');
      const clarifiesBound=state?.openBoundary && /^(?:(?:por|de|en) la )?(am|pm|manana|tarde|noche)[.!]?$/.test(fold(schedule));
      const search=(slots || clarifiesBound) && !slot;
      const parsed=readFollowup(schedule,{...state,kind:search?'availability':'create'},{timeZone:actor.zone,now:now()});
      const next=parsed.state;
      if (clarifiesBound && state.day!=null) next.day=state.day;
      if (parsed.question) return {state:next,question:parsed.question};
      const busy=range=>calendar.availability({owner:actor.owner,grant:cal.grant,...range});
      if (search) {
        const hours=planSlotHours(schedule,next,now());
        if (hours.question) return {state:next,question:hours.question};
        delete next.openBoundary;
        next.slotHours=hours.hours;
        const range=slotBounds(next);
        if (range.question) return {state:next,question:range.question};
        const options=availableSlots(range,await busy(range),next.durationMinutes??30,now());
        return {state:next,options,labels:options.map(s=>fmt(s.start,next.timeZone)),question:options.length?'Elige un horario para revisar la cita.':'Ese intervalo está ocupado. ¿Qué otro día u horario te serviría?'};
      }
      delete next.openBoundary;
      const when=slot || followupTime(next,now());
      if (when.question) return {state:next,question:when.question};
      if (Date.parse(when.start)<=now() || (await busy(when)).length) return {state:next,question:'Ese horario ya no está libre. ¿Qué otro horario te sirve?'};
      const action=next.action || 'seguimiento';
      const summary=({llamar:'Llamada',demo:'Demo',cita:'Cita',seguimiento:'Seguimiento'}[action] || 'Seguimiento')+' · '+(name || 'personal');
      const reminder=Object.hasOwn(next,'reminderMinutes')?next.reminderMinutes:15;
      const binding={owner:actor.owner,session:actor.session,grant:cal.grant,calendarId:cal.calendarId};
      const draft=await followups.prepare(binding,{...when,mode:'create',action,summary,name:name||'personal',timeZone:next.timeZone,
        reminderMinutes:reminder,checkAvailability:true,availabilitySearchId:randomBytes(16).toString('hex'),leadId:''});
      if (!draft) throw problem('La cita cambió. Pide una vista previa nueva.');
      const whenText=fmt(when.start,next.timeZone)+' ('+next.timeZone+')';
      return {state:next,proposal:{id:draft.id,grant:cal.grant,calendarId:cal.calendarId},whenText,
        preview:summary+' · '+whenText+', '+(Date.parse(when.end)-Date.parse(when.start))/60000+' min. Seguimientos TEC · '+cal.mailbox+'. Sin invitados. '+(reminder==null?'Sin recordatorio.':'Aviso '+reminder+' min antes.')};
    },
    async email(actor,{to,instruction,whenText='',optionLabels,priorDraft,editInstruction=''}) {
      personalPilot(actor);
      if (!isGmailAddress(to)) return {question:'¿A qué dirección de correo quieres escribirle?'};
      const alternatives=optionLabels?.length>0;
      const timed=alternatives || Boolean(whenText);
      if (!instruction && !priorDraft && !timed) return {question:'¿Qué te gustaría decirle en el correo?'};
      const doc=await mailDoc(actor.owner);
      if (!doc?.grant || !doc.mailbox) throw problem('Conecta Gmail desde tu propia cuenta en Mis conexiones.');
      // A concrete slot comes from the seller's Calendar. Append it locally, never as model input.
      const timingInstruction='El Coach adjuntará el horario propuesto al final. Omite la fecha y hora en tu redacción. La persona todavía debe confirmar.';
      let msg;
      if (priorDraft) {
        const edit=editInstruction ? await rewrite(priorDraft.body,editInstruction+(timed?'\n'+timingInstruction:'')) : {body:priorDraft.body};
        if (edit.question) return {question:edit.question};
        msg={to,subject:priorDraft.subject,body:edit.body};
      } else if (timed && (!instruction || aboutAppointmentOnly(instruction) || /^(?:y )?(?:d[eé]ja\w*|prepara\w*) (?:listo )?(?:el |un )?(?:correo|borrador)[.!]?$/i.test(instruction))) {
        msg={to,subject:'Propuesta de horario',body:'Hola,\n\nMe gustaría coordinar contigo la próxima visita.'};
      } else {
        msg=await compose((timed?timingInstruction+'\n':'')+'Redacta un correo a '+to+' para decirle: '+instruction);
      }
      if (msg.question) return {question:msg.question};
      const baseDraft={subject:msg.subject,body:msg.body};
      if (timed) {
        const baseText=fold(msg.subject+' '+msg.body);
        const claimsBooked=/\b(?:ya (?:esta|quedo|te|he|la he)|queda|quedo|esta|hemos|te (?:lo |la )?confirmo)\b.{0,30}\b(?:reservad[ao]|agendad[ao]|confirmad[ao]|cita|visita|reserva|reunion)\b|\b(?:cita|visita|reserva|reunion) (?:ha sido |esta |queda |ya )?(?:confirmada|reservada|agendada)\b/;
        const addsAnotherTime=/\b(?:a las? (?:\d{1,2}|un[ao]?|dos|tres|cuatro|cinco|seis|siete|ocho|nueve|diez|once|doce)|\d{1,2}(?::\d\d|\s*[ap]\.?\s*m\.?))\b/;
        if (claimsBooked.test(baseText) || addsAnotherTime.test(baseText)) {
          return {question:'¿Qué texto quieres acompañar al horario propuesto? La cita aún espera tu confirmación.'};
        }
        msg.body=alternatives ? msg.body.trim()+'\n\nOpciones de horario ('+actor.zone+'):\n'+optionLabels.join('\n')+'\n¿Cuál te funciona?' :
          msg.body.trim()+'\n\nHorario propuesto: '+whenText+'. ¿Te funciona?';
      }
      if (msg.to?.toLowerCase()!==to.toLowerCase()) throw problem('Confirma una sola dirección para el correo.');
      const draft=await drafts.prepare({owner:actor.owner,session:actor.session,grant:doc.grant,...msg});
      return {proposal:{id:draft.id,grant:doc.grant},baseDraft,preview:'Borrador en el Coach, desde '+doc.mailbox+'\nPara: '+to+'\nAsunto: '+msg.subject+'\n'+msg.body};
    },
    async confirmAppointment(actor,p) {
      personalPilot(actor);
      const cal=await calendar.getConnection(actor.owner);
      if (cal?.grant!==p.grant || cal.calendarId!==p.calendarId) throw problem('La conexión de Calendar cambió. Revisa una propuesta nueva.');
      const result=await followups.confirm({owner:actor.owner,session:actor.session,grant:p.grant,calendarId:p.calendarId},p.id,
        {saveEvent: async event=>{
          if (!await actor.current()) throw problem('La sesión cambió. Abre de nuevo tu Coach antes de confirmar.');
          return calendar.saveEvent(event);
        },syncLead:async()=>{}});
      if (result.status==='saved') return {status:'saved',message:'Google Calendar confirmó '+result.summary+' · '+fmt(result.start,result.timeZone)+'.'};
      return {status:['slotBusy','expired','unavailable','changed'].includes(result.status)?'blocked':'retry',message:result.status==='slotBusy'?'El horario se ocupó.':'Calendar todavía no confirmó esa cita.'};
    },
    async confirmEmail(actor,p) {
      personalPilot(actor);
      const doc=await mailDoc(actor.owner);
      if (!doc?.sendEnabled || doc.grant!==p.grant) throw problem('Comprueba en Mis conexiones el permiso de envío de tu Gmail.');
      const result=await drafts.confirm({owner:actor.owner,session:actor.session,grant:p.grant,id:p.id,deliver:async msg=>{
        let token, fresh;
        try {
          token=(await gmail.refresh(openToken(doc.encryptedToken,config.key,actor.owner))).access_token;
          fresh=await mailDoc(actor.owner);
          if (!token || !fresh?.sendEnabled || fresh.grant!==p.grant || !await actor.current()) throw Error();
        } catch {throw Object.assign(Error('El permiso cambió.'),{beforeSend:true});}
        return gmail.send(token,{...msg,from:fresh.mailbox});
      }});
      if (result.status==='sent') return {status:'saved',message:'Gmail confirmó el envío.'};
      return {status:['failed','cancelled','unavailable'].includes(result.status)?'blocked':'retry',message:'Gmail no ha confirmado el envío. Comprueba Enviados antes de preparar otra copia.'};
    }
  };
}
