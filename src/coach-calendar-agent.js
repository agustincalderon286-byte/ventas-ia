import {createHash,randomBytes} from 'node:crypto';
import {openToken,sealToken} from './coach-hycite-gmail.js';
import {calendarIntent,followupTime,readFollowup,followupWindow} from './coach-followup-input.js';
import {readSlotHours,slotBounds,slotChoice,availableSlots} from './coach-calendar-slots.js';
import {createFollowupStore} from './coach-followup-store.js';

const connections = {type:'connections',label:'Mis conexiones'};
const reply = (respuesta,actions=[],pending='') => ({
  mode:'coach',coachMailAgent:true,coachCalendarAgent:true,respuesta,coachHelpActions:actions,coachCalendarPending:pending
});
const help = 'Puedo buscar dos horarios libres para una demo, agendar o mover un seguimiento y mostrarte tus próximos seguimientos de TEC. Prueba «Agenda una llamada con María mañana a las 6 pm» o «Muestra mis seguimientos».';
const actionLabels = {llamar:'Llamada',demo:'Demo',cita:'Cita',seguimiento:'Seguimiento'};
const format = (date,zone) => new Intl.DateTimeFormat('es-US',{timeZone:zone,weekday:'long',year:'numeric',month:'long',day:'numeric',hour:'numeric',minute:'2-digit',hour12:true}).format(new Date(date))+' ('+zone+')';
const confirmAction = id => ({type:'calendarConfirm',id,label:'Confirmar cita'});
const cancelAction = id => ({type:'calendarCancel',id,label:'Descartar cita'});
const entry = (item,index) => (index+1)+'. '+item.summary+' · '+format(item.start,item.timeZone)+' · Ref. '+item.reference;
const calendarLink = raw => {
  try {
    const url=new URL(raw);
    if (url.protocol==='https:' && !url.username && !url.password && !url.port && ['www.google.com','calendar.google.com'].includes(url.host) && url.pathname.startsWith('/calendar/')) return url.href;
  } catch {}
  return '';
};

function showReceipt(result,id) {
  if (result.status==='saved') {
    let message='Google Calendar confirmó «'+result.summary+'» para el '+format(result.start,result.timeZone)+'.';
    if (result.crm==='updated') message+=' También actualicé la fecha de seguimiento del prospecto en TEC.';
    if (result.crm==='conflict') message+=' La ficha de TEC cambió después de la vista previa. Conservé la fecha que tiene ahora la ficha.';
    const link=calendarLink(result.htmlLink);
    return reply(message+' Referencia de la cita: '+result.reference+'.',link?[{type:'calendarEvent',label:'Abrir en Google Calendar',url:link}]:[]);
  }
  if (result.status==='crmPending') return reply('La cita ya está en Google Calendar. Vuelve a confirmar para terminar de actualizar la ficha de TEC.',[confirmAction(id)]);
  if (result.status==='retry') return reply('Todavía no tengo confirmación de Google Calendar. Reintenta con el mismo botón para verificar esta cita.',[confirmAction(id)]);
  if (result.status==='reconnect') return reply('Google solicita renovar la conexión de Calendar. Abre Mis conexiones y vuelve a revisar la cita.',[connections]);
  if (result.status==='slotBusy') return reply('Ese horario ya no está disponible. Pídeme «Busca dos horarios libres para la demo» para revisar otras opciones.');
  if (result.status==='changed') return reply('Google no aceptó el cambio. Revisa la cita en tu calendario antes de preparar otra propuesta.',[connections]);
  if (result.status==='working') return reply('Estoy confirmando esta cita. Vuelve a comprobarla con el mismo botón.',[confirmAction(id)]);
  return reply('La propuesta caducó, cambió o pertenece a otra conversación. Vuelve a pedirme el seguimiento.');
}

export function createCoachCalendarAgent({config,calendar,switched,findLeads,syncLead,followups=createFollowupStore({key:config.key}),now=Date.now}) {
  return async ({req,auth,sessionId,text}) => {
    const requested=calendarIntent(text);
    const pending=req.body?.coachCalendarPending;
    const slotCommand=/^Elegir horario ([12]) ([a-f0-9]{32})$/i.exec(String(text || '').trim());
    if (!requested && !slotCommand && (!pending || /\b(?:correos?|gmail|borrador|ventas|hy cite|enviar|responde)\b/i.test(text))) return null;
    if (req.body?.responseMode==='audio' && req.body?.coachMailVersion!==1) return reply('Actualiza el Coach para usar Calendar desde el chat.');
    const user=auth?.user;
    if (!user?._id || !auth.session?.tokenHash || req.body?.accountId!==String(user._id) || switched(req)) return reply('Abre tu propia cuenta del Coach para consultar o agendar seguimientos.');
    if (!config.pilot.includes('*') && !config.pilot.includes(String(user.email||'').toLowerCase())) return reply('Tu cuenta todavía no participa en el piloto de Google Calendar.');
    if (slotCommand && !pending) return reply('Esa lista ya cambió. Pídeme dos horarios libres para tu demo.');
    if (requested?.kind==='help') return reply(help,[connections]);
    if (requested?.kind==='recurring') return reply('Puedo programar una cita por vez. Dime la fecha de la primera cita que quieres agendar.');
    if (requested?.kind==='invitation') return reply('Los seguimientos del piloto son citas personales sin invitados. Si deseas un seguimiento personal, pídeme agendarlo para una fecha y hora.');
    try {
      if (!(config.calendarReady ?? config.ready)) return reply('Calendar está temporalmente fuera de servicio.',[connections]);
      const connection=await calendar.getConnection(user._id);
      if (!connection?.grant || !connection.calendarId) return reply('Conecta Google Calendar en Mis conexiones para usar el calendario «Seguimientos TEC» de tu propia cuenta.',[connections]);
      const binding={owner:user._id,session:createHash('sha256').update(auth.session.tokenHash+':'+sessionId).digest('hex'),grant:connection.grant,calendarId:connection.calendarId};
      if (requested?.kind==='confirm') return showReceipt(await followups.confirm(binding,requested.id,{saveEvent:data=>calendar.saveEvent(data),syncLead}),requested.id);
      if (requested?.kind==='discard') return reply(await followups.discard(binding,requested.id)==='discarded' ? 'Descarté la propuesta.':'La propuesta ya cambió, caducó o pertenece a otra conversación.');
      if (requested?.kind==='list') {
        const range=followupWindow(text,req.body?.coachTimeZone,now());
        if (range.question) return reply(range.question);
        const {state:filters}=readFollowup(text,{}, {timeZone:req.body?.coachTimeZone,now:now()});
        const rows=await followups.search(binding,{limit:6,...range,...(filters.name && {name:filters.name})});
        const caption=range.since!=null ? 'del período que pediste' : 'próximos';
        return reply(rows.length?'Estos son tus seguimientos '+caption+' registrados desde el Coach (hasta seis):\n'+rows.map(entry).join('\n'):'No encontré seguimientos '+caption+' registrados desde el Coach.');
      }
      const aad='calendar-question:'+binding.owner+':'+binding.session+':'+binding.grant;
      let state={};
      if (!requested) {
        try {
          if (typeof pending!=='string' || pending.length>8000) throw new Error();
          const decoded=JSON.parse(openToken(pending,config.key,aad));
          if (decoded.until<=now()) throw new Error();
          state=decoded.state;
        } catch { return reply('La propuesta anterior caducó. Vuelve a pedirme la cita.'); }
        if (/^(?:cancela(?:r)?|descarta(?:r)?|ol[vb][íi]dalo|ya no|no gracias)[.!]?$/i.test(text.trim())) return reply('Dejé de preparar la cita.');
      } else { state.kind=requested.kind; }
      const ask=(field,question,actions=[])=>reply(question,actions,sealToken(JSON.stringify({state:{...state,waiting:field},until:now()+15*60000}),config.key,aad));
      const selecting = state.waiting==='slot';
      if (slotCommand && (!selecting || slotCommand[2]!==state.availabilitySearchId)) return reply('Ese botón pertenece a otra búsqueda. Elige una opción de la lista más reciente.',[],pending);
      if (selecting) {
        const index=slotCommand ? Number(slotCommand[1])-1 : slotChoice(text);
        const slot=state.slots?.[index];
        if (!slot) return reply('Elige '+(state.slots?.length===1?'«1»':'«1» o «2»')+' para ver la cita antes de confirmarla.',[],pending);
        state.chosenSlot=slot;
        delete state.slots;
        delete state.waiting;
      } else {
        const parsed=readFollowup(text,state,{timeZone:req.body?.coachTimeZone,now:now()});
        state=parsed.state;
        if (parsed.question) return ask('details',parsed.question);
      }
      const search=state.kind==='availability';
      if (search && !connection.availabilityEnabled) return reply('Para buscar horarios en tu calendario principal y en «Seguimientos TEC», activa el permiso de disponibilidad de Google Calendar en Mis conexiones.',[connections]);
      let lead;
      if ((state.kind==='create' || search && !state.chosenSlot) && !state.name && !state.calendarOnly && /^[0-9a-f]{24}$/i.test(req.body?.activeCrmRecordId || '')) {
        const selected=await findLeads({owner:user._id,id:req.body.activeCrmRecordId});
        if (selected.length===1) { lead=selected[0]; state.name=lead.name; state.selectedId=String(lead.id); }
      }
      if (search && !state.chosenSlot) {
        const hours=readSlotHours(text,state.slotHours);
        if (hours.question || state.waiting==='hours' && !hours.hours) return ask('hours',hours.question || 'Dime un rango como «de 14:00 a 18:00».');
        state.slotHours=hours.hours;
        const range=slotBounds(state);
        if (range.question) return ask('details',range.question);
        const duration=state.durationMinutes ?? 45;
        if (!Number.isInteger(duration) || duration<1 || duration>1440) return ask('duration','Indica la duración de la demo en minutos, entre 1 y 1440.');
        state.durationMinutes=duration;
        const clock=new Intl.DateTimeFormat('es-US',{timeZone:state.timeZone,hour:'2-digit',minute:'2-digit',hourCycle:'h23'});
        const hoursText=clock.format(new Date(range.start))+'–'+clock.format(new Date(range.end));
        const day=new Intl.DateTimeFormat('es-US',{timeZone:state.timeZone,weekday:'long',month:'long',day:'numeric',year:'numeric'}).format(new Date(range.start));
        const caption=day+', '+hoursText+' ('+state.timeZone+'), '+duration+' min';
        if (Date.parse(range.end)<=now()) return ask('day','Ese intervalo ya pasó. ¿Qué otro día quieres consultar?');
        const busy=await calendar.availability({owner:binding.owner,grant:binding.grant,...range});
        state.slots=availableSlots(range,busy,duration,now());
        if (!state.slots.length) return ask('day','Revisé tu calendario principal y «Seguimientos TEC» para el '+caption+'. No encontré espacio. ¿Qué otro día u horario quieres revisar?');
        state.availabilitySearchId=randomBytes(16).toString('hex');
        state.action ??= 'demo';
        const options=state.slots.map((slot,i)=>(i+1)+'. '+clock.format(new Date(slot.start))+'–'+clock.format(new Date(slot.end)));
        const actions=state.slots.map((slot,i)=>({type:'calendarSlot',choice:i+1,searchId:state.availabilitySearchId,label:'Elegir '+clock.format(new Date(slot.start))}));
        return ask('slot','Revisé tu calendario principal y «Seguimientos TEC» para el '+caption+'.\n'+options.join('\n')+
          (state.slots.length===1?'\nEncontré una opción. Escribe «1»':'\nElige «1» o «2»')+' para revisar la cita antes de confirmarla.',actions);
      }
      if (!search && !state.name && !state.reference && !state.calendarOnly) return ask('name','¿Con quién es el seguimiento? Escribe el nombre como aparece en TEC, o «solo en mi calendario» para agendarlo sin prospecto.');
      let target;
      if (state.kind==='move') {
        if (!state.name && !state.reference) return ask('name','Indica el nombre o «cita» seguido de la referencia de 12 caracteres del seguimiento.');
        const matches=await followups.search(binding,{name:state.name,reference:state.reference});
        if (!matches.length) return reply('No encontré ese seguimiento en tus citas creadas desde el Coach. Puedes pedirme «Muestra mis seguimientos».');
        if (matches.length>1) return ask('event','Hay varias citas. Escribe «cita» y la referencia que quieres mover:\n'+matches.map(entry).join('\n'));
        target=matches[0];
        if (state.clock==null && state.day!=null) {
          const time=new Intl.DateTimeFormat('en-GB',{timeZone:state.timeZone || target.timeZone,hour:'2-digit',minute:'2-digit',hourCycle:'h23'}).formatToParts(new Date(target.start));
          state.clock={hour:+time.find(p=>p.type==='hour').value,minute:+time.find(p=>p.type==='minute').value};
        }
        if (!Object.hasOwn(state,'reminderMinutes')) state.reminderMinutes=target.reminderMinutes;
        state.durationMinutes ??= (Date.parse(target.end)-Date.parse(target.start))/60000;
      }
      if (search && Date.parse(state.chosenSlot.start)<=now()) return reply('Ese horario ya pasó. Pídeme dos horarios libres nuevos.');
      const when=search ? state.chosenSlot : followupTime(state,now());
      if (when.question) return ask(when.field,when.question);
      if (!target && !lead && state.name && !state.calendarOnly) {
        const candidates=await findLeads(state.selectedId ? {owner:user._id,id:state.selectedId} : {owner:user._id,name:state.name});
        if (candidates.length>1) return ask('choice','Coinciden varios prospectos de tu cuenta: '+candidates.map(c=>c.name).join(', ')+'. Escribe el nombre completo o «solo en mi calendario».');
        lead=candidates[0];
      }
      const action=target?.action || state.action || 'seguimiento';
      const name=target?.name || lead?.name || state.name || 'personal';
      const duration=(Date.parse(when.end)-Date.parse(when.start))/60000;
      const reminder=Object.hasOwn(state,'reminderMinutes') ? state.reminderMinutes : 15;
      const proposal={
        ...(search && {checkAvailability:true,availabilitySearchId:state.availabilitySearchId}),
        ...when,timeZone:state.timeZone,summary:target?.summary || (actionLabels[action]+' · '+name),name,action,reminderMinutes:reminder,
        mode:target?'move':'create',previousOperationId:target?.operationId || '',previousEtag:target?.etag || '',leadId:target?.leadId || lead?.id || '',
        expectedStart:target?.start || lead?.nextActionAt || null
      };
      const draft=await followups.prepare(binding,proposal,target);
      if (!draft) return reply('La cita cambió mientras preparaba la propuesta. Vuelve a solicitar el cambio.');
      let message=(target?'Mover «'+target.summary+'» del '+format(target.start,target.timeZone)+' al ':'Agendar «'+proposal.summary+'» el ')+format(when.start,state.timeZone)+'.';
      message+='\nCalendario: Seguimientos TEC · '+connection.mailbox+'\nDuración: '+duration+' min · '+(reminder===null?'Sin recordatorio':reminder===0?'Aviso al inicio':'Aviso '+reminder+' min antes')+'.';
      if (proposal.leadId) {
        message+='\nFicha de TEC: '+name+'. Actualizaré su próxima acción al confirmar.';
        if (!target && proposal.expectedStart) message+=' Fecha anterior: '+format(proposal.expectedStart,state.timeZone)+'.';
      } else {
        message+=state.name&&!state.calendarOnly?'\nSin coincidencia en tus prospectos. La cita se guardará solo en tu calendario.':'\nLa cita se guardará solo en tu calendario.';
      }
      if (search) message+='\nVolveré a comprobar ambos calendarios cuando confirmes.';
      message+='\nEs una cita personal, sin invitados. Confirma con el botón o escribe «Confirmar cita '+draft.id+'».';
      return reply(message,[confirmAction(draft.id),cancelAction(draft.id)]);
    } catch (error) {
      return reply(error?.reauthorize?'Google solicita volver a autorizar Calendar desde Mis conexiones.':'No pude completar la acción. Revisa la conexión con Google Calendar y vuelve a intentarlo.',[connections]);
    }
  };
}
