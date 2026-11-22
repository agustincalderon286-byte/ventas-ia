import {followupWindow, readFollowup, resolveInstant, validTimeZone} from './coach-followup-input.js';
import {readSlotHours, slotBounds} from './coach-calendar-slots.js';

const dayMs = 86400000;
const fold = text => String(text || '').normalize('NFD').replace(/\p{M}/gu,'').toLowerCase().replace(/\s+/g,' ').trim();
const dayQuestion = '¿Qué día reviso? Puedes decir «mañana», «este viernes» o una fecha como «2027-01-15».';
const sessionChanged = 'Tu sesión cambió. Abre tu propia cuenta del Coach y vuelve a pedir la agenda.';

function agendaRange(text,zone,now) {
  const t = fold(text);
  // Resolving only the first date in a spoken range would silently omit appointments.
  const dateWord = '(?:hoy|manana|lunes|martes|miercoles|jueves|viernes|sabado|domingo|\\d{4}-\\d{2}-\\d{2})';
  if (new RegExp(dateWord+' (?:y|al?|hasta|o) (?:el |la |el dia )?'+dateWord+'\\b').test(t) ||
      /\b(?:del|desde el) \d{1,2}(?: de \p{L}+)? (?:al?|hasta el) \d{1,2}\b/u.test(t) ||
      /\b(?:semana|mes) (?:que viene|pasad[oa]|siguiente)|\b(?:proxim[ao]|otra|siguiente|pasad[oa]) (?:semana|mes)|\b(?:dentro de|proximos|ultimos) \w+ dias\b/.test(t)) {
    return {question:'¿Qué día concreto reviso primero?'};
  }
  const dayText=t.replace(/\besta (?:manana|tarde|noche)\b/g,'hoy').replace(/\b(?:por|de|en) la manana\b/g,'');
  const window=followupWindow(dayText,zone,now);
  if (window.since==null || window.before==null) return {question:dayQuestion};
  // A standalone spoken hour such as «mañana, cuatro de la tarde» needs the same
  // handling as «a las cuatro de la tarde». Identify the clock before treating
  // «de la tarde» as the business-hours afternoon window.
  const clockWord = '(?:\\d{1,2}(?::\\d{2})?|una?|dos|tres|cuatro|cinco|seis|siete|ocho|nueve|diez|once|doce)';
  const spokenHour = new RegExp('\\b('+clockWord+'(?: y (?:media|cuarto))? (?:de la (?:tarde|noche|manana)|[ap]\\.?\\s*m\\.?))\\b');
  const clockText = t.replace(spokenHour,(hour,_matched,offset)=>
    /(?:a las? |entre las? |de las? |desde las? |hasta las? |^)$/.test(t.slice(0,offset)) ? hour : 'a las '+hour);
  if (/\b(?:a eso de|alrededor de|tipo las?)\b/.test(t) ||
      /\b(?:a las?|\d{1,2}:\d\d) .+? (?:o|y tambien) (?:a )?(?:las? )?(?:\d|una?|dos|tres|cuatro|cinco|seis|siete|ocho|nueve|diez|once|doce)\b/.test(t)) {
    return {question:'¿Qué intervalo exacto quieres que compruebe?'};
  }
  const bounds=readSlotHours(clockText);
  const dateState=readFollowup(t,{kind:'availability'},{timeZone:zone,now}).state;
  if (bounds.hours) {
    if (dateState.day==null || window.before-window.since>26*3600000) return {question:'¿Para qué día reviso ese horario?'};
    return slotBounds({...dateState,slotHours:bounds.hours});
  }
  if (bounds.question) {
    if (/\b(?:entre|antes de|despues de|desde|hasta|a partir|de las?)\b/.test(t.replace(/de la (?:tarde|manana|noche)/g,''))) return {question:bounds.question};
    const parsed=readFollowup(clockText,{}, {timeZone:zone,now});
    const clock=parsed.state.clock;
    if (!clock || parsed.question || dateState.day==null || window.before-window.since>26*3600000) return {question:bounds.question};
    if (clock.ambiguous) return {question:'¿A las '+clock.ambiguous+' de la mañana o de la tarde?'};
    const start=resolveInstant(dateState.day,clock.hour,clock.minute,zone);
    if (start==null) return {question:'Esa hora coincide con un cambio de horario. ¿Qué otro horario reviso?'};
    const minutes=parsed.state.durationMinutes ?? dateState.durationMinutes ?? 1;
    if (!Number.isInteger(minutes) || minutes<1 || minutes>1440) return {question:'¿De cuántos minutos es el intervalo que reviso?'};
    return {start:new Date(start).toISOString(),end:new Date(start+minutes*60000).toISOString(),point:minutes===1};
  }
  if (dateState.durationMinutes && /\b(?:libre|disponib\w*|cabe|cup[oa]|espacio|tiempo)\b/.test(t)) {
    return {question:'¿Desde qué hora quieres que compruebe esos '+dateState.durationMinutes+' minutos?'};
  }
  if (/\b(?:a (?:eso de |las? )?|tipo |sobre las?|desde |despues de |antes de )\s*(?:cuatro|dos|una|tres|cinco|seis|siete|ocho|nueve|diez|once|doce|\d{1,2})\b/.test(t)) {
    return {question:'¿A qué hora exactamente? Puedes decir «a las cuatro de la tarde».'};
  }
  return {start:new Date(window.since).toISOString(),end:new Date(window.before).toISOString(),wholeDays:true};
}

function sourceFailure(label,error) {
  if (error?.reauthorize) return 'Google pide renovar el permiso para '+label+' en Mis conexiones.';
  if (error?.status===422) return 'El período tiene demasiados eventos en '+label+'. Pídeme un día o un intervalo más corto.';
  return 'No pude comprobar '+label+' en Google. Puedes volver a consultar.';
}

export async function readCoachAgenda({calendar,owner,zone,text,now=Date.now,current}) {
  zone=validTimeZone(zone);
  if (!zone) return '¿En qué zona horaria reviso tu agenda?';
  const at=typeof now==='function'?now():now;
  const range=agendaRange(text,zone,at);
  if (range.question) return range.question;
  const t=fold(text);
  const principal = '(?:(?:mi |el )?calendario (?:principal|personal)|(?:el |mi )?principal)';
  const secondary = '(?:(?:(?:el |mi )?calendario (?:de )?)?seguimientos tec)';
  const mentions = source => new RegExp('\\b'+source+'\\b').test(t);
  const excluded = source => new RegExp('\\b(?:excepto|salvo|sin (?:consultar|revisar|ver|leer)|no (?:consultes|revises|veas|leas|uses)|no en) '+source+'\\b').test(t);
  const skipPrimary=excluded(principal), skipEvents=excluded(secondary);
  const includeEvents=!skipEvents && (skipPrimary || !mentions(principal) || mentions(secondary));
  const includePrimary=!skipPrimary && (skipEvents || !mentions(secondary) || mentions(principal));
  if (!includeEvents && !includePrimary) return '¿Qué calendario quieres que consulte?';
  try {
    if (!owner || !current || !await current()) return sessionChanged;
    const connection=await calendar.getConnection(owner);
    if (!connection?.grant || !connection.calendarId) return 'Conecta tu Google Calendar en Mis conexiones para que pueda revisar tu agenda.';
    const {events,busy,availabilityEnabled}=await calendar.readAgenda({
      owner, grant:connection.grant, includeEvents,includePrimary,zone, start:range.start,end:range.end
    });
    if (!await current()) return sessionChanged;
    const day=new Intl.DateTimeFormat('es-US',{timeZone:zone,day:'numeric',month:'short',year:'numeric'});
    const time=new Intl.DateTimeFormat('es-US',{timeZone:zone,hour:'numeric',minute:'2-digit'});
    const start=new Date(range.start), end=new Date(range.end);
    const first=day.format(start),last=day.format(new Date(end.getTime()-1));
    const dayLabel=first===last?first:first+' al '+last;
    const when=range.wholeDays?'':range.point?' a las '+time.format(start):' de '+time.format(start)+' a '+time.format(end);
    const lines=[dayLabel+when+' ('+zone+').'];
    const demoTitle=/\b(?:demos?|demostracion(?:es)?|demonstrations?)\b/;
    const onlyDemos=demoTitle.test(t) && !/\b(?:libre|disponib\w*|huecos?|bloques?|espacio|tiempo para|para (?:una|la) demo)\b/.test(t);
    const wantsFirst=/\b(?:primera?|primer[oa]|proxim[oa]|siguiente) (?:\w+ )?(?:citas?|demos?|demostracion|seguimiento)\b/.test(t);
    const next=/\b(?:proxim[oa]|siguiente) (?:\w+ )?(?:cita|demo|demostracion|seguimiento)\b/.test(t);
    const label=value=>{
      const instant=new Date(value);
      return first===last && day.format(instant)===first ? time.format(instant) : day.format(instant)+', '+time.format(instant);
    };
    const civil = value=>new Intl.DateTimeFormat('es-US',{timeZone:'UTC',month:'short',day:'numeric'}).format(new Date(value));
    const limited = (rows,format) => rows.slice(0,10).map(format).join('; ')+(rows.length>10?' (mostrando 10 de '+rows.length+')':'');
    if (includeEvents) {
      if (events.status==='rejected') lines.push(sourceFailure('Seguimientos TEC',events.reason));
      else {
        const includeDeclined=/\b(?:declinad[oa]s?|rechazad[oa]s?)\b/.test(t);
        const items=events.value.filter(item=>
          (includeDeclined ? item.attendance==='declined' : item.attendance!=='declined') &&
          (!onlyDemos || demoTitle.test(fold(item.title))) && (!next || item.allDay || Date.parse(item.start)>=at));
        const format=item=>{
          let hours;
          if (item.allDay) {
            const through=new Date(Date.parse(item.end)-dayMs).toISOString().slice(0,10);
            hours='todo el día '+civil(item.start)+(through===item.start?'':' al '+civil(through));
          } else hours=label(item.start)+(item.openEnd?' (fin sin especificar)':' a '+label(item.end));
          const invitation={needsAction:'invitación pendiente',tentative:'asistencia tentativa',declined:'invitación rechazada'}[item.attendance];
          const notes=[invitation,item.tentative && !invitation && 'tentativa',item.available && 'marcado libre'].filter(Boolean);
          return hours+' · '+item.title+(notes.length?' ('+notes.join(', ')+')':'');
        };
        if (wantsFirst && items.length && !includeDeclined) {
          // An all-day item has no clock time and cannot be put first by hour.
          const allDay=items.filter(item=>item.allDay), timed=items.filter(item=>!item.allDay);
          if (allDay.length) lines.push('Seguimientos TEC, sin hora asignada: '+limited(allDay,format)+'.');
          if (timed.length) {
            const firstAt = Math.min(...timed.map(row=>Date.parse(row.start)));
            const firstRows=timed.filter(row=>Date.parse(row.start)===firstAt);
            lines.push('Seguimientos TEC, '+(onlyDemos?'primera demo':'primera cita')+' con hora: '+limited(firstRows,format)+'.');
          }
        } else if (items.length) lines.push('Seguimientos TEC: '+limited(items,format)+'.');
        else lines.push(onlyDemos?'Seguimientos TEC: no encontré eventos con «demo» o «demostración» en el título en este período.':
          includeDeclined?'Seguimientos TEC: sin invitaciones rechazadas en este período.':'Seguimientos TEC: sin citas en este período.');
      }
    }
    if (includePrimary) {
      if (!availabilityEnabled) lines.push('Para ver los bloques ocupados de tu calendario principal, activa «Horarios libres» en Mis conexiones.');
      else if (busy.status==='rejected') lines.push(sourceFailure('tu calendario principal',busy.reason));
      else {
        lines.push(busy.value.length?'Calendario principal, bloques ocupados: '+limited(busy.value,b=>label(b.start)+' a '+label(b.end))+'.':'Calendario principal: sin bloques ocupados en este período.');
        if (busy.value.length || onlyDemos || wantsFirst || /\binvitacion(?:es)?\b/.test(t)) lines.push('Del principal solo puedo ver disponibilidad. Google no muestra los títulos ni el estado de las invitaciones con este permiso.');
      }
    }
    // Only answer a yes/no availability question for a concrete instant or interval
    // and the sources requested. A one-minute lookup says nothing about a whole visit.
    if (!range.wholeDays && /\b(?:libre|disponible|ocupad[oa]|cabe|hueco)\b/.test(t)) {
      const checkedEvents=!includeEvents || events.status==='fulfilled';
      const checkedPrimary=!includePrimary || (availabilityEnabled && busy.status==='fulfilled');
      const occupied=(includeEvents && checkedEvents && events.value.some(e=>!e.available && e.attendance!=='declined')) ||
        (includePrimary && checkedPrimary && busy.value.length>0);
      const referred=range.point?'a esa hora':'durante ese intervalo completo';
      if (occupied) lines.push('Aparece tiempo ocupado '+(range.point?'a esa hora.':'dentro del intervalo.'));
      else if (checkedEvents && checkedPrimary) lines.push('En los calendarios consultados tienes disponibilidad '+referred+'.');
    }
    return lines.join('\n');
  } catch (error) {
    return sourceFailure('tu agenda',error);
  }
}
