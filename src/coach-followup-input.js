const fold = input => String(input || '').toLowerCase().normalize('NFD').replace(/\p{M}/gu, '').replace(/\s+/g, ' ').trim();
const weekdays = ['domingo','lunes','martes','miercoles','jueves','viernes','sabado'];
const months = ['enero','febrero','marzo','abril','mayo','junio','julio','agosto','septiembre','octubre','noviembre','diciembre'];
const hours = ['cero','una','dos','tres','cuatro','cinco','seis','siete','ocho','nueve','diez','once','doce'];
const oneDay = 86400000;
const numbers = '(?:\\d{1,2}|una?|dos|tres|cuatro|cinco|seis|siete|ocho|nueve|diez|once|doce)';
const period = '(?:a\\.?\\s*m\\.?|p\\.?\\s*m\\.?|de la (?:tarde|noche|manana))';
const clockPattern = new RegExp('(?:\\ba (?:las? |eso de las? )?|\\b(?:hora|horario)[: ]+|^|\\b(?=\\d{1,2}(?::\\d{2}|\\s*' + period + ')))(' + numbers + ')(?::(\\d{2})| y (media|cuarto))?(?:\\s*(' + period + '))?(?![\\d:\\-/.])\\b','i');

export function calendarIntent(text) {
  const t = fold(text);
  if (/^solo (?:en )?(?:mi |el )?calendario[.!]?$/.test(t)) return null;
  if (/^(?:corrig|responde|contest|edita)|^(?:agrega|anade|incluye) (?:al final|al principio|la palabra)|^cambia (?:la palabra|el texto|la frase)|^cambia .+ por .+ (?:en|del) (?:la cita|el correo|el borrador)/.test(t)) return null;
  const command = /^(confirmar|descartar) cita\s+([a-f\d]{12})[.!]?$/.exec(t);
  if (command) return { kind: command[1] === 'confirmar' ? 'confirm' : 'discard', id: command[2].toUpperCase() };
  if (/^(?:confirmar|descartar) cita\b/.test(t)) return {kind:'help'};
  // Instructions for an email stay in the mail agent, including dates mentioned in its body.
  if (/\b(?:correo|gmail|email|borrador|responde|respondele|contestale)\b/.test(t) && !/^(?:agenda(?:me)?|programa(?:me)?|reagenda|reprograma|mueve)\b/.test(t)) return null;
  const object = /\b(?:seguimientos?|citas?|agendas?|calendarios?|calendar|llamadas?|demos?|reuniones?|visitas?|recordatorios?)\b/.test(t);
  const slotRequest = /\b(?:horarios?|espacios?|huecos?|horas?|bloques?|cupos?|ratos?|momentos?) (?:libres?|disponibles?)\b|\b(?:mi disponibilidad|(?:que )?disponibilidad (?:tengo|hay)|tengo disponibilidad|tengo (?:tiempo|espacio) libre|cuando puedo (?:agendar|hacer|programar|reservar)|(?:cuando (?:estoy|tengo)|estoy) libre)\b/.test(t)
    || /\b(?:busca(?:me)?|encuentra(?:me)?|propon(?:me)?|sugiere(?:me)?|dame|necesito|quiero)\b.*\b(?:espacios?|horarios?|huecos?|opciones|disponibilidad)\b/.test(t) && (object || /\b(?:para (?:hoy|manana)|de \d+ minutos|disponibilidad|(?:dos|2) (?:horarios|espacios|huecos))\b/.test(t));
  if (!object && !slotRequest) return null;
  if (/\b(?:como (?:puedo|debo|se)|ejemplo de|explicame|ense[nñ]ame|practicar|simula|supongamos|no (?:me )?(?:agendes|programes|muevas|quiero))\b/.test(t)) return null;
  if (/\b(?:cada|diari[ao]|semanal(?:mente)?|mensual(?:mente)?|todos los|todas las|recurrente)\b/.test(t)) return {kind:'recurring'};
  if (/\b(?:invita(?:r|le)?|invitacion|invitados|manda(?:le)? una invitacion)\b/.test(t) && !/\bsin invitados\b/.test(t)) return {kind:'invitation'};
  if (slotRequest) return {kind:'availability'};
  if (/\b(?:mueve(?:me)?|cambia(?:me)?|reagenda(?:me|r)?|reprograma(?:me|r)?|pospon(?:me)?|aplaza(?:me)?)\b/.test(t)) return {kind:'move'};
  if (/\b(?:muestra(?:me)?|consulta(?:r)?|que (?:tengo|citas|seguimientos)|ver mi|lista(?:me)?|revisa(?:me)?)\b/.test(t)) return {kind:'list'};
  if (/\b(?:agenda(?:me|r)?|programa(?:me|r)?|crea(?:me|r)?|agrega(?:me|r)?|anota(?:me|r)?|pon(?:me)?|reservame)\b/.test(t) && !/^(?:mi |la )?agenda$/.test(t)) return {kind:'create'};
  return /\b(?:google calendar|mi calendario)\b/.test(t) ? {kind:'help'} : null;
}

export function validTimeZone(value) {
  if (typeof value !== 'string' || value.length > 80) return '';
  try { return new Intl.DateTimeFormat('en', {timeZone:value}).resolvedOptions().timeZone; }
  catch { return ''; }
}

function localDate(now, zone) {
  const parts = new Intl.DateTimeFormat('en-CA',{timeZone:zone,year:'numeric',month:'numeric',day:'numeric'}).formatToParts(now);
  const read = type => Number(parts.find(p => p.type === type).value);
  return Date.UTC(read('year'),read('month')-1,read('day'));
}

function dayOf(year,month,day) {
  const stamp = Date.UTC(year,month-1,day);
  const value = new Date(stamp);
  return value.getUTCFullYear() === year && value.getUTCMonth() === month-1 && value.getUTCDate() === day ? stamp : NaN;
}

function parseDate(text, today) {
  // "A las ocho de la mañana" is a time of day, not a request for tomorrow.
  const t = text.replace(/(?:de|por) la manana/g,'');
  const numeric = /\b(\d{4})-(\d{2})-(\d{2})\b/.exec(t);
  if (numeric) return dayOf(+numeric[1],+numeric[2],+numeric[3]);
  const local = /\b(\d{1,2})[/-](\d{1,2})[/-](\d{4})\b/.exec(t);
  if (local) return dayOf(+local[3],+local[2],+local[1]);
  const date = /\b(\d{1,2}) de ([a-z]+)(?: (?:de |del )?(\d{4}))?\b/.exec(t);
  if (date && months.includes(date[2])) {
    let year = date[3] ? +date[3] : new Date(today).getUTCFullYear();
    const month = months.indexOf(date[2]) + 1;
    if (!date[3] && dayOf(year,month,+date[1]) < today) year++;
    return dayOf(year,month,+date[1]);
  }
  if (/\bpasado manana\b/.test(t)) return today + 2*oneDay;
  if (/\bmanana\b/.test(t)) return today+oneDay;
  if (/\bhoy\b/.test(t)) return today;
  const day = weekdays.findIndex(w => new RegExp('\\b'+w+'\\b').test(t));
  if (day >= 0) {
    const offset = (day - new Date(today).getUTCDay() + 7) % 7;
    return today + (offset || (/\bproximo\b/.test(t) ? 7 : 0))*oneDay;
  }
  return null;
}

function parseClock(t) {
  if (t.includes('mediodia')) return {hour:12,minute:0};
  if (t.includes('medianoche')) return {hour:0,minute:0};
  const match = clockPattern.exec(t);
  if (!match) return null;
  const [, number, minuteText, fraction, meridiem] = match;
  let hour = /^\d+$/.test(number) ? +number : number === 'un' || number === 'uno' ? 1 : hours.indexOf(number);
  const minute = minuteText ? +minuteText : fraction === 'media' ? 30 : fraction === 'cuarto' ? 15 : 0;
  if (hour < 0 || hour > 23 || minute > 59 || (meridiem && (hour < 1 || hour > 12))) return {invalid:true};
  if (!meridiem && hour > 0 && hour <= 12 && !/^\d{2}:\d{2}$/.test(match[0].trim().split(' ').pop())) return {ambiguous:hour, minute};
  if (meridiem) hour = hour % 12 + (/^p|tarde|noche/.test(meridiem) ? 12 : 0);
  return {hour,minute};
}

// Try offsets on both sides of a DST transition. A skipped or repeated local hour
// needs clarification before any calendar request.
export function resolveInstant(civilDay, hour, minute, zone) {
  const wall = civilDay + (hour*60+minute)*60000;
  const fmt = new Intl.DateTimeFormat('en-GB',{timeZone:zone, timeZoneName:'longOffset'});
  const candidates = new Set();
  for (const dayOffset of [-1,0,1]) {
    const offsetAt = ms => {
      const label = fmt.formatToParts(ms).find(p=>p.type==='timeZoneName').value;
      const m = /GMT([+-])(\d\d):(\d\d)/.exec(label);
      return m ? (m[1]==='-'?-1:1)*(Number(m[2])*60+Number(m[3]))*60000 : 0;
    };
    const offset = offsetAt(wall+dayOffset*oneDay);
    const utc = wall-offset;
    if (offsetAt(utc) === offset) candidates.add(utc);
  }
  return candidates.size === 1 ? [...candidates][0] : null;
}

export function readFollowup(text, previous = {}, {timeZone, now=Date.now()} = {}) {
  const clean = String(text || '').replace(/\s+/g,' ').trim();
  const t = fold(clean).replace(/\b([ap])\.\s*m\.?/g,'$1m');
  let state = {...previous};
  // For an answer to a question, a time zone typed explicitly takes precedence.
  const zoneName = clean.match(/\b(?:[A-Za-z_]+\/[A-Za-z_]+(?:\/[A-Za-z_]+)?|UTC)\b/)?.[0];
  const zone = validTimeZone(zoneName || state.timeZone || timeZone);
  state.timeZone = zone;
  const result = question => ({state,question});
  const today = zone ? localDate(now,zone) : null;
  const destination = previous.kind === 'move' ? t.match(/\bdel?\s+(?:lunes|martes|miercoles|jueves|viernes|sabado|domingo)\s+(?:al?|para el)\s+(.+)$/)?.[1] : '';
  const dateText = previous.kind === 'availability'
    ? t.replace(/\besta (?:manana|tarde|noche)\b/g,'hoy').replace(/\b(?:en|por|de) la manana\b/g,'')
    : t;
  const day = today == null ? null : parseDate(destination || dateText,today);
  if (Number.isNaN(day)) return result('Esa fecha no existe. ¿Qué fecha quieres usar?');
  // «Mañana viernes» when tomorrow is Thursday: two different days, so ask instead of picking one.
  const named = day == null ? -1 : weekdays.findIndex(w => new RegExp('\\b'+w+'\\b').test((destination || dateText).replace(/(?:de|por) la manana/g,'')));
  if (named >= 0 && new Date(day).getUTCDay() !== named) {
    const label = ['domingo','lunes','martes','miércoles','jueves','viernes','sábado'];
    const date = new Intl.DateTimeFormat('es-US',{timeZone:'UTC',day:'numeric',month:'long'}).format(day);
    return result('Dijiste ' + label[named] + ', pero esa fecha cae en ' + label[new Date(day).getUTCDay()] + ' ' + date + '. ¿Qué día quieres?');
  }
  if (day != null) state.day = day;
  const timeText = t.replace(/^\d{4}-\d{2}-\d{2}\b|^\d{1,2}[/-]\d{1,2}[/-]\d{4}\b|^\d{1,2} de [a-z]+\b/,'');
  const parsed = previous.kind === 'availability' ? null : parseClock(timeText);
  if (parsed?.invalid) return result('¿A qué hora? Escribe, por ejemplo, «18:30».');
  if (parsed) state.clock = parsed;
  else if (state.clock?.ambiguous && /^(?:por la |de la |en la )?(?:am|pm|tarde|noche|manana)[.!]?$/.test(t)) {
    state.clock = {hour:(state.clock.ambiguous % 12) + (/pm|tarde|noche/.test(t)?12:0),minute:state.clock.minute};
    if (t === 'manana' && previous.day != null) state.day = previous.day;
  }
  const contactText=previous.kind==='availability' ? clean.replace(/\b(?:por|en|de) la (?:ma[ñn]ana|tarde|noche)\b|\b(?:entre|de|desde) (?:las? )?\d+(?::\d+|\s)|\bde (?:media|una?|dos) horas?\b/giu,',') : clean;
  const nameLabel = /(?:\bcon\s+|\b(?:llam(?:ar|ada)|visitar)\s+a\s+|\bseguimiento\s+a\s+)(?:la (?:clienta|cliente) |el cliente )?(.+?)(?=\s+(?:(?:para |por )?el (?:lunes|martes|mi[eé]rcoles|jueves|viernes|s[aá]bado|domingo|\d)|(?:el |para )?d[ií]a\b|(?:para |del |el |al |este |pr[oó]ximo )?(?:hoy|pasado ma[ñn]ana|ma[ñn]ana|lunes|martes|mi[eé]rcoles|jueves|viernes|s[aá]bado|domingo|\d{4}[-/]|\d{1,2} (?:de |[/-]))|a la[s]?\b|en mi\b|(?:y )?(?:recu[eé]rdame|av[ií]same)|durante\b|por \d|solo (?:en )?(?:mi |el )?calendario)|[,.;!?]|$)/iu.exec(contactText);
  if (/^solo (?:en )?(?:mi |el )?calendario[.!]?$/.test(t)) state.calendarOnly=true;
  if (nameLabel) { state.name = nameLabel[1].trim(); delete state.selectedId; }
  else if (['name','choice'].includes(previous.waiting) && /^[\p{L}][\p{L} .’'-]{0,75}$/u.test(clean) && !day && !parsed && !zoneName && !state.calendarOnly) state.name = clean.replace(/[.!]+$/,'');
  if (/\bsolo (?:en )?(?:mi |el )?calendario\b/.test(t)) state.calendarOnly = true;
  if (state.name && (state.name.length > 80 || !/^[\p{L}\p{N} .’'-]+$/u.test(state.name))) delete state.name;
  const eventRef = /\b(?:cita|seguimiento|referencia|ref)\s+([0-9a-f]{12})\b/i.exec(t);
  if (eventRef) state.reference = eventRef[1].toUpperCase();
  const action = /\bllam(?:ada|ar)\b/.test(t) ? 'llamar' : /\bdemo\b/.test(t) ? 'demo' : /\b(?:cita|reunion|visita)\b/.test(t) ? 'cita' : '';
  if (action) state.action = action;
  const quantity = '(\\d+|media|una?|dos|quince|treinta)\\s*(minutos?|horas?)';
  const toMinutes = match => {
    const n = Number(match[1]) || ({media:0.5,un:1,una:1,dos:2,quince:15,treinta:30})[match[1]] || 0;
    return n * (match[2].startsWith('hora') ? 60 : 1);
  };
  const reminder = new RegExp(quantity+' antes').exec(t);
  const duration = new RegExp('(?:durante |duracion (?:de )?|dura |por )'+quantity).exec(t);
  if (reminder) state.reminderMinutes = toMinutes(reminder);
  if (duration) state.durationMinutes = toMinutes(duration);
  if (previous.kind==='availability') {
    const length=/\b(\d+(?:[,.]\d+)?|media|una?|dos|quince|treinta|cuarenta y cinco)\s*(min(?:utos?)?s?|h(?:oras?|rs?)?)(?: y (media|cuarto))?\b(?! antes)/.exec(t);
    if (length) {
      const n=Number(length[1].replace(',','.'));
      const amount=Number.isFinite(n) ? n : {media:0.5,un:1,una:1,dos:2,quince:15,treinta:30,'cuarenta y cinco':45}[length[1]];
      state.durationMinutes=amount*(length[2].startsWith('h')?60:1)+(length[3]==='media'?30:length[3]==='cuarto'?15:0);
    }
  }
  if (/\bsin (?:aviso|recordatorio)\b/.test(t)) state.reminderMinutes = null;
  return {state};
}

export function followupTime(state,now=Date.now()) {
  if (!state.timeZone) return {field:'zone',question:'¿En qué zona horaria estás? Por ejemplo «America/Chicago» o «America/Los_Angeles».'};
  if (state.day == null) return {field:'day',question:'¿Qué día quieres agendar? Puedes decir «mañana» o escribir la fecha como AAAA-MM-DD.'};
  if (!state.clock) return {field:'clock',question:'¿A qué hora? Puedes escribir «18:00» o «6 pm».'};
  if (state.clock.ambiguous) return {field:'clock',question:'¿A las '+state.clock.ambiguous+' de la mañana o de la tarde?'};
  const stamp = resolveInstant(state.day,state.clock.hour,state.clock.minute,state.timeZone);
  if (stamp == null) return {field:'clock',question:'Esa hora se omite o se repite en tu zona por el cambio de horario. Elige otra hora.'};
  if (stamp <= now) return {field:'day',question:'Esa fecha y hora ya pasaron. Dime una fecha y hora futuras.'};
  const duration = state.durationMinutes ?? 30;
  if (!Number.isInteger(duration) || duration < 1 || duration > 1440) return {field:'duration',question:'Indica una duración de entre 1 minuto y 24 horas, por ejemplo «durante 30 minutos».'};
  if (state.reminderMinutes != null && (!Number.isInteger(state.reminderMinutes) || state.reminderMinutes<0 || state.reminderMinutes>40320)) return {field:'reminder',question:'Indica un aviso de hasta cuatro semanas, por ejemplo «30 minutos antes».'};
  return {start:new Date(stamp).toISOString(),end:new Date(stamp+duration*60000).toISOString()};
}

// Event listings use local civil-day boundaries even when a day lasts 23 or 25 hours.
export function followupWindow(text,zoneValue,now) {
  const zone=validTimeZone(zoneValue);
  const t=fold(text);
  if (!zone) return {};
  const today=localDate(now,zone);
  let first=parseDate(t,today), next;
  if (/\besta semana\b/.test(t)) {
    first=today-((new Date(today).getUTCDay()+6)%7)*oneDay;
    next=first+7*oneDay;
  }
  if (/\beste mes\b/.test(t)) {
    const d=new Date(today);
    first=Date.UTC(d.getUTCFullYear(),d.getUTCMonth(),1);
    next=Date.UTC(d.getUTCFullYear(),d.getUTCMonth()+1,1);
  }
  if (first!=null && Number.isFinite(first)) {
    const start=resolveInstant(first,0,0,zone), end=resolveInstant(next ?? first+oneDay,0,0,zone);
    if (start!=null && end!=null) return {since:start,before:end};
  }
  if (/\b(?:hoy|ayer|manana|dias?|semanas?|mes(?:es)?|ano|lunes|martes|miercoles|jueves|viernes|sabado|domingo|diciembre|noviembre|octubre|enero|febrero|marzo|abril|mayo|junio|julio|agosto|septiembre)\b|\d/.test(t)) return {question:'Puedes pedirme tus seguimientos de hoy, de mañana, de esta semana o de una fecha como 2027-01-15.'};
  return {};
}
