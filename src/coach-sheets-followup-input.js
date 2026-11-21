const months = 'enero febrero marzo abril mayo junio julio agosto septiembre octubre noviembre diciembre'.split(' ');
const weekdays = 'domingo lunes martes miercoles jueves viernes sabado'.split(' ');
const weekdayNames = new Map(weekdays.flatMap(day => [[day,day],[day.slice(0,3),day]]));
const dayMs = 86400000;
const normalized = value => String(value ?? '').normalize('NFD').replace(/\p{M}/gu,'').toLowerCase().trim();
const fullDatePattern = String.raw`(?:\d{4}-\d{2}-\d{2}|\d{1,2}[/-]\d{1,2}[/-](?:\d{4}|\d{2})|\d{1,2}\s+de\s+(?:` +
  months.join('|') + String.raw`)\s+(?:de(?:l)?\s+)?\d{4})\b`;
const weekdayPattern = [...weekdayNames.keys()].join('|');
const headingStart = '\\s*[([{]?(?:(' + weekdayPattern + ')[.,]?\\s+)?(' + fullDatePattern + ')';
const datedHeading = new RegExp('^' + headingStart + '[)\\]}]?[.,:–—-]?\\s*(?:(\\d{1,2}):(\\d{2})\\s*([ap]m)?\\.?\\s*)?');
const nextDatedHeading = new RegExp('[;\\r\\n]+(?=\\s*[([{]?(?:(?:' + weekdayPattern + ')[.,]?\\s+)?' + fullDatePattern + ')');
const scheduledDate = new RegExp('^(' + fullDatePattern + ')');
const scheduledWeekday = new RegExp('^(' + weekdays.join('|') + '),?\\s+(' + fullDatePattern + ')');
const stopContact = /\b(?:no (?:volver (?:a )?)?(?:llamar(?:l[aeos]*)?|marcar(?:le)?|contactar(?:l[aeos]*)?|insistir)|no (?:quiere|desea|acepta) (?:que (?:se )?l[aeo] (?:llamen|llamemos|llame|contacten)|(?:mas )?(?:llamadas|contacto))|dejar de llamar|do not (?:call|contact)|opt[ -]?out)\b/;
const closed = /\b(?:no (?:le |esta )?interes(?:a|ad[oa])|rechaz(?:o|ad[oa])|descartad[oa]|(?:numero|telefono|linea) (?:equivocad[oa]|incorrect[oa]|invalid[oa]|desconectad[oa]|fuera de servicio)|no (?:recibe|acepta) llamadas entrantes|(?:seguimiento|caso) (?:cerrado|finalizado)|(?:cita|demo|visita) (?:ya )?(?:confirmada|agendada|realizada|efectuada)|venta (?:hecha|realizada|cerrada|confirmada)|vendid[oa])\b/;
const closedLabel = /^(?:fuera de servicio|desconectad[oa]|venta|vendid[oa]|cerrad[oa]|completad[oa]|cancelad[oa]|confirmad[oa]|atendid[oa]|realizad[oa]|no interesad[oa])[.!]?$/;
const callback = /\b(?:llamar(?:l[aeos]*|me|nos)?|llama(?:le|lo|la)|(?:que (?:se )?l[aeo] )llam(?:e|emos|en)|marcar(?:l[aeos]*)?|contactar(?:l[aeos]*)?|recontactar(?:l[aeos]*)?|volver a (?:intentar|marcar)|volver a (?:llamar|contactar)(?:l[aeos]*)?|(?:dar(?:le)?|hacer|proximo) seguimiento|seguimiento(?: pendiente| programado)?|llamada (?:pendiente|programada)|devolver (?:la )?llamada|reagendar|reprogramar|retomar)\b/g;
const attempt = /\b(?:no (?:me |le |se )?(?:contesto|contesta|respondio|responde|localizo|presento)|sin respuesta|buzon(?: de voz)?|dejo mensaje|se dejo (?:un )?mensaje|numero ocupado|no se pudo contactar)\b/;
const pendingLabel = /\b(?:pendiente(?:s)?|por contactar|por llamar|sin contactar|sin gestionar|por confirmar)\b/;
const attemptedAction = /(?:\b(?:intente|intento|intentamos|trate|trato|tratamos)(?: de)?|\b(?:al|sin poder)|\b(?:no (?:pude|pudo|pudimos|logre|logro)|no se (?:pudo|logro))|\b(?:se (?:hizo|realizo|completo|dio)|hice|hicimos|dimos)(?: (?:el|un))?)\s*$/;

// Sheet cell dates are civil days. UTC is used only for calendar arithmetic, never for deciding the seller's today.
export function sheetsToday(now, timeZone) {
  const parts = new Intl.DateTimeFormat('en', {timeZone,year:'numeric',month:'2-digit',day:'2-digit'}).formatToParts(now);
  const part = key => parts.find(p => p.type === key).value;
  return part('year') + '-' + part('month') + '-' + part('day');
}

function civilDay(year, month, day) {
  if (year < 1900 || year > 2199 || month < 1 || month > 12 || day < 1 || day > 31) return '';
  const date = new Date(Date.UTC(year,month-1,day));
  return date.getUTCMonth() === month-1 ? date.toISOString().slice(0,10) : '';
}

function numericOrder(locale) {
  if (!locale) return '';
  try {
    const parts = new Intl.DateTimeFormat(locale.replace(/_/g,'-'),{month:'numeric',day:'numeric'}).formatToParts(0);
    return parts.find(p => p.type === 'month' || p.type === 'day')?.type || '';
  } catch { return ''; }
}

function explicitDate(text, order = '') {
  let match = /^(\d{4})-(\d{2})-(\d{2})$/.exec(text);
  if (match) return civilDay(+match[1],+match[2],+match[3]);
  match = /^(\d{1,2}) de ([a-z]+) (?:de(?:l)? )?(\d{4})$/.exec(text);
  if (match) return civilDay(+match[3],months.indexOf(match[2])+1,+match[1]);
  match = /^(\d{1,2})([/-])(\d{1,2})\2(\d{4}|\d{2})$/.exec(text);
  if (!match) return '';
  const a = Number(match[1]), b = Number(match[3]);
  const year = match[4].length === 2 ? (Number(match[4]) < 70 ? 2000 : 1900) + Number(match[4]) : Number(match[4]);
  order = a > 12 ? 'day' : b > 12 || a === b ? 'month' : order;
  if (!order) return '';
  return order === 'day' ? civilDay(year,b,a) : civilDay(year,a,b);
}

function plusDays(ymd, delta) {
  return new Date(Date.parse(ymd+'T00:00:00Z') + delta*dayMs).toISOString().slice(0,10);
}

function nextDate(text, anchor, order) {
  // Dates describing a visit, a previous call or a purchase elsewhere in the note are not callback dates.
  const after = text.replace(/^[\s:–—-]*(?:(?:el dia|el|para el|para|este)\s+)?/, '');
  const match = scheduledDate.exec(after);
  if (match) return explicitDate(match[1],order);
  const datedWeekday = scheduledWeekday.exec(after);
  if (datedWeekday) {
    const date = explicitDate(datedWeekday[2],order);
    return date && weekdays[new Date(date+'T00:00:00Z').getUTCDay()] === datedWeekday[1] ? date : '';
  }
  if (!anchor) return '';
  const relative = /^(pasado manana|manana|hoy)\b/.exec(after);
  if (relative) return plusDays(anchor,relative[1] === 'hoy' ? 0 : relative[1] === 'manana' ? 1 : 2);
  const days = /^en (\d{1,2}) dias?\b(?!\s+(?:habiles|laborales))/.exec(after);
  if (days) return plusDays(anchor,+days[1]);
  const weekday = /^(?:(proximo) )?(domingo|lunes|martes|miercoles|jueves|viernes|sabado)\b/.exec(after);
  if (weekday) {
    const offset = (weekdays.indexOf(weekday[2])-new Date(anchor+'T00:00:00Z').getUTCDay()+7)%7;
    return offset || weekday[1] ? plusDays(anchor,offset || 7) : '';
  }
  return '';
}

function latestEntry(value) {
  const headings = new Set();
  // Inside one dated note, a new line or semicolon can continue the same call.
  // A spaced slash or a fresh date heading starts an independent entry.
  const sections = normalized(value).split(/\s+\/\s+/).flatMap(section => section.split(nextDatedHeading));
  const lines = sections.filter(s => s.trim()).map(s => {
    const text = s.trim().replace(/\s+/g,' ');
    const prefix = datedHeading.exec(text);
    if (!prefix) return {text};
    const [, weekdayLabel, token, hour, minute, period] = prefix;
    const weekday = weekdayNames.get(weekdayLabel);
    if (/^\d{1,2}[/-]/.test(token)) {
      const [first, second] = token.split(/[/-]/).map(Number);
      if (first !== second) {
        const possible = ['day','month'].filter(order => {
          if (order === 'day' ? second > 12 : first > 12) return false;
          const day = explicitDate(token,order);
          return day && (!weekday || weekdays[new Date(day+'T00:00:00Z').getUTCDay()] === weekday);
        });
        if (possible.length === 1) headings.add(possible[0]);
      }
    }
    const hours = period ? Number(hour)%12 + (period === 'pm' ? 12 : 0) : Number(hour);
    return {text:text.slice(prefix[0].length),token,weekday,minutes:hour ? hours*60+Number(minute) : null};
  });
  if (!lines.length) return {text:'',anchor:''};
  const order = headings.size === 1 ? [...headings][0] : '';
  for (const line of lines) {
    line.order = order;
    line.anchor = line.token ? explicitDate(line.token,order) : '';
    if (line.anchor && line.weekday && weekdays[new Date(line.anchor+'T00:00:00Z').getUTCDay()] !== line.weekday) line.anchor = '';
  }
  // Teams may put the newest dated entry first. When lines lack timestamps, only their written order is known.
  if (lines.every(line => line.anchor)) return lines.reduce((a,b) =>
    a.anchor > b.anchor || a.anchor === b.anchor && a.minutes != null && b.minutes != null && a.minutes > b.minutes ? a : b);
  return lines.at(-1);
}

function analyze(entry) {
  if (closed.test(entry.text) || closedLabel.test(entry.text)) return {closed:true};
  const mentions = [...entry.text.matchAll(callback)];
  const actions = mentions.filter(match => !attemptedAction.test(entry.text.slice(0,match.index))
    && !/^\s+(?:realizado|finalizado|cerrado|completado|efectuado)\b/.test(entry.text.slice(match.index+match[0].length)));
  if (actions.length) {
    const last = actions.at(-1);
    const reason = /llam|marcar/.test(last[0]) ? 'La última gestión indica que hay que volver a llamar.'
      : /reagendar|reprogramar/.test(last[0]) ? 'La última gestión pide reprogramar la cita.'
      : 'La última gestión indica que hay que retomar el contacto.';
    return {date:nextDate(entry.text.slice(last.index+last[0].length),entry.anchor,entry.order),reason};
  }
  if (/^n[.]?\s*c[.]?$/.test(entry.text.trim())) return {date:'',reason:'La última nota dice «NC». Confirma su significado y la próxima fecha.'};
  if (attempt.test(entry.text)) return {date:'',generic:true,reason:'El último intento quedó sin respuesta.'};
  if (pendingLabel.test(entry.text)) return {date:'',generic:true,reason:'La hoja marca el contacto como pendiente.'};
  return null;
}

export function classifySheetsFollowup({history, result, followupDate, status}, {today, locale} = {}) {
  for (const value of [history,result,status]) {
    if (stopContact.test(normalized(value))) return null;
  }
  const resultEntry = latestEntry(result);
  const outcome = analyze(resultEntry);
  const state = analyze(latestEntry(status));
  const log = latestEntry(history);
  const historyEntry = analyze(log);
  if (outcome?.closed || state?.closed || historyEntry?.closed) return null;
  const writtenDate = normalized(followupDate);
  const outcomeAt = outcome && resultEntry.anchor;
  const preferHistory = historyEntry && log.anchor &&
    (outcomeAt ? log.anchor > outcomeAt : outcome?.generic && historyEntry.date);
  const next = preferHistory ? historyEntry : outcome || historyEntry || state;
  if (!writtenDate && !next) return null;
  const date = writtenDate ? explicitDate(writtenDate,numericOrder(locale)) : next.date;
  const reason = writtenDate
    ? (date ? 'La columna de próxima gestión indica esta fecha.' : 'La fecha anotada para la próxima gestión requiere revisión.')
    : next.reason;
  const bucket = !date ? 'undated' : date === today ? 'today' : date < today ? 'overdue' : 'future';
  const lastContact=next===historyEntry ? log.anchor : next===outcome ? outcomeAt : '';
  return {bucket,date,reason:!writtenDate && lastContact ? reason+' Gestión del '+lastContact+'.' : reason};
}
