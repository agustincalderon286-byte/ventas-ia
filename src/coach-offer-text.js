import {availableSlots} from './coach-calendar-slots.js';

// Pure helpers for the Gmail -> two time options -> booking flow.
// Nothing here follows instructions found in an email: the prospect's reply is only
// matched against the two options that the seller already sent.

export const fold = value => String(value ?? '').toLowerCase().normalize('NFD').replace(/\p{M}/gu, '')
  .replace(/[¿¡]/g, '').replace(/\s+/g, ' ').trim();
const speaker = /^(?:(?:oye|coach(?: tec)?|por favor)[, ]+)*(?:(?:puedes|podrias)\s+)?/;
const ordinals = {primer:1,primero:1,primera:1,segundo:2,segunda:2,tercer:3,tercero:3,tercera:3,cuarto:4,cuarta:4,quinto:5,quinta:5,ultimo:1,'mas reciente':1};

const offerVerb = /\b(?:ofrec(?:e|ele|eles|er|erle)|propon(?:le|les|er|erle)?|sugier(?:e|ele)|sugerir(?:le)?|mand(?:a|ale|ar|arle)|envia(?:le|r|rle)?|respond(?:e|ele|er|erle)|contest(?:a|ale|ar|arle)|da(?:le)?|pasa(?:le)?)\b/;
const offerObject = /\b(?:(?:dos|2|un par de) )?(?:horarios?|opciones(?: de horarios?)?|espacios?|huecos?|fechas?(?: y horas?)?)(?: (?:libres?|disponibles?))?\b/;
// Digits only count after "al"/"el"/"correo"/"número" and never when they read as a time or date.
const target = /\b(?:(?:al?|el|la|del)\s+(?:(?:correo|e-?mail|mensaje|hilo)\s+)?(primer[oa]?|segund[oa]|tercer[oa]?|cuart[oa]|quint[oa]|ultimo|mas reciente)\b|(?:al|el|correo|mensaje|hilo|numero|n[º°])\s+(?:(?:correo|mensaje|hilo|numero)\s+)?#?([1-5])\b(?!\s*(?::|\/|-|am\b|pm\b|de |y media|y cuarto|horas?|minutos?|a \d)))/;

// "Ofrécele dos horarios al primero para mañana por la tarde"
export function offerRequest(text) {
  const t = fold(text).replace(speaker, '');
  if (!t || t.length > 600) return null;
  if (/\b(?:como (?:puedo|se|hago)|ejemplo|supongamos|imagina|no (?:le )?(?:ofrezcas|mandes|envies|respondas))\b/.test(t)) return null;
  if (!offerVerb.test(t) || !offerObject.test(t)) return null;
  // A booking request about an offer already sent is handled by bookingRequest.
  if (/\b(?:eligio|escogio|prefirio|respuesta de la oferta|de la oferta)\b/.test(t)) return null;
  const match = target.exec(t);
  const word = match?.[1] || match?.[2];
  const index = word ? ordinals[word] || Number(word) : null;
  const rest = (match ? t.slice(0, match.index) + ' ' + t.slice(match.index + match[0].length) : t)
    .replace(offerObject, ' ').replace(offerVerb, ' ').replace(/\s+/g, ' ').trim();
  return {kind:'offer', index, latest:word === 'ultimo' || word === 'mas reciente', rest};
}

const bookingVerb = /\b(?:agend(?:a|ame|ar|ale)|reserv(?:a|ame|ar)|program(?:a|ame|ar)|confirm(?:a|ar|ale))\b/;
const offerWords = /\b(?:oferta|ofrecid[oa]s?|que (?:te |me |le )?(?:ofreci|propuse|mande)|eligio|escogio|prefirio|respuesta|respondio|contesto|en el (?:hilo|correo))\b/;
const whoName = /\b(?:con|de|eligio|escogio|prefirio|respondio|contesto)\s+((?:\p{L}[\p{L}'’-]*)(?:\s+\p{L}[\p{L}'’-]*){0,2})/u;
const notNames = /^(?:demo|cita|horario|horarios|oferta|opcion|correo|correos|hilo|cliente|clienta|prospecto|prospecta|manana|hoy|ya|mi|la|el)$/;

// "Revisa la respuesta de la oferta", "¿Qué horario eligió María?", "Agenda la demo de la oferta".
export function bookingRequest(text) {
  const t = fold(text).replace(speaker, '');
  if (!t || t.length > 400) return null;
  if (/\b(?:como (?:puedo|se|hago)|ejemplo|supongamos|imagina|no (?:la )?(?:agendes|reserves))\b/.test(t)) return null;
  const ref=/\boferta\s+(?:ref\.?\s*)?([a-f0-9]{8})\b/.exec(t)?.[1];
  if (ref && /\b(?:revisa|muestra|lee|agenda|ver|abre)\b/.test(t)) return {kind:'booking',ref:ref.toUpperCase(),name:'',index:null};
  const explicit = /\bopcion (?:numero )?(1|2|uno|dos)\b|\b(?:la|el) (primer[ao]?|segund[ao]) (?:opcion|horario)\b/.exec(t);
  const index = explicit ? (/^(?:1|uno|primer[ao]?)$/.test(explicit[1] || explicit[2]) ? 1 : 2) : null;
  const reading = /\b(?:revis|chec|mir|lee|ve |ver )\w*\b.*\b(?:respuesta|respondio|contesto)\b/.test(t) ||
    /\bque (?:horario|opcion|hora|dia) (?:eligio|escogio|prefirio|quiere|pidio)\b/.test(t) ||
    /\b(?:ya )?(?:respondio|contesto)\b.*\b(?:oferta|horarios?|opciones|demo)\b/.test(t);
  const booking = bookingVerb.test(t) && /\b(?:demo|cita|horario|opcion)\b/.test(t) && offerWords.test(t);
  // A bare "agenda la demo (con María)" is only claimed when an offer exists and no time is given.
  const bare = /^(?:agend(?:a|ame|ar) (?:la|esa) (?:demo|cita))(?: (?:con|de) [\p{L} '’-]{1,60})?[.!]?$/u.test(t) &&
    !/\b(?:hoy|manana|pasado|lunes|martes|miercoles|jueves|viernes|sabado|domingo|semana|mediodia|noche|tarde|a las?)\b/.test(t);
  if (!reading && !booking && !bare && !(index && /\boferta\b/.test(t))) return null;
  const cleaned = t.replace(/[?.!,;:]/g, ' ').replace(/\b(?:la oferta|el hilo|el correo|la respuesta|la demo|la cita|el horario|la opcion)\b/g, ' ')
    .replace(/\s+/g, ' ');
  const raw = whoName.exec(cleaned)?.[1] || /\b(?:eligio|escogio|prefirio) (.+?)[?.!]*$/.exec(t)?.[1] || '';
  const words = raw.split(' ').filter(Boolean);
  while (words.length && notNames.test(words[0])) words.shift();
  const stop = words.findIndex(w => notNames.test(w) || /^(?:en|del|de|para|a|y|por)$/.test(w));
  const name = (stop >= 0 ? words.slice(0, stop) : words).join(' ');
  return {kind:'booking', index, bare: bare && !reading && !booking, name, email:t.match(/[^\s<>]+@[a-z0-9.-]+\.[a-z]{2,}/i)?.[0]};
}

export function decodeSnippet(value) {
  return String(value ?? '').replace(/&#(\d{1,6});/g, (_, n) => String.fromCodePoint(Math.min(Number(n), 0x10ffff)))
    .replace(/&quot;/g, '"').replace(/&apos;/g, "'").replace(/&lt;/g, '<').replace(/&gt;/g, '>').replace(/&amp;/g, '&')
    .replace(/[\p{Cc}‪-‮⁦-⁩]+/gu, ' ').replace(/\s+/g, ' ').trim();
}

// Remove the quoted history that clients append under a reply.
export function replyExcerpt(snippet) {
  const text = decodeSnippet(snippet);
  const cut = /\s(?:el|on) [^]{0,120}?(?:escribio|escribió|wrote):?|\s-{2,}\s*(?:mensaje original|original message)|\s>\s|\sde: .{0,80}@/i.exec(' ' + text);
  return (cut ? (' ' + text).slice(0, cut.index) : text).trim().slice(0, 240);
}

const localParts = (iso, timeZone) => {
  const parts = new Intl.DateTimeFormat('en-GB', {timeZone, weekday:'long', day:'numeric', hour:'2-digit', minute:'2-digit', hourCycle:'h23'})
    .formatToParts(new Date(iso));
  const get = type => parts.find(p => p.type === type)?.value;
  return {weekday:fold(get('weekday')), day:Number(get('day')), minutes:Number(get('hour')) * 60 + Number(get('minute'))};
};
const englishDays = {sunday:'domingo',monday:'lunes',tuesday:'martes',wednesday:'miercoles',thursday:'jueves',friday:'viernes',saturday:'sabado'};
const numberWords = {una:1,uno:1,un:1,dos:2,tres:3,cuatro:4,cinco:5,seis:6,siete:7,ocho:8,nueve:9,diez:10,once:11,doce:12};
const clockWord = '(\\d{1,2}|una|uno|dos|tres|cuatro|cinco|seis|siete|ocho|nueve|diez|once|doce)';
const meridiem = '(am|pm|de la manana|de la tarde|de la noche|del mediodia)';
const clockMentions = [
  new RegExp('\\b(?:a )?(?:las?|eso de las?)\\s+' + clockWord + '(?::(\\d{2})| y (media|cuarto))?(?:\\s*' + meridiem + ')?(?![\\d/])', 'g'),
  new RegExp('\\b(\\d{1,2})(?::(\\d{2}))?()\\s*' + meridiem + '\\b', 'g'),
  new RegExp('\\b(\\d{1,2}):(\\d{2})()()\\b', 'g')
];

function mentionedTimes(t) {
  const found = [];
  const seen = new Set();
  for (const pattern of clockMentions) {
    for (const m of t.matchAll(pattern)) {
      const at = m.index + m[0].indexOf(m[1]);
      if (seen.has(at)) continue;
      seen.add(at);
      const hour = /^\d+$/.test(m[1]) ? Number(m[1]) : numberWords[m[1]];
      const extra = m[2] ? Number(m[2]) : m[3] === 'media' ? 30 : m[3] === 'cuarto' ? 15 : 0;
      if (!(hour >= 0 && hour <= 23 && extra <= 59)) return null;
      const period = m[4] || '';
      const values = new Set();
      if (/^(?:pm|de la tarde|de la noche)$/.test(period)) { if (hour < 1 || hour > 12) return null; values.add((hour % 12 + 12) * 60 + extra); }
      else if (/^(?:am|de la manana)$/.test(period)) { if (hour < 1 || hour > 12) return null; values.add((hour % 12) * 60 + extra); }
      else if (period === 'del mediodia') values.add(12 * 60 + extra);
      else if (hour > 12 || hour === 0) values.add(hour * 60 + extra);
      else { values.add((hour % 12) * 60 + extra); values.add((hour % 12 + 12) * 60 + extra); }
      found.push(values);
    }
  }
  return found;
}

// Returns {index} only when the reply points to exactly one of the options, otherwise {reason}.
export function offerChoice(snippet, slots, timeZone) {
  if (!Array.isArray(slots) || !slots.length) return {reason:'unclear'};
  const t = fold(replyExcerpt(snippet)).replace(/\b([ap])\.\s*m\.?/g, '$1m');
  if (!t) return {reason:'unclear'};
  if (/\b(?:no|nunca|tampoco|imposible|rechazo|rechazad[oa]|cancel[ae]r?|excepto|salvo)\b/.test(t)) return {reason:'negative'};
  if (/\b(?:no (?:puedo|podre|podemos|me (?:queda|sirve|funciona|viene|es posible))|ninguna|ninguno|otro dia|otra (?:hora|fecha|semana)|mejor (?:otro|otra)|reprogram|cancel|tal vez|quizas|no se|no estoy segur)/.test(t)) return {reason:'negative'};
  if (/\b(?:ambas|cualquiera|cualquier (?:hora|opcion)|las dos (?:opciones|me|estan|son|funcionan|sirven|van)|me da igual|lo que (?:tu|usted) (?:diga|prefiera))\b/.test(t)) return {reason:'ambiguous'};
  if (/\?/.test(t) && !/\b(?:esta bien|perfecto|me sirve|me queda|confirmo|de acuerdo|ok|vale)\b/.test(t)) return {reason:'unclear'};
  const locals = slots.map(slot => localParts(slot.start, timeZone));
  const all = slots.map((_, i) => i);
  const signals = [];
  const ordinal = [...t.matchAll(/\bopcion (?:numero |#)?(1|2|uno|dos)\b|\b(?:la|el) (primer[ao]?|segund[ao])\b|^#?(1|2)[.!]?$/g)]
    .map(m => /^(?:1|uno|primer[ao]?)$/.test(m[1] || m[2] || m[3]) ? 0 : 1);
  if (ordinal.length) signals.push(new Set(ordinal.filter(i => i < slots.length)));
  const times = mentionedTimes(t);
  if (times === null) return {reason:'unclear'};
  if (times.length) {
    const hits = new Set();
    for (const values of times) locals.forEach((local, i) => { if (values.has(local.minutes)) hits.add(i); });
    signals.push(hits);
  }
  const days = Object.values(englishDays).filter(day => new RegExp('\\b' + day + '\\b').test(t));
  if (days.length) signals.push(new Set(all.filter(i => days.includes(englishDays[locals[i].weekday]))));
  if (!signals.length) return {reason:'unclear'};
  let result = new Set(all);
  for (const signal of signals) {
    if (!signal.size) return {reason:'nomatch'};
    result = new Set([...result].filter(i => signal.has(i)));
  }
  if (result.size !== 1) return {reason:result.size ? 'ambiguous' : 'nomatch'};
  // Every distinct mentioned time must agree with the chosen option.
  if (times.length > 1 && !times.every(values => values.has(locals[[...result][0]].minutes))) return {reason:'ambiguous'};
  return {index:[...result][0]};
}

// Direct offers and the interactive search show the same slots for the same date and availability.
export const offerSlots = availableSlots;

export function slotLabel(iso, timeZone) {
  return new Intl.DateTimeFormat('es-US', {timeZone, weekday:'long', day:'numeric', month:'long', hour:'numeric', minute:'2-digit', hour12:true})
    .format(new Date(iso));
}

export function offerBody({name, labels, timeZone}) {
  const greeting = name ? 'Hola, ' + name.split(/\s+/)[0] + ':' : 'Hola:';
  return greeting + '\n\nGracias por tu mensaje. Para la demostración tengo libres estos horarios (hora de ' + timeZone + '):\n\n' +
    labels.map((label, i) => (i + 1) + '. ' + label).join('\n') +
    '\n\nRespóndeme con la opción que prefieras y te la confirmo.';
}

// An edit may change the wording, never the options the prospect will choose from.
export function offerKeepsSlots(offer, body) {
  if (typeof body !== 'string' || offer?.labels?.length !== 2 || !offer.timeZone) return false;
  const block = '(hora de ' + offer.timeZone + '):\n\n' +
    offer.labels.map((label, i) => (i + 1) + '. ' + label).join('\n');
  if (!body.includes(block)) return false;
  const surrounding = fold(body.replace(block, ''));
  return !offer.labels.some(label => surrounding.includes(fold(label))) &&
    !/\b(?:lunes|martes|miercoles|jueves|viernes|sabado|domingo|manana|pasado manana|mediodia|medianoche|a las?|opcion [123]|\d{1,2}:\d{2}|\d{1,2} ?(?:am|pm))\b/.test(surrounding);
}
