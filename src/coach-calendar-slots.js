import {resolveInstant} from './coach-followup-input.js';

const minute = 60000;
const fold = value => String(value).toLowerCase().normalize('NFD').replace(/\p{M}/gu,'').trim();
const boundsHelp = '¿Entre qué horas quieres buscar? Puedes escribir «de 14:00 a 18:00», «por la mañana» o «por la tarde».';
const periodHours = {manana:[9,12],tarde:[12,19],noche:[19,22]};
const time = '(\\d{1,2})(?::(\\d{2}))?\\s*(am|pm)?';
const range = new RegExp('\\b(?:entre|de|desde) (?:las? )?'+time+'\\s+(?:y|a|hasta) (?:las? )?'+time+'\\b');
const clockMinutes = (hour,minuteText,meridian) => {
  const h = Number(hour), m = Number(minuteText || 0);
  if (h>23 || m>59 || (meridian && (h<1 || h>12))) return null;
  if (!meridian && !minuteText && h>=1 && h<=12) return null;
  return (meridian ? h%12 + (meridian==='pm'?12:0) : h)*60 + m;
};

export function readSlotHours(text, previous) {
  const t = fold(text).replace(/\b([ap])\.\s*m\.?/g,'$1m');
  const span = range.exec(t);
  if (span) {
    const from = clockMinutes(...span.slice(1,4)), to = clockMinutes(...span.slice(4,7));
    if (from==null || to==null || from>=to) return {question:boundsHelp};
    return {hours:[from,to]};
  }
  // A partly understood time constraint must not turn into a whole-day search.
  if (/\b(?:despues de|antes de|a partir de|entre las?|a las?|desde las?|hasta las?|de las?|mediodia|medianoche)\b|\b\d{1,2}(?::\d\d|\s*[ap]m)\b/.test(t)) return {question:boundsHelp};
  for (const [name,hours] of Object.entries(periodHours)) {
    if (t === name && name !== 'manana' || new RegExp('\\b(?:(?:por|en|de) la |esta )'+name+'\\b').test(t)) {
      return {hours:hours.map(h=>h*60)};
    }
  }
  return previous ? {hours:previous} : {};
}

export function slotBounds(state) {
  if (!state.timeZone) return {question:'¿En qué zona horaria buscas la demo? Por ejemplo, America/Chicago.'};
  if (state.day==null) return {question:'¿Para qué día buscas horarios? Puedes decir «mañana», un día de la semana o una fecha como 2027-01-15.'};
  const [from,to] = state.slotHours || [9*60,19*60];
  const instant = minutes => resolveInstant(state.day,Math.floor(minutes/60),minutes%60,state.timeZone);
  const start=instant(from), end=instant(to);
  if (start==null || end==null || start>=end) return {question:'Ese intervalo coincide con un cambio de horario. Dime otro rango de horas.'};
  return {start:new Date(start).toISOString(),end:new Date(end).toISOString()};
}

// Offer non-overlapping intervals, including gaps that do not begin on a quarter hour.
export function availableSlots({start,end}, busy, duration, now) {
  const length=duration*minute;
  if (!Number.isSafeInteger(duration) || duration<=0) throw new Error('Duración inválida');
  const endMs=Date.parse(end);
  let cursor=Math.max(Date.parse(start), Math.floor(now/minute)*minute + minute);
  const windows=busy.map(block=>{
    const a=Date.parse(block.start), b=Date.parse(block.end);
    if (!Number.isFinite(a) || !Number.isFinite(b) || b<=a) throw new Error('Respuesta de disponibilidad incompleta');
    return [a,b];
  }).sort((a,b)=>a[0]-b[0]);
  const slots=[];
  for (const [from,to] of [...windows,[endMs,endMs]]) {
    while (slots.length<2 && cursor+length<=Math.min(from,endMs)) {
      slots.push({start:new Date(cursor).toISOString(),end:new Date(cursor+length).toISOString()});
      cursor+=length;
    }
    if (slots.length===2) break;
    cursor=Math.max(cursor,Math.ceil(to/minute)*minute);
  }
  return slots;
}

export function slotChoice(text) {
  const t=fold(text).replace(/^[¡¿]|[.!?]$/g,'').replace(/^si,?\s+/,'').replace(/,? por favor$/,'');
  const answer=/^(?:(?:quiero|elijo|elige|escoge|selecciona|prefiero|tomo|me quedo con) )?(?:(?:el|la) )?(?:(?:opcion|horario) )?(?:numero )?(1|2|un[ao]|dos|primer[ao]?|segund[ao])(?: (?:opcion|horario))?$/.exec(t)?.[1];
  if (answer) return /^(?:1|un[ao]|primer[ao]?)$/.test(answer) ? 0 : 1;
  return -1;
}
