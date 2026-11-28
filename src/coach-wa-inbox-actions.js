import { calendarOf, CALL_RESULTS } from './coach-my-day.js';

function normalized(message) {
  return String(message || '').normalize('NFD').replace(/\p{M}/gu, '').toLowerCase()
    .replace(/[¿?¡!.,]/g, ' ').replace(/\s+/g, ' ').trim()
    .replace(/^(?:hola )?(?:coach )?jorge\s+/, '');
}

const help = 'Puedes decir «Qué sigue», «A quién le marco», «Qué quedó de mi demo» o «Registra que no contestó».';
const appLink = 'https://totalelitecoaching.com/coach/app/';

// A US number as +1XXXXXXXXXX (WhatsApp makes it tappable); anything else is left out rather than guessed.
function dialable(phone) {
  const digits = String(phone || '').replace(/\D/g, '');
  if (digits.length === 10) return '+1' + digits;
  if (digits.length === 11 && digits.startsWith('1')) return '+' + digits;
  return String(phone || '').trim().startsWith('+') && digits.length >= 8 && digits.length <= 15 ? '+' + digits : '';
}

export async function callReply(input, { actor, binding, myDay, now, saveCall }) {
  const phrase = normalized(input);
  const lookup = /^(?:a quien (?:le )?(?:marco|llamo)(?: primero)?|(?:dame |dime )?la (?:primera|siguiente|proxima) llamada|siguiente llamada)$/.test(phrase);
  // Anything after the result («volver a llamar el martes») is not understood yet: the reply says it was not saved.
  const words = phrase.match(/^(?:(?:registra|anota|pon) (?:que )?)?(?:hay que )?(no contesto|no respondio|volver a llamar|no le interesa|se saco cita|saco cita)(?: (.{1,200}))?$/);
  if (!lookup && !words) return '';
  if (!myDay) return 'La lista de llamadas no está disponible ahora. Puedes revisarla en Mi día.';

  const { day } = calendarOf(now, actor.zone);
  if (lookup) {
    const more = /\bsiguiente\b/.test(phrase);
    const { people } = await myDay.callBlock(actor, { more, limit: 1, skipDone: true });
    const next = people[0];
    if (!next) return 'Tu lista de telemarketing no tiene otra llamada pendiente.';
    const saved = await saveCall(next.ref, day);
    if (!saved?.matchedCount) return 'Tu WhatsApp ya no está vinculado. Revisa Mis conexiones.';
    // Agustín approved sending the number (28-nov-2026) so the seller can tap it and call from WhatsApp.
    const number = dialable(next.phone);
    return next.name + (number ? ', ' + number : '') + '. ' + next.reason + ' Cuando termines, dime cómo te fue.';
  }

  if (!binding.nextCall || binding.callDay !== day) return 'Primero dime «A quién le marco» para identificar la llamada de hoy.';
  const result = ({
    'no contesto': 'no_contesto', 'no respondio': 'no_contesto', 'volver a llamar': 'volver',
    'no le interesa': 'no_interesa', 'se saco cita': 'cita', 'saco cita': 'cita'
  })[words[1]];
  const saved = await myDay.callResult(actor, { ref: binding.nextCall, result });
  if (!saved.recorded) return 'Esa llamada ya no está en tu lista de hoy. Dime «A quién le marco» para volver a abrirla.';
  // Say exactly what changed: only the Mi día call count. The Hoja TEC, the CRM and the calendar stay as they were.
  const extra = words[2] ? ' Lo que dijiste después del resultado (fecha, hora o nota) no quedó guardado.' : '';
  const base = 'Anoté en tu conteo de Mi día: ' + CALL_RESULTS[result] + '.';
  if (result === 'cita') return base + ' La cita no quedó agendada ni cambió tu Hoja TEC: agéndala en el Coach con día y hora. ' + appLink + extra;
  if (result === 'volver') return base + ' No quedó un recordatorio con fecha ni cambió tu Hoja TEC: anota cuándo volver a llamar en el Coach. ' + appLink + extra;
  return base + ' Tu Hoja TEC y el CRM no cambiaron.' + extra + ' Dime «A quién le marco» para la siguiente.';
}

// Appointment changes need the Coach's confirm step; WhatsApp must not pretend it did them.
const appointmentChange = /^(?:ya )?(?:(?:me |le |nos )?(?:confirmo|confirmaron|cancelo|cancelaron|reagendo|reagendaron|movio|movieron|cambio|cambiaron|pospuso)|(?:confirma|cancela|reagenda|mueve|cambia|agenda|agendame|apunta) (?:la |una |mi )?cita)\b/;

export async function inboxReply(input, { actor, myDay, latestDemo }) {
  const phrase = normalized(input);
  if (/^(?:que (?:quedo|paso|se guardo) (?:de|en) (?:mi|la|mi ultima|la ultima) (?:demo|demostracion)|(?:dame |dime )?el resumen de (?:mi|la) (?:demo|demostracion)|mi ultima demo)$/.test(phrase)) {
    const demo = await latestDemo(actor.owner);
    if (!demo) return 'Todavía no encuentro una demo procesada en tu cuenta.';
    const { autoCloseReceipt: receipt, visitQuestions } = demo;
    if (!receipt) return 'Tu última demo tiene un reporte en Mis demostraciones. ' + appLink;
    // Each line names where the data went, and what did not get saved is said too, never left out.
    const notes = [];
    const count = value => Math.max(0, Number(value) || 0);
    if (receipt.survey?.state === 'saved') notes.push('Encuesta guardada en el CRM.');
    const referrals = receipt.referrals || {};
    if (count(referrals.saved)) notes.push('Referidos del 4 en 14 en tu Hoja TEC: ' + count(referrals.saved) + '.');
    if (count(referrals.duplicates)) notes.push('Ya estaban en tu hoja: ' + count(referrals.duplicates) + '.');
    if (count(referrals.pending)) notes.push('Referidos sin guardar, por revisar: ' + count(referrals.pending) + '.');
    if (referrals.state === 'connection' || referrals.state === 'error') notes.push('No pude escribir en tu Hoja TEC.');
    const appointment = receipt.appointment?.state;
    if (appointment === 'saved') notes.push('Cita de seguimiento en tu calendario «Seguimientos TEC».');
    else if (appointment === 'busy') notes.push('La cita de seguimiento no se guardó: ese horario ya estaba ocupado.');
    else if (appointment === 'time') notes.push('La cita de seguimiento no se guardó: falta confirmar el día u hora.');
    else if (appointment && !['none', 'pending'].includes(appointment)) notes.push('La cita de seguimiento no se guardó en tu calendario.');
    const instant = Array.isArray(receipt.referralAppointments) ? receipt.referralAppointments : [];
    const instantSaved = instant.filter(item => item?.state === 'saved').length;
    if (instantSaved) notes.push('Citas de referidos en tu calendario: ' + instantSaved + '.');
    if (instant.length > instantSaved) notes.push('Citas de referidos sin guardar: ' + (instant.length - instantSaved) + '.');
    if (count(visitQuestions)) notes.push('Faltan ' + count(visitQuestions) + ' datos por confirmar en Mis demostraciones.');
    return notes.length ? notes.join(' ') + ' Detalle: ' + appLink : 'Revisa el detalle de tu última demo en Mis demostraciones. ' + appLink;
  }
  if (/^(?:que sigue|que tengo hoy|(?:dame |dime )?mi agenda(?: de hoy)?|mis citas(?: de hoy)?|(?:cual es )?mi proxima cita)$/.test(phrase)) {
    if (!myDay) return 'La agenda no está disponible ahora. Puedes revisarla en Mi día.';
    const { citas } = await myDay.state(actor);
    const events = [...citas.events, ...citas.agenda.map(it => ({ title: it.client, time: it.time }))]
      .filter(it => it.title).sort((a, b) => String(a.time || '99').localeCompare(String(b.time || '99')));
    const today = events.slice(0, 3).map(it => (it.time ? it.time + ' · ' : '') + it.title);
    if (today.length) return 'En tu agenda de hoy: ' + today.join('; ') + '.';
    const next = citas.upcoming[0];
    if (next) return 'Tu próxima cita es ' + next.title + ' · ' + next.when + '.';
    return citas.notes?.length ? 'No pude consultar toda tu agenda. Revísala en Mi día. ' + appLink : 'No encontré citas próximas. Si vas a llamar, dime «A quién le marco».';
  }
  if (appointmentChange.test(phrase)) {
    return 'Todavía no puedo confirmar, mover ni cancelar citas desde WhatsApp, así que no cambié nada. Hazlo en el Coach: ' + appLink;
  }
  return help;
}
