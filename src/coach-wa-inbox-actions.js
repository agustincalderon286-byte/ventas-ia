import { calendarOf, CALL_RESULTS } from './coach-my-day.js';

function normalized(message) {
  return String(message || '').normalize('NFD').replace(/\p{M}/gu, '').toLowerCase()
    .replace(/[¿?¡!.,]/g, ' ').replace(/\s+/g, ' ').trim()
    .replace(/^(?:hola )?(?:coach )?jorge\s+/, '');
}

const help = 'Puedes decir «Qué sigue», «A quién le marco», «Qué quedó de mi demo» o «Registra que no contestó».';
const appLink = 'https://totalelitecoaching.com/coach/app/';

export async function callReply(input, { actor, binding, myDay, now, saveCall }) {
  const phrase = normalized(input);
  const lookup = /^(?:a quien (?:le )?(?:marco|llamo)(?: primero)?|(?:dame |dime )?la (?:primera|siguiente|proxima) llamada|siguiente llamada)$/.test(phrase);
  const words = phrase.match(/^(?:(?:registra|anota) (?:que )?)?(no contesto|no respondio|volver a llamar|no le interesa|se saco cita|saco cita)$/);
  if (!lookup && !words) return '';
  if (!myDay) return 'La lista de llamadas no está disponible ahora. Puedes revisarla en Mi día.';

  const { day } = calendarOf(now, actor.zone);
  if (lookup) {
    const more = /\bsiguiente\b/.test(phrase);
    const { people } = await myDay.callBlock(actor, { more, limit: 1 });
    const next = people[0];
    if (!next) return 'Tu lista de telemarketing no tiene otra llamada pendiente.';
    const saved = await saveCall(next.ref, day);
    if (!saved?.matchedCount) return 'Tu WhatsApp ya no está vinculado. Revisa Mis conexiones.';
    return next.name + '. ' + next.reason + ' Cuando termines, dime cómo te fue.';
  }

  if (!binding.nextCall || binding.callDay !== day) return 'Primero dime «A quién le marco» para identificar la llamada de hoy.';
  const result = ({
    'no contesto': 'no_contesto', 'no respondio': 'no_contesto', 'volver a llamar': 'volver',
    'no le interesa': 'no_interesa', 'se saco cita': 'cita', 'saco cita': 'cita'
  })[words[1]];
  const saved = await myDay.callResult(actor, { ref: binding.nextCall, result });
  if (!saved.recorded) return 'Esa llamada ya no está en tu lista de hoy. Dime «A quién le marco» para volver a abrirla.';
  return 'Resultado guardado en Mi día: ' + CALL_RESULTS[result] + '.';
}

export async function inboxReply(input, { actor, myDay, latestDemo }) {
  const phrase = normalized(input);
  if (/^(?:que (?:quedo|paso|se guardo) (?:de|en) (?:mi|la|mi ultima|la ultima) (?:demo|demostracion)|(?:dame |dime )?el resumen de (?:mi|la) (?:demo|demostracion)|mi ultima demo)$/.test(phrase)) {
    const demo = await latestDemo(actor.owner);
    if (!demo) return 'Todavía no encuentro una demo procesada en tu cuenta.';
    const { autoCloseReceipt: receipt, visitQuestions } = demo;
    if (!receipt) return 'Tu última demo tiene un reporte en Mis demostraciones. ' + appLink;
    const notes = [];
    if (receipt.survey?.state === 'saved') notes.push('Encuesta guardada.');
    if (receipt.referrals?.saved > 0) notes.push('Referidos guardados en el 4 en 14: ' + Number(receipt.referrals.saved) + '.');
    if (receipt.appointment?.state === 'saved') notes.push('Cita de seguimiento guardada.');
    if (visitQuestions > 0) notes.push('Faltan ' + Number(visitQuestions) + ' datos por confirmar.');
    return notes.join(' ') || 'Revisa el detalle de tu última demo en Mis demostraciones. ' + appLink;
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
  return help;
}
