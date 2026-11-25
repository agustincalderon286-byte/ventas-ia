import { createHash } from 'node:crypto';
import { followupTime, readFollowup, validTimeZone } from './coach-followup-input.js';
import { sheetsPhoneKey } from './coach-sheets-transfer.js';

const strings = (...fields) => Object.fromEntries(fields.map(field => [field, { type: 'string' }]));
const evidenceShape = { segment: { type: 'integer' }, quote: { type: 'string' } };
const strict = properties => ({ type: 'object', additionalProperties: false, required: Object.keys(properties), properties });
export const autoDemoCloseSchema = strict({
  referrals: { type: 'array', items: strict({ ...strings('fullName', 'phone'), ...evidenceShape }) },
  incompleteReferrals: { type: 'array', items: strict(evidenceShape) },
  followUp: { anyOf: [strict({ ...strings('when'), ...evidenceShape }), { type: 'null' }] }
});
export const autoDemoCloseInstructions = [
  'En autoClose prepara candidatos a guardado automático. Transcribe literalmente y deja fuera cualquier dato dudoso, incompleto, contradictorio o solo sugerido.',
  'Cada referido requiere nombre completo y teléfono completo dictado para ESE referido del 4 en 14. Ambos deben figurar juntos, con el contexto explícito de referencia, en la misma cita literal continua. Excluye al anfitrión, al vendedor, nombres hipotéticos y ejemplos. Devuelve hasta 11.',
  'En incompleteReferrals cita las referencias de esta visita que sí mencionaron pero quedaron incompletas o dudosas. Incluye una sola cita por referido y no repitas los que ya devolviste completos.',
  'En followUp devuelve únicamente un seguimiento aceptado expresamente: una cita literal continua con el acuerdo y una fecha y hora inequívocas. En when copia la frase exacta con el día y la hora. No conviertas fechas ni inventes horas. Si luego cambiaron o retiraron el acuerdo, usa el último acuerdo claro o devuelve null.',
  'Las citas de evidencia pertenecen al segmento que indicas. Mantén nombres y teléfonos de referidos únicamente en autoClose. El resumen de entrenamiento nunca los incluye.'
].join('\n');

const fold = text => String(text || '').normalize('NFD').replace(/\p{M}/gu, '').replace(/\s+/g, ' ').trim().toLowerCase();
const digest = input => createHash('sha256').update(JSON.stringify(input)).digest('hex').slice(0, 32);
const personName = /^[\p{L}][\p{L}\p{M} .’'\-]{1,110}$/u;
const definiteReferral = /\b(?:referid[oa]s?|referencias?|te recomiendo|te refiero|te doy (?:el (?:contacto|numero)|una referencia))\b/;
const uncertainty = /\b(?:creo|quizas?|tal vez|a lo mejor|no estoy segur[oa]|no recuerdo|por confirmar|por ejemplo|si (?:puede|pudiera|se puede|estoy|esta|me da|me queda|te queda)|todavia no|luego te confirmo)\b/;
const agreement = /\b(?:de acuerdo|acordamos|quedamos|queda (?:confirmado|confirmada|para)|si[, ]+(?:puedo|claro|esta bien|el |a las? |me queda|llamame|pasas?))\b/;
const followUpAction = /\b(?:llam\w+|seguimiento|cita|demo|visit\w*|marc\w+|nos vemos|regres\w+|vuelv\w+|pasas|venir|vienes|escrib\w+)\b/;
const changesOrAlternatives = /\b(?:mejor (?:el |el dia |a las? |manana)|pero (?:a las? |no |solo |hasta |el )|en vez de|(?:o|u) (?:el |la |a las? |manana|pasado|lunes|martes|miercoles|jueves|viernes|sabado|domingo|\d)|cance(?:lo|la|lar|lamos|lado)|no (?:llam\w+|voy a poder|puedo|confirm\w+|agend\w+|qued\w+|queda)|no me (?:llam\w+|queda)|no (?:te|me) (?:refier\w+|recomiend\w+))\b/;
const visiblePhones = /(?<!\d)(?:\+\d{1,3}[\s.-]*)?(?:\(\d{3}\)|\d{3})[\s.-]*\d{3}[\s.-]*\d{4}(?!\d)|(?<!\d)\+\d(?:[ .-]?\d){7,14}(?!\d)/g;
const cleanName = value => typeof value === 'string' && personName.test(value.trim()) && fold(value).split(' ').length >= 2 ? value.trim() : '';

function confirmsFollowUp(quote) {
  const question = quote.lastIndexOf('?');
  if (question < 0) return agreement.test(quote);
  return /^(?:si\b|de acuerdo\b|esta bien\b|perfecto\b)/.test(quote.slice(question + 1).trim());
}

export function verifyAutoDemoClose(raw, chunks) {
  const segments = new Map(chunks.map(chunk => [chunk.index + 1, fold(chunk.text)]));
  function evidence(candidate, review = false) {
    const quote = fold(candidate?.quote);
    const text = segments.get(candidate?.segment);
    const occurrence = text && quote ? text.indexOf(quote) : -1;
    if (quote.length < 12 || quote.length > 330 || occurrence < 0) return '';
    const before = text?.[occurrence - 1] || '';
    const after = text?.[occurrence + quote.length] || '';
    if (/[\p{L}\d]/u.test(before) || /[\p{L}\d]/u.test(after)) return '';
    if (review) return quote;
    if (uncertainty.test(quote) || changesOrAlternatives.test(quote)) return '';
    // Look at the surrounding clause so a candidate cannot crop off a qualifier or negation.
    const left = text.slice(Math.max(0, occurrence - 70), occurrence).split(/[.!?]/).pop();
    const right = text.slice(occurrence + quote.length, occurrence + quote.length + 70).split(/[.!?]/)[0];
    return uncertainty.test(left + ' ' + right) || changesOrAlternatives.test(left + ' ' + right) ? '' : quote;
  }
  const supplied = Array.isArray(raw?.referrals) ? raw.referrals.slice(0, 11) : [];
  const verified = supplied.map(row => {
    const fullName = cleanName(row?.fullName), phone = sheetsPhoneKey(row?.phone), quote = evidence(row);
    const names = fold(fullName);
    const at = quote.indexOf(names);
    const exactName = at >= 0 && !/[\p{L}\d]/u.test(quote[at - 1] || '') && !/[\p{L}\d]/u.test(quote[at + names.length] || '');
    const phones = [...quote.matchAll(visiblePhones)].map(hit => sheetsPhoneKey(hit[0])).filter(Boolean);
    return fullName && phone && exactName && definiteReferral.test(quote) && phones.length === 1 && phones[0] === phone ? { fullName, phone } : null;
  });
  const unique = verified.filter(row => row && verified.filter(other => other && (other.phone === row.phone || fold(other.fullName) === fold(row.fullName))).length === 1);
  const incomplete = new Set();
  if (Array.isArray(raw?.incompleteReferrals)) {
    for (const item of raw.incompleteReferrals.slice(0, 11 - supplied.length)) {
      const excerpt = evidence(item, true);
      if (definiteReferral.test(excerpt)) incomplete.add(excerpt);
    }
  }
  const quote = evidence(raw?.followUp), when = typeof raw?.followUp?.when === 'string' ? raw.followUp.when.trim() : '';
  const followUp = quote && when && when.length <= 120 && confirmsFollowUp(quote) && followUpAction.test(quote) &&
    quote.includes(fold(when)) ? { when, segment: raw.followUp.segment } : null;
  return { referrals: unique, pendingReferrals: supplied.length - unique.length + incomplete.size, followUp };
}

function whenFor(analysis, candidate, now) {
  const zone = validTimeZone(analysis.autoClose?.zone);
  if (!zone || !candidate?.when || !Number.isFinite(Number(new Date(analysis.startedAt)))) return null;
  const offsets = analysis.chunks.filter(chunk => chunk.index < candidate.segment - 1)
    .reduce((sum, chunk) => sum + Number(chunk.durationSeconds || 0), 0);
  const anchor = Number(new Date(analysis.startedAt)) + offsets * 1000;
  const phrase = fold(candidate.when);
  if (/\b(?:antes de|despues de|entre|alrededor|cerca de|aproximadamente|la semana que viene|el otro|o|u)\b/.test(phrase)) return null;
  const parsed = readFollowup(candidate.when, {}, { timeZone: zone, now: anchor });
  const result = followupTime(parsed.state, now);
  if (parsed.question || result.question || Date.parse(result.start) - now > 90 * 86400_000) return null;
  return { ...result, timeZone: parsed.state.timeZone, reminderMinutes: parsed.state.reminderMinutes === undefined ? 10 : parsed.state.reminderMinutes };
}

export function createAutoDemoClose({ calendar, followups, sheets, transfer, active, now = Date.now }) {
  async function saveCalendar(analysis, facts, contact, partial) {
    if (!facts.followUp) return { state: 'pending' };
    const when = !partial && whenFor(analysis, facts.followUp, now());
    if (!when) return { state: 'time' };
    if (!await active(analysis)) return { state: 'stopped' };
    const owner = analysis.ownerUserId;
    const connection = await calendar.getConnection(owner);
    if (!connection?.calendarId || !connection.availabilityEnabled || connection.grant !== analysis.autoClose.calendarGrant) return { state: 'connection' };
    const eventId = digest([String(owner), analysis.closeVisitId, 'demo-followup']);
    const operationId = 'demo-' + eventId;
    const name = contact.name || 'Visita TEC';
    const event = { owner, grant: connection.grant, eventId, operationId, ...when, summary: 'Seguimiento · ' + name,
      mode: 'create', checkAvailability: true, retryGoogle: true };
    if (!await active(analysis)) return { state: 'stopped' };
    const saved = await calendar.saveEvent(event);
    await followups.rememberEvent({ owner, calendarId: connection.calendarId }, {
      ...when, name, summary: event.summary, action: 'seguimiento', operationId, eventId,
      htmlLink: saved.htmlLink || '', etag: saved.etag || ''
    }).catch(() => console.error('No se pudo indexar una cita automática de demostración.'));
    return { state: 'saved', start: when.start, timeZone: when.timeZone, htmlLink: saved.htmlLink || '' };
  }

  async function saveReferrals(analysis, facts, contact) {
    const pending = facts.pendingReferrals;
    if (!facts.referrals.length) return { state: pending ? 'pending' : 'none', saved: 0, pending };
    const total = pending + facts.referrals.length;
    if (!contact.name) return { state: 'host', saved: 0, pending: total };
    if (!await active(analysis)) return { state: 'stopped', saved: 0, pending: total };
    const owner = analysis.ownerUserId;
    const destination = await sheets.getConnection(owner);
    if (!destination?.grant || destination.grant !== analysis.autoClose.sheetsGrant) return { state: 'connection', saved: 0, pending: total };
    const { token, ...book } = await sheets.access(owner);
    if (book.grant !== destination.grant || book.spreadsheetId !== destination.spreadsheetId) return { state: 'connection', saved: 0, pending: total };
    const ctx = { owner, session: 'demo:' + String(analysis._id), grant: book.grant, workbook: book, access: token,
      sellerName: analysis.ownerName || '', isCurrent: async () => await active(analysis) && await sheets.current(owner, book.grant) };
    const draft = await transfer.preview(ctx, { kind: 'referrals', hostName: contact.name, hostPhone: contact.phone || '', referrals: facts.referrals });
    const selected = draft.rows.filter(row => row.status === 'new').map(row => row.index);
    const duplicates = draft.rows.filter(row => row.status === 'duplicate').length;
    const review = pending + draft.rows.length - selected.length - duplicates;
    const result = selected.length ? await transfer.confirm(ctx, { previewId: draft.previewId, selected }) : { written: 0, sheetTitle: draft.sheetTitle };
    return { state: 'checked', saved: result.written, pending: review, duplicates, sheetTitle: result.sheetTitle, file: book.spreadsheetId };
  }

  return async function save(analysis, facts, contact, { partial = false, followUpMentioned = false } = {}) {
    const [appointment, referrals] = await Promise.allSettled([
      facts.followUp || followUpMentioned ? saveCalendar(analysis, facts, contact, partial) : null,
      saveReferrals(analysis, facts, contact)
    ]);
    return {
      appointment: appointment.status === 'fulfilled' ? appointment.value : { state: appointment.reason?.slotBusy ? 'busy' : 'error' },
      referrals: referrals.status === 'fulfilled' ? referrals.value : { state: 'error', saved: 0,
        pending: facts.pendingReferrals + facts.referrals.length }
    };
  };
}
