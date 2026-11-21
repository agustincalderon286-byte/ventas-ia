import { isGmailAddress } from './coach-hycite-gmail.js';

function normalized(text) {
  return typeof text === 'string' ? text.normalize('NFD').replace(/\p{M}/gu, '').toLowerCase().trim() : '';
}

const sales = /\b(?:ventas?|pedidos?|orden(?:es)?)\b/;
const report = /\b(?:resum(?:en|e|eme)|reporte|informe|recuento|total|cuantas|cantidad|resultados)\b/;
const consult = /\b(?:revisa(?:me|r)?|consulta(?:me|r)?|busca(?:me|r)?|checa(?:me|r)?|lee(?:me|r)?|cuenta(?:me)?|analiza(?:me|r)?|verifica(?:me|r)?)\b/;
const compose = /\b(?:prepara(?:me|r)?|redacta(?:me|r)?|escrib(?:e(?:me|le)?|ir)|haz(?:me)?|genera(?:me|r)?|crea(?:me|r)?)\b/;
const send = /\b(?:envi(?:a(?:me(?:l[oa]s?)?|le|l[oa]s?|sel[oa]s?|r(?:me|le|lo)?)?|es)|mand(?:a(?:me(?:l[oa]s?)?|le|l[oa]s?|sel[oa]s?|r(?:me|le|lo)?)?|es)|compart(?:e(?:me|lo|melo)?|ir(?:me|lo)?)|pasa(?:me|melo|lo))\b/;
const email = /\b(?:correos?|e-?mails?|gmail)\b/;
const recipientSelf = /\b(?:mandame(?:l[oa]s?)?|enviame(?:l[oa]s?)?|pasame(?:l[oa]s?)?|comparteme(?:l[oa]s?)?|a mi\s*(?:[,.;!?]|$)|(?:a|para|en) mi (?:propio )?(?:correo|e-?mail|gmail|buzon))\b/;
const nonAction = /\b(?:ejemplos?|hipotetic\w*|supongamos|suponiendo|simul(?:a|acion)|imagin(?:a|emos)|podria(?:s)?|haria(?:s)?|harias|pasaria|que (?:dirias|deberia decir|tendria que)|como (?:se |puedo |hago |puedes |haria)|si (?:te |yo |alguien |un vendedor )?(?:pidiera|pido|pide|dijera|digo|dice|quisiera|quisiese)|sin (?:enviar|mandar|revisar|consultar)|no (?:me |le |lo |las |se |quiero que |necesito que |vayas a )*(?:mand|envi|prepar|consult|revis|leas|lee|redact|gener|escri)\w*|nunca|evita)\b/;

export function classifySalesMailReport(text) {
  const value = normalized(text);
  if (!sales.test(value) || nonAction.test(value) || !send.test(value) && !compose.test(value)) return false;
  const addressedMail = email.test(value) || /\S@\S/.test(value) || recipientSelf.test(value);
  // A note asking a customer about *their* orders is not a report about the seller's figures.
  const own = /\b(?:mis|mi|nuestr[ao]s?)\s+(?:ventas?|pedidos?|orden(?:es)?|(?:resumen|reporte|informe)\s+de\s+(?:ventas?|pedidos?|orden(?:es)?))\b/.test(value);
  const hyCite = /\bhy[ -]?cite\b/.test(value);
  const ownReport = recipientSelf.test(value) && report.test(value) && !/\b(?:sus|tus) ventas|ventas (?:del equipo|de otro|de otra|de mi equipo)\b/.test(value);
  return addressedMail && (own || hyCite || ownReport) && (report.test(value) || consult.test(value));
}

function resolveRecipient(text, mailbox) {
  // Grab the whole token around each @. A malformed address must not yield a valid substring.
  const mentioned = text.split(/[\s,;<>«»“”"()]+/u).filter(part => part.includes('@'))
    .map(part => part.replace(/[.!?]+$/u, ''));
  const addresses = [...new Set(mentioned)];
  if (addresses.length > 1 || addresses.some(address => !isGmailAddress(address))) return null;
  const value = normalized(text);
  if (addresses.length === 1) {
    if (/\b(?:a mi|a mi correo|a mi gmail)\s+(?:y|e|tambien)\b/.test(value) && addresses[0].toLowerCase() !== String(mailbox).toLowerCase()) return null;
    return addresses[0];
  }
  return recipientSelf.test(value) && isGmailAddress(mailbox) ? mailbox : null;
}

const formatTime = new Intl.DateTimeFormat('es-US', {
  year:'numeric', month:'short', day:'numeric', hour:'numeric', minute:'2-digit', timeZone:'America/Chicago', timeZoneName:'short'
});

export function draftSalesReport(text, { snapshot, mailbox, days = 90, period, now = Date.now() }) {
  const to = resolveRecipient(typeof text === 'string' ? text : '', mailbox);
  if (!to) return {question:'¿Para qué dirección preparo el resumen de ventas? Escribe un solo correo o responde «a mi correo».'};
  const timestamp = Number(now);
  const start = period ? period.since * 1000 : timestamp - days * 86400000;
  const before = period ? period.before * 1000 : timestamp + 1;
  const calendarValid = period && Number.isSafeInteger(period.since) && Number.isSafeInteger(period.before) &&
    before > start && typeof period.label === 'string' && period.label.trim().length <= 80 && /^[\p{L}\p{N} ().-]+$/u.test(period.label);
  const daysValid = Number.isSafeInteger(days) && days >= 1 && days <= 90;
  if (!(period ? calendarValid : daysValid) || !Number.isFinite(timestamp) || start > timestamp ||
      start < timestamp - 90 * 86400000 || Number.isNaN(new Date(timestamp).getTime())) {
    return {question:'Elige un período dentro de los últimos 90 días para el resumen.'};
  }
  if (!Array.isArray(snapshot?.confirmed) || !Array.isArray(snapshot?.review)) {
    return {question:'No pude obtener los avisos de ventas. Vuelve a solicitar el resumen.'};
  }
  const end = Math.min(before, timestamp);
  const inPeriod = row => {
    const received = new Date(row?.date).getTime();
    return received >= start && received < before && received <= timestamp;
  };
  const confirmed = snapshot.confirmed.filter(inPeriod).length;
  const review = snapshot.review.filter(inPeriod).length;
  const body = [
    'Este es mi resumen de los avisos de Hy Cite recibidos por Gmail.',
    'Período del ' + formatTime.format(start) + ' al ' + formatTime.format(end) + '.',
    'Ventas confirmadas: ' + confirmed + '.',
    'Avisos que requieren revisión: ' + review + '.'
  ];
  if (snapshot.limited) body.push('El conteo es parcial. La consulta alcanzó el límite de 100 avisos.');
  const label = period ? period.label.trim() : days === 1 ? 'últimas 24 horas' : 'últimos ' + days + ' días';
  return {to, subject:'Resumen de ventas de Hy Cite · ' + label, body:body.join('\n\n')};
}
