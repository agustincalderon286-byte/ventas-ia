import { createHash } from 'node:crypto';
import { followupTime, readFollowup, validTimeZone } from './coach-followup-input.js';
import { sheetsPhoneKey } from './coach-sheets-transfer.js';
import { autoSurveyFields, autoSurveyInstructions, verifiedAutoSurvey } from './coach-demo-survey.js';
import { checkDemoFigures, demoQuoteMinute, figuresOf } from './coach-demo-figures.js';

const strings = (...fields) => Object.fromEntries(fields.map(field => [field, { type: 'string' }]));
const evidenceShape = { segment: { type: 'integer' }, quote: { type: 'string' } };
const strict = properties => ({ type: 'object', additionalProperties: false, required: Object.keys(properties), properties });
const notesSchema = { type:'array', items: strict({ ...strings('text'), ...evidenceShape }) };
export const autoDemoCloseSchema = strict({
  referrals: { type: 'array', items: strict({ ...strings('fullName', 'phone'), ...evidenceShape, notes:notesSchema }) },
  incompleteReferrals: { type: 'array', items: strict(evidenceShape) },
  survey: { type:'array', items:strict({ ...evidenceShape, field:{type:'string',enum:autoSurveyFields}, values:{type:'array',items:{type:'string'}} }) },
  products: { type:'array', items:strict({ ...strings('name'), ...evidenceShape, prices:notesSchema }) },
  followUp: { anyOf: [strict({ ...strings('when'), ...evidenceShape }), { type: 'null' }] },
  referralAppointments: { type: 'array', items: strict({ ...strings('fullName', 'when'), ...evidenceShape }) }
});
export const autoDemoCloseInstructions = [
  'En autoClose prepara candidatos a guardado automático. Transcribe literalmente y deja fuera cualquier dato dudoso, incompleto, contradictorio o solo sugerido.',
  'Cada referido requiere nombre completo y teléfono completo dictado para ESE referido del 4 en 14. Ambos deben figurar juntos, con el contexto explícito de referencia (por ejemplo «referido», «te recomiendo», «anota a…», «apunta a…» o «su número es…»), en la misma cita literal continua. Excluye al anfitrión, al vendedor, nombres hipotéticos y ejemplos. Devuelve hasta 11.',
  'En incompleteReferrals cita las referencias de esta visita que sí mencionaron pero quedaron incompletas o dudosas. Incluye una sola cita por referido y no repitas los que ya devolviste completos.',
  'En notes conserva TODAS las observaciones claras de ESE referido, una por elemento: estado civil (casado, soltero), vivienda (renta, es dueño), trabajo y horario, cuándo lo pueden llamar, la relación con el anfitrión, intereses u otras notas de contacto. Si una frase dice varias cosas («es soltera, renta y trabaja por la mañana») devuelve un elemento por cada una, con el mismo quote. En text copia el fragmento literal mínimo, incluyendo el no cuando corresponda. En quote copia la frase literal continua; puede ser la frase que sigue inmediatamente a la mención del referido aunque no repita su nombre, siempre que nadie más haya sido mencionado entre ambas. No guardes contraseñas, documentos, diagnósticos ni datos de cuentas en notas de referidos.',
  'En products incluye únicamente los productos por los que el cliente mostró interés. En quote cita literalmente ese interés con el nombre del producto. En prices copia los precios, anticipos o cuotas que discutieron para ese mismo producto, con monto y moneda o periodo si se dijo. Cada quote de precio debe identificar el producto y respaldar la cifra. Una cotización no prueba una compra.',
  'En followUp devuelve únicamente un seguimiento aceptado expresamente (por ejemplo «quedamos», «de acuerdo», «agéndame», «nos vemos» o un «sí» a la propuesta): una cita literal continua con el acuerdo, el día y la hora. En when copia la frase exacta con el día y la hora, aunque diga a la vez «mañana» y un día de la semana; TEC comprueba si coinciden. No conviertas fechas ni inventes horas. Si luego cambiaron o retiraron el acuerdo, usa el último acuerdo claro o devuelve null.',
  'En referralAppointments devuelve las citas instantáneas del 4 en 14: una cita (demo o visita) que un REFERIDO aceptó expresamente durante la visita, casi siempre por teléfono desde la casa del anfitrión. En fullName copia el nombre completo de ese referido tal como lo devolviste en referrals; en when la frase exacta con el día y la hora; en quote la frase literal continua con la propuesta y el sí. No incluyas aquí el seguimiento con el anfitrión. Devuelve hasta 3.',
  autoSurveyInstructions,
  'Las citas de evidencia pertenecen al segmento que indicas. Mantén nombres y teléfonos de referidos únicamente en autoClose. El resumen de entrenamiento nunca los incluye.'
].join('\n');

const fold = text => String(text || '').normalize('NFD').replace(/\p{M}/gu, '').replace(/\s+/g, ' ').trim().toLowerCase();
const digest = input => createHash('sha256').update(JSON.stringify(input)).digest('hex').slice(0, 32);
const personName = /^[\p{L}][\p{L}\p{M} .’'\-]{1,110}$/u;
// Dictating someone for the 4 en 14 («anota a…», «apunta…», «su número es…») is a referral as clear as saying the word.
const definiteReferral = /\b(?:referid[oa]s?|referencias?|te recomiendo|te refiero|te doy (?:el (?:contacto|numero)|una referencia)|anot(?:a|as|ale|alo|ala|e|en|ame)|apunt(?:a|as|ale|alo|ala|e|en|ame)|(?:4|cuatro) en (?:14|catorce)|su (?:numero|telefono|celular|cel)(?: es)?)\b/;
const uncertainty = /\b(?:creo|quizas?|tal vez|a lo mejor|no estoy segur[oa]|no recuerdo|por confirmar|por ejemplo|si (?:puede|pudiera|se puede|estoy|esta|me da|me queda|te queda)|todavia no|luego te confirmo)\b/;
const agreement = /\b(?:de acuerdo|acordamos|quedamos|queda (?:confirmado|confirmada|para)|si[, ]+(?:puedo|claro|esta bien|el |a las? |me queda|llamame|pasas?)|agend\w+|apunt\w+|nos vemos|te (?:veo|espero)|l[oa]s? (?:veo|espero)|me parece bien|esta bien|perfecto)\b/;
const followUpAction = /\b(?:llam\w+|seguimiento|cita|demo|visit\w*|marc\w+|nos vemos|regres\w+|vuelv\w+|pasas|venir|vienes|escrib\w+)\b/;
const changesOrAlternatives = /\b(?:mejor (?:el |el dia |a las? |manana)|pero (?:a las? |no |solo |hasta |el )|en vez de|(?:o|u) (?:el |la |a las? |manana|pasado|lunes|martes|miercoles|jueves|viernes|sabado|domingo|\d)|cance(?:lo|la|lar|lamos|lado)|no (?:llam\w+|voy a poder|puedo|confirm\w+|agend\w+|qued\w+|queda)|no me (?:llam\w+|queda)|no (?:te|me) (?:refier\w+|recomiend\w+))\b/;
const visiblePhones = /(?<!\d)(?:\+\d{1,3}[\s.-]*)?(?:\(\d{3}\)|\d{3})[\s.-]*\d{3}[\s.-]*\d{4}(?!\d)|(?<!\d)\+\d(?:[ .-]?\d){7,14}(?!\d)/g;
const cleanName = value => typeof value === 'string' && personName.test(value.trim()) && fold(value).split(' ').length >= 2 ? value.trim() : '';
const productInterest = /\b(?:me|nos|le) (?:interes\w+|gust\w+|sirv\w+|servir\w+|llam\w+ la atencion)\b|\b(?:quiero|queremos|prefiero|preferimos)\b/;
const productQuestion = /\b(?:te|le|les) (?:interes\w+|gust\w+|sirv\w+)|\b(?:prefier\w+|quiere\w*)\b/;
const rejectsInterest = /\b(?:no|nunca|jamas) (?:me |nos |le )?(?:interes\w+|gust\w+|sirv\w+|quier\w+|prefer\w+)\b/;
const privateNote = /@|https?:|\b(?:password|contrasena|seguro social|licencia de conducir|tarjeta|cuenta bancaria|diagnostico|medicamento|diabetes|hipertension|enfermedad)\b|(?:\d[ .()-]*){9,}/i;
const priceCue = /\$|\b(?:precio|vale|cuesta|cotiza\w*|mensual\w*|al mes|por mes|enganche|anticipo|total|dolares|pesos|usd|mxn)\b/;
const moneyText = /\d|\b(?:un|dos|tres|cuatro|cinco|seis|siete|ocho|nueve|diez|veint\w*|treinta|cuarenta|cincuenta|cien\w*|mil)\b/;

function wordAt(text, value) {
  if (!value) return false;
  const start = text.indexOf(value);
  return start >= 0 && !/[\p{L}\d]/u.test(text[start - 1] || '') && !/[\p{L}\d]/u.test(text[start + value.length] || '');
}

function literalNote(input, quote, max) {
  const text = typeof input === 'string' ? input.trim().replace(/\s+/g,' ') : '';
  return text && text.length <= max && !/[\p{Cc}<>]/u.test(text) && !privateNote.test(fold(text)) && wordAt(quote, fold(text)) ? text : '';
}

function lostNegation(quote, value) {
  const before = quote.slice(0,quote.indexOf(value));
  return /\b(?:no|nunca|tampoco)\s+(?:(?:me|le|es|esta|tiene|se)\s+){0,2}$/.test(before);
}

function showsInterest(quote, name) {
  if (rejectsInterest.test(quote)) return false;
  const question = quote.lastIndexOf('?');
  if (question < 0) return productInterest.test(quote);
  const ask = quote.slice(0,question), answer = quote.slice(question+1).trim();
  return wordAt(ask,name) && productQuestion.test(ask) && /^(?:si\b|me interesa\b|me gusta\b)/.test(answer) && !/\bpero\b/.test(answer);
}

function confirmsFollowUp(quote) {
  const question = quote.lastIndexOf('?');
  if (question < 0) return agreement.test(quote);
  return /^(?:si\b|de acuerdo\b|esta bien\b|perfecto\b)/.test(quote.slice(question + 1).trim());
}

// Kinds of referral notes the pilot measures. A note may belong to several.
const noteKinds = {
  estadoCivil: /\b(?:casad[oa]s?|solter[oa]s?|divorciad[oa]s?|viud[oa]s?|union libre|separad[oa]s?|tiene pareja|con pareja)\b/,
  vivienda: /\b(?:rent\w*|alquil\w*|duen[oa]s?|casa propia|propietari[oa]s?|apartamento|hipoteca|vive con|viven con)\b/,
  trabajo: /\b(?:trabaj\w*|emplead[oa]s?|turno|jornada|desemplead[oa]s?|jubilad[oa]s?|negocio propio)\b/,
  disponibilidad: /\b(?:llam\w*|marc\w*|despues de|antes de|por la (?:manana|tarde|noche)|fin(?:es)? de semana|horario|disponib\w*|en la (?:manana|tarde|noche))\b/,
  relacion: /\b(?:herman[oa]s?|prim[oa]s?|amig[oa]s?|companer[oa]s?|vecin[oa]s?|cunad[oa]s?|ti[oa]s?|suegr[oa]s?|hij[oa]s?|sobrin[oa]s?|mama|papa|madre|padre|novi[oa])\b/
};
const kindsOf = text => Object.keys(noteKinds).filter(kind => noteKinds[kind].test(fold(text)));
// A note that does not repeat the referral's name may only borrow it from the sentence right before,
// and never when it speaks in first or second person (then it is about the host or the seller).
const otherPerson = /\b(?:yo|me|mi|mis|nosotr[oa]s|nuestr[oa]s?|usted|ustedes|tu|tus|te|conmigo|contigo|(?:llam|marc|escrib|mand|avis|busc)\w*(?:me|nos))\b/;
const NEIGHBOR_CHARS = 320;

export function verifyAutoDemoClose(raw, chunks) {
  const segments = new Map(chunks.map(chunk => [chunk.index + 1, fold(chunk.text)]));
  const review = [];
  const minute = candidate => demoQuoteMinute(chunks, candidate?.segment, candidate?.quote);
  const figures = (kind, candidate, text) => checkDemoFigures(chunks, { segment: candidate?.segment, quote: candidate?.quote,
    kind, values: figuresOf(kind, text) });
  const note = (entry, check) => review.push({ ...entry, ...(check?.state === 'unchecked' && { verified: false }) });
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
  const referralNames = supplied.map(row => fold(cleanName(row?.fullName))).filter(Boolean);
  const referralPhones = supplied.map(row => sheetsPhoneKey(row?.phone)).filter(Boolean);

  // Where each referral is named (by name or phone) inside a segment.
  function mentions(text) {
    const found = [];
    for (const name of referralNames) for (let at = text.indexOf(name); at >= 0; at = text.indexOf(name, at + 1)) {
      if (!/[\p{L}\d]/u.test(text[at - 1] || '') && !/[\p{L}\d]/u.test(text[at + name.length] || '')) found.push({ at, end: at + name.length, name });
    }
    for (const hit of text.matchAll(visiblePhones)) {
      const phone = sheetsPhoneKey(hit[0]), index = referralPhones.indexOf(phone);
      if (index >= 0) found.push({ at: hit.index, end: hit.index + hit[0].length, name: fold(cleanName(supplied[index]?.fullName)) });
    }
    return found.sort((a, b) => a.at - b.at);
  }

  // A note quoted without the name still belongs to the referral named just before it in the
  // same segment, as long as nobody else is named in between.
  function neighborOf(item, detail, names) {
    const text = segments.get(item?.segment) || '';
    const at = text.indexOf(detail);
    // Without the name, only a plain personal observation can be attributed; never a price or product.
    if (at < 0 || otherPerson.test(detail) || priceCue.test(detail) || !kindsOf(detail).length) return false;
    const before = mentions(text).filter(hit => hit.end <= at);
    const last = before[before.length - 1];
    return Boolean(last && last.name === names && at - last.end <= NEIGHBOR_CHARS &&
      !mentions(text).some(hit => hit.name !== names && hit.at >= last.end && hit.at < at + detail.length) &&
      !anotherPerson(text.slice(last.end, at + detail.length)));
  }
  // A new referral cue or a phone in between means someone else may be the subject now, even if
  // Luna did not return that person.
  const phonePattern = new RegExp(visiblePhones.source);
  const anotherPerson = between => definiteReferral.test(between) || phonePattern.test(between);

  const verified = supplied.map(row => {
    const fullName = cleanName(row?.fullName), phone = sheetsPhoneKey(row?.phone), quote = evidence(row);
    const names = fold(fullName);
    const phones = [...quote.matchAll(visiblePhones)].map(hit => sheetsPhoneKey(hit[0])).filter(Boolean);
    if (!fullName || !phone || !wordAt(quote,names) || !definiteReferral.test(quote) || phones.length !== 1 || phones[0] !== phone) return null;
    const phoneCheck = figures('phone', row, phone);
    if (phoneCheck.state === 'doubt') {
      note({ field:'referral', label:fullName, state:'preguntar', reason:'Teléfono: ' + phoneCheck.reason, minute:minute(row) });
      return null;
    }
    const notes = new Map();
    for (const item of Array.isArray(row.notes) ? row.notes.slice(0,8) : []) {
      const detail = evidence(item), detailPhones = [...detail.matchAll(visiblePhones)].map(hit => sheetsPhoneKey(hit[0])).filter(Boolean);
      if (!detail || detailPhones.some(value=>value!==phone) || referralNames.some(other=>other!==names && wordAt(detail,other))) continue;
      if (!wordAt(detail,names) && !detailPhones.includes(phone) && !neighborOf(item, detail, names)) continue;
      const text = literalNote(item?.text, detail, 100);
      if (text && !lostNegation(detail,fold(text)) && !referralNames.some(name=>wordAt(fold(text),name))) {
        notes.set(fold(text), { text, minute:minute(item) });
      }
    }
    const saved = [...notes.values()];
    for (const item of saved) note({ field:'note', label:fullName, state:'guardar', text:item.text, kinds:kindsOf(item.text), minute:item.minute });
    note({ field:'referral', label:fullName, state:'guardar', minute:minute(row) }, phoneCheck);
    const joined = saved.map(item => item.text).join(' · ').slice(0,640);
    return { fullName, phone, ...(joined && { note: joined }) };
  });

  // Measure what the notes may have missed: observations right after a saved referral that no
  // saved note covers. They are listed for the seller and never written automatically.
  for (const [position, row] of supplied.entries()) {
    const kept = verified[position];
    if (!kept) continue;
    const names = fold(kept.fullName);
    const covered = phrase => review.some(item => item.field === 'note' && item.label === kept.fullName &&
      (phrase.includes(fold(item.text)) || fold(item.text).includes(phrase)));
    const text = segments.get(row.segment) || '';
    for (const hit of mentions(text).filter(item => item.name === names)) {
      const next = mentions(text).find(item => item.at > hit.end && item.name !== names);
      let window = text.slice(hit.end, Math.min(next ? next.at : text.length, hit.end + NEIGHBOR_CHARS));
      const stop = window.search(/\b(?:referid[oa]s?|referencias?)\b|(?:\d[\s().-]*){7,}/);
      if (stop >= 0) window = window.slice(0, stop);
      for (const clause of window.split(/[.;!?]|,|\by\b/)) {
        const phrase = clause.trim();
        if (phrase.length < 4 || otherPerson.test(phrase) || !kindsOf(phrase).length || covered(phrase) || privateNote.test(phrase)) continue;
        note({ field:'note', label:kept.fullName, state:'pendiente', text:phrase, kinds:kindsOf(phrase),
          reason:'Posible observación que no se guardó.' });
      }
    }
  }
  const unique = verified.filter(row => row && verified.filter(other => other && (other.phone === row.phone || fold(other.fullName) === fold(row.fullName))).length === 1);
  const incomplete = new Set();
  if (Array.isArray(raw?.incompleteReferrals)) {
    for (const item of raw.incompleteReferrals.slice(0, 11 - supplied.length)) {
      const excerpt = evidence(item, true);
      if (definiteReferral.test(excerpt)) incomplete.add(excerpt);
    }
  }
  const quote = evidence(raw?.followUp), when = typeof raw?.followUp?.when === 'string' ? raw.followUp.when.trim() : '';
  let followUp = quote && when && when.length <= 120 && confirmsFollowUp(quote) && followUpAction.test(quote) &&
    quote.includes(fold(when)) ? { when, segment: raw.followUp.segment } : null;
  if (followUp) {
    const check = figures('time', raw.followUp, when);
    if (check.state === 'doubt') {
      note({ field:'followUp', label:when, state:'preguntar', reason:'Fecha u hora: ' + check.reason, minute:minute(raw.followUp) });
      followUp = null;
    } else note({ field:'followUp', label:when, state:'guardar', minute:minute(raw.followUp) }, check);
  }
  const survey = verifiedAutoSurvey(raw?.survey, candidate => {
    const quoted = evidence(candidate);
    const values = Array.isArray(candidate?.values) ? candidate.values.filter(value => typeof value === 'string').join(' ') : '';
    if (!quoted || !figuresOf('amount', values).length) return quoted;
    const check = figures('amount', candidate, values);
    if (check.state !== 'doubt') return quoted;
    note({ field:'survey', label:candidate.field, state:'preguntar', text:values, reason:check.reason,
      ...(check.heard?.length && { heard:check.heard }), minute:minute(candidate) });
    return '';
  });
  for (const [field, value] of Object.entries(survey)) note({ field:'survey', label:field, state:'guardar', text:[].concat(value).join(', ') });
  const products = new Map();
  const productCandidates = Array.isArray(raw?.products) ? raw.products.slice(0,12) : [];
  const productNames = productCandidates.map(item=>fold(item?.name)).filter(Boolean);
  for (const item of productCandidates) {
    const source = evidence(item), name = literalNote(item?.name, source, 100);
    if (!name || !showsInterest(source,fold(name)) || lostNegation(segments.get(item.segment) || '',source)) continue;
    const key = fold(name), seen = products.get(key) || { name, prices:[] };
    for (const price of Array.isArray(item.prices) ? item.prices.slice(0,5) : []) {
      const detail = evidence(price), text = literalNote(price?.text, detail, 120);
      if (!text || !wordAt(detail,key) || productNames.some(other=>other!==key && !key.includes(other) && wordAt(detail,other)) ||
          !priceCue.test(fold(text)) || !moneyText.test(fold(text)) || seen.prices.some(value=>fold(value)===fold(text))) continue;
      const check = figures('amount', price, text);
      if (check.state === 'doubt') {
        note({ field:'price', label:name, state:'preguntar', text, reason:check.reason,
          ...(check.heard?.length && { heard:check.heard }), minute:minute(price) });
        continue;
      }
      seen.prices.push(text);
      note({ field:'price', label:name, state:'guardar', text, minute:minute(price) }, check);
    }
    seen.prices = seen.prices.slice(0,5);
    products.set(key,seen);
  }
  for (const excerpt of incomplete) review.push({ field:'referral', label:'', state:'preguntar', reason:'Referido incompleto o dudoso.', text:excerpt.slice(0,120) });
  // Instant appointments of the 4 en 14: a referral saved above accepted a demo on a day and time.
  const referralAppointments = [];
  for (const item of Array.isArray(raw?.referralAppointments) ? raw.referralAppointments.slice(0, 3) : []) {
    const name = fold(cleanName(item?.fullName)), row = unique.find(ref => fold(ref.fullName) === name);
    const text = evidence(item), whenText = typeof item?.when === 'string' ? item.when.trim() : '';
    const first = name.split(' ')[0];
    if (!row || referralAppointments.some(found => found.phone === row.phone) || !text || !whenText || whenText.length > 120 ||
        !text.includes(fold(whenText)) || !confirmsFollowUp(text) || !followUpAction.test(text) ||
        !(wordAt(text, name) || wordAt(text, first) || wordAt(segments.get(item.segment) || '', name))) continue;
    const check = figures('time', item, whenText);
    if (check.state === 'doubt') {
      note({ field:'referralAppointment', label:row.fullName, state:'preguntar', text:whenText, reason:'Fecha u hora: ' + check.reason, minute:minute(item) });
      continue;
    }
    note({ field:'referralAppointment', label:row.fullName, state:'guardar', text:whenText, minute:minute(item) }, check);
    referralAppointments.push({ fullName:row.fullName, phone:row.phone, when:whenText, segment:item.segment });
    // The 4 en 14 row keeps the appointment in its notes too.
    row.note = [row.note, 'Cita instantánea: ' + whenText].filter(Boolean).join(' · ').slice(0, 640);
  }
  return { referrals: unique, pendingReferrals: supplied.length - unique.length + incomplete.size, followUp,
    ...(referralAppointments.length && { referralAppointments }),
    ...(Object.keys(survey).length && { survey }), ...(products.size && { products:[...products.values()] }),
    ...(review.length && { review: review.slice(0, 60) }) };
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
  if (parsed.question) return { question: parsed.question };
  const result = followupTime(parsed.state, now);
  if (result.question) return { question: result.question };
  if (Date.parse(result.start) - now > 90 * 86400_000) return null;
  return { ...result, timeZone: parsed.state.timeZone, reminderMinutes: parsed.state.reminderMinutes === undefined ? 10 : parsed.state.reminderMinutes };
}

// What Jorge should ask about an agreed follow-up before it can be booked; '' when it can be booked.
export function followUpQuestion(analysis, candidate, now = Date.now()) {
  const when = whenFor(analysis, candidate, now);
  return !when ? 'No pude leer con certeza el día y la hora. ¿Para cuándo quedó?' : when.question || '';
}

export function createAutoDemoClose({ calendar, followups, sheets, transfer, active, now = Date.now }) {
  async function saveCalendar(analysis, facts, contact, partial) {
    if (!facts.followUp) return { state: 'pending' };
    const when = !partial && whenFor(analysis, facts.followUp, now());
    if (!when || when.question) return { state: 'time', ...(when?.question && { question: when.question }) };
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

  // Instant appointments with referrals: one Google Calendar event each, in the same calendar as the follow-up.
  async function saveReferralAppointments(analysis, facts, contact, partial) {
    const results = [];
    for (const appointment of facts.referralAppointments || []) {
      const label = { name: appointment.fullName };
      const when = !partial && whenFor(analysis, appointment, now());
      if (!when || when.question) { results.push({ ...label, state:'time', ...(when?.question && { question:when.question }) }); continue; }
      if (!await active(analysis)) { results.push({ ...label, state:'stopped' }); continue; }
      const owner = analysis.ownerUserId;
      const connection = await calendar.getConnection(owner);
      if (!connection?.calendarId || !connection.availabilityEnabled || connection.grant !== analysis.autoClose.calendarGrant) {
        results.push({ ...label, state:'connection' }); continue;
      }
      const eventId = digest([String(owner), analysis.closeVisitId, 'referral-appointment', appointment.phone]);
      const summary = ('Demo · ' + appointment.fullName + (contact.name ? ' (4 en 14 de ' + contact.name + ')' : '')).slice(0, 180);
      try {
        const saved = await calendar.saveEvent({ owner, grant: connection.grant, eventId, operationId: 'demo-' + eventId, ...when, summary,
          mode: 'create', checkAvailability: true, retryGoogle: true });
        await followups.rememberEvent({ owner, calendarId: connection.calendarId }, { ...when, name: appointment.fullName, summary,
          action: 'demo', operationId: 'demo-' + eventId, eventId, htmlLink: saved.htmlLink || '', etag: saved.etag || '' })
          .catch(() => console.error('No se pudo indexar una cita instantánea de demostración.'));
        results.push({ ...label, state:'saved', start: when.start, timeZone: when.timeZone, htmlLink: saved.htmlLink || '' });
      } catch (error) {
        results.push({ ...label, state: error?.slotBusy ? 'busy' : 'error' });
      }
    }
    return results;
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
    const notesToReview = draft.rows.filter(row => row.status !== 'new' && facts.referrals[row.index]?.note)
      .map(row => ({ name:facts.referrals[row.index].fullName, note:facts.referrals[row.index].note }));
    return { state: 'checked', saved: result.written, pending: review, duplicates, sheetTitle: result.sheetTitle, file: book.spreadsheetId,
      ...(notesToReview.length && { notesToReview }) };
  }

  return async function save(analysis, facts, contact, { partial = false, followUpMentioned = false } = {}) {
    const [appointment, referrals, instant] = await Promise.allSettled([
      facts.followUp || followUpMentioned ? saveCalendar(analysis, facts, contact, partial) : null,
      saveReferrals(analysis, facts, contact),
      saveReferralAppointments(analysis, facts, contact, partial)
    ]);
    const referralAppointments = instant.status === 'fulfilled' ? instant.value
      : (facts.referralAppointments || []).map(item => ({ name: item.fullName, state: 'error' }));
    // An instant appointment whose day does not add up is asked by Jorge too.
    for (const item of referralAppointments.filter(result => result.state === 'time')) {
      const source = (facts.referralAppointments || []).find(found => found.fullName === item.name);
      facts.review = [...(facts.review || []).filter(entry => !(entry.field === 'referralAppointment' && entry.label === item.name)),
        { field:'referralAppointment', label:item.name, state:'preguntar', text:source?.when || '',
          reason:'Fecha u hora: ' + (item.question || 'no se pudo leer con certeza.') }];
    }
    // A date the recording left in doubt («mañana viernes» when tomorrow is Thursday) is asked by Jorge, not guessed.
    if (facts.followUp && appointment.status === 'fulfilled' && appointment.value?.state === 'time') {
      const label = facts.followUp.when, reason = appointment.value.question || '';
      facts.review = [...(facts.review || []).filter(item => item.field !== 'followUp'),
        { field:'followUp', label, state:'preguntar', reason:'Fecha u hora: ' + (reason || 'no se pudo leer con certeza.'), ...(reason && { text:reason }) }];
    }
    const referralState = referrals.status === 'fulfilled' ? referrals.value : { state:'error', saved:0,
      pending:facts.pendingReferrals + facts.referrals.length };
    if (referralState.state !== 'checked') {
      const notes = facts.referrals.filter(row=>row.note).map(row=>({ name:row.fullName, note:row.note }));
      if (notes.length) referralState.notesToReview = notes;
    }
    return { appointment: appointment.status === 'fulfilled' ? appointment.value : { state:appointment.reason?.slotBusy ? 'busy' : 'error' },
      referrals:referralState, ...(referralAppointments.length && { referralAppointments }) };
  };
}
