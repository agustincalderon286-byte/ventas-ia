import { openToken, sealToken } from './coach-hycite-gmail.js';
import { sheetsPhoneKey } from './coach-sheets-transfer.js';
import { structuredResult } from './coach-mail-intent.js';
import { followUpQuestion } from './coach-demo-auto-close.js';

// The visit: one place that knows what the recorded demo left saved, waiting or in doubt.
// What could not be saved is kept sealed for 14 days, so TEC can retry it alone, Coach Jorge can ask
// for exactly what is missing, and the seller's spoken answer is saved where it belongs, with no forms.
export const VISIT_TTL_MS = 14 * 86400_000;
const fold = text => String(text || '').normalize('NFD').replace(/\p{M}/gu, '').replace(/\s+/g, ' ').trim().toLowerCase();
const clip = (text, max) => String(text || '').replace(/[\p{Cc}<>]/gu, ' ').replace(/\s+/g, ' ').trim().slice(0, max);
const personName = /^[\p{L}][\p{L}\p{M} .’'\-]{1,110}$/u;
const words = name => fold(name).replace(/[^\p{L} ]/gu, ' ').split(' ').filter(word => word.length >= 2);
const digits = text => String(text || '').replace(/\D/g, '');
const money = value => { const amount = Number(String(value || '').replace(/[$,\s]/g, '')); return Number.isFinite(amount) && amount > 0 && amount < 1e7 ? Math.round(amount * 100) / 100 : 0; };
// Every amount said in an answer, as whole numbers: «2,500», «2 mil», «2 mil 500» and «1500.50».
export function amountsIn(text) {
  const found = new Set();
  const plain = fold(text).replace(/(\d),(\d{3})\b/g, '$1$2');
  for (const hit of plain.matchAll(/(\d+(?:\.\d+)?)\s*mil\b(?:\s+(\d{1,3})\b)?/g)) found.add(Math.round(Number(hit[1]) * 1000 + Number(hit[2] || 0)));
  for (const hit of plain.matchAll(/\d+(?:\.\d{1,2})?/g)) found.add(Math.floor(Number(hit[0])));
  return found;
}
const surveyAsk = { cooksForCount: '¿Para cuántas personas cocinan?', foodSpendWeekly: '¿Cuánto gastan en comida a la semana?', waterSpendWeekly: '¿Cuánto gastan en agua a la semana?' };

export function emptyVisit() {
  return { v: 1, contact: {}, facts: { referrals: [], followUp: null, referralAppointments: [], products: [] }, review: [],
    done: { followUp: false, referrals: [], appointments: [], outcome: '', survey: [] }, outcome: { type: '', amount: 0, products: [] },
    wait: { sheets: false, calendar: false }, retry: false, busy: { followUp: false, appointments: [] }, closed: [], notes: [], saved: [] };
}

const questionId = (kind, key = '') => kind + ':' + fold(key).replace(/[^a-z0-9 ]/g, '').slice(0, 60);

// The questions Jorge shows, most important first. Every one names the thing it is about.
export function visitQuestions(state) {
  const list = [];
  const add = (kind, key, ask, data = {}) => {
    const id = questionId(kind, key);
    if (!state.closed.includes(id) && !list.some(item => item.id === id)) list.push({ id, kind, ask: clip(ask, 300), data });
  };
  const { contact, facts, done, outcome } = state;
  if (!contact.name) add('host', '', '¿En casa de quién fue la demo? Dime su nombre completo y, si lo tienes, su teléfono.');
  for (const item of state.review) {
    const label = clip(item.label, 100), text = clip(item.text, 140);
    if (item.field === 'referral' && label) add('refPhone', label, 'No me quedó claro el teléfono de ' + label + '. ¿Cuál es?', { fullName: label });
    else if (item.field === 'referral' && text) add('refNew', text, 'Mencionaron un referido que quedó incompleto: «' + text + '». ¿Cuál es su nombre completo y su teléfono?');
    else if (item.field === 'referralAppointment' && label) {
      const phone = facts.referrals.find(row => fold(row.fullName) === fold(label))?.phone || '';
      if (phone && !done.appointments.includes(phone)) add('apptTime', label, 'La cita con ' + label + (text ? ' («' + text + '»)' : '') +
        ' no quedó clara. ¿Qué día y a qué hora es?', { fullName: label, phone });
    } else if (item.field === 'followUp' && !done.followUp) add('followUp', '', 'El seguimiento' + (label ? ' «' + label + '»' : '') + ' no quedó claro. ' +
      (clip(item.text, 160) || '¿Qué día y a qué hora quedó?'));
    else if (item.field === 'price' && label) add('price', label, 'Del ' + label + ' oí «' + text + '», pero pudo quedar cortado. ¿Cuál fue el precio?', { label });
    else if (item.field === 'survey' && surveyAsk[item.label] && !done.survey.includes(item.label)) add('survey', item.label,
      surveyAsk[item.label] + ' Oí «' + text + '», pero no estoy seguro.', { field: item.label });
  }
  if (state.busy.followUp && !done.followUp && facts.followUp) add('followUp', '', 'El horario del seguimiento («' + clip(facts.followUp.when, 80) +
    '») ya está ocupado en tu calendario. ¿Qué otro día y hora?');
  for (const phone of state.busy.appointments) {
    const row = (facts.referralAppointments || []).find(item => item.phone === phone);
    if (row && !done.appointments.includes(phone)) add('apptTime', row.fullName, 'El horario de la cita con ' + row.fullName + ' («' + clip(row.when, 80) +
      '») ya está ocupado en tu calendario. ¿Qué otro día y hora?', { fullName: row.fullName, phone });
  }
  // The visit ended in a follow-up but no day was heard: the appointment is what the seller wants to see.
  if (outcome.type === 'follow_up' && !facts.followUp && !done.followUp) add('followUp', '', 'La visita quedó en seguimiento, pero no oí la fecha. ¿Qué día y a qué hora quedaron? Si no hay fecha, dime que no.');
  if (!done.outcome) {
    const what = outcome.products?.length ? ' de ' + outcome.products.slice(0, 3).join(', ') : '';
    if (outcome.type === 'venta' && outcome.amount) add('sale', '', 'Oí que cerraste la venta' + what + ' por $' + outcome.amount + '. ¿La registro?');
    else if (outcome.type === 'venta') add('sale', '', 'Oí que cerraste la venta' + what + '. ¿En cuánto quedó?');
    else if (!outcome.type) add('outcome', '', '¿Cómo terminó la visita: hubo venta, quedó en seguimiento o no les interesó?');
  }
  return list;
}

// What is waiting only for a Google connection: nothing to answer, TEC retries alone.
export function visitWaiting(state) {
  const notes = [];
  const referrals = state.facts.referrals.filter(row => !state.done.referrals.includes(row.phone)).length;
  if (state.wait.sheets && referrals) notes.push('Conecta tu hoja de Google en Mis conexiones: tengo ' + referrals +
    (referrals === 1 ? ' referido listo' : ' referidos listos') + ' para guardar en cuanto la conectes.');
  if (state.wait.calendar) notes.push('Conecta Google Calendar en Mis conexiones con el permiso de horarios libres: tengo una cita lista para agendar.');
  if (state.retry && !notes.length) notes.push('Una parte no se pudo guardar por un problema pasajero. Lo intento otra vez yo solo.');
  return notes;
}

// A save that failed for a passing reason (Google did not answer, the lease ended) is tried again.
const failed = receipt => [receipt?.referrals, receipt?.appointment, ...(receipt?.referralAppointments || [])]
  .some(item => item && ['error', 'stopped'].includes(item.state));

const answerSchema = {
  answers: { type: 'array', items: { type: 'object', additionalProperties: false,
    required: ['id', 'fullName', 'phone', 'when', 'amount', 'choice', 'quote'], properties: {
      id: { type: 'string' }, fullName: { type: 'string' }, phone: { type: 'string' }, when: { type: 'string' }, amount: { type: 'string' },
      choice: { type: 'string', enum: ['', 'si', 'no', 'venta', 'follow_up', 'no_venta'] }, quote: { type: 'string' } } } },
  later: { type: 'boolean' }
};
const answerSystem = 'Eres Coach Jorge. Un vendedor contesta, casi siempre con un audio transcrito, las preguntas pendientes de una visita de venta. ' +
  'Reparte su respuesta entre las preguntas: una misma respuesta puede contestar varias, en cualquier orden y con sus propias palabras. ' +
  'Para cada pregunta contestada devuelve su id y solo los datos que el vendedor dijo: fullName (nombre completo de la persona), phone (solo dígitos), ' +
  'when (la frase exacta del día y la hora, sin convertirla), amount (solo el número), choice (si/no ante una confirmación, y no también cuando dice que no hubo fecha; venta, follow_up o no_venta ante cómo terminó la visita). ' +
  'En quote copia el fragmento literal de su respuesta que lo dice. No inventes ni completes datos que no dijo; deja vacío lo que no aplica. ' +
  'Si la respuesta no contesta ninguna pregunta devuelve answers vacío. later es true solo si pide dejarlo para después.';

const dateLine = (start, timeZone) => { try { return new Intl.DateTimeFormat('es-US', { timeZone, weekday: 'short', day: 'numeric', month: 'short',
  hour: 'numeric', minute: '2-digit' }).format(new Date(start)); } catch { return ''; } };

// What the demo left saved, in the seller's words. No phones.
export function visitNews(receipt = {}, state, surveySaved = false) {
  const lines = [];
  const refs = receipt.referrals;
  if (refs?.saved) lines.push(refs.saved + (refs.saved === 1 ? ' referido' : ' referidos') + ' en tu hoja.');
  if (receipt.appointment?.state === 'saved') lines.push('El seguimiento del ' + dateLine(receipt.appointment.start, receipt.appointment.timeZone) + ' en tu calendario.');
  for (const item of receipt.referralAppointments || []) if (item.state === 'saved') lines.push('La cita con ' + clip(item.name, 80) + ' del ' +
    dateLine(item.start, item.timeZone) + '.');
  if (surveySaved) lines.push('La encuesta.');
  if (state.done.outcome === 'follow_up') lines.push('La visita quedó en seguimiento.');
  if (state.done.outcome === 'no_venta') lines.push('La visita quedó como no venta.');
  return lines;
}

export function createVisit({ analyses, key, save, saveOutcome, saveSurvey, findHost, connections, active, extract = structuredResult, now = Date.now }) {
  const aad = doc => 'visit:' + String(doc._id) + ':' + String(doc.ownerUserId);
  const open = doc => { try { return doc.visit?.sealed && Number(new Date(doc.visit.expiresAt)) > now() ? JSON.parse(openToken(doc.visit.sealed, key, aad(doc))) : null; } catch { return null; } };
  const sealed = (doc, state) => {
    const questions = visitQuestions(state).length, waiting = visitWaiting(state).length;
    return { visit: questions || waiting ? { sealed: sealToken(JSON.stringify(state), key, aad(doc)), expiresAt: new Date(now() + VISIT_TTL_MS) } : null,
      visitQuestions: questions, visitWaiting: waiting > 0 };
  };

  // Saves what is clear and not yet saved. Safe to repeat: each piece is saved once.
  async function settle(doc, state) {
    const { facts, done } = state;
    const referrals = facts.referrals.filter(row => !done.referrals.includes(row.phone));
    const appointments = (facts.referralAppointments || []).filter(row => !done.appointments.includes(row.phone));
    const followUp = done.followUp ? null : facts.followUp;
    const lines = [];
    let receipt = null;
    if (referrals.length || appointments.length || followUp) {
      // A connection made after the recording started is adopted once; a changed one never replaces it.
      const live = await connections(doc.ownerUserId).catch(() => ({}));
      const autoClose = { ...doc.autoClose, calendarGrant: doc.autoClose?.calendarGrant || live.calendarGrant || '',
        sheetsGrant: doc.autoClose?.sheetsGrant || live.sheetsGrant || '' };
      const pass = { referrals, knownReferrals: facts.referrals, pendingReferrals: 0, followUp, referralAppointments: appointments, products: facts.products, review: [] };
      receipt = await save({ ...doc, autoClose }, pass, state.contact, {});
      state.adopted = { calendarGrant: autoClose.calendarGrant, sheetsGrant: autoClose.sheetsGrant };
      if (receipt.appointment?.state === 'saved') { done.followUp = true; lines.push('Seguimiento en tu calendario.'); }
      state.busy.followUp = receipt.appointment?.state === 'busy';
      if (receipt.referrals?.state === 'checked') {
        done.referrals.push(...referrals.map(row => row.phone));
        if (receipt.referrals.saved) lines.push(receipt.referrals.saved + (receipt.referrals.saved === 1 ? ' referido guardado' : ' referidos guardados') + ' en tu hoja.');
      }
      state.busy.appointments = [];
      for (const [index, item] of (receipt.referralAppointments || []).entries()) {
        const phone = appointments[index]?.phone;
        if (item.state === 'saved' && phone) { done.appointments.push(phone); lines.push('Cita con ' + item.name + ' en tu calendario.'); }
        if (item.state === 'busy' && phone) state.busy.appointments.push(phone);
      }
      state.wait = { sheets: receipt.referrals?.state === 'connection',
        calendar: [receipt.appointment, ...(receipt.referralAppointments || [])].some(item => item?.state === 'connection') };
      state.retry = failed(receipt);
      // A date that still does not add up comes back as a question.
      for (const item of pass.review) if (item.state === 'preguntar' && !state.review.some(old => old.field === item.field && old.label === item.label)) state.review.push(item);
    }
    if (!done.outcome && ['follow_up', 'no_venta'].includes(state.outcome.type)) await outcome(doc, state, lines);
    return { receipt, lines };
  }

  async function outcome(doc, state, lines) {
    const { type, amount, products } = state.outcome;
    if (type === 'venta' && !amount) return;
    const saved = await saveOutcome(doc, { type, amount, products: products || [], crmId: state.contact.crmId || '',
      note: clip(state.outcome.note || state.notes.join(' · ') || 'Resultado tomado de la grabación de la demo.', 300) }).catch(error => {
      console.error('No se pudo guardar el resultado de la visita', { name: error.name });
      return null;
    });
    if (!saved) return;
    state.done.outcome = type;
    lines.push(type === 'venta' ? 'Venta registrada por $' + amount + '.' : type === 'follow_up' ? 'La visita quedó en seguimiento.' : 'La visita quedó como no venta.');
  }

  // The receipt the seller sees keeps up with what has been saved since.
  function mergeReceipt(old = {}, state, fresh) {
    const receipt = { ...old };
    if (fresh?.appointment && (fresh.appointment.state === 'saved' || !old.appointment || old.appointment.state !== 'saved')) receipt.appointment = fresh.appointment;
    if (fresh?.referrals?.state === 'checked') receipt.referrals = { ...fresh.referrals, saved: (old.referrals?.saved || 0) + fresh.referrals.saved,
      duplicates: (old.referrals?.duplicates || 0) + (fresh.referrals.duplicates || 0), pending: 0 };
    else if (fresh?.referrals && fresh.referrals.state !== 'none' && fresh.referrals.state !== 'pending') receipt.referrals = { ...old.referrals, ...fresh.referrals };
    if (fresh?.referralAppointments?.length) receipt.referralAppointments = [...(old.referralAppointments || [])
      .filter(item => !fresh.referralAppointments.some(found => found.name === item.name)), ...fresh.referralAppointments];
    if (state.contact.name && !old.host) receipt.host = { name: state.contact.name, source: state.contact.source || 'vendedor' };
    const openIds = new Set(visitQuestions(state).map(item => item.id));
    receipt.review = (old.review || []).filter(item => item.state !== 'preguntar' || openIds.has(questionId(
      item.field === 'referral' ? (item.label ? 'refPhone' : 'refNew') : item.field === 'referralAppointment' ? 'apptTime' : item.field,
      item.field === 'followUp' ? '' : item.label || item.text)));
    if (state.done.outcome) receipt.outcome = { type: state.done.outcome, ...(state.outcome.amount && { amount: state.outcome.amount }) };
    receipt.saved = state.saved.slice(-12);
    return receipt;
  }

  async function store(doc, state, fresh) {
    const update = { ...sealed(doc, state), autoCloseReceipt: mergeReceipt(doc.autoCloseReceipt, state, fresh),
      ...(state.contact.crmId && !doc.closeCrmId && { closeCrmId: state.contact.crmId }),
      ...(state.adopted?.calendarGrant && !doc.autoClose?.calendarGrant && { 'autoClose.calendarGrant': state.adopted.calendarGrant }),
      ...(state.adopted?.sheetsGrant && !doc.autoClose?.sheetsGrant && { 'autoClose.sheetsGrant': state.adopted.sheetsGrant }) };
    return update;
  }

  const view = (doc, state) => ({ id: String(doc._id), startedAt: doc.startedAt, host: state.contact.name || '',
    questions: visitQuestions(state).map(({ id, kind, ask }) => ({ id, kind, ask })), waiting: visitWaiting(state), saved: state.saved.slice(-6) });

  return {
    // Called once, when the report is ready: receives what the automatic close did and keeps the rest.
    async begin(doc, { facts, contact, receipt, closeFacts = {}, survey = {}, surveySaved = false }) {
      const state = emptyVisit();
      state.contact = { name: contact.name || '', phone: contact.phone || '', address: contact.address || '', crmId: contact.crmId || '', source: contact.source || '' };
      state.facts = { referrals: facts.referrals || [], followUp: facts.followUp || null, referralAppointments: facts.referralAppointments || [], products: facts.products || [] };
      state.review = (facts.review || []).filter(item => item?.state === 'preguntar').map(item => ({ field: item.field, label: item.label || '', text: item.text || '', state: 'preguntar' }));
      state.done.followUp = receipt.appointment?.state === 'saved';
      state.busy.followUp = receipt.appointment?.state === 'busy';
      if (receipt.referrals?.state === 'checked') state.done.referrals = state.facts.referrals.map(row => row.phone);
      for (const item of receipt.referralAppointments || []) {
        const phone = state.facts.referralAppointments.find(row => row.fullName === item.name)?.phone;
        if (phone && item.state === 'saved') state.done.appointments.push(phone);
        if (phone && item.state === 'busy') state.busy.appointments.push(phone);
      }
      state.wait = { sheets: receipt.referrals?.state === 'connection',
        calendar: [receipt.appointment, ...(receipt.referralAppointments || [])].some(item => item?.state === 'connection') };
      state.retry = failed(receipt);
      state.done.survey = Object.keys(survey || {});
      const type = { venta: 'venta', follow_up: 'follow_up', no_venta: 'no_venta' }[closeFacts.outcome] || '';
      state.outcome = { type, amount: type === 'venta' ? money(closeFacts.saleAmount) : 0,
        products: (state.facts.products || []).map(product => product.name).slice(0, 8), note: clip(closeFacts.followUp || closeFacts.objection || '', 300) };
      const lines = [];
      if (['follow_up', 'no_venta'].includes(type)) await outcome(doc, state, lines);
      state.saved.push(...lines);
      return { ...sealed(doc, state), ...(state.done.outcome && { outcome: { type: state.done.outcome } }),
        visitNews: { host: clip(state.contact.name, 100), lines: visitNews(receipt, state, surveySaved) }, visitNewsSeen: false };
    },

    // Everything Jorge still needs from this seller, newest visit first.
    async pending(owner) {
      const rows = await analyses.find({ ownerUserId: owner, status: 'completed', $or: [{ visitQuestions: { $gt: 0 } }, { visitWaiting: true }],
        'visit.expiresAt': { $gt: new Date(now()) }, $and: [{ $or: [{ visitSnoozedUntil: null }, { visitSnoozedUntil: { $lte: new Date(now()) } }] }] })
        .sort({ startedAt: -1 }).limit(3).lean();
      const items = [];
      for (let doc of rows) {
        let state = open(doc);
        if (!state) continue;
        // Something was only waiting for a connection: try again, at most every two minutes.
        if (doc.visitWaiting && (!doc.visitRetriedAt || now() - Number(new Date(doc.visitRetriedAt)) > 120_000) && await active(doc)) {
          await analyses.updateOne({ _id: doc._id }, { $set: { visitRetriedAt: new Date(now()) } });
          const { receipt, lines } = await settle(doc, state).catch(() => ({ lines: [] }));
          state.saved.push(...lines);
          await analyses.updateOne({ _id: doc._id, ownerUserId: owner }, { $set: await store(doc, state, receipt) });
        }
        const shown = view(doc, state);
        if (shown.questions.length || shown.waiting.length) items.push(shown);
      }
      return items;
    },

    // Demos finished in the last day and a half whose result the seller has not been told yet.
    async news(owner) {
      const rows = await analyses.find({ ownerUserId: owner, status: 'completed', visitNews: { $exists: true, $ne: null }, visitNewsSeen: { $ne: true },
        startedAt: { $gt: new Date(now() - 36 * 3600_000) } }).sort({ startedAt: -1 }).limit(3).lean();
      return rows.map(doc => ({ id: String(doc._id), host: doc.visitNews.host || '', lines: (doc.visitNews.lines || []).slice(0, 8), questions: doc.visitQuestions || 0 }));
    },
    seen: (owner, id) => analyses.updateOne({ _id: id, ownerUserId: owner }, { $set: { visitNewsSeen: true } }),

    // The seller's free answer (typed or a transcribed audio). Returns what was saved and what is still open.
    async answer(owner, id, text) {
      const doc = await analyses.findOne({ _id: id, ownerUserId: owner, status: 'completed' }).lean();
      const state = doc && open(doc);
      if (!state || typeof text !== 'string' || !text.trim() || text.length > 6000) return null;
      if (!await active(doc)) return null;
      const questions = visitQuestions(state);
      if (!questions.length) return { understood: false, saved: [], ...view(doc, state) };
      const reply = text.trim(), folded = fold(reply), spoken = digits(reply), amounts = amountsIn(reply);
      const proposed = await extract({ name: 'tec_respuesta_visita', timeout: 20000, properties: answerSchema, system: answerSystem,
        instruction: JSON.stringify({ preguntas: questions.map(({ id, ask }) => ({ id, pregunta: ask })), respuesta: reply }) }).catch(() => null);
      const before = questions.length;
      let understood = 0;
      const lines = [], clarify = [];
      for (const item of Array.isArray(proposed?.answers) ? proposed.answers.slice(0, 12) : []) {
        const question = questions.find(found => found.id === item?.id);
        const quote = fold(item?.quote);
        if (!question || quote.length < 2 || !folded.includes(quote) || state.closed.includes(question.id)) continue;
        const name = typeof item.fullName === 'string' && personName.test(item.fullName.trim()) && words(item.fullName).length >= 2 &&
          words(item.fullName).every(word => folded.includes(word)) ? item.fullName.trim().slice(0, 110) : '';
        const phone = sheetsPhoneKey(item.phone) && spoken.includes(digits(sheetsPhoneKey(item.phone))) ? sheetsPhoneKey(item.phone) : '';
        const when = typeof item.when === 'string' && item.when.trim() && item.when.length <= 120 && folded.includes(fold(item.when)) ? item.when.trim() : '';
        const amount = amounts.has(Math.floor(money(item.amount))) ? money(item.amount) : 0;
        const close = () => { state.closed.push(question.id); understood++; };
        const known = number => state.facts.referrals.some(row => row.phone === number);
        if (question.kind === 'host' && name) {
          const host = await findHost(doc, { name, phone }).catch(() => null);
          state.contact = { name: host?.name || name, phone: host?.phone || phone, address: host?.address || '', crmId: host?.crmId || '', source: 'vendedor' };
          lines.push('Anfitrión: ' + state.contact.name + '.');
          close();
        } else if (question.kind === 'refPhone' && phone) {
          if (!known(phone)) state.facts.referrals.push({ fullName: question.data.fullName, phone });
          close();
        } else if (question.kind === 'refNew' && name && phone) {
          if (!known(phone)) state.facts.referrals.push({ fullName: name, phone });
          close();
        } else if ((question.kind === 'apptTime' || question.kind === 'followUp') && !when && item.choice === 'no') {
          // «No quedamos en fecha»: nothing to book, and Jorge stops asking.
          close();
        } else if ((question.kind === 'apptTime' || question.kind === 'followUp') && when) {
          const candidate = { when, segment: 1, agreed: clip(item.quote, 240), anchor: now() };
          const doubt = followUpQuestion(doc, candidate, now());
          // A day that does not add up («mañana viernes» on a Wednesday) is asked again, never guessed.
          if (doubt) { clarify.push(doubt); continue; }
          if (question.kind === 'followUp') {
            state.facts.followUp = candidate; state.busy.followUp = false;
            state.review = state.review.filter(entry => entry.field !== 'followUp');
          } else {
            const { fullName, phone: number } = question.data;
            state.facts.referralAppointments = [...(state.facts.referralAppointments || []).filter(row => row.phone !== number), { ...candidate, fullName, phone: number }];
            state.busy.appointments = state.busy.appointments.filter(value => value !== number);
            state.review = state.review.filter(entry => !(entry.field === 'referralAppointment' && fold(entry.label) === fold(fullName)));
          }
          understood++;
        } else if (question.kind === 'sale' && (amount || item.choice === 'si' || item.choice === 'venta') && (amount || state.outcome.amount)) {
          state.outcome = { ...state.outcome, type: 'venta', amount: amount || state.outcome.amount };
          await outcome(doc, state, lines);
          if (state.done.outcome) understood++;
        } else if (question.kind === 'sale' && (item.choice === 'no' || item.choice === 'follow_up' || item.choice === 'no_venta')) {
          state.outcome = { ...state.outcome, type: item.choice === 'no' ? '' : item.choice, amount: 0 };
          understood++;
        } else if (question.kind === 'outcome' && ['venta', 'follow_up', 'no_venta'].includes(item.choice)) {
          state.outcome = { ...state.outcome, type: item.choice, amount: item.choice === 'venta' ? amount : 0 };
          if (item.choice === 'venta') await outcome(doc, state, lines);
          understood++;
        } else if (question.kind === 'price' && amount) {
          state.notes.push(clip(question.data.label + ': $' + amount, 120));
          lines.push('Precio de ' + question.data.label + ': $' + amount + '.');
          close();
        } else if (question.kind === 'survey' && amount) {
          const saved = await saveSurvey(doc, { [question.data.field]: String(amount) }, state.contact).catch(() => null);
          if (saved?.state === 'saved') { state.done.survey.push(question.data.field); lines.push('Encuesta actualizada.'); }
          close();
        }
      }
      let receipt = null;
      if (understood) {
        const settled = await settle(doc, state);
        receipt = settled.receipt;
        lines.push(...settled.lines);
        state.saved.push(...lines);
        const saved = await analyses.updateOne({ _id: doc._id, ownerUserId: owner, 'visit.sealed': doc.visit.sealed }, { $set: await store(doc, state, receipt) });
        if (!saved.modifiedCount) return { understood: false, changed: true, saved: [], ...view(doc, open(await analyses.findOne({ _id: id }).lean()) || state) };
      } else if (proposed?.later === true) {
        await analyses.updateOne({ _id: doc._id, ownerUserId: owner }, { $set: { visitSnoozedUntil: new Date(now() + 3 * 3600_000) } });
        return { understood: true, later: true, saved: [], ...view(doc, state) };
      }
      const shown = view(doc, state);
      return { understood: understood > 0 || clarify.length > 0, saved: lines, clarify, answered: before - shown.questions.length, ...shown };
    },
    open
  };
}

export function registerVisit(app, { active, allowed, visit }) {
  const guard = handler => async (req, res) => {
    res.set('Cache-Control', 'private, no-store').vary('Cookie');
    const auth = await active(req, res);
    if (!auth) return;
    if (!allowed(auth, req)) return res.json({ items: [] });
    if (req.method !== 'GET' && (req.get('Sec-Fetch-Site') ? req.get('Sec-Fetch-Site') !== 'same-origin' : req.get('Origin') !== req.protocol + '://' + req.get('host'))) {
      return res.status(403).json({ error: 'Contesta desde la página del Coach.' });
    }
    try { await handler(req, res, auth); } catch (error) {
      console.error('No se pudo atender la visita', { name: error.name });
      res.status(500).json({ error: 'No pude guardar tu respuesta. Inténtalo otra vez.' });
    }
  };
  app.get('/api/coach/visits/pending', guard(async (_req, res, auth) => {
    const [items, news] = await Promise.all([visit.pending(auth.user._id), visit.news(auth.user._id)]);
    res.json({ items, news });
  }));
  app.post('/api/coach/visits/:id/seen', guard(async (req, res, auth) => {
    if (/^[a-f0-9]{24}$/i.test(req.params.id)) await visit.seen(auth.user._id, req.params.id);
    res.status(204).end();
  }));
  app.post('/api/coach/visits/:id/answer', guard(async (req, res, auth) => {
    if (!/^[a-f0-9]{24}$/i.test(req.params.id)) return res.status(404).json({ error: 'Esa visita no existe.' });
    const result = await visit.answer(auth.user._id, req.params.id, req.body?.text);
    return result ? res.json(result) : res.status(404).json({ error: 'Esa visita ya no tiene preguntas.' });
  }));
}
