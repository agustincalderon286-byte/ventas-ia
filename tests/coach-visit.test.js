import test from 'node:test';
import assert from 'node:assert/strict';
import { randomBytes } from 'node:crypto';
import sift from 'sift';
import { amountsIn, createVisit, emptyVisit, visitQuestions } from '../src/coach-visit.js';

// Fictitious people and 555-01xx phones only.
function model() {
  const rows = [];
  const find = query => rows.filter(sift(query));
  const chain = value => { const query = { lean: async () => structuredClone(value), sort: () => query, limit: () => query, select: () => query }; return query; };
  const set = (row, path, value) => { const keys = path.split('.'), last = keys.pop(); keys.reduce((at, key) => at[key] ??= {}, row)[last] = value; };
  return { rows, find: query => chain(find(query)), findOne: query => chain(find(query)[0]),
    async updateOne(query, changes) {
      const match = find(query)[0];
      if (match) for (const [path, value] of Object.entries(changes.$set || {})) set(match, path, structuredClone(value));
      return { modifiedCount: Number(Boolean(match)) };
    } };
}

const wednesday = Date.parse('2026-11-25T20:00:00Z'); // 2 pm in Chicago
function fixture({ answers, receipts = [] } = {}) {
  const analyses = model(), calls = { save: [], outcome: [], host: [], survey: [] };
  let time = wednesday;
  const visit = createVisit({ analyses, key: randomBytes(32), now: () => time, active: async () => true,
    connections: async () => ({ calendarGrant: 'cal-now', sheetsGrant: 'sheets-now' }),
    async save(doc, facts, contact) {
      calls.save.push({ doc, facts: structuredClone(facts), contact: structuredClone(contact) });
      return receipts.shift() || { appointment: facts.followUp ? { state: 'saved', start: '2026-11-28T16:00:00.000Z', timeZone: 'America/Chicago' } : null,
        referrals: facts.referrals.length ? (contact.name ? { state: 'checked', saved: facts.referrals.length, pending: 0, duplicates: 0, sheetTitle: '4 en 14' }
          : { state: 'host', saved: 0, pending: facts.referrals.length }) : { state: 'none', saved: 0, pending: 0 },
        ...(facts.referralAppointments.length && { referralAppointments: facts.referralAppointments.map(row => ({ name: row.fullName, state: 'saved' })) }) };
    },
    saveOutcome: async (_doc, outcome) => { calls.outcome.push(outcome); return { id: 'outcome-1' }; },
    saveSurvey: async (_doc, values) => { calls.survey.push(values); return { state: 'saved' }; },
    findHost: async (_doc, heard) => { calls.host.push(heard); return { name: heard.name, phone: heard.phone, crmId: 'a'.repeat(24), address: '123 Calle Falsa' }; },
    extract: async ({ instruction }) => answers(JSON.parse(instruction)) });
  const doc = { _id: 'b'.repeat(24), ownerUserId: 'seller', status: 'completed', startedAt: new Date(wednesday - 3600_000), chunks: [],
    autoClose: { zone: 'America/Chicago', calendarGrant: '', sheetsGrant: '' }, autoCloseReceipt: {} };
  analyses.rows.push(doc);
  return { visit, analyses, calls, doc, advance: ms => { time += ms; } };
}

test('amounts said in an answer are read as numbers', () => {
  assert.deepEqual([...amountsIn('quedó en 2 mil 500, con 79 al mes')].sort((a, b) => a - b), [2, 79, 500, 2500]);
  assert.ok(amountsIn('fueron $2,499.50 en total').has(2499));
});

test('Jorge asks only what is missing, the most important first', () => {
  const state = emptyVisit();
  state.facts.referrals = [{ fullName: 'Julia Ríos', phone: '3125550147' }];
  state.review = [{ field: 'referral', label: 'Ana Soto', state: 'preguntar' },
    { field: 'referral', label: '', text: 'anota a mi cuñada Carla', state: 'preguntar' },
    { field: 'referralAppointment', label: 'Julia Ríos', text: 'mañana viernes a las seis', state: 'preguntar' }];
  assert.deepEqual(visitQuestions(state).map(item => item.kind), ['host', 'refPhone', 'refNew', 'apptTime', 'outcome']);
  state.contact.name = 'Silvia Prueba';
  state.outcome = { type: 'venta', amount: 2500, products: ['Sartén Premium'] };
  const sale = visitQuestions(state).find(item => item.kind === 'sale');
  assert.equal(sale.ask, 'Oí que cerraste la venta de Sartén Premium por $2500. ¿La registro?');
  state.done.outcome = 'venta';
  assert.equal(visitQuestions(state).some(item => item.kind === 'sale' || item.kind === 'outcome'), false);
});

test('one audio answers several questions and each answer is saved in its place', async () => {
  const spoken = 'Fue en casa de Silvia Prueba, su número es 773 555 0101. El teléfono de Ana Soto es 312 555 0109. ' +
    'Con Julia quedamos el viernes a las 6 de la tarde. Y sí, la venta quedó en 2 mil 500.';
  const { visit, analyses, calls, doc } = fixture({ answers: ({ preguntas }) => ({ later: false, answers: preguntas.map(({ id }) => ({
    id, fullName: id.startsWith('host') ? 'Silvia Prueba' : '', phone: id.startsWith('host') ? '7735550101' : id.startsWith('refPhone') ? '3125550109' : '',
    when: id.startsWith('apptTime') ? 'el viernes a las 6 de la tarde' : '', amount: id.startsWith('sale') ? '2500' : '', choice: id.startsWith('sale') ? 'si' : '',
    quote: id.startsWith('host') ? 'Fue en casa de Silvia Prueba' : id.startsWith('refPhone') ? 'El teléfono de Ana Soto es 312 555 0109'
      : id.startsWith('apptTime') ? 'quedamos el viernes a las 6 de la tarde' : 'la venta quedó en 2 mil 500' })) }) });
  const facts = { referrals: [{ fullName: 'Julia Ríos', phone: '3125550147' }], followUp: null, products: [{ name: 'Sartén Premium', prices: [] }],
    referralAppointments: [], review: [{ field: 'referral', label: 'Ana Soto', state: 'preguntar' },
      { field: 'referralAppointment', label: 'Julia Ríos', text: 'mañana viernes a las seis', state: 'preguntar' }] };
  // The recording did not say whose house it was: the referral waits and Jorge asks.
  Object.assign(doc, await visit.begin(doc, { facts, contact: {}, closeFacts: { outcome: 'venta' },
    receipt: { referrals: { state: 'host', saved: 0, pending: 1 } } }));
  assert.equal(doc.visitQuestions, 4);
  assert.doesNotMatch(doc.visit.sealed, /Julia|Ana|3125550147/, 'names and phones are sealed at rest');
  const [item] = await visit.pending('seller');
  assert.deepEqual(item.questions.map(question => question.kind), ['host', 'refPhone', 'apptTime', 'sale']);

  const result = await visit.answer('seller', doc._id, spoken);
  assert.equal(result.understood, true);
  assert.deepEqual(result.questions, []);
  assert.deepEqual(calls.host, [{ name: 'Silvia Prueba', phone: '7735550101' }]);
  const saved = calls.save.at(-1);
  assert.equal(saved.contact.name, 'Silvia Prueba');
  assert.deepEqual(saved.facts.referrals.map(row => row.phone), ['3125550147', '3125550109']);
  assert.equal(saved.facts.referralAppointments[0].fullName, 'Julia Ríos');
  assert.equal(saved.facts.referralAppointments[0].anchor, wednesday, '«el viernes» counts from the day of the answer');
  // A connection made after the recording started is used.
  assert.equal(saved.doc.autoClose.sheetsGrant, 'sheets-now');
  assert.deepEqual(calls.outcome, [{ type: 'venta', amount: 2500, products: ['Sartén Premium'], crmId: 'a'.repeat(24),
    note: 'Resultado tomado de la grabación de la demo.' }]);
  const row = analyses.rows[0];
  assert.equal(row.visitQuestions, 0);
  assert.equal(row.visit, null, 'nothing is kept once everything is saved');
  assert.equal(row.closeCrmId, 'a'.repeat(24));
  assert.equal(row.autoCloseReceipt.referrals.saved, 2);
  for (const line of [/Anfitrión: Silvia Prueba/, /2 referidos guardados/, /Cita con Julia Ríos/, /Venta registrada por \$2500/]) assert.match(result.saved.join(' '), line);
  // A second identical answer finds nothing to do and never saves twice.
  assert.equal(await visit.answer('seller', doc._id, spoken), null);
  assert.equal(calls.outcome.length, 1);
});

test('an invented value is ignored, a date that does not add up is asked again and unrelated talk is not an answer', async () => {
  let reply;
  const { visit, calls, doc } = fixture({ answers: () => reply });
  Object.assign(doc, await visit.begin(doc, { facts: { referrals: [{ fullName: 'Julia Ríos', phone: '3125550147' }], followUp: null, referralAppointments: [],
    review: [{ field: 'referral', label: 'Ana Soto', state: 'preguntar' }, { field: 'followUp', label: 'mañana viernes', state: 'preguntar' }] },
  contact: { name: 'Silvia Prueba', phone: '7735550101' }, closeFacts: { outcome: 'follow_up' },
  receipt: { referrals: { state: 'checked', saved: 1, pending: 0 } } }));
  // «Quedó en seguimiento» is recorded alone, with no question.
  assert.deepEqual(calls.outcome.map(item => item.type), ['follow_up']);
  // The model returns a phone the seller never said.
  reply = { later: false, answers: [{ id: 'refPhone:ana soto', fullName: '', phone: '3125550199', when: '', amount: '', choice: '', quote: 'no me lo sé' }] };
  let result = await visit.answer('seller', doc._id, 'El de Ana no me lo sé de memoria');
  assert.equal(result.understood, false);
  assert.equal(result.questions.length, 2);
  // Thursday is tomorrow; «mañana viernes» is asked again.
  reply = { later: false, answers: [{ id: 'followUp:', fullName: '', phone: '', when: 'mañana viernes a las 6 de la tarde', amount: '', choice: '', quote: 'mañana viernes a las 6 de la tarde' }] };
  result = await visit.answer('seller', doc._id, 'Quedamos mañana viernes a las 6 de la tarde');
  assert.match(result.clarify[0], /Dijiste viernes, pero esa fecha cae en jueves 26 de noviembre/);
  assert.equal(result.questions.some(item => item.kind === 'followUp'), true);
  // Something else entirely: not an answer, so the chat can treat it as a normal message.
  reply = { later: false, answers: [] };
  result = await visit.answer('seller', doc._id, '¿Cuánto cuesta el extractor de jugos?');
  assert.equal(result.understood, false);
  // «Después» hides the questions for a while without exact words.
  reply = { later: true, answers: [] };
  result = await visit.answer('seller', doc._id, 'ahorita voy manejando, luego te digo');
  assert.equal(result.later, true);
  assert.deepEqual(await visit.pending('seller'), []);
});

test('what only waited for a Google connection is saved alone when the seller connects', async () => {
  const waiting = { appointment: null, referrals: { state: 'connection', saved: 0, pending: 1 } };
  const { visit, analyses, calls, doc, advance } = fixture({ answers: () => ({ later: false, answers: [] }) });
  Object.assign(doc, await visit.begin(doc, { facts: { referrals: [{ fullName: 'Julia Ríos', phone: '3125550147' }], followUp: null, referralAppointments: [], review: [] },
    contact: { name: 'Silvia Prueba' }, closeFacts: { outcome: 'no_venta' }, receipt: waiting }));
  assert.equal(doc.visitQuestions, 0);
  assert.equal(doc.visitWaiting, true);
  advance(5 * 60_000);
  assert.deepEqual(await visit.pending('seller'), [], 'saved on the next look, nothing left to show');
  assert.equal(calls.save.length, 1);
  assert.equal(analyses.rows[0].autoCloseReceipt.referrals.saved, 1);
  assert.equal(analyses.rows[0].visitWaiting, false);
  assert.equal(analyses.rows[0]['autoClose.sheetsGrant'] ?? analyses.rows[0].autoClose.sheetsGrant, 'sheets-now');
});

test('a follow-up without a date is asked once, «no hay fecha» ends it, and a failed save is retried alone', async () => {
  let reply = { later: false, answers: [{ id: 'followUp:', fullName: '', phone: '', when: '', amount: '', choice: 'no', quote: 'no quedamos en fecha' }] };
  const failing = { appointment: null, referrals: { state: 'error', saved: 0, pending: 1 } };
  const { visit, analyses, calls, doc, advance } = fixture({ answers: () => reply });
  Object.assign(doc, await visit.begin(doc, { facts: { referrals: [{ fullName: 'Julia Ríos', phone: '3125550147' }], followUp: null, referralAppointments: [], review: [] },
    contact: { name: 'Silvia Prueba' }, closeFacts: { outcome: 'follow_up' }, receipt: failing }));
  assert.equal(doc.visitWaiting, true, 'Google failed for a moment: the referral is kept to try again');
  let [item] = await visit.pending('seller');
  assert.deepEqual(item.questions.map(question => question.kind), ['followUp']);
  assert.match(item.questions[0].ask, /quedó en seguimiento, pero no oí la fecha/);
  const result = await visit.answer('seller', doc._id, 'No, no quedamos en fecha todavía');
  assert.equal(result.understood, true);
  assert.deepEqual(result.questions, []);
  // The first look only showed the pending work; the next one, minutes later, saves it.
  advance(5 * 60_000);
  await visit.pending('seller');
  assert.ok(calls.save.length >= 1);
  assert.equal(analyses.rows[0].autoCloseReceipt.referrals.saved, 1);
  assert.equal(analyses.rows[0].visit, null);
});
