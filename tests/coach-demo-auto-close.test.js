import test from 'node:test';
import assert from 'node:assert/strict';
import { createAutoDemoClose, verifyAutoDemoClose } from '../src/coach-demo-auto-close.js';

const ref = { fullName: 'Julia Ríos', phone: '3125550147', segment: 1,
  quote: 'Como referencia anota a Julia Ríos con el número 312-555-0147' };
const followUp = { when: 'mañana a las tres de la tarde', segment: 1,
  quote: 'De acuerdo, llámame mañana a las tres de la tarde' };
const chunks = [{ index: 0, durationSeconds: 180, text: `${ref.quote}. ${followUp.quote}.` }];
const { review: factsReview, ...facts } = verifyAutoDemoClose({ referrals: [ref], followUp }, chunks);
const withoutReview = ({ review, ...rest }) => rest;

test('solo salen datos literales, con el teléfono completo junto al referido y un seguimiento acordado', () => {
  assert.deepEqual(facts, { referrals: [{ fullName: 'Julia Ríos', phone: '3125550147' }], pendingReferrals: 0,
    followUp: { segment: 1, when: followUp.when } });
  // A recording without second readings (older worker) still saves, marked as unchecked.
  assert.deepEqual(factsReview.map(({ field, state, verified }) => [field, state, verified]),
    [['referral', 'guardar', false], ['followUp', 'guardar', false]]);
  const changed = verifyAutoDemoClose({ referrals: [{ ...ref, phone: '3125550148' }], followUp: { ...followUp, when: 'viernes a las 4 pm' } }, chunks);
  assert.deepEqual(changed, { referrals: [], followUp: null, pendingReferrals: 1 });
  const doubt = verifyAutoDemoClose({ referrals: [ref], followUp }, [{ index: 0,
    text: `No estoy segura, ${ref.quote}. ${followUp.quote}, si me queda bien.` }]);
  assert.deepEqual(doubt, { referrals: [], followUp: null, pendingReferrals: 1 });
  const incomplete = 'Como referencia anota a Ana Soto, luego te paso el número';
  assert.deepEqual(withoutReview(verifyAutoDemoClose({ referrals:[], incompleteReferrals:[{ segment:1, quote:incomplete }] },
    [{ index:0, text:incomplete }])), { referrals:[], pendingReferrals:1, followUp:null });
  const asked = '¿Te llamo mañana a las tres de la tarde?';
  assert.equal(verifyAutoDemoClose({ followUp:{ ...followUp, quote:asked } }, [{ index:0, text:asked }]).followUp, null);
  const accepted = `${asked} Sí, está bien.`;
  assert.deepEqual(verifyAutoDemoClose({ followUp:{ ...followUp, quote:accepted } }, [{ index:0, text:accepted }]).followUp, facts.followUp);
});

test('las observaciones se quedan con el referido correcto y los precios con el producto que interesó', () => {
  const observation = 'Julia Ríos es soltera, renta y no trabaja. Para Julia Ríos, llamar después de las seis.';
  const interest = '¿Te interesa el Filtro Premium? Sí, me sirve.';
  const reject = 'No me interesa el Sartén de Hierro.';
  const price = 'El Filtro Premium cuesta $799 dólares. El Filtro Premium queda con $49 al mes.';
  const text = [ref.quote, observation, interest, reject, price].join('. ');
  const verified = verifyAutoDemoClose({ referrals:[{ ...ref, notes:[
    { segment:1, quote:'Julia Ríos es soltera, renta y no trabaja', text:'es soltera, renta y no trabaja' },
    { segment:1, quote:'Para Julia Ríos, llamar después de las seis', text:'llamar después de las seis' },
    { segment:1, quote:'Julia Ríos es soltera, renta y no trabaja', text:'trabaja' },
    { segment:1, quote:'El Filtro Premium cuesta $799 dólares', text:'cuesta $799 dólares' }
  ] }], products:[
    { name:'Filtro Premium', segment:1, quote:interest, prices:[
      { segment:1, quote:'El Filtro Premium cuesta $799 dólares', text:'cuesta $799 dólares' },
      { segment:1, quote:'El Filtro Premium queda con $49 al mes', text:'$49 al mes' },
      { segment:1, quote:'El Filtro Premium queda con $49 al mes', text:'$99 al mes' }
    ] },
    { name:'Sartén de Hierro', segment:1, quote:'me interesa el Sartén de Hierro', prices:[] }
  ] }, [{ index:0, text }]);
  assert.equal(verified.referrals[0].note, 'es soltera, renta y no trabaja · llamar después de las seis');
  assert.deepEqual(verified.products, [{name:'Filtro Premium',prices:['cuesta $799 dólares','$49 al mes']}]);
});

function fixture(opts = {}) {
  const writes = [], indexes = [];
  const file = '1qwertyuiopasdfghjklzxcvbnm12345';
  const analysis = { _id: 'demo-ficticia', closeVisitId:'visit-ficticia', ownerUserId:'seller', ownerName:'Vendedor',
    chunks, startedAt: '2026-11-23T23:30:00Z', autoClose: { zone:'America/Chicago', calendarGrant:'calendar-original', sheetsGrant:'sheets-original' } };
  const calendar = {
    getConnection: async owner => { assert.equal(owner, 'seller'); return { calendarId:'own-calendar', grant:opts.changed ? 'other' : 'calendar-original', availabilityEnabled:true }; },
    saveEvent: async event => {
      writes.push(['calendar',event]);
      if (opts.busy) throw Object.assign(new Error('ocupado'), {slotBusy:true});
      return { htmlLink:'https://calendar.google.com/calendar/event?eid=test', etag:'abc' };
    }
  };
  const followups = { async rememberEvent(binding, receipt) { indexes.push([binding, receipt]); return '12345'; } };
  const sheets = {
    getConnection: async owner => { assert.equal(owner, 'seller'); return { grant:opts.changed ? 'other' : 'sheets-original', spreadsheetId:file }; },
    access: async () => ({ grant:'sheets-original', spreadsheetId:file, token:'test' }),
    current: async () => true
  };
  const transfer = {
    async preview(ctx, draft) {
      assert.equal(await ctx.isCurrent(), true);
      assert.equal(draft.hostName, 'Silvia Prueba');
      assert.equal(draft.kind, 'referrals');
      return { sheetTitle:'4 en 14', previewId:'test-preview', rows:draft.referrals.map((_, index) => ({ index,
        status:['new','duplicate','possible'][index] })) };
    },
    async confirm(ctx, selection) {
      assert.equal(await ctx.isCurrent(), true);
      writes.push(['sheets',selection]);
      return { written:selection.selected.length, sheetTitle:'4 en 14' };
    }
  };
  const run = createAutoDemoClose({ calendar, followups, sheets, transfer, active:async () => !opts.inactive,
    now:() => Date.parse('2026-11-24T14:00:00Z') });
  return { run, analysis, writes, indexes, contact: { name:'Silvia Prueba', phone:'3125550133' } };
}

test('guarda de forma independiente la cita en la zona de la demo y solo los referidos nuevos', async () => {
  const { run, analysis, writes, indexes, contact } = fixture();
  const report = { ...facts, referrals: [...facts.referrals, { fullName:'Ya Existente', phone:'3125550152' }, { fullName:'Requiere Revisar', phone:'3125550153' }] };
  const first = await run(analysis, report, contact);
  assert.equal(first.appointment.state, 'saved');
  assert.equal(first.appointment.start, '2026-11-24T21:00:00.000Z'); // «mañana» relative to capture, not processing.
  assert.deepEqual(first.referrals, { state:'checked', saved:1, pending:1, duplicates:1, sheetTitle:'4 en 14', file:'1qwertyuiopasdfghjklzxcvbnm12345' });
  const booking = writes.find(([provider]) => provider==='calendar')[1];
  assert.equal(booking.checkAvailability, true);
  assert.equal(booking.retryGoogle, true);
  assert.equal(booking.attendees, undefined);
  assert.deepEqual(writes.find(([provider]) => provider==='sheets')[1].selected, [0]);
  assert.equal(indexes[0][1].eventId, booking.eventId);
  await run({ ...analysis, _id:'reingreso' }, report, contact);
  assert.equal(writes.filter(([provider]) => provider==='calendar')[1][1].eventId, booking.eventId);
});

test('un horario ocupado no detiene los referidos y las conexiones nuevas no reemplazan el destino original', async () => {
  const busy = fixture({ busy:true });
  const first = await busy.run(busy.analysis, facts, busy.contact);
  assert.equal(first.appointment.state, 'busy');
  assert.equal(first.referrals.saved, 1);
  const changed = fixture({ changed:true });
  const second = await changed.run(changed.analysis, facts, changed.contact);
  assert.equal(second.appointment.state, 'connection');
  assert.equal(second.referrals.state, 'connection');
  assert.equal(changed.writes.length, 0);
});

test('una hora sin periodo, un cierre truncado y un referido sin anfitrión se dejan pendientes', async () => {
  const { run, analysis, writes, contact } = fixture();
  const unclear = await run(analysis, { ...facts, followUp: { ...facts.followUp, when:'mañana a las tres' } }, {});
  assert.equal(unclear.appointment.state, 'time');
  assert.equal(unclear.referrals.state, 'host');
  assert.equal(writes.length, 0);
  const partial = await run(analysis, facts, contact, { partial:true });
  assert.equal(partial.appointment.state, 'time');
  assert.equal(partial.referrals.saved, 1);
});

test('un referido dictado para el 4 en 14 sin decir «referido» se guarda y un «agéndame» o «nos vemos» cuenta como acuerdo', () => {
  const said = [
    ['Anota a mi prima Juana Pérez, el número es 773-555-0145', 'Juana Pérez', '7735550145'],
    ['Para el 4 en 14, Pedro Ramírez, 773 555 0156, es mi compadre', 'Pedro Ramírez', '7735550156'],
    ['Mi hermana Rosa Gil, su teléfono es 773-555-0178', 'Rosa Gil', '7735550178']
  ];
  for (const [quote, fullName, phone] of said) {
    const saved = verifyAutoDemoClose({ referrals:[{ fullName, phone, segment:1, quote, notes:[] }] }, [{ index:0, text:quote }]);
    assert.deepEqual(saved.referrals, [{ fullName, phone }], quote);
  }
  // A name without a phone, or just mentioned, is still not saved.
  const mentioned = 'Mi prima Juana Pérez también cocina mucho';
  assert.deepEqual(verifyAutoDemoClose({ referrals:[{ fullName:'Juana Pérez', phone:'7735550145', segment:1, quote:mentioned }] },
    [{ index:0, text:mentioned }]).referrals, []);
  for (const quote of ['Sí, agéndame la cita mañana viernes a las 6pm', 'Ok, nos vemos mañana viernes a las 6 de la tarde para la demo']) {
    const when = quote.match(/mañana viernes a las (?:6pm|6 de la tarde)/)[0];
    assert.deepEqual(verifyAutoDemoClose({ followUp:{ when, segment:1, quote } }, [{ index:0, text:quote }]).followUp, { when, segment:1 }, quote);
  }
  const doubtful = 'Tal vez nos vemos mañana a las 6 de la tarde';
  assert.equal(verifyAutoDemoClose({ followUp:{ when:'mañana a las 6 de la tarde', segment:1, quote:doubtful } }, [{ index:0, text:doubtful }]).followUp, null);
});

test('«mañana viernes» cuando mañana es jueves no se agenda: Jorge pregunta el día', async () => {
  const { run, analysis, writes, contact } = fixture();
  // The demo was recorded on Monday 2026-11-23 in Chicago, so «mañana» is Tuesday.
  const report = { ...facts, review:[{ field:'followUp', label:'mañana viernes a las 6 de la tarde', state:'guardar' }],
    followUp:{ ...facts.followUp, when:'mañana viernes a las 6 de la tarde' } };
  const result = await run(analysis, report, contact);
  assert.equal(result.appointment.state, 'time');
  assert.match(result.appointment.question, /Dijiste viernes, pero esa fecha cae en martes 24 de noviembre/);
  assert.equal(writes.filter(([provider]) => provider === 'calendar').length, 0);
  assert.deepEqual(report.review.map(({ field, state }) => [field, state]), [['followUp', 'preguntar']]);
  assert.match(report.review[0].text, /¿Qué día quieres\?/);
  assert.equal(result.referrals.saved, 1);
});

test('Jorge pregunta el día exacto que no cuadró y el chat de citas tampoco adivina', async () => {
  const { demoCloseQuestions } = await import('../src/coach-demo-close.js');
  const { readFollowup } = await import('../src/coach-followup-input.js');
  const reason = 'Dijiste viernes, pero esa fecha cae en jueves 26 de noviembre. ¿Qué día quieres?';
  const questions = demoCloseQuestions({ fields:{}, confirmed:{}, sellerFields:[], declaredOutcome:'follow_up',
    doubts:[{ field:'followUp', label:'mañana viernes a las 6 de la tarde', text:reason, heard:[] }] });
  assert.deepEqual(questions, [{ field:'followUp', question:'Del seguimiento «mañana viernes a las 6 de la tarde»: ' + reason }]);
  const wednesday = { timeZone:'America/Chicago', now:Date.parse('2026-11-25T20:00:00Z') };
  assert.equal(readFollowup('mañana viernes a las 6 de la tarde', {}, wednesday).question, reason);
  assert.equal(readFollowup('mañana jueves a las 6 de la tarde', {}, wednesday).question, undefined);
  assert.equal(readFollowup('el viernes a las 8 de la mañana', {}, wednesday).question, undefined);
});

test('una cita instantánea del 4 en 14 se agenda en Calendar, queda en la nota del referido y, si el día no cuadra, Jorge la pregunta', async () => {
  const referral = 'Como referencia anota a Julia Ríos con el número 312-555-0147';
  const call = 'Julia, ¿te parece si paso el viernes a las seis de la tarde para la demo? Sí, está bien';
  const wrong = 'Entonces Julia, quedamos mañana viernes a las seis de la tarde para la demo';
  const text = `${referral}. ${call}. ${wrong}.`;
  const instant = verifyAutoDemoClose({
    referrals: [{ fullName: 'Julia Ríos', phone: '3125550147', segment: 1, quote: referral, notes: [] }],
    referralAppointments: [
      { fullName: 'Julia Ríos', when: 'el viernes a las seis de la tarde', segment: 1, quote: call },
      { fullName: 'Persona Inventada', when: 'el sábado a las diez', segment: 1, quote: 'Persona Inventada, ¿el sábado a las diez para la demo? Sí, está bien' }
    ]
  }, [{ index: 0, durationSeconds: 180, text }]);
  assert.deepEqual(instant.referralAppointments, [{ fullName: 'Julia Ríos', phone: '3125550147', when: 'el viernes a las seis de la tarde', segment: 1 }],
    'only a referral saved in this visit, with an explicit yes');
  assert.match(instant.referrals[0].note, /Cita instantánea: el viernes a las seis de la tarde/);
  // A proposal without a yes is not an appointment.
  const asked = 'Julia, ¿te parece si paso el viernes a las seis de la tarde para la demo?';
  assert.equal(verifyAutoDemoClose({ referrals: [{ fullName: 'Julia Ríos', phone: '3125550147', segment: 1, quote: referral }],
    referralAppointments: [{ fullName: 'Julia Ríos', when: 'el viernes a las seis de la tarde', segment: 1, quote: asked }] },
  [{ index: 0, text: `${referral}. ${asked}` }]).referralAppointments, undefined);

  const { run, analysis, writes, contact } = fixture();
  const saved = await run(analysis, { ...facts, referralAppointments: instant.referralAppointments }, contact);
  assert.equal(saved.referralAppointments[0].state, 'saved');
  // The demo was recorded on Monday 2026-11-23 in Chicago: «el viernes» is Friday the 27th, 6 pm.
  assert.equal(saved.referralAppointments[0].start, '2026-11-28T00:00:00.000Z');
  const events = writes.filter(([provider]) => provider === 'calendar').map(([, event]) => event);
  assert.deepEqual(events.map(event => event.summary).sort(), ['Demo · Julia Ríos (4 en 14 de Silvia Prueba)', 'Seguimiento · Silvia Prueba']);
  assert.notEqual(events[1].eventId, events[0].eventId);

  const doubtful = { ...facts, review: [], referralAppointments: [{ fullName: 'Julia Ríos', phone: '3125550147', when: 'mañana viernes a las seis de la tarde', segment: 1 }] };
  const asksJorge = await run(analysis, doubtful, contact);
  assert.equal(asksJorge.referralAppointments[0].state, 'time');
  assert.deepEqual(doubtful.review.map(({ field, state, label }) => [field, state, label]), [['referralAppointment', 'preguntar', 'Julia Ríos']]);
  assert.match(doubtful.review[0].reason, /Dijiste viernes, pero esa fecha cae en martes 24 de noviembre/);
});
