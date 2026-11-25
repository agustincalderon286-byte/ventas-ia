import test from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import vm from 'node:vm';
import { verifyAutoDemoClose } from '../src/coach-demo-auto-close.js';
import { verifiedDemoCloseFacts } from '../src/coach-demo-close-facts.js';
import { checkDemoFigures, demoFigureGuard } from '../src/coach-demo-figures.js';

// Real output of the Mac worker (scripts/tec-demo-worker.py) for a fictitious close read by two
// synthetic voices: clean, with «setecientos» half covered by noise, and almost fully covered.
// The last one reproduces the pilot error: Whisper heard «99 dólares» where the client heard 799.
const readings = JSON.parse(readFileSync(new URL('./fixtures/demo-cierre-ficticio-lecturas.json', import.meta.url), 'utf8'));
const chunksOf = name => [{ ...structuredClone(readings[name]), index: 0, durationSeconds: 40 }];

// What Luna returns for these visits. Quotes are literal fragments of each transcript.
function lunaFor(name) {
  const text = readings[name].text;
  const priceQuote = text.match(/El filtro premium cuesta[^.]*\.?[^.]*impuestos/i)[0];
  const priceText = priceQuote.match(/\$?\d+ d[oó]lares/)[0];
  return {
    referrals: [{ fullName: 'Julia Ríos', phone: '3125550147', segment: 1,
      quote: 'le doy una referencia, anote a Julia Ríos, su número es 312-555-0147', notes: [
        { segment: 1, quote: 'Julia Ríos es soltera, renta su casa y trabaja por la mañana', text: 'es soltera' },
        { segment: 1, quote: 'Julia Ríos es soltera, renta su casa y trabaja por la mañana', text: 'renta su casa' },
        { segment: 1, quote: 'Julia Ríos es soltera, renta su casa y trabaja por la mañana', text: 'trabaja por la mañana' }
      ] }],
    products: [{ name: 'Filtro Premium', segment: 1, quote: '¿Le interesa el filtro premium? Sí, me interesa el filtro de agua',
      prices: [{ segment: 1, quote: priceQuote, text: priceText }] }],
    followUp: { segment: 1, when: 'mañana a las 4 de la tarde',
      quote: '¿Le parece si regreso mañana a las 4 de la tarde? Sí, de acuerdo' },
    survey: [{ segment: 1, field: 'drinkingWaterType', values: ['Botella'],
      quote: '¿qué agua toman en la casa? Tomamos agua de botella' }]
  };
}
const table = review => review.map(({ field, state, text, label }) => `${field}:${state}:${text || label}`);

test('caso 799: con audio claro se guarda el precio y con la cifra tapada queda para preguntar, sin frenar lo demás', () => {
  for (const name of ['cierre-ficticio', 'tapado-0.25']) {
    const facts = verifyAutoDemoClose(lunaFor(name), chunksOf(name));
    assert.deepEqual(facts.products, [{ name: 'Filtro Premium', prices: [lunaFor(name).products[0].prices[0].text] }], name);
    assert.equal(facts.review.some(item => item.state !== 'guardar'), false, name);
    assert.equal(facts.review.some(item => item.verified === false), false, name);
  }

  const covered = verifyAutoDemoClose(lunaFor('tapado-0.12'), chunksOf('tapado-0.12'));
  assert.deepEqual(covered.products, [{ name: 'Filtro Premium', prices: [] }]);
  const price = covered.review.find(item => item.field === 'price');
  assert.equal(price.state, 'preguntar');
  assert.equal(price.text, '99 dólares');
  assert.match(price.reason, /voz sin transcribir/);
  // The referral, its three notes, the follow-up and the survey are still saved.
  assert.deepEqual(covered.referrals.map(row => row.note), ['es soltera · renta su casa · trabaja por la mañana']);
  assert.deepEqual(covered.followUp, { when: 'mañana a las 4 de la tarde', segment: 1 });
  assert.deepEqual(covered.survey, { drinkingWaterType: 'Botella' });
  assert.deepEqual(table(covered.review), [
    'note:guardar:es soltera', 'note:guardar:renta su casa', 'note:guardar:trabaja por la mañana', 'referral:guardar:Julia Ríos',
    'followUp:guardar:mañana a las 4 de la tarde', 'survey:guardar:Botella', 'price:preguntar:99 dólares'
  ]);

  // The reviewable close leaves the price out too, so the seller is asked instead of shown 99.
  const quote = 'El filtro premium cuesta. 99 dólares, antes de impuestos';
  const chunks = chunksOf('tapado-0.12');
  const candidates = [{ field: 'calcProduct', value: 'Filtro Premium', segment: 1, quote: 'El filtro premium cuesta' },
    { field: 'calcPrice', value: '99', segment: 1, quote }];
  assert.deepEqual(verifiedDemoCloseFacts(candidates, chunks), { calcProduct: 'Filtro Premium', calcPrice: '99.00' });
  assert.deepEqual(verifiedDemoCloseFacts(candidates, chunks, demoFigureGuard(chunks)), { calcProduct: 'Filtro Premium' });
});

test('si la segunda lectura oye otra cifra, falta o el teléfono no coincide, ese dato se pregunta', () => {
  const misheard = chunksOf('cierre-ficticio');
  const sentence = misheard[0].voice.findIndex(item => /799/.test(item.text));
  misheard[0].voice[sentence].text = misheard[0].voice[sentence].text.replace('799', '99');
  misheard[0].text = misheard[0].text.replace('799', '99');
  const luna = lunaFor('cierre-ficticio');
  luna.products[0].prices[0] = { segment: 1, quote: 'El filtro premium cuesta 99 dólares, antes de impuestos', text: 'cuesta 99 dólares' };
  const doubt = checkDemoFigures(misheard, { segment: 1, kind: 'amount', values: ['99'], quote: luna.products[0].prices[0].quote });
  assert.deepEqual(doubt, { state: 'doubt', reason: 'La segunda lectura oyó otra cifra.', heard: ['799'] });
  const facts = verifyAutoDemoClose(luna, misheard);
  assert.deepEqual(facts.review.find(item => item.field === 'price'), { field: 'price', label: 'Filtro Premium', state: 'preguntar',
    text: 'cuesta 99 dólares', reason: 'La segunda lectura oyó otra cifra.', heard: ['799'], minute: 0 });

  const phone = chunksOf('cierre-ficticio');
  const call = phone[0].voice.findIndex(item => /0147/.test(item.text));
  phone[0].checks.find(item => item.segment === call).second = 'anote a julia ríos su número es 3 1 2 5 5 5 0 1 7 4';
  const unsure = verifyAutoDemoClose(lunaFor('cierre-ficticio'), phone);
  assert.deepEqual(unsure.referrals, []);
  assert.equal(unsure.pendingReferrals, 1);
  assert.match(unsure.review.find(item => item.field === 'referral').reason, /^Teléfono: La segunda lectura/);

  const noSecond = chunksOf('cierre-ficticio');
  noSecond[0].checks.forEach(item => { item.second = null; });
  const withoutModel = verifyAutoDemoClose(lunaFor('cierre-ficticio'), noSecond);
  assert.deepEqual(withoutModel.referrals, []);
  assert.equal(withoutModel.followUp, null);
  assert.deepEqual(withoutModel.products[0].prices, []);
  assert.equal(withoutModel.review.filter(item => item.state === 'preguntar').length, 3);
});

test('las notas se quedan con su referido aunque la frase no repita el nombre, sin cruzarse ni perder el no', () => {
  const text = 'Le doy una referencia: anote a Julia Ríos, su número es 312-555-0147. Es soltera, renta su casa y trabaja por la mañana. ' +
    'Otra referencia es Pedro Gómez, su número es 312-555-0158. Es casado y no trabaja. El filtro premium cuesta 799 dólares. Yo también rento.';
  const chunks = [{ index: 0, text, durationSeconds: 60 }];
  const sentence = 'Es soltera, renta su casa y trabaja por la mañana';
  const facts = verifyAutoDemoClose({ referrals: [
    { fullName: 'Julia Ríos', phone: '3125550147', segment: 1, quote: 'referencia: anote a Julia Ríos, su número es 312-555-0147', notes: [
      { segment: 1, quote: sentence, text: 'Es soltera' }, { segment: 1, quote: sentence, text: 'renta su casa' },
      { segment: 1, quote: sentence, text: 'trabaja por la mañana' },
      // Crossed: this sentence comes after Pedro was named.
      { segment: 1, quote: 'Es casado y no trabaja', text: 'Es casado' },
      // Not personal: a price never becomes a referral note.
      { segment: 1, quote: 'El filtro premium cuesta 799 dólares', text: 'cuesta 799 dólares' }
    ] },
    { fullName: 'Pedro Gómez', phone: '3125550158', segment: 1, quote: 'Otra referencia es Pedro Gómez, su número es 312-555-0158', notes: [
      { segment: 1, quote: 'Es casado y no trabaja', text: 'Es casado' },
      { segment: 1, quote: 'Es casado y no trabaja', text: 'trabaja' },
      { segment: 1, quote: 'Es casado y no trabaja', text: 'no trabaja' },
      // First person: the host talking about herself.
      { segment: 1, quote: 'Yo también rento', text: 'rento' }
    ] }
  ] }, chunks);
  assert.deepEqual(facts.referrals.map(row => [row.fullName, row.note]), [
    ['Julia Ríos', 'Es soltera · renta su casa · trabaja por la mañana'],
    ['Pedro Gómez', 'Es casado · no trabaja']
  ]);
  assert.deepEqual(facts.review.filter(item => item.field === 'note').map(item => item.kinds),
    [['estadoCivil'], ['vivienda'], ['trabajo', 'disponibilidad'], ['estadoCivil'], ['trabajo']]);

  // An observation Luna skipped is listed for the seller, never written.
  const missed = verifyAutoDemoClose({ referrals: [{ fullName: 'Julia Ríos', phone: '3125550147', segment: 1,
    quote: 'referencia: anote a Julia Ríos, su número es 312-555-0147', notes: [{ segment: 1, quote: sentence, text: 'trabaja por la mañana' }] }] }, chunks);
  assert.equal(missed.referrals[0].note, 'trabaja por la mañana');
  assert.deepEqual(missed.review.filter(item => item.state === 'pendiente').map(item => item.text), ['es soltera', 'renta su casa']);
});

test('el recibo de Mis demostraciones dice qué no se guardó y por qué, sin teléfonos', () => {
  const app = readFileSync(new URL('../public/coach/app.js', import.meta.url), 'utf8');
  const source = app.slice(app.indexOf('function renderDemoAutoCloseReceipt'), app.indexOf('function initCoachDemoRecorder'));
  const escapeHtml = value => String(value).replace(/[&<>"]/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;' })[c]);
  const render = vm.runInNewContext(`(${source.trim()})`, { escapeHtml, Intl, URL });
  const { review } = verifyAutoDemoClose(lunaFor('tapado-0.12'), chunksOf('tapado-0.12'));
  const html = render({ referrals: { state: 'checked', saved: 1, pending: 0, sheetTitle: '4 en 14' }, review: [...review,
    { field: 'note', label: 'Julia Ríos', state: 'pendiente', text: 'tiene <dos> hijos' }] });
  assert.match(html, /No lo guardé porque no quedó claro/);
  assert.match(html, /Precio de Filtro Premium: oí «99 dólares», pero pudo quedar cortado\. · minuto 0/);
  assert.match(html, /Julia Ríos · «tiene &lt;dos&gt; hijos»/);
  assert.equal(/3125550147|312-555-0147/.test(html), false);
});
