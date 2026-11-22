import test from 'node:test';
import assert from 'node:assert/strict';
import { cleanDemoCloseFacts, verifiedDemoCloseFacts, extractSellerCloseReply } from '../src/coach-demo-close-facts.js';

const fact = (field, value, quote, segment = 1) => ({ field, value, quote, segment });
const chunks = text => [{ index: 0, text }];

test('propone el seguimiento y la encuesta a partir de evidencia de la visita, y distingue una cotización', () => {
  const household = 'Cocino para cuatro y gastamos treinta por semana en garrafón.';
  const callback = 'Márcame el martes después de las cinco.';
  const quote = 'La mensualidad sería de noventa.';
  const transcript = [{ index: 0, text: household }, { index: 3, text: callback + ' ' + quote }];
  const result = verifiedDemoCloseFacts([
    fact('cooksForCount', '4', household), fact('waterSpendWeekly', 'treinta por semana', household),
    fact('followUp', callback, callback, 4), fact('outcome', 'follow_up', callback, 4),
    fact('quotedMonthly', '90', quote, 4), fact('saleAmount', '90', quote, 4),
    fact('reason', 'Le encantó el producto.', 'Le encantó el producto.'), fact('phone', '3050000000', household),
    fact('productInterest', 'olla', 'Me gustó la olla.', 2)
  ], transcript);
  assert.deepEqual(result, { cooksForCount: '4', waterSpendWeekly: 'treinta por semana', followUp: callback,
    outcome: 'follow_up', quotedMonthly: '90.00' });
  assert.equal(JSON.stringify(result).includes(household), false);
});

test('descarta campos dudosos, contradicciones, fragmentos ausentes y montos sin respaldo', () => {
  const first = 'El precio base de la olla es 900 dólares. Doy 100 de anticipo.';
  const second = 'Para comer somos tres. Antes cocinaba para cinco.';
  assert.deepEqual(verifiedDemoCloseFacts([
    fact('calcProduct', 'olla', first), fact('calcPrice', '900', first), fact('calcDown', '999', first),
    fact('saleAmount', '900', first), fact('cooksForCount', '3', second), fact('cooksForCount', '5', second),
    fact('outcome', 'venta', 'Acepto el pedido.', 2), fact('calcDown', '100', first, 2)
  ], chunks(first + ' ' + second)), { calcProduct: 'olla', calcPrice: '900.00' });
  assert.deepEqual(verifiedDemoCloseFacts([fact('quotedMonthly', '30', 'Cuota mensual de ciento treinta.')],
    chunks('Cuota mensual de ciento treinta.')), {});
});

test('descarta conversiones de monedas, cifras de zona, instrucciones, teléfonos y diagnósticos', () => {
  const base = 'El precio base de la olla es 900 pesos.';
  const report = 'El reporte de EWG menciona para la muestra del grifo 200 ppm.';
  assert.deepEqual(verifiedDemoCloseFacts([
    fact('calcProduct', 'olla', base), fact('calcPrice', '900', base),
    fact('waterReading', 'muestra del grifo 200 ppm', report),
    fact('reason', 'tengo diabetes', 'porque tengo diabetes'), fact('followUp', 'Llama al 305-555-0100', 'Llama al 305-555-0100')
  ], chunks(base + ' ' + report + ' porque tengo diabetes. Llama al 305-555-0100.')), { calcProduct: 'olla' });
  assert.deepEqual(cleanDemoCloseFacts({ name: 'Alguien', transcript: 'datos privados', familyConditions: 'datos clínicos',
    calcPrice: '-90', cooksForCount: '0', waterReading: '200', followUp: 'texto <b>' }), {});
});

test('el silencio, una frase truncada, una pregunta y el condicional no prueban una venta', () => {
  for (const [text, excerpt] of [
    ['No lo compro.', 'lo compro'], ['Si me bajan el precio, lo compro.', 'lo compro'],
    ['¿Me lo llevo?', 'Me lo llevo'], ['Si me alcanza lo compro.', 'Si me alcanza lo compro.'],
    ['La compraremos después.', 'La compraremos después.']
  ]) assert.deepEqual(verifiedDemoCloseFacts([fact('outcome', 'venta', excerpt)], chunks(text)), {});
  const yes = 'Me lo llevo. Acepto el total de 1,200.';
  assert.deepEqual(verifiedDemoCloseFacts([fact('outcome', 'venta', yes), fact('saleAmount', '1200', yes)], chunks(yes)),
    { outcome: 'venta', saleAmount: '1200.00' });
});

test('agrega al esquema de la misma petición solo el cierre de una visita nueva vinculada y quita las citas del reporte', async t => {
  process.env.NODE_ENV = 'test';
  const { analizarCoachDemoTranscript } = await import('../server.js');
  const oldKey = process.env.OPENAI_API_KEY;
  process.env.OPENAI_API_KEY = 'test';
  t.after(() => { if (oldKey === undefined) delete process.env.OPENAI_API_KEY; else process.env.OPENAI_API_KEY = oldKey; });
  const text = 'La muestra de la llave midió 210 ppm.';
  const requests = [];
  t.mock.method(globalThis, 'fetch', async (_url, options) => {
    requests.push(JSON.parse(options.body));
    return Response.json({ output_text: JSON.stringify({ title: 'Resumen', closeFacts: [
      fact('waterReading', 'muestra de la llave midió 210 ppm', text),
      fact('saleAmount', '1200', 'datos que no se grabaron.')
    ] }) });
  });
  const old = await analizarCoachDemoTranscript({ transcript: text });
  const pilot = await analizarCoachDemoTranscript({ transcript: text, closeSegments: chunks(text) });
  assert.deepEqual(old, { title: 'Resumen' });
  assert.deepEqual(pilot.closeFacts, { waterReading: 'muestra de la llave midió 210 ppm' });
  assert.equal(requests.length, 2);
  assert.equal(requests[0].text.format.schema.properties.closeFacts, undefined);
  assert.deepEqual(requests[1].text.format.schema.properties.closeFacts.items.required, ['field', 'value', 'segment', 'quote']);
});

test('aclara por voz la hora del mismo seguimiento con una llamada y sin enviar historial, identidad ni grabación al modelo', async t => {
  const oldKey = process.env.OPENAI_API_KEY;
  process.env.OPENAI_API_KEY = 'test';
  t.after(() => { if (oldKey === undefined) delete process.env.OPENAI_API_KEY; else process.env.OPENAI_API_KEY = oldKey; });
  const requests = [];
  t.mock.method(globalThis,'fetch',async (_url, options) => {
    requests.push(JSON.parse(options.body));
    const proposed = {changes:[{field:'followUp',value:'a las seis',quote:'a las seis'}],note:'',noteQuote:''};
    return Response.json({choices:[{message:{content:JSON.stringify(proposed)}}]});
  });
  const result = await extractSellerCloseReply('a las seis',{
    fields:{outcome:'follow_up',followUp:'Llamar el martes después de las cinco.',transcript:'no compartir transcripción',fullName:'no compartir identidad'},
    questions:[{field:'followUp',question:'¿A qué hora?'}]
  });
  assert.deepEqual(result,{fields:{followUp:'Llamar el martes a las seis'}});
  assert.equal(requests.length,1);
  assert.equal(requests[0].store,false);
  assert.equal(JSON.stringify(requests[0]).includes('no compartir'),false);
});

test('la respuesta del vendedor aporta compra reportada sin inventar total y las notas tienen un vocabulario comercial acotado', async () => {
  const proposed = {changes:[{field:'outcome',value:'venta',quote:'sí compraron'},
    {field:'saleAmount',value:'900',quote:'mañana confirmo el monto'}],note:'pendingAmount',noteQuote:'mañana confirmo el monto'};
  const options = {request:async () => proposed};
  const result = await extractSellerCloseReply('sí compraron; mañana confirmo el monto', {questions:[],fields:{}}, options);
  assert.deepEqual(result,{fields:{outcome:'venta'},note:'El monto queda pendiente de confirmar.'});
  const malicious = {request: async () => ({changes:[],note:'Diagnóstico privado',noteQuote:'marcar mañana'})};
  assert.deepEqual(await extractSellerCloseReply('marcar mañana',{},malicious),{fields:{}});
});

test('asigna una cifra escueta solo a una pregunta inequívoca; omite hipótesis, aplazamientos y respuestas fuera de tema', async () => {
  const context = {fields:{outcome:'venta'},questions:[{field:'saleAmount',question:'¿En cuánto quedó la venta?'}]};
  const options = {request:async () => ({changes:[{field:'saleAmount',value:'1200',quote:'mil doscientos'}]})};
  assert.deepEqual(await extractSellerCloseReply('mil doscientos',context,options),{fields:{saleAmount:'1200.00'}});
  for (const text of ['quizás mil doscientos','si fuera mil doscientos','son mil doscientos pesos','no sé','después']) {
    assert.deepEqual(await extractSellerCloseReply(text,context,options),{fields:{}});
  }
  assert.deepEqual(await extractSellerCloseReply('mil doscientos',{...context,questions:[...context.questions,{field:'quotedMonthly',question:'¿De cuánto era la cuota?'}]},options),{fields:{}});
  const irrelevant = {request:async () => ({changes:[{field:'followUp',value:'buen clima',quote:'buen clima'}]})};
  assert.deepEqual(await extractSellerCloseReply('Qué buen clima',context,irrelevant),{fields:{}});
});

test('una respuesta corta a cómo terminó la visita aporta solo el resultado declarado, jamás una confirmación o una cita', async () => {
  const context={questions:[{field:'outcome',question:'¿Cómo terminó la visita?'}],fields:{}};
  const noModel={request:async()=>assert.fail('Una respuesta inequívoca no requiere inferencia')};
  for (const [answer,outcome] of [
    ['Venta.','venta'],['Sí, vendí.','venta'],['fue venta','venta'],['Sí, fue venta.','venta'],['hubo una venta','venta'],
    ['No hubo venta','no_venta'],['no vendimos','no_venta'],['quedó para seguimiento','follow_up'],['Seguimiento.','follow_up']
  ]) assert.deepEqual(await extractSellerCloseReply(answer,context,noModel),{fields:{outcome}});
  assert.deepEqual(await extractSellerCloseReply('venta',{...context,fields:{outcome:'venta'}},noModel),{fields:{outcome:'venta'}});
  assert.deepEqual(await extractSellerCloseReply('sí',context,noModel),{fields:{}});
  const noChanges={request:async()=>({changes:[]})};
  for (const answer of ['¿Fue venta?','hubiera sido venta','si vendo te aviso']) {
    assert.deepEqual(await extractSellerCloseReply(answer,context,noChanges),{fields:{}});
  }
  assert.deepEqual(await extractSellerCloseReply('venta',{questions:[{field:'saleAmount',question:'¿Cuál fue el total?'}]},noChanges),{fields:{}});
});
