import test from 'node:test';
import assert from 'node:assert/strict';
import express from 'express';
import { classifyButtonSpeech, registerOneButton } from '../src/coach-one-button.js';

test('Jorge decide si empieza una visita o es un mensaje, sin frases fijas y sin inventar el nombre', async () => {
  const asked = [];
  const model = answer => async ({ instruction }) => { asked.push(JSON.parse(instruction).dijo); return answer; };
  assert.deepEqual(await classifyButtonSpeech('Ya llegué con la señora Lucía Prueba, voy a empezar', { request: model({ kind: 'visita', host: 'Lucía Prueba' }) }),
    { kind: 'visita', host: 'Lucía Prueba' });
  // A name the seller never said is not shown.
  assert.deepEqual(await classifyButtonSpeech('Voy a empezar la demo', { request: model({ kind: 'visita', host: 'Marta Inventada' }) }), { kind: 'visita', host: '' });
  assert.deepEqual(await classifyButtonSpeech('¿Qué garantía tiene el sartén?', { request: model({ kind: 'mensaje', host: '' }) }), { kind: 'mensaje', host: '' });
  // If the model fails or the text is long, it is a message: the seller is never left without an answer.
  assert.deepEqual(await classifyButtonSpeech('Empiezo la visita', { request: async () => { throw new Error('sin modelo'); } }), { kind: 'mensaje', host: '' });
  const before = asked.length;
  assert.deepEqual(await classifyButtonSpeech('El teléfono de Pedro es 708 555 0188. '.repeat(20), { request: model({ kind: 'visita', host: '' }) }), { kind: 'mensaje', host: '' });
  assert.equal(asked.length, before, 'a long message never reaches the model');
  assert.deepEqual(await classifyButtonSpeech('', {}), { kind: 'mensaje', host: '' });
});

test('/boton abre la app en el botón único y la decisión solo se pide desde la página del piloto', async t => {
  const app = express();
  app.use(express.json());
  let allowed = true;
  registerOneButton(app, { active: async () => ({ user: { _id: 'seller' } }), allowed: () => allowed, classify: async text => ({ kind: text.includes('empiezo') ? 'visita' : 'mensaje', host: '' }) });
  const server = app.listen(0, '127.0.0.1');
  await new Promise(resolve => server.once('listening', resolve));
  t.after(() => new Promise(resolve => server.close(resolve)));
  const base = `http://127.0.0.1:${server.address().port}`;
  const page = await fetch(base + '/boton', { redirect: 'manual' });
  assert.equal(page.status, 302);
  assert.equal(page.headers.get('location'), '/coach/app/?boton=1');
  const ask = (text, headers = { Origin: base }) => fetch(base + '/api/coach/boton/intent', { method: 'POST',
    headers: { 'Content-Type': 'application/json', ...headers }, body: JSON.stringify({ text }) });
  assert.deepEqual(await (await ask('ya empiezo la visita')).json(), { kind: 'visita', host: '' });
  assert.equal((await ask('hola', { Origin: 'https://otro.example' })).status, 403);
  allowed = false;
  assert.equal((await ask('hola')).status, 403);
});
