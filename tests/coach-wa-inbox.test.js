import assert from 'node:assert/strict';
import { createHmac } from 'node:crypto';
import test from 'node:test';
import express from 'express';
import { COACH_WA_INBOX_WEBHOOK, createCoachWaInbox, waInboxConfig, waInboxMessage } from '../src/coach-wa-inbox.js';
import { callReply, inboxReply } from '../src/coach-wa-inbox-actions.js';
import { connectionsPage } from '../src/coach-connections-page.js';
import { coachWaInboxTransport } from '../src/coach-wa-inbox-transport.js';

const now = Date.parse('2026-11-27T16:00:00Z');
const key = Buffer.alloc(32, 1);
const seller = 'vendedora@example.com';
const phone = '12025550111';
const from = '12025550122';
const env = {
  COACH_WA_INBOX_ENABLED: '1', COACH_WA_INBOX_SELLERS: seller,
  COACH_WA_INBOX_PHONE: phone,
  TWILIO_ACCOUNT_SID: 'AC' + '1'.repeat(32),
  TWILIO_AUTH_TOKEN: 'firma-ficticia-para-pruebas'
};
const config = waInboxConfig(env, key);
const actor = { owner: 'owner1', email: seller, zone: 'America/Chicago' };
const message = 'SM' + 'a'.repeat(32);
const media = 'ME' + 'b'.repeat(32);
const mediaUrl = 'https://api.twilio.com/2010-04-01/Accounts/' + config.accountSid + '/Messages/' + message + '/Media/' + media;
function msg(values = {}) {
  return { MessageSid: message, From: 'whatsapp:+' + from, To: 'whatsapp:+' + phone,
    AccountSid: config.accountSid, Body: '¿Qué sigue?', NumMedia: '0', ...values };
}

test('exige activación explícita, correos concretos y credenciales de Twilio', () => {
  assert.equal(config.ready, true);
  assert.equal(waInboxConfig({ ...env, COACH_WA_INBOX_ENABLED: '0' }, key).ready, false);
  assert.equal(waInboxConfig({ ...env, COACH_WA_INBOX_SELLERS: '*' }, key).ready, false);
  assert.equal(waInboxConfig({ ...env, TWILIO_AUTH_TOKEN: '' }, key).ready, false);
  assert.equal(waInboxConfig(env, Buffer.alloc(0)).ready, false);
});

test('acepta texto y audio de la línea configurada en la cuenta de Twilio', () => {
  const audio = msg({ Body: '', NumMedia: '1', MediaUrl0: mediaUrl, MediaContentType0: 'audio/ogg' });
  assert.equal(waInboxMessage(msg(), config, now).text, '¿Qué sigue?');
  assert.deepEqual(waInboxMessage(audio, config, now).audio, { message, id: media });
  assert.equal(waInboxMessage(msg({ To: 'whatsapp:+12025550133' }), config, now), null);
  assert.equal(waInboxMessage(msg({ AccountSid: 'AC' + '2'.repeat(32) }), config, now), null);
  assert.equal(waInboxMessage({ ...audio, MediaUrl0: 'https://example.com/audio' }, config, now), null);
});

test('el formulario aparece para el piloto y el estado vinculado permite revocar', () => {
  const pending = connectionsPage({ personalCoach: { linked: false }, csrf: 'test' });
  assert.match(pending, /Vincular WhatsApp/);
  assert.match(pending, /name="csrf" value="test"/);
  assert.doesNotMatch(connectionsPage({}), /Mi Coach en WhatsApp/);
  assert.match(connectionsPage({ personalCoach: { linked: true } }), /Desvincular WhatsApp/);
});

test('agenda y demo usan solo el vendedor vinculado y los recibos guardados', async () => {
  const scoped = [];
  const myDay = { async state(a) {
    scoped.push(a.owner);
    return { citas: { events: [{ title: 'Demo propia', time: '11:00' }], agenda: [], upcoming: [] } };
  } };
  const latestDemo = async owner => {
    scoped.push(owner);
    return { autoCloseReceipt: { survey: { state: 'saved' }, appointment: { state: 'pending' }, referrals: { saved: 2 } }, visitQuestions: 1 };
  };
  assert.match(await inboxReply('Jorge, ¿qué sigue?', { actor, myDay, latestDemo }), /11:00 · Demo propia/);
  const info = await inboxReply('¿Qué quedó de mi demo?', { actor, myDay, latestDemo });
  assert.match(info, /Encuesta guardada en el CRM.*4 en 14 en tu Hoja TEC: 2.*1 datos/);
  assert.doesNotMatch(info, /Cita de seguimiento/);
  assert.deepEqual(scoped, ['owner1', 'owner1']);
});

test('registra una sola llamada del día y no inventa registros', async () => {
  const binding = {};
  let updated = 0;
  const ref = 'l0123456789ab';
  const myDay = {
    async callBlock(a, { limit }) {
      assert.equal(a.owner, 'owner1');
      assert.equal(limit, 1);
      return { people: [{ name: 'María Prueba', ref, reason: 'Seguimiento pendiente.' }] };
    },
    async callResult(a, values) {
      assert.equal(a.owner, 'owner1');
      assert.deepEqual(values, { ref, result: 'no_contesto' });
      updated++;
      return { recorded: true };
    }
  };
  const context = { actor, binding, myDay, now, saveCall: async (nextCall, callDay) => {
    Object.assign(binding, { nextCall, callDay });
    return { matchedCount: 1 };
  } };
  assert.match(await callReply('No contestó', context), /Primero dime/);
  assert.match(await callReply('¿A quién le marco?', context), /María Prueba/);
  assert.match(await callReply('Registra que no contestó.', context), /conteo de Mi día: No contestó.*Hoja TEC y el CRM no cambiaron/);
  assert.equal(updated, 1);
});

test('el recibo de la demo dice dónde quedó cada dato y qué no se guardó', async () => {
  const latestDemo = async () => ({ autoCloseReceipt: {
    survey: { state: 'saved' }, appointment: { state: 'busy' },
    referrals: { state: 'checked', saved: 3, duplicates: 1, pending: 2 },
    referralAppointments: [{ name: 'A', state: 'saved' }, { name: 'B', state: 'time' }]
  }, visitQuestions: 0 });
  const info = await inboxReply('Qué quedó de mi demo', { actor, latestDemo });
  assert.match(info, /Hoja TEC: 3\./);
  assert.match(info, /Ya estaban en tu hoja: 1\./);
  assert.match(info, /sin guardar, por revisar: 2\./);
  assert.match(info, /no se guardó: ese horario ya estaba ocupado/);
  assert.match(info, /Citas de referidos en tu calendario: 1\. Citas de referidos sin guardar: 1\./);
  assert.doesNotMatch(info, /Faltan/);
});

test('volver a llamar y sacó cita solo cuentan en Mi día y lo dicen; la siguiente llamada salta las ya registradas', async () => {
  const binding = {};
  const ref = 'l0123456789ab';
  const asked = [];
  const myDay = {
    async callBlock(_a, options) { asked.push(options); return { people: [{ name: 'Ana', ref, reason: 'Pendiente.' }] }; },
    async callResult() { return { recorded: true }; }
  };
  const context = { actor, binding, myDay, now, saveCall: async (nextCall, callDay) => {
    Object.assign(binding, { nextCall, callDay });
    return { matchedCount: 1 };
  } };
  await callReply('A quién le marco', context);
  assert.equal(asked[0].skipDone, true);
  const later = await callReply('Volver a llamar el martes a las 5', context);
  assert.match(later, /Volver a llamar.*No quedó un recordatorio con fecha/);
  assert.match(later, /no quedó guardado/);
  assert.match(await callReply('Sacó cita', context), /La cita no quedó agendada/);
});

test('confirmar o mover una cita por WhatsApp no finge haberlo hecho', async () => {
  for (const text of ['Confirmó el sábado a las diez', 'Ya me canceló la de mañana', 'Mueve la cita de Ana al lunes']) {
    assert.match(await inboxReply(text, { actor }), /no cambié nada/);
  }
  assert.match(await inboxReply('Hola', { actor }), /Puedes decir/);
});

test('webhook firmado guarda una versión cifrada, deduplica y no procesa audio de desconocidos', async t => {
  const rows = new Map();
  const jobs = { async updateOne({ _id }, { $setOnInsert }) {
    if (!rows.has(_id)) rows.set(_id, $setOnInsert);
    return { upsertedCount: 0 };
  } };
  const checks = [];
  const bindings = { async exists(query) { checks.push(query); return null; } };
  const inbox = createCoachWaInbox({
    config, jobs, bindings, transport: { async lineStatus() { return 'online'; } }, now: () => now,
    validate: req => req.get('x-twilio-signature') === createHmac('sha1', config.authToken)
      .update(Object.entries(req.body).sort().flat().join('')).digest('base64')
  });
  inbox.start();
  t.after(inbox.stop);
  await Promise.resolve();
  const app = express();
  inbox.registerWebhook(app);
  const server = await new Promise(resolve => { const s = app.listen(0, '127.0.0.1', () => resolve(s)); });
  t.after(() => server.close());
  const url = 'http://127.0.0.1:' + server.address().port + COACH_WA_INBOX_WEBHOOK;
  const fields = msg({ Body: '', NumMedia: '1', MediaUrl0: mediaUrl, MediaContentType0: 'audio/ogg' });
  const body = new URLSearchParams(fields);
  const request = signature => fetch(url, { method: 'POST', body,
    headers: { 'X-Twilio-Signature': signature } });
  assert.equal((await request('firma-inválida')).status, 403);
  assert.equal(rows.size, 0);
  const signature = createHmac('sha1', config.authToken)
    .update(Object.entries(fields).sort().flat().join('')).digest('base64');
  assert.equal((await request(signature)).status, 200);
  assert.equal((await request(signature)).status, 200);
  assert.equal(rows.size, 1);
  assert.equal(checks.length, 2);
  const saved = JSON.stringify([...rows.values()]);
  assert.doesNotMatch(saved, new RegExp(from + '|' + media));
});

test('Twilio confirma remitente ONLINE y envía notas de voz con MediaUrl', async () => {
  const requests = [];
  const transport = coachWaInboxTransport(config, { fetcher: async (url, init) => {
    requests.push({ url: String(url), init });
    if (String(url).includes('/Channels/Senders')) return Response.json({ senders: [{ sender_id: 'whatsapp:+' + phone, status: 'ONLINE', webhook: { callback_url: 'https://totalelitecoaching.com/webhooks/twilio/whatsapp' } }] });
    if (String(url).includes('/Media/')) return new Response(null, { status: 307, headers: { location: 'https://example.com/audio' } });
    return Response.json({ sid: message });
  } });
  assert.equal(await transport.lineStatus(), 'online');
  await assert.rejects(transport.audio({ message, id: media }));
  assert.equal(requests.length, 2);
  const attachment = 'https://totalelitecoaching.com/sample.ogg';
  await transport.send(from, 'Respuesta de prueba', attachment);
  assert.equal(requests[2].init.body.get('MediaUrl'), attachment);
  assert.equal(requests[2].init.body.get('Body'), null);
  assert.equal(requests[2].init.body.get('From'), 'whatsapp:+' + phone);
});

test('vincular mantiene una verificación reciente frente a fallos transitorios y rechaza una baja confirmada', async t => {
  let at = now;
  let status = 'online';
  let lookups = 0;
  const bindings = {
    async updateOne() { return { upsertedCount: 1 }; },
    async exists() { return false; }
  };
  const inbox = createCoachWaInbox({
    config, bindings, jobs: {}, now: () => at,
    transport: { async lineStatus() {
      lookups++;
      if (status === 'transient') throw Object.assign(new Error('Unavailable'), { status: 503 });
      return status;
    } }
  });
  const app = express();
  app.use(express.urlencoded({ extended: false }));
  inbox.registerConnections(app, fn => (req, res) => fn(req, res, { id: actor.owner, email: seller, direct: true }),
    (res, state) => res.status(409).send(state));
  const server = await new Promise(resolve => { const s = app.listen(0, '127.0.0.1', () => resolve(s)); });
  t.after(() => server.close());
  const url = 'http://127.0.0.1:' + server.address().port + '/api/coach/conexiones/mi-coach-whatsapp/link';
  const link = (number = from) => fetch(url, { method: 'POST', redirect: 'manual', body: new URLSearchParams({ phone: '+' + number }) });

  assert.equal(await (await link(phone)).text(), 'waInboxPersonalPhone');
  const first = await link();
  assert.equal(first.status, 303);
  assert.match(first.headers.get('location'), /^https:\/\/wa\.me\/\d+\?text=VINCULAR%20[a-f0-9]{32}$/);
  assert.equal(lookups, 1);

  status = 'transient';
  at += 60_000;
  assert.equal((await link()).status, 303);
  assert.equal(lookups, 1);

  at += 3 * 60_000;
  assert.equal((await link()).status, 303);
  assert.equal(lookups, 2);
  assert.equal((await inbox.state(actor.owner, seller)).online, true);

  status = 'offline';
  assert.equal(await (await link()).text(), 'waInboxUnavailable');
  assert.equal((await inbox.state(actor.owner, seller)).online, false);

  status = 'transient';
  assert.equal(await (await link()).text(), 'waInboxUnavailable');

  status = 'online';
  assert.equal((await link()).status, 303);
  at += 11 * 60_000;
  status = 'transient';
  assert.equal(await (await link()).text(), 'waInboxUnavailable');
});

test('la API distingue un fallo temporal de un rechazo de autenticación', async () => {
  let status = 429, accountStatus = 401;
  const transport = coachWaInboxTransport(config, { fetcher: async url =>
    Response.json({}, { status: String(url).includes('/Channels/Senders') ? status : accountStatus })
  });
  await assert.rejects(transport.lineStatus(), { status: 429 });
  status = 403;
  assert.equal(await transport.lineStatus(), 'unauthorized');
  accountStatus = 200;
  assert.equal(await transport.lineStatus(), 'signed-webhook-required');
});

test('con la línea rechazada, un mensaje firmado se queda en la cola y nunca pasa al bot Chef', async t => {
  const rows = new Map();
  const jobs = { async updateOne({ _id }, { $setOnInsert }) {
    if (!rows.has(_id)) rows.set(_id, $setOnInsert);
    return { upsertedCount: 1 };
  } };
  const inbox = createCoachWaInbox({
    config, jobs, bindings: { async exists() { return true; } }, now: () => now,
    transport: { async lineStatus() { return 'unauthorized'; } }, validate: () => true
  });
  inbox.start();
  t.after(inbox.stop);
  await new Promise(resolve => setTimeout(resolve, 10));
  let chef = 0;
  const app = express();
  inbox.registerWebhook(app);
  app.post(COACH_WA_INBOX_WEBHOOK, (_req, res) => { chef++; res.send('chef'); });
  const server = await new Promise(resolve => { const s = app.listen(0, '127.0.0.1', () => resolve(s)); });
  t.after(() => server.close());
  const response = await fetch('http://127.0.0.1:' + server.address().port + COACH_WA_INBOX_WEBHOOK,
    { method: 'POST', body: new URLSearchParams(msg()) });
  assert.equal(response.status, 200);
  assert.match(await response.text(), /<Response><\/Response>/);
  assert.equal(chef, 0);
  assert.equal(rows.size, 1);
  assert.equal(await inbox.state(actor.owner, seller).then(s => s?.online), false);
});

test('si procesar falla, el vendedor vinculado recibe un aviso una sola vez', async t => {
  const { sealToken } = await import('../src/coach-hycite-gmail.js');
  const event = { id: message, from, at: now, text: '¿Qué sigue?', audio: null };
  let job = { _id: 'job1', encrypted: sealToken(JSON.stringify(event), key, 'job1'), at: new Date(now), phase: 'queued' };
  const jobs = {
    findOneAndUpdate() {
      if (job.phase !== 'queued') return { lean: async () => null };
      job = { ...job, phase: 'working', lease: new Date(now + 300000) };
      const claimed = { ...job };
      return { lean: async () => claimed };
    },
    async updateOne(query, update) {
      if (query.phase !== job.phase) return { modifiedCount: 0 };
      job = { ...job, ...update.$set };
      return { modifiedCount: 1 };
    }
  };
  const sent = [];
  const bindings = {
    async exists() { return true; },
    findOne() { return { lean: async () => ({ _id: 'owner1', contact: 'c', zone: 'America/Chicago' }) }; }
  };
  const inbox = createCoachWaInbox({
    config, jobs, bindings, now: () => now, validate: () => true,
    getUser: async () => ({ _id: 'owner1', email: seller }), eligible: async () => true,
    myDay: () => ({ async state() { throw new Error('Mongo caído'); } }), latestDemo: async () => null,
    transport: { async lineStatus() { return 'online'; }, async send(to, text) { sent.push({ to, text }); } }
  });
  inbox.start();
  t.after(inbox.stop);
  await new Promise(resolve => setTimeout(resolve, 2300));
  assert.equal(job.phase, 'failed');
  assert.equal(sent.length, 1);
  assert.equal(sent[0].to, from);
  assert.match(sent[0].text, /No pude terminar/);
});

test('el remitente no cuenta como en línea si el webhook no lleva el token que exige el servidor', async () => {
  const guarded = waInboxConfig({ ...env, TWILIO_WHATSAPP_WEBHOOK_TOKEN: 'token-de-prueba' }, key);
  let callback = 'https://totalelitecoaching.com/webhooks/twilio/whatsapp';
  const transport = coachWaInboxTransport(guarded, { fetcher: async () => Response.json({
    senders: [{ sender_id: 'whatsapp:+' + phone, status: 'ONLINE', webhook: { callback_url: callback } }] }) });
  assert.equal(await transport.lineStatus(), 'webhook-pending');
  callback += '?token=token-de-prueba';
  assert.equal(await transport.lineStatus(), 'online');
});

test('«A quién le marco» da el teléfono listo para marcar, o ninguno si no es claro', async () => {
  let phoneOf = '(312) 555-0123';
  const myDay = { async callBlock() { return { people: [{ name: 'Ana', ref: 'l0123456789ab', phone: phoneOf, reason: 'Pendiente.' }] }; } };
  const context = { actor, binding: {}, myDay, now, saveCall: async () => ({ matchedCount: 1 }) };
  assert.match(await callReply('A quién le marco', context), /^Ana, \+13125550123\. Pendiente\./);
  phoneOf = '555';
  assert.match(await callReply('A quién le marco', context), /^Ana\. Pendiente\./);
});
