import test from 'node:test';
import assert from 'node:assert/strict';
import crypto from 'node:crypto';
import fs from 'node:fs';
import express from 'express';
import mongoose from 'mongoose';
import {
  COACH_META_WEBHOOK_PATH, COACH_META_WINDOW_MS, coachMetaContactKey,
  createCoachMetaInboundModel, createCoachMetaWebhook, extractCoachMetaInbound
} from '../src/coach-meta-webhook.js';

const SECRET = 'test-app-secret';
const PHONE_ID = '1098765432';
const TOKEN = 'verify-token-123';
const CUSTOMER = '5215512345678';
const NOW = Date.UTC(2026, 10, 20, 18, 0, 0);
const baseEnv = () => ({
  COACH_WA_WEBHOOK_VERIFY_TOKEN: TOKEN, COACH_WA_META_APP_SECRET: SECRET, COACH_WA_CLOUD_PHONE_NUMBER_ID: PHONE_ID
});

function fakeStore({ fail } = {}) {
  const docs = new Map(); const calls = [];
  return {
    docs, calls,
    async updateOne(filter, update, options) {
      calls.push({ filter, update, options });
      if (fail) throw fail;
      const doc = docs.get(filter._id) || (options?.upsert ? { _id: filter._id } : null);
      if (!doc) return;
      for (const [field, value] of Object.entries(update.$max || {})) {
        if (!doc[field] || doc[field] < value) doc[field] = value;
      }
      docs.set(filter._id, doc);
    },
    findOne(filter) {
      const doc = docs.get(filter._id);
      const ok = doc && (!filter.expiresAt || doc.expiresAt > filter.expiresAt.$gt);
      return { lean: async () => (ok ? { ...doc } : null) };
    }
  };
}

async function start(t, { store = fakeStore(), env = baseEnv(), now = () => NOW, logger } = {}) {
  const logs = [];
  const hook = createCoachMetaWebhook({ store, env, now, logger: logger || { error: (...args) => logs.push(args) } });
  const app = express();
  app.get(COACH_META_WEBHOOK_PATH, hook.verify);
  app.post(COACH_META_WEBHOOK_PATH, hook.rawBody, hook.receive);
  app.use(express.json());
  const server = await new Promise(resolve => { const s = app.listen(0, '127.0.0.1', () => resolve(s)); });
  t.after(() => { server.closeAllConnections(); server.close(); });
  const url = `http://127.0.0.1:${server.address().port}${COACH_META_WEBHOOK_PATH}`;
  return { hook, store, logs, url };
}

const sign = (raw, secret = SECRET) => `sha256=${crypto.createHmac('sha256', secret).update(raw).digest('hex')}`;
function post(url, payload, { signature, raw } = {}) {
  const body = raw ?? Buffer.from(JSON.stringify(payload));
  return fetch(url, { method: 'POST', body, headers: {
    'content-type': 'application/json', ...(signature === null ? {} : { 'x-hub-signature-256': signature ?? sign(body) })
  } });
}

function message(overrides = {}) {
  return { from: CUSTOMER, id: 'wamid.ABC', timestamp: String(Math.floor(NOW / 1000) - 60), type: 'text',
    text: { body: 'Hola, quiero informes' }, ...overrides };
}
function event({ messages = [message()], field = 'messages', phoneId = PHONE_ID, extra = {} } = {}) {
  return { object: 'whatsapp_business_account', entry: [{ id: 'WABA', changes: [{ field, value: {
    messaging_product: 'whatsapp',
    metadata: { display_phone_number: '15550001111', phone_number_id: phoneId },
    contacts: [{ profile: { name: 'Cliente' }, wa_id: CUSTOMER }],
    messages, ...extra
  } }] }] };
}

test('GET verifica el token y devuelve hub.challenge como texto', async t => {
  const { url } = await start(t);
  const ok = await fetch(`${url}?hub.mode=subscribe&hub.verify_token=${TOKEN}&hub.challenge=1158201444`);
  assert.equal(ok.status, 200);
  assert.match(ok.headers.get('content-type'), /^text\/plain/);
  assert.equal(await ok.text(), '1158201444');
  assert.equal(ok.headers.get('cache-control'), 'no-store');

  for (const query of [
    `hub.mode=subscribe&hub.verify_token=wrong&hub.challenge=1`,
    `hub.mode=subscribe&hub.verify_token=${TOKEN}x&hub.challenge=1`,
    `hub.mode=unsubscribe&hub.verify_token=${TOKEN}&hub.challenge=1`,
    `hub.mode=subscribe&hub.verify_token=${TOKEN}&hub.verify_token=${TOKEN}&hub.challenge=1`,
    `hub.mode=subscribe&hub.challenge=1`
  ]) {
    const res = await fetch(`${url}?${query}`);
    assert.equal(res.status, 403, query);
    assert.doesNotMatch(await res.text(), /1|verify/);
  }
  const xss = await fetch(`${url}?hub.mode=subscribe&hub.verify_token=${TOKEN}&hub.challenge=%3Cscript%3E`);
  assert.equal(xss.status, 400);
});

test('GET sin token configurado no verifica', async t => {
  const { url } = await start(t, { env: { ...baseEnv(), COACH_WA_WEBHOOK_VERIFY_TOKEN: '' } });
  const res = await fetch(`${url}?hub.mode=subscribe&hub.verify_token=&hub.challenge=1`);
  assert.equal(res.status, 503);
});

test('POST rechaza firmas ausentes, inválidas o de otros bytes antes de parsear', async t => {
  const { url, store } = await start(t);
  const raw = Buffer.from(JSON.stringify(event()));
  const tampered = Buffer.from(JSON.stringify(event()).replace('Hola', 'Hole'));
  for (const signature of [null, '', 'sha256=zz', sign(raw, 'otro-secreto'), sign(raw).replace('sha256=', 'sha1='), sign(raw)]) {
    const res = await post(url, null, { raw: signature === sign(raw) ? tampered : raw, signature });
    assert.equal(res.status, 401);
  }
  // Valid signature over non-JSON bytes: verified first, then rejected as payload.
  const bad = Buffer.from('{not json');
  assert.equal((await post(url, null, { raw: bad })).status, 400);
  assert.equal(store.calls.length, 0);
});

test('POST sin secreto o línea configurada devuelve 503 sin escribir', async t => {
  for (const env of [{ ...baseEnv(), COACH_WA_META_APP_SECRET: '' }, { ...baseEnv(), COACH_WA_CLOUD_PHONE_NUMBER_ID: '' }]) {
    const { url, store } = await start(t, { env });
    assert.equal((await post(url, event())).status, 503);
    assert.equal(store.calls.length, 0);
  }
});

test('POST rechaza cuerpos demasiado grandes', async t => {
  const { url, store } = await start(t);
  const raw = Buffer.alloc(600 * 1024, 'a');
  assert.equal((await post(url, null, { raw })).status, 413);
  assert.equal(store.calls.length, 0);
});

test('un entrante real guarda solo id HMAC, fecha y caducidad de 24 h', async t => {
  const { url, store } = await start(t);
  const res = await post(url, event());
  assert.equal(res.status, 200);
  assert.equal(await res.text(), 'EVENT_RECEIVED');
  const key = coachMetaContactKey({ appSecret: SECRET, phoneId: PHONE_ID, waId: CUSTOMER });
  assert.match(key, /^[a-f0-9]{64}$/);
  const call = store.calls[0];
  assert.deepEqual(call.filter, { _id: key });
  assert.deepEqual(Object.keys(call.update), ['$max']);
  assert.deepEqual(call.update.$max, {
    lastInboundAt: new Date(NOW - 60_000), expiresAt: new Date(NOW - 60_000 + COACH_META_WINDOW_MS)
  });
  assert.equal(call.options.upsert, true);
  const stored = JSON.stringify([...store.docs.values()]);
  for (const pii of [CUSTOMER, PHONE_ID, 'Hola', 'Cliente', 'wamid', '15550001111']) {
    assert.ok(!stored.includes(pii), `persistió ${pii}`);
  }
});

test('ignora eventos no aplicables y responde 200 sin escribir', async t => {
  const { url, store } = await start(t);
  const sec = Math.floor(NOW / 1000);
  const cases = {
    otraLinea: event({ phoneId: '999' }),
    estados: event({ messages: [], extra: { statuses: [{ id: 'wamid.X', status: 'read', recipient_id: CUSTOMER, timestamp: String(sec) }] } }),
    eco: event({ field: 'smb_message_echoes', messages: [], extra: { message_echoes: [message({ to: CUSTOMER })] } }),
    historial: event({ field: 'history', messages: [], extra: { history: [{ threads: [{ id: CUSTOMER, messages: [message()] }] }] } }),
    estadoApp: event({ field: 'smb_app_state_sync', messages: [], extra: { state_sync: [] } }),
    reaccion: event({ messages: [message({ type: 'reaction', reaction: { emoji: '👍' } })] }),
    sistema: event({ messages: [message({ type: 'system' })] }),
    noSoportado: event({ messages: [message({ type: 'unsupported' })] }),
    grupo: event({ messages: [message({ group_id: 'G1' })] }),
    propioNumero: event({ messages: [message({ from: '15550001111' })] }),
    futuro: event({ messages: [message({ timestamp: String(sec + 3600) })] }),
    viejo: event({ messages: [message({ timestamp: String(sec - 25 * 3600) })] }),
    fechaInvalida: event({ messages: [message({ timestamp: 'ayer' })] }),
    sinRemitente: event({ messages: [message({ from: 'abc' })] }),
    otroObjeto: { ...event(), object: 'page' },
    otroProducto: (() => { const e = event(); e.entry[0].changes[0].value.messaging_product = 'sms'; return e; })(),
    vacio: {}, lista: [], nulo: null
  };
  for (const [name, payload] of Object.entries(cases)) {
    const res = await post(url, payload);
    assert.equal(res.status, 200, name);
    assert.equal(store.calls.length, 0, name);
  }
});

test('un timestamp ligeramente adelantado no abre la ventana más allá de ahora', async t => {
  const { url, store } = await start(t);
  await post(url, event({ messages: [message({ timestamp: String(Math.floor(NOW / 1000) + 120) })] }));
  assert.deepEqual(store.calls[0].update.$max.lastInboundAt, new Date(NOW));
});

test('reintentos y eventos fuera de orden son idempotentes con $max', async t => {
  const { url, store, hook } = await start(t);
  const sec = Math.floor(NOW / 1000);
  const newer = event({ messages: [message({ timestamp: String(sec - 10) }), message({ timestamp: String(sec - 500) })] });
  const older = event({ messages: [message({ timestamp: String(sec - 3000) })] });
  await post(url, newer); await post(url, newer); await post(url, older);
  assert.equal(store.docs.size, 1);
  assert.equal(store.calls[0].update.$max.lastInboundAt.getTime(), NOW - 10_000, 'un solo upsert por par en el lote');
  assert.deepEqual((await hook.lastInboundAt(CUSTOMER)), new Date(NOW - 10_000));
});

test('un fallo del almacén devuelve 500 y no registra cuerpo ni credenciales', async t => {
  const fail = Object.assign(new Error(`boom ${CUSTOMER} ${SECRET}`), { name: 'MongoServerError', code: 91 });
  const { url, logs } = await start(t, { store: fakeStore({ fail }) });
  assert.equal((await post(url, event())).status, 500);
  const logged = JSON.stringify(logs);
  for (const secret of [CUSTOMER, SECRET, TOKEN, 'Hola', 'boom']) assert.ok(!logged.includes(secret));
  assert.match(logged, /MongoServerError/);
});

test('una carrera de upsert duplicado se reintenta una vez', async t => {
  const store = fakeStore(); let first = true; const original = store.updateOne.bind(store);
  store.updateOne = async (...args) => {
    if (first) { first = false; throw Object.assign(new Error('dup'), { code: 11000 }); }
    return original(...args);
  };
  const { url } = await start(t, { store });
  assert.equal((await post(url, event())).status, 200);
  assert.equal(store.docs.size, 1);
});

test('la consulta inyectable normaliza el número y respeta la ventana de 24 h', async t => {
  let now = NOW; const store = fakeStore();
  const { url, hook } = await start(t, { store, now: () => now });
  assert.equal(await hook.lastInboundAt(CUSTOMER), null);
  await post(url, event());
  assert.deepEqual(await hook.lastInboundAt('+52 1 55 1234 5678'), new Date(NOW - 60_000));
  assert.equal(await hook.isWindowOpen(CUSTOMER), true);
  assert.equal(await hook.lastInboundAt('5215500000000'), null);
  assert.equal(await hook.lastInboundAt(''), null);
  now = NOW - 60_000 + COACH_META_WINDOW_MS + 1;
  assert.equal(await hook.isWindowOpen(CUSTOMER), false);
});

test('el id HMAC depende de la línea y del secreto', () => {
  const a = coachMetaContactKey({ appSecret: SECRET, phoneId: PHONE_ID, waId: CUSTOMER });
  assert.notEqual(a, coachMetaContactKey({ appSecret: SECRET, phoneId: '42', waId: CUSTOMER }));
  assert.notEqual(a, coachMetaContactKey({ appSecret: 'otro', phoneId: PHONE_ID, waId: CUSTOMER }));
  assert.equal(coachMetaContactKey({ appSecret: '', phoneId: PHONE_ID, waId: CUSTOMER }), '');
  assert.equal(extractCoachMetaInbound(event(), { appSecret: SECRET, phoneId: PHONE_ID, nowMs: NOW }).size, 1);
});

test('el modelo usa TTL sobre expiresAt y no guarda campos extra', () => {
  const model = createCoachMetaInboundModel(mongoose);
  assert.ok(model.schema.indexes().some(([fields, opts]) => fields.expiresAt === 1 && opts.expireAfterSeconds === 0));
  assert.deepEqual(Object.keys(model.schema.paths).sort(), ['_id', 'expiresAt', 'lastInboundAt']);
});

test('server.js instala GET y POST antes del express.json global', () => {
  const source = fs.readFileSync(new URL('../server.js', import.meta.url), 'utf8');
  const getAt = source.indexOf('app.get(COACH_META_WEBHOOK_PATH, coachMetaWebhook.verify)');
  const postAt = source.indexOf('app.post(COACH_META_WEBHOOK_PATH, coachMetaWebhook.rawBody, coachMetaWebhook.receive)');
  const jsonAt = source.indexOf('app.use(express.json(');
  assert.ok(getAt > 0 && postAt > 0 && jsonAt > 0);
  assert.ok(getAt < jsonAt && postAt < jsonAt);
  assert.equal(COACH_META_WEBHOOK_PATH, '/webhooks/coach/meta-whatsapp');
});
