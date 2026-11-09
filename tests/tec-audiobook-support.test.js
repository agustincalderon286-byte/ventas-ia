import assert from 'node:assert/strict';
import test from 'node:test';
import express from 'express';
import fs from 'node:fs/promises';
import vm from 'node:vm';
import { createSupportMailer, registerAudiobookSupport, SUPPORT_EMAIL } from '../src/tec-audiobook-support.js';

const origin = 'https://totalelitecoaching.com';
const valid = { email: 'reader@example.com', name: 'Lector de prueba', topic: 'audio', message: 'El audio se detiene al iniciar la lectura.', service: 'audiolibros' };
async function fixture(t, options = {}) {
  const sent = [];
  const app = express(); app.use(express.json({ limit: '12kb' }));
  registerAudiobookSupport(app, { publicOrigin: origin, sendSupport: async row => sent.push(row), ...options });
  const server = app.listen(0, '127.0.0.1');
  await new Promise(resolve => server.once('listening', resolve));
  t.after(() => new Promise(resolve => server.close(resolve)));
  const post = (body, headers = {}) => fetch(`http://127.0.0.1:${server.address().port}/api/audiolibros/contacto`, {
    method: 'POST', headers: { 'Content-Type': 'application/json', Origin: origin, ...headers }, body: JSON.stringify(body)
  });
  return { post, sent };
}

test('public support delivers a plain-text request without a session and returns a reference', async t => {
  const { post, sent } = await fixture(t);
  const res = await post({ ...valid, to: 'attacker@example.com', name: '  Ana  ', message: '  <script>plain text only</script>  ' });
  assert.equal(res.status, 200); assert.equal(res.headers.get('cache-control'), 'private, no-store');
  assert.match((await res.json()).reference, /^[a-f0-9]{8}$/);
  assert.equal(sent.length, 1); assert.equal(sent[0].name, 'Ana'); assert.equal(sent[0].message, '<script>plain text only</script>');
  assert.equal(sent[0].to, undefined); assert.equal(sent[0].email, valid.email);
});

test('support blocks cross-site requests and non-JSON submissions without sending', async t => {
  const { post, sent } = await fixture(t);
  assert.equal((await post(valid, { Origin: 'https://other.example' })).status, 403);
  assert.equal((await post(valid, { 'Sec-Fetch-Site': 'cross-site' })).status, 403);
  assert.equal((await post(valid, { 'Content-Type': 'text/plain' })).status, 415);
  assert.equal(sent.length, 0);
});

test('invalid email/header injection, subject injection and oversized or empty messages are rejected', async t => {
  for (const change of [{ email: 'a@example.com\r\nBcc:a@b.com' }, { email: 'no-email' }, { name: 'Ana\nBcc' }, { topic: '__proto__' }, { message: 'short' }, { message: 'x'.repeat(4001) }, { service: 'fake' }, { message: ['bad'] }]) {
    const { post, sent } = await fixture(t);
    assert.equal((await post({ ...valid, ...change })).status, 400); assert.equal(sent.length, 0);
  }
});

test('honeypot does not send mail; three emails per half hour and eight attempts per IP are enforced and expire', async t => {
  let now = 0;
  const { post, sent } = await fixture(t, { now: () => now });
  assert.equal((await post({ ...valid, website: 'spam' })).status, 200); assert.equal(sent.length, 0);
  for (let i = 0; i < 3; i++) assert.equal((await post(valid)).status, 200);
  const throttled = await post(valid); assert.equal(throttled.status, 429); assert.equal(throttled.headers.get('retry-after'), '1800');
  for (let i = 0; i < 3; i++) assert.equal((await post({ ...valid, email: `other${i}@example.com` })).status, 200);
  assert.equal((await post({ ...valid, email: 'final@example.com' })).status, 429);
  now = 1800001; assert.equal((await post(valid)).status, 200);
});

test('provider failures and missing configuration never claim successful delivery', async t => {
  for (const sendSupport of [null, async () => { throw new Error('secret provider info'); }]) {
    const { post } = await fixture(t, { sendSupport });
    const res = await post(valid); assert.equal(res.status, 503);
    const body = await res.text(); assert.match(body, /agustin@totalelitecoaching.com/); assert.doesNotMatch(body, /secret provider info/);
  }
});

test('Resend receives fixed owner recipient and validated Reply-To, with no HTML or credentials in content', async () => {
  let delivery;
  const send = createSupportMailer({ apiKey: 'test-secret', from: 'TEC <support@tec.example>', fetchImpl: async (url, opts) => { delivery = { url, opts }; return { ok: true }; } });
  await send({ ...valid, reference: 'abcd1234-reference', to: 'ignored@example.com' });
  const body = JSON.parse(delivery.opts.body);
  assert.deepEqual(body.to, [SUPPORT_EMAIL]); assert.equal(body.reply_to, valid.email);
  assert.equal(body.html, undefined); assert.match(body.text, /El audio se detiene/); assert.doesNotMatch(delivery.opts.body, /test-secret|ignored@example/);
  assert.equal(delivery.opts.headers['Idempotency-Key'], 'tec-support-abcd1234-reference');
  assert.equal(createSupportMailer({}), null);
  await assert.rejects(createSupportMailer({ apiKey: 'x', from: 'y', fetchImpl: async () => ({ ok: false }) })({ ...valid, reference: 'x' }));
});

test('form retains text on failed or offline send; success resets it and enables the button', async () => {
  const source = await fs.readFile(new URL('../public/audiolibros/contacto/soporte.js', import.meta.url), 'utf8');
  for (const state of ['offline', 'failure', 'success']) {
    let submit, reset = false, called = false;
    const button = { disabled: false }; const status = { dataset: {} };
    const form = { dataset: { service: 'audiolibros' }, addEventListener(_name, fn) { submit = fn; }, reportValidity: () => true,
      querySelector: selector => selector === '[type="submit"]' ? button : status, reset() { reset = true; } };
    vm.runInNewContext(source, { document: { querySelectorAll: () => [form] }, navigator: { onLine: state !== 'offline' },
      FormData: class { *[Symbol.iterator]() { yield ['email', valid.email]; } }, AbortSignal,
      fetch: async () => { called = true; return { ok: state === 'success', json: async () => state === 'success' ? { ok: true, reference: '12345678' } : { error: 'No enviado' } }; } });
    await submit({ preventDefault() {} });
    assert.equal(reset, state === 'success'); assert.equal(called, state !== 'offline'); assert.equal(button.disabled, false);
    assert.equal(status.dataset.error, String(state !== 'success'));
  }
});

test('public contact is reachable from login/membership and does not load analytics or store private drafts', async () => {
  const html = await fs.readFile(new URL('../public/audiolibros/contacto/index.html', import.meta.url), 'utf8');
  const main = await fs.readFile(new URL('../public/audiolibros/index.html', import.meta.url), 'utf8');
  assert.match(html, /mailto:agustin@totalelitecoaching.com/); assert.match(html, /No necesitas iniciar sesión/);
  assert.doesNotMatch(html, /analytics\.js|googletagmanager|gtag\(/);
  assert.match(main, /Pide ayuda a soporte/); assert.match(main, /Contactar a soporte/);
});
