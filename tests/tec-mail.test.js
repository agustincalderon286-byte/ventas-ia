import test from 'node:test';
import assert from 'node:assert/strict';
import { createTecMailer, createAudioAccountMailer, TEC_CONTACT_EMAIL, TEC_SENDER_NAME } from '../src/tec-mail.js';

const address = 'lector@example.com';
const message = { to: [address], subject: 'Tu enlace para entrar a Audiolibros TEC', text: 'Enlace de acceso de prueba.' };

test('Hostinger selects the exact TEC mailbox and sends from it, retaining its API Sent behavior', async () => {
  const calls = [];
  const send = createTecMailer({ hostingerToken: 'private-token', resendApiKey: 'unused', resendFrom: 'old@example.com',
    fetchImpl: async (url, options) => {
      calls.push({ url, options });
      return url.endsWith('/me')
        ? { ok: true, json: async () => ({ data: { mailboxes: [
          { address: 'info@other.example', resourceId: 'other' },
          { address: TEC_CONTACT_EMAIL, resourceId: 'TEC-resource' }
        ] } }) } : { ok: true };
    } });
  assert.deepEqual(await send(message), { accepted: true, provider: 'hostinger' });
  await send({ ...message, subject: 'Otra consulta', replyTo: address });
  assert.equal(calls.length, 3); // successful mailbox lookup is cached
  assert.equal(calls[1].url, 'https://api.mail.hostinger.com/api/v1/mailboxes/TEC-resource/send');
  const body = JSON.parse(calls[1].options.body);
  assert.deepEqual(body.to, [address]); assert.equal(body.displayName, TEC_SENDER_NAME);
  assert.equal(body.from, undefined); assert.equal(body.html, undefined);
  assert.doesNotMatch(calls[1].options.body, /private-token|unused|other/);
  assert.equal(calls[1].options.headers.Authorization, 'Bearer private-token');
  assert.match(JSON.parse(calls[2].options.body).text, /mailto:lector@example.com/);
});

test('Hostinger refuses a token without the exact TEC mailbox and can recover after configuration is fixed', async () => {
  let lookups = 0, sends = 0;
  const send = createTecMailer({ hostingerToken: 'secret', fetchImpl: async url => {
    if (!url.endsWith('/me')) { sends++; return { ok: true }; }
    lookups++;
    return { ok: true, json: async () => ({ data: { mailboxes: lookups === 1
      ? [{ address: 'info@other.example', resourceId: 'other' }]
      : [{ address: TEC_CONTACT_EMAIL, resourceId: 'TEC-resource' }] } }) };
  } });
  await assert.rejects(send(message), /configuration_failed/); assert.equal(sends, 0);
  await send(message); assert.equal(sends, 1);
});

test('a failed or uncertain Hostinger send is not retried or sent a second time through Resend', async () => {
  for (const failure of ['http', 'timeout']) {
    const urls = [];
    const send = createTecMailer({ hostingerToken: 'secret', resendApiKey: 'resend', resendFrom: 'old@example.com',
      fetchImpl: async url => {
        urls.push(url);
        if (url.endsWith('/me')) return { ok: true, json: async () => ({ data: { mailboxes: [{ address: TEC_CONTACT_EMAIL, resourceId: 'TEC' }] } }) };
        if (failure === 'timeout') throw new Error('request_timeout');
        return { ok: false };
      } });
    await assert.rejects(send(message));
    assert.equal(urls.length, 2); assert.ok(urls.every(url => url.startsWith('https://api.mail.hostinger.com/')));
  }
});

test('legacy Resend configuration remains usable before Hostinger token activation, with TEC reply address', async () => {
  let call;
  const send = createTecMailer({ resendApiKey: 'secret', resendFrom: 'Biblioteca <biblioteca@acceso.totalelitecoaching.com>',
    fetchImpl: async (url, options) => { call = { url, options }; return { ok: true }; } });
  await send({ ...message, idempotencyKey: 'request-1' });
  assert.equal(call.url, 'https://api.resend.com/emails');
  const body = JSON.parse(call.options.body);
  assert.equal(body.reply_to, TEC_CONTACT_EMAIL);
  assert.equal(body.from, 'Audiolibros TEC <biblioteca@acceso.totalelitecoaching.com>');
  assert.equal(call.options.headers['Idempotency-Key'], 'request-1');
  assert.equal(createTecMailer(), null);
});

test('personal mailboxes cannot be configured as the fallback sender for TEC accounts', () => {
  assert.equal(createTecMailer({ resendApiKey: 'secret', resendFrom: 'Agustin <personal@gmail.com>' }), null);
  assert.equal(createTecMailer({ resendApiKey: 'secret', resendFrom: 'fake@totalelitecoaching.com.evil.test' }), null);
});

test('account confirmation and password recovery use TEC branding and never email a password', async () => {
  const messages = [];
  const send = createAudioAccountMailer(async value => { messages.push(value); });
  await send({ email: address, purpose: 'activation', url: 'https://totalelitecoaching.com/audiolibros/#cuenta=opaque' });
  await send({ email: address, purpose: 'reset', url: 'https://totalelitecoaching.com/audiolibros/#cuenta=opaque2' });
  await send({ email: address, purpose: 'password_changed', password: 'NEVER-SEND-THIS' });
  assert.match(messages[0].subject, /Confirma tu correo/); assert.match(messages[0].text, /pago.*confirmado/);
  assert.match(messages[0].text, /24 horas/); assert.match(messages[1].text, /30 minutos/);
  assert.match(messages[2].text, /cerramos las sesiones anteriores/);
  assert.ok(messages.every(value => value.to[0] === address && value.text.includes(TEC_CONTACT_EMAIL)));
  assert.doesNotMatch(JSON.stringify(messages), /NEVER-SEND-THIS|gmail/);
  assert.equal(createAudioAccountMailer(null), null);
});

test('header injection is rejected before contacting any provider', async () => {
  let called = false;
  const send = createTecMailer({ hostingerToken: 'secret', fetchImpl: async () => { called = true; } });
  for (const change of [{ to: ['reader@example.com\r\nBcc:other@example.com'] },
    { subject: 'Access\r\nBcc:other@example.com' }, { replyTo: 'a@example.com\nother@example.com' }]) {
    await assert.rejects(send({ ...message, ...change }), /invalid_message/);
  }
  assert.equal(called, false);
});
