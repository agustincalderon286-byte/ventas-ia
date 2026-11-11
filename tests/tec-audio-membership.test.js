import assert from 'node:assert/strict';
import crypto from 'node:crypto';
import { Readable } from 'node:stream';
import test from 'node:test';
import express from 'express';
import { audioAccountStore } from './helpers/audio-account-store.js';
import { audiobookCatalog, getAudiobookTranscript } from '../src/tec-audiobooks.js';
import { hasAudioMembership, isAudioMembershipPrice, registerAudioMembership } from '../src/tec-audio-membership.js';

const audience = 'tec-audiolibros';
const priceId = 'price_audio';
const price = { active: true, currency: 'usd', unit_amount: 2000,
  recurring: { interval: 'month', interval_count: 1, usage_type: 'licensed' } };
const subscription = { id: 'sub_audio', customer: 'cus_audio', status: 'active', metadata: { audience }, items: { data: [{ price: { id: priceId } }] } };

test('audio entitlement accepts only the active independent plan at the advertised monthly price', () => {
  assert.ok(isAudioMembershipPrice(price));
  assert.equal(isAudioMembershipPrice({ ...price, unit_amount: 2001 }), false);
  assert.equal(isAudioMembershipPrice({ ...price, recurring: { ...price.recurring, interval: 'year' } }), false);
  assert.ok(hasAudioMembership(subscription, priceId));
  assert.equal(hasAudioMembership(subscription, 'price_coach'), false);
  for (const status of ['incomplete', 'canceled', 'past_due', 'unpaid', 'trialing', 'paused']) {
    assert.equal(hasAudioMembership({ ...subscription, status }, priceId), false);
  }
  assert.equal(hasAudioMembership({ ...subscription, metadata: { productMode: 'coach' } }, priceId), false);
  assert.equal(hasAudioMembership({ ...subscription, pause_collection: {} }, priceId), false);
});

test('guest previews, paid-only activation, password access and recovery work through HTTP', async t => {
  const [book] = audiobookCatalog;
  let links = 0;
  const lease = { url: 'https://example.test/private-audio', expiresAt: Date.now() + 300000 };
  const rows = new Map();
  const accountStore = audioAccountStore();
  const members = accountStore.rows;
  const intents = audioAccountStore();
  const grants = new Map();
  const shared = new Map();
  const lean = value => ({ async lean() { return value; } });
  let link;
  let eligible = false;
  let checkoutData;
  const measurementCalls = [];
  const completedCheckout = { id: 'cs_live_audio_checkout_test_123456789', mode: 'subscription', status: 'open',
    livemode: true, payment_status: 'unpaid', currency: 'usd', amount_total: 2000, customer: 'cus_audio',
    subscription: 'sub_audio', invoice: 'in_audio', customer_details: { email: 'listener@example.test' },
    metadata: { audience, account_flow: 'paid-account-v1' } };
  const stripe = {
    subscriptions: { async *list() { if (eligible) yield subscription; }, async retrieve() { return subscription; } },
    invoices: { async retrieve() { return { id: 'in_audio', customer: 'cus_audio', subscription: 'sub_audio', livemode: true,
      status: 'paid', paid_out_of_band: false, currency: 'usd', amount_paid: 2000 }; } },
    prices: { async retrieve() { return price; } },
    billingPortal: { configurations: { async retrieve() { return { active: true, features: { subscription_cancel: { enabled: true } } }; } } },
    customers: { async create() { return { id: 'cus_audio' }; } },
    checkout: { sessions: {
      async list() { return { data: [] }; },
      async retrieve() { return completedCheckout; },
      async create(data) { checkoutData = data; return { id: completedCheckout.id, url: 'https://checkout.stripe.com/test' }; }
    } }
  };
  const app = express();
  app.use(express.json());
  registerAudioMembership(app, {
    paymentAnalytics: { async saveContext(user, analytics) { measurementCalls.push({ user, analytics }); },
      async revoke(user) { measurementCalls.push({ revoked: user.stripeCustomerId }); } },
    shares: { async create(row) { shared.set(row._id, row); }, findById(id) { return lean(shared.get(id)); } },
    r2: { ready: async () => true, delivery: async () => { links++; return lease; }, open: async () => { throw new Error('must not proxy'); } },
    stripe, priceId, portalId: 'bpc_audio', publicOrigin: 'https://totalelitecoaching.com',
    sendAccountMail: async message => { if (message.url) link = message.url; },
    accounts: accountStore.model, checkoutIntents: intents.model,
    grants: {
      async findOneAndUpdate({ _id }, { $set }) { grants.set(_id, $set); },
      findOneAndDelete({ digest, expiresAt }) {
        for (const [id, row] of grants) {
          if (row.digest === digest && row.expiresAt > expiresAt.$gt) { grants.delete(id); return lean(row); }
        }
        return lean(null);
      }
    },
    sessions: {
      async create(row) { rows.set(row._id, row); },
      findOne({ _id }) { return lean(rows.get(_id)); },
      async deleteOne({ _id }) { rows.delete(_id); }
    },
    listening: { findById() { return lean({ position: 12, speed: 1.5, marks: [7] }); } },
    media: { findOne() { return lean({ fileId: 'verified' }); } },
    getBucket: () => ({
      find() { return { limit: () => ({ toArray: async () => [{ _id: 'verified', length: book.audio.bytes }] }) }; },
      openDownloadStream(_id, { start, end }) { return Readable.from(Buffer.alloc(end - start, 65)); }
    })
  });
  const server = await new Promise(resolve => { const instance = app.listen(0, '127.0.0.1', () => resolve(instance)); });
  t.after(() => new Promise(resolve => { server.closeAllConnections(); server.close(resolve); }));
  const base = 'http://127.0.0.1:' + server.address().port + '/api/audiolibros';
  const request = (url, options) => fetch(base + url, { redirect: 'manual', ...options });
  const post = (url, body, cookie, origin = 'https://totalelitecoaching.com') => request(url, {
    method: 'POST', headers: { Origin: origin, 'Content-Type': 'application/json', ...(cookie && { Cookie: cookie }) }, body: JSON.stringify(body)
  });
  const path = '/libros/' + book.id;

  const catalog = await request('/catalogo').then(res => res.json());
  for (const [index, publicBook] of catalog.books.entries()) {
    assert.ok(publicBook.previewUrl);
    assert.ok(Math.abs(publicBook.previewDuration - audiobookCatalog[index].duration * .1) < .001);
    for (const field of ['audio', 'audioUrl', 'transcriptUrl', 'chapters']) assert.equal(field in publicBook, false);
    const previewText = await fetch('http://127.0.0.1:' + server.address().port + publicBook.previewTranscriptUrl);
    assert.equal(previewText.status, 200);
    const excerpt = await previewText.json();
    const expected = getAudiobookTranscript(publicBook.id).cues.filter(cue =>
      cue.start < publicBook.previewDuration && cue.end <= publicBook.previewDuration);
    assert.ok(excerpt.cues.length);
    assert.deepEqual(excerpt.cues, expected);
    assert.ok(excerpt.cues.length < getAudiobookTranscript(publicBook.id).cues.length);
    assert.equal(excerpt.trailingSeparator, '');
  }
  assert.equal((await request('/libros/unknown/muestra/texto')).status, 404);
  for (const suffix of ['', '/audio', '/audio?delivery=1', '/texto']) assert.equal((await request(path + suffix)).status, 401);
  assert.equal(links, 0);
  for (const publicBook of catalog.books) {
    const sample = await fetch('http://127.0.0.1:' + server.address().port + publicBook.previewUrl + '?delivery=1').then(r => r.json());
    assert.deepEqual(sample, lease);
  }
  assert.equal((await post('/compartir', { bookId: book.id, kind: 'audio', start: 5, end: 20 })).status, 201);
  assert.equal((await post('/compartir', { bookId: book.id, kind: 'audio', start: 1000, end: 1020 })).status, 401);

  assert.equal((await post('/checkout', { email: 'listener@example.test' }, null, 'https://other.test')).status, 403);
  assert.equal((await post('/entrar', { email: 'listener@example.test' })).status, 401);
  assert.equal((await post('/confirmar', { token: 'old-link' })).status, 410);
  assert.equal(members.size, 0);
  const analytics = { consentVersion: 2, clientId: '123.1794310000', sessionId: '1794310000', campaign: {} };
  assert.equal((await post('/checkout', { email: 'Listener@example.test', analytics })).status, 200);
  assert.equal(members.size, 0, 'an unpaid checkout never creates a library account');
  assert.equal(measurementCalls.at(-1).user.stripeCustomerId, 'cus_audio');
  assert.deepEqual(measurementCalls.at(-1).analytics, analytics);
  assert.equal((await post('/alta/estado', { sessionId: completedCheckout.id })).status, 200);
  assert.equal(members.size, 0);
  eligible = true; completedCheckout.status = 'complete'; completedCheckout.payment_status = 'paid';
  assert.deepEqual(await post('/alta/estado', { sessionId: completedCheckout.id }).then(res => res.json()), { paid: true });
  assert.equal(members.size, 1);
  assert.equal((await request(path + '/audio')).status, 401);
  const token = new URLSearchParams(new URL(link).hash.slice(1)).get('cuenta');
  assert.equal(members.get('listener@example.test').authGrantDigest, crypto.createHash('sha256').update(token).digest('hex'));
  assert.equal((await post('/credenciales/estado', { token })).status, 200);
  const password = 'Una frase para leer libros 2026';
  const confirmed = await post('/credenciales', { token, username: 'lector_tec', password, passwordConfirmation: password });
  assert.equal(confirmed.status, 200);
  assert.equal((await post('/credenciales', { token, username: 'lector_tec', password, passwordConfirmation: password })).status, 400);
  let cookie = confirmed.headers.getSetCookie().find(value => value.startsWith('tec_audio_session=')).split(';')[0];
  const headers = { Cookie: cookie, Range: 'bytes=4-7' };
  assert.equal((await post('/estadisticas/contexto', { analytics }, cookie, 'https://other.test')).status, 403);
  const beforeGuest = measurementCalls.length;
  assert.equal((await post('/estadisticas/contexto', { analytics })).status, 200);
  assert.equal(measurementCalls.length, beforeGuest);
  assert.equal((await post('/estadisticas/contexto', { analytics }, cookie)).status, 200);
  assert.deepEqual(measurementCalls.at(-1).analytics, analytics);
  assert.equal((await request('/estadisticas/consentimiento', { method: 'DELETE',
    headers: { Origin: 'https://other.test', Cookie: cookie } })).status, 403);
  assert.equal((await request('/estadisticas/consentimiento', { method: 'DELETE',
    headers: { Origin: 'https://totalelitecoaching.com', Cookie: cookie } })).status, 200);
  assert.equal(measurementCalls.at(-1).revoked, 'cus_audio');
  assert.equal(checkoutData.mode, 'subscription');
  assert.equal(checkoutData.line_items[0].price, priceId);
  assert.equal(checkoutData.customer, 'cus_audio');
  assert.equal(checkoutData.subscription_data.metadata.audience, audience);
  assert.match(checkoutData.success_url, /^https:\/\/totalelitecoaching\.com\/audiolibros\//);
  assert.equal((await request(path + '/audio', { headers })).status, 307);

  eligible = true;
  const memberShare = await post('/compartir', { bookId: book.id, kind: 'audio', start: 1000, end: 1020 }, cookie);
  assert.equal(memberShare.status, 201);
  assert.equal(shared.get((await memberShare.json()).id).start, 1000);
  assert.equal((await post('/checkout', {}, cookie)).status, 409);
  const detail = await request(path, { headers }).then(res => res.json());
  assert.deepEqual(detail.progress, { position: 12, speed: 1.5, marks: [7] });
  const audio = await request(path + '/audio', { headers });
  assert.equal(audio.status, 307);
  assert.equal(audio.headers.get('cache-control'), 'private, no-store');
  assert.equal(audio.headers.get('location'), lease.url);
  assert.equal(await audio.text(), '');
  assert.deepEqual(await request(path + '/audio?delivery=1', { headers }).then(r => r.json()), lease);
  const text = await request(path + '/texto', { headers }).then(res => res.json());
  assert.ok(text.cues.length > 100);
  const unknownRecovery = await post('/recuperar', { email: 'unknown@example.test' }).then(res => res.json());
  const knownRecovery = await post('/recuperar', { email: 'listener@example.test' }).then(res => res.json());
  assert.deepEqual(knownRecovery, unknownRecovery);
  const resetToken = new URLSearchParams(new URL(link).hash.slice(1)).get('cuenta');
  const nextPassword = 'Una nueva frase para los libros 2026';
  assert.equal((await post('/credenciales', { token: resetToken, password: nextPassword, passwordConfirmation: nextPassword })).status, 200);
  assert.equal((await request(path + '/audio', { headers })).status, 401);
  assert.equal((await post('/entrar', { identifier: 'lector_tec', password })).status, 401);
  const loggedIn = await post('/entrar', { identifier: 'LECTOR_TEC', password: nextPassword });
  assert.equal(loggedIn.status, 200);
  cookie = loggedIn.headers.getSetCookie().find(value => value.startsWith('tec_audio_session=')).split(';')[0];
  headers.Cookie = cookie;
  assert.equal((await request(path + '/audio', { headers })).status, 307);
  assert.equal((await post('/salir', {}, cookie)).status, 200);
  assert.equal((await request(path + '/audio', { headers })).status, 401);
});
