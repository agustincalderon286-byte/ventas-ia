import assert from 'node:assert/strict';
import crypto from 'node:crypto';
import { Readable } from 'node:stream';
import test from 'node:test';
import express from 'express';
import { audiobookCatalog } from '../src/tec-audiobooks.js';
import { hasAudioMembership, isAudioMembershipPrice, registerAudioMembership } from '../src/tec-audio-membership.js';

const audience = 'tec-audiolibros';
const priceId = 'price_audio';
const price = { active: true, currency: 'usd', unit_amount: 2000,
  recurring: { interval: 'month', interval_count: 1, usage_type: 'licensed' } };
const subscription = { status: 'active', metadata: { audience }, items: { data: [{ price: { id: priceId } }] } };

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

test('guest preview, one-use sign in, member-only media and dedicated Checkout work through HTTP', async t => {
  const [book] = audiobookCatalog;
  const rows = new Map();
  const members = new Map();
  const grants = new Map();
  const lean = value => ({ async lean() { return value; } });
  let link;
  let eligible = false;
  let checkoutData;
  const stripe = {
    subscriptions: { async *list() { if (eligible) yield subscription; } },
    prices: { async retrieve() { return price; } },
    billingPortal: { configurations: { async retrieve() { return { active: true, features: { subscription_cancel: { enabled: true } } }; } } },
    customers: { async create() { return { id: 'cus_audio' }; } },
    checkout: { sessions: {
      async list() { return { data: [] }; },
      async create(data) { checkoutData = data; return { url: 'https://checkout.stripe.com/test' }; }
    } }
  };
  const app = express();
  app.use(express.json());
  registerAudioMembership(app, {
    stripe, priceId, portalId: 'bpc_audio', publicOrigin: 'https://totalelitecoaching.com',
    sendSignIn: async (_email, url) => { link = url; },
    accounts: {
      findById(id) { return lean(members.get(id)); },
      async updateOne({ _id }, { $set, $setOnInsert }) { members.set(_id, { ...members.get(_id), ...$setOnInsert, ...$set }); }
    },
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
  const request = (url, options) => fetch(base + url, options);
  const post = (url, body, cookie, origin = 'https://totalelitecoaching.com') => request(url, {
    method: 'POST', headers: { Origin: origin, 'Content-Type': 'application/json', ...(cookie && { Cookie: cookie }) }, body: JSON.stringify(body)
  });
  const path = '/libros/' + book.id;

  const catalog = await request('/catalogo').then(res => res.json());
  for (const [index, publicBook] of catalog.books.entries()) {
    assert.ok(publicBook.previewUrl);
    assert.ok(Math.abs(publicBook.previewDuration - audiobookCatalog[index].duration * .1) < .001);
    for (const field of ['audio', 'audioUrl', 'transcriptUrl', 'chapters']) assert.equal(field in publicBook, false);
  }
  for (const suffix of ['', '/audio', '/texto']) assert.equal((await request(path + suffix)).status, 401);

  assert.equal((await post('/entrar', { email: 'listener@example.test' }, null, 'https://other.test')).status, 403);
  assert.equal((await post('/entrar', { email: 'Listener@example.test' })).status, 200);
  const token = new URLSearchParams(new URL(link).hash.slice(1)).get('entrar');
  assert.equal([...grants.values()][0].digest, crypto.createHash('sha256').update(token).digest('hex'));
  const confirmed = await post('/confirmar', { token });
  assert.equal(confirmed.status, 200);
  assert.equal((await post('/confirmar', { token })).status, 400);
  const cookie = confirmed.headers.get('set-cookie').split(';')[0];
  const headers = { Cookie: cookie, Range: 'bytes=4-7' };
  assert.equal((await request(path + '/audio', { headers })).status, 403);

  assert.equal((await post('/checkout', {}, cookie)).status, 200);
  assert.equal(checkoutData.mode, 'subscription');
  assert.equal(checkoutData.line_items[0].price, priceId);
  assert.equal(checkoutData.customer, 'cus_audio');
  assert.equal(checkoutData.subscription_data.metadata.audience, audience);
  assert.match(checkoutData.success_url, /^https:\/\/totalelitecoaching\.com\/audiolibros\//);
  assert.equal((await request(path + '/audio', { headers })).status, 403);

  eligible = true;
  assert.equal((await post('/checkout', {}, cookie)).status, 409);
  const detail = await request(path, { headers }).then(res => res.json());
  assert.deepEqual(detail.progress, { position: 12, speed: 1.5, marks: [7] });
  const audio = await request(path + '/audio', { headers });
  assert.equal(audio.status, 206);
  assert.equal(audio.headers.get('cache-control'), 'private, no-store');
  assert.equal(audio.headers.get('content-range'), 'bytes 4-7/' + book.audio.bytes);
  assert.equal(await audio.text(), 'AAAA');
  const text = await request(path + '/texto', { headers }).then(res => res.json());
  assert.ok(text.cues.length > 100);
  assert.equal((await post('/salir', {}, cookie)).status, 200);
  assert.equal((await request(path + '/audio', { headers })).status, 401);
});
