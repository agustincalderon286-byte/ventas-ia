import test from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import vm from 'node:vm';
import express from 'express';
import Stripe from 'stripe';
import { createCoachBillingSync } from '../src/coach-billing-sync.js';

// Local integration checks: real production handlers and Stripe signature
// verification, but simulated Stripe API responses and an in-memory database.
// No real Stripe keys, payments, production users, or network calls to Stripe.
const source = readFileSync(new URL('../server.js', import.meta.url), 'utf8');
function section(start, end) {
  const a = source.indexOf(start), b = source.indexOf(end, a);
  assert.ok(a >= 0 && b > a, `Missing source section: ${start}`);
  return source.slice(a, b);
}
const secret = 'whsec_local_tec_activation_fixture_only';
const sdk = new Stripe('sk_test_local_fixture_not_a_real_key');
const periodEnd = 1795564800;

async function harness(run) {
  const rows = new Map(), sessions = new Map(), currentSubscriptions = new Map();
  let failSave = false, listHook;
  const hydrate = row => row && {
    ...structuredClone(row),
    async save() {
      if (failSave) throw new Error('simulated persistence failure');
      const { save, ...data } = this;
      rows.set(String(this._id), structuredClone(data));
    }
  };
  const model = {
    findById: async id => hydrate(rows.get(String(id))),
    findOne: async query => hydrate([...rows.values()].find(row =>
      Object.entries(query).every(([key, value]) => row[key] === value))),
    findOneAndUpdate: async (query, change) => {
      if (failSave) throw new Error('simulated persistence failure');
      const row = [...rows.values()].find(row => Object.entries(query).every(([key, value]) => row[key] === value));
      if (!row) return null;
      for (const [key, value] of Object.entries(change.$inc || {})) row[key] = (row[key] || 0) + value;
      Object.assign(row, structuredClone(change.$set || {}));
      return hydrate(row);
    }
  };
  const stripe = {
    webhooks: sdk.webhooks,
    subscriptions: { list: async ({ customer }) => {
      const snapshot = { data: structuredClone([...currentSubscriptions.values()]
        .filter(sub => sub.customer === customer)), has_more: false };
      if (listHook) await listHook();
      return snapshot;
    } },
    checkout: { sessions: { retrieve: async id => {
      if (!sessions.has(id)) throw new Error('Unknown fixture session');
      return structuredClone(sessions.get(id));
    } } }
  };
  const context = vm.createContext({
    stripe, CoachUser: model, Date, Boolean, String, createCoachBillingSync,
    process: { env: { STRIPE_WEBHOOK_SECRET: secret, STRIPE_SECRET_KEY: 'sk_test_local_fixture' } },
    STRIPE_PRICE_ID_MONTHLY: 'price_tec_monthly_fixture',
    STRIPE_PRICE_ID_ANNUAL: 'price_tec_annual_fixture',
    normalizarEmail: value => String(value || '').trim().toLowerCase(),
    convertirUnixADate: value => value ? new Date(value * 1000) : null,
    console: { error() {} }
  });
  vm.runInContext(section('function coachTieneAcceso(status', 'function coachTieneAccesoDePrueba'), context);
  vm.runInContext(section('function stripeConfigurado()', '// =============================\n// FUNCION JSON SEGURA'), context);
  const app = express();
  app.post('/webhooks/stripe', express.raw({ type: 'application/json' }), context.manejarWebhookStripe);
  const server = await new Promise(resolve => {
    const listener = app.listen(0, '127.0.0.1', () => resolve(listener));
  });
  const url = `http://127.0.0.1:${server.address().port}/webhooks/stripe`;
  function addUser(id = 'qa-monthly') {
    const user = { _id: id, name: 'Fictional billing QA', email: `${id}@example.invalid`,
      accountType: 'owner', stripeCustomerId: `cus_${id}`, subscriptionActive: false,
      subscriptionStatus: 'inactive' };
    rows.set(id, user);
    return user;
  }
  function subscription(user, { plan = 'monthly', status = 'active', modern = false, ...extra } = {}) {
    return { id: `sub_${user._id}`, customer: user.stripeCustomerId, status,
      ...(modern ? {} : { current_period_end: periodEnd }), cancel_at_period_end: false,
      items: { data: [{ id: `si_${user._id}`, current_period_end: periodEnd,
        price: { id: `price_tec_${plan}_fixture` } }] }, ...extra };
  }
  function session(user, sub) {
    currentSubscriptions.set(sub.id, structuredClone(sub));
    const data = { id: `cs_test_${user._id}`, mode: 'subscription', livemode: false, status: 'complete', payment_status: 'paid',
      customer: user.stripeCustomerId, client_reference_id: user._id,
      metadata: { coachUserId: user._id }, subscription: sub };
    sessions.set(data.id, data);
    return data;
  }
  async function send(type, object, { invalidSignature = false, stale = false, ...extra } = {}) {
    if (type.startsWith('customer.subscription.') && !stale) currentSubscriptions.set(object.id, structuredClone(object));
    const payload = JSON.stringify({ id: 'evt_fixture', type, livemode: false,
      created: Math.floor(Date.now() / 1000), data: { object }, ...extra });
    const signature = sdk.webhooks.generateTestHeaderString({ payload,
      secret: invalidSignature ? 'whsec_wrong_fixture' : secret });
    const response = await fetch(url, { method: 'POST', headers: {
      'Content-Type': 'application/json', 'Stripe-Signature': signature }, body: payload });
    return { status: response.status, body: await response.text() };
  }
  try { await run({ rows, sessions, currentSubscriptions, addUser, subscription, session, send,
    sync: createCoachBillingSync({ stripe, users: model, priceIds: ['price_tec_monthly_fixture', 'price_tec_annual_fixture'] }),
    listHook: value => { listHook = value; }, failSave: value => { failSave = value; } }); }
  finally { await new Promise(resolve => server.close(resolve)); }
}

for (const plan of ['monthly', 'annual']) {
  test(`webhook activates ${plan} membership without a browser redirect`, () => harness(async h => {
    const user = h.addUser(`qa-${plan}`), sub = h.subscription(user, { plan });
    assert.equal(h.rows.get(user._id).subscriptionActive, false);
    assert.equal((await h.send('checkout.session.completed', h.session(user, sub))).status, 200);
    const saved = h.rows.get(user._id);
    assert.equal(saved.subscriptionActive, true);
    assert.equal(saved.subscriptionStatus, 'active');
    assert.equal(saved.stripePriceId, `price_tec_${plan}_fixture`);
    assert.equal(saved.stripeSubscriptionId, sub.id);
  }));
}

test('incomplete payment does not activate membership', () => harness(async h => {
  const user = h.addUser();
  const session = h.session(user, h.subscription(user, { status: 'incomplete' }));
  session.payment_status = 'unpaid'; h.sessions.set(session.id, session);
  assert.equal((await h.send('checkout.session.completed', session)).status, 200);
  assert.equal(h.rows.get(user._id).subscriptionActive, false);
}));

test('subscription creation alone activates a paid subscription', () => harness(async h => {
  const user = h.addUser();
  assert.equal((await h.send('customer.subscription.created', h.subscription(user))).status, 200);
  assert.equal(h.rows.get(user._id).subscriptionActive, true);
}));

test('scheduled cancellation preserves access; final cancellation removes it', () => harness(async h => {
  const user = h.addUser();
  await h.send('customer.subscription.updated', h.subscription(user, { cancel_at_period_end: true }));
  assert.equal(h.rows.get(user._id).subscriptionActive, true);
  assert.equal(h.rows.get(user._id).subscriptionCancelAtPeriodEnd, true);
  await h.send('customer.subscription.deleted', h.subscription(user, { status: 'canceled' }));
  assert.equal(h.rows.get(user._id).subscriptionActive, false);
}));

test('failed renewal removes access and successful recovery restores it', () => harness(async h => {
  const user = h.addUser();
  await h.send('customer.subscription.updated', h.subscription(user, { status: 'past_due' }));
  assert.equal(h.rows.get(user._id).subscriptionActive, false);
  await h.send('customer.subscription.updated', h.subscription(user));
  assert.equal(h.rows.get(user._id).subscriptionActive, true);
}));

test('forged webhook signature is rejected without changing users', () => harness(async h => {
  const user = h.addUser();
  assert.equal((await h.send('customer.subscription.created', h.subscription(user), { invalidSignature: true })).status, 400);
  assert.equal(h.rows.get(user._id).subscriptionActive, false);
}));

test('duplicate completed event keeps one membership and leaves other accounts unchanged', () => harness(async h => {
  const user = h.addUser(), other = h.addUser('qa-other');
  const session = h.session(user, h.subscription(user));
  await h.send('checkout.session.completed', session);
  await h.send('checkout.session.completed', session);
  assert.equal(h.rows.size, 2);
  assert.equal(h.rows.get(user._id).stripeSubscriptionId, session.subscription.id);
  assert.deepEqual(h.rows.get(other._id), other);
}));

test('persistence failure returns 500 so Stripe can retry', () => harness(async h => {
  const user = h.addUser(); h.failSave(true);
  assert.equal((await h.send('customer.subscription.updated', h.subscription(user))).status, 500);
  assert.equal(h.rows.get(user._id).subscriptionActive, false);
  h.failSave(false);
  assert.equal((await h.send('customer.subscription.updated', h.subscription(user))).status, 200);
  assert.equal(h.rows.get(user._id).subscriptionActive, true);
}));

// Regression expectations for the current API schema and delayed delivery.
for (const event of ['checkout.session.completed', 'customer.subscription.updated']) {
  test(`${event} stores period end from the current Stripe item-level schema`, () => harness(async h => {
    const user = h.addUser(), sub = h.subscription(user, { modern: true });
    await h.send(event, event === 'checkout.session.completed' ? h.session(user, sub) : sub);
    assert.equal(h.rows.get(user._id).subscriptionCurrentPeriodEnd?.getTime(), periodEnd * 1000);
  }));
}

test('an old active event cannot reactivate a canceled membership', () => harness(async h => {
  const user = h.addUser();
  await h.send('customer.subscription.deleted', h.subscription(user, { status: 'canceled' }), { created: 200 });
  await h.send('customer.subscription.updated', h.subscription(user), { created: 100, stale: true });
  assert.equal(h.rows.get(user._id).subscriptionActive, false);
}));

test('live-mode events are rejected by a test-mode server', () => harness(async h => {
  const user = h.addUser();
  assert.equal((await h.send('customer.subscription.created', h.subscription(user), { livemode: true })).status, 400);
  assert.equal(h.rows.get(user._id).subscriptionActive, false);
}));

test('checkout metadata cannot activate a different customer', () => harness(async h => {
  const user = h.addUser(), other = h.addUser('qa-other');
  const session = h.session(user, h.subscription(user));
  session.metadata.coachUserId = other._id;
  h.sessions.set(session.id, session);
  assert.equal((await h.send('checkout.session.completed', session)).status, 500);
  assert.equal(h.rows.get(other._id).subscriptionActive, false);
}));

test('unrelated price cannot grant Coach access', () => harness(async h => {
  const user = h.addUser(), sub = h.subscription(user);
  sub.items.data[0].price.id = 'price_unrelated';
  await h.send('customer.subscription.created', sub);
  assert.equal(h.rows.get(user._id).subscriptionActive, false);
}));

test('cancellation of an old subscription preserves the newer active membership', () => harness(async h => {
  const user = h.addUser();
  await h.send('customer.subscription.created', h.subscription(user, { id: 'sub_new', created: 200 }));
  await h.send('customer.subscription.deleted', h.subscription(user, { status: 'canceled', created: 100 }));
  assert.equal(h.rows.get(user._id).subscriptionActive, true);
  assert.equal(h.rows.get(user._id).stripeSubscriptionId, 'sub_new');
}));

test('a slower concurrent snapshot cannot overwrite a newer cancellation', () => harness(async h => {
  const user = h.addUser(), sub = h.subscription(user);
  h.currentSubscriptions.set(sub.id, sub);
  let resume, entered;
  const blocked = new Promise(resolve => { resume = resolve; });
  const started = new Promise(resolve => { entered = resolve; });
  h.listHook(async () => { entered(); await blocked; });
  const old = h.sync.syncUser(user._id);
  await started;
  h.listHook(null);
  h.currentSubscriptions.set(sub.id, { ...sub, status: 'canceled' });
  await h.sync.syncUser(user._id);
  resume(); await old;
  assert.equal(h.rows.get(user._id).subscriptionActive, false);
  assert.equal(h.rows.get(user._id).stripeSyncRevision, 2);
}));

test('Stripe API errors preserve previous membership and allow webhook retry', () => harness(async h => {
  const user = h.addUser();
  await h.send('customer.subscription.created', h.subscription(user));
  h.listHook(() => { throw new Error('simulated Stripe outage'); });
  assert.equal((await h.send('customer.subscription.updated', h.subscription(user, {status:'canceled'}))).status, 500);
  assert.equal(h.rows.get(user._id).subscriptionActive, true);
  h.listHook(null);
  await h.send('customer.subscription.updated', h.subscription(user, {status:'canceled'}));
  assert.equal(h.rows.get(user._id).subscriptionActive, false);
}));
