import test from 'node:test';
import assert from 'node:assert/strict';
import { createAudioPaymentAnalytics, normalizeAudioMeasurement } from '../src/tec-audio-payment-analytics.js';

const time = 1794312000000;
const user = { _id: 'listener@private.test', stripeCustomerId: 'cus_private' };
const analytics = { consentVersion: 2, clientId: '123456789.1794310000', sessionId: '1794310000',
  campaign: { campaign_source: 'chatgpt', campaign_medium: 'cpc', campaign_name: 'tec_clasicos_es', campaign_content: 'james_allen_muestra' } };
const invoice = { id: 'in_private', customer: 'cus_private', subscription: 'sub_private', livemode: true,
  status: 'paid', paid_out_of_band: false, amount_paid: 2000, currency: 'usd', billing_reason: 'subscription_create',
  status_transitions: { paid_at: time / 1000 - 60 }, customer_email: user._id };
const event = value => ({ type: 'invoice.paid', livemode: true, data: { object: value } });

function fixture({ account = user, goodPlan = true, secret = 'test-private-secret', fail = false } = {}) {
  const contexts = new Map(), deliveries = new Map(), sent = [];
  const lean = fn => ({ async lean() { return fn(); } });
  const service = createAudioPaymentAnalytics({ now: () => time, apiSecret: secret, priceId: 'price_audio',
    accounts: { findOne: () => lean(() => account) },
    contexts: {
      async updateOne({ _id }, { $set }) { contexts.set(_id, { ...$set }); },
      findById: id => lean(() => contexts.get(id)),
      async deleteOne({ _id }) { contexts.delete(_id); }
    },
    deliveries: {
      findOneAndUpdate({ _id }, { $set, $setOnInsert }) { return lean(() => {
        const previous = deliveries.get(_id);
        if (previous && (previous.status === 'sent' || previous.leaseUntil > new Date(time))) {
          throw Object.assign(new Error('duplicate'), { code: 11000 });
        }
        const row = { ...$setOnInsert, ...previous, ...$set }; deliveries.set(_id, row); return row;
      }); },
      async updateOne({ _id }, { $set, $unset }) {
        const row = deliveries.get(_id); Object.assign(row, $set);
        for (const key of Object.keys($unset || {})) delete row[key];
      },
      async deleteOne({ _id }) { deliveries.delete(_id); }
    },
    stripe: {
      subscriptions: { async retrieve() { return { metadata: { audience: 'tec-audiolibros' },
        items: { data: [{ price: { id: goodPlan ? 'price_audio' : 'price_coach' } }] } }; } },
      invoices: { async retrieve() { return invoice; } },
      invoicePayments: { async *list({ invoice: invoiceId }) { yield { invoice: invoiceId, status: 'paid', livemode: true,
        currency: 'usd', amount_paid: 2000, payment: { type: 'payment_intent', payment_intent: 'pi_private' } }; } }
    },
    fetchImpl: async (url, options) => {
      sent.push({ url: String(url), body: JSON.parse(options.body) });
      if (fail) { fail = false; throw new Error('private downstream details'); }
      return { ok: true };
    }
  });
  return { service, contexts, deliveries, sent };
}

test('measurement input accepts reviewed campaigns and rejects identifiers or consent that cannot be measured', () => {
  assert.deepEqual(normalizeAudioMeasurement(analytics), analytics);
  for (const input of [null, {}, { ...analytics, consentVersion: 1 }, { ...analytics, clientId: user._id },
    { ...analytics, sessionId: 'token-secret' }]) assert.equal(normalizeAudioMeasurement(input), null);
  assert.deepEqual(normalizeAudioMeasurement({ ...analytics, campaign: { campaign_source: user._id } }).campaign, {});
  assert.equal(normalizeAudioMeasurement({ ...analytics, campaign: {
    campaign_source: 'tec_test', campaign_medium: 'qa', campaign_name: 'verificacion_analytics' } }), null);
});

test('a verified paid invoice produces one purchase with actual value and no account or billing identity', async () => {
  const f = fixture(); await f.service.saveContext(user, analytics);
  assert.equal(await f.service.handleStripeEvent(event(invoice)), true);
  const [{ body }] = f.sent;
  assert.equal(body.client_id, analytics.clientId);
  const purchase = body.events[0];
  assert.equal(purchase.name, 'purchase'); assert.equal(purchase.params.value, 20);
  assert.equal(purchase.params.currency, 'USD'); assert.equal(purchase.params.session_id, analytics.sessionId);
  assert.equal(purchase.params.tec_audio_payment_type, 'new_subscription');
  assert.equal(purchase.params.tec_audio_source, 'chatgpt');
  assert.equal(purchase.params.campaign_source, undefined);
  assert.ok(purchase.params.transaction_id.startsWith('tec_audio_'));
  assert.deepEqual(body.consent, { ad_user_data: 'DENIED', ad_personalization: 'DENIED' });
  assert.ok(!JSON.stringify(body).match(/listener|private|cus_|sub_|in_|test-private-secret/));
  assert.equal(await f.service.handleStripeEvent(event(invoice)), false);
  assert.equal(await f.service.handleStripeEvent({ type: 'checkout.session.completed', livemode: true,
    data: { object: { metadata: { audience: 'tec-audiolibros' }, payment_status: 'paid', invoice: invoice.id } } }), false);
  assert.equal(f.sent.length, 1);
});

test('initial Checkout and modern Stripe invoice shape are both supported without relying on a return page', async () => {
  const f = fixture(); await f.service.saveContext(user, analytics);
  assert.equal(await f.service.handleStripeEvent({ type: 'checkout.session.completed', livemode: true,
    data: { object: { metadata: { audience: 'tec-audiolibros' }, payment_status: 'paid', invoice: invoice.id } } }), true);
  const other = { ...invoice, id: 'in_renewal', subscription: undefined, paid_out_of_band: undefined,
    parent: { subscription_details: { subscription: 'sub_private' } }, billing_reason: 'subscription_cycle' };
  assert.equal(await f.service.handleStripeEvent(event(other)), true);
  assert.equal(f.sent[1].body.events[0].params.tec_audio_payment_type, 'renewal');
});

test('no purchase for failed, free, test, manual, wrong-currency, stale or future invoices', async () => {
  const f = fixture(); await f.service.saveContext(user, analytics);
  for (const change of [{ status: 'open' }, { amount_paid: 0 }, { amount_paid: -20 }, { amount_paid: 20.5 },
    { currency: 'eur' }, { livemode: false }, { paid_out_of_band: true }, { subscription: null },
    { status_transitions: { paid_at: time / 1000 + 1 } }, { status_transitions: { paid_at: time / 1000 - 72 * 3600 } }]) {
    assert.equal(await f.service.handleStripeEvent(event({ ...invoice, ...change })), false, JSON.stringify(change));
  }
  assert.equal(await f.service.handleStripeEvent({ ...event(invoice), livemode: false }), false);
  assert.equal(f.sent.length, 0);
});

test('missing, expired or revoked consent and excluded accounts cannot produce revenue events', async () => {
  for (const options of [{ account: { ...user, isTest: true } }, { account: { ...user, analyticsExcluded: true } },
    { account: null }, { goodPlan: false }, { secret: '' }]) {
    const f = fixture(options); await f.service.saveContext(user, analytics);
    assert.equal(await f.service.handleStripeEvent(event(invoice)), false); assert.equal(f.sent.length, 0);
  }
  const f = fixture(); assert.equal(await f.service.handleStripeEvent(event(invoice)), false);
  await f.service.saveContext(user, analytics); await f.service.revoke(user);
  assert.equal(await f.service.handleStripeEvent(event(invoice)), false);
  await f.service.saveContext(user, analytics); for (const value of f.contexts.values()) value.expiresAt = new Date(time - 1);
  assert.equal(await f.service.handleStripeEvent(event(invoice)), false);
});

test('concurrent webhooks reserve one delivery, and failures are retryable with a stable transaction ID', async () => {
  const f = fixture(); await f.service.saveContext(user, analytics);
  await Promise.all([f.service.handleStripeEvent(event(invoice)), f.service.handleStripeEvent(event(invoice))]);
  assert.equal(f.sent.length, 1);
  const retry = fixture({ fail: true }); await retry.service.saveContext(user, analytics);
  await assert.rejects(retry.service.handleStripeEvent(event(invoice)), /^Error: Audio analytics delivery failed$/);
  assert.equal(await retry.service.handleStripeEvent(event(invoice)), true);
  assert.equal(retry.sent[0].body.events[0].params.transaction_id, retry.sent[1].body.events[0].params.transaction_id);
});

test('modern invoices require settled Stripe payments and reject manual or incomplete payment records', async () => {
  const f = fixture(); await f.service.saveContext(user, analytics);
  const payment = { invoice: invoice.id, status: 'paid', livemode: true, currency: 'usd', amount_paid: 2000,
    payment: { type: 'payment_intent', payment_intent: 'pi_private' } };
  for (const change of [{ payment: { type: 'payment_record', payment_record: 'pyr_manual' } },
    { status: 'open' }, { livemode: false }, { amount_paid: 1999 }, { invoice: 'in_other' }]) {
    assert.equal(await f.service.handleStripeEvent(event({ ...invoice, paid_out_of_band: undefined,
      payments: { data: [{ ...payment, ...change }], has_more: false } })), false);
  }
  assert.equal(await f.service.handleStripeEvent(event({ ...invoice, paid_out_of_band: undefined,
    payments: { data: [payment], has_more: false } })), true);
  assert.equal(f.sent.length, 1);
});

test('paid subscriptions retain the exact public reel and network through checkout context and Stripe invoice delivery', async () => {
  for (const [source, content] of [['instagram','ig_dfqtbe5ngiy'], ['facebook','fb_1835156844281065'], ['youtube','yt_bbknrqo9uu4'], ['tiktok','tt_7707440558070975775']]) {
    const f = fixture(); const measurement = { ...analytics, campaign: { campaign_source: source, campaign_medium: 'social', campaign_name: 'tec_redes_es', campaign_content: content } };
    assert.equal(await f.service.saveContext(user, measurement), true);
    assert.equal(await f.service.handleStripeEvent(event(invoice)), true);
    const params = f.sent[0].body.events[0].params;
    assert.equal(params.tec_audio_source, source); assert.equal(params.tec_audio_content, content);
    assert.ok(params.tec_audio_piece.length > 0); assert.equal(params.value, 20);
    assert.equal(params.campaign_source, undefined); assert.equal(params.campaign_content, undefined);
    assert.equal(await f.service.handleStripeEvent(event(invoice)), false);
    assert.ok(!JSON.stringify(f.sent[0].body).match(/listener|private|cus_|sub_|in_/));
    await f.service.revoke(user); assert.equal(await f.service.handleStripeEvent(event({ ...invoice, id: 'in_next' })), false);
  }
});
