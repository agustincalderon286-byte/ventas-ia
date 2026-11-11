import test from 'node:test';
import assert from 'node:assert/strict';
import { createPaidAudioAuth, createAudioPassword, verifyAudioPassword, validAudioPassword, AUDIO_ACCOUNT_FLOW } from '../src/tec-audio-auth.js';
import { audioAccountStore } from './helpers/audio-account-store.js';

const email = 'paid@example.test', password = 'Una frase larga para leer 2026';
const checkoutId = 'cs_live_paid_account_test_123456789';
function fixture() {
  let clock = Date.now();
  const store = audioAccountStore(), sent = [];
  const subscription = { id: 'sub_audio', customer: 'cus_audio', status: 'active', metadata: { audience: 'tec-audiolibros' }, items: { data: [{ price: { id: 'price_audio' } }] } };
  const invoice = { id: 'in_audio', customer: 'cus_audio', subscription: 'sub_audio', livemode: true, currency: 'usd', amount_paid: 2000, status: 'paid', paid_out_of_band: false };
  const checkout = { id: checkoutId, metadata: { audience: 'tec-audiolibros', account_flow: AUDIO_ACCOUNT_FLOW }, livemode: true,
    mode: 'subscription', status: 'complete', payment_status: 'paid', amount_total: 2000, currency: 'usd',
    customer: 'cus_audio', customer_details: { email }, subscription: 'sub_audio', invoice: 'in_audio' };
  let failMail = false;
  const service = createPaidAudioAuth({ accounts: store.model, priceId: 'price_audio', now: () => clock,
    stripe: {
      checkout: { sessions: { async retrieve() { return checkout; } } },
      subscriptions: { async retrieve() { return subscription; }, async *list() { yield subscription; } },
      invoices: { async retrieve() { return invoice; }, async *list() { yield invoice; } },
      invoicePayments: { async *list() {} }
    },
    sendAccountMail: async message => { if (failMail) throw new Error('private provider error'); sent.push(message); }
  });
  const token = () => new URLSearchParams(new URL(sent.findLast(message => message.url).url).hash.slice(1)).get('cuenta');
  const activate = name => service.completeCredentials({ token: token(), username: name, password, passwordConfirmation: password });
  return { ...store, service, checkout, subscription, invoice, sent, token, activate,
    advance(ms) { clock += ms; }, mailFailure(value) { failMail = value; } };
}

test('paid checkout is authoritative, creates only a pending account, and duplicate webhooks send one activation', async () => {
  const f = fixture();
  assert.equal(f.rows.size, 0);
  await Promise.all([f.service.fulfillCheckout(checkoutId), f.service.fulfillCheckout(checkoutId)]);
  assert.equal(f.rows.size, 1); assert.equal(f.sent.length, 1);
  const pending = f.rows.get(email);
  assert.equal(pending.emailVerifiedAt, undefined); assert.equal(pending.passwordHash, undefined);
  assert.equal(pending.activationMailState, 'sent');
  assert.ok(pending.authGrantDigest); assert.ok(!JSON.stringify(pending).includes(f.token()));
  assert.deepEqual(await f.service.grantInfo(f.token()), { purpose: 'activation' });
  await assert.rejects(f.service.login(email, password), /incorrectos/);
  const created = await f.activate('lector_tec');
  assert.equal(created.purpose, 'activation'); assert.ok(created.user.emailVerifiedAt);
  assert.equal(created.user.usernameCanonical, 'lector_tec');
  assert.ok(created.user.passwordHash); assert.ok(!JSON.stringify(created.user).includes(password));
  assert.equal((await f.service.login('LECTOR_TEC', password))._id, email);
  assert.equal((await f.service.login('PAID@example.test', password))._id, email);
  await assert.rejects(f.activate('otro_usuario'), /caducó o ya se usó/);
  assert.equal(await f.service.fulfillCheckout(checkoutId), true); assert.equal(f.sent.length, 1);
});

test('unpaid, zero-value, test-mode, manual, unrelated or mismatched payments never create an account', async () => {
  for (const change of [{ payment_status: 'unpaid' }, { status: 'open' }, { amount_total: 0 }, { livemode: false },
    { currency: 'eur' }, { metadata: { audience: 'coach', account_flow: AUDIO_ACCOUNT_FLOW } },
    { metadata: { audience: 'tec-audiolibros' } }]) {
    const f = fixture(); Object.assign(f.checkout, change);
    assert.equal(await f.service.fulfillCheckout(checkoutId), false); assert.equal(f.rows.size, 0); assert.equal(f.sent.length, 0);
  }
  for (const [target, change] of [['invoice', { status: 'open' }], ['invoice', { paid_out_of_band: true }],
    ['invoice', { customer: 'cus_other' }], ['invoice', { amount_paid: 0 }],
    ['invoice', { paid_out_of_band: undefined }], ['subscription', { status: 'trialing' }],
    ['subscription', { customer: 'cus_other' }], ['subscription', { items: { data: [{ price: { id: 'price_other' } }] } }]]) {
    const f = fixture(); Object.assign(f[target], change);
    assert.equal(await f.service.fulfillCheckout(checkoutId), false); assert.equal(f.rows.size, 0);
  }
});

test('recovery is email-confirmed and single-use; changing password rotates sessions and preserves username', async () => {
  const f = fixture(); await f.service.fulfillCheckout(checkoutId); await f.activate('lector_tec');
  const previous = f.rows.get(email).credentialVersion;
  await f.service.requestRecovery('unknown@example.test'); assert.equal(f.sent.length, 1);
  f.advance(61000); await f.service.requestRecovery(email);
  const reset = f.token(); assert.deepEqual(await f.service.grantInfo(reset), { purpose: 'reset', username: 'lector_tec' });
  assert.equal((await f.service.login(email, password)).credentialVersion, previous);
  const next = 'Otra frase para escuchar libros 2026';
  await f.service.completeCredentials({ token: reset, username: 'hijacked', password: next, passwordConfirmation: next });
  assert.notEqual(f.rows.get(email).credentialVersion, previous);
  assert.equal(f.rows.get(email).username, 'lector_tec');
  await assert.rejects(f.service.login(email, password), /incorrectos/);
  assert.equal((await f.service.login(email, next)).username, 'lector_tec');
  await assert.rejects(f.service.grantInfo(reset), /caducó o ya se usó/);
  assert.equal(f.sent.at(-1).purpose, 'password_changed');
});

test('expired or invalid links, mismatched passwords and duplicate usernames cannot change credentials', async () => {
  const f = fixture(); await f.service.fulfillCheckout(checkoutId);
  const secret = f.token();
  await assert.rejects(f.service.completeCredentials({ token: secret, username: 'reader', password, passwordConfirmation: 'wrong' }), /15 a 128/);
  assert.equal(f.rows.get(email).passwordHash, undefined);
  f.rows.set('other@example.test', { _id: 'other@example.test', usernameCanonical: 'reader' });
  await assert.rejects(f.activate('reader'), /ocupado/);
  assert.deepEqual(await f.service.grantInfo(secret), { purpose: 'activation' });
  f.advance(24 * 3600000 + 1); await assert.rejects(f.service.grantInfo(secret), /caducó o ya se usó/);
  await assert.rejects(f.service.grantInfo({ $ne: null }), /caducó o ya se usó/);
  assert.equal(f.rows.get(email).passwordHash, undefined);
});

test('existing free records and operator test accounts cannot use public recovery to create a paid login', async () => {
  const f = fixture();
  f.rows.set(email, { _id: email }); await f.service.requestRecovery(email);
  f.rows.set(email, { _id: email, stripeCustomerId: 'cus_audio', isTest: true }); await f.service.requestRecovery(email);
  assert.equal(f.sent.length, 0);
  f.rows.set(email, { _id: email, stripeCustomerId: 'cus_audio' });
  await f.service.requestRecovery(email); assert.equal(f.sent[0].purpose, 'activation');
});

test('activation delivery can recover after provider failure without losing a paid purchase', async () => {
  const f = fixture(); f.mailFailure(true);
  await assert.rejects(f.service.fulfillCheckout(checkoutId), /pago está guardado/);
  assert.equal(f.rows.get(email).stripeCustomerId, 'cus_audio'); assert.equal(f.rows.get(email).activationMailState, 'pending');
  assert.equal(f.rows.get(email).authGrantDigest, undefined);
  f.mailFailure(false); assert.equal(await f.service.fulfillCheckout(checkoutId), true);
  assert.equal(f.sent.length, 1); assert.equal(f.rows.get(email).activationMailState, 'sent');
});

test('customer ownership is never reassigned to a different billing identity', async () => {
  const f = fixture(); f.rows.set(email, { _id: email, stripeCustomerId: 'cus_other' });
  await assert.rejects(f.service.fulfillCheckout(checkoutId), /asociar tu pago/);
  assert.equal(f.rows.get(email).stripeCustomerId, 'cus_other'); assert.equal(f.sent.length, 0);
});

test('password hashing uses unique salts, accepts phrases and performs equal work for unknown users', async () => {
  assert.equal(validAudioPassword('short'), false); assert.equal(validAudioPassword('123456789012345'), false);
  assert.equal(validAudioPassword(password), true);
  const one = await createAudioPassword(password), two = await createAudioPassword(password);
  assert.notEqual(one.passwordHash, two.passwordHash); assert.notEqual(one.passwordSalt, two.passwordSalt);
  const user = { ...one, emailVerifiedAt: new Date(), usernameCanonical: 'lector', credentialVersion: 'version' };
  assert.equal(await verifyAudioPassword(password, user), true);
  assert.equal(await verifyAudioPassword(password + 'x', user), false);
  assert.equal(await verifyAudioPassword(password, null), false);
});

test('verified webhook dispatch respects live mode and only the paid-account Checkout flow', async () => {
  const f = fixture();
  const event = { type: 'checkout.session.completed', livemode: true, data: { object: f.checkout } };
  assert.equal(await f.service.handleStripeEvent({ ...event, livemode: false }), false);
  assert.equal(await f.service.handleStripeEvent({ ...event, type: 'invoice.paid' }), false);
  assert.equal(f.rows.size, 0);
  assert.equal(await f.service.handleStripeEvent(event), true);
  assert.equal(f.rows.size, 1); assert.equal(f.sent.length, 1);
});

test('two simultaneous uses of an activation link assign credentials only once', async () => {
  const f = fixture(); await f.service.fulfillCheckout(checkoutId);
  const results = await Promise.allSettled([f.activate('reader_one'), f.activate('reader_two')]);
  assert.equal(results.filter(value => value.status === 'fulfilled').length, 1);
  assert.equal(results.filter(value => value.status === 'rejected').length, 1);
  assert.ok(['reader_one', 'reader_two'].includes(f.rows.get(email).username));
});

test('a password-reset link expires in 30 minutes without changing the existing password', async () => {
  const f = fixture(); await f.service.fulfillCheckout(checkoutId); await f.activate('reader');
  f.advance(61000); await f.service.requestRecovery(email);
  const secret = f.token(), version = f.rows.get(email).credentialVersion;
  f.advance(30 * 60000 + 1);
  await assert.rejects(f.service.completeCredentials({ token: secret, password, passwordConfirmation: password }), /caducó o ya se usó/);
  assert.equal(f.rows.get(email).credentialVersion, version);
  assert.equal((await f.service.login(email, password)).username, 'reader');
});

test('an abandoned legacy checkout gets email-owned payment continuation, never a free password login', async () => {
  const f = fixture(); f.invoice.status = 'open';
  f.rows.set(email, { _id: email, stripeCustomerId: 'cus_audio' });
  await f.service.requestRecovery(email);
  assert.equal(f.sent.at(-1).purpose, 'checkout');
  const secret = f.token();
  assert.deepEqual(await f.service.grantInfo(secret), { purpose: 'checkout' });
  await assert.rejects(f.activate('reader'), /Primero completa el pago/);
  assert.equal((await f.service.checkoutOwner(secret)).stripeCustomerId, 'cus_audio');
  assert.equal(f.rows.get(email).passwordHash, undefined);
  await f.service.consumeCheckoutOwner(secret);
  await assert.rejects(f.service.checkoutOwner(secret), /caducó o ya se usó/);
  assert.equal(f.rows.size, 1);
});

test('credential revocation during password derivation cannot be overwritten by an older email link', async () => {
  const f = fixture(); await f.service.fulfillCheckout(checkoutId);
  f.model.init = async () => { f.rows.get(email).credentialVersion = 'revoked-by-operator'; };
  await assert.rejects(f.activate('reader'), /caducó o ya se usó/);
  assert.equal(f.rows.get(email).credentialVersion, 'revoked-by-operator');
  assert.equal(f.rows.get(email).passwordHash, undefined);
});
