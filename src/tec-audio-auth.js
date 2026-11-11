import crypto from 'node:crypto';
import { promisify } from 'node:util';

const scrypt = promisify(crypto.scrypt);
const hash = value => crypto.createHash('sha256').update(value).digest('hex');
const freshToken = () => crypto.randomBytes(32).toString('base64url');
const objectId = value => typeof value === 'string' ? value : value?.id;
export const AUDIO_ACCOUNT_FLOW = 'paid-account-v1';
export const normalizeAudioEmail = value => typeof value === 'string' ? value.trim().toLowerCase() : '';
export const validAudioEmail = value => typeof value === 'string' && value.length <= 254 && /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(value);
export const normalizeAudioUsername = value => typeof value === 'string' ? value.trim().toLowerCase() : '';
export const validAudioUsername = value => typeof value === 'string' && /^[a-z][a-z0-9_-]{2,31}$/.test(value);
export const hasAudioPassword = user => Boolean(user && !user.isTest && user.emailVerifiedAt &&
  user.passwordScheme === 'scrypt-v1' && user.credentialVersion && user.usernameCanonical);
export const validAudioPassword = value => typeof value === 'string' && Array.from(value).length >= 15 &&
  Array.from(value).length <= 128 && !/^(.)\1+$/u.test(value) &&
  !['123456789012345', '1234567890123456', 'passwordpassword', 'contraseñacontraseña'].includes(value.toLowerCase());

// OWASP's equivalent scrypt configuration: 32 MiB, N=2^15, r=8, p=3.
// Bound concurrent work on the existing small Render instance.
let activeHashes = 0;
const hashQueue = [];
async function derive(password, salt) {
  if (activeHashes >= 2) {
    if (hashQueue.length >= 8) throw new Error('audio_auth_busy');
    await new Promise(resolve => hashQueue.push(resolve));
  } else activeHashes++;
  try { return await scrypt(password.normalize('NFC'), salt, 64, { N: 32768, r: 8, p: 3, maxmem: 64 * 1024 * 1024 }); }
  finally { const next = hashQueue.shift(); if (next) next(); else activeHashes--; }
}
export async function createAudioPassword(password) {
  const passwordSalt = crypto.randomBytes(16).toString('hex');
  return { passwordSalt, passwordHash: (await derive(password, passwordSalt)).toString('hex'), passwordScheme: 'scrypt-v1' };
}
export async function verifyAudioPassword(password, user) {
  const supported = hasAudioPassword(user) && /^[a-f0-9]{32}$/.test(user.passwordSalt || '') && /^[a-f0-9]{128}$/.test(user.passwordHash || '');
  const actual = await derive(typeof password === 'string' && password.length <= 512 ? password : '', supported ? user.passwordSalt : '0'.repeat(32));
  return crypto.timingSafeEqual(actual, Buffer.from(supported ? user.passwordHash : '0'.repeat(128), 'hex')) && supported;
}
const failure = (status, message) => Object.assign(new Error(message), { status });
const invalidLink = () => failure(400, 'Este enlace caducó o ya se usó. Solicita otro desde Recuperar acceso.');

export function createPaidAudioAuth({ accounts, stripe, priceId, publicOrigin = 'https://totalelitecoaching.com',
  sendAccountMail, liveMode = true, now = () => Date.now() } = {}) {
  const landing = new URL('/audiolibros/', publicOrigin).href;
  const audience = 'tec-audiolibros';
  const correctPlan = subscription => subscription?.metadata?.audience === audience &&
    subscription.items?.data?.some(item => objectId(item.price) === priceId);

  async function paidInvoice(invoice, customer, subscription) {
    if (!invoice?.id || invoice.livemode !== liveMode || invoice.status !== 'paid' || invoice.currency !== 'usd' ||
      !Number.isSafeInteger(invoice.amount_paid) || invoice.amount_paid <= 0 || invoice.paid_out_of_band === true ||
      objectId(invoice.customer) !== customer || objectId(invoice.parent?.subscription_details?.subscription || invoice.subscription) !== subscription) return false;
    if (invoice.paid_out_of_band === false) return true;
    let amount = 0;
    for await (const payment of stripe.invoicePayments.list({ invoice: invoice.id, status: 'paid', limit: 100 })) {
      if (payment.livemode === liveMode && payment.status === 'paid' && payment.currency === 'usd' &&
        objectId(payment.invoice) === invoice.id && ['payment_intent', 'charge'].includes(payment.payment?.type) &&
        objectId(payment.payment[payment.payment.type]) && Number.isSafeInteger(payment.amount_paid) && payment.amount_paid > 0) amount += payment.amount_paid;
    }
    return amount >= invoice.amount_paid;
  }
  async function paidHistory(user) {
    if (!stripe || !user?.stripeCustomerId || user.isTest) return false;
    if (user.paidAt && user.paidCheckoutId) return true;
    const subscriptions = new Set();
    for await (const sub of stripe.subscriptions.list({ customer: user.stripeCustomerId, status: 'all', limit: 100 })) {
      if (correctPlan(sub)) subscriptions.add(sub.id);
    }
    for await (const invoice of stripe.invoices.list({ customer: user.stripeCustomerId, status: 'paid', limit: 100 })) {
      const sub = objectId(invoice.parent?.subscription_details?.subscription || invoice.subscription);
      if (subscriptions.has(sub) && await paidInvoice(invoice, user.stripeCustomerId, sub)) return true;
    }
    return false;
  }
  async function sendGrant(user, purpose) {
    if (!sendAccountMail) throw failure(503, 'Los correos de acceso están temporalmente ocupados. Inténtalo de nuevo.');
    const time = now(), secret = freshToken(), digest = hash(secret);
    const expires = purpose === 'activation' ? 24 * 3600000 : 30 * 60000;
    const claimed = await accounts.findOneAndUpdate({ _id: user._id, isTest: { $ne: true },
      credentialVersion: user.credentialVersion || { $exists: false },
      $or: [{ authGrantDigest: { $exists: false } }, { authGrantRequestedAt: { $exists: false } }, { authGrantRequestedAt: { $lte: new Date(time - 60000) } }] },
    { $set: { authGrantDigest: digest, authGrantPurpose: purpose, authGrantExpiresAt: new Date(time + expires),
      authGrantRequestedAt: new Date(time), authGrantCredentialVersion: user.credentialVersion || '' } }, { new: true }).lean();
    if (!claimed) return false;
    try { await sendAccountMail({ email: user._id, purpose, url: `${landing}#cuenta=${secret}` }); }
    catch {
      await accounts.updateOne({ _id: user._id, authGrantDigest: digest }, { $unset: {
        authGrantDigest: 1, authGrantExpiresAt: 1, authGrantRequestedAt: 1, authGrantPurpose: 1, authGrantCredentialVersion: 1 } });
      throw failure(503, 'No pudimos enviar el correo de acceso. Inténtalo de nuevo.');
    }
    return true;
  }
  async function requestRecovery(rawEmail) {
    const email = normalizeAudioEmail(rawEmail);
    if (!validAudioEmail(email)) throw failure(400, 'Escribe un correo válido.');
    const user = await accounts.findById(email).lean();
    if (!user || user.isTest) return;
    if (hasAudioPassword(user)) return sendGrant(user, 'reset');
    if (await paidHistory(user)) return sendGrant(user, 'activation');
    // A legacy reader may have abandoned Checkout before this change. Verify
    // ownership before reusing their billing customer; never create a free login.
    if (user.stripeCustomerId) await sendGrant(user, 'checkout');
  }
  async function grantUser(secret) {
    if (typeof secret !== 'string' || !/^[A-Za-z0-9_-]{43}$/.test(secret)) throw invalidLink();
    const user = await accounts.findOne({ authGrantDigest: hash(secret), authGrantExpiresAt: { $gt: new Date(now()) }, isTest: { $ne: true } }).lean();
    if (!user || !['activation', 'reset', 'checkout'].includes(user.authGrantPurpose) ||
      (user.credentialVersion || '') !== user.authGrantCredentialVersion ||
      (user.authGrantPurpose === 'reset' && !hasAudioPassword(user))) throw invalidLink();
    return user;
  }
  async function grantInfo(secret) {
    const user = await grantUser(secret);
    return { purpose: user.authGrantPurpose, ...(user.authGrantPurpose === 'reset' ? { username: user.username } : {}) };
  }
  async function completeCredentials({ token: secret, username, password, passwordConfirmation } = {}) {
    const user = await grantUser(secret);
    if (user.authGrantPurpose === 'checkout') throw failure(403, 'Primero completa el pago de tu membresía.');
    if (password !== passwordConfirmation || !validAudioPassword(password)) {
      throw failure(400, 'Usa una contraseña de 15 a 128 caracteres y escríbela igual en ambos campos. Puedes usar una frase.');
    }
    const activation = user.authGrantPurpose === 'activation';
    const name = activation ? normalizeAudioUsername(username) : user.usernameCanonical;
    if (!validAudioUsername(name)) throw failure(400, 'Elige un usuario de 3 a 32 caracteres: empieza con una letra y usa letras sin acentos, números, guion o guion bajo.');
    if (activation && !await paidHistory(user)) throw failure(403, 'Primero completa el pago de tu membresía.');
    // The unique index is ready before assigning the first public username.
    await accounts.init?.();
    const credentials = await createAudioPassword(password);
    let updated;
    try {
      updated = await accounts.findOneAndUpdate({ _id: user._id, authGrantDigest: hash(secret),
        authGrantExpiresAt: { $gt: new Date(now()) }, authGrantCredentialVersion: user.credentialVersion || '',
        credentialVersion: user.credentialVersion || { $exists: false }, authGrantPurpose: user.authGrantPurpose, isTest: { $ne: true } },
      { $set: { ...credentials, username: name, usernameCanonical: name, emailVerifiedAt: user.emailVerifiedAt || new Date(now()),
        credentialVersion: freshToken(), updatedAt: new Date(now()) }, $unset: {
          authGrantDigest: 1, authGrantExpiresAt: 1, authGrantPurpose: 1, authGrantCredentialVersion: 1 } }, { new: true }).lean();
    } catch (error) {
      if (error.code === 11000) throw failure(409, 'Ese usuario ya está ocupado. Elige otro.');
      throw error;
    }
    if (!updated) throw invalidLink();
    if (!activation && sendAccountMail) {
      try { await sendAccountMail({ email: user._id, purpose: 'password_changed' }); }
      catch { console.warn('TEC audio: password change notification could not be sent'); }
    }
    return { user: updated, purpose: activation ? 'activation' : 'reset' };
  }
  async function login(identifier, password) {
    const raw = typeof identifier === 'string' ? identifier.trim() : '';
    let user;
    if (validAudioEmail(normalizeAudioEmail(raw))) user = await accounts.findById(normalizeAudioEmail(raw)).lean();
    else if (validAudioUsername(normalizeAudioUsername(raw))) user = await accounts.findOne({ usernameCanonical: normalizeAudioUsername(raw) }).lean();
    if (!await verifyAudioPassword(password, user)) throw failure(401, 'Usuario o contraseña incorrectos.');
    return user;
  }
  async function checkoutOwner(secret) {
    const user = await grantUser(secret);
    if (user.authGrantPurpose !== 'checkout' || !user.stripeCustomerId || hasAudioPassword(user)) throw invalidLink();
    return user;
  }
  async function consumeCheckoutOwner(secret) {
    await accounts.updateOne({ authGrantDigest: hash(secret), authGrantPurpose: 'checkout' }, { $unset: {
      authGrantDigest: 1, authGrantExpiresAt: 1, authGrantPurpose: 1, authGrantCredentialVersion: 1 } });
  }
  async function fulfillCheckout(checkoutId) {
    if (!stripe || !priceId || typeof checkoutId !== 'string' || !/^cs_[A-Za-z0-9_]{10,220}$/.test(checkoutId)) return false;
    const checkout = await stripe.checkout.sessions.retrieve(checkoutId);
    if (checkout.metadata?.audience !== audience || checkout.metadata?.account_flow !== AUDIO_ACCOUNT_FLOW ||
      checkout.livemode !== liveMode || checkout.mode !== 'subscription' || checkout.status !== 'complete' ||
      checkout.payment_status !== 'paid' || checkout.currency !== 'usd' || !Number.isSafeInteger(checkout.amount_total) || checkout.amount_total <= 0) return false;
    const customer = objectId(checkout.customer), subscriptionId = objectId(checkout.subscription), invoiceId = objectId(checkout.invoice);
    const existingCustomer = customer ? await accounts.findOne({ stripeCustomerId: customer, isTest: { $ne: true } }).lean() : null;
    const email = normalizeAudioEmail(existingCustomer?._id || checkout.customer_details?.email || checkout.customer_email);
    if (!customer || !subscriptionId || !invoiceId || !validAudioEmail(email)) return false;
    const [subscription, invoice] = await Promise.all([stripe.subscriptions.retrieve(subscriptionId), stripe.invoices.retrieve(invoiceId)]);
    if (!correctPlan(subscription) || objectId(subscription.customer) !== customer || subscription.status !== 'active' || subscription.pause_collection ||
      !await paidInvoice(invoice, customer, subscriptionId)) return false;
    await accounts.updateOne({ _id: email }, { $setOnInsert: { _id: email, stripeCustomerId: customer,
      paidAt: new Date(now()), paidCheckoutId: checkout.id, createdAt: new Date(now()) } }, { upsert: true });
    let user = await accounts.findById(email).lean();
    if (user?.isTest || (user.stripeCustomerId && user.stripeCustomerId !== customer)) {
      throw failure(409, 'Contacta a TEC para asociar tu pago con tu cuenta existente.');
    }
    // Preserve an existing reader's progress while migrating a paid legacy account.
    if (!user.stripeCustomerId) {
      await accounts.updateOne({ _id: email, stripeCustomerId: { $exists: false } }, { $set: {
        stripeCustomerId: customer, paidAt: new Date(now()), paidCheckoutId: checkout.id } });
      user = await accounts.findById(email).lean();
      if (user.stripeCustomerId !== customer) throw failure(409, 'Contacta a TEC para asociar tu pago.');
    }
    if (hasAudioPassword(user)) return true;
    await accounts.updateOne({ _id: email, stripeCustomerId: customer }, { $set: {
      paidAt: user.paidAt || new Date(now()), paidCheckoutId: user.paidCheckoutId || checkout.id } });
    const time = now();
    const claim = await accounts.findOneAndUpdate({ _id: email, $or: [
      { activationMailCheckoutId: { $ne: checkout.id } }, { activationMailState: 'pending' },
      { activationMailState: 'sending', activationMailLeaseUntil: { $lte: new Date(time) } }
    ] }, { $set: { activationMailCheckoutId: checkout.id, activationMailState: 'sending', activationMailLeaseUntil: new Date(time + 60000) } }, { new: true }).lean();
    if (!claim) return true;
    try {
      const sent = await sendGrant(user, 'activation');
      // An explicit recovery request may already have sent the usable activation link.
      if (!sent && !await accounts.findOne({ _id: email, authGrantPurpose: 'activation', authGrantExpiresAt: { $gt: new Date(now()) } }).lean()) throw new Error('activation_pending');
      await accounts.updateOne({ _id: email, activationMailCheckoutId: checkout.id }, { $set: { activationMailState: 'sent' }, $unset: { activationMailLeaseUntil: 1 } });
    } catch {
      await accounts.updateOne({ _id: email, activationMailCheckoutId: checkout.id }, { $set: { activationMailState: 'pending' }, $unset: { activationMailLeaseUntil: 1 } });
      throw failure(503, 'Tu pago está guardado. Reintenta el envío desde Recuperar acceso.');
    }
    return true;
  }
  async function handleStripeEvent(event) {
    if (!stripe || event?.livemode !== liveMode || event.type !== 'checkout.session.completed' ||
      event.data.object.metadata?.account_flow !== AUDIO_ACCOUNT_FLOW) return false;
    return fulfillCheckout(event.data.object.id);
  }
  return { login, requestRecovery, grantInfo, completeCredentials, fulfillCheckout, handleStripeEvent, checkoutOwner, consumeCheckoutOwner };
}
