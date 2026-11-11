import { createPaidAudioAuth, AUDIO_ACCOUNT_FLOW, normalizeAudioEmail, validAudioEmail } from './tec-audio-auth.js';
import { hasTestAudioAccess, normalizeTestUsername, validTestUsername, verifyTestPassword } from './tec-audio-test-accounts.js';
import crypto from 'node:crypto';
import { existsSync } from 'node:fs';
import { audiobookCatalog, findReadyAudiobook, getAudiobookTranscript, streamAudiobook } from './tec-audiobooks.js';
import { createAudiobookShareModel, registerAudiobookSharing } from './tec-audiobook-sharing.js';
import { registerAudioAchievements } from './tec-audio-achievements.js';
import { registerReadingApi } from './tec-reading-books.js';

const api = '/api/audiolibros';
const cookie = 'tec_audio_session';
const audience = 'tec-audiolibros';
const sessionMs = 30 * 86400000;
const hash = value => crypto.createHash('sha256').update(value).digest('hex');
const token = () => crypto.randomBytes(32).toString('base64url');
const tokenPattern = /^[A-Za-z0-9_-]{43}$/;
const previewSeconds = book => Math.floor(book.duration * 100) / 1000;

export function createAudioMembershipModels(mongoose) {
  const model = (name, fields) => mongoose.models[name] || mongoose.model(name, new mongoose.Schema(fields));
  const expiresAt = { type: Date, expires: 0 };
  return {
    shares: createAudiobookShareModel(mongoose),
    accounts: model('TecAudioMember', { _id: String, stripeCustomerId: String, username: String, displayName: String,
      isTest: Boolean, analyticsExcluded: Boolean, accessType: String, testAccessEnabled: Boolean,
      passwordHash: String, passwordSalt: String, passwordScheme: String, credentialVersion: String,
      usernameCanonical: { type: String, unique: true, sparse: true }, emailVerifiedAt: Date,
      paidAt: Date, paidCheckoutId: String,
      authGrantDigest: { type: String, unique: true, sparse: true }, authGrantPurpose: String,
      authGrantExpiresAt: Date, authGrantRequestedAt: Date, authGrantCredentialVersion: String,
      activationMailCheckoutId: String, activationMailState: String, activationMailLeaseUntil: Date,
      createdAt: Date, updatedAt: Date }),
    checkoutIntents: model('TecAudioCheckoutIntent', { _id: String, stripeCustomerId: String, expiresAt }),
    grants: model('TecAudioGrant', { _id: String, email: String, digest: { type: String, index: true }, expiresAt, requestedAt: Date }),
    sessions: model('TecAudioSession', { _id: String, email: String, credentialVersion: String, expiresAt }),
    achievements: model('TecAudioAchievements', { _id: String, revision: Number, state: mongoose.Schema.Types.Mixed }),
    listening: model('TecAudioListening', { _id: String, position: Number, speed: Number, marks: [Number] }),
    readingProgress: model('TecReadingProgress', { _id: String, bookId: String, chapter: Number, paragraph: Number, updatedAt: Date })
  };
}

export function isAudioMembershipPrice(price) {
  return price?.active && price.currency === 'usd' && price.unit_amount === 2000 &&
    price.recurring?.interval === 'month' && price.recurring.interval_count === 1 &&
    price.recurring.usage_type === 'licensed';
}

export function hasAudioMembership(subscription, priceId) {
  return Boolean(priceId && subscription.status === 'active' && !subscription.pause_collection &&
    subscription.metadata?.audience === audience && subscription.items?.data?.some(item => item.price.id === priceId));
}

export function registerAudioMembership(app, {
  accounts, grants, checkoutIntents, sessions, listening, readingProgress, achievements, shares, media, getBucket, r2, stripe, priceId, portalId,
  publicOrigin, sendAccountMail, liveMode = true, secure = process.env.NODE_ENV === 'production',
  catalog = audiobookCatalog, getTranscript = getAudiobookTranscript, paymentAnalytics
}) {
  const origin = new URL(publicOrigin).origin;
  const landing = `${origin}/audiolibros/`;
  const byId = new Map(catalog.map(book => [book.id, book]));
  const sampleIds = new Set(catalog.filter(book => existsSync(new URL(`../public/audiolibros/muestras/${book.id}.m4a`, import.meta.url))).map(book => book.id));
  const cookieOptions = { httpOnly: true, secure, sameSite: 'lax', path: api };
  const accessCache = new Map();
  const mailAttempts = new Map();
  const passwordAttempts = new Map();
  const billingReady = Boolean(stripe && priceId && portalId && sendAccountMail);
  const paidAuth = createPaidAudioAuth({ accounts, stripe, priceId, publicOrigin, sendAccountMail, liveMode });
  const reply = (res, status, error) => res.status(status).json({ error });

  const route = (method, suffix, handler) => app[method](api + suffix, async (req, res) => {
    res.set({ 'Cache-Control': 'private, no-store', 'Vary': 'Cookie' });
    if (method !== 'get' && (req.get('Sec-Fetch-Site') === 'cross-site' ||
      (req.get('Origin') && req.get('Origin') !== origin))) {
      return reply(res, 403, 'Abre la biblioteca de TEC para continuar.');
    }
    try {
      await handler(req, res);
    } catch (error) {
      if (!res.destroyed && error.code !== 'ERR_STREAM_PREMATURE_CLOSE') console.error('TEC audio membership request failed', { route: suffix, code: error.code, type: error.name });
      if (!res.headersSent && !res.destroyed) reply(res, 503, 'El servicio está temporalmente ocupado. Inténtalo de nuevo.');
      else if (!res.destroyed) res.destroy();
    }
  });

  function sessionId(req) {
    const value = (req.get('Cookie') || '').split(/;\s*/).find(part => part.startsWith(cookie + '='))?.slice(cookie.length + 1);
    return value && tokenPattern.test(value) ? hash(value) : null;
  }

  async function account(req) {
    const id = sessionId(req);
    if (!id) return null;
    const session = await sessions.findOne({ _id: id, expiresAt: { $gt: new Date() } }).lean();
    if (!session) return null;
    const user = await accounts.findById(session.email).lean();
    if (user?.paidCheckoutId && !user.emailVerifiedAt) return null;
    if (user?.credentialVersion && session.credentialVersion !== user.credentialVersion) return null;
    return user;
  }

  async function findSubscriptions(user) {
    if (!stripe || !priceId || !user?.stripeCustomerId) return [];
    const result = [];
    for await (const sub of stripe.subscriptions.list({ customer: user.stripeCustomerId, status: 'all', limit: 100 })) {
      if (sub.metadata?.audience === audience) result.push(sub);
    }
    return result;
  }

  async function isMember(user) {
    if (user?.isTest) return hasTestAudioAccess(user);
    if (!stripe || !priceId || !user?.stripeCustomerId) return false;
    const id = user.stripeCustomerId;
    const cached = accessCache.get(id);
    if (cached?.until > Date.now()) return cached.check;
    if (accessCache.size >= 1000) accessCache.delete(accessCache.keys().next().value);
    const check = findSubscriptions(user)
      .then(items => items.some(sub => hasAudioMembership(sub, priceId)))
      .then(active => {
        if (!active && accessCache.get(id)?.check === check) accessCache.delete(id);
        return active;
      })
      .catch(error => {
        if (accessCache.get(id)?.check === check) accessCache.delete(id);
        throw error;
      });
    accessCache.set(id, { check, until: Date.now() + 30_000 });
    return check;
  }

  async function member(req, res) {
    const user = await account(req);
    if (!user) { reply(res, 401, 'Ingresa con tu usuario y contraseña para continuar.'); return null; }
    if (!await isMember(user)) { reply(res, 403, 'El libro completo está disponible con la membresía de audiolibros.'); return null; }
    return user;
  }

  async function bookFor(req, res) {
    const book = byId.get(req.params.id);
    const file = book && await findReadyAudiobook(book, media, r2);
    if (!file) { reply(res, 404, 'Este audiolibro todavía no está disponible.'); return null; }
    return { book, file };
  }

  function publicBook(book) {
    const { audio, chapters, ...info } = book;
    return { ...info, previewDuration: previewSeconds(book),
      previewUrl: sampleIds.has(book.id) ? `/audiolibros/muestras/${book.id}.m4a` : null,
      previewTranscriptUrl: sampleIds.has(book.id) && getTranscript(book.id) ? `${api}/libros/${book.id}/muestra/texto` : null };
  }

  if (shares) registerAudiobookSharing(app, {
    shares, publicOrigin: origin, media, getBucket, r2, catalog, getTranscript,
    authorize: async (req, res, book, fragment) => {
      if (sampleIds.has(book.id) && fragment.end <= previewSeconds(book)) return true;
      return Boolean(await member(req, res));
    }
  });

  function throttleMail(ip, now) {
    const period = 15 * 60_000;
    for (const [key, entry] of mailAttempts) if (entry.until <= now) mailAttempts.delete(key);
    const entry = mailAttempts.get(ip);
    if (entry) return ++entry.count > 8;
    if (mailAttempts.size >= 5000) return true;
    mailAttempts.set(ip, { count: 1, until: now + period });
    return false;
  }

  route('post', '/pruebas/entrar', async (req, res) => {
    const now = Date.now();
    for (const [ip, slot] of passwordAttempts) if (slot.until <= now) passwordAttempts.delete(ip);
    let slot = passwordAttempts.get(req.ip);
    if (!slot) {
      if (passwordAttempts.size >= 5000) return reply(res, 429, 'Espera unos minutos antes de intentarlo de nuevo.');
      slot = { count: 0, until: now + 15 * 60_000 };
      passwordAttempts.set(req.ip, slot);
    }
    if (++slot.count > 8) return reply(res, 429, 'Espera unos minutos antes de intentarlo de nuevo.');
    const username = normalizeTestUsername(req.body?.username);
    const password = req.body?.password;
    if (!validTestUsername(username) || typeof password !== 'string' || password.length < 12 || password.length > 128) {
      return reply(res, 401, 'Usuario o contraseña incorrectos.');
    }
    const user = await accounts.findById('test:' + username).lean();
    if (!await verifyTestPassword(password, user)) return reply(res, 401, 'Usuario o contraseña incorrectos.');
    const fresh = token();
    await sessions.create({ _id: hash(fresh), email: user._id, credentialVersion: user.credentialVersion,
      expiresAt: new Date(Date.now() + sessionMs) });
    const old = sessionId(req);
    if (old) await sessions.deleteOne({ _id: old });
    // Non-identifying opt-out marker. It never grants access; the server checks the account and session.
    res.cookie('tec_audio_test', '1', { secure, sameSite: 'lax', path: '/audiolibros', maxAge: sessionMs });
    res.cookie(cookie, fresh, { ...cookieOptions, maxAge: sessionMs }).json({ authenticated: true, testAccount: true });
  });

  async function openSession(req, res, user) {
    const old = sessionId(req);
    if (old) await sessions.deleteOne({ _id: old });
    const fresh = token();
    await sessions.create({ _id: hash(fresh), email: user._id, credentialVersion: user.credentialVersion,
      expiresAt: new Date(Date.now() + sessionMs) });
    res.clearCookie('tec_audio_test', { secure, sameSite: 'lax', path: '/audiolibros' });
    res.cookie(cookie, fresh, { ...cookieOptions, maxAge: sessionMs });
  }
  route('post', '/entrar', async (req, res) => {
    const identifier = typeof req.body?.identifier === 'string' ? req.body.identifier.trim().toLowerCase().slice(0, 254) : '';
    if (throttleMail('login:' + req.ip, Date.now()) || throttleMail('login-user:' + hash(identifier), Date.now())) return reply(res, 429, 'Espera unos minutos antes de intentarlo de nuevo.');
    try {
      const user = await paidAuth.login(req.body?.identifier, req.body?.password);
      await openSession(req, res, user);
      res.json({ authenticated: true });
    } catch (error) { if (error.status) return reply(res, error.status, error.message); throw error; }
  });
  // Old email links cannot create accounts or bypass the password flow.
  route('post', '/confirmar', async (_req, res) => reply(res, 410, 'Usa Ingresar o Recuperar acceso para continuar.'));
  route('post', '/recuperar', async (req, res) => {
    if (throttleMail('recovery:' + req.ip, Date.now())) return reply(res, 429, 'Espera unos minutos antes de solicitar otro enlace.');
    const email = normalizeAudioEmail(req.body?.email);
    if (!validAudioEmail(email)) return reply(res, 400, 'Escribe un correo válido.');
    // Delivery continues independently of the fixed public response time: a slow
    // mailbox or Stripe lookup must not reveal whether this email has an account.
    void paidAuth.requestRecovery(email).catch(() => { console.warn('TEC audio: recovery delivery could not be completed'); });
    await new Promise(resolve => setTimeout(resolve, 700));
    res.json({ sent: true });
  });
  route('post', '/credenciales/estado', async (req, res) => {
    if (throttleMail('grant:' + req.ip, Date.now())) return reply(res, 429, 'Espera unos minutos antes de intentarlo de nuevo.');
    try { res.json(await paidAuth.grantInfo(req.body?.token)); }
    catch (error) { if (error.status) return reply(res, error.status, error.message); throw error; }
  });
  route('post', '/credenciales', async (req, res) => {
    if (throttleMail('credentials:' + req.ip, Date.now())) return reply(res, 429, 'Espera unos minutos antes de intentarlo de nuevo.');
    try {
      const result = await paidAuth.completeCredentials(req.body);
      if (result.purpose === 'activation') await openSession(req, res, result.user);
      res.json({ ok: true, purpose: result.purpose, username: result.user.username, authenticated: result.purpose === 'activation' });
    } catch (error) { if (error.status) return reply(res, error.status, error.message); throw error; }
  });
  route('post', '/alta/estado', async (req, res) => {
    if (throttleMail('checkout-status:' + req.ip, Date.now())) return reply(res, 429, 'Espera unos minutos antes de intentarlo de nuevo.');
    try {
      const paid = await paidAuth.fulfillCheckout(req.body?.sessionId);
      res.json({ paid });
    } catch (error) {
      if (error.status) return reply(res, error.status, error.message);
      if (error.type === 'StripeInvalidRequestError') return reply(res, 400, 'No pudimos confirmar ese pago. Revisa tu correo o contacta a TEC.');
      throw error;
    }
  });

  route('post', '/salir', async (req, res) => {
    const id = sessionId(req);
    if (id) await sessions.deleteOne({ _id: id });
    res.clearCookie('tec_audio_test', { secure, sameSite: 'lax', path: '/audiolibros' });
    res.clearCookie(cookie, cookieOptions).json({ ok: true });
  });

  if (achievements) registerAudioAchievements({ route, account, isMember, model: achievements, catalog });
  registerReadingApi({ route, authorize: member, progressModel: readingProgress });

  route('get', '/cuenta', async (req, res) => {
    const user = await account(req);
    res.json({ authenticated: Boolean(user), testAccount: Boolean(user?.isTest), analyticsExcluded: Boolean(user?.analyticsExcluded), displayName: user?.isTest ? user.displayName : null, achievementKey: user ? hash('tec-audio-achievements:' + user._id) : null, member: await isMember(user), billingReady, signInReady: Boolean(sendAccountMail), customer: Boolean(user?.stripeCustomerId) });
  });

  route('post', '/checkout', async (req, res) => {
    let user = await account(req);
    if (user?.isTest) return reply(res, 403, 'Esta cuenta de prueba no utiliza cobros ni suscripciones.');
    if (!billingReady) return reply(res, 503, 'Las suscripciones se están preparando.');
    if (throttleMail('checkout:' + req.ip, Date.now())) return reply(res, 429, 'Espera unos minutos antes de intentarlo de nuevo.');
    if (req.body?.ownershipToken) {
      try { user = await paidAuth.checkoutOwner(req.body.ownershipToken); }
      catch (error) { if (error.status) return reply(res, error.status, error.message); throw error; }
    }
    const email = user ? user._id : normalizeAudioEmail(req.body?.email);
    if (!validAudioEmail(email)) return reply(res, 400, 'Escribe el correo que usarás para tu membresía.');
    if (!user) {
      const existing = await accounts.findById(email).lean();
      if (existing?.stripeCustomerId || existing?.passwordScheme) return reply(res, 409, 'Usa Ingresar o Recuperar acceso para continuar con ese correo.');
    }
    const [price, portal] = await Promise.all([
      stripe.prices.retrieve(priceId), stripe.billingPortal.configurations.retrieve(portalId)
    ]);
    if (!isAudioMembershipPrice(price) || !portal.active || !portal.features?.subscription_cancel?.enabled || portal.features?.subscription_update?.enabled) {
      return reply(res, 503, 'Las suscripciones se están preparando.');
    }
    const intentKey = hash(email);
    let customer = user?.stripeCustomerId;
    if (!customer) {
      const intent = await checkoutIntents.findById(intentKey).lean();
      customer = intent?.stripeCustomerId;
      if (!customer) {
        const created = await stripe.customers.create({ email, metadata: { audience } },
          { idempotencyKey: audience + '-guest-' + intentKey + '-' + new Date().toISOString().slice(0, 7) });
        customer = created.id;
        await checkoutIntents.updateOne({ _id: intentKey }, { $set: { stripeCustomerId: customer,
          expiresAt: new Date(Date.now() + 30 * 86400000) } }, { upsert: true });
      }
    }
    // A pending checkout is not a TEC account and never grants full-library access.
    if (paymentAnalytics) {
      try { await paymentAnalytics.saveContext({ stripeCustomerId: customer }, req.body?.analytics); } catch {}
    }
    const previous = await stripe.checkout.sessions.list({ customer, limit: 1 });
    const subscriptions = await findSubscriptions({ stripeCustomerId: customer });
    if (subscriptions.some(sub => !['canceled', 'incomplete_expired'].includes(sub.status))) {
      return reply(res, 409, 'Ya existe una suscripción. Usa Ingresar o Recuperar acceso para continuar.');
    }
    const last = previous.data[0];
    if (last?.status === 'open' && last.url && last.metadata?.account_flow === AUDIO_ACCOUNT_FLOW) {
      if (req.body?.ownershipToken) await paidAuth.consumeCheckoutOwner(req.body.ownershipToken);
      return res.json({ url: last.url });
    }
    const checkout = await stripe.checkout.sessions.create({
      customer, mode: 'subscription', payment_method_types: ['card'],
      metadata: { audience, account_flow: AUDIO_ACCOUNT_FLOW },
      line_items: [{ price: priceId, quantity: 1 }],
      subscription_data: { metadata: { audience } },
      success_url: landing + '?membresia=confirmada&sesion={CHECKOUT_SESSION_ID}', cancel_url: landing + '#membresia',
      locale: 'es'
    }, { idempotencyKey: audience + '-paid-account-checkout-' + customer + '-' + (last?.id || 'first') });
    if (req.body?.ownershipToken) await paidAuth.consumeCheckoutOwner(req.body.ownershipToken);
    res.json({ url: checkout.url });
  });

  route('post', '/estadisticas/contexto', async (req, res) => {
    const user = await account(req);
    if (paymentAnalytics && user) await paymentAnalytics.saveContext(user, req.body?.analytics);
    res.json({ ok: true });
  });
  route('delete', '/estadisticas/consentimiento', async (req, res) => {
    const user = await account(req);
    if (paymentAnalytics && user) await paymentAnalytics.revoke(user);
    res.json({ ok: true });
  });

  route('post', '/portal', async (req, res) => {
    const user = await account(req);
    if (user?.isTest) return reply(res, 403, 'Esta cuenta de prueba no utiliza cobros ni suscripciones.');
    if (!billingReady || !user?.stripeCustomerId) return reply(res, 404, 'Todavía no hay una suscripción para gestionar.');
    const portal = await stripe.billingPortal.sessions.create({ customer: user.stripeCustomerId, configuration: portalId, return_url: landing });
    res.json({ url: portal.url });
  });

  route('get', '/catalogo', async (_req, res) => {
    const books = await Promise.all(catalog.map(async book => book.audio.r2Key && !await r2?.ready(book.audio) ? null : publicBook(book)));
    res.json({ books: books.filter(Boolean) });
  });

  route('get', '/libros/:id', async (req, res) => {
    const user = await member(req, res);
    if (!user) return;
    const found = await bookFor(req, res);
    if (!found) return;
    const { book } = found;
    const saved = await listening.findById(`${book.id}:${user._id}`).lean();
    const base = `${api}/libros/${book.id}`;
    res.json({ book: { ...publicBook(book), chapters: book.chapters, audioUrl: `${base}/audio`,
      transcriptUrl: getTranscript(book.id) ? `${base}/texto` : null },
    progress: { position: saved?.position || 0, speed: saved?.speed || 1, marks: saved?.marks || [] } });
  });

  route('get', '/libros/:id/muestra/texto', async (req, res) => {
    const book = byId.get(req.params.id);
    const text = book && sampleIds.has(book.id) && getTranscript(book.id);
    if (!text) return reply(res, 404, 'El texto de esta muestra no está disponible.');
    const limit = previewSeconds(book);
    res.json({ cues: text.cues.filter(cue => cue.start < limit && cue.end <= limit), trailingSeparator: '' });
  });

  route('get', '/libros/:id/audio', async (req, res) => {
    if (!await member(req, res)) return;
    const found = await bookFor(req, res);
    if (found) await streamAudiobook(req, res, getBucket, found.book, found.file, r2);
  });

  route('get', '/libros/:id/texto', async (req, res) => {
    if (!await member(req, res)) return;
    const book = byId.get(req.params.id);
    const text = book && getTranscript(book.id);
    if (!text) return reply(res, 404, 'El texto no está disponible.');
    res.json({ cues: text.cues, trailingSeparator: text.trailingSeparator });
  });

  route('put', '/libros/:id/progreso', async (req, res) => {
    const user = await member(req, res);
    if (!user) return;
    const book = byId.get(req.params.id);
    if (!book) return reply(res, 404, 'No encontré ese audiolibro.');
    const { position, speed } = req.body || {};
    if (!Number.isFinite(position) || position < 0 || position > book.duration + 1 ||
      !Number.isFinite(speed) || speed < 0.5 || speed > 2) return reply(res, 400, 'Posición o velocidad inválida.');
    const key = { _id: `${book.id}:${user._id}` };
    const update = { $set: { position: Math.min(position, book.duration), speed } };
    try { await listening.updateOne(key, update, { upsert: true }); }
    catch (error) { if (error.code !== 11000) throw error; await listening.updateOne(key, update); }
    res.json({ saved: true });
  });

  route('put', '/libros/:id/marcadores', async (req, res) => {
    const user = await member(req, res);
    if (!user) return;
    const book = byId.get(req.params.id);
    if (!book) return reply(res, 404, 'No encontré ese audiolibro.');
    const { marks } = req.body || {};
    if (!Array.isArray(marks) || marks.length > 50 || !marks.every(mark => Number.isInteger(mark) && mark >= 0 && mark <= book.duration)) {
      return reply(res, 400, 'Puedes guardar hasta 50 marcadores válidos por libro.');
    }
    const key = { _id: `${book.id}:${user._id}` };
    const update = { $set: { marks: [...new Set(marks)].sort((a, b) => a - b) } };
    try { await listening.updateOne(key, update, { upsert: true }); }
    catch (error) { if (error.code !== 11000) throw error; await listening.updateOne(key, update); }
    res.json({ marks: update.$set.marks });
  });
  return { handleStripeEvent: paidAuth.handleStripeEvent };
}
