import crypto from 'node:crypto';
import { existsSync } from 'node:fs';
import { audiobookCatalog, findReadyAudiobook, getAudiobookTranscript, streamAudiobook } from './tec-audiobooks.js';
import { createAudiobookShareModel, registerAudiobookSharing } from './tec-audiobook-sharing.js';

const api = '/api/audiolibros';
const cookie = 'tec_audio_session';
const audience = 'tec-audiolibros';
const sessionMs = 30 * 86400000;
const hash = value => crypto.createHash('sha256').update(value).digest('hex');
const token = () => crypto.randomBytes(32).toString('base64url');
const tokenPattern = /^[A-Za-z0-9_-]{43}$/;
const emailPattern = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
const previewSeconds = book => Math.floor(book.duration * 100) / 1000;

export function createAudioMembershipModels(mongoose) {
  const model = (name, fields) => mongoose.models[name] || mongoose.model(name, new mongoose.Schema(fields));
  const expiresAt = { type: Date, expires: 0 };
  return {
    shares: createAudiobookShareModel(mongoose),
    accounts: model('TecAudioMember', { _id: String, stripeCustomerId: String }),
    grants: model('TecAudioGrant', { _id: String, email: String, digest: { type: String, index: true }, expiresAt, requestedAt: Date }),
    sessions: model('TecAudioSession', { _id: String, email: String, expiresAt }),
    listening: model('TecAudioListening', { _id: String, position: Number, speed: Number, marks: [Number] })
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
  accounts, grants, sessions, listening, shares, media, getBucket, r2, stripe, priceId, portalId,
  publicOrigin, sendSignIn, secure = process.env.NODE_ENV === 'production',
  catalog = audiobookCatalog, getTranscript = getAudiobookTranscript
}) {
  const origin = new URL(publicOrigin).origin;
  const landing = `${origin}/audiolibros/`;
  const byId = new Map(catalog.map(book => [book.id, book]));
  const sampleIds = new Set(catalog.filter(book => existsSync(new URL(`../public/audiolibros/muestras/${book.id}.m4a`, import.meta.url))).map(book => book.id));
  const cookieOptions = { httpOnly: true, secure, sameSite: 'lax', path: api };
  const accessCache = new Map();
  const mailAttempts = new Map();
  const billingReady = Boolean(stripe && priceId && portalId);
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
    return session ? accounts.findById(session.email).lean() : null;
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
    if (!user) { reply(res, 401, 'Ingresa con tu correo para continuar.'); return null; }
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

  route('post', '/entrar', async (req, res) => {
    if (!sendSignIn) return reply(res, 503, 'El acceso por correo se está preparando.');
    const email = typeof req.body?.email === 'string' ? req.body.email.trim().toLowerCase() : '';
    if (email.length > 254 || !emailPattern.test(email)) return reply(res, 400, 'Escribe un correo válido.');
    const now = Date.now();
    if (throttleMail(req.ip, now)) return reply(res, 429, 'Espera unos minutos antes de solicitar otro enlace.');
    const secret = token();
    const digest = hash(secret);
    try {
      await grants.findOneAndUpdate({ _id: hash(email), requestedAt: { $lte: new Date(now - 60_000) } }, {
        $set: { email, digest, requestedAt: new Date(now), expiresAt: new Date(now + 15 * 60_000) }
      }, { upsert: true });
    } catch (error) {
      if (error.code === 11000) return res.json({ sent: true });
      throw error;
    }
    try {
      await sendSignIn(email, `${landing}#entrar=${secret}`);
    } catch (error) {
      await grants.deleteOne({ _id: hash(email), digest });
      throw error;
    }
    res.json({ sent: true });
  });

  route('post', '/confirmar', async (req, res) => {
    const secret = req.body?.token;
    if (typeof secret !== 'string' || !tokenPattern.test(secret)) return reply(res, 400, 'El enlace no es válido.');
    const grant = await grants.findOneAndDelete({ digest: hash(secret), expiresAt: { $gt: new Date() } }).lean();
    if (!grant) return reply(res, 400, 'Este enlace caducó o ya se usó. Solicita otro.');
    try { await accounts.updateOne({ _id: grant.email }, { $setOnInsert: { _id: grant.email } }, { upsert: true }); }
    catch (error) { if (error.code !== 11000) throw error; }
    const oldSession = sessionId(req);
    if (oldSession) await sessions.deleteOne({ _id: oldSession });
    const fresh = token();
    await sessions.create({ _id: hash(fresh), email: grant.email, expiresAt: new Date(Date.now() + sessionMs) });
    res.cookie(cookie, fresh, { ...cookieOptions, maxAge: sessionMs }).json({ authenticated: true });
  });

  route('post', '/salir', async (req, res) => {
    const id = sessionId(req);
    if (id) await sessions.deleteOne({ _id: id });
    res.clearCookie(cookie, cookieOptions).json({ ok: true });
  });

  route('get', '/cuenta', async (req, res) => {
    const user = await account(req);
    res.json({ authenticated: Boolean(user), member: await isMember(user), billingReady, signInReady: Boolean(sendSignIn), customer: Boolean(user?.stripeCustomerId) });
  });

  route('post', '/checkout', async (req, res) => {
    if (!billingReady) return reply(res, 503, 'Las suscripciones se están preparando.');
    const user = await account(req);
    if (!user) return reply(res, 401, 'Ingresa con tu correo para suscribirte.');
    const [price, portal] = await Promise.all([
      stripe.prices.retrieve(priceId), stripe.billingPortal.configurations.retrieve(portalId)
    ]);
    if (!isAudioMembershipPrice(price) || !portal.active || !portal.features?.subscription_cancel?.enabled || portal.features?.subscription_update?.enabled) {
      return reply(res, 503, 'Las suscripciones se están preparando.');
    }
    let customer = user.stripeCustomerId;
    if (!customer) {
      const key = hash(user._id);
      const created = await stripe.customers.create({ email: user._id, metadata: { audience, account: key } }, { idempotencyKey: `${audience}-customer-${key}` });
      await accounts.updateOne({ _id: user._id, stripeCustomerId: { $exists: false } }, { $set: { stripeCustomerId: created.id } });
      customer = (await accounts.findById(user._id).lean()).stripeCustomerId;
    }
    const previous = await stripe.checkout.sessions.list({ customer, limit: 1 });
    const subscriptions = await findSubscriptions({ stripeCustomerId: customer });
    if (subscriptions.some(sub => !['canceled', 'incomplete_expired'].includes(sub.status))) {
      return reply(res, 409, 'Ya tienes una suscripción. Revísala desde Gestionar suscripción.');
    }
    const last = previous.data[0];
    if (last?.status === 'open' && last.url) return res.json({ url: last.url });
    const checkout = await stripe.checkout.sessions.create({
      customer, mode: 'subscription', payment_method_types: ['card'],
      metadata: { audience },
      line_items: [{ price: priceId, quantity: 1 }],
      subscription_data: { metadata: { audience } },
      success_url: `${landing}?membresia=confirmada`, cancel_url: landing,
      locale: 'es'
    }, { idempotencyKey: `${audience}-checkout-${customer}-${last?.id || 'first'}` });
    res.json({ url: checkout.url });
  });

  route('post', '/portal', async (req, res) => {
    const user = await account(req);
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
}
