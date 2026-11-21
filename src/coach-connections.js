import { createCoachMailAgent } from './coach-mail-agent.js';
import { createMailDrafts } from './coach-mail-drafts.js';
import { createHash, randomBytes } from 'node:crypto';
import mongoose from 'mongoose';
import { connectionsPage, notices } from './coach-connections-page.js';
import { gmailClient, openToken, sealToken, gmailScope, gmailSendScope } from './coach-hycite-gmail.js';

const page = '/mis-conexiones/';
const api = '/api/coach/conexiones/gmail/';
const random = () => randomBytes(32).toString('base64url');
const digest = data => createHash('sha256').update(data).digest('base64url');
const schema = new mongoose.Schema({
  _id: mongoose.Schema.Types.ObjectId, mailbox: String, encryptedToken: String,
  grant: String, stateHash: String, sessionId: String, deadline: Date, verifier: String,
  sendEnabled: Boolean, requestSend: Boolean, exchangeId: String,
  syncId: String, snapshot: mongoose.Schema.Types.Mixed, syncedAt: Date
}, { versionKey: false });
const Store = mongoose.models.CoachPersonalGmail || mongoose.model('CoachPersonalGmail', schema);

export function connectionConfig(env) {
  const key = Buffer.from(env.COACH_GMAIL_ENCRYPTION_KEY || '', 'base64');
  const origin = new URL(env.APP_BASE_URL || 'https://totalelitecoaching.com').origin;
  const pilot = (env.COACH_CONNECTIONS_PILOT_EMAILS ?? 'calderonrigoberto51@gmail.com').toLowerCase().split(',').map(v => v.trim()).filter(Boolean);
  const domains = (env.COACH_HYCITE_DOMAINS || 'hycite.com').toLowerCase().split(',').map(v => v.trim()).filter(v => /^(?:[a-z\d-]+\.)+[a-z]{2,}$/.test(v));
  const clientId = env.COACH_GMAIL_CLIENT_ID, secret = env.COACH_GMAIL_CLIENT_SECRET;
  return { key, origin, pilot, domains, clientId, secret, ready: Boolean(clientId && secret && key.length === 32 && domains.length) };
}

export function registerCoachConnections(app, { requireActive, switched, config = connectionConfig(process.env), store = Store, gmail = gmailClient(config), drafts = createMailDrafts({key:config.key}) }) {
  const back = (res, result) => res.redirect(303, page + '?estado=' + result);
  const own = handler => async (req, res) => {
    res.set({ 'Cache-Control': 'private, no-store', 'Referrer-Policy': 'no-referrer', 'X-Frame-Options': 'DENY' }).vary('Cookie');
    try {
      const auth = await requireActive(req, res);
      if (!auth) return;
      if (switched(req) || !auth.session?.tokenHash) return res.status(403).send('Abre Mis conexiones desde tu propia sesión.');
      const id = auth.user._id;
      const session = digest('coach-connections:' + auth.session.tokenHash);
      const pilot = config.pilot.includes('*') || config.pilot.includes(String(auth.user.email || '').toLowerCase());
      if (req.path !== page && req.path !== '/mis-conexiones' && !pilot) return res.status(403).send('Esta cuenta aún no participa en el piloto.');
      if (req.method === 'POST' && req.body?.csrf !== session) return res.status(403).send('La sesión cambió. Vuelve a abrir Mis conexiones.');
      await handler(req, res, { id, session, pilot });
    } catch (error) {
      if (req.path.startsWith(api)) return back(res, error.reauthorize ? 'reconnect' : 'failed');
      res.status(503).send('Mis conexiones no está disponible en este momento.');
    }
  };

  app.get(page, own(async (req, res, { id, session, pilot }) => {
    const doc = pilot ? await store.findById(id, 'mailbox sendEnabled snapshot syncedAt').lean() : null;
    const message = Object.hasOwn(notices, req.query.estado) ? notices[req.query.estado] : '';
    // Keep the origin on same-site form POSTs for the existing Coach request guard.
    res.set('Referrer-Policy', 'same-origin');
    // Chromium applies form-action to the redirect from our POST to Google as well.
    res.set('Content-Security-Policy', "default-src 'none'; style-src 'self'; base-uri 'none'; frame-ancestors 'none'; form-action 'self' https://accounts.google.com");
    res.send(connectionsPage({ doc, pilot, ready: config.ready, csrf: session, message }));
  }));

  const startAuthorization = requestSend => own(async (_req, res, { id, session }) => {
    if (!config.ready) return back(res, 'failed');
    const state = random(), verifier = random();
    const pending = {
      stateHash: digest(state), verifier, requestSend, sessionId: session, deadline: new Date(Date.now() + 10 * 60000)
    };
    if (requestSend) {
      const connection = await store.findOneAndUpdate({ _id: id, mailbox: { $exists: true } },
        { $set: pending, $unset: { exchangeId: 1 } }, { new: true }).lean();
      if (!connection) return back(res, 'busy');
      return res.redirect(303, gmail.authorize(state, digest(verifier), { send: true, mailbox: connection.mailbox }));
    }
    try {
      await store.updateOne({ _id: id, mailbox: { $exists: false } }, { $set: pending, $unset: { exchangeId: 1 } }, { upsert: true });
    } catch (error) {
      if (error.code === 11000) return back(res, 'busy');
      throw error;
    }
    res.redirect(303, gmail.authorize(state, digest(verifier)));
  });
  app.post(api + 'start', startAuthorization(false));
  app.post(api + 'enable-send', startAuthorization(true));

  app.get(api + 'callback', own(async (req, res, { id, session }) => {
    if (!config.ready || typeof req.query.state !== 'string') return back(res, 'expired');
    const exchangeId = random();
    const pending = await store.findOneAndUpdate({
      _id: id, sessionId: session, stateHash: digest(req.query.state), deadline: { $gt: new Date() }
    }, { $set: { exchangeId }, $unset: { stateHash: 1, verifier: 1, deadline: 1, sessionId: 1, requestSend: 1 } }, { new: false }).lean();
    if (!pending) return back(res, 'expired');
    if (req.query.error || typeof req.query.code !== 'string') return back(res, 'denied');
    const tokens = await gmail.exchange(req.query.code, pending.verifier);
    const scopes = new Set(String(tokens.scope || '').split(/\s+/));
    if (!tokens.refresh_token || !tokens.access_token || !scopes.has(gmailScope) ||
        (pending.requestSend && !scopes.has(gmailSendScope))) return back(res, 'denied');
    const { emailAddress } = await gmail.profile(tokens.access_token);
    if (!emailAddress) return back(res, 'failed');
    if (pending.requestSend && emailAddress.toLowerCase() !== pending.mailbox?.toLowerCase()) return back(res, 'wrongMailbox');
    const result = await store.updateOne({ _id: id, exchangeId }, { $set: {
      mailbox: emailAddress, encryptedToken: sealToken(tokens.refresh_token, config.key, id),
      grant: pending.grant || random(), sendEnabled: Boolean(pending.requestSend)
    }, $unset: { exchangeId: 1 } });
    if (!result.matchedCount) return back(res, 'busy');
    return back(res, pending.requestSend ? 'sendEnabled' : 'connected');
  }));

  async function syncSales(id, grant) {
    const syncId = random();
    const filter = { _id: id, mailbox: { $exists: true } };
    if (grant) filter.grant = grant;
    const doc = await store.findOneAndUpdate(filter, { $set: { syncId } }, { new: true }).lean();
    if (!doc) return null;
    const token = await gmail.refresh(openToken(doc.encryptedToken, config.key, id));
    const snapshot = await gmail.notices(token.access_token);
    const result = await store.updateOne({ _id: id, grant: doc.grant, syncId }, { $set: { snapshot, syncedAt: new Date() } });
    return result.matchedCount ? snapshot : null;
  }
  app.post(api + 'sync', own(async (_req, res, { id }) => {
    if (!config.ready) return back(res, 'failed');
    return back(res, await syncSales(id) ? 'updated' : 'busy');
  }));

  app.post(api + 'disconnect', own(async (_req, res, { id }) => {
    const doc = await store.findOneAndDelete({ _id: id }).lean();
    if (doc?.grant) await drafts.purge({owner:id, grant:doc.grant}).catch(() => {});
    if (doc?.encryptedToken) {
      try {
        if (!await gmail.revoke(openToken(doc.encryptedToken, config.key, id))) return back(res, 'localOnly');
      } catch { return back(res, 'localOnly'); }
    }
    back(res, 'disconnected');
  }));
  return createCoachMailAgent({config, store, gmail, drafts, syncSales, switched});
}
