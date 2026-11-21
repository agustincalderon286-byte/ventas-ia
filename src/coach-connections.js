import {scopeCoachPending} from './coach-chat-pending.js';
import { createCoachCalendarAgent } from './coach-calendar-agent.js';
import { createCoachWaAgent } from './coach-wa-chat.js';
import { createWaProfile } from './coach-wa-profile.js';
import { createWaCloud, waCloudConfig, waCloudSeller } from './coach-wa-cloud.js';
import { createWaCloudTemplate } from './coach-wa-cloud-template.js';
import { createCoachCalendarConnections } from './coach-calendar-connections.js';
import { createCoachMailAgent } from './coach-mail-agent.js';
import { createMailDrafts } from './coach-mail-drafts.js';
import { createMailOffers } from './coach-mail-offers.js';
import { createCoachOfferAgent, createOfferHooks } from './coach-offer-agent.js';
import { createFollowupStore, purgeCalendarProposals } from './coach-followup-store.js';
import { createHash, randomBytes } from 'node:crypto';
import mongoose from 'mongoose';
import { connectionsPage, notices } from './coach-connections-page.js';
import { gmailClient, openToken, sealToken, gmailScope, gmailSendScope } from './coach-hycite-gmail.js';

const page = '/mis-conexiones/';
const api = '/api/coach/conexiones/gmail/';
const waProfileApi = '/api/coach/conexiones/whatsapp/';
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
  return { key, origin, pilot, domains, clientId, secret, calendarReady: Boolean(clientId && secret && key.length === 32), ready: Boolean(clientId && secret && key.length === 32 && domains.length) };
}

export function registerCoachConnections(app, { requireActive, switched, config = connectionConfig(process.env), store = Store, gmail = gmailClient(config), drafts = createMailDrafts({key:config.key}),
  calendarStore, calendarGoogle, findLeads, syncLead, followups, waDrafts, waProfile = createWaProfile({ key: config.key }), waCloud = createWaCloud(waCloudConfig(process.env)),
  // Sin consulta del webhook auténtico la ventana de 24 h se trata como cerrada (no hay texto libre).
  waWindowOpen = null, waTemplates = createWaCloudTemplate(waCloud?.config || {}),
  // Gmail time offers need the real Mongo stores unless a test injects its own.
  offers }) {
  const followupStore = followups || createFollowupStore({key:config.key});
  const offerStore = offers !== undefined ? offers : store === Store ? createMailOffers({key:config.key,
    rememberEvent:(b,receipt) => followupStore.rememberEvent(b,receipt)}) : null;
  const calendar = createCoachCalendarConnections({config,store:calendarStore,google:calendarGoogle,
    ...(offerStore && { purge: async (args,key) => {
      await purgeCalendarProposals(args,key);
      await offerStore.purge({owner:args.owner,calendarGrant:args.grant}).catch(() => {});
    } })});
  const back = (res, result) => res.redirect(303, page + '?estado=' + result);
  const own = handler => async (req, res) => {
    res.set({ 'Cache-Control': 'private, no-store', 'Referrer-Policy': 'no-referrer', 'X-Frame-Options': 'DENY' }).vary('Cookie');
    try {
      const auth = await requireActive(req, res);
      if (!auth) return;
      if (switched(req) || !auth.session?.tokenHash) return res.status(403).send('Abre Mis conexiones desde tu propia sesión.');
      const id = auth.user._id;
      const session = digest('coach-connections:' + auth.session.tokenHash);
      const email = String(auth.user.email || '').trim().toLowerCase();
      const pilot = config.pilot.includes('*') || config.pilot.includes(email);
      if (req.path !== page && req.path !== '/mis-conexiones' && !pilot) return res.status(403).send('Esta cuenta aún no participa en el piloto.');
      if (req.method === 'POST' && req.body?.csrf !== session) return res.status(403).send('La sesión cambió. Vuelve a abrir Mis conexiones.');
      if (req.method === 'POST' && req.path.startsWith(waProfileApi)) {
        const site = req.get('Sec-Fetch-Site');
        const fromPanel = site ? site === 'same-origin' : req.get('Origin') === req.protocol + '://' + req.get('host');
        if (!fromPanel) return res.status(403).send('Abre el formulario desde Mis conexiones.');
      }
      await handler(req, res, { id, session, pilot, email });
    } catch (error) {
      if (req.path.startsWith(waProfileApi)) return back(res, 'waProfileFailed');
      if (req.path.startsWith('/api/coach/conexiones/calendar/') ||
          (req.path === api + 'callback' && typeof req.query.state === 'string' && req.query.state.startsWith('cal.'))) {
        return back(res, error.reauthorize ? 'calendarReconnect' : 'calendarFailed');
      }
      if (req.path.startsWith(api)) return back(res, error.reauthorize ? 'reconnect' : 'failed');
      res.status(503).send('Mis conexiones no está disponible en este momento.');
    }
  };

  calendar.register(app, own, back);
  app.post(waProfileApi + 'save', own(async (req,res,{id}) => {
    if (config.key?.length !== 32) return back(res, 'waProfileFailed');
    const saved = await waProfile.save(id, req.body.phone);
    return back(res, saved ? 'waProfileSaved' : 'waPhoneInvalid');
  }));
  app.post(waProfileApi + 'remove', own(async (_req,res,{id}) => {
    await waProfile.remove(id);
    return back(res, 'waProfileRemoved');
  }));
  app.get(page, own(async (req, res, { id, session, pilot, email }) => {
    const personalWhatsAppReady = config.key?.length === 32;
    // Solo el email exacto del piloto ve la línea Business, y solo con el alta completa (igual que el chat);
    // mientras tanto conserva el flujo manual. El estado lo confirma Graph en cada carga.
    const businessSeller = waCloudSeller(waCloud?.config, { email });
    const businessPending = !businessSeller && Boolean(waCloud?.config?.email) && email === waCloud.config.email;
    const [doc, cal, senderPhone, businessLine] = await Promise.all([
      pilot ? store.findById(id, 'mailbox sendEnabled snapshot syncedAt').lean() : null, pilot ? calendar.panel(id) : null,
      pilot && personalWhatsAppReady && !businessSeller ? waProfile.load(id).catch(() => null) : null,
      businessSeller ? (personalWhatsAppReady ? waCloud.verify().catch(() => ({ ok:false, reason:'unreachable' }))
        : { ok:false, reason:'notConfigured' }) : null
    ]);
    const message = Object.hasOwn(notices, req.query.estado) ? notices[req.query.estado] : '';
    // Keep the origin on same-site form POSTs for the existing Coach request guard.
    res.set('Referrer-Policy', 'same-origin');
    // Chromium applies form-action to the redirect from our POST to Google as well.
    res.set('Content-Security-Policy', "default-src 'none'; style-src 'self'; base-uri 'none'; frame-ancestors 'none'; form-action 'self' https://accounts.google.com");
    res.send(connectionsPage({ doc, cal, pilot, ready: config.ready, calendarReady:config.calendarReady,
      personalWhatsAppReady, senderPhone, businessLine, businessPending,
      businessTemplate: Boolean(businessSeller && waTemplates?.settings?.ready), coachEmail:email, csrf:session, message }));
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

  app.get(api + 'callback', own(async (req, res, identity) => {
    const {id,session} = identity;
    if (typeof req.query.state === 'string' && req.query.state.startsWith('cal.')) {
      return calendar.callback(req,res,identity,back);
    }
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
    if (doc?.grant && offerStore) await offerStore.purge({owner:id, gmailGrant:doc.grant}).catch(() => {});
    if (doc?.encryptedToken && config.calendarReady) return back(res, 'gmailDisconnectedLocal');
    if (doc?.encryptedToken) {
      try {
        if (!await gmail.revoke(openToken(doc.encryptedToken, config.key, id))) return back(res, 'localOnly');
      } catch { return back(res, 'localOnly'); }
    }
    back(res, 'disconnected');
  }));
  const runCalendar = createCoachCalendarAgent({config,calendar:calendar.service,switched,findLeads,syncLead,followups:followupStore});
  const offerHooks = offerStore ? createOfferHooks({config,calendar:calendar.service,gmail,offers:offerStore}) : null;
  const runMail = createCoachMailAgent({config,store,gmail,drafts,syncSales,switched,offerHooks});
  // Runs before Calendar and Gmail: it claims only offer requests and IDs tagged as offers.
  const runOffer = offerStore ? createCoachOfferAgent({config,store,gmail,drafts,calendar:calendar.service,offers:offerStore,findLeads,syncLead,switched})
    : async () => null;
  const runWhatsApp = createCoachWaAgent({config,switched,findLeads,waDrafts,cloud:waCloud,
    isWindowOpen:waWindowOpen,templates:waTemplates,getSenderPhone:owner=>waProfile.load(owner)});
  return async args => {
    if (args.req?.body) args.req.body = scopeCoachPending(args.req.body,args.text);
    return (await runWhatsApp(args)) || (await runOffer(args)) || (await runCalendar(args)) || runMail(args);
  };
}
