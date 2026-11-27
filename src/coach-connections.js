import {createCoachContactsAgent} from './coach-contacts-agent.js';
import {createCoachContactsConnections} from './coach-contacts-connections.js';
import {createCoachRouteAgent} from './coach-route-agent.js';
import {createTrafficRoutes} from './coach-route-google.js';
import {createAssistantLog} from './coach-chat-review.js';
import {createPostSaleReader} from './coach-postsale.js';
import {createConversationPlan} from './coach-conversation-plan.js';
import {agentConnectionReader,sellerAgentModel} from './coach-agent-pilot.js';
import {createSellerAgent,loopSellers} from './coach-agent-seller.js';
import {createAgentTools} from './coach-agent-tools.js';
import {createAgentBudget} from './coach-agent-budget.js';
import {createAgentConsent} from './coach-agent-consent.js';
import {readCoachAgenda} from './coach-agenda.js';
import {createPlanStore} from './coach-plan-store.js';
import {createPlanChannels} from './coach-plan-channels.js';
import {createCoachSheetTools} from './coach-sheets-tools.js';
import {createMyDay} from './coach-my-day.js';
import {registerMyDayRoutes} from './coach-my-day-routes.js';
import {coachPendingFields, directDraftInput, scopeCoachPending} from './coach-chat-pending.js';
import { createCoachCalendarAgent } from './coach-calendar-agent.js';
import { createCoachWaAgent } from './coach-wa-chat.js';
import { createWaProfile } from './coach-wa-profile.js';
import { createWaCloud, waCloudConfig, waCloudSeller } from './coach-wa-cloud.js';
import { createWaCloudTemplate } from './coach-wa-cloud-template.js';
import { createCoachSheetsConnections, sheetsApi } from './coach-sheets-connections.js';
import { createSheetsChatAgent, sheetsChatIntent } from './coach-sheets-agent.js';
import { createSheetsGoogle } from './coach-sheets-google.js';
import { registerCoachSheetsRoutes } from './coach-sheets-routes.js';
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
const pendingFields = {...coachPendingFields,sheets:'coachSheetsPending',route:'coachRoutePending',contacts:'coachContactsPending'};
const api = '/api/coach/conexiones/gmail/';
const calendarApi = '/api/coach/conexiones/calendar/';
const waProfileApi = '/api/coach/conexiones/whatsapp/';
const assistantApi = '/api/coach/conexiones/asistente/';
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
  const demoEmail = String(env.COACH_GOOGLE_VERIFICATION_DEMO_EMAIL || '').trim().toLowerCase();
  const demo = /^[^@,\s]+@[^@,\s]+\.[^@,\s]+$/.test(demoEmail) ? [demoEmail] : [];
  const calendarPilot = [...new Set([...pilot, ...demo])];
  const domains = (env.COACH_HYCITE_DOMAINS || 'hycite.com').toLowerCase().split(',').map(v => v.trim()).filter(v => /^(?:[a-z\d-]+\.)+[a-z]{2,}$/.test(v));
  const clientId = env.COACH_GMAIL_CLIENT_ID, secret = env.COACH_GMAIL_CLIENT_SECRET;
  const sheetsPickerKey = env.COACH_SHEETS_PICKER_KEY || '';
  const sheetsAppId = env.COACH_SHEETS_APP_ID || (/^(\d+)-/.exec(clientId || '') || [])[1] || '';
  const sheetsPilot = (env.COACH_SHEETS_PILOT_EMAILS ?? env.COACH_CONNECTIONS_PILOT_EMAILS ??
    'agustincalderon286@gmail.com,calderonrigoberto51@gmail.com').toLowerCase().split(',').map(v=>v.trim()).filter(Boolean);
  const sheetsReady = Boolean(clientId && secret && key.length===32 && sheetsPickerKey && /^\d+$/.test(sheetsAppId));
  const planPilot=(env.COACH_PLAN_PILOT_EMAILS ?? 'agustincalderon286@gmail.com,calderonrigoberto51@gmail.com').toLowerCase().split(',').map(v=>v.trim()).filter(Boolean);
  const contactsPilot = [...new Set([...planPilot, ...demo])];
  const loopMicros=Number(env.COACH_AGENT_LOOP_MONTHLY_MICROS ?? 5_000_000);
  return { key, origin, pilot, calendarPilot, planPilot, contactsPilot, loopPilot:loopSellers(env), loopMonthlyMicros:Number.isSafeInteger(loopMicros) && loopMicros>0 ? loopMicros : 0, agentModel:env.COACH_AGENT_PILOT_MODEL ?? 'gpt-6.2-sol', domains, mapsRoutesKey:env.COACH_MAPS_ROUTES_KEY || '', mapsFreeMonth:env.COACH_MAPS_FREE_MONTH, mapsFreeRequests:env.COACH_MAPS_FREE_REQUESTS, contactsReady:Boolean(clientId && secret && key.length===32), clientId, secret, sheetsPickerKey, sheetsAppId, sheetsPilot:[...new Set([...sheetsPilot, ...demo])], sheetsReady, calendarReady: Boolean(clientId && secret && key.length === 32), ready: Boolean(clientId && secret && key.length === 32 && domains.length) };
}

export function registerCoachConnections(app, { requireActive, switched, config = connectionConfig(process.env), store = Store, gmail = gmailClient(config), drafts = createMailDrafts({key:config.key}),
  calendarStore, calendarGoogle, contactsStore, contactsGoogle, contactsDependencies={}, routeDependencies={}, agentDependencies={}, agentConsent=createAgentConsent(), readSheetsSession, sheetsStore, sheetsGoogle, sheetsReader=createSheetsGoogle(), planDependencies={}, findLeads, syncLead, followups, waDrafts, waProfile = createWaProfile({ key: config.key }), waCloud = createWaCloud(waCloudConfig(process.env)),
  // Sin consulta del webhook auténtico la ventana de 24 h se trata como cerrada (no hay texto libre).
  waWindowOpen = null, waTemplates = createWaCloudTemplate(waCloud?.config || {}),
  // Gmail time offers need the real Mongo stores unless a test injects its own.
  offers,
  // «Mi día»: null lo apaga (pruebas); por defecto usa los modelos de TEC.
  myDayDependencies = {} }) {
  const followupStore = followups || createFollowupStore({key:config.key});
  const offerStore = offers !== undefined ? offers : store === Store ? createMailOffers({key:config.key,
    rememberEvent:(b,receipt) => followupStore.rememberEvent(b,receipt)}) : null;
  const contacts = createCoachContactsConnections({config,store:contactsStore,google:contactsGoogle});
  const sheetTools = createCoachSheetTools({google:sheetsReader});
  const sheets = createCoachSheetsConnections({config,store:sheetsStore,google:sheetsGoogle,reader:sheetsReader});
  const calendar = createCoachCalendarConnections({config,store:calendarStore,google:calendarGoogle,
    ...(offerStore && { purge: async (args,key) => {
      await purgeCalendarProposals(args,key);
      await offerStore.purge({owner:args.owner,calendarGrant:args.grant}).catch(() => {});
    } })});
  const calendarPilots = config.calendarPilot ?? config.pilot;
  const isCalendarRequest = req => req.path.startsWith(calendarApi) ||
    (req.path === api + 'callback' && typeof req.query.state === 'string' && req.query.state.startsWith('cal.'));
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
      const calendarPilot = calendarPilots.includes('*') || calendarPilots.includes(email);
      const calendarRequest = isCalendarRequest(req);
      if (calendarRequest && !calendarPilot) return res.status(403).send('Esta cuenta aún no participa en el piloto de Calendar.');
      const sheetsRequest = req.path.startsWith(sheetsApi) || (req.path===api+'callback' &&
        typeof req.query.state==='string' && req.query.state.startsWith('sheets.'));
      const direct = auth.session.accessMode === 'direct' || (!auth.session.accessMode && auth.user.accountType !== 'seat');
      const sheetsPilot = direct && Boolean(config.sheetsPilot?.some(value => value==='*' || value===email));
      const contactsPilot=direct && Boolean((config.contactsPilot ?? config.planPilot)?.some(value=>value==='*' || value===email));
      const contactsRequest=req.path.startsWith('/api/coach/conexiones/contactos/') || (req.path===api+'callback' && typeof req.query.state==='string' && req.query.state.startsWith('people.'));
      if (contactsRequest && !contactsPilot) return res.status(403).send('Abre Contactos desde tu propia cuenta del piloto.');
      if (sheetsRequest && !sheetsPilot) return res.status(403).send('Abre Google Sheets desde tu propia cuenta del piloto.');
      const assistantPilot = direct && Boolean(config.loopPilot?.some(value => value==='*' || value===email));
      const assistantRequest = req.path.startsWith(assistantApi);
      if (assistantRequest && !assistantPilot) return res.status(403).send('Esta cuenta aún no participa en el piloto del asistente.');
      if (!calendarRequest && !sheetsRequest && !contactsRequest && !assistantRequest && req.path !== page && req.path !== '/mis-conexiones' && !pilot) return res.status(403).send('Esta cuenta aún no participa en el piloto.');
      if (req.method === 'POST' && req.body?.csrf !== session) return res.status(403).send('La sesión cambió. Vuelve a abrir Mis conexiones.');
      if (req.method === 'POST' && (sheetsRequest || contactsRequest || assistantRequest || req.path.startsWith(waProfileApi))) {
        const site = req.get('Sec-Fetch-Site');
        const fromPanel = site ? site === 'same-origin' : req.get('Origin') === req.protocol + '://' + req.get('host');
        if (!fromPanel) return res.status(403).send('Abre el formulario desde Mis conexiones.');
      }
      const isSessionCurrent = async () => {
        if (!readSheetsSession || switched(req)) return false;
        const fresh = await readSheetsSession(req);
        if (!fresh?.session || !fresh.user || String(fresh.user._id)!==String(id) ||
          fresh.session.tokenHash!==auth.session.tokenHash) return false;
        return fresh.session.accessMode==='direct' || (!fresh.session.accessMode && fresh.user.accountType!=='seat');
      };
      await handler(req, res, { id, session, pilot, calendarPilot, sheetsPilot, contactsPilot, assistantPilot, email, sellerName:String(auth.user.name || '').trim(), isSessionCurrent });
    } catch (error) {
      if (req.path.startsWith('/api/coach/conexiones/contactos/') || (req.path===api+'callback' && req.query?.state?.startsWith?.('people.'))) return back(res,error.reauthorize?'contactsReconnect':'contactsFailed');
      if (req.path === sheetsApi+'picker-token') return res.status(error.status || 503).json({error:'Vuelve a abrir Google Sheets en Mis conexiones.'});
      if (req.path.startsWith(sheetsApi) || (req.path===api+'callback' && typeof req.query.state==='string' && req.query.state.startsWith('sheets.'))) {
        console.error('[sheets] falló', req.path, error.status || 500, String(error.message || '').replace(/(["'“«]).*?(["'”»])/g, '…').slice(0, 200));
        return back(res,error.reauthorize ? 'sheetsReconnect' : error.status===422 ? 'sheetsFileUnavailable' : 'sheetsFailed');
      }
      if (req.path.startsWith(waProfileApi)) return back(res, 'waProfileFailed');
      if (req.path.startsWith(assistantApi)) return back(res, 'assistantFailed');
      if (isCalendarRequest(req)) {
        return back(res, error.reauthorize ? 'calendarReconnect' : 'calendarFailed');
      }
      if (req.path.startsWith(api)) return back(res, error.reauthorize ? 'reconnect' : 'failed');
      res.status(503).send('Mis conexiones no está disponible en este momento.');
    }
  };

  calendar.register(app, own, back);
  contacts.register(app, own, back);
  sheets.register(app, own, back);
  registerCoachSheetsRoutes(app,{own,sheets:sheets.service});
  // Mi día: primero para las cuentas del piloto, desde su propia sesión.
  const myDay = myDayDependencies ? createMyDay({config,calendar:calendar.service,sheets:sheets.service,sheetTools,...myDayDependencies}) : null;
  if (myDay) registerMyDayRoutes(app,{requireActive,service:myDay,allowed:auth=>{
    const direct=auth.session?.accessMode==='direct' || (!auth.session?.accessMode && auth.user.accountType!=='seat');
    return direct && Boolean(config.planPilot?.some(email=>email==='*' || email===String(auth.user.email || '').toLowerCase()));
  }});
  app.post(waProfileApi + 'save', own(async (req,res,{id}) => {
    if (config.key?.length !== 32) return back(res, 'waProfileFailed');
    const saved = await waProfile.save(id, req.body.phone);
    return back(res, saved ? 'waProfileSaved' : 'waPhoneInvalid');
  }));
  app.post(waProfileApi + 'remove', own(async (_req,res,{id}) => {
    await waProfile.remove(id);
    return back(res, 'waProfileRemoved');
  }));
  app.post(assistantApi + 'accept', own(async (_req,res,{id}) => {
    await agentConsent.accept(id);
    return back(res, 'assistantOn');
  }));
  app.post(assistantApi + 'revoke', own(async (_req,res,{id}) => {
    await agentConsent.revoke(id);
    return back(res, 'assistantOff');
  }));
  app.get(page, own(async (req, res, { id, session, pilot, calendarPilot, sheetsPilot, contactsPilot, assistantPilot, email }) => {
    const personalWhatsAppReady = config.key?.length === 32;
    // Solo el email exacto del piloto ve la línea Business, y solo con el alta completa (igual que el chat);
    // mientras tanto conserva el flujo manual. El estado lo confirma Graph en cada carga.
    const businessSeller = waCloudSeller(waCloud?.config, { email });
    const businessPending = !businessSeller && Boolean(waCloud?.config?.email) && email === waCloud.config.email;
    const [doc, cal, sheetProfile, senderPhone, businessLine, contactsProfile, assistantAccepted] = await Promise.all([
      pilot ? store.findById(id, 'mailbox sendEnabled snapshot syncedAt').lean() : null, calendarPilot ? calendar.panel(id) : null, sheetsPilot ? sheets.panel(id) : null,
      pilot && personalWhatsAppReady && !businessSeller ? waProfile.load(id).catch(() => null) : null,
      businessSeller ? (personalWhatsAppReady ? waCloud.verify().catch(() => ({ ok:false, reason:'unreachable' }))
        : { ok:false, reason:'notConfigured' }) : null,
      contactsPilot && config.contactsReady ? contacts.panel(id) : null,
      assistantPilot ? agentConsent.state(id) : null
    ]);
    const message = Object.hasOwn(notices, req.query.estado) ? notices[req.query.estado] : '';
    // Keep the origin on same-site form POSTs for the existing Coach request guard.
    res.set('Referrer-Policy', 'same-origin');
    // Chromium applies form-action to the redirect from our POST to Google as well.
    res.set('Content-Security-Policy', "default-src 'none'; style-src 'self'; base-uri 'none'; frame-ancestors 'none'; form-action 'self' https://accounts.google.com");
    res.send(connectionsPage({ doc, cal, pilot, calendarPilot, contacts:contactsProfile, contactsPilot, contactsReady:config.contactsReady, sheets:sheetProfile, sheetsPilot, sheetsReady:config.sheetsReady, ready: config.ready, calendarReady:config.calendarReady,
      personalWhatsAppReady, senderPhone, businessLine, businessPending,
      businessTemplate: Boolean(businessSeller && waTemplates?.settings?.ready), assistant: assistantPilot ? assistantAccepted : null, coachEmail:email, csrf:session, message }));
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
    if (typeof req.query.state === 'string' && req.query.state.startsWith('people.')) return contacts.callback(req,res,identity,back);
    if (typeof req.query.state === 'string' && req.query.state.startsWith('sheets.')) return sheets.callback(req,res,identity,back);
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
  const runSheets = createSheetsChatAgent({config,sheets:sheets.service,google:sheetsReader,switched,readSession:readSheetsSession,calendar:runCalendar});
  const offerHooks = offerStore ? createOfferHooks({config,calendar:calendar.service,gmail,offers:offerStore}) : null;
  const runMail = createCoachMailAgent({config,store,gmail,drafts,syncSales,switched,offerHooks});
  // Runs before Calendar and Gmail: it claims only offer requests and IDs tagged as offers.
  const runOffer = offerStore ? createCoachOfferAgent({config,store,gmail,drafts,calendar:calendar.service,offers:offerStore,findLeads,syncLead,switched})
    : async () => null;
  const runWhatsApp = createCoachWaAgent({config,switched,findLeads,waDrafts,cloud:waCloud,
    isWindowOpen:waWindowOpen,templates:waTemplates,getSenderPhone:owner=>waProfile.load(owner)});
  const runContacts=createCoachContactsAgent({config,contacts:contacts.service,plans:createPlanStore({key:config.key}),
    sheets:sheets.service,sheetTools,switched,readSession:readSheetsSession,...contactsDependencies});
  const runRoute=createCoachRouteAgent({config,calendar:calendar.service,sheets:sheets.service,sheetsGoogle:sheetsReader,
    readSession:readSheetsSession,switched,...routeDependencies});
  // El plan de conversación y el asistente con ciclo preparan las citas con el mismo código.
  const planChannels=createPlanChannels({config,calendar:calendar.service,followups:followupStore,store,gmail,drafts});
  const runPlan=createConversationPlan({config,plans:createPlanStore({key:config.key}),sheets:sheets.service,
    readConnections:agentConnectionReader({config,store,sheets:sheets.service,calendar:calendar.service}),
    readMail:(args,readTool)=>runMail({...args,readTool}),
    readAgenda:(actor,text)=>{
      if (!calendarPilots.some(email=>email==='*' || email===actor.email.toLowerCase())) return 'Tu cuenta todavía no participa en el piloto de Calendar.';
      if (!config.calendarReady) return 'Google Calendar no está disponible en TEC en este momento.';
      return readCoachAgenda({calendar:calendar.service,owner:actor.owner,zone:actor.zone,current:actor.current,text});
    },
    sheetTools,readSales:createPostSaleReader({config,store,gmail}),channels:planChannels,
    readSession:readSheetsSession,switched,...planDependencies});
  // Asistente con ciclo de herramientas. Solo actúa para COACH_AGENT_LOOP_EMAILS; para los demás devuelve null.
  const mailAad=(owner,session,grant)=>'draft-question:'+owner+':'+session+':'+grant;
  const runLoop=createSellerAgent({config,plans:createPlanStore({key:config.key}),memory:createPlanStore({key:config.key}),
    bindTools:createAgentTools({config,calendar:calendar.service,store,gmail,syncSales,sheets:sheets.service,sheetTools,contacts:contacts.service,channels:planChannels,routes:createTrafficRoutes(config),myDay,
      readConnections:agentConnectionReader({config,store,sheets:sheets.service,calendar:calendar.service})}),
    // Mismo formato y vínculo que la ficha de bandeja del agente de Gmail, para que «Responde al primero» continúe allí.
    inboxToken:({owner,session,grant,ids})=>sealToken(JSON.stringify({kind:'inbox',ids,until:Date.now()+30*60000}),config.key,mailAad(owner,session,grant)),
    mailContext:async(owner,session,token)=>{
      const doc=await store.findById(owner,'grant').lean();
      if (!doc?.grant || token.length>40000) return '';
      try {
        const saved=JSON.parse(openToken(token,config.key,mailAad(owner,session,doc.grant)));
        return saved.until>Date.now() ? saved.kind || 'pending' : '';
      } catch { return ''; }
    },
    budget:createAgentBudget({limitMicros:config.loopMonthlyMicros}),consent:agentConsent,reviewLog:createAssistantLog({key:config.key}),readSession:readSheetsSession,switched,...agentDependencies});
  async function dispatch(args) {
    const body = args.req?.body;
    const {user,session}=args.auth || {};
    if (body?.coachPlanVersion===1 && sellerAgentModel(user?.email,config.agentModel)) {
      const directOwner=session?.accessMode==='direct' || (session && !session.accessMode && user.accountType!=='seat');
      if (!directOwner || !session.tokenHash || !args.sessionId || String(user._id)!==body.accountId || switched(args.req)) {
        return {mode:'coach',coachMailAgent:true,respuesta:'Abre tu propia sesión del Coach para consultar o cambiar tus conexiones.'};
      }
    }
    const planFocused = body?.coachPlanVersion === 1 && body.coachPlanPending && body.coachQuestionFocus === 'plan';
    const direct = directDraftInput(body,args.text);
    if (direct && !(direct.literal && planFocused)) {
      const agent = direct.channel === 'mail' ? runMail : runWhatsApp;
      return agent({...args,literalDraftInput:direct.literal});
    }
    if (!direct?.literal && !/^TEC(?:PLAN|PERSON|ROUTE) /.test(args.text) && !/\b[a-f0-9]{12,32}[.!]?\s*$/i.test(args.text)) {
      const looped=await runLoop(args);
      if (looped) return looped;
    }
    if (!direct?.literal && !/^TECPLAN /.test(args.text)) {
      const routed=/^TECPERSON /.test(args.text) ? await runContacts(args) : await runRoute(args) || await runContacts(args);
      if (routed) return routed;
    }
    const focus=body?.coachQuestionFocus;
    const legacyFocus=Object.hasOwn(pendingFields,focus) && body[pendingFields[focus]] ? focus : '';
    const explicitSheetSelection=!body?.coachPlanPending && body?.coachSheetsPending && sheetsChatIntent(args.text)?.kind === 'select';
    if (!explicitSheetSelection) {
      const planAnswer=await runPlan({...args,legacyFocus});
      if (planAnswer) return planAnswer;
      if (direct?.literal && planFocused) {
        return {mode:'coach',coachMailAgent:true,coachPlanAgent:true,coachPlanPending:body.coachPlanPending,
          respuesta:'¿Qué parte de la propuesta quieres corregir? Puedes indicar si es la nota, la cita o el correo.'};
      }
    }
    if (args.req?.body) args.req.body = scopeCoachPending(args.req.body,args.text);
    return (await runSheets(args)) || (await runWhatsApp(args)) || (await runOffer(args)) || (await runCalendar(args)) || runMail(args);
  }
  const agent = async args => {
    const result=await dispatch(args);
    if (!result?.coachMailAgent || args.req?.body?.responseMode !== 'audio') return result;
    const {user,session}=args.auth || {};
    const direct=session?.accessMode==='direct' || (session && !session.accessMode && user?.accountType!=='seat');
    const eligible=user && config.planPilot?.some(email=>email==='*' || email===String(user.email).toLowerCase());
    if (!direct || !eligible || switched(args.req) || String(user._id)!==args.req.body.accountId) return result;
    return {...result,coachPrivateVoice:true};
  };
  // El servidor principal usa myDay para el texto de los avisos al teléfono.
  agent.myDay = myDay;
  return agent;
}
