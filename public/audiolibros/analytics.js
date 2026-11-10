import { MEASUREMENT_ID, CAMPAIGN_KEY, pageContext, safeAudioEvent, safeReferrer, safeCampaign, savedCampaign } from './analytics-events.js?v=20261110-payments';

const KEY = 'tec.audiobooks.analytics.choice.v2';
const disableKey = 'ga-disable-' + MEASUREMENT_ID;
const context = pageContext(location.pathname);
const incomingCampaign = safeCampaign(location.search);
let campaign = {}, signedIn = false, contextSynced = false;
const native = Boolean(window.webkit?.messageHandlers?.tecNative || window.TecNativeOffline || window.TecNative || window.TECNative || window.Capacitor);
const protectedChoice = navigator.globalPrivacyControl === true || navigator.doNotTrack === '1';
const production = ['https://totalelitecoaching.com', 'https://www.totalelitecoaching.com'].includes(location.origin);
// A test entry is excluded even before sign-in; the marker contains no identity or entitlement.
let testAccount = /(?:^|;\s*)tec_audio_test=1(?:;|$)/.test(document.cookie || '') ||
  new URLSearchParams(location.search || '').get('acceso') === 'prueba';
let choice = '', loading = false, loaded = false, pageSent = false;
const samplesSent = new Set();
try { choice = localStorage.getItem(KEY) || ''; } catch {}
const allowed = () => context && production && !testAccount && !native && !protectedChoice && choice === 'accepted' && navigator.onLine !== false;
function store(value) { choice = value; try { localStorage.setItem(KEY, value); } catch {} }
function command() { window.dataLayer.push(arguments); }
function resolveCampaign() {
  let previous = {};
  try { previous = savedCampaign(localStorage.getItem(CAMPAIGN_KEY)); } catch {}
  let newExternalVisit = false;
  try {
    const referrer = new URL(document.referrer);
    newExternalVisit = !['totalelitecoaching.com', 'www.totalelitecoaching.com', 'checkout.stripe.com',
      'billing.stripe.com', 'invoice.stripe.com'].includes(referrer.hostname);
  } catch {}
  const hasNewCampaign = ['utm_source', 'utm_medium', 'utm_campaign'].some(key => new URLSearchParams(location.search).has(key));
  if (!incomingCampaign.campaign_name && (newExternalVisit || hasNewCampaign)) {
    previous = {};
    try { localStorage.removeItem(CAMPAIGN_KEY); } catch {}
  }
  campaign = incomingCampaign.campaign_name ? incomingCampaign : previous;
  if (incomingCampaign.campaign_name) {
    try { localStorage.setItem(CAMPAIGN_KEY, JSON.stringify({ at: Date.now(), campaign })); } catch {}
  }
}
async function measurementContext() {
  if (!allowed() || !loaded) return null;
  const get = field => new Promise(resolve => {
    const timer = setTimeout(() => resolve(null), 1000);
    command('get', MEASUREMENT_ID, field, value => { clearTimeout(timer); resolve(value); });
  });
  const [clientId, sessionId] = await Promise.all([get('client_id'), get('session_id')]);
  if (!allowed() || !/^\d{1,20}\.\d{1,20}$/.test(clientId || '') || !/^\d{1,16}$/.test(String(sessionId || ''))) return null;
  return { consentVersion: 2, clientId, sessionId: String(sessionId), campaign };
}
async function syncContext() {
  if (!signedIn || contextSynced || !allowed() || !loaded) return;
  contextSynced = true;
  const analytics = await measurementContext();
  if (!analytics || !allowed()) { contextSynced = false; return; }
  try {
    const response = await fetch('/api/audiolibros/estadisticas/contexto', { method: 'POST', credentials: 'same-origin',
      headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ analytics }) });
    if (!response.ok) contextSynced = false;
  } catch { contextSynced = false; }
}
function revokeServerConsent() {
  if (native || !production || navigator.onLine === false) return;
  try { void fetch('/api/audiolibros/estadisticas/consentimiento', { method: 'DELETE',
    credentials: 'same-origin' }).catch(() => {}); } catch {}
}
function emit(name, bookId) {
  if (!loaded || !allowed()) return false;
  const params = safeAudioEvent(name, context, bookId);
  if (!params) return false;
  command('event', name, { ...params, ...campaign, send_to: MEASUREMENT_ID, page_referrer: safeReferrer(document.referrer) });
  return true;
}
function pageView() { if (!pageSent && emit('page_view')) pageSent = true; }
function start() {
  if (!allowed() || loading) return;
  resolveCampaign();
  window[disableKey] = false;
  if (loaded) { pageView(); void syncContext(); return; }
  loading = true;
  window.dataLayer = window.dataLayer || [];
  command('consent', 'default', { analytics_storage: 'granted', ad_storage: 'denied', ad_user_data: 'denied', ad_personalization: 'denied' });
  const metadata = { ...campaign, page_location: 'https://totalelitecoaching.com' + context.path, page_title: 'Audiolibros TEC · ' + context.type, page_referrer: safeReferrer(document.referrer), allow_google_signals: false, allow_ad_personalization_signals: false };
  command('set', { ...metadata, ads_data_redaction: true, url_passthrough: false });
  command('js', new Date());
  command('config', MEASUREMENT_ID, { ...metadata, send_page_view: false, cookie_prefix: 'tec_audio', cookie_path: '/audiolibros', cookie_expires: 2592000, cookie_update: false, cookie_flags: 'SameSite=Lax;Secure' });
  const script = document.createElement('script');
  script.async = true; script.referrerPolicy = 'no-referrer';
  script.src = 'https://www.googletagmanager.com/gtag/js?id=' + MEASUREMENT_ID;
  script.onload = () => { loading = false; loaded = true; window[disableKey] = !allowed(); pageView(); void syncContext(); };
  script.onerror = () => { loading = false; window[disableKey] = true; window.dataLayer.length = 0; };
  document.head.append(script);
}
function reject() {
  store('rejected'); window[disableKey] = true;
  campaign = {}; contextSynced = false;
  try { localStorage.removeItem(CAMPAIGN_KEY); } catch {}
  revokeServerConsent();
  // Only this library's optional cookies. Never touch Coach cookies, login or progress.
  let cookieNames = [];
  try { cookieNames = document.cookie.split(';'); } catch {}
  for (const pair of cookieNames) {
    const name = pair.split('=')[0].trim();
    if (!/^tec_audio_ga(?:_[A-Z0-9]+)?$/i.test(name)) continue;
    for (const domain of ['', '; domain=totalelitecoaching.com', '; domain=.totalelitecoaching.com']) {
      try { document.cookie = name + '=; Max-Age=0; path=/audiolibros' + domain + '; SameSite=Lax; Secure'; } catch {}
    }
  }
}
window.TecAudiobookAnalytics = Object.freeze({
  checkoutContext: measurementContext,
  checkoutStarted() {
    if (!loaded || !allowed()) return;
    command('event', 'begin_checkout', { ...safeAudioEvent('page_view', context), ...campaign,
      send_to: MEASUREMENT_ID, page_referrer: safeReferrer(document.referrer), currency: 'USD', value: 20,
      items: [{ item_id: 'tec_audio_monthly', item_name: 'Membresía Audiolibros TEC', price: 20, quantity: 1 }] });
  },
  samplePlayed(bookId) {
    if (samplesSent.has(bookId)) return;
    if (emit('audio_sample_play', bookId)) samplesSent.add(bookId);
  }
});
document.addEventListener('tec-audio-test-account', () => {
  testAccount = true; window[disableKey] = true;
  try { localStorage.removeItem(CAMPAIGN_KEY); } catch {}
  if (window.dataLayer) window.dataLayer.length = 0;
});
document.addEventListener('tec-audio-account-context', event => {
  const account = event.detail?.account;
  if (account?.testAccount || account?.analyticsExcluded) {
    testAccount = true; window[disableKey] = true;
    try { localStorage.removeItem(CAMPAIGN_KEY); } catch {}
    return;
  }
  signedIn = Boolean(account?.authenticated);
  void syncContext();
});
if (context && !native && !testAccount) {
  window[disableKey] = true;
  const footer = document.querySelector('footer') || document.body;
  const panel = document.createElement('aside');
  panel.className = 'audio-analytics-choice'; panel.setAttribute('aria-label', 'Estadísticas opcionales de audiolibros');
  panel.innerHTML = '<strong>¿Nos ayudas a mejorar la biblioteca?</strong><p>Si aceptas, Google Analytics medirá visitas, muestras, campañas y suscripciones pagadas confirmadas por Stripe, con su importe y moneda. Conservamos la campaña hasta 30 días para relacionar la visita con el pago. Google recibe datos técnicos como IP y navegador. No enviamos nombres, correos, búsquedas, citas, audios ni datos de tarjeta. Es opcional.</p><div class="actions"><button type="button" class="button primary" data-audio-analytics-accept>Aceptar estadísticas</button><button type="button" class="button outline" data-audio-analytics-reject>Rechazar</button><a href="/audiolibros/privacidad/">Privacidad</a></div>';
  panel.hidden = Boolean(choice) || protectedChoice;
  panel.querySelector('[data-audio-analytics-accept]').disabled = protectedChoice;
  panel.querySelector('[data-audio-analytics-accept]').addEventListener('click', () => { if (!protectedChoice) { store('accepted'); start(); } panel.hidden = true; });
  panel.querySelector('[data-audio-analytics-reject]').addEventListener('click', () => { reject(); panel.hidden = true; });
  const preferences = document.createElement('button'); preferences.type = 'button'; preferences.className = 'audio-analytics-settings'; preferences.textContent = 'Estadísticas opcionales';
  preferences.addEventListener('click', () => { panel.hidden = !panel.hidden; if (!panel.hidden) panel.scrollIntoView?.({ block: 'nearest', behavior: 'smooth' }); });
  // Visible on arrival, in the document flow: never cover playback or reading controls.
  const header = document.querySelector('header');
  if (header) header.after(panel); else document.body.prepend(panel);
  footer.append(preferences);
  if (protectedChoice) reject(); else start();
  window.addEventListener('offline', () => { window[disableKey] = true; });
  window.addEventListener('online', () => { if (allowed()) start(); else if (choice === 'rejected') revokeServerConsent(); });
  window.addEventListener('storage', event => { if (event.key === KEY && event.newValue !== 'accepted') reject(); });
  document.addEventListener('click', event => {
    const target = event.target?.closest?.('a,button');
    if (!target || target.disabled) return;
    if (['subscribe', 'unlock'].includes(target.id) || target.getAttribute('href')?.endsWith('#membresia')) emit('audio_membership_click');
    else if (['share-open', 'reader-share'].includes(target.id)) emit('audio_share_open');
  });
  document.addEventListener('play', event => {
    const id = event.target?.dataset?.audioPreview;
    if (id) window.TecAudiobookAnalytics.samplePlayed(id);
  }, true);
}
