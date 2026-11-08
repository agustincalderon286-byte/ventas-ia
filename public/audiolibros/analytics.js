import { MEASUREMENT_ID, pageContext, safeAudioEvent, safeReferrer } from './analytics-events.js?v=20261107-guides';

const KEY = 'tec.audiobooks.analytics.choice.v1';
const disableKey = 'ga-disable-' + MEASUREMENT_ID;
const context = pageContext(location.pathname);
const native = Boolean(window.webkit?.messageHandlers?.tecNative || window.TecNativeOffline || window.TecNative || window.TECNative || window.Capacitor);
const protectedChoice = navigator.globalPrivacyControl === true || navigator.doNotTrack === '1';
const production = ['https://totalelitecoaching.com', 'https://www.totalelitecoaching.com'].includes(location.origin);
let choice = '', loading = false, loaded = false, pageSent = false;
const samplesSent = new Set();
try { choice = localStorage.getItem(KEY) || ''; } catch {}
const allowed = () => context && production && !native && !protectedChoice && choice === 'accepted' && navigator.onLine !== false;
function store(value) { choice = value; try { localStorage.setItem(KEY, value); } catch {} }
function command() { window.dataLayer.push(arguments); }
function emit(name, bookId) {
  if (!loaded || !allowed()) return false;
  const params = safeAudioEvent(name, context, bookId);
  if (!params) return false;
  command('event', name, { ...params, send_to: MEASUREMENT_ID, page_referrer: safeReferrer(document.referrer) });
  return true;
}
function pageView() { if (!pageSent && emit('page_view')) pageSent = true; }
function start() {
  if (!allowed() || loading) return;
  window[disableKey] = false;
  if (loaded) { pageView(); return; }
  loading = true;
  window.dataLayer = window.dataLayer || [];
  command('consent', 'default', { analytics_storage: 'granted', ad_storage: 'denied', ad_user_data: 'denied', ad_personalization: 'denied' });
  const metadata = { page_location: 'https://totalelitecoaching.com' + context.path, page_title: 'Audiolibros TEC · ' + context.type, page_referrer: safeReferrer(document.referrer), allow_google_signals: false, allow_ad_personalization_signals: false };
  command('set', { ...metadata, ads_data_redaction: true, url_passthrough: false });
  command('js', new Date());
  command('config', MEASUREMENT_ID, { ...metadata, send_page_view: false, cookie_prefix: 'tec_audio', cookie_path: '/audiolibros', cookie_expires: 2592000, cookie_update: false, cookie_flags: 'SameSite=Lax;Secure' });
  const script = document.createElement('script');
  script.async = true; script.referrerPolicy = 'no-referrer';
  script.src = 'https://www.googletagmanager.com/gtag/js?id=' + MEASUREMENT_ID;
  script.onload = () => { loading = false; loaded = true; window[disableKey] = !allowed(); pageView(); };
  script.onerror = () => { loading = false; window[disableKey] = true; window.dataLayer.length = 0; };
  document.head.append(script);
}
function reject() {
  store('rejected'); window[disableKey] = true;
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
  samplePlayed(bookId) {
    if (samplesSent.has(bookId)) return;
    if (emit('audio_sample_play', bookId)) samplesSent.add(bookId);
  }
});
if (context && !native) {
  window[disableKey] = true;
  const footer = document.querySelector('footer') || document.body;
  const panel = document.createElement('aside');
  panel.className = 'audio-analytics-choice'; panel.setAttribute('aria-label', 'Estadísticas opcionales de audiolibros');
  panel.innerHTML = '<strong>¿Nos ayudas a mejorar la biblioteca?</strong><p>Si aceptas, Google Analytics medirá visitas a páginas públicas, muestras reproducidas y clics de membresía. Google recibe datos técnicos, como IP y navegador. No enviamos correos, búsquedas, citas seleccionadas, audios ni datos de pago. Es opcional.</p><div class="actions"><button type="button" class="button primary" data-audio-analytics-accept>Aceptar estadísticas</button><button type="button" class="button outline" data-audio-analytics-reject>Rechazar</button><a href="/audiolibros/privacidad/">Privacidad</a></div>';
  panel.hidden = Boolean(choice) || protectedChoice;
  panel.querySelector('[data-audio-analytics-accept]').disabled = protectedChoice;
  panel.querySelector('[data-audio-analytics-accept]').addEventListener('click', () => { if (!protectedChoice) { store('accepted'); start(); } panel.hidden = true; });
  panel.querySelector('[data-audio-analytics-reject]').addEventListener('click', () => { reject(); panel.hidden = true; });
  const preferences = document.createElement('button'); preferences.type = 'button'; preferences.className = 'audio-analytics-settings'; preferences.textContent = 'Estadísticas opcionales';
  preferences.addEventListener('click', () => { panel.hidden = !panel.hidden; });
  footer.append(panel, preferences);
  if (protectedChoice) reject(); else start();
  window.addEventListener('offline', () => { window[disableKey] = true; });
  window.addEventListener('online', () => { if (allowed()) start(); });
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
