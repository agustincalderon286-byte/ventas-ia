import { MEASUREMENT_ID, AREAS, safeEvent, safePage } from './analytics-events.js?v=20261104-analytics1';
// Analytics is optional and isolated: no fetch/media/geolocation/storage monkey patches.
const KEY = 'tec.analytics.choice.v1';
const disableKey = 'ga-disable-' + MEASUREMENT_ID;
let choice = '', started = false, loaded = false, ready = false, loading = false, pending = [], timer = null, inflight = false;
let area = ({ '/coach/': 'inicio', '/coach/login/': 'login', '/coach/planes/': 'planes', '/coach/privacidad/': 'privacidad', '/coach/app/': 'cierre' })[location.pathname.replace(/\/?$/, '/')];
const surface = matchMedia('(display-mode: standalone)').matches || navigator.standalone ? 'pwa' : 'web';
const native = Boolean(window.webkit?.messageHandlers?.tecNative || window.TecNativeOffline || window.TecNative || window.TECNative || window.Capacitor);
const protectedChoice = navigator.globalPrivacyControl === true || navigator.doNotTrack === '1';
const busy = () => document.querySelector('[data-demo-recorder]')?.dataset.captureBusy === 'true';
const allowed = () => choice === 'accepted' && !protectedChoice && !native && navigator.onLine !== false && !busy();
function storeChoice(value) { try { localStorage.setItem(KEY, value); } catch {} }
try { choice = localStorage.getItem(KEY) || ''; } catch {}
window[disableKey] = true;
function command() { window.dataLayer.push(arguments); }
function gtag(...args) { if (ready && allowed()) command(...args); }
function start() {
  if (loading || !allowed()) return;
  if (started) { window[disableKey] = false; ready = loaded; emit('page_view'); return; }
  loading = true;
  // No Google contact at all before opt-in. Even automatic engagement has fixed page metadata.
  window.dataLayer = window.dataLayer || [];
  window[disableKey] = false;
  command('consent', 'default', { analytics_storage: 'granted', ad_storage: 'denied', ad_user_data: 'denied', ad_personalization: 'denied' });
  command('set', { page_location: safePage(area), page_referrer: '', page_title: 'TEC', allow_google_signals: false, allow_ad_personalization_signals: false });
  command('js', new Date());
  command('config', MEASUREMENT_ID, { send_page_view: false, page_location: safePage(area), page_referrer: '', page_title: 'TEC', ignore_referrer: true, allow_google_signals: false, allow_ad_personalization_signals: false, cookie_expires: 2592000, cookie_update: false });
  const script = document.createElement('script');
  script.async = true; script.referrerPolicy = 'no-referrer';
  script.src = 'https://www.googletagmanager.com/gtag/js?id=' + MEASUREMENT_ID;
  script.onload = () => { loading = false; started = true; loaded = true; ready = allowed(); window[disableKey] = !ready; if (ready) gtag('event', 'page_view', { send_to: MEASUREMENT_ID, tec_area: area, tec_surface: surface, page_location: safePage(area), page_referrer: '', page_title: 'TEC' }); };
  script.onerror = () => { loading = false; started = true; ready = false; window[disableKey] = true; window.dataLayer.length = 0; };
  document.head.append(script);
  emit('page_view');
}
function emit(name, eventArea = area) {
  if (!allowed()) return;
  const event = safeEvent(name, eventArea, surface);
  if (!event) return;
  // Our aggregate counts can work even if a browser blocks Google. These are separately labelled.
  if (pending.length < 8) pending.push(event);
  gtag('event', name, { send_to: MEASUREMENT_ID, tec_area: event.area, tec_surface: surface, page_location: safePage(event.area), page_referrer: '', page_title: 'TEC' });
  if (!timer) timer = setTimeout(flush, 2500);
}
async function flush() {
  timer = null;
  if (!allowed()) { pending = []; return; }
  if (inflight) { pending = []; return; }
  const events = pending.splice(0, 8);
  if (!events.length) return;
  inflight = true;
  const controller = new AbortController();
  const deadline = setTimeout(() => controller.abort(), 3000);
  try {
    await fetch('/api/coach/usage-events', { method: 'POST', credentials: 'same-origin', cache: 'no-store', headers: { 'Content-Type': 'application/json', 'X-TEC-Usage': 'v1' }, body: JSON.stringify({ events }), signal: controller.signal });
  } catch {} // Never retry or enter the forms/audio sync queue.
  finally { clearTimeout(deadline); inflight = false; }
}
function revoke() {
  choice = 'rejected'; storeChoice(choice); window[disableKey] = true; ready = false; pending = [];
  clearTimeout(timer); timer = null;
  // Remove only Analytics cookies; never touch authentication, drafts or recording storage.
  for (const name of ['_ga', '_ga_' + MEASUREMENT_ID.slice(2)]) {
    for (const domain of ['', '; domain=totalelitecoaching.com', '; domain=.totalelitecoaching.com']) document.cookie = name + '=; Max-Age=0; path=/' + domain + '; SameSite=Lax; Secure';
  }
}
function preferences() {
  let panel = document.getElementById('tec-analytics-choice');
  if (panel) { panel.hidden = false; return; }
  panel = document.createElement('aside'); panel.id = 'tec-analytics-choice'; panel.className = 'tec-analytics-choice'; panel.setAttribute('aria-label', 'Estadísticas opcionales');
  panel.innerHTML = '<strong>¿Nos ayudas a mejorar TEC?</strong><p>Podemos medir qué herramientas se usan con Google Analytics y contadores de TEC. No enviamos nombres, direcciones, formularios, chats ni audios. Google recibe datos técnicos, como IP y navegador. Es opcional.</p><div><button type="button" data-analytics-accept>Aceptar estadísticas</button><button type="button" data-analytics-reject>Rechazar</button><a href="/coach/privacidad/">Privacidad</a></div>';
  panel.querySelector('[data-analytics-accept]').addEventListener('click', () => { if (protectedChoice) { revoke(); } else { choice = 'accepted'; storeChoice(choice); if (started && ready) emit('page_view'); else start(); } panel.hidden = true; });
  panel.querySelector('[data-analytics-reject]').addEventListener('click', () => { revoke(); panel.hidden = true; });
  document.body.append(panel);
}
if (area && !native) {
  const settings = document.createElement('button'); settings.type = 'button'; settings.className = 'tec-analytics-settings'; settings.textContent = 'Privacidad · estadísticas'; settings.addEventListener('click', preferences);
  // In-flow, never covers map/recording controls.
  const footer = document.querySelector('footer'); (footer || document.body).append(settings);
  if (protectedChoice) revoke();
  else if (choice === 'accepted') start();
  else if (!choice) preferences();
  const recorder = document.querySelector('[data-demo-recorder]');
  if (recorder && typeof MutationObserver === 'function') new MutationObserver(() => {
    window[disableKey] = !allowed();
    if (!allowed()) { pending = []; clearTimeout(timer); timer = null; }
    else if (!started && !loading) start();
  }).observe(recorder, { attributes: true, attributeFilter: ['data-capture-busy'] });
  window.addEventListener('online', () => { window[disableKey] = !allowed(); if (!started) start(); });
  window.addEventListener('offline', () => { window[disableKey] = true; pending = []; clearTimeout(timer); timer = null; });
  window.addEventListener('storage', event => { if (event.key === KEY && event.newValue !== 'accepted') revoke(); });
  window.addEventListener('coach:workspace-change', event => { const next = event.detail?.workspace; if (AREAS.includes(next)) { area = next; emit('workspace_view'); } });
  document.addEventListener('click', event => {
    const target = event.target?.closest?.('button, a'); if (!target || target.disabled || target.getAttribute?.('aria-expanded') === 'false') return;
    const matches = [
      ['[data-property-walk]', 'route_open', 'mapa'], ['[data-order-calc-toggle]', 'calculator_open', 'cierre'],
      ['[data-health-survey-toggle]', 'survey_open', 'cierre'], ['[data-tec-document], [data-private-resource-open]', 'catalog_open', 'cierre'],
      ['[data-tec-video-id], .tec-product-video-button, [data-product-video-library]', 'video_open', 'biblioteca'],
      ['[data-demo-panel-toggle]', 'recording_open', 'cierre'], ['[data-coach-float-toggle]', 'coach_open', 'cierre'],
      ['[data-start-checkout]', 'checkout_start_requested', 'planes']
    ];
    for (const [selector, name, fixedArea] of matches) if (target.matches(selector)) { emit(name, fixedArea); break; }
  });
  document.addEventListener('submit', event => {
    const form = event.target;
    if (form.matches('[data-health-survey-form]')) emit('survey_submit_requested', 'cierre');
    else if (area === 'login' && form.matches('[data-login-form]')) emit('login_submit_requested', 'login');
    else if (area === 'planes' && form.matches('[data-signup-checkout-form]')) { emit('registration_submit_requested', 'planes'); emit('checkout_start_requested', 'planes'); }
  });
}
