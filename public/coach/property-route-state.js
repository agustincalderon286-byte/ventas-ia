import { propertyVisitKey } from './property-visit-helpers.js';

export const MAX_ROUTE_STOPS = 50;
export function routeProperty(value) {
  return Object.fromEntries(['pin','address','city','state','zipCode','formattedAddress','lat','lon'].map(key => [key, value[key]]));
}
export function emptyPropertyRoute(anchor = null) {
  return { anchor: anchor ? routeProperty(anchor) : null, stops: [], travelMode: 'walking', clientUpdatedAt: '', requestId: '', pending: false };
}
export function nextRouteStop(route) { return route?.stops.find(stop => !stop.completedAt) || null; }
export function routeDistance(a, b) {
  const rad = Math.PI / 180;
  const x = (b.lon - a.lon) * rad * Math.cos((a.lat + b.lat) * rad / 2);
  const y = (b.lat - a.lat) * rad;
  return Math.hypot(x, y) * 6371000;
}
export function orderRouteByProximity(route) {
  const done = route.stops.filter(stop => stop.completedAt);
  const pending = route.stops.filter(stop => !stop.completedAt);
  const lastVisit = done.reduce((last, stop) => !last || stop.completedAt > last.completedAt ? stop : last, null);
  let current = lastVisit?.property || route.anchor;
  if (!current) return route;
  const ordered = [];
  while (pending.length) {
    let best = 0;
    for (let i = 1; i < pending.length; i++) if (routeDistance(current, pending[i].property) < routeDistance(current, pending[best].property)) best = i;
    const [stop] = pending.splice(best, 1); ordered.push(stop); current = stop.property;
  }
  return { ...route, stops: [...done, ...ordered] };
}
export function toggleRouteStop(route, property, anchor) {
  const key = propertyVisitKey(property), found = route.stops.some(stop => propertyVisitKey(stop.property) === key);
  if (found) {
    const stops = route.stops.filter(stop => propertyVisitKey(stop.property) !== key);
    return { ...route, stops, anchor: stops.length ? route.anchor : null };
  }
  if (route.stops.length >= MAX_ROUTE_STOPS) throw new Error('Tu recorrido ya tiene 50 casas. Quita una para agregar otra.');
  return { ...route, anchor: route.anchor || routeProperty(anchor || property), stops: [...route.stops, { property: routeProperty(property), completedAt: null }] };
}
export function completeRouteStop(route, key, completedAt) {
  return { ...route, stops: route.stops.map(stop => propertyVisitKey(stop.property) === key ? { ...stop, completedAt } : stop) };
}
export function moveRouteStop(route, key, delta) {
  const stops = [...route.stops], from = stops.findIndex(stop => propertyVisitKey(stop.property) === key), to = from + delta;
  if (from < 0 || to < 0 || to >= stops.length) return route;
  [stops[from], stops[to]] = [stops[to], stops[from]];
  return { ...route, stops };
}
export function routeNavigationUrl(provider, stop, travelMode = 'walking') {
  if (!stop) return '';
  const property = stop.property || stop;
  const address = property.formattedAddress || [property.address, property.city, 'IL', property.zipCode].filter(Boolean).join(', ');
  const url = new URL(provider === 'apple' ? 'https://maps.apple.com/' : 'https://www.google.com/maps/dir/');
  if (provider === 'apple') { url.searchParams.set('daddr', address); url.searchParams.set('dirflg', travelMode === 'driving' ? 'd' : 'w'); }
  else { url.searchParams.set('api', '1'); url.searchParams.set('destination', address); url.searchParams.set('travelmode', travelMode === 'driving' ? 'driving' : 'walking'); url.searchParams.set('dir_action', 'navigate'); }
  // Omit origin: the navigation app uses the representative's current location.
  return url.href;
}
function savedRoute(value) { return value && !Array.isArray(value) && Array.isArray(value.stops) && value.stops.length <= MAX_ROUTE_STOPS; }
export function latestPropertyRoute(current, remote, pendingItems, accountId, email) {
  const pending = pendingItems.filter(item => item.kind === 'property-route' && item.ownerEmail === email && String(item.body?.accountId) === accountId && savedRoute(item.body?.route))
    .map(item => ({ ...item.body.route, requestId: item.id, pending: true }));
  const candidates = [current, remote, ...pending].filter(savedRoute);
  candidates.sort((a,b) => String(b.clientUpdatedAt).localeCompare(String(a.clientUpdatedAt)) || String(b.requestId).localeCompare(String(a.requestId)));
  const latest = candidates[0] || emptyPropertyRoute();
  return remote?.requestId && remote.requestId === latest.requestId ? { ...remote, pending: false } : latest;
}

// The existing durable outbox is authoritative until the server acknowledges it.
// The cache lets the route be reopened without internet, including in native TEC.
export function createPropertyRouteStore({ accountId, email, request, offline, onChange = () => {}, onStatus = () => {}, online = () => navigator.onLine, now = Date.now }) {
  const key = `property-route-v1:${accountId}:${email}`;
  let current = emptyPropertyRoute(), changes = Promise.resolve(), cacheWrites = Promise.resolve(), syncing, syncAgain = false, readyOk = false;
  const cache = () => {
    const snapshot = structuredClone(current);
    cacheWrites = cacheWrites.catch(() => {}).then(() => offline.write(key, snapshot));
    return cacheWrites;
  };
  const notify = () => { onChange(current); onStatus(current.pending ? 'Ruta guardada en este dispositivo. Pendiente de sincronizar con tu cuenta.' : current.requestId ? 'Ruta y avance guardados en tu cuenta.' : 'Selecciona hasta 50 casas. Cada cambio se guardará automáticamente.'); };
  const ready = (async () => {
    if (!offline || !accountId || !email) throw new Error('No pude activar el guardado de rutas. Vuelve a entrar a TEC.');
    const [stored, pending] = await Promise.all([offline.read(key), offline.pending()]);
    current = latestPropertyRoute(stored, null, pending, accountId, email);
    readyOk = true; notify();
  })().catch(error => { onStatus(`No pude recuperar tu ruta: ${error.message}`); throw error; });
  async function refresh() {
    try {
      await ready;
      if (!online()) { notify(); return; }
      const { route: remote } = await request('/api/coach/property-route', { timeoutMs: 15000 });
      const pending = await offline.pending();
      current = latestPropertyRoute(current, remote, pending, accountId, email);
      await cache(); notify();
    } catch (error) { onStatus(current.pending ? 'Ruta guardada en este dispositivo. No pude sincronizarla; pulsa Reintentar.' : 'Mostrando tu ruta guardada en este dispositivo. No pude consultar la cuenta.'); }
  }
  function sync() {
    syncAgain = true;
    if (syncing || !online()) return syncing;
    syncing = (async () => { await ready; while (syncAgain && online()) { syncAgain = false; await offline.flush(); await refresh(); } })()
      .catch(() => { onStatus('La ruta sigue guardada en este dispositivo. Pulsa Reintentar para sincronizar.'); })
      .finally(() => { syncing = null; });
    return syncing;
  }
  function change(update) {
    const action = changes.catch(() => {}).then(async () => {
      await ready;
      const next = update(structuredClone(current));
      if (!next || JSON.stringify(next) === JSON.stringify(current)) return current;
      next.clientUpdatedAt = new Date(Math.max(now(), Date.parse(current.clientUpdatedAt || '') + 1 || 0)).toISOString();
      const { requestId } = await request('/api/coach/property-route', { method: 'POST', body: { accountId, route: next }, offlineQueue: true, queueFirst: true, offlineKind: 'property-route' });
      current = { ...next, requestId, pending: true };
      // If the cache fails, the successfully committed outbox still contains the route.
      try { await cache(); } catch { /* recovered from outbox on next entry */ }
      notify(); sync(); return current;
    });
    changes = action;
    return action;
  }
  return { ready, refresh, sync, change, get: () => current, isReady: () => readyOk };
}
