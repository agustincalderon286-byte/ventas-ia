const TEC_CACHE_VERSION = "tec-coach-shell-20261021-pwa1";
const TEC_RUNTIME_CACHE = "tec-coach-runtime-20261021-pwa1";
const TEC_CATALOG_CACHE = "tec-coach-catalog-20261021-pwa1";
const TEC_OFFLINE_DB_NAME = "tec-coach-offline";
const TEC_OFFLINE_DB_VERSION = 1;

const TEC_CORE_ASSETS = [
  "/coach/",
  "/coach/login/",
  "/coach/site.webmanifest?v=20261021-pwa1",
  "/coach/app.js?v=20261021-pwa1",
  "/coach/styles.css?v=20261021-pwa1",
  "/coach/icons/favicon-64.png?v=20261021",
  "/coach/icons/apple-touch-icon.png?v=20261021",
  "/coach/icons/icon-192.png?v=20261021",
  "/coach/icons/icon-512.png?v=20261021",
  "/coach/assets/brand/tec-total-elite-coaching-transparent.png",
  "/coach/assets/coach-conexion-home.jpg",
  "/coach/assets/coach-summit-hero.jpg",
  "/coach/assets/steps/step-2-entrega-regalo.jpg",
  "/coach/assets/steps/step-3-programa-4-en-14.jpg",
  "/coach/assets/steps/step-4-cita-instantanea.jpg",
  "/coach/assets/steps/step-5-encuesta-salud.jpg",
  "/coach/assets/steps/step-6-demostracion.jpg",
  "/coach/assets/steps/step-7-cierre.jpg",
  "/coach/assets/steps/step-8-invitacion-negocio.jpg",
  "/coach/assets/tools/tool-prueba-agua.jpg",
  "/coach/assets/tools/tool-balance-decision.jpg",
  "/coach/assets/tools/tool-calculadora-pedido.jpg",
  "/coach/assets/tools/tool-perfil-cliente.jpg",
  "/coach/assets/tools/tool-oferta-especial.jpg"
];

async function cacheIndividually(cacheName, urls = []) {
  const cache = await caches.open(cacheName);
  await Promise.allSettled(
    urls.map(async url => {
      const response = await fetch(url, { credentials: "same-origin", cache: "reload" });
      if (response.ok && !response.redirected) {
        await cache.put(url, response.clone());
      }
    })
  );
}

self.addEventListener("install", event => {
  event.waitUntil(cacheIndividually(TEC_CACHE_VERSION, TEC_CORE_ASSETS).then(() => self.skipWaiting()));
});

self.addEventListener("activate", event => {
  event.waitUntil(
    caches
      .keys()
      .then(keys =>
        Promise.all(
          keys
            .filter(key => key.startsWith("tec-coach-") && ![TEC_CACHE_VERSION, TEC_RUNTIME_CACHE, TEC_CATALOG_CACHE].includes(key))
            .map(key => caches.delete(key))
        )
      )
      .then(() => self.clients.claim())
  );
});

async function networkFirst(request, cacheName, fallbackRequest = null) {
  const cache = await caches.open(cacheName);
  try {
    const response = await fetch(request);
    if (response.ok && !response.redirected) {
      await cache.put(request, response.clone());
    }
    return response;
  } catch (error) {
    return (await cache.match(request)) || (fallbackRequest ? await cache.match(fallbackRequest) : null) || Promise.reject(error);
  }
}

async function cacheFirst(request, cacheName) {
  const cache = await caches.open(cacheName);
  const cached = await cache.match(request);
  if (cached) {
    fetch(request)
      .then(response => {
        if (response.ok && !response.redirected) {
          return cache.put(request, response.clone());
        }
        return null;
      })
      .catch(() => null);
    return cached;
  }
  const response = await fetch(request);
  if (response.ok && !response.redirected) {
    await cache.put(request, response.clone());
  }
  return response;
}

self.addEventListener("fetch", event => {
  const request = event.request;
  if (request.method !== "GET") {
    return;
  }

  const url = new URL(request.url);
  if (url.origin !== self.location.origin) {
    return;
  }

  if (url.pathname === "/api/coach/catalog-offline" || url.pathname === "/api/coach/catalog-prices") {
    event.respondWith(networkFirst(request, TEC_CATALOG_CACHE));
    return;
  }

  if (request.mode === "navigate" && url.pathname.startsWith("/coach/")) {
    const fallback = ["/coach/app/", "/coach/telemarketing/"].includes(url.pathname) ? "/coach/app/" : "/coach/login/";
    event.respondWith(networkFirst(request, TEC_RUNTIME_CACHE, fallback));
    return;
  }

  if (url.pathname.startsWith("/coach/assets/catalog/")) {
    event.respondWith(cacheFirst(request, TEC_CATALOG_CACHE));
    return;
  }

  if (url.pathname.startsWith("/coach/")) {
    event.respondWith(cacheFirst(request, TEC_CACHE_VERSION));
  }
});

async function cacheCurrentCoachApp() {
  const response = await fetch("/coach/app/", { credentials: "include", cache: "no-store" });
  const responseUrl = new URL(response.url, self.location.origin);
  if (response.ok && !response.redirected && responseUrl.pathname === "/coach/app/") {
    const cache = await caches.open(TEC_RUNTIME_CACHE);
    await cache.put("/coach/app/", response.clone());
  }
}

self.addEventListener("message", event => {
  const type = event.data?.type || "";
  if (type === "CACHE_CURRENT_COACH_APP") {
    event.waitUntil(cacheCurrentCoachApp().catch(() => null));
    return;
  }
  if (type === "CACHE_CATALOG_ASSETS") {
    const urls = Array.isArray(event.data?.urls)
      ? event.data.urls.filter(url => String(url || "").startsWith("/coach/assets/catalog/")).slice(0, 300)
      : [];
    event.waitUntil(cacheIndividually(TEC_CATALOG_CACHE, urls));
  }
});

function openTecOfflineDb() {
  return new Promise((resolve, reject) => {
    const request = indexedDB.open(TEC_OFFLINE_DB_NAME, TEC_OFFLINE_DB_VERSION);
    request.addEventListener("upgradeneeded", () => {
      const db = request.result;
      if (!db.objectStoreNames.contains("outbox")) {
        const store = db.createObjectStore("outbox", { keyPath: "id" });
        store.createIndex("createdAt", "createdAt");
      }
      if (!db.objectStoreNames.contains("cache")) {
        db.createObjectStore("cache", { keyPath: "key" });
      }
      if (!db.objectStoreNames.contains("recordings")) {
        const store = db.createObjectStore("recordings", { keyPath: "localId" });
        store.createIndex("createdAt", "createdAt");
      }
      if (!db.objectStoreNames.contains("audioChunks")) {
        const store = db.createObjectStore("audioChunks", { keyPath: "key" });
        store.createIndex("localId", "localId");
      }
    });
    request.addEventListener("success", () => resolve(request.result));
    request.addEventListener("error", () => reject(request.error));
  });
}

async function getAllOutboxItems() {
  const db = await openTecOfflineDb();
  return await new Promise((resolve, reject) => {
    const request = db.transaction("outbox", "readonly").objectStore("outbox").getAll();
    request.addEventListener("success", () => resolve(request.result || []));
    request.addEventListener("error", () => reject(request.error));
  });
}

async function deleteOutboxItem(id) {
  const db = await openTecOfflineDb();
  return await new Promise((resolve, reject) => {
    const request = db.transaction("outbox", "readwrite").objectStore("outbox").delete(id);
    request.addEventListener("success", resolve);
    request.addEventListener("error", () => reject(request.error));
  });
}

async function broadcastOfflineStatus(synced = 0) {
  const windows = await self.clients.matchAll({ type: "window", includeUncontrolled: true });
  windows.forEach(client => client.postMessage({ type: "TEC_OFFLINE_SYNC", synced }));
}

async function flushOutboxInServiceWorker() {
  const viewerResponse = await fetch("/api/coach/me", { credentials: "include", cache: "no-store" });
  if (!viewerResponse.ok) {
    return;
  }
  const viewer = await viewerResponse.json().catch(() => ({}));
  const viewerEmail = String(viewer?.user?.email || "").trim().toLowerCase();
  const items = (await getAllOutboxItems()).sort((a, b) => String(a.createdAt || "").localeCompare(String(b.createdAt || "")));
  let synced = 0;

  for (const item of items) {
    if (item.ownerEmail && viewerEmail && item.ownerEmail !== viewerEmail) {
      continue;
    }
    const response = await fetch(item.url, {
      method: item.method || "POST",
      credentials: "include",
      headers: {
        ...(item.body ? { "Content-Type": "application/json" } : {}),
        ...(item.headers || {}),
        "X-TEC-Request-ID": item.id
      },
      ...(item.body ? { body: JSON.stringify(item.body) } : {})
    });
    if (!response.ok) {
      if (response.status === 401 || response.status === 403) {
        break;
      }
      continue;
    }
    await deleteOutboxItem(item.id);
    synced += 1;
  }

  await broadcastOfflineStatus(synced);
}

self.addEventListener("sync", event => {
  if (event.tag === "tec-sync-outbox") {
    event.waitUntil(flushOutboxInServiceWorker().catch(() => null));
  }
});
