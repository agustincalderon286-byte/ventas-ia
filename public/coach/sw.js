const TEC_CACHE_VERSION = "tec-coach-shell-20261022-audio-status1";
const TEC_RUNTIME_CACHE = "tec-coach-runtime-20261022-audio-status1";
const TEC_CATALOG_CACHE = "tec-coach-catalog-20261021-pwa1";
const TEC_DOCUMENT_CACHE = "tec-coach-documents-v1";
const TEC_PDF_CACHE = "tec-coach-pdfjs-6.4.299";
const TEC_OFFLINE_DB_NAME = "tec-coach-offline";
const TEC_OFFLINE_DB_VERSION = 1;

const TEC_CORE_ASSETS = [
  "/coach/",
  "/coach/login/",
  "/coach/site.webmanifest?v=20261022-audio-status1",
  "/coach/app.js?v=20261022-audio-status1",
  "/coach/order-calculator.js?v=20261022-audio-status1",
  "/coach/offline-forms.js?v=20261022-audio-status1",
  "/coach/pwa-safety.js?v=20261022-audio-status1",
  "/coach/ui-coherence.js?v=20261022-audio-status1",
  "/coach/styles.css?v=20261022-audio-status1",
  "/coach/property-map.js?v=20261022-audio-status1",
  "/coach/property-map.css?v=20261022-audio-status1",
  "/coach/property-visit-helpers.js?v=20261022-audio-status1",
  "/coach/document.html",
  "/coach/document-viewer.js?v=20261022-audio-status1",
  "/coach/document-viewer.css?v=20261022-audio-status1",
  "/coach/assets/documents/manifest-v1.json",
  "/coach/vendor/leaflet/leaflet.js",
  "/coach/vendor/leaflet/leaflet.css",
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
  event.waitUntil((async () => {
    // Reject a broken/quota-exhausted shell instead of replacing a working one.
    const critical = TEC_CORE_ASSETS.filter(url => /\.(?:js|css)(?:\?|$)/.test(url));
    const cache = await caches.open(TEC_CACHE_VERSION);
    await Promise.all(critical.map(async url => {
      const response = await fetch(url, { credentials: "same-origin", cache: "reload" });
      if (!response.ok || response.redirected) throw new Error("Incomplete Coach update");
      await cache.put(url, response);
    }));
    await cacheIndividually(TEC_CACHE_VERSION, TEC_CORE_ASSETS.filter(url => !critical.includes(url)));
    await cacheCurrentCoachApp().catch(() => null);
    // No skipWaiting: an update must not take over an open demo or recording.
  })());
});

self.addEventListener("activate", event => {
  event.waitUntil(
    caches
      .keys()
      .then(keys =>
        Promise.all(
          keys
            .filter(key => key.startsWith("tec-coach-") && ![
              TEC_CACHE_VERSION, TEC_RUNTIME_CACHE, TEC_CATALOG_CACHE, TEC_DOCUMENT_CACHE, TEC_PDF_CACHE,
              keys.filter(key => key.startsWith("tec-coach-shell-") && key !== TEC_CACHE_VERSION).at(-1),
              keys.filter(key => key.startsWith("tec-coach-runtime-") && key !== TEC_RUNTIME_CACHE).at(-1)
            ].includes(key))
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
    const cached = (await cache.match(request)) || (fallbackRequest ? await cache.match(fallbackRequest) : null);
    if (cached) return cached;
    if (cacheName === TEC_RUNTIME_CACHE) {
      const previous = (await caches.keys()).filter(key => key.startsWith("tec-coach-runtime-")).reverse();
      for (const name of previous) {
        const old = await caches.open(name);
        const page = (await old.match(request)) || (fallbackRequest ? await old.match(fallbackRequest) : null);
        if (page) return page;
      }
    }
    throw error;
  }
}

async function cacheFirst(request, cacheName) {
  const cache = await caches.open(cacheName);
  // Historical HTML must retain its own versioned dependencies when offline.
  const cached = (await cache.match(request)) || (await caches.match(request));
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

  if (["/coach/resources/novel-catalog-digital-3.pdf", "/coach/resources/novel-sales-presentation-2024.pdf"].includes(url.pathname)) {
    if (request.mode === "navigate") {
      const documentId = url.pathname.includes("sales-presentation") ? "carpeta" : "catalogo";
      event.respondWith(Response.redirect(new URL(`/coach/document.html?document=${documentId}`, self.location.origin)));
      return;
    }
    // PDF.js requests byte ranges. Never cache a 206 response as a complete PDF.
    // Only the viewer's explicit offline-save action writes complete public documents.
    event.respondWith(caches.open(TEC_DOCUMENT_CACHE).then(async cache => (await cache.match(url.pathname)) || fetch(request)));
    return;
  }

  if (url.pathname.startsWith("/coach/vendor/pdfjs/6.4.299/")) {
    event.respondWith(cacheFirst(request, TEC_PDF_CACHE));
    return;
  }

  if (url.pathname.startsWith("/coach/assets/documents/")) {
    event.respondWith((async () => {
      // A full storage quota must never prevent an online page from displaying.
      const cache = await caches.open(TEC_DOCUMENT_CACHE).catch(() => null);
      const cached = await cache?.match(request).catch(() => null);
      if (cached) return cached;
      const response = await fetch(request);
      if (cache && response.ok && response.status !== 206 && !response.redirected) {
        event.waitUntil(cache.put(request, response.clone()).catch(() => null));
      }
      return response;
    })());
    return;
  }

  if (request.mode === "navigate" && url.pathname.startsWith("/coach/")) {
    if (url.pathname === "/coach/document.html") {
      event.respondWith(networkFirst(request, TEC_CACHE_VERSION, "/coach/document.html"));
      return;
    }
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

async function completeOfflineForm(item, data) {
  const db = await openTecOfflineDb();
  await new Promise((resolve, reject) => {
    const tx = db.transaction(["cache", "outbox"], "readwrite");
    tx.objectStore("cache").put({ key: `form-result:${item.ownerEmail}:${item.id}`, ownerEmail: item.ownerEmail,
      kind: item.kind, requestId: item.id, value: data, body: item.body, savedAt: new Date().toISOString() });
    tx.objectStore("outbox").delete(item.id);
    tx.addEventListener("complete", resolve);
    tx.addEventListener("abort", () => reject(tx.error || new Error("No local confirmation")));
    tx.addEventListener("error", () => reject(tx.error || new Error("No local confirmation")));
  });
}

async function recordOfflineFormError(item, message, status) {
  const db = await openTecOfflineDb();
  await new Promise((resolve, reject) => {
    const tx = db.transaction("outbox", "readwrite");
    const store = tx.objectStore("outbox");
    const request = store.get(item.id);
    request.addEventListener("success", () => {
      if (request.result) store.put({ ...request.result, attempts: Number(request.result.attempts || 0) + 1,
        lastError: String(message || "No pude sincronizar.").slice(0, 240), lastStatus: Number(status || 0), lastAttemptAt: new Date().toISOString() });
    });
    tx.addEventListener("complete", resolve);
    tx.addEventListener("abort", () => reject(tx.error));
    tx.addEventListener("error", () => reject(tx.error));
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
  const items = (await getAllOutboxItems()).sort((a, b) => String(a.createdAt || "").localeCompare(String(b.createdAt || "")) || Number(a.kind === "property-lead") - Number(b.kind === "property-lead"));
  let synced = 0;

  for (const item of items) {
    if (!item.ownerEmail || !viewerEmail || item.ownerEmail !== viewerEmail) {
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
      if (["health-survey", "recruitment", "demo-outcome"].includes(item.kind)) {
        const data = await response.json().catch(() => ({}));
        await recordOfflineFormError(item, data.error || `Error ${response.status}`, response.status);
      }
      if (response.status === 401 || response.status === 403) {
        break;
      }
      continue;
    }
    if (["health-survey", "recruitment", "demo-outcome"].includes(item.kind)) {
      await completeOfflineForm(item, await response.json());
    } else {
      await deleteOutboxItem(item.id);
    }
    synced += 1;
  }

  await broadcastOfflineStatus(synced);
}

self.addEventListener("sync", event => {
  if (event.tag === "tec-sync-outbox") {
    event.waitUntil(flushOutboxInServiceWorker().catch(() => null));
  }
});
