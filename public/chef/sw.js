const CACHE_NAME = "agustin-chef-shell-v2";
const APP_SHELL = [
  "/chef/",
  "/chef/index.html",
  "/chef/styles.css",
  "/chef/app.js",
  "/chef/manifest.webmanifest",
  "/chef/icons/icon-180.png",
  "/chef/icons/icon-192.png",
  "/chef/icons/icon-512.png"
];

self.addEventListener("install", event => {
  event.waitUntil(
    caches.open(CACHE_NAME).then(cache => cache.addAll(APP_SHELL)).then(() => self.skipWaiting())
  );
});

self.addEventListener("activate", event => {
  event.waitUntil(
    caches.keys().then(keys =>
      Promise.all(
        keys.map(key => {
          if (key !== CACHE_NAME) {
            return caches.delete(key);
          }

          return Promise.resolve();
        })
      )
    ).then(() => self.clients.claim())
  );
});

self.addEventListener("fetch", event => {
  const requestUrl = new URL(event.request.url);

  if (requestUrl.origin !== self.location.origin) {
    return;
  }

  if (requestUrl.pathname.startsWith("/chat") || requestUrl.pathname.startsWith("/api/")) {
    return;
  }

  const isNavigationRequest =
    event.request.mode === "navigate" || event.request.destination === "document";

  if (isNavigationRequest) {
    event.respondWith(
      fetch(event.request)
        .then(networkResponse => {
          if (networkResponse && networkResponse.status === 200 && networkResponse.type === "basic") {
            const responseClone = networkResponse.clone();
            caches.open(CACHE_NAME).then(cache => cache.put(event.request, responseClone));
          }

          return networkResponse;
        })
        .catch(async () => {
          const cachedResponse = await caches.match(event.request);
          if (cachedResponse) {
            return cachedResponse;
          }

          return caches.match("/chef/index.html");
        })
    );
    return;
  }

  event.respondWith(
    caches.match(event.request).then(cachedResponse => {
      if (cachedResponse) {
        return cachedResponse;
      }

      return fetch(event.request).then(networkResponse => {
        if (!networkResponse || networkResponse.status !== 200 || networkResponse.type !== "basic") {
          return networkResponse;
        }

        const responseClone = networkResponse.clone();
        caches.open(CACHE_NAME).then(cache => cache.put(event.request, responseClone));
        return networkResponse;
      });
    })
  );
});
