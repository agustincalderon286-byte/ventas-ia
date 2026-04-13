const CACHE_NAME = "cmwf-prospector-shell-v1";
const MANAGED_NAVIGATION_PATHS = new Set([
  "/metalworks-crm/prospector",
  "/metalworks-crm/prospector/",
  "/metalworks-crm/prospector/login",
  "/metalworks-crm/prospector/login/",
]);
const MANAGED_ASSET_PATHS = new Set([
  "/metalworks-crm/styles.css",
  "/metalworks-crm/prospector-app.js",
  "/metalworks-crm/prospector-login.js",
  "/metalworks-crm/prospector.webmanifest",
  "/metalworks-crm/prospector-icon.svg",
]);
const APP_SHELL = [
  "/metalworks-crm/prospector/",
  "/metalworks-crm/prospector/login/",
  "/metalworks-crm/styles.css",
  "/metalworks-crm/prospector-app.js",
  "/metalworks-crm/prospector-login.js",
  "/metalworks-crm/prospector.webmanifest",
  "/metalworks-crm/prospector-icon.svg",
];

function isManagedNavigation(pathname = "") {
  return MANAGED_NAVIGATION_PATHS.has(pathname);
}

function isManagedAsset(pathname = "") {
  return MANAGED_ASSET_PATHS.has(pathname);
}

async function warmCache(urls = []) {
  const cache = await caches.open(CACHE_NAME);

  await Promise.allSettled(
    urls.map(async (url) => {
      const request = new Request(url, {
        credentials: "same-origin",
      });
      const response = await fetch(request);

      if (response && response.ok && response.type === "basic") {
        await cache.put(request, response.clone());
      }
    }),
  );
}

self.addEventListener("install", (event) => {
  event.waitUntil(
    warmCache(APP_SHELL).then(() => self.skipWaiting()),
  );
});

self.addEventListener("activate", (event) => {
  event.waitUntil(
    caches
      .keys()
      .then((keys) =>
        Promise.all(
          keys.map((key) => {
            if (key !== CACHE_NAME) {
              return caches.delete(key);
            }

            return Promise.resolve();
          }),
        ),
      )
      .then(() => self.clients.claim()),
  );
});

self.addEventListener("message", (event) => {
  const payload = event.data || {};

  if (payload.type !== "CACHE_URLS" || !Array.isArray(payload.urls)) {
    return;
  }

  event.waitUntil(warmCache(payload.urls));
});

self.addEventListener("fetch", (event) => {
  const requestUrl = new URL(event.request.url);

  if (requestUrl.origin !== self.location.origin) {
    return;
  }

  if (requestUrl.pathname.startsWith("/api/")) {
    return;
  }

  const isNavigationRequest =
    event.request.mode === "navigate" || event.request.destination === "document";

  if (isNavigationRequest && isManagedNavigation(requestUrl.pathname)) {
    event.respondWith(
      fetch(event.request)
        .then(async (networkResponse) => {
          if (
            networkResponse &&
            networkResponse.ok &&
            networkResponse.type === "basic" &&
            !networkResponse.redirected
          ) {
            const cache = await caches.open(CACHE_NAME);
            await cache.put(event.request, networkResponse.clone());
          }

          return networkResponse;
        })
        .catch(async () => {
          const cachedResponse =
            (await caches.match(event.request)) ||
            (await caches.match(
              requestUrl.pathname.includes("/login")
                ? "/metalworks-crm/prospector/login/"
                : "/metalworks-crm/prospector/",
            ));

          if (cachedResponse) {
            return cachedResponse;
          }

          return new Response("Offline", {
            status: 503,
            headers: {
              "Content-Type": "text/plain; charset=utf-8",
            },
          });
        }),
    );
    return;
  }

  if (!isManagedAsset(requestUrl.pathname)) {
    return;
  }

  event.respondWith(
    caches.match(event.request).then((cachedResponse) => {
      if (cachedResponse) {
        fetch(event.request)
          .then(async (networkResponse) => {
            if (
              networkResponse &&
              networkResponse.ok &&
              networkResponse.type === "basic"
            ) {
              const cache = await caches.open(CACHE_NAME);
              await cache.put(event.request, networkResponse.clone());
            }
          })
          .catch(() => {});

        return cachedResponse;
      }

      return fetch(event.request).then(async (networkResponse) => {
        if (
          networkResponse &&
          networkResponse.ok &&
          networkResponse.type === "basic"
        ) {
          const cache = await caches.open(CACHE_NAME);
          await cache.put(event.request, networkResponse.clone());
        }

        return networkResponse;
      });
    }),
  );
});
