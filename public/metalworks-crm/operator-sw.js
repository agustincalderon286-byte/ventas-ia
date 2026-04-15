const CACHE_NAME = "cmwf-operator-shell-v1";
const SHELL_URLS = [
  "/metalworks-crm/operator/",
  "/metalworks-crm/styles.css",
  "/metalworks-crm/operator-mobile.css",
  "/metalworks-crm/operator-mobile.js",
  "/metalworks-crm/operator.webmanifest",
  "/metalworks-crm/operator-icon.svg",
];

self.addEventListener("install", (event) => {
  event.waitUntil(
    caches.open(CACHE_NAME).then((cache) => cache.addAll(SHELL_URLS)).catch(() => null),
  );
  self.skipWaiting();
});

self.addEventListener("activate", (event) => {
  event.waitUntil(
    caches.keys().then((keys) =>
      Promise.all(
        keys
          .filter((key) => key !== CACHE_NAME && key.startsWith("cmwf-operator-shell"))
          .map((key) => caches.delete(key)),
      ),
    ),
  );
  self.clients.claim();
});

self.addEventListener("fetch", (event) => {
  if (event.request.method !== "GET") {
    return;
  }

  const requestUrl = new URL(event.request.url);

  if (requestUrl.origin !== self.location.origin) {
    return;
  }

  if (requestUrl.pathname === "/metalworks-crm/operator/" || requestUrl.pathname === "/metalworks-crm/operator") {
    event.respondWith(
      fetch(event.request)
        .then((response) => {
          const clone = response.clone();
          caches.open(CACHE_NAME).then((cache) => cache.put("/metalworks-crm/operator/", clone));
          return response;
        })
        .catch(() => caches.match("/metalworks-crm/operator/")),
    );
    return;
  }

  if (SHELL_URLS.includes(requestUrl.pathname)) {
    event.respondWith(caches.match(event.request).then((cached) => cached || fetch(event.request)));
  }
});

self.addEventListener("push", (event) => {
  const payload = (() => {
    try {
      return event.data ? JSON.parse(event.data.text()) : {};
    } catch {
      return {};
    }
  })();

  const title = String(payload.title || "New lead").trim() || "New lead";
  const body = String(payload.body || "Chicago Metal Works & Fencing sent an update.").trim();
  const url = String(payload.url || "/metalworks-crm/operator/").trim() || "/metalworks-crm/operator/";
  const setBadgePromise =
    "setAppBadge" in self.navigator
      ? self.navigator.setAppBadge().catch(() => null)
      : Promise.resolve();

  event.waitUntil(
    Promise.all([
      self.registration.showNotification(title, {
        body,
        icon: "/metalworks-crm/operator-icon.svg",
        badge: "/metalworks-crm/operator-icon.svg",
        data: {
          url,
        },
        tag: String(payload.leadId || payload.alertType || "cmwf-operator-alert").trim(),
        renotify: true,
      }),
      setBadgePromise,
    ]),
  );
});

self.addEventListener("notificationclick", (event) => {
  event.notification.close();
  const targetUrl =
    String(event.notification?.data?.url || "/metalworks-crm/operator/").trim() ||
    "/metalworks-crm/operator/";

  event.waitUntil(
    Promise.all([
      "clearAppBadge" in self.navigator ? self.navigator.clearAppBadge().catch(() => null) : null,
      clients.matchAll({ type: "window", includeUncontrolled: true }).then((clientList) => {
        for (const client of clientList) {
          if ("focus" in client && client.url.includes("/metalworks-crm/operator")) {
            client.navigate(targetUrl).catch(() => null);
            return client.focus();
          }
        }

        if (clients.openWindow) {
          return clients.openWindow(targetUrl);
        }

        return null;
      }),
    ]),
  );
});
