const CACHE_NAME = "cmwf-live-chat-shell-v2"
const SHELL_URLS = [
  "/metalworks-chat/",
  "/metalworks-chat/app.js",
  "/metalworks-chat/styles.css",
  "/metalworks-chat/chat.webmanifest",
  "/metalworks-crm/crm-icon.svg",
  "/metalworks-crm/crm-icon-180.png",
  "/metalworks-crm/crm-icon-192.png",
  "/metalworks-crm/crm-icon-512.png",
]

self.addEventListener("install", (event) => {
  event.waitUntil(
    caches.open(CACHE_NAME).then((cache) => cache.addAll(SHELL_URLS)).catch(() => null),
  )
  self.skipWaiting()
})

self.addEventListener("activate", (event) => {
  event.waitUntil(
    caches.keys().then((keys) =>
      Promise.all(
        keys
          .filter((key) => key !== CACHE_NAME && key.startsWith("cmwf-live-chat-shell"))
          .map((key) => caches.delete(key)),
      ),
    ),
  )
  self.clients.claim()
})

self.addEventListener("fetch", (event) => {
  if (event.request.method !== "GET") {
    return
  }

  const requestUrl = new URL(event.request.url)

  if (requestUrl.origin !== self.location.origin) {
    return
  }

  if (
    requestUrl.pathname === "/metalworks-chat/" ||
    requestUrl.pathname === "/metalworks-chat" ||
    requestUrl.pathname === "/metalworks-chat/app.js" ||
    requestUrl.pathname === "/metalworks-chat/styles.css" ||
    requestUrl.pathname === "/metalworks-chat/chat.webmanifest" ||
    requestUrl.pathname === "/metalworks-chat/chat-sw.js"
  ) {
    event.respondWith(
      fetch(event.request)
        .then((response) => {
          const clone = response.clone()
          caches.open(CACHE_NAME).then((cache) => cache.put(event.request, clone))
          return response
        })
        .catch(() => caches.match(event.request)),
    )
    return
  }

  if (SHELL_URLS.includes(requestUrl.pathname)) {
    event.respondWith(caches.match(event.request).then((cached) => cached || fetch(event.request)))
  }
})

self.addEventListener("push", (event) => {
  const payload = (() => {
    try {
      return event.data ? JSON.parse(event.data.text()) : {}
    } catch {
      return {}
    }
  })()

  const title = String(payload.title || "Chicago Metal Works").trim() || "Chicago Metal Works"
  const body = String(payload.body || "Open the chat to see the latest reply.").trim()
  const url = String(payload.url || "/metalworks-chat/").trim() || "/metalworks-chat/"

  event.waitUntil(
    self.registration.showNotification(title, {
      body,
      icon: "/metalworks-crm/crm-icon-192.png",
      badge: "/metalworks-crm/crm-icon-192.png",
      data: {
        url,
      },
      tag: String(payload.leadId || payload.alertType || "cmwf-live-chat-alert").trim(),
      renotify: true,
    }),
  )
})

self.addEventListener("notificationclick", (event) => {
  event.notification.close()
  const targetUrl =
    String(event.notification?.data?.url || "/metalworks-chat/").trim() || "/metalworks-chat/"

  event.waitUntil(
    clients.matchAll({ type: "window", includeUncontrolled: true }).then((clientList) => {
      for (const client of clientList) {
        if ("focus" in client && client.url.includes("/metalworks-chat/")) {
          client.navigate(targetUrl).catch(() => null)
          return client.focus()
        }
      }

      if (clients.openWindow) {
        return clients.openWindow(targetUrl)
      }

      return null
    }),
  )
})
