const SHELL_CACHE = "openclaw-dashboard-shell-v1";
const SHELL_ASSETS = [
  "/",
  "/public/index.html",
  "/public/styles.css",
  "/public/app.js",
  "/public/favicon.svg",
  "/public/app-icon.svg",
  "/public/site.webmanifest"
];

self.addEventListener("install", (event) => {
  event.waitUntil(
    caches.open(SHELL_CACHE).then((cache) => cache.addAll(SHELL_ASSETS)).then(() => self.skipWaiting())
  );
});

self.addEventListener("activate", (event) => {
  event.waitUntil(
    caches.keys().then((keys) =>
      Promise.all(
        keys
          .filter((key) => key !== SHELL_CACHE)
          .map((key) => caches.delete(key))
      )
    ).then(() => self.clients.claim())
  );
});

self.addEventListener("fetch", (event) => {
  const { request } = event;
  const url = new URL(request.url);

  if (request.method !== "GET") return;
  if (url.pathname.startsWith("/api/") || url.pathname === "/events") return;

  if (request.mode === "navigate") {
    event.respondWith(
      fetch(request).catch(() => caches.match("/") || caches.match("/public/index.html"))
    );
    return;
  }

  event.respondWith(
    caches.match(request).then((cached) => {
      if (cached) return cached;
      return fetch(request).then((response) => {
        const clone = response.clone();
        caches.open(SHELL_CACHE).then((cache) => cache.put(request, clone)).catch(() => {});
        return response;
      });
    })
  );
});
