/*!
 * Placeholder Service Worker (production-safe, WhiteNoise-friendly)
 * - Resolves missing static file error without changing templates/settings.
 * - No caching behavior; network passthrough by default.
 * - Safe if loaded as a normal <script> or actually registered as a SW.
 */

(function () {
  // If executed in window context (e.g., via <script src="...">), do nothing.
  if (typeof window !== 'undefined' && typeof document !== 'undefined') {
    return;
  }

  // In Service Worker context, keep behavior minimal and safe.
  try {
    self.addEventListener('install', function (_event) {
      // Activate immediately on install to avoid old versions lingering.
      self.skipWaiting();
    });

    self.addEventListener('activate', function (event) {
      // Become the active worker for all clients in scope.
      event.waitUntil(self.clients.claim());
    });

    // No fetch handler: the browser will use default network behavior.
  } catch (_e) {
    // Absolute no-op if anything unexpected happens.
  }
})();
