(() => {
  "use strict";
  function createAudioJournal({ localId, index, mimeType, writePart, finalize, onProtected = () => {} }) {
    const parts = [];
    let pending = Promise.resolve(), sequence = 0, ended = false, gap = false;
    return {
      append(blob, durationSeconds) {
        if (ended) return Promise.reject(new Error("El segmento ya terminó."));
        parts.push(blob);
        const part = { key: `${localId}:${String(index).padStart(4, "0")}:part:${String(sequence++).padStart(6, "0")}`,
          localId, index, draft: true, blob, mimeType, durationSeconds, createdAt: new Date().toISOString() };
        // Each blob keeps its original bytes and order. A timeslice is NOT a complete audio file.
        pending = pending.catch(() => null).then(() => writePart(part)).then(() => { if (!gap) onProtected(durationSeconds); })
          .catch(error => { gap = true; throw error; });
        return pending;
      },
      async finish(durationSeconds) {
        ended = true;
        await pending.catch(() => null);
        const blob = new Blob(parts, { type: mimeType });
        if (!blob.size) return null;
        await finalize({ localId, index, blob, durationSeconds, mimeType });
        parts.length = 0;
        return blob;
      },
      emergencyBlob() { return new Blob(parts, { type: mimeType }); }
    };
  }

  function offlineViewerAllowed(viewer, now = Date.now()) {
    const verified = new Date(viewer?.offlineVerifiedAt || "").getTime();
    if (!viewer?.authenticated || !viewer?.user?.email || !Number.isFinite(verified) ||
        now < verified || now - verified > 24 * 60 * 60 * 1000) return false;
    if (!(viewer.user.accessGranted || viewer.user.subscriptionActive)) return false;
    const end = new Date(viewer.user.subscriptionCurrentPeriodEnd || "").getTime();
    // Permanent test/team accounts are bounded by the same offline lease, not Stripe dates.
    if (viewer.user.subscriptionStatus !== "test_access" && viewer.user.accountType !== "seat" && Number.isFinite(end) && end <= now) return false;
    return true;
  }

  function watchUpdates(registration) {
    if (!navigator.serviceWorker.controller) return;
    let notice;
    const show = () => {
      if (!registration.waiting || notice) return;
      notice = document.createElement("aside");
      notice.className = "tec-update-notice";
      notice.setAttribute("role", "status");
      const text = document.createElement("p");
      text.textContent = "Hay una actualización lista. Puedes seguir trabajando y grabando. Se activará al cerrar todas las ventanas del Coach; los pendientes se conservan.";
      const dismiss = document.createElement("button");
      dismiss.type = "button";
      dismiss.className = "secondary-button";
      dismiss.textContent = "Seguir trabajando";
      dismiss.addEventListener("click", () => { notice.hidden = true; });
      notice.append(text, dismiss);
      document.body.append(notice);
    };
    show();
    registration.addEventListener("updatefound", () => {
      const worker = registration.installing;
      worker?.addEventListener("statechange", () => { if (worker.state === "installed") show(); });
    });
  }
  window.TecPwaSafety = Object.freeze({ createAudioJournal, offlineViewerAllowed, watchUpdates });
})();
