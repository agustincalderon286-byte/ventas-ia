/* TEC's own document surface: never navigate an installed PWA to a raw PDF/blob. */
(() => {
  const VENDOR = "/coach/vendor/pdfjs/6.4.299/";
  const CACHE = "tec-coach-documents-v1";
  const PAGE_MANIFEST = "/coach/assets/documents/manifest-v1.json";
  const DOCUMENTS = Object.freeze({
    catalogo: { title: "Catálogo Novel", url: "/coach/resources/novel-catalog-digital-3.pdf" },
    carpeta: { title: "Carpeta de presentación Novel", url: "/coach/resources/novel-sales-presentation-2024.pdf" }
  });
  let library;
  let active;
  let dialog;
  let closingHistory = Promise.resolve();
  let finishHistory;
  let pendingReturn;
  let pinDialog;
  let pinRequest;
  const standalone = location.pathname === "/coach/document.html";

  function privateSupportMessage() {
    const ua = typeof navigator === "undefined" ? "" : navigator.userAgent || "";
    const ios = /(?:iPhone|iPad|iPod)/.test(ua) && ua.match(/OS (\d+)[_.]/);
    const safari = /Safari\//.test(ua) && !/(?:Chrome|Chromium|Edg|OPR|Android)\//.test(ua) && ua.match(/Version\/(\d+)/);
    if ((ios && Number(ios[1]) < 18) || (safari && Number(safari[1]) < 18)) {
      return "Este dispositivo no es compatible con el visor de PDF privados (en Apple requiere Safari/iOS 18 o posterior). Entra con tu misma cuenta y PIN desde una computadora o tableta compatible. Los catálogos públicos sí están disponibles aquí. El archivo sigue privado; no lo subas a la sección pública.";
    }
    return "";
  }

  function requestPin() {
    if (pinRequest) return Promise.resolve(null);
    if (!pinDialog) {
      pinDialog = document.createElement("dialog");
      pinDialog.className = "tec-pin-dialog";
      pinDialog.setAttribute("aria-labelledby", "tec-private-pin-title");
      pinDialog.innerHTML = `<form data-private-pin-form>
        <h2 id="tec-private-pin-title">Abrir documento privado</h2>
        <label for="tec-private-pin">PIN de 4 números</label>
        <input id="tec-private-pin" data-private-pin type="password" inputmode="numeric" pattern="[0-9]{4}" minlength="4" maxlength="4" autocomplete="off" required autofocus>
        <p>El PIN se verifica con el servidor. No se guarda en este dispositivo.</p>
        <div><button type="submit">Abrir documento</button><button type="button" data-private-pin-cancel>Cancelar</button></div>
      </form>`;
      document.body.append(pinDialog);
      const finish = pin => {
        const request = pinRequest;
        pinRequest = null;
        pinDialog.querySelector("[data-private-pin]").value = "";
        pinDialog.close();
        request?.opener?.focus({ preventScroll: true });
        request?.resolve(pin);
      };
      pinDialog.querySelector("[data-private-pin-form]").addEventListener("submit", event => {
        event.preventDefault();
        const input = pinDialog.querySelector("[data-private-pin]");
        if (!/^\d{4}$/.test(input.value)) { input.reportValidity?.(); return; }
        finish(input.value);
      });
      pinDialog.querySelector("[data-private-pin-cancel]").addEventListener("click", () => finish(null));
      pinDialog.addEventListener("cancel", event => { event.preventDefault(); finish(null); });
    }
    return new Promise((resolve, reject) => {
      pinRequest = { resolve, opener: document.activeElement };
      try { pinDialog.showModal(); } catch (error) { pinRequest = null; reject(error); }
    });
  }

  function status(session, message) {
    if (active === session) dialog.querySelector("[data-document-status]").textContent = message;
  }

  function syncControls(session) {
    if (active !== session) return;
    const total = session.total || 0;
    dialog.querySelector("[data-document-prev]").disabled = !total || session.page <= 1;
    dialog.querySelector("[data-document-next]").disabled = !total || session.page >= total;
    const input = dialog.querySelector("[data-document-page]");
    input.disabled = !total;
    input.value = session.page;
    input.max = total || 1;
    dialog.querySelector("[data-document-total]").textContent = total ? `de ${total}` : "de …";
    dialog.querySelector("[data-document-zoom]").disabled = !total;
    dialog.querySelector("[data-document-zoom]").textContent = "＋";
    dialog.querySelector("[data-document-zoom-out]").disabled = !total || session.zoom <= 1;
    dialog.querySelector("[data-document-fit]").disabled = !total;
    dialog.querySelector("[data-document-scale]").textContent = `${Math.round(session.zoom * 100)}%`;
  }

  function restorePosition(session, done) {
    const restore = () => {
      session.opener?.focus({ preventScroll: true });
      const anchor = session.opener?.getBoundingClientRect?.();
      const top = anchor && typeof session.anchorTop === "number"
        ? window.scrollY + anchor.top - session.anchorTop : session.scrollY;
      window.scrollTo({ left: 0, top: Math.max(0, top), behavior: "instant" });
    };
    restore();
    const frame = window.requestAnimationFrame || (callback => setTimeout(callback, 0));
    // Back/Forward restoration happens after popstate; restore after layout too.
    frame(() => frame(() => {
      restore();
      if (session.historyRestoration) history.scrollRestoration = session.historyRestoration;
      done?.();
    }));
  }

  function close({ fromHistory = false } = {}) {
    const session = active;
    if (!session) return;
    active = null;
    session.abort.abort();
    session.cancelImage?.();
    session.render?.cancel();
    session.task?.destroy().catch(() => null);
    dialog.close();
    const canvas = dialog.querySelector("canvas");
    canvas.width = canvas.height = 0;
    canvas.hidden = true;
    dialog.querySelector("[data-document-image]").replaceChildren();
    document.body.style.cssText = session.bodyStyle;
    session.opener?.focus({ preventScroll: true });
    if (standalone) {
      location.replace("/coach/app/#paso-demostracion");
    } else if (!fromHistory && history.state?.tecDocument === session.id) {
      closingHistory = new Promise(resolve => { finishHistory = resolve; });
      pendingReturn = session;
      window.scrollTo({ left: 0, top: session.scrollY, behavior: "instant" });
      history.back();
    } else restorePosition(session);
  }

  function ensureDialog() {
    if (dialog) return;
    dialog = document.createElement("dialog");
    dialog.className = "tec-document-viewer";
    dialog.setAttribute("aria-labelledby", "tec-document-title");
    dialog.innerHTML = `
      <header class="tec-document-header">
        <div class="tec-document-topline">
          <button type="button" class="tec-document-back" data-document-close aria-label="Volver al Coach" autofocus>← Volver</button>
          <h2 class="tec-document-title" id="tec-document-title"></h2>
        </div>
        <div class="tec-document-controls" aria-label="Controles del documento">
          <button type="button" data-document-prev aria-label="Página anterior">‹</button>
          <label>Página <input data-document-page type="number" inputmode="numeric" min="1" value="1" aria-label="Número de página"></label>
          <span data-document-total>de …</span>
          <button type="button" data-document-next aria-label="Página siguiente">›</button>
          <button type="button" data-document-zoom-out aria-label="Reducir zoom">−</button><span data-document-scale aria-live="polite">100%</span><button type="button" data-document-zoom aria-label="Ampliar zoom">＋</button><button type="button" data-document-fit>Ajustar</button>
        </div>
        <button type="button" class="tec-document-save" data-document-save aria-label="Guardar sin internet" title="Guardar sin internet">↓ Sin internet</button>
        <button type="button" data-document-retry hidden>Reintentar página</button>
        <p class="tec-document-status" data-document-status role="status" aria-live="polite"></p>
      </header>
      <div class="tec-document-stage" tabindex="0" aria-label="Página del documento">
        <canvas hidden role="img" aria-label="Página del documento"></canvas>
        <div data-document-image></div>
      </div>`;
    document.body.append(dialog);
    dialog.querySelector("[data-document-close]").addEventListener("click", () => close());
    dialog.addEventListener("cancel", event => { event.preventDefault(); close(); });
    dialog.querySelector("[data-document-prev]").addEventListener("click", () => changePage(-1));
    dialog.querySelector("[data-document-next]").addEventListener("click", () => changePage(1));
    const goToPage = event => {
      if (!active?.total) return;
      active.page = Math.min(active.total, Math.max(1, Math.floor(Number(event.target.value) || 1)));
      render(active);
    };
    dialog.querySelector("[data-document-page]").addEventListener("change", goToPage);
    dialog.querySelector("[data-document-page]").addEventListener("keydown", event => {
      if (event.key === "Enter") { event.preventDefault(); goToPage(event); event.target.blur(); }
    });
    dialog.querySelector("[data-document-zoom]").addEventListener("click", () => setZoom((active?.zoom || 1) + 1));
    dialog.querySelector("[data-document-zoom-out]").addEventListener("click", () => setZoom((active?.zoom || 1) - .5));
    dialog.querySelector("[data-document-fit]").addEventListener("click", () => setZoom(1));
    installZoomGestures(dialog.querySelector(".tec-document-stage"));
    dialog.querySelector("[data-document-save]").addEventListener("click", () => saveOffline(active));
    dialog.querySelector("[data-document-retry]").addEventListener("click", () => { if (active?.total) render(active); });
    let resizeTimer;
    window.addEventListener("resize", () => {
      clearTimeout(resizeTimer);
      resizeTimer = setTimeout(() => { if (active?.total) render(active); }, 160);
    });
    window.addEventListener("popstate", () => {
      if (active && history.state?.tecDocument !== active.id) close({ fromHistory: true });
      if (pendingReturn) {
        const session = pendingReturn; pendingReturn = null;
        const done = finishHistory; finishHistory = null;
        restorePosition(session, done);
      }
    });
  }

  async function setZoom(value, anchor = {}) {
    const session = active;
    if (!session?.total) return;
    const next = Math.max(1, Math.min(4, value));
    const stage = dialog.querySelector(".tec-document-stage");
    const x = anchor.x ?? stage.clientWidth / 2, y = anchor.y ?? (stage.clientHeight || 0) / 2;
    const left = ((stage.scrollLeft || 0) + x) * next / session.zoom - x;
    const top = ((stage.scrollTop || 0) + y) * next / session.zoom - y;
    session.zoom = next;
    await render(session);
    if (active === session) { stage.scrollLeft = Math.max(0, left); stage.scrollTop = Math.max(0, top); }
  }

  function installZoomGestures(stage) {
    let pinch = null;
    const distance = touches => Math.hypot(touches[0].clientX - touches[1].clientX, touches[0].clientY - touches[1].clientY);
    stage.addEventListener("touchstart", event => {
      if (!active?.total || event.touches.length !== 2) return;
      const rect = stage.getBoundingClientRect?.() || { left: 0, top: 0 };
      pinch = { session: active, distance: Math.max(1, distance(event.touches)), zoom: active.zoom, next: active.zoom,
        x: (event.touches[0].clientX + event.touches[1].clientX) / 2 - rect.left,
        y: (event.touches[0].clientY + event.touches[1].clientY) / 2 - rect.top };
    }, { passive: true });
    stage.addEventListener("touchmove", event => {
      if (!pinch || active !== pinch.session || event.touches.length !== 2) return;
      event.preventDefault();
      pinch.next = Math.max(1, Math.min(4, pinch.zoom * distance(event.touches) / pinch.distance));
      const media = active.pages ? dialog.querySelector("[data-document-image]").children?.[0] : dialog.querySelector("canvas");
      if (!media) return;
      const width = Math.floor(Math.max(240, stage.clientWidth - 4) * pinch.next);
      media.style.width = `${width}px`;
      media.style.height = active.pages ? "auto" : `${width * media.height / media.width}px`;
    }, { passive: false });
    const finish = event => {
      if (!pinch || event.touches.length >= 2) return;
      const current = pinch; pinch = null;
      if (active === current.session) setZoom(current.next, current);
    };
    stage.addEventListener("touchend", finish);
    stage.addEventListener("touchcancel", finish);
    stage.addEventListener("dblclick", event => {
      if (!active?.total) return;
      event.preventDefault();
      const rect = stage.getBoundingClientRect?.() || { left: 0, top: 0 };
      setZoom(active.zoom > 1 ? 1 : 2, { x: event.clientX - rect.left, y: event.clientY - rect.top });
    });
  }

  function changePage(delta) {
    if (!active?.total) return;
    active.page = Math.max(1, Math.min(active.total, active.page + delta));
    render(active);
  }

  function renderImage(session) {
    const page = session.pages[session.page - 1];
    const stage = dialog.querySelector(".tec-document-stage");
    const image = document.createElement("img");
    image.className = "tec-document-page-image";
    image.alt = `${session.title}, página ${session.page} de ${session.total}`;
    image.width = page.width;
    image.height = page.height;
    image.style.width = `${Math.floor(Math.max(240, stage.clientWidth - 4) * session.zoom)}px`;
    image.style.height = "auto";
    image.hidden = true;
    stage.scrollTop = stage.scrollLeft = 0;
    return new Promise((resolve, reject) => {
      const clear = () => { image.onload = image.onerror = null; };
      session.cancelImage = () => { clear(); image.removeAttribute("src"); resolve(false); };
      image.onload = () => { clear(); image.hidden = false; resolve(true); };
      image.onerror = () => { clear(); reject(new Error("document-page-image")); };
      // Exactly one displayed/pending image, never all PDF pages or a PDF worker.
      dialog.querySelector("[data-document-image]").replaceChildren(image);
      if (window.TecNativeOffline) {
        window.TecNativeOffline.catalogURL(page.url).then(url => {
          if (active === session && image.isConnected) image.src = url;
        }).catch(() => { if (active === session && image.isConnected) image.src = page.url; });
      } else image.src = page.url;
    });
  }

  async function render(session) {
    const revision = session.revision = (session.revision || 0) + 1;
    const previous = session.render;
    previous?.cancel();
    session.cancelImage?.();
    dialog.querySelector("[data-document-retry]").hidden = true;
    syncControls(session);
    status(session, `Cargando página ${session.page}…`);
    try {
      if (session.pages) {
        const loaded = await renderImage(session);
        if (loaded && active === session && session.revision === revision) {
          status(session, `Página ${session.page} de ${session.total}. ${session.saved ? "Disponible sin internet." : "Puedes volver al Coach en cualquier momento."}`);
        }
        return;
      }
      await previous?.promise.catch(() => null);
      if (active !== session || session.revision !== revision) return;
      session.pageProxy?.cleanup();
      const page = await session.pdf.getPage(session.page);
      if (active !== session || session.revision !== revision) return;
      session.pageProxy = page;
      const stage = dialog.querySelector(".tec-document-stage");
      const canvas = dialog.querySelector("canvas");
      const base = page.getViewport({ scale: 1 });
      const width = Math.max(240, stage.clientWidth - 4) * session.zoom;
      const viewport = page.getViewport({ scale: width / base.width });
      // One canvas only, bounded to 6MP to avoid exhausting iPhone canvas memory.
      const density = Math.min(window.devicePixelRatio || 1, 2, Math.sqrt(6_000_000 / (viewport.width * viewport.height)));
      canvas.width = Math.floor(viewport.width * density);
      canvas.height = Math.floor(viewport.height * density);
      canvas.style.width = `${Math.floor(viewport.width)}px`;
      canvas.style.height = `${Math.floor(viewport.height)}px`;
      canvas.hidden = false;
      canvas.setAttribute("aria-label", `${session.title}, página ${session.page} de ${session.pdf.numPages}`);
      stage.scrollTop = stage.scrollLeft = 0;
      session.render = page.render({
        canvasContext: canvas.getContext("2d"), viewport,
        transform: [density, 0, 0, density, 0, 0]
      });
      await session.render.promise;
      if (session.revision === revision) status(session, `Página ${session.page} de ${session.pdf.numPages}. ${session.saved ? "Disponible sin internet." : "Puedes volver al Coach en cualquier momento."}`);
    } catch (error) {
      if (active === session && session.revision === revision && error.name !== "RenderingCancelledException") {
        dialog.querySelector("[data-document-retry]").hidden = false;
        status(session, "No se pudo cargar esta página. Pulsa Reintentar página o vuelve al Coach.");
      }
    }
  }

  async function saveOffline(session) {
    if (!session?.url || active !== session || session.saving) return;
    session.saving = true;
    const button = dialog.querySelector("[data-document-save]");
    button.disabled = true;
    status(session, "Guardando documento completo… Mantén esta pantalla abierta hasta que termine.");
    try {
      if (session.pages) {
        if (window.TecNativeOffline) {
          await window.TecNativeOffline.prepareCatalogs([]);
          session.saved = true;
          status(session, "Catálogos completos guardados en la memoria de TEC para usarlos sin internet.");
          return;
        }
        const cache = await caches.open(CACHE);
        const manifestResponse = await fetch(PAGE_MANIFEST, { signal: session.abort.signal });
        if (!manifestResponse.ok || !manifestResponse.headers.get("Content-Type")?.includes("application/json")) throw new Error("invalid-manifest");
        await cache.put(PAGE_MANIFEST, manifestResponse);
        let count = 0;
        for (const page of session.pages) {
          if (session.abort.signal.aborted) throw new Error("cancelled");
          status(session, `Guardando páginas ${count + 1} de ${session.total}… Mantén el visor abierto.`);
          const response = await fetch(page.url, { credentials: "same-origin", signal: session.abort.signal });
          if (!response.ok || response.status === 206 || !response.headers.get("Content-Type")?.includes("image/jpeg")) throw new Error("invalid-page");
          await cache.put(page.url, response);
          count++;
        }
        await cache.put(session.offlineKey, new Response("complete"));
        session.saved = true;
        status(session, "Todas las páginas están guardadas en este teléfono para abrirlas sin internet.");
        return;
      }
      const response = await fetch(session.url, { credentials: "same-origin", signal: session.abort.signal });
      if (!response.ok || response.status === 206 || !response.headers.get("Content-Type")?.includes("application/pdf")) throw new Error("invalid-pdf");
      const cache = await caches.open(CACHE);
      await cache.put(session.url, response);
      session.saved = true;
      status(session, "Documento guardado en este teléfono para abrirlo sin internet.");
    } catch (error) {
      if (!session.abort.signal.aborted) status(session, "No se pudo guardar. Revisa tu conexión y el espacio disponible. Puedes seguir viendo el documento.");
    } finally {
      session.saving = false;
      if (active === session) button.disabled = false;
    }
  }

  async function open({ documentId, title, load, save = false } = {}) {
    await closingHistory;
    if (active) return false;
    ensureDialog();
    const resource = DOCUMENTS[documentId];
    if (!resource && typeof load !== "function") return false;
    const session = {
      id: `tec-doc-${Date.now()}-${Math.random()}`, title: resource?.title || title || "Documento privado",
      url: resource?.url, page: 1, zoom: 1, abort: new AbortController(),
      opener: document.activeElement, scrollY: window.scrollY, bodyStyle: document.body.style.cssText, historyRestoration: history.scrollRestoration, anchorTop: document.activeElement?.getBoundingClientRect?.().top
    };
    active = session;
    history.scrollRestoration = "manual";
    dialog.querySelector("#tec-document-title").textContent = session.title;
    const saveButton = dialog.querySelector("[data-document-save]");
    saveButton.hidden = !resource || !("caches" in window);
    saveButton.disabled = false;
    syncControls(session);
    document.body.style.position = "fixed";
    document.body.style.top = `-${session.scrollY}px`;
    document.body.style.width = "100%";
    dialog.showModal();
    if (!standalone) history.pushState({ ...history.state, tecDocument: session.id }, "", location.href);
    status(session, "Abriendo documento… Puedes volver al Coach aunque esté cargando.");
    try {
      if (!resource && privateSupportMessage()) { status(session, privateSupportMessage()); return false; }
      const data = load ? await load(session.abort.signal) : null;
      session.privateFetched = Boolean(load);
      if (active !== session) return false;
      if (resource) {
        // Public documents are rendered at build time, not on the phone. This path
        // deliberately works with no PDF.js, worker, canvas or new Safari features.
        let manifest;
        try {
          const response = await fetch(PAGE_MANIFEST, { signal: session.abort.signal, credentials: "same-origin" });
          if (!response.ok) throw new Error("document-manifest");
          manifest = await response.json();
        } catch (error) {
          if (!window.TecNativeOffline || session.abort.signal.aborted) throw error;
          manifest = await window.TecNativeOffline.catalogJSON(PAGE_MANIFEST);
        }
        if (active !== session) return false;
        const pages = manifest.documents?.[documentId]?.pages;
        const prefix = `/coach/assets/documents/${documentId}-v1/page-`;
        if (manifest.version !== 1 || !Array.isArray(pages) || !pages.length || pages.length > 200 ||
            pages.some(page => !page.url?.startsWith(prefix) || !/^\d+\.jpg$/.test(page.url.slice(prefix.length)) ||
              !(page.width > 0 && page.width <= 1600 && page.height > 0 && page.height <= 1600))) {
          throw new Error("document-manifest-invalid");
        }
        session.pages = pages;
        session.total = pages.length;
        session.offlineKey = `/coach/assets/documents/${documentId}-v1/offline-complete`;
        try { session.saved = Boolean(await (await caches.open(CACHE)).match(session.offlineKey)); } catch { /* Reading doesn't require storage permission. */ }
        if (active !== session) return false;
        await render(session);
        if (save && active === session) saveOffline(session);
        return active === session;
      }
      library ||= import(`${VENDOR}legacy/build/pdf.min.mjs`).catch(error => { library = null; throw error; });
      const pdfjs = await library;
      if (active !== session) return false;
      pdfjs.GlobalWorkerOptions.workerSrc = `${VENDOR}legacy/build/pdf.worker.min.mjs`;
      session.task = pdfjs.getDocument({
        ...(data ? { data: new Uint8Array(await data.arrayBuffer()) } : { url: session.url }),
        cMapUrl: `${VENDOR}cmaps/`, cMapPacked: true,
        standardFontDataUrl: `${VENDOR}standard_fonts/`, wasmUrl: `${VENDOR}wasm/`, iccUrl: `${VENDOR}iccs/`,
        isEvalSupported: false, disableAutoFetch: true, disableStream: true
      });
      if (active !== session) { await session.task.destroy(); return false; }
      session.pdf = await session.task.promise;
      if (active !== session) return false;
      session.total = session.pdf.numPages;
      await render(session);
      if (save && active === session) saveOffline(session);
      return active === session;
    } catch (error) {
      status(session, load ? (session.privateFetched
        ? "No se pudo mostrar el PDF privado. Prueba con tu misma cuenta y PIN desde una computadora o tableta con navegador actualizado; en Apple, Safari/iOS 18 o posterior. Tu archivo no se hizo público. Puedes volver al Coach."
        : error.message || "No se pudo obtener el archivo privado. Vuelve al Coach e intenta con conexión.")
        : "No se pudo cargar el índice del documento. Vuelve al Coach e inténtalo otra vez con conexión.");
      return false;
    }
  }

  window.TecDocuments = Object.freeze({ open, close, privateSupportMessage, requestPin });
  document.addEventListener("click", event => {
    const link = event.target.closest("[data-tec-document]");
    if (!link || event.defaultPrevented || event.metaKey || event.ctrlKey || event.shiftKey || event.altKey) return;
    if (!DOCUMENTS[link.dataset.tecDocument]) return;
    event.preventDefault();
    open({ documentId: link.dataset.tecDocument, save: link.hasAttribute("data-tec-document-save") });
  });
  if (standalone) open({ documentId: new URLSearchParams(location.search).get("document"), save: new URLSearchParams(location.search).has("save") });
})();
