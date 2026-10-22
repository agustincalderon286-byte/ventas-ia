/* TEC's own document surface: never navigate an installed PWA to a raw PDF/blob. */
(() => {
  const VENDOR = "/coach/vendor/pdfjs/6.4.299/";
  const CACHE = "tec-coach-documents-v1";
  const DOCUMENTS = Object.freeze({
    catalogo: { title: "Catálogo Novel", url: "/coach/resources/novel-catalog-digital-3.pdf" },
    carpeta: { title: "Carpeta de presentación Novel", url: "/coach/resources/novel-sales-presentation-2024.pdf" }
  });
  let library;
  let active;
  let dialog;
  let closingHistory = Promise.resolve();
  let finishHistory;
  const standalone = location.pathname === "/coach/document.html";

  function status(session, message) {
    if (active === session) dialog.querySelector("[data-document-status]").textContent = message;
  }

  function syncControls(session) {
    if (active !== session) return;
    const total = session.pdf?.numPages || 0;
    dialog.querySelector("[data-document-prev]").disabled = !total || session.page <= 1;
    dialog.querySelector("[data-document-next]").disabled = !total || session.page >= total;
    const input = dialog.querySelector("[data-document-page]");
    input.disabled = !total;
    input.value = session.page;
    input.max = total || 1;
    dialog.querySelector("[data-document-total]").textContent = total ? `de ${total}` : "de …";
    dialog.querySelector("[data-document-zoom]").disabled = !total;
    dialog.querySelector("[data-document-zoom]").textContent = session.zoom === 1 ? "Ampliar +" : "Ajustar ancho";
  }

  function close({ fromHistory = false } = {}) {
    const session = active;
    if (!session) return;
    active = null;
    session.abort.abort();
    session.render?.cancel();
    // Destruction is asynchronous; no PDF resources survive in the UI after closing.
    session.task?.destroy().catch(() => null);
    dialog.close();
    const canvas = dialog.querySelector("canvas");
    canvas.width = canvas.height = 0;
    canvas.hidden = true;
    document.body.style.cssText = session.bodyStyle;
    window.scrollTo(0, session.scrollY);
    session.opener?.focus({ preventScroll: true });
    if (standalone) {
      location.replace("/coach/app/");
    } else if (!fromHistory && history.state?.tecDocument === session.id) {
      // Wait for this pop before permitting another open, avoiding a rapid close/open race.
      closingHistory = new Promise(resolve => { finishHistory = resolve; });
      history.back();
    }
  }

  function ensureDialog() {
    if (dialog) return;
    dialog = document.createElement("dialog");
    dialog.className = "tec-document-viewer";
    dialog.setAttribute("aria-labelledby", "tec-document-title");
    dialog.innerHTML = `
      <header class="tec-document-header">
        <div class="tec-document-topline">
          <button type="button" class="tec-document-back" data-document-close autofocus>← Volver al Coach</button>
          <h2 class="tec-document-title" id="tec-document-title"></h2>
        </div>
        <div class="tec-document-controls" aria-label="Controles del documento">
          <button type="button" data-document-prev aria-label="Página anterior">‹</button>
          <label>Página <input data-document-page type="number" inputmode="numeric" min="1" value="1" aria-label="Número de página"></label>
          <span data-document-total>de …</span>
          <button type="button" data-document-next aria-label="Página siguiente">›</button>
          <button type="button" data-document-zoom>Ampliar +</button>
        </div>
        <button type="button" class="tec-document-save" data-document-save>Guardar sin internet</button>
        <p class="tec-document-status" data-document-status role="status" aria-live="polite"></p>
      </header>
      <div class="tec-document-stage" tabindex="0" aria-label="Página del documento">
        <canvas hidden role="img" aria-label="Página del documento"></canvas>
      </div>`;
    document.body.append(dialog);
    dialog.querySelector("[data-document-close]").addEventListener("click", () => close());
    dialog.addEventListener("cancel", event => { event.preventDefault(); close(); });
    dialog.querySelector("[data-document-prev]").addEventListener("click", () => changePage(-1));
    dialog.querySelector("[data-document-next]").addEventListener("click", () => changePage(1));
    const goToPage = event => {
      if (!active?.pdf) return;
      active.page = Math.min(active.pdf.numPages, Math.max(1, Math.floor(Number(event.target.value) || 1)));
      render(active);
    };
    dialog.querySelector("[data-document-page]").addEventListener("change", goToPage);
    dialog.querySelector("[data-document-page]").addEventListener("keydown", event => {
      if (event.key === "Enter") { event.preventDefault(); goToPage(event); event.target.blur(); }
    });
    dialog.querySelector("[data-document-zoom]").addEventListener("click", () => {
      if (!active) return;
      active.zoom = active.zoom === 1 ? 2 : 1;
      render(active);
    });
    dialog.querySelector("[data-document-save]").addEventListener("click", () => saveOffline(active));
    let resizeTimer;
    window.addEventListener("resize", () => {
      clearTimeout(resizeTimer);
      resizeTimer = setTimeout(() => { if (active?.pdf) render(active); }, 160);
    });
    window.addEventListener("popstate", () => {
      if (active && history.state?.tecDocument !== active.id) close({ fromHistory: true });
      finishHistory?.();
      finishHistory = null;
    });
  }

  function changePage(delta) {
    if (!active?.pdf) return;
    active.page = Math.max(1, Math.min(active.pdf.numPages, active.page + delta));
    render(active);
  }

  async function render(session) {
    const revision = session.revision = (session.revision || 0) + 1;
    const previous = session.render;
    previous?.cancel();
    syncControls(session);
    status(session, `Cargando página ${session.page}…`);
    try {
      await previous?.promise.catch(() => null);
      if (active !== session || session.revision !== revision) return;
      session.pageProxy?.cleanup();
      const page = await session.pdf.getPage(session.page);
      if (active !== session || session.revision !== revision) return;
      session.pageProxy = page;
      const stage = dialog.querySelector(".tec-document-stage");
      const canvas = dialog.querySelector("canvas");
      const base = page.getViewport({ scale: 1 });
      const width = Math.max(240, stage.clientWidth - 20) * session.zoom;
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
        status(session, "No se pudo mostrar esta página. Intenta otra página o vuelve al Coach.");
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
      opener: document.activeElement, scrollY: window.scrollY, bodyStyle: document.body.style.cssText
    };
    active = session;
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
      const data = load ? await load(session.abort.signal) : null;
      if (active !== session) return false;
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
      await render(session);
      if (save && active === session) saveOffline(session);
      return active === session;
    } catch (error) {
      status(session, error.message && load ? error.message : "No se pudo abrir el documento. Revisa tu conexión o vuelve al Coach. Para usarlo sin internet, guárdalo primero desde este visor.");
      return false;
    }
  }

  window.TecDocuments = Object.freeze({ open, close });
  document.addEventListener("click", event => {
    const link = event.target.closest("[data-tec-document]");
    if (!link || event.defaultPrevented || event.metaKey || event.ctrlKey || event.shiftKey || event.altKey) return;
    if (!DOCUMENTS[link.dataset.tecDocument]) return;
    event.preventDefault();
    open({ documentId: link.dataset.tecDocument, save: link.hasAttribute("data-tec-document-save") });
  });
  if (standalone) open({ documentId: new URLSearchParams(location.search).get("document"), save: new URLSearchParams(location.search).has("save") });
})();
