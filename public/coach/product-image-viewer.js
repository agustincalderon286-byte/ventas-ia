(() => {
  let dialog, image, title, detail, error, opener, scrollPosition;

  const root = document.documentElement;
  const viewport = window.visualViewport;

  function fitViewport() {
    // Keep native pinch zoom available. At normal scale WebKit can pan its
    // visual viewport independently of the page while the keyboard retracts.
    if (viewport && Math.abs(viewport.scale - 1) < 0.01) {
      dialog.style.top = viewport.offsetTop + "px";
      dialog.style.left = viewport.offsetLeft + "px";
      dialog.style.width = viewport.width + "px";
      dialog.style.height = viewport.height + "px";
    } else {
      for (const property of ["top", "left", "width", "height"]) dialog.style.removeProperty(property);
    }
  }

  function createViewer() {
    dialog = document.createElement("dialog");
    dialog.className = "tec-product-image-dialog";
    dialog.setAttribute("aria-labelledby", "tec-product-image-title");
    dialog.innerHTML = `
      <header class="tec-product-image-header">
        <div><h2 id="tec-product-image-title"></h2><p data-product-image-detail></p></div>
        <button type="button" class="tec-product-image-close" autofocus aria-label="Cerrar imagen">✕ <span>Cerrar</span></button>
      </header>
      <div class="tec-product-image-content">
        <img data-product-image-full alt="" />
        <p data-product-image-error role="status" hidden>No se pudo cargar la imagen. Cierra esta vista para volver a la calculadora.</p>
      </div>`;
    document.body.append(dialog);
    image = dialog.querySelector("[data-product-image-full]");
    title = dialog.querySelector("h2");
    detail = dialog.querySelector("[data-product-image-detail]");
    error = dialog.querySelector("[data-product-image-error]");
    dialog.querySelector("button").addEventListener("click", () => dialog.close());
    image.addEventListener("error", () => { error.hidden = false; image.hidden = true; });
    image.addEventListener("load", () => { error.hidden = true; image.hidden = false; });
    dialog.addEventListener("close", () => {
      if (dialog.open) return;
      viewport?.removeEventListener("resize", fitViewport);
      viewport?.removeEventListener("scroll", fitViewport);
      root.classList.remove("tec-image-viewer-open");
      root.style.removeProperty("--tec-image-page-top");
      root.style.removeProperty("--tec-image-page-left");
      window.scrollTo({ ...scrollPosition, behavior: "instant" });
      image.removeAttribute("src");
      image.alt = "";
      if (opener?.isConnected) opener.focus({ preventScroll: true });
      opener = null;
    });
    window.addEventListener("coach:workspace-change", () => { if (dialog.open) dialog.close(); });
  }

  document.addEventListener("click", event => {
    const button = event.target.closest?.("[data-order-calc-image-open], [data-survey-image-open], [data-program-gift-image-open]");
    if (!button) return;
    const figure = button.closest("[data-order-calc-visual], [data-survey-product-visual], [data-program-gift-visual]");
    const source = figure?.querySelector("img");
    if (!source?.getAttribute("src") || figure.hidden) return;
    if (!dialog) createViewer();
    if (dialog.open) return;
    opener = button;
    title.textContent = button.getAttribute("data-image-title") || figure.querySelector("[data-order-calc-visual-title]")?.textContent || "Producto Royal Prestige";
    detail.textContent = button.getAttribute("data-image-detail") || figure.querySelector("[data-order-calc-visual-meta]")?.textContent || "";
    const origin = button.hasAttribute("data-program-gift-image-open") ? "4 en 14"
      : button.hasAttribute("data-survey-image-open") ? "la encuesta" : "la calculadora";
    dialog.querySelector("button").setAttribute("aria-label", `Cerrar imagen y volver a ${origin}`);
    error.textContent = `No se pudo cargar la imagen. Cierra esta vista para volver a ${origin}.`;
    error.hidden = true;
    image.hidden = false;
    image.alt = source.alt || title.textContent;
    // Reuse the displayed image, including its native/offline URL and browser cache.
    image.src = source.currentSrc || source.src;
    scrollPosition = { top: window.scrollY, left: window.scrollX };
    root.style.setProperty("--tec-image-page-top", -scrollPosition.top + "px");
    root.style.setProperty("--tec-image-page-left", -scrollPosition.left + "px");
    root.classList.add("tec-image-viewer-open");
    dialog.showModal();
    fitViewport();
    viewport?.addEventListener("resize", fitViewport);
    viewport?.addEventListener("scroll", fitViewport);
  });
})();
