(() => {
  let dialog, image, title, detail, error, opener, previousOverflow;

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
      document.body.style.overflow = previousOverflow;
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
    previousOverflow = document.body.style.overflow;
    dialog.showModal();
    document.body.style.overflow = "hidden";
  });
})();
