(() => {
  "use strict";
  const base = "/api/coach/conexiones/sheets/";
  const reasons = { name_phone: "mismo nombre y teléfono", phone: "mismo teléfono", name: "mismo nombre" };
  let dismissActive;

  function element(tag, text, className) {
    const node = document.createElement(tag);
    if (text) node.textContent = text;
    if (className) node.className = className;
    return node;
  }

  async function open(sheet, accountId, isCurrentSeller) {
    if (!isCurrentSeller()) return;
    dismissActive?.();
    const dialog = element("dialog", "", "coach-sheets-review");
    dialog.setAttribute("aria-label", "Revisar envío a Google Sheets");
    const content = element("section");
    const status = element("p");
    status.setAttribute("role", "status");
    const close = element("button", "Cerrar", "nav-button");
    close.type = "button";
    dialog.append(element("h3", "Enviar a Google Sheets"), content, status, close);
    document.body.append(dialog);
    const listeners = new AbortController();
    function dismiss() {
      listeners.abort();
      if (dialog.open) dialog.close();
      dialog.remove();
      if (dismissActive === dismiss) dismissActive = null;
    }
    dismissActive = dismiss;
    const check = () => { if (!isCurrentSeller()) dismiss(); return dialog.isConnected && isCurrentSeller(); };
    for (const event of ["storage", "focus", "pageshow", "tec:coach-session-change"]) {
      window.addEventListener(event, check, { signal: listeners.signal });
    }
    document.addEventListener("visibilitychange", check, { signal: listeners.signal });
    dialog.addEventListener("close", dismiss, { once: true });
    close.onclick = dismiss;
    dialog.showModal();
    status.textContent = "Comprobando tu conexión…";

    let csrf;
    async function request(operation, body) {
      const url = base + operation + (body ? "" : "?accountId=" + encodeURIComponent(accountId));
      const response = await fetch(url, {
        method: body ? "POST" : "GET", credentials: "same-origin", cache: "no-store", redirect: "error",
        signal: listeners.signal,
        headers: { Accept: "application/json", "X-TEC-Account-ID": accountId, ...(body && { "Content-Type": "application/json" }) },
        ...(body && { body: JSON.stringify({ ...body, accountId, csrf }) })
      });
      const data = await response.json().catch(() => ({}));
      if (!response.ok) {
        throw Object.assign(new Error(data.error || (response.status === 403 ? "Abre esta herramienta desde la cuenta que participa en el piloto." : "No se pudo consultar tu hoja.")), {
          status: response.status, preview: data.preview, retryable: data.retryable
        });
      }
      return data;
    }
    function render(preview, notice = "", previousSelection) {
      content.replaceChildren();
      content.append(element("p", preview.title + " · " + preview.sheetTitle));
      if (preview.hostName) content.append(element("p", "Anfitrión: " + preview.hostName + (preview.hostPhone ? " · " + preview.hostPhone : "")));
      const choices = [];
      const confirm = element("button", "Confirmar guardado en Sheets", "primary-button");
      confirm.type = "button";
      const update = () => { confirm.disabled = !choices.some(c => c.input.checked); };
      for (const row of preview.rows) {
        const block = element("div", "", "coach-sheets-contact");
        const label = element("label");
        const input = document.createElement("input");
        input.type = "checkbox";
        input.checked = row.status === "new" && (!previousSelection || previousSelection.has(row.index));
        input.disabled = row.status === "duplicate";
        const kind = row.status === "new" ? "Nuevo" : row.status === "duplicate" ? "Duplicado · se omite" : "Posible duplicado · revisa antes de incluir";
        label.append(input, element("span", row.fullName + " · " + row.phone));
        block.append(label, element("small", kind));
        for (const match of row.matches || []) {
          block.append(element("small", (match.fullName ? match.fullName + " · " : "") + match.sheetTitle + " · fila " + match.row + " · " + (reasons[match.reason] || "coincidencia")));
        }
        if (row.matchCount > row.matches?.length) block.append(element("small", "Coincidencias en total: " + row.matchCount));
        choices.push({ index: row.index, input });
        input.onchange = update;
        content.append(block);
      }
      update();
      status.textContent = notice;
      content.append(confirm);
      confirm.onclick = async () => {
        if (!check() || confirm.disabled) return;
        const selected = choices.filter(c => c.input.checked).map(c => c.index);
        confirm.disabled = true;
        for (const c of choices) c.input.disabled = true;
        status.textContent = "Volviendo a comprobar los duplicados y guardando tu selección…";
        try {
          const receipt = await request("confirm", { previewId: preview.previewId, selected });
          if (!check()) return;
          if (receipt.previewId !== preview.previewId || typeof receipt.sheetTitle !== "string" ||
              receipt.addedCount !== selected.length) {
            throw new Error("No se recibió el comprobante de este envío. Reintenta con la misma selección.");
          }
          content.replaceChildren();
          status.textContent = "Guardados en " + receipt.sheetTitle + ": " + receipt.addedCount + ".";
        } catch (error) {
          if (!check()) return;
          if (error.preview) return render(error.preview, error.message, new Set(selected));
          if (error.status >= 400 && error.status < 500 && error.status !== 429 && !error.retryable) {
            content.replaceChildren();
            status.textContent = error.message;
            if (error.status === 409 || error.status === 410) {
              const refresh = element("button", "Preparar otra vista previa", "secondary-button");
              refresh.type = "button";
              refresh.onclick = () => { if (check()) open(sheet, accountId, isCurrentSeller); };
              content.append(refresh);
            }
            return;
          }
          // Retrying the same reviewed selection lets the server recover a lost receipt.
          confirm.disabled = false;
          status.textContent = error.message || "No se pudo confirmar el guardado. Reintenta con la misma selección.";
        }
      };
    }
    try {
      const context = await request("context");
      if (!check()) return;
      csrf = context.csrf;
      if (!context.connected) {
        status.textContent = "Conecta tu cuenta de Google y elige las dos pestañas de destino.";
        const link = element("a", "Abrir Mis conexiones", "secondary-button");
        link.href = "/mis-conexiones/";
        link.target = "_blank";
        link.rel = "noopener";
        content.append(link);
        return;
      }
      status.textContent = "Buscando nombres y teléfonos repetidos en las dos pestañas…";
      const kind = sheet.documentType === "tickets" ? "tickets" : "referrals";
      const preview = await request("preview", { kind, hostName:sheet.hostName, hostPhone:sheet.hostPhone,
        startDate:sheet.startDate ? String(sheet.startDate).slice(0,10) : "", referrals:sheet.referrals });
      if (check()) render(preview);
    } catch (error) {
      if (check()) status.textContent = error.message || "No se pudo cargar la vista previa.";
    }
  }
  window.TecSheetsTransfer = { open };
})();
