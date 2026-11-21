(() => {
  "use strict";

  const bindings = new WeakMap();
  const maxJpegBytes = 2 * 1024 * 1024;
  const maxReferrals = 11;
  const unreadable = "No pude abrir la foto. Elige una imagen JPG, PNG o WebP o toma otra foto.";

  function loadImage(url) {
    return new Promise((resolve, reject) => {
      const image = new Image();
      image.onload = () => resolve(image);
      image.onerror = () => reject(new Error(unreadable));
      image.src = url;
    });
  }

  function readDataUrl(blob) {
    return new Promise((resolve, reject) => {
      const reader = new FileReader();
      reader.onload = () => resolve(reader.result);
      reader.onerror = reader.onabort = () => reject(new Error(unreadable));
      reader.readAsDataURL(blob);
    });
  }

  async function preparePhoto(url, current) {
    const canvas = document.createElement("canvas");
    try {
      const image = await loadImage(url);
      const longest = Math.max(image.naturalWidth, image.naturalHeight);
      if (!longest) throw new Error(unreadable);
      let edge = Math.min(3200, longest);
      while (current()) {
        canvas.width = Math.max(1, Math.round(image.naturalWidth * edge / longest));
        canvas.height = Math.max(1, Math.round(image.naturalHeight * edge / longest));
        const ctx = canvas.getContext("2d");
        if (!ctx) throw new Error(unreadable);
        ctx.fillStyle = "white";
        ctx.fillRect(0, 0, canvas.width, canvas.height);
        ctx.drawImage(image, 0, 0, canvas.width, canvas.height);
        for (const quality of [0.88, 0.80]) {
          const jpeg = await new Promise(resolve => canvas.toBlob(resolve, "image/jpeg", quality));
          if (!current()) return "";
          if (!jpeg?.size || jpeg.type !== "image/jpeg") throw new Error(unreadable);
          if (jpeg.size <= maxJpegBytes) return await readDataUrl(jpeg);
        }
        if (edge <= 800) throw new Error("La foto supera 2 MB al prepararla. Vuelve a tomarla con la hoja dentro del encuadre.");
        edge = Math.max(800, Math.floor(edge * 0.8));
      }
      return "";
    } finally {
      canvas.width = canvas.height = 1;
    }
  }

  function node(tag, text, className) {
    const element = document.createElement(tag);
    if (text) element.textContent = text;
    if (className) element.className = className;
    return element;
  }

  function button(text, className, click) {
    const element = node("button", text, className);
    element.type = "button";
    element.addEventListener("click", click);
    return element;
  }

  function field(parent, label, value, type = "text") {
    const wrapper = node("label", "", "native-lead-field");
    const input = node("input");
    input.type = type;
    input.autocomplete = "off";
    input.value = typeof value === "string" ? value : "";
    wrapper.append(node("span", label), input);
    parent.append(wrapper);
    return input;
  }

  function checkbox(parent, label, checked = false) {
    const wrapper = node("label", "", "fourteen-photo-check");
    const input = node("input");
    input.type = "checkbox";
    input.checked = checked;
    wrapper.append(input, node("span", label));
    parent.append(wrapper);
    return input;
  }

  function verifyPhone(parent, fields, label) {
    const input = checkbox(parent, label);
    let confirmed = "";
    const values = () => JSON.stringify(Object.values(fields).map(field => field.value.trim()));
    function invalidate() {
      input.checked = false;
      confirmed = "";
    }
    for (const field of Object.values(fields)) {
      field.addEventListener("input", invalidate);
      field.addEventListener("change", invalidate);
    }
    input.addEventListener("change", () => { confirmed = input.checked ? values() : ""; });
    return { input, invalidate, matches: () => input.checked && confirmed === values() };
  }

  function reviewRow(panel, row, index, tickets, uncertain) {
    const noun = tickets ? "Boleta " : "Referido ";
    const group = node("fieldset", "", "fourteen-sheet-referral");
    group.append(node("legend", noun + index));
    const include = checkbox(group, tickets ? "Incluir esta boleta" : "Incluir este referido", true);
    if (uncertain || !row.fullName || !row.phone) {
      group.append(node("p", "Comprueba los campos poco claros. Completa el nombre y el teléfono o quita la selección.", "mini-note"));
    }
    const grid = node("div", "", "fourteen-sheet-referral-grid");
    const fields = {
      fullName: field(grid, "Nombre", row.fullName),
      phone: field(grid, "Teléfono", row.phone, "tel"),
      address: field(grid, "Dirección", row.address),
      city: field(grid, "Ciudad", row.city),
      relationship: field(grid, "Relación o parentesco", row.relationship),
      location: field(grid, "Ubicación en la foto", row.location)
    };
    group.append(grid);
    const verified = verifyPhone(group, fields, "Comparé el teléfono con el nombre en la foto y confirmé cada dígito.");
    include.addEventListener("change", () => {
      verified.invalidate();
      for (const input of [...Object.values(fields), verified.input]) input.disabled = !include.checked;
    });
    panel.append(group);
    return { label: (tickets ? "la boleta " : "el referido ") + index, group, include, fields, verified };
  }

  function phoneProblem(phone, verified, label) {
    if (!window.TecReferralSheet.validPhotoPhone(phone.value.trim())) {
      return { input: phone, message: "Completa el teléfono en " + label + " con el código de área y sus 10 dígitos, o con + y el código del país si es internacional. Si está tapado, exclúyelo." };
    }
    if (!verified.matches()) {
      return { input: verified.input, message: "Compara cada dígito del teléfono en " + label + " con la foto y marca su confirmación." };
    }
    return null;
  }

  function bind({ form, container = form, readPhoto, apply, exportTickets, saveTickets, openLeads, createRequestId, isCurrentSeller }) {
    bindings.get(form)?.destroy();
    const gallery = container.querySelector("[data-fourteen-photo]");
    const camera = container.querySelector("[data-fourteen-photo-camera]");
    const read = container.querySelector("[data-fourteen-photo-read]");
    const preview = container.querySelector("[data-fourteen-photo-preview]");
    const status = container.querySelector("[data-fourteen-photo-status]");
    if (!gallery || !read || !preview || !status || typeof isCurrentSeller !== "function") return;

    const pickers = [gallery, camera].filter(Boolean);
    const listeners = [];
    let revision = 0;
    let visit = "";
    let dataUrl = "";
    let originalUrl = "";
    let removePhoto;
    let active = true;
    function listen(element, type, handler) {
      element.addEventListener(type, handler);
      listeners.push(() => element.removeEventListener(type, handler));
    }
    function say(message, state = "info") {
      status.textContent = message;
      status.dataset.state = state;
      status.hidden = !message;
    }
    function clear(keepPicker) {
      revision++;
      visit = "";
      dataUrl = "";
      if (originalUrl) URL.revokeObjectURL(originalUrl);
      originalUrl = "";
      read.disabled = true;
      for (const picker of pickers) {
        picker.disabled = false;
        if (picker !== keepPicker) picker.value = "";
      }
      removePhoto = null;
      preview.replaceChildren();
      preview.hidden = true;
      say("");
    }
    function reset() { clear(); }
    function current(id) {
      if (!active || id !== revision) return false;
      if (isCurrentSeller() && visit === (form.dataset.tecVisitId || "")) return true;
      reset();
      return false;
    }
    function checkOwner() {
      if (!isCurrentSeller() || (visit && visit !== (form.dataset.tecVisitId || ""))) reset();
    }

    function showReview(sheet, id) {
      const panel = node("section", "", "fourteen-photo-review");
      const tickets = sheet.documentType === "tickets";
      const rows = sheet.referrals;
      const uncertain = new Set(sheet.review?.incompleteRows || []);
      const overflow = Boolean(sheet.review?.overflow);
      panel.append(node("strong", tickets ? "Revisa las boletas" : "Revisa la hoja de referidos"));
      if (sheet.review?.lowResolution) {
        panel.append(node("p", "La resolución es baja. Si no distingues cada dígito al ampliar la foto, toma otra más cerca y con buena luz.", "mini-note"));
      }
      if (tickets) {
        panel.append(node("p", "Si las boletas se superponen, verifica que cada nombre y teléfono aparezcan en la misma boleta. Excluye las que tengan números tapados.", "mini-note"));
        if (overflow) panel.append(node("p", "Quedaron boletas fuera de la lectura. Toma otra foto de las restantes.", "mini-note"));
      } else if (overflow || rows.length > maxReferrals) {
        panel.append(node("p", "La foto contiene más de 11 referidos. La hoja admite 11. Registra los restantes en otra hoja.", "mini-note"));
      } else if (rows.length === maxReferrals) {
        panel.append(node("p", "Se alcanzó el límite de 11 referidos por hoja.", "mini-note"));
      }
      panel.append(node("p", "Deja vacíos los datos opcionales que no aparezcan en la foto.", "mini-note"));
      let host;
      if (!tickets) {
        const grid = node("div", "", "fourteen-sheet-referral-grid");
        const fields = {
          hostName: field(grid, "Nombre del anfitrión", sheet.hostName),
          hostPhone: field(grid, "Teléfono del anfitrión", sheet.hostPhone, "tel")
        };
        panel.append(grid);
        const verified = verifyPhone(panel, fields, "Comparé y confirmé cada dígito del teléfono del anfitrión.");
        const syncHostCheck = () => { verified.input.parentElement.hidden = !fields.hostPhone.value.trim(); };
        syncHostCheck();
        fields.hostPhone.addEventListener("input", syncHostCheck);
        fields.hostPhone.addEventListener("change", syncHostCheck);
        panel.append(node("p", "Si ya capturaste al anfitrión en el formulario, puedes dejar estos campos vacíos.", "mini-note"));
        host = { fields, verified };
      }
      const editors = rows.map((row, i) => reviewRow(panel, row, i + 1, tickets, uncertain.has(i + 1)));
      const report = problem => {
        say(problem.message, "error");
        problem.input.focus();
      };
      function collect() {
        const selected = editors.filter(editor => editor.include.checked);
        if (!tickets && selected.length > maxReferrals) return say("Selecciona hasta 11 referidos para esta hoja.", "error");
        if (tickets && !selected.length) return say("Selecciona al menos una boleta con nombre y teléfono completos.", "error");
        const result = { documentType: sheet.documentType, referrals: [] };
        for (const row of selected) {
          if (!row.fields.fullName.value.trim() || !row.fields.phone.value.trim()) {
            return report({ message: "Completa el nombre y el teléfono en " + row.label + " o quita su selección.",
              input: row.fields.fullName.value.trim() ? row.fields.phone : row.fields.fullName });
          }
          const problem = phoneProblem(row.fields.phone, row.verified, row.label);
          if (problem) return report(problem);
          result.referrals.push(Object.fromEntries(Object.entries(row.fields).map(([key, input]) => [key, input.value.trim()])));
        }
        if (host) {
          result.hostName = host.fields.hostName.value.trim();
          result.hostPhone = host.fields.hostPhone.value.trim();
          if (!result.hostName && !result.hostPhone && !selected.length) return say("Incluye un referido o un dato del anfitrión.", "error");
          if (result.hostPhone) {
            const problem = phoneProblem(host.fields.hostPhone, host.verified, "el anfitrión");
            if (problem) return report(problem);
          }
        }
        return result;
      }
      const excludedCopy = count => count ? (tickets ? " Boletas que dejaste fuera: " : " Referidos que dejaste fuera: ") + count + "." : "";
      const remainingCopy = overflow || (!tickets && rows.length > maxReferrals)
        ? (tickets ? " Toma otra foto de las boletas restantes." : " Revisa en la foto cuáles quedan para otra hoja.") : "";
      function transfer() {
        if (!current(id) || confirm.disabled) return;
        const result = collect();
        if (!result) return;
        try {
          const feedback = tickets ? exportTickets(result) : apply(result);
          if (tickets ? feedback?.exported === false : !feedback?.applied) {
            return say(feedback?.message || "Revisa los datos y vuelve a intentarlo.", "error");
          }
          if (!tickets) reset();
          let message = feedback?.message || (tickets ? "CSV de boletas preparado." : "Datos incorporados al formulario.");
          message += excludedCopy(editors.length - result.referrals.length) + remainingCopy;
          if (!tickets) message += " Comprueba la hoja y pulsa Guardar en TEC o Descargar CSV cuando esté lista.";
          say(message, "success");
        } catch (error) {
          say(error.message || "Revisa los datos y vuelve a intentarlo.", "error");
        }
      }
      const actions = node("div", "", "dashboard-actions");
      const confirm = button(tickets ? "Descargar CSV de boletas" : "Pasar al formulario", "secondary-button", transfer);
      if (tickets && saveTickets) {
        let previousBatch = "", requestId = "", saved = false;
        const openFolder = button("Ver mis leads", "primary-button", () => {
          if (saved && current(id)) openLeads?.();
        });
        openFolder.hidden = true;
        const save = button("Guardar en mis leads", "primary-button", async () => {
          if (!current(id) || save.disabled || saved) return;
          if (editors.filter(row => row.include.checked).length > maxReferrals) {
            return say("Selecciona hasta 11 boletas por guardado. Puedes descargar todas las boletas revisadas en CSV.", "error");
          }
          const result = collect();
          if (!result) return;
          const batch = JSON.stringify(result.referrals);
          if (batch !== previousBatch) {
            requestId = createRequestId();
            previousBatch = batch;
          }
          const rows = editors.map(editor => editor.group);
          for (const element of [...rows, ...pickers, removePhoto, save, confirm]) element.disabled = true;
          save.dataset.loading = "true";
          say("Guardando las boletas seleccionadas en Mis leads…");
          try {
            const receipt = await saveTickets(result, requestId);
            if (!current(id)) return;
            if (!Number.isInteger(receipt?.createdCount) || receipt.createdCount < 0 ||
                !Number.isInteger(receipt.duplicateCount) || receipt.duplicateCount < 0 ||
                receipt.createdCount + receipt.duplicateCount !== result.referrals.length) {
              throw new Error("No recibí la confirmación de TEC. Reintenta con la misma selección.");
            }
            saved = true;
            save.textContent = "Boletas procesadas";
            if (openLeads) {
              save.hidden = true;
              openFolder.hidden = false;
            }
            say("Leads nuevos: " + receipt.createdCount + ". Boletas repetidas: " + receipt.duplicateCount + "."
              + excludedCopy(editors.length - result.referrals.length) + remainingCopy, "success");
          } catch (error) {
            if (current(id)) say(error.message || "No pude confirmar el guardado. Inténtalo de nuevo.", "error");
          } finally {
            if (current(id)) {
              for (const element of [...pickers, removePhoto, confirm]) element.disabled = false;
              for (const row of rows) row.disabled = saved;
              save.disabled = saved;
              delete save.dataset.loading;
            }
          }
        });
        actions.append(save, openFolder);
      }
      actions.append(confirm);
      panel.append(actions);
      preview.append(panel);
    }

    async function choose(picker, event) {
      event.stopPropagation();
      const file = picker.files?.[0];
      if (!file) return;
      clear(picker);
      visit = form.dataset.tecVisitId || "";
      const id = revision;
      if (!current(id)) return;
      say("Preparando la foto…");
      try {
        if (!file.size || (file.type && (!file.type.startsWith("image/") || file.type === "image/svg+xml"))) {
          throw new Error(unreadable);
        }
        originalUrl = URL.createObjectURL(file);
        const result = await preparePhoto(originalUrl, () => current(id));
        if (!current(id)) return;
        if (!result?.startsWith("data:image/jpeg;base64,")) throw new Error(unreadable);
        dataUrl = result;
        const image = node("img", "", "fourteen-photo-image");
        image.src = result;
        image.alt = "Foto seleccionada para comparar con la lectura";
        const viewport = node("div", "", "fourteen-photo-zoom");
        viewport.append(image);
        let enlarged = false;
        const zoom = button("Ampliar foto original", "nav-button", () => {
          if (!current(id)) return;
          enlarged = !enlarged;
          image.src = enlarged ? originalUrl : dataUrl;
          image.className = enlarged ? "fourteen-photo-original" : "fourteen-photo-image";
          zoom.setAttribute("aria-expanded", String(enlarged));
          zoom.textContent = enlarged ? "Reducir foto" : "Ampliar foto original";
        });
        zoom.setAttribute("aria-expanded", "false");
        removePhoto = button("Quitar foto", "nav-button", () => { if (!removePhoto?.disabled && current(id)) reset(); });
        preview.replaceChildren(zoom, viewport, removePhoto);
        preview.hidden = false;
        read.disabled = false;
        say("Foto lista. Pulsa " + read.textContent.trim() + " para extraer los datos.");
      } catch (error) {
        if (current(id)) {
          reset();
          say(error.message || unreadable, "error");
        }
      }
    }
    async function startRead() {
      const id = revision;
      if (!dataUrl || read.disabled || !current(id)) return;
      read.disabled = true;
      say("Leyendo la hoja…");
      try {
        const sheet = await readPhoto(dataUrl.slice("data:image/jpeg;base64,".length));
        if (!current(id)) return;
        if (!["referrals", "tickets"].includes(sheet?.documentType)) {
          throw new Error("No pude identificar una hoja de referidos ni boletas. Acerca la cámara, separa los documentos y vuelve a tomar la foto.");
        }
        if (!Array.isArray(sheet.referrals) || (!sheet.referrals.length && (sheet.documentType === "tickets" || (!sheet.hostName && !sheet.hostPhone)))) {
          throw new Error("No se distinguieron datos. Elige una foto mejor iluminada o escribe los datos a mano.");
        }
        showReview(sheet, id);
        say("Amplía la foto, compara los teléfonos y marca cada número que hayas confirmado.");
      } catch (error) {
        if (current(id)) {
          say(error.message || "No pude leer la foto. Vuelve a intentarlo.", "error");
          read.disabled = false;
        }
      }
    }
    for (const picker of pickers) {
      listen(picker, "input", event => event.stopPropagation());
      listen(picker, "change", event => choose(picker, event));
    }
    listen(preview, "input", event => event.stopPropagation());
    listen(preview, "change", event => event.stopPropagation());
    listen(preview, "keydown", event => {
      if (event.key === "Enter" && !event.isComposing && event.target.tagName === "INPUT") event.preventDefault();
    });
    listen(read, "click", startRead);
    listen(form, "reset", reset);
    listen(window, "pagehide", reset);
    listen(window, "storage", checkOwner);
    listen(window, "tec:coach-session-change", checkOwner);
    listen(window, "focus", checkOwner);
    listen(document, "visibilitychange", checkOwner);
    reset();
    const binding = {
      reset() { if (active) reset(); },
      destroy() {
        if (!active) return;
        active = false;
        reset();
        listeners.forEach(remove => remove());
        bindings.delete(form);
      }
    };
    bindings.set(form, binding);
    return binding;
  }

  window.TecReferralPhoto = { bind };
})();
