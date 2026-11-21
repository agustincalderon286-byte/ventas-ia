(() => {
  "use strict";

  const bindings = new WeakMap();
  const maxBase64Length = 2 * 1024 * 1024;
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

  async function preparePhoto(file, current) {
    if (!file.size || (file.type && (!file.type.startsWith("image/") || file.type === "image/svg+xml"))) {
      throw new Error(unreadable);
    }
    const url = URL.createObjectURL(file);
    const canvas = document.createElement("canvas");
    try {
      const image = await loadImage(url);
      const longest = Math.max(image.naturalWidth, image.naturalHeight);
      if (!longest) throw new Error(unreadable);
      let edge = Math.min(2400, longest);
      while (current()) {
        canvas.width = Math.max(1, Math.round(image.naturalWidth * edge / longest));
        canvas.height = Math.max(1, Math.round(image.naturalHeight * edge / longest));
        const ctx = canvas.getContext("2d");
        if (!ctx) throw new Error(unreadable);
        ctx.fillStyle = "white";
        ctx.fillRect(0, 0, canvas.width, canvas.height);
        ctx.drawImage(image, 0, 0, canvas.width, canvas.height);
        const jpeg = await new Promise(resolve => canvas.toBlob(resolve, "image/jpeg", 0.88));
        if (!jpeg?.size || jpeg.type !== "image/jpeg") throw new Error(unreadable);
        if (4 * Math.ceil(jpeg.size / 3) <= maxBase64Length) {
          return current() ? await readDataUrl(jpeg) : "";
        }
        if (edge <= 800) throw new Error("La foto supera 2 MB al prepararla. Vuelve a tomarla con la hoja dentro del encuadre.");
        edge = Math.max(800, Math.floor(edge * 0.8));
      }
      return "";
    } finally {
      URL.revokeObjectURL(url);
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

  function bind({ form, readPhoto, apply, isCurrentSeller }) {
    bindings.get(form)?.destroy();
    const gallery = form.querySelector("[data-fourteen-photo]");
    const camera = form.querySelector("[data-fourteen-photo-camera]");
    const read = form.querySelector("[data-fourteen-photo-read]");
    const preview = form.querySelector("[data-fourteen-photo-preview]");
    const status = form.querySelector("[data-fourteen-photo-status]");
    if (!gallery || !read || !preview || !status || typeof isCurrentSeller !== "function") return;

    const pickers = [gallery, camera].filter(Boolean);
    const listeners = [];
    let revision = 0;
    let visit = "";
    let dataUrl = "";
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
      read.disabled = true;
      for (const picker of pickers) if (picker !== keepPicker) picker.value = "";
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
      const rows = sheet.referrals;
      const uncertain = new Set(sheet.review?.incompleteRows || []);
      const overflow = Boolean(sheet.review?.overflow);
      const overflowCopy = "La foto contiene más de 11 referidos. La hoja admite 11. Registra los restantes en otra hoja.";
      panel.append(node("strong", "Revisa la lectura con la foto"));
      if (overflow || rows.length > maxReferrals) panel.append(node("p", overflowCopy, "mini-note"));
      else if (rows.length === maxReferrals) panel.append(node("p", "Se alcanzó el límite de 11 referidos por hoja.", "mini-note"));
      const host = node("div", "", "fourteen-sheet-referral-grid");
      const hostName = field(host, "Nombre del anfitrión", sheet.hostName);
      const hostPhone = field(host, "Teléfono del anfitrión", sheet.hostPhone, "tel");
      panel.append(host, node("p", "Si ya capturaste al anfitrión en el formulario, puedes dejar estos campos vacíos.", "mini-note"));
      const editors = rows.map((row, i) => {
        const group = node("fieldset", "", "fourteen-sheet-referral");
        const index = i + 1;
        group.append(node("legend", "Referido " + index));
        const label = node("label");
        const include = node("input");
        include.type = "checkbox";
        include.checked = true;
        label.append(include, node("span", "Incluir este referido"));
        group.append(label);
        if (uncertain.has(index) || !row.fullName || !row.phone) {
          group.append(node("p", "Comprueba los datos de este renglón. Completa el nombre y el teléfono o quita la selección.", "mini-note"));
        }
        const grid = node("div", "", "fourteen-sheet-referral-grid");
        const fullName = field(grid, "Nombre", row.fullName);
        const phone = field(grid, "Teléfono", row.phone, "tel");
        const address = field(grid, "Dirección", row.address);
        include.addEventListener("change", () => {
          for (const input of [fullName, phone, address]) input.disabled = !include.checked;
        });
        group.append(grid);
        panel.append(group);
        return { index, include, fullName, phone, address };
      });

      const confirm = button("Pasar al formulario", "secondary-button", () => {
        if (!current(id) || confirm.disabled) return;
        const selected = editors.filter(editor => editor.include.checked);
        if (selected.length > maxReferrals) {
          say("Selecciona hasta 11 referidos para esta hoja.", "error");
          return;
        }
        for (const row of selected) {
          const missing = [row.fullName, row.phone].find(input => !input.value.trim());
          if (missing) {
            say("Completa el nombre y el teléfono del referido " + row.index + " o quita su selección.", "error");
            missing.focus();
            return;
          }
        }
        const result = {
          hostName: hostName.value.trim(),
          hostPhone: hostPhone.value.trim(),
          referrals: selected.map(row => ({
            fullName: row.fullName.value.trim(), phone: row.phone.value.trim(), address: row.address.value.trim()
          }))
        };
        if (!result.hostName && !result.hostPhone && !selected.length) {
          say("Incluye al menos un referido o un dato del anfitrión para pasar al formulario.", "error");
          return;
        }
        confirm.disabled = true;
        try {
          const feedback = apply(result);
          if (!feedback?.applied) {
            say(feedback?.message || "Revisa los datos y vuelve a intentarlo.", "error");
            confirm.disabled = false;
            return;
          }
          reset();
          const excluded = editors.length - selected.length;
          const messages = [feedback.message || "Datos incorporados al formulario."];
          if (excluded) messages.push("Referidos que dejaste fuera: " + excluded + ".");
          if (overflow || rows.length > maxReferrals) messages.push("Revisa en la foto cuáles quedan para otra hoja.");
          messages.push("Comprueba la hoja y pulsa Guardar en TEC o Descargar CSV cuando esté lista.");
          say(messages.join(" "), "success");
        } catch (error) {
          say(error.message || "Revisa los datos y vuelve a intentarlo.", "error");
          confirm.disabled = false;
        }
      });
      panel.append(confirm);
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
        const result = await preparePhoto(file, () => current(id));
        if (!current(id)) return;
        if (!result?.startsWith("data:image/jpeg;base64,")) throw new Error(unreadable);
        dataUrl = result;
        const image = node("img", "", "fourteen-photo-image");
        image.src = result;
        image.alt = "Foto de la hoja 4 en 14 seleccionada";
        const remove = button("Quitar foto", "nav-button", () => { if (current(id)) reset(); });
        preview.replaceChildren(image, remove);
        preview.hidden = false;
        read.disabled = false;
        say("Foto lista. Pulsa " + read.textContent.trim() + " para extraer los datos.");
      } catch (error) {
        if (current(id)) {
          picker.value = "";
          say(error.message || unreadable, "error");
        }
      }
    }
    async function startRead() {
      const id = revision;
      if (!current(id) || !dataUrl || read.disabled) return;
      read.disabled = true;
      say("Leyendo la hoja…");
      try {
        const sheet = await readPhoto(dataUrl.slice("data:image/jpeg;base64,".length));
        if (!current(id)) return;
        if (!Array.isArray(sheet?.referrals) || (!sheet.hostName && !sheet.hostPhone && !sheet.referrals.length)) {
          throw new Error("No se distinguieron datos. Elige una foto mejor iluminada o continúa en el formulario.");
        }
        showReview(sheet, id);
        say("Compara los datos con la foto y corrige o excluye los renglones antes de pasarlos al formulario.");
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
