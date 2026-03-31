function setMessage(target, message, state = "info") {
  if (!target) {
    return;
  }

  target.textContent = message || "";
  target.classList.remove("is-success", "is-error");

  if (state === "success") {
    target.classList.add("is-success");
  } else if (state === "error") {
    target.classList.add("is-error");
  }
}

function clearMessage(target) {
  setMessage(target, "", "info");
}

function setButtonLoading(button, loading, label = "Procesando...") {
  if (!button) {
    return;
  }

  if (!button.dataset.defaultLabel) {
    button.dataset.defaultLabel = button.textContent;
  }

  button.disabled = loading;
  button.textContent = loading ? label : button.dataset.defaultLabel;
}

async function apiRequest(url, options = {}) {
  const response = await fetch(url, {
    method: options.method || "GET",
    headers: {
      "Content-Type": "application/json",
      ...(options.headers || {})
    },
    body: options.body ? JSON.stringify(options.body) : undefined
  });

  const data = await response.json().catch(() => ({}));

  if (!response.ok) {
    throw new Error(data.error || "No pude completar esta accion.");
  }

  return data;
}

function getShareCodeFromPath() {
  const match = window.location.pathname.match(/\/contactos\/([a-f0-9]+)\/?/i);
  return match?.[1] || "";
}

function detectImportMode(fileName = "", rawText = "") {
  const safeName = String(fileName || "").toLowerCase();
  const safeText = String(rawText || "");

  if (safeName.endsWith(".vcf") || safeName.endsWith(".vcard") || /BEGIN:VCARD/i.test(safeText)) {
    return "vcf";
  }

  if (safeName.endsWith(".csv")) {
    return "csv";
  }

  return "paste";
}

async function initContactSharePage() {
  const page = document.querySelector("[data-contact-share-page]");

  if (!page) {
    return;
  }

  const shareCode = getShareCodeFromPath();
  const recipientNode = document.querySelector("[data-contact-share-recipient]");
  const subtitleNode = document.querySelector("[data-contact-share-subtitle]");
  const form = document.querySelector("[data-contact-share-form]");
  const fileInput = document.querySelector("[data-contact-share-file]");
  const textInput = document.querySelector("[data-contact-share-text]");
  const targetSelect = document.querySelector("[data-contact-share-target]");
  const targetCopyNode = document.querySelector("[data-contact-share-target-copy]");
  const modeHintNode = document.querySelector("[data-contact-share-mode-hint]");
  const recommendationsNode = document.querySelector("[data-contact-share-recommendations]");
  const feedbackNode = document.querySelector("[data-contact-share-feedback]");
  const submitButton = document.querySelector("[data-contact-share-submit]");

  if (!shareCode || !form || !fileInput || !textInput) {
    return;
  }

  const updateTargetCopy = () => {
    const targetType = targetSelect?.value || "contacts";

    if (submitButton) {
      submitButton.textContent = targetType === "program_4_14" ? "Guardar 4 en 14" : "Guardar contactos";
      submitButton.dataset.defaultLabel = submitButton.textContent;
    }

    if (modeHintNode) {
      modeHintNode.textContent = targetType === "program_4_14" ? "CSV o texto tabular" : "CSV, VCF o texto";
    }

    if (textInput) {
      textInput.placeholder =
        targetType === "program_4_14"
          ? "Ej.\nNombre,Telefono,Nombre de Anfitrion,Relacion con el cliente,Direccion,Comentario de Telemarketing\nMaria Lopez,7735551234,Ana Ruiz,Prima,2059 Desplaines St,Quiere que la llamen el jueves"
          : "Ej.\nMaria Lopez, 7735551234, maria@email.com\nJuan Perez, 3125559876";
    }

    if (targetCopyNode) {
      targetCopyNode.textContent =
        targetType === "program_4_14"
          ? "Usa un CSV del programa 4 en 14 con anfitrion, nombre y telefono del referido. El sistema lo agrupa y lo guarda como hoja real."
          : "Usa CSV, VCF o texto simple si solo quieres guardar contactos para seguimiento.";
    }

    if (recommendationsNode) {
      recommendationsNode.innerHTML =
        targetType === "program_4_14"
          ? `
            <li>Sube un CSV con columnas como Nombre, Telefono y Nombre de Anfitrion.</li>
            <li>Tambien puedes incluir relacion, direccion, comentarios y resultado.</li>
            <li>Cada anfitrion se agrupa y se guarda en la pestaña 4 en 14.</li>
          `
          : `
            <li>Google Contacts: exporta como CSV y subelo aqui.</li>
            <li>iPhone / iCloud: exporta como VCF y subelo aqui.</li>
            <li>Si solo son pocos, pegarlos en texto tambien funciona.</li>
          `;
    }
  };

  updateTargetCopy();
  targetSelect?.addEventListener("change", () => {
    clearMessage(feedbackNode);
    updateTargetCopy();
  });

  try {
    const meta = await apiRequest(`/api/public/contact-share/${encodeURIComponent(shareCode)}`);
    if (recipientNode) {
      recipientNode.textContent = `Estos contactos se guardaran para ${meta.recipientName || "tu distribuidor"}.`;
    }
    if (subtitleNode) {
      subtitleNode.textContent = meta.ownerName
        ? `Quedaran listos para seguimiento dentro del equipo de ${meta.ownerName}.`
        : "Quedaran guardados para seguimiento dentro de Agustin 2.0.";
    }
  } catch (error) {
    setMessage(feedbackNode, error.message, "error");
    if (submitButton) {
      submitButton.disabled = true;
    }
    return;
  }

  form.addEventListener("submit", async event => {
    event.preventDefault();
    clearMessage(feedbackNode);

    const file = fileInput.files?.[0] || null;
    const pastedText = String(textInput.value || "").trim();

    if (!file && !pastedText) {
      setMessage(feedbackNode, "Sube un archivo o pega contactos antes de guardar.", "error");
      return;
    }

    if (file && file.size > 5 * 1024 * 1024) {
      setMessage(feedbackNode, "El archivo es demasiado pesado. Usa uno de hasta 5 MB.", "error");
      return;
    }

    let rawText = pastedText;
    let fileName = "";
    const targetType = targetSelect?.value || "contacts";

    if (file) {
      rawText = await file.text();
      fileName = file.name || "";
    }

    setButtonLoading(submitButton, true, "Guardando...");

    try {
      const data = await apiRequest("/api/public/contact-share/import", {
        method: "POST",
        body: {
          shareCode,
          targetType,
          importMode: detectImportMode(fileName, rawText),
          rawText,
          fileName
        }
      });

      form.reset();
      updateTargetCopy();
      setMessage(
        feedbackNode,
        targetType === "program_4_14"
          ? `Listo. Se procesaron ${data.parsedCount || 0} hojas 4 en 14. Referencias creadas o actualizadas: ${data.createdLeadCount || 0}. Duplicados: ${data.duplicateCount || 0}.`
          : `Listo. Se procesaron ${data.parsedCount || 0} contactos. Nuevos: ${data.importedCount || 0}. Ya existentes: ${data.duplicateCount || 0}.`,
        "success"
      );
    } catch (error) {
      setMessage(feedbackNode, error.message, "error");
    } finally {
      setButtonLoading(submitButton, false);
    }
  });

  form.addEventListener("reset", () => {
    clearMessage(feedbackNode);
    window.requestAnimationFrame(() => updateTargetCopy());
  });
}

initContactSharePage();
