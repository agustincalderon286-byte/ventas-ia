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
  const feedbackNode = document.querySelector("[data-contact-share-feedback]");
  const submitButton = document.querySelector("[data-contact-share-submit]");

  if (!shareCode || !form || !fileInput || !textInput) {
    return;
  }

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
          importMode: detectImportMode(fileName, rawText),
          rawText,
          fileName
        }
      });

      form.reset();
      setMessage(
        feedbackNode,
        `Listo. Se procesaron ${data.parsedCount || 0} contactos. Nuevos: ${data.importedCount || 0}. Ya existentes: ${data.duplicateCount || 0}.`,
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
  });
}

initContactSharePage();
