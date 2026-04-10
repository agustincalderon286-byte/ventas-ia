const TRANSIENT_STATUS_CODES = new Set([502, 503, 504]);
const GET_RETRY_DELAYS_MS = [450, 1100, 2200];
const PROSPECTOR_DRAFT_STORAGE_KEY = "cmwf_prospector_draft_v1";
const MAX_LEAD_PHOTOS = 8;

function wait(ms) {
  return new Promise((resolve) => {
    window.setTimeout(resolve, ms);
  });
}

function createApiError(message = "", status = 0, retryable = false) {
  const error = new Error(message || "No pude completar esa accion.");
  error.status = status;
  error.retryable = retryable;
  return error;
}

function readStoredJson(key, fallback = null) {
  try {
    const raw = window.localStorage.getItem(key);

    if (!raw) {
      return fallback;
    }

    const parsed = JSON.parse(raw);
    return parsed ?? fallback;
  } catch {
    return fallback;
  }
}

function writeStoredJson(key, value) {
  try {
    if (value == null) {
      window.localStorage.removeItem(key);
      return;
    }

    window.localStorage.setItem(key, JSON.stringify(value));
  } catch {}
}

async function apiRequest(url, options = {}) {
  const method = String(options.method || "GET").toUpperCase();
  const retryDelays =
    Array.isArray(options.retryDelays) && options.retryDelays.length
      ? options.retryDelays
      : method === "GET"
        ? GET_RETRY_DELAYS_MS
        : [];

  const config = {
    method,
    credentials: "same-origin",
    headers: {
      ...(options.body ? { "Content-Type": "application/json" } : {}),
      ...(options.headers || {}),
    },
  };

  if (options.body) {
    config.body = JSON.stringify(options.body);
  }

  for (let attempt = 0; attempt <= retryDelays.length; attempt += 1) {
    try {
      const response = await fetch(url, config);
      const contentType = String(response.headers.get("content-type") || "").toLowerCase();
      const data = contentType.includes("application/json")
        ? await response.json().catch(() => ({}))
        : {};

      if (!response.ok) {
        throw createApiError(
          data.error ||
            (response.status === 401
              ? "Necesitas iniciar sesion."
              : "No pude completar esa accion."),
          response.status,
          TRANSIENT_STATUS_CODES.has(response.status),
        );
      }

      return data;
    } catch (error) {
      const status = Number(error?.status || 0);
      const retryable =
        Boolean(error?.retryable) ||
        TRANSIENT_STATUS_CODES.has(status) ||
        error instanceof TypeError;

      if (attempt < retryDelays.length && retryable) {
        await wait(retryDelays[attempt]);
        continue;
      }

      if (error instanceof Error) {
        throw error;
      }

      throw createApiError("No pude completar esa accion.", status, retryable);
    }
  }

  throw createApiError("No pude completar esa accion.");
}

function escapeHtml(value = "") {
  return String(value || "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;");
}

function formatDate(value = "") {
  if (!value) {
    return "";
  }

  const date = new Date(value);

  if (Number.isNaN(date.getTime())) {
    return "";
  }

  return date.toLocaleString("en-US", {
    month: "short",
    day: "numeric",
    hour: "numeric",
    minute: "2-digit",
  });
}

const state = {
  auth: null,
  dashboard: null,
  photos: [],
  submitting: false,
};

const form = document.querySelector("[data-prospector-form]");
const feedback = document.querySelector("[data-prospector-feedback]");
const statusNode = document.querySelector("[data-prospector-status]");
const userChip = document.querySelector("[data-prospector-user-chip]");
const summaryWrap = document.querySelector("[data-prospector-summary]");
const recentLeadsWrap = document.querySelector("[data-prospector-recent-leads]");
const lastSyncNode = document.querySelector("[data-prospector-last-sync]");
const refreshButton = document.querySelector("[data-prospector-refresh]");
const logoutButton = document.querySelector("[data-prospector-logout]");
const submitButton = document.querySelector("[data-prospector-submit]");
const cameraInput = document.querySelector("[data-capture-photo-input]");
const galleryInput = document.querySelector("[data-gallery-photo-input]");
const openCameraButton = document.querySelector("[data-open-camera-input]");
const openGalleryButton = document.querySelector("[data-open-gallery-input]");
const photoPreview = document.querySelector("[data-capture-photo-preview]");

const DRAFT_FIELDS = [
  "fullName",
  "phone",
  "email",
  "projectType",
  "addressLine",
  "zipCode",
  "city",
  "propertyType",
  "projectSize",
  "timeline",
  "ownershipStatus",
  "budgetRange",
  "urgency",
  "bestContactWindow",
  "preferredLanguage",
  "qualificationTier",
  "notes",
  "qualificationNotes",
];

function setStatus(message = "", tone = "muted") {
  if (!statusNode) {
    return;
  }

  statusNode.hidden = !message;
  statusNode.textContent = message;
  statusNode.dataset.tone = tone;
}

function setFeedback(message = "", tone = "") {
  if (!feedback) {
    return;
  }

  feedback.textContent = message;
  feedback.dataset.tone = tone;
}

function setSubmitting(isSubmitting) {
  state.submitting = Boolean(isSubmitting);

  if (submitButton) {
    submitButton.disabled = state.submitting;
    submitButton.textContent = state.submitting
      ? "Guardando..."
      : "Guardar lead premium";
  }
}

function setUserChip(auth = null) {
  if (!userChip) {
    return;
  }

  if (!auth?.email) {
    userChip.textContent = "Sin sesion";
    return;
  }

  userChip.textContent = `${auth.name || "Prospectador"} · ${auth.email}`;
}

function renderSummary(summary = {}) {
  if (!summaryWrap) {
    return;
  }

  const metrics = [
    {
      label: "Total",
      value: Number(summary.totalLeads || 0),
      note: "Leads enviados",
    },
    {
      label: "Nuevos",
      value: Number(summary.newLeads || 0),
      note: "Aun sin trabajar",
    },
    {
      label: "Contactados",
      value: Number(summary.contactedLeads || 0),
      note: "Ventas ya hablo",
    },
    {
      label: "Cotizados",
      value: Number(summary.quotedLeads || 0),
      note: "Ya recibieron precio",
    },
    {
      label: "Ganados",
      value: Number(summary.wonLeads || 0),
      note: "Trabajos cerrados",
    },
  ];

  summaryWrap.innerHTML = metrics
    .map(
      (item) => `
        <article class="crm-metric-card">
          <span>${escapeHtml(item.label)}</span>
          <strong>${escapeHtml(String(item.value))}</strong>
          <small>${escapeHtml(item.note)}</small>
        </article>
      `,
    )
    .join("");
}

function renderRecentLeads(leads = []) {
  if (!recentLeadsWrap) {
    return;
  }

  if (!Array.isArray(leads) || !leads.length) {
    recentLeadsWrap.innerHTML = `
      <div class="crm-prospector-empty">
        Aun no hay leads enviados desde esta cuenta.
      </div>
    `;
    return;
  }

  recentLeadsWrap.innerHTML = leads
    .map((lead) => {
      const photoCount = Array.isArray(lead.photoFileNames) ? lead.photoFileNames.length : 0;

      return `
        <article class="crm-mini-lead-card">
          <div class="crm-lead-card-head">
            <div>
              <h3>${escapeHtml(lead.fullName || "Lead sin nombre")}</h3>
              <div class="crm-lead-card-meta">
                <span>${escapeHtml(lead.projectType || "Sin servicio")}</span>
                <span>${escapeHtml(lead.addressLine || lead.location || "Sin direccion")}</span>
                <span>${escapeHtml(formatDate(lead.createdAt) || "Sin fecha")}</span>
              </div>
            </div>
            <span class="crm-status-badge" data-status="${escapeHtml(lead.status || "new")}">
              ${escapeHtml(lead.statusLabel || "New")}
            </span>
          </div>
          <div class="crm-micro-list">
            ${lead.zipCode ? `<span class="crm-chip">${escapeHtml(lead.zipCode)}</span>` : ""}
            ${lead.qualificationTier ? `<span class="crm-chip">Tier ${escapeHtml(lead.qualificationTier)}</span>` : ""}
            ${photoCount ? `<span class="crm-chip">${escapeHtml(String(photoCount))} photos</span>` : ""}
          </div>
        </article>
      `;
    })
    .join("");
}

function renderDashboard(snapshot = null) {
  state.dashboard = snapshot;
  renderSummary(snapshot?.summary || {});
  renderRecentLeads(snapshot?.recentLeads || []);

  if (lastSyncNode) {
    lastSyncNode.textContent = `Actualizado ${formatDate(new Date().toISOString())}`;
  }
}

function serializeDraft() {
  if (!form) {
    return {};
  }

  return DRAFT_FIELDS.reduce((accumulator, fieldName) => {
    accumulator[fieldName] = String(form.elements[fieldName]?.value || "");
    return accumulator;
  }, {});
}

function saveDraft() {
  if (!form) {
    return;
  }

  writeStoredJson(PROSPECTOR_DRAFT_STORAGE_KEY, {
    savedAt: Date.now(),
    data: serializeDraft(),
  });
}

function restoreDraft() {
  if (!form) {
    return;
  }

  const entry = readStoredJson(PROSPECTOR_DRAFT_STORAGE_KEY, null);
  const draft = entry?.data;

  if (!draft || typeof draft !== "object") {
    return;
  }

  let restored = false;

  DRAFT_FIELDS.forEach((fieldName) => {
    const element = form.elements[fieldName];

    if (!element) {
      return;
    }

    const nextValue = String(draft[fieldName] || "");

    if (!nextValue) {
      return;
    }

    element.value = nextValue;
    restored = true;
  });

  if (restored) {
    setStatus("Restauramos tu borrador local en este dispositivo.", "muted");
  }
}

function clearDraft() {
  writeStoredJson(PROSPECTOR_DRAFT_STORAGE_KEY, null);
}

function resetFormAfterSubmit() {
  if (!form) {
    return;
  }

  form.reset();
  form.elements.city.value = "Chicago";
  form.elements.urgency.value = "medium";
  form.elements.preferredLanguage.value = "Bilingual";
  state.photos = [];
  renderCapturePhotoPreview();
  clearDraft();
}

function fileToDataUrl(file) {
  return new Promise((resolve, reject) => {
    const reader = new FileReader();
    reader.onload = () => resolve(String(reader.result || ""));
    reader.onerror = () => reject(new Error("No pude leer una foto."));
    reader.readAsDataURL(file);
  });
}

function loadImageFromDataUrl(dataUrl) {
  return new Promise((resolve, reject) => {
    const image = new Image();
    image.onload = () => resolve(image);
    image.onerror = () => reject(new Error("No pude procesar una foto."));
    image.src = dataUrl;
  });
}

async function compressImageFile(file) {
  const originalDataUrl = await fileToDataUrl(file);
  const image = await loadImageFromDataUrl(originalDataUrl);
  const maxSide = 1280;
  const scale = Math.min(1, maxSide / Math.max(image.width || maxSide, image.height || maxSide));
  const width = Math.max(1, Math.round((image.width || maxSide) * scale));
  const height = Math.max(1, Math.round((image.height || maxSide) * scale));
  const canvas = document.createElement("canvas");
  canvas.width = width;
  canvas.height = height;
  const context = canvas.getContext("2d");

  if (!context) {
    return {
      fileName: file.name || "photo.jpg",
      mimeType: file.type || "image/jpeg",
      dataUrl: originalDataUrl,
    };
  }

  context.drawImage(image, 0, 0, width, height);
  const dataUrl = canvas.toDataURL("image/jpeg", 0.78);

  return {
    fileName: file.name || "photo.jpg",
    mimeType: "image/jpeg",
    dataUrl,
  };
}

async function readLeadPhotos(fileList) {
  const files = Array.from(fileList || []).slice(0, MAX_LEAD_PHOTOS);
  const photos = [];

  for (const file of files) {
    photos.push(await compressImageFile(file));
  }

  return photos;
}

function mergePreparedPhotos(existingPhotos = [], incomingPhotos = []) {
  const merged = [...existingPhotos];

  incomingPhotos.forEach((photo) => {
    const duplicate = merged.some((item) => item.dataUrl === photo.dataUrl);

    if (!duplicate && merged.length < MAX_LEAD_PHOTOS) {
      merged.push(photo);
    }
  });

  return merged.slice(0, MAX_LEAD_PHOTOS);
}

function renderCapturePhotoPreview() {
  if (!photoPreview) {
    return;
  }

  if (!Array.isArray(state.photos) || !state.photos.length) {
    photoPreview.innerHTML = `
      <div class="crm-prospector-empty">
        Agrega fotos del proyecto para que ventas tenga contexto visual real.
      </div>
    `;
    return;
  }

  photoPreview.innerHTML = state.photos
    .map(
      (photo, index) => `
        <figure class="crm-capture-photo-card">
          <img
            src="${photo.dataUrl}"
            alt="${escapeHtml(photo.fileName || "Foto del proyecto")}"
            loading="lazy"
          />
          <figcaption>${escapeHtml(photo.fileName || "Foto del proyecto")}</figcaption>
          <button
            type="button"
            class="crm-secondary-button crm-photo-remove"
            data-remove-capture-photo="${index}"
          >
            Quitar
          </button>
        </figure>
      `,
    )
    .join("");
}

async function handlePhotoSelection(fileList) {
  try {
    const incomingPhotos = await readLeadPhotos(fileList);
    const nextPhotos = mergePreparedPhotos(state.photos, incomingPhotos);

    if (incomingPhotos.length + state.photos.length > nextPhotos.length) {
      setFeedback("Solo se guardaron las primeras 8 fotos.", "muted");
    }

    state.photos = nextPhotos;
    renderCapturePhotoPreview();
  } catch (error) {
    setFeedback(error.message || "No pude preparar esas fotos.", "error");
  } finally {
    if (cameraInput) {
      cameraInput.value = "";
    }

    if (galleryInput) {
      galleryInput.value = "";
    }
  }
}

function buildPayload() {
  if (!form) {
    return {};
  }

  const formData = new FormData(form);
  const notes = String(formData.get("notes") || "").trim();

  return {
    fullName: String(formData.get("fullName") || "").trim(),
    phone: String(formData.get("phone") || "").trim(),
    email: String(formData.get("email") || "").trim(),
    projectType: String(formData.get("projectType") || "").trim(),
    addressLine: String(formData.get("addressLine") || "").trim(),
    zipCode: String(formData.get("zipCode") || "").trim(),
    city: String(formData.get("city") || "").trim(),
    propertyType: String(formData.get("propertyType") || "").trim(),
    projectSize: String(formData.get("projectSize") || "").trim(),
    timeline: String(formData.get("timeline") || "").trim(),
    ownershipStatus: String(formData.get("ownershipStatus") || "").trim(),
    budgetRange: String(formData.get("budgetRange") || "").trim(),
    urgency: String(formData.get("urgency") || "").trim(),
    bestContactWindow: String(formData.get("bestContactWindow") || "").trim(),
    preferredLanguage: String(formData.get("preferredLanguage") || "").trim(),
    qualificationTier: String(formData.get("qualificationTier") || "").trim(),
    qualificationNotes: String(formData.get("qualificationNotes") || "").trim(),
    notes,
    details: notes,
    photos: state.photos.map((photo) => ({
      fileName: photo.fileName,
      mimeType: photo.mimeType,
      dataUrl: photo.dataUrl,
    })),
  };
}

async function loadDashboard() {
  const snapshot = await apiRequest("/api/metalworks-crm/prospector/dashboard");

  state.auth = snapshot?.prospector || state.auth;
  setUserChip(state.auth);
  renderDashboard(snapshot);
  return snapshot;
}

async function init() {
  renderCapturePhotoPreview();

  try {
    const me = await apiRequest("/api/metalworks-crm/prospector/me");

    if (!me.authenticated) {
      window.location.href = "/metalworks-crm/prospector/login/";
      return;
    }

    state.auth = {
      name: me.name || "",
      email: me.email || "",
    };
    setUserChip(state.auth);
    restoreDraft();
    await loadDashboard();
  } catch (error) {
    if (Number(error?.status || 0) === 401) {
      window.location.href = "/metalworks-crm/prospector/login/";
      return;
    }

    setStatus(
      TRANSIENT_STATUS_CODES.has(Number(error?.status || 0))
        ? "El portal se esta despertando. Espera unos segundos y vuelve a intentar."
        : error.message,
      TRANSIENT_STATUS_CODES.has(Number(error?.status || 0)) ? "warning" : "error",
    );
  }
}

if (form) {
  form.addEventListener("input", () => {
    saveDraft();
  });

  form.addEventListener("submit", async (event) => {
    event.preventDefault();
    setSubmitting(true);
    setFeedback("Guardando lead...", "muted");

    try {
      const result = await apiRequest("/api/metalworks-crm/prospector/leads", {
        method: "POST",
        body: buildPayload(),
      });

      renderDashboard(result.dashboard || null);
      resetFormAfterSubmit();
      setFeedback(
        result.duplicate
          ? "Este lead ya existia y se actualizo en el CRM."
          : result.notified
            ? "Lead guardado y equipo alertado."
            : "Lead guardado directo en el CRM.",
        "success",
      );
      setStatus("El lead ya esta disponible para el equipo de ventas.", "success");
    } catch (error) {
      if (Number(error?.status || 0) === 401) {
        window.location.href = "/metalworks-crm/prospector/login/";
        return;
      }

      setFeedback(error.message, "error");
    } finally {
      setSubmitting(false);
    }
  });
}

if (refreshButton) {
  refreshButton.addEventListener("click", async () => {
    setStatus("Recargando portal...", "muted");

    try {
      await loadDashboard();
      setStatus("Panel actualizado.", "success");
    } catch (error) {
      if (Number(error?.status || 0) === 401) {
        window.location.href = "/metalworks-crm/prospector/login/";
        return;
      }

      setStatus(error.message, "error");
    }
  });
}

if (logoutButton) {
  logoutButton.addEventListener("click", async () => {
    try {
      await apiRequest("/api/metalworks-crm/prospector/logout", {
        method: "POST",
      });
    } catch {}

    window.location.href = "/metalworks-crm/prospector/login/";
  });
}

if (openCameraButton && cameraInput) {
  openCameraButton.addEventListener("click", () => {
    cameraInput.click();
  });
}

if (openGalleryButton && galleryInput) {
  openGalleryButton.addEventListener("click", () => {
    galleryInput.click();
  });
}

if (cameraInput) {
  cameraInput.addEventListener("change", async () => {
    await handlePhotoSelection(cameraInput.files);
  });
}

if (galleryInput) {
  galleryInput.addEventListener("change", async () => {
    await handlePhotoSelection(galleryInput.files);
  });
}

if (photoPreview) {
  photoPreview.addEventListener("click", (event) => {
    const removeButton = event.target.closest("[data-remove-capture-photo]");

    if (!removeButton) {
      return;
    }

    const index = Number(removeButton.getAttribute("data-remove-capture-photo"));

    if (!Number.isFinite(index) || index < 0) {
      return;
    }

    state.photos = state.photos.filter((_, photoIndex) => photoIndex !== index);
    renderCapturePhotoPreview();
  });
}

init();
