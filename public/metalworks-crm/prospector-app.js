const TRANSIENT_STATUS_CODES = new Set([502, 503, 504]);
const GET_RETRY_DELAYS_MS = [450, 1100, 2200];
const PROSPECTOR_DRAFT_STORAGE_KEY = "cmwf_prospector_draft_v1";
const PROSPECTOR_QUEUE_DB_NAME = "cmwf_prospector_queue_v1";
const PROSPECTOR_QUEUE_STORE_NAME = "leadQueue";
const PROSPECTOR_QUEUE_DB_VERSION = 1;
const MAX_LEAD_PHOTOS = 8;
const MAX_LEAD_PHOTO_BYTES = 2 * 1024 * 1024;
const MAX_LEAD_PHOTO_TOTAL_BYTES = 6 * 1024 * 1024;

let queueDbPromise = null;

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

function normalizeEmailValue(value = "") {
  return String(value || "").trim().toLowerCase();
}

function supportsQueueStorage() {
  return typeof window !== "undefined" && "indexedDB" in window;
}

function createClientSubmissionId() {
  if (window.crypto?.randomUUID) {
    return `prospector_${window.crypto.randomUUID()}`;
  }

  if (window.crypto?.getRandomValues) {
    const bytes = window.crypto.getRandomValues(new Uint32Array(2));
    return `prospector_${Date.now().toString(36)}_${Array.from(bytes)
      .map((item) => item.toString(36))
      .join("")}`;
  }

  return `prospector_${Date.now().toString(36)}_${Math.random().toString(36).slice(2, 10)}`;
}

function estimateDataUrlBytes(dataUrl = "") {
  const match = String(dataUrl || "").match(/^data:[^;]+;base64,([A-Za-z0-9+/=\s]+)$/i);

  if (!match?.[1]) {
    return 0;
  }

  const base64 = match[1].replace(/\s+/g, "");
  const padding = base64.endsWith("==") ? 2 : base64.endsWith("=") ? 1 : 0;
  return Math.max(0, Math.floor((base64.length * 3) / 4) - padding);
}

function openQueueDb() {
  if (!supportsQueueStorage()) {
    return Promise.reject(new Error("Este dispositivo no soporta respaldo local."));
  }

  if (!queueDbPromise) {
    queueDbPromise = new Promise((resolve, reject) => {
      const request = window.indexedDB.open(
        PROSPECTOR_QUEUE_DB_NAME,
        PROSPECTOR_QUEUE_DB_VERSION,
      );

      request.onupgradeneeded = () => {
        const db = request.result;

        if (!db.objectStoreNames.contains(PROSPECTOR_QUEUE_STORE_NAME)) {
          db.createObjectStore(PROSPECTOR_QUEUE_STORE_NAME, {
            keyPath: "localId",
          });
        }
      };

      request.onsuccess = () => resolve(request.result);
      request.onerror = () => reject(request.error || new Error("No pude abrir el respaldo local."));
    });
  }

  return queueDbPromise;
}

async function listQueuedLeadsFromDb() {
  const db = await openQueueDb();

  return new Promise((resolve, reject) => {
    const transaction = db.transaction(PROSPECTOR_QUEUE_STORE_NAME, "readonly");
    const store = transaction.objectStore(PROSPECTOR_QUEUE_STORE_NAME);
    const request = store.getAll();

    request.onsuccess = () => resolve(Array.isArray(request.result) ? request.result : []);
    request.onerror = () => reject(request.error || new Error("No pude leer el respaldo local."));
  });
}

async function putQueuedLeadInDb(item) {
  const db = await openQueueDb();

  return new Promise((resolve, reject) => {
    const transaction = db.transaction(PROSPECTOR_QUEUE_STORE_NAME, "readwrite");
    const store = transaction.objectStore(PROSPECTOR_QUEUE_STORE_NAME);
    const request = store.put(item);

    request.onsuccess = () => resolve(item);
    request.onerror = () => reject(request.error || new Error("No pude guardar el lead local."));
  });
}

async function deleteQueuedLeadFromDb(localId = "") {
  const db = await openQueueDb();

  return new Promise((resolve, reject) => {
    const transaction = db.transaction(PROSPECTOR_QUEUE_STORE_NAME, "readwrite");
    const store = transaction.objectStore(PROSPECTOR_QUEUE_STORE_NAME);
    const request = store.delete(localId);

    request.onsuccess = () => resolve();
    request.onerror = () => reject(request.error || new Error("No pude limpiar el lead sincronizado."));
  });
}

function sortQueuedLeadsForRender(leads = []) {
  return [...leads].sort((left, right) => {
    const leftTime = Date.parse(left?.updatedAt || left?.createdAt || 0) || 0;
    const rightTime = Date.parse(right?.updatedAt || right?.createdAt || 0) || 0;
    return rightTime - leftTime;
  });
}

function sortQueuedLeadsForSync(leads = []) {
  return [...leads].sort((left, right) => {
    const leftTime = Date.parse(left?.createdAt || left?.updatedAt || 0) || 0;
    const rightTime = Date.parse(right?.createdAt || right?.updatedAt || 0) || 0;
    return leftTime - rightTime;
  });
}

const state = {
  auth: null,
  dashboard: null,
  photos: [],
  submitting: false,
  queue: [],
  queueReady: false,
  queueSyncInFlight: false,
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
const offlineSummaryWrap = document.querySelector("[data-prospector-offline-summary]");
const offlineListWrap = document.querySelector("[data-prospector-offline-list]");
const syncNowButton = document.querySelector("[data-prospector-sync-now]");

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
    userChip.textContent = navigator.onLine ? "Sin sesion" : "Modo local";
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

function buildPayload(clientSubmissionId = "") {
  if (!form) {
    return {};
  }

  const formData = new FormData(form);
  const notes = String(formData.get("notes") || "").trim();

  return {
    clientSubmissionId: clientSubmissionId || createClientSubmissionId(),
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

function validatePayload(payload = {}) {
  if (
    !payload.fullName ||
    !payload.phone ||
    !payload.projectType ||
    !payload.addressLine ||
    !payload.zipCode ||
    !payload.timeline ||
    !payload.ownershipStatus ||
    !payload.details
  ) {
    return "Llena nombre, telefono, servicio, direccion, ZIP, timeline, owner y notas.";
  }

  if (!payload.qualificationTier || !payload.qualificationNotes) {
    return "Pon tier del lead y nota corta de calificacion.";
  }

  const photos = Array.isArray(payload.photos) ? payload.photos : [];

  if (photos.length > MAX_LEAD_PHOTOS) {
    return "Puedes subir hasta 8 fotos por lead.";
  }

  const totalSizeBytes = photos.reduce((sum, item) => sum + estimateDataUrlBytes(item?.dataUrl || ""), 0);

  if (
    photos.some((item) => estimateDataUrlBytes(item?.dataUrl || "") > MAX_LEAD_PHOTO_BYTES)
  ) {
    return "Cada foto debe pesar 2 MB o menos despues de comprimirse.";
  }

  if (totalSizeBytes > MAX_LEAD_PHOTO_TOTAL_BYTES) {
    return "El total de fotos es demasiado grande. Comprime o manda menos archivos.";
  }

  return "";
}

function setQueueItems(items = []) {
  state.queue = sortQueuedLeadsForRender(items);
  renderOfflineQueue();
}

function upsertQueueStateItem(item) {
  const remaining = state.queue.filter((entry) => entry.localId !== item.localId);
  setQueueItems([...remaining, item]);
}

function removeQueueStateItem(localId = "") {
  setQueueItems(state.queue.filter((item) => item.localId !== localId));
}

async function refreshQueueState() {
  if (!supportsQueueStorage()) {
    state.queueReady = false;
    state.queue = [];
    renderOfflineQueue();
    return [];
  }

  try {
    const queuedLeads = await listQueuedLeadsFromDb();
    state.queueReady = true;
    setQueueItems(queuedLeads);
    return queuedLeads;
  } catch (error) {
    state.queueReady = false;
    state.queue = [];
    renderOfflineQueue();
    return [];
  }
}

function buildQueueItem(payload = {}) {
  const now = new Date().toISOString();
  const clientSubmissionId = String(payload.clientSubmissionId || createClientSubmissionId()).trim();

  return {
    localId: `local_${clientSubmissionId}`,
    clientSubmissionId,
    prospectorEmail: normalizeEmailValue(state.auth?.email || ""),
    prospectorName: String(state.auth?.name || "").trim(),
    fullName: payload.fullName || "",
    projectType: payload.projectType || "",
    addressLine: payload.addressLine || "",
    zipCode: payload.zipCode || "",
    photoCount: Array.isArray(payload.photos) ? payload.photos.length : 0,
    payload: {
      ...payload,
      clientSubmissionId,
    },
    status: "pending",
    blockedReason: "",
    lastError: "",
    createdAt: now,
    updatedAt: now,
    lastAttemptAt: "",
  };
}

async function queueLeadPayload(payload = {}) {
  const item = buildQueueItem(payload);
  await putQueuedLeadInDb(item);
  upsertQueueStateItem(item);
  return item;
}

function getQueueStatusMeta(item = {}) {
  if (item.status === "syncing") {
    return {
      badgeStatus: "syncing",
      label: "Enviando",
      note: "Mandando al CRM ahora mismo.",
    };
  }

  if (item.status === "blocked" && item.blockedReason === "login") {
    return {
      badgeStatus: "blocked",
      label: "Login",
      note: item.lastError || "Inicia sesion otra vez para mandarlo.",
    };
  }

  if (item.status === "blocked") {
    return {
      badgeStatus: "blocked",
      label: "Revisar",
      note: item.lastError || "Este lead necesita revision antes de sincronizarse.",
    };
  }

  return {
    badgeStatus: "pending",
    label: "Pendiente",
    note: item.lastError || "Guardado en este dispositivo y listo para enviarse.",
  };
}

function canSyncQueuedLead(item = {}) {
  return item.status !== "blocked" || item.blockedReason === "login";
}

function renderOfflineQueue() {
  if (!offlineSummaryWrap || !offlineListWrap) {
    return;
  }

  if (!supportsQueueStorage()) {
    offlineSummaryWrap.innerHTML = `
      <span class="crm-chip">Sin respaldo local</span>
      <span class="crm-chip">Prueba otro navegador</span>
    `;
    offlineListWrap.innerHTML = `
      <div class="crm-prospector-empty">
        Este dispositivo no soporta la cola offline. Si se va la senal, intenta no cerrar esta pagina hasta que el lead quede enviado.
      </div>
    `;

    if (syncNowButton) {
      syncNowButton.disabled = true;
    }

    return;
  }

  if (!state.queueReady) {
    offlineSummaryWrap.innerHTML = `
      <span class="crm-chip">Abriendo respaldo local...</span>
    `;
    offlineListWrap.innerHTML = `
      <div class="crm-prospector-empty">
        Estamos preparando el respaldo local en este dispositivo.
      </div>
    `;

    if (syncNowButton) {
      syncNowButton.disabled = true;
    }

    return;
  }

  const queuedLeads = Array.isArray(state.queue) ? state.queue : [];
  const syncableCount = queuedLeads.filter(canSyncQueuedLead).length;
  const blockedCount = queuedLeads.filter((item) => item.status === "blocked").length;
  const totalPhotos = queuedLeads.reduce((sum, item) => sum + Number(item.photoCount || 0), 0);

  offlineSummaryWrap.innerHTML = [
    `<span class="crm-chip">${queuedLeads.length} pendiente${queuedLeads.length === 1 ? "" : "s"}</span>`,
    `<span class="crm-chip">${totalPhotos} foto${totalPhotos === 1 ? "" : "s"}</span>`,
    `<span class="crm-chip">${navigator.onLine ? "Con senal" : "Sin senal"}</span>`,
    blockedCount ? `<span class="crm-chip">${blockedCount} con revision</span>` : "",
  ]
    .filter(Boolean)
    .join("");

  if (syncNowButton) {
    syncNowButton.disabled = state.queueSyncInFlight || !syncableCount || !navigator.onLine;
    syncNowButton.textContent = state.queueSyncInFlight ? "Sincronizando..." : "Sincronizar ahora";
  }

  if (!queuedLeads.length) {
    offlineListWrap.innerHTML = `
      <div class="crm-prospector-empty">
        Todo lo que mandes con mala senal aparecera aqui hasta que el CRM lo reciba.
      </div>
    `;
    return;
  }

  offlineListWrap.innerHTML = queuedLeads
    .map((item) => {
      const meta = getQueueStatusMeta(item);
      const ownerLabel = item.prospectorEmail
        ? `Prospector ${escapeHtml(item.prospectorEmail)}`
        : "Prospector del dispositivo";

      return `
        <article class="crm-mini-lead-card">
          <div class="crm-lead-card-head">
            <div>
              <h3>${escapeHtml(item.fullName || "Lead pendiente")}</h3>
              <div class="crm-lead-card-meta">
                <span>${escapeHtml(item.projectType || "Sin servicio")}</span>
                <span>${escapeHtml(item.addressLine || "Sin direccion")}</span>
                <span>${escapeHtml(formatDate(item.createdAt) || "Sin fecha")}</span>
              </div>
            </div>
            <span class="crm-status-badge" data-status="${escapeHtml(meta.badgeStatus)}">
              ${escapeHtml(meta.label)}
            </span>
          </div>
          <div class="crm-micro-list">
            ${item.zipCode ? `<span class="crm-chip">${escapeHtml(item.zipCode)}</span>` : ""}
            ${item.photoCount ? `<span class="crm-chip">${escapeHtml(String(item.photoCount))} photos</span>` : ""}
            <span class="crm-chip">${ownerLabel}</span>
          </div>
          <p class="crm-queue-error">${escapeHtml(meta.note)}</p>
        </article>
      `;
    })
    .join("");
}

async function loadDashboard() {
  const snapshot = await apiRequest("/api/metalworks-crm/prospector/dashboard");

  state.auth = snapshot?.prospector || state.auth;
  setUserChip(state.auth);
  renderDashboard(snapshot);
  return snapshot;
}

async function markQueueItem(item, changes = {}) {
  const nextItem = {
    ...item,
    ...changes,
    updatedAt: new Date().toISOString(),
  };

  await putQueuedLeadInDb(nextItem);
  upsertQueueStateItem(nextItem);
  return nextItem;
}

function prioritizeQueuedLead(items = [], localId = "") {
  if (!localId) {
    return items;
  }

  const target = items.find((item) => item.localId === localId);

  if (!target) {
    return items;
  }

  return [target, ...items.filter((item) => item.localId !== localId)];
}

async function syncQueuedLeads(options = {}) {
  if (!supportsQueueStorage() || state.queueSyncInFlight) {
    return {
      syncedIds: [],
      resultsById: {},
      blockedByLogin: false,
    };
  }

  const preferredLocalId = String(options.preferredLocalId || "").trim();
  const announce = Boolean(options.announce);
  const refreshDashboardAfterSync = Boolean(options.refreshDashboard);
  const resultsById = {};
  const syncedIds = [];
  let blockedByLogin = false;
  let latestDashboard = null;

  const queuedLeads = prioritizeQueuedLead(
    sortQueuedLeadsForSync(state.queue.filter(canSyncQueuedLead)),
    preferredLocalId,
  );

  if (!queuedLeads.length) {
    renderOfflineQueue();
    return {
      syncedIds,
      resultsById,
      blockedByLogin,
    };
  }

  state.queueSyncInFlight = true;
  renderOfflineQueue();

  if (announce) {
    setStatus("La senal regreso. Estamos sincronizando los leads pendientes.", "muted");
  }

  try {
    for (const item of queuedLeads) {
      const currentUserEmail = normalizeEmailValue(state.auth?.email || "");

      if (
        item.prospectorEmail &&
        currentUserEmail &&
        item.prospectorEmail !== currentUserEmail
      ) {
        await markQueueItem(item, {
          status: "blocked",
          blockedReason: "login",
          lastError: `Este lead fue guardado por ${item.prospectorEmail}. Entra con esa cuenta para sincronizarlo.`,
          lastAttemptAt: new Date().toISOString(),
        });
        blockedByLogin = true;
        continue;
      }

      const syncingItem = await markQueueItem(item, {
        status: "syncing",
        blockedReason: "",
        lastError: "",
        lastAttemptAt: new Date().toISOString(),
      });

      try {
        const result = await apiRequest("/api/metalworks-crm/prospector/leads", {
          method: "POST",
          body: syncingItem.payload,
        });

        resultsById[item.localId] = result;
        syncedIds.push(item.localId);

        if (result?.dashboard) {
          latestDashboard = result.dashboard;
          renderDashboard(result.dashboard);
        }

        await deleteQueuedLeadFromDb(item.localId);
        removeQueueStateItem(item.localId);
      } catch (error) {
        const safeMessage = error?.message || "No pude sincronizar este lead todavia.";

        if (Number(error?.status || 0) === 401) {
          await markQueueItem(item, {
            status: "blocked",
            blockedReason: "login",
            lastError: "Tu sesion expiro. Vuelve a iniciar sesion para mandar este lead.",
            lastAttemptAt: new Date().toISOString(),
          });
          blockedByLogin = true;
          continue;
        }

        if (Number(error?.status || 0) >= 400 && Number(error?.status || 0) < 500) {
          await markQueueItem(item, {
            status: "blocked",
            blockedReason: "validation",
            lastError: safeMessage,
            lastAttemptAt: new Date().toISOString(),
          });
          continue;
        }

        await markQueueItem(item, {
          status: "pending",
          blockedReason: "",
          lastError: safeMessage,
          lastAttemptAt: new Date().toISOString(),
        });
      }
    }
  } finally {
    state.queueSyncInFlight = false;
    renderOfflineQueue();
  }

  if (refreshDashboardAfterSync && latestDashboard) {
    renderDashboard(latestDashboard);
  }

  return {
    syncedIds,
    resultsById,
    blockedByLogin,
  };
}

function buildQueuedLeadFeedback(result = null) {
  if (!result) {
    return {
      message: "Lead guardado en este dispositivo. Se mandara al CRM cuando vuelva la senal.",
      tone: "warning",
    };
  }

  return {
    message: result.duplicate
      ? "Este lead ya existia y se actualizo en el CRM."
      : result.notified
        ? "Lead guardado y equipo alertado."
        : "Lead guardado directo en el CRM.",
    tone: "success",
  };
}

function updateConnectivityStatus() {
  if (!navigator.onLine) {
    const pendingCount = Array.isArray(state.queue) ? state.queue.length : 0;
    const pendingText = pendingCount
      ? ` Ya hay ${pendingCount} lead${pendingCount === 1 ? "" : "s"} guardado${pendingCount === 1 ? "" : "s"} en este dispositivo.`
      : "";
    setStatus(
      `Sin senal. Puedes seguir capturando y el respaldo local enviara todo cuando vuelva el internet.${pendingText}`,
      "warning",
    );
    return;
  }

  if (!state.queue.length) {
    return;
  }

  setStatus(
    `Con senal otra vez. Hay ${state.queue.length} lead${state.queue.length === 1 ? "" : "s"} pendiente${state.queue.length === 1 ? "" : "s"} por sincronizar.`,
    "muted",
  );
}

async function handleQueuedSubmit() {
  const payload = buildPayload();
  const validationMessage = validatePayload(payload);

  if (validationMessage) {
    setFeedback(validationMessage, "error");
    return;
  }

  let queuedItem = null;

  try {
    queuedItem = await queueLeadPayload(payload);
  } catch (error) {
    if (!navigator.onLine) {
      setFeedback(
        "No pude guardar el respaldo local en este dispositivo. Necesitas senal o un navegador compatible.",
        "error",
      );
      setStatus(
        "El respaldo local fallo en este dispositivo. Usa otro navegador o recupera la senal antes de cerrar la pagina.",
        "error",
      );
      return;
    }

    let directResult = null;

    try {
      directResult = await apiRequest("/api/metalworks-crm/prospector/leads", {
        method: "POST",
        body: payload,
      });
    } catch (directError) {
      directError.leadQueuedLocally = false;
      throw directError;
    }

    renderDashboard(directResult.dashboard || null);
    resetFormAfterSubmit();
    const feedbackState = buildQueuedLeadFeedback(directResult);
    setFeedback(feedbackState.message, feedbackState.tone);
    setStatus("El lead ya esta disponible para el equipo de ventas.", "success");
    return;
  }

  resetFormAfterSubmit();

  if (!navigator.onLine) {
    setFeedback(
      "Lead guardado en este dispositivo. Se mandara al CRM cuando vuelva la senal.",
      "warning",
    );
    updateConnectivityStatus();
    return;
  }

  setFeedback("Lead guardado localmente. Intentando mandarlo al CRM...", "muted");

  const syncResult = await syncQueuedLeads({
    preferredLocalId: queuedItem.localId,
    refreshDashboard: true,
  });
  const leadResult = syncResult.resultsById[queuedItem.localId] || null;
  const queuedState = state.queue.find((item) => item.localId === queuedItem.localId) || null;

  if (leadResult) {
    const feedbackState = buildQueuedLeadFeedback(leadResult);
    setFeedback(feedbackState.message, feedbackState.tone);
    setStatus("El lead ya esta disponible para el equipo de ventas.", "success");
    return;
  }

  if (syncResult.blockedByLogin) {
    setFeedback(
      "Lead guardado en este dispositivo. Vuelve a iniciar sesion para enviarlo al CRM.",
      "warning",
    );
    setStatus(
      "Tu lead quedo protegido en este dispositivo, pero hace falta iniciar sesion otra vez para sincronizarlo.",
      "warning",
    );
    return;
  }

  if (queuedState?.status === "blocked" && queuedState.blockedReason === "validation") {
    setFeedback(
      queuedState.lastError || "Lead guardado localmente, pero hay que revisar los datos antes de sincronizarlo.",
      "error",
    );
    setStatus(
      "El lead quedo guardado en este dispositivo, pero necesita revision antes de enviarse al CRM.",
      "warning",
    );
    return;
  }

  setFeedback(
    "Lead guardado en este dispositivo. Se enviara al CRM en cuanto la senal vuelva estable.",
    "warning",
  );
  setStatus(
    "El respaldo local ya quedo listo. El portal reintentara la sincronizacion automaticamente.",
    "warning",
  );
}

async function init() {
  renderCapturePhotoPreview();
  restoreDraft();
  await refreshQueueState();
  updateConnectivityStatus();

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
  } catch (error) {
    if (Number(error?.status || 0) === 401) {
      window.location.href = "/metalworks-crm/prospector/login/";
      return;
    }

    setUserChip(state.auth);
    updateConnectivityStatus();
  }

  try {
    await loadDashboard();
  } catch (error) {
    if (Number(error?.status || 0) === 401) {
      window.location.href = "/metalworks-crm/prospector/login/";
      return;
    }

    setStatus(
      !navigator.onLine
        ? "Sin senal. Puedes seguir capturando y todo quedara guardado localmente."
        : TRANSIENT_STATUS_CODES.has(Number(error?.status || 0))
          ? "El portal se esta despertando. Espera unos segundos y vuelve a intentar."
          : error.message,
      !navigator.onLine || TRANSIENT_STATUS_CODES.has(Number(error?.status || 0))
        ? "warning"
        : "error",
    );
  }

  if (navigator.onLine && state.queue.some(canSyncQueuedLead)) {
    await syncQueuedLeads({
      announce: true,
      refreshDashboard: true,
    });
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
      await handleQueuedSubmit();
    } catch (error) {
      if (Number(error?.status || 0) === 401) {
        if (error?.leadQueuedLocally === false) {
          setFeedback("Tu sesion expiro. Inicia sesion otra vez para guardar este lead.", "error");
          setStatus(
            "No pudimos enviarlo al CRM porque la sesion expiro antes de guardar el respaldo local.",
            "error",
          );
        } else {
          setFeedback(
            "Lead guardado localmente, pero tu sesion expiro. Inicia sesion otra vez para sincronizarlo.",
            "warning",
          );
          setStatus(
            "Tu lead no se perdio. Vuelve a iniciar sesion para que el CRM lo reciba.",
            "warning",
          );
        }
        return;
      }

      setFeedback(error.message || "No pude guardar este lead.", "error");
    } finally {
      setSubmitting(false);
    }
  });
}

if (refreshButton) {
  refreshButton.addEventListener("click", async () => {
    if (!navigator.onLine) {
      updateConnectivityStatus();
      return;
    }

    setStatus("Recargando portal...", "muted");

    try {
      await loadDashboard();
      await syncQueuedLeads({
        refreshDashboard: true,
      });
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

if (syncNowButton) {
  syncNowButton.addEventListener("click", async () => {
    if (!navigator.onLine) {
      updateConnectivityStatus();
      return;
    }

    try {
      const result = await syncQueuedLeads({
        announce: true,
        refreshDashboard: true,
      });

      if (result.syncedIds.length) {
        setStatus(
          `Sincronizamos ${result.syncedIds.length} lead${result.syncedIds.length === 1 ? "" : "s"} al CRM.`,
          "success",
        );
      } else if (result.blockedByLogin) {
        setStatus("Hay leads guardados localmente, pero hace falta iniciar sesion para mandarlos.", "warning");
      } else {
        setStatus("No habia leads pendientes por sincronizar.", "muted");
      }
    } catch (error) {
      setStatus(error.message || "No pude sincronizar los leads pendientes.", "error");
    }
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

window.addEventListener("online", async () => {
  updateConnectivityStatus();

  if (state.queue.some(canSyncQueuedLead)) {
    await syncQueuedLeads({
      announce: true,
      refreshDashboard: true,
    });
  }
});

window.addEventListener("offline", () => {
  updateConnectivityStatus();
});

init();
