const TRANSIENT_STATUS_CODES = new Set([502, 503, 504]);
const GET_RETRY_DELAYS_MS = [450, 1100, 2200];
const PROSPECTOR_DRAFT_STORAGE_KEY = "cmwf_prospector_draft_v1";
const PROSPECTOR_AUTH_STORAGE_KEY = "cmwf_prospector_auth_v1";
const PROSPECTOR_DASHBOARD_STORAGE_KEY = "cmwf_prospector_dashboard_v1";
const PROSPECTOR_QUEUE_DB_NAME = "cmwf_prospector_queue_v1";
const PROSPECTOR_QUEUE_STORE_NAME = "leadQueue";
const PROSPECTOR_QUEUE_DB_VERSION = 1;
const PROSPECTOR_SERVICE_WORKER_PATH = "/metalworks-crm/prospector-sw.js";
const PROSPECTOR_SERVICE_WORKER_SCOPE = "/metalworks-crm/";
const PROSPECTOR_SHELL_CACHE_URLS = [
  "/metalworks-crm/prospector/",
  "/metalworks-crm/prospector/login/",
  "/metalworks-crm/styles.css",
  "/metalworks-crm/prospector-app.js",
  "/metalworks-crm/prospector-login.js",
  "/metalworks-crm/prospector.webmanifest",
  "/metalworks-crm/prospector-icon.svg",
];
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
  const error = new Error(message || "I couldn't complete that action.");
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

function readCachedProspectorAuth() {
  const entry = readStoredJson(PROSPECTOR_AUTH_STORAGE_KEY, null);

  if (!entry || typeof entry !== "object") {
    return null;
  }

  const email = normalizeEmailValue(entry.email || "");

  if (!email) {
    return null;
  }

  return {
    name: String(entry.name || "").trim(),
    email,
  };
}

function writeCachedProspectorAuth(auth = null) {
  if (!auth?.email) {
    writeStoredJson(PROSPECTOR_AUTH_STORAGE_KEY, null);
    return;
  }

  writeStoredJson(PROSPECTOR_AUTH_STORAGE_KEY, {
    name: String(auth.name || "").trim(),
    email: normalizeEmailValue(auth.email || ""),
    savedAt: Date.now(),
  });
}

function readCachedDashboardEntry() {
  const entry = readStoredJson(PROSPECTOR_DASHBOARD_STORAGE_KEY, null);
  return entry && typeof entry === "object" ? entry : null;
}

function writeCachedDashboard(snapshot = null) {
  if (!snapshot || typeof snapshot !== "object") {
    writeStoredJson(PROSPECTOR_DASHBOARD_STORAGE_KEY, null);
    return;
  }

  writeStoredJson(PROSPECTOR_DASHBOARD_STORAGE_KEY, {
    snapshot,
    savedAt: Date.now(),
  });
}

function clearProspectorCachedState() {
  writeStoredJson(PROSPECTOR_AUTH_STORAGE_KEY, null);
  writeStoredJson(PROSPECTOR_DASHBOARD_STORAGE_KEY, null);
}

async function clearProspectorOfflineShellCache() {
  if (!("caches" in window)) {
    return;
  }

  try {
    const cacheKeys = await window.caches.keys();
    await Promise.all(
      cacheKeys
        .filter((key) => String(key || "").startsWith("cmwf-prospector-shell"))
        .map((key) => window.caches.delete(key)),
    );
  } catch {}
}

async function requestProspectorShellCaching(urls = PROSPECTOR_SHELL_CACHE_URLS) {
  if (!("serviceWorker" in navigator)) {
    return;
  }

  try {
    const registration = await navigator.serviceWorker.ready;
    registration.active?.postMessage({
      type: "CACHE_URLS",
      urls,
    });
  } catch {}
}

async function registerProspectorServiceWorker() {
  if (!("serviceWorker" in navigator)) {
    return;
  }

  try {
    const registration = await navigator.serviceWorker.register(
      PROSPECTOR_SERVICE_WORKER_PATH,
      {
        scope: PROSPECTOR_SERVICE_WORKER_SCOPE,
      },
    );
    registration.update().catch(() => {});
    await requestProspectorShellCaching();
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
              ? "You need to sign in."
              : "I couldn't complete that action."),
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

      throw createApiError("I couldn't complete that action.", status, retryable);
    }
  }

  throw createApiError("I couldn't complete that action.");
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
    return Promise.reject(new Error("This device does not support local backup."));
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
      request.onerror = () => reject(request.error || new Error("I couldn't open local backup."));
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
    request.onerror = () => reject(request.error || new Error("I couldn't read local backup."));
  });
}

async function putQueuedLeadInDb(item) {
  const db = await openQueueDb();

  return new Promise((resolve, reject) => {
    const transaction = db.transaction(PROSPECTOR_QUEUE_STORE_NAME, "readwrite");
    const store = transaction.objectStore(PROSPECTOR_QUEUE_STORE_NAME);
    const request = store.put(item);

    request.onsuccess = () => resolve(item);
    request.onerror = () => reject(request.error || new Error("I couldn't save the lead locally."));
  });
}

async function deleteQueuedLeadFromDb(localId = "") {
  const db = await openQueueDb();

  return new Promise((resolve, reject) => {
    const transaction = db.transaction(PROSPECTOR_QUEUE_STORE_NAME, "readwrite");
    const store = transaction.objectStore(PROSPECTOR_QUEUE_STORE_NAME);
    const request = store.delete(localId);

    request.onsuccess = () => resolve();
    request.onerror = () => reject(request.error || new Error("I couldn't clear the synced lead."));
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
      ? "Saving..."
      : "Save lead";
  }
}

function setUserChip(auth = null) {
  if (!userChip) {
    return;
  }

  if (!auth?.email) {
    userChip.textContent = navigator.onLine ? "No session" : "Offline mode";
    return;
  }

  userChip.textContent = `${auth.name || "Prospector"} · ${auth.email}`;
}

function renderSummary(summary = {}) {
  if (!summaryWrap) {
    return;
  }

  const metrics = [
    {
      label: "Total",
      value: Number(summary.totalLeads || 0),
      note: "Leads submitted",
    },
    {
      label: "New",
      value: Number(summary.newLeads || 0),
      note: "Still untouched",
    },
    {
      label: "Contacted",
      value: Number(summary.contactedLeads || 0),
      note: "Sales already reached out",
    },
    {
      label: "Quoted",
      value: Number(summary.quotedLeads || 0),
      note: "Pricing already sent",
    },
    {
      label: "Won",
      value: Number(summary.wonLeads || 0),
      note: "Closed jobs",
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
        No leads have been submitted from this account yet.
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
              <h3>${escapeHtml(lead.fullName || "Unnamed lead")}</h3>
              <div class="crm-lead-card-meta">
                <span>${escapeHtml(lead.projectType || "No service")}</span>
                <span>${escapeHtml(lead.addressLine || lead.location || "No address")}</span>
                <span>${escapeHtml(formatDate(lead.createdAt) || "No date")}</span>
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
    lastSyncNode.textContent = `Updated ${formatDate(new Date().toISOString())}`;
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
    setStatus("We restored your local draft on this device.", "muted");
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
    reader.onerror = () => reject(new Error("I couldn't read one of the photos."));
    reader.readAsDataURL(file);
  });
}

function loadImageFromDataUrl(dataUrl) {
  return new Promise((resolve, reject) => {
    const image = new Image();
    image.onload = () => resolve(image);
    image.onerror = () => reject(new Error("I couldn't process one of the photos."));
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
        Add project photos so sales has real visual context.
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
            alt="${escapeHtml(photo.fileName || "Project photo")}"
            loading="lazy"
          />
          <figcaption>${escapeHtml(photo.fileName || "Project photo")}</figcaption>
          <button
            type="button"
            class="crm-secondary-button crm-photo-remove"
            data-remove-capture-photo="${index}"
          >
            Remove
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
      setFeedback("Only the first 8 photos were kept.", "muted");
    }

    state.photos = nextPhotos;
    renderCapturePhotoPreview();
  } catch (error) {
    setFeedback(error.message || "I couldn't prepare those photos.", "error");
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
    return "Fill in client name, phone, service, address, ZIP, timeline, owner status, and notes.";
  }

  if (!payload.qualificationTier || !payload.qualificationNotes) {
    return "Add the lead tier and a short qualification note.";
  }

  const photos = Array.isArray(payload.photos) ? payload.photos : [];

  if (photos.length > MAX_LEAD_PHOTOS) {
    return "You can upload up to 8 photos per lead.";
  }

  const totalSizeBytes = photos.reduce((sum, item) => sum + estimateDataUrlBytes(item?.dataUrl || ""), 0);

  if (
    photos.some((item) => estimateDataUrlBytes(item?.dataUrl || "") > MAX_LEAD_PHOTO_BYTES)
  ) {
    return "Each photo must be 2 MB or less after compression.";
  }

  if (totalSizeBytes > MAX_LEAD_PHOTO_TOTAL_BYTES) {
    return "The total photo payload is too large. Compress them or send fewer files.";
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
      label: "Syncing",
      note: "Sending to the CRM right now.",
    };
  }

  if (item.status === "blocked" && item.blockedReason === "login") {
    return {
      badgeStatus: "blocked",
      label: "Login",
      note: item.lastError || "Sign in again to send it.",
    };
  }

  if (item.status === "blocked") {
    return {
      badgeStatus: "blocked",
      label: "Review",
      note: item.lastError || "This lead needs review before it can sync.",
    };
  }

  return {
    badgeStatus: "pending",
    label: "Pending",
    note: item.lastError || "Saved on this device and ready to send.",
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
      <span class="crm-chip">No local backup</span>
      <span class="crm-chip">Try another browser</span>
    `;
    offlineListWrap.innerHTML = `
      <div class="crm-prospector-empty">
        This device does not support the offline queue. If signal drops, try not to close this page until the lead has been sent.
      </div>
    `;

    if (syncNowButton) {
      syncNowButton.disabled = true;
    }

    return;
  }

  if (!state.queueReady) {
    offlineSummaryWrap.innerHTML = `
      <span class="crm-chip">Opening local backup...</span>
    `;
    offlineListWrap.innerHTML = `
      <div class="crm-prospector-empty">
        Preparing local backup on this device.
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
    `<span class="crm-chip">${queuedLeads.length} pending</span>`,
    `<span class="crm-chip">${totalPhotos} photo${totalPhotos === 1 ? "" : "s"}</span>`,
    `<span class="crm-chip">${navigator.onLine ? "Online" : "Offline"}</span>`,
    blockedCount ? `<span class="crm-chip">${blockedCount} needs review</span>` : "",
  ]
    .filter(Boolean)
    .join("");

  if (syncNowButton) {
    syncNowButton.disabled = state.queueSyncInFlight || !syncableCount || !navigator.onLine;
    syncNowButton.textContent = state.queueSyncInFlight ? "Syncing..." : "Sync now";
  }

  if (!queuedLeads.length) {
    offlineListWrap.innerHTML = `
      <div class="crm-prospector-empty">
        Anything saved with poor signal will appear here until the CRM receives it.
      </div>
    `;
    return;
  }

  offlineListWrap.innerHTML = queuedLeads
    .map((item) => {
      const meta = getQueueStatusMeta(item);
      const ownerLabel = item.prospectorEmail
        ? `Prospector ${escapeHtml(item.prospectorEmail)}`
        : "Device prospector";

      return `
        <article class="crm-mini-lead-card">
          <div class="crm-lead-card-head">
            <div>
              <h3>${escapeHtml(item.fullName || "Pending lead")}</h3>
              <div class="crm-lead-card-meta">
                <span>${escapeHtml(item.projectType || "No service")}</span>
                <span>${escapeHtml(item.addressLine || "No address")}</span>
                <span>${escapeHtml(formatDate(item.createdAt) || "No date")}</span>
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
  writeCachedProspectorAuth(state.auth);
  setUserChip(state.auth);
  renderDashboard(snapshot);
  writeCachedDashboard(snapshot);
  requestProspectorShellCaching();
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
    setStatus("Signal is back. Syncing pending leads now.", "muted");
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
          lastError: `This lead was saved by ${item.prospectorEmail}. Sign in with that account to sync it.`,
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
        const safeMessage = error?.message || "I couldn't sync this lead yet.";

        if (Number(error?.status || 0) === 401) {
          await markQueueItem(item, {
            status: "blocked",
            blockedReason: "login",
            lastError: "Your session expired. Sign in again to send this lead.",
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
      message: "Lead saved on this device. It will be sent to the CRM when signal returns.",
      tone: "warning",
    };
  }

  return {
    message: result.duplicate
      ? "This lead already existed and was updated in the CRM."
      : result.notified
        ? "Lead saved and the team was alerted."
        : "Lead saved directly in the CRM.",
    tone: "success",
  };
}

function updateConnectivityStatus() {
  if (!navigator.onLine) {
    const pendingCount = Array.isArray(state.queue) ? state.queue.length : 0;
    const pendingText = pendingCount
      ? ` There ${pendingCount === 1 ? "is" : "are"} already ${pendingCount} lead${pendingCount === 1 ? "" : "s"} saved on this device.`
      : "";
    setStatus(
      `Offline. You can keep capturing leads and local backup will send everything when internet returns.${pendingText}`,
      "warning",
    );
    return;
  }

  if (!state.queue.length) {
    return;
  }

  setStatus(
    `Back online. ${state.queue.length} pending lead${state.queue.length === 1 ? "" : "s"} still need syncing.`,
    "muted",
  );
}

function restoreCachedDashboardSnapshot() {
  const entry = readCachedDashboardEntry();
  const snapshot = entry?.snapshot;

  if (!snapshot || typeof snapshot !== "object") {
    return false;
  }

  renderDashboard(snapshot);

  if (lastSyncNode) {
    lastSyncNode.textContent = entry?.savedAt
      ? `Last saved panel ${formatDate(entry.savedAt)}`
      : "Last panel saved on this device";
  }

  return true;
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
        "I couldn't save the local backup on this device. You need signal or a compatible browser.",
        "error",
      );
      setStatus(
        "Local backup failed on this device. Use another browser or recover signal before closing the page.",
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
    setStatus("The lead is already available for the sales team.", "success");
    return;
  }

  resetFormAfterSubmit();

  if (!navigator.onLine) {
    setFeedback(
      "Lead saved on this device. It will be sent to the CRM when signal returns.",
      "warning",
    );
    updateConnectivityStatus();
    return;
  }

  setFeedback("Lead saved locally. Trying to send it to the CRM...", "muted");

  const syncResult = await syncQueuedLeads({
    preferredLocalId: queuedItem.localId,
    refreshDashboard: true,
  });
  const leadResult = syncResult.resultsById[queuedItem.localId] || null;
  const queuedState = state.queue.find((item) => item.localId === queuedItem.localId) || null;

  if (leadResult) {
    const feedbackState = buildQueuedLeadFeedback(leadResult);
    setFeedback(feedbackState.message, feedbackState.tone);
    setStatus("The lead is already available for the sales team.", "success");
    return;
  }

  if (syncResult.blockedByLogin) {
    setFeedback(
      "Lead saved on this device. Sign in again to send it to the CRM.",
      "warning",
    );
    setStatus(
      "Your lead is protected on this device, but it needs a fresh sign-in before it can sync.",
      "warning",
    );
    return;
  }

  if (queuedState?.status === "blocked" && queuedState.blockedReason === "validation") {
    setFeedback(
      queuedState.lastError || "Lead saved locally, but the data needs review before it can sync.",
      "error",
    );
    setStatus(
      "The lead is saved on this device, but it needs review before it can be sent to the CRM.",
      "warning",
    );
    return;
  }

  setFeedback(
    "Lead saved on this device. It will be sent to the CRM as soon as the connection is stable again.",
    "warning",
  );
  setStatus(
    "Local backup is ready. The portal will retry syncing automatically.",
    "warning",
  );
}

async function init() {
  renderCapturePhotoPreview();
  const cachedAuth = readCachedProspectorAuth();
  const restoredCachedDashboard = restoreCachedDashboardSnapshot();

  if (cachedAuth) {
    state.auth = cachedAuth;
    setUserChip(state.auth);
  }

  registerProspectorServiceWorker();
  restoreDraft();
  await refreshQueueState();
  updateConnectivityStatus();

  try {
    const me = await apiRequest("/api/metalworks-crm/prospector/me");

    if (!me.authenticated) {
      clearProspectorCachedState();
      window.location.href = "/metalworks-crm/prospector/login/";
      return;
    }

    state.auth = {
      name: me.name || "",
      email: me.email || "",
    };
    writeCachedProspectorAuth(state.auth);
    setUserChip(state.auth);
  } catch (error) {
    if (Number(error?.status || 0) === 401) {
      clearProspectorCachedState();
      window.location.href = "/metalworks-crm/prospector/login/";
      return;
    }

    setUserChip(state.auth);

    if (!navigator.onLine && cachedAuth?.email) {
      setStatus(
        restoredCachedDashboard
          ? "Offline. Opened the last portal saved on this device."
          : "Offline. You can keep capturing leads and the CRM will update once internet returns.",
        "warning",
      );
    } else {
      updateConnectivityStatus();
    }
  }

  try {
    await loadDashboard();
  } catch (error) {
    if (Number(error?.status || 0) === 401) {
      clearProspectorCachedState();
      window.location.href = "/metalworks-crm/prospector/login/";
      return;
    }

    setStatus(
      !navigator.onLine
        ? restoredCachedDashboard
          ? "Offline. Showing the last panel saved on this device."
          : "Offline. You can keep capturing leads and everything will stay backed up locally."
        : TRANSIENT_STATUS_CODES.has(Number(error?.status || 0))
          ? "The portal is waking up. Give it a few seconds and try again."
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
    setFeedback("Saving lead...", "muted");

    try {
      await handleQueuedSubmit();
    } catch (error) {
      if (Number(error?.status || 0) === 401) {
        if (error?.leadQueuedLocally === false) {
          setFeedback("Your session expired. Sign in again to save this lead.", "error");
          setStatus(
            "We couldn't send it to the CRM because the session expired before local backup could be saved.",
            "error",
          );
        } else {
          setFeedback(
            "Lead saved locally, but your session expired. Sign in again to sync it.",
            "warning",
          );
          setStatus(
            "Your lead was not lost. Sign in again so the CRM can receive it.",
            "warning",
          );
        }
        return;
      }

      setFeedback(error.message || "I couldn't save this lead.", "error");
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

    setStatus("Refreshing portal...", "muted");

    try {
      await loadDashboard();
      await syncQueuedLeads({
        refreshDashboard: true,
      });
      setStatus("Portal updated.", "success");
    } catch (error) {
      if (Number(error?.status || 0) === 401) {
        clearProspectorCachedState();
        window.location.href = "/metalworks-crm/prospector/login/";
        return;
      }

      setStatus(error.message, "error");
    }
  });
}

if (logoutButton) {
  logoutButton.addEventListener("click", async () => {
    clearProspectorCachedState();
    await clearProspectorOfflineShellCache();

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
          `Synced ${result.syncedIds.length} lead${result.syncedIds.length === 1 ? "" : "s"} to the CRM.`,
          "success",
        );
      } else if (result.blockedByLogin) {
        setStatus("There are leads saved locally, but a fresh sign-in is required before they can be sent.", "warning");
      } else {
        setStatus("There were no pending leads to sync.", "muted");
      }
    } catch (error) {
      setStatus(error.message || "I couldn't sync the pending leads.", "error");
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
  requestProspectorShellCaching();

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
