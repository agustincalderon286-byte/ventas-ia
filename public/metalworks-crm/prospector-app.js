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
  const error = new Error(message || "I could not complete that action.");
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
              ? "You need to sign in."
              : "I could not complete that action."),
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

      throw createApiError("I could not complete that action.", status, retryable);
    }
  }

  throw createApiError("I could not complete that action.");
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
      ? "Saving..."
      : "Save premium lead";
  }
}

function setUserChip(auth = null) {
  if (!userChip) {
    return;
  }

  if (!auth?.email) {
    userChip.textContent = "No session";
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
      note: "Sales has reached out",
    },
    {
      label: "Quoted",
      value: Number(summary.quotedLeads || 0),
      note: "Quote already sent",
    },
    {
      label: "Won",
      value: Number(summary.wonLeads || 0),
      note: "Jobs won",
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
                <span>${escapeHtml(lead.projectType || "No service selected")}</span>
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
    reader.onerror = () => reject(new Error("I could not read one of the photos."));
    reader.readAsDataURL(file);
  });
}

function loadImageFromDataUrl(dataUrl) {
  return new Promise((resolve, reject) => {
    const image = new Image();
    image.onload = () => resolve(image);
    image.onerror = () => reject(new Error("I could not process one of the photos."));
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
        Add project photos so the sales team has real visual context.
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
    setFeedback(error.message || "I could not prepare those photos.", "error");
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
        ? "The portal is waking up. Wait a few seconds and try again."
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
    setFeedback("Saving lead...", "muted");

    try {
      const result = await apiRequest("/api/metalworks-crm/prospector/leads", {
        method: "POST",
        body: buildPayload(),
      });

      renderDashboard(result.dashboard || null);
      resetFormAfterSubmit();
      setFeedback(
        result.duplicate
          ? "This lead already existed and was updated in the CRM."
          : result.notified
            ? "Lead saved and team alerted."
            : "Lead saved directly to the CRM.",
        "success",
      );
      setStatus("The lead is now available to the sales team.", "success");
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
    setStatus("Refreshing portal...", "muted");

    try {
      await loadDashboard();
      setStatus("Dashboard updated.", "success");
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
