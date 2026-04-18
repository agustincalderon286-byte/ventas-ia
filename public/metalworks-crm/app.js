const TRANSIENT_STATUS_CODES = new Set([502, 503, 504]);
const GET_RETRY_DELAYS_MS = [450, 1100, 2200];
const CRM_THEME_STORAGE_KEY = "cmwf_crm_theme_v1";
const CRM_DASHBOARD_CACHE_KEY = "cmwf_crm_dashboard_v1";
const CRM_LEAD_DETAIL_CACHE_KEY = "cmwf_crm_lead_detail_v1";
const CRM_SELECTED_LEAD_STORAGE_KEY = "cmwf_crm_selected_lead_v1";
const CRM_SELECTED_APPLICANT_STORAGE_KEY = "cmwf_crm_selected_applicant_v1";
const CRM_SELECTED_VIEW_STORAGE_KEY = "cmwf_crm_selected_view_v1";
const CRM_MOBILE_PANE_STORAGE_KEY = "cmwf_crm_mobile_pane_v1";
const CRM_CACHE_MAX_AGE_MS = 30 * 60 * 1000;
const CRM_MOBILE_BREAKPOINT_PX = 1080;
const PROSPECTOR_PASSWORD_MIN_LENGTH = 8;
const CRM_SERVICE_WORKER_PATH = "/metalworks-crm/operator-sw.js";
const CRM_SERVICE_WORKER_SCOPE = "/metalworks-crm/";
const MAX_CRM_PHOTO_FILES = 4;
const MAX_CRM_PHOTO_BYTES = 2 * 1024 * 1024;
const MAX_CRM_PHOTO_TOTAL_BYTES = 6 * 1024 * 1024;
const MIN_CRM_PHOTO_TARGET_BYTES = 320 * 1024;
const MAX_CRM_PHOTO_DIMENSION = 1600;
const crmMobileMediaQuery =
  typeof window.matchMedia === "function"
    ? window.matchMedia(`(max-width: ${CRM_MOBILE_BREAKPOINT_PX}px)`)
    : null;

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

function getCacheEntry(key, maxAgeMs = CRM_CACHE_MAX_AGE_MS) {
  const entry = readStoredJson(key, null);

  if (!entry || typeof entry !== "object" || !entry.savedAt || !entry.data) {
    return null;
  }

  if (Date.now() - Number(entry.savedAt || 0) > maxAgeMs) {
    return null;
  }

  return entry;
}

function setCacheEntry(key, data) {
  if (!data) {
    writeStoredJson(key, null);
    return;
  }

  writeStoredJson(key, {
    savedAt: Date.now(),
    data,
  });
}

function readSelectedLeadFromQuery() {
  try {
    const params = new URLSearchParams(window.location.search || "");
    return String(params.get("lead") || "").trim();
  } catch {
    return "";
  }
}

function syncSelectedLeadUrl(leadId = "") {
  try {
    const url = new URL(window.location.href);
    const safeLeadId = String(leadId || "").trim();

    if (safeLeadId) {
      url.searchParams.set("lead", safeLeadId);
    } else {
      url.searchParams.delete("lead");
    }

    const nextUrl = `${url.pathname}${url.search}${url.hash}`;
    window.history.replaceState({}, "", nextUrl);
  } catch {}
}

function formatCacheAge(savedAt = 0) {
  const ageMs = Math.max(0, Date.now() - Number(savedAt || 0));
  const minutes = Math.max(1, Math.round(ageMs / 60000));

  if (minutes < 60) {
    return `hace ${minutes} min`;
  }

  const hours = Math.max(1, Math.round(minutes / 60));
  return `hace ${hours} h`;
}

function buildAbsoluteAppUrl(pathOrUrl = "") {
  const safeValue = String(pathOrUrl || "").trim();

  if (!safeValue) {
    return "";
  }

  if (/^https?:\/\//i.test(safeValue)) {
    return safeValue;
  }

  const normalizedPath = safeValue.startsWith("/") ? safeValue : `/${safeValue}`;
  return `${window.location.origin}${normalizedPath}`;
}

function buildResourceQrImageUrl(pathOrUrl = "") {
  const absoluteUrl = buildAbsoluteAppUrl(pathOrUrl);

  if (!absoluteUrl) {
    return "";
  }

  return `https://api.qrserver.com/v1/create-qr-code/?size=320x320&data=${encodeURIComponent(
    absoluteUrl,
  )}`;
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

const METALWORKS_CONTACT = {
  companyName: "Chicago Metal Works & Fencing",
  phoneDisplay: "773 798 4107",
  phoneDigits: "7737984107",
  email: "agustincalderon286@gmail.com",
  website: "https://www.chicagometalworksandfencing.com/",
};
const DEFAULT_CLIENT_DOCUMENT_WARRANTY =
  "Chicago Metal Works & Fencing stands behind the approved scope of work. Warranty coverage and any exclusions follow the written agreement for this job.";

const ESTIMATE_COST_FIELDS = [
  "estimateMaterialsCost",
  "estimateLaborCost",
  "estimateCoatingCost",
  "estimateMiscCost",
  "estimateDiscount",
];
const CRM_LEAD_REMINDER_OPTIONS = [
  { value: 60, label: "1 hour before" },
  { value: 120, label: "2 hours before" },
  { value: 1440, label: "1 day before" },
  { value: 2880, label: "2 days before" },
];
const CRM_LEAD_REMINDER_OPTION_MAP = new Map(
  CRM_LEAD_REMINDER_OPTIONS.map((option) => [option.value, option.label]),
);
const APPLICANT_STATUS_OPTIONS = [
  { value: "new", label: "Nuevo candidato" },
  { value: "interview_requested", label: "Entrevista pedida" },
  { value: "interview_scheduled", label: "Entrevista agendada" },
  { value: "archived", label: "Archivado" },
];
const APPLICANT_NOTES_MARKER = "[Agustin Applicant Notes]";

function normalizeCrmView(value = "") {
  return String(value || "").trim().toLowerCase() === "applicants" ? "applicants" : "leads";
}

function normalizeCrmMobilePane(value = "") {
  const safeValue = String(value || "").trim().toLowerCase();

  if (safeValue === "workspace" || safeValue === "agenda" || safeValue === "more") {
    return safeValue;
  }

  return "inbox";
}

const state = {
  me: null,
  dashboard: null,
  applicants: [],
  prospectorAdmin: {
    summary: null,
    prospectors: [],
    loginUrl: "",
    portalUrl: "",
    latestCredentials: null,
  },
  leadDetail: null,
  applicantDetail: null,
  view: normalizeCrmView(readStoredJson(CRM_SELECTED_VIEW_STORAGE_KEY, "leads") || "leads"),
  mobilePane: normalizeCrmMobilePane(
    readStoredJson(CRM_MOBILE_PANE_STORAGE_KEY, "inbox") || "inbox",
  ),
  selectedLeadId:
    readSelectedLeadFromQuery() || String(readStoredJson(CRM_SELECTED_LEAD_STORAGE_KEY, "") || ""),
  selectedApplicantId: String(readStoredJson(CRM_SELECTED_APPLICANT_STORAGE_KEY, "") || ""),
  detailTab: "profile",
  applicantDetailTab: "profile",
  filters: {
    search: "",
    status: "",
    projectType: "",
  },
  applicantFilters: {
    search: "",
    status: "",
    role: "",
  },
  pushConfig: null,
  pushRegistration: null,
  pushSubscription: null,
  pushBusy: false,
  liveChatReplyBusy: false,
  manualPhotoUploadBusy: false,
  manualLeadBusy: false,
  searchTimer: null,
  bindingsReady: false,
};

const summaryWrap = document.querySelector("[data-crm-summary]");
const agendaPanel = document.querySelector("[data-crm-agenda-panel]");
const agendaList = document.querySelector("[data-crm-agenda-list]");
const agendaCount = document.querySelector("[data-crm-agenda-count]");
const resourceHub = document.querySelector("[data-crm-resource-hub]");
const resourcesWrap = document.querySelector("[data-crm-resource-sections]");
const resourcesFeedback = document.querySelector("[data-crm-resource-feedback]");
const newLeadToggleButton = document.querySelector("[data-crm-new-lead-toggle]");
const manualLeadPanel = document.querySelector("[data-crm-manual-lead-panel]");
const manualLeadForm = document.querySelector("[data-crm-manual-lead-form]");
const manualLeadSaveButton = document.querySelector("[data-crm-manual-lead-save]");
const manualLeadCancelButton = document.querySelector("[data-crm-manual-lead-cancel]");
const manualLeadFeedback = document.querySelector("[data-crm-manual-lead-feedback]");
const mobileShell = document.querySelector("[data-crm-mobile-shell]");
const mobileShellTitle = document.querySelector("[data-crm-mobile-title]");
const mobileShellCopy = document.querySelector("[data-crm-mobile-copy]");
const mobilePaneButtons = Array.from(document.querySelectorAll("[data-crm-mobile-pane-button]"));
const mobileSecondaryMoreButton = document.querySelector(
  "[data-crm-mobile-secondary-button=\"more\"]",
);
const mobilePaneTargets = Array.from(document.querySelectorAll("[data-crm-mobile-pane-target]"));
const mobileBackButton = document.querySelector("[data-crm-mobile-back]");
const mobileCollapsibles = Array.from(document.querySelectorAll("[data-crm-mobile-collapsible]"));
const prospectorAdminWrap = document.querySelector("[data-crm-prospector-admin]");
const prospectorSummary = document.querySelector("[data-crm-prospector-summary]");
const prospectorForm = document.querySelector("[data-crm-prospector-form]");
const prospectorSaveButton = document.querySelector("[data-crm-prospector-save]");
const prospectorList = document.querySelector("[data-crm-prospector-list]");
const prospectorFeedback = document.querySelector("[data-crm-prospector-feedback]");
const prospectorCredentialsCard = document.querySelector("[data-crm-prospector-credentials]");
const prospectorCredentialsTitle = document.querySelector(
  "[data-crm-prospector-credentials-title]",
);
const prospectorCredentialsList = document.querySelector(
  "[data-crm-prospector-credentials-list]",
);
const prospectorCopyEmailButton = document.querySelector("[data-crm-prospector-copy-email]");
const prospectorCopyPasswordButton = document.querySelector(
  "[data-crm-prospector-copy-password]",
);
const prospectorCopyLoginButton = document.querySelector("[data-crm-prospector-copy-login]");
const leadList = document.querySelector("[data-crm-lead-list]");
const emptyState = document.querySelector("[data-crm-empty-state]");
const collectionTitle = document.querySelector("[data-crm-collection-title]");
const collectionCount = document.querySelector("[data-crm-collection-count]");
const searchLabel = document.querySelector("[data-crm-search-label]");
const statusLabel = document.querySelector("[data-crm-status-label]");
const serviceLabel = document.querySelector("[data-crm-service-label]");
const viewButtons = Array.from(document.querySelectorAll("[data-crm-view-button]"));
const detailWrap = document.querySelector("[data-crm-detail-wrap]");
const applicantDetailWrap = document.querySelector("[data-crm-applicant-detail-wrap]");
const detailEmpty = document.querySelector("[data-crm-detail-empty]");
const detailTitle = document.querySelector("[data-crm-detail-title]");
const detailMeta = document.querySelector("[data-crm-detail-meta]");
const detailStatus = document.querySelector("[data-crm-detail-status]");
const detailForm = document.querySelector("[data-crm-detail-form]");
const detailPanel = document.querySelector(".crm-detail-panel");
const mainGrid = document.querySelector(".crm-main-grid");
const detailFeedback = document.querySelector("[data-crm-detail-feedback]");
const actionFeedback = document.querySelector("[data-crm-action-feedback]");
const activityList = document.querySelector("[data-crm-activity-list]");
const globalActivityList = document.querySelector("[data-crm-global-activity]");
const globalActivitySummary = document.querySelector("[data-crm-global-activity-summary]");
const statusFilter = document.querySelector("[data-crm-status-filter]");
const serviceFilter = document.querySelector("[data-crm-service-filter]");
const searchInput = document.querySelector("[data-crm-search]");
const userChip = document.querySelector("[data-crm-user-chip]");
const refreshButton = document.querySelector("[data-crm-refresh]");
const logoutButton = document.querySelector("[data-crm-logout]");
const enablePushButton = document.querySelector("[data-crm-enable-push]");
const testPushButton = document.querySelector("[data-crm-test-push]");
const pushFeedback = document.querySelector("[data-crm-push-feedback]");
const statusInput = document.querySelector("[data-crm-detail-status-input]");
const themeBadge = document.querySelector("[data-crm-theme-badge]");
const systemStatus = document.querySelector("[data-crm-system-status]");
const callLink = document.querySelector("[data-crm-call-link]");
const textLink = document.querySelector("[data-crm-text-link]");
const mapLink = document.querySelector("[data-crm-map-link]");
const markQuotedButton = document.querySelector("[data-crm-mark-quoted]");
const sendEstimateButton = document.querySelector("[data-crm-send-estimate]");
const openEmailDraftButton = document.querySelector("[data-crm-open-email-draft]");
const copyEstimateButton = document.querySelector("[data-crm-copy-estimate]");
const deleteLeadButton = document.querySelector("[data-crm-delete-lead]");
const detailTabButtons = Array.from(document.querySelectorAll("[data-crm-detail-tab]"));
const detailViews = Array.from(document.querySelectorAll("[data-crm-detail-view]"));
const conversationThread = document.querySelector("[data-crm-conversation-thread]");
const conversationSummary = document.querySelector("[data-crm-conversation-summary]");
const liveChatPanel = document.querySelector("[data-crm-live-chat-panel]");
const liveChatForm = document.querySelector("[data-crm-live-chat-form]");
const liveChatSendButton = document.querySelector("[data-crm-live-chat-send]");
const liveChatFeedback = document.querySelector("[data-crm-live-chat-feedback]");
const applicantDetailTabButtons = Array.from(
  document.querySelectorAll("[data-crm-applicant-detail-tab]"),
);
const applicantDetailViews = Array.from(
  document.querySelectorAll("[data-crm-applicant-detail-view]"),
);
const applicantMeta = document.querySelector("[data-crm-applicant-meta]");
const applicantProfileCard = document.querySelector("[data-crm-applicant-profile-card]");
const applicantConversationThread = document.querySelector(
  "[data-crm-applicant-conversation-thread]",
);
const applicantConversationSummary = document.querySelector(
  "[data-crm-applicant-conversation-summary]",
);
const applicantActivityList = document.querySelector("[data-crm-applicant-activity-list]");
const applicantCallLink = document.querySelector("[data-crm-applicant-call-link]");
const applicantTextLink = document.querySelector("[data-crm-applicant-text-link]");
const applicantEmailLink = document.querySelector("[data-crm-applicant-email-link]");
const photoSection = document.querySelector("[data-crm-photo-section]");
const photoGrid = document.querySelector("[data-crm-photo-grid]");
const photoSummary = document.querySelector("[data-crm-photo-summary]");
const photoUploadTrigger = document.querySelector("[data-crm-photo-upload-trigger]");
const photoUploadInput = document.querySelector("[data-crm-photo-upload-input]");
const photoUploadFeedback = document.querySelector("[data-crm-photo-upload-feedback]");

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
    year: "numeric",
    hour: "numeric",
    minute: "2-digit",
  });
}

function formatDateOnly(value = "") {
  if (!value) {
    return "";
  }

  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return "";
  }

  return date.toLocaleDateString("en-US", {
    month: "short",
    day: "numeric",
    year: "numeric",
  });
}

function formatCurrency(value = 0) {
  const amount = Number(value || 0) || 0;
  return new Intl.NumberFormat("en-US", {
    style: "currency",
    currency: "USD",
    minimumFractionDigits: 0,
    maximumFractionDigits: 2,
  }).format(amount);
}

function formatLeadSource(value = "") {
  const source = String(value || "").trim();
  const labels = {
    website_form: "Website form",
    website_live_chat: "Website live chat",
    assistant_chat: "Assistant chat",
    assistant_whatsapp: "WhatsApp assistant",
    assistant_chat_photo: "Assistant photo upload",
    assistant_booking: "Assistant callback",
    field_prospector: "Field prospector",
    lead_distribution_prospector: "Prospector intake",
    manual_crm_entry: "Manual CRM lead",
  };

  return labels[source] || source.replace(/_/g, " ").trim();
}

function formatApplicantSource(applicant = null) {
  const sourceLabel = String(applicant?.sourceLabel || "").trim();

  if (sourceLabel) {
    return sourceLabel;
  }

  const source = String(applicant?.sourceType || "").trim();
  const labels = {
    assistant_chat_job: "Website hiring assistant",
    assistant_whatsapp_job: "WhatsApp hiring assistant",
    whatsapp_job: "WhatsApp hiring",
  };

  return labels[source] || source.replace(/_/g, " ").trim() || "Hiring assistant";
}

function formatApplicantAnswer(value = "") {
  const normalized = String(value || "").trim().toLowerCase();

  if (!normalized) {
    return "Pending";
  }

  if (normalized === "yes") {
    return "Yes";
  }

  if (normalized === "no") {
    return "No";
  }

  return String(value || "").trim();
}

function normalizeLeadReminderOffsets(values = []) {
  const safeValues = Array.isArray(values) ? values : [values];
  const unique = [];
  const seen = new Set();

  safeValues.forEach((value) => {
    const minutes = Math.round(Number(value || 0));

    if (!CRM_LEAD_REMINDER_OPTION_MAP.has(minutes) || seen.has(minutes)) {
      return;
    }

    seen.add(minutes);
    unique.push(minutes);
  });

  return unique.sort((left, right) => left - right);
}

function formatLeadReminderLabel(value = 0) {
  return CRM_LEAD_REMINDER_OPTION_MAP.get(Math.round(Number(value || 0))) || "";
}

function buildLeadReminderSummary(lead = null) {
  const labels = normalizeLeadReminderOffsets(lead?.nextActionReminderOffsets || [])
    .map((value) => formatLeadReminderLabel(value))
    .filter(Boolean);

  return labels.join(" · ");
}

function syncLeadReminderInputs(offsets = []) {
  if (!detailForm) {
    return;
  }

  const selectedValues = new Set(normalizeLeadReminderOffsets(offsets));

  detailForm.querySelectorAll('input[name="nextActionReminderOffsets"]').forEach((input) => {
    input.checked = selectedValues.has(Math.round(Number(input.value || 0)));
  });
}

function stripGeneratedApplicantNotes(value = "") {
  const source = String(value || "");
  const markerIndex = source.indexOf(APPLICANT_NOTES_MARKER);

  if (markerIndex === -1) {
    return source.trim();
  }

  return source.slice(0, markerIndex).trim();
}

function truncateText(value = "", maxLength = 180) {
  const safeValue = String(value || "").trim();

  if (!safeValue || safeValue.length <= maxLength) {
    return safeValue;
  }

  return `${safeValue.slice(0, Math.max(0, maxLength - 1)).trim()}…`;
}

function buildLeadSummaryText(lead = null) {
  if (!lead) {
    return "";
  }

  if (lead.lastUserMessage) {
    return `Latest message: ${truncateText(lead.lastUserMessage, 140)}`;
  }

  if (lead.details) {
    return truncateText(lead.details, 140);
  }

  return "";
}

function toDatetimeLocalValue(value = "") {
  if (!value) {
    return "";
  }

  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return "";
  }

  const year = date.getFullYear();
  const month = String(date.getMonth() + 1).padStart(2, "0");
  const day = String(date.getDate()).padStart(2, "0");
  const hours = String(date.getHours()).padStart(2, "0");
  const minutes = String(date.getMinutes()).padStart(2, "0");
  return `${year}-${month}-${day}T${hours}:${minutes}`;
}

function toDateInputValue(value = "") {
  if (!value) {
    return "";
  }

  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return "";
  }

  const year = date.getFullYear();
  const month = String(date.getMonth() + 1).padStart(2, "0");
  const day = String(date.getDate()).padStart(2, "0");
  return `${year}-${month}-${day}`;
}

function setDetailFeedback(message = "", tone = "") {
  [detailFeedback, actionFeedback].forEach((element) => {
    if (!element) {
      return;
    }

    element.textContent = message;
    element.dataset.tone = tone;
  });
}

function setLiveChatFeedback(message = "", tone = "") {
  if (!liveChatFeedback) {
    return;
  }

  liveChatFeedback.textContent = message;
  liveChatFeedback.dataset.tone = tone;
}

function setSystemStatus(message = "", tone = "") {
  if (!systemStatus) {
    return;
  }

  systemStatus.hidden = !message;
  systemStatus.textContent = message;
  systemStatus.dataset.tone = tone;
}

function setPushFeedback(message = "", tone = "") {
  if (!pushFeedback) {
    return;
  }

  pushFeedback.textContent = message;
  pushFeedback.dataset.tone = tone;
}

function setPhotoUploadFeedback(message = "", tone = "") {
  if (!photoUploadFeedback) {
    return;
  }

  photoUploadFeedback.textContent = message;
  photoUploadFeedback.dataset.tone = tone;
}

function setManualLeadFeedback(message = "", tone = "") {
  if (!manualLeadFeedback) {
    return;
  }

  manualLeadFeedback.textContent = message;
  manualLeadFeedback.dataset.tone = tone;
}

function supportsWebPush() {
  return Boolean(
    window.isSecureContext &&
      "Notification" in window &&
      "serviceWorker" in navigator &&
      "PushManager" in window,
  );
}

function isAppleMobileDevice() {
  return /iphone|ipad|ipod/i.test(String(navigator.userAgent || ""));
}

function isStandaloneApp() {
  try {
    return (
      window.matchMedia("(display-mode: standalone)").matches ||
      window.navigator.standalone === true
    );
  } catch {
    return window.navigator.standalone === true;
  }
}

function needsHomeScreenInstallForPush() {
  return isAppleMobileDevice() && !isStandaloneApp();
}

function supportsAppBadge() {
  return Boolean("setAppBadge" in navigator || "clearAppBadge" in navigator);
}

async function clearCrmBadge() {
  if (!supportsAppBadge() || !("clearAppBadge" in navigator)) {
    return;
  }

  try {
    await navigator.clearAppBadge();
  } catch {}
}

function urlBase64ToUint8Array(value = "") {
  const padding = "=".repeat((4 - (value.length % 4)) % 4);
  const base64 = `${value}${padding}`.replace(/-/g, "+").replace(/_/g, "/");
  const rawData = window.atob(base64);
  return Uint8Array.from(rawData, (char) => char.charCodeAt(0));
}

function detectBrowserName() {
  const userAgent = String(navigator.userAgent || "");

  if (/edg\//i.test(userAgent)) return "Edge";
  if (/opr\//i.test(userAgent)) return "Opera";
  if (/chrome\//i.test(userAgent) && !/edg\//i.test(userAgent)) return "Chrome";
  if (/firefox\//i.test(userAgent)) return "Firefox";
  if (/safari\//i.test(userAgent) && !/chrome\//i.test(userAgent)) return "Safari";
  return "Browser";
}

function buildPushDeviceName() {
  const platform = String(navigator.platform || "device").trim();
  return `${detectBrowserName()} on ${platform}`.slice(0, 120);
}

function syncPushControls() {
  const hasSupport = supportsWebPush();
  const installRequired = needsHomeScreenInstallForPush();
  const permission = hasSupport ? Notification.permission : "unsupported";
  const pushConfigured = Boolean(state.pushConfig?.webPushConfigured);
  const isEnabled = Boolean(state.pushSubscription && permission === "granted");

  if (enablePushButton) {
    enablePushButton.disabled =
      state.pushBusy || !hasSupport || !pushConfigured || installRequired;
    enablePushButton.textContent = isEnabled
      ? "Alerts Enabled"
      : installRequired
        ? "Install App First"
        : permission === "denied"
          ? "Alerts Blocked"
          : "Enable Alerts";
  }

  if (testPushButton) {
    testPushButton.disabled = state.pushBusy || !isEnabled;
  }
}

async function registerCrmServiceWorker() {
  if (!supportsWebPush()) {
    return null;
  }

  if (state.pushRegistration) {
    return state.pushRegistration;
  }

  const registration = await navigator.serviceWorker.register(CRM_SERVICE_WORKER_PATH, {
    scope: CRM_SERVICE_WORKER_SCOPE,
  });
  state.pushRegistration = await navigator.serviceWorker.ready;
  registration.update().catch(() => null);
  return state.pushRegistration;
}

async function refreshExistingPushSubscription({ syncServer = false } = {}) {
  if (!supportsWebPush()) {
    state.pushSubscription = null;
    syncPushControls();
    return null;
  }

  const registration = await registerCrmServiceWorker();
  const subscription = await registration.pushManager.getSubscription();
  state.pushSubscription = subscription;

  if (
    syncServer &&
    subscription &&
    Notification.permission === "granted" &&
    state.pushConfig?.webPushConfigured
  ) {
    await apiRequest("/api/metalworks-crm/push/web/register", {
      method: "POST",
      body: {
        subscription: subscription.toJSON(),
        deviceName: buildPushDeviceName(),
        browserName: detectBrowserName(),
        notificationPath: "/metalworks-crm/",
        authorizationStatus: Notification.permission,
        notificationsEnabled: true,
      },
    });
  }

  syncPushControls();
  return subscription;
}

async function loadPushConfig({ silent = false } = {}) {
  if (needsHomeScreenInstallForPush()) {
    setPushFeedback(
      "On iPhone, add this CRM to your Home Screen first. Then open the installed app and tap Enable Alerts.",
      "muted",
    );
    syncPushControls();
    return null;
  }

  if (!supportsWebPush()) {
    setPushFeedback("This browser does not support secure push notifications.", "muted");
    syncPushControls();
    return null;
  }

  try {
    state.pushConfig = await apiRequest("/api/metalworks-crm/push/config");
    await refreshExistingPushSubscription({ syncServer: true });

    if (!state.pushConfig?.webPushConfigured && !silent) {
      setPushFeedback("Web alerts still need VAPID keys on the server.", "warning");
    } else if (!state.pushSubscription && Notification.permission === "default" && !silent) {
      setPushFeedback("Enable alerts to get new lead notifications on this device.", "muted");
    } else if (Notification.permission === "denied" && !silent) {
      setPushFeedback("Browser alerts are blocked for this device.", "warning");
    } else if (state.pushSubscription && !silent) {
      setPushFeedback("Lead alerts are active on this device.", "success");
    }

    syncPushControls();
    return state.pushConfig;
  } catch (error) {
    if (!silent) {
      setPushFeedback(error.message || "I could not load the alert settings.", "error");
    }
    syncPushControls();
    return null;
  }
}

async function handleEnablePush() {
  if (!supportsWebPush()) {
    setPushFeedback("This browser does not support secure push notifications.", "error");
    return;
  }

  if (!state.pushConfig?.webPushConfigured || !state.pushConfig?.vapidPublicKey) {
    setPushFeedback("Web alerts still need server keys before this can work.", "warning");
    syncPushControls();
    return;
  }

  state.pushBusy = true;
  syncPushControls();
  setPushFeedback("Enabling alerts...", "muted");

  try {
    const permission = await Notification.requestPermission();

    if (permission !== "granted") {
      setPushFeedback(
        permission === "denied"
          ? "Alerts were blocked for this browser."
          : "Notification permission was not granted.",
        "warning",
      );
      return;
    }

    const registration = await registerCrmServiceWorker();
    let subscription = await registration.pushManager.getSubscription();

    if (!subscription) {
      subscription = await registration.pushManager.subscribe({
        userVisibleOnly: true,
        applicationServerKey: urlBase64ToUint8Array(state.pushConfig.vapidPublicKey),
      });
    }

    await apiRequest("/api/metalworks-crm/push/web/register", {
      method: "POST",
      body: {
        subscription: subscription.toJSON(),
        deviceName: buildPushDeviceName(),
        browserName: detectBrowserName(),
        notificationPath: "/metalworks-crm/",
        authorizationStatus: permission,
        notificationsEnabled: true,
      },
    });

    state.pushSubscription = subscription;
    setPushFeedback("Lead alerts are active on this device.", "success");
  } catch (error) {
    setPushFeedback(error.message || "I could not enable alerts on this browser.", "error");
  } finally {
    state.pushBusy = false;
    syncPushControls();
  }
}

async function handleTestPush() {
  if (!state.pushSubscription) {
    setPushFeedback("Enable alerts first on this device.", "warning");
    return;
  }

  state.pushBusy = true;
  syncPushControls();
  setPushFeedback("Sending test alert...", "muted");

  try {
    const result = await apiRequest("/api/metalworks-crm/push/test", {
      method: "POST",
    });
    setPushFeedback(result.message || "Test alert sent.", result.ok ? "success" : "warning");
  } catch (error) {
    setPushFeedback(error.message || "I could not send the test alert.", "error");
  } finally {
    state.pushBusy = false;
    syncPushControls();
  }
}

function persistThemeProfile(profile = {}, email = "") {
  setCacheEntry(CRM_THEME_STORAGE_KEY, {
    email: String(email || "").trim(),
    profile: profile && typeof profile === "object" ? profile : {},
  });
}

function applyCachedTheme() {
  const entry = getCacheEntry(CRM_THEME_STORAGE_KEY, 365 * 24 * 60 * 60 * 1000);
  const cached = entry?.data;

  if (!cached?.profile) {
    return false;
  }

  applyProfileTheme(cached.profile, cached.email || "");
  return true;
}

function rememberSelectedView(view = "leads") {
  state.view = normalizeCrmView(view);
  writeStoredJson(CRM_SELECTED_VIEW_STORAGE_KEY, state.view);
  syncSelectedLeadUrl(state.view === "leads" ? state.selectedLeadId : "");
}

function rememberSelectedLead(leadId = "") {
  state.selectedLeadId = String(leadId || "").trim();
  writeStoredJson(CRM_SELECTED_LEAD_STORAGE_KEY, state.selectedLeadId || "");
  if (state.view === "leads") {
    syncSelectedLeadUrl(state.selectedLeadId);
  }
}

function rememberSelectedApplicant(applicantId = "") {
  state.selectedApplicantId = String(applicantId || "").trim();
  writeStoredJson(CRM_SELECTED_APPLICANT_STORAGE_KEY, state.selectedApplicantId || "");
}

function isMobileCrmLayout() {
  if (crmMobileMediaQuery) {
    return crmMobileMediaQuery.matches;
  }

  return window.innerWidth <= CRM_MOBILE_BREAKPOINT_PX;
}

function getWorkspaceEntity() {
  return state.view === "applicants"
    ? state.applicantDetail?.applicant || null
    : state.leadDetail?.lead || null;
}

function hasWorkspaceSelection() {
  if (getWorkspaceEntity()?.id) {
    return true;
  }

  return state.view === "applicants"
    ? Boolean(state.selectedApplicantId)
    : Boolean(state.selectedLeadId);
}

function resolveMobilePane() {
  const requestedPane = normalizeCrmMobilePane(state.mobilePane);

  if (requestedPane === "workspace" && !hasWorkspaceSelection()) {
    return "inbox";
  }

  return requestedPane;
}

function rememberMobilePane(pane = "inbox") {
  state.mobilePane = normalizeCrmMobilePane(pane);
  writeStoredJson(CRM_MOBILE_PANE_STORAGE_KEY, state.mobilePane);
}

function syncMobilePaneButtons(activePane = resolveMobilePane()) {
  mobilePaneButtons.forEach((button) => {
    const paneName = normalizeCrmMobilePane(button.dataset.crmMobilePaneButton || "inbox");
    const isActive = paneName === activePane;
    button.classList.toggle("is-active", isActive);
    button.setAttribute("aria-pressed", isActive ? "true" : "false");
  });
}

function syncMobileShellCopy(activePane = resolveMobilePane()) {
  if (!mobileShellTitle || !mobileShellCopy) {
    return;
  }

  const workspaceEntity = getWorkspaceEntity();
  const entityLabel = state.view === "applicants" ? "candidate" : "lead";

  if (activePane === "workspace") {
    mobileShellTitle.textContent = workspaceEntity?.fullName
      ? truncateText(workspaceEntity.fullName, 36)
      : "Workspace";
    mobileShellCopy.textContent = workspaceEntity?.fullName
      ? `Manage this ${entityLabel}, review activity, and keep the conversation in one place.`
      : `Open a ${entityLabel} from Inbox to work it here.`;
    return;
  }

  if (activePane === "agenda") {
    mobileShellTitle.textContent = "Agenda";
    mobileShellCopy.textContent =
      "See the leads that already have a callback, visit, or follow-up time scheduled.";
    return;
  }

  if (activePane === "more") {
    mobileShellTitle.textContent = "More Tools";
    mobileShellCopy.textContent =
      "Metrics, quick links, prospector accounts, and the activity feed stay here.";
    return;
  }

  mobileShellTitle.textContent = "Inbox";
  mobileShellCopy.textContent =
    state.view === "applicants"
      ? "Review candidates first, then open one profile to work it."
      : "Open a lead and work it like a contact on your phone.";
}

function syncMobileSecondaryActions(activePane = resolveMobilePane()) {
  if (!mobileSecondaryMoreButton) {
    return;
  }

  mobileSecondaryMoreButton.textContent =
    activePane === "more" ? "Back to Agenda" : "More Tools";
  mobileSecondaryMoreButton.setAttribute("aria-pressed", activePane === "more" ? "true" : "false");
}

function setMobileCollapsibleDefaults({ reset = false } = {}) {
  const mobileLayout = isMobileCrmLayout();

  mobileCollapsibles.forEach((element) => {
    if (!(element instanceof HTMLDetailsElement)) {
      return;
    }

    if (!mobileLayout) {
      element.open = true;
      delete element.dataset.crmMobileInitialized;
      return;
    }

    if (reset || !element.dataset.crmMobileInitialized) {
      element.open = false;
      element.dataset.crmMobileInitialized = "true";
    }
  });
}

function applyMobilePaneLayout() {
  const mobileLayout = isMobileCrmLayout();

  if (!mobileLayout) {
    delete document.body.dataset.crmMobilePane;
    delete document.body.dataset.crmMobileLayout;
    mobilePaneTargets.forEach((element) => {
      element.classList.remove("crm-mobile-pane-hidden");
    });
    mainGrid?.classList.remove("crm-mobile-main-hidden");
    if (mobileBackButton) {
      mobileBackButton.hidden = true;
    }
    setMobileCollapsibleDefaults({ reset: true });
    syncMobilePaneButtons(resolveMobilePane());
    syncMobileShellCopy(resolveMobilePane());
    syncMobileSecondaryActions(resolveMobilePane());
    return;
  }

  const activePane = resolveMobilePane();

  if (activePane !== state.mobilePane) {
    rememberMobilePane(activePane);
  }

  document.body.dataset.crmMobilePane = activePane;
  document.body.dataset.crmMobileLayout = "true";

  mobilePaneTargets.forEach((element) => {
    const targetPane = normalizeCrmMobilePane(element.dataset.crmMobilePaneTarget || "inbox");
    element.classList.toggle("crm-mobile-pane-hidden", targetPane !== activePane);
  });

  mainGrid?.classList.toggle(
    "crm-mobile-main-hidden",
    activePane === "more" || activePane === "agenda",
  );

  if (mobileBackButton) {
    mobileBackButton.hidden = activePane !== "workspace";
  }

  setMobileCollapsibleDefaults();
  syncMobilePaneButtons(activePane);
  syncMobileShellCopy(activePane);
  syncMobileSecondaryActions(activePane);
}

function openMobileWorkspacePane() {
  if (!isMobileCrmLayout()) {
    return;
  }

  rememberMobilePane("workspace");
  applyMobilePaneLayout();
}

function getLeadDetailCache() {
  const cache = readStoredJson(CRM_LEAD_DETAIL_CACHE_KEY, {});
  return cache && typeof cache === "object" ? cache : {};
}

function persistLeadDetail(detail = null) {
  const leadId = String(detail?.lead?.id || "").trim();

  if (!leadId) {
    return;
  }

  const cache = getLeadDetailCache();
  cache[leadId] = {
    savedAt: Date.now(),
    data: detail,
  };
  writeStoredJson(CRM_LEAD_DETAIL_CACHE_KEY, cache);
}

function readCachedLeadDetail(leadId = "") {
  const safeLeadId = String(leadId || "").trim();

  if (!safeLeadId) {
    return null;
  }

  const entry = getLeadDetailCache()[safeLeadId];

  if (!entry?.savedAt || !entry?.data) {
    return null;
  }

  if (Date.now() - Number(entry.savedAt || 0) > CRM_CACHE_MAX_AGE_MS) {
    return null;
  }

  return entry;
}

function applyProfileTheme(profile = {}, fallbackEmail = "") {
  const skin = String(profile.skin || "classic").trim() || "classic";
  const displayName = String(profile.displayName || fallbackEmail || "Admin").trim();
  const label = String(profile.themeLabel || "").trim();
  document.body.dataset.crmSkin = skin;

  if (userChip) {
    userChip.textContent = displayName;
  }

  if (themeBadge) {
    themeBadge.hidden = !label;
    themeBadge.textContent = label || "";
  }
}

function handleCrmError(error, { fallbackMessage = "", allowRedirect = true } = {}) {
  console.error(error);

  if (Number(error?.status || 0) === 401 && allowRedirect) {
    window.location.href = "/metalworks-crm/login/";
    return;
  }

  const tone =
    TRANSIENT_STATUS_CODES.has(Number(error?.status || 0)) || error instanceof TypeError
      ? "warning"
      : "error";
  const message =
    fallbackMessage ||
    (tone === "warning"
      ? "La conexion del CRM esta inestable. Conservando el ultimo snapshot mientras vuelve."
      : error?.message || "No pude completar esa accion.");

  setSystemStatus(message, tone);
}

function buildQueryString(filters = {}) {
  const params = new URLSearchParams();

  Object.entries(filters).forEach(([key, value]) => {
    const safeValue = String(value || "").trim();
    if (safeValue) {
      params.set(key, safeValue);
    }
  });

  return params.toString();
}

function normalizeClientDocumentType(value = "") {
  return String(value || "").trim().toLowerCase() === "invoice"
    ? "invoice"
    : "estimate";
}

function getClientDocumentLabel(value = "") {
  return normalizeClientDocumentType(value) === "invoice" ? "Invoice" : "Estimate";
}

function normalizePhoneDigits(value = "") {
  const digits = String(value || "").replace(/\D/g, "");

  if (digits.length === 11 && digits.startsWith("1")) {
    return digits.slice(1);
  }

  if (digits.length >= 10) {
    return digits.slice(0, 10);
  }

  return digits;
}

function getLeadPhoneDigits(lead = null) {
  return normalizePhoneDigits(lead?.phone || lead?.phoneDisplay || "");
}

function buildTelHref(phoneDigits = "") {
  if (!phoneDigits) {
    return "#";
  }

  return `tel:+1${phoneDigits}`;
}

function buildSmsHref(phoneDigits = "") {
  if (!phoneDigits) {
    return "#";
  }

  return `sms:+1${phoneDigits}`;
}

function renameFileExtension(fileName = "", extension = ".jpg") {
  const safeExtension = String(extension || ".jpg").startsWith(".")
    ? String(extension || ".jpg")
    : `.${String(extension || "jpg")}`;
  const baseName = String(fileName || "project-photo")
    .trim()
    .replace(/\.[A-Za-z0-9]+$/, "");

  return `${baseName || "project-photo"}${safeExtension}`;
}

function canvasToBlob(canvas, mimeType = "image/jpeg", quality = 0.82) {
  return new Promise((resolve, reject) => {
    canvas.toBlob(
      (blob) => {
        if (blob) {
          resolve(blob);
          return;
        }

        reject(new Error("Could not prepare this image for upload."));
      },
      mimeType,
      quality,
    );
  });
}

function loadImageFromFile(file) {
  return new Promise((resolve, reject) => {
    const objectUrl = URL.createObjectURL(file);
    const image = new Image();

    image.onload = () => {
      URL.revokeObjectURL(objectUrl);
      resolve(image);
    };

    image.onerror = () => {
      URL.revokeObjectURL(objectUrl);
      reject(new Error(`Could not process ${file?.name || "this image"}.`));
    };

    image.src = objectUrl;
  });
}

async function optimizeCrmPhotoFile(file, { targetBytes = MAX_CRM_PHOTO_BYTES } = {}) {
  const safeTargetBytes = Math.max(
    MIN_CRM_PHOTO_TARGET_BYTES,
    Math.min(
      MAX_CRM_PHOTO_BYTES,
      Number(targetBytes || MAX_CRM_PHOTO_BYTES) || MAX_CRM_PHOTO_BYTES,
    ),
  );

  if (!(file instanceof File)) {
    throw new Error("Could not read this image file.");
  }

  if (!String(file.type || "").startsWith("image/")) {
    throw new Error("Only image uploads are allowed.");
  }

  if (/^image\/gif$/i.test(String(file.type || ""))) {
    if (file.size <= safeTargetBytes) {
      return file;
    }

    throw new Error(`${file.name || "This image"} is too large. Please choose a smaller file.`);
  }

  if (
    file.size <= safeTargetBytes &&
    file.size <= MAX_CRM_PHOTO_BYTES &&
    !/^image\/(?:heic|heif)$/i.test(String(file.type || ""))
  ) {
    return file;
  }

  const image = await loadImageFromFile(file);
  const originalWidth = Number(image.naturalWidth || image.width || 0) || 1;
  const originalHeight = Number(image.naturalHeight || image.height || 0) || 1;
  const baseScale = Math.min(
    1,
    MAX_CRM_PHOTO_DIMENSION / Math.max(originalWidth, originalHeight),
  );
  const canvas = document.createElement("canvas");
  const context = canvas.getContext("2d", { alpha: false });

  if (!context) {
    throw new Error("This device could not prepare the image for upload.");
  }

  const dimensionScales = [1, 0.88, 0.76, 0.64, 0.52];
  const qualitySteps = [0.9, 0.82, 0.74, 0.66, 0.58, 0.5, 0.42];
  let bestBlob = null;

  for (const dimensionScale of dimensionScales) {
    const width = Math.max(1, Math.round(originalWidth * baseScale * dimensionScale));
    const height = Math.max(1, Math.round(originalHeight * baseScale * dimensionScale));

    canvas.width = width;
    canvas.height = height;
    context.fillStyle = "#ffffff";
    context.fillRect(0, 0, width, height);
    context.drawImage(image, 0, 0, width, height);

    for (const quality of qualitySteps) {
      const blob = await canvasToBlob(canvas, "image/jpeg", quality);

      if (!bestBlob || blob.size < bestBlob.size) {
        bestBlob = blob;
      }

      if (blob.size <= safeTargetBytes && blob.size <= MAX_CRM_PHOTO_BYTES) {
        return new File([blob], renameFileExtension(file.name, ".jpg"), {
          type: "image/jpeg",
          lastModified: Date.now(),
        });
      }
    }
  }

  if (bestBlob && bestBlob.size <= MAX_CRM_PHOTO_BYTES) {
    return new File([bestBlob], renameFileExtension(file.name, ".jpg"), {
      type: "image/jpeg",
      lastModified: Date.now(),
    });
  }

  throw new Error(
    `${file.name || "This image"} is still too large after compression. Try cropping it before uploading.`,
  );
}

async function prepareCrmPhotoFilesForUpload(selectedFiles = []) {
  const perFileTarget = Math.max(
    MIN_CRM_PHOTO_TARGET_BYTES,
    Math.min(
      MAX_CRM_PHOTO_BYTES,
      Math.floor(MAX_CRM_PHOTO_TOTAL_BYTES / Math.max(selectedFiles.length, 1)),
    ),
  );

  let optimizedFiles = await Promise.all(
    selectedFiles.map((file) => optimizeCrmPhotoFile(file, { targetBytes: perFileTarget })),
  );

  let totalBytes = optimizedFiles.reduce((sum, file) => sum + (Number(file?.size || 0) || 0), 0);

  if (totalBytes <= MAX_CRM_PHOTO_TOTAL_BYTES) {
    return optimizedFiles;
  }

  const tighterTarget = Math.max(MIN_CRM_PHOTO_TARGET_BYTES, Math.floor(perFileTarget * 0.82));
  optimizedFiles = await Promise.all(
    optimizedFiles.map((file) => optimizeCrmPhotoFile(file, { targetBytes: tighterTarget })),
  );
  totalBytes = optimizedFiles.reduce((sum, file) => sum + (Number(file?.size || 0) || 0), 0);

  if (totalBytes <= MAX_CRM_PHOTO_TOTAL_BYTES) {
    return optimizedFiles;
  }

  throw new Error("These photos are still too large. Try fewer photos or crop them before uploading.");
}

function readFileAsDataUrl(file) {
  return new Promise((resolve, reject) => {
    const reader = new FileReader();
    reader.onload = () => resolve(String(reader.result || ""));
    reader.onerror = () => reject(new Error("I could not read one of these photos."));
    reader.readAsDataURL(file);
  });
}

function buildLeadLocationQuery(lead = null) {
  const values = [lead?.addressLine, lead?.city, lead?.zipCode, lead?.location]
    .map((value) => String(value || "").trim())
    .filter(Boolean);
  const uniqueValues = [];
  const seen = new Set();

  values.forEach((value) => {
    const key = value.toLowerCase();

    if (seen.has(key)) {
      return;
    }

    seen.add(key);
    uniqueValues.push(value);
  });

  return uniqueValues.join(", ");
}

function buildMapsHref(lead = null) {
  const query = buildLeadLocationQuery(lead);

  if (!query) {
    return "#";
  }

  const userAgent = String(navigator.userAgent || "");

  if (/iphone|ipad|ipod/i.test(userAgent)) {
    return `https://maps.apple.com/?q=${encodeURIComponent(query)}`;
  }

  if (/android/i.test(userAgent)) {
    return `geo:0,0?q=${encodeURIComponent(query)}`;
  }

  return `https://www.google.com/maps/search/?api=1&query=${encodeURIComponent(query)}`;
}

function getNumberInputValue(input) {
  const raw = String(input?.value || "").trim();
  if (!raw) {
    return 0;
  }

  const amount = Number(raw);
  return Number.isFinite(amount) ? amount : 0;
}

function hasBreakdownValues() {
  const form = detailForm?.elements;

  if (!form) {
    return false;
  }

  return ESTIMATE_COST_FIELDS.some((fieldName) =>
    String(form[fieldName]?.value || "").trim(),
  );
}

function calculateEstimateTotal(lead = null) {
  const form = detailForm?.elements;

  if (!form) {
    return Number(lead?.estimateAmount || 0) || 0;
  }

  const materials = getNumberInputValue(form.estimateMaterialsCost);
  const labor = getNumberInputValue(form.estimateLaborCost);
  const coating = getNumberInputValue(form.estimateCoatingCost);
  const misc = getNumberInputValue(form.estimateMiscCost);
  const discount = getNumberInputValue(form.estimateDiscount);
  const total = materials + labor + coating + misc - discount;
  return Math.max(0, Math.round(total * 100) / 100);
}

function syncEstimateTotalFromForm() {
  if (!detailForm?.elements?.estimateAmount) {
    return;
  }

  const amountInput = detailForm.elements.estimateAmount;
  const breakdownMode = hasBreakdownValues();

  amountInput.readOnly = breakdownMode;
  amountInput.dataset.mode = breakdownMode ? "auto" : "manual";

  if (breakdownMode) {
    amountInput.value = calculateEstimateTotal(state.leadDetail?.lead).toFixed(2);
  }
}

function getInvoiceValidationMessage(snapshot = null) {
  const safeSnapshot = snapshot || buildEstimateSnapshot();

  if (safeSnapshot.deposit > 0 && safeSnapshot.total <= 0) {
    return "Add the total before recording a deposit.";
  }

  if (safeSnapshot.deposit > safeSnapshot.total) {
    return "Deposit can't be higher than the total.";
  }

  return "";
}

function buildEstimateSnapshot() {
  const lead = state.leadDetail?.lead || {};
  const form = detailForm?.elements;
  const breakdownMode = hasBreakdownValues();
  const documentType = normalizeClientDocumentType(
    form?.clientDocumentType?.value || lead.clientDocumentType || "estimate",
  );
  const total = breakdownMode
    ? calculateEstimateTotal(lead)
    : Number(form?.estimateAmount?.value || lead.estimateAmount || 0) || 0;
  const deposit = Math.max(
    0,
    Math.round(
      (Number(form?.invoiceDepositAmount?.value || lead.invoiceDepositAmount || 0) || 0) * 100,
    ) / 100,
  );
  const balanceDue = Math.max(0, Math.round((total - deposit) * 100) / 100);

  return {
    documentType,
    documentLabel: getClientDocumentLabel(documentType),
    fullName: String(form?.fullName?.value || lead.fullName || "").trim(),
    firstName:
      String(form?.fullName?.value || lead.fullName || "")
        .trim()
        .split(/\s+/)
        .filter(Boolean)[0] || "there",
    phoneDisplay: String(form?.phoneDisplay?.value || lead.phoneDisplay || lead.phone || "").trim(),
    email: String(form?.email?.value || lead.email || "").trim(),
    projectType: String(form?.projectType?.value || lead.projectType || "").trim(),
    projectLabel: String(form?.projectType?.value || lead.projectType || "").trim() || "metal work project",
    location: String(form?.location?.value || lead.location || "").trim(),
    internalTitle: String(form?.estimateTitle?.value || lead.estimateTitle || "").trim(),
    internalScope: String(form?.estimateScope?.value || lead.estimateScope || "").trim(),
    internalNotes: String(form?.estimateNotes?.value || lead.estimateNotes || "").trim(),
    description:
      String(
        form?.clientDocumentDescription?.value ||
          lead.clientDocumentDescription ||
          form?.details?.value ||
          lead.details ||
          "",
      ).trim(),
    warranty:
      String(
        form?.clientDocumentWarranty?.value ||
          lead.clientDocumentWarranty ||
          DEFAULT_CLIENT_DOCUMENT_WARRANTY,
      ).trim(),
    workDate:
      String(form?.clientDocumentWorkDate?.value || "").trim() ||
      toDateInputValue(lead.clientDocumentWorkDate || lead.nextActionAt || ""),
    validUntil:
      String(form?.estimateValidUntil?.value || "").trim() ||
      toDateInputValue(lead.estimateValidUntil || ""),
    total,
    deposit,
    balanceDue,
  };
}

function setDetailTab(tab = "profile") {
  const nextTab = String(tab || "profile").trim() || "profile";
  state.detailTab = nextTab;

  detailTabButtons.forEach((button) => {
    const isActive = String(button.dataset.crmDetailTab || "") === nextTab;
    button.classList.toggle("is-active", isActive);
    button.setAttribute("aria-pressed", isActive ? "true" : "false");
  });

  detailViews.forEach((view) => {
    const viewName = String(view.dataset.crmDetailView || "").trim();
    view.hidden = viewName !== nextTab;
  });
}

function buildEstimateSubject(snapshot) {
  return `${snapshot.documentLabel} from Chicago Metal Works & Fencing - ${snapshot.projectLabel}`;
}

function buildEstimateMailto(snapshot) {
  return `mailto:${encodeURIComponent(snapshot.email)}?subject=${encodeURIComponent(
    buildEstimateSubject(snapshot),
  )}&body=${encodeURIComponent(buildEstimateBody(snapshot))}`;
}

function buildEstimateBody(snapshot) {
  const lines = [
    `Hi ${snapshot.firstName},`,
    "",
    `Thank you for working with ${METALWORKS_CONTACT.companyName}.`,
    "",
    `${snapshot.documentLabel}: ${snapshot.projectLabel}`,
    `Client: ${snapshot.fullName || "Not provided"}`,
    snapshot.location ? `Job location: ${snapshot.location}` : "",
    snapshot.phoneDisplay ? `Phone: ${snapshot.phoneDisplay}` : "",
    snapshot.email ? `Email: ${snapshot.email}` : "",
    snapshot.workDate ? `Work date: ${formatDateOnly(snapshot.workDate)}` : "",
    snapshot.documentType === "estimate" && snapshot.validUntil
      ? `Valid until: ${formatDateOnly(snapshot.validUntil)}`
      : "",
    snapshot.documentType === "invoice" && snapshot.total > 0
      ? `Total project amount: ${formatCurrency(snapshot.total)}`
      : snapshot.total > 0
        ? `Total: ${formatCurrency(snapshot.total)}`
        : "",
    snapshot.documentType === "invoice" && snapshot.deposit > 0
      ? `Deposit received: ${formatCurrency(snapshot.deposit)}`
      : "",
    snapshot.documentType === "invoice" && snapshot.total > 0
      ? `Balance due: ${formatCurrency(snapshot.balanceDue)}`
      : "",
    "",
    snapshot.description ? `Work to be performed:\n${snapshot.description}` : "",
    snapshot.warranty ? `Warranty / terms:\n${snapshot.warranty}` : "",
    "",
    `To move forward, reply to this email or call/text ${METALWORKS_CONTACT.phoneDisplay}.`,
    "",
    METALWORKS_CONTACT.companyName,
    METALWORKS_CONTACT.phoneDisplay,
    METALWORKS_CONTACT.email,
    METALWORKS_CONTACT.website,
  ].filter(Boolean);

  return lines.join("\n");
}

function buildEstimateTextMessage(snapshot) {
  const lines = [
    `${snapshot.documentLabel} - ${METALWORKS_CONTACT.companyName}`,
    snapshot.projectLabel ? `Project: ${snapshot.projectLabel}` : "",
    snapshot.fullName ? `Client: ${snapshot.fullName}` : "",
    snapshot.location ? `Location: ${snapshot.location}` : "",
    snapshot.workDate ? `Work date: ${formatDateOnly(snapshot.workDate)}` : "",
    snapshot.documentType === "invoice" && snapshot.total > 0
      ? `Total project amount: ${formatCurrency(snapshot.total)}`
      : snapshot.total > 0
        ? `Total: ${formatCurrency(snapshot.total)}`
        : "",
    snapshot.documentType === "invoice" && snapshot.deposit > 0
      ? `Deposit received: ${formatCurrency(snapshot.deposit)}`
      : "",
    snapshot.documentType === "invoice" && snapshot.total > 0
      ? `Balance due: ${formatCurrency(snapshot.balanceDue)}`
      : "",
    "",
    snapshot.description ? `Work: ${snapshot.description}` : "",
    snapshot.warranty ? `Warranty: ${snapshot.warranty}` : "",
    "",
    `Call/Text: ${METALWORKS_CONTACT.phoneDisplay}`,
  ].filter(Boolean);

  return lines.join("\n");
}

function openEmailDraft({ silent = false } = {}) {
  const snapshot = buildEstimateSnapshot();
  const validationMessage = getInvoiceValidationMessage(snapshot);

  if (validationMessage) {
    if (!silent) {
      setDetailFeedback(validationMessage, "error");
    }
    return false;
  }

  if (!snapshot.email) {
    setDetailFeedback("Este lead no tiene correo todavia.", "error");
    return false;
  }

  const mailto = buildEstimateMailto(snapshot);
  const link = document.createElement("a");
  link.href = mailto;
  link.style.display = "none";
  document.body.appendChild(link);
  link.click();
  link.remove();

  if (!silent) {
    setDetailFeedback("Draft abierto en tu correo.", "muted");
  }

  return true;
}

async function copyTextWithFallback(text = "") {
  const safeText = String(text || "");

  if (!safeText) {
    return false;
  }

  if (navigator.clipboard?.writeText && window.isSecureContext) {
    try {
      await navigator.clipboard.writeText(safeText);
      return true;
    } catch {}
  }

  const helper = document.createElement("textarea");
  helper.value = safeText;
  helper.setAttribute("readonly", "true");
  helper.style.position = "fixed";
  helper.style.opacity = "0";
  helper.style.pointerEvents = "none";
  document.body.appendChild(helper);
  helper.focus();
  helper.select();

  let copied = false;

  try {
    copied = document.execCommand("copy");
  } catch {
    copied = false;
  }

  helper.remove();

  if (copied) {
    return true;
  }

  window.prompt("Copy this estimate text:", safeText);
  return false;
}

async function copyEstimateText() {
  const snapshot = buildEstimateSnapshot();
  const validationMessage = getInvoiceValidationMessage(snapshot);

  if (validationMessage) {
    setDetailFeedback(validationMessage, "error");
    return;
  }

  if (!snapshot.description && !snapshot.total) {
    setDetailFeedback(
      `Primero arma la descripcion o el total del ${snapshot.documentType === "invoice" ? "invoice" : "estimate"} para poder copiarlo.`,
      "error",
    );
    return;
  }

  try {
    const copied = await copyTextWithFallback(buildEstimateTextMessage(snapshot));
    setDetailFeedback(
      copied
        ? `${snapshot.documentLabel} text copiado.`
        : `No pude copiarlo automatico. Te deje el ${snapshot.documentType === "invoice" ? "invoice" : "estimate"} listo para copiar manualmente.`,
      copied ? "success" : "muted",
    );
  } catch (error) {
    setDetailFeedback("No pude copiarlo automaticamente.", "error");
  }
}

function focusEstimateComposer() {
  setDetailTab("profile");

  const estimateTitleInput =
    detailForm?.elements?.clientDocumentDescription ||
    detailForm?.elements?.estimateAmount ||
    detailForm?.elements?.estimateTitle;

  if (!estimateTitleInput) {
    return;
  }

  estimateTitleInput.scrollIntoView({
    behavior: "smooth",
    block: "center",
  });

  window.setTimeout(() => {
    estimateTitleInput.focus();
    if (typeof estimateTitleInput.select === "function") {
      estimateTitleInput.select();
    }
  }, 140);
}

async function openQuoteComposer(leadId = "") {
  const safeLeadId = String(leadId || "").trim();

  if (!safeLeadId) {
    return;
  }

  if (state.selectedLeadId !== safeLeadId) {
    rememberSelectedLead(safeLeadId);
    if (state.dashboard?.leads) {
      renderLeadList(state.dashboard.leads || []);
    }
    await loadLeadDetail(safeLeadId);
  }

  openMobileWorkspacePane();
  scrollDetailIntoView();
  focusEstimateComposer();
  const snapshot = buildEstimateSnapshot();
  setDetailFeedback(`${snapshot.documentLabel} listo para editar o enviar.`, "muted");
}

function buildDetailActionPreview() {
  const lead = state.leadDetail?.lead || {};
  const form = detailForm?.elements;

  return {
    ...lead,
    id: state.selectedLeadId || lead.id || "",
    phoneDisplay: String(form?.phoneDisplay?.value || lead.phoneDisplay || lead.phone || "").trim(),
    phone: String(form?.phoneDisplay?.value || lead.phoneDisplay || lead.phone || "").trim(),
    email: String(form?.email?.value || lead.email || "").trim(),
    location: String(form?.location?.value || lead.location || "").trim(),
    addressLine: String(lead.addressLine || "").trim(),
    city: String(lead.city || "").trim(),
    zipCode: String(lead.zipCode || "").trim(),
    clientDocumentType: normalizeClientDocumentType(
      form?.clientDocumentType?.value || lead.clientDocumentType || "estimate",
    ),
  };
}

function scrollDetailIntoView() {
  if (!detailPanel || window.innerWidth > 960 || isMobileCrmLayout()) {
    return;
  }

  window.setTimeout(() => {
    detailPanel.scrollIntoView({
      behavior: "smooth",
      block: "start",
    });
  }, 80);
}

function renderSummary(summary = {}, serviceBreakdown = []) {
  if (!summaryWrap) {
    return;
  }

  const cards = [
    {
      label: "Leads totales",
      value: summary.totalLeads || 0,
      note: `${summary.newLeads || 0} nuevos · ${summary.newApplicants || 0} candidatos`,
    },
    {
      label: "Seguimiento activo",
      value: (summary.contactedLeads || 0) + (summary.quotedLeads || 0),
      note: `${summary.bookedLeads || 0} agendados`,
    },
    {
      label: "Quotes 30 dias",
      value: summary.quoteSubmits30d || 0,
      note: `${summary.phoneClicks30d || 0} clicks al telefono`,
    },
    {
      label: "Ganados",
      value: summary.wonLeads || 0,
      note: `${summary.lostLeads || 0} perdidos · ${summary.totalApplicants || 0} candidatos`,
    },
  ];

  const breakdown = serviceBreakdown
    .map((item) => `<span class="crm-chip">${escapeHtml(item.label)}: ${item.count}</span>`)
    .join("");

  summaryWrap.innerHTML = cards
    .map(
      (card, index) => `
        <article class="crm-summary-card">
          <span>${escapeHtml(card.label)}</span>
          <strong>${card.value}</strong>
          <span>${escapeHtml(card.note)}</span>
          ${index === cards.length - 1 && breakdown ? `<div class="crm-micro-list">${breakdown}</div>` : ""}
        </article>
      `,
    )
    .join("");
}

async function openAgendaLead(leadId = "") {
  const safeLeadId = String(leadId || "").trim();

  if (!safeLeadId) {
    return;
  }

  if (state.view !== "leads") {
    await setCrmView("leads");
  }

  rememberSelectedLead(safeLeadId);

  if (state.dashboard?.leads) {
    renderLeadList(state.dashboard.leads || []);
  }

  await loadLeadDetail(safeLeadId);
  openMobileWorkspacePane();
  scrollDetailIntoView();
}

function renderAgenda(leads = []) {
  if (!agendaPanel || !agendaList || !agendaCount) {
    return;
  }

  const safeLeads = Array.isArray(leads) ? leads.filter((lead) => lead?.id) : [];

  agendaPanel.hidden = false;
  agendaCount.textContent = `${safeLeads.length} scheduled`;

  if (!safeLeads.length) {
    agendaList.innerHTML =
      '<p class="crm-empty-state">No scheduled leads yet. As soon as a callback or visit gets a time, it will show here.</p>';
    return;
  }

  agendaList.innerHTML = safeLeads
    .map((lead) => {
      const phoneDigits = getLeadPhoneDigits(lead);
      const scheduleLabel = formatDate(lead.nextActionAt || "") || "No time set";
      const reminderSummary = buildLeadReminderSummary(lead);

      return `
        <article class="crm-agenda-card" data-crm-agenda-lead-id="${escapeHtml(lead.id)}">
          <div class="crm-agenda-card-head">
            <div>
              <h3>${escapeHtml(lead.fullName || "Unknown lead")}</h3>
              <p>${escapeHtml(lead.projectType || "Service not set")} · ${escapeHtml(lead.location || lead.phoneDisplay || lead.email || "")}</p>
            </div>
            <span class="crm-status-badge" data-status="${escapeHtml(lead.status || "new")}">
              ${escapeHtml(lead.statusLabel || "Open")}
            </span>
          </div>
          <div class="crm-micro-list">
            <span class="crm-chip">${escapeHtml(scheduleLabel)}</span>
            ${lead.nextAction ? `<span class="crm-chip">${escapeHtml(lead.nextAction)}</span>` : ""}
            ${reminderSummary ? `<span class="crm-chip">Alerts: ${escapeHtml(reminderSummary)}</span>` : ""}
            ${lead.callbackIntent === "yes" ? '<span class="crm-chip">Callback</span>' : ""}
          </div>
          <div class="crm-lead-card-summary">
            <span>${escapeHtml(lead.details || lead.lastUserMessage || "Open this lead to continue the follow-up.")}</span>
          </div>
          <div class="crm-card-actions">
            <button type="button" class="crm-card-action" data-crm-agenda-open="${escapeHtml(lead.id)}">
              Open
            </button>
            ${
              phoneDigits
                ? `<a href="${escapeHtml(buildTelHref(phoneDigits))}" class="crm-card-action" data-crm-agenda-prevent>Call</a>`
                : ""
            }
            ${
              phoneDigits
                ? `<a href="${escapeHtml(buildSmsHref(phoneDigits))}" class="crm-card-action" data-crm-agenda-prevent>Text</a>`
                : ""
            }
          </div>
        </article>
      `;
    })
    .join("");

  agendaList.querySelectorAll("[data-crm-agenda-prevent]").forEach((element) => {
    element.addEventListener("click", (event) => {
      event.stopPropagation();
    });
  });

  agendaList.querySelectorAll("[data-crm-agenda-open]").forEach((button) => {
    button.addEventListener("click", async (event) => {
      event.stopPropagation();
      await openAgendaLead(button.getAttribute("data-crm-agenda-open") || "");
    });
  });

  agendaList.querySelectorAll("[data-crm-agenda-lead-id]").forEach((card) => {
    card.addEventListener("click", async () => {
      await openAgendaLead(card.getAttribute("data-crm-agenda-lead-id") || "");
    });
  });
}

function setResourceFeedback(message = "", tone = "muted") {
  if (!resourcesFeedback) {
    return;
  }

  resourcesFeedback.textContent = message;
  resourcesFeedback.dataset.tone = tone;
  resourcesFeedback.hidden = !message;
}

function renderResourceHub(resourceSections = []) {
  if (!resourceHub || !resourcesWrap) {
    return;
  }

  const sections = (Array.isArray(resourceSections) ? resourceSections : [])
    .map((section) => ({
      ...section,
      items: (Array.isArray(section?.items) ? section.items : []).filter(
        (item) => String(item?.label || "").trim() && String(item?.url || "").trim(),
      ),
    }))
    .filter((section) => section.items.length);

  if (!sections.length) {
    resourceHub.hidden = true;
    resourcesWrap.innerHTML = "";
    setResourceFeedback("", "muted");
    return;
  }

  resourceHub.hidden = false;
  setResourceFeedback("", "muted");

  resourcesWrap.innerHTML = sections
    .map(
      (section, sectionIndex) => `
        <article class="crm-resource-section">
          <div class="crm-panel-head tight crm-resource-section-head">
            <div>
              <h3>${escapeHtml(section.title || "Links")}</h3>
              <p>${escapeHtml(section.description || "")}</p>
            </div>
          </div>
          <div class="crm-resource-list">
            ${section.items
              .map(
                (item, itemIndex) => {
                  const resourceId =
                    String(item.id || "").trim() || `resource-${sectionIndex}-${itemIndex}`;
                  const qrImageUrl = buildResourceQrImageUrl(item.url || "");

                  return `
                  <article class="crm-resource-item">
                    <div class="crm-resource-copy">
                      <strong>${escapeHtml(item.label || "Link")}</strong>
                      <p>${escapeHtml(item.description || "")}</p>
                      <span class="crm-resource-url">${escapeHtml(item.url || "")}</span>
                    </div>
                    <div class="crm-resource-actions">
                      <a
                        class="crm-card-action"
                        href="${escapeHtml(item.url || "#")}"
                        target="_blank"
                        rel="noopener"
                      >
                        Open
                      </a>
                      <button
                        type="button"
                        class="crm-card-action"
                        data-resource-copy="${escapeHtml(item.url || "")}"
                        data-resource-label="${escapeHtml(item.label || "Link")}"
                      >
                        Copy
                      </button>
                      <button
                        type="button"
                        class="crm-card-action"
                        data-resource-qr-toggle="${escapeHtml(resourceId)}"
                        aria-expanded="false"
                      >
                        QR
                      </button>
                    </div>
                    <div
                      class="crm-resource-qr"
                      data-resource-qr-panel="${escapeHtml(resourceId)}"
                      hidden
                    >
                      <div class="crm-resource-qr-card">
                        <img
                          class="crm-resource-qr-image"
                          src="${escapeHtml(qrImageUrl)}"
                          alt="${escapeHtml(`QR code for ${item.label || "Link"}`)}"
                          loading="lazy"
                        />
                        <div class="crm-resource-qr-copy">
                          <strong>${escapeHtml(item.label || "Link")}</strong>
                          <p>Escanea este QR para abrir la pagina directo en otro telefono.</p>
                          <span class="crm-resource-url">${escapeHtml(
                            buildAbsoluteAppUrl(item.url || ""),
                          )}</span>
                        </div>
                      </div>
                    </div>
                  </article>
                `;
                },
              )
              .join("")}
          </div>
        </article>
      `,
    )
    .join("");

  resourcesWrap.querySelectorAll("[data-resource-copy]").forEach((button) => {
    button.addEventListener("click", async () => {
      const url = String(button.dataset.resourceCopy || "").trim();
      const label = String(button.dataset.resourceLabel || "Link").trim();

      if (!url) {
        setResourceFeedback("Ese link no esta disponible ahorita.", "error");
        return;
      }

      const copied = await copyTextWithFallback(url);
      setResourceFeedback(
        copied
          ? `${label} copiado al portapapeles.`
          : `No pude copiar ${label} automatico, pero ya te deje el link listo para copiar.`,
        copied ? "success" : "muted",
      );
    });
  });

  resourcesWrap.querySelectorAll("[data-resource-qr-toggle]").forEach((button) => {
    button.addEventListener("click", () => {
      const resourceId = String(button.dataset.resourceQrToggle || "").trim();

      if (!resourceId) {
        return;
      }

      const panel = Array.from(resourcesWrap.querySelectorAll("[data-resource-qr-panel]")).find(
        (node) => String(node.dataset.resourceQrPanel || "").trim() === resourceId,
      );

      if (!panel) {
        return;
      }

      const shouldOpen = panel.hidden;

      resourcesWrap.querySelectorAll("[data-resource-qr-panel]").forEach((node) => {
        node.hidden = true;
      });
      resourcesWrap.querySelectorAll("[data-resource-qr-toggle]").forEach((node) => {
        node.setAttribute("aria-expanded", "false");
        node.textContent = "QR";
      });

      if (shouldOpen) {
        panel.hidden = false;
        button.setAttribute("aria-expanded", "true");
        button.textContent = "Ocultar QR";
      }
    });
  });
}

function setProspectorFeedback(message = "", tone = "muted") {
  if (!prospectorFeedback) {
    return;
  }

  prospectorFeedback.textContent = message;
  prospectorFeedback.dataset.tone = tone;
}

function renderProspectorCredentials() {
  if (!prospectorCredentialsCard || !prospectorCredentialsList) {
    return;
  }

  const latest = state.prospectorAdmin?.latestCredentials;

  if (!latest?.email || !latest?.temporaryPassword) {
    prospectorCredentialsCard.hidden = true;
    prospectorCredentialsList.innerHTML = "";
    if (prospectorCredentialsTitle) {
      prospectorCredentialsTitle.textContent = "New account ready";
    }
    return;
  }

  prospectorCredentialsCard.hidden = false;

  if (prospectorCredentialsTitle) {
    prospectorCredentialsTitle.textContent = latest.title || "New account ready";
  }

  prospectorCredentialsList.innerHTML = `
    <p><strong>Email:</strong> ${escapeHtml(latest.email)}</p>
    <p><strong>${escapeHtml(latest.passwordLabel || "Password")}:</strong> ${escapeHtml(latest.temporaryPassword)}</p>
    ${
      latest.loginUrl
        ? `<p><strong>Login page:</strong> ${escapeHtml(latest.loginUrl)}</p>`
        : ""
    }
  `;
}

function renderProspectorAdmin(snapshot = {}) {
  if (!prospectorAdminWrap || !prospectorList || !prospectorSummary) {
    return;
  }

  const summary = snapshot?.summary || {};
  const prospectors = Array.isArray(snapshot?.prospectors) ? snapshot.prospectors : [];
  state.prospectorAdmin = {
    ...state.prospectorAdmin,
    ...snapshot,
    summary,
    prospectors,
  };

  prospectorSummary.innerHTML = [
    `<span class="crm-chip">Total: ${Number(summary.totalProspectors || 0)}</span>`,
    `<span class="crm-chip">Active: ${Number(summary.activeProspectors || 0)}</span>`,
    `<span class="crm-chip">Paused: ${Number(summary.pausedProspectors || 0)}</span>`,
    `<span class="crm-chip">Leads: ${Number(summary.totalLeads || 0)}</span>`,
  ].join("");

  if (!prospectors.length) {
    prospectorList.innerHTML =
      '<div class="crm-prospector-empty">No prospector accounts yet. Create the first one here and send them the login credentials.</div>';
    renderProspectorCredentials();
    return;
  }

  prospectorList.innerHTML = prospectors
    .map(
      (prospector) => `
        <article class="crm-prospector-user-card" data-prospector-id="${escapeHtml(prospector.id || "")}">
          <div class="crm-lead-card-head">
            <div>
              <h3>${escapeHtml(prospector.name || "Prospector")}</h3>
              <div class="crm-lead-card-meta">
                <span>${escapeHtml(prospector.email || "No email")}</span>
                <span>${escapeHtml(prospector.createdAt ? formatDate(prospector.createdAt) : "No date")}</span>
              </div>
            </div>
            <span class="crm-status-badge" data-status="${escapeHtml(prospector.status || "active")}">
              ${escapeHtml(prospector.statusLabel || "Active")}
            </span>
          </div>
          <div class="crm-micro-list">
            <span class="crm-chip">Leads: ${Number(prospector.counts?.totalLeads || 0)}</span>
            <span class="crm-chip">New: ${Number(prospector.counts?.newLeads || 0)}</span>
            <span class="crm-chip">Quoted: ${Number(prospector.counts?.quotedLeads || 0)}</span>
            <span class="crm-chip">Won: ${Number(prospector.counts?.wonLeads || 0)}</span>
          </div>
          <div class="crm-lead-card-summary">
            <span><strong>Last login:</strong> ${escapeHtml(prospector.lastLoginAt ? formatDate(prospector.lastLoginAt) : "Never")}</span>
            <span><strong>Last lead sent:</strong> ${escapeHtml(prospector.lastLeadSubmittedAt ? formatDate(prospector.lastLeadSubmittedAt) : "No leads yet")}</span>
          </div>
          <div class="crm-card-actions">
            <button type="button" class="crm-card-action" data-prospector-copy-email="${escapeHtml(prospector.email || "")}">
              Copy Email
            </button>
            <button type="button" class="crm-card-action" data-prospector-reset-password>
              Reset Password
            </button>
            <button
              type="button"
              class="crm-card-action"
              data-prospector-toggle-status="${escapeHtml(prospector.status === "active" ? "paused" : "active")}"
            >
              ${prospector.status === "active" ? "Pause Access" : "Reactivate"}
            </button>
          </div>
        </article>
      `,
    )
    .join("");

  renderProspectorCredentials();
}

function syncViewButtons() {
  viewButtons.forEach((button) => {
    const viewName = normalizeCrmView(button.dataset.crmViewButton || "leads");
    const isActive = viewName === state.view;
    button.classList.toggle("is-active", isActive);
    button.setAttribute("aria-pressed", isActive ? "true" : "false");
  });
}

function syncManualLeadUi() {
  const leadsView = state.view === "leads";

  if (newLeadToggleButton) {
    newLeadToggleButton.hidden = !leadsView;
    newLeadToggleButton.disabled = state.manualLeadBusy;
    newLeadToggleButton.textContent =
      manualLeadPanel?.hidden === false ? "Hide Form" : "New Lead";
  }

  if (manualLeadPanel && !leadsView) {
    manualLeadPanel.hidden = true;
  }

  if (manualLeadSaveButton) {
    manualLeadSaveButton.disabled = state.manualLeadBusy;
    manualLeadSaveButton.textContent = state.manualLeadBusy ? "Creating..." : "Create Lead";
  }

  if (manualLeadCancelButton) {
    manualLeadCancelButton.disabled = state.manualLeadBusy;
  }
}

function openManualLeadPanel() {
  if (!manualLeadPanel || !manualLeadForm) {
    return;
  }

  manualLeadPanel.hidden = false;
  setManualLeadFeedback("", "");
  syncManualLeadUi();

  window.setTimeout(() => {
    manualLeadForm.elements.fullName?.focus();
  }, 60);
}

function closeManualLeadPanel({ resetForm = true } = {}) {
  if (!manualLeadPanel) {
    return;
  }

  manualLeadPanel.hidden = true;

  if (resetForm) {
    manualLeadForm?.reset();
  }

  setManualLeadFeedback("", "");
  syncManualLeadUi();
}

function syncCollectionChrome() {
  const applicantsView = state.view === "applicants";

  if (collectionTitle) {
    collectionTitle.textContent = applicantsView ? "Applicants" : "Leads";
  }

  if (detailTitle) {
    detailTitle.textContent = applicantsView ? "Detalle del candidato" : "Detalle del lead";
  }

  if (detailEmpty) {
    detailEmpty.textContent = applicantsView
      ? "Selecciona un candidato para ver su perfil, conversacion y timeline."
      : "Selecciona un lead para ver su informacion y trabajar seguimiento.";
  }

  if (searchLabel) {
    searchLabel.textContent = "Buscar";
  }

  if (statusLabel) {
    statusLabel.textContent = "Status";
  }

  if (serviceLabel) {
    serviceLabel.textContent = applicantsView ? "Puesto" : "Servicio";
  }

  if (searchInput) {
    searchInput.placeholder = applicantsView
      ? "Nombre, puesto, telefono, email o idioma"
      : "Nombre, telefono, servicio o direccion";
  }

  syncViewButtons();
  syncManualLeadUi();
  syncMobileShellCopy();
}

function renderLeadFilters(dashboard) {
  if (!dashboard) {
    return;
  }

  const statusOptions = Array.isArray(dashboard.statusOptions)
    ? dashboard.statusOptions
    : [];
  const services = Array.isArray(dashboard.serviceBreakdown)
    ? dashboard.serviceBreakdown.map((item) => item.label).filter(Boolean)
    : [];

  if (statusFilter) {
    statusFilter.innerHTML = ['<option value="">Todos</option>']
      .concat(
        statusOptions.map(
          (item) =>
            `<option value="${escapeHtml(item.value)}">${escapeHtml(item.label)}</option>`,
        ),
      )
      .join("");
    statusFilter.value = state.filters.status;
  }

  if (statusInput) {
    statusInput.innerHTML = statusOptions
      .map(
        (item) =>
          `<option value="${escapeHtml(item.value)}">${escapeHtml(item.label)}</option>`,
      )
      .join("");
  }

  if (serviceFilter) {
    serviceFilter.innerHTML = ['<option value="">Todos</option>']
      .concat(
        services.map(
          (item) => `<option value="${escapeHtml(item)}">${escapeHtml(item)}</option>`,
        ),
      )
      .join("");
    serviceFilter.value = state.filters.projectType;
  }

  if (searchInput) {
    searchInput.value = state.filters.search;
  }
}

function renderApplicantFilters(applicants = []) {
  const roles = [...new Set(
    (Array.isArray(applicants) ? applicants : [])
      .map((item) => String(item?.positionApplied || item?.roleTrack || "").trim())
      .filter(Boolean),
  )].sort((left, right) => left.localeCompare(right));

  if (statusFilter) {
    statusFilter.innerHTML = ['<option value="">Todos</option>']
      .concat(
        APPLICANT_STATUS_OPTIONS.map(
          (item) =>
            `<option value="${escapeHtml(item.value)}">${escapeHtml(item.label)}</option>`,
        ),
      )
      .join("");
    statusFilter.value = state.applicantFilters.status;
  }

  if (serviceFilter) {
    serviceFilter.innerHTML = ['<option value="">Todos</option>']
      .concat(
        roles.map((item) => `<option value="${escapeHtml(item)}">${escapeHtml(item)}</option>`),
      )
      .join("");
    serviceFilter.value = state.applicantFilters.role;
  }

  if (searchInput) {
    searchInput.value = state.applicantFilters.search;
  }
}

function renderFilters(dashboard) {
  syncCollectionChrome();

  if (state.view === "applicants") {
    renderApplicantFilters(state.applicants);
    return;
  }

  renderLeadFilters(dashboard);
}

function getFilteredApplicants(applicants = []) {
  const filters = state.applicantFilters;
  const safeApplicants = Array.isArray(applicants) ? applicants : [];
  const searchNeedle = String(filters.search || "").trim().toLowerCase();

  return safeApplicants.filter((applicant) => {
    if (filters.status && applicant.status !== filters.status) {
      return false;
    }

    const roleValue = String(applicant.positionApplied || applicant.roleTrack || "").trim();
    if (filters.role && roleValue !== filters.role) {
      return false;
    }

    if (!searchNeedle) {
      return true;
    }

    const haystack = [
      applicant.fullName,
      applicant.phoneDisplay,
      applicant.email,
      applicant.positionApplied,
      applicant.roleTrack,
      applicant.languages,
      applicant.yearsExperience,
      applicant.experienceSummary,
      applicant.location,
      applicant.detailsSummary,
    ]
      .filter(Boolean)
      .join(" ")
      .toLowerCase();

    return haystack.includes(searchNeedle);
  });
}

function renderLeadList(leads = []) {
  if (!leadList) {
    return;
  }

  if (collectionCount) {
    collectionCount.textContent = `${leads.length} lead${leads.length === 1 ? "" : "s"}`;
  }

  if (!leads.length) {
    leadList.innerHTML = "";
    emptyState.hidden = false;
    return;
  }

  emptyState.hidden = true;
  leadList.innerHTML = leads
    .map((lead) => {
      const isActive = lead.id === state.selectedLeadId;
      const phoneDigits = getLeadPhoneDigits(lead);
      const documentLabel = getClientDocumentLabel(lead.clientDocumentType || "");
      const summaryText = buildLeadSummaryText(lead);

      return `
        <article class="crm-lead-card ${isActive ? "is-active" : ""}" data-lead-id="${escapeHtml(lead.id)}">
          <div class="crm-lead-card-head">
            <div>
              <h3>${escapeHtml(lead.fullName || "Sin nombre")}</h3>
              <div class="crm-lead-card-meta">
                <span>${escapeHtml(lead.projectType || "Servicio no definido")}</span>
                <span>${escapeHtml(lead.phoneDisplay || lead.phone || "")}</span>
                <span>${escapeHtml(lead.location || lead.email || "")}</span>
              </div>
            </div>
            <span class="crm-status-badge" data-status="${escapeHtml(lead.status)}">
              ${escapeHtml(lead.statusLabel)}
            </span>
          </div>
          <div class="crm-micro-list">
            <span class="crm-chip">${escapeHtml(formatDate(lead.createdAt) || "Sin fecha")}</span>
            ${lead.estimateAmount ? `<span class="crm-chip">${escapeHtml(formatCurrency(lead.estimateAmount))}</span>` : ""}
            ${lead.sourceType ? `<span class="crm-chip">${escapeHtml(formatLeadSource(lead.sourceType))}</span>` : ""}
            ${lead.nextAction ? `<span class="crm-chip">Next: ${escapeHtml(lead.nextAction)}</span>` : ""}
            ${
              lead.callbackIntent === "yes" && lead.nextActionAt
                ? `<span class="crm-chip">Callback: ${escapeHtml(formatDate(lead.nextActionAt))}</span>`
                : ""
            }
          </div>
          <div class="crm-lead-card-summary">
            ${
              lead.lastContactAt
                ? `<span><strong>Last contact:</strong> ${escapeHtml(formatDate(lead.lastContactAt))}</span>`
                : ""
            }
            ${
              lead.estimateSentAt
                ? `<span><strong>Document sent:</strong> ${escapeHtml(formatDate(lead.estimateSentAt))}</span>`
                : ""
            }
            ${summaryText ? `<span>${escapeHtml(summaryText)}</span>` : ""}
          </div>
          <div class="crm-card-actions">
            ${
              phoneDigits
                ? `<a href="${escapeHtml(buildTelHref(phoneDigits))}" class="crm-card-action" data-prevent-select>Call</a>`
                : ""
            }
            ${
              phoneDigits
                ? `<a href="${escapeHtml(buildSmsHref(phoneDigits))}" class="crm-card-action" data-prevent-select>Text</a>`
                : ""
            }
            <button type="button" class="crm-card-action" data-card-open-quote="${escapeHtml(lead.id)}">Open ${escapeHtml(documentLabel)}</button>
          </div>
        </article>
      `;
    })
    .join("");

  leadList.querySelectorAll("[data-prevent-select]").forEach((element) => {
    element.addEventListener("click", (event) => {
      event.stopPropagation();
    });
  });

  leadList.querySelectorAll("[data-card-open-quote]").forEach((button) => {
    button.addEventListener("click", async (event) => {
      event.stopPropagation();
      const leadId = String(button.getAttribute("data-card-open-quote") || "").trim();
      if (!leadId) {
        return;
      }
      await openQuoteComposer(leadId);
    });
  });

  leadList.querySelectorAll("[data-lead-id]").forEach((card) => {
    card.addEventListener("click", async () => {
      const leadId = String(card.getAttribute("data-lead-id") || "").trim();
      if (!leadId) {
        return;
      }

      rememberSelectedLead(leadId);
      renderLeadList(leads);
      await loadLeadDetail(leadId);
      openMobileWorkspacePane();
      scrollDetailIntoView();
    });
  });
}

function renderApplicantList(applicants = []) {
  if (!leadList) {
    return;
  }

  if (collectionCount) {
    collectionCount.textContent = `${applicants.length} applicant${applicants.length === 1 ? "" : "s"}`;
  }

  if (!applicants.length) {
    leadList.innerHTML = "";
    emptyState.hidden = false;
    return;
  }

  emptyState.hidden = true;
  leadList.innerHTML = applicants
    .map((applicant) => {
      const isActive = applicant.id === state.selectedApplicantId;
      const phoneDigits = normalizePhoneDigits(applicant.phone || applicant.phoneDisplay || "");
      const roleLabel = applicant.positionApplied || applicant.roleTrack || "Role pending";
      const interviewChip = applicant.nextActionAt
        ? `Interview: ${formatDate(applicant.nextActionAt)}`
        : applicant.nextAction
          ? `Next: ${applicant.nextAction}`
          : "";
      const summaryText =
        truncateText(applicant.detailsSummary || applicant.experienceSummary || "", 160) ||
        "No summary captured yet.";

      return `
        <article class="crm-lead-card crm-applicant-card ${isActive ? "is-active" : ""}" data-applicant-id="${escapeHtml(applicant.id)}">
          <div class="crm-lead-card-head">
            <div>
              <h3>${escapeHtml(applicant.fullName || "Sin nombre")}</h3>
              <div class="crm-lead-card-meta">
                <span>${escapeHtml(roleLabel)}</span>
                <span>${escapeHtml(applicant.phoneDisplay || applicant.email || "Sin contacto")}</span>
                <span>${escapeHtml(applicant.location || applicant.languages || "")}</span>
              </div>
            </div>
            <span class="crm-status-badge" data-status="${escapeHtml(applicant.status)}">
              ${escapeHtml(applicant.statusLabel)}
            </span>
          </div>
          <div class="crm-micro-list">
            <span class="crm-chip">${escapeHtml(formatDate(applicant.createdAt) || "Sin fecha")}</span>
            ${applicant.languages ? `<span class="crm-chip">${escapeHtml(applicant.languages)}</span>` : ""}
            ${applicant.yearsExperience ? `<span class="crm-chip">${escapeHtml(applicant.yearsExperience)} yrs</span>` : ""}
            ${interviewChip ? `<span class="crm-chip">${escapeHtml(interviewChip)}</span>` : ""}
            <span class="crm-chip">${escapeHtml(formatApplicantSource(applicant))}</span>
          </div>
          <div class="crm-lead-card-summary">
            <span>${escapeHtml(summaryText)}</span>
          </div>
          <div class="crm-card-actions">
            ${
              phoneDigits
                ? `<a href="${escapeHtml(buildTelHref(phoneDigits))}" class="crm-card-action" data-prevent-select>Call</a>`
                : ""
            }
            ${
              phoneDigits
                ? `<a href="${escapeHtml(buildSmsHref(phoneDigits))}" class="crm-card-action" data-prevent-select>Text</a>`
                : ""
            }
            ${
              applicant.email
                ? `<a href="mailto:${escapeHtml(applicant.email)}" class="crm-card-action" data-prevent-select>Email</a>`
                : `<button type="button" class="crm-card-action" data-card-open-applicant="${escapeHtml(applicant.id)}">Open profile</button>`
            }
          </div>
        </article>
      `;
    })
    .join("");

  leadList.querySelectorAll("[data-prevent-select]").forEach((element) => {
    element.addEventListener("click", (event) => {
      event.stopPropagation();
    });
  });

  leadList.querySelectorAll("[data-card-open-applicant]").forEach((button) => {
    button.addEventListener("click", async (event) => {
      event.stopPropagation();
      const applicantId = String(button.getAttribute("data-card-open-applicant") || "").trim();

      if (!applicantId) {
        return;
      }

      rememberSelectedApplicant(applicantId);
      renderApplicantList(applicants);
      await loadApplicantDetail(applicantId);
      openMobileWorkspacePane();
      scrollDetailIntoView();
    });
  });

  leadList.querySelectorAll("[data-applicant-id]").forEach((card) => {
    card.addEventListener("click", async () => {
      const applicantId = String(card.getAttribute("data-applicant-id") || "").trim();

      if (!applicantId) {
        return;
      }

      rememberSelectedApplicant(applicantId);
      renderApplicantList(applicants);
      await loadApplicantDetail(applicantId);
      openMobileWorkspacePane();
      scrollDetailIntoView();
    });
  });
}

function renderActivityCards(target, activities = [], options = {}) {
  if (!target) {
    return;
  }

  const hideBody = Boolean(options.hideBody);

  if (!activities.length) {
    target.innerHTML = '<p class="crm-empty-state">Aun no hay actividad guardada.</p>';
    return;
  }

  target.innerHTML = activities
    .map(
      (item) => `
        <article class="crm-activity-card">
          <div class="crm-lead-card-head">
            <h3>${escapeHtml(item.title || "Actividad")}</h3>
            <span class="crm-chip">${escapeHtml(formatDate(item.createdAt) || "")}</span>
          </div>
          ${item.body && !hideBody ? `<p>${escapeHtml(item.body)}</p>` : ""}
          ${
            item.pagePath
              ? `<div class="crm-activity-meta"><span>${escapeHtml(item.pagePath)}</span></div>`
              : ""
          }
        </article>
      `,
    )
    .join("");
}

function renderConversationThread(
  target,
  summaryTarget,
  messages = [],
  {
    assistantLabel = "Agustin 2.0",
    userLabel = "Cliente",
    emptySummary = "Sin transcript guardado.",
    emptyBody = "Todavia no hay conversacion guardada para este registro.",
    conversationSummary = "",
  } = {},
) {
  if (!target || !summaryTarget) {
    return;
  }

  const safeMessages = Array.isArray(messages) ? messages.filter((item) => item?.content) : [];
  const safeConversationSummary = String(conversationSummary || "").trim();

  if (!safeMessages.length) {
    summaryTarget.textContent = safeConversationSummary || emptySummary;
    target.innerHTML = `<p class="crm-empty-state">${escapeHtml(emptyBody)}</p>`;
    return;
  }

  summaryTarget.textContent = safeConversationSummary
    ? `${safeConversationSummary} · ${safeMessages.length} mensaje${safeMessages.length === 1 ? "" : "s"} guardados`
    : `${safeMessages.length} mensaje${safeMessages.length === 1 ? "" : "s"} guardados`;
  target.innerHTML = safeMessages
    .map((item) => {
      const role = item.role === "assistant" ? "assistant" : "user";
      const roleLabel = role === "assistant" ? assistantLabel : userLabel;

      return `
        <article class="crm-conversation-card" data-role="${escapeHtml(role)}">
          <div class="crm-lead-card-head">
            <h3>${escapeHtml(roleLabel)}</h3>
            <span class="crm-chip">${escapeHtml(formatDate(item.createdAt) || "")}</span>
          </div>
          <p>${escapeHtml(item.content || "")}</p>
        </article>
      `;
    })
    .join("");
}

function renderLeadAssets(assets = []) {
  if (!photoSection || !photoGrid || !photoSummary) {
    return;
  }

  const safeAssets = Array.isArray(assets) ? assets.filter((item) => item?.downloadUrl) : [];
  photoSection.hidden = false;

  if (!safeAssets.length) {
    photoSummary.textContent = "No photos yet.";
    photoGrid.innerHTML = `
      <div class="crm-photo-empty">
        Upload site photos, inspiration shots, or follow-up pictures so the whole team can see them from this lead.
      </div>
    `;
    return;
  }

  photoSummary.textContent = `${safeAssets.length} photo${safeAssets.length === 1 ? "" : "s"} saved`;
  photoGrid.innerHTML = safeAssets
    .map(
      (asset) => `
        <a
          class="crm-photo-card"
          href="${escapeHtml(asset.downloadUrl || "#")}"
          target="_blank"
          rel="noreferrer"
        >
          <img
            src="${escapeHtml(asset.downloadUrl || "")}"
            alt="${escapeHtml(asset.fileName || "Project photo")}"
            loading="lazy"
          />
          <span>${escapeHtml(asset.fileName || "Project photo")}</span>
        </a>
      `,
    )
    .join("");
}

function syncManualPhotoUploadUi(lead = null) {
  const enabled = Boolean(lead?.id) && !state.manualPhotoUploadBusy;

  if (photoUploadTrigger) {
    photoUploadTrigger.disabled = !enabled;
    photoUploadTrigger.textContent = state.manualPhotoUploadBusy ? "Uploading..." : "Add Photos";
  }

  if (photoUploadInput) {
    photoUploadInput.disabled = !enabled;
  }
}

function syncDetailQuickActions(lead = null) {
  const phoneDigits = getLeadPhoneDigits(lead);
  const hasPhone = Boolean(phoneDigits);
  const hasEmail = Boolean(lead?.email);
  const mapsHref = buildMapsHref(lead);
  const hasMap = mapsHref !== "#";
  const documentType = normalizeClientDocumentType(lead?.clientDocumentType || "");
  const documentLabel = getClientDocumentLabel(documentType);

  if (callLink) {
    callLink.hidden = !hasPhone;
    callLink.href = hasPhone ? buildTelHref(phoneDigits) : "#";
  }

  if (textLink) {
    textLink.hidden = !hasPhone;
    textLink.href = hasPhone ? buildSmsHref(phoneDigits) : "#";
  }

  if (mapLink) {
    mapLink.hidden = !hasMap;
    mapLink.href = hasMap ? mapsHref : "#";
  }

  if (markQuotedButton) {
    markQuotedButton.disabled = !lead?.id;
  }

  if (sendEstimateButton) {
    sendEstimateButton.disabled = !lead?.id || !hasEmail;
    sendEstimateButton.textContent = `Send ${documentLabel}`;
  }

  if (openEmailDraftButton) {
    openEmailDraftButton.disabled = !lead?.id || !hasEmail;
    openEmailDraftButton.textContent = `Open ${documentLabel} Draft`;
  }

  if (copyEstimateButton) {
    copyEstimateButton.disabled = !lead?.id;
    copyEstimateButton.textContent = `Copy ${documentLabel} Text`;
  }

  if (deleteLeadButton) {
    deleteLeadButton.disabled = !lead?.id;
  }

  syncManualPhotoUploadUi(lead);
}

function syncLiveChatComposer(lead = null) {
  const enabled = Boolean(lead?.id && lead?.supportsLiveChatReply);

  if (liveChatPanel) {
    liveChatPanel.hidden = !enabled;
  }

  if (liveChatForm && !enabled) {
    liveChatForm.reset();
  }

  if (liveChatSendButton) {
    liveChatSendButton.disabled = !enabled || state.liveChatReplyBusy;
  }

  if (!enabled) {
    setLiveChatFeedback("", "");
  }
}

function syncApplicantQuickActions(applicant = null) {
  const phoneDigits = normalizePhoneDigits(applicant?.phone || applicant?.phoneDisplay || "");
  const hasPhone = Boolean(phoneDigits);
  const hasEmail = Boolean(applicant?.email);

  if (applicantCallLink) {
    applicantCallLink.hidden = !hasPhone;
    applicantCallLink.href = hasPhone ? buildTelHref(phoneDigits) : "#";
  }

  if (applicantTextLink) {
    applicantTextLink.hidden = !hasPhone;
    applicantTextLink.href = hasPhone ? buildSmsHref(phoneDigits) : "#";
  }

  if (applicantEmailLink) {
    applicantEmailLink.hidden = !hasEmail;
    applicantEmailLink.href = hasEmail ? `mailto:${applicant.email}` : "#";
  }
}

function setApplicantDetailTab(tab = "profile") {
  const nextTab = String(tab || "profile").trim() || "profile";
  state.applicantDetailTab = nextTab;

  applicantDetailTabButtons.forEach((button) => {
    const isActive = String(button.dataset.crmApplicantDetailTab || "") === nextTab;
    button.classList.toggle("is-active", isActive);
    button.setAttribute("aria-pressed", isActive ? "true" : "false");
  });

  applicantDetailViews.forEach((view) => {
    const viewName = String(view.dataset.crmApplicantDetailView || "").trim();
    view.hidden = viewName !== nextTab;
  });
}

function renderApplicantDetail(detail = null) {
  state.applicantDetail = detail;

  if (!detail?.applicant) {
    applicantDetailWrap.hidden = true;
    if (state.view === "applicants") {
      detailWrap.hidden = true;
      detailEmpty.hidden = false;
      detailStatus.textContent = "Selecciona un candidato";
    }
    if (applicantMeta) {
      applicantMeta.innerHTML = "";
    }
    if (applicantProfileCard) {
      applicantProfileCard.innerHTML =
        '<p class="crm-empty-state">Aun no hay informacion detallada para este candidato.</p>';
    }
    renderConversationThread(
      applicantConversationThread,
      applicantConversationSummary,
      [],
      {
        assistantLabel: "Agustin Hiring",
        userLabel: "Candidato",
        emptyBody: "Todavia no hay conversacion guardada para este candidato.",
      },
    );
    renderActivityCards(applicantActivityList, []);
    syncApplicantQuickActions(null);
    setApplicantDetailTab("profile");
    applyMobilePaneLayout();
    return;
  }

  const applicant = detail.applicant;
  const manualNotes = stripGeneratedApplicantNotes(applicant.privateNotes || "");
  const roleLabel = applicant.positionApplied || applicant.roleTrack || "Role pending";

  detailWrap.hidden = true;
  applicantDetailWrap.hidden = false;
  detailEmpty.hidden = true;
  detailStatus.textContent = `${applicant.statusLabel} · ${roleLabel}`;

  if (applicantMeta) {
    applicantMeta.innerHTML = `
      <div class="crm-micro-list">
        ${applicant.phoneDisplay ? `<span class="crm-chip">${escapeHtml(applicant.phoneDisplay)}</span>` : ""}
        ${applicant.email ? `<span class="crm-chip">${escapeHtml(applicant.email)}</span>` : ""}
        ${applicant.location ? `<span class="crm-chip">${escapeHtml(applicant.location)}</span>` : ""}
        <span class="crm-chip">${escapeHtml(formatApplicantSource(applicant))}</span>
      </div>
      <div class="crm-detail-meta">
        <span><strong>Creado:</strong> ${escapeHtml(formatDate(applicant.createdAt) || "Sin fecha")}</span>
        <span><strong>Actualizado:</strong> ${escapeHtml(formatDate(applicant.updatedAt) || "Sin registrar")}</span>
        ${
          applicant.lastContactAt
            ? `<span><strong>Ultimo contacto:</strong> ${escapeHtml(formatDate(applicant.lastContactAt))}</span>`
            : ""
        }
        ${
          applicant.interviewRequestedAt
            ? `<span><strong>Interview requested:</strong> ${escapeHtml(formatDate(applicant.interviewRequestedAt))}</span>`
            : ""
        }
        ${
          applicant.nextActionAt
            ? `<span><strong>Next action:</strong> ${escapeHtml(formatDate(applicant.nextActionAt))}</span>`
            : ""
        }
        <span><strong>Pagina:</strong> ${escapeHtml(applicant.pagePath || applicant.pageUrl || "Sin dato")}</span>
      </div>
    `;
  }

  if (applicantProfileCard) {
    applicantProfileCard.innerHTML = `
      <div class="crm-panel-head tight">
        <div>
          <h3>Hiring Profile</h3>
          <p>Calificacion capturada por Agustin 2.0</p>
        </div>
      </div>
      <div class="crm-intake-grid">
        <span><strong>Position:</strong> ${escapeHtml(roleLabel)}</span>
        <span><strong>Languages:</strong> ${escapeHtml(applicant.languages || "Pending")}</span>
        <span><strong>Years experience:</strong> ${escapeHtml(applicant.yearsExperience || "Pending")}</span>
        <span><strong>Own tools:</strong> ${escapeHtml(formatApplicantAnswer(applicant.hasTools))}</span>
        <span><strong>Transportation:</strong> ${escapeHtml(formatApplicantAnswer(applicant.hasTransportation))}</span>
        <span><strong>Field ready:</strong> ${escapeHtml(formatApplicantAnswer(applicant.fieldReady))}</span>
        <span><strong>Best day:</strong> ${escapeHtml(applicant.nextAction || "Pending")}</span>
        <span><strong>Interview time:</strong> ${escapeHtml(applicant.nextActionAt ? formatDate(applicant.nextActionAt) : "Pending")}</span>
      </div>
      ${
        applicant.experienceSummary
          ? `<p class="crm-intake-note"><strong>Background:</strong> ${escapeHtml(applicant.experienceSummary)}</p>`
          : ""
      }
      ${
        applicant.detailsSummary
          ? `<p class="crm-intake-note"><strong>Summary:</strong> ${escapeHtml(applicant.detailsSummary)}</p>`
          : ""
      }
      ${
        manualNotes
          ? `<p class="crm-intake-note"><strong>Manual notes:</strong> ${escapeHtml(manualNotes)}</p>`
          : ""
      }
      ${
        applicant.lastUserMessage
          ? `<p class="crm-intake-note"><strong>Last candidate message:</strong> ${escapeHtml(applicant.lastUserMessage)}</p>`
          : ""
      }
    `;
  }

  syncApplicantQuickActions(applicant);
  renderConversationThread(
    applicantConversationThread,
    applicantConversationSummary,
    applicant.conversationHistory || [],
    {
      assistantLabel: "Agustin Hiring",
      userLabel: "Candidato",
      emptyBody: "Todavia no hay conversacion guardada para este candidato.",
    },
  );
  renderActivityCards(applicantActivityList, detail.activity || []);
  setApplicantDetailTab(state.applicantDetailTab || "profile");
  applyMobilePaneLayout();
}

function renderLeadDetail(detail = null) {
  state.leadDetail = detail;

  if (!detail?.lead) {
    detailWrap.hidden = true;
    if (state.view === "leads") {
      applicantDetailWrap.hidden = true;
      detailEmpty.hidden = false;
      detailStatus.textContent = "Selecciona un lead";
    }
    detailMeta.innerHTML = "";
    activityList.innerHTML = "";
    renderConversationThread(conversationThread, conversationSummary, []);
    renderLeadAssets([]);
    setPhotoUploadFeedback("", "");
    syncDetailQuickActions(null);
    syncLiveChatComposer(null);
    setDetailTab("profile");
    applyMobilePaneLayout();
    return;
  }

  const lead = detail.lead;
  const isLiveChatThread = Boolean(lead.supportsLiveChatReply);
  persistLeadDetail(detail);
  detailWrap.hidden = false;
  applicantDetailWrap.hidden = true;
  detailEmpty.hidden = true;
  detailStatus.textContent = `${lead.statusLabel} · ${lead.projectType || "Sin servicio"}`;
  detailMeta.innerHTML = `
    <div class="crm-micro-list">
      <span class="crm-chip">${escapeHtml(lead.phoneDisplay || lead.phone || "")}</span>
      ${lead.email ? `<span class="crm-chip">${escapeHtml(lead.email)}</span>` : ""}
      ${lead.location ? `<span class="crm-chip">${escapeHtml(lead.location)}</span>` : ""}
      ${lead.estimateAmount ? `<span class="crm-chip">${escapeHtml(formatCurrency(lead.estimateAmount))}</span>` : ""}
      ${
        lead.invoiceDepositAmount
          ? `<span class="crm-chip">Deposit ${escapeHtml(formatCurrency(lead.invoiceDepositAmount))}</span>`
          : ""
      }
      ${
        lead.invoiceBalanceDue && lead.clientDocumentType === "invoice"
          ? `<span class="crm-chip">Balance ${escapeHtml(formatCurrency(lead.invoiceBalanceDue))}</span>`
          : ""
      }
      ${lead.sourceType ? `<span class="crm-chip">${escapeHtml(formatLeadSource(lead.sourceType))}</span>` : ""}
      ${
        lead.callbackIntent === "yes" && lead.nextActionAt
          ? `<span class="crm-chip">Callback ${escapeHtml(formatDate(lead.nextActionAt))}</span>`
          : ""
      }
    </div>
    <div class="crm-detail-meta">
      <span><strong>Creado:</strong> ${escapeHtml(formatDate(lead.createdAt) || "Sin fecha")}</span>
      <span><strong>Ultimo contacto:</strong> ${escapeHtml(formatDate(lead.lastContactAt) || "Sin registrar")}</span>
      ${
        lead.callbackRequestedAt
          ? `<span><strong>Callback requested:</strong> ${escapeHtml(formatDate(lead.callbackRequestedAt))}</span>`
          : ""
      }
      ${
        lead.callbackAlertedAt
          ? `<span><strong>Callback alerted:</strong> ${escapeHtml(formatDate(lead.callbackAlertedAt))}</span>`
          : ""
      }
      ${
        lead.estimateSentAt
          ? `<span><strong>Document sent:</strong> ${escapeHtml(formatDate(lead.estimateSentAt))}</span>`
          : ""
      }
      ${
        lead.estimateSentTo
          ? `<span><strong>Sent to:</strong> ${escapeHtml(lead.estimateSentTo)}</span>`
          : ""
      }
      <span><strong>Pagina:</strong> ${escapeHtml(lead.pagePath || lead.pageUrl || "Sin dato")}</span>
      ${
        lead.tracking?.utmCampaign
          ? `<span><strong>UTM campaign:</strong> ${escapeHtml(lead.tracking.utmCampaign)}</span>`
          : ""
      }
      ${
        lead.tracking?.gclid
          ? `<span><strong>GCLID:</strong> ${escapeHtml(lead.tracking.gclid)}</span>`
          : ""
      }
      ${
        isLiveChatThread
          ? "<span><strong>Canal:</strong> Chat web conectado al CRM</span>"
          : ""
      }
      <span><strong>Proyecto:</strong> ${escapeHtml(lead.details || "")}</span>
      ${
        Array.isArray(lead.photoFileNames) && lead.photoFileNames.length
          ? `<span><strong>Fotos:</strong> ${escapeHtml(lead.photoFileNames.join(", "))}</span>`
          : ""
      }
    </div>
    ${
      lead.sourceType === "field_prospector" || lead.sourceProspectorName || lead.qualificationTier
        ? `
          <section class="crm-intake-card">
            <div class="crm-panel-head tight">
              <h3>Field Intake</h3>
              <p>Info capturada por prospectador en campo</p>
            </div>
            <div class="crm-intake-grid">
              ${
                lead.sourceProspectorName
                  ? `<span><strong>Prospectador:</strong> ${escapeHtml(lead.sourceProspectorName)}${lead.sourceProspectorEmail ? ` · ${escapeHtml(lead.sourceProspectorEmail)}` : ""}</span>`
                  : ""
              }
              ${lead.addressLine ? `<span><strong>Direccion:</strong> ${escapeHtml(lead.addressLine)}</span>` : ""}
              ${lead.zipCode ? `<span><strong>ZIP:</strong> ${escapeHtml(lead.zipCode)}</span>` : ""}
              ${lead.city ? `<span><strong>Ciudad:</strong> ${escapeHtml(lead.city)}</span>` : ""}
              ${lead.propertyType ? `<span><strong>Propiedad:</strong> ${escapeHtml(lead.propertyType)}</span>` : ""}
              ${lead.projectSize ? `<span><strong>Tamano:</strong> ${escapeHtml(lead.projectSize)}</span>` : ""}
              ${lead.timeline ? `<span><strong>Timeline:</strong> ${escapeHtml(lead.timeline)}</span>` : ""}
              ${lead.ownershipStatus ? `<span><strong>Decision maker:</strong> ${escapeHtml(lead.ownershipStatus)}</span>` : ""}
              ${lead.budgetRange ? `<span><strong>Budget:</strong> ${escapeHtml(lead.budgetRange)}</span>` : ""}
              ${lead.urgency ? `<span><strong>Urgencia:</strong> ${escapeHtml(lead.urgency)}</span>` : ""}
              ${lead.bestContactWindow ? `<span><strong>Best time:</strong> ${escapeHtml(lead.bestContactWindow)}</span>` : ""}
              ${lead.preferredLanguage ? `<span><strong>Idioma:</strong> ${escapeHtml(lead.preferredLanguage)}</span>` : ""}
              ${lead.qualificationTier ? `<span><strong>Tier:</strong> ${escapeHtml(lead.qualificationTier)}</span>` : ""}
            </div>
            ${
              lead.qualificationNotes
                ? `<p class="crm-intake-note"><strong>Nota de calificacion:</strong> ${escapeHtml(lead.qualificationNotes)}</p>`
                : ""
            }
          </section>
        `
        : ""
    }
  `;

  if (detailForm) {
    detailForm.elements.fullName.value = lead.fullName || "";
    detailForm.elements.phoneDisplay.value = lead.phoneDisplay || lead.phone || "";
    detailForm.elements.email.value = lead.email || "";
    detailForm.elements.projectType.value = lead.projectType || "";
    detailForm.elements.location.value = lead.location || "";
    detailForm.elements.status.value = lead.status || "new";
    detailForm.elements.bestContactDay.value = lead.bestContactDay || "";
    detailForm.elements.bestContactTime.value = lead.bestContactTime || "";
    detailForm.elements.nextAction.value = lead.nextAction || "";
    detailForm.elements.nextActionAt.value = toDatetimeLocalValue(lead.nextActionAt);
    syncLeadReminderInputs(lead.nextActionReminderOffsets || []);
    detailForm.elements.details.value = lead.details || "";
    detailForm.elements.clientDocumentType.value = normalizeClientDocumentType(
      lead.clientDocumentType || "estimate",
    );
    detailForm.elements.clientDocumentWorkDate.value = toDateInputValue(
      lead.clientDocumentWorkDate || lead.nextActionAt,
    );
    detailForm.elements.estimateAmount.value = lead.estimateAmount || "";
    detailForm.elements.invoiceDepositAmount.value = lead.invoiceDepositAmount || "";
    detailForm.elements.estimateTitle.value = lead.estimateTitle || "";
    detailForm.elements.clientDocumentDescription.value =
      lead.clientDocumentDescription || lead.details || "";
    detailForm.elements.clientDocumentWarranty.value =
      lead.clientDocumentWarranty || DEFAULT_CLIENT_DOCUMENT_WARRANTY;
    detailForm.elements.estimateScope.value = lead.estimateScope || "";
    detailForm.elements.estimateMaterialsCost.value = lead.estimateMaterialsCost || "";
    detailForm.elements.estimateLaborCost.value = lead.estimateLaborCost || "";
    detailForm.elements.estimateCoatingCost.value = lead.estimateCoatingCost || "";
    detailForm.elements.estimateMiscCost.value = lead.estimateMiscCost || "";
    detailForm.elements.estimateDiscount.value = lead.estimateDiscount || "";
    detailForm.elements.estimateValidUntil.value = toDateInputValue(lead.estimateValidUntil);
    detailForm.elements.estimateNotes.value = lead.estimateNotes || "";
    detailForm.elements.privateNotes.value = lead.privateNotes || "";
    if (detailForm.elements.textThreadImportSource) {
      detailForm.elements.textThreadImportSource.value = "";
    }
    if (detailForm.elements.textThreadImport) {
      detailForm.elements.textThreadImport.value = "";
    }
    detailForm.elements.note.value = "";
  }

  syncEstimateTotalFromForm();
  setPhotoUploadFeedback("", "");
  syncDetailQuickActions(lead);
  syncLiveChatComposer(lead);
  renderLeadAssets(detail.assets || []);
  renderConversationThread(conversationThread, conversationSummary, lead.conversationHistory || [], {
    assistantLabel: isLiveChatThread ? "Chicago Metal Works" : "Agustin 2.0",
    userLabel: isLiveChatThread ? "Cliente web" : "Cliente",
    conversationSummary: lead.conversationSummary || "",
  });
  renderActivityCards(activityList, detail.activity || []);
  setDetailTab(state.detailTab || "profile");
  applyMobilePaneLayout();
}

function buildLeadPayloadFromForm() {
  const formData = new FormData(detailForm);
  const lead = state.leadDetail?.lead || {};
  const breakdownMode = hasBreakdownValues();
  const body = {
    fullName: String(formData.get("fullName") || "").trim(),
    phoneDisplay: String(formData.get("phoneDisplay") || "").trim(),
    email: String(formData.get("email") || "").trim(),
    projectType: String(formData.get("projectType") || "").trim(),
    location: String(formData.get("location") || "").trim(),
    status: String(formData.get("status") || "").trim(),
    bestContactDay: String(formData.get("bestContactDay") || "").trim(),
    bestContactTime: String(formData.get("bestContactTime") || "").trim(),
    nextAction: String(formData.get("nextAction") || "").trim(),
    nextActionAt: String(formData.get("nextActionAt") || "").trim(),
    nextActionReminderOffsets: normalizeLeadReminderOffsets(
      formData.getAll("nextActionReminderOffsets"),
    ),
    details: String(formData.get("details") || "").trim(),
    clientDocumentType: String(formData.get("clientDocumentType") || "estimate").trim(),
    clientDocumentWorkDate: String(formData.get("clientDocumentWorkDate") || "").trim(),
    estimateAmount: breakdownMode
      ? String(calculateEstimateTotal(lead))
      : String(formData.get("estimateAmount") || "").trim(),
    invoiceDepositAmount: String(formData.get("invoiceDepositAmount") || "").trim(),
    estimateTitle: String(formData.get("estimateTitle") || "").trim(),
    clientDocumentDescription: String(formData.get("clientDocumentDescription") || "").trim(),
    clientDocumentWarranty: String(formData.get("clientDocumentWarranty") || "").trim(),
    estimateScope: String(formData.get("estimateScope") || "").trim(),
    estimateValidUntil: String(formData.get("estimateValidUntil") || "").trim(),
    estimateNotes: String(formData.get("estimateNotes") || "").trim(),
    privateNotes: String(formData.get("privateNotes") || "").trim(),
    textThreadImportSource: String(formData.get("textThreadImportSource") || "").trim(),
    textThreadImport: String(formData.get("textThreadImport") || "").trim(),
    note: String(formData.get("note") || "").trim(),
  };

  if (breakdownMode) {
    ESTIMATE_COST_FIELDS.forEach((fieldName) => {
      body[fieldName] = String(formData.get(fieldName) || "").trim();
    });
  }

  return body;
}

async function saveLeadChanges(
  overrides = {},
  { successMessage = "", refreshDashboard = true, showFeedback = true } = {},
) {
  if (!state.selectedLeadId || !detailForm) {
    return null;
  }

  const validationMessage = getInvoiceValidationMessage();

  if (validationMessage) {
    if (showFeedback) {
      setDetailFeedback(validationMessage, "error");
    }

    throw createApiError(validationMessage, 400, false);
  }

  if (showFeedback) {
    setDetailFeedback("Guardando...", "muted");
  }

  const body = {
    ...buildLeadPayloadFromForm(),
    ...overrides,
  };

  const detail = await apiRequest(
    `/api/metalworks-crm/leads/${encodeURIComponent(state.selectedLeadId)}`,
    {
      method: "PATCH",
      body,
    },
  );

  renderLeadDetail(detail);

  if (successMessage) {
    setDetailFeedback(successMessage, "success");
  }

  if (refreshDashboard) {
    await loadDashboard();
  }

  return detail;
}

async function renderLeadSnapshot(dashboard, { fromCache = false } = {}) {
  renderFilters(dashboard);
  renderLeadList(dashboard.leads || []);

  if (dashboard.leads?.length) {
    const selectedStillVisible = dashboard.leads.some(
      (lead) => lead.id === state.selectedLeadId,
    );

    if (!selectedStillVisible) {
      rememberSelectedLead(dashboard.leads[0].id);
    }

    if (fromCache) {
      const cachedDetail = readCachedLeadDetail(state.selectedLeadId);

      if (cachedDetail?.data) {
        renderLeadDetail(cachedDetail.data);
      } else {
        renderLeadDetail(null);
      }
    } else {
      await loadLeadDetail(state.selectedLeadId);
    }
  } else {
    rememberSelectedLead("");
    renderLeadDetail(null);
  }
}

async function renderApplicantCollection() {
  renderFilters(state.dashboard);
  const filteredApplicants = getFilteredApplicants(state.applicants);
  renderApplicantList(filteredApplicants);

  if (!filteredApplicants.length) {
    rememberSelectedApplicant("");
    renderApplicantDetail(null);
    return;
  }

  const selectedStillVisible = filteredApplicants.some(
    (applicant) => applicant.id === state.selectedApplicantId,
  );

  if (!selectedStillVisible) {
    rememberSelectedApplicant(filteredApplicants[0].id);
  }

  if (state.applicantDetail?.applicant?.id === state.selectedApplicantId) {
    renderApplicantDetail(state.applicantDetail);
    return;
  }

  try {
    await loadApplicantDetail(state.selectedApplicantId);
  } catch (error) {
    handleCrmError(error, {
      fallbackMessage:
        "No pude cargar el detalle de este candidato. La lista sigue disponible mientras vuelve la conexion.",
    });
    renderApplicantDetail(null);
  }
}

async function loadApplicants() {
  const result = await apiRequest("/api/metalworks-crm/applicants");
  state.applicants = Array.isArray(result.applicants) ? result.applicants : [];
  await renderApplicantCollection();
  return state.applicants;
}

async function loadProspectorAdmin() {
  const snapshot = await apiRequest("/api/metalworks-crm/prospectors");
  renderProspectorAdmin(snapshot);
  return snapshot;
}

async function renderDashboardSnapshot(dashboard, { fromCache = false, savedAt = 0 } = {}) {
  state.dashboard = dashboard;
  const query = buildQueryString(state.filters);
  renderSummary(dashboard.summary, dashboard.serviceBreakdown);
  renderAgenda(dashboard.agendaLeads || []);
  renderActivityCards(globalActivityList, dashboard.recentActivity || [], { hideBody: true });

  if (globalActivitySummary) {
    const totalEvents = Array.isArray(dashboard.recentActivity) ? dashboard.recentActivity.length : 0;
    globalActivitySummary.textContent = `${totalEvents} evento${totalEvents === 1 ? "" : "s"}`;
  }

  if (state.view === "applicants") {
    renderFilters(dashboard);

    if (fromCache) {
      if (state.applicants.length) {
        await renderApplicantCollection();
      } else {
        renderApplicantList([]);
        renderApplicantDetail(null);
      }
    } else {
      await loadApplicants();
    }
  } else {
    await renderLeadSnapshot(dashboard, { fromCache });
  }

  if (fromCache) {
    setSystemStatus(
      `Conexion inestable. Mostrando snapshot guardado ${formatCacheAge(savedAt)}.`,
      "warning",
    );
  } else if (query || state.dashboard) {
    setSystemStatus("", "");
  }
}

function renderCachedDashboard() {
  const entry = getCacheEntry(CRM_DASHBOARD_CACHE_KEY);

  if (!entry?.data) {
    return false;
  }

  renderDashboardSnapshot(entry.data, {
    fromCache: true,
    savedAt: entry.savedAt,
  }).catch((error) => {
    console.error(error);
  });
  return true;
}

async function loadDashboard() {
  const query = buildQueryString(state.filters);
  const url = query
    ? `/api/metalworks-crm/dashboard?${query}`
    : "/api/metalworks-crm/dashboard";

  try {
    const dashboard = await apiRequest(url);
    setCacheEntry(CRM_DASHBOARD_CACHE_KEY, dashboard);
    await renderDashboardSnapshot(dashboard);
    return dashboard;
  } catch (error) {
    if (renderCachedDashboard()) {
      return state.dashboard;
    }

    throw error;
  }
}

async function loadLeadDetail(leadId) {
  if (!leadId) {
    renderLeadDetail(null);
    return;
  }

  try {
    const detail = await apiRequest(`/api/metalworks-crm/leads/${encodeURIComponent(leadId)}`);
    renderLeadDetail(detail);
    return detail;
  } catch (error) {
    const cachedDetail = readCachedLeadDetail(leadId);

    if (cachedDetail?.data) {
      renderLeadDetail(cachedDetail.data);
      setSystemStatus(
        `Conexion inestable. Mostrando detalle guardado ${formatCacheAge(cachedDetail.savedAt)}.`,
        "warning",
      );
      return cachedDetail.data;
    }

    throw error;
  }
}

async function loadApplicantDetail(applicantId) {
  if (!applicantId) {
    renderApplicantDetail(null);
    return;
  }

  const detail = await apiRequest(
    `/api/metalworks-crm/applicants/${encodeURIComponent(applicantId)}`,
  );
  renderApplicantDetail(detail);
  return detail;
}

async function handleSaveLead(event) {
  event.preventDefault();

  try {
    await saveLeadChanges({}, { successMessage: "Seguimiento guardado." });
  } catch (error) {
    setDetailFeedback(error.message, "error");
  }
}

async function handleMarkQuoted() {
  if (!state.selectedLeadId) {
    return;
  }

  if (statusInput) {
    statusInput.value = "quoted";
  }

  try {
    await saveLeadChanges({ status: "quoted" }, { successMessage: "Lead marcado como quoted." });
  } catch (error) {
    setDetailFeedback(error.message, "error");
  }
}

function handleManualLeadToggle() {
  if (!manualLeadPanel) {
    return;
  }

  if (manualLeadPanel.hidden) {
    openManualLeadPanel();
    return;
  }

  closeManualLeadPanel();
}

function handleManualLeadCancel() {
  closeManualLeadPanel();
}

async function handleManualLeadCreate(event) {
  event.preventDefault();

  if (!manualLeadForm) {
    return;
  }

  const formData = new FormData(manualLeadForm);
  const body = {
    fullName: String(formData.get("fullName") || "").trim(),
    phoneDisplay: String(formData.get("phoneDisplay") || "").trim(),
    email: String(formData.get("email") || "").trim(),
    projectType: String(formData.get("projectType") || "").trim(),
    location: String(formData.get("location") || "").trim(),
    status: String(formData.get("status") || "new").trim(),
    details: String(formData.get("details") || "").trim(),
  };

  if (!body.fullName) {
    setManualLeadFeedback("Add the customer's name first.", "error");
    manualLeadForm.elements.fullName?.focus();
    return;
  }

  state.manualLeadBusy = true;
  syncManualLeadUi();
  setManualLeadFeedback("Creating lead...", "muted");

  try {
    const detail = await apiRequest("/api/metalworks-crm/leads", {
      method: "POST",
      body,
    });

    if (detail?.lead?.id) {
      rememberSelectedLead(detail.lead.id);
    }

    closeManualLeadPanel();
    await loadDashboard();

    if (detail?.lead?.id) {
      rememberSelectedLead(detail.lead.id);
      renderLeadDetail(detail);
      openMobileWorkspacePane();
      scrollDetailIntoView();
    }

    setSystemStatus("Manual lead created.", "success");
  } catch (error) {
    setManualLeadFeedback(error.message || "I could not create that lead.", "error");
  } finally {
    state.manualLeadBusy = false;
    syncManualLeadUi();
  }
}

async function handleDeleteLead() {
  const lead = state.leadDetail?.lead || null;

  if (!lead?.id) {
    return;
  }

  const confirmed = window.confirm(
    `Delete ${lead.fullName || "this lead"} and remove its photos and activity history? This cannot be undone.`,
  );

  if (!confirmed) {
    return;
  }

  try {
    setDetailFeedback("Deleting lead...", "muted");

    await apiRequest(`/api/metalworks-crm/leads/${encodeURIComponent(lead.id)}`, {
      method: "DELETE",
    });

    rememberSelectedLead("");
    renderLeadDetail(null);
    await loadDashboard();
    setSystemStatus("Lead deleted.", "success");
  } catch (error) {
    setDetailFeedback(error.message || "I could not delete that lead.", "error");
  }
}

async function handleSendEstimate() {
  const snapshot = buildEstimateSnapshot();
  const documentWord = snapshot.documentType === "invoice" ? "invoice" : "estimate";
  const validationMessage = getInvoiceValidationMessage(snapshot);

  if (validationMessage) {
    setDetailFeedback(validationMessage, "error");
    return;
  }

  if (!snapshot.email) {
    setDetailFeedback("Este lead no tiene correo todavia.", "error");
    return;
  }

  if (!snapshot.description && !snapshot.total) {
    setDetailFeedback(`Primero arma la descripcion o el total del ${documentWord} para poder enviarlo.`, "error");
    return;
  }

  try {
    setDetailFeedback(`Guardando ${documentWord}...`, "muted");
    await saveLeadChanges({}, { refreshDashboard: false, showFeedback: false });
    setDetailFeedback(`Enviando ${documentWord}...`, "muted");

    const result = await apiRequest(
      `/api/metalworks-crm/leads/${encodeURIComponent(state.selectedLeadId)}/send-estimate`,
      {
        method: "POST",
      },
    );

    renderLeadDetail({
      lead: result.lead,
      assets: result.assets || [],
      activity: result.activity || [],
    });

    if (result.delivered) {
      setDetailFeedback(result.message || `${snapshot.documentLabel} enviado al cliente.`, "success");
      await loadDashboard();
      return;
    }

    const draftOpened = openEmailDraft({ silent: true });
    setDetailFeedback(
      draftOpened
        ? result.message || `No pude enviar el ${documentWord} desde el sistema. Te abri un draft prellenado para mandarlo rapido.`
        : result.message || `No pude enviar el ${documentWord} desde el sistema. Usa Open Email Draft para mandarlo rapido.`,
      "muted",
    );
    await loadDashboard();
  } catch (error) {
    const draftOpened = openEmailDraft({ silent: true });
    setDetailFeedback(
      draftOpened
        ? `No pude enviar el ${documentWord} desde el sistema. Te abri un draft para terminarlo rapido.`
        : error.message,
      draftOpened ? "muted" : "error",
    );
  }
}

function handlePhotoUploadTrigger() {
  if (!state.selectedLeadId || !photoUploadInput || state.manualPhotoUploadBusy) {
    return;
  }

  photoUploadInput.click();
}

async function handleManualPhotoUpload(event) {
  const selectedFiles = Array.from(event.target?.files || []);

  if (photoUploadInput) {
    photoUploadInput.value = "";
  }

  if (!state.selectedLeadId || !selectedFiles.length) {
    return;
  }

  if (selectedFiles.length > MAX_CRM_PHOTO_FILES) {
    setPhotoUploadFeedback(`Upload up to ${MAX_CRM_PHOTO_FILES} photos at a time.`, "error");
    return;
  }

  state.manualPhotoUploadBusy = true;
  syncManualPhotoUploadUi(state.leadDetail?.lead || null);
  setPhotoUploadFeedback("Preparing photos...", "muted");

  try {
    const preparedFiles = await prepareCrmPhotoFilesForUpload(selectedFiles);
    const filePayloads = await Promise.all(
      preparedFiles.map(async (file) => ({
        fileName: file.name || "project-photo.jpg",
        mimeType: file.type || "image/jpeg",
        dataUrl: await readFileAsDataUrl(file),
      })),
    );

    setPhotoUploadFeedback("Uploading photos...", "muted");

    const result = await apiRequest(
      `/api/metalworks-crm/leads/${encodeURIComponent(state.selectedLeadId)}/assets`,
      {
        method: "POST",
        body: {
          files: filePayloads,
        },
      },
    );

    renderLeadDetail(result);
    const uploadedCount = Number(result.uploadedCount || 0) || 0;
    setPhotoUploadFeedback(
      uploadedCount
        ? `${uploadedCount} photo${uploadedCount === 1 ? "" : "s"} saved to this lead.`
        : "Those photos were already saved on this lead.",
      uploadedCount ? "success" : "muted",
    );
    await refreshDashboardSafely();
  } catch (error) {
    setPhotoUploadFeedback(error.message || "I could not save those photos.", "error");
  } finally {
    state.manualPhotoUploadBusy = false;
    syncManualPhotoUploadUi(state.leadDetail?.lead || null);
  }
}

async function handleSendLiveChatReply(event) {
  event.preventDefault();

  if (!state.selectedLeadId || !liveChatForm) {
    return;
  }

  const lead = state.leadDetail?.lead || null;

  if (!lead?.supportsLiveChatReply) {
    setLiveChatFeedback("Este lead no tiene un chat web conectado.", "error");
    return;
  }

  const formData = new FormData(liveChatForm);
  const message = String(formData.get("message") || "").trim();

  if (!message) {
    setLiveChatFeedback("Escribe la respuesta primero.", "error");
    return;
  }

  state.liveChatReplyBusy = true;
  syncLiveChatComposer(lead);
  setLiveChatFeedback("Mandando respuesta al chat...", "muted");

  try {
    const result = await apiRequest(
      `/api/metalworks-crm/leads/${encodeURIComponent(state.selectedLeadId)}/live-chat-reply`,
      {
        method: "POST",
        body: {
          message,
        },
      },
    );

    renderLeadDetail(result);
    liveChatForm.reset();
    setDetailTab("conversation");
    setLiveChatFeedback("Respuesta enviada al hilo del cliente.", "success");
    await refreshDashboardSafely();
  } catch (error) {
    setLiveChatFeedback(error.message || "No pude mandar la respuesta.", "error");
  } finally {
    state.liveChatReplyBusy = false;
    syncLiveChatComposer(state.leadDetail?.lead || lead);
  }
}

async function setCrmView(view = "leads") {
  const nextView = normalizeCrmView(view);
  rememberSelectedView(nextView);
  renderFilters(state.dashboard);

  if (!state.dashboard) {
    await refreshDashboardSafely();
    return;
  }

  if (nextView === "applicants") {
    await refreshApplicantsSafely();
    return;
  }

  try {
    await renderLeadSnapshot(state.dashboard);
    setSystemStatus("", "");
  } catch (error) {
    handleCrmError(error, {
      fallbackMessage:
        "No pude cargar el detalle del lead en este momento. La lista sigue disponible mientras vuelve la conexion.",
    });
  }
}

async function handleLogout() {
  await apiRequest("/api/metalworks-crm/logout", {
    method: "POST",
  });
  window.location.href = "/metalworks-crm/login/";
}

function bindFilters() {
  statusFilter?.addEventListener("change", async () => {
    if (state.view === "applicants") {
      state.applicantFilters.status = String(statusFilter.value || "").trim();
      await renderApplicantCollection();
      return;
    }

    state.filters.status = String(statusFilter.value || "").trim();
    await refreshDashboardSafely();
  });

  serviceFilter?.addEventListener("change", async () => {
    if (state.view === "applicants") {
      state.applicantFilters.role = String(serviceFilter.value || "").trim();
      await renderApplicantCollection();
      return;
    }

    state.filters.projectType = String(serviceFilter.value || "").trim();
    await refreshDashboardSafely();
  });

  searchInput?.addEventListener("input", () => {
    window.clearTimeout(state.searchTimer);
    state.searchTimer = window.setTimeout(async () => {
      if (state.view === "applicants") {
        state.applicantFilters.search = String(searchInput.value || "").trim();
        await renderApplicantCollection();
        return;
      }

      state.filters.search = String(searchInput.value || "").trim();
      await refreshDashboardSafely();
    }, 220);
  });
}

function bindDetailActions() {
  detailForm?.addEventListener("submit", handleSaveLead);
  manualLeadForm?.addEventListener("submit", handleManualLeadCreate);
  liveChatForm?.addEventListener("submit", handleSendLiveChatReply);
  newLeadToggleButton?.addEventListener("click", handleManualLeadToggle);
  manualLeadCancelButton?.addEventListener("click", handleManualLeadCancel);
  markQuotedButton?.addEventListener("click", handleMarkQuoted);
  sendEstimateButton?.addEventListener("click", handleSendEstimate);
  openEmailDraftButton?.addEventListener("click", () => {
    openEmailDraft();
  });
  copyEstimateButton?.addEventListener("click", copyEstimateText);
  deleteLeadButton?.addEventListener("click", handleDeleteLead);
  photoUploadTrigger?.addEventListener("click", handlePhotoUploadTrigger);
  photoUploadInput?.addEventListener("change", handleManualPhotoUpload);

  const syncDetailFormActions = (event) => {
    const fieldName = event.target?.name || "";

    if (ESTIMATE_COST_FIELDS.includes(fieldName)) {
      syncEstimateTotalFromForm();
    }

    if (
      [
        "phoneDisplay",
        "email",
        "location",
        "clientDocumentType",
        ...ESTIMATE_COST_FIELDS,
      ].includes(fieldName)
    ) {
      syncDetailQuickActions(buildDetailActionPreview());
    }
  };

  detailForm?.addEventListener("input", syncDetailFormActions);
  detailForm?.addEventListener("change", syncDetailFormActions);

  detailTabButtons.forEach((button) => {
    button.addEventListener("click", () => {
      setDetailTab(button.dataset.crmDetailTab || "profile");
    });
  });

  applicantDetailTabButtons.forEach((button) => {
    button.addEventListener("click", () => {
      setApplicantDetailTab(button.dataset.crmApplicantDetailTab || "profile");
    });
  });
}

async function refreshDashboardSafely() {
  try {
    await loadDashboard();
  } catch (error) {
    handleCrmError(error, {
      fallbackMessage: state.dashboard
        ? "La conexion del CRM esta inestable. Conservando el ultimo snapshot mientras vuelve."
        : "El CRM se esta despertando o la conexion esta inestable. Intenta otra vez en unos segundos.",
    });
  }
}

async function refreshApplicantsSafely() {
  try {
    await loadApplicants();
    setSystemStatus("", "");
  } catch (error) {
    handleCrmError(error, {
      fallbackMessage: state.applicants.length
        ? "No pude refrescar candidatos en este momento. Conservando la ultima vista cargada."
      : "No pude cargar los candidatos del hiring assistant. Intenta otra vez en unos segundos.",
    });
  }
}

async function refreshProspectorsSafely() {
  try {
    await loadProspectorAdmin();
    if (!state.prospectorAdmin?.latestCredentials?.email) {
      setProspectorFeedback("", "muted");
    }
  } catch (error) {
    setProspectorFeedback(
      TRANSIENT_STATUS_CODES.has(Number(error?.status || 0))
        ? "The prospector accounts panel is waking up. Try again in a few seconds."
        : error.message || "No pude cargar las cuentas de prospectadores.",
      TRANSIENT_STATUS_CODES.has(Number(error?.status || 0)) ? "muted" : "error",
    );
  }
}

async function refreshWorkspaceSafely() {
  await Promise.all([refreshDashboardSafely(), refreshProspectorsSafely()]);
}

function bindProspectorAdmin() {
  if (!prospectorForm || !prospectorList) {
    return;
  }

  prospectorForm.addEventListener("submit", async (event) => {
    event.preventDefault();
    setProspectorFeedback("Creating account...", "muted");

    if (prospectorSaveButton) {
      prospectorSaveButton.disabled = true;
    }

    const formData = new FormData(prospectorForm);
    const customPassword = String(formData.get("password") || "").trim();
    const payload = {
      name: String(formData.get("name") || "").trim(),
      email: String(formData.get("email") || "").trim(),
      password: customPassword,
    };

    if (customPassword && customPassword.length < PROSPECTOR_PASSWORD_MIN_LENGTH) {
      setProspectorFeedback(
        `Password must be at least ${PROSPECTOR_PASSWORD_MIN_LENGTH} characters.`,
        "error",
      );
      if (prospectorSaveButton) {
        prospectorSaveButton.disabled = false;
      }
      return;
    }

    try {
      const result = await apiRequest("/api/metalworks-crm/prospectors", {
        method: "POST",
        body: payload,
      });

      state.prospectorAdmin.latestCredentials = {
        ...(result.credentials || {}),
        loginUrl:
          state.prospectorAdmin?.loginUrl ||
          "https://www.chicagometalworksandfencing.com/metalworks-crm/prospector/login/",
        title: payload.name
          ? `${payload.name} account ready`
          : "New account ready",
      };
      renderProspectorCredentials();
      setProspectorFeedback(
        result.credentials?.passwordMode === "custom"
          ? "Prospector account created with your custom password."
          : "Prospector account created.",
        "success",
      );
      prospectorForm.reset();
      await loadProspectorAdmin();
    } catch (error) {
      setProspectorFeedback(error.message, "error");
    } finally {
      if (prospectorSaveButton) {
        prospectorSaveButton.disabled = false;
      }
    }
  });

  prospectorList.addEventListener("click", async (event) => {
    const copyEmailButton = event.target.closest("[data-prospector-copy-email]");
    const resetPasswordButton = event.target.closest("[data-prospector-reset-password]");
    const toggleStatusButton = event.target.closest("[data-prospector-toggle-status]");
    const card = event.target.closest("[data-prospector-id]");
    const prospectorId = String(card?.dataset.prospectorId || "").trim();

    if (copyEmailButton) {
      const email = String(copyEmailButton.dataset.prospectorCopyEmail || "").trim();
      const copied = await copyTextWithFallback(email);
      setProspectorFeedback(
        copied ? "Email copied." : "No pude copiar el email automaticamente.",
        copied ? "success" : "muted",
      );
      return;
    }

    if (resetPasswordButton) {
      if (!prospectorId) {
        return;
      }

      const promptedPassword = window.prompt(
        "Enter a new password for this prospector. Leave it blank to auto-generate one and sign them out of active sessions.",
        "",
      );

      if (promptedPassword === null) {
        return;
      }

      const customPassword = String(promptedPassword || "").trim();

      if (customPassword && customPassword.length < PROSPECTOR_PASSWORD_MIN_LENGTH) {
        setProspectorFeedback(
          `Password must be at least ${PROSPECTOR_PASSWORD_MIN_LENGTH} characters.`,
          "error",
        );
        return;
      }

      try {
        setProspectorFeedback(
          customPassword ? "Updating password..." : "Resetting password...",
          "muted",
        );
        const result = await apiRequest(
          `/api/metalworks-crm/prospectors/${encodeURIComponent(prospectorId)}/reset-password`,
          {
            method: "POST",
            body: customPassword ? { password: customPassword } : {},
          },
        );

        state.prospectorAdmin.latestCredentials = {
          ...(result.credentials || {}),
          loginUrl:
            state.prospectorAdmin?.loginUrl ||
            "https://www.chicagometalworksandfencing.com/metalworks-crm/prospector/login/",
          title: result.prospector?.name
            ? `${result.prospector.name} password reset`
            : "Temporary password ready",
        };
        renderProspectorCredentials();
        setProspectorFeedback(
          result.credentials?.passwordMode === "custom"
            ? "Password updated."
            : "Temporary password created.",
          "success",
        );
        await loadProspectorAdmin();
      } catch (error) {
        setProspectorFeedback(error.message, "error");
      }

      return;
    }

    if (toggleStatusButton) {
      if (!prospectorId) {
        return;
      }

      const nextStatus = String(toggleStatusButton.dataset.prospectorToggleStatus || "").trim();

      if (!nextStatus) {
        return;
      }

      try {
        setProspectorFeedback(
          nextStatus === "paused" ? "Pausing access..." : "Reactivating access...",
          "muted",
        );
        const result = await apiRequest(
          `/api/metalworks-crm/prospectors/${encodeURIComponent(prospectorId)}`,
          {
            method: "PATCH",
            body: {
              status: nextStatus,
            },
          },
        );
        setProspectorFeedback(
          result.forcedSignOut
            ? "Access updated and active sessions were signed out."
            : "Prospector access updated.",
          "success",
        );
        await loadProspectorAdmin();
      } catch (error) {
        setProspectorFeedback(error.message, "error");
      }
    }
  });

  prospectorCopyEmailButton?.addEventListener("click", async () => {
    const copied = await copyTextWithFallback(
      state.prospectorAdmin?.latestCredentials?.email || "",
    );
    setProspectorFeedback(
      copied ? "Email copied." : "No pude copiar el email automaticamente.",
      copied ? "success" : "muted",
    );
  });

  prospectorCopyPasswordButton?.addEventListener("click", async () => {
    const copied = await copyTextWithFallback(
      state.prospectorAdmin?.latestCredentials?.temporaryPassword || "",
    );
    setProspectorFeedback(
      copied ? "Password copied." : "No pude copiar el password automaticamente.",
      copied ? "success" : "muted",
    );
  });

  prospectorCopyLoginButton?.addEventListener("click", async () => {
    const copied = await copyTextWithFallback(
      state.prospectorAdmin?.latestCredentials?.loginUrl ||
        state.prospectorAdmin?.loginUrl ||
        "",
    );
    setProspectorFeedback(
      copied ? "Login link copied." : "No pude copiar el link automaticamente.",
      copied ? "success" : "muted",
    );
  });
}

function bindAppShell() {
  if (state.bindingsReady) {
    return;
  }

  syncPushControls();
  bindFilters();
  bindDetailActions();
  bindProspectorAdmin();
  refreshButton?.addEventListener("click", refreshWorkspaceSafely);
  logoutButton?.addEventListener("click", handleLogout);
  enablePushButton?.addEventListener("click", handleEnablePush);
  testPushButton?.addEventListener("click", handleTestPush);
  mobilePaneButtons.forEach((button) => {
    button.addEventListener("click", () => {
      rememberMobilePane(button.dataset.crmMobilePaneButton || "inbox");
      applyMobilePaneLayout();
    });
  });
  mobileSecondaryMoreButton?.addEventListener("click", () => {
    rememberMobilePane(state.mobilePane === "more" ? "agenda" : "more");
    applyMobilePaneLayout();
  });
  mobileBackButton?.addEventListener("click", () => {
    rememberMobilePane("inbox");
    applyMobilePaneLayout();
  });
  if (crmMobileMediaQuery) {
    const handleMobileLayoutChange = () => {
      applyMobilePaneLayout();
    };

    if (typeof crmMobileMediaQuery.addEventListener === "function") {
      crmMobileMediaQuery.addEventListener("change", handleMobileLayoutChange);
    } else if (typeof crmMobileMediaQuery.addListener === "function") {
      crmMobileMediaQuery.addListener(handleMobileLayoutChange);
    }
  }
  viewButtons.forEach((button) => {
    button.addEventListener("click", async () => {
      const nextView = button.dataset.crmViewButton || "leads";
      await setCrmView(nextView);
    });
  });
  state.bindingsReady = true;
}

async function init() {
  bindAppShell();
  await clearCrmBadge();
  applyCachedTheme();
  applyMobilePaneLayout();
  renderCachedDashboard();

  let me;

  try {
    me = await apiRequest("/api/metalworks-crm/me");
  } catch (error) {
    handleCrmError(error, {
      fallbackMessage: state.dashboard
        ? "La conexion del CRM esta inestable. Mostrando el ultimo snapshot mientras vuelve."
        : "El CRM se esta despertando o la conexion esta inestable. Intenta otra vez en unos segundos.",
      allowRedirect: false,
    });
    return;
  }

  if (!me.authenticated) {
    window.location.href = "/metalworks-crm/login/";
    return;
  }

  state.me = me;
  applyProfileTheme(me.profile || {}, me.email || "");
  persistThemeProfile(me.profile || {}, me.email || "");
  renderResourceHub(me.resourceSections || []);
  renderProspectorCredentials();
  await loadPushConfig();
  applyMobilePaneLayout();
  setSystemStatus("", "");
  await Promise.all([refreshDashboardSafely(), refreshProspectorsSafely()]);
}

init().catch((error) => {
  handleCrmError(error, {
    fallbackMessage: state.dashboard
      ? "La conexion del CRM esta inestable. Mostrando el ultimo snapshot mientras vuelve."
      : "El CRM se esta despertando o la conexion esta inestable. Intenta otra vez en unos segundos.",
  });
});
