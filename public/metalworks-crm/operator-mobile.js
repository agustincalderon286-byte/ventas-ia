const TRANSIENT_STATUS_CODES = new Set([502, 503, 504]);
const GET_RETRY_DELAYS_MS = [450, 1100, 2200];
const CRM_SELECTED_LEAD_STORAGE_KEY = "cmwf_crm_selected_lead_v1";
const CRM_THEME_STORAGE_KEY = "cmwf_crm_theme_v1";
const OPERATOR_CHAT_HISTORY_STORAGE_KEY = "cmwf_operator_chat_history_v1";
const OPERATOR_STATUS_OPTIONS = [
  { value: "new", label: "Nuevo" },
  { value: "contacted", label: "Contacted" },
  { value: "quoted", label: "Quoted" },
  { value: "booked", label: "Booked" },
  { value: "won", label: "Won" },
  { value: "lost", label: "Lost" },
  { value: "archived", label: "Archived" },
];

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

function formatRelativeMinutes(value = "") {
  if (!value) {
    return "";
  }

  const date = new Date(value);

  if (Number.isNaN(date.getTime())) {
    return "";
  }

  const diffMs = Date.now() - date.getTime();
  const diffMinutes = Math.round(diffMs / 60000);

  if (Math.abs(diffMinutes) < 60) {
    if (diffMinutes >= 0) {
      return `${Math.max(diffMinutes, 1)} min ago`;
    }

    return `in ${Math.abs(diffMinutes)} min`;
  }

  const diffHours = Math.round(diffMinutes / 60);

  if (Math.abs(diffHours) < 24) {
    if (diffHours >= 0) {
      return `${Math.max(diffHours, 1)} h ago`;
    }

    return `in ${Math.abs(diffHours)} h`;
  }

  const diffDays = Math.round(diffHours / 24);
  if (diffDays >= 0) {
    return `${Math.max(diffDays, 1)} d ago`;
  }

  return `in ${Math.abs(diffDays)} d`;
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

function buildTelHref(phoneDigits = "") {
  return phoneDigits ? `tel:+1${phoneDigits}` : "#";
}

function buildSmsHref(phoneDigits = "") {
  return phoneDigits ? `sms:+1${phoneDigits}` : "#";
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

function readLeadIdFromQuery() {
  try {
    const params = new URLSearchParams(window.location.search || "");
    return String(params.get("lead") || "").trim();
  } catch {
    return "";
  }
}

const summaryWrap = document.querySelector("[data-operator-summary]");
const focusCount = document.querySelector("[data-operator-focus-count]");
const agendaCount = document.querySelector("[data-operator-agenda-count]");
const focusList = document.querySelector("[data-operator-focus-list]");
const agendaList = document.querySelector("[data-operator-agenda-list]");
const globalActivityList = document.querySelector("[data-operator-global-activity]");
const statusNode = document.querySelector("[data-operator-status]");
const userChip = document.querySelector("[data-operator-user-chip]");
const themeBadge = document.querySelector("[data-operator-theme-badge]");
const refreshButton = document.querySelector("[data-operator-refresh]");
const logoutButton = document.querySelector("[data-operator-logout]");
const openCrmButton = document.querySelector("[data-operator-open-crm]");
const detailWrap = document.querySelector("[data-operator-detail-wrap]");
const detailEmpty = document.querySelector("[data-operator-detail-empty]");
const detailHeading = document.querySelector("[data-operator-detail-heading]");
const selectedStatus = document.querySelector("[data-operator-selected-status]");
const detailForm = document.querySelector("[data-operator-detail-form]");
const detailFeedback = document.querySelector("[data-operator-detail-feedback]");
const detailCallLink = document.querySelector("[data-operator-call]");
const detailTextLink = document.querySelector("[data-operator-text]");
const openSelectedCrmButton = document.querySelector("[data-operator-open-selected-crm]");
const conversationPreview = document.querySelector("[data-operator-conversation-preview]");
const activityPreview = document.querySelector("[data-operator-activity-preview]");
const statusInput = document.querySelector("[data-operator-status-input]");
const chatThread = document.querySelector("[data-operator-chat-thread]");
const chatForm = document.querySelector("[data-operator-chat-form]");
const chatInput = document.querySelector("[data-operator-chat-input]");
const chatFeedback = document.querySelector("[data-operator-chat-feedback]");
const quickPromptButtons = Array.from(
  document.querySelectorAll("[data-operator-prompt]"),
);

const state = {
  snapshot: null,
  selectedLeadId: readLeadIdFromQuery() || String(readStoredJson(CRM_SELECTED_LEAD_STORAGE_KEY, "") || ""),
  leadDetail: null,
  chatHistory: Array.isArray(readStoredJson(OPERATOR_CHAT_HISTORY_STORAGE_KEY, []))
    ? readStoredJson(OPERATOR_CHAT_HISTORY_STORAGE_KEY, [])
    : [],
  loadingSnapshot: false,
};

function setStatus(message = "", tone = "") {
  if (!statusNode) {
    return;
  }

  statusNode.hidden = !message;
  statusNode.textContent = message;
  statusNode.dataset.tone = tone;
}

function setDetailFeedback(message = "", tone = "") {
  if (!detailFeedback) {
    return;
  }

  detailFeedback.textContent = message;
  detailFeedback.dataset.tone = tone;
}

function setChatFeedback(message = "", tone = "") {
  if (!chatFeedback) {
    return;
  }

  chatFeedback.textContent = message;
  chatFeedback.dataset.tone = tone;
}

function persistSelectedLead(leadId = "") {
  state.selectedLeadId = String(leadId || "").trim();
  writeStoredJson(CRM_SELECTED_LEAD_STORAGE_KEY, state.selectedLeadId || "");
}

function persistChatHistory() {
  writeStoredJson(OPERATOR_CHAT_HISTORY_STORAGE_KEY, state.chatHistory.slice(-18));
}

function applyProfileTheme(profile = {}, email = "") {
  const safeProfile = profile && typeof profile === "object" ? profile : {};
  const displayName = String(safeProfile.displayName || email || "Admin").trim();
  const skin = String(safeProfile.skin || "classic").trim() || "classic";
  const themeLabel = String(safeProfile.themeLabel || "").trim();

  document.body.dataset.crmSkin = skin;

  if (userChip) {
    userChip.textContent = displayName;
  }

  if (themeBadge) {
    themeBadge.hidden = !themeLabel;
    themeBadge.textContent = themeLabel || "";
  }

  writeStoredJson(CRM_THEME_STORAGE_KEY, {
    savedAt: Date.now(),
    data: {
      email,
      profile: safeProfile,
    },
  });
}

function buildLeadCardMarkup(lead = null, { active = false, compact = false } = {}) {
  if (!lead?.id) {
    return "";
  }

  const phoneDigits = normalizePhoneDigits(lead.phone || lead.phoneDisplay || "");
  const notes = [
    lead.nextAction ? `Next: ${lead.nextAction}` : "",
    lead.nextActionAt ? `When: ${formatDate(lead.nextActionAt)}` : "",
    lead.lastContactAt ? `Last touch: ${formatRelativeMinutes(lead.lastContactAt)}` : "",
  ]
    .filter(Boolean)
    .slice(0, compact ? 2 : 3);

  return `
    <article class="operator-lead-card ${active ? "is-active" : ""}" data-operator-lead-card="${escapeHtml(lead.id)}">
      <div class="operator-lead-head">
        <div>
          <h3>${escapeHtml(lead.fullName || "Unknown lead")}</h3>
          <div class="operator-lead-meta">
            <span>${escapeHtml(lead.projectType || "Service not set")}</span>
            <span>${escapeHtml(lead.location || lead.email || lead.phoneDisplay || "")}</span>
          </div>
        </div>
        <span class="crm-status-badge" data-status="${escapeHtml(lead.status || "new")}">
          ${escapeHtml(lead.statusLabel || lead.status || "New")}
        </span>
      </div>
      <div class="crm-micro-list">
        ${lead.phoneDisplay ? `<span class="crm-chip">${escapeHtml(lead.phoneDisplay)}</span>` : ""}
        ${lead.callbackIntent === "yes" ? '<span class="crm-chip">Callback requested</span>' : ""}
        ${lead.estimateAmount ? `<span class="crm-chip">$${escapeHtml(String(lead.estimateAmount))}</span>` : ""}
        ${lead.updatedAt ? `<span class="crm-chip">${escapeHtml(formatRelativeMinutes(lead.updatedAt))}</span>` : ""}
      </div>
      <div class="operator-lead-notes">
        ${notes.map((item) => `<span>${escapeHtml(item)}</span>`).join("")}
        ${
          lead.details
            ? `<span>${escapeHtml(String(lead.details || "").slice(0, compact ? 110 : 160))}</span>`
            : ""
        }
      </div>
      <div class="operator-lead-actions">
        <button type="button" class="crm-secondary-button" data-operator-action="select" data-operator-lead-id="${escapeHtml(lead.id)}">
          Open
        </button>
        ${
          phoneDigits
            ? `<a href="${escapeHtml(buildTelHref(phoneDigits))}" class="crm-secondary-button crm-action-link" data-operator-action="call" data-operator-lead-id="${escapeHtml(lead.id)}">Call</a>`
            : ""
        }
        ${
          phoneDigits
            ? `<a href="${escapeHtml(buildSmsHref(phoneDigits))}" class="crm-secondary-button crm-action-link" data-operator-action="text" data-operator-lead-id="${escapeHtml(lead.id)}">Text</a>`
            : ""
        }
        <button type="button" class="crm-secondary-button" data-operator-action="full-crm" data-operator-lead-id="${escapeHtml(lead.id)}">
          Full CRM
        </button>
      </div>
    </article>
  `;
}

function renderSummary(summary = {}) {
  if (!summaryWrap) {
    return;
  }

  const cards = [
    {
      label: "New Leads",
      value: summary.newLeads || 0,
      note: `${summary.totalLeads || 0} total in CRM`,
    },
    {
      label: "Active Follow-ups",
      value: summary.activeFollowups || 0,
      note: `${summary.callbacksScheduled || 0} callbacks scheduled`,
    },
    {
      label: "Booked",
      value: summary.bookedLeads || 0,
      note: `${summary.quoteSubmits30d || 0} site quotes in 30 days`,
    },
    {
      label: "Won",
      value: summary.wonLeads || 0,
      note: `${summary.phoneClicks30d || 0} phone clicks in 30 days`,
    },
  ];

  summaryWrap.innerHTML = cards
    .map(
      (card) => `
        <article class="operator-summary-card">
          <span>${escapeHtml(card.label)}</span>
          <strong>${escapeHtml(String(card.value))}</strong>
          <small>${escapeHtml(card.note)}</small>
        </article>
      `,
    )
    .join("");
}

function renderLeadList(root, leads = [], countNode = null) {
  if (!root) {
    return;
  }

  if (countNode) {
    countNode.textContent = `${leads.length} ${leads.length === 1 ? "lead" : "leads"}`;
  }

  if (!Array.isArray(leads) || !leads.length) {
    root.innerHTML = '<p class="operator-empty-state">Nothing here yet.</p>';
    return;
  }

  root.innerHTML = leads
    .map((lead) =>
      buildLeadCardMarkup(lead, {
        active: lead.id === state.selectedLeadId,
      }),
    )
    .join("");
}

function renderActivityList(root, activities = []) {
  if (!root) {
    return;
  }

  if (!Array.isArray(activities) || !activities.length) {
    root.innerHTML = '<p class="operator-empty-state">No activity yet.</p>';
    return;
  }

  root.innerHTML = activities
    .map(
      (item) => `
        <article class="operator-activity-card">
          <div class="operator-lead-head">
            <h3>${escapeHtml(item.title || "Activity")}</h3>
            <span class="crm-chip">${escapeHtml(formatDate(item.createdAt) || "")}</span>
          </div>
          ${item.body ? `<p>${escapeHtml(item.body)}</p>` : ""}
        </article>
      `,
    )
    .join("");
}

function syncDetailActions(lead = null) {
  const phoneDigits = normalizePhoneDigits(lead?.phone || lead?.phoneDisplay || "");
  const hasPhone = Boolean(phoneDigits);

  if (detailCallLink) {
    detailCallLink.hidden = !hasPhone;
    detailCallLink.href = hasPhone ? buildTelHref(phoneDigits) : "#";
  }

  if (detailTextLink) {
    detailTextLink.hidden = !hasPhone;
    detailTextLink.href = hasPhone ? buildSmsHref(phoneDigits) : "#";
  }

  if (selectedStatus) {
    selectedStatus.textContent = lead?.statusLabel || "No lead selected";
  }

  if (detailHeading) {
    detailHeading.textContent = lead?.id
      ? `${lead.fullName || "Lead"} · ${lead.projectType || "Service not set"}`
      : "Select a lead to work it from your phone.";
  }
}

function renderDetail(detail = null) {
  state.leadDetail = detail;
  const lead = detail?.lead || null;

  if (!lead) {
    detailWrap.hidden = true;
    detailEmpty.hidden = false;
    syncDetailActions(null);
    if (conversationPreview) {
      conversationPreview.textContent = "No conversation saved yet.";
    }
    if (activityPreview) {
      activityPreview.innerHTML = "";
    }
    return;
  }

  detailWrap.hidden = false;
  detailEmpty.hidden = true;
  syncDetailActions(lead);

  if (detailForm) {
    detailForm.elements.fullName.value = lead.fullName || "";
    detailForm.elements.phoneDisplay.value = lead.phoneDisplay || lead.phone || "";
    detailForm.elements.email.value = lead.email || "";
    detailForm.elements.projectType.value = lead.projectType || "";
    detailForm.elements.location.value = lead.location || "";
    detailForm.elements.status.value = lead.status || "new";
    detailForm.elements.nextAction.value = lead.nextAction || "";
    detailForm.elements.nextActionAt.value = toDatetimeLocalValue(lead.nextActionAt);
    detailForm.elements.details.value = lead.details || "";
    detailForm.elements.note.value = "";
  }

  const conversationItems = Array.isArray(lead.conversationHistory)
    ? lead.conversationHistory.slice(-4)
    : [];

  if (conversationPreview) {
    if (!conversationItems.length) {
      conversationPreview.textContent = "No conversation saved yet.";
    } else {
      conversationPreview.textContent = conversationItems
        .map((item) => {
          const role = item.role === "assistant" ? "Agustin" : "Client";
          return `${role}: ${item.content}`;
        })
        .join("\n\n");
    }
  }

  const activity = Array.isArray(detail.activity) ? detail.activity.slice(0, 4) : [];

  if (activityPreview) {
    if (!activity.length) {
      activityPreview.innerHTML = '<p class="operator-empty-state">No activity yet.</p>';
    } else {
      activityPreview.innerHTML = activity
        .map(
          (item) => `
            <article>
              <strong>${escapeHtml(item.title || "Activity")}</strong>
              <p>${escapeHtml(item.body || "")}</p>
            </article>
          `,
        )
        .join("");
    }
  }
}

function buildDetailPayload() {
  const formData = new FormData(detailForm);

  return {
    fullName: String(formData.get("fullName") || "").trim(),
    phoneDisplay: String(formData.get("phoneDisplay") || "").trim(),
    email: String(formData.get("email") || "").trim(),
    projectType: String(formData.get("projectType") || "").trim(),
    location: String(formData.get("location") || "").trim(),
    status: String(formData.get("status") || "").trim(),
    nextAction: String(formData.get("nextAction") || "").trim(),
    nextActionAt: String(formData.get("nextActionAt") || "").trim(),
    details: String(formData.get("details") || "").trim(),
    note: String(formData.get("note") || "").trim(),
  };
}

function ensureChatHistory() {
  if (state.chatHistory.length) {
    return;
  }

  state.chatHistory = [
    {
      role: "assistant",
      content:
        "Ask me who to call first, what changed today, or draft a short follow-up for the selected lead.",
      createdAt: new Date().toISOString(),
    },
  ];
}

function renderChatHistory() {
  if (!chatThread) {
    return;
  }

  ensureChatHistory();
  chatThread.innerHTML = state.chatHistory
    .slice(-14)
    .map(
      (entry) => `
        <article class="operator-chat-card" data-role="${escapeHtml(entry.role || "assistant")}">
          <small>${entry.role === "user" ? "You" : "Agustin Operator"} · ${escapeHtml(formatDate(entry.createdAt) || "")}</small>
          <p>${escapeHtml(entry.content || "")}</p>
        </article>
      `,
    )
    .join("");

  chatThread.scrollTop = chatThread.scrollHeight;
}

async function loadLeadDetail(leadId = "") {
  const safeLeadId = String(leadId || "").trim();

  if (!safeLeadId) {
    renderDetail(null);
    return null;
  }

  const detail = await apiRequest(`/api/metalworks-crm/leads/${encodeURIComponent(safeLeadId)}`);
  renderDetail(detail);
  return detail;
}

async function loadSnapshot({ silent = false } = {}) {
  if (state.loadingSnapshot) {
    return state.snapshot;
  }

  state.loadingSnapshot = true;

  if (!silent) {
    setStatus("Refreshing mobile operator...", "muted");
  }

  try {
    const snapshot = await apiRequest("/api/metalworks-crm/operator/snapshot");
    state.snapshot = snapshot;
    applyProfileTheme(snapshot.profile || {}, snapshot.email || "");
    renderSummary(snapshot.summary || {});
    renderLeadList(focusList, snapshot.focusLeads || [], focusCount);
    renderLeadList(agendaList, snapshot.agendaLeads || [], agendaCount);
    renderActivityList(globalActivityList, snapshot.recentActivity || []);

    if (!state.selectedLeadId) {
      state.selectedLeadId =
        snapshot.focusLeads?.[0]?.id || snapshot.agendaLeads?.[0]?.id || "";
      persistSelectedLead(state.selectedLeadId);
    }

    if (state.selectedLeadId) {
      await loadLeadDetail(state.selectedLeadId);
    } else {
      renderDetail(null);
    }

    setStatus("", "");
    return snapshot;
  } finally {
    state.loadingSnapshot = false;
  }
}

function openFullCrm(leadId = "") {
  const safeLeadId = String(leadId || "").trim();

  if (safeLeadId) {
    persistSelectedLead(safeLeadId);
  }

  window.location.href = safeLeadId
    ? `/metalworks-crm/?lead=${encodeURIComponent(safeLeadId)}`
    : "/metalworks-crm/";
}

async function handleLeadSelect(leadId = "") {
  const safeLeadId = String(leadId || "").trim();

  if (!safeLeadId) {
    return;
  }

  persistSelectedLead(safeLeadId);
  renderLeadList(focusList, state.snapshot?.focusLeads || [], focusCount);
  renderLeadList(agendaList, state.snapshot?.agendaLeads || [], agendaCount);
  setDetailFeedback("", "");
  await loadLeadDetail(safeLeadId);
  detailWrap?.scrollIntoView({
    behavior: "smooth",
    block: "start",
  });
}

async function handleSaveLead(event) {
  event.preventDefault();

  if (!state.selectedLeadId) {
    return;
  }

  setDetailFeedback("Saving...", "muted");

  try {
    const result = await apiRequest(
      `/api/metalworks-crm/leads/${encodeURIComponent(state.selectedLeadId)}`,
      {
        method: "PATCH",
        body: buildDetailPayload(),
      },
    );

    renderDetail(result);
    await loadSnapshot({ silent: true });
    setDetailFeedback("Lead saved.", "success");
  } catch (error) {
    setDetailFeedback(error.message, "error");
  }
}

async function handleChatSubmit(event) {
  event.preventDefault();

  const message = String(chatInput?.value || "").trim();

  if (!message) {
    setChatFeedback("Write a prompt first.", "error");
    return;
  }

  state.chatHistory.push({
    role: "user",
    content: message,
    createdAt: new Date().toISOString(),
  });
  renderChatHistory();
  persistChatHistory();
  setChatFeedback("Thinking...", "muted");
  chatInput.value = "";

  try {
    const result = await apiRequest("/api/metalworks-crm/operator/chat", {
      method: "POST",
      body: {
        message,
        leadId: state.selectedLeadId || "",
      },
    });

    state.chatHistory.push({
      role: "assistant",
      content: String(result.reply || "").trim() || "I could not answer right now.",
      createdAt: new Date().toISOString(),
    });
    persistChatHistory();
    renderChatHistory();
    setChatFeedback(result.usedFallback ? "Fallback reply used." : "", result.usedFallback ? "muted" : "");
  } catch (error) {
    state.chatHistory.push({
      role: "assistant",
      content: error.message || "I could not answer right now.",
      createdAt: new Date().toISOString(),
    });
    persistChatHistory();
    renderChatHistory();
    setChatFeedback(error.message, "error");
  }
}

async function handleLogout() {
  await apiRequest("/api/metalworks-crm/logout", {
    method: "POST",
  });
  window.location.href = "/metalworks-crm/login/";
}

function bindCardList(root) {
  root?.addEventListener("click", async (event) => {
    const trigger = event.target.closest("[data-operator-action]");

    if (!trigger) {
      return;
    }

    const action = String(trigger.getAttribute("data-operator-action") || "").trim();
    const leadId = String(trigger.getAttribute("data-operator-lead-id") || "").trim();

    if (action === "select") {
      event.preventDefault();
      await handleLeadSelect(leadId);
      return;
    }

    if (action === "full-crm") {
      event.preventDefault();
      openFullCrm(leadId);
    }
  });
}

function bindQuickPrompts() {
  quickPromptButtons.forEach((button) => {
    button.addEventListener("click", async () => {
      if (!chatInput) {
        return;
      }

      chatInput.value = String(button.getAttribute("data-operator-prompt") || "").trim();
      await handleChatSubmit(new Event("submit"));
    });
  });
}

function hydrateStatusOptions() {
  if (!statusInput) {
    return;
  }

  statusInput.innerHTML = OPERATOR_STATUS_OPTIONS.map(
    (item) => `<option value="${escapeHtml(item.value)}">${escapeHtml(item.label)}</option>`,
  ).join("");
}

function bindApp() {
  hydrateStatusOptions();
  renderChatHistory();
  bindCardList(focusList);
  bindCardList(agendaList);
  detailForm?.addEventListener("submit", handleSaveLead);
  chatForm?.addEventListener("submit", handleChatSubmit);
  refreshButton?.addEventListener("click", () => {
    loadSnapshot().catch((error) => {
      setStatus(error.message, "error");
    });
  });
  logoutButton?.addEventListener("click", handleLogout);
  openCrmButton?.addEventListener("click", () => openFullCrm(state.selectedLeadId));
  openSelectedCrmButton?.addEventListener("click", () => openFullCrm(state.selectedLeadId));
  bindQuickPrompts();
}

async function init() {
  bindApp();

  try {
    await loadSnapshot();
  } catch (error) {
    if (Number(error?.status || 0) === 401) {
      window.location.href = "/metalworks-crm/login/";
      return;
    }

    setStatus(
      TRANSIENT_STATUS_CODES.has(Number(error?.status || 0))
        ? "The operator is waking up. Try again in a few seconds."
        : error.message || "I could not load the mobile operator.",
      TRANSIENT_STATUS_CODES.has(Number(error?.status || 0)) ? "warning" : "error",
    );
  }
}

init().catch((error) => {
  setStatus(error.message || "I could not load the mobile operator.", "error");
});
