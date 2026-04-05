async function apiRequest(url, options = {}) {
  const config = {
    method: options.method || "GET",
    credentials: "same-origin",
    headers: {
      ...(options.body ? { "Content-Type": "application/json" } : {}),
      ...(options.headers || {}),
    },
  };

  if (options.body) {
    config.body = JSON.stringify(options.body);
  }

  const response = await fetch(url, config);
  const data = await response.json().catch(() => ({}));

  if (!response.ok) {
    throw new Error(data.error || "No pude completar esa accion.");
  }

  return data;
}

const METALWORKS_CONTACT = {
  phoneDisplay: "773 798 4107",
  phoneDigits: "7737984107",
  email: "agustincalderon286@gmail.com",
};

const ESTIMATE_COST_FIELDS = [
  "estimateMaterialsCost",
  "estimateLaborCost",
  "estimateCoatingCost",
  "estimateMiscCost",
  "estimateDiscount",
];

const state = {
  dashboard: null,
  leadDetail: null,
  selectedLeadId: "",
  detailTab: "profile",
  filters: {
    search: "",
    status: "",
    projectType: "",
  },
  searchTimer: null,
};

const summaryWrap = document.querySelector("[data-crm-summary]");
const leadList = document.querySelector("[data-crm-lead-list]");
const emptyState = document.querySelector("[data-crm-empty-state]");
const detailWrap = document.querySelector("[data-crm-detail-wrap]");
const detailEmpty = document.querySelector("[data-crm-detail-empty]");
const detailMeta = document.querySelector("[data-crm-detail-meta]");
const detailStatus = document.querySelector("[data-crm-detail-status]");
const detailForm = document.querySelector("[data-crm-detail-form]");
const detailPanel = document.querySelector(".crm-detail-panel");
const detailFeedback = document.querySelector("[data-crm-detail-feedback]");
const activityList = document.querySelector("[data-crm-activity-list]");
const globalActivityList = document.querySelector("[data-crm-global-activity]");
const globalActivitySummary = document.querySelector("[data-crm-global-activity-summary]");
const leadsCount = document.querySelector("[data-crm-leads-count]");
const statusFilter = document.querySelector("[data-crm-status-filter]");
const serviceFilter = document.querySelector("[data-crm-service-filter]");
const searchInput = document.querySelector("[data-crm-search]");
const userChip = document.querySelector("[data-crm-user-chip]");
const refreshButton = document.querySelector("[data-crm-refresh]");
const logoutButton = document.querySelector("[data-crm-logout]");
const statusInput = document.querySelector("[data-crm-detail-status-input]");
const themeBadge = document.querySelector("[data-crm-theme-badge]");
const callLink = document.querySelector("[data-crm-call-link]");
const textLink = document.querySelector("[data-crm-text-link]");
const markQuotedButton = document.querySelector("[data-crm-mark-quoted]");
const sendEstimateButton = document.querySelector("[data-crm-send-estimate]");
const openEmailDraftButton = document.querySelector("[data-crm-open-email-draft]");
const copyEstimateButton = document.querySelector("[data-crm-copy-estimate]");
const detailTabButtons = Array.from(document.querySelectorAll("[data-crm-detail-tab]"));
const detailViews = Array.from(document.querySelectorAll("[data-crm-detail-view]"));
const conversationThread = document.querySelector("[data-crm-conversation-thread]");
const conversationSummary = document.querySelector("[data-crm-conversation-summary]");
const photoSection = document.querySelector("[data-crm-photo-section]");
const photoGrid = document.querySelector("[data-crm-photo-grid]");
const photoSummary = document.querySelector("[data-crm-photo-summary]");

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
    assistant_chat: "Assistant chat",
    assistant_booking: "Assistant callback",
  };

  return labels[source] || source.replace(/_/g, " ").trim();
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
  if (!detailFeedback) {
    return;
  }

  detailFeedback.textContent = message;
  detailFeedback.dataset.tone = tone;
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

function buildEstimateSnapshot() {
  const lead = state.leadDetail?.lead || {};
  const form = detailForm?.elements;
  const breakdownMode = hasBreakdownValues();
  const total = breakdownMode
    ? calculateEstimateTotal(lead)
    : Number(form?.estimateAmount?.value || lead.estimateAmount || 0) || 0;

  return {
    fullName: String(form?.fullName?.value || lead.fullName || "").trim(),
    firstName:
      String(form?.fullName?.value || lead.fullName || "")
        .trim()
        .split(/\s+/)
        .filter(Boolean)[0] || "there",
    email: String(form?.email?.value || lead.email || "").trim(),
    projectType: String(form?.projectType?.value || lead.projectType || "").trim(),
    location: String(form?.location?.value || lead.location || "").trim(),
    title: String(form?.estimateTitle?.value || lead.estimateTitle || "").trim(),
    scope: String(form?.estimateScope?.value || lead.estimateScope || "").trim(),
    notes: String(form?.estimateNotes?.value || lead.estimateNotes || "").trim(),
    validUntil:
      String(form?.estimateValidUntil?.value || "").trim() ||
      toDateInputValue(lead.estimateValidUntil || ""),
    total,
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
  const projectLabel = snapshot.title || snapshot.projectType || "metal repair estimate";
  return `Estimate from Chicago Metal Works & Fencing - ${projectLabel}`;
}

function buildEstimateBody(snapshot) {
  const lines = [
    `Hi ${snapshot.firstName},`,
    "",
    "Thank you for contacting Chicago Metal Works & Fencing.",
    "",
    `Project: ${snapshot.title || snapshot.projectType || "Metal repair"}`,
    `Estimated total: ${formatCurrency(snapshot.total)}`,
    snapshot.validUntil
      ? `Valid until: ${formatDateOnly(snapshot.validUntil)}`
      : "",
    snapshot.location ? `Location: ${snapshot.location}` : "",
    "",
    snapshot.scope ? `Scope of work:\n${snapshot.scope}` : "",
    snapshot.notes ? `Notes / exclusions:\n${snapshot.notes}` : "",
    "",
    `To move forward, reply to this email or call/text ${METALWORKS_CONTACT.phoneDisplay}.`,
    "",
    "Chicago Metal Works & Fencing",
    METALWORKS_CONTACT.phoneDisplay,
    METALWORKS_CONTACT.email,
  ].filter(Boolean);

  return lines.join("\n");
}

function openEmailDraft({ silent = false } = {}) {
  const snapshot = buildEstimateSnapshot();

  if (!snapshot.email) {
    setDetailFeedback("Este lead no tiene correo todavia.", "error");
    return false;
  }

  const mailto = `mailto:${encodeURIComponent(snapshot.email)}?subject=${encodeURIComponent(
    buildEstimateSubject(snapshot),
  )}&body=${encodeURIComponent(buildEstimateBody(snapshot))}`;

  window.location.href = mailto;

  if (!silent) {
    setDetailFeedback("Draft abierto en tu correo.", "muted");
  }

  return true;
}

async function copyEstimateText() {
  const snapshot = buildEstimateSnapshot();

  if (!snapshot.title && !snapshot.scope && !snapshot.total) {
    setDetailFeedback("Primero arma el estimate para poder copiarlo.", "error");
    return;
  }

  try {
    await navigator.clipboard.writeText(buildEstimateBody(snapshot));
    setDetailFeedback("Estimate copiado.", "success");
  } catch (error) {
    setDetailFeedback("No pude copiarlo automaticamente.", "error");
  }
}

function scrollDetailIntoView() {
  if (!detailPanel || window.innerWidth > 960) {
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
      note: `${summary.newLeads || 0} nuevos`,
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
      note: `${summary.lostLeads || 0} perdidos`,
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

function renderFilters(dashboard) {
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

function renderLeadList(leads = []) {
  if (!leadList) {
    return;
  }

  leadsCount.textContent = `${leads.length} lead${leads.length === 1 ? "" : "s"}`;

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
                ? `<span><strong>Estimate sent:</strong> ${escapeHtml(formatDate(lead.estimateSentAt))}</span>`
                : ""
            }
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
            <button type="button" class="crm-card-action" data-card-open-quote="${escapeHtml(lead.id)}">Quote</button>
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

      state.selectedLeadId = leadId;
      renderLeadList(leads);
      await loadLeadDetail(leadId);
      scrollDetailIntoView();
    });
  });

  leadList.querySelectorAll("[data-lead-id]").forEach((card) => {
    card.addEventListener("click", async () => {
      const leadId = String(card.getAttribute("data-lead-id") || "").trim();
      if (!leadId) {
        return;
      }

      state.selectedLeadId = leadId;
      renderLeadList(leads);
      await loadLeadDetail(leadId);
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

function renderConversationThread(messages = []) {
  if (!conversationThread || !conversationSummary) {
    return;
  }

  const safeMessages = Array.isArray(messages) ? messages.filter((item) => item?.content) : [];

  if (!safeMessages.length) {
    conversationSummary.textContent = "Sin transcript guardado.";
    conversationThread.innerHTML =
      '<p class="crm-empty-state">Todavia no hay conversacion guardada para este lead.</p>';
    return;
  }

  conversationSummary.textContent = `${safeMessages.length} mensaje${safeMessages.length === 1 ? "" : "s"} guardados`;
  conversationThread.innerHTML = safeMessages
    .map((item) => {
      const role = item.role === "assistant" ? "assistant" : "user";
      const roleLabel = role === "assistant" ? "Agustin 2.0" : "Cliente";

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

  if (!safeAssets.length) {
    photoSection.hidden = true;
    photoSummary.textContent = "No photos yet.";
    photoGrid.innerHTML = "";
    return;
  }

  photoSection.hidden = false;
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

function syncDetailQuickActions(lead = null) {
  const phoneDigits = getLeadPhoneDigits(lead);
  const hasPhone = Boolean(phoneDigits);
  const hasEmail = Boolean(lead?.email);

  if (callLink) {
    callLink.hidden = !hasPhone;
    callLink.href = hasPhone ? buildTelHref(phoneDigits) : "#";
  }

  if (textLink) {
    textLink.hidden = !hasPhone;
    textLink.href = hasPhone ? buildSmsHref(phoneDigits) : "#";
  }

  if (markQuotedButton) {
    markQuotedButton.disabled = !lead?.id;
  }

  if (sendEstimateButton) {
    sendEstimateButton.disabled = !lead?.id || !hasEmail;
  }

  if (openEmailDraftButton) {
    openEmailDraftButton.disabled = !lead?.id || !hasEmail;
  }

  if (copyEstimateButton) {
    copyEstimateButton.disabled = !lead?.id;
  }
}

function renderLeadDetail(detail = null) {
  state.leadDetail = detail;

  if (!detail?.lead) {
    detailWrap.hidden = true;
    detailEmpty.hidden = false;
    detailStatus.textContent = "Selecciona un lead";
    detailMeta.innerHTML = "";
    activityList.innerHTML = "";
    renderConversationThread([]);
    renderLeadAssets([]);
    syncDetailQuickActions(null);
    setDetailTab("profile");
    return;
  }

  const lead = detail.lead;
  detailWrap.hidden = false;
  detailEmpty.hidden = true;
  detailStatus.textContent = `${lead.statusLabel} · ${lead.projectType || "Sin servicio"}`;
  detailMeta.innerHTML = `
    <div class="crm-micro-list">
      <span class="crm-chip">${escapeHtml(lead.phoneDisplay || lead.phone || "")}</span>
      ${lead.email ? `<span class="crm-chip">${escapeHtml(lead.email)}</span>` : ""}
      ${lead.location ? `<span class="crm-chip">${escapeHtml(lead.location)}</span>` : ""}
      ${lead.estimateAmount ? `<span class="crm-chip">${escapeHtml(formatCurrency(lead.estimateAmount))}</span>` : ""}
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
          ? `<span><strong>Estimate sent:</strong> ${escapeHtml(formatDate(lead.estimateSentAt))}</span>`
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
      <span><strong>Proyecto:</strong> ${escapeHtml(lead.details || "")}</span>
      ${
        Array.isArray(lead.photoFileNames) && lead.photoFileNames.length
          ? `<span><strong>Fotos:</strong> ${escapeHtml(lead.photoFileNames.join(", "))}</span>`
          : ""
      }
    </div>
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
    detailForm.elements.details.value = lead.details || "";
    detailForm.elements.estimateAmount.value = lead.estimateAmount || "";
    detailForm.elements.estimateTitle.value = lead.estimateTitle || "";
    detailForm.elements.estimateScope.value = lead.estimateScope || "";
    detailForm.elements.estimateMaterialsCost.value = lead.estimateMaterialsCost || "";
    detailForm.elements.estimateLaborCost.value = lead.estimateLaborCost || "";
    detailForm.elements.estimateCoatingCost.value = lead.estimateCoatingCost || "";
    detailForm.elements.estimateMiscCost.value = lead.estimateMiscCost || "";
    detailForm.elements.estimateDiscount.value = lead.estimateDiscount || "";
    detailForm.elements.estimateValidUntil.value = toDateInputValue(lead.estimateValidUntil);
    detailForm.elements.estimateNotes.value = lead.estimateNotes || "";
    detailForm.elements.privateNotes.value = lead.privateNotes || "";
    detailForm.elements.note.value = "";
  }

  syncEstimateTotalFromForm();
  syncDetailQuickActions(lead);
  renderLeadAssets(detail.assets || []);
  renderConversationThread(lead.conversationHistory || []);
  renderActivityCards(activityList, detail.activity || []);
  setDetailTab(state.detailTab || "profile");
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
    details: String(formData.get("details") || "").trim(),
    estimateAmount: breakdownMode
      ? String(calculateEstimateTotal(lead))
      : String(formData.get("estimateAmount") || "").trim(),
    estimateTitle: String(formData.get("estimateTitle") || "").trim(),
    estimateScope: String(formData.get("estimateScope") || "").trim(),
    estimateValidUntil: String(formData.get("estimateValidUntil") || "").trim(),
    estimateNotes: String(formData.get("estimateNotes") || "").trim(),
    privateNotes: String(formData.get("privateNotes") || "").trim(),
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

async function loadDashboard() {
  const query = buildQueryString(state.filters);
  const url = query
    ? `/api/metalworks-crm/dashboard?${query}`
    : "/api/metalworks-crm/dashboard";

  const dashboard = await apiRequest(url);
  state.dashboard = dashboard;
  renderSummary(dashboard.summary, dashboard.serviceBreakdown);
  renderFilters(dashboard);
  renderLeadList(dashboard.leads || []);
  renderActivityCards(globalActivityList, dashboard.recentActivity || [], { hideBody: true });
  if (globalActivitySummary) {
    const totalEvents = Array.isArray(dashboard.recentActivity) ? dashboard.recentActivity.length : 0;
    globalActivitySummary.textContent = `${totalEvents} evento${totalEvents === 1 ? "" : "s"}`;
  }

  if (dashboard.leads?.length) {
    const selectedStillVisible = dashboard.leads.some(
      (lead) => lead.id === state.selectedLeadId,
    );
    if (!selectedStillVisible) {
      state.selectedLeadId = dashboard.leads[0].id;
    }
    await loadLeadDetail(state.selectedLeadId);
  } else {
    state.selectedLeadId = "";
    renderLeadDetail(null);
  }
}

async function loadLeadDetail(leadId) {
  if (!leadId) {
    renderLeadDetail(null);
    return;
  }

  const detail = await apiRequest(`/api/metalworks-crm/leads/${encodeURIComponent(leadId)}`);
  renderLeadDetail(detail);
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

async function handleSendEstimate() {
  const snapshot = buildEstimateSnapshot();

  if (!snapshot.email) {
    setDetailFeedback("Este lead no tiene correo todavia.", "error");
    return;
  }

  if (!snapshot.title && !snapshot.scope && !snapshot.total) {
    setDetailFeedback("Primero arma el estimate para poder enviarlo.", "error");
    return;
  }

  try {
    setDetailFeedback("Guardando estimate...", "muted");
    await saveLeadChanges({}, { refreshDashboard: false, showFeedback: false });
    setDetailFeedback("Enviando estimate...", "muted");

    const result = await apiRequest(
      `/api/metalworks-crm/leads/${encodeURIComponent(state.selectedLeadId)}/send-estimate`,
      {
        method: "POST",
      },
    );

    renderLeadDetail({
      lead: result.lead,
      activity: result.activity || [],
    });

    if (result.delivered) {
      setDetailFeedback(result.message || "Estimate enviado al cliente.", "success");
      await loadDashboard();
      return;
    }

    openEmailDraft({ silent: true });
    setDetailFeedback(
      result.message || "No pude enviarlo desde el sistema. Te abri un draft para mandarlo rapido.",
      "muted",
    );
    await loadDashboard();
  } catch (error) {
    const draftOpened = openEmailDraft({ silent: true });
    setDetailFeedback(
      draftOpened
        ? "No pude enviarlo desde el sistema. Te abri un draft para terminarlo rapido."
        : error.message,
      draftOpened ? "muted" : "error",
    );
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
    state.filters.status = String(statusFilter.value || "").trim();
    await loadDashboard();
  });

  serviceFilter?.addEventListener("change", async () => {
    state.filters.projectType = String(serviceFilter.value || "").trim();
    await loadDashboard();
  });

  searchInput?.addEventListener("input", () => {
    window.clearTimeout(state.searchTimer);
    state.searchTimer = window.setTimeout(async () => {
      state.filters.search = String(searchInput.value || "").trim();
      await loadDashboard();
    }, 220);
  });
}

function bindDetailActions() {
  detailForm?.addEventListener("submit", handleSaveLead);
  markQuotedButton?.addEventListener("click", handleMarkQuoted);
  sendEstimateButton?.addEventListener("click", handleSendEstimate);
  openEmailDraftButton?.addEventListener("click", () => {
    openEmailDraft();
  });
  copyEstimateButton?.addEventListener("click", copyEstimateText);

  detailForm?.addEventListener("input", (event) => {
    const fieldName = event.target?.name || "";
    if (!ESTIMATE_COST_FIELDS.includes(fieldName)) {
      return;
    }

    syncEstimateTotalFromForm();
  });

  detailTabButtons.forEach((button) => {
    button.addEventListener("click", () => {
      setDetailTab(button.dataset.crmDetailTab || "profile");
    });
  });
}

async function init() {
  const me = await apiRequest("/api/metalworks-crm/me");

  if (!me.authenticated) {
    window.location.href = "/metalworks-crm/login/";
    return;
  }

  applyProfileTheme(me.profile || {}, me.email || "");

  bindFilters();
  bindDetailActions();
  refreshButton?.addEventListener("click", loadDashboard);
  logoutButton?.addEventListener("click", handleLogout);

  await loadDashboard();
}

init().catch((error) => {
  console.error(error);
  window.location.href = "/metalworks-crm/login/";
});
