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

const state = {
  dashboard: null,
  leadDetail: null,
  selectedLeadId: "",
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
const detailFeedback = document.querySelector("[data-crm-detail-feedback]");
const activityList = document.querySelector("[data-crm-activity-list]");
const globalActivityList = document.querySelector("[data-crm-global-activity]");
const leadsCount = document.querySelector("[data-crm-leads-count]");
const statusFilter = document.querySelector("[data-crm-status-filter]");
const serviceFilter = document.querySelector("[data-crm-service-filter]");
const searchInput = document.querySelector("[data-crm-search]");
const userChip = document.querySelector("[data-crm-user-chip]");
const refreshButton = document.querySelector("[data-crm-refresh]");
const logoutButton = document.querySelector("[data-crm-logout]");
const statusInput = document.querySelector("[data-crm-detail-status-input]");

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

function formatCurrency(value = 0) {
  const amount = Number(value || 0) || 0;
  return new Intl.NumberFormat("en-US", {
    style: "currency",
    currency: "USD",
    maximumFractionDigits: 0,
  }).format(amount);
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

function setDetailFeedback(message = "", tone = "") {
  if (!detailFeedback) {
    return;
  }

  detailFeedback.textContent = message;
  detailFeedback.dataset.tone = tone;
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
          </div>
        </article>
      `;
    })
    .join("");

  leadList.querySelectorAll("[data-lead-id]").forEach((card) => {
    card.addEventListener("click", () => {
      const leadId = String(card.getAttribute("data-lead-id") || "").trim();
      if (!leadId) {
        return;
      }

      state.selectedLeadId = leadId;
      renderLeadList(leads);
      loadLeadDetail(leadId);
    });
  });
}

function renderActivityCards(target, activities = []) {
  if (!target) {
    return;
  }

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
          ${
            item.body
              ? `<p>${escapeHtml(item.body)}</p>`
              : ""
          }
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

function renderLeadDetail(detail = null) {
  state.leadDetail = detail;

  if (!detail?.lead) {
    detailWrap.hidden = true;
    detailEmpty.hidden = false;
    detailStatus.textContent = "Selecciona un lead";
    detailMeta.innerHTML = "";
    activityList.innerHTML = "";
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
    </div>
    <div class="crm-detail-meta">
      <span><strong>Creado:</strong> ${escapeHtml(formatDate(lead.createdAt) || "Sin fecha")}</span>
      <span><strong>Ultimo contacto:</strong> ${escapeHtml(formatDate(lead.lastContactAt) || "Sin registrar")}</span>
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
    detailForm.elements.status.value = lead.status || "new";
    detailForm.elements.nextAction.value = lead.nextAction || "";
    detailForm.elements.nextActionAt.value = toDatetimeLocalValue(lead.nextActionAt);
    detailForm.elements.estimateAmount.value = lead.estimateAmount || "";
    detailForm.elements.privateNotes.value = lead.privateNotes || "";
    detailForm.elements.note.value = "";
  }

  renderActivityCards(activityList, detail.activity || []);
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
  renderActivityCards(globalActivityList, dashboard.recentActivity || []);

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

  if (!state.selectedLeadId || !detailForm) {
    return;
  }

  setDetailFeedback("Guardando...", "muted");

  const formData = new FormData(detailForm);
  const body = {
    status: String(formData.get("status") || "").trim(),
    nextAction: String(formData.get("nextAction") || "").trim(),
    nextActionAt: String(formData.get("nextActionAt") || "").trim(),
    estimateAmount: String(formData.get("estimateAmount") || "").trim(),
    privateNotes: String(formData.get("privateNotes") || "").trim(),
    note: String(formData.get("note") || "").trim(),
  };

  try {
    const detail = await apiRequest(
      `/api/metalworks-crm/leads/${encodeURIComponent(state.selectedLeadId)}`,
      {
        method: "PATCH",
        body,
      },
    );

    renderLeadDetail(detail);
    setDetailFeedback("Seguimiento guardado.", "success");
    await loadDashboard();
  } catch (error) {
    setDetailFeedback(error.message, "error");
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

async function init() {
  const me = await apiRequest("/api/metalworks-crm/me");

  if (!me.authenticated) {
    window.location.href = "/metalworks-crm/login/";
    return;
  }

  if (userChip) {
    userChip.textContent = me.email || "Admin";
  }

  bindFilters();
  refreshButton?.addEventListener("click", loadDashboard);
  logoutButton?.addEventListener("click", handleLogout);
  detailForm?.addEventListener("submit", handleSaveLead);

  await loadDashboard();
}

init().catch((error) => {
  console.error(error);
  window.location.href = "/metalworks-crm/login/";
});
