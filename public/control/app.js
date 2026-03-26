const OVERVIEW_API_URL = "/api/control/overview";

function escapeHtml(value = "") {
  return String(value)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

function formatDateTime(value = "") {
  if (!value) {
    return "Sin dato";
  }

  const date = new Date(value);

  if (Number.isNaN(date.getTime())) {
    return "Sin dato";
  }

  return new Intl.DateTimeFormat("es-US", {
    month: "short",
    day: "numeric",
    hour: "numeric",
    minute: "2-digit"
  }).format(date);
}

function renderTags(target, values = [], formatter = value => value) {
  if (!target) {
    return;
  }

  const items = Array.isArray(values) ? values.filter(Boolean) : [];

  if (!items.length) {
    target.innerHTML = "<span>Sin datos todavia</span>";
    return;
  }

  target.innerHTML = items
    .map(item => `<span>${escapeHtml(formatter(item))}</span>`)
    .join("");
}

function renderTableRows(target, rows = [], mapper) {
  if (!target) {
    return;
  }

  if (!rows.length) {
    target.innerHTML = `<tr><td colspan="${target.dataset.cols || 4}" class="table-empty">Sin datos todavia.</td></tr>`;
    return;
  }

  target.innerHTML = rows.map(mapper).join("");
}

async function cargarControlTower() {
  const response = await fetch(OVERVIEW_API_URL, {
    credentials: "include"
  });

  const data = await response.json().catch(() => ({}));

  if (!response.ok) {
    throw new Error(data.error || "No pude cargar la torre de control.");
  }

  return data;
}

function setText(selector, value) {
  const el = document.querySelector(selector);

  if (el) {
    el.textContent = value;
  }
}

function hydrateDashboard(data) {
  setText("[data-control-updated]", `Actualizado ${formatDateTime(data.updatedAt)}`);

  setText("[data-kpi-chef]", String(data.overview?.chefFamiliesGuided || 0));
  setText("[data-kpi-coach]", String(data.overview?.coachDistributors || 0));
  setText("[data-kpi-wa-today]", String(data.overview?.whatsappRepliesToday || 0));
  setText("[data-kpi-ready]", String(data.overview?.callsReady || 0));

  setText("[data-chef-active-today]", String(data.chef?.activosHoy || 0));
  setText("[data-chef-active-week]", String(data.chef?.activos7Dias || 0));
  setText("[data-chef-questions]", String(data.chef?.preguntasTotales || 0));
  renderTags(document.querySelector("[data-chef-topics]"), data.chef?.topTopics || []);

  setText("[data-coach-active-today]", String(data.coach?.activeToday || 0));
  setText("[data-coach-active-week]", String(data.coach?.activeLast7Days || 0));
  setText("[data-coach-questions]", String(data.coach?.totalQuestions || 0));
  renderTags(document.querySelector("[data-coach-topics]"), data.coach?.topTopics || []);
  renderTags(document.querySelector("[data-coach-objections]"), data.coach?.topObjections || []);
  renderTags(document.querySelector("[data-coach-stages]"), data.coach?.topStages || []);

  setText("[data-wa-replies-today]", String(data.whatsapp?.repliesToday || 0));
  setText("[data-wa-replies-week]", String(data.whatsapp?.replies7Days || 0));
  renderTags(document.querySelector("[data-wa-topics]"), data.whatsapp?.topTopics || []);

  setText("[data-leads-total]", String(data.leads?.totalProfiles || 0));
  setText("[data-leads-interested]", String(data.leads?.interestedProfiles || 0));
  setText("[data-leads-customers]", String(data.leads?.customerProfiles || 0));
  renderTags(
    document.querySelector("[data-leads-products]"),
    data.leads?.topInterestProducts || [],
    item => `${item.label} (${item.count})`
  );

  const waTable = document.querySelector("[data-wa-recent-table]");
  if (waTable) {
    waTable.dataset.cols = "4";
  }
  renderTableRows(
    waTable,
    data.whatsapp?.recentReplies || [],
    item => `
      <tr>
        <td>${escapeHtml(formatDateTime(item.createdAt))}</td>
        <td>${escapeHtml(item.phone || "Sin dato")}</td>
        <td>${escapeHtml(item.content || "Sin mensaje")}</td>
        <td>${escapeHtml(item.intent || "general")}</td>
      </tr>
    `
  );

  const leadsTable = document.querySelector("[data-leads-ready-table]");
  if (leadsTable) {
    leadsTable.dataset.cols = "5";
  }
  renderTableRows(
    leadsTable,
    data.leads?.recentReadyLeads || [],
    item => `
      <tr>
        <td>${escapeHtml(item.name || "Sin nombre")}</td>
        <td>${escapeHtml(item.phone || "Sin dato")}</td>
        <td>${escapeHtml(`${item.bestCallDay || "Sin dia"} / ${item.bestCallTime || "Sin hora"}`)}</td>
        <td>${escapeHtml(item.leadStatus || "sin estado")}</td>
        <td>${escapeHtml(item.summary || "Sin resumen")}</td>
      </tr>
    `
  );
}

async function init() {
  try {
    const data = await cargarControlTower();
    hydrateDashboard(data);
  } catch (error) {
    setText("[data-control-updated]", error.message || "No pude cargar la torre de control.");
  }
}

init();
