const OVERVIEW_API_URL = "/api/control/overview";
const SYSTEM_CODE_LINES = [
  "lead.score = warm",
  "chef.intent = recipe_support",
  "coach.route = follow_up",
  "territory.signal = active",
  "wa.reply_window = monitoring",
  "handoff.status = synchronized",
  "call.queue = optimizing",
  "notes.summary = refreshed",
  "prospect.flow = active",
  "pipeline.signal = healthy",
  "lead.memory = linked",
  "system.guard = private"
];
const SYSTEM_STATUS_LINES = [
  "Prospectando y generando clientes en tiempo real",
  "Analizando conversaciones y detectando interes comercial",
  "Sincronizando Chef, Coach y handoff operativo",
  "Moviendo oportunidades activas dentro del territorio"
];

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

function formatMoney(value = 0) {
  return `$${(Number(value) || 0).toFixed(2)}`;
}

function formatTrackedStatusLabel(status = "") {
  const safe = String(status || "").trim().toLowerCase();

  if (safe === "pendiente_registro") {
    return "Pendiente de registro";
  }

  if (safe === "test_access") {
    return "Prueba gratis";
  }

  if (safe === "activa" || safe === "active" || safe === "trialing") {
    return "Activa";
  }

  return safe ? safe.replaceAll("_", " ") : "Sin estado";
}

function formatAccountTypeLabel(value = "") {
  const safe = String(value || "").trim().toLowerCase();

  if (safe === "leader") {
    return "Leader";
  }

  if (safe === "seat") {
    return "Subcuenta";
  }

  return safe === "owner" ? "Principal" : "Cuenta";
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

function buildCodeStream() {
  const target = document.querySelector("[data-code-stream-track]");

  if (!target) {
    return;
  }

  const lines = [...SYSTEM_CODE_LINES, ...SYSTEM_CODE_LINES];

  target.innerHTML = lines
    .map((line, index) => {
      const accent = index % 4 === 1 ? " code-line-accent" : "";
      return `<div class="code-line${accent}">${escapeHtml(line)}</div>`;
    })
    .join("");
}

function randomBinaryRow(length = 18) {
  return Array.from({ length }, () => (Math.random() > 0.5 ? "1" : "0")).join("");
}

function buildBinaryGrid() {
  const target = document.querySelector("[data-binary-grid]");

  if (!target) {
    return;
  }

  const columns = Array.from({ length: 15 }, (_, index) => {
    const left = 3 + index * 6.5;
    const duration = 10 + (index % 5) * 2.2;
    const delay = -(index * 1.15);
    const opacity = 0.42 + (index % 4) * 0.1;
    const rows = Array.from({ length: 22 }, () => randomBinaryRow(14)).join("<br>");

    return `
      <div
        class="binary-column"
        style="left:${left}%;animation-duration:${duration}s;animation-delay:${delay}s;opacity:${opacity};"
      >${rows}</div>
    `;
  });

  target.innerHTML = columns.join("");
}

function rotateSystemStatus() {
  const target = document.querySelector("[data-system-status-line]");

  if (!target) {
    return;
  }

  let index = 0;
  target.textContent = SYSTEM_STATUS_LINES[index];

  window.setInterval(() => {
    index = (index + 1) % SYSTEM_STATUS_LINES.length;
    target.textContent = SYSTEM_STATUS_LINES[index];
  }, 3200);
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
  setText("[data-leads-ready]", String(data.leads?.readyToCallProfiles || 0));
  setText("[data-leads-follow-up]", String(data.leads?.followUpNeededProfiles || 0));
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

  const followUpTable = document.querySelector("[data-leads-followup-table]");
  if (followUpTable) {
    followUpTable.dataset.cols = "5";
  }
  renderTableRows(
    followUpTable,
    data.leads?.recentFollowUpLeads || [],
    item => `
      <tr>
        <td>${escapeHtml(item.name || "Sin nombre")}</td>
        <td>${escapeHtml(item.phone || "Sin dato")}</td>
        <td>${escapeHtml(item.pending || "seguimiento")}</td>
        <td>${escapeHtml(item.leadStatus || "sin estado")}</td>
        <td>${escapeHtml(item.summary || "Sin resumen")}</td>
      </tr>
    `
  );

  setText("[data-sponsor-accounts]", String(data.sponsor?.sponsoredAccounts || 0));
  setText("[data-sponsor-sales]", String(data.sponsor?.salesCount || 0));
  setText("[data-sponsor-amount]", formatMoney(data.sponsor?.soldAmount || 0));
  setText("[data-sponsor-commission]", formatMoney(data.sponsor?.estimatedCommission || 0));

  const sponsorTable = document.querySelector("[data-sponsor-sales-table]");
  if (sponsorTable) {
    sponsorTable.dataset.cols = "5";
  }
  renderTableRows(
    sponsorTable,
    data.sponsor?.recentSales || [],
    item => `
      <tr>
        <td>${escapeHtml(formatDateTime(item.createdAt))}</td>
        <td>${escapeHtml(item.ownerName || "Cuenta patrocinada")}</td>
        <td>${escapeHtml(item.generatedByName || "Sin dato")}</td>
        <td>${escapeHtml(formatMoney(item.saleAmount || 0))}</td>
        <td>${escapeHtml(formatMoney(item.sponsorCommissionAmount || 0))}</td>
      </tr>
    `
  );

  setText("[data-tracked-accounts]", String(data.tracked?.summary?.trackedAccounts || 0));
  setText("[data-tracked-active]", String(data.tracked?.summary?.activeAccounts || 0));
  setText("[data-tracked-seats]", String(data.tracked?.summary?.activeSeats || 0));
  setText("[data-tracked-territory-count]", String((data.tracked?.summary?.territories || []).length || 0));
  setText("[data-tracked-synced]", String(data.tracked?.summary?.syncedLeads || 0));
  setText("[data-tracked-open]", String(data.tracked?.summary?.openOpportunities || 0));

  renderTags(document.querySelector("[data-tracked-territories]"), data.tracked?.summary?.territories || []);

  const trackedTable = document.querySelector("[data-tracked-accounts-table]");
  if (trackedTable) {
    trackedTable.dataset.cols = "9";
  }
  renderTableRows(
    trackedTable,
    data.tracked?.accounts || [],
    item => `
      <tr>
        <td>${escapeHtml(item.name || "Cuenta vigilada")}</td>
        <td>${escapeHtml(item.email || "Sin correo")}</td>
        <td>${escapeHtml(formatTrackedStatusLabel(item.status))}</td>
        <td>${escapeHtml(
          [formatAccountTypeLabel(item.accountType), item.teamRole || item.seatLabel || ""].filter(Boolean).join(" · ")
        )}</td>
        <td>${escapeHtml([item.officeId || "", item.territoryId || ""].filter(Boolean).join(" · ") || "Sin dato")}</td>
        <td>${escapeHtml(`${item.activeSeats || 0}/${item.totalSeats || 0}`)}</td>
        <td>${escapeHtml(String(item.totalLeads || 0))}</td>
        <td>${escapeHtml(`${item.salesCount || 0} · ${formatMoney(item.soldAmount || 0)}`)}</td>
        <td>${escapeHtml(
          `Leads ${item.syncedLeads || 0} · Opp ${item.openOpportunities || 0}${
            item.campaignSignals ? ` · Camp ${item.campaignSignals}` : ""
          }`
        )}</td>
      </tr>
    `
  );
}

async function init() {
  buildCodeStream();
  buildBinaryGrid();
  rotateSystemStatus();

  try {
    const data = await cargarControlTower();
    hydrateDashboard(data);
  } catch (error) {
    setText("[data-control-updated]", error.message || "No pude cargar la torre de control.");
  }
}

init();
