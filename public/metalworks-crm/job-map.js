(() => {
  const state = {
    range: "week",
    data: null,
    selectedId: "",
  };

  const boundsFallback = {
    north: 42.18,
    south: 41.35,
    west: -88.45,
    east: -87.35,
  };

  const $ = (selector) => document.querySelector(selector);
  const $$ = (selector) => Array.from(document.querySelectorAll(selector));

  function escapeHtml(value = "") {
    return String(value || "")
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;")
      .replace(/'/g, "&#39;");
  }

  function formatDateTime(value = "", mode = "dateTime") {
    if (!value) {
      return "Sin fecha";
    }

    const date = new Date(value);

    if (Number.isNaN(date.getTime())) {
      return "Sin fecha";
    }

    if (mode === "date") {
      return date.toLocaleDateString("en-US", {
        timeZone: "America/Chicago",
        weekday: "short",
        month: "short",
        day: "numeric",
      });
    }

    return date.toLocaleString("en-US", {
      timeZone: "America/Chicago",
      weekday: "short",
      month: "short",
      day: "numeric",
      hour: "numeric",
      minute: "2-digit",
    });
  }

  function formatMoney(value = 0) {
    const amount = Number(value || 0);

    if (!Number.isFinite(amount) || amount <= 0) {
      return "";
    }

    return amount.toLocaleString("en-US", {
      style: "currency",
      currency: "USD",
      maximumFractionDigits: 0,
    });
  }

  function buildMapsUrl(item = {}) {
    const query = item.address || item.location || [item.city, item.zipCode].filter(Boolean).join(" ");
    return `https://www.google.com/maps/search/?api=1&query=${encodeURIComponent(query || item.fullName || "Chicago")}`;
  }

  function getPointStyle(point = null, bounds = boundsFallback) {
    if (!point) {
      return "";
    }

    const x = ((point.lng - bounds.west) / (bounds.east - bounds.west)) * 100;
    const y = ((bounds.north - point.lat) / (bounds.north - bounds.south)) * 100;
    const clampedX = Math.max(2, Math.min(95, x));
    const clampedY = Math.max(3, Math.min(94, y));

    return `left:${clampedX.toFixed(2)}%;top:${clampedY.toFixed(2)}%;`;
  }

  function getItemAccent(item = {}) {
    if (item.appointmentStatus === "confirmed") {
      return "confirmed";
    }

    if (item.appointmentStatus === "en_route") {
      return "en-route";
    }

    if (item.status === "won" || item.status === "booked") {
      return "booked";
    }

    return "scheduled";
  }

  function renderStats() {
    const statsNode = $("[data-job-map-stats]");
    const stats = state.data?.stats || { total: 0, mapped: 0, unmapped: 0 };
    const generatedAt = state.data?.generatedAt ? formatDateTime(state.data.generatedAt) : "";

    if (!statsNode) {
      return;
    }

    statsNode.innerHTML = [
      ["Total", stats.total || 0],
      ["Con pin", stats.mapped || 0],
      ["Sin pin", stats.unmapped || 0],
      ["Actualizado", generatedAt || "Ahora"],
    ]
      .map(
        ([label, value]) => `
          <div class="crm-job-map-stat">
            <span>${escapeHtml(label)}</span>
            <strong>${escapeHtml(value)}</strong>
          </div>
        `,
      )
      .join("");
  }

  function renderPins() {
    const pinsNode = $("[data-job-map-pins]");
    const emptyNode = $("[data-job-map-empty]");
    const items = state.data?.items || [];
    const mappedItems = items.filter((item) => item.mapPoint);
    const bounds = state.data?.bounds || boundsFallback;

    if (!pinsNode) {
      return;
    }

    pinsNode.innerHTML = mappedItems
      .map((item, index) => {
        const accent = getItemAccent(item);
        const isSelected = item.id === state.selectedId;
        return `
          <button
            class="crm-job-map-pin ${isSelected ? "is-selected" : ""}"
            type="button"
            style="${getPointStyle(item.mapPoint, bounds)}"
            data-job-map-select="${escapeHtml(item.id)}"
            data-accent="${escapeHtml(accent)}"
            aria-label="${escapeHtml(item.fullName || "Job")} ${escapeHtml(formatDateTime(item.startsAt, item.scheduleMode))}"
          >
            <span>${index + 1}</span>
          </button>
        `;
      })
      .join("");

    if (emptyNode) {
      emptyNode.hidden = items.length > 0;
    }
  }

  function renderItemCard(item = {}, index = 0, { compact = false } = {}) {
    const accent = getItemAccent(item);
    const money = formatMoney(item.estimateAmount);
    const address = item.address || item.location || [item.city, item.zipCode].filter(Boolean).join(" ");
    const selectedClass = item.id === state.selectedId ? "is-selected" : "";
    const mapsUrl = buildMapsUrl(item);
    const crmUrl = `/metalworks-crm/?lead=${encodeURIComponent(item.id)}`;
    const calendarUrl = item.googleCalendarEventHtmlLink || "";

    return `
      <article class="crm-job-map-card ${selectedClass}" data-accent="${escapeHtml(accent)}">
        <button class="crm-job-map-card-main" type="button" data-job-map-select="${escapeHtml(item.id)}">
          <span class="crm-job-map-card-index">${index + 1}</span>
          <span>
            <strong>${escapeHtml(item.fullName || "Lead sin nombre")}</strong>
            <em>${escapeHtml(formatDateTime(item.startsAt, item.scheduleMode))}</em>
          </span>
        </button>
        <div class="crm-job-map-card-body">
          <p>${escapeHtml(item.projectType || "Trabajo")}</p>
          <p>${escapeHtml(address || "Direccion pendiente")}</p>
          ${
            compact
              ? ""
              : `<p>${escapeHtml(item.appointmentStatusLabel || item.statusLabel || "Agendado")}${
                  item.appointmentAssignedTo ? ` · ${escapeHtml(item.appointmentAssignedTo)}` : ""
                }${money ? ` · ${escapeHtml(money)}` : ""}</p>`
          }
        </div>
        <div class="crm-job-map-card-actions">
          <a href="${escapeHtml(crmUrl)}">CRM</a>
          <a href="${escapeHtml(mapsUrl)}" target="_blank" rel="noopener">Maps</a>
          ${calendarUrl ? `<a href="${escapeHtml(calendarUrl)}" target="_blank" rel="noopener">Calendar</a>` : ""}
        </div>
      </article>
    `;
  }

  function renderLists() {
    const listNode = $("[data-job-map-list]");
    const unmappedPanel = $("[data-job-map-unmapped-panel]");
    const unmappedNode = $("[data-job-map-unmapped]");
    const items = state.data?.items || [];
    const mappedItems = items.filter((item) => item.mapPoint);
    const unmappedItems = items.filter((item) => !item.mapPoint);

    if (listNode) {
      listNode.innerHTML = mappedItems.length
        ? mappedItems.map((item, index) => renderItemCard(item, index)).join("")
        : `<p class="crm-empty-copy">No hay trabajos con pin en este rango.</p>`;
    }

    if (unmappedPanel) {
      unmappedPanel.hidden = unmappedItems.length === 0;
    }

    if (unmappedNode) {
      unmappedNode.innerHTML = unmappedItems
        .map((item, index) => renderItemCard(item, index, { compact: true }))
        .join("");
    }
  }

  function setRange(range = "week") {
    state.range = range;
    $$("[data-job-map-range]").forEach((button) => {
      const active = button.dataset.jobMapRange === range;
      button.classList.toggle("is-active", active);
      button.setAttribute("aria-pressed", active ? "true" : "false");
    });
    loadJobMap();
  }

  function selectItem(id = "") {
    state.selectedId = id;
    renderPins();
    renderLists();
  }

  async function loadJobMap() {
    const statsNode = $("[data-job-map-stats]");

    if (statsNode) {
      statsNode.innerHTML = `<div class="crm-job-map-loading">Cargando mapa...</div>`;
    }

    try {
      const response = await fetch(`/api/metalworks-crm/job-map?range=${encodeURIComponent(state.range)}`, {
        credentials: "same-origin",
      });
      const data = await response.json().catch(() => null);

      if (!response.ok) {
        throw new Error(data?.message || "No pude cargar el mapa.");
      }

      state.data = data || {};
      state.selectedId = state.data?.items?.find((item) => item.mapPoint)?.id || "";
      renderStats();
      renderPins();
      renderLists();
    } catch (error) {
      state.data = { items: [], stats: { total: 0, mapped: 0, unmapped: 0 } };
      renderStats();
      renderPins();
      renderLists();
      if (statsNode) {
        statsNode.innerHTML = `
          <div class="crm-form-result crm-error">
            ${escapeHtml(error?.message || "No pude cargar el mapa.")}
          </div>
        `;
      }
    }
  }

  document.addEventListener("click", (event) => {
    const rangeButton = event.target.closest("[data-job-map-range]");
    if (rangeButton) {
      setRange(rangeButton.dataset.jobMapRange || "week");
      return;
    }

    const selectButton = event.target.closest("[data-job-map-select]");
    if (selectButton) {
      selectItem(selectButton.dataset.jobMapSelect || "");
      return;
    }

    if (event.target.closest("[data-job-map-refresh]")) {
      loadJobMap();
    }
  });

  loadJobMap();
})();
