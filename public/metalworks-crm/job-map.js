(() => {
  const state = {
    range: "week",
    data: null,
    selectedId: "",
    leafletMap: null,
    leafletMarkers: new Map(),
    leafletLayer: null,
    fullscreenFallback: false,
  };

  const boundsFallback = {
    north: 42.18,
    south: 41.35,
    west: -88.45,
    east: -87.35,
  };

  const shopBase = {
    id: "cmw-shop-base",
    label: "Base / Taller",
    name: "Chicago Metal Works & Fencing",
    address: "2059 Desplaines St, Blue Island, IL",
    lat: 41.6576,
    lng: -87.6804,
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

  function getMappedItems() {
    return (state.data?.items || []).filter((item) => item.mapPoint);
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

  function getMarkerColor(item = {}) {
    const accent = getItemAccent(item);
    if (accent === "booked") {
      return "#1f7a55";
    }
    if (accent === "confirmed") {
      return "#266ab0";
    }
    if (accent === "en-route") {
      return "#b67320";
    }
    return "#d64a31";
  }

  function buildPopupHtml(item = {}, index = 0) {
    const money = formatMoney(item.estimateAmount);
    const address = item.address || item.location || [item.city, item.zipCode].filter(Boolean).join(" ");
    const mapsUrl = buildMapsUrl(item);
    const crmUrl = `/metalworks-crm/?lead=${encodeURIComponent(item.id)}`;
    const calendarUrl = item.googleCalendarEventHtmlLink || "";

    return `
      <div class="crm-job-map-popup">
        <div class="crm-job-map-popup-kicker">Job ${index + 1}</div>
        <strong>${escapeHtml(item.fullName || "Lead sin nombre")}</strong>
        <span>${escapeHtml(formatDateTime(item.startsAt, item.scheduleMode))}</span>
        <p>${escapeHtml(item.projectType || "Trabajo")}</p>
        <p>${escapeHtml(address || "Direccion pendiente")}</p>
        <small>${escapeHtml(item.appointmentStatusLabel || item.statusLabel || "Agendado")}${
          item.appointmentAssignedTo ? ` · ${escapeHtml(item.appointmentAssignedTo)}` : ""
        }${money ? ` · ${escapeHtml(money)}` : ""}</small>
        <div>
          <a href="${escapeHtml(crmUrl)}">CRM</a>
          <a href="${escapeHtml(mapsUrl)}" target="_blank" rel="noopener">Google Maps</a>
          ${calendarUrl ? `<a href="${escapeHtml(calendarUrl)}" target="_blank" rel="noopener">Calendar</a>` : ""}
        </div>
      </div>
    `;
  }

  function buildBasePopupHtml() {
    const mapsUrl = `https://www.google.com/maps/search/?api=1&query=${encodeURIComponent(shopBase.address)}`;

    return `
      <div class="crm-job-map-popup">
        <div class="crm-job-map-popup-kicker">${escapeHtml(shopBase.label)}</div>
        <strong>${escapeHtml(shopBase.name)}</strong>
        <span>Referencia fija para distancias</span>
        <p>${escapeHtml(shopBase.address)}</p>
        <div>
          <a href="${escapeHtml(mapsUrl)}" target="_blank" rel="noopener">Google Maps</a>
        </div>
      </div>
    `;
  }

  function ensureLeafletMap() {
    const realMapNode = $("[data-job-map-real]");
    const canvasNode = $("[data-job-map-canvas]");

    if (!realMapNode || typeof window.L === "undefined") {
      return false;
    }

    if (state.leafletMap) {
      window.setTimeout(() => state.leafletMap.invalidateSize(), 0);
      return true;
    }

    canvasNode?.classList.add("has-real-map");
    state.leafletMap = window.L.map(realMapNode, {
      center: [41.878, -87.73],
      zoom: 10,
      minZoom: 8,
      maxZoom: 18,
      scrollWheelZoom: true,
    });
    window.L.tileLayer("https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png", {
      attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a>',
      maxZoom: 19,
    }).addTo(state.leafletMap);
    state.leafletLayer = window.L.layerGroup().addTo(state.leafletMap);

    window.setTimeout(() => state.leafletMap.invalidateSize(), 0);
    return true;
  }

  function renderLeafletMarkers() {
    if (!ensureLeafletMap() || !state.leafletLayer) {
      return false;
    }

    const mappedItems = getMappedItems();
    state.leafletLayer.clearLayers();
    state.leafletMarkers.clear();

    const baseMarker = window.L.marker([shopBase.lat, shopBase.lng], {
      title: shopBase.label,
      zIndexOffset: 1000,
      icon: window.L.divIcon({
        className: "crm-job-map-leaflet-marker crm-job-map-leaflet-marker-base",
        html: `<span><b>CMW</b></span>`,
        iconSize: [46, 54],
        iconAnchor: [23, 52],
        popupAnchor: [0, -48],
      }),
    });
    baseMarker.bindPopup(buildBasePopupHtml(), {
      closeButton: true,
      maxWidth: 300,
    });
    baseMarker.addTo(state.leafletLayer);

    const latLngs = [[shopBase.lat, shopBase.lng]];
    mappedItems.forEach((item, index) => {
      const point = item.mapPoint;
      const marker = window.L.marker([point.lat, point.lng], {
        title: item.fullName || "Job",
        icon: window.L.divIcon({
          className: "crm-job-map-leaflet-marker",
          html: `<span style="--marker-color:${escapeHtml(getMarkerColor(item))}"><b>${index + 1}</b></span>`,
          iconSize: [34, 44],
          iconAnchor: [17, 42],
          popupAnchor: [0, -38],
        }),
      });

      marker.bindPopup(buildPopupHtml(item, index), {
        closeButton: true,
        maxWidth: 300,
      });
      marker.on("click", () => selectItem(item.id));
      marker.addTo(state.leafletLayer);
      state.leafletMarkers.set(item.id, marker);
      latLngs.push([point.lat, point.lng]);
    });

    if (latLngs.length > 1) {
      state.leafletMap.fitBounds(latLngs, { padding: [44, 44], maxZoom: 12 });
    } else if (latLngs.length === 1) {
      state.leafletMap.setView(latLngs[0], 12);
    } else {
      state.leafletMap.setView([41.878, -87.73], 10);
    }

    return true;
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

  function resizeMapSoon() {
    if (!state.leafletMap) {
      return;
    }

    window.setTimeout(() => {
      state.leafletMap.invalidateSize();
      const marker = state.selectedId ? state.leafletMarkers.get(state.selectedId) : null;
      if (marker) {
        state.leafletMap.panTo(marker.getLatLng(), { animate: false });
      }
    }, 160);
  }

  function isMapFullscreen() {
    const targetNode = $("[data-job-map-fullscreen-target]");
    return document.fullscreenElement === targetNode || state.fullscreenFallback;
  }

  function syncFullscreenButton() {
    const button = $("[data-job-map-fullscreen]");
    const targetNode = $("[data-job-map-fullscreen-target]");
    const fullscreen = isMapFullscreen();

    targetNode?.classList.toggle("is-map-fullscreen", state.fullscreenFallback);
    document.body.classList.toggle("is-job-map-fullscreen-fallback", state.fullscreenFallback);

    if (!button) {
      return;
    }

    button.textContent = fullscreen ? "Salir de pantalla completa" : "Pantalla completa";
    button.setAttribute("aria-label", fullscreen ? "Salir de pantalla completa" : "Expandir mapa a pantalla completa");
    button.setAttribute("aria-pressed", fullscreen ? "true" : "false");
  }

  async function toggleMapFullscreen() {
    const targetNode = $("[data-job-map-fullscreen-target]");

    if (!targetNode) {
      return;
    }

    if (document.fullscreenElement === targetNode) {
      await document.exitFullscreen?.();
      syncFullscreenButton();
      resizeMapSoon();
      return;
    }

    if (state.fullscreenFallback) {
      state.fullscreenFallback = false;
      syncFullscreenButton();
      resizeMapSoon();
      return;
    }

    if (targetNode.requestFullscreen) {
      try {
        await targetNode.requestFullscreen();
      } catch (error) {
        state.fullscreenFallback = true;
      }
    } else {
      state.fullscreenFallback = true;
    }

    syncFullscreenButton();
    resizeMapSoon();
  }

  function renderPins() {
    const pinsNode = $("[data-job-map-pins]");
    const emptyNode = $("[data-job-map-empty]");
    const items = state.data?.items || [];
    const mappedItems = getMappedItems();
    const bounds = state.data?.bounds || boundsFallback;
    const renderedLeaflet = renderLeafletMarkers();

    if (!pinsNode) {
      return;
    }

    if (renderedLeaflet) {
      pinsNode.innerHTML = "";
      if (emptyNode) {
        emptyNode.hidden = items.length > 0;
      }
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

    pinsNode.insertAdjacentHTML(
      "afterbegin",
      `
        <div
          class="crm-job-map-pin crm-job-map-pin-base"
          style="${getPointStyle({ lat: shopBase.lat, lng: shopBase.lng }, bounds)}"
          aria-label="${escapeHtml(shopBase.label)} ${escapeHtml(shopBase.address)}"
        >
          <span>CMW</span>
        </div>
      `,
    );

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

    const marker = state.leafletMarkers.get(id);
    if (marker && state.leafletMap) {
      const latLng = marker.getLatLng();
      state.leafletMap.panTo(latLng, { animate: true, duration: 0.35 });
      marker.openPopup();
    }
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
      return;
    }

    if (event.target.closest("[data-job-map-fullscreen]")) {
      toggleMapFullscreen();
    }
  });

  document.addEventListener("fullscreenchange", () => {
    if (document.fullscreenElement) {
      state.fullscreenFallback = false;
    }
    syncFullscreenButton();
    resizeMapSoon();
  });

  document.addEventListener("keydown", (event) => {
    if (event.key === "Escape" && state.fullscreenFallback) {
      state.fullscreenFallback = false;
      syncFullscreenButton();
      resizeMapSoon();
    }
  });

  syncFullscreenButton();
  loadJobMap();
})();
