import { PROPERTY_VISIT_STATUSES, propertyVisitKey, mergePropertyVisits, propertyVisitFromQueuedItem } from "./property-visit-helpers.js?v=20261023-product-picker2";
import { initTecAddressSuggestions } from "./address-suggestions.js?v=20261028-visit-tools2";

import { MAX_ROUTE_STOPS as MAX_STOPS, emptyPropertyRoute, createPropertyRouteStore, toggleRouteStop, completeRouteStop, moveRouteStop, nextRouteStop, orderRouteByProximity, routeNavigationUrl } from "./property-route-state.js?v=20261028-route50-1";

(() => {

  let leafletPromise;
  function loadLeaflet() {
    if (window.L) return Promise.resolve(window.L);
    if (!leafletPromise) {
      leafletPromise = new Promise((resolve, reject) => {
        if (!document.querySelector("[data-tec-leaflet-style]")) {
          const style = document.createElement("link");
          style.rel = "stylesheet";
          style.href = "/coach/vendor/leaflet/leaflet.css";
          style.dataset.tecLeafletStyle = "true";
          document.head.append(style);
        }
        const script = document.createElement("script");
        script.src = "/coach/vendor/leaflet/leaflet.js";
        script.onload = () => window.L ? resolve(window.L) : reject(new Error("No pude cargar el mapa."));
        script.onerror = () => { script.remove(); reject(new Error("No pude cargar el mapa. La lista de propiedades sigue disponible.")); };
        document.head.append(script);
      }).catch(error => { leafletPromise = null; throw error; });
    }
    return leafletPromise;
  }

  function node(tag, className, text) {
    const element = document.createElement(tag);
    if (className) element.className = className;
    if (text != null) element.textContent = text;
    return element;
  }

  window.initTecPropertyMap = function ({ request, user = {}, offline = null, getCurrentAppointment = () => null }) {
    const root = document.querySelector("[data-property-workspace]");
    if (!root || root.dataset.initialized) return;
    root.dataset.initialized = "true";
    const find = selector => root.querySelector(`[data-property-${selector}]`);
    const form = find("search"), feedback = find("feedback"), candidatesBox = find("candidates"), results = find("results");
    const addressSuggestions = initTecAddressSuggestions({ form, request });
    const mapBox = find("map"), list = find("list"), route = find("route"), clear = find("clear");
    const appleRoute = find("apple-route");
    let routeState = emptyPropertyRoute(), routeBusy = false;
    let data = null, selected = [], markers = new Map(), map = null, layers = null, loading = false;
    let visits = new Map(), pendingLeadKeys = new Set(), refreshPromise = null, syncPromise = null, syncRequested = false, editingProperty = null, awaitingSync = false;
    const accountId = String(user.id || user._id || "");
    const email = String(user.email || "").trim().toLowerCase();
    const cacheKey = `property-visits:${accountId}:${email}`;
    const routeStore = createPropertyRouteStore({ accountId, email, request, offline,
      onChange: value => { routeState = value; selected = value.stops.map(stop => stop.property); syncRoute(); if (selected.length && (!map || results.hidden)) ensureMap(); },
      onStatus: text => { find("route-status").textContent = text; }
    });
    async function changeRoute(update) {
      if (routeBusy) return false;
      routeBusy = true; syncRoute();
      try { await routeStore.change(update); return true; }
      catch (error) { find("route-status").textContent = `No pude guardar el cambio: ${error.message}. Inténtalo de nuevo.`; return false; }
      finally { routeBusy = false; syncRoute(); }
    }
    routeStore.ready.then(() => { syncRoute(); ensureMap(); routeStore.sync(); }).catch(() => {});
    const visitMeta = status => PROPERTY_VISIT_STATUSES.find(item => item.value === status);
    const dateLabel = value => value ? new Date(value).toLocaleString("es-US", { dateStyle: "medium", timeStyle: "short" }) : "";
    const uniqueId = prefix => `${prefix}-${crypto.randomUUID()}`;

    async function pendingItems() {
      const items = offline ? await offline.pending() : [];
      return items.filter(item => item.ownerEmail === email && String(item.body?.accountId || "") === accountId);
    }

    function visitLabel(record) {
      return `${visitMeta(record.status)?.label || "Seguimiento"}${record.nextVisitAt ? ` · ${dateLabel(record.nextVisitAt)}` : ""}${record.pending ? " · Pendiente de sincronizar" : ""}`;
    }

    function refreshVisitDecorations() {
      for (const card of list.children) {
        const property = data?.properties.find(item => item.pin === card.dataset.pin);
        const badge = card.querySelector("[data-visit-badge]");
        if (!property || !badge) continue;
        const record = visits.get(propertyVisitKey(property));
        badge.replaceChildren();
        if (record) {
          const status = node("span", "tec-visit-badge", visitLabel(record));
          status.style.setProperty("--visit-color", visitMeta(record.status)?.color || "#173d64");
          badge.append(status);
          if (record.notes) badge.append(node("p", "tec-visit-note-preview", record.notes));
        }
      }
      const saved = [...visits.values()].sort((a, b) => String(b.clientUpdatedAt).localeCompare(String(a.clientUpdatedAt)));
      find("saved-count").textContent = String(saved.length);
      find("saved-list").replaceChildren();
      for (const record of saved.slice(0, 100)) {
        const button = node("button", "tec-visit-saved-row"); button.type = "button";
        button.append(node("strong", "", record.property.formattedAddress), node("span", "", visitLabel(record)));
        button.addEventListener("click", () => openVisit(record.property));
        find("saved-list").append(button);
      }
      if (!saved.length) find("saved-list").append(node("p", "", "Todavía no tienes seguimientos. Usa el botón Seguimiento de cualquier propiedad."));
      drawMarkers();
    }

    async function rememberVisits() {
      await offline?.write(cacheKey, [...visits.values()].slice(0, 300)).catch(() => null);
    }

    async function loadLocalVisits() {
      const stored = offline ? await offline.read(cacheKey).catch(() => []) : [];
      const pending = await pendingItems().catch(() => []);
      pendingLeadKeys = new Set(pending.filter(item => item.kind === "property-lead").map(item => propertyVisitKey(item.body.property)));
      const records = mergePropertyVisits(Array.isArray(stored) ? stored : [], pending.map(propertyVisitFromQueuedItem).filter(Boolean));
      visits = new Map(records.map(record => [record.propertyKey, record]));
      refreshVisitDecorations();
    }

    async function refreshVisits() {
      if (refreshPromise) return refreshPromise;
      if (!navigator.onLine || !accountId) return;
      refreshPromise = (async () => {
        try {
          const recent = await request("/api/coach/property-visits");
          let zone = [];
          if (data) {
            const lookup = await request("/api/coach/property-visits/lookup", { method: "POST", body: { keys: [data.anchor, ...data.properties].map(propertyVisitKey) } });
            zone = lookup.visits || [];
          }
          const all = new Map([...visits.values(), ...(recent.visits || []), ...zone].map(record => [record.propertyKey, record]));
          const pending = await pendingItems().catch(() => []);
          pendingLeadKeys = new Set(pending.filter(item => item.kind === "property-lead").map(item => propertyVisitKey(item.body.property)));
          const combined = mergePropertyVisits([...all.values()], pending.map(propertyVisitFromQueuedItem).filter(Boolean));
          visits = new Map(combined.slice(0, 300).map(record => [record.propertyKey, record]));
          find("saved-status").textContent = "Las fichas y notas son privadas de tu cuenta. Los cambios pendientes se sincronizan al recuperar conexión.";
          await rememberVisits();
          refreshVisitDecorations();
          syncLeadButton();
          const current = editingProperty && visits.get(propertyVisitKey(editingProperty));
          if (dialog.open && awaitingSync && current && !current.pending && !pendingLeadKeys.has(current.propertyKey)) {
            visitMessage(current.linkedLeadId ? "Seguimiento sincronizado y lead creado en tu cuenta de TEC." : "Seguimiento sincronizado con tu cuenta.");
            awaitingSync = false;
          }
        } catch (error) {
          find("saved-status").textContent = "Mostrando las fichas guardadas en este teléfono. No pude actualizar tu cuenta en este momento.";
        }
      })();
      try { return await refreshPromise; } finally { refreshPromise = null; }
    }

    function kickSync() {
      syncRequested = true;
      if (syncPromise || !navigator.onLine || !offline) return;
      syncPromise = (async () => {
        while (syncRequested && navigator.onLine) {
          syncRequested = false;
          await offline.flush();
          await refreshVisits();
        }
      })().catch(() => null).finally(() => { syncPromise = null; });
    }

    const dialog = node("dialog", "tec-visit-dialog");
    dialog.setAttribute("aria-labelledby", "tec-property-visit-title");
    dialog.innerHTML = `<div class="tec-visit-dialog-heading"><div><span>Ficha privada de la casa</span><h2 data-visit-title></h2><p data-visit-location></p></div><button type="button" class="nav-button" data-visit-close aria-label="Cerrar seguimiento">Cerrar</button></div>
      <form data-visit-form><label>Resultado de la visita<select name="status" required><option value="">Selecciona qué pasó</option></select></label><label data-visit-date-wrap hidden><span data-visit-date-label>Fecha y hora para volver</span><input name="nextVisitAt" type="datetime-local" /></label><label>Notas de esta casa<textarea name="notes" rows="4" maxlength="2000" placeholder="Ej. Pidieron regresar el sábado después de las 5."></textarea></label><button type="submit" class="primary-button" data-visit-save>Guardar seguimiento</button></form>
      <p class="tec-visit-feedback" data-visit-feedback role="status" aria-live="polite"></p><p class="tec-visit-privacy">El seguimiento se guarda en tu cuenta. Si no hay internet, queda protegido en este teléfono hasta sincronizarse.</p>
      <details class="tec-visit-contact"><summary>Crear lead con datos de contacto</summary><p>Escribe los datos de quien te atendió; puede ser una persona diferente del propietario registrado.</p><form data-visit-lead-form><label>Nombre del contacto<input name="fullName" maxlength="120" autocomplete="name" required /></label><label>Teléfono<input name="phone" type="tel" maxlength="40" autocomplete="tel" /></label><label>Correo (opcional)<input name="email" type="email" maxlength="160" autocomplete="email" /></label><button type="submit" class="nav-button" data-visit-lead-save>Crear lead en TEC</button></form></details>`;
    root.append(dialog);
    dialog.querySelector("[data-visit-title]").id = "tec-property-visit-title";
    const d = selector => dialog.querySelector(`[data-visit-${selector}]`);
    const visitForm = d("form"), leadForm = d("lead-form");
    for (const option of PROPERTY_VISIT_STATUSES) {
      const element = node("option", "", option.label); element.value = option.value; visitForm.elements.status.append(element);
    }
    function visitMessage(text, error = false) { d("feedback").textContent = text; d("feedback").dataset.error = String(error); }
    function updateVisitDate() {
      const timed = ["return_later", "appointment"].includes(visitForm.elements.status.value);
      d("date-wrap").hidden = !timed; visitForm.elements.nextVisitAt.required = timed;
      d("date-label").textContent = visitForm.elements.status.value === "appointment" ? "Fecha y hora de la cita" : "Fecha y hora para volver";
    }
    function syncLeadButton() {
      if (!editingProperty) return;
      const key = propertyVisitKey(editingProperty), linked = visits.get(key)?.linkedLeadId;
      d("lead-save").disabled = Boolean(linked || pendingLeadKeys.has(key));
      d("lead-save").textContent = linked ? "Lead creado en TEC" : pendingLeadKeys.has(key) ? "Lead pendiente de sincronizar" : "Crear lead en TEC";
    }
    function openVisit(property) {
      editingProperty = property;
      awaitingSync = false;
      const record = visits.get(propertyVisitKey(property));
      d("title").textContent = property.address;
      d("location").textContent = `${property.city}, IL ${property.zipCode || ""}`;
      visitForm.elements.status.value = record?.status || "";
      visitForm.elements.notes.value = record?.notes || "";
      if (record?.nextVisitAt) {
        const local = new Date(record.nextVisitAt);
        visitForm.elements.nextVisitAt.value = new Date(local.getTime() - local.getTimezoneOffset() * 60000).toISOString().slice(0, 16);
      } else visitForm.elements.nextVisitAt.value = "";
      leadForm.reset();
      dialog.querySelector("details").open = false;
      updateVisitDate(); syncLeadButton();
      visitMessage(record ? visitLabel(record) : "Marca el resultado y guarda tus notas.");
      if (!dialog.open) dialog.showModal();
    }

    async function saveVisit() {
      if (d("save").disabled) return false;
      if (!visitForm.reportValidity()) return false;
      if (!offline || !accountId || !email) { visitMessage("No pude activar el guardado seguro. Recarga TEC y entra a tu cuenta.", true); return false; }
      const body = {
        accountId, property: editingProperty, status: visitForm.elements.status.value, notes: visitForm.elements.notes.value,
        nextVisitAt: ["return_later", "appointment"].includes(visitForm.elements.status.value) ? new Date(visitForm.elements.nextVisitAt.value).toISOString() : null,
        clientUpdatedAt: new Date().toISOString()
      };
      d("save").disabled = true;
      try {
        const response = await request("/api/coach/property-visits", { method: "POST", body, requestId: uniqueId("property-visit"), offlineQueue: true, queueFirst: true, offlineKind: "property-visit" });
        const record = { ...body, propertyKey: propertyVisitKey(editingProperty), requestId: response.requestId, pending: true, linkedLeadId: visits.get(propertyVisitKey(editingProperty))?.linkedLeadId || "" };
        visits.set(record.propertyKey, record);
        awaitingSync = true;
        await rememberVisits(); refreshVisitDecorations();
        visitMessage(navigator.onLine ? "Seguimiento protegido en este teléfono. Sincronizando con tu cuenta…" : "Seguimiento protegido en este teléfono. Se sincronizará cuando vuelva internet.");
        const key = propertyVisitKey(editingProperty);
        if (routeState.stops.some(stop => propertyVisitKey(stop.property) === key && !stop.completedAt)) {
          const saved = await changeRoute(value => completeRouteStop(value, key, new Date().toISOString()));
          if (!saved) visitMessage("El seguimiento quedó guardado. Falta marcar esta casa como visitada en el recorrido.", true);
        }
        kickSync(); return true;
      } catch (error) { visitMessage(`No pude guardar: ${error.message}. Conserva esta ficha abierta e intenta nuevamente.`, true); return false; }
      finally { d("save").disabled = false; }
    }
    visitForm.addEventListener("submit", async event => { event.preventDefault(); await saveVisit(); });
    visitForm.elements.status.addEventListener("change", updateVisitDate);
    d("close").addEventListener("click", () => dialog.close());
    leadForm.addEventListener("submit", async event => {
      event.preventDefault();
      if (pendingLeadKeys.has(propertyVisitKey(editingProperty)) || visits.get(propertyVisitKey(editingProperty))?.linkedLeadId) return;
      if (!leadForm.elements.phone.value.trim() && !leadForm.elements.email.value.trim()) { visitMessage("Escribe al menos un teléfono o correo del contacto.", true); return; }
      const digits = leadForm.elements.phone.value.replace(/\D/g, "");
      if (leadForm.elements.phone.value.trim() && (digits.length < 10 || digits.length > 15)) { visitMessage("Revisa el teléfono: debe tener de 10 a 15 dígitos.", true); return; }
      const contactEmail = leadForm.elements.email.value.trim();
      if (contactEmail && !/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(contactEmail)) { visitMessage("Revisa el correo del contacto.", true); return; }
      if (d("lead-save").disabled) return;
      d("lead-save").disabled = true;
      if (!(await saveVisit())) { syncLeadButton(); return; }
      try {
        await request("/api/coach/property-visits/lead", {
          method: "POST", requestId: uniqueId("property-lead"), offlineQueue: true, queueFirst: true, offlineKind: "property-lead",
          body: { accountId, property: editingProperty, fullName: leadForm.elements.fullName.value.trim(), phone: leadForm.elements.phone.value.trim(), email: leadForm.elements.email.value.trim(), clientUpdatedAt: new Date().toISOString() }
        });
        pendingLeadKeys.add(propertyVisitKey(editingProperty)); syncLeadButton();
        awaitingSync = true;
        visitMessage(navigator.onLine ? "Lead protegido en este teléfono. Se está enviando a tu cuenta de TEC." : "Lead protegido en este teléfono. Se creará en TEC cuando vuelva internet.");
        kickSync();
      } catch (error) { visitMessage(error.message, true); syncLeadButton(); }
    });
    find("saved").addEventListener("toggle", () => { if (find("saved").open) refreshVisits(); });
    window.addEventListener("tec:offline-status", event => { if (event.detail?.synced) { refreshVisits(); routeStore.refresh(); } });
    window.addEventListener("online", () => { kickSync(); routeStore.sync(); });
    loadLocalVisits().catch(() => null).then(() => { if (!root.hidden) refreshVisits(); });

    function message(text, type = "info") { feedback.textContent = text; feedback.dataset.type = type; }
    function busy(value) {
      loading = value;
      find("submit").disabled = value;
      find("submit").textContent = value ? "Consultando propiedades…" : "Buscar propiedades";
      form.setAttribute("aria-busy", String(value));
      candidatesBox.querySelectorAll("button").forEach(button => { button.disabled = value; });
    }

    function mapProperties() {
      return [...new Map([...(data?.properties || []), ...selected].map(property => [propertyVisitKey(property), property])).values()];
    }
    async function ensureMap() {
      const anchor = data?.anchor || routeState.anchor;
      if (root.hidden || !anchor || (!data && !selected.length)) return;
      results.hidden = false;
      try {
        const L = await loadLeaflet();
        if (!map) {
          map = L.map(mapBox, { scrollWheelZoom: false }).setView([anchor.lat, anchor.lon], 17);
          let failedTiles = 0;
          const tiles = L.tileLayer("https://tile.openstreetmap.org/{z}/{x}/{y}.png", {
            maxZoom: 19, attribution: '&copy; <a href="https://www.openstreetmap.org/copyright" target="_blank" rel="noopener noreferrer">OpenStreetMap</a> contributors'
          });
          tiles.on("tileerror", () => {
            if (++failedTiles >= 3) find("map-note").textContent = "El mapa de calles no pudo cargarse. Puedes seguir usando la lista y abrir las direcciones en Google Maps o Apple Maps.";
          });
          tiles.addTo(map);
          layers = L.layerGroup().addTo(map);
        }
        map.invalidateSize();
        drawMarkers();
        map.fitBounds(L.latLngBounds([anchor, ...mapProperties()].map(item => [item.lat, item.lon])), { padding: [28, 28], maxZoom: 18 });
      } catch (error) { find("map-note").textContent = error.message; }
    }

    function ownerText(property) {
      const names = property.ownerNames?.length ? property.ownerNames : (property.ownerName ? [property.ownerName] : []);
      return names.length ? names.slice(0, 4).join(" · ") : "Propietario no disponible";
    }

    function drawMarkers() {
      if (!map || !window.L) return;
      const L = window.L;
      layers.clearLayers(); markers = new Map();
      if (data) L.circle([data.anchor.lat, data.anchor.lon], { radius: data.radiusMeters, color: "#1c5a8f", weight: 1, fillOpacity: 0.035 }).addTo(layers);
      const anchor = data?.anchor || (selected.length ? routeState.anchor : null);
      if (anchor) L.circleMarker([anchor.lat, anchor.lon], { radius: 11, color: "#fff", weight: 3, fillColor: "#d51e36", fillOpacity: 1 })
        .bindTooltip(data ? "Tu cita" : "Inicio del recorrido", { permanent: true, direction: "top", className: "tec-property-anchor-label" }).addTo(layers);
      for (const property of mapProperties()) {
        const index = selected.findIndex(item => propertyVisitKey(item) === propertyVisitKey(property));
        const visit = visits.get(propertyVisitKey(property));
        const marker = L.circleMarker([property.lat, property.lon], { radius: index >= 0 ? 9 : 6, color: "#fff", weight: 2, fillColor: index >= 0 ? (routeState.stops[index].completedAt ? "#16704b" : "#d51e36") : visitMeta(visit?.status)?.color || "#123d69", fillOpacity: 1 });
        const popup = node("div", "tec-property-popup");
        popup.append(node("strong", "", property.address), node("p", "", ownerText(property)));
        const toggle = node("button", "nav-button", index >= 0 ? "Quitar parada" : "Agregar parada");
        toggle.type = "button";
        toggle.addEventListener("click", () => toggleStop(property));
        const followup = node("button", "nav-button", "Seguimiento"); followup.type = "button"; followup.addEventListener("click", () => openVisit(property));
        if (visit) popup.append(node("p", "", visitLabel(visit)));
        popup.append(toggle, followup);
        marker.bindPopup(popup).addTo(layers);
        if (index >= 0) marker.bindTooltip(String(index + 1), { permanent: true, direction: "center", className: "tec-property-number-label" });
        markers.set(property.pin, marker);
      }
    }

    function syncRoute() {
      const next = nextRouteStop(routeState), completed = routeState.stops.filter(stop => stop.completedAt).length;
      find("count").textContent = `${selected.length} / ${MAX_STOPS}`;
      find("progress").textContent = `${completed} visitadas · ${selected.length - completed} pendientes`;
      find("next").textContent = next ? next.property.formattedAddress : selected.length ? "Recorrido terminado" : "Agrega casas para empezar tu recorrido";
      find("mode").value = routeState.travelMode;
      for (const [link, provider] of [[route, "google"], [appleRoute, "apple"]]) {
        const url = routeNavigationUrl(provider, next, routeState.travelMode);
        if (url) link.href = url; else link.removeAttribute("href");
        link.setAttribute("aria-disabled", String(!url));
      }
      const disabled = routeBusy || !routeStore.isReady();
      find("complete").disabled = disabled || !next;
      find("followup").disabled = !next;
      find("sort").disabled = disabled || selected.length - completed < 2;
      find("mode").disabled = disabled;
      clear.disabled = disabled || !selected.length;
      find("stops").replaceChildren(...routeState.stops.map((stop, index) => {
        const property = stop.property, key = propertyVisitKey(property);
        const li = node("li", stop.completedAt ? "is-completed" : "");
        const title = node("div", "tec-route-stop-title");
        title.append(node("strong", "", `${index + 1}. ${property.address}`), node("span", "", `${property.city}${stop.completedAt ? " · Visitada" : ""}`));
        const actions = node("div", "tec-route-stop-actions");
        const button = (text, label, fn, blocked = false) => {
          const el = node("button", "nav-button", text); el.type = "button"; el.setAttribute("aria-label", label); el.disabled = disabled || blocked; el.addEventListener("click", fn); actions.append(el);
        };
        button(stop.completedAt ? "Deshacer" : "Visitada", `${stop.completedAt ? "Reabrir" : "Marcar visitada"} ${property.address}`, () => changeRoute(value => completeRouteStop(value, key, stop.completedAt ? null : new Date().toISOString())));
        button("Notas", `Seguimiento de ${property.address}`, () => openVisit(property));
        button("↑", `Subir ${property.address}`, () => changeRoute(value => moveRouteStop(value, key, -1)), index === 0);
        button("↓", `Bajar ${property.address}`, () => changeRoute(value => moveRouteStop(value, key, 1)), index === selected.length - 1);
        button("Quitar", `Quitar ${property.address} del recorrido`, () => toggleStop(property));
        li.append(title, actions); return li;
      }));
      for (const card of list.children) {
        const index = selected.findIndex(item => propertyVisitKey(item) === card.dataset.propertyKey);
        const button = card.querySelector("[data-select-stop]");
        if (!button) continue;
        button.textContent = index >= 0 ? `Parada ${index + 1} · Quitar` : "Agregar a mi ruta";
        button.setAttribute("aria-pressed", String(index >= 0));
        button.disabled = disabled || loading || (index < 0 && selected.length >= MAX_STOPS);
        card.classList.toggle("is-selected", index >= 0);
      }
      if (!data && !selected.length) results.hidden = true;
      drawMarkers();
    }
    function toggleStop(property) {
      if (loading) return;
      return changeRoute(value => toggleRouteStop(value, property, data?.anchor));
    }

    function renderList() {
      list.replaceChildren();
      for (const property of data.properties) {
        const card = node("article", "tec-property-card"); card.dataset.pin = property.pin; card.dataset.propertyKey = propertyVisitKey(property);
        const heading = node("div", "tec-property-card-head");
        heading.append(node("h3", "", property.address), node("span", "tec-property-distance", `${property.distanceMeters} m`));
        card.append(heading, node("p", "tec-property-owner-label", "Propietario registrado"), node("p", "tec-property-owner", ownerText(property)), node("p", "tec-property-city", `${property.city}, IL ${property.zipCode}`));
        if (property.parcelCount > 1) card.append(node("span", "tec-property-parcel-note", `${property.parcelCount} parcelas en esta dirección`));
        const actions = node("div", "tec-property-card-actions");
        const select = node("button", "nav-button", "Agregar a mi ruta");
        select.type = "button"; select.dataset.selectStop = "true";
        select.addEventListener("click", () => toggleStop(property));
        const locate = node("button", "tec-property-locate", "Ver en mapa"); locate.type = "button";
        locate.addEventListener("click", () => {
          if (!map) { ensureMap(); return; }
          map.setView([property.lat, property.lon], 18); markers.get(property.pin)?.openPopup();
          mapBox.scrollIntoView({ behavior: "smooth", block: "center" });
        });
        const followup = node("button", "nav-button", "Seguimiento"); followup.type = "button"; followup.addEventListener("click", () => openVisit(property));
        const badge = node("div", "tec-visit-card-details"); badge.dataset.visitBadge = "true";
        actions.append(select, locate, followup); card.append(badge, actions); list.append(card);
      }
      if (!data.properties.length) list.append(node("p", "tec-property-empty", "No encontré propiedades con dirección disponible en este radio. Prueba ampliar la distancia."));
    }

    async function loadNearby(candidate) {
      const payload = await request(`/api/coach/properties/nearby?${new URLSearchParams({ pin: candidate.pin, radius: form.elements.radius.value })}`);
      data = payload; candidatesBox.hidden = true; results.hidden = false;
      const summary = find("summary"); summary.replaceChildren();
      summary.append(node("strong", "", `Tu cita: ${data.anchor.formattedAddress}`), node("span", "", `${data.properties.length} direcciones · radio de ${data.radiusMeters} m · registros ${data.year}`));
      const anchorFollowup = node("button", "nav-button tec-visit-anchor-button", "Seguimiento de esta cita"); anchorFollowup.type = "button"; anchorFollowup.addEventListener("click", () => openVisit(data.anchor)); summary.append(anchorFollowup);
      if (data.truncated) summary.append(node("span", "tec-property-limit-note", "La zona tiene más propiedades. Mostramos una selección limitada; reduce el radio para revisar con más detalle."));
      if (data.unavailableAddresses) summary.append(node("span", "tec-property-limit-note", `${data.unavailableAddresses} parcelas no tienen dirección disponible para mostrarlas.`));
      find("map-note").textContent = "La distancia indicada es en línea recta. El navegador calcula el recorrido por calles.";
      renderList();
      refreshVisitDecorations();
      refreshVisits();
      await ensureMap();
      message(`${data.properties.length} direcciones disponibles. Elige las que quieras visitar.`, "success");
    }

    async function chooseCandidate(candidate) {
      busy(true);
      try { await loadNearby(candidate); }
      catch (error) { message(error.message, "error"); }
      finally { busy(false); syncRoute(); }
    }

    form.addEventListener("submit", async event => {
      event.preventDefault();
      if (loading) return;
      if (!navigator.onLine) { message("Esta búsqueda necesita internet. Vuelve a intentarlo cuando tengas conexión.", "error"); return; }
      busy(true); candidatesBox.hidden = true; syncRoute();
      message("Buscando la dirección y sus propiedades cercanas…");
      try {
        const payload = await request(`/api/coach/properties/search?${new URLSearchParams({ address: form.elements.address.value.trim(), city: form.elements.city.value.trim() })}`);
        if (!payload.candidates.length) {
          message(payload.missingCoordinates ? "Encontré la dirección, pero no tiene coordenadas disponibles. Prueba otra dirección cercana." : "No encontré esa dirección. Revisa número, calle y ciudad. Por ahora cubrimos Cook County, Illinois.", "error");
        } else if (payload.candidates.length === 1) await loadNearby(payload.candidates[0]);
        else {
          candidatesBox.replaceChildren(node("p", "", "Encontré varias propiedades. Selecciona la dirección exacta de tu cita:"));
          for (const candidate of payload.candidates) {
            const button = node("button", "nav-button", candidate.formattedAddress + ` · PIN ${candidate.pin}`);
            button.type = "button"; button.addEventListener("click", () => chooseCandidate(candidate)); candidatesBox.append(button);
          }
          candidatesBox.hidden = false;
          message(payload.truncated ? "Hay más coincidencias. Agrega el número de unidad para precisar la búsqueda." : "Selecciona una coincidencia para ver sus alrededores.");
        }
      } catch (error) { message(error.message, "error"); }
      finally { busy(false); syncRoute(); }
    });
    find("use-appointment").addEventListener("click", () => {
      const appointment = getCurrentAppointment();
      if (!appointment?.address) { message("Primero abre un cliente con dirección, o escribe aquí la dirección de tu cita."); return; }
      form.elements.address.value = appointment.address;
      form.elements.city.value = appointment.city || "";
      message("Dirección copiada. Revisa que la calle y la ciudad estén en sus campos y pulsa Buscar propiedades.");
      form.elements.address.focus();
      addressSuggestions.refresh?.();
    });
    clear.addEventListener("click", () => {
      if (window.confirm("¿Limpiar el recorrido y su avance? Los seguimientos y notas de cada casa se conservan.")) changeRoute(() => emptyPropertyRoute());
    });
    find("sort").addEventListener("click", () => changeRoute(orderRouteByProximity));
    find("mode").addEventListener("change", event => { const mode = event.target.value; changeRoute(value => ({ ...value, travelMode: mode })); });
    find("complete").addEventListener("click", () => { const next = nextRouteStop(routeState); if (next) changeRoute(value => completeRouteStop(value, propertyVisitKey(next.property), new Date().toISOString())); });
    find("followup").addEventListener("click", () => { const next = nextRouteStop(routeState); if (next) openVisit(next.property); });
    find("retry").addEventListener("click", () => routeStore.sync());
    appleRoute.addEventListener("click", event => { if (!appleRoute.hasAttribute("href")) event.preventDefault(); });
    route.addEventListener("click", event => { if (!route.hasAttribute("href")) event.preventDefault(); });
    window.addEventListener("coach:workspace-change", event => {
      if (event.detail?.workspace === "mapa") { requestAnimationFrame(ensureMap); refreshVisits(); routeStore.refresh(); }
      else { addressSuggestions.close(); if (dialog.open) dialog.close(); }
    });
  };
})();
