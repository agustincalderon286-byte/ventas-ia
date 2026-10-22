(() => {
  const MAX_STOPS = 4;
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

  function buildRouteUrl(anchor, stops) {
    if (!anchor || !stops.length) return "";
    const url = new URL("https://www.google.com/maps/dir/");
    url.searchParams.set("api", "1");
    url.searchParams.set("origin", anchor.formattedAddress);
    url.searchParams.set("destination", stops.at(-1).formattedAddress);
    url.searchParams.set("travelmode", "walking");
    if (stops.length > 1) url.searchParams.set("waypoints", stops.slice(0, -1).map(item => item.formattedAddress).join("|"));
    return url.href;
  }

  window.initTecPropertyMap = function ({ request, getCurrentAppointment = () => null }) {
    const root = document.querySelector("[data-property-workspace]");
    if (!root || root.dataset.initialized) return;
    root.dataset.initialized = "true";
    const find = selector => root.querySelector(`[data-property-${selector}]`);
    const form = find("search"), feedback = find("feedback"), candidatesBox = find("candidates"), results = find("results");
    const mapBox = find("map"), list = find("list"), route = find("route"), clear = find("clear");
    let data = null, selected = [], markers = new Map(), map = null, layers = null, loading = false;

    function message(text, type = "info") { feedback.textContent = text; feedback.dataset.type = type; }
    function busy(value) {
      loading = value;
      find("submit").disabled = value;
      find("submit").textContent = value ? "Consultando propiedades…" : "Buscar propiedades";
      form.setAttribute("aria-busy", String(value));
      candidatesBox.querySelectorAll("button").forEach(button => { button.disabled = value; });
    }

    async function ensureMap() {
      if (root.hidden || !data) return;
      try {
        const L = await loadLeaflet();
        if (!map) {
          map = L.map(mapBox, { scrollWheelZoom: false }).setView([data.anchor.lat, data.anchor.lon], 17);
          let failedTiles = 0;
          const tiles = L.tileLayer("https://tile.openstreetmap.org/{z}/{x}/{y}.png", {
            maxZoom: 19, attribution: '&copy; <a href="https://www.openstreetmap.org/copyright" target="_blank" rel="noopener noreferrer">OpenStreetMap</a> contributors'
          });
          tiles.on("tileerror", () => {
            if (++failedTiles >= 3) find("map-note").textContent = "El mapa de calles no pudo cargarse. Puedes seguir usando la lista y abrir la ruta en Google Maps.";
          });
          tiles.addTo(map);
          layers = L.layerGroup().addTo(map);
        }
        map.invalidateSize();
        drawMarkers();
        map.fitBounds(L.latLngBounds([data.anchor, ...data.properties].map(item => [item.lat, item.lon])), { padding: [28, 28], maxZoom: 18 });
      } catch (error) { find("map-note").textContent = error.message; }
    }

    function ownerText(property) {
      const names = property.ownerNames?.length ? property.ownerNames : (property.ownerName ? [property.ownerName] : []);
      return names.length ? names.slice(0, 4).join(" · ") : "Propietario no disponible";
    }

    function drawMarkers() {
      if (!map || !data || !window.L) return;
      const L = window.L;
      layers.clearLayers(); markers = new Map();
      L.circle([data.anchor.lat, data.anchor.lon], { radius: data.radiusMeters, color: "#1c5a8f", weight: 1, fillOpacity: 0.035 }).addTo(layers);
      L.circleMarker([data.anchor.lat, data.anchor.lon], { radius: 11, color: "#fff", weight: 3, fillColor: "#d51e36", fillOpacity: 1 })
        .bindTooltip("Tu cita", { permanent: true, direction: "top", className: "tec-property-anchor-label" }).addTo(layers);
      for (const property of data.properties) {
        const index = selected.findIndex(item => item.pin === property.pin);
        const marker = L.circleMarker([property.lat, property.lon], { radius: index >= 0 ? 9 : 6, color: "#fff", weight: 2, fillColor: index >= 0 ? "#d51e36" : "#123d69", fillOpacity: 1 });
        const popup = node("div", "tec-property-popup");
        popup.append(node("strong", "", property.address), node("p", "", ownerText(property)));
        const toggle = node("button", "nav-button", index >= 0 ? "Quitar parada" : "Agregar parada");
        toggle.type = "button";
        toggle.addEventListener("click", () => toggleStop(property));
        popup.append(toggle);
        marker.bindPopup(popup).addTo(layers);
        if (index >= 0) marker.bindTooltip(String(index + 1), { permanent: true, direction: "center", className: "tec-property-number-label" });
        markers.set(property.pin, marker);
      }
    }

    function syncRoute() {
      find("count").textContent = `${selected.length} / ${MAX_STOPS}`;
      find("stops").replaceChildren(...selected.map(property => {
        const li = node("li", "", property.address);
        const remove = node("button", "tec-property-remove", "Quitar");
        remove.type = "button";
        remove.setAttribute("aria-label", `Quitar ${property.address} del recorrido`);
        remove.addEventListener("click", () => toggleStop(property));
        li.append(remove); return li;
      }));
      const url = buildRouteUrl(data?.anchor, selected);
      if (url) route.href = url; else route.removeAttribute("href");
      route.setAttribute("aria-disabled", String(!url));
      clear.disabled = !selected.length;
      for (const card of list.children) {
        const index = selected.findIndex(item => item.pin === card.dataset.pin);
        const button = card.querySelector("[data-select-stop]");
        button.textContent = index >= 0 ? `Parada ${index + 1} · Quitar` : "Agregar a mi ruta";
        button.setAttribute("aria-pressed", String(index >= 0));
        button.disabled = loading || (index < 0 && selected.length >= MAX_STOPS);
        card.classList.toggle("is-selected", index >= 0);
      }
      drawMarkers();
    }

    function toggleStop(property) {
      if (loading) return;
      const index = selected.findIndex(item => item.pin === property.pin);
      if (index >= 0) selected.splice(index, 1);
      else if (selected.length < MAX_STOPS) selected.push(property);
      else { message("La ruta admite hasta 4 paradas. Quita una para agregar otra."); return; }
      syncRoute();
    }

    function renderList() {
      list.replaceChildren();
      for (const property of data.properties) {
        const card = node("article", "tec-property-card"); card.dataset.pin = property.pin;
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
        actions.append(select, locate); card.append(actions); list.append(card);
      }
      if (!data.properties.length) list.append(node("p", "tec-property-empty", "No encontré propiedades con dirección disponible en este radio. Prueba ampliar la distancia."));
    }

    async function loadNearby(candidate) {
      const payload = await request(`/api/coach/properties/nearby?${new URLSearchParams({ pin: candidate.pin, radius: form.elements.radius.value })}`);
      data = payload; selected = []; candidatesBox.hidden = true; results.hidden = false;
      const summary = find("summary"); summary.replaceChildren();
      summary.append(node("strong", "", `Tu cita: ${data.anchor.formattedAddress}`), node("span", "", `${data.properties.length} direcciones · radio de ${data.radiusMeters} m · registros ${data.year}`));
      if (data.truncated) summary.append(node("span", "tec-property-limit-note", "La zona tiene más propiedades. Mostramos una selección limitada; reduce el radio para revisar con más detalle."));
      if (data.unavailableAddresses) summary.append(node("span", "tec-property-limit-note", `${data.unavailableAddresses} parcelas no tienen dirección disponible para mostrarlas.`));
      find("map-note").textContent = "La distancia indicada es en línea recta. El navegador calcula el recorrido por calles.";
      renderList();
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
      busy(true); results.hidden = true; candidatesBox.hidden = true; data = null; selected = []; syncRoute();
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
      if (appointment.city) form.elements.city.value = appointment.city;
      message("Dirección copiada. Revisa que la calle y la ciudad estén en sus campos y pulsa Buscar propiedades.");
      form.elements.address.focus();
    });
    clear.addEventListener("click", () => { selected = []; syncRoute(); });
    route.addEventListener("click", event => { if (!route.hasAttribute("href")) event.preventDefault(); });
    window.addEventListener("coach:workspace-change", event => { if (event.detail?.workspace === "mapa") requestAnimationFrame(ensureMap); });
  };
})();
