const CHAT_API_URL = "/chat";
const CHEF_STATS_API_URL = "/api/chef/stats";
const PLATFORM_CONFIG_API_URL = "/api/platform/config";
const VISITOR_KEY = "agustin-chef-visitor-id";
const SESSION_KEY = "agustin-chef-session-id";

const input = document.getElementById("chat-input");
const button = document.getElementById("chat-btn");
const mensajesDiv = document.getElementById("chat-mensajes");
const typing = document.getElementById("typing-indicator");
const statusBadge = document.getElementById("chefStatusBadge");
const promptButtons = document.querySelectorAll("[data-chef-prompt]");
const installButtons = document.querySelectorAll("[data-chef-install]");
const installHintNodes = document.querySelectorAll("[data-chef-install-hint]");
const chefCalendlyButtons = document.querySelectorAll("[data-open-chef-calendly]");
const chefCalendlyCard = document.querySelector("[data-chef-calendly-card]");
const chefCalendlyModal = document.querySelector("[data-chef-calendly-modal]");
const chefCalendlyCloseButtons = document.querySelectorAll("[data-close-chef-calendly]");
const chefCalendlyFrame = document.querySelector("[data-chef-calendly-frame]");
const chefCalendlyOpenLink = document.querySelector("[data-chef-calendly-open-link]");
let deferredInstallPrompt = null;
let chefCalendlyUrl = "";

function crearId(prefijo) {
  if (window.crypto && typeof window.crypto.randomUUID === "function") {
    return `${prefijo}-${window.crypto.randomUUID()}`;
  }

  return `${prefijo}-${Date.now()}-${Math.random().toString(36).slice(2, 10)}`;
}

function obtenerVisitorId() {
  try {
    let id = localStorage.getItem(VISITOR_KEY);

    if (!id) {
      id = crearId("chef-visitor");
      localStorage.setItem(VISITOR_KEY, id);
    }

    return id;
  } catch (error) {
    return crearId("chef-visitor");
  }
}

function obtenerSessionId() {
  try {
    let id = sessionStorage.getItem(SESSION_KEY);

    if (!id) {
      id = crearId("chef-session");
      sessionStorage.setItem(SESSION_KEY, id);
    }

    return id;
  } catch (error) {
    return crearId("chef-session");
  }
}

const visitorId = obtenerVisitorId();
const sessionId = obtenerSessionId();

function autoResize() {
  if (!input) {
    return;
  }

  input.style.height = "auto";
  input.style.height = `${Math.min(input.scrollHeight, 180)}px`;
}

function setLoading(loading) {
  if (typing) {
    typing.style.display = loading ? "block" : "none";
  }

  if (button) {
    button.disabled = loading;
  }

  if (input) {
    input.disabled = loading;
  }

  if (statusBadge) {
    statusBadge.textContent = loading ? "Pensando..." : "Listo";
  }
}

function setInstallHint(message) {
  installHintNodes.forEach(node => {
    node.textContent = message;
  });
}

function isIosDevice() {
  const ua = window.navigator.userAgent || "";
  return /iPad|iPhone|iPod/.test(ua) || (window.navigator.platform === "MacIntel" && window.navigator.maxTouchPoints > 1);
}

function isStandaloneMode() {
  return window.matchMedia("(display-mode: standalone)").matches || window.navigator.standalone === true;
}

function syncInstallButtons() {
  const isInstalled = isStandaloneMode();
  const canPrompt = Boolean(deferredInstallPrompt);
  const isIos = isIosDevice();

  installButtons.forEach(buttonNode => {
    if (!buttonNode.dataset.defaultLabel) {
      buttonNode.dataset.defaultLabel = buttonNode.textContent;
    }

    if (isInstalled) {
      buttonNode.textContent = "Ya esta guardado";
      buttonNode.disabled = true;
      return;
    }

    buttonNode.disabled = false;
    buttonNode.textContent = canPrompt
      ? "Guardar en tu telefono"
      : isIos
        ? "Como guardarlo"
        : buttonNode.dataset.defaultLabel || "Guardar en tu telefono";
  });

  if (isInstalled) {
    setInstallHint("Ya quedo guardado en tu telefono. Lo puedes abrir directo desde tu pantalla.");
  } else if (canPrompt) {
    setInstallHint("Toca el boton y, si tu navegador lo permite, te saldra la opcion de instalarlo o guardarlo.");
  } else if (isIos) {
    setInstallHint("En iPhone abre esta pagina en Safari, toca Compartir y luego Agregar a pantalla de inicio.");
  } else {
    setInstallHint("Abre el Chef desde tu navegador y guardalo en favoritos o en tu pantalla si tu dispositivo lo permite.");
  }
}

function agregarMensaje(texto, clase) {
  if (!mensajesDiv) {
    return;
  }

  const bubble = document.createElement("div");
  bubble.className = clase;
  bubble.appendChild(construirFragmentoMensaje(texto));
  mensajesDiv.appendChild(bubble);
  mensajesDiv.scrollTop = mensajesDiv.scrollHeight;
}

function construirFragmentoMensaje(texto) {
  const fragment = document.createDocumentFragment();
  const source = String(texto || "");
  const urlRegex = /(https?:\/\/[^\s]+)/gi;
  let lastIndex = 0;
  let match = null;

  while ((match = urlRegex.exec(source))) {
    agregarTextoConSaltos(fragment, source.slice(lastIndex, match.index));
    fragment.appendChild(crearLinkMensaje(match[0]));
    lastIndex = match.index + match[0].length;
  }

  agregarTextoConSaltos(fragment, source.slice(lastIndex));
  return fragment;
}

function agregarTextoConSaltos(fragment, text) {
  const parts = String(text || "").split("\n");

  parts.forEach((part, index) => {
    if (part) {
      fragment.appendChild(document.createTextNode(part));
    }

    if (index < parts.length - 1) {
      fragment.appendChild(document.createElement("br"));
    }
  });
}

function crearLinkMensaje(url) {
  const link = document.createElement("a");
  const safeUrl = String(url || "").trim();

  link.href = safeUrl;
  link.target = "_blank";
  link.rel = "noreferrer";
  link.className = "msg-action-link";

  if (/wa\.me|whatsapp/i.test(safeUrl)) {
    link.textContent = "Abrir WhatsApp";
    return link;
  }

  if (/calendly/i.test(safeUrl)) {
    link.textContent = "Abrir agenda";
    return link;
  }

  link.textContent = safeUrl;
  return link;
}

function formatNumber(value) {
  return new Intl.NumberFormat("es-US").format(Number(value || 0));
}

function formatTimestamp(value) {
  if (!value) {
    return "sin actualizar";
  }

  const date = new Date(value);

  if (Number.isNaN(date.getTime())) {
    return "sin actualizar";
  }

  return new Intl.DateTimeFormat("es-US", {
    month: "short",
    day: "numeric",
    hour: "numeric",
    minute: "2-digit"
  }).format(date);
}

function formatTopicLabel(value) {
  return String(value || "")
    .replace(/_/g, " ")
    .replace(/\s+/g, " ")
    .trim()
    .replace(/\b\w/g, letter => letter.toUpperCase());
}

function renderTopics(targets, items) {
  targets.forEach(target => {
    if (!target) {
      return;
    }

    target.innerHTML = "";
    const values = Array.isArray(items) ? items.filter(Boolean) : [];

    if (!values.length) {
      const chip = document.createElement("span");
      chip.className = "tag-chip";
      chip.textContent = "Aun no hay temas dominantes";
      target.appendChild(chip);
      return;
    }

    values.forEach(item => {
      const chip = document.createElement("span");
      chip.className = "tag-chip";
      chip.textContent = formatTopicLabel(item);
      target.appendChild(chip);
    });
  });
}

function renderChefStats(stats = {}) {
  document.querySelectorAll("[data-chef-stat-families], [data-chef-stat-family-hero]").forEach(node => {
    node.textContent = formatNumber(stats.familiasGuiadas || 0);
  });

  document.querySelectorAll("[data-chef-stat-today], [data-chef-stat-today-hero], [data-chef-stat-today-side]").forEach(
    node => {
      node.textContent = formatNumber(stats.activosHoy || 0);
    }
  );

  document.querySelectorAll("[data-chef-stat-week], [data-chef-stat-week-side]").forEach(node => {
    node.textContent = formatNumber(stats.activos7Dias || 0);
  });

  document
    .querySelectorAll("[data-chef-stat-questions], [data-chef-stat-questions-hero]")
    .forEach(node => {
      node.textContent = formatNumber(stats.preguntasTotales || 0);
    });

  document.querySelectorAll("[data-chef-stat-updated]").forEach(node => {
    node.textContent = formatTimestamp(stats.updatedAt);
  });

  document.querySelectorAll("[data-chef-stat-questions-side]").forEach(node => {
    node.textContent = formatNumber(stats.preguntasTotales || 0);
  });

  renderTopics(
    [
      document.querySelector("[data-chef-top-topics]"),
      document.querySelector("[data-chef-side-topics]")
    ],
    stats.topTopics
  );
}

function buildCalendlyEmbedUrl(url) {
  try {
    const calendlyUrl = new URL(url);
    calendlyUrl.searchParams.set("hide_gdpr_banner", "1");
    calendlyUrl.searchParams.set("hide_event_type_details", "1");
    return calendlyUrl.toString();
  } catch (error) {
    return url;
  }
}

function applyChefCalendlyConfig(config = {}) {
  chefCalendlyUrl = String(config.calendly?.chefUrl || "").trim();
  const enabled = Boolean(config.calendly?.chefEnabled && chefCalendlyUrl);

  chefCalendlyButtons.forEach(buttonNode => {
    buttonNode.hidden = !enabled;
  });

  if (chefCalendlyCard) {
    chefCalendlyCard.hidden = !enabled;
  }

  if (!enabled) {
    return;
  }

  if (chefCalendlyFrame) {
    chefCalendlyFrame.src = buildCalendlyEmbedUrl(chefCalendlyUrl);
  }

  if (chefCalendlyOpenLink) {
    chefCalendlyOpenLink.href = chefCalendlyUrl;
  }
}

function openChefCalendlyModal() {
  if (!chefCalendlyModal || !chefCalendlyUrl) {
    return;
  }

  chefCalendlyModal.hidden = false;
}

function closeChefCalendlyModal() {
  if (!chefCalendlyModal) {
    return;
  }

  chefCalendlyModal.hidden = true;
}

async function loadPlatformConfig() {
  try {
    const response = await fetch(PLATFORM_CONFIG_API_URL, {
      method: "GET"
    });
    const data = await response.json().catch(() => ({}));

    if (!response.ok) {
      throw new Error(data.error || "No pude cargar la configuracion.");
    }

    applyChefCalendlyConfig(data);
  } catch (error) {
    applyChefCalendlyConfig();
  }
}

async function loadChefStats() {
  try {
    const response = await fetch(CHEF_STATS_API_URL, {
      method: "GET"
    });
    const data = await response.json().catch(() => ({}));

    if (!response.ok) {
      throw new Error(data.error || "No pude cargar las metricas.");
    }

    renderChefStats(data);
  } catch (error) {
    renderChefStats();
  }
}

async function enviarPregunta(forcedText = "") {
  const pregunta = String(forcedText || input?.value || "").trim();

  if (!pregunta) {
    return;
  }

  agregarMensaje(pregunta, "msg-user");

  if (input) {
    input.value = "";
    autoResize();
  }

  setLoading(true);

  try {
    const res = await fetch(CHAT_API_URL, {
      method: "POST",
      headers: {
        "Content-Type": "application/json"
      },
      body: JSON.stringify({
        pregunta,
        sessionId,
        visitorId,
        mode: "chef"
      })
    });

    const data = await res.json().catch(() => ({}));
    setLoading(false);

    if (!res.ok) {
      agregarMensaje(data.error || "No pude responder en este momento.", "msg-ai");
      return;
    }

    agregarMensaje(data.respuesta || "No pude responder en este momento.", "msg-ai");
    await loadChefStats();
  } catch (error) {
    setLoading(false);
    agregarMensaje("Error conectando con servidor", "msg-ai");
  } finally {
    if (input) {
      input.disabled = false;
      input.focus();
    }
    if (button) {
      button.disabled = false;
    }
  }
}

async function handleInstallClick() {
  if (isStandaloneMode()) {
    setInstallHint("Ya lo tienes guardado en tu telefono.");
    return;
  }

  if (deferredInstallPrompt) {
    const promptEvent = deferredInstallPrompt;
    deferredInstallPrompt = null;
    promptEvent.prompt();

    try {
      await promptEvent.userChoice;
    } catch (error) {
      // noop
    }

    syncInstallButtons();
    return;
  }

  if (isIosDevice()) {
    setInstallHint("En iPhone abre esta pagina en Safari, toca Compartir y luego Agregar a pantalla de inicio.");
    return;
  }

  setInstallHint("Tu navegador no mostro instalacion directa. Guarda esta pagina en tu pantalla o favoritos para volver rapido.");
}

async function registerChefServiceWorker() {
  if (!("serviceWorker" in navigator)) {
    return;
  }

  try {
    await navigator.serviceWorker.register("/chef/sw.js", {
      scope: "/chef/"
    });
  } catch (error) {
    // noop
  }
}

window.addEventListener("beforeinstallprompt", event => {
  event.preventDefault();
  deferredInstallPrompt = event;
  syncInstallButtons();
});

window.addEventListener("appinstalled", () => {
  deferredInstallPrompt = null;
  syncInstallButtons();
  setInstallHint("Listo. Agustin 2.0 Chef ya quedo guardado en tu telefono.");
});

button?.addEventListener("click", () => {
  enviarPregunta();
});

input?.addEventListener("keydown", event => {
  if (event.key === "Enter" && !event.shiftKey) {
    event.preventDefault();
    enviarPregunta();
  }
});

input?.addEventListener("input", autoResize);

promptButtons.forEach(buttonNode => {
  buttonNode.addEventListener("click", () => {
    enviarPregunta(buttonNode.textContent || "");
  });
});

installButtons.forEach(buttonNode => {
  buttonNode.addEventListener("click", () => {
    handleInstallClick();
  });
});

chefCalendlyButtons.forEach(buttonNode => {
  buttonNode.addEventListener("click", () => {
    openChefCalendlyModal();
  });
});

chefCalendlyCloseButtons.forEach(buttonNode => {
  buttonNode.addEventListener("click", () => {
    closeChefCalendlyModal();
  });
});

chefCalendlyModal?.addEventListener("click", event => {
  if (event.target === chefCalendlyModal) {
    closeChefCalendlyModal();
  }
});

document.addEventListener("keydown", event => {
  if (event.key === "Escape" && chefCalendlyModal && !chefCalendlyModal.hidden) {
    closeChefCalendlyModal();
  }
});

document.addEventListener("DOMContentLoaded", async () => {
  autoResize();
  await registerChefServiceWorker();
  await loadPlatformConfig();
  syncInstallButtons();
  await loadChefStats();
});
