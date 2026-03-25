const CHAT_API_URL = "/chat";
const CHEF_STATS_API_URL = "/api/chef/stats";
const VISITOR_KEY = "agustin-chef-visitor-id";
const SESSION_KEY = "agustin-chef-session-id";

const input = document.getElementById("chat-input");
const button = document.getElementById("chat-btn");
const mensajesDiv = document.getElementById("chat-mensajes");
const typing = document.getElementById("typing-indicator");
const statusBadge = document.getElementById("chefStatusBadge");
const promptButtons = document.querySelectorAll("[data-chef-prompt]");

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

function agregarMensaje(texto, clase) {
  if (!mensajesDiv) {
    return;
  }

  const bubble = document.createElement("div");
  bubble.className = clase;
  bubble.textContent = texto;
  mensajesDiv.appendChild(bubble);
  mensajesDiv.scrollTop = mensajesDiv.scrollHeight;
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

  document
    .querySelectorAll("[data-chef-stat-interest], [data-chef-stat-interest-side]")
    .forEach(node => {
      node.textContent = formatNumber(stats.interesDetectado || 0);
    });

  document.querySelectorAll("[data-chef-stat-updated]").forEach(node => {
    node.textContent = formatTimestamp(stats.updatedAt);
  });

  renderTopics(
    [
      document.querySelector("[data-chef-top-topics]"),
      document.querySelector("[data-chef-side-topics]")
    ],
    stats.topTopics
  );
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

document.addEventListener("DOMContentLoaded", async () => {
  autoResize();
  await loadChefStats();
});
