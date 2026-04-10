const TRANSIENT_STATUS_CODES = new Set([502, 503, 504]);
const GET_RETRY_DELAYS_MS = [450, 1100, 2200];
const PROSPECTOR_IDENTITY_STORAGE_KEY = "cmwf_prospector_identity_v1";

function wait(ms) {
  return new Promise((resolve) => {
    window.setTimeout(resolve, ms);
  });
}

function createApiError(message = "", status = 0, retryable = false) {
  const error = new Error(message || "No pude completar esa accion.");
  error.status = status;
  error.retryable = retryable;
  return error;
}

function readStoredJson(key, fallback = null) {
  try {
    const raw = window.localStorage.getItem(key);

    if (!raw) {
      return fallback;
    }

    const parsed = JSON.parse(raw);
    return parsed ?? fallback;
  } catch {
    return fallback;
  }
}

function writeStoredJson(key, value) {
  try {
    if (value == null) {
      window.localStorage.removeItem(key);
      return;
    }

    window.localStorage.setItem(key, JSON.stringify(value));
  } catch {}
}

async function apiRequest(url, options = {}) {
  const config = {
    method: String(options.method || "GET").toUpperCase(),
    credentials: "same-origin",
    headers: {
      ...(options.body ? { "Content-Type": "application/json" } : {}),
      ...(options.headers || {}),
    },
  };

  if (options.body) {
    config.body = JSON.stringify(options.body);
  }

  const retryDelays =
    config.method === "GET"
      ? GET_RETRY_DELAYS_MS
      : [];

  for (let attempt = 0; attempt <= retryDelays.length; attempt += 1) {
    try {
      const response = await fetch(url, config);
      const contentType = String(response.headers.get("content-type") || "").toLowerCase();
      const data = contentType.includes("application/json")
        ? await response.json().catch(() => ({}))
        : {};

      if (!response.ok) {
        throw createApiError(
          data.error || "No pude completar esa accion.",
          response.status,
          TRANSIENT_STATUS_CODES.has(response.status),
        );
      }

      return data;
    } catch (error) {
      const status = Number(error?.status || 0);
      const retryable =
        Boolean(error?.retryable) ||
        TRANSIENT_STATUS_CODES.has(status) ||
        error instanceof TypeError;

      if (attempt < retryDelays.length && retryable) {
        await wait(retryDelays[attempt]);
        continue;
      }

      if (error instanceof Error) {
        throw error;
      }

      throw createApiError("No pude completar esa accion.", status, retryable);
    }
  }

  throw createApiError("No pude completar esa accion.");
}

const loginForm = document.querySelector("[data-prospector-login-form]");
const feedback = document.querySelector("[data-prospector-login-feedback]");

function setFeedback(message = "", tone = "") {
  if (!feedback) {
    return;
  }

  feedback.textContent = message;
  feedback.dataset.tone = tone;
}

function hydrateStoredIdentity() {
  if (!loginForm) {
    return;
  }

  const stored = readStoredJson(PROSPECTOR_IDENTITY_STORAGE_KEY, null);

  if (!stored?.name && !stored?.email) {
    return;
  }

  if (loginForm.elements.name && stored.name) {
    loginForm.elements.name.value = stored.name;
  }

  if (loginForm.elements.email && stored.email) {
    loginForm.elements.email.value = stored.email;
  }
}

async function init() {
  hydrateStoredIdentity();

  try {
    const me = await apiRequest("/api/metalworks-crm/prospector/me");

    if (me.authenticated) {
      window.location.href = "/metalworks-crm/prospector/";
      return;
    }

    if (!me.configured) {
      setFeedback(
        "Primero configura METALWORKS_PROSPECTOR_PASSWORD en el backend para abrir este portal.",
        "error",
      );
    }
  } catch (error) {
    setFeedback(
      TRANSIENT_STATUS_CODES.has(Number(error?.status || 0))
        ? "El portal se esta despertando. Espera unos segundos y vuelve a intentar."
        : error.message,
      TRANSIENT_STATUS_CODES.has(Number(error?.status || 0)) ? "muted" : "error",
    );
  }
}

if (loginForm) {
  loginForm.addEventListener("submit", async (event) => {
    event.preventDefault();
    setFeedback("Entrando...", "muted");

    const formData = new FormData(loginForm);
    const payload = {
      name: String(formData.get("name") || "").trim(),
      email: String(formData.get("email") || "").trim(),
      password: String(formData.get("password") || ""),
    };

    try {
      const result = await apiRequest("/api/metalworks-crm/prospector/login", {
        method: "POST",
        body: payload,
      });

      writeStoredJson(PROSPECTOR_IDENTITY_STORAGE_KEY, {
        name: result.name || payload.name,
        email: result.email || payload.email,
      });

      window.location.href = "/metalworks-crm/prospector/";
    } catch (error) {
      setFeedback(error.message, "error");
    }
  });
}

init();
