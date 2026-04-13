const TRANSIENT_STATUS_CODES = new Set([502, 503, 504]);
const GET_RETRY_DELAYS_MS = [450, 1100, 2200];
const PROSPECTOR_AUTH_STORAGE_KEY = "cmwf_prospector_auth_v1";

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

  const retryDelays = config.method === "GET" ? GET_RETRY_DELAYS_MS : [];

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

function writeCachedProspectorAuth(auth = null) {
  try {
    if (!auth?.email) {
      window.localStorage.removeItem(PROSPECTOR_AUTH_STORAGE_KEY);
      return;
    }

    window.localStorage.setItem(
      PROSPECTOR_AUTH_STORAGE_KEY,
      JSON.stringify({
        name: String(auth.name || "").trim(),
        email: String(auth.email || "").trim().toLowerCase(),
        savedAt: Date.now(),
      }),
    );
  } catch {}
}

async function init() {
  try {
    const me = await apiRequest("/api/metalworks-crm/prospector/me");

    if (me.authenticated) {
      window.location.href = "/metalworks-crm/prospector/";
      return;
    }

    if (!me.configured) {
      setFeedback(
        "Ask your admin to create your prospector account before signing in here.",
        "error",
      );
    }
  } catch (error) {
    setFeedback(
      TRANSIENT_STATUS_CODES.has(Number(error?.status || 0))
        ? "The portal is waking up. Give it a few seconds and try again."
        : error.message,
      TRANSIENT_STATUS_CODES.has(Number(error?.status || 0)) ? "muted" : "error",
    );
  }
}

if (loginForm) {
  loginForm.addEventListener("submit", async (event) => {
    event.preventDefault();
    setFeedback("Signing in...", "muted");

    const formData = new FormData(loginForm);
    const payload = {
      email: String(formData.get("email") || "").trim(),
      password: String(formData.get("password") || ""),
    };

    try {
      const result = await apiRequest("/api/metalworks-crm/prospector/login", {
        method: "POST",
        body: payload,
      });
      writeCachedProspectorAuth(result);

      window.location.href = "/metalworks-crm/prospector/";
    } catch (error) {
      setFeedback(error.message, "error");
    }
  });
}

init();
