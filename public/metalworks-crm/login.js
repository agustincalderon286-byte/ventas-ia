const TRANSIENT_STATUS_CODES = new Set([502, 503, 504]);
const GET_RETRY_DELAYS_MS = [450, 1100, 2200];
const CRM_THEME_STORAGE_KEY = "cmwf_crm_theme_v1";

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
}

const loginForm = document.querySelector("[data-crm-login-form]");
const feedback = document.querySelector("[data-crm-login-feedback]");
const aliasBadge = document.querySelector("[data-crm-login-alias]");

const CRM_THEME_PRESETS = {
  "agustincalderon286@gmail.com": {
    displayName: "Agustin",
    skin: "intel-ops",
    themeLabel: "Intel Ops Mode",
  },
  "agustincalderon423@gmail.com": {
    displayName: "Agustin",
    skin: "intel-ops",
    themeLabel: "Intel Ops Mode",
  },
  "calderonrigoberto51@gmail.com": {
    displayName: "Rigo",
    skin: "goku-blue",
    themeLabel: "Rigo // Goku Blue Mode",
  },
};

function setFeedback(message = "", tone = "") {
  if (!feedback) {
    return;
  }

  feedback.textContent = message;
  feedback.dataset.tone = tone;
}

function resolveProfile(email = "") {
  const safeEmail = String(email || "").trim().toLowerCase();
  const preset = CRM_THEME_PRESETS[safeEmail] || {};
  return {
    displayName: preset.displayName || "",
    skin: preset.skin || "classic",
    themeLabel: preset.themeLabel || "",
  };
}

function applyThemePreview(email = "") {
  const profile = resolveProfile(email);
  document.body.dataset.crmSkin = profile.skin || "classic";

  if (!aliasBadge) {
    return;
  }

  aliasBadge.hidden = !profile.themeLabel;
  aliasBadge.textContent = profile.themeLabel || "";
}

function applyCachedTheme() {
  const cached = readStoredJson(CRM_THEME_STORAGE_KEY, null);

  if (!cached?.data?.profile) {
    return false;
  }

  const profile = cached.data.profile;
  document.body.dataset.crmSkin = profile.skin || "classic";

  if (aliasBadge) {
    aliasBadge.hidden = !profile.themeLabel;
    aliasBadge.textContent = profile.themeLabel || "";
  }

  if (loginForm?.elements?.email && cached.data.email) {
    loginForm.elements.email.value = cached.data.email;
  }

  return true;
}

async function init() {
  applyCachedTheme();

  try {
    const me = await apiRequest("/api/metalworks-crm/me");

    if (me.authenticated) {
      window.location.href = "/metalworks-crm/";
      return;
    }

    if (!me.configured) {
      setFeedback(
        "Primero configura METALWORKS_CRM_PASSWORD en el backend para abrir este panel.",
        "error",
      );
      return;
    }

    if (loginForm?.elements?.email && me.allowedEmail) {
      loginForm.elements.email.value = me.allowedEmail;
    }

    applyThemePreview(me.email || me.allowedEmail || "");
  } catch (error) {
    setFeedback(
      TRANSIENT_STATUS_CODES.has(Number(error?.status || 0))
        ? "El CRM se esta despertando. Espera unos segundos y vuelve a intentar."
        : error.message,
      TRANSIENT_STATUS_CODES.has(Number(error?.status || 0)) ? "muted" : "error",
    );
  }
}

if (loginForm) {
  loginForm.elements.email?.addEventListener("input", () => {
    applyThemePreview(loginForm.elements.email.value);
  });

  loginForm.addEventListener("submit", async (event) => {
    event.preventDefault();
    setFeedback("Entrando...", "muted");

    const formData = new FormData(loginForm);

    try {
      const result = await apiRequest("/api/metalworks-crm/login", {
        method: "POST",
        body: {
          email: String(formData.get("email") || "").trim(),
          password: String(formData.get("password") || ""),
        },
      });

      writeStoredJson(CRM_THEME_STORAGE_KEY, {
        savedAt: Date.now(),
        data: {
          email: result.email || String(formData.get("email") || "").trim(),
          profile: result.profile || resolveProfile(formData.get("email") || ""),
        },
      });

      window.location.href = "/metalworks-crm/";
    } catch (error) {
      setFeedback(error.message, "error");
    }
  });
}

init();
