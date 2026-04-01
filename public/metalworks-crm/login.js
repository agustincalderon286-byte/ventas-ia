async function apiRequest(url, options = {}) {
  const config = {
    method: options.method || "GET",
    credentials: "same-origin",
    headers: {
      ...(options.body ? { "Content-Type": "application/json" } : {}),
      ...(options.headers || {}),
    },
  };

  if (options.body) {
    config.body = JSON.stringify(options.body);
  }

  const response = await fetch(url, config);
  const data = await response.json().catch(() => ({}));

  if (!response.ok) {
    throw new Error(data.error || "No pude completar esa accion.");
  }

  return data;
}

const loginForm = document.querySelector("[data-crm-login-form]");
const feedback = document.querySelector("[data-crm-login-feedback]");

function setFeedback(message = "", tone = "") {
  if (!feedback) {
    return;
  }

  feedback.textContent = message;
  feedback.dataset.tone = tone;
}

async function init() {
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
  } catch (error) {
    setFeedback(error.message, "error");
  }
}

if (loginForm) {
  loginForm.addEventListener("submit", async (event) => {
    event.preventDefault();
    setFeedback("Entrando...", "muted");

    const formData = new FormData(loginForm);

    try {
      await apiRequest("/api/metalworks-crm/login", {
        method: "POST",
        body: {
          email: String(formData.get("email") || "").trim(),
          password: String(formData.get("password") || ""),
        },
      });

      window.location.href = "/metalworks-crm/";
    } catch (error) {
      setFeedback(error.message, "error");
    }
  });
}

init();
