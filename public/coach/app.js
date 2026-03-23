async function apiRequest(url, options = {}) {
  const config = {
    method: options.method || "GET",
    credentials: "same-origin",
    headers: {
      ...(options.body ? { "Content-Type": "application/json" } : {}),
      ...(options.headers || {})
    }
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

function setMessage(target, message, state = "info") {
  if (!target) {
    return;
  }

  target.textContent = message;
  target.dataset.state = state;
  target.classList.add("is-visible");
}

function clearMessage(target) {
  if (!target) {
    return;
  }

  target.textContent = "";
  target.dataset.state = "info";
  target.classList.remove("is-visible");
}

function setButtonLoading(button, loading, label = "Procesando...") {
  if (!button) {
    return;
  }

  if (!button.dataset.defaultLabel) {
    button.dataset.defaultLabel = button.textContent;
  }

  button.disabled = loading;
  button.textContent = loading ? label : button.dataset.defaultLabel;
}

function formatDate(dateString) {
  if (!dateString) {
    return "Sin fecha";
  }

  const date = new Date(dateString);

  if (Number.isNaN(date.getTime())) {
    return "Sin fecha";
  }

  return new Intl.DateTimeFormat("es-US", {
    year: "numeric",
    month: "long",
    day: "numeric"
  }).format(date);
}

async function fetchViewer() {
  try {
    return await apiRequest("/api/coach/me");
  } catch (error) {
    return { authenticated: false, error: error.message };
  }
}

function updateAuthTargets(user) {
  document.querySelectorAll("[data-coach-user-name]").forEach(node => {
    node.textContent = user?.name || "Distribuidor";
  });

  document.querySelectorAll("[data-coach-user-email]").forEach(node => {
    node.textContent = user?.email || "Sin correo";
  });

  document.querySelectorAll("[data-coach-subscription-status]").forEach(node => {
    node.textContent = user?.subscriptionActive
      ? "Activa"
      : user?.subscriptionStatus === "past_due"
        ? "Pago pendiente"
        : "Sin activar";
  });

  document.querySelectorAll("[data-coach-subscription-period]").forEach(node => {
    node.textContent = user?.subscriptionCurrentPeriodEnd
      ? formatDate(user.subscriptionCurrentPeriodEnd)
      : "Todavia no disponible";
  });
}

function initFaq() {
  document.querySelectorAll(".faq-card").forEach(card => {
    const trigger = card.querySelector(".faq-trigger");

    if (!trigger) {
      return;
    }

    trigger.addEventListener("click", () => {
      card.classList.toggle("is-open");
    });
  });
}

async function initPlanPage() {
  const planPage = document.querySelector("[data-coach-plan-page]");

  if (!planPage) {
    return;
  }

  const signupSection = document.querySelector("[data-signup-section]");
  const memberSection = document.querySelector("[data-member-section]");
  const activeSection = document.querySelector("[data-active-section]");
  const signupForm = document.querySelector("[data-signup-checkout-form]");
  const signupMessage = signupForm?.querySelector(".form-result");
  const startCheckoutButton = document.querySelector("[data-start-checkout]");
  const portalButton = document.querySelector("[data-open-billing-portal]");

  const me = await fetchViewer();
  const user = me.user || null;
  updateAuthTargets(user);

  if (me.authenticated && user?.subscriptionActive) {
    signupSection?.setAttribute("hidden", "hidden");
    memberSection?.setAttribute("hidden", "hidden");
    activeSection?.removeAttribute("hidden");
  } else if (me.authenticated) {
    signupSection?.setAttribute("hidden", "hidden");
    activeSection?.setAttribute("hidden", "hidden");
    memberSection?.removeAttribute("hidden");
  } else {
    memberSection?.setAttribute("hidden", "hidden");
    activeSection?.setAttribute("hidden", "hidden");
    signupSection?.removeAttribute("hidden");
  }

  signupForm?.addEventListener("submit", async event => {
    event.preventDefault();
    clearMessage(signupMessage);

    const submitButton = signupForm.querySelector('button[type="submit"]');
    const formData = new FormData(signupForm);

    setButtonLoading(submitButton, true, "Abriendo pago...");

    try {
      const data = await apiRequest("/api/coach/signup-checkout", {
        method: "POST",
        body: {
          name: formData.get("name"),
          email: formData.get("email"),
          password: formData.get("password")
        }
      });

      window.location.href = data.url;
    } catch (error) {
      setMessage(signupMessage, error.message, "error");
      setButtonLoading(submitButton, false);
    }
  });

  startCheckoutButton?.addEventListener("click", async event => {
    event.preventDefault();
    const messageBox = memberSection?.querySelector(".form-result");
    clearMessage(messageBox);
    setButtonLoading(startCheckoutButton, true, "Abriendo pago...");

    try {
      const data = await apiRequest("/api/coach/create-checkout-session", {
        method: "POST"
      });

      window.location.href = data.url;
    } catch (error) {
      setMessage(messageBox, error.message, "error");
      setButtonLoading(startCheckoutButton, false);
    }
  });

  portalButton?.addEventListener("click", async event => {
    event.preventDefault();
    const messageBox = activeSection?.querySelector(".form-result");
    clearMessage(messageBox);
    setButtonLoading(portalButton, true, "Abriendo portal...");

    try {
      const data = await apiRequest("/api/coach/create-portal-session", {
        method: "POST"
      });

      window.location.href = data.url;
    } catch (error) {
      setMessage(messageBox, error.message, "error");
      setButtonLoading(portalButton, false);
    }
  });
}

async function initLoginPage() {
  const loginPage = document.querySelector("[data-coach-login-page]");

  if (!loginPage) {
    return;
  }

  const loginForm = document.querySelector("[data-login-form]");
  const loginMessage = loginForm?.querySelector(".form-result");
  const infoBanner = document.querySelector("[data-login-banner]");
  const me = await fetchViewer();

  if (me.authenticated && me.user?.subscriptionActive) {
    setMessage(
      infoBanner,
      "Tu cuenta ya esta activa. Puedes entrar directo al Coach privado.",
      "success"
    );
  } else if (me.authenticated) {
    setMessage(
      infoBanner,
      "Tu cuenta ya existe. Solo falta activar o retomar tu suscripcion.",
      "info"
    );
  }

  loginForm?.addEventListener("submit", async event => {
    event.preventDefault();
    clearMessage(loginMessage);

    const submitButton = loginForm.querySelector('button[type="submit"]');
    const formData = new FormData(loginForm);

    setButtonLoading(submitButton, true, "Entrando...");

    try {
      const data = await apiRequest("/api/coach/login", {
        method: "POST",
        body: {
          email: formData.get("email"),
          password: formData.get("password")
        }
      });

      if (data.user?.subscriptionActive) {
        window.location.href = "/coach/app/";
        return;
      }

      window.location.href = "/coach/planes/";
    } catch (error) {
      setMessage(loginMessage, error.message, "error");
      setButtonLoading(submitButton, false);
    }
  });
}

async function initCoachAppPage() {
  const appPage = document.querySelector("[data-coach-app-page]");

  if (!appPage) {
    return;
  }

  const me = await fetchViewer();

  if (!me.authenticated) {
    window.location.href = "/coach/login/";
    return;
  }

  if (!me.user?.subscriptionActive) {
    window.location.href = "/coach/planes/";
    return;
  }

  updateAuthTargets(me.user);

  const portalButton = document.querySelector("[data-open-billing-portal]");
  const logoutButton = document.querySelector("[data-coach-logout]");
  const appMessage = document.querySelector("[data-coach-app-message]");

  portalButton?.addEventListener("click", async event => {
    event.preventDefault();
    clearMessage(appMessage);
    setButtonLoading(portalButton, true, "Abriendo portal...");

    try {
      const data = await apiRequest("/api/coach/create-portal-session", {
        method: "POST"
      });

      window.location.href = data.url;
    } catch (error) {
      setMessage(appMessage, error.message, "error");
      setButtonLoading(portalButton, false);
    }
  });

  logoutButton?.addEventListener("click", async event => {
    event.preventDefault();
    clearMessage(appMessage);
    setButtonLoading(logoutButton, true, "Cerrando...");

    try {
      await apiRequest("/api/coach/logout", {
        method: "POST"
      });

      window.location.href = "/coach/login/";
    } catch (error) {
      setMessage(appMessage, error.message, "error");
      setButtonLoading(logoutButton, false);
    }
  });
}

async function initSuccessPage() {
  const successPage = document.querySelector("[data-coach-success-page]");

  if (!successPage) {
    return;
  }

  const statusBox = document.querySelector("[data-success-status]");
  const params = new URLSearchParams(window.location.search);
  const sessionId = params.get("session_id");

  if (!sessionId) {
    setMessage(statusBox, "No recibi el numero de sesion de Stripe. Entra por login para revisar tu cuenta.", "error");
    return;
  }

  setMessage(statusBox, "Estoy confirmando tu pago y activando tu Coach...", "info");

  try {
    const data = await apiRequest(`/api/coach/checkout-session?session_id=${encodeURIComponent(sessionId)}`);
    updateAuthTargets(data.user);

    if (data.user?.subscriptionActive) {
      setMessage(
        statusBox,
        "Tu pago ya quedo confirmado y tu Coach esta activo. Ya puedes entrar.",
        "success"
      );
      return;
    }

    setMessage(
      statusBox,
      "Tu pago ya entro, pero la activacion todavia se esta terminando de acomodar. Intenta entrar en un momento.",
      "info"
    );
  } catch (error) {
    setMessage(statusBox, error.message, "error");
  }
}

document.addEventListener("DOMContentLoaded", () => {
  initFaq();
  initPlanPage();
  initLoginPage();
  initCoachAppPage();
  initSuccessPage();
});
