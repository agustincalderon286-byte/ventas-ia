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

const COACH_CHAT_API_URL = "/chat";
const COACH_CHAT_SESSION_KEY = "agustin-coach-chat-session-id";
const COACH_CHAT_VISITOR_KEY = "agustin-coach-visitor-id";
const COACH_PLAN_CONFIG = {
  trial: {
    name: "Prueba gratis de 7 dias",
    copy: "Empiezas sin tarjeta. Si despues quieres seguir, activas tu plan de pago.",
    signupLabel: "Crear cuenta y empezar gratis",
    memberLabel: "Empezar prueba gratis",
    memberCopy: "Tu cuenta ya existe. Puedes arrancar tu prueba gratis de 7 dias sin tarjeta."
  },
  monthly: {
    name: "Plan mensual de $30",
    copy: "Acceso privado completo por 30 dolares al mes para usar el Coach de forma continua.",
    signupLabel: "Crear cuenta y continuar al mensual",
    memberLabel: "Continuar con plan mensual",
    memberCopy: "Tu cuenta ya existe. Solo falta abrir el plan mensual para activar tu Coach."
  },
  annual: {
    name: "Plan anual de $300",
    copy: "Acceso completo por 12 meses. Ahorras 2 meses al pagar anual.",
    signupLabel: "Crear cuenta y continuar al anual",
    memberLabel: "Continuar con plan anual",
    memberCopy: "Tu cuenta ya existe. Solo falta abrir el plan anual para activar tu Coach."
  }
};
const ORDER_CALC_PRODUCT_COUNT = 3;

function buildCoachId(prefix) {
  if (window.crypto && typeof window.crypto.randomUUID === "function") {
    return `${prefix}-${window.crypto.randomUUID()}`;
  }

  return `${prefix}-${Date.now()}-${Math.random().toString(36).slice(2, 10)}`;
}

function getCoachChatSessionId() {
  const saved = window.sessionStorage.getItem(COACH_CHAT_SESSION_KEY);

  if (saved) {
    return saved;
  }

  const newId = buildCoachId("coach-session");
  window.sessionStorage.setItem(COACH_CHAT_SESSION_KEY, newId);
  return newId;
}

function getCoachVisitorId() {
  const saved = window.localStorage.getItem(COACH_CHAT_VISITOR_KEY);

  if (saved) {
    return saved;
  }

  const newId = buildCoachId("coach-visitor");
  window.localStorage.setItem(COACH_CHAT_VISITOR_KEY, newId);
  return newId;
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

function formatMoney(value) {
  return `$${(Number(value) || 0).toFixed(2)}`;
}

function autoResizeTextarea(textarea) {
  if (!textarea) {
    return;
  }

  textarea.style.height = "auto";
  textarea.style.height = `${Math.min(textarea.scrollHeight, 180)}px`;
}

async function copyTextToClipboard(text) {
  if (navigator.clipboard?.writeText) {
    await navigator.clipboard.writeText(text);
    return true;
  }

  const tempInput = document.createElement("textarea");
  tempInput.value = text;
  tempInput.setAttribute("readonly", "readonly");
  tempInput.style.position = "absolute";
  tempInput.style.left = "-9999px";
  document.body.appendChild(tempInput);
  tempInput.select();
  const copied = document.execCommand("copy");
  document.body.removeChild(tempInput);
  return copied;
}

function sanitizeZipCode(value = "") {
  return String(value || "").replace(/\D/g, "").slice(0, 5);
}

function createOrderCalcProductCard(index) {
  return `
    <section class="order-calc-product" data-order-calc-product>
      <div class="order-calc-product-head">
        <div>
          <strong>Producto ${index}</strong>
          <span>Calcula bruto, ahorro y balance real.</span>
        </div>
        <div class="order-calc-product-total" data-order-calc-gross>$0.00</div>
      </div>

      <div class="order-calc-grid">
        <label class="order-calc-field">
          <span>Nombre del producto</span>
          <input type="text" data-order-calc-name placeholder="Ej. Olla de presion" />
        </label>
        <label class="order-calc-field">
          <span>Precio</span>
          <input type="number" min="0" step="0.01" data-order-calc-price placeholder="0.00" />
        </label>
        <label class="order-calc-field">
          <span>Down payment</span>
          <input type="number" min="0" step="0.01" data-order-calc-down placeholder="0.00" />
        </label>
        <label class="order-calc-field">
          <span>Descuento fijo</span>
          <input type="number" min="0" step="0.01" data-order-calc-desc-fixed placeholder="0.00" />
        </label>
        <label class="order-calc-field">
          <span>Descuento %</span>
          <input type="number" min="0" step="0.01" data-order-calc-desc-percent placeholder="0" />
        </label>
      </div>

      <div class="order-calc-metrics">
        <div class="order-calc-metric">
          <span>Total bruto</span>
          <strong data-order-calc-total>$0.00</strong>
        </div>
        <div class="order-calc-metric">
          <span>Ahorro aplicado</span>
          <strong data-order-calc-savings>$0.00</strong>
        </div>
        <div class="order-calc-metric is-balance">
          <span>Balance individual</span>
          <strong data-order-calc-balance>$0.00</strong>
        </div>
        <div class="order-calc-metric">
          <span>Pago mensual</span>
          <strong data-order-calc-monthly>$0.00</strong>
        </div>
        <div class="order-calc-metric">
          <span>Pago semanal</span>
          <strong data-order-calc-weekly>$0.00</strong>
        </div>
        <div class="order-calc-metric">
          <span>Pago diario</span>
          <strong data-order-calc-daily>$0.00</strong>
        </div>
      </div>
    </section>
  `;
}

function readCalcNumber(input) {
  return Number.parseFloat(input?.value || "") || 0;
}

function initOrderCalculator() {
  const root = document.querySelector("[data-order-calc]");

  if (!root) {
    return;
  }

  const productsContainer = root.querySelector("[data-order-calc-products]");
  if (productsContainer && !productsContainer.children.length) {
    productsContainer.innerHTML = Array.from({ length: ORDER_CALC_PRODUCT_COUNT }, (_, index) =>
      createOrderCalcProductCard(index + 1)
    ).join("");
  }

  const productCards = Array.from(root.querySelectorAll("[data-order-calc-product]"));
  const summaryDown = root.querySelector("[data-order-calc-down-total]");
  const summaryDescFixed = root.querySelector("[data-order-calc-desc-fixed-total]");
  const summaryDescPercent = root.querySelector("[data-order-calc-desc-percent-total]");
  const totalNode = root.querySelector("[data-order-calc-summary-total]");
  const downNode = root.querySelector("[data-order-calc-summary-down]");
  const discountNode = root.querySelector("[data-order-calc-summary-discount]");
  const balanceNode = root.querySelector("[data-order-calc-summary-balance]");
  const monthlyNode = root.querySelector("[data-order-calc-summary-monthly]");
  const weeklyNode = root.querySelector("[data-order-calc-summary-weekly]");
  const dailyNode = root.querySelector("[data-order-calc-summary-daily]");

  function recalculate() {
    let totalGeneral = 0;
    let totalDown = 0;
    let totalDiscount = 0;

    productCards.forEach(card => {
      const price = readCalcNumber(card.querySelector("[data-order-calc-price]"));
      const down = readCalcNumber(card.querySelector("[data-order-calc-down]"));
      const descFixed = readCalcNumber(card.querySelector("[data-order-calc-desc-fixed]"));
      const descPercent = readCalcNumber(card.querySelector("[data-order-calc-desc-percent]"));

      const tax = price * 0.1;
      const shipping = price * 0.05;
      const grossTotal = price + tax + shipping;
      const discountAmount = descFixed + grossTotal * (descPercent / 100);
      const savings = down + discountAmount;
      const balance = Math.max(0, grossTotal - savings);
      const monthly = balance * 0.05;
      const weekly = monthly / 4;
      const daily = weekly / 7;

      const grossNode = card.querySelector("[data-order-calc-gross]");
      const totalProductNode = card.querySelector("[data-order-calc-total]");
      const savingsNode = card.querySelector("[data-order-calc-savings]");
      const balanceProductNode = card.querySelector("[data-order-calc-balance]");
      const monthlyProductNode = card.querySelector("[data-order-calc-monthly]");
      const weeklyProductNode = card.querySelector("[data-order-calc-weekly]");
      const dailyProductNode = card.querySelector("[data-order-calc-daily]");

      if (grossNode) grossNode.textContent = formatMoney(grossTotal);
      if (totalProductNode) totalProductNode.textContent = formatMoney(grossTotal);
      if (savingsNode) savingsNode.textContent = formatMoney(savings);
      if (balanceProductNode) balanceProductNode.textContent = formatMoney(balance);
      if (monthlyProductNode) monthlyProductNode.textContent = formatMoney(monthly);
      if (weeklyProductNode) weeklyProductNode.textContent = formatMoney(weekly);
      if (dailyProductNode) dailyProductNode.textContent = formatMoney(daily);

      totalGeneral += grossTotal;
      totalDown += down;
      totalDiscount += discountAmount;
    });

    const extraDown = readCalcNumber(summaryDown);
    const extraDescFixed = readCalcNumber(summaryDescFixed);
    const extraDescPercent = readCalcNumber(summaryDescPercent);
    const extraDiscount = extraDescFixed + totalGeneral * (extraDescPercent / 100);
    const finalDown = totalDown + extraDown;
    const finalDiscount = totalDiscount + extraDiscount;
    const finalBalance = Math.max(0, totalGeneral - finalDown - finalDiscount);
    const finalMonthly = finalBalance * 0.05;
    const finalWeekly = finalMonthly / 4;
    const finalDaily = finalWeekly / 7;

    if (totalNode) totalNode.textContent = formatMoney(totalGeneral);
    if (downNode) downNode.textContent = formatMoney(finalDown);
    if (discountNode) discountNode.textContent = formatMoney(finalDiscount);
    if (balanceNode) balanceNode.textContent = formatMoney(finalBalance);
    if (monthlyNode) monthlyNode.textContent = formatMoney(finalMonthly);
    if (weeklyNode) weeklyNode.textContent = formatMoney(finalWeekly);
    if (dailyNode) dailyNode.textContent = formatMoney(finalDaily);
  }

  root.querySelectorAll("input").forEach(input => {
    input.addEventListener("input", recalculate);
  });

  recalculate();
}

function addCoachMessage(container, role, content) {
  if (!container) {
    return;
  }

  const card = document.createElement("article");
  card.className = `coach-message ${role}`;

  const paragraph = document.createElement("p");
  paragraph.textContent = content;
  card.appendChild(paragraph);

  container.appendChild(card);
  container.scrollTop = container.scrollHeight;
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
    node.textContent = user?.subscriptionStatus === "test_access" || user?.subscriptionStatus === "trialing"
      ? "Prueba activa"
      : user?.subscriptionActive
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

function formatCoachInsightLabel(value) {
  const limpio = String(value || "").trim();

  if (!limpio) {
    return "Sin datos";
  }

  return limpio
    .replace(/_/g, " ")
    .replace(/\s+/g, " ")
    .replace(/\b\w/g, letter => letter.toUpperCase());
}

function joinCoachInsightList(items, emptyText = "Sin datos todavia.") {
  return Array.isArray(items) && items.length
    ? items.map(formatCoachInsightLabel).join(" · ")
    : emptyText;
}

function renderCoachTagList(target, items, emptyText = "Sin datos") {
  if (!target) {
    return;
  }

  target.innerHTML = "";
  const values = Array.isArray(items) ? items.filter(Boolean) : [];

  if (!values.length) {
    const chip = document.createElement("span");
    chip.className = "tag-chip is-muted";
    chip.textContent = emptyText;
    target.appendChild(chip);
    return;
  }

  values.forEach(item => {
    const chip = document.createElement("span");
    chip.className = "tag-chip";
    chip.textContent = formatCoachInsightLabel(item);
    target.appendChild(chip);
  });
}

function renderCoachProfile(profile) {
  const safeProfile = profile || {};

  document.querySelectorAll("[data-coach-profile-level]").forEach(node => {
    node.textContent = formatCoachInsightLabel(safeProfile.level || "novato");
  });

  document.querySelectorAll("[data-coach-profile-questions]").forEach(node => {
    node.textContent = String(safeProfile.questionsCount || 0);
  });

  document.querySelectorAll("[data-coach-profile-sessions]").forEach(node => {
    node.textContent = String(safeProfile.totalSessions || 0);
  });

  document.querySelectorAll("[data-coach-profile-focus]").forEach(node => {
    node.textContent = joinCoachInsightList(safeProfile.focusAreas, "Todavia no hay foco claro.");
  });

  document.querySelectorAll("[data-coach-profile-pain]").forEach(node => {
    node.textContent = joinCoachInsightList(safeProfile.painAreas, "Todavia no hay dolor claro.");
  });

  document.querySelectorAll("[data-coach-profile-close]").forEach(node => {
    node.textContent = safeProfile.preferredCloseStyle
      ? formatCoachInsightLabel(safeProfile.preferredCloseStyle)
      : "Todavia no hay cierre dominante.";
  });

  document.querySelectorAll("[data-coach-profile-style]").forEach(node => {
    node.textContent = formatCoachInsightLabel(safeProfile.supportStyle || "directo");
  });
}

function renderCoachNetworkSummary(summary) {
  const safeSummary = summary || {};

  document.querySelectorAll("[data-coach-network-total]").forEach(node => {
    node.textContent = String(safeSummary.totalDistributors || 0);
  });

  document.querySelectorAll("[data-coach-network-today]").forEach(node => {
    node.textContent = String(safeSummary.activeToday || 0);
  });

  document.querySelectorAll("[data-coach-network-week]").forEach(node => {
    node.textContent = String(safeSummary.activeLast7Days || 0);
  });

  renderCoachTagList(
    document.querySelector("[data-coach-network-topics]"),
    safeSummary.topTopics,
    "Aun no hay datos globales."
  );
  renderCoachTagList(
    document.querySelector("[data-coach-network-objections]"),
    safeSummary.topObjections,
    "Aun no hay objeciones globales."
  );
  renderCoachTagList(
    document.querySelector("[data-coach-network-stages]"),
    safeSummary.topStages,
    "Aun no hay etapas globales."
  );
}

function renderCoachRepLeadSummary(summary) {
  const safeSummary = summary || {};
  const scoreboard = safeSummary.scoreboard || {};

  document.querySelectorAll("[data-coach-rifa-hot]").forEach(node => {
    node.textContent = String(scoreboard.hot || 0);
  });

  document.querySelectorAll("[data-coach-rifa-warm]").forEach(node => {
    node.textContent = String(scoreboard.warm || 0);
  });

  document.querySelectorAll("[data-coach-rifa-cold]").forEach(node => {
    node.textContent = String(scoreboard.cold || 0);
  });

  document.querySelectorAll("[data-coach-rifa-dead]").forEach(node => {
    node.textContent = String(scoreboard.dead || 0);
  });

  document.querySelectorAll("[data-coach-rifa-statuses]").forEach(node => {
    node.textContent = joinCoachInsightList(safeSummary.topStatuses, "Sin datos todavia.");
  });

  document.querySelectorAll("[data-coach-rifa-angles]").forEach(node => {
    node.textContent = joinCoachInsightList(safeSummary.topScriptAngles, "Sin datos todavia.");
  });
}

function renderActiveLeadContext(context) {
  const safeContext = context || {};
  const scoreCopy =
    safeContext.leadTemperature || safeContext.callStatus
      ? `${formatCoachInsightLabel(safeContext.leadTemperature || "sin score")} · ${formatCoachInsightLabel(
          safeContext.callStatus || "sin estado"
        )}`
      : "Sin score todavia.";

  document.querySelectorAll("[data-coach-active-lead-name]").forEach(node => {
    node.textContent = safeContext.leadName
      ? `${safeContext.leadName}${safeContext.leadId ? ` · Lead ${safeContext.leadId}` : ""}`
      : "Sin lead detectado todavia.";
  });

  document.querySelectorAll("[data-coach-active-lead-score]").forEach(node => {
    node.textContent = scoreCopy;
  });

  document.querySelectorAll("[data-coach-active-lead-next]").forEach(node => {
    node.textContent = safeContext.nextStep
      ? formatCoachInsightLabel(safeContext.nextStep)
      : "Sin siguiente paso todavia.";
  });

  document.querySelectorAll("[data-coach-active-lead-angle]").forEach(node => {
    node.textContent = safeContext.bestScriptAngle
      ? formatCoachInsightLabel(safeContext.bestScriptAngle)
      : "Sin angulo todavia.";
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
  const planInput = document.querySelector("[data-plan-input]");
  const signupSubmitLabel = document.querySelector("[data-plan-submit-label]");
  const selectedPlanName = document.querySelector("[data-selected-plan-name]");
  const selectedPlanCopy = document.querySelector("[data-selected-plan-copy]");
  const memberPlanCopy = document.querySelector("[data-member-plan-copy]");
  const memberPlanLabel = document.querySelector("[data-member-plan-label]");
  const planCards = document.querySelectorAll("[data-plan-card]");
  const planSelectButtons = document.querySelectorAll("[data-plan-select]");

  planSelectButtons.forEach(button => {
    if (!button.dataset.defaultLabel) {
      button.dataset.defaultLabel = button.textContent;
    }
  });

  const me = await fetchViewer();
  const user = me.user || null;
  updateAuthTargets(user);
  let selectedPlan = user && !user.trialEligible ? "monthly" : "trial";

  function getPlanConfig(plan) {
    return COACH_PLAN_CONFIG[plan] || COACH_PLAN_CONFIG.monthly;
  }

  function syncPlanUi() {
    const selectedConfig = getPlanConfig(selectedPlan);

    if (planInput) {
      planInput.value = selectedPlan;
    }

    if (selectedPlanName) {
      selectedPlanName.textContent = selectedConfig.name;
    }

    if (selectedPlanCopy) {
      selectedPlanCopy.textContent = selectedConfig.copy;
    }

    if (signupSubmitLabel) {
      signupSubmitLabel.textContent = selectedConfig.signupLabel;
    }

    if (memberPlanCopy) {
      memberPlanCopy.textContent = selectedConfig.memberCopy;
    }

    if (memberPlanLabel) {
      memberPlanLabel.textContent = selectedConfig.memberLabel;
    }

    planCards.forEach(card => {
      card.classList.toggle("is-selected", card.dataset.plan === selectedPlan);
      card.classList.toggle("is-disabled", card.dataset.plan === "trial" && Boolean(user) && !user.trialEligible);
    });

    planSelectButtons.forEach(button => {
      const isTrialDisabled = button.dataset.planSelect === "trial" && Boolean(user) && !user.trialEligible;
      button.disabled = isTrialDisabled;

      if (isTrialDisabled) {
        button.textContent = "Prueba ya utilizada";
      } else {
        button.textContent = button.dataset.defaultLabel || button.textContent;
      }
    });
  }

  planSelectButtons.forEach(button => {
    button.addEventListener("click", () => {
      const plan = button.dataset.planSelect || "monthly";

      if (plan === "trial" && user && !user.trialEligible) {
        return;
      }

      selectedPlan = plan;
      syncPlanUi();
      signupSection?.scrollIntoView({ behavior: "smooth", block: "start" });
    });
  });

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

  syncPlanUi();

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
          password: formData.get("password"),
          plan: formData.get("plan")
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
        method: "POST",
        body: {
          plan: selectedPlan
        }
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
  renderCoachProfile(me.profile);
  renderCoachNetworkSummary(me.networkSummary);
  renderCoachRepLeadSummary(me.repLeadSummary);
  renderActiveLeadContext(me.activeLeadContext);
  initOrderCalculator();

  const chatMessages = document.querySelector("[data-coach-chat-messages]");
  const chatForm = document.querySelector("[data-coach-chat-form]");
  const chatInput = document.querySelector("[data-coach-chat-input]");
  const chatSendButton = document.querySelector("[data-coach-chat-send]");
  const chatStatus = document.querySelector("[data-coach-chat-status]");
  const portalButtons = document.querySelectorAll("[data-open-billing-portal]");
  const logoutButtons = document.querySelectorAll("[data-coach-logout]");
  const chefShareButtons = document.querySelectorAll("[data-open-chef-share]");
  const chefShareModal = document.querySelector("[data-chef-share-modal]");
  const chefShareCloseButtons = document.querySelectorAll("[data-close-chef-share]");
  const chefShareUrlNodes = document.querySelectorAll("[data-chef-share-url]");
  const chefShareOpenLinks = document.querySelectorAll("[data-chef-share-open-link]");
  const copyChefLinkButton = document.querySelector("[data-copy-chef-link]");
  const nativeShareChefButton = document.querySelector("[data-native-share-chef]");
  const chefShareFeedback = document.querySelector("[data-chef-share-feedback]");
  const orderCalcToggle = document.querySelector("[data-order-calc-toggle]");
  const orderCalcWrap = document.querySelector("[data-order-calc-wrap]");
  const waterCheckForm = document.querySelector("[data-water-check-form]");
  const waterCheckInput = document.querySelector("[data-water-check-input]");
  const waterCheckFeedback = document.querySelector("[data-water-check-feedback]");
  const appMessage = document.querySelector("[data-coach-app-message]");
  const chefShareUrl = `${window.location.origin}/chef/`;

  chefShareUrlNodes.forEach(node => {
    node.textContent = chefShareUrl;
  });

  chefShareOpenLinks.forEach(node => {
    node.href = chefShareUrl;
  });

  if (nativeShareChefButton && typeof navigator.share !== "function") {
    nativeShareChefButton.hidden = true;
  }

  const syncOrderCalcToggle = open => {
    if (orderCalcWrap) {
      orderCalcWrap.hidden = !open;
    }

    if (orderCalcToggle) {
      orderCalcToggle.setAttribute("aria-expanded", open ? "true" : "false");
      orderCalcToggle.textContent = open ? "Cerrar calculadora" : "Abrir calculadora";
    }
  };

  syncOrderCalcToggle(false);

  const openChefShareModal = () => {
    if (!chefShareModal) {
      return;
    }

    if (chefShareFeedback) {
      chefShareFeedback.textContent = "";
    }
    chefShareModal.hidden = false;
    document.body.classList.add("modal-open");
  };

  const closeChefShareModal = () => {
    if (!chefShareModal) {
      return;
    }

    chefShareModal.hidden = true;
    document.body.classList.remove("modal-open");
  };

  const sendCoachMessage = async rawText => {
    const text = String(rawText || "").trim();

    if (!text) {
      return;
    }

    addCoachMessage(chatMessages, "user", text);

    if (chatInput) {
      chatInput.value = "";
      autoResizeTextarea(chatInput);
      chatInput.disabled = true;
    }

    if (chatSendButton) {
      chatSendButton.disabled = true;
    }

    if (chatStatus) {
      chatStatus.textContent = "Pensando...";
    }

    try {
      const data = await apiRequest(COACH_CHAT_API_URL, {
        method: "POST",
        body: {
          pregunta: text,
          sessionId: getCoachChatSessionId(),
          visitorId: getCoachVisitorId(),
          mode: "coach"
        }
      });

      addCoachMessage(
        chatMessages,
        "assistant",
        data.respuesta || "No pude responder en este momento."
      );

      if (data.profile) {
        renderCoachProfile(data.profile);
      }

      renderCoachRepLeadSummary(data.repLeadSummary || null);
      renderActiveLeadContext(data.activeLeadContext || null);
    } catch (error) {
      addCoachMessage(
        chatMessages,
        "assistant",
        error.message || "No pude responder en este momento."
      );
    } finally {
      if (chatInput) {
        chatInput.disabled = false;
        chatInput.focus();
      }

      if (chatSendButton) {
        chatSendButton.disabled = false;
      }

      if (chatStatus) {
        chatStatus.textContent = "Listo";
      }
    }
  };

  chatForm?.addEventListener("submit", event => {
    event.preventDefault();
    sendCoachMessage(chatInput?.value || "");
  });

  chatInput?.addEventListener("input", () => {
    autoResizeTextarea(chatInput);
  });

  chatInput?.addEventListener("keydown", event => {
    if (event.key === "Enter" && !event.shiftKey) {
      event.preventDefault();
      sendCoachMessage(chatInput?.value || "");
    }
  });

  autoResizeTextarea(chatInput);

  chefShareButtons.forEach(button => {
    button.addEventListener("click", event => {
      event.preventDefault();
      openChefShareModal();
    });
  });

  chefShareCloseButtons.forEach(button => {
    button.addEventListener("click", () => {
      closeChefShareModal();
    });
  });

  chefShareModal?.addEventListener("click", event => {
    if (event.target === chefShareModal) {
      closeChefShareModal();
    }
  });

  document.addEventListener("keydown", event => {
    if (event.key === "Escape" && chefShareModal && !chefShareModal.hidden) {
      closeChefShareModal();
    }
  });

  copyChefLinkButton?.addEventListener("click", async () => {
    try {
      await copyTextToClipboard(chefShareUrl);
      if (chefShareFeedback) {
        chefShareFeedback.textContent = "El link del Chef ya quedo copiado.";
      }
    } catch (error) {
      if (chefShareFeedback) {
        chefShareFeedback.textContent = "No pude copiar el link del Chef.";
      } else {
        setMessage(appMessage, "No pude copiar el link del Chef.", "error");
      }
    }
  });

  nativeShareChefButton?.addEventListener("click", async () => {
    try {
      await navigator.share({
        title: "Agustin 2.0 Chef",
        text: "Te comparto Agustin 2.0 Chef para recetas y cocina saludable.",
        url: chefShareUrl
      });
    } catch (error) {
      // noop
    }
  });

  orderCalcToggle?.addEventListener("click", () => {
    const isOpen = orderCalcWrap ? !orderCalcWrap.hidden : false;
    syncOrderCalcToggle(!isOpen);
  });

  waterCheckInput?.addEventListener("input", () => {
    const safeZip = sanitizeZipCode(waterCheckInput.value);
    if (waterCheckInput.value !== safeZip) {
      waterCheckInput.value = safeZip;
    }
  });

  waterCheckForm?.addEventListener("submit", async event => {
    event.preventDefault();

    const zip = sanitizeZipCode(waterCheckInput?.value || "");

    if (!zip || zip.length < 5) {
      setMessage(waterCheckFeedback, "Ingresa un ZIP Code valido de 5 digitos.", "error");
      waterCheckInput?.focus();
      return;
    }

    clearMessage(waterCheckFeedback);

    try {
      await copyTextToClipboard(zip);
      setMessage(
        waterCheckFeedback,
        `Se abrio EWG en una nueva pestana y el ZIP ${zip} quedo copiado para pegarlo en la busqueda.`,
        "success"
      );
    } catch (error) {
      setMessage(
        waterCheckFeedback,
        `Se abrio EWG en una nueva pestana. Usa el ZIP ${zip} en la busqueda.`,
        "info"
      );
    }

    window.open("https://www.ewg.org/tapwater/", "_blank", "noopener,noreferrer");
  });

  portalButtons.forEach(button => {
    button.addEventListener("click", async event => {
      event.preventDefault();
      clearMessage(appMessage);
      setButtonLoading(button, true, "Abriendo portal...");

      try {
        const data = await apiRequest("/api/coach/create-portal-session", {
          method: "POST"
        });

        window.location.href = data.url;
      } catch (error) {
        setMessage(appMessage, error.message, "error");
        setButtonLoading(button, false);
      }
    });
  });

  logoutButtons.forEach(button => {
    button.addEventListener("click", async event => {
      event.preventDefault();
      clearMessage(appMessage);
      setButtonLoading(button, true, "Cerrando...");

      try {
        await apiRequest("/api/coach/logout", {
          method: "POST"
        });

        window.location.href = "/coach/login/";
      } catch (error) {
        setMessage(appMessage, error.message, "error");
        setButtonLoading(button, false);
      }
    });
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
      if (data.user?.subscriptionStatus === "trialing") {
        setMessage(
          statusBox,
          "Tu prueba gratis ya quedo activa. Ya puedes entrar al Coach.",
          "success"
        );
        return;
      }

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
