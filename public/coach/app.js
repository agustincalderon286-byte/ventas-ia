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
const GOOGLE_RAFFLE_FORM_URL =
  "https://docs.google.com/forms/d/e/1FAIpQLSfoxNU7_3BbGUCaal6U04v8ymJCGCuc9sGvfXoHiMxqbQmNyw/viewform";
const GOOGLE_RAFFLE_FORM_EMBED_URL = `${GOOGLE_RAFFLE_FORM_URL}?embedded=true`;
const COACH_WORKSPACE_TAB_KEY = "agustin-coach-workspace-tab";
const COACH_LEAD_STATUS_OPTIONS = [
  { value: "nuevo", label: "Nuevo" },
  { value: "contactado", label: "Contactado" },
  { value: "agendado", label: "Agendado" },
  { value: "cliente", label: "Cliente" },
  { value: "archivado", label: "Archivado" }
];
const COACH_LEAD_SOURCE_LABELS = {
  captura_manual: "Captura manual",
  rifa_digital: "Rifa digital",
  llamada: "Llamada",
  demo: "Demo",
  referencia: "Referencia",
  evento: "Evento",
  otro: "Otro"
};
const COACH_LEAD_DESTINATION_LABELS = {
  carpeta_privada: "Solo mi carpeta privada",
  google_sheets: "Google Sheets / Apps Script",
  webhook_crm: "Webhook / CRM"
};
const COACH_LEAD_NEXT_ACTION_OPTIONS = [
  { value: "", label: "Sin proxima accion" },
  { value: "llamar", label: "Llamar" },
  { value: "whatsapp", label: "WhatsApp" },
  { value: "cita", label: "Agendar cita" },
  { value: "demo", label: "Preparar demo" },
  { value: "seguimiento", label: "Dar seguimiento" },
  { value: "correo", label: "Mandar correo" }
];
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
const DECISION_TOOL_ROW_COUNT = 5;
const BUYER_PROFILE_OPTIONS = [
  { value: "0", label: "Elige una opcion" },
  { value: "1", label: "Poco" },
  { value: "2", label: "Algo" },
  { value: "3", label: "Mucho" },
  { value: "4", label: "Muchisimo" }
];
const BUYER_PROFILE_QUESTIONS = [
  {
    id: "family_health",
    title: "Que tanto te importa cuidar la salud de tu casa?",
    hint: "Sirve para ver si lo mueve la familia y el bienestar.",
    group: "family"
  },
  {
    id: "family_clean_cooking",
    title: "Que tanto te importa cocinar con menos grasa?",
    hint: "Ayuda a saber si le importa una cocina mas limpia.",
    group: "family"
  },
  {
    id: "money_savings",
    title: "Que tanto te importa ahorrar con el tiempo?",
    hint: "Mide si le pesa el ahorro y el valor real.",
    group: "money"
  },
  {
    id: "money_durable",
    title: "Que tanto te importa comprar algo que dure?",
    hint: "Mide si piensa a largo plazo.",
    group: "money"
  },
  {
    id: "practical_speed",
    title: "Que tanto te ayudaria cocinar mas rapido?",
    hint: "Nos dice si quiere facilidad y menos vuelta.",
    group: "practical"
  },
  {
    id: "practical_daily",
    title: "Que tanto usarias esto en tu dia a dia?",
    hint: "Mide si lo ve util de verdad.",
    group: "practical"
  },
  {
    id: "proof_evidence",
    title: "Antes de comprar, que tanto ocupas ver pruebas?",
    hint: "Nos dice si primero necesita confiar.",
    group: "proof"
  },
  {
    id: "proof_water",
    title: "Que tanto te ayudaria ver datos del agua de tu zona?",
    hint: "Mide si le ayuda ver evidencia concreta.",
    group: "proof"
  }
];

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

function formatDateTime(dateString) {
  if (!dateString) {
    return "Sin fecha";
  }

  const date = new Date(dateString);

  if (Number.isNaN(date.getTime())) {
    return "Sin fecha";
  }

  return new Intl.DateTimeFormat("es-US", {
    year: "numeric",
    month: "short",
    day: "numeric",
    hour: "numeric",
    minute: "2-digit"
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

function initCoachWorkspaceTabs() {
  const tabButtons = Array.from(document.querySelectorAll("[data-coach-workspace-tab]"));
  const workspaceSections = Array.from(document.querySelectorAll("[data-coach-workspace-section]"));

  if (!tabButtons.length || !workspaceSections.length) {
    return;
  }

  const validTabs = new Set(tabButtons.map(button => button.dataset.coachWorkspaceTab).filter(Boolean));
  let activeTab = window.sessionStorage.getItem(COACH_WORKSPACE_TAB_KEY) || "cierre";

  if (!validTabs.has(activeTab)) {
    activeTab = "cierre";
  }

  const syncWorkspace = nextTab => {
    const safeTab = validTabs.has(nextTab) ? nextTab : "cierre";

    tabButtons.forEach(button => {
      const isActive = button.dataset.coachWorkspaceTab === safeTab;
      button.classList.toggle("is-active", isActive);
      button.setAttribute("aria-selected", isActive ? "true" : "false");
    });

    workspaceSections.forEach(section => {
      section.hidden = section.dataset.coachWorkspaceSection !== safeTab;
    });

    window.sessionStorage.setItem(COACH_WORKSPACE_TAB_KEY, safeTab);
  };

  tabButtons.forEach(button => {
    button.addEventListener("click", () => {
      syncWorkspace(button.dataset.coachWorkspaceTab || "cierre");
    });
  });

  syncWorkspace(activeTab);
}

function normalizeLeadPhone(value = "") {
  const digits = String(value || "").replace(/\D/g, "");

  if (!digits) {
    return "";
  }

  if (digits.length === 11 && digits.startsWith("1")) {
    return digits.slice(1);
  }

  return digits;
}

function formatLeadNextActionLabel(value = "") {
  const found = COACH_LEAD_NEXT_ACTION_OPTIONS.find(option => option.value === value);
  return found?.label || "Sin proxima accion";
}

function formatLeadDestinationLabel(type = "") {
  return COACH_LEAD_DESTINATION_LABELS[type] || COACH_LEAD_DESTINATION_LABELS.carpeta_privada;
}

function formatDateInputValue(dateString) {
  if (!dateString) {
    return "";
  }

  const date = new Date(dateString);

  if (Number.isNaN(date.getTime())) {
    return "";
  }

  const year = date.getFullYear();
  const month = String(date.getMonth() + 1).padStart(2, "0");
  const day = String(date.getDate()).padStart(2, "0");
  return `${year}-${month}-${day}`;
}

function formatTimeInputValue(dateString) {
  if (!dateString) {
    return "";
  }

  const date = new Date(dateString);

  if (Number.isNaN(date.getTime())) {
    return "";
  }

  const hours = String(date.getHours()).padStart(2, "0");
  const minutes = String(date.getMinutes()).padStart(2, "0");
  return `${hours}:${minutes}`;
}

function buildLeadDateTimeValue(dateValue = "", timeValue = "") {
  if (!dateValue) {
    return "";
  }

  const safeTime = timeValue || "09:00";
  const parsed = new Date(`${dateValue}T${safeTime}`);
  return Number.isNaN(parsed.getTime()) ? "" : parsed.toISOString();
}

function formatLeadPhone(value = "") {
  const digits = normalizeLeadPhone(value);

  if (digits.length === 10) {
    return `(${digits.slice(0, 3)}) ${digits.slice(3, 6)}-${digits.slice(6)}`;
  }

  return value || "Sin telefono";
}

function formatLeadStatusLabel(status = "") {
  const found = COACH_LEAD_STATUS_OPTIONS.find(option => option.value === status);
  return found?.label || "Nuevo";
}

function formatLeadSourceLabel(source = "") {
  return COACH_LEAD_SOURCE_LABELS[source] || "Captura manual";
}

function escapeHtml(value = "") {
  return String(value || "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#39;");
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

function createDecisionToolRow(kind, index) {
  const placeholder = kind === "pro" ? "Razón a favor" : "Objeción o freno";

  return `
    <div class="decision-tool-row">
      <input type="text" data-decision-text data-decision-kind="${kind}" placeholder="${placeholder} ${index}" />
      <select data-decision-weight data-decision-kind="${kind}">
        <option value="0">Peso 0</option>
        <option value="1">1</option>
        <option value="2">2</option>
        <option value="3">3</option>
        <option value="4">4</option>
        <option value="5">5</option>
        <option value="6">6</option>
        <option value="7">7</option>
        <option value="8">8</option>
        <option value="9">9</option>
        <option value="10">10</option>
      </select>
    </div>
  `;
}

function initDecisionTool() {
  const wrap = document.querySelector("[data-decision-tool-wrap]");
  const root = document.querySelector("[data-decision-tool]");
  const toggle = document.querySelector("[data-decision-tool-toggle]");

  if (!root || !wrap || !toggle) {
    return;
  }

  const prosContainer = root.querySelector("[data-decision-pros]");
  const consContainer = root.querySelector("[data-decision-cons]");
  const runButton = root.querySelector("[data-decision-tool-run]");
  const resetButton = root.querySelector("[data-decision-tool-reset]");
  const totalProsNode = root.querySelector("[data-decision-total-pros]");
  const totalConsNode = root.querySelector("[data-decision-total-cons]");
  const percentNode = root.querySelector("[data-decision-percent]");
  const messageNode = root.querySelector("[data-decision-message]");
  const objectionNode = root.querySelector("[data-decision-objection]");
  const nextStepNode = root.querySelector("[data-decision-next-step]");
  const prosBar = root.querySelector("[data-decision-bar-pros]");
  const consBar = root.querySelector("[data-decision-bar-cons]");

  if (prosContainer && !prosContainer.children.length) {
    prosContainer.innerHTML = Array.from({ length: DECISION_TOOL_ROW_COUNT }, (_, index) =>
      createDecisionToolRow("pro", index + 1)
    ).join("");
  }

  if (consContainer && !consContainer.children.length) {
    consContainer.innerHTML = Array.from({ length: DECISION_TOOL_ROW_COUNT }, (_, index) =>
      createDecisionToolRow("con", index + 1)
    ).join("");
  }

  const syncDecisionToggle = open => {
    wrap.hidden = !open;
    toggle.setAttribute("aria-expanded", open ? "true" : "false");
    toggle.textContent = open ? "Cerrar balance" : "Abrir balance";
  };

  const analyze = () => {
    const prosRows = Array.from(prosContainer?.querySelectorAll(".decision-tool-row") || []);
    const consRows = Array.from(consContainer?.querySelectorAll(".decision-tool-row") || []);
    let totalPros = 0;
    let totalCons = 0;
    let maxConValue = 0;
    let maxConText = "";

    prosRows.forEach(row => {
      const text = String(row.querySelector("[data-decision-text]")?.value || "").trim();
      const weight = Number.parseInt(row.querySelector("[data-decision-weight]")?.value || "0", 10) || 0;
      if (text || weight > 0) {
        totalPros += weight;
      }
    });

    consRows.forEach(row => {
      const text = String(row.querySelector("[data-decision-text]")?.value || "").trim();
      const weight = Number.parseInt(row.querySelector("[data-decision-weight]")?.value || "0", 10) || 0;
      if (text || weight > 0) {
        totalCons += weight;
      }
      if (weight > maxConValue) {
        maxConValue = weight;
        maxConText = text || "Objeción no especificada";
      }
    });

    const total = totalPros + totalCons;
    const percent = total > 0 ? Math.round((totalPros / total) * 100) : 0;
    const prosPercentOfMax = Math.min(100, Math.round((totalPros / (DECISION_TOOL_ROW_COUNT * 10)) * 100));
    const consPercentOfMax = Math.min(100, Math.round((totalCons / (DECISION_TOOL_ROW_COUNT * 10)) * 100));

    let message = "Agrega razones y objeciones para leer el cierre.";
    let nextStep = "Usalo cuando el cliente diga que lo quiere pensar.";
    let topObjection = maxConText || "Sin objecion principal todavia.";

    if (total > 0 && totalPros >= totalCons && percent >= 70) {
      message = "Los beneficios ya pesan mas que las objeciones.";
      nextStep = "Pide decision con calma y refuerza el valor que mas peso tiene.";
    } else if (total > 0 && totalPros >= totalCons) {
      message = "Hay inclinacion positiva, pero conviene rematar la objecion principal.";
      nextStep = "Resuelve el freno principal y vuelve a pedir el siguiente paso.";
    } else if (total > 0) {
      message = "La indecision sigue cargada hacia una objecion importante.";
      nextStep = "No cierres duro todavia. Resuelve la objecion principal antes de volver a pedir compra.";
    }

    if (totalProsNode) totalProsNode.textContent = String(totalPros);
    if (totalConsNode) totalConsNode.textContent = String(totalCons);
    if (percentNode) percentNode.textContent = `${percent}%`;
    if (messageNode) messageNode.textContent = message;
    if (objectionNode) objectionNode.textContent = topObjection;
    if (nextStepNode) nextStepNode.textContent = nextStep;
    if (prosBar) prosBar.style.width = `${prosPercentOfMax}%`;
    if (consBar) consBar.style.width = `${consPercentOfMax}%`;
  };

  const reset = () => {
    root.querySelectorAll("input").forEach(input => {
      input.value = "";
    });
    root.querySelectorAll("select").forEach(select => {
      select.value = "0";
    });
    analyze();
  };

  syncDecisionToggle(false);
  analyze();

  toggle.addEventListener("click", () => {
    syncDecisionToggle(wrap.hidden);
  });

  runButton?.addEventListener("click", analyze);
  resetButton?.addEventListener("click", reset);
}

function createBuyerProfileQuestionRow(question) {
  const options = BUYER_PROFILE_OPTIONS.map(
    option => `<option value="${option.value}">${option.label}</option>`
  ).join("");

  return `
    <label class="buyer-profile-row">
      <div>
        <strong>${question.title}</strong>
        <span>${question.hint}</span>
      </div>
      <select data-buyer-profile-score data-buyer-profile-group="${question.group}">
        ${options}
      </select>
    </label>
  `;
}

function initBuyerProfileTool() {
  const wrap = document.querySelector("[data-buyer-profile-wrap]");
  const root = document.querySelector("[data-buyer-profile-tool]");
  const toggle = document.querySelector("[data-buyer-profile-toggle]");

  if (!root || !wrap || !toggle) {
    return;
  }

  const questionsContainer = root.querySelector("[data-buyer-profile-questions]");
  const runButton = root.querySelector("[data-buyer-profile-run]");
  const resetButton = root.querySelector("[data-buyer-profile-reset]");
  const mainNode = root.querySelector("[data-buyer-profile-main]");
  const driverNode = root.querySelector("[data-buyer-profile-driver]");
  const scriptNode = root.querySelector("[data-buyer-profile-script]");
  const recommendationNode = root.querySelector("[data-buyer-profile-recommendation]");

  if (questionsContainer && !questionsContainer.children.length) {
    questionsContainer.innerHTML = BUYER_PROFILE_QUESTIONS.map(createBuyerProfileQuestionRow).join("");
  }

  const profiles = {
    family: {
      label: "Cliente de familia",
      driver: "Lo mueve cuidar a su casa, su comida y su bienestar.",
      script: "Habla simple. Conectalo con salud, familia, cocina limpia y uso diario.",
      recommendation: "Abre calidad del agua o comparte el Chef con recetas y apoyo."
    },
    money: {
      label: "Cliente de ahorro",
      driver: "Le importa que el dinero rinda y comprar algo que dure.",
      script: "Habla de valor real, ahorro a largo plazo y uso de todos los dias.",
      recommendation: "Abre la calculadora de pedido y ensena pagos claros."
    },
    practical: {
      label: "Cliente practico",
      driver: "Quiere cocinar mas rapido, batallar menos y usarlo seguido.",
      script: "Habla de tiempo, facilidad, limpieza y comodidad diaria.",
      recommendation: "Muestra una demo rapida o una receta sencilla."
    },
    proof: {
      label: "Cliente que ocupa pruebas",
      driver: "Necesita confiar primero y ver pruebas antes de avanzar.",
      script: "No cierres duro. Habla con calma y ensena pruebas, garantia y evidencia.",
      recommendation: "Abre balance de decision o calidad del agua para darle seguridad."
    }
  };

  const syncBuyerProfileToggle = open => {
    wrap.hidden = !open;
    toggle.setAttribute("aria-expanded", open ? "true" : "false");
    toggle.textContent = open ? "Cerrar lectura" : "Abrir lectura";
  };

  const applyProfile = profile => {
    const safeProfile = profile || {
      label: "Sin lectura todavia.",
      driver: "Completa la lectura para verlo.",
      script: "Usa preguntas sencillas y escucha bien.",
      recommendation: "Abre esta lectura cuando notes interes pero no claridad."
    };

    if (mainNode) {
      mainNode.textContent = safeProfile.label;
    }
    if (driverNode) {
      driverNode.textContent = safeProfile.driver;
    }
    if (scriptNode) {
      scriptNode.textContent = safeProfile.script;
    }
    if (recommendationNode) {
      recommendationNode.textContent = safeProfile.recommendation;
    }
  };

  const analyze = () => {
    const scores = {
      family: 0,
      money: 0,
      practical: 0,
      proof: 0
    };

    const selects = Array.from(root.querySelectorAll("[data-buyer-profile-score]"));

    selects.forEach(select => {
      const group = select.dataset.buyerProfileGroup;
      const value = Number.parseInt(select.value || "0", 10) || 0;
      if (Object.prototype.hasOwnProperty.call(scores, group)) {
        scores[group] += value;
      }
    });

    const ranking = Object.entries(scores).sort((left, right) => right[1] - left[1]);
    const [topKey, topValue] = ranking[0] || [];
    const [, secondValue] = ranking[1] || [];

    if (!topKey || topValue <= 0) {
      applyProfile();
      return;
    }

    const topProfile = profiles[topKey];
    const isMixed = typeof secondValue === "number" && topValue - secondValue <= 1 && secondValue > 0;

    if (!isMixed) {
      applyProfile(topProfile);
      return;
    }

    applyProfile({
      label: `${topProfile.label} con mezcla`,
      driver: `${topProfile.driver} Tambien trae otra motivacion muy cerca, asi que conviene escuchar mas antes de empujar cierre.`,
      script: `${topProfile.script} Haz una pregunta mas para confirmar que es lo que de verdad pesa hoy.`,
      recommendation: topProfile.recommendation
    });
  };

  const reset = () => {
    root.querySelectorAll("[data-buyer-profile-score]").forEach(select => {
      select.value = "0";
    });
    applyProfile();
  };

  syncBuyerProfileToggle(false);
  applyProfile();

  toggle.addEventListener("click", () => {
    syncBuyerProfileToggle(wrap.hidden);
  });

  runButton?.addEventListener("click", analyze);
  resetButton?.addEventListener("click", reset);
}

function initLeadFormTool() {
  const wrap = document.querySelector("[data-lead-form-wrap]");
  const toggle = document.querySelector("[data-lead-form-toggle]");
  const frame = document.querySelector("[data-lead-form-frame]");
  const openLink = document.querySelector("[data-lead-form-open-link]");

  if (!wrap || !toggle || !frame) {
    return;
  }

  if (openLink) {
    openLink.href = GOOGLE_RAFFLE_FORM_URL;
  }

  const syncLeadFormToggle = open => {
    wrap.hidden = !open;
    toggle.setAttribute("aria-expanded", open ? "true" : "false");
    toggle.textContent = open ? "Cerrar rifa" : "Abrir rifa";
  };

  syncLeadFormToggle(false);

  toggle.addEventListener("click", () => {
    const willOpen = wrap.hidden;

    if (willOpen && !frame.getAttribute("src")) {
      frame.setAttribute("src", GOOGLE_RAFFLE_FORM_EMBED_URL);
    }

    syncLeadFormToggle(willOpen);
  });
}

function initLeadDestinationSettings(initialDestination = null) {
  const form = document.querySelector("[data-lead-destination-form]");
  const typeSelect = document.querySelector("[data-lead-destination-type]");
  const labelInput = document.querySelector("[data-lead-destination-label]");
  const urlInput = document.querySelector("[data-lead-destination-url]");
  const extraWrap = document.querySelector("[data-lead-destination-extra]");
  const currentNode = document.querySelector("[data-lead-destination-current]");
  const feedbackNode = document.querySelector("[data-lead-destination-feedback]");
  const saveButton = document.querySelector("[data-lead-destination-save]");

  if (!form || !typeSelect || !labelInput || !urlInput || !extraWrap || !currentNode) {
    return;
  }

  const buildSummary = destination => {
    const safeDestination = destination || {};
    const type = safeDestination.type || "carpeta_privada";
    const baseLabel = safeDestination.label || formatLeadDestinationLabel(type);

    if (type === "carpeta_privada") {
      return "Tus leads viven solo en tu carpeta privada por ahora.";
    }

    if (!safeDestination.url) {
      return `Tu destino actual es ${baseLabel}, pero todavia falta la URL.`;
    }

    return `Tus leads se guardan en tu carpeta y tambien se mandan a ${baseLabel}.`;
  };

  const syncExtraFields = () => {
    const type = typeSelect.value || "carpeta_privada";
    const needsExtra = type !== "carpeta_privada";
    extraWrap.hidden = !needsExtra;
    urlInput.required = needsExtra;
    labelInput.placeholder =
      type === "google_sheets" ? "Ej. Mi hoja de rifa" : "Ej. Mi GoHighLevel o Mi CRM";
  };

  const applyDestination = destination => {
    const safeDestination = destination || { type: "carpeta_privada", label: "", url: "" };
    typeSelect.value = safeDestination.type || "carpeta_privada";
    labelInput.value = safeDestination.label && safeDestination.type !== "carpeta_privada" ? safeDestination.label : "";
    urlInput.value = safeDestination.url || "";
    currentNode.textContent = buildSummary(safeDestination);
    syncExtraFields();
  };

  applyDestination(initialDestination);

  typeSelect.addEventListener("change", () => {
    syncExtraFields();
    clearMessage(feedbackNode);
  });

  form.addEventListener("submit", async event => {
    event.preventDefault();
    clearMessage(feedbackNode);
    setButtonLoading(saveButton, true, "Guardando...");

    try {
      const data = await apiRequest("/api/coach/lead-destination", {
        method: "PUT",
        body: {
          type: typeSelect.value,
          label: labelInput.value,
          url: urlInput.value
        }
      });

      applyDestination(data.destination);

      if (data.profile) {
        renderCoachProfile(data.profile);
      }

      setMessage(feedbackNode, "Destino guardado. Esta parte ya se siente mas tuya.", "success");
    } catch (error) {
      setMessage(feedbackNode, error.message, "error");
    } finally {
      setButtonLoading(saveButton, false);
    }
  });
}

function initCoachLeadWorkspace() {
  const captureWrap = document.querySelector("[data-native-lead-wrap]");
  const captureToggle = document.querySelector("[data-native-lead-toggle]");
  const captureForm = document.querySelector("[data-native-lead-form]");
  const captureFeedback = document.querySelector("[data-native-lead-feedback]");
  const folderWrap = document.querySelector("[data-lead-folder-wrap]");
  const folderToggle = document.querySelector("[data-lead-folder-toggle]");
  const leadList = document.querySelector("[data-coach-lead-list]");
  const leadListNote = document.querySelector("[data-coach-lead-list-note]");
  const exportButton = document.querySelector("[data-coach-leads-export]");
  const printButton = document.querySelector("[data-coach-leads-print]");
  const filterButtons = Array.from(document.querySelectorAll("[data-lead-filter]"));
  const totalNode = document.querySelector("[data-coach-leads-total]");
  const newNode = document.querySelector("[data-coach-leads-new]");
  const bookedNode = document.querySelector("[data-coach-leads-booked]");
  const clientsNode = document.querySelector("[data-coach-leads-clients]");

  if (!captureWrap || !captureToggle || !captureForm || !folderWrap || !folderToggle || !leadList) {
    return;
  }

  const state = {
    leads: [],
    filter: "todos"
  };

  const syncCaptureToggle = open => {
    captureWrap.hidden = !open;
    captureToggle.setAttribute("aria-expanded", open ? "true" : "false");
    captureToggle.textContent = open ? "Cerrar captura" : "Abrir captura";
  };

  const syncFolderToggle = open => {
    folderWrap.hidden = !open;
    folderToggle.setAttribute("aria-expanded", open ? "true" : "false");
    folderToggle.textContent = open ? "Cerrar carpeta" : "Abrir carpeta";
  };

  const getFilteredLeads = () => {
    if (state.filter === "todos") {
      return state.leads;
    }

    return state.leads.filter(lead => lead.status === state.filter);
  };

  const renderSummary = summary => {
    if (totalNode) totalNode.textContent = String(summary?.total || 0);
    if (newNode) newNode.textContent = String(summary?.nuevo || 0);
    if (bookedNode) bookedNode.textContent = String(summary?.agendado || 0);
    if (clientsNode) clientsNode.textContent = String(summary?.cliente || 0);
  };

  const renderLeadList = () => {
    const filteredLeads = getFilteredLeads();
    leadList.innerHTML = "";

    filterButtons.forEach(button => {
      button.classList.toggle("is-active", button.dataset.leadFilter === state.filter);
    });

    if (!filteredLeads.length) {
      leadList.innerHTML = `<div class="lead-folder-empty">No hay leads en este filtro todavia.</div>`;
      if (leadListNote) {
        leadListNote.textContent = "Captura uno nuevo o cambia el filtro para ver mas.";
      }
      return;
    }

    const fragment = document.createDocumentFragment();

    filteredLeads.forEach(lead => {
      const card = document.createElement("article");
      card.className = "lead-folder-item";
      card.dataset.coachLeadId = lead.id;

      const statusOptions = COACH_LEAD_STATUS_OPTIONS.map(option => {
        const selected = option.value === lead.status ? " selected" : "";
        return `<option value="${option.value}"${selected}>${option.label}</option>`;
      }).join("");
      const nextActionOptions = COACH_LEAD_NEXT_ACTION_OPTIONS.map(option => {
        const selected = option.value === (lead.nextAction || "") ? " selected" : "";
        return `<option value="${option.value}"${selected}>${option.label}</option>`;
      }).join("");

      const phoneHref = normalizeLeadPhone(lead.phone);
      const dateCopy = formatDate(lead.createdAt);
      const nextActionCopy = lead.nextAction ? formatLeadNextActionLabel(lead.nextAction) : "Sin proxima accion";
      const nextActionAtCopy = lead.nextActionAt ? formatDateTime(lead.nextActionAt) : "Sin fecha";
      const metaChips = [
        lead.phone ? `<span class="lead-folder-meta-chip">${escapeHtml(formatLeadPhone(lead.phone))}</span>` : "",
        lead.email ? `<span class="lead-folder-meta-chip">${escapeHtml(lead.email)}</span>` : "",
        lead.interest ? `<span class="lead-folder-meta-chip">${escapeHtml(lead.interest)}</span>` : "",
        lead.source ? `<span class="lead-folder-meta-chip">${escapeHtml(formatLeadSourceLabel(lead.source))}</span>` : "",
        lead.city ? `<span class="lead-folder-meta-chip">${escapeHtml(lead.city)}</span>` : "",
        lead.zipCode ? `<span class="lead-folder-meta-chip">ZIP ${escapeHtml(lead.zipCode)}</span>` : ""
      ]
        .filter(Boolean)
        .join("");

      card.innerHTML = `
        <div class="lead-folder-head">
          <div>
            <strong>${escapeHtml(lead.fullName || "Nombre pendiente")}</strong>
            <span>Guardado ${escapeHtml(dateCopy)}</span>
          </div>
          <span class="lead-status-badge">${escapeHtml(formatLeadStatusLabel(lead.status))}</span>
        </div>
        <p class="lead-folder-copy">${escapeHtml(lead.summary || "Sin resumen todavia.")}</p>
        <div class="lead-folder-meta">${metaChips}</div>
        <div class="lead-folder-followup">
          <div class="lead-folder-followup-card">
            <span>Proxima accion</span>
            <strong>${escapeHtml(nextActionCopy)}</strong>
          </div>
          <div class="lead-folder-followup-card">
            <span>Cuando</span>
            <strong>${escapeHtml(nextActionAtCopy)}</strong>
          </div>
        </div>
        <div class="lead-folder-actions-row">
          ${
            phoneHref
              ? `<a class="secondary-button" href="tel:+1${phoneHref}">Llamar</a>
                 <a class="nav-button" href="sms:+1${phoneHref}">SMS</a>`
              : ""
          }
          ${
            lead.email
              ? `<a class="nav-button" href="mailto:${encodeURIComponent(lead.email)}">Correo</a>`
              : ""
          }
        </div>
        <div class="lead-folder-followup-grid">
          <label class="lead-folder-field">
            <span>Proxima accion</span>
            <select data-coach-lead-next-action>
              ${nextActionOptions}
            </select>
          </label>
          <label class="lead-folder-field">
            <span>Dia</span>
            <input type="date" value="${escapeHtml(formatDateInputValue(lead.nextActionAt))}" data-coach-lead-date />
          </label>
          <label class="lead-folder-field">
            <span>Hora</span>
            <input type="time" value="${escapeHtml(formatTimeInputValue(lead.nextActionAt))}" data-coach-lead-time />
          </label>
          <label class="lead-folder-field lead-folder-field-full">
            <span>Nota rapida</span>
            <input type="text" maxlength="180" placeholder="Ej. prefiere despues de las 6 pm" data-coach-lead-note />
          </label>
        </div>
        <div class="lead-folder-actions-row lead-folder-status-row">
          <select data-coach-lead-status>
            ${statusOptions}
          </select>
          <button type="button" class="secondary-button" data-coach-lead-save>Guardar seguimiento</button>
        </div>
      `;

      fragment.appendChild(card);
    });

    leadList.appendChild(fragment);

    if (leadListNote) {
      leadListNote.textContent = `${filteredLeads.length} lead(s) en este filtro.`;
    }
  };

  const exportLeads = () => {
    const rows = [
      [
        "nombre",
        "telefono",
        "email",
        "ciudad",
        "zip_code",
        "interes",
        "fuente",
        "status",
        "proxima_accion",
        "proxima_fecha",
        "notas",
        "fecha"
      ]
    ];

    getFilteredLeads().forEach(lead => {
      rows.push([
        lead.fullName || "",
        lead.phone || "",
        lead.email || "",
        lead.city || "",
        lead.zipCode || "",
        lead.interest || "",
        formatLeadSourceLabel(lead.source),
        formatLeadStatusLabel(lead.status),
        formatLeadNextActionLabel(lead.nextAction || ""),
        lead.nextActionAt ? new Date(lead.nextActionAt).toISOString() : "",
        lead.notes || "",
        lead.createdAt ? new Date(lead.createdAt).toISOString() : ""
      ]);
    });

    const csv = rows
      .map(row =>
        row
          .map(value => `"${String(value || "").replace(/"/g, '""')}"`)
          .join(",")
      )
      .join("\n");

    const blob = new Blob([csv], { type: "text/csv;charset=utf-8;" });
    const url = URL.createObjectURL(blob);
    const link = document.createElement("a");
    link.href = url;
    link.download = "coach-leads.csv";
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
    URL.revokeObjectURL(url);
  };

  const printLeads = () => {
    const filteredLeads = getFilteredLeads();

    if (!filteredLeads.length) {
      if (leadListNote) {
        leadListNote.textContent = "No hay leads en este filtro para imprimir.";
      }
      return;
    }

    const printWindow = window.open("", "_blank", "width=920,height=760");

    if (!printWindow) {
      return;
    }

    const rows = filteredLeads
      .map(
        lead => `
          <tr>
            <td>${escapeHtml(lead.fullName || "")}</td>
            <td>${escapeHtml(formatLeadPhone(lead.phone || ""))}</td>
            <td>${escapeHtml(lead.interest || "")}</td>
            <td>${escapeHtml(formatLeadStatusLabel(lead.status))}</td>
            <td>${escapeHtml(formatLeadSourceLabel(lead.source))}</td>
            <td>${escapeHtml(formatLeadNextActionLabel(lead.nextAction || ""))}</td>
            <td>${escapeHtml(lead.nextActionAt ? formatDateTime(lead.nextActionAt) : "")}</td>
            <td>${escapeHtml(lead.notes || "")}</td>
          </tr>
        `
      )
      .join("");

    printWindow.document.write(`
      <html>
        <head>
          <title>Mis leads</title>
          <style>
            body { font-family: Arial, sans-serif; padding: 24px; color: #111827; }
            h1 { margin: 0 0 16px; }
            table { width: 100%; border-collapse: collapse; }
            th, td { border: 1px solid #d1d5db; padding: 10px; text-align: left; vertical-align: top; }
            th { background: #f3f4f6; }
          </style>
        </head>
        <body>
          <h1>Mis leads</h1>
          <table>
            <thead>
              <tr>
                <th>Nombre</th>
                <th>Telefono</th>
                <th>Interes</th>
                <th>Status</th>
                <th>Fuente</th>
                <th>Proxima accion</th>
                <th>Cuando</th>
                <th>Notas</th>
              </tr>
            </thead>
            <tbody>${rows}</tbody>
          </table>
        </body>
      </html>
    `);
    printWindow.document.close();
    printWindow.focus();
    window.setTimeout(() => {
      printWindow.print();
    }, 250);
  };

  const loadLeads = async () => {
    const data = await apiRequest("/api/coach/leads");
    state.leads = Array.isArray(data.leads) ? data.leads : [];
    renderSummary(data.summary || {});
    renderLeadList();
  };

  syncCaptureToggle(false);
  syncFolderToggle(false);

  captureToggle.addEventListener("click", () => {
    syncCaptureToggle(captureWrap.hidden);
  });

  folderToggle.addEventListener("click", async () => {
    const willOpen = folderWrap.hidden;
    syncFolderToggle(willOpen);

    if (willOpen) {
      await loadLeads().catch(() => {
        if (leadListNote) {
          leadListNote.textContent = "No pude cargar tu carpeta de leads.";
        }
      });
    }
  });

  captureForm.addEventListener("submit", async event => {
    event.preventDefault();
    clearMessage(captureFeedback);

    const submitButton = captureForm.querySelector('button[type="submit"]');
    const formData = new FormData(captureForm);
    const payload = {
      fullName: formData.get("fullName"),
      phone: formData.get("phone"),
      email: formData.get("email"),
      city: formData.get("city"),
      zipCode: formData.get("zipCode"),
      interest: formData.get("interest"),
      source: formData.get("source"),
      notes: formData.get("notes"),
      consentGiven: formData.get("consentGiven") === "on"
    };

    setButtonLoading(submitButton, true, "Guardando...");

    try {
      const data = await apiRequest("/api/coach/leads", {
        method: "POST",
        body: payload
      });

      const deliveryCopy = data.delivery?.attempted
        ? data.delivery?.delivered
          ? " Tambien lo mande a tu destino."
          : " Lo guarde, pero no pude mandarlo a tu destino todavia."
        : "";

      setMessage(
        captureFeedback,
        data.duplicate
          ? `Este lead ya existia. Lo actualice en tu carpeta.${deliveryCopy}`
          : `Lead guardado en tu carpeta privada.${deliveryCopy}`,
        "success"
      );
      captureForm.reset();
      await loadLeads();
      syncFolderToggle(true);
    } catch (error) {
      setMessage(captureFeedback, error.message, "error");
    } finally {
      setButtonLoading(submitButton, false);
    }
  });

  captureForm.addEventListener("reset", () => {
    clearMessage(captureFeedback);
  });

  filterButtons.forEach(button => {
    button.addEventListener("click", () => {
      state.filter = button.dataset.leadFilter || "todos";
      renderLeadList();
    });
  });

  leadList.addEventListener("click", async event => {
    const saveButton = event.target.closest("[data-coach-lead-save]");

    if (!saveButton) {
      return;
    }

    const card = saveButton.closest("[data-coach-lead-id]");
    const leadId = card?.dataset.coachLeadId || "";
    const statusSelect = card?.querySelector("[data-coach-lead-status]");
    const nextActionSelect = card?.querySelector("[data-coach-lead-next-action]");
    const nextDateInput = card?.querySelector("[data-coach-lead-date]");
    const nextTimeInput = card?.querySelector("[data-coach-lead-time]");
    const noteInput = card?.querySelector("[data-coach-lead-note]");
    const nextStatus = statusSelect?.value || "nuevo";
    const nextAction = nextActionSelect?.value || "";
    const nextActionAt = buildLeadDateTimeValue(nextDateInput?.value || "", nextTimeInput?.value || "");
    const noteToAppend = noteInput?.value || "";

    if (!leadId) {
      return;
    }

    setButtonLoading(saveButton, true, "Guardando...");

    try {
      await apiRequest(`/api/coach/leads/${encodeURIComponent(leadId)}`, {
        method: "PATCH",
        body: {
          status: nextStatus,
          nextAction,
          nextActionAt,
          notes: noteToAppend
        }
      });
      await loadLeads();
    } catch (error) {
      if (leadListNote) {
        leadListNote.textContent = error.message;
      }
    } finally {
      setButtonLoading(saveButton, false);
    }
  });

  exportButton?.addEventListener("click", exportLeads);
  printButton?.addEventListener("click", printLeads);
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
  initCoachWorkspaceTabs();
  initLeadDestinationSettings(me.profile?.leadDestination || null);
  initLeadFormTool();
  initCoachLeadWorkspace();
  initOrderCalculator();
  initDecisionTool();
  initBuyerProfileTool();

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
  const royalOneToggle = document.querySelector("[data-royalone-toggle]");
  const royalOnePanel = document.querySelector("[data-royalone-panel]");
  const royalOneCancelButton = document.querySelector("[data-royalone-cancel]");
  const royalOneFeedback = document.querySelector("[data-royalone-feedback]");
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

  const syncRoyalOneDock = open => {
    if (royalOnePanel) {
      royalOnePanel.hidden = !open;
    }

    if (royalOneToggle) {
      royalOneToggle.setAttribute("aria-expanded", open ? "true" : "false");
      royalOneToggle.textContent = open ? "Cerrar RoyalOne" : "RoyalOne";
    }
  };

  syncRoyalOneDock(false);

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
      return;
    }

    if (event.key === "Escape" && royalOnePanel && !royalOnePanel.hidden) {
      syncRoyalOneDock(false);
    }
  });

  royalOneToggle?.addEventListener("click", () => {
    if (royalOneFeedback) {
      royalOneFeedback.textContent = "Pensado para tenerlo a la mano sin robar espacio dentro del Coach.";
    }
    syncRoyalOneDock(royalOnePanel?.hidden ?? true);
  });

  document.addEventListener("click", event => {
    if (!royalOnePanel || !royalOneToggle || royalOnePanel.hidden) {
      return;
    }

    const dock = document.querySelector("[data-royalone-dock]");

    if (dock && !dock.contains(event.target)) {
      syncRoyalOneDock(false);
    }
  });

  royalOneCancelButton?.addEventListener("click", () => {
    syncRoyalOneDock(false);
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
