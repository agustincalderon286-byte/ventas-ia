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
const COACH_WORKSPACE_TAB_KEY = "agustin-coach-workspace-tab";
const COACH_ACTIVE_HEALTH_SURVEY_KEY = "agustin-coach-active-health-survey";
const COACH_ACTIVE_PROGRAM_414_KEY = "agustin-coach-active-program-414";
const COACH_ACTIVE_ORDER_CALC_KEY = "agustin-coach-active-order-calc";
const COACH_ACTIVE_DECISION_KEY = "agustin-coach-active-decision";
const COACH_ACTIVE_DEMO_OUTCOME_KEY = "agustin-coach-active-demo-outcome";
const COACH_DAILY_PRIZE_KEY = "agustin-coach-daily-prize";
const COACH_DEMO_STAGE_KEY = "agustin-coach-demo-stage";
const COACH_DEMO_EVENTS_KEY = "agustin-coach-demo-events";
const DAILY_PRIZE_DURATION_MS = 5 * 60 * 1000;
const COACH_DEMO_STAGE_CONFIG = [
  {
    id: "rompe_hielo",
    label: "Rompe hielo",
    copy: "Conecta primero y deja que la conversacion agarre confianza.",
    coachReply:
      "Rompe de hielo activo. Habla de donde son, en que trabajan, cuantos anos tienen aqui y solo busca conectar. Todavia no cierres ni expliques todo."
  },
  {
    id: "entrega_regalo",
    label: "Entrega regalo",
    copy: "Cumple primero y usa el regalo como puente natural para lo que sigue.",
    coachReply:
      "Entrega de regalo activa. Cumple primero, crea confianza y conecta ese regalo con el programa de regalos para abrir la siguiente parte."
  },
  {
    id: "programa_4_14",
    label: "Programa 4 en 14",
    copy: "Pide referencias con claridad y deja bien explicado como se gana el regalo.",
    coachReply:
      "Programa 4 en 14 activo. Pide hasta 10 referencias y recuerda que el regalo se gana si logran 4 demos en 14 dias."
  },
  {
    id: "cita_instantanea",
    label: "Cita instantanea",
    copy: "Aqui solo buscas dia y hora. La meta es apartar la cita.",
    coachReply:
      "Cita instantanea activa. Haz que el anfitrion diga hola y te pase el telefono. Tu solo busca dia y hora, no vendas producto."
  },
  {
    id: "encuesta_salud",
    label: "Encuesta",
    copy: "Recoge informacion util para personalizar mejor la presentacion y el cierre.",
    coachReply:
      "Encuesta de salud activa. Llenala simple, escucha mucho y deja que sus respuestas te preparen los cierres de despues."
  },
  {
    id: "demo_producto",
    label: "Demo",
    copy: "Aqui presentas productos, pruebas y valor segun lo que mas le sirve a esa casa.",
    coachReply:
      "Demo activa. Ensena catalogo, uso diario, pruebas de agua y conecta lo visto con lo que mas les serviria en su casa."
  },
  {
    id: "cierre_final",
    label: "Cierre",
    copy: "Aqui ya puedes usar todas las herramientas para pedir decision con claridad.",
    coachReply:
      "Cierre activo. Ya puedes usar calculadora, balance, descuentos, regalos y apoyo del jefe para pedir decision con claridad."
  },
  {
    id: "invitacion_negocio",
    label: "Invitacion al negocio",
    copy: "Ultimo paso para presentar la oportunidad solo si viste apertura real.",
    coachReply:
      "Invitacion al negocio activa. Ofrece la oportunidad con calma y segun la apertura que viste en la casa, sin mezclarla temprano con la venta principal."
  }
];
const DAILY_PRIZE_OFFERS = {
  "305": {
    code: "305",
    type: "discount",
    title: "Descuento especial",
    copy: "Activalo solo cuando ya viste interes real y quieras ayudarles a tomar la decision hoy.",
    note: "Descuento real disponible solo durante esta activacion."
  },
  "14407": {
    code: "14407",
    type: "gift",
    title: "Regalo especial",
    copy: "Usalo cuando la compra ya va en serio y quieras cerrar con valor extra, no como gancho temprano.",
    note: "Regalo valido para compra arriba de $2500 mientras esta activacion siga viva."
  }
};
const FOURTEEN_SHEET_DEFAULT_REFERRALS = 4;
const FOURTEEN_SHEET_MAX_REFERRALS = 11;
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
  programa_4_en_14: "Programa 4 en 14",
  chef_personal: "Chef personal",
  llamada: "Llamada",
  demo: "Demo",
  referencia: "Referencia",
  evento: "Evento",
  otro: "Otro"
};
const COACH_LEAD_DESTINATION_LABELS = {
  carpeta_privada: "Solo mi carpeta privada",
  correo_personal: "Mi correo personal",
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
const DAILY_PRIZE_DEFAULT_STATE = {
  code: "",
  offerCode: "",
  status: "idle",
  startedAt: "",
  expiresAt: "",
  claimedAt: ""
};

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

function hasCoachAccess(user = null) {
  return Boolean(user && (user.accessGranted || user.subscriptionActive));
}

function getCoachEffectiveOwnerId(user = null) {
  return String(user?.ownerUserId || user?.teamOwnerUserId || user?.id || "").trim();
}

function syncCoachManagerUi(user = null) {
  const canManageTeam = Boolean(user?.managesTeam);

  document.querySelectorAll("[data-team-manager-only]").forEach(node => {
    if (!canManageTeam) {
      node.hidden = true;
      return;
    }

    if (node.dataset.coachWorkspaceSection) {
      node.hidden = true;
      return;
    }

    node.hidden = false;
  });
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

function sanitizePin4(value = "") {
  return String(value || "")
    .replace(/\D/g, "")
    .slice(0, 4);
}

function readFileAsDataUrl(file) {
  return new Promise((resolve, reject) => {
    const reader = new FileReader();
    reader.onload = () => resolve(String(reader.result || ""));
    reader.onerror = () => reject(new Error("No pude leer ese archivo."));
    reader.readAsDataURL(file);
  });
}

function sanitizeZipCode(value = "") {
  return String(value || "").replace(/\D/g, "").slice(0, 5);
}

function getCoachDemoStageMeta(stageId = "") {
  return (
    COACH_DEMO_STAGE_CONFIG.find(item => item.id === String(stageId || "").trim()) ||
    COACH_DEMO_STAGE_CONFIG[0]
  );
}

function getCoachDemoStageId() {
  try {
    return getCoachDemoStageMeta(window.sessionStorage.getItem(COACH_DEMO_STAGE_KEY) || "").id;
  } catch (error) {
    return COACH_DEMO_STAGE_CONFIG[0].id;
  }
}

function sanitizeCoachDemoEvent(event = null) {
  if (!event || typeof event !== "object") {
    return null;
  }

  const id = String(event.id || "").trim().slice(0, 80);
  const label = String(event.label || "").trim().slice(0, 120);
  const detail = String(event.detail || "").trim().slice(0, 220);
  const occurredAt = String(event.occurredAt || new Date().toISOString()).trim();

  if (!label) {
    return null;
  }

  return {
    id,
    label,
    detail,
    occurredAt
  };
}

function getCoachDemoEvents() {
  try {
    const raw = window.sessionStorage.getItem(COACH_DEMO_EVENTS_KEY);
    const parsed = raw ? JSON.parse(raw) : [];
    return Array.isArray(parsed) ? parsed.map(sanitizeCoachDemoEvent).filter(Boolean).slice(0, 8) : [];
  } catch (error) {
    return [];
  }
}

function syncCoachDemoContextBroadcast() {
  window.dispatchEvent(
    new CustomEvent("coach:demo-sync", {
      detail: {
        stageId: getCoachDemoStageId(),
        events: getCoachDemoEvents()
      }
    })
  );
}

function setCoachDemoStageId(stageId = "") {
  const safeStage = getCoachDemoStageMeta(stageId).id;
  window.sessionStorage.setItem(COACH_DEMO_STAGE_KEY, safeStage);
  syncCoachDemoContextBroadcast();
}

function registerCoachDemoEvent(event = null) {
  const safeEvent = sanitizeCoachDemoEvent(event);

  if (!safeEvent) {
    return;
  }

  const current = getCoachDemoEvents();
  const [latest] = current;
  const isRepeat =
    latest &&
    latest.id === safeEvent.id &&
    latest.label === safeEvent.label &&
    latest.detail === safeEvent.detail;

  const nextEvents = isRepeat ? current : [safeEvent, ...current].slice(0, 8);
  window.sessionStorage.setItem(COACH_DEMO_EVENTS_KEY, JSON.stringify(nextEvents));
  syncCoachDemoContextBroadcast();
}

function initCoachWorkspaceTabs() {
  const tabButtons = Array.from(document.querySelectorAll("[data-coach-workspace-tab]"));
  const workspaceSections = Array.from(document.querySelectorAll("[data-coach-workspace-section]"));

  if (!tabButtons.length || !workspaceSections.length) {
    return;
  }

  const visibleTabButtons = tabButtons.filter(button => !button.hidden);
  const validTabs = new Set(visibleTabButtons.map(button => button.dataset.coachWorkspaceTab).filter(Boolean));
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
      const nextTab = button.dataset.coachWorkspaceTab || "cierre";
      syncWorkspace(nextTab);
      const labelMeta =
        {
          prospeccion: {
            label: "Cambio a modo prospeccion",
            detail: "Entraste al lado de captar leads y trabajar calle."
          },
          equipo: {
            label: "Cambio a modo equipo",
            detail: "Entraste al panel para mover subcuentas y revisar produccion del equipo."
          },
          cierre: {
            label: "Cambio a modo cierre",
            detail: "Entraste al lado de demo, objeciones y cierre."
          }
        }[nextTab] || {
          label: "Cambio de area",
          detail: "Entraste a otra area del Coach."
        };

      registerCoachDemoEvent({
        id: `workspace_${nextTab}`,
        label: labelMeta.label,
        detail: labelMeta.detail
      });
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

function buildAbsoluteAppUrl(pathOrUrl = "") {
  const safeValue = String(pathOrUrl || "").trim();

  if (!safeValue) {
    return "";
  }

  if (/^https?:\/\//i.test(safeValue)) {
    return safeValue;
  }

  const normalizedPath = safeValue.startsWith("/") ? safeValue : `/${safeValue}`;
  return `${window.location.origin}${normalizedPath}`;
}

function buildChefQrImageUrl(pathOrUrl = "") {
  const absoluteUrl = buildAbsoluteAppUrl(pathOrUrl);

  if (!absoluteUrl) {
    return "/chef/share-qr.svg";
  }

  return `https://api.qrserver.com/v1/create-qr-code/?size=280x280&data=${encodeURIComponent(absoluteUrl)}`;
}

function escapeHtml(value = "") {
  return String(value || "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#39;");
}

function getCheckedValues(root, fieldName) {
  return Array.from(root.querySelectorAll(`input[name="${fieldName}"]:checked`))
    .map(input => String(input.value || "").trim())
    .filter(Boolean);
}

function setCheckedValues(root, fieldName, values = []) {
  const selected = new Set((Array.isArray(values) ? values : []).map(item => String(item || "").trim()));
  root.querySelectorAll(`input[name="${fieldName}"]`).forEach(input => {
    input.checked = selected.has(String(input.value || "").trim());
  });
}

function setNamedFieldValue(form, fieldName, value) {
  const field = form.elements.namedItem(fieldName);

  if (!field) {
    return;
  }

  if (typeof field.value !== "undefined") {
    field.value = value ?? "";
  }
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
        <label class="order-calc-field order-calc-field-name">
          <span>Nombre del producto</span>
          <input type="text" data-order-calc-name placeholder="Ej. Olla de presion" autocomplete="off" />
          <div class="order-calc-match" data-order-calc-match>Escribe nombre o codigo para sugerir precio base.</div>
          <div class="order-calc-suggestions" data-order-calc-suggestions hidden></div>
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
  const submitContextButton = root.querySelector("[data-order-calc-submit-context]");
  const catalogSearchCache = new Map();

  const normalizeCatalogLookupText = value =>
    String(value || "")
      .toLowerCase()
      .normalize("NFD")
      .replace(/[\u0300-\u036f]/g, "")
      .replace(/[^a-z0-9ñ]+/g, " ")
      .trim();

  const setProgrammaticInputValue = (input, value, source, sourceKey = "orderCalcSource") => {
    if (!input) {
      return;
    }

    input.dataset.skipManualMark = "true";
    input.value = value ?? "";
    input.dataset[sourceKey] = source;
    window.requestAnimationFrame(() => {
      delete input.dataset.skipManualMark;
    });
  };

  const getProductNodes = card => ({
    nameInput: card.querySelector("[data-order-calc-name]"),
    priceInput: card.querySelector("[data-order-calc-price]"),
    matchNode: card.querySelector("[data-order-calc-match]"),
    suggestionsNode: card.querySelector("[data-order-calc-suggestions]")
  });

  const setMatchMessage = (card, message = "", tone = "") => {
    const { matchNode } = getProductNodes(card);

    if (!matchNode) {
      return;
    }

    matchNode.textContent = message || "Escribe nombre o codigo para sugerir precio base.";
    matchNode.dataset.tone = tone || "neutral";
  };

  const hideSuggestions = card => {
    const { suggestionsNode } = getProductNodes(card);

    if (!suggestionsNode) {
      return;
    }

    suggestionsNode.hidden = true;
    suggestionsNode.innerHTML = "";
  };

  const applyCatalogMatch = (card, match, options = {}) => {
    const { nameInput, priceInput } = getProductNodes(card);

    if (!match || !nameInput || !priceInput) {
      return;
    }

    const shouldReplaceName = options.replaceName !== false;
    const shouldReplacePrice =
      options.forcePrice ||
      !String(priceInput.value || "").trim() ||
      priceInput.dataset.orderCalcPriceSource === "catalog";

    if (shouldReplaceName) {
      setProgrammaticInputValue(nameInput, match.nombre_producto || "", options.nameSource || "catalog");
    }

    if (shouldReplacePrice && Number.isFinite(Number(match.precio_base_catalogo))) {
      setProgrammaticInputValue(
        priceInput,
        String(match.precio_base_catalogo),
        options.priceSource || "catalog",
        "orderCalcPriceSource"
      );
    }

    card.dataset.orderCalcCatalogCode = String(match.codigo_producto || "").trim();
    card.dataset.orderCalcCatalogName = String(match.nombre_producto || "").trim();

    setMatchMessage(
      card,
      `${match.codigo_producto || "Sin codigo"} · Base ${formatMoney(Number(match.precio_base_catalogo) || 0)}`,
      "success"
    );
    hideSuggestions(card);
    recalculate();
  };

  const searchCatalog = async rawQuery => {
    const query = String(rawQuery || "").trim();

    if (query.length < 3) {
      return [];
    }

    const cacheKey = normalizeCatalogLookupText(query);
    if (catalogSearchCache.has(cacheKey)) {
      return catalogSearchCache.get(cacheKey);
    }

    const data = await apiRequest(`/api/coach/catalog-prices?query=${encodeURIComponent(query)}`);
    const matches = Array.isArray(data.matches) ? data.matches : [];
    catalogSearchCache.set(cacheKey, matches);
    return matches;
  };

  const renderSuggestions = (card, matches = []) => {
    const { suggestionsNode } = getProductNodes(card);

    if (!suggestionsNode) {
      return;
    }

    if (!matches.length) {
      hideSuggestions(card);
      return;
    }

    suggestionsNode.innerHTML = matches
      .map(
        (match, index) => `
          <button
            type="button"
            class="order-calc-suggestion${index === 0 ? " is-top" : ""}"
            data-order-calc-suggestion="${escapeHtml(match.codigo_producto || match.nombre_producto || "")}"
          >
            <strong>${escapeHtml(match.nombre_producto || "Producto sin nombre")}</strong>
            <span>${escapeHtml(match.codigo_producto || "Sin codigo")} · Base ${escapeHtml(
              formatMoney(Number(match.precio_base_catalogo) || 0)
            )}</span>
          </button>
        `
      )
      .join("");
    suggestionsNode.hidden = false;
    suggestionsNode._matches = matches;
  };

  const tryAutofillCatalogFromName = async (card, query, options = {}) => {
    const safeQuery = String(query || "").trim();

    if (safeQuery.length < 3) {
      if (options.clearWhenShort) {
        hideSuggestions(card);
        setMatchMessage(card, "Escribe nombre o codigo para sugerir precio base.");
      }
      return;
    }

    try {
      const matches = await searchCatalog(safeQuery);

      if (!matches.length) {
        renderSuggestions(card, []);
        setMatchMessage(card, "No encontre un match claro. Prueba con codigo o nombre mas exacto.", "warning");
        return;
      }

      renderSuggestions(card, matches);

      const queryNormalized = normalizeCatalogLookupText(safeQuery);
      const topMatch = matches[0];
      const topName = normalizeCatalogLookupText(topMatch.nombre_producto || "");
      const topCode = normalizeCatalogLookupText(topMatch.codigo_producto || "");
      const exactEnough =
        queryNormalized === topName ||
        queryNormalized === topCode ||
        (matches.length === 1 && (topName.includes(queryNormalized) || queryNormalized.includes(topName)));

      if (options.autoApplyTop && exactEnough) {
        applyCatalogMatch(card, topMatch, {
          replaceName: options.replaceName,
          forcePrice: options.forcePrice,
          nameSource: options.nameSource || "catalog",
          priceSource: options.priceSource || "catalog"
        });
        return;
      }

      setMatchMessage(card, "Elige una opcion para llenar nombre y precio base.", "neutral");
    } catch (error) {
      hideSuggestions(card);
      setMatchMessage(card, "No pude revisar el catalogo ahorita.", "warning");
    }
  };

  const syncProductsFromSurvey = context => {
    const safeContext = context?.id ? context : getActiveCoachHealthSurveyContext();
    const topProducts = Array.isArray(safeContext?.topProducts)
      ? safeContext.topProducts.map(item => String(item || "").trim()).filter(Boolean)
      : [];

    if (!topProducts.length) {
      return;
    }

    const lastSurveyId = root.dataset.orderCalcSurveyId || "";
    const nextSurveyId = safeContext?.id || "";
    const shouldRefreshAll = Boolean(nextSurveyId) && nextSurveyId !== lastSurveyId;

    productCards.forEach((card, index) => {
      const { nameInput, priceInput } = getProductNodes(card);

      if (!nameInput || !priceInput) {
        return;
      }

      const nextName = topProducts[index] || "";
      const currentValue = String(nameInput.value || "").trim();
      const isAutofill = nameInput.dataset.orderCalcSource === "survey";

      if (!nextName) {
        if (shouldRefreshAll && isAutofill) {
          setProgrammaticInputValue(nameInput, "", "survey");
          delete nameInput.dataset.orderCalcSource;
          if (priceInput.dataset.orderCalcPriceSource === "catalog") {
            setProgrammaticInputValue(priceInput, "", "catalog", "orderCalcPriceSource");
            delete priceInput.dataset.orderCalcPriceSource;
          }
        }
        return;
      }

      if (shouldRefreshAll || !currentValue || isAutofill) {
        setProgrammaticInputValue(nameInput, nextName, "survey");
        tryAutofillCatalogFromName(card, nextName, {
          autoApplyTop: true,
          replaceName: false,
          forcePrice: !String(priceInput.value || "").trim() || priceInput.dataset.orderCalcPriceSource === "catalog",
          nameSource: "survey",
          priceSource: "catalog"
        });
      }
    });

    if (nextSurveyId) {
      root.dataset.orderCalcSurveyId = nextSurveyId;
    }
  };

  const buildCurrentOrderCalcContext = () => {
    const products = [];
    let totalGeneral = 0;
    let totalDown = 0;
    let totalDiscount = 0;

    productCards.forEach((card, index) => {
      const { nameInput } = getProductNodes(card);
      const name = String(nameInput?.value || "").trim();
      const code = String(card.dataset.orderCalcCatalogCode || "").trim();
      const basePrice = readCalcNumber(card.querySelector("[data-order-calc-price]"));
      const down = readCalcNumber(card.querySelector("[data-order-calc-down]"));
      const descFixed = readCalcNumber(card.querySelector("[data-order-calc-desc-fixed]"));
      const descPercent = readCalcNumber(card.querySelector("[data-order-calc-desc-percent]"));
      const tax = basePrice * 0.1;
      const shipping = basePrice * 0.05;
      const grossTotal = basePrice + tax + shipping;
      const discountAmount = descFixed + grossTotal * (descPercent / 100);
      const balance = Math.max(0, grossTotal - down - discountAmount);
      const monthly = balance * 0.05;
      const weekly = monthly / 4;
      const daily = weekly / 7;

      totalGeneral += grossTotal;
      totalDown += down;
      totalDiscount += discountAmount;

      if (
        name ||
        code ||
        basePrice > 0 ||
        down > 0 ||
        discountAmount > 0 ||
        balance > 0
      ) {
        products.push({
          slot: index + 1,
          name,
          code,
          basePrice,
          grossTotal,
          downPayment: down,
          discountAmount,
          balance,
          monthly,
          weekly,
          daily
        });
      }
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

    return buildCoachOrderCalcContext({
      ownerUserId: root.dataset.orderCalcOwnerUserId || "",
      products,
      summary: {
        totalGross: totalGeneral,
        totalDown: finalDown,
        totalDiscount: finalDiscount,
        balanceFinal: finalBalance,
        monthly: finalMonthly,
        weekly: finalWeekly,
        daily: finalDaily,
        extraDown,
        extraDescFixed,
        extraDescPercent
      }
    });
  };

  const syncOrderCalcCoachState = () => {
    const activeContext = getActiveCoachOrderCalcContext();
    const draftContext = buildCurrentOrderCalcContext();

    root._orderCalcDraftContext = draftContext;

    if (!activeContext?.signature) {
      renderActiveCoachOrderCalcContext(null);
      return;
    }

    if (draftContext?.signature && draftContext.signature !== activeContext.signature) {
      renderActiveCoachOrderCalcContext({
        ...activeContext,
        isDirty: true
      });
      return;
    }

    renderActiveCoachOrderCalcContext(activeContext);
  };

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

    syncOrderCalcCoachState();
  }

  root.querySelectorAll("input").forEach(input => {
    input.addEventListener("input", recalculate);
  });

  productCards.forEach(card => {
    const { nameInput, priceInput, suggestionsNode } = getProductNodes(card);

    nameInput?.addEventListener("input", () => {
      if (nameInput.dataset.skipManualMark === "true") {
        return;
      }

      nameInput.dataset.orderCalcSource = "manual";
      delete card.dataset.orderCalcCatalogCode;
      delete card.dataset.orderCalcCatalogName;
      window.clearTimeout(nameInput._lookupTimer);
      const query = String(nameInput.value || "").trim();

      if (query.length < 3) {
        hideSuggestions(card);
        setMatchMessage(card, "Escribe nombre o codigo para sugerir precio base.");
        return;
      }

      nameInput._lookupTimer = window.setTimeout(() => {
        tryAutofillCatalogFromName(card, query, {
          autoApplyTop: false,
          replaceName: false,
          forcePrice: false
        });
      }, 260);
    });

    nameInput?.addEventListener("blur", () => {
      const query = String(nameInput.value || "").trim();

      if (query.length >= 3) {
        tryAutofillCatalogFromName(card, query, {
          autoApplyTop: true,
          replaceName: true,
          forcePrice: false
        });
      }

      window.setTimeout(() => {
        hideSuggestions(card);
      }, 120);
    });

    priceInput?.addEventListener("input", () => {
      if (priceInput.dataset.skipManualMark === "true") {
        return;
      }

      priceInput.dataset.orderCalcPriceSource = "manual";
    });

    suggestionsNode?.addEventListener("mousedown", event => {
      event.preventDefault();
    });

    suggestionsNode?.addEventListener("click", event => {
      const button = event.target.closest("[data-order-calc-suggestion]");

      if (!button) {
        return;
      }

      const matches = Array.isArray(suggestionsNode._matches) ? suggestionsNode._matches : [];
      const matchKey = button.dataset.orderCalcSuggestion || "";
      const match = matches.find(
        item => String(item.codigo_producto || item.nombre_producto || "") === matchKey
      );

      if (!match) {
        return;
      }

      applyCatalogMatch(card, match, {
        replaceName: true,
        forcePrice: true,
        nameSource: "catalog",
        priceSource: "catalog"
      });
    });
  });

  recalculate();
  syncProductsFromSurvey();
  syncOrderCalcCoachState();

  submitContextButton?.addEventListener("click", () => {
    const context = buildCurrentOrderCalcContext();

    if (!context?.signature) {
      renderActiveCoachOrderCalcContext({
        isDirty: true
      });
      document.querySelectorAll("[data-order-calc-context-note]").forEach(node => {
        node.textContent = "Agrega al menos un producto o un escenario de pago antes de pasarlo al Coach.";
        node.dataset.state = "warning";
      });
      return;
    }

    const nextContext = buildCoachOrderCalcContext({
      ...context,
      activatedAt: new Date().toISOString(),
      ownerUserId: root.dataset.orderCalcOwnerUserId || context.ownerUserId || ""
    });

    setActiveCoachOrderCalcContext(nextContext);
    registerCoachDemoEvent({
      id: "order_calc_shared",
      label: "Escenario de pago enviado",
      detail: `Balance ${formatMoney(nextContext.summary.balanceFinal)} · Semanal ${formatMoney(nextContext.summary.weekly)}`
    });
    addCoachMessage(null, "assistant", buildCoachOrderCalcReply(nextContext));
  });

  root.dataset.orderCalcSyncReady = "true";
  root.syncProductsFromSurvey = syncProductsFromSurvey;
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
  const submitContextButton = root.querySelector("[data-decision-tool-submit-context]");

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

  const buildCurrentDecisionContext = () => {
    const prosRows = Array.from(prosContainer?.querySelectorAll(".decision-tool-row") || []);
    const consRows = Array.from(consContainer?.querySelectorAll(".decision-tool-row") || []);
    const pros = [];
    const cons = [];
    let totalPros = 0;
    let totalCons = 0;
    let maxConValue = 0;
    let maxConText = "";

    prosRows.forEach((row, index) => {
      const text = String(row.querySelector("[data-decision-text]")?.value || "").trim();
      const weight = Number.parseInt(row.querySelector("[data-decision-weight]")?.value || "0", 10) || 0;
      if (text || weight > 0) {
        totalPros += weight;
        pros.push({
          slot: index + 1,
          text,
          weight
        });
      }
    });

    consRows.forEach((row, index) => {
      const text = String(row.querySelector("[data-decision-text]")?.value || "").trim();
      const weight = Number.parseInt(row.querySelector("[data-decision-weight]")?.value || "0", 10) || 0;
      if (text || weight > 0) {
        totalCons += weight;
        cons.push({
          slot: index + 1,
          text,
          weight
        });
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

    return buildCoachDecisionContext({
      ownerUserId: root.dataset.decisionOwnerUserId || "",
      pros,
      cons,
      summary: {
        totalPros,
        totalCons,
        percent,
        message,
        topObjection,
        nextStep
      }
    });
  };

  const renderDecisionAnalysis = context => {
    const safeContext = buildCoachDecisionContext(context);
    const totalPros = safeContext?.summary?.totalPros || 0;
    const totalCons = safeContext?.summary?.totalCons || 0;
    const percent = safeContext?.summary?.percent || 0;
    const topObjection = safeContext?.summary?.topObjection || "Sin objecion principal todavia.";
    const message = safeContext?.summary?.message || "Agrega razones y objeciones para leer el cierre.";
    const nextStep = safeContext?.summary?.nextStep || "Usalo cuando el cliente diga que lo quiere pensar.";
    const prosPercentOfMax = Math.min(100, Math.round((totalPros / (DECISION_TOOL_ROW_COUNT * 10)) * 100));
    const consPercentOfMax = Math.min(100, Math.round((totalCons / (DECISION_TOOL_ROW_COUNT * 10)) * 100));

    if (totalProsNode) totalProsNode.textContent = String(totalPros);
    if (totalConsNode) totalConsNode.textContent = String(totalCons);
    if (percentNode) percentNode.textContent = `${percent}%`;
    if (messageNode) messageNode.textContent = message;
    if (objectionNode) objectionNode.textContent = topObjection;
    if (nextStepNode) nextStepNode.textContent = nextStep;
    if (prosBar) prosBar.style.width = `${prosPercentOfMax}%`;
    if (consBar) consBar.style.width = `${consPercentOfMax}%`;
  };

  const syncDecisionCoachState = () => {
    const activeContext = getActiveCoachDecisionContext();
    const draftContext = buildCurrentDecisionContext();

    root._decisionDraftContext = draftContext;

    if (!activeContext?.signature) {
      renderActiveCoachDecisionContext(null);
      return;
    }

    if (draftContext?.signature && draftContext.signature !== activeContext.signature) {
      renderActiveCoachDecisionContext({
        ...activeContext,
        isDirty: true
      });
      return;
    }

    renderActiveCoachDecisionContext(activeContext);
  };

  const analyze = () => {
    const context = buildCurrentDecisionContext();
    renderDecisionAnalysis(context);
    syncDecisionCoachState();

    if ((context?.summary?.totalPros || 0) + (context?.summary?.totalCons || 0) > 0) {
      registerCoachDemoEvent({
        id: "decision_balance",
        label: "Se corrio balance de decision",
        detail: `A favor ${context.summary.percent}%. Objecion principal: ${context.summary.topObjection}.`
      });
    }
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
  syncDecisionCoachState();

  toggle.addEventListener("click", () => {
    syncDecisionToggle(wrap.hidden);
  });

  runButton?.addEventListener("click", analyze);
  resetButton?.addEventListener("click", reset);

  root.querySelectorAll("input, select").forEach(field => {
    const eventName = field.tagName === "SELECT" ? "change" : "input";
    field.addEventListener(eventName, () => {
      renderDecisionAnalysis(buildCurrentDecisionContext());
      syncDecisionCoachState();
    });
  });

  submitContextButton?.addEventListener("click", () => {
    const context = buildCurrentDecisionContext();

    if (!context?.signature) {
      renderActiveCoachDecisionContext({
        isDirty: true
      });
      document.querySelectorAll("[data-decision-context-note]").forEach(node => {
        node.textContent = "Primero llena al menos una razon o una objecion con peso para pasarlo al Coach.";
        node.dataset.state = "warning";
      });
      return;
    }

    const nextContext = buildCoachDecisionContext({
      ...context,
      activatedAt: new Date().toISOString(),
      ownerUserId: root.dataset.decisionOwnerUserId || context.ownerUserId || ""
    });

    setActiveCoachDecisionContext(nextContext);
    registerCoachDemoEvent({
      id: "decision_shared",
      label: "Balance enviado",
      detail: `A favor ${nextContext.summary.percent}%. Objecion: ${nextContext.summary.topObjection}.`
    });
    addCoachMessage(null, "assistant", buildCoachDecisionReply(nextContext));
  });
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
      registerCoachDemoEvent({
        id: "buyer_profile",
        label: "Se corrio lectura del cliente",
        detail: `Perfil dominante: ${topProfile.label}.`
      });
      return;
    }

    applyProfile({
      label: `${topProfile.label} con mezcla`,
      driver: `${topProfile.driver} Tambien trae otra motivacion muy cerca, asi que conviene escuchar mas antes de empujar cierre.`,
      script: `${topProfile.script} Haz una pregunta mas para confirmar que es lo que de verdad pesa hoy.`,
      recommendation: topProfile.recommendation
    });
    registerCoachDemoEvent({
      id: "buyer_profile",
      label: "Se corrio lectura del cliente",
      detail: `Perfil dominante: ${topProfile.label} con mezcla.`
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

function initDailyPrizeTool() {
  const wrap = document.querySelector("[data-daily-prize-wrap]");
  const root = document.querySelector("[data-daily-prize-tool]");
  const toggle = document.querySelector("[data-daily-prize-toggle]");

  if (!root || !wrap || !toggle) {
    return;
  }

  const codeInput = root.querySelector("[data-daily-prize-code]");
  const activateButton = root.querySelector("[data-daily-prize-activate]");
  const resetButton = root.querySelector("[data-daily-prize-reset]");
  const feedbackNode = root.querySelector("[data-daily-prize-feedback]");
  const resultNode = root.querySelector("[data-daily-prize-result]");
  const titleNode = root.querySelector("[data-daily-prize-title]");
  const timerNode = root.querySelector("[data-daily-prize-timer]");
  const copyNode = root.querySelector("[data-daily-prize-copy]");
  const noteOutputNode = root.querySelector("[data-daily-prize-note-output]");
  const progressNode = root.querySelector("[data-daily-prize-progress]");
  const claimButton = root.querySelector("[data-daily-prize-claim]");
  let timerInterval = null;
  let currentState = { ...DAILY_PRIZE_DEFAULT_STATE };

  const syncPrizeToggle = open => {
    wrap.hidden = !open;
    toggle.setAttribute("aria-expanded", open ? "true" : "false");
    toggle.textContent = open ? "Cerrar oferta" : "Abrir oferta";
  };

  const getStoredState = () => {
    try {
      const raw = window.localStorage.getItem(COACH_DAILY_PRIZE_KEY);
      const parsed = raw ? JSON.parse(raw) : null;
      return parsed && typeof parsed === "object" ? parsed : null;
    } catch (error) {
      return null;
    }
  };

  const saveStoredState = state => {
    if (!state || (state.status === "idle" && !state.offerCode && !state.code)) {
      window.localStorage.removeItem(COACH_DAILY_PRIZE_KEY);
      return;
    }

    window.localStorage.setItem(COACH_DAILY_PRIZE_KEY, JSON.stringify(state));
  };

  const normalizeState = state => ({
    ...DAILY_PRIZE_DEFAULT_STATE,
    ...(state || {})
  });

  const getOffer = code => DAILY_PRIZE_OFFERS[String(code || "").trim()] || null;

  const stopTimer = () => {
    if (timerInterval) {
      window.clearInterval(timerInterval);
      timerInterval = null;
    }
  };

  const setFeedback = (message = "", state = "info") => {
    if (!feedbackNode) {
      return;
    }

    if (!message) {
      feedbackNode.hidden = true;
      feedbackNode.textContent = "";
      feedbackNode.dataset.state = "info";
      return;
    }

    feedbackNode.hidden = false;
    feedbackNode.textContent = message;
    feedbackNode.dataset.state = state;
  };

  const formatCountdown = remainingMs => {
    const totalSeconds = Math.max(0, Math.ceil(remainingMs / 1000));
    const minutes = Math.floor(totalSeconds / 60);
    const seconds = totalSeconds % 60;
    return `${minutes}:${String(seconds).padStart(2, "0")}`;
  };

  const updateTimerUi = remainingMs => {
    const clampedMs = Number.isFinite(remainingMs) ? Math.max(0, remainingMs) : 0;

    if (timerNode) {
      timerNode.textContent = formatCountdown(clampedMs);
    }

    if (progressNode) {
      const percent = Math.max(0, Math.min(100, (clampedMs / DAILY_PRIZE_DURATION_MS) * 100));
      progressNode.style.width = `${percent}%`;
    }
  };

  const applyStateToFields = state => {
    if (codeInput) {
      codeInput.value = state.code || state.offerCode || "";
    }
  };

  const markExpired = () => {
    currentState = {
      ...currentState,
      status: "expired"
    };
    saveStoredState(currentState);
    renderState(currentState);
  };

  const startTimer = () => {
    stopTimer();

    const tick = () => {
      const expiresAt = new Date(currentState.expiresAt).getTime();
      if (!Number.isFinite(expiresAt)) {
        markExpired();
        return;
      }

      const remainingMs = expiresAt - Date.now();

      if (remainingMs <= 0) {
        markExpired();
        return;
      }

      updateTimerUi(remainingMs);
    };

    tick();
    timerInterval = window.setInterval(tick, 1000);
  };

  const renderState = incomingState => {
    currentState = normalizeState(incomingState);
    const offer = getOffer(currentState.offerCode || currentState.code);

    applyStateToFields(currentState);

    if (!resultNode || !titleNode || !copyNode || !noteOutputNode || !claimButton) {
      return;
    }

    if (!offer || currentState.status === "idle") {
      stopTimer();
      resultNode.hidden = true;
      claimButton.disabled = true;
      updateTimerUi(DAILY_PRIZE_DURATION_MS);
      return;
    }

    const expiresAtMs = new Date(currentState.expiresAt).getTime();

    if (currentState.status === "active" && (!Number.isFinite(expiresAtMs) || expiresAtMs <= Date.now())) {
      currentState = {
        ...currentState,
        status: "expired"
      };
      saveStoredState(currentState);
    }

    resultNode.hidden = false;
    titleNode.textContent = offer.title;
    copyNode.textContent = offer.copy;
    noteOutputNode.textContent = offer.note;

    if (currentState.status === "claimed") {
      stopTimer();
      claimButton.disabled = true;
      if (timerNode) {
        timerNode.textContent = "Reclamada";
      }
      if (progressNode) {
        progressNode.style.width = "100%";
      }
      setFeedback("Oferta reclamada. Ya puedes seguir con el cierre final.", "success");
      return;
    }

    if (currentState.status === "expired") {
      stopTimer();
      claimButton.disabled = true;
      updateTimerUi(0);
      setFeedback("Oferta agotada.", "error");
      return;
    }

    claimButton.disabled = false;
    setFeedback("Oferta activa. El tiempo solo ayuda a tomar la decision en este momento.", "info");
    startTimer();
  };

  currentState = normalizeState(getStoredState());
  applyStateToFields(currentState);
  renderState(currentState);
  syncPrizeToggle(false);

  toggle.addEventListener("click", () => {
    syncPrizeToggle(wrap.hidden);
  });

  codeInput?.addEventListener("input", () => {
    codeInput.value = String(codeInput.value || "").replace(/[^\d]/g, "").slice(0, 12);
  });

  activateButton?.addEventListener("click", () => {
    const code = String(codeInput?.value || "").trim();
    const offer = getOffer(code);

    if (!offer) {
      currentState = {
        ...DAILY_PRIZE_DEFAULT_STATE,
        code
      };
      saveStoredState(currentState);
      renderState(currentState);
      setFeedback("Codigo invalido. Usa 305 para descuento o 14407 para regalo.", "error");
      return;
    }

    currentState = {
      code,
      offerCode: offer.code,
      status: "active",
      startedAt: new Date().toISOString(),
      expiresAt: new Date(Date.now() + DAILY_PRIZE_DURATION_MS).toISOString(),
      claimedAt: ""
    };
    saveStoredState(currentState);
    renderState(currentState);
    registerCoachDemoEvent({
      id: "daily_prize_active",
      label: "Se activo oferta especial",
      detail: `${offer.title} con codigo ${offer.code}.`
    });
  });

  resetButton?.addEventListener("click", () => {
    stopTimer();
    currentState = { ...DAILY_PRIZE_DEFAULT_STATE };
    saveStoredState(currentState);
    setFeedback("", "info");
    renderState(currentState);
    codeInput?.focus();
  });

  claimButton?.addEventListener("click", () => {
    if (currentState.status !== "active") {
      return;
    }

    stopTimer();
    currentState = {
      ...currentState,
      status: "claimed",
      claimedAt: new Date().toISOString()
    };
    saveStoredState(currentState);
    renderState(currentState);
    registerCoachDemoEvent({
      id: "daily_prize_claimed",
      label: "Se reclamo oferta especial",
      detail: getOffer(currentState.offerCode || currentState.code)?.title || "Oferta especial."
    });
  });
}

function initEmbeddedLeadForm({ wrapSelector, toggleSelector, frameSelector, openLinkSelector, url, openLabel, closeLabel }) {
  const wrap = document.querySelector(wrapSelector);
  const toggle = document.querySelector(toggleSelector);
  const frame = document.querySelector(frameSelector);
  const openLink = document.querySelector(openLinkSelector);

  if (!wrap || !toggle || !frame) {
    return;
  }

  if (openLink) {
    openLink.href = url;
  }

  const embedHeight = Number.parseInt(frame.dataset.embedHeight || "0", 10);

  if (embedHeight > 0) {
    frame.style.minHeight = `${embedHeight}px`;
  }

  const syncLeadFormToggle = open => {
    wrap.hidden = !open;
    toggle.setAttribute("aria-expanded", open ? "true" : "false");
    toggle.textContent = open ? closeLabel : openLabel;
  };

  syncLeadFormToggle(false);

  toggle.addEventListener("click", () => {
    const willOpen = wrap.hidden;

    if (willOpen && !frame.getAttribute("src")) {
      frame.setAttribute("src", `${url}?embedded=true`);
    }

    syncLeadFormToggle(willOpen);
  });
}

function initLeadFormTool() {
  initEmbeddedLeadForm({
    wrapSelector: "[data-lead-form-wrap]",
    toggleSelector: "[data-lead-form-toggle]",
    frameSelector: "[data-lead-form-frame]",
    openLinkSelector: "[data-lead-form-open-link]",
    url: GOOGLE_RAFFLE_FORM_URL,
    openLabel: "Abrir rifa",
    closeLabel: "Cerrar rifa"
  });
}

function initLeadDestinationSettings(initialDestination = null) {
  const form = document.querySelector("[data-lead-destination-form]");
  const typeSelect = document.querySelector("[data-lead-destination-type]");
  const labelInput = document.querySelector("[data-lead-destination-label]");
  const emailInput = document.querySelector("[data-lead-destination-email]");
  const urlInput = document.querySelector("[data-lead-destination-url]");
  const extraWrap = document.querySelector("[data-lead-destination-extra]");
  const emailField = document.querySelector("[data-lead-destination-email-field]");
  const urlField = document.querySelector("[data-lead-destination-url-field]");
  const currentNode = document.querySelector("[data-lead-destination-current]");
  const feedbackNode = document.querySelector("[data-lead-destination-feedback]");
  const saveButton = document.querySelector("[data-lead-destination-save]");

  if (!form || !typeSelect || !labelInput || !emailInput || !urlInput || !extraWrap || !currentNode) {
    return;
  }

  const buildSummary = destination => {
    const safeDestination = destination || {};
    const type = safeDestination.type || "carpeta_privada";
    const baseLabel = safeDestination.label || formatLeadDestinationLabel(type);

    if (type === "carpeta_privada") {
      return "Tus leads se estan guardando aqui para que los trabajes con orden.";
    }

    if (type === "correo_personal") {
      if (!safeDestination.email) {
        return "Tu destino actual es correo personal, pero aun falta capturarlo.";
      }

      return `Tus leads se guardan en tu carpeta y tambien te llegan a ${safeDestination.email}.`;
    }

    if (!safeDestination.url) {
      return `${baseLabel} esta seleccionado, pero aun falta la URL.`;
    }

    return `Tus leads se guardan en tu carpeta y tambien se mandan a ${baseLabel}.`;
  };

  const syncExtraFields = () => {
    const type = typeSelect.value || "carpeta_privada";
    const needsExtra = type !== "carpeta_privada";
    const needsEmail = type === "correo_personal";
    const needsUrl = type === "google_sheets" || type === "webhook_crm";
    extraWrap.hidden = !needsExtra;
    if (emailField) {
      emailField.hidden = !needsEmail;
    }
    if (urlField) {
      urlField.hidden = !needsUrl;
    }
    emailInput.required = needsEmail;
    urlInput.required = needsUrl;
    labelInput.placeholder =
      type === "google_sheets"
        ? "Ej. Mi hoja de rifa"
        : type === "correo_personal"
          ? "Ej. Correo de trabajo"
          : "Ej. Mi GoHighLevel o Mi CRM";
  };

  const applyDestination = destination => {
    const safeDestination = destination || { type: "carpeta_privada", label: "", url: "" };
    typeSelect.value = safeDestination.type || "carpeta_privada";
    labelInput.value = safeDestination.label && safeDestination.type !== "carpeta_privada" ? safeDestination.label : "";
    emailInput.value = safeDestination.email || "";
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
          email: emailInput.value,
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

function createFourteenSheetReferralRow(index) {
  const row = document.createElement("section");
  row.className = "fourteen-sheet-referral";
  row.dataset.fourteenReferralRow = String(index);
  row.innerHTML = `
    <div class="fourteen-sheet-referral-head">
      <div>
        <strong>Referido ${index}</strong>
        <span>Nombre, telefono y una nota corta si hace falta.</span>
      </div>
    </div>
    <div class="fourteen-sheet-referral-grid">
      <label class="native-lead-field">
        <span>Nombre</span>
        <input type="text" name="referralName${index}" placeholder="Nombre del referido" />
      </label>
      <label class="native-lead-field">
        <span>Telefono</span>
        <input type="tel" name="referralPhone${index}" placeholder="7735551234" />
      </label>
      <label class="native-lead-field native-lead-field-full">
        <span>Notas</span>
        <input type="text" name="referralNotes${index}" maxlength="240" placeholder="Ej. mejor despues de las 6 pm" />
      </label>
    </div>
  `;
  return row;
}

function initFourteenSheetTool({ loadLeads, syncFolderToggle }) {
  const wrap = document.querySelector("[data-fourteen-sheet-wrap]");
  const toggle = document.querySelector("[data-fourteen-sheet-toggle]");
  const form = document.querySelector("[data-fourteen-sheet-form]");
  const referralsRoot = document.querySelector("[data-fourteen-sheet-referrals]");
  const addButton = document.querySelector("[data-fourteen-sheet-add]");
  const feedbackNode = document.querySelector("[data-fourteen-sheet-feedback]");
  const savedWrap = document.querySelector("[data-fourteen-saved-wrap]");
  const savedSummary = document.querySelector("[data-fourteen-saved-summary]");
  const savedList = document.querySelector("[data-fourteen-saved-list]");
  const savedNote = document.querySelector("[data-fourteen-saved-note]");
  const savedRefresh = document.querySelector("[data-fourteen-saved-refresh]");
  const instantWrap = document.querySelector("[data-fourteen-instant-wrap]");
  const instantSummary = document.querySelector("[data-fourteen-instant-summary]");
  const instantList = document.querySelector("[data-fourteen-instant-list]");
  const instantDetail = document.querySelector("[data-fourteen-instant-detail]");
  const instantName = document.querySelector("[data-fourteen-instant-name]");
  const instantPhone = document.querySelector("[data-fourteen-instant-phone]");
  const instantStatus = document.querySelector("[data-fourteen-instant-status]");
  const instantHostScript = document.querySelector("[data-fourteen-instant-host-script]");
  const instantRepScript = document.querySelector("[data-fourteen-instant-rep-script]");
  const instantFocus = document.querySelector("[data-fourteen-instant-focus]");
  const instantNotes = document.querySelector("[data-fourteen-instant-notes]");
  const instantAppointment = document.querySelector("[data-fourteen-instant-appointment]");
  const instantFeedback = document.querySelector("[data-fourteen-instant-feedback]");
  const instantResultButtons = Array.from(document.querySelectorAll("[data-fourteen-instant-result]"));
  const state = {
    savedSheets: []
  };
  let skipNextResetFeedback = false;
  let activeSheet = null;
  let activeReferralIndex = -1;

  if (!wrap || !toggle || !form || !referralsRoot || !addButton) {
    return;
  }

  const syncToggle = open => {
    wrap.hidden = !open;
    toggle.setAttribute("aria-expanded", open ? "true" : "false");
    toggle.textContent = open ? "Cerrar hoja" : "Abrir hoja";
  };

  const syncAddButton = () => {
    const currentCount = referralsRoot.querySelectorAll("[data-fourteen-referral-row]").length;
    addButton.disabled = currentCount >= FOURTEEN_SHEET_MAX_REFERRALS;
    addButton.textContent =
      currentCount >= FOURTEEN_SHEET_MAX_REFERRALS ? "Limite alcanzado" : "Agregar otro nombre";
  };

  const appendReferralRow = () => {
    const currentCount = referralsRoot.querySelectorAll("[data-fourteen-referral-row]").length;

    if (currentCount >= FOURTEEN_SHEET_MAX_REFERRALS) {
      syncAddButton();
      return;
    }

    referralsRoot.appendChild(createFourteenSheetReferralRow(currentCount + 1));
    syncAddButton();
  };

  const rebuildForm = ({ clearFeedback = true, resetFields = false } = {}) => {
    if (resetFields) {
      form.reset();
    }
    referralsRoot.innerHTML = "";
    for (let index = 0; index < FOURTEEN_SHEET_DEFAULT_REFERRALS; index += 1) {
      appendReferralRow();
    }
    if (clearFeedback) {
      clearMessage(feedbackNode);
    }
  };

  const setInstantFeedback = (message = "", state = "info") => {
    if (!instantFeedback) {
      return;
    }

    if (!message) {
      clearMessage(instantFeedback);
      return;
    }

    setMessage(instantFeedback, message, state);
  };

  const renderSavedList = () => {
    if (!savedWrap || !savedSummary || !savedList) {
      return;
    }

    savedWrap.hidden = false;

    if (!state.savedSheets.length) {
      savedSummary.textContent = "Aqui aparecera tu hoja mas reciente para retomar cita instantanea.";
      savedList.innerHTML = '<div class="health-survey-folder-empty">Aun no hay hojas 4 en 14 guardadas.</div>';

      if (savedNote) {
        savedNote.textContent = "Cuando guardes una hoja, podras volver a abrirla desde aqui.";
      }
      return;
    }

    const latestSheet = state.savedSheets[0];
    savedSummary.textContent = `Ultima hoja: ${latestSheet.hostName || "Casa sin nombre"} · ${formatDateTime(
      latestSheet.updatedAt || latestSheet.createdAt
    )} · ${latestSheet.referralCount || latestSheet.referrals?.length || 0} referido(s).`;

    savedList.innerHTML = state.savedSheets
      .map((sheet, index) => {
        const chips = [
          sheet.hostPhone ? `<span class="health-survey-folder-chip">${escapeHtml(formatLeadPhone(sheet.hostPhone))}</span>` : "",
          sheet.giftSelected ? `<span class="health-survey-folder-chip">${escapeHtml(sheet.giftSelected)}</span>` : "",
          `<span class="health-survey-folder-chip">${escapeHtml(
            `${sheet.referralCount || sheet.referrals?.length || 0} referido(s)`
          )}</span>`,
          sheet.representativeName
            ? `<span class="health-survey-folder-chip">${escapeHtml(sheet.representativeName)}</span>`
            : ""
        ]
          .filter(Boolean)
          .join("");

        const isCurrent = activeSheet?.id === sheet.id;

        return `
          <article class="fourteen-saved-item ${isCurrent ? "is-current" : ""}" data-fourteen-saved-id="${sheet.id}">
            <div class="health-survey-folder-head">
              <div>
                <strong>${escapeHtml(sheet.hostName || "Casa sin nombre")}</strong>
                <span>${escapeHtml(formatDateTime(sheet.updatedAt || sheet.createdAt))}</span>
              </div>
              <span class="lead-status-badge">${index === 0 ? "Ultima hoja" : isCurrent ? "Hoja activa" : "Guardada"}</span>
            </div>
            <p class="health-survey-folder-copy">${escapeHtml(sheet.summary || "Sin resumen todavia.")}</p>
            <div class="health-survey-folder-meta">${chips}</div>
            <div class="health-survey-folder-actions">
              <button type="button" class="secondary-button" data-fourteen-saved-open>
                ${isCurrent ? "Trabajando esta" : "Abrir hoja"}
              </button>
            </div>
          </article>
        `;
      })
      .join("");

    if (savedNote) {
      savedNote.textContent = "Elige la referencia que vas a trabajar para recibir apoyo mas preciso en la llamada.";
    }
  };

  const resetInstantSelection = ({ keepContext = false } = {}) => {
    activeReferralIndex = -1;

    if (instantDetail) {
      instantDetail.hidden = true;
    }

    if (instantNotes) {
      instantNotes.value = "";
    }

    if (instantAppointment) {
      instantAppointment.value = "";
    }

    if (!keepContext) {
      setActiveCoachProgram414Context(null);
    }

    setInstantFeedback("", "info");
  };

  const activateSavedSheet = (sheet, options = {}) => {
    activeSheet = sheet?.id ? sheet : null;

    if (!activeSheet?.id) {
      renderSavedList();
      renderInstantZone(null);
      return;
    }

    const existingContext = getActiveCoachProgram414Context();
    const shouldKeepContext =
      options.preserveContextIfSameSheet !== false &&
      existingContext?.sheetId === activeSheet.id &&
      Number.isInteger(existingContext?.referralIndex) &&
      Boolean(activeSheet.referrals?.[existingContext.referralIndex]);

    if (shouldKeepContext) {
      activeReferralIndex = existingContext.referralIndex;
    } else {
      resetInstantSelection({ keepContext: false });
    }

    renderSavedList();
    renderInstantZone(activeSheet);

    if (options.scrollToInstant && instantWrap && !instantWrap.hidden) {
      instantWrap.scrollIntoView({ behavior: "smooth", block: "start" });
    }
  };

  const loadSavedSheets = async ({ focusLatest = false, focusSheetId = "", scrollToInstant = false } = {}) => {
    const data = await apiRequest("/api/coach/program-4-in-14");
    state.savedSheets = Array.isArray(data.sheets) ? data.sheets : [];

    let sheetToActivate = null;

    if (focusSheetId) {
      sheetToActivate = state.savedSheets.find(item => item.id === focusSheetId) || null;
    }

    if (!sheetToActivate && focusLatest) {
      sheetToActivate = state.savedSheets[0] || null;
    }

    if (sheetToActivate) {
      activateSavedSheet(sheetToActivate, { scrollToInstant });
      return;
    }

    activeSheet = null;
    resetInstantSelection({ keepContext: false });
    renderSavedList();
    renderInstantZone(null);
  };

  const renderInstantSelection = () => {
    if (!instantDetail || !instantName || !instantPhone || !instantStatus || !instantHostScript || !instantRepScript || !instantFocus) {
      return;
    }

    const referral = activeSheet?.referrals?.[activeReferralIndex];

    if (!referral) {
      resetInstantSelection();
      return;
    }

    instantDetail.hidden = false;
    instantName.textContent = referral.fullName || "Referencia";
    instantPhone.textContent = referral.phone ? formatLeadPhone(referral.phone) : "Sin telefono";
    instantStatus.textContent = formatProgram414StatusLabel(referral.instantCallStatus || "seleccionado");
    instantHostScript.textContent = referral.scripts?.hostScript || "Sin guion todavia.";
    instantRepScript.textContent = referral.scripts?.repScript || "Sin guion todavia.";
    instantFocus.textContent = referral.scripts?.focus || "Cierra la cita, no el producto.";
    if (instantNotes) {
      instantNotes.value = referral.instantCallNotes || "";
    }
    if (instantAppointment) {
      instantAppointment.value = referral.appointmentDetails || "";
    }

    Array.from(instantList?.querySelectorAll("[data-fourteen-instant-pick]") || []).forEach(button => {
      button.classList.toggle("is-active", Number.parseInt(button.dataset.fourteenInstantPick || "-1", 10) === activeReferralIndex);
    });
  };

  const renderInstantZone = sheet => {
    activeSheet = sheet?.id ? sheet : null;

    if (!instantWrap || !instantSummary || !instantList) {
      return;
    }

    if (!activeSheet?.id || !Array.isArray(activeSheet.referrals) || !activeSheet.referrals.length) {
      instantWrap.hidden = true;
      instantList.innerHTML = "";
      resetInstantSelection();
      return;
    }

    instantWrap.hidden = false;
    instantSummary.textContent = `Anfitrion: ${activeSheet.hostName || "Sin nombre"}. Ahora pregunta: ¿a cual de estas personas le podemos llamar ahorita mismo?`;
    instantList.innerHTML = activeSheet.referrals
      .map(
        referral => `
          <button
            type="button"
            class="fourteen-instant-picker"
            data-fourteen-instant-pick="${referral.index}"
          >
            <strong>${escapeHtml(referral.fullName || "Sin nombre")}</strong>
            <span>${escapeHtml(referral.phone ? formatLeadPhone(referral.phone) : "Sin telefono")} · ${escapeHtml(
              formatProgram414StatusLabel(referral.instantCallStatus || "")
            )}${referral.notes ? ` · ${escapeHtml(referral.notes)}` : ""}</span>
          </button>
        `
      )
      .join("");

    Array.from(instantList.querySelectorAll("[data-fourteen-instant-pick]")).forEach(button => {
      button.addEventListener("click", async () => {
        const nextIndex = Number.parseInt(button.dataset.fourteenInstantPick || "-1", 10);

        if (!Number.isInteger(nextIndex) || nextIndex < 0 || !activeSheet?.id) {
          return;
        }

        try {
          const data = await apiRequest(
            `/api/coach/program-4-in-14/${encodeURIComponent(activeSheet.id)}/referrals/${nextIndex}/instant-call`,
            {
              method: "PATCH",
              body: {
                activate: true
              }
            }
          );

          activeSheet = data.sheet || activeSheet;
          activeReferralIndex = nextIndex;
          const context = buildCoachProgram414Context(activeSheet, nextIndex);
          setActiveCoachProgram414Context(context);
          renderInstantZone(activeSheet);
          renderInstantSelection();

          if (context) {
            registerCoachDemoEvent({
              id: "program_414_pick",
              label: "Se eligio referencia para cita instantanea",
              detail: `${context.referral.fullName || "Referencia"} desde 4 en 14.`
            });
            addCoachMessage(null, "assistant", buildCoachProgram414Reply(context));
          }
        } catch (error) {
          setInstantFeedback(error.message, "error");
        }
      });
    });

    const existingContext = getActiveCoachProgram414Context();
    const shouldUseStoredContext =
      existingContext?.sheetId === activeSheet.id &&
      Number.isInteger(existingContext?.referralIndex) &&
      Boolean(activeSheet.referrals?.[existingContext.referralIndex]);

    if (shouldUseStoredContext) {
      activeReferralIndex = existingContext.referralIndex;
    } else if (!activeSheet.referrals[activeReferralIndex]) {
      resetInstantSelection({ keepContext: false });
    }

    renderInstantSelection();
  };

  syncToggle(false);
  rebuildForm();
  renderSavedList();
  renderInstantZone(null);

  toggle.addEventListener("click", async () => {
    const willOpen = wrap.hidden;
    syncToggle(willOpen);

    if (!willOpen) {
      return;
    }

    try {
      await loadSavedSheets({ focusLatest: true });
    } catch (error) {
      savedWrap && (savedWrap.hidden = false);
      if (savedSummary) {
        savedSummary.textContent = "No pude cargar tus hojas guardadas en este momento.";
      }
      if (savedList) {
        savedList.innerHTML = '<div class="health-survey-folder-empty">No pude cargar tus hojas 4 en 14.</div>';
      }
      if (savedNote) {
        savedNote.textContent = error.message || "Intenta otra vez en un momento.";
      }
    }
  });

  addButton.addEventListener("click", () => {
    appendReferralRow();
  });

  savedRefresh?.addEventListener("click", async () => {
    setButtonLoading(savedRefresh, true, "Actualizando...");

    try {
      await loadSavedSheets({
        focusLatest: !activeSheet?.id,
        focusSheetId: activeSheet?.id || ""
      });
    } catch (error) {
      if (savedNote) {
        savedNote.textContent = error.message || "No pude refrescar tus hojas.";
      }
    } finally {
      setButtonLoading(savedRefresh, false);
    }
  });

  savedList?.addEventListener("click", event => {
    const openButton = event.target.closest("[data-fourteen-saved-open]");

    if (!openButton) {
      return;
    }

    const card = openButton.closest("[data-fourteen-saved-id]");
    const sheetId = card?.dataset.fourteenSavedId || "";
    const sheet = state.savedSheets.find(item => item.id === sheetId);

    if (!sheet) {
      return;
    }

    activateSavedSheet(sheet, { scrollToInstant: true });
  });

  form.addEventListener("submit", async event => {
    event.preventDefault();
    clearMessage(feedbackNode);

    const submitButton = form.querySelector('button[type="submit"]');
    const formData = new FormData(form);
    const referrals = Array.from(referralsRoot.querySelectorAll("[data-fourteen-referral-row]"))
      .map((row, index) => ({
        fullName: row.querySelector(`[name="referralName${index + 1}"]`)?.value || "",
        phone: row.querySelector(`[name="referralPhone${index + 1}"]`)?.value || "",
        notes: row.querySelector(`[name="referralNotes${index + 1}"]`)?.value || ""
      }))
      .filter(item => item.fullName || item.phone || item.notes);

    const payload = {
      hostName: formData.get("hostName"),
      hostPhone: formData.get("hostPhone"),
      giftSelected: formData.get("giftSelected"),
      representativeName: formData.get("representativeName"),
      representativePhone: formData.get("representativePhone"),
      startWindow: formData.get("startWindow"),
      notes: formData.get("notes"),
      referrals
    };

    setButtonLoading(submitButton, true, "Guardando...");

    try {
      const data = await apiRequest("/api/coach/program-4-in-14", {
        method: "POST",
        body: payload
      });

      const duplicateCopy = data.duplicateCount
        ? ` ${data.duplicateCount} ya existian y se actualizaron.`
        : "";
      const deliveryCopy = data.delivery?.queued
        ? " Tambien voy mandando la hoja a tu destino."
        : data.delivery?.attempted
          ? " Ya intente mandarla a tu destino."
          : "";

      setMessage(
        feedbackNode,
        `Hoja guardada. Se registraron ${data.createdLeadCount || 0} referido(s).${duplicateCopy}${deliveryCopy}`,
        "success"
      );
      await loadSavedSheets({
        focusSheetId: data.sheet?.id || "",
        scrollToInstant: true
      });
      skipNextResetFeedback = true;
      rebuildForm({ clearFeedback: false, resetFields: true });
      await loadLeads();
      syncFolderToggle(true);
    } catch (error) {
      setMessage(feedbackNode, error.message, "error");
    } finally {
      setButtonLoading(submitButton, false);
    }
  });

  form.addEventListener("reset", () => {
    window.requestAnimationFrame(() => {
      if (skipNextResetFeedback) {
        skipNextResetFeedback = false;
        rebuildForm({ clearFeedback: false, resetFields: false });
        return;
      }

      rebuildForm({ clearFeedback: true, resetFields: false });
    });
  });

  instantResultButtons.forEach(button => {
    button.addEventListener("click", async () => {
      if (!activeSheet?.id || activeReferralIndex < 0) {
        setInstantFeedback("Primero elige a quien le van a marcar ahorita.", "error");
        return;
      }

      const nextStatus = String(button.dataset.fourteenInstantResult || "").trim();
      setButtonLoading(button, true, "Guardando...");

      try {
        const data = await apiRequest(
          `/api/coach/program-4-in-14/${encodeURIComponent(activeSheet.id)}/referrals/${activeReferralIndex}/instant-call`,
          {
            method: "PATCH",
            body: {
              instantCallStatus: nextStatus,
              instantCallNotes: instantNotes?.value || "",
              appointmentDetails: instantAppointment?.value || ""
            }
          }
        );

        activeSheet = data.sheet || activeSheet;
        const context = buildCoachProgram414Context(activeSheet, activeReferralIndex);
        setActiveCoachProgram414Context(context);
        renderInstantZone(activeSheet);
        renderInstantSelection();
        await loadLeads();
        registerCoachDemoEvent({
          id: "program_414_result",
          label: "Se guardo resultado de cita instantanea",
          detail: `${formatProgram414StatusLabel(nextStatus)} con ${context?.referral?.fullName || "la referencia"}.`
        });

        setInstantFeedback(`Resultado guardado: ${formatProgram414StatusLabel(nextStatus)}.`, "success");
      } catch (error) {
        setInstantFeedback(error.message, "error");
      } finally {
        setButtonLoading(button, false);
      }
    });
  });
}

function initHealthSurveyTool() {
  const formWrap = document.querySelector("[data-health-survey-wrap]");
  const formToggle = document.querySelector("[data-health-survey-toggle]");
  const form = document.querySelector("[data-health-survey-form]");
  const feedbackNode = document.querySelector("[data-health-survey-feedback]");
  const saveButton = document.querySelector("[data-health-survey-save]");
  const chatMessages = document.querySelector("[data-coach-chat-messages]");
  const folderWrap = document.querySelector("[data-health-survey-folder-wrap]");
  const folderToggle = document.querySelector("[data-health-survey-folder-toggle]");
  const folderList = document.querySelector("[data-health-survey-folder-list]");
  const folderNote = document.querySelector("[data-health-survey-folder-note]");
  const totalNode = document.querySelector("[data-health-survey-total]");
  const healthNode = document.querySelector("[data-health-survey-health]");
  const waterNode = document.querySelector("[data-health-survey-water]");
  const creditNode = document.querySelector("[data-health-survey-credit]");

  if (!formWrap || !formToggle || !form || !folderWrap || !folderToggle || !folderList) {
    return;
  }

  const state = {
    surveys: []
  };

  const activateSurvey = (survey, options = {}) => {
    const context = buildCoachHealthSurveyContext(survey);

    if (!context) {
      return;
    }

    setActiveCoachHealthSurveyContext(context);

    registerCoachDemoEvent({
      id: "health_survey_active",
      label: options.announce ? "Se guardo encuesta de salud" : "Se activo encuesta de salud",
      detail: `${context.fullName || "Casa activa"}${context.salesAnalysis?.recommendedClose ? ` · ${context.salesAnalysis.recommendedClose}` : ""}.`
    });

    if (options.announce && context.salesAnalysis?.coachReply) {
      addCoachMessage(chatMessages, "assistant", context.salesAnalysis.coachReply);
    }
  };

  const syncFormToggle = open => {
    formWrap.hidden = !open;
    formToggle.setAttribute("aria-expanded", open ? "true" : "false");
    formToggle.textContent = open ? "Cerrar encuesta" : "Abrir encuesta";
  };

  const syncFolderToggle = open => {
    folderWrap.hidden = !open;
    folderToggle.setAttribute("aria-expanded", open ? "true" : "false");
    folderToggle.textContent = open ? "Cerrar carpeta" : "Abrir carpeta";
  };

  const resetFormState = () => {
    form.reset();
    setNamedFieldValue(form, "surveyId", "");
    setCheckedValues(form, "cookingMaterials", []);
    setCheckedValues(form, "familyConditions", []);
    clearMessage(feedbackNode);
  };

  const fillForm = survey => {
    form.reset();
    setNamedFieldValue(form, "surveyId", survey?.id || "");
    setNamedFieldValue(form, "fullName", survey?.fullName || "");
    setNamedFieldValue(form, "phone", survey?.phone || "");
    setNamedFieldValue(form, "secondName", survey?.secondName || "");
    setNamedFieldValue(form, "workingStatus", survey?.workingStatus || "");
    setNamedFieldValue(form, "heardRoyal", survey?.heardRoyal || "");
    setNamedFieldValue(form, "familyPriority", survey?.familyPriority || "");
    setNamedFieldValue(form, "qualityReason", survey?.qualityReason || "");
    setNamedFieldValue(form, "productLikingScore", survey?.productLikingScore || "");
    setNamedFieldValue(form, "cooksForCount", survey?.cooksForCount || "");
    setNamedFieldValue(form, "foodSpendWeekly", survey?.foodSpendWeekly || "");
    setNamedFieldValue(form, "mealPrepTime", survey?.mealPrepTime || "");
    setCheckedValues(form, "cookingMaterials", survey?.cookingMaterials || []);
    setCheckedValues(form, "familyConditions", survey?.familyConditions || []);
    setNamedFieldValue(form, "lowFatHealthy", survey?.lowFatHealthy || "");
    setNamedFieldValue(form, "lowFatHealthyReason", survey?.lowFatHealthyReason || "");
    setNamedFieldValue(form, "cookwareAffects", survey?.cookwareAffects || "");
    setNamedFieldValue(form, "cookwareAffectsReason", survey?.cookwareAffectsReason || "");
    setNamedFieldValue(form, "qualityInterest", survey?.qualityInterest || "");
    setNamedFieldValue(form, "qualityInterestReason", survey?.qualityInterestReason || "");
    setNamedFieldValue(form, "drinkingWaterType", survey?.drinkingWaterType || "");
    setNamedFieldValue(form, "cookingWaterType", survey?.cookingWaterType || "");
    setNamedFieldValue(form, "tapWaterConcern", survey?.tapWaterConcern || "");
    setNamedFieldValue(form, "waterSpendWeekly", survey?.waterSpendWeekly || "");
    setNamedFieldValue(form, "likesNaturalJuices", survey?.likesNaturalJuices || "");
    setNamedFieldValue(form, "juiceFrequency", survey?.juiceFrequency || "");
    setNamedFieldValue(form, "creditProblems", survey?.creditProblems || "");
    setNamedFieldValue(form, "creditImproveInterest", survey?.creditImproveInterest || "");
    setNamedFieldValue(form, "familyHealthInvestment", survey?.familyHealthInvestment || "");
    setNamedFieldValue(form, "weeklyBudget", survey?.weeklyBudget || "");
    setNamedFieldValue(form, "monthlyBudget", survey?.monthlyBudget || "");
    setNamedFieldValue(form, "topProduct1", survey?.topProducts?.[0] || "");
    setNamedFieldValue(form, "topProduct2", survey?.topProducts?.[1] || "");
    setNamedFieldValue(form, "topProduct3", survey?.topProducts?.[2] || "");
    clearMessage(feedbackNode);
  };

  const collectPayload = () => {
    const formData = new FormData(form);
    const topProducts = [formData.get("topProduct1"), formData.get("topProduct2"), formData.get("topProduct3")]
      .map(item => String(item || "").trim())
      .filter(Boolean);

    return {
      surveyId: formData.get("surveyId"),
      fullName: formData.get("fullName"),
      phone: formData.get("phone"),
      secondName: formData.get("secondName"),
      workingStatus: formData.get("workingStatus"),
      heardRoyal: formData.get("heardRoyal"),
      familyPriority: formData.get("familyPriority"),
      qualityReason: formData.get("qualityReason"),
      productLikingScore: formData.get("productLikingScore"),
      cooksForCount: formData.get("cooksForCount"),
      foodSpendWeekly: formData.get("foodSpendWeekly"),
      mealPrepTime: formData.get("mealPrepTime"),
      cookingMaterials: getCheckedValues(form, "cookingMaterials"),
      familyConditions: getCheckedValues(form, "familyConditions"),
      lowFatHealthy: formData.get("lowFatHealthy"),
      lowFatHealthyReason: formData.get("lowFatHealthyReason"),
      cookwareAffects: formData.get("cookwareAffects"),
      cookwareAffectsReason: formData.get("cookwareAffectsReason"),
      qualityInterest: formData.get("qualityInterest"),
      qualityInterestReason: formData.get("qualityInterestReason"),
      drinkingWaterType: formData.get("drinkingWaterType"),
      cookingWaterType: formData.get("cookingWaterType"),
      tapWaterConcern: formData.get("tapWaterConcern"),
      waterSpendWeekly: formData.get("waterSpendWeekly"),
      likesNaturalJuices: formData.get("likesNaturalJuices"),
      juiceFrequency: formData.get("juiceFrequency"),
      creditProblems: formData.get("creditProblems"),
      creditImproveInterest: formData.get("creditImproveInterest"),
      familyHealthInvestment: formData.get("familyHealthInvestment"),
      weeklyBudget: formData.get("weeklyBudget"),
      monthlyBudget: formData.get("monthlyBudget"),
      topProducts
    };
  };

  const renderFolderSummary = () => {
    if (totalNode) {
      totalNode.textContent = String(state.surveys.length);
    }

    if (healthNode) {
      healthNode.textContent = String(state.surveys.filter(item => item.familyPriority === "Salud").length);
    }

    if (waterNode) {
      waterNode.textContent = String(state.surveys.filter(item => item.tapWaterConcern === "Si").length);
    }

    if (creditNode) {
      creditNode.textContent = String(state.surveys.filter(item => item.creditImproveInterest === "Si").length);
    }
  };

  const renderFolderList = () => {
    folderList.innerHTML = "";
    renderFolderSummary();

    if (!state.surveys.length) {
      folderList.innerHTML = '<div class="health-survey-folder-empty">Aun no hay encuestas guardadas.</div>';
      if (folderNote) {
        folderNote.textContent = "Aqui apareceran las encuestas que vayas guardando.";
      }
      return;
    }

    const fragment = document.createDocumentFragment();

    state.surveys.forEach(survey => {
      const card = document.createElement("article");
      card.className = "health-survey-folder-item";
      card.dataset.healthSurveyId = survey.id;

      const chips = [
        survey.phone ? `<span class="health-survey-folder-chip">${escapeHtml(formatLeadPhone(survey.phone))}</span>` : "",
        survey.familyPriority ? `<span class="health-survey-folder-chip">${escapeHtml(survey.familyPriority)}</span>` : "",
        survey.weeklyBudget ? `<span class="health-survey-folder-chip">Semanal ${escapeHtml(survey.weeklyBudget)}</span>` : "",
        survey.monthlyBudget ? `<span class="health-survey-folder-chip">Mensual ${escapeHtml(survey.monthlyBudget)}</span>` : "",
        ...(Array.isArray(survey.topProducts) ? survey.topProducts.slice(0, 3).map(item => `<span class="health-survey-folder-chip">${escapeHtml(item)}</span>`) : [])
      ]
        .filter(Boolean)
        .join("");

      card.innerHTML = `
        <div class="health-survey-folder-head">
          <div>
            <strong>${escapeHtml(survey.fullName || "Casa sin nombre")}</strong>
            <span>${escapeHtml(formatDateTime(survey.updatedAt || survey.createdAt))}</span>
          </div>
          <span class="lead-status-badge">Encuesta guardada</span>
        </div>
        <p class="health-survey-folder-copy">${escapeHtml(survey.summary || "Sin resumen todavia.")}</p>
        <div class="health-survey-folder-meta">${chips}</div>
        <div class="health-survey-folder-actions">
          <button type="button" class="secondary-button" data-health-survey-open>Abrir encuesta</button>
        </div>
      `;

      fragment.appendChild(card);
    });

    folderList.appendChild(fragment);

    if (folderNote) {
      folderNote.textContent = `${state.surveys.length} encuesta(s) listas para consultar.`;
    }
  };

  const loadSurveys = async () => {
    const data = await apiRequest("/api/coach/health-surveys");
    state.surveys = Array.isArray(data.surveys) ? data.surveys : [];

    const currentContext = getActiveCoachHealthSurveyContext();

    if (currentContext?.id) {
      const refreshedSurvey = state.surveys.find(item => item.id === currentContext.id);

      if (refreshedSurvey) {
        setActiveCoachHealthSurveyContext(buildCoachHealthSurveyContext(refreshedSurvey));
      }
    }

    renderFolderList();
  };

  syncFormToggle(false);
  syncFolderToggle(false);

  formToggle.addEventListener("click", () => {
    syncFormToggle(formWrap.hidden);
  });

  folderToggle.addEventListener("click", async () => {
    const willOpen = folderWrap.hidden;
    syncFolderToggle(willOpen);

    if (willOpen) {
      await loadSurveys().catch(() => {
        if (folderNote) {
          folderNote.textContent = "No pude cargar tu carpeta de encuestas.";
        }
      });
    }
  });

  form.addEventListener("submit", async event => {
    event.preventDefault();
    clearMessage(feedbackNode);
    setButtonLoading(saveButton, true, "Guardando...");

    try {
      const payload = collectPayload();
      const data = await apiRequest("/api/coach/health-surveys", {
        method: "POST",
        body: payload
      });

      activateSurvey(data.survey, { announce: true });
      fillForm(data.survey);
      await loadSurveys();
      syncFolderToggle(true);
      setMessage(
        feedbackNode,
        data.created
          ? `Encuesta guardada. ${data.survey?.salesAnalysis?.coachReply || ""}`.trim()
          : `Encuesta actualizada. ${data.survey?.salesAnalysis?.coachReply || ""}`.trim(),
        "success"
      );
    } catch (error) {
      setMessage(feedbackNode, error.message, "error");
    } finally {
      setButtonLoading(saveButton, false);
    }
  });

  form.addEventListener("reset", () => {
    window.requestAnimationFrame(() => {
      resetFormState();
    });
  });

  folderList.addEventListener("click", event => {
    const openButton = event.target.closest("[data-health-survey-open]");

    if (!openButton) {
      return;
    }

    const card = openButton.closest("[data-health-survey-id]");
    const surveyId = card?.dataset.healthSurveyId || "";
    const survey = state.surveys.find(item => item.id === surveyId);

    if (!survey) {
      return;
    }

    fillForm(survey);
    activateSurvey(survey);
    syncFormToggle(true);
    form.scrollIntoView({ behavior: "smooth", block: "start" });
  });
}

function initRecruitmentTool() {
  const formControllers = Array.from(document.querySelectorAll("[data-recruitment-root]"))
    .map(root => ({
      root,
      wrap: root.querySelector("[data-recruitment-wrap]"),
      toggle: root.querySelector("[data-recruitment-toggle]"),
      form: root.querySelector("[data-recruitment-form]"),
      feedbackNode: root.querySelector("[data-recruitment-feedback]"),
      saveButton: root.querySelector("[data-recruitment-save]")
    }))
    .filter(controller => controller.wrap && controller.toggle && controller.form && controller.feedbackNode);
  const folderWrap = document.querySelector("[data-recruitment-folder-wrap]");
  const folderToggle = document.querySelector("[data-recruitment-folder-toggle]");
  const folderList = document.querySelector("[data-recruitment-folder-list]");
  const folderNote = document.querySelector("[data-recruitment-folder-note]");
  const totalNode = document.querySelector("[data-recruitment-total]");
  const fullTimeNode = document.querySelector("[data-recruitment-full-time]");
  const drivingNode = document.querySelector("[data-recruitment-driving]");
  const salesNode = document.querySelector("[data-recruitment-sales]");

  if (!formControllers.length) {
    return;
  }

  const folderReady = Boolean(folderWrap && folderToggle && folderList);
  const state = {
    applications: []
  };

  const syncFormToggle = (controller, open) => {
    controller.wrap.hidden = !open;
    controller.toggle.setAttribute("aria-expanded", open ? "true" : "false");
    controller.toggle.textContent = open ? "Cerrar aplicacion" : "Abrir aplicacion";
  };

  const syncFolderToggle = open => {
    if (!folderReady) {
      return;
    }

    folderWrap.hidden = !open;
    folderToggle.setAttribute("aria-expanded", open ? "true" : "false");
    folderToggle.textContent = open ? "Cerrar carpeta" : "Abrir carpeta";
  };

  const resetControllerForm = controller => {
    controller.form.reset();
    setNamedFieldValue(controller.form, "applicationId", "");
    clearMessage(controller.feedbackNode);
  };

  const resetAllForms = () => {
    formControllers.forEach(resetControllerForm);
  };

  const fillAllForms = application => {
    formControllers.forEach(controller => {
      controller.form.reset();
      setNamedFieldValue(controller.form, "applicationId", application?.id || "");
      setNamedFieldValue(controller.form, "fullName", application?.fullName || "");
      setNamedFieldValue(controller.form, "phone", application?.phone || "");
      setNamedFieldValue(controller.form, "email", application?.email || "");
      setNamedFieldValue(controller.form, "drives", application?.drives || "");
      setNamedFieldValue(controller.form, "hasCar", application?.hasCar || "");
      setNamedFieldValue(controller.form, "customerServiceExperience", application?.customerServiceExperience || "");
      setNamedFieldValue(controller.form, "workPreference", application?.workPreference || "");
      setNamedFieldValue(controller.form, "salesExperience", application?.salesExperience || "");
      setNamedFieldValue(controller.form, "about", application?.about || "");
      clearMessage(controller.feedbackNode);
    });
  };

  const collectPayload = form => {
    const formData = new FormData(form);

    return {
      applicationId: formData.get("applicationId"),
      fullName: formData.get("fullName"),
      phone: formData.get("phone"),
      email: formData.get("email"),
      drives: formData.get("drives"),
      hasCar: formData.get("hasCar"),
      customerServiceExperience: formData.get("customerServiceExperience"),
      workPreference: formData.get("workPreference"),
      salesExperience: formData.get("salesExperience"),
      about: formData.get("about")
    };
  };

  const getPreferredController = () => {
    const currentWorkspace = window.sessionStorage.getItem(COACH_WORKSPACE_TAB_KEY) || "cierre";
    const workspaceMatch = formControllers.find(controller => {
      const section = controller.root.dataset.coachWorkspaceSection || controller.root.closest("[data-coach-workspace-section]")?.dataset.coachWorkspaceSection || "";
      return section === currentWorkspace;
    });

    return workspaceMatch || formControllers[0];
  };

  const renderSummary = () => {
    if (totalNode) {
      totalNode.textContent = String(state.applications.length);
    }

    if (fullTimeNode) {
      fullTimeNode.textContent = String(
        state.applications.filter(item => item.workPreference === "Tiempo completo").length
      );
    }

    if (drivingNode) {
      drivingNode.textContent = String(state.applications.filter(item => item.drives === "Si").length);
    }

    if (salesNode) {
      salesNode.textContent = String(state.applications.filter(item => item.salesExperience === "Si").length);
    }
  };

  const renderList = () => {
    if (!folderReady) {
      return;
    }

    folderList.innerHTML = "";
    renderSummary();

    if (!state.applications.length) {
      folderList.innerHTML = '<div class="recruitment-folder-empty">Aun no hay aplicaciones guardadas.</div>';
      if (folderNote) {
        folderNote.textContent = "Aqui apareceran las aplicaciones de trabajo que vayas guardando.";
      }
      return;
    }

    const fragment = document.createDocumentFragment();

    state.applications.forEach(application => {
      const card = document.createElement("article");
      card.className = "recruitment-folder-item";
      card.dataset.recruitmentId = application.id;

      const chips = [
        application.phone ? `<span class="recruitment-folder-chip">${escapeHtml(formatLeadPhone(application.phone))}</span>` : "",
        application.email ? `<span class="recruitment-folder-chip">${escapeHtml(application.email)}</span>` : "",
        application.workPreference ? `<span class="recruitment-folder-chip">${escapeHtml(application.workPreference)}</span>` : "",
        application.drives ? `<span class="recruitment-folder-chip">Maneja: ${escapeHtml(application.drives)}</span>` : "",
        application.hasCar ? `<span class="recruitment-folder-chip">Auto: ${escapeHtml(application.hasCar)}</span>` : "",
        application.salesExperience ? `<span class="recruitment-folder-chip">Ventas: ${escapeHtml(application.salesExperience)}</span>` : ""
      ]
        .filter(Boolean)
        .join("");

      card.innerHTML = `
        <div class="recruitment-folder-head">
          <div>
            <strong>${escapeHtml(application.fullName || "Candidato sin nombre")}</strong>
            <span>${escapeHtml(formatDateTime(application.updatedAt || application.createdAt))}</span>
          </div>
          <span class="lead-status-badge">Aplicacion guardada</span>
        </div>
        <p class="recruitment-folder-copy">${escapeHtml(application.summary || "Sin resumen todavia.")}</p>
        <div class="recruitment-folder-meta">${chips}</div>
        <div class="recruitment-folder-actions">
          <button type="button" class="secondary-button" data-recruitment-open>Abrir aplicacion</button>
        </div>
      `;

      fragment.appendChild(card);
    });

    folderList.appendChild(fragment);

    if (folderNote) {
      folderNote.textContent = `${state.applications.length} aplicacion(es) listas para revisar.`;
    }
  };

  const loadApplications = async () => {
    const data = await apiRequest("/api/coach/recruitment-applications");
    state.applications = Array.isArray(data.applications) ? data.applications : [];
    renderList();
  };

  formControllers.forEach(controller => {
    syncFormToggle(controller, false);
  });
  syncFolderToggle(false);

  formControllers.forEach(controller => {
    controller.toggle.addEventListener("click", () => {
      syncFormToggle(controller, controller.wrap.hidden);
    });

    controller.form.addEventListener("submit", async event => {
      event.preventDefault();
      clearMessage(controller.feedbackNode);
      setButtonLoading(controller.saveButton, true, "Guardando...");

      try {
        const data = await apiRequest("/api/coach/recruitment-applications", {
          method: "POST",
          body: collectPayload(controller.form)
        });

        const deliveryCopy = data.delivery?.queued
          ? " Tambien la estoy mandando a tu destino."
          : data.delivery?.attempted
            ? " Ya intente mandarla a tu destino."
            : "";

        resetAllForms();
        await loadApplications();
        syncFolderToggle(true);
        setMessage(
          controller.feedbackNode,
          data.created
            ? `Aplicacion guardada.${deliveryCopy}`
            : `Aplicacion actualizada correctamente.${deliveryCopy}`,
          "success"
        );
      } catch (error) {
        setMessage(controller.feedbackNode, error.message, "error");
      } finally {
        setButtonLoading(controller.saveButton, false);
      }
    });

    controller.form.addEventListener("reset", () => {
      window.requestAnimationFrame(() => {
        resetControllerForm(controller);
      });
    });
  });

  folderToggle?.addEventListener("click", async () => {
    const willOpen = folderWrap.hidden;
    syncFolderToggle(willOpen);

    if (willOpen) {
      await loadApplications().catch(() => {
        if (folderNote) {
          folderNote.textContent = "No pude cargar tu carpeta de aplicaciones.";
        }
      });
    }
  });

  folderList?.addEventListener("click", event => {
    const openButton = event.target.closest("[data-recruitment-open]");

    if (!openButton) {
      return;
    }

    const card = openButton.closest("[data-recruitment-id]");
    const applicationId = card?.dataset.recruitmentId || "";
    const application = state.applications.find(item => item.id === applicationId);

    if (!application) {
      return;
    }

    fillAllForms(application);

    const preferredController = getPreferredController();
    syncFormToggle(preferredController, true);
    preferredController.form.scrollIntoView({ behavior: "smooth", block: "start" });
  });
}

function initCoachPrivateResources() {
  const slotIds = ["catalogo_privado", "lista_precios_privada"];

  const getNodes = slotId => ({
    status: document.querySelector(`[data-private-resource-status="${slotId}"]`),
    toggle: document.querySelector(`[data-private-resource-toggle="${slotId}"]`),
    open: document.querySelector(`[data-private-resource-open="${slotId}"]`),
    remove: document.querySelector(`[data-private-resource-delete="${slotId}"]`),
    form: document.querySelector(`[data-private-resource-form="${slotId}"]`),
    file: document.querySelector(`[data-private-resource-file="${slotId}"]`),
    pin: document.querySelector(`[data-private-resource-pin="${slotId}"]`),
    save: document.querySelector(`[data-private-resource-save="${slotId}"]`),
    feedback: document.querySelector(`[data-private-resource-feedback="${slotId}"]`)
  });

  if (!slotIds.some(slotId => getNodes(slotId).status)) {
    return;
  }

  const state = {
    resources: {}
  };

  const syncFormVisibility = (slotId, open) => {
    const { form, toggle } = getNodes(slotId);

    if (!form || !toggle) {
      return;
    }

    form.hidden = !open;
    toggle.setAttribute("aria-expanded", open ? "true" : "false");

    const hasFile = Boolean(state.resources?.[slotId]?.hasFile);
    toggle.textContent = open ? "Cerrar carga" : hasFile ? "Reemplazar archivo" : "Subir archivo";
  };

  const renderSlot = slotId => {
    const nodes = getNodes(slotId);
    const resource = state.resources?.[slotId] || {};
    const defaultCopy =
      slotId === "catalogo_privado"
        ? "Aun no has subido tu catalogo privado."
        : "Aun no has subido tu lista de precios privada.";

    if (nodes.status) {
      nodes.status.textContent = resource.hasFile
        ? `${resource.fileName || "archivo-privado.pdf"} guardado ${formatDateTime(resource.uploadedAt)}. Protegido con PIN.`
        : defaultCopy;
    }

    if (nodes.open) {
      nodes.open.disabled = !resource.hasFile;
    }

    if (nodes.remove) {
      nodes.remove.disabled = !resource.hasFile;
    }

    syncFormVisibility(slotId, !nodes.form?.hidden);
  };

  const loadResources = async () => {
    const data = await apiRequest("/api/coach/private-resources");
    state.resources = data.resources || {};
    slotIds.forEach(renderSlot);
  };

  const openResourceFile = async slotId => {
    const nodes = getNodes(slotId);
    const resource = state.resources?.[slotId];

    if (!resource?.hasFile) {
      setMessage(nodes.feedback, "Primero sube un archivo en ese espacio.", "error");
      return;
    }

    const pin = sanitizePin4(window.prompt("Escribe tu PIN de 4 numeros para abrir este archivo.") || "");

    if (!pin) {
      setMessage(nodes.feedback, "Necesitas un PIN valido de 4 numeros.", "error");
      return;
    }

    clearMessage(nodes.feedback);
    const viewer = window.open("", "_blank");

    if (viewer) {
      viewer.document.write("<p style=\"font-family:Arial,sans-serif;padding:24px\">Abriendo archivo privado...</p>");
      viewer.document.close();
    }

    try {
      const response = await fetch(`/api/coach/private-resources/${slotId}/file`, {
        method: "POST",
        credentials: "same-origin",
        headers: {
          "Content-Type": "application/json"
        },
        body: JSON.stringify({ pin })
      });

      if (!response.ok) {
        const data = await response.json().catch(() => ({}));
        throw new Error(data.error || "No pude abrir ese archivo.");
      }

      const blob = await response.blob();
      const fileUrl = window.URL.createObjectURL(blob);

      if (viewer) {
        viewer.location.replace(fileUrl);
      } else {
        const tempLink = document.createElement("a");
        tempLink.href = fileUrl;
        tempLink.target = "_blank";
        tempLink.rel = "noreferrer";
        document.body.appendChild(tempLink);
        tempLink.click();
        document.body.removeChild(tempLink);
      }

      window.setTimeout(() => {
        window.URL.revokeObjectURL(fileUrl);
      }, 60_000);

      setMessage(nodes.feedback, "Archivo abierto.", "success");
    } catch (error) {
      if (viewer && !viewer.closed) {
        viewer.close();
      }

      setMessage(nodes.feedback, error.message, "error");
    }
  };

  slotIds.forEach(slotId => {
    const nodes = getNodes(slotId);

    if (!nodes.status) {
      return;
    }

    syncFormVisibility(slotId, false);

    nodes.pin?.addEventListener("input", () => {
      nodes.pin.value = sanitizePin4(nodes.pin.value);
    });

    nodes.toggle?.addEventListener("click", () => {
      syncFormVisibility(slotId, nodes.form?.hidden);
      clearMessage(nodes.feedback);
    });

    nodes.form?.addEventListener("submit", async event => {
      event.preventDefault();
      clearMessage(nodes.feedback);

      const file = nodes.file?.files?.[0];
      const pin = sanitizePin4(nodes.pin?.value || "");

      if (!file) {
        setMessage(nodes.feedback, "Selecciona un PDF antes de guardar.", "error");
        return;
      }

      if (!/\.pdf$/i.test(file.name) && file.type !== "application/pdf") {
        setMessage(nodes.feedback, "Solo puedo guardar archivos PDF.", "error");
        return;
      }

      if (!pin || pin.length !== 4) {
        setMessage(nodes.feedback, "El PIN debe tener 4 numeros.", "error");
        return;
      }

      if (file.size > 8 * 1024 * 1024) {
        setMessage(nodes.feedback, "El PDF es demasiado pesado. Usa uno de hasta 8 MB.", "error");
        return;
      }

      setButtonLoading(nodes.save, true, "Guardando...");

      try {
        const fileData = await readFileAsDataUrl(file);
        const data = await apiRequest(`/api/coach/private-resources/${slotId}/upload`, {
          method: "POST",
          body: {
            fileName: file.name,
            fileData,
            pin
          }
        });

        state.resources[slotId] = data.resource || {};
        nodes.form.reset();
        syncFormVisibility(slotId, false);
        renderSlot(slotId);
        setMessage(nodes.feedback, "Archivo guardado y protegido con PIN.", "success");
      } catch (error) {
        setMessage(nodes.feedback, error.message, "error");
      } finally {
        setButtonLoading(nodes.save, false);
      }
    });

    nodes.open?.addEventListener("click", () => {
      openResourceFile(slotId);
    });

    nodes.remove?.addEventListener("click", async () => {
      clearMessage(nodes.feedback);

      const confirmed = window.confirm(
        "Si borras este archivo, tambien se borra el PIN. Luego podras subirlo de nuevo con otro PIN. Quieres seguir?"
      );

      if (!confirmed) {
        return;
      }

      try {
        await apiRequest(`/api/coach/private-resources/${slotId}`, {
          method: "DELETE"
        });

        state.resources[slotId] = {};
        nodes.form?.reset();
        syncFormVisibility(slotId, false);
        renderSlot(slotId);
        setMessage(nodes.feedback, "Archivo borrado. Ya puedes subir uno nuevo con otro PIN.", "success");
      } catch (error) {
        setMessage(nodes.feedback, error.message, "error");
      }
    });
  });

  loadResources().catch(() => {
    slotIds.forEach(slotId => {
      const nodes = getNodes(slotId);

      if (nodes.feedback) {
        setMessage(nodes.feedback, "No pude cargar tus archivos privados.", "error");
      }
    });
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
  const panelButtons = Array.from(document.querySelectorAll("[data-lead-panel]"));
  const totalNode = document.querySelector("[data-coach-leads-total]");
  const newNode = document.querySelector("[data-coach-leads-new]");
  const bookedNode = document.querySelector("[data-coach-leads-booked]");
  const clientsNode = document.querySelector("[data-coach-leads-clients]");
  const panelRifaNode = document.querySelector("[data-coach-leads-panel-rifa]");
  const panel414Node = document.querySelector("[data-coach-leads-panel-414]");

  if (!captureWrap || !captureToggle || !captureForm || !folderWrap || !folderToggle || !leadList) {
    return;
  }

  const state = {
    leads: [],
    filter: "todos",
    panel: "rifa_capture"
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

  const getLeadPanel = lead => (lead?.source === "programa_4_en_14" ? "program_4_14" : "rifa_capture");

  const getPanelLabel = panel =>
    panel === "program_4_14" ? "4 en 14" : "Rifa y captura";

  const getPanelLeads = () => state.leads.filter(lead => getLeadPanel(lead) === state.panel);

  const getFilteredLeads = () => {
    const panelLeads = getPanelLeads();

    if (state.filter === "todos") {
      return panelLeads;
    }

    return panelLeads.filter(lead => lead.status === state.filter);
  };

  const buildSummary = leads => {
    const items = Array.isArray(leads) ? leads : [];
    return {
      total: items.length,
      nuevo: items.filter(lead => lead.status === "nuevo").length,
      agendado: items.filter(lead => lead.status === "agendado").length,
      cliente: items.filter(lead => lead.status === "cliente").length
    };
  };

  const renderSummary = () => {
    const panelLeads = getPanelLeads();
    const summary = buildSummary(panelLeads);

    if (totalNode) totalNode.textContent = String(summary.total || 0);
    if (newNode) newNode.textContent = String(summary.nuevo || 0);
    if (bookedNode) bookedNode.textContent = String(summary.agendado || 0);
    if (clientsNode) clientsNode.textContent = String(summary.cliente || 0);
    if (panelRifaNode) {
      panelRifaNode.textContent = String(state.leads.filter(lead => getLeadPanel(lead) === "rifa_capture").length);
    }
    if (panel414Node) {
      panel414Node.textContent = String(state.leads.filter(lead => getLeadPanel(lead) === "program_4_14").length);
    }
  };

  const renderLeadList = () => {
    const filteredLeads = getFilteredLeads();
    leadList.innerHTML = "";

    filterButtons.forEach(button => {
      button.classList.toggle("is-active", button.dataset.leadFilter === state.filter);
    });
    panelButtons.forEach(button => {
      const isActive = button.dataset.leadPanel === state.panel;
      button.classList.toggle("is-active", isActive);
      button.setAttribute("aria-selected", isActive ? "true" : "false");
    });

    if (!filteredLeads.length) {
      leadList.innerHTML = `<div class="lead-folder-empty">No hay leads en este panel con ese filtro todavia.</div>`;
      if (leadListNote) {
        leadListNote.textContent = `No hay resultados en ${getPanelLabel(state.panel)}. Cambia el filtro o guarda uno nuevo.`;
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
      leadListNote.textContent = `${filteredLeads.length} lead(s) en ${getPanelLabel(state.panel)}.`;
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
    link.download = state.panel === "program_4_14" ? "coach-leads-4-en-14.csv" : "coach-leads-rifa-y-captura.csv";
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
          <title>${getPanelLabel(state.panel)}</title>
          <style>
            body { font-family: Arial, sans-serif; padding: 24px; color: #111827; }
            h1 { margin: 0 0 16px; }
            table { width: 100%; border-collapse: collapse; }
            th, td { border: 1px solid #d1d5db; padding: 10px; text-align: left; vertical-align: top; }
            th { background: #f3f4f6; }
          </style>
        </head>
        <body>
          <h1>${getPanelLabel(state.panel)}</h1>
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
    renderSummary();
    renderLeadList();
  };

  syncCaptureToggle(false);
  syncFolderToggle(false);
  initFourteenSheetTool({ loadLeads, syncFolderToggle });

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

      const deliveryCopy = data.delivery?.queued
        ? " Tambien lo estoy mandando a tu destino."
        : data.delivery?.attempted
          ? " Ya intente mandarlo a tu destino."
          : "";

      setMessage(
        captureFeedback,
        data.duplicate
          ? `Este contacto ya existia. Lo actualice correctamente.${deliveryCopy}`
          : `Lead guardado correctamente.${deliveryCopy}`,
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

  panelButtons.forEach(button => {
    button.addEventListener("click", () => {
      state.panel = button.dataset.leadPanel || "rifa_capture";
      renderSummary();
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
  const targets = Array.from(
    new Set(
      [container, ...document.querySelectorAll("[data-coach-chat-messages], [data-coach-chat-messages-floating]")]
        .filter(Boolean)
    )
  );

  if (!targets.length) {
    return;
  }

  targets.forEach(target => {
    const card = document.createElement("article");
    card.className = `coach-message ${role}`;

    const paragraph = document.createElement("p");
    paragraph.textContent = content;
    card.appendChild(paragraph);

    target.appendChild(card);
    target.scrollTop = target.scrollHeight;
  });
}

async function fetchViewer() {
  try {
    return await apiRequest("/api/coach/me");
  } catch (error) {
    return { authenticated: false, error: error.message };
  }
}

function updateAuthTargets(user) {
  const accessLabel =
    user?.accountType === "seat" && hasCoachAccess(user)
      ? "Activa por equipo"
      : user?.subscriptionStatus === "test_access" || user?.subscriptionStatus === "trialing"
        ? "Prueba activa"
        : hasCoachAccess(user)
          ? "Activa"
          : user?.subscriptionStatus === "past_due"
            ? "Pago pendiente"
            : user?.accountType === "seat"
              ? "Acceso pausado"
              : "Sin activar";

  document.querySelectorAll("[data-coach-user-name]").forEach(node => {
    node.textContent = user?.name || "Distribuidor";
  });

  document.querySelectorAll("[data-coach-user-email]").forEach(node => {
    node.textContent = user?.email || "Sin correo";
  });

  document.querySelectorAll("[data-coach-subscription-status]").forEach(node => {
    node.textContent = accessLabel;
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

function formatDateTimeShort(value) {
  if (!value) {
    return "Sin movimiento todavia";
  }

  const date = new Date(value);

  if (Number.isNaN(date.getTime())) {
    return "Sin movimiento todavia";
  }

  return new Intl.DateTimeFormat("es-US", {
    month: "short",
    day: "numeric",
    hour: "numeric",
    minute: "2-digit"
  }).format(date);
}

function formatTeamSeatStatusLabel(status = "") {
  const labels = {
    active: "Activa",
    paused: "Pausada",
    closed: "Cerrada"
  };

  return labels[String(status || "").trim()] || "Sin estado";
}

function renderCoachTeamSummary(summary = null) {
  const safeSummary = summary || {};

  document.querySelectorAll("[data-team-summary-total]").forEach(node => {
    node.textContent = String(safeSummary.totalSeats || 0);
  });

  document.querySelectorAll("[data-team-summary-active]").forEach(node => {
    node.textContent = String(safeSummary.activeSeats || 0);
  });

  document.querySelectorAll("[data-team-summary-paused]").forEach(node => {
    node.textContent = String(safeSummary.pausedSeats || 0);
  });

  document.querySelectorAll("[data-team-summary-leads]").forEach(node => {
    node.textContent = String(safeSummary.totalGeneratedLeads || 0);
  });
}

function renderCoachTeamCreatedCredentials(credentials = null) {
  const wrap = document.querySelector("[data-team-created-card]");
  const emailNode = document.querySelector("[data-team-created-email]");
  const passwordNode = document.querySelector("[data-team-created-password]");

  if (!wrap || !emailNode || !passwordNode) {
    return;
  }

  if (!credentials?.email || !credentials?.temporaryPassword) {
    wrap.hidden = true;
    return;
  }

  emailNode.textContent = credentials.email;
  passwordNode.textContent = credentials.temporaryPassword;
  wrap.hidden = false;
}

function renderCoachTeamSeats(seats = []) {
  const list = document.querySelector("[data-team-seat-list]");

  if (!list) {
    return;
  }

  const safeSeats = Array.isArray(seats) ? seats : [];

  if (!safeSeats.length) {
    list.innerHTML = '<div class="team-seat-empty">Todavia no has creado subcuentas en este equipo.</div>';
    return;
  }

  list.innerHTML = safeSeats
    .map(
      seat => `
        <article class="team-seat-card" data-team-seat-id="${escapeHtml(seat.id || "")}">
          <div class="team-seat-head">
            <div>
              <strong>${escapeHtml(seat.seatLabel || seat.name || "Subcuenta")}</strong>
              <span>${escapeHtml(seat.name || "Sin nombre")} · ${escapeHtml(seat.email || "Sin correo")}</span>
            </div>
            <span class="team-seat-status" data-state="${escapeHtml(seat.seatStatus || "active")}">
              ${escapeHtml(formatTeamSeatStatusLabel(seat.seatStatus))}
            </span>
          </div>

          <div class="team-seat-metrics">
            <span>Leads: <strong>${Number(seat.counts?.leads || 0)}</strong></span>
            <span>Encuestas: <strong>${Number(seat.counts?.surveys || 0)}</strong></span>
            <span>4 en 14: <strong>${Number(seat.counts?.programSheets || 0)}</strong></span>
            <span>Aplicaciones: <strong>${Number(seat.counts?.applications || 0)}</strong></span>
          </div>

          <p class="mini-note team-seat-note">
            Ultima entrada: ${escapeHtml(formatDateTimeShort(seat.lastLoginAt))}.
          </p>

          <div class="dashboard-actions compact-top">
            <button type="button" class="nav-button" data-team-seat-copy-email="${escapeHtml(seat.email || "")}">
              Copiar correo
            </button>
            <button
              type="button"
              class="nav-button"
              data-team-seat-open-chef-share="${escapeHtml(seat.chef?.sharePath || "")}"
              data-team-seat-label="${escapeHtml(seat.seatLabel || seat.name || "Subcuenta")}"
            >
              Chef y QR
            </button>
            <a
              class="nav-button"
              href="${escapeHtml(seat.chef?.sharePath || "/chef/")}"
              target="_blank"
              rel="noreferrer"
            >
              Abrir Chef
            </a>
            <button
              type="button"
              class="primary-button"
              data-team-seat-toggle="${escapeHtml(seat.seatStatus === "active" ? "paused" : "active")}"
            >
              ${seat.seatStatus === "active" ? "Pausar acceso" : "Reactivar acceso"}
            </button>
          </div>
        </article>
      `
    )
    .join("");
}

function initCoachTeamWorkspace(user = null) {
  const form = document.querySelector("[data-team-seat-form]");
  const feedbackNode = document.querySelector("[data-team-seat-feedback]");
  const submitButton = document.querySelector("[data-team-seat-save]");
  const list = document.querySelector("[data-team-seat-list]");
  const copyEmailButton = document.querySelector("[data-team-copy-email]");
  const copyPasswordButton = document.querySelector("[data-team-copy-password]");

  if (!form || !list) {
    return;
  }

  if (!user?.managesTeam) {
    renderCoachTeamCreatedCredentials(null);
    renderCoachTeamSummary(null);
    renderCoachTeamSeats([]);
    return;
  }

  let latestCredentials = null;

  const loadTeam = async () => {
    const data = await apiRequest("/api/coach/team");
    renderCoachTeamSummary(data.summary || null);
    renderCoachTeamSeats(data.seats || []);
  };

  copyEmailButton?.addEventListener("click", async () => {
    if (!latestCredentials?.email) {
      return;
    }

    try {
      await copyTextToClipboard(latestCredentials.email);
      setMessage(feedbackNode, "El correo ya quedo copiado.", "success");
    } catch (error) {
      setMessage(feedbackNode, "No pude copiar el correo.", "error");
    }
  });

  copyPasswordButton?.addEventListener("click", async () => {
    if (!latestCredentials?.temporaryPassword) {
      return;
    }

    try {
      await copyTextToClipboard(latestCredentials.temporaryPassword);
      setMessage(feedbackNode, "La contrasena temporal ya quedo copiada.", "success");
    } catch (error) {
      setMessage(feedbackNode, "No pude copiar la contrasena.", "error");
    }
  });

  form.addEventListener("submit", async event => {
    event.preventDefault();
    clearMessage(feedbackNode);
    setButtonLoading(submitButton, true, "Creando...");

    try {
      const formData = new FormData(form);
      const data = await apiRequest("/api/coach/team/seats", {
        method: "POST",
        body: {
          name: formData.get("name"),
          email: formData.get("email"),
          seatLabel: formData.get("seatLabel")
        }
      });

      latestCredentials = data.credentials || null;
      renderCoachTeamCreatedCredentials(latestCredentials);
      setMessage(feedbackNode, "Subcuenta creada correctamente.", "success");
      form.reset();
      await loadTeam();
    } catch (error) {
      setMessage(feedbackNode, error.message, "error");
    } finally {
      setButtonLoading(submitButton, false);
    }
  });

  list.addEventListener("click", async event => {
    const copyButton = event.target.closest("[data-team-seat-copy-email]");

    if (copyButton) {
      try {
        await copyTextToClipboard(copyButton.dataset.teamSeatCopyEmail || "");
        setMessage(feedbackNode, "El correo ya quedo copiado.", "success");
      } catch (error) {
        setMessage(feedbackNode, "No pude copiar el correo.", "error");
      }
      return;
    }

    const shareButton = event.target.closest("[data-team-seat-open-chef-share]");

    if (shareButton) {
      window.dispatchEvent(
        new CustomEvent("coach:open-chef-share", {
          detail: {
            label: shareButton.dataset.teamSeatLabel || "Subcuenta",
            url: buildAbsoluteAppUrl(shareButton.dataset.teamSeatOpenChefShare || "")
          }
        })
      );
      return;
    }

    const toggleButton = event.target.closest("[data-team-seat-toggle]");

    if (!toggleButton) {
      return;
    }

    const card = toggleButton.closest("[data-team-seat-id]");
    const seatId = card?.dataset.teamSeatId || "";
    const nextStatus = toggleButton.dataset.teamSeatToggle || "";

    if (!seatId || !nextStatus) {
      return;
    }

    setButtonLoading(toggleButton, true, nextStatus === "paused" ? "Pausando..." : "Activando...");

    try {
      await apiRequest(`/api/coach/team/seats/${encodeURIComponent(seatId)}`, {
        method: "PATCH",
        body: {
          seatStatus: nextStatus
        }
      });

      await loadTeam();
      setMessage(
        feedbackNode,
        nextStatus === "paused" ? "La subcuenta quedo pausada." : "La subcuenta volvio a estar activa.",
        "success"
      );
    } catch (error) {
      setMessage(feedbackNode, error.message, "error");
    } finally {
      setButtonLoading(toggleButton, false);
    }
  });

  loadTeam().catch(error => {
    setMessage(feedbackNode, error.message || "No pude cargar tu equipo.", "error");
  });
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

function buildCoachHealthSurveyContext(survey = null) {
  if (!survey?.id) {
    return null;
  }

  return {
    id: survey.id,
    ownerUserId: survey.ownerUserId || "",
    fullName: survey.fullName || "",
    phone: survey.phone || "",
    summary: survey.summary || "",
    topProducts: Array.isArray(survey.topProducts) ? survey.topProducts : [],
    salesAnalysis: survey.salesAnalysis || {}
  };
}

function buildCoachOrderCalcContext(context = null) {
  if (!context || typeof context !== "object") {
    return null;
  }

  const cleanMoney = value => {
    const parsed = Number(value);
    return Number.isFinite(parsed) ? Number(parsed.toFixed(2)) : 0;
  };

  const products = Array.isArray(context.products)
    ? context.products
        .map((product, index) => {
          const name = String(product?.name || "").trim();
          const code = String(product?.code || "").trim();
          const basePrice = cleanMoney(product?.basePrice);
          const grossTotal = cleanMoney(product?.grossTotal);
          const downPayment = cleanMoney(product?.downPayment);
          const discountAmount = cleanMoney(product?.discountAmount);
          const balance = cleanMoney(product?.balance);
          const monthly = cleanMoney(product?.monthly);
          const weekly = cleanMoney(product?.weekly);
          const daily = cleanMoney(product?.daily);
          const slot = Number.parseInt(product?.slot || index + 1, 10);

          if (
            !name &&
            !code &&
            basePrice <= 0 &&
            grossTotal <= 0 &&
            downPayment <= 0 &&
            discountAmount <= 0 &&
            balance <= 0
          ) {
            return null;
          }

          return {
            slot: Number.isInteger(slot) && slot > 0 ? slot : index + 1,
            name,
            code,
            basePrice,
            grossTotal,
            downPayment,
            discountAmount,
            balance,
            monthly,
            weekly,
            daily
          };
        })
        .filter(Boolean)
        .slice(0, ORDER_CALC_PRODUCT_COUNT)
    : [];

  const summary = {
    totalGross: cleanMoney(context.summary?.totalGross),
    totalDown: cleanMoney(context.summary?.totalDown),
    totalDiscount: cleanMoney(context.summary?.totalDiscount),
    balanceFinal: cleanMoney(context.summary?.balanceFinal),
    monthly: cleanMoney(context.summary?.monthly),
    weekly: cleanMoney(context.summary?.weekly),
    daily: cleanMoney(context.summary?.daily),
    extraDown: cleanMoney(context.summary?.extraDown),
    extraDescFixed: cleanMoney(context.summary?.extraDescFixed),
    extraDescPercent: cleanMoney(context.summary?.extraDescPercent)
  };

  if (!products.length && summary.totalGross <= 0 && summary.balanceFinal <= 0) {
    return null;
  }

  const signature = JSON.stringify({
    products: products.map(product => ({
      slot: product.slot,
      name: product.name,
      code: product.code,
      basePrice: product.basePrice,
      downPayment: product.downPayment,
      discountAmount: product.discountAmount,
      balance: product.balance
    })),
    summary: {
      totalGross: summary.totalGross,
      totalDown: summary.totalDown,
      totalDiscount: summary.totalDiscount,
      balanceFinal: summary.balanceFinal,
      monthly: summary.monthly,
      weekly: summary.weekly,
      daily: summary.daily
    }
  });

  return {
    ownerUserId: String(context.ownerUserId || "").trim(),
    activatedAt: String(context.activatedAt || "").trim(),
    products,
    summary,
    signature
  };
}

function getActiveCoachOrderCalcContext() {
  try {
    const raw = window.sessionStorage.getItem(COACH_ACTIVE_ORDER_CALC_KEY);
    return raw ? buildCoachOrderCalcContext(JSON.parse(raw)) : null;
  } catch (error) {
    return null;
  }
}

function buildCoachOrderCalcReply(context = null) {
  const safeContext = buildCoachOrderCalcContext(context);

  if (!safeContext) {
    return "";
  }

  const productCopy = safeContext.products.length
    ? safeContext.products.map(product => product.name || product.code || `Producto ${product.slot}`).join(", ")
    : "sin producto";

  return [
    `Escenario de pago listo para Coach.`,
    `Productos: ${productCopy}.`,
    `Balance final ${formatMoney(safeContext.summary.balanceFinal)}.`,
    `Semanal ${formatMoney(safeContext.summary.weekly)}.`,
    "Si sale objecion por precio, Agustin ya puede responder con estos numeros."
  ].join(" ");
}

function renderActiveCoachOrderCalcContext(context = null) {
  const safeContext = context?.signature ? context : buildCoachOrderCalcContext(context);
  const isDirty = Boolean(safeContext?.isDirty);
  const hasContext = Boolean(safeContext?.signature);
  const defaultCopy = "Cuando el escenario quede listo, pasalo al Coach para responder con estos numeros.";
  const productCopy = safeContext?.products?.length
    ? safeContext.products.map(product => product.name || product.code || `Producto ${product.slot}`).join(", ")
    : "";
  const summaryCopy = hasContext
    ? `${productCopy || "Escenario listo"} · Balance ${formatMoney(safeContext.summary.balanceFinal)} · Semanal ${formatMoney(
        safeContext.summary.weekly
      )}.`
    : defaultCopy;
  const noteCopy = isDirty
    ? "Cambiaste los numeros. Presiona actualizar para pasar este nuevo escenario al Coach."
    : summaryCopy;

  document.querySelectorAll("[data-order-calc-context-note]").forEach(node => {
    node.textContent = noteCopy;
    node.dataset.state = isDirty ? "warning" : hasContext ? "success" : "idle";
  });

  document.querySelectorAll("[data-order-calc-submit-context]").forEach(button => {
    button.textContent = hasContext ? "Actualizar cierre en Coach" : "Usar este cierre con Coach";
  });
}

function setActiveCoachOrderCalcContext(context) {
  const next = buildCoachOrderCalcContext(context);

  if (next) {
    window.sessionStorage.setItem(COACH_ACTIVE_ORDER_CALC_KEY, JSON.stringify(next));
  } else {
    window.sessionStorage.removeItem(COACH_ACTIVE_ORDER_CALC_KEY);
  }

  renderActiveCoachOrderCalcContext(next);
}

function buildCoachDecisionContext(context = null) {
  if (!context || typeof context !== "object") {
    return null;
  }

  const cleanRows = rows =>
    Array.isArray(rows)
      ? rows
          .map((row, index) => {
            const text = String(row?.text || "").trim();
            const weight = Number.parseInt(row?.weight || "0", 10) || 0;
            const slot = Number.parseInt(row?.slot || index + 1, 10);

            if (!text && weight <= 0) {
              return null;
            }

            return {
              slot: Number.isInteger(slot) && slot > 0 ? slot : index + 1,
              text,
              weight
            };
          })
          .filter(Boolean)
          .slice(0, DECISION_TOOL_ROW_COUNT)
      : [];

  const pros = cleanRows(context.pros);
  const cons = cleanRows(context.cons);
  const totalPros = Number.parseInt(context.summary?.totalPros || "0", 10) || 0;
  const totalCons = Number.parseInt(context.summary?.totalCons || "0", 10) || 0;
  const percent = Number.parseInt(context.summary?.percent || "0", 10) || 0;
  const message = String(context.summary?.message || "").trim();
  const topObjection = String(context.summary?.topObjection || "").trim();
  const nextStep = String(context.summary?.nextStep || "").trim();

  if (!pros.length && !cons.length && totalPros <= 0 && totalCons <= 0) {
    return null;
  }

  const signature = JSON.stringify({
    pros: pros.map(row => ({ slot: row.slot, text: row.text, weight: row.weight })),
    cons: cons.map(row => ({ slot: row.slot, text: row.text, weight: row.weight })),
    summary: {
      totalPros,
      totalCons,
      percent,
      message,
      topObjection,
      nextStep
    }
  });

  return {
    ownerUserId: String(context.ownerUserId || "").trim(),
    activatedAt: String(context.activatedAt || "").trim(),
    pros,
    cons,
    summary: {
      totalPros,
      totalCons,
      percent,
      message: message || "Sin lectura todavia.",
      topObjection: topObjection || "Sin objecion principal todavia.",
      nextStep: nextStep || "Sin siguiente paso todavia."
    },
    signature
  };
}

function getActiveCoachDecisionContext() {
  try {
    const raw = window.sessionStorage.getItem(COACH_ACTIVE_DECISION_KEY);
    return raw ? buildCoachDecisionContext(JSON.parse(raw)) : null;
  } catch (error) {
    return null;
  }
}

function buildCoachDecisionReply(context = null) {
  const safeContext = buildCoachDecisionContext(context);

  if (!safeContext) {
    return "";
  }

  return [
    "Balance de decision listo para Coach.",
    `Compra a favor ${safeContext.summary.percent}%.`,
    `Objecion principal: ${safeContext.summary.topObjection}.`,
    `Siguiente paso: ${safeContext.summary.nextStep}`
  ].join(" ");
}

function renderActiveCoachDecisionContext(context = null) {
  const safeContext = context?.signature ? context : buildCoachDecisionContext(context);
  const isDirty = Boolean(safeContext?.isDirty);
  const hasContext = Boolean(safeContext?.signature);
  const defaultCopy =
    "Cuando veas clara la objecion real, pasale este balance al Coach para que te responda desde ahi.";
  const successCopy = hasContext
    ? `${safeContext.summary.message} Objecion: ${safeContext.summary.topObjection}.`
    : defaultCopy;
  const noteCopy = isDirty
    ? "Cambiaste el balance. Presiona actualizar para pasar esta nueva lectura al Coach."
    : successCopy;

  document.querySelectorAll("[data-decision-context-note]").forEach(node => {
    node.textContent = noteCopy;
    node.dataset.state = isDirty ? "warning" : hasContext ? "success" : "idle";
  });

  document.querySelectorAll("[data-decision-tool-submit-context]").forEach(button => {
    button.textContent = hasContext ? "Actualizar balance en Coach" : "Usar este balance con Coach";
  });
}

function setActiveCoachDecisionContext(context) {
  const next = buildCoachDecisionContext(context);

  if (next) {
    window.sessionStorage.setItem(COACH_ACTIVE_DECISION_KEY, JSON.stringify(next));
  } else {
    window.sessionStorage.removeItem(COACH_ACTIVE_DECISION_KEY);
  }

  renderActiveCoachDecisionContext(next);
}

function formatCoachDemoOutcomeLabel(value = "") {
  const labels = {
    venta: "Venta",
    follow_up: "Follow up",
    no_venta: "No venta",
    no_atendio: "No atendio"
  };

  return labels[String(value || "").trim()] || "Resultado";
}

function buildCoachDemoOutcomeContext(context = null) {
  if (!context || typeof context !== "object") {
    return null;
  }

  const resultType = String(context.resultType || "").trim();
  const saleAmount = Number(context.saleAmount);
  const products = Array.isArray(context.products)
    ? context.products.map(item => String(item || "").trim()).filter(Boolean).slice(0, 8)
    : [];
  const privateReason = String(context.privateReason || "").trim();
  const activeDemoStage = String(context.activeDemoStage || "").trim();
  const activeDemoStageLabel = String(context.activeDemoStageLabel || "").trim();
  const summary = String(context.summary || "").trim();

  if (!resultType && !saleAmount && !products.length && !privateReason && !summary) {
    return null;
  }

  return {
    id: String(context.id || "").trim(),
    ownerUserId: String(context.ownerUserId || "").trim(),
    resultType,
    saleAmount: Number.isFinite(saleAmount) ? Number(saleAmount.toFixed(2)) : 0,
    products,
    privateReason,
    activeDemoStage,
    activeDemoStageLabel,
    summary
  };
}

function getActiveCoachDemoOutcomeContext() {
  try {
    const raw = window.sessionStorage.getItem(COACH_ACTIVE_DEMO_OUTCOME_KEY);
    return raw ? buildCoachDemoOutcomeContext(JSON.parse(raw)) : null;
  } catch (error) {
    return null;
  }
}

function renderActiveCoachDemoOutcomeContext(context = null) {
  const safeContext = context?.resultType ? context : buildCoachDemoOutcomeContext(context);
  const defaultCopy = "Cuando guardes el resultado, Agustin tomara esta demo desde ese estado real.";
  const noteCopy = safeContext
    ? `${formatCoachDemoOutcomeLabel(safeContext.resultType)} guardada${safeContext.saleAmount > 0 ? ` · ${formatMoney(
        safeContext.saleAmount
      )}` : ""}${safeContext.activeDemoStageLabel ? ` · ${safeContext.activeDemoStageLabel}` : ""}.`
    : defaultCopy;

  document.querySelectorAll("[data-demo-outcome-context-note]").forEach(node => {
    node.textContent = noteCopy;
    node.dataset.state = safeContext ? "success" : "idle";
  });
}

function setActiveCoachDemoOutcomeContext(context) {
  const next = buildCoachDemoOutcomeContext(context);

  if (next) {
    window.sessionStorage.setItem(COACH_ACTIVE_DEMO_OUTCOME_KEY, JSON.stringify(next));
  } else {
    window.sessionStorage.removeItem(COACH_ACTIVE_DEMO_OUTCOME_KEY);
  }

  renderActiveCoachDemoOutcomeContext(next);
}

function renderCoachDemoOutcomeWorkspace(payload = null) {
  const personalSummary = payload?.personalSummary || {};
  const teamSummary = payload?.teamSummary || {};
  const outcomes = Array.isArray(payload?.outcomes) ? payload.outcomes : [];
  const teamOutcomes = Array.isArray(payload?.teamOutcomes) ? payload.teamOutcomes : [];

  document.querySelectorAll("[data-demo-outcome-personal-total]").forEach(node => {
    node.textContent = String(personalSummary.totalResults || 0);
  });
  document.querySelectorAll("[data-demo-outcome-personal-sales]").forEach(node => {
    node.textContent = String(personalSummary.salesCount || 0);
  });
  document.querySelectorAll("[data-demo-outcome-personal-amount]").forEach(node => {
    node.textContent = formatMoney(personalSummary.soldAmount || 0);
  });
  document.querySelectorAll("[data-demo-outcome-team-total]").forEach(node => {
    node.textContent = String(teamSummary.totalResults || 0);
  });
  document.querySelectorAll("[data-demo-outcome-team-sales]").forEach(node => {
    node.textContent = String(teamSummary.salesCount || 0);
  });
  document.querySelectorAll("[data-demo-outcome-team-amount]").forEach(node => {
    node.textContent = formatMoney(teamSummary.soldAmount || 0);
  });

  const personalList = document.querySelector("[data-demo-outcome-list]");
  const personalNote = document.querySelector("[data-demo-outcome-list-note]");

  if (personalList) {
    if (!outcomes.length) {
      personalList.innerHTML = '<div class="team-seat-empty">Todavia no has reportado resultados en esta cuenta.</div>';
    } else {
      personalList.innerHTML = outcomes
        .map(
          outcome => `
            <article class="demo-outcome-item">
              <div class="demo-outcome-head">
                <div>
                  <strong>${escapeHtml(outcome.resultLabel || "Resultado")}</strong>
                  <span>${escapeHtml(
                    outcome.activeDemoStageLabel || outcome.activeDemoStage || "Sin paso reportado"
                  )}</span>
                </div>
                <span class="demo-outcome-pill" data-state="${escapeHtml(outcome.resultType || "follow_up")}">
                  ${escapeHtml(outcome.saleAmount > 0 ? formatMoney(outcome.saleAmount) : outcome.resultLabel || "Resultado")}
                </span>
              </div>
              <p>${escapeHtml(outcome.summary || "Sin resumen.")}</p>
              <div class="demo-outcome-meta">
                <span>${escapeHtml(formatDateTimeShort(outcome.createdAt))}</span>
                <span>${escapeHtml(
                  Array.isArray(outcome.products) && outcome.products.length
                    ? outcome.products.join(", ")
                    : "Sin producto reportado"
                )}</span>
              </div>
            </article>
          `
        )
        .join("");
    }
  }

  if (personalNote) {
    personalNote.textContent = outcomes.length
      ? "Estos son tus resultados mas recientes dentro de esta cuenta."
      : "Aqui apareceran tus resultados mas recientes.";
  }

  document.querySelectorAll("[data-team-outcome-total]").forEach(node => {
    node.textContent = String(teamSummary.totalResults || 0);
  });
  document.querySelectorAll("[data-team-outcome-sales]").forEach(node => {
    node.textContent = String(teamSummary.salesCount || 0);
  });
  document.querySelectorAll("[data-team-outcome-followups]").forEach(node => {
    node.textContent = String(teamSummary.followUpCount || 0);
  });
  document.querySelectorAll("[data-team-outcome-amount]").forEach(node => {
    node.textContent = formatMoney(teamSummary.soldAmount || 0);
  });

  const teamList = document.querySelector("[data-team-outcome-list]");
  const teamNote = document.querySelector("[data-team-outcome-note]");

  if (teamList) {
    if (!teamOutcomes.length) {
      teamList.innerHTML = '<div class="team-seat-empty">Todavia no hay resultados guardados en este equipo.</div>';
    } else {
      teamList.innerHTML = teamOutcomes
        .map(
          outcome => `
            <article class="demo-outcome-item">
              <div class="demo-outcome-head">
                <div>
                  <strong>${escapeHtml(outcome.generatedByName || "Equipo")}</strong>
                  <span>${escapeHtml(outcome.resultLabel || "Resultado")}</span>
                </div>
                <span class="demo-outcome-pill" data-state="${escapeHtml(outcome.resultType || "follow_up")}">
                  ${escapeHtml(outcome.saleAmount > 0 ? formatMoney(outcome.saleAmount) : outcome.resultLabel || "Resultado")}
                </span>
              </div>
              <p>${escapeHtml(outcome.summary || "Sin resumen.")}</p>
              <div class="demo-outcome-meta">
                <span>${escapeHtml(formatDateTimeShort(outcome.createdAt))}</span>
                <span>${escapeHtml(
                  Array.isArray(outcome.products) && outcome.products.length
                    ? outcome.products.join(", ")
                    : "Sin producto reportado"
                )}</span>
              </div>
            </article>
          `
        )
        .join("");
    }
  }

  if (teamNote) {
    teamNote.textContent = teamOutcomes.length
      ? "Estos son los resultados mas recientes del equipo."
      : "Los resultados recientes del equipo apareceran aqui.";
  }
}

function initCoachDemoOutcomeWorkspace(user = null) {
  const form = document.querySelector("[data-demo-outcome-form]");
  const feedbackNode = document.querySelector("[data-demo-outcome-feedback]");
  const saveButton = document.querySelector("[data-demo-outcome-save]");
  const typeSelect = document.querySelector("[data-demo-outcome-type]");
  const amountField = document.querySelector("[data-demo-outcome-amount-field]");
  const amountInput = document.querySelector("[data-demo-outcome-amount]");

  if (!form || !typeSelect) {
    return;
  }

  const syncTypeUi = () => {
    const isSale = typeSelect.value === "venta";
    if (amountField) {
      amountField.hidden = !isSale;
    }
    if (!isSale && amountInput) {
      amountInput.value = "";
    }
  };

  const loadOutcomes = async () => {
    const data = await apiRequest("/api/coach/demo-outcomes");
    renderCoachDemoOutcomeWorkspace(data);
  };

  typeSelect.addEventListener("change", syncTypeUi);
  syncTypeUi();

  form.addEventListener("submit", async event => {
    event.preventDefault();
    clearMessage(feedbackNode);
    setButtonLoading(saveButton, true, "Guardando...");

    try {
      const formData = new FormData(form);
      const activeDemoStageMeta = getCoachDemoStageMeta(getCoachDemoStageId());
      const data = await apiRequest("/api/coach/demo-outcomes", {
        method: "POST",
        body: {
          resultType: formData.get("resultType"),
          saleAmount: formData.get("saleAmount"),
          products: String(formData.get("products") || "")
            .split(/[\n,;]+/g)
            .map(item => item.trim())
            .filter(Boolean),
          privateReason: formData.get("privateReason"),
          activeWorkspace: window.sessionStorage.getItem(COACH_WORKSPACE_TAB_KEY) || "cierre",
          activeDemoStage: activeDemoStageMeta.id,
          activeDemoStageLabel: activeDemoStageMeta.label,
          activeHealthSurveyId: getActiveCoachHealthSurveyContext()?.id || "",
          activeProgram414SheetId: getActiveCoachProgram414Context()?.sheetId || "",
          activeProgram414ReferralIndex: Number.isInteger(getActiveCoachProgram414Context()?.referralIndex)
            ? getActiveCoachProgram414Context().referralIndex
            : "",
          activeOrderCalcContext: getActiveCoachOrderCalcContext() || null,
          activeDecisionContext: getActiveCoachDecisionContext() || null,
          recentCoachEvents: getCoachDemoEvents()
        }
      });

      setActiveCoachDemoOutcomeContext(data.activeDemoOutcomeContext || null);
      setMessage(feedbackNode, "Resultado guardado correctamente.", "success");

      registerCoachDemoEvent({
        id: `demo_outcome_${data.outcome?.resultType || "follow_up"}`,
        label: "Se reporto resultado",
        detail: data.outcome?.summary || data.coachReply || "La demo ya quedo marcada."
      });

      form.reset();
      syncTypeUi();
      await loadOutcomes();
    } catch (error) {
      setMessage(feedbackNode, error.message, "error");
    } finally {
      setButtonLoading(saveButton, false);
    }
  });

  loadOutcomes().catch(error => {
    setMessage(feedbackNode, error.message || "No pude cargar los resultados.", "error");
  });
}

function formatProgram414StatusLabel(status = "") {
  const safeStatus = String(status || "").trim();
  const labels = {
    seleccionado: "Lista para marcar",
    cita_lograda: "Cita lograda",
    no_contesto: "No contesto",
    llamar_despues: "Llamar despues",
    no_quiso: "No quiso"
  };

  return labels[safeStatus] || "Sin mover";
}

function buildCoachProgram414Context(sheet = null, referralIndex = -1) {
  if (!sheet?.id || !Array.isArray(sheet.referrals) || referralIndex < 0 || !sheet.referrals[referralIndex]) {
    return null;
  }

  return {
    sheetId: sheet.id,
    ownerUserId: sheet.ownerUserId || "",
    hostName: sheet.hostName || "",
    hostPhone: sheet.hostPhone || "",
    giftSelected: sheet.giftSelected || "",
    representativeName: sheet.representativeName || "",
    representativePhone: sheet.representativePhone || "",
    startWindow: sheet.startWindow || "",
    summary: sheet.summary || "",
    referralIndex,
    referral: sheet.referrals[referralIndex]
  };
}

function getActiveCoachProgram414Context() {
  try {
    const raw = window.sessionStorage.getItem(COACH_ACTIVE_PROGRAM_414_KEY);
    return raw ? JSON.parse(raw) : null;
  } catch (error) {
    return null;
  }
}

function setActiveCoachProgram414Context(context) {
  if (!context?.sheetId) {
    window.sessionStorage.removeItem(COACH_ACTIVE_PROGRAM_414_KEY);
    return;
  }

  window.sessionStorage.setItem(COACH_ACTIVE_PROGRAM_414_KEY, JSON.stringify(context));
}

function buildCoachProgram414Reply(context = null) {
  if (!context?.referral?.fullName) {
    return "";
  }

  const hostScript = context.referral?.scripts?.hostScript || "";
  const repScript = context.referral?.scripts?.repScript || "";
  const focus = context.referral?.scripts?.focus || "Cierra cita, no producto.";
  const statusCopy = formatProgram414StatusLabel(context.referral?.instantCallStatus || "seleccionado");
  return [
    `Cita instantanea lista con ${context.referral.fullName}.`,
    hostScript ? `Anfitrion dice: "${hostScript}"` : "",
    repScript ? `Tu dices: "${repScript}"` : "",
    `Estado actual: ${statusCopy}. ${focus}`
  ]
    .filter(Boolean)
    .join(" ");
}

function getActiveCoachHealthSurveyContext() {
  try {
    const raw = window.sessionStorage.getItem(COACH_ACTIVE_HEALTH_SURVEY_KEY);
    return raw ? JSON.parse(raw) : null;
  } catch (error) {
    return null;
  }
}

function renderActiveHealthSurveyContext(context) {
  const safeContext = context || {};
  const closeCopy = safeContext.salesAnalysis?.recommendedClose
    ? `Cierre recomendado: ${safeContext.salesAnalysis.recommendedClose}. Producto: ${safeContext.salesAnalysis.recommendedProduct || "sin producto"}.`
    : "Abre una encuesta de salud para recibir una lectura mas precisa.";
  const anchorCopy = safeContext.salesAnalysis?.objectionAnchor || "La clave principal de cierre aparecera aqui.";

  document.querySelectorAll("[data-coach-health-survey-name]").forEach(node => {
    node.textContent = safeContext.fullName
      ? `${safeContext.fullName}${safeContext.phone ? ` · ${formatLeadPhone(safeContext.phone)}` : ""}`
      : "Selecciona una casa activa.";
  });

  document.querySelectorAll("[data-coach-health-survey-close]").forEach(node => {
    node.textContent = closeCopy;
  });

  document.querySelectorAll("[data-coach-health-survey-anchor]").forEach(node => {
    node.textContent = anchorCopy;
  });
}

function setActiveCoachHealthSurveyContext(context) {
  const next = context?.id ? context : null;

  if (next) {
    window.sessionStorage.setItem(COACH_ACTIVE_HEALTH_SURVEY_KEY, JSON.stringify(next));
  } else {
    window.sessionStorage.removeItem(COACH_ACTIVE_HEALTH_SURVEY_KEY);
  }

  renderActiveHealthSurveyContext(next);

  const orderCalcRoot = document.querySelector("[data-order-calc]");
  if (orderCalcRoot?.dataset.orderCalcSyncReady === "true" && typeof orderCalcRoot.syncProductsFromSurvey === "function") {
    orderCalcRoot.syncProductsFromSurvey(next);
  }
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

  if (me.authenticated && hasCoachAccess(user)) {
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

  if (me.authenticated && hasCoachAccess(me.user)) {
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

      if (hasCoachAccess(data.user)) {
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

  if (!hasCoachAccess(me.user)) {
    window.location.href = "/coach/planes/";
    return;
  }

  syncCoachManagerUi(me.user);
  updateAuthTargets(me.user);
  renderCoachProfile(me.profile);
  renderCoachNetworkSummary(me.networkSummary);
  renderCoachRepLeadSummary(me.repLeadSummary);
  renderActiveLeadContext(me.activeLeadContext);
  const effectiveOwnerId = getCoachEffectiveOwnerId(me.user);
  const storedHealthSurveyContext = getActiveCoachHealthSurveyContext();
  const storedProgram414Context = getActiveCoachProgram414Context();
  const storedOrderCalcContext = getActiveCoachOrderCalcContext();
  const storedDecisionContext = getActiveCoachDecisionContext();
  const storedDemoOutcomeContext = getActiveCoachDemoOutcomeContext();

  if (storedHealthSurveyContext?.ownerUserId && storedHealthSurveyContext.ownerUserId !== effectiveOwnerId) {
    setActiveCoachHealthSurveyContext(null);
  } else {
    renderActiveHealthSurveyContext(storedHealthSurveyContext);
  }

  if (storedProgram414Context?.ownerUserId && storedProgram414Context.ownerUserId !== effectiveOwnerId) {
    setActiveCoachProgram414Context(null);
  }

  if (storedOrderCalcContext?.ownerUserId && storedOrderCalcContext.ownerUserId !== effectiveOwnerId) {
    setActiveCoachOrderCalcContext(null);
  } else {
    renderActiveCoachOrderCalcContext(storedOrderCalcContext);
  }

  if (storedDecisionContext?.ownerUserId && storedDecisionContext.ownerUserId !== effectiveOwnerId) {
    setActiveCoachDecisionContext(null);
  } else {
    renderActiveCoachDecisionContext(storedDecisionContext);
  }

  if (storedDemoOutcomeContext?.ownerUserId && storedDemoOutcomeContext.ownerUserId !== effectiveOwnerId) {
    setActiveCoachDemoOutcomeContext(null);
  } else {
    renderActiveCoachDemoOutcomeContext(storedDemoOutcomeContext);
  }

  initCoachWorkspaceTabs();
  if (me.user?.managesTeam) {
    initLeadDestinationSettings(me.profile?.leadDestination || null);
  }
  initCoachPrivateResources();
  initLeadFormTool();
  initCoachLeadWorkspace();
  initCoachTeamWorkspace(me.user);
  initRecruitmentTool();
  initHealthSurveyTool();
  initOrderCalculator();
  const orderCalcRoot = document.querySelector("[data-order-calc]");
  if (orderCalcRoot) {
    orderCalcRoot.dataset.orderCalcOwnerUserId = effectiveOwnerId;
  }
  initDecisionTool();
  const decisionRoot = document.querySelector("[data-decision-tool]");
  if (decisionRoot) {
    decisionRoot.dataset.decisionOwnerUserId = effectiveOwnerId;
  }
  initBuyerProfileTool();
  initDailyPrizeTool();
  initCoachDemoOutcomeWorkspace(me.user);

  const chatMessages = document.querySelector("[data-coach-chat-messages]");
  const chatForm = document.querySelector("[data-coach-chat-form]");
  const chatInput = document.querySelector("[data-coach-chat-input]");
  const chatSendButton = document.querySelector("[data-coach-chat-send]");
  const chatStatus = document.querySelector("[data-coach-chat-status]");
  const floatingChatMessages = document.querySelector("[data-coach-chat-messages-floating]");
  const floatingChatForm = document.querySelector("[data-coach-chat-form-floating]");
  const floatingChatInput = document.querySelector("[data-coach-chat-input-floating]");
  const floatingChatSendButton = document.querySelector("[data-coach-chat-send-floating]");
  const floatingChatStatus = document.querySelector("[data-coach-float-status]");
  const floatingCoachToggle = document.querySelector("[data-coach-float-toggle]");
  const floatingCoachPanel = document.querySelector("[data-coach-float-panel]");
  const floatingCoachClose = document.querySelector("[data-coach-float-close]");
  const demoStageButtons = Array.from(document.querySelectorAll("[data-coach-stage-activate]"));
  const demoStageBadge = document.querySelector("[data-coach-demo-stage-badge]");
  const demoStageName = document.querySelector("[data-coach-demo-stage-name]");
  const demoStageCopy = document.querySelector("[data-coach-demo-stage-copy]");
  const demoEventsRoot = document.querySelector("[data-coach-demo-events]");
  const portalButtons = document.querySelectorAll("[data-open-billing-portal]");
  const logoutButtons = document.querySelectorAll("[data-coach-logout]");
  const chefSelfOpenLinks = document.querySelectorAll("[data-chef-self-open-link]");
  const chefShareButtons = document.querySelectorAll("[data-open-chef-share]");
  const chefShareModal = document.querySelector("[data-chef-share-modal]");
  const chefShareCloseButtons = document.querySelectorAll("[data-close-chef-share]");
  const chefShareUrlNodes = document.querySelectorAll("[data-chef-share-url]");
  const chefShareOpenLinks = document.querySelectorAll("[data-chef-share-open-link]");
  const chefShareQrNode = document.querySelector("[data-chef-share-qr]");
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
  const royalOneOpenLink = document.querySelector("[data-royalone-open-link]");
  const royalOneFeedback = document.querySelector("[data-royalone-feedback]");
  const ownChefSharePath = me.profile?.chef?.sharePath || "/chef/";
  const ownChefShareUrl = buildAbsoluteAppUrl(ownChefSharePath);
  let activeChefShare = {
    label: "Agustin 2.0 Chef",
    url: ownChefShareUrl
  };
  let floatingCoachMode = "idle";
  let floatingCoachArmTimeout = null;

  chefSelfOpenLinks.forEach(node => {
    node.href = ownChefShareUrl;
  });

  if (nativeShareChefButton && typeof navigator.share !== "function") {
    nativeShareChefButton.hidden = true;
  }

  const renderActiveChefShare = shareTarget => {
    activeChefShare = {
      label: String(shareTarget?.label || "Agustin 2.0 Chef").trim() || "Agustin 2.0 Chef",
      url: buildAbsoluteAppUrl(shareTarget?.url || ownChefShareUrl)
    };

    chefShareUrlNodes.forEach(node => {
      node.textContent = activeChefShare.url;
    });

    chefShareOpenLinks.forEach(node => {
      node.href = activeChefShare.url;
    });

    if (chefShareQrNode) {
      chefShareQrNode.src = buildChefQrImageUrl(activeChefShare.url);
      chefShareQrNode.alt = `Codigo QR para abrir ${activeChefShare.label}`;
    }
  };

  renderActiveChefShare(activeChefShare);

  const syncOrderCalcToggle = open => {
    if (orderCalcWrap) {
      orderCalcWrap.hidden = !open;
    }

    if (orderCalcToggle) {
      orderCalcToggle.setAttribute("aria-expanded", open ? "true" : "false");
      orderCalcToggle.textContent = open ? "Cerrar calculadora" : "Abrir calculadora";
    }

    if (open) {
      const orderCalcRoot = document.querySelector("[data-order-calc]");
      if (
        orderCalcRoot?.dataset.orderCalcSyncReady === "true" &&
        typeof orderCalcRoot.syncProductsFromSurvey === "function"
      ) {
        orderCalcRoot.syncProductsFromSurvey(getActiveCoachHealthSurveyContext());
      }
    }
  };

  syncOrderCalcToggle(false);

  if (chatMessages && floatingChatMessages) {
    floatingChatMessages.innerHTML = chatMessages.innerHTML;
    floatingChatMessages.scrollTop = floatingChatMessages.scrollHeight;
  }

  const clearFloatingCoachArmTimeout = () => {
    if (floatingCoachArmTimeout) {
      window.clearTimeout(floatingCoachArmTimeout);
      floatingCoachArmTimeout = null;
    }
  };

  const syncFloatingCoachDock = nextMode => {
    if (!floatingCoachToggle || !floatingCoachPanel) {
      return;
    }

    clearFloatingCoachArmTimeout();
    floatingCoachMode = nextMode;

    floatingCoachToggle.classList.toggle("is-armed", nextMode === "armed");
    floatingCoachToggle.classList.toggle("is-open", nextMode === "open");
    floatingCoachPanel.hidden = nextMode !== "open";
    floatingCoachToggle.setAttribute("aria-expanded", nextMode === "open" ? "true" : "false");

    if (nextMode === "open") {
      floatingCoachToggle.textContent = "Coach activo";
      window.requestAnimationFrame(() => {
        if (floatingChatMessages) {
          floatingChatMessages.scrollTop = floatingChatMessages.scrollHeight;
        }
        floatingChatInput?.focus();
      });
      return;
    }

    if (nextMode === "armed") {
      floatingCoachToggle.textContent = "Abrir Coach";
      floatingCoachArmTimeout = window.setTimeout(() => {
        syncFloatingCoachDock("idle");
      }, 2000);
      return;
    }

    floatingCoachToggle.textContent = "Coach";
  };

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
  syncFloatingCoachDock("idle");

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

  window.addEventListener("coach:open-chef-share", event => {
    renderActiveChefShare(event.detail || null);
    openChefShareModal();
  });

  const closeChefShareModal = () => {
    if (!chefShareModal) {
      return;
    }

    chefShareModal.hidden = true;
    document.body.classList.remove("modal-open");
  };

  const chatInputs = [chatInput, floatingChatInput].filter(Boolean);
  const chatSendButtons = [chatSendButton, floatingChatSendButton].filter(Boolean);
  const chatStatusNodes = [chatStatus, floatingChatStatus].filter(Boolean);

  const renderDemoStage = (stageId = getCoachDemoStageId()) => {
    const meta = getCoachDemoStageMeta(stageId);

    if (demoStageBadge) {
      demoStageBadge.textContent = meta.label;
    }

    if (demoStageName) {
      demoStageName.textContent = meta.label;
    }

    if (demoStageCopy) {
      demoStageCopy.textContent = meta.copy;
    }

    demoStageButtons.forEach(button => {
      button.classList.toggle("is-active", button.dataset.coachStageActivate === meta.id);
    });
  };

  const renderDemoEvents = (events = getCoachDemoEvents()) => {
    if (!demoEventsRoot) {
      return;
    }

    if (!events.length) {
      demoEventsRoot.innerHTML =
        '<div class="coach-demo-event-empty">Aun no hay actividad marcada en esta presentacion.</div>';
      return;
    }

    demoEventsRoot.innerHTML = events
      .map(
        event => `
          <div class="coach-demo-event">
            <strong>${escapeHtml(event.label || "Senal reciente")}</strong>
            <span>${escapeHtml(event.detail || "Sin detalle extra.")}</span>
          </div>
        `
      )
      .join("");
  };

  const syncDemoContextBar = detail => {
    renderDemoStage(detail?.stageId || getCoachDemoStageId());
    renderDemoEvents(Array.isArray(detail?.events) ? detail.events : getCoachDemoEvents());
  };

  window.addEventListener("coach:demo-sync", event => {
    syncDemoContextBar(event.detail || null);
  });

  demoStageButtons.forEach(button => {
    button.addEventListener("click", () => {
      const nextStage = button.dataset.coachStageActivate || COACH_DEMO_STAGE_CONFIG[0].id;
      const currentStage = getCoachDemoStageId();
      const stageMeta = getCoachDemoStageMeta(nextStage);
      setCoachDemoStageId(nextStage);

      registerCoachDemoEvent({
        id: `stage_${stageMeta.id}`,
        label: "Paso reportado",
        detail: stageMeta.label
      });

      if (currentStage !== stageMeta.id && stageMeta.coachReply) {
        addCoachMessage(null, "assistant", stageMeta.coachReply);
      }
    });
  });

  syncDemoContextBar();

  const toggleCoachChatBusy = loading => {
    chatInputs.forEach(input => {
      input.disabled = loading;
    });

    chatSendButtons.forEach(button => {
      button.disabled = loading;
    });

    chatStatusNodes.forEach(node => {
      node.textContent = loading ? "Pensando..." : "Listo";
    });
  };

  const clearCoachChatInputs = () => {
    chatInputs.forEach(input => {
      input.value = "";
      autoResizeTextarea(input);
    });
  };

  const sendCoachMessage = async rawText => {
    const text = String(rawText || "").trim();

    if (!text) {
      return;
    }

    addCoachMessage(chatMessages, "user", text);
    clearCoachChatInputs();
    toggleCoachChatBusy(true);

    try {
      const activeDemoStageMeta = getCoachDemoStageMeta(getCoachDemoStageId());
      const data = await apiRequest(COACH_CHAT_API_URL, {
        method: "POST",
        body: {
          pregunta: text,
          sessionId: getCoachChatSessionId(),
          visitorId: getCoachVisitorId(),
          activeWorkspace: window.sessionStorage.getItem(COACH_WORKSPACE_TAB_KEY) || "cierre",
          activeDemoStage: activeDemoStageMeta.id,
          activeDemoStageLabel: activeDemoStageMeta.label,
          activeDemoStageCopy: activeDemoStageMeta.copy,
          recentCoachEvents: getCoachDemoEvents(),
          activeHealthSurveyId: getActiveCoachHealthSurveyContext()?.id || "",
          activeProgram414SheetId: getActiveCoachProgram414Context()?.sheetId || "",
          activeProgram414ReferralIndex: Number.isInteger(getActiveCoachProgram414Context()?.referralIndex)
            ? getActiveCoachProgram414Context().referralIndex
            : "",
          activeOrderCalcContext: getActiveCoachOrderCalcContext() || null,
          activeDecisionContext: getActiveCoachDecisionContext() || null,
          activeDemoOutcomeContext: getActiveCoachDemoOutcomeContext() || null,
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
      if (data.activeHealthSurveyContext?.id) {
        setActiveCoachHealthSurveyContext(buildCoachHealthSurveyContext(data.activeHealthSurveyContext));
      }

      if (data.activeProgram414Context?.sheetId) {
        setActiveCoachProgram414Context(data.activeProgram414Context);
      }

      if (data.activeOrderCalcContext?.summary) {
        setActiveCoachOrderCalcContext(data.activeOrderCalcContext);
      }

      if (data.activeDecisionContext?.summary) {
        setActiveCoachDecisionContext(data.activeDecisionContext);
      }

      if (data.activeDemoOutcomeContext?.resultType) {
        setActiveCoachDemoOutcomeContext(data.activeDemoOutcomeContext);
      }
    } catch (error) {
      addCoachMessage(
        chatMessages,
        "assistant",
        error.message || "No pude responder en este momento."
      );
    } finally {
      toggleCoachChatBusy(false);

      if (floatingCoachMode === "open" && floatingChatInput) {
        floatingChatInput.focus();
      } else if (chatInput) {
        chatInput.focus();
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

  floatingChatForm?.addEventListener("submit", event => {
    event.preventDefault();
    sendCoachMessage(floatingChatInput?.value || "");
  });

  floatingChatInput?.addEventListener("input", () => {
    autoResizeTextarea(floatingChatInput);
  });

  floatingChatInput?.addEventListener("keydown", event => {
    if (event.key === "Enter" && !event.shiftKey) {
      event.preventDefault();
      sendCoachMessage(floatingChatInput?.value || "");
    }
  });

  autoResizeTextarea(chatInput);
  autoResizeTextarea(floatingChatInput);

  floatingCoachToggle?.addEventListener("click", () => {
    if (floatingCoachMode === "open") {
      floatingChatInput?.focus();
      return;
    }

    if (floatingCoachMode === "armed") {
      syncFloatingCoachDock("open");
      return;
    }

    syncFloatingCoachDock("armed");
  });

  floatingCoachClose?.addEventListener("click", () => {
    syncFloatingCoachDock("idle");
  });

  chefShareButtons.forEach(button => {
    button.addEventListener("click", event => {
      event.preventDefault();
      renderActiveChefShare({
        label: "Agustin 2.0 Chef",
        url: ownChefShareUrl
      });
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

    if (event.key === "Escape" && floatingCoachPanel && !floatingCoachPanel.hidden) {
      syncFloatingCoachDock("idle");
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
    const willOpen = royalOnePanel?.hidden ?? true;
    syncRoyalOneDock(willOpen);

    if (willOpen) {
      registerCoachDemoEvent({
        id: "royalone_ready",
        label: "Se preparo salida a RoyalOne",
        detail: "Ya estaban listos para intentar el pedido final."
      });
    }
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
      await copyTextToClipboard(activeChefShare.url);
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
        title: activeChefShare.label || "Agustin 2.0 Chef",
        text: "Te comparto Agustin 2.0 Chef para recetas y cocina saludable.",
        url: activeChefShare.url
      });
    } catch (error) {
      // noop
    }
  });

  orderCalcToggle?.addEventListener("click", () => {
    const isOpen = orderCalcWrap ? !orderCalcWrap.hidden : false;
    const willOpen = !isOpen;
    syncOrderCalcToggle(willOpen);

    if (willOpen) {
      registerCoachDemoEvent({
        id: "order_calc_open",
        label: "Se abrio calculadora de pedido",
        detail: "Ya entraron a hablar de precio, down payment o pagos."
      });
    }
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

    registerCoachDemoEvent({
      id: "water_zip_check",
      label: "Se reviso agua por ZIP",
      detail: `Consultaron el area ${zip} para hablar de calidad del agua.`
    });
    window.open("https://www.ewg.org/tapwater/", "_blank", "noopener,noreferrer");
  });

  royalOneOpenLink?.addEventListener("click", () => {
    registerCoachDemoEvent({
      id: "royalone_open",
      label: "Se abrio RoyalOne",
      detail: "La demo llego al intento de pedido final."
    });
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

    if (hasCoachAccess(data.user)) {
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
