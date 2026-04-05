import crypto from "node:crypto";
import path from "node:path";

const METALWORKS_CRM_SESSION_COOKIE = "cmwf_crm_session";
const METALWORKS_CRM_SESSION_DAYS = 30;
const METALWORKS_CRM_DEFAULT_EMAIL = "agustincalderon286@gmail.com";
const METALWORKS_CONTACT_PHONE_DISPLAY = "773 798 4107";
const METALWORKS_CONTACT_EMAIL = "agustincalderon286@gmail.com";
const METALWORKS_CRM_USER_PROFILES = {
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
const METALWORKS_ASSISTANT_MAX_MESSAGES_PER_DAY = Math.max(
  1,
  Number(process.env.METALWORKS_ASSISTANT_MAX_MESSAGES_PER_DAY || 20),
);
const METALWORKS_CALLBACK_TIME_ZONE = "America/Chicago";
const METALWORKS_ASSISTANT_HISTORY_LIMIT = 18;
const METALWORKS_ASSISTANT_NOTES_MARKER = "[Agustin Assistant Notes]";
const METALWORKS_ASSISTANT_PLACEHOLDER_NAME = "Website chat lead";
const METALWORKS_CRM_STATUS_OPTIONS = [
  "new",
  "contacted",
  "quoted",
  "booked",
  "won",
  "lost",
  "archived",
];
const METALWORKS_CRM_PUBLIC_EVENT_TYPES = new Set([
  "phone_click",
  "email_click",
  "quote_submit",
  "quote_submit_fallback",
  "assistant_open",
  "assistant_cta_click",
]);
const METALWORKS_ASSISTANT_SYSTEM_PROMPT = `
You are Agustin 2.0 for Chicago Metal Works & Fencing.

ROLE:
- You help website visitors for a Chicago metalwork business.
- Main services: railings, handrails, gate repair, gate installation, fence repair, fence fabrication, mobile welding, custom metal fabrication, stairs, balconies, porch railings, ornamental ironwork.

GOAL:
- Help the visitor quickly understand the next best step.
- Increase conversions to quote request or phone call.
- Qualify the job without sounding pushy.

VOICE:
- Default to English.
- If the visitor writes in Spanish, you may answer in Spanish.
- Sound practical, friendly, clear, and contractor-like.
- Do not sound like a generic corporate chatbot.
- Keep it short.

RULES:
- Most replies should be 2 or 3 short sentences.
- Ask for photos early when that will help.
- Ask whether it is repair, replacement, or new installation when useful.
- Ask for ZIP code or job location when useful.
- Ask for the best phone number only when it helps move the quote forward.
- If the visitor asks for a callback or phone call, collect name, best phone number, best day/time to call, and job ZIP code in as few messages as possible.
- If the job sounds unsafe or urgent, tell them to call 773 798 4107 now.
- Do not give exact final pricing without enough detail.
- If enough context exists, you may give a rough range and clearly frame it as preliminary.
- Push toward quote form or phone call when the visitor shows real buying intent.

SERVICE FIT:
- High-fit: metalwork, welding, railings, handrails, gates, fences, fabrication, stairs, balconies, porch railings.
- Low-fit: painting, flooring, handyman-only work, foundation-only work, door-only work that is not metal-related.
- If the project is low-fit, politely say the business mainly focuses on metalwork and related repairs or fabrication.

QUOTE GUIDANCE:
- Fastest quote path: photos, rough measurements, ZIP code, and whether the project is repair or new build.
- If the visitor asks price too early, ask for photos and measurements first.

DO NOT:
- Do not talk about Chef, Coach, Royal Prestige, cooking, product sales, or internal distributor tools.
- Do not invent licensing, permits, warranties, or timelines you do not know.
- Do not make safety guarantees or structural promises.
`;

function cleanText(value = "", maxLength = 0) {
  const text = String(value || "").replace(/\s+/g, " ").trim();
  return maxLength > 0 ? text.slice(0, maxLength) : text;
}

function normalizeEmail(value = "") {
  return cleanText(value).toLowerCase();
}

function normalizePhone(value = "") {
  const digits = String(value || "").replace(/\D/g, "");

  if (digits.length === 11 && digits.startsWith("1")) {
    return digits.slice(1);
  }

  if (digits.length === 10) {
    return digits;
  }

  return digits.slice(0, 15);
}

function parseEmailList(value = "") {
  return String(value || "")
    .split(/[,\n;]/)
    .map((item) => normalizeEmail(item))
    .filter(Boolean);
}

function getMetalworksNotificationEmails() {
  return Array.from(
    new Set(
      parseEmailList(
        process.env.METALWORKS_LEAD_NOTIFY_EMAILS ||
          process.env.METALWORKS_LEAD_NOTIFY_EMAIL ||
          METALWORKS_CONTACT_EMAIL,
      ),
    ),
  ).filter(Boolean);
}

function escapeRegex(value = "") {
  return String(value || "").replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

function generateToken() {
  return crypto.randomBytes(32).toString("base64url");
}

function hashToken(token = "") {
  return crypto.createHash("sha256").update(String(token || "")).digest("hex");
}

function parseCookies(header = "") {
  return String(header || "")
    .split(";")
    .map((item) => item.trim())
    .filter(Boolean)
    .reduce((accumulator, fragment) => {
      const separatorIndex = fragment.indexOf("=");

      if (separatorIndex === -1) {
        return accumulator;
      }

      const key = fragment.slice(0, separatorIndex).trim();
      const value = decodeURIComponent(fragment.slice(separatorIndex + 1).trim());
      accumulator[key] = value;
      return accumulator;
    }, {});
}

function requestIsSecure(req) {
  return Boolean(req.secure || req.headers["x-forwarded-proto"] === "https");
}

function compareSecrets(input = "", expected = "") {
  const left = Buffer.from(String(input || ""), "utf8");
  const right = Buffer.from(String(expected || ""), "utf8");

  if (!left.length || left.length !== right.length) {
    return false;
  }

  return crypto.timingSafeEqual(left, right);
}

function getClientIp(req) {
  const forwarded = String(req.headers["x-forwarded-for"] || "")
    .split(",")
    .map((item) => item.trim())
    .filter(Boolean)[0];

  return forwarded || req.ip || req.socket?.remoteAddress || "";
}

function getAllowedEmails() {
  return Array.from(
    new Set([
      METALWORKS_CRM_DEFAULT_EMAIL,
      ...Object.keys(METALWORKS_CRM_USER_PROFILES),
      ...parseEmailList(process.env.METALWORKS_CRM_ALLOWED_EMAILS || ""),
    ]),
  ).filter(Boolean);
}

function getMetalworksPassword() {
  return String(process.env.METALWORKS_CRM_PASSWORD || "").trim();
}

function metalworksCrmConfigured() {
  return Boolean(getAllowedEmails().length && getMetalworksPassword());
}

function getMetalworksCrmProfile(email = "") {
  const safeEmail = normalizeEmail(email || "");
  const preset = METALWORKS_CRM_USER_PROFILES[safeEmail] || {};

  return {
    email: safeEmail,
    displayName: preset.displayName || (safeEmail ? safeEmail.split("@")[0] : "CMWF Admin"),
    skin: preset.skin || "classic",
    themeLabel: preset.themeLabel || "",
  };
}

function normalizeStatus(value = "") {
  const status = cleanText(value).toLowerCase();
  return METALWORKS_CRM_STATUS_OPTIONS.includes(status) ? status : "new";
}

function normalizeMoney(value = 0) {
  const amount = Number(value || 0);

  if (!Number.isFinite(amount)) {
    return 0;
  }

  return Math.max(0, Math.round(amount * 100) / 100);
}

function parseDateOnly(value = "") {
  const safeValue = cleanText(value || "", 40);

  if (!safeValue) {
    return null;
  }

  if (safeValue instanceof Date) {
    return Number.isNaN(safeValue.getTime()) ? null : safeValue;
  }

  const normalized = /^\d{4}-\d{2}-\d{2}$/.test(safeValue)
    ? `${safeValue}T12:00:00`
    : safeValue;
  const parsed = new Date(normalized);
  return Number.isNaN(parsed.getTime()) ? null : parsed;
}

function formatMoneyLabel(value = 0) {
  return new Intl.NumberFormat("en-US", {
    style: "currency",
    currency: "USD",
    maximumFractionDigits: 2,
  }).format(normalizeMoney(value));
}

function formatDateLabel(value = "") {
  if (!value) {
    return "";
  }

  const date = value instanceof Date ? value : new Date(value);

  if (Number.isNaN(date.getTime())) {
    return "";
  }

  return date.toLocaleDateString("en-US", {
    month: "short",
    day: "numeric",
    year: "numeric",
  });
}

function formatDateTimeLabel(value = "", timeZone = "") {
  if (!value) {
    return "";
  }

  const date = value instanceof Date ? value : new Date(value);

  if (Number.isNaN(date.getTime())) {
    return "";
  }

  const options = {
    month: "short",
    day: "numeric",
    year: "numeric",
    hour: "numeric",
    minute: "2-digit",
  };

  if (timeZone) {
    try {
      return date.toLocaleString("en-US", {
        ...options,
        timeZone,
      });
    } catch {}
  }

  return date.toLocaleString("en-US", options);
}

function escapeHtmlMarkup(value = "") {
  return String(value || "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#39;");
}

function formatMultilineHtml(value = "") {
  return escapeHtmlMarkup(value).replace(/\n/g, "<br />");
}

function buildMetalworksEstimateEmail(lead = null, replyTo = "") {
  const fullName = cleanText(lead?.fullName || "", 120);
  const firstName = fullName.split(/\s+/).filter(Boolean)[0] || "there";
  const projectLabel =
    cleanText(lead?.estimateTitle || "", 160) ||
    cleanText(lead?.projectType || "", 120) ||
    "metal repair project";
  const total = formatMoneyLabel(lead?.estimateAmount || 0);
  const validUntil = formatDateLabel(lead?.estimateValidUntil || "");
  const location = cleanText(lead?.location || "", 160);
  const scope = cleanText(lead?.estimateScope || lead?.details || "", 2400);
  const notes = cleanText(lead?.estimateNotes || "", 2400);
  const subject = `Estimate from Chicago Metal Works & Fencing - ${projectLabel}`;
  const textLines = [
    `Hi ${firstName},`,
    "",
    "Thank you for contacting Chicago Metal Works & Fencing.",
    "",
    `Project: ${projectLabel}`,
    `Estimated total: ${total}`,
    validUntil ? `Valid until: ${validUntil}` : "",
    location ? `Location: ${location}` : "",
    "",
    scope ? `Scope of work:\n${scope}` : "",
    notes ? `Notes / exclusions:\n${notes}` : "",
    "",
    `To move forward, reply to this email or call/text ${METALWORKS_CONTACT_PHONE_DISPLAY}.`,
    "",
    "Chicago Metal Works & Fencing",
    METALWORKS_CONTACT_PHONE_DISPLAY,
    METALWORKS_CONTACT_EMAIL,
  ].filter(Boolean);

  const html = `
    <div style="font-family:Arial,sans-serif;background:#f8f5ef;padding:24px;color:#1e2428">
      <div style="max-width:680px;margin:0 auto;background:#ffffff;border:1px solid #e5ddd0;border-radius:18px;padding:28px">
        <p style="margin:0 0 16px">Hi ${escapeHtmlMarkup(firstName)},</p>
        <p style="margin:0 0 16px">Thank you for contacting <strong>Chicago Metal Works &amp; Fencing</strong>.</p>
        <div style="border:1px solid #eadfcd;border-radius:16px;padding:18px;margin:0 0 18px;background:#fffaf2">
          <p style="margin:0 0 10px"><strong>Project:</strong> ${escapeHtmlMarkup(projectLabel)}</p>
          <p style="margin:0 0 10px"><strong>Estimated total:</strong> ${escapeHtmlMarkup(total)}</p>
          ${validUntil ? `<p style="margin:0 0 10px"><strong>Valid until:</strong> ${escapeHtmlMarkup(validUntil)}</p>` : ""}
          ${location ? `<p style="margin:0"><strong>Location:</strong> ${escapeHtmlMarkup(location)}</p>` : ""}
        </div>
        ${
          scope
            ? `<div style="margin:0 0 18px"><p style="margin:0 0 8px"><strong>Scope of work</strong></p><p style="margin:0;white-space:pre-wrap">${formatMultilineHtml(scope)}</p></div>`
            : ""
        }
        ${
          notes
            ? `<div style="margin:0 0 18px"><p style="margin:0 0 8px"><strong>Notes / exclusions</strong></p><p style="margin:0;white-space:pre-wrap">${formatMultilineHtml(notes)}</p></div>`
            : ""
        }
        <p style="margin:0 0 18px">To move forward, reply to this email or call/text <strong>${escapeHtmlMarkup(METALWORKS_CONTACT_PHONE_DISPLAY)}</strong>.</p>
        <p style="margin:0;color:#66717a">
          Chicago Metal Works &amp; Fencing<br />
          ${escapeHtmlMarkup(METALWORKS_CONTACT_PHONE_DISPLAY)}<br />
          ${escapeHtmlMarkup(METALWORKS_CONTACT_EMAIL)}
        </p>
      </div>
    </div>
  `;

  return {
    to: normalizeEmail(lead?.email || ""),
    subject,
    text: textLines.join("\n"),
    html,
    replyTo: normalizeEmail(replyTo || "") || METALWORKS_CONTACT_EMAIL,
  };
}

async function sendMetalworksEstimateEmail(lead = null, replyTo = "") {
  const apiKey = String(process.env.RESEND_API_KEY || "").trim();
  const fromEmail = String(process.env.RESEND_FROM_EMAIL || "").trim();

  if (!apiKey || !fromEmail) {
    return {
      attempted: false,
      delivered: false,
      error: "Email sending is not configured yet.",
    };
  }

  const payload = buildMetalworksEstimateEmail(lead, replyTo);

  if (!payload.to) {
    return {
      attempted: false,
      delivered: false,
      error: "This lead does not have an email address yet.",
    };
  }

  const body = {
    from: fromEmail,
    to: [payload.to],
    subject: payload.subject,
    text: payload.text,
    html: payload.html,
    reply_to: payload.replyTo,
  };

  const response = await fetch("https://api.resend.com/emails", {
    method: "POST",
    headers: {
      Authorization: `Bearer ${apiKey}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify(body),
  });

  if (!response.ok) {
    const errorData = await response.json().catch(() => ({}));
    return {
      attempted: true,
      delivered: false,
      status: response.status,
      error: errorData?.message || `Email service responded ${response.status}.`,
    };
  }

  return {
    attempted: true,
    delivered: true,
    status: response.status,
  };
}

function buildMetalworksLeadAlertEmail({
  lead = null,
  alertType = "lead",
  requestedAt = "",
  requestedAtLabel = "",
  timeZone = "",
  pagePath = "",
  pageUrl = "",
  conversationDigest = "",
} = {}) {
  const fullName = cleanText(lead?.fullName || "", 120) || "Lead";
  const projectLabel =
    cleanText(lead?.projectType || "", 120) ||
    cleanText(lead?.estimateTitle || "", 160) ||
    "metalwork request";
  const location = cleanText(lead?.location || "", 160) || "Not provided";
  const phone = cleanText(lead?.phoneDisplay || lead?.phone || "", 40) || "Not provided";
  const email = normalizeEmail(lead?.email || "") || "Not provided";
  const details = cleanText(lead?.details || "", 2400) || "No details provided.";
  const notifyTo = getMetalworksNotificationEmails();
  const callbackLabel =
    cleanText(requestedAtLabel || "", 120) ||
    formatDateTimeLabel(requestedAt, timeZone) ||
    "No time selected";
  const subjectPrefix =
    alertType === "assistant_callback" ? "New assistant callback request" : "New Metal Works lead";
  const subject = `${subjectPrefix} - ${fullName}`;
  const intro =
    alertType === "assistant_callback"
      ? "A website visitor asked Agustin 2.0 for a callback and the request was saved in the CRM."
      : "A new lead was saved in the Chicago Metal Works CRM.";
  const textLines = [
    intro,
    "",
    `Name: ${fullName}`,
    `Phone: ${phone}`,
    `Email: ${email}`,
    `Project type: ${projectLabel}`,
    `Location: ${location}`,
    alertType === "assistant_callback" ? `Requested callback time: ${callbackLabel}` : "",
    pagePath ? `Page: ${pagePath}` : "",
    pageUrl ? `URL: ${pageUrl}` : "",
    "",
    "Project details:",
    details,
    conversationDigest ? "Recent assistant conversation:" : "",
    conversationDigest || "",
  ].filter(Boolean);
  const html = `
    <div style="font-family:Arial,sans-serif;background:#f8f5ef;padding:24px;color:#1e2428">
      <div style="max-width:700px;margin:0 auto;background:#ffffff;border:1px solid #e5ddd0;border-radius:18px;padding:28px">
        <p style="margin:0 0 16px">${escapeHtmlMarkup(intro)}</p>
        <div style="border:1px solid #eadfcd;border-radius:16px;padding:18px;margin:0 0 18px;background:#fffaf2">
          <p style="margin:0 0 10px"><strong>Name:</strong> ${escapeHtmlMarkup(fullName)}</p>
          <p style="margin:0 0 10px"><strong>Phone:</strong> ${escapeHtmlMarkup(phone)}</p>
          <p style="margin:0 0 10px"><strong>Email:</strong> ${escapeHtmlMarkup(email)}</p>
          <p style="margin:0 0 10px"><strong>Project type:</strong> ${escapeHtmlMarkup(projectLabel)}</p>
          <p style="margin:0 0 10px"><strong>Location:</strong> ${escapeHtmlMarkup(location)}</p>
          ${
            alertType === "assistant_callback"
              ? `<p style="margin:0 0 10px"><strong>Requested callback time:</strong> ${escapeHtmlMarkup(callbackLabel)}</p>`
              : ""
          }
          ${pagePath ? `<p style="margin:0 0 10px"><strong>Page:</strong> ${escapeHtmlMarkup(pagePath)}</p>` : ""}
          ${pageUrl ? `<p style="margin:0"><strong>URL:</strong> ${escapeHtmlMarkup(pageUrl)}</p>` : ""}
        </div>
        <div style="margin:0 0 18px">
          <p style="margin:0 0 8px"><strong>Project details</strong></p>
          <p style="margin:0;white-space:pre-wrap">${formatMultilineHtml(details)}</p>
        </div>
        ${
          conversationDigest
            ? `<div style="margin:0"><p style="margin:0 0 8px"><strong>Recent assistant conversation</strong></p><p style="margin:0;white-space:pre-wrap">${formatMultilineHtml(conversationDigest)}</p></div>`
            : ""
        }
      </div>
    </div>
  `;

  return {
    to: notifyTo,
    subject,
    text: textLines.join("\n"),
    html,
    replyTo: normalizeEmail(lead?.email || "") || METALWORKS_CONTACT_EMAIL,
  };
}

async function sendMetalworksLeadAlertEmail(options = {}) {
  const apiKey = String(process.env.RESEND_API_KEY || "").trim();
  const fromEmail = String(process.env.RESEND_FROM_EMAIL || "").trim();

  if (!apiKey || !fromEmail) {
    return {
      attempted: false,
      delivered: false,
      error: "Email sending is not configured yet.",
    };
  }

  const payload = buildMetalworksLeadAlertEmail(options);

  if (!payload.to.length) {
    return {
      attempted: false,
      delivered: false,
      error: "No destination email is configured for Metal Works lead alerts.",
    };
  }

  const response = await fetch("https://api.resend.com/emails", {
    method: "POST",
    headers: {
      Authorization: `Bearer ${apiKey}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      from: fromEmail,
      to: payload.to,
      subject: payload.subject,
      text: payload.text,
      html: payload.html,
      reply_to: payload.replyTo,
    }),
  });

  if (!response.ok) {
    const errorData = await response.json().catch(() => ({}));
    return {
      attempted: true,
      delivered: false,
      status: response.status,
      error: errorData?.message || `Email service responded ${response.status}.`,
    };
  }

  return {
    attempted: true,
    delivered: true,
    status: response.status,
  };
}

function buildTrackingPayload(payload = {}) {
  return {
    gclid: cleanText(payload?.gclid || "", 120),
    gbraid: cleanText(payload?.gbraid || "", 120),
    wbraid: cleanText(payload?.wbraid || "", 120),
    utmSource: cleanText(payload?.utmSource || "", 80),
    utmMedium: cleanText(payload?.utmMedium || "", 80),
    utmCampaign: cleanText(payload?.utmCampaign || "", 120),
    utmTerm: cleanText(payload?.utmTerm || "", 120),
    utmContent: cleanText(payload?.utmContent || "", 120),
    landingPath: cleanText(payload?.landingPath || "", 240),
    landingUrl: cleanText(payload?.landingUrl || "", 500),
    referrer: cleanText(payload?.referrer || "", 500),
  };
}

function labelStatus(status = "") {
  const labels = {
    new: "Nuevo",
    contacted: "Contactado",
    quoted: "Cotizado",
    booked: "Agendado",
    won: "Ganado",
    lost: "Perdido",
    archived: "Archivado",
  };

  return labels[normalizeStatus(status)] || "Nuevo";
}

function formatActivityTitle(type = "") {
  const labels = {
    quote_submit: "Quote enviado",
    quote_submit_fallback: "Quote por email",
    phone_click: "Click al telefono",
    email_click: "Click al correo",
    lead_created: "Lead creado",
    lead_updated: "Lead actualizado",
    note_added: "Nota agregada",
    estimate_sent: "Estimate enviado",
    assistant_open: "Assistant abierto",
    assistant_cta_click: "Assistant CTA",
    assistant_user_message: "Mensaje al assistant",
    assistant_ai_reply: "Respuesta del assistant",
    assistant_fallback: "Fallback del assistant",
    assistant_booking_requested: "Cita pedida desde assistant",
  };

  return labels[type] || "Actividad";
}

function detectSpanish(value = "") {
  return /[¿¡]|\b(hola|precio|cotiza|reparacion|reparación|porton|portón|barandal|soldadura|cerca|reja|gracias|necesito|quiero|ayuda)\b/i.test(
    String(value || ""),
  );
}

function buildAssistantFallbackReply(message = "", conversationState = null) {
  const text = cleanText(message, 500).toLowerCase();
  const inSpanish = detectSpanish(message);
  const callbackIntent = conversationState?.callbackIntent === "yes";
  const callbackMissingFields = Array.isArray(conversationState?.callbackMissingFields)
    ? conversationState.callbackMissingFields
    : [];

  if (callbackIntent) {
    if (callbackMissingFields.length) {
      const missingLabel = callbackMissingFields.join(", ");

      return inSpanish
        ? `Claro. Para dejar la llamada bien pedida mandame solo esto: ${missingLabel}.`
        : `Absolutely. To save the callback request, send just these details: ${missingLabel}.`;
    }

    const callbackLabel =
      cleanText(conversationState?.callbackLabel || "", 120) || "your requested time";

    return inSpanish
      ? `Perfecto. Ya tengo tu solicitud de llamada para ${callbackLabel}. Si puedes, manda fotos y tu ZIP code para preparar mejor el seguimiento.`
      : `Perfect. I have your callback request for ${callbackLabel}. If you can, send photos and your ZIP code so we can prep the follow-up faster.`;
  }

  if (
    /unsafe|danger|falling|broken loose|asap|today|urgent|emergency|unsafe stair|unsafe railing/i.test(
      text,
    )
  ) {
    return inSpanish
      ? "Si la pieza esta floja, peligrosa o urgente, llama ahora al 773 798 4107. Si puedes, manda fotos y tu ZIP code para decirte mas rapido si parece reparacion o reemplazo."
      : "If the metalwork is loose, unsafe, or urgent, call 773 798 4107 now. If you can also send photos and your ZIP code, we can tell you faster whether it looks like a repair or a replacement.";
  }

  if (/price|pricing|quote|estimate|cost|how much|precio|cotiza|estimate/i.test(text)) {
    return inSpanish
      ? "La forma mas rapida de cotizar es mandar fotos, medidas aproximadas, tu ZIP code y decir si es reparacion o trabajo nuevo. Si quieres moverlo mas rapido, usa el formulario o llama al 773 798 4107."
      : "The fastest way to get pricing is to send photos, rough measurements, your ZIP code, and whether you need a repair or a new build. If you want to move faster, use the quote form or call 773 798 4107.";
  }

  if (/gate|gates|hinge|latch|dragging|sagging|porton|portón/i.test(text)) {
    return inSpanish
      ? "Si, ayudamos con reparacion de portones, bisagras, latches, portones arrastrando y portones nuevos. Manda una foto, dime si es reparacion o reemplazo, y agrega tu ZIP code para decirte el siguiente paso."
      : "Yes, we help with gate repair, hinges, latches, dragging gates, and new metal gates. Send a photo, tell me if it is a repair or replacement, and include your ZIP code so I can point you to the next step.";
  }

  if (/railing|handrail|stairs|stair|balcony|porch|barandal|pasamano|pasamanos/i.test(text)) {
    return inSpanish
      ? "Trabajamos barandales, pasamanos, escaleras, balcones y porches. Si mandas fotos, medidas aproximadas y tu ZIP code, te digo si parece reparacion o instalacion nueva."
      : "We work on porch railings, handrails, stairs, balconies, and related repairs or replacement work. If you send photos, rough measurements, and your ZIP code, I can help you figure out whether it looks like a repair or a new install.";
  }

  if (/fence|fencing|ornamental|iron fence|metal fence|cerca|reja/i.test(text)) {
    return inSpanish
      ? "Si, hacemos reparacion de cercas metalicas, secciones dañadas, fabricacion y trabajo nuevo. Manda unas fotos, dime si es reparacion o nuevo trabajo, y agrega tu ZIP code."
      : "Yes, we handle metal fence repair, damaged sections, fabrication, and new fence work. Send a few photos, tell me if it is repair or new work, and include your ZIP code.";
  }

  if (/weld|welding|mobile welding|on[- ]site|solda/i.test(text)) {
    return inSpanish
      ? "Si hacemos soldadura y reparaciones de metal en muchos trabajos del area de Chicago. Manda fotos de la pieza o del daño, junto con la ubicacion, y te digo el mejor siguiente paso."
      : "Yes, we do welding and metal repair work for many Chicago-area projects. Send photos of the damaged metalwork or the piece you want built, along with the job location, and I’ll point you to the best next step.";
  }

  if (/where|service area|zip|coverage|chicago|blue island|suburb|cobertura/i.test(text)) {
    return inSpanish
      ? "Trabajamos Chicago, Blue Island y suburbios cercanos. Mandame tu ciudad o ZIP code con una nota corta del proyecto y te confirmo cobertura."
      : "We serve Chicago, Blue Island, and nearby suburbs. Send your city or ZIP code with a short note about the project, and I can confirm coverage fast.";
  }

  return inSpanish
    ? "Puedo ayudar con portones, barandales, cercas, soldadura y fabricacion metalica. Dime que necesita reparacion o que quieres construir, agrega tu ZIP code, y si tienes fotos usa el quote form para moverlo mas rapido."
    : "I can help with gates, railings, fence work, welding, and custom metal fabrication. Tell me what needs repair or what you want built, include your ZIP code, and if you have photos, use the quote form so we can move faster.";
}

function buildAssistantContext(message = "", pagePath = "") {
  const text = cleanText(message, 500).toLowerCase();
  const contextParts = [];

  contextParts.push(`
METAL WORKS WEBSITE CONTEXT:
- Business: Chicago Metal Works & Fencing
- Service area: Chicago, Blue Island, and nearby suburbs
- Main CTA phone: 773 798 4107
- Best quote path: photos, rough measurements, ZIP code, and whether the job is repair or new build
- Public website page: ${cleanText(pagePath || "", 120) || "/"}
`);

  if (/price|pricing|quote|estimate|cost|how much|precio|cotiza/i.test(text)) {
    contextParts.push(`
QUOTE RULE:
- Do not give a firm final price without enough detail.
- Ask for photos, rough measurements, ZIP code, and whether the job is repair or new installation.
- If enough context exists, frame price as a preliminary range.
`);
  }

  if (/gate|hinge|latch|dragging|sagging|porton|portón/i.test(text)) {
    contextParts.push(`
GATE CONTEXT:
- Common issues: dragging gates, latch issues, hinge issues, frame repairs, rewelds, replacement sections.
- Move toward photo + repair vs replace + ZIP code.
`);
  }

  if (/railing|handrail|stairs|stair|balcony|porch|barandal|pasamano|pasamanos/i.test(text)) {
    contextParts.push(`
RAILING CONTEXT:
- Typical work: porch railings, stair handrails, balcony railings, repairs, replacements, new installs.
- Ask where the railing is located and whether it is repair or new work.
`);
  }

  if (/fence|fencing|ornamental|iron fence|metal fence|cerca|reja/i.test(text)) {
    contextParts.push(`
FENCE CONTEXT:
- Typical work: metal fence repair, damaged sections, ornamental fence fabrication, new fence installs.
- Ask for photos and the damaged area or total linear area if known.
`);
  }

  if (/weld|welding|mobile welding|on[- ]site|solda/i.test(text)) {
    contextParts.push(`
WELDING CONTEXT:
- Mobile welding and on-site metal repairs are part of the service mix.
- Ask what piece needs welding, whether it is still installed, and where the job is located.
`);
  }

  if (
    /painting|floor|flooring|handyman|foundation|drywall|plumbing|electric|electrical|roof|door only/i.test(
      text,
    )
  ) {
    contextParts.push(`
FIT FILTER:
- The company mainly focuses on metalwork, welding, gates, fences, railings, stairs, and fabrication.
- If the request is not really metal-related, say so politely and do not oversell.
`);
  }

  return contextParts.join("\n");
}

function normalizeAssistantHistory(history = []) {
  return (Array.isArray(history) ? history : [])
    .map((item) => ({
      role: item?.role === "assistant" ? "assistant" : "user",
      content: cleanText(item?.content || "", 500),
    }))
    .filter((item) => item.content)
    .slice(-6);
}

function buildAssistantHistoryDigest(history = []) {
  return normalizeAssistantHistory(history)
    .map((item) => `${item.role === "assistant" ? "Agustin" : "Visitor"}: ${item.content}`)
    .join("\n")
    .slice(0, 1800);
}

function normalizeAssistantSearchText(value = "") {
  return String(value || "")
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .toLowerCase()
    .replace(/\s+/g, " ")
    .trim();
}

function selectAssistantText(...values) {
  for (const value of values) {
    const safeValue = cleanText(value || "", 160);

    if (safeValue) {
      return safeValue;
    }
  }

  return "";
}

function selectAssistantLongestText(...values) {
  return values
    .map((value) => cleanText(value || "", 320))
    .filter(Boolean)
    .sort((left, right) => right.length - left.length)[0] || "";
}

function mergeAssistantUniqueValues(...lists) {
  return Array.from(
    new Set(
      lists.flatMap((list) => (Array.isArray(list) ? list : [list])).map((item) => cleanText(item || "", 120)).filter(Boolean),
    ),
  );
}

function extractAssistantContactInfo(text = "") {
  const source = String(text || "");
  const emailMatch = source.match(/[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}/i);
  const phoneMatch = source.match(
    /(?:\+?1[\s.-]?)?(?:\(?\d{3}\)?[\s.-]?\d{3}[\s.-]?\d{4})/,
  );
  const email = emailMatch ? normalizeEmail(emailMatch[0]) : "";
  const phoneDisplay = phoneMatch ? cleanText(phoneMatch[0], 40) : "";
  const phone = phoneDisplay ? normalizePhone(phoneDisplay) : "";

  return {
    email,
    phone,
    phoneDisplay,
  };
}

function assistantNameLooksReliable(value = "") {
  const safeValue = cleanText(value || "", 80).replace(/[.,;!?]+$/, "");

  if (!safeValue) {
    return false;
  }

  const normalized = normalizeAssistantSearchText(safeValue);
  const blockedPattern =
    /\b(quote|estimate|repair|gate|fence|railing|welding|fabrication|project|callback|call|phone|number|email|zip|address|service|need|looking|quiero|necesito|cotizacion|cotiza|llamada|telefono|correo|zip code|today|tomorrow|monday|tuesday|wednesday|thursday|friday|saturday|sunday|hoy|manana|lunes|martes|miercoles|jueves|viernes|sabado|domingo|morning|afternoon|evening|night|weekday|weekend|am|pm)\b/;

  if (blockedPattern.test(normalized)) {
    return false;
  }

  const words = safeValue.split(/\s+/).filter(Boolean);
  return words.length >= 1 && words.length <= 4;
}

function sanitizeAssistantStoredName(value = "") {
  const safeValue = cleanText(value || "", 80).replace(/[.,;!?]+$/, "");
  const normalized = normalizeAssistantSearchText(safeValue);

  if (!safeValue) {
    return "";
  }

  if (
    normalized === normalizeAssistantSearchText(METALWORKS_ASSISTANT_PLACEHOLDER_NAME) ||
    normalized === "lead" ||
    normalized === "website lead"
  ) {
    return "";
  }

  return safeValue;
}

function extractAssistantName(text = "") {
  const patterns = [
    /(?:my name is|this is|mi nombre es|soy)\s+([a-zA-ZÀ-ÿ' -]{2,60})/i,
    /(?:i am|i'm|im|me llamo)\s+([a-zA-ZÀ-ÿ' -]{2,60})/i,
  ];

  for (const pattern of patterns) {
    const match = String(text || "").match(pattern);

    if (!match?.[1]) {
      continue;
    }

    const candidate = match[1]
      .split(/\s+(?:and|y)\s+(?:my|mi)\b/i)[0]
      .split(/\s+(?:phone|telefono|tel|email|correo|zip|address|direccion)\b/i)[0]
      .trim()
      .replace(/[.,;!?]+$/, "");

    if (assistantNameLooksReliable(candidate)) {
      return candidate;
    }
  }

  return "";
}

function extractAssistantZipCode(text = "") {
  const source = String(text || "");
  const zipMatch = source.match(/\b(\d{5})(?:-\d{4})?\b/);
  return zipMatch ? zipMatch[1] : "";
}

function inferAssistantProjectTypeFromText(text = "") {
  const normalized = normalizeAssistantSearchText(text);

  if (!normalized) {
    return "";
  }

  if (/\b(porch|landing|rust|repaint|primer|top coat|paint failure)\b/.test(normalized)) {
    return "Metal porch repair / restoration";
  }

  if (/\b(gate|hinge|latch|dragging|sagging|porton)\b/.test(normalized)) {
    return "Gate fabrication / gate repair";
  }

  if (/\b(railing|handrail|stairs|stair|balcony|barandal|pasamano|pasamanos)\b/.test(normalized)) {
    return "Custom railings / handrails";
  }

  if (/\b(fence|fencing|ornamental|iron fence|metal fence|cerca|reja)\b/.test(normalized)) {
    return "Fence fabrication / fence repair";
  }

  if (/\b(weld|welding|mobile welding|solda)\b/.test(normalized)) {
    return "Mobile welding";
  }

  if (/\b(fabricat|custom metal|custom build)\b/.test(normalized)) {
    return "Custom metal fabrication";
  }

  if (/\b(repair|broken|damage|loose|replace|replacement|install)\b/.test(normalized)) {
    return "Metal repair";
  }

  return "";
}

function extractAssistantLocation(text = "") {
  const safeText = String(text || "");
  const zipCode = extractAssistantZipCode(safeText);

  if (zipCode) {
    return `ZIP ${zipCode}`;
  }

  const cityLabels = [
    "Chicago",
    "Blue Island",
    "Oak Lawn",
    "Evergreen Park",
    "Cicero",
    "Berwyn",
    "Bridgeview",
    "Burbank",
    "Alsip",
    "Crestwood",
    "Tinley Park",
  ];
  const normalized = normalizeAssistantSearchText(safeText);

  for (const city of cityLabels) {
    if (normalized.includes(normalizeAssistantSearchText(city))) {
      return city;
    }
  }

  const addressMatch = safeText.match(
    /(?:address is|located at|at|direccion es|estoy en|vivo en)\s+([^.\n]+)/i,
  );

  if (addressMatch?.[1]) {
    return cleanText(addressMatch[1], 160).replace(/[.,;!?]+$/, "");
  }

  return "";
}

function detectAssistantCallbackIntent(text = "") {
  const normalized = normalizeAssistantSearchText(text);

  return /(?:call me|give me a call|can you call|can someone call|talk by phone|talk on the phone|phone call|schedule a call|set up a call|reach me|follow up by phone|llamame|llamarme|me pueden llamar|quiero una llamada|quiero llamada|agendar llamada|agendar una llamada|hablar por telefono|marcame)/.test(
    normalized,
  );
}

function detectAssistantCallbackDecline(text = "") {
  const normalized = normalizeAssistantSearchText(text);

  return /(?:do not call|dont call|no call please|prefer email|text only|solo texto|no me llamen|no quiero llamada|sin llamada|prefiero mensaje|prefiero texto)/.test(
    normalized,
  );
}

function extractAssistantPreferredDay(text = "") {
  const normalized = normalizeAssistantSearchText(text);
  const patterns = [
    /(?:best day(?: to call)?|prefer(?:ably)?|call me|llamame|me pueden llamar|mejor dia(?: para llamar)?|prefiero)\s+([^.\n]+)/i,
    /\b(today|tomorrow|weekday|weekdays|weekend|monday|tuesday|wednesday|thursday|friday|saturday|sunday|hoy|manana|entre semana|fin de semana|lunes|martes|miercoles|jueves|viernes|sabado|domingo)\b/i,
  ];
  const catalog = [
    "entre semana",
    "fin de semana",
    "weekday",
    "weekdays",
    "weekend",
    "hoy",
    "today",
    "manana",
    "tomorrow",
    "lunes",
    "monday",
    "martes",
    "tuesday",
    "miercoles",
    "wednesday",
    "jueves",
    "thursday",
    "viernes",
    "friday",
    "sabado",
    "saturday",
    "domingo",
    "sunday",
  ];

  for (const pattern of patterns) {
    const match = normalized.match(pattern);

    if (!match) {
      continue;
    }

    const segment = cleanText(match[1] || match[0], 80);
    const normalizedSegment = normalizeAssistantSearchText(segment);

    for (const item of catalog) {
      if (normalizedSegment.includes(item)) {
        return item;
      }
    }
  }

  return "";
}

function extractAssistantPreferredTime(text = "") {
  const normalized = normalizeAssistantSearchText(text);
  const segments = [normalized];
  const contextPatterns = [
    /(?:best time(?: to call)?|call me|llamame|me pueden llamar|mejor hora(?: para llamar)?|prefiero)\s+([^.\n]+)/i,
  ];

  for (const pattern of contextPatterns) {
    const match = normalized.match(pattern);

    if (match?.[1]) {
      segments.unshift(cleanText(match[1], 90));
    }
  }

  const timePatterns = [
    /\b((?:after|before|despues|antes)\s+(?:the\s+)?(?:las\s+)?\d{1,2}(?::\d{2})?\s*(?:am|pm)?)\b/i,
    /\b((?:around|about|como|tipo)\s+\d{1,2}(?::\d{2})?\s*(?:am|pm)?)\b/i,
    /\b(\d{1,2}(?::\d{2})?\s*(?:am|pm))\b/i,
    /\b(morning|afternoon|evening|night|noon|temprano|mediodia|manana|tarde|noche)\b/i,
  ];

  for (const segment of segments) {
    for (const pattern of timePatterns) {
      const match = String(segment || "").match(pattern);

      if (match?.[1]) {
        return cleanText(match[1], 50);
      }
    }
  }

  return "";
}

function getAssistantZonedParts(date = new Date(), timeZone = METALWORKS_CALLBACK_TIME_ZONE) {
  const formatter = new Intl.DateTimeFormat("en-US", {
    timeZone,
    year: "numeric",
    month: "numeric",
    day: "numeric",
    hour: "numeric",
    minute: "numeric",
    hour12: false,
  });
  const parts = formatter.formatToParts(date);
  const map = {};

  parts.forEach((part) => {
    if (part.type !== "literal") {
      map[part.type] = part.value;
    }
  });

  return {
    year: Number(map.year || 0),
    month: Number(map.month || 0),
    day: Number(map.day || 0),
    hour: Number(map.hour || 0),
    minute: Number(map.minute || 0),
  };
}

function addAssistantCalendarDays(base = null, dayOffset = 0) {
  if (!base?.year || !base?.month || !base?.day) {
    return null;
  }

  const reference = new Date(Date.UTC(base.year, base.month - 1, base.day));
  reference.setUTCDate(reference.getUTCDate() + Number(dayOffset || 0));

  return {
    year: reference.getUTCFullYear(),
    month: reference.getUTCMonth() + 1,
    day: reference.getUTCDate(),
  };
}

function buildAssistantZonedDate(
  { year, month, day, hour = 0, minute = 0 } = {},
  timeZone = METALWORKS_CALLBACK_TIME_ZONE,
) {
  if (!year || !month || !day) {
    return null;
  }

  const initialUtc = Date.UTC(year, month - 1, day, hour, minute, 0, 0);
  const initialDate = new Date(initialUtc);
  const zonedParts = getAssistantZonedParts(initialDate, timeZone);
  const zonedUtc = Date.UTC(
    zonedParts.year || year,
    (zonedParts.month || month) - 1,
    zonedParts.day || day,
    zonedParts.hour || hour,
    zonedParts.minute || minute,
    0,
    0,
  );

  return new Date(initialUtc - (zonedUtc - initialUtc));
}

function resolveAssistantTimeParts(label = "") {
  const normalized = normalizeAssistantSearchText(label || "");

  if (!normalized) {
    return null;
  }

  if (normalized.includes("noon") || normalized.includes("mediodia")) {
    return { hour: 12, minute: 0 };
  }

  if (normalized.includes("morning") || normalized.includes("temprano") || normalized.includes("manana")) {
    return { hour: 10, minute: 0 };
  }

  if (normalized.includes("afternoon") || normalized.includes("tarde")) {
    return { hour: 15, minute: 0 };
  }

  if (normalized.includes("evening") || normalized.includes("night") || normalized.includes("noche")) {
    return { hour: 18, minute: 0 };
  }

  const match = normalized.match(/(\d{1,2})(?::(\d{2}))?\s*(am|pm)?/i);

  if (!match) {
    return null;
  }

  let hour = Number(match[1] || 0);
  const minute = Number(match[2] || 0);
  let meridiem = String(match[3] || "").toLowerCase();

  if (!meridiem) {
    if (/\b(pm|afternoon|evening|night|tarde|noche)\b/.test(normalized)) {
      meridiem = "pm";
    } else if (/\b(am|morning|temprano|manana)\b/.test(normalized)) {
      meridiem = "am";
    }
  }

  if (meridiem === "pm" && hour < 12) {
    hour += 12;
  } else if (meridiem === "am" && hour === 12) {
    hour = 0;
  }

  if (hour > 23 || minute > 59) {
    return null;
  }

  return { hour, minute };
}

function resolveAssistantDayParts(
  bestContactDay = "",
  timeParts = null,
  now = new Date(),
  timeZone = METALWORKS_CALLBACK_TIME_ZONE,
) {
  const normalized = normalizeAssistantSearchText(bestContactDay || "");

  if (!normalized) {
    return null;
  }

  const nowParts = getAssistantZonedParts(now, timeZone);
  const todayParts = {
    year: nowParts.year,
    month: nowParts.month,
    day: nowParts.day,
  };
  const todayIndex = new Date(Date.UTC(nowParts.year, nowParts.month - 1, nowParts.day)).getUTCDay();
  const nowMinutes = (nowParts.hour || 0) * 60 + (nowParts.minute || 0);
  const targetMinutes = ((timeParts?.hour || 0) * 60) + (timeParts?.minute || 0);
  const hasFutureTimeToday = targetMinutes > nowMinutes + 2;
  const weekdayMap = {
    sunday: 0,
    domingo: 0,
    monday: 1,
    lunes: 1,
    tuesday: 2,
    martes: 2,
    wednesday: 3,
    miercoles: 3,
    thursday: 4,
    jueves: 4,
    friday: 5,
    viernes: 5,
    saturday: 6,
    sabado: 6,
  };

  if (normalized.includes("today") || normalized.includes("hoy")) {
    return hasFutureTimeToday ? todayParts : addAssistantCalendarDays(todayParts, 1);
  }

  if (normalized.includes("tomorrow") || normalized.includes("manana")) {
    return addAssistantCalendarDays(todayParts, 1);
  }

  if (normalized.includes("weekday") || normalized.includes("weekdays") || normalized.includes("entre semana")) {
    if (todayIndex >= 1 && todayIndex <= 5 && hasFutureTimeToday) {
      return todayParts;
    }

    for (let offset = 1; offset < 8; offset += 1) {
      const candidateIndex = (todayIndex + offset) % 7;

      if (candidateIndex >= 1 && candidateIndex <= 5) {
        return addAssistantCalendarDays(todayParts, offset);
      }
    }
  }

  if (normalized.includes("weekend") || normalized.includes("fin de semana")) {
    if ((todayIndex === 6 || todayIndex === 0) && hasFutureTimeToday) {
      return todayParts;
    }

    const saturdayOffset = (6 - todayIndex + 7) % 7;
    return addAssistantCalendarDays(todayParts, saturdayOffset === 0 ? 7 : saturdayOffset);
  }

  for (const [label, dayIndex] of Object.entries(weekdayMap)) {
    if (!normalized.includes(label)) {
      continue;
    }

    let offset = (dayIndex - todayIndex + 7) % 7;

    if (offset === 0 && !hasFutureTimeToday) {
      offset = 7;
    }

    return addAssistantCalendarDays(todayParts, offset);
  }

  return null;
}

function buildAssistantNextActionAt(
  bestContactDay = "",
  bestContactTime = "",
  now = new Date(),
  timeZone = METALWORKS_CALLBACK_TIME_ZONE,
) {
  const timeParts = resolveAssistantTimeParts(bestContactTime);
  const dayParts = resolveAssistantDayParts(bestContactDay, timeParts, now, timeZone);

  if (!timeParts || !dayParts) {
    return null;
  }

  return buildAssistantZonedDate(
    {
      year: dayParts.year,
      month: dayParts.month,
      day: dayParts.day,
      hour: timeParts.hour,
      minute: timeParts.minute,
    },
    timeZone,
  );
}

function formatAssistantCallbackLabel({
  nextActionAt = null,
  bestContactDay = "",
  bestContactTime = "",
  timeZone = METALWORKS_CALLBACK_TIME_ZONE,
} = {}) {
  if (nextActionAt instanceof Date && !Number.isNaN(nextActionAt.getTime())) {
    return formatDateTimeLabel(nextActionAt, timeZone);
  }

  const parts = [cleanText(bestContactDay || "", 40), cleanText(bestContactTime || "", 40)].filter(Boolean);
  return parts.join(" ").trim();
}

function buildAssistantConversationItems({
  history = [],
  message = "",
  reply = "",
} = {}) {
  const items = normalizeAssistantHistory(history);
  const safeMessage = cleanText(message || "", 500);
  const safeReply = cleanText(reply || "", 1500);
  const lastItem = items[items.length - 1];

  if (safeMessage && !(lastItem?.role === "user" && lastItem?.content === safeMessage)) {
    items.push({
      role: "user",
      content: safeMessage,
    });
  }

  const lastAfterUser = items[items.length - 1];

  if (safeReply && !(lastAfterUser?.role === "assistant" && lastAfterUser?.content === safeReply)) {
    items.push({
      role: "assistant",
      content: safeReply,
    });
  }

  return items.slice(-METALWORKS_ASSISTANT_HISTORY_LIMIT);
}

function buildAssistantConversationSignals({
  history = [],
  lead = null,
  pagePath = "",
} = {}) {
  const items = normalizeAssistantHistory(history);
  const userMessages = items.filter((item) => item.role === "user").map((item) => item.content);
  const combinedUserText = userMessages.join("\n");
  const latestUserMessage = userMessages[userMessages.length - 1] || "";
  let name = sanitizeAssistantStoredName(lead?.fullName || "");
  let email = normalizeEmail(lead?.email || "");
  let phone = normalizePhone(lead?.phone || "");
  let phoneDisplay = cleanText(lead?.phoneDisplay || "", 40);
  let projectType = cleanText(lead?.projectType || "", 120);
  let location = cleanText(lead?.location || "", 160);
  let bestContactDay = cleanText(lead?.bestContactDay || "", 80);
  let bestContactTime = cleanText(lead?.bestContactTime || "", 80);
  let callbackIntent = cleanText(lead?.callbackIntent || "", 12);
  let previousAssistantMessage = "";

  items.forEach((entry) => {
    if (entry.role === "assistant") {
      previousAssistantMessage = entry.content || "";
      return;
    }

    const entryText = entry.content || "";
    const contactInfo = extractAssistantContactInfo(entryText);
    const entryName = extractAssistantName(entryText);
    const entryProjectType = inferAssistantProjectTypeFromText(entryText);
    const entryLocation = extractAssistantLocation(entryText);
    const entryBestDay = extractAssistantPreferredDay(entryText);
    const entryBestTime = extractAssistantPreferredTime(entryText);
    const normalizedPreviousAssistant = normalizeAssistantSearchText(previousAssistantMessage);
    const cleanedEntry = cleanText(entryText, 80).replace(/[.,;!?]+$/, "");
    const askedForName =
      /\b(name|nombre)\b/.test(normalizedPreviousAssistant) &&
      !/\b(company|empresa|service|job|quote|estimate)\b/.test(normalizedPreviousAssistant);

    if (entryName) {
      name = entryName;
    } else if (!name && askedForName && assistantNameLooksReliable(cleanedEntry)) {
      name = cleanedEntry;
    }

    if (contactInfo.email) {
      email = contactInfo.email;
    }

    if (contactInfo.phone) {
      phone = contactInfo.phone;
      phoneDisplay = contactInfo.phoneDisplay || phoneDisplay || contactInfo.phone;
    }

    if (entryProjectType) {
      projectType = entryProjectType;
    }

    if (entryLocation) {
      location = entryLocation;
    }

    if (entryBestDay) {
      bestContactDay = entryBestDay;
    }

    if (entryBestTime) {
      bestContactTime = entryBestTime;
    }

    if (detectAssistantCallbackIntent(entryText)) {
      callbackIntent = "yes";
    }

    if (detectAssistantCallbackDecline(entryText)) {
      callbackIntent = "no";
    }
  });

  if (!projectType && /metal-porch-repair|porch/i.test(pagePath || "")) {
    projectType = "Metal porch repair / restoration";
  }

  const nextActionAt = buildAssistantNextActionAt(bestContactDay, bestContactTime);
  const callbackLabel = formatAssistantCallbackLabel({
    nextActionAt,
    bestContactDay,
    bestContactTime,
  });
  const callbackMissingFields =
    callbackIntent === "yes"
      ? [
          !name ? "name" : "",
          !phone && !email ? "phone or email" : "",
          !bestContactDay ? "best day to call" : "",
          !bestContactTime ? "best time to call" : "",
        ].filter(Boolean)
      : [];
  const serviceSummarySource =
    [...userMessages]
      .reverse()
      .find((entry) => inferAssistantProjectTypeFromText(entry)) ||
    [...userMessages]
      .reverse()
      .find((entry) =>
        /\b(gate|fence|railing|handrail|stairs|stair|balcony|porch|weld|welding|rust|repair|replace|install|fabricat|metal)\b/i.test(
          normalizeAssistantSearchText(entry),
        ),
      ) ||
    latestUserMessage;
  const detailsSummary = [
    projectType ? `Service: ${projectType}.` : "",
    location ? `Location: ${location}.` : "",
    serviceSummarySource ? `Request: ${cleanText(serviceSummarySource, 320)}.` : "",
    callbackIntent === "yes" ? "Visitor asked for a callback." : "",
    callbackLabel ? `Best callback window: ${callbackLabel}.` : "",
  ]
    .filter(Boolean)
    .join(" ")
    .trim()
    .slice(0, 1500);
  const inSpanish = detectSpanish(combinedUserText || latestUserMessage);

  return {
    items,
    userMessages,
    combinedUserText,
    latestUserMessage,
    inSpanish,
    name,
    email,
    phone,
    phoneDisplay,
    projectType,
    location,
    bestContactDay,
    bestContactTime,
    callbackIntent: callbackIntent === "no" ? "no" : callbackIntent === "yes" ? "yes" : "",
    callbackMissingFields,
    nextActionAt,
    callbackLabel,
    detailsSummary,
    shouldCreateLead: Boolean(lead?._id || phone || email || callbackIntent === "yes"),
    shouldAlert: (callbackIntent === "yes" || lead?.callbackIntent === "yes") && Boolean(phone || email),
    conversationDigest: buildAssistantHistoryDigest(items),
  };
}

function buildAssistantStatePrompt(state = {}) {
  const callbackIntent = state?.callbackIntent === "yes" ? "yes" : state?.callbackIntent === "no" ? "no" : "unknown";
  const missingFields = Array.isArray(state?.callbackMissingFields) ? state.callbackMissingFields.join(", ") : "";
  const callbackLabel = cleanText(state?.callbackLabel || "", 120) || "pending";

  return `
CALL CAPTURE STATE:
- callback_intent: ${callbackIntent}
- visitor_name: ${state?.name || "pending"}
- phone: ${state?.phoneDisplay || state?.phone || "pending"}
- email: ${state?.email || "pending"}
- project_type: ${state?.projectType || "pending"}
- location: ${state?.location || "pending"}
- best_day_to_call: ${state?.bestContactDay || "pending"}
- best_time_to_call: ${state?.bestContactTime || "pending"}
- callback_window: ${callbackLabel}
- missing_callback_fields: ${missingFields || "none"}

INSTRUCTIONS:
- If callback_intent is yes and there are missing callback fields, ask only for the missing callback fields in one short message.
- If callback_intent is yes and contact details are already present, confirm the callback request and ask for photos or ZIP code only if still useful.
- Do not say the callback is booked unless the visitor actually gave a specific day and time.
- Keep replies practical, short, and contractor-like.
`;
}

function stripAssistantNotesBlock(value = "") {
  const source = String(value || "");
  const markerIndex = source.indexOf(METALWORKS_ASSISTANT_NOTES_MARKER);

  if (markerIndex === -1) {
    return source.trim();
  }

  return source.slice(0, markerIndex).trim();
}

function buildAssistantPrivateNotes(state = {}) {
  return [
    "Source: Agustin 2.0 website assistant.",
    state?.projectType ? `Project type: ${state.projectType}.` : "",
    state?.location ? `Location: ${state.location}.` : "",
    state?.callbackIntent === "yes" ? "Callback requested: yes." : "",
    state?.callbackIntent === "no" ? "Callback requested: no." : "",
    state?.callbackLabel ? `Best callback window: ${state.callbackLabel}.` : "",
    state?.detailsSummary ? `Summary: ${state.detailsSummary}` : "",
  ]
    .filter(Boolean)
    .join("\n")
    .slice(0, 2600);
}

function mergeAssistantPrivateNotes(existingNotes = "", state = {}) {
  const manualNotes = stripAssistantNotesBlock(existingNotes);
  const generatedNotes = buildAssistantPrivateNotes(state);

  if (!generatedNotes) {
    return manualNotes;
  }

  return [manualNotes, `${METALWORKS_ASSISTANT_NOTES_MARKER}\n${generatedNotes}`]
    .filter(Boolean)
    .join("\n\n")
    .trim()
    .slice(0, 4000);
}

function extractAssistantResponseText(data = null) {
  const directText = cleanText(data?.output_text || "", 1500);

  if (directText) {
    return directText;
  }

  const outputItems = Array.isArray(data?.output) ? data.output : [];

  for (const item of outputItems) {
    if (item?.type !== "message") {
      continue;
    }

    const parts = Array.isArray(item?.content) ? item.content : [];

    for (const part of parts) {
      const text = cleanText(part?.text || "", 1500);

      if (part?.type === "output_text" && text) {
        return text;
      }
    }
  }

  return "";
}

async function generateAssistantReply({
  message = "",
  history = [],
  pagePath = "",
  conversationState = null,
} = {}) {
  const fallbackReply = buildAssistantFallbackReply(message, conversationState);
  const apiKey = String(process.env.OPENAI_API_KEY || "").trim();

  if (!apiKey) {
    console.warn("[metalworks-assistant] OPENAI_API_KEY missing or empty");
    return {
      reply: fallbackReply,
      usedFallback: true,
      reason: "OPENAI_API_KEY missing",
    };
  }

  try {
    const response = await fetch("https://api.openai.com/v1/responses", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Authorization: `Bearer ${apiKey}`,
      },
      body: JSON.stringify({
        model: "gpt-5-mini",
        max_output_tokens: 420,
        reasoning: {
          effort: "low",
        },
        text: {
          verbosity: "low",
          format: {
            type: "text",
          },
        },
        input: [
          {
            role: "system",
            content: METALWORKS_ASSISTANT_SYSTEM_PROMPT,
          },
          {
            role: "system",
            content: buildAssistantContext(message, pagePath),
          },
          {
            role: "system",
            content: buildAssistantStatePrompt(conversationState || {}),
          },
          ...normalizeAssistantHistory(history),
          {
            role: "user",
            content: cleanText(message, 500),
          },
        ],
      }),
    });

    const data = await response.json().catch(() => null);
    const reply = extractAssistantResponseText(data);

    if (!response.ok || !reply) {
      const errorMessage =
        cleanText(
          data?.error?.message || data?.message || `OpenAI error ${response.status}`,
          240,
        ) || "OpenAI error";
      console.warn("[metalworks-assistant] OpenAI fallback", {
        status: response.status,
        reason: errorMessage,
      });
      return {
        reply: fallbackReply,
        usedFallback: true,
        reason: errorMessage,
      };
    }

    return {
      reply,
      usedFallback: false,
      reason: "",
    };
  } catch (error) {
    console.warn("[metalworks-assistant] Assistant request failed", {
      reason: cleanText(error?.message || "Assistant error", 240),
    });
    return {
      reply: fallbackReply,
      usedFallback: true,
      reason: cleanText(error?.message || "Assistant error", 240),
    };
  }
}

function cleanLead(doc = null, { includeConversation = false } = {}) {
  if (!doc) {
    return null;
  }

  return {
    id: String(doc._id || ""),
    fullName: doc.fullName || "",
    phone: doc.phone || "",
    phoneDisplay: doc.phoneDisplay || "",
    email: doc.email || "",
    projectType: doc.projectType || "",
    location: doc.location || "",
    details: doc.details || "",
    photoFileNames: Array.isArray(doc.photoFileNames) ? doc.photoFileNames : [],
    status: normalizeStatus(doc.status || "new"),
    statusLabel: labelStatus(doc.status || "new"),
    nextAction: doc.nextAction || "",
    nextActionAt: doc.nextActionAt ? new Date(doc.nextActionAt).toISOString() : "",
    bestContactDay: doc.bestContactDay || "",
    bestContactTime: doc.bestContactTime || "",
    callbackIntent: doc.callbackIntent || "",
    callbackRequestedAt: doc.callbackRequestedAt
      ? new Date(doc.callbackRequestedAt).toISOString()
      : "",
    callbackAlertedAt: doc.callbackAlertedAt ? new Date(doc.callbackAlertedAt).toISOString() : "",
    privateNotes: doc.privateNotes || "",
    lastUserMessage: doc.lastUserMessage || "",
    lastAssistantMessage: doc.lastAssistantMessage || "",
    conversationHistory: includeConversation
      ? (Array.isArray(doc.conversationHistory) ? doc.conversationHistory : [])
          .map((entry) => ({
            role: entry?.role === "assistant" ? "assistant" : "user",
            content: cleanText(entry?.content || "", 1500),
            createdAt: entry?.createdAt ? new Date(entry.createdAt).toISOString() : "",
          }))
          .filter((entry) => entry.content)
      : [],
    estimateTitle: doc.estimateTitle || "",
    estimateScope: doc.estimateScope || "",
    estimateMaterialsCost: normalizeMoney(doc.estimateMaterialsCost || 0),
    estimateLaborCost: normalizeMoney(doc.estimateLaborCost || 0),
    estimateCoatingCost: normalizeMoney(doc.estimateCoatingCost || 0),
    estimateMiscCost: normalizeMoney(doc.estimateMiscCost || 0),
    estimateDiscount: normalizeMoney(doc.estimateDiscount || 0),
    estimateAmount: Number(doc.estimateAmount || 0) || 0,
    estimateValidUntil: doc.estimateValidUntil ? new Date(doc.estimateValidUntil).toISOString() : "",
    estimateNotes: doc.estimateNotes || "",
    estimateSentAt: doc.estimateSentAt ? new Date(doc.estimateSentAt).toISOString() : "",
    estimateSentTo: doc.estimateSentTo || "",
    pageTitle: doc.pageTitle || "",
    pagePath: doc.pagePath || "",
    pageUrl: doc.pageUrl || "",
    referrer: doc.referrer || "",
    sourceType: doc.sourceType || "website_form",
    tracking: doc.tracking || {},
    createdAt: doc.createdAt ? new Date(doc.createdAt).toISOString() : "",
    updatedAt: doc.updatedAt ? new Date(doc.updatedAt).toISOString() : "",
    lastContactAt: doc.lastContactAt ? new Date(doc.lastContactAt).toISOString() : "",
  };
}

function cleanActivity(doc = null) {
  if (!doc) {
    return null;
  }

  return {
    id: String(doc._id || ""),
    leadId: doc.leadId ? String(doc.leadId) : "",
    activityType: doc.activityType || "",
    title: doc.title || formatActivityTitle(doc.activityType || ""),
    body: doc.body || "",
    pagePath: doc.pagePath || "",
    pageUrl: doc.pageUrl || "",
    meta: doc.meta || null,
    createdAt: doc.createdAt ? new Date(doc.createdAt).toISOString() : "",
  };
}

function buildLeadQuery(filters = {}) {
  const query = {};
  const status = normalizeStatus(filters?.status || "");
  const search = cleanText(filters?.search || "", 120);
  const projectType = cleanText(filters?.projectType || "", 80);

  if (filters?.status && METALWORKS_CRM_STATUS_OPTIONS.includes(status)) {
    query.status = status;
  }

  if (projectType) {
    query.projectType = projectType;
  }

  if (search) {
    const pattern = new RegExp(escapeRegex(search), "i");
    query.$or = [
      { fullName: pattern },
      { phoneDisplay: pattern },
      { phone: pattern },
      { email: pattern },
      { location: pattern },
      { details: pattern },
      { projectType: pattern },
      { estimateTitle: pattern },
      { estimateScope: pattern },
    ];
  }

  return query;
}

async function buildDashboardSnapshot(MetalworksLead, MetalworksLeadActivity, filters = {}) {
  const query = buildLeadQuery(filters);
  const thirtyDaysAgo = new Date(Date.now() - 30 * 24 * 60 * 60 * 1000);

  const [
    leads,
    recentActivity,
    totalLeads,
    newLeads,
    contactedLeads,
    quotedLeads,
    bookedLeads,
    wonLeads,
    lostLeads,
    archivedLeads,
    phoneClicks30d,
    emailClicks30d,
    quoteSubmits30d,
    serviceBreakdown,
  ] = await Promise.all([
    MetalworksLead.find(query).sort({ updatedAt: -1, createdAt: -1 }).limit(250).lean(),
    MetalworksLeadActivity.find({
      activityType: {
        $nin: ["assistant_user_message", "assistant_ai_reply", "assistant_fallback"],
      },
    })
      .sort({ createdAt: -1 })
      .limit(40)
      .lean(),
    MetalworksLead.countDocuments({}),
    MetalworksLead.countDocuments({ status: "new" }),
    MetalworksLead.countDocuments({ status: "contacted" }),
    MetalworksLead.countDocuments({ status: "quoted" }),
    MetalworksLead.countDocuments({ status: "booked" }),
    MetalworksLead.countDocuments({ status: "won" }),
    MetalworksLead.countDocuments({ status: "lost" }),
    MetalworksLead.countDocuments({ status: "archived" }),
    MetalworksLeadActivity.countDocuments({
      activityType: "phone_click",
      createdAt: { $gte: thirtyDaysAgo },
    }),
    MetalworksLeadActivity.countDocuments({
      activityType: "email_click",
      createdAt: { $gte: thirtyDaysAgo },
    }),
    MetalworksLeadActivity.countDocuments({
      activityType: { $in: ["quote_submit", "quote_submit_fallback"] },
      createdAt: { $gte: thirtyDaysAgo },
    }),
    MetalworksLead.aggregate([
      { $match: {} },
      {
        $group: {
          _id: "$projectType",
          count: { $sum: 1 },
        },
      },
      { $sort: { count: -1, _id: 1 } },
      { $limit: 8 },
    ]),
  ]);

  return {
    summary: {
      totalLeads,
      newLeads,
      contactedLeads,
      quotedLeads,
      bookedLeads,
      wonLeads,
      lostLeads,
      archivedLeads,
      phoneClicks30d,
      emailClicks30d,
      quoteSubmits30d,
    },
    filters: {
      status: query.status || "",
      search: cleanText(filters?.search || "", 120),
      projectType: cleanText(filters?.projectType || "", 80),
    },
    serviceBreakdown: serviceBreakdown
      .map((item) => ({
        label: item?._id || "Sin categoria",
        count: Number(item?.count || 0) || 0,
      }))
      .filter((item) => item.count > 0),
    leads: leads.map(cleanLead).filter(Boolean),
    recentActivity: recentActivity.map(cleanActivity).filter(Boolean),
    statusOptions: METALWORKS_CRM_STATUS_OPTIONS.map((status) => ({
      value: status,
      label: labelStatus(status),
    })),
  };
}

export function registerMetalworksCrm(app, { mongoose, publicDir, privateDir }) {
  const trackingSchema = new mongoose.Schema(
    {
      gclid: String,
      gbraid: String,
      wbraid: String,
      utmSource: String,
      utmMedium: String,
      utmCampaign: String,
      utmTerm: String,
      utmContent: String,
      landingPath: String,
      landingUrl: String,
      referrer: String,
    },
    { _id: false },
  );
  const conversationEntrySchema = new mongoose.Schema(
    {
      role: String,
      content: String,
      createdAt: { type: Date, default: Date.now },
    },
    { _id: false },
  );

  const metalworksLeadSchema = new mongoose.Schema({
    fullName: { type: String, required: true, trim: true, index: true },
    phone: { type: String, index: true },
    phoneDisplay: String,
    email: { type: String, index: true },
    projectType: String,
    location: String,
    details: String,
    photoFileNames: [String],
    status: { type: String, default: "new", index: true },
    nextAction: String,
    nextActionAt: Date,
    privateNotes: String,
    estimateTitle: String,
    estimateScope: String,
    estimateMaterialsCost: { type: Number, default: 0 },
    estimateLaborCost: { type: Number, default: 0 },
    estimateCoatingCost: { type: Number, default: 0 },
    estimateMiscCost: { type: Number, default: 0 },
    estimateDiscount: { type: Number, default: 0 },
    estimateAmount: { type: Number, default: 0 },
    estimateValidUntil: Date,
    estimateNotes: String,
    estimateSentAt: Date,
    estimateSentTo: String,
    sourceType: { type: String, default: "website_form", index: true },
    pageTitle: String,
    pagePath: String,
    pageUrl: String,
    referrer: String,
    ipAddress: String,
    userAgent: String,
    tracking: trackingSchema,
    visitorIds: [String],
    sessionIds: [String],
    conversationHistory: [conversationEntrySchema],
    lastUserMessage: String,
    lastAssistantMessage: String,
    bestContactDay: String,
    bestContactTime: String,
    callbackIntent: String,
    callbackRequestedAt: Date,
    callbackAlertedAt: Date,
    lastContactAt: Date,
    updatedAt: Date,
    createdAt: { type: Date, default: Date.now },
  });

  const metalworksLeadActivitySchema = new mongoose.Schema({
    leadId: {
      type: mongoose.Schema.Types.ObjectId,
      ref: "MetalworksLead",
      default: null,
      index: true,
    },
    activityType: { type: String, default: "lead_updated", index: true },
    title: String,
    body: String,
    meta: { type: mongoose.Schema.Types.Mixed, default: null },
    pagePath: String,
    pageUrl: String,
    ipAddress: String,
    userAgent: String,
    tracking: trackingSchema,
    createdAt: { type: Date, default: Date.now, index: true },
  });

  const metalworksCrmSessionSchema = new mongoose.Schema({
    adminEmail: { type: String, required: true, index: true },
    tokenHash: { type: String, required: true, unique: true, index: true },
    ipAddress: String,
    userAgent: String,
    expiresAt: { type: Date, required: true },
    lastSeenAt: { type: Date, default: Date.now },
    createdAt: { type: Date, default: Date.now },
  });

  metalworksLeadSchema.index({ createdAt: -1 });
  metalworksLeadSchema.index({ updatedAt: -1 });
  metalworksLeadSchema.index({ status: 1, updatedAt: -1 });
  metalworksLeadSchema.index({ visitorIds: 1 });
  metalworksLeadSchema.index({ sessionIds: 1 });
  metalworksLeadActivitySchema.index({ leadId: 1, createdAt: -1 });
  metalworksLeadActivitySchema.index({ activityType: 1, createdAt: -1 });
  metalworksCrmSessionSchema.index({ expiresAt: 1 }, { expireAfterSeconds: 0 });

  const MetalworksLead =
    mongoose.models.MetalworksLead ||
    mongoose.model("MetalworksLead", metalworksLeadSchema);
  const MetalworksLeadActivity =
    mongoose.models.MetalworksLeadActivity ||
    mongoose.model("MetalworksLeadActivity", metalworksLeadActivitySchema);
  const MetalworksCrmSession =
    mongoose.models.MetalworksCrmSession ||
    mongoose.model("MetalworksCrmSession", metalworksCrmSessionSchema);

  function setSessionCookie(res, req, token) {
    res.cookie(METALWORKS_CRM_SESSION_COOKIE, token, {
      httpOnly: true,
      sameSite: "lax",
      secure: requestIsSecure(req),
      path: "/",
      maxAge: METALWORKS_CRM_SESSION_DAYS * 24 * 60 * 60 * 1000,
    });
  }

  function clearSessionCookie(res, req) {
    res.clearCookie(METALWORKS_CRM_SESSION_COOKIE, {
      httpOnly: true,
      sameSite: "lax",
      secure: requestIsSecure(req),
      path: "/",
    });
  }

  function respondError(res, statusCode, message) {
    return res.status(statusCode).json({ error: message });
  }

  async function getAuth(req, { touch = true } = {}) {
    const cookies = parseCookies(req.headers.cookie || "");
    const token = String(cookies[METALWORKS_CRM_SESSION_COOKIE] || "").trim();

    if (!token) {
      return { session: null, email: "" };
    }

    const session = await MetalworksCrmSession.findOne({
      tokenHash: hashToken(token),
      expiresAt: { $gt: new Date() },
    }).lean();

    if (!session?.adminEmail) {
      return { session: null, email: "" };
    }

    if (touch) {
      await MetalworksCrmSession.updateOne(
        { _id: session._id },
        {
          $set: {
            lastSeenAt: new Date(),
          },
        },
      );
    }

    return {
      session,
      email: session.adminEmail,
    };
  }

  async function requireAuth(req, res) {
    if (!metalworksCrmConfigured()) {
      respondError(
        res,
        503,
        "El CRM de Metal Works todavia no tiene password configurado.",
      );
      return null;
    }

    const auth = await getAuth(req);

    if (!auth.email) {
      respondError(res, 401, "Necesitas iniciar sesion.");
      return null;
    }

    return auth;
  }

  async function createSession(req, res, email) {
    const token = generateToken();
    const expiresAt = new Date(
      Date.now() + METALWORKS_CRM_SESSION_DAYS * 24 * 60 * 60 * 1000,
    );

    await MetalworksCrmSession.create({
      adminEmail: email,
      tokenHash: hashToken(token),
      ipAddress: getClientIp(req),
      userAgent: cleanText(req.headers["user-agent"] || "", 400),
      expiresAt,
      lastSeenAt: new Date(),
    });

    setSessionCookie(res, req, token);
  }

  async function destroySession(req, res) {
    const cookies = parseCookies(req.headers.cookie || "");
    const token = String(cookies[METALWORKS_CRM_SESSION_COOKIE] || "").trim();

    if (token) {
      await MetalworksCrmSession.deleteOne({
        tokenHash: hashToken(token),
      });
    }

    clearSessionCookie(res, req);
  }

  async function appendActivity({
    leadId = null,
    activityType = "",
    title = "",
    body = "",
    meta = null,
    pagePath = "",
    pageUrl = "",
    req = null,
    tracking = {},
  } = {}) {
    await MetalworksLeadActivity.create({
      leadId,
      activityType,
      title: title || formatActivityTitle(activityType),
      body: cleanText(body || "", 1200),
      meta,
      pagePath: cleanText(pagePath || "", 240),
      pageUrl: cleanText(pageUrl || "", 500),
      ipAddress: req ? cleanText(getClientIp(req), 120) : "",
      userAgent: req ? cleanText(req.headers["user-agent"] || "", 400) : "",
      tracking: buildTrackingPayload(tracking),
    });
  }

  async function resolveConversationLead({ visitorId = "", sessionId = "", email = "", phone = "" } = {}) {
    const conditions = [];

    if (phone) {
      conditions.push({ phone });
    }

    if (email) {
      conditions.push({ email });
    }

    if (visitorId) {
      conditions.push({ visitorIds: visitorId });
    }

    if (sessionId) {
      conditions.push({ sessionIds: sessionId });
    }

    if (!conditions.length) {
      return null;
    }

    return MetalworksLead.findOne({ $or: conditions }).sort({ updatedAt: -1, createdAt: -1 });
  }

  function resolveAssistantLeadStatus(currentLead = null, state = {}) {
    const currentStatus = normalizeStatus(currentLead?.status || "new");

    if (["won", "lost", "archived"].includes(currentStatus)) {
      return currentStatus;
    }

    if (state?.callbackIntent === "yes" && state?.nextActionAt) {
      return "booked";
    }

    if (state?.callbackIntent === "yes" || state?.phone || state?.email) {
      return "contacted";
    }

    return currentStatus || "new";
  }

  function normalizeStoredConversationHistory(history = []) {
    return (Array.isArray(history) ? history : [])
      .map((entry) => ({
        role: entry?.role === "assistant" ? "assistant" : "user",
        content: cleanText(entry?.content || "", 500),
        createdAt: entry?.createdAt ? new Date(entry.createdAt) : new Date(),
      }))
      .filter((entry) => entry.content)
      .slice(-METALWORKS_ASSISTANT_HISTORY_LIMIT);
  }

  function mergeConversationHistory(existingHistory = [], nextHistory = []) {
    const merged = [
      ...normalizeStoredConversationHistory(existingHistory),
      ...normalizeStoredConversationHistory(nextHistory),
    ];
    const deduped = [];

    merged.forEach((entry) => {
      const lastEntry = deduped[deduped.length - 1];

      if (lastEntry?.role === entry.role && lastEntry?.content === entry.content) {
        return;
      }

      deduped.push(entry);
    });

    return deduped.slice(-METALWORKS_ASSISTANT_HISTORY_LIMIT);
  }

  async function upsertConversationLead({
    currentLead = null,
    state = {},
    pageTitle = "",
    pagePath = "",
    pageUrl = "",
    referrer = "",
    tracking = {},
    req = null,
    assistantReply = "",
  } = {}) {
    if (!state?.shouldCreateLead && !currentLead?._id) {
      return null;
    }

    const now = new Date();
    const leadDoc = currentLead || new MetalworksLead();
    const existingConversationHistory = currentLead?.conversationHistory || [];
    const mergedHistory = mergeConversationHistory(existingConversationHistory, state.items || []);
    const effectiveName =
      selectAssistantText(
        sanitizeAssistantStoredName(state?.name || ""),
        sanitizeAssistantStoredName(currentLead?.fullName || ""),
      ) || METALWORKS_ASSISTANT_PLACEHOLDER_NAME;
    const effectivePhone = normalizePhone(state?.phone || currentLead?.phone || "");
    const effectivePhoneDisplay =
      cleanText(state?.phoneDisplay || currentLead?.phoneDisplay || "", 40) || effectivePhone;
    const effectiveEmail = normalizeEmail(state?.email || currentLead?.email || "");
    const effectiveProjectType = selectAssistantText(
      state?.projectType || "",
      currentLead?.projectType || "",
    );
    const effectiveLocation = selectAssistantLongestText(
      state?.location || "",
      currentLead?.location || "",
    );
    const effectiveDetails = selectAssistantLongestText(
      state?.detailsSummary || "",
      currentLead?.details || "",
    );

    leadDoc.fullName = effectiveName;
    leadDoc.phone = effectivePhone;
    leadDoc.phoneDisplay = effectivePhoneDisplay;
    leadDoc.email = effectiveEmail;
    leadDoc.projectType = effectiveProjectType;
    leadDoc.location = effectiveLocation;
    leadDoc.details = effectiveDetails;
    leadDoc.status = resolveAssistantLeadStatus(currentLead, state);
    leadDoc.nextAction =
      state?.callbackIntent === "yes"
        ? "call back from assistant chat"
        : currentLead?.nextAction || "";
    leadDoc.nextActionAt = state?.nextActionAt || currentLead?.nextActionAt || null;
    leadDoc.privateNotes = mergeAssistantPrivateNotes(currentLead?.privateNotes || "", state);
    leadDoc.sourceType =
      cleanText(currentLead?.sourceType || "", 80) || "assistant_chat";
    leadDoc.pageTitle = cleanText(pageTitle || currentLead?.pageTitle || "", 160);
    leadDoc.pagePath = cleanText(pagePath || currentLead?.pagePath || "", 240);
    leadDoc.pageUrl = cleanText(pageUrl || currentLead?.pageUrl || "", 500);
    leadDoc.referrer = cleanText(referrer || currentLead?.referrer || "", 500);
    leadDoc.ipAddress = req ? cleanText(getClientIp(req), 120) : cleanText(currentLead?.ipAddress || "", 120);
    leadDoc.userAgent = req
      ? cleanText(req.headers["user-agent"] || "", 400)
      : cleanText(currentLead?.userAgent || "", 400);
    leadDoc.tracking = buildTrackingPayload(tracking || currentLead?.tracking || {});
    leadDoc.visitorIds = mergeAssistantUniqueValues(currentLead?.visitorIds || [], state?.visitorId || "");
    leadDoc.sessionIds = mergeAssistantUniqueValues(currentLead?.sessionIds || [], state?.sessionId || "");
    leadDoc.conversationHistory = mergedHistory;
    leadDoc.lastUserMessage = cleanText(state?.latestUserMessage || currentLead?.lastUserMessage || "", 500);
    leadDoc.lastAssistantMessage = cleanText(
      assistantReply || currentLead?.lastAssistantMessage || "",
      1500,
    );
    leadDoc.bestContactDay = cleanText(state?.bestContactDay || currentLead?.bestContactDay || "", 80);
    leadDoc.bestContactTime = cleanText(state?.bestContactTime || currentLead?.bestContactTime || "", 80);
    leadDoc.callbackIntent =
      state?.callbackIntent === "yes" || state?.callbackIntent === "no"
        ? state.callbackIntent
        : cleanText(currentLead?.callbackIntent || "", 12);

    if (state?.callbackIntent === "yes" && !leadDoc.callbackRequestedAt) {
      leadDoc.callbackRequestedAt = now;
    }

    if (state?.callbackIntent === "yes" || state?.phone || state?.email) {
      leadDoc.lastContactAt = now;
    }

    leadDoc.updatedAt = now;

    if (!leadDoc.createdAt) {
      leadDoc.createdAt = now;
    }

    await leadDoc.save();
    return leadDoc;
  }

  app.get(
    ["/metalworks-crm/login", "/metalworks-crm/login/"],
    async (req, res) => {
      const auth = await getAuth(req, { touch: false });

      if (auth.email) {
        return res.redirect("/metalworks-crm/");
      }

      res.sendFile(path.join(publicDir, "metalworks-crm", "login.html"));
    },
  );

  app.get(["/metalworks-crm", "/metalworks-crm/"], async (req, res) => {
    const auth = await getAuth(req, { touch: false });

    if (!auth.email) {
      return res.redirect("/metalworks-crm/login/");
    }

    res.sendFile(path.join(privateDir, "metalworks-crm.html"));
  });

  app.get("/api/metalworks-crm/me", async (req, res) => {
    try {
      const auth = await getAuth(req, { touch: true });
      const fallbackEmail =
        auth.email || getAllowedEmails()[0] || METALWORKS_CRM_DEFAULT_EMAIL;
      res.json({
        authenticated: Boolean(auth.email),
        configured: metalworksCrmConfigured(),
        email: auth.email || "",
        allowedEmail: fallbackEmail,
        profile: getMetalworksCrmProfile(fallbackEmail),
      });
    } catch (error) {
      console.error("Error loading Metal Works auth:", error.message);
      respondError(res, 500, "No pude revisar la sesion del CRM.");
    }
  });

  app.post("/api/metalworks-crm/login", async (req, res) => {
    const email = normalizeEmail(req.body?.email || "");
    const password = String(req.body?.password || "");
    const allowedEmails = getAllowedEmails();
    const expectedPassword = getMetalworksPassword();

    if (!metalworksCrmConfigured()) {
      return respondError(
        res,
        503,
        "Primero configura METALWORKS_CRM_PASSWORD en el backend.",
      );
    }

    if (!email || !password) {
      return respondError(res, 400, "Correo y password son requeridos.");
    }

    if (!allowedEmails.includes(email)) {
      return respondError(res, 403, "Ese correo no tiene acceso al CRM.");
    }

    if (!compareSecrets(password, expectedPassword)) {
      return respondError(res, 401, "Correo o password incorrectos.");
    }

    try {
      await createSession(req, res, email);
      res.json({ ok: true, email, profile: getMetalworksCrmProfile(email) });
    } catch (error) {
      console.error("Error logging into Metal Works CRM:", error.message);
      respondError(res, 500, "No pude iniciar sesion en el CRM.");
    }
  });

  app.post("/api/metalworks-crm/logout", async (req, res) => {
    try {
      await destroySession(req, res);
      res.json({ ok: true });
    } catch (error) {
      console.error("Error logging out of Metal Works CRM:", error.message);
      respondError(res, 500, "No pude cerrar la sesion.");
    }
  });

  app.get("/api/metalworks-crm/dashboard", async (req, res) => {
    const auth = await requireAuth(req, res);

    if (!auth) {
      return;
    }

    try {
      const snapshot = await buildDashboardSnapshot(
        MetalworksLead,
        MetalworksLeadActivity,
        {
          status: req.query?.status || "",
          search: req.query?.search || "",
          projectType: req.query?.projectType || "",
        },
      );

      res.json(snapshot);
    } catch (error) {
      console.error("Error loading Metal Works dashboard:", error.message);
      respondError(res, 500, "No pude cargar el dashboard del CRM.");
    }
  });

  app.get("/api/metalworks-crm/leads/:leadId", async (req, res) => {
    const auth = await requireAuth(req, res);

    if (!auth) {
      return;
    }

    const leadId = String(req.params?.leadId || "").trim();

    if (!leadId || !mongoose.Types.ObjectId.isValid(leadId)) {
      return respondError(res, 400, "Lead invalido.");
    }

    try {
      const [leadDoc, activityDocs] = await Promise.all([
        MetalworksLead.findById(leadId).lean(),
        MetalworksLeadActivity.find({ leadId })
          .sort({ createdAt: -1 })
          .limit(80)
          .lean(),
      ]);

      if (!leadDoc) {
        return respondError(res, 404, "No encontre ese lead.");
      }

      res.json({
        lead: cleanLead(leadDoc, { includeConversation: true }),
        activity: activityDocs.map(cleanActivity).filter(Boolean),
      });
    } catch (error) {
      console.error("Error loading Metal Works lead detail:", error.message);
      respondError(res, 500, "No pude cargar ese lead.");
    }
  });

  app.patch("/api/metalworks-crm/leads/:leadId", async (req, res) => {
    const auth = await requireAuth(req, res);

    if (!auth) {
      return;
    }

    const leadId = String(req.params?.leadId || "").trim();

    if (!leadId || !mongoose.Types.ObjectId.isValid(leadId)) {
      return respondError(res, 400, "Lead invalido.");
    }

    try {
      const leadDoc = await MetalworksLead.findById(leadId);

      if (!leadDoc) {
        return respondError(res, 404, "No encontre ese lead.");
      }

      const changes = [];
      const fullName = Object.prototype.hasOwnProperty.call(req.body || {}, "fullName")
        ? cleanText(req.body?.fullName || "", 120)
        : null;
      const phoneDisplayRaw = Object.prototype.hasOwnProperty.call(req.body || {}, "phoneDisplay")
        ? cleanText(req.body?.phoneDisplay || "", 40)
        : null;
      const phone = phoneDisplayRaw !== null ? normalizePhone(phoneDisplayRaw) : null;
      const email = Object.prototype.hasOwnProperty.call(req.body || {}, "email")
        ? normalizeEmail(req.body?.email || "")
        : null;
      const projectType = Object.prototype.hasOwnProperty.call(req.body || {}, "projectType")
        ? cleanText(req.body?.projectType || "", 120)
        : null;
      const location = Object.prototype.hasOwnProperty.call(req.body || {}, "location")
        ? cleanText(req.body?.location || "", 160)
        : null;
      const details = Object.prototype.hasOwnProperty.call(req.body || {}, "details")
        ? cleanText(req.body?.details || "", 3000)
        : null;
      const bestContactDay = Object.prototype.hasOwnProperty.call(req.body || {}, "bestContactDay")
        ? cleanText(req.body?.bestContactDay || "", 80)
        : null;
      const bestContactTime = Object.prototype.hasOwnProperty.call(req.body || {}, "bestContactTime")
        ? cleanText(req.body?.bestContactTime || "", 80)
        : null;
      const nextStatus = Object.prototype.hasOwnProperty.call(req.body || {}, "status")
        ? normalizeStatus(req.body?.status || "new")
        : null;
      const nextAction = Object.prototype.hasOwnProperty.call(req.body || {}, "nextAction")
        ? cleanText(req.body?.nextAction || "", 160)
        : null;
      const nextActionAtRaw = Object.prototype.hasOwnProperty.call(req.body || {}, "nextActionAt")
        ? String(req.body?.nextActionAt || "").trim()
        : null;
      const nextActionAt = nextActionAtRaw
        ? new Date(nextActionAtRaw)
        : nextActionAtRaw === ""
          ? null
          : undefined;
      const privateNotes = Object.prototype.hasOwnProperty.call(req.body || {}, "privateNotes")
        ? cleanText(req.body?.privateNotes || "", 4000)
        : null;
      const estimateAmount = Object.prototype.hasOwnProperty.call(req.body || {}, "estimateAmount")
        ? normalizeMoney(req.body?.estimateAmount || 0)
        : null;
      const estimateTitle = Object.prototype.hasOwnProperty.call(req.body || {}, "estimateTitle")
        ? cleanText(req.body?.estimateTitle || "", 160)
        : null;
      const estimateScope = Object.prototype.hasOwnProperty.call(req.body || {}, "estimateScope")
        ? cleanText(req.body?.estimateScope || "", 2400)
        : null;
      const estimateMaterialsCost = Object.prototype.hasOwnProperty.call(req.body || {}, "estimateMaterialsCost")
        ? normalizeMoney(req.body?.estimateMaterialsCost || 0)
        : null;
      const estimateLaborCost = Object.prototype.hasOwnProperty.call(req.body || {}, "estimateLaborCost")
        ? normalizeMoney(req.body?.estimateLaborCost || 0)
        : null;
      const estimateCoatingCost = Object.prototype.hasOwnProperty.call(req.body || {}, "estimateCoatingCost")
        ? normalizeMoney(req.body?.estimateCoatingCost || 0)
        : null;
      const estimateMiscCost = Object.prototype.hasOwnProperty.call(req.body || {}, "estimateMiscCost")
        ? normalizeMoney(req.body?.estimateMiscCost || 0)
        : null;
      const estimateDiscount = Object.prototype.hasOwnProperty.call(req.body || {}, "estimateDiscount")
        ? normalizeMoney(req.body?.estimateDiscount || 0)
        : null;
      const estimateValidUntilRaw = Object.prototype.hasOwnProperty.call(req.body || {}, "estimateValidUntil")
        ? String(req.body?.estimateValidUntil || "").trim()
        : null;
      const estimateValidUntil = estimateValidUntilRaw === null
        ? null
        : estimateValidUntilRaw
          ? parseDateOnly(estimateValidUntilRaw)
          : null;
      const estimateNotes = Object.prototype.hasOwnProperty.call(req.body || {}, "estimateNotes")
        ? cleanText(req.body?.estimateNotes || "", 2400)
        : null;
      const note = cleanText(req.body?.note || "", 600);
      let estimateChanged = false;
      let estimateMoneyChanged = false;
      let profileChanged = false;

      if (fullName !== null) {
        const nextFullName =
          fullName || sanitizeAssistantStoredName(leadDoc.fullName || "") || METALWORKS_ASSISTANT_PLACEHOLDER_NAME;

        if (leadDoc.fullName !== nextFullName) {
          leadDoc.fullName = nextFullName;
          profileChanged = true;
        }
      }

      if (phoneDisplayRaw !== null) {
        const nextPhoneDisplay = phoneDisplayRaw || "";

        if (leadDoc.phone !== phone || leadDoc.phoneDisplay !== nextPhoneDisplay) {
          leadDoc.phone = phone || "";
          leadDoc.phoneDisplay = nextPhoneDisplay;
          profileChanged = true;
        }
      }

      if (email !== null && leadDoc.email !== email) {
        leadDoc.email = email;
        profileChanged = true;
      }

      if (projectType !== null && leadDoc.projectType !== projectType) {
        leadDoc.projectType = projectType;
        profileChanged = true;
      }

      if (location !== null && leadDoc.location !== location) {
        leadDoc.location = location;
        profileChanged = true;
      }

      if (details !== null && leadDoc.details !== details) {
        leadDoc.details = details;
        profileChanged = true;
      }

      if (bestContactDay !== null && leadDoc.bestContactDay !== bestContactDay) {
        leadDoc.bestContactDay = bestContactDay;
        profileChanged = true;
      }

      if (bestContactTime !== null && leadDoc.bestContactTime !== bestContactTime) {
        leadDoc.bestContactTime = bestContactTime;
        profileChanged = true;
      }

      if (nextStatus && leadDoc.status !== nextStatus) {
        changes.push(`Estado: ${labelStatus(leadDoc.status)} -> ${labelStatus(nextStatus)}`);
        leadDoc.status = nextStatus;
      }

      if (nextAction !== null && leadDoc.nextAction !== nextAction) {
        changes.push(`Proxima accion: ${nextAction || "Sin accion"}`);
        leadDoc.nextAction = nextAction;
      }

      if (nextActionAt !== undefined && String(leadDoc.nextActionAt || "") !== String(nextActionAt || "")) {
        changes.push(
          `Seguimiento: ${
            nextActionAt instanceof Date && !Number.isNaN(nextActionAt.getTime())
              ? nextActionAt.toLocaleString("en-US")
              : "Sin fecha"
          }`,
        );
        leadDoc.nextActionAt =
          nextActionAt instanceof Date && !Number.isNaN(nextActionAt.getTime())
            ? nextActionAt
            : null;
      }

      if (privateNotes !== null) {
        leadDoc.privateNotes = privateNotes;
      }

      if (estimateTitle !== null) {
        if (leadDoc.estimateTitle !== estimateTitle) {
          estimateChanged = true;
        }
        leadDoc.estimateTitle = estimateTitle;
      }

      if (estimateScope !== null) {
        if (leadDoc.estimateScope !== estimateScope) {
          estimateChanged = true;
        }
        leadDoc.estimateScope = estimateScope;
      }

      if (estimateMaterialsCost !== null) {
        if (normalizeMoney(leadDoc.estimateMaterialsCost || 0) !== estimateMaterialsCost) {
          estimateChanged = true;
          estimateMoneyChanged = true;
        }
        leadDoc.estimateMaterialsCost = estimateMaterialsCost;
      }

      if (estimateLaborCost !== null) {
        if (normalizeMoney(leadDoc.estimateLaborCost || 0) !== estimateLaborCost) {
          estimateChanged = true;
          estimateMoneyChanged = true;
        }
        leadDoc.estimateLaborCost = estimateLaborCost;
      }

      if (estimateCoatingCost !== null) {
        if (normalizeMoney(leadDoc.estimateCoatingCost || 0) !== estimateCoatingCost) {
          estimateChanged = true;
          estimateMoneyChanged = true;
        }
        leadDoc.estimateCoatingCost = estimateCoatingCost;
      }

      if (estimateMiscCost !== null) {
        if (normalizeMoney(leadDoc.estimateMiscCost || 0) !== estimateMiscCost) {
          estimateChanged = true;
          estimateMoneyChanged = true;
        }
        leadDoc.estimateMiscCost = estimateMiscCost;
      }

      if (estimateDiscount !== null) {
        if (normalizeMoney(leadDoc.estimateDiscount || 0) !== estimateDiscount) {
          estimateChanged = true;
          estimateMoneyChanged = true;
        }
        leadDoc.estimateDiscount = estimateDiscount;
      }

      if (estimateValidUntilRaw !== null) {
        if (String(leadDoc.estimateValidUntil || "") !== String(estimateValidUntil || "")) {
          estimateChanged = true;
        }
        leadDoc.estimateValidUntil = estimateValidUntil;
      }

      if (estimateNotes !== null) {
        if (leadDoc.estimateNotes !== estimateNotes) {
          estimateChanged = true;
        }
        leadDoc.estimateNotes = estimateNotes;
      }

      if (estimateMoneyChanged || estimateAmount !== null) {
        const nextEstimateAmount = estimateMoneyChanged
          ? normalizeMoney(
              (estimateMaterialsCost !== null ? estimateMaterialsCost : leadDoc.estimateMaterialsCost || 0) +
                (estimateLaborCost !== null ? estimateLaborCost : leadDoc.estimateLaborCost || 0) +
                (estimateCoatingCost !== null ? estimateCoatingCost : leadDoc.estimateCoatingCost || 0) +
                (estimateMiscCost !== null ? estimateMiscCost : leadDoc.estimateMiscCost || 0) -
                (estimateDiscount !== null ? estimateDiscount : leadDoc.estimateDiscount || 0),
            )
          : normalizeMoney(estimateAmount || 0);

        if (normalizeMoney(leadDoc.estimateAmount || 0) !== nextEstimateAmount) {
          estimateChanged = true;
        }

        leadDoc.estimateAmount = nextEstimateAmount;
      }

      if (estimateChanged) {
        changes.push(`Estimate: ${formatMoneyLabel(leadDoc.estimateAmount || 0)}`);
      }

      if (profileChanged) {
        changes.push("Perfil del cliente actualizado");
      }

      if (changes.length || note) {
        leadDoc.lastContactAt = new Date();
      }

      leadDoc.updatedAt = new Date();
      await leadDoc.save();

      if (changes.length) {
        await appendActivity({
          leadId: leadDoc._id,
          activityType: "lead_updated",
          title: "Lead actualizado",
          body: changes.join(". "),
          meta: {
            adminEmail: auth.email,
          },
          req,
        });
      }

      if (note) {
        await appendActivity({
          leadId: leadDoc._id,
          activityType: "note_added",
          title: "Nota privada",
          body: note,
          meta: {
            adminEmail: auth.email,
          },
          req,
        });
      }

      const [updatedLead, activityDocs] = await Promise.all([
        MetalworksLead.findById(leadId).lean(),
        MetalworksLeadActivity.find({ leadId })
          .sort({ createdAt: -1 })
          .limit(80)
          .lean(),
      ]);

      res.json({
        lead: cleanLead(updatedLead, { includeConversation: true }),
        activity: activityDocs.map(cleanActivity).filter(Boolean),
      });
    } catch (error) {
      console.error("Error updating Metal Works lead:", error.message);
      respondError(res, 500, "No pude guardar ese lead.");
    }
  });

  app.post("/api/metalworks-crm/leads/:leadId/send-estimate", async (req, res) => {
    const auth = await requireAuth(req, res);

    if (!auth) {
      return;
    }

    const leadId = String(req.params?.leadId || "").trim();

    if (!leadId || !mongoose.Types.ObjectId.isValid(leadId)) {
      return respondError(res, 400, "Lead invalido.");
    }

    try {
      const leadDoc = await MetalworksLead.findById(leadId);

      if (!leadDoc) {
        return respondError(res, 404, "No encontre ese lead.");
      }

      if (!normalizeEmail(leadDoc.email || "")) {
        return respondError(res, 400, "Este lead no tiene correo todavia.");
      }

      if (
        !cleanText(leadDoc.estimateTitle || "", 160) &&
        !cleanText(leadDoc.estimateScope || "", 2400) &&
        !normalizeMoney(leadDoc.estimateAmount || 0)
      ) {
        return respondError(res, 400, "Primero guarda un estimate para poder enviarlo.");
      }

      const delivery = await sendMetalworksEstimateEmail(leadDoc, auth.email);
      let activityDocs = [];

      if (delivery.delivered) {
        const sentAt = new Date();
        const statusBefore = normalizeStatus(leadDoc.status || "new");
        let statusLine = "";

        leadDoc.estimateSentAt = sentAt;
        leadDoc.estimateSentTo = normalizeEmail(leadDoc.email || "");
        leadDoc.lastContactAt = sentAt;
        leadDoc.updatedAt = sentAt;

        if (["new", "contacted"].includes(statusBefore)) {
          leadDoc.status = "quoted";
          statusLine = " Status changed to Quoted.";
        }

        await leadDoc.save();

        await appendActivity({
          leadId: leadDoc._id,
          activityType: "estimate_sent",
          title: "Estimate sent",
          body: `Estimate sent to ${leadDoc.estimateSentTo}.${statusLine}`.trim(),
          meta: {
            adminEmail: auth.email,
            sentTo: leadDoc.estimateSentTo,
          },
          req,
        });
      }

      const updatedLead = await MetalworksLead.findById(leadId).lean();
      activityDocs = await MetalworksLeadActivity.find({ leadId })
        .sort({ createdAt: -1 })
        .limit(80)
        .lean();

      res.json({
        ok: true,
        delivered: Boolean(delivery.delivered),
        fallbackUsed: !delivery.delivered,
        message: delivery.delivered
          ? "Estimate sent to the client."
          : delivery.error || "I could not send it from the system.",
        lead: cleanLead(updatedLead),
        activity: activityDocs.map(cleanActivity).filter(Boolean),
      });
    } catch (error) {
      console.error("Error sending Metal Works estimate:", error.message);
      respondError(res, 500, "No pude enviar ese estimate.");
    }
  });

  app.post("/api/public/metalworks/leads", async (req, res) => {
    try {
      const honeypot = cleanText(req.body?.company || "", 120);

      if (honeypot) {
        return res.json({ ok: true, ignored: true });
      }

      const fullName = cleanText(req.body?.name || "", 120);
      const phone = normalizePhone(req.body?.phone || "");
      const phoneDisplay = cleanText(req.body?.phone || "", 40);
      const email = normalizeEmail(req.body?.email || "");
      const projectType = cleanText(req.body?.projectType || "", 120);
      const location = cleanText(req.body?.location || "", 160);
      const details = cleanText(req.body?.details || "", 3000);
      const pageTitle = cleanText(req.body?.pageTitle || "", 160);
      const pagePath = cleanText(req.body?.pagePath || "", 240);
      const pageUrl = cleanText(req.body?.pageUrl || "", 500);
      const referrer = cleanText(req.body?.referrer || "", 500);
      const photoFileNames = Array.isArray(req.body?.selectedPhotos)
        ? req.body.selectedPhotos.map((item) => cleanText(item, 120)).filter(Boolean).slice(0, 20)
        : [];
      const tracking = buildTrackingPayload(req.body?.tracking || {});

      if (!fullName) {
        return respondError(res, 400, "El nombre es requerido.");
      }

      if (!phone) {
        return respondError(res, 400, "El telefono es requerido.");
      }

      if (!email) {
        return respondError(res, 400, "El correo es requerido.");
      }

      if (!details) {
        return respondError(res, 400, "Los detalles del proyecto son requeridos.");
      }

      const fourteenDaysAgo = new Date(Date.now() - 14 * 24 * 60 * 60 * 1000);
      const duplicateQuery = {
        createdAt: { $gte: fourteenDaysAgo },
        details,
        $or: [{ phone }, { email }],
      };

      let leadDoc = await MetalworksLead.findOne(duplicateQuery).sort({
        createdAt: -1,
      });
      const now = new Date();
      const duplicate = Boolean(leadDoc);

      if (leadDoc) {
        leadDoc.fullName = fullName;
        leadDoc.phone = phone;
        leadDoc.phoneDisplay = phoneDisplay;
        leadDoc.email = email;
        leadDoc.projectType = projectType;
        leadDoc.location = location;
        leadDoc.photoFileNames = photoFileNames;
        leadDoc.pageTitle = pageTitle;
        leadDoc.pagePath = pagePath;
        leadDoc.pageUrl = pageUrl;
        leadDoc.referrer = referrer;
        leadDoc.ipAddress = cleanText(getClientIp(req), 120);
        leadDoc.userAgent = cleanText(req.headers["user-agent"] || "", 400);
        leadDoc.tracking = tracking;
        leadDoc.updatedAt = now;
        await leadDoc.save();
      } else {
        leadDoc = await MetalworksLead.create({
          fullName,
          phone,
          phoneDisplay,
          email,
          projectType,
          location,
          details,
          photoFileNames,
          status: "new",
          sourceType: "website_form",
          pageTitle,
          pagePath,
          pageUrl,
          referrer,
          ipAddress: cleanText(getClientIp(req), 120),
          userAgent: cleanText(req.headers["user-agent"] || "", 400),
          tracking,
          updatedAt: now,
          createdAt: now,
        });

        await appendActivity({
          leadId: leadDoc._id,
          activityType: "lead_created",
          title: "Lead creado",
          body: `${fullName} envio un nuevo request de quote.`,
          meta: {
            projectType,
            location,
          },
          req,
          pagePath,
          pageUrl,
          tracking,
        });
      }

      await appendActivity({
        leadId: leadDoc._id,
        activityType: "quote_submit",
        title: duplicate ? "Quote repetido" : "Quote enviado",
        body: duplicate
          ? "El cliente volvio a enviar el formulario."
          : "El formulario del sitio se guardo en el CRM.",
        meta: {
          duplicate,
          projectType,
          location,
          photoFileCount: photoFileNames.length,
        },
        req,
        pagePath,
        pageUrl,
        tracking,
      });

      res.json({
        ok: true,
        duplicate,
        lead: cleanLead(leadDoc.toObject ? leadDoc.toObject() : leadDoc),
      });
    } catch (error) {
      console.error("Error saving public Metal Works lead:", error.message);
      respondError(res, 500, "No pude guardar tu quote en este momento.");
    }
  });

  app.post("/api/public/metalworks/appointments", async (req, res) => {
    try {
      const fullName = cleanText(req.body?.name || "", 120);
      const phone = normalizePhone(req.body?.phone || "");
      const phoneDisplay = cleanText(req.body?.phone || "", 40);
      const email = normalizeEmail(req.body?.email || "");
      const projectType = cleanText(req.body?.projectType || "", 120);
      const location = cleanText(req.body?.location || "", 160);
      const details = cleanText(req.body?.details || "", 3000);
      const preferredDateTimeRaw = cleanText(req.body?.preferredDateTime || "", 80);
      const preferredDateTime = preferredDateTimeRaw ? new Date(preferredDateTimeRaw) : null;
      const timeZone = cleanText(req.body?.timezone || "", 80);
      const visitorId = cleanText(req.body?.visitorId || "", 120);
      const sessionId = cleanText(req.body?.sessionId || "", 120);
      const pageTitle = cleanText(req.body?.pageTitle || "", 160);
      const pagePath = cleanText(req.body?.pagePath || "", 240);
      const pageUrl = cleanText(req.body?.pageUrl || "", 500);
      const referrer = cleanText(req.body?.referrer || "", 500);
      const tracking = buildTrackingPayload(req.body?.tracking || {});
      const conversationDigest = buildAssistantHistoryDigest(req.body?.conversationHistory || []);

      if (!fullName) {
        return respondError(res, 400, "El nombre es requerido.");
      }

      if (!phone) {
        return respondError(res, 400, "El telefono es requerido.");
      }

      if (!details) {
        return respondError(res, 400, "Los detalles del proyecto son requeridos.");
      }

      if (!(preferredDateTime instanceof Date) || Number.isNaN(preferredDateTime.getTime())) {
        return respondError(res, 400, "Selecciona una fecha valida para la cita.");
      }

      const thirtyDaysAgo = new Date(Date.now() - 30 * 24 * 60 * 60 * 1000);
      const duplicateMatch = [{ phone }];

      if (email) {
        duplicateMatch.push({ email });
      }

      let leadDoc = await MetalworksLead.findOne({
        createdAt: { $gte: thirtyDaysAgo },
        $or: duplicateMatch,
      }).sort({
        createdAt: -1,
      });
      const now = new Date();
      const duplicate = Boolean(leadDoc);
      const requestedAtLabel = formatDateTimeLabel(preferredDateTime, timeZone);
      const callbackNote = [
        `Callback requested for ${requestedAtLabel}${timeZone ? ` (${timeZone})` : ""}.`,
        conversationDigest ? `Recent assistant conversation:\n${conversationDigest}` : "",
      ]
        .filter(Boolean)
        .join("\n\n")
        .slice(0, 3200);

      if (leadDoc) {
        leadDoc.fullName = fullName || leadDoc.fullName;
        leadDoc.phone = phone;
        leadDoc.phoneDisplay = phoneDisplay;
        leadDoc.email = email || leadDoc.email || "";
        leadDoc.projectType = projectType || leadDoc.projectType || "";
        leadDoc.location = location || leadDoc.location || "";
        leadDoc.details = details || leadDoc.details || "";
        leadDoc.status = "booked";
        leadDoc.nextAction = "callback_requested";
        leadDoc.nextActionAt = preferredDateTime;
        leadDoc.sourceType = leadDoc.sourceType || "assistant_booking";
        leadDoc.pageTitle = pageTitle || leadDoc.pageTitle || "";
        leadDoc.pagePath = pagePath || leadDoc.pagePath || "";
        leadDoc.pageUrl = pageUrl || leadDoc.pageUrl || "";
        leadDoc.referrer = referrer || leadDoc.referrer || "";
        leadDoc.ipAddress = cleanText(getClientIp(req), 120);
        leadDoc.userAgent = cleanText(req.headers["user-agent"] || "", 400);
        leadDoc.tracking = tracking;
        leadDoc.lastContactAt = now;
        leadDoc.privateNotes = [String(leadDoc.privateNotes || "").trim(), callbackNote]
          .filter(Boolean)
          .join("\n\n")
          .slice(0, 4000);
        leadDoc.updatedAt = now;
        await leadDoc.save();
      } else {
        leadDoc = await MetalworksLead.create({
          fullName,
          phone,
          phoneDisplay,
          email,
          projectType,
          location,
          details,
          status: "booked",
          nextAction: "callback_requested",
          nextActionAt: preferredDateTime,
          privateNotes: callbackNote,
          sourceType: "assistant_booking",
          pageTitle,
          pagePath,
          pageUrl,
          referrer,
          ipAddress: cleanText(getClientIp(req), 120),
          userAgent: cleanText(req.headers["user-agent"] || "", 400),
          tracking,
          lastContactAt: now,
          updatedAt: now,
          createdAt: now,
        });

        await appendActivity({
          leadId: leadDoc._id,
          activityType: "lead_created",
          title: "Lead creado",
          body: `${fullName} pidio una cita desde el assistant.`,
          meta: {
            projectType,
            location,
            requestedAt: preferredDateTime.toISOString(),
          },
          req,
          pagePath,
          pageUrl,
          tracking,
        });
      }

      await appendActivity({
        leadId: leadDoc._id,
        activityType: "assistant_booking_requested",
        title: duplicate ? "Cita del assistant actualizada" : "Cita del assistant guardada",
        body: duplicate
          ? `El visitante actualizo la hora pedida para ${requestedAtLabel}.`
          : `El assistant guardo una cita pedida para ${requestedAtLabel}.`,
        meta: {
          duplicate,
          requestedAt: preferredDateTime.toISOString(),
          timeZone,
          visitorId,
          sessionId,
          pageTitle,
        },
        req,
        pagePath,
        pageUrl,
        tracking,
      });

      let alertDelivery = {
        attempted: false,
        delivered: false,
      };

      try {
        alertDelivery = await sendMetalworksLeadAlertEmail({
          lead: leadDoc.toObject ? leadDoc.toObject() : leadDoc,
          alertType: "assistant_callback",
          requestedAt: preferredDateTime,
          requestedAtLabel,
          timeZone,
          pagePath,
          pageUrl,
          conversationDigest,
        });
      } catch (error) {
        console.error("Error sending Metal Works callback alert:", error.message);
      }

      res.json({
        ok: true,
        duplicate,
        notified: Boolean(alertDelivery.delivered),
        requestedAtLabel,
        lead: cleanLead(leadDoc.toObject ? leadDoc.toObject() : leadDoc),
      });
    } catch (error) {
      console.error("Error saving Metal Works assistant appointment:", error.message);
      respondError(res, 500, "No pude guardar la cita en este momento.");
    }
  });

  app.post("/api/public/metalworks/events", async (req, res) => {
    try {
      const activityType = cleanText(req.body?.type || "", 80).toLowerCase();

      if (!METALWORKS_CRM_PUBLIC_EVENT_TYPES.has(activityType)) {
        return respondError(res, 400, "Tipo de evento invalido.");
      }

      const pagePath = cleanText(req.body?.pagePath || "", 240);
      const pageUrl = cleanText(req.body?.pageUrl || "", 500);
      const tracking = buildTrackingPayload(req.body?.tracking || {});

      await appendActivity({
        activityType,
        title: formatActivityTitle(activityType),
        body: cleanText(req.body?.label || "", 240),
        meta: {
          href: cleanText(req.body?.href || "", 500),
          pageTitle: cleanText(req.body?.pageTitle || "", 160),
        },
        req,
        pagePath,
        pageUrl,
        tracking,
      });

      res.json({ ok: true });
    } catch (error) {
      console.error("Error saving Metal Works public event:", error.message);
      respondError(res, 500, "No pude guardar ese evento.");
    }
  });

  app.post("/api/public/metalworks/assistant", async (req, res) => {
    try {
      const message = cleanText(req.body?.message || "", 500);
      const visitorId = cleanText(req.body?.visitorId || "", 120);
      const sessionId = cleanText(req.body?.sessionId || "", 120);
      const pageTitle = cleanText(req.body?.pageTitle || "", 160);
      const pagePath = cleanText(req.body?.pagePath || "", 240);
      const pageUrl = cleanText(req.body?.pageUrl || "", 500);
      const referrer = cleanText(req.body?.referrer || "", 500);
      const tracking = buildTrackingPayload(req.body?.tracking || {});
      const history = normalizeAssistantHistory(req.body?.history || []);

      if (!message) {
        return respondError(res, 400, "Message is required.");
      }

      const startOfDay = new Date();
      startOfDay.setHours(0, 0, 0, 0);

      const usedToday = visitorId
        ? await MetalworksLeadActivity.countDocuments({
            activityType: "assistant_user_message",
            createdAt: { $gte: startOfDay },
            "meta.visitorId": visitorId,
          })
        : 0;

      if (visitorId && usedToday >= METALWORKS_ASSISTANT_MAX_MESSAGES_PER_DAY) {
        return res.status(429).json({
          error: `You reached the daily limit of ${METALWORKS_ASSISTANT_MAX_MESSAGES_PER_DAY} assistant messages for today. Please call 773 798 4107 or try again tomorrow.`,
        });
      }

      const userConversationItems = buildAssistantConversationItems({
        history,
        message,
      });
      let conversationState = buildAssistantConversationSignals({
        history: userConversationItems,
        pagePath,
      });
      let currentLead = await resolveConversationLead({
        visitorId,
        sessionId,
        email: conversationState.email,
        phone: conversationState.phone,
      });

      if (currentLead) {
        conversationState = buildAssistantConversationSignals({
          history: userConversationItems,
          lead: currentLead,
          pagePath,
        });
      }

      conversationState = {
        ...conversationState,
        visitorId,
        sessionId,
      };

      const leadExistedBeforeMessage = Boolean(currentLead?._id);
      const previousLeadStatus = normalizeStatus(currentLead?.status || "new");
      const previousLeadNextActionAt = currentLead?.nextActionAt
        ? new Date(currentLead.nextActionAt).toISOString()
        : "";
      let leadDoc = await upsertConversationLead({
        currentLead,
        state: conversationState,
        pageTitle,
        pagePath,
        pageUrl,
        referrer,
        tracking,
        req,
      });

      if (!leadExistedBeforeMessage && leadDoc?._id) {
        await appendActivity({
          leadId: leadDoc._id,
          activityType: "lead_created",
          title: "Lead creado",
          body:
            conversationState.callbackIntent === "yes"
              ? "Agustin 2.0 creo un lead conversacional para seguimiento de llamada."
              : "Agustin 2.0 creo un lead conversacional desde el chat del sitio.",
          meta: {
            sourceType: "assistant_chat",
            projectType: leadDoc.projectType || "",
            location: leadDoc.location || "",
            visitorId,
            sessionId,
            pageTitle,
          },
          req,
          pagePath,
          pageUrl,
          tracking,
        });
      }

      await appendActivity({
        leadId: leadDoc?._id || currentLead?._id || null,
        activityType: "assistant_user_message",
        title: "Mensaje al assistant",
        body: message,
        meta: {
          visitorId,
          sessionId,
          pageTitle,
        },
        req,
        pagePath,
        pageUrl,
        tracking,
      });

      const result = await generateAssistantReply({
        message,
        history: userConversationItems,
        pagePath,
        conversationState,
      });

      const conversationItemsWithReply = buildAssistantConversationItems({
        history: userConversationItems,
        reply: result.reply,
      });
      const finalState = {
        ...buildAssistantConversationSignals({
          history: conversationItemsWithReply,
          lead: leadDoc || currentLead,
          pagePath,
        }),
        visitorId,
        sessionId,
      };

      leadDoc = await upsertConversationLead({
        currentLead: leadDoc || currentLead,
        state: finalState,
        pageTitle,
        pagePath,
        pageUrl,
        referrer,
        tracking,
        req,
        assistantReply: result.reply,
      });

      const currentLeadNextActionAt = leadDoc?.nextActionAt
        ? new Date(leadDoc.nextActionAt).toISOString()
        : "";

      if (
        leadDoc?._id &&
        finalState.callbackIntent === "yes" &&
        finalState.nextActionAt &&
        (previousLeadStatus !== "booked" || previousLeadNextActionAt !== currentLeadNextActionAt)
      ) {
        await appendActivity({
          leadId: leadDoc._id,
          activityType: "assistant_booking_requested",
          title: leadExistedBeforeMessage
            ? "Cita del assistant actualizada"
            : "Cita del assistant guardada",
          body: finalState.callbackLabel
            ? `Agustin 2.0 detecto una llamada pedida para ${finalState.callbackLabel}.`
            : "Agustin 2.0 detecto una llamada pedida desde la conversacion.",
          meta: {
            duplicate: leadExistedBeforeMessage,
            requestedAt: finalState.nextActionAt.toISOString(),
            visitorId,
            sessionId,
            pageTitle,
          },
          req,
          pagePath,
          pageUrl,
          tracking,
        });
      }

      let alertDelivery = {
        attempted: false,
        delivered: false,
      };

      if (leadDoc?._id && finalState.shouldAlert && !leadDoc.callbackAlertedAt) {
        try {
          alertDelivery = await sendMetalworksLeadAlertEmail({
            lead: leadDoc.toObject ? leadDoc.toObject() : leadDoc,
            alertType:
              finalState.callbackIntent === "yes" ? "assistant_callback" : "assistant_lead",
            requestedAt: finalState.nextActionAt,
            requestedAtLabel: finalState.callbackLabel,
            timeZone: METALWORKS_CALLBACK_TIME_ZONE,
            pagePath,
            pageUrl,
            conversationDigest: finalState.conversationDigest,
          });

          if (alertDelivery.delivered) {
            leadDoc.callbackAlertedAt = new Date();
            leadDoc.updatedAt = new Date();
            await leadDoc.save();
          }
        } catch (error) {
          console.error("Error sending Metal Works assistant alert:", error.message);
        }
      }

      await appendActivity({
        leadId: leadDoc?._id || currentLead?._id || null,
        activityType: result.usedFallback ? "assistant_fallback" : "assistant_ai_reply",
        title: result.usedFallback ? "Fallback del assistant" : "Respuesta del assistant",
        body: result.reply,
        meta: {
          visitorId,
          sessionId,
          reason: result.reason || "",
        },
        req,
        pagePath,
        pageUrl,
        tracking,
      });

      res.json({
        ok: true,
        respuesta: result.reply,
        usedFallback: result.usedFallback,
        leadCaptured: Boolean(leadDoc?._id),
        leadId: leadDoc?._id ? String(leadDoc._id) : "",
        callbackCaptured: Boolean(
          leadDoc?._id && finalState.callbackIntent === "yes" && finalState.nextActionAt,
        ),
        callbackLabel: finalState.callbackLabel || "",
        notified: Boolean(alertDelivery.delivered),
        remainingToday: visitorId
          ? Math.max(METALWORKS_ASSISTANT_MAX_MESSAGES_PER_DAY - (usedToday + 1), 0)
          : METALWORKS_ASSISTANT_MAX_MESSAGES_PER_DAY,
      });
    } catch (error) {
      console.error("Error in Metal Works assistant:", error.message);
      respondError(res, 500, "I could not answer right now.");
    }
  });
}
