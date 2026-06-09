import crypto from "node:crypto";
import http2 from "node:http2";
import path from "node:path";

const METALWORKS_CRM_SESSION_COOKIE = "cmwf_crm_session";
const METALWORKS_CRM_SESSION_DAYS = 30;
const METALWORKS_PROSPECTOR_SESSION_COOKIE = "cmwf_prospector_session";
const METALWORKS_PROSPECTOR_SESSION_DAYS = 14;
const METALWORKS_CRM_DEFAULT_EMAIL = "agustincalderon286@gmail.com";
const METALWORKS_CONTACT_PHONE_DISPLAY = "773 798 4107";
const METALWORKS_CONTACT_EMAIL = "agustincalderon286@gmail.com";
const METALWORKS_WEBSITE_URL = "https://www.chicagometalworksandfencing.com/";
const METALWORKS_DEFAULT_CLIENT_WARRANTY =
  "Chicago Metal Works & Fencing stands behind the approved scope of work. Warranty coverage and any exclusions follow the written agreement for this job.";
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
const METALWORKS_ASSISTANT_VISION_MAX_IMAGES = Math.max(
  0,
  Number(process.env.METALWORKS_ASSISTANT_VISION_MAX_IMAGES || 2),
);
const METALWORKS_ASSISTANT_NOTES_MARKER = "[Agustin Assistant Notes]";
const METALWORKS_ASSISTANT_PLACEHOLDER_NAME = "Website chat lead";
const METALWORKS_LEAD_ASSET_MAX_FILES = 4;
const METALWORKS_LEAD_ASSET_MAX_BYTES = 2 * 1024 * 1024;
const METALWORKS_LEAD_ASSET_MAX_TOTAL_BYTES = 6 * 1024 * 1024;
const METALWORKS_WHATSAPP_WINDOW_MS = 24 * 60 * 60 * 1000;
const METALWORKS_WHATSAPP_FOLLOWUP_POLL_MS = Math.max(
  15 * 1000,
  Number(process.env.METALWORKS_WHATSAPP_FOLLOWUP_POLL_MS || 60 * 1000),
);
const METALWORKS_WHATSAPP_FOLLOWUP_LOCK_MS = 2 * 60 * 1000;
const METALWORKS_WHATSAPP_FOLLOWUP_BATCH_SIZE = Math.max(
  1,
  Number(process.env.METALWORKS_WHATSAPP_FOLLOWUP_BATCH_SIZE || 4),
);
const METALWORKS_WHATSAPP_FOLLOWUP_STEPS = Object.freeze([
  {
    step: "nudge_10m",
    delayMs: 10 * 60 * 1000,
    maxLagMs: 90 * 60 * 1000,
    label: "10-minute follow-up",
  },
  {
    step: "nudge_6h",
    delayMs: 6 * 60 * 60 * 1000,
    maxLagMs: 4 * 60 * 60 * 1000,
    label: "6-hour follow-up",
  },
  {
    step: "last_chance_23h",
    delayMs: 23 * 60 * 60 * 1000,
    maxLagMs: 45 * 60 * 1000,
    label: "final 24-hour follow-up",
  },
]);
const METALWORKS_WHATSAPP_CLOSED_LEAD_STATUSES = new Set([
  "booked",
  "won",
  "lost",
  "archived",
]);
const METALWORKS_EXTERNAL_SYNC_TOKEN = String(
  process.env.METALWORKS_EXTERNAL_SYNC_TOKEN || "",
).trim();
const METALWORKS_IOS_APP_BUNDLE_ID = "com.agustincalderon.agustin2";
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
const METALWORKS_PUSH_INVALID_REASONS = new Set([
  "BadDeviceToken",
  "DeviceTokenNotForTopic",
  "Unregistered",
]);
const METALWORKS_APNS_JWT_CACHE = {
  token: "",
  expiresAt: 0,
  cacheKey: "",
};
const METALWORKS_ASSISTANT_SYSTEM_PROMPT = `
You are Agustin 2.0 for Chicago Metal Works & Fencing.

ROLE:
- You help website visitors for a Chicago metalwork business.
- Main services: railings, handrails, gate repair, gate installation, fence repair, fence fabrication, mobile welding, custom metal fabrication, stairs, balconies, porch railings, ornamental ironwork.

GOAL:
- Help the visitor quickly understand the next best step.
- Increase conversions to a text-back estimate or quote follow-up.
- Qualify the job without sounding pushy.

VOICE:
- Default to English.
- If the visitor writes in Spanish, you may answer in Spanish.
- Sound practical, friendly, clear, and contractor-like.
- Do not sound like a generic corporate chatbot.
- Keep it short.

RULES:
- Most replies should be 2 or 3 short sentences.
- Ask one clear next-step question at a time unless you are collecting callback fields.
- Ask for the visitor's name and best phone number early, ideally before deeper qualification, so the team can text back if the chat disconnects.
- Ask for photos early when that will help.
- Ask whether it is repair, replacement, or new installation when useful.
- Ask for ZIP code or job location when useful.
- After name and phone are captured, move toward photos, ZIP code, and the job description needed for a text-back estimate.
- If the visitor asks for a callback or phone call, collect name, best phone number, best day/time to call, and job ZIP code in as few messages as possible.
- If the visitor has project photos, tell them they can upload them directly in the chat.
- If recent project photos are attached, use them to identify the likely metalwork type and any clearly visible issues.
- Only describe what is reasonably visible in the photos. If an image is blurry, partial, or not enough, say so and ask for a better angle or one more photo.
- If the job sounds unsafe or urgent, tell them to call 773 798 4107 now.
- Do not give exact final pricing without enough detail.
- If enough context exists, you may give a rough range and clearly frame it as preliminary.
- When you already have the name, phone, job type, and enough details to follow up, stop asking extra questions and close by saying the team will text them back with an estimate or ask for one more detail if needed.
- Do not claim exact measurements, hidden damage, or structural certainty from a photo alone.
- Only ask for email, address, or appointment timing after the basic lead capture when it truly helps.

CONVERSION PLAYBOOK:
- For a fresh lead, usually move in this order: name, best phone number, service type, ZIP or area, repair vs replacement vs new install, short scope details, photos, then text-back estimate or one final follow-up question.
- If the visitor already gave one of those items, do not ask for it again.
- For gate jobs, ask about dragging, sagging, hinge, latch, frame damage, or new gate fabrication.
- For railing jobs, ask whether it is for porch steps, stairs, balcony, or another area, and whether it is repair or new install.
- For fence jobs, ask whether it is one damaged section, a gate section, or a larger new run.
- For welding jobs, ask what piece needs welding, whether it is still installed or loose, and where the job is located.
- Once enough details are present, stop asking endless questions and tell the visitor the team will text back with an estimate or ask for one more detail if needed.
- For WhatsApp, prefer 1 or 2 short sentences and end with one direct question.

SERVICE FIT:
- High-fit: metalwork, welding, railings, handrails, gates, fences, fabrication, stairs, balconies, porch railings.
- Low-fit: painting, flooring, handyman-only work, foundation-only work, door-only work that is not metal-related.
- If the project is low-fit, politely say the business mainly focuses on metalwork and related repairs or fabrication.

QUOTE GUIDANCE:
- Fastest quote path: name, best phone number to text back, photos, rough measurements, ZIP code, and whether the project is repair or new build.
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

function getExternalSyncToken(req) {
  const authHeader = String(req.headers.authorization || "").trim();

  if (/^Bearer\s+/i.test(authHeader)) {
    return authHeader.replace(/^Bearer\s+/i, "").trim();
  }

  return cleanText(req.headers["x-metalworks-sync-token"] || "", 240);
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

function getMetalworksProspectorPassword() {
  return String(process.env.METALWORKS_PROSPECTOR_PASSWORD || "").trim();
}

function metalworksCrmConfigured() {
  return Boolean(getAllowedEmails().length && getMetalworksPassword());
}

function metalworksProspectorConfigured() {
  return Boolean(getMetalworksProspectorPassword());
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

function normalizeClientDocumentType(value = "") {
  return cleanText(value || "", 24).toLowerCase() === "invoice"
    ? "invoice"
    : "estimate";
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

function buildMetalworksClientDocumentSnapshot(lead = null) {
  const fullName =
    sanitizeAssistantStoredName(cleanText(lead?.fullName || "", 120)) ||
    cleanText(lead?.fullName || "", 120);
  const firstName = fullName.split(/\s+/).filter(Boolean)[0] || "there";
  const documentType = normalizeClientDocumentType(lead?.clientDocumentType || "");
  const documentLabel = documentType === "invoice" ? "Invoice" : "Estimate";
  const projectLabel =
    cleanText(lead?.projectType || "", 120) ||
    "metalwork project";
  const description =
    cleanText(lead?.clientDocumentDescription || "", 3200) ||
    cleanText(lead?.details || "", 2400);
  const workDate = formatDateLabel(lead?.clientDocumentWorkDate || "");
  const validUntil = formatDateLabel(lead?.estimateValidUntil || "");
  const warranty =
    cleanText(lead?.clientDocumentWarranty || "", 2400) || METALWORKS_DEFAULT_CLIENT_WARRANTY;
  const totalAmount = normalizeMoney(lead?.estimateAmount || 0);
  const total = totalAmount > 0 ? formatMoneyLabel(totalAmount) : "";
  const location = cleanText(lead?.location || "", 160);
  const phone = cleanText(lead?.phoneDisplay || lead?.phone || "", 40);
  const email = normalizeEmail(lead?.email || "");

  return {
    fullName,
    firstName,
    documentType,
    documentLabel,
    projectLabel,
    description,
    workDate,
    validUntil,
    warranty,
    totalAmount,
    total,
    location,
    phone,
    email,
  };
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

function buildThumbtackOauthCallbackPage({
  code = "",
  state = "",
  error = "",
  errorDescription = "",
} = {}) {
  const safeCode = cleanText(code || "", 240);
  const safeState = cleanText(state || "", 240);
  const safeError = cleanText(error || "", 160);
  const safeErrorDescription = cleanText(errorDescription || "", 600);
  const completed = Boolean(safeCode) && !safeError;
  const title = safeError
    ? "Thumbtack connection was not completed"
    : completed
      ? "Thumbtack connection request received"
      : "Thumbtack callback endpoint is ready";
  const message = safeError
    ? safeErrorDescription || "Thumbtack returned an authorization error."
    : completed
      ? "Chicago Metal Works & Fencing received the authorization redirect. You can close this window and return to the app."
      : "This endpoint is reserved for official Thumbtack OAuth redirects for Chicago Metal Works & Fencing.";

  return `<!DOCTYPE html>
<html lang="en">
  <head>
    <meta charset="UTF-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1.0" />
    <title>${escapeHtmlMarkup(title)}</title>
    <style>
      :root {
        color-scheme: dark;
      }

      * {
        box-sizing: border-box;
      }

      body {
        margin: 0;
        min-height: 100vh;
        display: grid;
        place-items: center;
        padding: 24px;
        font-family: Manrope, system-ui, sans-serif;
        color: #f7f4ef;
        background:
          radial-gradient(circle at top left, rgba(255, 122, 69, 0.18), transparent 28%),
          radial-gradient(circle at 82% 12%, rgba(122, 184, 200, 0.12), transparent 24%),
          linear-gradient(180deg, #0c1117 0%, #111723 48%, #0d1117 100%);
      }

      .card {
        width: min(100%, 720px);
        padding: 32px;
        border: 1px solid rgba(210, 217, 224, 0.12);
        border-radius: 28px;
        background: rgba(17, 24, 33, 0.92);
        box-shadow: 0 26px 70px rgba(0, 0, 0, 0.34);
      }

      .eyebrow {
        margin: 0 0 12px;
        color: #ffd2bf;
        text-transform: uppercase;
        letter-spacing: 0.14em;
        font-size: 0.76rem;
        font-weight: 700;
      }

      h1 {
        margin: 0;
        font-family: Oswald, Impact, sans-serif;
        font-size: clamp(2rem, 5vw, 3.4rem);
        letter-spacing: 0.04em;
        text-transform: uppercase;
      }

      p,
      li {
        color: #c1cad3;
        line-height: 1.75;
      }

      .meta {
        margin-top: 22px;
        padding: 18px;
        border: 1px solid rgba(210, 217, 224, 0.12);
        border-radius: 18px;
        background: rgba(255, 255, 255, 0.04);
      }

      ul {
        margin: 14px 0 0;
        padding-left: 18px;
      }

      a {
        color: #f7f4ef;
        font-weight: 700;
      }
    </style>
  </head>
  <body>
    <article class="card">
      <p class="eyebrow">Chicago Metal Works &amp; Fencing</p>
      <h1>${escapeHtmlMarkup(title)}</h1>
      <p>${escapeHtmlMarkup(message)}</p>
      <div class="meta">
        <ul>
          <li>Authorization code received: ${completed ? "Yes" : "No"}</li>
          <li>State received: ${safeState ? "Yes" : "No"}</li>
          ${safeError ? `<li>Error: ${escapeHtmlMarkup(safeError)}</li>` : ""}
        </ul>
      </div>
      <p>
        Need help? Call <a href="tel:+17737984107">773 798 4107</a> or email
        <a href="mailto:${escapeHtmlMarkup(METALWORKS_CONTACT_EMAIL)}">${escapeHtmlMarkup(
          METALWORKS_CONTACT_EMAIL,
        )}</a>.
      </p>
    </article>
  </body>
</html>`;
}

function buildMetalworksEstimateEmail(lead = null, replyTo = "") {
  const snapshot = buildMetalworksClientDocumentSnapshot(lead);
  const subject = `${snapshot.documentLabel} from Chicago Metal Works & Fencing - ${snapshot.projectLabel}`;
  const textLines = [
    `Hi ${snapshot.firstName},`,
    "",
    "Thank you for contacting Chicago Metal Works & Fencing.",
    "",
    `${snapshot.documentLabel}: ${snapshot.projectLabel}`,
    `Customer: ${snapshot.fullName || "Not provided"}`,
    snapshot.location ? `Job location: ${snapshot.location}` : "",
    snapshot.phone ? `Phone: ${snapshot.phone}` : "",
    snapshot.email ? `Email: ${snapshot.email}` : "",
    snapshot.workDate ? `Work date: ${snapshot.workDate}` : "",
    snapshot.documentType === "estimate" && snapshot.validUntil
      ? `Valid until: ${snapshot.validUntil}`
      : "",
    snapshot.total ? `Total: ${snapshot.total}` : "",
    "",
    snapshot.description ? `Work to be performed:\n${snapshot.description}` : "",
    snapshot.warranty ? `Warranty / terms:\n${snapshot.warranty}` : "",
    "",
    `To move forward, reply to this email or call/text ${METALWORKS_CONTACT_PHONE_DISPLAY}.`,
    "",
    "Chicago Metal Works & Fencing",
    METALWORKS_CONTACT_PHONE_DISPLAY,
    METALWORKS_CONTACT_EMAIL,
    METALWORKS_WEBSITE_URL,
  ].filter(Boolean);

  const html = `
    <div style="font-family:Arial,sans-serif;background:#f8f5ef;padding:24px;color:#1e2428">
      <div style="max-width:680px;margin:0 auto;background:#ffffff;border:1px solid #e5ddd0;border-radius:18px;padding:28px">
        <p style="margin:0 0 16px">Hi ${escapeHtmlMarkup(snapshot.firstName)},</p>
        <p style="margin:0 0 16px">Thank you for contacting <strong>Chicago Metal Works &amp; Fencing</strong>.</p>
        <div style="border:1px solid #eadfcd;border-radius:16px;padding:18px;margin:0 0 18px;background:#fffaf2">
          <p style="margin:0 0 10px"><strong>${escapeHtmlMarkup(snapshot.documentLabel)}:</strong> ${escapeHtmlMarkup(snapshot.projectLabel)}</p>
          <p style="margin:0 0 10px"><strong>Customer:</strong> ${escapeHtmlMarkup(snapshot.fullName || "Not provided")}</p>
          ${snapshot.location ? `<p style="margin:0 0 10px"><strong>Job location:</strong> ${escapeHtmlMarkup(snapshot.location)}</p>` : ""}
          ${snapshot.workDate ? `<p style="margin:0 0 10px"><strong>Work date:</strong> ${escapeHtmlMarkup(snapshot.workDate)}</p>` : ""}
          ${
            snapshot.documentType === "estimate" && snapshot.validUntil
              ? `<p style="margin:0 0 10px"><strong>Valid until:</strong> ${escapeHtmlMarkup(snapshot.validUntil)}</p>`
              : ""
          }
          ${
            snapshot.total
              ? `<p style="margin:0"><strong>Total:</strong> ${escapeHtmlMarkup(snapshot.total)}</p>`
              : ""
          }
        </div>
        ${
          snapshot.description
            ? `<div style="margin:0 0 18px"><p style="margin:0 0 8px"><strong>Work to be performed</strong></p><p style="margin:0;white-space:pre-wrap">${formatMultilineHtml(snapshot.description)}</p></div>`
            : ""
        }
        ${
          snapshot.warranty
            ? `<div style="margin:0 0 18px"><p style="margin:0 0 8px"><strong>Warranty / terms</strong></p><p style="margin:0;white-space:pre-wrap">${formatMultilineHtml(snapshot.warranty)}</p></div>`
            : ""
        }
        <p style="margin:0 0 18px">To move forward, reply to this email or call/text <strong>${escapeHtmlMarkup(METALWORKS_CONTACT_PHONE_DISPLAY)}</strong>.</p>
        <p style="margin:0;color:#66717a">
          Chicago Metal Works &amp; Fencing<br />
          ${escapeHtmlMarkup(METALWORKS_CONTACT_PHONE_DISPLAY)}<br />
          ${escapeHtmlMarkup(METALWORKS_CONTACT_EMAIL)}<br />
          <a href="${escapeHtmlMarkup(METALWORKS_WEBSITE_URL)}" style="color:#66717a">${escapeHtmlMarkup(METALWORKS_WEBSITE_URL)}</a>
        </p>
      </div>
    </div>
  `;

  return {
    to: snapshot.email,
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
  const fullName =
    sanitizeAssistantStoredName(cleanText(lead?.fullName || "", 120)) ||
    cleanText(lead?.fullName || "", 120) ||
    "Lead";
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

function normalizePushEnvironment(value = "") {
  return cleanText(value || "", 40).toLowerCase() === "production"
    ? "production"
    : "sandbox";
}

function normalizePushDeviceToken(value = "") {
  return String(value || "")
    .replace(/[^0-9a-f]/gi, "")
    .toLowerCase()
    .slice(0, 400);
}

function decodeEnvPrivateKey(value = "") {
  return String(value || "")
    .replace(/\r/g, "")
    .replace(/\\n/g, "\n")
    .trim();
}

function base64UrlEncode(value) {
  return Buffer.from(value)
    .toString("base64")
    .replace(/\+/g, "-")
    .replace(/\//g, "_")
    .replace(/=+$/g, "");
}

function getMetalworksApnsConfig() {
  const teamId = cleanText(process.env.METALWORKS_APNS_TEAM_ID || "", 80);
  const keyId = cleanText(process.env.METALWORKS_APNS_KEY_ID || "", 80);
  const privateKey = decodeEnvPrivateKey(process.env.METALWORKS_APNS_PRIVATE_KEY || "");
  const topic =
    cleanText(process.env.METALWORKS_APNS_TOPIC || "", 160) || METALWORKS_IOS_APP_BUNDLE_ID;

  return {
    teamId,
    keyId,
    privateKey,
    topic,
    configured: Boolean(teamId && keyId && privateKey && topic),
  };
}

function metalworksApnsConfigured() {
  return getMetalworksApnsConfig().configured;
}

function getMetalworksApnsJwt() {
  const config = getMetalworksApnsConfig();

  if (!config.configured) {
    throw new Error("Apple push credentials are not configured yet.");
  }

  const cacheKey = `${config.teamId}:${config.keyId}:${hashToken(config.privateKey)}`;

  if (
    METALWORKS_APNS_JWT_CACHE.token &&
    METALWORKS_APNS_JWT_CACHE.cacheKey === cacheKey &&
    METALWORKS_APNS_JWT_CACHE.expiresAt > Date.now() + 60 * 1000
  ) {
    return METALWORKS_APNS_JWT_CACHE.token;
  }

  const issuedAt = Math.floor(Date.now() / 1000);
  const header = base64UrlEncode(JSON.stringify({ alg: "ES256", kid: config.keyId }));
  const payload = base64UrlEncode(JSON.stringify({ iss: config.teamId, iat: issuedAt }));
  const unsignedToken = `${header}.${payload}`;
  const signer = crypto.createSign("SHA256");
  signer.update(unsignedToken);
  signer.end();

  const signature = base64UrlEncode(signer.sign(config.privateKey));
  const token = `${unsignedToken}.${signature}`;

  METALWORKS_APNS_JWT_CACHE.token = token;
  METALWORKS_APNS_JWT_CACHE.cacheKey = cacheKey;
  METALWORKS_APNS_JWT_CACHE.expiresAt = Date.now() + 50 * 60 * 1000;

  return token;
}

function trimPushCopy(value = "", maxLength = 140) {
  return cleanText(value || "", maxLength);
}

function buildMetalworksPushCopy({
  lead = null,
  alertType = "assistant_lead",
  requestedAtLabel = "",
} = {}) {
  const fullName =
    trimPushCopy(sanitizeAssistantStoredName(lead?.fullName || ""), 60) ||
    trimPushCopy(lead?.fullName || "", 60) ||
    "New lead";
  const projectType =
    trimPushCopy(lead?.projectType || "", 54) ||
    trimPushCopy(lead?.estimateTitle || "", 54) ||
    trimPushCopy(lead?.location || "", 54) ||
    "metalwork request";
  const callbackLabel = trimPushCopy(requestedAtLabel || "", 64);

  if (alertType === "assistant_callback" || alertType === "callback_request") {
    return {
      title: "Callback requested",
      body: callbackLabel
        ? `${fullName} asked for ${callbackLabel}.`
        : `${fullName} asked for a callback about ${projectType}.`,
    };
  }

  if (alertType === "crm_test") {
    return {
      title: "Agustin 2.0 CRM",
      body: "Test alert delivered to this iPhone from Chicago Metal Works & Fencing.",
    };
  }

  return {
    title: "New lead",
    body: `${fullName} • ${projectType}`,
  };
}

async function sendMetalworksApnsNotification({
  deviceToken = "",
  alertType = "assistant_lead",
  title = "",
  body = "",
  leadId = "",
  appEnvironment = "sandbox",
} = {}) {
  const config = getMetalworksApnsConfig();

  if (!config.configured) {
    return {
      attempted: false,
      delivered: false,
      error: "Apple push credentials are not configured yet.",
      reason: "",
    };
  }

  const safeToken = normalizePushDeviceToken(deviceToken);

  if (!safeToken) {
    return {
      attempted: false,
      delivered: false,
      error: "The device token is missing.",
      reason: "MissingDeviceToken",
    };
  }

  const host =
    normalizePushEnvironment(appEnvironment) === "production"
      ? "api.push.apple.com"
      : "api.sandbox.push.apple.com";
  const jwt = getMetalworksApnsJwt();

  return await new Promise((resolve) => {
    const client = http2.connect(`https://${host}`);
    let settled = false;

    const finalize = (result) => {
      if (settled) {
        return;
      }

      settled = true;
      client.close();
      resolve(result);
    };

    client.on("error", (error) => {
      finalize({
        attempted: true,
        delivered: false,
        error: error.message || "Apple push connection failed.",
        reason: "",
      });
    });

    const request = client.request({
      ":method": "POST",
      ":path": `/3/device/${safeToken}`,
      authorization: `bearer ${jwt}`,
      "apns-topic": config.topic,
      "apns-priority": "10",
      "apns-push-type": "alert",
      "content-type": "application/json",
    });

    let statusCode = 0;
    let responseBody = "";

    request.setEncoding("utf8");
    request.on("response", (headers) => {
      statusCode = Number(headers[":status"] || 0);
    });
    request.on("data", (chunk) => {
      responseBody += chunk;
    });
    request.on("end", () => {
      let reason = "";

      if (responseBody) {
        try {
          reason = cleanText(JSON.parse(responseBody)?.reason || "", 120);
        } catch {}
      }

      finalize({
        attempted: true,
        delivered: statusCode >= 200 && statusCode < 300,
        status: statusCode,
        error:
          statusCode >= 200 && statusCode < 300
            ? ""
            : reason || `Apple push returned status ${statusCode || "unknown"}.`,
        reason,
      });
    });
    request.on("error", (error) => {
      finalize({
        attempted: true,
        delivered: false,
        error: error.message || "Apple push request failed.",
        reason: "",
      });
    });

    request.end(
      JSON.stringify({
        aps: {
          alert: {
            title: trimPushCopy(title || "", 60),
            body: trimPushCopy(body || "", 140),
          },
          sound: "default",
        },
        leadId: cleanText(leadId || "", 80),
        alertType: cleanText(alertType || "", 60),
      }),
    );
  });
}

function cleanPushDevice(doc = null) {
  if (!doc) {
    return null;
  }

  return {
    id: String(doc._id || ""),
    deviceName: doc.deviceName || "",
    bundleId: doc.bundleId || "",
    appEnvironment: normalizePushEnvironment(doc.appEnvironment || "sandbox"),
    notificationsEnabled: Boolean(doc.notificationsEnabled),
    isActive: Boolean(doc.isActive),
    lastSeenAt: doc.lastSeenAt ? new Date(doc.lastSeenAt).toISOString() : "",
    lastPushAt: doc.lastPushAt ? new Date(doc.lastPushAt).toISOString() : "",
    lastPushError: doc.lastPushError || "",
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
    prospector_lead_submitted: "Lead de prospectador",
    estimate_sent: "Estimate enviado",
    assistant_open: "Assistant abierto",
    assistant_cta_click: "Assistant CTA",
    assistant_user_message: "Mensaje al assistant",
    assistant_ai_reply: "Respuesta del assistant",
    assistant_fallback: "Fallback del assistant",
    assistant_booking_requested: "Cita pedida desde assistant",
    assistant_photo_uploaded: "Fotos subidas desde assistant",
    assistant_whatsapp_followups_scheduled: "Follow-ups de WhatsApp programados",
    assistant_whatsapp_followup_sent: "Follow-up de WhatsApp enviado",
    assistant_whatsapp_followup_skipped: "Follow-up de WhatsApp omitido",
  };

  return labels[type] || "Actividad";
}

function detectSpanish(value = "") {
  return /[¿¡]|\b(hola|precio|cotiza|reparacion|reparación|porton|portón|barandal|soldadura|cerca|reja|gracias|necesito|quiero|ayuda)\b/i.test(
    String(value || ""),
  );
}

function looksLikeStandaloneApplicantRole(value = "") {
  const normalized = normalizeAssistantSearchText(value || "");

  return [
    "welder",
    "fabricator",
    "welder fabricator",
    "welder-fabricator",
    "soldador",
    "fabricador",
    "soldador fabricador",
    "soldador-fabricador",
    "sales",
    "ventas",
    "prospector",
    "prospectador",
  ].includes(normalized);
}

function detectEmploymentCorrection(value = "") {
  const normalized = normalizeAssistantSearchText(value || "");

  if (!normalized) {
    return false;
  }

  return (
    /\b(?:not|is not|isnt|isn't|no)\b[^.!?\n]{0,60}\b(?:hiring|employment|job|jobs|apply|application|position|interview)\b/.test(
      normalized,
    ) ||
    /\b(?:this|it|its|it's)\b[^.!?\n]{0,40}\b(?:project|quote|estimate)\b/.test(normalized) ||
    /\bfor (?:a|this) project\b/.test(normalized) ||
    /\bnot (?:looking for|applying for)\b[^.!?\n]{0,30}\b(?:job|employment|position)\b/.test(
      normalized,
    )
  );
}

function detectEmploymentIntent(value = "") {
  const normalized = normalizeAssistantSearchText(value || "");

  if (!normalized || detectEmploymentCorrection(normalized)) {
    return false;
  }

  if (
    /\b(employment|hiring|hire me|apply|application|position|job opening|open position|position opening|vacante|vacantes|empleo|contratando|aplicar|solicitud|interview|interview for|phone interview|entrevista|trabajar con ustedes|work for you|work with you|are you hiring|looking for a job|need a job|busco trabajo|quiero trabajo|oportunidad de trabajo)\b/.test(
      normalized,
    )
  ) {
    return true;
  }

  return looksLikeStandaloneApplicantRole(normalized);
}

function buildAssistantFallbackReply(message = "", conversationState = null) {
  const text = cleanText(message, 500).toLowerCase();
  const inSpanish = detectSpanish(message);
  const callbackIntent = conversationState?.callbackIntent === "yes";
  const callbackMissingFields = Array.isArray(conversationState?.callbackMissingFields)
    ? conversationState.callbackMissingFields
    : [];
  const leadContactMissingFields = Array.isArray(conversationState?.leadContactMissingFields)
    ? conversationState.leadContactMissingFields
    : [];
  const readyForTextEstimate = conversationState?.readyForTextEstimate === true;
  const serviceBucket = inferAssistantServiceBucket({
    message,
    state: conversationState,
  });

  if (!callbackIntent && leadContactMissingFields.length) {
    if (leadContactMissingFields.includes("name") && leadContactMissingFields.includes("best phone number")) {
      return inSpanish
        ? "Claro. Antes de seguir, ¿con quien tengo el gusto y cual es el mejor numero para textearte por si se corta el chat?"
        : "Absolutely. Before we keep going, who do I have the pleasure of speaking with, and what is the best phone number to text you back in case the chat disconnects?";
    }

    if (leadContactMissingFields.includes("name")) {
      return inSpanish
        ? "Claro. Antes de seguir, ¿con quien tengo el gusto?"
        : "Absolutely. Before we keep going, who do I have the pleasure of speaking with?";
    }

    return inSpanish
      ? "Perfecto. ¿Cual es el mejor numero para textearte por si se corta el chat?"
      : "Perfect. What is the best phone number to text you back in case the chat disconnects?";
  }

  if (callbackIntent) {
    if (callbackMissingFields.length) {
      const missingLabel = callbackMissingFields.join(", ");

      return inSpanish
        ? `Claro. Para dejar la llamada o cita bien pedida mandame solo esto: ${missingLabel}.`
        : `Absolutely. To save the callback or appointment request, send just these details: ${missingLabel}.`;
    }

    const callbackLabel =
      cleanText(conversationState?.callbackLabel || "", 120) || "your requested time";

    return inSpanish
      ? `Perfecto. Ya tengo tu solicitud para ${callbackLabel}. Si puedes, manda fotos y tu ZIP code para preparar mejor el seguimiento.`
      : `Perfect. I have your request for ${callbackLabel}. If you can, send photos and your ZIP code so we can prep the follow-up faster.`;
  }

  if (readyForTextEstimate) {
    return inSpanish
      ? "Perfecto, ya tengo suficiente para pasarlo al equipo. Te vamos a textear a este numero con un estimado basado en esta informacion, o te pediremos un detalle mas si hace falta."
      : "Perfect, I have enough to pass this to the team. We will text you back at this number with an estimate based on this information, or we will ask for one more detail if needed.";
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

  const hotLeadReply = buildAssistantHotLeadReply({
    bucket: serviceBucket,
    state: conversationState,
    inSpanish,
  });

  if (hotLeadReply) {
    return hotLeadReply;
  }

  if (/price|pricing|quote|estimate|cost|how much|precio|cotiza|estimate/i.test(text)) {
    const quoteReply = buildAssistantServiceQuoteReply({
      bucket: serviceBucket,
      state: conversationState,
      inSpanish,
    });

    if (quoteReply) {
      return quoteReply;
    }

    return inSpanish
      ? "La forma mas rapida de cotizar es subir fotos aqui en el chat, mandar medidas aproximadas, tu ZIP code y decir si es reparacion o trabajo nuevo. Si quieres moverlo mas rapido, usa el formulario o llama al 773 798 4107."
      : "The fastest way to get pricing is to upload photos here in the chat, send rough measurements, your ZIP code, and whether you need a repair or a new build. If you want to move faster, use the quote form or call 773 798 4107.";
  }

  if (/where|service area|zip|coverage|chicago|blue island|suburb|cobertura/i.test(text)) {
    return inSpanish
      ? "Trabajamos Chicago, Blue Island y suburbios cercanos. Mandame tu ciudad o ZIP code con una nota corta del proyecto y te confirmo cobertura."
      : "We serve Chicago, Blue Island, and nearby suburbs. Send your city or ZIP code with a short note about the project, and I can confirm coverage fast.";
  }

  const scriptedReply = buildAssistantServiceIntakeReply({
    bucket: serviceBucket,
    state: conversationState,
    message,
    inSpanish,
  });

  if (scriptedReply) {
    return scriptedReply;
  }

  return inSpanish
    ? "Puedo ayudar con portones, barandales, cercas, soldadura y fabricacion metalica. Dime que necesita reparacion o que quieres construir, agrega tu ZIP code, y si tienes fotos subelas aqui en el chat para moverlo mas rapido."
    : "I can help with gates, railings, fence work, welding, and custom metal fabrication. Tell me what needs repair or what you want built, include your ZIP code, and if you have photos, upload them here in the chat so we can move faster.";
}

function buildAssistantHiringDisabledReply(message = "") {
  const inSpanish = detectSpanish(message);

  return inSpanish
    ? "Este asistente del website solo ayuda con clientes, cotizaciones y trabajos de metal. Si necesitas ayuda con un proyecto, mandame una breve descripcion, tu ZIP code y fotos del trabajo."
    : "This website assistant only helps with customer projects, quotes, and metalwork jobs. If you need help with a project, send a short description, your ZIP code, and photos of the work.";
}

function buildAssistantServiceQuoteReply({
  bucket = "",
  state = null,
  inSpanish = false,
} = {}) {
  const hasLocation = Boolean(cleanText(state?.location || "", 160));
  const scopeKind = detectAssistantScopeKind(
    [state?.projectType || "", state?.detailsSummary || "", state?.latestUserMessage || ""].join(" "),
  );

  if (bucket === "gate") {
    return inSpanish
      ? `La forma mas rapida de cotizar un porton es con foto, ${hasLocation ? "el area del trabajo" : "tu ZIP code"}, y si es ${scopeKind ? `un trabajo de ${scopeKind}` : "reparacion, reemplazo o porton nuevo"}. ${hasLocation ? "Cual es el problema principal del porton?" : "Que ZIP code o area es?"}`
      : `The fastest way to quote a gate job is a photo, ${hasLocation ? "the job area" : "your ZIP code"}, and whether this is ${scopeKind ? `a ${scopeKind} job` : "a repair, replacement, or brand-new gate"}. ${hasLocation ? "What is the main issue with the gate?" : "What ZIP code or area is it in?"}`;
  }

  if (bucket === "railing") {
    return inSpanish
      ? `Para cotizar barandales o pasamanos, manda foto, ${hasLocation ? "la ubicacion" : "tu ZIP code"}, y si es reparacion o instalacion nueva. ${hasLocation ? "Es para porch, stairs o balcony?" : "Que ZIP code o area es?"}`
      : `For railings or handrails, the fastest quote path is a photo, ${hasLocation ? "the location" : "your ZIP code"}, and whether it is a repair or new install. ${hasLocation ? "Is it for porch steps, stairs, or a balcony?" : "What ZIP code or area is it in?"}`;
  }

  if (bucket === "fence") {
    return inSpanish
      ? `Para una cerca metalica, manda fotos, ${hasLocation ? "la ubicacion" : "tu ZIP code"}, y dime si es una seccion dañada o trabajo nuevo. ${hasLocation ? "Es una sola seccion o una corrida mas grande?" : "Que ZIP code o area es?"}`
      : `For a metal fence job, send photos, ${hasLocation ? "the location" : "your ZIP code"}, and tell me if this is one damaged section or new work. ${hasLocation ? "Is it one damaged section or a larger new run?" : "What ZIP code or area is it in?"}`;
  }

  if (bucket === "welding") {
    return inSpanish
      ? `Para soldadura, manda fotos de la pieza o del daño, ${hasLocation ? "la ubicacion" : "tu ZIP code"}, y dime si la pieza sigue instalada. ${hasLocation ? "Que pieza ocupa soldadura?" : "Que ZIP code o area es?"}`
      : `For welding, send photos of the damaged metal or the piece you need welded, ${hasLocation ? "the location" : "your ZIP code"}, and tell me if the piece is still installed. ${hasLocation ? "What piece needs welding?" : "What ZIP code or area is it in?"}`;
  }

  return "";
}

function buildAssistantHotLeadReply({
  bucket = "",
  state = null,
  inSpanish = false,
} = {}) {
  if (cleanText(state?.leadTemperature || "", 20) !== "hot") {
    return "";
  }

  if (state?.callbackIntent === "yes" || state?.callbackIntent === "no") {
    return "";
  }

  const hasLocation = Boolean(cleanText(state?.location || "", 160));
  const hasDetail = assistantServiceHasSpecificDetail(
    bucket,
    [state?.detailsSummary || "", state?.latestUserMessage || ""].join(" "),
  );

  if (!hasLocation) {
    return inSpanish
      ? "Perfecto. Te ayudo a moverlo rapido. Que ZIP code o area es el trabajo?"
      : "Perfect. I can help move this fast. What ZIP code or area is the job in?";
  }

  if (bucket === "gate" && !hasDetail) {
    return inSpanish
      ? "Perfecto. Cual es el problema principal del porton para mover esto rapido?"
      : "Perfect. What is the main gate issue so we can move this faster?";
  }

  if (bucket === "railing" && !hasDetail) {
    return inSpanish
      ? "Perfecto. Es para porch, stairs, balcony, o otra area?"
      : "Perfect. Is it for porch steps, stairs, a balcony, or another area?";
  }

  if (bucket === "fence" && !hasDetail) {
    return inSpanish
      ? "Perfecto. Es una sola seccion dañada o una corrida mas grande?"
      : "Perfect. Is it one damaged section or a larger run?";
  }

  if (bucket === "welding" && !hasDetail) {
    return inSpanish
      ? "Perfecto. Que pieza ocupa soldadura y sigue instalada o esta suelta?"
      : "Perfect. What piece needs welding, and is it still installed or already loose?";
  }

  return inSpanish
    ? "Perfecto. Ya casi lo tengo. Si puedes, manda fotos y con esto te texteamos con un estimado o con un detalle final si hace falta."
    : "Perfect. We are almost there. If you can, send photos, and we will text you back with an estimate or one final detail if needed.";
}

function buildAssistantServiceIntakeReply({
  bucket = "",
  state = null,
  message = "",
  inSpanish = false,
} = {}) {
  if (!bucket) {
    return "";
  }

  const hasLocation = Boolean(cleanText(state?.location || "", 160));
  const scopeKind = detectAssistantScopeKind(
    [
      message,
      state?.projectType || "",
      state?.detailsSummary || "",
      state?.latestUserMessage || "",
    ].join(" "),
  );
  const hasDetail = assistantServiceHasSpecificDetail(
    bucket,
    [message, state?.detailsSummary || "", state?.latestUserMessage || ""].join(" "),
  );

  if (bucket === "gate") {
    if (!hasLocation) {
      return inSpanish
        ? "Si ayudamos con reparacion de portones y portones nuevos. Que ZIP code o area es el trabajo?"
        : "Yes, we help with gate repair and new metal gates. What ZIP code or area is the job in?";
    }

    if (!scopeKind) {
      return inSpanish
        ? "Perfecto. Es reparacion del porton actual, reemplazo, o porton nuevo?"
        : "Perfect. Is this a repair on the current gate, a replacement, or a brand-new gate?";
    }

    if (!hasDetail) {
      return inSpanish
        ? "Cual es el problema principal del porton: arrastra, esta caido, bisagra, latch, marco, o algo mas?"
        : "What is the main issue with the gate: dragging, sagging, hinge, latch, frame damage, or something else?";
    }

    return inSpanish
      ? "Perfecto. Si puedes, manda una foto del porton y con eso te texteamos con un estimado o con un detalle final si hace falta."
      : "Perfect. If you can, send a photo of the gate, and we will text you back with an estimate or one final detail if needed.";
  }

  if (bucket === "railing") {
    if (!hasLocation) {
      return inSpanish
        ? "Si trabajamos barandales y pasamanos. Que ZIP code o area es el trabajo?"
        : "Yes, we work on railings and handrails. What ZIP code or area is the job in?";
    }

    if (!scopeKind) {
      return inSpanish
        ? "Es reparacion, reemplazo, o instalacion nueva?"
        : "Is this a repair, replacement, or a new install?";
    }

    if (!hasDetail) {
      return inSpanish
        ? "Es para porch, stairs, balcony, o otra area?"
        : "Is it for porch steps, stairs, a balcony, or another area?";
    }

    return inSpanish
      ? "Perfecto. Si puedes, manda una foto y medida aproximada o cuantos escalones son, y con eso te texteamos con un estimado o con un detalle final si hace falta."
      : "Perfect. If you can, send a photo and a rough length or number of steps, and we will text you back with an estimate or one final detail if needed.";
  }

  if (bucket === "fence") {
    if (!hasLocation) {
      return inSpanish
        ? "Si hacemos reparacion de cercas metalicas y trabajo nuevo. Que ZIP code o area es?"
        : "Yes, we handle metal fence repair and new fence work. What ZIP code or area is it in?";
    }

    if (!scopeKind) {
      return inSpanish
        ? "Es reparacion de una cerca actual, reemplazo, o trabajo nuevo?"
        : "Is this a repair on an existing fence, a replacement, or new work?";
    }

    if (!hasDetail) {
      return inSpanish
        ? "Es una sola seccion dañada, una puerta de cerca, o una corrida mas grande?"
        : "Is it one damaged section, a fence gate section, or a larger run?";
    }

    return inSpanish
      ? "Perfecto. Si puedes, manda fotos de la seccion y una medida aproximada, y con eso te texteamos con un estimado o con un detalle final si hace falta."
      : "Perfect. If you can, send photos of the section and a rough measurement, and we will text you back with an estimate or one final detail if needed.";
  }

  if (bucket === "welding") {
    if (!hasLocation) {
      return inSpanish
        ? "Si hacemos soldadura y reparaciones de metal. Que ZIP code o area es el trabajo?"
        : "Yes, we do welding and metal repair work. What ZIP code or area is the job in?";
    }

    if (!scopeKind) {
      return inSpanish
        ? "Es reparacion de una pieza actual o quieres fabricar algo nuevo?"
        : "Is this a repair on an existing piece or do you need something custom built?";
    }

    if (!hasDetail) {
      return inSpanish
        ? "Que pieza ocupa soldadura y sigue instalada o esta suelta?"
        : "What piece needs welding, and is it still installed or already loose?";
    }

    return inSpanish
      ? "Perfecto. Si puedes, manda una foto clara de la pieza o del daño, y con eso te texteamos con un estimado o con un detalle final si hace falta."
      : "Perfect. If you can, send a clear photo of the piece or the damage, and we will text you back with an estimate or one final detail if needed.";
  }

  return "";
}

function buildAssistantContext(message = "", pagePath = "") {
  const text = cleanText(message, 500).toLowerCase();
  const contextParts = [];
  const bucket = inferAssistantServiceBucket({
    message,
    pagePath,
  });

  contextParts.push(`
METAL WORKS WEBSITE CONTEXT:
- Business: Chicago Metal Works & Fencing
- Service area: Chicago, Blue Island, and nearby suburbs
- Main CTA phone: 773 798 4107
- Best quote path: name, best phone number to text back, photos, rough measurements, ZIP code, and whether the job is repair or new build
- Public website page: ${cleanText(pagePath || "", 120) || "/"}
- Intake order: name, best phone number, service type, ZIP or area, repair vs replacement vs new install, short scope details, photos, then text-back estimate
`);

  if (/price|pricing|quote|estimate|cost|how much|precio|cotiza/i.test(text)) {
    contextParts.push(`
QUOTE RULE:
- Do not give a firm final price without enough detail.
- Ask for photos, rough measurements, ZIP code, and whether the job is repair or new installation.
- If enough context exists, frame price as a preliminary range.
`);
  }

  if (bucket === "gate") {
    contextParts.push(`
GATE CONTEXT:
- Common issues: dragging gates, latch issues, hinge issues, frame repairs, rewelds, replacement sections.
- Ask in this order when needed: ZIP, repair vs replace vs new gate, main issue, photo, callback or site visit.
`);
  }

  if (bucket === "railing") {
    contextParts.push(`
RAILING CONTEXT:
- Typical work: porch railings, stair handrails, balcony railings, repairs, replacements, new installs.
- Ask in this order when needed: ZIP, repair vs new install, porch/stairs/balcony, rough length or steps, photo, callback or site visit.
`);
  }

  if (bucket === "fence") {
    contextParts.push(`
FENCE CONTEXT:
- Typical work: metal fence repair, damaged sections, ornamental fence fabrication, new fence installs.
- Ask in this order when needed: ZIP, repair vs new work, damaged section vs larger run, rough footage if known, photo, callback or site visit.
`);
  }

  if (bucket === "welding") {
    contextParts.push(`
WELDING CONTEXT:
- Mobile welding and on-site metal repairs are part of the service mix.
- Ask in this order when needed: ZIP, repair vs custom build, what piece needs welding, whether it is still installed, photo, callback or site visit.
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

function detectAssistantScopeKind(value = "") {
  const normalized = normalizeAssistantSearchText(value);

  if (!normalized) {
    return "";
  }

  if (/\b(repair|repairing|fix|fixing|broken|damage|damaged|loose|reweld|rust repair|weld repair|dragging|sagging)\b/.test(normalized)) {
    return "repair";
  }

  if (/\b(replace|replacement|swap out|tear out)\b/.test(normalized)) {
    return "replace";
  }

  if (/\b(new|new install|new installation|install|installation|fabricat|custom build|build new)\b/.test(normalized)) {
    return "new";
  }

  return "";
}

function inferAssistantServiceBucket({
  message = "",
  state = null,
  pagePath = "",
} = {}) {
  const combined = normalizeAssistantSearchText(
    [
      message,
      state?.projectType || "",
      state?.detailsSummary || "",
      state?.latestUserMessage || "",
      pagePath,
    ]
      .filter(Boolean)
      .join(" "),
  );

  if (!combined) {
    return "";
  }

  if (/\b(gate|gates|hinge|hinges|latch|dragging|sagging|porton)\b/.test(combined)) {
    return "gate";
  }

  if (/\b(railing|railings|handrail|handrails|stairs|stair|steps|balcony|porch|barandal|pasamano|pasamanos)\b/.test(combined)) {
    return "railing";
  }

  if (/\b(fence|fencing|ornamental|iron fence|metal fence|cerca|reja)\b/.test(combined)) {
    return "fence";
  }

  if (/\b(weld|welding|mobile welding|on site|onsite|solda)\b/.test(combined)) {
    return "welding";
  }

  return "";
}

function assistantServiceHasSpecificDetail(bucket = "", value = "") {
  const normalized = normalizeAssistantSearchText(value);

  if (!normalized) {
    return false;
  }

  if (bucket === "gate") {
    return /\b(dragging|sagging|hinge|hinges|latch|frame|post|wheel|track|wont close|won't close|doesnt close|doesn't close)\b/.test(normalized);
  }

  if (bucket === "railing") {
    return /\b(porch|stairs|stair|steps|balcony|interior|exterior|loose|rust|broken|landing)\b/.test(normalized);
  }

  if (bucket === "fence") {
    return /\b(section|panel|post|gate section|ornamental|picket|foot|feet|linear|damaged area)\b/.test(normalized);
  }

  if (bucket === "welding") {
    return /\b(crack|broken|piece|part|bracket|frame|steel|iron|aluminum|installed|on site|onsite|loose)\b/.test(normalized);
  }

  return false;
}

function detectAssistantBuyingIntent(text = "") {
  const normalized = normalizeAssistantSearchText(text);

  if (!normalized) {
    return false;
  }

  return /(?:ready to move forward|ready to start|ready to book|want to book|want to schedule|need to schedule|want this fixed|need this fixed|need this done|need someone out|need somebody out|can you come|can someone come|can somebody come|when can you come|when can someone come|when are you available|availability|are you available|stop by|send someone|come tomorrow|come this week|next week works|this week works|want a site visit|need a site visit|in person estimate|i want to hire|quiero avanzar|listo para avanzar|listo para empezar|quiero agendar|quiero visita|necesito que vengan|pueden venir|cuando pueden venir|cuando estan disponibles|manden a alguien|quiero contratar)/.test(
    normalized,
  );
}

function resolveAssistantLeadTemperature({
  callbackIntent = "",
  buyingIntent = false,
  serviceBucket = "",
  location = "",
  detailsSummary = "",
  latestUserMessage = "",
  photoFileCount = 0,
} = {}) {
  const hasLocation = Boolean(cleanText(location || "", 160));
  const combinedDetails = [detailsSummary, latestUserMessage].filter(Boolean).join(" ");
  const hasServiceDetail =
    assistantServiceHasSpecificDetail(serviceBucket, combinedDetails) ||
    Boolean(cleanText(detailsSummary || "", 120));

  if (callbackIntent === "yes") {
    return "hot";
  }

  if (buyingIntent && (hasLocation || hasServiceDetail || Number(photoFileCount || 0) > 0)) {
    return "hot";
  }

  if (buyingIntent || (serviceBucket && (hasLocation || hasServiceDetail || Number(photoFileCount || 0) > 0))) {
    return "warm";
  }

  return "cold";
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

  return /(?:call me|give me a call|can you call|can someone call|talk by phone|talk on the phone|phone call|schedule a call|set up a call|set up an appointment|schedule an appointment|set up a visit|schedule a visit|come by|come out|when can you come|when are you available|availability|stop by|send someone|site visit|estimate visit|quote visit|in person estimate|reach me|follow up by phone|llamame|llamarme|me pueden llamar|quiero una llamada|quiero llamada|agendar llamada|agendar una llamada|agendar cita|agendar una cita|agendar visita|agendar una visita|pueden venir|pueden pasar|cuando pueden venir|cuando estan disponibles|manden a alguien|visita para estimate|visita para cotizacion|hablar por telefono|marcame)/.test(
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
    /\b(?:at|a las|alas|sobre las|por ahi de las)\s+(\d{1,2}(?::\d{2})?\s*(?:am|pm)?)\b/i,
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

function formatAssistantCalendarDayKey(value = "", timeZone = METALWORKS_CALLBACK_TIME_ZONE) {
  if (!value) {
    return "";
  }

  const date = value instanceof Date ? value : new Date(value);

  if (Number.isNaN(date.getTime())) {
    return "";
  }

  const parts = getAssistantZonedParts(date, timeZone);

  if (!parts.year || !parts.month || !parts.day) {
    return "";
  }

  return `${String(parts.year).padStart(4, "0")}-${String(parts.month).padStart(2, "0")}-${String(parts.day).padStart(2, "0")}`;
}

function parseAssistantCalendarDayKey(value = "") {
  const safeValue = cleanText(value || "", 40);

  if (!safeValue) {
    return null;
  }

  const match = safeValue.match(/^(\d{4})-(\d{2})-(\d{2})$/);

  if (!match) {
    return null;
  }

  const year = Number(match[1] || 0);
  const month = Number(match[2] || 0);
  const day = Number(match[3] || 0);

  if (!year || !month || !day) {
    return null;
  }

  return { year, month, day };
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
  const explicitDayParts = parseAssistantCalendarDayKey(bestContactDay);

  if (explicitDayParts) {
    return explicitDayParts;
  }

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
  const photoFileCount = Array.isArray(lead?.photoFileNames)
    ? lead.photoFileNames.filter(Boolean).length
    : 0;
  const storedNextActionAt =
    lead?.nextActionAt instanceof Date
      ? lead.nextActionAt
      : lead?.nextActionAt
        ? new Date(lead.nextActionAt)
        : null;
  const hasStoredNextActionAt =
    storedNextActionAt instanceof Date && !Number.isNaN(storedNextActionAt.getTime());
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

  const latestBestContactDay = extractAssistantPreferredDay(latestUserMessage);
  const latestBestContactTime = extractAssistantPreferredTime(latestUserMessage);
  const storedCalendarDayKey = hasStoredNextActionAt
    ? formatAssistantCalendarDayKey(storedNextActionAt, METALWORKS_CALLBACK_TIME_ZONE)
    : "";
  const bestContactDayForResolution =
    latestBestContactDay || !storedCalendarDayKey ? bestContactDay : storedCalendarDayKey;

  if (!projectType && /metal-porch-repair|porch/i.test(pagePath || "")) {
    projectType = "Metal porch repair / restoration";
  }

  const nextActionAt =
    hasStoredNextActionAt && !latestBestContactDay && !latestBestContactTime
      ? storedNextActionAt
      : buildAssistantNextActionAt(bestContactDayForResolution, bestContactTime);
  const normalizedBestContactDay =
    nextActionAt instanceof Date && !Number.isNaN(nextActionAt.getTime())
      ? formatAssistantCalendarDayKey(nextActionAt, METALWORKS_CALLBACK_TIME_ZONE)
      : bestContactDay;
  const callbackLabel = formatAssistantCallbackLabel({
    nextActionAt,
    bestContactDay: normalizedBestContactDay,
    bestContactTime,
  });
  const callbackMissingFields =
    callbackIntent === "yes"
      ? [
          !name ? "name" : "",
          !phone && !email ? "phone or email" : "",
          !normalizedBestContactDay ? "best day" : "",
          !bestContactTime ? "best time" : "",
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
    callbackIntent === "yes" ? "Visitor asked for a call or appointment." : "",
    callbackLabel ? `Best requested time: ${callbackLabel}.` : "",
  ]
    .filter(Boolean)
    .join(" ")
    .trim()
    .slice(0, 1500);
  const serviceBucket = inferAssistantServiceBucket({
    message: combinedUserText || latestUserMessage,
    state: {
      projectType,
      detailsSummary,
      latestUserMessage,
    },
    pagePath,
  });
  const buyingIntent = detectAssistantBuyingIntent(combinedUserText || latestUserMessage);
  const leadTemperature = resolveAssistantLeadTemperature({
    callbackIntent,
    buyingIntent,
    serviceBucket,
    location,
    detailsSummary,
    latestUserMessage,
    photoFileCount,
  });
  const inSpanish = detectSpanish(combinedUserText || latestUserMessage);
  const hasProjectRequest = Boolean(
    projectType ||
      serviceBucket ||
      detectAssistantProjectLeadIntent(detailsSummary || latestUserMessage),
  );
  const leadContactMissingFields = [
    !name ? "name" : "",
    !phone ? "best phone number" : "",
  ].filter(Boolean);
  const leadProjectMissingFields = [
    !hasProjectRequest ? "what needs to be repaired or built" : "",
    !location ? "ZIP code" : "",
    photoFileCount > 0 ? "" : "photos",
  ].filter(Boolean);
  const readyForTextEstimate = Boolean(
    name &&
      phone &&
      hasProjectRequest &&
      (location || photoFileCount > 0),
  );

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
    leadContactMissingFields,
    leadProjectMissingFields,
    readyForTextEstimate,
    bestContactDay: normalizedBestContactDay,
    bestContactTime,
    callbackIntent: callbackIntent === "no" ? "no" : callbackIntent === "yes" ? "yes" : "",
    callbackMissingFields,
    nextActionAt,
    callbackLabel,
    serviceBucket,
    buyingIntent,
    leadTemperature,
    detailsSummary,
    photoFileCount,
    shouldCreateLead: Boolean(
      lead?._id || phone || email || callbackIntent === "yes" || photoFileCount > 0 || hasProjectRequest,
    ),
    shouldAlert:
      ((callbackIntent === "yes" || lead?.callbackIntent === "yes") && Boolean(phone || email)) ||
      readyForTextEstimate,
    conversationDigest: buildAssistantHistoryDigest(items),
  };
}

function buildAssistantStatePrompt(state = {}) {
  const callbackIntent = state?.callbackIntent === "yes" ? "yes" : state?.callbackIntent === "no" ? "no" : "unknown";
  const missingFields = Array.isArray(state?.callbackMissingFields) ? state.callbackMissingFields.join(", ") : "";
  const missingContactFields = Array.isArray(state?.leadContactMissingFields)
    ? state.leadContactMissingFields.join(", ")
    : "";
  const missingProjectFields = Array.isArray(state?.leadProjectMissingFields)
    ? state.leadProjectMissingFields.join(", ")
    : "";
  const callbackLabel = cleanText(state?.callbackLabel || "", 120) || "pending";
  const responseChannel = cleanText(state?.sourceChannel || "web", 40) || "web";
  const leadTemperature = cleanText(state?.leadTemperature || "cold", 20) || "cold";
  const serviceBucket = cleanText(state?.serviceBucket || "general", 40) || "general";
  const buyingIntent = state?.buyingIntent ? "yes" : "no";
  const readyForTextEstimate = state?.readyForTextEstimate === true ? "yes" : "no";
  const visionImageCount = Number(state?.visionImageCount || 0) || 0;

  return `
TEXT QUOTE CAPTURE STATE:
- response_channel: ${responseChannel}
- lead_temperature: ${leadTemperature}
- buying_intent: ${buyingIntent}
- service_bucket: ${serviceBucket}
- callback_intent: ${callbackIntent}
- visitor_name: ${state?.name || "pending"}
- phone: ${state?.phoneDisplay || state?.phone || "pending"}
- email: ${state?.email || "pending"}
- project_type: ${state?.projectType || "pending"}
- location: ${state?.location || "pending"}
- uploaded_photos: ${Number(state?.photoFileCount || 0) || 0}
- vision_images_attached: ${visionImageCount}
- ready_for_text_estimate: ${readyForTextEstimate}
- missing_contact_fields: ${missingContactFields || "none"}
- missing_project_fields: ${missingProjectFields || "none"}
- best_day: ${state?.bestContactDay || "pending"}
- best_time: ${state?.bestContactTime || "pending"}
- callback_window: ${callbackLabel}
- missing_callback_fields: ${missingFields || "none"}

INSTRUCTIONS:
- If response_channel is whatsapp, keep replies extra short and text-message friendly.
- If callback_intent is not yes and there are missing_contact_fields, ask only for the missing contact fields first in one short message. Explain it is so the team can text back if the chat disconnects.
- After contact is captured, keep moving toward photos, ZIP code, and the job details needed for a text-back estimate.
- If lead_temperature is hot, stop educating and move directly toward the next missing detail needed to text back an estimate.
- If ready_for_text_estimate is yes and callback_intent is not yes, stop qualifying and close the conversation. Tell the visitor the team will text them back with an estimate based on this information, or ask for one more detail if needed.
- If callback_intent is yes and there are missing callback fields, ask only for the missing callback fields in one short message.
- If callback_intent is yes and contact details are already present, confirm the appointment or callback request and ask for photos or ZIP code only if still useful.
- If uploaded_photos is greater than 0, acknowledge the photos are already attached to the lead.
- If vision_images_attached is greater than 0, use those images to identify the likely issue or job type when that is genuinely visible. If the photos are not enough, say what is unclear and ask for one more angle.
- Do not say the appointment is booked unless the visitor actually gave a specific day and time.
- Only ask for email, address, or scheduling details after the basic lead capture if that truly helps.
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
  const sourceLabel =
    cleanText(state?.sourceLabel || "", 120) || "Agustin 2.0 website assistant";

  return [
    `Source: ${sourceLabel}.`,
    state?.leadTemperature ? `Lead temperature: ${state.leadTemperature}.` : "",
    state?.buyingIntent ? "Buying intent detected: yes." : "",
    state?.projectType ? `Project type: ${state.projectType}.` : "",
    state?.location ? `Location: ${state.location}.` : "",
    state?.photoFileCount ? `Uploaded photos: ${state.photoFileCount}.` : "",
    state?.callbackIntent === "yes" ? "Call or appointment requested: yes." : "",
    state?.callbackIntent === "no" ? "Callback requested: no." : "",
    state?.callbackLabel ? `Best requested time: ${state.callbackLabel}.` : "",
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

function sanitizeLeadAssetFileName(value = "", fallbackName = "project-photo.jpg") {
  const rawName = String(value || "").trim();
  const sanitized = rawName
    .replace(/[/\\?%*:|"<>]/g, "-")
    .replace(/\s+/g, " ")
    .trim();

  return cleanText(sanitized || fallbackName, 120) || fallbackName;
}

function normalizeLeadAssetMimeType(value = "") {
  const safeValue = cleanText(value || "", 80).toLowerCase();

  if (/^image\/(?:jpeg|jpg|png|webp|gif|heic|heif|bmp)$/i.test(safeValue)) {
    return safeValue === "image/jpg" ? "image/jpeg" : safeValue;
  }

  return "";
}

function parseAssistantLeadAssetUpload(payload = {}) {
  const explicitMimeType = normalizeLeadAssetMimeType(payload?.mimeType || "");
  const dataUrl = String(payload?.dataUrl || "").trim();
  const dataUrlMatch = dataUrl.match(/^data:([^;]+);base64,([A-Za-z0-9+/=\s]+)$/i);
  const mimeType = normalizeLeadAssetMimeType(dataUrlMatch?.[1] || explicitMimeType);

  if (!mimeType) {
    throw new Error("Only image uploads are allowed.");
  }

  if (!dataUrlMatch?.[2]) {
    throw new Error("Image payload is invalid.");
  }

  const buffer = Buffer.from(dataUrlMatch[2].replace(/\s+/g, ""), "base64");

  if (!buffer.length) {
    throw new Error("Image payload is empty.");
  }

  if (buffer.length > METALWORKS_LEAD_ASSET_MAX_BYTES) {
    throw new Error("Each image must be 2 MB or less after upload.");
  }

  return {
    fileName: sanitizeLeadAssetFileName(payload?.fileName || ""),
    mimeType,
    sizeBytes: buffer.length,
    fileData: buffer,
  };
}

function cleanLeadAsset(doc = null) {
  if (!doc) {
    return null;
  }

  return {
    id: String(doc._id || ""),
    leadId: doc.leadId ? String(doc.leadId) : "",
    fileName: doc.fileName || "",
    mimeType: doc.mimeType || "",
    sizeBytes: Number(doc.sizeBytes || 0) || 0,
    uploadedAt: doc.uploadedAt ? new Date(doc.uploadedAt).toISOString() : "",
    downloadUrl: doc._id
      ? `/api/metalworks-crm/assets/${encodeURIComponent(String(doc._id))}/content`
      : "",
  };
}

function normalizeLeadAssetBuffer(value = null) {
  if (!value) {
    return Buffer.alloc(0);
  }

  if (Buffer.isBuffer(value)) {
    return value;
  }

  if (value instanceof Uint8Array) {
    return Buffer.from(value);
  }

  if (value?.buffer instanceof ArrayBuffer) {
    return Buffer.from(value.buffer);
  }

  if (Array.isArray(value)) {
    return Buffer.from(value);
  }

  if (typeof value === "object" && value?.type === "Buffer" && Array.isArray(value?.data)) {
    return Buffer.from(value.data);
  }

  return Buffer.alloc(0);
}

async function loadRecentLeadAssetsForVision({
  leadId = null,
  limit = METALWORKS_ASSISTANT_VISION_MAX_IMAGES,
} = {}) {
  if (!leadId || limit < 1) {
    return [];
  }

  const assetDocs = await MetalworksLeadAsset.find({ leadId })
    .sort({ uploadedAt: -1, createdAt: -1 })
    .limit(limit)
    .select("fileName mimeType fileData sizeBytes uploadedAt createdAt");

  return assetDocs
    .map((doc) => {
      const mimeType = normalizeLeadAssetMimeType(doc?.mimeType || "");
      const fileData = normalizeLeadAssetBuffer(doc?.fileData);

      if (!mimeType || !fileData.length) {
        return null;
      }

      return {
        fileName: sanitizeLeadAssetFileName(doc?.fileName || ""),
        mimeType,
        sizeBytes: Number(doc?.sizeBytes || fileData.length) || fileData.length,
        uploadedAt: doc?.uploadedAt || doc?.createdAt || null,
        dataUrl: `data:${mimeType};base64,${fileData.toString("base64")}`,
      };
    })
    .filter(Boolean);
}

function buildAssistantUserInputContent({
  message = "",
  visionAssets = [],
} = {}) {
  const safeMessage = cleanText(message || "", 500);
  const assets = Array.isArray(visionAssets) ? visionAssets.filter(Boolean) : [];
  const content = [];
  const recentPhotoLabel = assets.length
    ? `Recent uploaded project photos are attached below (${assets.length}, most recent first). Use them only as visual context for this project.`
    : "";
  const inputText = [safeMessage, recentPhotoLabel].filter(Boolean).join("\n\n").trim();

  if (inputText) {
    content.push({
      type: "input_text",
      text: inputText,
    });
  }

  assets.forEach((asset) => {
    if (!asset?.dataUrl) {
      return;
    }

    content.push({
      type: "input_image",
      image_url: asset.dataUrl,
      detail: "auto",
    });
  });

  return content.length
    ? content
    : [
        {
          type: "input_text",
          text: safeMessage,
        },
      ];
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
  visionAssets = [],
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
            content: buildAssistantUserInputContent({
              message,
              visionAssets,
            }),
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

function getMetalworksWhatsAppFollowupStepConfig(step = "") {
  return (
    METALWORKS_WHATSAPP_FOLLOWUP_STEPS.find((item) => item.step === cleanText(step || "", 40)) ||
    null
  );
}

function metalworksWhatsAppFollowupsEnabled() {
  return String(process.env.METALWORKS_WHATSAPP_FOLLOWUPS_ENABLED || "").toLowerCase() === "true";
}

function buildAssistantFollowupFallbackReply({
  step = "",
  conversationState = null,
} = {}) {
  const stepConfig = getMetalworksWhatsAppFollowupStepConfig(step);
  const inSpanish = Boolean(conversationState?.inSpanish);
  const missingFields = Array.isArray(conversationState?.callbackMissingFields)
    ? conversationState.callbackMissingFields
    : [];
  const serviceBucket = cleanText(conversationState?.serviceBucket || "", 40);
  const callbackLabel =
    cleanText(conversationState?.callbackLabel || "", 120) || "your requested time";

  if (conversationState?.callbackIntent === "yes" && missingFields.length) {
    const missingLabel = missingFields.join(", ");
    return inSpanish
      ? `Solo me faltan estos datos para dejar la llamada o visita lista: ${missingLabel}.`
      : `I just need these details to line up the callback or site visit: ${missingLabel}.`;
  }

  if (stepConfig?.step === "nudge_10m" && cleanText(conversationState?.leadTemperature || "", 20) === "hot") {
    return inSpanish
      ? "Sigo aqui para mover esto rapido. Prefieres llamada o visita para estimate?"
      : "I’m still here to move this fast. Would you like a callback or a site visit?";
  }

  if (stepConfig?.step === "nudge_10m") {
    if (serviceBucket === "gate") {
      return inSpanish
        ? "Si gustas, manda una foto del porton y tu ZIP code y te ayudo con el siguiente paso."
        : "If you want, send a photo of the gate and your ZIP code and I’ll help with the next step.";
    }

    if (serviceBucket === "railing") {
      return inSpanish
        ? "Si gustas, manda una foto del barandal o pasamanos y tu ZIP code y seguimos de alli."
        : "If you want, send a photo of the railing or handrail and your ZIP code and we can keep moving.";
    }

    if (serviceBucket === "fence") {
      return inSpanish
        ? "Si gustas, manda fotos de la cerca y tu ZIP code y te ayudo a mover la cotizacion."
        : "If you want, send photos of the fence and your ZIP code and I’ll help move the quote forward.";
    }

    if (serviceBucket === "welding") {
      return inSpanish
        ? "Si gustas, manda una foto de la pieza o del daño y tu ZIP code y seguimos de alli."
        : "If you want, send a photo of the piece or the damage and your ZIP code and we can keep moving.";
    }
  }

  if (stepConfig?.step === "nudge_6h") {
    return inSpanish
      ? "Si todavia ocupas ayuda con este trabajo, responde aqui y tambien puedes mandar fotos para avanzar mas rapido."
      : "If you still need help with this job, reply here and you can also send photos to move it faster.";
  }

  if (conversationState?.callbackIntent === "yes" && !missingFields.length) {
    return inSpanish
      ? `Sigo teniendo tu solicitud para ${callbackLabel}. Si quieres agregar fotos o detalles, responde aqui.`
      : `I still have your request for ${callbackLabel}. If you want to add photos or details, reply here.`;
  }

  return inSpanish
    ? "Todavia te puedo ayudar por aqui. Si quieres llamada, visita o mandar fotos, responde a este mensaje."
    : "I can still help here. If you want a callback, site visit, or to send photos, just reply to this message.";
}

function buildAssistantFollowupSystemPrompt({
  step = "",
  conversationState = null,
} = {}) {
  const stepConfig = getMetalworksWhatsAppFollowupStepConfig(step);

  return `
WHATSAPP FOLLOW-UP TASK:
- You are sending an outbound follow-up inside an active customer-service WhatsApp window.
- The visitor has not replied since the last assistant message.
- Keep the follow-up under 240 characters.
- Sound human, practical, helpful, and low-pressure.
- Do not use a long greeting.
- Ask at most one direct question.
- Do not mention policies, the 24-hour window, automation, or templates.
- If lead_temperature is hot, move directly toward callback or site visit.
- If callback_intent is yes and fields are still missing, ask only for the missing fields.
- If photos would help, you may mention sending photos.
- Step right now: ${stepConfig?.label || cleanText(step || "", 80) || "follow-up"}.
`;
}

async function generateAssistantFollowupReply({
  step = "",
  history = [],
  pagePath = "",
  conversationState = null,
} = {}) {
  const fallbackReply = buildAssistantFollowupFallbackReply({
    step,
    conversationState,
  });
  const apiKey = String(process.env.OPENAI_API_KEY || "").trim();

  if (!apiKey) {
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
        max_output_tokens: 160,
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
            content: buildAssistantContext(
              cleanText(
                conversationState?.latestUserMessage ||
                  conversationState?.detailsSummary ||
                  conversationState?.projectType ||
                  "",
                500,
              ),
              pagePath,
            ),
          },
          {
            role: "system",
            content: buildAssistantStatePrompt(conversationState || {}),
          },
          {
            role: "system",
            content: buildAssistantFollowupSystemPrompt({
              step,
              conversationState,
            }),
          },
          ...normalizeAssistantHistory(history),
          {
            role: "user",
            content: `Create the ${cleanText(step || "follow-up", 40)} WhatsApp follow-up now.`,
          },
        ],
      }),
    });

    const data = await response.json().catch(() => null);
    const reply = extractAssistantResponseText(data);

    if (!response.ok || !reply) {
      return {
        reply: fallbackReply,
        usedFallback: true,
        reason:
          cleanText(
            data?.error?.message || data?.message || `OpenAI error ${response.status}`,
            240,
          ) || "OpenAI error",
      };
    }

    return {
      reply,
      usedFallback: false,
      reason: "",
    };
  } catch (error) {
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

  const safeFullName =
    sanitizeAssistantStoredName(doc.fullName || "") || cleanText(doc.fullName || "", 120);

  return {
    id: String(doc._id || ""),
    fullName: safeFullName,
    phone: doc.phone || "",
    phoneDisplay: doc.phoneDisplay || "",
    email: doc.email || "",
    projectType: doc.projectType || "",
    location: doc.location || "",
    addressLine: doc.addressLine || "",
    zipCode: doc.zipCode || "",
    city: doc.city || "",
    propertyType: doc.propertyType || "",
    projectSize: doc.projectSize || "",
    timeline: doc.timeline || "",
    ownershipStatus: doc.ownershipStatus || "",
    budgetRange: doc.budgetRange || "",
    urgency: doc.urgency || "",
    bestContactWindow: doc.bestContactWindow || "",
    preferredLanguage: doc.preferredLanguage || "",
    qualificationTier: doc.qualificationTier || "",
    qualificationNotes: doc.qualificationNotes || "",
    sourceProspectorName: doc.sourceProspectorName || "",
    sourceProspectorEmail: doc.sourceProspectorEmail || "",
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
    clientDocumentType: normalizeClientDocumentType(doc.clientDocumentType || ""),
    clientDocumentDescription: doc.clientDocumentDescription || "",
    clientDocumentWorkDate: doc.clientDocumentWorkDate ? new Date(doc.clientDocumentWorkDate).toISOString() : "",
    clientDocumentWarranty: doc.clientDocumentWarranty || "",
    estimateSentAt: doc.estimateSentAt ? new Date(doc.estimateSentAt).toISOString() : "",
    estimateSentTo: doc.estimateSentTo || "",
    pageTitle: doc.pageTitle || "",
    pagePath: doc.pagePath || "",
    pageUrl: doc.pageUrl || "",
    referrer: doc.referrer || "",
    sourceType: doc.sourceType || "website_form",
    sourceExternalId: doc.sourceExternalId || "",
    sourceExternalSystem: doc.sourceExternalSystem || "",
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
      { addressLine: pattern },
      { zipCode: pattern },
      { city: pattern },
      { details: pattern },
      { projectType: pattern },
      { qualificationNotes: pattern },
      { sourceProspectorName: pattern },
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

async function buildProspectorDashboardSnapshot(MetalworksLead, prospectorEmail = "") {
  const safeEmail = normalizeEmail(prospectorEmail || "");

  if (!safeEmail) {
    return {
      summary: {
        totalLeads: 0,
        newLeads: 0,
        contactedLeads: 0,
        quotedLeads: 0,
        wonLeads: 0,
      },
      recentLeads: [],
    };
  }

  const baseQuery = {
    sourceProspectorEmail: safeEmail,
  };

  const [recentLeads, totalLeads, newLeads, contactedLeads, quotedLeads, wonLeads] =
    await Promise.all([
      MetalworksLead.find(baseQuery)
        .sort({ createdAt: -1, updatedAt: -1 })
        .limit(18)
        .lean(),
      MetalworksLead.countDocuments(baseQuery),
      MetalworksLead.countDocuments({ ...baseQuery, status: "new" }),
      MetalworksLead.countDocuments({ ...baseQuery, status: "contacted" }),
      MetalworksLead.countDocuments({ ...baseQuery, status: "quoted" }),
      MetalworksLead.countDocuments({ ...baseQuery, status: "won" }),
    ]);

  return {
    summary: {
      totalLeads,
      newLeads,
      contactedLeads,
      quotedLeads,
      wonLeads,
    },
    recentLeads: recentLeads.map(cleanLead).filter(Boolean),
  };
}

export function registerMetalworksCrm(
  app,
  {
    mongoose,
    publicDir,
    privateDir,
    redisGetJson = null,
    redisSetJson = null,
    redisDelete = null,
    redisSessionTtlSeconds = 0,
  },
) {
  const assistantFastCacheTtlSeconds = Math.max(
    5 * 60,
    Number(redisSessionTtlSeconds || 6 * 60 * 60) || 6 * 60 * 60,
  );

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
    addressLine: String,
    zipCode: String,
    city: String,
    propertyType: String,
    projectSize: String,
    timeline: String,
    ownershipStatus: String,
    budgetRange: String,
    urgency: String,
    bestContactWindow: String,
    preferredLanguage: String,
    qualificationTier: String,
    qualificationNotes: String,
    sourceProspectorName: String,
    sourceProspectorEmail: String,
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
    clientDocumentType: { type: String, default: "estimate" },
    clientDocumentDescription: String,
    clientDocumentWorkDate: Date,
    clientDocumentWarranty: String,
    estimateSentAt: Date,
    estimateSentTo: String,
    sourceType: { type: String, default: "website_form", index: true },
    sourceExternalId: { type: String, index: true },
    sourceExternalSystem: String,
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

  const metalworksLeadAssetSchema = new mongoose.Schema({
    leadId: {
      type: mongoose.Schema.Types.ObjectId,
      ref: "MetalworksLead",
      default: null,
      index: true,
    },
    visitorId: { type: String, index: true },
    sessionId: { type: String, index: true },
    sourceType: { type: String, default: "assistant_chat_photo" },
    fileName: String,
    mimeType: String,
    sizeBytes: Number,
    fileData: Buffer,
    uploadedAt: { type: Date, default: Date.now, index: true },
    updatedAt: Date,
    createdAt: { type: Date, default: Date.now },
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
  const metalworksProspectorSessionSchema = new mongoose.Schema({
    prospectorName: { type: String, required: true },
    prospectorEmail: { type: String, required: true, index: true },
    tokenHash: { type: String, required: true, unique: true, index: true },
    ipAddress: String,
    userAgent: String,
    expiresAt: { type: Date, required: true },
    lastSeenAt: { type: Date, default: Date.now },
    createdAt: { type: Date, default: Date.now },
  });
  const metalworksCrmPushDeviceSchema = new mongoose.Schema({
    adminEmail: { type: String, required: true, index: true },
    deviceToken: { type: String, required: true, unique: true, index: true },
    platform: { type: String, default: "ios" },
    bundleId: { type: String, default: METALWORKS_IOS_APP_BUNDLE_ID },
    appEnvironment: { type: String, default: "sandbox" },
    deviceName: String,
    appVersion: String,
    buildNumber: String,
    authorizationStatus: String,
    notificationsEnabled: { type: Boolean, default: false },
    isActive: { type: Boolean, default: true, index: true },
    lastSeenAt: { type: Date, default: Date.now, index: true },
    lastPushAt: Date,
    lastPushError: String,
    createdAt: { type: Date, default: Date.now },
    updatedAt: { type: Date, default: Date.now },
  });
  const metalworksWhatsAppFollowupSchema = new mongoose.Schema({
    leadId: {
      type: mongoose.Schema.Types.ObjectId,
      ref: "MetalworksLead",
      default: null,
      index: true,
    },
    phone: { type: String, index: true },
    phoneDisplay: String,
    visitorId: { type: String, index: true },
    sessionId: { type: String, index: true },
    step: { type: String, required: true, index: true },
    status: { type: String, default: "queued", index: true },
    dueAt: { type: Date, required: true, index: true },
    windowExpiresAt: { type: Date, required: true, index: true },
    lastInboundAt: { type: Date, required: true, index: true },
    serviceBucket: String,
    leadTemperature: String,
    language: String,
    messageBody: String,
    attempts: { type: Number, default: 0 },
    lockedAt: Date,
    sentAt: Date,
    canceledAt: Date,
    cancelReason: String,
    error: String,
    createdAt: { type: Date, default: Date.now },
    updatedAt: { type: Date, default: Date.now },
  });

  metalworksLeadSchema.index({ createdAt: -1 });
  metalworksLeadSchema.index({ updatedAt: -1 });
  metalworksLeadSchema.index({ status: 1, updatedAt: -1 });
  metalworksLeadSchema.index({ visitorIds: 1 });
  metalworksLeadSchema.index({ sessionIds: 1 });
  metalworksLeadActivitySchema.index({ leadId: 1, createdAt: -1 });
  metalworksLeadActivitySchema.index({ activityType: 1, createdAt: -1 });
  metalworksLeadAssetSchema.index({ leadId: 1, uploadedAt: -1 });
  metalworksLeadAssetSchema.index({ visitorId: 1, uploadedAt: -1 });
  metalworksLeadAssetSchema.index({ sessionId: 1, uploadedAt: -1 });
  metalworksCrmSessionSchema.index({ expiresAt: 1 }, { expireAfterSeconds: 0 });
  metalworksProspectorSessionSchema.index({ expiresAt: 1 }, { expireAfterSeconds: 0 });
  metalworksProspectorSessionSchema.index({ prospectorEmail: 1, lastSeenAt: -1 });
  metalworksCrmPushDeviceSchema.index({ adminEmail: 1, lastSeenAt: -1 });
  metalworksCrmPushDeviceSchema.index({ isActive: 1, lastSeenAt: -1 });
  metalworksWhatsAppFollowupSchema.index({ status: 1, dueAt: 1, createdAt: 1 });
  metalworksWhatsAppFollowupSchema.index({ leadId: 1, status: 1, dueAt: 1 });
  metalworksWhatsAppFollowupSchema.index({ leadId: 1, step: 1, lastInboundAt: 1 });

  const MetalworksLead =
    mongoose.models.MetalworksLead ||
    mongoose.model("MetalworksLead", metalworksLeadSchema);
  const MetalworksLeadActivity =
    mongoose.models.MetalworksLeadActivity ||
    mongoose.model("MetalworksLeadActivity", metalworksLeadActivitySchema);
  const MetalworksLeadAsset =
    mongoose.models.MetalworksLeadAsset ||
    mongoose.model("MetalworksLeadAsset", metalworksLeadAssetSchema);
  const MetalworksCrmSession =
    mongoose.models.MetalworksCrmSession ||
    mongoose.model("MetalworksCrmSession", metalworksCrmSessionSchema);
  const MetalworksProspectorSession =
    mongoose.models.MetalworksProspectorSession ||
    mongoose.model("MetalworksProspectorSession", metalworksProspectorSessionSchema);
  const MetalworksCrmPushDevice =
    mongoose.models.MetalworksCrmPushDevice ||
    mongoose.model("MetalworksCrmPushDevice", metalworksCrmPushDeviceSchema);
  const MetalworksWhatsAppFollowup =
    mongoose.models.MetalworksWhatsAppFollowup ||
    mongoose.model("MetalworksWhatsAppFollowup", metalworksWhatsAppFollowupSchema);
  let metalworksWhatsAppFollowupWorkerRunning = false;
  let metalworksWhatsAppFollowupWorkerInterval = null;
  let metalworksWhatsAppFollowupWakeTimer = null;

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

  function setProspectorSessionCookie(res, req, token) {
    res.cookie(METALWORKS_PROSPECTOR_SESSION_COOKIE, token, {
      httpOnly: true,
      sameSite: "lax",
      secure: requestIsSecure(req),
      path: "/",
      maxAge: METALWORKS_PROSPECTOR_SESSION_DAYS * 24 * 60 * 60 * 1000,
    });
  }

  function clearProspectorSessionCookie(res, req) {
    res.clearCookie(METALWORKS_PROSPECTOR_SESSION_COOKIE, {
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

  async function getProspectorAuth(req, { touch = true } = {}) {
    const cookies = parseCookies(req.headers.cookie || "");
    const token = String(cookies[METALWORKS_PROSPECTOR_SESSION_COOKIE] || "").trim();

    if (!token) {
      return { session: null, name: "", email: "" };
    }

    const session = await MetalworksProspectorSession.findOne({
      tokenHash: hashToken(token),
      expiresAt: { $gt: new Date() },
    }).lean();

    if (!session?.prospectorEmail || !session?.prospectorName) {
      return { session: null, name: "", email: "" };
    }

    if (touch) {
      await MetalworksProspectorSession.updateOne(
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
      name: cleanText(session.prospectorName || "", 120),
      email: normalizeEmail(session.prospectorEmail || ""),
    };
  }

  async function requireProspectorAuth(req, res) {
    if (!metalworksProspectorConfigured()) {
      respondError(
        res,
        503,
        "The prospector portal does not have a password configured yet.",
      );
      return null;
    }

    const auth = await getProspectorAuth(req);

    if (!auth.email || !auth.name) {
      respondError(res, 401, "You need to sign in as a prospector.");
      return null;
    }

    return auth;
  }

  async function createProspectorSession(req, res, { name = "", email = "" } = {}) {
    const token = generateToken();
    const expiresAt = new Date(
      Date.now() + METALWORKS_PROSPECTOR_SESSION_DAYS * 24 * 60 * 60 * 1000,
    );

    await MetalworksProspectorSession.create({
      prospectorName: cleanText(name || "", 120),
      prospectorEmail: normalizeEmail(email || ""),
      tokenHash: hashToken(token),
      ipAddress: getClientIp(req),
      userAgent: cleanText(req.headers["user-agent"] || "", 400),
      expiresAt,
      lastSeenAt: new Date(),
    });

    setProspectorSessionCookie(res, req, token);
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

  async function destroyProspectorSession(req, res) {
    const cookies = parseCookies(req.headers.cookie || "");
    const token = String(cookies[METALWORKS_PROSPECTOR_SESSION_COOKIE] || "").trim();

    if (token) {
      await MetalworksProspectorSession.deleteOne({
        tokenHash: hashToken(token),
      });
    }

    clearProspectorSessionCookie(res, req);
  }

  app.use("/api/metalworks-crm", (req, res, next) => {
    res.set("Cache-Control", "no-store");
    next();
  });

  async function sendMetalworksPushAlert({
    lead = null,
    alertType = "assistant_lead",
    requestedAtLabel = "",
    adminEmail = "",
  } = {}) {
    const query = {
      isActive: true,
      notificationsEnabled: true,
    };

    if (adminEmail) {
      query.adminEmail = normalizeEmail(adminEmail);
    }

    const deviceDocs = await MetalworksCrmPushDevice.find(query)
      .sort({ lastSeenAt: -1, updatedAt: -1 })
      .limit(adminEmail ? 8 : 24)
      .lean();

    if (!deviceDocs.length) {
      return {
        attempted: false,
        delivered: false,
        deliveredCount: 0,
        deviceCount: 0,
        error: "No iPhone devices are registered for push alerts yet.",
      };
    }

    const copy = buildMetalworksPushCopy({
      lead,
      alertType,
      requestedAtLabel,
    });
    const results = await Promise.all(
      deviceDocs.map(async (device) => {
        const result = await sendMetalworksApnsNotification({
          deviceToken: device.deviceToken,
          alertType,
          title: copy.title,
          body: copy.body,
          leadId: lead?._id ? String(lead._id) : "",
          appEnvironment: device.appEnvironment || "sandbox",
        });
        const update = {
          lastSeenAt: new Date(),
          updatedAt: new Date(),
          lastPushAt: result.delivered ? new Date() : device.lastPushAt || null,
          lastPushError: result.delivered ? "" : cleanText(result.error || "", 240),
        };

        if (METALWORKS_PUSH_INVALID_REASONS.has(result.reason || "")) {
          update.isActive = false;
          update.notificationsEnabled = false;
        }

        await MetalworksCrmPushDevice.updateOne(
          { _id: device._id },
          {
            $set: update,
          },
        );

        return result;
      }),
    );
    const deliveredCount = results.filter((item) => item.delivered).length;
    const firstError = results.find((item) => item.error)?.error || "";

    return {
      attempted: true,
      delivered: deliveredCount > 0,
      deliveredCount,
      deviceCount: deviceDocs.length,
      error: firstError,
    };
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
    createdAt = null,
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
      createdAt:
        createdAt instanceof Date && !Number.isNaN(createdAt.getTime()) ? createdAt : new Date(),
    });
  }

  function isMetalworksWhatsAppFollowupFinalLeadStatus(status = "") {
    return METALWORKS_WHATSAPP_CLOSED_LEAD_STATUSES.has(normalizeStatus(status || "new"));
  }

  function buildMetalworksWhatsAppConversationState(leadDoc = null) {
    if (!leadDoc) {
      return null;
    }

    const lead =
      leadDoc && typeof leadDoc.toObject === "function"
        ? leadDoc.toObject()
        : { ...(leadDoc || {}) };

    return {
      ...buildAssistantConversationSignals({
        history: Array.isArray(lead?.conversationHistory) ? lead.conversationHistory : [],
        lead,
        pagePath: cleanText(lead?.pagePath || "", 240) || "/whatsapp",
      }),
      sourceChannel: "whatsapp",
      sourceLabel: "Agustin 2.0 WhatsApp assistant",
    };
  }

  function shouldSkipMetalworksWhatsAppFollowups(leadDoc = null, conversationState = null) {
    if (!leadDoc?._id) {
      return {
        skip: true,
        reason: "missing_lead",
      };
    }

    if (isMetalworksWhatsAppFollowupFinalLeadStatus(leadDoc.status)) {
      return {
        skip: true,
        reason: `lead_${normalizeStatus(leadDoc.status || "new")}`,
      };
    }

    const state = conversationState || buildMetalworksWhatsAppConversationState(leadDoc);
    const phone = normalizePhone(leadDoc?.phone || state?.phone || "");
    const hasScheduledCallback =
      state?.nextActionAt instanceof Date &&
      !Number.isNaN(state.nextActionAt.getTime()) &&
      state?.callbackIntent === "yes" &&
      (!Array.isArray(state?.callbackMissingFields) || !state.callbackMissingFields.length);

    if (!phone) {
      return {
        skip: true,
        reason: "missing_phone",
      };
    }

    if (hasScheduledCallback) {
      return {
        skip: true,
        reason: "callback_already_booked",
      };
    }

    return {
      skip: false,
      reason: "",
    };
  }

  async function cancelMetalworksWhatsAppFollowupsForLead({
    leadId = null,
    reason = "",
    lastInboundAt = null,
    excludeFollowupId = null,
  } = {}) {
    if (!leadId) {
      return 0;
    }

    const query = {
      leadId,
      status: { $in: ["queued", "retrying"] },
    };

    if (lastInboundAt instanceof Date && !Number.isNaN(lastInboundAt.getTime())) {
      query.lastInboundAt = lastInboundAt;
    }

    if (excludeFollowupId) {
      query._id = { $ne: excludeFollowupId };
    }

    const result = await MetalworksWhatsAppFollowup.updateMany(query, {
      $set: {
        status: "canceled",
        cancelReason: cleanText(reason || "", 160),
        canceledAt: new Date(),
        lockedAt: null,
        updatedAt: new Date(),
      },
    });

    return Number(result?.modifiedCount || 0) || 0;
  }

  async function scheduleMetalworksWhatsAppFollowups({
    leadDoc = null,
    inboundAt = null,
    sourceChannel = "",
    tracking = {},
  } = {}) {
    if (!leadDoc?._id || cleanText(sourceChannel || "", 40) !== "whatsapp") {
      return {
        scheduled: 0,
        skipped: true,
        reason: "not_whatsapp",
      };
    }

    const safeInboundAt =
      inboundAt instanceof Date && !Number.isNaN(inboundAt.getTime()) ? inboundAt : new Date();
    const conversationState = buildMetalworksWhatsAppConversationState(leadDoc);
    const skipState = shouldSkipMetalworksWhatsAppFollowups(leadDoc, conversationState);

    await cancelMetalworksWhatsAppFollowupsForLead({
      leadId: leadDoc._id,
      reason: skipState.skip ? skipState.reason : "reset_by_new_inbound",
    });

    if (!metalworksWhatsAppFollowupsEnabled()) {
      return {
        scheduled: 0,
        skipped: true,
        reason: "disabled",
      };
    }

    if (skipState.skip) {
      return {
        scheduled: 0,
        skipped: true,
        reason: skipState.reason,
      };
    }

    const phone = normalizePhone(leadDoc.phone || conversationState?.phone || "");

    if (!phone) {
      return {
        scheduled: 0,
        skipped: true,
        reason: "missing_phone",
      };
    }

    const phoneDisplay = cleanText(
      leadDoc.phoneDisplay || conversationState?.phoneDisplay || phone,
      40,
    );
    const windowExpiresAt = new Date(safeInboundAt.getTime() + METALWORKS_WHATSAPP_WINDOW_MS);
    const docs = METALWORKS_WHATSAPP_FOLLOWUP_STEPS.map((stepConfig) => ({
      leadId: leadDoc._id,
      phone,
      phoneDisplay,
      visitorId: cleanText(
        (Array.isArray(leadDoc.visitorIds) ? leadDoc.visitorIds[0] : "") ||
          conversationState?.visitorId ||
          "",
        120,
      ),
      sessionId: cleanText(
        (Array.isArray(leadDoc.sessionIds) ? leadDoc.sessionIds[0] : "") ||
          conversationState?.sessionId ||
          "",
        120,
      ),
      step: stepConfig.step,
      status: "queued",
      dueAt: new Date(safeInboundAt.getTime() + stepConfig.delayMs),
      windowExpiresAt,
      lastInboundAt: safeInboundAt,
      serviceBucket: cleanText(conversationState?.serviceBucket || "", 40),
      leadTemperature: cleanText(conversationState?.leadTemperature || "", 20),
      language: conversationState?.inSpanish ? "es" : "en",
      attempts: 0,
      createdAt: new Date(),
      updatedAt: new Date(),
    }));

    if (!docs.length) {
      return {
        scheduled: 0,
        skipped: true,
        reason: "no_steps",
      };
    }

    await MetalworksWhatsAppFollowup.insertMany(docs, { ordered: true });
    await appendActivity({
      leadId: leadDoc._id,
      activityType: "assistant_whatsapp_followups_scheduled",
      body: `Queued ${docs.length} WhatsApp follow-ups inside the active reply window.`,
      meta: {
        count: docs.length,
        steps: docs.map((item) => item.step),
        lastInboundAt: safeInboundAt.toISOString(),
        windowExpiresAt: windowExpiresAt.toISOString(),
        sourceChannel: "whatsapp",
      },
      pagePath: cleanText(leadDoc.pagePath || "", 240) || "/whatsapp",
      pageUrl: cleanText(leadDoc.pageUrl || "", 500) || "whatsapp://twilio/inbound",
      tracking,
    });

    despertarMetalworksWhatsAppFollowupWorker(500);

    return {
      scheduled: docs.length,
      skipped: false,
      reason: "",
      windowExpiresAt,
    };
  }

  async function hasNewerWhatsAppInboundSince({ leadId = null, lastInboundAt = null } = {}) {
    if (!leadId || !(lastInboundAt instanceof Date) || Number.isNaN(lastInboundAt.getTime())) {
      return false;
    }

    const match = await MetalworksLeadActivity.exists({
      leadId,
      activityType: "assistant_user_message",
      createdAt: { $gt: lastInboundAt },
      "meta.sourceChannel": "whatsapp",
    });

    return Boolean(match);
  }

  async function tomarMetalworksWhatsAppFollowupPendiente() {
    const now = new Date();
    const staleLockAt = new Date(now.getTime() - METALWORKS_WHATSAPP_FOLLOWUP_LOCK_MS);

    return MetalworksWhatsAppFollowup.findOneAndUpdate(
      {
        $or: [
          {
            status: "queued",
            dueAt: { $lte: now },
          },
          {
            status: "retrying",
            dueAt: { $lte: now },
          },
          {
            status: "processing",
            lockedAt: { $lte: staleLockAt },
          },
        ],
      },
      {
        $set: {
          status: "processing",
          lockedAt: now,
          updatedAt: now,
        },
        $inc: {
          attempts: 1,
        },
      },
      {
        sort: {
          dueAt: 1,
          createdAt: 1,
        },
        new: true,
      },
    );
  }

  async function markMetalworksWhatsAppFollowupSent(
    followupDoc = null,
    { messageBody = "", error = "" } = {},
  ) {
    if (!followupDoc?._id) {
      return;
    }

    const now = new Date();
    await MetalworksWhatsAppFollowup.updateOne(
      { _id: followupDoc._id },
      {
        $set: {
          status: "sent",
          messageBody: cleanText(messageBody || "", 1500),
          error: cleanText(error || "", 240),
          sentAt: now,
          lockedAt: null,
          updatedAt: now,
        },
      },
    );
  }

  async function markMetalworksWhatsAppFollowupSkipped(
    followupDoc = null,
    reason = "",
  ) {
    if (!followupDoc?._id) {
      return;
    }

    const now = new Date();
    await MetalworksWhatsAppFollowup.updateOne(
      { _id: followupDoc._id },
      {
        $set: {
          status: "skipped",
          cancelReason: cleanText(reason || "", 160),
          canceledAt: now,
          lockedAt: null,
          updatedAt: now,
        },
      },
    );
  }

  async function markMetalworksWhatsAppFollowupFailed(
    followupDoc = null,
    error = "",
  ) {
    if (!followupDoc?._id) {
      return;
    }

    const now = new Date();
    const safeError = cleanText(error || "Unknown follow-up error", 240);
    const windowExpiresAt =
      followupDoc.windowExpiresAt instanceof Date &&
      !Number.isNaN(followupDoc.windowExpiresAt.getTime())
        ? followupDoc.windowExpiresAt
        : followupDoc.windowExpiresAt
          ? new Date(followupDoc.windowExpiresAt)
          : null;
    const retryAt = new Date(
      now.getTime() + Math.min(Math.max(Number(followupDoc.attempts || 1), 1), 3) * 5 * 60 * 1000,
    );
    const canRetry =
      Number(followupDoc.attempts || 0) < 3 &&
      windowExpiresAt instanceof Date &&
      !Number.isNaN(windowExpiresAt.getTime()) &&
      retryAt.getTime() < windowExpiresAt.getTime();

    await MetalworksWhatsAppFollowup.updateOne(
      { _id: followupDoc._id },
      {
        $set: {
          status: canRetry ? "retrying" : "failed",
          dueAt: canRetry ? retryAt : followupDoc.dueAt,
          error: safeError,
          lockedAt: null,
          updatedAt: now,
        },
      },
    );
  }

  function shouldLogMetalworksWhatsAppFollowupSkip(reason = "") {
    return !["newer_inbound_message", "stale_followup"].includes(cleanText(reason || "", 80));
  }

  async function procesarMetalworksWhatsAppFollowups() {
    if (
      metalworksWhatsAppFollowupWorkerRunning ||
      mongoose.connection.readyState !== 1 ||
      !metalworksWhatsAppFollowupsEnabled()
    ) {
      return;
    }

    if (typeof app.locals.sendMetalworksWhatsAppMessage !== "function") {
      return;
    }

    metalworksWhatsAppFollowupWorkerRunning = true;

    try {
      for (let index = 0; index < METALWORKS_WHATSAPP_FOLLOWUP_BATCH_SIZE; index += 1) {
        const followupDoc = await tomarMetalworksWhatsAppFollowupPendiente();

        if (!followupDoc?._id) {
          break;
        }

        try {
          const leadDoc = followupDoc.leadId
            ? await MetalworksLead.findById(followupDoc.leadId)
            : await resolveConversationLead({
                visitorId: followupDoc.visitorId,
                sessionId: followupDoc.sessionId,
                phone: followupDoc.phone,
              });
          const pagePath = cleanText(leadDoc?.pagePath || "", 240) || "/whatsapp";
          const pageUrl = cleanText(leadDoc?.pageUrl || "", 500) || "whatsapp://twilio/followup";
          const tracking = buildTrackingPayload(leadDoc?.tracking || {});

          if (!leadDoc?._id) {
            await markMetalworksWhatsAppFollowupSkipped(followupDoc, "missing_lead");
            continue;
          }

          const stepConfig = getMetalworksWhatsAppFollowupStepConfig(followupDoc.step);

          if (!stepConfig) {
            await markMetalworksWhatsAppFollowupSkipped(followupDoc, "invalid_step");
            continue;
          }

          const conversationState = buildMetalworksWhatsAppConversationState(leadDoc);
          const skipState = shouldSkipMetalworksWhatsAppFollowups(leadDoc, conversationState);

          if (skipState.skip) {
            await markMetalworksWhatsAppFollowupSkipped(followupDoc, skipState.reason);
            await cancelMetalworksWhatsAppFollowupsForLead({
              leadId: leadDoc._id,
              reason: skipState.reason,
            });

            if (shouldLogMetalworksWhatsAppFollowupSkip(skipState.reason)) {
              await appendActivity({
                leadId: leadDoc._id,
                activityType: "assistant_whatsapp_followup_skipped",
                body: `Skipped WhatsApp follow-up (${followupDoc.step}) because ${skipState.reason}.`,
                meta: {
                  reason: skipState.reason,
                  step: followupDoc.step,
                  sourceChannel: "whatsapp",
                },
                pagePath,
                pageUrl,
                tracking,
              });
            }

            continue;
          }

          const now = new Date();
          const windowExpiresAt =
            followupDoc.windowExpiresAt instanceof Date &&
            !Number.isNaN(followupDoc.windowExpiresAt.getTime())
              ? followupDoc.windowExpiresAt
              : new Date(followupDoc.windowExpiresAt);
          const lastInboundAt =
            followupDoc.lastInboundAt instanceof Date &&
            !Number.isNaN(followupDoc.lastInboundAt.getTime())
              ? followupDoc.lastInboundAt
              : new Date(followupDoc.lastInboundAt);
          const dueAt =
            followupDoc.dueAt instanceof Date && !Number.isNaN(followupDoc.dueAt.getTime())
              ? followupDoc.dueAt
              : new Date(followupDoc.dueAt);

          if (!(windowExpiresAt instanceof Date) || Number.isNaN(windowExpiresAt.getTime())) {
            await markMetalworksWhatsAppFollowupSkipped(followupDoc, "missing_window");
            continue;
          }

          if (now.getTime() >= windowExpiresAt.getTime()) {
            await markMetalworksWhatsAppFollowupSkipped(followupDoc, "window_expired");
            continue;
          }

          if (
            dueAt instanceof Date &&
            !Number.isNaN(dueAt.getTime()) &&
            stepConfig.maxLagMs > 0 &&
            now.getTime() - dueAt.getTime() > stepConfig.maxLagMs
          ) {
            await markMetalworksWhatsAppFollowupSkipped(followupDoc, "stale_followup");
            continue;
          }

          if (await hasNewerWhatsAppInboundSince({ leadId: leadDoc._id, lastInboundAt })) {
            await markMetalworksWhatsAppFollowupSkipped(followupDoc, "newer_inbound_message");
            await cancelMetalworksWhatsAppFollowupsForLead({
              leadId: leadDoc._id,
              reason: "newer_inbound_message",
              lastInboundAt,
              excludeFollowupId: followupDoc._id,
            });
            continue;
          }

          const followupResult = await generateAssistantFollowupReply({
            step: followupDoc.step,
            history: Array.isArray(leadDoc.conversationHistory) ? leadDoc.conversationHistory : [],
            pagePath,
            conversationState,
          });
          const reply = cleanText(followupResult?.reply || "", 1500);

          if (!reply) {
            await markMetalworksWhatsAppFollowupSkipped(followupDoc, "blank_reply");
            continue;
          }

          if (await hasNewerWhatsAppInboundSince({ leadId: leadDoc._id, lastInboundAt })) {
            await markMetalworksWhatsAppFollowupSkipped(followupDoc, "newer_inbound_message");
            await cancelMetalworksWhatsAppFollowupsForLead({
              leadId: leadDoc._id,
              reason: "newer_inbound_message",
              lastInboundAt,
              excludeFollowupId: followupDoc._id,
            });
            continue;
          }

          const sendResult = await app.locals.sendMetalworksWhatsAppMessage({
            to: leadDoc.phone || followupDoc.phone,
            body: reply,
          });

          if (!sendResult?.ok) {
            throw new Error(sendResult?.error || "Twilio WhatsApp send failed.");
          }

          const sentAt = new Date();
          leadDoc.conversationHistory = mergeConversationHistory(leadDoc.conversationHistory || [], [
            {
              role: "assistant",
              content: reply,
              createdAt: sentAt,
            },
          ]);
          leadDoc.lastAssistantMessage = reply;
          leadDoc.lastContactAt = sentAt;
          leadDoc.updatedAt = sentAt;
          await leadDoc.save();
          await markMetalworksWhatsAppFollowupSent(followupDoc, {
            messageBody: reply,
          });
          await appendActivity({
            leadId: leadDoc._id,
            activityType: "assistant_whatsapp_followup_sent",
            body: reply,
            meta: {
              step: followupDoc.step,
              twilioSid: cleanText(sendResult?.sid || "", 120),
              usedFallback: Boolean(followupResult?.usedFallback),
              reason: cleanText(followupResult?.reason || "", 240),
              sourceChannel: "whatsapp",
            },
            pagePath,
            pageUrl,
            tracking,
            createdAt: sentAt,
          });
        } catch (error) {
          console.error(
            "Error sending Metal Works WhatsApp follow-up:",
            followupDoc?.step,
            error.message,
          );
          await markMetalworksWhatsAppFollowupFailed(
            followupDoc,
            error?.message || "WhatsApp follow-up failed.",
          );
        }
      }
    } finally {
      metalworksWhatsAppFollowupWorkerRunning = false;
    }
  }

  function despertarMetalworksWhatsAppFollowupWorker(delayMs = 200) {
    if (metalworksWhatsAppFollowupWakeTimer) {
      return;
    }

    metalworksWhatsAppFollowupWakeTimer = setTimeout(() => {
      metalworksWhatsAppFollowupWakeTimer = null;
      procesarMetalworksWhatsAppFollowups().catch((error) => {
        console.error("Error waking Metal Works WhatsApp follow-up worker:", error.message);
      });
    }, delayMs);
  }

  function iniciarMetalworksWhatsAppFollowupWorker() {
    if (metalworksWhatsAppFollowupWorkerInterval) {
      return;
    }

    metalworksWhatsAppFollowupWorkerInterval = setInterval(() => {
      procesarMetalworksWhatsAppFollowups().catch((error) => {
        console.error("Error in Metal Works WhatsApp follow-up worker:", error.message);
      });
    }, METALWORKS_WHATSAPP_FOLLOWUP_POLL_MS);

    despertarMetalworksWhatsAppFollowupWorker(500);
  }

  async function syncLeadAssetsToLead({
    leadId = null,
    visitorIds = [],
    sessionIds = [],
  } = {}) {
    if (!leadId) {
      return 0;
    }

    const conditions = [];
    const safeVisitorIds = mergeAssistantUniqueValues(visitorIds || []);
    const safeSessionIds = mergeAssistantUniqueValues(sessionIds || []);

    if (safeVisitorIds.length) {
      conditions.push({ visitorId: { $in: safeVisitorIds } });
    }

    if (safeSessionIds.length) {
      conditions.push({ sessionId: { $in: safeSessionIds } });
    }

    if (!conditions.length) {
      return 0;
    }

    const result = await MetalworksLeadAsset.updateMany(
      {
        $or: conditions,
        leadId: { $ne: leadId },
      },
      {
        $set: {
          leadId,
          updatedAt: new Date(),
        },
      },
    );

    return Number(result?.modifiedCount || 0) || 0;
  }

  async function listLeadAssets(leadId = null) {
    if (!leadId) {
      return [];
    }

    const docs = await MetalworksLeadAsset.find({ leadId })
      .sort({ uploadedAt: -1, createdAt: -1 })
      .limit(24)
      .lean();

    return docs.map(cleanLeadAsset).filter(Boolean);
  }

  async function resolveConversationLead({
    visitorId = "",
    sessionId = "",
    email = "",
    phone = "",
    leadId = "",
  } = {}) {
    const safeLeadId = cleanText(leadId || "", 80);

    if (safeLeadId) {
      const leadById = await MetalworksLead.findById(safeLeadId);

      if (leadById?._id) {
        return leadById;
      }
    }

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

  function buildAssistantFastCacheKeys({
    visitorId = "",
    sessionId = "",
    sourceChannel = "web",
  } = {}) {
    const safeVisitorId = cleanText(visitorId || "", 120);
    const safeSessionId = cleanText(sessionId || "", 120);
    const safeSourceChannel =
      cleanText(sourceChannel || "web", 40).trim().toLowerCase() || "web";
    const keys = [];

    if (safeSessionId) {
      keys.push(`metalworks:assistant:session:${safeSourceChannel}:${safeSessionId}`);
    }

    if (safeVisitorId) {
      keys.push(`metalworks:assistant:visitor:${safeSourceChannel}:${safeVisitorId}`);
    }

    return Array.from(new Set(keys));
  }

  function cleanAssistantFastCacheLeadSeed(value = null) {
    const source =
      value && typeof value.toObject === "function"
        ? value.toObject()
        : value
          ? { ...value }
          : {};
    const fullName = sanitizeAssistantStoredName(
      cleanText(source.fullName || source.name || "", 120),
    );
    const phone = normalizePhone(source.phone || "");
    const phoneDisplay =
      cleanText(source.phoneDisplay || source.phone || "", 40) || phone;
    const email = normalizeEmail(source.email || "");
    const projectType = cleanText(source.projectType || "", 120);
    const location = cleanText(source.location || "", 160);
    const details = cleanText(source.details || source.detailsSummary || "", 1500);
    const bestContactDay = cleanText(source.bestContactDay || "", 80);
    const bestContactTime = cleanText(source.bestContactTime || "", 80);
    const callbackIntent = cleanText(source.callbackIntent || "", 12);
    const lastUserMessage = cleanText(source.lastUserMessage || source.latestUserMessage || "", 500);
    const lastAssistantMessage = cleanText(
      source.lastAssistantMessage || source.latestAssistantMessage || "",
      1500,
    );
    const photoFileNames = mergeAssistantUniqueValues(source.photoFileNames || []).slice(
      -METALWORKS_ASSISTANT_HISTORY_LIMIT,
    );
    const nextActionAt =
      source.nextActionAt instanceof Date
        ? source.nextActionAt.toISOString()
        : source.nextActionAt
          ? new Date(source.nextActionAt).toISOString()
          : "";

    if (
      !fullName &&
      !phone &&
      !email &&
      !projectType &&
      !location &&
      !details &&
      !bestContactDay &&
      !bestContactTime &&
      !callbackIntent &&
      !photoFileNames.length &&
      !lastUserMessage &&
      !lastAssistantMessage &&
      !nextActionAt
    ) {
      return null;
    }

    return {
      fullName,
      phone,
      phoneDisplay,
      email,
      projectType,
      location,
      details,
      bestContactDay,
      bestContactTime,
      callbackIntent,
      photoFileNames,
      lastUserMessage,
      lastAssistantMessage,
      nextActionAt,
    };
  }

  function cleanAssistantFastCacheSnapshot(snapshot = null) {
    if (!snapshot || typeof snapshot !== "object") {
      return null;
    }

    const sourceChannel =
      cleanText(snapshot.sourceChannel || "web", 40).trim().toLowerCase() || "web";
    const sourceLabel = cleanText(snapshot.sourceLabel || "", 120);
    const visitorId = cleanText(snapshot.visitorId || "", 120);
    const sessionId = cleanText(snapshot.sessionId || "", 120);
    const leadId = cleanText(snapshot.leadId || "", 80);
    const history = normalizeStoredConversationHistory(snapshot.history || []);
    const leadSeed = cleanAssistantFastCacheLeadSeed(snapshot.leadSeed || null);
    const updatedAt =
      snapshot.updatedAt instanceof Date
        ? snapshot.updatedAt.toISOString()
        : snapshot.updatedAt
          ? new Date(snapshot.updatedAt).toISOString()
          : new Date().toISOString();

    if (!visitorId && !sessionId) {
      return null;
    }

    if (!leadId && !leadSeed && !history.length) {
      return null;
    }

    return {
      sourceChannel,
      sourceLabel,
      visitorId,
      sessionId,
      leadId,
      leadSeed,
      history,
      updatedAt,
    };
  }

  async function readAssistantFastCacheSnapshot({
    visitorId = "",
    sessionId = "",
    sourceChannel = "web",
  } = {}) {
    if (typeof redisGetJson !== "function") {
      return null;
    }

    const keys = buildAssistantFastCacheKeys({
      visitorId,
      sessionId,
      sourceChannel,
    });

    for (const key of keys) {
      const payload = await redisGetJson(key);
      const snapshot = cleanAssistantFastCacheSnapshot(payload);

      if (snapshot) {
        return snapshot;
      }
    }

    return null;
  }

  async function writeAssistantFastCacheSnapshot(snapshot = null) {
    if (typeof redisSetJson !== "function") {
      return false;
    }

    const safeSnapshot = cleanAssistantFastCacheSnapshot(snapshot);

    if (!safeSnapshot) {
      return false;
    }

    const keys = buildAssistantFastCacheKeys(safeSnapshot);

    if (!keys.length) {
      return false;
    }

    const results = await Promise.all(
      keys.map((key) =>
        redisSetJson(key, safeSnapshot, assistantFastCacheTtlSeconds),
      ),
    );

    return results.some(Boolean);
  }

  async function deleteAssistantFastCacheSnapshot({
    visitorId = "",
    sessionId = "",
    sourceChannel = "web",
  } = {}) {
    if (typeof redisDelete !== "function") {
      return false;
    }

    const keys = buildAssistantFastCacheKeys({
      visitorId,
      sessionId,
      sourceChannel,
    });

    if (!keys.length) {
      return false;
    }

    const results = await Promise.all(keys.map((key) => redisDelete(key)));
    return results.some(Boolean);
  }

  async function persistAssistantFastCache({
    visitorId = "",
    sessionId = "",
    sourceChannel = "web",
    sourceLabel = "",
    leadDoc = null,
    state = null,
    history = [],
  } = {}) {
    const leadId = leadDoc?._id ? String(leadDoc._id) : "";
    const seedSource = {
      ...(leadDoc && typeof leadDoc.toObject === "function"
        ? leadDoc.toObject()
        : leadDoc
          ? { ...leadDoc }
          : {}),
      ...(state || {}),
      photoFileNames: Array.isArray(leadDoc?.photoFileNames)
        ? leadDoc.photoFileNames
        : state?.photoFileNames || [],
    };

    return writeAssistantFastCacheSnapshot({
      visitorId,
      sessionId,
      sourceChannel,
      sourceLabel,
      leadId,
      leadSeed: seedSource,
      history,
      updatedAt: new Date(),
    });
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
    sourceType = "",
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
        ? "scheduled follow-up from assistant chat"
        : currentLead?.nextAction || "";
    leadDoc.nextActionAt = state?.nextActionAt || currentLead?.nextActionAt || null;
    leadDoc.privateNotes = mergeAssistantPrivateNotes(currentLead?.privateNotes || "", state);
    leadDoc.sourceType =
      cleanText(sourceType || currentLead?.sourceType || "", 80) || "assistant_chat";
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
    await syncLeadAssetsToLead({
      leadId: leadDoc._id,
      visitorIds: leadDoc.visitorIds || [],
      sessionIds: leadDoc.sessionIds || [],
    });
    return leadDoc;
  }

  function buildAssistantHintSeedLead(
    lead = null,
    { nameHint = "", phoneHint = "", phoneDisplayHint = "" } = {},
  ) {
    const baseLead =
      lead && typeof lead.toObject === "function"
        ? lead.toObject()
        : lead
          ? { ...lead }
          : {};
    const safeName = sanitizeAssistantStoredName(nameHint || "");
    const safePhone = normalizePhone(phoneHint || "");
    const safePhoneDisplay =
      cleanText(phoneDisplayHint || phoneHint || "", 40) || safePhone;
    const existingName = sanitizeAssistantStoredName(baseLead.fullName || "");
    const existingPhone = normalizePhone(baseLead.phone || "");

    if (
      safeName &&
      (!existingName || existingName === METALWORKS_ASSISTANT_PLACEHOLDER_NAME)
    ) {
      baseLead.fullName = safeName;
    }

    if (safePhone && !existingPhone) {
      baseLead.phone = safePhone;
    }

    if (safePhoneDisplay && (!cleanText(baseLead.phoneDisplay || "", 40) || !existingPhone)) {
      baseLead.phoneDisplay = safePhoneDisplay;
    }

    return baseLead;
  }

  async function buildAssistantConversationContext({
    history = [],
    message = "",
    pagePath = "",
    visitorId = "",
    sessionId = "",
    nameHint = "",
    phoneHint = "",
    phoneDisplayHint = "",
    allowEmployment = true,
    sourceChannel = "web",
  } = {}) {
    const cachedSnapshot = await readAssistantFastCacheSnapshot({
      visitorId,
      sessionId,
      sourceChannel,
    });
    const cachedLeadSeed = cleanAssistantFastCacheLeadSeed(cachedSnapshot?.leadSeed || null);
    const cachedHistory = normalizeStoredConversationHistory(cachedSnapshot?.history || []);
    let currentLead = await resolveConversationLead({
      visitorId,
      sessionId,
      phone: normalizePhone(phoneHint || ""),
      leadId: cachedSnapshot?.leadId || "",
    });
    let seededLead = buildAssistantHintSeedLead(currentLead || cachedLeadSeed, {
      nameHint,
      phoneHint,
      phoneDisplayHint,
    });
    let mergedHistory = mergeConversationHistory(
      currentLead?.conversationHistory || cachedHistory,
      normalizeAssistantHistory(history),
    );
    let userConversationItems = buildAssistantConversationItems({
      history: mergedHistory,
      message,
    });
    let conversationState = buildAssistantConversationSignals({
      history: userConversationItems,
      lead: seededLead,
      pagePath,
    });
    const resolvedLead = await resolveConversationLead({
      visitorId,
      sessionId,
      email: conversationState.email,
      phone: conversationState.phone || normalizePhone(phoneHint || ""),
      leadId: currentLead?._id ? String(currentLead._id) : cachedSnapshot?.leadId || "",
    });

    if (
      resolvedLead?._id &&
      String(resolvedLead._id) !== String(currentLead?._id || "")
    ) {
      currentLead = resolvedLead;
      seededLead = buildAssistantHintSeedLead(currentLead || cachedLeadSeed, {
        nameHint,
        phoneHint,
        phoneDisplayHint,
      });
      mergedHistory = mergeConversationHistory(
        currentLead?.conversationHistory || cachedHistory,
        normalizeAssistantHistory(history),
      );
      userConversationItems = buildAssistantConversationItems({
        history: mergedHistory,
        message,
      });
      conversationState = buildAssistantConversationSignals({
        history: userConversationItems,
        lead: seededLead,
        pagePath,
      });
    } else if (resolvedLead?._id) {
      currentLead = resolvedLead;
    }

    return {
      currentLead,
      userConversationItems,
      conversationState,
    };
  }

  async function processMetalworksAssistantMessage({
    message = "",
    visitorId = "",
    sessionId = "",
    pageTitle = "",
    pagePath = "",
    pageUrl = "",
    referrer = "",
    tracking = {},
    history = [],
    req = null,
    nameHint = "",
    phoneHint = "",
    phoneDisplayHint = "",
    sourceType = "assistant_chat",
    sourceChannel = "web",
    sourceLabel = "Agustin 2.0 website assistant",
    allowEmployment = true,
  } = {}) {
    const safeMessage = cleanText(message || "", 500);
    const safeVisitorId = cleanText(visitorId || "", 120);
    const safeSessionId = cleanText(sessionId || "", 120);
    const safePageTitle = cleanText(pageTitle || "", 160);
    const safePagePath = cleanText(pagePath || "", 240);
    const safePageUrl = cleanText(pageUrl || "", 500);
    const safeReferrer = cleanText(referrer || "", 500);
    const safeTracking = buildTrackingPayload(tracking || {});
    const inboundReceivedAt = new Date();

    if (!safeMessage) {
      return {
        ok: false,
        status: 400,
        error: "Message is required.",
      };
    }

    const startOfDay = new Date();
    startOfDay.setHours(0, 0, 0, 0);

    const usedToday = safeVisitorId
      ? await MetalworksLeadActivity.countDocuments({
          activityType: "assistant_user_message",
          createdAt: { $gte: startOfDay },
          "meta.visitorId": safeVisitorId,
        })
      : 0;

    if (safeVisitorId && usedToday >= METALWORKS_ASSISTANT_MAX_MESSAGES_PER_DAY) {
      return {
        ok: false,
        status: 429,
        error: `You reached the daily limit of ${METALWORKS_ASSISTANT_MAX_MESSAGES_PER_DAY} assistant messages for today. Please call 773 798 4107 or try again tomorrow.`,
      };
    }

    const combinedConversationText = [
      safeMessage,
      ...normalizeAssistantHistory(history).map((item) => item.content || ""),
    ]
      .filter(Boolean)
      .join("\n");

    if (
      !allowEmployment &&
      !detectEmploymentCorrection(combinedConversationText) &&
      detectEmploymentIntent(combinedConversationText)
    ) {
      return {
        ok: true,
        status: 200,
        respuesta: buildAssistantHiringDisabledReply(safeMessage),
        usedFallback: false,
        leadCaptured: false,
        leadId: "",
        applicantCaptured: false,
        applicantId: "",
        callbackCaptured: false,
        callbackLabel: "",
        notified: false,
        remainingToday: safeVisitorId
          ? Math.max(METALWORKS_ASSISTANT_MAX_MESSAGES_PER_DAY - usedToday, 0)
          : METALWORKS_ASSISTANT_MAX_MESSAGES_PER_DAY,
      };
    }

    const {
      currentLead,
      userConversationItems,
      conversationState: initialConversationState,
    } = await buildAssistantConversationContext({
      history,
      message: safeMessage,
      pagePath: safePagePath,
      visitorId: safeVisitorId,
      sessionId: safeSessionId,
      nameHint,
      phoneHint,
      phoneDisplayHint,
      allowEmployment,
      sourceChannel,
    });

    let conversationState = {
      ...initialConversationState,
      visitorId: safeVisitorId,
      sessionId: safeSessionId,
      sourceChannel: cleanText(sourceChannel || "web", 40) || "web",
      sourceLabel: cleanText(sourceLabel || "", 120) || "Agustin 2.0 website assistant",
    };

    const leadExistedBeforeMessage = Boolean(currentLead?._id);
    const previousLeadStatus = normalizeStatus(currentLead?.status || "new");
    const previousLeadNextActionAt = currentLead?.nextActionAt
      ? new Date(currentLead.nextActionAt).toISOString()
      : "";
    let leadDoc = await upsertConversationLead({
      currentLead,
      state: conversationState,
      pageTitle: safePageTitle,
      pagePath: safePagePath,
      pageUrl: safePageUrl,
      referrer: safeReferrer,
      tracking: safeTracking,
      req,
      sourceType,
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
          sourceType,
          sourceChannel: conversationState.sourceChannel,
          projectType: leadDoc.projectType || "",
          location: leadDoc.location || "",
          visitorId: safeVisitorId,
          sessionId: safeSessionId,
          pageTitle: safePageTitle,
        },
        req,
        pagePath: safePagePath,
        pageUrl: safePageUrl,
        tracking: safeTracking,
      });
    }

    await appendActivity({
      leadId: leadDoc?._id || currentLead?._id || null,
      activityType: "assistant_user_message",
      title: "Mensaje al assistant",
      body: safeMessage,
      meta: {
        visitorId: safeVisitorId,
        sessionId: safeSessionId,
        pageTitle: safePageTitle,
        sourceType,
        sourceChannel: conversationState.sourceChannel,
      },
      req,
      pagePath: safePagePath,
      pageUrl: safePageUrl,
      tracking: safeTracking,
      createdAt: inboundReceivedAt,
    });

    let visionAssets = [];

    try {
      visionAssets = await loadRecentLeadAssetsForVision({
        leadId: leadDoc?._id || currentLead?._id || null,
      });
    } catch (error) {
      console.error("Error loading Metal Works assistant vision photos:", error.message);
    }

    const replyConversationState = {
      ...conversationState,
      visionImageCount: Array.isArray(visionAssets) ? visionAssets.length : 0,
    };

    const result = await generateAssistantReply({
      message: safeMessage,
      history: userConversationItems,
      pagePath: safePagePath,
      conversationState: replyConversationState,
      visionAssets,
    });

    const conversationItemsWithReply = buildAssistantConversationItems({
      history: userConversationItems,
      reply: result.reply,
    });
    const finalState = {
      ...buildAssistantConversationSignals({
        history: conversationItemsWithReply,
        lead: buildAssistantHintSeedLead(leadDoc || currentLead, {
          nameHint,
          phoneHint,
          phoneDisplayHint,
        }),
        pagePath: safePagePath,
      }),
      visitorId: safeVisitorId,
      sessionId: safeSessionId,
      sourceChannel: conversationState.sourceChannel,
      sourceLabel: conversationState.sourceLabel,
    };

    leadDoc = await upsertConversationLead({
      currentLead: leadDoc || currentLead,
      state: finalState,
      pageTitle: safePageTitle,
      pagePath: safePagePath,
      pageUrl: safePageUrl,
      referrer: safeReferrer,
      tracking: safeTracking,
      req,
      assistantReply: result.reply,
      sourceType,
    });

    await persistAssistantFastCache({
      visitorId: safeVisitorId,
      sessionId: safeSessionId,
      sourceChannel: finalState.sourceChannel,
      sourceLabel: finalState.sourceLabel,
      leadDoc,
      state: finalState,
      history: conversationItemsWithReply,
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
          visitorId: safeVisitorId,
          sessionId: safeSessionId,
          pageTitle: safePageTitle,
          sourceType,
          sourceChannel: finalState.sourceChannel,
        },
        req,
        pagePath: safePagePath,
        pageUrl: safePageUrl,
        tracking: safeTracking,
      });
    }

    let alertDelivery = {
      attempted: false,
      delivered: false,
    };
    let pushDelivery = {
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
          pagePath: safePagePath,
          pageUrl: safePageUrl,
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

      try {
        pushDelivery = await sendMetalworksPushAlert({
          lead: leadDoc.toObject ? leadDoc.toObject() : leadDoc,
          alertType:
            finalState.callbackIntent === "yes" ? "assistant_callback" : "assistant_lead",
          requestedAtLabel: finalState.callbackLabel,
        });

        if (pushDelivery.delivered && !leadDoc.callbackAlertedAt) {
          leadDoc.callbackAlertedAt = new Date();
          leadDoc.updatedAt = new Date();
          await leadDoc.save();
        }
      } catch (error) {
        console.error("Error sending Metal Works assistant push:", error.message);
      }
    }

    await appendActivity({
      leadId: leadDoc?._id || currentLead?._id || null,
      activityType: result.usedFallback ? "assistant_fallback" : "assistant_ai_reply",
      title: result.usedFallback ? "Fallback del assistant" : "Respuesta del assistant",
      body: result.reply,
      meta: {
        visitorId: safeVisitorId,
        sessionId: safeSessionId,
        reason: result.reason || "",
        sourceType,
        sourceChannel: finalState.sourceChannel,
      },
      req,
      pagePath: safePagePath,
      pageUrl: safePageUrl,
      tracking: safeTracking,
    });

    if (leadDoc?._id && finalState.sourceChannel === "whatsapp") {
      try {
        await scheduleMetalworksWhatsAppFollowups({
          leadDoc,
          inboundAt: inboundReceivedAt,
          sourceChannel: finalState.sourceChannel,
          tracking: safeTracking,
        });
      } catch (error) {
        console.error("Error scheduling Metal Works WhatsApp follow-ups:", error.message);
      }
    }

    return {
      ok: true,
      status: 200,
      respuesta: result.reply,
      usedFallback: result.usedFallback,
      leadCaptured: Boolean(leadDoc?._id),
      leadId: leadDoc?._id ? String(leadDoc._id) : "",
      callbackCaptured: Boolean(
        leadDoc?._id && finalState.callbackIntent === "yes" && finalState.nextActionAt,
      ),
      callbackLabel: finalState.callbackLabel || "",
      notified: Boolean(alertDelivery.delivered || pushDelivery.delivered),
      remainingToday: safeVisitorId
        ? Math.max(METALWORKS_ASSISTANT_MAX_MESSAGES_PER_DAY - (usedToday + 1), 0)
        : METALWORKS_ASSISTANT_MAX_MESSAGES_PER_DAY,
    };
  }

  app.locals.processMetalworksAssistantMessage = processMetalworksAssistantMessage;
  app.locals.startMetalworksWhatsAppFollowupWorker = iniciarMetalworksWhatsAppFollowupWorker;

  app.get(
    [
      "/integrations/thumbtack/oauth/callback",
      "/api/integrations/thumbtack/oauth/callback",
    ],
    (req, res) => {
      const code = cleanText(req.query?.code || "", 240);
      const state = cleanText(req.query?.state || "", 240);
      const error = cleanText(req.query?.error || "", 160);
      const errorDescription = cleanText(req.query?.error_description || "", 600);

      res.setHeader("Cache-Control", "no-store");
      res.type("html").send(
        buildThumbtackOauthCallbackPage({
          code,
          state,
          error,
          errorDescription,
        }),
      );
    },
  );

  app.use("/api/metalworks-crm", (req, res, next) => {
    res.set("Cache-Control", "no-store");
    next();
  });

  app.get(
    ["/metalworks-crm/prospector/login", "/metalworks-crm/prospector/login/"],
    async (req, res) => {
      res.set("Cache-Control", "no-store");
      const auth = await getProspectorAuth(req, { touch: false });

      if (auth.email) {
        return res.redirect("/metalworks-crm/prospector/");
      }

      res.sendFile(path.join(publicDir, "metalworks-crm", "prospector-login.html"));
    },
  );

  app.get(["/metalworks-crm/prospector", "/metalworks-crm/prospector/"], async (req, res) => {
    res.set("Cache-Control", "no-store");
    const auth = await getProspectorAuth(req, { touch: false });

    if (!auth.email) {
      return res.redirect("/metalworks-crm/prospector/login/");
    }

    res.sendFile(path.join(privateDir, "metalworks-prospector.html"));
  });

  app.get("/api/metalworks-crm/prospector/me", async (req, res) => {
    try {
      const auth = await getProspectorAuth(req, { touch: true });
      res.json({
        authenticated: Boolean(auth.email),
        configured: metalworksProspectorConfigured(),
        name: auth.name || "",
        email: auth.email || "",
      });
    } catch (error) {
      console.error("Error loading Metal Works prospector auth:", error.message);
      respondError(res, 500, "I could not verify the prospector session.");
    }
  });

  app.post("/api/metalworks-crm/prospector/login", async (req, res) => {
    const name = cleanText(req.body?.name || "", 120);
    const email = normalizeEmail(req.body?.email || "");
    const password = String(req.body?.password || "");
    const expectedPassword = getMetalworksProspectorPassword();

    if (!metalworksProspectorConfigured()) {
      return respondError(
        res,
        503,
        "Configure METALWORKS_PROSPECTOR_PASSWORD on the backend first.",
      );
    }

    if (!name || !email || !password) {
      return respondError(res, 400, "Name, email, and password are required.");
    }

    if (!compareSecrets(password, expectedPassword)) {
      return respondError(res, 401, "Incorrect password.");
    }

    try {
      await createProspectorSession(req, res, { name, email });
      res.json({ ok: true, name, email });
    } catch (error) {
      console.error("Error logging into Metal Works prospector portal:", error.message);
      respondError(res, 500, "I could not sign you into the prospector portal.");
    }
  });

  app.post("/api/metalworks-crm/prospector/logout", async (req, res) => {
    try {
      await destroyProspectorSession(req, res);
      res.json({ ok: true });
    } catch (error) {
      console.error("Error logging out of Metal Works prospector portal:", error.message);
      respondError(res, 500, "I could not sign you out of the prospector portal.");
    }
  });

  app.get("/api/metalworks-crm/prospector/dashboard", async (req, res) => {
    const auth = await requireProspectorAuth(req, res);

    if (!auth) {
      return;
    }

    try {
      const snapshot = await buildProspectorDashboardSnapshot(
        MetalworksLead,
        auth.email,
      );
      res.json({
        ...snapshot,
        prospector: {
          name: auth.name,
          email: auth.email,
        },
      });
    } catch (error) {
      console.error("Error loading Metal Works prospector dashboard:", error.message);
      respondError(res, 500, "I could not load the prospector dashboard.");
    }
  });

  app.post("/api/metalworks-crm/prospector/leads", async (req, res) => {
    const auth = await requireProspectorAuth(req, res);

    if (!auth) {
      return;
    }

    try {
      const fullName = cleanText(req.body?.fullName || "", 120);
      const phoneDisplay = cleanText(req.body?.phone || "", 40);
      const phone = normalizePhone(phoneDisplay);
      const email = normalizeEmail(req.body?.email || "");
      const projectType = cleanText(req.body?.projectType || "", 120);
      const addressLine = cleanText(req.body?.addressLine || "", 160);
      const zipCode = cleanText(req.body?.zipCode || "", 20);
      const city = cleanText(req.body?.city || "", 120);
      const propertyType = cleanText(req.body?.propertyType || "", 80);
      const projectSize = cleanText(req.body?.projectSize || "", 120);
      const timeline = cleanText(req.body?.timeline || "", 80);
      const ownershipStatus = cleanText(req.body?.ownershipStatus || "", 80);
      const budgetRange = cleanText(req.body?.budgetRange || "", 80);
      const urgency = cleanText(req.body?.urgency || "", 40);
      const bestContactWindow = cleanText(req.body?.bestContactWindow || "", 120);
      const preferredLanguage = cleanText(req.body?.preferredLanguage || "", 80);
      const qualificationTier = cleanText(req.body?.qualificationTier || "", 12).toUpperCase();
      const qualificationNotes = cleanText(req.body?.qualificationNotes || "", 1200);
      const details = cleanText(req.body?.details || req.body?.notes || "", 3000);
      const location = cleanText(
        [addressLine, city, zipCode].filter(Boolean).join(", "),
        160,
      );
      const parsedFiles = Array.isArray(req.body?.photos)
        ? req.body.photos
            .map((item) => parseAssistantLeadAssetUpload(item))
            .filter(Boolean)
            .slice(0, 8)
        : [];
      const totalSizeBytes = parsedFiles.reduce(
        (sum, item) => sum + Number(item?.sizeBytes || 0),
        0,
      );
      const photoFileNames = mergeAssistantUniqueValues(
        ...(Array.isArray(req.body?.photoFileNames)
          ? req.body.photoFileNames
              .map((item) => cleanText(item, 120))
              .filter(Boolean)
          : []),
        ...parsedFiles.map((item) => item.fileName || ""),
      ).slice(0, 20);

      if (
        !fullName ||
        !phone ||
        !projectType ||
        !addressLine ||
        !zipCode ||
        !timeline ||
        !ownershipStatus ||
        !details
      ) {
        return respondError(
          res,
          400,
          "Fill in the customer name, phone, service, address, ZIP, timeline, owner status, and notes.",
        );
      }

      if (!qualificationTier || !qualificationNotes) {
        return respondError(res, 400, "Add the lead tier and a short qualification note.");
      }

      if (parsedFiles.length > 8) {
        return respondError(res, 400, "You can upload up to 8 photos per lead.");
      }

      if (totalSizeBytes > METALWORKS_LEAD_ASSET_MAX_TOTAL_BYTES) {
        return respondError(
          res,
          400,
          "The total photo upload is too large. Compress them or send fewer files.",
        );
      }

      const now = new Date();
      const fourteenDaysAgo = new Date(Date.now() - 14 * 24 * 60 * 60 * 1000);
      let leadDoc = await MetalworksLead.findOne({
        createdAt: { $gte: fourteenDaysAgo },
        phone,
        zipCode,
      }).sort({ createdAt: -1 });
      const duplicate = Boolean(leadDoc);

      if (leadDoc) {
        leadDoc.fullName = fullName;
        leadDoc.phone = phone;
        leadDoc.phoneDisplay = phoneDisplay;
        leadDoc.email = email;
        leadDoc.projectType = projectType;
        leadDoc.location = location;
        leadDoc.addressLine = addressLine;
        leadDoc.zipCode = zipCode;
        leadDoc.city = city;
        leadDoc.propertyType = propertyType;
        leadDoc.projectSize = projectSize;
        leadDoc.timeline = timeline;
        leadDoc.ownershipStatus = ownershipStatus;
        leadDoc.budgetRange = budgetRange;
        leadDoc.urgency = urgency;
        leadDoc.bestContactWindow = bestContactWindow;
        leadDoc.preferredLanguage = preferredLanguage;
        leadDoc.qualificationTier = qualificationTier;
        leadDoc.qualificationNotes = qualificationNotes;
        leadDoc.sourceProspectorName = auth.name;
        leadDoc.sourceProspectorEmail = auth.email;
        leadDoc.details = details;
        leadDoc.photoFileNames = photoFileNames;
        leadDoc.sourceType = "field_prospector";
        leadDoc.pageTitle = "Metal Works Prospector Intake";
        leadDoc.pagePath = "/metalworks-crm/prospector/";
        leadDoc.pageUrl = `${METALWORKS_WEBSITE_URL.replace(/\/$/, "")}/metalworks-crm/prospector/`;
        leadDoc.referrer = leadDoc.pageUrl;
        leadDoc.ipAddress = cleanText(getClientIp(req), 120);
        leadDoc.userAgent = cleanText(req.headers["user-agent"] || "", 400);
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
          addressLine,
          zipCode,
          city,
          propertyType,
          projectSize,
          timeline,
          ownershipStatus,
          budgetRange,
          urgency,
          bestContactWindow,
          preferredLanguage,
          qualificationTier,
          qualificationNotes,
          sourceProspectorName: auth.name,
          sourceProspectorEmail: auth.email,
          details,
          photoFileNames,
          status: "new",
          sourceType: "field_prospector",
          pageTitle: "Metal Works Prospector Intake",
          pagePath: "/metalworks-crm/prospector/",
          pageUrl: `${METALWORKS_WEBSITE_URL.replace(/\/$/, "")}/metalworks-crm/prospector/`,
          referrer: `${METALWORKS_WEBSITE_URL.replace(/\/$/, "")}/metalworks-crm/prospector/`,
          ipAddress: cleanText(getClientIp(req), 120),
          userAgent: cleanText(req.headers["user-agent"] || "", 400),
          updatedAt: now,
          createdAt: now,
        });

        await appendActivity({
          leadId: leadDoc._id,
          activityType: "lead_created",
          title: "Lead created",
          body: `${fullName} was captured by ${auth.name} from the prospector portal.`,
          meta: {
            prospectorName: auth.name,
            prospectorEmail: auth.email,
            projectType,
            location,
          },
          req,
          pagePath: "/metalworks-crm/prospector/",
          pageUrl: `${METALWORKS_WEBSITE_URL.replace(/\/$/, "")}/metalworks-crm/prospector/`,
        });
      }

      let syncedAssetCount = 0;

      if (leadDoc?._id && parsedFiles.length) {
        const existingAssets = await MetalworksLeadAsset.find({ leadId: leadDoc._id })
          .select("fileName sizeBytes")
          .lean();
        const existingKeys = new Set(
          existingAssets.map(
            (item) =>
              `${sanitizeLeadAssetFileName(item?.fileName || "")}:${Number(item?.sizeBytes || 0)}`,
          ),
        );
        const newFiles = parsedFiles.filter((item) => {
          const key = `${sanitizeLeadAssetFileName(item.fileName || "")}:${Number(
            item.sizeBytes || 0,
          )}`;

          if (existingKeys.has(key)) {
            return false;
          }

          existingKeys.add(key);
          return true;
        });

        if (newFiles.length) {
          await Promise.all(
            newFiles.map((item) =>
              MetalworksLeadAsset.create({
                leadId: leadDoc._id,
                sourceType: "field_prospector",
                fileName: item.fileName,
                mimeType: item.mimeType,
                sizeBytes: item.sizeBytes,
                fileData: item.fileData,
                uploadedAt: now,
                updatedAt: now,
                createdAt: now,
              }),
            ),
          );
          syncedAssetCount = newFiles.length;
          leadDoc.photoFileNames = mergeAssistantUniqueValues(
            leadDoc.photoFileNames || [],
            ...newFiles.map((item) => item.fileName || ""),
          ).slice(0, 20);
          leadDoc.updatedAt = now;
          await leadDoc.save();
        }
      }

      await appendActivity({
        leadId: leadDoc._id,
        activityType: "prospector_lead_submitted",
        title: duplicate ? "Prospector lead updated" : "Prospector lead saved",
        body: duplicate
          ? `${auth.name} updated this lead from the field.`
          : `${auth.name} submitted this lead from the prospector portal.`,
        meta: {
          duplicate,
          prospectorName: auth.name,
          prospectorEmail: auth.email,
          qualificationTier,
          syncedAssetCount,
        },
        req,
        pagePath: "/metalworks-crm/prospector/",
        pageUrl: `${METALWORKS_WEBSITE_URL.replace(/\/$/, "")}/metalworks-crm/prospector/`,
      });

      let pushDelivery = {
        attempted: false,
        delivered: false,
      };

      if (!duplicate) {
        try {
          pushDelivery = await sendMetalworksPushAlert({
            lead: leadDoc.toObject ? leadDoc.toObject() : leadDoc,
            alertType: "website_lead",
          });
        } catch (error) {
          console.error("Error sending Metal Works prospector push:", error.message);
        }
      }

      const snapshot = await buildProspectorDashboardSnapshot(MetalworksLead, auth.email);

      res.json({
        ok: true,
        duplicate,
        notified: Boolean(pushDelivery.delivered),
        lead: cleanLead(leadDoc.toObject ? leadDoc.toObject() : leadDoc),
        dashboard: snapshot,
      });
    } catch (error) {
      console.error("Error saving Metal Works prospector lead:", error.message);
      respondError(res, 500, "I could not save this prospector lead.");
    }
  });

  app.get(
    ["/metalworks-crm/login", "/metalworks-crm/login/"],
    async (req, res) => {
      res.set("Cache-Control", "no-store");
      const auth = await getAuth(req, { touch: false });

      if (auth.email) {
        return res.redirect("/metalworks-crm/");
      }

      res.sendFile(path.join(publicDir, "metalworks-crm", "login.html"));
    },
  );

  app.get(["/metalworks-crm", "/metalworks-crm/"], async (req, res) => {
    res.set("Cache-Control", "no-store");
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

  app.post("/api/metalworks-crm/push/register", async (req, res) => {
    const auth = await requireAuth(req, res);

    if (!auth) {
      return;
    }

    const deviceToken = normalizePushDeviceToken(req.body?.deviceToken || "");
    const bundleId =
      cleanText(req.body?.bundleId || "", 160) || METALWORKS_IOS_APP_BUNDLE_ID;
    const appEnvironment = normalizePushEnvironment(req.body?.appEnvironment || "sandbox");
    const deviceName = cleanText(req.body?.deviceName || "", 120);
    const appVersion = cleanText(req.body?.appVersion || "", 40);
    const buildNumber = cleanText(req.body?.buildNumber || "", 40);
    const authorizationStatus = cleanText(req.body?.authorizationStatus || "", 40);
    const notificationsEnabled =
      req.body?.notificationsEnabled === false ? false : Boolean(deviceToken);

    if (!deviceToken) {
      return respondError(res, 400, "El device token de Apple es requerido.");
    }

    try {
      const now = new Date();
      const doc = await MetalworksCrmPushDevice.findOneAndUpdate(
        { deviceToken },
        {
          $set: {
            adminEmail: auth.email,
            bundleId,
            appEnvironment,
            deviceName,
            appVersion,
            buildNumber,
            authorizationStatus,
            notificationsEnabled,
            isActive: notificationsEnabled,
            lastSeenAt: now,
            updatedAt: now,
          },
          $setOnInsert: {
            platform: "ios",
            createdAt: now,
          },
        },
        {
          upsert: true,
          new: true,
          setDefaultsOnInsert: true,
        },
      );

      res.json({
        ok: true,
        apnsConfigured: metalworksApnsConfigured(),
        message: metalworksApnsConfigured()
          ? "This iPhone is ready for live lead alerts."
          : "This iPhone is linked, but Apple push keys still need setup on the server.",
        device: cleanPushDevice(doc),
      });
    } catch (error) {
      console.error("Error registering Metal Works push device:", error.message);
      respondError(res, 500, "No pude registrar este iPhone para alerts.");
    }
  });

  app.post("/api/metalworks-crm/push/test", async (req, res) => {
    const auth = await requireAuth(req, res);

    if (!auth) {
      return;
    }

    try {
      const delivery = await sendMetalworksPushAlert({
        alertType: "crm_test",
        adminEmail: auth.email,
      });

      res.json({
        ok: delivery.delivered,
        apnsConfigured: metalworksApnsConfigured(),
        delivered: delivery.delivered,
        deliveredCount: delivery.deliveredCount || 0,
        deviceCount: delivery.deviceCount || 0,
        message: delivery.delivered
          ? "Test push sent to this iPhone."
          : delivery.error || "I could not send the test push yet.",
      });
    } catch (error) {
      console.error("Error sending Metal Works test push:", error.message);
      respondError(res, 500, "No pude mandar el test push.");
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

      const assets = await listLeadAssets(leadId);

      res.json({
        lead: cleanLead(leadDoc, { includeConversation: true }),
        assets,
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
      const clientDocumentType = Object.prototype.hasOwnProperty.call(req.body || {}, "clientDocumentType")
        ? normalizeClientDocumentType(req.body?.clientDocumentType || "estimate")
        : null;
      const clientDocumentDescription = Object.prototype.hasOwnProperty.call(req.body || {}, "clientDocumentDescription")
        ? cleanText(req.body?.clientDocumentDescription || "", 3200)
        : null;
      const clientDocumentWorkDateRaw = Object.prototype.hasOwnProperty.call(req.body || {}, "clientDocumentWorkDate")
        ? String(req.body?.clientDocumentWorkDate || "").trim()
        : null;
      const clientDocumentWorkDate = clientDocumentWorkDateRaw === null
        ? null
        : clientDocumentWorkDateRaw
          ? parseDateOnly(clientDocumentWorkDateRaw)
          : null;
      const clientDocumentWarranty = Object.prototype.hasOwnProperty.call(req.body || {}, "clientDocumentWarranty")
        ? cleanText(req.body?.clientDocumentWarranty || "", 2400)
        : null;
      const note = cleanText(req.body?.note || "", 600);
      let estimateChanged = false;
      let estimateMoneyChanged = false;
      let clientDocumentChanged = false;
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

      if (clientDocumentType !== null) {
        if (normalizeClientDocumentType(leadDoc.clientDocumentType || "") !== clientDocumentType) {
          clientDocumentChanged = true;
        }
        leadDoc.clientDocumentType = clientDocumentType;
      }

      if (clientDocumentDescription !== null) {
        if (leadDoc.clientDocumentDescription !== clientDocumentDescription) {
          clientDocumentChanged = true;
        }
        leadDoc.clientDocumentDescription = clientDocumentDescription;
      }

      if (clientDocumentWorkDateRaw !== null) {
        if (String(leadDoc.clientDocumentWorkDate || "") !== String(clientDocumentWorkDate || "")) {
          clientDocumentChanged = true;
        }
        leadDoc.clientDocumentWorkDate = clientDocumentWorkDate;
      }

      if (clientDocumentWarranty !== null) {
        if (leadDoc.clientDocumentWarranty !== clientDocumentWarranty) {
          clientDocumentChanged = true;
        }
        leadDoc.clientDocumentWarranty = clientDocumentWarranty;
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

      if (clientDocumentChanged) {
        changes.push("Documento para cliente actualizado");
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
      const assets = await listLeadAssets(leadId);

      res.json({
        lead: cleanLead(updatedLead, { includeConversation: true }),
        assets,
        activity: activityDocs.map(cleanActivity).filter(Boolean),
      });
    } catch (error) {
      console.error("Error updating Metal Works lead:", error.message);
      respondError(res, 500, "No pude guardar ese lead.");
    }
  });

  app.get("/api/metalworks-crm/assets/:assetId/content", async (req, res) => {
    const auth = await requireAuth(req, res);

    if (!auth) {
      return;
    }

    const assetId = String(req.params?.assetId || "").trim();

    if (!assetId || !mongoose.Types.ObjectId.isValid(assetId)) {
      return respondError(res, 400, "Asset invalido.");
    }

    try {
      const assetDoc = await MetalworksLeadAsset.findById(assetId).select(
        "mimeType fileName fileData",
      );

      if (!assetDoc?.fileData) {
        return respondError(res, 404, "No encontre esa foto.");
      }

      res.setHeader(
        "Content-Type",
        normalizeLeadAssetMimeType(assetDoc.mimeType || "") || "application/octet-stream",
      );
      res.setHeader(
        "Content-Disposition",
        `inline; filename="${sanitizeLeadAssetFileName(assetDoc.fileName || "project-photo.jpg")}"`,
      );
      res.setHeader("Cache-Control", "private, max-age=3600");
      res.send(assetDoc.fileData);
    } catch (error) {
      console.error("Error loading Metal Works asset:", error.message);
      respondError(res, 500, "No pude cargar esa foto.");
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

      const clientDocument = buildMetalworksClientDocumentSnapshot(leadDoc);

      if (!normalizeEmail(leadDoc.email || "")) {
        return respondError(res, 400, "Este lead no tiene correo todavia.");
      }

      if (
        !cleanText(clientDocument.description || "", 3200) &&
        !normalizeMoney(leadDoc.estimateAmount || 0)
      ) {
        return respondError(
          res,
          400,
          `Primero guarda la descripcion o el total del ${clientDocument.documentType === "invoice" ? "invoice" : "estimate"} para poder enviarlo.`,
        );
      }

      const delivery = await sendMetalworksEstimateEmail(leadDoc, auth.email);
      let activityDocs = [];

      if (delivery.delivered) {
        const sentAt = new Date();
        const statusBefore = normalizeStatus(leadDoc.status || "new");
        let statusLine = "";
        const documentLabel = clientDocument.documentLabel;

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
          title: `${documentLabel} sent`,
          body: `${documentLabel} sent to ${leadDoc.estimateSentTo}.${statusLine}`.trim(),
          meta: {
            adminEmail: auth.email,
            sentTo: leadDoc.estimateSentTo,
            documentType: clientDocument.documentType,
          },
          req,
        });
      }

      const updatedLead = await MetalworksLead.findById(leadId).lean();
      activityDocs = await MetalworksLeadActivity.find({ leadId })
        .sort({ createdAt: -1 })
        .limit(80)
        .lean();
      const assets = await listLeadAssets(leadId);

      res.json({
        ok: true,
        delivered: Boolean(delivery.delivered),
        fallbackUsed: !delivery.delivered,
        message: delivery.delivered
          ? `${clientDocument.documentLabel} sent to the client.`
          : delivery.error || "I could not send it from the system.",
        lead: cleanLead(updatedLead, { includeConversation: true }),
        assets,
        activity: activityDocs.map(cleanActivity).filter(Boolean),
      });
    } catch (error) {
      console.error("Error sending Metal Works estimate:", error.message);
      respondError(res, 500, "No pude enviar ese documento.");
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

      let pushDelivery = {
        attempted: false,
        delivered: false,
      };

      if (!duplicate) {
        try {
          pushDelivery = await sendMetalworksPushAlert({
            lead: leadDoc.toObject ? leadDoc.toObject() : leadDoc,
            alertType: "website_lead",
          });
        } catch (error) {
          console.error("Error sending Metal Works website lead push:", error.message);
        }
      }

      res.json({
        ok: true,
        duplicate,
        notified: Boolean(pushDelivery.delivered),
        lead: cleanLead(leadDoc.toObject ? leadDoc.toObject() : leadDoc),
      });
    } catch (error) {
      console.error("Error saving public Metal Works lead:", error.message);
      respondError(res, 500, "No pude guardar tu quote en este momento.");
    }
  });

  app.post("/api/public/metalworks/external-leads", async (req, res) => {
    try {
      if (!METALWORKS_EXTERNAL_SYNC_TOKEN) {
        return respondError(
          res,
          503,
          "Metal Works external sync token is not configured yet.",
        );
      }

      const syncToken = getExternalSyncToken(req);

      if (!compareSecrets(syncToken, METALWORKS_EXTERNAL_SYNC_TOKEN)) {
        return respondError(res, 401, "Unauthorized external sync request.");
      }

      const externalLeadId = cleanText(req.body?.externalLeadId || "", 120);
      const externalSystem = cleanText(req.body?.externalSystem || "", 80) || "external_sync";
      const fullName = cleanText(req.body?.fullName || req.body?.name || "", 120);
      const phoneDisplay = cleanText(req.body?.phoneDisplay || req.body?.phone || "", 40);
      const phone = normalizePhone(phoneDisplay || req.body?.phone || "");
      const email = normalizeEmail(req.body?.email || "");
      const projectType = cleanText(req.body?.projectType || "", 120);
      const location = cleanText(req.body?.location || "", 160);
      const details = cleanText(req.body?.details || "", 3000);
      const sourceType = cleanText(req.body?.sourceType || "", 80) || externalSystem;
      const pageTitle = cleanText(req.body?.pageTitle || "", 160);
      const pagePath = cleanText(req.body?.pagePath || "", 240);
      const pageUrl = cleanText(req.body?.pageUrl || "", 500);
      const referrer = cleanText(req.body?.referrer || "", 500);
      const tracking = buildTrackingPayload(req.body?.tracking || {});
      const parsedFiles = Array.isArray(req.body?.photos)
        ? req.body.photos
            .map((item) => parseAssistantLeadAssetUpload(item))
            .filter(Boolean)
            .slice(0, METALWORKS_LEAD_ASSET_MAX_FILES)
        : [];
      const rawPhotoFileNames = Array.isArray(req.body?.photoFileNames)
        ? req.body.photoFileNames
            .map((item) => cleanText(item, 120))
            .filter(Boolean)
            .slice(0, 20)
        : [];
      const photoFileNames = mergeAssistantUniqueValues(
        rawPhotoFileNames,
        ...parsedFiles.map((item) => item.fileName || ""),
      ).slice(0, 20);

      if (!externalLeadId) {
        return respondError(res, 400, "Missing external lead id.");
      }

      if (!fullName) {
        return respondError(res, 400, "The full name is required.");
      }

      if (!phone) {
        return respondError(res, 400, "The phone number is required.");
      }

      if (!details) {
        return respondError(res, 400, "The lead details are required.");
      }

      const now = new Date();
      let leadDoc = await MetalworksLead.findOne({
        sourceExternalId: externalLeadId,
      }).sort({ updatedAt: -1, createdAt: -1 });
      const duplicate = Boolean(leadDoc);

      if (leadDoc) {
        leadDoc.fullName = fullName;
        leadDoc.phone = phone;
        leadDoc.phoneDisplay = phoneDisplay || phone;
        leadDoc.email = email;
        leadDoc.projectType = projectType;
        leadDoc.location = location;
        leadDoc.details = details;
        leadDoc.photoFileNames = photoFileNames;
        leadDoc.sourceType = sourceType;
        leadDoc.sourceExternalId = externalLeadId;
        leadDoc.sourceExternalSystem = externalSystem;
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
          phoneDisplay: phoneDisplay || phone,
          email,
          projectType,
          location,
          details,
          photoFileNames,
          status: "new",
          sourceType,
          sourceExternalId: externalLeadId,
          sourceExternalSystem: externalSystem,
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
          body: `${fullName} entro desde ${externalSystem}.`,
          meta: {
            externalLeadId,
            externalSystem,
            projectType,
            location,
          },
          req,
          pagePath,
          pageUrl,
          tracking,
        });
      }

      let syncedAssetCount = 0;

      if (leadDoc?._id && parsedFiles.length) {
        const existingAssets = await MetalworksLeadAsset.find({ leadId: leadDoc._id })
          .select("fileName sizeBytes")
          .lean();
        const existingKeys = new Set(
          existingAssets.map(
            (item) =>
              `${sanitizeLeadAssetFileName(item?.fileName || "")}:${Number(item?.sizeBytes || 0)}`,
          ),
        );
        const newFiles = parsedFiles.filter((item) => {
          const key = `${sanitizeLeadAssetFileName(item.fileName || "")}:${Number(
            item.sizeBytes || 0,
          )}`;

          if (existingKeys.has(key)) {
            return false;
          }

          existingKeys.add(key);
          return true;
        });

        if (newFiles.length) {
          await Promise.all(
            newFiles.map((item) =>
              MetalworksLeadAsset.create({
                leadId: leadDoc._id,
                sourceType,
                fileName: item.fileName,
                mimeType: item.mimeType,
                sizeBytes: item.sizeBytes,
                fileData: item.fileData,
                uploadedAt: now,
                updatedAt: now,
                createdAt: now,
              }),
            ),
          );
          syncedAssetCount = newFiles.length;
          leadDoc.photoFileNames = mergeAssistantUniqueValues(
            leadDoc.photoFileNames || [],
            ...newFiles.map((item) => item.fileName || ""),
          ).slice(0, 20);
          leadDoc.updatedAt = now;
          await leadDoc.save();
        }
      }

      await appendActivity({
        leadId: leadDoc._id,
        activityType: "external_sync",
        title: duplicate ? "Lead externo actualizado" : "Lead externo sincronizado",
        body: duplicate
          ? `El lead externo ${externalLeadId} se actualizo desde ${externalSystem}.`
          : `El lead externo ${externalLeadId} se guardo desde ${externalSystem}.`,
        meta: {
          duplicate,
          externalLeadId,
          externalSystem,
          syncedAssetCount,
          photoFileCount: photoFileNames.length,
        },
        req,
        pagePath,
        pageUrl,
        tracking,
      });

      let pushDelivery = {
        attempted: false,
        delivered: false,
      };

      if (!duplicate) {
        try {
          pushDelivery = await sendMetalworksPushAlert({
            lead: leadDoc.toObject ? leadDoc.toObject() : leadDoc,
            alertType: "website_lead",
          });
        } catch (error) {
          console.error("Error sending Metal Works external sync push:", error.message);
        }
      }

      res.json({
        ok: true,
        duplicate,
        syncedAssetCount,
        notified: Boolean(pushDelivery.delivered),
        lead: cleanLead(leadDoc.toObject ? leadDoc.toObject() : leadDoc),
      });
    } catch (error) {
      console.error("Error saving external Metal Works lead:", error.message);
      respondError(res, 500, "No pude sincronizar este lead externo.");
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
      let pushDelivery = {
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

      try {
        pushDelivery = await sendMetalworksPushAlert({
          lead: leadDoc.toObject ? leadDoc.toObject() : leadDoc,
          alertType: "assistant_callback",
          requestedAtLabel,
        });
      } catch (error) {
        console.error("Error sending Metal Works callback push:", error.message);
      }

      await syncLeadAssetsToLead({
        leadId: leadDoc._id,
        visitorIds: [visitorId],
        sessionIds: [sessionId],
      });

      res.json({
        ok: true,
        duplicate,
        notified: Boolean(alertDelivery.delivered || pushDelivery.delivered),
        requestedAtLabel,
        lead: cleanLead(leadDoc.toObject ? leadDoc.toObject() : leadDoc),
      });
    } catch (error) {
      console.error("Error saving Metal Works assistant appointment:", error.message);
      respondError(res, 500, "No pude guardar la cita en este momento.");
    }
  });

  app.post("/api/public/metalworks/assistant/photos", async (req, res) => {
    try {
      const visitorId = cleanText(req.body?.visitorId || "", 120);
      const sessionId = cleanText(req.body?.sessionId || "", 120);
      const pageTitle = cleanText(req.body?.pageTitle || "", 160);
      const pagePath = cleanText(req.body?.pagePath || "", 240);
      const pageUrl = cleanText(req.body?.pageUrl || "", 500);
      const referrer = cleanText(req.body?.referrer || "", 500);
      const tracking = buildTrackingPayload(req.body?.tracking || {});
      const filePayloads = Array.isArray(req.body?.files) ? req.body.files : [];

      if (!visitorId && !sessionId) {
        return respondError(res, 400, "Missing assistant visitor session.");
      }

      if (!filePayloads.length) {
        return respondError(res, 400, "Add at least one image.");
      }

      if (filePayloads.length > METALWORKS_LEAD_ASSET_MAX_FILES) {
        return respondError(
          res,
          400,
          `Upload up to ${METALWORKS_LEAD_ASSET_MAX_FILES} images at a time.`,
        );
      }

      const parsedFiles = filePayloads.map((item) => parseAssistantLeadAssetUpload(item));
      const totalBytes = parsedFiles.reduce((sum, item) => sum + (item.sizeBytes || 0), 0);

      if (totalBytes > METALWORKS_LEAD_ASSET_MAX_TOTAL_BYTES) {
        return respondError(res, 400, "Total image upload is too large.");
      }

      const cachedSnapshot = await readAssistantFastCacheSnapshot({
        visitorId,
        sessionId,
        sourceChannel: "web",
      });

      let leadDoc = await resolveConversationLead({
        visitorId,
        sessionId,
        leadId: cachedSnapshot?.leadId || "",
      });

      if (!leadDoc) {
        leadDoc = await upsertConversationLead({
          currentLead: null,
          state: {
            shouldCreateLead: true,
            visitorId,
            sessionId,
            items: [],
            latestUserMessage: "",
            detailsSummary: "Visitor uploaded project photos from the assistant chat.",
          },
          pageTitle,
          pagePath,
          pageUrl,
          referrer,
          tracking,
          req,
        });
      }

      if (!leadDoc?._id) {
        return respondError(res, 500, "I could not create a lead for these photos.");
      }

      const now = new Date();
      const assetDocs = await Promise.all(
        parsedFiles.map((item) =>
          MetalworksLeadAsset.create({
            leadId: leadDoc._id,
            visitorId,
            sessionId,
            sourceType: "assistant_chat_photo",
            fileName: item.fileName,
            mimeType: item.mimeType,
            sizeBytes: item.sizeBytes,
            fileData: item.fileData,
            uploadedAt: now,
            updatedAt: now,
            createdAt: now,
          }),
        ),
      );

      leadDoc.photoFileNames = mergeAssistantUniqueValues(
        leadDoc.photoFileNames || [],
        parsedFiles.map((item) => item.fileName),
      );
      leadDoc.lastContactAt = now;
      leadDoc.updatedAt = now;
      await leadDoc.save();

      const cachedHistory = normalizeStoredConversationHistory(cachedSnapshot?.history || []);
      await persistAssistantFastCache({
        visitorId,
        sessionId,
        sourceChannel: "web",
        sourceLabel: "Agustin 2.0 website assistant",
        leadDoc,
        state: {
          photoFileNames: leadDoc.photoFileNames || [],
          photoFileCount: Array.isArray(leadDoc.photoFileNames)
            ? leadDoc.photoFileNames.length
            : 0,
          latestUserMessage: cleanText(leadDoc.lastUserMessage || "", 500),
        },
        history: cachedHistory,
      });

      await appendActivity({
        leadId: leadDoc._id,
        activityType: "assistant_photo_uploaded",
        title: "Fotos subidas desde assistant",
        body: `El visitante subio ${assetDocs.length} foto${assetDocs.length === 1 ? "" : "s"} en el chat.`,
        meta: {
          visitorId,
          sessionId,
          pageTitle,
          fileNames: parsedFiles.map((item) => item.fileName),
        },
        req,
        pagePath,
        pageUrl,
        tracking,
      });

      res.json({
        ok: true,
        uploadedCount: assetDocs.length,
        leadId: String(leadDoc._id),
        assets: assetDocs.map(cleanLeadAsset).filter(Boolean),
      });
    } catch (error) {
      console.error("Error saving Metal Works assistant photos:", error.message);
      respondError(res, 500, error?.message || "I could not save those photos.");
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
      const result = await processMetalworksAssistantMessage({
        message: req.body?.message || "",
        visitorId: req.body?.visitorId || "",
        sessionId: req.body?.sessionId || "",
        pageTitle: req.body?.pageTitle || "",
        pagePath: req.body?.pagePath || "",
        pageUrl: req.body?.pageUrl || "",
        referrer: req.body?.referrer || "",
        tracking: req.body?.tracking || {},
        history: req.body?.history || [],
        req,
        sourceType: "assistant_chat",
        sourceChannel: "web",
        sourceLabel: "Agustin 2.0 website assistant",
        allowEmployment: false,
      });

      if (!result.ok) {
        return respondError(res, result.status || 500, result.error || "I could not answer right now.");
      }

      res.json({
        ok: true,
        respuesta: result.respuesta,
        usedFallback: result.usedFallback,
        leadCaptured: result.leadCaptured,
        leadId: result.leadId,
        callbackCaptured: result.callbackCaptured,
        callbackLabel: result.callbackLabel,
        notified: result.notified,
        remainingToday: result.remainingToday,
      });
    } catch (error) {
      console.error("Error in Metal Works assistant:", error.message);
      respondError(res, 500, "I could not answer right now.");
    }
  });
}
