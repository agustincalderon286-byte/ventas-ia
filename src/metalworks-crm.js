import crypto from "node:crypto";
import http2 from "node:http2";
import path from "node:path";

import webpush from "web-push";

import { buildThumbtackWebhookEvent } from "./thumbtack-webhook.js";

const METALWORKS_CRM_SESSION_COOKIE = "cmwf_crm_session";
const METALWORKS_CRM_SESSION_DAYS = 30;
const METALWORKS_PROSPECTOR_SESSION_COOKIE = "cmwf_prospector_session";
const METALWORKS_PROSPECTOR_SESSION_DAYS = 14;
const METALWORKS_PUBLIC_CHAT_THREAD_COOKIE = "cmwf_live_chat_thread";
const METALWORKS_PUBLIC_CHAT_THREAD_DAYS = 180;
const METALWORKS_PROSPECTOR_STATUS_OPTIONS = ["active", "paused"];
const METALWORKS_PROSPECTOR_PASSWORD_MIN = 8;
const METALWORKS_CRM_DEFAULT_EMAIL = "agustincalderon286@gmail.com";
const METALWORKS_CONTACT_PHONE_DISPLAY = "773 798 4107";
const METALWORKS_CONTACT_EMAIL = "agustincalderon286@gmail.com";
const METALWORKS_WEBSITE_URL = "https://www.chicagometalworksandfencing.com/";
const METALWORKS_THUMBTACK_PROFILE_URL =
  "https://www.thumbtack.com/il/blue-island/metal-fabricators/chicago-metal-works-fencing/service/456785560962318359";
const METALWORKS_DEFAULT_CLIENT_WARRANTY =
  "Chicago Metal Works & Fencing stands behind the approved scope of work. Warranty coverage and any exclusions follow the written agreement for this job.";
const METALWORKS_CRM_USER_PROFILES = {
  "agustincalderon286@gmail.com": {
    displayName: "Agustin",
    skin: "executive-steel",
    themeLabel: "Clear Day Mode",
  },
  "agustincalderon423@gmail.com": {
    displayName: "Agustin",
    skin: "executive-steel",
    themeLabel: "Clear Day Mode",
  },
  "calderonrigoberto51@gmail.com": {
    displayName: "Rigo",
    skin: "executive-steel",
    themeLabel: "Clear Day Mode",
  },
};
const METALWORKS_ASSISTANT_MAX_MESSAGES_PER_DAY = Math.max(
  1,
  Number(process.env.METALWORKS_ASSISTANT_MAX_MESSAGES_PER_DAY || 20),
);
const METALWORKS_CALLBACK_TIME_ZONE = "America/Chicago";
const METALWORKS_ASSISTANT_HISTORY_LIMIT = 18;
const METALWORKS_ASSISTANT_NOTES_MARKER = "[Agustin Assistant Notes]";
const METALWORKS_APPLICANT_NOTES_MARKER = "[Agustin Applicant Notes]";
const METALWORKS_ASSISTANT_PLACEHOLDER_NAME = "Website chat lead";
const METALWORKS_APPLICANT_PLACEHOLDER_NAME = "Job applicant";
const METALWORKS_WEBSITE_CHAT_SOURCE_TYPE = "website_live_chat";
const METALWORKS_WEBSITE_CHAT_PLACEHOLDER_NAME = "Website chat visitor";
const METALWORKS_WEBSITE_CHAT_PLACEHOLDER_PREFIX = "Website chat";
const METALWORKS_LEAD_ASSET_MAX_FILES = 4;
const METALWORKS_LEAD_ASSET_MAX_BYTES = 2 * 1024 * 1024;
const METALWORKS_LEAD_ASSET_MAX_TOTAL_BYTES = 6 * 1024 * 1024;
const METALWORKS_EXTERNAL_SYNC_TOKEN = String(
  process.env.METALWORKS_EXTERNAL_SYNC_TOKEN || "",
).trim();
const THUMBTACK_WEBHOOK_USERNAME =
  cleanText(process.env.THUMBTACK_WEBHOOK_USERNAME || "thumbtack", 120) || "thumbtack";
const THUMBTACK_WEBHOOK_PASSWORD = String(process.env.THUMBTACK_WEBHOOK_PASSWORD || "").trim();
const THUMBTACK_WEBHOOK_TOKEN = String(process.env.THUMBTACK_WEBHOOK_TOKEN || "").trim();
const METALWORKS_WEB_PUSH_VAPID_PUBLIC_KEY = String(
  process.env.METALWORKS_WEB_PUSH_VAPID_PUBLIC_KEY || "",
).trim();
const METALWORKS_WEB_PUSH_VAPID_PRIVATE_KEY = String(
  process.env.METALWORKS_WEB_PUSH_VAPID_PRIVATE_KEY || "",
).trim();
const METALWORKS_WEB_PUSH_SUBJECT =
  cleanText(
    process.env.METALWORKS_WEB_PUSH_SUBJECT || `mailto:${METALWORKS_CONTACT_EMAIL}`,
    200,
  ) || `mailto:${METALWORKS_CONTACT_EMAIL}`;
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
const METALWORKS_APPLICANT_STATUS_OPTIONS = [
  "new",
  "interview_requested",
  "interview_scheduled",
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
const METALWORKS_LEAD_REMINDER_OPTIONS = [
  { minutes: 60, label: "1 hour before" },
  { minutes: 120, label: "2 hours before" },
  { minutes: 1440, label: "1 day before" },
  { minutes: 2880, label: "2 days before" },
];
const METALWORKS_LEAD_REMINDER_MINUTES = new Set(
  METALWORKS_LEAD_REMINDER_OPTIONS.map((option) => option.minutes),
);
const METALWORKS_LEAD_REMINDER_POLL_MS = 60 * 1000;
const METALWORKS_LEAD_REMINDER_GRACE_MS = 12 * 60 * 1000;
const METALWORKS_LEAD_REMINDER_SCAN_WINDOW_MS =
  Math.max(...METALWORKS_LEAD_REMINDER_OPTIONS.map((option) => option.minutes)) * 60 * 1000 +
  METALWORKS_LEAD_REMINDER_GRACE_MS;
const METALWORKS_LEAD_REMINDER_WORKER = {
  started: false,
  timer: null,
  running: false,
};
const METALWORKS_EXTERNAL_LOCK_TTL_MS = 45 * 1000;
const METALWORKS_EXTERNAL_LOCK_MAX_ATTEMPTS = 24;
const METALWORKS_EXTERNAL_LOCK_RETRY_MS = 150;
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
- If the visitor has project photos, tell them they can upload them directly in the chat.
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
const METALWORKS_ASSISTANT_EMPLOYMENT_SYSTEM_PROMPT = `
You are Agustin 2.0 for Chicago Metal Works & Fencing.

ROLE:
- You help job candidates who contact the business through the website assistant or WhatsApp.
- Your goal is to move qualified candidates toward a phone interview.

OPEN ROLES:
- Welder
- Fabricator
- Welder-Fabricator
- Sales
- Prospector

VOICE:
- Default to English.
- If the candidate writes in Spanish, reply in Spanish.
- Sound practical, direct, respectful, and human.
- Keep it short and text-message friendly.

HIRING RULES:
- Ask one main question at a time.
- If the role is not clear, ask which position they want.
- For welder, fabricator, and welder-fabricator roles, qualify for experience, tools, transportation, and field/outdoor work.
- For sales and prospector roles, qualify for experience, languages, transportation, and whether they are comfortable speaking with customers.
- Move the conversation toward a phone interview.
- If the candidate asks about pay, say compensation depends on the role and experience, and keep moving toward qualification.
- Do not promise a job, a start date, benefits, hours, or pay you do not know.
- Do not ask for unnecessary personal data.
- Keep replies to 1 or 2 short paragraphs, usually 1 or 2 short sentences total on WhatsApp.

DO NOT:
- Do not switch back into customer quote mode unless the person is clearly asking as a customer.
- Do not talk about Chef, Coach, Royal Prestige, cooking, or product sales.
`;

function cleanText(value = "", maxLength = 0) {
  const text = String(value || "").replace(/\s+/g, " ").trim();
  return maxLength > 0 ? text.slice(0, maxLength) : text;
}

function cleanMultilineText(value = "", maxLength = 0) {
  const text = String(value || "")
    .replace(/\r\n?/g, "\n")
    .replace(/[ \t]+\n/g, "\n")
    .replace(/\n{3,}/g, "\n\n")
    .trim();

  return maxLength > 0 ? text.slice(0, maxLength).trim() : text;
}

function normalizeEmail(value = "") {
  return cleanText(value).toLowerCase();
}

function normalizePasswordInput(value = "", maxLength = 120) {
  const safeValue = String(value || "").trim();
  return maxLength > 0 ? safeValue.slice(0, maxLength) : safeValue;
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

function parseJsonObject(value = "") {
  const raw = String(value || "").trim();

  if (!raw) {
    return {};
  }

  try {
    const parsed = JSON.parse(raw);
    return parsed && typeof parsed === "object" && !Array.isArray(parsed) ? parsed : {};
  } catch {
    return {};
  }
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

function waitMs(ms = 0) {
  return new Promise((resolve) => {
    setTimeout(resolve, Math.max(0, Number(ms || 0)));
  });
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

function normalizePublicChatThreadKey(value = "") {
  return String(value || "")
    .trim()
    .replace(/[^A-Za-z0-9_-]/g, "")
    .slice(0, 120);
}

function buildPublicChatThreadPath(threadKey = "") {
  const safeThreadKey = normalizePublicChatThreadKey(threadKey);

  if (!safeThreadKey) {
    return "/metalworks-chat/";
  }

  return `/metalworks-chat/?thread=${encodeURIComponent(safeThreadKey)}`;
}

function compareSecrets(input = "", expected = "") {
  const left = Buffer.from(String(input || ""), "utf8");
  const right = Buffer.from(String(expected || ""), "utf8");

  if (!left.length || left.length !== right.length) {
    return false;
  }

  return crypto.timingSafeEqual(left, right);
}

function parseBasicAuthorizationHeader(header = "") {
  const value = String(header || "").trim();

  if (!/^Basic\s+/i.test(value)) {
    return { username: "", password: "" };
  }

  try {
    const decoded = Buffer.from(value.replace(/^Basic\s+/i, "").trim(), "base64").toString(
      "utf8",
    );
    const separatorIndex = decoded.indexOf(":");

    if (separatorIndex === -1) {
      return { username: "", password: "" };
    }

    return {
      username: cleanText(decoded.slice(0, separatorIndex), 160),
      password: decoded.slice(separatorIndex + 1).trim(),
    };
  } catch (error) {
    return { username: "", password: "" };
  }
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

function thumbtackWebhookConfigured() {
  return Boolean(THUMBTACK_WEBHOOK_PASSWORD || THUMBTACK_WEBHOOK_TOKEN);
}

function requestHasThumbtackWebhookAccess(req) {
  if (THUMBTACK_WEBHOOK_PASSWORD) {
    const basicAuth = parseBasicAuthorizationHeader(req.headers.authorization || "");

    if (
      compareSecrets(basicAuth.username, THUMBTACK_WEBHOOK_USERNAME) &&
      compareSecrets(basicAuth.password, THUMBTACK_WEBHOOK_PASSWORD)
    ) {
      return true;
    }
  }

  if (THUMBTACK_WEBHOOK_TOKEN) {
    const token = getExternalSyncToken(req);

    if (compareSecrets(token, THUMBTACK_WEBHOOK_TOKEN)) {
      return true;
    }
  }

  return false;
}

function shouldApplyExternalLeadStatus(currentStatus = "", incomingStatus = "") {
  const current = cleanText(currentStatus || "", 24).toLowerCase();
  const next = cleanText(incomingStatus || "", 24).toLowerCase();

  if (!next || next === current) {
    return false;
  }

  if (!current || current === "new") {
    return true;
  }

  if (next === "won" || next === "lost") {
    return true;
  }

  if (current === "contacted" && next === "quoted") {
    return true;
  }

  return false;
}

export function resolveExternalLeadCreateStatus(
  incomingStatus = "",
  externalSystem = "",
  sourceType = "",
) {
  const normalizedStatus = normalizeStatus(incomingStatus || "new");
  const normalizedSystem = cleanText(externalSystem || "", 80).toLowerCase();
  const normalizedSource = cleanText(sourceType || "", 80).toLowerCase();

  if (
    normalizedStatus === "contacted" &&
    normalizedSystem === "thumbtack" &&
    normalizedSource.startsWith("thumbtack_")
  ) {
    return "new";
  }

  return normalizedStatus || "new";
}

function getAllowedEmails() {
  const passwordOverrides = Object.keys(getMetalworksUserPasswordOverrides());

  return Array.from(
    new Set([
      METALWORKS_CRM_DEFAULT_EMAIL,
      ...Object.keys(METALWORKS_CRM_USER_PROFILES),
      ...passwordOverrides,
      ...parseEmailList(process.env.METALWORKS_CRM_ALLOWED_EMAILS || ""),
    ]),
  ).filter(Boolean);
}

function getMetalworksPassword() {
  return String(process.env.METALWORKS_CRM_PASSWORD || "").trim();
}

function getMetalworksUserPasswordOverrides() {
  const source = parseJsonObject(process.env.METALWORKS_CRM_USER_PASSWORDS_JSON || "");
  const normalized = {};

  Object.entries(source).forEach(([email, password]) => {
    const safeEmail = normalizeEmail(email || "");
    const safePassword = normalizePasswordInput(password || "");

    if (!safeEmail || !safePassword) {
      return;
    }

    normalized[safeEmail] = safePassword;
  });

  return normalized;
}

function getMetalworksPasswordForEmail(email = "") {
  const safeEmail = normalizeEmail(email || "");
  const overrides = getMetalworksUserPasswordOverrides();

  if (safeEmail && overrides[safeEmail]) {
    return overrides[safeEmail];
  }

  return getMetalworksPassword();
}

function metalworksCrmConfigured() {
  return Boolean(
    getAllowedEmails().length &&
      (getMetalworksPassword() || Object.keys(getMetalworksUserPasswordOverrides()).length),
  );
}

function normalizeProspectorStatus(value = "") {
  const status = cleanText(value || "", 24).toLowerCase();
  return METALWORKS_PROSPECTOR_STATUS_OPTIONS.includes(status) ? status : "active";
}

function labelProspectorStatus(value = "") {
  return normalizeProspectorStatus(value) === "paused" ? "Paused" : "Active";
}

function createSecurePasswordHash(password = "") {
  const salt = crypto.randomBytes(16).toString("hex");
  const hash = crypto.scryptSync(String(password || ""), salt, 64).toString("hex");

  return { salt, hash };
}

function verifySecurePasswordHash(password = "", salt = "", storedHash = "") {
  if (!password || !salt || !storedHash) {
    return false;
  }

  const candidateHash = crypto.scryptSync(String(password || ""), String(salt || ""), 64);
  const originalHash = Buffer.from(String(storedHash || ""), "hex");

  if (candidateHash.length !== originalHash.length) {
    return false;
  }

  return crypto.timingSafeEqual(candidateHash, originalHash);
}

function generateProspectorTemporaryPassword() {
  return crypto.randomBytes(10).toString("base64url");
}

function prospectorPasswordIsValid(password = "") {
  return normalizePasswordInput(password).length >= METALWORKS_PROSPECTOR_PASSWORD_MIN;
}

function getMetalworksCrmProfile(email = "") {
  const safeEmail = normalizeEmail(email || "");
  const preset = METALWORKS_CRM_USER_PROFILES[safeEmail] || {};

  return {
    email: safeEmail,
    displayName: preset.displayName || (safeEmail ? safeEmail.split("@")[0] : "CMWF Admin"),
    skin: preset.skin || "executive-steel",
    themeLabel: preset.themeLabel || "",
  };
}

function normalizeStatus(value = "") {
  const status = cleanText(value).toLowerCase();
  return METALWORKS_CRM_STATUS_OPTIONS.includes(status) ? status : "new";
}

function normalizeApplicantStatus(value = "") {
  const status = cleanText(value || "", 40).toLowerCase();
  return METALWORKS_APPLICANT_STATUS_OPTIONS.includes(status) ? status : "new";
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

export function parseCrmDatetimeInput(
  value = "",
  timeZone = METALWORKS_CALLBACK_TIME_ZONE,
) {
  const safeValue = String(value || "").trim();

  if (!safeValue) {
    return null;
  }

  if (value instanceof Date) {
    return Number.isNaN(value.getTime()) ? null : value;
  }

  if (/[zZ]$|[+-]\d{2}:\d{2}$/.test(safeValue)) {
    const parsed = new Date(safeValue);
    return Number.isNaN(parsed.getTime()) ? null : parsed;
  }

  const localMatch = safeValue.match(
    /^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2})(?::(\d{2}))?$/,
  );

  if (localMatch) {
    return buildAssistantZonedDate(
      {
        year: Number(localMatch[1] || 0),
        month: Number(localMatch[2] || 0),
        day: Number(localMatch[3] || 0),
        hour: Number(localMatch[4] || 0),
        minute: Number(localMatch[5] || 0),
      },
      timeZone,
    );
  }

  const parsed = new Date(safeValue);
  return Number.isNaN(parsed.getTime()) ? null : parsed;
}

function parseLegacyCrmScheduleLabel(
  value = "",
  timeZone = METALWORKS_CALLBACK_TIME_ZONE,
) {
  const safeValue = cleanText(value || "", 80);

  if (!safeValue) {
    return null;
  }

  const match = safeValue.match(
    /^(\d{1,2})\/(\d{1,2})\/(\d{4}),\s*(\d{1,2}):(\d{2})(?::(\d{2}))?\s*(AM|PM)$/i,
  );

  if (!match) {
    return null;
  }

  let hour = Number(match[4] || 0);
  const meridiem = String(match[7] || "").toUpperCase();

  if (meridiem === "PM" && hour < 12) {
    hour += 12;
  } else if (meridiem === "AM" && hour === 12) {
    hour = 0;
  }

  return buildAssistantZonedDate(
    {
      month: Number(match[1] || 0),
      day: Number(match[2] || 0),
      year: Number(match[3] || 0),
      hour,
      minute: Number(match[5] || 0),
    },
    timeZone,
  );
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

export function buildMetalworksClientDocumentSnapshot(lead = null) {
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
  const depositAmount = normalizeMoney(lead?.invoiceDepositAmount || 0);
  const balanceDueAmount = Math.max(0, normalizeMoney(totalAmount - depositAmount));
  const total = totalAmount > 0 ? formatMoneyLabel(totalAmount) : "";
  const deposit = depositAmount > 0 ? formatMoneyLabel(depositAmount) : "";
  const balanceDue = totalAmount > 0 ? formatMoneyLabel(balanceDueAmount) : "";
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
    depositAmount,
    deposit,
    balanceDueAmount,
    balanceDue,
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

export function buildMetalworksEstimateEmail(lead = null, replyTo = "") {
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
    snapshot.documentType === "invoice" && snapshot.total
      ? `Total project amount: ${snapshot.total}`
      : snapshot.total
        ? `Total: ${snapshot.total}`
        : "",
    snapshot.documentType === "invoice" && snapshot.deposit
      ? `Deposit received: ${snapshot.deposit}`
      : "",
    snapshot.documentType === "invoice" && snapshot.balanceDue
      ? `Balance due: ${snapshot.balanceDue}`
      : "",
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
            snapshot.documentType === "invoice" && snapshot.total
              ? `<p style="margin:0 0 10px"><strong>Total project amount:</strong> ${escapeHtmlMarkup(snapshot.total)}</p>`
              : snapshot.total
                ? `<p style="margin:0"><strong>Total:</strong> ${escapeHtmlMarkup(snapshot.total)}</p>`
                : ""
          }
          ${
            snapshot.documentType === "invoice" && snapshot.deposit
              ? `<p style="margin:0 0 10px"><strong>Deposit received:</strong> ${escapeHtmlMarkup(snapshot.deposit)}</p>`
              : ""
          }
          ${
            snapshot.documentType === "invoice" && snapshot.balanceDue
              ? `<p style="margin:0"><strong>Balance due:</strong> ${escapeHtmlMarkup(snapshot.balanceDue)}</p>`
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

function buildMetalworksApplicantAlertEmail({
  applicant = null,
  requestedAtLabel = "",
  pagePath = "",
  pageUrl = "",
  conversationDigest = "",
} = {}) {
  const fullName =
    sanitizeAssistantStoredName(cleanText(applicant?.fullName || "", 120)) ||
    cleanText(applicant?.fullName || "", 120) ||
    "Job applicant";
  const positionApplied = cleanText(applicant?.positionApplied || "", 120) || "Open role";
  const phone = cleanText(applicant?.phoneDisplay || applicant?.phone || "", 40) || "Not provided";
  const email = normalizeEmail(applicant?.email || "") || "Not provided";
  const languages = cleanText(applicant?.languages || "", 60) || "Not provided";
  const yearsExperience = cleanText(applicant?.yearsExperience || "", 60) || "Not provided";
  const experienceSummary = cleanText(applicant?.experienceSummary || "", 240) || "Not provided";
  const hasTools = cleanText(applicant?.hasTools || "", 20) || "Not provided";
  const hasTransportation = cleanText(applicant?.hasTransportation || "", 20) || "Not provided";
  const fieldReady = cleanText(applicant?.fieldReady || "", 20) || "Not provided";
  const interviewLabel = cleanText(requestedAtLabel || applicant?.bestInterviewDay || "", 120) || "Not provided";
  const notifyTo = getMetalworksNotificationEmails();
  const subject = `New job applicant - ${fullName}`;
  const intro =
    "A new hiring conversation was captured by Agustin 2.0 for Chicago Metal Works & Fencing.";
  const textLines = [
    intro,
    "",
    `Name: ${fullName}`,
    `Role: ${positionApplied}`,
    `Phone: ${phone}`,
    `Email: ${email}`,
    `Languages: ${languages}`,
    `Years of experience: ${yearsExperience}`,
    `Background: ${experienceSummary}`,
    `Own tools: ${hasTools}`,
    `Transportation: ${hasTransportation}`,
    `Field ready: ${fieldReady}`,
    `Phone interview window: ${interviewLabel}`,
    pagePath ? `Page: ${pagePath}` : "",
    pageUrl ? `URL: ${pageUrl}` : "",
    "",
    "Applicant summary:",
    cleanText(applicant?.detailsSummary || "", 1200) || "No summary provided.",
    conversationDigest ? "Recent conversation:" : "",
    conversationDigest || "",
  ].filter(Boolean);
  const html = `
    <div style="font-family:Arial,sans-serif;background:#f8f5ef;padding:24px;color:#1e2428">
      <div style="max-width:700px;margin:0 auto;background:#ffffff;border:1px solid #e5ddd0;border-radius:18px;padding:28px">
        <p style="margin:0 0 16px">${escapeHtmlMarkup(intro)}</p>
        <div style="border:1px solid #eadfcd;border-radius:16px;padding:18px;margin:0 0 18px;background:#fffaf2">
          <p style="margin:0 0 10px"><strong>Name:</strong> ${escapeHtmlMarkup(fullName)}</p>
          <p style="margin:0 0 10px"><strong>Role:</strong> ${escapeHtmlMarkup(positionApplied)}</p>
          <p style="margin:0 0 10px"><strong>Phone:</strong> ${escapeHtmlMarkup(phone)}</p>
          <p style="margin:0 0 10px"><strong>Email:</strong> ${escapeHtmlMarkup(email)}</p>
          <p style="margin:0 0 10px"><strong>Languages:</strong> ${escapeHtmlMarkup(languages)}</p>
          <p style="margin:0 0 10px"><strong>Years of experience:</strong> ${escapeHtmlMarkup(yearsExperience)}</p>
          <p style="margin:0 0 10px"><strong>Own tools:</strong> ${escapeHtmlMarkup(hasTools)}</p>
          <p style="margin:0 0 10px"><strong>Transportation:</strong> ${escapeHtmlMarkup(hasTransportation)}</p>
          <p style="margin:0 0 10px"><strong>Field ready:</strong> ${escapeHtmlMarkup(fieldReady)}</p>
          <p style="margin:0 0 10px"><strong>Phone interview window:</strong> ${escapeHtmlMarkup(interviewLabel)}</p>
          ${pagePath ? `<p style="margin:0 0 10px"><strong>Page:</strong> ${escapeHtmlMarkup(pagePath)}</p>` : ""}
          ${pageUrl ? `<p style="margin:0"><strong>URL:</strong> ${escapeHtmlMarkup(pageUrl)}</p>` : ""}
        </div>
        <div style="margin:0 0 18px">
          <p style="margin:0 0 8px"><strong>Background</strong></p>
          <p style="margin:0;white-space:pre-wrap">${formatMultilineHtml(experienceSummary)}</p>
        </div>
        <div style="margin:0 0 18px">
          <p style="margin:0 0 8px"><strong>Applicant summary</strong></p>
          <p style="margin:0;white-space:pre-wrap">${formatMultilineHtml(
            cleanText(applicant?.detailsSummary || "", 1200) || "No summary provided.",
          )}</p>
        </div>
        ${
          conversationDigest
            ? `<div style="margin:0"><p style="margin:0 0 8px"><strong>Recent conversation</strong></p><p style="margin:0;white-space:pre-wrap">${formatMultilineHtml(conversationDigest)}</p></div>`
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
    replyTo: normalizeEmail(applicant?.email || "") || METALWORKS_CONTACT_EMAIL,
  };
}

async function sendMetalworksApplicantAlertEmail(options = {}) {
  const apiKey = String(process.env.RESEND_API_KEY || "").trim();
  const fromEmail = String(process.env.RESEND_FROM_EMAIL || "").trim();

  if (!apiKey || !fromEmail) {
    return {
      attempted: false,
      delivered: false,
      error: "Email sending is not configured yet.",
    };
  }

  const payload = buildMetalworksApplicantAlertEmail(options);

  if (!payload.to.length) {
    return {
      attempted: false,
      delivered: false,
      error: "No destination email is configured for Metal Works hiring alerts.",
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

function getMetalworksWebPushConfig() {
  return {
    publicKey: METALWORKS_WEB_PUSH_VAPID_PUBLIC_KEY,
    privateKey: METALWORKS_WEB_PUSH_VAPID_PRIVATE_KEY,
    subject: METALWORKS_WEB_PUSH_SUBJECT,
    configured: Boolean(
      METALWORKS_WEB_PUSH_VAPID_PUBLIC_KEY &&
        METALWORKS_WEB_PUSH_VAPID_PRIVATE_KEY &&
        METALWORKS_WEB_PUSH_SUBJECT,
    ),
  };
}

function metalworksWebPushConfigured() {
  return getMetalworksWebPushConfig().configured;
}

function normalizeWebPushSubscription(input = null) {
  const subscription = input && typeof input === "object" ? input : null;
  const endpoint = cleanText(subscription?.endpoint || "", 1200);
  const auth = cleanText(subscription?.keys?.auth || "", 400);
  const p256dh = cleanText(subscription?.keys?.p256dh || "", 600);

  if (!endpoint || !auth || !p256dh) {
    return null;
  }

  return {
    endpoint,
    expirationTime:
      subscription?.expirationTime === null || subscription?.expirationTime === undefined
        ? null
        : Number(subscription.expirationTime) || null,
    keys: {
      auth,
      p256dh,
    },
  };
}

function normalizeMetalworksNotificationPath(
  value = "",
  fallback = "/metalworks-crm/operator/",
) {
  const safeValue = String(value || "").trim();

  if (
    !safeValue.startsWith("/metalworks-crm") &&
    !safeValue.startsWith("/metalworks-chat")
  ) {
    return fallback;
  }

  const normalized = safeValue.endsWith("/") ? safeValue : `${safeValue}/`;
  return cleanText(normalized, 240) || fallback;
}

function buildMetalworksNotificationUrl(leadId = "", basePath = "/metalworks-crm/operator/") {
  const safeLeadId = cleanText(leadId || "", 80);
  const safeBasePath = normalizeMetalworksNotificationPath(basePath);
  return safeLeadId
    ? `${safeBasePath}?lead=${encodeURIComponent(safeLeadId)}`
    : safeBasePath;
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
  applicant = null,
  alertType = "assistant_lead",
  requestedAtLabel = "",
} = {}) {
  const fullName =
    trimPushCopy(
      sanitizeAssistantStoredName(applicant?.fullName || lead?.fullName || ""),
      60,
    ) ||
    trimPushCopy(applicant?.fullName || lead?.fullName || "", 60) ||
    "New lead";
  const projectType =
    trimPushCopy(applicant?.positionApplied || "", 54) ||
    trimPushCopy(lead?.projectType || "", 54) ||
    trimPushCopy(lead?.estimateTitle || "", 54) ||
    trimPushCopy(applicant?.location || lead?.location || "", 54) ||
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

  if (alertType === "assistant_photo_uploaded") {
    return {
      title: "New project photos",
      body: `${fullName} uploaded photos for ${projectType}.`,
    };
  }

  if (alertType === "assistant_lead") {
    const lastMessage =
      trimPushCopy(lead?.lastUserMessage || "", 78) ||
      trimPushCopy(lead?.details || "", 78) ||
      trimPushCopy(projectType || "", 78);

    return {
      title: "New Agustin 2.0 lead",
      body: lastMessage ? `${fullName}: ${lastMessage}` : `${fullName} • ${projectType}`,
    };
  }

  if (alertType === "crm_test") {
    return {
      title: "Agustin 2.0 CRM",
      body: "Test alert delivered from Chicago Metal Works & Fencing.",
    };
  }

  if (alertType === "job_applicant") {
    return {
      title: "New applicant",
      body: callbackLabel
        ? `${fullName} • ${projectType} • ${callbackLabel}`
        : `${fullName} • ${projectType}`,
    };
  }

  if (alertType === "website_live_chat") {
    const lastMessage =
      trimPushCopy(lead?.lastUserMessage || "", 78) ||
      trimPushCopy(lead?.details || "", 78) ||
      trimPushCopy(projectType || "", 78);

    return {
      title: "New website chat",
      body: lastMessage ? `${fullName}: ${lastMessage}` : `${fullName} sent a website chat.`,
    };
  }

  if (alertType === "lead_followup_reminder") {
    const actionLabel =
      trimPushCopy(lead?.nextAction || "", 54) || trimPushCopy(projectType || "", 54);

    return {
      title: "Lead reminder",
      body: callbackLabel
        ? `${fullName} • ${actionLabel} • ${callbackLabel}`
        : `${fullName} needs a follow-up soon.`,
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

async function sendMetalworksWebPushNotification({
  subscription = null,
  alertType = "assistant_lead",
  title = "",
  body = "",
  leadId = "",
  notificationPath = "/metalworks-crm/operator/",
  targetUrl = "",
} = {}) {
  const config = getMetalworksWebPushConfig();

  if (!config.configured) {
    return {
      attempted: false,
      delivered: false,
      error: "Web push credentials are not configured yet.",
      reason: "",
      status: 0,
    };
  }

  const safeSubscription = normalizeWebPushSubscription(subscription);

  if (!safeSubscription) {
    return {
      attempted: false,
      delivered: false,
      error: "The web push subscription is missing.",
      reason: "MissingWebPushSubscription",
      status: 0,
    };
  }

  try {
    const safeTargetUrl = cleanText(targetUrl || "", 500);
    webpush.setVapidDetails(config.subject, config.publicKey, config.privateKey);
    await webpush.sendNotification(
      safeSubscription,
      JSON.stringify({
        title: trimPushCopy(title || "", 60),
        body: trimPushCopy(body || "", 140),
        alertType: cleanText(alertType || "", 60),
        leadId: cleanText(leadId || "", 80),
        url: safeTargetUrl || buildMetalworksNotificationUrl(leadId, notificationPath),
      }),
      {
        TTL: 90,
        urgency: "high",
      },
    );

    return {
      attempted: true,
      delivered: true,
      error: "",
      reason: "",
      status: 201,
    };
  } catch (error) {
    return {
      attempted: true,
      delivered: false,
      error: cleanText(error?.body || error?.message || "Web push failed.", 240),
      reason: cleanText(error?.name || error?.code || "", 120),
      status: Number(error?.statusCode || 0) || 0,
    };
  }
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

function labelApplicantStatus(status = "") {
  const normalized = normalizeApplicantStatus(status);
  const labels = {
    new: "Nuevo candidato",
    interview_requested: "Entrevista pedida",
    interview_scheduled: "Entrevista agendada",
    archived: "Archivado",
  };

  return labels[normalized] || "Nuevo candidato";
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
    prospector_lead_submitted: "Prospector lead",
    estimate_sent: "Estimate enviado",
    assistant_open: "Assistant abierto",
    assistant_cta_click: "Assistant CTA",
    assistant_user_message: "Mensaje al assistant",
    assistant_ai_reply: "Respuesta del assistant",
    assistant_fallback: "Fallback del assistant",
    assistant_booking_requested: "Cita pedida desde assistant",
    assistant_photo_uploaded: "Fotos subidas desde assistant",
    website_live_chat_message: "Mensaje del chat web",
    website_live_chat_photo_uploaded: "Fotos subidas desde chat web",
    website_live_chat_reply: "Respuesta del CRM",
    text_thread_imported: "Textos importados",
    lead_followup_reminder_sent: "Reminder enviado",
    job_applicant_created: "Candidato nuevo",
    job_applicant_updated: "Candidato actualizado",
    job_applicant_interview_requested: "Entrevista de candidato",
    applicant_user_message: "Mensaje del candidato",
    applicant_ai_reply: "Respuesta para candidato",
    applicant_fallback: "Fallback candidato",
  };

  return labels[type] || "Actividad";
}

function buildExternalLeadLockKey(externalSystem = "", externalLeadId = "") {
  const safeSystem = cleanText(externalSystem || "", 80);
  const safeExternalLeadId = cleanText(externalLeadId || "", 120);

  if (!safeSystem || !safeExternalLeadId) {
    return "";
  }

  return `${safeSystem}:${safeExternalLeadId}`;
}

export function buildThumbtackExternalEventKey(parsedEvent = {}) {
  const eventType = cleanText(parsedEvent?.eventType || "", 80);
  const entityType = cleanText(parsedEvent?.entityType || "", 40);
  const externalLeadId = cleanText(parsedEvent?.leadCandidate?.externalLeadId || "", 120);
  const negotiationId =
    cleanText(parsedEvent?.activity?.meta?.negotiationId || "", 120) || externalLeadId;
  const messageId = cleanText(parsedEvent?.activity?.meta?.messageId || "", 120);
  const reviewId = cleanText(parsedEvent?.activity?.meta?.reviewId || "", 120);

  if (entityType === "message" && negotiationId && messageId) {
    return `thumbtack:${negotiationId}:message:${messageId}`;
  }

  if (entityType === "review" && reviewId) {
    return `thumbtack:review:${reviewId}`;
  }

  if (entityType === "negotiation" && negotiationId && eventType) {
    return `thumbtack:${negotiationId}:negotiation:${eventType}`;
  }

  if (externalLeadId && eventType) {
    return `thumbtack:${externalLeadId}:${eventType}`;
  }

  return "";
}

export function mergeLeadTextImportIntoPrivateNotes(
  existingNotes = "",
  importedText = "",
  {
    sourceLabel = "",
    importedAt = new Date(),
    timeZone = METALWORKS_CALLBACK_TIME_ZONE,
    maxLength = 12000,
  } = {},
) {
  const safeImportedText = cleanMultilineText(importedText || "", 8000);

  if (!safeImportedText) {
    return cleanMultilineText(existingNotes || "", maxLength);
  }

  const safeExistingNotes = cleanMultilineText(existingNotes || "", maxLength);
  const safeSourceLabel = cleanText(sourceLabel || "", 60) || "Text thread";
  const importedAtLabel =
    formatDateTimeLabel(importedAt, timeZone) || formatDateTimeLabel(new Date(), timeZone);
  const importBlock = cleanMultilineText(
    [`[${safeSourceLabel} import • ${importedAtLabel}]`, safeImportedText].join("\n"),
    maxLength,
  );

  return cleanMultilineText(
    [importBlock, safeExistingNotes].filter(Boolean).join("\n\n"),
    maxLength,
  );
}

export function normalizeLeadReminderOffsets(values = []) {
  const safeValues = Array.isArray(values) ? values : [values];
  const unique = [];
  const seen = new Set();

  safeValues.forEach((value) => {
    const minutes = Math.round(Number(value || 0));

    if (!METALWORKS_LEAD_REMINDER_MINUTES.has(minutes) || seen.has(minutes)) {
      return;
    }

    seen.add(minutes);
    unique.push(minutes);
  });

  return unique.sort((left, right) => left - right);
}

export function formatLeadReminderOffsetLabel(value = 0) {
  const minutes = Math.round(Number(value || 0));
  return (
    METALWORKS_LEAD_REMINDER_OPTIONS.find((option) => option.minutes === minutes)?.label || ""
  );
}

function buildLeadReminderSentKey(nextActionAt = null, offsetMinutes = 0) {
  const schedule =
    nextActionAt instanceof Date
      ? nextActionAt
      : nextActionAt
        ? new Date(nextActionAt)
        : null;

  if (!(schedule instanceof Date) || Number.isNaN(schedule.getTime())) {
    return "";
  }

  const safeOffset = Math.round(Number(offsetMinutes || 0));

  if (!METALWORKS_LEAD_REMINDER_MINUTES.has(safeOffset)) {
    return "";
  }

  return `${schedule.toISOString()}|${safeOffset}`;
}

function pruneLeadReminderSentKeys(sentKeys = [], nextActionAt = null, reminderOffsets = []) {
  const safeKeys = Array.isArray(sentKeys) ? sentKeys : [];
  const allowedOffsets = normalizeLeadReminderOffsets(reminderOffsets);

  if (!allowedOffsets.length) {
    return [];
  }

  const allowedKeys = new Set(
    allowedOffsets
      .map((offsetMinutes) => buildLeadReminderSentKey(nextActionAt, offsetMinutes))
      .filter(Boolean),
  );

  return safeKeys
    .map((key) => cleanText(key || "", 120))
    .filter((key) => key && allowedKeys.has(key));
}

export function collectDueLeadReminderOffsets({
  nextActionAt = null,
  reminderOffsets = [],
  sentKeys = [],
  now = new Date(),
  graceMs = METALWORKS_LEAD_REMINDER_GRACE_MS,
} = {}) {
  const schedule =
    nextActionAt instanceof Date
      ? nextActionAt
      : nextActionAt
        ? new Date(nextActionAt)
        : null;
  const safeNow = now instanceof Date ? now : new Date(now);

  if (
    !(schedule instanceof Date) ||
    Number.isNaN(schedule.getTime()) ||
    !(safeNow instanceof Date) ||
    Number.isNaN(safeNow.getTime())
  ) {
    return [];
  }

  const sentKeySet = new Set(
    (Array.isArray(sentKeys) ? sentKeys : [])
      .map((key) => cleanText(key || "", 120))
      .filter(Boolean),
  );

  return normalizeLeadReminderOffsets(reminderOffsets)
    .map((offsetMinutes) => {
      const triggerAtMs = schedule.getTime() - offsetMinutes * 60 * 1000;
      const diffMs = safeNow.getTime() - triggerAtMs;
      const key = buildLeadReminderSentKey(schedule, offsetMinutes);

      if (!key || diffMs < 0 || diffMs > graceMs || sentKeySet.has(key)) {
        return null;
      }

      return {
        offsetMinutes,
        label: formatLeadReminderOffsetLabel(offsetMinutes),
        key,
        triggerAt: new Date(triggerAtMs),
      };
    })
    .filter(Boolean);
}

function detectSpanish(value = "") {
  return /[¿¡]|\b(hola|precio|cotiza|reparacion|reparación|porton|portón|barandal|soldadura|cerca|reja|gracias|necesito|quiero|ayuda)\b/i.test(
    String(value || ""),
  );
}

function detectAffirmative(value = "") {
  return /\b(yes|yeah|yep|si|sí|claro|correct|correcto|of course|sure|i do|tengo|cuento con|available|disponible)\b/i.test(
    String(value || ""),
  );
}

function detectNegative(value = "") {
  return /\b(no|nope|nah|not really|para nada|ninguno|ninguna|dont|don't|do not|sin)\b/i.test(
    String(value || ""),
  );
}

function normalizeApplicantYesNo(value = "") {
  if (detectAffirmative(value) && !detectNegative(value)) {
    return "yes";
  }

  if (detectNegative(value)) {
    return "no";
  }

  return "";
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

export function detectAssistantProjectLeadIntent(value = "") {
  const normalized = normalizeAssistantSearchText(value || "");

  if (!normalized) {
    return false;
  }

  return /\b(project|quote|estimate|repair|replace|replacement|new install|new installation|install|installation|guardrail|guardrails|railing|railings|handrail|handrails|gate|gates|fence|fencing|weld|welding|fabricat|stairs|stair|balcony|porch|awning|opening|opening width|opening height|safety issue|unsafe)\b/.test(
    normalized,
  );
}

export function detectEmploymentCorrection(value = "") {
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

export function detectEmploymentIntent(value = "") {
  const normalized = normalizeAssistantSearchText(value || "");

  if (!normalized) {
    return false;
  }

  if (detectEmploymentCorrection(normalized)) {
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

function inferApplicantRole(value = "") {
  const normalized = normalizeAssistantSearchText(value || "");

  if (!normalized) {
    return "";
  }

  if (
    /\b(welder[- ]fabricator|welder fabricator|fabricator welder|soldador[- ]fabricador|soldador fabricador|fabricador soldador)\b/.test(
      normalized,
    )
  ) {
    return "Welder-Fabricator";
  }

  if (/\b(welder|soldador)\b/.test(normalized)) {
    return "Welder";
  }

  if (/\b(fabricator|fabricador)\b/.test(normalized)) {
    return "Fabricator";
  }

  if (/\b(sales|ventas|sales rep|salesperson)\b/.test(normalized)) {
    return "Sales";
  }

  if (/\b(prospector|prospectador|door to door|door-knocking|door knocking)\b/.test(normalized)) {
    return "Prospector";
  }

  return "";
}

function applicantLooksLikeMisclassifiedCustomer(applicant = null) {
  if (!applicant) {
    return false;
  }

  return detectEmploymentCorrection(
    [
      applicant?.positionApplied || "",
      applicant?.detailsSummary || "",
      applicant?.experienceSummary || "",
      applicant?.lastUserMessage || "",
      applicant?.lastAssistantMessage || "",
    ]
      .filter(Boolean)
      .join("\n"),
  );
}

function inferApplicantRoleTrack(value = "") {
  const role = cleanText(value || "", 80);

  if (!role) {
    return "";
  }

  if (["Welder", "Fabricator", "Welder-Fabricator"].includes(role)) {
    return "trade";
  }

  if (role === "Sales") {
    return "sales";
  }

  if (role === "Prospector") {
    return "prospector";
  }

  return "";
}

function extractApplicantLanguages(value = "") {
  const normalized = normalizeAssistantSearchText(value || "");

  if (!normalized) {
    return "";
  }

  if (
    /\b(bilingual|both|english and spanish|spanish and english|ingles y espanol|espanol e ingles|bilingue)\b/.test(
      normalized,
    )
  ) {
    return "Bilingual";
  }

  if (/\b(english only|english|ingles)\b/.test(normalized) && !/\b(spanish|espanol)\b/.test(normalized)) {
    return "English";
  }

  if (/\b(spanish only|spanish|espanol)\b/.test(normalized) && !/\b(english|ingles)\b/.test(normalized)) {
    return "Spanish";
  }

  return "";
}

function extractApplicantYearsExperience(value = "") {
  const source = String(value || "");
  const normalized = normalizeAssistantSearchText(source);

  if (!normalized) {
    return "";
  }

  if (/\b(no experience|sin experiencia)\b/.test(normalized)) {
    return "0 years";
  }

  const match = source.match(/(\d{1,2})(?:\+)?\s*(?:years?|yrs?|anos?|años?)/i);

  if (!match?.[1]) {
    return "";
  }

  const amount = Number(match[1] || 0);

  if (!amount && amount !== 0) {
    return "";
  }

  return `${amount} year${amount === 1 ? "" : "s"}`;
}

function extractApplicantExperienceSummary(value = "") {
  const safeValue = cleanText(value || "", 180);

  if (!safeValue) {
    return "";
  }

  if (detectEmploymentIntent(safeValue) && safeValue.split(/\s+/).length <= 4) {
    return "";
  }

  return safeValue;
}

function getApplicantMissingFieldQuestion(field = "", inSpanish = false, roleTrack = "") {
  const questions = {
    positionApplied: inSpanish
      ? "¿Que puesto te interesa: soldador, fabricador, soldador-fabricador, ventas o prospectador?"
      : "Which position are you interested in: welder, fabricator, welder-fabricator, sales, or prospector?",
    fullName: inSpanish
      ? "¿Cual es tu nombre completo?"
      : "What is your full name?",
    phoneOrEmail: inSpanish
      ? "¿Cual es tu mejor numero de telefono o correo para contactarte?"
      : "What is the best phone number or email to reach you?",
    yearsExperience: inSpanish
      ? "¿Cuantos anos de experiencia tienes?"
      : "How many years of experience do you have?",
    experienceSummary:
      roleTrack === "sales"
        ? inSpanish
          ? "¿Tienes experiencia en ventas, seguimiento de leads o atencion al cliente?"
          : "Do you have experience in sales, lead follow-up, or customer service?"
        : roleTrack === "prospector"
          ? inSpanish
            ? "¿Tienes experiencia prospectando, tocando puertas o generando leads?"
            : "Do you have experience prospecting, door knocking, or generating leads?"
          : inSpanish
            ? "¿En que tipo de trabajo tienes mas experiencia?"
            : "What type of work do you have the most experience in?",
    hasTools: inSpanish
      ? "¿Tienes tus propias herramientas?"
      : "Do you have your own tools?",
    hasTransportation: inSpanish
      ? "¿Tienes transporte propio?"
      : "Do you have your own transportation?",
    fieldReady: inSpanish
      ? "¿Estas comodo trabajando afuera en el campo?"
      : "Are you comfortable working outside in the field?",
    languages: inSpanish
      ? "¿Hablas ingles, espanol o bilingue?"
      : "Do you speak English, Spanish, or both?",
    bestInterviewDay: inSpanish
      ? "¿Que dia te queda mejor para una entrevista por telefono?"
      : "What day works best for a phone interview?",
    bestInterviewTime: inSpanish
      ? "¿Y que hora te queda mejor para esa llamada?"
      : "What time works best for that call?",
  };

  return questions[field] || (inSpanish ? "Mandame un poco mas de informacion." : "Send me a little more information.");
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
        ? `Claro. Para dejar la llamada o cita bien pedida mandame solo esto: ${missingLabel}.`
        : `Absolutely. To save the callback or appointment request, send just these details: ${missingLabel}.`;
    }

    const callbackLabel =
      cleanText(conversationState?.callbackLabel || "", 120) || "your requested time";

    return inSpanish
      ? `Perfecto. Ya tengo tu solicitud para ${callbackLabel}. Si puedes, manda fotos y tu ZIP code para preparar mejor el seguimiento.`
      : `Perfect. I have your request for ${callbackLabel}. If you can, send photos and your ZIP code so we can prep the follow-up faster.`;
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
      ? "La forma mas rapida de cotizar es subir fotos aqui en el chat, mandar medidas aproximadas, tu ZIP code y decir si es reparacion o trabajo nuevo. Si quieres moverlo mas rapido, usa el formulario o llama al 773 798 4107."
      : "The fastest way to get pricing is to upload photos here in the chat, send rough measurements, your ZIP code, and whether you need a repair or a new build. If you want to move faster, use the quote form or call 773 798 4107.";
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
    ? "Puedo ayudar con portones, barandales, cercas, soldadura y fabricacion metalica. Dime que necesita reparacion o que quieres construir, agrega tu ZIP code, y si tienes fotos subelas aqui en el chat para moverlo mas rapido."
    : "I can help with gates, railings, fence work, welding, and custom metal fabrication. Tell me what needs repair or what you want built, include your ZIP code, and if you have photos, upload them here in the chat so we can move faster.";
}

function buildEmploymentFallbackReply(message = "", conversationState = null) {
  const inSpanish = detectSpanish(message) || conversationState?.inSpanish;
  const roleTrack = cleanText(conversationState?.roleTrack || "", 40);
  const missingFields = Array.isArray(conversationState?.applicantMissingFields)
    ? conversationState.applicantMissingFields
    : [];
  const nextField = missingFields[0] || "";
  const interviewLabel =
    cleanText(conversationState?.interviewLabel || "", 120) || "your phone interview window";
  const text = normalizeAssistantSearchText(message);

  if (!conversationState?.positionApplied) {
    return inSpanish
      ? "Claro. Estamos hablando con candidatos para soldador, fabricador, soldador-fabricador, ventas y prospectador. ¿Que puesto te interesa?"
      : "Sure. We are speaking with candidates for welder, fabricator, welder-fabricator, sales, and prospector roles. Which position interests you?";
  }

  if (/\b(pay|paid|salary|wage|compensation|benefits|cuanto pagan|paga|sueldo|salario)\b/.test(text)) {
    return inSpanish
      ? "La paga depende del puesto y de la experiencia. Primero quiero dejar tu perfil bien guardado para moverte a entrevista por telefono. " +
          getApplicantMissingFieldQuestion(nextField || "phoneOrEmail", true, roleTrack)
      : "Pay depends on the role and your experience. First I want to save your profile correctly and move you to a phone interview. " +
          getApplicantMissingFieldQuestion(nextField || "phoneOrEmail", false, roleTrack);
  }

  if (nextField) {
    return getApplicantMissingFieldQuestion(nextField, inSpanish, roleTrack);
  }

  if (conversationState?.nextActionAt || conversationState?.bestInterviewDay || conversationState?.bestInterviewTime) {
    return inSpanish
      ? `Perfecto. Ya deje tu informacion lista para entrevista por telefono en ${interviewLabel}. Si cambia tu horario, me lo puedes escribir aqui.`
      : `Perfect. I saved your information for a phone interview around ${interviewLabel}. If your availability changes, you can message me here.`;
  }

  return inSpanish
    ? "Gracias. Ya guarde tu informacion para el equipo de contratacion. Si puedes, mandame tu mejor horario para una entrevista por telefono."
    : "Thanks. I saved your information for the hiring team. If you can, send the best time for a phone interview.";
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

function buildEmploymentContext(message = "", pagePath = "") {
  const text = cleanText(message, 500).toLowerCase();
  const contextParts = [];

  contextParts.push(`
METAL WORKS HIRING CONTEXT:
- Business: Chicago Metal Works & Fencing
- Main hiring follow-up path: phone interview
- Priority roles: welder, fabricator, welder-fabricator, sales, prospector
- Public website page: ${cleanText(pagePath || "", 120) || "/"}
`);

  if (/\b(welder|fabricator|soldador|fabricador)\b/.test(text)) {
    contextParts.push(`
SKILLED TRADE FIT:
- Qualify for years of experience, type of work done, tools, transportation, and comfort with field/outdoor work.
- Typical trade work includes railings, gates, fences, stairs, repairs, and fabrication.
`);
  }

  if (/\b(sales|ventas)\b/.test(text)) {
    contextParts.push(`
SALES FIT:
- Qualify for sales, estimates, customer service, and lead follow-up experience.
- Confirm languages and transportation.
`);
  }

  if (/\b(prospector|prospectador|door)\b/.test(text)) {
    contextParts.push(`
PROSPECTOR FIT:
- Qualify for door knocking, field prospecting, lead generation, transportation, and comfort working outside.
- Confirm languages early.
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

function normalizeAssistantSummaryHistory(history = []) {
  return (Array.isArray(history) ? history : [])
    .map((item) => ({
      role: item?.role === "assistant" ? "assistant" : "user",
      content: cleanText(item?.content || "", 320),
    }))
    .filter((item) => item.content)
    .slice(-24);
}

function dedupeAssistantSummaryHistory(history = []) {
  const deduped = [];
  const seen = new Set();

  normalizeAssistantSummaryHistory(history).forEach((item) => {
    const key = `${item.role}:${normalizeAssistantSearchText(item.content)}`;

    if (!key || seen.has(key)) {
      return;
    }

    seen.add(key);
    deduped.push(item);
  });

  return deduped;
}

function isLowSignalAssistantConversationMessage(value = "") {
  const normalized = normalizeAssistantSearchText(value);

  if (!normalized) {
    return true;
  }

  return [
    "hi",
    "hello",
    "hola",
    "ho",
    "hey",
    "ok",
    "okay",
    "si",
    "sí",
    "yes",
    "sr",
    "senor",
    "señor",
    "perfecto",
    "gracias",
    "thanks",
    "thank you",
  ].includes(normalized);
}

function extractAssistantRequestSummary(value = "") {
  const safeValue = cleanText(value || "", 500);

  if (!safeValue) {
    return "";
  }

  const requestMatch = safeValue.match(
    /Request:\s*(.+?)(?:(?:\.\s*)(?:Best callback window|Visitor asked for a call or appointment|Best requested time)|$)/i,
  );

  if (requestMatch?.[1]) {
    return cleanText(requestMatch[1], 220);
  }

  const summaryMatch = safeValue.match(/Summary:\s*(.+)$/i);

  if (summaryMatch?.[1]) {
    return cleanText(summaryMatch[1], 220);
  }

  return cleanText(safeValue, 220);
}

function buildAssistantConversationSummary({
  history = [],
  detailsSummary = "",
  lastUserMessage = "",
  lastAssistantMessage = "",
} = {}) {
  const dedupedHistory = dedupeAssistantSummaryHistory(history);
  const userMessages = dedupedHistory
    .filter((item) => item.role === "user")
    .map((item) => cleanText(item.content || "", 220))
    .filter(Boolean);
  const meaningfulUserMessages = userMessages.filter(
    (item) => !isLowSignalAssistantConversationMessage(item) && item.split(/\s+/).length >= 4,
  );
  const requestSummary = extractAssistantRequestSummary(detailsSummary) || meaningfulUserMessages[0] || "";
  const latestVisitorMessage =
    cleanText(lastUserMessage || "", 220) || meaningfulUserMessages[meaningfulUserMessages.length - 1] || "";
  const latestAssistantStep =
    cleanText(lastAssistantMessage || "", 260) ||
    dedupedHistory
      .filter((item) => item.role === "assistant")
      .map((item) => item.content)
      .filter(Boolean)
      .slice(-1)[0] ||
    "";
  const seenMessages = new Set(
    [requestSummary, latestVisitorMessage]
      .map((item) => normalizeAssistantSearchText(item))
      .filter(Boolean),
  );
  const keyPoints = [];

  meaningfulUserMessages.forEach((item) => {
    const normalized = normalizeAssistantSearchText(item);

    if (!normalized || seenMessages.has(normalized)) {
      return;
    }

    seenMessages.add(normalized);
    keyPoints.push(item);
  });

  return [
    requestSummary ? `Visitor goal: ${requestSummary}` : "",
    keyPoints.length ? `Key points: ${keyPoints.slice(0, 2).join(" / ")}` : "",
    latestVisitorMessage ? `Latest visitor message: ${latestVisitorMessage}` : "",
    latestAssistantStep ? `Latest assistant step: ${latestAssistantStep}` : "",
  ]
    .filter(Boolean)
    .join("\n")
    .slice(0, 1400);
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

  return /(?:call me|give me a call|can you call|can someone call|talk by phone|talk on the phone|phone call|schedule a call|set up a call|set up an appointment|schedule an appointment|set up a visit|schedule a visit|come by|come out|site visit|estimate visit|quote visit|in person estimate|reach me|follow up by phone|llamame|llamarme|me pueden llamar|quiero una llamada|quiero llamada|agendar llamada|agendar una llamada|agendar cita|agendar una cita|agendar visita|agendar una visita|pueden venir|pueden pasar|visita para estimate|visita para cotizacion|hablar por telefono|marcame)/.test(
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

function buildApplicantConversationSignals({
  history = [],
  applicant = null,
} = {}) {
  const items = normalizeAssistantHistory(history);
  const userMessages = items.filter((item) => item.role === "user").map((item) => item.content);
  const combinedUserText = userMessages.join("\n");
  const latestUserMessage = userMessages[userMessages.length - 1] || "";
  let fullName = sanitizeAssistantStoredName(applicant?.fullName || "");
  let email = normalizeEmail(applicant?.email || "");
  let phone = normalizePhone(applicant?.phone || "");
  let phoneDisplay = cleanText(applicant?.phoneDisplay || "", 40);
  let positionApplied = cleanText(applicant?.positionApplied || "", 80);
  let roleTrack = cleanText(applicant?.roleTrack || "", 40) || inferApplicantRoleTrack(positionApplied);
  let languages = cleanText(applicant?.languages || "", 40);
  let yearsExperience = cleanText(applicant?.yearsExperience || "", 40);
  let experienceSummary = cleanText(applicant?.experienceSummary || "", 180);
  let hasTools = cleanText(applicant?.hasTools || "", 12);
  let hasTransportation = cleanText(applicant?.hasTransportation || "", 12);
  let fieldReady = cleanText(applicant?.fieldReady || "", 12);
  let location = cleanText(applicant?.location || "", 160);
  let bestInterviewDay = cleanText(applicant?.bestInterviewDay || "", 80);
  let bestInterviewTime = cleanText(applicant?.bestInterviewTime || "", 80);
  const storedNextActionAt =
    applicant?.nextActionAt instanceof Date
      ? applicant.nextActionAt
      : applicant?.nextActionAt
        ? new Date(applicant.nextActionAt)
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
    const cleanedEntry = cleanText(entryText, 180).replace(/[.,;!?]+$/, "");
    const normalizedPreviousAssistant = normalizeAssistantSearchText(previousAssistantMessage);
    const contactInfo = extractAssistantContactInfo(entryText);
    const entryName = extractAssistantName(entryText);
    const entryRole = inferApplicantRole(entryText);
    const entryLanguages = extractApplicantLanguages(entryText);
    const entryYearsExperience = extractApplicantYearsExperience(entryText);
    const entryLocation = extractAssistantLocation(entryText);
    const entryBestDay = extractAssistantPreferredDay(entryText);
    const entryBestTime = extractAssistantPreferredTime(entryText);
    const entryYesNo = normalizeApplicantYesNo(entryText);
    const entryExperienceSummary = extractApplicantExperienceSummary(entryText);

    const askedForName =
      /\b(name|nombre)\b/.test(normalizedPreviousAssistant) &&
      !/\b(company|empresa|job site|project|service)\b/.test(normalizedPreviousAssistant);
    const askedForRole =
      /\b(position|puesto|role|vacante|applying for|applying as|interested in)\b/.test(
        normalizedPreviousAssistant,
      );
    const askedForYearsExperience =
      /\b(years of experience|experience do you have|cuantos anos|cuanta experiencia)\b/.test(
        normalizedPreviousAssistant,
      );
    const askedForLanguages =
      /\b(english|spanish|bilingual|ingles|espanol|bilingue)\b/.test(
        normalizedPreviousAssistant,
      );
    const askedForTools = /\b(own tools|herramientas)\b/.test(normalizedPreviousAssistant);
    const askedForTransportation =
      /\b(transportation|car|vehicle|truck|transporte|carro|troca|auto propio)\b/.test(
        normalizedPreviousAssistant,
      );
    const askedForFieldReady =
      /\b(outside|field|outdoors|afuera|campo)\b/.test(normalizedPreviousAssistant);
    const askedForExperienceSummary =
      /\b(type of work|sales|customer service|lead follow-up|prospecting|door knocking|tipo de trabajo|ventas|atencion al cliente|seguimiento de leads|prospectando)\b/.test(
        normalizedPreviousAssistant,
      );

    if (entryName) {
      fullName = entryName;
    } else if (!fullName && askedForName && assistantNameLooksReliable(cleanedEntry)) {
      fullName = cleanedEntry;
    }

    if (contactInfo.email) {
      email = contactInfo.email;
    }

    if (contactInfo.phone) {
      phone = contactInfo.phone;
      phoneDisplay = contactInfo.phoneDisplay || phoneDisplay || contactInfo.phone;
    }

    if (entryRole) {
      positionApplied = entryRole;
      roleTrack = inferApplicantRoleTrack(entryRole);
    } else if (!positionApplied && askedForRole && cleanedEntry) {
      positionApplied = cleanText(cleanedEntry, 80);
      roleTrack = inferApplicantRoleTrack(positionApplied);
    }

    if (entryLanguages) {
      languages = entryLanguages;
    } else if (!languages && askedForLanguages && cleanedEntry) {
      languages = cleanText(cleanedEntry, 40);
    }

    if (entryYearsExperience) {
      yearsExperience = entryYearsExperience;
    } else if (!yearsExperience && askedForYearsExperience && cleanedEntry) {
      yearsExperience = cleanText(cleanedEntry, 40);
    }

    if (entryLocation) {
      location = entryLocation;
    }

    if (entryBestDay) {
      bestInterviewDay = entryBestDay;
    }

    if (entryBestTime) {
      bestInterviewTime = entryBestTime;
    }

    if (
      entryExperienceSummary &&
      (!experienceSummary ||
        askedForExperienceSummary ||
        entryExperienceSummary.length > experienceSummary.length)
    ) {
      experienceSummary = entryExperienceSummary;
    }

    if (askedForTools && entryYesNo) {
      hasTools = entryYesNo;
    }

    if (askedForTransportation && entryYesNo) {
      hasTransportation = entryYesNo;
    }

    if (askedForFieldReady && entryYesNo) {
      fieldReady = entryYesNo;
    }
  });

  if (!positionApplied) {
    positionApplied = inferApplicantRole(combinedUserText) || "";
    roleTrack = inferApplicantRoleTrack(positionApplied);
  }

  if (!languages) {
    languages = extractApplicantLanguages(combinedUserText);
  }

  const latestBestInterviewDay = extractAssistantPreferredDay(latestUserMessage);
  const latestBestInterviewTime = extractAssistantPreferredTime(latestUserMessage);
  const storedCalendarDayKey = hasStoredNextActionAt
    ? formatAssistantCalendarDayKey(storedNextActionAt, METALWORKS_CALLBACK_TIME_ZONE)
    : "";
  const bestInterviewDayForResolution =
    latestBestInterviewDay || !storedCalendarDayKey ? bestInterviewDay : storedCalendarDayKey;
  const nextActionAt =
    hasStoredNextActionAt && !latestBestInterviewDay && !latestBestInterviewTime
      ? storedNextActionAt
      : buildAssistantNextActionAt(bestInterviewDayForResolution, bestInterviewTime);
  const normalizedBestInterviewDay =
    nextActionAt instanceof Date && !Number.isNaN(nextActionAt.getTime())
      ? formatAssistantCalendarDayKey(nextActionAt, METALWORKS_CALLBACK_TIME_ZONE)
      : bestInterviewDay;
  const interviewLabel = formatAssistantCallbackLabel({
    nextActionAt,
    bestContactDay: normalizedBestInterviewDay,
    bestContactTime: bestInterviewTime,
  });
  const inSpanish = detectSpanish(combinedUserText || latestUserMessage);
  const baseMissingFields = [
    !positionApplied ? "positionApplied" : "",
    !fullName ? "fullName" : "",
    !phone && !email ? "phoneOrEmail" : "",
  ];
  const trackSpecificMissingFields =
    roleTrack === "trade"
      ? [
          !yearsExperience ? "yearsExperience" : "",
          !experienceSummary ? "experienceSummary" : "",
          !hasTools ? "hasTools" : "",
          !hasTransportation ? "hasTransportation" : "",
          !fieldReady ? "fieldReady" : "",
        ]
      : roleTrack === "sales"
        ? [
            !experienceSummary ? "experienceSummary" : "",
            !hasTransportation ? "hasTransportation" : "",
          ]
        : roleTrack === "prospector"
          ? [
              !experienceSummary ? "experienceSummary" : "",
              !hasTransportation ? "hasTransportation" : "",
              !fieldReady ? "fieldReady" : "",
            ]
          : [];
  const commonMissingFields = [
    !languages ? "languages" : "",
    !normalizedBestInterviewDay ? "bestInterviewDay" : "",
    !bestInterviewTime ? "bestInterviewTime" : "",
  ];
  const applicantMissingFields = [
    ...baseMissingFields,
    ...trackSpecificMissingFields,
    ...commonMissingFields,
  ].filter(Boolean);
  const detailsSummary = [
    positionApplied ? `Role: ${positionApplied}.` : "",
    yearsExperience ? `Experience: ${yearsExperience}.` : "",
    experienceSummary ? `Background: ${experienceSummary}.` : "",
    languages ? `Languages: ${languages}.` : "",
    hasTools ? `Own tools: ${hasTools}.` : "",
    hasTransportation ? `Transportation: ${hasTransportation}.` : "",
    fieldReady ? `Field ready: ${fieldReady}.` : "",
    interviewLabel ? `Interview window: ${interviewLabel}.` : "",
  ]
    .filter(Boolean)
    .join(" ")
    .trim()
    .slice(0, 1500);

  return {
    items,
    userMessages,
    combinedUserText,
    latestUserMessage,
    inSpanish,
    positionApplied,
    roleTrack,
    fullName,
    email,
    phone,
    phoneDisplay,
    languages,
    yearsExperience,
    experienceSummary,
    hasTools,
    hasTransportation,
    fieldReady,
    location,
    bestInterviewDay: normalizedBestInterviewDay,
    bestInterviewTime,
    nextActionAt,
    interviewLabel,
    applicantMissingFields,
    detailsSummary,
    shouldCreateApplicant: Boolean(applicant?._id || phone || email || positionApplied || userMessages.length > 1),
    shouldAlert: Boolean(phone || email) && Boolean(positionApplied || yearsExperience || experienceSummary || languages),
    conversationDigest: buildAssistantHistoryDigest(items),
  };
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
  const inSpanish = detectSpanish(combinedUserText || latestUserMessage);
  const strongProjectIntent =
    detectAssistantProjectLeadIntent(combinedUserText) ||
    detectAssistantProjectLeadIntent(serviceSummarySource) ||
    detectAssistantProjectLeadIntent(projectType) ||
    detectAssistantProjectLeadIntent(location);

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
    bestContactDay: normalizedBestContactDay,
    bestContactTime,
    callbackIntent: callbackIntent === "no" ? "no" : callbackIntent === "yes" ? "yes" : "",
    callbackMissingFields,
    nextActionAt,
    callbackLabel,
    detailsSummary,
    photoFileCount,
    shouldCreateLead: Boolean(
      lead?._id ||
        phone ||
        email ||
        callbackIntent === "yes" ||
        photoFileCount > 0 ||
        strongProjectIntent,
    ),
    shouldAlert: (callbackIntent === "yes" || lead?.callbackIntent === "yes") && Boolean(phone || email),
    conversationDigest: buildAssistantHistoryDigest(items),
  };
}

function buildAssistantStatePrompt(state = {}) {
  const callbackIntent = state?.callbackIntent === "yes" ? "yes" : state?.callbackIntent === "no" ? "no" : "unknown";
  const missingFields = Array.isArray(state?.callbackMissingFields) ? state.callbackMissingFields.join(", ") : "";
  const callbackLabel = cleanText(state?.callbackLabel || "", 120) || "pending";
  const responseChannel = cleanText(state?.sourceChannel || "web", 40) || "web";

  return `
CALL CAPTURE STATE:
- response_channel: ${responseChannel}
- callback_intent: ${callbackIntent}
- visitor_name: ${state?.name || "pending"}
- phone: ${state?.phoneDisplay || state?.phone || "pending"}
- email: ${state?.email || "pending"}
- project_type: ${state?.projectType || "pending"}
- location: ${state?.location || "pending"}
- uploaded_photos: ${Number(state?.photoFileCount || 0) || 0}
- best_day: ${state?.bestContactDay || "pending"}
- best_time: ${state?.bestContactTime || "pending"}
- callback_window: ${callbackLabel}
- missing_callback_fields: ${missingFields || "none"}

INSTRUCTIONS:
- If response_channel is whatsapp, keep replies extra short and text-message friendly.
- If callback_intent is yes and there are missing callback fields, ask only for the missing callback fields in one short message.
- If callback_intent is yes and contact details are already present, confirm the appointment or callback request and ask for photos or ZIP code only if still useful.
- If uploaded_photos is greater than 0, acknowledge the photos are already attached to the lead.
- Do not say the appointment is booked unless the visitor actually gave a specific day and time.
- Keep replies practical, short, and contractor-like.
`;
}

function buildApplicantStatePrompt(state = {}) {
  const responseChannel = cleanText(state?.sourceChannel || "web", 40) || "web";
  const missingFields = Array.isArray(state?.applicantMissingFields)
    ? state.applicantMissingFields.join(", ")
    : "";

  return `
JOB APPLICANT STATE:
- response_channel: ${responseChannel}
- position_applied: ${state?.positionApplied || "pending"}
- role_track: ${state?.roleTrack || "pending"}
- candidate_name: ${state?.fullName || "pending"}
- phone: ${state?.phoneDisplay || state?.phone || "pending"}
- email: ${state?.email || "pending"}
- languages: ${state?.languages || "pending"}
- years_experience: ${state?.yearsExperience || "pending"}
- experience_summary: ${state?.experienceSummary || "pending"}
- has_tools: ${state?.hasTools || "pending"}
- has_transportation: ${state?.hasTransportation || "pending"}
- field_ready: ${state?.fieldReady || "pending"}
- location: ${state?.location || "pending"}
- best_interview_day: ${state?.bestInterviewDay || "pending"}
- best_interview_time: ${state?.bestInterviewTime || "pending"}
- interview_window: ${state?.interviewLabel || "pending"}
- missing_fields: ${missingFields || "none"}

INSTRUCTIONS:
- If response_channel is whatsapp, keep replies extra short and text-message friendly.
- Ask only the next highest-priority missing field unless two tiny fields naturally fit together.
- If position_applied is pending, list the available roles and ask which one they want.
- If the candidate asks about pay, say pay depends on the role and experience, then continue qualification.
- If best_interview_day and best_interview_time are both present, confirm the phone interview window and keep the tone concise.
- Do not promise hiring or a start date.
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

function isAssistantLeadSourceType(value = "") {
  return cleanText(value || "", 80).startsWith("assistant_");
}

function isWebsiteLiveChatLeadSourceType(value = "") {
  return cleanText(value || "", 80) === METALWORKS_WEBSITE_CHAT_SOURCE_TYPE;
}

function isWebsiteLiveChatLead(doc = null) {
  return Boolean(
    doc &&
      (isWebsiteLiveChatLeadSourceType(doc.sourceType || "") ||
        cleanText(doc.sourceExternalSystem || "", 80) === "website_live_chat" ||
        cleanText(doc.pagePath || "", 240).startsWith("/metalworks-chat")),
  );
}

function buildWebsiteLiveChatPlaceholderName(visitorId = "") {
  const suffix = cleanText(visitorId || "", 120)
    .replace(/[^a-z0-9]/gi, "")
    .slice(-4)
    .toUpperCase();

  if (!suffix) {
    return METALWORKS_WEBSITE_CHAT_PLACEHOLDER_NAME;
  }

  return `${METALWORKS_WEBSITE_CHAT_PLACEHOLDER_PREFIX} ${suffix}`;
}

function getAssistantLeadSourceLabel({ sourceType = "", sourceLabel = "" } = {}) {
  const explicitLabel = cleanText(sourceLabel || "", 120);

  if (explicitLabel) {
    return explicitLabel;
  }

  const safeSourceType = cleanText(sourceType || "", 80);
  const sourceLabels = {
    assistant_chat: "Agustin 2.0 website assistant",
    assistant_chat_photo: "Agustin 2.0 website assistant",
    assistant_whatsapp: "Agustin 2.0 WhatsApp assistant",
    assistant_booking: "Agustin 2.0 callback assistant",
  };

  return sourceLabels[safeSourceType] || "Agustin 2.0 website assistant";
}

function buildAssistantNotesStateFromLead(lead = null) {
  if (!lead) {
    return {};
  }

  const nextActionAt =
    lead?.nextActionAt instanceof Date
      ? lead.nextActionAt
      : lead?.nextActionAt
        ? new Date(lead.nextActionAt)
        : null;
  const safeNextActionAt =
    nextActionAt instanceof Date && !Number.isNaN(nextActionAt.getTime()) ? nextActionAt : null;

  return {
    sourceType: cleanText(lead?.sourceType || "", 80),
    sourceLabel: getAssistantLeadSourceLabel({
      sourceType: lead?.sourceType || "",
      sourceLabel: lead?.sourceLabel || "",
    }),
    projectType: cleanText(lead?.projectType || "", 120),
    location: cleanText(lead?.location || "", 160),
    photoFileCount: Array.isArray(lead?.photoFileNames)
      ? lead.photoFileNames.filter(Boolean).length
      : 0,
    callbackIntent: cleanText(lead?.callbackIntent || "", 12),
    callbackLabel: formatAssistantCallbackLabel({
      nextActionAt: safeNextActionAt,
      bestContactDay: cleanText(lead?.bestContactDay || "", 80),
      bestContactTime: cleanText(lead?.bestContactTime || "", 80),
    }),
    detailsSummary: cleanText(lead?.details || "", 1500),
    latestUserMessage: cleanText(lead?.lastUserMessage || "", 500),
    lastAssistantMessage: cleanText(lead?.lastAssistantMessage || "", 1500),
    items: Array.isArray(lead?.conversationHistory) ? lead.conversationHistory : [],
  };
}

function buildAssistantPrivateNotes(state = {}, history = []) {
  const sourceLabel =
    getAssistantLeadSourceLabel({
      sourceType: state?.sourceType || "",
      sourceLabel: state?.sourceLabel || "",
    });
  const conversationSummary = buildAssistantConversationSummary({
    history: Array.isArray(history) && history.length ? history : state?.items || [],
    detailsSummary: state?.detailsSummary || state?.details || "",
    lastUserMessage: state?.latestUserMessage || state?.lastUserMessage || "",
    lastAssistantMessage: state?.lastAssistantMessage || "",
  });

  return [
    `Source: ${sourceLabel}.`,
    state?.projectType ? `Project type: ${state.projectType}.` : "",
    state?.location ? `Location: ${state.location}.` : "",
    state?.photoFileCount ? `Uploaded photos: ${state.photoFileCount}.` : "",
    state?.callbackIntent === "yes" ? "Call or appointment requested: yes." : "",
    state?.callbackIntent === "no" ? "Callback requested: no." : "",
    state?.callbackLabel ? `Best requested time: ${state.callbackLabel}.` : "",
    state?.detailsSummary ? `Summary: ${state.detailsSummary}` : "",
    conversationSummary ? `Conversation recap:\n${conversationSummary}` : "",
  ]
    .filter(Boolean)
    .join("\n")
    .slice(0, 2600);
}

function mergeAssistantPrivateNotes(existingNotes = "", state = {}, history = []) {
  const manualNotes = stripAssistantNotesBlock(existingNotes);
  const generatedNotes = buildAssistantPrivateNotes(state, history);

  if (!generatedNotes) {
    return manualNotes;
  }

  return [manualNotes, `${METALWORKS_ASSISTANT_NOTES_MARKER}\n${generatedNotes}`]
    .filter(Boolean)
    .join("\n\n")
    .trim()
    .slice(0, 4000);
}

function stripApplicantNotesBlock(value = "") {
  const source = String(value || "");
  const markerIndex = source.indexOf(METALWORKS_APPLICANT_NOTES_MARKER);

  if (markerIndex === -1) {
    return source.trim();
  }

  return source.slice(0, markerIndex).trim();
}

function buildApplicantPrivateNotes(state = {}) {
  const sourceLabel =
    cleanText(state?.sourceLabel || "", 120) || "Agustin 2.0 hiring assistant";

  return [
    `Source: ${sourceLabel}.`,
    state?.positionApplied ? `Role: ${state.positionApplied}.` : "",
    state?.languages ? `Languages: ${state.languages}.` : "",
    state?.yearsExperience ? `Years experience: ${state.yearsExperience}.` : "",
    state?.experienceSummary ? `Background: ${state.experienceSummary}.` : "",
    state?.hasTools ? `Own tools: ${state.hasTools}.` : "",
    state?.hasTransportation ? `Transportation: ${state.hasTransportation}.` : "",
    state?.fieldReady ? `Field/outdoor ready: ${state.fieldReady}.` : "",
    state?.interviewLabel ? `Interview window: ${state.interviewLabel}.` : "",
    state?.detailsSummary ? `Summary: ${state.detailsSummary}` : "",
  ]
    .filter(Boolean)
    .join("\n")
    .slice(0, 2600);
}

function mergeApplicantPrivateNotes(existingNotes = "", state = {}) {
  const manualNotes = stripApplicantNotesBlock(existingNotes);
  const generatedNotes = buildApplicantPrivateNotes(state);

  if (!generatedNotes) {
    return manualNotes;
  }

  return [manualNotes, `${METALWORKS_APPLICANT_NOTES_MARKER}\n${generatedNotes}`]
    .filter(Boolean)
    .join("\n\n")
    .trim()
    .slice(0, 4000);
}

function buildApplicantPrivateNotesSeed(applicant = null) {
  if (!applicant) {
    return {};
  }

  const interviewLabel = [
    cleanText(applicant.bestInterviewDay || "", 80),
    cleanText(applicant.bestInterviewTime || "", 80),
  ]
    .filter(Boolean)
    .join(" · ");

  return {
    sourceLabel: cleanText(applicant.sourceLabel || "", 120),
    positionApplied: cleanText(applicant.positionApplied || "", 120),
    languages: cleanText(applicant.languages || "", 160),
    yearsExperience: cleanText(applicant.yearsExperience || "", 80),
    experienceSummary: cleanText(applicant.experienceSummary || "", 1600),
    hasTools: cleanText(applicant.hasTools || "", 40),
    hasTransportation: cleanText(applicant.hasTransportation || "", 40),
    fieldReady: cleanText(applicant.fieldReady || "", 40),
    interviewLabel,
    detailsSummary: cleanText(applicant.detailsSummary || "", 800),
  };
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

function getLeadAssetExtensionForMimeType(mimeType = "") {
  const normalized = normalizeLeadAssetMimeType(mimeType || "");

  if (normalized === "image/png") {
    return ".png";
  }

  if (normalized === "image/webp") {
    return ".webp";
  }

  if (normalized === "image/gif") {
    return ".gif";
  }

  if (normalized === "image/heic") {
    return ".heic";
  }

  if (normalized === "image/heif") {
    return ".heif";
  }

  if (normalized === "image/bmp") {
    return ".bmp";
  }

  return ".jpg";
}

function buildLeadAssetFileNameFromUrl(url = "", mimeType = "", fallbackName = "project-photo") {
  const safeFallbackName = cleanText(fallbackName || "", 80) || "project-photo";

  try {
    const parsedUrl = new URL(String(url || "").trim());
    const candidateName = decodeURIComponent(parsedUrl.pathname.split("/").pop() || "").trim();

    if (candidateName) {
      return sanitizeLeadAssetFileName(candidateName);
    }
  } catch {}

  return sanitizeLeadAssetFileName(
    `${safeFallbackName}${getLeadAssetExtensionForMimeType(mimeType)}`,
  );
}

async function fetchExternalLeadAssetUpload(
  attachment = {},
  { timeoutMs = 12000, fallbackName = "external-photo" } = {},
) {
  const safeUrl = cleanText(attachment?.url || "", 1200);

  if (!/^https?:\/\//i.test(safeUrl)) {
    throw new Error("Attachment URL is invalid.");
  }

  const controller = new AbortController();
  const timeoutHandle = setTimeout(() => controller.abort(), timeoutMs);

  try {
    const response = await fetch(safeUrl, {
      method: "GET",
      redirect: "follow",
      signal: controller.signal,
      headers: {
        Accept: "image/*",
        "User-Agent": "Chicago Metal Works CRM Thumbtack Import",
      },
    });

    if (!response.ok) {
      throw new Error(`Attachment download failed with status ${response.status}.`);
    }

    const responseMimeType = cleanText(
      String(response.headers.get("content-type") || "").split(";")[0] || "",
      80,
    );
    const mimeType = normalizeLeadAssetMimeType(responseMimeType || attachment?.mimeType || "");

    if (!mimeType) {
      throw new Error("Attachment is not a supported image.");
    }

    const arrayBuffer = await response.arrayBuffer();
    const fileData = Buffer.from(arrayBuffer);

    if (!fileData.length) {
      throw new Error("Attachment payload is empty.");
    }

    if (fileData.length > METALWORKS_LEAD_ASSET_MAX_BYTES) {
      throw new Error("Attachment image is larger than 2 MB.");
    }

    return {
      fileName: sanitizeLeadAssetFileName(
        attachment?.fileName || buildLeadAssetFileNameFromUrl(safeUrl, mimeType, fallbackName),
      ),
      mimeType,
      sizeBytes: fileData.length,
      fileData,
    };
  } finally {
    clearTimeout(timeoutHandle);
  }
}

async function fetchExternalLeadAssetUploads(
  attachments = [],
  { limit = METALWORKS_LEAD_ASSET_MAX_FILES, fallbackPrefix = "external-photo" } = {},
) {
  const safeAttachments = Array.isArray(attachments) ? attachments : [];
  const deduped = [];
  const seenUrls = new Set();

  for (const attachment of safeAttachments) {
    const safeUrl = cleanText(attachment?.url || "", 1200);

    if (!safeUrl || seenUrls.has(safeUrl)) {
      continue;
    }

    seenUrls.add(safeUrl);
    deduped.push({
      url: safeUrl,
      fileName: cleanText(attachment?.fileName || "", 120),
      mimeType: cleanText(attachment?.mimeType || "", 80),
    });

    if (deduped.length >= limit) {
      break;
    }
  }

  const parsedFiles = [];
  const errors = [];

  for (let index = 0; index < deduped.length; index += 1) {
    const attachment = deduped[index];

    try {
      const file = await fetchExternalLeadAssetUpload(attachment, {
        fallbackName: `${fallbackPrefix}-${index + 1}`,
      });
      parsedFiles.push(file);
    } catch (error) {
      errors.push({
        url: attachment.url,
        message: cleanText(error?.message || "Attachment import failed.", 240),
      });
    }
  }

  return {
    parsedFiles,
    importedCount: parsedFiles.length,
    attemptedCount: deduped.length,
    skippedCount: Math.max(deduped.length - parsedFiles.length, 0),
    errors,
  };
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
  mode = "customer",
} = {}) {
  const assistantMode = cleanText(mode || "", 40).toLowerCase() === "employment"
    ? "employment"
    : "customer";
  const fallbackReply =
    assistantMode === "employment"
      ? buildEmploymentFallbackReply(message, conversationState)
      : buildAssistantFallbackReply(message, conversationState);
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
            content:
              assistantMode === "employment"
                ? METALWORKS_ASSISTANT_EMPLOYMENT_SYSTEM_PROMPT
                : METALWORKS_ASSISTANT_SYSTEM_PROMPT,
          },
          {
            role: "system",
            content:
              assistantMode === "employment"
                ? buildEmploymentContext(message, pagePath)
                : buildAssistantContext(message, pagePath),
          },
          {
            role: "system",
            content:
              assistantMode === "employment"
                ? buildApplicantStatePrompt(conversationState || {})
                : buildAssistantStatePrompt(conversationState || {}),
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

  const safeFullName =
    sanitizeAssistantStoredName(doc.fullName || "") || cleanText(doc.fullName || "", 120);
  const assistantNotesState =
    includeConversation &&
    (isAssistantLeadSourceType(doc.sourceType || "") ||
      (Array.isArray(doc.conversationHistory) && doc.conversationHistory.length))
      ? buildAssistantNotesStateFromLead(doc)
      : null;
  const composedAssistantNotes = assistantNotesState
    ? mergeAssistantPrivateNotes(doc.privateNotes || "", assistantNotesState, assistantNotesState.items || [])
    : doc.privateNotes || "";
  const conversationSummary = assistantNotesState
    ? buildAssistantConversationSummary({
        history: assistantNotesState.items || [],
        detailsSummary: assistantNotesState.detailsSummary || "",
        lastUserMessage: assistantNotesState.latestUserMessage || "",
        lastAssistantMessage: assistantNotesState.lastAssistantMessage || "",
      })
    : "";

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
    nextActionReminderOffsets: normalizeLeadReminderOffsets(doc.nextActionReminderOffsets || []),
    bestContactDay: doc.bestContactDay || "",
    bestContactTime: doc.bestContactTime || "",
    callbackIntent: doc.callbackIntent || "",
    callbackRequestedAt: doc.callbackRequestedAt
      ? new Date(doc.callbackRequestedAt).toISOString()
      : "",
    callbackAlertedAt: doc.callbackAlertedAt ? new Date(doc.callbackAlertedAt).toISOString() : "",
    privateNotes: composedAssistantNotes,
    conversationSummary,
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
    invoiceDepositAmount: normalizeMoney(doc.invoiceDepositAmount || 0),
    invoiceBalanceDue: Math.max(
      0,
      normalizeMoney((doc.estimateAmount || 0) - (doc.invoiceDepositAmount || 0)),
    ),
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
    supportsLiveChatReply:
      isWebsiteLiveChatLead(doc) &&
      ((Array.isArray(doc.conversationHistory) ? doc.conversationHistory.length > 0 : false) ||
        (Array.isArray(doc.photoFileNames) ? doc.photoFileNames.length > 0 : false)),
    sourceExternalId: doc.sourceExternalId || "",
    sourceExternalSystem: doc.sourceExternalSystem || "",
    tracking: doc.tracking || {},
    createdAt: doc.createdAt ? new Date(doc.createdAt).toISOString() : "",
    updatedAt: doc.updatedAt ? new Date(doc.updatedAt).toISOString() : "",
    lastContactAt: doc.lastContactAt ? new Date(doc.lastContactAt).toISOString() : "",
  };
}

function cleanPublicWebsiteLiveChatThread(doc = null) {
  if (!doc || !isWebsiteLiveChatLead(doc)) {
    return null;
  }

  const safeFullName =
    sanitizeAssistantStoredName(doc.fullName || "") ||
    cleanText(doc.fullName || "", 120) ||
    METALWORKS_WEBSITE_CHAT_PLACEHOLDER_NAME;

  return {
    leadId: String(doc._id || ""),
    threadKey: normalizePublicChatThreadKey(doc.publicChatThreadKey || ""),
    fullName: safeFullName,
    phoneDisplay: doc.phoneDisplay || "",
    email: doc.email || "",
    photoFileNames: Array.isArray(doc.photoFileNames) ? doc.photoFileNames : [],
    updatedAt: doc.updatedAt ? new Date(doc.updatedAt).toISOString() : "",
    createdAt: doc.createdAt ? new Date(doc.createdAt).toISOString() : "",
    messages: (Array.isArray(doc.conversationHistory) ? doc.conversationHistory : [])
      .map((entry) => ({
        role: entry?.role === "assistant" ? "assistant" : "user",
        content: cleanText(entry?.content || "", 1500),
        createdAt: entry?.createdAt ? new Date(entry.createdAt).toISOString() : "",
      }))
      .filter((entry) => entry.content),
  };
}

function cleanApplicant(doc = null, { includeConversation = false } = {}) {
  if (!doc) {
    return null;
  }

  const safeFullName =
    sanitizeAssistantStoredName(doc.fullName || "") || cleanText(doc.fullName || "", 120);

  return {
    id: String(doc._id || ""),
    fullName: safeFullName || METALWORKS_APPLICANT_PLACEHOLDER_NAME,
    phone: doc.phone || "",
    phoneDisplay: doc.phoneDisplay || doc.phone || "",
    email: doc.email || "",
    positionApplied: doc.positionApplied || "",
    roleTrack: doc.roleTrack || "",
    languages: doc.languages || "",
    yearsExperience: doc.yearsExperience || "",
    experienceSummary: doc.experienceSummary || "",
    hasTools: doc.hasTools || "",
    hasTransportation: doc.hasTransportation || "",
    fieldReady: doc.fieldReady || "",
    location: doc.location || "",
    bestInterviewDay: doc.bestInterviewDay || "",
    bestInterviewTime: doc.bestInterviewTime || "",
    status: normalizeApplicantStatus(doc.status || "new"),
    statusLabel: labelApplicantStatus(doc.status || "new"),
    nextAction: doc.nextAction || "",
    nextActionAt: doc.nextActionAt ? new Date(doc.nextActionAt).toISOString() : "",
    interviewRequestedAt: doc.interviewRequestedAt
      ? new Date(doc.interviewRequestedAt).toISOString()
      : "",
    alertSentAt: doc.alertSentAt ? new Date(doc.alertSentAt).toISOString() : "",
    privateNotes: doc.privateNotes || "",
    detailsSummary: doc.detailsSummary || "",
    sourceType: doc.sourceType || "assistant_chat_job",
    sourceChannel: doc.sourceChannel || "",
    sourceLabel: doc.sourceLabel || "",
    pageTitle: doc.pageTitle || "",
    pagePath: doc.pagePath || "",
    pageUrl: doc.pageUrl || "",
    referrer: doc.referrer || "",
    tracking: doc.tracking || {},
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
    applicantId: doc.applicantId ? String(doc.applicantId) : "",
    activityType: doc.activityType || "",
    title: doc.title || formatActivityTitle(doc.activityType || ""),
    body: doc.body || "",
    pagePath: doc.pagePath || "",
    pageUrl: doc.pageUrl || "",
    meta: doc.meta || null,
    createdAt: doc.createdAt ? new Date(doc.createdAt).toISOString() : "",
  };
}

function cleanWebPushDevice(doc = null) {
  if (!doc) {
    return null;
  }

  return {
    id: String(doc._id || ""),
    endpoint: cleanText(doc.endpoint || "", 240),
    platform: cleanText(doc.platform || "web", 40) || "web",
    deviceName: cleanText(doc.deviceName || "", 120),
    browserName: cleanText(doc.browserName || "", 80),
    notificationPath: normalizeMetalworksNotificationPath(
      doc.notificationPath || "/metalworks-crm/operator/",
    ),
    authorizationStatus: cleanText(doc.authorizationStatus || "", 40),
    notificationsEnabled: Boolean(doc.notificationsEnabled),
    isActive: Boolean(doc.isActive),
    lastSeenAt: doc.lastSeenAt ? new Date(doc.lastSeenAt).toISOString() : "",
    lastPushAt: doc.lastPushAt ? new Date(doc.lastPushAt).toISOString() : "",
    lastPushError: cleanText(doc.lastPushError || "", 240),
    createdAt: doc.createdAt ? new Date(doc.createdAt).toISOString() : "",
    updatedAt: doc.updatedAt ? new Date(doc.updatedAt).toISOString() : "",
  };
}

function cleanPublicChatWebPushDevice(doc = null) {
  if (!doc) {
    return null;
  }

  return {
    id: String(doc._id || ""),
    leadId: doc.leadId ? String(doc.leadId) : "",
    visitorId: cleanText(doc.visitorId || "", 120),
    sessionId: cleanText(doc.sessionId || "", 120),
    endpoint: cleanText(doc.endpoint || "", 240),
    platform: cleanText(doc.platform || "web", 40) || "web",
    deviceName: cleanText(doc.deviceName || "", 120),
    browserName: cleanText(doc.browserName || "", 80),
    notificationPath: normalizeMetalworksNotificationPath(
      doc.notificationPath || "/metalworks-chat/",
      "/metalworks-chat/",
    ),
    authorizationStatus: cleanText(doc.authorizationStatus || "", 40),
    notificationsEnabled: Boolean(doc.notificationsEnabled),
    isActive: Boolean(doc.isActive),
    lastSeenAt: doc.lastSeenAt ? new Date(doc.lastSeenAt).toISOString() : "",
    lastPushAt: doc.lastPushAt ? new Date(doc.lastPushAt).toISOString() : "",
    lastPushError: cleanText(doc.lastPushError || "", 240),
    createdAt: doc.createdAt ? new Date(doc.createdAt).toISOString() : "",
    updatedAt: doc.updatedAt ? new Date(doc.updatedAt).toISOString() : "",
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

async function buildDashboardSnapshot(
  MetalworksLead,
  MetalworksLeadActivity,
  MetalworksApplicant = null,
  filters = {},
) {
  const query = buildLeadQuery(filters);
  const statusFilter = normalizeStatus(filters?.status || "");
  const hasLeadStatusFilter = Boolean(
    filters?.status && METALWORKS_CRM_STATUS_OPTIONS.includes(statusFilter),
  );
  const leadQuery = hasLeadStatusFilter
    ? query
    : {
        ...query,
        status: { $nin: ["won", "lost", "archived"] },
      };
  const completedLeadQuery = hasLeadStatusFilter
    ? null
    : {
        ...query,
        status: "won",
      };
  const thirtyDaysAgo = new Date(Date.now() - 30 * 24 * 60 * 60 * 1000);
  const applicantModelAvailable = Boolean(MetalworksApplicant?.find);

  const [
    leads,
    completedLeads,
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
    totalApplicants,
    newApplicants,
    interviewApplicants,
    recentApplicants,
  ] = await Promise.all([
    MetalworksLead.find(leadQuery).sort({ updatedAt: -1, createdAt: -1 }).limit(250).lean(),
    completedLeadQuery
      ? MetalworksLead.find(completedLeadQuery)
          .sort({ updatedAt: -1, createdAt: -1 })
          .limit(60)
          .lean()
      : [],
    MetalworksLeadActivity.find({
      activityType: {
        $nin: [
          "assistant_user_message",
          "assistant_ai_reply",
          "assistant_fallback",
          "website_live_chat_message",
          "website_live_chat_reply",
          "applicant_user_message",
          "applicant_ai_reply",
          "applicant_fallback",
        ],
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
    applicantModelAvailable ? MetalworksApplicant.countDocuments({}) : 0,
    applicantModelAvailable ? MetalworksApplicant.countDocuments({ status: "new" }) : 0,
    applicantModelAvailable
      ? MetalworksApplicant.countDocuments({
          status: { $in: ["interview_requested", "interview_scheduled"] },
        })
      : 0,
    applicantModelAvailable
      ? MetalworksApplicant.find({})
          .sort({ updatedAt: -1, createdAt: -1 })
          .limit(12)
          .lean()
      : [],
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
      totalApplicants: Number(totalApplicants || 0) || 0,
      newApplicants: Number(newApplicants || 0) || 0,
      interviewApplicants: Number(interviewApplicants || 0) || 0,
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
    completedLeads: (Array.isArray(completedLeads) ? completedLeads : [])
      .map((item) => cleanLead(item))
      .filter(Boolean),
    recentApplicants: (Array.isArray(recentApplicants) ? recentApplicants : [])
      .map((item) => cleanApplicant(item))
      .filter(Boolean),
    recentActivity: recentActivity.map(cleanActivity).filter(Boolean),
    statusOptions: METALWORKS_CRM_STATUS_OPTIONS.map((status) => ({
      value: status,
      label: labelStatus(status),
    })),
  };
}

function isMetalworksOperatorOpenStatus(status = "") {
  return !["won", "lost", "archived"].includes(normalizeStatus(status || "new"));
}

function scoreMetalworksOperatorLead(lead = null, now = new Date()) {
  if (!lead?.id) {
    return -9999;
  }

  const status = normalizeStatus(lead.status || "new");

  if (!isMetalworksOperatorOpenStatus(status)) {
    return -9999;
  }

  let score = 0;

  if (status === "new") {
    score += 120;
  } else if (status === "contacted") {
    score += 92;
  } else if (status === "quoted") {
    score += 84;
  } else if (status === "booked") {
    score += 68;
  }

  if (lead.callbackIntent === "yes") {
    score += 38;
  }

  if (lead.estimateAmount) {
    score += 8;
  }

  const lastContactAt = lead.lastContactAt ? new Date(lead.lastContactAt) : null;
  const updatedAt = lead.updatedAt ? new Date(lead.updatedAt) : null;
  const nextActionAt = lead.nextActionAt ? new Date(lead.nextActionAt) : null;

  if (lastContactAt instanceof Date && !Number.isNaN(lastContactAt.getTime())) {
    const hoursSinceLastContact = (now.getTime() - lastContactAt.getTime()) / (60 * 60 * 1000);

    if (hoursSinceLastContact <= 2) {
      score += 10;
    } else if (hoursSinceLastContact <= 24) {
      score += 6;
    }
  } else {
    score += 12;
  }

  if (updatedAt instanceof Date && !Number.isNaN(updatedAt.getTime())) {
    const hoursSinceUpdate = (now.getTime() - updatedAt.getTime()) / (60 * 60 * 1000);

    if (hoursSinceUpdate <= 2) {
      score += 14;
    } else if (hoursSinceUpdate <= 12) {
      score += 10;
    } else if (hoursSinceUpdate <= 48) {
      score += 6;
    }
  }

  if (nextActionAt instanceof Date && !Number.isNaN(nextActionAt.getTime())) {
    const diffHours = (nextActionAt.getTime() - now.getTime()) / (60 * 60 * 1000);

    if (diffHours < 0) {
      score += 70;
    } else if (diffHours <= 6) {
      score += 56;
    } else if (diffHours <= 24) {
      score += 42;
    } else if (diffHours <= 72) {
      score += 24;
    }
  }

  return score;
}

function summarizeLeadForOperator(lead = null) {
  if (!lead?.id) {
    return null;
  }

  return {
    id: lead.id,
    fullName: lead.fullName || "",
    phone: lead.phone || "",
    phoneDisplay: lead.phoneDisplay || lead.phone || "",
    email: lead.email || "",
    projectType: lead.projectType || "",
    location: lead.location || "",
    status: normalizeStatus(lead.status || "new"),
    statusLabel: lead.statusLabel || labelStatus(lead.status || "new"),
    nextAction: lead.nextAction || "",
    nextActionAt: lead.nextActionAt || "",
    nextActionReminderOffsets: normalizeLeadReminderOffsets(lead.nextActionReminderOffsets || []),
    callbackIntent: lead.callbackIntent || "",
    estimateAmount: Number(lead.estimateAmount || 0) || 0,
    sourceType: lead.sourceType || "",
    lastContactAt: lead.lastContactAt || "",
    createdAt: lead.createdAt || "",
    updatedAt: lead.updatedAt || "",
    details: cleanText(lead.details || "", 600),
    lastUserMessage: cleanText(lead.lastUserMessage || "", 240),
    lastAssistantMessage: cleanText(lead.lastAssistantMessage || "", 240),
  };
}

function buildMetalworksOperatorSnapshot(dashboard = {}) {
  const now = new Date();
  const nowTime = now.getTime();
  const leads = Array.isArray(dashboard?.leads) ? dashboard.leads : [];
  const recentActivity = Array.isArray(dashboard?.recentActivity) ? dashboard.recentActivity : [];
  const focusLeads = leads
    .map((lead) => ({
      lead: summarizeLeadForOperator(lead),
      score: scoreMetalworksOperatorLead(lead, now),
    }))
    .filter((item) => item.lead?.id && item.score > -9999)
    .sort((left, right) => {
      if (right.score !== left.score) {
        return right.score - left.score;
      }

      return String(right.lead.updatedAt || "").localeCompare(String(left.lead.updatedAt || ""));
    })
    .slice(0, 10)
    .map((item) => item.lead);
  const agendaLeads = leads
    .filter((lead) => {
      if (!isMetalworksOperatorOpenStatus(lead.status || "new") || !lead.nextActionAt) {
        return false;
      }

      const nextActionAt = new Date(lead.nextActionAt);

      return !Number.isNaN(nextActionAt.getTime()) && nextActionAt.getTime() >= nowTime;
    })
    .sort((left, right) =>
      String(left.nextActionAt || "").localeCompare(String(right.nextActionAt || "")),
    )
    .slice(0, 8)
    .map(summarizeLeadForOperator)
    .filter(Boolean);
  const callbackCount = leads.filter(
    (lead) => {
      if (
        !isMetalworksOperatorOpenStatus(lead.status || "new") ||
        lead.callbackIntent !== "yes" ||
        !lead.nextActionAt
      ) {
        return false;
      }

      const nextActionAt = new Date(lead.nextActionAt);

      return !Number.isNaN(nextActionAt.getTime()) && nextActionAt.getTime() >= nowTime;
    },
  ).length;

  return {
    generatedAt: now.toISOString(),
    summary: {
      totalLeads: Number(dashboard?.summary?.totalLeads || 0) || 0,
      newLeads: Number(dashboard?.summary?.newLeads || 0) || 0,
      totalApplicants: Number(dashboard?.summary?.totalApplicants || 0) || 0,
      newApplicants: Number(dashboard?.summary?.newApplicants || 0) || 0,
      interviewApplicants: Number(dashboard?.summary?.interviewApplicants || 0) || 0,
      activeFollowups:
        (Number(dashboard?.summary?.contactedLeads || 0) || 0) +
        (Number(dashboard?.summary?.quotedLeads || 0) || 0),
      bookedLeads: Number(dashboard?.summary?.bookedLeads || 0) || 0,
      wonLeads: Number(dashboard?.summary?.wonLeads || 0) || 0,
      callbacksScheduled: callbackCount,
      quoteSubmits30d: Number(dashboard?.summary?.quoteSubmits30d || 0) || 0,
      phoneClicks30d: Number(dashboard?.summary?.phoneClicks30d || 0) || 0,
    },
    focusLeads,
    agendaLeads,
    recentActivity: recentActivity.slice(0, 12),
    fullCrmUrl: "/metalworks-crm/",
  };
}

function buildMetalworksDirectWhatsAppUrl() {
  const safePhone = String(process.env.WHATSAPP_CHEF_NUMBER || "12603087201").replace(/\D+/g, "");
  const safeText = cleanText(
    process.env.WHATSAPP_CHEF_TEXT ||
      "Hi, I need a quote for a metal project in Chicago. I can send photos here.",
    240,
  );

  if (!safePhone) {
    return "";
  }

  return safeText
    ? `https://wa.me/${safePhone}?text=${encodeURIComponent(safeText)}`
    : `https://wa.me/${safePhone}`;
}

function buildMetalworksCrmResourceSections() {
  const websiteBase = METALWORKS_WEBSITE_URL.replace(/\/$/, "");
  const directWhatsAppUrl = buildMetalworksDirectWhatsAppUrl();
  const sections = [
    {
      id: "public-links",
      title: "Public Links",
      description: "Pages you share with customers, leads, and prospects.",
      items: [
        {
          id: "website-home",
          label: "Main Website",
          description: "Homepage with quote form and service overview.",
          url: `${websiteBase}/`,
          symbol: "house.fill",
        },
        {
          id: "projects-page",
          label: "Projects Page",
          description: "Portfolio page you can send when someone asks for examples.",
          url: `${websiteBase}/projects.html`,
          symbol: "photo.on.rectangle.angled",
        },
        {
          id: "whatsapp-landing",
          label: "WhatsApp Quote Page",
          description: "Landing page built for ads, Facebook, and fast photo-first leads.",
          url: `${websiteBase}/whatsapp-quote-chicago.html`,
          symbol: "message.fill",
        },
        {
          id: "whatsapp-direct",
          label: "Direct WhatsApp Chat",
          description: "Opens the live WhatsApp conversation with the starter message ready.",
          url: directWhatsAppUrl,
          symbol: "message.circle.fill",
        },
      ],
    },
    {
      id: "crm-links",
      title: "CRM Links",
      description: "Internal tools for office and field teams.",
      items: [
        {
          id: "crm-main",
          label: "Main CRM",
          description: "Lead inbox and applicant pipeline.",
          url: `${websiteBase}/metalworks-crm/`,
          symbol: "rectangle.stack.fill",
        },
        {
          id: "prospector-login",
          label: "Prospector Login",
          description: "Field team login page for prospectors.",
          url: `${websiteBase}/metalworks-crm/prospector/login/`,
          symbol: "person.badge.key.fill",
        },
        {
          id: "prospector-portal",
          label: "Prospector Portal",
          description: "Direct prospecting intake page once the rep is logged in.",
          url: `${websiteBase}/metalworks-crm/prospector/`,
          symbol: "person.2.badge.gearshape.fill",
        },
        {
          id: "lead-distribution",
          label: "Lead Distribution",
          description: "Dedicated lead distribution project page.",
          url: `${websiteBase}/lead-distribution/`,
          symbol: "arrow.triangle.branch",
        },
      ],
    },
    {
      id: "trust-links",
      title: "Trust & Business",
      description: "Useful profile and business links for approvals and sharing.",
      items: [
        {
          id: "thumbtack-profile",
          label: "Thumbtack Profile",
          description: "Public Thumbtack profile and reviews.",
          url: METALWORKS_THUMBTACK_PROFILE_URL,
          symbol: "star.fill",
        },
        {
          id: "privacy-policy",
          label: "Privacy Policy",
          description: "Public privacy page used for integrations and trust.",
          url: `${websiteBase}/privacy.html`,
          symbol: "lock.shield.fill",
        },
        {
          id: "terms-page",
          label: "Terms of Service",
          description: "Public terms page used for approvals and client trust.",
          url: `${websiteBase}/terms.html`,
          symbol: "doc.text.fill",
        },
      ],
    },
  ];

  return sections
    .map((section) => ({
      ...section,
      items: (Array.isArray(section.items) ? section.items : []).filter(
        (item) => cleanText(item?.url || "", 600),
      ),
    }))
    .filter((section) => section.items.length);
}

function buildMetalworksOperatorFallbackReply({
  message = "",
  operatorSnapshot = null,
  selectedLead = null,
} = {}) {
  const normalized = cleanText(message || "", 240).toLowerCase();
  const firstFocus = operatorSnapshot?.focusLeads?.[0] || null;

  if (selectedLead?.id && /text|mensaje|reply|respond/.test(normalized)) {
    const firstName =
      cleanText(selectedLead.fullName || "", 120).split(/\s+/).filter(Boolean)[0] || "there";
    return `Try this: Hi ${firstName}, this is Chicago Metal Works & Fencing. I wanted to follow up on your ${selectedLead.projectType || "project"}. Are you available for a quick call or would you rather text here?`;
  }

  if (selectedLead?.id) {
    return `${selectedLead.fullName || "This lead"} is ${selectedLead.statusLabel || "active"}. Next step: ${selectedLead.nextAction || "call or text the client"}. ${selectedLead.nextActionAt ? `Scheduled for ${selectedLead.nextActionAt}.` : "No follow-up time is set yet."}`;
  }

  if (firstFocus?.id) {
    return `Start with ${firstFocus.fullName || "the top lead"} for ${firstFocus.projectType || "the latest job"}. ${firstFocus.nextAction ? `Next step: ${firstFocus.nextAction}.` : "Call or text first and lock the next step."}`;
  }

  return "Refresh the operator queue and open the most recent lead first. From there, call, text, or save the next follow-up step.";
}

function buildMetalworksOperatorSystemPrompt() {
  return `
You are Agustin Operator, a private mobile copilot for Chicago Metal Works & Fencing.

ROLE:
- Help the owner work from a phone or tablet.
- Prioritize leads, suggest next actions, summarize jobs, and draft short client follow-ups.
- You are not the public website assistant. You are an internal operator copilot.

STYLE:
- Be practical, short, and action-first.
- Keep most replies under 160 words.
- Prefer 1 to 3 concrete next steps.
- If asked for a message draft, write it ready to copy.
- If the answer depends on missing CRM data, say exactly what is missing.

RULES:
- Never claim that you called, texted, saved, or changed a lead unless that action is present in the provided context.
- Use only the CRM snapshot and selected lead context provided in the prompt.
- If there is a selected lead, anchor the answer to that lead first.
- If multiple leads matter, rank them clearly.
`;
}

async function generateMetalworksOperatorReply({
  message = "",
  operatorSnapshot = null,
  selectedLead = null,
  selectedActivity = [],
} = {}) {
  const fallbackReply = buildMetalworksOperatorFallbackReply({
    message,
    operatorSnapshot,
    selectedLead,
  });
  const apiKey = String(process.env.OPENAI_API_KEY || "").trim();

  if (!apiKey) {
    return {
      reply: fallbackReply,
      usedFallback: true,
      reason: "OPENAI_API_KEY missing",
    };
  }

  const contextPayload = {
    summary: operatorSnapshot?.summary || {},
    focusLeads: Array.isArray(operatorSnapshot?.focusLeads)
      ? operatorSnapshot.focusLeads.slice(0, 8)
      : [],
    agendaLeads: Array.isArray(operatorSnapshot?.agendaLeads)
      ? operatorSnapshot.agendaLeads.slice(0, 6)
      : [],
    selectedLead: selectedLead || null,
    selectedActivity: Array.isArray(selectedActivity)
      ? selectedActivity.slice(0, 10).map((item) => ({
          title: item?.title || "",
          body: cleanText(item?.body || "", 240),
          createdAt: item?.createdAt || "",
        }))
      : [],
  };

  try {
    const response = await fetch("https://api.openai.com/v1/responses", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Authorization: `Bearer ${apiKey}`,
      },
      body: JSON.stringify({
        model: "gpt-5-mini",
        max_output_tokens: 240,
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
            content: buildMetalworksOperatorSystemPrompt(),
          },
          {
            role: "system",
            content: `Current CRM context:\n${JSON.stringify(contextPayload, null, 2)}`.slice(
              0,
              12000,
            ),
          },
          {
            role: "user",
            content: cleanText(message || "", 500),
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
      reason: cleanText(error?.message || "Operator chat error", 240),
    };
  }
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

function cleanProspectorUser(doc = null, stats = {}) {
  if (!doc) {
    return null;
  }

  return {
    id: String(doc._id || ""),
    name: cleanText(doc.name || "", 120),
    email: normalizeEmail(doc.email || ""),
    status: normalizeProspectorStatus(doc.status || "active"),
    statusLabel: labelProspectorStatus(doc.status || "active"),
    lastLoginAt: doc.lastLoginAt ? new Date(doc.lastLoginAt).toISOString() : "",
    lastLeadSubmittedAt: doc.lastLeadSubmittedAt
      ? new Date(doc.lastLeadSubmittedAt).toISOString()
      : "",
    createdByAdminEmail: normalizeEmail(doc.createdByAdminEmail || ""),
    createdAt: doc.createdAt ? new Date(doc.createdAt).toISOString() : "",
    updatedAt: doc.updatedAt ? new Date(doc.updatedAt).toISOString() : "",
    counts: {
      totalLeads: Number(stats?.totalLeads || 0) || 0,
      newLeads: Number(stats?.newLeads || 0) || 0,
      quotedLeads: Number(stats?.quotedLeads || 0) || 0,
      bookedLeads: Number(stats?.bookedLeads || 0) || 0,
      wonLeads: Number(stats?.wonLeads || 0) || 0,
    },
  };
}

async function buildProspectorAdminSnapshot(
  MetalworksProspectorUser,
  MetalworksLead,
  { websiteBase = METALWORKS_WEBSITE_URL } = {},
) {
  const prospectorDocs = await MetalworksProspectorUser.find({})
    .sort({ status: 1, name: 1, createdAt: -1 })
    .lean();
  const emails = prospectorDocs
    .map((item) => normalizeEmail(item?.email || ""))
    .filter(Boolean);
  const statsDocs = emails.length
    ? await MetalworksLead.aggregate([
        {
          $match: {
            sourceProspectorEmail: { $in: emails },
          },
        },
        {
          $group: {
            _id: "$sourceProspectorEmail",
            totalLeads: { $sum: 1 },
            newLeads: {
              $sum: {
                $cond: [{ $eq: ["$status", "new"] }, 1, 0],
              },
            },
            quotedLeads: {
              $sum: {
                $cond: [{ $eq: ["$status", "quoted"] }, 1, 0],
              },
            },
            bookedLeads: {
              $sum: {
                $cond: [{ $eq: ["$status", "booked"] }, 1, 0],
              },
            },
            wonLeads: {
              $sum: {
                $cond: [{ $eq: ["$status", "won"] }, 1, 0],
              },
            },
          },
        },
      ])
    : [];
  const statsMap = new Map(
    statsDocs.map((item) => [normalizeEmail(item?._id || ""), item || {}]),
  );
  const prospectors = prospectorDocs
    .map((doc) => cleanProspectorUser(doc, statsMap.get(normalizeEmail(doc?.email || "")) || {}))
    .filter(Boolean);

  return {
    summary: {
      totalProspectors: prospectors.length,
      activeProspectors: prospectors.filter((item) => item.status === "active").length,
      pausedProspectors: prospectors.filter((item) => item.status === "paused").length,
      totalLeads: prospectors.reduce(
        (sum, item) => sum + Number(item?.counts?.totalLeads || 0),
        0,
      ),
    },
    loginUrl: `${String(websiteBase || METALWORKS_WEBSITE_URL).replace(/\/$/, "")}/metalworks-crm/prospector/login/`,
    portalUrl: `${String(websiteBase || METALWORKS_WEBSITE_URL).replace(/\/$/, "")}/metalworks-crm/prospector/`,
    prospectors,
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
    clientSubmissionId: { type: String, index: true },
    details: String,
    photoFileNames: [String],
    status: { type: String, default: "new", index: true },
    nextAction: String,
    nextActionAt: Date,
    nextActionReminderOffsets: { type: [Number], default: [] },
    nextActionReminderSentKeys: { type: [String], default: [] },
    privateNotes: String,
    estimateTitle: String,
    estimateScope: String,
    estimateMaterialsCost: { type: Number, default: 0 },
    estimateLaborCost: { type: Number, default: 0 },
    estimateCoatingCost: { type: Number, default: 0 },
    estimateMiscCost: { type: Number, default: 0 },
    estimateDiscount: { type: Number, default: 0 },
    estimateAmount: { type: Number, default: 0 },
    invoiceDepositAmount: { type: Number, default: 0 },
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
    publicChatThreadKey: { type: String, index: true },
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

  const metalworksApplicantSchema = new mongoose.Schema({
    fullName: { type: String, required: true, trim: true, index: true },
    phone: { type: String, index: true },
    phoneDisplay: String,
    email: { type: String, index: true },
    positionApplied: String,
    roleTrack: String,
    languages: String,
    yearsExperience: String,
    experienceSummary: String,
    hasTools: String,
    hasTransportation: String,
    fieldReady: String,
    location: String,
    bestInterviewDay: String,
    bestInterviewTime: String,
    status: { type: String, default: "new", index: true },
    nextAction: String,
    nextActionAt: Date,
    interviewRequestedAt: Date,
    alertSentAt: Date,
    privateNotes: String,
    detailsSummary: String,
    sourceType: { type: String, default: "assistant_chat_job", index: true },
    sourceChannel: String,
    sourceLabel: String,
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
    applicantId: {
      type: mongoose.Schema.Types.ObjectId,
      ref: "MetalworksApplicant",
      default: null,
      index: true,
    },
    activityType: { type: String, default: "lead_updated", index: true },
    externalEventKey: { type: String, index: true },
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
  const metalworksProspectorUserSchema = new mongoose.Schema({
    name: { type: String, required: true },
    email: { type: String, required: true, unique: true, index: true },
    passwordHash: { type: String, required: true },
    passwordSalt: { type: String, required: true },
    status: { type: String, default: "active", index: true },
    createdByAdminEmail: String,
    lastLoginAt: Date,
    lastLeadSubmittedAt: Date,
    updatedAt: { type: Date, default: Date.now },
    createdAt: { type: Date, default: Date.now },
  });
  const metalworksProspectorSessionSchema = new mongoose.Schema({
    prospectorUserId: {
      type: mongoose.Schema.Types.ObjectId,
      ref: "MetalworksProspectorUser",
      required: true,
      index: true,
    },
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
  const metalworksCrmWebPushDeviceSchema = new mongoose.Schema({
    adminEmail: { type: String, required: true, index: true },
    endpoint: { type: String, required: true, unique: true, index: true },
    platform: { type: String, default: "web" },
    browserName: String,
    deviceName: String,
    notificationPath: { type: String, default: "/metalworks-crm/operator/" },
    authorizationStatus: String,
    subscription: { type: mongoose.Schema.Types.Mixed, required: true },
    vapidPublicKey: String,
    notificationsEnabled: { type: Boolean, default: false },
    isActive: { type: Boolean, default: true, index: true },
    lastSeenAt: { type: Date, default: Date.now, index: true },
    lastPushAt: Date,
    lastPushError: String,
    ipAddress: String,
    userAgent: String,
    createdAt: { type: Date, default: Date.now },
    updatedAt: { type: Date, default: Date.now },
  });
  const metalworksPublicChatWebPushDeviceSchema = new mongoose.Schema({
    endpoint: { type: String, required: true, unique: true, index: true },
    leadId: {
      type: mongoose.Schema.Types.ObjectId,
      ref: "MetalworksLead",
      default: null,
      index: true,
    },
    visitorId: { type: String, index: true },
    sessionId: { type: String, index: true },
    platform: { type: String, default: "web" },
    browserName: String,
    deviceName: String,
    notificationPath: { type: String, default: "/metalworks-chat/" },
    authorizationStatus: String,
    subscription: { type: mongoose.Schema.Types.Mixed, required: true },
    vapidPublicKey: String,
    notificationsEnabled: { type: Boolean, default: false },
    isActive: { type: Boolean, default: true, index: true },
    lastSeenAt: { type: Date, default: Date.now, index: true },
    lastPushAt: Date,
    lastPushError: String,
    ipAddress: String,
    userAgent: String,
    createdAt: { type: Date, default: Date.now },
    updatedAt: { type: Date, default: Date.now },
  });
  const metalworksExternalLeadLockSchema = new mongoose.Schema({
    key: { type: String, required: true, unique: true, index: true },
    expiresAt: { type: Date, required: true, index: true },
    createdAt: { type: Date, default: Date.now },
    updatedAt: { type: Date, default: Date.now },
  });

  metalworksLeadSchema.index({ createdAt: -1 });
  metalworksLeadSchema.index({ updatedAt: -1 });
  metalworksLeadSchema.index({ status: 1, updatedAt: -1 });
  metalworksLeadSchema.index({ publicChatThreadKey: 1 });
  metalworksLeadSchema.index({ visitorIds: 1 });
  metalworksLeadSchema.index({ sessionIds: 1 });
  metalworksApplicantSchema.index({ createdAt: -1 });
  metalworksApplicantSchema.index({ updatedAt: -1 });
  metalworksApplicantSchema.index({ status: 1, updatedAt: -1 });
  metalworksApplicantSchema.index({ visitorIds: 1 });
  metalworksApplicantSchema.index({ sessionIds: 1 });
  metalworksLeadActivitySchema.index({ leadId: 1, createdAt: -1 });
  metalworksLeadActivitySchema.index({ applicantId: 1, createdAt: -1 });
  metalworksLeadActivitySchema.index({ activityType: 1, createdAt: -1 });
  metalworksLeadActivitySchema.index({ externalEventKey: 1, createdAt: -1 });
  metalworksLeadAssetSchema.index({ leadId: 1, uploadedAt: -1 });
  metalworksLeadAssetSchema.index({ visitorId: 1, uploadedAt: -1 });
  metalworksLeadAssetSchema.index({ sessionId: 1, uploadedAt: -1 });
  metalworksCrmSessionSchema.index({ expiresAt: 1 }, { expireAfterSeconds: 0 });
  metalworksProspectorUserSchema.index({ status: 1, name: 1 });
  metalworksProspectorUserSchema.index({ createdAt: -1 });
  metalworksProspectorSessionSchema.index({ expiresAt: 1 }, { expireAfterSeconds: 0 });
  metalworksProspectorSessionSchema.index({ prospectorUserId: 1, lastSeenAt: -1 });
  metalworksProspectorSessionSchema.index({ prospectorEmail: 1, lastSeenAt: -1 });
  metalworksCrmPushDeviceSchema.index({ adminEmail: 1, lastSeenAt: -1 });
  metalworksCrmPushDeviceSchema.index({ isActive: 1, lastSeenAt: -1 });
  metalworksCrmWebPushDeviceSchema.index({ adminEmail: 1, lastSeenAt: -1 });
  metalworksCrmWebPushDeviceSchema.index({ isActive: 1, lastSeenAt: -1 });
  metalworksPublicChatWebPushDeviceSchema.index({ leadId: 1, lastSeenAt: -1 });
  metalworksPublicChatWebPushDeviceSchema.index({ visitorId: 1, lastSeenAt: -1 });
  metalworksPublicChatWebPushDeviceSchema.index({ sessionId: 1, lastSeenAt: -1 });
  metalworksPublicChatWebPushDeviceSchema.index({ isActive: 1, lastSeenAt: -1 });
  metalworksExternalLeadLockSchema.index({ expiresAt: 1 }, { expireAfterSeconds: 0 });

  const MetalworksLead =
    mongoose.models.MetalworksLead ||
    mongoose.model("MetalworksLead", metalworksLeadSchema);
  const MetalworksApplicant =
    mongoose.models.MetalworksApplicant ||
    mongoose.model("MetalworksApplicant", metalworksApplicantSchema);
  const MetalworksLeadActivity =
    mongoose.models.MetalworksLeadActivity ||
    mongoose.model("MetalworksLeadActivity", metalworksLeadActivitySchema);
  const MetalworksLeadAsset =
    mongoose.models.MetalworksLeadAsset ||
    mongoose.model("MetalworksLeadAsset", metalworksLeadAssetSchema);
  const MetalworksCrmSession =
    mongoose.models.MetalworksCrmSession ||
    mongoose.model("MetalworksCrmSession", metalworksCrmSessionSchema);
  const MetalworksProspectorUser =
    mongoose.models.MetalworksProspectorUser ||
    mongoose.model("MetalworksProspectorUser", metalworksProspectorUserSchema);
  const MetalworksProspectorSession =
    mongoose.models.MetalworksProspectorSession ||
    mongoose.model("MetalworksProspectorSession", metalworksProspectorSessionSchema);
  const MetalworksCrmPushDevice =
    mongoose.models.MetalworksCrmPushDevice ||
    mongoose.model("MetalworksCrmPushDevice", metalworksCrmPushDeviceSchema);
  const MetalworksCrmWebPushDevice =
    mongoose.models.MetalworksCrmWebPushDevice ||
    mongoose.model("MetalworksCrmWebPushDevice", metalworksCrmWebPushDeviceSchema);
  const MetalworksPublicChatWebPushDevice =
    mongoose.models.MetalworksPublicChatWebPushDevice ||
    mongoose.model("MetalworksPublicChatWebPushDevice", metalworksPublicChatWebPushDeviceSchema);
  const MetalworksExternalLeadLock =
    mongoose.models.MetalworksExternalLeadLock ||
    mongoose.model("MetalworksExternalLeadLock", metalworksExternalLeadLockSchema);

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

  function setPublicChatThreadCookie(res, req, threadKey) {
    const safeThreadKey = normalizePublicChatThreadKey(threadKey);

    if (!safeThreadKey) {
      return;
    }

    res.cookie(METALWORKS_PUBLIC_CHAT_THREAD_COOKIE, safeThreadKey, {
      httpOnly: true,
      sameSite: "lax",
      secure: requestIsSecure(req),
      path: "/",
      maxAge: METALWORKS_PUBLIC_CHAT_THREAD_DAYS * 24 * 60 * 60 * 1000,
    });
  }

  function getPublicChatThreadKey(req) {
    const bodyThreadKey = normalizePublicChatThreadKey(
      req.body?.threadKey || req.query?.thread || req.query?.t || "",
    );

    if (bodyThreadKey) {
      return bodyThreadKey;
    }

    const cookies = parseCookies(req.headers.cookie || "");
    return normalizePublicChatThreadKey(cookies[METALWORKS_PUBLIC_CHAT_THREAD_COOKIE] || "");
  }

  function ensureWebsiteLiveChatThreadKey(leadDoc = null, fallbackThreadKey = "") {
    if (!leadDoc) {
      return "";
    }

    const currentThreadKey = normalizePublicChatThreadKey(leadDoc.publicChatThreadKey || "");

    if (currentThreadKey) {
      leadDoc.publicChatThreadKey = currentThreadKey;
      return currentThreadKey;
    }

    const nextThreadKey = normalizePublicChatThreadKey(fallbackThreadKey || "") || generateToken();
    leadDoc.publicChatThreadKey = nextThreadKey;
    return nextThreadKey;
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
      return { session: null, userId: "", name: "", email: "", status: "" };
    }

    const session = await MetalworksProspectorSession.findOne({
      tokenHash: hashToken(token),
      expiresAt: { $gt: new Date() },
    }).lean();

    if (!session?.prospectorEmail) {
      return { session: null, userId: "", name: "", email: "", status: "" };
    }

    const prospectorUser =
      (session?.prospectorUserId
        ? await MetalworksProspectorUser.findById(session.prospectorUserId).lean()
        : null) ||
      (session?.prospectorEmail
        ? await MetalworksProspectorUser.findOne({
            email: normalizeEmail(session.prospectorEmail || ""),
          }).lean()
        : null);

    if (
      !prospectorUser?.email ||
      normalizeProspectorStatus(prospectorUser.status || "active") !== "active"
    ) {
      await MetalworksProspectorSession.deleteOne({ _id: session._id }).catch(() => null);
      return { session: null, userId: "", name: "", email: "", status: "" };
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
      userId: String(prospectorUser._id || ""),
      name: cleanText(prospectorUser.name || session.prospectorName || "", 120),
      email: normalizeEmail(prospectorUser.email || session.prospectorEmail || ""),
      status: normalizeProspectorStatus(prospectorUser.status || "active"),
    };
  }

  async function requireProspectorAuth(req, res) {
    const auth = await getProspectorAuth(req);

    if (!auth.email || !auth.name) {
      respondError(res, 401, "You need to sign in as a prospector.");
      return null;
    }

    return auth;
  }

  async function createProspectorSession(req, res, { prospectorUser = null } = {}) {
    const safeUserId = String(prospectorUser?._id || "").trim();
    const safeName = cleanText(prospectorUser?.name || "", 120);
    const safeEmail = normalizeEmail(prospectorUser?.email || "");

    if (!safeUserId || !safeName || !safeEmail) {
      throw new Error("Prospector account is incomplete.");
    }

    const token = generateToken();
    const expiresAt = new Date(
      Date.now() + METALWORKS_PROSPECTOR_SESSION_DAYS * 24 * 60 * 60 * 1000,
    );

    await MetalworksProspectorSession.create({
      prospectorUserId: prospectorUser._id,
      prospectorName: safeName,
      prospectorEmail: safeEmail,
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
    applicant = null,
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

    const [deviceDocs, webDeviceDocs] = await Promise.all([
      MetalworksCrmPushDevice.find(query)
        .sort({ lastSeenAt: -1, updatedAt: -1 })
        .limit(adminEmail ? 8 : 24)
        .lean(),
      MetalworksCrmWebPushDevice.find(query)
        .sort({ lastSeenAt: -1, updatedAt: -1 })
        .limit(adminEmail ? 8 : 24)
        .lean(),
    ]);

    if (!deviceDocs.length && !webDeviceDocs.length) {
      return {
        attempted: false,
        delivered: false,
        deliveredCount: 0,
        deviceCount: 0,
        error: "No active push devices are registered for lead alerts yet.",
      };
    }

    const copy = buildMetalworksPushCopy({
      lead,
      applicant,
      alertType,
      requestedAtLabel,
    });
    const apnsResults = await Promise.all(
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
    const webResults = await Promise.all(
      webDeviceDocs.map(async (device) => {
        const result = await sendMetalworksWebPushNotification({
          subscription: device.subscription,
          alertType,
          title: copy.title,
          body: copy.body,
          leadId: lead?._id ? String(lead._id) : "",
          notificationPath: normalizeMetalworksNotificationPath(
            device.notificationPath || "/metalworks-crm/operator/",
          ),
        });
        const update = {
          lastSeenAt: new Date(),
          updatedAt: new Date(),
          lastPushAt: result.delivered ? new Date() : device.lastPushAt || null,
          lastPushError: result.delivered ? "" : cleanText(result.error || "", 240),
        };

        if ([404, 410].includes(Number(result.status || 0))) {
          update.isActive = false;
          update.notificationsEnabled = false;
        }

        await MetalworksCrmWebPushDevice.updateOne(
          { _id: device._id },
          {
            $set: update,
          },
        );

        return result;
      }),
    );
    const results = [...apnsResults, ...webResults];
    const deliveredCount = results.filter((item) => item.delivered).length;
    const firstError = results.find((item) => item.error)?.error || "";

    return {
      attempted: true,
      delivered: deliveredCount > 0,
      deliveredCount,
      deviceCount: deviceDocs.length + webDeviceDocs.length,
      error: firstError,
    };
  }

  async function processMetalworksLeadReminders() {
    if (METALWORKS_LEAD_REMINDER_WORKER.running) {
      return;
    }

    METALWORKS_LEAD_REMINDER_WORKER.running = true;

    try {
      const now = new Date();
      const candidateLeads = await MetalworksLead.find({
        status: { $nin: ["won", "lost", "archived"] },
        nextActionAt: {
          $gte: new Date(now.getTime() - METALWORKS_LEAD_REMINDER_GRACE_MS),
          $lte: new Date(now.getTime() + METALWORKS_LEAD_REMINDER_SCAN_WINDOW_MS),
        },
        nextActionReminderOffsets: { $exists: true, $ne: [] },
      })
        .sort({ nextActionAt: 1, updatedAt: -1 })
        .limit(80);

      for (const leadDoc of candidateLeads) {
        if (!isMetalworksOperatorOpenStatus(leadDoc.status || "new")) {
          continue;
        }

        const dueReminders = collectDueLeadReminderOffsets({
          nextActionAt: leadDoc.nextActionAt,
          reminderOffsets: leadDoc.nextActionReminderOffsets || [],
          sentKeys: leadDoc.nextActionReminderSentKeys || [],
          now,
        });

        if (!dueReminders.length) {
          continue;
        }

        const scheduleLabel = leadDoc.nextActionAt
          ? formatDateTimeLabel(leadDoc.nextActionAt, METALWORKS_CALLBACK_TIME_ZONE)
          : "";
        const deliveredReminderKeys = [];

        for (const reminder of dueReminders) {
          let delivery = null;

          try {
            delivery = await sendMetalworksPushAlert({
              lead: leadDoc,
              alertType: "lead_followup_reminder",
              requestedAtLabel: [reminder.label, scheduleLabel].filter(Boolean).join(" • "),
            });
          } catch (error) {
            console.error("Error sending Metal Works lead reminder push:", error.message);
            continue;
          }

          if (!delivery?.delivered) {
            continue;
          }

          deliveredReminderKeys.push(reminder.key);

          await appendActivity({
            leadId: leadDoc._id,
            activityType: "lead_followup_reminder_sent",
            title: "Reminder push sent",
            body: [reminder.label, scheduleLabel].filter(Boolean).join(" • "),
          }).catch(() => null);
        }

        if (!deliveredReminderKeys.length) {
          continue;
        }

        leadDoc.nextActionReminderSentKeys = Array.from(
          new Set([
            ...pruneLeadReminderSentKeys(
              leadDoc.nextActionReminderSentKeys || [],
              leadDoc.nextActionAt,
              leadDoc.nextActionReminderOffsets || [],
            ),
            ...deliveredReminderKeys,
          ]),
        );
        leadDoc.updatedAt = new Date();
        await leadDoc.save();
      }
    } finally {
      METALWORKS_LEAD_REMINDER_WORKER.running = false;
    }
  }

  function startMetalworksLeadReminderWorker() {
    if (METALWORKS_LEAD_REMINDER_WORKER.started) {
      return;
    }

    METALWORKS_LEAD_REMINDER_WORKER.started = true;
    const tick = async () => {
      try {
        await processMetalworksLeadReminders();
      } catch (error) {
        console.error("Error processing Metal Works lead reminders:", error.message);
      }
    };

    METALWORKS_LEAD_REMINDER_WORKER.timer = setInterval(
      tick,
      METALWORKS_LEAD_REMINDER_POLL_MS,
    );

    if (typeof METALWORKS_LEAD_REMINDER_WORKER.timer?.unref === "function") {
      METALWORKS_LEAD_REMINDER_WORKER.timer.unref();
    }

    void tick();
  }

  startMetalworksLeadReminderWorker();
  void repairExistingExternalLeadDuplicates({ externalSystem: "thumbtack" });
  void repairUnlinkedThumbtackReviewActivities();
  void repairScheduledReminderDrift();

  async function withExternalLeadLock(lockKey = "", task = async () => null) {
    const safeLockKey = cleanText(lockKey || "", 200);

    if (!safeLockKey) {
      return await task();
    }

    let acquired = false;

    try {
      for (let attempt = 0; attempt < METALWORKS_EXTERNAL_LOCK_MAX_ATTEMPTS; attempt += 1) {
        try {
          await MetalworksExternalLeadLock.create({
            key: safeLockKey,
            expiresAt: new Date(Date.now() + METALWORKS_EXTERNAL_LOCK_TTL_MS),
            createdAt: new Date(),
            updatedAt: new Date(),
          });
          acquired = true;
          break;
        } catch (error) {
          if (error?.code !== 11000) {
            throw error;
          }

          await MetalworksExternalLeadLock.deleteMany({
            key: safeLockKey,
            expiresAt: { $lte: new Date() },
          }).catch(() => null);
          await waitMs(METALWORKS_EXTERNAL_LOCK_RETRY_MS + attempt * 30);
        }
      }

      if (!acquired) {
        throw createRequestError(503, "Timed out waiting for the external lead lock.");
      }

      return await task();
    } finally {
      if (acquired) {
        await MetalworksExternalLeadLock.deleteOne({ key: safeLockKey }).catch(() => null);
      }
    }
  }

  async function mergeDuplicateExternalLeadsByKey({
    externalSystem = "",
    externalLeadId = "",
  } = {}) {
    const safeExternalSystem = cleanText(externalSystem || "", 80);
    const safeExternalLeadId = cleanText(externalLeadId || "", 120);

    if (!safeExternalSystem || !safeExternalLeadId) {
      return null;
    }

    const leadDocs = await MetalworksLead.find({
      sourceExternalSystem: safeExternalSystem,
      sourceExternalId: safeExternalLeadId,
    })
      .sort({ updatedAt: -1, createdAt: -1 })
      .limit(20);

    if (leadDocs.length <= 1) {
      return leadDocs[0] || null;
    }

    const leadIds = leadDocs.map((doc) => doc._id);
    const [activityCounts, assetCounts] = await Promise.all([
      MetalworksLeadActivity.aggregate([
        { $match: { leadId: { $in: leadIds } } },
        { $group: { _id: "$leadId", count: { $sum: 1 } } },
      ]),
      MetalworksLeadAsset.aggregate([
        { $match: { leadId: { $in: leadIds } } },
        { $group: { _id: "$leadId", count: { $sum: 1 } } },
      ]),
    ]);
    const activityCountMap = new Map(activityCounts.map((item) => [String(item._id || ""), Number(item.count || 0)]));
    const assetCountMap = new Map(assetCounts.map((item) => [String(item._id || ""), Number(item.count || 0)]));
    const statusRank = {
      won: 7,
      booked: 6,
      quoted: 5,
      contacted: 4,
      new: 3,
      lost: 2,
      archived: 1,
    };
    const genericProjectTypes = new Set(["thumbtack lead", "thumbtack conversation"]);
    const scoredLeads = leadDocs
      .map((doc) => {
        const docId = String(doc._id || "");
        const activityCount = activityCountMap.get(docId) || 0;
        const assetCount = assetCountMap.get(docId) || 0;
        const status = normalizeStatus(doc.status || "new");
        const projectType = cleanText(doc.projectType || "", 120).toLowerCase();
        const projectTypeBonus =
          projectType && !genericProjectTypes.has(projectType) ? 30 : 0;
        const sourceBonus = String(doc.sourceType || "").includes("message") ? 40 : 0;
        const score =
          activityCount * 100 +
          assetCount * 30 +
          (statusRank[status] || 0) * 20 +
          projectTypeBonus +
          sourceBonus +
          (doc.updatedAt ? new Date(doc.updatedAt).getTime() / 1e11 : 0);

        return { doc, score };
      })
      .sort((left, right) => right.score - left.score);

    const primaryDoc = scoredLeads[0]?.doc || leadDocs[0];
    const duplicateDocs = leadDocs.filter((doc) => String(doc._id || "") !== String(primaryDoc._id || ""));

    const pickBestText = (...values) =>
      values
        .map((value) => cleanText(value || "", 3000))
        .filter(Boolean)
        .sort((left, right) => right.length - left.length)[0] || "";
    const pickBestProjectType = (...values) =>
      values
        .map((value) => cleanText(value || "", 120))
        .filter(Boolean)
        .sort((left, right) => {
          const leftGeneric = genericProjectTypes.has(left.toLowerCase());
          const rightGeneric = genericProjectTypes.has(right.toLowerCase());

          if (leftGeneric !== rightGeneric) {
            return leftGeneric ? 1 : -1;
          }

          return right.length - left.length;
        })[0] || "";

    primaryDoc.fullName = pickBestText(...leadDocs.map((doc) => doc.fullName || "")).slice(0, 120) || primaryDoc.fullName;
    primaryDoc.phone =
      leadDocs.map((doc) => normalizePhone(doc.phone || doc.phoneDisplay || "")).find(Boolean) ||
      primaryDoc.phone ||
      "";
    primaryDoc.phoneDisplay =
      leadDocs.map((doc) => cleanText(doc.phoneDisplay || doc.phone || "", 40)).find(Boolean) ||
      primaryDoc.phoneDisplay ||
      primaryDoc.phone ||
      "";
    primaryDoc.email =
      leadDocs.map((doc) => normalizeEmail(doc.email || "")).find(Boolean) || primaryDoc.email || "";
    primaryDoc.projectType =
      pickBestProjectType(...leadDocs.map((doc) => doc.projectType || "")) || primaryDoc.projectType || "";
    primaryDoc.location =
      pickBestText(...leadDocs.map((doc) => doc.location || "")).slice(0, 160) || primaryDoc.location || "";
    primaryDoc.addressLine =
      pickBestText(...leadDocs.map((doc) => doc.addressLine || "")).slice(0, 160) ||
      primaryDoc.addressLine ||
      "";
    primaryDoc.zipCode =
      leadDocs.map((doc) => cleanText(doc.zipCode || "", 20)).find(Boolean) || primaryDoc.zipCode || "";
    primaryDoc.city =
      leadDocs.map((doc) => cleanText(doc.city || "", 120)).find(Boolean) || primaryDoc.city || "";
    primaryDoc.details =
      pickBestText(...leadDocs.map((doc) => doc.details || "")) || primaryDoc.details || "";
    primaryDoc.photoFileNames = mergeAssistantUniqueValues(
      ...leadDocs.map((doc) => (Array.isArray(doc.photoFileNames) ? doc.photoFileNames : [])),
    ).slice(0, 20);
    primaryDoc.lastUserMessage =
      pickBestText(...leadDocs.map((doc) => doc.lastUserMessage || "")).slice(0, 500) ||
      primaryDoc.lastUserMessage ||
      "";
    primaryDoc.lastAssistantMessage =
      pickBestText(...leadDocs.map((doc) => doc.lastAssistantMessage || "")).slice(0, 1500) ||
      primaryDoc.lastAssistantMessage ||
      "";
    primaryDoc.status = leadDocs
      .map((doc) => normalizeStatus(doc.status || "new"))
      .sort((left, right) => (statusRank[right] || 0) - (statusRank[left] || 0))[0] || primaryDoc.status;
    primaryDoc.createdAt = leadDocs
      .map((doc) => (doc.createdAt ? new Date(doc.createdAt) : null))
      .filter(Boolean)
      .sort((left, right) => left.getTime() - right.getTime())[0] || primaryDoc.createdAt;
    primaryDoc.updatedAt = new Date();
    await primaryDoc.save();

    const duplicateIds = duplicateDocs.map((doc) => doc._id);

    if (duplicateIds.length) {
      await Promise.all([
        MetalworksLeadActivity.updateMany(
          { leadId: { $in: duplicateIds } },
          { $set: { leadId: primaryDoc._id } },
        ),
        MetalworksLeadAsset.updateMany(
          { leadId: { $in: duplicateIds } },
          { $set: { leadId: primaryDoc._id, updatedAt: new Date() } },
        ),
        MetalworksPublicChatWebPushDevice.updateMany(
          { leadId: { $in: duplicateIds } },
          { $set: { leadId: primaryDoc._id, updatedAt: new Date() } },
        ),
      ]);

      await MetalworksLead.deleteMany({ _id: { $in: duplicateIds } });
    }

    return primaryDoc;
  }

  async function findThumbtackLeadByNegotiationId(negotiationId = "") {
    const safeNegotiationId = cleanText(negotiationId || "", 120);

    if (!safeNegotiationId) {
      return null;
    }

    return (
      (await mergeDuplicateExternalLeadsByKey({
        externalSystem: "thumbtack",
        externalLeadId: safeNegotiationId,
      })) ||
      (await MetalworksLead.findOne({
        sourceExternalSystem: "thumbtack",
        sourceExternalId: safeNegotiationId,
      }).sort({ updatedAt: -1, createdAt: -1 }))
    );
  }

  function getThumbtackReviewNegotiationIdFromActivity(activityDoc = null) {
    return cleanText(
      activityDoc?.meta?.negotiationId ||
        activityDoc?.meta?.webhookPayload?.data?.negotiationID ||
        activityDoc?.meta?.webhookPayload?.data?.negotiationId ||
        activityDoc?.meta?.webhookPayload?.review?.negotiationID ||
        activityDoc?.meta?.webhookPayload?.review?.negotiationId ||
        "",
      120,
    );
  }

  async function repairExistingExternalLeadDuplicates({ externalSystem = "" } = {}) {
    const match = {
      sourceExternalId: { $exists: true, $ne: "" },
    };

    if (externalSystem) {
      match.sourceExternalSystem = cleanText(externalSystem || "", 80);
    }

    const groups = await MetalworksLead.aggregate([
      { $match: match },
      {
        $group: {
          _id: {
            sourceExternalSystem: "$sourceExternalSystem",
            sourceExternalId: "$sourceExternalId",
          },
          count: { $sum: 1 },
        },
      },
      { $match: { count: { $gt: 1 } } },
      { $limit: 50 },
    ]);

    for (const group of groups) {
      await mergeDuplicateExternalLeadsByKey({
        externalSystem: group?._id?.sourceExternalSystem || "",
        externalLeadId: group?._id?.sourceExternalId || "",
      }).catch((error) => {
        console.error("Error repairing duplicate external lead:", error.message);
      });
    }
  }

  async function repairUnlinkedThumbtackReviewActivities() {
    const reviewActivities = await MetalworksLeadActivity.find({
      activityType: "thumbtack_review",
      $or: [{ leadId: null }, { leadId: { $exists: false } }],
    })
      .sort({ createdAt: -1 })
      .limit(50);

    for (const activityDoc of reviewActivities) {
      try {
        const negotiationId = getThumbtackReviewNegotiationIdFromActivity(activityDoc);

        if (!negotiationId) {
          continue;
        }

        const leadDoc = await findThumbtackLeadByNegotiationId(negotiationId);

        if (!leadDoc?._id) {
          continue;
        }

        activityDoc.leadId = leadDoc._id;
        activityDoc.meta = {
          ...(activityDoc.meta || {}),
          negotiationId,
        };
        activityDoc.updatedAt = new Date();
        await activityDoc.save();
      } catch (error) {
        console.error("Error repairing unlinked Thumbtack review:", error.message);
      }
    }
  }

  async function repairScheduledReminderDrift() {
    const now = new Date();
    const candidateLeads = await MetalworksLead.find({
      status: { $nin: ["won", "lost", "archived"] },
      nextActionAt: { $gte: new Date(now.getTime() - 12 * 60 * 60 * 1000) },
      nextActionReminderOffsets: { $exists: true, $ne: [] },
    })
      .sort({ updatedAt: -1 })
      .limit(60);

    for (const leadDoc of candidateLeads) {
      if (Array.isArray(leadDoc.nextActionReminderSentKeys) && leadDoc.nextActionReminderSentKeys.length) {
        continue;
      }

      const latestScheduleActivity = await MetalworksLeadActivity.findOne({
        leadId: leadDoc._id,
        activityType: "lead_updated",
        body: /Seguimiento:/i,
      })
        .sort({ createdAt: -1 })
        .lean();

      const scheduleLabelMatch = String(latestScheduleActivity?.body || "").match(
        /Seguimiento:\s*([0-9]{1,2}\/[0-9]{1,2}\/[0-9]{4},\s*[0-9]{1,2}:\d{2}(?::\d{2})?\s*[AP]M)/i,
      );
      const intendedDate = parseLegacyCrmScheduleLabel(scheduleLabelMatch?.[1] || "");
      const currentDate =
        leadDoc.nextActionAt instanceof Date
          ? leadDoc.nextActionAt
          : leadDoc.nextActionAt
            ? new Date(leadDoc.nextActionAt)
            : null;

      if (
        !(intendedDate instanceof Date) ||
        Number.isNaN(intendedDate.getTime()) ||
        !(currentDate instanceof Date) ||
        Number.isNaN(currentDate.getTime())
      ) {
        continue;
      }

      const diffHours = Math.abs(intendedDate.getTime() - currentDate.getTime()) / (60 * 60 * 1000);

      if (diffHours < 4 || diffHours > 6.5) {
        continue;
      }

      leadDoc.nextActionAt = intendedDate;
      leadDoc.nextActionReminderSentKeys = [];
      leadDoc.updatedAt = new Date();
      await leadDoc.save().catch(() => null);
    }
  }

  async function appendActivity({
    leadId = null,
    applicantId = null,
    activityType = "",
    externalEventKey = "",
    title = "",
    body = "",
    meta = null,
    pagePath = "",
    pageUrl = "",
    req = null,
    tracking = {},
  } = {}) {
    const safeExternalEventKey = cleanText(externalEventKey || "", 240);
    const createActivity = async () => {
      if (safeExternalEventKey) {
        const existingActivity = await MetalworksLeadActivity.findOne({
          externalEventKey: safeExternalEventKey,
        }).lean();

        if (existingActivity?._id) {
          return existingActivity;
        }
      }

      return await MetalworksLeadActivity.create({
        leadId,
        applicantId,
        activityType,
        externalEventKey: safeExternalEventKey,
        title: title || formatActivityTitle(activityType),
        body: cleanText(body || "", 1200),
        meta,
        pagePath: cleanText(pagePath || "", 240),
        pageUrl: cleanText(pageUrl || "", 500),
        ipAddress: req ? cleanText(getClientIp(req), 120) : "",
        userAgent: req ? cleanText(req.headers["user-agent"] || "", 400) : "",
        tracking: buildTrackingPayload(tracking),
      });
    };

    if (!safeExternalEventKey) {
      return await createActivity();
    }

    return await withExternalLeadLock(`activity:${safeExternalEventKey}`, createActivity);
  }

  function createRequestError(statusCode = 500, message = "Request error") {
    const error = new Error(message);
    error.statusCode = statusCode;
    return error;
  }

  async function upsertExternalLeadRecord({
    externalLeadId = "",
    externalSystem = "external_sync",
    fullName = "",
    phone = "",
    phoneDisplay = "",
    email = "",
    projectType = "",
    location = "",
    addressLine = "",
    zipCode = "",
    city = "",
    details = "",
    sourceType = "",
    pageTitle = "",
    pagePath = "",
    pageUrl = "",
    referrer = "",
    tracking = {},
    parsedFiles = [],
    rawPhotoFileNames = [],
    req = null,
    crmStatus = "new",
    requirePhone = true,
  } = {}) {
    const safeExternalLeadId = cleanText(externalLeadId || "", 120);
    const safeExternalSystem = cleanText(externalSystem || "", 80) || "external_sync";
    const safeFullName = cleanText(fullName || "", 120);
    const safePhoneDisplay = cleanText(phoneDisplay || phone || "", 40);
    const safePhone = normalizePhone(phone || phoneDisplay || "");
    const safeEmail = normalizeEmail(email || "");
    const safeProjectType = cleanText(projectType || "", 120);
    const safeLocation = cleanText(location || "", 160);
    const safeAddressLine = cleanText(addressLine || "", 160);
    const safeZipCode = cleanText(zipCode || "", 20);
    const safeCity = cleanText(city || "", 120);
    const safeDetails = cleanText(details || "", 3000);
    const safeSourceType = cleanText(sourceType || "", 80) || safeExternalSystem;
    const safePageTitle = cleanText(pageTitle || "", 160);
    const safePagePath = cleanText(pagePath || "", 240);
    const safePageUrl = cleanText(pageUrl || "", 500);
    const safeReferrer = cleanText(referrer || "", 500);
    const safeTracking = buildTrackingPayload(tracking || {});
    const safeCrmStatus = METALWORKS_CRM_STATUS_OPTIONS.includes(cleanText(crmStatus || "", 24))
      ? cleanText(crmStatus || "", 24)
      : "new";
    const safeParsedFiles = Array.isArray(parsedFiles)
      ? parsedFiles.slice(0, METALWORKS_LEAD_ASSET_MAX_FILES)
      : [];
    const safeRawPhotoFileNames = Array.isArray(rawPhotoFileNames)
      ? rawPhotoFileNames
          .map((item) => cleanText(item, 120))
          .filter(Boolean)
          .slice(0, 20)
      : [];
    const photoFileNames = mergeAssistantUniqueValues(
      safeRawPhotoFileNames,
      ...safeParsedFiles.map((item) => item.fileName || ""),
    ).slice(0, 20);

    if (!safeExternalLeadId) {
      throw createRequestError(400, "Missing external lead id.");
    }

    if (!safeFullName) {
      throw createRequestError(400, "The full name is required.");
    }

    if (requirePhone && !safePhone) {
      throw createRequestError(400, "The phone number is required.");
    }

    if (!safeDetails) {
      throw createRequestError(400, "The lead details are required.");
    }

    const lockKey = buildExternalLeadLockKey(safeExternalSystem, safeExternalLeadId);

    return await withExternalLeadLock(lockKey, async () => {
      const now = new Date();
      let leadDoc = await mergeDuplicateExternalLeadsByKey({
        externalSystem: safeExternalSystem,
        externalLeadId: safeExternalLeadId,
      });

      if (!leadDoc) {
        leadDoc = await MetalworksLead.findOne({
          sourceExternalSystem: safeExternalSystem,
          sourceExternalId: safeExternalLeadId,
        }).sort({ updatedAt: -1, createdAt: -1 });
      }

      const duplicate = Boolean(leadDoc);

      if (leadDoc) {
        leadDoc.fullName = safeFullName || leadDoc.fullName;
        leadDoc.phone = safePhone || leadDoc.phone || "";
        leadDoc.phoneDisplay =
          safePhoneDisplay || safePhone || leadDoc.phoneDisplay || leadDoc.phone || "";
        leadDoc.email = safeEmail || leadDoc.email || "";
        leadDoc.projectType = safeProjectType || leadDoc.projectType || "";
        leadDoc.location = safeLocation || leadDoc.location || "";
        leadDoc.addressLine = safeAddressLine || leadDoc.addressLine || "";
        leadDoc.zipCode = safeZipCode || leadDoc.zipCode || "";
        leadDoc.city = safeCity || leadDoc.city || "";
        leadDoc.details = safeDetails || leadDoc.details || "";
        leadDoc.photoFileNames = photoFileNames.length
          ? mergeAssistantUniqueValues(leadDoc.photoFileNames || [], photoFileNames).slice(0, 20)
          : Array.isArray(leadDoc.photoFileNames)
            ? leadDoc.photoFileNames
            : [];
        leadDoc.sourceType = safeSourceType;
        leadDoc.sourceExternalId = safeExternalLeadId;
        leadDoc.sourceExternalSystem = safeExternalSystem;
        leadDoc.pageTitle = safePageTitle || leadDoc.pageTitle || "";
        leadDoc.pagePath = safePagePath || leadDoc.pagePath || "";
        leadDoc.pageUrl = safePageUrl || leadDoc.pageUrl || "";
        leadDoc.referrer = safeReferrer || leadDoc.referrer || "";
        leadDoc.ipAddress = req ? cleanText(getClientIp(req), 120) : leadDoc.ipAddress || "";
        leadDoc.userAgent =
          req ? cleanText(req.headers["user-agent"] || "", 400) : leadDoc.userAgent || "";
        leadDoc.tracking = safeTracking;

        if (shouldApplyExternalLeadStatus(leadDoc.status || "", safeCrmStatus)) {
          leadDoc.status = safeCrmStatus;
        }

        leadDoc.updatedAt = now;
        await leadDoc.save();
      } else {
        const createStatus = resolveExternalLeadCreateStatus(
          safeCrmStatus,
          safeExternalSystem,
          safeSourceType,
        );

        leadDoc = await MetalworksLead.create({
          fullName: safeFullName,
          phone: safePhone,
          phoneDisplay: safePhoneDisplay || safePhone,
          email: safeEmail,
          projectType: safeProjectType,
          location: safeLocation,
          addressLine: safeAddressLine,
          zipCode: safeZipCode,
          city: safeCity,
          details: safeDetails,
          photoFileNames,
          status: createStatus,
          sourceType: safeSourceType,
          sourceExternalId: safeExternalLeadId,
          sourceExternalSystem: safeExternalSystem,
          pageTitle: safePageTitle,
          pagePath: safePagePath,
          pageUrl: safePageUrl,
          referrer: safeReferrer,
          ipAddress: req ? cleanText(getClientIp(req), 120) : "",
          userAgent: req ? cleanText(req.headers["user-agent"] || "", 400) : "",
          tracking: safeTracking,
          updatedAt: now,
          createdAt: now,
        });
      }

      let syncedAssetCount = 0;

      if (leadDoc?._id && safeParsedFiles.length) {
        const existingAssets = await MetalworksLeadAsset.find({ leadId: leadDoc._id })
          .select("fileName sizeBytes")
          .lean();
        const existingKeys = new Set(
          existingAssets.map(
            (item) =>
              `${sanitizeLeadAssetFileName(item?.fileName || "")}:${Number(item?.sizeBytes || 0)}`,
          ),
        );
        const newFiles = safeParsedFiles.filter((item) => {
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
                sourceType: safeSourceType,
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

      return {
        duplicate,
        syncedAssetCount,
        photoFileNames,
        leadDoc,
      };
    });
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

  async function resolveWebsiteLiveChatLead({
    visitorId = "",
    sessionId = "",
    threadKey = "",
  } = {}) {
    const safeThreadKey = normalizePublicChatThreadKey(threadKey);
    const conditions = [];

    if (safeThreadKey) {
      const threadMatch = await MetalworksLead.findOne({
        sourceType: METALWORKS_WEBSITE_CHAT_SOURCE_TYPE,
        publicChatThreadKey: safeThreadKey,
      }).sort({ updatedAt: -1, createdAt: -1 });

      if (threadMatch) {
        return threadMatch;
      }
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

    return MetalworksLead.findOne({
      sourceType: METALWORKS_WEBSITE_CHAT_SOURCE_TYPE,
      $or: conditions,
    }).sort({ updatedAt: -1, createdAt: -1 });
  }

  async function syncPublicChatPushDevicesToLead(leadDoc = null) {
    if (!leadDoc?._id) {
      return 0;
    }

    const conditions = [];
    const visitorIds = Array.isArray(leadDoc.visitorIds) ? leadDoc.visitorIds.filter(Boolean) : [];
    const sessionIds = Array.isArray(leadDoc.sessionIds) ? leadDoc.sessionIds.filter(Boolean) : [];

    if (visitorIds.length) {
      conditions.push({ visitorId: { $in: visitorIds } });
    }

    if (sessionIds.length) {
      conditions.push({ sessionId: { $in: sessionIds } });
    }

    if (!conditions.length) {
      return 0;
    }

    const result = await MetalworksPublicChatWebPushDevice.updateMany(
      {
        $or: conditions,
      },
      {
        $set: {
          leadId: leadDoc._id,
          updatedAt: new Date(),
        },
      },
    );

    return Number(result?.modifiedCount || 0) || 0;
  }

  async function sendWebsiteLiveChatReplyPushAlert({ lead = null, message = "" } = {}) {
    if (!lead?._id) {
      return {
        attempted: false,
        delivered: false,
        deliveredCount: 0,
        deviceCount: 0,
        error: "Lead is required for public chat push.",
      };
    }

    const conditions = [{ leadId: lead._id }];
    const visitorIds = Array.isArray(lead.visitorIds) ? lead.visitorIds.filter(Boolean) : [];
    const sessionIds = Array.isArray(lead.sessionIds) ? lead.sessionIds.filter(Boolean) : [];

    if (visitorIds.length) {
      conditions.push({ visitorId: { $in: visitorIds } });
    }

    if (sessionIds.length) {
      conditions.push({ sessionId: { $in: sessionIds } });
    }

    const deviceDocs = await MetalworksPublicChatWebPushDevice.find({
      isActive: true,
      notificationsEnabled: true,
      $or: conditions,
    })
      .sort({ lastSeenAt: -1, updatedAt: -1 })
      .limit(24)
      .lean();

    if (!deviceDocs.length) {
      return {
        attempted: false,
        delivered: false,
        deliveredCount: 0,
        deviceCount: 0,
        error: "No active public chat devices are registered for this lead.",
      };
    }

    const title = "Chicago Metal Works replied";
    const body =
      trimPushCopy(message || "", 140) ||
      "Open your chat to see the latest update from Chicago Metal Works & Fencing.";
    const targetUrl = buildPublicChatThreadPath(lead.publicChatThreadKey || "");
    const results = await Promise.all(
      deviceDocs.map(async (device) => {
        const result = await sendMetalworksWebPushNotification({
          subscription: device.subscription,
          alertType: "website_live_chat_reply",
          title,
          body,
          leadId: String(lead._id || ""),
          notificationPath: "/metalworks-chat/",
          targetUrl,
        });
        const update = {
          lastSeenAt: new Date(),
          updatedAt: new Date(),
          lastPushAt: result.delivered ? new Date() : device.lastPushAt || null,
          lastPushError: result.delivered ? "" : cleanText(result.error || "", 240),
        };

        if (
          METALWORKS_PUSH_INVALID_REASONS.has(result.reason || "") ||
          Number(result.status || 0) === 410
        ) {
          update.isActive = false;
          update.notificationsEnabled = false;
        }

        await MetalworksPublicChatWebPushDevice.updateOne(
          { _id: device._id },
          {
            $set: update,
          },
        );

        return result;
      }),
    );

    return {
      attempted: true,
      delivered: results.some((item) => item.delivered),
      deliveredCount: results.filter((item) => item.delivered).length,
      deviceCount: deviceDocs.length,
      results,
      error: results.some((item) => item.delivered)
        ? ""
        : results[0]?.error || "No pude entregar alertas del chat publico.",
    };
  }

  async function resolveConversationApplicant({
    visitorId = "",
    sessionId = "",
    email = "",
    phone = "",
  } = {}) {
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

    return MetalworksApplicant.findOne({ $or: conditions }).sort({ updatedAt: -1, createdAt: -1 });
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

  function resolveApplicantStatus(currentApplicant = null, state = {}) {
    const currentStatus = cleanText(currentApplicant?.status || "new", 40).toLowerCase();

    if (currentStatus === "archived") {
      return currentStatus;
    }

    if (state?.nextActionAt) {
      return "interview_requested";
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
    leadDoc.privateNotes = mergeAssistantPrivateNotes(
      currentLead?.privateNotes || "",
      {
        ...state,
        sourceType: cleanText(sourceType || currentLead?.sourceType || "", 80) || "assistant_chat",
        lastAssistantMessage: cleanText(assistantReply || currentLead?.lastAssistantMessage || "", 1500),
      },
      mergedHistory,
    );
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

  async function upsertConversationApplicant({
    currentApplicant = null,
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
    if (!state?.shouldCreateApplicant && !currentApplicant?._id) {
      return null;
    }

    const now = new Date();
    const applicantDoc = currentApplicant || new MetalworksApplicant();
    const existingConversationHistory = currentApplicant?.conversationHistory || [];
    const mergedHistory = mergeConversationHistory(existingConversationHistory, state.items || []);
    const effectiveName =
      selectAssistantText(
        sanitizeAssistantStoredName(state?.fullName || ""),
        sanitizeAssistantStoredName(currentApplicant?.fullName || ""),
      ) || METALWORKS_APPLICANT_PLACEHOLDER_NAME;
    const effectivePhone = normalizePhone(state?.phone || currentApplicant?.phone || "");
    const effectivePhoneDisplay =
      cleanText(state?.phoneDisplay || currentApplicant?.phoneDisplay || "", 40) || effectivePhone;
    const effectiveEmail = normalizeEmail(state?.email || currentApplicant?.email || "");
    const effectiveRole = selectAssistantText(
      state?.positionApplied || "",
      currentApplicant?.positionApplied || "",
    );
    const effectiveRoleTrack = selectAssistantText(
      state?.roleTrack || "",
      currentApplicant?.roleTrack || "",
      inferApplicantRoleTrack(effectiveRole),
    );
    const effectiveLanguages = selectAssistantText(
      state?.languages || "",
      currentApplicant?.languages || "",
    );
    const effectiveYearsExperience = selectAssistantText(
      state?.yearsExperience || "",
      currentApplicant?.yearsExperience || "",
    );
    const effectiveExperienceSummary = selectAssistantLongestText(
      state?.experienceSummary || "",
      currentApplicant?.experienceSummary || "",
    );
    const effectiveLocation = selectAssistantLongestText(
      state?.location || "",
      currentApplicant?.location || "",
    );

    applicantDoc.fullName = effectiveName;
    applicantDoc.phone = effectivePhone;
    applicantDoc.phoneDisplay = effectivePhoneDisplay;
    applicantDoc.email = effectiveEmail;
    applicantDoc.positionApplied = effectiveRole;
    applicantDoc.roleTrack = effectiveRoleTrack;
    applicantDoc.languages = effectiveLanguages;
    applicantDoc.yearsExperience = effectiveYearsExperience;
    applicantDoc.experienceSummary = effectiveExperienceSummary;
    applicantDoc.hasTools = selectAssistantText(state?.hasTools || "", currentApplicant?.hasTools || "");
    applicantDoc.hasTransportation = selectAssistantText(
      state?.hasTransportation || "",
      currentApplicant?.hasTransportation || "",
    );
    applicantDoc.fieldReady = selectAssistantText(state?.fieldReady || "", currentApplicant?.fieldReady || "");
    applicantDoc.location = effectiveLocation;
    applicantDoc.bestInterviewDay = cleanText(
      state?.bestInterviewDay || currentApplicant?.bestInterviewDay || "",
      80,
    );
    applicantDoc.bestInterviewTime = cleanText(
      state?.bestInterviewTime || currentApplicant?.bestInterviewTime || "",
      80,
    );
    applicantDoc.status = resolveApplicantStatus(currentApplicant, state);
    applicantDoc.nextAction =
      state?.nextActionAt ? "phone interview follow-up" : currentApplicant?.nextAction || "";
    applicantDoc.nextActionAt = state?.nextActionAt || currentApplicant?.nextActionAt || null;
    applicantDoc.privateNotes = mergeApplicantPrivateNotes(currentApplicant?.privateNotes || "", state);
    applicantDoc.detailsSummary = selectAssistantLongestText(
      state?.detailsSummary || "",
      currentApplicant?.detailsSummary || "",
    );
    applicantDoc.sourceType =
      cleanText(sourceType || currentApplicant?.sourceType || "", 80) || "assistant_chat_job";
    applicantDoc.sourceChannel = cleanText(
      state?.sourceChannel || currentApplicant?.sourceChannel || "",
      40,
    );
    applicantDoc.sourceLabel = cleanText(
      state?.sourceLabel || currentApplicant?.sourceLabel || "",
      120,
    );
    applicantDoc.pageTitle = cleanText(pageTitle || currentApplicant?.pageTitle || "", 160);
    applicantDoc.pagePath = cleanText(pagePath || currentApplicant?.pagePath || "", 240);
    applicantDoc.pageUrl = cleanText(pageUrl || currentApplicant?.pageUrl || "", 500);
    applicantDoc.referrer = cleanText(referrer || currentApplicant?.referrer || "", 500);
    applicantDoc.ipAddress = req
      ? cleanText(getClientIp(req), 120)
      : cleanText(currentApplicant?.ipAddress || "", 120);
    applicantDoc.userAgent = req
      ? cleanText(req.headers["user-agent"] || "", 400)
      : cleanText(currentApplicant?.userAgent || "", 400);
    applicantDoc.tracking = buildTrackingPayload(tracking || currentApplicant?.tracking || {});
    applicantDoc.visitorIds = mergeAssistantUniqueValues(
      currentApplicant?.visitorIds || [],
      state?.visitorId || "",
    );
    applicantDoc.sessionIds = mergeAssistantUniqueValues(
      currentApplicant?.sessionIds || [],
      state?.sessionId || "",
    );
    applicantDoc.conversationHistory = mergedHistory;
    applicantDoc.lastUserMessage = cleanText(
      state?.latestUserMessage || currentApplicant?.lastUserMessage || "",
      500,
    );
    applicantDoc.lastAssistantMessage = cleanText(
      assistantReply || currentApplicant?.lastAssistantMessage || "",
      1500,
    );

    if (state?.nextActionAt && !applicantDoc.interviewRequestedAt) {
      applicantDoc.interviewRequestedAt = now;
    }

    if (state?.phone || state?.email || state?.positionApplied) {
      applicantDoc.lastContactAt = now;
    }

    applicantDoc.updatedAt = now;

    if (!applicantDoc.createdAt) {
      applicantDoc.createdAt = now;
    }

    await applicantDoc.save();
    return applicantDoc;
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

  function buildAssistantHintSeedApplicant(
    applicant = null,
    { nameHint = "", phoneHint = "", phoneDisplayHint = "" } = {},
  ) {
    const baseApplicant =
      applicant && typeof applicant.toObject === "function"
        ? applicant.toObject()
        : applicant
          ? { ...applicant }
          : {};
    const safeName = sanitizeAssistantStoredName(nameHint || "");
    const safePhone = normalizePhone(phoneHint || "");
    const safePhoneDisplay =
      cleanText(phoneDisplayHint || phoneHint || "", 40) || safePhone;
    const existingName = sanitizeAssistantStoredName(baseApplicant.fullName || "");
    const existingPhone = normalizePhone(baseApplicant.phone || "");

    if (
      safeName &&
      (!existingName || existingName === METALWORKS_APPLICANT_PLACEHOLDER_NAME)
    ) {
      baseApplicant.fullName = safeName;
    }

    if (safePhone && !existingPhone) {
      baseApplicant.phone = safePhone;
    }

    if (safePhoneDisplay && (!cleanText(baseApplicant.phoneDisplay || "", 40) || !existingPhone)) {
      baseApplicant.phoneDisplay = safePhoneDisplay;
    }

    return baseApplicant;
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
  } = {}) {
    const normalizedPhoneHint = normalizePhone(phoneHint || "");
    const incomingHistory = normalizeAssistantHistory(history);
    const incomingConversationText = [message, ...incomingHistory.map((item) => item.content || "")]
      .filter(Boolean)
      .join("\n");
    const initialEmploymentHint = detectEmploymentIntent(incomingConversationText);
    let currentLead = await resolveConversationLead({
      visitorId,
      sessionId,
      phone: normalizedPhoneHint,
    });
    let currentApplicant = await resolveConversationApplicant({
      visitorId,
      sessionId,
      phone: normalizedPhoneHint,
    });
    let seededLead = buildAssistantHintSeedLead(currentLead, {
      nameHint,
      phoneHint,
      phoneDisplayHint,
    });
    let seededApplicant = buildAssistantHintSeedApplicant(currentApplicant, {
      nameHint,
      phoneHint,
      phoneDisplayHint,
    });
    let forceCustomerIntent =
      detectEmploymentCorrection(incomingConversationText) ||
      applicantLooksLikeMisclassifiedCustomer(currentApplicant);
    let mergedHistory = mergeConversationHistory(
      forceCustomerIntent
        ? currentLead?.conversationHistory || []
        : currentApplicant?.conversationHistory ||
            (initialEmploymentHint ? [] : currentLead?.conversationHistory || []),
      incomingHistory,
    );
    let userConversationItems = buildAssistantConversationItems({
      history: mergedHistory,
      message,
    });
    let leadConversationState = buildAssistantConversationSignals({
      history: userConversationItems,
      lead: seededLead,
      pagePath,
    });
    let applicantConversationState = buildApplicantConversationSignals({
      history: userConversationItems,
      applicant: seededApplicant,
    });
    const resolvedLead = await resolveConversationLead({
      visitorId,
      sessionId,
      email: leadConversationState.email,
      phone: leadConversationState.phone || normalizedPhoneHint,
    });
    const resolvedApplicant = await resolveConversationApplicant({
      visitorId,
      sessionId,
      email: applicantConversationState.email,
      phone: applicantConversationState.phone || normalizedPhoneHint,
    });

    if (
      (resolvedLead?._id && String(resolvedLead._id) !== String(currentLead?._id || "")) ||
      (resolvedApplicant?._id &&
        String(resolvedApplicant._id) !== String(currentApplicant?._id || ""))
    ) {
      currentLead = resolvedLead || currentLead;
      currentApplicant = resolvedApplicant || currentApplicant;
      seededLead = buildAssistantHintSeedLead(currentLead, {
        nameHint,
        phoneHint,
        phoneDisplayHint,
      });
      seededApplicant = buildAssistantHintSeedApplicant(currentApplicant, {
        nameHint,
        phoneHint,
        phoneDisplayHint,
      });
      forceCustomerIntent =
        detectEmploymentCorrection(incomingConversationText) ||
        applicantLooksLikeMisclassifiedCustomer(currentApplicant);
      mergedHistory = mergeConversationHistory(
        forceCustomerIntent
          ? currentLead?.conversationHistory || []
          : currentApplicant?.conversationHistory ||
              (initialEmploymentHint ? [] : currentLead?.conversationHistory || []),
        incomingHistory,
      );
      userConversationItems = buildAssistantConversationItems({
        history: mergedHistory,
        message,
      });
      leadConversationState = buildAssistantConversationSignals({
        history: userConversationItems,
        lead: seededLead,
        pagePath,
      });
      applicantConversationState = buildApplicantConversationSignals({
        history: userConversationItems,
        applicant: seededApplicant,
      });
    } else {
      currentLead = resolvedLead || currentLead;
      currentApplicant = resolvedApplicant || currentApplicant;
    }

    const intentType =
      !forceCustomerIntent &&
      (currentApplicant?._id ||
        detectEmploymentIntent(
          applicantConversationState.combinedUserText ||
            applicantConversationState.latestUserMessage ||
            message,
        ))
        ? "employment"
        : "customer";

    return {
      currentLead,
      currentApplicant,
      userConversationItems,
      conversationState:
        intentType === "employment" ? applicantConversationState : leadConversationState,
      leadConversationState,
      applicantConversationState,
      intentType,
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
  } = {}) {
    const safeMessage = cleanText(message || "", 500);
    const safeVisitorId = cleanText(visitorId || "", 120);
    const safeSessionId = cleanText(sessionId || "", 120);
    const safePageTitle = cleanText(pageTitle || "", 160);
    const safePagePath = cleanText(pagePath || "", 240);
    const safePageUrl = cleanText(pageUrl || "", 500);
    const safeReferrer = cleanText(referrer || "", 500);
    const safeTracking = buildTrackingPayload(tracking || {});

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
          activityType: { $in: ["assistant_user_message", "applicant_user_message"] },
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

    const {
      currentLead,
      currentApplicant,
      userConversationItems,
      conversationState: initialConversationState,
      intentType,
    } = await buildAssistantConversationContext({
      history,
      message: safeMessage,
      pagePath: safePagePath,
      visitorId: safeVisitorId,
      sessionId: safeSessionId,
      nameHint,
      phoneHint,
      phoneDisplayHint,
    });

    let conversationState = {
      ...initialConversationState,
      visitorId: safeVisitorId,
      sessionId: safeSessionId,
      sourceChannel: cleanText(sourceChannel || "web", 40) || "web",
      sourceLabel: cleanText(sourceLabel || "", 120) || "Agustin 2.0 website assistant",
    };

    if (intentType === "employment") {
      const safeApplicantSourceType = cleanText(sourceType || "", 80).endsWith("_job")
        ? cleanText(sourceType || "", 80)
        : `${cleanText(sourceType || "assistant_chat", 60) || "assistant_chat"}_job`;
      const applicantExistedBeforeMessage = Boolean(currentApplicant?._id);
      const previousApplicantStatus = cleanText(currentApplicant?.status || "new", 40).toLowerCase();
      const previousApplicantNextActionAt = currentApplicant?.nextActionAt
        ? new Date(currentApplicant.nextActionAt).toISOString()
        : "";
      let applicantDoc = await upsertConversationApplicant({
        currentApplicant,
        state: conversationState,
        pageTitle: safePageTitle,
        pagePath: safePagePath,
        pageUrl: safePageUrl,
        referrer: safeReferrer,
        tracking: safeTracking,
        req,
        sourceType: safeApplicantSourceType,
      });

      if (!applicantExistedBeforeMessage && applicantDoc?._id) {
        await appendActivity({
          applicantId: applicantDoc._id,
          activityType: "job_applicant_created",
          title: applicantDoc.positionApplied
            ? `${applicantDoc.fullName || "Candidato"} · ${applicantDoc.positionApplied}`
            : applicantDoc.fullName || "Candidato nuevo",
          body: conversationState.positionApplied
            ? `Agustin 2.0 guardo un candidato para ${conversationState.positionApplied}.`
            : "Agustin 2.0 abrio un nuevo expediente de candidato.",
          meta: {
            sourceType: safeApplicantSourceType,
            sourceChannel: conversationState.sourceChannel,
            positionApplied: applicantDoc.positionApplied || "",
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
        applicantId: applicantDoc?._id || currentApplicant?._id || null,
        activityType: "applicant_user_message",
        title: "Mensaje del candidato",
        body: safeMessage,
        meta: {
          visitorId: safeVisitorId,
          sessionId: safeSessionId,
          pageTitle: safePageTitle,
          sourceType: safeApplicantSourceType,
          sourceChannel: conversationState.sourceChannel,
        },
        req,
        pagePath: safePagePath,
        pageUrl: safePageUrl,
        tracking: safeTracking,
      });

      const result = await generateAssistantReply({
        message: safeMessage,
        history: userConversationItems,
        pagePath: safePagePath,
        conversationState,
        mode: "employment",
      });

      const conversationItemsWithReply = buildAssistantConversationItems({
        history: userConversationItems,
        reply: result.reply,
      });
      const finalState = {
        ...buildApplicantConversationSignals({
          history: conversationItemsWithReply,
          applicant: buildAssistantHintSeedApplicant(applicantDoc || currentApplicant, {
            nameHint,
            phoneHint,
            phoneDisplayHint,
          }),
        }),
        visitorId: safeVisitorId,
        sessionId: safeSessionId,
        sourceChannel: conversationState.sourceChannel,
        sourceLabel: conversationState.sourceLabel,
      };

      applicantDoc = await upsertConversationApplicant({
        currentApplicant: applicantDoc || currentApplicant,
        state: finalState,
        pageTitle: safePageTitle,
        pagePath: safePagePath,
        pageUrl: safePageUrl,
        referrer: safeReferrer,
        tracking: safeTracking,
        req,
        assistantReply: result.reply,
        sourceType: safeApplicantSourceType,
      });

      const currentApplicantNextActionAt = applicantDoc?.nextActionAt
        ? new Date(applicantDoc.nextActionAt).toISOString()
        : "";

      if (
        applicantDoc?._id &&
        finalState.nextActionAt &&
        (previousApplicantStatus !== "interview_requested" ||
          previousApplicantNextActionAt !== currentApplicantNextActionAt)
      ) {
        await appendActivity({
          applicantId: applicantDoc._id,
          activityType: "job_applicant_interview_requested",
          title: applicantDoc.fullName
            ? `${applicantDoc.fullName} · entrevista`
            : applicantExistedBeforeMessage
              ? "Entrevista de candidato actualizada"
              : "Entrevista de candidato guardada",
          body: finalState.interviewLabel
            ? `Agustin 2.0 dejo entrevista por telefono para ${finalState.interviewLabel}.`
            : "Agustin 2.0 pidio seguimiento de entrevista por telefono.",
          meta: {
            duplicate: applicantExistedBeforeMessage,
            requestedAt: finalState.nextActionAt.toISOString(),
            visitorId: safeVisitorId,
            sessionId: safeSessionId,
            pageTitle: safePageTitle,
            sourceType: safeApplicantSourceType,
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

      if (applicantDoc?._id && finalState.shouldAlert && !applicantDoc.alertSentAt) {
        try {
          alertDelivery = await sendMetalworksApplicantAlertEmail({
            applicant: applicantDoc.toObject ? applicantDoc.toObject() : applicantDoc,
            requestedAtLabel: finalState.interviewLabel,
            pagePath: safePagePath,
            pageUrl: safePageUrl,
            conversationDigest: finalState.conversationDigest,
          });

          if (alertDelivery.delivered) {
            applicantDoc.alertSentAt = new Date();
            applicantDoc.updatedAt = new Date();
            await applicantDoc.save();
          }
        } catch (error) {
          console.error("Error sending Metal Works applicant alert:", error.message);
        }

        try {
          pushDelivery = await sendMetalworksPushAlert({
            applicant: applicantDoc.toObject ? applicantDoc.toObject() : applicantDoc,
            alertType: "job_applicant",
            requestedAtLabel: finalState.interviewLabel,
          });

          if (pushDelivery.delivered && !applicantDoc.alertSentAt) {
            applicantDoc.alertSentAt = new Date();
            applicantDoc.updatedAt = new Date();
            await applicantDoc.save();
          }
        } catch (error) {
          console.error("Error sending Metal Works applicant push:", error.message);
        }
      }

      await appendActivity({
        applicantId: applicantDoc?._id || currentApplicant?._id || null,
        activityType: result.usedFallback ? "applicant_fallback" : "applicant_ai_reply",
        title: result.usedFallback ? "Fallback candidato" : "Respuesta para candidato",
        body: result.reply,
        meta: {
          visitorId: safeVisitorId,
          sessionId: safeSessionId,
          reason: result.reason || "",
          sourceType: safeApplicantSourceType,
          sourceChannel: finalState.sourceChannel,
        },
        req,
        pagePath: safePagePath,
        pageUrl: safePageUrl,
        tracking: safeTracking,
      });

      return {
        ok: true,
        status: 200,
        respuesta: result.reply,
        usedFallback: result.usedFallback,
        leadCaptured: false,
        leadId: "",
        applicantCaptured: Boolean(applicantDoc?._id),
        applicantId: applicantDoc?._id ? String(applicantDoc._id) : "",
        callbackCaptured: false,
        callbackLabel: "",
        notified: Boolean(alertDelivery.delivered || pushDelivery.delivered),
        remainingToday: safeVisitorId
          ? Math.max(METALWORKS_ASSISTANT_MAX_MESSAGES_PER_DAY - (usedToday + 1), 0)
          : METALWORKS_ASSISTANT_MAX_MESSAGES_PER_DAY,
      };
    }

    const leadExistedBeforeMessage = Boolean(currentLead?._id);
    const previousLeadStatus = normalizeStatus(currentLead?.status || "new");
    const previousLeadNextActionAt = currentLead?.nextActionAt
      ? new Date(currentLead.nextActionAt).toISOString()
      : "";
    let leadCreatedActivityRecorded = false;
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
      leadCreatedActivityRecorded = true;
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
    });

    const result = await generateAssistantReply({
      message: safeMessage,
      history: userConversationItems,
      pagePath: safePagePath,
      conversationState,
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

    const leadCreatedThisTurn = Boolean(!leadExistedBeforeMessage && leadDoc?._id);

    if (leadCreatedThisTurn && !leadCreatedActivityRecorded) {
      await appendActivity({
        leadId: leadDoc._id,
        activityType: "lead_created",
        title: "Lead creado",
        body:
          finalState.callbackIntent === "yes"
            ? "Agustin 2.0 creo un lead conversacional para seguimiento de llamada."
            : "Agustin 2.0 creo un lead conversacional desde el chat del sitio.",
        meta: {
          sourceType,
          sourceChannel: finalState.sourceChannel,
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
      leadCreatedActivityRecorded = true;
    }

    const currentLeadNextActionAt = leadDoc?.nextActionAt
      ? new Date(leadDoc.nextActionAt).toISOString()
      : "";
    const callbackCapturedThisTurn = Boolean(
      leadDoc?._id &&
        finalState.callbackIntent === "yes" &&
        finalState.nextActionAt &&
        (previousLeadStatus !== "booked" || previousLeadNextActionAt !== currentLeadNextActionAt),
    );

    if (callbackCapturedThisTurn) {
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

    const shouldSendAssistantCallbackAlert = Boolean(
      leadDoc?._id &&
        (finalState.shouldAlert || callbackCapturedThisTurn) &&
        (callbackCapturedThisTurn || !leadDoc.callbackAlertedAt),
    );
    const shouldSendAssistantLeadPush = Boolean(
      leadDoc?._id && leadCreatedThisTurn && !shouldSendAssistantCallbackAlert,
    );

    if (shouldSendAssistantCallbackAlert) {
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
    } else if (shouldSendAssistantLeadPush) {
      try {
        pushDelivery = await sendMetalworksPushAlert({
          lead: leadDoc.toObject ? leadDoc.toObject() : leadDoc,
          alertType: "assistant_lead",
        });
      } catch (error) {
        console.error("Error sending Metal Works assistant lead push:", error.message);
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
      const [auth, totalAccounts, activeAccounts] = await Promise.all([
        getProspectorAuth(req, { touch: true }),
        MetalworksProspectorUser.countDocuments({}),
        MetalworksProspectorUser.countDocuments({ status: "active" }),
      ]);

      res.json({
        authenticated: Boolean(auth.email),
        configured: totalAccounts > 0,
        totalAccounts,
        activeAccounts,
        userId: auth.userId || "",
        name: auth.name || "",
        email: auth.email || "",
        status: auth.status || "",
      });
    } catch (error) {
      console.error("Error loading Metal Works prospector auth:", error.message);
      respondError(res, 500, "I couldn't check the prospector session.");
    }
  });

  app.post("/api/metalworks-crm/prospector/login", async (req, res) => {
    const email = normalizeEmail(req.body?.email || "");
    const password = String(req.body?.password || "");

    if (!email || !password) {
      return respondError(res, 400, "Email and password are required.");
    }

    try {
      const totalAccounts = await MetalworksProspectorUser.countDocuments({});

      if (!totalAccounts) {
        return respondError(
          res,
          503,
          "Ask an admin to create your prospector account in the Metal Works CRM first.",
        );
      }

      const prospectorUser = await MetalworksProspectorUser.findOne({ email });

      if (!prospectorUser) {
        return respondError(res, 401, "No account was found with that email.");
      }

      if (normalizeProspectorStatus(prospectorUser.status || "active") !== "active") {
        return respondError(
          res,
          403,
          "This account is paused. Ask an admin to reactivate it.",
        );
      }

      if (
        !verifySecurePasswordHash(
          password,
          prospectorUser.passwordSalt,
          prospectorUser.passwordHash,
        )
      ) {
        return respondError(res, 401, "Incorrect password.");
      }

      prospectorUser.lastLoginAt = new Date();
      prospectorUser.updatedAt = new Date();
      await prospectorUser.save();

      await createProspectorSession(req, res, { prospectorUser });
      res.json({
        ok: true,
        userId: String(prospectorUser._id || ""),
        name: cleanText(prospectorUser.name || "", 120),
        email: normalizeEmail(prospectorUser.email || ""),
        status: normalizeProspectorStatus(prospectorUser.status || "active"),
      });
    } catch (error) {
      console.error("Error logging into Metal Works prospector portal:", error.message);
      respondError(res, 500, "I couldn't sign you into the prospector portal.");
    }
  });

  app.post("/api/metalworks-crm/prospector/logout", async (req, res) => {
    try {
      await destroyProspectorSession(req, res);
      res.json({ ok: true });
    } catch (error) {
      console.error("Error logging out of Metal Works prospector portal:", error.message);
      respondError(res, 500, "I couldn't sign you out of the prospector portal.");
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
          id: auth.userId || "",
          name: auth.name,
          email: auth.email,
          status: auth.status || "active",
        },
      });
    } catch (error) {
      console.error("Error loading Metal Works prospector dashboard:", error.message);
      respondError(res, 500, "I couldn't load the prospector dashboard.");
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
      const clientSubmissionId = cleanText(req.body?.clientSubmissionId || "", 120);
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
          "Fill in client name, phone, service, address, ZIP, timeline, owner status, and notes.",
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
          "The total photo size is too large. Compress or send fewer files.",
        );
      }

      const now = new Date();
      const fourteenDaysAgo = new Date(Date.now() - 14 * 24 * 60 * 60 * 1000);

      let leadDoc = clientSubmissionId
        ? await MetalworksLead.findOne({ clientSubmissionId }).sort({ createdAt: -1 })
        : null;

      if (!leadDoc) {
        leadDoc = await MetalworksLead.findOne({
          createdAt: { $gte: fourteenDaysAgo },
          phone,
          zipCode,
        }).sort({ createdAt: -1 });
      }

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
        if (!leadDoc.clientSubmissionId && clientSubmissionId) {
          leadDoc.clientSubmissionId = clientSubmissionId;
        }
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
          clientSubmissionId,
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

      if (auth.userId && mongoose.Types.ObjectId.isValid(auth.userId)) {
        await MetalworksProspectorUser.updateOne(
          { _id: auth.userId },
          {
            $set: {
              lastLeadSubmittedAt: now,
              updatedAt: now,
            },
          },
        );
      }

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
      respondError(res, 500, "I couldn't save this prospector lead.");
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

  app.get(
    ["/metalworks-crm/operator", "/metalworks-crm/operator/"],
    async (req, res) => {
      res.set("Cache-Control", "no-store");
      const auth = await getAuth(req, { touch: false });

      if (!auth.email) {
        return res.redirect("/metalworks-crm/login/");
      }

      return res.redirect("/metalworks-crm/");
    },
  );

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
        resourceSections: buildMetalworksCrmResourceSections(),
      });
    } catch (error) {
      console.error("Error loading Metal Works auth:", error.message);
      respondError(res, 500, "No pude revisar la sesion del CRM.");
    }
  });

  app.get("/api/metalworks-crm/prospectors", async (req, res) => {
    const auth = await requireAuth(req, res);

    if (!auth) {
      return;
    }

    try {
      const snapshot = await buildProspectorAdminSnapshot(
        MetalworksProspectorUser,
        MetalworksLead,
      );
      res.json(snapshot);
    } catch (error) {
      console.error("Error loading Metal Works prospectors:", error.message);
      respondError(res, 500, "No pude cargar las cuentas de prospectadores.");
    }
  });

  app.post("/api/metalworks-crm/prospectors", async (req, res) => {
    const auth = await requireAuth(req, res);

    if (!auth) {
      return;
    }

    const name = cleanText(req.body?.name || "", 120);
    const email = normalizeEmail(req.body?.email || "");
    const customPassword = normalizePasswordInput(req.body?.password || "");

    if (!name) {
      return respondError(res, 400, "El nombre del prospectador es requerido.");
    }

    if (!email) {
      return respondError(res, 400, "El correo del prospectador es requerido.");
    }

    if (customPassword && !prospectorPasswordIsValid(customPassword)) {
      return respondError(
        res,
        400,
        `El password debe tener al menos ${METALWORKS_PROSPECTOR_PASSWORD_MIN} caracteres.`,
      );
    }

    try {
      const existingUser = await MetalworksProspectorUser.findOne({ email }).select("_id");

      if (existingUser) {
        return respondError(res, 409, "Ese correo ya tiene una cuenta creada.");
      }

      const passwordToSave = customPassword || generateProspectorTemporaryPassword();
      const securePassword = createSecurePasswordHash(passwordToSave);
      const now = new Date();
      const prospectorUser = await MetalworksProspectorUser.create({
        name,
        email,
        passwordHash: securePassword.hash,
        passwordSalt: securePassword.salt,
        status: "active",
        createdByAdminEmail: auth.email,
        updatedAt: now,
        createdAt: now,
      });

      res.json({
        prospector: cleanProspectorUser(prospectorUser.toObject ? prospectorUser.toObject() : prospectorUser),
        credentials: {
          email,
          temporaryPassword: passwordToSave,
          passwordLabel: customPassword ? "Password" : "Temporary password",
          passwordMode: customPassword ? "custom" : "generated",
        },
      });
    } catch (error) {
      console.error("Error creating Metal Works prospector account:", error.message);
      respondError(res, 500, "No pude crear la cuenta del prospectador.");
    }
  });

  app.patch("/api/metalworks-crm/prospectors/:prospectorId", async (req, res) => {
    const auth = await requireAuth(req, res);

    if (!auth) {
      return;
    }

    const prospectorId = String(req.params?.prospectorId || "").trim();

    if (!prospectorId || !mongoose.Types.ObjectId.isValid(prospectorId)) {
      return respondError(res, 400, "Prospectador invalido.");
    }

    const name = cleanText(req.body?.name || "", 120);
    const rawStatus = cleanText(req.body?.status || "", 24).toLowerCase();
    const requestedStatus = normalizeProspectorStatus(rawStatus);
    const shouldUpdateName = Object.prototype.hasOwnProperty.call(req.body || {}, "name") && name;
    const shouldUpdateStatus =
      Object.prototype.hasOwnProperty.call(req.body || {}, "status") &&
      METALWORKS_PROSPECTOR_STATUS_OPTIONS.includes(rawStatus);

    if (!shouldUpdateName && !shouldUpdateStatus) {
      return respondError(res, 400, "No recibi cambios para este prospectador.");
    }

    try {
      const prospectorUser = await MetalworksProspectorUser.findById(prospectorId);

      if (!prospectorUser) {
        return respondError(res, 404, "No encontre ese prospectador.");
      }

      const previousStatus = normalizeProspectorStatus(prospectorUser.status || "active");
      const nextStatus = shouldUpdateStatus ? requestedStatus : previousStatus;
      const now = new Date();

      if (shouldUpdateName) {
        prospectorUser.name = name;
      }

      if (shouldUpdateStatus) {
        prospectorUser.status = requestedStatus;
      }

      prospectorUser.updatedAt = now;
      await prospectorUser.save();

      const forcedSignOut = previousStatus === "active" && nextStatus !== "active";

      if (forcedSignOut) {
        await MetalworksProspectorSession.deleteMany({
          $or: [
            { prospectorUserId: prospectorUser._id },
            { prospectorEmail: normalizeEmail(prospectorUser.email || "") },
          ],
        });
      }

      const statsSnapshot = await buildProspectorAdminSnapshot(
        MetalworksProspectorUser,
        MetalworksLead,
      );
      const cleanProspector =
        statsSnapshot.prospectors.find((item) => item.id === String(prospectorUser._id || "")) ||
        cleanProspectorUser(prospectorUser.toObject ? prospectorUser.toObject() : prospectorUser);

      res.json({
        prospector: cleanProspector,
        forcedSignOut,
      });
    } catch (error) {
      console.error("Error updating Metal Works prospector account:", error.message);
      respondError(res, 500, "No pude actualizar la cuenta del prospectador.");
    }
  });

  app.post("/api/metalworks-crm/prospectors/:prospectorId/reset-password", async (req, res) => {
    const auth = await requireAuth(req, res);

    if (!auth) {
      return;
    }

    const prospectorId = String(req.params?.prospectorId || "").trim();
    const customPassword = normalizePasswordInput(req.body?.password || "");

    if (!prospectorId || !mongoose.Types.ObjectId.isValid(prospectorId)) {
      return respondError(res, 400, "Prospectador invalido.");
    }

    if (customPassword && !prospectorPasswordIsValid(customPassword)) {
      return respondError(
        res,
        400,
        `El password debe tener al menos ${METALWORKS_PROSPECTOR_PASSWORD_MIN} caracteres.`,
      );
    }

    try {
      const prospectorUser = await MetalworksProspectorUser.findById(prospectorId);

      if (!prospectorUser) {
        return respondError(res, 404, "No encontre ese prospectador.");
      }

      const passwordToSave = customPassword || generateProspectorTemporaryPassword();
      const securePassword = createSecurePasswordHash(passwordToSave);

      prospectorUser.passwordHash = securePassword.hash;
      prospectorUser.passwordSalt = securePassword.salt;
      prospectorUser.updatedAt = new Date();
      await prospectorUser.save();

      await MetalworksProspectorSession.deleteMany({
        $or: [
          { prospectorUserId: prospectorUser._id },
          { prospectorEmail: normalizeEmail(prospectorUser.email || "") },
        ],
      });

      const statsSnapshot = await buildProspectorAdminSnapshot(
        MetalworksProspectorUser,
        MetalworksLead,
      );
      const cleanProspector =
        statsSnapshot.prospectors.find((item) => item.id === String(prospectorUser._id || "")) ||
        cleanProspectorUser(prospectorUser.toObject ? prospectorUser.toObject() : prospectorUser);

      res.json({
        prospector: cleanProspector,
        credentials: {
          email: normalizeEmail(prospectorUser.email || ""),
          temporaryPassword: passwordToSave,
          passwordLabel: customPassword ? "Password" : "Temporary password",
          passwordMode: customPassword ? "custom" : "generated",
        },
        forcedSignOut: true,
      });
    } catch (error) {
      console.error("Error resetting Metal Works prospector password:", error.message);
      respondError(res, 500, "No pude resetear el password de este prospectador.");
    }
  });

  app.get("/api/metalworks-crm/operator/snapshot", async (req, res) => {
    const auth = await requireAuth(req, res);

    if (!auth) {
      return;
    }

    try {
      const dashboard = await buildDashboardSnapshot(
        MetalworksLead,
        MetalworksLeadActivity,
        MetalworksApplicant,
        {},
      );
      const snapshot = buildMetalworksOperatorSnapshot(dashboard);

      res.json({
        email: auth.email,
        profile: getMetalworksCrmProfile(auth.email),
        ...snapshot,
      });
    } catch (error) {
      console.error("Error loading Metal Works operator snapshot:", error.message);
      respondError(res, 500, "No pude cargar la cola movil del operador.");
    }
  });

  app.post("/api/metalworks-crm/operator/chat", async (req, res) => {
    const auth = await requireAuth(req, res);

    if (!auth) {
      return;
    }

    const message = cleanText(req.body?.message || "", 500);
    const leadId = String(req.body?.leadId || "").trim();

    if (!message) {
      return respondError(res, 400, "Escribe un mensaje para el operador.");
    }

    try {
      const dashboard = await buildDashboardSnapshot(
        MetalworksLead,
        MetalworksLeadActivity,
        MetalworksApplicant,
        {},
      );
      const operatorSnapshot = buildMetalworksOperatorSnapshot(dashboard);
      let selectedLead = null;
      let selectedActivity = [];

      if (leadId && mongoose.Types.ObjectId.isValid(leadId)) {
        const [leadDoc, activityDocs] = await Promise.all([
          MetalworksLead.findById(leadId).lean(),
          MetalworksLeadActivity.find({ leadId })
            .sort({ createdAt: -1 })
            .limit(12)
            .lean(),
        ]);

        if (leadDoc) {
          selectedLead = summarizeLeadForOperator(
            cleanLead(leadDoc, { includeConversation: true }),
          );
          selectedActivity = activityDocs.map(cleanActivity).filter(Boolean);
        }
      }

      const result = await generateMetalworksOperatorReply({
        message,
        operatorSnapshot,
        selectedLead,
        selectedActivity,
      });

      res.json({
        ok: true,
        reply: result.reply,
        usedFallback: result.usedFallback,
        reason: result.reason,
      });
    } catch (error) {
      console.error("Error in Metal Works operator chat:", error.message);
      respondError(res, 500, "No pude responder desde el operador movil.");
    }
  });

  app.post("/api/metalworks-crm/login", async (req, res) => {
    const email = normalizeEmail(req.body?.email || "");
    const password = String(req.body?.password || "");
    const allowedEmails = getAllowedEmails();
    const expectedPassword = getMetalworksPasswordForEmail(email);

    if (!metalworksCrmConfigured()) {
      return respondError(
        res,
        503,
        "Primero configura METALWORKS_CRM_PASSWORD o METALWORKS_CRM_USER_PASSWORDS_JSON en el backend.",
      );
    }

    if (!email || !password) {
      return respondError(res, 400, "Correo y password son requeridos.");
    }

    if (!allowedEmails.includes(email)) {
      return respondError(res, 403, "Ese correo no tiene acceso al CRM.");
    }

    if (!expectedPassword) {
      return respondError(res, 403, "Ese correo no tiene password configurado para el CRM.");
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

  app.get("/api/metalworks-crm/push/config", async (req, res) => {
    const auth = await requireAuth(req, res);

    if (!auth) {
      return;
    }

    try {
      const [iosDeviceCount, webDeviceCount] = await Promise.all([
        MetalworksCrmPushDevice.countDocuments({
          adminEmail: auth.email,
          isActive: true,
          notificationsEnabled: true,
        }),
        MetalworksCrmWebPushDevice.countDocuments({
          adminEmail: auth.email,
          isActive: true,
          notificationsEnabled: true,
        }),
      ]);

      res.json({
        ok: true,
        apnsConfigured: metalworksApnsConfigured(),
        webPushConfigured: metalworksWebPushConfigured(),
        vapidPublicKey: metalworksWebPushConfigured()
          ? METALWORKS_WEB_PUSH_VAPID_PUBLIC_KEY
          : "",
        subject: metalworksWebPushConfigured() ? METALWORKS_WEB_PUSH_SUBJECT : "",
        iosDeviceCount,
        webDeviceCount,
      });
    } catch (error) {
      console.error("Error loading Metal Works push config:", error.message);
      respondError(res, 500, "No pude revisar la configuracion de alerts.");
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

  app.post("/api/metalworks-crm/push/web/register", async (req, res) => {
    const auth = await requireAuth(req, res);

    if (!auth) {
      return;
    }

    if (!metalworksWebPushConfigured()) {
      return respondError(
        res,
        503,
        "Web push credentials are not configured yet.",
      );
    }

    const subscription = normalizeWebPushSubscription(req.body?.subscription || null);
    const deviceName = cleanText(req.body?.deviceName || "", 120);
    const browserName = cleanText(req.body?.browserName || "", 80);
    const notificationPath = normalizeMetalworksNotificationPath(
      req.body?.notificationPath || "/metalworks-crm/operator/",
    );
    const authorizationStatus = cleanText(req.body?.authorizationStatus || "", 40);
    const notificationsEnabled =
      req.body?.notificationsEnabled === false ? false : Boolean(subscription);

    if (!subscription) {
      return respondError(res, 400, "Web push subscription is required.");
    }

    try {
      const now = new Date();
      const doc = await MetalworksCrmWebPushDevice.findOneAndUpdate(
        { endpoint: subscription.endpoint },
        {
          $set: {
            adminEmail: auth.email,
            deviceName,
            browserName,
            notificationPath,
            authorizationStatus,
            subscription,
            vapidPublicKey: METALWORKS_WEB_PUSH_VAPID_PUBLIC_KEY,
            notificationsEnabled,
            isActive: notificationsEnabled,
            ipAddress: cleanText(getClientIp(req), 120),
            userAgent: cleanText(req.headers["user-agent"] || "", 400),
            lastSeenAt: now,
            updatedAt: now,
          },
          $setOnInsert: {
            platform: "web",
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
        webPushConfigured: metalworksWebPushConfigured(),
        message: "This browser is ready for live lead alerts.",
        device: cleanWebPushDevice(doc),
      });
    } catch (error) {
      console.error("Error registering Metal Works web push device:", error.message);
      respondError(res, 500, "No pude registrar este navegador para alerts.");
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
          ? "Test push sent to your active device."
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

    const filters = {
      status: req.query?.status || "",
      search: req.query?.search || "",
      projectType: req.query?.projectType || "",
    };
    const hasFilters = Boolean(
      cleanText(filters.status || "", 40) ||
        cleanText(filters.search || "", 120) ||
        cleanText(filters.projectType || "", 80),
    );

    try {
      const [snapshot, agendaSource] = hasFilters
        ? await Promise.all([
            buildDashboardSnapshot(
              MetalworksLead,
              MetalworksLeadActivity,
              MetalworksApplicant,
              filters,
            ),
            buildDashboardSnapshot(
              MetalworksLead,
              MetalworksLeadActivity,
              MetalworksApplicant,
              {},
            ),
          ])
        : [
            await buildDashboardSnapshot(
              MetalworksLead,
              MetalworksLeadActivity,
              MetalworksApplicant,
              filters,
            ),
            null,
          ];
      const agendaSnapshot = buildMetalworksOperatorSnapshot(agendaSource || snapshot);

      res.json({
        ...snapshot,
        agendaLeads: Array.isArray(agendaSnapshot?.agendaLeads)
          ? agendaSnapshot.agendaLeads.slice(0, 8)
          : [],
      });
    } catch (error) {
      console.error("Error loading Metal Works dashboard:", error.message);
      respondError(res, 500, "No pude cargar el dashboard del CRM.");
    }
  });

  app.post("/api/metalworks-crm/leads", async (req, res) => {
    const auth = await requireAuth(req, res);

    if (!auth) {
      return;
    }

    const fullName = cleanText(req.body?.fullName || "", 120);
    const phoneDisplay = cleanText(req.body?.phoneDisplay || "", 40);
    const phone = normalizePhone(phoneDisplay);
    const email = normalizeEmail(req.body?.email || "");
    const projectType = cleanText(req.body?.projectType || "", 120);
    const location = cleanText(req.body?.location || "", 160);
    const details = cleanText(req.body?.details || "", 3000);
    const status = normalizeStatus(req.body?.status || "new");

    if (!fullName) {
      return respondError(res, 400, "El nombre es requerido.");
    }

    try {
      const now = new Date();
      const leadDoc = await MetalworksLead.create({
        fullName,
        phone,
        phoneDisplay,
        email,
        projectType,
        location,
        details,
        status,
        sourceType: "manual_crm_entry",
        sourceExternalSystem: "crm_manual",
        lastContactAt: now,
        updatedAt: now,
        createdAt: now,
      });

      await appendActivity({
        leadId: leadDoc._id,
        activityType: "lead_created",
        title: "Lead creado manualmente",
        body: `${fullName} se agrego manualmente al CRM.`,
        meta: {
          adminEmail: auth.email,
          sourceType: "manual_crm_entry",
          projectType,
          location,
        },
        req,
      });

      const [activityDocs, assets] = await Promise.all([
        MetalworksLeadActivity.find({ leadId: leadDoc._id })
          .sort({ createdAt: -1 })
          .limit(80)
          .lean(),
        listLeadAssets(leadDoc._id),
      ]);

      res.status(201).json({
        lead: cleanLead(leadDoc.toObject ? leadDoc.toObject() : leadDoc, {
          includeConversation: true,
        }),
        assets,
        activity: activityDocs.map(cleanActivity).filter(Boolean),
      });
    } catch (error) {
      console.error("Error creating manual Metal Works lead:", error.message);
      respondError(res, 500, "No pude crear ese lead manual.");
    }
  });

  app.get("/api/metalworks-crm/applicants", async (req, res) => {
    const auth = await requireAuth(req, res);

    if (!auth) {
      return;
    }

    try {
      const applicantDocs = await MetalworksApplicant.find({})
        .sort({ updatedAt: -1, createdAt: -1 })
        .limit(120)
        .lean();

      res.json({
        applicants: applicantDocs.map((doc) => cleanApplicant(doc)).filter(Boolean),
      });
    } catch (error) {
      console.error("Error loading Metal Works applicants:", error.message);
      respondError(res, 500, "No pude cargar los candidatos.");
    }
  });

  app.get("/api/metalworks-crm/applicants/:applicantId", async (req, res) => {
    const auth = await requireAuth(req, res);

    if (!auth) {
      return;
    }

    const applicantId = String(req.params?.applicantId || "").trim();

    if (!applicantId || !mongoose.Types.ObjectId.isValid(applicantId)) {
      return respondError(res, 400, "Candidato invalido.");
    }

    try {
      const [applicantDoc, activityDocs] = await Promise.all([
        MetalworksApplicant.findById(applicantId).lean(),
        MetalworksLeadActivity.find({ applicantId })
          .sort({ createdAt: -1 })
          .limit(80)
          .lean(),
      ]);

      if (!applicantDoc) {
        return respondError(res, 404, "No encontre ese candidato.");
      }

      res.json({
        applicant: cleanApplicant(applicantDoc, { includeConversation: true }),
        activity: activityDocs.map(cleanActivity).filter(Boolean),
      });
    } catch (error) {
      console.error("Error loading Metal Works applicant detail:", error.message);
      respondError(res, 500, "No pude cargar ese candidato.");
    }
  });

  app.patch("/api/metalworks-crm/applicants/:applicantId", async (req, res) => {
    const auth = await requireAuth(req, res);

    if (!auth) {
      return;
    }

    const applicantId = String(req.params?.applicantId || "").trim();

    if (!applicantId || !mongoose.Types.ObjectId.isValid(applicantId)) {
      return respondError(res, 400, "Candidato invalido.");
    }

    try {
      const applicantDoc = await MetalworksApplicant.findById(applicantId);

      if (!applicantDoc) {
        return respondError(res, 404, "No encontre ese candidato.");
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
      const positionApplied = Object.prototype.hasOwnProperty.call(req.body || {}, "positionApplied")
        ? cleanText(req.body?.positionApplied || "", 120)
        : null;
      const languages = Object.prototype.hasOwnProperty.call(req.body || {}, "languages")
        ? cleanText(req.body?.languages || "", 160)
        : null;
      const yearsExperience = Object.prototype.hasOwnProperty.call(req.body || {}, "yearsExperience")
        ? cleanText(req.body?.yearsExperience || "", 80)
        : null;
      const experienceSummary = Object.prototype.hasOwnProperty.call(req.body || {}, "experienceSummary")
        ? cleanText(req.body?.experienceSummary || "", 2400)
        : null;
      const hasTools = Object.prototype.hasOwnProperty.call(req.body || {}, "hasTools")
        ? normalizeApplicantYesNo(req.body?.hasTools || "") || cleanText(req.body?.hasTools || "", 40)
        : null;
      const hasTransportation = Object.prototype.hasOwnProperty.call(req.body || {}, "hasTransportation")
        ? normalizeApplicantYesNo(req.body?.hasTransportation || "") ||
          cleanText(req.body?.hasTransportation || "", 40)
        : null;
      const fieldReady = Object.prototype.hasOwnProperty.call(req.body || {}, "fieldReady")
        ? normalizeApplicantYesNo(req.body?.fieldReady || "") || cleanText(req.body?.fieldReady || "", 40)
        : null;
      const location = Object.prototype.hasOwnProperty.call(req.body || {}, "location")
        ? cleanText(req.body?.location || "", 160)
        : null;
      const bestInterviewDay = Object.prototype.hasOwnProperty.call(req.body || {}, "bestInterviewDay")
        ? cleanText(req.body?.bestInterviewDay || "", 80)
        : null;
      const bestInterviewTime = Object.prototype.hasOwnProperty.call(req.body || {}, "bestInterviewTime")
        ? cleanText(req.body?.bestInterviewTime || "", 80)
        : null;
      const nextStatus = Object.prototype.hasOwnProperty.call(req.body || {}, "status")
        ? normalizeApplicantStatus(req.body?.status || "new")
        : null;
      const nextAction = Object.prototype.hasOwnProperty.call(req.body || {}, "nextAction")
        ? cleanText(req.body?.nextAction || "", 160)
        : null;
      const nextActionAtRaw = Object.prototype.hasOwnProperty.call(req.body || {}, "nextActionAt")
        ? String(req.body?.nextActionAt || "").trim()
        : null;
      const nextActionAt = nextActionAtRaw
        ? parseCrmDatetimeInput(nextActionAtRaw)
        : nextActionAtRaw === ""
          ? null
          : undefined;
      const privateNotes = Object.prototype.hasOwnProperty.call(req.body || {}, "privateNotes")
        ? cleanText(stripApplicantNotesBlock(req.body?.privateNotes || ""), 4000)
        : null;
      const note = cleanText(req.body?.note || "", 600);
      let profileChanged = false;
      let notesChanged = false;
      let privateNotesSeedChanged = false;

      if (fullName !== null) {
        const nextFullName =
          fullName ||
          sanitizeAssistantStoredName(applicantDoc.fullName || "") ||
          METALWORKS_APPLICANT_PLACEHOLDER_NAME;

        if (applicantDoc.fullName !== nextFullName) {
          applicantDoc.fullName = nextFullName;
          profileChanged = true;
        }
      }

      if (phoneDisplayRaw !== null) {
        const nextPhoneDisplay = phoneDisplayRaw || "";

        if (applicantDoc.phone !== phone || applicantDoc.phoneDisplay !== nextPhoneDisplay) {
          applicantDoc.phone = phone || "";
          applicantDoc.phoneDisplay = nextPhoneDisplay;
          profileChanged = true;
        }
      }

      if (email !== null && applicantDoc.email !== email) {
        applicantDoc.email = email;
        profileChanged = true;
      }

      if (positionApplied !== null && applicantDoc.positionApplied !== positionApplied) {
        applicantDoc.positionApplied = positionApplied;
        const inferredRoleTrack = inferApplicantRoleTrack(positionApplied);
        if (inferredRoleTrack && applicantDoc.roleTrack !== inferredRoleTrack) {
          applicantDoc.roleTrack = inferredRoleTrack;
        }
        profileChanged = true;
        privateNotesSeedChanged = true;
      }

      if (languages !== null && applicantDoc.languages !== languages) {
        applicantDoc.languages = languages;
        profileChanged = true;
        privateNotesSeedChanged = true;
      }

      if (yearsExperience !== null && applicantDoc.yearsExperience !== yearsExperience) {
        applicantDoc.yearsExperience = yearsExperience;
        profileChanged = true;
        privateNotesSeedChanged = true;
      }

      if (experienceSummary !== null && applicantDoc.experienceSummary !== experienceSummary) {
        applicantDoc.experienceSummary = experienceSummary;
        profileChanged = true;
        privateNotesSeedChanged = true;
      }

      if (hasTools !== null && applicantDoc.hasTools !== hasTools) {
        applicantDoc.hasTools = hasTools;
        profileChanged = true;
        privateNotesSeedChanged = true;
      }

      if (hasTransportation !== null && applicantDoc.hasTransportation !== hasTransportation) {
        applicantDoc.hasTransportation = hasTransportation;
        profileChanged = true;
        privateNotesSeedChanged = true;
      }

      if (fieldReady !== null && applicantDoc.fieldReady !== fieldReady) {
        applicantDoc.fieldReady = fieldReady;
        profileChanged = true;
        privateNotesSeedChanged = true;
      }

      if (location !== null && applicantDoc.location !== location) {
        applicantDoc.location = location;
        profileChanged = true;
      }

      if (bestInterviewDay !== null && applicantDoc.bestInterviewDay !== bestInterviewDay) {
        applicantDoc.bestInterviewDay = bestInterviewDay;
        profileChanged = true;
        privateNotesSeedChanged = true;
      }

      if (bestInterviewTime !== null && applicantDoc.bestInterviewTime !== bestInterviewTime) {
        applicantDoc.bestInterviewTime = bestInterviewTime;
        profileChanged = true;
        privateNotesSeedChanged = true;
      }

      const currentStatus = normalizeApplicantStatus(applicantDoc.status || "new");
      if (nextStatus && currentStatus !== nextStatus) {
        changes.push(
          `Estado: ${labelApplicantStatus(currentStatus)} -> ${labelApplicantStatus(nextStatus)}`,
        );
        applicantDoc.status = nextStatus;
      }

      if (nextAction !== null && applicantDoc.nextAction !== nextAction) {
        changes.push(`Proxima accion: ${nextAction || "Sin accion"}`);
        applicantDoc.nextAction = nextAction;
      }

      if (nextActionAt !== undefined && String(applicantDoc.nextActionAt || "") !== String(nextActionAt || "")) {
        changes.push(
          `Seguimiento: ${
            nextActionAt instanceof Date && !Number.isNaN(nextActionAt.getTime())
              ? nextActionAt.toLocaleString("en-US")
              : "Sin fecha"
          }`,
        );
        applicantDoc.nextActionAt =
          nextActionAt instanceof Date && !Number.isNaN(nextActionAt.getTime())
            ? nextActionAt
            : null;
      }

      if (privateNotes !== null || privateNotesSeedChanged) {
        const manualNotes =
          privateNotes !== null
            ? privateNotes
            : stripApplicantNotesBlock(applicantDoc.privateNotes || "");
        const mergedPrivateNotes = mergeApplicantPrivateNotes(
          manualNotes,
          buildApplicantPrivateNotesSeed(applicantDoc),
        );

        if (applicantDoc.privateNotes !== mergedPrivateNotes) {
          applicantDoc.privateNotes = mergedPrivateNotes;
          notesChanged = true;
        }
      }

      if (profileChanged) {
        changes.push("Perfil del candidato actualizado");
      }

      if (notesChanged) {
        changes.push("Notas privadas actualizadas");
      }

      if (changes.length || note) {
        applicantDoc.lastContactAt = new Date();
      }

      applicantDoc.updatedAt = new Date();
      await applicantDoc.save();

      if (changes.length) {
        await appendActivity({
          applicantId: applicantDoc._id,
          activityType: "job_applicant_updated",
          title: "Candidato actualizado",
          body: changes.join(". "),
          meta: {
            adminEmail: auth.email,
          },
          req,
        });
      }

      if (note) {
        await appendActivity({
          applicantId: applicantDoc._id,
          activityType: "note_added",
          title: "Nota privada del candidato",
          body: note,
          meta: {
            adminEmail: auth.email,
          },
          req,
        });
      }

      const [updatedApplicant, activityDocs] = await Promise.all([
        MetalworksApplicant.findById(applicantId).lean(),
        MetalworksLeadActivity.find({ applicantId })
          .sort({ createdAt: -1 })
          .limit(80)
          .lean(),
      ]);

      res.json({
        applicant: cleanApplicant(updatedApplicant, { includeConversation: true }),
        activity: activityDocs.map(cleanActivity).filter(Boolean),
      });
    } catch (error) {
      console.error("Error updating Metal Works applicant:", error.message);
      respondError(res, 500, "No pude guardar ese candidato.");
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
        ? parseCrmDatetimeInput(nextActionAtRaw)
        : nextActionAtRaw === ""
          ? null
          : undefined;
      const nextActionReminderOffsets = Object.prototype.hasOwnProperty.call(
        req.body || {},
        "nextActionReminderOffsets",
      )
        ? normalizeLeadReminderOffsets(req.body?.nextActionReminderOffsets || [])
        : null;
      const privateNotes = Object.prototype.hasOwnProperty.call(req.body || {}, "privateNotes")
        ? cleanMultilineText(req.body?.privateNotes || "", 12000)
        : null;
      const textThreadImportSource = Object.prototype.hasOwnProperty.call(
        req.body || {},
        "textThreadImportSource",
      )
        ? cleanText(req.body?.textThreadImportSource || "", 60)
        : "";
      const textThreadImport = Object.prototype.hasOwnProperty.call(req.body || {}, "textThreadImport")
        ? cleanMultilineText(req.body?.textThreadImport || "", 8000)
        : null;
      const estimateAmount = Object.prototype.hasOwnProperty.call(req.body || {}, "estimateAmount")
        ? normalizeMoney(req.body?.estimateAmount || 0)
        : null;
      const invoiceDepositAmount = Object.prototype.hasOwnProperty.call(req.body || {}, "invoiceDepositAmount")
        ? normalizeMoney(req.body?.invoiceDepositAmount || 0)
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
      let invoiceDepositChanged = false;
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

      if (nextActionReminderOffsets !== null) {
        const currentReminderOffsets = normalizeLeadReminderOffsets(
          leadDoc.nextActionReminderOffsets || [],
        );

        if (
          JSON.stringify(currentReminderOffsets) !== JSON.stringify(nextActionReminderOffsets)
        ) {
          changes.push(
            nextActionReminderOffsets.length
              ? `Reminders: ${nextActionReminderOffsets
                  .map((value) => formatLeadReminderOffsetLabel(value))
                  .filter(Boolean)
                  .join(", ")}`
              : "Reminders: Off",
          );
        }

        leadDoc.nextActionReminderOffsets = nextActionReminderOffsets;
      }

      if (privateNotes !== null || textThreadImport) {
        const nextPrivateNotesBase =
          privateNotes !== null
            ? privateNotes
            : cleanMultilineText(leadDoc.privateNotes || "", 12000);
        const nextPrivateNotes = textThreadImport
          ? mergeLeadTextImportIntoPrivateNotes(nextPrivateNotesBase, textThreadImport, {
              sourceLabel: textThreadImportSource || "Text thread",
            })
          : nextPrivateNotesBase;

        if (leadDoc.privateNotes !== nextPrivateNotes) {
          leadDoc.privateNotes = nextPrivateNotes;
          changes.push(
            textThreadImport ? "Text thread guardado en notas privadas" : "Notas privadas actualizadas",
          );
        }
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

      const nextInvoiceDepositAmount =
        invoiceDepositAmount !== null
          ? invoiceDepositAmount
          : normalizeMoney(leadDoc.invoiceDepositAmount || 0);

      if (nextInvoiceDepositAmount > 0 && normalizeMoney(leadDoc.estimateAmount || 0) <= 0) {
        return respondError(res, 400, "Add the total before recording a deposit.");
      }

      if (nextInvoiceDepositAmount > normalizeMoney(leadDoc.estimateAmount || 0)) {
        return respondError(res, 400, "Deposit can't be higher than the total.");
      }

      if (invoiceDepositAmount !== null) {
        if (normalizeMoney(leadDoc.invoiceDepositAmount || 0) !== invoiceDepositAmount) {
          clientDocumentChanged = true;
          invoiceDepositChanged = true;
        }

        leadDoc.invoiceDepositAmount = invoiceDepositAmount;
      }

      if (estimateChanged) {
        changes.push(`Estimate: ${formatMoneyLabel(leadDoc.estimateAmount || 0)}`);
      }

      if (invoiceDepositChanged) {
        changes.push(`Invoice deposit: ${formatMoneyLabel(leadDoc.invoiceDepositAmount || 0)}`);
      }

      if (clientDocumentChanged) {
        changes.push("Documento para cliente actualizado");
      }

      if (profileChanged) {
        changes.push("Perfil del cliente actualizado");
      }

      if (nextActionAt !== undefined || nextActionReminderOffsets !== null) {
        leadDoc.nextActionReminderSentKeys = pruneLeadReminderSentKeys(
          leadDoc.nextActionReminderSentKeys || [],
          leadDoc.nextActionAt,
          leadDoc.nextActionReminderOffsets || [],
        );
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

      if (textThreadImport) {
        await appendActivity({
          leadId: leadDoc._id,
          activityType: "text_thread_imported",
          title: "Text thread imported",
          body: cleanText(
            `${textThreadImportSource || "Text thread"} saved to private notes.`,
            280,
          ),
          meta: {
            adminEmail: auth.email,
            source: textThreadImportSource || "Text thread",
            charCount: textThreadImport.length,
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

  app.delete("/api/metalworks-crm/leads/:leadId", async (req, res) => {
    const auth = await requireAuth(req, res);

    if (!auth) {
      return;
    }

    const leadId = String(req.params?.leadId || "").trim();

    if (!leadId || !mongoose.Types.ObjectId.isValid(leadId)) {
      return respondError(res, 400, "Lead invalido.");
    }

    try {
      const leadDoc = await MetalworksLead.findById(leadId).select("fullName");

      if (!leadDoc) {
        return respondError(res, 404, "No encontre ese lead.");
      }

      const now = new Date();

      await Promise.all([
        MetalworksLeadActivity.deleteMany({ leadId: leadDoc._id }),
        MetalworksLeadAsset.deleteMany({ leadId: leadDoc._id }),
        MetalworksPublicChatWebPushDevice.updateMany(
          { leadId: leadDoc._id },
          {
            $set: {
              leadId: null,
              updatedAt: now,
            },
          },
        ),
        MetalworksLead.deleteOne({ _id: leadDoc._id }),
      ]);

      res.json({
        ok: true,
        deletedLeadId: leadId,
        deletedLeadName: cleanText(leadDoc.fullName || "", 120),
        deletedBy: auth.email,
      });
    } catch (error) {
      console.error("Error deleting Metal Works lead:", error.message);
      respondError(res, 500, "No pude borrar ese lead.");
    }
  });

  app.post("/api/metalworks-crm/leads/:leadId/assets", async (req, res) => {
    const auth = await requireAuth(req, res);

    if (!auth) {
      return;
    }

    const leadId = String(req.params?.leadId || "").trim();

    if (!leadId || !mongoose.Types.ObjectId.isValid(leadId)) {
      return respondError(res, 400, "Lead invalido.");
    }

    const filePayloads = Array.isArray(req.body?.files) ? req.body.files : [];

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

    try {
      const leadDoc = await MetalworksLead.findById(leadId);

      if (!leadDoc) {
        return respondError(res, 404, "No encontre ese lead.");
      }

      const parsedFiles = filePayloads.map((item) => parseAssistantLeadAssetUpload(item));
      const totalBytes = parsedFiles.reduce((sum, item) => sum + (item.sizeBytes || 0), 0);

      if (totalBytes > METALWORKS_LEAD_ASSET_MAX_TOTAL_BYTES) {
        return respondError(res, 400, "Total image upload is too large.");
      }

      const now = new Date();
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
      const assetDocs = await Promise.all(
        newFiles.map((item) =>
          MetalworksLeadAsset.create({
            leadId: leadDoc._id,
            sourceType: "crm_manual_photo",
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

      if (assetDocs.length) {
        leadDoc.photoFileNames = mergeAssistantUniqueValues(
          leadDoc.photoFileNames || [],
          newFiles.map((item) => item.fileName),
        );
        leadDoc.updatedAt = now;
        await leadDoc.save();

        await appendActivity({
          leadId: leadDoc._id,
          activityType: "crm_photo_uploaded",
          title: "Fotos agregadas manualmente",
          body: `Se agregaron ${assetDocs.length} foto${assetDocs.length === 1 ? "" : "s"} desde el CRM.`,
          meta: {
            adminEmail: auth.email,
            fileNames: newFiles.map((item) => item.fileName),
          },
          req,
        });
      }

      const [updatedLead, activityDocs, assets] = await Promise.all([
        MetalworksLead.findById(leadId).lean(),
        MetalworksLeadActivity.find({ leadId })
          .sort({ createdAt: -1 })
          .limit(80)
          .lean(),
        listLeadAssets(leadId),
      ]);

      res.json({
        ok: true,
        uploadedCount: assetDocs.length,
        lead: cleanLead(updatedLead, { includeConversation: true }),
        assets,
        activity: activityDocs.map(cleanActivity).filter(Boolean),
      });
    } catch (error) {
      console.error("Error saving manual Metal Works photos:", error.message);
      respondError(res, 500, error?.message || "No pude guardar esas fotos.");
    }
  });

  app.post("/api/metalworks-crm/leads/:leadId/live-chat-reply", async (req, res) => {
    const auth = await requireAuth(req, res);

    if (!auth) {
      return;
    }

    const leadId = String(req.params?.leadId || "").trim();

    if (!leadId || !mongoose.Types.ObjectId.isValid(leadId)) {
      return respondError(res, 400, "Lead invalido.");
    }

    const message = cleanText(req.body?.message || "", 500);

    if (!message) {
      return respondError(res, 400, "El mensaje es requerido.");
    }

    try {
      const leadDoc = await MetalworksLead.findById(leadId);

      if (!leadDoc) {
        return respondError(res, 404, "No encontre ese lead.");
      }

      if (!isWebsiteLiveChatLead(leadDoc)) {
        return respondError(res, 400, "Este lead no usa el chat web conectado.");
      }

      const now = new Date();
      const currentStatus = normalizeStatus(leadDoc.status || "new");

      leadDoc.conversationHistory = mergeConversationHistory(leadDoc.conversationHistory || [], [
        {
          role: "assistant",
          content: message,
          createdAt: now,
        },
      ]);
      leadDoc.lastAssistantMessage = message;
      leadDoc.lastContactAt = now;
      leadDoc.updatedAt = now;

      if (currentStatus === "new") {
        leadDoc.status = "contacted";
      }

      await leadDoc.save();

      await appendActivity({
        leadId: leadDoc._id,
        activityType: "website_live_chat_reply",
        title: "Respuesta del CRM",
        body: message,
        meta: {
          adminEmail: auth.email,
          sourceType: leadDoc.sourceType || METALWORKS_WEBSITE_CHAT_SOURCE_TYPE,
        },
        req,
      });

      try {
        await sendWebsiteLiveChatReplyPushAlert({
          lead: leadDoc.toObject ? leadDoc.toObject() : leadDoc,
          message,
        });
      } catch (error) {
        console.error("Error sending website live chat reply push:", error.message);
      }

      const [activityDocs, assets] = await Promise.all([
        MetalworksLeadActivity.find({ leadId: leadDoc._id })
          .sort({ createdAt: -1 })
          .limit(80)
          .lean(),
        listLeadAssets(leadDoc._id),
      ]);

      res.json({
        ok: true,
        lead: cleanLead(leadDoc.toObject ? leadDoc.toObject() : leadDoc, {
          includeConversation: true,
        }),
        assets,
        activity: activityDocs.map(cleanActivity).filter(Boolean),
      });
    } catch (error) {
      console.error("Error replying to Metal Works website chat:", error.message);
      respondError(res, 500, "No pude mandar la respuesta al chat web.");
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
      const hasTracking = Object.values(tracking).some(Boolean);
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
      const { duplicate, syncedAssetCount, photoFileNames, leadDoc } =
        await upsertExternalLeadRecord({
          externalLeadId,
          externalSystem,
          fullName,
          phone,
          phoneDisplay,
          email,
          projectType,
          location,
          details,
          sourceType,
          pageTitle,
          pagePath,
          pageUrl,
          referrer,
          tracking,
          parsedFiles,
          rawPhotoFileNames,
          req,
          crmStatus: "new",
          requirePhone: true,
        });

      if (!duplicate) {
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
      respondError(
        res,
        error?.statusCode || 500,
        error?.statusCode ? error.message : "No pude sincronizar este lead externo.",
      );
    }
  });

  app.post(
    ["/integrations/thumbtack/webhook", "/api/integrations/thumbtack/webhook"],
    async (req, res) => {
      try {
        if (!thumbtackWebhookConfigured()) {
          return respondError(
            res,
            503,
            "Thumbtack webhook auth is not configured yet.",
          );
        }

        if (!requestHasThumbtackWebhookAccess(req)) {
          res.setHeader("WWW-Authenticate", 'Basic realm="Thumbtack Webhook"');
          return respondError(res, 401, "Unauthorized Thumbtack webhook request.");
        }

        const parsedEvent = buildThumbtackWebhookEvent(req.body || {});
        const thumbtackEventKey = buildThumbtackExternalEventKey(parsedEvent);
        const tracking = buildTrackingPayload({
          utmSource: "thumbtack",
          utmMedium: "webhook",
          utmCampaign: parsedEvent.eventType || "",
        });
        const pagePath = "/integrations/thumbtack/webhook";
        const pageUrl = METALWORKS_THUMBTACK_PROFILE_URL;
        let leadDoc = null;
        let duplicate = false;
        let syncedAssetCount = 0;
        let attachmentImport = {
          attemptedCount: 0,
          importedCount: 0,
          skippedCount: 0,
          errors: [],
        };

        if (parsedEvent.leadCandidate) {
          if (Array.isArray(parsedEvent.leadCandidate.attachments) && parsedEvent.leadCandidate.attachments.length) {
            try {
              attachmentImport = await fetchExternalLeadAssetUploads(
                parsedEvent.leadCandidate.attachments,
                {
                  fallbackPrefix: "thumbtack-photo",
                },
              );
            } catch (error) {
              attachmentImport = {
                attemptedCount: Array.isArray(parsedEvent.leadCandidate.attachments)
                  ? parsedEvent.leadCandidate.attachments.length
                  : 0,
                importedCount: 0,
                skippedCount: Array.isArray(parsedEvent.leadCandidate.attachments)
                  ? parsedEvent.leadCandidate.attachments.length
                  : 0,
                errors: [
                  {
                    url: "",
                    message: cleanText(error?.message || "Thumbtack attachment import failed.", 240),
                  },
                ],
              };
            }
          }

          const syncResult = await upsertExternalLeadRecord({
            externalLeadId: parsedEvent.leadCandidate.externalLeadId,
            externalSystem: parsedEvent.leadCandidate.externalSystem,
            fullName: parsedEvent.leadCandidate.fullName,
            phone: parsedEvent.leadCandidate.phone,
            phoneDisplay: parsedEvent.leadCandidate.phoneDisplay,
            email: parsedEvent.leadCandidate.email,
            projectType: parsedEvent.leadCandidate.projectType,
            location: parsedEvent.leadCandidate.location,
            addressLine: parsedEvent.leadCandidate.addressLine,
            zipCode: parsedEvent.leadCandidate.zipCode,
            city: parsedEvent.leadCandidate.city,
            details: parsedEvent.leadCandidate.details,
            sourceType: parsedEvent.leadCandidate.sourceType,
            pageTitle: "Thumbtack",
            pagePath,
            pageUrl,
            referrer: pageUrl,
            tracking,
            parsedFiles: attachmentImport.parsedFiles || [],
            rawPhotoFileNames: [],
            req,
            crmStatus: parsedEvent.leadCandidate.crmStatus || "new",
            requirePhone: false,
          });

          leadDoc = syncResult.leadDoc;
          duplicate = syncResult.duplicate;
          syncedAssetCount = syncResult.syncedAssetCount;

          if (!duplicate) {
            await appendActivity({
              leadId: leadDoc._id,
              activityType: "lead_created",
              title: "Lead creado",
              body: `${parsedEvent.leadCandidate.fullName} entro desde Thumbtack.`,
              meta: {
                externalLeadId: parsedEvent.leadCandidate.externalLeadId,
                externalSystem: "thumbtack",
                eventType: parsedEvent.eventType,
                entityType: parsedEvent.entityType,
                attachmentAttemptedCount: attachmentImport.attemptedCount || 0,
                attachmentImportedCount: attachmentImport.importedCount || 0,
              },
              externalEventKey: thumbtackEventKey ? `${thumbtackEventKey}:lead_created` : "",
              req,
              pagePath,
              pageUrl,
              tracking,
            });
          }
        }

        if (!leadDoc && parsedEvent.entityType === "review") {
          leadDoc = await findThumbtackLeadByNegotiationId(
            parsedEvent.activity?.meta?.negotiationId || "",
          );
        }

        await appendActivity({
          leadId: leadDoc?._id || null,
          activityType: parsedEvent.activity.activityType,
          title: parsedEvent.activity.title,
          body: parsedEvent.activity.body,
          meta: {
            ...(parsedEvent.activity.meta || {}),
            duplicate,
            syncedAssetCount,
            attachmentAttemptedCount: attachmentImport.attemptedCount || 0,
            attachmentImportedCount: attachmentImport.importedCount || 0,
            attachmentSkippedCount: attachmentImport.skippedCount || 0,
            attachmentImportErrors: Array.isArray(attachmentImport.errors)
              ? attachmentImport.errors
              : [],
            webhookPayload: req.body || {},
          },
          externalEventKey: thumbtackEventKey,
          req,
          pagePath,
          pageUrl,
          tracking,
        });

        let pushDelivery = {
          attempted: false,
          delivered: false,
        };

        if (leadDoc && !duplicate) {
          try {
            pushDelivery = await sendMetalworksPushAlert({
              lead: leadDoc.toObject ? leadDoc.toObject() : leadDoc,
              alertType: "website_lead",
            });
          } catch (error) {
            console.error("Error sending Thumbtack webhook push:", error.message);
          }
        }

        res.json({
          ok: true,
          eventType: parsedEvent.eventType || "",
          entityType: parsedEvent.entityType || "unknown",
          duplicate,
          syncedAssetCount,
          notified: Boolean(pushDelivery.delivered),
          lead: leadDoc ? cleanLead(leadDoc.toObject ? leadDoc.toObject() : leadDoc) : null,
        });
      } catch (error) {
        console.error("Error handling Thumbtack webhook:", error.message);
        respondError(
          res,
          error?.statusCode || 500,
          error?.statusCode ? error.message : "No pude procesar el webhook de Thumbtack.",
        );
      }
    },
  );

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

      let leadDoc = await resolveConversationLead({
        visitorId,
        sessionId,
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
      const assetDocs = await Promise.all(
        newFiles.map((item) =>
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
      let pushDelivery = {
        attempted: false,
        delivered: false,
      };

      if (assetDocs.length) {
        leadDoc.photoFileNames = mergeAssistantUniqueValues(
          leadDoc.photoFileNames || [],
          newFiles.map((item) => item.fileName),
        );
        leadDoc.lastContactAt = now;
        leadDoc.updatedAt = now;
        await leadDoc.save();

        await appendActivity({
          leadId: leadDoc._id,
          activityType: "assistant_photo_uploaded",
          title: "Fotos subidas desde assistant",
          body: `El visitante subio ${assetDocs.length} foto${assetDocs.length === 1 ? "" : "s"} en el chat.`,
          meta: {
            visitorId,
            sessionId,
            pageTitle,
            fileNames: newFiles.map((item) => item.fileName),
          },
          req,
          pagePath,
          pageUrl,
          tracking,
        });

        try {
          pushDelivery = await sendMetalworksPushAlert({
            lead: leadDoc.toObject ? leadDoc.toObject() : leadDoc,
            alertType: "assistant_photo_uploaded",
          });
        } catch (error) {
          console.error("Error sending Metal Works assistant photo push:", error.message);
        }
      }

      res.json({
        ok: true,
        uploadedCount: assetDocs.length,
        leadId: String(leadDoc._id),
        notified: Boolean(pushDelivery.delivered),
        assets: assetDocs.map(cleanLeadAsset).filter(Boolean),
      });
    } catch (error) {
      console.error("Error saving Metal Works assistant photos:", error.message);
      respondError(res, 500, error?.message || "I could not save those photos.");
    }
  });

  app.get("/api/public/metalworks/live-chat/push/config", async (req, res) => {
    try {
      res.json({
        ok: true,
        webPushConfigured: metalworksWebPushConfigured(),
        vapidPublicKey: metalworksWebPushConfigured()
          ? METALWORKS_WEB_PUSH_VAPID_PUBLIC_KEY
          : "",
        subject: metalworksWebPushConfigured() ? METALWORKS_WEB_PUSH_SUBJECT : "",
      });
    } catch (error) {
      console.error("Error loading public chat push config:", error.message);
      respondError(res, 500, "I could not load push configuration.");
    }
  });

  app.post("/api/public/metalworks/live-chat/push/register", async (req, res) => {
    if (!metalworksWebPushConfigured()) {
      return respondError(res, 503, "Web push credentials are not configured yet.");
    }

    const subscription = normalizeWebPushSubscription(req.body?.subscription || null);
    const visitorId = cleanText(req.body?.visitorId || "", 120);
    const sessionId = cleanText(req.body?.sessionId || "", 120);
    const threadKey = getPublicChatThreadKey(req);
    const deviceName = cleanText(req.body?.deviceName || "", 120);
    const browserName = cleanText(req.body?.browserName || "", 80);
    const notificationPath = normalizeMetalworksNotificationPath(
      req.body?.notificationPath || "/metalworks-chat/",
      "/metalworks-chat/",
    );
    const authorizationStatus = cleanText(req.body?.authorizationStatus || "", 40);
    const notificationsEnabled =
      req.body?.notificationsEnabled === false ? false : Boolean(subscription);

    if (!subscription) {
      return respondError(res, 400, "La suscripcion web es requerida.");
    }

    if (!visitorId && !sessionId && !threadKey) {
      return respondError(res, 400, "Missing live chat visitor session.");
    }

    try {
      const now = new Date();
      const leadDoc = await resolveWebsiteLiveChatLead({
        visitorId,
        sessionId,
        threadKey,
      });
      let resolvedThreadKey = normalizePublicChatThreadKey(threadKey);

      if (leadDoc?._id) {
        const previousVisitorCount = Array.isArray(leadDoc.visitorIds) ? leadDoc.visitorIds.length : 0;
        const previousSessionCount = Array.isArray(leadDoc.sessionIds) ? leadDoc.sessionIds.length : 0;
        const previousThreadKey = normalizePublicChatThreadKey(leadDoc.publicChatThreadKey || "");
        resolvedThreadKey = ensureWebsiteLiveChatThreadKey(leadDoc, threadKey);
        leadDoc.visitorIds = mergeAssistantUniqueValues(leadDoc.visitorIds || [], visitorId);
        leadDoc.sessionIds = mergeAssistantUniqueValues(leadDoc.sessionIds || [], sessionId);
        const visitorCount = Array.isArray(leadDoc.visitorIds) ? leadDoc.visitorIds.length : 0;
        const sessionCount = Array.isArray(leadDoc.sessionIds) ? leadDoc.sessionIds.length : 0;
        const identityChanged =
          visitorCount !== previousVisitorCount ||
          sessionCount !== previousSessionCount ||
          normalizePublicChatThreadKey(leadDoc.publicChatThreadKey || "") !== previousThreadKey;

        if (identityChanged) {
          leadDoc.updatedAt = now;
          await leadDoc.save();
          await syncPublicChatPushDevicesToLead(leadDoc);
        }
      }

      if (resolvedThreadKey) {
        setPublicChatThreadCookie(res, req, resolvedThreadKey);
      }
      const doc = await MetalworksPublicChatWebPushDevice.findOneAndUpdate(
        { endpoint: subscription.endpoint },
        {
          $set: {
            leadId: leadDoc?._id || null,
            visitorId,
            sessionId,
            deviceName,
            browserName,
            notificationPath,
            authorizationStatus,
            subscription,
            vapidPublicKey: METALWORKS_WEB_PUSH_VAPID_PUBLIC_KEY,
            notificationsEnabled,
            isActive: notificationsEnabled,
            ipAddress: cleanText(getClientIp(req), 120),
            userAgent: cleanText(req.headers["user-agent"] || "", 400),
            lastSeenAt: now,
            updatedAt: now,
          },
          $setOnInsert: {
            platform: "web",
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
        webPushConfigured: metalworksWebPushConfigured(),
        message: "This chat is ready for live reply alerts.",
        device: cleanPublicChatWebPushDevice(doc),
      });
    } catch (error) {
      console.error("Error registering public chat web push device:", error.message);
      respondError(res, 500, "Could not register this browser for chat alerts.");
    }
  });

  app.post("/api/public/metalworks/live-chat/thread", async (req, res) => {
    try {
      const visitorId = cleanText(req.body?.visitorId || "", 120);
      const sessionId = cleanText(req.body?.sessionId || "", 120);
      const threadKey = getPublicChatThreadKey(req);

      if (!visitorId && !sessionId && !threadKey) {
        return respondError(res, 400, "Missing live chat visitor session.");
      }

      const leadDoc = await resolveWebsiteLiveChatLead({
        visitorId,
        sessionId,
        threadKey,
      });

      if (leadDoc?._id) {
        const previousVisitorCount = Array.isArray(leadDoc.visitorIds) ? leadDoc.visitorIds.length : 0;
        const previousSessionCount = Array.isArray(leadDoc.sessionIds) ? leadDoc.sessionIds.length : 0;
        const previousThreadKey = normalizePublicChatThreadKey(leadDoc.publicChatThreadKey || "");
        const resolvedThreadKey = ensureWebsiteLiveChatThreadKey(leadDoc, threadKey);
        leadDoc.visitorIds = mergeAssistantUniqueValues(leadDoc.visitorIds || [], visitorId);
        leadDoc.sessionIds = mergeAssistantUniqueValues(leadDoc.sessionIds || [], sessionId);
        const visitorCount = Array.isArray(leadDoc.visitorIds) ? leadDoc.visitorIds.length : 0;
        const sessionCount = Array.isArray(leadDoc.sessionIds) ? leadDoc.sessionIds.length : 0;
        const identityChanged =
          visitorCount !== previousVisitorCount ||
          sessionCount !== previousSessionCount ||
          normalizePublicChatThreadKey(leadDoc.publicChatThreadKey || "") !== previousThreadKey;

        if (identityChanged) {
          leadDoc.updatedAt = new Date();
          await leadDoc.save();
          await syncPublicChatPushDevicesToLead(leadDoc);
        }

        setPublicChatThreadCookie(res, req, resolvedThreadKey);
      }

      res.json({
        ok: true,
        thread: cleanPublicWebsiteLiveChatThread(leadDoc),
      });
    } catch (error) {
      console.error("Error loading Metal Works website chat thread:", error.message);
      respondError(res, 500, "I could not load this conversation.");
    }
  });

  app.post("/api/public/metalworks/live-chat/photos", async (req, res) => {
    try {
      const visitorId = cleanText(req.body?.visitorId || "", 120);
      const sessionId = cleanText(req.body?.sessionId || "", 120);
      const threadKey = getPublicChatThreadKey(req);
      const fullName = sanitizeAssistantStoredName(req.body?.profile?.fullName || "");
      const phoneDisplay = cleanText(req.body?.profile?.phoneDisplay || "", 40);
      const phone = normalizePhone(phoneDisplay);
      const email = normalizeEmail(req.body?.profile?.email || "");
      const pageTitle = cleanText(req.body?.pageTitle || "", 160);
      const pagePath = cleanText(req.body?.pagePath || "", 240);
      const pageUrl = cleanText(req.body?.pageUrl || "", 500);
      const referrer = cleanText(req.body?.referrer || "", 500);
      const tracking = buildTrackingPayload(req.body?.tracking || {});
      const hasTracking = Object.values(tracking).some(Boolean);
      const filePayloads = Array.isArray(req.body?.files) ? req.body.files : [];

      if (!visitorId && !sessionId && !threadKey) {
        return respondError(res, 400, "Missing live chat visitor session.");
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

      let leadDoc = await resolveWebsiteLiveChatLead({
        visitorId,
        sessionId,
        threadKey,
      });
      const now = new Date();

      if (!leadDoc) {
        leadDoc = new MetalworksLead();
      }

      leadDoc.fullName =
        fullName ||
        sanitizeAssistantStoredName(leadDoc.fullName || "") ||
        buildWebsiteLiveChatPlaceholderName(visitorId || sessionId);
      leadDoc.phone = phone || leadDoc.phone || "";
      leadDoc.phoneDisplay = phoneDisplay || leadDoc.phoneDisplay || leadDoc.phone || "";
      leadDoc.email = email || leadDoc.email || "";
      leadDoc.projectType = cleanText(leadDoc.projectType || "", 120) || "Website chat";
      leadDoc.details =
        cleanText(leadDoc.details || "", 3000) ||
        "Visitor uploaded project photos from the website chat.";
      leadDoc.status = normalizeStatus(leadDoc.status || "new");
      leadDoc.sourceType = METALWORKS_WEBSITE_CHAT_SOURCE_TYPE;
      leadDoc.sourceExternalSystem = "website_live_chat";
      leadDoc.publicChatThreadKey = ensureWebsiteLiveChatThreadKey(leadDoc, threadKey);
      leadDoc.sourceExternalId =
        cleanText(leadDoc.sourceExternalId || visitorId || sessionId, 120) || "";
      leadDoc.pageTitle = cleanText(pageTitle || leadDoc.pageTitle || "", 160);
      leadDoc.pagePath = cleanText(pagePath || leadDoc.pagePath || "", 240);
      leadDoc.pageUrl = cleanText(pageUrl || leadDoc.pageUrl || "", 500);
      leadDoc.referrer = cleanText(referrer || leadDoc.referrer || "", 500);
      leadDoc.ipAddress = cleanText(getClientIp(req), 120);
      leadDoc.userAgent = cleanText(req.headers["user-agent"] || "", 400);
      leadDoc.tracking = hasTracking
        ? tracking
        : buildTrackingPayload(leadDoc.tracking || {});
      leadDoc.visitorIds = mergeAssistantUniqueValues(leadDoc.visitorIds || [], visitorId);
      leadDoc.sessionIds = mergeAssistantUniqueValues(leadDoc.sessionIds || [], sessionId);
      leadDoc.lastContactAt = now;
      leadDoc.updatedAt = now;

      if (!leadDoc.createdAt) {
        leadDoc.createdAt = now;
      }

      await leadDoc.save();
      setPublicChatThreadCookie(res, req, leadDoc.publicChatThreadKey || "");

      const assetDocs = await Promise.all(
        parsedFiles.map((item) =>
          MetalworksLeadAsset.create({
            leadId: leadDoc._id,
            visitorId,
            sessionId,
            sourceType: "website_live_chat_photo",
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
      leadDoc.updatedAt = new Date();
      await leadDoc.save();
      await syncPublicChatPushDevicesToLead(leadDoc);

      await appendActivity({
        leadId: leadDoc._id,
        activityType: "website_live_chat_photo_uploaded",
        title: "Fotos subidas desde chat web",
        body: `El visitante subio ${assetDocs.length} foto${assetDocs.length === 1 ? "" : "s"} desde el chat directo.`,
        meta: {
          visitorId,
          sessionId,
          pageTitle,
          fileNames: parsedFiles.map((item) => item.fileName),
          sourceType: METALWORKS_WEBSITE_CHAT_SOURCE_TYPE,
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
        thread: cleanPublicWebsiteLiveChatThread(leadDoc),
      });
    } catch (error) {
      console.error("Error saving Metal Works website chat photos:", error.message);
      respondError(res, 500, error?.message || "I could not save those photos.");
    }
  });

  app.post("/api/public/metalworks/live-chat/messages", async (req, res) => {
    try {
      const message = cleanText(req.body?.message || "", 500);
      const visitorId = cleanText(req.body?.visitorId || "", 120);
      const sessionId = cleanText(req.body?.sessionId || "", 120);
      const threadKey = getPublicChatThreadKey(req);
      const fullName = sanitizeAssistantStoredName(req.body?.profile?.fullName || "");
      const phoneDisplay = cleanText(req.body?.profile?.phoneDisplay || "", 40);
      const phone = normalizePhone(phoneDisplay);
      const email = normalizeEmail(req.body?.profile?.email || "");
      const pageTitle = cleanText(req.body?.pageTitle || "", 160);
      const pagePath = cleanText(req.body?.pagePath || "", 240);
      const pageUrl = cleanText(req.body?.pageUrl || "", 500);
      const referrer = cleanText(req.body?.referrer || "", 500);
      const tracking = buildTrackingPayload(req.body?.tracking || {});
      const hasTracking = Object.values(tracking).some(Boolean);

      if (!message) {
        return respondError(res, 400, "Message is required.");
      }

      if (!visitorId && !sessionId && !threadKey) {
        return respondError(res, 400, "Missing live chat visitor session.");
      }

      let leadDoc = await resolveWebsiteLiveChatLead({
        visitorId,
        sessionId,
        threadKey,
      });
      const leadExistedBeforeMessage = Boolean(leadDoc?._id);
      const now = new Date();
      const existingName = sanitizeAssistantStoredName(leadDoc?.fullName || "");
      const existingDetails = cleanText(leadDoc?.details || "", 3000);
      const previousLastUserMessage = cleanText(leadDoc?.lastUserMessage || "", 500);
      const currentStatus = normalizeStatus(leadDoc?.status || "new");

      if (!leadDoc) {
        leadDoc = new MetalworksLead();
      }

      leadDoc.fullName =
        fullName ||
        existingName ||
        buildWebsiteLiveChatPlaceholderName(visitorId || sessionId);
      leadDoc.phone = phone || leadDoc.phone || "";
      leadDoc.phoneDisplay = phoneDisplay || leadDoc.phoneDisplay || leadDoc.phone || "";
      leadDoc.email = email || leadDoc.email || "";
      leadDoc.projectType = cleanText(leadDoc.projectType || "", 120) || "Website chat";
      leadDoc.details =
        !existingDetails || existingDetails === previousLastUserMessage
          ? selectAssistantLongestText(message, existingDetails)
          : existingDetails;
      leadDoc.status =
        ["won", "lost", "archived"].includes(currentStatus) ? currentStatus : currentStatus || "new";
      leadDoc.sourceType = METALWORKS_WEBSITE_CHAT_SOURCE_TYPE;
      leadDoc.sourceExternalSystem = "website_live_chat";
      leadDoc.publicChatThreadKey = ensureWebsiteLiveChatThreadKey(leadDoc, threadKey);
      leadDoc.sourceExternalId =
        cleanText(leadDoc.sourceExternalId || visitorId || sessionId, 120) || "";
      leadDoc.pageTitle = cleanText(pageTitle || leadDoc.pageTitle || "", 160);
      leadDoc.pagePath = cleanText(pagePath || leadDoc.pagePath || "", 240);
      leadDoc.pageUrl = cleanText(pageUrl || leadDoc.pageUrl || "", 500);
      leadDoc.referrer = cleanText(referrer || leadDoc.referrer || "", 500);
      leadDoc.ipAddress = cleanText(getClientIp(req), 120);
      leadDoc.userAgent = cleanText(req.headers["user-agent"] || "", 400);
      leadDoc.tracking = hasTracking
        ? tracking
        : buildTrackingPayload(leadDoc.tracking || {});
      leadDoc.visitorIds = mergeAssistantUniqueValues(leadDoc.visitorIds || [], visitorId);
      leadDoc.sessionIds = mergeAssistantUniqueValues(leadDoc.sessionIds || [], sessionId);
      leadDoc.conversationHistory = mergeConversationHistory(leadDoc.conversationHistory || [], [
        {
          role: "user",
          content: message,
          createdAt: now,
        },
      ]);
      leadDoc.lastUserMessage = message;
      leadDoc.lastContactAt = now;
      leadDoc.updatedAt = now;

      if (!leadDoc.createdAt) {
        leadDoc.createdAt = now;
      }

      await leadDoc.save();
      setPublicChatThreadCookie(res, req, leadDoc.publicChatThreadKey || "");
      await syncPublicChatPushDevicesToLead(leadDoc);

      if (!leadExistedBeforeMessage && leadDoc?._id) {
        await appendActivity({
          leadId: leadDoc._id,
          activityType: "lead_created",
          title: "Lead creado",
          body: "El chat web creo un lead nuevo desde la pagina de mensajes.",
          meta: {
            sourceType: METALWORKS_WEBSITE_CHAT_SOURCE_TYPE,
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
        leadId: leadDoc._id,
        activityType: "website_live_chat_message",
        title: "Mensaje del chat web",
        body: message,
        meta: {
          visitorId,
          sessionId,
          pageTitle,
          sourceType: METALWORKS_WEBSITE_CHAT_SOURCE_TYPE,
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

      try {
        pushDelivery = await sendMetalworksPushAlert({
          lead: leadDoc.toObject ? leadDoc.toObject() : leadDoc,
          alertType: "website_live_chat",
        });
      } catch (error) {
        console.error("Error sending website chat push:", error.message);
      }

      res.json({
        ok: true,
        duplicate: leadExistedBeforeMessage,
        notified: Boolean(pushDelivery.delivered),
        thread: cleanPublicWebsiteLiveChatThread(leadDoc),
      });
    } catch (error) {
      console.error("Error saving Metal Works website chat message:", error.message);
      respondError(res, 500, "I could not send this message right now.");
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
        applicantCaptured: result.applicantCaptured,
        applicantId: result.applicantId,
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
