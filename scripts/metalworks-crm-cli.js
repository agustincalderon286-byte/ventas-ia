import "dotenv/config";
import fs from "node:fs/promises";
import path from "node:path";

const DEFAULT_BASE_URL =
  cleanUrl(process.env.METALWORKS_CRM_API_BASE_URL || "") ||
  "https://cmwf-crm-api.onrender.com";
const DEFAULT_EMAIL =
  cleanText(
    process.env.METALWORKS_CRM_LOGIN_EMAIL ||
      firstEmailFromList(process.env.METALWORKS_CRM_ALLOWED_EMAILS || "") ||
      "agustincalderon286@gmail.com",
    160,
  ) || "agustincalderon286@gmail.com";
const DEFAULT_PASSWORD = String(
  process.env.METALWORKS_CRM_LOGIN_PASSWORD || process.env.METALWORKS_CRM_PASSWORD || "",
).trim();
const CRM_SESSION_COOKIE = "cmwf_crm_session";
const STATUS_OPTIONS = new Set([
  "new",
  "contacted",
  "quoted",
  "booked",
  "won",
  "lost",
  "archived",
]);
const ATLAS_WORKFLOW_ACTIONS = new Set([
  "waiting-customer",
  "waiting-photos",
  "waiting-price",
  "ready-to-schedule",
  "followup",
  "quoted",
  "booked",
  "won",
  "lost",
]);
const SOURCE_GROUP_OPTIONS = new Set([
  "thumbtack",
  "search_kings_google_ads",
  "google_ads",
  "atlas_commercial_outreach",
  "assistant_organic",
  "prospector",
  "website",
  "manual",
  "other",
]);
const CLEAR_SENTINELS = new Set(["clear", "none", "null"]);

const HELP_TEXT = `
Chicago Metal Works CRM CLI

Usage:
  node scripts/metalworks-crm-cli.js <command> [options]

Commands:
  me
      Checks CRM auth and returns the authenticated operator profile.

  search-leads [--query <text>] [--status <status>] [--project-type <text>] [--source-group <group>] [--limit <n>] [--json]
      Searches the CRM lead dashboard and returns matching leads.

  recent-leads [--status <status>] [--limit <n>] [--json]
      Returns the most recently created CRM leads.

  followup-queue [--bucket <overdue|today|upcoming|stale|all>] [--days-without-contact <n>] [--limit <n>] [--json]
      Returns the lead follow-up queue for office and field work.

  get-lead --lead-id <id> [--json]
      Loads one lead with activity and assets.

  create-lead --full-name <name> [fields...] [--json]
      Creates a CRM lead. Use --source-group atlas_commercial_outreach for Atlas email outreach.

  update-lead --lead-id <id> [fields...] [--json]
      Updates one lead using the same safe fields the CRM UI uses.

  atlas-workflow --lead-id <id> --action <action> [--when <ISO>] [--note <text>] [--json]
      Applies a standardized Atlas workflow action without changing the CRM UI.

  create-agenda-event --title <text> --starts-at <ISO> [--event-type <personal|client|interview|internal>] [--owner <text>] [--notes <text>] [--json]
      Creates a non-lead agenda card for personal, internal, interview, or client scheduling.

Common options:
  --base-url <url>       CRM base URL. Default: ${DEFAULT_BASE_URL}
  --email <email>        CRM operator email. Default: ${DEFAULT_EMAIL}
  --password <password>  CRM operator password. Prefer env vars instead.
  --json                 Prints machine-friendly JSON.
  --help                 Shows this help text.

Supported update fields:
  --status <new|contacted|quoted|booked|won|lost|archived>
  --full-name <text>
  --phone-display <text>
  --email-value <text>
  --project-type <text>
  --location <text>
  --source-group <thumbtack|search_kings_google_ads|google_ads|atlas_commercial_outreach|assistant_organic|prospector|website|manual|other>
  --details <text>
  --best-contact-day <text>
  --best-contact-time <text>
  --next-action <text>
  --next-action-at <ISO-or-clear>
  --private-notes <text-or-clear>
  --text-thread-import <text>
  --text-thread-import-file <path>
  --text-thread-import-source <label>
  --note <text>

Atlas workflow actions:
  waiting-customer
  waiting-photos
  waiting-price
  ready-to-schedule
  followup
  quoted
  booked
  won
  lost

Examples:
  node scripts/metalworks-crm-cli.js me
  node scripts/metalworks-crm-cli.js search-leads --query "Rigoberto" --json
  node scripts/metalworks-crm-cli.js recent-leads --limit 5 --json
  node scripts/metalworks-crm-cli.js followup-queue --bucket today --json
  node scripts/metalworks-crm-cli.js create-lead --full-name "ABC Property Management" --email-value vendor@example.com --project-type "Property management outreach" --source-group atlas_commercial_outreach --status contacted --json
  node scripts/metalworks-crm-cli.js get-lead --lead-id 6848beef6848beef6848beef --json
  node scripts/metalworks-crm-cli.js update-lead --lead-id 6848beef6848beef6848beef --status contacted --next-action "Call tomorrow at 9am" --json
  node scripts/metalworks-crm-cli.js atlas-workflow --lead-id 6848beef6848beef6848beef --action waiting-photos --json
  node scripts/metalworks-crm-cli.js create-agenda-event --title "Daughter birthday" --starts-at "2026-06-22T09:00" --event-type personal --json

Recommended env vars:
  METALWORKS_CRM_API_BASE_URL
  METALWORKS_CRM_LOGIN_EMAIL
  METALWORKS_CRM_LOGIN_PASSWORD
`.trim();

async function main() {
  try {
    const parsed = parseArgs(process.argv.slice(2));

    if (parsed.help || parsed.command === "help") {
      console.log(HELP_TEXT);
      return;
    }

    const session = await createSession(parsed.options);

    switch (parsed.command) {
      case "me":
        await runMe(session, parsed.options);
        return;
      case "search-leads":
        await runSearchLeads(session, parsed.options);
        return;
      case "recent-leads":
        await runRecentLeads(session, parsed.options);
        return;
      case "followup-queue":
        await runFollowupQueue(session, parsed.options);
        return;
      case "get-lead":
        await runGetLead(session, parsed.options);
        return;
      case "create-lead":
        await runCreateLead(session, parsed.options);
        return;
      case "update-lead":
        await runUpdateLead(session, parsed.options);
        return;
      case "atlas-workflow":
        await runAtlasWorkflow(session, parsed.options);
        return;
      case "create-agenda-event":
        await runCreateAgendaEvent(session, parsed.options);
        return;
      default:
        throw createCliError(
          `Unknown command "${parsed.command}". Use --help to see available commands.`,
        );
    }
  } catch (error) {
    const message = error instanceof Error ? error.message : "Unknown CLI error.";
    console.error(message);
    process.exitCode = Number(error?.exitCode || 1) || 1;
  }
}

function parseArgs(argv = []) {
  const tokens = [...argv];
  let command = "help";
  const options = {};
  let help = false;

  if (tokens[0] && !String(tokens[0]).startsWith("-")) {
    command = String(tokens.shift() || "").trim();
  }

  for (let index = 0; index < tokens.length; index += 1) {
    const token = String(tokens[index] || "").trim();

    if (!token) {
      continue;
    }

    if (token === "--help" || token === "-h") {
      help = true;
      continue;
    }

    if (token === "--json") {
      options.json = true;
      continue;
    }

    if (!token.startsWith("--")) {
      throw createCliError(`Unexpected argument "${token}". Use --help to see usage.`);
    }

    const eqIndex = token.indexOf("=");
    const rawKey = eqIndex >= 0 ? token.slice(2, eqIndex) : token.slice(2);
    const key = toCamelCase(rawKey);

    if (!key) {
      continue;
    }

    if (eqIndex >= 0) {
      options[key] = token.slice(eqIndex + 1);
      continue;
    }

    const next = tokens[index + 1];

    if (!next || String(next).startsWith("--")) {
      options[key] = true;
      continue;
    }

    options[key] = next;
    index += 1;
  }

  return {
    command,
    help,
    options,
  };
}

function toCamelCase(value = "") {
  return String(value || "")
    .trim()
    .replace(/-([a-z])/g, (_, letter) => letter.toUpperCase())
    .replace(/[^a-zA-Z0-9]/g, "");
}

function cleanText(value = "", maxLength = 1000) {
  return String(value || "").replace(/\s+/g, " ").trim().slice(0, maxLength);
}

function cleanMultilineText(value = "", maxLength = 12000) {
  return String(value || "")
    .replace(/\r\n/g, "\n")
    .replace(/\u0000/g, "")
    .trim()
    .slice(0, maxLength);
}

function cleanUrl(value = "") {
  const safe = String(value || "").trim();
  return safe ? safe.replace(/\/+$/, "") : "";
}

function firstEmailFromList(value = "") {
  return String(value || "")
    .split(/[,\n]/)
    .map((item) => cleanText(item, 160).toLowerCase())
    .find(Boolean);
}

function normalizeStatus(value = "") {
  const safe = cleanText(value, 24).toLowerCase();
  return STATUS_OPTIONS.has(safe) ? safe : "";
}

function normalizeSourceGroup(value = "") {
  const safe = cleanText(value, 40).toLowerCase();
  return SOURCE_GROUP_OPTIONS.has(safe) ? safe : "";
}

function normalizeQueueBucket(value = "") {
  const safe = cleanText(value, 24).toLowerCase();
  return ["overdue", "today", "upcoming", "stale", "all"].includes(safe)
    ? safe
    : "overdue";
}

function normalizePhone(value = "") {
  const digits = String(value || "").replace(/\D+/g, "");

  if (!digits) {
    return "";
  }

  if (digits.length === 10) {
    return `+1${digits}`;
  }

  if (digits.length === 11 && digits.startsWith("1")) {
    return `+${digits}`;
  }

  return digits.startsWith("+") ? digits : `+${digits}`;
}

function parsePositiveInt(value, fallback = 25) {
  const parsed = Number.parseInt(String(value || ""), 10);

  if (!Number.isFinite(parsed) || parsed <= 0) {
    return fallback;
  }

  return Math.min(parsed, 250);
}

function createCliError(message, exitCode = 1) {
  const error = new Error(message);
  error.exitCode = exitCode;
  return error;
}

function resolveCredentialOptions(options = {}) {
  const baseUrl = cleanUrl(options.baseUrl || DEFAULT_BASE_URL) || DEFAULT_BASE_URL;
  const email =
    cleanText(options.email || DEFAULT_EMAIL, 160).toLowerCase() || DEFAULT_EMAIL;
  const password = String(options.password || DEFAULT_PASSWORD || "").trim();

  if (!baseUrl) {
    throw createCliError(
      "Missing CRM base URL. Set METALWORKS_CRM_API_BASE_URL or pass --base-url.",
    );
  }

  if (!email) {
    throw createCliError(
      "Missing CRM email. Set METALWORKS_CRM_LOGIN_EMAIL or pass --email.",
    );
  }

  if (!password) {
    throw createCliError(
      "Missing CRM password. Set METALWORKS_CRM_LOGIN_PASSWORD or pass --password.",
    );
  }

  return {
    baseUrl,
    email,
    password,
  };
}

async function createSession(options = {}) {
  const credentials = resolveCredentialOptions(options);
  const cookie = await loginToCrm(credentials);
  return {
    ...credentials,
    cookie,
  };
}

async function loginToCrm({ baseUrl, email, password }) {
  const response = await fetch(`${baseUrl}/api/metalworks-crm/login`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      email,
      password,
    }),
  });
  const data = await readJson(response);

  if (!response.ok) {
    throw createCliError(
      data.error || `CRM login failed with status ${response.status}.`,
      1,
    );
  }

  const cookie = extractSessionCookie(response.headers);

  if (!cookie) {
    throw createCliError("CRM login succeeded but no session cookie was returned.");
  }

  return cookie;
}

function extractSessionCookie(headers) {
  const setCookieValues =
    typeof headers.getSetCookie === "function"
      ? headers.getSetCookie()
      : [headers.get("set-cookie")].filter(Boolean);

  for (const entry of setCookieValues) {
    const safeEntry = String(entry || "").trim();
    const match = safeEntry.match(
      new RegExp(`(?:^|,\\s*)(${CRM_SESSION_COOKIE}=[^;]+)`),
    );

    if (match?.[1]) {
      return match[1];
    }
  }

  return "";
}

async function readJson(response) {
  const contentType = String(response.headers.get("content-type") || "").toLowerCase();

  if (!contentType.includes("application/json")) {
    return {};
  }

  try {
    return await response.json();
  } catch {
    return {};
  }
}

async function crmRequest(session, requestPath, { method = "GET", body = null } = {}) {
  const response = await fetch(`${session.baseUrl}${requestPath}`, {
    method,
    headers: {
      Cookie: session.cookie,
      ...(body ? { "Content-Type": "application/json" } : {}),
    },
    body: body ? JSON.stringify(body) : undefined,
  });
  const data = await readJson(response);

  if (!response.ok) {
    throw createCliError(
      data.error || `CRM request failed with status ${response.status}.`,
      1,
    );
  }

  return data;
}

async function runMe(session, options = {}) {
  const me = await crmRequest(session, "/api/metalworks-crm/me");
  printOutput(
    {
      ok: true,
      authenticated: Boolean(me.authenticated),
      configured: Boolean(me.configured),
      email: me.email || session.email,
      profile: me.profile || null,
    },
    options,
    formatMeOutput(me, session),
  );
}

function formatMeOutput(me = {}, session = {}) {
  const email = cleanText(me.email || session.email, 160);
  const name = cleanText(me.profile?.displayName || "", 80);
  const theme = cleanText(me.profile?.themeLabel || "", 120);
  return [
    `Authenticated: ${me.authenticated ? "yes" : "no"}`,
    `Email: ${email || "unknown"}`,
    name ? `Display name: ${name}` : "",
    theme ? `Theme: ${theme}` : "",
  ]
    .filter(Boolean)
    .join("\n");
}

async function runSearchLeads(session, options = {}) {
  const params = new URLSearchParams();
  const query = cleanText(options.query || "", 120);
  const status = normalizeStatus(options.status || "");
  const projectType = cleanText(options.projectType || "", 80);
  const sourceGroup = normalizeSourceGroup(options.sourceGroup || "");
  const limit = parsePositiveInt(options.limit, 25);

  if (query) {
    params.set("search", query);
  }

  if (status) {
    params.set("status", status);
  }

  if (projectType) {
    params.set("projectType", projectType);
  }

  if (sourceGroup) {
    params.set("sourceGroup", sourceGroup);
  }

  const requestPath = params.size
    ? `/api/metalworks-crm/dashboard?${params.toString()}`
    : "/api/metalworks-crm/dashboard";
  const dashboard = await crmRequest(session, requestPath);
  const combinedLeads = dedupeLeads([
    ...(Array.isArray(dashboard.leads) ? dashboard.leads : []),
    ...(Array.isArray(dashboard.completedLeads) ? dashboard.completedLeads : []),
  ]).slice(0, limit);

  printOutput(
    {
      ok: true,
      filters: dashboard.filters || {},
      summary: dashboard.summary || {},
      count: combinedLeads.length,
      leads: combinedLeads,
    },
    options,
    formatSearchOutput(combinedLeads, dashboard.summary || {}, dashboard.filters || {}),
  );
}

function dedupeLeads(leads = []) {
  const seen = new Set();
  const items = [];

  for (const lead of Array.isArray(leads) ? leads : []) {
    const id = cleanText(lead?.id || "", 64);

    if (!id || seen.has(id)) {
      continue;
    }

    seen.add(id);
    items.push(lead);
  }

  return items;
}

function formatSearchOutput(leads = [], summary = {}, filters = {}) {
  const lines = [
    `Matches: ${Number(leads.length || 0)}`,
    `Active leads in CRM: ${Number(summary.totalLeads || 0)}`,
  ];

  if (filters.search) {
    lines.push(`Search: ${filters.search}`);
  }

  for (const lead of leads) {
    lines.push("");
    lines.push(formatLeadSummary(lead));
  }

  return lines.join("\n");
}

function formatLeadSummary(lead = {}) {
  const when = cleanText(lead.nextActionAt || "", 80);
  const atlasStage = cleanText(lead?.atlas?.stageLabel || "", 80);
  const missing = Array.isArray(lead?.atlas?.missingFieldsLabel)
    ? lead.atlas.missingFieldsLabel.slice(0, 4).join(", ")
    : "";
  const parts = [
    `${lead.fullName || "Unnamed lead"} [${lead.id || "no-id"}]`,
    `Status: ${lead.status || "new"}`,
    atlasStage ? `Atlas: ${atlasStage}` : "",
    lead.phoneDisplay ? `Phone: ${lead.phoneDisplay}` : "",
    lead.projectType ? `Project: ${lead.projectType}` : "",
    lead.location ? `Location: ${lead.location}` : "",
    lead.nextAction ? `Next: ${lead.nextAction}` : "",
    when ? `When: ${when}` : "",
    missing ? `Missing: ${missing}` : "",
  ];

  return parts.filter(Boolean).join("\n");
}

async function runRecentLeads(session, options = {}) {
  const dashboard = await crmRequest(session, "/api/metalworks-crm/dashboard");
  const limit = parsePositiveInt(options.limit, 10);
  const status = normalizeStatus(options.status || "");
  const leads = dedupeLeads([
    ...(Array.isArray(dashboard.leads) ? dashboard.leads : []),
    ...(Array.isArray(dashboard.completedLeads) ? dashboard.completedLeads : []),
  ])
    .filter((lead) => (status ? normalizeStatus(lead?.status || "") === status : true))
    .sort(compareByCreatedAtDesc)
    .slice(0, limit);

  printOutput(
    {
      ok: true,
      count: leads.length,
      status: status || "",
      leads,
    },
    options,
    formatRecentLeadsOutput(leads, status),
  );
}

function compareByCreatedAtDesc(left = {}, right = {}) {
  const leftTime = parseIsoToTime(left.createdAt || left.updatedAt || "");
  const rightTime = parseIsoToTime(right.createdAt || right.updatedAt || "");

  if (rightTime !== leftTime) {
    return rightTime - leftTime;
  }

  return String(right.updatedAt || "").localeCompare(String(left.updatedAt || ""));
}

function formatRecentLeadsOutput(leads = [], status = "") {
  const lines = [
    `Recent leads: ${Number(leads.length || 0)}`,
    status ? `Status filter: ${status}` : "",
  ].filter(Boolean);

  for (const lead of leads) {
    const createdAt = cleanText(lead.createdAt || "", 80);
    lines.push("");
    lines.push(formatLeadSummary(lead));
    if (createdAt) {
      lines.push(`Created: ${createdAt}`);
    }
  }

  return lines.join("\n");
}

async function runFollowupQueue(session, options = {}) {
  const dashboard = await crmRequest(session, "/api/metalworks-crm/dashboard");
  const limit = parsePositiveInt(options.limit, 10);
  const bucket = normalizeQueueBucket(options.bucket || "overdue");
  const daysWithoutContact = parsePositiveInt(options.daysWithoutContact, 3);
  const now = new Date();
  const openLeads = dedupeLeads(Array.isArray(dashboard.leads) ? dashboard.leads : []);
  const queue = buildFollowupQueue(openLeads, {
    bucket,
    limit,
    now,
    daysWithoutContact,
  });

  printOutput(
    {
      ok: true,
      bucket,
      count: queue.length,
      daysWithoutContact,
      leads: queue,
    },
    options,
    formatFollowupQueueOutput(queue, {
      bucket,
      daysWithoutContact,
    }),
  );
}

function buildFollowupQueue(leads = [], { bucket = "overdue", limit = 10, now = new Date(), daysWithoutContact = 3 } = {}) {
  const nowTime = now.getTime();
  const staleThresholdMs = Math.max(1, daysWithoutContact) * 24 * 60 * 60 * 1000;
  const enriched = leads
    .map((lead) => enrichQueueLead(lead, now))
    .filter((lead) => lead.id && isOpenLeadStatus(lead.status));

  let queue = [];

  if (bucket === "overdue") {
    queue = enriched
      .filter((lead) => lead.nextActionTime && lead.nextActionTime < nowTime)
      .sort((left, right) => left.nextActionTime - right.nextActionTime);
  } else if (bucket === "today") {
    queue = enriched
      .filter((lead) => lead.nextActionTime && isSameLocalDay(lead.nextActionTime, now))
      .sort((left, right) => left.nextActionTime - right.nextActionTime);
  } else if (bucket === "upcoming") {
    queue = enriched
      .filter((lead) => lead.nextActionTime && lead.nextActionTime > nowTime)
      .sort((left, right) => left.nextActionTime - right.nextActionTime);
  } else if (bucket === "stale") {
    queue = enriched
      .filter((lead) => lead.lastTouchTime && nowTime - lead.lastTouchTime >= staleThresholdMs)
      .sort((left, right) => left.lastTouchTime - right.lastTouchTime);
  } else {
    queue = enriched
      .filter(
        (lead) =>
          (lead.nextActionTime && lead.nextActionTime < nowTime) ||
          (lead.lastTouchTime && nowTime - lead.lastTouchTime >= staleThresholdMs),
      )
      .sort((left, right) => {
        const leftPriority = leadQueuePriority(left, nowTime, staleThresholdMs);
        const rightPriority = leadQueuePriority(right, nowTime, staleThresholdMs);

        if (rightPriority !== leftPriority) {
          return rightPriority - leftPriority;
        }

        if (left.nextActionTime && right.nextActionTime) {
          return left.nextActionTime - right.nextActionTime;
        }

        if (left.lastTouchTime && right.lastTouchTime) {
          return left.lastTouchTime - right.lastTouchTime;
        }

        return 0;
      });
  }

  return queue.slice(0, limit).map(stripQueueLeadMeta);
}

function enrichQueueLead(lead = {}, now = new Date()) {
  const nextActionTime = parseIsoToTime(lead.nextActionAt || "");
  const createdAtTime = parseIsoToTime(lead.createdAt || "");
  const updatedAtTime = parseIsoToTime(lead.updatedAt || "");
  const lastContactTime = parseIsoToTime(lead.lastContactAt || "");
  const lastTouchTime = lastContactTime || updatedAtTime || createdAtTime || 0;
  const nowTime = now.getTime();
  const hoursSinceLastTouch =
    lastTouchTime && lastTouchTime <= nowTime
      ? Math.round(((nowTime - lastTouchTime) / (60 * 60 * 1000)) * 10) / 10
      : 0;

  return {
    ...lead,
    nextActionTime,
    createdAtTime,
    updatedAtTime,
    lastContactTime,
    lastTouchTime,
    hoursSinceLastTouch,
  };
}

function stripQueueLeadMeta(lead = {}) {
  const {
    nextActionTime,
    createdAtTime,
    updatedAtTime,
    lastContactTime,
    lastTouchTime,
    ...rest
  } = lead;

  return rest;
}

function isOpenLeadStatus(status = "") {
  return !["won", "lost", "archived"].includes(normalizeStatus(status || "new"));
}

function parseIsoToTime(value = "") {
  const safe = String(value || "").trim();

  if (!safe) {
    return 0;
  }

  const parsed = new Date(safe).getTime();
  return Number.isFinite(parsed) ? parsed : 0;
}

function isSameLocalDay(leftTime, rightDate = new Date()) {
  const left = new Date(leftTime);
  const right = rightDate instanceof Date ? rightDate : new Date(rightDate);

  return (
    left.getFullYear() === right.getFullYear() &&
    left.getMonth() === right.getMonth() &&
    left.getDate() === right.getDate()
  );
}

function leadQueuePriority(lead = {}, nowTime = Date.now(), staleThresholdMs = 3 * 24 * 60 * 60 * 1000) {
  if (lead.nextActionTime && lead.nextActionTime < nowTime) {
    return 3;
  }

  if (lead.lastTouchTime && nowTime - lead.lastTouchTime >= staleThresholdMs) {
    return 2;
  }

  return 1;
}

function formatFollowupQueueOutput(leads = [], { bucket = "overdue", daysWithoutContact = 3 } = {}) {
  const bucketLabels = {
    overdue: "Overdue follow-ups",
    today: "Today's follow-ups",
    upcoming: "Upcoming follow-ups",
    stale: `Leads without response for ${daysWithoutContact}+ day(s)`,
    all: "Priority follow-up queue",
  };
  const lines = [
    `${bucketLabels[bucket] || "Follow-up queue"}: ${Number(leads.length || 0)}`,
  ];

  for (const lead of leads) {
    lines.push("");
    lines.push(formatLeadSummary(lead));
    if (lead.lastContactAt) {
      lines.push(`Last contact: ${lead.lastContactAt}`);
    } else if (lead.updatedAt) {
      lines.push(`Last update: ${lead.updatedAt}`);
    }
  }

  return lines.join("\n");
}

async function runGetLead(session, options = {}) {
  const leadId = cleanText(options.leadId || "", 64);

  if (!leadId) {
    throw createCliError("Missing --lead-id for get-lead.");
  }

  const detail = await crmRequest(
    session,
    `/api/metalworks-crm/leads/${encodeURIComponent(leadId)}`,
  );

  printOutput(
    {
      ok: true,
      lead: detail.lead || null,
      assets: Array.isArray(detail.assets) ? detail.assets : [],
      activity: Array.isArray(detail.activity) ? detail.activity : [],
    },
    options,
    formatLeadDetailOutput(detail),
  );
}

async function runCreateLead(session, options = {}) {
  const fullName = cleanText(options.fullName || "", 120);
  const sourceGroup = normalizeSourceGroup(options.sourceGroup || "manual") || "manual";
  const status = normalizeStatus(options.status || "new") || "new";

  if (!fullName) {
    throw createCliError("Missing --full-name for create-lead.");
  }

  if (options.sourceGroup && !normalizeSourceGroup(options.sourceGroup || "")) {
    throw createCliError(
      `Invalid source group "${options.sourceGroup}". Use one of: ${Array.from(SOURCE_GROUP_OPTIONS).join(", ")}.`,
    );
  }

  if (options.status && !normalizeStatus(options.status || "")) {
    throw createCliError(
      `Invalid status "${options.status}". Use one of: ${Array.from(STATUS_OPTIONS).join(", ")}.`,
    );
  }

  let detail = await crmRequest(session, "/api/metalworks-crm/leads", {
    method: "POST",
    body: {
      fullName,
      phoneDisplay: cleanText(options.phoneDisplay || "", 40),
      email: cleanText(options.emailValue || options.email || "", 160).toLowerCase(),
      projectType: cleanText(options.projectType || "", 120),
      location: cleanText(options.location || "", 160),
      status,
      sourceGroup,
      details: cleanMultilineText(options.details || "", 3000),
    },
  });

  const leadId = cleanText(detail?.lead?.id || "", 64);
  const followupOptions = {};
  const followupKeys = [
    "bestContactDay",
    "bestContactTime",
    "nextAction",
    "nextActionAt",
    "privateNotes",
    "textThreadImport",
    "textThreadImportFile",
    "textThreadImportSource",
    "note",
  ];

  for (const key of followupKeys) {
    if (Object.prototype.hasOwnProperty.call(options, key)) {
      followupOptions[key] = options[key];
    }
  }

  if (leadId && Object.keys(followupOptions).length) {
    const followupBody = await buildUpdateBody(followupOptions);

    if (Object.keys(followupBody).length) {
      detail = await crmRequest(
        session,
        `/api/metalworks-crm/leads/${encodeURIComponent(leadId)}`,
        {
          method: "PATCH",
          body: followupBody,
        },
      );
    }
  }

  printOutput(
    {
      ok: true,
      created: true,
      lead: detail.lead || null,
      assets: Array.isArray(detail.assets) ? detail.assets : [],
      activity: Array.isArray(detail.activity) ? detail.activity : [],
    },
    options,
    [`Created lead ${fullName}.`, "", formatLeadDetailOutput(detail)].join("\n"),
  );
}

function formatLeadDetailOutput(detail = {}) {
  const lead = detail.lead || {};
  const lines = [
    formatLeadSummary(lead),
    lead.details ? `Details: ${lead.details}` : "",
    lead.privateNotes ? `Private notes: ${lead.privateNotes}` : "",
    lead.conversationSummary ? `Conversation summary: ${lead.conversationSummary}` : "",
    lead?.atlas?.brief ? `Atlas brief: ${lead.atlas.brief}` : "",
    Array.isArray(lead?.atlas?.suggestedActions) && lead.atlas.suggestedActions.length
      ? `Atlas actions: ${lead.atlas.suggestedActions.join(", ")}`
      : "",
    `Assets: ${Array.isArray(detail.assets) ? detail.assets.length : 0}`,
    `Activity items: ${Array.isArray(detail.activity) ? detail.activity.length : 0}`,
  ];

  return lines.filter(Boolean).join("\n");
}

async function runUpdateLead(session, options = {}) {
  const leadId = cleanText(options.leadId || "", 64);

  if (!leadId) {
    throw createCliError("Missing --lead-id for update-lead.");
  }

  const body = await buildUpdateBody(options);

  if (!Object.keys(body).length) {
    throw createCliError(
      "No update fields were provided. Pass at least one editable field for update-lead.",
    );
  }

  const detail = await crmRequest(
    session,
    `/api/metalworks-crm/leads/${encodeURIComponent(leadId)}`,
    {
      method: "PATCH",
      body,
    },
  );

  printOutput(
    {
      ok: true,
      updatedFields: Object.keys(body),
      lead: detail.lead || null,
      assets: Array.isArray(detail.assets) ? detail.assets : [],
      activity: Array.isArray(detail.activity) ? detail.activity : [],
    },
    options,
    [
      `Updated lead ${leadId}.`,
      `Fields: ${Object.keys(body).join(", ")}`,
      "",
      formatLeadDetailOutput(detail),
    ].join("\n"),
  );
}

function normalizeAtlasAction(value = "") {
  const safe = cleanText(value, 40).toLowerCase();
  return ATLAS_WORKFLOW_ACTIONS.has(safe) ? safe : "";
}

function buildAtlasWorkflowUpdate(action = "", options = {}) {
  const when = Object.prototype.hasOwnProperty.call(options, "when")
    ? String(options.when || "").trim()
    : "";
  const note = cleanText(options.note || "", 600);

  switch (action) {
    case "waiting-customer":
      return {
        status: "contacted",
        nextAction: "Waiting for customer reply",
        nextActionAt: when,
        note,
      };
    case "waiting-photos":
      return {
        status: "contacted",
        nextAction: "Waiting for customer photos",
        nextActionAt: when,
        note,
      };
    case "waiting-price":
      return {
        status: "contacted",
        nextAction: "Need price from Agustin",
        nextActionAt: when,
        note,
      };
    case "ready-to-schedule":
      return {
        status: "contacted",
        nextAction: "Ready to schedule visit",
        nextActionAt: when,
        note,
      };
    case "followup":
      return {
        status: "contacted",
        nextAction: "Follow up with customer",
        nextActionAt: when,
        note,
      };
    case "quoted":
      return {
        status: "quoted",
        nextAction: "Quote sent - waiting on customer",
        nextActionAt: when,
        note,
      };
    case "booked":
      return {
        status: "booked",
        nextAction: "Booked job",
        nextActionAt: when,
        note,
      };
    case "won":
      return {
        status: "won",
        nextAction: "",
        nextActionAt: "",
        note,
      };
    case "lost":
      return {
        status: "lost",
        nextAction: "",
        nextActionAt: "",
        note,
      };
    default:
      throw createCliError(
        `Invalid --action "${action}". Use one of: ${Array.from(ATLAS_WORKFLOW_ACTIONS).join(", ")}.`,
      );
  }
}

async function runAtlasWorkflow(session, options = {}) {
  const leadId = cleanText(options.leadId || "", 64);
  const action = normalizeAtlasAction(options.action || "");

  if (!leadId) {
    throw createCliError("Missing --lead-id for atlas-workflow.");
  }

  if (!action) {
    throw createCliError(
      `Missing or invalid --action. Use one of: ${Array.from(ATLAS_WORKFLOW_ACTIONS).join(", ")}.`,
    );
  }

  const body = buildAtlasWorkflowUpdate(action, options);
  const detail = await crmRequest(
    session,
    `/api/metalworks-crm/leads/${encodeURIComponent(leadId)}`,
    {
      method: "PATCH",
      body,
    },
  );

  printOutput(
    {
      ok: true,
      action,
      lead: detail.lead || null,
      assets: Array.isArray(detail.assets) ? detail.assets : [],
      activity: Array.isArray(detail.activity) ? detail.activity : [],
    },
    options,
    [
      `Applied Atlas workflow action "${action}" to lead ${leadId}.`,
      "",
      formatLeadDetailOutput(detail),
    ].join("\n"),
  );
}

async function runCreateAgendaEvent(session, options = {}) {
  const title = cleanText(options.title || "", 120);
  const startsAt = cleanText(options.startsAt || options.when || "", 80);
  const eventType = normalizeAgendaEventType(options.eventType || "internal");
  const owner = cleanText(options.owner || "", 80);
  const notes = cleanText(options.notes || options.note || "", 1000);

  if (!title) {
    throw createCliError("Missing --title for create-agenda-event.");
  }

  if (!startsAt) {
    throw createCliError("Missing --starts-at for create-agenda-event.");
  }

  const result = await crmRequest(session, "/api/metalworks-crm/agenda-events", {
    method: "POST",
    body: {
      title,
      eventType,
      owner,
      notes,
      startsAt,
      source: "atlas_cli",
    },
  });

  printOutput(
    {
      ok: true,
      agendaEvent: result.agendaEvent || null,
    },
    options,
    formatAgendaEventOutput(result.agendaEvent || {}),
  );
}

function normalizeAgendaEventType(value = "") {
  const safe = cleanText(value, 40).toLowerCase().replace(/[^a-z0-9_]+/g, "_");
  return ["personal", "client", "interview", "internal"].includes(safe) ? safe : "internal";
}

function formatAgendaEventOutput(event = {}) {
  return [
    `Created agenda event: ${event.title || "Agenda event"}`,
    event.eventTypeLabel || event.eventType ? `Type: ${event.eventTypeLabel || event.eventType}` : "",
    event.startsAt ? `When: ${event.startsAt}` : "",
    event.owner ? `Owner: ${event.owner}` : "",
    event.id ? `ID: ${event.id}` : "",
  ]
    .filter(Boolean)
    .join("\n");
}

async function buildUpdateBody(options = {}) {
  const body = {};
  const mappings = [
    ["fullName", "fullName"],
    ["phoneDisplay", "phoneDisplay"],
    ["emailValue", "email"],
    ["projectType", "projectType"],
    ["location", "location"],
    ["details", "details"],
    ["bestContactDay", "bestContactDay"],
    ["bestContactTime", "bestContactTime"],
    ["nextAction", "nextAction"],
    ["privateNotes", "privateNotes"],
    ["textThreadImport", "textThreadImport"],
    ["textThreadImportSource", "textThreadImportSource"],
    ["note", "note"],
  ];

  for (const [optionKey, bodyKey] of mappings) {
    if (!Object.prototype.hasOwnProperty.call(options, optionKey)) {
      continue;
    }

    const value = normalizeClearableText(options[optionKey]);
    body[bodyKey] =
      bodyKey === "details" || bodyKey === "privateNotes" || bodyKey === "textThreadImport"
        ? cleanMultilineText(value, bodyKey === "details" ? 3000 : 12000)
        : cleanText(value, bodyKey === "note" ? 600 : 3000);
  }

  if (Object.prototype.hasOwnProperty.call(options, "status")) {
    const status = normalizeStatus(options.status || "");

    if (!status) {
      throw createCliError(
        `Invalid status "${options.status}". Use one of: ${Array.from(STATUS_OPTIONS).join(", ")}.`,
      );
    }

    body.status = status;
  }

  if (Object.prototype.hasOwnProperty.call(options, "sourceGroup")) {
    const sourceGroup = normalizeSourceGroup(options.sourceGroup || "");

    if (!sourceGroup) {
      throw createCliError(
        `Invalid source group "${options.sourceGroup}". Use one of: ${Array.from(SOURCE_GROUP_OPTIONS).join(", ")}.`,
      );
    }

    body.sourceGroup = sourceGroup;
  }

  if (Object.prototype.hasOwnProperty.call(options, "nextActionAt")) {
    const rawValue = String(options.nextActionAt || "").trim();
    body.nextActionAt = CLEAR_SENTINELS.has(rawValue.toLowerCase()) ? "" : rawValue;
  }

  if (Object.prototype.hasOwnProperty.call(options, "textThreadImportFile")) {
    const filePath = path.resolve(String(options.textThreadImportFile || ""));
    const fileContent = await fs.readFile(filePath, "utf8");
    body.textThreadImport = cleanMultilineText(fileContent, 8000);
  }

  if (body.phoneDisplay) {
    body.phoneDisplay = cleanText(body.phoneDisplay, 40);
  }

  if (body.email !== undefined) {
    body.email = cleanText(body.email, 160).toLowerCase();
  }

  if (body.textThreadImport && !body.textThreadImportSource) {
    body.textThreadImportSource = "Atlas CRM import";
  }

  return body;
}

function normalizeClearableText(value) {
  const raw = String(value ?? "").trim();
  return CLEAR_SENTINELS.has(raw.toLowerCase()) ? "" : raw;
}

function printOutput(payload, options = {}, fallbackText = "") {
  if (options.json) {
    console.log(JSON.stringify(payload, null, 2));
    return;
  }

  console.log(fallbackText || JSON.stringify(payload, null, 2));
}

await main();
