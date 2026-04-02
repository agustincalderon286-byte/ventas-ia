import crypto from "node:crypto";
import path from "node:path";

const METALWORKS_CRM_SESSION_COOKIE = "cmwf_crm_session";
const METALWORKS_CRM_SESSION_DAYS = 30;
const METALWORKS_CRM_DEFAULT_EMAIL = "agustincalderon286@gmail.com";
const METALWORKS_ASSISTANT_MAX_MESSAGES_PER_DAY = Math.max(
  1,
  Number(process.env.METALWORKS_ASSISTANT_MAX_MESSAGES_PER_DAY || 20),
);
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
  return parseEmailList(
    process.env.METALWORKS_CRM_ALLOWED_EMAILS || METALWORKS_CRM_DEFAULT_EMAIL,
  );
}

function getMetalworksPassword() {
  return String(process.env.METALWORKS_CRM_PASSWORD || "").trim();
}

function metalworksCrmConfigured() {
  return Boolean(getAllowedEmails().length && getMetalworksPassword());
}

function normalizeStatus(value = "") {
  const status = cleanText(value).toLowerCase();
  return METALWORKS_CRM_STATUS_OPTIONS.includes(status) ? status : "new";
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
    assistant_open: "Assistant abierto",
    assistant_cta_click: "Assistant CTA",
    assistant_user_message: "Mensaje al assistant",
    assistant_ai_reply: "Respuesta del assistant",
    assistant_fallback: "Fallback del assistant",
  };

  return labels[type] || "Actividad";
}

function detectSpanish(value = "") {
  return /[¿¡]|\b(hola|precio|cotiza|reparacion|reparación|porton|portón|barandal|soldadura|cerca|reja|gracias|necesito|quiero|ayuda)\b/i.test(
    String(value || ""),
  );
}

function buildAssistantFallbackReply(message = "") {
  const text = cleanText(message, 500).toLowerCase();
  const inSpanish = detectSpanish(message);

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

async function generateAssistantReply({
  message = "",
  history = [],
  pagePath = "",
} = {}) {
  const fallbackReply = buildAssistantFallbackReply(message);
  const apiKey = String(process.env.OPENAI_API_KEY || "").trim();

  if (!apiKey) {
    return {
      reply: fallbackReply,
      usedFallback: true,
      reason: "OPENAI_API_KEY missing",
    };
  }

  try {
    const response = await fetch("https://api.openai.com/v1/chat/completions", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Authorization: `Bearer ${apiKey}`,
      },
      body: JSON.stringify({
        model: "gpt-5-mini",
        max_completion_tokens: 220,
        messages: [
          {
            role: "system",
            content: METALWORKS_ASSISTANT_SYSTEM_PROMPT,
          },
          {
            role: "system",
            content: buildAssistantContext(message, pagePath),
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
    const reply = cleanText(data?.choices?.[0]?.message?.content || "", 1500);

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

function cleanLead(doc = null) {
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
    privateNotes: doc.privateNotes || "",
    estimateAmount: Number(doc.estimateAmount || 0) || 0,
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
    MetalworksLeadActivity.find({})
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
    estimateAmount: { type: Number, default: 0 },
    sourceType: { type: String, default: "website_form", index: true },
    pageTitle: String,
    pagePath: String,
    pageUrl: String,
    referrer: String,
    ipAddress: String,
    userAgent: String,
    tracking: trackingSchema,
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
    expiresAt: { type: Date, required: true, index: true },
    lastSeenAt: { type: Date, default: Date.now },
    createdAt: { type: Date, default: Date.now },
  });

  metalworksLeadSchema.index({ createdAt: -1 });
  metalworksLeadSchema.index({ updatedAt: -1 });
  metalworksLeadSchema.index({ status: 1, updatedAt: -1 });
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
      res.json({
        authenticated: Boolean(auth.email),
        configured: metalworksCrmConfigured(),
        email: auth.email || "",
        allowedEmail:
          getAllowedEmails()[0] || METALWORKS_CRM_DEFAULT_EMAIL,
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
      res.json({ ok: true, email });
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
        lead: cleanLead(leadDoc),
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
        ? Number(req.body?.estimateAmount || 0) || 0
        : null;
      const note = cleanText(req.body?.note || "", 600);

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

      if (estimateAmount !== null) {
        leadDoc.estimateAmount = estimateAmount;
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
        lead: cleanLead(updatedLead),
        activity: activityDocs.map(cleanActivity).filter(Boolean),
      });
    } catch (error) {
      console.error("Error updating Metal Works lead:", error.message);
      respondError(res, 500, "No pude guardar ese lead.");
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
      const pagePath = cleanText(req.body?.pagePath || "", 240);
      const pageUrl = cleanText(req.body?.pageUrl || "", 500);
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

      await appendActivity({
        activityType: "assistant_user_message",
        title: "Mensaje al assistant",
        body: message,
        meta: {
          visitorId,
          sessionId,
          pageTitle: cleanText(req.body?.pageTitle || "", 160),
        },
        req,
        pagePath,
        pageUrl,
        tracking,
      });

      const result = await generateAssistantReply({
        message,
        history,
        pagePath,
      });

      await appendActivity({
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
