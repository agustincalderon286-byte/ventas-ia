import crypto from "node:crypto";
import path from "node:path";

const METALWORKS_CRM_SESSION_COOKIE = "cmwf_crm_session";
const METALWORKS_CRM_SESSION_DAYS = 30;
const METALWORKS_CRM_DEFAULT_EMAIL = "agustincalderon286@gmail.com";
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
]);

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
  };

  return labels[type] || "Actividad";
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
}
