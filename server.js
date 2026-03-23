import "dotenv/config";
import express from "express";
import cors from "cors";
import crypto from "crypto";
import fs from "fs";
import mongoose from "mongoose";
import path from "path";
import Stripe from "stripe";
import {
  buscarKnowledgeVectorial,
  construirContextoVectorial,
  inferirTiposFuentePorPregunta
} from "./src/knowledge/vector-store.js";

const app = express();
app.use(cors());
const PUBLIC_DIR = path.join(process.cwd(), "public");
const PRIVATE_DIR = path.join(process.cwd(), "private");

const conversaciones = {};
const estadosConversacion = {};
const ENABLE_VECTOR_SEARCH = String(process.env.ENABLE_VECTOR_SEARCH || "").toLowerCase() === "true";
const stripe = process.env.STRIPE_SECRET_KEY ? new Stripe(process.env.STRIPE_SECRET_KEY) : null;
const STRIPE_PRICE_ID_MONTHLY = process.env.STRIPE_PRICE_ID || "";
const STRIPE_PRICE_ID_ANNUAL = process.env.STRIPE_PRICE_ID_ANNUAL || "";
const COACH_SESSION_COOKIE = "agustin_coach_session";
const COACH_SESSION_DAYS = 30;
const COACH_PASSWORD_MIN = 8;
const COACH_TRIAL_DAYS = Math.max(1, Number(process.env.COACH_TRIAL_DAYS || 7));
const COACH_TEST_ACCESS_EMAILS = String(process.env.COACH_TEST_ACCESS_EMAILS || "")
  .split(",")
  .map(email => normalizarEmail(email))
  .filter(Boolean);

const conversationEntrySchema = new mongoose.Schema(
  {
    role: String,
    content: String,
    createdAt: { type: Date, default: Date.now }
  },
  { _id: false }
);

const leadSchema = new mongoose.Schema({
  visitorIds: [String],
  sessionIds: [String],
  name: String,
  email: String,
  phone: String,
  message: String,
  ocupacion: String,
  tieneProductos: String,
  necesitaGarantia: String,
  quiereLlamada: String,
  notes: [
    {
      text: String,
      createdAt: { type: Date, default: Date.now }
    }
  ],
  productos: [String],
  temasInteres: [String],
  conversationHistory: [conversationEntrySchema],
  cocinaPara: String,
  esCliente: String,
  direccion: String,
  leadStatus: String,
  lastInteractionAt: Date,
  lastAssistantMessage: String,
  updatedAt: Date,
  createdAt: { type: Date, default: Date.now }
});

const profileSchema = new mongoose.Schema({
  visitorId: { type: String, required: true, unique: true },
  visitorIds: [String],
  sessionIds: [String],
  leadId: mongoose.Schema.Types.ObjectId,
  name: String,
  email: String,
  phone: String,
  direccion: String,
  ocupacion: String,
  esCliente: String,
  tieneProductos: String,
  necesitaGarantia: String,
  quiereLlamada: String,
  productos: [String],
  productosInteres: [String],
  temasInteres: [String],
  cocinaPara: String,
  leadStatus: String,
  profileSummary: String,
  recentHistory: [conversationEntrySchema],
  conversationCount: { type: Number, default: 0 },
  lastUserMessage: String,
  lastAssistantMessage: String,
  lastInteractionAt: Date,
  updatedAt: Date,
  createdAt: { type: Date, default: Date.now }
});

const messageSchema = new mongoose.Schema({
  visitorId: { type: String, required: true, index: true },
  sessionId: { type: String, index: true },
  profileId: mongoose.Schema.Types.ObjectId,
  leadId: mongoose.Schema.Types.ObjectId,
  role: String,
  content: String,
  intent: String,
  detectedTopics: [String],
  createdAt: { type: Date, default: Date.now }
});

const coachUserSchema = new mongoose.Schema({
  name: { type: String, required: true, trim: true },
  email: { type: String, required: true, unique: true, trim: true, lowercase: true },
  passwordHash: { type: String, required: true },
  passwordSalt: { type: String, required: true },
  stripeCustomerId: String,
  stripeSubscriptionId: String,
  stripePriceId: String,
  subscriptionStatus: { type: String, default: "inactive" },
  subscriptionActive: { type: Boolean, default: false },
  subscriptionCurrentPeriodEnd: Date,
  subscriptionCancelAtPeriodEnd: { type: Boolean, default: false },
  lastCheckoutSessionId: String,
  lastLoginAt: Date,
  updatedAt: Date,
  createdAt: { type: Date, default: Date.now }
});

const coachSessionSchema = new mongoose.Schema({
  userId: {
    type: mongoose.Schema.Types.ObjectId,
    ref: "CoachUser",
    required: true,
    index: true
  },
  tokenHash: { type: String, required: true, unique: true },
  expiresAt: { type: Date, required: true },
  lastSeenAt: { type: Date, default: Date.now },
  createdAt: { type: Date, default: Date.now }
});

leadSchema.index({ email: 1 });
leadSchema.index({ phone: 1 });
leadSchema.index({ sessionIds: 1 });
leadSchema.index({ visitorIds: 1 });
profileSchema.index({ email: 1 });
profileSchema.index({ phone: 1 });
profileSchema.index({ visitorIds: 1 });
profileSchema.index({ leadId: 1 });
messageSchema.index({ visitorId: 1, createdAt: -1 });
messageSchema.index({ sessionId: 1, createdAt: -1 });
coachUserSchema.index({ stripeCustomerId: 1 });
coachUserSchema.index({ stripeSubscriptionId: 1 });
coachSessionSchema.index({ expiresAt: 1 }, { expireAfterSeconds: 0 });

const Lead = mongoose.models.Lead || mongoose.model("Lead", leadSchema);
const Profile = mongoose.models.Profile || mongoose.model("Profile", profileSchema);
const Message = mongoose.models.Message || mongoose.model("Message", messageSchema);
const CoachUser = mongoose.models.CoachUser || mongoose.model("CoachUser", coachUserSchema);
const CoachSession = mongoose.models.CoachSession || mongoose.model("CoachSession", coachSessionSchema);

app.post("/webhooks/stripe", express.raw({ type: "application/json" }), manejarWebhookStripe);
app.use(express.json());

function normalizarEmail(email = "") {
  return String(email || "").trim().toLowerCase();
}

function passwordEsValido(password = "") {
  return typeof password === "string" && password.trim().length >= COACH_PASSWORD_MIN;
}

function crearPasswordSeguro(password) {
  const salt = crypto.randomBytes(16).toString("hex");
  const hash = crypto.scryptSync(password, salt, 64).toString("hex");

  return { salt, hash };
}

function verificarPasswordSeguro(password, salt, hashGuardado) {
  if (!password || !salt || !hashGuardado) {
    return false;
  }

  const hashCalculado = crypto.scryptSync(password, salt, 64);
  const hashOriginal = Buffer.from(hashGuardado, "hex");

  if (hashCalculado.length !== hashOriginal.length) {
    return false;
  }

  return crypto.timingSafeEqual(hashCalculado, hashOriginal);
}

function generarTokenSeguro() {
  return crypto.randomBytes(32).toString("base64url");
}

function hashTokenSeguro(token) {
  return crypto.createHash("sha256").update(token).digest("hex");
}

function parsearCookies(header = "") {
  return header
    .split(";")
    .map(fragmento => fragmento.trim())
    .filter(Boolean)
    .reduce((acumulado, fragmento) => {
      const indiceSeparador = fragmento.indexOf("=");

      if (indiceSeparador === -1) {
        return acumulado;
      }

      const key = fragmento.slice(0, indiceSeparador).trim();
      const value = decodeURIComponent(fragmento.slice(indiceSeparador + 1).trim());
      acumulado[key] = value;
      return acumulado;
    }, {});
}

function requestEsSeguro(req) {
  return req.secure || req.headers["x-forwarded-proto"] === "https";
}

function obtenerBaseUrl(req) {
  if (process.env.APP_BASE_URL) {
    return process.env.APP_BASE_URL.replace(/\/+$/, "");
  }

  const protocol = requestEsSeguro(req) ? "https" : "http";
  return `${protocol}://${req.get("host")}`;
}

function convertirUnixADate(unix) {
  if (!unix) {
    return null;
  }

  return new Date(unix * 1000);
}

function coachTieneAcceso(status = "") {
  return ["active", "trialing"].includes(String(status || "").toLowerCase());
}

function coachTieneAccesoDePrueba(email = "") {
  return COACH_TEST_ACCESS_EMAILS.includes(normalizarEmail(email));
}

function coachTieneAccesoTotal(userDoc = null) {
  if (!userDoc) {
    return false;
  }

  return Boolean(userDoc.subscriptionActive) || coachTieneAccesoDePrueba(userDoc.email);
}

function coachPuedeIniciarTrial(userDoc = null) {
  if (!userDoc) {
    return true;
  }

  if (coachTieneAccesoDePrueba(userDoc.email)) {
    return false;
  }

  return !userDoc.stripeSubscriptionId;
}

function obtenerCoachStatusVisible(userDoc = null) {
  if (!userDoc) {
    return "inactive";
  }

  if (coachTieneAccesoDePrueba(userDoc.email) && !coachTieneAcceso(userDoc.subscriptionStatus)) {
    return "test_access";
  }

  return userDoc.subscriptionStatus || "inactive";
}

function limpiarCoachUser(userDoc) {
  if (!userDoc) {
    return null;
  }

  return {
    id: String(userDoc._id),
    name: userDoc.name || "",
    email: userDoc.email || "",
    subscriptionStatus: obtenerCoachStatusVisible(userDoc),
    subscriptionActive: coachTieneAccesoTotal(userDoc),
    subscriptionCurrentPeriodEnd: userDoc.subscriptionCurrentPeriodEnd || null,
    subscriptionCancelAtPeriodEnd: Boolean(userDoc.subscriptionCancelAtPeriodEnd),
    stripeCustomerId: userDoc.stripeCustomerId || "",
    lastCheckoutSessionId: userDoc.lastCheckoutSessionId || "",
    trialEligible: coachPuedeIniciarTrial(userDoc)
  };
}

function normalizarPlanCoach(plan = "") {
  const value = String(plan || "").trim().toLowerCase();

  if (value === "annual") {
    return "annual";
  }

  if (value === "trial") {
    return "trial";
  }

  return "monthly";
}

function obtenerPlanCoach(plan = "", userDoc = null) {
  const selectedPlan = normalizarPlanCoach(plan);

  if (selectedPlan === "annual") {
    if (!STRIPE_PRICE_ID_ANNUAL) {
      throw new Error("El precio anual del Coach todavia no esta configurado.");
    }

    return {
      code: "annual",
      label: "plan anual",
      priceId: STRIPE_PRICE_ID_ANNUAL,
      trialDays: 0
    };
  }

  if (selectedPlan === "trial") {
    if (!STRIPE_PRICE_ID_MONTHLY) {
      throw new Error("El precio mensual del Coach todavia no esta configurado.");
    }

    if (!coachPuedeIniciarTrial(userDoc)) {
      throw new Error("La prueba gratis ya no esta disponible para esta cuenta.");
    }

    return {
      code: "trial",
      label: "prueba gratis",
      priceId: STRIPE_PRICE_ID_MONTHLY,
      trialDays: COACH_TRIAL_DAYS
    };
  }

  if (!STRIPE_PRICE_ID_MONTHLY) {
    throw new Error("El precio mensual del Coach todavia no esta configurado.");
  }

  return {
    code: "monthly",
    label: "plan mensual",
    priceId: STRIPE_PRICE_ID_MONTHLY,
    trialDays: 0
  };
}

function responderCoachError(res, statusCode, message) {
  return res.status(statusCode).json({ error: message });
}

function setCoachCookie(res, req, token) {
  res.cookie(COACH_SESSION_COOKIE, token, {
    httpOnly: true,
    sameSite: "lax",
    secure: requestEsSeguro(req),
    path: "/",
    maxAge: COACH_SESSION_DAYS * 24 * 60 * 60 * 1000
  });
}

function clearCoachCookie(res, req) {
  res.clearCookie(COACH_SESSION_COOKIE, {
    httpOnly: true,
    sameSite: "lax",
    secure: requestEsSeguro(req),
    path: "/"
  });
}

async function crearCoachSesion(req, res, userId) {
  const token = generarTokenSeguro();
  const tokenHash = hashTokenSeguro(token);
  const expiresAt = new Date(Date.now() + COACH_SESSION_DAYS * 24 * 60 * 60 * 1000);

  await CoachSession.create({
    userId,
    tokenHash,
    expiresAt,
    lastSeenAt: new Date()
  });

  setCoachCookie(res, req, token);
}

async function destruirCoachSesion(req, res) {
  const cookies = parsearCookies(req.headers.cookie || "");
  const token = cookies[COACH_SESSION_COOKIE];

  if (token) {
    await CoachSession.deleteOne({ tokenHash: hashTokenSeguro(token) });
  }

  clearCoachCookie(res, req);
}

async function obtenerCoachAuth(req) {
  const cookies = parsearCookies(req.headers.cookie || "");
  const token = cookies[COACH_SESSION_COOKIE];

  if (!token) {
    return { session: null, user: null };
  }

  const tokenHash = hashTokenSeguro(token);
  const session = await CoachSession.findOne({
    tokenHash,
    expiresAt: { $gt: new Date() }
  }).populate("userId");

  if (!session || !session.userId) {
    return { session: null, user: null };
  }

  session.lastSeenAt = new Date();
  await session.save();

  return {
    session,
    user: session.userId
  };
}

async function requireCoachUser(req, res) {
  const auth = await obtenerCoachAuth(req);

  if (!auth.user) {
    responderCoachError(res, 401, "Necesitas iniciar sesion.");
    return null;
  }

  return auth;
}

async function requireCoachActivo(req, res) {
  const auth = await requireCoachUser(req, res);

  if (!auth) {
    return null;
  }

  if (!coachTieneAccesoTotal(auth.user)) {
    responderCoachError(res, 403, "Tu cuenta no tiene una suscripcion activa.");
    return null;
  }

  return auth;
}

function stripeConfigurado() {
  return Boolean(stripe && STRIPE_PRICE_ID_MONTHLY && process.env.STRIPE_WEBHOOK_SECRET);
}

function stripeListoParaCheckout() {
  return Boolean(stripe && STRIPE_PRICE_ID_MONTHLY);
}

function construirCheckoutCoach(req, userDoc, plan = "monthly") {
  const selectedPlan = obtenerPlanCoach(plan, userDoc);
  const checkoutConfig = {
    mode: "subscription",
    customer: userDoc.stripeCustomerId,
    client_reference_id: String(userDoc._id),
    line_items: [
      {
        price: selectedPlan.priceId,
        quantity: 1
      }
    ],
    success_url: `${obtenerBaseUrl(req)}/coach/success/?session_id={CHECKOUT_SESSION_ID}`,
    cancel_url: `${obtenerBaseUrl(req)}/coach/cancel/`,
    allow_promotion_codes: true,
    metadata: {
      coachUserId: String(userDoc._id),
      coachPlan: selectedPlan.code
    },
    subscription_data: {
      metadata: {
        coachUserId: String(userDoc._id),
        productMode: "coach",
        coachPlan: selectedPlan.code
      }
    }
  };

  if (selectedPlan.code === "trial") {
    checkoutConfig.payment_method_collection = "if_required";
    checkoutConfig.subscription_data.trial_period_days = selectedPlan.trialDays;
    checkoutConfig.subscription_data.trial_settings = {
      end_behavior: {
        missing_payment_method: "pause"
      }
    };
  }

  return {
    selectedPlan,
    checkoutConfig
  };
}

async function asegurarCoachCustomer(userDoc) {
  if (!stripe) {
    throw new Error("Stripe no configurado.");
  }

  if (userDoc.stripeCustomerId) {
    return userDoc;
  }

  const customer = await stripe.customers.create({
    email: userDoc.email,
    name: userDoc.name,
    metadata: {
      coachUserId: String(userDoc._id)
    }
  });

  userDoc.stripeCustomerId = customer.id;
  userDoc.updatedAt = new Date();
  await userDoc.save();
  return userDoc;
}

async function actualizarCoachSuscripcion({
  coachUserId = "",
  customerId = "",
  subscriptionId = "",
  priceId = "",
  status = "",
  currentPeriodEnd = null,
  cancelAtPeriodEnd = false,
  checkoutSessionId = ""
}) {
  let userDoc = null;

  if (coachUserId) {
    userDoc = await CoachUser.findById(coachUserId);
  }

  if (!userDoc && customerId) {
    userDoc = await CoachUser.findOne({ stripeCustomerId: customerId });
  }

  if (!userDoc && subscriptionId) {
    userDoc = await CoachUser.findOne({ stripeSubscriptionId: subscriptionId });
  }

  if (!userDoc) {
    return null;
  }

  if (customerId) {
    userDoc.stripeCustomerId = customerId;
  }

  if (subscriptionId) {
    userDoc.stripeSubscriptionId = subscriptionId;
  }

  if (priceId) {
    userDoc.stripePriceId = priceId;
  }

  if (checkoutSessionId) {
    userDoc.lastCheckoutSessionId = checkoutSessionId;
  }

  userDoc.subscriptionStatus = status || "inactive";
  userDoc.subscriptionActive = coachTieneAcceso(status);
  userDoc.subscriptionCurrentPeriodEnd = currentPeriodEnd || null;
  userDoc.subscriptionCancelAtPeriodEnd = Boolean(cancelAtPeriodEnd);
  userDoc.updatedAt = new Date();

  await userDoc.save();
  return userDoc;
}

async function encontrarCoachUserPorCheckout(session) {
  const coachUserId = session?.metadata?.coachUserId || session?.client_reference_id || "";

  if (coachUserId) {
    const userById = await CoachUser.findById(coachUserId);

    if (userById) {
      return userById;
    }
  }

  if (session?.customer) {
    const userByCustomer = await CoachUser.findOne({ stripeCustomerId: session.customer });

    if (userByCustomer) {
      return userByCustomer;
    }
  }

  const email = normalizarEmail(session?.customer_details?.email || session?.customer_email || "");

  if (email) {
    return CoachUser.findOne({ email });
  }

  return null;
}

async function sincronizarCoachDesdeCheckoutSession(sessionId) {
  if (!stripe) {
    throw new Error("Stripe no configurado.");
  }

  const session = await stripe.checkout.sessions.retrieve(sessionId, {
    expand: ["subscription"]
  });
  const userDoc = await encontrarCoachUserPorCheckout(session);

  if (!userDoc) {
    return null;
  }

  const subscription = session.subscription && typeof session.subscription === "object"
    ? session.subscription
    : null;

  return actualizarCoachSuscripcion({
    coachUserId: String(userDoc._id),
    customerId: typeof session.customer === "string" ? session.customer : userDoc.stripeCustomerId || "",
    subscriptionId: subscription?.id || (typeof session.subscription === "string" ? session.subscription : ""),
    priceId: subscription?.items?.data?.[0]?.price?.id || STRIPE_PRICE_ID_MONTHLY || "",
    status: subscription?.status || userDoc.subscriptionStatus || "inactive",
    currentPeriodEnd: convertirUnixADate(subscription?.current_period_end),
    cancelAtPeriodEnd: Boolean(subscription?.cancel_at_period_end),
    checkoutSessionId: session.id
  });
}

async function manejarCheckoutCompletadoStripe(session) {
  if (!stripe) {
    return;
  }

  await sincronizarCoachDesdeCheckoutSession(session.id);
}

async function manejarSubscriptionEventStripe(subscription) {
  const customerId =
    typeof subscription.customer === "string" ? subscription.customer : subscription.customer?.id || "";

  await actualizarCoachSuscripcion({
    customerId,
    subscriptionId: subscription.id,
    priceId: subscription.items?.data?.[0]?.price?.id || "",
    status: subscription.status || "inactive",
    currentPeriodEnd: convertirUnixADate(subscription.current_period_end),
    cancelAtPeriodEnd: Boolean(subscription.cancel_at_period_end)
  });
}

async function manejarWebhookStripe(req, res) {
  if (!stripeConfigurado()) {
    return res.status(503).send("Stripe webhook no configurado");
  }

  const signature = req.headers["stripe-signature"];
  let event;

  try {
    event = stripe.webhooks.constructEvent(
      req.body,
      signature,
      process.env.STRIPE_WEBHOOK_SECRET
    );
  } catch (error) {
    console.error("Webhook Stripe invalido:", error.message);
    return res.status(400).send(`Webhook Error: ${error.message}`);
  }

  try {
    switch (event.type) {
      case "checkout.session.completed":
        await manejarCheckoutCompletadoStripe(event.data.object);
        break;
      case "customer.subscription.created":
      case "customer.subscription.updated":
      case "customer.subscription.deleted":
        await manejarSubscriptionEventStripe(event.data.object);
        break;
      default:
        break;
    }

    res.json({ received: true });
  } catch (error) {
    console.error("Error procesando webhook Stripe:", error.message);
    res.status(500).json({ error: "Error procesando webhook Stripe" });
  }
}

// =============================
// FUNCION JSON SEGURA
// =============================
function cargarJSON(ruta) {
  try {
    return JSON.parse(fs.readFileSync(ruta, "utf8"));
  } catch (error) {
    console.log("Error cargando:", ruta);
    return null;
  }
}

function extraerLeadInfo(texto) {
  if (!texto) {
    return null;
  }

  const emailMatch = texto.match(/[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}/i);
  const phoneMatch = texto.match(/(?:\+?\d[\d()\-\s]{7,}\d)/);
  const email = emailMatch ? emailMatch[0].toLowerCase() : "";
  const phone = phoneMatch ? phoneMatch[0].replace(/[^\d+]/g, "") : "";

  if (!email && !phone) {
    return null;
  }

  return {
    name: "",
    email,
    phone,
    message: texto
  };
}

function extraerNombre(texto) {
  const nombreMatch = texto.match(
    /(?:mi nombre es|soy)\s+([a-záéíóúñ][a-záéíóúñ\s]{1,60})/i
  );

  if (!nombreMatch) {
    return "";
  }

  return nombreMatch[1]
    .split(/\s+(?:y\s+mi|mi\s+n[uú]mero|mi\s+telefono|mi\s+tel[eé]fono|mi\s+correo|mi\s+email|quiero|para)\b/i)[0]
    .trim()
    .replace(/[.,;!?]+$/, "");
}

function extraerProductos(texto) {
  const productosDetectados = [];
  const catalogoProductos = [
    { nombre: "extractor", regex: /\bextractor\b/i },
    { nombre: "olla de presion", regex: /\bolla(?:\s+de)?\s+presi[oó]n\b/i },
    { nombre: "ollas", regex: /\bollas\b/i },
    { nombre: "sarten", regex: /\bsart(?:e|é)n(?:es)?\b/i },
    { nombre: "easy release", regex: /\beasy\s+release\b/i },
    { nombre: "cacerola", regex: /\bcacerolas?\b/i },
    { nombre: "bateria de cocina", regex: /\bbater[ií]a\s+de\s+cocina\b/i },
    { nombre: "vaporeras", regex: /\bvaporeras?\b/i },
    { nombre: "comal", regex: /\bcomal(?:es)?\b/i },
    { nombre: "wok", regex: /\bwok\b/i },
    { nombre: "cuchillo santoku", regex: /\bsantoku\b/i },
    { nombre: "cuchillo", regex: /\bcuchill(?:o|os)\b/i },
    { nombre: "royal prestige", regex: /\broyal\s+prestige\b/i }
  ];

  for (const producto of catalogoProductos) {
    if (producto.regex.test(texto)) {
      productosDetectados.push(producto.nombre);
    }
  }

  return [...new Set(productosDetectados)];
}

function extraerCocinaPara(texto) {
  const match = texto.match(
    /(?:cocino\s+para|somos|para)\s+(\d{1,2})\s+personas?/i
  );

  if (!match) {
    return "";
  }

  return `${match[1]} personas`;
}

function extraerEstadoCliente(texto) {
  if (/(?:no\s+soy\s+client[ea]|a[uú]n\s+no\s+soy\s+client[ea]|todav[ií]a\s+no\s+soy\s+client[ea])/i.test(texto)) {
    return "no";
  }

  if (/(?:ya\s+soy\s+client[ea]|soy\s+client[ea]|ya\s+compr[eé]|ya\s+tengo\s+productos?)/i.test(texto)) {
    return "si";
  }

  return "";
}

function extraerDireccion(texto) {
  const match = texto.match(
    /(?:mi direcci[oó]n es|vivo en|estoy en|me ubico en)\s+([^.\n]+)/i
  );

  if (!match) {
    return "";
  }

  const resto = match[1].trim();
  const restoNormalizado = resto.toLowerCase();
  const marcadoresCorte = [
    ", no soy",
    ", soy cliente",
    ", ya soy cliente",
    ", no tengo",
    ", tengo",
    ", ya tengo",
    ", no necesito",
    ", necesito",
    ", cocino",
    ", somos",
    ", me dedico",
    ", trabajo",
    ", quiero",
    ", me interesa",
    " y no soy",
    " y soy cliente",
    " y no tengo",
    " y tengo",
    " y no necesito",
    " y cocino",
    " y me dedico",
    " y trabajo",
    " y quiero",
    " y me interesa"
  ];
  let indiceCorte = resto.length;

  for (const marcador of marcadoresCorte) {
    const indice = restoNormalizado.indexOf(marcador);

    if (indice !== -1 && indice < indiceCorte) {
      indiceCorte = indice;
    }
  }

  return resto.slice(0, indiceCorte).trim().replace(/[.,;!?]+$/, "");
}

function extraerOcupacion(texto) {
  const patrones = [
    /(?:me dedico a|trabajo en|trabajo de)\s+([^.\n]+)/i,
    /(?:soy)\s+([a-záéíóúñ][a-záéíóúñ\s]{2,50})/i
  ];

  for (const patron of patrones) {
    const match = texto.match(patron);

    if (!match) {
      continue;
    }

    const ocupacion = match[1]
      .split(/\s+(?:y\s+mi|mi\s+n[uú]mero|mi\s+telefono|mi\s+tel[eé]fono|mi\s+correo|mi\s+email|quiero|necesito|vivo|estoy|para)\b/i)[0]
      .trim()
      .replace(/[.,;!?]+$/, "");

    if (
      ocupacion &&
      !/(?:client[ea]|interesad[oa]|de\s+[a-záéíóúñ]+|royal\s+prestige|garant[ií]a)/i.test(
        ocupacion
      )
    ) {
      return ocupacion;
    }
  }

  return "";
}

function extraerTemasInteres(texto) {
  const temas = [];
  const mapaTemas = [
    { nombre: "recetas saludables", regex: /\b(?:receta|saludable|cocinar|comida|cena|desayuno)\b/i },
    { nombre: "pollo", regex: /\bpollo\b/i },
    { nombre: "carne", regex: /\b(?:carne|res|bistec)\b/i },
    { nombre: "pescado", regex: /\b(?:pescado|salmon|salm[oó]n)\b/i },
    { nombre: "pancakes", regex: /\b(?:pancake|hotcake|panqueque)\b/i },
    { nombre: "garantia", regex: /\bgarant[ií]a\b/i },
    { nombre: "precios", regex: /\b(?:precio|precios|cuesta|pagos?|financiamiento)\b/i },
    { nombre: "llamada informativa", regex: /\b(?:llamada|agendar|cita|representante)\b/i }
  ];

  for (const tema of mapaTemas) {
    if (tema.regex.test(texto)) {
      temas.push(tema.nombre);
    }
  }

  return combinarListas(temas, extraerProductos(texto));
}

function extraerDetallesLead(texto) {
  return {
    name: extraerNombre(texto),
    productos: extraerProductos(texto),
    temasInteres: extraerTemasInteres(texto),
    cocinaPara: extraerCocinaPara(texto),
    esCliente: extraerEstadoCliente(texto),
    direccion: extraerDireccion(texto),
    ocupacion: extraerOcupacion(texto)
  };
}

function extraerTieneProductos(texto, productosDetectados = []) {
  if (/(?:no\s+tengo\s+productos?|no\s+tengo\s+royal\s+prestige|todav[ií]a\s+no\s+tengo\s+productos?|a[uú]n\s+no\s+tengo\s+productos?)/i.test(texto)) {
    return "no";
  }

  if (
    /(?:tengo\s+productos?|ya\s+tengo\s+productos?|cuento\s+con\s+productos?|soy\s+client[ea]|ya\s+soy\s+client[ea]|ya\s+compr[eé])/i.test(texto) ||
    (productosDetectados.length && /(?:tengo|ya\s+tengo|cuento\s+con)/i.test(texto))
  ) {
    return "si";
  }

  return "";
}

function extraerNecesitaGarantia(texto) {
  if (
    /garant[ií]a/i.test(texto) &&
    /(?:todo\s+est[aá]\s+bien|no\s+necesito|no\s+ocupo|sin\s+problema)/i.test(texto)
  ) {
    return "no";
  }

  if (
    /garant[ií]a/i.test(texto) &&
    /(?:necesito|ocupo|quiero|ayuda|problema|reclamo|cambio|soporte|fall[oó]|no\s+sirve)/i.test(texto)
  ) {
    return "si";
  }

  return "";
}

function detectarEnvioDatosContacto(texto) {
  const leadInfo = extraerLeadInfo(texto);
  const detallesLead = extraerDetallesLead(texto);

  return Boolean(leadInfo?.phone || leadInfo?.email) && Boolean(
    detallesLead.name ||
      detallesLead.direccion ||
      detallesLead.cocinaPara ||
      detallesLead.esCliente ||
      detallesLead.ocupacion
  );
}

function detectarConsultaPrecio(texto) {
  return /(?:precio|precios|cu[aá]nto|cu[aá]ntos|cuesta|cost[oó]|pagos?|diario|semanal|mensual|financiamiento|plan(?:es)?\s+de\s+pago)/i.test(
    texto
  );
}

function detectarInteresComercial(texto) {
  return /(?:precio|precios|cuesta|cost[oó]|pago|pagos|interesa|interesado|quiero\s+informaci[oó]n|quiero\s+saber|cat[aá]logo|representante|agendar|cita|llamada|demo|demostraci[oó]n|comprar|promoci[oó]n|oferta)/i.test(
    texto
  );
}

function detectarAceptacionLlamada(texto, estado) {
  const textoLimpio = texto.trim().toLowerCase();

  if (
    /(?:agend|ll[aá]mame|ll[aá]menme|marc[aá]me|marquenme|quiero\s+la\s+llamada|quiero\s+agendar|s[ií],?\s+me\s+interesa\s+la\s+llamada|s[ií],?\s+agendemos)/i.test(
      texto
    )
  ) {
    return true;
  }

  if (
    (estado.interesComercial || estado.consultaPrecio) &&
    /^(si|sí|claro|perfecto|ok|dale|est[aá]\s+bien)$/i.test(textoLimpio)
  ) {
    return true;
  }

  return false;
}

function detectarRechazoLlamada(texto, estado) {
  if (!(estado.interesComercial || estado.consultaPrecio)) {
    return false;
  }

  return /^(no|ahorita no|despu[eé]s|luego|solo\s+era\s+una\s+pregunta)$/i.test(
    texto.trim().toLowerCase()
  );
}

function combinarListas(base = [], nuevas = []) {
  return [...new Set([...base, ...nuevas].filter(Boolean))];
}

function detectarIntentoMensaje(texto, estado = null) {
  const estadoBase = estado || { interesComercial: false, consultaPrecio: false };

  if (detectarConsultaPrecio(texto)) {
    return "consulta_precio";
  }

  if (detectarAceptacionLlamada(texto, estadoBase)) {
    return "acepta_llamada";
  }

  if (detectarRechazoLlamada(texto, estadoBase)) {
    return "rechaza_llamada";
  }

  if (detectarEnvioDatosContacto(texto)) {
    return "comparte_datos";
  }

  if (/\b(?:receta|cocinar|desayuno|comida|cena|pollo|carne|pescado|salm[oó]n|pancake|hotcake|panqueque)\b/i.test(texto)) {
    return "consulta_receta";
  }

  if (/\b(?:garant[ií]a|material|producto|productos)\b/i.test(texto)) {
    return "consulta_producto";
  }

  return "general";
}

function inferirLeadStatus({
  leadGuardado = null,
  estadoConversacion = null,
  tieneDatosContacto = false,
  esCliente = "",
  quiereLlamada = ""
}) {
  if (esCliente === "si" || leadGuardado?.esCliente === "si") {
    return "cliente";
  }

  if ((quiereLlamada === "si" || leadGuardado?.quiereLlamada === "si") && tieneDatosContacto) {
    return "calificado";
  }

  if (quiereLlamada === "si" || leadGuardado?.quiereLlamada === "si") {
    return "interesado";
  }

  if (tieneDatosContacto) {
    return "interesado";
  }

  if (estadoConversacion?.interesComercial || estadoConversacion?.consultaPrecio) {
    return "interesado";
  }

  return "anonimo";
}

function construirProfileSummary(profile) {
  if (!profile) {
    return "";
  }

  const partes = [];

  if (profile.name) {
    partes.push(`Nombre: ${profile.name}.`);
  }

  if (profile.ocupacion) {
    partes.push(`Ocupacion: ${profile.ocupacion}.`);
  }

  if (profile.cocinaPara) {
    partes.push(`Cocina para ${profile.cocinaPara}.`);
  }

  if (profile.esCliente) {
    partes.push(`Estado cliente: ${profile.esCliente}.`);
  }

  if (profile.tieneProductos) {
    partes.push(`Tiene productos: ${profile.tieneProductos}.`);
  }

  if (profile.productos?.length) {
    partes.push(`Productos confirmados: ${profile.productos.join(", ")}.`);
  }

  if (profile.productosInteres?.length) {
    partes.push(`Productos de interes: ${profile.productosInteres.join(", ")}.`);
  }

  if (profile.temasInteres?.length) {
    partes.push(`Temas de interes: ${profile.temasInteres.join(", ")}.`);
  }

  if (profile.necesitaGarantia) {
    partes.push(`Garantia: ${profile.necesitaGarantia}.`);
  }

  if (profile.quiereLlamada) {
    partes.push(`Interes en llamada: ${profile.quiereLlamada}.`);
  }

  if (profile.leadStatus) {
    partes.push(`Estado comercial: ${profile.leadStatus}.`);
  }

  return partes.join(" ");
}

async function resolverPerfilExistente(visitorId, email = "", phone = "", leadId = null) {
  const condiciones = [];

  if (visitorId) {
    condiciones.push({ visitorId });
    condiciones.push({ visitorIds: visitorId });
  }

  if (email) {
    condiciones.push({ email });
  }

  if (phone) {
    condiciones.push({ phone });
  }

  if (leadId) {
    condiciones.push({ leadId });
  }

  if (!condiciones.length) {
    return null;
  }

  return await Profile.findOne({ $or: condiciones }).sort({ createdAt: 1 });
}

async function guardarMensajeRaw({
  visitorId,
  sessionId,
  profileId = null,
  leadId = null,
  role,
  content,
  estadoConversacion = null
}) {
  if (!visitorId || !content) {
    return null;
  }

  try {
    return await Message.create({
      visitorId,
      sessionId,
      profileId,
      leadId,
      role,
      content,
      intent: role === "user" ? detectarIntentoMensaje(content, estadoConversacion) : "respuesta_ai",
      detectedTopics: role === "user" ? extraerTemasInteres(content) : [],
      createdAt: new Date()
    });
  } catch (error) {
    console.log("Error guardando mensaje raw MongoDB:", error.message);
    return null;
  }
}

async function guardarOActualizarPerfil({
  visitorId,
  sessionId,
  texto,
  estadoConversacion = null,
  leadGuardado = null
}) {
  if (!visitorId) {
    return null;
  }

  const leadInfo = extraerLeadInfo(texto);
  const detallesLead = extraerDetallesLead(texto);
  const tieneProductos = extraerTieneProductos(texto, detallesLead.productos);
  const necesitaGarantia = extraerNecesitaGarantia(texto);
  const perfilExistente = await resolverPerfilExistente(
    visitorId,
    leadGuardado?.email || leadInfo?.email || "",
    leadGuardado?.phone || leadInfo?.phone || "",
    leadGuardado?._id || null
  );
  const productosConfirmados = tieneProductos === "si"
    ? combinarListas(
        perfilExistente?.productos || [],
        detallesLead.productos
      )
    : perfilExistente?.productos || [];
  const productosInteres = tieneProductos === "si"
    ? perfilExistente?.productosInteres || []
    : combinarListas(
        perfilExistente?.productosInteres || [],
        detallesLead.productos
      );
  const profilePayload = {
    visitorId: perfilExistente?.visitorId || visitorId,
    visitorIds: combinarListas(
      perfilExistente?.visitorIds || [],
      visitorId ? [visitorId] : []
    ),
    sessionIds: combinarListas(perfilExistente?.sessionIds || [], sessionId ? [sessionId] : []),
    leadId: leadGuardado?._id || perfilExistente?.leadId || null,
    name: seleccionarValorMasCompleto(
      leadGuardado?.name,
      detallesLead.name,
      estadoConversacion?.name,
      perfilExistente?.name
    ),
    email: seleccionarValorString(
      leadGuardado?.email,
      leadInfo?.email,
      perfilExistente?.email
    ),
    phone: seleccionarValorString(
      leadGuardado?.phone,
      leadInfo?.phone,
      perfilExistente?.phone
    ),
    direccion: seleccionarValorMasCompleto(
      leadGuardado?.direccion,
      detallesLead.direccion,
      estadoConversacion?.direccion,
      perfilExistente?.direccion
    ),
    ocupacion: seleccionarValorMasCompleto(
      leadGuardado?.ocupacion,
      detallesLead.ocupacion,
      estadoConversacion?.ocupacion,
      perfilExistente?.ocupacion
    ),
    esCliente: seleccionarValorString(
      leadGuardado?.esCliente,
      detallesLead.esCliente,
      estadoConversacion?.esCliente,
      perfilExistente?.esCliente
    ),
    tieneProductos: seleccionarValorString(
      leadGuardado?.tieneProductos,
      tieneProductos,
      estadoConversacion?.tieneProductos,
      perfilExistente?.tieneProductos
    ),
    necesitaGarantia: seleccionarValorString(
      leadGuardado?.necesitaGarantia,
      necesitaGarantia,
      estadoConversacion?.necesitaGarantia,
      perfilExistente?.necesitaGarantia
    ),
    quiereLlamada: seleccionarValorString(
      leadGuardado?.quiereLlamada,
      estadoConversacion?.quiereLlamada,
      perfilExistente?.quiereLlamada
    ),
    productos: productosConfirmados,
    productosInteres,
    temasInteres: combinarListas(
      perfilExistente?.temasInteres || [],
      combinarListas(detallesLead.temasInteres, estadoConversacion?.temasInteres || [])
    ),
    cocinaPara: seleccionarValorMasCompleto(
      leadGuardado?.cocinaPara,
      detallesLead.cocinaPara,
      estadoConversacion?.cocinaPara,
      perfilExistente?.cocinaPara
    ),
    leadStatus: inferirLeadStatus({
      leadGuardado,
      estadoConversacion,
      tieneDatosContacto: Boolean(
        leadGuardado?.phone ||
        leadGuardado?.email ||
        leadInfo?.phone ||
        leadInfo?.email
      ),
      esCliente: leadGuardado?.esCliente || detallesLead.esCliente || estadoConversacion?.esCliente || "",
      quiereLlamada: leadGuardado?.quiereLlamada || estadoConversacion?.quiereLlamada || ""
    }),
    lastUserMessage: texto,
    lastInteractionAt: new Date(),
    updatedAt: new Date(),
    conversationCount: (perfilExistente?.conversationCount || 0) + 1
  };

  profilePayload.profileSummary = construirProfileSummary(profilePayload);

  try {
    return await Profile.findOneAndUpdate(
      perfilExistente?._id ? { _id: perfilExistente._id } : { visitorId },
      {
        $set: profilePayload,
        $push: {
          recentHistory: {
            $each: [
              {
                role: "user",
                content: texto,
                createdAt: new Date()
              }
            ],
            $slice: -8
          }
        },
        $setOnInsert: {
          createdAt: new Date()
        }
      },
      {
        new: true,
        upsert: true
      }
    );
  } catch (error) {
    console.log("Error guardando profile MongoDB:", error.message);
    return null;
  }
}

async function guardarRespuestaIAEnProfile(profile, respuestaIA, leadGuardado = null) {
  if (!profile || !respuestaIA) {
    return profile;
  }

  const payload = {
    lastAssistantMessage: respuestaIA,
    lastInteractionAt: new Date(),
    updatedAt: new Date(),
    leadId: leadGuardado?._id || profile.leadId || null
  };
  const profileBase = typeof profile.toObject === "function" ? profile.toObject() : profile;
  const profileActualizado = {
    ...profileBase,
    ...payload
  };
  profileActualizado.profileSummary = construirProfileSummary(profileActualizado);

  try {
    return await Profile.findByIdAndUpdate(
      profile._id,
      {
        $set: {
          ...payload,
          profileSummary: profileActualizado.profileSummary
        },
        $push: {
          recentHistory: {
            $each: [
              {
                role: "assistant",
                content: respuestaIA,
                createdAt: new Date()
              }
            ],
            $slice: -8
          }
        }
      },
      {
        new: true
      }
    );
  } catch (error) {
    console.log("Error guardando respuesta IA en profile MongoDB:", error.message);
    return profile;
  }
}

function obtenerEstadoConversacion(sessionId) {
  if (!estadosConversacion[sessionId]) {
    estadosConversacion[sessionId] = {
      interesComercial: false,
      consultaPrecio: false,
      llamadaInformativaAceptada: false,
      envioDatosContactoReciente: false,
      quiereLlamada: "",
      name: "",
      email: "",
      phone: "",
      direccion: "",
      ocupacion: "",
      esCliente: "",
      tieneProductos: "",
      necesitaGarantia: "",
      cocinaPara: "",
      productos: [],
      temasInteres: []
    };
  }

  return estadosConversacion[sessionId];
}

function actualizarEstadoConversacion(sessionId, texto, leadGuardado = null) {
  const estado = obtenerEstadoConversacion(sessionId);
  const leadInfo = extraerLeadInfo(texto);
  const detallesLead = extraerDetallesLead(texto);
  const tieneProductos = extraerTieneProductos(texto, detallesLead.productos);
  const necesitaGarantia = extraerNecesitaGarantia(texto);

  estado.envioDatosContactoReciente = detectarEnvioDatosContacto(texto);

  if (leadInfo?.email) {
    estado.email = leadInfo.email;
  }

  if (leadInfo?.phone) {
    estado.phone = leadInfo.phone;
  }

  if (detallesLead.name) {
    estado.name = detallesLead.name;
  }

  if (detallesLead.direccion) {
    estado.direccion = detallesLead.direccion;
  }

  if (detallesLead.ocupacion) {
    estado.ocupacion = detallesLead.ocupacion;
  }

  if (detallesLead.esCliente) {
    estado.esCliente = detallesLead.esCliente;
  }

  if (detallesLead.cocinaPara) {
    estado.cocinaPara = detallesLead.cocinaPara;
  }

  if (tieneProductos) {
    estado.tieneProductos = tieneProductos;
  }

  if (necesitaGarantia) {
    estado.necesitaGarantia = necesitaGarantia;
  }

  if (detallesLead.productos.length) {
    estado.productos = combinarListas(estado.productos, detallesLead.productos);
  }

  if (detallesLead.temasInteres.length) {
    estado.temasInteres = combinarListas(estado.temasInteres, detallesLead.temasInteres);
  }

  if (detectarConsultaPrecio(texto)) {
    estado.consultaPrecio = true;
    estado.interesComercial = true;
  }

  if (detectarInteresComercial(texto)) {
    estado.interesComercial = true;
  }

  if (detectarAceptacionLlamada(texto, estado)) {
    estado.llamadaInformativaAceptada = true;
    estado.quiereLlamada = "si";
  }

  if (detectarRechazoLlamada(texto, estado)) {
    estado.quiereLlamada = "no";
  }

  if (leadGuardado) {
    estado.name = leadGuardado.name || estado.name;
    estado.email = leadGuardado.email || estado.email;
    estado.phone = leadGuardado.phone || estado.phone;
    estado.direccion = leadGuardado.direccion || estado.direccion;
    estado.ocupacion = leadGuardado.ocupacion || estado.ocupacion;
    estado.esCliente = leadGuardado.esCliente || estado.esCliente;
    estado.cocinaPara = leadGuardado.cocinaPara || estado.cocinaPara;
    estado.tieneProductos = leadGuardado.tieneProductos || estado.tieneProductos;
    estado.necesitaGarantia = leadGuardado.necesitaGarantia || estado.necesitaGarantia;
    estado.quiereLlamada = leadGuardado.quiereLlamada || estado.quiereLlamada;
    estado.productos = combinarListas(estado.productos, leadGuardado.productos || []);
    estado.temasInteres = combinarListas(estado.temasInteres, leadGuardado.temasInteres || []);

    if (leadGuardado.quiereLlamada === "si") {
      estado.llamadaInformativaAceptada = true;
      estado.interesComercial = true;
    }
  }

  return estado;
}

function obtenerSiguienteDatoLead(estado) {
  const datosVitales = [];
  const datosComplementarios = [];

  if (!estado.name) {
    datosVitales.push("nombre completo");
  }

  if (!estado.phone) {
    datosVitales.push("telefono");
  }

  if (!estado.direccion) {
    datosComplementarios.push("ciudad o direccion");
  }

  if (!estado.esCliente) {
    datosComplementarios.push("si ya es cliente");
  }

  if (!estado.tieneProductos) {
    datosComplementarios.push("si ya tiene productos");
  }

  if (estado.tieneProductos === "si" && !estado.productos.length) {
    datosComplementarios.push("que productos tiene");
  }

  if (!estado.necesitaGarantia) {
    datosComplementarios.push("si necesita garantia");
  }

  if (!estado.cocinaPara) {
    datosComplementarios.push("para cuantas personas cocina");
  }

  if (!estado.ocupacion) {
    datosComplementarios.push("a que se dedica");
  }

  return {
    datosVitales,
    datosComplementarios
  };
}

function construirEstadoPrompt(sessionId) {
  const estado = obtenerEstadoConversacion(sessionId);
  const { datosVitales, datosComplementarios } = obtenerSiguienteDatoLead(estado);
  let fase = "chef-y-guia-de-uso";
  let instruccion = "Enfocate en ayudar con cocina saludable, recetas y uso correcto de productos Royal Prestige.";

  if (estado.interesComercial || estado.consultaPrecio) {
    fase = "interes-comercial";
    instruccion = "Si el usuario muestra interes comercial, invitalo a una llamada informativa sin compromiso con un representante 5 estrellas.";
  }

  if (estado.llamadaInformativaAceptada) {
    fase = "captura-breve-de-lead";

    if (estado.envioDatosContactoReciente && !datosVitales.length) {
      instruccion = "La persona acaba de compartir sus datos. Agradece brevemente, confirma la llamada informativa con el numero registrado y solo si cabe agrega una recomendacion util muy breve.";
    } else if (datosVitales.length) {
      const solicitudBreve = [...datosVitales, ...datosComplementarios].join(", ");
      instruccion = `La llamada informativa ya fue aceptada. Pide en un solo mensaje breve estos datos: ${solicitudBreve}. Hazlo natural y facil de contestar.`;
    } else if (datosComplementarios.length) {
      instruccion = `Ya tienes nombre y telefono. Si es natural, pide en un solo mensaje breve estos datos complementarios: ${datosComplementarios.join(", ")}.`;
    } else {
      instruccion = "Ya tienes los datos clave; confirma que un representante 5 estrellas puede continuar con la llamada informativa.";
    }
  }

  return `
ESTADO ACTUAL:
- fase: ${fase}
- interes_comercial: ${estado.interesComercial ? "si" : "no"}
- consulta_precio: ${estado.consultaPrecio ? "si" : "no"}
- llamada_informativa_aceptada: ${estado.llamadaInformativaAceptada ? "si" : "no"}
- envio_datos_contacto_reciente: ${estado.envioDatosContactoReciente ? "si" : "no"}
- nombre: ${estado.name || "pendiente"}
- email: ${estado.email || "pendiente"}
- telefono: ${estado.phone || "pendiente"}
- direccion: ${estado.direccion || "pendiente"}
- ocupacion: ${estado.ocupacion || "pendiente"}
- es_cliente: ${estado.esCliente || "pendiente"}
- tiene_productos: ${estado.tieneProductos || "pendiente"}
- productos_mencionados: ${estado.productos.length ? estado.productos.join(", ") : "pendiente"}
- necesita_garantia: ${estado.necesitaGarantia || "pendiente"}
- cocina_para: ${estado.cocinaPara || "pendiente"}
- temas_interes: ${estado.temasInteres.length ? estado.temasInteres.join(", ") : "pendiente"}
- datos_vitales_faltantes: ${datosVitales.length ? datosVitales.join(", ") : "ninguno"}
- datos_complementarios_faltantes: ${datosComplementarios.length ? datosComplementarios.join(", ") : "ninguno"}

INSTRUCCION OPERATIVA:
${instruccion}
`;
}

function seleccionarValorString(...values) {
  for (const value of values) {
    if (typeof value === "string" && value.trim()) {
      return value.trim();
    }
  }

  return "";
}

function seleccionarValorMasCompleto(...values) {
  const candidatos = values
    .filter(value => typeof value === "string" && value.trim())
    .map(value => value.trim())
    .sort((a, b) => b.length - a.length);

  return candidatos[0] || "";
}

function obtenerTimestampSeguro(value) {
  const fecha = value ? new Date(value) : null;

  if (!fecha || Number.isNaN(fecha.getTime())) {
    return 0;
  }

  return fecha.getTime();
}

function normalizarNotas(notas = []) {
  return notas
    .filter(note => note?.text)
    .map(note => ({
      text: note.text,
      createdAt: note.createdAt || new Date()
    }))
    .sort((a, b) => obtenerTimestampSeguro(a.createdAt) - obtenerTimestampSeguro(b.createdAt))
    .slice(-40);
}

function normalizarHistorialConversacion(historial = []) {
  return historial
    .filter(entry => entry?.role && entry?.content)
    .map(entry => ({
      role: entry.role,
      content: entry.content,
      createdAt: entry.createdAt || new Date()
    }))
    .sort((a, b) => obtenerTimestampSeguro(a.createdAt) - obtenerTimestampSeguro(b.createdAt))
    .slice(-30);
}

async function fusionarLeads(primaryLead, duplicateLeads = []) {
  if (!primaryLead || !duplicateLeads.length) {
    return primaryLead;
  }

  const allVisitorIds = combinarListas(
    primaryLead.visitorIds || [],
    duplicateLeads.flatMap(lead => lead.visitorIds || [])
  );
  const allSessionIds = combinarListas(
    primaryLead.sessionIds || [],
    duplicateLeads.flatMap(lead => lead.sessionIds || [])
  );
  const allProductos = combinarListas(
    primaryLead.productos || [],
    duplicateLeads.flatMap(lead => lead.productos || [])
  );
  const allTemasInteres = combinarListas(
    primaryLead.temasInteres || [],
    duplicateLeads.flatMap(lead => lead.temasInteres || [])
  );
  const allNotas = normalizarNotas([
    ...(primaryLead.notes || []),
    ...duplicateLeads.flatMap(lead => lead.notes || [])
  ]);
  const allHistorial = normalizarHistorialConversacion([
    ...(primaryLead.conversationHistory || []),
    ...duplicateLeads.flatMap(lead => lead.conversationHistory || [])
  ]);

  primaryLead.visitorIds = allVisitorIds;
  primaryLead.sessionIds = allSessionIds;
  primaryLead.productos = allProductos;
  primaryLead.temasInteres = allTemasInteres;
  primaryLead.notes = allNotas;
  primaryLead.conversationHistory = allHistorial;
  primaryLead.name = seleccionarValorMasCompleto(
    primaryLead.name,
    ...duplicateLeads.map(lead => lead.name)
  );
  primaryLead.email = seleccionarValorString(
    primaryLead.email,
    ...duplicateLeads.map(lead => lead.email)
  );
  primaryLead.phone = seleccionarValorString(
    primaryLead.phone,
    ...duplicateLeads.map(lead => lead.phone)
  );
  primaryLead.message = seleccionarValorMasCompleto(
    duplicateLeads
      .map(lead => lead.message)
      .filter(Boolean)
      .slice(-1)[0],
    primaryLead.message
  );
  primaryLead.ocupacion = seleccionarValorMasCompleto(
    primaryLead.ocupacion,
    ...duplicateLeads.map(lead => lead.ocupacion)
  );
  primaryLead.tieneProductos = seleccionarValorString(
    primaryLead.tieneProductos,
    ...duplicateLeads.map(lead => lead.tieneProductos)
  );
  primaryLead.necesitaGarantia = seleccionarValorString(
    primaryLead.necesitaGarantia,
    ...duplicateLeads.map(lead => lead.necesitaGarantia)
  );
  primaryLead.quiereLlamada = seleccionarValorString(
    primaryLead.quiereLlamada,
    ...duplicateLeads.map(lead => lead.quiereLlamada)
  );
  primaryLead.cocinaPara = seleccionarValorMasCompleto(
    primaryLead.cocinaPara,
    ...duplicateLeads.map(lead => lead.cocinaPara)
  );
  primaryLead.esCliente = seleccionarValorString(
    primaryLead.esCliente,
    ...duplicateLeads.map(lead => lead.esCliente)
  );
  primaryLead.direccion = seleccionarValorMasCompleto(
    primaryLead.direccion,
    ...duplicateLeads.map(lead => lead.direccion)
  );
  primaryLead.lastAssistantMessage = seleccionarValorMasCompleto(
    primaryLead.lastAssistantMessage,
    ...duplicateLeads.map(lead => lead.lastAssistantMessage)
  );
  primaryLead.leadStatus = seleccionarValorString(
    primaryLead.leadStatus,
    ...duplicateLeads.map(lead => lead.leadStatus)
  );
  primaryLead.lastInteractionAt = new Date(
    Math.max(
      obtenerTimestampSeguro(primaryLead.lastInteractionAt),
      ...duplicateLeads.map(lead => obtenerTimestampSeguro(lead.lastInteractionAt))
    )
  );
  primaryLead.updatedAt = new Date();

  await primaryLead.save();
  await Lead.deleteMany({
    _id: { $in: duplicateLeads.map(lead => lead._id) }
  });

  return primaryLead;
}

async function resolverLeadExistente(sessionId, visitorId = "", email = "", phone = "") {
  const condiciones = [];

  if (visitorId) {
    condiciones.push({ visitorIds: visitorId });
  }

  if (email) {
    condiciones.push({ email });
  }

  if (phone) {
    condiciones.push({ phone });
  }

  if (sessionId) {
    condiciones.push({ sessionIds: sessionId });
  }

  if (!condiciones.length) {
    return null;
  }

  const coincidencias = await Lead.find({ $or: condiciones }).sort({ createdAt: 1 });

  if (!coincidencias.length) {
    return null;
  }

  let primaryLead =
    coincidencias.find(lead => (email && lead.email === email) || (phone && lead.phone === phone)) ||
    coincidencias.find(lead => lead.email || lead.phone) ||
    coincidencias[0];

  const duplicateLeads = coincidencias.filter(lead => !lead._id.equals(primaryLead._id));

  if (duplicateLeads.length) {
    primaryLead = await fusionarLeads(primaryLead, duplicateLeads);
  }

  return primaryLead;
}

function construirPerfilHistoricoPrompt(profile = null, lead = null) {
  const fuente = profile || lead;

  if (!fuente) {
    return `
PERFIL HISTORICO:
- sin_historial_previo: si

INSTRUCCION:
Si aun no conoces a la persona, atiendela con calidez y usa solo lo que diga en esta conversacion.
`;
  }

  const notasRecientes = normalizarNotas(lead?.notes || [])
    .slice(-4)
    .map(note => note.text)
    .join(" | ");
  const historialReciente = normalizarHistorialConversacion(
    profile?.recentHistory || lead?.conversationHistory || []
  )
    .slice(-6)
    .map(entry => `- ${entry.role}: ${entry.content}`)
    .join("\n");

  return `
PERFIL HISTORICO:
- sin_historial_previo: no
- nombre: ${fuente.name || "desconocido"}
- telefono: ${fuente.phone || "desconocido"}
- email: ${fuente.email || "desconocido"}
- direccion: ${fuente.direccion || "desconocida"}
- ocupacion: ${fuente.ocupacion || "desconocida"}
- es_cliente: ${fuente.esCliente || "desconocido"}
- tiene_productos: ${fuente.tieneProductos || "desconocido"}
- productos_confirmados_del_cliente: ${fuente.tieneProductos === "si" && fuente.productos?.length ? fuente.productos.join(", ") : "sin productos confirmados"}
- productos_de_interes: ${profile?.productosInteres?.length ? profile.productosInteres.join(", ") : "ninguno"}
- productos_mencionados_en_historial: ${fuente.productos?.length ? fuente.productos.join(", ") : "ninguno"}
- cocina_para: ${fuente.cocinaPara || "desconocido"}
- necesita_garantia: ${fuente.necesitaGarantia || "desconocido"}
- temas_interes: ${fuente.temasInteres?.length ? fuente.temasInteres.join(", ") : "ninguno"}
- estado_comercial: ${profile?.leadStatus || lead?.leadStatus || "desconocido"}
- resumen_de_perfil: ${profile?.profileSummary || "sin resumen"}
- notas_relevantes: ${notasRecientes || "ninguna"}
- historial_reciente:
${historialReciente || "- sin historial reciente"}

INSTRUCCION:
Usa este perfil para personalizar recetas, recomendaciones y seguimiento comercial. No inventes datos faltantes ni recites el perfil completo al usuario.
`;
}

async function sincronizarLeadAGoogleSheets(lead) {
  if (!lead || !process.env.GOOGLE_SHEETS_WEBHOOK_URL) {
    return;
  }

  try {
    const payload = typeof lead.toObject === "function" ? lead.toObject() : lead;
    const response = await fetch(process.env.GOOGLE_SHEETS_WEBHOOK_URL, {
      method: "POST",
      headers: {
        "Content-Type": "application/json"
      },
      body: JSON.stringify(payload)
    });

    if (!response.ok) {
      console.log("Error Google Sheets:", response.status, response.statusText);
    }
  } catch (error) {
    console.log("Error sincronizando Google Sheets:", error.message);
  }
}

async function guardarLeadSiExiste(texto, sessionId, visitorId = "", estadoConversacion = null) {
  const leadInfo = extraerLeadInfo(texto);
  const detallesLead = extraerDetallesLead(texto);
  const email = leadInfo?.email || estadoConversacion?.email || "";
  const phone = leadInfo?.phone || estadoConversacion?.phone || "";

  try {
    const tieneProductos =
      extraerTieneProductos(texto, detallesLead.productos) || estadoConversacion?.tieneProductos || "";
    const necesitaGarantia =
      extraerNecesitaGarantia(texto) || estadoConversacion?.necesitaGarantia || "";
    const productosDetectados = combinarListas(
      estadoConversacion?.productos || [],
      detallesLead.productos
    );
    const temasInteresDetectados = combinarListas(
      estadoConversacion?.temasInteres || [],
      detallesLead.temasInteres
    );
    const leadExistente = await resolverLeadExistente(sessionId, visitorId, email, phone);

    if (!leadExistente && !email && !phone) {
      return null;
    }

    const leadId = leadExistente?._id || new mongoose.Types.ObjectId();
    const visitorIds = combinarListas(
      leadExistente?.visitorIds || [],
      visitorId ? [visitorId] : []
    );
    const sessionIds = combinarListas(
      leadExistente?.sessionIds || [],
      sessionId ? [sessionId] : []
    );
    const leadStatus = inferirLeadStatus({
      leadGuardado: leadExistente,
      estadoConversacion,
      tieneDatosContacto: Boolean(email || phone),
      esCliente: detallesLead.esCliente || estadoConversacion?.esCliente || leadExistente?.esCliente || "",
      quiereLlamada: estadoConversacion?.quiereLlamada || leadExistente?.quiereLlamada || ""
    });
    const camposActualizar = {
      visitorIds,
      sessionIds,
      message: texto,
      updatedAt: new Date(),
      lastInteractionAt: new Date(),
      quiereLlamada: estadoConversacion?.quiereLlamada || leadExistente?.quiereLlamada || "",
      leadStatus
    };
    const actualizacion = {
      $set: camposActualizar,
      $push: {
        notes: {
          $each: [
            {
              text: texto,
              createdAt: new Date()
            }
          ],
          $slice: -40
        },
        conversationHistory: {
          $each: [
            {
              role: "user",
              content: texto,
              createdAt: new Date()
            }
          ],
          $slice: -30
        }
      },
      $setOnInsert: {
        createdAt: new Date()
      }
    };

    if (email) {
      camposActualizar.email = email;
    }

    if (phone) {
      camposActualizar.phone = phone;
    }

    const nombre = seleccionarValorMasCompleto(
      detallesLead.name,
      estadoConversacion?.name,
      leadExistente?.name
    );
    const cocinaPara = seleccionarValorMasCompleto(
      detallesLead.cocinaPara,
      estadoConversacion?.cocinaPara,
      leadExistente?.cocinaPara
    );
    const esCliente = seleccionarValorString(
      detallesLead.esCliente,
      estadoConversacion?.esCliente,
      leadExistente?.esCliente
    );
    const direccion = seleccionarValorMasCompleto(
      detallesLead.direccion,
      estadoConversacion?.direccion,
      leadExistente?.direccion
    );
    const ocupacion = seleccionarValorMasCompleto(
      detallesLead.ocupacion,
      estadoConversacion?.ocupacion,
      leadExistente?.ocupacion
    );

    if (nombre) {
      camposActualizar.name = nombre;
    }

    if (cocinaPara) {
      camposActualizar.cocinaPara = cocinaPara;
    }

    if (esCliente) {
      camposActualizar.esCliente = esCliente;
    }

    if (direccion) {
      camposActualizar.direccion = direccion;
    }

    if (ocupacion) {
      camposActualizar.ocupacion = ocupacion;
    }

    if (tieneProductos) {
      camposActualizar.tieneProductos = tieneProductos;
    }

    if (necesitaGarantia) {
      camposActualizar.necesitaGarantia = necesitaGarantia;
    }

    if (productosDetectados.length) {
      actualizacion.$addToSet = {
        ...(actualizacion.$addToSet || {}),
        productos: { $each: productosDetectados }
      };
    }

    if (temasInteresDetectados.length) {
      actualizacion.$addToSet = {
        ...(actualizacion.$addToSet || {}),
        temasInteres: { $each: temasInteresDetectados }
      };
    }

    return await Lead.findByIdAndUpdate(leadId, actualizacion, {
      new: true,
      upsert: true
    });
  } catch (error) {
    console.log("Error guardando lead MongoDB:", error.message);
    return null;
  }
}

async function guardarRespuestaIAEnPerfil(lead, respuestaIA) {
  if (!lead || !respuestaIA) {
    return lead;
  }

  try {
    return await Lead.findByIdAndUpdate(
      lead._id,
      {
        $set: {
          lastAssistantMessage: respuestaIA,
          updatedAt: new Date(),
          lastInteractionAt: new Date()
        },
        $push: {
          conversationHistory: {
            $each: [
              {
                role: "assistant",
                content: respuestaIA,
                createdAt: new Date()
              }
            ],
            $slice: -30
          }
        }
      },
      {
        new: true
      }
    );
  } catch (error) {
    console.log("Error guardando respuesta IA en MongoDB:", error.message);
    return lead;
  }
}

// =============================
// BASES LIGERAS (SIEMPRE ACTIVAS)
// =============================
const preciosCatalogo = cargarJSON("./src/data/lista_de_precios.json");
const beneficiosProductos = cargarJSON("./src/data/Caracteristicas_Ventajas_Beneficios");
const encuestaVentas = cargarJSON("./src/data/Encuesta_intelijente");

// =============================
// BASES PESADAS (SOLO REFERENCIA)
// =============================
const inteligenciaVentas = cargarJSON("./src/data/Eric_Material_viejo");
const recetasRoyalPrestige = cargarJSON("./src/data/recetas_royal_prestige");
const especificacionesRoyalPrestige = cargarJSON("./src/data/especificasiones_royal_prestige");
const opcionesPagoRoyalPrestige = cargarJSON("./src/data/royalprestige_pagos_site.json");
const demoVenta = cargarJSON("./src/data/Demo_venta_1");

const cierresAlexDey = cargarJSON("./src/data/12_cierres_alex_dey.json");
const mentalidadOlmedo = cargarJSON("./src/data/mentalidad_ventas_olmedo.json");
const reclutamientoCiprian = cargarJSON("./src/data/reclutamiento_ciprian.json");
const sistema4Citas = cargarJSON("./src/data/sistema_4_citas_14_dias.json");

// =============================
// REDFIN
// =============================
const redfinPath = path.join(process.cwd(), "src/data/redfin_23");
let redfinProperties = [];

try {
  const raw = fs.readFileSync(redfinPath, "utf8");
  redfinProperties = JSON.parse(raw);
  console.log(`Loaded ${redfinProperties.length} properties`);
} catch (e) {
  console.log("Error redfin:", e.message);
}

// =============================
// PROMPT OPTIMIZADO
// =============================
const chefSystemPrompt = `
Eres Agustin 2.0, un chef inteligente, cercano y paciente. Ayudas gratis a familias latinas a cocinar mas rico, mas saludable y a usar bien sus productos Royal Prestige. Tambien eres un asistente comercial amable que detecta interes real y ayuda a pasar a llamada informativa sin presionar.

VOZ Y TONO:
- Habla como una persona calida, sencilla y de confianza
- Usa palabras faciles y frases cortas
- Suena cercano, natural y humano
- Explica como si estuvieras ayudando a una familia en su cocina
- Nunca suenes frio, corporativo, tecnico, presumido ni como call center
- Evita palabras rebuscadas como: optimizar, maximizar, proceder, consultivo, alineado, personalizado, gestionar, recopilar
- En vez de sonar tecnico, habla claro y aterrizado
- Ejemplo bueno: "Para esta receta te recomiendo la sarten Easy Release porque ahi se te despega mas facil y usas menos grasa."
- Ejemplo bueno: "Usa tu cuchillo Santoku para cortar la carne parejita y mas rapido."
- Ejemplo bueno: "Si quieres, te ayudo a que te llamen y te expliquen bien, sin compromiso."
- Ejemplo malo: "Este utensilio optimiza la distribucion termica."
- Ejemplo malo: "Te recomiendo agendar una llamada informativa con un representante especializado."

OBJETIVO PRINCIPAL:
- Ayudar gratis a cocinar saludable y usar bien los productos
- Recomendar recetas practicas, faciles y utiles para la vida diaria
- Mencionar el producto exacto que conviene usar y explicar para que sirve de forma simple
- Cuando detectes interes real por productos o precios, invitar con suavidad a una llamada informativa
- Despues de que acepten la llamada, captar la mayor parte de los datos en un solo mensaje breve

REGLAS DE RESPUESTA:
- Maximo 3 oraciones
- Espanol claro, calido, sencillo y seguro
- Si hablan de recetas o ingredientes, responde primero como chef
- Da respuestas practicas, no discursos largos
- Si recomiendas un producto, di por que les ayuda en esa receta o situacion
- Evita demasiada informacion en un solo mensaje
- Nunca hagas sentir presion

PRECIOS:
- Nunca des precio total exacto
- Solo da rangos aproximados por dia usando el catalogo y estas reglas internas:
  tax 10%
  envio 5%
  mensual = precio mas tax mas envio * 5%
  diario = mensual / 30
- Nunca expliques la matematica
- Si preguntan precio, responde facil de entender y cierra invitando a una llamada informativa sin compromiso

VENTAS:
- Primero llamada informativa, despues cita informativa
- Si el usuario acepta la llamada, pide en un solo mensaje breve la mayoria de los datos utiles
- Datos vitales: nombre y telefono
- Datos de calificacion: direccion o ciudad, si ya es cliente, si tiene productos, cuales tiene, si necesita garantia, para cuantas personas cocina y a que se dedica si lo quiere compartir
- Si despues de ese mensaje aun falta nombre o telefono, pide solo el dato vital faltante
- Si aun no aceptan llamada, no pidas toda la ficha completa
- Cuando invites a llamada, hazlo suave y natural
- Mejor di: "Si quieres, te puedo ayudar a que te llamen y te expliquen bien."
- Evita sonar agresivo o insistente

COCINA:
- Guias a las personas para cocinar saludable y usar Royal Prestige con confianza
- Prioriza recetas practicas, saludables y faciles de replicar
- Cuando sea util, conecta la receta con beneficios simples como menos grasa, mejor coccion, practicidad y facilidad
- Explica como usar el producto sin complicar a la persona

REAL ESTATE:
rent_ratio = renta / precio
cashflow = (renta * 12) - (precio * 0.1)

>1% excelente
0.8-1% bueno
<0.8% debil

IMPORTANTE:
Tienes acceso a multiples bases de conocimiento.
Usa SOLO la informacion necesaria segun la pregunta.
No repitas informacion innecesaria.
- No inventes productos ni beneficios fuera del contexto disponible.
- Si ya conoces el historial de la persona, usalo para personalizar recetas, seguimiento y recomendaciones de producto.
- Siempre prioriza que la persona se sienta comoda, entendida y bienvenida.
`;

const coachSystemPrompt = `
Eres Agustin 2.0 Coach, el copiloto privado para distribuidores. Ayudas a vender mejor, responder objeciones, sacar precios y pagos, armar paquetes, negociar y cerrar con mas claridad.

VOZ Y TONO:
- Habla claro, directo, calido y util
- Usa palabras simples, de uso diario, faciles de leer rapido
- Suena como un coach de ventas paciente, no como maestro, call center o vendedor agresivo
- Da respuestas cortas que el distribuidor pueda usar de inmediato
- Di mucho en poco
- No metas relleno
- Si algo se puede decir mas simple, dilo mas simple
- Piensa en distribuidores latinos que trabajan duro, leen rapido y necesitan ayuda al momento

OBJETIVO PRINCIPAL:
- Ayudar al distribuidor a contestar objeciones y cerrar mejor
- Sacar precios, pagos mensuales y paquetes sin enredarse
- Dar apoyo para negociar con mas claridad y menos vueltas
- Aterrizar el conocimiento del producto en palabras faciles
- Convertir informacion dispersa en respuestas practicas

PRECIOS Y PAGOS DEL COACH:
- Cuando el distribuidor te diga un numero como "1000" o "2500", toma ese numero como precio base de catalogo
- Formula interna:
  tax = base * 10%
  envio = base * 5%
  total = base + tax + envio
  mensualidad = total * 5%
- Si hay varios productos para paquete, suma primero todas las bases y luego aplica la formula al total base
- Ejemplo interno:
  base 2500
  tax 250
  envio 125
  total 2875
  mensualidad 143.75
- No expliques esta formula a menos que el distribuidor te la pida
- Si solo te piden el pago, da el pago
- Si te piden total y pago, da ambos
- Si ocupan negociar, usa los numeros para mover al siguiente paso sin dar discurso largo

REGLAS DE RESPUESTA:
- Prioriza claridad y utilidad inmediata
- Respuesta normal: 2 bloques cortos
  1. Di esto:
  2. Siguiente:
- Maximo 4 lineas cortas en la mayoria de los casos
- No expliques "por que funciona" a menos que el distribuidor te lo pida
- No des teoria si el momento pide accion
- Si la pregunta es una objecion, da una frase util y luego el siguiente paso
- Si la pregunta es sobre producto, explica como presentarlo facil y que preguntar despues
- Si la pregunta es sobre precio o pago, da el numero claro y luego el siguiente paso mas util para vender
- Si la pregunta es sobre seguimiento, da un guion corto o el proximo paso mas fuerte
- Si la pregunta es sobre reclutamiento, habla claro y sin exagerar
- Si ya sabes en que momento de la demo va, usa ese contexto para dar la accion que sigue
- Si el mejor movimiento es cerrar, cierra
- Si el mejor movimiento es callar y amarrar, dilo sin rodeos
- Si el cliente ya escucho todo, ya entendio el pago y se queda callado, trata ese momento como cierre final
- En ese momento no sigas explicando; manda al distribuidor a asumir la venta con la frase correcta
- Si usas un cierre, da solo el mejor para ese momento
- Si das ejemplo, da solo uno y pegado al contexto del chat
- No des listas largas de opciones
- No des varios cierres a la vez salvo que te los pidan
- No pidas nombre, telefono ni datos de lead
- No trates al distribuidor como cliente final
- No invites a una llamada informativa como lo haria el Chef
- No hables como recetario a menos que el distribuidor este preparando una demo o ejemplo para cliente

FORMATO IDEAL:
- Di esto: "frase corta que pueda repetir ya"
- Siguiente: "paso breve y claro"

CUANDO SEA UTIL:
- Usa cierres cortos tipo doble alternativa, amarre, Benjamin Franklin, rebote o silencio
- Ejemplo bueno:
  Di esto: "Claro, tomelo con calma. ?Que ocupa, 3 dias o 15 para pensarlo?"
  Siguiente: "Cuando escoja tiempo, siga llenando la papeleria sin explicar de mas."
- Ejemplo bueno:
  Di esto: "?Que le queda mejor, sabado o domingo?"
  Siguiente: "No abras mas opciones. Deja solo esas dos."
- Ejemplo bueno:
  Di esto: "Bienvenido a Royal Prestige, me facilita su ID."
  Siguiente: "Si te da el ID, sigue con la orden. Si te objeta, esa ya es la objecion real."

ENFOQUE:
- El usuario aqui es distribuidor, no prospecto
- Piensa como coach de ventas, no como chef publico
- Puedes usar recetas o producto solo si ayudan a vender, demostrar o explicar mejor
- Si la informacion privada todavia no esta conectada, responde con lo mejor que haya en entrenamiento, producto y seguimiento
- Nunca reveles que esta area es publica o abierta; esta es una herramienta privada del distribuidor

IMPORTANTE:
Tienes acceso a multiples bases de conocimiento.
Usa primero lo mas relevante para ventas, objeciones, demos, producto y seguimiento.
No inventes beneficios ni politicas fuera del contexto disponible.
`;

const coachCierreFinalInterno = `
CIERRE FINAL INTERNO DEL COACH:
- Este es el cierre final real cuando ya se presento todo, ya se hablo del pago y el cliente se queda callado
- En ese momento no conviene seguir explicando
- El movimiento correcto es asumir la venta para descubrir si ya esta listo o si todavia trae una objecion escondida
- El representante se levanta, se acerca, extiende la mano con una sonrisa y dice:
  "Bienvenido a Royal Prestige, me facilita su ID"
- Si el cliente entrega el ID, ya no expliques de mas: sigue con la orden, comprobante de domicilio y deposito
- Si el cliente no esta listo, ahi mismo va a sacar la objecion real y esa se rebate
- Cuando el silencio ya es de cierre, no mandes al distribuidor a seguir hablando ni a volver a presentar
- Usa este cierre solo cuando el cliente ya escucho todo, ya se hablo del plan y solo quedo el silencio final
`;

// =============================
// FUNCION INTELIGENTE (FILTRA DATA)
// =============================
function normalizarModoChat(mode = "") {
  return String(mode || "").trim().toLowerCase() === "coach" ? "coach" : "chef";
}

function construirContextoEstaticoChef(pregunta) {
  const preguntaNormalizada = pregunta.toLowerCase();
  let contexto = `
CATALOGO:
${JSON.stringify(preciosCatalogo)}

CARACTERISTICAS:
${JSON.stringify(beneficiosProductos)}

ENCUESTA:
${JSON.stringify(encuestaVentas)}
`;

  if (
    /receta|cocinar|pollo|carne|res|pescado|salmon|huevo|pancake|hotcake|panqueque|sopa|arroz|pasta|verdura|ensalada|desayuno|comida|cena/i.test(
      preguntaNormalizada
    )
  ) {
    contexto += `\nRECETAS:\n${JSON.stringify(recetasRoyalPrestige)}`;
  }

  if (
    preguntaNormalizada.includes("garantia") ||
    preguntaNormalizada.includes("material") ||
    /olla|sarten|cuchillo|santoku|easy release|paellera|vaporera|royal prestige/i.test(
      preguntaNormalizada
    )
  ) {
    contexto += `\nESPECIFICACIONES:\n${JSON.stringify(especificacionesRoyalPrestige)}`;
  }

  if (
    preguntaNormalizada.includes("venta") ||
    preguntaNormalizada.includes("cerrar") ||
    detectarInteresComercial(pregunta)
  ) {
    contexto += `\nVENTAS:\n${JSON.stringify(inteligenciaVentas)}`;
    contexto += `\nDEMO:\n${JSON.stringify(demoVenta)}`;
    contexto += `\nCIERRES:\n${JSON.stringify(cierresAlexDey)}`;
  }

  if (preguntaNormalizada.includes("equipo") || preguntaNormalizada.includes("reclutar")) {
    contexto += `\nRECLUTAMIENTO:\n${JSON.stringify(reclutamientoCiprian)}`;
  }

  if (preguntaNormalizada.includes("casa") || preguntaNormalizada.includes("inversion")) {
    contexto += `\nPROPIEDADES:\n${JSON.stringify(redfinProperties)}`;
  }

  return contexto;
}

function construirContextoEstaticoCoach(pregunta) {
  const preguntaNormalizada = pregunta.toLowerCase();
  const contextoBase = [];
  const temaCoach = detectarTemaCoach(preguntaNormalizada);

  contextoBase.push(`
GUIA DEL COACH:
- Si es objecion o cierre, prioriza Alex Dey
- Si es frustracion, actitud o liderazgo personal, prioriza Adrian Olmedo
- Si es seguimiento despues de demo, prioriza Sistema 4 Citas en 14 Dias
- Si es reclutamiento o retencion, prioriza Jose Miguel Ciprian
- Si es demo o presentacion, prioriza Alejandro Jaramillo
- Si es precio, usa lista de precios y pagos
- Para sacar pagos del Coach, usa la formula interna de base + 10% tax + 5% envio; mensualidad = total * 5%
- Si es producto, usa caracteristicas y especificaciones
`);

  if (temaCoach.precio) {
    contextoBase.push(`
FORMULA INTERNA DE PRECIOS DEL COACH:
- numero recibido = precio base de catalogo
- tax = base * 10%
- envio = base * 5%
- total = base + tax + envio
- mensualidad = total * 5%
- si es paquete, suma primero las bases y luego aplica la formula al total base
- no expliques la formula salvo que te la pidan

PRECIOS:
${JSON.stringify(preciosCatalogo)}

PAGOS:
${JSON.stringify(opcionesPagoRoyalPrestige)}

CALCULOS DETECTADOS:
${construirCalculosPreciosCoach(pregunta)}
`);
  }

  if (temaCoach.producto || temaCoach.demo) {
    contextoBase.push(`
PRODUCTO Y BENEFICIOS:
${JSON.stringify(beneficiosProductos)}

ESPECIFICACIONES:
${JSON.stringify(especificacionesRoyalPrestige)}
`);
  }

  if (temaCoach.demo) {
    contextoBase.push(`
DEMO:
${JSON.stringify(demoVenta)}
`);
  }

  if (temaCoach.objecion || temaCoach.cierre) {
    contextoBase.push(`
CIERRES:
${JSON.stringify(cierresAlexDey)}
`);
  }

  if (temaCoach.cierreFinal) {
    contextoBase.push(coachCierreFinalInterno);
  }

  if (temaCoach.mentalidad) {
    contextoBase.push(`
MENTALIDAD:
${JSON.stringify(mentalidadOlmedo)}
`);
  }

  if (temaCoach.seguimiento) {
    contextoBase.push(`
SEGUIMIENTO:
${JSON.stringify(sistema4Citas)}
`);
  }

  if (temaCoach.reclutamiento) {
    contextoBase.push(`
RECLUTAMIENTO:
${JSON.stringify(reclutamientoCiprian)}
`);
  }

  const experienciaReal = construirExperienciaRealCoach(pregunta);

  if (experienciaReal) {
    contextoBase.push(experienciaReal);
  }

  if (
    !temaCoach.precio &&
    !temaCoach.producto &&
    !temaCoach.demo &&
    !temaCoach.objecion &&
    !temaCoach.cierre &&
    !temaCoach.mentalidad &&
    !temaCoach.seguimiento &&
    !temaCoach.reclutamiento
  ) {
    contextoBase.push(`
BASE GENERAL COACH:
${JSON.stringify(beneficiosProductos)}

CIERRES:
${JSON.stringify(cierresAlexDey)}

MENTALIDAD:
${JSON.stringify(mentalidadOlmedo)}
`);
  }

  return contextoBase.join("\n\n");
}

function detectarTemaCoach(preguntaNormalizada = "") {
  return {
    precio: /precio|precios|cu[aá]nto|cuesta|plan|planes|mensual|diario|financiamiento|pago|pagos|mensualidad|paquete|paquetes|combo|combos|negociar|negociacion|matematica|matematicas|catalogo|total/i.test(
      preguntaNormalizada
    ),
    producto: /producto|extractor|olla|ollas|sarten|cuchillo|santoku|easy release|paellera|vaporera|garantia|material/i.test(
      preguntaNormalizada
    ),
    demo: /demo|demostraci[oó]n|presentaci[oó]n|mostrar|explicar|presentar/i.test(
      preguntaNormalizada
    ),
    objecion: /objeci[oó]n|esta caro|caro|muy caro|no me alcanza|no tengo dinero|lo voy a pensar|no tengo tiempo|no estoy segura|no estoy seguro|no me interesa|ya tengo|ya compr[eé]|despu[eé]s/i.test(
      preguntaNormalizada
    ),
    cierre: /cerrar|cierre|amarre|benjamin franklin|doble alternativa|puercoesp[ií]n|rebote|silencio/i.test(
      preguntaNormalizada
    ),
    cierreFinal: /se queda callado|se queda en silencio|se quedo callado|se quedo en silencio|no responde|no me responde|silencio final|momento final|final de la demo|me facilita su id|id|comprobante de domicilio|deposito|orden|asumo la venta/i.test(
      preguntaNormalizada
    ),
    mentalidad: /mentalidad|frustrad|desanimad|disciplina|constancia|miedo|seguridad|liderazgo|confianza|actitud/i.test(
      preguntaNormalizada
    ),
    seguimiento: /seguimiento|despu[eé]s de la demo|despu[eé]s de la cita|referid|4 citas|14 d[ií]as|llamar luego|volver a llamar|pr[oó]ximo paso/i.test(
      preguntaNormalizada
    ),
    reclutamiento: /reclut|equipo|distribuidor|lider|liderazgo de equipo|retener|conservar gente|entrevista|candidato/i.test(
      preguntaNormalizada
    )
  };
}

function normalizarTextoBusquedaCoach(value = "") {
  return String(value || "")
    .toLowerCase()
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, " ");
}

function extraerPalabrasClaveCoach(pregunta = "") {
  const base = normalizarTextoBusquedaCoach(pregunta)
    .split(/[^a-z0-9ñ]+/i)
    .map(word => word.trim())
    .filter(word => word.length >= 4);

  const claves = new Set(base);

  if (/caro|precio|dinero|cuota|pago/i.test(pregunta)) {
    ["caro", "interesada", "interesado", "no esta interesada", "no esta interesado"].forEach(item =>
      claves.add(item)
    );
  }

  if (/ya tengo|ya tiene|cliente|productos/i.test(pregunta)) {
    ["cliente", "tiene productos", "sus productos estan bien"].forEach(item => claves.add(item));
  }

  if (/llamar|seguimiento|despues|ocupad/i.test(pregunta)) {
    ["llamar", "ocupada", "ocupado", "llamar luego", "llamar siguiente"].forEach(item => claves.add(item));
  }

  if (/regalo|demo|cita|recibir/i.test(pregunta)) {
    ["regalo", "recibir", "cita", "visita"].forEach(item => claves.add(item));
  }

  return Array.from(claves);
}

function redondearMonedaCoach(valor) {
  return Math.round((Number(valor) + Number.EPSILON) * 100) / 100;
}

function extraerMontosCoach(pregunta = "") {
  const coincidencias = String(pregunta || "").match(/\$?\d[\d,]*(?:\.\d+)?/g) || [];
  const montos = coincidencias
    .map(item => Number(item.replace(/[$,]/g, "")))
    .filter(numero => Number.isFinite(numero) && numero >= 100);

  return Array.from(new Set(montos));
}

function calcularPagoCoach(base = 0) {
  const tax = redondearMonedaCoach(base * 0.1);
  const envio = redondearMonedaCoach(base * 0.05);
  const total = redondearMonedaCoach(base + tax + envio);
  const mensualidad = redondearMonedaCoach(total * 0.05);

  return { base, tax, envio, total, mensualidad };
}

function construirCalculosPreciosCoach(pregunta = "") {
  const montos = extraerMontosCoach(pregunta);

  if (!montos.length) {
    return "- Sin monto directo en la pregunta. Si el distribuidor te dice un precio base, aplica la formula interna.";
  }

  const lineas = montos.map(base => {
    const calculo = calcularPagoCoach(base);
    return `- Base ${calculo.base}: tax ${calculo.tax}, envio ${calculo.envio}, total ${calculo.total}, mensualidad ${calculo.mensualidad}`;
  });

  if (montos.length > 1) {
    const basePaquete = redondearMonedaCoach(montos.reduce((acc, monto) => acc + monto, 0));
    const paquete = calcularPagoCoach(basePaquete);
    lineas.push(
      `- Paquete total base ${paquete.base}: tax ${paquete.tax}, envio ${paquete.envio}, total ${paquete.total}, mensualidad ${paquete.mensualidad}`
    );
  }

  return lineas.join("\n");
}

function construirExperienciaRealCoach(pregunta = "") {
  if (!Array.isArray(inteligenciaVentas) || !inteligenciaVentas.length) {
    return "";
  }

  const palabrasClave = extraerPalabrasClaveCoach(pregunta);

  if (!palabrasClave.length) {
    return "";
  }

  const coincidencias = inteligenciaVentas
    .map(registro => {
      const corpus = normalizarTextoBusquedaCoach(
        [
          registro?.nombre,
          registro?.observaciones_vendedor,
          registro?.comentario_telemarketing,
          registro?.resultado_cita
        ]
          .filter(Boolean)
          .join(" ")
      );

      let score = 0;
      for (const palabra of palabrasClave) {
        if (corpus.includes(palabra)) {
          score += palabra.includes(" ") ? 3 : 1;
        }
      }

      return {
        registro,
        score
      };
    })
    .filter(item => item.score > 0)
    .sort((a, b) => b.score - a.score)
    .slice(0, 5)
    .map(item => ({
      nombre: item.registro?.nombre || "Cliente",
      ciudad_zip: item.registro?.ciudad_zip || "",
      observaciones_vendedor: item.registro?.observaciones_vendedor || "",
      comentario_telemarketing: item.registro?.comentario_telemarketing || "",
      resultado_cita: item.registro?.resultado_cita || ""
    }));

  if (!coincidencias.length) {
    return "";
  }

  return `
EXPERIENCIA REAL DE CLIENTES Y TELEMARKETING:
Usa estas notas reales solo como referencia practica para detectar patrones de objecion, seguimiento y reaccion del cliente.
${JSON.stringify(coincidencias)}
`;
}

function inferirTiposFuentePorModo(pregunta, modo = "chef") {
  const tipos = new Set(inferirTiposFuentePorPregunta(pregunta));

  if (modo === "coach") {
    tipos.delete("real_estate");
    tipos.delete("general");
    tipos.add("sales_training");
  }

  if (!tipos.size) {
    tipos.add(modo === "coach" ? "sales_training" : "general");
  }

  return Array.from(tipos);
}

async function construirContexto(pregunta, modo = "chef") {
  const contextoEstatico =
    modo === "coach" ? construirContextoEstaticoCoach(pregunta) : construirContextoEstaticoChef(pregunta);

  if (!ENABLE_VECTOR_SEARCH) {
    return contextoEstatico;
  }

  const sourceTypes = inferirTiposFuentePorModo(pregunta, modo);
  const matches = await buscarKnowledgeVectorial({
    mongoose,
    question: pregunta,
    sourceTypes,
    logger: console
  });

  if (!matches.length) {
    return contextoEstatico;
  }

  return construirContextoVectorial(matches);
}

function construirPromptModo(modo = "chef") {
  return modo === "coach" ? coachSystemPrompt : chefSystemPrompt;
}

function construirContextoModoPrompt(modo = "chef", coachUser = null) {
  if (modo !== "coach") {
    return `
MODO ACTIVO:
- modo: chef
- tipo_usuario: cliente_o_prospecto
- acceso_privado: no
`;
  }

  return `
MODO ACTIVO:
- modo: coach
- tipo_usuario: distribuidor
- acceso_privado: si
- nombre_distribuidor: ${coachUser?.name || "desconocido"}
- correo_distribuidor: ${coachUser?.email || "desconocido"}
- suscripcion: ${coachUser?.subscriptionStatus || "desconocida"}

INSTRUCCION:
Responde como Coach privado de ventas. No trates a este usuario como lead ni como cliente final.
`;
}

// =============================
// COACH: AUTH, STRIPE Y RUTAS PRIVADAS
// =============================
app.post("/api/coach/signup-checkout", async (req, res) => {
  if (!stripeListoParaCheckout()) {
    return responderCoachError(
      res,
      503,
      "Stripe todavia no esta configurado. Falta conectar las claves del Coach."
    );
  }

  const name = String(req.body?.name || "").trim();
  const email = normalizarEmail(req.body?.email || "");
  const password = String(req.body?.password || "");
  const plan = normalizarPlanCoach(req.body?.plan || "trial");

  if (!name) {
    return responderCoachError(res, 400, "Tu nombre es requerido.");
  }

  if (!email) {
    return responderCoachError(res, 400, "Tu correo es requerido.");
  }

  if (!passwordEsValido(password)) {
    return responderCoachError(
      res,
      400,
      `Tu contrasena debe tener al menos ${COACH_PASSWORD_MIN} caracteres.`
    );
  }

  const existente = await CoachUser.findOne({ email });

  if (existente) {
    return responderCoachError(
      res,
      409,
      "Ese correo ya tiene cuenta. Entra por login para continuar con tu Coach."
    );
  }

  try {
    const passwordSeguro = crearPasswordSeguro(password);
    const accesoDePrueba = coachTieneAccesoDePrueba(email);
    let userDoc = await CoachUser.create({
      name,
      email,
      passwordHash: passwordSeguro.hash,
      passwordSalt: passwordSeguro.salt,
      subscriptionStatus: accesoDePrueba ? "test_access" : "inactive",
      subscriptionActive: false,
      updatedAt: new Date()
    });

    await crearCoachSesion(req, res, userDoc._id);

    if (accesoDePrueba) {
      return res.json({
        url: "/coach/app/",
        bypass: true,
        user: limpiarCoachUser(userDoc)
      });
    }

    userDoc = await asegurarCoachCustomer(userDoc);

    const { selectedPlan, checkoutConfig } = construirCheckoutCoach(req, userDoc, plan);
    const checkoutSession = await stripe.checkout.sessions.create(checkoutConfig);

    userDoc.lastCheckoutSessionId = checkoutSession.id;
    userDoc.updatedAt = new Date();
    await userDoc.save();

    res.json({
      url: checkoutSession.url,
      plan: selectedPlan.code,
      user: limpiarCoachUser(userDoc)
    });
  } catch (error) {
    console.error("Error creando signup checkout Coach:", error.message);
    responderCoachError(res, 500, error.message || "No pude crear tu suscripcion en este momento.");
  }
});

app.post("/api/coach/login", async (req, res) => {
  const email = normalizarEmail(req.body?.email || "");
  const password = String(req.body?.password || "");

  if (!email || !password) {
    return responderCoachError(res, 400, "Correo y contrasena son requeridos.");
  }

  try {
    const userDoc = await CoachUser.findOne({ email });

    if (!userDoc || !verificarPasswordSeguro(password, userDoc.passwordSalt, userDoc.passwordHash)) {
      return responderCoachError(res, 401, "Correo o contrasena incorrectos.");
    }

    userDoc.lastLoginAt = new Date();
    userDoc.updatedAt = new Date();
    await userDoc.save();
    await crearCoachSesion(req, res, userDoc._id);

    res.json({ user: limpiarCoachUser(userDoc) });
  } catch (error) {
    console.error("Error login Coach:", error.message);
    responderCoachError(res, 500, "No pude iniciar sesion en este momento.");
  }
});

app.post("/api/coach/logout", async (req, res) => {
  try {
    await destruirCoachSesion(req, res);
    res.json({ ok: true });
  } catch (error) {
    console.error("Error logout Coach:", error.message);
    responderCoachError(res, 500, "No pude cerrar la sesion.");
  }
});

app.get("/api/coach/me", async (req, res) => {
  try {
    const auth = await obtenerCoachAuth(req);

    if (!auth.user) {
      return res.json({
        authenticated: false,
        stripeReady: stripeListoParaCheckout()
      });
    }

    res.json({
      authenticated: true,
      stripeReady: stripeListoParaCheckout(),
      user: limpiarCoachUser(auth.user)
    });
  } catch (error) {
    console.error("Error obteniendo usuario Coach:", error.message);
    responderCoachError(res, 500, "No pude revisar tu cuenta.");
  }
});

app.post("/api/coach/create-checkout-session", async (req, res) => {
  if (!stripeListoParaCheckout()) {
    return responderCoachError(
      res,
      503,
      "Stripe todavia no esta configurado. Falta conectar las claves del Coach."
    );
  }

  const auth = await requireCoachUser(req, res);
  const plan = normalizarPlanCoach(req.body?.plan || "monthly");

  if (!auth) {
    return;
  }

  if (coachTieneAccesoTotal(auth.user)) {
    return responderCoachError(
      res,
      409,
      coachTieneAccesoDePrueba(auth.user.email)
        ? "Tu cuenta de prueba ya puede entrar al Coach sin pagar."
        : "Tu cuenta ya tiene una suscripcion activa. Entra al Coach o abre el portal de facturacion."
    );
  }

  try {
    const userDoc = await asegurarCoachCustomer(auth.user);
    const { selectedPlan, checkoutConfig } = construirCheckoutCoach(req, userDoc, plan);
    const checkoutSession = await stripe.checkout.sessions.create(checkoutConfig);

    userDoc.lastCheckoutSessionId = checkoutSession.id;
    userDoc.updatedAt = new Date();
    await userDoc.save();

    res.json({ url: checkoutSession.url, plan: selectedPlan.code });
  } catch (error) {
    console.error("Error creando checkout Coach:", error.message);
    responderCoachError(res, 500, error.message || "No pude abrir el pago del Coach.");
  }
});

app.post("/api/coach/create-portal-session", async (req, res) => {
  if (!stripe) {
    return responderCoachError(
      res,
      503,
      "Stripe todavia no esta configurado. Falta conectar las claves del Coach."
    );
  }

  const auth = await requireCoachActivo(req, res);

  if (!auth) {
    return;
  }

  if (coachTieneAccesoDePrueba(auth.user.email) && !auth.user.stripeCustomerId) {
    return responderCoachError(
      res,
      400,
      "Tu cuenta de prueba no necesita portal de pago. Ya puedes entrar y revisar el Coach."
    );
  }

  if (!auth.user.stripeCustomerId) {
    return responderCoachError(
      res,
      400,
      "Tu cuenta aun no tiene cliente de Stripe conectado."
    );
  }

  try {
    const portalSession = await stripe.billingPortal.sessions.create({
      customer: auth.user.stripeCustomerId,
      return_url: `${obtenerBaseUrl(req)}/coach/app/`
    });

    res.json({ url: portalSession.url });
  } catch (error) {
    console.error("Error creando portal Coach:", error.message);
    responderCoachError(res, 500, "No pude abrir tu portal de suscripcion.");
  }
});

app.get("/api/coach/checkout-session", async (req, res) => {
  if (!stripe) {
    return responderCoachError(
      res,
      503,
      "Stripe todavia no esta configurado. Falta conectar las claves del Coach."
    );
  }

  const auth = await requireCoachUser(req, res);

  if (!auth) {
    return;
  }

  const sessionId = String(req.query?.session_id || "").trim();

  if (!sessionId) {
    return responderCoachError(res, 400, "session_id requerido.");
  }

  try {
    const checkoutSession = await stripe.checkout.sessions.retrieve(sessionId);
    const coachUserId =
      checkoutSession.metadata?.coachUserId || checkoutSession.client_reference_id || "";
    const sameUser =
      coachUserId === String(auth.user._id) ||
      checkoutSession.customer === auth.user.stripeCustomerId ||
      normalizarEmail(checkoutSession.customer_email || "") === auth.user.email;

    if (!sameUser) {
      return responderCoachError(res, 403, "Esa sesion no pertenece a tu cuenta.");
    }

    const userActualizado = await sincronizarCoachDesdeCheckoutSession(sessionId);

    res.json({
      user: limpiarCoachUser(userActualizado || auth.user),
      session: {
        id: checkoutSession.id,
        status: checkoutSession.status,
        paymentStatus: checkoutSession.payment_status
      }
    });
  } catch (error) {
    console.error("Error revisando checkout Coach:", error.message);
    responderCoachError(res, 500, "No pude revisar tu pago todavia.");
  }
});

app.get(["/coach/app", "/coach/app/"], async (req, res) => {
  const auth = await obtenerCoachAuth(req);

  if (!auth.user) {
    return res.redirect("/coach/login/");
  }

  if (!coachTieneAccesoTotal(auth.user)) {
    return res.redirect("/coach/planes/");
  }

  res.sendFile(path.join(PRIVATE_DIR, "coach-app.html"));
});

app.use(express.static(PUBLIC_DIR));

// =============================
// CHAT
// =============================
app.post("/chat", async (req, res) => {
  const { pregunta, sessionId, visitorId, mode } = req.body;
  const modoChat = normalizarModoChat(mode);
  const preguntaLimpia = typeof pregunta === "string" ? pregunta.trim() : "";
  const visitorIdLimpio =
    typeof visitorId === "string" && visitorId.trim() ? visitorId.trim() : sessionId;
  let coachAuth = null;

  if (!preguntaLimpia) {
    return res.status(400).json({ error: "pregunta requerida" });
  }

  if (!sessionId) {
    return res.status(400).json({ error: "sessionId requerido" });
  }

  if (modoChat === "coach") {
    coachAuth = await requireCoachActivo(req, res);

    if (!coachAuth) {
      return;
    }
  }

  if (!conversaciones[sessionId]) {
    conversaciones[sessionId] = [];
  }

  conversaciones[sessionId].push({
    role: "user",
    content: preguntaLimpia
  });

  try {
    let leadGuardado = null;
    let profileGuardado = null;
    const modoPrompt = construirContextoModoPrompt(modoChat, coachAuth?.user);
    let estadoPrompt = "";
    let perfilPrompt = "";

    if (modoChat === "coach") {
      estadoPrompt = `
ESTADO DEL COACH:
- area_privada: si
- tipo_ayuda: objeciones, cierre, precios, pagos, paquetes, negociacion
`;
      perfilPrompt = `
CONTEXTO INTERNO DEL COACH:
- esta conversacion pertenece al area privada del distribuidor
- no capturar lead
- no pedir telefono
- no mandar informacion a Google Sheets
- enfocate en objeciones, seguimiento, demo, cierre, reclutamiento y estrategia
`;
    } else {
      actualizarEstadoConversacion(sessionId, preguntaLimpia);
      const estadoActual = obtenerEstadoConversacion(sessionId);

      leadGuardado = await guardarLeadSiExiste(
        preguntaLimpia,
        sessionId,
        visitorIdLimpio,
        estadoActual
      );
      const estadoConLead = actualizarEstadoConversacion(sessionId, preguntaLimpia, leadGuardado);
      profileGuardado = await guardarOActualizarPerfil({
        visitorId: visitorIdLimpio,
        sessionId,
        texto: preguntaLimpia,
        estadoConversacion: estadoConLead,
        leadGuardado
      });
      await guardarMensajeRaw({
        visitorId: visitorIdLimpio,
        sessionId,
        profileId: profileGuardado?._id || null,
        leadId: leadGuardado?._id || null,
        role: "user",
        content: preguntaLimpia,
        estadoConversacion: estadoConLead
      });

      estadoPrompt = construirEstadoPrompt(sessionId);
      perfilPrompt = construirPerfilHistoricoPrompt(profileGuardado, leadGuardado);
    }

    const contexto = await construirContexto(preguntaLimpia, modoChat);

    const response = await fetch("https://api.openai.com/v1/chat/completions", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Authorization: `Bearer ${process.env.OPENAI_API_KEY}`
      },
      body: JSON.stringify({
        model: "gpt-5-mini",
        messages: [
          { role: "system", content: construirPromptModo(modoChat) },
          { role: "system", content: modoPrompt },
          { role: "system", content: contexto },
          { role: "system", content: estadoPrompt },
          { role: "system", content: perfilPrompt },
          ...conversaciones[sessionId]
        ]
      })
    });

    const data = await response.json().catch(() => null);

    if (!response.ok) {
      return res.status(response.status).json({
        error: "Error OpenAI",
        detalle: data || { message: "Respuesta invalida del API" }
      });
    }

    if (!data?.choices?.[0]?.message) {
      return res.status(500).json({ error: "Error OpenAI", detalle: data });
    }

    const respuestaIA = data.choices[0].message;

    conversaciones[sessionId].push(respuestaIA);

    if (modoChat === "coach") {
      return res.json({
        respuesta: respuestaIA.content,
        mode: modoChat
      });
    }

    const leadFinal = await guardarRespuestaIAEnPerfil(leadGuardado, respuestaIA.content);
    const profileFinal = await guardarRespuestaIAEnProfile(
      profileGuardado,
      respuestaIA.content,
      leadFinal
    );

    await guardarMensajeRaw({
      visitorId: visitorIdLimpio,
      sessionId,
      profileId: profileFinal?._id || profileGuardado?._id || null,
      leadId: leadFinal?._id || leadGuardado?._id || null,
      role: "assistant",
      content: respuestaIA.content,
      estadoConversacion: obtenerEstadoConversacion(sessionId)
    });
    await sincronizarLeadAGoogleSheets(leadFinal?.email || leadFinal?.phone ? leadFinal : null);

    res.json({
      respuesta: respuestaIA.content,
      mode: modoChat
    });
  } catch (error) {
    console.error(error);
    res.status(500).json({ error: "Error servidor" });
  }
});

// =============================
const PORT = process.env.PORT || 3000;

async function iniciarServidor() {
  if (!process.env.MONGODB_URI) {
    console.error("MONGODB_URI no configurada");
    process.exit(1);
  }

  try {
    await mongoose.connect(process.env.MONGODB_URI);
    console.log("MongoDB Atlas conectado");

    app.listen(PORT, () => {
      console.log(`Servidor corriendo en puerto ${PORT}`);
    });
  } catch (error) {
    console.error("Error conectando MongoDB Atlas:", error.message);
    process.exit(1);
  }
}

iniciarServidor();
