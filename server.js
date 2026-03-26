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
const COACH_MAX_ACTIVE_SESSIONS = Math.max(1, Number(process.env.COACH_MAX_ACTIVE_SESSIONS || 2));
const COACH_MAX_MESSAGES_PER_DAY = Math.max(1, Number(process.env.COACH_MAX_MESSAGES_PER_DAY || 100));
const CHEF_MAX_MESSAGES_PER_DAY = Math.max(1, Number(process.env.CHEF_MAX_MESSAGES_PER_DAY || 50));
const TWILIO_WHATSAPP_ENABLED = String(process.env.TWILIO_WHATSAPP_ENABLED || "").toLowerCase() === "true";
const TWILIO_WHATSAPP_WEBHOOK_TOKEN = String(process.env.TWILIO_WHATSAPP_WEBHOOK_TOKEN || "").trim();
const WHATSAPP_CHEF_NUMBER = String(process.env.WHATSAPP_CHEF_NUMBER || "").trim();
const WHATSAPP_CHEF_TEXT = String(process.env.WHATSAPP_CHEF_TEXT || "Hola, quiero ayuda con Agustin 2.0 Chef.").trim();
const CALENDLY_CHEF_URL = String(process.env.CALENDLY_CHEF_URL || "").trim();
const CALENDLY_COACH_URL = String(process.env.CALENDLY_COACH_URL || "").trim();
const MAX_PROMPT_HISTORY_MESSAGES = Math.max(4, Number(process.env.MAX_PROMPT_HISTORY_MESSAGES || 12));
const MAX_RAM_SESSION_MESSAGES = Math.max(
  MAX_PROMPT_HISTORY_MESSAGES,
  Number(process.env.MAX_RAM_SESSION_MESSAGES || 16)
);
const MAX_RAM_SESSION_STATES = Math.max(100, Number(process.env.MAX_RAM_SESSION_STATES || 500));
const RAM_SESSION_TTL_MS = Math.max(60 * 60 * 1000, Number(process.env.RAM_SESSION_TTL_MS || 6 * 60 * 60 * 1000));
const COACH_TEST_ACCESS_EMAILS = String(process.env.COACH_TEST_ACCESS_EMAILS || "")
  .split(",")
  .map(email => normalizarEmail(email))
  .filter(Boolean);
const actividadSesiones = {};
const RIFA_PROFILE_COLLECTION = "agustin_rifa_lead_profiles";
const RIFA_STATE_COLLECTION = "agustin_rifa_lead_contact_state";
const RIFA_INSIGHTS_COLLECTION = "agustin_rifa_lead_insights";

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
  bestCallDay: String,
  bestCallTime: String,
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
  bestCallDay: String,
  bestCallTime: String,
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

const coachMetricSchema = new mongoose.Schema(
  {
    label: String,
    count: { type: Number, default: 0 },
    lastSeenAt: Date
  },
  { _id: false }
);

const coachSessionSummarySchema = new mongoose.Schema(
  {
    sessionId: String,
    summary: String,
    createdAt: { type: Date, default: Date.now }
  },
  { _id: false }
);

const coachDailyRollupSchema = new mongoose.Schema(
  {
    dateKey: String,
    questions: { type: Number, default: 0 },
    replies: { type: Number, default: 0 },
    topics: [coachMetricSchema],
    objections: [coachMetricSchema],
    products: [coachMetricSchema],
    stages: [coachMetricSchema],
    closeStyles: [coachMetricSchema]
  },
  { _id: false }
);

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

const coachDistributorProfileSchema = new mongoose.Schema({
  userId: {
    type: mongoose.Schema.Types.ObjectId,
    ref: "CoachUser",
    required: true,
    unique: true,
    index: true
  },
  name: String,
  email: String,
  subscriptionStatus: String,
  questionsCount: { type: Number, default: 0 },
  coachRepliesCount: { type: Number, default: 0 },
  pricingQuestionsCount: { type: Number, default: 0 },
  followUpQuestionsCount: { type: Number, default: 0 },
  docuciteQuestionsCount: { type: Number, default: 0 },
  recruitingQuestionsCount: { type: Number, default: 0 },
  demoQuestionsCount: { type: Number, default: 0 },
  objectionQuestionsCount: { type: Number, default: 0 },
  productQuestionsCount: { type: Number, default: 0 },
  closingQuestionsCount: { type: Number, default: 0 },
  mindsetQuestionsCount: { type: Number, default: 0 },
  businessQuestionsCount: { type: Number, default: 0 },
  topTopics: [coachMetricSchema],
  topObjections: [coachMetricSchema],
  topProducts: [coachMetricSchema],
  topStages: [coachMetricSchema],
  closeStylesConsulted: [coachMetricSchema],
  focusAreas: [String],
  painAreas: [String],
  preferredCloseStyle: String,
  lastQuestion: String,
  lastCoachReply: String,
  lastInteractionAt: Date,
  recentSessionsSummary: [coachSessionSummarySchema],
  updatedAt: Date,
  createdAt: { type: Date, default: Date.now }
});

const coachDistributorAnalyticsSchema = new mongoose.Schema({
  userId: {
    type: mongoose.Schema.Types.ObjectId,
    ref: "CoachUser",
    required: true,
    unique: true,
    index: true
  },
  name: String,
  email: String,
  firstInteractionAt: Date,
  lastInteractionAt: Date,
  lastSessionId: String,
  totalQuestions: { type: Number, default: 0 },
  totalReplies: { type: Number, default: 0 },
  totalSessions: { type: Number, default: 0 },
  topTopics: [coachMetricSchema],
  topObjections: [coachMetricSchema],
  topProducts: [coachMetricSchema],
  topStages: [coachMetricSchema],
  closeStylesConsulted: [coachMetricSchema],
  dailyRollup: [coachDailyRollupSchema],
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
  ipAddress: String,
  userAgent: String,
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
messageSchema.index({ intent: 1, role: 1, createdAt: -1 });
coachUserSchema.index({ stripeCustomerId: 1 });
coachUserSchema.index({ stripeSubscriptionId: 1 });
coachDistributorProfileSchema.index({ email: 1 });
coachDistributorAnalyticsSchema.index({ email: 1 });
coachSessionSchema.index({ expiresAt: 1 }, { expireAfterSeconds: 0 });

const Lead = mongoose.models.Lead || mongoose.model("Lead", leadSchema);
const Profile = mongoose.models.Profile || mongoose.model("Profile", profileSchema);
const Message = mongoose.models.Message || mongoose.model("Message", messageSchema);
const CoachUser = mongoose.models.CoachUser || mongoose.model("CoachUser", coachUserSchema);
const CoachDistributorProfile =
  mongoose.models.CoachDistributorProfile || mongoose.model("CoachDistributorProfile", coachDistributorProfileSchema);
const CoachDistributorAnalytics =
  mongoose.models.CoachDistributorAnalytics ||
  mongoose.model("CoachDistributorAnalytics", coachDistributorAnalyticsSchema);
const CoachSession = mongoose.models.CoachSession || mongoose.model("CoachSession", coachSessionSchema);

app.post("/webhooks/stripe", express.raw({ type: "application/json" }), manejarWebhookStripe);
app.use(express.json());

function normalizarEmail(email = "") {
  return String(email || "").trim().toLowerCase();
}

function truncarTextoPrompt(value = "", maxLength = 180) {
  const limpio = String(value || "").replace(/\s+/g, " ").trim();

  if (limpio.length <= maxLength) {
    return limpio;
  }

  return `${limpio.slice(0, Math.max(maxLength - 3, 0)).trim()}...`;
}

function obtenerEtiquetaPrompt(node, fallback = "item") {
  if (!node || typeof node !== "object" || Array.isArray(node)) {
    return fallback;
  }

  const campos = ["id", "nombre", "titulo", "pregunta", "producto", "codigo", "Codigo Del Producto", "Nombre Del Producto NOVEL"];

  for (const campo of campos) {
    if (node[campo]) {
      return truncarTextoPrompt(node[campo], 80);
    }
  }

  return fallback;
}

function resumirDatoProfundoPrompt(value, options = {}) {
  const { maxArrayItems = 6, maxStringLength = 140, maxObjectKeys = 6 } = options;

  if (value === null || value === undefined) {
    return value;
  }

  if (typeof value === "string") {
    return truncarTextoPrompt(value, maxStringLength);
  }

  if (typeof value === "number" || typeof value === "boolean") {
    return value;
  }

  if (Array.isArray(value)) {
    return value.slice(0, Math.max(1, Math.min(maxArrayItems, 3))).map(item => {
      if (typeof item === "string") {
        return truncarTextoPrompt(item, maxStringLength);
      }

      return obtenerEtiquetaPrompt(item);
    });
  }

  const resultado = {};
  const entradas = Object.entries(value)
    .filter(([, item]) => item !== undefined && item !== null)
    .slice(0, maxObjectKeys);

  for (const [key, item] of entradas) {
    if (typeof item === "string") {
      resultado[key] = truncarTextoPrompt(item, maxStringLength);
      continue;
    }

    if (typeof item === "number" || typeof item === "boolean") {
      resultado[key] = item;
      continue;
    }

    if (Array.isArray(item)) {
      resultado[key] = item.slice(0, Math.max(1, Math.min(maxArrayItems, 3))).map(entry => {
        if (typeof entry === "string") {
          return truncarTextoPrompt(entry, maxStringLength);
        }

        return obtenerEtiquetaPrompt(entry);
      });
      continue;
    }

    resultado[key] = obtenerEtiquetaPrompt(item);
  }

  return resultado;
}

function compactarDatoParaPrompt(value, options = {}, depth = 0) {
  const { maxDepth = 2, maxArrayItems = 8, maxObjectKeys = 10, maxStringLength = 180 } = options;

  if (value === null || value === undefined) {
    return value;
  }

  if (typeof value === "string") {
    return truncarTextoPrompt(value, maxStringLength);
  }

  if (typeof value === "number" || typeof value === "boolean") {
    return value;
  }

  if (depth >= maxDepth) {
    return resumirDatoProfundoPrompt(value, options);
  }

  if (Array.isArray(value)) {
    return value.slice(0, maxArrayItems).map(item => compactarDatoParaPrompt(item, options, depth + 1));
  }

  const entradas = Object.entries(value)
    .filter(([, item]) => item !== undefined)
    .sort(([a], [b]) => {
      const prioridades = ["metadata", "id", "nombre", "titulo", "pregunta", "descripcion", "respuesta_corta"];
      const indexA = prioridades.indexOf(a);
      const indexB = prioridades.indexOf(b);

      if (indexA === -1 && indexB === -1) {
        return 0;
      }

      if (indexA === -1) {
        return 1;
      }

      if (indexB === -1) {
        return -1;
      }

      return indexA - indexB;
    })
    .slice(0, maxObjectKeys);

  return Object.fromEntries(
    entradas.map(([key, item]) => [key, compactarDatoParaPrompt(item, options, depth + 1)])
  );
}

function construirBloquePrompt(label, data, options = {}) {
  return `\n${label}:\n${JSON.stringify(compactarDatoParaPrompt(data, options))}`;
}

function crearEstadoConversacionBase() {
  return {
    interesComercial: false,
    consultaPrecio: false,
    llamadaInformativaAceptada: false,
    envioDatosContactoReciente: false,
    quiereLlamada: "",
    name: "",
    email: "",
    phone: "",
    bestCallDay: "",
    bestCallTime: "",
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

function marcarSesionActiva(sessionId = "") {
  if (!sessionId) {
    return;
  }

  actividadSesiones[sessionId] = Date.now();
}

function limpiarMemoriaSesiones() {
  const ahora = Date.now();
  const sesiones = Object.entries(actividadSesiones);

  for (const [sessionId, lastSeenAt] of sesiones) {
    if (ahora - Number(lastSeenAt || 0) > RAM_SESSION_TTL_MS) {
      delete actividadSesiones[sessionId];
      delete conversaciones[sessionId];
      delete estadosConversacion[sessionId];
    }
  }

  const activas = Object.entries(actividadSesiones).sort((a, b) => Number(b[1]) - Number(a[1]));

  if (activas.length <= MAX_RAM_SESSION_STATES) {
    return;
  }

  for (const [sessionId] of activas.slice(MAX_RAM_SESSION_STATES)) {
    delete actividadSesiones[sessionId];
    delete conversaciones[sessionId];
    delete estadosConversacion[sessionId];
  }
}

function registrarMensajeMemoria(sessionId = "", role = "", content = "") {
  if (!sessionId || !role || !content) {
    return;
  }

  marcarSesionActiva(sessionId);
  limpiarMemoriaSesiones();

  if (!conversaciones[sessionId]) {
    conversaciones[sessionId] = [];
  }

  conversaciones[sessionId].push({ role, content });
  conversaciones[sessionId] = conversaciones[sessionId].slice(-MAX_RAM_SESSION_MESSAGES);
}

function hidratarEstadoConversacion(sessionId, profile = null, lead = null) {
  const estado = obtenerEstadoConversacion(sessionId);
  const fuente = profile || lead;

  if (!fuente) {
    return estado;
  }

  estado.name = estado.name || fuente.name || "";
  estado.email = estado.email || fuente.email || "";
  estado.phone = estado.phone || fuente.phone || "";
  estado.bestCallDay = estado.bestCallDay || fuente.bestCallDay || "";
  estado.bestCallTime = estado.bestCallTime || fuente.bestCallTime || "";
  estado.direccion = estado.direccion || fuente.direccion || "";
  estado.ocupacion = estado.ocupacion || fuente.ocupacion || "";
  estado.esCliente = estado.esCliente || fuente.esCliente || "";
  estado.tieneProductos = estado.tieneProductos || fuente.tieneProductos || "";
  estado.necesitaGarantia = estado.necesitaGarantia || fuente.necesitaGarantia || "";
  estado.quiereLlamada = estado.quiereLlamada || fuente.quiereLlamada || "";
  estado.cocinaPara = estado.cocinaPara || fuente.cocinaPara || "";
  estado.productos = combinarListas(estado.productos, fuente.productos || []);
  estado.temasInteres = combinarListas(estado.temasInteres, fuente.temasInteres || []);

  if (fuente.leadStatus === "llamada_aceptada" || fuente.quiereLlamada === "si") {
    estado.interesComercial = true;
    estado.llamadaInformativaAceptada = true;
  } else if (fuente.leadStatus && fuente.leadStatus !== "solo_soporte") {
    estado.interesComercial = true;
  }

  return estado;
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

async function obtenerChefPublicStats() {
  const ahora = new Date();
  const inicioHoy = new Date(ahora);
  inicioHoy.setHours(0, 0, 0, 0);
  const hace7Dias = new Date(ahora.getTime() - 7 * 24 * 60 * 60 * 1000);
  const baseMessageQuery = {
    intent: "chef_chat",
    role: "user"
  };

  const [familiasGuiadasIds, activosHoyIds, activos7DiasIds, preguntasTotales, perfilesChef, topTopics] =
    await Promise.all([
      Message.distinct("visitorId", baseMessageQuery),
      Message.distinct("visitorId", { ...baseMessageQuery, createdAt: { $gte: inicioHoy } }),
      Message.distinct("visitorId", { ...baseMessageQuery, createdAt: { $gte: hace7Dias } }),
      Message.countDocuments(baseMessageQuery),
      Profile.countDocuments({ conversationCount: { $gt: 0 } }),
      Message.aggregate([
        { $match: { ...baseMessageQuery, detectedTopics: { $exists: true, $ne: [] } } },
        { $unwind: "$detectedTopics" },
        { $match: { detectedTopics: { $type: "string", $ne: "" } } },
        { $group: { _id: "$detectedTopics", count: { $sum: 1 } } },
        { $sort: { count: -1 } },
        { $limit: 6 }
      ])
    ]);

  return {
    familiasGuiadas: Math.max(familiasGuiadasIds.length, perfilesChef),
    activosHoy: activosHoyIds.length,
    activos7Dias: activos7DiasIds.length,
    preguntasTotales,
    topTopics: topTopics.map(item => item._id).filter(Boolean),
    updatedAt: ahora.toISOString()
  };
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

function limpiarCoachProfile(profileDoc = null, analyticsDoc = null) {
  if (!profileDoc && !analyticsDoc) {
    return null;
  }

  const totalQuestions = profileDoc?.questionsCount || analyticsDoc?.totalQuestions || 0;
  const totalSessions = analyticsDoc?.totalSessions || 0;
  let level = "novato";

  if (totalQuestions >= 80 || totalSessions >= 30) {
    level = "avanzado";
  } else if (totalQuestions >= 25 || totalSessions >= 10) {
    level = "intermedio";
  }

  const focusAreas = profileDoc?.focusAreas || [];
  const painAreas = profileDoc?.painAreas || [];
  let supportStyle = "directo";

  if (painAreas.some(item => /precio|esta caro|dinero|pensar|objecion/i.test(item))) {
    supportStyle = "cierre_y_objeciones";
  } else if (painAreas.some(item => /docucite|orden|papeleria/i.test(item))) {
    supportStyle = "operativo_post_cierre";
  } else if (focusAreas.some(item => /reclutamiento|negocio/i.test(item))) {
    supportStyle = "crecimiento_y_negocio";
  } else if (focusAreas.some(item => /demo|producto/i.test(item))) {
    supportStyle = "demo_y_producto";
  }

  return {
    questionsCount: totalQuestions,
    coachRepliesCount: profileDoc?.coachRepliesCount || 0,
    totalSessions,
    level,
    supportStyle,
    topTopics: extraerTopLabels(profileDoc?.topTopics || analyticsDoc?.topTopics || [], 5),
    topObjections: extraerTopLabels(profileDoc?.topObjections || analyticsDoc?.topObjections || [], 5),
    topProducts: extraerTopLabels(profileDoc?.topProducts || analyticsDoc?.topProducts || [], 5),
    topStages: extraerTopLabels(profileDoc?.topStages || analyticsDoc?.topStages || [], 5),
    focusAreas,
    painAreas,
    preferredCloseStyle: profileDoc?.preferredCloseStyle || "",
    lastInteractionAt: profileDoc?.lastInteractionAt || analyticsDoc?.lastInteractionAt || null,
    recentSessionsSummary: (profileDoc?.recentSessionsSummary || []).slice(0, 4).map(item => ({
      sessionId: item?.sessionId || "",
      summary: truncarTextoPrompt(item?.summary || "", 180),
      createdAt: item?.createdAt || null
    }))
  };
}

function construirContextoPerfilCoachPrompt(profileDoc = null, analyticsDoc = null) {
  const perfil = limpiarCoachProfile(profileDoc, analyticsDoc);

  if (!perfil) {
    return `
PERFIL OPERATIVO DEL DISTRIBUIDOR:
- nivel_estimado: novato
- estilo_de_apoyo: directo_y_simple

INSTRUCCION DE PERSONALIZACION:
Asume que este distribuidor necesita respuestas cortas, faciles de leer y con un siguiente paso muy claro.
No menciones que lo estas perfilando.
`;
  }

  const ultimasSesiones = (perfil.recentSessionsSummary || [])
    .slice(0, 3)
    .map(item => item.summary)
    .filter(Boolean);

  return `
PERFIL OPERATIVO DEL DISTRIBUIDOR:
- nivel_estimado: ${perfil.level}
- estilo_de_apoyo: ${perfil.supportStyle}
- preguntas_acumuladas: ${perfil.questionsCount}
- sesiones_acumuladas: ${perfil.totalSessions}
- temas_mas_consultados: ${perfil.topTopics.length ? perfil.topTopics.join(", ") : "sin datos"}
- objeciones_recurrentes: ${perfil.topObjections.length ? perfil.topObjections.join(", ") : "sin datos"}
- productos_mas_mencionados: ${perfil.topProducts.length ? perfil.topProducts.join(", ") : "sin datos"}
- etapas_mas_consultadas: ${perfil.topStages.length ? perfil.topStages.join(", ") : "sin datos"}
- areas_fuertes: ${perfil.focusAreas.length ? perfil.focusAreas.join(", ") : "sin datos"}
- areas_de_dolor: ${perfil.painAreas.length ? perfil.painAreas.join(", ") : "sin datos"}
- cierre_mas_consultado: ${perfil.preferredCloseStyle || "sin dato"}
- sesiones_recientes: ${ultimasSesiones.length ? ultimasSesiones.join(" || ") : "sin historial corto"}

INSTRUCCION DE PERSONALIZACION:
- no menciones este perfil ni digas que lo estas analizando
- si es novato, usa palabras mas simples, menos teoria y un solo siguiente paso
- si es intermedio, ve directo al punto y da una accion principal con una alternativa corta
- si es avanzado, responde mas ejecutivo y asume que ya entiende la demo
- si suele batallar en precio u objeciones, prioriza frase exacta + siguiente movimiento
- si suele batallar en ordenes o docucite, agrega el paso operativo despues del cierre
- si suele consultar producto o demo, conecta tu respuesta con beneficios reales del producto
- usa el cierre mas consultado solo si siembra bien con la situacion actual
`;
}

function mezclarMetricasCoach(metricasDocs = [], field = "", limit = 5) {
  let acumulado = [];

  for (const doc of Array.isArray(metricasDocs) ? metricasDocs : []) {
    for (const item of Array.isArray(doc?.[field]) ? doc[field] : []) {
      acumulado = incrementarMetricaCoach(acumulado, item?.label || "", Number(item?.count || 0) || 1);
    }
  }

  return extraerTopLabels(acumulado, limit);
}

async function obtenerCoachNetworkSummary() {
  const analyticsDocs = await CoachDistributorAnalytics.find(
    {},
    {
      totalQuestions: 1,
      lastInteractionAt: 1,
      topTopics: 1,
      topObjections: 1,
      topStages: 1
    }
  )
    .sort({ lastInteractionAt: -1 })
    .lean();

  const ahora = Date.now();
  const inicioHoy = new Date();
  inicioHoy.setHours(0, 0, 0, 0);
  const hace7Dias = new Date(ahora - 7 * 24 * 60 * 60 * 1000);

  return {
    totalDistributors: analyticsDocs.length,
    activeToday: analyticsDocs.filter(doc => doc?.lastInteractionAt && new Date(doc.lastInteractionAt) >= inicioHoy).length,
    activeLast7Days: analyticsDocs.filter(doc => doc?.lastInteractionAt && new Date(doc.lastInteractionAt) >= hace7Dias).length,
    totalQuestions: analyticsDocs.reduce((sum, doc) => sum + Number(doc?.totalQuestions || 0), 0),
    topTopics: mezclarMetricasCoach(analyticsDocs, "topTopics", 5),
    topObjections: mezclarMetricasCoach(analyticsDocs, "topObjections", 5),
    topStages: mezclarMetricasCoach(analyticsDocs, "topStages", 5)
  };
}

function obtenerLeadMemoryCollections() {
  const db = mongoose.connection?.db;

  if (!db) {
    return null;
  }

  return {
    rifaProfiles: db.collection(RIFA_PROFILE_COLLECTION),
    rifaStates: db.collection(RIFA_STATE_COLLECTION),
    rifaInsights: db.collection(RIFA_INSIGHTS_COLLECTION)
  };
}

function escapeRegex(value = "") {
  return String(value || "").replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

function normalizarTextoBusqueda(value = "") {
  return String(value || "")
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .toLowerCase()
    .replace(/\s+/g, " ")
    .trim();
}

function cleanText(value = "") {
  return String(value || "")
    .replace(/\s+/g, " ")
    .trim();
}

function cleanLower(value = "") {
  return cleanText(value).toLowerCase();
}

function normalizePhone(value = "") {
  const digits = String(value || "").replace(/\D/g, "");

  if (!digits) {
    return "";
  }

  if (digits.length === 11 && digits.startsWith("1")) {
    return digits.slice(1);
  }

  return digits;
}

function extraerTelefonosDesdeTexto(texto = "") {
  const matches = String(texto || "").match(/(?:\+?1[\s.-]?)?(?:\(?\d{3}\)?[\s.-]?)?\d{3}[\s.-]?\d{4}/g) || [];
  return [...new Set(matches.map(item => normalizePhone(item)).filter(Boolean))];
}

function extraerLeadIdDesdeTexto(texto = "") {
  const match = String(texto || "").match(/\blead\s*id\b[:#\s-]*([A-Za-z0-9_-]+)/i) || String(texto || "").match(/\bid\b[:#\s-]+([A-Za-z0-9_-]{1,12})/i);
  return cleanText(match?.[1] || "");
}

function extraerEmailDesdeTexto(texto = "") {
  const match = String(texto || "").match(/[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}/i);
  return normalizarEmail(match?.[0] || "");
}

function extraerTokensRepresentante(coachUser = null) {
  const tokens = new Set();
  const nombre = cleanText(coachUser?.name || "");

  nombre
    .split(/\s+/)
    .map(cleanText)
    .filter(token => token.length >= 4)
    .forEach(token => tokens.add(token));

  const emailLocal = normalizarEmail(coachUser?.email || "").split("@")[0] || "";
  emailLocal
    .split(/[._-]+/)
    .map(cleanText)
    .filter(token => token.length >= 4)
    .forEach(token => tokens.add(token));

  return [...tokens];
}

function construirResumenScoreLeads(insights = []) {
  const base = { hot: 0, warm: 0, cold: 0, dead: 0 };

  for (const item of insights) {
    const key = cleanLower(item?.leadTemperature || "");
    if (Object.prototype.hasOwnProperty.call(base, key)) {
      base[key] += 1;
    }
  }

  return base;
}

function obtenerTopConteos(items = [], field = "", limit = 5) {
  const counts = new Map();

  for (const item of Array.isArray(items) ? items : []) {
    const value = cleanText(item?.[field] || "");
    if (!value) {
      continue;
    }
    counts.set(value, (counts.get(value) || 0) + 1);
  }

  return [...counts.entries()]
    .sort((a, b) => b[1] - a[1])
    .slice(0, limit)
    .map(([label]) => label);
}

function construirLeadMemoryVisible(profile = null, state = null, insight = null) {
  if (!profile) {
    return null;
  }

  return {
    leadId: profile.leadId || "",
    leadName: profile.leadName || "",
    repName: profile.repName || "",
    phone: profile.phone || profile.phoneRaw || "",
    eventSource: profile.eventSource || "",
    productInterest: profile.productInterestRaw || "",
    waterSource: profile.waterSourceRaw || "",
    hasRoyalPrestige:
      typeof profile.hasRoyalPrestige === "boolean" ? (profile.hasRoyalPrestige ? "si" : "no") : "sin dato",
    productsOwned: profile.productsOwnedRaw || "",
    callStatus: state?.callStatusNormalized || state?.callStatusRaw || "",
    nextStep: state?.nextStep || "",
    leadTemperature: insight?.leadTemperature || "",
    bestScriptAngle: insight?.bestScriptAngle || "",
    primaryObjection: insight?.primaryObjection || "",
    appointmentDetected: Boolean(state?.appointmentDetected),
    callbackRecommended: Boolean(insight?.callbackRecommended),
    requiresSpousePresent: Boolean(insight?.requiresSpousePresent),
    worksLate: Boolean(insight?.worksLate),
    travellingOrUnavailable: Boolean(insight?.travellingOrUnavailable),
    lastContactSummary: state?.lastContactNoteSummary || profile.notesRaw || ""
  };
}

function construirPromptMemoriaLead(context = null, mode = "coach") {
  if (!context) {
    return "";
  }

  return `
MEMORIA REAL DEL LEAD:
- lead_detectado: si
- lead_id: ${context.leadId || "sin dato"}
- nombre: ${context.leadName || "sin dato"}
- representante_asignado: ${context.repName || "sin dato"}
- score_actual: ${context.leadTemperature || "sin dato"}
- estado_llamada: ${context.callStatus || "sin dato"}
- siguiente_paso_sugerido: ${context.nextStep || "sin dato"}
- angulo_recomendado: ${context.bestScriptAngle || "sin dato"}
- objecion_principal: ${context.primaryObjection || "sin dato"}
- producto_interes: ${context.productInterest || "sin dato"}
- agua: ${context.waterSource || "sin dato"}
- tiene_royal: ${context.hasRoyalPrestige || "sin dato"}
- productos_que_tiene: ${context.productsOwned || "sin dato"}
- fuente_del_lead: ${context.eventSource || "sin dato"}
- pareja_necesaria: ${context.requiresSpousePresent ? "si" : "no"}
- trabaja_tarde: ${context.worksLate ? "si" : "no"}
- viaje_o_no_disponible: ${context.travellingOrUnavailable ? "si" : "no"}
- resumen_ultimo_contacto: ${context.lastContactSummary || "sin dato"}

INSTRUCCION:
${mode === "coach"
    ? "Si la pregunta trata de esta persona, usa primero su score, su ultimo estado real y el siguiente paso. No ignores la memoria del lead."
    : "Usa este contexto para recordar interes real, situacion del hogar y producto correcto sin sonar invasivo ni recitar la ficha."}
`;
}

function construirPromptPipelineRepresentante(summary = null) {
  if (!summary) {
    return "";
  }

  return `
PIPELINE REAL DEL REPRESENTANTE:
- leads_asignados: ${summary.totalLeads || 0}
- score_hot: ${summary.scoreboard?.hot || 0}
- score_warm: ${summary.scoreboard?.warm || 0}
- score_cold: ${summary.scoreboard?.cold || 0}
- score_dead: ${summary.scoreboard?.dead || 0}
- estados_mas_repetidos: ${summary.topStatuses?.length ? summary.topStatuses.join(", ") : "sin datos"}
- angulos_que_mas_aparecen: ${summary.topScriptAngles?.length ? summary.topScriptAngles.join(", ") : "sin datos"}
- leads_con_callback: ${summary.callbackLeads || 0}
- leads_con_cita: ${summary.appointmentLeads || 0}

INSTRUCCION:
Si no hay un lead especifico, usa estas senales para sugerir el mejor siguiente movimiento comercial para su pipeline real.
`;
}

async function buscarLeadRifaPorContexto({
  collections,
  question = "",
  leadDoc = null,
  profileDoc = null,
  candidateProfiles = []
}) {
  const leadIdFromQuestion = extraerLeadIdDesdeTexto(question);
  const emails = combinarListas(
    [],
    [extraerEmailDesdeTexto(question), normalizarEmail(leadDoc?.email || ""), normalizarEmail(profileDoc?.email || "")]
  ).filter(Boolean);
  const phones = combinarListas(
    [],
    [...extraerTelefonosDesdeTexto(question), normalizePhone(leadDoc?.phone || ""), normalizePhone(profileDoc?.phone || "")]
  ).filter(Boolean);
  const nameCandidates = combinarListas([], [leadDoc?.name || "", profileDoc?.name || ""]).filter(Boolean);

  let matchedProfile = null;

  if (leadIdFromQuestion) {
    matchedProfile = await collections.rifaProfiles.findOne({ leadId: leadIdFromQuestion });
  }

  if (!matchedProfile && phones.length) {
    matchedProfile = await collections.rifaProfiles.findOne({ phone: { $in: phones } });
  }

  if (!matchedProfile && emails.length) {
    matchedProfile = await collections.rifaProfiles.findOne({ email: { $in: emails } });
  }

  if (!matchedProfile && nameCandidates.length) {
    for (const name of nameCandidates) {
      const regex = new RegExp(`^${escapeRegex(cleanText(name))}$`, "i");
      matchedProfile = await collections.rifaProfiles.findOne({ leadName: regex });
      if (matchedProfile) {
        break;
      }
    }
  }

  if (!matchedProfile && candidateProfiles.length) {
    const questionNormalized = normalizarTextoBusqueda(question);
    matchedProfile =
      candidateProfiles.find(profile => {
        const normalizedName = normalizarTextoBusqueda(profile?.leadName || "");
        if (normalizedName && normalizedName.length >= 5 && questionNormalized.includes(normalizedName)) {
          return true;
        }

        const phone = String(profile?.phone || "").replace(/\D/g, "");
        return phone.length >= 7 && questionNormalized.includes(phone.slice(-7));
      }) || null;
  }

  if (!matchedProfile) {
    return null;
  }

  const [state, insight] = await Promise.all([
    collections.rifaStates.findOne({ leadId: matchedProfile.leadId }),
    collections.rifaInsights.findOne({ leadId: matchedProfile.leadId })
  ]);

  return construirLeadMemoryVisible(matchedProfile, state, insight);
}

async function obtenerResumenPipelineRepresentante(coachUser = null, collections = null) {
  const tokens = extraerTokensRepresentante(coachUser);

  if (!collections || !tokens.length) {
    return null;
  }

  const profiles = await collections.rifaProfiles
    .find({
      $or: tokens.map(token => ({
        repName: new RegExp(`^${escapeRegex(token)}\\b`, "i")
      }))
    })
    .toArray();

  if (!profiles.length) {
    return null;
  }

  const leadIds = profiles.map(item => item.leadId).filter(Boolean);
  const [states, insights] = await Promise.all([
    collections.rifaStates.find({ leadId: { $in: leadIds } }).toArray(),
    collections.rifaInsights.find({ leadId: { $in: leadIds } }).toArray()
  ]);

  const stateByLeadId = new Map(states.map(item => [item.leadId, item]));
  const insightByLeadId = new Map(insights.map(item => [item.leadId, item]));
  const visibleLeads = profiles
    .map(profile => construirLeadMemoryVisible(profile, stateByLeadId.get(profile.leadId), insightByLeadId.get(profile.leadId)))
    .filter(Boolean);

  return {
    repName: profiles[0]?.repName || tokens[0] || "",
    totalLeads: profiles.length,
    scoreboard: construirResumenScoreLeads(insights),
    topStatuses: obtenerTopConteos(states, "callStatusNormalized", 4),
    topScriptAngles: obtenerTopConteos(insights, "bestScriptAngle", 4),
    callbackLeads: insights.filter(item => item?.callbackRecommended).length,
    appointmentLeads: states.filter(item => item?.appointmentDetected).length,
    sampleLeads: visibleLeads.slice(0, 4)
  };
}

async function obtenerMemoriaLeadRelacionada({
  question = "",
  mode = "chef",
  leadDoc = null,
  profileDoc = null,
  coachUser = null
}) {
  const collections = obtenerLeadMemoryCollections();

  if (!collections) {
    return {
      leadContext: null,
      repLeadSummary: null
    };
  }

  let repLeadSummary = null;
  let candidateProfiles = [];

  if (mode === "coach" && coachUser) {
    repLeadSummary = await obtenerResumenPipelineRepresentante(coachUser, collections);
    candidateProfiles = repLeadSummary?.sampleLeads?.length
      ? await collections.rifaProfiles.find({ repName: new RegExp(`^${escapeRegex(repLeadSummary.repName)}\\b`, "i") }).toArray()
      : [];
  }

  const leadContext = await buscarLeadRifaPorContexto({
    collections,
    question,
    leadDoc,
    profileDoc,
    candidateProfiles
  });

  return {
    leadContext,
    repLeadSummary
  };
}

function incrementarMetricaCoach(metricas = [], label = "", amount = 1) {
  if (!label) {
    return metricas || [];
  }

  const ahora = new Date();
  const lista = Array.isArray(metricas) ? [...metricas] : [];
  const normalizada = String(label).trim().toLowerCase();
  const existente = lista.find(item => String(item?.label || "").trim().toLowerCase() === normalizada);

  if (existente) {
    existente.count = (existente.count || 0) + amount;
    existente.lastSeenAt = ahora;
  } else {
    lista.push({
      label: String(label).trim(),
      count: amount,
      lastSeenAt: ahora
    });
  }

  return lista
    .filter(item => item?.label)
    .sort((a, b) => (b.count || 0) - (a.count || 0))
    .slice(0, 8);
}

function obtenerDateKey(date = new Date()) {
  const year = date.getFullYear();
  const month = String(date.getMonth() + 1).padStart(2, "0");
  const day = String(date.getDate()).padStart(2, "0");
  return `${year}-${month}-${day}`;
}

function detectarDolorCoach(texto = "") {
  return /no se|no s[eé]|me atoro|me trabo|batallo|me cuesta|confundo|confundido|complicado|dif[ií]cil|ayuda|no entiendo|no me sale|me gana/i.test(
    texto
  );
}

function extraerObjecionesCoach(texto = "") {
  const normalizado = normalizarTextoBusquedaCoach(texto);
  const objeciones = [];
  const catalogo = [
    { label: "esta caro", regex: /esta caro|muy caro|caro/ },
    { label: "lo voy a pensar", regex: /lo voy a pensar|pensarlo|pensar/ },
    { label: "no tengo dinero", regex: /no tengo dinero|no me alcanza|no puedo ahorita/ },
    { label: "ya tengo", regex: /ya tengo|ya compre|ya compr[eé]/ },
    { label: "no tengo tiempo", regex: /no tengo tiempo|despues|mas adelante/ },
    { label: "no me interesa", regex: /no me interesa|no estoy interesad/ }
  ];

  for (const item of catalogo) {
    if (item.regex.test(normalizado)) {
      objeciones.push(item.label);
    }
  }

  return objeciones;
}

function extraerCierresConsultadosCoach(texto = "") {
  const normalizado = normalizarTextoBusquedaCoach(texto);
  const cierres = [];
  const catalogo = [
    { label: "benjamin franklin", regex: /benjamin franklin/ },
    { label: "doble alternativa", regex: /doble alternativa/ },
    { label: "rebote", regex: /rebote/ },
    { label: "silencio", regex: /silencio|se queda callado|se queda en silencio/ },
    { label: "puercoespin", regex: /puercoesp[ii]n/ },
    { label: "llamada de cierre", regex: /llamada de cierre/ },
    { label: "me facilita su id", regex: /me facilita su id|bienvenido a royal prestige/ }
  ];

  for (const item of catalogo) {
    if (item.regex.test(normalizado)) {
      cierres.push(item.label);
    }
  }

  return cierres;
}

function extraerEtapasCoach(pregunta = "", temaCoach = null) {
  const tema = temaCoach || detectarTemaCoach(normalizarTextoBusquedaCoach(pregunta));
  const etapas = [];

  if (tema.demo) etapas.push("demo");
  if (tema.producto) etapas.push("presentacion_producto");
  if (tema.precio) etapas.push("negociacion_precio");
  if (tema.objecion) etapas.push("objecion");
  if (tema.cierre) etapas.push("cierre");
  if (tema.cierreFinal) etapas.push("cierre_final");
  if (tema.ordenes) etapas.push("post_cierre_orden");
  if (tema.seguimiento) etapas.push("seguimiento");
  if (tema.reclutamiento) etapas.push("reclutamiento");
  if (tema.negocio) etapas.push("plan_de_negocio");
  if (tema.mentalidad) etapas.push("mentalidad");

  return etapas;
}

function extraerTopLabels(metricas = [], limit = 3) {
  return (Array.isArray(metricas) ? metricas : [])
    .slice()
    .sort((a, b) => (b?.count || 0) - (a?.count || 0))
    .slice(0, limit)
    .map(item => item.label)
    .filter(Boolean);
}

function construirResumenSesionCoach(question = "", reply = "") {
  const pregunta = truncarTextoPrompt(question, 120);
  const respuesta = truncarTextoPrompt(reply, 140);
  return `Pregunta: ${pregunta} | Coach: ${respuesta}`;
}

async function actualizarPerfilYAnalyticsCoach({ userDoc, sessionId = "", question = "", reply = "" }) {
  if (!userDoc || !question) {
    return null;
  }

  const ahora = new Date();
  const tema = detectarTemaCoach(normalizarTextoBusquedaCoach(question));
  const topicos = [
    tema.precio ? "precio" : "",
    tema.producto ? "producto" : "",
    tema.demo ? "demo" : "",
    tema.objecion ? "objecion" : "",
    tema.cierre ? "cierre" : "",
    tema.cierreFinal ? "cierre_final" : "",
    tema.ordenes ? "ordenes" : "",
    tema.mentalidad ? "mentalidad" : "",
    tema.seguimiento ? "seguimiento" : "",
    tema.reclutamiento ? "reclutamiento" : "",
    tema.negocio ? "negocio" : ""
  ].filter(Boolean);
  const objeciones = extraerObjecionesCoach(question);
  const productos = extraerProductos(question);
  const etapas = extraerEtapasCoach(question, tema);
  const cierres = extraerCierresConsultadosCoach(`${question} ${reply}`);
  const sessionSummary = {
    sessionId,
    summary: construirResumenSesionCoach(question, reply),
    createdAt: ahora
  };

  const profile = (await CoachDistributorProfile.findOne({ userId: userDoc._id })) || new CoachDistributorProfile({
    userId: userDoc._id,
    createdAt: ahora
  });

  profile.name = userDoc.name || profile.name || "";
  profile.email = userDoc.email || profile.email || "";
  profile.subscriptionStatus = obtenerCoachStatusVisible(userDoc);
  profile.questionsCount = (profile.questionsCount || 0) + 1;
  profile.coachRepliesCount = (profile.coachRepliesCount || 0) + (reply ? 1 : 0);
  profile.lastQuestion = question;
  profile.lastCoachReply = reply || profile.lastCoachReply || "";
  profile.lastInteractionAt = ahora;
  profile.updatedAt = ahora;

  if (tema.precio) profile.pricingQuestionsCount = (profile.pricingQuestionsCount || 0) + 1;
  if (tema.seguimiento) profile.followUpQuestionsCount = (profile.followUpQuestionsCount || 0) + 1;
  if (tema.ordenes) profile.docuciteQuestionsCount = (profile.docuciteQuestionsCount || 0) + 1;
  if (tema.reclutamiento) profile.recruitingQuestionsCount = (profile.recruitingQuestionsCount || 0) + 1;
  if (tema.demo) profile.demoQuestionsCount = (profile.demoQuestionsCount || 0) + 1;
  if (tema.objecion) profile.objectionQuestionsCount = (profile.objectionQuestionsCount || 0) + 1;
  if (tema.producto) profile.productQuestionsCount = (profile.productQuestionsCount || 0) + 1;
  if (tema.cierre || tema.cierreFinal) profile.closingQuestionsCount = (profile.closingQuestionsCount || 0) + 1;
  if (tema.mentalidad) profile.mindsetQuestionsCount = (profile.mindsetQuestionsCount || 0) + 1;
  if (tema.negocio) profile.businessQuestionsCount = (profile.businessQuestionsCount || 0) + 1;

  for (const item of topicos) {
    profile.topTopics = incrementarMetricaCoach(profile.topTopics, item);
  }

  for (const item of objeciones) {
    profile.topObjections = incrementarMetricaCoach(profile.topObjections, item);
  }

  for (const item of productos) {
    profile.topProducts = incrementarMetricaCoach(profile.topProducts, item);
  }

  for (const item of etapas) {
    profile.topStages = incrementarMetricaCoach(profile.topStages, item);
  }

  for (const item of cierres) {
    profile.closeStylesConsulted = incrementarMetricaCoach(profile.closeStylesConsulted, item);
  }

  profile.focusAreas = extraerTopLabels(profile.topTopics, 3);
  profile.painAreas = [
    ...new Set([
      ...extraerTopLabels(profile.topObjections, 3),
      ...(detectarDolorCoach(question) ? extraerTopLabels(profile.topTopics, 2) : [])
    ])
  ].slice(0, 3);
  profile.preferredCloseStyle = extraerTopLabels(profile.closeStylesConsulted, 1)[0] || profile.preferredCloseStyle || "";
  profile.recentSessionsSummary = [sessionSummary, ...(profile.recentSessionsSummary || [])].slice(0, 10);
  await profile.save();

  const analytics =
    (await CoachDistributorAnalytics.findOne({ userId: userDoc._id })) ||
    new CoachDistributorAnalytics({
      userId: userDoc._id,
      createdAt: ahora,
      firstInteractionAt: ahora
    });

  analytics.name = userDoc.name || analytics.name || "";
  analytics.email = userDoc.email || analytics.email || "";
  analytics.lastInteractionAt = ahora;
  analytics.updatedAt = ahora;
  analytics.totalQuestions = (analytics.totalQuestions || 0) + 1;
  analytics.totalReplies = (analytics.totalReplies || 0) + (reply ? 1 : 0);

  if (sessionId && analytics.lastSessionId !== sessionId) {
    analytics.totalSessions = (analytics.totalSessions || 0) + 1;
    analytics.lastSessionId = sessionId;
  }

  for (const item of topicos) {
    analytics.topTopics = incrementarMetricaCoach(analytics.topTopics, item);
  }

  for (const item of objeciones) {
    analytics.topObjections = incrementarMetricaCoach(analytics.topObjections, item);
  }

  for (const item of productos) {
    analytics.topProducts = incrementarMetricaCoach(analytics.topProducts, item);
  }

  for (const item of etapas) {
    analytics.topStages = incrementarMetricaCoach(analytics.topStages, item);
  }

  for (const item of cierres) {
    analytics.closeStylesConsulted = incrementarMetricaCoach(analytics.closeStylesConsulted, item);
  }

  const dateKey = obtenerDateKey(ahora);
  const dailyRollup = Array.isArray(analytics.dailyRollup) ? [...analytics.dailyRollup] : [];
  let daily = dailyRollup.find(item => item.dateKey === dateKey);

  if (!daily) {
    daily = {
      dateKey,
      questions: 0,
      replies: 0,
      topics: [],
      objections: [],
      products: [],
      stages: [],
      closeStyles: []
    };
    dailyRollup.unshift(daily);
  }

  daily.questions += 1;
  daily.replies += reply ? 1 : 0;

  for (const item of topicos) {
    daily.topics = incrementarMetricaCoach(daily.topics, item);
  }

  for (const item of objeciones) {
    daily.objections = incrementarMetricaCoach(daily.objections, item);
  }

  for (const item of productos) {
    daily.products = incrementarMetricaCoach(daily.products, item);
  }

  for (const item of etapas) {
    daily.stages = incrementarMetricaCoach(daily.stages, item);
  }

  for (const item of cierres) {
    daily.closeStyles = incrementarMetricaCoach(daily.closeStyles, item);
  }

  analytics.dailyRollup = dailyRollup.slice(0, 45);
  await analytics.save();

  return {
    profile,
    analytics
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

function obtenerCoachIp(req) {
  const forwarded = String(req.headers["x-forwarded-for"] || "")
    .split(",")
    .map(item => item.trim())
    .filter(Boolean)[0];

  return forwarded || req.ip || req.socket?.remoteAddress || "";
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
  const activeSessions = await CoachSession.find({
    userId,
    expiresAt: { $gt: new Date() }
  })
    .sort({ lastSeenAt: -1, createdAt: -1 });

  if (activeSessions.length >= COACH_MAX_ACTIVE_SESSIONS) {
    const sessionsToRemove = activeSessions.slice(COACH_MAX_ACTIVE_SESSIONS - 1);

    if (sessionsToRemove.length) {
      await CoachSession.deleteMany({
        _id: { $in: sessionsToRemove.map(session => session._id) }
      });
    }
  }

  await CoachSession.create({
    userId,
    tokenHash,
    ipAddress: obtenerCoachIp(req),
    userAgent: String(req.headers["user-agent"] || "").slice(0, 400),
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

function obtenerInicioDia(date = new Date()) {
  const inicio = new Date(date);
  inicio.setHours(0, 0, 0, 0);
  return inicio;
}

async function validarLimiteUsoCoach(userDoc = null) {
  if (!userDoc || coachTieneAccesoDePrueba(userDoc.email)) {
    return {
      allowed: true,
      usedToday: 0,
      remainingToday: COACH_MAX_MESSAGES_PER_DAY
    };
  }

  const startOfDay = obtenerInicioDia();
  const usedToday = await Message.countDocuments({
    leadId: null,
    profileId: null,
    role: "user",
    createdAt: { $gte: startOfDay },
    intent: "coach_chat",
    detectedTopics: `coach_user:${String(userDoc._id)}`
  });

  return {
    allowed: usedToday < COACH_MAX_MESSAGES_PER_DAY,
    usedToday,
    remainingToday: Math.max(COACH_MAX_MESSAGES_PER_DAY - usedToday, 0)
  };
}

async function validarLimiteUsoChef(visitorId = "") {
  if (!visitorId) {
    return {
      allowed: true,
      usedToday: 0,
      remainingToday: CHEF_MAX_MESSAGES_PER_DAY
    };
  }

  const startOfDay = obtenerInicioDia();
  const usedToday = await Message.countDocuments({
    visitorId,
    role: "user",
    createdAt: { $gte: startOfDay },
    intent: "chef_chat"
  });

  return {
    allowed: usedToday < CHEF_MAX_MESSAGES_PER_DAY,
    usedToday,
    remainingToday: Math.max(CHEF_MAX_MESSAGES_PER_DAY - usedToday, 0)
  };
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

function limpiarRespuestaBreveLead(texto = "", maxLength = 80) {
  return String(texto || "")
    .replace(/\s+/g, " ")
    .trim()
    .replace(/[.,;!?]+$/, "")
    .slice(0, maxLength);
}

function extraerMejorDiaLlamada(texto = "") {
  const normalizado = normalizarTextoBusqueda(texto);
  const patronesContexto = [
    /(?:mejor\s+dia(?:\s+para\s+llamar)?\s*(?:es)?|prefiero\s+que\s+me\s+llamen(?:\s+el)?|pueden\s+llamarme(?:\s+el)?|llamenme(?:\s+el)?|llamame(?:\s+el)?|me\s+queda\s+mejor(?:\s+el)?|seria\s+mejor(?:\s+el)?)([^.\n]+)/i,
    /\b(?:hoy|manana|lunes|martes|miercoles|jueves|viernes|sabado|domingo|entre\s+semana|fin\s+de\s+semana)\b/i
  ];
  const catalogo = [
    "entre semana",
    "fin de semana",
    "hoy",
    "manana",
    "lunes",
    "martes",
    "miercoles",
    "jueves",
    "viernes",
    "sabado",
    "domingo"
  ];

  for (const patron of patronesContexto) {
    const match = normalizado.match(patron);

    if (!match) {
      continue;
    }

    const segmento = limpiarRespuestaBreveLead(match[1] || match[0], 60);

    for (const item of catalogo) {
      if (segmento.includes(item)) {
        return item;
      }
    }
  }

  return "";
}

function extraerMejorHoraLlamada(texto = "") {
  const normalizado = normalizarTextoBusqueda(texto);
  const segmentos = [normalizado];
  const patronesContexto = [
    /(?:mejor\s+hora(?:\s+para\s+llamar)?\s*(?:es)?|prefiero\s+que\s+me\s+llamen|pueden\s+llamarme|llamenme|llamame|me\s+queda\s+mejor|seria\s+mejor)\s+([^.\n]+)/i
  ];

  for (const patron of patronesContexto) {
    const match = normalizado.match(patron);

    if (match?.[1]) {
      segmentos.unshift(limpiarRespuestaBreveLead(match[1], 80));
    }
  }

  const patrones = [
    /\b((?:despues|antes)\s+de\s+las\s+\d{1,2}(?::\d{2})?\s*(?:am|pm)?)\b/i,
    /\b(entre\s+las\s+\d{1,2}(?::\d{2})?\s*(?:am|pm)?\s+y\s+\d{1,2}(?::\d{2})?\s*(?:am|pm)?)\b/i,
    /\b((?:a\s+las|como\s+a\s+las|tipo)\s+\d{1,2}(?::\d{2})?\s*(?:am|pm)?)\b/i,
    /\b(\d{1,2}(?::\d{2})?\s*(?:am|pm))\b/i,
    /\b(por\s+la\s+manana|por\s+la\s+tarde|por\s+la\s+noche|temprano|al\s+mediodia)\b/i
  ];

  for (const segmento of segmentos) {
    for (const patron of patrones) {
      const match = segmento.match(patron);

      if (match?.[1]) {
        return limpiarRespuestaBreveLead(match[1], 50);
      }
    }
  }

  return "";
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

  if (profile.bestCallDay) {
    partes.push(`Mejor dia para llamar: ${profile.bestCallDay}.`);
  }

  if (profile.bestCallTime) {
    partes.push(`Mejor hora para llamar: ${profile.bestCallTime}.`);
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
  estadoConversacion = null,
  intent = "",
  detectedTopics = []
}) {
  if (!visitorId || !content) {
    return null;
  }

  try {
    const intentFinal = intent || (role === "user" ? detectarIntentoMensaje(content, estadoConversacion) : "respuesta_ai");
    const detectedTopicsFinal = Array.isArray(detectedTopics)
      ? detectedTopics.filter(Boolean)
      : role === "user"
        ? extraerTemasInteres(content)
        : [];

    return await Message.create({
      visitorId,
      sessionId,
      profileId,
      leadId,
      role,
      content,
      intent: intentFinal,
      detectedTopics: detectedTopicsFinal,
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
  const bestCallDay = extraerMejorDiaLlamada(texto);
  const bestCallTime = extraerMejorHoraLlamada(texto);
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
    bestCallDay: seleccionarValorMasCompleto(
      leadGuardado?.bestCallDay,
      bestCallDay,
      estadoConversacion?.bestCallDay,
      perfilExistente?.bestCallDay
    ),
    bestCallTime: seleccionarValorMasCompleto(
      leadGuardado?.bestCallTime,
      bestCallTime,
      estadoConversacion?.bestCallTime,
      perfilExistente?.bestCallTime
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
  marcarSesionActiva(sessionId);
  limpiarMemoriaSesiones();

  if (!estadosConversacion[sessionId]) {
    estadosConversacion[sessionId] = crearEstadoConversacionBase();
  }

  return estadosConversacion[sessionId];
}

function actualizarEstadoConversacion(sessionId, texto, leadGuardado = null) {
  const estado = obtenerEstadoConversacion(sessionId);
  const leadInfo = extraerLeadInfo(texto);
  const detallesLead = extraerDetallesLead(texto);
  const bestCallDay = extraerMejorDiaLlamada(texto);
  const bestCallTime = extraerMejorHoraLlamada(texto);
  const tieneProductos = extraerTieneProductos(texto, detallesLead.productos);
  const necesitaGarantia = extraerNecesitaGarantia(texto);

  estado.envioDatosContactoReciente = detectarEnvioDatosContacto(texto);

  if (leadInfo?.email) {
    estado.email = leadInfo.email;
  }

  if (leadInfo?.phone) {
    estado.phone = leadInfo.phone;
  }

  if (bestCallDay) {
    estado.bestCallDay = bestCallDay;
  }

  if (bestCallTime) {
    estado.bestCallTime = bestCallTime;
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
    estado.bestCallDay = leadGuardado.bestCallDay || estado.bestCallDay;
    estado.bestCallTime = leadGuardado.bestCallTime || estado.bestCallTime;
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
  const datosAgenda = [];

  if (!estado.name) {
    datosVitales.push("nombre completo");
  }

  if (!estado.phone) {
    datosVitales.push("telefono");
  }

  if (!estado.bestCallDay) {
    datosAgenda.push("mejor dia para llamar");
  }

  if (!estado.bestCallTime) {
    datosAgenda.push("mejor hora para llamar");
  }

  return {
    datosVitales,
    datosAgenda
  };
}

function construirEstadoPrompt(sessionId) {
  const estado = obtenerEstadoConversacion(sessionId);
  const { datosVitales, datosAgenda } = obtenerSiguienteDatoLead(estado);
  let fase = "chef-y-guia-de-uso";
  let instruccion = "Enfocate en ayudar con cocina saludable, recetas y uso correcto de productos Royal Prestige.";

  if (estado.interesComercial || estado.consultaPrecio) {
    fase = "interes-comercial";
    instruccion = "Si el usuario muestra interes comercial, invitalo a una llamada informativa sin compromiso con un representante 5 estrellas.";
  }

  if (estado.llamadaInformativaAceptada) {
    fase = "captura-breve-de-lead";

    if (estado.envioDatosContactoReciente && !datosVitales.length && !datosAgenda.length) {
      instruccion = "La persona acaba de compartir sus datos. Agradece brevemente, confirma la llamada informativa con el numero registrado y solo si cabe agrega una recomendacion util muy breve.";
    } else if (datosVitales.length) {
      const solicitudBreve = [...datosVitales, ...datosAgenda].join(", ");
      instruccion = `La llamada informativa ya fue aceptada. Pide en un solo mensaje breve estos datos: ${solicitudBreve}. Hazlo natural y facil de contestar.`;
    } else if (datosAgenda.length) {
      instruccion = `Ya tienes nombre y telefono. Pide en un solo mensaje breve estos datos de agenda: ${datosAgenda.join(", ")}.`;
    } else {
      instruccion = "Ya tienes los datos clave y de agenda; confirma que un representante 5 estrellas puede continuar con la llamada informativa. No pidas mas datos.";
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
- mejor_dia_para_llamar: ${estado.bestCallDay || "pendiente"}
- mejor_hora_para_llamar: ${estado.bestCallTime || "pendiente"}
- productos_mencionados: ${estado.productos.length ? estado.productos.join(", ") : "pendiente"}
- temas_interes: ${estado.temasInteres.length ? estado.temasInteres.join(", ") : "pendiente"}
- datos_vitales_faltantes: ${datosVitales.length ? datosVitales.join(", ") : "ninguno"}
- datos_agenda_faltantes: ${datosAgenda.length ? datosAgenda.join(", ") : "ninguno"}

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
  primaryLead.bestCallDay = seleccionarValorMasCompleto(
    primaryLead.bestCallDay,
    ...duplicateLeads.map(lead => lead.bestCallDay)
  );
  primaryLead.bestCallTime = seleccionarValorMasCompleto(
    primaryLead.bestCallTime,
    ...duplicateLeads.map(lead => lead.bestCallTime)
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
- mejor_dia_para_llamar: ${fuente.bestCallDay || "desconocido"}
- mejor_hora_para_llamar: ${fuente.bestCallTime || "desconocido"}
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

async function obtenerHistorialConversacionPrompt(sessionId, limit = MAX_PROMPT_HISTORY_MESSAGES) {
  if (!sessionId) {
    return [];
  }

  const safeLimit = Math.max(1, Number(limit || MAX_PROMPT_HISTORY_MESSAGES));

  try {
    const historialMongo = await Message.find({ sessionId })
      .sort({ createdAt: -1 })
      .limit(safeLimit)
      .select({ role: 1, content: 1, _id: 0 })
      .lean();

    if (historialMongo.length) {
      return historialMongo
        .reverse()
        .filter(entry => entry?.role && entry?.content)
        .map(entry => ({
          role: entry.role,
          content: entry.content
        }));
    }
  } catch (error) {
    console.log("Error leyendo historial MongoDB:", error.message);
  }

  return (conversaciones[sessionId] || []).slice(-safeLimit);
}

function obtenerTextoOperativoLead(lead = null, profile = null) {
  const notasRecientes = normalizarNotas(lead?.notes || [])
    .slice(-8)
    .map(note => note.text)
    .filter(Boolean);
  const historialReciente = normalizarHistorialConversacion([
    ...(profile?.recentHistory || []),
    ...(lead?.conversationHistory || [])
  ])
    .slice(-12)
    .map(entry => entry.content)
    .filter(Boolean);

  return [lead?.message || "", profile?.lastUserMessage || "", ...notasRecientes, ...historialReciente]
    .filter(Boolean)
    .join(" ");
}

function inferirTemperaturaLead(lead = null, profile = null) {
  const texto = obtenerTextoOperativoLead(lead, profile);
  const preguntaPrecio = detectarConsultaPrecio(texto);
  const productos = combinarListas(
    lead?.productos || [],
    profile?.productos || [],
    profile?.productosInteres || []
  );
  const quiereLlamada = (lead?.quiereLlamada || profile?.quiereLlamada || "") === "si";
  const consultaGarantia = /garant[ií]a/i.test(texto) || lead?.necesitaGarantia === "si" || profile?.necesitaGarantia === "si";
  const interesDirecto = /comprar|me interesa|quiero informacion|quiero saber|agendar|llamada|demo|demostracion|representante/i.test(
    texto
  );

  if (quiereLlamada && (preguntaPrecio || productos.length || interesDirecto)) {
    return "caliente";
  }

  if (quiereLlamada || preguntaPrecio || productos.length || consultaGarantia || interesDirecto) {
    return "interesado";
  }

  return "curioso";
}

function construirResumenLeadOperativo(lead = null, profile = null) {
  const texto = obtenerTextoOperativoLead(lead, profile);
  const temperatura = inferirTemperaturaLead(lead, profile);
  const partes = [`Lead ${temperatura}.`];
  const productos = combinarListas(
    profile?.productosInteres || [],
    lead?.productos || [],
    profile?.productos || []
  );
  const temas = combinarListas(profile?.temasInteres || [], lead?.temasInteres || []);

  if (productos.length) {
    partes.push(`Productos o intereses: ${productos.join(", ")}.`);
  }

  if ((lead?.quiereLlamada || profile?.quiereLlamada || "") === "si") {
    partes.push("Acepto llamada.");
  }

  if (detectarConsultaPrecio(texto)) {
    partes.push("Pregunto precio o pagos.");
  }

  if (/garant[ií]a/i.test(texto) || lead?.necesitaGarantia === "si" || profile?.necesitaGarantia === "si") {
    partes.push("Pregunto garantia o soporte.");
  }

  if (profile?.cocinaPara || lead?.cocinaPara) {
    partes.push(`Cocina para ${profile?.cocinaPara || lead?.cocinaPara}.`);
  }

  if (profile?.esCliente || lead?.esCliente) {
    partes.push(`Estado cliente: ${profile?.esCliente || lead?.esCliente}.`);
  }

  if (temas.length) {
    partes.push(`Temas tocados: ${temas.join(", ")}.`);
  }

  const ultimoMensaje = profile?.lastUserMessage || lead?.message || "";

  if (ultimoMensaje) {
    partes.push(`Ultimo mensaje: ${truncarTextoPrompt(ultimoMensaje, 140)}.`);
  }

  return partes.join(" ");
}

function construirPayloadLeadParaGoogleSheets(lead = null, profile = null) {
  if (!lead) {
    return null;
  }

  return {
    nombre: lead?.name || profile?.name || "",
    telefono: lead?.phone || profile?.phone || "",
    mejor_dia_para_llamar: lead?.bestCallDay || profile?.bestCallDay || "",
    mejor_hora_para_llamar: lead?.bestCallTime || profile?.bestCallTime || "",
    notas: truncarTextoPrompt(construirResumenLeadOperativo(lead, profile), 480)
  };
}

async function sincronizarLeadAGoogleSheets(lead, profile = null) {
  if (!lead || !process.env.GOOGLE_SHEETS_WEBHOOK_URL) {
    return;
  }

  try {
    const leadPayload = typeof lead.toObject === "function" ? lead.toObject() : lead;
    const profilePayload = profile && typeof profile.toObject === "function" ? profile.toObject() : profile;
    const payload = construirPayloadLeadParaGoogleSheets(leadPayload, profilePayload);

    if (!payload?.telefono || (leadPayload?.quiereLlamada || profilePayload?.quiereLlamada || "") !== "si") {
      return;
    }

    const response = await fetch(process.env.GOOGLE_SHEETS_WEBHOOK_URL, {
      method: "POST",
      headers: {
        "Content-Type": "application/json"
      },
      body: JSON.stringify(payload),
      redirect: "manual"
    });

    const locationHeader = response.headers.get("location") || "";
    const googleAppsRedirect =
      (response.status === 302 || response.status === 303) &&
      /script\.googleusercontent\.com\/macros\/echo/i.test(locationHeader);

    if (!response.ok && !googleAppsRedirect) {
      console.log("Error Google Sheets:", response.status, response.statusText);
    }
  } catch (error) {
    console.log("Error sincronizando Google Sheets:", error.message);
  }
}

async function guardarLeadSiExiste(texto, sessionId, visitorId = "", estadoConversacion = null) {
  const leadInfo = extraerLeadInfo(texto);
  const detallesLead = extraerDetallesLead(texto);
  const bestCallDay = extraerMejorDiaLlamada(texto) || estadoConversacion?.bestCallDay || "";
  const bestCallTime = extraerMejorHoraLlamada(texto) || estadoConversacion?.bestCallTime || "";
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

    if (bestCallDay) {
      camposActualizar.bestCallDay = bestCallDay;
    }

    if (bestCallTime) {
      camposActualizar.bestCallTime = bestCallTime;
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
const chefCocinaMexicanaSaludable = cargarJSON("./src/data/chef_cocina_mexicana_saludable_publico.json");
const chefFiltracionAguaEwg = cargarJSON("./src/data/chef_filtracion_agua_ewg_publico.json");

const cierresAlexDey = cargarJSON("./src/data/12_cierres_alex_dey.json");
const mentalidadOlmedo = cargarJSON("./src/data/mentalidad_ventas_olmedo.json");
const reclutamientoCiprian = cargarJSON("./src/data/reclutamiento_ciprian.json");
const sistema4Citas = cargarJSON("./src/data/sistema_4_citas_14_dias.json");
const manualNovatoCoach = cargarJSON("./src/data/manual_novato_2016_coach.json");
const planNegocio2026Coach = cargarJSON("./src/data/plan_negocio_2026_us_coach.json");
const docuciteOrdenesCoach = cargarJSON("./src/data/docucite_ordenes_publico_coach.json");

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
- Despues de que acepten la llamada, pedir solo los datos operativos minimos en un solo mensaje breve

REGLAS DE RESPUESTA:
- Maximo 3 oraciones
- Espanol claro, calido, sencillo y seguro
- Si hablan de recetas o ingredientes, responde primero como chef
- Da respuestas practicas, no discursos largos
- Si recomiendas un producto, di por que les ayuda en esa receta o situacion
- Evita demasiada informacion en un solo mensaje
- Nunca hagas sentir presion

PRECIOS:
- No des precios, mensualidades ni cotizaciones exactas desde el Chef
- Si preguntan precio, promociones o cuanto cuesta, di que para precios es mejor hablar con un distribuidor autorizado
- Cuando ayude, ofrece pedir una llamada con un distribuidor autorizado

VENTAS:
- Primero llamada informativa, despues cita informativa
- Si el usuario acepta la llamada, pide en un solo mensaje breve solo los datos necesarios para agendar
- Datos vitales: nombre y telefono
- Datos de agenda: mejor dia para llamar y mejor hora para llamar
- No pidas direccion, ciudad, ocupacion, garantia ni otras preguntas de calificacion en esta fase
- Si despues de ese mensaje aun falta un dato, pide solo el dato faltante
- Si aun no aceptan llamada, no pidas toda la ficha completa
- Cuando invites a llamada, hazlo suave y natural
- Mejor di: "Si quieres, te puedo ayudar a que te llamen y te expliquen bien."
- Evita sonar agresivo o insistente

COCINA:
- Guias a las personas para cocinar saludable y usar Royal Prestige con confianza
- Prioriza recetas practicas, saludables y faciles de replicar
- Cuando sea util, conecta la receta con beneficios simples como menos grasa, mejor coccion, practicidad y facilidad
- Explica como usar el producto sin complicar a la persona
- Si preguntan por diabetes, colesterol o presion alta, responde como guia de cocina y alimentacion general, no como medico
- Puedes hablar de porciones, menos sodio, menos grasa saturada, menos azucar y comida casera mas balanceada
- Si hace falta, di algo corto como: "Si ya lleva plan de su doctor o nutriologa, siga ese plan"
- Nunca digas que un producto, jugo o receta cura enfermedades o reemplaza medicinas

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
- Ayudar a meter orden, pedir documentos correctos y no perder ventas por proceso mal hecho despues del cierre

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
- Si la pregunta es sobre orden, DocuCite, documentos o aprobacion, di el paso exacto que sigue y que revisar
- Si la pregunta es sobre reclutamiento, habla claro y sin exagerar
- Si ya sabes en que momento de la demo va, usa ese contexto para dar la accion que sigue
- Si el mejor movimiento es cerrar, cierra
- Si el mejor movimiento es callar y amarrar, dilo sin rodeos
- Si el cliente dice "lo voy a pensar", no te vayas por defecto al cierre de 3 o 15 dias
- En ese caso, primero busca la objecion real con una pregunta corta y clara
- Solo usa el cierre de 3 o 15 dias si el distribuidor te pide Alex Dey o Benjamin Franklin, o si deja claro que ya sabe continuar con papeleria y devolucion
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
  Di esto: "Claro. ?Que parte quiere pensar bien, el producto o el pago?"
  Siguiente: "No salgas de la objecion madre hasta sacar la razon real."
- Ejemplo bueno:
  Di esto: "?Que le queda mejor, sabado o domingo?"
  Siguiente: "No abras mas opciones. Deja solo esas dos."
- Ejemplo bueno:
  Di esto: "Bienvenido a Royal Prestige, me facilita su ID."
  Siguiente: "Si te da el ID, sigue con la orden. Si te objeta, esa ya es la objecion real."
- Ejemplo bueno:
  Di esto: "Perfecto, ahora vamos a meter su orden y subir sus documentos bien claritos."
  Siguiente: "Confirma si vas en pedido nuevo, anexar documentos o aprobacion."

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
- Si aparece Alex Dey o Benjamin Franklin en el contexto, no los conviertas en respuesta automatica por reflejo.
- Primero piensa si ese cierre de verdad le conviene al distribuidor que esta preguntando.
- Si ves riesgo de que el novato lo use mal, dale una version mas simple, mas segura y mas facil de repetir.
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

function limpiarUrlExterna(value = "") {
  const text = String(value || "").trim();
  return /^https?:\/\//i.test(text) ? text : "";
}

function construirWhatsAppUrl(phone = "", text = "") {
  const phoneNormalizado = String(phone || "").replace(/\D+/g, "");

  if (!phoneNormalizado) {
    return "";
  }

  const message = String(text || "").trim();
  const baseUrl = `https://wa.me/${phoneNormalizado}`;

  if (!message) {
    return baseUrl;
  }

  return `${baseUrl}?text=${encodeURIComponent(message)}`;
}

function escapeXml(value = "") {
  return String(value || "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&apos;");
}

function construirTwilioMessageResponse(message = "") {
  const body = cleanText(String(message || "No pude responder en este momento.")).slice(0, 1590);
  return `<?xml version="1.0" encoding="UTF-8"?><Response><Message><Body>${escapeXml(body)}</Body></Message></Response>`;
}

async function sembrarLeadYPerfilCanal({ sessionId, visitorId, phoneHint = "", nameHint = "" }) {
  const phone = normalizePhone(phoneHint);
  const name = cleanText(nameHint);
  let leadDoc = await resolverLeadExistente(sessionId, visitorId, "", phone);
  let profileDoc = await resolverPerfilExistente(visitorId, "", phone, leadDoc?._id || null);

  if (!leadDoc && phone) {
    leadDoc = await Lead.create({
      visitorIds: [visitorId],
      sessionIds: [sessionId],
      phone,
      name: name || undefined,
      leadStatus: "interesado",
      lastInteractionAt: new Date(),
      updatedAt: new Date()
    });
  } else if (leadDoc) {
    let dirty = false;
    const visitorIdsActualizados = combinarListas(leadDoc.visitorIds || [], [visitorId]);
    const sessionIdsActualizados = combinarListas(leadDoc.sessionIds || [], [sessionId]);

    if (visitorIdsActualizados.length !== (leadDoc.visitorIds || []).length) {
      leadDoc.visitorIds = visitorIdsActualizados;
      dirty = true;
    }

    if (sessionIdsActualizados.length !== (leadDoc.sessionIds || []).length) {
      leadDoc.sessionIds = sessionIdsActualizados;
      dirty = true;
    }

    if (phone && !leadDoc.phone) {
      leadDoc.phone = phone;
      dirty = true;
    }

    if (name && !leadDoc.name) {
      leadDoc.name = name;
      dirty = true;
    }

    if (dirty) {
      leadDoc.updatedAt = new Date();
      leadDoc.lastInteractionAt = new Date();
      await leadDoc.save();
    }
  }

  if (!profileDoc) {
    profileDoc = await Profile.create({
      visitorId,
      visitorIds: [visitorId],
      sessionIds: [sessionId],
      leadId: leadDoc?._id || undefined,
      phone: phone || undefined,
      name: name || undefined,
      profileSummary: "",
      recentHistory: [],
      conversationCount: 0,
      updatedAt: new Date()
    });
  } else {
    let dirty = false;
    const visitorIdsActualizados = combinarListas(profileDoc.visitorIds || [], [visitorId]);
    const sessionIdsActualizados = combinarListas(profileDoc.sessionIds || [], [sessionId]);

    if (visitorIdsActualizados.length !== (profileDoc.visitorIds || []).length) {
      profileDoc.visitorIds = visitorIdsActualizados;
      dirty = true;
    }

    if (sessionIdsActualizados.length !== (profileDoc.sessionIds || []).length) {
      profileDoc.sessionIds = sessionIdsActualizados;
      dirty = true;
    }

    if (leadDoc?._id && !profileDoc.leadId) {
      profileDoc.leadId = leadDoc._id;
      dirty = true;
    }

    if (phone && !profileDoc.phone) {
      profileDoc.phone = phone;
      dirty = true;
    }

    if (name && !profileDoc.name) {
      profileDoc.name = name;
      dirty = true;
    }

    if (dirty) {
      profileDoc.updatedAt = new Date();
      await profileDoc.save();
    }
  }

  return { leadDoc, profileDoc };
}

async function procesarChatChefCanal({
  pregunta,
  sessionId,
  visitorId,
  phoneHint = "",
  nameHint = "",
  source = "web"
}) {
  const preguntaLimpia = cleanText(pregunta);
  const visitorIdLimpio = cleanText(visitorId || sessionId);
  const fastChannel = /^whatsapp/i.test(source);

  if (!preguntaLimpia) {
    return { ok: false, status: 400, error: "pregunta requerida" };
  }

  if (!sessionId) {
    return { ok: false, status: 400, error: "sessionId requerido" };
  }

  const chefUsage = await validarLimiteUsoChef(visitorIdLimpio);

  if (!chefUsage.allowed) {
    return {
      ok: false,
      status: 429,
      error: `Por hoy ya usaste tus ${CHEF_MAX_MESSAGES_PER_DAY} mensajes gratis. Manana se reinicia tu acceso.`
    };
  }

  marcarSesionActiva(sessionId);
  limpiarMemoriaSesiones();

  try {
    const modoChat = "chef";
    const modoPrompt = construirContextoModoPrompt(modoChat);
    const modoPromptCanal = fastChannel
      ? `
CANAL ACTIVO:
- canal: whatsapp
- prioridad: velocidad y claridad
- respuesta_maxima: 3 oraciones cortas o lista breve
- evita respuestas largas, adornos y explicaciones extensas
- si hace falta, primero responde lo mas util y practico
`
      : "";
    const { leadDoc: leadInicial, profileDoc: profileInicial } = await sembrarLeadYPerfilCanal({
      sessionId,
      visitorId: visitorIdLimpio,
      phoneHint,
      nameHint
    });

    hidratarEstadoConversacion(sessionId, profileInicial, leadInicial);
    actualizarEstadoConversacion(sessionId, preguntaLimpia);
    const estadoActual = obtenerEstadoConversacion(sessionId);

    let leadGuardado = await guardarLeadSiExiste(preguntaLimpia, sessionId, visitorIdLimpio, estadoActual);

    if (leadGuardado && (phoneHint || nameHint)) {
      let dirtyLead = false;
      const phoneNormalizado = normalizePhone(phoneHint);

      if (phoneNormalizado && !leadGuardado.phone) {
        leadGuardado.phone = phoneNormalizado;
        dirtyLead = true;
      }

      if (cleanText(nameHint) && !leadGuardado.name) {
        leadGuardado.name = cleanText(nameHint);
        dirtyLead = true;
      }

      if (dirtyLead) {
        leadGuardado.updatedAt = new Date();
        await leadGuardado.save();
      }
    }

    const estadoConLead = actualizarEstadoConversacion(sessionId, preguntaLimpia, leadGuardado || leadInicial);
    let profileGuardado = await guardarOActualizarPerfil({
      visitorId: visitorIdLimpio,
      sessionId,
      texto: preguntaLimpia,
      estadoConversacion: estadoConLead,
      leadGuardado: leadGuardado || leadInicial
    });

    if (profileGuardado && (phoneHint || nameHint)) {
      let dirtyProfile = false;
      const phoneNormalizado = normalizePhone(phoneHint);

      if (phoneNormalizado && !profileGuardado.phone) {
        profileGuardado.phone = phoneNormalizado;
        dirtyProfile = true;
      }

      if (cleanText(nameHint) && !profileGuardado.name) {
        profileGuardado.name = cleanText(nameHint);
        dirtyProfile = true;
      }

      if (dirtyProfile) {
        profileGuardado.updatedAt = new Date();
        await profileGuardado.save();
      }
    }

    await guardarMensajeRaw({
      visitorId: visitorIdLimpio,
      sessionId,
      profileId: profileGuardado?._id || profileInicial?._id || null,
      leadId: leadGuardado?._id || leadInicial?._id || null,
      role: "user",
      content: preguntaLimpia,
      intent: "chef_chat",
      estadoConversacion: estadoConLead,
      detectedTopics: combinarListas(extraerTemasInteres(preguntaLimpia), [`canal:${source}`])
    });

    const estadoPrompt = construirEstadoPrompt(sessionId);
    let perfilPrompt = construirPerfilHistoricoPrompt(profileGuardado || profileInicial, leadGuardado || leadInicial);
    const leadMemory = await obtenerMemoriaLeadRelacionada({
      question: preguntaLimpia,
      mode: "chef",
      leadDoc: leadGuardado || leadInicial,
      profileDoc: profileGuardado || profileInicial
    });
    const activeLeadContext = leadMemory?.leadContext || null;
    perfilPrompt += construirPromptMemoriaLead(activeLeadContext, "chef");

    const contexto = fastChannel
      ? construirContextoEstaticoChef(preguntaLimpia)
      : await construirContexto(preguntaLimpia, modoChat);
    registrarMensajeMemoria(sessionId, "user", preguntaLimpia);
    const historialPrompt = await obtenerHistorialConversacionPrompt(sessionId, fastChannel ? 4 : MAX_PROMPT_HISTORY_MESSAGES);

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
          { role: "system", content: modoPromptCanal },
          { role: "system", content: contexto },
          { role: "system", content: estadoPrompt },
          { role: "system", content: perfilPrompt },
          ...historialPrompt
        ]
      })
    });

    const data = await response.json().catch(() => null);

    if (!response.ok) {
      return {
        ok: false,
        status: response.status,
        error: "Error OpenAI",
        detalle: data || { message: "Respuesta invalida del API" }
      };
    }

    if (!data?.choices?.[0]?.message) {
      return { ok: false, status: 500, error: "Error OpenAI", detalle: data };
    }

    const respuestaIA = data.choices[0].message;
    registrarMensajeMemoria(sessionId, respuestaIA.role, respuestaIA.content);

    const leadFinal = await guardarRespuestaIAEnPerfil(leadGuardado || leadInicial, respuestaIA.content);
    const profileFinal = await guardarRespuestaIAEnProfile(
      profileGuardado || profileInicial,
      respuestaIA.content,
      leadFinal
    );

    await guardarMensajeRaw({
      visitorId: visitorIdLimpio,
      sessionId,
      profileId: profileFinal?._id || profileGuardado?._id || profileInicial?._id || null,
      leadId: leadFinal?._id || leadGuardado?._id || leadInicial?._id || null,
      role: "assistant",
      content: respuestaIA.content,
      intent: "chef_chat",
      estadoConversacion: obtenerEstadoConversacion(sessionId),
      detectedTopics: [`canal:${source}`]
    });

    if (!fastChannel) {
      await sincronizarLeadAGoogleSheets(leadFinal?.phone ? leadFinal : null, profileFinal);
    }

    return {
      ok: true,
      status: 200,
      respuesta: respuestaIA.content,
      mode: "chef",
      activeLeadContext
    };
  } catch (error) {
    console.error(error);
    return { ok: false, status: 500, error: "Error servidor" };
  }
}

function construirContextoEstaticoChef(pregunta) {
  const preguntaNormalizada = pregunta.toLowerCase();
  let contexto = "";

  if (detectarConsultaPrecio(preguntaNormalizada)) {
    contexto += `
GUIA DE PRECIOS PARA CHEF:
- no compartas precios, mensualidades ni cotizaciones exactas
- si la persona quiere precio, promo o cuanto cuesta, recomienda hablar con un distribuidor autorizado
- si hay agenda disponible, puedes invitar a una llamada informativa
`;
  }

  if (
    /producto|olla|ollas|sarten|cuchillo|extractor|licuadora|blender|filtro|innove|novel|easy release|fresca(flow|pure)|royal prestige|palomitas|perfect pop|hervidor|juicer/i.test(
      preguntaNormalizada
    )
  ) {
    contexto += construirBloquePrompt("CARACTERISTICAS", beneficiosProductos, {
      maxArrayItems: 10,
      maxObjectKeys: 8,
      maxDepth: 2,
      maxStringLength: 150
    });
  }

  if (/encuesta|salud|tiempo|dinero|laboratorio|hogar/i.test(preguntaNormalizada)) {
    contexto += construirBloquePrompt("ENCUESTA", encuestaVentas, {
      maxArrayItems: 8,
      maxObjectKeys: 8,
      maxDepth: 2,
      maxStringLength: 150
    });
  }

  if (
    /receta|cocinar|pollo|carne|res|pescado|salmon|huevo|pancake|hotcake|panqueque|sopa|arroz|pasta|verdura|ensalada|desayuno|comida|cena|saludable|diabetes|glucosa|az[uú]car|colesterol|presi[oó]n|hipertensi[oó]n|sodio|grasa saturada|pozole|enchilada|enchiladas|frijol|frijoles|tamal|tamales|agua fresca/i.test(
      preguntaNormalizada
    )
  ) {
    contexto += construirBloquePrompt("RECETAS", recetasRoyalPrestige, {
      maxArrayItems: 12,
      maxObjectKeys: 8,
      maxDepth: 2,
      maxStringLength: 160
    });
  }

  if (
    /saludable|diabetes|glucosa|az[uú]car|colesterol|presi[oó]n|hipertensi[oó]n|sodio|grasa saturada|corazon|cardio|plato|porci[oó]n|mexicana|pozole|enchilada|enchiladas|frijol|frijoles|tamal|tamales|antojito/i.test(
      preguntaNormalizada
    )
  ) {
    contexto += construirBloquePrompt("COCINA MEXICANA SALUDABLE", chefCocinaMexicanaSaludable, {
      maxArrayItems: 10,
      maxObjectKeys: 8,
      maxDepth: 2,
      maxStringLength: 170
    });
  }

  if (
    preguntaNormalizada.includes("garantia") ||
    preguntaNormalizada.includes("material") ||
    /olla|ollas|sarten|cuchillo|santoku|easy release|paellera|vaporera|royal prestige|innove|perfect pop|palomitas|popcorn|colador|hervidor|extractor|juicer|licuadora|blender|precision cook|fresca(flow|pure)|salad machine|filtracion|air filtration/i.test(
      preguntaNormalizada
    )
  ) {
    contexto += construirBloquePrompt("ESPECIFICACIONES", especificacionesRoyalPrestige, {
      maxArrayItems: 10,
      maxObjectKeys: 8,
      maxDepth: 2,
      maxStringLength: 150
    });
  }

  if (
    /agua|grifo|llave|tap water|ewg|plomo|cloro|contaminante|filtraci[oó]n|filtro|purificador|cartucho|ccr|consumer confidence/i.test(
      preguntaNormalizada
    )
  ) {
    contexto += construirBloquePrompt("AGUA Y FILTRACION", chefFiltracionAguaEwg, {
      maxArrayItems: 6,
      maxObjectKeys: 8,
      maxDepth: 2,
      maxStringLength: 170
    });
  }

  if (
    preguntaNormalizada.includes("venta") ||
    preguntaNormalizada.includes("cerrar") ||
    detectarInteresComercial(pregunta)
  ) {
    contexto += construirBloquePrompt("VENTAS", inteligenciaVentas, {
      maxArrayItems: 3,
      maxObjectKeys: 8,
      maxDepth: 2,
      maxStringLength: 160
    });
    contexto += construirBloquePrompt("DEMO", demoVenta, {
      maxArrayItems: 6,
      maxObjectKeys: 8,
      maxDepth: 2,
      maxStringLength: 160
    });
    contexto += construirBloquePrompt("CIERRES", cierresAlexDey, {
      maxArrayItems: 6,
      maxObjectKeys: 8,
      maxDepth: 2,
      maxStringLength: 150
    });
  }

  if (preguntaNormalizada.includes("equipo") || preguntaNormalizada.includes("reclutar")) {
    contexto += construirBloquePrompt("RECLUTAMIENTO", reclutamientoCiprian, {
      maxArrayItems: 6,
      maxObjectKeys: 8,
      maxDepth: 2,
      maxStringLength: 150
    });
  }

  if (
    /inversion|propiedad|propiedades|redfin|roi|renta|cashflow|cash flow|house hack|flip|flipping|cap rate|caprate|hipoteca|mortgage|rental/i.test(
      preguntaNormalizada
    )
  ) {
    contexto += `\nPROPIEDADES:\n${JSON.stringify(redfinProperties)}`;
  }

  return contexto;
}

function construirContextoEstaticoCoach(pregunta) {
  const preguntaNormalizada = pregunta.toLowerCase();
  const contextoBase = [];
  const temaCoach = detectarTemaCoach(preguntaNormalizada);
  const preciosRelevantesCoach = temaCoach.precio ? construirPreciosRelevantesCoach(pregunta) : null;

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
- Si es orden, DocuCite o documentos despues del cierre, prioriza la base publica de DocuCite y papeleria del manual
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
- no uses costos internos, piezas sueltas ni listas de partes para cotizar productos
`);
    contextoBase.push(
      construirBloquePrompt("PRECIOS_RELEVANTES", preciosRelevantesCoach?.coincidencias || [], {
        maxArrayItems: 5,
        maxObjectKeys: 8,
        maxDepth: 2,
        maxStringLength: 140
      })
    );
    contextoBase.push(`
GUIA_PRECIO_RELEVANTE:
${preciosRelevantesCoach?.guidance || "- Si no encuentras el producto exacto, pide codigo o nombre exacto y no inventes precio."}
`);
    contextoBase.push(
      construirBloquePrompt("PAGOS", opcionesPagoRoyalPrestige, {
        maxArrayItems: 8,
        maxObjectKeys: 8,
        maxDepth: 2,
        maxStringLength: 140
      })
    );
    contextoBase.push(`
CALCULOS DETECTADOS:
${construirCalculosPreciosCoach(pregunta)}
`);
  }

  if (temaCoach.producto || temaCoach.demo) {
    contextoBase.push(
      construirBloquePrompt("PRODUCTO Y BENEFICIOS", beneficiosProductos, {
        maxArrayItems: 10,
        maxObjectKeys: 8,
        maxDepth: 2,
        maxStringLength: 150
      })
    );
    contextoBase.push(
      construirBloquePrompt("ESPECIFICACIONES", especificacionesRoyalPrestige, {
        maxArrayItems: 10,
        maxObjectKeys: 8,
        maxDepth: 2,
        maxStringLength: 150
      })
    );
  }

  if (
    temaCoach.precio ||
    temaCoach.demo ||
    temaCoach.objecion ||
    temaCoach.cierre ||
    temaCoach.ordenes ||
    temaCoach.mentalidad ||
    temaCoach.seguimiento ||
    temaCoach.reclutamiento
  ) {
    contextoBase.push(
      construirBloquePrompt("MANUAL DEL NOVATO CURADO", manualNovatoCoach, {
        maxArrayItems: 8,
        maxObjectKeys: 8,
        maxDepth: 2,
        maxStringLength: 160
      })
    );
  }

  if (temaCoach.negocio) {
    contextoBase.push(
      construirBloquePrompt("PLAN DE NEGOCIO 2026", planNegocio2026Coach, {
        maxArrayItems: 8,
        maxObjectKeys: 8,
        maxDepth: 2,
        maxStringLength: 160
      })
    );
  }

  if (temaCoach.demo) {
    contextoBase.push(
      construirBloquePrompt("DEMO", demoVenta, {
        maxArrayItems: 8,
        maxObjectKeys: 8,
        maxDepth: 2,
        maxStringLength: 160
      })
    );
  }

  if (temaCoach.objecion || temaCoach.cierre) {
    contextoBase.push(
      construirBloquePrompt("CIERRES", cierresAlexDey, {
        maxArrayItems: 8,
        maxObjectKeys: 8,
        maxDepth: 2,
        maxStringLength: 150
      })
    );
  }

  if (temaCoach.cierreFinal) {
    contextoBase.push(coachCierreFinalInterno);
  }

  if (temaCoach.ordenes || temaCoach.cierreFinal) {
    contextoBase.push(
      construirBloquePrompt("DOCUCITE Y ORDENES", docuciteOrdenesCoach, {
        maxArrayItems: 8,
        maxObjectKeys: 8,
        maxDepth: 2,
        maxStringLength: 160
      })
    );
  }

  if (temaCoach.mentalidad) {
    contextoBase.push(
      construirBloquePrompt("MENTALIDAD", mentalidadOlmedo, {
        maxArrayItems: 8,
        maxObjectKeys: 8,
        maxDepth: 2,
        maxStringLength: 150
      })
    );
  }

  if (temaCoach.seguimiento) {
    contextoBase.push(
      construirBloquePrompt("SEGUIMIENTO", sistema4Citas, {
        maxArrayItems: 8,
        maxObjectKeys: 8,
        maxDepth: 2,
        maxStringLength: 150
      })
    );
  }

  if (temaCoach.reclutamiento) {
    contextoBase.push(
      construirBloquePrompt("RECLUTAMIENTO", reclutamientoCiprian, {
        maxArrayItems: 8,
        maxObjectKeys: 8,
        maxDepth: 2,
        maxStringLength: 150
      })
    );
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
    !temaCoach.ordenes &&
    !temaCoach.mentalidad &&
    !temaCoach.seguimiento &&
    !temaCoach.reclutamiento
  ) {
    contextoBase.push(
      construirBloquePrompt("BASE GENERAL COACH", beneficiosProductos, {
        maxArrayItems: 8,
        maxObjectKeys: 8,
        maxDepth: 2,
        maxStringLength: 150
      })
    );
    contextoBase.push(
      construirBloquePrompt("CIERRES", cierresAlexDey, {
        maxArrayItems: 6,
        maxObjectKeys: 8,
        maxDepth: 2,
        maxStringLength: 150
      })
    );
    contextoBase.push(
      construirBloquePrompt("MENTALIDAD", mentalidadOlmedo, {
        maxArrayItems: 6,
        maxObjectKeys: 8,
        maxDepth: 2,
        maxStringLength: 150
      })
    );
  }

  return contextoBase.join("\n\n");
}

function detectarTemaCoach(preguntaNormalizada = "") {
  return {
    precio: /precio|precios|cu[aá]nto|cuesta|plan|planes|mensual|diario|financiamiento|pago|pagos|mensualidad|paquete|paquetes|combo|combos|negociar|negociacion|matematica|matematicas|catalogo|total/i.test(
      preguntaNormalizada
    ),
    producto: /producto|extractor|juicer|licuadora|blender|olla|ollas|sarten|cuchillo|santoku|easy release|paellera|vaporera|garantia|material|innove|perfect pop|palomitas|popcorn|colador|hervidor|precision cook|fresca(flow|pure)|salad machine|filtracion|air filtration/i.test(
      preguntaNormalizada
    ),
    demo: /demo|demostraci[oó]n|presentaci[oó]n|mostrar|explicar|presentar|rompe hielo|rompiendo el hielo|cita instant[aá]nea|encuesta|cuestionario|salud|4\s*&\s*14|4 y 14|4 en 14|beneficios|ventajas|caracter[ií]sticas/i.test(
      preguntaNormalizada
    ),
    objecion: /objeci[oó]n|esta caro|caro|muy caro|no me alcanza|no tengo dinero|lo voy a pensar|no tengo tiempo|no estoy segura|no estoy seguro|no me interesa|ya tengo|ya compr[eé]|despu[eé]s/i.test(
      preguntaNormalizada
    ),
    cierre: /cerrar|cierre|amarre|benjamin franklin|doble alternativa|puercoesp[ií]n|rebote|silencio|llamada de cierre|envolvente|compromiso/i.test(
      preguntaNormalizada
    ),
    cierreFinal: /se queda callado|se queda en silencio|se quedo callado|se quedo en silencio|no responde|no me responde|silencio final|momento final|final de la demo|me facilita su id|id|comprobante de domicilio|deposito|orden|asumo la venta/i.test(
      preguntaNormalizada
    ),
    ordenes: /docucite|pedido|pedido nuevo|nuevo pedido|orden|ordenes|meter la orden|subir documentos|anexar documentos|order review|aprobaci[oó]n del pedido|aprobar pedido|procesamiento de pagos|esignature|match|uploaded|doc review|biometric|biometrica|captura|camara|notificaciones|sin conexi[oó]n|offline/i.test(
      preguntaNormalizada
    ),
    mentalidad: /mentalidad|frustrad|desanimad|disciplina|constancia|miedo|seguridad|liderazgo|confianza|actitud/i.test(
      preguntaNormalizada
    ),
    seguimiento: /seguimiento|despu[eé]s de la demo|despu[eé]s de la cita|referid|4 citas|14 d[ií]as|llamar luego|volver a llamar|pr[oó]ximo paso|prospect|prospecci[oó]n|estrella|dad|boleto/i.test(
      preguntaNormalizada
    ),
    reclutamiento: /reclut|equipo|distribuidor|lider|liderazgo de equipo|retener|conservar gente|entrevista|candidato|coach|novato/i.test(
      preguntaNormalizada
    ),
    negocio: /plan de negocio|negocio 2026|iniciativa|iniciativas|blue network|royal network|premier|elite|network leader|network|bono|bonos|distribuidor junior|dj|nivel de precio|nivel 3|nivel 4|liderazgo|recluta|reclutas|compras de reclutas|compras de compania|opcion 1|opcion 2|recuperar nivel/i.test(
      preguntaNormalizada
    )
  };
}

function normalizarTextoBusquedaCoach(value = "") {
  return String(value || "")
    .toLowerCase()
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/[^a-z0-9ñ]+/g, " ")
    .trim();
}

function tokenizarTextoBusquedaCoach(value = "") {
  return normalizarTextoBusquedaCoach(value)
    .split(/\s+/)
    .map(token => token.trim())
    .filter(Boolean);
}

function obtenerCodigoPrecioCatalogo(item = {}) {
  return String(item?.["Codigo Del Producto"] || item?.codigo || item?.code || "")
    .trim()
    .toUpperCase();
}

function obtenerNombrePrecioCatalogo(item = {}) {
  return String(item?.["Nombre Del Producto NOVEL"] || item?.nombre || item?.descripcion || "").trim();
}

function obtenerPrecioBaseCatalogo(item = {}) {
  const precio = Number(item?.Precio ?? item?.precio);
  return Number.isFinite(precio) ? precio : null;
}

function construirPreciosRelevantesCoach(pregunta = "") {
  if (!Array.isArray(preciosCatalogo) || !preciosCatalogo.length) {
    return {
      coincidencias: [],
      guidance: "- No se pudo cargar la lista de catalogo. No inventes precios."
    };
  }

  const stopwords = new Set([
    "precio",
    "precios",
    "cuanto",
    "cuesta",
    "costo",
    "costos",
    "pago",
    "pagos",
    "mensual",
    "mensualidad",
    "mensualidades",
    "plan",
    "planes",
    "catalogo",
    "producto",
    "productos",
    "royal",
    "prestige",
    "quiero",
    "saber",
    "dime",
    "dar",
    "dame",
    "del",
    "de",
    "la",
    "el",
    "los",
    "las",
    "por",
    "para",
    "una",
    "uno",
    "que",
    "con"
  ]);
  const preguntaNormalizada = normalizarTextoBusquedaCoach(pregunta);
  const tokens = tokenizarTextoBusquedaCoach(pregunta).filter(token => /^\d+$/.test(token) || !stopwords.has(token));
  const buscaSet = /\bset\b/.test(preguntaNormalizada);
  const buscaSistema = /\bsistema\b/.test(preguntaNormalizada);
  const buscaTapa = /\btapa\b/.test(preguntaNormalizada);
  const buscaOlla = /\bolla\b/.test(preguntaNormalizada);
  const buscaComal = /\bcomal\b/.test(preguntaNormalizada);
  const buscaSarten = /\bsarten\b/.test(preguntaNormalizada);
  const buscaCuchillo = /\bcuchill(?:o|os)\b/.test(preguntaNormalizada);
  const piezasMatch = preguntaNormalizada.match(/\b(\d+)\s+piezas\b/);
  const piezasBuscadas = piezasMatch ? `${piezasMatch[1]} piezas` : "";

  const coincidencias = preciosCatalogo
    .map((item, index) => {
      const codigo = obtenerCodigoPrecioCatalogo(item);
      const nombre = obtenerNombrePrecioCatalogo(item);
      const precio = obtenerPrecioBaseCatalogo(item);

      if (!codigo || !nombre || !Number.isFinite(precio)) {
        return null;
      }

      const codigoNormalizado = normalizarTextoBusquedaCoach(codigo);
      const nombreNormalizado = normalizarTextoBusquedaCoach(nombre);
      const corpus = `${codigoNormalizado} ${nombreNormalizado}`.trim();
      const tokensProducto = new Set(tokenizarTextoBusquedaCoach(corpus));
      const piezasProductoMatch = nombreNormalizado.match(/\b(\d+)\s+piezas\b/);
      const piezasProducto = piezasProductoMatch ? `${piezasProductoMatch[1]} piezas` : "";
      let score = 0;

      if (codigoNormalizado && preguntaNormalizada.includes(codigoNormalizado)) {
        score += 120;
      }

      if (nombreNormalizado && preguntaNormalizada === nombreNormalizado) {
        score += 100;
      }

      if (nombreNormalizado && preguntaNormalizada.includes(nombreNormalizado) && preguntaNormalizada.length > 5) {
        score += 45;
      }

      if (preguntaNormalizada.includes("chocolatera") && nombreNormalizado.includes("chocolatera")) {
        score += 80;
      }

      if (piezasBuscadas && nombreNormalizado.includes(piezasBuscadas)) {
        score += 35;
      }

      if (buscaSet && /\bset\b/.test(nombreNormalizado)) {
        score += 20;
      }

      if (buscaSet && !/\bset\b/.test(nombreNormalizado) && /\bsistema\b/.test(nombreNormalizado)) {
        score += 14;
      }

      if (buscaSistema && /\bsistema\b/.test(nombreNormalizado)) {
        score += 16;
      }

      for (const token of tokens) {
        if (codigoNormalizado === token) {
          score += 80;
          continue;
        }

        if (tokensProducto.has(token)) {
          score += /^\d+$/.test(token) ? 10 : token.length >= 5 ? 8 : 5;
        }
      }

      if ((buscaSet || buscaSistema || piezasBuscadas) && !buscaTapa && /\btapa\b/.test(nombreNormalizado)) {
        score -= 30;
      }

      if ((buscaSet || buscaSistema || piezasBuscadas) && !buscaOlla && /\bolla\b/.test(nombreNormalizado)) {
        score -= 18;
      }

      if ((buscaSet || buscaSistema || piezasBuscadas) && !buscaComal && /\bcomal\b/.test(nombreNormalizado)) {
        score -= 18;
      }

      if ((buscaSet || buscaSistema || piezasBuscadas) && !buscaSarten && /\bsarten\b/.test(nombreNormalizado)) {
        score -= 18;
      }

      if (piezasBuscadas && piezasProducto && piezasProducto !== piezasBuscadas) {
        score -= 35;
      }

      if (
        (buscaSet || buscaSistema || piezasBuscadas) &&
        !buscaCuchillo &&
        /(?:\bcuchill(?:o|os)\b|\bjuego\b)/.test(nombreNormalizado)
      ) {
        score -= 25;
      }

      if (/folleto|repuesto|kit|manual/i.test(nombreNormalizado)) {
        score -= 25;
      }

      return {
        index,
        codigo,
        nombre,
        precio,
        score
      };
    })
    .filter(item => item && item.score > 0)
    .sort((a, b) => {
      if (b.score !== a.score) {
        return b.score - a.score;
      }

      return a.index - b.index;
    })
    .slice(0, 5)
    .map(item => {
      const calculo = calcularPagoCoach(item.precio);
      return {
        codigo_producto: item.codigo,
        nombre_producto: item.nombre,
        precio_base_catalogo: item.precio,
        total_estimado_coach: calculo.total,
        mensualidad_estimada_coach: calculo.mensualidad
      };
    });

  if (coincidencias.length) {
    return {
      coincidencias,
      guidance:
        "- Usa solo estos productos encontrados del catalogo publico para cotizar. Si ves varias coincidencias parecidas, dilo antes de cotizar una sola. Si el nombre exacto no aparece aqui, pide codigo o nombre exacto y no inventes precio."
    };
  }

  return {
    coincidencias: [],
    guidance: extraerMontosCoach(pregunta).length
      ? "- No se detecto un producto exacto del catalogo. Si ya te dieron una base numerica, usa CALCULOS DETECTADOS y aclara que la base vino del distribuidor."
      : "- No se detecto un producto exacto del catalogo. Pide el codigo o nombre exacto antes de cotizar y no inventes precio."
  };
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

  if (detectarConsultaPrecio(pregunta)) {
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
    const chefCalendlyPrompt = limpiarUrlExterna(CALENDLY_CHEF_URL)
      ? `\nCALENDLY DISPONIBLE:\n- si el usuario quiere una llamada, apoyo humano o agendar, puedes compartir este link exacto: ${limpiarUrlExterna(CALENDLY_CHEF_URL)}\n- no lo fuerces en cada respuesta; usalo solo cuando sea natural\n`
      : "";
    const chefPricingPrompt = `\nPRECIOS Y COTIZACION:\n- no compartas precios, mensualidades ni cotizaciones exactas desde el Chef\n- si preguntan por precio, promociones o cuanto cuesta, explica que para precios es mejor hablar con un distribuidor autorizado\n- cuando convenga, invita a pedir una llamada con un distribuidor autorizado\n`;
    return `
MODO ACTIVO:
- modo: chef
- tipo_usuario: cliente_o_prospecto
- acceso_privado: no
${chefCalendlyPrompt}${chefPricingPrompt}`;
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

    const [profileDoc, analyticsDoc, networkSummary, leadMemory] = await Promise.all([
      CoachDistributorProfile.findOne({ userId: auth.user._id }).lean(),
      CoachDistributorAnalytics.findOne({ userId: auth.user._id }).lean(),
      coachTieneAccesoTotal(auth.user) ? obtenerCoachNetworkSummary() : Promise.resolve(null),
      coachTieneAccesoTotal(auth.user)
        ? obtenerMemoriaLeadRelacionada({
            mode: "coach",
            coachUser: auth.user
          })
        : Promise.resolve({ leadContext: null, repLeadSummary: null })
    ]);

    res.json({
      authenticated: true,
      stripeReady: stripeListoParaCheckout(),
      user: limpiarCoachUser(auth.user),
      profile: limpiarCoachProfile(profileDoc, analyticsDoc),
      networkSummary,
      repLeadSummary: leadMemory?.repLeadSummary || null,
      activeLeadContext: leadMemory?.leadContext || null
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

app.get("/api/chef/stats", async (req, res) => {
  try {
    const stats = await obtenerChefPublicStats();
    res.json(stats);
  } catch (error) {
    console.error("Error obteniendo stats Chef:", error.message);
    res.status(500).json({
      error: "No pude cargar las metricas del Chef."
    });
  }
});

app.get("/api/platform/config", (req, res) => {
  res.json({
    calendly: {
      chefUrl: limpiarUrlExterna(CALENDLY_CHEF_URL),
      coachUrl: limpiarUrlExterna(CALENDLY_COACH_URL),
      chefEnabled: Boolean(limpiarUrlExterna(CALENDLY_CHEF_URL)),
      coachEnabled: Boolean(limpiarUrlExterna(CALENDLY_COACH_URL))
    },
    whatsapp: {
      chefUrl: construirWhatsAppUrl(WHATSAPP_CHEF_NUMBER, WHATSAPP_CHEF_TEXT),
      chefEnabled: Boolean(construirWhatsAppUrl(WHATSAPP_CHEF_NUMBER, WHATSAPP_CHEF_TEXT))
    }
  });
});

app.get("/webhooks/twilio/whatsapp", (req, res) => {
  res.json({
    ok: true,
    mode: "chef",
    sandboxEnabled: TWILIO_WHATSAPP_ENABLED,
    webhookPath: "/webhooks/twilio/whatsapp",
    tokenRequired: Boolean(TWILIO_WHATSAPP_WEBHOOK_TOKEN)
  });
});

app.post("/webhooks/twilio/whatsapp", express.urlencoded({ extended: false }), async (req, res) => {
  if (!TWILIO_WHATSAPP_ENABLED) {
    return res.type("text/xml").send(construirTwilioMessageResponse("El sandbox de WhatsApp todavia no esta activado."));
  }

  if (TWILIO_WHATSAPP_WEBHOOK_TOKEN && cleanText(req.query.token || "") !== TWILIO_WHATSAPP_WEBHOOK_TOKEN) {
    return res.status(403).type("text/plain").send("Webhook no autorizado");
  }

  const body = cleanText(req.body?.Body || "");
  const fromRaw = cleanText(req.body?.From || "");
  const phone = normalizePhone(req.body?.WaId || fromRaw);
  const profileName = cleanText(req.body?.ProfileName || "");

  if (!body) {
    return res.type("text/xml").send(construirTwilioMessageResponse("Recibi tu mensaje vacio. Intenta otra vez con texto."));
  }

  if (!phone) {
    return res
      .type("text/xml")
      .send(construirTwilioMessageResponse("No pude identificar tu numero de WhatsApp. Intenta de nuevo."));
  }

  const sessionId = `wa-session-${phone}`;
  const visitorId = `wa-visitor-${phone}`;
  const resultado = await procesarChatChefCanal({
    pregunta: body,
    sessionId,
    visitorId,
    phoneHint: phone,
    nameHint: profileName,
    source: "whatsapp_sandbox"
  });

  const mensaje = resultado.ok ? resultado.respuesta : resultado.error || "No pude responder en este momento.";
  return res.type("text/xml").send(construirTwilioMessageResponse(mensaje));
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
  let coachUsage = null;
  let chefUsage = null;

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

    coachUsage = await validarLimiteUsoCoach(coachAuth.user);

    if (!coachUsage.allowed) {
      return res.status(429).json({
        error: `Ya llegaste al limite de ${COACH_MAX_MESSAGES_PER_DAY} mensajes por hoy en tu plan individual. Manana se reinicia tu acceso diario.`
      });
    }
  } else {
    chefUsage = await validarLimiteUsoChef(visitorIdLimpio);

    if (!chefUsage.allowed) {
      return res.status(429).json({
        error: `Por hoy ya usaste tus ${CHEF_MAX_MESSAGES_PER_DAY} mensajes gratis. Manana se reinicia tu acceso.`
      });
    }
  }

  marcarSesionActiva(sessionId);
  limpiarMemoriaSesiones();

  try {
    let leadGuardado = null;
    let profileGuardado = null;
    let coachProfileDoc = null;
    let coachAnalyticsDoc = null;
    let repLeadSummary = null;
    let activeLeadContext = null;
    const modoPrompt = construirContextoModoPrompt(modoChat, coachAuth?.user);
    let estadoPrompt = "";
    let perfilPrompt = "";

    if (modoChat === "coach") {
      [coachProfileDoc, coachAnalyticsDoc] = await Promise.all([
        CoachDistributorProfile.findOne({ userId: coachAuth.user._id }).lean(),
        CoachDistributorAnalytics.findOne({ userId: coachAuth.user._id }).lean()
      ]);

      estadoPrompt = `
ESTADO DEL COACH:
- area_privada: si
- tipo_ayuda: objeciones, cierre, precios, pagos, paquetes, negociacion, ordenes y docucite
`;
      perfilPrompt = `
CONTEXTO INTERNO DEL COACH:
- esta conversacion pertenece al area privada del distribuidor
- no capturar lead
- no pedir telefono
- no mandar informacion a Google Sheets
- enfocate en objeciones, seguimiento, demo, cierre, reclutamiento, estrategia y ordenes
${construirContextoPerfilCoachPrompt(coachProfileDoc, coachAnalyticsDoc)}
`;
      const leadMemory = await obtenerMemoriaLeadRelacionada({
        question: preguntaLimpia,
        mode: "coach",
        coachUser: coachAuth.user
      });
      repLeadSummary = leadMemory?.repLeadSummary || null;
      activeLeadContext = leadMemory?.leadContext || null;
      perfilPrompt += construirPromptPipelineRepresentante(repLeadSummary);
      perfilPrompt += construirPromptMemoriaLead(activeLeadContext, "coach");
      await guardarMensajeRaw({
        visitorId: visitorIdLimpio,
        sessionId,
        profileId: null,
        leadId: null,
        role: "user",
        content: preguntaLimpia,
        intent: "coach_chat",
        detectedTopics: [`coach_user:${String(coachAuth.user._id)}`]
      });
    } else {
      const leadPrevio = await resolverLeadExistente(sessionId, visitorIdLimpio, "", "");
      const perfilPrevio = await resolverPerfilExistente(visitorIdLimpio, "", "", leadPrevio?._id || null);
      hidratarEstadoConversacion(sessionId, perfilPrevio, leadPrevio);
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
        intent: "chef_chat",
        estadoConversacion: estadoConLead,
        detectedTopics: extraerTemasInteres(preguntaLimpia)
      });

      estadoPrompt = construirEstadoPrompt(sessionId);
      perfilPrompt = construirPerfilHistoricoPrompt(profileGuardado, leadGuardado);
      const leadMemory = await obtenerMemoriaLeadRelacionada({
        question: preguntaLimpia,
        mode: "chef",
        leadDoc: leadGuardado,
        profileDoc: profileGuardado
      });
      activeLeadContext = leadMemory?.leadContext || null;
      perfilPrompt += construirPromptMemoriaLead(activeLeadContext, "chef");
    }

    const contexto = await construirContexto(preguntaLimpia, modoChat);
    registrarMensajeMemoria(sessionId, "user", preguntaLimpia);
    const historialPrompt = await obtenerHistorialConversacionPrompt(sessionId);

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
          ...historialPrompt
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

    registrarMensajeMemoria(sessionId, respuestaIA.role, respuestaIA.content);

    if (modoChat === "coach") {
      await guardarMensajeRaw({
        visitorId: visitorIdLimpio,
        sessionId,
        profileId: null,
        leadId: null,
        role: "assistant",
        content: respuestaIA.content,
        intent: "coach_chat",
        detectedTopics: [`coach_user:${String(coachAuth.user._id)}`]
      });

      const coachProfileActualizado = await actualizarPerfilYAnalyticsCoach({
        userDoc: coachAuth.user,
        sessionId,
        question: preguntaLimpia,
        reply: respuestaIA.content
      });

      return res.json({
        respuesta: respuestaIA.content,
        mode: modoChat,
        profile: limpiarCoachProfile(coachProfileActualizado?.profile, coachProfileActualizado?.analytics),
        repLeadSummary,
        activeLeadContext,
        usage: {
          usedToday: (coachUsage?.usedToday || 0) + 1,
          remainingToday: Math.max((coachUsage?.remainingToday || COACH_MAX_MESSAGES_PER_DAY) - 1, 0),
          limitPerDay: COACH_MAX_MESSAGES_PER_DAY
        }
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
      intent: "chef_chat",
      estadoConversacion: obtenerEstadoConversacion(sessionId)
    });
    await sincronizarLeadAGoogleSheets(leadFinal?.phone ? leadFinal : null, profileFinal);

    res.json({
      respuesta: respuestaIA.content,
      mode: modoChat,
      activeLeadContext
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
