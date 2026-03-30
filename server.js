import "dotenv/config";
import express from "express";
import cors from "cors";
import crypto from "crypto";
import fs from "fs";
import mongoose from "mongoose";
import path from "path";
import { createClient } from "redis";
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
const PRIVATE_COACH_RESOURCES_DIR = path.join(PRIVATE_DIR, "resources");
const PRIVATE_COACH_PRICE_LIST_FILE = path.join(
  PRIVATE_COACH_RESOURCES_DIR,
  "royal-prestige-alianza-prices-2026.pdf"
);

const conversaciones = {};
const estadosConversacion = {};
const rutasCanalTelefono = {};
const ENABLE_VECTOR_SEARCH = String(process.env.ENABLE_VECTOR_SEARCH || "").toLowerCase() === "true";
const REDIS_URL = String(process.env.REDIS_URL || "").trim();
const stripe = process.env.STRIPE_SECRET_KEY ? new Stripe(process.env.STRIPE_SECRET_KEY) : null;
const STRIPE_PRICE_ID_MONTHLY = process.env.STRIPE_PRICE_ID || "";
const STRIPE_PRICE_ID_ANNUAL = process.env.STRIPE_PRICE_ID_ANNUAL || "";
const COACH_SESSION_COOKIE = "agustin_coach_session";
const COACH_RETURN_SESSION_COOKIE = "agustin_coach_session_return";
const COACH_SESSION_DAYS = 30;
const COACH_RETURN_SESSION_MS = 12 * 60 * 60 * 1000;
const COACH_PASSWORD_MIN = 8;
const COACH_TRIAL_DAYS = Math.max(1, Number(process.env.COACH_TRIAL_DAYS || 7));
const COACH_MAX_ACTIVE_SESSIONS = Math.max(1, Number(process.env.COACH_MAX_ACTIVE_SESSIONS || 2));
const COACH_MAX_MESSAGES_PER_DAY = Math.max(1, Number(process.env.COACH_MAX_MESSAGES_PER_DAY || 100));
const CHEF_MAX_MESSAGES_PER_DAY = Math.max(1, Number(process.env.CHEF_MAX_MESSAGES_PER_DAY || 50));
const TWILIO_ACCOUNT_SID = String(process.env.TWILIO_ACCOUNT_SID || "").trim();
const TWILIO_AUTH_TOKEN = String(process.env.TWILIO_AUTH_TOKEN || "").trim();
const TWILIO_WHATSAPP_ENABLED = String(process.env.TWILIO_WHATSAPP_ENABLED || "").toLowerCase() === "true";
const TWILIO_WHATSAPP_WEBHOOK_TOKEN = String(process.env.TWILIO_WHATSAPP_WEBHOOK_TOKEN || "").trim();
const TWILIO_SMS_ENABLED = String(process.env.TWILIO_SMS_ENABLED || "").toLowerCase() === "true";
const TWILIO_SMS_WEBHOOK_TOKEN = String(process.env.TWILIO_SMS_WEBHOOK_TOKEN || "").trim();
const TWILIO_SMS_AI_REPLY_ENABLED = String(process.env.TWILIO_SMS_AI_REPLY_ENABLED || "").toLowerCase() === "true";
const TWILIO_SMS_FROM = String(process.env.TWILIO_SMS_FROM || "").trim();
const TWILIO_MESSAGING_SERVICE_SID = String(process.env.TWILIO_MESSAGING_SERVICE_SID || "").trim();
const WHATSAPP_CHEF_NUMBER = String(process.env.WHATSAPP_CHEF_NUMBER || "").trim();
const WHATSAPP_CHEF_TEXT = String(process.env.WHATSAPP_CHEF_TEXT || "Hola, quiero ayuda con Agustin 2.0 Chef.").trim();
const WHATSAPP_CHEF_OWNER_EMAIL = parseCoachEmailList(process.env.WHATSAPP_CHEF_OWNER_EMAIL || "")[0] || "";
const CHEF_PUBLIC_OWNER_EMAIL =
  parseCoachEmailList(process.env.CHEF_PUBLIC_OWNER_EMAIL || "")[0] || WHATSAPP_CHEF_OWNER_EMAIL || "";
const CALENDLY_CHEF_URL = String(process.env.CALENDLY_CHEF_URL || "").trim();
const CALENDLY_COACH_URL = String(process.env.CALENDLY_COACH_URL || "").trim();
const RESEND_API_KEY = String(process.env.RESEND_API_KEY || "").trim();
const RESEND_FROM_EMAIL = String(process.env.RESEND_FROM_EMAIL || "").trim();
const HIGHLEVEL_API_BASE_URL = "https://services.leadconnectorhq.com";
const HIGHLEVEL_API_VERSION = String(process.env.HIGHLEVEL_API_VERSION || "2021-07-28").trim() || "2021-07-28";
const HIGHLEVEL_SYNC_ENABLED = String(process.env.HIGHLEVEL_SYNC_ENABLED || "").toLowerCase() === "true";
const HIGHLEVEL_PRIVATE_TOKEN = String(process.env.HIGHLEVEL_PRIVATE_TOKEN || "").trim();
const HIGHLEVEL_LOCATION_ID = String(process.env.HIGHLEVEL_LOCATION_ID || "").trim();
const HIGHLEVEL_PIPELINE_ID = String(process.env.HIGHLEVEL_PIPELINE_ID || "").trim();
const HIGHLEVEL_PIPELINE_STAGE_ID = String(process.env.HIGHLEVEL_PIPELINE_STAGE_ID || "").trim();
const HIGHLEVEL_ASSIGNED_TO_USER_ID = String(process.env.HIGHLEVEL_ASSIGNED_TO_USER_ID || "").trim();
const HIGHLEVEL_WEBHOOK_TOKEN = String(process.env.HIGHLEVEL_WEBHOOK_TOKEN || "").trim();
const AR_PROTOTYPE_PATH_PREFIX = "/prototipo-agustin20-chef-ar";
const AR_PROTOTYPE_UNLOCK_PATH = `${AR_PROTOTYPE_PATH_PREFIX}/unlock`;
const AR_PROTOTYPE_LOCK_PATH = `${AR_PROTOTYPE_PATH_PREFIX}/lock`;
const AR_PROTOTYPE_COOKIE = "agustin_ar_prototype_access";
const AR_PROTOTYPE_PIN = String(process.env.AR_PROTOTYPE_PIN || "1645").trim();
const AR_PROTOTYPE_COOKIE_TTL_MS = 12 * 60 * 60 * 1000;
const COACH_PRIVATE_RESOURCE_MAX_BYTES = 8 * 1024 * 1024;
const COACH_PRIVATE_RESOURCE_SLOTS = {
  catalogo_privado: {
    label: "Catalogo privado",
    defaultFileName: "catalogo-privado.pdf"
  },
  lista_precios_privada: {
    label: "Lista de precios privada",
    defaultFileName: "lista-precios-privada.pdf"
  }
};
const MAX_PROMPT_HISTORY_MESSAGES = Math.max(4, Number(process.env.MAX_PROMPT_HISTORY_MESSAGES || 12));
const COACH_PROMPT_HISTORY_MESSAGES = Math.max(
  4,
  Math.min(MAX_PROMPT_HISTORY_MESSAGES, Number(process.env.COACH_PROMPT_HISTORY_MESSAGES || 8))
);
const COACH_TURBO_MODE = String(process.env.COACH_TURBO_MODE || "true").toLowerCase() !== "false";
const COACH_TURBO_MAX_HISTORY_MESSAGES = Math.max(
  2,
  Math.min(COACH_PROMPT_HISTORY_MESSAGES, Number(process.env.COACH_TURBO_MAX_HISTORY_MESSAGES || 4))
);
const COACH_TURBO_MAX_COMPLETION_TOKENS = Math.max(
  120,
  Number(process.env.COACH_TURBO_MAX_COMPLETION_TOKENS || 220)
);
const MAX_RAM_SESSION_MESSAGES = Math.max(
  MAX_PROMPT_HISTORY_MESSAGES,
  Number(process.env.MAX_RAM_SESSION_MESSAGES || 16)
);
const MAX_RAM_SESSION_STATES = Math.max(100, Number(process.env.MAX_RAM_SESSION_STATES || 500));
const RAM_SESSION_TTL_MS = Math.max(60 * 60 * 1000, Number(process.env.RAM_SESSION_TTL_MS || 6 * 60 * 60 * 1000));
const REDIS_SESSION_TTL_SECONDS = Math.max(
  60 * 60,
  Number(process.env.REDIS_SESSION_TTL_SECONDS || Math.floor(RAM_SESSION_TTL_MS / 1000) || 6 * 60 * 60)
);
const REDIS_AI_CACHE_TTL_SECONDS = Math.max(
  10 * 60,
  Number(process.env.REDIS_AI_CACHE_TTL_SECONDS || 6 * 60 * 60)
);
const COACH_TEST_ACCESS_EMAILS = parseCoachEmailList(process.env.COACH_TEST_ACCESS_EMAILS || "");
const CONTROL_TOWER_ACCESS_EMAILS = parseCoachEmailList(process.env.CONTROL_TOWER_ACCESS_EMAILS || "");
const CONTROL_TOWER_TRACKED_EMAILS = parseCoachEmailList(process.env.CONTROL_TOWER_TRACKED_EMAILS || "");
const COACH_ACCOUNT_PRESETS_JSON = String(process.env.COACH_ACCOUNT_PRESETS_JSON || "").trim();
const COACH_DEFAULT_SPONSOR_EMAIL = parseCoachEmailList(process.env.COACH_DEFAULT_SPONSOR_EMAIL || "")[0] || "";
const COACH_DEFAULT_SPONSOR_RATE = Number(process.env.COACH_DEFAULT_SPONSOR_RATE || 0) || 0;
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
  coachOwnerUserId: { type: mongoose.Schema.Types.ObjectId, ref: "CoachUser", index: true, default: null },
  coachOwnerEmail: { type: String, index: true },
  coachOwnerName: String,
  generatedByUserId: { type: mongoose.Schema.Types.ObjectId, ref: "CoachUser", index: true, default: null },
  generatedByName: String,
  generatedByAccountType: String,
  chefSlug: { type: String, index: true },
  coachInboxLeadId: { type: mongoose.Schema.Types.ObjectId, ref: "CoachLeadInbox", default: null },
  coachInboxLastSyncedAt: Date,
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
  coachOwnerUserId: { type: mongoose.Schema.Types.ObjectId, ref: "CoachUser", index: true, default: null },
  coachOwnerEmail: { type: String, index: true },
  coachOwnerName: String,
  generatedByUserId: { type: mongoose.Schema.Types.ObjectId, ref: "CoachUser", index: true, default: null },
  generatedByName: String,
  generatedByAccountType: String,
  chefSlug: { type: String, index: true },
  coachInboxLeadId: { type: mongoose.Schema.Types.ObjectId, ref: "CoachLeadInbox", default: null },
  coachInboxLastSyncedAt: Date,
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
  coachOwnerUserId: { type: mongoose.Schema.Types.ObjectId, ref: "CoachUser", index: true, default: null },
  generatedByUserId: { type: mongoose.Schema.Types.ObjectId, ref: "CoachUser", index: true, default: null },
  generatedByAccountType: String,
  chefSlug: { type: String, index: true },
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

const coachDemoEventSnapshotSchema = new mongoose.Schema(
  {
    id: String,
    label: String,
    detail: String,
    occurredAt: Date
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
  accountType: { type: String, default: "owner" },
  parentUserId: { type: mongoose.Schema.Types.ObjectId, ref: "CoachUser", index: true, default: null },
  billingOwnerUserId: { type: mongoose.Schema.Types.ObjectId, ref: "CoachUser", index: true, default: null },
  teamOwnerUserId: { type: mongoose.Schema.Types.ObjectId, ref: "CoachUser", index: true, default: null },
  sponsorUserId: { type: mongoose.Schema.Types.ObjectId, ref: "CoachUser", index: true, default: null },
  sponsorName: String,
  sponsorCommissionRate: { type: Number, default: 0 },
  seatStatus: { type: String, default: "active" },
  officeId: { type: String, index: true },
  territoryId: { type: String, index: true },
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
  chefSlug: String,
  chefShareCode: String,
  chefEnabled: { type: Boolean, default: true },
  teamRole: String,
  seatLabel: String,
  leadDestinationType: { type: String, default: "carpeta_privada" },
  leadDestinationLabel: String,
  leadDestinationUrl: String,
  leadDestinationEmail: String,
  leadDestinationUpdatedAt: Date,
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

const coachLeadInboxSchema = new mongoose.Schema({
  ownerUserId: {
    type: mongoose.Schema.Types.ObjectId,
    ref: "CoachUser",
    required: true,
    index: true
  },
  ownerEmail: { type: String, index: true },
  ownerName: String,
  generatedByUserId: {
    type: mongoose.Schema.Types.ObjectId,
    ref: "CoachUser",
    index: true,
    default: null
  },
  generatedByName: String,
  generatedByAccountType: String,
  fullName: { type: String, required: true, trim: true },
  phone: String,
  email: String,
  city: String,
  zipCode: String,
  interest: String,
  source: { type: String, default: "captura_manual" },
  notes: String,
  consentGiven: { type: Boolean, default: false },
  status: { type: String, default: "nuevo" },
  nextAction: String,
  nextActionAt: Date,
  summary: String,
  highLevelContactId: String,
  highLevelOpportunityId: String,
  highLevelLocationId: String,
  highLevelLastSyncAt: Date,
  highLevelLastSyncStatus: String,
  highLevelLastSyncError: String,
  highLevelLastWebhookAt: Date,
  highLevelOpportunityStatus: String,
  highLevelOpportunityStageId: String,
  highLevelOpportunityValue: Number,
  highLevelCampaignId: String,
  highLevelCampaignName: String,
  highLevelCampaignStatus: String,
  highLevelResultSummary: String,
  tags: [String],
  lastContactAt: Date,
  lastStatusChangeAt: Date,
  updatedAt: Date,
  createdAt: { type: Date, default: Date.now }
});

const coachProgramSheetReferralSchema = new mongoose.Schema(
  {
    fullName: String,
    phone: String,
    notes: String,
    createdLeadId: { type: mongoose.Schema.Types.ObjectId, ref: "CoachLeadInbox", default: null },
    instantCallStatus: String,
    instantCallNotes: String,
    appointmentDetails: String,
    selectedForInstantCallAt: Date,
    lastOutcomeAt: Date
  },
  { _id: false }
);

const coachProgramSheetSchema = new mongoose.Schema({
  ownerUserId: {
    type: mongoose.Schema.Types.ObjectId,
    ref: "CoachUser",
    required: true,
    index: true
  },
  ownerEmail: { type: String, index: true },
  ownerName: String,
  generatedByUserId: {
    type: mongoose.Schema.Types.ObjectId,
    ref: "CoachUser",
    index: true,
    default: null
  },
  generatedByName: String,
  generatedByAccountType: String,
  linkedCrmRecordId: {
    type: mongoose.Schema.Types.ObjectId,
    ref: "CoachCrmRecord",
    default: null,
    index: true
  },
  programType: { type: String, default: "4_en_14" },
  hostName: String,
  hostPhone: String,
  giftSelected: String,
  representativeName: String,
  representativePhone: String,
  startWindow: String,
  notes: String,
  referrals: [coachProgramSheetReferralSchema],
  referralCount: { type: Number, default: 0 },
  createdLeadIds: [{ type: mongoose.Schema.Types.ObjectId, ref: "CoachLeadInbox" }],
  summary: String,
  updatedAt: Date,
  createdAt: { type: Date, default: Date.now }
});

const coachHealthSurveySchema = new mongoose.Schema({
  ownerUserId: {
    type: mongoose.Schema.Types.ObjectId,
    ref: "CoachUser",
    required: true,
    index: true
  },
  ownerEmail: { type: String, index: true },
  ownerName: String,
  generatedByUserId: {
    type: mongoose.Schema.Types.ObjectId,
    ref: "CoachUser",
    index: true,
    default: null
  },
  generatedByName: String,
  generatedByAccountType: String,
  linkedCrmRecordId: {
    type: mongoose.Schema.Types.ObjectId,
    ref: "CoachCrmRecord",
    default: null,
    index: true
  },
  fullName: { type: String, required: true, trim: true },
  phone: String,
  secondName: String,
  workingStatus: String,
  heardRoyal: String,
  familyPriority: String,
  qualityReason: String,
  productLikingScore: Number,
  cooksForCount: Number,
  foodSpendWeekly: String,
  mealPrepTime: String,
  cookingMaterials: [String],
  familyConditions: [String],
  lowFatHealthy: String,
  lowFatHealthyReason: String,
  cookwareAffects: String,
  cookwareAffectsReason: String,
  qualityInterest: String,
  qualityInterestReason: String,
  drinkingWaterType: String,
  cookingWaterType: String,
  tapWaterConcern: String,
  waterSpendWeekly: String,
  likesNaturalJuices: String,
  juiceFrequency: String,
  creditProblems: String,
  creditImproveInterest: String,
  familyHealthInvestment: String,
  weeklyBudget: String,
  monthlyBudget: String,
  topProducts: [String],
  summary: String,
  updatedAt: Date,
  createdAt: { type: Date, default: Date.now }
});

const coachRecruitmentApplicationSchema = new mongoose.Schema({
  ownerUserId: {
    type: mongoose.Schema.Types.ObjectId,
    ref: "CoachUser",
    required: true,
    index: true
  },
  ownerEmail: { type: String, index: true },
  ownerName: String,
  generatedByUserId: {
    type: mongoose.Schema.Types.ObjectId,
    ref: "CoachUser",
    index: true,
    default: null
  },
  generatedByName: String,
  generatedByAccountType: String,
  fullName: { type: String, required: true, trim: true },
  phone: String,
  email: String,
  drives: String,
  hasCar: String,
  customerServiceExperience: String,
  workPreference: String,
  salesExperience: String,
  about: String,
  summary: String,
  updatedAt: Date,
  createdAt: { type: Date, default: Date.now }
});

const coachPrivateResourceSchema = new mongoose.Schema({
  ownerUserId: {
    type: mongoose.Schema.Types.ObjectId,
    ref: "CoachUser",
    required: true,
    index: true
  },
  ownerEmail: { type: String, index: true },
  ownerName: String,
  slotType: { type: String, required: true, trim: true },
  fileName: String,
  mimeType: String,
  fileSize: Number,
  pinHash: String,
  fileData: Buffer,
  uploadedAt: Date,
  updatedAt: Date,
  createdAt: { type: Date, default: Date.now }
});

const coachDemoOutcomeSchema = new mongoose.Schema({
  ownerUserId: {
    type: mongoose.Schema.Types.ObjectId,
    ref: "CoachUser",
    required: true,
    index: true
  },
  ownerEmail: { type: String, index: true },
  ownerName: String,
  generatedByUserId: {
    type: mongoose.Schema.Types.ObjectId,
    ref: "CoachUser",
    index: true,
    default: null
  },
  generatedByName: String,
  generatedByAccountType: String,
  linkedCrmRecordId: {
    type: mongoose.Schema.Types.ObjectId,
    ref: "CoachCrmRecord",
    default: null,
    index: true
  },
  sponsorUserId: {
    type: mongoose.Schema.Types.ObjectId,
    ref: "CoachUser",
    index: true,
    default: null
  },
  sponsorName: String,
  sponsorCommissionRate: { type: Number, default: 0 },
  sponsorCommissionAmount: { type: Number, default: 0 },
  reportedByUserId: {
    type: mongoose.Schema.Types.ObjectId,
    ref: "CoachUser",
    default: null
  },
  reportedByName: String,
  resultType: { type: String, default: "follow_up" },
  saleAmount: { type: Number, default: 0 },
  products: [String],
  privateReason: String,
  summary: String,
  activeWorkspace: String,
  activeDemoStage: String,
  activeDemoStageLabel: String,
  surveyId: { type: mongoose.Schema.Types.ObjectId, ref: "CoachHealthSurvey", default: null },
  surveyName: String,
  programSheetId: { type: mongoose.Schema.Types.ObjectId, ref: "CoachProgramSheet", default: null },
  programHostName: String,
  programReferralName: String,
  programReferralIndex: { type: Number, default: -1 },
  orderCalcContext: { type: mongoose.Schema.Types.Mixed, default: null },
  decisionContext: { type: mongoose.Schema.Types.Mixed, default: null },
  demoEvents: [coachDemoEventSnapshotSchema],
  updatedAt: Date,
  createdAt: { type: Date, default: Date.now }
});

const coachTerritorySchema = new mongoose.Schema({
  name: { type: String, required: true, trim: true },
  officeId: { type: String, index: true },
  territoryId: { type: String, index: true },
  createdByUserId: {
    type: mongoose.Schema.Types.ObjectId,
    ref: "CoachUser",
    required: true,
    index: true
  },
  createdByName: String,
  status: { type: String, default: "active" },
  updatedAt: Date,
  createdAt: { type: Date, default: Date.now }
});

const coachTerritoryMembershipSchema = new mongoose.Schema({
  territoryId: {
    type: mongoose.Schema.Types.ObjectId,
    ref: "CoachTerritory",
    required: true,
    index: true
  },
  userId: {
    type: mongoose.Schema.Types.ObjectId,
    ref: "CoachUser",
    required: true,
    index: true
  },
  role: { type: String, default: "member" },
  status: { type: String, default: "active" },
  invitedByUserId: {
    type: mongoose.Schema.Types.ObjectId,
    ref: "CoachUser",
    default: null
  },
  invitedByName: String,
  updatedAt: Date,
  createdAt: { type: Date, default: Date.now }
});

const coachTerritoryInviteSchema = new mongoose.Schema({
  territoryId: {
    type: mongoose.Schema.Types.ObjectId,
    ref: "CoachTerritory",
    required: true,
    index: true
  },
  inviteeEmail: { type: String, required: true, index: true },
  inviteeUserId: {
    type: mongoose.Schema.Types.ObjectId,
    ref: "CoachUser",
    default: null
  },
  role: { type: String, default: "member" },
  status: { type: String, default: "pending" },
  invitedByUserId: {
    type: mongoose.Schema.Types.ObjectId,
    ref: "CoachUser",
    required: true,
    index: true
  },
  invitedByName: String,
  note: String,
  respondedAt: Date,
  updatedAt: Date,
  createdAt: { type: Date, default: Date.now }
});

const coachAnnouncementSchema = new mongoose.Schema({
  authorUserId: {
    type: mongoose.Schema.Types.ObjectId,
    ref: "CoachUser",
    required: true,
    index: true
  },
  authorName: String,
  scopeType: { type: String, default: "global" },
  territoryId: {
    type: mongoose.Schema.Types.ObjectId,
    ref: "CoachTerritory",
    default: null,
    index: true
  },
  territoryName: String,
  teamOwnerUserId: {
    type: mongoose.Schema.Types.ObjectId,
    ref: "CoachUser",
    default: null,
    index: true
  },
  teamOwnerName: String,
  title: { type: String, required: true, trim: true },
  body: { type: String, required: true, trim: true },
  priority: { type: String, default: "normal" },
  status: { type: String, default: "active" },
  updatedAt: Date,
  createdAt: { type: Date, default: Date.now }
});

const coachAnnouncementReceiptSchema = new mongoose.Schema({
  announcementId: {
    type: mongoose.Schema.Types.ObjectId,
    ref: "CoachAnnouncement",
    required: true,
    index: true
  },
  userId: {
    type: mongoose.Schema.Types.ObjectId,
    ref: "CoachUser",
    required: true,
    index: true
  },
  readAt: { type: Date, default: Date.now },
  createdAt: { type: Date, default: Date.now }
});

const coachSupportThreadSchema = new mongoose.Schema({
  ownerUserId: {
    type: mongoose.Schema.Types.ObjectId,
    ref: "CoachUser",
    required: true,
    unique: true,
    index: true
  },
  ownerName: String,
  ownerEmail: String,
  status: { type: String, default: "open" },
  ownerUnreadCount: { type: Number, default: 0 },
  supportUnreadCount: { type: Number, default: 0 },
  lastMessageAt: Date,
  lastMessagePreview: String,
  updatedAt: Date,
  createdAt: { type: Date, default: Date.now }
});

const coachSupportMessageSchema = new mongoose.Schema({
  threadId: {
    type: mongoose.Schema.Types.ObjectId,
    ref: "CoachSupportThread",
    required: true,
    index: true
  },
  senderUserId: {
    type: mongoose.Schema.Types.ObjectId,
    ref: "CoachUser",
    required: true,
    index: true
  },
  senderName: String,
  senderScope: { type: String, default: "coach_user" },
  body: { type: String, required: true, trim: true },
  createdAt: { type: Date, default: Date.now }
});

const coachDirectThreadSchema = new mongoose.Schema({
  participantUserIds: [
    {
      type: mongoose.Schema.Types.ObjectId,
      ref: "CoachUser",
      required: true
    }
  ],
  participantKey: { type: String, required: true, unique: true, index: true },
  unreadByUserIds: [{ type: mongoose.Schema.Types.ObjectId, ref: "CoachUser" }],
  lastMessageAt: Date,
  lastMessagePreview: String,
  updatedAt: Date,
  createdAt: { type: Date, default: Date.now }
});

const coachDirectMessageSchema = new mongoose.Schema({
  threadId: {
    type: mongoose.Schema.Types.ObjectId,
    ref: "CoachDirectThread",
    required: true,
    index: true
  },
  senderUserId: {
    type: mongoose.Schema.Types.ObjectId,
    ref: "CoachUser",
    required: true,
    index: true
  },
  senderName: String,
  body: { type: String, required: true, trim: true },
  createdAt: { type: Date, default: Date.now }
});

const coachCrmRecordSchema = new mongoose.Schema({
  crmRecordId: { type: String, required: true, unique: true, index: true },
  ownerUserId: {
    type: mongoose.Schema.Types.ObjectId,
    ref: "CoachUser",
    required: true,
    index: true
  },
  ownerEmail: { type: String, index: true },
  ownerName: String,
  generatedByUserId: {
    type: mongoose.Schema.Types.ObjectId,
    ref: "CoachUser",
    index: true,
    default: null
  },
  generatedByName: String,
  generatedByAccountType: String,
  sourceType: { type: String, default: "lead", index: true },
  sourceRecordId: { type: String, required: true },
  sourceParentId: { type: String, default: "" },
  sourceSubIndex: { type: Number, default: -1 },
  linkedLeadId: {
    type: mongoose.Schema.Types.ObjectId,
    ref: "CoachLeadInbox",
    default: null,
    index: true
  },
  linkedProgramSheetId: {
    type: mongoose.Schema.Types.ObjectId,
    ref: "CoachProgramSheet",
    default: null,
    index: true
  },
  linkedApplicationId: {
    type: mongoose.Schema.Types.ObjectId,
    ref: "CoachRecruitmentApplication",
    default: null,
    index: true
  },
  linkedHealthSurveyId: {
    type: mongoose.Schema.Types.ObjectId,
    ref: "CoachHealthSurvey",
    default: null,
    index: true
  },
  linkedHealthSurveyName: String,
  latestProgramSheetId: {
    type: mongoose.Schema.Types.ObjectId,
    ref: "CoachProgramSheet",
    default: null,
    index: true
  },
  latestProgramSummary: String,
  latestDemoOutcomeId: {
    type: mongoose.Schema.Types.ObjectId,
    ref: "CoachDemoOutcome",
    default: null,
    index: true
  },
  latestDemoOutcomeType: String,
  latestDemoOutcomeSummary: String,
  officeId: String,
  territoryId: String,
  leadName: String,
  phone: String,
  email: String,
  address: String,
  city: String,
  zipCode: String,
  interest: String,
  status: { type: String, default: "nuevo", index: true },
  statusColor: { type: String, default: "blue" },
  nextAction: String,
  nextActionAt: Date,
  appointmentAt: Date,
  assignedTelemarketerUserId: {
    type: mongoose.Schema.Types.ObjectId,
    ref: "CoachUser",
    default: null,
    index: true
  },
  assignedTelemarketerName: String,
  appointmentRepUserId: {
    type: mongoose.Schema.Types.ObjectId,
    ref: "CoachUser",
    default: null,
    index: true
  },
  appointmentRepName: String,
  briefHistory: String,
  privateNotes: String,
  lastNote: String,
  lastContactAt: Date,
  saleResult: String,
  saleAmount: { type: Number, default: 0 },
  soldAt: Date,
  closedAt: Date,
  sourceUpdatedAt: Date,
  updatedAt: Date,
  createdAt: { type: Date, default: Date.now }
});

const coachCrmActivitySchema = new mongoose.Schema({
  crmRecordId: { type: String, required: true, index: true },
  ownerUserId: {
    type: mongoose.Schema.Types.ObjectId,
    ref: "CoachUser",
    required: true,
    index: true
  },
  actorUserId: {
    type: mongoose.Schema.Types.ObjectId,
    ref: "CoachUser",
    default: null,
    index: true
  },
  actorName: String,
  type: { type: String, default: "note" },
  body: String,
  meta: { type: mongoose.Schema.Types.Mixed, default: null },
  createdAt: { type: Date, default: Date.now }
});

const coachAsyncJobSchema = new mongoose.Schema({
  jobType: { type: String, required: true, index: true },
  dedupeKey: { type: String, default: "", index: true },
  ownerUserId: {
    type: mongoose.Schema.Types.ObjectId,
    ref: "CoachUser",
    default: null,
    index: true
  },
  status: { type: String, default: "pending", index: true },
  attempts: { type: Number, default: 0 },
  maxAttempts: { type: Number, default: 4 },
  nextRunAt: { type: Date, default: Date.now, index: true },
  lockedAt: Date,
  lastAttemptAt: Date,
  completedAt: Date,
  failedAt: Date,
  lastError: String,
  lastResult: { type: mongoose.Schema.Types.Mixed, default: null },
  payload: { type: mongoose.Schema.Types.Mixed, default: null },
  createdAt: { type: Date, default: Date.now },
  updatedAt: { type: Date, default: Date.now }
});

leadSchema.index({ email: 1 });
leadSchema.index({ phone: 1 });
leadSchema.index({ sessionIds: 1 });
leadSchema.index({ visitorIds: 1 });
leadSchema.index({ coachOwnerUserId: 1, chefSlug: 1, createdAt: -1 });
profileSchema.index({ email: 1 });
profileSchema.index({ phone: 1 });
profileSchema.index({ visitorIds: 1 });
profileSchema.index({ leadId: 1 });
profileSchema.index({ coachOwnerUserId: 1, chefSlug: 1, updatedAt: -1 });
messageSchema.index({ visitorId: 1, createdAt: -1 });
messageSchema.index({ sessionId: 1, createdAt: -1 });
messageSchema.index({ intent: 1, role: 1, createdAt: -1 });
messageSchema.index({ coachOwnerUserId: 1, chefSlug: 1, createdAt: -1 });
coachUserSchema.index({ stripeCustomerId: 1 });
coachUserSchema.index({ stripeSubscriptionId: 1 });
coachUserSchema.index({ parentUserId: 1, seatStatus: 1 });
coachUserSchema.index({ teamOwnerUserId: 1, seatStatus: 1 });
coachUserSchema.index({ sponsorUserId: 1, accountType: 1 });
coachDistributorProfileSchema.index({ email: 1 });
coachDistributorProfileSchema.index({ chefSlug: 1 }, { unique: true, sparse: true });
coachDistributorProfileSchema.index({ chefShareCode: 1 }, { unique: true, sparse: true });
coachDistributorAnalyticsSchema.index({ email: 1 });
coachSessionSchema.index({ expiresAt: 1 }, { expireAfterSeconds: 0 });
coachLeadInboxSchema.index({ ownerUserId: 1, createdAt: -1 });
coachLeadInboxSchema.index({ ownerUserId: 1, status: 1, createdAt: -1 });
coachLeadInboxSchema.index({ ownerUserId: 1, generatedByUserId: 1, createdAt: -1 });
coachLeadInboxSchema.index({ ownerUserId: 1, phone: 1 });
coachLeadInboxSchema.index({ ownerUserId: 1, email: 1 });
coachProgramSheetSchema.index({ ownerUserId: 1, programType: 1, createdAt: -1 });
coachProgramSheetSchema.index({ ownerUserId: 1, generatedByUserId: 1, createdAt: -1 });
coachHealthSurveySchema.index({ ownerUserId: 1, updatedAt: -1 });
coachHealthSurveySchema.index({ ownerUserId: 1, generatedByUserId: 1, updatedAt: -1 });
coachHealthSurveySchema.index({ ownerUserId: 1, phone: 1, updatedAt: -1 });
coachRecruitmentApplicationSchema.index({ ownerUserId: 1, updatedAt: -1 });
coachRecruitmentApplicationSchema.index({ ownerUserId: 1, generatedByUserId: 1, updatedAt: -1 });
coachRecruitmentApplicationSchema.index({ ownerUserId: 1, phone: 1, updatedAt: -1 });
coachRecruitmentApplicationSchema.index({ ownerUserId: 1, email: 1, updatedAt: -1 });
coachPrivateResourceSchema.index({ ownerUserId: 1, slotType: 1 }, { unique: true });
coachDemoOutcomeSchema.index({ ownerUserId: 1, createdAt: -1 });
coachDemoOutcomeSchema.index({ ownerUserId: 1, generatedByUserId: 1, createdAt: -1 });
coachDemoOutcomeSchema.index({ ownerUserId: 1, resultType: 1, createdAt: -1 });
coachDemoOutcomeSchema.index({ sponsorUserId: 1, createdAt: -1 });
coachDemoOutcomeSchema.index({ sponsorUserId: 1, resultType: 1, createdAt: -1 });
coachTerritorySchema.index({ createdByUserId: 1, createdAt: -1 });
coachTerritorySchema.index({ officeId: 1, territoryId: 1, createdAt: -1 });
coachTerritoryMembershipSchema.index({ territoryId: 1, userId: 1 }, { unique: true });
coachTerritoryMembershipSchema.index({ userId: 1, status: 1, createdAt: -1 });
coachTerritoryInviteSchema.index({ territoryId: 1, status: 1, createdAt: -1 });
coachTerritoryInviteSchema.index({ inviteeEmail: 1, status: 1, createdAt: -1 });
coachAnnouncementSchema.index({ scopeType: 1, territoryId: 1, createdAt: -1 });
coachAnnouncementSchema.index({ authorUserId: 1, createdAt: -1 });
coachAnnouncementSchema.index({ scopeType: 1, teamOwnerUserId: 1, createdAt: -1 });
coachAnnouncementReceiptSchema.index({ announcementId: 1, userId: 1 }, { unique: true });
coachSupportThreadSchema.index({ status: 1, lastMessageAt: -1 });
coachSupportMessageSchema.index({ threadId: 1, createdAt: -1 });
coachDirectThreadSchema.index({ participantUserIds: 1, lastMessageAt: -1 });
coachDirectMessageSchema.index({ threadId: 1, createdAt: -1 });
coachCrmRecordSchema.index({ ownerUserId: 1, sourceType: 1, sourceRecordId: 1 }, { unique: true });
coachCrmRecordSchema.index({ ownerUserId: 1, status: 1, updatedAt: -1 });
coachCrmRecordSchema.index({ ownerUserId: 1, nextActionAt: 1, updatedAt: -1 });
coachCrmRecordSchema.index({ ownerUserId: 1, assignedTelemarketerUserId: 1, updatedAt: -1 });
coachCrmActivitySchema.index({ crmRecordId: 1, createdAt: -1 });
coachAsyncJobSchema.index({ status: 1, nextRunAt: 1, lockedAt: 1 });
coachAsyncJobSchema.index({ jobType: 1, dedupeKey: 1, status: 1, updatedAt: -1 });

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
const CoachLeadInbox = mongoose.models.CoachLeadInbox || mongoose.model("CoachLeadInbox", coachLeadInboxSchema);
const CoachProgramSheet =
  mongoose.models.CoachProgramSheet || mongoose.model("CoachProgramSheet", coachProgramSheetSchema);
const CoachHealthSurvey =
  mongoose.models.CoachHealthSurvey || mongoose.model("CoachHealthSurvey", coachHealthSurveySchema);
const CoachRecruitmentApplication =
  mongoose.models.CoachRecruitmentApplication ||
  mongoose.model("CoachRecruitmentApplication", coachRecruitmentApplicationSchema);
const CoachPrivateResource =
  mongoose.models.CoachPrivateResource || mongoose.model("CoachPrivateResource", coachPrivateResourceSchema);
const CoachDemoOutcome =
  mongoose.models.CoachDemoOutcome || mongoose.model("CoachDemoOutcome", coachDemoOutcomeSchema);
const CoachTerritory = mongoose.models.CoachTerritory || mongoose.model("CoachTerritory", coachTerritorySchema);
const CoachTerritoryMembership =
  mongoose.models.CoachTerritoryMembership ||
  mongoose.model("CoachTerritoryMembership", coachTerritoryMembershipSchema);
const CoachTerritoryInvite =
  mongoose.models.CoachTerritoryInvite || mongoose.model("CoachTerritoryInvite", coachTerritoryInviteSchema);
const CoachAnnouncement =
  mongoose.models.CoachAnnouncement || mongoose.model("CoachAnnouncement", coachAnnouncementSchema);
const CoachAnnouncementReceipt =
  mongoose.models.CoachAnnouncementReceipt ||
  mongoose.model("CoachAnnouncementReceipt", coachAnnouncementReceiptSchema);
const CoachSupportThread =
  mongoose.models.CoachSupportThread || mongoose.model("CoachSupportThread", coachSupportThreadSchema);
const CoachSupportMessage =
  mongoose.models.CoachSupportMessage || mongoose.model("CoachSupportMessage", coachSupportMessageSchema);
const CoachDirectThread =
  mongoose.models.CoachDirectThread || mongoose.model("CoachDirectThread", coachDirectThreadSchema);
const CoachDirectMessage =
  mongoose.models.CoachDirectMessage || mongoose.model("CoachDirectMessage", coachDirectMessageSchema);
const CoachCrmRecord =
  mongoose.models.CoachCrmRecord || mongoose.model("CoachCrmRecord", coachCrmRecordSchema);
const CoachCrmActivity =
  mongoose.models.CoachCrmActivity || mongoose.model("CoachCrmActivity", coachCrmActivitySchema);
const CoachAsyncJob =
  mongoose.models.CoachAsyncJob || mongoose.model("CoachAsyncJob", coachAsyncJobSchema);

app.post("/webhooks/stripe", express.raw({ type: "application/json" }), manejarWebhookStripe);
app.use(express.json({ limit: "15mb" }));

const COACH_ASYNC_JOB_POLL_MS = 4000;
const COACH_ASYNC_JOB_LOCK_MS = 5 * 60 * 1000;
const COACH_ASYNC_JOB_BATCH_SIZE = 6;
let coachAsyncJobWorkerRunning = false;
let coachAsyncJobWorkerInterval = null;
let coachAsyncJobWakeTimer = null;
let redisClient = null;
let redisReady = false;

function construirRedisSessionKey(type = "", sessionId = "") {
  const safeType = cleanText(type || "")
    .trim()
    .toLowerCase();
  const safeSessionId = cleanText(sessionId || "").trim();

  if (!safeType || !safeSessionId) {
    return "";
  }

  return `agustin:session:${safeType}:${safeSessionId}`;
}

function construirRedisRouteKey(channel = "", phone = "") {
  const routeKey = construirRutaCanalTelefonoKey(channel, phone);

  if (!routeKey) {
    return "";
  }

  return `agustin:route:${routeKey}`;
}

function programarPersistenciaRedis(task, label = "Redis") {
  Promise.resolve()
    .then(task)
    .catch(error => {
      console.error(`Error sincronizando ${label}:`, error.message);
    });
}

async function obtenerRedisClient() {
  if (!REDIS_URL) {
    return null;
  }

  if (!redisClient) {
    redisClient = createClient({
      url: REDIS_URL,
      socket: {
        reconnectStrategy(retries) {
          return Math.min(retries * 250, 3000);
        }
      }
    });

    redisClient.on("ready", () => {
      redisReady = true;
    });
    redisClient.on("end", () => {
      redisReady = false;
    });
    redisClient.on("error", error => {
      redisReady = false;
      console.error("Redis error:", error.message);
    });
  }

  if (!redisClient.isOpen) {
    await redisClient.connect();
  }

  return redisClient;
}

async function iniciarRedis() {
  if (!REDIS_URL) {
    console.log("Redis no configurado");
    return null;
  }

  try {
    const client = await obtenerRedisClient();

    if (client?.isReady || redisReady) {
      console.log("Redis conectado");
    }

    return client;
  } catch (error) {
    redisReady = false;
    console.error("Error conectando Redis:", error.message);
    return null;
  }
}

async function redisGetJson(key = "") {
  if (!key) {
    return null;
  }

  try {
    const client = await obtenerRedisClient();

    if (!client) {
      return null;
    }

    const raw = await client.get(key);
    return raw ? JSON.parse(raw) : null;
  } catch (error) {
    console.error("Error leyendo JSON de Redis:", error.message);
    return null;
  }
}

async function redisSetJson(key = "", value = null, ttlSeconds = REDIS_SESSION_TTL_SECONDS) {
  if (!key || value === undefined) {
    return false;
  }

  try {
    const client = await obtenerRedisClient();

    if (!client) {
      return false;
    }

    const payload = JSON.stringify(value ?? null);

    if (ttlSeconds > 0) {
      await client.set(key, payload, {
        EX: ttlSeconds
      });
    } else {
      await client.set(key, payload);
    }

    return true;
  } catch (error) {
    console.error("Error guardando JSON en Redis:", error.message);
    return false;
  }
}

async function redisDelete(key = "") {
  if (!key) {
    return false;
  }

  try {
    const client = await obtenerRedisClient();

    if (!client) {
      return false;
    }

    await client.del(key);
    return true;
  } catch (error) {
    console.error("Error borrando clave en Redis:", error.message);
    return false;
  }
}

function preguntaEsCacheableParaIA(question = "", mode = "coach") {
  const normalized = normalizarTextoBusquedaCoach(question || "");

  if (!normalized || normalized.length < 6) {
    return false;
  }

  if (/[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}/i.test(question || "")) {
    return false;
  }

  if (/(?:\+?\d[\d()\-\s]{7,}\d)/.test(question || "")) {
    return false;
  }

  if (mode === "chef" && detectarEnvioDatosContacto(question || "")) {
    return false;
  }

  return true;
}

function construirAiCacheSignature(value = null) {
  return crypto.createHash("sha1").update(JSON.stringify(compactarDatoParaPrompt(value, {
    maxDepth: 2,
    maxArrayItems: 4,
    maxObjectKeys: 8,
    maxStringLength: 120
  }))).digest("hex");
}

function construirAiCacheKey({
  mode = "coach",
  scope = "",
  question = "",
  activeWorkspace = "",
  activeDemoStage = "",
  statePrompt = "",
  profilePrompt = "",
  history = [],
  extra = null
} = {}) {
  const normalizedQuestion = normalizarTextoBusquedaCoach(question || "");
  const safeMode = cleanLower(mode || "coach") || "coach";
  const safeScope = cleanText(scope || "").trim() || "public";

  if (!normalizedQuestion) {
    return "";
  }

  const signature = construirAiCacheSignature({
    mode: safeMode,
    scope: safeScope,
    question: normalizedQuestion,
    activeWorkspace: cleanText(activeWorkspace || "").trim(),
    activeDemoStage: cleanText(activeDemoStage || "").trim(),
    statePrompt: truncarTextoPrompt(statePrompt || "", 500),
    profilePrompt: truncarTextoPrompt(profilePrompt || "", 1000),
    history: Array.isArray(history)
      ? history
          .filter(entry => entry?.role && entry?.content)
          .slice(-4)
          .map(entry => ({
            role: entry.role,
            content: truncarTextoPrompt(entry.content || "", 120)
          }))
      : [],
    extra
  });

  return `agustin:ai-cache:${safeMode}:${safeScope}:${signature}`;
}

async function obtenerRespuestaAiCacheada(options = {}) {
  if (!preguntaEsCacheableParaIA(options.question || "", options.mode || "coach")) {
    return null;
  }

  const cacheKey = construirAiCacheKey(options);

  if (!cacheKey) {
    return null;
  }

  const cachedPayload = await redisGetJson(cacheKey);

  if (!cachedPayload?.reply) {
    return null;
  }

  return {
    cacheKey,
    reply: String(cachedPayload.reply || ""),
    createdAt: cachedPayload.createdAt || null
  };
}

async function guardarRespuestaAiCacheada(options = {}, reply = "") {
  if (!reply || !preguntaEsCacheableParaIA(options.question || "", options.mode || "coach")) {
    return false;
  }

  const cacheKey = construirAiCacheKey(options);

  if (!cacheKey) {
    return false;
  }

  return redisSetJson(
    cacheKey,
    {
      reply: String(reply || "").trim().slice(0, 6000),
      createdAt: new Date().toISOString()
    },
    REDIS_AI_CACHE_TTL_SECONDS
  );
}

function normalizarEmail(email = "") {
  return String(email || "").trim().toLowerCase();
}

function parseCoachEmailList(value = "") {
  return String(value || "")
    .split(/[\n,;]+/)
    .map(email => normalizarEmail(email))
    .filter(Boolean);
}

function obtenerCoachAccountPresets() {
  if (!COACH_ACCOUNT_PRESETS_JSON) {
    return [];
  }

  try {
    const raw = JSON.parse(COACH_ACCOUNT_PRESETS_JSON);

    if (!Array.isArray(raw)) {
      return [];
    }

    return raw
      .map(item => {
        const email = normalizarEmail(item?.email || "");

        if (!email) {
          return null;
        }

        return {
          email,
          label: cleanText(item?.label || "").slice(0, 80),
          officeId: cleanText(item?.officeId || "").slice(0, 60),
          territoryId: cleanText(item?.territoryId || "").slice(0, 60),
          teamRole: cleanText(item?.teamRole || "").slice(0, 40),
          seatLabel: cleanText(item?.seatLabel || "").slice(0, 80),
          sponsorEmail: normalizarEmail(item?.sponsorEmail || COACH_DEFAULT_SPONSOR_EMAIL || ""),
          sponsorCommissionRate: Number(item?.sponsorCommissionRate ?? COACH_DEFAULT_SPONSOR_RATE ?? 0) || 0,
          watch: item?.watch === false ? false : true
        };
      })
      .filter(Boolean);
  } catch (error) {
    console.error("Error leyendo COACH_ACCOUNT_PRESETS_JSON:", error.message);
    return [];
  }
}

function obtenerCoachAccountPreset(email = "") {
  const safeEmail = normalizarEmail(email);

  if (!safeEmail) {
    return null;
  }

  return obtenerCoachAccountPresets().find(item => item.email === safeEmail) || null;
}

function obtenerCoachTrackedEmails() {
  const presetEmails = obtenerCoachAccountPresets()
    .filter(item => item.watch !== false)
    .map(item => item.email);

  return Array.from(new Set([...COACH_TEST_ACCESS_EMAILS, ...CONTROL_TOWER_TRACKED_EMAILS, ...presetEmails])).filter(Boolean);
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
  programarPersistenciaRedis(() => persistirActividadSesionRedis(sessionId), "actividad de sesion en Redis");
}

function construirRutaCanalTelefonoKey(channel = "", phone = "") {
  const safeChannel = cleanText(channel || "")
    .trim()
    .toLowerCase();
  const safePhone = normalizePhone(phone || "");

  if (!safeChannel || !safePhone) {
    return "";
  }

  return `${safeChannel}:${safePhone}`;
}

function registrarRutaCanalTelefono(channel = "", phone = "", ownerUserId = "") {
  const routeKey = construirRutaCanalTelefonoKey(channel, phone);
  const safeOwnerUserId = String(ownerUserId || "").trim();

  if (!routeKey || !safeOwnerUserId) {
    return;
  }

  rutasCanalTelefono[routeKey] = {
    ownerUserId: safeOwnerUserId,
    updatedAt: Date.now()
  };
  programarPersistenciaRedis(
    () => persistirRutaCanalTelefonoRedis(channel, phone, safeOwnerUserId),
    "ruta de canal en Redis"
  );
}

function obtenerRutaCanalTelefono(channel = "", phone = "") {
  const routeKey = construirRutaCanalTelefonoKey(channel, phone);

  if (!routeKey) {
    return "";
  }

  const route = rutasCanalTelefono[routeKey];

  return route?.ownerUserId ? String(route.ownerUserId).trim() : "";
}

function construirCanalSessionId(channel = "", phone = "", ownerUserId = "") {
  const safeChannel = cleanText(channel || "")
    .trim()
    .toLowerCase();
  const safePhone = normalizePhone(phone || "");
  const safeOwnerUserId = String(ownerUserId || "public")
    .trim()
    .replace(/[^a-z0-9_-]/gi, "");

  if (!safeChannel || !safePhone) {
    return "";
  }

  return `${safeChannel}-session-${safeOwnerUserId || "public"}-${safePhone}`;
}

function construirCanalVisitorId(channel = "", phone = "", ownerUserId = "") {
  const safeChannel = cleanText(channel || "")
    .trim()
    .toLowerCase();
  const safePhone = normalizePhone(phone || "");
  const safeOwnerUserId = String(ownerUserId || "public")
    .trim()
    .replace(/[^a-z0-9_-]/gi, "");

  if (!safeChannel || !safePhone) {
    return "";
  }

  return `${safeChannel}-visitor-${safeOwnerUserId || "public"}-${safePhone}`;
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

  for (const [routeKey, route] of Object.entries(rutasCanalTelefono)) {
    if (ahora - Number(route?.updatedAt || 0) > RAM_SESSION_TTL_MS) {
      delete rutasCanalTelefono[routeKey];
    }
  }
}

async function persistirActividadSesionRedis(sessionId = "") {
  if (!sessionId) {
    return false;
  }

  return redisSetJson(
    construirRedisSessionKey("activity", sessionId),
    {
      lastSeenAt: Number(actividadSesiones[sessionId] || Date.now())
    },
    REDIS_SESSION_TTL_SECONDS
  );
}

async function persistirHistorialSesionRedis(sessionId = "") {
  if (!sessionId) {
    return false;
  }

  return redisSetJson(
    construirRedisSessionKey("history", sessionId),
    Array.isArray(conversaciones[sessionId]) ? conversaciones[sessionId] : [],
    REDIS_SESSION_TTL_SECONDS
  );
}

async function persistirEstadoSesionRedis(sessionId = "") {
  if (!sessionId) {
    return false;
  }

  return redisSetJson(
    construirRedisSessionKey("state", sessionId),
    estadosConversacion[sessionId] || crearEstadoConversacionBase(),
    REDIS_SESSION_TTL_SECONDS
  );
}

async function asegurarSesionRedisCargada(sessionId = "") {
  if (!sessionId || !REDIS_URL) {
    return false;
  }

  const needActivity = !actividadSesiones[sessionId];
  const needHistory = !Array.isArray(conversaciones[sessionId]) || !conversaciones[sessionId].length;
  const needState = !estadosConversacion[sessionId];

  if (!needActivity && !needHistory && !needState) {
    return false;
  }

  const [activityPayload, historyPayload, statePayload] = await Promise.all([
    needActivity ? redisGetJson(construirRedisSessionKey("activity", sessionId)) : Promise.resolve(null),
    needHistory ? redisGetJson(construirRedisSessionKey("history", sessionId)) : Promise.resolve(null),
    needState ? redisGetJson(construirRedisSessionKey("state", sessionId)) : Promise.resolve(null)
  ]);

  if (needActivity && activityPayload?.lastSeenAt) {
    actividadSesiones[sessionId] = Number(activityPayload.lastSeenAt) || Date.now();
  }

  if (needHistory && Array.isArray(historyPayload) && historyPayload.length) {
    conversaciones[sessionId] = historyPayload
      .filter(entry => entry?.role && entry?.content)
      .slice(-MAX_RAM_SESSION_MESSAGES);
  }

  if (needState && statePayload && typeof statePayload === "object") {
    estadosConversacion[sessionId] = {
      ...crearEstadoConversacionBase(),
      ...statePayload
    };
  }

  return Boolean(activityPayload || historyPayload || statePayload);
}

async function persistirRutaCanalTelefonoRedis(channel = "", phone = "", ownerUserId = "") {
  const routeKey = construirRedisRouteKey(channel, phone);
  const safeOwnerUserId = String(ownerUserId || "").trim();

  if (!routeKey || !safeOwnerUserId) {
    return false;
  }

  return redisSetJson(
    routeKey,
    {
      ownerUserId: safeOwnerUserId,
      updatedAt: Date.now()
    },
    REDIS_SESSION_TTL_SECONDS
  );
}

async function obtenerRutaCanalTelefonoRedis(channel = "", phone = "") {
  const routeKey = construirRedisRouteKey(channel, phone);

  if (!routeKey) {
    return "";
  }

  const payload = await redisGetJson(routeKey);
  return payload?.ownerUserId ? String(payload.ownerUserId).trim() : "";
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
  programarPersistenciaRedis(() => persistirHistorialSesionRedis(sessionId), "historial de sesion en Redis");
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

  programarPersistenciaRedis(() => persistirEstadoSesionRedis(sessionId), "estado de sesion en Redis");
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

function construirAccessoArPrototypeHash() {
  return crypto
    .createHash("sha256")
    .update(`${AR_PROTOTYPE_PIN}|${process.env.AR_PROTOTYPE_PIN_SECRET || "agustin-ar-prototype-v1"}`)
    .digest("hex");
}

function construirRedirectArPrototype(value = "") {
  const input = String(value || "").trim();

  if (!input.startsWith(AR_PROTOTYPE_PATH_PREFIX)) {
    return `${AR_PROTOTYPE_PATH_PREFIX}/`;
  }

  return input;
}

function escaparHtmlLigero(value = "") {
  return String(value || "")
    .replace(/&/g, "&amp;")
    .replace(/"/g, "&quot;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;");
}

function arPrototypeTieneAcceso(req) {
  const cookies = parsearCookies(req.headers?.cookie || "");
  return cookies[AR_PROTOTYPE_COOKIE] === construirAccessoArPrototypeHash();
}

function renderArPrototypeLockPage(req, options = {}) {
  const redirectPath = construirRedirectArPrototype(options.redirectPath || req.originalUrl || `${AR_PROTOTYPE_PATH_PREFIX}/`);
  const showError = Boolean(options.showError);
  const pageTitle = "Acceso privado";
  const actionUrl = `${obtenerBaseUrl(req)}${AR_PROTOTYPE_UNLOCK_PATH}`;
  const lockUrl = `${obtenerBaseUrl(req)}${AR_PROTOTYPE_LOCK_PATH}`;
  const errorHtml = showError
    ? `<div class="lock-error">El PIN no coincide. Intenta otra vez.</div>`
    : `<div class="lock-note">Esta vista privada solo abre con PIN.</div>`;

  return `<!DOCTYPE html>
<html lang="es">
  <head>
    <meta charset="UTF-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1.0" />
    <title>${pageTitle}</title>
    <style>
      :root {
        --navy: #102b5a;
        --navy-deep: #081a38;
        --paper: rgba(255, 255, 255, 0.92);
        --line: rgba(16, 43, 90, 0.12);
        --ink: #15243f;
        --muted: #60728e;
        --red: #c62839;
        --gold: #dcb86a;
        --shadow: 0 30px 60px rgba(9, 26, 56, 0.16);
        --radius: 28px;
      }

      * {
        box-sizing: border-box;
      }

      body {
        margin: 0;
        min-height: 100vh;
        display: grid;
        place-items: center;
        padding: 22px;
        font-family: Avenir, Montserrat, "Helvetica Neue", Arial, sans-serif;
        color: var(--ink);
        background:
          radial-gradient(circle at top left, rgba(198, 40, 57, 0.13), transparent 24%),
          radial-gradient(circle at top right, rgba(16, 43, 90, 0.12), transparent 28%),
          linear-gradient(180deg, #f8fbff 0%, #eef4fb 48%, #f9f6ef 100%);
      }

      .lock-shell {
        width: min(100%, 460px);
        padding: 30px;
        border-radius: var(--radius);
        border: 1px solid var(--line);
        background: var(--paper);
        box-shadow: var(--shadow);
        backdrop-filter: blur(14px);
      }

      .eyebrow {
        display: inline-flex;
        align-items: center;
        padding: 8px 14px;
        border-radius: 999px;
        background: rgba(198, 40, 57, 0.1);
        color: var(--red);
        font-size: 0.78rem;
        font-weight: 800;
        letter-spacing: 0.06em;
        text-transform: uppercase;
      }

      h1 {
        margin: 16px 0 10px;
        color: var(--navy);
        font-size: clamp(2rem, 6vw, 2.8rem);
        line-height: 0.95;
        letter-spacing: -0.04em;
      }

      p {
        margin: 0;
        color: var(--muted);
        line-height: 1.72;
      }

      form {
        margin-top: 22px;
      }

      input {
        width: 100%;
        padding: 16px 18px;
        border-radius: 18px;
        border: 1px solid rgba(16, 43, 90, 0.16);
        font-size: 1.15rem;
        letter-spacing: 0.22em;
        text-align: center;
        color: var(--navy-deep);
        background: rgba(255, 255, 255, 0.92);
      }

      input:focus {
        outline: 2px solid rgba(16, 43, 90, 0.2);
        border-color: rgba(16, 43, 90, 0.28);
      }

      .lock-actions {
        display: flex;
        flex-wrap: wrap;
        gap: 12px;
        margin-top: 16px;
      }

      button {
        flex: 1 1 180px;
        min-height: 52px;
        border: none;
        border-radius: 16px;
        font-weight: 800;
        font-size: 1rem;
        cursor: pointer;
      }

      .primary {
        background: linear-gradient(135deg, var(--navy) 0%, var(--navy-deep) 100%);
        color: #fff;
      }

      .ghost {
        background: rgba(255, 255, 255, 0.82);
        color: var(--navy);
        border: 1px solid rgba(16, 43, 90, 0.14);
      }

      .lock-error,
      .lock-note {
        margin-top: 16px;
        padding: 12px 14px;
        border-radius: 16px;
        font-size: 0.95rem;
      }

      .lock-error {
        background: rgba(198, 40, 57, 0.1);
        color: var(--red);
      }

      .lock-note {
        background: rgba(220, 184, 106, 0.16);
        color: var(--navy);
      }

      .fineprint {
        margin-top: 18px;
        font-size: 0.9rem;
      }
    </style>
  </head>
  <body>
    <main class="lock-shell">
      <div class="eyebrow">Acceso privado</div>
      <h1>Prototipo AR</h1>
      <p>Ingresa el PIN de 4 numeros para abrir esta experiencia privada.</p>
      <form method="post" action="${actionUrl}">
        <input type="hidden" name="redirect" value="${escaparHtmlLigero(redirectPath)}" />
        <input
          type="password"
          name="pin"
          inputmode="numeric"
          autocomplete="one-time-code"
          maxlength="4"
          pattern="[0-9]{4}"
          placeholder="PIN de 4 numeros"
          required
        />
        ${errorHtml}
        <div class="lock-actions">
          <button class="primary" type="submit">Abrir experiencia</button>
          <button class="ghost" type="submit" formaction="${lockUrl}" formmethod="post">Cerrar acceso</button>
        </div>
      </form>
      <p class="fineprint">Si compartes el link, la persona tambien necesitara el PIN para entrar.</p>
    </main>
  </body>
</html>`;
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

function usuarioPuedeVerTorreControl(userDoc = null) {
  if (!userDoc) {
    return false;
  }

  const email = normalizarEmail(userDoc.email);

  if (CONTROL_TOWER_ACCESS_EMAILS.length) {
    return CONTROL_TOWER_ACCESS_EMAILS.includes(email);
  }

  return coachTieneAccesoDePrueba(email);
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

function normalizarCoachAccountType(value = "") {
  const safeValue = String(value || "")
    .trim()
    .toLowerCase();
  const validValues = ["owner", "seat", "leader"];
  return validValues.includes(safeValue) ? safeValue : "owner";
}

function normalizarCoachSeatStatus(value = "") {
  const safeValue = String(value || "")
    .trim()
    .toLowerCase();
  const validValues = ["active", "paused", "promoted", "closed"];
  return validValues.includes(safeValue) ? safeValue : "active";
}

function normalizarCoachTerritoryRole(value = "") {
  const safeValue = String(value || "")
    .trim()
    .toLowerCase()
    .replace(/\s+/g, "_");

  const aliases = {
    distribuidor: "member",
    member: "member",
    miembro: "member",
    distribuidor_junior: "junior",
    junior: "junior",
    manager: "manager",
    coadmin: "manager",
    lider: "manager",
    owner: "owner"
  };

  return aliases[safeValue] || "member";
}

function normalizarCoachTerritoryMembershipStatus(value = "") {
  const safeValue = String(value || "")
    .trim()
    .toLowerCase();
  const validValues = ["active", "paused", "left"];
  return validValues.includes(safeValue) ? safeValue : "active";
}

function normalizarCoachTerritoryInviteStatus(value = "") {
  const safeValue = String(value || "")
    .trim()
    .toLowerCase();
  const validValues = ["pending", "accepted", "rejected", "cancelled"];
  return validValues.includes(safeValue) ? safeValue : "pending";
}

function coachPuedeUsarTerritorios(userDoc = null) {
  if (!userDoc) {
    return false;
  }

  return normalizarCoachAccountType(userDoc.accountType || "owner") !== "seat";
}

function coachTerritoryRolePuedeAdministrar(role = "") {
  return ["owner", "manager"].includes(normalizarCoachTerritoryRole(role));
}

function limpiarCoachTerritoryText(value = "", maxLength = 80) {
  return cleanText(value || "").slice(0, maxLength);
}

function normalizarCoachAnnouncementScopeType(value = "") {
  const safeValue = String(value || "")
    .trim()
    .toLowerCase();
  return ["global", "territory", "team"].includes(safeValue) ? safeValue : "global";
}

function normalizarCoachAnnouncementPriority(value = "") {
  const safeValue = String(value || "")
    .trim()
    .toLowerCase();
  return ["normal", "high"].includes(safeValue) ? safeValue : "normal";
}

function limpiarCoachAnnouncementTitle(value = "") {
  return cleanText(value || "").slice(0, 120);
}

function limpiarCoachAnnouncementBody(value = "") {
  return cleanText(value || "").slice(0, 1200);
}

function limpiarCoachSupportMessageBody(value = "") {
  return cleanText(value || "").slice(0, 1200);
}

function construirCoachDirectParticipantKey(userIdA = "", userIdB = "") {
  return [String(userIdA || "").trim(), String(userIdB || "").trim()]
    .filter(Boolean)
    .sort()
    .join("__");
}

function resolverCoachTeamRoleDefault(userDoc = null) {
  const accountType = normalizarCoachAccountType(userDoc?.accountType || "owner");

  if (accountType === "seat") {
    return "novato";
  }

  if (accountType === "leader") {
    return "lider";
  }

  return "distribuidor";
}

function normalizarCoachTeamRole(value = "", userDoc = null) {
  const safeValue = cleanText(value || "")
    .trim()
    .toLowerCase();
  const validRoles = ["novato", "telemarketing", "distribuidor", "junior", "lider"];
  return validRoles.includes(safeValue) ? safeValue : resolverCoachTeamRoleDefault(userDoc);
}

function resolverCoachPortalMode(userDoc = null, profileDoc = null) {
  const accountType = normalizarCoachAccountType(userDoc?.accountType || "owner");
  const teamRole = normalizarCoachTeamRole(profileDoc?.teamRole || "", userDoc);

  if (accountType === "seat" && teamRole === "telemarketing") {
    return "telemarketing";
  }

  return "default";
}

function construirCoachHomePath(userDoc = null, profileDoc = null) {
  return resolverCoachPortalMode(userDoc, profileDoc) === "telemarketing" ? "/coach/telemarketing/" : "/coach/app/";
}

function normalizarCoachChefSlugSegment(value = "", maxLength = 36) {
  const normalized = String(value || "")
    .toLowerCase()
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/-+/g, "-")
    .replace(/^-|-$/g, "")
    .slice(0, maxLength);

  return normalized || "";
}

function normalizarCoachShareCodeSegment(value = "", maxLength = 24) {
  return String(value || "")
    .toLowerCase()
    .replace(/[^a-f0-9]/g, "")
    .slice(0, maxLength);
}

function construirCoachChefPath(chefSlug = "") {
  const slug = normalizarCoachChefSlugSegment(chefSlug, 48);
  return slug ? `/chef/${slug}/` : "/chef/";
}

function construirCoachChefWhatsAppPath() {
  return "/chef/whatsapp/";
}

function construirCoachContactSharePath(shareCode = "") {
  const safeCode = normalizarCoachShareCodeSegment(shareCode, 24);
  return safeCode ? `/contactos/${safeCode}/` : "/coach/app/";
}

function construirCoachChefTrackingFields(coachChefContext = null) {
  if (!coachChefContext?.ownership?.ownerUserId) {
    return {};
  }

  return {
    coachOwnerUserId: coachChefContext.ownership.ownerUserId,
    coachOwnerEmail: coachChefContext.ownership.ownerEmail || "",
    coachOwnerName: coachChefContext.ownership.ownerName || "",
    generatedByUserId: coachChefContext.ownership.generatedByUserId || null,
    generatedByName: coachChefContext.ownership.generatedByName || "",
    generatedByAccountType: coachChefContext.ownership.generatedByAccountType || "",
    chefSlug: coachChefContext.slug || ""
  };
}

async function resolverCoachChefContextPorSlug(chefSlug = "") {
  const slug = normalizarCoachChefSlugSegment(chefSlug, 48);

  if (!slug) {
    return null;
  }

  const profileDoc = await CoachDistributorProfile.findOne({
    chefSlug: slug,
    chefEnabled: { $ne: false }
  });

  if (!profileDoc?.userId) {
    return null;
  }

  let userDoc = await CoachUser.findById(profileDoc.userId);

  if (!userDoc) {
    return null;
  }

  userDoc = await asegurarCoachUserBase(userDoc);

  if (!(await coachTieneAccesoOperativo(userDoc))) {
    return null;
  }

  const accountType = normalizarCoachAccountType(userDoc.accountType || "owner");
  const seatStatus = normalizarCoachSeatStatus(userDoc.seatStatus || "active");

  if (accountType === "seat" && seatStatus !== "active") {
    return null;
  }

  const ownerProfileDoc = await obtenerCoachOwnerProfile(userDoc);
  const ownership = await construirCoachOwnershipSnapshot(userDoc);

  return {
    slug,
    userDoc,
    profileDoc,
    ownerProfileDoc: ownerProfileDoc || profileDoc,
    ownership
  };
}

async function resolverCoachChefContextPorOwnerEmail(email = "") {
  const safeEmail = normalizarEmail(email);

  if (!safeEmail) {
    return null;
  }

  let userDoc = await CoachUser.findOne({ email: safeEmail });

  if (!userDoc) {
    return null;
  }

  userDoc = await asegurarCoachUserBase(userDoc);

  if (!(await coachTieneAccesoOperativo(userDoc))) {
    return null;
  }

  const accountType = normalizarCoachAccountType(userDoc.accountType || "owner");
  const seatStatus = normalizarCoachSeatStatus(userDoc.seatStatus || "active");

  if (accountType === "seat" && seatStatus !== "active") {
    return null;
  }

  const profileDoc = await CoachDistributorProfile.findOne({ userId: userDoc._id });
  const ownerProfileDoc = await obtenerCoachOwnerProfile(userDoc);
  const effectiveOwnerProfile = ownerProfileDoc || profileDoc;

  if (!effectiveOwnerProfile) {
    return null;
  }

  const ownership = await construirCoachOwnershipSnapshot(userDoc);

  return {
    slug: effectiveOwnerProfile?.chefSlug || profileDoc?.chefSlug || "",
    userDoc,
    profileDoc: profileDoc || effectiveOwnerProfile,
    ownerProfileDoc: effectiveOwnerProfile,
    ownership
  };
}

async function resolverCoachChefContextPorOwnerUserId(userId = "") {
  const safeUserId = String(userId || "").trim();

  if (!safeUserId || !mongoose.Types.ObjectId.isValid(safeUserId)) {
    return null;
  }

  let userDoc = await CoachUser.findById(safeUserId);

  if (!userDoc) {
    return null;
  }

  userDoc = await asegurarCoachUserBase(userDoc);

  if (!(await coachTieneAccesoOperativo(userDoc))) {
    return null;
  }

  const accountType = normalizarCoachAccountType(userDoc.accountType || "owner");
  const seatStatus = normalizarCoachSeatStatus(userDoc.seatStatus || "active");

  if (accountType === "seat" && seatStatus !== "active") {
    return null;
  }

  const profileDoc = await CoachDistributorProfile.findOne({ userId: userDoc._id });
  const ownerProfileDoc = await obtenerCoachOwnerProfile(userDoc);
  const effectiveOwnerProfile = ownerProfileDoc || profileDoc;

  if (!effectiveOwnerProfile) {
    return null;
  }

  const ownership = await construirCoachOwnershipSnapshot(userDoc);

  return {
    slug: effectiveOwnerProfile?.chefSlug || profileDoc?.chefSlug || "",
    userDoc,
    profileDoc: profileDoc || effectiveOwnerProfile,
    ownerProfileDoc: effectiveOwnerProfile,
    ownership
  };
}

async function resolverCoachChefContextPublicoPredeterminado() {
  const candidateEmails = Array.from(
    new Set([
      WHATSAPP_CHEF_OWNER_EMAIL,
      ...CONTROL_TOWER_TRACKED_EMAILS,
      ...CONTROL_TOWER_ACCESS_EMAILS,
      ...COACH_TEST_ACCESS_EMAILS
    ])
  ).filter(Boolean);

  for (const email of candidateEmails) {
    const context = await resolverCoachChefContextPorOwnerEmail(email);

    if (context?.userDoc?._id) {
      return context;
    }
  }

  return null;
}

async function resolverCoachChefLeadRoutingContext(coachChefContext = null) {
  if (!coachChefContext?.ownership?.ownerUserId || !coachChefContext?.userDoc) {
    return null;
  }

  const sharerOwnership = coachChefContext.ownership;
  const fallbackContext = {
    userDoc: coachChefContext.userDoc,
    ownerProfileDoc: coachChefContext.ownerProfileDoc || coachChefContext.profileDoc || null,
    ownership: {
      accountType: sharerOwnership.accountType || "owner",
      ownerUserId: sharerOwnership.ownerUserId,
      ownerName: sharerOwnership.ownerName || "",
      ownerEmail: sharerOwnership.ownerEmail || "",
      generatedByUserId: sharerOwnership.generatedByUserId || coachChefContext.userDoc._id,
      generatedByName: sharerOwnership.generatedByName || coachChefContext.userDoc.name || coachChefContext.userDoc.email || "",
      generatedByAccountType:
        sharerOwnership.generatedByAccountType || sharerOwnership.accountType || coachChefContext.userDoc.accountType || "owner"
    }
  };

  const overrideEmail = normalizarEmail(CHEF_PUBLIC_OWNER_EMAIL || "");

  if (!overrideEmail || overrideEmail === normalizarEmail(fallbackContext.ownership.ownerEmail || "")) {
    return fallbackContext;
  }

  const ownerContext = await resolverCoachChefContextPorOwnerEmail(overrideEmail);

  if (!ownerContext?.ownership?.ownerUserId || !ownerContext?.userDoc) {
    return fallbackContext;
  }

  return {
    userDoc: ownerContext.userDoc,
    ownerProfileDoc: ownerContext.ownerProfileDoc || ownerContext.profileDoc || fallbackContext.ownerProfileDoc,
    ownership: {
      accountType: ownerContext.ownership.accountType || "owner",
      ownerUserId: ownerContext.ownership.ownerUserId,
      ownerName: ownerContext.ownership.ownerName || "",
      ownerEmail: ownerContext.ownership.ownerEmail || "",
      generatedByUserId: fallbackContext.ownership.generatedByUserId,
      generatedByName: fallbackContext.ownership.generatedByName,
      generatedByAccountType: fallbackContext.ownership.generatedByAccountType
    }
  };
}

async function resolverCoachContactShareContextPorCode(shareCode = "") {
  const safeCode = normalizarCoachShareCodeSegment(shareCode, 24);

  if (!safeCode) {
    return null;
  }

  const profileDoc = await CoachDistributorProfile.findOne({
    chefShareCode: safeCode
  });

  if (!profileDoc?.userId) {
    return null;
  }

  let userDoc = await CoachUser.findById(profileDoc.userId);

  if (!userDoc) {
    return null;
  }

  userDoc = await asegurarCoachUserBase(userDoc);

  if (!(await coachTieneAccesoOperativo(userDoc))) {
    return null;
  }

  const accountType = normalizarCoachAccountType(userDoc.accountType || "owner");
  const seatStatus = normalizarCoachSeatStatus(userDoc.seatStatus || "active");

  if (accountType === "seat" && seatStatus !== "active") {
    return null;
  }

  const ownerProfileDoc = await obtenerCoachOwnerProfile(userDoc);
  const ownership = await construirCoachOwnershipSnapshot(userDoc);

  return {
    shareCode: safeCode,
    userDoc,
    profileDoc,
    ownerProfileDoc: ownerProfileDoc || profileDoc,
    ownership
  };
}

function construirCoachChefSlugBase(userDoc = null) {
  const nameBase = normalizarCoachChefSlugSegment(userDoc?.name || "", 28);
  const emailBase = normalizarCoachChefSlugSegment(String(userDoc?.email || "").split("@")[0] || "", 28);
  const idBase = normalizarCoachChefSlugSegment(String(userDoc?._id || "").slice(-8), 12);
  return nameBase || emailBase || (idBase ? `chef-${idBase}` : "chef");
}

async function generarCoachChefSlugDisponible(userDoc = null, excludeProfileId = null) {
  const base = construirCoachChefSlugBase(userDoc);

  for (let attempt = 0; attempt < 8; attempt += 1) {
    const suffix = attempt === 0 ? "" : `-${crypto.randomBytes(2).toString("hex")}`;
    const candidate = normalizarCoachChefSlugSegment(`${base}${suffix}`, 42);
    const query = { chefSlug: candidate };

    if (excludeProfileId) {
      query._id = { $ne: excludeProfileId };
    }

    const existing = await CoachDistributorProfile.findOne(query).select("_id").lean();

    if (!existing) {
      return candidate;
    }
  }

  return normalizarCoachChefSlugSegment(`chef-${crypto.randomBytes(4).toString("hex")}`, 42);
}

async function generarCoachChefShareCodeDisponible(excludeProfileId = null) {
  for (let attempt = 0; attempt < 8; attempt += 1) {
    const candidate = crypto.randomBytes(4).toString("hex");
    const query = { chefShareCode: candidate };

    if (excludeProfileId) {
      query._id = { $ne: excludeProfileId };
    }

    const existing = await CoachDistributorProfile.findOne(query).select("_id").lean();

    if (!existing) {
      return candidate;
    }
  }

  return crypto.randomBytes(6).toString("hex");
}

async function asegurarCoachUserBase(userDoc = null) {
  if (!userDoc?._id) {
    return userDoc;
  }

  let dirty = false;

  if (!userDoc.accountType) {
    userDoc.accountType = "owner";
    dirty = true;
  }

  if (!userDoc.seatStatus) {
    userDoc.seatStatus = "active";
    dirty = true;
  }

  if (!userDoc.billingOwnerUserId) {
    userDoc.billingOwnerUserId = userDoc._id;
    dirty = true;
  }

  if (!userDoc.teamOwnerUserId) {
    userDoc.teamOwnerUserId = userDoc._id;
    dirty = true;
  }

  if (typeof userDoc.sponsorCommissionRate !== "number") {
    userDoc.sponsorCommissionRate = limpiarCoachCommissionRate(userDoc.sponsorCommissionRate || 0);
    dirty = true;
  }

  if (dirty) {
    userDoc.updatedAt = new Date();
    await userDoc.save();
  }

  return userDoc;
}

async function asegurarCoachDistributorProfile(userDoc = null, profileDoc = null) {
  if (!userDoc?._id) {
    return profileDoc || null;
  }

  const now = new Date();
  const doc =
    profileDoc instanceof CoachDistributorProfile
      ? profileDoc
      : profileDoc?._id
        ? await CoachDistributorProfile.findById(profileDoc._id)
        : (await CoachDistributorProfile.findOne({ userId: userDoc._id })) ||
          new CoachDistributorProfile({
            userId: userDoc._id,
            createdAt: now
          });

  let dirty = !doc.createdAt;

  if (!doc.createdAt) {
    doc.createdAt = now;
  }

  if (doc.name !== (userDoc.name || "")) {
    doc.name = userDoc.name || "";
    dirty = true;
  }

  if (doc.email !== (userDoc.email || "")) {
    doc.email = userDoc.email || "";
    dirty = true;
  }

  const nextSubscriptionStatus = obtenerCoachStatusVisible(userDoc);
  if (doc.subscriptionStatus !== nextSubscriptionStatus) {
    doc.subscriptionStatus = nextSubscriptionStatus;
    dirty = true;
  }

  if (!doc.teamRole) {
    doc.teamRole = resolverCoachTeamRoleDefault(userDoc);
    dirty = true;
  }

  if (!doc.seatLabel && normalizarCoachAccountType(userDoc.accountType || "owner") === "seat") {
    doc.seatLabel = userDoc.name || userDoc.email || "Subcuenta";
    dirty = true;
  }

  if (typeof doc.chefEnabled !== "boolean") {
    doc.chefEnabled = true;
    dirty = true;
  }

  if (!doc.chefSlug) {
    doc.chefSlug = await generarCoachChefSlugDisponible(userDoc, doc._id);
    dirty = true;
  }

  if (!doc.chefShareCode) {
    doc.chefShareCode = await generarCoachChefShareCodeDisponible(doc._id);
    dirty = true;
  }

  if (dirty) {
    doc.updatedAt = now;
    await doc.save();
  }

  return doc;
}

async function construirCoachOwnershipSnapshot(userDoc = null) {
  if (!userDoc?._id) {
    return {
      accountType: "owner",
      ownerUserId: null,
      ownerName: "",
      ownerEmail: "",
      generatedByUserId: null,
      generatedByName: "",
      generatedByAccountType: "owner"
    };
  }

  const accountType = normalizarCoachAccountType(userDoc.accountType || "owner");
  const ownerUserId =
    accountType === "seat" && userDoc.teamOwnerUserId ? userDoc.teamOwnerUserId : userDoc._id;
  let ownerName = userDoc.name || "";
  let ownerEmail = userDoc.email || "";

  if (ownerUserId && String(ownerUserId) !== String(userDoc._id)) {
    const ownerUserDoc = await CoachUser.findById(ownerUserId).select("name email").lean();

    if (ownerUserDoc) {
      ownerName = ownerUserDoc.name || ownerName;
      ownerEmail = ownerUserDoc.email || ownerEmail;
    }
  }

  return {
    accountType,
    ownerUserId,
    ownerName,
    ownerEmail,
    generatedByUserId: userDoc._id,
    generatedByName: userDoc.name || userDoc.email || "",
    generatedByAccountType: accountType
  };
}

function limpiarCoachCommissionRate(value = 0) {
  const parsed = Number(value);

  if (!Number.isFinite(parsed) || parsed <= 0) {
    return 0;
  }

  return Number(parsed.toFixed(4));
}

async function construirCoachSponsorSnapshot(userDoc = null) {
  if (!userDoc?._id) {
    return {
      sponsorUserId: null,
      sponsorName: "",
      sponsorCommissionRate: 0
    };
  }

  let sponsorUserId = userDoc.sponsorUserId || null;
  let sponsorName = String(userDoc.sponsorName || "").trim();
  let sponsorCommissionRate = limpiarCoachCommissionRate(userDoc.sponsorCommissionRate || 0);

  if (!sponsorUserId && userDoc.teamOwnerUserId && String(userDoc.teamOwnerUserId) !== String(userDoc._id)) {
    const ownerUserDoc = await CoachUser.findById(userDoc.teamOwnerUserId)
      .select("sponsorUserId sponsorName sponsorCommissionRate")
      .lean();

    if (ownerUserDoc?.sponsorUserId) {
      sponsorUserId = ownerUserDoc.sponsorUserId;
      sponsorName = String(ownerUserDoc.sponsorName || sponsorName).trim();
      sponsorCommissionRate = limpiarCoachCommissionRate(ownerUserDoc.sponsorCommissionRate || sponsorCommissionRate);
    }
  }

  if (sponsorUserId && !sponsorName) {
    const sponsorUserDoc = await CoachUser.findById(sponsorUserId).select("name email").lean();

    if (sponsorUserDoc) {
      sponsorName = sponsorUserDoc.name || sponsorUserDoc.email || sponsorName;
    }
  }

  return {
    sponsorUserId,
    sponsorName,
    sponsorCommissionRate
  };
}

async function aplicarCoachAccountPreset(userDoc = null, profileDoc = null) {
  try {
    if (!userDoc?._id || !userDoc.email) {
      return {
        userDoc,
        profileDoc,
        applied: false
      };
    }

    const preset = obtenerCoachAccountPreset(userDoc.email);

    if (!preset) {
      return {
        userDoc,
        profileDoc,
        applied: false
      };
    }

    let userDirty = false;
    let profileDirty = false;
    let workingProfileDoc = profileDoc || null;

    if (preset.officeId && userDoc.officeId !== preset.officeId) {
      userDoc.officeId = preset.officeId;
      userDirty = true;
    }

    if (preset.territoryId && userDoc.territoryId !== preset.territoryId) {
      userDoc.territoryId = preset.territoryId;
      userDirty = true;
    }

    if (workingProfileDoc) {
      if (preset.teamRole && workingProfileDoc.teamRole !== preset.teamRole) {
        workingProfileDoc.teamRole = preset.teamRole;
        profileDirty = true;
      }

      if (preset.seatLabel && workingProfileDoc.seatLabel !== preset.seatLabel) {
        workingProfileDoc.seatLabel = preset.seatLabel;
        profileDirty = true;
      }
    }

    const sponsorEmail = normalizarEmail(preset.sponsorEmail || "");
    const sponsorCommissionRate = limpiarCoachCommissionRate(preset.sponsorCommissionRate || 0);
    const userEmail = normalizarEmail(userDoc.email || "");

    if (
      sponsorEmail &&
      sponsorEmail !== userEmail &&
      (!userDoc.sponsorUserId || !userDoc.sponsorName || !userDoc.sponsorCommissionRate)
    ) {
      const sponsorUserDoc = await CoachUser.findOne({ email: sponsorEmail }).select("_id name email").lean();

      if (sponsorUserDoc?._id) {
        if (!userDoc.sponsorUserId || String(userDoc.sponsorUserId) !== String(sponsorUserDoc._id)) {
          userDoc.sponsorUserId = sponsorUserDoc._id;
          userDirty = true;
        }

        const sponsorName = sponsorUserDoc.name || sponsorUserDoc.email || "";

        if (sponsorName && userDoc.sponsorName !== sponsorName) {
          userDoc.sponsorName = sponsorName;
          userDirty = true;
        }

        if (sponsorCommissionRate > 0 && userDoc.sponsorCommissionRate !== sponsorCommissionRate) {
          userDoc.sponsorCommissionRate = sponsorCommissionRate;
          userDirty = true;
        }
      }
    }

    if (userDirty) {
      userDoc.updatedAt = new Date();
      await userDoc.save();
    }

    if (workingProfileDoc && profileDirty) {
      workingProfileDoc.updatedAt = new Date();
      await workingProfileDoc.save();
    }

    return {
      userDoc,
      profileDoc: workingProfileDoc,
      applied: userDirty || profileDirty
    };
  } catch (error) {
    console.error("Error aplicando preset de cuenta Coach:", error.message);
    return {
      userDoc,
      profileDoc,
      applied: false
    };
  }
}

function esCoachTeamManager(userDoc = null) {
  const accountType = normalizarCoachAccountType(userDoc?.accountType || "owner");
  return accountType === "owner" || accountType === "leader";
}

function resolverCoachOwnerUserId(userDoc = null) {
  if (!userDoc?._id) {
    return null;
  }

  const accountType = normalizarCoachAccountType(userDoc.accountType || "owner");
  return accountType === "seat" && userDoc.teamOwnerUserId ? userDoc.teamOwnerUserId : userDoc._id;
}

function construirCoachWorkspaceQuery(userDoc = null, extra = {}) {
  const ownerUserId = resolverCoachOwnerUserId(userDoc);
  const query = {
    ...extra,
    ownerUserId
  };

  const accountType = normalizarCoachAccountType(userDoc?.accountType || "owner");

  if (accountType === "seat" && userDoc?._id && ownerUserId && String(ownerUserId) !== String(userDoc._id)) {
    query.generatedByUserId = userDoc._id;
  }

  return query;
}

async function coachTieneAccesoOperativo(userDoc = null) {
  if (!userDoc) {
    return false;
  }

  const accountType = normalizarCoachAccountType(userDoc.accountType || "owner");
  const seatStatus = normalizarCoachSeatStatus(userDoc.seatStatus || "active");

  if (accountType === "seat" && seatStatus !== "active") {
    return false;
  }

  if (coachTieneAccesoTotal(userDoc)) {
    return true;
  }

  if (accountType !== "seat" || !userDoc.teamOwnerUserId) {
    return false;
  }

  const ownerUserDoc = await CoachUser.findById(userDoc.teamOwnerUserId)
    .select("email subscriptionActive subscriptionStatus")
    .lean();

  return coachTieneAccesoTotal(ownerUserDoc);
}

async function construirCoachUserView(userDoc = null, profileDoc = null) {
  const cleanedUser = limpiarCoachUser(userDoc);

  if (!cleanedUser) {
    return null;
  }

  const ownProfileDoc =
    profileDoc?._id || profileDoc?.userId
      ? profileDoc
      : userDoc?._id
        ? await CoachDistributorProfile.findOne({ userId: userDoc._id }).lean()
        : null;

  cleanedUser.accessGranted = await coachTieneAccesoOperativo(userDoc);
  cleanedUser.ownerUserId = resolverCoachOwnerUserId(userDoc) ? String(resolverCoachOwnerUserId(userDoc)) : "";
  cleanedUser.teamRole = normalizarCoachTeamRole(ownProfileDoc?.teamRole || "", userDoc);
  cleanedUser.portalMode = resolverCoachPortalMode(userDoc, ownProfileDoc);
  cleanedUser.homePath = construirCoachHomePath(userDoc, ownProfileDoc);
  return cleanedUser;
}

async function obtenerCoachOwnerProfile(userDoc = null, options = {}) {
  const ownerUserId = resolverCoachOwnerUserId(userDoc);

  if (!ownerUserId) {
    return null;
  }

  const query = CoachDistributorProfile.findOne({ userId: ownerUserId });
  return options?.lean ? query.lean() : query;
}

function limpiarCoachSeatLabel(value = "") {
  return String(value || "").trim().slice(0, 80);
}

function normalizarCoachSeatManagedRole(value = "") {
  const safeValue = cleanText(value || "")
    .trim()
    .toLowerCase();
  return ["novato", "telemarketing"].includes(safeValue) ? safeValue : "novato";
}

function generarCoachSeatPasswordTemporal() {
  return `A2-${crypto.randomBytes(4).toString("hex")}`;
}

function limpiarCoachUser(userDoc) {
  if (!userDoc) {
    return null;
  }

  const accountType = normalizarCoachAccountType(userDoc.accountType || "owner");
  const seatStatus = normalizarCoachSeatStatus(userDoc.seatStatus || "active");

  return {
    id: String(userDoc._id),
    name: userDoc.name || "",
    email: userDoc.email || "",
    accountType,
    parentUserId: userDoc.parentUserId ? String(userDoc.parentUserId) : "",
    billingOwnerUserId: userDoc.billingOwnerUserId ? String(userDoc.billingOwnerUserId) : "",
    teamOwnerUserId: userDoc.teamOwnerUserId ? String(userDoc.teamOwnerUserId) : "",
    sponsorUserId: userDoc.sponsorUserId ? String(userDoc.sponsorUserId) : "",
    sponsorName: userDoc.sponsorName || "",
    sponsorCommissionRate: limpiarCoachCommissionRate(userDoc.sponsorCommissionRate || 0),
    seatStatus,
    officeId: userDoc.officeId || "",
    territoryId: userDoc.territoryId || "",
    managesTeam: accountType === "owner" || accountType === "leader",
    canViewControlTower: usuarioPuedeVerTorreControl(userDoc),
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
    teamRole: profileDoc?.teamRole || "",
    seatLabel: profileDoc?.seatLabel || "",
    chef: {
      enabled: profileDoc?.chefEnabled !== false,
      slug: profileDoc?.chefSlug || "",
      shareCode: profileDoc?.chefShareCode || "",
      sharePath: construirCoachChefPath(profileDoc?.chefSlug || ""),
      shareUrl: construirCoachChefPath(profileDoc?.chefSlug || ""),
      whatsappSharePath: construirCoachChefWhatsAppPath(),
      whatsappShareUrl: construirCoachChefWhatsAppPath()
    },
    contactShare: {
      shareCode: profileDoc?.chefShareCode || "",
      sharePath: construirCoachContactSharePath(profileDoc?.chefShareCode || ""),
      shareUrl: construirCoachContactSharePath(profileDoc?.chefShareCode || "")
    },
    leadDestination: limpiarCoachLeadDestination(profileDoc),
    preferredCloseStyle: profileDoc?.preferredCloseStyle || "",
    lastInteractionAt: profileDoc?.lastInteractionAt || analyticsDoc?.lastInteractionAt || null,
    recentSessionsSummary: (profileDoc?.recentSessionsSummary || []).slice(0, 4).map(item => ({
      sessionId: item?.sessionId || "",
      summary: truncarTextoPrompt(item?.summary || "", 180),
      createdAt: item?.createdAt || null
    }))
  };
}

function normalizarCoachLeadDestinationType(type = "") {
  const value = String(type || "").trim().toLowerCase();
  const validTypes = ["carpeta_privada", "google_sheets", "webhook_crm", "correo_personal"];
  return validTypes.includes(value) ? value : "carpeta_privada";
}

function limpiarCoachLeadDestination(profileDoc = null) {
  const type = normalizarCoachLeadDestinationType(profileDoc?.leadDestinationType || "carpeta_privada");
  const url = limpiarUrlExterna(profileDoc?.leadDestinationUrl || "");
  const email = normalizarEmail(profileDoc?.leadDestinationEmail || "");
  const label = String(profileDoc?.leadDestinationLabel || "").trim().slice(0, 80);
  const destinationLabels = {
    carpeta_privada: "Solo mi carpeta privada",
    google_sheets: "Google Sheets",
    webhook_crm: "Webhook / CRM",
    correo_personal: "Mi correo personal"
  };

  return {
    type,
    label: label || destinationLabels[type],
    url,
    email,
    enabled:
      type === "correo_personal"
        ? Boolean(email)
        : Boolean(type !== "carpeta_privada" && url),
    updatedAt: profileDoc?.leadDestinationUpdatedAt || null
  };
}

function highLevelReadOnlyEstaConfigurado() {
  return Boolean(HIGHLEVEL_PRIVATE_TOKEN);
}

function highLevelLeadSyncEstaConfigurado() {
  return Boolean(HIGHLEVEL_SYNC_ENABLED && HIGHLEVEL_PRIVATE_TOKEN && HIGHLEVEL_LOCATION_ID);
}

function highLevelSyncEstaConfigurado() {
  return highLevelLeadSyncEstaConfigurado();
}

function highLevelOpportunitySyncEstaConfigurado() {
  return Boolean(highLevelLeadSyncEstaConfigurado() && HIGHLEVEL_PIPELINE_ID && HIGHLEVEL_PIPELINE_STAGE_ID);
}

function highLevelSyncAplicaALead(lead = null) {
  return highLevelLeadSyncEstaConfigurado() && normalizarCoachLeadSource(lead?.source || "") === "chef_personal";
}

function construirHighLevelHeaders(extraHeaders = {}, hasJsonBody = false) {
  const headers = {
    Authorization: `Bearer ${HIGHLEVEL_PRIVATE_TOKEN}`,
    Version: HIGHLEVEL_API_VERSION,
    Accept: "application/json",
    ...extraHeaders
  };

  if (hasJsonBody) {
    headers["Content-Type"] = "application/json";
  }

  return headers;
}

function leerValorAnidado(obj = null, pathKey = "") {
  if (!obj || !pathKey) {
    return undefined;
  }

  return String(pathKey)
    .split(".")
    .filter(Boolean)
    .reduce((acc, key) => (acc && typeof acc === "object" ? acc[key] : undefined), obj);
}

function extraerPrimerValorHighLevel(obj = null, pathKeys = []) {
  for (const pathKey of Array.isArray(pathKeys) ? pathKeys : []) {
    const value = leerValorAnidado(obj, pathKey);
    if (value !== undefined && value !== null && String(value).trim()) {
      return String(value).trim();
    }
  }

  return "";
}

function extraerColeccionHighLevel(obj = null, pathKeys = []) {
  for (const pathKey of Array.isArray(pathKeys) ? pathKeys : []) {
    const value = leerValorAnidado(obj, pathKey);
    if (Array.isArray(value)) {
      return value;
    }
  }

  return Array.isArray(obj) ? obj : [];
}

function partirNombreHighLevel(fullName = "") {
  const safeName = seleccionarNombreConfiable(fullName) || cleanText(fullName);
  const parts = safeName.split(/\s+/).filter(Boolean);

  if (!parts.length) {
    return {
      firstName: "Lead",
      lastName: ""
    };
  }

  if (parts.length === 1) {
    return {
      firstName: parts[0],
      lastName: ""
    };
  }

  return {
    firstName: parts[0],
    lastName: parts.slice(1).join(" ")
  };
}

function construirHighLevelLeadTags(lead = null) {
  const source = normalizarCoachLeadSource(lead?.source || "captura_manual");
  const tags = ["Agustin 2.0", "Agustin Chef", "Chef personal", source.replace(/_/g, " ")];
  return Array.from(new Set(tags.filter(Boolean))).slice(0, 10);
}

function construirPayloadHighLevelContacto(lead = null) {
  if (!lead) {
    return null;
  }

  const safeName = seleccionarNombreConfiable(lead.fullName) || cleanText(lead.fullName || "");
  const nameParts = partirNombreHighLevel(safeName);
  const phone = formatearTelefonoE164(lead.phone || "");
  const email = normalizarEmail(lead.email || "");
  const city = cleanText(lead.city || "").slice(0, 80);
  const postalCode = normalizarZipCode(lead.zipCode || "");
  const payload = {
    locationId: HIGHLEVEL_LOCATION_ID,
    firstName: nameParts.firstName,
    lastName: nameParts.lastName,
    name: safeName || `${nameParts.firstName} ${nameParts.lastName}`.trim(),
    source: "Agustin 2.0 Chef",
    tags: construirHighLevelLeadTags(lead)
  };

  if (email) {
    payload.email = email;
  }

  if (phone) {
    payload.phone = phone;
  }

  if (city) {
    payload.city = city;
  }

  if (postalCode) {
    payload.postalCode = postalCode;
  }

  return payload;
}

function construirPayloadHighLevelOpportunity(lead = null, contactId = "") {
  if (!lead || !contactId || !highLevelOpportunitySyncEstaConfigurado()) {
    return null;
  }

  const safeName = seleccionarNombreConfiable(lead.fullName) || cleanText(lead.fullName || "");

  return {
    locationId: HIGHLEVEL_LOCATION_ID,
    contactId,
    pipelineId: HIGHLEVEL_PIPELINE_ID,
    pipelineStageId: HIGHLEVEL_PIPELINE_STAGE_ID,
    status: "open",
    name: `${safeName || "Lead"} · Agustin Chef`,
    source: "Agustin 2.0 Chef",
    monetaryValue: 0,
    assignedTo: HIGHLEVEL_ASSIGNED_TO_USER_ID || undefined
  };
}

async function highLevelFetchJson(apiPath = "", options = {}) {
  if (!HIGHLEVEL_PRIVATE_TOKEN) {
    return {
      ok: false,
      status: 0,
      error: "Falta HIGHLEVEL_PRIVATE_TOKEN."
    };
  }

  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), Number(options.timeoutMs || 12000));
  const requestInit = {
    method: options.method || "GET",
    headers: construirHighLevelHeaders(options.headers || {}, options.body !== undefined),
    signal: controller.signal
  };

  if (options.body !== undefined) {
    requestInit.body = JSON.stringify(options.body);
  }

  try {
    const response = await fetch(`${HIGHLEVEL_API_BASE_URL}${apiPath}`, requestInit);
    const data = await response.json().catch(() => null);
    clearTimeout(timeout);

    if (!response.ok) {
      return {
        ok: false,
        status: response.status,
        data,
        error:
          extraerPrimerValorHighLevel(data, ["message", "error", "errors.0.message"]) ||
          `HighLevel respondio ${response.status}`
      };
    }

    return {
      ok: true,
      status: response.status,
      data
    };
  } catch (error) {
    clearTimeout(timeout);
    return {
      ok: false,
      status: 0,
      error: error?.name === "AbortError" ? "HighLevel tardo demasiado en responder." : error.message
    };
  }
}

function normalizarListaHighLevel(items = []) {
  return (Array.isArray(items) ? items : [])
    .map(item => ({
      id: extraerPrimerValorHighLevel(item, ["id", "_id", "campaignId", "workflowId"]),
      name: extraerPrimerValorHighLevel(item, ["name", "title"]),
      status: extraerPrimerValorHighLevel(item, ["status", "state"]),
      type: extraerPrimerValorHighLevel(item, ["type"])
    }))
    .filter(item => item.id || item.name);
}

async function listarHighLevelCampaigns() {
  if (!highLevelReadOnlyEstaConfigurado()) {
    return {
      ok: false,
      items: [],
      error: "HighLevel todavia no esta configurado."
    };
  }

  const result = await highLevelFetchJson("/campaigns/");

  if (!result.ok) {
    return {
      ok: false,
      items: [],
      error: result.error || "No pude leer las campanas de HighLevel."
    };
  }

  return {
    ok: true,
    items: normalizarListaHighLevel(
      extraerColeccionHighLevel(result.data, ["campaigns", "data.campaigns", "data", "items"])
    ).slice(0, 100)
  };
}

async function listarHighLevelWorkflows() {
  if (!highLevelReadOnlyEstaConfigurado()) {
    return {
      ok: false,
      items: [],
      error: "HighLevel todavia no esta configurado."
    };
  }

  const result = await highLevelFetchJson("/workflows/");

  if (!result.ok) {
    return {
      ok: false,
      items: [],
      error: result.error || "No pude leer los workflows de HighLevel."
    };
  }

  return {
    ok: true,
    items: normalizarListaHighLevel(
      extraerColeccionHighLevel(result.data, ["workflows", "data.workflows", "data", "items"])
    ).slice(0, 100)
  };
}

function highLevelWebhookAutorizado(req = null) {
  const token = cleanText(req?.query?.token || req?.headers?.["x-agustin-highlevel-token"] || "");

  if (!HIGHLEVEL_WEBHOOK_TOKEN || !token) {
    return false;
  }

  const expected = Buffer.from(HIGHLEVEL_WEBHOOK_TOKEN);
  const received = Buffer.from(token);

  if (expected.length !== received.length) {
    return false;
  }

  return crypto.timingSafeEqual(expected, received);
}

function limpiarCoachTeamSeat(userDoc = null, profileDoc = null, stats = {}) {
  if (!userDoc?._id) {
    return null;
  }

  const teamRole = normalizarCoachTeamRole(profileDoc?.teamRole || "", userDoc);

  return {
    id: String(userDoc._id),
    name: userDoc.name || "",
    email: userDoc.email || "",
    accountType: normalizarCoachAccountType(userDoc.accountType || "seat"),
    seatStatus: normalizarCoachSeatStatus(userDoc.seatStatus || "active"),
    seatLabel: profileDoc?.seatLabel || "",
    teamRole,
    homePath: construirCoachHomePath(userDoc, { teamRole }),
    chefEnabled: profileDoc?.chefEnabled !== false,
    chef: {
      enabled: profileDoc?.chefEnabled !== false,
      slug: profileDoc?.chefSlug || "",
      shareCode: profileDoc?.chefShareCode || "",
      sharePath: construirCoachChefPath(profileDoc?.chefSlug || ""),
      shareUrl: construirCoachChefPath(profileDoc?.chefSlug || ""),
      whatsappSharePath: construirCoachChefWhatsAppPath(),
      whatsappShareUrl: construirCoachChefWhatsAppPath()
    },
    contactShare: {
      shareCode: profileDoc?.chefShareCode || "",
      sharePath: construirCoachContactSharePath(profileDoc?.chefShareCode || ""),
      shareUrl: construirCoachContactSharePath(profileDoc?.chefShareCode || "")
    },
    createdAt: userDoc.createdAt || null,
    lastLoginAt: userDoc.lastLoginAt || null,
    counts: {
      leads: Number(stats?.leads || 0),
      surveys: Number(stats?.surveys || 0),
      programSheets: Number(stats?.programSheets || 0),
      applications: Number(stats?.applications || 0)
    }
  };
}

function construirCoachTeamSummary(seats = []) {
  const safeSeats = Array.isArray(seats) ? seats : [];

  return {
    totalSeats: safeSeats.length,
    activeSeats: safeSeats.filter(seat => seat?.seatStatus === "active").length,
    pausedSeats: safeSeats.filter(seat => seat?.seatStatus === "paused").length,
    closedSeats: safeSeats.filter(seat => seat?.seatStatus === "closed").length,
    totalGeneratedLeads: safeSeats.reduce((acc, seat) => acc + Number(seat?.counts?.leads || 0), 0)
  };
}

function formatearCoachTerritoryRole(role = "") {
  const labels = {
    owner: "Propietario",
    manager: "Manager",
    junior: "Distribuidor junior",
    member: "Distribuidor"
  };

  return labels[normalizarCoachTerritoryRole(role)] || "Distribuidor";
}

function limpiarCoachTerritoryInvite(inviteDoc = null, territoryDoc = null, options = {}) {
  if (!inviteDoc?._id) {
    return null;
  }

  const includeTerritory = Boolean(options.includeTerritory);
  const status = normalizarCoachTerritoryInviteStatus(inviteDoc.status || "pending");
  const role = normalizarCoachTerritoryRole(inviteDoc.role || "member");

  return {
    id: String(inviteDoc._id),
    territoryId: inviteDoc.territoryId ? String(inviteDoc.territoryId) : "",
    territoryName: includeTerritory ? territoryDoc?.name || "" : "",
    officeId: includeTerritory ? territoryDoc?.officeId || "" : "",
    territoryLabel: includeTerritory ? territoryDoc?.territoryId || "" : "",
    email: inviteDoc.inviteeEmail || "",
    inviteeUserId: inviteDoc.inviteeUserId ? String(inviteDoc.inviteeUserId) : "",
    role,
    roleLabel: formatearCoachTerritoryRole(role),
    status,
    invitedByName: inviteDoc.invitedByName || "",
    note: inviteDoc.note || "",
    createdAt: inviteDoc.createdAt || null,
    respondedAt: inviteDoc.respondedAt || null
  };
}

function limpiarCoachTerritoryMember(membershipDoc = null, userDoc = null, profileDoc = null, stats = {}) {
  if (!membershipDoc?.userId || !userDoc?._id) {
    return null;
  }

  const role = normalizarCoachTerritoryRole(membershipDoc.role || "member");
  const seatStats = stats?.seatStats || {};

  return {
    membershipId: String(membershipDoc._id || ""),
    userId: String(userDoc._id),
    name: userDoc.name || profileDoc?.name || "Miembro",
    email: userDoc.email || profileDoc?.email || "",
    role,
    roleLabel: formatearCoachTerritoryRole(role),
    accountType: normalizarCoachAccountType(userDoc.accountType || "owner"),
    teamRole: profileDoc?.teamRole || "",
    officeId: userDoc.officeId || "",
    territoryId: userDoc.territoryId || "",
    seatLabel: profileDoc?.seatLabel || "",
    createdAt: membershipDoc.createdAt || null,
    lastLoginAt: userDoc.lastLoginAt || null,
    counts: {
      leads: Number(stats?.leads || 0),
      chefLeads: Number(stats?.chefLeads || 0),
      programSheets: Number(stats?.programSheets || 0),
      applications: Number(stats?.applications || 0),
      sales: Number(stats?.sales || 0),
      soldAmount: limpiarCoachDemoOutcomeAmount(stats?.soldAmount || 0)
    },
    seats: {
      total: Number(seatStats.totalSeats || 0),
      active: Number(seatStats.activeSeats || 0)
    }
  };
}

async function obtenerCoachTerritoryWorkspace(userDoc = null) {
  if (!userDoc?._id || !coachPuedeUsarTerritorios(userDoc)) {
    return {
      canCreateTerritory: false,
      territories: [],
      pendingInvites: []
    };
  }

  const safeEmail = normalizarEmail(userDoc.email || "");
  const [myMembershipDocs, myPendingInviteDocs] = await Promise.all([
    CoachTerritoryMembership.find({
      userId: userDoc._id,
      status: "active"
    })
      .sort({ createdAt: -1 })
      .lean(),
    safeEmail
      ? CoachTerritoryInvite.find({
          inviteeEmail: safeEmail,
          status: "pending"
        })
          .sort({ createdAt: -1 })
          .lean()
      : Promise.resolve([])
  ]);

  const activeTerritoryIds = Array.from(
    new Set(
      myMembershipDocs
        .map(item => (item.territoryId ? String(item.territoryId) : ""))
        .filter(Boolean)
    )
  );
  const pendingTerritoryIds = Array.from(
    new Set(
      myPendingInviteDocs
        .map(item => (item.territoryId ? String(item.territoryId) : ""))
        .filter(Boolean)
    )
  );
  const allTerritoryIds = Array.from(new Set([...activeTerritoryIds, ...pendingTerritoryIds]));

  const territoryDocs = allTerritoryIds.length
    ? await CoachTerritory.find({
        _id: { $in: allTerritoryIds },
        status: "active"
      })
        .sort({ createdAt: -1 })
        .lean()
    : [];
  const territoryMap = new Map(territoryDocs.map(doc => [String(doc._id), doc]));
  const activeDocs = territoryDocs.filter(doc => activeTerritoryIds.includes(String(doc._id)));

  const [territoryMembershipDocs, managedInviteDocs] = await Promise.all([
    activeTerritoryIds.length
      ? CoachTerritoryMembership.find({
          territoryId: { $in: activeTerritoryIds },
          status: "active"
        }).lean()
      : Promise.resolve([]),
    activeTerritoryIds.length
      ? CoachTerritoryInvite.find({
          territoryId: { $in: activeTerritoryIds },
          status: "pending"
        })
          .sort({ createdAt: -1 })
          .lean()
      : Promise.resolve([])
  ]);

  const memberUserIds = Array.from(
    new Set(
      territoryMembershipDocs
        .map(item => (item.userId ? String(item.userId) : ""))
        .filter(Boolean)
    )
  );

  const [memberUserDocs, profileDocs, seatDocs, leadAgg, programAgg, applicationAgg, salesAgg, recentOutcomeDocs] =
    await Promise.all([
      memberUserIds.length
        ? CoachUser.find({
            _id: { $in: memberUserIds }
          }).lean()
        : Promise.resolve([]),
      memberUserIds.length
        ? CoachDistributorProfile.find({
            userId: { $in: memberUserIds }
          }).lean()
        : Promise.resolve([]),
      memberUserIds.length
        ? CoachUser.find({
            accountType: "seat",
            teamOwnerUserId: { $in: memberUserIds }
          })
            .select("teamOwnerUserId seatStatus")
            .lean()
        : Promise.resolve([]),
      memberUserIds.length
        ? CoachLeadInbox.aggregate([
            {
              $match: {
                ownerUserId: { $in: memberUserIds }
              }
            },
            {
              $group: {
                _id: "$ownerUserId",
                totalLeads: { $sum: 1 },
                chefLeads: {
                  $sum: {
                    $cond: [{ $eq: ["$source", "chef_personal"] }, 1, 0]
                  }
                }
              }
            }
          ])
        : Promise.resolve([]),
      memberUserIds.length
        ? CoachProgramSheet.aggregate([
            {
              $match: {
                ownerUserId: { $in: memberUserIds }
              }
            },
            {
              $group: {
                _id: "$ownerUserId",
                total: { $sum: 1 }
              }
            }
          ])
        : Promise.resolve([]),
      memberUserIds.length
        ? CoachRecruitmentApplication.aggregate([
            {
              $match: {
                ownerUserId: { $in: memberUserIds }
              }
            },
            {
              $group: {
                _id: "$ownerUserId",
                total: { $sum: 1 }
              }
            }
          ])
        : Promise.resolve([]),
      memberUserIds.length
        ? CoachDemoOutcome.aggregate([
            {
              $match: {
                ownerUserId: { $in: memberUserIds }
              }
            },
            {
              $group: {
                _id: "$ownerUserId",
                sales: {
                  $sum: { $cond: [{ $eq: ["$resultType", "venta"] }, 1, 0] }
                },
                soldAmount: {
                  $sum: {
                    $cond: [{ $eq: ["$resultType", "venta"] }, "$saleAmount", 0]
                  }
                }
              }
            }
          ])
        : Promise.resolve([]),
      memberUserIds.length
        ? CoachDemoOutcome.find({
            ownerUserId: { $in: memberUserIds }
          })
            .sort({ createdAt: -1 })
            .limit(12)
            .lean()
        : Promise.resolve([])
    ]);

  const memberUserMap = new Map(memberUserDocs.map(doc => [String(doc._id), doc]));
  const profileMap = new Map(profileDocs.map(doc => [String(doc.userId), doc]));
  const leadMap = new Map(leadAgg.map(item => [String(item._id), item]));
  const programMap = new Map(programAgg.map(item => [String(item._id), Number(item.total || 0)]));
  const applicationMap = new Map(applicationAgg.map(item => [String(item._id), Number(item.total || 0)]));
  const salesMap = new Map(salesAgg.map(item => [String(item._id), item]));
  const seatMap = new Map();

  for (const seatDoc of seatDocs) {
    const ownerId = String(seatDoc.teamOwnerUserId || "");

    if (!ownerId) {
      continue;
    }

    const current = seatMap.get(ownerId) || {
      totalSeats: 0,
      activeSeats: 0
    };
    current.totalSeats += 1;

    if (normalizarCoachSeatStatus(seatDoc.seatStatus || "active") === "active") {
      current.activeSeats += 1;
    }

    seatMap.set(ownerId, current);
  }

  const recentOutcomeByOwner = new Map();

  for (const outcomeDoc of recentOutcomeDocs) {
    const ownerId = String(outcomeDoc.ownerUserId || "");

    if (!ownerId) {
      continue;
    }

    const current = recentOutcomeByOwner.get(ownerId) || [];
    if (current.length >= 3) {
      continue;
    }

    current.push(limpiarCoachDemoOutcome(outcomeDoc, { includeGenerator: true }));
    recentOutcomeByOwner.set(ownerId, current);
  }

  const membershipsByTerritory = new Map();
  territoryMembershipDocs.forEach(item => {
    const territoryKey = String(item.territoryId || "");
    const current = membershipsByTerritory.get(territoryKey) || [];
    current.push(item);
    membershipsByTerritory.set(territoryKey, current);
  });

  const invitesByTerritory = new Map();
  managedInviteDocs.forEach(item => {
    const territoryKey = String(item.territoryId || "");
    const current = invitesByTerritory.get(territoryKey) || [];
    current.push(item);
    invitesByTerritory.set(territoryKey, current);
  });

  const myMembershipMap = new Map(myMembershipDocs.map(item => [String(item.territoryId || ""), item]));

  const territories = activeDocs.map(territoryDoc => {
    const territoryKey = String(territoryDoc._id);
    const myMembership = myMembershipMap.get(territoryKey) || null;
    const myRole = normalizarCoachTerritoryRole(myMembership?.role || "member");
    const canManage = coachTerritoryRolePuedeAdministrar(myRole);
    const memberRows = (membershipsByTerritory.get(territoryKey) || [])
      .map(memberDoc => {
        const memberId = String(memberDoc.userId || "");
        const memberUserDoc = memberUserMap.get(memberId);
        if (!memberUserDoc) {
          return null;
        }

        const leadStats = leadMap.get(memberId) || { totalLeads: 0, chefLeads: 0 };
        const salesStats = salesMap.get(memberId) || { sales: 0, soldAmount: 0 };

        return limpiarCoachTerritoryMember(memberDoc, memberUserDoc, profileMap.get(memberId) || null, {
          leads: leadStats.totalLeads || 0,
          chefLeads: leadStats.chefLeads || 0,
          programSheets: programMap.get(memberId) || 0,
          applications: applicationMap.get(memberId) || 0,
          sales: salesStats.sales || 0,
          soldAmount: salesStats.soldAmount || 0,
          seatStats: seatMap.get(memberId) || null
        });
      })
      .filter(Boolean)
      .sort((a, b) => a.name.localeCompare(b.name, "es", { sensitivity: "base" }));

    const summary = memberRows.reduce(
      (acc, member) => {
        acc.totalMembers += 1;
        acc.totalSeats += Number(member.seats?.total || 0);
        acc.activeSeats += Number(member.seats?.active || 0);
        acc.totalLeads += Number(member.counts?.leads || 0);
        acc.chefLeads += Number(member.counts?.chefLeads || 0);
        acc.programSheets += Number(member.counts?.programSheets || 0);
        acc.applications += Number(member.counts?.applications || 0);
        acc.sales += Number(member.counts?.sales || 0);
        acc.soldAmount += Number(member.counts?.soldAmount || 0);
        return acc;
      },
      {
        totalMembers: 0,
        totalSeats: 0,
        activeSeats: 0,
        totalLeads: 0,
        chefLeads: 0,
        programSheets: 0,
        applications: 0,
        sales: 0,
        soldAmount: 0
      }
    );

    const territoryInvites = canManage
      ? (invitesByTerritory.get(territoryKey) || [])
          .map(inviteDoc => limpiarCoachTerritoryInvite(inviteDoc, territoryDoc))
          .filter(Boolean)
      : [];

    const recentResults = memberRows.flatMap(member => recentOutcomeByOwner.get(member.userId) || []).slice(0, 8);

    return {
      id: territoryKey,
      name: territoryDoc.name || "Territorio",
      officeId: territoryDoc.officeId || "",
      territoryId: territoryDoc.territoryId || "",
      myRole,
      myRoleLabel: formatearCoachTerritoryRole(myRole),
      canManage,
      createdAt: territoryDoc.createdAt || null,
      members: memberRows,
      invites: territoryInvites,
      recentResults,
      summary: {
        totalMembers: summary.totalMembers,
        totalSeats: summary.totalSeats,
        activeSeats: summary.activeSeats,
        totalLeads: summary.totalLeads,
        chefLeads: summary.chefLeads,
        programSheets: summary.programSheets,
        applications: summary.applications,
        sales: summary.sales,
        soldAmount: limpiarCoachDemoOutcomeAmount(summary.soldAmount || 0)
      }
    };
  });

  return {
    canCreateTerritory: true,
    territories,
    pendingInvites: myPendingInviteDocs
      .map(inviteDoc =>
        limpiarCoachTerritoryInvite(inviteDoc, territoryMap.get(String(inviteDoc.territoryId || "")) || null, {
          includeTerritory: true
        })
      )
      .filter(Boolean)
  };
}

async function resolverCoachAudienceOwner(userDoc = null) {
  if (!userDoc?._id) {
    return null;
  }

  const accountType = normalizarCoachAccountType(userDoc.accountType || "owner");

  if (accountType === "seat" && userDoc.teamOwnerUserId && String(userDoc.teamOwnerUserId) !== String(userDoc._id)) {
    const ownerUserDoc = await CoachUser.findById(userDoc.teamOwnerUserId)
      .select("_id name email officeId territoryId accountType")
      .lean();
    return ownerUserDoc || userDoc;
  }

  return userDoc;
}

async function obtenerCoachVisibleTerritoryIds(userDoc = null) {
  const audienceUser = await resolverCoachAudienceOwner(userDoc);

  if (!audienceUser?._id) {
    return [];
  }

  const membershipDocs = await CoachTerritoryMembership.find({
    userId: audienceUser._id,
    status: "active"
  })
    .select("territoryId")
    .lean();

  return Array.from(
    new Set(
      membershipDocs
        .map(item => (item.territoryId ? String(item.territoryId) : ""))
      .filter(Boolean)
    )
  );
}

async function obtenerCoachReachableContacts(userDoc = null) {
  if (!userDoc?._id) {
    return [];
  }

  const selfId = String(userDoc._id);
  const audienceUser = await resolverCoachAudienceOwner(userDoc);
  const audienceUserId = String(audienceUser?._id || "");
  const visibleTerritoryIds = await obtenerCoachVisibleTerritoryIds(userDoc);
  const reachableIds = new Set();
  const relationMap = new Map();

  const markRelation = (userId, label) => {
    const safeId = String(userId || "").trim();
    if (!safeId || safeId === selfId) {
      return;
    }

    reachableIds.add(safeId);
    const current = relationMap.get(safeId) || new Set();
    if (label) {
      current.add(label);
    }
    relationMap.set(safeId, current);
  };

  if (audienceUser?.sponsorUserId) {
    markRelation(audienceUser.sponsorUserId, "Sponsor");
  }

  if (userDoc.sponsorUserId && String(userDoc.sponsorUserId) !== String(audienceUser?.sponsorUserId || "")) {
    markRelation(userDoc.sponsorUserId, "Sponsor");
  }

  if (normalizarCoachAccountType(userDoc.accountType || "owner") === "seat" && audienceUserId && audienceUserId !== selfId) {
    markRelation(audienceUserId, "Upline directo");
  }

  if (audienceUserId) {
    const seatDocs = await CoachUser.find({
      teamOwnerUserId: audienceUserId,
      accountType: "seat",
      seatStatus: "active"
    })
      .select("_id")
      .lean();

    seatDocs.forEach(seatDoc => {
      const label =
        String(seatDoc._id || "") === selfId
          ? ""
          : normalizarCoachAccountType(userDoc.accountType || "owner") === "seat"
            ? "Companero de equipo"
            : "Subcuenta";
      markRelation(seatDoc._id, label);
    });
  }

  if (visibleTerritoryIds.length) {
    const membershipDocs = await CoachTerritoryMembership.find({
      territoryId: { $in: visibleTerritoryIds },
      status: "active"
    })
      .select("userId role territoryId")
      .lean();

    membershipDocs.forEach(item => {
      markRelation(item.userId, formatearCoachTerritoryRole(item.role || "member"));
    });
  }

  const userIds = Array.from(reachableIds);

  if (!userIds.length) {
    return [];
  }

  const [userDocs, profileDocs] = await Promise.all([
    CoachUser.find({
      _id: { $in: userIds }
    })
      .select("name email accountType seatStatus officeId territoryId teamOwnerUserId sponsorUserId")
      .lean(),
    CoachDistributorProfile.find({
      userId: { $in: userIds }
    }).lean()
  ]);

  const profileMap = new Map(profileDocs.map(doc => [String(doc.userId), doc]));

  return userDocs
    .filter(candidate => {
      const accountType = normalizarCoachAccountType(candidate.accountType || "owner");
      const seatStatus = normalizarCoachSeatStatus(candidate.seatStatus || "active");
      return accountType !== "seat" || seatStatus === "active";
    })
    .map(candidate => {
      const candidateId = String(candidate._id || "");
      const relationLabels = Array.from(relationMap.get(candidateId) || []).filter(Boolean);
      const profileDoc = profileMap.get(candidateId) || null;

      return {
        id: candidateId,
        name: candidate.name || candidate.email || "Cuenta",
        email: candidate.email || "",
        accountType: normalizarCoachAccountType(candidate.accountType || "owner"),
        teamRole: profileDoc?.teamRole || "",
        officeId: candidate.officeId || "",
        territoryId: candidate.territoryId || "",
        relationLabel: relationLabels.join(" · ")
      };
    })
    .sort((a, b) => a.name.localeCompare(b.name, "es", { sensitivity: "base" }));
}

function limpiarCoachAnnouncement(announcementDoc = null, options = {}) {
  if (!announcementDoc?._id) {
    return null;
  }

  return {
    id: String(announcementDoc._id),
    authorName: announcementDoc.authorName || "",
    scopeType: normalizarCoachAnnouncementScopeType(announcementDoc.scopeType || "global"),
    territoryId: announcementDoc.territoryId ? String(announcementDoc.territoryId) : "",
    territoryName: announcementDoc.territoryName || "",
    teamOwnerUserId: announcementDoc.teamOwnerUserId ? String(announcementDoc.teamOwnerUserId) : "",
    teamOwnerName: announcementDoc.teamOwnerName || "",
    title: announcementDoc.title || "",
    body: announcementDoc.body || "",
    priority: normalizarCoachAnnouncementPriority(announcementDoc.priority || "normal"),
    status: announcementDoc.status || "active",
    createdAt: announcementDoc.createdAt || null,
    updatedAt: announcementDoc.updatedAt || null,
    read: Boolean(options.read)
  };
}

function limpiarCoachSupportThread(threadDoc = null, options = {}) {
  if (!threadDoc?._id) {
    return null;
  }

  return {
    id: String(threadDoc._id),
    ownerUserId: threadDoc.ownerUserId ? String(threadDoc.ownerUserId) : "",
    ownerName: threadDoc.ownerName || "",
    ownerEmail: threadDoc.ownerEmail || "",
    status: threadDoc.status || "open",
    ownerUnreadCount: Number(threadDoc.ownerUnreadCount || 0),
    supportUnreadCount: Number(threadDoc.supportUnreadCount || 0),
    lastMessageAt: threadDoc.lastMessageAt || null,
    lastMessagePreview: threadDoc.lastMessagePreview || "",
    messages: Array.isArray(options.messages) ? options.messages : []
  };
}

function limpiarCoachSupportMessage(messageDoc = null) {
  if (!messageDoc?._id) {
    return null;
  }

  return {
    id: String(messageDoc._id),
    threadId: messageDoc.threadId ? String(messageDoc.threadId) : "",
    senderUserId: messageDoc.senderUserId ? String(messageDoc.senderUserId) : "",
    senderName: messageDoc.senderName || "",
    senderScope: messageDoc.senderScope || "coach_user",
    body: messageDoc.body || "",
    createdAt: messageDoc.createdAt || null
  };
}

function limpiarCoachDirectMessage(messageDoc = null) {
  if (!messageDoc?._id) {
    return null;
  }

  return {
    id: String(messageDoc._id),
    threadId: messageDoc.threadId ? String(messageDoc.threadId) : "",
    senderUserId: messageDoc.senderUserId ? String(messageDoc.senderUserId) : "",
    senderName: messageDoc.senderName || "",
    body: messageDoc.body || "",
    createdAt: messageDoc.createdAt || null
  };
}

function limpiarCoachDirectThread(threadDoc = null, contact = null, messages = []) {
  if (!threadDoc?._id) {
    return null;
  }

  return {
    id: String(threadDoc._id),
    contact,
    unread: Array.isArray(threadDoc.unreadByUserIds) ? threadDoc.unreadByUserIds.length > 0 : false,
    lastMessageAt: threadDoc.lastMessageAt || null,
    lastMessagePreview: threadDoc.lastMessagePreview || "",
    messages: Array.isArray(messages) ? messages : []
  };
}

async function encontrarOCrearCoachDirectThread(userDoc = null, targetUserId = "") {
  if (!userDoc?._id || !targetUserId || !mongoose.Types.ObjectId.isValid(targetUserId)) {
    return null;
  }

  const participantKey = construirCoachDirectParticipantKey(userDoc._id, targetUserId);

  if (!participantKey) {
    return null;
  }

  let threadDoc = await CoachDirectThread.findOne({ participantKey });

  if (threadDoc) {
    return threadDoc;
  }

  threadDoc = await CoachDirectThread.create({
    participantUserIds: [userDoc._id, targetUserId],
    participantKey,
    unreadByUserIds: [],
    updatedAt: new Date()
  });

  return threadDoc;
}

async function obtenerCoachDirectThreadsOverview(userDoc = null, contacts = []) {
  if (!userDoc?._id) {
    return {
      contacts: [],
      threads: [],
      unreadCount: 0
    };
  }

  const safeContacts = Array.isArray(contacts) ? contacts : [];
  const contactMap = new Map(safeContacts.map(item => [String(item.id || ""), item]));
  const threadDocs = await CoachDirectThread.find({
    participantUserIds: userDoc._id
  })
    .sort({ lastMessageAt: -1, createdAt: -1 })
    .limit(20)
    .lean();

  const threadIds = threadDocs.map(item => item._id).filter(Boolean);
  const messageDocs = threadIds.length
    ? await CoachDirectMessage.find({
        threadId: { $in: threadIds }
      })
        .sort({ createdAt: -1 })
        .lean()
    : [];
  const messagesByThread = new Map();

  for (const messageDoc of messageDocs) {
    const threadId = String(messageDoc.threadId || "");
    const current = messagesByThread.get(threadId) || [];

    if (current.length >= 25) {
      continue;
    }

    current.push(messageDoc);
    messagesByThread.set(threadId, current);
  }

  const unresolvedContactIds = new Set();
  threadDocs.forEach(threadDoc => {
    const participantIds = Array.isArray(threadDoc.participantUserIds) ? threadDoc.participantUserIds.map(item => String(item)) : [];
    const otherUserId = participantIds.find(item => item && item !== String(userDoc._id));

    if (otherUserId && !contactMap.has(otherUserId)) {
      unresolvedContactIds.add(otherUserId);
    }
  });

  if (unresolvedContactIds.size) {
    const missingUsers = await CoachUser.find({
      _id: { $in: Array.from(unresolvedContactIds) }
    })
      .select("name email accountType officeId territoryId")
      .lean();

    missingUsers.forEach(userItem => {
      contactMap.set(String(userItem._id), {
        id: String(userItem._id),
        name: userItem.name || userItem.email || "Cuenta",
        email: userItem.email || "",
        accountType: normalizarCoachAccountType(userItem.accountType || "owner"),
        officeId: userItem.officeId || "",
        territoryId: userItem.territoryId || "",
        teamRole: "",
        relationLabel: "Conversacion activa"
      });
    });
  }

  const threads = threadDocs
    .map(threadDoc => {
      const participantIds = Array.isArray(threadDoc.participantUserIds) ? threadDoc.participantUserIds.map(item => String(item)) : [];
      const otherUserId = participantIds.find(item => item && item !== String(userDoc._id));
      const directMessages = (messagesByThread.get(String(threadDoc._id)) || [])
        .slice()
        .reverse()
        .map(item => limpiarCoachDirectMessage(item))
        .filter(Boolean);

      const unreadByUserIds = Array.isArray(threadDoc.unreadByUserIds)
        ? threadDoc.unreadByUserIds.map(item => String(item))
        : [];

      return limpiarCoachDirectThread(
        {
          ...threadDoc,
          unreadByUserIds: unreadByUserIds.includes(String(userDoc._id)) ? [userDoc._id] : []
        },
        contactMap.get(otherUserId || "") || null,
        directMessages
      );
    })
    .filter(Boolean);

  return {
    contacts: safeContacts,
    threads,
    unreadCount: threads.filter(item => item.unread).length
  };
}

async function asegurarCoachSupportThread(userDoc = null) {
  if (!userDoc?._id) {
    return null;
  }

  let threadDoc = await CoachSupportThread.findOne({ ownerUserId: userDoc._id });

  if (threadDoc) {
    return threadDoc;
  }

  threadDoc = await CoachSupportThread.create({
    ownerUserId: userDoc._id,
    ownerName: userDoc.name || userDoc.email || "",
    ownerEmail: userDoc.email || "",
    status: "open",
    ownerUnreadCount: 0,
    supportUnreadCount: 0,
    updatedAt: new Date()
  });

  return threadDoc;
}

async function obtenerCoachMessagesOverview(userDoc = null) {
  if (!userDoc?._id) {
    return {
      announcements: [],
      supportThread: null,
      direct: {
        contacts: [],
        threads: [],
        unreadCount: 0
      },
      bulletinOptions: {
        canSendTeam: false,
        canSendTerritory: false,
        territories: []
      },
      unread: {
        announcements: 0,
        support: 0,
        direct: 0,
        total: 0
      }
    };
  }

  const audienceUser = await resolverCoachAudienceOwner(userDoc);
  const visibleTerritoryIds = await obtenerCoachVisibleTerritoryIds(userDoc);
  const reachableContacts = await obtenerCoachReachableContacts(userDoc);
  const announcementQuery = {
    status: "active",
    $or: [{ scopeType: "global" }]
  };

  if (audienceUser?._id) {
    announcementQuery.$or.push({
      scopeType: "team",
      teamOwnerUserId: audienceUser._id
    });
  }

  if (visibleTerritoryIds.length) {
    announcementQuery.$or.push({
      scopeType: "territory",
      territoryId: { $in: visibleTerritoryIds }
    });
  }

  const [announcementDocs, threadDoc, directOverview, territoryWorkspace] = await Promise.all([
    CoachAnnouncement.find(announcementQuery)
      .sort({ createdAt: -1 })
      .limit(20)
      .lean(),
    asegurarCoachSupportThread(userDoc),
    obtenerCoachDirectThreadsOverview(userDoc, reachableContacts),
    coachPuedeUsarTerritorios(userDoc) ? obtenerCoachTerritoryWorkspace(userDoc) : Promise.resolve(null)
  ]);

  const announcementIds = announcementDocs.map(item => item._id).filter(Boolean);
  const [receiptDocs, supportMessageDocs] = await Promise.all([
    announcementIds.length
      ? CoachAnnouncementReceipt.find({
          announcementId: { $in: announcementIds },
          userId: userDoc._id
        })
          .select("announcementId")
          .lean()
      : Promise.resolve([]),
    threadDoc?._id
      ? CoachSupportMessage.find({ threadId: threadDoc._id })
          .sort({ createdAt: 1 })
          .limit(40)
          .lean()
      : Promise.resolve([])
  ]);

  const receiptSet = new Set(receiptDocs.map(item => String(item.announcementId || "")));
  const announcements = announcementDocs
    .map(item =>
      limpiarCoachAnnouncement(item, {
        read: receiptSet.has(String(item._id))
      })
    )
    .filter(Boolean);

  const unreadAnnouncements = announcements.filter(item => !item.read).length;
  const supportMessages = supportMessageDocs.map(item => limpiarCoachSupportMessage(item)).filter(Boolean);
  const supportThread = limpiarCoachSupportThread(threadDoc, { messages: supportMessages });
  const unreadSupport = Number(threadDoc?.ownerUnreadCount || 0);

  return {
    announcements,
    supportThread,
    direct: directOverview,
    bulletinOptions: {
      canSendTeam: esCoachTeamManager(userDoc),
      canSendTerritory: Array.isArray(territoryWorkspace?.territories)
        ? territoryWorkspace.territories.some(item => item?.canManage)
        : false,
      territories: Array.isArray(territoryWorkspace?.territories)
        ? territoryWorkspace.territories
            .filter(item => item?.canManage)
            .map(item => ({
              id: item.id,
              name: item.name || "",
              officeId: item.officeId || "",
              territoryId: item.territoryId || ""
            }))
        : []
    },
    unread: {
      announcements: unreadAnnouncements,
      support: unreadSupport,
      direct: Number(directOverview?.unreadCount || 0),
      total: unreadAnnouncements + unreadSupport + Number(directOverview?.unreadCount || 0)
    }
  };
}

async function obtenerCoachCrmAssignableContacts(userDoc = null) {
  if (!userDoc?._id) {
    return [];
  }

  const contacts = await obtenerCoachReachableContacts(userDoc);
  const selfContact = {
    id: String(userDoc._id),
    name: userDoc.name || userDoc.email || "Mi cuenta",
    email: userDoc.email || "",
    accountType: normalizarCoachAccountType(userDoc.accountType || "owner"),
    teamRole: "",
    officeId: userDoc.officeId || "",
    territoryId: userDoc.territoryId || "",
    relationLabel: "Mi cuenta"
  };

  return [selfContact, ...contacts]
    .filter((item, index, array) => array.findIndex(other => String(other.id || "") === String(item.id || "")) === index)
    .sort((a, b) => a.name.localeCompare(b.name, "es", { sensitivity: "base" }));
}

async function registrarCoachCrmActivity(recordDoc = null, userDoc = null, type = "note", body = "", meta = null) {
  if (!recordDoc?.crmRecordId || !recordDoc?.ownerUserId || !body) {
    return null;
  }

  return CoachCrmActivity.create({
    crmRecordId: recordDoc.crmRecordId,
    ownerUserId: recordDoc.ownerUserId,
    actorUserId: userDoc?._id || null,
    actorName: userDoc?.name || userDoc?.email || "Coach",
    type,
    body: cleanText(body || "").slice(0, 1200),
    meta: meta && typeof meta === "object" ? meta : null,
    createdAt: new Date()
  });
}

async function aplicarCoachCrmActualizacionALeadSource(recordDoc = null, updates = {}) {
  if (!recordDoc?.sourceRecordId || !mongoose.Types.ObjectId.isValid(recordDoc.sourceRecordId)) {
    return;
  }

  const leadDoc = await CoachLeadInbox.findById(recordDoc.sourceRecordId);

  if (!leadDoc) {
    return;
  }

  const now = new Date();

  if (updates.status) {
    switch (normalizarCoachCrmStatus(updates.status)) {
      case "seguimiento":
      case "intentando":
      case "no_atendio":
        leadDoc.status = "contactado";
        break;
      case "cita_agendada":
      case "reagendada":
      case "ya_afuera":
      case "entro_a_casa":
      case "demo_hecha":
        leadDoc.status = "agendado";
        break;
      case "venta":
        leadDoc.status = "cliente";
        break;
      case "no_venta":
        leadDoc.status = "archivado";
        break;
      default:
        leadDoc.status = "nuevo";
        break;
    }

    leadDoc.lastStatusChangeAt = now;
  }

  if (Object.prototype.hasOwnProperty.call(updates || {}, "nextAction")) {
    leadDoc.nextAction = normalizarCoachLeadNextAction(updates.nextAction || "");
  }

  if (Object.prototype.hasOwnProperty.call(updates || {}, "nextActionAt")) {
    leadDoc.nextActionAt = parseCoachLeadNextActionAt(updates.nextActionAt);
  }

  if (Object.prototype.hasOwnProperty.call(updates || {}, "city")) {
    leadDoc.city = cleanText(updates.city || "").slice(0, 80);
  }

  if (Object.prototype.hasOwnProperty.call(updates || {}, "zipCode")) {
    leadDoc.zipCode = normalizarZipCode(updates.zipCode || "");
  }

  if (Object.prototype.hasOwnProperty.call(updates || {}, "privateNotesFull")) {
    leadDoc.notes = cleanText(updates.privateNotesFull || "").slice(0, 1200);
  }

  if (updates.note) {
    leadDoc.notes = leadDoc.notes ? `${leadDoc.notes}\n\n${updates.note}` : updates.note;
  }

  if (["contactado", "agendado", "cliente"].includes(leadDoc.status)) {
    leadDoc.lastContactAt = now;
  }

  leadDoc.summary = construirCoachLeadSummary(leadDoc);
  leadDoc.updatedAt = now;
  await leadDoc.save();
}

async function aplicarCoachCrmActualizacionAProgramaSource(recordDoc = null, updates = {}) {
  if (!recordDoc?.sourceParentId || !mongoose.Types.ObjectId.isValid(recordDoc.sourceParentId)) {
    return;
  }

  const sheetDoc = await CoachProgramSheet.findById(recordDoc.sourceParentId);

  if (!sheetDoc || !Array.isArray(sheetDoc.referrals)) {
    return;
  }

  const referralIndex = Number(recordDoc.sourceSubIndex ?? -1);
  const referral = Number.isInteger(referralIndex) && referralIndex >= 0 ? sheetDoc.referrals[referralIndex] : null;

  if (!referral) {
    return;
  }

  const now = new Date();

  if (updates.status) {
    referral.instantCallStatus = mapearCoachCrmStatusAPrograma414(updates.status);
    referral.lastOutcomeAt = now;
  }

  if (Object.prototype.hasOwnProperty.call(updates || {}, "privateNotesFull")) {
    referral.instantCallNotes = cleanText(updates.privateNotesFull || "").slice(0, 320);
  }

  if (updates.note) {
    referral.instantCallNotes = referral.instantCallNotes
      ? `${referral.instantCallNotes}\n\n${updates.note}`
      : updates.note;
  }

  if (Object.prototype.hasOwnProperty.call(updates || {}, "nextActionAt") && updates.nextActionAt) {
    referral.appointmentDetails = `Seguimiento: ${new Date(updates.nextActionAt).toLocaleString("en-US", {
      month: "2-digit",
      day: "2-digit",
      year: "numeric",
      hour: "numeric",
      minute: "2-digit"
    })}`;
  }

  sheetDoc.updatedAt = now;
  await sheetDoc.save();

  if (recordDoc.linkedLeadId && mongoose.Types.ObjectId.isValid(recordDoc.linkedLeadId)) {
    await aplicarCoachCrmActualizacionALeadSource(
      {
        ...recordDoc,
        sourceRecordId: String(recordDoc.linkedLeadId)
      },
      updates
    );
  }
}

async function aplicarCoachCrmActualizacionAFuentes(recordDoc = null, updates = {}) {
  if (!recordDoc?._id) {
    return;
  }

  if (recordDoc.sourceType === "lead") {
    await aplicarCoachCrmActualizacionALeadSource(recordDoc, updates);
  } else if (recordDoc.sourceType === "programa_4_en_14") {
    await aplicarCoachCrmActualizacionAProgramaSource(recordDoc, updates);
  }
}

function resolverCoachCrmStatusDesdeDemoOutcome(resultType = "", currentStatus = "") {
  const safeResult = normalizarCoachDemoOutcomeType(resultType || "");

  switch (safeResult) {
    case "venta":
      return "venta";
    case "no_venta":
      return "no_venta";
    case "no_atendio":
      return "no_atendio";
    case "follow_up":
      return "seguimiento";
    default:
      return normalizarCoachCrmStatus(currentStatus || "demo_hecha");
  }
}

async function vincularCoachCrmConEncuesta(recordDoc = null, surveyDoc = null, userDoc = null) {
  if (!recordDoc?._id || !surveyDoc?._id) {
    return null;
  }

  const now = new Date();
  const note = `Encuesta guardada${surveyDoc.fullName ? ` para ${surveyDoc.fullName}` : ""}.`;
  const nextStatus = ["venta", "no_venta"].includes(recordDoc.status)
    ? normalizarCoachCrmStatus(recordDoc.status)
    : "entro_a_casa";

  recordDoc.linkedHealthSurveyId = surveyDoc._id;
  recordDoc.linkedHealthSurveyName = surveyDoc.fullName || "";
  recordDoc.status = nextStatus;
  recordDoc.statusColor = resolverCoachCrmStatusColor(nextStatus);
  recordDoc.lastNote = note;
  if (!recordDoc.briefHistory && surveyDoc.summary) {
    recordDoc.briefHistory = truncarCoachCrmText(surveyDoc.summary, 320);
  }
  recordDoc.lastContactAt = now;
  recordDoc.updatedAt = now;
  await recordDoc.save();

  const updates = {
    status: nextStatus,
    note
  };

  await aplicarCoachCrmActualizacionAFuentes(recordDoc, updates);
  await registrarCoachCrmActivity(
    recordDoc,
    userDoc,
    "health_survey",
    `${note} ${truncarCoachCrmText(surveyDoc.summary || "", 240)}`.trim(),
    {
      surveyId: String(surveyDoc._id)
    }
  );

  return recordDoc;
}

async function vincularCoachCrmConPrograma(recordDoc = null, sheetDoc = null, userDoc = null) {
  if (!recordDoc?._id || !sheetDoc?._id) {
    return null;
  }

  const now = new Date();
  const referralCount = Number(sheetDoc.referralCount || 0);
  const note = `Hoja 4 en 14 guardada${referralCount ? ` con ${referralCount} referido(s)` : ""}.`;
  const nextStatus = ["venta", "no_venta", "demo_hecha"].includes(recordDoc.status)
    ? normalizarCoachCrmStatus(recordDoc.status)
    : "entro_a_casa";

  recordDoc.latestProgramSheetId = sheetDoc._id;
  recordDoc.latestProgramSummary = truncarCoachCrmText(sheetDoc.summary || note, 240);
  recordDoc.status = nextStatus;
  recordDoc.statusColor = resolverCoachCrmStatusColor(nextStatus);
  recordDoc.lastNote = note;
  recordDoc.lastContactAt = now;
  recordDoc.updatedAt = now;
  await recordDoc.save();

  const updates = {
    status: nextStatus,
    note
  };

  await aplicarCoachCrmActualizacionAFuentes(recordDoc, updates);
  await registrarCoachCrmActivity(
    recordDoc,
    userDoc,
    "program_4_in_14",
    `${note} ${truncarCoachCrmText(sheetDoc.summary || "", 240)}`.trim(),
    {
      sheetId: String(sheetDoc._id),
      referralCount
    }
  );

  return recordDoc;
}

async function vincularCoachCrmConResultadoDemo(recordDoc = null, outcomeDoc = null, userDoc = null) {
  if (!recordDoc?._id || !outcomeDoc?._id) {
    return null;
  }

  const now = new Date();
  const nextStatus = resolverCoachCrmStatusDesdeDemoOutcome(outcomeDoc.resultType || "", recordDoc.status || "");
  const note =
    truncarCoachCrmText(outcomeDoc.summary || construirResumenCoachDemoOutcome(outcomeDoc), 240) ||
    `Resultado guardado: ${formatearCoachDemoOutcomeLabel(outcomeDoc.resultType || "")}.`;

  recordDoc.latestDemoOutcomeId = outcomeDoc._id;
  recordDoc.latestDemoOutcomeType = normalizarCoachDemoOutcomeType(outcomeDoc.resultType || "");
  recordDoc.latestDemoOutcomeSummary = truncarCoachCrmText(outcomeDoc.summary || note, 240);
  recordDoc.status = nextStatus;
  recordDoc.statusColor = resolverCoachCrmStatusColor(nextStatus);
  recordDoc.lastNote = note;
  recordDoc.lastContactAt = now;
  recordDoc.saleAmount = limpiarCoachDemoOutcomeAmount(outcomeDoc.saleAmount || 0);
  recordDoc.saleResult = nextStatus === "venta" ? "venta" : nextStatus === "no_venta" ? "no_venta" : "";
  recordDoc.closedAt = ["venta", "no_venta"].includes(nextStatus) ? now : null;
  recordDoc.soldAt = nextStatus === "venta" ? now : null;

  if (outcomeDoc.surveyId) {
    recordDoc.linkedHealthSurveyId = outcomeDoc.surveyId;
    recordDoc.linkedHealthSurveyName = outcomeDoc.surveyName || recordDoc.linkedHealthSurveyName || "";
  }

  recordDoc.updatedAt = now;
  await recordDoc.save();

  const updates = {
    status: nextStatus,
    note
  };

  await aplicarCoachCrmActualizacionAFuentes(recordDoc, updates);
  await registrarCoachCrmActivity(
    recordDoc,
    userDoc,
    "demo_outcome",
    note,
    {
      outcomeId: String(outcomeDoc._id),
      resultType: normalizarCoachDemoOutcomeType(outcomeDoc.resultType || "")
    }
  );

  return recordDoc;
}

async function obtenerCoachCrmWorkspace(userDoc = null, filters = {}) {
  if (!userDoc?._id) {
    return {
      summary: construirCoachCrmSummary([]),
      records: [],
      assignees: []
    };
  }

  try {
    await asegurarCoachCrmWorkspaceSiVacio(userDoc);
  } catch (error) {
    console.error("Error inicializando workspace CRM del Coach:", error.message);
  }
  const baseQuery = await construirCoachCrmViewerQuery(userDoc);
  const query = {
    ...baseQuery
  };
  const sourceType = normalizarCoachCrmSourceType(filters?.sourceType || "");
  const status = normalizarCoachCrmStatus(filters?.status || "");

  if (filters?.sourceType) {
    query.sourceType = sourceType;
  }

  if (filters?.status) {
    query.status = status;
  }

  if (filters?.assignedToUserId && mongoose.Types.ObjectId.isValid(filters.assignedToUserId)) {
    query.$or = [
      { assignedTelemarketerUserId: filters.assignedToUserId },
      { appointmentRepUserId: filters.assignedToUserId }
    ];
  }

  const [recordDocs, allRecordDocs, assignees] = await Promise.all([
    CoachCrmRecord.find(query)
      .sort({ updatedAt: -1, nextActionAt: 1, createdAt: -1 })
      .limit(500)
      .lean(),
    CoachCrmRecord.find(baseQuery)
      .sort({ updatedAt: -1 })
      .limit(800)
      .lean(),
    obtenerCoachCrmAssignableContacts(userDoc)
  ]);

  return {
    summary: construirCoachCrmSummary(allRecordDocs),
    records: recordDocs.map(limpiarCoachCrmRecord).filter(Boolean),
    assignees
  };
}

async function obtenerCoachCrmRecordDetail(userDoc = null, recordId = "") {
  if (!userDoc?._id || !recordId || !mongoose.Types.ObjectId.isValid(recordId)) {
    return null;
  }

  try {
    await asegurarCoachCrmWorkspaceSiVacio(userDoc);
  } catch (error) {
    console.error("Error inicializando detalle CRM del Coach:", error.message);
  }

  const recordQuery = await construirCoachCrmViewerQuery(userDoc, {
    _id: recordId
  });
  const recordDoc = await CoachCrmRecord.findOne(recordQuery).lean();

  if (!recordDoc?._id) {
    return null;
  }

  const activityDocs = await CoachCrmActivity.find({
    crmRecordId: recordDoc.crmRecordId
  })
    .sort({ createdAt: -1 })
    .limit(25)
    .lean();

  return {
    record: limpiarCoachCrmRecord(recordDoc),
    activities: activityDocs.map(limpiarCoachCrmActivity).filter(Boolean)
  };
}

function construirCoachAgendaWorkspaceQuery(userDoc = null) {
  const query = {
    ...construirCoachCrmWorkspaceQuery(userDoc),
    status: {
      $in: ["cita_agendada", "reagendada", "ya_afuera", "entro_a_casa", "demo_hecha"]
    }
  };

  if (normalizarCoachAccountType(userDoc?.accountType || "owner") === "seat" && userDoc?._id) {
    query.$or = [
      { appointmentRepUserId: userDoc._id },
      {
        appointmentRepUserId: null,
        generatedByUserId: userDoc._id
      }
    ];
  }

  return query;
}

function construirCoachAgendaMetricsQuery(userDoc = null) {
  const query = {
    ...construirCoachCrmWorkspaceQuery(userDoc)
  };

  if (normalizarCoachAccountType(userDoc?.accountType || "owner") === "seat" && userDoc?._id) {
    query.$or = [
      { appointmentRepUserId: userDoc._id },
      {
        appointmentRepUserId: null,
        generatedByUserId: userDoc._id
      }
    ];
  }

  return query;
}

function construirCoachAgendaMapsUrl(recordDoc = null) {
  const query = [recordDoc?.address || "", recordDoc?.city || "", recordDoc?.zipCode || ""]
    .map(item => cleanText(item || "").trim())
    .filter(Boolean)
    .join(", ");

  if (!query) {
    return "";
  }

  return `https://www.google.com/maps/search/?api=1&query=${encodeURIComponent(query)}`;
}

function limpiarCoachAgendaRecord(recordDoc = null) {
  const cleaned = limpiarCoachCrmRecord(recordDoc);

  if (!cleaned) {
    return null;
  }

  const appointmentAt = cleaned.appointmentAt || cleaned.nextActionAt || null;
  const phoneDigits = formatearTelefonoE164(cleaned.phone || "");

  return {
    ...cleaned,
    appointmentAt,
    phoneHref: phoneDigits || "",
    mapsUrl: construirCoachAgendaMapsUrl(cleaned)
  };
}

async function obtenerCoachAgendaWorkspace(userDoc = null) {
  if (!userDoc?._id) {
    return {
      viewerMode: "rep",
      summary: {
        today: 0,
        week: 0,
        pendingResults: 0,
        outside: 0
      },
      today: [],
      week: []
    };
  }

  try {
    await asegurarCoachCrmWorkspaceSiVacio(userDoc);
  } catch (error) {
    console.error("Error inicializando agenda CRM del Coach:", error.message);
  }
  const query = construirCoachAgendaWorkspaceQuery(userDoc);
  const recordDocs = await CoachCrmRecord.find(query)
    .sort({ appointmentAt: 1, nextActionAt: 1, updatedAt: -1 })
    .limit(250)
    .lean();

  const items = recordDocs
    .map(limpiarCoachAgendaRecord)
    .filter(item => item?.appointmentAt)
    .sort((a, b) => new Date(a.appointmentAt).getTime() - new Date(b.appointmentAt).getTime());

  const now = new Date();
  const startOfToday = obtenerInicioDia(now);
  const endOfToday = obtenerFinDia(now);
  const endOfWeek = obtenerFinDia(new Date(now.getTime() + 6 * 24 * 60 * 60 * 1000));
  const metricsQuery = {
    ...construirCoachAgendaMetricsQuery(userDoc),
    appointmentAt: {
      $gte: startOfToday,
      $lte: endOfWeek
    }
  };
  const weekMetricDocs = await CoachCrmRecord.find(metricsQuery)
    .sort({ appointmentAt: 1, updatedAt: -1 })
    .limit(400)
    .lean();
  const weekMetricItems = weekMetricDocs.map(limpiarCoachCrmRecord).filter(Boolean);
  const representativeBuckets = new Map();

  weekMetricItems.forEach(record => {
    const repKey = record.appointmentRepUserId || record.generatedByUserId || "";
    const repLabel = record.appointmentRepName || record.generatedByName || "";
    registrarCoachCrmPerformanceBucket(representativeBuckets, repKey, repLabel, record);
  });

  const today = items.filter(item => {
    const appointmentDate = new Date(item.appointmentAt);
    return appointmentDate >= startOfToday && appointmentDate <= endOfToday;
  });
  const week = items.filter(item => {
    const appointmentDate = new Date(item.appointmentAt);
    return appointmentDate >= startOfToday && appointmentDate <= endOfWeek;
  });

  return {
    viewerMode: esCoachTeamManager(userDoc) ? "manager" : "rep",
    summary: {
      today: today.length,
      week: week.length,
      pendingResults: items.filter(item =>
        ["cita_agendada", "reagendada", "ya_afuera", "entro_a_casa"].includes(item.status)
      ).length,
      outside: items.filter(item => item.status === "ya_afuera").length,
      inside: weekMetricItems.filter(item => ["entro_a_casa", "demo_hecha", "venta"].includes(item.status)).length,
      completedWeek: weekMetricItems.filter(item => ["demo_hecha", "venta", "no_venta"].includes(item.status)).length,
      salesWeek: weekMetricItems.filter(item => item.status === "venta").length,
      soldWeekAmount: weekMetricItems.reduce(
        (sum, item) => sum + limpiarCoachDemoOutcomeAmount(item.saleAmount || 0),
        0
      ),
      topRepresentatives: construirCoachCrmPerformanceTopList(representativeBuckets)
    },
    today,
    week
  };
}

async function obtenerControlCommunicationsOverview() {
  const [territoryDocs, announcementDocs, threadDocs] = await Promise.all([
    CoachTerritory.find({ status: "active" })
      .sort({ createdAt: -1 })
      .select("name officeId territoryId")
      .lean(),
    CoachAnnouncement.find({ status: "active" })
      .sort({ createdAt: -1 })
      .limit(20)
      .lean(),
    CoachSupportThread.find({})
      .sort({ supportUnreadCount: -1, lastMessageAt: -1, createdAt: -1 })
      .limit(18)
      .lean()
  ]);

  const threadIds = threadDocs.map(item => item._id).filter(Boolean);
  const messages = threadIds.length
    ? await CoachSupportMessage.find({ threadId: { $in: threadIds } })
        .sort({ createdAt: -1 })
        .lean()
    : [];
  const messagesByThread = new Map();

  for (const message of messages) {
    const threadId = String(message.threadId || "");
    const current = messagesByThread.get(threadId) || [];
    if (current.length >= 8) {
      continue;
    }
    current.push(message);
    messagesByThread.set(threadId, current);
  }

  return {
    territories: territoryDocs.map(item => ({
      id: String(item._id),
      name: item.name || "",
      officeId: item.officeId || "",
      territoryId: item.territoryId || ""
    })),
    announcements: announcementDocs.map(item => limpiarCoachAnnouncement(item)).filter(Boolean),
    supportThreads: threadDocs.map(threadDoc =>
      limpiarCoachSupportThread(threadDoc, {
        messages: (messagesByThread.get(String(threadDoc._id)) || [])
          .slice()
          .reverse()
          .map(item => limpiarCoachSupportMessage(item))
          .filter(Boolean)
      })
    )
  };
}

function normalizarCoachDemoOutcomeType(value = "") {
  const safeValue = String(value || "")
    .trim()
    .toLowerCase();
  const validValues = ["venta", "follow_up", "no_venta", "no_atendio"];
  return validValues.includes(safeValue) ? safeValue : "follow_up";
}

function formatearCoachDemoOutcomeLabel(value = "") {
  const labels = {
    venta: "Venta",
    follow_up: "Follow up",
    no_venta: "No venta",
    no_atendio: "No atendio"
  };

  return labels[normalizarCoachDemoOutcomeType(value)] || "Resultado";
}

function limpiarCoachDemoOutcomeAmount(value = 0) {
  const parsed = Number(value);
  return Number.isFinite(parsed) && parsed > 0 ? Number(parsed.toFixed(2)) : 0;
}

function limpiarCoachDemoOutcomeProducts(value = []) {
  const items = Array.isArray(value)
    ? value
    : String(value || "")
        .split(/[\n,;]+/g)
        .map(item => item.trim());

  return Array.from(
    new Set(
      items
        .map(item => truncarTextoPrompt(String(item || "").trim(), 120))
        .filter(Boolean)
        .slice(0, 8)
    )
  );
}

function limpiarCoachDemoOutcomeReason(value = "") {
  return truncarTextoPrompt(String(value || "").trim(), 320);
}

function limpiarCoachDemoOutcomeContext(context = null) {
  if (!context || typeof context !== "object") {
    return null;
  }

  const saleAmount = limpiarCoachDemoOutcomeAmount(context.saleAmount || 0);
  const products = limpiarCoachDemoOutcomeProducts(context.products || []);
  const privateReason = limpiarCoachDemoOutcomeReason(context.privateReason || "");
  const activeDemoStage = truncarTextoPrompt(String(context.activeDemoStage || "").trim(), 80);
  const activeDemoStageLabel = truncarTextoPrompt(String(context.activeDemoStageLabel || "").trim(), 120);
  const summary = truncarTextoPrompt(String(context.summary || "").trim(), 220);
  const rawResultType = String(context.resultType || "").trim();

  if (!rawResultType && !saleAmount && !products.length && !privateReason && !summary) {
    return null;
  }

  const resultType = normalizarCoachDemoOutcomeType(rawResultType);

  return {
    id: mongoose.Types.ObjectId.isValid(context.id || "") ? String(context.id) : "",
    ownerUserId: String(context.ownerUserId || "").trim(),
    resultType,
    saleAmount,
    products,
    privateReason,
    activeDemoStage,
    activeDemoStageLabel,
    summary
  };
}

function construirResumenCoachDemoOutcome(outcome = null) {
  if (!outcome) {
    return "";
  }

  const parts = [`Resultado: ${formatearCoachDemoOutcomeLabel(outcome.resultType)}.`];

  if (limpiarCoachDemoOutcomeAmount(outcome.saleAmount) > 0) {
    parts.push(`Monto: ${limpiarCoachDemoOutcomeAmount(outcome.saleAmount)}.`);
  }

  if (Array.isArray(outcome.products) && outcome.products.length) {
    parts.push(`Productos: ${outcome.products.join(", ")}.`);
  }

  if (outcome.activeDemoStageLabel) {
    parts.push(`Paso: ${outcome.activeDemoStageLabel}.`);
  }

  if (outcome.privateReason) {
    parts.push(`Nota privada: ${outcome.privateReason}.`);
  }

  return parts.join(" ").trim();
}

function limpiarCoachDemoOutcome(outcomeDoc = null, options = {}) {
  if (!outcomeDoc?._id) {
    return null;
  }

  const includeGenerator = Boolean(options.includeGenerator);
  const includeSponsor = Boolean(options.includeSponsor);
  const saleAmount = limpiarCoachDemoOutcomeAmount(outcomeDoc.saleAmount || 0);
  const sponsorCommissionAmount = limpiarCoachDemoOutcomeAmount(outcomeDoc.sponsorCommissionAmount || 0);

  return {
    id: String(outcomeDoc._id),
    linkedCrmRecordId: outcomeDoc.linkedCrmRecordId ? String(outcomeDoc.linkedCrmRecordId) : "",
    ownerUserId: outcomeDoc.ownerUserId ? String(outcomeDoc.ownerUserId) : "",
    ownerName: outcomeDoc.ownerName || "",
    generatedByUserId: outcomeDoc.generatedByUserId ? String(outcomeDoc.generatedByUserId) : "",
    generatedByName: includeGenerator ? outcomeDoc.generatedByName || "" : "",
    generatedByAccountType: includeGenerator
      ? normalizarCoachAccountType(outcomeDoc.generatedByAccountType || "owner")
      : "",
    resultType: normalizarCoachDemoOutcomeType(outcomeDoc.resultType || ""),
    resultLabel: formatearCoachDemoOutcomeLabel(outcomeDoc.resultType || ""),
    saleAmount,
    products: limpiarCoachDemoOutcomeProducts(outcomeDoc.products || []),
    privateReason: outcomeDoc.privateReason || "",
    summary:
      truncarTextoPrompt(String(outcomeDoc.summary || "").trim(), 220) ||
      construirResumenCoachDemoOutcome(outcomeDoc),
    activeDemoStage: outcomeDoc.activeDemoStage || "",
    activeDemoStageLabel: outcomeDoc.activeDemoStageLabel || "",
    sponsorCommissionAmount: includeSponsor ? sponsorCommissionAmount : 0,
    sponsorName: includeSponsor ? outcomeDoc.sponsorName || "" : "",
    createdAt: outcomeDoc.createdAt || null,
    updatedAt: outcomeDoc.updatedAt || null
  };
}

async function obtenerCoachDemoOutcomeSummary(match = {}) {
  const [summary] = await CoachDemoOutcome.aggregate([
    { $match: match },
    {
      $group: {
        _id: null,
        totalResults: { $sum: 1 },
        salesCount: {
          $sum: { $cond: [{ $eq: ["$resultType", "venta"] }, 1, 0] }
        },
        followUpCount: {
          $sum: { $cond: [{ $eq: ["$resultType", "follow_up"] }, 1, 0] }
        },
        noSaleCount: {
          $sum: { $cond: [{ $eq: ["$resultType", "no_venta"] }, 1, 0] }
        },
        noAnswerCount: {
          $sum: { $cond: [{ $eq: ["$resultType", "no_atendio"] }, 1, 0] }
        },
        soldAmount: {
          $sum: {
            $cond: [{ $eq: ["$resultType", "venta"] }, "$saleAmount", 0]
          }
        },
        estimatedCommission: { $sum: "$sponsorCommissionAmount" }
      }
    }
  ]);

  return {
    totalResults: Number(summary?.totalResults || 0),
    salesCount: Number(summary?.salesCount || 0),
    followUpCount: Number(summary?.followUpCount || 0),
    noSaleCount: Number(summary?.noSaleCount || 0),
    noAnswerCount: Number(summary?.noAnswerCount || 0),
    soldAmount: limpiarCoachDemoOutcomeAmount(summary?.soldAmount || 0),
    estimatedCommission: limpiarCoachDemoOutcomeAmount(summary?.estimatedCommission || 0)
  };
}

async function obtenerCoachSponsorOverview(userDoc = null) {
  if (!userDoc?._id) {
    return {
      sponsoredAccounts: 0,
      salesCount: 0,
      soldAmount: 0,
      estimatedCommission: 0,
      recentSales: []
    };
  }

  const sponsorUserId = userDoc._id;
  const [sponsoredAccounts, summary, recentSalesDocs] = await Promise.all([
    CoachUser.countDocuments({
      sponsorUserId,
      accountType: { $in: ["owner", "leader"] }
    }),
    obtenerCoachDemoOutcomeSummary({
      sponsorUserId,
      resultType: "venta"
    }),
    CoachDemoOutcome.find({
      sponsorUserId,
      resultType: "venta"
    })
      .sort({ createdAt: -1 })
      .limit(8)
      .lean()
  ]);

  return {
    sponsoredAccounts,
    salesCount: Number(summary.salesCount || 0),
    soldAmount: limpiarCoachDemoOutcomeAmount(summary.soldAmount || 0),
    estimatedCommission: limpiarCoachDemoOutcomeAmount(summary.estimatedCommission || 0),
    recentSales: recentSalesDocs.map(doc => limpiarCoachDemoOutcome(doc, { includeGenerator: true, includeSponsor: true }))
  };
}

async function obtenerControlTowerTrackedAccounts(viewerUserDoc = null) {
  const watchedEmails = obtenerCoachTrackedEmails();
  const presetMap = new Map(obtenerCoachAccountPresets().map(item => [item.email, item]));
  const ownerMatch = [];

  if (watchedEmails.length) {
    ownerMatch.push({ email: { $in: watchedEmails } });
  }

  if (viewerUserDoc?._id) {
    ownerMatch.push({
      sponsorUserId: viewerUserDoc._id,
      accountType: { $in: ["owner", "leader"] }
    });
  }

  if (!ownerMatch.length) {
    return {
      summary: {
        trackedAccounts: 0,
        activeAccounts: 0,
        pendingAccounts: 0,
        activeSeats: 0,
        totalSeats: 0,
        territories: [],
        syncedLeads: 0,
        openOpportunities: 0
      },
      accounts: []
    };
  }

  const ownerDocs = await CoachUser.find({
    $or: ownerMatch,
    accountType: { $in: ["owner", "leader"] }
  })
    .sort({ createdAt: -1, name: 1 })
    .lean();
  const ownerIds = ownerDocs.map(doc => doc._id).filter(Boolean);

  const [profileDocs, seatDocs, leadAgg, highLevelAgg, salesAgg] = await Promise.all([
    ownerIds.length
      ? CoachDistributorProfile.find({ userId: { $in: ownerIds } }).lean()
      : Promise.resolve([]),
    ownerIds.length
      ? CoachUser.find({
          accountType: "seat",
          teamOwnerUserId: { $in: ownerIds }
        })
          .select("teamOwnerUserId seatStatus")
          .lean()
      : Promise.resolve([]),
    ownerIds.length
      ? CoachLeadInbox.aggregate([
          {
            $match: {
              ownerUserId: { $in: ownerIds }
            }
          },
          {
            $group: {
              _id: "$ownerUserId",
              totalLeads: { $sum: 1 },
              syncedLeads: {
                $sum: {
                  $cond: [{ $and: [{ $ne: ["$highLevelContactId", null] }, { $ne: ["$highLevelContactId", ""] }] }, 1, 0]
                }
              },
              openOpportunities: {
                $sum: {
                  $cond: [
                    {
                      $and: [
                        { $ne: ["$highLevelOpportunityId", null] },
                        { $ne: ["$highLevelOpportunityId", ""] },
                        { $ne: ["$highLevelOpportunityStatus", "won"] },
                        { $ne: ["$highLevelOpportunityStatus", "lost"] }
                      ]
                    },
                    1,
                    0
                  ]
                }
              }
            }
          }
        ])
      : Promise.resolve([]),
    ownerIds.length
      ? CoachLeadInbox.aggregate([
          {
            $match: {
              ownerUserId: { $in: ownerIds },
              $or: [
                { highLevelCampaignId: { $exists: true, $ne: "" } },
                { highLevelCampaignStatus: { $exists: true, $ne: "" } }
              ]
            }
          },
          {
            $group: {
              _id: "$ownerUserId",
              campaignSignals: { $sum: 1 }
            }
          }
        ])
      : Promise.resolve([]),
    ownerIds.length
      ? CoachDemoOutcome.aggregate([
          {
            $match: {
              ownerUserId: { $in: ownerIds },
              resultType: "venta"
            }
          },
          {
            $group: {
              _id: "$ownerUserId",
              salesCount: { $sum: 1 },
              soldAmount: { $sum: "$saleAmount" }
            }
          }
        ])
      : Promise.resolve([])
  ]);

  const profileMap = new Map(profileDocs.map(doc => [String(doc.userId), doc]));
  const seatStatsMap = new Map();
  const leadMap = new Map(leadAgg.map(item => [String(item._id), item]));
  const highLevelMap = new Map(highLevelAgg.map(item => [String(item._id), item]));
  const salesMap = new Map(salesAgg.map(item => [String(item._id), item]));

  for (const seat of seatDocs) {
    const ownerId = String(seat.teamOwnerUserId || "");

    if (!ownerId) {
      continue;
    }

    const current = seatStatsMap.get(ownerId) || { totalSeats: 0, activeSeats: 0 };
    current.totalSeats += 1;

    if (normalizarCoachSeatStatus(seat.seatStatus || "active") === "active") {
      current.activeSeats += 1;
    }

    seatStatsMap.set(ownerId, current);
  }

  const accounts = ownerDocs.map(userDoc => {
    const ownerId = String(userDoc._id);
    const profileDoc = profileMap.get(ownerId) || null;
    const preset = presetMap.get(normalizarEmail(userDoc.email || "")) || null;
    const seatStats = seatStatsMap.get(ownerId) || { totalSeats: 0, activeSeats: 0 };
    const leadStats = leadMap.get(ownerId) || { totalLeads: 0, syncedLeads: 0, openOpportunities: 0 };
    const campaignStats = highLevelMap.get(ownerId) || { campaignSignals: 0 };
    const salesStats = salesMap.get(ownerId) || { salesCount: 0, soldAmount: 0 };
    const status = coachTieneAcceso(userDoc.subscriptionStatus) || coachTieneAccesoDePrueba(userDoc.email)
      ? "activa"
      : userDoc.subscriptionStatus || "pendiente";

    return {
      email: userDoc.email || "",
      accountId: ownerId,
      name: userDoc.name || profileDoc?.name || preset?.label || "Cuenta vigilada",
      status,
      accountType: normalizarCoachAccountType(userDoc.accountType || "owner"),
      teamRole: profileDoc?.teamRole || preset?.teamRole || "",
      seatLabel: profileDoc?.seatLabel || preset?.seatLabel || "",
      officeId: userDoc.officeId || preset?.officeId || "",
      territoryId: userDoc.territoryId || preset?.territoryId || "",
      totalSeats: seatStats.totalSeats,
      activeSeats: seatStats.activeSeats,
      totalLeads: Number(leadStats.totalLeads || 0),
      syncedLeads: Number(leadStats.syncedLeads || 0),
      openOpportunities: Number(leadStats.openOpportunities || 0),
      campaignSignals: Number(campaignStats.campaignSignals || 0),
      salesCount: Number(salesStats.salesCount || 0),
      soldAmount: limpiarCoachDemoOutcomeAmount(salesStats.soldAmount || 0),
      sponsorLinked: Boolean(userDoc.sponsorUserId),
      sponsorName: userDoc.sponsorName || "",
      updatedAt: userDoc.updatedAt || userDoc.createdAt || null
    };
  });

  const existingEmails = new Set(accounts.map(item => normalizarEmail(item.email)));

  for (const email of watchedEmails) {
    if (existingEmails.has(email)) {
      continue;
    }

    const preset = presetMap.get(email) || null;
    accounts.push({
      email,
      accountId: "",
      name: preset?.label || "Pendiente de registro",
      status: "pendiente_registro",
      accountType: "owner",
      teamRole: preset?.teamRole || "",
      seatLabel: preset?.seatLabel || "",
      officeId: preset?.officeId || "",
      territoryId: preset?.territoryId || "",
      totalSeats: 0,
      activeSeats: 0,
      totalLeads: 0,
      syncedLeads: 0,
      openOpportunities: 0,
      campaignSignals: 0,
      salesCount: 0,
      soldAmount: 0,
      sponsorLinked: false,
      sponsorName: "",
      updatedAt: null
    });
  }

  accounts.sort((a, b) => {
    const aPending = a.status === "pendiente_registro" ? 1 : 0;
    const bPending = b.status === "pendiente_registro" ? 1 : 0;

    if (aPending !== bPending) {
      return aPending - bPending;
    }

    return (a.name || "").localeCompare(b.name || "", "es");
  });

  const territories = Array.from(
    new Set(
      accounts
        .flatMap(item => [item.territoryId || "", item.officeId || ""])
        .map(item => cleanText(item))
        .filter(Boolean)
    )
  );

  return {
    summary: {
      trackedAccounts: accounts.length,
      activeAccounts: accounts.filter(item => item.status !== "pendiente_registro").length,
      pendingAccounts: accounts.filter(item => item.status === "pendiente_registro").length,
      totalSeats: accounts.reduce((sum, item) => sum + Number(item.totalSeats || 0), 0),
      activeSeats: accounts.reduce((sum, item) => sum + Number(item.activeSeats || 0), 0),
      territories,
      syncedLeads: accounts.reduce((sum, item) => sum + Number(item.syncedLeads || 0), 0),
      openOpportunities: accounts.reduce((sum, item) => sum + Number(item.openOpportunities || 0), 0)
    },
    accounts
  };
}

function construirPromptResultadoDemoActivo(context = null) {
  if (!context?.resultType) {
    return "";
  }

  const productsCopy = Array.isArray(context.products) && context.products.length ? context.products.join(" | ") : "sin productos";

  return `
RESULTADO DE DEMO ACTIVO EN ESTA SESION:
- resultado_demo_activo: si
- resultado_final: ${formatearCoachDemoOutcomeLabel(context.resultType)}
- monto_reportado: ${limpiarCoachDemoOutcomeAmount(context.saleAmount || 0)}
- productos_reportados: ${productsCopy}
- paso_al_reportar: ${context.activeDemoStageLabel || context.activeDemoStage || "sin paso"}
- nota_privada: ${context.privateReason || "sin nota"}
- resumen: ${context.summary || "sin resumen"}

INSTRUCCION:
- si el distribuidor sigue escribiendo despues de reportar este resultado, responde desde este estado real y no como si la demo siguiera en cero
- si fue venta, enfocate en post cierre, negocio, seguimiento o siguiente paso
- si fue follow up, no venta o no atendio, usa la nota privada y el paso reportado para sugerir el siguiente movimiento con mas precision
`;
}

function normalizarCoachPrivateResourceSlot(slot = "") {
  const normalized = String(slot || "")
    .trim()
    .toLowerCase();

  return COACH_PRIVATE_RESOURCE_SLOTS[normalized] ? normalized : "";
}

function limpiarPinCoachPrivateResource(pin = "") {
  const limpio = String(pin || "")
    .replace(/\D/g, "")
    .slice(0, 4);

  return /^\d{4}$/.test(limpio) ? limpio : "";
}

function construirHashPinCoachPrivateResource(userId = "", slotType = "", pin = "") {
  return crypto.createHash("sha256").update(`${String(userId)}:${slotType}:${pin}`).digest("hex");
}

function normalizarNombreArchivoPdf(value = "", fallback = "archivo.pdf") {
  const limpio = String(value || "")
    .replace(/[^\w.\- ]+/g, "")
    .replace(/\s+/g, "-")
    .replace(/-+/g, "-")
    .replace(/^-|-$/g, "")
    .slice(0, 120);

  const base = limpio || fallback.replace(/\.pdf$/i, "");
  return /\.pdf$/i.test(base) ? base : `${base}.pdf`;
}

function extraerPdfBufferCoachPrivateResource(value = "") {
  const raw = String(value || "").trim();

  if (!raw) {
    return null;
  }

  const match = raw.match(/^data:application\/pdf;base64,(.+)$/i) || raw.match(/^data:.*;base64,(.+)$/i);
  const base64 = (match ? match[1] : raw).replace(/\s+/g, "");

  try {
    const buffer = Buffer.from(base64, "base64");

    if (!buffer.length || buffer.slice(0, 4).toString("utf8") !== "%PDF") {
      return null;
    }

    return buffer;
  } catch (error) {
    return null;
  }
}

function limpiarCoachPrivateResource(resourceDoc = null, slotFallback = "") {
  const slotType = normalizarCoachPrivateResourceSlot(resourceDoc?.slotType || slotFallback);
  const config = COACH_PRIVATE_RESOURCE_SLOTS[slotType] || {};

  return {
    slotType,
    label: config.label || "Archivo privado",
    hasFile: Boolean(resourceDoc?.fileData?.length || resourceDoc?.fileSize),
    fileName: resourceDoc?.fileName || "",
    mimeType: resourceDoc?.mimeType || "application/pdf",
    fileSize: Number(resourceDoc?.fileSize || 0),
    uploadedAt: resourceDoc?.uploadedAt || resourceDoc?.updatedAt || null,
    updatedAt: resourceDoc?.updatedAt || null
  };
}

function construirMapaCoachPrivateResources(resourceDocs = []) {
  const docs = Array.isArray(resourceDocs) ? resourceDocs : [];
  const map = {};

  for (const slotType of Object.keys(COACH_PRIVATE_RESOURCE_SLOTS)) {
    const found = docs.find(item => normalizarCoachPrivateResourceSlot(item?.slotType) === slotType);
    map[slotType] = limpiarCoachPrivateResource(found, slotType);
  }

  return map;
}

function construirCoachLeadDeliveryPayload(userDoc = null, lead = null, destination = null) {
  return {
    app: "Agustin 2.0 Coach",
    kind: "lead",
    sentAt: new Date().toISOString(),
    owner: {
      id: userDoc?._id ? String(userDoc._id) : "",
      name: userDoc?.name || "",
      email: userDoc?.email || ""
    },
    destination: {
      type: destination?.type || "carpeta_privada",
      label: destination?.label || ""
    },
    lead: {
      id: lead?.id || "",
      fullName: lead?.fullName || "",
      phone: lead?.phone || "",
      email: lead?.email || "",
      city: lead?.city || "",
      zipCode: lead?.zipCode || "",
      interest: lead?.interest || "",
      source: lead?.source || "",
      status: lead?.status || "",
      nextAction: lead?.nextAction || "",
      nextActionAt: lead?.nextActionAt || null,
      notes: lead?.notes || "",
      summary: lead?.summary || "",
      createdAt: lead?.createdAt || null,
      updatedAt: lead?.updatedAt || null
    }
  };
}

function construirCorreoLeadCoach(userDoc = null, lead = null, destination = null) {
  const ownerName = userDoc?.name || "Distribuidor";
  const leadName = lead?.fullName || "Lead nuevo";
  const subject = `Nuevo lead para ${ownerName}: ${leadName}`;
  const nextActionCopy = lead?.nextAction ? String(lead.nextAction).replace(/_/g, " ") : "sin proxima accion";
  const nextActionAtCopy = lead?.nextActionAt ? new Date(lead.nextActionAt).toLocaleString("es-US") : "sin fecha";
  const lines = [
    `Hola ${ownerName},`,
    "",
    "Te acaba de entrar un lead nuevo en Agustin 2.0 Coach.",
    "",
    `Nombre: ${lead?.fullName || "Sin nombre"}`,
    `Telefono: ${lead?.phone || "Sin telefono"}`,
    `Correo: ${lead?.email || "Sin correo"}`,
    `Ciudad: ${lead?.city || "Sin ciudad"}`,
    `ZIP Code: ${lead?.zipCode || "Sin ZIP"}`,
    `Interes: ${lead?.interest || "Sin interes"}`,
    `Fuente: ${lead?.source || "Sin fuente"}`,
    `Estado: ${lead?.status || "nuevo"}`,
    `Proxima accion: ${nextActionCopy}`,
    `Cuando: ${nextActionAtCopy}`,
    "",
    `Resumen: ${lead?.summary || "Sin resumen"}`,
    "",
    `Notas: ${lead?.notes || "Sin notas"}`,
    "",
    "El lead tambien quedo guardado en tu carpeta privada dentro del Coach."
  ];

  const html = `
    <div style="font-family:Arial,sans-serif;padding:24px;color:#0f172a">
      <h2 style="margin:0 0 12px">Nuevo lead para ${escapeXml(ownerName)}</h2>
      <p style="margin:0 0 18px;color:#475569">Te acaba de entrar un lead nuevo en Agustin 2.0 Coach.</p>
      <table style="width:100%;border-collapse:collapse">
        <tr><td style="padding:8px;border:1px solid #e2e8f0"><strong>Nombre</strong></td><td style="padding:8px;border:1px solid #e2e8f0">${escapeXml(lead?.fullName || "Sin nombre")}</td></tr>
        <tr><td style="padding:8px;border:1px solid #e2e8f0"><strong>Telefono</strong></td><td style="padding:8px;border:1px solid #e2e8f0">${escapeXml(lead?.phone || "Sin telefono")}</td></tr>
        <tr><td style="padding:8px;border:1px solid #e2e8f0"><strong>Correo</strong></td><td style="padding:8px;border:1px solid #e2e8f0">${escapeXml(lead?.email || "Sin correo")}</td></tr>
        <tr><td style="padding:8px;border:1px solid #e2e8f0"><strong>Ciudad</strong></td><td style="padding:8px;border:1px solid #e2e8f0">${escapeXml(lead?.city || "Sin ciudad")}</td></tr>
        <tr><td style="padding:8px;border:1px solid #e2e8f0"><strong>ZIP Code</strong></td><td style="padding:8px;border:1px solid #e2e8f0">${escapeXml(lead?.zipCode || "Sin ZIP")}</td></tr>
        <tr><td style="padding:8px;border:1px solid #e2e8f0"><strong>Interes</strong></td><td style="padding:8px;border:1px solid #e2e8f0">${escapeXml(lead?.interest || "Sin interes")}</td></tr>
        <tr><td style="padding:8px;border:1px solid #e2e8f0"><strong>Fuente</strong></td><td style="padding:8px;border:1px solid #e2e8f0">${escapeXml(lead?.source || "Sin fuente")}</td></tr>
        <tr><td style="padding:8px;border:1px solid #e2e8f0"><strong>Estado</strong></td><td style="padding:8px;border:1px solid #e2e8f0">${escapeXml(lead?.status || "nuevo")}</td></tr>
        <tr><td style="padding:8px;border:1px solid #e2e8f0"><strong>Proxima accion</strong></td><td style="padding:8px;border:1px solid #e2e8f0">${escapeXml(nextActionCopy)}</td></tr>
        <tr><td style="padding:8px;border:1px solid #e2e8f0"><strong>Cuando</strong></td><td style="padding:8px;border:1px solid #e2e8f0">${escapeXml(nextActionAtCopy)}</td></tr>
      </table>
      <p style="margin:18px 0 0"><strong>Resumen:</strong> ${escapeXml(lead?.summary || "Sin resumen")}</p>
      <p style="margin:10px 0 0"><strong>Notas:</strong> ${escapeXml(lead?.notes || "Sin notas")}</p>
      <p style="margin:18px 0 0;color:#475569">Este lead tambien quedo guardado en tu carpeta privada dentro del Coach.</p>
    </div>
  `;

  return {
    subject,
    text: lines.join("\n"),
    html,
    to: destination?.email || ""
  };
}

function construirCoachRecruitmentApplicationDeliveryPayload(userDoc = null, application = null, destination = null) {
  return {
    app: "Agustin 2.0 Coach",
    kind: "reclutamiento",
    sentAt: new Date().toISOString(),
    owner: {
      id: userDoc?._id ? String(userDoc._id) : "",
      name: userDoc?.name || "",
      email: userDoc?.email || ""
    },
    destination: {
      type: destination?.type || "carpeta_privada",
      label: destination?.label || ""
    },
    application: {
      id: application?.id || "",
      fullName: application?.fullName || "",
      phone: application?.phone || "",
      email: application?.email || "",
      drives: application?.drives || "",
      hasCar: application?.hasCar || "",
      customerServiceExperience: application?.customerServiceExperience || "",
      workPreference: application?.workPreference || "",
      salesExperience: application?.salesExperience || "",
      about: application?.about || "",
      summary: application?.summary || "",
      createdAt: application?.createdAt || null,
      updatedAt: application?.updatedAt || null
    }
  };
}

function construirCorreoCoachRecruitmentApplication(userDoc = null, application = null, destination = null) {
  const ownerName = userDoc?.name || "Distribuidor";
  const applicantName = application?.fullName || "Candidato nuevo";
  const subject = `Nueva aplicacion para ${ownerName}: ${applicantName}`;
  const lines = [
    `Hola ${ownerName},`,
    "",
    "Te acaba de entrar una aplicacion rapida de reclutamiento en Agustin 2.0 Coach.",
    "",
    `Nombre: ${application?.fullName || "Sin nombre"}`,
    `Telefono: ${application?.phone || "Sin telefono"}`,
    `Correo: ${application?.email || "Sin correo"}`,
    `Maneja: ${application?.drives || "Sin dato"}`,
    `Auto propio: ${application?.hasCar || "Sin dato"}`,
    `Atencion al cliente: ${application?.customerServiceExperience || "Sin dato"}`,
    `Busca: ${application?.workPreference || "Sin dato"}`,
    `Experiencia en ventas: ${application?.salesExperience || "Sin dato"}`,
    "",
    `Resumen: ${application?.summary || "Sin resumen"}`,
    "",
    `Sobre la persona: ${application?.about || "Sin texto"}`,
    "",
    "La aplicacion tambien quedo guardada en tu carpeta privada dentro del Coach."
  ];

  const html = `
    <div style="font-family:Arial,sans-serif;padding:24px;color:#0f172a">
      <h2 style="margin:0 0 12px">Nueva aplicacion de reclutamiento</h2>
      <p style="margin:0 0 18px;color:#475569">Te acaba de entrar una aplicacion rapida dentro de Agustin 2.0 Coach.</p>
      <table style="width:100%;border-collapse:collapse">
        <tr><td style="padding:8px;border:1px solid #e2e8f0"><strong>Nombre</strong></td><td style="padding:8px;border:1px solid #e2e8f0">${escapeXml(application?.fullName || "Sin nombre")}</td></tr>
        <tr><td style="padding:8px;border:1px solid #e2e8f0"><strong>Telefono</strong></td><td style="padding:8px;border:1px solid #e2e8f0">${escapeXml(application?.phone || "Sin telefono")}</td></tr>
        <tr><td style="padding:8px;border:1px solid #e2e8f0"><strong>Correo</strong></td><td style="padding:8px;border:1px solid #e2e8f0">${escapeXml(application?.email || "Sin correo")}</td></tr>
        <tr><td style="padding:8px;border:1px solid #e2e8f0"><strong>Maneja</strong></td><td style="padding:8px;border:1px solid #e2e8f0">${escapeXml(application?.drives || "Sin dato")}</td></tr>
        <tr><td style="padding:8px;border:1px solid #e2e8f0"><strong>Auto propio</strong></td><td style="padding:8px;border:1px solid #e2e8f0">${escapeXml(application?.hasCar || "Sin dato")}</td></tr>
        <tr><td style="padding:8px;border:1px solid #e2e8f0"><strong>Atencion al cliente</strong></td><td style="padding:8px;border:1px solid #e2e8f0">${escapeXml(application?.customerServiceExperience || "Sin dato")}</td></tr>
        <tr><td style="padding:8px;border:1px solid #e2e8f0"><strong>Busca</strong></td><td style="padding:8px;border:1px solid #e2e8f0">${escapeXml(application?.workPreference || "Sin dato")}</td></tr>
        <tr><td style="padding:8px;border:1px solid #e2e8f0"><strong>Experiencia en ventas</strong></td><td style="padding:8px;border:1px solid #e2e8f0">${escapeXml(application?.salesExperience || "Sin dato")}</td></tr>
      </table>
      <p style="margin:18px 0 0"><strong>Resumen:</strong> ${escapeXml(application?.summary || "Sin resumen")}</p>
      <p style="margin:10px 0 0"><strong>Sobre la persona:</strong> ${escapeXml(application?.about || "Sin texto")}</p>
      <p style="margin:18px 0 0;color:#475569">La aplicacion tambien quedo guardada en tu carpeta privada dentro del Coach.</p>
    </div>
  `;

  return {
    subject,
    text: lines.join("\n"),
    html,
    to: destination?.email || ""
  };
}

function construirCoachProgramSheetDeliveryPayload(userDoc = null, sheetDoc = null, destination = null, createdLeads = []) {
  return {
    app: "Agustin 2.0 Coach",
    kind: "programa_4_en_14",
    sentAt: new Date().toISOString(),
    owner: {
      id: userDoc?._id ? String(userDoc._id) : "",
      name: userDoc?.name || "",
      email: userDoc?.email || ""
    },
    destination: {
      type: destination?.type || "carpeta_privada",
      label: destination?.label || ""
    },
    lead: {
      fullName: sheetDoc?.hostName || "",
      phone: sheetDoc?.hostPhone || "",
      interest: sheetDoc?.giftSelected || "Programa 4 en 14",
      source: "programa_4_en_14",
      notes: sheetDoc?.summary || ""
    },
    sheet: {
      id: sheetDoc?._id ? String(sheetDoc._id) : "",
      programType: sheetDoc?.programType || "4_en_14",
      hostName: sheetDoc?.hostName || "",
      hostPhone: sheetDoc?.hostPhone || "",
      giftSelected: sheetDoc?.giftSelected || "",
      representativeName: sheetDoc?.representativeName || "",
      representativePhone: sheetDoc?.representativePhone || "",
      startWindow: sheetDoc?.startWindow || "",
      notes: sheetDoc?.notes || "",
      summary: sheetDoc?.summary || "",
      referralCount: Number(sheetDoc?.referralCount || 0),
      referrals: Array.isArray(sheetDoc?.referrals) ? sheetDoc.referrals : [],
      createdAt: sheetDoc?.createdAt || null,
      updatedAt: sheetDoc?.updatedAt || null
    },
    createdLeads: Array.isArray(createdLeads) ? createdLeads : []
  };
}

function construirCorreoPrograma414Coach(userDoc = null, sheetDoc = null, destination = null, createdLeads = []) {
  const ownerName = userDoc?.name || "Distribuidor";
  const hostName = sheetDoc?.hostName || "Programa 4 en 14";
  const rows = Array.isArray(sheetDoc?.referrals) ? sheetDoc.referrals : [];
  const textLines = [
    `Hola ${ownerName},`,
    "",
    "Acabas de guardar una hoja nativa de 4 en 14 en Agustin 2.0 Coach.",
    "",
    `Anfitrion: ${sheetDoc?.hostName || "Sin nombre"}`,
    `Telefono anfitrion: ${sheetDoc?.hostPhone || "Sin telefono"}`,
    `Regalo elegido: ${sheetDoc?.giftSelected || "Sin regalo"}`,
    `Representante: ${sheetDoc?.representativeName || "Sin representante"}`,
    `Telefono representante: ${sheetDoc?.representativePhone || "Sin telefono"}`,
    `Inicio y vencimiento: ${sheetDoc?.startWindow || "Sin fecha"}`,
    `Referidos guardados: ${rows.length}`,
    "",
    `Resumen: ${sheetDoc?.summary || "Sin resumen"}`,
    "",
    "Referidos:"
  ];

  rows.forEach((referral, index) => {
    textLines.push(
      `${index + 1}. ${referral?.fullName || "Sin nombre"} · ${referral?.phone || "Sin telefono"}${referral?.notes ? ` · ${referral.notes}` : ""}`
    );
  });

  textLines.push("", `Leads creados o actualizados en tu carpeta: ${createdLeads.length}`);

  const referralRows = rows
    .map(
      (referral, index) => `
        <tr>
          <td style="padding:8px;border:1px solid #e2e8f0">${index + 1}</td>
          <td style="padding:8px;border:1px solid #e2e8f0">${escapeXml(referral?.fullName || "Sin nombre")}</td>
          <td style="padding:8px;border:1px solid #e2e8f0">${escapeXml(referral?.phone || "Sin telefono")}</td>
          <td style="padding:8px;border:1px solid #e2e8f0">${escapeXml(referral?.notes || "")}</td>
        </tr>
      `
    )
    .join("");

  const html = `
    <div style="font-family:Arial,sans-serif;padding:24px;color:#0f172a">
      <h2 style="margin:0 0 12px">Hoja 4 en 14 guardada</h2>
      <p style="margin:0 0 18px;color:#475569">Tu hoja nativa del programa ya quedo dentro del Coach y sus referidos ya se movieron a tu carpeta.</p>
      <table style="width:100%;border-collapse:collapse">
        <tr><td style="padding:8px;border:1px solid #e2e8f0"><strong>Anfitrion</strong></td><td style="padding:8px;border:1px solid #e2e8f0">${escapeXml(sheetDoc?.hostName || "Sin nombre")}</td></tr>
        <tr><td style="padding:8px;border:1px solid #e2e8f0"><strong>Telefono anfitrion</strong></td><td style="padding:8px;border:1px solid #e2e8f0">${escapeXml(sheetDoc?.hostPhone || "Sin telefono")}</td></tr>
        <tr><td style="padding:8px;border:1px solid #e2e8f0"><strong>Regalo elegido</strong></td><td style="padding:8px;border:1px solid #e2e8f0">${escapeXml(sheetDoc?.giftSelected || "Sin regalo")}</td></tr>
        <tr><td style="padding:8px;border:1px solid #e2e8f0"><strong>Representante</strong></td><td style="padding:8px;border:1px solid #e2e8f0">${escapeXml(sheetDoc?.representativeName || "Sin representante")}</td></tr>
        <tr><td style="padding:8px;border:1px solid #e2e8f0"><strong>Telefono representante</strong></td><td style="padding:8px;border:1px solid #e2e8f0">${escapeXml(sheetDoc?.representativePhone || "Sin telefono")}</td></tr>
        <tr><td style="padding:8px;border:1px solid #e2e8f0"><strong>Inicio y vencimiento</strong></td><td style="padding:8px;border:1px solid #e2e8f0">${escapeXml(sheetDoc?.startWindow || "Sin fecha")}</td></tr>
        <tr><td style="padding:8px;border:1px solid #e2e8f0"><strong>Referidos guardados</strong></td><td style="padding:8px;border:1px solid #e2e8f0">${escapeXml(String(rows.length))}</td></tr>
      </table>
      <p style="margin:18px 0 10px"><strong>Resumen:</strong> ${escapeXml(sheetDoc?.summary || "Sin resumen")}</p>
      <table style="width:100%;border-collapse:collapse;margin-top:12px">
        <thead>
          <tr>
            <th style="padding:8px;border:1px solid #e2e8f0;text-align:left;background:#f8fafc">#</th>
            <th style="padding:8px;border:1px solid #e2e8f0;text-align:left;background:#f8fafc">Nombre</th>
            <th style="padding:8px;border:1px solid #e2e8f0;text-align:left;background:#f8fafc">Telefono</th>
            <th style="padding:8px;border:1px solid #e2e8f0;text-align:left;background:#f8fafc">Notas</th>
          </tr>
        </thead>
        <tbody>${referralRows}</tbody>
      </table>
      <p style="margin:18px 0 0;color:#475569">Leads creados o actualizados en tu carpeta: ${escapeXml(String(createdLeads.length))}</p>
    </div>
  `;

  return {
    subject: `Hoja 4 en 14 guardada para ${ownerName}: ${hostName}`,
    text: textLines.join("\n"),
    html,
    to: destination?.email || ""
  };
}

async function enviarCorreoLeadCoach(userDoc = null, lead = null, destination = null) {
  if (!RESEND_API_KEY || !RESEND_FROM_EMAIL) {
    return {
      attempted: true,
      delivered: false,
      destination,
      error: "El correo del sistema todavia no esta configurado."
    };
  }

  if (!destination?.email) {
    return {
      attempted: false,
      delivered: false,
      destination,
      error: "No hay correo destino configurado."
    };
  }

  const emailPayload = construirCorreoLeadCoach(userDoc, lead, destination);
  const response = await fetch("https://api.resend.com/emails", {
    method: "POST",
    headers: {
      Authorization: `Bearer ${RESEND_API_KEY}`,
      "Content-Type": "application/json"
    },
    body: JSON.stringify({
      from: RESEND_FROM_EMAIL,
      to: [emailPayload.to],
      subject: emailPayload.subject,
      text: emailPayload.text,
      html: emailPayload.html
    })
  });

  if (!response.ok) {
    const errorData = await response.json().catch(() => ({}));
    return {
      attempted: true,
      delivered: false,
      destination,
      status: response.status,
      error: errorData?.message || `Correo respondio ${response.status}`
    };
  }

  return {
    attempted: true,
    delivered: true,
    destination,
    status: response.status
  };
}

async function enviarCoachLeadADestino(userDoc = null, profileDoc = null, lead = null) {
  const destination = limpiarCoachLeadDestination(profileDoc);

  if (!destination.enabled || !lead) {
    return {
      attempted: false,
      delivered: false,
      destination
    };
  }

  if (destination.type === "correo_personal") {
    return enviarCorreoLeadCoach(userDoc, lead, destination);
  }

  if (!destination.url) {
    return {
      attempted: false,
      delivered: false,
      destination
    };
  }

  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), 9000);

  try {
    const response = await fetch(destination.url, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(construirCoachLeadDeliveryPayload(userDoc, lead, destination)),
      signal: controller.signal
    });

    clearTimeout(timeout);

    return {
      attempted: true,
      delivered: response.ok,
      destination,
      status: response.status,
      error: response.ok ? "" : `Destino respondio ${response.status}`
    };
  } catch (error) {
    clearTimeout(timeout);
    return {
      attempted: true,
      delivered: false,
      destination,
      error: error?.name === "AbortError" ? "El destino tardo demasiado en responder." : error.message
    };
  }
}

async function enviarCoachRecruitmentApplicationADestino(userDoc = null, profileDoc = null, application = null) {
  const destination = limpiarCoachLeadDestination(profileDoc);

  if (!destination.enabled || !application) {
    return {
      attempted: false,
      delivered: false,
      destination
    };
  }

  if (destination.type === "correo_personal") {
    if (!RESEND_API_KEY || !RESEND_FROM_EMAIL) {
      return {
        attempted: true,
        delivered: false,
        destination,
        error: "El correo del sistema todavia no esta configurado."
      };
    }

    if (!destination.email) {
      return {
        attempted: false,
        delivered: false,
        destination,
        error: "No hay correo destino configurado."
      };
    }

    const emailPayload = construirCorreoCoachRecruitmentApplication(userDoc, application, destination);
    const response = await fetch("https://api.resend.com/emails", {
      method: "POST",
      headers: {
        Authorization: `Bearer ${RESEND_API_KEY}`,
        "Content-Type": "application/json"
      },
      body: JSON.stringify({
        from: RESEND_FROM_EMAIL,
        to: [emailPayload.to],
        subject: emailPayload.subject,
        text: emailPayload.text,
        html: emailPayload.html
      })
    });

    if (!response.ok) {
      const errorData = await response.json().catch(() => ({}));
      return {
        attempted: true,
        delivered: false,
        destination,
        status: response.status,
        error: errorData?.message || `Correo respondio ${response.status}`
      };
    }

    return {
      attempted: true,
      delivered: true,
      destination,
      status: response.status
    };
  }

  if (!destination.url) {
    return {
      attempted: false,
      delivered: false,
      destination
    };
  }

  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), 9000);

  try {
    const response = await fetch(destination.url, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(construirCoachRecruitmentApplicationDeliveryPayload(userDoc, application, destination)),
      signal: controller.signal
    });

    clearTimeout(timeout);

    return {
      attempted: true,
      delivered: response.ok,
      destination,
      status: response.status,
      error: response.ok ? "" : `Destino respondio ${response.status}`
    };
  } catch (error) {
    clearTimeout(timeout);
    return {
      attempted: true,
      delivered: false,
      destination,
      error: error?.name === "AbortError" ? "El destino tardo demasiado en responder." : error.message
    };
  }
}

async function enviarCoachProgramSheetADestino(userDoc = null, profileDoc = null, sheetDoc = null, createdLeads = []) {
  const destination = limpiarCoachLeadDestination(profileDoc);

  if (!destination.enabled || !sheetDoc) {
    return {
      attempted: false,
      delivered: false,
      destination
    };
  }

  if (destination.type === "correo_personal") {
    if (!RESEND_API_KEY || !RESEND_FROM_EMAIL) {
      return {
        attempted: true,
        delivered: false,
        destination,
        error: "El correo del sistema todavia no esta configurado."
      };
    }

    if (!destination.email) {
      return {
        attempted: false,
        delivered: false,
        destination,
        error: "No hay correo destino configurado."
      };
    }

    const emailPayload = construirCorreoPrograma414Coach(userDoc, sheetDoc, destination, createdLeads);
    const response = await fetch("https://api.resend.com/emails", {
      method: "POST",
      headers: {
        Authorization: `Bearer ${RESEND_API_KEY}`,
        "Content-Type": "application/json"
      },
      body: JSON.stringify({
        from: RESEND_FROM_EMAIL,
        to: [emailPayload.to],
        subject: emailPayload.subject,
        text: emailPayload.text,
        html: emailPayload.html
      })
    });

    if (!response.ok) {
      const errorData = await response.json().catch(() => ({}));
      return {
        attempted: true,
        delivered: false,
        destination,
        status: response.status,
        error: errorData?.message || `Correo respondio ${response.status}`
      };
    }

    return {
      attempted: true,
      delivered: true,
      destination,
      status: response.status
    };
  }

  if (!destination.url) {
    return {
      attempted: false,
      delivered: false,
      destination
    };
  }

  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), 9000);

  try {
    const response = await fetch(destination.url, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(construirCoachProgramSheetDeliveryPayload(userDoc, sheetDoc, destination, createdLeads)),
      signal: controller.signal
    });

    clearTimeout(timeout);

    return {
      attempted: true,
      delivered: response.ok,
      destination,
      status: response.status,
      error: response.ok ? "" : `Destino respondio ${response.status}`
    };
  } catch (error) {
    clearTimeout(timeout);
    return {
      attempted: true,
      delivered: false,
      destination,
      error: error?.name === "AbortError" ? "El destino tardo demasiado en responder." : error.message
    };
  }
}

function construirCoachAsyncJobDedupeKey(jobType = "", userDoc = null, payload = null) {
  const ownerUserId = String(resolverCoachOwnerUserId(userDoc) || userDoc?._id || "").trim();

  if (!jobType || !ownerUserId) {
    return "";
  }

  let sourceId = "";

  if (jobType === "lead_destination") {
    sourceId = String(payload?.lead?.id || payload?.lead?._id || "").trim();
  } else if (jobType === "recruitment_destination") {
    sourceId = String(payload?.application?.id || payload?.application?._id || "").trim();
  } else if (jobType === "program_destination") {
    sourceId = String(payload?.sheetDoc?.id || payload?.sheetDoc?._id || payload?.sheet?.id || payload?.sheet?._id || "").trim();
  } else if (jobType === "highlevel_sync") {
    sourceId = String(payload?.leadId || payload?.lead?.id || payload?.lead?._id || "").trim();
  }

  if (!sourceId) {
    return "";
  }

  return `${jobType}:${ownerUserId}:${sourceId}`;
}

function construirCoachAsyncJobRetryAt(attempts = 0) {
  const safeAttempts = Math.max(Number(attempts) || 0, 0);
  const delayMs = Math.min(5 * 60 * 1000, 15000 * Math.pow(2, Math.max(safeAttempts - 1, 0)));
  return new Date(Date.now() + delayMs);
}

async function encolarCoachAsyncJob({ jobType = "", userDoc = null, payload = null, maxAttempts = 4 } = {}) {
  const ownerUserId = normalizarCoachCrmObjectId(resolverCoachOwnerUserId(userDoc) || userDoc?._id);

  if (!jobType || !ownerUserId) {
    return null;
  }

  const dedupeKey = construirCoachAsyncJobDedupeKey(jobType, userDoc, payload);
  const now = new Date();

  if (dedupeKey) {
    const queuedJob = await CoachAsyncJob.findOneAndUpdate(
      {
        jobType,
        dedupeKey,
        status: { $in: ["pending", "retrying"] }
      },
      {
        $set: {
          ownerUserId,
          payload,
          nextRunAt: now,
          updatedAt: now,
          lastError: ""
        }
      },
      {
        sort: { updatedAt: -1 },
        new: true
      }
    );

    if (queuedJob?._id) {
      despertarCoachAsyncJobWorker();
      return queuedJob;
    }
  }

  const jobDoc = await CoachAsyncJob.create({
    jobType,
    dedupeKey,
    ownerUserId,
    payload,
    status: "pending",
    attempts: 0,
    maxAttempts: Math.max(Number(maxAttempts) || 0, 1),
    nextRunAt: now,
    createdAt: now,
    updatedAt: now
  });

  despertarCoachAsyncJobWorker();
  return jobDoc;
}

async function cargarCoachAsyncJobContext(jobDoc = null) {
  const ownerUserId = normalizarCoachCrmObjectId(jobDoc?.ownerUserId);

  if (!ownerUserId) {
    return {
      userDoc: null,
      profileDoc: null
    };
  }

  let userDoc = await CoachUser.findById(ownerUserId);

  if (!userDoc?._id) {
    return {
      userDoc: null,
      profileDoc: null
    };
  }

  userDoc = await asegurarCoachUserBase(userDoc);
  const profileDoc = await obtenerCoachOwnerProfile(userDoc, { lean: true });

  return {
    userDoc,
    profileDoc
  };
}

async function ejecutarCoachAsyncJob(jobDoc = null) {
  if (!jobDoc?.jobType) {
    return {
      attempted: false,
      error: "Trabajo asincrono invalido."
    };
  }

  const { userDoc, profileDoc } = await cargarCoachAsyncJobContext(jobDoc);

  if (!userDoc?._id) {
    return {
      attempted: false,
      error: "No encontre la cuenta duena de este trabajo."
    };
  }

  if (jobDoc.jobType === "lead_destination") {
    return enviarCoachLeadADestino(userDoc, profileDoc, jobDoc.payload?.lead || null);
  }

  if (jobDoc.jobType === "recruitment_destination") {
    return enviarCoachRecruitmentApplicationADestino(userDoc, profileDoc, jobDoc.payload?.application || null);
  }

  if (jobDoc.jobType === "program_destination") {
    return enviarCoachProgramSheetADestino(
      userDoc,
      profileDoc,
      jobDoc.payload?.sheetDoc || jobDoc.payload?.sheet || null,
      Array.isArray(jobDoc.payload?.createdLeads) ? jobDoc.payload.createdLeads : []
    );
  }

  if (jobDoc.jobType === "highlevel_sync") {
    return sincronizarCoachLeadAHighLevel(userDoc, profileDoc, {
      _id: jobDoc.payload?.leadId || jobDoc.payload?.lead?.id || jobDoc.payload?.lead?._id || ""
    });
  }

  return {
    attempted: false,
    error: `No reconozco el tipo de job ${jobDoc.jobType}.`
  };
}

function coachAsyncJobFueExitoso(jobDoc = null, result = null) {
  if (!jobDoc?.jobType) {
    return false;
  }

  if (jobDoc.jobType === "highlevel_sync") {
    return (
      result?.synced === true ||
      (result?.attempted === false && ["not_applicable", "invalid_lead", "not_found"].includes(result?.reason || ""))
    );
  }

  return result?.delivered === true || (result?.attempted === false && !result?.error);
}

function construirCoachAsyncJobError(result = null, error = null) {
  return (
    cleanText(error?.message || "") ||
    cleanText(result?.error || "") ||
    cleanText(result?.reason || "") ||
    "El trabajo asincrono fallo sin detalle."
  );
}

async function marcarCoachAsyncJobCompletado(jobDoc = null, result = null) {
  if (!jobDoc?._id) {
    return;
  }

  await CoachAsyncJob.updateOne(
    { _id: jobDoc._id },
    {
      $set: {
        status: "completed",
        lockedAt: null,
        completedAt: new Date(),
        updatedAt: new Date(),
        lastError: "",
        lastResult: result || null
      }
    }
  );
}

async function marcarCoachAsyncJobFallido(jobDoc = null, result = null, error = null) {
  if (!jobDoc?._id) {
    return;
  }

  const now = new Date();
  const lastError = construirCoachAsyncJobError(result, error);
  const reachedMaxAttempts = Number(jobDoc.attempts || 0) >= Math.max(Number(jobDoc.maxAttempts || 0), 1);

  await CoachAsyncJob.updateOne(
    { _id: jobDoc._id },
    {
      $set: {
        status: reachedMaxAttempts ? "failed" : "retrying",
        nextRunAt: reachedMaxAttempts ? now : construirCoachAsyncJobRetryAt(jobDoc.attempts || 0),
        lockedAt: null,
        failedAt: reachedMaxAttempts ? now : null,
        updatedAt: now,
        lastError,
        lastResult: result || null
      }
    }
  );
}

async function tomarCoachAsyncJobPendiente() {
  const now = new Date();
  const staleLockAt = new Date(now.getTime() - COACH_ASYNC_JOB_LOCK_MS);

  return CoachAsyncJob.findOneAndUpdate(
    {
      $or: [
        {
          status: "pending",
          nextRunAt: { $lte: now }
        },
        {
          status: "retrying",
          nextRunAt: { $lte: now }
        },
        {
          status: "processing",
          lockedAt: { $lte: staleLockAt }
        }
      ]
    },
    {
      $set: {
        status: "processing",
        lockedAt: now,
        lastAttemptAt: now,
        updatedAt: now
      },
      $inc: {
        attempts: 1
      }
    },
    {
      sort: {
        nextRunAt: 1,
        createdAt: 1
      },
      new: true
    }
  );
}

async function procesarCoachAsyncJobs() {
  if (coachAsyncJobWorkerRunning || mongoose.connection.readyState !== 1) {
    return;
  }

  coachAsyncJobWorkerRunning = true;

  try {
    for (let index = 0; index < COACH_ASYNC_JOB_BATCH_SIZE; index += 1) {
      const jobDoc = await tomarCoachAsyncJobPendiente();

      if (!jobDoc?._id) {
        break;
      }

      try {
        const result = await ejecutarCoachAsyncJob(jobDoc);

        if (coachAsyncJobFueExitoso(jobDoc, result)) {
          await marcarCoachAsyncJobCompletado(jobDoc, result);
        } else {
          await marcarCoachAsyncJobFallido(jobDoc, result);
        }
      } catch (error) {
        console.error("Error procesando job asincrono del Coach:", jobDoc.jobType, error.message);
        await marcarCoachAsyncJobFallido(jobDoc, null, error);
      }
    }
  } finally {
    coachAsyncJobWorkerRunning = false;
  }
}

function despertarCoachAsyncJobWorker(delayMs = 200) {
  if (coachAsyncJobWakeTimer) {
    return;
  }

  coachAsyncJobWakeTimer = setTimeout(() => {
    coachAsyncJobWakeTimer = null;
    procesarCoachAsyncJobs().catch(error => {
      console.error("Error despertando cola asincrona del Coach:", error.message);
    });
  }, delayMs);
}

function iniciarCoachAsyncJobWorker() {
  if (coachAsyncJobWorkerInterval) {
    return;
  }

  coachAsyncJobWorkerInterval = setInterval(() => {
    procesarCoachAsyncJobs().catch(error => {
      console.error("Error en worker asincrono del Coach:", error.message);
    });
  }, COACH_ASYNC_JOB_POLL_MS);

  despertarCoachAsyncJobWorker(500);
}

async function programarEnvioCoachLeadADestino(userDoc = null, profileDoc = null, lead = null) {
  const destination = limpiarCoachLeadDestination(profileDoc);

  if (!destination.enabled || !lead) {
    return {
      attempted: false,
      queued: false,
      destination
    };
  }

  try {
    const jobDoc = await encolarCoachAsyncJob({
      jobType: "lead_destination",
      userDoc,
      payload: {
        lead
      },
      maxAttempts: 4
    });

    return {
      attempted: true,
      queued: Boolean(jobDoc?._id),
      queueId: jobDoc?._id ? String(jobDoc._id) : "",
      destination
    };
  } catch (error) {
    console.error("Error encolando lead del Coach al destino:", error.message);
    return {
      attempted: true,
      queued: false,
      destination,
      error: error.message
    };
  }
}

async function programarEnvioCoachRecruitmentApplicationADestino(userDoc = null, profileDoc = null, application = null) {
  const destination = limpiarCoachLeadDestination(profileDoc);

  if (!destination.enabled || !application) {
    return {
      attempted: false,
      queued: false,
      destination
    };
  }

  try {
    const jobDoc = await encolarCoachAsyncJob({
      jobType: "recruitment_destination",
      userDoc,
      payload: {
        application
      },
      maxAttempts: 4
    });

    return {
      attempted: true,
      queued: Boolean(jobDoc?._id),
      queueId: jobDoc?._id ? String(jobDoc._id) : "",
      destination
    };
  } catch (error) {
    console.error("Error encolando aplicacion de reclutamiento al destino:", error.message);
    return {
      attempted: true,
      queued: false,
      destination,
      error: error.message
    };
  }
}

async function programarEnvioCoachProgramSheetADestino(userDoc = null, profileDoc = null, sheetDoc = null, createdLeads = []) {
  const destination = limpiarCoachLeadDestination(profileDoc);

  if (!destination.enabled || !sheetDoc) {
    return {
      attempted: false,
      queued: false,
      destination
    };
  }

  try {
    const jobDoc = await encolarCoachAsyncJob({
      jobType: "program_destination",
      userDoc,
      payload: {
        sheetDoc,
        createdLeads
      },
      maxAttempts: 4
    });

    return {
      attempted: true,
      queued: Boolean(jobDoc?._id),
      queueId: jobDoc?._id ? String(jobDoc._id) : "",
      destination
    };
  } catch (error) {
    console.error("Error encolando hoja 4 en 14 al destino:", error.message);
    return {
      attempted: true,
      queued: false,
      destination,
      error: error.message
    };
  }
}

function normalizarCoachLeadStatus(status = "") {
  const value = String(status || "").trim().toLowerCase();
  const validStatuses = ["nuevo", "contactado", "agendado", "cliente", "archivado"];
  return validStatuses.includes(value) ? value : "nuevo";
}

function normalizarCoachLeadSource(source = "") {
  const value = String(source || "").trim().toLowerCase();
  const validSources = [
    "rifa_digital",
    "programa_4_en_14",
    "contactos_compartidos",
    "chef_personal",
    "llamada",
    "demo",
    "referencia",
    "evento",
    "captura_manual",
    "otro"
  ];
  return validSources.includes(value) ? value : "captura_manual";
}

function normalizarCoachLeadNextAction(action = "") {
  const value = String(action || "").trim().toLowerCase();
  const validActions = ["", "llamar", "whatsapp", "cita", "demo", "seguimiento", "correo"];
  return validActions.includes(value) ? value : "";
}

function normalizarZipCode(value = "") {
  return String(value || "").replace(/\D/g, "").slice(0, 10);
}

function inferCoachCityZipFromAddress(value = "") {
  const safeValue = cleanText(value || "")
    .replace(/\s+/g, " ")
    .trim()
    .slice(0, 240);

  if (!safeValue) {
    return { city: "", zipCode: "" };
  }

  const zipMatch = safeValue.match(/\b(\d{5}(?:-\d{4})?)\b/);
  const zipCode = normalizarZipCode(zipMatch?.[1] || "");
  const stateZipMatch = safeValue.match(/(.+?)(?:,?\s+)([A-Za-z]{2})\s+(\d{5}(?:-\d{4})?)$/);
  let city = "";

  if (stateZipMatch) {
    const beforeState = stateZipMatch[1].trim().replace(/,+$/g, "");
    const commaParts = beforeState
      .split(",")
      .map(part => cleanText(part).trim())
      .filter(Boolean);

    if (commaParts.length >= 2) {
      city = commaParts[commaParts.length - 1];
    } else {
      const tokens = beforeState.split(/\s+/).filter(Boolean);
      const streetSuffixes = new Set([
        "st",
        "street",
        "ave",
        "avenue",
        "rd",
        "road",
        "dr",
        "drive",
        "ct",
        "court",
        "pl",
        "place",
        "blvd",
        "boulevard",
        "ln",
        "lane",
        "way",
        "pkwy",
        "parkway",
        "cir",
        "circle",
        "trl",
        "trail",
        "hwy",
        "highway",
        "ter",
        "terrace"
      ]);
      let streetEndIndex = -1;

      tokens.forEach((token, index) => {
        const normalizedToken = String(token || "")
          .toLowerCase()
          .replace(/[^a-z0-9]/g, "");

        if (streetSuffixes.has(normalizedToken)) {
          streetEndIndex = index;
        }
      });

      if (streetEndIndex >= 0 && streetEndIndex < tokens.length - 1) {
        city = tokens.slice(streetEndIndex + 1).join(" ");
      } else if (tokens.length >= 2) {
        city = tokens.slice(-2).join(" ");
      } else {
        city = beforeState;
      }
    }
  }

  city = cleanText(city)
    .replace(/\s+/g, " ")
    .trim()
    .slice(0, 80);

  if (city) {
    city = city.toLowerCase().replace(/\b\w/g, char => char.toUpperCase());
  }

  return { city, zipCode };
}

function parseCoachLeadNextActionAt(value) {
  if (!value) {
    return null;
  }

  const parsed = new Date(value);
  return Number.isNaN(parsed.getTime()) ? null : parsed;
}

function normalizarCoachCrmSourceType(value = "") {
  const safeValue = String(value || "")
    .trim()
    .toLowerCase();
  return ["lead", "programa_4_en_14", "reclutamiento"].includes(safeValue) ? safeValue : "lead";
}

function normalizarCoachCrmStatus(value = "") {
  const safeValue = String(value || "")
    .trim()
    .toLowerCase();
  const validStatuses = [
    "nuevo",
    "intentando",
    "no_atendio",
    "seguimiento",
    "cita_agendada",
    "reagendada",
    "ya_afuera",
    "entro_a_casa",
    "demo_hecha",
    "venta",
    "no_venta"
  ];
  return validStatuses.includes(safeValue) ? safeValue : "nuevo";
}

function resolverCoachCrmStatusColor(status = "") {
  switch (normalizarCoachCrmStatus(status)) {
    case "seguimiento":
      return "amber";
    case "cita_agendada":
    case "reagendada":
      return "orange";
    case "ya_afuera":
    case "entro_a_casa":
    case "demo_hecha":
      return "violet";
    case "venta":
      return "green";
    case "no_venta":
      return "rose";
    case "no_atendio":
      return "slate";
    case "intentando":
      return "sky";
    default:
      return "blue";
  }
}

function formatearCoachCrmStatusLabel(status = "") {
  switch (normalizarCoachCrmStatus(status)) {
    case "intentando":
      return "Intentando";
    case "no_atendio":
      return "No atendio";
    case "seguimiento":
      return "Seguimiento";
    case "cita_agendada":
      return "Cita agendada";
    case "reagendada":
      return "Reagendada";
    case "ya_afuera":
      return "Ya afuera";
    case "entro_a_casa":
      return "Entro a casa";
    case "demo_hecha":
      return "Demo hecha";
    case "venta":
      return "Venta";
    case "no_venta":
      return "No venta";
    default:
      return "Nuevo";
  }
}

function resolverCoachCrmStatusDesdeLead(leadDoc = null) {
  const leadStatus = normalizarCoachLeadStatus(leadDoc?.status || "nuevo");

  switch (leadStatus) {
    case "contactado":
      return "seguimiento";
    case "agendado":
      return "cita_agendada";
    case "cliente":
      return "venta";
    case "archivado":
      return "no_venta";
    default:
      return "nuevo";
  }
}

function resolverCoachCrmStatusDesdePrograma414(referral = null, linkedLead = null) {
  if (linkedLead?._id) {
    return resolverCoachCrmStatusDesdeLead(linkedLead);
  }

  const instantStatus = normalizarCoachProgramInstantStatus(referral?.instantCallStatus || "");

  switch (instantStatus) {
    case "cita_lograda":
      return "cita_agendada";
    case "llamar_despues":
    case "seleccionado":
      return "seguimiento";
    case "no_contesto":
      return "no_atendio";
    case "no_quiso":
      return "no_venta";
    default:
      return "nuevo";
  }
}

function resolverCoachCrmStatusDesdeRecruitment(existingStatus = "") {
  return existingStatus ? normalizarCoachCrmStatus(existingStatus) : "nuevo";
}

function mapearCoachCrmStatusAPrograma414(status = "") {
  switch (normalizarCoachCrmStatus(status)) {
    case "cita_agendada":
    case "reagendada":
      return "cita_lograda";
    case "no_atendio":
      return "no_contesto";
    case "seguimiento":
    case "intentando":
      return "llamar_despues";
    case "no_venta":
      return "no_quiso";
    default:
      return "seleccionado";
  }
}

function normalizarCoachCrmObjectId(value = null) {
  if (!value || !mongoose.Types.ObjectId.isValid(value)) {
    return null;
  }

  return value instanceof mongoose.Types.ObjectId ? value : new mongoose.Types.ObjectId(value);
}

function construirCoachCrmRecordId(sourceType = "", sourceRecordId = "", ownerUserId = "") {
  return [
    "crm",
    normalizarCoachCrmSourceType(sourceType || "lead"),
    String(ownerUserId || "").trim(),
    String(sourceRecordId || "").trim()
  ]
    .filter(Boolean)
    .join("_");
}

function truncarCoachCrmText(value = "", maxLength = 240) {
  return cleanText(value || "").slice(0, maxLength);
}

function construirCoachCrmProgramSummary(sheetDoc = null, referral = null) {
  const parts = [];

  if (sheetDoc?.hostName) {
    parts.push(`Anfitrion: ${truncarTextoPrompt(sheetDoc.hostName, 80)}.`);
  }

  if (referral?.fullName) {
    parts.push(`Referido: ${truncarTextoPrompt(referral.fullName, 80)}.`);
  }

  if (sheetDoc?.giftSelected) {
    parts.push(`Regalo: ${truncarTextoPrompt(sheetDoc.giftSelected, 80)}.`);
  }

  if (referral?.appointmentDetails) {
    parts.push(`Cita: ${truncarTextoPrompt(referral.appointmentDetails, 120)}.`);
  }

  if (referral?.notes) {
    parts.push(`Notas: ${truncarTextoPrompt(referral.notes, 120)}.`);
  }

  if (referral?.instantCallNotes) {
    parts.push(`Seguimiento: ${truncarTextoPrompt(referral.instantCallNotes, 120)}.`);
  }

  return parts.join(" ").trim();
}

function construirCoachCrmProgramNotes(sheetDoc = null, referral = null) {
  return [referral?.notes || "", referral?.instantCallNotes || "", sheetDoc?.notes || ""]
    .map(item => cleanText(item || "").trim())
    .filter(Boolean)
    .join("\n\n")
    .slice(0, 1200);
}

function construirCoachCrmLeadSeedOperation(leadDoc = null, defaultTelemarketer = null) {
  if (!leadDoc?._id || normalizarCoachLeadSource(leadDoc.source || "") === "programa_4_en_14") {
    return null;
  }

  const sourceRecordId = String(leadDoc._id);
  const ownerUserId = normalizarCoachCrmObjectId(leadDoc.ownerUserId);

  if (!ownerUserId) {
    return null;
  }

  const status = resolverCoachCrmStatusDesdeLead(leadDoc);
  const nextActionAt = leadDoc.nextActionAt || null;
  const appointmentAt = leadDoc.status === "agendado" ? nextActionAt : null;
  const defaultTelemarketerUserId = normalizarCoachCrmObjectId(defaultTelemarketer?.userId || defaultTelemarketer?._id);
  const defaultTelemarketerName = defaultTelemarketer?.name || defaultTelemarketer?.email || "";

  return {
    updateOne: {
      filter: {
        ownerUserId,
        sourceType: "lead",
        sourceRecordId
      },
      update: {
        $set: {
          crmRecordId: construirCoachCrmRecordId("lead", sourceRecordId, ownerUserId),
          ownerUserId,
          ownerEmail: leadDoc.ownerEmail || "",
          ownerName: leadDoc.ownerName || "",
          generatedByUserId: normalizarCoachCrmObjectId(leadDoc.generatedByUserId),
          generatedByName: leadDoc.generatedByName || "",
          generatedByAccountType: leadDoc.generatedByAccountType || "",
          sourceType: "lead",
          sourceRecordId,
          sourceParentId: "",
          sourceSubIndex: -1,
          linkedLeadId: normalizarCoachCrmObjectId(leadDoc._id),
          linkedProgramSheetId: null,
          linkedApplicationId: null,
          leadName: leadDoc.fullName || "",
          phone: leadDoc.phone || "",
          email: leadDoc.email || "",
          city: leadDoc.city || "",
          zipCode: leadDoc.zipCode || "",
          interest: leadDoc.interest || "",
          status,
          statusColor: resolverCoachCrmStatusColor(status),
          nextAction: normalizarCoachLeadNextAction(leadDoc.nextAction || ""),
          nextActionAt,
          appointmentAt,
          lastContactAt: leadDoc.lastContactAt || null,
          briefHistory: truncarCoachCrmText(leadDoc.summary || construirCoachLeadSummary(leadDoc), 320),
          privateNotes: truncarCoachCrmText(leadDoc.notes || "", 1200),
          lastNote: truncarCoachCrmText(leadDoc.notes || "", 240),
          saleResult: status === "venta" ? "venta" : "",
          saleAmount: status === "venta" ? limpiarCoachDemoOutcomeAmount(leadDoc.highLevelOpportunityValue || 0) : 0,
          soldAt: status === "venta" ? leadDoc.updatedAt || leadDoc.createdAt || null : null,
          closedAt: ["venta", "no_venta"].includes(status) ? leadDoc.updatedAt || leadDoc.createdAt || null : null,
          sourceUpdatedAt: leadDoc.updatedAt || leadDoc.createdAt || null,
          updatedAt: new Date()
        },
        $setOnInsert: {
          createdAt: leadDoc.createdAt || new Date(),
          assignedTelemarketerUserId: defaultTelemarketerUserId,
          assignedTelemarketerName: defaultTelemarketerName
        }
      },
      upsert: true
    }
  };
}

function construirCoachCrmProgramSeedOperation(
  sheetDoc = null,
  referral = null,
  referralIndex = -1,
  linkedLead = null,
  defaultTelemarketer = null
) {
  if (!sheetDoc?._id || !referral || !referral.fullName || !referral.phone || !Number.isInteger(referralIndex) || referralIndex < 0) {
    return null;
  }

  const ownerUserId = normalizarCoachCrmObjectId(sheetDoc.ownerUserId);

  if (!ownerUserId) {
    return null;
  }

  const sourceRecordId = `${String(sheetDoc._id)}:${referralIndex}`;
  const status = resolverCoachCrmStatusDesdePrograma414(referral, linkedLead);
  const nextActionAt = linkedLead?.nextActionAt || null;
  const appointmentAt =
    status === "cita_agendada"
      ? nextActionAt || (referral?.appointmentDetails ? sheetDoc.updatedAt || sheetDoc.createdAt || null : null)
      : null;
  const defaultTelemarketerUserId = normalizarCoachCrmObjectId(defaultTelemarketer?.userId || defaultTelemarketer?._id);
  const defaultTelemarketerName = defaultTelemarketer?.name || defaultTelemarketer?.email || "";

  return {
    updateOne: {
      filter: {
        ownerUserId,
        sourceType: "programa_4_en_14",
        sourceRecordId
      },
      update: {
        $set: {
          crmRecordId: construirCoachCrmRecordId("programa_4_en_14", sourceRecordId, ownerUserId),
          ownerUserId,
          ownerEmail: sheetDoc.ownerEmail || "",
          ownerName: sheetDoc.ownerName || "",
          generatedByUserId: normalizarCoachCrmObjectId(sheetDoc.generatedByUserId),
          generatedByName: sheetDoc.generatedByName || "",
          generatedByAccountType: sheetDoc.generatedByAccountType || "",
          sourceType: "programa_4_en_14",
          sourceRecordId,
          sourceParentId: String(sheetDoc._id),
          sourceSubIndex: referralIndex,
          linkedLeadId: normalizarCoachCrmObjectId(linkedLead?._id || referral.createdLeadId),
          linkedProgramSheetId: normalizarCoachCrmObjectId(sheetDoc._id),
          linkedApplicationId: null,
          leadName: referral.fullName || "",
          phone: referral.phone || "",
          email: linkedLead?.email || "",
          city: linkedLead?.city || "",
          zipCode: linkedLead?.zipCode || "",
          interest:
            linkedLead?.interest ||
            (sheetDoc.giftSelected ? `Programa 4 en 14 · ${sheetDoc.giftSelected}` : "Programa 4 en 14"),
          status,
          statusColor: resolverCoachCrmStatusColor(status),
          nextAction: linkedLead?.nextAction || "",
          nextActionAt,
          appointmentAt,
          lastContactAt: linkedLead?.lastContactAt || referral.lastOutcomeAt || referral.selectedForInstantCallAt || null,
          briefHistory: truncarCoachCrmText(construirCoachCrmProgramSummary(sheetDoc, referral), 320),
          privateNotes: truncarCoachCrmText(construirCoachCrmProgramNotes(sheetDoc, referral), 1200),
          lastNote: truncarCoachCrmText(referral.instantCallNotes || referral.notes || "", 240),
          saleResult: "",
          saleAmount: 0,
          soldAt: null,
          closedAt: status === "no_venta" ? linkedLead?.updatedAt || sheetDoc.updatedAt || null : null,
          sourceUpdatedAt: sheetDoc.updatedAt || sheetDoc.createdAt || null,
          updatedAt: new Date()
        },
        $setOnInsert: {
          createdAt: sheetDoc.createdAt || new Date(),
          assignedTelemarketerUserId: defaultTelemarketerUserId,
          assignedTelemarketerName: defaultTelemarketerName
        }
      },
      upsert: true
    }
  };
}

function construirCoachCrmRecruitmentSeedOperation(applicationDoc = null, defaultTelemarketer = null) {
  if (!applicationDoc?._id) {
    return null;
  }

  const sourceRecordId = String(applicationDoc._id);
  const ownerUserId = normalizarCoachCrmObjectId(applicationDoc.ownerUserId);
  const defaultTelemarketerUserId = normalizarCoachCrmObjectId(defaultTelemarketer?.userId || defaultTelemarketer?._id);
  const defaultTelemarketerName = defaultTelemarketer?.name || defaultTelemarketer?.email || "";

  if (!ownerUserId) {
    return null;
  }

  return {
    updateOne: {
      filter: {
        ownerUserId,
        sourceType: "reclutamiento",
        sourceRecordId
      },
      update: {
        $set: {
          crmRecordId: construirCoachCrmRecordId("reclutamiento", sourceRecordId, ownerUserId),
          ownerUserId,
          ownerEmail: applicationDoc.ownerEmail || "",
          ownerName: applicationDoc.ownerName || "",
          generatedByUserId: normalizarCoachCrmObjectId(applicationDoc.generatedByUserId),
          generatedByName: applicationDoc.generatedByName || "",
          generatedByAccountType: applicationDoc.generatedByAccountType || "",
          sourceType: "reclutamiento",
          sourceRecordId,
          sourceParentId: "",
          sourceSubIndex: -1,
          linkedLeadId: null,
          linkedProgramSheetId: null,
          linkedApplicationId: normalizarCoachCrmObjectId(applicationDoc._id),
          leadName: applicationDoc.fullName || "",
          phone: applicationDoc.phone || "",
          email: applicationDoc.email || "",
          interest: applicationDoc.workPreference || "Reclutamiento",
          briefHistory: truncarCoachCrmText(
            applicationDoc.summary || construirCoachRecruitmentApplicationSummary(applicationDoc),
            320
          ),
          privateNotes: truncarCoachCrmText(applicationDoc.about || "", 1200),
          lastNote: truncarCoachCrmText(applicationDoc.about || "", 240),
          sourceUpdatedAt: applicationDoc.updatedAt || applicationDoc.createdAt || null,
          updatedAt: new Date()
        },
        $setOnInsert: {
          createdAt: applicationDoc.createdAt || new Date(),
          status: resolverCoachCrmStatusDesdeRecruitment(""),
          statusColor: resolverCoachCrmStatusColor("nuevo"),
          nextAction: "",
          nextActionAt: null,
          appointmentAt: null,
          city: "",
          zipCode: "",
          address: "",
          officeId: "",
          territoryId: "",
          assignedTelemarketerUserId: defaultTelemarketerUserId,
          assignedTelemarketerName: defaultTelemarketerName,
          appointmentRepUserId: null,
          appointmentRepName: "",
          lastContactAt: null,
          saleResult: "",
          saleAmount: 0,
          soldAt: null,
          closedAt: null
        }
      },
      upsert: true
    }
  };
}

function construirCoachCrmWorkspaceQuery(userDoc = null, extra = {}) {
  const query = {
    ownerUserId: resolverCoachOwnerUserId(userDoc),
    ...extra
  };

  const accountType = normalizarCoachAccountType(userDoc?.accountType || "owner");

  if (accountType === "seat" && userDoc?._id && query.ownerUserId && String(query.ownerUserId) !== String(userDoc._id)) {
    query.generatedByUserId = userDoc._id;
  }

  return query;
}

async function construirCoachCrmViewerQuery(userDoc = null, extra = {}) {
  const query = {
    ownerUserId: resolverCoachOwnerUserId(userDoc),
    ...extra
  };

  const accountType = normalizarCoachAccountType(userDoc?.accountType || "owner");

  if (accountType === "seat" && userDoc?._id && query.ownerUserId && String(query.ownerUserId) !== String(userDoc._id)) {
    const profileDoc = await CoachDistributorProfile.findOne({ userId: userDoc._id })
      .select("teamRole")
      .lean();
    const teamRole = normalizarCoachTeamRole(profileDoc?.teamRole || "", userDoc);

    if (teamRole === "telemarketing") {
      query.assignedTelemarketerUserId = userDoc._id;
    } else {
      query.generatedByUserId = userDoc._id;
    }
  }

  return query;
}

async function resolverCoachCrmTelemarketerPorDefecto(userDoc = null) {
  const ownerUserId = normalizarCoachCrmObjectId(resolverCoachOwnerUserId(userDoc));

  if (!ownerUserId) {
    return null;
  }

  const seatDocs = await CoachUser.find({
    teamOwnerUserId: ownerUserId,
    accountType: "seat",
    seatStatus: "active"
  })
    .select("_id name email")
    .lean();

  if (!seatDocs.length) {
    return null;
  }

  const seatIds = seatDocs.map(doc => doc._id).filter(Boolean);
  const teleProfileDocs = await CoachDistributorProfile.find({
    userId: { $in: seatIds },
    teamRole: "telemarketing"
  })
    .select("userId teamRole")
    .lean();

  const teleIds = teleProfileDocs.map(doc => String(doc.userId || "")).filter(Boolean);
  const teleSeatDocs = seatDocs.filter(doc => teleIds.includes(String(doc._id || "")));

  if (teleSeatDocs.length !== 1) {
    return null;
  }

  return {
    userId: teleSeatDocs[0]._id,
    name: teleSeatDocs[0].name || teleSeatDocs[0].email || "Telemarketing",
    email: teleSeatDocs[0].email || ""
  };
}

async function asegurarCoachCrmAutoAsignacionTelemarketing(userDoc = null, defaultTelemarketer = null) {
  const ownerUserId = normalizarCoachCrmObjectId(resolverCoachOwnerUserId(userDoc));
  const assignedTelemarketerUserId = normalizarCoachCrmObjectId(
    defaultTelemarketer?.userId || defaultTelemarketer?._id
  );

  if (!ownerUserId || !assignedTelemarketerUserId) {
    return;
  }

  await CoachCrmRecord.updateMany(
    {
      ownerUserId,
      $or: [{ assignedTelemarketerUserId: null }, { assignedTelemarketerUserId: { $exists: false } }]
    },
    {
      $set: {
        assignedTelemarketerUserId,
        assignedTelemarketerName: defaultTelemarketer?.name || defaultTelemarketer?.email || "Telemarketing"
      }
    }
  );
}

async function ejecutarCoachCrmSeedOperations(userDoc = null, operations = [], defaultTelemarketer = null) {
  const safeOperations = Array.isArray(operations) ? operations.filter(Boolean) : [];

  if (!userDoc?._id || !safeOperations.length) {
    return 0;
  }

  try {
    await CoachCrmRecord.bulkWrite(safeOperations, { ordered: false });
    await asegurarCoachCrmAutoAsignacionTelemarketing(userDoc, defaultTelemarketer);
  } catch (error) {
    if (error?.code !== 11000) {
      throw error;
    }
  }

  return safeOperations.length;
}

async function sincronizarCoachLeadInboxACrmRecord(userDoc = null, leadDoc = null, defaultTelemarketer = null) {
  if (!userDoc?._id || !leadDoc?._id) {
    return 0;
  }

  const telemarketer = defaultTelemarketer || (await resolverCoachCrmTelemarketerPorDefecto(userDoc));
  const operations = [];
  const leadOperation = construirCoachCrmLeadSeedOperation(leadDoc, telemarketer);

  if (leadOperation) {
    operations.push(leadOperation);
  }

  if (normalizarCoachLeadSource(leadDoc.source || "") === "programa_4_en_14") {
    const sheetDocs = await CoachProgramSheet.find({
      ownerUserId: leadDoc.ownerUserId,
      "referrals.createdLeadId": leadDoc._id
    }).lean();

    sheetDocs.forEach(sheetDoc => {
      (Array.isArray(sheetDoc.referrals) ? sheetDoc.referrals : []).forEach((referral, referralIndex) => {
        if (String(referral?.createdLeadId || "") !== String(leadDoc._id)) {
          return;
        }

        const programOperation = construirCoachCrmProgramSeedOperation(
          sheetDoc,
          referral,
          referralIndex,
          leadDoc,
          telemarketer
        );

        if (programOperation) {
          operations.push(programOperation);
        }
      });
    });
  }

  return ejecutarCoachCrmSeedOperations(userDoc, operations, telemarketer);
}

async function sincronizarCoachRecruitmentApplicationACrmRecord(userDoc = null, applicationDoc = null, defaultTelemarketer = null) {
  if (!userDoc?._id || !applicationDoc?._id) {
    return 0;
  }

  const telemarketer = defaultTelemarketer || (await resolverCoachCrmTelemarketerPorDefecto(userDoc));
  const operation = construirCoachCrmRecruitmentSeedOperation(applicationDoc, telemarketer);

  if (!operation) {
    return 0;
  }

  return ejecutarCoachCrmSeedOperations(userDoc, [operation], telemarketer);
}

async function sincronizarCoachProgramSheetACrmRecords(userDoc = null, sheetDoc = null, defaultTelemarketer = null) {
  if (!userDoc?._id || !sheetDoc?._id) {
    return 0;
  }

  const telemarketer = defaultTelemarketer || (await resolverCoachCrmTelemarketerPorDefecto(userDoc));
  const linkedLeadIds = (Array.isArray(sheetDoc.referrals) ? sheetDoc.referrals : [])
    .map(referral => referral?.createdLeadId)
    .filter(id => mongoose.Types.ObjectId.isValid(id));
  const linkedLeadDocs = linkedLeadIds.length
    ? await CoachLeadInbox.find({ _id: { $in: linkedLeadIds } }).lean()
    : [];
  const linkedLeadMap = new Map(linkedLeadDocs.map(doc => [String(doc._id), doc]));
  const operations = [];

  (Array.isArray(sheetDoc.referrals) ? sheetDoc.referrals : []).forEach((referral, referralIndex) => {
    const linkedLead =
      referral?.createdLeadId ? linkedLeadMap.get(String(referral.createdLeadId)) || null : null;
    const operation = construirCoachCrmProgramSeedOperation(
      sheetDoc,
      referral,
      referralIndex,
      linkedLead,
      telemarketer
    );

    if (operation) {
      operations.push(operation);
    }
  });

  return ejecutarCoachCrmSeedOperations(userDoc, operations, telemarketer);
}

async function resolverCoachUserParaLeadInbox(leadDoc = null) {
  if (!leadDoc) {
    return null;
  }

  const candidateUserIds = [leadDoc.ownerUserId, leadDoc.generatedByUserId]
    .map(value => normalizarCoachCrmObjectId(value))
    .filter(Boolean);

  for (const userId of candidateUserIds) {
    let userDoc = await CoachUser.findById(userId);

    if (!userDoc?._id) {
      continue;
    }

    userDoc = await asegurarCoachUserBase(userDoc);
    return userDoc;
  }

  return null;
}

async function sincronizarCoachLeadInboxACrmDesdeLead(leadDoc = null, userDoc = null, defaultTelemarketer = null) {
  if (!leadDoc?._id) {
    return 0;
  }

  const effectiveUserDoc = userDoc?._id ? userDoc : await resolverCoachUserParaLeadInbox(leadDoc);

  if (!effectiveUserDoc?._id) {
    return 0;
  }

  return sincronizarCoachLeadInboxACrmRecord(effectiveUserDoc, leadDoc, defaultTelemarketer);
}

async function asegurarCoachCrmWorkspaceSiVacio(userDoc = null) {
  const ownerUserId = normalizarCoachCrmObjectId(resolverCoachOwnerUserId(userDoc));

  if (!userDoc?._id || !ownerUserId) {
    return false;
  }

  const hasExistingRecords = await CoachCrmRecord.exists({ ownerUserId });

  if (hasExistingRecords) {
    return false;
  }

  await asegurarCoachCrmWorkspace(userDoc);
  return true;
}

async function asegurarCoachCrmWorkspace(userDoc = null) {
  if (!userDoc?._id) {
    return;
  }

  const defaultTelemarketer = await resolverCoachCrmTelemarketerPorDefecto(userDoc);

  const [leadDocs, applicationDocs, sheetDocs] = await Promise.all([
    CoachLeadInbox.find({
      ...construirCoachWorkspaceQuery(userDoc),
      source: { $ne: "programa_4_en_14" }
    })
      .sort({ updatedAt: -1, createdAt: -1 })
      .limit(400)
      .lean(),
    CoachRecruitmentApplication.find(construirCoachWorkspaceQuery(userDoc))
      .sort({ updatedAt: -1, createdAt: -1 })
      .limit(300)
      .lean(),
    CoachProgramSheet.find(
      construirCoachWorkspaceQuery(userDoc, {
        programType: "4_en_14"
      })
    )
      .sort({ updatedAt: -1, createdAt: -1 })
      .limit(140)
      .lean()
  ]);

  const linkedLeadIds = [];
  sheetDocs.forEach(sheetDoc => {
    (Array.isArray(sheetDoc.referrals) ? sheetDoc.referrals : []).forEach(referral => {
      if (referral?.createdLeadId && mongoose.Types.ObjectId.isValid(referral.createdLeadId)) {
        linkedLeadIds.push(referral.createdLeadId);
      }
    });
  });

  const linkedLeadDocs = linkedLeadIds.length
    ? await CoachLeadInbox.find({
        _id: { $in: linkedLeadIds }
      }).lean()
    : [];
  const linkedLeadMap = new Map(linkedLeadDocs.map(doc => [String(doc._id), doc]));
  const operations = [];

  leadDocs.forEach(leadDoc => {
    const operation = construirCoachCrmLeadSeedOperation(leadDoc, defaultTelemarketer);
    if (operation) {
      operations.push(operation);
    }
  });

  applicationDocs.forEach(applicationDoc => {
    const operation = construirCoachCrmRecruitmentSeedOperation(applicationDoc, defaultTelemarketer);
    if (operation) {
      operations.push(operation);
    }
  });

  sheetDocs.forEach(sheetDoc => {
    (Array.isArray(sheetDoc.referrals) ? sheetDoc.referrals : []).forEach((referral, referralIndex) => {
      const linkedLead = referral?.createdLeadId ? linkedLeadMap.get(String(referral.createdLeadId)) || null : null;
      const operation = construirCoachCrmProgramSeedOperation(
        sheetDoc,
        referral,
        referralIndex,
        linkedLead,
        defaultTelemarketer
      );
      if (operation) {
        operations.push(operation);
      }
    });
  });

  if (!operations.length) {
    return;
  }

  await ejecutarCoachCrmSeedOperations(userDoc, operations, defaultTelemarketer);
}

function crearCoachCrmPerformanceBucket(label = "") {
  return {
    label: cleanText(label || "").trim().slice(0, 120),
    total: 0,
    appointments: 0,
    sales: 0,
    soldAmount: 0,
    noAnswer: 0,
    followUps: 0,
    outside: 0,
    inside: 0,
    completed: 0
  };
}

function registrarCoachCrmPerformanceBucket(bucketMap = new Map(), key = "", label = "", record = null) {
  const safeKey = String(key || "").trim();
  const safeLabel = cleanText(label || "").trim();

  if (!safeKey || !safeLabel || !record) {
    return;
  }

  const bucket = bucketMap.get(safeKey) || crearCoachCrmPerformanceBucket(safeLabel);
  const status = normalizarCoachCrmStatus(record.status || "nuevo");

  bucket.total += 1;

  if (["cita_agendada", "reagendada", "ya_afuera", "entro_a_casa"].includes(status)) {
    bucket.appointments += 1;
  }

  if (status === "no_atendio") {
    bucket.noAnswer += 1;
  }

  if (status === "seguimiento") {
    bucket.followUps += 1;
  }

  if (status === "ya_afuera") {
    bucket.outside += 1;
  }

  if (["entro_a_casa", "demo_hecha", "venta"].includes(status)) {
    bucket.inside += 1;
  }

  if (["demo_hecha", "venta", "no_venta"].includes(status)) {
    bucket.completed += 1;
  }

  if (status === "venta") {
    bucket.sales += 1;
    bucket.soldAmount += limpiarCoachDemoOutcomeAmount(record.saleAmount || 0);
  }

  bucketMap.set(safeKey, bucket);
}

function construirCoachCrmPerformanceTopList(bucketMap = new Map(), limit = 3) {
  return Array.from(bucketMap.values())
    .filter(item => item?.label)
    .sort((a, b) => {
      if (b.soldAmount !== a.soldAmount) {
        return b.soldAmount - a.soldAmount;
      }
      if (b.sales !== a.sales) {
        return b.sales - a.sales;
      }
      if (b.appointments !== a.appointments) {
        return b.appointments - a.appointments;
      }
      if (b.total !== a.total) {
        return b.total - a.total;
      }
      return String(a.label || "").localeCompare(String(b.label || ""), "es", { sensitivity: "base" });
    })
    .slice(0, limit)
    .map(item => ({
      ...item,
      soldAmount: limpiarCoachDemoOutcomeAmount(item.soldAmount || 0)
    }));
}

function construirCoachCrmSummary(records = []) {
  const safeRecords = Array.isArray(records) ? records : [];
  const now = new Date();
  const startOfToday = obtenerInicioDia(now);
  const endOfToday = obtenerFinDia(now);
  const telemarketerBuckets = new Map();
  const representativeBuckets = new Map();
  const territoryBuckets = new Map();
  const summary = {
    total: 0,
    pendingToday: 0,
    appointments: 0,
    sales: 0,
    soldAmount: 0,
    noAnswerCount: 0,
    followUpCount: 0,
    territoryCount: 0,
    bySource: {
      lead: 0,
      programa_4_en_14: 0,
      reclutamiento: 0
    },
    byStatus: {}
  };

  safeRecords.forEach(record => {
    const status = normalizarCoachCrmStatus(record.status || "nuevo");
    const sourceType = normalizarCoachCrmSourceType(record.sourceType || "lead");
    summary.total += 1;
    summary.bySource[sourceType] = Number(summary.bySource[sourceType] || 0) + 1;
    summary.byStatus[status] = Number(summary.byStatus[status] || 0) + 1;

    if (record.nextActionAt) {
      const nextDate = new Date(record.nextActionAt);
      if (nextDate >= startOfToday && nextDate <= endOfToday) {
        summary.pendingToday += 1;
      }
    }

    if (["cita_agendada", "reagendada", "ya_afuera", "entro_a_casa"].includes(status)) {
      summary.appointments += 1;
    }

    if (status === "no_atendio") {
      summary.noAnswerCount += 1;
    }

    if (status === "seguimiento") {
      summary.followUpCount += 1;
    }

    if (status === "venta") {
      summary.sales += 1;
      summary.soldAmount += limpiarCoachDemoOutcomeAmount(record.saleAmount || 0);
    }

    registrarCoachCrmPerformanceBucket(
      telemarketerBuckets,
      record.assignedTelemarketerUserId || "",
      record.assignedTelemarketerName || "",
      record
    );
    registrarCoachCrmPerformanceBucket(
      representativeBuckets,
      record.appointmentRepUserId || "",
      record.appointmentRepName || "",
      record
    );
    registrarCoachCrmPerformanceBucket(
      territoryBuckets,
      record.territoryId || record.officeId || "",
      record.territoryId || record.officeId || "",
      record
    );
  });

  summary.territoryCount = territoryBuckets.size;
  summary.closeRate = summary.total ? Number(((summary.sales / summary.total) * 100).toFixed(1)) : 0;
  summary.appointmentCloseRate = summary.appointments
    ? Number(((summary.sales / summary.appointments) * 100).toFixed(1))
    : 0;
  summary.topTelemarketers = construirCoachCrmPerformanceTopList(telemarketerBuckets);
  summary.topRepresentatives = construirCoachCrmPerformanceTopList(representativeBuckets);
  summary.topTerritories = construirCoachCrmPerformanceTopList(territoryBuckets);

  return summary;
}

function limpiarCoachCrmRecord(recordDoc = null) {
  if (!recordDoc?._id) {
    return null;
  }

  return {
    id: String(recordDoc._id),
    crmRecordId: recordDoc.crmRecordId || "",
    ownerUserId: recordDoc.ownerUserId ? String(recordDoc.ownerUserId) : "",
    generatedByUserId: recordDoc.generatedByUserId ? String(recordDoc.generatedByUserId) : "",
    generatedByName: recordDoc.generatedByName || "",
    generatedByAccountType: normalizarCoachAccountType(recordDoc.generatedByAccountType || "owner"),
    sourceType: normalizarCoachCrmSourceType(recordDoc.sourceType || "lead"),
    sourceRecordId: recordDoc.sourceRecordId || "",
    sourceParentId: recordDoc.sourceParentId || "",
    sourceSubIndex: Number(recordDoc.sourceSubIndex ?? -1),
    linkedLeadId: recordDoc.linkedLeadId ? String(recordDoc.linkedLeadId) : "",
    linkedProgramSheetId: recordDoc.linkedProgramSheetId ? String(recordDoc.linkedProgramSheetId) : "",
    linkedApplicationId: recordDoc.linkedApplicationId ? String(recordDoc.linkedApplicationId) : "",
    linkedHealthSurveyId: recordDoc.linkedHealthSurveyId ? String(recordDoc.linkedHealthSurveyId) : "",
    linkedHealthSurveyName: recordDoc.linkedHealthSurveyName || "",
    latestProgramSheetId: recordDoc.latestProgramSheetId ? String(recordDoc.latestProgramSheetId) : "",
    latestProgramSummary: recordDoc.latestProgramSummary || "",
    latestDemoOutcomeId: recordDoc.latestDemoOutcomeId ? String(recordDoc.latestDemoOutcomeId) : "",
    latestDemoOutcomeType: normalizarCoachDemoOutcomeType(recordDoc.latestDemoOutcomeType || ""),
    latestDemoOutcomeSummary: recordDoc.latestDemoOutcomeSummary || "",
    officeId: recordDoc.officeId || "",
    territoryId: recordDoc.territoryId || "",
    leadName: recordDoc.leadName || "",
    phone: recordDoc.phone || "",
    email: recordDoc.email || "",
    address: recordDoc.address || "",
    city: recordDoc.city || "",
    zipCode: recordDoc.zipCode || "",
    interest: recordDoc.interest || "",
    status: normalizarCoachCrmStatus(recordDoc.status || "nuevo"),
    statusColor: recordDoc.statusColor || resolverCoachCrmStatusColor(recordDoc.status || "nuevo"),
    nextAction: normalizarCoachLeadNextAction(recordDoc.nextAction || ""),
    nextActionAt: recordDoc.nextActionAt || null,
    appointmentAt: recordDoc.appointmentAt || null,
    assignedTelemarketerUserId: recordDoc.assignedTelemarketerUserId ? String(recordDoc.assignedTelemarketerUserId) : "",
    assignedTelemarketerName: recordDoc.assignedTelemarketerName || "",
    appointmentRepUserId: recordDoc.appointmentRepUserId ? String(recordDoc.appointmentRepUserId) : "",
    appointmentRepName: recordDoc.appointmentRepName || "",
    briefHistory: recordDoc.briefHistory || "",
    privateNotes: recordDoc.privateNotes || "",
    lastNote: recordDoc.lastNote || "",
    lastContactAt: recordDoc.lastContactAt || null,
    saleResult: recordDoc.saleResult || "",
    saleAmount: limpiarCoachDemoOutcomeAmount(recordDoc.saleAmount || 0),
    soldAt: recordDoc.soldAt || null,
    closedAt: recordDoc.closedAt || null,
    sourceUpdatedAt: recordDoc.sourceUpdatedAt || null,
    updatedAt: recordDoc.updatedAt || null,
    createdAt: recordDoc.createdAt || null
  };
}

function limpiarCoachCrmActivity(activityDoc = null) {
  if (!activityDoc?._id) {
    return null;
  }

  return {
    id: String(activityDoc._id),
    crmRecordId: activityDoc.crmRecordId || "",
    actorUserId: activityDoc.actorUserId ? String(activityDoc.actorUserId) : "",
    actorName: activityDoc.actorName || "",
    type: activityDoc.type || "note",
    body: activityDoc.body || "",
    meta: activityDoc.meta || null,
    createdAt: activityDoc.createdAt || null
  };
}

function normalizarCoachContactImportText(value = "", maxLength = 1200000) {
  return String(value || "").replace(/\r\n?/g, "\n").slice(0, maxLength);
}

function normalizarCoachContactHeader(value = "") {
  return String(value || "")
    .toLowerCase()
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/[^a-z0-9]+/g, " ")
    .trim();
}

function parseCoachCsvRows(text = "") {
  const input = normalizarCoachContactImportText(text);
  const rows = [];
  let row = [];
  let current = "";
  let inQuotes = false;

  for (let index = 0; index < input.length; index += 1) {
    const char = input[index];
    const next = input[index + 1];

    if (char === '"') {
      if (inQuotes && next === '"') {
        current += '"';
        index += 1;
      } else {
        inQuotes = !inQuotes;
      }
      continue;
    }

    if (!inQuotes && char === ",") {
      row.push(current);
      current = "";
      continue;
    }

    if (!inQuotes && char === "\n") {
      row.push(current);
      rows.push(row);
      row = [];
      current = "";
      continue;
    }

    current += char;
  }

  if (current || row.length) {
    row.push(current);
    rows.push(row);
  }

  return rows
    .map(item => item.map(value => String(value || "").trim()))
    .filter(item => item.some(value => value));
}

function detectarCoachCsvHeaderMap(headers = []) {
  const normalized = headers.map(normalizarCoachContactHeader);
  const hasHeaders = normalized.some(header =>
    /(name|nombre|phone|telefono|email|correo|mail)/i.test(header)
  );
  const findIndex = matcher => normalized.findIndex(matcher);

  return {
    hasHeaders,
    nameIndex: findIndex(header => /^(name|full name|nombre|nombre completo|fn|display name)$/.test(header)),
    firstNameIndex: findIndex(header => /^(given name|first name|nombre|nombre 1|nombre de pila)$/.test(header)),
    lastNameIndex: findIndex(header => /^(family name|last name|apellido|apellidos)$/.test(header)),
    phoneIndex: findIndex(header => /(phone|telefono|mobile|celular)/.test(header) && (!/type/.test(header) || /value/.test(header))),
    emailIndex: findIndex(header => /(email|correo|mail)/.test(header) && (!/type/.test(header) || /value/.test(header))),
    notesIndex: findIndex(header => /(notes|nota|company|empresa|organization|organizacion)/.test(header))
  };
}

function construirCoachImportedContact(candidate = {}) {
  const phone = normalizePhone(candidate.phone || "");
  const email = normalizarEmail(candidate.email || "");
  const notes = cleanText(candidate.notes || "").slice(0, 280);
  let fullName = seleccionarNombreConfiable(
    candidate.fullName || "",
    [candidate.firstName || "", candidate.lastName || ""].filter(Boolean).join(" "),
    candidate.firstName || "",
    candidate.lastName || ""
  );

  if (!phone && !email) {
    return null;
  }

  if (!fullName) {
    fullName = email || (phone ? `Contacto ${phone.slice(-4)}` : "Contacto compartido");
  }

  return {
    fullName: cleanText(fullName).slice(0, 120),
    phone,
    email,
    notes
  };
}

function deduplicarCoachImportedContacts(contacts = []) {
  const seen = new Map();

  (Array.isArray(contacts) ? contacts : []).forEach(contact => {
    if (!contact) {
      return;
    }

    const key =
      contact.phone ||
      contact.email ||
      normalizarCoachContactHeader(contact.fullName || "");

    if (!key) {
      return;
    }

    if (!seen.has(key)) {
      seen.set(key, contact);
      return;
    }

    const previous = seen.get(key);
    seen.set(key, {
      ...previous,
      fullName: previous.fullName || contact.fullName,
      phone: previous.phone || contact.phone,
      email: previous.email || contact.email,
      notes: [previous.notes || "", contact.notes || ""].filter(Boolean).join(" | ").slice(0, 280)
    });
  });

  return Array.from(seen.values());
}

function parseCoachCsvContacts(text = "") {
  const rows = parseCoachCsvRows(text);

  if (!rows.length) {
    return [];
  }

  const headerMap = detectarCoachCsvHeaderMap(rows[0]);
  const dataRows = headerMap.hasHeaders ? rows.slice(1) : rows;

  return deduplicarCoachImportedContacts(
    dataRows
      .map(row => {
        const firstName = headerMap.firstNameIndex >= 0 ? row[headerMap.firstNameIndex] || "" : "";
        const lastName = headerMap.lastNameIndex >= 0 ? row[headerMap.lastNameIndex] || "" : "";
        const fullName =
          headerMap.nameIndex >= 0
            ? row[headerMap.nameIndex] || ""
            : [firstName, lastName].filter(Boolean).join(" ").trim();
        const phone =
          headerMap.phoneIndex >= 0
            ? row[headerMap.phoneIndex] || ""
            : row.find(value => normalizePhone(value));
        const email =
          headerMap.emailIndex >= 0
            ? row[headerMap.emailIndex] || ""
            : row.find(value => normalizarEmail(value));
        const notes = headerMap.notesIndex >= 0 ? row[headerMap.notesIndex] || "" : "";
        return construirCoachImportedContact({ fullName, firstName, lastName, phone, email, notes });
      })
      .filter(Boolean)
  );
}

function decodeCoachVcardValue(value = "") {
  return String(value || "")
    .replace(/\\n/gi, " ")
    .replace(/\\,/g, ",")
    .replace(/\\;/g, ";")
    .trim();
}

function parseCoachVcardContacts(text = "") {
  const safeText = normalizarCoachContactImportText(text).replace(/\n[ \t]/g, "");
  const blocks = safeText.match(/BEGIN:VCARD[\s\S]*?END:VCARD/gi) || [];

  return deduplicarCoachImportedContacts(
    blocks
      .map(block => {
        const fullNameMatch = block.match(/^FN[^:]*:(.+)$/im);
        const nMatch = block.match(/^N[^:]*:(.+)$/im);
        const phoneMatch = block.match(/^TEL[^:]*:(.+)$/im);
        const emailMatch = block.match(/^EMAIL[^:]*:(.+)$/im);
        const noteMatch = block.match(/^NOTE[^:]*:(.+)$/im);
        let fullName = decodeCoachVcardValue(fullNameMatch?.[1] || "");

        if (!fullName && nMatch?.[1]) {
          const parts = decodeCoachVcardValue(nMatch[1]).split(";").map(part => part.trim());
          fullName = [parts[1] || "", parts[0] || ""].filter(Boolean).join(" ").trim();
        }

        return construirCoachImportedContact({
          fullName,
          phone: decodeCoachVcardValue(phoneMatch?.[1] || ""),
          email: decodeCoachVcardValue(emailMatch?.[1] || ""),
          notes: decodeCoachVcardValue(noteMatch?.[1] || "")
        });
      })
      .filter(Boolean)
  );
}

function parseCoachPastedContacts(text = "") {
  const safeText = normalizarCoachContactImportText(text);

  if (!safeText.trim()) {
    return [];
  }

  if (/BEGIN:VCARD/i.test(safeText)) {
    return parseCoachVcardContacts(safeText);
  }

  const lines = safeText
    .split("\n")
    .map(line => line.trim())
    .filter(Boolean);

  if (!lines.length) {
    return [];
  }

  if ((safeText.includes(",") || safeText.includes("\t")) && lines.length > 1) {
    const csvContacts = parseCoachCsvContacts(safeText);

    if (csvContacts.length) {
      return csvContacts;
    }
  }

  return deduplicarCoachImportedContacts(
    lines
      .map(line => {
        const emailMatch = line.match(/[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}/i);
        const phoneMatch = line.match(/(?:\+?1[\s.-]?)?(?:\(?\d{3}\)?[\s.-]?)\d{3}[\s.-]?\d{4}/);
        const email = emailMatch?.[0] || "";
        const phone = phoneMatch?.[0] || "";
        const fullName = line
          .replace(email, " ")
          .replace(phone, " ")
          .replace(/[|,;]+/g, " ")
          .replace(/\s+/g, " ")
          .trim();

        return construirCoachImportedContact({ fullName, phone, email });
      })
      .filter(Boolean)
  );
}

function parseCoachImportedContacts(importMode = "", rawText = "") {
  const safeMode = String(importMode || "").trim().toLowerCase();
  const safeText = normalizarCoachContactImportText(rawText);

  if (!safeText.trim()) {
    return [];
  }

  if (safeMode === "vcf") {
    return parseCoachVcardContacts(safeText);
  }

  if (safeMode === "csv") {
    return parseCoachCsvContacts(safeText);
  }

  return parseCoachPastedContacts(safeText);
}

function limpiarCoachProgramReferral(item = null) {
  const fullName = seleccionarNombreConfiable(item?.fullName || item?.name || "") || String(item?.fullName || item?.name || "").trim();
  const phone = normalizePhone(item?.phone || "");
  const notes = String(item?.notes || "").trim().slice(0, 240);

  if (!fullName && !phone && !notes) {
    return null;
  }

  if (!fullName || !phone) {
    return null;
  }

  return {
    fullName,
    phone,
    notes
  };
}

function construirCoachProgramSheetSummary(sheet = null) {
  if (!sheet) {
    return "";
  }

  const parts = [];

  if (sheet.hostName) {
    parts.push(`Anfitrion: ${truncarTextoPrompt(sheet.hostName, 80)}.`);
  }

  if (sheet.giftSelected) {
    parts.push(`Regalo: ${truncarTextoPrompt(sheet.giftSelected, 80)}.`);
  }

  if (sheet.startWindow) {
    parts.push(`Periodo: ${truncarTextoPrompt(sheet.startWindow, 90)}.`);
  }

  if (sheet.referralCount) {
    parts.push(`Referidos guardados: ${sheet.referralCount}.`);
  }

  if (sheet.representativeName) {
    parts.push(`Representante: ${truncarTextoPrompt(sheet.representativeName, 80)}.`);
  }

  if (sheet.notes) {
    parts.push(`Notas: ${truncarTextoPrompt(sheet.notes, 120)}.`);
  }

  return parts.join(" ").trim();
}

function normalizarCoachProgramInstantStatus(value = "") {
  const normalizado = String(value || "")
    .trim()
    .toLowerCase();
  const permitidos = new Set(["", "seleccionado", "cita_lograda", "no_contesto", "llamar_despues", "no_quiso"]);
  return permitidos.has(normalizado) ? normalizado : "";
}

function construirCoachProgram414Scripts(sheet = null, referral = null) {
  const referralName = referral?.fullName || "la persona";
  const hostName = sheet?.hostName || "el anfitrion";
  const representativeName = sheet?.representativeName || "el representante";
  const giftLabel = sheet?.giftSelected ? ` por el programa de regalos de ${sheet.giftSelected}` : " por el programa de regalos";
  const hostScript = `Hola ${referralName}, estoy aqui con ${representativeName} de Royal Prestige${giftLabel}. Te lo paso.`;
  const repScript = `Hola ${referralName}, gusto saludarte. Estoy aqui con ${hostName} y te marco porque estamos apartando visitas cortas${giftLabel}. Tengo un espacio hoy y otro manana, cual te queda mejor?`;
  const focus = "No vendas producto completo aqui. Solo amarra dia y hora para la visita.";

  return {
    hostScript,
    repScript,
    focus
  };
}

function limpiarCoachProgramReferralView(referral = null, index = 0, sheet = null) {
  if (!referral) {
    return null;
  }

  const scripts = construirCoachProgram414Scripts(sheet, referral);

  return {
    index,
    fullName: referral.fullName || "",
    phone: referral.phone || "",
    notes: referral.notes || "",
    createdLeadId: referral.createdLeadId ? String(referral.createdLeadId) : "",
    instantCallStatus: normalizarCoachProgramInstantStatus(referral.instantCallStatus || ""),
    instantCallNotes: referral.instantCallNotes || "",
    appointmentDetails: referral.appointmentDetails || "",
    selectedForInstantCallAt: referral.selectedForInstantCallAt || null,
    lastOutcomeAt: referral.lastOutcomeAt || null,
    scripts
  };
}

function limpiarCoachProgramSheet(sheetDoc = null) {
  if (!sheetDoc) {
    return null;
  }

  return {
    id: String(sheetDoc._id),
    linkedCrmRecordId: sheetDoc.linkedCrmRecordId ? String(sheetDoc.linkedCrmRecordId) : "",
    ownerUserId: sheetDoc.ownerUserId ? String(sheetDoc.ownerUserId) : "",
    ownerEmail: sheetDoc.ownerEmail || "",
    ownerName: sheetDoc.ownerName || "",
    generatedByUserId: sheetDoc.generatedByUserId ? String(sheetDoc.generatedByUserId) : "",
    generatedByName: sheetDoc.generatedByName || "",
    generatedByAccountType: normalizarCoachAccountType(sheetDoc.generatedByAccountType || "owner"),
    programType: sheetDoc.programType || "4_en_14",
    hostName: sheetDoc.hostName || "",
    hostPhone: sheetDoc.hostPhone || "",
    giftSelected: sheetDoc.giftSelected || "",
    representativeName: sheetDoc.representativeName || "",
    representativePhone: sheetDoc.representativePhone || "",
    startWindow: sheetDoc.startWindow || "",
    notes: sheetDoc.notes || "",
    referralCount: Number(sheetDoc.referralCount || 0),
    summary: sheetDoc.summary || "",
    referrals: Array.isArray(sheetDoc.referrals)
      ? sheetDoc.referrals.map((referral, index) => limpiarCoachProgramReferralView(referral, index, sheetDoc)).filter(Boolean)
      : [],
    updatedAt: sheetDoc.updatedAt || null,
    createdAt: sheetDoc.createdAt || null
  };
}

function limpiarCoachProgram414ActiveContext(context = null) {
  if (!context || typeof context !== "object") {
    return null;
  }

  const sheetId = cleanText(context.sheetId || context.id || "");
  const referral = context.referral && typeof context.referral === "object" ? context.referral : null;
  const referralFullName = cleanText(referral?.fullName || "");

  if (!sheetId || !referralFullName) {
    return null;
  }

  return {
    sheetId,
    ownerUserId: cleanText(context.ownerUserId || ""),
    hostName: cleanText(context.hostName || ""),
    hostPhone: cleanText(context.hostPhone || ""),
    giftSelected: cleanText(context.giftSelected || ""),
    representativeName: cleanText(context.representativeName || ""),
    representativePhone: cleanText(context.representativePhone || ""),
    startWindow: cleanText(context.startWindow || ""),
    summary: cleanText(context.summary || ""),
    referralIndex: Number.isInteger(context.referralIndex) ? context.referralIndex : -1,
    referral: {
      fullName: referralFullName,
      phone: cleanText(referral?.phone || ""),
      notes: cleanText(referral?.notes || ""),
      instantCallStatus: cleanText(referral?.instantCallStatus || ""),
      instantCallNotes: cleanText(referral?.instantCallNotes || ""),
      appointmentDetails: cleanText(referral?.appointmentDetails || ""),
      scripts: {
        hostScript: cleanText(referral?.scripts?.hostScript || ""),
        repScript: cleanText(referral?.scripts?.repScript || ""),
        focus: cleanText(referral?.scripts?.focus || "")
      }
    }
  };
}

function construirNotasLeadDesdePrograma414(sheet = null, referral = null, index = 0) {
  const parts = [];

  parts.push(`Programa 4 en 14 · referido ${index + 1}.`);

  if (sheet?.hostName) {
    parts.push(`Anfitrion: ${sheet.hostName}.`);
  }

  if (sheet?.hostPhone) {
    parts.push(`Telefono anfitrion: ${sheet.hostPhone}.`);
  }

  if (sheet?.giftSelected) {
    parts.push(`Regalo elegido: ${sheet.giftSelected}.`);
  }

  if (sheet?.startWindow) {
    parts.push(`Inicio y vencimiento: ${sheet.startWindow}.`);
  }

  if (sheet?.representativeName) {
    parts.push(`Representante: ${sheet.representativeName}.`);
  }

  if (sheet?.representativePhone) {
    parts.push(`Telefono representante: ${sheet.representativePhone}.`);
  }

  if (referral?.notes) {
    parts.push(`Notas del referido: ${referral.notes}.`);
  }

  if (sheet?.notes) {
    parts.push(`Notas de la hoja: ${truncarTextoPrompt(sheet.notes, 120)}.`);
  }

  return parts.join(" ").trim();
}

function construirPromptPrograma414Activo(context = null) {
  if (!context?.sheetId || !context?.referral?.fullName) {
    return "";
  }

  return `
PROGRAMA 4 EN 14 ACTIVO EN ESTA SESION:
- programa_4_en_14_activo: si
- sheet_id: ${context.sheetId}
- anfitrion: ${context.hostName || "sin nombre"}
- telefono_anfitrion: ${context.hostPhone || "sin telefono"}
- referido_activo: ${context.referral.fullName || "sin nombre"}
- telefono_referido: ${context.referral.phone || "sin telefono"}
- regalo_programa: ${context.giftSelected || "sin regalo"}
- representante: ${context.representativeName || "sin nombre"}
- estado_llamada: ${context.referral.instantCallStatus || "seleccionado"}
- notas_del_referido: ${context.referral.notes || "sin notas"}
- notas_de_llamada: ${context.referral.instantCallNotes || "sin notas"}
- detalle_de_cita: ${context.referral.appointmentDetails || "sin detalle"}
- guion_anfitrion: ${context.referral.scripts?.hostScript || "sin guion"}
- guion_representante: ${context.referral.scripts?.repScript || "sin guion"}
- enfoque: ${context.referral.scripts?.focus || "cerrar cita"}

INSTRUCCION:
- toma este contexto como llamada de cita instantanea, no como venta completa del producto
- si el distribuidor te escribe objeciones como "ahorita no puede", "mandame informacion" o "habla con mi esposo", contesta con frases para cerrar cita, no para vender producto
- usa el programa de regalos solo como puente para sacar dia y hora
- no inventes datos fuera de esta hoja
`;
}

function limpiarCoachOrderCalcAmount(value = 0) {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? Number(parsed.toFixed(2)) : 0;
}

function limpiarCoachOrderCalcContext(context = null) {
  if (!context || typeof context !== "object") {
    return null;
  }

  const products = Array.isArray(context.products)
    ? context.products
        .map((product, index) => {
          const name = truncarTextoPrompt(String(product?.name || "").trim(), 120);
          const code = truncarTextoPrompt(String(product?.code || "").trim(), 40);
          const basePrice = limpiarCoachOrderCalcAmount(product?.basePrice);
          const grossTotal = limpiarCoachOrderCalcAmount(product?.grossTotal);
          const downPayment = limpiarCoachOrderCalcAmount(product?.downPayment);
          const discountAmount = limpiarCoachOrderCalcAmount(product?.discountAmount);
          const balance = limpiarCoachOrderCalcAmount(product?.balance);
          const monthly = limpiarCoachOrderCalcAmount(product?.monthly);
          const weekly = limpiarCoachOrderCalcAmount(product?.weekly);
          const daily = limpiarCoachOrderCalcAmount(product?.daily);
          const slot = Number.parseInt(product?.slot || "", 10);

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
        .slice(0, 3)
    : [];

  const summary = {
    totalGross: limpiarCoachOrderCalcAmount(context.summary?.totalGross),
    totalDown: limpiarCoachOrderCalcAmount(context.summary?.totalDown),
    totalDiscount: limpiarCoachOrderCalcAmount(context.summary?.totalDiscount),
    balanceFinal: limpiarCoachOrderCalcAmount(context.summary?.balanceFinal),
    monthly: limpiarCoachOrderCalcAmount(context.summary?.monthly),
    weekly: limpiarCoachOrderCalcAmount(context.summary?.weekly),
    daily: limpiarCoachOrderCalcAmount(context.summary?.daily)
  };

  if (!products.length && summary.totalGross <= 0 && summary.balanceFinal <= 0) {
    return null;
  }

  return {
    ownerUserId: String(context.ownerUserId || "").trim(),
    activatedAt: String(context.activatedAt || "").trim(),
    products,
    summary
  };
}

function construirPromptCalculadoraActiva(context = null) {
  if (!context?.summary) {
    return "";
  }

  const productLines = Array.isArray(context.products)
    ? context.products
        .map(product => {
          const label = product.name || product.code || `Producto ${product.slot}`;
          return `${label} · base ${product.basePrice} · balance ${product.balance} · semanal ${product.weekly}`;
        })
        .join(" | ")
    : "";

  return `
CALCULADORA DE PAGOS ACTIVA EN ESTA SESION:
- escenario_pago_activo: si
- productos_activos: ${productLines || "sin producto"}
- total_bruto: ${context.summary.totalGross}
- down_payment_total: ${context.summary.totalDown}
- descuento_total: ${context.summary.totalDiscount}
- balance_final: ${context.summary.balanceFinal}
- pago_mensual: ${context.summary.monthly}
- pago_semanal: ${context.summary.weekly}
- pago_diario: ${context.summary.daily}

INSTRUCCION:
- usa estos numeros solo cuando la objecion toque precio, pagos, down payment o descuento
- responde desde el escenario actual y no inventes rebajas nuevas que no estan aqui
- si el distribuidor dice "esta caro", apoya primero con semanal, mensual o balance final segun convenga
- si ya hay un buen down payment o descuento cargado, defendelo antes de saltar a otro cierre
`;
}

function limpiarCoachDecisionContext(context = null) {
  if (!context || typeof context !== "object") {
    return null;
  }

  const cleanRows = rows =>
    Array.isArray(rows)
      ? rows
          .map((row, index) => {
            const text = truncarTextoPrompt(String(row?.text || "").trim(), 140);
            const weight = Number.parseInt(row?.weight || "0", 10) || 0;
            const slot = Number.parseInt(row?.slot || "", 10);

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
          .slice(0, 5)
      : [];

  const pros = cleanRows(context.pros);
  const cons = cleanRows(context.cons);
  const summary = {
    totalPros: Number.parseInt(context.summary?.totalPros || "0", 10) || 0,
    totalCons: Number.parseInt(context.summary?.totalCons || "0", 10) || 0,
    percent: Number.parseInt(context.summary?.percent || "0", 10) || 0,
    message: truncarTextoPrompt(String(context.summary?.message || "").trim(), 220),
    topObjection: truncarTextoPrompt(String(context.summary?.topObjection || "").trim(), 180),
    nextStep: truncarTextoPrompt(String(context.summary?.nextStep || "").trim(), 220)
  };

  if (!pros.length && !cons.length && summary.totalPros <= 0 && summary.totalCons <= 0) {
    return null;
  }

  return {
    ownerUserId: String(context.ownerUserId || "").trim(),
    activatedAt: String(context.activatedAt || "").trim(),
    pros,
    cons,
    summary
  };
}

function construirPromptBalanceDecisionActivo(context = null) {
  if (!context?.summary) {
    return "";
  }

  const prosCopy = Array.isArray(context.pros)
    ? context.pros.map(row => `${row.text || "sin texto"} (${row.weight})`).join(" | ")
    : "";
  const consCopy = Array.isArray(context.cons)
    ? context.cons.map(row => `${row.text || "sin texto"} (${row.weight})`).join(" | ")
    : "";

  return `
BALANCE DE DECISION ACTIVO EN ESTA SESION:
- balance_decision_activo: si
- razones_a_favor: ${prosCopy || "sin razones"}
- objeciones_y_frenos: ${consCopy || "sin objeciones"}
- total_beneficios: ${context.summary.totalPros}
- total_objeciones: ${context.summary.totalCons}
- compra_a_favor: ${context.summary.percent}%
- lectura_sugerida: ${context.summary.message || "sin lectura"}
- objecion_principal: ${context.summary.topObjection || "sin objecion"}
- siguiente_paso: ${context.summary.nextStep || "sin paso"}

INSTRUCCION:
- usa este balance cuando el distribuidor te pida ayuda con indecision, "lo voy a pensar", "esta caro" o una objecion que ya aparecio aqui
- responde desde la objecion principal y desde lo que mas peso tuvo a favor
- no ignores esta lectura para brincar a cierres genericos
- si el balance todavia no favorece compra, ayuda a resolver el freno antes de pedir decision
`;
}

function limpiarCoachDemoEventPrompt(event = null) {
  if (!event || typeof event !== "object") {
    return null;
  }

  const label = truncarTextoPrompt(String(event.label || "").trim(), 100);
  const detail = truncarTextoPrompt(String(event.detail || "").trim(), 180);

  if (!label) {
    return null;
  }

  return {
    label,
    detail
  };
}

function construirPromptCoachDemoActivo(context = null) {
  const workspace = String(context?.workspace || "").trim() || "cierre";
  const stageId = String(context?.stageId || "").trim() || "rompe_hielo";
  const stageLabel = truncarTextoPrompt(String(context?.stageLabel || "").trim(), 80) || "Rompe hielo";
  const stageCopy = truncarTextoPrompt(String(context?.stageCopy || "").trim(), 160) || "Sin nota de etapa.";
  const events = Array.isArray(context?.events) ? context.events.map(limpiarCoachDemoEventPrompt).filter(Boolean).slice(0, 6) : [];

  if (!stageLabel && !events.length) {
    return "";
  }

  const eventLines = events.length
    ? events.map((event, index) => `- senal_${index + 1}: ${event.label}${event.detail ? ` | ${event.detail}` : ""}`).join("\n")
    : "- sin_senales_recientes: todavia no hay eventos marcados en esta sesion";

  return `
CONTEXTO DE DEMO ACTIVO EN ESTA SESION:
- modo_actual: ${workspace}
- paso_actual_id: ${stageId}
- paso_actual: ${stageLabel}
- instruccion_de_paso: ${stageCopy}
${eventLines}

INSTRUCCION:
- respeta el paso actual de la demo y no te adelantes a cierres que todavia no toca usar
- usa las senales recientes solo como pistas reales de esta sesion
- si ya se reviso agua, calculadora, oferta o RoyalOne, toma eso en cuenta antes de responder
- si ya se abrio RoyalOne y luego aparece una objecion, asume que la venta ya iba avanzada y ayuda a rearmar el cierre desde ahi
`;
}

async function guardarCoachInboxLead({
  userDoc = null,
  profileDoc = null,
  payload = {},
  sendDestination = true,
  ownershipOverride = null
} = {}) {
  const baseOwnership = await construirCoachOwnershipSnapshot(userDoc);
  const ownership = ownershipOverride?.ownerUserId
    ? {
        ...baseOwnership,
        ...ownershipOverride,
        ownerUserId: ownershipOverride.ownerUserId,
        ownerName: ownershipOverride.ownerName || baseOwnership.ownerName || "",
        ownerEmail: ownershipOverride.ownerEmail || baseOwnership.ownerEmail || "",
        generatedByUserId: ownershipOverride.generatedByUserId || baseOwnership.generatedByUserId || null,
        generatedByName: ownershipOverride.generatedByName || baseOwnership.generatedByName || "",
        generatedByAccountType:
          ownershipOverride.generatedByAccountType || baseOwnership.generatedByAccountType || "owner"
      }
    : baseOwnership;
  const rawName = String(payload?.fullName || payload?.name || "").trim();
  const fullName = seleccionarNombreConfiable(rawName) || rawName;
  const phone = normalizePhone(payload?.phone || "");
  const email = normalizarEmail(payload?.email || "");
  const city = String(payload?.city || "").trim().slice(0, 80);
  const zipCode = normalizarZipCode(payload?.zipCode || payload?.zip || "");
  const interest = String(payload?.interest || "").trim().slice(0, 120);
  const source = normalizarCoachLeadSource(payload?.source || "captura_manual");
  const notes = String(payload?.notes || "").trim().slice(0, 1200);
  const consentGiven = Boolean(payload?.consentGiven);
  const nextAction = normalizarCoachLeadNextAction(payload?.nextAction || "");
  const nextActionAt = parseCoachLeadNextActionAt(payload?.nextActionAt);
  const rawStatus = String(payload?.status || "").trim();
  const nextStatus = rawStatus ? normalizarCoachLeadStatus(rawStatus) : "";
  const summaryOverride = String(payload?.summary || "").trim().slice(0, 600);
  const replaceNotes = payload?.replaceNotes === true;

  if (!fullName) {
    const error = new Error("El nombre es requerido.");
    error.status = 400;
    throw error;
  }

  if (!phone && !email) {
    const error = new Error("Necesitas telefono o correo para guardar el lead.");
    error.status = 400;
    throw error;
  }

  const duplicateQuery = [];

  if (phone) {
    duplicateQuery.push({ phone });
  }

  if (email) {
    duplicateQuery.push({ email });
  }

  const now = new Date();
  let leadDoc = null;

  if (duplicateQuery.length) {
    leadDoc = await CoachLeadInbox.findOne({
      ownerUserId: ownership.ownerUserId,
      $or: duplicateQuery
    }).sort({ createdAt: -1 });
  }

  if (leadDoc) {
    const currentStatus = normalizarCoachLeadStatus(leadDoc.status || "nuevo");
    const statusRank = {
      nuevo: 0,
      contactado: 1,
      agendado: 2,
      cliente: 3
    };

    leadDoc.fullName = fullName || leadDoc.fullName;
    leadDoc.phone = phone || leadDoc.phone || "";
    leadDoc.email = email || leadDoc.email || "";
    leadDoc.city = city || leadDoc.city || "";
    leadDoc.zipCode = zipCode || leadDoc.zipCode || "";
    leadDoc.interest = interest || leadDoc.interest || "";
    leadDoc.source = source || leadDoc.source || "captura_manual";
    leadDoc.consentGiven = consentGiven || leadDoc.consentGiven;
    leadDoc.nextAction = nextAction || leadDoc.nextAction || "";
    if (!leadDoc.generatedByUserId && ownership.generatedByUserId) {
      leadDoc.generatedByUserId = ownership.generatedByUserId;
    }
    if (!leadDoc.generatedByName && ownership.generatedByName) {
      leadDoc.generatedByName = ownership.generatedByName;
    }
    if (!leadDoc.generatedByAccountType && ownership.generatedByAccountType) {
      leadDoc.generatedByAccountType = ownership.generatedByAccountType;
    }

    if (
      nextStatus &&
      currentStatus !== "archivado" &&
      (Number(statusRank[nextStatus] || 0) >= Number(statusRank[currentStatus] || 0))
    ) {
      if (leadDoc.status !== nextStatus) {
        leadDoc.lastStatusChangeAt = now;
      }

      leadDoc.status = nextStatus;
    }

    if (nextActionAt) {
      leadDoc.nextActionAt = nextActionAt;
    }

    if (replaceNotes) {
      leadDoc.notes = notes || leadDoc.notes || "";
    } else if (notes && notes !== leadDoc.notes) {
      leadDoc.notes = leadDoc.notes ? `${leadDoc.notes}\n\n${notes}` : notes;
    }

    leadDoc.summary = summaryOverride || construirCoachLeadSummary(leadDoc);
    leadDoc.updatedAt = now;
    await leadDoc.save();

    try {
      await sincronizarCoachLeadInboxACrmRecord(userDoc, leadDoc);
    } catch (error) {
      console.error("Error sincronizando lead del Coach al CRM:", error.message);
    }

    const cleanedLead = limpiarCoachInboxLead(leadDoc.toObject());
    const [delivery, highLevel] = await Promise.all([
      sendDestination
        ? programarEnvioCoachLeadADestino(userDoc, profileDoc, cleanedLead)
        : Promise.resolve({
            attempted: false,
            queued: false,
            destination: limpiarCoachLeadDestination(profileDoc)
          }),
      programarSincronizacionCoachLeadAHighLevel(userDoc, profileDoc, leadDoc.toObject())
    ]);

    return {
      leadDoc,
      lead: cleanedLead,
      duplicate: true,
      delivery,
      highLevel
    };
  }

  leadDoc = await CoachLeadInbox.create({
    ownerUserId: ownership.ownerUserId,
    ownerEmail: ownership.ownerEmail || "",
    ownerName: ownership.ownerName || "",
    generatedByUserId: ownership.generatedByUserId,
    generatedByName: ownership.generatedByName || "",
    generatedByAccountType: ownership.generatedByAccountType,
    fullName,
    phone,
    email,
    city,
    zipCode,
    interest,
    source,
    notes,
    consentGiven,
    status: nextStatus || "nuevo",
    nextAction,
    nextActionAt,
    summary: summaryOverride || construirCoachLeadSummary({ interest, city, zipCode, source, nextAction, notes }),
    lastStatusChangeAt: now,
    updatedAt: now
  });

  try {
    await sincronizarCoachLeadInboxACrmRecord(userDoc, leadDoc);
  } catch (error) {
    console.error("Error sincronizando lead nuevo del Coach al CRM:", error.message);
  }

  const cleanedLead = limpiarCoachInboxLead(leadDoc.toObject());
  const [delivery, highLevel] = await Promise.all([
    sendDestination
      ? programarEnvioCoachLeadADestino(userDoc, profileDoc, cleanedLead)
      : Promise.resolve({
          attempted: false,
          queued: false,
          destination: limpiarCoachLeadDestination(profileDoc)
        }),
    programarSincronizacionCoachLeadAHighLevel(userDoc, profileDoc, leadDoc.toObject())
  ]);

  return {
    leadDoc,
    lead: cleanedLead,
    duplicate: false,
    delivery,
    highLevel
  };
}

function construirCoachLeadSummary(leadDoc = null) {
  if (!leadDoc) {
    return "";
  }

  const parts = [];

  if (leadDoc.interest) {
    parts.push(`Interes principal: ${truncarTextoPrompt(leadDoc.interest, 80)}.`);
  }

  if (leadDoc.city || leadDoc.zipCode) {
    const zone = [leadDoc.city || "", leadDoc.zipCode ? `ZIP ${leadDoc.zipCode}` : ""].filter(Boolean).join(" · ");
    if (zone) {
      parts.push(`Zona: ${zone}.`);
    }
  }

  if (leadDoc.source) {
    parts.push(`Fuente: ${String(leadDoc.source).replace(/_/g, " ")}.`);
  }

  if (leadDoc.nextAction) {
    parts.push(`Siguiente paso: ${String(leadDoc.nextAction).replace(/_/g, " ")}.`);
  }

  if (leadDoc.notes) {
    parts.push(`Notas: ${truncarTextoPrompt(leadDoc.notes, 120)}.`);
  }

  const highLevelSummary = cleanText(leadDoc.highLevelResultSummary || "").slice(0, 180);

  if (highLevelSummary) {
    parts.push(`HighLevel: ${highLevelSummary}.`);
  }

  return parts.join(" ").trim();
}

async function guardarMensajeCoachLeadCanal(leadDoc = null, options = {}) {
  if (!leadDoc?._id) {
    return null;
  }

  const role = options.role === "assistant" ? "assistant" : "user";
  const content = cleanText(options.content || "");

  if (!content) {
    return null;
  }

  try {
    return await Message.create({
      visitorId: `coach-lead-${String(leadDoc._id)}`,
      sessionId: `coach-lead-sms-${normalizePhone(leadDoc.phone || "") || String(leadDoc._id)}`,
      leadId: leadDoc._id,
      coachOwnerUserId: leadDoc.ownerUserId || null,
      generatedByUserId: leadDoc.generatedByUserId || null,
      generatedByAccountType: leadDoc.generatedByAccountType || "",
      role,
      content,
      intent: options.intent || (role === "assistant" ? "sms_saliente" : "sms_entrante"),
      detectedTopics: ["sms"],
      createdAt: new Date()
    });
  } catch (error) {
    console.error("Error guardando mensaje SMS del lead del Coach:", error.message);
    return null;
  }
}

async function registrarActividadSmsCoachLead(leadDoc = null, options = {}) {
  if (!leadDoc?._id) {
    return null;
  }

  const direction = options.direction === "entrante" ? "entrante" : "saliente";
  const message = cleanText(options.message || "");
  const sid = cleanText(options.sid || "");
  const now = new Date();

  if (message) {
    const prefix = direction === "entrante" ? "SMS entrante" : "SMS enviado";
    const sidCopy = sid ? ` SID ${sid}.` : "";
    const note = `${prefix} ${now.toLocaleString("es-US")}: ${message}.${sidCopy}`.trim();
    leadDoc.notes = leadDoc.notes ? `${leadDoc.notes}\n\n${note}` : note;
  }

  if (leadDoc.status === "nuevo") {
    leadDoc.status = "contactado";
    leadDoc.lastStatusChangeAt = now;
  }

  leadDoc.lastContactAt = now;
  leadDoc.updatedAt = now;
  leadDoc.summary = construirCoachLeadSummary(leadDoc);
  await leadDoc.save();

  try {
    await sincronizarCoachLeadInboxACrmDesdeLead(leadDoc, options.userDoc || null);
  } catch (error) {
    console.error("Error sincronizando actividad SMS al CRM:", error.message);
  }

  await guardarMensajeCoachLeadCanal(leadDoc, {
    role: direction === "saliente" ? "assistant" : "user",
    content: message,
    intent:
      cleanText(options.intent || "") ||
      (direction === "saliente" ? "sms_saliente" : "sms_entrante")
  });

  return leadDoc;
}

async function resolverCoachChefContextPorLead(leadDoc = null) {
  if (!leadDoc) {
    return null;
  }

  const candidateUserIds = [];

  if (leadDoc.generatedByUserId) {
    candidateUserIds.push(String(leadDoc.generatedByUserId));
  }

  if (leadDoc.ownerUserId && String(leadDoc.ownerUserId) !== String(leadDoc.generatedByUserId || "")) {
    candidateUserIds.push(String(leadDoc.ownerUserId));
  }

  for (const userId of candidateUserIds) {
    const profileDoc = await CoachDistributorProfile.findOne({
      userId,
      chefEnabled: { $ne: false }
    });

    if (!profileDoc?.userId) {
      continue;
    }

    let userDoc = await CoachUser.findById(profileDoc.userId);

    if (!userDoc) {
      continue;
    }

    userDoc = await asegurarCoachUserBase(userDoc);

    if (!(await coachTieneAccesoOperativo(userDoc))) {
      continue;
    }

    const accountType = normalizarCoachAccountType(userDoc.accountType || "owner");
    const seatStatus = normalizarCoachSeatStatus(userDoc.seatStatus || "active");

    if (accountType === "seat" && seatStatus !== "active") {
      continue;
    }

    const ownerProfileDoc = await obtenerCoachOwnerProfile(userDoc);
    const ownership = await construirCoachOwnershipSnapshot(userDoc);

    return {
      slug: profileDoc.chefSlug || "",
      userDoc,
      profileDoc,
      ownerProfileDoc: ownerProfileDoc || profileDoc,
      ownership
    };
  }

  return null;
}

async function adoptarLeadPublicoEnCoachContext(leadDoc = null, coachChefContext = null) {
  if (!leadDoc?._id || !coachChefContext?.ownership?.ownerUserId) {
    return leadDoc;
  }

  const trackingFields = construirCoachChefTrackingFields(coachChefContext);
  let dirty = false;

  for (const [field, value] of Object.entries(trackingFields)) {
    const normalizedCurrent =
      field.endsWith("UserId") || field === "coachOwnerUserId" ? String(leadDoc?.[field] || "") : leadDoc?.[field] || "";
    const normalizedNext =
      field.endsWith("UserId") || field === "coachOwnerUserId" ? String(value || "") : value || "";

    if (normalizedNext && normalizedCurrent !== normalizedNext) {
      leadDoc[field] = value;
      dirty = true;
    }
  }

  if (!dirty) {
    return leadDoc;
  }

  leadDoc.updatedAt = new Date();
  leadDoc.lastInteractionAt = new Date();
  await leadDoc.save();
  return leadDoc;
}

function limpiarCoachInboxLead(leadDoc = null) {
  if (!leadDoc) {
    return null;
  }

  return {
    id: String(leadDoc._id),
    ownerUserId: leadDoc.ownerUserId ? String(leadDoc.ownerUserId) : "",
    ownerEmail: leadDoc.ownerEmail || "",
    ownerName: leadDoc.ownerName || "",
    generatedByUserId: leadDoc.generatedByUserId ? String(leadDoc.generatedByUserId) : "",
    generatedByName: leadDoc.generatedByName || "",
    generatedByAccountType: normalizarCoachAccountType(leadDoc.generatedByAccountType || "owner"),
    fullName: leadDoc.fullName || "",
    phone: leadDoc.phone || "",
    email: leadDoc.email || "",
    city: leadDoc.city || "",
    zipCode: leadDoc.zipCode || "",
    interest: leadDoc.interest || "",
    source: leadDoc.source || "captura_manual",
    notes: leadDoc.notes || "",
    consentGiven: Boolean(leadDoc.consentGiven),
    status: normalizarCoachLeadStatus(leadDoc.status),
    nextAction: normalizarCoachLeadNextAction(leadDoc.nextAction),
    nextActionAt: leadDoc.nextActionAt || null,
    summary: leadDoc.summary || "",
    highLevel: {
      contactId: leadDoc.highLevelContactId || "",
      opportunityId: leadDoc.highLevelOpportunityId || "",
      locationId: leadDoc.highLevelLocationId || "",
      lastSyncAt: leadDoc.highLevelLastSyncAt || null,
      lastSyncStatus: leadDoc.highLevelLastSyncStatus || "",
      lastSyncError: leadDoc.highLevelLastSyncError || "",
      lastWebhookAt: leadDoc.highLevelLastWebhookAt || null,
      opportunityStatus: leadDoc.highLevelOpportunityStatus || "",
      opportunityStageId: leadDoc.highLevelOpportunityStageId || "",
      opportunityValue: Number(leadDoc.highLevelOpportunityValue || 0) || 0,
      campaignId: leadDoc.highLevelCampaignId || "",
      campaignName: leadDoc.highLevelCampaignName || "",
      campaignStatus: leadDoc.highLevelCampaignStatus || "",
      resultSummary: leadDoc.highLevelResultSummary || ""
    },
    lastContactAt: leadDoc.lastContactAt || null,
    lastStatusChangeAt: leadDoc.lastStatusChangeAt || null,
    updatedAt: leadDoc.updatedAt || null,
    createdAt: leadDoc.createdAt || null
  };
}

function construirNotaHighLevelLead(prefix = "", detail = "") {
  const safePrefix = cleanText(prefix || "").trim();
  const safeDetail = cleanText(detail || "").trim();
  return [safePrefix, safeDetail].filter(Boolean).join(": ").trim();
}

function mapearStatusLeadDesdeHighLevel(eventType = "", opportunityStatus = "") {
  const safeType = String(eventType || "").trim();
  const safeStatus = String(opportunityStatus || "").trim().toLowerCase();

  if (safeStatus === "won") {
    return "cliente";
  }

  if (["lost", "abandoned", "canceled", "cancelled"].includes(safeStatus)) {
    return "archivado";
  }

  if (safeType === "OpportunityStageUpdate") {
    return "agendado";
  }

  if (safeStatus === "open" || safeType === "OpportunityCreate") {
    return "contactado";
  }

  return "";
}

async function sincronizarCoachLeadAHighLevel(userDoc = null, profileDoc = null, lead = null) {
  if (!highLevelSyncAplicaALead(lead)) {
    return {
      attempted: false,
      synced: false,
      reason: "not_applicable"
    };
  }

  const leadId = typeof lead?._id === "object" ? String(lead._id) : String(lead?.id || lead?._id || "").trim();

  if (!leadId || !mongoose.Types.ObjectId.isValid(leadId)) {
    return {
      attempted: false,
      synced: false,
      reason: "invalid_lead"
    };
  }

  const leadDoc = await CoachLeadInbox.findById(leadId);

  if (!leadDoc) {
    return {
      attempted: false,
      synced: false,
      reason: "not_found"
    };
  }

  const contactPayload = construirPayloadHighLevelContacto(leadDoc.toObject());

  if (!contactPayload || (!contactPayload.phone && !contactPayload.email)) {
    leadDoc.highLevelLastSyncAt = new Date();
    leadDoc.highLevelLastSyncStatus = "skipped";
    leadDoc.highLevelLastSyncError = "Falta telefono o correo para sincronizar con HighLevel.";
    leadDoc.summary = construirCoachLeadSummary(leadDoc);
    await leadDoc.save();
    try {
      await sincronizarCoachLeadInboxACrmDesdeLead(leadDoc, userDoc);
    } catch (error) {
      console.error("Error reflejando sync omitida de HighLevel en CRM:", error.message);
    }
    return {
      attempted: true,
      synced: false,
      error: leadDoc.highLevelLastSyncError
    };
  }

  let contactResult = null;
  let contactId = cleanText(leadDoc.highLevelContactId || "");

  if (contactId) {
    contactResult = await highLevelFetchJson(`/contacts/${contactId}`, {
      method: "PUT",
      body: contactPayload
    });
  } else {
    contactResult = await highLevelFetchJson("/contacts/", {
      method: "POST",
      body: contactPayload
    });
  }

  if (!contactResult?.ok) {
    leadDoc.highLevelLastSyncAt = new Date();
    leadDoc.highLevelLastSyncStatus = "error";
    leadDoc.highLevelLastSyncError = contactResult?.error || "No pude guardar el contacto en HighLevel.";
    leadDoc.highLevelResultSummary = "No pude sincronizar el contacto.";
    leadDoc.summary = construirCoachLeadSummary(leadDoc);
    await leadDoc.save();
    try {
      await sincronizarCoachLeadInboxACrmDesdeLead(leadDoc, userDoc);
    } catch (error) {
      console.error("Error reflejando error de HighLevel en CRM:", error.message);
    }

    return {
      attempted: true,
      synced: false,
      error: leadDoc.highLevelLastSyncError
    };
  }

  contactId =
    contactId ||
    extraerPrimerValorHighLevel(contactResult.data, ["contact.id", "contact._id", "id", "_id", "contactId"]);

  if (!contactId) {
    leadDoc.highLevelLastSyncAt = new Date();
    leadDoc.highLevelLastSyncStatus = "error";
    leadDoc.highLevelLastSyncError = "HighLevel no regreso el id del contacto.";
    leadDoc.highLevelResultSummary = "El contacto entro sin id confirmado.";
    leadDoc.summary = construirCoachLeadSummary(leadDoc);
    await leadDoc.save();
    try {
      await sincronizarCoachLeadInboxACrmDesdeLead(leadDoc, userDoc);
    } catch (error) {
      console.error("Error reflejando contacto sin id de HighLevel en CRM:", error.message);
    }

    return {
      attempted: true,
      synced: false,
      error: leadDoc.highLevelLastSyncError
    };
  }

  leadDoc.highLevelContactId = contactId;
  leadDoc.highLevelLocationId = HIGHLEVEL_LOCATION_ID;
  leadDoc.highLevelLastSyncAt = new Date();
  leadDoc.highLevelLastSyncStatus = "contact_synced";
  leadDoc.highLevelLastSyncError = "";

  let opportunityId = cleanText(leadDoc.highLevelOpportunityId || "");
  let opportunityResult = null;

  if (!opportunityId && highLevelOpportunitySyncEstaConfigurado()) {
    const opportunityPayload = construirPayloadHighLevelOpportunity(leadDoc.toObject(), contactId);
    opportunityResult = await highLevelFetchJson("/opportunities", {
      method: "POST",
      body: opportunityPayload
    });

    if (opportunityResult?.ok) {
      opportunityId = extraerPrimerValorHighLevel(opportunityResult.data, [
        "opportunity.id",
        "opportunity._id",
        "id",
        "_id",
        "opportunityId"
      ]);
    } else {
      leadDoc.highLevelLastSyncStatus = "partial";
      leadDoc.highLevelLastSyncError = opportunityResult?.error || "No pude crear la oportunidad en HighLevel.";
    }
  }

  if (opportunityId) {
    leadDoc.highLevelOpportunityId = opportunityId;
    leadDoc.highLevelOpportunityStatus = leadDoc.highLevelOpportunityStatus || "open";
    leadDoc.highLevelOpportunityStageId =
      leadDoc.highLevelOpportunityStageId || HIGHLEVEL_PIPELINE_STAGE_ID || "";
  }

  const resultParts = ["Contacto sincronizado en HighLevel"];

  if (leadDoc.highLevelOpportunityId) {
    resultParts.push("oportunidad abierta");
  } else if (highLevelOpportunitySyncEstaConfigurado()) {
    resultParts.push("sin oportunidad confirmada todavia");
  }

  leadDoc.highLevelResultSummary = resultParts.join(", ");
  leadDoc.summary = construirCoachLeadSummary(leadDoc);
  await leadDoc.save();
  try {
    await sincronizarCoachLeadInboxACrmDesdeLead(leadDoc, userDoc);
  } catch (error) {
    console.error("Error reflejando sync de HighLevel en CRM:", error.message);
  }

  return {
    attempted: true,
    synced: true,
    contactId,
    opportunityId: leadDoc.highLevelOpportunityId || "",
    partial: leadDoc.highLevelLastSyncStatus === "partial",
    error: leadDoc.highLevelLastSyncError || ""
  };
}

async function programarSincronizacionCoachLeadAHighLevel(userDoc = null, profileDoc = null, lead = null) {
  if (!highLevelSyncAplicaALead(lead)) {
    return {
      attempted: false,
      queued: false
    };
  }

  try {
    const leadId =
      typeof lead?._id === "object" ? String(lead._id) : String(lead?.id || lead?._id || "").trim();
    const jobDoc = await encolarCoachAsyncJob({
      jobType: "highlevel_sync",
      userDoc,
      payload: {
        leadId
      },
      maxAttempts: 4
    });

    return {
      attempted: true,
      queued: Boolean(jobDoc?._id),
      queueId: jobDoc?._id ? String(jobDoc._id) : ""
    };
  } catch (error) {
    console.error("Error encolando sync de HighLevel del Coach:", error.message);
    return {
      attempted: true,
      queued: false,
      error: error.message
    };
  }
}

async function aplicarEventoHighLevelALead(payload = {}) {
  const eventType = cleanText(payload?.type || "");
  const opportunityId =
    extraerPrimerValorHighLevel(payload, ["id", "opportunityId"]) && /^Opportunity/i.test(eventType)
      ? extraerPrimerValorHighLevel(payload, ["id", "opportunityId"])
      : "";
  const contactId = extraerPrimerValorHighLevel(payload, ["contactId", "contact.id"]);
  const phone = normalizePhone(payload?.phone || "");
  const email = normalizarEmail(payload?.email || "");
  const lookup = [];

  if (opportunityId) {
    lookup.push({ highLevelOpportunityId: opportunityId });
  }

  if (contactId) {
    lookup.push({ highLevelContactId: contactId });
  }

  if (phone) {
    lookup.push({ phone });
  }

  if (email) {
    lookup.push({ email });
  }

  if (!lookup.length) {
    return null;
  }

  const leadDoc = await CoachLeadInbox.findOne({ $or: lookup }).sort({ updatedAt: -1, createdAt: -1 });

  if (!leadDoc) {
    return null;
  }

  const noteParts = [];
  const nextLeadStatus = mapearStatusLeadDesdeHighLevel(eventType, payload?.status || "");

  if (contactId && !leadDoc.highLevelContactId) {
    leadDoc.highLevelContactId = contactId;
  }

  if (opportunityId && !leadDoc.highLevelOpportunityId) {
    leadDoc.highLevelOpportunityId = opportunityId;
  }

  if (payload?.locationId) {
    leadDoc.highLevelLocationId = cleanText(payload.locationId);
  }

  if (payload?.status !== undefined) {
    const safeStatus = cleanText(payload.status);
    if (/^CampaignStatusUpdate$/i.test(eventType)) {
      leadDoc.highLevelCampaignStatus = safeStatus;
    } else {
      leadDoc.highLevelOpportunityStatus = safeStatus;
    }
    noteParts.push(`status ${safeStatus}`);
  }

  if (payload?.pipelineStageId) {
    leadDoc.highLevelOpportunityStageId = cleanText(payload.pipelineStageId);
    noteParts.push(`etapa ${leadDoc.highLevelOpportunityStageId}`);
  }

  if (Number.isFinite(Number(payload?.monetaryValue))) {
    leadDoc.highLevelOpportunityValue = Number(payload.monetaryValue) || 0;
    noteParts.push(`valor $${leadDoc.highLevelOpportunityValue}`);
  }

  if (/^CampaignStatusUpdate$/i.test(eventType)) {
    leadDoc.highLevelCampaignId = extraerPrimerValorHighLevel(payload, ["campaignId", "id"]);
    leadDoc.highLevelCampaignName = extraerPrimerValorHighLevel(payload, ["name", "campaignName"]);
  }

  if (nextLeadStatus && leadDoc.status !== nextLeadStatus) {
    leadDoc.status = nextLeadStatus;
    leadDoc.lastStatusChangeAt = new Date();
  }

  leadDoc.highLevelLastWebhookAt = new Date();
  leadDoc.highLevelLastSyncStatus = "webhook";
  leadDoc.highLevelLastSyncError = "";

  const summaryParts = [];

  if (/^CampaignStatusUpdate$/i.test(eventType)) {
    summaryParts.push("Campana actualizada");
    if (leadDoc.highLevelCampaignName) {
      summaryParts.push(leadDoc.highLevelCampaignName);
    }
    if (leadDoc.highLevelCampaignStatus) {
      summaryParts.push(leadDoc.highLevelCampaignStatus);
    }
  } else {
    summaryParts.push(`Evento ${eventType || "HighLevel"}`);
    if (leadDoc.highLevelOpportunityStatus) {
      summaryParts.push(`status ${leadDoc.highLevelOpportunityStatus}`);
    }
    if (leadDoc.highLevelOpportunityValue > 0) {
      summaryParts.push(`valor $${leadDoc.highLevelOpportunityValue}`);
    }
  }

  leadDoc.highLevelResultSummary = summaryParts.join(" · ").slice(0, 220);

  const note = construirNotaHighLevelLead(
    `HighLevel ${eventType || "evento"}`,
    noteParts.join(" · ") || leadDoc.highLevelResultSummary
  );

  if (note) {
    leadDoc.notes = leadDoc.notes ? `${leadDoc.notes}\n\n${note}` : note;
  }

  leadDoc.updatedAt = new Date();
  leadDoc.summary = construirCoachLeadSummary(leadDoc);
  await leadDoc.save();
  try {
    await sincronizarCoachLeadInboxACrmDesdeLead(leadDoc);
  } catch (error) {
    console.error("Error reflejando webhook de HighLevel en CRM:", error.message);
  }

  return leadDoc;
}

function construirCoachChefCampaignMessage(leadDoc = null) {
  const firstName = seleccionarNombreConfiable(String(leadDoc?.fullName || "").split(/\s+/).slice(0, 2).join(" ")) || "hola";
  return `Hola ${firstName}, soy Agustin 2.0 Chef. Me compartieron tu contacto porque tal vez te puede servir ayuda gratis con recetas, cocina saludable y tips para sacarle mas provecho a tu cocina. Si quieres, te puedo ayudar por aqui o apartarte una llamada corta. Responde STOP si prefieres no recibir mensajes.`;
}

function renderCoachChefCampaignMessage(template = "", leadDoc = null) {
  const fallback = construirCoachChefCampaignMessage(leadDoc);
  const firstName =
    seleccionarNombreConfiable(String(leadDoc?.fullName || "").split(/\s+/).slice(0, 2).join(" ")) || "amiga";
  const safeTemplate = cleanText(template || "").slice(0, 600);

  if (!safeTemplate) {
    return fallback;
  }

  return safeTemplate
    .replace(/\{\{\s*nombre\s*\}\}/gi, firstName)
    .replace(/\{\{\s*primer_nombre\s*\}\}/gi, firstName);
}

async function obtenerCoachChefCampaignSnapshot(userDoc = null) {
  const query = {
    ...construirCoachWorkspaceQuery(userDoc),
    source: "contactos_compartidos",
    phone: { $nin: ["", null] }
  };
  const leadDocs = await CoachLeadInbox.find(query).sort({ createdAt: -1 }).lean();
  const leadIds = leadDocs.map(item => item?._id).filter(Boolean);
  const sentLeadIds = leadIds.length
    ? await Message.distinct("leadId", {
        leadId: { $in: leadIds },
        role: "assistant",
        detectedTopics: "sms"
      })
    : [];
  const sentSet = new Set(sentLeadIds.map(item => String(item)));
  const eligibleLeads = leadDocs.filter(item => item?._id && !sentSet.has(String(item._id)));

  return {
    totalImported: leadDocs.length,
    alreadyMessaged: sentSet.size,
    eligibleCount: eligibleLeads.length,
    eligibleLeads,
    sample: eligibleLeads.slice(0, 5).map(limpiarCoachInboxLead).filter(Boolean)
  };
}

function construirCoachLeadInboxSummary(leads = []) {
  const items = Array.isArray(leads) ? leads : [];
  const summary = {
    total: items.length,
    nuevo: 0,
    contactado: 0,
    agendado: 0,
    cliente: 0,
    archivado: 0
  };

  items.forEach(item => {
    const status = normalizarCoachLeadStatus(item?.status);
    summary[status] = (summary[status] || 0) + 1;
  });

  return summary;
}

function limpiarCoachHealthText(value = "", maxLength = 180) {
  return String(value || "").replace(/\s+/g, " ").trim().slice(0, maxLength);
}

function limpiarCoachHealthYesNo(value = "") {
  const limpio = limpiarCoachHealthText(value, 12).toLowerCase();

  if (limpio === "si") {
    return "Si";
  }

  if (limpio === "no") {
    return "No";
  }

  return "";
}

function limpiarCoachHealthNumber(value = "", min = 0, max = 999) {
  const parsed = Number.parseInt(String(value || "").trim(), 10);

  if (!Number.isFinite(parsed)) {
    return null;
  }

  return Math.min(max, Math.max(min, parsed));
}

function limpiarCoachHealthList(values = [], { maxItems = 8, maxLength = 80 } = {}) {
  const items = Array.isArray(values) ? values : [values];
  const unique = [];

  for (const item of items) {
    const clean = limpiarCoachHealthText(item, maxLength);

    if (!clean) {
      continue;
    }

    const exists = unique.some(saved => saved.toLowerCase() === clean.toLowerCase());

    if (exists) {
      continue;
    }

    unique.push(clean);

    if (unique.length >= maxItems) {
      break;
    }
  }

  return unique;
}

function construirCoachHealthSurveySummary(surveyDoc = null) {
  if (!surveyDoc) {
    return "";
  }

  const parts = [];

  if (surveyDoc.familyPriority) {
    parts.push(`Lo mas importante: ${surveyDoc.familyPriority}.`);
  }

  if (surveyDoc.qualityReason) {
    parts.push(`Compra calidad por: ${truncarTextoPrompt(surveyDoc.qualityReason, 70)}.`);
  }

  if (Number.isFinite(surveyDoc.productLikingScore) && surveyDoc.productLikingScore > 0) {
    parts.push(`Le gustaron los productos: ${surveyDoc.productLikingScore}/10.`);
  }

  if (Number.isFinite(surveyDoc.cooksForCount) && surveyDoc.cooksForCount > 0) {
    parts.push(`Cocina para ${surveyDoc.cooksForCount} persona(s).`);
  }

  if (Array.isArray(surveyDoc.cookingMaterials) && surveyDoc.cookingMaterials.length) {
    parts.push(`Hoy cocina con ${surveyDoc.cookingMaterials.join(", ")}.`);
  }

  if (Array.isArray(surveyDoc.familyConditions) && surveyDoc.familyConditions.length) {
    parts.push(`Dolencias en casa: ${surveyDoc.familyConditions.join(", ")}.`);
  }

  if (surveyDoc.tapWaterConcern) {
    parts.push(`Contaminantes en el agua: ${surveyDoc.tapWaterConcern}.`);
  }

  if (surveyDoc.creditImproveInterest) {
    parts.push(`Quiere mejorar credito: ${surveyDoc.creditImproveInterest}.`);
  }

  if (Array.isArray(surveyDoc.topProducts) && surveyDoc.topProducts.length) {
    parts.push(`Productos de mas uso: ${surveyDoc.topProducts.join(", ")}.`);
  }

  return parts.join(" ").trim();
}

function extraerMontoEncuestaCoach(value = "") {
  const match = String(value || "")
    .replace(/,/g, "")
    .match(/(\d+(?:\.\d+)?)/);
  const amount = match ? Number(match[1]) : NaN;
  return Number.isFinite(amount) ? amount : null;
}

function construirCoachHealthSurveySalesAnalysis(surveyDoc = null) {
  if (!surveyDoc) {
    return {
      recommendedProduct: "",
      recommendedClose: "",
      objectionAnchor: "",
      usefulAngle: "",
      coachReply: ""
    };
  }

  const topProducts = Array.isArray(surveyDoc.topProducts)
    ? surveyDoc.topProducts.map(item => String(item || "").trim()).filter(Boolean)
    : [];
  const cookingMaterials = Array.isArray(surveyDoc.cookingMaterials)
    ? surveyDoc.cookingMaterials.map(item => String(item || "").trim()).filter(Boolean)
    : [];
  const familyConditions = Array.isArray(surveyDoc.familyConditions)
    ? surveyDoc.familyConditions.map(item => String(item || "").trim()).filter(Boolean)
    : [];
  const weeklyBudgetAmount = extraerMontoEncuestaCoach(surveyDoc.weeklyBudget);
  const monthlyBudgetAmount = extraerMontoEncuestaCoach(surveyDoc.monthlyBudget);
  const priority = String(surveyDoc.familyPriority || "").trim();
  const likesProducts = Number.isFinite(surveyDoc.productLikingScore) && surveyDoc.productLikingScore >= 7;
  const waterConcern =
    surveyDoc.tapWaterConcern === "Si" ||
    /llave/i.test(String(surveyDoc.drinkingWaterType || "")) ||
    /llave/i.test(String(surveyDoc.cookingWaterType || ""));

  let recommendedProduct = topProducts[0] || "";

  if (!recommendedProduct) {
    if (waterConcern) {
      recommendedProduct = "sistema de agua";
    } else if (surveyDoc.likesNaturalJuices === "Si") {
      recommendedProduct = "extractor";
    } else if (cookingMaterials.some(item => /tefelon|aluminio/i.test(item))) {
      recommendedProduct = "olla o sarten de uso diario";
    } else {
      recommendedProduct = "producto de mas uso";
    }
  }

  let usefulAngle = "uso diario";

  if (priority === "Salud" || familyConditions.length) {
    usefulAngle = "salud familiar";
  } else if (waterConcern) {
    usefulAngle = "agua y contaminantes";
  } else if (priority === "Dinero" || weeklyBudgetAmount || monthlyBudgetAmount) {
    usefulAngle = "presupuesto y ahorro";
  } else if (priority === "Tiempo") {
    usefulAngle = "tiempo y practicidad";
  }

  let recommendedClose = "cierre por uso diario";

  if (priority === "Salud" || familyConditions.length) {
    recommendedClose = "cierre por salud";
  } else if (waterConcern) {
    recommendedClose = "cierre por agua";
  } else if (weeklyBudgetAmount || monthlyBudgetAmount) {
    recommendedClose = "cierre por presupuesto";
  } else if (surveyDoc.creditImproveInterest === "Si") {
    recommendedClose = "cierre por credito y plan";
  } else if (likesProducts) {
    recommendedClose = "cierre por gusto y uso";
  }

  let objectionAnchor = "";

  if (weeklyBudgetAmount) {
    objectionAnchor = `Dijo que ${surveyDoc.weeklyBudget} por semana no le afecta tanto.`;
  } else if (monthlyBudgetAmount) {
    objectionAnchor = `Dijo que ${surveyDoc.monthlyBudget} al mes lo ve posible.`;
  } else if (surveyDoc.creditImproveInterest === "Si") {
    objectionAnchor = "Dijo que le gustaria mejorar o establecer su credito.";
  } else if (likesProducts) {
    objectionAnchor = `Califico los productos con ${surveyDoc.productLikingScore}/10.`;
  } else if (priority) {
    objectionAnchor = `Dijo que lo mas importante para su familia es ${priority.toLowerCase()}.`;
  }

  const replyParts = [
    `Cierre recomendado: ${recommendedClose}.`,
    `Producto recomendado: ${recommendedProduct}.`
  ];

  if (objectionAnchor) {
    replyParts.push(`Ancla util: ${objectionAnchor}`);
  }

  if (usefulAngle) {
    replyParts.push(`Habla primero desde ${usefulAngle}.`);
  }

  return {
    recommendedProduct,
    recommendedClose,
    objectionAnchor,
    usefulAngle,
    coachReply: replyParts.join(" ")
  };
}

function construirPromptEncuestaSaludActiva(context = null) {
  if (!context?.id) {
    return "";
  }

  return `
ENCUESTA DE SALUD ACTIVA EN ESTA SESION:
- casa_activa: si
- survey_id: ${context.id}
- nombre: ${context.fullName || "sin nombre"}
- telefono: ${context.phone || "sin telefono"}
- resumen: ${context.summary || "sin resumen"}
- prioridad_familiar: ${context.familyPriority || "sin dato"}
- presupuesto_semanal: ${context.weeklyBudget || "sin dato"}
- presupuesto_mensual: ${context.monthlyBudget || "sin dato"}
- interes_en_mejorar_credito: ${context.creditImproveInterest || "sin dato"}
- top_productos: ${Array.isArray(context.topProducts) && context.topProducts.length ? context.topProducts.join(", ") : "sin dato"}
- producto_recomendado: ${context.salesAnalysis?.recommendedProduct || "sin producto"}
- cierre_recomendado: ${context.salesAnalysis?.recommendedClose || "sin cierre"}
- ancla_objecion: ${context.salesAnalysis?.objectionAnchor || "sin ancla"}
- angulo_util: ${context.salesAnalysis?.usefulAngle || "sin angulo"}

INSTRUCCION:
- usa esta encuesta solo para esta casa y esta sesion
- si el distribuidor reporta objeciones como "esta caro", "lo voy a pensar" o "ahorita no puedo", responde usando primero lo que el cliente ya dijo en esta encuesta
- no inventes respuestas que la encuesta no dijo
- si la encuesta ya dio un presupuesto o deseo de mejorar credito, usalo como ancla con tacto
`;
}

function limpiarCoachHealthSurvey(surveyDoc = null) {
  if (!surveyDoc) {
    return null;
  }

  const salesAnalysis = construirCoachHealthSurveySalesAnalysis(surveyDoc);

  return {
    id: String(surveyDoc._id),
    linkedCrmRecordId: surveyDoc.linkedCrmRecordId ? String(surveyDoc.linkedCrmRecordId) : "",
    ownerUserId: surveyDoc.ownerUserId ? String(surveyDoc.ownerUserId) : "",
    ownerEmail: surveyDoc.ownerEmail || "",
    ownerName: surveyDoc.ownerName || "",
    generatedByUserId: surveyDoc.generatedByUserId ? String(surveyDoc.generatedByUserId) : "",
    generatedByName: surveyDoc.generatedByName || "",
    generatedByAccountType: normalizarCoachAccountType(surveyDoc.generatedByAccountType || "owner"),
    fullName: surveyDoc.fullName || "",
    phone: surveyDoc.phone || "",
    secondName: surveyDoc.secondName || "",
    workingStatus: surveyDoc.workingStatus || "",
    heardRoyal: surveyDoc.heardRoyal || "",
    familyPriority: surveyDoc.familyPriority || "",
    qualityReason: surveyDoc.qualityReason || "",
    productLikingScore: Number.isFinite(surveyDoc.productLikingScore) ? surveyDoc.productLikingScore : null,
    cooksForCount: Number.isFinite(surveyDoc.cooksForCount) ? surveyDoc.cooksForCount : null,
    foodSpendWeekly: surveyDoc.foodSpendWeekly || "",
    mealPrepTime: surveyDoc.mealPrepTime || "",
    cookingMaterials: Array.isArray(surveyDoc.cookingMaterials) ? surveyDoc.cookingMaterials : [],
    familyConditions: Array.isArray(surveyDoc.familyConditions) ? surveyDoc.familyConditions : [],
    lowFatHealthy: surveyDoc.lowFatHealthy || "",
    lowFatHealthyReason: surveyDoc.lowFatHealthyReason || "",
    cookwareAffects: surveyDoc.cookwareAffects || "",
    cookwareAffectsReason: surveyDoc.cookwareAffectsReason || "",
    qualityInterest: surveyDoc.qualityInterest || "",
    qualityInterestReason: surveyDoc.qualityInterestReason || "",
    drinkingWaterType: surveyDoc.drinkingWaterType || "",
    cookingWaterType: surveyDoc.cookingWaterType || "",
    tapWaterConcern: surveyDoc.tapWaterConcern || "",
    waterSpendWeekly: surveyDoc.waterSpendWeekly || "",
    likesNaturalJuices: surveyDoc.likesNaturalJuices || "",
    juiceFrequency: surveyDoc.juiceFrequency || "",
    creditProblems: surveyDoc.creditProblems || "",
    creditImproveInterest: surveyDoc.creditImproveInterest || "",
    familyHealthInvestment: surveyDoc.familyHealthInvestment || "",
    weeklyBudget: surveyDoc.weeklyBudget || "",
    monthlyBudget: surveyDoc.monthlyBudget || "",
    topProducts: Array.isArray(surveyDoc.topProducts) ? surveyDoc.topProducts : [],
    summary: surveyDoc.summary || "",
    salesAnalysis,
    updatedAt: surveyDoc.updatedAt || null,
    createdAt: surveyDoc.createdAt || null
  };
}

function limpiarCoachHealthSurveyActiveContext(context = null) {
  if (!context || typeof context !== "object") {
    return null;
  }

  const id = cleanText(context.id || context._id || "");

  if (!id) {
    return null;
  }

  return {
    id,
    ownerUserId: cleanText(context.ownerUserId || ""),
    fullName: cleanText(context.fullName || ""),
    phone: cleanText(context.phone || ""),
    summary: cleanText(context.summary || ""),
    topProducts: Array.isArray(context.topProducts)
      ? context.topProducts.map(item => cleanText(item)).filter(Boolean).slice(0, 6)
      : [],
    salesAnalysis: {
      recommendedClose: cleanText(context.salesAnalysis?.recommendedClose || ""),
      recommendedProduct: cleanText(context.salesAnalysis?.recommendedProduct || ""),
      objectionAnchor: cleanText(context.salesAnalysis?.objectionAnchor || "")
    }
  };
}

function formatearMonedaCoachUsd(value = 0) {
  const amount = Number(value);

  if (!Number.isFinite(amount) || amount <= 0) {
    return "";
  }

  return `$${amount.toFixed(2)}`;
}

function construirCoachTurboReply({
  question = "",
  activeDemoStage = "",
  activeHealthSurveyContext = null,
  activeProgram414Context = null,
  activeOrderCalcContext = null,
  activeDecisionContext = null
} = {}) {
  const normalized = normalizarTextoBusquedaCoach(question);

  if (!normalized) {
    return "";
  }

  const referral = activeProgram414Context?.referral || null;
  const paymentSummary = activeOrderCalcContext?.summary || null;
  const weekly = formatearMonedaCoachUsd(paymentSummary?.weekly);
  const monthly = formatearMonedaCoachUsd(paymentSummary?.monthly);
  const daily = formatearMonedaCoachUsd(paymentSummary?.daily);
  const productNames = Array.isArray(activeOrderCalcContext?.products)
    ? activeOrderCalcContext.products
        .map(item => cleanText(item?.name || item?.code || ""))
        .filter(Boolean)
        .slice(0, 2)
    : [];
  const productLabel =
    activeHealthSurveyContext?.salesAnalysis?.recommendedProduct ||
    productNames[0] ||
    "el producto";
  const objectionAnchor = cleanText(activeHealthSurveyContext?.salesAnalysis?.objectionAnchor || "");
  const closeLabel = cleanText(activeHealthSurveyContext?.salesAnalysis?.recommendedClose || "");
  const topObjection = cleanText(activeDecisionContext?.summary?.topObjection || "");
  const nextStep = cleanText(activeDecisionContext?.summary?.nextStep || "");
  const isInstantCallLane =
    Boolean(referral?.fullName) ||
    /cita_instantanea/i.test(activeDemoStage) ||
    /cita instantanea|cita instantánea|llamar ahora|marcar ahorita/.test(normalized);

  if (isInstantCallLane) {
    if (/mandame informacion|manda informacion|manda info|texto|mensaje/.test(normalized)) {
      return [
        "No vendas producto por texto en esta llamada.",
        `Di: "Claro, te mando detalles despues, pero ahorita solo quiero apartarte un espacio rapido. ¿Te queda mejor hoy o manana?"`,
        referral?.scripts?.focus ? `Enfoque: ${referral.scripts.focus}.` : ""
      ]
        .filter(Boolean)
        .join(" ");
    }

    if (/mi esposo|mi esposa|mi pareja|consultarlo con mi esposo|consultarlo con mi esposa/.test(normalized)) {
      return [
        "No discutas todo ahorita.",
        `Di: "Perfecto, justo para eso quiero dejarles la cita ya apartada y verla juntos. ¿Que te queda mejor, hoy mas tarde o manana?"`,
        referral?.scripts?.repScript ? `Guion corto: "${referral.scripts.repScript}"` : ""
      ]
        .filter(Boolean)
        .join(" ");
    }

    if (/ahorita no puedo|ahora no puedo|ahorita no puede|estoy ocupad|ocupada|ocupado|despues|luego/.test(normalized)) {
      return [
        "No busques vender. Solo aparta hora.",
        `Di: "Perfecto, no te quito tiempo. Solo dime que te queda mejor, hoy mas tarde o manana."`,
        referral?.scripts?.focus ? `Enfoque: ${referral.scripts.focus}.` : ""
      ]
        .filter(Boolean)
        .join(" ");
    }
  }

  if (/esta caro|muy caro|\bcaro\b/.test(normalized)) {
    return [
      "No pelees el total. Bajalo a uso y a pago.",
      weekly || monthly
        ? `Di: "Entiendo. Lo importante es que ya viste el valor en tu casa. Si te queda en ${
            weekly || monthly
          }${monthly && weekly ? ` (${monthly} al mes)` : ""}, ¿eso si lo puedes acomodar?"`
        : `Di: "Entiendo. Lo importante es si esto si le sirve a tu casa todos los dias. ¿Que parte es la que si te hace sentido?"`,
      objectionAnchor ? `Ancla: ${objectionAnchor}.` : "",
      closeLabel ? `Cierre recomendado: ${closeLabel}.` : ""
    ]
      .filter(Boolean)
      .join(" ");
  }

  if (/lo voy a pensar|voy a pensarlo|pensarlo|lo pensare|lo pensar[eé]/.test(normalized)) {
    return [
      "Aisla primero la objecion real.",
      `Di: "Antes de pensarlo, dime que parte te frena mas, el precio o que quieres consultarlo?"`,
      topObjection ? `Objecion detectada: ${topObjection}.` : "",
      nextStep ? `Siguiente paso: ${nextStep}.` : ""
    ]
      .filter(Boolean)
      .join(" ");
  }

  if (/ahorita no puedo|ahora no puedo|no puedo ahorita|no se puede ahorita/.test(normalized)) {
    return [
      "No discutas todo. Recorta el siguiente paso.",
      weekly || daily
        ? `Di: "Si todo lo demas te gusto, lo unico que necesito saber es si ${
            weekly || daily
          } si lo puedes acomodar sin sentirlo pesado."`
        : `Di: "Si lo unico que te frena es el momento, dime que parte si puedes resolver hoy para avanzarte."`,
      productLabel ? `Regresa al valor diario de ${productLabel}.` : ""
    ]
      .filter(Boolean)
      .join(" ");
  }

  if (/que le digo|como le contesto|como respondo|que respondo/.test(normalized) && /caro|pensar|ahorita no/.test(normalized)) {
    return [
      "Ve directo y corto.",
      weekly
        ? `Usa esto: "Entiendo. Lo importante es que ya viste el valor. Si te queda en ${weekly} por semana, ¿si te funciona?"`
        : `Usa esto: "Entiendo. Antes de dejarlo abierto, dime que parte es la que de verdad te frena."`,
      topObjection ? `Objecion principal que traes: ${topObjection}.` : ""
    ]
      .filter(Boolean)
      .join(" ");
  }

  if (/que producto|cual producto|por donde empiezo|que le enseño|que le recomiendo/.test(normalized)) {
    return [
      productLabel ? `Empieza por ${productLabel}.` : "Empieza por el producto mas util para esa casa.",
      objectionAnchor ? `Conecta con esto: ${objectionAnchor}.` : "",
      closeLabel ? `Y cuando veas interes, remata con ${closeLabel}.` : ""
    ]
      .filter(Boolean)
      .join(" ");
  }

  return "";
}

function construirCoachRecruitmentApplicationSummary(applicationDoc = null) {
  if (!applicationDoc) {
    return "";
  }

  const parts = [];

  if (applicationDoc.workPreference) {
    parts.push(`Busca ${String(applicationDoc.workPreference).toLowerCase()}.`);
  }

  if (applicationDoc.drives) {
    parts.push(`Maneja: ${applicationDoc.drives}.`);
  }

  if (applicationDoc.hasCar) {
    parts.push(`Auto propio: ${applicationDoc.hasCar}.`);
  }

  if (applicationDoc.customerServiceExperience) {
    parts.push(`Atencion al cliente: ${applicationDoc.customerServiceExperience}.`);
  }

  if (applicationDoc.salesExperience) {
    parts.push(`Ventas: ${applicationDoc.salesExperience}.`);
  }

  if (applicationDoc.about) {
    parts.push(`Perfil: ${truncarTextoPrompt(applicationDoc.about, 150)}.`);
  }

  return parts.join(" ").trim();
}

function limpiarCoachRecruitmentApplication(applicationDoc = null) {
  if (!applicationDoc) {
    return null;
  }

  return {
    id: String(applicationDoc._id),
    ownerUserId: applicationDoc.ownerUserId ? String(applicationDoc.ownerUserId) : "",
    ownerEmail: applicationDoc.ownerEmail || "",
    ownerName: applicationDoc.ownerName || "",
    generatedByUserId: applicationDoc.generatedByUserId ? String(applicationDoc.generatedByUserId) : "",
    generatedByName: applicationDoc.generatedByName || "",
    generatedByAccountType: normalizarCoachAccountType(applicationDoc.generatedByAccountType || "owner"),
    fullName: applicationDoc.fullName || "",
    phone: applicationDoc.phone || "",
    email: applicationDoc.email || "",
    drives: applicationDoc.drives || "",
    hasCar: applicationDoc.hasCar || "",
    customerServiceExperience: applicationDoc.customerServiceExperience || "",
    workPreference: applicationDoc.workPreference || "",
    salesExperience: applicationDoc.salesExperience || "",
    about: applicationDoc.about || "",
    summary: applicationDoc.summary || "",
    updatedAt: applicationDoc.updatedAt || null,
    createdAt: applicationDoc.createdAt || null
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

function limpiarTagControl(label = "") {
  const limpio = cleanText(label);

  if (!limpio || /^canal:/i.test(limpio)) {
    return "";
  }

  return limpio;
}

async function obtenerControlTowerStats(viewerUserDoc = null) {
  const ahora = new Date();
  const inicioHoy = new Date(ahora);
  inicioHoy.setHours(0, 0, 0, 0);
  const hace7Dias = new Date(ahora.getTime() - 7 * 24 * 60 * 60 * 1000);
  const whatsappBaseQuery = {
    sessionId: /^wa-session-/,
    role: "user"
  };

  const phoneReadyQuery = {
    quiereLlamada: "si",
    phone: { $exists: true, $ne: "" },
    bestCallDay: { $exists: true, $ne: "" },
    bestCallTime: { $exists: true, $ne: "" }
  };
  const phoneFollowUpQuery = {
    quiereLlamada: "si",
    phone: { $exists: true, $ne: "" },
    $or: [
      { bestCallDay: { $exists: false } },
      { bestCallDay: "" },
      { bestCallTime: { $exists: false } },
      { bestCallTime: "" }
    ]
  };

  const [
    chefSummary,
    coachSummary,
    sponsorOverview,
    trackedAccounts,
    whatsappTodayIds,
    whatsapp7DayIds,
    recentWhatsAppReplies,
    whatsappTopics,
    totalProfiles,
    interestedProfiles,
    readyToCallProfiles,
    followUpNeededProfiles,
    customerProfiles,
    recentReadyLeads,
    recentFollowUpLeads,
    topInterestProducts
  ] = await Promise.all([
    obtenerChefPublicStats(),
    obtenerCoachNetworkSummary(),
    obtenerCoachSponsorOverview(viewerUserDoc),
    obtenerControlTowerTrackedAccounts(viewerUserDoc),
    Message.distinct("visitorId", {
      ...whatsappBaseQuery,
      createdAt: { $gte: inicioHoy }
    }),
    Message.distinct("visitorId", {
      ...whatsappBaseQuery,
      createdAt: { $gte: hace7Dias }
    }),
    Message.find(whatsappBaseQuery, {
      sessionId: 1,
      visitorId: 1,
      content: 1,
      intent: 1,
      detectedTopics: 1,
      createdAt: 1
    })
      .where("createdAt")
      .gte(hace7Dias)
      .sort({ createdAt: -1 })
      .limit(12)
      .lean(),
    Message.aggregate([
      {
        $match: {
          ...whatsappBaseQuery,
          createdAt: { $gte: hace7Dias },
          detectedTopics: { $exists: true, $ne: [] }
        }
      },
      { $unwind: "$detectedTopics" },
      { $match: { detectedTopics: { $type: "string", $ne: "" } } },
      { $group: { _id: "$detectedTopics", count: { $sum: 1 } } },
      { $sort: { count: -1 } },
      { $limit: 8 }
    ]),
    Profile.countDocuments({ conversationCount: { $gt: 0 } }),
    Profile.countDocuments({ leadStatus: "interesado" }),
    Profile.countDocuments(phoneReadyQuery),
    Profile.countDocuments(phoneFollowUpQuery),
    Profile.countDocuments({ leadStatus: "cliente" }),
    Profile.find(
      phoneReadyQuery,
      {
        name: 1,
        phone: 1,
        leadStatus: 1,
        bestCallDay: 1,
        bestCallTime: 1,
        profileSummary: 1,
        lastInteractionAt: 1,
        productosInteres: 1
      }
    )
      .sort({ lastInteractionAt: -1 })
      .limit(10)
      .lean(),
    Profile.find(
      phoneFollowUpQuery,
      {
        name: 1,
        phone: 1,
        leadStatus: 1,
        bestCallDay: 1,
        bestCallTime: 1,
        profileSummary: 1,
        lastInteractionAt: 1,
        productosInteres: 1
      }
    )
      .sort({ lastInteractionAt: -1 })
      .limit(10)
      .lean(),
    Profile.aggregate([
      {
        $project: {
          productos: {
            $setUnion: [
              { $ifNull: ["$productosInteres", []] },
              { $ifNull: ["$productos", []] }
            ]
          }
        }
      },
      { $unwind: "$productos" },
      { $match: { productos: { $type: "string", $ne: "" } } },
      { $group: { _id: "$productos", count: { $sum: 1 } } },
      { $sort: { count: -1 } },
      { $limit: 8 }
    ])
  ]);

  const recentWhatsAppPhones = Array.from(
    new Set(
      recentWhatsAppReplies
        .map(item => String(item.sessionId || "").replace(/^wa-session-/, "").trim())
        .filter(Boolean)
    )
  );

  const whatsappLeadDocs = recentWhatsAppPhones.length
    ? await CoachLeadInbox.find(
        {
          phone: { $in: recentWhatsAppPhones }
        },
        {
          fullName: 1,
          phone: 1,
          status: 1,
          source: 1,
          ownerName: 1,
          generatedByName: 1,
          nextActionAt: 1,
          updatedAt: 1
        }
      )
        .sort({ updatedAt: -1, createdAt: -1 })
        .lean()
    : [];

  const whatsappLeadMap = new Map();

  for (const leadDoc of whatsappLeadDocs) {
    const phoneKey = String(leadDoc?.phone || "").trim();

    if (!phoneKey) {
      continue;
    }

    const currentLead = whatsappLeadMap.get(phoneKey);
    const currentIsChef = normalizarCoachLeadSource(currentLead?.source || "") === "chef_personal";
    const nextIsChef = normalizarCoachLeadSource(leadDoc?.source || "") === "chef_personal";

    if (!currentLead || (!currentIsChef && nextIsChef)) {
      whatsappLeadMap.set(phoneKey, leadDoc);
    }
  }

  return {
    updatedAt: ahora.toISOString(),
    overview: {
      chefFamiliesGuided: chefSummary.familiasGuiadas,
      coachDistributors: coachSummary.totalDistributors,
      coachActive7Days: coachSummary.activeLast7Days,
      whatsappRepliesToday: whatsappTodayIds.length,
      whatsappReplies7Days: whatsapp7DayIds.length,
      callsReady: readyToCallProfiles,
      callsFollowUp: followUpNeededProfiles
    },
    chef: chefSummary,
    coach: coachSummary,
    sponsor: sponsorOverview,
    tracked: trackedAccounts,
    whatsapp: {
      repliesToday: whatsappTodayIds.length,
      replies7Days: whatsapp7DayIds.length,
      recentReplies: recentWhatsAppReplies.map(item => ({
        sessionId: item.sessionId || "",
        phone: (item.sessionId || "").replace(/^wa-session-/, ""),
        content: truncarTextoPrompt(item.content || "", 200),
        intent: item.intent || "",
        topics: (Array.isArray(item.detectedTopics) ? item.detectedTopics : [])
          .map(limpiarTagControl)
          .filter(Boolean),
        createdAt: item.createdAt || null,
        linkedLead: (() => {
          const phone = String(item.sessionId || "").replace(/^wa-session-/, "").trim();
          const leadDoc = whatsappLeadMap.get(phone);

          if (!leadDoc?._id) {
            return null;
          }

          return {
            id: String(leadDoc._id),
            fullName: leadDoc.fullName || "",
            status: leadDoc.status || "",
            source: leadDoc.source || "",
            ownerName: leadDoc.ownerName || "",
            generatedByName: leadDoc.generatedByName || "",
            nextActionAt: leadDoc.nextActionAt || null,
            updatedAt: leadDoc.updatedAt || null
          };
        })()
      })),
      topTopics: whatsappTopics
        .map(item => limpiarTagControl(item._id))
        .filter(Boolean)
    },
    leads: {
      totalProfiles,
      interestedProfiles,
      readyToCallProfiles,
      followUpNeededProfiles,
      customerProfiles,
      topInterestProducts: topInterestProducts
        .map(item => ({ label: item._id, count: item.count }))
        .filter(item => item.label),
      recentReadyLeads: recentReadyLeads.map(item => ({
        name: formatearNombreOperativo(item.name),
        phone: item.phone || "",
        leadStatus: item.leadStatus || "sin estado",
        bestCallDay: item.bestCallDay || "",
        bestCallTime: item.bestCallTime || "",
        products: Array.isArray(item.productosInteres) ? item.productosInteres : [],
        summary: truncarTextoPrompt(item.profileSummary || "", 220),
        lastInteractionAt: item.lastInteractionAt || null
      })),
      recentFollowUpLeads: recentFollowUpLeads.map(item => ({
        name: formatearNombreOperativo(item.name),
        phone: item.phone || "",
        leadStatus: item.leadStatus || "sin estado",
        bestCallDay: item.bestCallDay || "",
        bestCallTime: item.bestCallTime || "",
        pending: [
          item.bestCallDay ? "" : "dia",
          item.bestCallTime ? "" : "hora"
        ].filter(Boolean).join(" y ") || "seguimiento",
        products: Array.isArray(item.productosInteres) ? item.productosInteres : [],
        summary: truncarTextoPrompt(item.profileSummary || "", 220),
        lastInteractionAt: item.lastInteractionAt || null
      }))
    }
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

function limpiarNombreCandidato(value = "") {
  return cleanText(value)
    .replace(/\s+/g, " ")
    .replace(/[.,;!?]+$/g, "")
    .trim();
}

function esNombreConfiable(value = "") {
  const nombre = limpiarNombreCandidato(value);

  if (!nombre || nombre.length < 2 || nombre.length > 48 || /\d/.test(nombre)) {
    return false;
  }

  const palabras = nombre.split(/\s+/).filter(Boolean);

  if (!palabras.length || palabras.length > 4) {
    return false;
  }

  const nombreLower = cleanLower(nombre);
  const terminosRuido = [
    "cliente",
    "telefono",
    "teléfono",
    "numero",
    "número",
    "correo",
    "email",
    "direccion",
    "dirección",
    "vivo",
    "cocino",
    "personas",
    "llamada",
    "interes",
    "precio",
    "garantia",
    "garantía"
  ];

  if (terminosRuido.some(termino => nombreLower.includes(termino))) {
    return false;
  }

  return palabras.every(palabra => /^[a-záéíóúñ'’-]+$/i.test(palabra));
}

function puntuarNombreConfiable(value = "") {
  const nombre = limpiarNombreCandidato(value);
  const palabras = nombre.split(/\s+/).filter(Boolean).length;
  return Math.min(palabras, 4) * 100 + nombre.length;
}

function seleccionarNombreConfiable(...values) {
  const candidatos = values
    .map(limpiarNombreCandidato)
    .filter(esNombreConfiable)
    .sort((a, b) => puntuarNombreConfiable(b) - puntuarNombreConfiable(a));

  return candidatos[0] || "";
}

function formatearNombreOperativo(value = "") {
  return seleccionarNombreConfiable(value) || "Nombre pendiente";
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

  const profile = await asegurarCoachDistributorProfile(
    userDoc,
    await CoachDistributorProfile.findOne({ userId: userDoc._id })
  );

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

function setCoachReturnCookie(res, req, token) {
  res.cookie(COACH_RETURN_SESSION_COOKIE, token, {
    httpOnly: true,
    sameSite: "lax",
    secure: requestEsSeguro(req),
    path: "/",
    maxAge: COACH_RETURN_SESSION_MS
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

function clearCoachReturnCookie(res, req) {
  res.clearCookie(COACH_RETURN_SESSION_COOKIE, {
    httpOnly: true,
    sameSite: "lax",
    secure: requestEsSeguro(req),
    path: "/"
  });
}

async function obtenerCoachAuthPorToken(token = "", options = {}) {
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

  if (options.touch !== false) {
    session.lastSeenAt = new Date();
    await session.save();
  }

  return {
    session,
    user: session.userId
  };
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
  const returnToken = cookies[COACH_RETURN_SESSION_COOKIE];

  if (token) {
    await CoachSession.deleteOne({ tokenHash: hashTokenSeguro(token) });
  }

  if (returnToken) {
    await CoachSession.deleteOne({ tokenHash: hashTokenSeguro(returnToken) });
  }

  clearCoachCookie(res, req);
  clearCoachReturnCookie(res, req);
}

async function obtenerCoachAuth(req) {
  const cookies = parsearCookies(req.headers.cookie || "");
  const token = cookies[COACH_SESSION_COOKIE];

  return obtenerCoachAuthPorToken(token, { touch: true });
}

async function obtenerCoachReturnAuth(req) {
  const cookies = parsearCookies(req.headers.cookie || "");
  const token = cookies[COACH_RETURN_SESSION_COOKIE];
  return obtenerCoachAuthPorToken(token, { touch: false });
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

  if (!(await coachTieneAccesoOperativo(auth.user))) {
    responderCoachError(res, 403, "Tu cuenta no tiene una suscripcion activa.");
    return null;
  }

  return auth;
}

async function requireCoachTeamManager(req, res) {
  const auth = await requireCoachActivo(req, res);

  if (!auth) {
    return null;
  }

  if (!esCoachTeamManager(auth.user)) {
    responderCoachError(res, 403, "Esta area solo esta disponible para quien administra el equipo.");
    return null;
  }

  return auth;
}

async function requireCoachTeamManagerUser(req, res) {
  const auth = await requireCoachUser(req, res);

  if (!auth) {
    return null;
  }

  if (!esCoachTeamManager(auth.user)) {
    responderCoachError(res, 403, "Esta accion solo esta disponible para quien administra el equipo.");
    return null;
  }

  return auth;
}

async function requireCoachTerritoryUser(req, res) {
  const auth = await requireCoachActivo(req, res);

  if (!auth) {
    return null;
  }

  if (!coachPuedeUsarTerritorios(auth.user)) {
    responderCoachError(res, 403, "Esta area territorial solo esta disponible para cuentas propias.");
    return null;
  }

  return auth;
}

async function requireControlTowerAccess(req, res) {
  const auth = await requireCoachUser(req, res);

  if (!auth) {
    return null;
  }

  if (!usuarioPuedeVerTorreControl(auth.user)) {
    responderCoachError(res, 403, "Esta area privada solo esta disponible para administracion.");
    return null;
  }

  return auth;
}

function obtenerInicioDia(date = new Date()) {
  const inicio = new Date(date);
  inicio.setHours(0, 0, 0, 0);
  return inicio;
}

function obtenerFinDia(date = new Date()) {
  const fin = new Date(date);
  fin.setHours(23, 59, 59, 999);
  return fin;
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

  const nombre = nombreMatch[1]
    .split(/\s+(?:y\s+mi|mi\s+n[uú]mero|mi\s+telefono|mi\s+tel[eé]fono|mi\s+correo|mi\s+email|quiero|para)\b/i)[0]
    .trim()
    .replace(/[.,;!?]+$/, "");

  return esNombreConfiable(nombre) ? nombre : "";
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

const CHEF_AGENDA_TIME_ZONE = "America/Chicago";

function obtenerComponentesFechaEnZona(date = new Date(), timeZone = CHEF_AGENDA_TIME_ZONE) {
  const formatter = new Intl.DateTimeFormat("en-US", {
    timeZone,
    year: "numeric",
    month: "numeric",
    day: "numeric",
    hour: "numeric",
    minute: "numeric",
    hour12: false
  });
  const parts = formatter.formatToParts(date);
  const map = {};

  parts.forEach(part => {
    if (part.type !== "literal") {
      map[part.type] = part.value;
    }
  });

  return {
    year: Number(map.year || 0),
    month: Number(map.month || 0),
    day: Number(map.day || 0),
    hour: Number(map.hour || 0),
    minute: Number(map.minute || 0)
  };
}

function sumarDiasCalendario(base = null, dayOffset = 0) {
  if (!base?.year || !base?.month || !base?.day) {
    return null;
  }

  const ref = new Date(Date.UTC(base.year, base.month - 1, base.day));
  ref.setUTCDate(ref.getUTCDate() + Number(dayOffset || 0));

  return {
    year: ref.getUTCFullYear(),
    month: ref.getUTCMonth() + 1,
    day: ref.getUTCDate()
  };
}

function construirFechaEnZonaLocal({ year, month, day, hour = 0, minute = 0 } = {}, timeZone = CHEF_AGENDA_TIME_ZONE) {
  if (!year || !month || !day) {
    return null;
  }

  const initialUtc = Date.UTC(year, month - 1, day, hour, minute, 0, 0);
  const initialDate = new Date(initialUtc);
  const zonedParts = obtenerComponentesFechaEnZona(initialDate, timeZone);
  const zonedUtc = Date.UTC(
    zonedParts.year || year,
    (zonedParts.month || month) - 1,
    zonedParts.day || day,
    zonedParts.hour || hour,
    zonedParts.minute || minute,
    0,
    0
  );

  return new Date(initialUtc - (zonedUtc - initialUtc));
}

function resolverHoraAgendaChef(label = "") {
  const normalized = normalizarTextoBusqueda(label || "");

  if (!normalized) {
    return null;
  }

  if (normalized.includes("mediodia")) {
    return { hour: 12, minute: 0 };
  }

  if (normalized.includes("temprano")) {
    return { hour: 9, minute: 0 };
  }

  if (normalized.includes("por la manana")) {
    return { hour: 10, minute: 0 };
  }

  if (normalized.includes("por la tarde")) {
    return { hour: 15, minute: 0 };
  }

  if (normalized.includes("por la noche")) {
    return { hour: 19, minute: 0 };
  }

  const match = normalized.match(/(\d{1,2})(?::(\d{2}))?\s*(am|pm)?/i);

  if (!match) {
    return null;
  }

  let hour = Number(match[1] || 0);
  const minute = Number(match[2] || 0);
  let meridiem = String(match[3] || "").toLowerCase();

  if (!meridiem) {
    if (/\bpm\b|tarde|noche/i.test(normalized)) {
      meridiem = "pm";
    } else if (/\bam\b|manana|temprano/i.test(normalized)) {
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

function resolverFechaAgendaChef(bestCallDay = "", timeParts = null, now = new Date(), timeZone = CHEF_AGENDA_TIME_ZONE) {
  const normalized = normalizarTextoBusqueda(bestCallDay || "");

  if (!normalized) {
    return null;
  }

  const nowParts = obtenerComponentesFechaEnZona(now, timeZone);
  const todayParts = {
    year: nowParts.year,
    month: nowParts.month,
    day: nowParts.day
  };
  const todayDayIndex = new Date(Date.UTC(nowParts.year, nowParts.month - 1, nowParts.day)).getUTCDay();
  const nowMinutes = (nowParts.hour || 0) * 60 + (nowParts.minute || 0);
  const targetMinutes = ((timeParts?.hour || 0) * 60) + (timeParts?.minute || 0);
  const hasFutureTimeToday = targetMinutes > nowMinutes + 2;
  const weekdayMap = {
    domingo: 0,
    lunes: 1,
    martes: 2,
    miercoles: 3,
    jueves: 4,
    viernes: 5,
    sabado: 6
  };

  if (normalized.includes("hoy")) {
    return hasFutureTimeToday ? todayParts : sumarDiasCalendario(todayParts, 1);
  }

  if (normalized.includes("manana")) {
    return sumarDiasCalendario(todayParts, 1);
  }

  if (normalized.includes("entre semana")) {
    if (todayDayIndex >= 1 && todayDayIndex <= 5 && hasFutureTimeToday) {
      return todayParts;
    }

    let offset = 1;

    while (offset < 8) {
      const candidateIndex = (todayDayIndex + offset) % 7;

      if (candidateIndex >= 1 && candidateIndex <= 5) {
        return sumarDiasCalendario(todayParts, offset);
      }

      offset += 1;
    }
  }

  if (normalized.includes("fin de semana")) {
    if ((todayDayIndex === 6 || todayDayIndex === 0) && hasFutureTimeToday) {
      return todayParts;
    }

    const saturdayOffset = todayDayIndex <= 6 ? (6 - todayDayIndex + 7) % 7 : 6;
    return sumarDiasCalendario(todayParts, saturdayOffset === 0 ? 7 : saturdayOffset);
  }

  for (const [label, dayIndex] of Object.entries(weekdayMap)) {
    if (!normalized.includes(label)) {
      continue;
    }

    let offset = (dayIndex - todayDayIndex + 7) % 7;

    if (offset === 0 && !hasFutureTimeToday) {
      offset = 7;
    }

    return sumarDiasCalendario(todayParts, offset);
  }

  return null;
}

function construirFechaAgendaChef(bestCallDay = "", bestCallTime = "", now = new Date(), timeZone = CHEF_AGENDA_TIME_ZONE) {
  const timeParts = resolverHoraAgendaChef(bestCallTime);
  const dayParts = resolverFechaAgendaChef(bestCallDay, timeParts, now, timeZone);

  if (!timeParts || !dayParts) {
    return null;
  }

  return construirFechaEnZonaLocal(
    {
      year: dayParts.year,
      month: dayParts.month,
      day: dayParts.day,
      hour: timeParts.hour,
      minute: timeParts.minute
    },
    timeZone
  );
}

function formatearFechaAgendaChef(date = null, timeZone = CHEF_AGENDA_TIME_ZONE) {
  if (!date) {
    return "";
  }

  const safeDate = date instanceof Date ? date : new Date(date);

  if (Number.isNaN(safeDate.getTime())) {
    return "";
  }

  return new Intl.DateTimeFormat("es-US", {
    timeZone,
    weekday: "long",
    month: "short",
    day: "numeric",
    hour: "numeric",
    minute: "2-digit"
  }).format(safeDate);
}

function construirDigestConversacionChef(leadDoc = null, profileDoc = null) {
  const historialBase =
    Array.isArray(leadDoc?.conversationHistory) && leadDoc.conversationHistory.length
      ? leadDoc.conversationHistory
      : profileDoc?.recentHistory || [];

  return normalizarHistorialConversacion(historialBase)
    .slice(-6)
    .map(entry => {
      const roleLabel = entry.role === "assistant" ? "Chef" : entry.role === "user" ? "Cliente" : "Nota";
      return `${roleLabel}: ${truncarTextoPrompt(entry.content || "", 90)}`;
    })
    .filter(Boolean)
    .join(" | ")
    .slice(0, 420);
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

async function resolverPerfilExistente(visitorId, email = "", phone = "", leadId = null, coachChefContext = null) {
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

  const ownerUserId = coachChefContext?.ownership?.ownerUserId || null;
  const scopedConditions = ownerUserId
    ? condiciones.map(condicion => ({
        ...condicion,
        coachOwnerUserId: ownerUserId
      }))
    : condiciones;

  return await Profile.findOne({ $or: scopedConditions }).sort({ createdAt: 1 });
}

async function guardarMensajeRaw({
  visitorId,
  sessionId,
  profileId = null,
  leadId = null,
  coachChefContext = null,
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
      ...construirCoachChefTrackingFields(coachChefContext),
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
  leadGuardado = null,
  coachChefContext = null
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
    leadGuardado?._id || null,
    coachChefContext
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
    coachOwnerUserId:
      leadGuardado?.coachOwnerUserId ||
      coachChefContext?.ownership?.ownerUserId ||
      perfilExistente?.coachOwnerUserId ||
      null,
    coachOwnerEmail:
      leadGuardado?.coachOwnerEmail ||
      coachChefContext?.ownership?.ownerEmail ||
      perfilExistente?.coachOwnerEmail ||
      "",
    coachOwnerName:
      leadGuardado?.coachOwnerName ||
      coachChefContext?.ownership?.ownerName ||
      perfilExistente?.coachOwnerName ||
      "",
    generatedByUserId:
      leadGuardado?.generatedByUserId ||
      coachChefContext?.ownership?.generatedByUserId ||
      perfilExistente?.generatedByUserId ||
      null,
    generatedByName:
      leadGuardado?.generatedByName ||
      coachChefContext?.ownership?.generatedByName ||
      perfilExistente?.generatedByName ||
      "",
    generatedByAccountType:
      leadGuardado?.generatedByAccountType ||
      coachChefContext?.ownership?.generatedByAccountType ||
      perfilExistente?.generatedByAccountType ||
      "",
    chefSlug:
      leadGuardado?.chefSlug ||
      coachChefContext?.slug ||
      perfilExistente?.chefSlug ||
      "",
    coachInboxLeadId: leadGuardado?.coachInboxLeadId || perfilExistente?.coachInboxLeadId || null,
    coachInboxLastSyncedAt:
      leadGuardado?.coachInboxLastSyncedAt ||
      perfilExistente?.coachInboxLastSyncedAt ||
      null,
    name: seleccionarNombreConfiable(
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
    programarPersistenciaRedis(() => persistirEstadoSesionRedis(sessionId), "estado de sesion en Redis");
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

  programarPersistenciaRedis(() => persistirEstadoSesionRedis(sessionId), "estado de sesion en Redis");
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
  primaryLead.name = seleccionarNombreConfiable(
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

async function resolverLeadExistente(sessionId, visitorId = "", email = "", phone = "", coachChefContext = null) {
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

  const ownerUserId = coachChefContext?.ownership?.ownerUserId || null;
  const scopedConditions = ownerUserId
    ? condiciones.map(condicion => ({
        ...condicion,
        coachOwnerUserId: ownerUserId
      }))
    : condiciones;
  const coincidencias = await Lead.find({ $or: scopedConditions }).sort({ createdAt: 1 });

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

function obtenerHistorialConversacionMemoria(sessionId, limit = MAX_PROMPT_HISTORY_MESSAGES) {
  if (!sessionId) {
    return [];
  }

  const safeLimit = Math.max(1, Number(limit || MAX_PROMPT_HISTORY_MESSAGES));

  return (conversaciones[sessionId] || [])
    .slice(-safeLimit)
    .filter(entry => entry?.role && entry?.content)
    .map(entry => ({
      role: entry.role,
      content: entry.content
    }));
}

async function obtenerHistorialConversacionPrompt(sessionId, limit = MAX_PROMPT_HISTORY_MESSAGES, options = {}) {
  if (!sessionId) {
    return [];
  }

  await asegurarSesionRedisCargada(sessionId);
  const safeLimit = Math.max(1, Number(limit || MAX_PROMPT_HISTORY_MESSAGES));
  const preferMemory = options?.preferMemory === true;
  const historialMemoria = obtenerHistorialConversacionMemoria(sessionId, safeLimit);

  if (preferMemory && historialMemoria.length) {
    return historialMemoria;
  }

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

  return historialMemoria;
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

async function guardarLeadSiExiste(
  texto,
  sessionId,
  visitorId = "",
  estadoConversacion = null,
  coachChefContext = null
) {
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
    const leadExistente = await resolverLeadExistente(sessionId, visitorId, email, phone, coachChefContext);

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
      ...construirCoachChefTrackingFields(coachChefContext),
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

    const nombre = seleccionarNombreConfiable(
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

function construirPayloadCoachLeadDesdeChef(leadDoc = null, profileDoc = null, coachChefContext = null) {
  if (!leadDoc && !profileDoc) {
    return null;
  }

  const fullName =
    seleccionarNombreConfiable(leadDoc?.name, profileDoc?.name) ||
    String(leadDoc?.name || profileDoc?.name || "").trim();
  const phone = normalizePhone(leadDoc?.phone || profileDoc?.phone || "");
  const email = normalizarEmail(leadDoc?.email || profileDoc?.email || "");

  if (!fullName || (!phone && !email)) {
    return null;
  }

  const productos = combinarListas(
    profileDoc?.productosInteres || [],
    leadDoc?.productos || [],
    profileDoc?.productos || []
  );
  const temas = combinarListas(profileDoc?.temasInteres || [], leadDoc?.temasInteres || []);
  const interest = productos[0] || temas[0] || "Chef personal";
  const address = cleanText(leadDoc?.direccion || profileDoc?.direccion || "")
    .replace(/\s+/g, " ")
    .trim()
    .slice(0, 240);
  const inferredZone = inferCoachCityZipFromAddress(address);
  const generatedByName =
    coachChefContext?.ownership?.generatedByName ||
    coachChefContext?.userDoc?.name ||
    coachChefContext?.userDoc?.email ||
    "";
  const bestCallDay = leadDoc?.bestCallDay || profileDoc?.bestCallDay || "";
  const bestCallTime = leadDoc?.bestCallTime || profileDoc?.bestCallTime || "";
  const appointmentAt = construirFechaAgendaChef(bestCallDay, bestCallTime);
  const appointmentLabel = formatearFechaAgendaChef(appointmentAt);
  const quiereLlamada = (leadDoc?.quiereLlamada || profileDoc?.quiereLlamada || "") === "si";
  const operationalSummary = truncarTextoPrompt(construirResumenLeadOperativo(leadDoc, profileDoc), 280);
  const profileSummary = truncarTextoPrompt(profileDoc?.profileSummary || "", 220);
  const conversationDigest = construirDigestConversacionChef(leadDoc, profileDoc);
  const summary = [
    operationalSummary || "",
    appointmentLabel ? `Cita pedida para ${appointmentLabel}.` : quiereLlamada ? "Pidio llamada informativa." : "",
    profileSummary ? `Perfil: ${profileSummary}.` : ""
  ]
    .filter(Boolean)
    .join(" ")
    .trim()
    .slice(0, 600);
  const notes = [
    "Lead captado desde Chef personal por WhatsApp.",
    coachChefContext?.slug ? `Chef: ${coachChefContext.slug}.` : "",
    generatedByName ? `Compartido por: ${generatedByName}.` : "",
    appointmentLabel ? `Cita confirmada: ${appointmentLabel}.` : "",
    bestCallDay ? `Mejor dia: ${bestCallDay}.` : "",
    bestCallTime ? `Mejor hora: ${bestCallTime}.` : "",
    productos.length ? `Productos: ${productos.join(", ")}.` : "",
    temas.length ? `Temas: ${temas.join(", ")}.` : "",
    address ? `Direccion mencionada: ${address}.` : "",
    summary ? `Resumen operativo: ${summary}` : "",
    conversationDigest ? `Conversacion reciente: ${conversationDigest}` : ""
  ]
    .filter(Boolean)
    .join(" ")
    .trim()
    .slice(0, 1200);
  const status = appointmentAt ? "agendado" : quiereLlamada ? "contactado" : "nuevo";
  const nextAction = appointmentAt ? "cita" : quiereLlamada ? "llamar" : "";

  return {
    fullName,
    phone,
    email,
    city: inferredZone.city || "",
    zipCode: inferredZone.zipCode || "",
    interest,
    notes,
    summary,
    source: "chef_personal",
    consentGiven: true,
    status,
    nextAction,
    nextActionAt: appointmentAt ? appointmentAt.toISOString() : null,
    replaceNotes: true
  };
}

async function sincronizarLeadPublicoACoachInbox({
  texto = "",
  leadDoc = null,
  profileDoc = null,
  coachChefContext = null
} = {}) {
  if (!coachChefContext?.userDoc || !leadDoc) {
    return null;
  }

  const leadRoutingContext = (await resolverCoachChefLeadRoutingContext(coachChefContext)) || {
    userDoc: coachChefContext.userDoc,
    ownerProfileDoc: coachChefContext.ownerProfileDoc || coachChefContext.profileDoc || null,
    ownership: coachChefContext.ownership || null
  };
  const payload = construirPayloadCoachLeadDesdeChef(leadDoc, profileDoc, leadRoutingContext);

  if (!payload) {
    return null;
  }

  const leadInfo = extraerLeadInfo(texto);
  const contactSharedNow = Boolean(leadInfo?.phone || leadInfo?.email || detectarEnvioDatosContacto(texto));
  const alreadySynced = Boolean(leadDoc.coachInboxLeadId || profileDoc?.coachInboxLeadId);

  if (!contactSharedNow && !alreadySynced) {
    return null;
  }

  try {
    const ownerProfileDoc = leadRoutingContext.ownerProfileDoc?.toObject
      ? leadRoutingContext.ownerProfileDoc.toObject()
      : leadRoutingContext.ownerProfileDoc || null;
    const result = await guardarCoachInboxLead({
      userDoc: leadRoutingContext.userDoc,
      profileDoc: ownerProfileDoc,
      payload,
      sendDestination: !alreadySynced,
      ownershipOverride: leadRoutingContext.ownership || null
    });

    if (!result?.leadDoc?._id) {
      return null;
    }

    const syncPayload = {
      coachInboxLeadId: result.leadDoc._id,
      coachInboxLastSyncedAt: new Date()
    };

    await Promise.all([
      Lead.findByIdAndUpdate(leadDoc._id, {
        $set: syncPayload
      }),
      profileDoc?._id
        ? Profile.findByIdAndUpdate(profileDoc._id, {
            $set: syncPayload
          })
        : Promise.resolve()
    ]);

    return result;
  } catch (error) {
    console.error("Error sincronizando lead del Chef personal al Coach:", error.message);
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
- Si la pregunta es sobre como usar la pagina, CRM, Agenda, Prospeccion, Equipo, Territorio, Mensajes o Destino de leads, responde como guia interna del sistema
- En preguntas de uso del sistema da pasos cortos y claros con el nombre real de la pestana o boton
- En modo ayuda interna no vendas ni cierres; solo explica como se usa la herramienta
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

function construirTwilioSmsApiAuthHeader() {
  if (!TWILIO_ACCOUNT_SID || !TWILIO_AUTH_TOKEN) {
    return "";
  }

  return `Basic ${Buffer.from(`${TWILIO_ACCOUNT_SID}:${TWILIO_AUTH_TOKEN}`).toString("base64")}`;
}

function construirTwilioWebhookAbsoluteUrl(req) {
  const protocolHeader = String(req.headers["x-forwarded-proto"] || "")
    .split(",")[0]
    .trim();
  const protocol = protocolHeader || (requestEsSeguro(req) ? "https" : "http");
  const host = req.get("host") || "";

  return host ? `${protocol}://${host}${req.originalUrl || req.url || ""}` : "";
}

function construirTwilioWebhookExpectedSignature(req, authToken = "") {
  const safeAuthToken = String(authToken || "").trim();
  const url = construirTwilioWebhookAbsoluteUrl(req);

  if (!safeAuthToken || !url) {
    return "";
  }

  const params = req.body && typeof req.body === "object" ? req.body : {};
  const payload = Object.keys(params)
    .sort()
    .reduce((acc, key) => {
      const value = params[key];

      if (Array.isArray(value)) {
        return `${acc}${key}${value.join("")}`;
      }

      return `${acc}${key}${String(value ?? "")}`;
    }, url);

  return crypto.createHmac("sha1", safeAuthToken).update(payload, "utf8").digest("base64");
}

function validarFirmaTwilioWebhook(req) {
  const providedSignature = String(req.headers["x-twilio-signature"] || "").trim();

  if (!TWILIO_AUTH_TOKEN || !providedSignature) {
    return false;
  }

  const expectedSignature = construirTwilioWebhookExpectedSignature(req, TWILIO_AUTH_TOKEN);

  if (!expectedSignature) {
    return false;
  }

  const expected = Buffer.from(expectedSignature);
  const provided = Buffer.from(providedSignature);

  if (expected.length !== provided.length) {
    return false;
  }

  return crypto.timingSafeEqual(expected, provided);
}

function resolverOwnerUnicoDesdeDocs(docs = [], fieldName = "") {
  const ownerIds = Array.from(
    new Set(
      (Array.isArray(docs) ? docs : [])
        .map(doc => String(doc?.[fieldName] || "").trim())
        .filter(Boolean)
    )
  );

  return ownerIds.length === 1 ? ownerIds[0] : "";
}

async function resolverCoachInboundRouteContext({ channel = "", phone = "", collection = "lead_inbox" } = {}) {
  const safePhone = normalizePhone(phone || "");
  const safeChannel = cleanText(channel || "")
    .trim()
    .toLowerCase();

  if (!safePhone || !safeChannel) {
    return {
      matchedDoc: null,
      coachChefContext: null,
      ownerUserId: ""
    };
  }

  let routeOwnerUserId = obtenerRutaCanalTelefono(safeChannel, safePhone);

  if (!routeOwnerUserId) {
    routeOwnerUserId = await obtenerRutaCanalTelefonoRedis(safeChannel, safePhone);
  }
  const isPublicLead = collection === "public_lead";
  const Model = isPublicLead ? Lead : CoachLeadInbox;
  const ownerField = isPublicLead ? "coachOwnerUserId" : "ownerUserId";
  const docs = await Model.find({ phone: safePhone })
    .sort({ updatedAt: -1, createdAt: -1 })
    .limit(12);
  let matchedDoc = null;

  if (routeOwnerUserId) {
    matchedDoc =
      docs.find(doc => String(doc?.[ownerField] || "").trim() === routeOwnerUserId) || null;
  }

  if (!matchedDoc) {
    const uniqueOwnerUserId = resolverOwnerUnicoDesdeDocs(docs, ownerField);

    if (uniqueOwnerUserId) {
      matchedDoc =
        docs.find(doc => String(doc?.[ownerField] || "").trim() === uniqueOwnerUserId) || docs[0] || null;
    } else if (docs.length === 1) {
      matchedDoc = docs[0];
    }
  }

  let coachChefContext = matchedDoc ? await resolverCoachChefContextPorLead(matchedDoc) : null;
  const resolvedOwnerUserId =
    String(coachChefContext?.ownership?.ownerUserId || "") ||
    String(matchedDoc?.[ownerField] || "").trim() ||
    routeOwnerUserId;

  if (!coachChefContext && routeOwnerUserId) {
    coachChefContext = await resolverCoachChefContextPorOwnerUserId(routeOwnerUserId);
  }

  return {
    matchedDoc: matchedDoc || null,
    coachChefContext: coachChefContext || null,
    ownerUserId: resolvedOwnerUserId || ""
  };
}

function twilioSmsEstaConfigurado() {
  return Boolean(
    TWILIO_SMS_ENABLED &&
      TWILIO_ACCOUNT_SID &&
      TWILIO_AUTH_TOKEN &&
      (TWILIO_MESSAGING_SERVICE_SID || TWILIO_SMS_FROM)
  );
}

function formatearTelefonoE164(value = "") {
  const raw = String(value || "").trim();

  if (!raw) {
    return "";
  }

  if (raw.startsWith("+")) {
    const digits = raw.replace(/\D+/g, "");
    return digits ? `+${digits}` : "";
  }

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

  return digits.length >= 8 ? `+${digits}` : "";
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

function construirTwilioEmptyResponse() {
  return `<?xml version="1.0" encoding="UTF-8"?><Response></Response>`;
}

async function enviarTwilioSms({ to = "", body = "", statusCallbackUrl = "" } = {}) {
  const message = cleanText(String(body || "")).slice(0, 1600);
  const toPhone = formatearTelefonoE164(to);

  if (!twilioSmsEstaConfigurado()) {
    return {
      ok: false,
      error: "Twilio SMS todavia no esta configurado."
    };
  }

  if (!toPhone) {
    return {
      ok: false,
      error: "No encontre un telefono valido para enviar SMS."
    };
  }

  if (!message) {
    return {
      ok: false,
      error: "El mensaje SMS viene vacio."
    };
  }

  const params = new URLSearchParams();
  params.set("To", toPhone);
  params.set("Body", message);

  if (TWILIO_MESSAGING_SERVICE_SID) {
    params.set("MessagingServiceSid", TWILIO_MESSAGING_SERVICE_SID);
  } else {
    params.set("From", TWILIO_SMS_FROM);
  }

  if (statusCallbackUrl) {
    params.set("StatusCallback", statusCallbackUrl);
  }

  try {
    const response = await fetch(`https://api.twilio.com/2010-04-01/Accounts/${TWILIO_ACCOUNT_SID}/Messages.json`, {
      method: "POST",
      headers: {
        Authorization: construirTwilioSmsApiAuthHeader(),
        "Content-Type": "application/x-www-form-urlencoded"
      },
      body: params.toString()
    });

    const data = await response.json().catch(() => ({}));

    if (!response.ok) {
      return {
        ok: false,
        status: response.status,
        error: data?.message || `Twilio respondio ${response.status}`
      };
    }

    return {
      ok: true,
      status: response.status,
      sid: data?.sid || "",
      to: data?.to || toPhone,
      from: data?.from || TWILIO_SMS_FROM || "",
      raw: data
    };
  } catch (error) {
    return {
      ok: false,
      error: error.message || "No pude enviar el SMS."
    };
  }
}

async function sembrarLeadYPerfilCanal({
  sessionId,
  visitorId,
  phoneHint = "",
  nameHint = "",
  coachChefContext = null
}) {
  const phone = normalizePhone(phoneHint);
  const name = cleanText(nameHint);
  const coachTrackingFields = construirCoachChefTrackingFields(coachChefContext);
  let leadDoc = await resolverLeadExistente(sessionId, visitorId, "", phone, coachChefContext);
  let profileDoc = await resolverPerfilExistente(visitorId, "", phone, leadDoc?._id || null, coachChefContext);

  if (!leadDoc && phone) {
    leadDoc = await Lead.create({
      ...coachTrackingFields,
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

    if (coachTrackingFields.coachOwnerUserId && String(leadDoc.coachOwnerUserId || "") !== String(coachTrackingFields.coachOwnerUserId)) {
      leadDoc.coachOwnerUserId = coachTrackingFields.coachOwnerUserId;
      dirty = true;
    }

    if (coachTrackingFields.coachOwnerEmail && leadDoc.coachOwnerEmail !== coachTrackingFields.coachOwnerEmail) {
      leadDoc.coachOwnerEmail = coachTrackingFields.coachOwnerEmail;
      dirty = true;
    }

    if (coachTrackingFields.coachOwnerName && leadDoc.coachOwnerName !== coachTrackingFields.coachOwnerName) {
      leadDoc.coachOwnerName = coachTrackingFields.coachOwnerName;
      dirty = true;
    }

    if (coachTrackingFields.generatedByUserId && String(leadDoc.generatedByUserId || "") !== String(coachTrackingFields.generatedByUserId)) {
      leadDoc.generatedByUserId = coachTrackingFields.generatedByUserId;
      dirty = true;
    }

    if (coachTrackingFields.generatedByName && leadDoc.generatedByName !== coachTrackingFields.generatedByName) {
      leadDoc.generatedByName = coachTrackingFields.generatedByName;
      dirty = true;
    }

    if (
      coachTrackingFields.generatedByAccountType &&
      leadDoc.generatedByAccountType !== coachTrackingFields.generatedByAccountType
    ) {
      leadDoc.generatedByAccountType = coachTrackingFields.generatedByAccountType;
      dirty = true;
    }

    if (coachTrackingFields.chefSlug && leadDoc.chefSlug !== coachTrackingFields.chefSlug) {
      leadDoc.chefSlug = coachTrackingFields.chefSlug;
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
      ...coachTrackingFields,
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

    if (
      coachTrackingFields.coachOwnerUserId &&
      String(profileDoc.coachOwnerUserId || "") !== String(coachTrackingFields.coachOwnerUserId)
    ) {
      profileDoc.coachOwnerUserId = coachTrackingFields.coachOwnerUserId;
      dirty = true;
    }

    if (coachTrackingFields.coachOwnerEmail && profileDoc.coachOwnerEmail !== coachTrackingFields.coachOwnerEmail) {
      profileDoc.coachOwnerEmail = coachTrackingFields.coachOwnerEmail;
      dirty = true;
    }

    if (coachTrackingFields.coachOwnerName && profileDoc.coachOwnerName !== coachTrackingFields.coachOwnerName) {
      profileDoc.coachOwnerName = coachTrackingFields.coachOwnerName;
      dirty = true;
    }

    if (
      coachTrackingFields.generatedByUserId &&
      String(profileDoc.generatedByUserId || "") !== String(coachTrackingFields.generatedByUserId)
    ) {
      profileDoc.generatedByUserId = coachTrackingFields.generatedByUserId;
      dirty = true;
    }

    if (coachTrackingFields.generatedByName && profileDoc.generatedByName !== coachTrackingFields.generatedByName) {
      profileDoc.generatedByName = coachTrackingFields.generatedByName;
      dirty = true;
    }

    if (
      coachTrackingFields.generatedByAccountType &&
      profileDoc.generatedByAccountType !== coachTrackingFields.generatedByAccountType
    ) {
      profileDoc.generatedByAccountType = coachTrackingFields.generatedByAccountType;
      dirty = true;
    }

    if (coachTrackingFields.chefSlug && profileDoc.chefSlug !== coachTrackingFields.chefSlug) {
      profileDoc.chefSlug = coachTrackingFields.chefSlug;
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
  source = "web",
  chefSlug = "",
  coachChefContextOverride = null
}) {
  const preguntaLimpia = cleanText(pregunta);
  const visitorIdLimpio = cleanText(visitorId || sessionId);
  const fastChannel = /^(whatsapp|sms)/i.test(source);
  const coachChefContext =
    coachChefContextOverride || (fastChannel ? null : await resolverCoachChefContextPorSlug(chefSlug));
  const sourceChannel = /^sms/i.test(source) ? "sms" : /whatsapp/i.test(source) ? "whatsapp" : "";

  if (!preguntaLimpia) {
    return { ok: false, status: 400, error: "pregunta requerida" };
  }

  if (!sessionId) {
    return { ok: false, status: 400, error: "sessionId requerido" };
  }

  await asegurarSesionRedisCargada(sessionId);

  const chefUsage = await validarLimiteUsoChef(visitorIdLimpio);

  if (!chefUsage.allowed) {
    return {
      ok: false,
      status: 429,
      error: `Por hoy ya usaste tus ${CHEF_MAX_MESSAGES_PER_DAY} mensajes gratis. Manana se reinicia tu acceso.`
    };
  }

  if (fastChannel && sourceChannel && phoneHint && coachChefContext?.ownership?.ownerUserId) {
    registrarRutaCanalTelefono(sourceChannel, phoneHint, coachChefContext.ownership.ownerUserId);
  }

  marcarSesionActiva(sessionId);
  limpiarMemoriaSesiones();

  try {
    const modoChat = "chef";
    const modoPrompt = construirContextoModoPrompt(modoChat);
    const canalActivo = /^sms/i.test(source) ? "sms" : "whatsapp";
    const modoPromptCanal = fastChannel
      ? `
CANAL ACTIVO:
- canal: ${canalActivo}
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
      nameHint,
      coachChefContext
    });

    hidratarEstadoConversacion(sessionId, profileInicial, leadInicial);
    actualizarEstadoConversacion(sessionId, preguntaLimpia);
    const estadoActual = obtenerEstadoConversacion(sessionId);

    let leadGuardado = await guardarLeadSiExiste(
      preguntaLimpia,
      sessionId,
      visitorIdLimpio,
      estadoActual,
      coachChefContext
    );

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
      leadGuardado: leadGuardado || leadInicial,
      coachChefContext
    });

    await sincronizarLeadPublicoACoachInbox({
      texto: preguntaLimpia,
      leadDoc: leadGuardado || leadInicial,
      profileDoc: profileGuardado || profileInicial,
      coachChefContext
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
      coachChefContext,
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

    registrarMensajeMemoria(sessionId, "user", preguntaLimpia);
    const cacheHistory = obtenerHistorialConversacionMemoria(sessionId, fastChannel ? 4 : 6);
    const statePrompt = `${estadoPrompt}\n${modoPromptCanal}`.trim();
    const aiCacheOptions = {
      mode: modoChat,
      scope:
        String(coachChefContext?.ownership?.ownerUserId || "").trim() ||
            cleanText(coachChefContext?.slug || chefSlug || "").trim() ||
            "public",
      question: preguntaLimpia,
      activeWorkspace: source,
      activeDemoStage: canalActivo,
      statePrompt,
      profilePrompt,
      history: cacheHistory,
      extra: {
        fastChannel,
        activeLeadContext
      }
    };
    const cachedAiReply = await obtenerRespuestaAiCacheada(aiCacheOptions);
    let respuestaIA = null;
    let cacheHit = false;

    if (cachedAiReply?.reply) {
      respuestaIA = {
        role: "assistant",
        content: cachedAiReply.reply
      };
      cacheHit = true;
    } else {
      const contexto = fastChannel
        ? construirContextoEstaticoChef(preguntaLimpia)
        : await construirContexto(preguntaLimpia, modoChat);
      const historialPrompt = await obtenerHistorialConversacionPrompt(
        sessionId,
        fastChannel ? 4 : MAX_PROMPT_HISTORY_MESSAGES
      );

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

      respuestaIA = data.choices[0].message;
      programarPersistenciaRedis(
        () => guardarRespuestaAiCacheada(aiCacheOptions, respuestaIA?.content || ""),
        "cache IA del Chef"
      );
    }

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
      coachChefContext,
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
      activeLeadContext,
      cacheHit
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

function normalizarCoachHelpText(value = "") {
  return String(value || "")
    .toLowerCase()
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/[^a-z0-9\s]/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function coachHelpMatchesAny(text = "", patterns = []) {
  return patterns.some(pattern => {
    if (pattern instanceof RegExp) {
      return pattern.test(text);
    }

    return text.includes(normalizarCoachHelpText(pattern));
  });
}

function formatearCoachHelpWorkspaceLabel(workspace = "") {
  const labels = {
    cierre: "Cierre",
    prospeccion: "Prospeccion",
    crm: "CRM",
    agenda: "Agenda",
    mensajes: "Mensajes",
    equipo: "Equipo",
    territorio: "Territorio"
  };

  return labels[String(workspace || "").trim().toLowerCase()] || "el modulo correcto";
}

function construirCoachHelpStepsReply({ intro = "", steps = [], outro = "" } = {}) {
  const numberedSteps = (Array.isArray(steps) ? steps : [])
    .filter(Boolean)
    .map((item, index) => `${index + 1}. ${item}`);

  return [intro, numberedSteps.join("\n"), outro].filter(Boolean).join("\n");
}

function coachQuestionPareceAyudaSistema(question = "", activeWorkspace = "") {
  const normalizedQuestion = normalizarCoachHelpText(question);

  if (!normalizedQuestion) {
    return false;
  }

  const helpIntent = coachHelpMatchesAny(normalizedQuestion, [
    /manual/,
    /como/,
    /donde/,
    /puedo/,
    /busco/,
    /veo/,
    /abro/,
    /entro/,
    /agrego/,
    /configuro/,
    /cambio/,
    /recibo/,
    /sirve/,
    /funciona/,
    /uso/,
    /usar/,
    /explica/,
    /explicame/,
    /ayuda/
  ]);

  const systemKeywords = coachHelpMatchesAny(normalizedQuestion, [
    /crm/,
    /agenda/,
    /prospeccion/,
    /equipo/,
    /territorio/,
    /mensajes/,
    /boletin/,
    /soporte/,
    /subcuenta/,
    /telemarketing/,
    /4 en 14/,
    /4 y 14/,
    /reclutamiento/,
    /campana chef/,
    /chef/,
    /contactos compartidos/,
    /subir contactos/,
    /qr/,
    /destino/,
    /correo/,
    /email/,
    /pagina/,
    /app/,
    /coach/
  ]);

  const workspaceHint = ["crm", "agenda", "prospeccion", "equipo", "territorio", "mensajes"].includes(
    String(activeWorkspace || "").trim().toLowerCase()
  );

  const workspaceActionHint =
    workspaceHint &&
    coachHelpMatchesAny(normalizedQuestion, [
      /cita/,
      /seguimiento/,
      /nota/,
      /resultado/,
      /no atendio/,
      /follow up/,
      /reagend/,
      /venta/,
      /no venta/,
      /mapa/,
      /direccion/
    ]);

  return (
    (helpIntent && systemKeywords) ||
    workspaceActionHint ||
    coachHelpMatchesAny(normalizedQuestion, [
      /como funciona la pagina/,
      /como funciona el coach/,
      /como se usa/,
      /que puedes explicar/,
      /que me puedes explicar/,
      /que puedo hacer aqui/
    ])
  );
}

function resolverCoachHelpTopic(question = "", activeWorkspace = "") {
  const normalizedQuestion = normalizarCoachHelpText(question);
  const workspace = String(activeWorkspace || "").trim().toLowerCase();

  if (!normalizedQuestion) {
    return "";
  }

  if (
    coachHelpMatchesAny(normalizedQuestion, [
      /manual/,
      /como funciona la pagina/,
      /como funciona el coach/,
      /como se usa/,
      /que puedes explicar/,
      /que me puedes explicar/,
      /que puedo hacer aqui/
    ])
  ) {
    return "manual_general";
  }

  if (
    coachHelpMatchesAny(normalizedQuestion, [/4 en 14/, /4 y 14/, /4 14/]) &&
    coachHelpMatchesAny(normalizedQuestion, [/crm/, /buscar/, /busco/, /donde/, /veo/, /fila/, /encuentro/])
  ) {
    return "crm_4_14";
  }

  if (
    coachHelpMatchesAny(normalizedQuestion, [/correo/, /email/]) &&
    coachHelpMatchesAny(normalizedQuestion, [/copia/, /copias/, /lead/, /leads/, /destino/, /recibir/])
  ) {
    return "lead_destination_email";
  }

  if (
    coachHelpMatchesAny(normalizedQuestion, [
      /agenda/,
      /hoy/,
      /semana/,
      /mapa/,
      /direccion/,
      /ya afuera/,
      /entro a casa/,
      /reagend/,
      /follow up/,
      /no atendio/
    ])
  ) {
    return "agenda";
  }

  if (
    coachHelpMatchesAny(normalizedQuestion, [
      /crm/,
      /fila/,
      /seguimiento/,
      /nota/,
      /detalle/,
      /telemarketing/,
      /representante/
    ])
  ) {
    return "crm_workflow";
  }

  if (coachHelpMatchesAny(normalizedQuestion, [/equipo/, /subcuenta/, /novato/, /contrasena temporal/, /crear cuenta/])) {
    return "team";
  }

  if (coachHelpMatchesAny(normalizedQuestion, [/territorio/, /invita/, /invitacion/, /upline/, /co admin/])) {
    return "territory";
  }

  if (coachHelpMatchesAny(normalizedQuestion, [/mensajes/, /boletin/, /boletines/, /soporte/, /chat interno/, /notificacion/])) {
    return "messages";
  }

  if (
    coachHelpMatchesAny(normalizedQuestion, [
      /chef/,
      /campana chef/,
      /subir contactos/,
      /contactos compartidos/,
      /qr/,
      /prospeccion/,
      /destino de leads/
    ])
  ) {
    return "prospection";
  }

  if (coachHelpMatchesAny(normalizedQuestion, [/reclutamiento/, /aplicacion de trabajo/, /trabajo/])) {
    return "recruitment";
  }

  if (workspace === "agenda") {
    return "agenda";
  }

  if (workspace === "crm") {
    return "crm_workflow";
  }

  if (workspace === "prospeccion") {
    return "prospection";
  }

  if (workspace === "equipo") {
    return "team";
  }

  if (workspace === "territorio") {
    return "territory";
  }

  if (workspace === "mensajes") {
    return "messages";
  }

  return "manual_general";
}

function construirCoachHelpReply({ userDoc = null, profileDoc = null, question = "", activeWorkspace = "" } = {}) {
  if (!userDoc || !coachQuestionPareceAyudaSistema(question, activeWorkspace)) {
    return null;
  }

  const accountType = normalizarCoachAccountType(userDoc.accountType || "owner");
  const teamRole = normalizarCoachTeamRole(profileDoc?.teamRole || "", userDoc);
  const portalMode = resolverCoachPortalMode(userDoc, profileDoc);
  const isTelemarketingPortal = portalMode === "telemarketing";
  const isTeamManager = esCoachTeamManager(userDoc);
  const canUseTerritories = coachPuedeUsarTerritorios(userDoc);
  const topicId = resolverCoachHelpTopic(question, activeWorkspace);
  const crmWorkspaceHint =
    String(activeWorkspace || "").trim().toLowerCase() === "crm"
      ? ""
      : "Si no estas ahi, cambiate a CRM y te sigues guiando desde esa vista.";
  const agendaWorkspaceHint =
    String(activeWorkspace || "").trim().toLowerCase() === "agenda"
      ? ""
      : "Si no estas ahi, cambiate a Agenda y te sigues guiando desde esa vista.";
  const prospectionWorkspaceHint =
    String(activeWorkspace || "").trim().toLowerCase() === "prospeccion"
      ? ""
      : "Si no estas ahi, cambiate a Prospeccion y te sigues guiando desde esa vista.";
  const messagesWorkspaceHint =
    String(activeWorkspace || "").trim().toLowerCase() === "mensajes"
      ? ""
      : "Si no estas ahi, cambiate a Mensajes y te sigues guiando desde esa vista.";

  if (!topicId) {
    return null;
  }

  const crmAction = [{ type: "workspace", workspace: "crm", label: "Ir a CRM" }];
  const agendaAction = [{ type: "workspace", workspace: "agenda", label: "Abrir Agenda" }];
  const prospectionAction = [{ type: "workspace", workspace: "prospeccion", label: "Ir a Prospeccion" }];
  const messagesAction = [{ type: "workspace", workspace: "mensajes", label: "Abrir Mensajes" }];
  const teamAction = isTeamManager ? [{ type: "workspace", workspace: "equipo", label: "Abrir Equipo" }] : [];
  const territoryAction = canUseTerritories
    ? [{ type: "workspace", workspace: "territorio", label: "Abrir Territorio" }]
    : [];

  switch (topicId) {
    case "crm_4_14":
      return {
        topicId,
        actions: crmAction,
        reply: construirCoachHelpStepsReply({
          intro: "Para encontrar un 4 en 14 dentro del CRM:",
          steps: [
            "Abre CRM.",
            "En Fuente elige 4 en 14.",
            "Usa Buscar rapido con nombre, telefono, ciudad o una nota del referido.",
            "Cada referido entra como fila propia; no veras una sola hoja completa.",
            "Si abres Detalle o tocas 4 en 14, vuelves a la hoja ligada a esa misma casa."
          ],
          outro: crmWorkspaceHint
        })
      };
    case "lead_destination_email":
      return {
        topicId,
        actions: isTeamManager ? prospectionAction : [],
        reply: isTeamManager
          ? construirCoachHelpStepsReply({
              intro: "Para mandarte copia de tus leads al correo:",
              steps: [
                "Abre Prospeccion.",
                "Baja a Destino de leads.",
                "En Tipo elige Mandarme copia a mi correo.",
                "Escribe el email donde quieres recibir esas copias.",
                "Toca Guardar destino y desde ahi las copias nuevas salen a ese correo."
              ]
            })
          : construirCoachHelpStepsReply({
              intro:
                "Esa parte la cambia quien administra la cuenta. Si eres novato o telemarketing, no la configuras desde tu portal.",
              steps: [
                "Pidele al dueno, distribuidor o junior que abra Prospeccion.",
                "En Destino de leads que elija Mandarme copia a mi correo.",
                "Que escriba el email y toque Guardar destino."
              ]
            })
      };
    case "agenda":
      return {
        topicId,
        actions: agendaAction,
        reply: construirCoachHelpStepsReply({
          intro: "Asi se trabaja la Agenda rapida:",
          steps: [
            "Abre Agenda.",
            "Hoy muestra lo urgente y Semana muestra lo que sigue.",
            "Desde cada cita puedes llamar, abrir mapa o copiar direccion.",
            "Usa Ya afuera, Entro a casa, No atendio, Reagendar, Follow up, Venta o No venta.",
            "Lo que marques en Agenda regresa al mismo registro del CRM."
          ],
          outro: agendaWorkspaceHint
        })
      };
    case "crm_workflow":
      return {
        topicId,
        actions: crmAction,
        reply: construirCoachHelpStepsReply({
          intro: "Asi se mueve una fila dentro del CRM:",
          steps: [
            "Busca la fila por nombre, telefono, ciudad o nota.",
            "Cambia Estado, Telemarketing, Representante, Proxima accion, Seguimiento o Nota rapida directo en la tabla.",
            "Usa Llamar, No atendio, Follow up o Cita para moverla sin abrir detalle.",
            "Toca Guardar si cambiaste campos inline.",
            "Abre Detalle solo si ocupas direccion, notas privadas o mas contexto."
          ],
          outro: crmWorkspaceHint
        })
      };
    case "team":
      return {
        topicId,
        actions: teamAction,
        reply: isTeamManager
          ? construirCoachHelpStepsReply({
              intro: "Para crear y mover subcuentas desde Equipo:",
              steps: [
                "Abre Equipo.",
                "Crea la subcuenta y elige si sera novato o telemarketing.",
                "Guarda el acceso temporal para esa persona.",
                "Si es telemarketing, entra por su portal de telemarketing.",
                "Si es novato, trabajara dentro de la cuenta principal segun sus permisos."
              ]
            })
          : construirCoachHelpStepsReply({
              intro: "Las subcuentas las crea quien administra el equipo.",
              steps: [
                "Si ocupas una nueva cuenta, escribela al dueno o al junior que maneja el equipo.",
                "Ellos la crean desde Equipo y te comparten tu acceso."
              ]
            })
      };
    case "territory":
      return {
        topicId,
        actions: territoryAction,
        reply: canUseTerritories
          ? construirCoachHelpStepsReply({
              intro: "Para mover cuentas propias dentro de Territorio:",
              steps: [
                "Abre Territorio.",
                "Crea o abre el territorio donde vas a trabajar.",
                "Invita por correo a distribuidor, junior o co admin.",
                "Si esa persona ya tiene cuenta, la invitacion le aparece dentro del Coach.",
                "Los leads propios siguen siendo de cada cuenta; el territorio solo comparte actividad y panel."
              ]
            })
          : construirCoachHelpStepsReply({
              intro: "Territorio solo aparece para cuentas propias, junior en adelante.",
              steps: [
                "Si tu cuenta no ve Territorio, todavia trabajas dentro de tu cuenta o de tu jefe.",
                "Cuando te suban a cuenta propia o junior, ahi ya puedes entrar a esa capa."
              ]
            })
      };
    case "messages":
      return {
        topicId,
        actions: messagesAction,
        reply: construirCoachHelpStepsReply({
          intro: "Mensajes tiene tres usos internos:",
          steps: [
            "Boletines para avisos de sistema, territorio o equipo.",
            "Soporte para escribir preguntas tecnicas sin salir del Coach.",
            "Chats directos para hablar con equipo y cuentas permitidas.",
            "Si ya leiste todo, usa Marcar todo como leido."
          ],
          outro: messagesWorkspaceHint
        })
      };
    case "prospection":
      return {
        topicId,
        actions: isTelemarketingPortal ? [{ type: "workspace", workspace: "crm", label: "Volver a CRM" }] : prospectionAction,
        reply: isTelemarketingPortal
          ? construirCoachHelpStepsReply({
              intro:
                "En tu portal de telemarketing lo principal es CRM y Agenda. Prospeccion la maneja quien administra la cuenta.",
              steps: [
                "Si ocupas un dato de Chef, contactos o destino de leads, pidelo al distribuidor o junior que administra la cuenta.",
                "Tu trabajo operativo vive en CRM y Agenda."
              ]
            })
          : construirCoachHelpStepsReply({
              intro: "En Prospeccion tienes estas piezas principales:",
              steps: [
                "Campana Chef para el primer mensaje a Contactos compartidos.",
                "Subir contactos para CSV, VCF o pegar nombres y telefonos.",
                "Captura propia y carpeta de leads para rifa y seguimiento.",
                "Destino de leads para correo personal, Google Sheets o webhook/CRM."
              ],
              outro: prospectionWorkspaceHint
            })
      };
    case "recruitment":
      return {
        topicId,
        actions: crmAction,
        reply: construirCoachHelpStepsReply({
          intro: "Las aplicaciones de trabajo ya entran al CRM sin mover nada manual:",
          steps: [
            "Abre CRM.",
            "En Fuente elige Reclutamiento.",
            "Desde ahi las trabajas igual que cualquier otra fila: estado, seguimiento, nota, cita y resultado."
          ],
          outro: crmWorkspaceHint
        })
      };
    case "manual_general":
    default:
      return {
        topicId: "manual_general",
        actions: isTelemarketingPortal
          ? [
              { type: "workspace", workspace: "crm", label: "Ir a CRM" },
              { type: "workspace", workspace: "agenda", label: "Abrir Agenda" },
              { type: "workspace", workspace: "mensajes", label: "Mensajes" }
            ]
          : [
              { type: "workspace", workspace: "prospeccion", label: "Prospeccion" },
              { type: "workspace", workspace: "crm", label: "CRM" },
              ...(isTeamManager ? [{ type: "workspace", workspace: "equipo", label: "Equipo" }] : [])
            ],
        reply: isTelemarketingPortal
          ? construirCoachHelpStepsReply({
              intro: "Si la pregunta es de uso del sistema, aqui te sirvo como copiloto del portal.",
              steps: [
                "CRM para buscar filas, dejar notas y mover estados.",
                "Agenda para ver hoy o semana y marcar resultados.",
                "Mensajes para boletines y soporte si tu cuenta lo tiene."
              ],
              outro:
                "Pruebame con algo como: como busco un 4 en 14, como marco una cita o como dejo un follow up."
            })
          : construirCoachHelpStepsReply({
              intro: "Si la pregunta es de uso del sistema, te la explico paso a paso.",
              steps: [
                "Prospeccion: Chef, Subir contactos, carpeta y destino de leads.",
                "CRM y Agenda: seguimiento, citas, resultados y ventas.",
                "Equipo y Territorio: subcuentas, invitaciones y actividad.",
                "Mensajes: boletines, chat interno y soporte."
              ],
              outro:
                "Pruebame con algo como: como agrego mi correo, como busco un 4 en 14 o como invito a alguien al territorio."
            })
      };
  }
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
      accountType: "owner",
      seatStatus: "active",
      subscriptionStatus: accesoDePrueba ? "test_access" : "inactive",
      subscriptionActive: false,
      updatedAt: new Date()
    });

    userDoc.billingOwnerUserId = userDoc._id;
    userDoc.teamOwnerUserId = userDoc._id;
    await userDoc.save();
    let profileDoc = await asegurarCoachDistributorProfile(userDoc);
    ({ userDoc, profileDoc } = await aplicarCoachAccountPreset(userDoc, profileDoc));

    await crearCoachSesion(req, res, userDoc._id);

    if (accesoDePrueba) {
      const userView = await construirCoachUserView(userDoc);
      return res.json({
        url: "/coach/app/",
        bypass: true,
        user: userView
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
      user: await construirCoachUserView(userDoc)
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
    let userDoc = await CoachUser.findOne({ email });

    if (!userDoc || !verificarPasswordSeguro(password, userDoc.passwordSalt, userDoc.passwordHash)) {
      return responderCoachError(res, 401, "Correo o contrasena incorrectos.");
    }

    userDoc = await asegurarCoachUserBase(userDoc);
    userDoc.lastLoginAt = new Date();
    userDoc.updatedAt = new Date();
    await userDoc.save();
    let profileDoc = await asegurarCoachDistributorProfile(userDoc);
    ({ userDoc, profileDoc } = await aplicarCoachAccountPreset(userDoc, profileDoc));
    await crearCoachSesion(req, res, userDoc._id);

    res.json({ user: await construirCoachUserView(userDoc) });
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

    let userDoc = await asegurarCoachUserBase(auth.user);

    const [profileDocRaw, analyticsDoc, networkSummary, leadMemory, returnAuth] = await Promise.all([
      CoachDistributorProfile.findOne({ userId: userDoc._id }),
      CoachDistributorAnalytics.findOne({ userId: userDoc._id }).lean(),
      coachTieneAccesoTotal(userDoc) ? obtenerCoachNetworkSummary() : Promise.resolve(null),
      coachTieneAccesoTotal(userDoc)
        ? obtenerMemoriaLeadRelacionada({
            mode: "coach",
            coachUser: userDoc
          })
        : Promise.resolve({ leadContext: null, repLeadSummary: null }),
      obtenerCoachReturnAuth(req)
    ]);

    let profileDoc = await asegurarCoachDistributorProfile(userDoc, profileDocRaw);
    ({ userDoc, profileDoc } = await aplicarCoachAccountPreset(userDoc, profileDoc));
    const returnUserView = returnAuth?.user ? await construirCoachUserView(returnAuth.user) : null;

    res.json({
      authenticated: true,
      stripeReady: stripeListoParaCheckout(),
      user: await construirCoachUserView(userDoc),
      profile: limpiarCoachProfile(profileDoc?.toObject ? profileDoc.toObject() : profileDoc, analyticsDoc),
      integrations: {
        highLevel: {
          readOnlyConfigured: highLevelReadOnlyEstaConfigurado(),
          syncEnabled: highLevelSyncEstaConfigurado(),
          locationId: HIGHLEVEL_LOCATION_ID || "",
          pipelineId: HIGHLEVEL_PIPELINE_ID || "",
          pipelineStageId: HIGHLEVEL_PIPELINE_STAGE_ID || "",
          webhookPath: "/webhooks/highlevel"
        }
      },
      sessionSwitch: {
        canReturn: Boolean(returnUserView),
        returnUser: returnUserView
      },
      networkSummary,
      repLeadSummary: leadMemory?.repLeadSummary || null,
      activeLeadContext: leadMemory?.leadContext || null
    });
  } catch (error) {
    console.error("Error obteniendo usuario Coach:", error.message);
    responderCoachError(res, 500, "No pude revisar tu cuenta.");
  }
});

app.get("/api/coach/integrations/highlevel", async (req, res) => {
  const auth = await requireCoachActivo(req, res);

  if (!auth) {
    return;
  }

  try {
    const ownerUserId = resolverCoachOwnerUserId(auth.user);
    const [campaignsResult, workflowsResult, recentLeadDocs] = await Promise.all([
      listarHighLevelCampaigns(),
      listarHighLevelWorkflows(),
      CoachLeadInbox.find({
        ownerUserId,
        $or: [{ highLevelContactId: { $ne: "" } }, { highLevelOpportunityId: { $ne: "" } }]
      })
        .sort({ highLevelLastWebhookAt: -1, highLevelLastSyncAt: -1, updatedAt: -1 })
        .limit(25)
        .lean()
    ]);

    res.json({
      configured: highLevelReadOnlyEstaConfigurado(),
      syncEnabled: highLevelSyncEstaConfigurado(),
      locationId: HIGHLEVEL_LOCATION_ID || "",
      pipelineId: HIGHLEVEL_PIPELINE_ID || "",
      pipelineStageId: HIGHLEVEL_PIPELINE_STAGE_ID || "",
      webhookPath: "/webhooks/highlevel",
      campaigns: campaignsResult.items || [],
      workflows: workflowsResult.items || [],
      campaignsError: campaignsResult.ok ? "" : campaignsResult.error || "",
      workflowsError: workflowsResult.ok ? "" : workflowsResult.error || "",
      recentLeads: recentLeadDocs.map(doc => limpiarCoachInboxLead(doc)).filter(Boolean)
    });
  } catch (error) {
    console.error("Error leyendo integracion HighLevel:", error.message);
    responderCoachError(res, 500, "No pude revisar la integracion de HighLevel.");
  }
});

app.get("/api/coach/team", async (req, res) => {
  const auth = await requireCoachTeamManager(req, res);

  if (!auth) {
    return;
  }

  try {
    const seatDocs = await CoachUser.find({
      teamOwnerUserId: auth.user._id,
      accountType: "seat"
    })
      .sort({ createdAt: -1, name: 1 })
      .lean();

    const seatIds = seatDocs.map(seat => seat._id).filter(Boolean);

    if (!seatIds.length) {
      return res.json({
        summary: construirCoachTeamSummary([]),
        seats: []
      });
    }

    const [profileDocs, leadCounts, surveyCounts, programCounts, applicationCounts] = await Promise.all([
      CoachDistributorProfile.find({ userId: { $in: seatIds } }).lean(),
      CoachLeadInbox.aggregate([
        {
          $match: {
            ownerUserId: auth.user._id,
            generatedByUserId: { $in: seatIds }
          }
        },
        { $group: { _id: "$generatedByUserId", total: { $sum: 1 } } }
      ]),
      CoachHealthSurvey.aggregate([
        {
          $match: {
            ownerUserId: auth.user._id,
            generatedByUserId: { $in: seatIds }
          }
        },
        { $group: { _id: "$generatedByUserId", total: { $sum: 1 } } }
      ]),
      CoachProgramSheet.aggregate([
        {
          $match: {
            ownerUserId: auth.user._id,
            generatedByUserId: { $in: seatIds }
          }
        },
        { $group: { _id: "$generatedByUserId", total: { $sum: 1 } } }
      ]),
      CoachRecruitmentApplication.aggregate([
        {
          $match: {
            ownerUserId: auth.user._id,
            generatedByUserId: { $in: seatIds }
          }
        },
        { $group: { _id: "$generatedByUserId", total: { $sum: 1 } } }
      ])
    ]);

    const profileMap = new Map(profileDocs.map(profile => [String(profile.userId), profile]));
    const leadMap = new Map(leadCounts.map(item => [String(item._id), Number(item.total || 0)]));
    const surveyMap = new Map(surveyCounts.map(item => [String(item._id), Number(item.total || 0)]));
    const programMap = new Map(programCounts.map(item => [String(item._id), Number(item.total || 0)]));
    const applicationMap = new Map(applicationCounts.map(item => [String(item._id), Number(item.total || 0)]));

    const seats = seatDocs
      .map(seatDoc =>
        limpiarCoachTeamSeat(seatDoc, profileMap.get(String(seatDoc._id)) || null, {
          leads: leadMap.get(String(seatDoc._id)) || 0,
          surveys: surveyMap.get(String(seatDoc._id)) || 0,
          programSheets: programMap.get(String(seatDoc._id)) || 0,
          applications: applicationMap.get(String(seatDoc._id)) || 0
        })
      )
      .filter(Boolean);

    res.json({
      summary: construirCoachTeamSummary(seats),
      seats
    });
  } catch (error) {
    console.error("Error cargando equipo del Coach:", error.message);
    responderCoachError(res, 500, "No pude cargar tu equipo en este momento.");
  }
});

app.post("/api/coach/team/seats", async (req, res) => {
  const auth = await requireCoachTeamManager(req, res);

  if (!auth) {
    return;
  }

  const name = String(req.body?.name || "").trim().slice(0, 120);
  const email = normalizarEmail(req.body?.email || "");
  const seatLabel = limpiarCoachSeatLabel(req.body?.seatLabel || "");
  const teamRole = normalizarCoachSeatManagedRole(req.body?.teamRole || "");

  if (!name) {
    return responderCoachError(res, 400, "El nombre de la subcuenta es requerido.");
  }

  if (!email) {
    return responderCoachError(res, 400, "El correo de la subcuenta es requerido.");
  }

  try {
    const existingUser = await CoachUser.findOne({ email }).select("_id");

    if (existingUser) {
      return responderCoachError(res, 409, "Ese correo ya tiene una cuenta creada.");
    }

    const temporaryPassword = generarCoachSeatPasswordTemporal();
    const passwordSeguro = crearPasswordSeguro(temporaryPassword);
    const now = new Date();
    const seatDoc = await CoachUser.create({
      name,
      email,
      passwordHash: passwordSeguro.hash,
      passwordSalt: passwordSeguro.salt,
      accountType: "seat",
      parentUserId: auth.user._id,
      billingOwnerUserId: auth.user._id,
      teamOwnerUserId: auth.user._id,
      sponsorUserId: auth.user.sponsorUserId || null,
      sponsorName: auth.user.sponsorName || "",
      sponsorCommissionRate: limpiarCoachCommissionRate(auth.user.sponsorCommissionRate || 0),
      seatStatus: "active",
      officeId: auth.user.officeId || "",
      territoryId: auth.user.territoryId || "",
      subscriptionStatus: "team_access",
      subscriptionActive: false,
      updatedAt: now
    });

    const seatProfile = await asegurarCoachDistributorProfile(seatDoc);

    if (seatLabel && seatProfile.seatLabel !== seatLabel) {
      seatProfile.seatLabel = seatLabel;
    }

    if (seatProfile.teamRole !== teamRole) {
      seatProfile.teamRole = teamRole;
    }

    seatProfile.updatedAt = now;
    await seatProfile.save();

    const cleanedSeat = limpiarCoachTeamSeat(seatDoc.toObject(), seatProfile.toObject(), {
      leads: 0,
      surveys: 0,
      programSheets: 0,
      applications: 0
    });

    res.json({
      seat: cleanedSeat,
      credentials: {
        email,
        temporaryPassword
      }
    });
  } catch (error) {
    console.error("Error creando subcuenta del Coach:", error.message);
    responderCoachError(res, 500, "No pude crear la subcuenta en este momento.");
  }
});

app.patch("/api/coach/team/seats/:userId", async (req, res) => {
  const auth = await requireCoachTeamManager(req, res);

  if (!auth) {
    return;
  }

  const userId = String(req.params?.userId || "").trim();

  if (!userId || !mongoose.Types.ObjectId.isValid(userId)) {
    return responderCoachError(res, 400, "Subcuenta invalida.");
  }

  const requestedStatus = normalizarCoachSeatStatus(req.body?.seatStatus || "");
  const seatLabel = limpiarCoachSeatLabel(req.body?.seatLabel || "");
  const teamRole = normalizarCoachSeatManagedRole(req.body?.teamRole || "");
  const shouldUpdateStatus = ["active", "paused", "closed"].includes(requestedStatus);
  const shouldUpdateRole = Object.prototype.hasOwnProperty.call(req.body || {}, "teamRole");

  if (!shouldUpdateStatus && !seatLabel && !shouldUpdateRole) {
    return responderCoachError(res, 400, "No recibi cambios para esa subcuenta.");
  }

  try {
    const seatDoc = await CoachUser.findOne({
      _id: userId,
      teamOwnerUserId: auth.user._id,
      accountType: "seat"
    });

    if (!seatDoc) {
      return responderCoachError(res, 404, "No encontre esa subcuenta.");
    }

    const now = new Date();

    if (shouldUpdateStatus) {
      seatDoc.seatStatus = requestedStatus;
    }

    seatDoc.updatedAt = now;
    await seatDoc.save();

    const seatProfile = await asegurarCoachDistributorProfile(seatDoc);

    let profileDirty = false;

    if (seatLabel && seatProfile.seatLabel !== seatLabel) {
      seatProfile.seatLabel = seatLabel;
      profileDirty = true;
    }

    if (shouldUpdateRole && seatProfile.teamRole !== teamRole) {
      seatProfile.teamRole = teamRole;
      profileDirty = true;
    }

    if (profileDirty) {
      seatProfile.updatedAt = now;
      await seatProfile.save();
    }

    res.json({
      seat: limpiarCoachTeamSeat(seatDoc.toObject(), seatProfile.toObject(), {
        leads: await CoachLeadInbox.countDocuments({
          ownerUserId: auth.user._id,
          generatedByUserId: seatDoc._id
        }),
        surveys: await CoachHealthSurvey.countDocuments({
          ownerUserId: auth.user._id,
          generatedByUserId: seatDoc._id
        }),
        programSheets: await CoachProgramSheet.countDocuments({
          ownerUserId: auth.user._id,
          generatedByUserId: seatDoc._id
        }),
        applications: await CoachRecruitmentApplication.countDocuments({
          ownerUserId: auth.user._id,
          generatedByUserId: seatDoc._id
        })
      })
    });
  } catch (error) {
    console.error("Error actualizando subcuenta del Coach:", error.message);
    responderCoachError(res, 500, "No pude actualizar la subcuenta en este momento.");
  }
});

app.post("/api/coach/team/seats/:userId/switch", async (req, res) => {
  const auth = await requireCoachTeamManager(req, res);

  if (!auth) {
    return;
  }

  const userId = String(req.params?.userId || "").trim();

  if (!userId || !mongoose.Types.ObjectId.isValid(userId)) {
    return responderCoachError(res, 400, "Subcuenta invalida.");
  }

  try {
    const cookies = parsearCookies(req.headers.cookie || "");
    const currentToken = String(cookies[COACH_SESSION_COOKIE] || "").trim();

    if (!currentToken) {
      return responderCoachError(res, 401, "Necesitas una sesion activa para cambiar de cuenta.");
    }

    const seatDoc = await CoachUser.findOne({
      _id: userId,
      teamOwnerUserId: auth.user._id,
      accountType: "seat",
      seatStatus: "active"
    });

    if (!seatDoc) {
      return responderCoachError(res, 404, "No encontre esa subcuenta activa.");
    }

    const seatProfile = await asegurarCoachDistributorProfile(seatDoc);
    const teamRole = normalizarCoachTeamRole(seatProfile?.teamRole || "", seatDoc);

    if (teamRole !== "telemarketing") {
      return responderCoachError(res, 400, "Por ahora este acceso directo solo esta disponible para telemarketing.");
    }

    setCoachReturnCookie(res, req, currentToken);
    await crearCoachSesion(req, res, seatDoc._id);

    res.json({
      ok: true,
      url: construirCoachHomePath(seatDoc, { teamRole }),
      seat: limpiarCoachTeamSeat(seatDoc.toObject(), seatProfile?.toObject ? seatProfile.toObject() : seatProfile, {})
    });
  } catch (error) {
    console.error("Error cambiando a subcuenta telemarketing:", error.message);
    responderCoachError(res, 500, "No pude abrir esa subcuenta en este momento.");
  }
});

app.post("/api/coach/session/return", async (req, res) => {
  try {
    const cookies = parsearCookies(req.headers.cookie || "");
    const currentToken = String(cookies[COACH_SESSION_COOKIE] || "").trim();
    const returnToken = String(cookies[COACH_RETURN_SESSION_COOKIE] || "").trim();

    if (!returnToken) {
      return responderCoachError(res, 400, "No encontre una cuenta principal para retomar.");
    }

    let returnAuth = await obtenerCoachReturnAuth(req);

    if (!returnAuth.user) {
      clearCoachReturnCookie(res, req);
      return responderCoachError(res, 400, "La cuenta original ya no esta disponible.");
    }

    let returnUserDoc = await asegurarCoachUserBase(returnAuth.user);

    if (!(await coachTieneAccesoOperativo(returnUserDoc))) {
      clearCoachReturnCookie(res, req);
      return responderCoachError(res, 403, "La cuenta principal ya no tiene acceso operativo.");
    }

    if (currentToken && currentToken !== returnToken) {
      await CoachSession.deleteOne({ tokenHash: hashTokenSeguro(currentToken) });
    }

    setCoachCookie(res, req, returnToken);
    clearCoachReturnCookie(res, req);
    returnAuth = await obtenerCoachAuthPorToken(returnToken, { touch: false });
    returnUserDoc = await asegurarCoachUserBase(returnAuth.user || returnUserDoc);
    const returnUserView = await construirCoachUserView(returnUserDoc);

    res.json({
      ok: true,
      url: returnUserView?.homePath || construirCoachHomePath(returnUserDoc),
      user: returnUserView
    });
  } catch (error) {
    console.error("Error regresando a cuenta principal del Coach:", error.message);
    responderCoachError(res, 500, "No pude regresar a tu cuenta principal.");
  }
});

app.get("/api/coach/territories", async (req, res) => {
  const auth = await requireCoachTerritoryUser(req, res);

  if (!auth) {
    return;
  }

  try {
    const workspace = await obtenerCoachTerritoryWorkspace(auth.user);
    res.json(workspace);
  } catch (error) {
    console.error("Error cargando territorios del Coach:", error.message);
    responderCoachError(res, 500, "No pude cargar tus territorios en este momento.");
  }
});

app.post("/api/coach/territories", async (req, res) => {
  const auth = await requireCoachTerritoryUser(req, res);

  if (!auth) {
    return;
  }

  const name = limpiarCoachTerritoryText(req.body?.name || "", 100);
  const officeId = limpiarCoachTerritoryText(req.body?.officeId || auth.user.officeId || "", 60);
  const territoryId = limpiarCoachTerritoryText(req.body?.territoryId || auth.user.territoryId || "", 60);

  if (!name) {
    return responderCoachError(res, 400, "Ponle un nombre al territorio.");
  }

  try {
    const now = new Date();
    const territoryDoc = await CoachTerritory.create({
      name,
      officeId,
      territoryId,
      createdByUserId: auth.user._id,
      createdByName: auth.user.name || auth.user.email || "",
      status: "active",
      updatedAt: now
    });

    await CoachTerritoryMembership.create({
      territoryId: territoryDoc._id,
      userId: auth.user._id,
      role: "owner",
      status: "active",
      invitedByUserId: auth.user._id,
      invitedByName: auth.user.name || auth.user.email || "",
      updatedAt: now
    });

    if (!auth.user.officeId && officeId) {
      auth.user.officeId = officeId;
    }

    if (!auth.user.territoryId && territoryId) {
      auth.user.territoryId = territoryId;
    }

    if (auth.user.isModified?.()) {
      auth.user.updatedAt = now;
      await auth.user.save();
    }

    res.json({
      territory: {
        id: String(territoryDoc._id),
        name: territoryDoc.name || "",
        officeId: territoryDoc.officeId || "",
        territoryId: territoryDoc.territoryId || ""
      }
    });
  } catch (error) {
    console.error("Error creando territorio del Coach:", error.message);
    responderCoachError(res, 500, "No pude crear el territorio en este momento.");
  }
});

app.post("/api/coach/territories/:territoryId/invites", async (req, res) => {
  const auth = await requireCoachTerritoryUser(req, res);

  if (!auth) {
    return;
  }

  const territoryId = String(req.params?.territoryId || "").trim();
  const inviteeEmail = normalizarEmail(req.body?.email || "");
  const role = normalizarCoachTerritoryRole(req.body?.role || "member");
  const note = limpiarCoachTerritoryText(req.body?.note || "", 220);

  if (!territoryId || !mongoose.Types.ObjectId.isValid(territoryId)) {
    return responderCoachError(res, 400, "Territorio invalido.");
  }

  if (!inviteeEmail) {
    return responderCoachError(res, 400, "Pon el correo de la persona que quieres invitar.");
  }

  try {
    const [territoryDoc, myMembership] = await Promise.all([
      CoachTerritory.findOne({ _id: territoryId, status: "active" }).lean(),
      CoachTerritoryMembership.findOne({
        territoryId,
        userId: auth.user._id,
        status: "active"
      }).lean()
    ]);

    if (!territoryDoc) {
      return responderCoachError(res, 404, "No encontre ese territorio.");
    }

    if (!myMembership || !coachTerritoryRolePuedeAdministrar(myMembership.role)) {
      return responderCoachError(res, 403, "No tienes permiso para invitar personas a este territorio.");
    }

    const existingUser = await CoachUser.findOne({ email: inviteeEmail }).select("_id accountType name email").lean();

    if (existingUser && normalizarCoachAccountType(existingUser.accountType || "owner") === "seat") {
      return responderCoachError(
        res,
        409,
        "Ese correo ya pertenece a una subcuenta. Usa Equipo si quieres mover novatos."
      );
    }

    if (existingUser?._id) {
      const existingMembership = await CoachTerritoryMembership.findOne({
        territoryId,
        userId: existingUser._id,
        status: "active"
      }).lean();

      if (existingMembership) {
        return responderCoachError(res, 409, "Esa cuenta ya forma parte de este territorio.");
      }
    }

    const now = new Date();
    let inviteDoc = await CoachTerritoryInvite.findOne({
      territoryId,
      inviteeEmail,
      status: "pending"
    });

    if (inviteDoc) {
      inviteDoc.role = role;
      inviteDoc.note = note;
      inviteDoc.inviteeUserId = existingUser?._id || null;
      inviteDoc.invitedByUserId = auth.user._id;
      inviteDoc.invitedByName = auth.user.name || auth.user.email || "";
      inviteDoc.updatedAt = now;
      await inviteDoc.save();
    } else {
      inviteDoc = await CoachTerritoryInvite.create({
        territoryId,
        inviteeEmail,
        inviteeUserId: existingUser?._id || null,
        role,
        status: "pending",
        invitedByUserId: auth.user._id,
        invitedByName: auth.user.name || auth.user.email || "",
        note,
        updatedAt: now
      });
    }

    res.json({
      invite: limpiarCoachTerritoryInvite(inviteDoc.toObject ? inviteDoc.toObject() : inviteDoc, territoryDoc, {
        includeTerritory: true
      })
    });
  } catch (error) {
    console.error("Error invitando cuenta al territorio:", error.message);
    responderCoachError(res, 500, "No pude crear la invitacion en este momento.");
  }
});

app.post("/api/coach/territory-invites/:inviteId/respond", async (req, res) => {
  const auth = await requireCoachTerritoryUser(req, res);

  if (!auth) {
    return;
  }

  const inviteId = String(req.params?.inviteId || "").trim();
  const action = String(req.body?.action || "").trim().toLowerCase();

  if (!inviteId || !mongoose.Types.ObjectId.isValid(inviteId)) {
    return responderCoachError(res, 400, "Invitacion invalida.");
  }

  if (!["accept", "reject"].includes(action)) {
    return responderCoachError(res, 400, "Accion invalida.");
  }

  try {
    const inviteDoc = await CoachTerritoryInvite.findById(inviteId);

    if (!inviteDoc || normalizarCoachTerritoryInviteStatus(inviteDoc.status || "pending") !== "pending") {
      return responderCoachError(res, 404, "No encontre esa invitacion pendiente.");
    }

    const userEmail = normalizarEmail(auth.user.email || "");

    if (inviteDoc.inviteeEmail !== userEmail) {
      return responderCoachError(res, 403, "Esa invitacion no pertenece a tu cuenta.");
    }

    const territoryDoc = await CoachTerritory.findOne({
      _id: inviteDoc.territoryId,
      status: "active"
    });

    if (!territoryDoc) {
      return responderCoachError(res, 404, "El territorio ya no esta disponible.");
    }

    const now = new Date();

    if (action === "reject") {
      inviteDoc.status = "rejected";
      inviteDoc.inviteeUserId = auth.user._id;
      inviteDoc.respondedAt = now;
      inviteDoc.updatedAt = now;
      await inviteDoc.save();
      return res.json({ ok: true, status: "rejected" });
    }

    let membershipDoc = await CoachTerritoryMembership.findOne({
      territoryId: territoryDoc._id,
      userId: auth.user._id
    });

    if (membershipDoc) {
      membershipDoc.role = normalizarCoachTerritoryRole(inviteDoc.role || "member");
      membershipDoc.status = "active";
      membershipDoc.invitedByUserId = inviteDoc.invitedByUserId || null;
      membershipDoc.invitedByName = inviteDoc.invitedByName || "";
      membershipDoc.updatedAt = now;
      await membershipDoc.save();
    } else {
      membershipDoc = await CoachTerritoryMembership.create({
        territoryId: territoryDoc._id,
        userId: auth.user._id,
        role: normalizarCoachTerritoryRole(inviteDoc.role || "member"),
        status: "active",
        invitedByUserId: inviteDoc.invitedByUserId || null,
        invitedByName: inviteDoc.invitedByName || "",
        updatedAt: now
      });
    }

    inviteDoc.status = "accepted";
    inviteDoc.inviteeUserId = auth.user._id;
    inviteDoc.respondedAt = now;
    inviteDoc.updatedAt = now;
    await inviteDoc.save();

    let userDirty = false;

    if (!auth.user.officeId && territoryDoc.officeId) {
      auth.user.officeId = territoryDoc.officeId;
      userDirty = true;
    }

    if (!auth.user.territoryId && territoryDoc.territoryId) {
      auth.user.territoryId = territoryDoc.territoryId;
      userDirty = true;
    }

    if (userDirty) {
      auth.user.updatedAt = now;
      await auth.user.save();
    }

    const profileDoc = await asegurarCoachDistributorProfile(auth.user);
    const role = normalizarCoachTerritoryRole(inviteDoc.role || "member");

    if (role === "junior" && profileDoc.teamRole !== "junior") {
      profileDoc.teamRole = "junior";
      profileDoc.updatedAt = now;
      await profileDoc.save();
    }

    res.json({
      ok: true,
      status: "accepted",
      membershipId: String(membershipDoc._id)
    });
  } catch (error) {
    console.error("Error respondiendo invitacion territorial:", error.message);
    responderCoachError(res, 500, "No pude responder la invitacion en este momento.");
  }
});

app.get("/api/coach/messages/overview", async (req, res) => {
  const auth = await requireCoachActivo(req, res);

  if (!auth) {
    return;
  }

  try {
    const overview = await obtenerCoachMessagesOverview(auth.user);
    res.json(overview);
  } catch (error) {
    console.error("Error cargando mensajes del Coach:", error.message);
    responderCoachError(res, 500, "No pude cargar tus mensajes en este momento.");
  }
});

app.post("/api/coach/messages/read-all", async (req, res) => {
  const auth = await requireCoachActivo(req, res);

  if (!auth) {
    return;
  }

  try {
    const overview = await obtenerCoachMessagesOverview(auth.user);
    const unreadAnnouncementIds = overview.announcements.filter(item => !item.read).map(item => item.id).filter(Boolean);

    if (unreadAnnouncementIds.length) {
      const now = new Date();
      await Promise.all(
        unreadAnnouncementIds.map(announcementId =>
          CoachAnnouncementReceipt.updateOne(
            {
              announcementId,
              userId: auth.user._id
            },
            {
              $set: {
                readAt: now
              },
              $setOnInsert: {
                createdAt: now
              }
            },
            { upsert: true }
          )
        )
      );
    }

    const threadDoc = await asegurarCoachSupportThread(auth.user);
    if (threadDoc && threadDoc.ownerUnreadCount > 0) {
      threadDoc.ownerUnreadCount = 0;
      threadDoc.updatedAt = new Date();
      await threadDoc.save();
    }

    await CoachDirectThread.updateMany(
      {
        participantUserIds: auth.user._id,
        unreadByUserIds: auth.user._id
      },
      {
        $pull: {
          unreadByUserIds: auth.user._id
        },
        $set: {
          updatedAt: new Date()
        }
      }
    );

    res.json({ ok: true });
  } catch (error) {
    console.error("Error marcando mensajes del Coach como leidos:", error.message);
    responderCoachError(res, 500, "No pude marcar tus mensajes en este momento.");
  }
});

app.post("/api/coach/support/messages", async (req, res) => {
  const auth = await requireCoachActivo(req, res);

  if (!auth) {
    return;
  }

  const body = limpiarCoachSupportMessageBody(req.body?.body || "");

  if (!body) {
    return responderCoachError(res, 400, "Escribe tu mensaje antes de enviarlo.");
  }

  try {
    const now = new Date();
    const threadDoc = await asegurarCoachSupportThread(auth.user);

    await CoachSupportMessage.create({
      threadId: threadDoc._id,
      senderUserId: auth.user._id,
      senderName: auth.user.name || auth.user.email || "",
      senderScope: "coach_user",
      body
    });

    threadDoc.ownerName = auth.user.name || auth.user.email || "";
    threadDoc.ownerEmail = auth.user.email || "";
    threadDoc.status = "open";
    threadDoc.supportUnreadCount = Number(threadDoc.supportUnreadCount || 0) + 1;
    threadDoc.lastMessageAt = now;
    threadDoc.lastMessagePreview = truncarTextoPrompt(body, 180);
    threadDoc.updatedAt = now;
    await threadDoc.save();

    res.json({
      ok: true,
      thread: limpiarCoachSupportThread(threadDoc)
    });
  } catch (error) {
    console.error("Error enviando mensaje de soporte del Coach:", error.message);
    responderCoachError(res, 500, "No pude enviar tu mensaje en este momento.");
  }
});

app.post("/api/coach/announcements", async (req, res) => {
  const auth = await requireCoachActivo(req, res);

  if (!auth) {
    return;
  }

  const scopeType = normalizarCoachAnnouncementScopeType(req.body?.scopeType || "team");
  const territoryId = String(req.body?.territoryId || "").trim();
  const title = limpiarCoachAnnouncementTitle(req.body?.title || "");
  const body = limpiarCoachAnnouncementBody(req.body?.body || "");
  const priority = normalizarCoachAnnouncementPriority(req.body?.priority || "normal");

  if (!["team", "territory"].includes(scopeType)) {
    return responderCoachError(res, 400, "Solo puedes mandar boletines al equipo o al territorio.");
  }

  if (!title) {
    return responderCoachError(res, 400, "Pon un titulo para el boletin.");
  }

  if (!body) {
    return responderCoachError(res, 400, "Escribe el contenido del boletin.");
  }

  try {
    let territoryDoc = null;
    let audienceOwner = null;

    if (scopeType === "team") {
      if (!esCoachTeamManager(auth.user)) {
        return responderCoachError(res, 403, "Solo quien administra el equipo puede mandar ese boletin.");
      }

      audienceOwner = await resolverCoachAudienceOwner(auth.user);

      if (!audienceOwner?._id) {
        return responderCoachError(res, 400, "No pude encontrar el equipo que va a recibir ese boletin.");
      }
    }

    if (scopeType === "territory") {
      if (!territoryId || !mongoose.Types.ObjectId.isValid(territoryId)) {
        return responderCoachError(res, 400, "Selecciona un territorio valido.");
      }

      territoryDoc = await CoachTerritory.findOne({
        _id: territoryId,
        status: "active"
      }).lean();

      if (!territoryDoc) {
        return responderCoachError(res, 404, "No encontre ese territorio.");
      }

      const membershipDoc = await CoachTerritoryMembership.findOne({
        territoryId: territoryDoc._id,
        userId: auth.user._id,
        status: "active"
      }).lean();

      if (!membershipDoc || !coachTerritoryRolePuedeAdministrar(membershipDoc.role || "member")) {
        return responderCoachError(res, 403, "Solo quien administra ese territorio puede mandar ese boletin.");
      }
    }

    const now = new Date();
    const announcementDoc = await CoachAnnouncement.create({
      authorUserId: auth.user._id,
      authorName: auth.user.name || auth.user.email || "Coach",
      scopeType,
      territoryId: scopeType === "territory" ? territoryDoc._id : null,
      territoryName: scopeType === "territory" ? territoryDoc.name || "" : "",
      teamOwnerUserId: scopeType === "team" ? audienceOwner._id : null,
      teamOwnerName:
        scopeType === "team"
          ? audienceOwner.name || audienceOwner.email || auth.user.name || auth.user.email || "Equipo"
          : "",
      title,
      body,
      priority,
      status: "active",
      updatedAt: now
    });

    res.json({
      ok: true,
      announcement: limpiarCoachAnnouncement(announcementDoc.toObject ? announcementDoc.toObject() : announcementDoc)
    });
  } catch (error) {
    console.error("Error creando boletin interno del Coach:", error.message);
    responderCoachError(res, 500, "No pude mandar ese boletin en este momento.");
  }
});

app.post("/api/coach/direct/threads", async (req, res) => {
  const auth = await requireCoachActivo(req, res);

  if (!auth) {
    return;
  }

  const targetUserId = String(req.body?.targetUserId || "").trim();

  if (!targetUserId || !mongoose.Types.ObjectId.isValid(targetUserId)) {
    return responderCoachError(res, 400, "Selecciona una cuenta valida para abrir el chat.");
  }

  try {
    const contacts = await obtenerCoachReachableContacts(auth.user);
    const targetContact = contacts.find(item => item.id === targetUserId);

    if (!targetContact) {
      return responderCoachError(res, 403, "Esa cuenta no esta disponible para chat interno.");
    }

    const threadDoc = await encontrarOCrearCoachDirectThread(auth.user, targetUserId);

    if (!threadDoc?._id) {
      return responderCoachError(res, 500, "No pude abrir ese chat en este momento.");
    }

    await CoachDirectThread.updateOne(
      {
        _id: threadDoc._id
      },
      {
        $pull: {
          unreadByUserIds: auth.user._id
        },
        $set: {
          updatedAt: new Date()
        }
      }
    );

    res.json({
      ok: true,
      threadId: String(threadDoc._id),
      contact: targetContact
    });
  } catch (error) {
    console.error("Error abriendo chat interno del Coach:", error.message);
    responderCoachError(res, 500, "No pude abrir ese chat en este momento.");
  }
});

app.post("/api/coach/direct/threads/:threadId/read", async (req, res) => {
  const auth = await requireCoachActivo(req, res);

  if (!auth) {
    return;
  }

  const threadId = String(req.params?.threadId || "").trim();

  if (!threadId || !mongoose.Types.ObjectId.isValid(threadId)) {
    return responderCoachError(res, 400, "No encontre ese chat.");
  }

  try {
    const threadDoc = await CoachDirectThread.findOne({
      _id: threadId,
      participantUserIds: auth.user._id
    });

    if (!threadDoc) {
      return responderCoachError(res, 404, "No encontre ese chat.");
    }

    threadDoc.unreadByUserIds = Array.isArray(threadDoc.unreadByUserIds)
      ? threadDoc.unreadByUserIds.filter(item => String(item) !== String(auth.user._id))
      : [];
    threadDoc.updatedAt = new Date();
    await threadDoc.save();

    res.json({ ok: true, threadId: String(threadDoc._id) });
  } catch (error) {
    console.error("Error marcando chat interno como leido:", error.message);
    responderCoachError(res, 500, "No pude actualizar ese chat.");
  }
});

app.post("/api/coach/direct/threads/:threadId/messages", async (req, res) => {
  const auth = await requireCoachActivo(req, res);

  if (!auth) {
    return;
  }

  const threadId = String(req.params?.threadId || "").trim();
  const body = limpiarCoachSupportMessageBody(req.body?.body || "");

  if (!threadId || !mongoose.Types.ObjectId.isValid(threadId)) {
    return responderCoachError(res, 400, "No encontre ese chat.");
  }

  if (!body) {
    return responderCoachError(res, 400, "Escribe tu mensaje antes de enviarlo.");
  }

  try {
    const threadDoc = await CoachDirectThread.findOne({
      _id: threadId,
      participantUserIds: auth.user._id
    });

    if (!threadDoc) {
      return responderCoachError(res, 404, "No encontre ese chat.");
    }

    const participantIds = Array.isArray(threadDoc.participantUserIds)
      ? threadDoc.participantUserIds.map(item => String(item))
      : [];
    const targetUserId = participantIds.find(item => item && item !== String(auth.user._id));

    if (!targetUserId) {
      return responderCoachError(res, 400, "Ese chat no tiene un destinatario valido.");
    }

    const now = new Date();

    await CoachDirectMessage.create({
      threadId: threadDoc._id,
      senderUserId: auth.user._id,
      senderName: auth.user.name || auth.user.email || "",
      body
    });

    threadDoc.lastMessageAt = now;
    threadDoc.lastMessagePreview = truncarTextoPrompt(body, 180);
    threadDoc.unreadByUserIds = participantIds
      .filter(item => item !== String(auth.user._id))
      .map(item => new mongoose.Types.ObjectId(item));
    threadDoc.updatedAt = now;
    await threadDoc.save();

    res.json({
      ok: true,
      threadId: String(threadDoc._id)
    });
  } catch (error) {
    console.error("Error enviando chat interno del Coach:", error.message);
    responderCoachError(res, 500, "No pude enviar ese mensaje en este momento.");
  }
});

app.get("/api/coach/crm/records", async (req, res) => {
  const auth = await requireCoachActivo(req, res);

  if (!auth) {
    return;
  }

  try {
    const sourceTypeRaw = String(req.query?.sourceType || "").trim().toLowerCase();
    const statusRaw = String(req.query?.status || "").trim().toLowerCase();
    const assignedToUserId = String(req.query?.assignedToUserId || "").trim();
    const allowedStatuses = [
      "nuevo",
      "intentando",
      "no_atendio",
      "seguimiento",
      "cita_agendada",
      "reagendada",
      "ya_afuera",
      "entro_a_casa",
      "demo_hecha",
      "venta",
      "no_venta"
    ];
    const data = await obtenerCoachCrmWorkspace(auth.user, {
      sourceType: ["lead", "programa_4_en_14", "reclutamiento"].includes(sourceTypeRaw) ? sourceTypeRaw : "",
      status: allowedStatuses.includes(statusRaw) ? statusRaw : "",
      assignedToUserId
    });

    res.json(data);
  } catch (error) {
    console.error("Error cargando CRM del Coach:", error.stack || error.message);

    if (String(req.query?.debug || "").trim() === "1" && coachTieneAccesoTotal(auth.user)) {
      return res.status(500).json({
        error: "No pude cargar el CRM en este momento.",
        detail: error?.message || "Error desconocido"
      });
    }

    responderCoachError(res, 500, "No pude cargar el CRM en este momento.");
  }
});

app.get("/api/coach/crm/records/:recordId", async (req, res) => {
  const auth = await requireCoachActivo(req, res);

  if (!auth) {
    return;
  }

  try {
    const detail = await obtenerCoachCrmRecordDetail(auth.user, String(req.params?.recordId || "").trim());

    if (!detail?.record) {
      return responderCoachError(res, 404, "No encontre ese registro del CRM.");
    }

    res.json(detail);
  } catch (error) {
    console.error("Error cargando detalle del CRM:", error.message);
    responderCoachError(res, 500, "No pude cargar ese registro del CRM.");
  }
});

app.get("/api/coach/agenda", async (req, res) => {
  const auth = await requireCoachActivo(req, res);

  if (!auth) {
    return;
  }

  try {
    const data = await obtenerCoachAgendaWorkspace(auth.user);
    res.json(data);
  } catch (error) {
    console.error("Error cargando agenda del Coach:", error.stack || error.message);

    if (String(req.query?.debug || "").trim() === "1" && coachTieneAccesoTotal(auth.user)) {
      return res.status(500).json({
        error: "No pude cargar la agenda en este momento.",
        detail: error?.message || "Error desconocido"
      });
    }

    responderCoachError(res, 500, "No pude cargar la agenda en este momento.");
  }
});

app.patch("/api/coach/crm/records/:recordId", async (req, res) => {
  const auth = await requireCoachActivo(req, res);

  if (!auth) {
    return;
  }

  const recordId = String(req.params?.recordId || "").trim();

  if (!recordId || !mongoose.Types.ObjectId.isValid(recordId)) {
    return responderCoachError(res, 400, "Registro de CRM invalido.");
  }

  try {
    const recordQuery = await construirCoachCrmViewerQuery(auth.user, {
      _id: recordId
    });
    const recordDoc = await CoachCrmRecord.findOne(recordQuery);

    if (!recordDoc) {
      return responderCoachError(res, 404, "No encontre ese registro del CRM.");
    }

    const assignableContacts = await obtenerCoachCrmAssignableContacts(auth.user);
    const contactMap = new Map(assignableContacts.map(item => [String(item.id || ""), item]));
    const updates = {};
    const activityLines = [];
    const now = new Date();

    if (Object.prototype.hasOwnProperty.call(req.body || {}, "status")) {
      const nextStatus = normalizarCoachCrmStatus(req.body?.status || "nuevo");
      recordDoc.status = nextStatus;
      recordDoc.statusColor = resolverCoachCrmStatusColor(nextStatus);
      recordDoc.closedAt = ["venta", "no_venta"].includes(nextStatus) ? now : null;
      recordDoc.saleResult = nextStatus === "venta" ? "venta" : nextStatus === "no_venta" ? "no_venta" : "";

      if (nextStatus === "venta") {
        recordDoc.soldAt = now;
      } else if (nextStatus !== "venta") {
        recordDoc.soldAt = null;
      }

      updates.status = nextStatus;
      activityLines.push(`Estado CRM: ${formatearCoachCrmStatusLabel(nextStatus)}.`);
    }

    if (Object.prototype.hasOwnProperty.call(req.body || {}, "nextAction")) {
      const nextAction = normalizarCoachLeadNextAction(req.body?.nextAction || "");
      recordDoc.nextAction = nextAction;
      updates.nextAction = nextAction;
    }

    if (Object.prototype.hasOwnProperty.call(req.body || {}, "nextActionAt")) {
      const nextActionAt = parseCoachLeadNextActionAt(req.body?.nextActionAt);
      recordDoc.nextActionAt = nextActionAt;
      recordDoc.appointmentAt = ["cita_agendada", "reagendada"].includes(recordDoc.status) ? nextActionAt : recordDoc.appointmentAt;
      updates.nextActionAt = nextActionAt;
      if (nextActionAt) {
        activityLines.push(`Seguimiento: ${nextActionAt.toLocaleString("en-US", {
          month: "2-digit",
          day: "2-digit",
          year: "numeric",
          hour: "numeric",
          minute: "2-digit"
        })}.`);
      }
    }

    if (Object.prototype.hasOwnProperty.call(req.body || {}, "address")) {
      const nextAddress = truncarCoachCrmText(req.body?.address || "", 240);
      const inferredLocation = inferCoachCityZipFromAddress(nextAddress);
      recordDoc.address = nextAddress;
      updates.address = nextAddress;

      if (inferredLocation.city) {
        recordDoc.city = inferredLocation.city;
        updates.city = inferredLocation.city;
      }

      if (inferredLocation.zipCode) {
        recordDoc.zipCode = inferredLocation.zipCode;
        updates.zipCode = inferredLocation.zipCode;
      }
    }

    if (Object.prototype.hasOwnProperty.call(req.body || {}, "briefHistory")) {
      recordDoc.briefHistory = truncarCoachCrmText(req.body?.briefHistory || "", 320);
    }

    if (Object.prototype.hasOwnProperty.call(req.body || {}, "privateNotes")) {
      recordDoc.privateNotes = truncarCoachCrmText(req.body?.privateNotes || "", 1200);
      updates.privateNotesFull = recordDoc.privateNotes;
    }

    if (Object.prototype.hasOwnProperty.call(req.body || {}, "saleAmount")) {
      recordDoc.saleAmount = limpiarCoachDemoOutcomeAmount(req.body?.saleAmount || 0);
      if (recordDoc.saleAmount > 0) {
        recordDoc.saleResult = recordDoc.status === "venta" ? "venta" : recordDoc.saleResult || "";
      }
    }

    if (Object.prototype.hasOwnProperty.call(req.body || {}, "assignedTelemarketerUserId")) {
      const nextAssigneeId = String(req.body?.assignedTelemarketerUserId || "").trim();
      if (nextAssigneeId && !contactMap.has(nextAssigneeId)) {
        return responderCoachError(res, 400, "Selecciona un telemarketer valido.");
      }
      const nextAssignee = nextAssigneeId ? contactMap.get(nextAssigneeId) : null;
      recordDoc.assignedTelemarketerUserId = nextAssigneeId && mongoose.Types.ObjectId.isValid(nextAssigneeId)
        ? new mongoose.Types.ObjectId(nextAssigneeId)
        : null;
      recordDoc.assignedTelemarketerName = nextAssignee?.name || "";
      activityLines.push(
        `Telemarketing: ${nextAssignee?.name || "Sin asignar"}.`
      );
    }

    if (Object.prototype.hasOwnProperty.call(req.body || {}, "appointmentRepUserId")) {
      const nextRepId = String(req.body?.appointmentRepUserId || "").trim();
      if (nextRepId && !contactMap.has(nextRepId)) {
        return responderCoachError(res, 400, "Selecciona un representante valido.");
      }
      const nextRep = nextRepId ? contactMap.get(nextRepId) : null;
      recordDoc.appointmentRepUserId =
        nextRepId && mongoose.Types.ObjectId.isValid(nextRepId) ? new mongoose.Types.ObjectId(nextRepId) : null;
      recordDoc.appointmentRepName = nextRep?.name || "";
      activityLines.push(`Representante: ${nextRep?.name || "Sin asignar"}.`);
    }

    const note = truncarCoachCrmText(req.body?.lastNote || "", 240);
    if (note) {
      recordDoc.lastNote = note;
      recordDoc.privateNotes = recordDoc.privateNotes ? `${recordDoc.privateNotes}\n\n${note}`.slice(0, 1200) : note;
      updates.note = note;
      activityLines.push(`Nota: ${note}`);
    }

    if (activityLines.length || note) {
      recordDoc.lastContactAt = now;
    }

    recordDoc.updatedAt = now;
    await recordDoc.save();

    if (recordDoc.sourceType === "lead") {
      await aplicarCoachCrmActualizacionALeadSource(recordDoc, updates);
    } else if (recordDoc.sourceType === "programa_4_en_14") {
      await aplicarCoachCrmActualizacionAProgramaSource(recordDoc, updates);
    }

    if (activityLines.length) {
      await registrarCoachCrmActivity(recordDoc, auth.user, "update", activityLines.join(" "), {
        status: updates.status || "",
        nextAction: updates.nextAction || "",
        nextActionAt: updates.nextActionAt || null
      });
    }

    const detail = await obtenerCoachCrmRecordDetail(auth.user, recordId);
    res.json(detail);
  } catch (error) {
    console.error("Error actualizando CRM del Coach:", error.message);
    responderCoachError(res, 500, "No pude guardar ese seguimiento del CRM.");
  }
});

app.put("/api/coach/lead-destination", async (req, res) => {
  const auth = await requireCoachTeamManager(req, res);

  if (!auth) {
    return;
  }

  const nextType = normalizarCoachLeadDestinationType(req.body?.type || "carpeta_privada");
  const nextLabel = String(req.body?.label || "").trim().slice(0, 80);
  const nextUrl = limpiarUrlExterna(req.body?.url || "");
  const nextEmail = normalizarEmail(req.body?.email || "");

  if (nextType === "correo_personal" && !nextEmail) {
    return responderCoachError(res, 400, "Pon el correo donde quieres recibir tus leads.");
  }

  if (["google_sheets", "webhook_crm"].includes(nextType) && !nextUrl) {
    return responderCoachError(res, 400, "Pon la URL de tu destino para guardar esta conexion.");
  }

  try {
    const now = new Date();
    const profileDoc = await asegurarCoachDistributorProfile(auth.user);
    profileDoc.leadDestinationType = nextType;
    profileDoc.leadDestinationLabel = nextLabel;
    profileDoc.leadDestinationUrl = ["google_sheets", "webhook_crm"].includes(nextType) ? nextUrl : "";
    profileDoc.leadDestinationEmail = nextType === "correo_personal" ? nextEmail : "";
    profileDoc.leadDestinationUpdatedAt = now;
    profileDoc.updatedAt = now;
    await profileDoc.save();

    res.json({
      destination: limpiarCoachLeadDestination(profileDoc),
      profile: limpiarCoachProfile(profileDoc, null)
    });
  } catch (error) {
    console.error("Error guardando destino de leads del Coach:", error.message);
    responderCoachError(res, 500, "No pude guardar el destino de leads.");
  }
});

app.get("/api/coach/catalog-prices", async (req, res) => {
  const auth = await requireCoachActivo(req, res);

  if (!auth) {
    return;
  }

  try {
    const query = String(req.query?.query || "").trim();

    if (query.length < 3) {
      return res.json({ matches: [] });
    }

    const result = construirPreciosRelevantesCoach(query);

    res.json({
      matches: Array.isArray(result.coincidencias) ? result.coincidencias : []
    });
  } catch (error) {
    console.error("Error buscando precios del catalogo del Coach:", error.message);
    responderCoachError(res, 500, "No pude revisar el catalogo ahorita.");
  }
});

app.get("/api/coach/leads", async (req, res) => {
  const auth = await requireCoachActivo(req, res);

  if (!auth) {
    return;
  }

  try {
    const leadDocs = await CoachLeadInbox.find(construirCoachWorkspaceQuery(auth.user))
      .sort({ createdAt: -1 })
      .limit(300)
      .lean();
    const leads = leadDocs.map(limpiarCoachInboxLead).filter(Boolean);

    res.json({
      summary: construirCoachLeadInboxSummary(leads),
      leads
    });
  } catch (error) {
    console.error("Error obteniendo carpeta de leads del Coach:", error.message);
    responderCoachError(res, 500, "No pude cargar tu carpeta de leads.");
  }
});

app.get("/api/coach/campaigns/chef-intro", async (req, res) => {
  const auth = await requireCoachActivo(req, res);

  if (!auth) {
    return;
  }

  try {
    const snapshot = await obtenerCoachChefCampaignSnapshot(auth.user);
    res.json({
      totalImported: snapshot.totalImported,
      alreadyMessaged: snapshot.alreadyMessaged,
      eligibleCount: snapshot.eligibleCount,
      sample: snapshot.sample,
      smsEnabled: twilioSmsEstaConfigurado(),
      aiReplyEnabled: TWILIO_SMS_AI_REPLY_ENABLED,
      calendlyEnabled: Boolean(limpiarUrlExterna(CALENDLY_CHEF_URL))
    });
  } catch (error) {
    console.error("Error cargando campana Chef:", error.message);
    responderCoachError(res, 500, "No pude cargar la campana del Chef.");
  }
});

app.post("/api/coach/campaigns/chef-intro/send", async (req, res) => {
  const auth = await requireCoachActivo(req, res);

  if (!auth) {
    return;
  }

  if (!twilioSmsEstaConfigurado()) {
    return responderCoachError(res, 503, "Twilio SMS todavia no esta configurado.");
  }

  try {
    const snapshot = await obtenerCoachChefCampaignSnapshot(auth.user);
    const requestedLimit = Number.parseInt(req.body?.limit || "", 10);
    const limit = Math.max(1, Math.min(Number.isFinite(requestedLimit) ? requestedLimit : 25, 200));
    const template = String(req.body?.messageTemplate || "").trim();
    const leadsToSend = snapshot.eligibleLeads.slice(0, limit);

    if (!leadsToSend.length) {
      return responderCoachError(res, 400, "No encontre contactos compartidos pendientes para esta campana.");
    }

    let sent = 0;
    let failed = 0;
    const errors = [];

    for (const lead of leadsToSend) {
      const message = renderCoachChefCampaignMessage(template, lead);
      const smsResult = await enviarTwilioSms({
        to: lead.phone || "",
        body: message
      });

      if (!smsResult.ok) {
        failed += 1;

        if (errors.length < 5) {
          errors.push(`${lead.fullName || "Contacto"}: ${smsResult.error || "sin detalle"}`);
        }
        continue;
      }

      const leadDoc = await CoachLeadInbox.findById(lead._id);

      if (leadDoc) {
        await registrarActividadSmsCoachLead(leadDoc, {
          direction: "saliente",
          message,
          sid: smsResult.sid || "",
          intent: "chef_outreach_campaign"
        });
      }

      sent += 1;
    }

    const nextSnapshot = await obtenerCoachChefCampaignSnapshot(auth.user);

    res.json({
      ok: true,
      sent,
      failed,
      attempted: leadsToSend.length,
      errors,
      remainingEligible: nextSnapshot.eligibleCount,
      totalImported: nextSnapshot.totalImported,
      alreadyMessaged: nextSnapshot.alreadyMessaged
    });
  } catch (error) {
    console.error("Error enviando campana Chef:", error.message);
    responderCoachError(res, 500, "No pude enviar la campana del Chef en este momento.");
  }
});

app.post("/api/coach/leads", async (req, res) => {
  const auth = await requireCoachActivo(req, res);

  if (!auth) {
    return;
  }

  try {
    const profileDoc = await obtenerCoachOwnerProfile(auth.user, { lean: true });
    const result = await guardarCoachInboxLead({
      userDoc: auth.user,
      profileDoc,
      payload: req.body || {},
      sendDestination: true
    });

    res.json({
      lead: result.lead,
      duplicate: result.duplicate,
      delivery: result.delivery
    });
  } catch (error) {
    console.error("Error guardando lead del Coach:", error.message);
    responderCoachError(res, error.status || 500, error.message || "No pude guardar el lead en este momento.");
  }
});

app.get("/api/coach/recruitment-applications", async (req, res) => {
  const auth = await requireCoachActivo(req, res);

  if (!auth) {
    return;
  }

  try {
    const applicationDocs = await CoachRecruitmentApplication.find(construirCoachWorkspaceQuery(auth.user))
      .sort({ updatedAt: -1, createdAt: -1 })
      .limit(300)
      .lean();

    res.json({
      applications: applicationDocs.map(limpiarCoachRecruitmentApplication).filter(Boolean)
    });
  } catch (error) {
    console.error("Error cargando aplicaciones del Coach:", error.message);
    responderCoachError(res, 500, "No pude cargar tu carpeta de aplicaciones.");
  }
});

app.post("/api/coach/recruitment-applications", async (req, res) => {
  const auth = await requireCoachActivo(req, res);

  if (!auth) {
    return;
  }

  const applicationId = String(req.body?.applicationId || "").trim();
  const fullName =
    seleccionarNombreConfiable(req.body?.fullName || "") || limpiarCoachHealthText(req.body?.fullName || "", 120);
  const phone = normalizePhone(req.body?.phone || "");
  const email = normalizarEmail(req.body?.email || "");
  const drives = limpiarCoachHealthYesNo(req.body?.drives || "");
  const hasCar = limpiarCoachHealthYesNo(req.body?.hasCar || "");
  const customerServiceExperience = limpiarCoachHealthYesNo(req.body?.customerServiceExperience || "");
  const workPreference = limpiarCoachHealthText(req.body?.workPreference || "", 80);
  const salesExperience = limpiarCoachHealthYesNo(req.body?.salesExperience || "");
  const about = limpiarCoachHealthText(req.body?.about || "", 700);

  if (!fullName) {
    return responderCoachError(res, 400, "El nombre es requerido.");
  }

  if (!phone) {
    return responderCoachError(res, 400, "El telefono es requerido.");
  }

  if (applicationId && !mongoose.Types.ObjectId.isValid(applicationId)) {
    return responderCoachError(res, 400, "Aplicacion invalida.");
  }

  try {
    const now = new Date();
    const profileDoc = await obtenerCoachOwnerProfile(auth.user, { lean: true });
    const ownership = await construirCoachOwnershipSnapshot(auth.user);
    const applicationPayload = {
      ownerUserId: ownership.ownerUserId,
      ownerEmail: ownership.ownerEmail || "",
      ownerName: ownership.ownerName || "",
      generatedByUserId: ownership.generatedByUserId,
      generatedByName: ownership.generatedByName || "",
      generatedByAccountType: ownership.generatedByAccountType,
      fullName,
      phone,
      email,
      drives,
      hasCar,
      customerServiceExperience,
      workPreference,
      salesExperience,
      about,
      updatedAt: now
    };

    applicationPayload.summary = construirCoachRecruitmentApplicationSummary(applicationPayload);

    let applicationDoc = null;
    let created = false;

    if (applicationId) {
      applicationDoc = await CoachRecruitmentApplication.findOne(
        construirCoachWorkspaceQuery(auth.user, { _id: applicationId })
      );

      if (!applicationDoc) {
        return responderCoachError(res, 404, "No encontre esa aplicacion.");
      }

      Object.assign(applicationDoc, applicationPayload);
      await applicationDoc.save();
    } else {
      created = true;
      applicationDoc = await CoachRecruitmentApplication.create({
        ...applicationPayload,
        createdAt: now
      });
    }

    const cleanedApplication = limpiarCoachRecruitmentApplication(applicationDoc.toObject());
    try {
      await sincronizarCoachRecruitmentApplicationACrmRecord(auth.user, applicationDoc);
    } catch (error) {
      console.error("Error sincronizando aplicacion de reclutamiento al CRM:", error.message);
    }
    const delivery = await programarEnvioCoachRecruitmentApplicationADestino(auth.user, profileDoc, cleanedApplication);

    res.json({
      created,
      application: cleanedApplication,
      delivery
    });
  } catch (error) {
    console.error("Error guardando aplicacion de reclutamiento del Coach:", error.message);
    responderCoachError(res, 500, "No pude guardar la aplicacion en este momento.");
  }
});

app.get("/api/coach/health-surveys", async (req, res) => {
  const auth = await requireCoachActivo(req, res);

  if (!auth) {
    return;
  }

  try {
    const surveyDocs = await CoachHealthSurvey.find(construirCoachWorkspaceQuery(auth.user))
      .sort({ updatedAt: -1, createdAt: -1 })
      .limit(300)
      .lean();

    res.json({
      surveys: surveyDocs.map(limpiarCoachHealthSurvey).filter(Boolean)
    });
  } catch (error) {
    console.error("Error cargando encuestas de salud del Coach:", error.message);
    responderCoachError(res, 500, "No pude cargar tu carpeta de encuestas.");
  }
});

app.post("/api/coach/health-surveys", async (req, res) => {
  const auth = await requireCoachActivo(req, res);

  if (!auth) {
    return;
  }

  const surveyId = String(req.body?.surveyId || "").trim();
  const fullName =
    seleccionarNombreConfiable(req.body?.fullName || "") || limpiarCoachHealthText(req.body?.fullName || "", 120);
  const phone = normalizePhone(req.body?.phone || "");
  const secondName =
    seleccionarNombreConfiable(req.body?.secondName || "") || limpiarCoachHealthText(req.body?.secondName || "", 120);
  const workingStatus = limpiarCoachHealthText(req.body?.workingStatus || "", 80);
  const heardRoyal = limpiarCoachHealthText(req.body?.heardRoyal || "", 260);
  const familyPriority = limpiarCoachHealthText(req.body?.familyPriority || "", 80);
  const qualityReason = limpiarCoachHealthText(req.body?.qualityReason || "", 120);
  const productLikingScore = limpiarCoachHealthNumber(req.body?.productLikingScore || "", 1, 10);
  const cooksForCount = limpiarCoachHealthNumber(req.body?.cooksForCount || "", 1, 50);
  const foodSpendWeekly = limpiarCoachHealthText(req.body?.foodSpendWeekly || "", 60);
  const mealPrepTime = limpiarCoachHealthText(req.body?.mealPrepTime || "", 80);
  const cookingMaterials = limpiarCoachHealthList(req.body?.cookingMaterials || [], { maxItems: 6, maxLength: 60 });
  const familyConditions = limpiarCoachHealthList(req.body?.familyConditions || [], { maxItems: 8, maxLength: 60 });
  const lowFatHealthy = limpiarCoachHealthYesNo(req.body?.lowFatHealthy || "");
  const lowFatHealthyReason = limpiarCoachHealthText(req.body?.lowFatHealthyReason || "", 220);
  const cookwareAffects = limpiarCoachHealthYesNo(req.body?.cookwareAffects || "");
  const cookwareAffectsReason = limpiarCoachHealthText(req.body?.cookwareAffectsReason || "", 220);
  const qualityInterest = limpiarCoachHealthYesNo(req.body?.qualityInterest || "");
  const qualityInterestReason = limpiarCoachHealthText(req.body?.qualityInterestReason || "", 220);
  const drinkingWaterType = limpiarCoachHealthText(req.body?.drinkingWaterType || "", 80);
  const cookingWaterType = limpiarCoachHealthText(req.body?.cookingWaterType || "", 80);
  const tapWaterConcern = limpiarCoachHealthYesNo(req.body?.tapWaterConcern || "");
  const waterSpendWeekly = limpiarCoachHealthText(req.body?.waterSpendWeekly || "", 60);
  const likesNaturalJuices = limpiarCoachHealthYesNo(req.body?.likesNaturalJuices || "");
  const juiceFrequency = limpiarCoachHealthText(req.body?.juiceFrequency || "", 60);
  const creditProblems = limpiarCoachHealthYesNo(req.body?.creditProblems || "");
  const creditImproveInterest = limpiarCoachHealthYesNo(req.body?.creditImproveInterest || "");
  const familyHealthInvestment = limpiarCoachHealthYesNo(req.body?.familyHealthInvestment || "");
  const weeklyBudget = limpiarCoachHealthText(req.body?.weeklyBudget || "", 40);
  const monthlyBudget = limpiarCoachHealthText(req.body?.monthlyBudget || "", 40);
  const topProducts = limpiarCoachHealthList(req.body?.topProducts || [], { maxItems: 3, maxLength: 120 });
  const activeCrmRecordId = String(req.body?.activeCrmRecordId || "").trim();

  if (!fullName) {
    return responderCoachError(res, 400, "El nombre es requerido.");
  }

  if (!phone) {
    return responderCoachError(res, 400, "El telefono es requerido.");
  }

  if (surveyId && !mongoose.Types.ObjectId.isValid(surveyId)) {
    return responderCoachError(res, 400, "Encuesta invalida.");
  }

  try {
    const now = new Date();
    const ownership = await construirCoachOwnershipSnapshot(auth.user);
    const linkedCrmRecordDoc =
      activeCrmRecordId && mongoose.Types.ObjectId.isValid(activeCrmRecordId)
        ? await CoachCrmRecord.findOne(
            construirCoachCrmWorkspaceQuery(auth.user, {
              _id: activeCrmRecordId
            })
          )
        : null;
    const surveyPayload = {
      ownerUserId: ownership.ownerUserId,
      ownerEmail: ownership.ownerEmail || "",
      ownerName: ownership.ownerName || "",
      generatedByUserId: ownership.generatedByUserId,
      generatedByName: ownership.generatedByName || "",
      generatedByAccountType: ownership.generatedByAccountType,
      linkedCrmRecordId: linkedCrmRecordDoc?._id || null,
      fullName,
      phone,
      secondName,
      workingStatus,
      heardRoyal,
      familyPriority,
      qualityReason,
      productLikingScore,
      cooksForCount,
      foodSpendWeekly,
      mealPrepTime,
      cookingMaterials,
      familyConditions,
      lowFatHealthy,
      lowFatHealthyReason,
      cookwareAffects,
      cookwareAffectsReason,
      qualityInterest,
      qualityInterestReason,
      drinkingWaterType,
      cookingWaterType,
      tapWaterConcern,
      waterSpendWeekly,
      likesNaturalJuices,
      juiceFrequency,
      creditProblems,
      creditImproveInterest,
      familyHealthInvestment,
      weeklyBudget,
      monthlyBudget,
      topProducts,
      updatedAt: now
    };

    surveyPayload.summary = construirCoachHealthSurveySummary(surveyPayload);

    let surveyDoc = null;
    let created = false;

    if (surveyId) {
      surveyDoc = await CoachHealthSurvey.findOne(construirCoachWorkspaceQuery(auth.user, { _id: surveyId }));

      if (!surveyDoc) {
        return responderCoachError(res, 404, "No encontre esa encuesta.");
      }

      Object.assign(surveyDoc, surveyPayload);
      await surveyDoc.save();
    } else {
      created = true;
      surveyDoc = await CoachHealthSurvey.create({
        ...surveyPayload,
        createdAt: now
      });
    }

    if (linkedCrmRecordDoc) {
      await vincularCoachCrmConEncuesta(linkedCrmRecordDoc, surveyDoc, auth.user);
    }

    res.json({
      created,
      survey: limpiarCoachHealthSurvey(surveyDoc.toObject())
    });
  } catch (error) {
    console.error("Error guardando encuesta de salud del Coach:", error.message);
    responderCoachError(res, 500, "No pude guardar la encuesta en este momento.");
  }
});

app.get("/api/coach/program-4-in-14", async (req, res) => {
  const auth = await requireCoachActivo(req, res);

  if (!auth) {
    return;
  }

  try {
    const sheetDocs = await CoachProgramSheet.find(
      construirCoachWorkspaceQuery(auth.user, {
        programType: "4_en_14"
      })
    )
      .sort({ updatedAt: -1, createdAt: -1 })
      .limit(100)
      .lean();

    res.json({
      sheets: sheetDocs.map(limpiarCoachProgramSheet).filter(Boolean)
    });
  } catch (error) {
    console.error("Error cargando hojas 4 en 14 del Coach:", error.message);
    responderCoachError(res, 500, "No pude cargar tus hojas 4 en 14.");
  }
});

app.post("/api/coach/program-4-in-14", async (req, res) => {
  const auth = await requireCoachActivo(req, res);

  if (!auth) {
    return;
  }

  const hostName =
    seleccionarNombreConfiable(req.body?.hostName || "") || String(req.body?.hostName || "").trim();
  const hostPhone = normalizePhone(req.body?.hostPhone || "");
  const giftSelected = String(req.body?.giftSelected || "").trim().slice(0, 120);
  const representativeName =
    seleccionarNombreConfiable(req.body?.representativeName || "") ||
    String(req.body?.representativeName || "").trim().slice(0, 100);
  const representativePhone = normalizePhone(req.body?.representativePhone || "");
  const startWindow = String(req.body?.startWindow || "").trim().slice(0, 120);
  const notes = String(req.body?.notes || "").trim().slice(0, 500);
  const activeCrmRecordId = String(req.body?.activeCrmRecordId || "").trim();
  const referrals = Array.isArray(req.body?.referrals)
    ? req.body.referrals.map(limpiarCoachProgramReferral).filter(Boolean).slice(0, 11)
    : [];

  if (!hostName) {
    return responderCoachError(res, 400, "El nombre del anfitrion es requerido.");
  }

  if (!hostPhone) {
    return responderCoachError(res, 400, "El telefono del anfitrion es requerido.");
  }

  if (!referrals.length) {
    return responderCoachError(res, 400, "Pon por lo menos un referido con nombre y telefono.");
  }

  try {
    const profileDoc = await obtenerCoachOwnerProfile(auth.user, { lean: true });
    const now = new Date();
    const ownership = await construirCoachOwnershipSnapshot(auth.user);
    const linkedCrmRecordDoc =
      activeCrmRecordId && mongoose.Types.ObjectId.isValid(activeCrmRecordId)
        ? await CoachCrmRecord.findOne(
            construirCoachCrmWorkspaceQuery(auth.user, {
              _id: activeCrmRecordId
            })
          )
        : null;
    const sheetBase = {
      ownerUserId: ownership.ownerUserId,
      ownerEmail: ownership.ownerEmail || "",
      ownerName: ownership.ownerName || "",
      generatedByUserId: ownership.generatedByUserId,
      generatedByName: ownership.generatedByName || "",
      generatedByAccountType: ownership.generatedByAccountType,
      linkedCrmRecordId: linkedCrmRecordDoc?._id || null,
      programType: "4_en_14",
      hostName,
      hostPhone,
      giftSelected,
      representativeName,
      representativePhone,
      startWindow,
      notes,
      referrals,
      referralCount: referrals.length,
      updatedAt: now
    };

    const sheetDoc = await CoachProgramSheet.create({
      ...sheetBase,
      summary: construirCoachProgramSheetSummary(sheetBase)
    });

    const createdLeadIds = [];
    const createdLeads = [];
    let duplicates = 0;

    for (let index = 0; index < referrals.length; index += 1) {
      const referral = referrals[index];
      const leadResult = await guardarCoachInboxLead({
        userDoc: auth.user,
        profileDoc,
        sendDestination: false,
        payload: {
          fullName: referral.fullName,
          phone: referral.phone,
          interest: giftSelected
            ? `Programa 4 en 14 · ${giftSelected}`
            : "Programa 4 en 14",
          source: "programa_4_en_14",
          notes: construirNotasLeadDesdePrograma414(sheetBase, referral, index),
          consentGiven: true
        }
      });

      if (leadResult?.leadDoc?._id) {
        createdLeadIds.push(leadResult.leadDoc._id);
        if (sheetDoc.referrals[index]) {
          sheetDoc.referrals[index].createdLeadId = leadResult.leadDoc._id;
        }
      }

      if (leadResult?.lead) {
        createdLeads.push(leadResult.lead);
      }

      if (leadResult?.duplicate) {
        duplicates += 1;
      }
    }

    sheetDoc.createdLeadIds = createdLeadIds;
    sheetDoc.summary = construirCoachProgramSheetSummary({
      ...sheetBase,
      referralCount: referrals.length
    });
    await sheetDoc.save();

    if (linkedCrmRecordDoc) {
      await vincularCoachCrmConPrograma(linkedCrmRecordDoc, sheetDoc, auth.user);
    }

    try {
      await sincronizarCoachProgramSheetACrmRecords(auth.user, sheetDoc);
    } catch (error) {
      console.error("Error sincronizando programa 4 en 14 al CRM:", error.message);
    }

    const delivery = await programarEnvioCoachProgramSheetADestino(
      auth.user,
      profileDoc,
      sheetDoc.toObject(),
      createdLeads
    );

    res.json({
      sheet: limpiarCoachProgramSheet(sheetDoc.toObject()),
      createdLeadCount: createdLeads.length,
      duplicateCount: duplicates,
      delivery
    });
  } catch (error) {
    console.error("Error guardando hoja 4 en 14 del Coach:", error.message);
    responderCoachError(res, 500, error.message || "No pude guardar la hoja 4 en 14 en este momento.");
  }
});

app.patch("/api/coach/program-4-in-14/:sheetId/referrals/:referralIndex/instant-call", async (req, res) => {
  const auth = await requireCoachActivo(req, res);

  if (!auth) {
    return;
  }

  const sheetId = String(req.params?.sheetId || "").trim();
  const referralIndex = Number.parseInt(req.params?.referralIndex || "", 10);
  const activate = Boolean(req.body?.activate);
  const instantCallStatus = normalizarCoachProgramInstantStatus(req.body?.instantCallStatus || "");
  const instantCallNotes = String(req.body?.instantCallNotes || "").trim().slice(0, 320);
  const appointmentDetails = String(req.body?.appointmentDetails || "").trim().slice(0, 180);

  if (!sheetId || !mongoose.Types.ObjectId.isValid(sheetId)) {
    return responderCoachError(res, 400, "Hoja invalida.");
  }

  if (!Number.isInteger(referralIndex) || referralIndex < 0) {
    return responderCoachError(res, 400, "Referido invalido.");
  }

  try {
    const sheetDoc = await CoachProgramSheet.findOne({
      ...construirCoachWorkspaceQuery(auth.user),
      _id: sheetId
    });

    if (!sheetDoc) {
      return responderCoachError(res, 404, "No encontre esa hoja.");
    }

    if (!Array.isArray(sheetDoc.referrals) || !sheetDoc.referrals[referralIndex]) {
      return responderCoachError(res, 404, "No encontre ese referido.");
    }

    const referral = sheetDoc.referrals[referralIndex];
    const now = new Date();
    const noteParts = [
      `Cita instantanea 4 en 14.`,
      `Anfitrion: ${sheetDoc.hostName || "Sin nombre"}.`,
      `Referido: ${referral.fullName || "Sin nombre"}.`
    ];

    if (activate) {
      referral.selectedForInstantCallAt = now;

      if (!referral.instantCallStatus) {
        referral.instantCallStatus = "seleccionado";
      }
    }

    if (instantCallStatus) {
      referral.instantCallStatus = instantCallStatus;
      referral.lastOutcomeAt = now;
    }

    if (Object.prototype.hasOwnProperty.call(req.body || {}, "instantCallNotes")) {
      referral.instantCallNotes = instantCallNotes;
    }

    if (Object.prototype.hasOwnProperty.call(req.body || {}, "appointmentDetails")) {
      referral.appointmentDetails = appointmentDetails;
    }

    if (instantCallStatus) {
      noteParts.push(`Resultado: ${instantCallStatus.replace(/_/g, " ")}.`);
    }

    if (appointmentDetails) {
      noteParts.push(`Detalle: ${appointmentDetails}.`);
    }

    if (instantCallNotes) {
      noteParts.push(`Notas: ${instantCallNotes}.`);
    }

    sheetDoc.updatedAt = now;
    await sheetDoc.save();
    let linkedLeadDoc = null;

    if (referral.createdLeadId && mongoose.Types.ObjectId.isValid(referral.createdLeadId)) {
      linkedLeadDoc = await CoachLeadInbox.findOne({
        ...construirCoachWorkspaceQuery(auth.user),
        _id: referral.createdLeadId
      });

      if (linkedLeadDoc) {
        if (instantCallStatus === "cita_lograda") {
          linkedLeadDoc.status = "agendado";
          linkedLeadDoc.nextAction = "cita";
          linkedLeadDoc.lastContactAt = now;
          linkedLeadDoc.lastStatusChangeAt = now;
        } else if (instantCallStatus === "llamar_despues") {
          linkedLeadDoc.status = "contactado";
          linkedLeadDoc.nextAction = "seguimiento";
          linkedLeadDoc.lastContactAt = now;
          linkedLeadDoc.lastStatusChangeAt = now;
        } else if (instantCallStatus === "no_contesto") {
          linkedLeadDoc.status = "contactado";
          linkedLeadDoc.nextAction = "llamar";
          linkedLeadDoc.lastContactAt = now;
          linkedLeadDoc.lastStatusChangeAt = now;
        } else if (instantCallStatus === "no_quiso") {
          linkedLeadDoc.status = "archivado";
          linkedLeadDoc.nextAction = "";
          linkedLeadDoc.lastContactAt = now;
          linkedLeadDoc.lastStatusChangeAt = now;
        }

        if (noteParts.length) {
          const appendedNote = noteParts.join(" ").trim();
          linkedLeadDoc.notes = linkedLeadDoc.notes ? `${linkedLeadDoc.notes}\n\n${appendedNote}` : appendedNote;
        }

        linkedLeadDoc.summary = construirCoachLeadSummary(linkedLeadDoc);
        linkedLeadDoc.updatedAt = now;
        await linkedLeadDoc.save();
      }
    }

    const crmRecordDoc = await CoachCrmRecord.findOne(
      construirCoachCrmWorkspaceQuery(auth.user, {
        sourceType: "programa_4_en_14",
        sourceParentId: sheetId,
        sourceSubIndex: referralIndex
      })
    );

    if (crmRecordDoc) {
      const nextCrmStatus = resolverCoachCrmStatusDesdePrograma414(referral, linkedLeadDoc);
      const appendedNote = noteParts.join(" ").trim();

      crmRecordDoc.status = nextCrmStatus;
      crmRecordDoc.statusColor = resolverCoachCrmStatusColor(nextCrmStatus);
      crmRecordDoc.latestProgramSheetId = sheetDoc._id;
      crmRecordDoc.latestProgramSummary = truncarCoachCrmText(sheetDoc.summary || "", 240);
      crmRecordDoc.lastNote = truncarCoachCrmText(appendedNote, 240);
      crmRecordDoc.lastContactAt = now;
      crmRecordDoc.updatedAt = now;
      await crmRecordDoc.save();

      await registrarCoachCrmActivity(
        crmRecordDoc,
        auth.user,
        "program_414_result",
        truncarCoachCrmText(appendedNote, 320),
        {
          sheetId,
          referralIndex,
          instantCallStatus
        }
      );
    }

    const cleanedSheet = limpiarCoachProgramSheet(sheetDoc.toObject());
    res.json({
      sheet: cleanedSheet,
      referral: cleanedSheet.referrals[referralIndex] || null
    });
  } catch (error) {
    console.error("Error actualizando cita instantanea 4 en 14:", error.message);
    responderCoachError(res, 500, "No pude guardar ese resultado en este momento.");
  }
});

app.post("/api/coach/leads/:leadId/sms", async (req, res) => {
  const auth = await requireCoachActivo(req, res);

  if (!auth) {
    return;
  }

  const leadId = String(req.params?.leadId || "").trim();
  const message = cleanText(req.body?.message || "").slice(0, 1600);

  if (!mongoose.Types.ObjectId.isValid(leadId)) {
    return responderCoachError(res, 400, "Lead invalido.");
  }

  if (!message) {
    return responderCoachError(res, 400, "Escribe el mensaje que quieres mandar.");
  }

  if (!twilioSmsEstaConfigurado()) {
    return responderCoachError(res, 503, "Twilio SMS todavia no esta configurado.");
  }

  try {
    const leadDoc = await CoachLeadInbox.findOne(construirCoachWorkspaceQuery(auth.user, { _id: leadId }));

    if (!leadDoc) {
      return responderCoachError(res, 404, "No encontre ese lead.");
    }

    const smsResult = await enviarTwilioSms({
      to: leadDoc.phone || "",
      body: message
    });

    if (!smsResult.ok) {
      return responderCoachError(res, 502, smsResult.error || "No pude enviar el SMS.");
    }

    await registrarActividadSmsCoachLead(leadDoc, {
      direction: "saliente",
      message,
      sid: smsResult.sid || "",
      userDoc: auth.user
    });

    res.json({
      ok: true,
      sid: smsResult.sid || "",
      lead: limpiarCoachInboxLead(leadDoc.toObject())
    });
  } catch (error) {
    console.error("Error enviando SMS del Coach:", error.message);
    responderCoachError(res, 500, "No pude enviar el SMS en este momento.");
  }
});

app.patch("/api/coach/leads/:leadId", async (req, res) => {
  const auth = await requireCoachActivo(req, res);

  if (!auth) {
    return;
  }

  const leadId = String(req.params?.leadId || "").trim();

  if (!leadId || !mongoose.Types.ObjectId.isValid(leadId)) {
    return responderCoachError(res, 400, "Lead invalido.");
  }

  const nextStatus = normalizarCoachLeadStatus(req.body?.status || "nuevo");
  const noteToAppend = String(req.body?.notes || "").trim().slice(0, 300);
  const hasNextAction = Object.prototype.hasOwnProperty.call(req.body || {}, "nextAction");
  const hasNextActionAt = Object.prototype.hasOwnProperty.call(req.body || {}, "nextActionAt");
  const nextAction = normalizarCoachLeadNextAction(req.body?.nextAction || "");
  const nextActionAt = parseCoachLeadNextActionAt(req.body?.nextActionAt);

  try {
    const leadDoc = await CoachLeadInbox.findOne(construirCoachWorkspaceQuery(auth.user, { _id: leadId }));

    if (!leadDoc) {
      return responderCoachError(res, 404, "No encontre ese lead.");
    }

    const now = new Date();
    leadDoc.status = nextStatus;
    leadDoc.lastStatusChangeAt = now;

    if (hasNextAction) {
      leadDoc.nextAction = nextAction;
    }

    if (hasNextActionAt) {
      leadDoc.nextActionAt = nextActionAt;
    }

    if (["contactado", "agendado", "cliente"].includes(nextStatus)) {
      leadDoc.lastContactAt = now;
    }

    if (noteToAppend) {
      leadDoc.notes = leadDoc.notes ? `${leadDoc.notes}\n\n${noteToAppend}` : noteToAppend;
    }

    leadDoc.summary = construirCoachLeadSummary(leadDoc);
    leadDoc.updatedAt = now;
    await leadDoc.save();

    try {
      await sincronizarCoachLeadInboxACrmRecord(auth.user, leadDoc);
    } catch (error) {
      console.error("Error sincronizando actualizacion de lead al CRM:", error.message);
    }

    res.json({
      lead: limpiarCoachInboxLead(leadDoc.toObject())
    });
  } catch (error) {
    console.error("Error actualizando lead del Coach:", error.message);
    responderCoachError(res, 500, "No pude actualizar ese lead.");
  }
});

app.get("/api/coach/demo-outcomes", async (req, res) => {
  const auth = await requireCoachActivo(req, res);

  if (!auth) {
    return;
  }

  try {
    const ownerUserId = resolverCoachOwnerUserId(auth.user);
    const managesTeam = esCoachTeamManager(auth.user);
    const personalMatch = {
      ownerUserId,
      generatedByUserId: auth.user._id
    };
    const teamMatch = { ownerUserId };

    const [personalSummary, teamSummary, personalDocs, teamDocs] = await Promise.all([
      obtenerCoachDemoOutcomeSummary(personalMatch),
      obtenerCoachDemoOutcomeSummary(teamMatch),
      CoachDemoOutcome.find(personalMatch)
        .sort({ createdAt: -1 })
        .limit(8)
        .lean(),
      managesTeam
        ? CoachDemoOutcome.find(teamMatch)
            .sort({ createdAt: -1 })
            .limit(12)
            .lean()
        : Promise.resolve([])
    ]);

    res.json({
      viewerMode: managesTeam ? "manager" : "seat",
      personalSummary,
      teamSummary,
      outcomes: personalDocs.map(doc => limpiarCoachDemoOutcome(doc)),
      teamOutcomes: managesTeam
        ? teamDocs.map(doc => limpiarCoachDemoOutcome(doc, { includeGenerator: true }))
        : []
    });
  } catch (error) {
    console.error("Error cargando resultados de demo del Coach:", error.message);
    responderCoachError(res, 500, "No pude cargar los resultados de demo.");
  }
});

app.post("/api/coach/demo-outcomes", async (req, res) => {
  const auth = await requireCoachActivo(req, res);

  if (!auth) {
    return;
  }

  const resultType = normalizarCoachDemoOutcomeType(req.body?.resultType || "");
  const saleAmount = limpiarCoachDemoOutcomeAmount(req.body?.saleAmount || 0);
  const products = limpiarCoachDemoOutcomeProducts(req.body?.products || []);
  const privateReason = limpiarCoachDemoOutcomeReason(req.body?.privateReason || "");
  const activeWorkspace = truncarTextoPrompt(String(req.body?.activeWorkspace || "").trim(), 40);
  const activeDemoStage = truncarTextoPrompt(String(req.body?.activeDemoStage || "").trim(), 80);
  const activeDemoStageLabel = truncarTextoPrompt(String(req.body?.activeDemoStageLabel || "").trim(), 120);
  const recentCoachEvents = Array.isArray(req.body?.recentCoachEvents)
    ? req.body.recentCoachEvents.map(limpiarCoachDemoEventPrompt).filter(Boolean).slice(0, 6)
    : [];
  const activeHealthSurveyId =
    typeof req.body?.activeHealthSurveyId === "string" ? req.body.activeHealthSurveyId.trim() : "";
  const activeProgram414SheetId =
    typeof req.body?.activeProgram414SheetId === "string" ? req.body.activeProgram414SheetId.trim() : "";
  const activeProgram414ReferralIndex = Number.parseInt(req.body?.activeProgram414ReferralIndex || "", 10);
  const activeCrmRecordId = String(req.body?.activeCrmRecordId || "").trim();
  let activeOrderCalcContext = limpiarCoachOrderCalcContext(req.body?.activeOrderCalcContext);
  let activeDecisionContext = limpiarCoachDecisionContext(req.body?.activeDecisionContext);

  if (resultType === "venta" && saleAmount <= 0) {
    return responderCoachError(res, 400, "Pon la cantidad de la venta para guardarla bien.");
  }

  if (!privateReason && !products.length && resultType !== "venta") {
    return responderCoachError(res, 400, "Agrega una nota corta para recordar por que termino asi.");
  }

  try {
    const now = new Date();
    const ownerUserId = resolverCoachOwnerUserId(auth.user);
    const ownershipSnapshot = await construirCoachOwnershipSnapshot(auth.user);
    const sponsorSnapshot = await construirCoachSponsorSnapshot(auth.user);
    const linkedCrmRecordDoc =
      activeCrmRecordId && mongoose.Types.ObjectId.isValid(activeCrmRecordId)
        ? await CoachCrmRecord.findOne(
            construirCoachCrmWorkspaceQuery(auth.user, {
              _id: activeCrmRecordId
            })
          )
        : null;
    let surveyId = null;
    let surveyName = "";
    let programSheetId = null;
    let programHostName = "";
    let programReferralName = "";
    let programReferralIndex = -1;

    if (activeOrderCalcContext?.ownerUserId && activeOrderCalcContext.ownerUserId !== String(ownerUserId)) {
      activeOrderCalcContext = null;
    }

    if (activeDecisionContext?.ownerUserId && activeDecisionContext.ownerUserId !== String(ownerUserId)) {
      activeDecisionContext = null;
    }

    if (activeHealthSurveyId && mongoose.Types.ObjectId.isValid(activeHealthSurveyId)) {
      const surveyDoc = await CoachHealthSurvey.findOne(
        construirCoachWorkspaceQuery(auth.user, {
          _id: activeHealthSurveyId
        })
      )
        .select("_id fullName")
        .lean();

      if (surveyDoc?._id) {
        surveyId = surveyDoc._id;
        surveyName = surveyDoc.fullName || "";
      }
    }

    if (
      activeProgram414SheetId &&
      mongoose.Types.ObjectId.isValid(activeProgram414SheetId) &&
      Number.isInteger(activeProgram414ReferralIndex) &&
      activeProgram414ReferralIndex >= 0
    ) {
      const programSheetDoc = await CoachProgramSheet.findOne({
        ...construirCoachWorkspaceQuery(auth.user),
        _id: activeProgram414SheetId
      })
        .select("_id hostName referrals")
        .lean();

      const selectedReferral = Array.isArray(programSheetDoc?.referrals)
        ? programSheetDoc.referrals[activeProgram414ReferralIndex] || null
        : null;

      if (programSheetDoc?._id && selectedReferral) {
        programSheetId = programSheetDoc._id;
        programHostName = programSheetDoc.hostName || "";
        programReferralName = selectedReferral.fullName || "";
        programReferralIndex = activeProgram414ReferralIndex;
      }
    }

    const sponsorCommissionRate = limpiarCoachCommissionRate(sponsorSnapshot.sponsorCommissionRate || 0);
    const sponsorCommissionAmount =
      resultType === "venta" && sponsorSnapshot.sponsorUserId
        ? limpiarCoachDemoOutcomeAmount(saleAmount * sponsorCommissionRate)
        : 0;

    const summary = construirResumenCoachDemoOutcome({
      resultType,
      saleAmount,
      products,
      privateReason,
      activeDemoStageLabel
    });

    const outcomeDoc = await CoachDemoOutcome.create({
      ownerUserId: ownershipSnapshot.ownerUserId,
      ownerEmail: ownershipSnapshot.ownerEmail || "",
      ownerName: ownershipSnapshot.ownerName || "",
      generatedByUserId: ownershipSnapshot.generatedByUserId || null,
      generatedByName: ownershipSnapshot.generatedByName || "",
      generatedByAccountType: ownershipSnapshot.generatedByAccountType || "owner",
      linkedCrmRecordId: linkedCrmRecordDoc?._id || null,
      sponsorUserId: sponsorSnapshot.sponsorUserId || null,
      sponsorName: sponsorSnapshot.sponsorName || "",
      sponsorCommissionRate,
      sponsorCommissionAmount,
      reportedByUserId: auth.user._id,
      reportedByName: auth.user.name || auth.user.email || "",
      resultType,
      saleAmount,
      products,
      privateReason,
      summary,
      activeWorkspace,
      activeDemoStage,
      activeDemoStageLabel,
      surveyId,
      surveyName,
      programSheetId,
      programHostName,
      programReferralName,
      programReferralIndex,
      orderCalcContext: activeOrderCalcContext,
      decisionContext: activeDecisionContext,
      demoEvents: recentCoachEvents.map(event => ({
        id: event.id || "",
        label: event.label || "",
        detail: event.detail || "",
        occurredAt: now
      })),
      updatedAt: now,
      createdAt: now
    });

    if (linkedCrmRecordDoc) {
      await vincularCoachCrmConResultadoDemo(linkedCrmRecordDoc, outcomeDoc, auth.user);
    }

    const cleanedOutcome = limpiarCoachDemoOutcome(outcomeDoc.toObject(), {
      includeGenerator: esCoachTeamManager(auth.user),
      includeSponsor: Boolean(sponsorSnapshot.sponsorUserId)
    });

    res.json({
      outcome: cleanedOutcome,
      activeDemoOutcomeContext: limpiarCoachDemoOutcomeContext({
        id: cleanedOutcome.id,
        ownerUserId: String(ownerUserId || ""),
        resultType: cleanedOutcome.resultType,
        saleAmount: cleanedOutcome.saleAmount,
        products: cleanedOutcome.products,
        privateReason: cleanedOutcome.privateReason,
        activeDemoStage: cleanedOutcome.activeDemoStage,
        activeDemoStageLabel: cleanedOutcome.activeDemoStageLabel,
        summary: cleanedOutcome.summary
      }),
      coachReply:
        resultType === "venta"
          ? "Venta reportada. Esta demo ya quedo marcada como ganada."
          : `Resultado guardado: ${formatearCoachDemoOutcomeLabel(resultType)}.`
    });
  } catch (error) {
    console.error("Error guardando resultado de demo del Coach:", error.message);
    responderCoachError(res, 500, "No pude guardar el resultado de esta demo.");
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

  const auth = await requireCoachTeamManagerUser(req, res);
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

  const auth = await requireCoachTeamManager(req, res);

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
      user: await construirCoachUserView(userActualizado || auth.user),
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

  if (!(await coachTieneAccesoOperativo(auth.user))) {
    return res.redirect("/coach/planes/");
  }

  const portalContext = await construirCoachUserView(auth.user);
  if (portalContext?.portalMode === "telemarketing") {
    return res.redirect("/coach/telemarketing/");
  }

  res.sendFile(path.join(PRIVATE_DIR, "coach-app.html"));
});

app.get(["/coach/telemarketing", "/coach/telemarketing/"], async (req, res) => {
  const auth = await obtenerCoachAuth(req);

  if (!auth.user) {
    return res.redirect("/coach/login/");
  }

  if (!(await coachTieneAccesoOperativo(auth.user))) {
    return res.redirect("/coach/planes/");
  }

  res.sendFile(path.join(PRIVATE_DIR, "coach-app.html"));
});

app.get("/api/coach/private-resources", async (req, res) => {
  const auth = await requireCoachActivo(req, res);

  if (!auth) {
    return;
  }

  try {
    const resourceDocs = await CoachPrivateResource.find({ ownerUserId: auth.user._id })
      .select("slotType fileName mimeType fileSize uploadedAt updatedAt")
      .lean();

    res.json({
      resources: construirMapaCoachPrivateResources(resourceDocs)
    });
  } catch (error) {
    console.error("Error cargando archivos privados del Coach:", error.message);
    responderCoachError(res, 500, "No pude cargar tus archivos privados.");
  }
});

app.post("/api/coach/private-resources/:slot/upload", async (req, res) => {
  const auth = await requireCoachActivo(req, res);

  if (!auth) {
    return;
  }

  const slotType = normalizarCoachPrivateResourceSlot(req.params?.slot || "");
  const pin = limpiarPinCoachPrivateResource(req.body?.pin || "");
  const buffer = extraerPdfBufferCoachPrivateResource(req.body?.fileData || "");
  const fileName = normalizarNombreArchivoPdf(
    req.body?.fileName || COACH_PRIVATE_RESOURCE_SLOTS[slotType]?.defaultFileName || "archivo-privado.pdf"
  );

  if (!slotType) {
    return responderCoachError(res, 400, "Archivo privado invalido.");
  }

  if (!pin) {
    return responderCoachError(res, 400, "El PIN debe tener 4 numeros.");
  }

  if (!buffer) {
    return responderCoachError(res, 400, "Solo puedo guardar archivos PDF validos.");
  }

  if (buffer.length > COACH_PRIVATE_RESOURCE_MAX_BYTES) {
    return responderCoachError(res, 400, "El PDF es demasiado pesado. Usa uno de hasta 8 MB.");
  }

  try {
    const now = new Date();
    const resourceDoc = await CoachPrivateResource.findOneAndUpdate(
      { ownerUserId: auth.user._id, slotType },
      {
        $set: {
          ownerEmail: auth.user.email || "",
          ownerName: auth.user.name || "",
          slotType,
          fileName,
          mimeType: "application/pdf",
          fileSize: buffer.length,
          pinHash: construirHashPinCoachPrivateResource(auth.user._id, slotType, pin),
          fileData: buffer,
          uploadedAt: now,
          updatedAt: now
        },
        $setOnInsert: {
          ownerUserId: auth.user._id,
          createdAt: now
        }
      },
      {
        new: true,
        upsert: true
      }
    ).lean();

    res.json({
      resource: limpiarCoachPrivateResource(resourceDoc, slotType)
    });
  } catch (error) {
    console.error("Error guardando archivo privado del Coach:", error.message);
    responderCoachError(res, 500, "No pude guardar ese archivo privado.");
  }
});

app.delete("/api/coach/private-resources/:slot", async (req, res) => {
  const auth = await requireCoachActivo(req, res);

  if (!auth) {
    return;
  }

  const slotType = normalizarCoachPrivateResourceSlot(req.params?.slot || "");

  if (!slotType) {
    return responderCoachError(res, 400, "Archivo privado invalido.");
  }

  try {
    const deleted = await CoachPrivateResource.findOneAndDelete({
      ownerUserId: auth.user._id,
      slotType
    });

    res.json({
      deleted: Boolean(deleted),
      resource: limpiarCoachPrivateResource(null, slotType)
    });
  } catch (error) {
    console.error("Error borrando archivo privado del Coach:", error.message);
    responderCoachError(res, 500, "No pude borrar ese archivo privado.");
  }
});

app.post("/api/coach/private-resources/:slot/file", async (req, res) => {
  const auth = await requireCoachActivo(req, res);

  if (!auth) {
    return;
  }

  const slotType = normalizarCoachPrivateResourceSlot(req.params?.slot || "");
  const pin = limpiarPinCoachPrivateResource(req.body?.pin || "");

  if (!slotType) {
    return responderCoachError(res, 400, "Archivo privado invalido.");
  }

  if (!pin) {
    return responderCoachError(res, 400, "Escribe tu PIN de 4 numeros.");
  }

  try {
    const resourceDoc = await CoachPrivateResource.findOne({
      ownerUserId: auth.user._id,
      slotType
    });

    if (!resourceDoc?.fileData?.length) {
      return responderCoachError(res, 404, "Todavia no has subido archivo en ese espacio.");
    }

    const expectedPinHash = construirHashPinCoachPrivateResource(auth.user._id, slotType, pin);

    if (resourceDoc.pinHash !== expectedPinHash) {
      return responderCoachError(res, 403, "Ese PIN no coincide.");
    }

    res.setHeader("Cache-Control", "private, no-store");
    res.setHeader("X-Robots-Tag", "noindex, nofollow");
    res.setHeader("Content-Disposition", `inline; filename="${resourceDoc.fileName || "archivo-privado.pdf"}"`);
    res.type(resourceDoc.mimeType || "application/pdf");
    res.send(resourceDoc.fileData);
  } catch (error) {
    console.error("Error abriendo archivo privado del Coach:", error.message);
    responderCoachError(res, 500, "No pude abrir ese archivo privado.");
  }
});

app.get("/coach/resources/novel-catalog-digital-3.pdf", async (req, res) => {
  res.setHeader("Cache-Control", "private, no-store");
  res.setHeader("X-Robots-Tag", "noindex, nofollow");
  res.sendFile(path.join(PUBLIC_DIR, "coach", "resources", "novel-catalog-digital-3.pdf"));
});

app.get(["/coach/resources/lista-precios-2026", "/coach/resources/lista-precios-2026/"], async (req, res) => {
  res.redirect("/coach/app/");
});

app.get(
  ["/coach/resources/lista-precios-2026/file", "/coach/resources/lista-precios-2026/file/"],
  async (req, res) => {
    res.redirect("/coach/app/");
  }
);

app.get(["/control", "/control/", "/control/app", "/control/app/"], async (req, res) => {
  const auth = await obtenerCoachAuth(req);

  if (!auth.user) {
    return res.redirect("/coach/login/");
  }

  if (!usuarioPuedeVerTorreControl(auth.user)) {
    return res.redirect("/coach/app/");
  }

  res.sendFile(path.join(PRIVATE_DIR, "control-tower.html"));
});

app.get("/api/control/overview", async (req, res) => {
  const auth = await requireControlTowerAccess(req, res);

  if (!auth) {
    return;
  }

  try {
    const stats = await obtenerControlTowerStats(auth.user);
    res.json(stats);
  } catch (error) {
    console.error("Error obteniendo torre de control:", error.message);
    res.status(500).json({ error: "No pude cargar la torre de control." });
  }
});

app.get("/api/control/communications", async (req, res) => {
  const auth = await requireControlTowerAccess(req, res);

  if (!auth) {
    return;
  }

  try {
    const data = await obtenerControlCommunicationsOverview();
    res.json(data);
  } catch (error) {
    console.error("Error cargando comunicaciones de la torre:", error.message);
    res.status(500).json({ error: "No pude cargar las comunicaciones internas." });
  }
});

app.post("/api/control/announcements", async (req, res) => {
  const auth = await requireControlTowerAccess(req, res);

  if (!auth) {
    return;
  }

  const scopeType = normalizarCoachAnnouncementScopeType(req.body?.scopeType || "global");
  const territoryId = String(req.body?.territoryId || "").trim();
  const title = limpiarCoachAnnouncementTitle(req.body?.title || "");
  const body = limpiarCoachAnnouncementBody(req.body?.body || "");
  const priority = normalizarCoachAnnouncementPriority(req.body?.priority || "normal");

  if (!["global", "territory"].includes(scopeType)) {
    return res.status(400).json({ error: "La torre solo puede mandar boletines globales o territoriales." });
  }

  if (!title) {
    return res.status(400).json({ error: "Pon un titulo para el boletin." });
  }

  if (!body) {
    return res.status(400).json({ error: "Escribe el contenido del boletin." });
  }

  try {
    let territoryDoc = null;

    if (scopeType === "territory") {
      if (!territoryId || !mongoose.Types.ObjectId.isValid(territoryId)) {
        return res.status(400).json({ error: "Selecciona un territorio valido." });
      }

      territoryDoc = await CoachTerritory.findOne({
        _id: territoryId,
        status: "active"
      }).lean();

      if (!territoryDoc) {
        return res.status(404).json({ error: "No encontre ese territorio." });
      }
    }

    const now = new Date();
    const announcementDoc = await CoachAnnouncement.create({
      authorUserId: auth.user._id,
      authorName: auth.user.name || auth.user.email || "Control Tower",
      scopeType,
      territoryId: scopeType === "territory" ? territoryDoc._id : null,
      territoryName: scopeType === "territory" ? territoryDoc.name || "" : "",
      title,
      body,
      priority,
      status: "active",
      updatedAt: now
    });

    res.json({
      announcement: limpiarCoachAnnouncement(announcementDoc.toObject ? announcementDoc.toObject() : announcementDoc)
    });
  } catch (error) {
    console.error("Error creando boletin maestro:", error.message);
    res.status(500).json({ error: "No pude mandar ese boletin en este momento." });
  }
});

app.post("/api/control/support/:threadId/messages", async (req, res) => {
  const auth = await requireControlTowerAccess(req, res);

  if (!auth) {
    return;
  }

  const threadId = String(req.params?.threadId || "").trim();
  const body = limpiarCoachSupportMessageBody(req.body?.body || "");

  if (!threadId || !mongoose.Types.ObjectId.isValid(threadId)) {
    return res.status(400).json({ error: "Thread invalido." });
  }

  if (!body) {
    return res.status(400).json({ error: "Escribe tu respuesta antes de mandarla." });
  }

  try {
    const threadDoc = await CoachSupportThread.findById(threadId);

    if (!threadDoc) {
      return res.status(404).json({ error: "No encontre esa conversacion de soporte." });
    }

    const now = new Date();
    await CoachSupportMessage.create({
      threadId: threadDoc._id,
      senderUserId: auth.user._id,
      senderName: auth.user.name || auth.user.email || "Soporte",
      senderScope: "control_tower",
      body
    });

    threadDoc.status = "open";
    threadDoc.supportUnreadCount = 0;
    threadDoc.ownerUnreadCount = Number(threadDoc.ownerUnreadCount || 0) + 1;
    threadDoc.lastMessageAt = now;
    threadDoc.lastMessagePreview = truncarTextoPrompt(body, 180);
    threadDoc.updatedAt = now;
    await threadDoc.save();

    res.json({
      ok: true,
      thread: limpiarCoachSupportThread(threadDoc)
    });
  } catch (error) {
    console.error("Error respondiendo soporte desde torre:", error.message);
    res.status(500).json({ error: "No pude mandar la respuesta de soporte." });
  }
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
    },
    sms: {
      enabled: twilioSmsEstaConfigurado(),
      from: TWILIO_SMS_FROM,
      messagingServiceSid: TWILIO_MESSAGING_SERVICE_SID,
      webhookPath: "/webhooks/twilio/sms"
    }
  });
});

app.get("/webhooks/highlevel", (req, res) => {
  const authorized = highLevelWebhookAutorizado(req);

  res.json({
    ok: true,
    webhookPath: "/webhooks/highlevel",
    configured: Boolean(HIGHLEVEL_WEBHOOK_TOKEN),
    authorized,
    syncEnabled: highLevelSyncEstaConfigurado()
  });
});

app.post("/webhooks/highlevel", async (req, res) => {
  if (!highLevelWebhookAutorizado(req)) {
    return res.status(401).json({ error: "Webhook token invalido." });
  }

  try {
    const updatedLead = await aplicarEventoHighLevelALead(req.body || {});

    return res.json({
      ok: true,
      received: true,
      eventType: cleanText(req.body?.type || ""),
      matchedLeadId: updatedLead?._id ? String(updatedLead._id) : ""
    });
  } catch (error) {
    console.error("Error procesando webhook de HighLevel:", error.message);
    return res.status(500).json({ error: "No pude procesar el webhook de HighLevel." });
  }
});

app.get("/webhooks/twilio/sms", (req, res) => {
  res.json({
    ok: true,
    mode: "sms",
    enabled: TWILIO_SMS_ENABLED,
    messagingServiceSidConfigured: Boolean(TWILIO_MESSAGING_SERVICE_SID),
    fromConfigured: Boolean(TWILIO_SMS_FROM),
    aiReplyEnabled: TWILIO_SMS_AI_REPLY_ENABLED,
    webhookPath: "/webhooks/twilio/sms",
    tokenRequired: Boolean(TWILIO_SMS_WEBHOOK_TOKEN)
  });
});

app.post("/webhooks/twilio/sms", express.urlencoded({ extended: false }), async (req, res) => {
  if (!TWILIO_SMS_ENABLED) {
    return res.type("text/xml").send(construirTwilioEmptyResponse());
  }

  if (!validarFirmaTwilioWebhook(req)) {
    return res.status(403).type("text/plain").send("Firma Twilio invalida");
  }

  if (TWILIO_SMS_WEBHOOK_TOKEN && cleanText(req.query.token || "") !== TWILIO_SMS_WEBHOOK_TOKEN) {
    return res.status(403).type("text/plain").send("Webhook no autorizado");
  }

  const body = cleanText(req.body?.Body || "");
  const fromRaw = cleanText(req.body?.From || "");
  const phone = normalizePhone(fromRaw);
  const messageSid = cleanText(req.body?.MessageSid || "");
  let matchedLead = null;
  let leadChefContext = null;
  let ownerUserId = "";

  if (phone) {
    try {
      const resolved = await resolverCoachInboundRouteContext({
        channel: "sms",
        phone,
        collection: "lead_inbox"
      });
      matchedLead = resolved.matchedDoc;
      leadChefContext = resolved.coachChefContext;
      ownerUserId = resolved.ownerUserId;

      if (matchedLead && body) {
        await registrarActividadSmsCoachLead(matchedLead, {
          direction: "entrante",
          message: body,
          sid: messageSid,
          userDoc: leadChefContext?.userDoc || null
        });
      }
    } catch (error) {
      console.error("Error guardando SMS entrante del Coach:", error.message);
    }
  }

  if (!matchedLead || !leadChefContext?.ownership?.ownerUserId) {
    return res.type("text/xml").send(
      construirTwilioMessageResponse(
        "No pude identificar la cuenta correcta para este SMS. Pidele a tu distribuidor que te escriba primero desde su CRM."
      )
    );
  }

  if (!TWILIO_SMS_AI_REPLY_ENABLED) {
    return res.type("text/xml").send(construirTwilioEmptyResponse());
  }

  if (!body || !phone) {
    return res.type("text/xml").send(construirTwilioEmptyResponse());
  }

  const sessionId = construirCanalSessionId("sms", phone, ownerUserId || leadChefContext.ownership.ownerUserId);
  const visitorId = construirCanalVisitorId("sms", phone, ownerUserId || leadChefContext.ownership.ownerUserId);
  const resultado = await procesarChatChefCanal({
    pregunta: body,
    sessionId,
    visitorId,
    phoneHint: phone,
    source: "sms",
    coachChefContextOverride: leadChefContext
  });

  const mensaje = resultado.ok ? resultado.respuesta : resultado.error || "No pude responder en este momento.";
  return res.type("text/xml").send(construirTwilioMessageResponse(mensaje));
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

  if (!validarFirmaTwilioWebhook(req)) {
    return res.status(403).type("text/plain").send("Firma Twilio invalida");
  }

  if (TWILIO_WHATSAPP_WEBHOOK_TOKEN && cleanText(req.query.token || "") !== TWILIO_WHATSAPP_WEBHOOK_TOKEN) {
    return res.status(403).type("text/plain").send("Webhook no autorizado");
  }

  const body = cleanText(req.body?.Body || "");
  const fromRaw = cleanText(req.body?.From || "");
  const phone = normalizePhone(req.body?.WaId || fromRaw);
  const profileName = cleanText(req.body?.ProfileName || "");
  let matchedLead = null;
  let leadChefContext = null;
  let ownerUserId = "";

  if (!body) {
    return res.type("text/xml").send(construirTwilioMessageResponse("Recibi tu mensaje vacio. Intenta otra vez con texto."));
  }

  if (!phone) {
    return res
      .type("text/xml")
      .send(construirTwilioMessageResponse("No pude identificar tu numero de WhatsApp. Intenta de nuevo."));
  }

  try {
    const resolved = await resolverCoachInboundRouteContext({
      channel: "whatsapp",
      phone,
      collection: "public_lead"
    });
    matchedLead = resolved.matchedDoc;
    leadChefContext = resolved.coachChefContext;
    ownerUserId = resolved.ownerUserId;

    if (!leadChefContext) {
      leadChefContext = await resolverCoachChefContextPublicoPredeterminado();
      ownerUserId = ownerUserId || String(leadChefContext?.ownership?.ownerUserId || "").trim();
    }

    if (matchedLead && leadChefContext) {
      matchedLead = await adoptarLeadPublicoEnCoachContext(matchedLead, leadChefContext);
    }
  } catch (error) {
    console.error("Error resolviendo contexto de WhatsApp Chef:", error.message);
  }

  const sessionId = construirCanalSessionId("whatsapp", phone, ownerUserId || leadChefContext?.ownership?.ownerUserId || "");
  const visitorId = construirCanalVisitorId("whatsapp", phone, ownerUserId || leadChefContext?.ownership?.ownerUserId || "");
  const resultado = await procesarChatChefCanal({
    pregunta: body,
    sessionId,
    visitorId,
    phoneHint: phone,
    nameHint: profileName,
    source: "whatsapp_sandbox",
    coachChefContextOverride: leadChefContext
  });

  const mensaje = resultado.ok ? resultado.respuesta : resultado.error || "No pude responder en este momento.";
  return res.type("text/xml").send(construirTwilioMessageResponse(mensaje));
});

app.get("/chef/manifest.webmanifest", async (req, res) => {
  const requestedSlug = normalizarCoachChefSlugSegment(req.query?.slug || "", 48);
  const chefContext = requestedSlug ? await resolverCoachChefContextPorSlug(requestedSlug) : null;
  const startPath = chefContext?.slug ? construirCoachChefPath(chefContext.slug) : "/chef/";

  res.type("application/manifest+json");
  res.send(
    JSON.stringify(
      {
        name: "Agustin 2.0 Chef",
        short_name: "Agustin Chef",
        description:
          "Chef inteligente gratis para recetas, cocina mexicana saludable, agua y uso practico de productos.",
        start_url: startPath,
        scope: "/chef/",
        display: "standalone",
        background_color: "#f6fbff",
        theme_color: "#0a2247",
        icons: [
          {
            src: "/chef/icons/brand-mark-192.png",
            sizes: "192x192",
            type: "image/png"
          },
          {
            src: "/chef/icons/brand-mark-512.png",
            sizes: "512x512",
            type: "image/png"
          }
        ]
      },
      null,
      2
    )
  );
});

app.get(/^\/chef\/whatsapp\/?$/i, (req, res) => {
  res.sendFile(path.join(PUBLIC_DIR, "chef", "whatsapp", "index.html"));
});

app.get(/^\/chef\/([a-z0-9-]+)\/?$/i, async (req, res) => {
  const chefSlug = normalizarCoachChefSlugSegment(req.params?.[0] || "", 48);
  const chefContext = await resolverCoachChefContextPorSlug(chefSlug);

  if (!chefContext) {
    return res.redirect("/chef/");
  }

  res.sendFile(path.join(PUBLIC_DIR, "chef", "index.html"));
});

app.get(/^\/contactos\/([a-f0-9]+)\/?$/i, async (req, res) => {
  const shareCode = normalizarCoachShareCodeSegment(req.params?.[0] || "", 24);
  const shareContext = await resolverCoachContactShareContextPorCode(shareCode);

  if (!shareContext) {
    return res.redirect("/coach/");
  }

  res.sendFile(path.join(PUBLIC_DIR, "contact-share", "index.html"));
});

app.get("/api/public/contact-share/:shareCode", async (req, res) => {
  try {
    const shareCode = normalizarCoachShareCodeSegment(req.params?.shareCode || "", 24);
    const shareContext = await resolverCoachContactShareContextPorCode(shareCode);

    if (!shareContext) {
      return res.status(404).json({ error: "Ese acceso ya no esta disponible." });
    }

    res.json({
      ok: true,
      shareCode,
      recipientName:
        shareContext.profileDoc?.seatLabel ||
        shareContext.profileDoc?.name ||
        shareContext.userDoc?.name ||
        "Distribuidor",
      ownerName: shareContext.ownership.ownerName || "",
      ownerEmail: shareContext.ownership.ownerEmail || "",
      teamRole: shareContext.profileDoc?.teamRole || "",
      sharePath: construirCoachContactSharePath(shareCode)
    });
  } catch (error) {
    console.error("Error cargando metadata de contacto compartido:", error.message);
    res.status(500).json({ error: "No pude abrir esta pagina en este momento." });
  }
});

app.post("/api/public/contact-share/import", async (req, res) => {
  try {
    const shareCode = normalizarCoachShareCodeSegment(req.body?.shareCode || "", 24);
    const importMode = String(req.body?.importMode || "").trim().toLowerCase();
    const rawText = normalizarCoachContactImportText(req.body?.rawText || "");
    const fileName = cleanText(req.body?.fileName || "").slice(0, 120);
    const shareContext = await resolverCoachContactShareContextPorCode(shareCode);

    if (!shareContext) {
      return res.status(404).json({ error: "Ese acceso ya no esta disponible." });
    }

    if (!rawText.trim()) {
      return res.status(400).json({ error: "Sube un archivo o pega contactos antes de guardar." });
    }

    const parsedContacts = parseCoachImportedContacts(importMode, rawText).slice(0, 500);

    if (!parsedContacts.length) {
      return res.status(400).json({
        error: "No pude encontrar contactos validos. Usa CSV, VCF o pega nombres con telefono o correo."
      });
    }

    let importedCount = 0;
    let duplicateCount = 0;
    const sampleLeads = [];

    for (const contact of parsedContacts) {
      const notes = [
        fileName
          ? `Importado desde pagina privada de contactos (${fileName}).`
          : "Importado desde pagina privada de contactos.",
        contact.notes || ""
      ]
        .filter(Boolean)
        .join(" ")
        .trim();

      const result = await guardarCoachInboxLead({
        userDoc: shareContext.userDoc,
        profileDoc: shareContext.ownerProfileDoc || shareContext.profileDoc,
        payload: {
          fullName: contact.fullName,
          phone: contact.phone,
          email: contact.email,
          interest: "Lista de contactos",
          source: "contactos_compartidos",
          notes,
          consentGiven: false
        },
        sendDestination: false
      });

      if (result.duplicate) {
        duplicateCount += 1;
      } else {
        importedCount += 1;
      }

      if (sampleLeads.length < 5 && result.lead) {
        sampleLeads.push(result.lead);
      }
    }

    res.json({
      ok: true,
      parsedCount: parsedContacts.length,
      importedCount,
      duplicateCount,
      sampleLeads,
      recipientName:
        shareContext.profileDoc?.seatLabel ||
        shareContext.profileDoc?.name ||
        shareContext.userDoc?.name ||
        "Distribuidor"
    });
  } catch (error) {
    console.error("Error importando contactos compartidos:", error.message);
    res.status(500).json({ error: "No pude guardar esos contactos en este momento." });
  }
});

app.post(AR_PROTOTYPE_UNLOCK_PATH, express.urlencoded({ extended: false }), (req, res) => {
  const submittedPin = String(req.body?.pin || "").trim();
  const redirectPath = construirRedirectArPrototype(req.body?.redirect || `${AR_PROTOTYPE_PATH_PREFIX}/`);

  if (submittedPin !== AR_PROTOTYPE_PIN) {
    return res.status(401).type("html").send(renderArPrototypeLockPage(req, { redirectPath, showError: true }));
  }

  res.cookie(AR_PROTOTYPE_COOKIE, construirAccessoArPrototypeHash(), {
    httpOnly: true,
    sameSite: "lax",
    secure: requestEsSeguro(req),
    path: AR_PROTOTYPE_PATH_PREFIX,
    maxAge: AR_PROTOTYPE_COOKIE_TTL_MS
  });

  return res.redirect(redirectPath);
});

app.post(AR_PROTOTYPE_LOCK_PATH, express.urlencoded({ extended: false }), (req, res) => {
  res.clearCookie(AR_PROTOTYPE_COOKIE, {
    httpOnly: true,
    sameSite: "lax",
    secure: requestEsSeguro(req),
    path: AR_PROTOTYPE_PATH_PREFIX
  });

  return res.redirect(`${AR_PROTOTYPE_PATH_PREFIX}/`);
});

app.use(AR_PROTOTYPE_PATH_PREFIX, (req, res, next) => {
  if (arPrototypeTieneAcceso(req)) {
    return next();
  }

  return res.status(401).type("html").send(renderArPrototypeLockPage(req));
});

app.use(express.static(PUBLIC_DIR));

// =============================
// CHAT
// =============================
app.post("/chat", async (req, res) => {
  const { pregunta, sessionId, visitorId, mode } = req.body;
  const chefSlug = typeof req.body?.chefSlug === "string" ? req.body.chefSlug.trim() : "";
  const modoChat = normalizarModoChat(mode);
  const preguntaLimpia = typeof pregunta === "string" ? pregunta.trim() : "";
  const activeWorkspace = typeof req.body?.activeWorkspace === "string" ? req.body.activeWorkspace.trim() : "";
  const activeDemoStage = typeof req.body?.activeDemoStage === "string" ? req.body.activeDemoStage.trim() : "";
  const activeDemoStageLabel =
    typeof req.body?.activeDemoStageLabel === "string" ? req.body.activeDemoStageLabel.trim() : "";
  const activeDemoStageCopy =
    typeof req.body?.activeDemoStageCopy === "string" ? req.body.activeDemoStageCopy.trim() : "";
  const recentCoachEvents = Array.isArray(req.body?.recentCoachEvents)
    ? req.body.recentCoachEvents.map(limpiarCoachDemoEventPrompt).filter(Boolean).slice(0, 6)
    : [];
  const activeHealthSurveyId =
    typeof req.body?.activeHealthSurveyId === "string" ? req.body.activeHealthSurveyId.trim() : "";
  const activeProgram414SheetId =
    typeof req.body?.activeProgram414SheetId === "string" ? req.body.activeProgram414SheetId.trim() : "";
  const activeProgram414ReferralIndex = Number.parseInt(req.body?.activeProgram414ReferralIndex || "", 10);
  let activeHealthSurveyContextFromBody = limpiarCoachHealthSurveyActiveContext(req.body?.activeHealthSurveyContext);
  let activeProgram414ContextFromBody = limpiarCoachProgram414ActiveContext(req.body?.activeProgram414Context);
  let activeOrderCalcContext = limpiarCoachOrderCalcContext(req.body?.activeOrderCalcContext);
  let activeDecisionContext = limpiarCoachDecisionContext(req.body?.activeDecisionContext);
  let activeDemoOutcomeContext = limpiarCoachDemoOutcomeContext(req.body?.activeDemoOutcomeContext);
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

  await asegurarSesionRedisCargada(sessionId);

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
    let coachChefContext = null;
    let coachProfileDoc = null;
    let coachAnalyticsDoc = null;
    let repLeadSummary = null;
    let activeLeadContext = null;
    let activeHealthSurveyContext = null;
    let activeProgram414Context = null;
    const modoPrompt = construirContextoModoPrompt(modoChat, coachAuth?.user);
    let estadoPrompt = "";
    let perfilPrompt = "";
    let preferFastCoachContext = false;
    let coachUserMessageTask = null;

    if (modoChat === "coach") {
      const coachOwnerUserId = String(resolverCoachOwnerUserId(coachAuth.user) || coachAuth.user._id);
      if (
        activeHealthSurveyContextFromBody?.ownerUserId &&
        activeHealthSurveyContextFromBody.ownerUserId !== coachOwnerUserId
      ) {
        activeHealthSurveyContextFromBody = null;
      }

      if (
        activeProgram414ContextFromBody?.ownerUserId &&
        activeProgram414ContextFromBody.ownerUserId !== coachOwnerUserId
      ) {
        activeProgram414ContextFromBody = null;
      }

      [coachProfileDoc, coachAnalyticsDoc] = await Promise.all([
        CoachDistributorProfile.findOne({ userId: coachAuth.user._id }).lean(),
        COACH_TURBO_MODE ? Promise.resolve(null) : CoachDistributorAnalytics.findOne({ userId: coachAuth.user._id }).lean()
      ]);
      coachUserMessageTask = guardarMensajeRaw({
        visitorId: visitorIdLimpio,
        sessionId,
        profileId: null,
        leadId: null,
        role: "user",
        content: preguntaLimpia,
        intent: "coach_chat",
        detectedTopics: [`coach_user:${String(coachAuth.user._id)}`]
      });

      const coachHelpReply = construirCoachHelpReply({
        userDoc: coachAuth.user,
        profileDoc: coachProfileDoc,
        question: preguntaLimpia,
        activeWorkspace
      });

      if (coachHelpReply?.reply) {
        registrarMensajeMemoria(sessionId, "user", preguntaLimpia);
        registrarMensajeMemoria(sessionId, "assistant", coachHelpReply.reply);

        void Promise.allSettled([
          coachUserMessageTask,
          guardarMensajeRaw({
            visitorId: visitorIdLimpio,
            sessionId,
            profileId: null,
            leadId: null,
            role: "assistant",
            content: coachHelpReply.reply,
            intent: "coach_help",
            detectedTopics: [
              `coach_user:${String(coachAuth.user._id)}`,
              `coach_help:${coachHelpReply.topicId || "general"}`
            ]
          })
        ]).catch(error => {
          console.error("Error guardando ayuda interna del Coach:", error.message);
        });

        return res.json({
          respuesta: coachHelpReply.reply,
          mode: modoChat,
          coachHelpMode: true,
          coachHelpTopic: coachHelpReply.topicId || "general",
          coachHelpActions: Array.isArray(coachHelpReply.actions) ? coachHelpReply.actions : [],
          usage: {
            usedToday: (coachUsage?.usedToday || 0) + 1,
            remainingToday: Math.max((coachUsage?.remainingToday || COACH_MAX_MESSAGES_PER_DAY) - 1, 0),
            limitPerDay: COACH_MAX_MESSAGES_PER_DAY
          }
        });
      }

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
      perfilPrompt += construirPromptCoachDemoActivo({
        workspace: activeWorkspace,
        stageId: activeDemoStage,
        stageLabel: activeDemoStageLabel,
        stageCopy: activeDemoStageCopy,
        events: recentCoachEvents
      });

      if (
        activeOrderCalcContext?.ownerUserId &&
        activeOrderCalcContext.ownerUserId !== coachOwnerUserId
      ) {
        activeOrderCalcContext = null;
      }

      if (activeOrderCalcContext) {
        perfilPrompt += construirPromptCalculadoraActiva(activeOrderCalcContext);
      }

      if (
        activeDecisionContext?.ownerUserId &&
        activeDecisionContext.ownerUserId !== coachOwnerUserId
      ) {
        activeDecisionContext = null;
      }

      if (activeDecisionContext) {
        perfilPrompt += construirPromptBalanceDecisionActivo(activeDecisionContext);
      }

      if (
        activeDemoOutcomeContext?.ownerUserId &&
        activeDemoOutcomeContext.ownerUserId !== coachOwnerUserId
      ) {
        activeDemoOutcomeContext = null;
      }

      if (activeDemoOutcomeContext) {
        perfilPrompt += construirPromptResultadoDemoActivo(activeDemoOutcomeContext);
      }

      activeHealthSurveyContext = activeHealthSurveyContextFromBody || null;
      activeProgram414Context = activeProgram414ContextFromBody || null;

      const turboQuickReply = COACH_TURBO_MODE
        ? construirCoachTurboReply({
            question: preguntaLimpia,
            activeDemoStage,
            activeHealthSurveyContext,
            activeProgram414Context,
            activeOrderCalcContext,
            activeDecisionContext
          })
        : "";

      if (turboQuickReply) {
        registrarMensajeMemoria(sessionId, "user", preguntaLimpia);
        registrarMensajeMemoria(sessionId, "assistant", turboQuickReply);

        void Promise.allSettled([
          coachUserMessageTask,
          guardarMensajeRaw({
            visitorId: visitorIdLimpio,
            sessionId,
            profileId: null,
            leadId: null,
            role: "assistant",
            content: turboQuickReply,
            intent: "coach_chat",
            detectedTopics: [`coach_user:${String(coachAuth.user._id)}`]
          }),
          actualizarPerfilYAnalyticsCoach({
            userDoc: coachAuth.user,
            sessionId,
            question: preguntaLimpia,
            reply: turboQuickReply
          })
        ]).catch(error => {
          console.error("Error guardando respuesta turbo del Coach:", error.message);
        });

        return res.json({
          respuesta: turboQuickReply,
          mode: modoChat,
          turboMode: true,
          turboLane: "quick",
          activeHealthSurveyContext,
          activeProgram414Context,
          activeOrderCalcContext,
          activeDecisionContext,
          activeDemoOutcomeContext,
          usage: {
            usedToday: (coachUsage?.usedToday || 0) + 1,
            remainingToday: Math.max((coachUsage?.remainingToday || COACH_MAX_MESSAGES_PER_DAY) - 1, 0),
            limitPerDay: COACH_MAX_MESSAGES_PER_DAY
          }
        });
      }

      const leadMemoryPromise = COACH_TURBO_MODE
        ? Promise.resolve({ leadContext: null, repLeadSummary: null })
        : obtenerMemoriaLeadRelacionada({
            question: preguntaLimpia,
            mode: "coach",
            coachUser: coachAuth.user
          });
      const activeHealthSurveyPromise =
        activeHealthSurveyContext
          ? Promise.resolve(activeHealthSurveyContext)
          : activeHealthSurveyId && mongoose.Types.ObjectId.isValid(activeHealthSurveyId)
          ? CoachHealthSurvey.findOne(
              construirCoachWorkspaceQuery(coachAuth.user, {
                _id: activeHealthSurveyId
              })
            ).lean()
          : Promise.resolve(null);
      const activeProgram414Promise =
        activeProgram414Context
          ? Promise.resolve(activeProgram414Context)
          : activeProgram414SheetId &&
            mongoose.Types.ObjectId.isValid(activeProgram414SheetId) &&
            Number.isInteger(activeProgram414ReferralIndex) &&
            activeProgram414ReferralIndex >= 0
          ? CoachProgramSheet.findOne({
              ...construirCoachWorkspaceQuery(coachAuth.user),
              _id: activeProgram414SheetId
            }).lean()
          : Promise.resolve(null);
      const [leadMemory, surveyDoc, programSheetDoc] = await Promise.all([
        leadMemoryPromise,
        activeHealthSurveyPromise,
        activeProgram414Promise,
        COACH_TURBO_MODE ? Promise.resolve(null) : coachUserMessageTask
      ]);

      repLeadSummary = leadMemory?.repLeadSummary || null;
      activeLeadContext = leadMemory?.leadContext || null;
      perfilPrompt += construirPromptPipelineRepresentante(repLeadSummary);
      perfilPrompt += construirPromptMemoriaLead(activeLeadContext, "coach");

      if (surveyDoc) {
        activeHealthSurveyContext = surveyDoc?.salesAnalysis ? surveyDoc : limpiarCoachHealthSurvey(surveyDoc);
        perfilPrompt += construirPromptEncuestaSaludActiva(activeHealthSurveyContext);
      }

      if (programSheetDoc) {
        if (programSheetDoc?.referral?.fullName) {
          activeProgram414Context = programSheetDoc;
          perfilPrompt += construirPromptPrograma414Activo(activeProgram414Context);
        } else {
          const cleanedSheet = limpiarCoachProgramSheet(programSheetDoc);
          const selectedReferral = cleanedSheet?.referrals?.[activeProgram414ReferralIndex] || null;

          if (cleanedSheet?.id && selectedReferral) {
            activeProgram414Context = {
              ...cleanedSheet,
              sheetId: cleanedSheet.id,
              referralIndex: activeProgram414ReferralIndex,
              referral: selectedReferral
            };
            perfilPrompt += construirPromptPrograma414Activo(activeProgram414Context);
          }
        }
      }

      if (COACH_TURBO_MODE) {
        perfilPrompt += `

MODO TURBO DEL COACH:
- responde en espanol simple, corto y accionable
- usa maximo 4 lineas o un parrafo breve
- no des teoria larga
- si ya hay encuesta, calculadora, balance o resultado activo, apoyate primero en eso
`;
      }

      preferFastCoachContext = Boolean(
        COACH_TURBO_MODE ||
          activeHealthSurveyContext ||
          activeProgram414Context ||
          activeOrderCalcContext ||
          activeDecisionContext ||
          activeDemoOutcomeContext
      );
    } else {
      coachChefContext = await resolverCoachChefContextPorSlug(chefSlug);
      const leadPrevio = await resolverLeadExistente(
        sessionId,
        visitorIdLimpio,
        "",
        "",
        coachChefContext
      );
      const perfilPrevio = await resolverPerfilExistente(
        visitorIdLimpio,
        "",
        "",
        leadPrevio?._id || null,
        coachChefContext
      );
      hidratarEstadoConversacion(sessionId, perfilPrevio, leadPrevio);
      actualizarEstadoConversacion(sessionId, preguntaLimpia);
      const estadoActual = obtenerEstadoConversacion(sessionId);

      leadGuardado = await guardarLeadSiExiste(
        preguntaLimpia,
        sessionId,
        visitorIdLimpio,
        estadoActual,
        coachChefContext
      );
      const estadoConLead = actualizarEstadoConversacion(sessionId, preguntaLimpia, leadGuardado);
      profileGuardado = await guardarOActualizarPerfil({
        visitorId: visitorIdLimpio,
        sessionId,
        texto: preguntaLimpia,
        estadoConversacion: estadoConLead,
        leadGuardado,
        coachChefContext
      });
      await sincronizarLeadPublicoACoachInbox({
        texto: preguntaLimpia,
        leadDoc: leadGuardado,
        profileDoc: profileGuardado,
        coachChefContext
      });
      await guardarMensajeRaw({
        visitorId: visitorIdLimpio,
        sessionId,
        profileId: profileGuardado?._id || null,
        leadId: leadGuardado?._id || null,
        coachChefContext,
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

    registrarMensajeMemoria(sessionId, "user", preguntaLimpia);
    const historyLimit =
      modoChat === "coach"
        ? COACH_TURBO_MODE
          ? COACH_TURBO_MAX_HISTORY_MESSAGES
          : COACH_PROMPT_HISTORY_MESSAGES
        : MAX_PROMPT_HISTORY_MESSAGES;
    const cacheHistory = obtenerHistorialConversacionMemoria(sessionId, Math.min(historyLimit, 6));
    const statePrompt = estadoPrompt;
    const aiCacheOptions = {
      mode: modoChat,
      scope:
        modoChat === "coach"
          ? String(resolverCoachOwnerUserId(coachAuth?.user) || coachAuth?.user?._id || "").trim() || "coach"
          : String(coachChefContext?.ownership?.ownerUserId || "").trim() ||
            cleanText(coachChefContext?.slug || chefSlug || "").trim() ||
            "public",
      question: preguntaLimpia,
      activeWorkspace,
      activeDemoStage,
      statePrompt,
      profilePrompt,
      history: cacheHistory,
      extra: {
        preferFastCoachContext,
        activeLeadContext,
        repLeadSummary,
        activeHealthSurveyContext,
        activeProgram414Context,
        activeOrderCalcContext,
        activeDecisionContext,
        activeDemoOutcomeContext
      }
    };
    const cachedAiReply = await obtenerRespuestaAiCacheada(aiCacheOptions);
    let respuestaIA = null;
    let cacheHit = false;

    if (cachedAiReply?.reply) {
      respuestaIA = {
        role: "assistant",
        content: cachedAiReply.reply
      };
      cacheHit = true;
    } else {
      const contextoPromise =
        modoChat === "coach" && preferFastCoachContext
          ? Promise.resolve(construirContextoEstaticoCoach(preguntaLimpia))
          : construirContexto(preguntaLimpia, modoChat);
      const historialPromptPromise = obtenerHistorialConversacionPrompt(sessionId, historyLimit, {
        preferMemory: modoChat === "coach"
      });
      const [contexto, historialPrompt] = await Promise.all([contextoPromise, historialPromptPromise]);

      const openAiPayload = {
        model: "gpt-5-mini",
        messages: [
          { role: "system", content: construirPromptModo(modoChat) },
          { role: "system", content: modoPrompt },
          { role: "system", content: contexto },
          { role: "system", content: estadoPrompt },
          { role: "system", content: perfilPrompt },
          ...historialPrompt
        ]
      };

      if (modoChat === "coach" && COACH_TURBO_MODE) {
        openAiPayload.max_completion_tokens = COACH_TURBO_MAX_COMPLETION_TOKENS;
      }

      const response = await fetch("https://api.openai.com/v1/chat/completions", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          Authorization: `Bearer ${process.env.OPENAI_API_KEY}`
        },
        body: JSON.stringify(openAiPayload)
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

      respuestaIA = data.choices[0].message;
      programarPersistenciaRedis(
        () => guardarRespuestaAiCacheada(aiCacheOptions, respuestaIA?.content || ""),
        "cache IA de chat"
      );
    }

    registrarMensajeMemoria(sessionId, respuestaIA.role, respuestaIA.content);

    if (modoChat === "coach") {
      if (COACH_TURBO_MODE) {
        void Promise.allSettled([
          coachUserMessageTask,
          guardarMensajeRaw({
            visitorId: visitorIdLimpio,
            sessionId,
            profileId: null,
            leadId: null,
            role: "assistant",
            content: respuestaIA.content,
            intent: "coach_chat",
            detectedTopics: [`coach_user:${String(coachAuth.user._id)}`]
          }),
          actualizarPerfilYAnalyticsCoach({
            userDoc: coachAuth.user,
            sessionId,
            question: preguntaLimpia,
            reply: respuestaIA.content
          })
        ]).catch(error => {
          console.error("Error guardando respuesta turbo del Coach:", error.message);
        });

        return res.json({
          respuesta: respuestaIA.content,
          mode: modoChat,
          turboMode: true,
          turboLane: cacheHit ? "cache" : "model",
          cacheHit,
          repLeadSummary,
          activeLeadContext,
          activeHealthSurveyContext,
          activeProgram414Context,
          activeOrderCalcContext,
          activeDecisionContext,
          activeDemoOutcomeContext,
          usage: {
            usedToday: (coachUsage?.usedToday || 0) + 1,
            remainingToday: Math.max((coachUsage?.remainingToday || COACH_MAX_MESSAGES_PER_DAY) - 1, 0),
            limitPerDay: COACH_MAX_MESSAGES_PER_DAY
          }
        });
      }

      const [, coachProfileActualizado] = await Promise.all([
        guardarMensajeRaw({
          visitorId: visitorIdLimpio,
          sessionId,
          profileId: null,
          leadId: null,
          role: "assistant",
          content: respuestaIA.content,
          intent: "coach_chat",
          detectedTopics: [`coach_user:${String(coachAuth.user._id)}`]
        }),
        actualizarPerfilYAnalyticsCoach({
          userDoc: coachAuth.user,
          sessionId,
          question: preguntaLimpia,
          reply: respuestaIA.content
        })
      ]);

      return res.json({
        respuesta: respuestaIA.content,
        mode: modoChat,
        cacheHit,
        profile: limpiarCoachProfile(coachProfileActualizado?.profile, coachProfileActualizado?.analytics),
        repLeadSummary,
        activeLeadContext,
        activeHealthSurveyContext,
        activeProgram414Context,
        activeOrderCalcContext,
        activeDecisionContext,
        activeDemoOutcomeContext,
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
      coachChefContext,
      role: "assistant",
      content: respuestaIA.content,
      intent: "chef_chat",
      estadoConversacion: obtenerEstadoConversacion(sessionId)
    });
    await sincronizarLeadAGoogleSheets(leadFinal?.phone ? leadFinal : null, profileFinal);

    res.json({
      respuesta: respuestaIA.content,
      mode: modoChat,
      activeLeadContext,
      cacheHit
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
    iniciarCoachAsyncJobWorker();

    app.listen(PORT, () => {
      console.log(`Servidor corriendo en puerto ${PORT}`);
    });
  } catch (error) {
    console.error("Error conectando MongoDB Atlas:", error.message);
    process.exit(1);
  }
}

if (process.env.NODE_ENV !== "test") {
  iniciarServidor();
}

export {
  construirCoachCrmLeadSeedOperation,
  construirCoachCrmSummary,
  construirPayloadCoachLeadDesdeChef,
  detectarEnvioDatosContacto,
  extraerLeadInfo,
  limpiarCoachAgendaRecord,
  parseCoachLeadNextActionAt,
  resolverCoachCrmStatusDesdeDemoOutcome,
  resolverCoachCrmStatusDesdeLead
};
