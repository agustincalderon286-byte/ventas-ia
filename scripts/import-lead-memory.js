import "dotenv/config";
import crypto from "crypto";
import fs from "fs/promises";
import path from "path";
import mongoose from "mongoose";

const DEFAULT_4EN14_CSV = path.join(process.cwd(), "tmp_exports", "4-en-14-cnl.csv");
const DEFAULT_RIFA_CSV = path.join(process.cwd(), "tmp_exports", "esmeralda-rifa-digital.csv");
const DEFAULT_OUTPUT_DIR = path.join(process.cwd(), "tmp_exports", "normalized");

const STATUS_MAP = new Map([
  ["no contesto1", "no_contesto"],
  ["llamar mas adelnate", "reagendar"],
  ["no interesado", "no_interesado"],
  ["cita agendada", "cita_agendada"],
  ["sin servicio", "sin_servicio"],
  ["no califica", "no_califica"],
  ["cosinado", "cocinando_o_ocupado"],
  ["cosinado ", "cocinando_o_ocupado"],
  ["no llamar", "no_llamar"]
]);

function parseArgs(argv) {
  const options = {
    csv4En14: DEFAULT_4EN14_CSV,
    csvRifa: DEFAULT_RIFA_CSV,
    outputDir: DEFAULT_OUTPUT_DIR,
    skipImport: false
  };

  for (let index = 0; index < argv.length; index += 1) {
    const arg = argv[index];

    if (arg === "--skip-import") {
      options.skipImport = true;
      continue;
    }

    if (arg === "--csv-4en14") {
      options.csv4En14 = argv[index + 1];
      index += 1;
      continue;
    }

    if (arg === "--csv-rifa") {
      options.csvRifa = argv[index + 1];
      index += 1;
      continue;
    }

    if (arg === "--output-dir") {
      options.outputDir = argv[index + 1];
      index += 1;
      continue;
    }
  }

  return options;
}

function cleanText(value = "") {
  return String(value || "").replace(/\uFEFF/g, "").replace(/\s+/g, " ").trim();
}

function cleanLower(value = "") {
  return cleanText(value).toLowerCase();
}

function slugify(value = "") {
  return cleanLower(value)
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/[^a-z0-9]+/g, "_")
    .replace(/^_+|_+$/g, "");
}

function parseCsv(content) {
  const rows = [];
  let row = [];
  let cell = "";
  let inQuotes = false;

  for (let index = 0; index < content.length; index += 1) {
    const char = content[index];
    const next = content[index + 1];

    if (char === '"') {
      if (inQuotes && next === '"') {
        cell += '"';
        index += 1;
      } else {
        inQuotes = !inQuotes;
      }
      continue;
    }

    if (char === "," && !inQuotes) {
      row.push(cell);
      cell = "";
      continue;
    }

    if ((char === "\n" || char === "\r") && !inQuotes) {
      if (char === "\r" && next === "\n") {
        index += 1;
      }
      row.push(cell);
      cell = "";
      if (row.some(entry => entry !== "")) {
        rows.push(row);
      }
      row = [];
      continue;
    }

    cell += char;
  }

  row.push(cell);
  if (row.some(entry => entry !== "")) {
    rows.push(row);
  }

  return rows;
}

function parseCsvFile(content) {
  const rows = parseCsv(content);

  if (!rows.length) {
    return { headers: [], dataRows: [] };
  }

  const headers = rows[0].map(value => String(value || "").replace(/\uFEFF/g, ""));
  const dataRows = rows.slice(1).filter(row => row.some(cell => cleanText(cell)));

  return { headers, dataRows };
}

function normalizePhone(raw = "") {
  const digits = String(raw || "").replace(/\D/g, "");

  if (!digits) {
    return "";
  }

  if (digits.length === 10) {
    return `+1${digits}`;
  }

  if (digits.length === 11 && digits.startsWith("1")) {
    return `+${digits}`;
  }

  return digits;
}

function normalizeDate(raw = "") {
  const text = cleanText(raw);

  if (!text) {
    return null;
  }

  const parsed = new Date(text);

  if (!Number.isNaN(parsed.getTime())) {
    return parsed.toISOString();
  }

  return null;
}

function normalizeBoolean(raw = "") {
  const value = cleanLower(raw);

  if (!value) {
    return null;
  }

  if (["si", "sí", "yes", "true"].includes(value)) {
    return true;
  }

  if (["no", "false"].includes(value)) {
    return false;
  }

  return null;
}

function isTestRecord(...values) {
  return values.some(value => /test|prueb/i.test(String(value || "")));
}

function createStableId(...parts) {
  return crypto.createHash("sha1").update(parts.map(part => cleanText(part)).join("|")).digest("hex");
}

function extractFirstMatch(text, pattern) {
  const match = String(text || "").match(pattern);
  return match ? cleanText(match[1] || match[0]) : "";
}

function deriveRelationshipHint(note = "") {
  const value = cleanLower(note);
  const options = [
    "hermano",
    "hermana",
    "tia",
    "tio",
    "prima",
    "primo",
    "vecina",
    "vecino",
    "amiga",
    "amigo",
    "sobrino",
    "sobrina",
    "compañero",
    "companero",
    "cuñada",
    "cunada",
    "clienta",
    "cliente"
  ];

  return options.find(option => value.includes(option)) || "";
}

function deriveInterestHint(note = "") {
  const value = cleanLower(note);

  if (value.includes("quiere trabajar") || value.includes("tranajr en royal") || value.includes("trabajar en royal")) {
    return "oportunidad_royal";
  }

  if (value.includes("quiere credito")) {
    return "credito";
  }

  if (value.includes("quiere comprar")) {
    return "compra";
  }

  if (value.includes("tiene royal")) {
    return "ya_tiene_royal";
  }

  return "";
}

function deriveMaritalHint(note = "") {
  const value = cleanLower(note);

  if (value.includes("casad")) {
    return "casado";
  }

  if (value.includes("solter")) {
    return "soltero";
  }

  return "";
}

function deriveChildrenHint(note = "") {
  const value = cleanLower(note);

  if (value.includes("sin hijos")) {
    return "sin_hijos";
  }

  if (value.includes("hijos")) {
    return "con_hijos";
  }

  return "";
}

function deriveEmploymentHint(note = "") {
  const value = cleanLower(note);

  if (value.includes("no trabaja")) {
    return "no_trabaja";
  }

  if (value.includes("trabaja")) {
    return "trabaja";
  }

  return "";
}

function summarizeText(...parts) {
  const joined = parts.map(cleanText).filter(Boolean).join(" | ");
  if (!joined) {
    return "";
  }
  if (joined.length <= 220) {
    return joined;
  }
  return `${joined.slice(0, 217).trim()}...`;
}

function normalizeCallStatus(raw = "") {
  const slug = slugify(raw);
  return STATUS_MAP.get(slug.replace(/_+/g, " ")) || slug || "";
}

function estimateAttemptCount(callLog = "") {
  const value = cleanText(callLog);
  if (!value) {
    return 0;
  }

  const segments = value
    .split("//")
    .map(segment => cleanText(segment))
    .filter(Boolean);

  return segments.length || 1;
}

function splitAttemptSegments(callLog = "") {
  return cleanText(callLog)
    .split("//")
    .map(segment => cleanText(segment))
    .filter(Boolean);
}

function deriveLeadTemperature(status = "", notes = "", callLog = "") {
  if (status === "cita_agendada") {
    return "hot";
  }

  if (status === "no_interesado" || status === "no_llamar") {
    return "dead";
  }

  if (status === "reagendar") {
    return "warm";
  }

  if (status === "sin_servicio" || status === "no_contesto") {
    return "cold";
  }

  const text = cleanLower(`${notes} ${callLog}`);

  if (/cita|agend/.test(text)) {
    return "hot";
  }

  if (/interes|amable|le gusta|productos son buenos/.test(text)) {
    return "warm";
  }

  return "cold";
}

function derivePrimaryObjection(status = "", notes = "", callLog = "") {
  const text = cleanLower(`${status} ${notes} ${callLog}`);

  if (/esposo|esposa/.test(text)) {
    return "pareja_no_presente";
  }

  if (/viaje|vuelve en un mes/.test(text)) {
    return "sin_tiempo_por_viaje";
  }

  if (/trabaja|llega muy tarde/.test(text)) {
    return "horario_complicado";
  }

  if (/no interesado|colgo|no llamar/.test(text)) {
    return "desinteres";
  }

  if (/buzon|no contesto|sin servicio/.test(text)) {
    return "no_contacto";
  }

  return "";
}

function deriveBestScriptAngle(profile = {}, state = {}) {
  const productInterest = cleanLower(profile.productInterestRaw);
  const notes = cleanLower(`${profile.notesRaw} ${state.callLogRaw}`);
  const water = cleanLower(profile.waterSourceRaw);
  const owned = cleanLower(profile.productsOwnedRaw);

  if (/aro|valvula|manchad|limpiador/.test(notes) || /aro|valvula/.test(owned)) {
    return "reemplazo_y_servicio";
  }

  if (/filtro|agua/.test(productInterest) || /filtro|agua/.test(water) || /salud/.test(notes)) {
    return "agua_y_salud";
  }

  if (/extractor|jugo|blender/.test(productInterest)) {
    return "jugos_y_salud";
  }

  if (/sistema|cosina|cocina|olla|ollas/.test(productInterest)) {
    return "cocina_y_familia";
  }

  if (cleanText(profile.prizeWonRaw)) {
    return "regalo_y_visita";
  }

  return "descubrimiento";
}

function deriveTopicFlags(profile = {}, state = {}) {
  const text = cleanLower(
    [
      profile.waterSourceRaw,
      profile.productInterestRaw,
      profile.productsOwnedRaw,
      profile.notesRaw,
      state.callLogRaw
    ]
      .filter(Boolean)
      .join(" ")
  );

  return {
    interestWater: /agua|filtro|botella|llave/.test(text),
    interestFilter: /filtro|frescapure|frescaflow/.test(text),
    interestCookingSystem: /sistema|cosina|cocina|olla|ollas|easy release/.test(text),
    interestExtractor: /extractor|jugo|jugo|blender/.test(text),
    interestHealth: /salud|filtr|agua|cocina saludable/.test(text),
    ownsCompetitor: /renaware|competencia|otra marca/.test(text),
    needsReplacementParts: /aro|aros|valvula|válvula|limpiador|manchad/.test(text),
    requiresSpousePresent: /esposo|esposa/.test(text),
    worksLate: /trabaja|llega muy tarde/.test(text),
    travellingOrUnavailable: /viaje|vuelve en un mes/.test(text),
    callbackRecommended: /reagendar|llamar mas|llamar después|llamar despues/.test(text) || state.callStatusNormalized === "reagendar",
    conversionSignal:
      state.callStatusNormalized === "cita_agendada" || /cita|agend|productos son buenos|le gustan/.test(text)
  };
}

function build4En14Dataset(headers, rows) {
  const events = [];
  const referrals = [];
  let skippedTests = 0;

  for (const row of rows) {
    const hostName = row[1] || "";
    const repName = row[4] || "";

    if (!cleanText(hostName) && !cleanText(repName)) {
      continue;
    }

    if (isTestRecord(hostName, repName)) {
      skippedTests += 1;
      continue;
    }

    const sourceRowId = createStableId(row[0], hostName, row[2], repName, row[5]);
    const event = {
      source: "4_en_14",
      sourceRowId,
      createdAtRaw: cleanText(row[0]),
      createdAt: normalizeDate(row[0]),
      hostName: cleanText(hostName),
      hostPhoneRaw: cleanText(row[2]),
      hostPhone: normalizePhone(row[2]),
      giftSelected: cleanText(row[3]),
      repName: cleanText(repName),
      repPhoneRaw: cleanText(row[5]),
      repPhone: normalizePhone(row[5]),
      programWindowRaw: cleanText(row[6]),
      programStartDate: null,
      programEndDate: null,
      referralCount: 0,
      importedAt: new Date().toISOString()
    };

    for (let index = 7, slot = 1; index < headers.length; index += 3, slot += 1) {
      const referralName = row[index] || "";
      const referralPhone = row[index + 1] || "";
      const referralNote = row[index + 2] || "";

      if (!cleanText(referralName) && !cleanText(referralPhone) && !cleanText(referralNote)) {
        continue;
      }

      if (isTestRecord(referralName, referralNote)) {
        continue;
      }

      const note = cleanText(referralNote);
      referrals.push({
        source: "4_en_14",
        referralId: createStableId(sourceRowId, slot, referralName, referralPhone, referralNote),
        eventSourceRowId: sourceRowId,
        slotIndex: slot,
        referralName: cleanText(referralName),
        referralPhoneRaw: cleanText(referralPhone),
        referralPhone: normalizePhone(referralPhone),
        referralNoteRaw: note,
        relationshipHint: deriveRelationshipHint(note),
        interestHint: deriveInterestHint(note),
        locationHint: extractFirstMatch(note, /vive en ([^,]+)/i),
        maritalHint: deriveMaritalHint(note),
        childrenHint: deriveChildrenHint(note),
        employmentHint: deriveEmploymentHint(note),
        importedAt: new Date().toISOString()
      });
    }

    event.referralCount = referrals.filter(referral => referral.eventSourceRowId === sourceRowId).length;
    events.push(event);
  }

  return {
    events,
    referrals,
    summary: {
      totalRows: rows.length,
      importedEvents: events.length,
      importedReferrals: referrals.length,
      skippedTests
    }
  };
}

function buildRifaDataset(headers, rows) {
  const profiles = [];
  const states = [];
  const insights = [];
  const attempts = [];
  let skippedTests = 0;

  const headerIndex = Object.fromEntries(headers.map((header, index) => [header, index]));
  const get = (row, name) => row[headerIndex[name]] || "";

  for (const row of rows) {
    const leadName = get(row, "Nombre");
    const phone = get(row, "Telefono");
    const rawLeadId = cleanText(get(row, "Lead ID"));

    if (!cleanText(leadName) && !cleanText(phone) && !rawLeadId) {
      continue;
    }

    if (isTestRecord(leadName, get(row, "Email"))) {
      skippedTests += 1;
      continue;
    }

    const leadId = rawLeadId || createStableId(get(row, "Timestamp"), leadName, phone);
    const profile = {
      source: "rifa_digital",
      leadId,
      createdAtRaw: cleanText(get(row, "Timestamp")),
      createdAt: normalizeDate(get(row, "Timestamp")),
      leadName: cleanText(leadName),
      email: cleanLower(get(row, "Email")),
      phoneRaw: cleanText(phone),
      phone: normalizePhone(phone),
      waterSourceRaw: cleanText(get(row, "Toma Agua")),
      bestCallWindowRaw: cleanText(get(row, "Mejor hora para llamar")),
      scheduledCallWindowRaw: cleanText(get(row, "Hora Para Llamar")),
      productInterestRaw: cleanText(get(row, "A cual producto le daria mas uso")),
      knowsBrand: normalizeBoolean(get(row, "Conose ")),
      hasRoyalPrestige: normalizeBoolean(get(row, "Tiene Royal Prestige")),
      productsOwnedRaw: cleanText(get(row, "Que productos tiene?")),
      repName: cleanText(get(row, "Nombre del representante")),
      addressRaw: cleanText(get(row, "Direccion")),
      homeTypeRaw: cleanText(get(row, "Que es?")),
      notesRaw: cleanText(get(row, "Cita/Notas")),
      eventSource: cleanText(get(row, "Donde Participo?")),
      prizeWonRaw: cleanText(get(row, "Que se gano?")),
      maritalStatusRaw: cleanText(get(row, "Casado?")),
      followupFlagRaw: cleanText(get(row, "Cita")),
      sourceDateRaw: cleanText(get(row, "Fecha")),
      importedAt: new Date().toISOString()
    };

    const state = {
      leadId,
      callStatusRaw: cleanText(get(row, "Estado de llamada")),
      callStatusNormalized: normalizeCallStatus(get(row, "Estado de llamada")),
      callLogRaw: cleanText(get(row, "Hora de primera llamada")),
      responseTimeRaw: cleanText(get(row, "Tiempo D Respuesta")),
      nextStep: "",
      appointmentDetected: false,
      doNotCall: false,
      hasVoicemailPattern: false,
      attemptCountEstimated: 0,
      lastContactNoteSummary: "",
      importedAt: new Date().toISOString()
    };

    state.appointmentDetected =
      state.callStatusNormalized === "cita_agendada" ||
      /cita|miercoles|miércoles|jueves|viernes|sabado|sábado|domingo|lunes|martes/.test(
        cleanLower(`${profile.notesRaw} ${state.callLogRaw}`)
      );
    state.doNotCall = state.callStatusNormalized === "no_llamar";
    state.hasVoicemailPattern = /buzon/.test(cleanLower(state.callLogRaw));
    state.attemptCountEstimated = estimateAttemptCount(state.callLogRaw);
    state.lastContactNoteSummary = summarizeText(profile.notesRaw, state.callLogRaw);
    state.nextStep =
      state.doNotCall
        ? "descartar"
        : state.callStatusNormalized === "cita_agendada"
          ? "confirmar_cita"
          : state.callStatusNormalized === "reagendar"
            ? "reagendar"
            : state.callStatusNormalized === "no_interesado"
              ? "cerrar_sin_interes"
              : state.callStatusNormalized === "sin_servicio"
                ? "validar_numero"
                : "nuevo_intento";

    const flags = deriveTopicFlags(profile, state);
    const insight = {
      leadId,
      leadTemperature: deriveLeadTemperature(state.callStatusNormalized, profile.notesRaw, state.callLogRaw),
      interestWater: flags.interestWater,
      interestFilter: flags.interestFilter,
      interestCookingSystem: flags.interestCookingSystem,
      interestExtractor: flags.interestExtractor,
      interestHealth: flags.interestHealth,
      ownsCompetitor: flags.ownsCompetitor,
      needsReplacementParts: flags.needsReplacementParts,
      requiresSpousePresent: flags.requiresSpousePresent,
      worksLate: flags.worksLate,
      travellingOrUnavailable: flags.travellingOrUnavailable,
      callbackRecommended: flags.callbackRecommended,
      conversionSignal: flags.conversionSignal,
      primaryObjection: derivePrimaryObjection(state.callStatusNormalized, profile.notesRaw, state.callLogRaw),
      bestScriptAngle: deriveBestScriptAngle(profile, state),
      importedAt: new Date().toISOString()
    };

    splitAttemptSegments(state.callLogRaw).forEach((segment, attemptIndex) => {
      attempts.push({
        attemptId: createStableId(leadId, attemptIndex + 1, segment),
        leadId,
        attemptIndex: attemptIndex + 1,
        attemptDatetimeRaw: extractFirstMatch(segment, /([A-ZÁÉÍÓÚ]+ \d{2}-\d{2}-\d{2} HORA [^/]+)/i),
        attemptOutcomeRaw: segment,
        attemptOutcomeNormalized: normalizeCallStatus(segment),
        attemptRepName: extractFirstMatch(segment, /SE LLAMA ([A-ZÁÉÍÓÚ ]+)/i),
        importedAt: new Date().toISOString()
      });
    });

    profiles.push(profile);
    states.push(state);
    insights.push(insight);
  }

  return {
    profiles,
    states,
    insights,
    attempts,
    summary: {
      totalRows: rows.length,
      importedProfiles: profiles.length,
      importedStates: states.length,
      importedInsights: insights.length,
      importedAttempts: attempts.length,
      skippedTests
    }
  };
}

async function writeJson(outputPath, data) {
  await fs.mkdir(path.dirname(outputPath), { recursive: true });
  await fs.writeFile(outputPath, JSON.stringify(data, null, 2), "utf8");
}

async function importCollections(payload) {
  if (!process.env.MONGODB_URI) {
    throw new Error("MONGODB_URI no configurada");
  }

  await mongoose.connect(process.env.MONGODB_URI);

  try {
    const db = mongoose.connection.db;
    const collections = {
      eventos4en14: db.collection("agustin_programa_4en14_eventos"),
      referidos4en14: db.collection("agustin_programa_4en14_referidos"),
      rifaProfiles: db.collection("agustin_rifa_lead_profiles"),
      rifaState: db.collection("agustin_rifa_lead_contact_state"),
      rifaInsights: db.collection("agustin_rifa_lead_insights"),
      rifaAttempts: db.collection("agustin_rifa_contact_attempts"),
      importRuns: db.collection("agustin_lead_memory_import_runs")
    };

    await Promise.all([
      collections.eventos4en14.deleteMany({}),
      collections.referidos4en14.deleteMany({}),
      collections.rifaProfiles.deleteMany({}),
      collections.rifaState.deleteMany({}),
      collections.rifaInsights.deleteMany({}),
      collections.rifaAttempts.deleteMany({})
    ]);

    if (payload.eventos4en14.length) {
      await collections.eventos4en14.insertMany(payload.eventos4en14, { ordered: false });
    }
    if (payload.referidos4en14.length) {
      await collections.referidos4en14.insertMany(payload.referidos4en14, { ordered: false });
    }
    if (payload.rifaProfiles.length) {
      await collections.rifaProfiles.insertMany(payload.rifaProfiles, { ordered: false });
    }
    if (payload.rifaState.length) {
      await collections.rifaState.insertMany(payload.rifaState, { ordered: false });
    }
    if (payload.rifaInsights.length) {
      await collections.rifaInsights.insertMany(payload.rifaInsights, { ordered: false });
    }
    if (payload.rifaAttempts.length) {
      await collections.rifaAttempts.insertMany(payload.rifaAttempts, { ordered: false });
    }

    await Promise.all([
      collections.eventos4en14.createIndex({ sourceRowId: 1 }, { unique: true }),
      collections.referidos4en14.createIndex({ referralId: 1 }, { unique: true }),
      collections.referidos4en14.createIndex({ eventSourceRowId: 1 }),
      collections.rifaProfiles.createIndex({ leadId: 1 }, { unique: true }),
      collections.rifaProfiles.createIndex({ phone: 1 }),
      collections.rifaProfiles.createIndex({ repName: 1 }),
      collections.rifaState.createIndex({ leadId: 1 }, { unique: true }),
      collections.rifaInsights.createIndex({ leadId: 1 }, { unique: true }),
      collections.rifaAttempts.createIndex({ attemptId: 1 }, { unique: true }),
      collections.rifaAttempts.createIndex({ leadId: 1, attemptIndex: 1 })
    ]);

    await collections.importRuns.insertOne({
      importedAt: new Date(),
      summary: payload.summary
    });
  } finally {
    await mongoose.disconnect();
  }
}

async function main() {
  const options = parseArgs(process.argv.slice(2));
  const csv4En14 = await fs.readFile(options.csv4En14, "utf8");
  const csvRifa = await fs.readFile(options.csvRifa, "utf8");

  const parsed4En14 = parseCsvFile(csv4En14);
  const parsedRifa = parseCsvFile(csvRifa);

  const dataset4En14 = build4En14Dataset(parsed4En14.headers, parsed4En14.dataRows);
  const datasetRifa = buildRifaDataset(parsedRifa.headers, parsedRifa.dataRows);

  const summary = {
    importedAt: new Date().toISOString(),
    files: {
      csv4En14: options.csv4En14,
      csvRifa: options.csvRifa
    },
    counts: {
      eventos4En14: dataset4En14.summary.importedEvents,
      referidos4En14: dataset4En14.summary.importedReferrals,
      rifaProfiles: datasetRifa.summary.importedProfiles,
      rifaState: datasetRifa.summary.importedStates,
      rifaInsights: datasetRifa.summary.importedInsights,
      rifaAttempts: datasetRifa.summary.importedAttempts
    },
    skipped: {
      tests4En14: dataset4En14.summary.skippedTests,
      testsRifa: datasetRifa.summary.skippedTests
    }
  };

  await fs.mkdir(options.outputDir, { recursive: true });
  await Promise.all([
    writeJson(path.join(options.outputDir, "4en14-eventos.json"), dataset4En14.events),
    writeJson(path.join(options.outputDir, "4en14-referidos.json"), dataset4En14.referrals),
    writeJson(path.join(options.outputDir, "rifa-lead-profiles.json"), datasetRifa.profiles),
    writeJson(path.join(options.outputDir, "rifa-lead-contact-state.json"), datasetRifa.states),
    writeJson(path.join(options.outputDir, "rifa-lead-insights.json"), datasetRifa.insights),
    writeJson(path.join(options.outputDir, "rifa-contact-attempts.json"), datasetRifa.attempts),
    writeJson(path.join(options.outputDir, "lead-memory-summary.json"), summary)
  ]);

  if (!options.skipImport) {
    await importCollections({
      eventos4en14: dataset4En14.events,
      referidos4en14: dataset4En14.referrals,
      rifaProfiles: datasetRifa.profiles,
      rifaState: datasetRifa.states,
      rifaInsights: datasetRifa.insights,
      rifaAttempts: datasetRifa.attempts,
      summary
    });
  }

  console.log(JSON.stringify(summary, null, 2));
}

main().catch(error => {
  console.error("Error importando lead memory:", error.message);
  process.exit(1);
});
