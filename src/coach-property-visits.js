import mongoose from "mongoose";
import { PROPERTY_VISIT_STATUSES, propertyVisitKey } from "../public/coach/property-visit-helpers.js";

const statuses = new Set(PROPERTY_VISIT_STATUSES.map(item => item.value));
const schema = new mongoose.Schema({
  ownerUserId: { type: mongoose.Schema.Types.ObjectId, required: true },
  propertyKey: { type: String, required: true },
  requestId: { type: String, required: true },
  property: {
    pin: String, address: String, city: String, state: String, zipCode: String,
    formattedAddress: String, lat: Number, lon: Number
  },
  status: { type: String, enum: [...statuses], required: true },
  notes: String,
  nextVisitAt: { type: Date, default: null },
  clientUpdatedAt: { type: Date, required: true },
  linkedLeadId: { type: String, default: "" },
  createdAt: { type: Date, default: Date.now }
});
schema.index({ ownerUserId: 1, requestId: 1 }, { unique: true });
schema.index({ ownerUserId: 1, propertyKey: 1, clientUpdatedAt: -1, createdAt: -1 });
const CoachPropertyVisit = mongoose.models.CoachPropertyVisit || mongoose.model("CoachPropertyVisit", schema);

function failure(status, message) { return Object.assign(new Error(message), { status }); }
function text(value, max, label) {
  if (typeof value !== "string" || value.length > max || /\p{Cc}/u.test(value.replace(/[\n\r\t]/g, ""))) throw failure(400, `Revisa ${label}.`);
  return value.trim();
}

export function validatePropertySnapshot(input) {
  if (!input || typeof input !== "object" || Array.isArray(input)) throw failure(400, "Selecciona una propiedad del mapa.");
  const address = text(input.address, 160, "la dirección"), city = text(input.city, 60, "la ciudad");
  if (!address || !city || !/^\d{14}$/.test(input.pin || "")) throw failure(400, "Selecciona una propiedad válida del mapa.");
  const lat = Number(input.lat), lon = Number(input.lon);
  if (input.lat == null || input.lon == null || !Number.isFinite(lat) || !Number.isFinite(lon) || lat <= 41 || lat >= 43 || lon <= -89 || lon >= -87) throw failure(400, "La propiedad no tiene una ubicación válida.");
  const zipCode = text(input.zipCode || "", 10, "el código postal");
  return { pin: input.pin, address, city, state: "IL", zipCode, lat, lon, formattedAddress: [address, city, "IL", zipCode].filter(Boolean).join(", ") };
}

function timestamp(value, now) {
  const parsed = new Date(value);
  if (typeof value !== "string" || !Number.isFinite(parsed.getTime()) || parsed.getUTCFullYear() < 2000) throw failure(400, "No pude leer la fecha del seguimiento.");
  return new Date(Math.min(parsed.getTime(), now()));
}

function serialize(record) {
  const iso = value => value ? new Date(value).toISOString() : null;
  return {
    id: String(record._id), propertyKey: record.propertyKey, requestId: record.requestId,
    property: record.property, status: record.status, notes: record.notes || "",
    nextVisitAt: iso(record.nextVisitAt), clientUpdatedAt: iso(record.clientUpdatedAt),
    updatedAt: iso(record.createdAt), linkedLeadId: String(record.linkedLeadId || "")
  };
}

export function createPropertyVisitService({ model = CoachPropertyVisit, now = Date.now, toOwnerId = id => new mongoose.Types.ObjectId(String(id)) } = {}) {
  async function list(userId, keys = null) {
    if (keys !== null && (!Array.isArray(keys) || keys.length > 61 || keys.some(key => typeof key !== "string" || key.length > 240 || !key.endsWith("|IL")))) throw failure(400, "La lista de propiedades no es válida.");
    if (keys?.length === 0) return [];
    const records = await model.aggregate([
      { $match: { ownerUserId: toOwnerId(userId), ...(keys ? { propertyKey: { $in: keys } } : {}) } },
      { $sort: { clientUpdatedAt: -1, createdAt: -1, _id: -1 } },
      { $group: { _id: "$propertyKey", record: { $first: "$$ROOT" }, linkedLeadId: { $max: "$linkedLeadId" } } },
      { $sort: { "record.clientUpdatedAt": -1, "record.createdAt": -1 } },
      { $limit: keys ? Math.max(1, keys.length) : 100 }
    ]);
    return records.map(item => serialize({ ...item.record, linkedLeadId: item.linkedLeadId || item.record.linkedLeadId }));
  }

  async function findRequest(userId, requestId) {
    return model.findOne({ ownerUserId: toOwnerId(userId), requestId }).lean();
  }

  function requestKey(requestId) {
    if (typeof requestId !== "string" || !/^[a-zA-Z0-9:_-]{8,100}$/.test(requestId)) throw failure(400, "Falta el identificador de guardado. Intenta guardar nuevamente.");
    return requestId;
  }

  async function save(userId, input, requestId, { linkedLeadId = "" } = {}) {
    requestKey(requestId);
    const property = validatePropertySnapshot(input.property);
    const propertyKey = propertyVisitKey(property);
    if (!statuses.has(input.status)) throw failure(400, "Selecciona qué pasó durante la visita.");
    const notes = text(input.notes ?? "", 2000, "las notas (máximo 2000 caracteres)");
    let nextVisitAt = null;
    if (["return_later", "appointment"].includes(input.status)) {
      nextVisitAt = new Date(input.nextVisitAt);
      if (!input.nextVisitAt || !Number.isFinite(nextVisitAt.getTime()) || nextVisitAt.getUTCFullYear() < 2000) throw failure(400, "Indica la fecha y hora para volver o para la cita.");
    }
    const clientUpdatedAt = timestamp(input.clientUpdatedAt, now);
    let existing = await findRequest(userId, requestId);
    if (!existing) {
      try {
        await model.init?.();
        const created = await model.create({ ownerUserId: toOwnerId(userId), propertyKey, requestId, property, status: input.status, notes, nextVisitAt, clientUpdatedAt, linkedLeadId, createdAt: new Date(now()) });
        existing = created.toObject ? created.toObject() : created;
      } catch (error) {
        if (error.code !== 11000) throw error;
        existing = await findRequest(userId, requestId);
        if (!existing) throw error;
      }
    }
    if (existing.propertyKey !== propertyKey) throw failure(409, "El identificador corresponde a otra propiedad. Guarda nuevamente.");
    const [latest] = await list(userId, [propertyKey]);
    return latest || serialize(existing);
  }

  async function convertToLead(user, input, requestId, createLead) {
    requestKey(requestId);
    const property = validatePropertySnapshot(input.property), key = propertyVisitKey(property);
    const previousRequest = await findRequest(user._id, requestId);
    if (previousRequest) {
      if (previousRequest.propertyKey !== key) throw failure(409, "Ese guardado corresponde a otra propiedad.");
      const [visit] = await list(user._id, [key]);
      return { visit, leadId: previousRequest.linkedLeadId };
    }
    const [current] = await list(user._id, [key]);
    if (!current) throw failure(400, "Guarda el seguimiento antes de crear el lead.");
    if (current.linkedLeadId) return { visit: current, leadId: current.linkedLeadId, duplicate: true };
    const fullName = text(input.fullName, 120, "el nombre del contacto"), phone = text(input.phone || "", 40, "el teléfono"), email = text(input.email || "", 160, "el correo");
    if (!fullName || (!phone && !email)) throw failure(400, "Escribe el nombre y al menos un teléfono o correo del contacto.");
    const digits = phone.replace(/\D/g, "");
    if (phone && (digits.length < 10 || digits.length > 15)) throw failure(400, "Revisa el teléfono del contacto.");
    if (email && !/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email)) throw failure(400, "Revisa el correo del contacto.");
    const result = await createLead(user, {
      fullName, phone, email, address: property.address, city: property.city, zipCode: property.zipCode,
      source: "captura_manual", sourceDetail: "Mapa de propiedades", notes: `Visita desde Mapa: ${property.formattedAddress}.\n${current.notes}`.slice(0, 1200),
      ...(current.nextVisitAt ? { nextAction: current.status === "appointment" ? "cita" : "seguimiento", nextActionAt: current.nextVisitAt } : {}),
      offlineRequestId: requestId
    });
    const leadId = String(result.lead?.id || result.leadDoc?._id || "");
    if (!leadId) throw failure(503, "No pude confirmar el lead. El envío se puede reintentar sin duplicarlo.");
    const visit = await save(user._id, { property, status: current.status, notes: current.notes, nextVisitAt: current.nextVisitAt, clientUpdatedAt: input.clientUpdatedAt }, requestId, { linkedLeadId: leadId });
    return { visit, leadId, duplicate: Boolean(result.duplicate) };
  }
  return { list, save, convertToLead };
}

export function registerCoachPropertyVisitRoutes(app, { requireCoachActivo, createLead, service = createPropertyVisitService() }) {
  const authorized = handler => async (req, res, next) => {
    res.set("Cache-Control", "private, no-store");
    try {
      const auth = await requireCoachActivo(req, res);
      if (!auth) return;
      if (req.method !== "GET" && req.path !== "/api/coach/property-visits/lookup" && String(req.body?.accountId || "") !== String(auth.user._id)) {
        return res.status(403).json({ error: "Este seguimiento pertenece a otra cuenta. Entra con la cuenta que lo guardó para sincronizarlo." });
      }
      await handler(req, res, auth.user);
    } catch (error) {
      if (error.status) res.status(error.status).json({ error: error.message });
      else { console.error("Error en seguimiento de propiedades:", error.name || "Error"); res.status(503).json({ error: "No pude guardar o cargar el seguimiento. Tus pendientes se pueden reintentar." }); }
    }
  };
  app.get("/api/coach/property-visits", authorized(async (_req, res, user) => res.json({ visits: await service.list(user._id) })));
  app.post("/api/coach/property-visits/lookup", authorized(async (req, res, user) => res.json({ visits: await service.list(user._id, req.body?.keys || []) })));
  app.post("/api/coach/property-visits", authorized(async (req, res, user) => res.json({ visit: await service.save(user._id, req.body || {}, req.headers["x-tec-request-id"]) })));
  app.post("/api/coach/property-visits/lead", authorized(async (req, res, user) => res.json(await service.convertToLead(user, req.body || {}, req.headers["x-tec-request-id"], createLead))));
}
