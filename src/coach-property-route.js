import mongoose from 'mongoose';
import { validatePropertySnapshot } from './coach-property-visits.js';
import { propertyVisitKey } from '../public/coach/property-visit-helpers.js';
const schema = new mongoose.Schema({
  ownerUserId: { type: mongoose.Schema.Types.ObjectId, required: true, unique: true },
  requestId: { type: String, required: true },
  clientUpdatedAt: { type: String, required: true },
  anchor: mongoose.Schema.Types.Mixed,
  stops: [mongoose.Schema.Types.Mixed],
  travelMode: { type: String, enum: ['walking','driving'], default: 'walking' }
}, { timestamps: true });
const Model = mongoose.models.CoachPropertyRoute || mongoose.model('CoachPropertyRoute', schema);
const fail = message => Object.assign(new Error(message), { status: 400 });
function validTime(value, now) {
  const date = new Date(value);
  if (typeof value !== 'string' || !Number.isFinite(date.getTime()) || date.getUTCFullYear() < 2000 || date.getTime() > now() + 300000) throw fail('Revisa la fecha y hora de tu dispositivo para guardar la ruta.');
  return date.toISOString();
}
export function validatePropertyRoute(input, now = Date.now) {
  if (!input || !Array.isArray(input.stops) || input.stops.length > 50) throw fail('La ruta admite hasta 50 casas.');
  if (!['walking','driving'].includes(input.travelMode)) throw fail('Selecciona un recorrido a pie o en auto.');
  const seen = new Set();
  const stops = input.stops.map(stop => {
    const property = validatePropertySnapshot(stop?.property), key = propertyVisitKey(property);
    if (seen.has(key)) throw fail('Una casa no puede aparecer dos veces en la misma ruta.');
    seen.add(key);
    return { property, completedAt: stop.completedAt == null ? null : validTime(stop.completedAt, now) };
  });
  const anchor = input.anchor ? validatePropertySnapshot(input.anchor) : null;
  if (stops.length && !anchor) throw fail('Falta el punto inicial del recorrido.');
  return { anchor, stops, travelMode: input.travelMode, clientUpdatedAt: validTime(input.clientUpdatedAt, now) };
}
const serialize = record => record ? ({ anchor: record.anchor || null, stops: record.stops, travelMode: record.travelMode, clientUpdatedAt: record.clientUpdatedAt, requestId: record.requestId }) : null;
export function createPropertyRouteService({ model = Model, now = Date.now, toOwnerId = id => new mongoose.Types.ObjectId(String(id)) } = {}) {
  const get = async userId => serialize(await model.findOne({ ownerUserId: toOwnerId(userId) }).lean());
  async function save(userId, input, requestId) {
    if (typeof requestId !== 'string' || !/^[a-zA-Z0-9:_-]{8,100}$/.test(requestId)) throw fail('Falta identificar el guardado de la ruta.');
    const route = validatePropertyRoute(input, now), ownerUserId = toOwnerId(userId);
    await model.init?.();
    // An older offline snapshot cannot reset newer progress. The owner unique
    // index also makes initial concurrent writes safe and retries idempotent.
    const filter = { ownerUserId, $or: [ { clientUpdatedAt: { $lt: route.clientUpdatedAt } }, { clientUpdatedAt: route.clientUpdatedAt, requestId: { $lt: requestId } } ] };
    try {
      const stored = await model.findOneAndUpdate(filter, { $set: { ownerUserId, ...route, requestId } }, { upsert: true, new: true, runValidators: true }).lean();
      return serialize(stored);
    } catch (error) {
      if (error.code !== 11000) throw error;
      // Another initial insert may have won the unique-index race. Retry the
      // conditional update without upsert so a newer incoming snapshot still wins.
      const retried = await model.findOneAndUpdate(filter, { $set: { ownerUserId, ...route, requestId } }, { new: true, runValidators: true }).lean();
      const current = retried ? serialize(retried) : await get(userId);
      if (!current) throw error;
      return current;
    }
  }
  return { get, save };
}
export function registerCoachPropertyRoute(app, { requireCoachActivo, service = createPropertyRouteService() }) {
  const authorized = handler => async (req,res) => {
    res.set('Cache-Control','private, no-store');
    try {
      const auth = await requireCoachActivo(req,res); if (!auth) return;
      if (req.method !== 'GET' && String(req.body?.accountId || '') !== String(auth.user._id)) return res.status(403).json({error:'Esta ruta pertenece a otra cuenta. Entra con la cuenta que la guardó.'});
      await handler(req,res,auth.user);
    } catch(error) { res.status(error.status || 503).json({error:error.status ? error.message : 'No pude guardar o cargar tu ruta. Puedes reintentar.'}); }
  };
  app.get('/api/coach/property-route',authorized(async (_req,res,user)=>res.json({route:await service.get(user._id)})));
  app.post('/api/coach/property-route',authorized(async (req,res,user)=>res.json({route:await service.save(user._id,req.body?.route,req.headers['x-tec-request-id'])})));
}
