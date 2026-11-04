// Aggregated operational counters, not a proxy for Google reports or billing.
import { EVENTS, AREAS, safeEvent, ANALYTICS_PROPERTY_ID } from '../public/coach/analytics-events.js';
export function createUsageModel(mongoose) {
  const schema = new mongoose.Schema({
    _id: String, day: String, name: { type: String, enum: EVENTS }, area: { type: String, enum: AREAS },
    surface: { type: String, enum: ['web', 'pwa'] }, count: Number, expiresAt: Date
  }, { versionKey: false, strict: 'throw' });
  schema.index({ expiresAt: 1 }, { expireAfterSeconds: 0 });
  return mongoose.models.TecUsageDaily || mongoose.model('TecUsageDaily', schema);
}
export function sanitizeUsageBatch(body) {
  if (!body || Object.keys(body).some(key => key !== 'events') || !Array.isArray(body.events) || body.events.length < 1 || body.events.length > 8) return null;
  const events = [];
  for (const event of body.events) {
    if (!event || typeof event !== 'object' || Object.keys(event).some(key => !['name', 'area', 'surface'].includes(key))) return null;
    const safe = safeEvent(event.name, event.area, event.surface);
    if (!safe) return null;
    events.push(safe);
  }
  return events;
}
export function usageOperations(events, now = new Date()) {
  const day = now.toISOString().slice(0, 10), counts = new Map();
  for (const event of events) {
    const id = [day, event.name, event.area, event.surface].join(':');
    const row = counts.get(id) || { ...event, count: 0 };
    row.count++; counts.set(id, row);
  }
  return [...counts].map(([_id, row]) => ({ updateOne: { filter: { _id }, update: {
    $inc: { count: row.count }, $setOnInsert: { day, name: row.name, area: row.area, surface: row.surface,
      expiresAt: new Date(new Date(day + 'T00:00:00Z').getTime() + 90 * 86400000) }
  }, upsert: true } }));
}
export function registerTecUsage(app, { model, requireAdmin, now = () => Date.now() }) {
  const rate = new Map();
  app.post('/api/coach/usage-events', async (req, res) => {
    res.set('Cache-Control', 'no-store');
    // Browser-only same-origin JSON with a non-simple header. Never log request bodies or IPs.
    let origin;
    try { origin = new URL(req.get('Origin')); } catch { return res.sendStatus(403); }
    if (!['https://totalelitecoaching.com', 'https://www.totalelitecoaching.com'].includes(origin.origin) ||
      req.get('X-TEC-Usage') !== 'v1' || req.get('Sec-Fetch-Site') !== 'same-origin' || !req.is('application/json')) return res.sendStatus(403);
    const events = sanitizeUsageBatch(req.body);
    if (!events) return res.sendStatus(400);
    const current = now(), key = req.ip; // In-memory abuse limit only; expires within one minute.
    if (rate.size >= 2000) for (const [ip, item] of rate) if (item.until <= current) rate.delete(ip);
    if (!rate.has(key) && rate.size >= 2000) return res.sendStatus(429);
    const slot = rate.get(key);
    const value = slot && slot.until > current ? slot : { count: 0, until: current + 60000 };
    value.count += events.length; rate.set(key, value);
    if (value.count > 120) return res.sendStatus(429);
    try { await model.bulkWrite(usageOperations(events, new Date(current)), { ordered: false }); res.sendStatus(204); }
    catch { res.sendStatus(503); } // Analytics failure must never alter a lead, recording or payment.
  });
  app.get('/api/control/tec/usage', async (req, res) => {
    res.set('Cache-Control', 'private, no-store');
    if (!await requireAdmin(req, res)) return;
    const days = [7, 30, 90].includes(Number(req.query.days)) ? Number(req.query.days) : 30;
    const from = new Date(now() - (days - 1) * 86400000).toISOString().slice(0, 10);
    try {
      const rows = await model.aggregate([{ $match: { day: { $gte: from } } },
        { $group: { _id: '$name', count: { $sum: '$count' } } }, { $sort: { count: -1 } }]);
      res.json({ source: 'tec_aggregate_counters', days, dayBasis: 'UTC', updatedAt: new Date(now()),
        googleUrl: `https://analytics.google.com/analytics/web/#/p${ANALYTICS_PROPERTY_ID}/reports/intelligenthome`,
        events: rows.filter(row => EVENTS.includes(row._id)).map(row => ({ name: row._id, count: row.count })) });
    } catch { res.status(503).json({ error: 'No se pudieron consultar las estadísticas. Intenta después.' }); }
  });
}
