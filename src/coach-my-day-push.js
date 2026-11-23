// Avisos de «Mi día» en el teléfono del vendedor: 8 am, 1 pm y 9 pm en su zona horaria.
// Usa las mismas llaves y la misma suscripción del navegador que el chat del equipo (un navegador tiene una sola
// suscripción por aplicación). Al proveedor de avisos solo le llega una señal genérica; el texto lo pide el
// teléfono a TEC con la sesión del vendedor y se calcula en ese momento.
import webpush from 'web-push';
import { createHash, randomUUID } from 'node:crypto';
import { loadChatPushKeys, normalizeChatPushSubscription } from './coach-team-chat-push.js';

export const DAY_PUSH_SIGNAL = '{"type":"tec-day"}';
export const DAY_SLOTS = Object.freeze([8, 13, 21]);
// Un aviso que no salió a su hora (servidor reiniciado, teléfono sin conexión) se manda dentro de estas horas; después se omite.
const SLOT_WINDOW_HOURS = 3;
const hash = value => createHash('sha256').update(String(value)).digest('hex');
const api = '/api/coach/mi-dia/push/';

export function validZone(value) {
  if (typeof value !== 'string' || value.length > 64) return '';
  try { new Intl.DateTimeFormat('en-US', { timeZone: value }); return value; } catch { return ''; }
}

// Día AAAA-MM-DD y hora local de un instante en una zona.
export function localClock(at, zone) {
  const parts = new Intl.DateTimeFormat('en-CA', { timeZone: zone, year: 'numeric', month: '2-digit', day: '2-digit', hour: '2-digit', minute: '2-digit', hourCycle: 'h23' })
    .formatToParts(new Date(at));
  const part = type => parts.find(p => p.type === type).value;
  return { day: part('year') + '-' + part('month') + '-' + part('day'), hour: Number(part('hour')), minute: Number(part('minute')) };
}

// El aviso que toca ahora, o null. Entre dos avisos gana el más reciente.
export function dueSlot(at, zone) {
  const { day, hour } = localClock(at, zone);
  const slot = [...DAY_SLOTS].reverse().find(start => hour >= start && hour < start + SLOT_WINDOW_HOURS);
  return slot == null ? null : { day, slot, key: day + ':' + slot };
}

export function createDayPushModel(mongoose) {
  const device = new mongoose.Schema({
    endpointHash: { type: String, required: true, unique: true },
    accountId: { type: mongoose.Schema.Types.ObjectId, required: true },
    sessionId: { type: mongoose.Schema.Types.ObjectId, required: true },
    push: { type: mongoose.Schema.Types.Mixed, required: true, select: false },
    vapidKey: String, zone: String, enabled: Boolean,
    sent: [String], lastSlot: String, lastSentAt: Date, leaseUntil: Date, leaseToken: String,
    expiresAt: Date, updatedAt: Date
  });
  device.index({ enabled: 1, expiresAt: 1 });
  device.index({ expiresAt: 1 }, { expireAfterSeconds: 0 });
  return mongoose.models.CoachDayPushDevice || mongoose.model('CoachDayPushDevice', device);
}

// message({owner,zone,slot,now}) → {title,body}: el texto del aviso, calculado con los números del vendedor.
export function createCoachDayPush({ devices, configs, requireActive, message, allowed = () => true, now = Date.now, send = (...args) => webpush.sendNotification(...args), log = console.warn }) {
  let keys = null, running = false, stopped = false, timer;
  const unavailable = (res, error) => res.status([400, 403, 409].includes(error?.status) ? error.status : 503)
    .json({ error: [400, 403, 409].includes(error?.status) ? error.message : 'No pude actualizar los avisos de Mi día. Vuelve a intentar.' });

  async function context(req, res, write = false) {
    res.set('Cache-Control', 'private, no-store').vary('Cookie');
    const auth = await requireActive(req, res); if (!auth) return null;
    const accountId = String(auth.user._id);
    if ((req.get('X-TEC-Account-ID') && req.get('X-TEC-Account-ID') !== accountId) || (write && req.body?.accountId !== accountId)) {
      res.status(403).json({ error: 'La cuenta cambió. Vuelve a abrir TEC.' }); return null;
    }
    if (!allowed(auth)) { res.status(403).json({ error: 'Mi día todavía no está activo en tu cuenta.' }); return null; }
    return { auth, accountId };
  }

  function register(app) {
    app.get(api + 'config', async (req, res) => {
      try {
        const ctx = await context(req, res); if (!ctx) return;
        if (!keys) return unavailable(res);
        res.json({ ready: true, publicKey: keys.publicKey, accountId: ctx.accountId, slots: DAY_SLOTS });
      } catch (error) { unavailable(res, error); }
    });
    for (const action of ['status', 'subscribe', 'unsubscribe', 'inbox']) app.post(api + action, async (req, res) => {
      try {
        const ctx = await context(req, res, true); if (!ctx) return;
        const subscription = normalizeChatPushSubscription(req.body?.subscription), endpointHash = hash(subscription.endpoint);
        const current = await devices.findOne({ endpointHash }).select('+push').lean();
        const sameKeys = current && current.push?.keys?.auth === subscription.keys.auth && current.push?.keys?.p256dh === subscription.keys.p256dh;
        const owned = current && String(current.accountId) === ctx.accountId;
        const active = owned && sameKeys && current.enabled && String(current.sessionId) === String(ctx.auth.session?._id) && current.vapidKey === keys?.publicKey;
        if (action === 'status') return res.json({ enabled: Boolean(active) });
        if (action === 'inbox') {
          // Solo un aviso reciente de este mismo teléfono y sesión; el texto se calcula ahora con los números del vendedor.
          if (!active || !current.lastSlot || !current.lastSentAt || now() - new Date(current.lastSentAt).getTime() > 3600000) return res.json({ notify: false });
          const slot = Number(current.lastSlot.split(':')[1]);
          const text = await message({ owner: ctx.auth.user._id, user: ctx.auth.user, zone: current.zone, slot, now: now() });
          return res.json({ notify: true, title: text.title, body: text.body, url: '/coach/app/?miDia=1' });
        }
        if (action === 'unsubscribe') {
          if (owned && sameKeys) await devices.updateOne({ _id: current._id, accountId: ctx.accountId }, { $set: { enabled: false, updatedAt: new Date(now()) } });
          return res.json({ enabled: false });
        }
        if (!keys || !ctx.auth.session?._id) return unavailable(res);
        if (current && !sameKeys) return unavailable(res, Object.assign(new Error('Renueva los avisos de este teléfono y vuelve a intentar.'), { status: 409 }));
        const zone = validZone(req.body?.zone) || 'America/Chicago';
        // Un aviso que ya pasó hoy no se repite al activar: se marca como enviado.
        const due = dueSlot(now(), zone);
        await devices.updateOne({ endpointHash }, {
          $set: { accountId: ctx.accountId, sessionId: ctx.auth.session._id, push: subscription, vapidKey: keys.publicKey, zone, enabled: true,
            updatedAt: new Date(now()), expiresAt: ctx.auth.session.expiresAt },
          ...(due && !(owned && current.sent?.includes(due.key)) && { $push: { sent: { $each: [due.key], $slice: -12 } } })
        }, { upsert: true });
        res.json({ enabled: true });
      } catch (error) { unavailable(res, error); }
    });
  }

  // Un teléfono por vuelta: se aparta con un permiso temporal para que dos servidores no manden el mismo aviso.
  async function deliverOne() {
    const at = new Date(now());
    const candidates = await devices.find({ enabled: true, expiresAt: { $gt: at }, $or: [{ leaseUntil: null }, { leaseUntil: { $lt: at } }] })
      .sort({ updatedAt: 1 }).limit(200).lean();
    for (const device of candidates) {
      const due = dueSlot(at.getTime(), validZone(device.zone) || 'America/Chicago');
      if (!due || device.sent?.includes(due.key)) continue;
      const token = randomUUID();
      const claimed = await devices.findOneAndUpdate({ _id: device._id, sent: { $ne: due.key }, $or: [{ leaseUntil: null }, { leaseUntil: { $lt: at } }] },
        { $set: { leaseUntil: new Date(at.getTime() + 45000), leaseToken: token } }, { new: true }).select('+push').lean();
      if (!claimed) continue;
      const release = extra => devices.updateOne({ _id: device._id, leaseToken: token }, { ...extra, $unset: { leaseUntil: 1, leaseToken: 1 } });
      try {
        if (claimed.vapidKey !== keys.publicKey) { await release({ $set: { enabled: false } }); return true; }
        await send(normalizeChatPushSubscription(claimed.push), DAY_PUSH_SIGNAL, { vapidDetails: keys, TTL: 3600, urgency: 'normal', topic: 'tec-day', timeout: 8000 });
        await release({ $set: { lastSlot: due.key, lastSentAt: at }, $push: { sent: { $each: [due.key], $slice: -12 } } });
      } catch (error) {
        if ([404, 410].includes(error.statusCode)) await release({ $set: { enabled: false } });
        // Un fallo pasajero se reintenta en la siguiente vuelta, dentro de la ventana del aviso.
        else await release({});
      }
      return true;
    }
    return false;
  }

  async function run() {
    if (running || stopped || !keys) return;
    running = true;
    try { for (let n = 0; n < 50 && !stopped; n++) if (!(await deliverOne())) break; }
    catch { log('TEC Mi día: avisos pendientes'); }
    finally { running = false; }
  }

  return { register, run, deliverOne, async initialize() { keys = await loadChatPushKeys(configs); },
    start() { stopped = false; void run(); timer = setInterval(() => void run(), 60000); timer.unref?.(); },
    stop() { stopped = true; clearInterval(timer); } };
}
