// Official Meta webhook for WhatsApp coexistence. Only the last genuine inbound
// time per (line, customer) pair is kept, under an HMAC id, for 24 hours.
// Never persists or logs phone numbers, message content, bodies or credentials.
import crypto from 'crypto';
import express from 'express';

export const COACH_META_WEBHOOK_PATH = '/webhooks/coach/meta-whatsapp';
export const COACH_META_WINDOW_MS = 24 * 60 * 60 * 1000;
const FUTURE_SKEW_MS = 5 * 60 * 1000;
const MAX_BODY = '512kb';
const MAX_KEYS_PER_EVENT = 500;
// User-initiated message types that count as a real inbound message.
// Excluded on purpose: reaction, system, unsupported, request_welcome, errors, etc.
const INBOUND_TYPES = new Set([
  'text', 'image', 'audio', 'video', 'document', 'sticker',
  'location', 'contacts', 'interactive', 'button', 'order'
]);

export function createCoachMetaInboundModel(mongoose) {
  const schema = new mongoose.Schema({
    _id: String, lastInboundAt: Date, expiresAt: Date
  }, { versionKey: false, strict: true });
  schema.index({ expiresAt: 1 }, { expireAfterSeconds: 0 });
  return mongoose.models.CoachMetaWaInbound || mongoose.model('CoachMetaWaInbound', schema);
}

const str = value => (typeof value === 'string' ? value.trim() : '');
const digits = value => String(value ?? '').replace(/\D/g, '');
const sha256 = value => crypto.createHash('sha256').update(String(value), 'utf8').digest();

function safeEqualText(a, b) {
  // Hashing first gives equal-length buffers, so length is not leaked either.
  return crypto.timingSafeEqual(sha256(a), sha256(b));
}

function config(env) {
  return {
    verifyToken: str(env.COACH_WA_WEBHOOK_VERIFY_TOKEN),
    appSecret: str(env.COACH_WA_META_APP_SECRET),
    // Same single line id as the Cloud API sender (coach-wa-cloud.js).
    phoneId: digits(env.COACH_WA_CLOUD_PHONE_NUMBER_ID)
  };
}

// HMAC id for the line/customer pair. Uses a key derived from the app secret
// with domain separation; rotating the secret just expires old windows.
export function coachMetaContactKey({ appSecret, phoneId, waId }) {
  const line = digits(phoneId); const customer = digits(waId);
  if (!appSecret || !line || customer.length < 6 || customer.length > 20) return '';
  const key = crypto.createHmac('sha256', appSecret).update('tec-coach-wa-contact:v1').digest();
  return crypto.createHmac('sha256', key).update(`${line}:${customer}`).digest('hex');
}

export function verifyMetaSignature(raw, header, appSecret) {
  if (!Buffer.isBuffer(raw) || !appSecret) return false;
  const match = /^sha256=([a-f0-9]{64})$/i.exec(str(header));
  if (!match) return false;
  const expected = crypto.createHmac('sha256', appSecret).update(raw).digest();
  return crypto.timingSafeEqual(expected, Buffer.from(match[1], 'hex'));
}

// Returns Map<key, Date> of genuine inbound messages for the configured line.
export function extractCoachMetaInbound(payload, { appSecret, phoneId, nowMs }) {
  const found = new Map();
  if (!payload || typeof payload !== 'object' || payload.object !== 'whatsapp_business_account') return found;
  for (const entry of Array.isArray(payload.entry) ? payload.entry : []) {
    for (const change of Array.isArray(entry?.changes) ? entry.changes : []) {
      // Coexistence also sends smb_message_echoes, history, smb_app_state_sync, etc.
      if (change?.field !== 'messages') continue;
      const value = change.value;
      if (!value || typeof value !== 'object' || value.messaging_product !== 'whatsapp') continue;
      if (digits(value.metadata?.phone_number_id) !== phoneId) continue;
      const ownNumber = digits(value.metadata?.display_phone_number);
      for (const message of Array.isArray(value.messages) ? value.messages : []) {
        if (!message || typeof message !== 'object') continue;
        if (!INBOUND_TYPES.has(message.type)) continue;
        if (message.group_id || message.context?.group_id) continue;
        if (typeof message.from !== 'string' || !/^\d{6,20}$/.test(message.from)) continue;
        if (ownNumber && message.from === ownNumber) continue;
        if (typeof message.timestamp !== 'string' && typeof message.timestamp !== 'number') continue;
        const seconds = Number(message.timestamp);
        if (!Number.isInteger(seconds) || seconds <= 0) continue;
        const atMs = seconds * 1000;
        if (atMs > nowMs + FUTURE_SKEW_MS || atMs <= nowMs - COACH_META_WINDOW_MS) continue;
        // Never let a skewed clock extend the window beyond now.
        const at = Math.min(atMs, nowMs);
        const key = coachMetaContactKey({ appSecret, phoneId, waId: message.from });
        if (!key) continue;
        if (!found.has(key) && found.size >= MAX_KEYS_PER_EVENT) continue;
        if (!found.has(key) || found.get(key) < at) found.set(key, at);
      }
    }
  }
  return new Map([...found].map(([key, at]) => [key, new Date(at)]));
}

export function createCoachMetaWebhook({ store, env = process.env, now = Date.now, logger = console } = {}) {
  const rawParser = express.raw({ type: () => true, limit: MAX_BODY });

  function rawBody(req, res, next) {
    rawParser(req, res, error => {
      if (!error) return next();
      res.set('Cache-Control', 'no-store');
      return res.status(error.status === 413 ? 413 : 400).type('text/plain').send('Invalid request');
    });
  }

  function verify(req, res) {
    res.set('Cache-Control', 'no-store');
    const { verifyToken } = config(env);
    if (!verifyToken) return res.status(503).type('text/plain').send('Not configured');
    const mode = req.query?.['hub.mode'];
    const token = req.query?.['hub.verify_token'];
    const challenge = req.query?.['hub.challenge'];
    if (mode !== 'subscribe' || typeof token !== 'string' || !safeEqualText(token, verifyToken)) {
      return res.status(403).type('text/plain').send('Forbidden');
    }
    if (typeof challenge !== 'string' || !/^[A-Za-z0-9_-]{1,256}$/.test(challenge)) {
      return res.status(400).type('text/plain').send('Invalid challenge');
    }
    return res.status(200).type('text/plain').send(challenge);
  }

  async function receive(req, res) {
    res.set('Cache-Control', 'no-store');
    const { appSecret, phoneId } = config(env);
    if (!appSecret || !phoneId || !store) return res.status(503).type('text/plain').send('Not configured');
    if (!verifyMetaSignature(req.body, req.get('x-hub-signature-256'), appSecret)) {
      return res.status(401).type('text/plain').send('Invalid signature');
    }
    let payload;
    try { payload = JSON.parse(req.body.toString('utf8')); } catch {
      return res.status(400).type('text/plain').send('Invalid payload');
    }
    const nowMs = now();
    const inbound = extractCoachMetaInbound(payload, { appSecret, phoneId, nowMs });
    // $max keeps replays and out-of-order deliveries idempotent.
    const save = ([key, at]) => store.updateOne(
      { _id: key },
      { $max: { lastInboundAt: at, expiresAt: new Date(at.getTime() + COACH_META_WINDOW_MS) } },
      { upsert: true }
    );
    try {
      try { await Promise.all([...inbound].map(save)); } catch (error) {
        // Concurrent upsert race on the same id: retrying once is safe with $max.
        if (error?.code !== 11000) throw error;
        await Promise.all([...inbound].map(save));
      }
    } catch (error) {
      logger.error?.('Coach Meta webhook store failed', { name: error?.name || 'Error', code: error?.code || '' });
      return res.status(500).type('text/plain').send('Retry later');
    }
    return res.status(200).type('text/plain').send('EVENT_RECEIVED');
  }

  // Injectable lookup for other Coach modules: last real inbound within 24h, or null.
  async function lastInboundAt(waId) {
    const { appSecret, phoneId } = config(env);
    const key = coachMetaContactKey({ appSecret, phoneId, waId });
    if (!key || !store) return null;
    const nowMs = now();
    const query = store.findOne({ _id: key, expiresAt: { $gt: new Date(nowMs) } }, { lastInboundAt: 1 });
    const doc = typeof query?.lean === 'function' ? await query.lean() : await query;
    const at = doc?.lastInboundAt instanceof Date ? doc.lastInboundAt : null;
    if (!at || at.getTime() > nowMs || at.getTime() <= nowMs - COACH_META_WINDOW_MS) return null;
    return at;
  }

  async function isWindowOpen(waId) { return Boolean(await lastInboundAt(waId)); }

  return { rawBody, verify, receive, lastInboundAt, isWindowOpen };
}
