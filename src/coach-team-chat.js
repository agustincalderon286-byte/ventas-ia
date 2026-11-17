import { createHash } from 'node:crypto';
import { decodeChatMedia, prepareChatMedia, chatMediaRange } from './coach-team-chat-media.js';
export const teamChatRoomId = key => `team-chat:${createHash('sha256').update(key).digest('hex')}`;

const objectId = /^[a-f0-9]{24}$/i;
const uuid = /^[a-f0-9]{8}-[a-f0-9]{4}-[1-8][a-f0-9]{3}-[89ab][a-f0-9]{3}-[a-f0-9]{12}$/i;
const pageSize = 50;

export function createCoachTeamChatModels(mongoose) {
  const messageSchema = new mongoose.Schema({
    roomId: { type: String, required: true },
    senderId: { type: mongoose.Schema.Types.ObjectId, ref: 'CoachUser', required: true },
    senderName: { type: String, required: true, maxlength: 160 },
    clientMessageId: { type: String, required: true },
    body: { type: String, default: '', maxlength: 2000 },
    kind: { type: String, enum: ['text', 'media', 'sale'], default: 'text' },
    attachment: { kind: String, mime: String, bytes: Number, duration: Number, width: Number, height: Number, hash: String },
    mediaData: { type: Buffer, select: false },
    transcript: { type: String, maxlength: 8000 },
    transcriptionState: { type: String, enum: ['pending', 'working', 'ready', 'unavailable'] },
    transcriptionAttempts: { type: Number, default: 0 },
    transcriptionLeaseUntil: Date,
    sale: { outcomeId: mongoose.Schema.Types.ObjectId, amount: Number, products: [String], reportedAt: Date },
    pushState: { type: String, enum: ['pending','working','done'] },
    pushLeaseUntil: Date, pushLeaseToken: String, pushFanoutAfter: mongoose.Schema.Types.ObjectId,
    createdAt: { type: Date, default: Date.now, required: true }
  });
  messageSchema.index({ roomId: 1, _id: -1 });
  messageSchema.index({ pushState: 1, createdAt: 1, pushLeaseUntil: 1 });
  messageSchema.index({ roomId: 1, senderId: 1, clientMessageId: 1 }, { unique: true });
  messageSchema.index({ body: 'text', transcript: 'text', senderName: 'text' }, { default_language: 'spanish' });
  messageSchema.index({ roomId: 1, kind: 1, createdAt: -1 });
  messageSchema.index({ transcriptionState: 1, transcriptionLeaseUntil: 1 });
  const readerSchema = new mongoose.Schema({
    roomId: { type: String, required: true },
    accountId: { type: mongoose.Schema.Types.ObjectId, ref: 'CoachUser', required: true },
    lastReadMessageId: { type: mongoose.Schema.Types.ObjectId },
    createdAt: { type: Date, default: Date.now },
    updatedAt: { type: Date, default: Date.now }
  });
  readerSchema.index({ roomId: 1, accountId: 1 }, { unique: true });
  return {
    messages: mongoose.models.CoachTeamChatMessage || mongoose.model('CoachTeamChatMessage', messageSchema),
    readers: mongoose.models.CoachTeamChatReader || mongoose.model('CoachTeamChatReader', readerSchema)
  };
}

// The caller installs the shared Coach request guard before registering these routes.
// A room identifier is only an account-switch check, never an authorization credential.
export function registerCoachTeamChat(app, { requireActive, getTeamKey, messages, readers, getRooms, onAudioSaved, onMessageSaved, now = Date.now }) {
  const rates = new Map(), mediaActive = new Set();
  function consume(accountId, action, max, res) {
    const at = now();
    if (rates.size >= 2000) for (const [key, value] of rates) if (value.until <= at) rates.delete(key);
    const key = `${accountId}:${action}`;
    if (!rates.has(key) && rates.size >= 2000) {
      res.set('Retry-After', '60').status(429).json({ error: 'Espera un minuto y vuelve a intentar.' });
      return false;
    }
    const previous = rates.get(key);
    const value = previous?.until > at ? previous : { count: 0, until: at + 60000 };
    value.count++;
    rates.set(key, value);
    if (value.count > max) {
      res.set('Retry-After', String(Math.max(1, Math.ceil((value.until - at) / 1000)))).status(429).json({ error: 'Espera un momento antes de continuar en el chat.' });
      return false;
    }
    return true;
  }
  async function context(req, res, write = false) {
    res.set('Cache-Control', 'private, no-store').set('X-TEC-Team-Chat-Version', '20261117-groups2').vary('Cookie');
    const auth = await requireActive(req, res);
    if (!auth) return null;
    const accountId = String(auth.user._id);
    const requestedHeader = req.get('X-TEC-Account-ID');
    if ((requestedHeader && requestedHeader !== accountId) || (write && req.body?.accountId !== accountId)) {
      res.status(403).json({ error: 'La cuenta cambió. Vuelve a abrir el chat del equipo.' });
      return null;
    }
    const selectedOwner = write ? req.body?.teamOwnerId : req.query.teamOwnerId;
    let teamKey;
    try { teamKey = await getTeamKey(auth.user, selectedOwner); }
    catch (error) { if (error.status === 403) { res.status(403).json({ error: error.message }); return null; } throw error; }
    if (typeof teamKey !== 'string' || !teamKey.trim() || teamKey.length > 512) {
      res.status(403).json({ error: 'Esta cuenta no tiene un equipo disponible.' });
      return null;
    }
    const roomId = teamChatRoomId(teamKey);
    if ((write && req.body?.roomId !== roomId) || (!write && req.query.roomId !== undefined && req.query.roomId !== roomId)) {
      res.status(403).json({ error: 'El equipo cambió. Vuelve a abrir el chat del equipo.' });
      return null;
    }
    return { accountId, roomId, ownerId: teamKey, user: auth.user };
  }
  function serialize(message, ownerId) {
    return { id: String(message._id), senderId: String(message.senderId), senderName: message.senderName, body: message.body, createdAt: new Date(message.createdAt).toISOString(),
      ...(message.attachment?.kind ? { attachment: { kind: message.attachment.kind, mime: message.attachment.mime, bytes: message.attachment.bytes, duration: message.attachment.duration, width: message.attachment.width, height: message.attachment.height, url: `/api/coach/team-chat/media/${message._id}?roomId=${encodeURIComponent(message.roomId)}&teamOwnerId=${encodeURIComponent(ownerId)}`, transcript: message.transcript || '', transcriptionState: message.transcriptionState || '' } } : {}),
      ...(message.kind === 'sale' ? { kind: 'sale', sale: message.sale } : {}) };
  }
  async function unreadCount({ roomId, accountId }) {
    const receipt = await readers.findOne({ roomId, accountId }).lean();
    const filter = { roomId, senderId: { $ne: accountId } };
    if (receipt?.lastReadMessageId) filter._id = { $gt: receipt.lastReadMessageId };
    return messages.countDocuments(filter);
  }
  function unavailable(res) {
    return res.status(503).json({ error: 'No pude actualizar el chat. Conserva tu mensaje y vuelve a intentar.' });
  }

  app.get('/api/coach/team-chat/rooms', async (req,res) => {
    try { const ctx=await context(req,res); if(!ctx)return; const rooms=getRooms ? await getRooms(ctx.user) : []; res.json({rooms,preferredOwnerId:rooms.find(room=>room.preferred)?.ownerId || ''}); } catch { return unavailable(res); }
  });

  app.get('/api/coach/team-chat', async (req, res) => {
    try {
      const ctx = await context(req, res);
      if (!ctx || !consume(ctx.accountId, 'load', 120, res)) return;
      const before = req.query.before;
      const filter = { roomId: ctx.roomId };
      if (before !== undefined) {
        if (typeof before !== 'string' || !objectId.test(before)) return res.status(400).json({ error: 'La página de mensajes no es válida.' });
        const cursor = await messages.findOne({ roomId: ctx.roomId, _id: before }).lean();
        if (!cursor) return res.status(404).json({ error: 'Ese mensaje no está disponible en este equipo.' });
        filter._id = { $lt: cursor._id };
      }
      const [rows, unread] = await Promise.all([
        messages.find(filter).sort({ _id: -1 }).limit(pageSize + 1).lean(), unreadCount(ctx)
      ]);
      return res.json({ roomId: ctx.roomId, messages: rows.slice(0, pageSize).reverse().map(message => serialize(message, ctx.ownerId)), hasMore: rows.length > pageSize, unreadCount: unread });
    } catch { return unavailable(res); }
  });

  app.post('/api/coach/team-chat/messages', async (req, res) => {
    try {
      const ctx = await context(req, res, true);
      if (!ctx) return;
      const clientMessageId = req.body?.clientMessageId;
      const body = typeof req.body?.body === 'string' ? req.body.body.trim() : '';
      if (typeof clientMessageId !== 'string' || !uuid.test(clientMessageId) || (!body && !req.body?.media) || body.length > 2000) {
        return res.status(400).json({ error: 'Escribe un mensaje de hasta 2000 caracteres y vuelve a intentar.' });
      }
      if (!consume(ctx.accountId, 'send-attempt', 120, res)) return;
      const rawMedia = req.body?.media ? decodeChatMedia(req.body.media) : null;
      const filter = { roomId: ctx.roomId, senderId: ctx.accountId, clientMessageId: clientMessageId.toLowerCase() };
      function respond(message, status = 200) {
        if (message.body !== body || (message.attachment?.hash || '') !== (rawMedia?.hash || '')) return res.status(409).json({ error: 'Ese envío ya tiene otro texto. Vuelve a abrir el chat para revisarlo.' });
        return res.status(status).json({ roomId: ctx.roomId, message: serialize(message, ctx.ownerId) });
      }
      const existing = await messages.findOne(filter).lean();
      if (existing) return respond(existing);
      if (!consume(ctx.accountId, 'send-new', 30, res)) return;
      if (rawMedia && (!consume(ctx.accountId, 'media-new', 8, res) || mediaActive.has(ctx.accountId) || mediaActive.size >= 2)) {
        if (!res.headersSent) res.status(429).json({ error: 'Espera a que termine el archivo anterior y vuelve a intentar.' });
        return;
      }
      if (rawMedia) mediaActive.add(ctx.accountId);
      try {
        const prepared = rawMedia ? await prepareChatMedia(rawMedia) : null;
        const message = await messages.create({ ...filter, ...(prepared ? { kind: 'media', attachment: prepared.meta, mediaData: prepared.data, ...(prepared.meta.kind === 'audio' ? { transcriptionState: 'pending' } : {}) } : {}), senderName: typeof ctx.user.name === 'string' && ctx.user.name.trim() ? ctx.user.name.trim().slice(0, 160) : 'Miembro del equipo', body, pushState: 'pending', createdAt: new Date(now()) });
        if (prepared?.meta.kind === 'audio') void Promise.resolve().then(() => onAudioSaved?.()).catch(() => {});
        void Promise.resolve().then(() => onMessageSaved?.()).catch(() => {});
        return respond(message, 201);
      } catch (error) {
        // Concurrent retries are resolved by the unique index, including across servers.
        if (error?.code !== 11000) throw error;
        const duplicate = await messages.findOne(filter).lean();
        if (!duplicate) throw error;
        return respond(duplicate);
      } finally { if (rawMedia) mediaActive.delete(ctx.accountId); }
    } catch (error) { if ([400, 503].includes(error.status)) return res.status(error.status).json({ error: error.message }); return unavailable(res); }
  });

  app.get('/api/coach/team-chat/media/:id', async (req, res) => {
    try {
      const ctx = await context(req, res);
      if (!ctx || !consume(ctx.accountId, 'media-get', 240, res)) return;
      if (!objectId.test(req.params.id)) return res.status(404).end();
      const message = await messages.findOne({ _id: req.params.id, roomId: ctx.roomId }).select('+mediaData').lean();
      if (!message?.attachment?.kind || !message.mediaData) return res.status(404).end();
      const data = Buffer.isBuffer(message.mediaData) ? message.mediaData : Buffer.from(message.mediaData.value ? message.mediaData.value(true) : message.mediaData.buffer);
      res.set({ 'Content-Type': message.attachment.mime, 'X-Content-Type-Options': 'nosniff', 'Content-Security-Policy': "default-src 'none'; sandbox", 'Accept-Ranges': 'bytes', 'Content-Disposition': 'inline; filename="chat.' + (message.attachment.kind === 'audio' ? 'mp3' : message.attachment.kind === 'gif' ? 'gif' : 'webp') + '"' });
      const range = chatMediaRange(req.get('Range'), data.length);
      if (range === false) return res.status(416).set('Content-Range', `bytes */${data.length}`).end();
      if (range) return res.status(206).set('Content-Range', `bytes ${range.start}-${range.end}/${data.length}`).send(data.subarray(range.start, range.end + 1));
      return res.send(data);
    } catch { return unavailable(res); }
  });

  app.post('/api/coach/team-chat/read', async (req, res) => {
    try {
      const ctx = await context(req, res, true);
      if (!ctx || !consume(ctx.accountId, 'read', 120, res)) return;
      const lastMessageId = req.body?.lastMessageId;
      if (typeof lastMessageId !== 'string' || !objectId.test(lastMessageId)) return res.status(400).json({ error: 'El mensaje leído no es válido.' });
      const cursor = await messages.findOne({ roomId: ctx.roomId, _id: lastMessageId }).lean();
      if (!cursor) return res.status(404).json({ error: 'Ese mensaje no está disponible en este equipo.' });
      const filter = { roomId: ctx.roomId, accountId: ctx.accountId };
      const update = { $max: { lastReadMessageId: cursor._id }, $set: { updatedAt: new Date(now()) }, $setOnInsert: { createdAt: new Date(now()) } };
      try { await readers.updateOne(filter, update, { upsert: true }); }
      catch (error) {
        if (error?.code !== 11000) throw error;
        await readers.updateOne(filter, update, { upsert: false });
      }
      const [receipt, unread] = await Promise.all([readers.findOne(filter).lean(), unreadCount(ctx)]);
      return res.json({ roomId: ctx.roomId, lastMessageId: String(receipt.lastReadMessageId), unreadCount: unread });
    } catch { return unavailable(res); }
  });
}
