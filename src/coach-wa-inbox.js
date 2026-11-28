import { createHash, createHmac, randomBytes } from 'node:crypto';
import express from 'express';
import { openToken, sealToken } from './coach-hycite-gmail.js';
import { validZone } from './coach-my-day-push.js';
import { callReply, inboxReply } from './coach-wa-inbox-actions.js';

export const COACH_WA_INBOX_WEBHOOK = '/webhooks/twilio/whatsapp';
const mediaPath = '/webhooks/coach/whatsapp-audio/';
const MINUTE = 60_000;
const DAY = 24 * 60 * MINUTE;
const LINE_REFRESH = 2 * MINUTE;
const LINE_GRACE = 10 * MINUTE;
export const COACH_WA_INBOX_PATH = '/api/coach/conexiones/mi-coach-whatsapp/';
const connectPage = 'https://totalelitecoaching.com/mis-conexiones/';
const phonePattern = /^[1-9]\d{6,14}$/;
const hash = value => createHash('sha256').update(value).digest('hex');

export function createWaInboxModels(mongoose) {
  const { Schema } = mongoose;
  const binding = new Schema({
    _id: Schema.Types.ObjectId, contact: String, challenge: String, until: Date,
    expectedContact: String, nextCall: String, callDay: String, zone: String
  });
  binding.index({ contact: 1 }, { unique: true, sparse: true });
  binding.index({ challenge: 1 }, { unique: true, sparse: true });
  const job = new Schema({
    _id: String, encrypted: String, at: Date, expiresAt: Date, lease: Date, phase: String
  });
  job.index({ expiresAt: 1 }, { expireAfterSeconds: 0 });
  job.index({ phase: 1, at: 1 });
  const audio = new Schema({ _id: String, encrypted: String, expiresAt: Date });
  audio.index({ expiresAt: 1 }, { expireAfterSeconds: 0 });
  return {
    audio: mongoose.models.CoachWaInboxAudio || mongoose.model('CoachWaInboxAudio', audio),
    bindings: mongoose.models.CoachWaInboxBinding || mongoose.model('CoachWaInboxBinding', binding),
    jobs: mongoose.models.CoachWaInboxJob || mongoose.model('CoachWaInboxJob', job)
  };
}

export function waInboxConfig(env, key) {
  const read = name => String(env['COACH_WA_INBOX_' + name] || '').trim();
  const phone = read('PHONE').replace(/^\+/, '');
  const separate = read('TWILIO_ACCOUNT_SID') || read('TWILIO_AUTH_TOKEN');
  const accountSid = separate ? read('TWILIO_ACCOUNT_SID') : String(env.TWILIO_ACCOUNT_SID || '').trim();
  const authToken = separate ? read('TWILIO_AUTH_TOKEN') : String(env.TWILIO_AUTH_TOKEN || '').trim();
  const sellers = new Set(read('SELLERS').toLowerCase().split(',').map(email => email.trim())
    .filter(email => /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email)));
  const ready = read('ENABLED') === '1' && phonePattern.test(phone)
    && /^AC[a-f0-9]{32}$/i.test(accountSid) && authToken.length >= 16 && key?.length === 32 && sellers.size > 0;
  // server.js also requires ?token= on this webhook when this variable is set; the sender check verifies it.
  const webhookToken = String(env.TWILIO_WHATSAPP_WEBHOOK_TOKEN || '').trim();
  return { ready, phone, accountSid, authToken, sellers, key, webhookToken };
}

export function waInboxMessage(body, config, at) {
  if (body?.To !== 'whatsapp:+' + config.phone || body.AccountSid !== config.accountSid) return null;
  const from = /^whatsapp:\+([1-9]\d{6,14})$/.exec(body.From || '')?.[1];
  const id = body.MessageSid;
  if (!from || from === config.phone || typeof id !== 'string' || !/^(?:SM|MM)[a-f0-9]{32}$/i.test(id)) return null;
  const text = typeof body.Body === 'string' ? body.Body.trim().slice(0, 2000) : '';
  const path = '/2010-04-01/Accounts/' + config.accountSid + '/Messages/' + id + '/Media/';
  const media = typeof body.MediaUrl0 === 'string' && body.MediaUrl0.startsWith('https://api.twilio.com' + path)
    ? body.MediaUrl0.slice(('https://api.twilio.com' + path).length) : '';
  const audio = body.NumMedia === '1' && /^audio\//i.test(body.MediaContentType0 || '') && /^ME[a-f0-9]{32}$/i.test(media)
    ? { message: id, id: media } : null;
  return text || audio ? { id, from, at, text, audio } : null;
}

export function createWaInboxAudio({ model, key, now = Date.now }) {
  return {
    async store(buffer) {
      const id = randomBytes(32).toString('hex');
      await model.create({ _id: id, encrypted: sealToken(buffer.toString('base64'), key, id), expiresAt: new Date(now() + 15 * MINUTE) });
      return 'https://totalelitecoaching.com' + mediaPath + id + '.ogg';
    },
    register(app) {
      app.get(mediaPath + ':id', async (req, res) => {
        res.set('Cache-Control', 'private, no-store');
        const match = /^([a-f0-9]{64})\.ogg$/.exec(req.params.id);
        if (!match) return res.sendStatus(404);
        const id = match[1];
        try {
          const entry = await model.findOne({ _id: id, expiresAt: { $gt: new Date(now()) } }).lean();
          if (!entry) return res.sendStatus(404);
          return res.type('audio/ogg').send(Buffer.from(openToken(entry.encrypted, key, id), 'base64'));
        } catch { return res.sendStatus(503); }
      });
    }
  };
}

export function createCoachWaInbox({ config, bindings, jobs, transport, voice, validate, getUser, eligible, myDay, latestDemo, now = Date.now }) {
  const available = config.ready && Boolean(bindings && jobs && transport);
  const contactOf = phone => createHmac('sha256', config.key).update('coach-wa-inbox:' + config.phone + ':' + phone).digest('hex');
  const waLink = 'https://wa.me/' + config.phone;
  const seller = email => available && config.sellers.has(String(email || '').toLowerCase());
  const parser = express.urlencoded({ extended: false, limit: '128kb' });
  let worker, monitor, checking, lastOnline = null, status = 'checking', running = false;
  const recent = duration => lastOnline !== null && now() - lastOnline < duration;
  const usable = new Set(['online', 'signed-webhook-required']);
  const lineOnline = () => usable.has(status) || (status === 'unavailable' && recent(LINE_GRACE));

  async function checkLine({ cached = false } = {}) {
    if (cached && recent(LINE_REFRESH)) return true;
    if (checking) return checking;
    checking = (async () => {
      const checked = await transport.lineStatus().catch(error => {
        const reason = typeof error?.status === 'number' ? String(error.status)
          : error?.name === 'TimeoutError' ? 'timeout' : 'connection';
        if (status !== 'unavailable') console.warn('[mi-coach-whatsapp] Twilio lookup failed:', reason);
        return 'unavailable';
      });
      if (usable.has(checked)) lastOnline = now();
      else if (checked !== 'unavailable') lastOnline = null;
      if (checked !== status) console.info('[mi-coach-whatsapp] Twilio sender status:', checked);
      status = checked;
      return lineOnline();
    })();
    try { return await checking; }
    finally { checking = undefined; }
  }

  function registerWebhook(app) {
    if (!available) return;
    app.post(COACH_WA_INBOX_WEBHOOK, parser, async (req, res, next) => {
      if (req.body?.To !== 'whatsapp:+' + config.phone) return next();
      if (!validate(req) || req.body.AccountSid !== config.accountSid) return res.sendStatus(403);
      // A signed message for this line is always handled here, never by the public Chef bot on the same path:
      // link codes and seller questions must not end up in a public-lead chat. While the line is not usable
      // (suspended or rejected credentials) it waits in the queue and is answered within 24 h once Twilio recovers.
      const event = waInboxMessage(req.body, config, now());
      if (event) {
        try {
          const contact = contactOf(event.from);
          const code = /^VINCULAR ([a-f0-9]{32})$/i.exec(event.text);
          const linked = code
            ? await bindings.exists({ challenge: hash(code[1].toLowerCase()), expectedContact: contact, until: { $gt: now() } })
            : await bindings.exists({ contact });
          const id = linked ? hash(config.phone + ':' + event.id) : hash(contact + ':welcome:' + Math.floor(event.at / DAY));
          const encrypted = sealToken(JSON.stringify(linked ? event : { from: event.from, at: event.at, welcome: true }), config.key, id);
          const queued = await jobs.updateOne({ _id: id }, {
            $setOnInsert: { encrypted, phase: 'queued', at: new Date(event.at), expiresAt: new Date(event.at + DAY) }
          }, { upsert: true });
          if (linked && queued.upsertedCount) console.info('[mi-coach-whatsapp] Seller message queued:', code ? 'link' : event.audio ? 'voice' : 'text');
        } catch (error) {
          if (error?.code !== 11000) return res.sendStatus(503);
        }
      }
      return res.type('text/xml').send('<Response></Response>');
    });
  }

  async function state(owner, email) {
    if (!seller(email)) return null;
    return { online: lineOnline(), linked: Boolean(await bindings.exists({ _id: owner, contact: { $exists: true } })) };
  }

  function registerConnections(app, own, back) {
    app.post(COACH_WA_INBOX_PATH + 'link', own(async (req, res, actor) => {
      if (!seller(actor.email) || !actor.direct) return res.sendStatus(403);
      const phone = String(req.body.phone || '').trim().replace(/^\+/, '').replace(/[\s()-]/g, '');
      if (!phonePattern.test(phone)) return back(res, 'waInboxPhone');
      if (phone === config.phone) return back(res, 'waInboxPersonalPhone');
      if (!await checkLine({ cached: true })) return back(res, 'waInboxUnavailable');
      const code = randomBytes(16).toString('hex');
      try {
        await bindings.updateOne({ _id: actor.id, contact: { $exists: false } }, {
          $set: { challenge: hash(code), expectedContact: contactOf(phone), until: new Date(now() + 10 * MINUTE), zone: validZone(req.body.zone) || 'America/Chicago' }
        }, { upsert: true });
      } catch (error) {
        if (error?.code === 11000) return back(res, 'waInboxAlreadyLinked');
        throw error;
      }
      return res.redirect(303, waLink + '?text=' + encodeURIComponent('VINCULAR ' + code));
    }));
    app.post(COACH_WA_INBOX_PATH + 'unlink', own(async (_req, res, actor) => {
      if (!actor.direct || !bindings) return res.sendStatus(403);
      await bindings.deleteOne({ _id: actor.id });
      return back(res, 'waInboxUnlinked');
    }));
  }

  async function reply(event) {
    if (event.welcome) return { text: 'Soy Jorge, de TEC Coach. Si eres vendedor, vincula tu WhatsApp en Mis conexiones: ' + connectPage };
    const contact = contactOf(event.from);
    const code = /^VINCULAR ([a-f0-9]{32})$/i.exec(event.text);
    if (code) {
      try {
        const binding = await bindings.findOneAndUpdate({
          challenge: hash(code[1].toLowerCase()), expectedContact: contact, until: { $gt: now() }, contact: { $exists: false }
        }, { $set: { contact }, $unset: { challenge: 1, expectedContact: 1, until: 1 } }, { new: true }).lean();
        if (binding) {
          const user = await getUser(binding._id);
          if (seller(user?.email) && await eligible(user)) return { owner: binding._id, contact, text: 'Tu WhatsApp quedó vinculado a TEC. Puedes preguntarme «¿Qué sigue?», «¿A quién le marco?» o «¿Qué quedó de mi demo?».' };
          await bindings.deleteOne({ _id: binding._id, contact });
        }
      } catch (error) {
        if (error?.code !== 11000) throw error;
      }
      return { text: 'Revisa la vinculación en Mis conexiones: ' + connectPage };
    }
    const binding = await bindings.findOne({ contact }).lean();
    const user = binding ? await getUser(binding._id) : null;
    if (!seller(user?.email) || !await eligible(user)) return null;
    let text;
    try {
      if (event.audio) {
        const audio = await transport.audio(event.audio);
        text = await voice.transcribe(audio.buffer, audio.mime, {
          prompt: 'Transcribe el mensaje para Jorge de TEC Coach. Agenda, citas, telemarketing, resultados de llamadas y 4 en 14. No inventes palabras.'
        });
      } else text = event.text;
    } catch { return { text: 'No pude transcribir ese audio. Prueba una nota más corta o envíame tu pregunta por escrito.' }; }
    const actor = { owner: user._id, email: user.email, name: user.name, zone: binding.zone || 'America/Chicago', session: 'whatsapp:' + contact };
    const context = { actor, binding, myDay: myDay(), latestDemo, now: now(),
      saveCall: (ref, day) => bindings.updateOne({ _id: binding._id, contact }, { $set: { nextCall: ref, callDay: day } }) };
    const result = await callReply(text, context) || await inboxReply(text, context);
    if (!await bindings.exists({ _id: binding._id, contact })) return null;
    return { text: result, voice: Boolean(event.audio), owner: binding._id, contact };
  }

  async function runOne() {
    const nowMs = now();
    const job = await jobs.findOneAndUpdate({
      expiresAt: { $gt: new Date(nowMs) },
      $or: [{ phase: 'queued' }, { phase: 'working', lease: { $lt: new Date(nowMs) } }]
    }, { $set: { phase: 'working', lease: new Date(nowMs + 5 * MINUTE) } }, {
      new: true, sort: { at: 1 }
    }).lean();
    if (!job) return;
    try {
      const event = JSON.parse(openToken(job.encrypted, config.key, job._id));
      const replyData = await reply(event);
      const voiceId = replyData?.voice ? await transport.voice(replyData.text).catch(() => null) : null;
      const claim = await jobs.updateOne({ _id: job._id, phase: 'working', lease: job.lease },
        { $set: { phase: 'done' }, $unset: { encrypted: 1, lease: 1 } });
      // Commit before sending. A network failure with unknown delivery never causes a duplicate reply.
      if (claim.modifiedCount && replyData?.text && now() - event.at < DAY &&
        (!replyData.owner || await bindings.exists({ _id: replyData.owner, contact: replyData.contact }))) {
        try {
          await transport.send(event.from, replyData.text, voiceId);
          if (replyData.owner) console.info('[mi-coach-whatsapp] Seller reply accepted:', voiceId ? 'voice' : 'text');
        } catch (error) {
          await jobs.updateOne({ _id: job._id, phase: 'done' }, { $set: { phase: 'send-failed' } });
          console.warn('[mi-coach-whatsapp] Reply send failed:', Number.isInteger(error?.status) ? error.status : 'connection');
        }
      }
    } catch {
      const failed = await jobs.updateOne({ _id: job._id, phase: 'working', lease: job.lease },
        { $set: { phase: 'failed' }, $unset: { encrypted: 1, lease: 1 } }).catch(() => null);
      // Tell a linked seller once that the message failed, instead of leaving it unanswered.
      let event = null;
      try { event = JSON.parse(openToken(job.encrypted, config.key, job._id)); } catch { /* unreadable job */ }
      if (failed?.modifiedCount && event?.from && !event.welcome && now() - event.at < DAY) {
        try {
          if (await bindings.exists({ contact: contactOf(event.from) })) {
            await transport.send(event.from, 'No pude terminar de procesar ese mensaje. Revisa Mi día en el Coach antes de repetirlo, por si algo alcanzó a guardarse.');
          }
        } catch { console.warn('[mi-coach-whatsapp] Failure notice not sent.'); }
      }
    }
  }

  function start() {
    if (!available || worker) return;
    void checkLine();
    monitor = setInterval(checkLine, LINE_REFRESH);
    monitor.unref();
    worker = setInterval(async () => {
      if (running || !lineOnline()) return;
      running = true;
      try { await runOne(); }
      catch { console.error('[mi-coach-whatsapp] Worker temporarily unavailable.'); }
      finally { running = false; }
    }, 1000);
    worker.unref();
  }
  return { registerWebhook, registerConnections, state, start, stop: () => { clearInterval(worker); clearInterval(monitor); worker = undefined; }, busy: () => running };
}
