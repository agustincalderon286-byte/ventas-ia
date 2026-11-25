import { randomUUID } from 'node:crypto';
import { followUpQuestion } from './coach-demo-auto-close.js';

// Coach Jorge during the demo: the Mac transcribes each 4-minute piece while the visit goes on,
// Luna looks for what the recording left unclear, and the seller's phone gets a short notice.
// Nothing is saved from here; the full analysis at the end still decides what is written.
export const LIVE_MAX_NOTICES = 4;
export const LIVE_MAX_CHECKS = 15;
const leaseTime = 10 * 60 * 1000;
const maxChunkText = 16_000;
const fold = text => String(text || '').normalize('NFD').replace(/\p{M}/gu, '').replace(/\s+/g, ' ').trim().toLowerCase();
const clip = (text, max) => String(text || '').replace(/[\p{Cc}<>]/gu, ' ').replace(/\s+/g, ' ').trim().slice(0, max);

// Turns what the live check left in doubt into notices. «short» has no names, phones or amounts: it is
// what a locked screen shows. «text» is what the seller reads in TEC.
export function liveNoticesFrom(facts, { followUp = '', lastSegmentText = '' } = {}) {
  const notices = [];
  const latest = fold(lastSegmentText);
  for (const item of Array.isArray(facts?.review) ? facts.review : []) {
    if (item?.state !== 'preguntar') continue;
    const label = clip(item.label, 80), text = clip(item.text, 120);
    if (item.field === 'referral' && label) notices.push({ key: 'referral:' + fold(label), short: 'Un referido tiene el teléfono dudoso.',
      text: 'Referido ' + label + ': el teléfono no quedó claro. Pídeselo otra vez en voz alta.' });
    // The visit goes on: a referral cut at the end of the newest piece may be completed in the next one.
    else if (item.field === 'referral' && text && !(latest && latest.includes(fold(text).slice(0, 60)))) notices.push({
      key: 'incomplete:' + fold(text).slice(0, 60), short: 'Un referido quedó incompleto.',
      text: 'Referido incompleto: «' + text + '». Pide el nombre completo y el teléfono.' });
    else if (item.field === 'price' && label) notices.push({ key: 'price:' + fold(label), short: 'Un precio no quedó claro.',
      text: 'Precio de ' + label + ': oí «' + text + '», pero pudo quedar cortado. Repítelo con el cliente.' });
    else if (item.field === 'followUp') notices.push({ key: 'followUp:' + fold(label), short: 'La fecha de la cita no quedó clara.',
      text: 'Cita «' + label + '»: ' + (text || 'no quedó clara la fecha o la hora.') });
  }
  if (followUp && facts?.followUp?.when) notices.push({ key: 'followUp:' + fold(facts.followUp.when), short: 'La fecha de la cita no quedó clara.',
    text: 'Cita «' + clip(facts.followUp.when, 80) + '»: ' + clip(followUp, 160) });
  const seen = new Set();
  return notices.filter(notice => !seen.has(notice.key) && seen.add(notice.key));
}

export function createLiveDemo({ analyses, audio, analyze, now = Date.now }) {
  const leased = (id, lease) => ({ _id: id, status: 'recording', 'live.lease': lease, 'live.leaseUntil': { $gt: new Date(now()) } });

  async function check(id) {
    const doc = await analyses.findOneAndUpdate({ _id: id, status: 'recording', 'live.analyzing': { $ne: true },
      'live.checks': { $lt: LIVE_MAX_CHECKS } }, { $set: { 'live.analyzing': true }, $inc: { 'live.checks': 1 } }, { new: true }).lean();
    if (!doc) return;
    try {
      const texts = [...(doc.live.texts || [])].sort((a, b) => a.index - b.index);
      const durations = new Map((doc.chunks || []).map(chunk => [chunk.index, chunk.durationSeconds]));
      const chunks = texts.map(item => ({ ...item, durationSeconds: durations.get(item.index) || 0 }));
      if (chunks.map(chunk => chunk.text).join(' ').trim().length < 40) return;
      const facts = await analyze({ chunks });
      const followUp = facts?.followUp ? followUpQuestion(doc, facts.followUp, now()) : '';
      const found = liveNoticesFrom(facts, { followUp, lastSegmentText: chunks[chunks.length - 1]?.text });
      const known = new Set((doc.live.notices || []).map(notice => notice.key));
      const fresh = found.filter(notice => !known.has(notice.key)).slice(0, Math.max(0, LIVE_MAX_NOTICES - known.size));
      if (!fresh.length) return;
      const at = new Date(now());
      await analyses.updateOne({ _id: id, status: 'recording' }, { $push: { 'live.notices': { $each: fresh.map((notice, offset) =>
        ({ ...notice, n: known.size + offset + 1, at })) } } });
    } catch (error) {
      console.error('No se pudo revisar la demo en vivo', { name: error.name });
    } finally {
      await analyses.updateOne({ _id: id }, { $set: { 'live.analyzing': false } }).catch(() => null);
    }
  }

  return {
    initial: () => ({ enabled: true, pending: [], texts: [], notices: [], checks: 0 }),
    async queue(id, index) {
      await analyses.updateOne({ _id: id, status: 'recording', 'live.enabled': true }, { $addToSet: { 'live.pending': index } });
    },
    async claim() {
      const time = new Date(now()), lease = randomUUID();
      const job = await analyses.findOneAndUpdate({ status: 'recording', 'live.enabled': true, 'live.pending.0': { $exists: true },
        $or: [{ 'live.leaseUntil': null }, { 'live.leaseUntil': { $lte: time } }] },
      { $set: { 'live.lease': lease, 'live.leaseUntil': new Date(now() + leaseTime) } },
      { new: true, sort: { updatedAt: 1 } }).select('_id chunks.index chunks.mimeType chunks.status live.pending').lean();
      if (!job) return null;
      const chunks = job.chunks.filter(chunk => chunk.status === 'staged' && job.live.pending.includes(chunk.index)).slice(0, 2);
      if (!chunks.length) {
        await analyses.updateOne({ _id: job._id, 'live.lease': lease }, { $set: { 'live.pending': [], 'live.lease': '', 'live.leaseUntil': null } });
        return null;
      }
      return { id: String(job._id), lease, chunks: chunks.map(({ index, mimeType }) => ({ index, mimeType })) };
    },
    async read(id, index, lease) {
      const job = await analyses.findOne(leased(id, lease)).select('ownerUserId live.pending').lean();
      if (!job?.live?.pending?.includes(index)) return null;
      const chunk = await audio.findOne({ _id: `${job._id}:${index}`, analysisId: job._id, ownerUserId: job.ownerUserId,
        expiresAt: { $gt: new Date(now()) } }).select('+bytes');
      return chunk?.bytes || null;
    },
    async submit(id, lease, result) {
      const job = await analyses.findOne(leased(id, lease)).select('live.pending live.texts').lean();
      if (!job) return 409;
      const release = { 'live.lease': '', 'live.leaseUntil': null };
      if (result?.failed === true || result?.expiredAudio === true) {
        await analyses.updateOne(leased(id, lease), { $set: { ...release, ...(result.expiredAudio && { 'live.pending': [] }) } });
        return 204;
      }
      const inputs = Array.isArray(result?.chunks) ? result.chunks : null;
      if (!inputs?.length || inputs.length > 2 || inputs.some(chunk => !job.live.pending.includes(chunk?.index) ||
          typeof chunk.text !== 'string' || chunk.text.length > maxChunkText ||
          (chunk.voice !== undefined && (!Array.isArray(chunk.voice) || chunk.voice.length > 400)) ||
          (chunk.checks !== undefined && (!Array.isArray(chunk.checks) || chunk.checks.length > 60)))) return 422;
      const done = inputs.map(chunk => chunk.index);
      const texts = [...(job.live.texts || []).filter(item => !done.includes(item.index)), ...inputs.map(chunk => ({
        index: chunk.index, text: chunk.text.trim(), ...(Array.isArray(chunk.voice) && { voice: chunk.voice, checks: chunk.checks || [] }) }))];
      const saved = await analyses.updateOne(leased(id, lease), { $set: { ...release, 'live.texts': texts },
        $pull: { 'live.pending': { $in: done } } });
      if (!saved.modifiedCount) return 409;
      // The worker goes on to the next piece; Luna's check runs on its own.
      check(id).catch(() => null);
      return 204;
    },
    async notices(owner, id, after = 0) {
      const doc = await analyses.findOne({ _id: id, ownerUserId: owner }).select('status live.enabled live.notices').lean();
      if (!doc) return null;
      return { recording: doc.status === 'recording', enabled: Boolean(doc.live?.enabled),
        notices: (doc.live?.notices || []).filter(notice => notice.n > after).map(({ n, short, text }) => ({ n, short, text })) };
    },
    check
  };
}

export function registerLiveDemo(app, { key, live, active }) {
  if (key) {
    const prefix = '/api/coach/demo-worker/live';
    const valid = req => /^[a-f0-9]{24}$/i.test(req.params.id) && /^[a-f0-9-]{36}$/i.test(req.get('X-TEC-Demo-Lease') || '');
    // The worker key is checked by the middleware of /api/coach/demo-worker.
    app.post(`${prefix}/claim`, async (req, res) => {
      const job = await live.claim();
      return job ? res.json(job) : res.status(204).end();
    });
    app.get(`${prefix}/:id/audio/:index`, async (req, res) => {
      if (!valid(req) || !/^\d{1,2}$/.test(req.params.index)) return res.sendStatus(404);
      const bytes = await live.read(req.params.id, Number(req.params.index), req.get('X-TEC-Demo-Lease'));
      return bytes ? res.type('application/octet-stream').send(Buffer.from(bytes)) : res.sendStatus(404);
    });
    app.post(`${prefix}/:id/result`, async (req, res) => {
      if (!valid(req) || !req.body || typeof req.body !== 'object') return res.sendStatus(400);
      res.status(await live.submit(req.params.id, req.get('X-TEC-Demo-Lease'), req.body)).end();
    });
  }
  app.get('/api/coach/demo-analyses/:id/live', async (req, res) => {
    res.set('Cache-Control', 'private, no-store').vary('Cookie');
    const auth = await active(req, res);
    if (!auth) return;
    if (!/^[a-f0-9]{24}$/i.test(req.params.id)) return res.status(404).json({ error: 'Esa grabación no existe.' });
    const after = Number.parseInt(req.query?.after || '0', 10);
    const result = await live.notices(auth.user._id, req.params.id, Number.isInteger(after) && after >= 0 ? after : 0);
    return result ? res.json(result) : res.status(404).json({ error: 'Esa grabación no existe.' });
  });
}
