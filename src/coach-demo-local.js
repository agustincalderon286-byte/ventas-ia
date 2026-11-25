import { createHash, randomUUID, timingSafeEqual } from 'node:crypto';

const hour = 60 * 60 * 1000;
const retention = 48 * hour;
const queueLifetime = 24 * hour;
const leaseTime = 30 * 60 * 1000;
const maxAttempts = 2;
const maxChunkText = 16_000;
const stoppedMessage = 'La Mac no pudo terminar esta grabación. Puedes volver a enviarla desde Revisar audio.';
const queued = 'queued_local';

export function createLocalDemoAudioModel(mongoose) {
  const schema = new mongoose.Schema({
    _id: String, analysisId: mongoose.Schema.Types.ObjectId, index: Number, ownerUserId: mongoose.Schema.Types.ObjectId,
    bytes: { type: Buffer, select: false }, expiresAt: Date
  }, { versionKey: false });
  schema.index({ expiresAt: 1 }, { expireAfterSeconds: 0 });
  schema.index({ analysisId: 1, ownerUserId: 1 });
  return mongoose.models.CoachLocalDemoAudio || mongoose.model('CoachLocalDemoAudio', schema);
}

const seconds = value => typeof value === 'number' && Number.isFinite(value) && value >= 0 && value <= 400;
const smallText = value => typeof value === 'string' && value.length <= 1000 && !/[\u0000-\u0008\u000b\u000c\u000e-\u001f]/.test(value);

// Sentences with their times, and the second reading of those with figures. Returns {} when the
// worker sent neither (older format) and false when what it sent is malformed.
function timedReadings(chunk) {
  if (chunk.voice === undefined && chunk.checks === undefined) return {};
  const { voice, checks } = chunk;
  if (!Array.isArray(voice) || !Array.isArray(checks) || voice.length > 400 || checks.length > 60) return false;
  if (voice.some(item => !item || !seconds(item.start) || !seconds(item.end) || item.end < item.start || !smallText(item.text))) return false;
  const used = new Set();
  for (const item of checks) {
    if (!item || !Number.isInteger(item.segment) || item.segment < 0 || item.segment >= voice.length || used.has(item.segment) ||
        !(item.second === null || smallText(item.second)) || !seconds(item.gapBefore) || !seconds(item.gapAfter)) return false;
    used.add(item.segment);
  }
  return {
    voice: voice.map(({ start, end, text }) => ({ start, end, text })),
    checks: checks.map(({ segment, second, gapBefore, gapAfter }) => ({ segment, second, gapBefore, gapAfter }))
  };
}

export function createLocalDemoQueue({ analyses, audio, finish, now = Date.now }) {
  const audioKey = (analysis, index) => `${analysis._id}:${index}`;
  const audioOwner = analysis => ({ analysisId: analysis._id, ownerUserId: analysis.ownerUserId });
  const kept = analysis => ({ ...audioOwner(analysis), expiresAt: { $gt: new Date(now()) } });
  const leased = (id, lease) => ({ _id: id, status: queued, 'localProcessing.lease': lease,
    'localProcessing.leaseUntil': { $gt: new Date(now()) } });
  const noActiveLease = time => ({ $or: [{ 'localProcessing.leaseUntil': null }, { 'localProcessing.leaseUntil': { $lte: time } }] });
  const release = { 'localProcessing.lease': '', 'localProcessing.leaseUntil': null, 'localProcessing.analyzing': false };

  async function fail(job, lease, retry, analyzing = false) {
    const canRetry = retry && job.localProcessing.attempts < maxAttempts;
    const saved = await analyses.updateOne({ ...leased(job._id, lease), 'localProcessing.analyzing': analyzing }, { $set: {
      ...release, status: canRetry ? queued : 'error', error: canRetry ? '' : stoppedMessage,
      'localProcessing.nextAt': new Date(now() + 30_000), updatedAt: new Date(now())
    } });
    return saved.modifiedCount ? 204 : 409;
  }

  return {
    async stage(analysis, index, bytes, duplicate) {
      const key = { _id: audioKey(analysis, index), ...audioOwner(analysis), index };
      if (duplicate && await audio.exists({ ...key, expiresAt: { $gt: new Date(now()) } })) return false;
      await audio.updateOne(key, { $set: { bytes, expiresAt: new Date(now() + retention) } }, { upsert: true });
      return true;
    },
    async enqueue(analysis, selection) {
      const ids = selection.indexes.map(index => audioKey(analysis, index));
      const renewed = await audio.updateMany({ ...kept(analysis), _id: { $in: ids } },
        { $set: { expiresAt: new Date(now() + retention) } });
      if (renewed.matchedCount !== ids.length) return false;
      const time = new Date(now());
      analysis.localProcessing = { ...selection, attempts: 0, queuedAt: time, nextAt: time,
        expiresAt: new Date(now() + queueLifetime) };
      analysis.status = queued;
      analysis.error = '';
      analysis.stoppedAt = time;
      analysis.updatedAt = time;
      await analysis.save();
      return true;
    },
    async expire(scope = {}) {
      const time = new Date(now());
      await analyses.updateMany({ ...scope, status: queued, $and: [noActiveLease(time), { $or: [
        { 'localProcessing.expiresAt': { $lte: time } }, { 'localProcessing.attempts': { $gte: maxAttempts } },
        { 'localProcessing.analyzing': true }
      ] }] }, { $set: { ...release, status: 'error', error: stoppedMessage, updatedAt: time } });
    },
    async claim() {
      const time = new Date(now());
      const lease = randomUUID();
      const job = await analyses.findOneAndUpdate({ status: queued, ...noActiveLease(time), 'localProcessing.analyzing': { $ne: true },
        'localProcessing.attempts': { $lt: maxAttempts }, 'localProcessing.nextAt': { $lte: time },
        'localProcessing.expiresAt': { $gt: time }
      }, { $set: { 'localProcessing.lease': lease, 'localProcessing.leaseUntil': new Date(now() + leaseTime),
        'localProcessing.analyzing': false }, $inc: { 'localProcessing.attempts': 1 } },
      { new: true, sort: { 'localProcessing.queuedAt': 1 } }).select('_id chunks.index chunks.mimeType localProcessing').lean();
      if (!job) return null;
      const { indexes } = job.localProcessing;
      return { id: String(job._id), lease, chunks: job.chunks.filter(chunk => indexes.includes(chunk.index)) };
    },
    async read(id, index, lease) {
      const job = await analyses.findOne(leased(id, lease)).select('ownerUserId localProcessing.indexes').lean();
      if (!job?.localProcessing.indexes.includes(index)) return null;
      const chunk = await audio.findOne({ ...kept(job), _id: audioKey(job, index) }).select('+bytes');
      return chunk?.bytes || null;
    },
    async submit(id, lease, result) {
      const query = leased(id, lease);
      const job = await analyses.findOne(query).lean();
      if (!job) return 409;
      if (result.failed === true || result.expiredAudio === true) {
        return fail(job, lease, result.failed === true);
      }
      const inputs = result.chunks;
      const { indexes, partial, gaps, knownOutcome } = job.localProcessing;
      if (!Array.isArray(inputs) || inputs.length !== indexes.length ||
          new Set(inputs.map(chunk => chunk?.index)).size !== indexes.length || inputs.some(chunk =>
            !indexes.includes(chunk?.index) || typeof chunk.text !== 'string' || chunk.text.length > maxChunkText ||
            (chunk.language !== '' && !/^[a-z]{2,3}$/.test(chunk.language || ''))) ||
          inputs.reduce((length, chunk) => length + chunk.text.length, 0) > 140_000) return 422;

      const readings = new Map(inputs.map(chunk => [chunk.index, timedReadings(chunk)]));
      if ([...readings.values()].includes(false)) return 422;

      // A repeated POST with the same lease cannot start another Luna request.
      const active = await analyses.updateOne({ ...query, 'localProcessing.analyzing': false }, { $set: {
        'localProcessing.analyzing': true, 'localProcessing.leaseUntil': new Date(now() + 5 * 60 * 1000)
      } });
      if (!active.modifiedCount) return 409;
      const chunks = job.chunks.map(chunk => {
        const input = inputs.find(item => item.index === chunk.index);
        if (!input) return chunk;
        // Older workers send text only; their figures are saved as before, marked as unchecked.
        const { voice, checks } = readings.get(chunk.index) || {};
        const { voice: oldVoice, checks: oldChecks, ...base } = chunk;
        return { ...base, status: 'transcribed', text: input.text.trim(), languages: input.language ? [input.language] : [],
          transcribedAt: new Date(now()), error: '', ...(voice && { voice, checks }) };
      });
      let completion;
      try {
        completion = await finish({ ...job, chunks }, { partial, included: indexes, rawGaps: gaps, knownOutcome });
      } catch (error) {
        console.error('No se pudo preparar el reporte local', { name: error.name });
        return fail(job, lease, false, true);
      }
      const saved = await analyses.updateOne(leased(id, lease), { $set: { ...release, ...completion, chunks } });
      if (!saved.modifiedCount) return 409;
      await audio.deleteMany(audioOwner(job)).catch(() => null);
      return 204;
    },
    remove: analysis => audio.deleteMany(audioOwner(analysis))
  };
}

export function registerLocalDemoWorker(app, { key, queue }) {
  if (!key) return;
  const prefix = '/api/coach/demo-worker';
  const digest = value => createHash('sha256').update(value).digest();
  const expected = digest(`Bearer ${key}`);
  app.use(prefix, (req, res, next) => {
    if (!timingSafeEqual(digest(req.get('Authorization') || ''), expected)) return res.sendStatus(404);
    next();
  });
  app.post(`${prefix}/claim`, async (req, res) => {
    await queue.expire();
    const job = await queue.claim();
    return job ? res.json(job) : res.status(204).end();
  });
  const valid = req => /^[a-f0-9]{24}$/i.test(req.params.id) && /^[a-f0-9-]{36}$/i.test(req.get('X-TEC-Demo-Lease') || '');
  app.get(`${prefix}/:id/audio/:index`, async (req, res) => {
    if (!valid(req) || !/^\d{1,2}$/.test(req.params.index)) return res.sendStatus(404);
    const bytes = await queue.read(req.params.id, Number(req.params.index), req.get('X-TEC-Demo-Lease'));
    return bytes ? res.type('application/octet-stream').send(Buffer.from(bytes)) : res.sendStatus(404);
  });
  app.post(`${prefix}/:id/result`, async (req, res) => {
    if (!valid(req) || !req.body || typeof req.body !== 'object') return res.sendStatus(400);
    const status = await queue.submit(req.params.id, req.get('X-TEC-Demo-Lease'), req.body);
    res.status(status).end();
  });
}
