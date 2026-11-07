import fs from 'node:fs';
import crypto from 'node:crypto';
import { Transform } from 'node:stream';
import { pipeline } from 'node:stream/promises';
import { parseAudioRange } from './tec-audio-lab.js';

export const audiobookCatalog = JSON.parse(fs.readFileSync(new URL('./knowledge/tec-audiobooks.json', import.meta.url), 'utf8'));
const transcripts = new Map();
export function getAudiobookTranscript(id) {
  if (!audiobookCatalog.some(book => book.id === id)) return null;
  if (!transcripts.has(id)) {
    const file = new URL(`./knowledge/audiobook-texts/${id}.json`, import.meta.url);
    transcripts.set(id, fs.existsSync(file) ? JSON.parse(fs.readFileSync(file, 'utf8')) : null);
  }
  return transcripts.get(id);
}
export function createAudiobookModels(mongoose) {
  const schema = fields => new mongoose.Schema(fields, { timestamps: true });
  const media = schema({ _id: String, state: String, fileId: mongoose.Schema.Types.ObjectId, sha256: String, bytes: Number, leaseUntil: Date });
  const review = schema({ _id: String, bookId: { type: String, index: true }, userId: String, author: String, rating: Number, text: String });
  const progress = schema({ _id: String, userId: String, bookId: String, position: Number, speed: Number });
  return {
    media: mongoose.models.TecAudiobookMedia || mongoose.model('TecAudiobookMedia', media),
    reviews: mongoose.models.TecAudiobookReview || mongoose.model('TecAudiobookReview', review),
    progress: mongoose.models.TecAudiobookProgress || mongoose.model('TecAudiobookProgress', progress)
  };
}
export function normalizeAudiobookReview(body = {}) {
  const text = typeof body.text === 'string' ? body.text.trim() : '';
  if (!Number.isInteger(body.rating) || body.rating < 1 || body.rating > 5) throw new Error('Elige una calificación de 1 a 5 estrellas.');
  if (text.length > 2000) throw new Error('La reseña admite hasta 2,000 caracteres.');
  return { rating: body.rating, text: text.replace(/[\u0000-\u0008\u000b\u000c\u000e-\u001f]/g, '') };
}
export function validImportGrant(req, grant, bookId, now = Date.now()) {
  if (!grant || grant.bookId !== bookId || !Number.isFinite(Date.parse(grant.expiresAt)) || Date.parse(grant.expiresAt) < now) return false;
  const value = req.get('Authorization') || '';
  if (!/^Bearer [A-Za-z0-9_-]{40,150}$/.test(value) || !/^[a-f0-9]{64}$/.test(grant.tokenHash || '')) return false;
  return crypto.timingSafeEqual(crypto.createHash('sha256').update(value.slice(7)).digest(), Buffer.from(grant.tokenHash, 'hex'));
}
export async function findReadyAudiobook(book, media, r2) {
  if (book.audio.r2Key) return await r2?.ready(book.audio) ? { storage: 'r2' } : null;
  return media.findOne({ _id: book.id, state: 'ready', sha256: book.audio.sha256 }).lean();
}

export async function streamAudiobook(req, res, getBucket, book, record, r2) {
  const { bytes, filename } = book.audio;
  const fromR2 = record.storage === 'r2';
  const bucket = fromR2 ? null : getBucket();
  if (bucket) {
    const [file] = await bucket.find({ _id: record.fileId, 'metadata.bookId': book.id, 'metadata.sha256': book.audio.sha256 }).limit(1).toArray();
    if (!file || file.length !== bytes) return res.status(404).end();
  }
  let range;
  try { range = parseAudioRange(req.get('Range'), bytes); }
  catch { return res.status(416).set('Content-Range', `bytes */${bytes}`).end(); }
  const abort = new AbortController();
  const onClose = () => {
    if (!res.writableFinished) abort.abort();
  };
  res.once('close', onClose);
  try {
    let stream;
    if (req.method !== 'HEAD') {
      stream = fromR2 ? await r2.open(book.audio, range, abort.signal)
        : bucket.openDownloadStream(record.fileId, range ? { start: range.start, end: range.end + 1 } : {});
    }
    res.set({ 'Content-Type': 'audio/mp4', 'Accept-Ranges': 'bytes', 'Content-Disposition': `inline; filename="${filename}"` });
    if (range) res.status(206).set({ 'Content-Range': `bytes ${range.start}-${range.end}/${bytes}`, 'Content-Length': String(range.end - range.start + 1) });
    else res.set('Content-Length', String(bytes));
    if (!stream) return res.end();
    await pipeline(stream, res, { signal: abort.signal });
  } finally {
    res.off('close', onClose);
  }
}
export function registerTecAudiobooks(app, { requireActive, requireAdmin, media, reviews, progress, getBucket, r2, catalog = audiobookCatalog, importGrant = null, getTranscript = getAudiobookTranscript }) {
  const byId = new Map(catalog.map(book => [book.id, book]));
  const privateHeaders = res => res.set({ 'Cache-Control': 'private, no-store', 'Vary': 'Cookie', 'X-Robots-Tag': 'noindex, nofollow' });
  const wrap = handler => async (req, res) => {
    privateHeaders(res);
    try { await handler(req, res); }
    catch (error) {
      if (!res.destroyed && error.code !== 'ERR_STREAM_PREMATURE_CLOSE') console.error('TEC Audiobooks request failed', { name: error.name, code: error.code || '' });
      if (!res.headersSent && !res.destroyed) res.status(503).json({ error: 'No pude completar la solicitud. Inténtalo de nuevo.' });
      else if (!res.destroyed) res.destroy();
    }
  };
  function sameOrigin(req, res) {
    try {
      const origin = req.get('Origin');
      if (req.get('Sec-Fetch-Site') === 'cross-site' || (origin && new URL(origin).host !== req.get('host'))) throw new Error();
      return true;
    } catch { res.status(403).json({ error: 'Abre Audiolibros desde TEC para guardar cambios.' }); return false; }
  }
  const reviewKey = (id, user) => `${id}:${user._id}`;
  const publicBook = (book, ready = false) => {
    const { audio, ...info } = book;
    return { ...info, available: ready, audioUrl: ready ? `/api/coach/audiobooks/${book.id}/audio` : null,
      transcriptUrl: ready && getTranscript(book.id) ? `/api/coach/audiobooks/${book.id}/text` : null };
  };
  const publicReview = (row, user) => ({ author: row.author || 'Lector TEC', rating: row.rating, text: row.text || '', updatedAt: row.updatedAt, mine: row.userId === String(user._id) });
  async function readyBook(req, res) {
    const book = byId.get(req.params.id);
    if (!book) { res.status(404).json({ error: 'No encontré ese audiolibro.' }); return null; }
    const file = await findReadyAudiobook(book, media, r2);
    if (!file) { res.status(404).json({ error: 'El audio todavía no está disponible.' }); return null; }
    return { book, file };
  }
  async function stats(id) {
    const [value] = await reviews.aggregate([{ $match: { bookId: id } }, { $group: { _id: null, count: { $sum: 1 }, average: { $avg: '$rating' } } }]);
    return { count: value?.count || 0, average: value ? Math.round(value.average * 10) / 10 : 0 };
  }
  app.get('/api/coach/audiobooks', wrap(async (req, res) => {
    const auth = await requireActive(req, res); if (!auth) return;
    const files = await media.find({ state: 'ready', _id: { $in: catalog.map(b => b.id) } }).lean();
    const books = await Promise.all(catalog.map(async book => publicBook(book, book.audio.r2Key
      ? Boolean(await r2?.ready(book.audio))
      : files.some(f => f._id === book.id && f.sha256 === book.audio.sha256))));
    res.json({ books });
  }));
  app.get('/api/coach/audiobooks/:id', wrap(async (req, res) => {
    const auth = await requireActive(req, res); if (!auth) return;
    const found = await readyBook(req, res); if (!found) return;
    const [rating, saved] = await Promise.all([stats(found.book.id), progress.findOne({ _id: reviewKey(found.book.id, auth.user) }).lean()]);
    res.json({ book: publicBook(found.book, true), rating, progress: { position: saved?.position || 0, speed: saved?.speed || 1 } });
  }));
  app.get('/api/coach/audiobooks/:id/text', wrap(async (req, res) => {
    const auth = await requireActive(req, res); if (!auth) return;
    const found = await readyBook(req, res); if (!found) return;
    const text = getTranscript(found.book.id);
    if (!text) return res.status(404).json({ error: 'El texto no está disponible.' });
    res.json({ bookId: found.book.id, title: found.book.title, cues: text.cues, trailingSeparator: text.trailingSeparator });
  }));
  // Express also handles HEAD here; metadata and permissions are checked before opening any stream.
  app.get('/api/coach/audiobooks/:id/audio', wrap(async (req, res) => {
    const auth = await requireActive(req, res); if (!auth) return;
    const found = await readyBook(req, res); if (!found) return;
    await streamAudiobook(req, res, getBucket, found.book, found.file, r2);
  }));
  app.get('/api/coach/audiobooks/:id/reviews', wrap(async (req, res) => {
    const auth = await requireActive(req, res); if (!auth) return;
    if (!await readyBook(req, res)) return;
    const page = Math.min(500, Math.max(1, Number.parseInt(req.query.page, 10) || 1));
    const [rows, mine, rating] = await Promise.all([
      reviews.find({ bookId: req.params.id }).sort({ updatedAt: -1, _id: 1 }).skip((page - 1) * 20).limit(20).lean(),
      reviews.findOne({ _id: reviewKey(req.params.id, auth.user) }).lean(), stats(req.params.id)
    ]);
    res.json({ reviews: rows.map(r => publicReview(r, auth.user)), mine: mine ? publicReview(mine, auth.user) : null, rating, page, hasMore: page * 20 < rating.count });
  }));
  app.put('/api/coach/audiobooks/:id/review', wrap(async (req, res) => {
    const auth = await requireActive(req, res); if (!auth || !sameOrigin(req, res)) return;
    if (!await readyBook(req, res)) return;
    let body;
    try { body = normalizeAudiobookReview(req.body); } catch (e) { return res.status(400).json({ error: e.message }); }
    // An atomic document id guarantees one review per account even during concurrent upserts.
    const key = reviewKey(req.params.id, auth.user);
    const name = typeof auth.user.name === 'string' && !auth.user.name.includes('@') ? auth.user.name.trim().slice(0, 80) : 'Lector TEC';
    const update = { $set: { ...body, bookId: req.params.id, userId: String(auth.user._id), author: name || 'Lector TEC' } };
    let row;
    try { row = await reviews.findOneAndUpdate({ _id: key }, update, { upsert: true, new: true, runValidators: true }).lean(); }
    catch (e) { if (e.code !== 11000) throw e; row = await reviews.findOneAndUpdate({ _id: key }, update, { new: true, runValidators: true }).lean(); }
    res.json({ review: publicReview(row, auth.user), rating: await stats(req.params.id) });
  }));
  app.delete('/api/coach/audiobooks/:id/review', wrap(async (req, res) => {
    const auth = await requireActive(req, res); if (!auth || !sameOrigin(req, res)) return;
    if (!await readyBook(req, res)) return;
    await reviews.deleteOne({ _id: reviewKey(req.params.id, auth.user) });
    res.json({ deleted: true, rating: await stats(req.params.id) });
  }));
  app.put('/api/coach/audiobooks/:id/progress', wrap(async (req, res) => {
    const auth = await requireActive(req, res); if (!auth || !sameOrigin(req, res)) return;
    const found = await readyBook(req, res); if (!found) return;
    const { position, speed } = req.body || {};
    if (!Number.isFinite(position) || position < 0 || position > found.book.duration + 1 || !Number.isFinite(speed) || speed < 0.5 || speed > 2) return res.status(400).json({ error: 'Posición o velocidad inválida.' });
    const query = { _id: reviewKey(req.params.id, auth.user) };
    const update = { $set: { userId: String(auth.user._id), bookId: req.params.id, position: Math.min(position, found.book.duration), speed } };
    try { await progress.updateOne(query, update, { upsert: true }); }
    catch (e) { if (e.code !== 11000) throw e; await progress.updateOne(query, update); }
    res.json({ saved: true });
  }));
  // The initial installer may add only a manifest-pinned byte-for-byte asset. It cannot
  // read accounts, alter catalog data, or replace an installed book. The temporary
  // hash grant expires and is removed from the deployment after the import.
  app.post('/api/control/audiobooks/:id/import', wrap(async (req, res) => {
    const book = byId.get(req.params.id);
    if (!book) return res.status(404).end();
    if (!validImportGrant(req, importGrant, book.id)) {
      if (!await requireAdmin(req, res) || !sameOrigin(req, res)) return;
    }
    if (book.audio.r2Key) return res.status(409).json({ error: 'Este libro se administra en el almacenamiento de audiolibros.' });
    if (!['audio/mp4', 'audio/x-m4b'].includes((req.get('Content-Type') || '').split(';')[0])) return res.status(415).json({ error: 'Se requiere el archivo M4B aprobado.' });
    if (Number(req.get('Content-Length')) !== book.audio.bytes) return res.status(400).json({ error: 'El tamaño no coincide con la edición aprobada.' });
    const now = new Date();
    try {
      await media.findOneAndUpdate({ _id: book.id, state: { $ne: 'ready' }, $or: [{ leaseUntil: { $lt: now } }, { leaseUntil: { $exists: false } }] },
        { $set: { state: 'uploading', leaseUntil: new Date(Date.now() + 20 * 60 * 1000) } }, { upsert: true, new: true });
    } catch (e) { if (e.code === 11000) return res.status(409).json({ error: 'Ya está instalado o hay una carga en curso.' }); throw e; }
    const bucket = getBucket();
    const upload = bucket.openUploadStream(book.audio.filename, { metadata: { bookId: book.id, sha256: book.audio.sha256, contentType: 'audio/mp4' } });
    const hash = crypto.createHash('sha256'); let size = 0; let committed = false;
    const limiter = new Transform({ transform(chunk, _encoding, done) {
      size += chunk.length;
      if (size > book.audio.bytes) return done(new Error('size_limit'));
      hash.update(chunk); done(null, chunk);
    } });
    const timeout = setTimeout(() => req.destroy(), 15 * 60 * 1000); timeout.unref?.();
    try {
      await pipeline(req, limiter, upload);
      if (size !== book.audio.bytes || hash.digest('hex') !== book.audio.sha256) return res.status(422).json({ error: 'El archivo no coincide con el audio aprobado. No se publicó.' });
      await media.updateOne({ _id: book.id, state: 'uploading' }, { $set: { state: 'ready', fileId: upload.id, sha256: book.audio.sha256, bytes: size }, $unset: { leaseUntil: '' } });
      committed = true;
      res.status(201).json({ installed: true, bookId: book.id, sha256: book.audio.sha256, bytes: size });
    } finally {
      clearTimeout(timeout);
      if (!committed) {
        await upload.abort().catch(() => {});
        await bucket.delete(upload.id).catch(() => {});
        await media.updateOne({ _id: book.id, state: 'uploading' }, { $set: { state: 'failed' }, $unset: { leaseUntil: '' } });
      }
    }
  }));
}
