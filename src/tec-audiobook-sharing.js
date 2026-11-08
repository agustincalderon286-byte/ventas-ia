import crypto from 'node:crypto';
import express from 'express';
import { spawn } from 'node:child_process';
import { mkdtemp, writeFile, readFile, rm, mkdir, readdir, stat } from 'node:fs/promises';
import path from 'node:path';
import os from 'node:os';
import sharp from 'sharp';
import QRCode from 'qrcode';
import ffmpeg from 'ffmpeg-static';
import { audiobookCatalog, getAudiobookTranscript, findReadyAudiobook, streamAudiobook } from './tec-audiobooks.js';

export const SHARE_LIMITS = Object.freeze({ characters: 600, seconds: 45, maxPassages: 6 });
const idPattern = /^[a-f0-9]{32}$/;
const escape = value => String(value).replace(/[&<>"']/g, char => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[char]));
const normalize = text => text.replace(/\s+/g, ' ').trim();
const publicDir = new URL('../public/', import.meta.url);

export function createAudiobookShareModel(mongoose) {
  return mongoose.models.TecAudiobookShare || mongoose.model('TecAudiobookShare', new mongoose.Schema({
    _id: String, bookId: String, kind: String, quote: String, start: Number, end: Number, createdAt: Date
  }));
}

export function resolveShare(book, transcript, input) {
  const cues = transcript?.cues || [];
  if (!book || !cues.length || !['quote', 'audio'].includes(input?.kind)) throw new Error('Elige un libro y un fragmento.');
  let start, end, quote;
  if (input.kind === 'quote') {
    const { first, last } = input;
    if (!Number.isInteger(first) || !Number.isInteger(last) || first < 0 || last < first ||
      last >= cues.length || last - first >= SHARE_LIMITS.maxPassages) throw new Error('Elige entre uno y seis pasajes consecutivos.');
    start = cues[first].start; end = cues[last].end;
    quote = normalize(cues.slice(first, last + 1).map(c => c.text).join(''));
    if (!quote || [...quote].length > SHARE_LIMITS.characters) throw new Error('La cita admite hasta 600 caracteres. Elige menos texto.');
  } else {
    ({ start, end } = input);
    if (!Number.isFinite(start) || !Number.isFinite(end) || start < 0 || end > book.duration || end - start < 3 || end - start > SHARE_LIMITS.seconds) {
      throw new Error('El fragmento de audio debe durar entre 3 y 45 segundos.');
    }
    const text = normalize(cues.filter(c => c.start < end && c.end > start).map(c => c.text).join(''));
    quote = [...text].length > 380 ? [...text].slice(0, 377).join('') + '…' : text;
  }
  if (start < 0 || end > book.duration + .1) throw new Error('El fragmento está fuera del libro.');
  return { bookId: book.id, kind: input.kind, start, end, quote };
}

function linesFor(text, columns) {
  const lines = []; let line = '';
  const words = normalize(text).split(' ').flatMap(word => {
    const letters = [...word], chunks = [];
    while (letters.length > columns) chunks.push(letters.splice(0, columns).join(''));
    return chunks.concat(letters.join(''));
  });
  for (const word of words) {
    if (line && [...line + ' ' + word].length > columns) { lines.push(line); line = word; }
    else line += (line ? ' ' : '') + word;
  }
  if (line) lines.push(line);
  return lines;
}

export async function quoteCard(book, share, url, format = 'feed') {
  const height = format === 'story' ? 1920 : 1350;
  let fontSize = [...share.quote].length > 400 ? 36 : [...share.quote].length > 230 ? 48 : 60;
  let lines = linesFor(share.quote, Math.floor(850 / (fontSize * .57)));
  const top = format === 'story' ? 360 : 220;
  const available = height - 410 - top;
  while (lines.length * fontSize * 1.34 > available && fontSize > 26) {
    fontSize -= 2; lines = linesFor(share.quote, Math.floor(850 / (fontSize * .57)));
  }
  const qr = await QRCode.toBuffer(url, { width: 180, margin: 1, color: { dark: '#153f32', light: '#fff9e9' } });
  const cover = await readFile(new URL(book.cover.replace(/^\//, ''), publicDir));
  const coverPNG = await sharp(cover).resize(170, 228, { fit: 'cover' }).png().toBuffer();
  const y = top + Math.max(0, (available - lines.length * fontSize * 1.34) / 2);
  const svg = `<svg xmlns="http://www.w3.org/2000/svg" width="1080" height="${height}" viewBox="0 0 1080 ${height}">
    <defs><linearGradient id="bg" x2="1" y2="1"><stop stop-color="#123c30"/><stop offset="1" stop-color="#071f1a"/></linearGradient></defs>
    <rect width="1080" height="${height}" fill="url(#bg)"/><rect x="38" y="38" width="1004" height="${height - 76}" rx="22" fill="none" stroke="#c3a56a" stroke-opacity=".5"/>
    <text x="80" y="118" font-family="sans-serif" font-size="26" letter-spacing="5" fill="#e4c887">AUDIOLIBROS TEC</text>
    <text x="76" y="${Math.max(200, y - 20)}" font-family="serif" font-size="60" fill="#c3a56a">“</text>
    <text font-family="Georgia,DejaVu Serif,serif" font-size="${fontSize}" fill="#fff8e7">${lines.map((line, i) => `<tspan x="90" y="${y + 55 + i * fontSize * 1.34}">${escape(line)}</tspan>`).join('')}</text>
    <line x1="80" y1="${height - 365}" x2="1000" y2="${height - 365}" stroke="#a78e60" stroke-opacity=".5"/>
    <image x="80" y="${height - 320}" width="170" height="228" href="data:image/png;base64,${coverPNG.toString('base64')}"/>
    <text font-family="sans-serif" fill="#fff8e7" font-size="30">${linesFor(book.title, 28).slice(0, 2).map((s, i) => `<tspan x="280" y="${height - 270 + i * 40}">${escape(s)}</tspan>`).join('')}</text>
    <text x="280" y="${height - 160}" font-family="sans-serif" font-size="22" fill="#e4c887">${escape(book.author)}</text>
    <text x="280" y="${height - 110}" font-family="sans-serif" font-size="22" fill="#c3d2c4">Escanea y escucha este fragmento</text>
    <image x="820" y="${height - 300}" width="180" height="180" href="data:image/png;base64,${qr.toString('base64')}"/>
    <text x="80" y="${height - 58}" font-family="sans-serif" font-size="20" fill="#e4c887">totalelitecoaching.com/audiolibros</text>
  </svg>`;
  return sharp(Buffer.from(svg)).png().toBuffer();
}

// One bounded job at a time: the listener's normal audio stream is independent.
export function createClipRenderer({ media, getBucket, r2, cacheDir = path.join(os.tmpdir(), 'tec-share-clips'), binary = ffmpeg } = {}) {
  let active = false;
  const inFlight = new Map();
  async function trimCache() {
    await mkdir(cacheDir, { recursive: true });
    const entries = await readdir(cacheDir);
    const files = await Promise.all(entries.filter(n => /^[a-f0-9]{32}\.(mp3|mp4)$/.test(n)).map(async name => ({ name, ...(await stat(path.join(cacheDir, name))) })));
    let total = files.reduce((n, f) => n + f.size, 0);
    for (const f of files.sort((a, b) => a.mtimeMs - b.mtimeMs)) {
      if (Date.now() - f.mtimeMs > 3600_000 || total > 100 * 1024 * 1024) {
        await rm(path.join(cacheDir, f.name), { force: true }); total -= f.size;
      }
    }
  }
  return async function render(book, share, format, card) {
    const key = share._id + '.' + format;
    if (inFlight.has(key)) return inFlight.get(key);
    await trimCache();
    const cached = path.join(cacheDir, key);
    try { return await readFile(cached); } catch {}
    if (active) throw Object.assign(new Error('Se está preparando otro fragmento. Inténtalo en unos segundos.'), { status: 429 });
    active = true;
    const job = (async () => {
      const record = await findReadyAudiobook(book, media, r2);
      if (!record || !binary) throw new Error('El audio no está disponible.');
      const work = await mkdtemp(path.join(cacheDir, 'job-'));
      const proxy = express();
      const route = '/' + crypto.randomBytes(24).toString('hex');
      let transferred = 0;
      proxy.use((_req, res, next) => {
        const write = res.write;
        res.write = function(chunk, ...args) {
          transferred += chunk.length;
          if (transferred > 32 * 1024 * 1024) { this.destroy(); return false; }
          return write.call(this, chunk, ...args);
        };
        next();
      });
      // Never expose a signed R2 URL, session cookie, or full-book URL to the recipient.
      proxy.get(route, (req, res) => streamAudiobook(req, res, getBucket, book, record, r2).catch(() => res.destroy()));
      const server = await new Promise(resolve => { const s = proxy.listen(0, '127.0.0.1', () => resolve(s)); });
      try {
        const source = `http://127.0.0.1:${server.address().port}${route}`;
        const output = path.join(work, 'clip.' + format);
        const args = ['-nostdin', '-hide_banner', '-loglevel', 'error', '-filter_threads', '1', '-y'];
        if (format === 'mp4') {
          await writeFile(path.join(work, 'card.png'), card);
          args.push('-loop', '1', '-framerate', '10', '-i', path.join(work, 'card.png'));
        }
        args.push('-rw_timeout', '15000000', '-ss', String(share.start), '-i', source, '-t', String(share.end - share.start));
        if (format === 'mp4') args.push('-map', '0:v:0', '-map', '1:a:0', '-vf', 'scale=720:1280', '-c:v', 'libx264', '-preset', 'ultrafast', '-tune', 'stillimage', '-pix_fmt', 'yuv420p', '-c:a', 'aac', '-b:a', '96k', '-movflags', '+faststart');
        else args.push('-vn', '-c:a', 'libmp3lame', '-b:a', '96k');
        args.push('-threads', '1', '-map_metadata', '-1', '-metadata', 'title=' + book.title + ' · Audiolibros TEC', output);
        await new Promise((resolve, reject) => {
          const process = spawn(binary, args, { stdio: ['ignore', 'ignore', 'pipe'], timeout: 90000, killSignal: 'SIGKILL' });
          let diagnostics = '';
          process.stderr.on('data', data => { diagnostics = (diagnostics + data).slice(-2000); });
          process.once('error', reject);
          process.once('close', code => code === 0 ? resolve() : reject(new Error('No se pudo preparar el fragmento de audio.')));
        });
        const bytes = await readFile(output);
        if (!bytes.length || bytes.length > 12 * 1024 * 1024) throw new Error('El fragmento no se pudo preparar.');
        await writeFile(cached, bytes);
        return bytes;
      } finally {
        server.closeAllConnections(); await new Promise(resolve => server.close(resolve));
        await rm(work, { recursive: true, force: true });
      }
    })();
    inFlight.set(key, job);
    try { return await job; } finally { active = false; inFlight.delete(key); }
  };
}

export function registerAudiobookSharing(app, {
  shares, authorize, publicOrigin, media, getBucket, r2, catalog = audiobookCatalog,
  getTranscript = getAudiobookTranscript, renderClip = createClipRenderer({ media, getBucket, r2 })
}) {
  const origin = new URL(publicOrigin).origin;
  const byId = new Map(catalog.map(b => [b.id, b]));
  const attempts = new Map();
  const sendError = (res, status, message) => res.status(status).json({ error: message });
  const wrap = handler => async (req, res) => {
    try { await handler(req, res); }
    catch (error) { if (!res.headersSent) sendError(res, error.status || 503, error.status === 429 ? error.message : 'No se pudo preparar el fragmento. Inténtalo de nuevo.'); }
  };
  const publicURL = id => `${origin}/audiolibros/compartir/${id}`;
  async function lookup(req, res) {
    if (!idPattern.test(req.params.shareId)) { sendError(res, 404, 'No encontré este fragmento.'); return null; }
    const share = await shares.findById(req.params.shareId).lean();
    const book = share && byId.get(share.bookId);
    if (!book) { sendError(res, 404, 'No encontré este fragmento.'); return null; }
    return { share, book };
  }
  app.post('/api/audiolibros/compartir', wrap(async (req, res) => {
    res.set('Cache-Control', 'private, no-store');
    if (req.get('Sec-Fetch-Site') === 'cross-site' || (req.get('Origin') && req.get('Origin') !== origin)) return sendError(res, 403, 'Abre la biblioteca de TEC para compartir.');
    const now = Date.now();
    for (const [key, value] of attempts) if (value.until <= now) attempts.delete(key);
    const key = req.ip;
    const entry = attempts.get(key) || { count: 0, until: now + 3600_000 };
    if (++entry.count > 40 || attempts.size > 5000) return sendError(res, 429, 'Ya compartiste muchos fragmentos. Inténtalo más tarde.');
    attempts.set(key, entry);
    const book = byId.get(req.body?.bookId);
    let descriptor;
    try { descriptor = resolveShare(book, book && getTranscript(book.id), req.body); }
    catch (error) { return sendError(res, 400, error.message); }
    if (!await authorize(req, res, book, descriptor)) return;
    const share = { _id: crypto.randomBytes(16).toString('hex'), ...descriptor, createdAt: new Date() };
    await shares.create(share);
    const base = publicURL(share._id);
    res.status(201).json({ id: share._id, url: base, quote: share.quote, start: share.start, end: share.end,
      image: base + '/tarjeta.png', audio: share.kind === 'audio' ? base + '/audio.mp3' : null,
      video: share.kind === 'audio' ? base + '/video.mp4' : null });
  }));
  app.get('/audiolibros/compartir/:shareId', wrap(async (req, res) => {
    const found = await lookup(req, res); if (!found) return;
    const { book, share } = found;
    const url = publicURL(share._id);
    const back = `${origin}/audiolibros/?libro=${encodeURIComponent(book.id)}&t=${Math.floor(share.start)}`;
    const description = share.kind === 'audio' ? 'Escucha este fragmento de ' + book.title + ' en Audiolibros TEC.' : share.quote;
    res.set({ 'Cache-Control': 'public, max-age=300', 'Content-Type': 'text/html; charset=utf-8', 'Referrer-Policy': 'no-referrer' });
    res.send(`<!doctype html><html lang="es"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>${escape(book.title)} · Audiolibros TEC</title><meta name="description" content="${escape(description)}"><link rel="canonical" href="${url}"><meta property="og:type" content="article"><meta property="og:site_name" content="Audiolibros TEC"><meta property="og:title" content="${escape(book.title)} · ${escape(book.author)}"><meta property="og:description" content="${escape(description)}"><meta property="og:url" content="${url}"><meta property="og:image" content="${url}/tarjeta.png"><meta property="og:image:width" content="1080"><meta property="og:image:height" content="1350"><meta name="twitter:card" content="summary_large_image"><link rel="stylesheet" href="/audiolibros/compartir.css"></head><body class="shared-page"><main><a class="shared-brand" href="/audiolibros/">AUDIOLIBROS TEC</a><p class="share-eyebrow">${share.kind === 'audio' ? 'UN FRAGMENTO PARA ESCUCHAR' : 'UNA IDEA PARA COMPARTIR'}</p><blockquote>${escape(share.quote)}</blockquote><h1>${escape(book.title)}</h1><p>${escape(book.author)}</p>${share.kind === 'audio' ? `<audio controls preload="none" src="${url}/audio.mp3" aria-label="Fragmento de ${escape(book.title)}"></audio><p class="shared-hint">Si tarda unos segundos, estamos preparando el fragmento.</p>` : ''}<a class="share-primary" href="${back}">Seguir escuchando en TEC →</a><p class="shared-hint">Prueba gratis el 10 % inicial de cada libro. El catálogo completo está disponible con membresía.</p><a href="${url}/tarjeta.png" download="cita-tec.png">Guardar tarjeta</a></main></body></html>`);
  }));
  app.get('/audiolibros/compartir/:shareId/tarjeta.png', wrap(async (req, res) => {
    const found = await lookup(req, res); if (!found) return;
    const bytes = await quoteCard(found.book, found.share, publicURL(found.share._id), req.query.formato === 'story' ? 'story' : 'feed');
    res.set({ 'Cache-Control': 'public, max-age=86400', 'Content-Type': 'image/png', 'Content-Disposition': 'inline; filename="cita-tec.png"' }).send(bytes);
  }));
  for (const format of ['mp3', 'mp4']) app.get(`/audiolibros/compartir/:shareId/${format === 'mp3' ? 'audio.mp3' : 'video.mp4'}`, wrap(async (req, res) => {
    const found = await lookup(req, res); if (!found) return;
    if (found.share.kind !== 'audio') return sendError(res, 404, 'Este fragmento es una cita de texto.');
    const card = format === 'mp4' ? await quoteCard(found.book, found.share, publicURL(found.share._id), 'story') : null;
    const bytes = await renderClip(found.book, found.share, format, card);
    res.set({ 'Cache-Control': 'public, max-age=3600', 'Content-Type': format === 'mp3' ? 'audio/mpeg' : 'video/mp4', 'Accept-Ranges': 'bytes', 'Content-Disposition': `inline; filename="fragmento-tec.${format}"` });
    const range = req.get('Range');
    if (range) {
      const match = /^bytes=(\d+)-(\d*)$/.exec(range);
      const start = match ? Number(match[1]) : NaN, end = match?.[2] ? Math.min(Number(match[2]), bytes.length - 1) : bytes.length - 1;
      if (!Number.isSafeInteger(start) || start < 0 || start > end || start >= bytes.length) return res.status(416).set('Content-Range', `bytes */${bytes.length}`).end();
      return res.status(206).set('Content-Range', `bytes ${start}-${end}/${bytes.length}`).send(bytes.subarray(start, end + 1));
    }
    res.send(bytes);
  }));
}
