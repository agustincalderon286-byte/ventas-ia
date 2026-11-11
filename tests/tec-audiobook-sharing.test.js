import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import { mkdtemp, rm } from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import express from 'express';
import sharp from 'sharp';
import vm from 'node:vm';
import { audiobookCatalog, getAudiobookTranscript } from '../src/tec-audiobooks.js';
import { SHARE_LIMITS, resolveShare, quoteCard, registerAudiobookSharing, createClipRenderer } from '../src/tec-audiobook-sharing.js';

const book = audiobookCatalog[0];
const transcript = getAudiobookTranscript(book.id);
const origin = 'https://totalelitecoaching.com';

test('the share selector builds complete adjacent passages and validates clock input without changing the reading clock', () => {
  const scope = { window: {} };
  vm.runInNewContext(fs.readFileSync(new URL('../public/audiolibros/compartir.js', import.meta.url), 'utf8'), scope);
  const helpers = scope.window.TecAudioSharing;
  assert.equal(helpers.seconds('01:02:15'), 3735);
  assert.equal(helpers.seconds('02:15'), 135);
  for (const value of ['1:99', '-01:02', '12:34:99', 'abc', '']) assert.ok(Number.isNaN(helpers.seconds(value)));
  assert.equal(helpers.clock(3735), '1:02:15');
  const cues = [{ text: 'Una frase completa.' }, { text: 'Esta frase ' }, { text: 'tiene dos partes.' }, { text: 'La siguiente frase.' }];
  const range = helpers.passageRange(cues, 2);
  assert.equal(range.first, 1); assert.equal(range.last, 2);
  const long = helpers.passageRange(Array.from({ length: 20 }, () => ({ text: 'x'.repeat(150) + ' ' })), 8);
  assert.ok(long.last - long.first < 6);
  assert.ok(helpers.textFor(Array.from({ length: 20 }, () => ({ text: 'x'.repeat(150) + ' ' })).slice(long.first, long.last + 1)).length <= 600);
});

test('sharing derives canonical passages and rejects arbitrary text, unbounded indices and audio extraction', () => {
  const quote = resolveShare(book, transcript, { kind: 'quote', first: 5, last: 6, quote: 'Invented marketing text' });
  assert.equal(quote.quote, transcript.cues.slice(5, 7).map(c => c.text).join('').replace(/\s+/g, ' ').trim());
  assert.equal(quote.start, transcript.cues[5].start);
  for (const input of [
    { kind: 'quote', first: -1, last: 0 }, { kind: 'quote', first: 0, last: 6 },
    { kind: 'quote', first: 999999, last: 999999 }, { kind: 'quote', first: '0', last: 0 },
    { kind: 'audio', start: 0, end: 46 }, { kind: 'audio', start: -1, end: 20 },
    { kind: 'audio', start: 0, end: 2 }, { kind: 'audio', start: 0, end: Infinity },
    { kind: 'audio', start: 1, end: book.duration + 1 }, { kind: 'arbitrary' }
  ]) assert.throws(() => resolveShare(book, transcript, input));
  assert.equal(SHARE_LIMITS.seconds, 45);
  assert.throws(() => resolveShare(book, { cues: [{ start: 0, end: 4, text: 'a'.repeat(601) }] }, { kind: 'quote', first: 0, last: 0 }));
});

test('quote cards render real PNGs in feed and story sizes with long Spanish text', async () => {
  const share = { quote: 'El pensamiento y la perseverancia acompañan cada nuevo camino. '.repeat(9).trim() };
  for (const [format, height] of [['feed', 1350], ['story', 1920]]) {
    const png = await quoteCard(book, share, origin + '/audiolibros/compartir/' + 'a'.repeat(32), format);
    const info = await sharp(png).metadata();
    assert.equal(info.format, 'png'); assert.equal(info.width, 1080); assert.equal(info.height, height);
    assert.ok(png.length > 10000);
  }
});

test('public share pages and media expose only approved excerpts, retain canonical attribution and handle ranges', async t => {
  const rows = new Map(); let rendered = 0;
  const app = express(); app.use(express.json());
  registerAudiobookSharing(app, {
    publicOrigin: origin, catalog: [book], getTranscript: () => transcript,
    shares: { async create(row) { rows.set(row._id, row); }, findById(id) { return { async lean() { return rows.get(id); } }; } },
    authorize: async (req, res, _book, clip) => {
      if (clip.end <= 50 || req.get('X-Test-Member') === 'yes') return true;
      res.status(401).json({ error: 'Ingresa para compartir este fragmento.' }); return false;
    },
    renderClip: async () => { rendered++; return Buffer.from('0123456789'); }
  });
  const server = await new Promise(resolve => { const s = app.listen(0, '127.0.0.1', () => resolve(s)); });
  t.after(() => new Promise(resolve => { server.closeAllConnections(); server.close(resolve); }));
  const base = 'http://127.0.0.1:' + server.address().port;
  const create = (body, headers = {}) => fetch(base + '/api/audiolibros/compartir', {
    method: 'POST', headers: { Origin: origin, 'Content-Type': 'application/json', ...headers }, body: JSON.stringify(body)
  });
  assert.equal((await create({ bookId: book.id, kind: 'audio', start: 0, end: 5 }, { Origin: 'https://evil.test' })).status, 403);
  assert.equal((await create({ bookId: book.id, kind: 'audio', start: 100, end: 105 })).status, 401);
  const created = await create({ bookId: book.id, kind: 'quote', first: 2, last: 3 });
  assert.equal(created.status, 201); const shared = await created.json();
  assert.match(shared.id, /^[a-f0-9]{32}$/); assert.equal(shared.audio, null);
  const stored = rows.get(shared.id);
  for (const privateField of ['email', 'userId', 'stripeCustomerId', 'audioUrl', 'r2Key', 'fileId']) assert.equal(privateField in stored, false);
  const page = await fetch(base + new URL(shared.url).pathname).then(r => r.text());
  assert.ok(page.includes('og:image')); assert.ok(page.includes('og:description')); assert.ok(page.includes(book.author));
  assert.ok(page.includes('?libro=' + book.id + '&t=14'));
  assert.ok(!page.includes('r2.cloudflarestorage.com')); assert.ok(!page.includes('/libros/' + book.id + '/audio'));
  const image = await fetch(base + new URL(shared.image).pathname);
  assert.equal(image.headers.get('content-type'), 'image/png');
  assert.equal((await fetch(base + '/audiolibros/compartir/invalid')).status, 404);
  assert.equal((await fetch(base + new URL(shared.url).pathname + '/audio.mp3')).status, 404);
  const audioShare = await create({ bookId: book.id, kind: 'audio', start: 100, end: 120 }, { 'X-Test-Member': 'yes' }).then(r => r.json());
  for (const file of ['audio.mp3', 'video.mp4']) {
    const response = await fetch(base + new URL(audioShare.url).pathname + '/' + file, { headers: { Range: 'bytes=2-5' } });
    assert.equal(response.status, 206); assert.equal(response.headers.get('content-range'), 'bytes 2-5/10');
    assert.equal(await response.text(), '2345');
  }
  assert.equal((await fetch(base + new URL(audioShare.audio).pathname, { headers: { Range: 'bytes=999-' } })).status, 416);
  assert.ok(rendered >= 2);
});

test('real FFmpeg extracts a bounded MP3 and vertical H264 video using range requests without copying the book', async t => {
  const sample = new URL('../public/audiolibros/muestras/como-un-hombre-piensa.m4a', import.meta.url);
  const bytes = fs.statSync(sample).size;
  const fixtureBook = { ...book, audio: { ...book.audio, r2Key: undefined, r2ETag: undefined, bytes } };
  const cacheDir = await mkdtemp(path.join(os.tmpdir(), 'tec-sharing-test-'));
  t.after(() => rm(cacheDir, { recursive: true, force: true }));
  let transferred = 0;
  const renderer = createClipRenderer({ cacheDir,
    media: { findOne() { return { async lean() { return { fileId: 'sample' }; } }; } },
    getBucket: () => ({
      find() { return { limit() { return { async toArray() { return [{ length: bytes }]; } }; } }; },
      openDownloadStream(_id, range = {}) {
        const stream = fs.createReadStream(sample, { ...(range.start !== undefined ? { start: range.start } : {}), ...(range.end !== undefined ? { end: range.end - 1 } : {}) });
        stream.on('data', data => { transferred += data.length; }); return stream;
      }
    })
  });
  const clip = { _id: 'b'.repeat(32), start: 90, end: 94, quote: 'Como un hombre piensa.' };
  const mp3 = await renderer(fixtureBook, clip, 'mp3');
  assert.ok(mp3.length > 20000 && mp3.length < 150000);
  assert.deepEqual(await renderer(fixtureBook, clip, 'mp3'), mp3);
  const card = await quoteCard(book, clip, origin + '/audiolibros/compartir/' + clip._id, 'story');
  const mp4 = await renderer(fixtureBook, clip, 'mp4', card);
  assert.equal(mp4.toString('ascii', 4, 8), 'ftyp'); assert.ok(mp4.length > 30000);
  assert.ok(transferred < bytes * 2, 'Seeking must avoid downloading the full book for each clip');
});
