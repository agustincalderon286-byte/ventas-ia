import test from 'node:test';
import assert from 'node:assert/strict';
import express from 'express';
import crypto from 'node:crypto';
import fs from 'node:fs';
import { Readable, Writable } from 'node:stream';
import { registerTecAudiobooks, normalizeAudiobookReview, validImportGrant, audiobookCatalog, createAudiobookModels, getAudiobookTranscript } from '../src/tec-audiobooks.js';
import vm from 'node:vm';
import mongoose from 'mongoose';

function memoryModel() {
  const rows = new Map();
  const match = (row, query) => Object.entries(query).every(([key, value]) => {
    if (key === '$or') return value.some(q => match(row, q));
    if (value && typeof value === 'object' && !(value instanceof Date)) {
      if ('$ne' in value) return row[key] !== value.$ne;
      if ('$lt' in value) return row[key] < value.$lt;
      if ('$exists' in value) return (key in row) === value.$exists;
      if ('$in' in value) return value.$in.includes(row[key]);
    }
    return String(row[key]) === String(value);
  });
  const query = get => ({ sort(order) { this.order = order; return this; }, skip(n) { this.offset = n; return this; }, limit(n) { this.count = n; return this; }, async lean() {
    let result = get();
    if (!Array.isArray(result)) return result ? { ...result } : null;
    if (this.order) result.sort((a, b) => b.updatedAt - a.updatedAt || String(a._id).localeCompare(String(b._id)));
    return result.slice(this.offset || 0, (this.offset || 0) + (this.count ?? Infinity)).map(r => ({ ...r }));
  }, then(resolve, reject) { return this.lean().then(resolve, reject); } });
  function update(filter, value, opts = {}) {
    let row = [...rows.values()].find(r => match(r, filter));
    if (!row && opts.upsert) {
      if (rows.has(filter._id)) throw Object.assign(new Error('duplicate'), { code: 11000 });
      row = { _id: filter._id, createdAt: new Date() }; rows.set(row._id, row);
    }
    if (!row) return null;
    Object.assign(row, value.$set, { updatedAt: new Date() });
    for (const key of Object.keys(value.$unset || {})) delete row[key];
    return row;
  }
  return {
    rows, find: q => query(() => [...rows.values()].filter(r => match(r, q))), findOne: q => query(() => [...rows.values()].find(r => match(r, q))),
    findOneAndUpdate(q, value, opts) { return query(() => update(q, value, opts)); },
    async updateOne(q, value, opts) { update(q, value, opts); }, async deleteOne(q) { for (const row of rows.values()) if (match(row, q)) rows.delete(row._id); },
    async aggregate(stages) { const records = [...rows.values()].filter(r => match(r, stages[0].$match)); return records.length ? [{ count: records.length, average: records.reduce((n, r) => n + r.rating, 0) / records.length }] : []; }
  };
}
async function fixture(r2) {
  const media = memoryModel(), reviews = memoryModel(), progress = memoryModel(), files = new Map();
  const bytes = Buffer.from('0000ftypM4B audiolibro fixture bytes only');
  const sourceBook = r2 ? audiobookCatalog.find(b => b.audio.r2Key) : audiobookCatalog[0];
  const book = { ...sourceBook, audio: { ...sourceBook.audio, bytes: bytes.length, sha256: crypto.createHash('sha256').update(bytes).digest('hex') } };
  let sequence = 0, downloads = 0;
  const bucket = {
    find(q) { return { limit() { return this; }, async toArray() { return [...files.values()].filter(f => f._id === q._id && f.metadata.bookId === q['metadata.bookId'] && f.metadata.sha256 === q['metadata.sha256']); } }; },
    openUploadStream(name, { metadata }) {
      const id = `file-${++sequence}`, chunks = [];
      const stream = new Writable({ write(c, _enc, done) { chunks.push(c); done(); }, final(done) { const data = Buffer.concat(chunks); files.set(id, { _id: id, metadata, bytes: data, length: data.length }); done(); } });
      stream.id = id; stream.abort = async () => files.delete(id); return stream;
    },
    openDownloadStream(id, range = {}) { downloads++; return Readable.from(files.get(id).bytes.subarray(range.start || 0, range.end)); },
    async delete(id) { files.delete(id); }
  };
  const token = 'T'.repeat(64), grant = { bookId: book.id, tokenHash: crypto.createHash('sha256').update(token).digest('hex'), expiresAt: new Date(Date.now() + 60000).toISOString() };
  const auth = async (req, res, admin = false) => {
    const role = req.get('X-Test-User');
    if (!role) { res.status(401).json({ error: 'login' }); return null; }
    if (role === 'inactive' || (admin && role !== 'admin')) { res.status(403).json({ error: 'forbidden' }); return null; }
    return { user: { _id: role, name: role === 'a' ? 'Ana' : 'Bruno', email: 'never-expose@example.test' } };
  };
  const app = express(); app.use(express.json());
  registerTecAudiobooks(app, { requireActive: auth, requireAdmin: (r, s) => auth(r, s, true), media, reviews, progress, getBucket: () => bucket, catalog: [book], importGrant: grant, r2 });
  const server = await new Promise(resolve => { const instance = app.listen(0, '127.0.0.1', () => resolve(instance)); });
  const base = `http://127.0.0.1:${server.address().port}`;
  const path = `/api/coach/audiobooks/${book.id}`;
  const request = (url, { user = 'a', body, ...options } = {}) => fetch(base + url, { ...options, headers: { ...(user ? { 'X-Test-User': user } : {}), ...(body !== undefined ? { 'Content-Type': 'application/json' } : {}), ...options.headers }, ...(body !== undefined ? { body: JSON.stringify(body) } : {}) });
  const install = (data = bytes, headers = {}) => fetch(`${base}/api/control/audiobooks/${book.id}/import`, { method: 'POST', headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'audio/mp4', ...headers }, body: data });
  return { base, path, book, request, install, media, reviews, progress, files, bytes, grant, token, downloads: () => downloads, close: () => new Promise(resolve => { server.closeAllConnections?.(); server.close(resolve); }) };
}

test('catalog manifest has all nine parts, monotonic chapter starts and the exact voice-only BF16 asset', () => {
  const [book] = audiobookCatalog;
  assert.equal(book.chapters.length, 9); assert.equal(book.duration, 2891.01); assert.equal(book.audio.bytes, 70489653);
  assert.equal(book.audio.sha256, 'd614e63ebc3d519e98a2ed55574a63da6a30d5870821bc425bc5d872ab8905b8');
  assert.match(book.credits, /Edición en español/);
  let cursor = 0; for (const c of book.chapters) { assert.ok(Math.abs(c.start - cursor) < 0.02); cursor += c.duration; }
  assert.ok(Math.abs(cursor - book.duration) < .02);
  const models = createAudiobookModels(mongoose);
  assert.equal(models.reviews.schema.path('_id').instance, 'String'); // canonical ids enforce account/book uniqueness without relying on a later index build
});

test('complete read-along texts keep canonical hashes and ordered, bounded audio cues for both books', () => {
  const expected = {
    'como-un-hombre-piensa': '2fbf8ae9856e53b7992887d06e089712d3060f483bb60fb480edbd37a396ef07',
    'acres-de-diamantes': '0e9f5dc1d4221ac8f9ef6035f4608edfae7779a7fcfeda5a06563598e268b647'
  };
  for (const id of Object.keys(expected)) {
    const book = audiobookCatalog.find(b => b.id === id);
    const text = getAudiobookTranscript(book.id);
    assert.ok(text.cues.length > 100);
    const master = text.cues.map(c => c.separator + c.text).join('') + text.trailingSeparator;
    assert.equal(crypto.createHash('sha256').update(master).digest('hex'), text.textSha256);
    assert.equal(text.textSha256, expected[book.id]);
    let end = 0;
    for (const cue of text.cues) {
      assert.ok(cue.start >= end - .002 && cue.end > cue.start && cue.end <= book.duration);
      assert.ok(cue.chapter >= 0 && cue.chapter < book.chapters.length);
      end = cue.end;
    }
    assert.doesNotMatch(JSON.stringify(book), /inteligencia artificial|\bBF16\b|asistida por IA|pendiente|checkpoints/i);
  }
  const acres = audiobookCatalog.find(b => b.id === 'acres-de-diamantes');
  assert.equal(acres.duration, 5495.3); assert.equal(acres.chapters.length, 8);
  assert.equal(acres.audio.sha256, 'c6e0e8ddcd653e3e4632df91c9e028a98a12e2d4b49ea7f94d6b09d6f6e73e7e');
  assert.equal(getAudiobookTranscript('../server'), null);
});

test('private R2 audio is listed after verification and streams exact chapter seek ranges for active accounts', async () => {
  let verified = false;
  let reads = 0;
  const f = await fixture({
    ready: async () => verified,
    open: async (_audio, range) => {
      reads++;
      return Readable.from(f.bytes.subarray(range.start, range.end + 1));
    }
  });
  try {
    const listing = () => f.request('/api/coach/audiobooks').then(r => r.json());
    assert.equal((await listing()).books[0].available, false);
    assert.equal((await f.request(f.path + '/audio')).status, 404);
    verified = true;
    assert.equal((await listing()).books[0].available, true);
    assert.equal((await f.request(f.path + '/audio', { user: null })).status, 401);
    assert.equal((await f.request(f.path + '/audio', { user: 'inactive' })).status, 403);
    assert.equal((await f.request(f.path + '/audio', { method: 'HEAD' })).status, 200);
    assert.equal(reads, 0);
    const response = await f.request(f.path + '/audio', { headers: { Range: 'bytes=-4' } });
    assert.equal(response.status, 206);
    assert.equal(response.headers.get('content-type'), 'audio/mp4');
    assert.equal(response.headers.get('content-range'), `bytes ${f.bytes.length - 4}-${f.bytes.length - 1}/${f.bytes.length}`);
    assert.deepEqual(Buffer.from(await response.arrayBuffer()), f.bytes.subarray(-4));
    assert.equal(reads, 1);
    assert.equal(f.downloads(), 0);
  } finally { await f.close(); }
});

test('read-along endpoint requires an active account and an installed book', async () => {
  const f = await fixture();
  try {
    assert.equal((await f.request(f.path + '/text', { user: null })).status, 401);
    assert.equal((await f.request(f.path + '/text', { user: 'inactive' })).status, 403);
    assert.equal((await f.request(f.path + '/text')).status, 404);
    await f.install();
    const response = await f.request(f.path + '/text');
    assert.equal(response.status, 200); assert.equal(response.headers.get('cache-control'), 'private, no-store');
    const data = await response.json(); assert.equal(data.bookId, f.book.id); assert.ok(data.cues.length > 100);
    assert.equal('audio' in data, false);
    const detail = await (await f.request(f.path)).json(); assert.equal(detail.book.transcriptUrl, f.path + '/text');
  } finally { await f.close(); }
});

test('reader follows the media clock after seeking, at boundaries, during silence and at any playback speed', () => {
  const scope = {}; vm.runInNewContext(fs.readFileSync(new URL('../public/coach/audiobook-reader.js', import.meta.url), 'utf8'), scope);
  const find = scope.TecBookReader.cueIndexAt;
  const cues = [{ start: 2, end: 5 }, { start: 5, end: 9 }, { start: 12, end: 16 }];
  for (const [time, expected] of [[0,-1],[2,0],[4.99,0],[5,1],[9,-1],[11,-1],[12,2],[16,-1],[100,-1],[NaN,-1]]) assert.equal(find(cues, time), expected);
  assert.equal(find(cues, 3), 0); // jumping backwards must not retain a stale forward cursor
  assert.equal(find([], 3), -1);
});

test('import permission is pinned to a book, hash, expiry and a nonempty strong secret', () => {
  const token = 'x'.repeat(64), req = { get: () => `Bearer ${token}` };
  const grant = { bookId: 'book', expiresAt: new Date(Date.now() + 60000).toISOString(), tokenHash: crypto.createHash('sha256').update(token).digest('hex') };
  assert.equal(validImportGrant(req, grant, 'book'), true);
  for (const invalid of [null, { ...grant, bookId: 'other' }, { ...grant, expiresAt: 'invalid' }, { ...grant, expiresAt: '2020-01-01' }, { ...grant, tokenHash: 'invalid' }]) assert.equal(validImportGrant(req, invalid, 'book'), false);
  assert.equal(validImportGrant({ get: () => 'Bearer wrong' }, grant, 'book'), false);
});

test('book installation verifies bytes, is single use and never exposes audio without active TEC authentication', async () => {
  const f = await fixture();
  try {
    assert.equal((await f.request('/api/coach/audiobooks', { user: null })).status, 401);
    assert.equal((await f.request('/api/coach/audiobooks', { user: 'inactive' })).status, 403);
    assert.equal((await (await f.request('/api/coach/audiobooks')).json()).books[0].available, false);
    assert.equal((await f.request(f.path + '/audio')).status, 404);
    assert.equal((await f.install(f.bytes, { Authorization: 'Bearer invalid' })).status, 401);
    assert.equal((await f.install(f.bytes, { 'Content-Type': 'text/plain' })).status, 415);
    assert.equal((await f.install(Buffer.from('bad-size'))).status, 400);
    const corrupt = Buffer.from(f.bytes); corrupt[20] ^= 1;
    assert.equal((await f.install(corrupt)).status, 422); assert.equal(f.files.size, 0);
    assert.equal((await f.install()).status, 201); assert.equal(f.files.size, 1);
    assert.equal((await f.install()).status, 409); assert.equal(f.files.size, 1);
    const catalog = await (await f.request('/api/coach/audiobooks')).json();
    assert.equal(catalog.books[0].available, true); assert.equal('audio' in catalog.books[0], false);
    assert.equal((await f.request(f.path + '/audio', { user: null })).status, 401);
    assert.equal((await f.request(f.path + '/audio', { user: 'inactive' })).status, 403);
    const head = await f.request(f.path + '/audio', { method: 'HEAD' });
    assert.equal(head.status, 200); assert.equal(Number(head.headers.get('content-length')), f.bytes.length); assert.equal(f.downloads(), 0);
    const all = await f.request(f.path + '/audio'); assert.equal(all.headers.get('cache-control'), 'private, no-store'); assert.deepEqual(Buffer.from(await all.arrayBuffer()), f.bytes);
    for (const [range, expected] of [['bytes=0-3', f.bytes.subarray(0, 4)], ['bytes=8-', f.bytes.subarray(8)], ['bytes=-4', f.bytes.subarray(-4)]]) {
      const r = await f.request(f.path + '/audio', { headers: { Range: range } }); assert.equal(r.status, 206); assert.equal(r.headers.get('content-type'), 'audio/mp4'); assert.deepEqual(Buffer.from(await r.arrayBuffer()), expected);
    }
    for (const range of ['bytes=-0', 'bytes=1000-', 'bytes=8-3', 'bytes=1-2,4-5']) assert.equal((await f.request(f.path + '/audio', { headers: { Range: range } })).status, 416);
  } finally { await f.close(); }
});

test('reviews are real authenticated account opinions: one per user, scoped edits/deletion, escaped UI, validation and pagination', async () => {
  const f = await fixture();
  try {
    await f.install();
    for (const rating of [0, 6, 1.5, '5', null]) assert.equal((await f.request(f.path + '/review', { method: 'PUT', body: { rating } })).status, 400);
    assert.equal((await f.request(f.path + '/review', { method: 'PUT', body: { rating: 4, text: 'x'.repeat(2001) } })).status, 400);
    assert.equal((await f.request(f.path + '/review', { method: 'PUT', body: { rating: 4 }, headers: { 'Sec-Fetch-Site': 'cross-site' } })).status, 403);
    assert.equal((await f.request(f.path + '/review', { method: 'PUT', body: { rating: 4 }, headers: { Origin: 'https://other.example' } })).status, 403);
    const posted = await f.request(f.path + '/review', { method: 'PUT', body: { rating: 5, text: '<img src=x onerror=alert(1)>', userId: 'b', author: 'Forged', _id: `${f.book.id}:b` } });
    assert.equal(posted.status, 200); const data = await posted.json(); assert.equal(data.review.author, 'Ana'); assert.equal(data.review.mine, true); assert.equal(f.reviews.rows.size, 1);
    assert.equal(f.reviews.rows.get(`${f.book.id}:a`).userId, 'a');
    await Promise.all([3, 4, 5].map(rating => f.request(f.path + '/review', { method: 'PUT', body: { rating, text: 'Updated' } })));
    assert.equal(f.reviews.rows.size, 1);
    await f.request(f.path + '/review', { user: 'b', method: 'PUT', body: { rating: 3, text: 'Otra opinión', userId: 'a' } });
    assert.equal(f.reviews.rows.size, 2);
    const list = await (await f.request(f.path + '/reviews')).json(); assert.equal(list.rating.count, 2); assert.equal(list.mine.author, 'Ana'); assert.equal(list.reviews.filter(r => r.mine).length, 1);
    assert.equal(JSON.stringify(list).includes('never-expose'), false); assert.equal(JSON.stringify(list).includes('userId'), false);
    await f.request(f.path + '/review', { method: 'DELETE', body: { userId: 'b' } });
    assert.equal(f.reviews.rows.size, 1); assert.ok(f.reviews.rows.has(`${f.book.id}:b`));
    assert.equal((await (await f.request(f.path + '/reviews')).json()).mine, null);
    for (let i = 0; i < 22; i++) await f.request(f.path + '/review', { user: `reader-${i}`, method: 'PUT', body: { rating: 4 } });
    const page1 = await (await f.request(f.path + '/reviews')).json(); assert.equal(page1.reviews.length, 20); assert.equal(page1.hasMore, true);
    const page2 = await (await f.request(f.path + '/reviews?page=2')).json(); assert.equal(page2.reviews.length, 3); assert.equal(page2.hasMore, false);
    const script = fs.readFileSync(new URL('../public/coach/audiobooks.js', import.meta.url), 'utf8');
    assert.doesNotMatch(script, /innerHTML|insertAdjacentHTML/); // all account-supplied reviews and author names enter textContent
    assert.throws(() => normalizeAudiobookReview({ rating: 0 }));
  } finally { await f.close(); }
});

test('listening position and speed are private per account; invalid positions and cross-site writes fail', async () => {
  const f = await fixture();
  try {
    await f.install();
    assert.equal((await f.request(f.path + '/progress', { method: 'PUT', body: { position: 25, speed: 1.25, userId: 'b' } })).status, 200);
    assert.deepEqual((await (await f.request(f.path)).json()).progress, { position: 25, speed: 1.25 });
    assert.deepEqual((await (await f.request(f.path, { user: 'b' })).json()).progress, { position: 0, speed: 1 });
    for (const body of [{ position: -1, speed: 1 }, { position: 99999, speed: 1 }, { position: 1, speed: 100 }, { position: '20', speed: 1 }]) assert.equal((await f.request(f.path + '/progress', { method: 'PUT', body })).status, 400);
    assert.equal((await f.request(f.path + '/progress', { method: 'PUT', body: { position: 2, speed: 1 }, headers: { Origin: 'null' } })).status, 403);
    assert.equal(f.progress.rows.size, 1); assert.equal(f.progress.rows.values().next().value.userId, 'a');
  } finally { await f.close(); }
});
