import test from 'node:test';
import assert from 'node:assert/strict';
import express from 'express';
import { audiobookCatalog, findReadyAudiobook, registerTecAudiobooks } from '../src/tec-audiobooks.js';
import { externalAudioReady, externalAudioDelivery } from '../src/tec-audio-external.js';
import { registerAudiobookSeo } from '../src/tec-audiobook-seo.js';

const book = audiobookCatalog.find(b => b.id === 'el-criterio');
test('external audio is pinned to a curated edition and never requires an R2/GridFS copy', async () => {
  const media = { findOne() { throw new Error('must not read GridFS'); } };
  const r2 = { ready() { throw new Error('must not read R2'); } };
  assert.deepEqual(await findReadyAudiobook(book, media, r2), { storage: 'internet-archive' });
  assert.equal(book.audio.sha256, undefined);
  for (const change of [{ id: 'other' }, { audio: { ...book.audio, url: book.audio.url + '?x=1' } }, { audio: { ...book.audio, url: 'https://evil.test/audio' } }]) {
    const invalid = { ...book, ...change };
    assert.equal(externalAudioReady(invalid), false);
    assert.equal(await findReadyAudiobook(invalid, media, r2), null);
  }
  assert.throws(() => externalAudioDelivery({ ...book.audio, url: 'https://evil.test/audio' }));
});

test('normal Coach routes authorize external audio, serve no media bytes and retire the free pilot', async t => {
  const app = express();
  const requireActive = async (req, res) => {
    if (!req.get('X-Test-Member')) { res.status(401).end(); return null; }
    if (req.get('X-Test-Member') === 'inactive') { res.status(403).end(); return null; }
    return { user: { _id: 'fixture' } };
  };
  registerTecAudiobooks(app, { requireActive, requireAdmin: requireActive, catalog: [book],
    media: { find: () => ({ lean: async () => [] }), findOne() { throw new Error('no GridFS'); } },
    getBucket() { throw new Error('no media proxy'); },
    reviews: { aggregate: async () => [] }, progress: { findOne: () => ({ lean: async () => null }) } });
  registerAudiobookSeo(app);
  const server = await new Promise(resolve => { const s = app.listen(0, '127.0.0.1', () => resolve(s)); });
  t.after(() => new Promise(resolve => { server.closeAllConnections(); server.close(resolve); }));
  const request = (path, options) => fetch(`http://127.0.0.1:${server.address().port}` + path, { redirect: 'manual', ...options });
  const route = '/api/coach/audiobooks/el-criterio';
  for (const suffix of ['', '/audio', '/audio?delivery=1', '/text']) {
    assert.equal((await request(route + suffix)).status, 401);
    assert.equal((await request(route + suffix, { headers: { 'X-Test-Member': 'inactive' } })).status, 403);
  }
  const headers = { 'X-Test-Member': 'active' };
  const response = await request(route + '/audio', { headers });
  assert.equal(response.status, 307); assert.equal(response.headers.get('location'), book.audio.url);
  assert.equal(await response.text(), '');
  const head = await request(route + '/audio', { method: 'HEAD', headers: { ...headers, Range: 'bytes=8-11' } });
  assert.equal(head.status, 206); assert.equal(head.headers.get('content-range'), `bytes 8-11/${book.audio.bytes}`);
  assert.equal(head.headers.get('content-length'), '4'); assert.equal(await head.text(), '');
  const badRange = await request(route + '/audio', { headers: { ...headers, Range: `bytes=${book.audio.bytes}-` } });
  assert.equal(badRange.status, 416);
  const detail = await request(route, { headers }).then(r => r.json());
  assert.equal(detail.book.chapters.length, 27); assert.equal(detail.book.transcriptUrl, null);
  assert.equal('audio' in detail.book, false);
  for (const path of ['/audiolibros/pruebas/el-criterio/', '/audiolibros/pruebas/el-criterio/index.html']) {
    const old = await request(path); assert.equal(old.status, 301);
    assert.equal(old.headers.get('location'), '/audiolibros/libros/el-criterio/');
  }
  for (const file of ['libro.json', 'reproductor.js']) assert.equal((await request('/audiolibros/pruebas/el-criterio/' + file)).status, 410);
});
