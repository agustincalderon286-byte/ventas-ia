import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import vm from 'node:vm';
import { createAudiobookR2, AUDIO_LINK_SECONDS } from '../src/tec-audiobook-r2.js';
import { audiobookCatalog } from '../src/tec-audiobooks.js';

const code = fs.readFileSync(new URL('../public/audiolibros/audio-delivery.js', import.meta.url), 'utf8');
const host = 'a'.repeat(32) + '.r2.cloudflarestorage.com';
class Audio extends EventTarget {
  constructor() { super(); this.attrs = {}; this.currentTime = 0; this.duration = 10000; this.playbackRate = 1; this.paused = true; }
  set src(value) { this.attrs.src = value; } get src() { return this.attrs.src; }
  getAttribute(name) { return this.attrs[name] || null; } removeAttribute(name) { delete this.attrs[name]; }
  load() {} pause() { this.paused = true; this.dispatchEvent(new Event('pause')); }
  async play() { this.paused = false; this.dispatchEvent(new Event('play')); }
  metadata() { this.currentTime = 0; this.dispatchEvent(new Event('loadedmetadata')); }
}
function fixture(fetchOverride) {
  let clock = 1000000, calls = 0; const failures = [], requests = [];
  const scope = { window: {}, navigator: { onLine: true }, location: new URL('https://totalelitecoaching.com/audiolibros/'), URL, AbortController, setTimeout, clearTimeout, fetch: () => {} };
  vm.runInNewContext(code, scope);
  const audio = new Audio();
  const fetchImpl = async (url, options) => { calls++; requests.push({url, options});
    return fetchOverride ? fetchOverride(url, options) : { ok: true, json: async () => ({ url: `https://${host}/tec-audiolibros/book?lease=${calls}`, expiresAt: clock + 300000 }) }; };
  const controller = new scope.window.TecAudioDelivery.Controller(audio, { fetchImpl, now: () => clock, onFailure: text => failures.push(text) });
  return { audio, controller, failures, requests, calls: () => calls, advance: seconds => { clock += seconds * 1000; } };
}
const flush = () => new Promise(resolve => setImmediate(resolve));

test('all published full books and separate clipped samples use private R2 keys', () => {
  const samples = JSON.parse(fs.readFileSync(new URL('../src/knowledge/tec-audio-previews.json', import.meta.url)));
  for (const book of audiobookCatalog) {
    if (book.id === 'el-criterio') {
      assert.equal(book.audio.provider, 'internet-archive');
      assert.equal(samples[book.id], undefined);
      continue;
    }
    assert.ok(book.audio.r2Key); assert.ok(book.audio.r2ETag);
    const sample = samples[book.id]; assert.ok(sample.r2Key); assert.ok(sample.r2ETag);
    assert.notEqual(sample.r2Key, book.audio.r2Key); assert.ok(sample.bytes < book.audio.bytes);
  }
});
test('signed audio links expire in five minutes and support browser-chosen range requests', async () => {
  const r2 = createAudiobookR2({ TEC_AUDIO_R2_ACCOUNT_ID: 'a'.repeat(32), TEC_AUDIO_R2_ACCESS_KEY_ID: 'test-key', TEC_AUDIO_R2_SECRET_ACCESS_KEY: 'test-secret' });
  const before = Date.now(); const lease = await r2.delivery(audiobookCatalog[0].audio); const url = new URL(lease.url);
  assert.equal(url.hostname, host); assert.equal(url.searchParams.get('X-Amz-Expires'), String(AUDIO_LINK_SECONDS));
  assert.equal(url.searchParams.get('X-Amz-SignedHeaders'), 'host');
  assert.equal(url.searchParams.get('response-cache-control'), 'private, no-store');
  assert.ok(lease.expiresAt >= before + 300000 && lease.expiresAt <= Date.now() + 300000);
});
test('expired playback renews directly from R2 and restores time, speed and playback intent', async () => {
  const f = fixture(); await f.controller.load('/api/audiolibros/libros/book/audio', { position: 123, rate: 1.5 }); f.audio.metadata();
  assert.equal(f.audio.currentTime, 123); assert.equal(f.audio.playbackRate, 1.5);
  f.controller.play(); await flush(); f.audio.currentTime = 230; f.audio.dispatchEvent(new Event('timeupdate')); f.controller.pause();
  f.advance(301); f.controller.play(); await flush(); f.audio.metadata(); await flush();
  assert.equal(f.calls(), 2); assert.equal(f.audio.currentTime, 230); assert.equal(f.audio.playbackRate, 1.5); assert.equal(f.audio.paused, false);
  assert.equal(new URL(f.audio.src).hostname, host);
  assert.equal(new URL(f.requests[0].url).searchParams.get('delivery'), '1'); assert.equal(f.requests[0].options.credentials, 'same-origin');
  f.advance(301); f.controller.seek(550, true); await flush(); f.audio.metadata();
  assert.equal(f.audio.currentTime, 550); assert.equal(f.calls(), 3); assert.equal(f.failures.length, 0);
});
test('renewal rejects inactive membership and never switches audio to an unauthorized response', async () => {
  const f = fixture(async () => ({ ok: false, status: 403 }));
  await assert.rejects(f.controller.load('/api/audiolibros/libros/book/audio'), /membresía activa/);
  assert.equal(f.audio.getAttribute('src'), null);
});
test('switching books discards stale link responses and rejects foreign delivery targets', async () => {
  const pending = [];
  const f = fixture(() => new Promise(resolve => pending.push(resolve)));
  const first = f.controller.load('/first'); const second = f.controller.load('/second');
  pending[1]({ ok: true, json: async () => ({ url: `https://${host}/tec-audiolibros/second`, expiresAt: 1300000 }) }); await second;
  pending[0]({ ok: true, json: async () => ({ url: `https://${host}/tec-audiolibros/first`, expiresAt: 1300000 }) }); await first;
  assert.match(f.audio.src, /second$/); f.controller.stop(); assert.equal(f.audio.getAttribute('src'), null);
  const bad = fixture(async () => ({ ok: true, json: async () => ({ url: 'https://other.test/book', expiresAt: 1300000 }) }));
  await assert.rejects(bad.controller.load('/audio')); assert.equal(bad.audio.getAttribute('src'), null);
});

test('browser fetch keeps the Window receiver required by native Chrome fetch', async () => {
  const scope = { window: {}, navigator: { onLine: true }, location: new URL('https://totalelitecoaching.com/audiolibros/'), URL, AbortController, setTimeout, clearTimeout };
  scope.fetch = function () { assert.equal(this, scope.window); return Promise.resolve({ ok: true, json: async () => ({ url: `https://${host}/tec-audiolibros/book`, expiresAt: Date.now() + 300000 }) }); };
  vm.runInNewContext(code, scope);
  const audio = new Audio(); const controller = new scope.window.TecAudioDelivery.Controller(audio);
  await controller.load('/audio'); assert.equal(new URL(audio.src).hostname, host);
});

const archiveURL = 'https://archive.org/download/criterio_2408_librivox/Criterio_librivox.m4b';
test('curated external narration keeps the normal player, chapter seeking and membership refresh', async () => {
  const f = fixture(async () => ({ ok: true, json: async () => ({ provider: 'internet-archive', url: archiveURL, expiresAt: 1300000 }) }));
  f.audio.duration = 30859;
  await f.controller.load('/api/audiolibros/libros/el-criterio/audio', { external: true, position: 12500, rate: 1.25 });
  f.audio.metadata(); assert.equal(f.audio.src, archiveURL); assert.equal(f.audio.currentTime, 12500);
  assert.equal(f.audio.playbackRate, 1.25); assert.equal(f.audio.getAttribute('crossorigin'), null);
  f.controller.seek(600, true); await flush(); assert.equal(f.audio.currentTime, 600); assert.equal(f.audio.paused, false);
  assert.equal(f.requests[0].options.credentials, 'same-origin');
  f.advance(301); assert.equal(f.controller.expired(), true);
});
test('external playback rejects other Archive files, disguised hosts and mismatched book routes', async () => {
  for (const url of [archiveURL + '?download=1', archiveURL.replace('archive.org', 'archive.org.evil.test'), archiveURL.replace('Criterio_librivox.m4b', 'other.mp3'), 'http:' + archiveURL.slice(6)]) {
    const f = fixture(async () => ({ ok: true, json: async () => ({ provider: 'internet-archive', url, expiresAt: 1300000 }) }));
    await assert.rejects(f.controller.load('/api/audiolibros/libros/el-criterio/audio', { external: true }));
    assert.equal(f.audio.src, undefined);
  }
  const f = fixture(async () => ({ ok: true, json: async () => ({ provider: 'internet-archive', url: archiveURL, expiresAt: 1300000 }) }));
  await assert.rejects(f.controller.load('/api/audiolibros/libros/other/audio', { external: true }));
});
