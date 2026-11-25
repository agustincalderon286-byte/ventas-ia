import test from 'node:test';
import assert from 'node:assert/strict';
import express from 'express';
import mongoose from 'mongoose';
import sift from 'sift';
import { createLocalDemoQueue, registerLocalDemoWorker } from '../src/coach-demo-local.js';

function model() {
  const rows = [];
  const find = query => rows.filter(sift(query));
  const result = value => {
    const query = { lean: async () => structuredClone(value), then: resolve => resolve(value), select: () => query };
    return query;
  };
  const get = (row, path) => path.split('.').reduce((value, key) => value?.[key], row);
  function set(row, path, value) {
    const keys = path.split('.');
    const property = keys.pop();
    const target = keys.reduce((current, key) => current[key] ??= {}, row);
    target[property] = value;
  }
  function update(row, changes) {
    for (const [path, value] of Object.entries(changes.$set || {})) set(row, path, value);
    for (const [path, value] of Object.entries(changes.$inc || {})) set(row, path, (get(row, path) || 0) + value);
  }
  return {
    rows,
    findOne: query => result(find(query)[0]),
    findOneAndUpdate(query, changes) {
      const match = find(query)[0];
      if (match) update(match, changes);
      return result(match);
    },
    exists: async query => Boolean(find(query).length),
    async updateOne(query, changes, options = {}) {
      let match = find(query)[0];
      if (!match && options.upsert) { match = { ...query }; rows.push(match); }
      if (match) update(match, changes);
      return { modifiedCount: Number(Boolean(match)) };
    },
    async updateMany(query, changes) {
      const matches = find(query);
      matches.forEach(row => update(row, changes));
      return { matchedCount: matches.length };
    },
    async deleteMany(query) {
      for (const row of find(query)) rows.splice(rows.indexOf(row), 1);
    }
  };
}

function fixture(finish) {
  const analyses = model(), audio = model();
  let time = Date.UTC(2026, 10, 24);
  const queue = createLocalDemoQueue({ analyses, audio, finish, now: () => time });
  const analysis = {
    _id: String(new mongoose.Types.ObjectId()), ownerUserId: String(new mongoose.Types.ObjectId()),
    status: 'recording', chunks: [{ index: 0, status: 'staged' }, { index: 1, status: 'staged' }],
    async save() { const { save, ...row } = this; analyses.rows.splice(0, 1, structuredClone(row)); }
  };
  const selection = { indexes: [0], partial: true, gaps: [], knownOutcome: 'follow_up' };
  const bytes = Buffer.from('audio ficticio');
  async function enqueue() {
    await queue.stage(analysis, 0, bytes, false);
    await queue.stage(analysis, 1, bytes, false);
    assert.equal(await queue.enqueue(analysis, selection), true);
  }
  return { analyses, audio, queue, analysis, bytes, enqueue, moveTime: ms => { time += ms; } };
}

test('la Mac recibe solo segmentos seleccionados y dos entregas con la misma concesión usan Luna una vez', async () => {
  let calls = 0, release;
  const barrier = new Promise(resolve => { release = resolve; });
  const { analyses, audio, queue, analysis, bytes, enqueue } = fixture(async (doc, options) => {
    calls++;
    assert.equal(doc.chunks[0].text, 'Llámame el viernes a las 3 para revisar el filtro.');
    assert.equal(doc.chunks[1].status, 'staged');
    assert.equal(options.partial, true);
    await barrier;
    return { status: 'completed', report: { title: 'Seguimiento del filtro' } };
  });
  await enqueue();
  const job = await queue.claim();
  assert.equal(await queue.claim(), null);
  assert.equal(await queue.read(analysis._id, 0, 'unknown'), null);
  assert.equal(await queue.read(analysis._id, 1, job.lease), null);
  assert.deepEqual(await queue.read(analysis._id, 0, job.lease), bytes);
  assert.equal(await queue.submit(job.id, job.lease, { chunks: [] }), 422);
  const result = { chunks: [{ index: 0, text: 'Llámame el viernes a las 3 para revisar el filtro.', language: 'es' }] };
  const first = queue.submit(job.id, job.lease, result);
  assert.equal(await queue.submit(job.id, job.lease, result), 409);
  release();
  assert.equal(await first, 204);
  assert.equal(calls, 1);
  assert.equal(analyses.rows[0].status, 'completed');
  assert.equal(audio.rows.length, 0);
});

test('la cola guarda las frases con tiempos y la segunda lectura, rechaza lecturas mal formadas y acepta el formato anterior', async () => {
  let received;
  const { analyses, queue, enqueue } = fixture(async doc => { received = doc.chunks[0]; return { status: 'completed', report: {} }; });
  await enqueue();
  const job = await queue.claim();
  const text = 'El filtro cuesta 799 dólares.';
  const voice = [{ start: 0, end: 2.5, text }];
  const bad = [
    { voice, checks: [{ segment: 3, second: text, gapBefore: 0, gapAfter: 0 }] },
    { voice, checks: [{ segment: 0, second: text, gapBefore: -1, gapAfter: 0 }] },
    { voice: [{ start: 2, end: 1, text }], checks: [] },
    { voice }
  ];
  for (const extra of bad) assert.equal(await queue.submit(job.id, job.lease, { chunks: [{ index: 0, text, language: 'es', ...extra }] }), 422);
  const checks = [{ segment: 0, second: 'El filtro cuesta 799 dólares.', gapBefore: 0.06, gapAfter: 0.04, extra: 'fuera' }];
  assert.equal(await queue.submit(job.id, job.lease, { chunks: [{ index: 0, text, language: 'es', voice, checks }] }), 204);
  assert.deepEqual(received.checks, [{ segment: 0, second: 'El filtro cuesta 799 dólares.', gapBefore: 0.06, gapAfter: 0.04 }]);
  assert.deepEqual(analyses.rows[0].chunks[0].voice, voice);

  const legacy = fixture(async doc => { received = doc.chunks[0]; return { status: 'completed', report: {} }; });
  await legacy.enqueue();
  const old = await legacy.queue.claim();
  assert.equal(await legacy.queue.submit(old.id, old.lease, { chunks: [{ index: 0, text, language: 'es' }] }), 204);
  assert.equal(received.voice, undefined);
  assert.equal(received.checks, undefined);
});

test('la cola recupera una concesión perdida una vez y conserva el audio al agotar sus intentos', async () => {
  const { analyses, audio, queue, enqueue, moveTime } = fixture(() => assert.fail('No se envió diálogo'));
  await enqueue();
  const first = await queue.claim();
  moveTime(31 * 60_000);
  await queue.expire();
  const second = await queue.claim();
  assert.notEqual(first.lease, second.lease);
  assert.equal(await queue.submit(first.id, first.lease, { failed: true }), 409);
  assert.equal(await queue.submit(second.id, second.lease, { failed: true }), 204);
  assert.equal(await queue.claim(), null);
  assert.equal(analyses.rows[0].status, 'error');
  assert.equal(audio.rows.length, 2);
});

test('el puerto del worker no reclama trabajos sin la credencial de la Mac', async t => {
  const app = express();
  let claimed = 0;
  const key = 'key-for-fictional-local-queue-tests';
  registerLocalDemoWorker(app, { key, queue: { expire() {}, claim() { claimed++; return null; } } });
  const server = app.listen(0, '127.0.0.1');
  await new Promise(resolve => server.once('listening', resolve));
  t.after(() => new Promise(resolve => server.close(resolve)));
  const url = `http://127.0.0.1:${server.address().port}/api/coach/demo-worker/claim`;
  assert.equal((await fetch(url, { method: 'POST' })).status, 404);
  assert.equal((await fetch(url, { method: 'POST', headers: { Authorization: `Bearer ${key}` } })).status, 204);
  assert.equal(claimed, 1);
});
