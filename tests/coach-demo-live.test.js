import test from 'node:test';
import assert from 'node:assert/strict';
import mongoose from 'mongoose';
import sift from 'sift';
import { createLiveDemo, liveNoticesFrom, LIVE_MAX_NOTICES } from '../src/coach-demo-live.js';

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
    for (const [path, value] of Object.entries(changes.$set || {})) set(row, path, structuredClone(value));
    for (const [path, value] of Object.entries(changes.$inc || {})) set(row, path, (get(row, path) || 0) + value);
    for (const [path, value] of Object.entries(changes.$addToSet || {})) {
      const list = get(row, path) || [];
      if (!list.includes(value)) set(row, path, [...list, value]);
    }
    for (const [path, value] of Object.entries(changes.$pull || {})) set(row, path, (get(row, path) || []).filter(item => !value.$in.includes(item)));
    for (const [path, value] of Object.entries(changes.$push || {})) set(row, path, [...(get(row, path) || []), ...structuredClone(value.$each)]);
  }
  return {
    rows,
    findOne: query => result(find(query)[0]),
    findOneAndUpdate(query, changes) {
      const match = find(query)[0];
      if (match) update(match, changes);
      return result(match);
    },
    async updateOne(query, changes) {
      const match = find(query)[0];
      if (match) update(match, changes);
      return { modifiedCount: Number(Boolean(match)) };
    }
  };
}

const owner = String(new mongoose.Types.ObjectId());
// A Wednesday afternoon in Chicago: «mañana» is Thursday.
const wednesday = Date.parse('2026-11-25T20:00:00Z');

function fixture(analyze) {
  const analyses = model(), audio = model();
  let time = wednesday;
  const live = createLiveDemo({ analyses, audio, analyze, now: () => time });
  const id = String(new mongoose.Types.ObjectId());
  analyses.rows.push({ _id: id, ownerUserId: owner, status: 'recording', startedAt: new Date(wednesday - 10 * 60_000),
    autoClose: { zone: 'America/Chicago' }, live: live.initial(),
    chunks: [{ index: 0, status: 'staged', mimeType: 'audio/mp4', durationSeconds: 240 }] });
  audio.rows.push({ _id: `${id}:0`, analysisId: id, ownerUserId: owner, bytes: Buffer.from('audio ficticio'), expiresAt: new Date(wednesday + 3600_000) });
  return { analyses, live, id, advance: ms => { time += ms; } };
}

const settle = () => new Promise(resolve => setImmediate(resolve));
const piece = 'Te doy una referencia, mi prima Juana Pérez, su teléfono es 773 555 01. Quedamos mañana viernes a las 6 de la tarde para la demo.';

test('las notas en vivo no muestran nombres, teléfonos ni montos con la pantalla bloqueada', () => {
  const notices = liveNoticesFrom({
    review: [
      { field: 'referral', label: 'Juana Pérez', state: 'preguntar' },
      { field: 'referral', label: '', state: 'preguntar', text: 'Como referencia anota a Ana Soto, luego' },
      { field: 'price', label: 'Filtro Premium', state: 'preguntar', text: '$79 al mes' },
      { field: 'referral', label: 'Rosa Gil', state: 'guardar' }
    ],
    followUp: { when: 'mañana viernes a las 6 de la tarde' }
  }, { followUp: 'Dijiste viernes, pero esa fecha cae en jueves 26 de noviembre. ¿Qué día quieres?', lastSegmentText: 'Bueno, gracias.' });
  assert.deepEqual(notices.map(notice => notice.short), ['Un referido tiene el teléfono dudoso.', 'Un referido quedó incompleto.',
    'Un precio no quedó claro.', 'La fecha de la cita no quedó clara.']);
  for (const notice of notices) assert.doesNotMatch(notice.short, /\d|Juana|Ana|Filtro/);
  assert.match(notices[0].text, /Juana Pérez/);
  assert.match(notices[3].text, /Cita «mañana viernes a las 6 de la tarde»: Dijiste viernes/);
  // A referral cut at the end of the newest piece may be completed in the next one.
  const waiting = liveNoticesFrom({ review: [{ field: 'referral', label: '', state: 'preguntar', text: 'Como referencia anota a Ana Soto, luego' }] },
    { lastSegmentText: 'Y como referencia anota a Ana Soto, luego' });
  assert.deepEqual(waiting, []);
});

test('la Mac transcribe cada pedazo durante la demo y Jorge avisa una sola vez lo que no cuadra', async () => {
  let calls = 0;
  const { analyses, live, id } = fixture(async ({ chunks }) => {
    calls++;
    assert.equal(chunks[0].text, piece);
    return { referrals: [], pendingReferrals: 1, followUp: { when: 'mañana viernes a las 6 de la tarde', segment: 1 },
      review: [{ field: 'referral', label: 'Juana Pérez', state: 'preguntar' }] };
  });
  await live.queue(id, 0);
  const job = await live.claim();
  assert.deepEqual(job.chunks, [{ index: 0, mimeType: 'audio/mp4' }]);
  assert.equal(await live.claim(), null, 'one piece is not handed to two workers');
  assert.equal(String(await live.read(id, 0, job.lease)), 'audio ficticio');
  assert.equal(await live.read(id, 0, 'otra-llave'), null);
  assert.equal(await live.submit(id, job.lease, { chunks: [{ index: 0, text: piece, language: 'es' }] }), 204);
  await settle(); await settle();
  const row = analyses.rows[0];
  assert.deepEqual(row.live.pending, []);
  assert.equal(row.live.analyzing, false);
  const first = await live.notices(owner, id, 0);
  assert.equal(first.recording, true);
  assert.deepEqual(first.notices.map(notice => notice.n), [1, 2]);
  assert.match(first.notices[1].text, /Dijiste viernes, pero esa fecha cae en jueves 26 de noviembre/);
  // The next check finds the same doubts: nothing new is sent.
  await live.check(id);
  assert.equal(calls, 2);
  assert.deepEqual((await live.notices(owner, id, 2)).notices, []);
  assert.equal(await live.notices(String(new mongoose.Types.ObjectId()), id, 0), null, 'only the seller reads their notices');
});

test('como mucho cuatro avisos por demo y nada se procesa cuando la demo ya terminó', async () => {
  const { analyses, live, id } = fixture(async () => ({ referrals: [], pendingReferrals: 0, followUp: null,
    review: ['Ana Uno', 'Bea Dos', 'Carla Tres', 'Dora Cuatro', 'Eva Cinco'].map(label => ({ field: 'referral', label, state: 'preguntar' })) }));
  analyses.rows[0].live.texts = [{ index: 0, text: piece }];
  await live.check(id);
  assert.equal((await live.notices(owner, id, 0)).notices.length, LIVE_MAX_NOTICES);
  analyses.rows[0].status = 'queued_local';
  await live.queue(id, 1);
  assert.deepEqual(analyses.rows[0].live.pending, []);
  assert.equal(await live.claim(), null);
  await live.check(id);
  assert.equal((await live.notices(owner, id, 0)).recording, false);
});
