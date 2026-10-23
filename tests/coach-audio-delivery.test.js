import test from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import vm from "node:vm";
import express from "express";
import mongoose from "mongoose";

const source = readFileSync(new URL("../server.js", import.meta.url), "utf8");
const routes = source.slice(source.indexOf('app.post("/api/coach/demo-analyses",'), source.indexOf('app.delete("/api/coach/demo-analyses/:id",'));
async function harness(run) {
  const app = express(); app.use(express.json());
  const records = new Map(), owner = new mongoose.Types.ObjectId();
  const counts = { transcriptions: 0, reports: 0 };
  const hydrate = value => {
    const copy = structuredClone(value);
    copy.toObject = () => { const { save, toObject, ...data } = copy; return structuredClone(data); };
    copy.save = async () => { records.set(String(copy._id), copy.toObject()); };
    return copy;
  };
  const models = {
    async create(data) { const value = { ...data, ownerUserId: String(data.ownerUserId), _id: String(new mongoose.Types.ObjectId()) }; records.set(value._id, value); return hydrate(value); },
    async findOne(query) {
      const value = [...records.values()].find(row => Object.entries(query).every(([key, expected]) => String(row[key]) === String(expected)));
      return value ? hydrate(value) : null;
    }
  };
  const context = {
    app, express, mongoose, Buffer, console, CoachDemoAnalysis: models,
    requireCoachActivo: async (req, res) => {
      if (req.headers['x-test-account'] === 'other') return { user: { _id: new mongoose.Types.ObjectId(), email: 'other@example.invalid' } };
      return { user: { _id: owner, email: 'test@example.invalid', name: 'Fictional test' } };
    },
    responderCoachError: (res, status, error) => res.status(status).json({ error }),
    limpiarCoachDemoAnalysis: row => ({ ...row, id: String(row._id) }),
    normalizarCoachDemoKnownOutcome: value => value || 'unknown', truncarTextoPrompt: text => text,
    COACH_DEMO_MAX_CHUNKS: 30, COACH_DEMO_MAX_SECONDS: 5400, COACH_DEMO_MAX_CHUNK_BYTES: 8 * 1024 * 1024,
    COACH_DEMO_TRANSCRIPTION_MODEL: 'mock', COACH_DEMO_ANALYSIS_MODEL: 'mock',
    transcribirCoachDemoChunk: async buffer => { counts.transcriptions++; await new Promise(resolve => setTimeout(resolve, 20)); return { text: `Conversación ficticia suficientemente larga para la prueba ${buffer.toString()}`, languages: [] }; },
    analizarCoachDemoTranscript: async () => { counts.reports++; await new Promise(resolve => setTimeout(resolve, 20)); return { title: 'Fictional report' }; }
  };
  // Execute the actual production route handlers, with only database/auth/AI boundaries replaced.
  const lockStart = source.indexOf('const coachDemoMutations = new Map();');
  if (lockStart >= 0) vm.runInNewContext(source.slice(lockStart, source.indexOf('app.post("/api/coach/demo-analyses",', lockStart)) + routes, context);
  else vm.runInNewContext(routes, context);
  const server = await new Promise(resolve => { const s = app.listen(0, '127.0.0.1', () => resolve(s)); });
  const base = `http://127.0.0.1:${server.address().port}/api/coach/demo-analyses`;
  const request = async (path = '', body = {}, extra = {}) => {
    const response = await fetch(base + path, { method: 'POST', headers: { 'Content-Type': 'application/json', ...extra.headers }, body: JSON.stringify(body), ...extra });
    return { status: response.status, data: await response.json() };
  };
  const create = async () => (await request('', { consentConfirmed: true, clientRecordingId: `fixture-${records.size}` })).data.analysis.id;
  const chunk = (id, index, extra = {}) => request(`/${id}/chunks`, {}, {
    headers: { 'Content-Type': 'audio/mp4;codecs=mp4a.40.2', 'X-Coach-Chunk-Index': String(index), 'X-Coach-Chunk-Duration': '240', ...extra }, body: Buffer.from(`audio-${index}`)
  });
  try { await run({ request, create, chunk, counts, records, base }); }
  finally { await new Promise(resolve => server.close(resolve)); }
}

test('a lost upload receipt can be retried after completion without leaving a permanent HTTP 409', () => harness(async ({ create, chunk, request, counts }) => {
  const id = await create();
  assert.equal((await chunk(id, 0)).status, 200);
  assert.equal((await request(`/${id}/complete`)).status, 200);
  const retried = await chunk(id, 0);
  assert.equal(retried.status, 200);
  assert.equal(retried.data.chunk.status, 'transcribed');
  assert.equal(retried.data.duplicate, true);
  assert.equal((await request(`/${id}/complete`)).status, 200);
  assert.deepEqual(counts, { transcriptions: 1, reports: 1 });
  assert.equal((await chunk(id, 1)).status, 409, 'unprocessed audio must never be acknowledged as saved');
  assert.equal((await chunk(id, 0, { 'X-Test-Account': 'other' })).status, 404);
}));

test('concurrent uploads and completion retries do not lose segments or run analysis twice', () => harness(async ({ create, chunk, request, counts, records }) => {
  const id = await create();
  const uploads = await Promise.all([chunk(id, 0), chunk(id, 1), chunk(id, 0)]);
  assert.ok(uploads.every(response => response.status === 200));
  assert.equal(records.get(id).chunks.length, 2);
  const completed = await Promise.all([request(`/${id}/complete`), request(`/${id}/complete`)]);
  assert.ok(completed.every(response => response.status === 200));
  assert.deepEqual(counts, { transcriptions: 2, reports: 1 });
}));

test('completion waits for all expected segments and an analysis failure does not block upload recovery', () => harness(async ({ create, chunk, request, counts, records }) => {
  const id = await create();
  await chunk(id, 0);
  assert.equal((await request(`/${id}/complete`, { expectedChunkIndexes: [0, 1] })).status, 409);
  assert.equal(counts.reports, 0);
  records.get(id).status = 'error';
  assert.equal((await chunk(id, 1)).status, 200);
  assert.equal((await request(`/${id}/complete`, { expectedChunkIndexes: [0, 1] })).status, 200);
  assert.equal(records.get(id).chunks.length, 2);
  assert.equal(counts.reports, 1);
}));

test('a transcribed silent segment is acknowledged on retry without another transcription', () => harness(async ({ create, chunk, request, counts, records }) => {
  const id = await create();
  await chunk(id, 0);
  await chunk(id, 1);
  records.get(id).chunks[0].text = '';
  await request(`/${id}/complete`);
  assert.equal((await chunk(id, 0)).status, 200);
  assert.equal(counts.transcriptions, 2);
}));

test('actual client sync recovers lost upload and report responses, clears pending state and keeps one report', () => harness(async ({ create, counts, records, base }) => {
  const id = await create();
  const client = readFileSync(new URL('../public/coach/app.js', import.meta.url), 'utf8');
  let local = { localId: 'local', serverSessionId: id, ownerEmail: 'test@example.invalid', consentConfirmed: true, status: 'ready', protectedChunks: 2 };
  const chunks = new Map([0, 1].map(index => [String(index), { key: String(index), index, durationSeconds: 240, mimeType: 'audio/mp4', blob: new Blob([`audio-${index}`]) }]));
  const events = [];
  let loseChunkReceipt = true, loseReportReceipt = true;
  const ctx = vm.createContext({
    navigator: { onLine: true }, Blob, AbortController, CustomEvent, tecActiveRecordingIds: new Set(),
    window: { setTimeout, clearTimeout, dispatchEvent: event => events.push(event) },
    fetch: async (url, options) => {
      const response = await fetch(base + url.replace('/api/coach/demo-analyses', ''), options);
      if (url.endsWith('/chunks') && loseChunkReceipt) { loseChunkReceipt = false; throw new TypeError('Lost upload response'); }
      if (url.endsWith('/complete') && loseReportReceipt) { loseReportReceipt = false; throw new TypeError('Lost report response'); }
      return response;
    },
    tecOfflineGet: async () => local,
    getTecCachedViewer: async () => ({ user: { email: 'test@example.invalid' } }),
    getTecAudioTail: async () => [], getTecOfflineAudioChunks: async () => [...chunks.values()],
    saveTecOfflineRecording: async value => (local = { ...local, ...value }),
    tecOfflineDelete: async (store, key) => { if (store === 'recordings') local = null; else chunks.delete(key); }
  });
  vm.runInContext(client.slice(0, client.indexOf('\nconst ')) +
    client.slice(client.indexOf('async function uploadTecStoredAudioChunk'), client.indexOf('async function syncTecOfflineRecording(localId')) +
    client.slice(client.indexOf('async function syncTecOfflineRecordingUnlocked'), client.indexOf('async function syncTecOfflineRecordings()')), ctx);
  const sync = () => vm.runInContext('syncTecOfflineRecordingUnlocked("local")', ctx);
  await assert.rejects(sync(), /Lost upload/);
  assert.equal(chunks.size, 2); assert.equal(local.status, 'ready');
  await assert.rejects(sync(), /Lost report/);
  assert.equal(chunks.size, 2); assert.equal(local.status, 'ready');
  assert.equal(records.get(id).status, 'completed');
  const recovered = await sync();
  assert.equal(recovered.id, id); assert.equal(local, null); assert.equal(chunks.size, 0);
  assert.equal(events.at(-1).detail.analysis.id, id);
  assert.deepEqual(counts, { transcriptions: 2, reports: 1 });
}));

test('the production empty-transcript case finishes in review instead of permanent HTTP 400 retries', () => harness(async ({ create, chunk, request, counts, records }) => {
  const id = await create();
  await chunk(id, 0);
  records.get(id).chunks[0].text = '';
  assert.equal((await request(`/${id}/complete`)).status, 422, 'old open clients must not mistake review for a completed report');
  const result = await request(`/${id}/complete`, { expectedChunkIndexes: [0], audioReviewVersion: 1 });
  assert.equal(result.status, 200); assert.equal(result.data.analysis.status, 'needs_audio');
  assert.equal(counts.reports, 0);
  assert.equal(records.get(id).chunks.length, 1);
  // An explicit retry can replace an empty transcript without inventing dialogue.
  assert.equal((await chunk(id, 0, { 'X-Coach-Retry-Empty': '1' })).status, 200);
  assert.equal((await request(`/${id}/complete`, { expectedChunkIndexes: [0] })).data.analysis.status, 'completed');
  assert.equal(counts.reports, 1); assert.equal(counts.transcriptions, 2);
}));

test('client retains audio for empty transcripts and stops automatic retries', async () => {
  const client = readFileSync(new URL('../public/coach/app.js', import.meta.url), 'utf8');
  let local = { localId: 'local', serverSessionId: 'server', ownerEmail: 'test@example.invalid', consentConfirmed: true, status: 'ready', protectedChunks: 1 };
  let requests = 0, deleted = 0;
  const sync = vm.runInNewContext(`(${client.slice(client.indexOf('async function syncTecOfflineRecordingUnlocked'), client.indexOf('async function syncTecOfflineRecordings()')).trim()})`, {
    navigator: { onLine: true }, tecActiveRecordingIds: new Set(),
    tecOfflineGet: async () => local, getTecCachedViewer: async () => ({ user: { email: 'test@example.invalid' } }),
    getTecAudioTail: async () => [], getTecOfflineAudioChunks: async () => [{ key: 'audio', blob: new Blob(['original audio']) }],
    uploadTecStoredAudioChunk: async () => {},
    apiRequest: async () => { requests++; return { analysis: { id: 'server', status: 'needs_audio', error: 'No dialogue' } }; },
    saveTecOfflineRecording: async value => (local = { ...local, ...value }),
    tecOfflineDelete: async () => { deleted++; }, window: { dispatchEvent() {} }, CustomEvent
  });
  assert.equal((await sync('local')).status, 'needs_audio');
  assert.equal(local.status, 'needs-audio'); assert.equal(deleted, 0);
  assert.equal(await sync('local'), null); assert.equal(requests, 1);
});

test('transcription rejects malformed success responses rather than silently accepting an empty transcript', async () => {
  const body = source.slice(source.indexOf('async function transcribirCoachDemoChunk'), source.indexOf('const COACH_DEMO_REPORT_SCHEMA'));
  let payload = {}, form;
  const transcribe = vm.runInNewContext(`(${body.trim()})`, {
    process: { env: { OPENAI_API_KEY: 'fictional-test-key' } }, COACH_DEMO_TRANSCRIPTION_MODEL: 'gpt-transcribe',
    FormData, Blob, fetch: async (_url, options) => { form = options.body; return { ok: true, json: async () => payload }; }
  });
  await assert.rejects(transcribe(Buffer.from('fictional')), /respuesta incompleta/);
  assert.equal(form.get('response_format'), 'json');
  payload = { text: '' }; assert.equal((await transcribe(Buffer.from('fictional'))).text, '');
  payload = { text: ' Texto de prueba ' }; assert.equal((await transcribe(Buffer.from('fictional'))).text, 'Texto de prueba');
});
