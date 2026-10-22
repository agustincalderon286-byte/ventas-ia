import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";
import vm from "node:vm";

const safety = fs.readFileSync(new URL("../public/coach/pwa-safety.js", import.meta.url), "utf8");
const app = fs.readFileSync(new URL("../public/coach/app.js", import.meta.url), "utf8");
const worker = fs.readFileSync(new URL("../public/coach/sw.js", import.meta.url), "utf8");
function api() { const window = {}; vm.runInNewContext(safety, { window, Blob }); return window.TecPwaSafety; }
const tick = () => new Promise(setImmediate);

test("incremental journal preserves byte order and publishes progress only after committed writes", async () => {
  const parts = [], progress = []; let release, finalized;
  const wait = new Promise(resolve => { release = resolve; });
  const journal = api().createAudioJournal({ localId: "local", index: 0, mimeType: "audio/webm",
    writePart: async part => { await wait; parts.push(part); }, finalize: async value => { finalized = value; }, onProtected: value => progress.push(value) });
  const first = journal.append(new Blob(["HEADER"]), 1);
  const second = journal.append(new Blob(["MEDIA"]), 2);
  await tick(); assert.equal(parts.length, 0); assert.equal(progress.length, 0);
  release(); await first; await second;
  assert.deepEqual(progress, [1, 2]);
  assert.ok(parts.every(part => part.draft));
  assert.notEqual(parts[0].key, parts[1].key);
  await journal.finish(2);
  assert.equal(await finalized.blob.text(), "HEADERMEDIA");
  assert.equal(finalized.index, 0);
});

test("journal write failures do not claim a contiguous backup, but finalization can preserve all RAM bytes", async () => {
  let fail = true, final, progress = [];
  const journal = api().createAudioJournal({ localId: "local", index: 1, mimeType: "audio/mp4",
    writePart: async () => { if (fail) throw new Error("quota"); }, finalize: async value => { final = value; }, onProtected: value => progress.push(value) });
  await assert.rejects(journal.append(new Blob(["FIRST"]), 1), /quota/);
  fail = false; await journal.append(new Blob(["SECOND"]), 2);
  assert.equal(progress.length, 0);
  await journal.finish(2);
  assert.equal(await final.blob.text(), "FIRSTSECOND");
});

test("failed finalization keeps an emergency copy and never labels an unfinished tail as a complete file", async () => {
  const parts = [];
  const journal = api().createAudioJournal({ localId: "local", index: 2, mimeType: "audio/webm",
    writePart: async part => parts.push(part), finalize: async () => { throw new Error("full"); } });
  await journal.append(new Blob(["unfinished bytes"]), 3);
  assert.equal(parts[0].draft, true);
  await assert.rejects(journal.finish(3), /full/);
  assert.equal(await journal.emergencyBlob().text(), "unfinished bytes");
});

const now = Date.parse("2026-10-23T12:00:00Z");
const viewer = () => ({ authenticated: true, offlineVerifiedAt: "2026-10-23T11:00:00Z", user: { email: "one@example.invalid", accessGranted: true } });
test("offline access is bounded to 24 hours and does not outlive an ordinary subscription", () => {
  const allowed = api().offlineViewerAllowed;
  assert.equal(allowed(viewer(), now), true);
  for (const timestamp of ["", "invalid", "2026-10-21T11:00:00Z", "2026-10-24T11:00:00Z"]) {
    assert.equal(allowed({ ...viewer(), offlineVerifiedAt: timestamp }, now), false);
  }
  assert.equal(allowed({ ...viewer(), user: { email: "x", accessGranted: false } }, now), false);
  assert.equal(allowed({ ...viewer(), user: { ...viewer().user, subscriptionCurrentPeriodEnd: "2026-10-23T10:00:00Z" } }, now), false);
});

test("network errors may use a valid cached session, but 401/403 and anonymous responses revoke it", async () => {
  const body = app.slice(app.indexOf("async function fetchViewer()"), app.indexOf("function updateAuthTargets"));
  let status = 0, clears = 0, anonymous = false, cached = viewer();
  const fetchViewer = vm.runInNewContext(`(${body.trim()})`, {
    apiRequest: async () => { if (anonymous) return { authenticated: false }; throw Object.assign(new TypeError("network"), { status }); },
    cacheTecViewer: async () => {}, clearTecCachedViewer: async () => { clears++; }, getTecCachedViewer: async () => cached,
    window: { TecPwaSafety: { offlineViewerAllowed: value => api().offlineViewerAllowed(value, now) } }
  });
  assert.equal((await fetchViewer()).offline, true);
  status = 503; assert.equal((await fetchViewer()).offline, true);
  for (const code of [401, 403]) { status = code; assert.equal((await fetchViewer()).authenticated, false); }
  assert.equal(clears, 2);
  anonymous = true; assert.equal((await fetchViewer()).authenticated, false); assert.equal(clears, 3);
  anonymous = false; status = 0; cached = { ...viewer(), offlineVerifiedAt: "2026-10-20T00:00:00Z" };
  assert.equal((await fetchViewer()).authenticated, false);
});

test("sync does not auto-complete active or interrupted recordings, or access another account", async () => {
  const body = app.slice(app.indexOf("async function syncTecOfflineRecordingUnlocked"), app.indexOf("async function syncTecOfflineRecordings()"));
  let row = { localId: "local", ownerEmail: "one@example.invalid", status: "recording", consentConfirmed: true }, writes = 0;
  const sync = vm.runInNewContext(`(${body.trim()})`, { navigator: { onLine: true }, tecActiveRecordingIds: new Set(),
    tecOfflineGet: async () => row, getTecCachedViewer: async () => ({ user: { email: "one@example.invalid" } }),
    getTecAudioTail: async () => [{ draft: true }], saveTecOfflineRecording: async value => { row = value; return value; },
    apiRequest: async () => { writes++; throw new Error("Should not send"); } });
  assert.equal(await sync("local"), null);
  row.status = "ready"; row.ownerEmail = "another@example.invalid";
  assert.equal(await sync("local"), null);
  row.ownerEmail = "one@example.invalid";
  assert.equal(await sync("local"), null);
  assert.equal(row.status, "needs-review");
  assert.equal(writes, 0);
});

test("recording synchronization is single flight and honors another tab's capture lock", async () => {
  const body = app.slice(app.indexOf("async function syncTecOfflineRecording(localId"), app.indexOf("async function syncTecOfflineRecordingUnlocked"));
  let calls = 0, finish;
  const wait = new Promise(resolve => { finish = resolve; });
  const sync = vm.runInNewContext(`(${body.trim()})`, { tecRecordingSyncTasks: new Map(),
    withTecRecordingLock: async (_id, action) => action(), syncTecOfflineRecordingUnlocked: async () => { calls++; await wait; return "done"; } });
  const first = sync("local"), second = sync("local"); finish();
  assert.equal(await first, "done"); assert.equal(await second, "done"); assert.equal(calls, 1);
  const lockBody = app.slice(app.indexOf("async function withTecRecordingLock"), app.indexOf("async function holdTecRecordingLock"));
  const locked = vm.runInNewContext(`(${lockBody.trim()})`, { navigator: { locks: { request: async (_name, _opts, callback) => callback(null) } } });
  assert.equal(await locked("local", () => { throw new Error("must not run"); }), null);
});

test("releasing the capture lock waits until the lock manager actually releases it", async () => {
  const body = app.slice(app.indexOf("async function holdTecRecordingLock"), app.indexOf("async function uploadTecStoredAudioChunk"));
  let held = false;
  const acquire = vm.runInNewContext(`(${body.trim()})`, { navigator: { locks: { request: async (_name, _opts, callback) => {
    held = true; await callback({}); await tick(); held = false;
  } } } });
  const release = await acquire("local");
  assert.equal(held, true);
  await release();
  assert.equal(held, false);
});

test("a new service worker waits for old windows, and a broken shell cannot replace the working version", async () => {
  const events = {}; let waitingSkipped = 0, failed = false, install;
  const self = { addEventListener: (name, handler) => { events[name] = handler; }, skipWaiting() { waitingSkipped++; } };
  const puts = [];
  vm.runInNewContext(worker, { self, caches: { open: async () => ({ put: async key => puts.push(key) }) },
    fetch: async url => ({ ok: !(failed && url.includes("pwa-safety")), redirected: false, clone() { return this; } }) });
  events.install({ waitUntil(promise) { install = promise; } }); await install;
  assert.equal(waitingSkipped, 0); assert.ok(puts.some(url => url.includes("pwa-safety")));
  failed = true;
  events.install({ waitUntil(promise) { install = promise; } });
  await assert.rejects(install, /Incomplete Coach update/);
  assert.equal(waitingSkipped, 0);
});

test("complete audio replaces incremental parts atomically and an aborted transaction is not a successful save", async () => {
  const body = app.slice(app.indexOf("async function finalizeTecAudioSegment"), app.indexOf("async function getTecAudioTail"));
  const tx = new EventTarget(), request = new EventTarget(), writes = [];
  tx.objectStore = () => ({ put: value => writes.push(["put", value]), delete: key => writes.push(["delete", key]), openKeyCursor: () => request });
  const finalize = vm.runInNewContext(`(${body.trim()})`, { openTecOfflineDb: async () => ({ transaction: () => tx }), IDBKeyRange: { bound: (a, b) => [a, b] } });
  let done = false;
  const saving = finalize({ localId: "local", index: 0, blob: new Blob(["HEADERMEDIA"]) }).then(() => { done = true; });
  await tick();
  request.result = { primaryKey: "local:0000:part:000000", continue() { request.result = null; } };
  request.dispatchEvent(new Event("success"));
  assert.equal(done, false);
  assert.equal(writes[0][1].finalized, true);
  assert.deepEqual(writes[1], ["delete", "local:0000:part:000000"]);
  tx.dispatchEvent(new Event("complete")); await saving;
  const failing = finalize({ localId: "local", index: 1, blob: new Blob(["audio"]) }); await tick();
  tx.error = new Error("full"); tx.dispatchEvent(new Event("abort")); await assert.rejects(failing, /full/);
});

test("worker activation retains one previous shell and private runtime without deleting offline documents", async () => {
  const events = {}, deleted = []; let activation, claimed = false;
  const currentShell = worker.match(/TEC_CACHE_VERSION = "([^"]+)"/)[1];
  const currentRuntime = worker.match(/TEC_RUNTIME_CACHE = "([^"]+)"/)[1];
  const keys = ["unrelated", "tec-coach-shell-oldest", "tec-coach-runtime-oldest", "tec-coach-shell-previous",
    "tec-coach-runtime-previous", currentShell, currentRuntime, "tec-coach-documents-v1", "tec-coach-pdfjs-6.4.299", "tec-coach-catalog-20261021-pwa1"];
  vm.runInNewContext(worker, { self: { addEventListener: (name, handler) => { events[name] = handler; }, clients: { claim: async () => { claimed = true; } } },
    caches: { keys: async () => keys, delete: async key => { deleted.push(key); } } });
  events.activate({ waitUntil(promise) { activation = promise; } }); await activation;
  assert.deepEqual(deleted, ["tec-coach-shell-oldest", "tec-coach-runtime-oldest"]);
  assert.equal(claimed, true);
});

test("offline private HTML and its exact versioned assets can fall back to the previous cache", async () => {
  const html = { body: "previous private shell" }, asset = { body: "previous matching JS" };
  const context = vm.createContext({ self: { addEventListener() {} },
    caches: { keys: async () => ["tec-coach-runtime-previous"],
      open: async name => ({ match: async request => name.endsWith("previous") && request === "/coach/app/" ? html : null }),
      match: async request => request === "/coach/app.js?v=previous" ? asset : null },
    fetch: async () => { throw new TypeError("offline"); } });
  vm.runInContext(worker, context);
  assert.equal(await vm.runInContext('networkFirst("/coach/app/", TEC_RUNTIME_CACHE)', context), html);
  assert.equal(await vm.runInContext('cacheFirst("/coach/app.js?v=previous", TEC_CACHE_VERSION)', context), asset);
});
