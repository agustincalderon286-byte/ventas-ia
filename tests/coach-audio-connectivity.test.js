import test from "node:test";
import assert from "node:assert/strict";
import vm from "node:vm";
import { readFileSync } from "node:fs";
const app = readFileSync(new URL("../public/coach/app.js", import.meta.url), "utf8");
const safety = readFileSync(new URL("../public/coach/pwa-safety.js", import.meta.url), "utf8");
function helpers() { const window = {}; vm.runInNewContext(safety, { window, Blob }); return window.TecPwaSafety; }

test("an HTTP failure or timeout is never presented as confirmed lack of internet", () => {
  const describe = helpers().describeAudioFailure;
  assert.equal(describe({ status: 401 }, true).kind, "auth");
  assert.equal(describe({ status: 403 }, true).kind, "auth");
  assert.equal(describe({ status: 503 }, true).kind, "server");
  assert.equal(describe({ status: 502 }, false).kind, "server", "a server response is stronger evidence than the browser's offline flag");
  assert.equal(describe({ name: "AbortError" }, true).kind, "timeout");
  assert.equal(describe({ lastStatus: 413 }, true).kind, "rejected");
  assert.equal(describe({ lastStatus: 429 }, true).kind, "busy");
  assert.equal(describe(new TypeError("network"), true).kind, "unconfirmed");
  assert.equal(describe({}, false).kind, "offline");
});
test("pending badge excludes active captures, interrupted drafts, partial reports and another account", async () => {
  const rows = [
    { localId: "active", status: "recording", ownerEmail: "me@example.invalid" },
    { localId: "stale-capture", status: "recording", ownerEmail: "me@example.invalid" },
    { localId: "ready", status: "ready", ownerEmail: "me@example.invalid" },
    { localId: "syncing", status: "syncing", ownerEmail: "me@example.invalid" },
    { localId: "review", status: "needs-review", ownerEmail: "me@example.invalid" },
    { localId: "partial", status: "partial-synced", ownerEmail: "me@example.invalid" },
    { localId: "another", status: "ready", ownerEmail: "other@example.invalid" }
  ];
  const body = app.slice(app.indexOf("async function getTecOfflinePendingCount()"), app.indexOf("function attachTecOfflineForm"));
  const count = vm.runInNewContext(`(${body.trim()})`, { window: { TecPwaSafety: helpers() }, tecActiveRecordingIds: new Set(["active"]),
    tecOfflineGetAll: async store => store === "recordings" ? rows : [{ ownerEmail: "me@example.invalid" }, { ownerEmail: "other@example.invalid" }],
    getTecCachedViewer: async () => ({ user: { email: "me@example.invalid" } }) });
  assert.equal(await count(), 3);
});
test("failed sync preserves HTTP status, phase and local recording without deleting audio", async () => {
  const body = app.slice(app.indexOf("async function syncTecOfflineRecordingUnlocked"), app.indexOf("async function syncTecOfflineRecordings()"));
  for (const [stage, responseStatus] of [["session", 401], ["transcription", 502], ["analysis", 503]]) {
    let recording = { localId: "local", ownerEmail: "me@example.invalid", status: "ready", consentConfirmed: true,
      serverSessionId: stage === "session" ? "" : "server" }, deletes = 0;
    const sync = vm.runInNewContext(`(${body.trim()})`, {
      navigator: { onLine: true }, tecActiveRecordingIds: new Set(), tecOfflineGet: async () => recording,
      getTecCachedViewer: async () => ({ user: { email: "me@example.invalid" } }), getTecAudioTail: async () => [],
      saveTecOfflineRecording: async row => { recording = { ...recording, ...row }; return recording; },
      getTecOfflineAudioChunks: async () => stage === "transcription" ? [{ key: "audio" }] : [],
      uploadTecStoredAudioChunk: async () => { throw Object.assign(new Error("Transcription failed"), { status: responseStatus }); },
      apiRequest: async () => { throw Object.assign(new Error("Service failed"), { status: responseStatus }); },
      tecOfflineDelete: async () => { deletes++; }
    });
    await assert.rejects(sync("local"));
    assert.equal(recording.lastStatus, responseStatus); assert.equal(recording.failureStage, stage);
    assert.equal(recording.status, "ready"); assert.equal(deletes, 0);
  }
});
test("real recorder startup preserves server errors and avoids the old false offline message", () => {
  const start = app.slice(app.indexOf("  const startRecording = async () =>"), app.indexOf('  consent.addEventListener("change"', app.indexOf("  const startRecording = async () =>")));
  assert.match(start, /sessionError = error/);
  assert.match(start, /lastStatus: Number\(sessionError\?\.status/);
  assert.doesNotMatch(start, /Grabacion activa sin internet|Modo sin conexion/);
  assert.match(start, /audioFailure\(sessionError/);
  assert.doesNotMatch(app, /TEC volvera a intentar cuando haya una conexion estable/);
});
test("a successful HTTP response without a transcription receipt cannot delete local audio", async () => {
  const body = app.slice(app.indexOf("async function uploadTecStoredAudioChunk"), app.indexOf("async function syncTecOfflineRecording(localId"));
  let payload = {}, httpStatus = 200, deletes = 0;
  const upload = vm.runInNewContext(`(${body.trim()})`, {
    fetch: async () => ({ ok: httpStatus === 200, status: httpStatus, json: async () => payload }),
    tecOfflineDelete: async () => { deletes++; }
  });
  const chunk = { key: "local:0000", mimeType: "audio/mp4", blob: new Blob(["fictional audio"]), durationSeconds: 10 };
  await assert.rejects(upload("session", chunk), error => error.status === 502);
  assert.equal(deletes, 0);
  httpStatus = 401; payload = { error: "Session ended" };
  await assert.rejects(upload("session", chunk), error => error.status === 401);
  assert.equal(deletes, 0);
  httpStatus = 200; payload = { ok: true, chunk: { status: "transcribed", index: 0 } };
  await upload("session", chunk);
  assert.equal(deletes, 1);
});
test("connection check reads the authenticated session only, without triggering audio analysis", () => {
  const check = app.slice(app.indexOf('      checkConnection.addEventListener("click"'), app.indexOf("      card.append(checkConnection"));
  assert.match(check, /apiRequest\("\/api\/coach\/me"/);
  assert.doesNotMatch(check, /demo-analyses|method: "POST"|\/complete/);
  assert.match(check, /Esto no confirma todavía/);
});
