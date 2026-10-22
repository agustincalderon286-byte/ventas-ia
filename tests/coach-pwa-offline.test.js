import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const appSource = fs.readFileSync(path.join(root, "public/coach/app.js"), "utf8");
const workerSource = fs.readFileSync(path.join(root, "public/coach/sw.js"), "utf8");
const serverSource = fs.readFileSync(path.join(root, "server.js"), "utf8");
const manifest = JSON.parse(fs.readFileSync(path.join(root, "public/coach/site.webmanifest"), "utf8"));

test("TEC conserva su instalacion PWA y abre directo al Coach", () => {
  assert.equal(manifest.display, "standalone");
  assert.equal(manifest.start_url, "/coach/app/");
  assert.equal(manifest.scope, "/coach/");
  assert.ok(manifest.icons.some(icon => icon.sizes === "512x512"));
});

test("el service worker conserva interfaz y catalogo para uso offline", () => {
  assert.match(workerSource, /TEC_CORE_ASSETS/);
  assert.match(workerSource, /networkFirst\(request, TEC_RUNTIME_CACHE/);
  assert.match(workerSource, /CACHE_CATALOG_ASSETS/);
  assert.match(workerSource, /tec-sync-outbox/);
});

test("leads y 4 en 14 entran a la bandeja local cuando falla la red", () => {
  assert.match(appSource, /offlineKind: "lead"/);
  assert.match(appSource, /offlineKind: "programa-4-en-14"/);
  assert.match(appSource, /Lead protegido en este telefono/);
  assert.match(appSource, /Hoja protegida en este telefono/);
});

test("el audio se escribe localmente antes de intentar subirlo", () => {
  assert.match(appSource, /finalize: finalizeTecAudioSegment/);
  const recorderStop = appSource.slice(appSource.indexOf('recorder.addEventListener("stop"'));
  assert.ok(recorderStop.indexOf("await journal.finish") < recorderStop.indexOf(".then(() => uploadSegment"));
  assert.match(appSource, /Grabacion protegida en este telefono/);
});

test("el servidor hace idempotentes los reintentos offline", () => {
  assert.match(serverSource, /x-tec-request-id/);
  assert.match(serverSource, /partialFilterExpression: \{ offlineRequestId: \{ \$type: "string" \} \}/);
  assert.match(serverSource, /clientRecordingId/);
  assert.match(serverSource, /app\.get\("\/api\/coach\/catalog-offline"/);
});
