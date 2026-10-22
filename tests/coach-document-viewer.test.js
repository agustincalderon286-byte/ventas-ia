import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";
import vm from "node:vm";

const source = fs.readFileSync(new URL("../public/coach/document-viewer.js", import.meta.url), "utf8");
const workerSource = fs.readFileSync(new URL("../public/coach/sw.js", import.meta.url), "utf8");
const tick = () => new Promise(setImmediate);
function deferred() {
  let resolve, reject;
  const promise = new Promise((yes, no) => { resolve = yes; reject = no; });
  return { promise, resolve, reject };
}

function viewerHarness() {
  const events = {};
  const nodes = new Map();
  const renders = [];
  const pageCleanups = [];
  const tasks = [];
  let focusRestored = false;
  let lastScroll;
  const node = selector => {
    if (!nodes.has(selector)) nodes.set(selector, {
      hidden: true, disabled: false, style: {}, value: "1", clientWidth: 375,
      textContent: "", addEventListener(type, callback) { this[type] = callback; },
      setAttribute() {}, getContext() { return {}; }
    });
    return nodes.get(selector);
  };
  const dialog = {
    querySelector: node, setAttribute() {}, addEventListener(type, fn) { this[type] = fn; },
    showModal() { this.open = true; }, close() { this.open = false; }
  };
  const history = {
    state: { keep: "existing-state" }, backs: 0,
    pushState(state) { this.state = state; },
    back() { this.backs++; this.state = { keep: "existing-state" }; queueMicrotask(() => events.popstate()); }
  };
  const win = {
    scrollY: 1234, devicePixelRatio: 3, caches: {},
    scrollTo(x, y) { lastScroll = [x, y]; },
    addEventListener(type, callback) { events[type] = callback; }
  };
  const document = {
    body: { style: { cssText: "color: navy" }, append() {} },
    activeElement: { focus() { focusRestored = true; } },
    createElement() { return dialog; }, addEventListener(type, callback) { events[type] = callback; }
  };
  const library = {
    GlobalWorkerOptions: {},
    getDocument(options) {
      const task = { destroyed: false, options, destroy: async () => { task.destroyed = true; } };
      task.promise = Promise.resolve({
        numPages: 3, async getPage(number) {
          return {
            cleanup() { pageCleanups.push(number); },
            getViewport: ({ scale }) => ({ width: 600 * scale, height: 800 * scale }),
            render(options) { renders.push({ number, ...options }); return { promise: Promise.resolve(), cancel() {} }; }
          };
        }
      });
      tasks.push(task);
      return task;
    }
  };
  let cacheWrites = 0;
  const context = vm.createContext({
    window: win, document, history,
    location: { pathname: "/coach/app/", href: "https://example.invalid/coach/app/" },
    AbortController, Uint8Array, URLSearchParams, setTimeout, clearTimeout,
    caches: { open: async () => ({ put: async () => { cacheWrites++; } }) },
    fetch: async () => new Response("%PDF-test", { headers: { "Content-Type": "application/pdf" } }),
    __loadPdfLibrary: async () => library
  });
  // Only the lazy vendor import is replaced; all lifecycle/render code is exercised.
  vm.runInContext(source.replace('import(`${VENDOR}legacy/build/pdf.min.mjs`)', "__loadPdfLibrary()"), context);
  return { api: win.TecDocuments, dialog, node, events, history, document, renders, pageCleanups, tasks,
    get cacheWrites() { return cacheWrites; }, get lastScroll() { return lastScroll; }, get focusRestored() { return focusRestored; } };
}

test("closing during a slow private fetch aborts it, restores the Coach, and never reopens on late data", async () => {
  const h = viewerHarness();
  const wait = deferred();
  let signal;
  const opening = h.api.open({ title: "Lista privada", load: current => { signal = current; return wait.promise; } });
  await tick();
  assert.equal(h.dialog.open, true);
  assert.match(h.node("[data-document-status]").textContent, /Abriendo/);
  h.api.close();
  assert.equal(signal.aborted, true);
  assert.equal(h.dialog.open, false);
  assert.deepEqual(h.lastScroll, [0, 1234]);
  assert.equal(h.focusRestored, true);
  assert.equal(h.document.body.style.cssText, "color: navy");
  assert.equal(h.history.backs, 1);
  wait.resolve(new Blob(["%PDF"]));
  assert.equal(await opening, false);
  assert.equal(h.tasks.length, 0);
});

test("a rejected PIN or failed fetch retains a working exit and never caches private content", async () => {
  const h = viewerHarness();
  assert.equal(await h.api.open({ title: "Privado", load: async () => { throw new Error("Ese PIN no coincide."); } }), false);
  assert.equal(h.dialog.open, true);
  assert.equal(h.node("[data-document-status]").textContent, "Ese PIN no coincide.");
  assert.equal(h.node("[data-document-save]").hidden, true);
  h.node("[data-document-close]").click();
  assert.equal(h.dialog.open, false);
  assert.equal(h.cacheWrites, 0);
});

test("public documents render inside TEC, clamp page numbers and bound the single canvas", async () => {
  const h = viewerHarness();
  assert.equal(await h.api.open({ documentId: "carpeta" }), true);
  assert.equal(h.tasks[0].options.url, "/coach/resources/novel-sales-presentation-2024.pdf");
  assert.equal(h.tasks[0].options.isEvalSupported, false);
  assert.equal(h.history.state.keep, "existing-state");
  assert.equal(h.renders[0].number, 1);
  h.node("[data-document-page]").change({ target: { value: "999" } });
  await tick();
  assert.equal(h.renders.at(-1).number, 3);
  assert.equal(h.node("[data-document-next]").disabled, true);
  h.node("[data-document-zoom]").click();
  await tick();
  const canvas = h.node("canvas");
  assert.ok(canvas.width * canvas.height <= 6_000_000);
  assert.ok(h.pageCleanups.length >= 2);
  h.api.close();
  assert.equal(canvas.width, 0);
  assert.equal(h.tasks[0].destroyed, true);
});

test("device/browser Back closes the document instead of leaving the Coach", async () => {
  const h = viewerHarness();
  await h.api.open({ documentId: "catalogo" });
  h.history.state = null;
  h.events.popstate();
  assert.equal(h.dialog.open, false);
  assert.equal(h.history.backs, 0);
});

test("a late old open cannot replace a new document after rapid close and reopen", async () => {
  const h = viewerHarness();
  const wait = deferred();
  const old = h.api.open({ title: "Viejo", load: () => wait.promise });
  await tick();
  h.api.close();
  assert.equal(await h.api.open({ documentId: "carpeta" }), true);
  wait.resolve(new Blob(["%PDF-old"]));
  assert.equal(await old, false);
  assert.equal(h.dialog.open, true);
  assert.equal(h.tasks.length, 1);
  assert.equal(h.node("#tec-document-title").textContent, "Carpeta de presentación Novel");
});

function workerHarness(cached) {
  const handlers = {};
  const writes = [];
  let network = 0;
  const context = vm.createContext({
    self: { location: { origin: "https://example.invalid" }, addEventListener(type, fn) { handlers[type] = fn; } },
    importScripts() {}, URL, Response,
    caches: { open: async () => ({ match: async () => cached, put: async (...args) => writes.push(args) }) },
    fetch: async () => { network++; return new Response("partial", { status: 206 }); }
  });
  vm.runInContext(workerSource, context);
  return {
    async fetch(mode) {
      let result;
      handlers.fetch({ request: { method: "GET", mode, url: "https://example.invalid/coach/resources/novel-catalog-digital-3.pdf" }, respondWith(promise) { result = promise; } });
      return await result;
    }, writes, get network() { return network; }
  };
}

test("PDF range responses pass through without being cached as complete files", async () => {
  const h = workerHarness();
  assert.equal((await h.fetch("cors")).status, 206);
  assert.equal(h.writes.length, 0);
});

test("an explicitly saved complete PDF is served without network, including offline", async () => {
  const h = workerHarness(new Response("complete PDF"));
  assert.equal(await (await h.fetch("cors")).text(), "complete PDF");
  assert.equal(h.network, 0);
});

test("old raw-PDF navigation redirects to an HTML viewer with its own return link", async () => {
  const h = workerHarness();
  const response = await h.fetch("navigate");
  assert.equal(response.status, 302);
  assert.equal(response.headers.get("Location"), "https://example.invalid/coach/document.html?document=catalogo");
  const html = fs.readFileSync(new URL("../public/coach/document.html", import.meta.url), "utf8");
  assert.match(html, /href="\/coach\/app\/">← Volver al Coach/);
});
