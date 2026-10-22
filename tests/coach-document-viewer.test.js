import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";
import vm from "node:vm";
import crypto from "node:crypto";

const source = fs.readFileSync(new URL("../public/coach/document-viewer.js", import.meta.url), "utf8");
const workerSource = fs.readFileSync(new URL("../public/coach/sw.js", import.meta.url), "utf8");
const tick = () => new Promise(setImmediate);
function deferred() {
  let resolve, reject;
  const promise = new Promise((yes, no) => { resolve = yes; reject = no; });
  return { promise, resolve, reject };
}

function viewerHarness({ userAgent = "" } = {}) {
  const events = {};
  const nodes = new Map();
  const renders = [];
  const pageCleanups = [];
  const tasks = [];
  let focusRestored = false;
  let lastScroll;
  let libraryLoads = 0;
  let failImages = false;
  const requests = [];
  const cached = new Map();
  const manifest = { version: 1, documents: Object.fromEntries(["catalogo", "carpeta"].map(id => [id, { pages: [1, 2, 3].map(number => ({
    url: `/coach/assets/documents/${id}-v1/page-${String(number).padStart(2, "0")}.jpg`, width: 1600, height: 1200
  })) }])) };
  const node = selector => {
    if (!nodes.has(selector)) nodes.set(selector, {
      hidden: true, disabled: false, style: {}, value: "1", clientWidth: 375,
      textContent: "", addEventListener(type, callback) { this[type] = callback; },
      setAttribute() {}, getContext() { return {}; }, replaceChildren(...children) { this.children = children; }
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
    createElement(tag) {
      if (tag === "dialog") return dialog;
      const image = { style: {}, removeAttribute() { this.url = ""; } };
      Object.defineProperty(image, "src", { get() { return this.url; }, set(url) {
        this.url = url;
        queueMicrotask(() => { if (failImages) this.onerror?.(); else this.onload?.(); });
      } });
      return image;
    }, addEventListener(type, callback) { events[type] = callback; }
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
    navigator: { userAgent },
    window: win, document, history,
    location: { pathname: "/coach/app/", href: "https://example.invalid/coach/app/" },
    AbortController, Uint8Array, URLSearchParams, setTimeout, clearTimeout,
    Response,
    caches: { open: async () => ({ match: async key => cached.get(key), put: async (key, response) => { cacheWrites++; cached.set(key, response); } }) },
    fetch: async url => {
      requests.push(url);
      return url.endsWith(".json") ? Response.json(manifest) : new Response("jpeg", { headers: { "Content-Type": "image/jpeg" } });
    },
    __loadPdfLibrary: async () => { libraryLoads++; return library; }
  });
  // Only the lazy vendor import is replaced; all lifecycle/render code is exercised.
  vm.runInContext(source.replace('import(`${VENDOR}legacy/build/pdf.min.mjs`)', "__loadPdfLibrary()"), context);
  return { api: win.TecDocuments, dialog, node, events, history, document, renders, pageCleanups, tasks, requests, cached,
    get libraryLoads() { return libraryLoads; }, set failImages(value) { failImages = value; },
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

test("old iPhone private documents show a safe alternative without requesting PIN bytes, while public pages still work", async () => {
  for (const userAgent of ["Mozilla/5.0 (iPhone; CPU iPhone OS 16_7 like Mac OS X) AppleWebKit/605.1.15 Version/16.6 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 16_7 like Mac OS X) AppleWebKit/605.1.15 CriOS/130.0 Mobile/15E148 Safari/604.1"]) {
    const h = viewerHarness({ userAgent }); let loaded = false;
    assert.equal(await h.api.open({ title: "Privado", load: async () => { loaded = true; return new Blob(["pdf"]); } }), false);
    assert.equal(loaded, false); assert.equal(h.cacheWrites, 0); assert.equal(h.libraryLoads, 0);
    assert.match(h.node("[data-document-status]").textContent, /misma cuenta y PIN/);
    h.api.close();
    assert.equal(await h.api.open({ documentId: "catalogo" }), true);
    assert.equal(h.libraryLoads, 0);
  }
});

test("private PIN dialog clears the secret and restores focus on submit and cancellation without storing it", async () => {
  const h = viewerHarness();
  const first = h.api.requestPin();
  assert.equal(h.dialog.open, true);
  h.node("[data-private-pin]").value = "1234";
  h.node("[data-private-pin-form]").submit({ preventDefault() {} });
  assert.equal(await first, "1234");
  assert.equal(h.node("[data-private-pin]").value, "");
  assert.equal(h.dialog.open, false); assert.equal(h.focusRestored, true);
  const next = h.api.requestPin();
  assert.equal(await h.api.requestPin(), null);
  h.node("[data-private-pin]").value = "9999";
  h.node("[data-private-pin-cancel]").click();
  assert.equal(await next, null);
  assert.equal(h.node("[data-private-pin]").value, "");
  assert.equal(h.cacheWrites, 0); assert.equal(h.requests.length, 0);
});

test("public documents use one bounded image and never load PDF.js, a worker or the full PDF", async () => {
  const h = viewerHarness();
  assert.equal(await h.api.open({ documentId: "carpeta" }), true);
  assert.equal(h.libraryLoads, 0);
  assert.equal(h.tasks.length, 0);
  assert.equal(h.requests.some(url => url.endsWith(".pdf")), false);
  assert.equal(h.history.state.keep, "existing-state");
  assert.match(h.node("[data-document-image]").children[0].src, /page-01.jpg$/);
  h.node("[data-document-page]").change({ target: { value: "999" } });
  await tick();
  assert.match(h.node("[data-document-image]").children[0].src, /page-03.jpg$/);
  assert.equal(h.node("[data-document-next]").disabled, true);
  h.node("[data-document-zoom]").click();
  await tick();
  const images = h.node("[data-document-image]").children;
  assert.equal(images.length, 1);
  assert.ok(images[0].width * images[0].height <= 2_560_000);
  assert.equal(images[0].style.width, "710px");
  assert.equal(h.renders.length, 0);
  h.api.close();
  assert.equal(h.node("canvas").width, 0);
  assert.equal(h.node("[data-document-image]").children.length, 0);
});

test("private PDFs keep their memory-only renderer and release its canvas on close", async () => {
  const h = viewerHarness();
  assert.equal(await h.api.open({ title: "Privado", load: async () => new Blob(["%PDF-test"]) }), true);
  assert.equal(h.libraryLoads, 1);
  assert.ok(h.tasks[0].options.data instanceof Uint8Array);
  assert.equal(h.tasks[0].options.isEvalSupported, false);
  assert.equal(h.node("[data-document-save]").hidden, true);
  h.api.close();
  assert.equal(h.tasks[0].destroyed, true);
  assert.equal(h.node("canvas").width, 0);
  assert.equal(h.cacheWrites, 0);
});

test("page image failures offer retry without leaving the app or requiring a PDF engine", async () => {
  const h = viewerHarness();
  h.failImages = true;
  await h.api.open({ documentId: "catalogo" });
  assert.equal(h.node("[data-document-retry]").hidden, false);
  assert.match(h.node("[data-document-status]").textContent, /Reintentar/);
  h.failImages = false;
  h.node("[data-document-retry]").click();
  await tick();
  assert.equal(h.node("[data-document-retry]").hidden, true);
  assert.match(h.node("[data-document-status]").textContent, /Página 1 de 3/);
  assert.equal(h.libraryLoads, 0);
});

test("offline save confirms completion only after all page images and the manifest are stored", async () => {
  const h = viewerHarness();
  await h.api.open({ documentId: "catalogo" });
  h.node("[data-document-save]").click();
  await tick();
  assert.equal(h.cacheWrites, 5); // manifest + 3 images + completion marker
  assert.match(h.node("[data-document-status]").textContent, /Todas las páginas están guardadas/);
  assert.equal(h.requests.some(url => url.endsWith(".pdf")), false);
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
  assert.equal(h.tasks.length, 0);
  assert.equal(h.node("#tec-document-title").textContent, "Carpeta de presentación Novel");
});

test("every public PDF page has an ordered, complete and memory-bounded JPEG asset", () => {
  const base = new URL("../public", import.meta.url);
  const manifest = JSON.parse(fs.readFileSync(new URL("../public/coach/assets/documents/manifest-v1.json", import.meta.url), "utf8"));
  for (const [id, count] of [["catalogo", 42], ["carpeta", 50]]) {
    const pages = manifest.documents[id].pages;
    assert.equal(pages.length, count);
    pages.forEach((page, index) => {
      assert.equal(page.url, `/coach/assets/documents/${id}-v1/page-${String(index + 1).padStart(2, "0")}.jpg`);
      const bytes = fs.readFileSync(new URL(`.${page.url}`, `${base.href}/`));
      assert.equal(bytes.length, page.bytes);
      assert.equal(crypto.createHash("sha256").update(bytes).digest("hex"), page.sha256);
      assert.ok(page.width > 0 && page.height > 0 && page.width * page.height <= 2_560_000);
    });
  }
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
