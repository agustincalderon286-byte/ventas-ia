import test from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import vm from "node:vm";

const source = readFileSync(new URL("../public/coach/offline-forms.js", import.meta.url), "utf8");
const tick = () => new Promise(setImmediate);
async function settle() { await tick(); await tick(); }
const user = { id: "one", email: "one@example.invalid" };
function memory() {
  const stores = { cache: new Map(), outbox: new Map() };
  let serial = 0;
  const storage = {
    stores, viewerValue: { user }, failPut: false, failQueue: false, hold: null,
    id: () => `form-request-${++serial}`,
    async get(store, key) { return structuredClone(stores[store].get(key)); },
    async put(store, value) { if (storage.hold) await storage.hold; if (storage.failPut) throw new Error("quota"); stores[store].set(value.key || value.id, structuredClone(value)); },
    async all(store) { return structuredClone([...stores[store].values()]); },
    async remove(store, key) { stores[store].delete(key); },
    async viewer() { return storage.viewerValue; },
    async queue(item) {
      if (storage.failQueue) throw new Error("No space");
      stores.outbox.set(item.id, structuredClone({ ...item, ownerEmail: storage.viewerValue.user.email, createdAt: "2026-10-22T12:00:00Z" }));
      if (stores.cache.get(item.draftKey)?.value.revision === item.formSnapshot.revision) stores.cache.delete(item.draftKey);
    },
    async flush() { return {}; }
  };
  return storage;
}
function harness(storage, options = {}) {
  const node = tag => ({ tag, children: [], dataset: {}, attributes: {}, hidden: false, textContent: "", listeners: {},
    setAttribute(key, value) { this.attributes[key] = value; }, append(...children) { this.children.push(...children); },
    replaceChildren(...children) { this.children = children; }, addEventListener(type, handler) { this.listeners[type] = handler; } });
  const form = node("form");
  form.elements = [ { name: "fullName", value: "", type: "text" }, { name: "phone", value: "", type: "tel" },
    { name: "materials", value: "Acero", type: "checkbox", checked: false }, { name: "materials", value: "Aluminio", type: "checkbox", checked: false } ];
  const events = {};
  let house = "house-one", synced = 0;
  class FormData {
    constructor(form) { this.entries = form.elements.filter(field => !["checkbox", "radio"].includes(field.type) || field.checked).map(field => [field.name, field.value]); }
    *[Symbol.iterator]() { yield* this.entries; }
  }
  const ctx = vm.createContext({ window: { addEventListener: (type, handler) => { events[type] = handler; } },
    document: { createElement: node }, FormData });
  vm.runInContext(source, ctx);
  const controller = ctx.window.TecOfflineForms.attach(form, { user: options.user || user, kind: options.kind || "health-survey", storage,
    collect: () => ({ fullName: form.elements[0].value, phone: form.elements[1].value, activeCrmRecordId: house }),
    contextFields: ["activeCrmRecordId"], onSynced: () => { synced++; } });
  return { controller, form, events, status: form.children[0], pending: form.children[1], set house(value) { house = value; },
    get synced() { return synced; }, input(name, value) { form.elements.find(field => field.name === name).value = value; form.listeners.input(); } };
}

test("a draft survives a reload with checkboxes and its original visit context", async () => {
  const storage = memory(); const h = harness(storage); await h.controller.ready;
  h.input("fullName", "Fictitious house"); h.input("phone", "2025550101");
  h.form.elements[2].checked = true; h.form.listeners.change(); await settle();
  assert.match(h.status.textContent, /Borrador guardado/);
  const reloaded = harness(storage); reloaded.house = "house-two"; await reloaded.controller.ready;
  assert.equal(reloaded.form.elements[0].value, "Fictitious house");
  assert.equal(reloaded.form.elements[2].checked, true);
  const result = await reloaded.controller.submit();
  assert.equal(result.offlineQueued, true);
  assert.equal([...storage.stores.outbox.values()][0].body.activeCrmRecordId, "house-one");
  assert.equal([...storage.stores.outbox.values()][0].body.accountId, "one");
  assert.equal(storage.stores.cache.size, 0);
});

test("another account cannot read or send a draft or its pending copies", async () => {
  const storage = memory(); const first = harness(storage); await first.controller.ready;
  first.input("fullName", "Private fictitious record"); await settle(); await first.controller.submit();
  const second = harness(storage, { user: { id: "two", email: "two@example.invalid" } }); await second.controller.ready;
  assert.equal(second.form.elements[0].value, "");
  assert.equal(second.pending.hidden, true);
  await assert.rejects(second.controller.submit(), /cuenta cambió/);
  storage.viewerValue = { user: { id: "two", email: "two@example.invalid" } };
  await assert.rejects(first.controller.submit(), /cuenta cambió/);
  assert.equal(storage.stores.outbox.size, 1);
});

test("an IndexedDB delay never announces saved early and quota failure retains the current fields", async () => {
  const storage = memory(); const h = harness(storage); await h.controller.ready;
  let release; storage.hold = new Promise(resolve => { release = resolve; });
  h.input("fullName", "Not committed yet"); await tick();
  assert.match(h.status.textContent, /Guardando borrador/);
  assert.equal(storage.stores.cache.size, 0);
  storage.failPut = true; release(); await settle();
  assert.equal(h.status.dataset.state, "error");
  assert.equal(h.form.elements[0].value, "Not committed yet");
  await assert.rejects(h.controller.submit(), /quota/);
  assert.equal(storage.stores.outbox.size, 0);
});

test("a failed queue does not clear the draft or pretend the server received it", async () => {
  const storage = memory(); const h = harness(storage); await h.controller.ready;
  h.input("fullName", "Kept after failure"); await settle(); storage.failQueue = true;
  await assert.rejects(h.controller.submit(), /No space/);
  assert.equal(storage.stores.cache.size, 1);
  assert.equal(h.controller.hasDraft(), true);
  assert.equal(h.form.elements[0].value, "Kept after failure");
});

test("a queued copy survives reload, and a receipt confirms it without overwriting a new draft", async () => {
  const storage = memory(); const h = harness(storage); await h.controller.ready;
  h.input("fullName", "First house"); await settle(); const result = await h.controller.submit();
  const reloaded = harness(storage); await reloaded.controller.ready;
  assert.equal(reloaded.form.elements[0].value, "");
  assert.equal(reloaded.pending.hidden, false);
  assert.match(reloaded.pending.children[1].textContent, /First house/);
  reloaded.input("fullName", "Second house"); await settle();
  const item = storage.stores.outbox.get(result.requestId);
  storage.stores.cache.set("receipt", { key: `form-result:${user.email}:${item.id}`, ownerEmail: user.email,
    kind: item.kind, requestId: item.id, body: item.body, value: { survey: { id: "saved" } } });
  storage.stores.outbox.delete(item.id);
  reloaded.events["tec:offline-status"](); await settle();
  assert.equal(reloaded.synced, 1);
  assert.equal(reloaded.form.elements[0].value, "Second house");
  assert.equal(reloaded.controller.hasDraft(), true);
  assert.equal(reloaded.pending.hidden, true);
});

test("validation rejections can be recovered for correction, but uncertain writes cannot be resent as new", async () => {
  const storage = memory(); const h = harness(storage); await h.controller.ready;
  h.input("fullName", "Correct me"); await settle(); const result = await h.controller.submit();
  const item = storage.stores.outbox.get(result.requestId); item.lastStatus = 400; item.lastError = "Invalid phone";
  h.events["tec:offline-status"](); await settle();
  const button = h.pending.children.find(child => child.tag === "button"); assert.ok(button);
  await button.listeners.click();
  assert.equal(h.form.elements[0].value, "Correct me");
  assert.equal(storage.stores.outbox.size, 0);
  assert.equal(storage.stores.cache.size, 1);
  const corrected = [...storage.stores.cache.values()][0].value;
  assert.notEqual(corrected.requestId, result.requestId);
  item.lastStatus = 409; storage.stores.outbox.set(item.id, item);
  h.events["tec:offline-status"](); await settle();
  assert.equal(h.pending.children.some(child => child.tag === "button"), false);
});
