import test from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import vm from "node:vm";

const app = readFileSync(new URL("../public/coach/app.js", import.meta.url), "utf8");
const sw = readFileSync(new URL("../public/coach/sw.js", import.meta.url), "utf8");
const extract = (source, from, to) => source.slice(source.indexOf(from), source.indexOf(to));
const tick = () => new Promise(setImmediate);
function transaction(getValue = null) {
  const tx = new EventTarget(), request = new EventTarget(), writes = [];
  request.result = getValue;
  tx.objectStore = name => ({ get() { return request; }, put(value) { writes.push(["put", name, value]); }, delete(key) { writes.push(["delete", name, key]); } });
  return { tx, request, writes, db: { transaction: () => tx } };
}
const item = { id: "request-one", ownerEmail: "one@example.invalid", kind: "health-survey", body: { accountId: "one" } };

for (const [label, source, start, end] of [
  ["page", app, "async function completeTecOfflineForm", "async function recordTecOfflineFormError"],
  ["worker", sw, "async function completeOfflineForm", "async function recordOfflineFormError"]
]) {
  test(`${label} commits the receipt and outbox removal together, and reports no success on abort`, async () => {
    const h = transaction();
    const complete = vm.runInNewContext(`(${extract(source, start, end).trim()})`, { openTecOfflineDb: async () => h.db });
    let resolved = false;
    const saving = complete(item, { survey: { id: "saved" } }).then(() => { resolved = true; });
    await tick();
    assert.equal(resolved, false);
    assert.equal(h.writes[0][1], "cache");
    assert.equal(h.writes[0][2].key, "form-result:one@example.invalid:request-one");
    assert.deepEqual(h.writes[1], ["delete", "outbox", "request-one"]);
    h.tx.dispatchEvent(new Event("complete")); await saving;
    const failing = complete(item, {}); await tick();
    h.tx.error = new Error("quota"); h.tx.dispatchEvent(new Event("abort"));
    await assert.rejects(failing, /quota/);
  });
}

test("queueing replaces only the submitted draft revision in the same transaction", async () => {
  for (const revision of [1, 2]) {
    const h = transaction({ value: { revision } }); let notified = false;
    const queue = vm.runInNewContext(`(${extract(app, "async function queueTecOfflineRequest", "async function getTecOfflinePendingCount").trim()})`, {
      getTecCachedViewer: async () => ({ user: { id: "one", email: "one@example.invalid" } }), openTecOfflineDb: async () => h.db,
      window: { dispatchEvent() { notified = true; } }, navigator: {}, CustomEvent: class {}
    });
    const saving = queue({ ...item, draftKey: "draft", formSnapshot: { revision: 1 } });
    await tick(); h.request.dispatchEvent(new Event("success"));
    assert.equal(notified, false);
    assert.equal(h.writes.some(write => write[0] === "delete"), revision === 1);
    h.tx.dispatchEvent(new Event("complete")); await saving;
    assert.equal(notified, true);
    assert.equal(h.writes[0][2].body.accountId, "one");
  }
});

test("a late failed synchronizer cannot resurrect an already confirmed form", async () => {
  for (const [source, start, end] of [
    [app, "async function recordTecOfflineFormError", "async function flushTecOfflineOutbox"],
    [sw, "async function recordOfflineFormError", "async function broadcastOfflineStatus"]
  ]) {
    const h = transaction(null);
    const record = vm.runInNewContext(`(${extract(source, start, end).trim()})`, { openTecOfflineDb: async () => h.db });
    const saving = record(item, new Error("late error"));
    await tick(); h.request.dispatchEvent(new Event("success")); h.tx.dispatchEvent(new Event("complete")); await saving;
    assert.equal(h.writes.length, 0);
  }
});
