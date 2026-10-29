import test from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import vm from "node:vm";
import { createPropertyVisitService, registerCoachPropertyVisitRoutes, validatePropertySnapshot } from "../src/coach-property-visits.js";
import { propertyVisitKey, mergePropertyVisits, propertyVisitFromQueuedItem } from "../public/coach/property-visit-helpers.js";
import { memoryPropertyVisitModel } from "./helpers/property-visit-model.js";

const property = { pin: "00000000000001", address: "100 TEST ST", city: "HILLSIDE", zipCode: "60162", lat: 41.87, lon: -87.9 };
const owner = "000000000000000000000001", other = "000000000000000000000002";
const now = () => Date.parse("2026-10-22T12:00:00Z");
const payload = (extra = {}) => ({ property, status: "not_answered", notes: "No contestaron.", clientUpdatedAt: "2026-10-22T10:00:00Z", ...extra });
function service() { const model = memoryPropertyVisitModel(); return { model, service: createPropertyVisitService({ model, now, toOwnerId: String }) }; }

test("house keys survive changes in PIN or missing ZIP and keep different units separate", () => {
  assert.equal(propertyVisitKey(property), propertyVisitKey({ ...property, pin: "00000000000002", zipCode: "", address: "100 test st." }));
  assert.notEqual(propertyVisitKey(property), propertyVisitKey({ ...property, address: "100 TEST ST UNIT 2" }));
  assert.equal(validatePropertySnapshot({ ...property, ownerUserId: other, ownerName: "Not a resident" }).ownerName, undefined);
  assert.throws(() => validatePropertySnapshot({ ...property, lat: null }), { status: 400 });
});

test("visits are private per authenticated user even when both visit the same house", async () => {
  const { service: visits } = service();
  await visits.save(owner, payload(), "property-visit-001");
  assert.equal((await visits.list(owner)).length, 1);
  assert.deepEqual(await visits.list(other), []);
  await visits.save(other, payload({ notes: "Different private note" }), "property-visit-001");
  assert.equal((await visits.list(owner))[0].notes, "No contestaron.");
  assert.equal((await visits.list(other))[0].notes, "Different private note");
});

test("retries create one event and reusing a request for another house is rejected", async () => {
  const { service: visits, model } = service();
  await visits.save(owner, payload(), "property-visit-001");
  await visits.save(owner, payload(), "property-visit-001");
  assert.equal(model.events.length, 1);
  await assert.rejects(visits.save(owner, payload({ property: { ...property, address: "102 TEST ST" } }), "property-visit-001"), { status: 409 });
});

test("late offline saves retain history and do not replace a more recent result", async () => {
  const { service: visits, model } = service();
  await visits.save(owner, payload({ status: "answered", notes: "They answered later.", clientUpdatedAt: "2026-10-22T11:00:00Z" }), "property-visit-new");
  const current = await visits.save(owner, payload(), "property-visit-old");
  assert.equal(model.events.length, 2);
  assert.equal(current.status, "answered");
  assert.equal(current.notes, "They answered later.");
});

test("return visits and appointments require a valid date; other outcomes clear it", async () => {
  const { service: visits } = service();
  await assert.rejects(visits.save(owner, payload({ status: "return_later" }), "property-visit-bad"), { status: 400 });
  const result = await visits.save(owner, payload({ status: "appointment", nextVisitAt: "2026-10-25T20:00:00Z" }), "property-visit-good");
  assert.equal(result.nextVisitAt, "2026-10-25T20:00:00.000Z");
  const cleared = await visits.save(owner, payload({ status: "do_not_visit", clientUpdatedAt: "2026-10-22T11:00:00Z", nextVisitAt: "2026-10-25T20:00:00Z" }), "property-visit-clear");
  assert.equal(cleared.nextVisitAt, null);
  await assert.rejects(visits.save(owner, payload({ status: "arbitrary" }), "property-visit-003"), { status: 400 });
  await assert.rejects(visits.save(owner, payload({ notes: "x".repeat(2001) }), "property-visit-004"), { status: 400 });
  await assert.rejects(visits.list(owner, { $ne: null }), { status: 400 });
});

test("lead creation uses contact fields, preserves privacy, and is idempotent", async () => {
  const { service: visits } = service();
  await visits.save(owner, payload(), "property-visit-001");
  let calls = 0;
  const createLead = async (user, data) => {
    calls++;
    assert.equal(String(user._id), owner);
    assert.equal(data.fullName, "Test Contact");
    assert.equal(data.offlineRequestId, "property-lead-001");
    assert.equal(data.address, property.address);
    return { lead: { id: "000000000000000000000099" } };
  };
  const input = { property, fullName: "Test Contact", phone: "2025550143", clientUpdatedAt: "2026-10-22T11:00:00Z" };
  const first = await visits.convertToLead({ _id: owner }, input, "property-lead-001", createLead);
  assert.ok(first.visit.linkedLeadId);
  const retry = await visits.convertToLead({ _id: owner }, input, "property-lead-001", createLead);
  assert.equal(retry.leadId, first.leadId);
  const another = await visits.convertToLead({ _id: owner }, input, "property-lead-002", createLead);
  assert.equal(another.duplicate, true);
  assert.equal(calls, 1);
  await visits.save(owner, payload({ notes: "Later visit", clientUpdatedAt: "2026-10-22T11:30:00Z" }), "property-visit-003");
  assert.equal((await visits.list(owner))[0].linkedLeadId, first.leadId);
  await assert.rejects(visits.convertToLead({ _id: other }, input, "property-lead-001", createLead), { status: 400 });
});

test("local pending notes overlay older server values and can be recovered from the durable outbox", () => {
  const key = propertyVisitKey(property);
  const pending = propertyVisitFromQueuedItem({ kind: "property-visit", id: "property-visit-local", body: payload({ clientUpdatedAt: "2026-10-22T11:00:00.000Z" }) });
  const remote = { ...payload({ clientUpdatedAt: "2026-10-22T10:00:00.000Z" }), propertyKey: key, linkedLeadId: "existing-lead" };
  const [merged] = mergePropertyVisits([remote], [pending]);
  assert.equal(merged.pending, true);
  assert.equal(merged.linkedLeadId, "existing-lead");
  assert.equal(merged.requestId, "property-visit-local");
  const newerRemote = { ...remote, clientUpdatedAt: "2026-10-22T11:30:00.000Z" };
  assert.equal(mergePropertyVisits([newerRemote], [pending])[0].pending, false);
  assert.equal(propertyVisitFromQueuedItem({ kind: "lead" }), null);
});

test("write routes reject missing sessions and queued writes belonging to another account", async () => {
  const handlers = new Map(); let authenticated = false, writes = 0;
  registerCoachPropertyVisitRoutes({ get: (url, handler) => handlers.set(`GET ${url}`, handler), post: (url, handler) => handlers.set(`POST ${url}`, handler) }, {
    requireCoachActivo: async (req, res) => authenticated ? { user: { _id: owner } } : (res.status(401).json({ error: "login" }), null),
    service: { save: async () => { writes++; return {}; } }
  });
  const run = async accountId => {
    const res = { code: 200, headers: {}, set(key, value) { this.headers[key] = value; return this; }, status(code) { this.code = code; return this; }, json(body) { this.body = body; return this; } };
    await handlers.get("POST /api/coach/property-visits")({ method: "POST", path: "/api/coach/property-visits", body: { accountId }, headers: {} }, res);
    return res;
  };
  assert.equal((await run(owner)).code, 401);
  authenticated = true;
  assert.equal((await run(other)).code, 403);
  assert.equal((await run("")).code, 403);
  assert.equal(writes, 0);
  assert.equal((await run(owner)).code, 200);
  assert.equal(writes, 1);
});

test("IndexedDB writes report success only after commit, and aborts reject the save", async () => {
  const source = readFileSync(new URL("../public/coach/app.js", import.meta.url), "utf8");
  const body = source.slice(source.indexOf("async function tecOfflineStoreRequest"), source.indexOf("const tecOfflinePut"));
  const tx = new EventTarget(), request = new EventTarget();
  request.result = "stored";
  tx.objectStore = () => ({ put: () => request });
  const write = vm.runInNewContext(`(${body.trim()})`, { openTecOfflineDb: async () => ({ transaction: () => tx }), Error });
  let resolved = false;
  const saving = write("cache", "readwrite", store => store.put({})).then(value => { resolved = true; return value; });
  await new Promise(setImmediate);
  request.dispatchEvent(new Event("success"));
  await new Promise(setImmediate);
  assert.equal(resolved, false);
  tx.dispatchEvent(new Event("complete"));
  assert.equal(await saving, "stored");
  const failing = write("cache", "readwrite", store => store.put({}));
  await new Promise(setImmediate);
  tx.error = new Error("Storage quota reached");
  tx.dispatchEvent(new Event("abort"));
  await assert.rejects(failing, /Storage quota reached/);
});

test("queue-first saves persist before returning and never report success when queuing fails", async () => {
  const source = readFileSync(new URL("../public/coach/app.js", import.meta.url), "utf8");
  const body = source.slice(0, source.indexOf("const COACH_CHAT_API_URL"));
  let queued = null, fail = false;
  const api = vm.runInNewContext(`(${body.trim()})`, {
    createTecOfflineId: () => "property-visit-test",
    queueTecOfflineRequest: async item => { if (fail) throw new Error("No storage"); queued = item; },
    fetch: () => { throw new Error("Must not send before durable queue"); }
  });
  const options = { method: "POST", body: { accountId: owner }, offlineQueue: true, queueFirst: true, offlineKind: "property-visit" };
  const result = await api("/api/coach/property-visits", options);
  assert.equal(result.offlineQueued, true);
  assert.equal(queued.body.accountId, owner);
  assert.equal(queued.headers["X-TEC-Request-ID"], "property-visit-test");
  fail = true;
  await assert.rejects(api("/api/coach/property-visits", options), /No storage/);
});

test("offline sync never sends another account's data and sends a tied visit before its lead", async () => {
  const source = readFileSync(new URL("../public/coach/app.js", import.meta.url), "utf8");
  const body = source.slice(source.indexOf("async function flushTecOfflineOutbox"), source.indexOf("function initTecOfflineStatus"));
  const shared = { createdAt: "2026-10-22T10:00:00Z", ownerEmail: "one@example.invalid", method: "POST", body: {} };
  const items = [{ ...shared, id: "lead", kind: "property-lead", url: "/lead" }, { ...shared, id: "other", ownerEmail: "other@example.invalid", url: "/private-other" }, { ...shared, id: "visit", kind: "property-visit", url: "/visit" }, { ...shared, id: "unknown", ownerEmail: "", url: "/unknown" }];
  const sent = [], removed = [];
  const sync = vm.runInNewContext(`(${body.trim()})`, {
    navigator: { onLine: true }, tecOutboxFlushPromise: null,
    getTecCachedViewer: async () => ({ user: { email: "one@example.invalid" } }),
    tecOfflineGetAll: async () => items,
    tecOfflineDelete: async (_store, id) => removed.push(id), tecOfflinePut: async () => {},
    fetch: async url => { sent.push(url); return { ok: true, status: 200 }; },
    syncTecOfflineRecordings: async () => ({ synced: 0 }), getTecOfflinePendingCount: async () => 2,
    window: { dispatchEvent() {} }, CustomEvent: class {}
  });
  await sync();
  assert.deepEqual(sent, ["/visit", "/lead"]);
  assert.deepEqual(removed, ["visit", "lead"]);
});

test("large map searches load notes for all 200 houses without exceeding the lookup limit", async () => {
  const source = readFileSync(new URL('../public/coach/property-map.js', import.meta.url), 'utf8');
  const body = source.slice(source.indexOf('    async function refreshVisits()'), source.indexOf('    function kickSync()'));
  const keysRequested = [], status = {};
  const properties = Array.from({length:200}, (_,i)=>({...property,address:`${200+i} TEST ST`,pin:String(i+2).padStart(14,'0')}));
  const context = vm.createContext({refreshPromise:null,navigator:{onLine:true},accountId:owner,data:{anchor:property,properties},visits:new Map(),pendingLeadKeys:new Set(),editingProperty:null,dialog:{open:false},awaitingSync:false,
    request:async (url,options)=>{
      if(url.endsWith('/lookup')) {
        assert.ok(options.body.keys.length <= 61); keysRequested.push(...options.body.keys);
        return {visits:options.body.keys.map(key=>({propertyKey:key,clientUpdatedAt:'2026-10-28T12:00:00Z',status:'not_answered',notes:'Saved note'}))};
      }
      return {visits:[]};
    },pendingItems:async()=>[],propertyVisitKey,propertyVisitFromQueuedItem,mergePropertyVisits,find:()=>status,rememberVisits:async()=>{},refreshVisitDecorations:()=>{},syncLeadButton:()=>{}});
  await vm.runInContext(body+'\nrefreshVisits();',context);
  assert.equal(keysRequested.length,201); assert.equal(new Set(keysRequested).size,201);
  assert.equal(context.visits.size,201);
  assert.equal(context.visits.get(propertyVisitKey(properties[199])).notes,'Saved note');
  assert.match(status.textContent,/privadas de tu cuenta/);
});
