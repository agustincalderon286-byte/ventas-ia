import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";
import vm from "node:vm";

const read = path => fs.readFileSync(new URL(`../${path}`, import.meta.url), "utf8");
const app = read("public/coach/app.js"), helper = read("public/coach/ui-coherence.js");
const html = read("private/coach-app.html");
function ui() { const window = {}; vm.runInNewContext(helper, { window }); return window.TecUi; }
const tick = () => new Promise(setImmediate);

test("billing controls distinguish permanent tests, team seats, trialing and actual Stripe customers", () => {
  const policy = ui().billingState;
  assert.equal(policy({ subscriptionStatus: "test_access", managesTeam: true }).portal, false);
  assert.match(policy({ subscriptionStatus: "test_access" }).label, /sin vencimiento/);
  assert.equal(policy({ subscriptionStatus: "test_access" }).period, "Sin vencimiento");
  assert.equal(policy({ accountType: "seat", managesTeam: true, stripeCustomerId: "cus_parent" }).portal, false);
  assert.match(policy({ accountType: "seat", accessGranted: true }).label, /equipo/);
  assert.match(policy({ subscriptionStatus: "trialing" }).label, /temporal/);
  assert.equal(policy({ managesTeam: true, stripeCustomerId: "cus_self", subscriptionActive: true }).portal, true);
  // A real historical Stripe customer must retain cancellation access even if current access is a test grant.
  assert.equal(policy({ managesTeam: true, stripeCustomerId: "cus_self", subscriptionStatus: "test_access" }).portal, true);
  assert.equal(policy({ managesTeam: true, subscriptionStatus: "inactive" }).portal, false);
});

test("flow order changes the actual DOM, not only the visual CSS order, and keeps equal orders stable", () => {
  const child = (name, value, card = true) => ({ name, style: { getPropertyValue: () => value }, classList: { contains: () => card } });
  const container = { children: [child("tabs", "", false), child("6", "60"), child("3", "30"), child("5", "50"), child("a", ""), child("b", "")],
    append(node) { this.children = this.children.filter(item => item !== node); this.children.push(node); } };
  ui().alignFlow(container);
  assert.deepEqual(container.children.map(item => item.name), ["tabs", "3", "5", "6", "a", "b"]);
});

function offerHarness({ account = "A", store = new Map(), visit = { id: "house1", leadName: "Casa ficticia" }, brokenStorage = false } = {}) {
  const events = {}, timers = new Set(), nodes = new Map(); let serial = 0, currentVisit = visit;
  const node = selector => {
    if (!nodes.has(selector)) nodes.set(selector, { value: "", hidden: true, disabled: false, dataset: {}, style: {}, textContent: "",
      addEventListener: function (name, handler) { this[name] = handler; }, setAttribute() {}, focus() {} });
    return nodes.get(selector);
  };
  const root = { querySelector: node }, win = { TecUi: ui(),
    localStorage: { getItem: key => store.get(key) || null, setItem: (key, value) => { if (brokenStorage) throw new Error("quota"); store.set(key, value); }, removeItem: key => store.delete(key) },
    sessionStorage: { getItem: () => null, setItem() {} }, addEventListener: (name, fn) => { events[name] = fn; },
    setInterval: fn => { timers.add(fn); return fn; }, clearInterval: fn => timers.delete(fn) };
  const context = vm.createContext({ window: win, document: { querySelector: selector => selector === "[data-daily-prize-tool]" ? root : node(selector) },
    getActiveCoachCrmContext: () => currentVisit, getActiveCoachHealthSurveyContext: () => null, getActiveCoachProgram414Context: () => null,
    createTecOfflineId: () => `anonymous-${++serial}`, registerCoachDemoEvent() {},
    DAILY_PRIZE_DURATION_MS: 300000, DAILY_PRIZE_DEFAULT_STATE: { status: "idle" },
    DAILY_PRIZE_OFFERS: { "305": { code: "305", title: "Descuento", copy: "Manual", note: "No automático" } } });
  vm.runInContext(app.slice(app.indexOf("function initDailyPrizeTool("), app.indexOf("function initCoachDestinationSettings(")) + `\ninitDailyPrizeTool({id:${JSON.stringify(account)}});`, context);
  return { store, node, events, timers, changeVisit(value) { currentVisit = value; events["coach:visit-change"](); },
    setStorageBroken(value) { brokenStorage = value; },
    activate() { node("[data-daily-prize-code]").value = "305"; node("[data-daily-prize-activate]").click(); } };
}

test("offer activation and claim stay in the current account and visit, ignoring old unowned global state", () => {
  const store = new Map([["agustin-coach-daily-prize", JSON.stringify({ offerCode: "305", status: "claimed" })]]);
  const h = offerHarness({ store });
  assert.equal(h.node("[data-daily-prize-result]").hidden, true);
  h.activate(); h.node("[data-daily-prize-claim]").click();
  assert.match(h.node("[data-daily-prize-feedback]").textContent, /manualmente/);
  assert.equal(JSON.parse(store.get(ui().offerStorageKey("a", "crm:house1"))).status, "claimed");
  h.changeVisit({ id: "house2" });
  assert.equal(h.node("[data-daily-prize-result]").hidden, true);
  h.node("[data-daily-prize-claim]").click();
  assert.equal(store.has(ui().offerStorageKey("a", "crm:house2")), false);
  const other = offerHarness({ account: "B", store });
  assert.equal(other.node("[data-daily-prize-result]").hidden, true);
  h.changeVisit({ id: "house1" });
  assert.match(h.node("[data-daily-prize-timer]").textContent, /Marcada/);
  assert.equal(h.timers.size, 0);
});

test("an anonymous new visit cannot reuse the previous offer and storage failure never claims success", () => {
  const h = offerHarness({ visit: null }); h.activate();
  assert.equal(h.node("[data-daily-prize-result]").hidden, false);
  h.node("[data-daily-prize-new-visit]").click();
  assert.equal(h.node("[data-daily-prize-result]").hidden, true);
  assert.equal(h.timers.size, 0);
  const failed = offerHarness({ brokenStorage: true }); failed.activate();
  assert.match(failed.node("[data-daily-prize-feedback]").textContent, /No quedó confirmada/);
  assert.equal(failed.node("[data-daily-prize-result]").hidden, true);
  assert.equal(failed.store.size, 0);
});

test("expired offers cannot be marked and a failed claim remains active without a false confirmation", () => {
  const h = offerHarness(); h.activate();
  h.setStorageBroken(true); h.node("[data-daily-prize-claim]").click();
  assert.match(h.node("[data-daily-prize-feedback]").textContent, /No quedó confirmada/);
  assert.equal(h.timers.size, 1);
  const key = ui().offerStorageKey("a", "crm:house1");
  assert.equal(JSON.parse(h.store.get(key)).status, "active");
  h.setStorageBroken(false);
  h.store.set(key, JSON.stringify({ ...JSON.parse(h.store.get(key)), expiresAt: "2000-01-01T00:00:00Z" }));
  h.events.storage({ key }); h.node("[data-daily-prize-claim]").click();
  assert.equal(JSON.parse(h.store.get(key)).status, "expired");
  assert.equal(h.node("[data-daily-prize-claim]").disabled, true);
});

test("private resource feedback is outside the collapsible upload forms", () => {
  const forms = [...html.matchAll(/<form[^>]*data-private-resource-form[^>]*>[\s\S]*?<\/form>/g)].map(match => match[0]);
  assert.equal(forms.length, 1);
  forms.forEach(form => assert.doesNotMatch(form, /data-private-resource-feedback/));
  assert.match(html, /data-private-resource-feedback="catalogo_privado" role="status"/);
});

test("the load gate never exposes restricted UI while initialization is pending or failed", async () => {
  let resolve, fail = false;
  const waiting = new Promise(done => { resolve = done; });
  const appPage = { hidden: true, querySelector: () => null }, loading = { hidden: false }, message = {}, retry = { hidden: true, addEventListener() {} };
  const init = vm.runInNewContext(`(${app.slice(app.indexOf("async function initCoachAppPage()"), app.indexOf("async function loadCoachAppPage()")).trim()})`, {
    window: { TecUi: ui() }, document: { querySelector: selector => ({ "[data-coach-app-page]": appPage, "[data-coach-loading]": loading,
      "[data-coach-loading-message]": message, "[data-coach-load-retry]": retry })[selector] },
    loadCoachAppPage: async () => { await waiting; if (fail) throw Error("failed"); }, console: { error() {} }
  });
  const starting = init(); await tick();
  assert.equal(appPage.hidden, true); assert.equal(loading.hidden, false);
  resolve(); await starting;
  assert.equal(appPage.hidden, false); assert.equal(loading.hidden, true);
  fail = true; await init();
  assert.equal(appPage.hidden, true); assert.equal(loading.hidden, false); assert.equal(retry.hidden, false);
  assert.match(message.textContent, /no se borraron/);
});

test("EWG viewer remains available when clipboard permission fails and only receives a validated ZIP", async () => {
  const events = {}, messages = [], opened = [], input = { value: "60162", setAttribute() {}, removeAttribute() {}, focus() {} };
  const scope = app.slice(app.indexOf('  const waterOpen = document.querySelector("[data-water-check-open]")'), app.indexOf('  royalOneOpenLink?.addEventListener("click"'));
  vm.runInNewContext(scope, { document: { querySelector: () => ({ addEventListener: (name, fn) => { events.link = fn; } }) },
    waterCheckInput: input, waterCheckFeedback: {}, waterCheckForm: { addEventListener: (name, fn) => { events.submit = fn; } },
    sanitizeZipCode: value => value.replace(/\D/g, "").slice(0, 5), setMessage: (_node, message) => messages.push(message), clearMessage() {},
    copyTextToClipboard: async () => { throw Error("denied"); }, registerCoachDemoEvent() {}, window: { TecExternalTools: { openWater: zip => opened.push(zip) } } });
  await events.submit({ preventDefault() {} });
  assert.match(messages.at(-1), /No se pudo copiar/);
  let blocked = false; events.link({ preventDefault() { blocked = true; } }); assert.equal(blocked, true);
  assert.deepEqual(opened, ["60162"]);
  assert.match(messages.at(-1), /Consulta del ZIP 60162 en EWG/);
  input.value = "12"; events.link({ preventDefault() { blocked = true; } }); assert.equal(blocked, true);
  assert.doesNotMatch(scope, /window\.open\s*\(|Se abrio EWG/);
  assert.match(html, /data-water-check-open href="https:\/\/www.ewg.org\/tapwater\/"/);
  assert.doesNotMatch(html, /data-water-check-open[^>]*target="_blank"/);
});

test("balance fields have unique associated labels, and login explains manual recovery without soliciting secrets", () => {
  const create = vm.runInNewContext(`(${app.slice(app.indexOf("function createDecisionToolRow("), app.indexOf("function initDecisionTool()")).trim()})`);
  const rows = [create("pro", 1), create("con", 1), create("pro", 2)].join("");
  const ids = [...rows.matchAll(/ id="([^"]+)"/g)].map(match => match[1]);
  assert.equal(new Set(ids).size, 6);
  ids.forEach(id => assert.ok(rows.includes(`for="${id}"`)));
  assert.match(rows, /Peso de razón a favor 1/);
  assert.match(read("public/coach/login/index.html"), /Olvidé mi contraseña/);
  assert.match(read("public/coach/contacto/index.html"), /id="recuperar-acceso"/);
  assert.match(read("public/coach/contacto/index.html"), /verificar tu identidad/);
  assert.doesNotMatch(html, /Iniciar practica|Agustin tomara|Follow up/);
  assert.doesNotMatch(app, /Reset password|Agustin tomara/);
});

test("the new UI dependency is present before app.js on every app consumer and in the offline shell", () => {
  for (const path of ["private/coach-app.html", "public/coach/index.html", "public/coach/login/index.html", "public/coach/planes/index.html", "public/coach/cancel/index.html", "public/coach/success/index.html"]) {
    const source = read(path);
    assert.ok(source.indexOf("/coach/ui-coherence.js") > 0, path);
    assert.ok(source.indexOf("/coach/ui-coherence.js") < source.indexOf("/coach/app.js"), path);
  }
  assert.match(read("public/coach/sw.js"), /\/coach\/ui-coherence.js/);
  assert.match(html, /data-coach-app-page hidden/);
});
