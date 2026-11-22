import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";
import vm from "node:vm";

const load = name => fs.readFileSync(new URL("../public/coach/" + name + ".js", import.meta.url), "utf8");
function fixture() {
  const crm = { id: "c1", leadName: "Casa Ficticia", phone: "3125550111" };
  const user = { id: "seller1", email: "vendedor@example.invalid" };
  function input(name, type = "text", values = []) {
    return { name, type, value: "", tagName: values.length ? "SELECT" : "INPUT", events: [], maxLength: -1,
      options: values.map(value => ({ value })), dispatchEvent(event) { this.events.push(event.type); } };
  }
  function form(fields) {
    const elements = fields.map(([name, type, values]) => input(name, type, values));
    elements.namedItem = name => elements.find(input => input.name === name);
    return { elements, dataset: { tecVisitId: "v1" },
      _tecOfflineController: { hasDraft: () => false, ready: Promise.resolve() },
      reset() { elements.forEach(input => { input.value = input.options?.[0]?.value || ""; }); } };
  }
  const survey = form([["fullName"], ["phone"], ["surveyId"], ["cooksForCount", "number"], ["waterSpendWeekly"], ["secondName"],
    ["drinkingWaterType", "select", ["", "Llave", "Botella", "Filtro"]], ...[1, 2, 3].map(i => ["topProduct" + i])]);
  const outcome = form([["resultType", "select", ["venta", "follow_up", "no_venta"]], ["saleAmount", "number"], ["products"], ["privateReason"]]);
  outcome.reset();
  function calcScope(names) {
    const inputs = names.map(name => input(name));
    return { inputs, querySelector(selector) { return inputs.find(input => selector === "[data-order-calc-" + input.name + "]"); },
      querySelectorAll(selector) { return selector === "input" ? inputs : []; } };
  }
  const cards = [0, 1, 2].map(() => calcScope(["name", "price", "down", "desc-percent", "desc-fixed"]));
  const calc = calcScope(["down-total", "desc-fixed-total", "desc-percent-total"]);
  calc.dataset = { orderCalcSyncReady: "true" };
  calc.querySelectorAll = () => cards;
  const nodes = { "[data-health-survey-form]": survey, "[data-demo-outcome-form]": outcome, "[data-order-calc]": calc };
  const outbox = new Map(), cache = new Map(), requests = [];
  const state = { visit: "v1", user, crm, sync: true };
  const tab = new Map();
  const window = {
    addEventListener() {},
    sessionStorage: { getItem: key => key === "tec-current-visit" ? state.visit : tab.get(key) || null,
      setItem: (key, value) => tab.set(key, value), removeItem: key => tab.delete(key) }, getActiveCoachCrmContext: () => state.crm,
    normalizeLeadPhone: str => String(str || "").replace(/\D/g, ""),
    getTecCachedViewer: async () => ({ user: state.user }),
    tecOfflineGet: async (store, id) => (store === "cache" ? cache : outbox).get(id),
    apiRequest: async (url, opt) => {
      assert.ok(opt.queueFirst && opt.offlineQueue); requests.push({ url, opt });
      outbox.set(opt.requestId, { body: opt.body, ownerEmail: state.user.email });
    },
    flushTecOfflineOutbox: async () => {
      if (!state.sync) return;
      for (const [key, item] of outbox) {
        cache.set("form-result:" + item.ownerEmail + ":" + key, item); outbox.delete(key);
      }
    }
  };
  const ctx = vm.createContext({ document: { querySelector: selector => nodes[selector] }, window, Event, queueMicrotask });
  vm.runInContext(load("order-calculator"), ctx); vm.runInContext(load("demo-close-tools"), ctx);
  return { api: window.TecDemoCloseTools, state, window, requests, survey, outcome, calc, cards,
    field: (form, name) => form.elements.namedItem(name),
    options: { save: true, accountId: user.id, analysisId: "0123456789abcdef01234567", crmId: crm.id, visitId: "v1" } };
}

test("selección en una visita genera dos registros independientes con recibos y claves estables, sin duplicados al repetir", async () => {
  const h = fixture();
  const fields = { cooksForCount: "4", drinkingWaterType: "botella", outcome: "follow_up", reason: "Pidió que le marque el martes." };
  const previous = h.api.snapshot(fields);
  assert.equal(previous.outcome, "");
  const result = await h.api.apply(fields, { ...h.options, previous });
  assert.match(result.message, /confirmado en TEC/);
  assert.equal(h.requests.length, 2);
  const [survey] = h.requests.filter(r => r.url.endsWith("health-surveys"));
  assert.deepEqual(Object.keys(survey.opt.body).sort(), ["accountId","activeCrmRecordId","cooksForCount","drinkingWaterType","fullName","phone","visitId"].sort());
  assert.equal(survey.opt.body.drinkingWaterType, "Botella");
  assert.equal(survey.opt.body.fullName, "Casa Ficticia");
  await h.api.apply(fields, { ...h.options, previous });
  assert.equal(h.requests.length, 2);
});

test("una corrección posterior a la vista previa o un borrador de otra casa se conserva", async () => {
  const h = fixture(), count = h.field(h.survey, "cooksForCount"); count.value = "2";
  const fields = { cooksForCount: "4" }, previous = h.api.snapshot(fields); count.value = "3";
  const result = await h.api.apply(fields, { ...h.options, previous });
  assert.deepEqual(Array.from(result.skipped), ["cooksForCount"]); assert.equal(count.value, "3");
  h.survey._tecOfflineController = { hasDraft: () => true, getDraftContext: () => ({ visitId: "old", activeCrmRecordId: "c1" }) };
  assert.equal(h.api.snapshot(fields).cooksForCount, undefined);
  await h.api.apply(fields, h.options);
  assert.equal(h.requests.length, 0);
  h.survey._tecOfflineController = { hasDraft: () => false };
  h.field(h.survey, "secondName").value = "Dato manual sin confirmar";
  await h.api.apply(fields, { ...h.options, previous: { cooksForCount: "3" } });
  assert.equal(count.value, "4"); assert.equal(h.requests.length, 0);
});

test("una venta precisa monto y elección específica por el aviso de equipo", async () => {
  const h = fixture(), fields = { outcome: "venta", saleAmount: "1200" };
  await h.api.apply({ outcome: "venta" }, { ...h.options, saveSale: true }); assert.equal(h.requests.length, 0);
  await h.api.apply(fields, h.options); assert.equal(h.requests.length, 0);
  h.state.sync = false;
  const report = await h.api.apply(fields, { ...h.options, saveSale: true, previous: h.api.snapshot(fields) });
  assert.match(report.message, /cola de sincronización/); assert.equal(h.requests.length, 1);
  await h.api.apply({ outcome: "venta", saleAmount: "1100" }, { ...h.options, saveSale: true });
  assert.equal(h.requests.length, 1); assert.equal(h.requests[0].opt.body.saleAmount, "1200");
});

test("la calculadora conserva productos, verifica los importes y deja la mensualidad como referencia", async () => {
  const h = fixture(), [a,b] = h.cards;
  a.querySelector("[data-order-calc-name]").value = "Sartén";
  a.querySelector("[data-order-calc-price]").value = "250";
  const fields = { calcProduct: "Olla", calcPrice: "500", calcDown: "100", quotedMonthly: "90" };
  const r = await h.api.apply(fields, { ...h.options, previous: h.api.snapshot(fields) });
  assert.equal(b.querySelector("[data-order-calc-name]").value, "Olla");
  assert.equal(b.querySelector("[data-order-calc-price]").value, "500");
  assert.equal(a.querySelector("[data-order-calc-price]").value, "250");
  assert.equal(h.calc.querySelector("[data-order-calc-down-total]").value, "100");
  assert.ok(!r.applied.includes("quotedMonthly")); assert.equal(h.requests.length, 0);
  const invalid = await h.api.apply({ calcDown: "99000" }, { previous: h.api.snapshot(fields) });
  assert.ok(invalid.skipped.includes("calcDown"));
  await h.api.apply(fields);
  assert.equal(h.cards[2].querySelector("[data-order-calc-name]").value, "");
});

test("cambio de sesión, visita, selección faltante o cliente con encuesta vinculada no producen envíos", async () => {
  const h = fixture(), f = { cooksForCount: "3" };
  h.state.visit = "v2"; await h.api.apply(f, h.options);
  h.state.visit = "v1"; h.state.user = { id: "b", email: "otra@example.invalid" }; await h.api.apply(f, h.options);
  assert.equal(h.field(h.survey,"cooksForCount").value,"");
  h.state.user = { id: "seller1" }; h.state.crm.linkedHealthSurveyId = "ya-existe";
  await h.api.apply(f, h.options);
  assert.equal(h.requests.length, 0);
  assert.equal(h.field(h.survey, "cooksForCount").value, "3");
});

test("un código del catálogo reutiliza el producto ya seleccionado y una recarga mantiene el escenario solo para su cuenta", async () => {
  const h = fixture(), slot = h.cards[0];
  slot.dataset = { orderCalcCatalogCode: "P001" };
  slot.querySelector("[data-order-calc-name]").value = "Olla del catálogo";
  const fields = { calcProduct: "P001", calcPrice: "450" };
  await h.api.apply(fields, { ...h.options, previous: h.api.snapshot(fields) });
  assert.equal(slot.querySelector("[data-order-calc-name]").value, "Olla del catálogo");
  assert.equal(h.cards[1].querySelector("[data-order-calc-name]").value, "");
  const price = slot.querySelector("[data-order-calc-price]");
  price.value = "";
  h.calc.addEventListener = () => {};
  h.state.user.id = "another"; h.api.mountCalculator(h.calc);
  await new Promise(setImmediate); assert.equal(price.value, "");
  h.state.user.id = "seller1"; h.api.mountCalculator(h.calc);
  await new Promise(setImmediate); assert.equal(price.value, "450");
});

test("la identidad de la casa y el formulario se vuelven a comprobar si cambian durante la confirmación", async () => {
  const h = fixture();
  let resolve, calls = 0;
  h.window.getTecCachedViewer = async () => {
    if (++calls === 2) await new Promise(r => { resolve = r; });
    return { user: h.state.user };
  };
  const running = h.api.apply({ cooksForCount: "2" }, h.options);
  await new Promise(setImmediate);
  assert.ok(resolve);
  h.field(h.survey, "secondName").value = "Nueva edición sin aprobar";
  resolve(); const result = await running;
  assert.equal(h.requests.length, 0);
  assert.match(result.message, /pendiente de comprobar/);
  assert.equal(h.field(h.survey, "secondName").value, "Nueva edición sin aprobar");
});
