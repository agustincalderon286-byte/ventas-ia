import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";
import vm from "node:vm";

const helper = fs.readFileSync(new URL("../public/coach/order-calculator.js", import.meta.url), "utf8");
const app = fs.readFileSync(new URL("../public/coach/app.js", import.meta.url), "utf8");
const html = fs.readFileSync(new URL("../private/coach-app.html", import.meta.url), "utf8");
const worker = fs.readFileSync(new URL("../public/coach/sw.js", import.meta.url), "utf8");
const context = vm.createContext({ window: {} });
vm.runInContext(helper, context);
const calculate = input => JSON.parse(JSON.stringify(context.window.TecOrderCalculator.calculateOrder(input)));

test("valid order keeps the existing estimate formulas and separates discounts from down payment", () => {
  const result = calculate({ products: [{ basePrice: "1000", down: "200", descPercent: "10" }] });
  assert.equal(result.valid, true);
  assert.deepEqual(result.products[0], { slot: 1, name: "", code: "", basePrice: 1000, tax: 100, shipping: 50,
    grossTotal: 1150, downPayment: 200, discountAmount: 115, balance: 835, monthly: 41.75, weekly: 10.44, daily: 1.49 });
  assert.equal(result.summary.balanceFinal, 835);
  assert.equal(result.summary.totalDiscount, 115);
});

test("individual and general discounts use gross, with all down payments counted once", () => {
  const result = calculate({ products: [{ basePrice: 1000, down: 100, descFixed: 15, descPercent: 10 }, { basePrice: 100, down: 10 }],
    summary: { extraDown: 50, extraDescFixed: 5, extraDescPercent: 10 } });
  assert.equal(result.valid, true);
  assert.equal(result.summary.totalGross, 1265);
  assert.equal(result.summary.totalDown, 160);
  assert.equal(result.summary.totalDiscount, 261.5);
  assert.equal(result.summary.balanceFinal, 843.5);
});

test("100 percent or exact full payment is valid, but excess discount and down payment are rejected", () => {
  assert.equal(calculate({ products: [{ basePrice: 100, descPercent: 100 }] }).summary.balanceFinal, 0);
  assert.equal(calculate({ products: [{ basePrice: 100, down: 115 }] }).summary.balanceFinal, 0);
  for (const product of [{ basePrice: 100, descPercent: 100, down: 0.01 }, { basePrice: 100, descFixed: 116 },
    { basePrice: 100, down: 116 }, { basePrice: 100, descFixed: 60, descPercent: 50 }]) {
    const result = calculate({ products: [product] });
    assert.equal(result.valid, false);
    assert.equal(result.summary, null);
    assert.match(result.errors[0].message, /no pueden superar/);
  }
});

test("invalid numeric values cannot produce totals or a Coach scenario", () => {
  for (const field of ["basePrice", "down", "descFixed", "descPercent"]) {
    for (const value of ["-1", "Infinity", "NaN", "12oops", "0x10", "1.005", "1e100"]) {
      const result = calculate({ products: [{ basePrice: 100, [field]: value }] });
      assert.equal(result.valid, false, `${field}=${value}`);
      assert.equal(result.summary, null);
      assert.equal(result.products.length, 0);
      assert.ok(result.errors.some(error => error.field === field));
    }
  }
  for (const field of ["extraDown", "extraDescFixed", "extraDescPercent"]) {
    for (const value of ["-1", "Infinity", "1.005", "12oops"]) {
      assert.equal(calculate({ products: [{ basePrice: 100 }], summary: { [field]: value } }).valid, false);
    }
  }
});

test("percentages above 100 are rejected at both product and summary level without clamping", () => {
  for (const percent of [100.01, 150]) {
    const individual = calculate({ products: [{ basePrice: 1000, descPercent: percent }] });
    assert.equal(individual.valid, false);
    assert.match(individual.errors[0].message, /entre 0 y 100/);
    assert.equal(calculate({ products: [{ basePrice: 1000 }], summary: { extraDescPercent: percent } }).valid, false);
  }
});

test("summary extras cannot overdraw a valid product, including combined individual and general discounts", () => {
  for (const summary of [{ extraDown: 16 }, { extraDescFixed: 16 }, { extraDescPercent: 20 }]) {
    const result = calculate({ products: [{ basePrice: 100, down: 100 }], summary });
    assert.equal(result.valid, false);
    assert.ok(result.errors.some(error => error.scope === "summary"));
  }
  assert.equal(calculate({ products: [{ basePrice: 100, descPercent: 60 }], summary: { extraDescPercent: 50 } }).valid, false);
  assert.equal(calculate({ products: [{ basePrice: 100 }], summary: { extraDescPercent: 100 } }).summary.balanceFinal, 0);
});

test("blank fields are zero, cents round consistently, and unsafe sums fail closed", () => {
  assert.equal(calculate({ products: [{ basePrice: "" }] }).summary.totalGross, 0);
  const result = calculate({ products: [{ basePrice: 0.1 }, { basePrice: 0.2 }] });
  assert.equal(result.summary.totalGross, 0.35);
  assert.equal(result.products[0].shipping, 0.01);
  assert.equal(calculate({ products: [{ basePrice: "90071992547400" }] }).valid, false);
});

function uiHarness({ loadHelper = true } = {}) {
  const element = (value = "") => ({ value, dataset: {}, validity: { badInput: false }, attributes: {}, textContent: "",
    hidden: false, disabled: false, listeners: {},
    addEventListener(type, handler) {
      const previous = this.listeners[type];
      this.listeners[type] = (...args) => { previous?.(...args); handler(...args); };
    },
    setCustomValidity(message) { this.validationMessage = message; },
    setAttribute(name, value) { this.attributes[name] = value; },
    removeAttribute(name) { delete this.attributes[name]; }, focus() { this.focused = true; } });
  const cardFields = ["name", "price", "down", "desc-fixed", "desc-percent", "gross", "total", "savings", "balance", "monthly", "weekly", "daily", "breakdown", "product-validation"];
  const cards = Array.from({ length: 3 }, () => {
    const nodes = Object.fromEntries(cardFields.map(key => [`[data-order-calc-${key}]`, element()]));
    return { dataset: {}, nodes, querySelector: selector => nodes[selector] || null,
      querySelectorAll: () => ['name','price','down','desc-fixed','desc-percent'].map(key=>nodes[`[data-order-calc-${key}]`]) };
  });
  const nodes = Object.fromEntries(["down-total", "desc-fixed-total", "desc-percent-total", "summary-total", "summary-down", "summary-discount",
    "summary-balance", "summary-monthly", "summary-weekly", "summary-daily", "submit-context", "validation"].map(key => [`[data-order-calc-${key}]`, element()]));
  nodes["[data-order-calc-products]"] = { children: cards };
  const numeric = [...cards.flatMap(card => ["price", "down", "desc-fixed", "desc-percent"].map(key => card.nodes[`[data-order-calc-${key}]`])),
    ...["down-total", "desc-fixed-total", "desc-percent-total"].map(key => nodes[`[data-order-calc-${key}]`])];
  const root = { dataset: {}, querySelector: selector => selector === '[aria-invalid="true"]' ? numeric.find(input => input.attributes["aria-invalid"]) : nodes[selector] || null,
    querySelectorAll: selector => selector === "[data-order-calc-product]" ? cards : selector === "input" ? [...numeric, ...cards.map(card => card.nodes["[data-order-calc-name]"])] : numeric };
  let active = null;
  const win = { requestAnimationFrame: fn => fn() };
  const ctx = vm.createContext({ window: win, document: { querySelector: () => root, querySelectorAll: () => [] },
    formatMoney: amount => `$${amount.toFixed(2)}`, getActiveCoachHealthSurveyContext: () => null,
    getActiveCoachOrderCalcContext: () => active, renderActiveCoachOrderCalcContext() {},
    buildCoachOrderCalcContext: value => ({ ...value, signature: JSON.stringify(value) }),
    setActiveCoachOrderCalcContext: value => { active = value; }, registerCoachDemoEvent() {}, addCoachMessage() {},
    buildCoachOrderCalcReply: () => "", ORDER_CALC_PRODUCT_COUNT: 3, clearTimeout,
    normalizeCatalogLookupText: value => value.toLowerCase(), searchTecProductCatalog: async () => [] });
  if (loadHelper) vm.runInContext(helper, ctx);
  vm.runInContext(app.slice(app.indexOf("function initOrderCalculator()"), app.indexOf("function createDecisionToolRow(")) + "\ninitOrderCalculator();", ctx);
  return { root, cards, nodes, input(index, field, value, badInput = false) {
    const node = cards[index].nodes[`[data-order-calc-${field}]`];
    node.value = value; node.validity.badInput = badInput; node.listeners.input();
  }, get active() { return active; } };
}

test("real UI calculation blocks invalid input, clears stale Coach values and recovers on correction", () => {
  const h = uiHarness();
  const node = key => h.nodes[`[data-order-calc-${key}]`];
  h.input(0, "price", "1000"); h.input(0, "down", "200"); h.input(0, "desc-percent", "10");
  assert.equal(node("summary-balance").textContent, "$835.00");
  assert.equal(h.cards[0].nodes["[data-order-calc-savings]"].textContent, "$115.00");
  node("submit-context").listeners.click();
  assert.equal(h.active.summary.balanceFinal, 835);
  assert.equal(h.active.products[0].discountAmount, 115);
  h.input(0, "desc-percent", "150");
  assert.equal(h.root.dataset.orderCalcValid, "false");
  assert.equal(node("submit-context").disabled, true);
  assert.equal(node("summary-balance").textContent, "—");
  assert.equal(node("validation").hidden, false);
  assert.equal(h.cards[0].nodes["[data-order-calc-desc-percent]"].attributes["aria-invalid"], "true");
  assert.equal(h.cards[0].nodes["[data-order-calc-desc-percent]"].attributes["aria-describedby"], "order-calc-product-validation-1");
  assert.equal(h.cards[0].nodes["[data-order-calc-product-validation]"].hidden, false);
  assert.equal(h.active, null);
  node("submit-context").listeners.click(); // Even a programmatic activation cannot bypass validation.
  assert.equal(h.active, null);
  h.input(0, "desc-percent", "10");
  assert.equal(node("submit-context").disabled, false);
  assert.equal(node("validation").hidden, true);
  assert.equal(h.cards[0].nodes["[data-order-calc-desc-percent]"].attributes["aria-invalid"], undefined);
  assert.equal(node("summary-balance").textContent, "$835.00");
  h.input(0, "price", "", true); // A browser's badInput empty value is not a real zero.
  assert.equal(node("submit-context").disabled, true);
});

test("missing calculator dependency explains recovery and never shares unvalidated numbers", () => {
  const h = uiHarness({ loadHelper: false });
  assert.equal(h.nodes["[data-order-calc-submit-context]"].disabled, true);
  assert.equal(h.nodes["[data-order-calc-validation]"].hidden, false);
  assert.match(h.nodes["[data-order-calc-validation]"].textContent, /con conexión/);
  assert.equal(h.active, null);
});

test("survey products keep their slots and selected catalog price; new client clears all totals", () => {
  const h=uiHarness();
  h.root.syncProductsFromSurvey({id:'new-visit',topProducts:['','SARTEN 12','OLLA']});
  assert.deepEqual(h.cards.map(c=>c.nodes['[data-order-calc-name]'].value),['','SARTEN 12','OLLA']);
  h.root.applySurveyProduct(1,{nombre_producto:'SARTEN 12',codigo_producto:'C12',precio_base_catalogo:999});
  assert.equal(h.cards[1].nodes['[data-order-calc-price]'].value,'999');
  assert.equal(h.cards[1].dataset.orderCalcCatalogCode,'C12');
  h.input(1,'down','100');
  h.root.resetForNewClient();
  assert.deepEqual(h.cards.map(c=>c.nodes['[data-order-calc-name]'].value),['','','']);
  assert.deepEqual(h.cards.map(c=>c.nodes['[data-order-calc-price]'].value),['','','']);
  assert.equal(h.nodes['[data-order-calc-summary-balance]'].textContent,'$0.00');
});

test("calculator dependency is loaded before app and included in offline shell", () => {
  assert.ok(html.indexOf('/coach/order-calculator.js?v=') < html.indexOf('/coach/app.js?v='));
  assert.match(worker, /\/coach\/order-calculator\.js\?v=/);
  assert.match(html, /max="100"[^>]*data-order-calc-desc-percent-total/);
  assert.match(html, /id="order-calc-validation"[^>]*role="alert"/);
});
