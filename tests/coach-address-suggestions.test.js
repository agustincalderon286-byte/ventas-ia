import test from "node:test";
import assert from "node:assert/strict";
import vm from "node:vm";
import { readFileSync } from "node:fs";
const source = readFileSync(new URL("../public/coach/address-suggestions.js", import.meta.url), "utf8");

function harness(request) {
  function element() {
    const events = {}, attrs = {};
    return { events, attrs, style:{}, children: [], value: "", hidden: true,
      addEventListener(k, fn) { events[k] = fn; },
      setAttribute(k,v) { attrs[k] = v; }, removeAttribute(k) { delete attrs[k]; },
      replaceChildren(...items) { this.children = items; }, append(item) { this.children.push(item); },
      closest() { return null; }, setCustomValidity(v) { this.validityMessage=v; }, reportValidity() {}, focus() {}, scrollIntoView() {}, contains(target) { return target === input || target === city; }
    };
  }
  const input = element(), city = element(), popup = element(), list = element(), status = element(), parent = element();
  popup.parentElement = parent; list.id = "addresses"; city.value = "Hillside";
  const form = element(); form.elements = { address: input, city };form.append = item => { form.elements[item.name]=item; }; const choices = [];
  form.querySelector = selector => ({ "[data-address-popup]": popup, "[data-address-options]": list, "[data-address-status]": status })[selector];
  const timers = new Map(); let id = 0;
  const context = vm.createContext({ document: { createElement: element, addEventListener() {} }, window: { addEventListener() {} }, navigator: { onLine: true }, URLSearchParams,
    Promise, setTimeout: fn => { timers.set(++id, fn); return id; }, clearTimeout: id => timers.delete(id) });
  const init = vm.runInContext(source.replaceAll("export function", "function") + "\ninitTecAddressSuggestions", context);
  init({ form, request, onChoose: value => choices.push(value) });
  return { input, city, popup, list, status, choices, form,
    type(value) { input.value = value; input.events.input(); },
    flush() { const pending = [...timers.values()]; timers.clear(); return Promise.all(pending.map(fn => fn())); }
  };
}
const suggestion = (address, city = "HILLSIDE") => ({ address, city, formattedAddress: `${address}, ${city}, IL`, approximate: false });

test("typing is debounced and keyboard choice fills the exact street and city without submitting", async () => {
  let calls=0;
  const h = harness(async () => { calls++; return { suggestions: [suggestion("4415 HARRISON ST")] }; });
  h.type("4415 h, Hillside"); h.type("4415 ha, Hillside"); h.type("4415 har, Hillside"); await h.flush();
  assert.equal(calls, 1); assert.equal(h.popup.hidden, false);
  assert.equal(h.list.children[0].textContent, "4415 HARRISON ST, HILLSIDE, IL");
  let prevented=0;
  h.input.events.keydown({ key: "ArrowDown", preventDefault() { prevented++; } });
  h.input.events.keydown({ key: "Enter", preventDefault() { prevented++; } });
  assert.equal(prevented, 2); assert.equal(h.form.elements.address.value, "4415 HARRISON ST");
  assert.equal(h.city.value, "HILLSIDE"); assert.equal(h.popup.hidden, true);
  assert.equal(h.choices.length, 1); assert.equal(h.input.attrs["aria-expanded"], "false");
});

test("an old response or a response after submit cannot overwrite current suggestions", async () => {
  const pending=[];
  const h=harness(() => new Promise(resolve => pending.push(resolve)));
  h.type("4415 har, Hillside"); const first=h.flush();
  h.type("123 main, Chicago"); const second=h.flush();
  pending[1]({ suggestions: [suggestion("123 MAIN ST")] }); await second;
  pending[0]({ suggestions: [suggestion("4415 HARRISON ST")] }); await first;
  assert.equal(h.list.children[0].textContent, "123 MAIN ST, HILLSIDE, IL");
  h.type("999 test, Hillside"); const third=h.flush(); h.form.events.submit({preventDefault(){},stopImmediatePropagation(){}});
  pending[2]({ suggestions: [suggestion("999 TEST ST")] }); await third;
  assert.equal(h.popup.hidden, true);
});

test("tap selection works and failed suggestions leave manual address search available", async () => {
  const h=harness(async () => ({ suggestions: [suggestion("123 N MAIN ST", "CHICAGO")] }));
  h.type("123 main, Chicago"); await h.flush();
  h.list.children[0].events.click();
  assert.equal(h.form.elements.address.value, "123 N MAIN ST"); assert.equal(h.city.value, "CHICAGO");
  const failed=harness(async () => { throw new Error("offline"); });
  failed.type("123 main, Chicago"); await failed.flush();
  assert.match(failed.status.textContent, /Incluye calle y ciudad/);
  assert.equal(failed.input.value, "123 main, Chicago");
});

test("combined field requires explicit city and never assumes Hillside", async () => {
 const requests=[];const h=harness(async url=>{requests.push(url);return {suggestions:[suggestion('123 MAIN ST','CHICAGO')]}});
 h.type('123 main');await h.flush();assert.equal(requests.length,0);assert.match(h.status.textContent,/ciudad/);
 h.type('123 main, Chicago');await h.flush();assert.equal(new URL('https://tec.example'+requests[0]).searchParams.get('city'),'Chicago');
 h.list.children[0].events.click();assert.equal(h.form.elements.address.value,'123 MAIN ST');assert.equal(h.city.value,'CHICAGO');
});
