import test from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import vm from "node:vm";

const source = readFileSync(new URL("../public/coach/referral-sheet-photo.js", import.meta.url), "utf8");
const sheetTools = readFileSync(new URL("../public/coach/referral-sheet.js", import.meta.url), "utf8");
const syntheticPhoto = { name: "hoja.png", type: "image/png", size: 5_000_000 };
const contact = (i = 1) => ({
  fullName: "Referido " + i, phone: "555010" + String(i).padStart(4, "0"), address: "Calle " + i,
  city: "", relationship: "", location: ""
});
const sheet = (referrals = [contact()]) => ({ documentType: "referrals", hostName: "Anfitriona", hostPhone: "5550100000", referrals });
const tickets = (referrals = [contact()]) => ({ documentType: "tickets", referrals });

class Element {
  constructor(tag = "div") {
    this.tagName = tag.toUpperCase();
    this.children = [];
    this.dataset = {};
    this.listeners = new Map();
    this.value = this.textContent = "";
  }
  append(...nodes) {
    for (const node of nodes) { node.parent = this; node.parentElement = this; this.children.push(node); }
  }
  replaceChildren(...nodes) {
    this.children.forEach(node => { node.parent = null; });
    this.children = [];
    this.append(...nodes);
  }
  walk() { return [this, ...this.children.flatMap(node => node.walk())]; }
  tagged(tag) { return this.walk().filter(node => node.tagName === tag.toUpperCase()); }
  named(tag, text) { return this.tagged(tag).find(node => node.textContent === text); }
  field(label) { return this.tagged("label").find(el => el.children.some(child => child.textContent === label))?.tagged("input")[0]; }
  containsText(text) { return this.walk().some(node => node.textContent.includes(text)); }
  setAttribute(name, value) { this[name] = value; }
  focus() { this.focused = true; }
  addEventListener(type, handle) {
    if (!this.listeners.has(type)) this.listeners.set(type, new Set());
    this.listeners.get(type).add(handle);
  }
  removeEventListener(type, handle) { this.listeners.get(type)?.delete(handle); }
  async fire(type, extra = {}) {
    const event = { target: this, stopPropagation() { this.stopped = true; }, preventDefault() { this.prevented = true; }, ...extra };
    for (let node = this; node; node = node.parent) {
      for (const handle of node.listeners.get(type) || []) await handle(event);
      if (event.stopped) break;
    }
    return event;
  }
}

function pending() {
  let resolve, reject;
  const promise = new Promise((yes, no) => { resolve = yes; reject = no; });
  return { promise, resolve, reject };
}

function harness(options = {}) {
  const form = new Element("form");
  form.dataset.tecVisitId = "visit-a";
  const controls = {};
  for (const [suffix, tag] of [["", "input"], ["-camera", "input"], ["-read", "button"], ["-preview", "div"], ["-status", "p"]]) {
    controls[suffix] = new Element(tag);
    form.append(controls[suffix]);
  }
  const [gallery, camera, read, preview, status] = Object.values(controls);
  read.textContent = "Leer con IA";
  form.querySelector = selector => controls[selector.slice("[data-fourteen-photo".length, -1)];
  const window = new Element();
  const document = new Element();
  const shots = [], encodings = [], reads = [], applied = [], exported = [], filesRead = [];
  const revoked = [];
  const URL = {
    createObjectURL(file) { shots.push(file); return "blob:local/" + shots.length; },
    revokeObjectURL(url) { revoked.push(url); }
  };
  class Image {
    set src(url) {
      const photo = shots[Number(url.split("/").at(-1)) - 1];
      this.naturalWidth = photo.width || 4000;
      this.naturalHeight = photo.height || 6000;
      if (photo.load) photo.load.promise.then(() => this.onload());
      else if (photo.broken) this.onerror();
      else this.onload();
    }
  }
  class FileReader {
    readAsDataURL(blob) {
      filesRead.push(blob);
      if (options.readerError) return this.onerror();
      this.result = "data:image/jpeg;base64," + Buffer.alloc(blob.size, 23).toString("base64");
      this.onload();
    }
  }
  document.createElement = tag => {
    const el = new Element(tag);
    if (tag === "canvas") {
      el.getContext = () => ({ fillRect() {}, drawImage() {} });
      el.toBlob = (done, type, quality) => {
        encodings.push({ width: el.width, height: el.height, type, quality });
        const size = options.encodedSize?.(el.width, el.height, quality) ?? 120;
        done({ type, size });
      };
    }
    return el;
  };
  const context = vm.createContext({ window, document, URL, Image, FileReader });
  vm.runInContext(sheetTools, context);
  vm.runInContext(source, context);
  let owner = true;
  const args = {
    form, isCurrentSeller: () => owner,
    readPhoto: async base64 => {
      reads.push(base64);
      return options.readPhoto ? options.readPhoto() : sheet();
    },
    apply: data => {
      applied.push(JSON.parse(JSON.stringify(data)));
      return options.apply ? options.apply(data) : { applied: true };
    },
    exportTickets: data => {
      exported.push(JSON.parse(JSON.stringify(data)));
      return options.exportTickets ? options.exportTickets(data) : { exported: true };
    }
  };
  const binding = window.TecReferralPhoto.bind(args);
  const select = (picker = gallery, file = syntheticPhoto) => {
    picker.files = [file];
    picker.value = file.name;
    return picker.fire("change");
  };
  const readSheet = async () => { await select(); await read.fire("click"); };
  const verify = async container => {
    for (const field of container.tagged("input").filter(el => el.type === "checkbox" && el.parent.containsText("Comparé"))) {
      if (field.disabled || field.parent.hidden) continue;
      field.checked = true;
      await field.fire("change");
    }
  };
  return {
    form, window, document, gallery, camera, read, preview, status, encodings, reads, applied,
    filesRead, shots, revoked, binding, args, select, readSheet, exported, verify: (el = preview) => verify(el),
    csv() { return preview.named("button", "Descargar CSV de boletas"); },
    setOwner(value) { owner = value; }, confirm() { return preview.named("button", "Pasar al formulario"); }
  };
}

test("gallery and camera shrink the whole portrait locally and send only a bounded JPEG after Leer con IA", async () => {
  const f = harness({ encodedSize: (_w, h) => h > 2560 ? 2_097_153 : 2_097_152 });
  assert.equal(f.read.disabled, true);
  await f.select();
  assert.equal(f.encodings[0].width, 2133);
  assert.equal(f.encodings[0].height, 3200);
  assert.equal(f.encodings[0].type, "image/jpeg");
  assert.deepEqual(f.encodings.map(e => [e.height, e.quality]), [[3200, 0.88], [3200, 0.80], [2560, 0.88]]);
  assert.equal(f.reads.length, 0);
  assert.equal(f.preview.tagged("img").length, 1);
  assert.match(f.status.textContent, /Pulsa Leer con IA/);
  assert.equal(f.read.disabled, false);
  await f.select(f.camera);
  assert.equal(f.gallery.value, "");
  assert.equal(f.revoked.length, 1);
  assert.equal(f.reads.length, 0);
  await f.read.fire("click");
  assert.equal(f.reads.length, 1);
  assert.equal(f.reads[0].length, 4 * Math.ceil(2 * 1024 * 1024 / 3));
  assert.equal(Buffer.from(f.reads[0], "base64").length, 2 * 1024 * 1024);
  assert.equal(f.applied.length, 0);
  assert.equal(f.filesRead.every(blob => blob.type === "image/jpeg"), true);
});

test("review inputs do not participate in form submission or draft events and only explicit approval transfers corrections", async () => {
  const f = harness({ readPhoto: () => sheet([contact(1), contact(2)]) });
  const drafts = [];
  for (const type of ["input", "change", "submit"]) f.form.addEventListener(type, () => drafts.push(type));
  await f.readSheet();
  const entries = f.preview.tagged("fieldset");
  const [include, name, phone, address] = entries[0].tagged("input");
  name.value = "Nombre corregido";
  phone.value = "5550100123";
  address.value = "Calle nueva";
  await name.fire("input");
  const enter = await name.fire("keydown", { key: "Enter" });
  assert.equal(enter.prevented, true);
  const second = entries[1].tagged("input")[0];
  second.checked = false;
  await second.fire("change");
  assert.equal(entries[1].tagged("input")[1].disabled, true);
  assert.ok(f.preview.tagged("input").every(el => !el.name));
  assert.ok(f.preview.tagged("button").every(el => el.type === "button"));
  assert.equal(include.checked, true);
  assert.deepEqual(drafts, []);
  const approval = f.confirm();
  assert.equal(f.applied.length, 0);
  await f.verify();
  await approval.fire("click");
  await approval.fire("click");
  assert.deepEqual(f.applied, [{ documentType: "referrals", hostName: "Anfitriona", hostPhone: "5550100000", referrals: [
    { ...contact(), fullName: name.value, phone: phone.value, address: address.value }
  ] }]);
  assert.equal(f.preview.hidden, true);
  assert.match(f.status.textContent, /fuera: 1/);
  assert.match(f.status.textContent, /Guardar en TEC/);
});

test("shows uncertain and excess rows, requires completing or excluding a partial row, and keeps all readable rows", async () => {
  const response = sheet(Array.from({ length: 11 }, (_, i) => contact(i + 1)));
  response.referrals[3].phone = "";
  response.review = { incompleteRows: [4, 6], overflow: true };
  const f = harness({ readPhoto: () => response });
  await f.readSheet();
  assert.equal(f.preview.containsText("más de 11"), true);
  const rows = f.preview.tagged("fieldset");
  assert.equal(rows.length, 11);
  assert.ok(rows[3].containsText("Comprueba"));
  assert.ok(rows[5].containsText("Comprueba"));
  await f.verify();
  await f.confirm().fire("click");
  assert.equal(f.applied.length, 0);
  assert.match(f.status.textContent, /referido 4/);
  const phone = rows[3].tagged("input")[2];
  assert.equal(phone.focused, true);
  phone.value = "5550100444";
  await f.verify(rows[3]);
  await f.confirm().fire("click");
  assert.equal(f.applied[0].referrals.length, 11);
  assert.match(f.status.textContent, /otra hoja/);
});

test("excess returned rows remain available to choose and transactional conflicts stay editable", async () => {
  const response = sheet(Array.from({ length: 12 }, (_, i) => contact(i + 1)));
  const f = harness({ readPhoto: () => response, apply: data => !data.hostName
    ? { applied: true, message: "Se incorporaron 11 referidos." }
    : { applied: false, message: "El anfitrión ya está capturado." } });
  await f.readSheet();
  const rows = f.preview.tagged("fieldset");
  assert.equal(rows.length, 12);
  await f.confirm().fire("click");
  assert.equal(f.applied.length, 0);
  assert.match(f.status.textContent, /hasta 11/);
  rows[0].tagged("input")[0].checked = false;
  await f.verify();
  await f.confirm().fire("click");
  assert.equal(f.confirm().disabled, false);
  assert.match(f.status.textContent, /ya está capturado/);
  f.preview.tagged("input")[0].value = "";
  await f.verify();
  await f.confirm().fire("click");
  assert.equal(f.applied[1].referrals.length, 11);
  assert.deepEqual(f.applied[1].referrals.at(-1), contact(12));
});

test("a second read click is ignored while waiting, but a failed request is retryable with the same prepared image", async () => {
  const first = pending();
  let calls = 0;
  const f = harness({ readPhoto: () => ++calls === 1 ? first.promise : sheet() });
  await f.select();
  const inFlight = f.read.fire("click");
  await f.read.fire("click");
  assert.equal(calls, 1);
  first.reject(new Error("Sin conexión"));
  await inFlight;
  assert.match(f.status.textContent, /Sin conexión/);
  assert.equal(f.read.disabled, false);
  await f.read.fire("click");
  assert.equal(calls, 2);
  assert.equal(f.filesRead.length, 1);
  assert.ok(f.confirm());
});

test("an empty extraction can be retried and a manual form remains available", async () => {
  const f = harness({ readPhoto: () => ({ documentType: "referrals", hostName: "", hostPhone: "", referrals: [] }) });
  await f.readSheet();
  assert.match(f.status.textContent, /No se distinguieron datos/);
  assert.equal(f.read.disabled, false);
  assert.equal(f.confirm(), undefined);
  assert.equal(f.applied.length, 0);
});

test("unreadable files, failed JPEG conversion, and oversized output never reach the reader", async () => {
  for (const [file, options] of [
    [{ ...syntheticPhoto, size: 0 }, {}],
    [{ ...syntheticPhoto, type: "application/pdf" }, {}],
    [{ ...syntheticPhoto, broken: true }, {}],
    [syntheticPhoto, { readerError: true }],
    [syntheticPhoto, { encodedSize: () => 0 }],
    [syntheticPhoto, { encodedSize: () => 3_000_000 }]
  ]) {
    const f = harness(options);
    await f.select(f.gallery, file);
    await f.read.fire("click");
    assert.equal(f.reads.length, 0);
    assert.equal(f.read.disabled, true);
    assert.equal(f.gallery.value, "");
    assert.equal(f.status.dataset.state, "error");
    assert.equal(f.shots.length, f.revoked.length);
  }
});

test("an abandoned decode cannot display the previous photo after a new camera selection", async () => {
  const slow = pending();
  const f = harness();
  const old = f.select(f.gallery, { ...syntheticPhoto, load: slow });
  await f.select(f.camera);
  const displayed = f.preview.tagged("img")[0];
  slow.resolve();
  await old;
  assert.equal(f.preview.tagged("img")[0], displayed);
  assert.equal(f.filesRead.length, 1);
  assert.deepEqual(f.revoked, ["blob:local/1"]);
});

test("a pending read is invalidated on reset, removal, page exit, visit change, seller change, or controller replacement", async () => {
  const interruptions = [
    f => f.form.fire("reset"),
    f => f.preview.named("button", "Quitar foto").fire("click"),
    f => f.window.fire("pagehide"),
    f => { f.form.dataset.tecVisitId = "visit-b"; },
    f => f.setOwner(false),
    f => f.window.TecReferralPhoto.bind(f.args)
  ];
  for (const interrupt of interruptions) {
    const slow = pending(), f = harness({ readPhoto: () => slow.promise });
    await f.select();
    const read = f.read.fire("click");
    await interrupt(f);
    slow.resolve(sheet());
    await read;
    assert.equal(f.preview.hidden, true);
    assert.equal(f.preview.children.length, 0);
    assert.equal(f.status.textContent, "");
    assert.equal(f.read.disabled, true);
    assert.equal(f.applied.length, 0);
  }
});

test("changing seller after review clears personal data and a detached approval cannot apply it", async () => {
  const f = harness();
  await f.readSheet();
  const old = f.confirm();
  f.setOwner(false);
  await f.window.fire("storage");
  assert.equal(f.preview.hidden, true);
  await old.fire("click");
  await f.select();
  assert.equal(f.applied.length, 0);
  assert.equal(f.shots.length, 1);
});

test("zoom compares against the full local original without a second request and releases it on reset", async () => {
  const f = harness({ encodedSize: (_w, _h, quality) => quality > 0.80 ? 3_000_000 : 1_900_000 });
  await f.select();
  assert.deepEqual(f.encodings.map(e => e.height), [3200, 3200]);
  const zoom = f.preview.named("button", "Ampliar foto original");
  const image = f.preview.tagged("img")[0];
  const thumbnail = image.src;
  await zoom.fire("click");
  assert.equal(image.src, "blob:local/1");
  assert.equal(image.className, "fourteen-photo-original");
  assert.equal(zoom["aria-expanded"], "true");
  await zoom.fire("click");
  assert.equal(image.src, thumbnail);
  assert.equal(f.reads.length, 0);
  await f.form.fire("reset");
  assert.deepEqual(f.revoked, ["blob:local/1"]);
  await zoom.fire("click");
  assert.equal(zoom["aria-expanded"], "false");
});

test("ticket CSV sends selected verified rows with their corrected location, city, and relationship, independent of the host form", async () => {
  const response = { ...tickets([contact(), contact(2)]), hostName: "Ignorar", hostPhone: "5550108888",
    review: { incompleteRows: [2], lowResolution: true, overflow: true } };
  const f = harness({ readPhoto: () => response });
  await f.readSheet();
  assert.equal(f.confirm(), undefined);
  assert.equal(f.preview.containsText("anfitrión"), false);
  assert.equal(f.preview.containsText("superponen"), true);
  assert.equal(f.preview.containsText("resolución es baja"), true);
  const [first, second] = f.preview.tagged("fieldset");
  const includeSecond = second.field("Incluir esta boleta");
  includeSecond.checked = false;
  await includeSecond.fire("change");
  await f.csv().fire("click");
  assert.equal(f.exported.length, 0);
  assert.match(f.status.textContent, /confirmación/);
  const fields = [["Ubicación en la foto", "Abajo derecha"], ["Ciudad", "El Paso"], ["Relación o parentesco", "Amiga"]];
  for (const [label, value] of fields) {
    const input = first.field(label);
    input.value = value;
    await input.fire("input");
  }
  await f.verify(first);
  await f.csv().fire("click");
  assert.equal(f.applied.length, 0);
  assert.deepEqual(f.exported, [{ documentType: "tickets", referrals: [
    { ...contact(), location: "Abajo derecha", city: "El Paso", relationship: "Amiga" }
  ] }]);
  assert.match(f.status.textContent, /CSV de boletas preparado/);
  assert.match(f.status.textContent, /Boletas que dejaste fuera: 1/);
});

test("only complete numbers can be approved and each changed name or phone needs another confirmation", async () => {
  const f = harness({ readPhoto: () => tickets([{ ...contact(), phone: "5550100" }]) });
  await f.readSheet();
  const row = f.preview.tagged("fieldset")[0];
  const status = row.tagged("input").at(-1);
  await f.verify(row);
  await f.csv().fire("click");
  assert.equal(f.exported.length, 0);
  assert.match(f.status.textContent, /código de área/);
  const phone = row.field("Teléfono");
  phone.value = "5550100123";
  await phone.fire("input");
  assert.equal(status.checked, false);
  await f.verify(row);
  row.field("Nombre").value = "Nombre corregido";
  await f.csv().fire("click");
  assert.match(f.status.textContent, /confirmación/);
  assert.equal(f.exported.length, 0);
  await f.verify(row);
  await f.csv().fire("click");
  assert.equal(f.exported[0].referrals[0].phone, "5550100123");
});

test("referrals verify the host phone and transfer the additional fields without losing the original row", async () => {
  const row = { ...contact(), location: "Renglón 3", city: "Chicago", relationship: "Vecino" };
  const f = harness({ readPhoto: () => sheet([row]) });
  await f.readSheet();
  await f.verify(f.preview.tagged("fieldset")[0]);
  await f.confirm().fire("click");
  assert.match(f.status.textContent, /anfitrión/);
  assert.equal(f.applied.length, 0);
  await f.verify();
  await f.confirm().fire("click");
  assert.deepEqual(f.applied[0], sheet([row]));
});

test("unknown or missing document type cannot be applied or exported even when the response includes names", async () => {
  for (const type of ["unknown", undefined, "unrecognized"]) {
    const f = harness({ readPhoto: () => ({ ...sheet(), documentType: type }) });
    await f.readSheet();
    assert.match(f.status.textContent, /identificar una hoja de referidos ni boletas/);
    assert.equal(f.csv(), undefined);
    assert.equal(f.confirm(), undefined);
    assert.equal(f.read.disabled, false);
    assert.equal(f.applied.length + f.exported.length, 0);
  }
});

test("ticket export rejects an empty selection, keeps a recoverable CSV error editable and guards a changed visit", async () => {
  const f = harness({ readPhoto: () => tickets(), exportTickets: () => ({ exported: false, message: "Revisa el número repetido." }) });
  await f.readSheet();
  const csv = f.csv();
  const incl = f.preview.field("Incluir esta boleta");
  incl.checked = false;
  await incl.fire("change");
  await csv.fire("click");
  assert.match(f.status.textContent, /al menos una boleta/);
  assert.equal(f.exported.length, 0);
  incl.checked = true;
  await incl.fire("change");
  await f.verify();
  await csv.fire("click");
  assert.match(f.status.textContent, /repetido/);
  assert.equal(csv.disabled, false);
  f.form.dataset.tecVisitId = "new";
  await csv.fire("click");
  assert.equal(f.exported.length, 1);
  assert.equal(f.preview.hidden, true);
  assert.equal(f.applied.length, 0);
});
