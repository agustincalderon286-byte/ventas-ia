import test from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import vm from "node:vm";

const source = readFileSync(new URL("../public/coach/referral-sheet-photo.js", import.meta.url), "utf8");
const sheetTools = readFileSync(new URL("../public/coach/referral-sheet.js", import.meta.url), "utf8");
const appSource = readFileSync(new URL("../public/coach/app.js", import.meta.url), "utf8");
const appPage = readFileSync(new URL("../private/coach-app.html", import.meta.url), "utf8");
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
    this.disabled = false;
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
  const shots = [], encodings = [], reads = [], applied = [], exported = [], saved = [], filesRead = [];
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
  let requestNumber = 0, folderOpens = 0;
  const args = {
    form, isCurrentSeller: () => owner,
    openLeads: () => { folderOpens++; },
    createRequestId: () => "test-key-" + (++requestNumber),
    saveTickets: options.visiting ? null : async (data, requestId) => {
      saved.push({ data: JSON.parse(JSON.stringify(data)), requestId });
      return options.saveTickets ? options.saveTickets(data, requestId) : { createdCount: data.referrals.length, duplicateCount: 0 };
    },
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
    filesRead, shots, revoked, binding, args, select, readSheet, exported, saved, verify: (el = preview) => verify(el),
    save() { return preview.named("button", "Guardar en mis leads"); },
    folderOpens: () => folderOpens,
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

test("saving tickets is an explicit action independent from CSV and the host sheet, including two people sharing a phone", async () => {
  const people = [contact(), { ...contact(), fullName: "Segunda persona" }, contact(3)];
  const f = harness({ readPhoto: () => tickets(people), saveTickets: () => ({ createdCount: 1, duplicateCount: 1 }) });
  await f.readSheet();
  const save = f.save();
  const open = f.preview.named("button", "Ver mis leads");
  assert.equal(open.hidden, true);
  await save.fire("click");
  assert.equal(f.saved.length, 0);
  const exclude = f.preview.tagged("fieldset")[2].field("Incluir esta boleta");
  exclude.checked = false;
  await exclude.fire("change");
  await f.verify();
  await f.csv().fire("click");
  assert.equal(f.saved.length, 0);
  assert.equal(save.disabled, false);
  await save.fire("click");
  assert.equal(f.applied.length, 0);
  assert.equal(f.saved.length, 1);
  assert.deepEqual(f.saved[0].data, tickets(people.slice(0, 2)));
  assert.match(f.status.textContent, /Leads nuevos: 1.*repetidas: 1.*fuera: 1/);
  assert.equal(save.disabled, true);
  assert.equal(open.hidden, false);
  await open.fire("click");
  assert.equal(f.folderOpens(), 1);
  await save.fire("click");
  await f.csv().fire("click");
  assert.equal(f.saved.length, 1);
  assert.equal(f.exported.length, 2);
});

test("a failed ticket save keeps the same request key on retry and a different reviewed batch gets a fresh key", async () => {
  const initial = pending();
  let attempts = 0;
  const f = harness({ readPhoto: () => tickets(), saveTickets: () => {
    attempts++;
    if (attempts === 1) return initial.promise;
    if (attempts === 2) throw new Error("Conexión interrumpida");
    return { createdCount: 1, duplicateCount: 0 };
  } });
  await f.readSheet();
  await f.verify();
  const save = f.save(), csv = f.csv();
  const flight = save.fire("click");
  assert.equal(f.gallery.disabled, true);
  assert.equal(save.disabled, true);
  assert.equal(csv.disabled, true);
  await save.fire("click");
  assert.equal(f.saved.length, 1);
  initial.resolve({});
  await flight;
  assert.match(f.status.textContent, /No recibí la confirmación/);
  assert.equal(save.disabled, false);
  assert.equal(f.gallery.disabled, false);
  await save.fire("click");
  assert.equal(f.saved[0].requestId, f.saved[1].requestId);
  const name = f.preview.field("Nombre");
  name.value = "Nombre corregido";
  await name.fire("input");
  await save.fire("click");
  assert.equal(f.saved.length, 2);
  await f.verify();
  await save.fire("click");
  assert.notEqual(f.saved[2].requestId, f.saved[1].requestId);
});

test("a late save receipt cannot write into the next visit or seller session; visits and referrals cannot import tickets", async () => {
  const pendingSave = pending();
  const f = harness({ readPhoto: () => tickets(), saveTickets: () => pendingSave.promise });
  await f.readSheet();
  await f.verify();
  const flight = f.save().fire("click");
  f.setOwner(false);
  await f.window.fire("storage");
  pendingSave.resolve({ createdCount: 1, duplicateCount: 0 });
  await flight;
  assert.equal(f.preview.hidden, true);
  assert.equal(f.status.textContent, "");
  const visitor = harness({ readPhoto: () => tickets(), visiting: true });
  await visitor.readSheet();
  assert.equal(visitor.save(), undefined);
  assert.ok(visitor.csv());
  const referrals = harness();
  await referrals.readSheet();
  assert.equal(referrals.save(), undefined);
  assert.ok(referrals.confirm());
});

test("el botón de referencias y el de Mis leads abren el mismo lector inicialmente cerrado", () => {
  const photo = appPage.match(/<dialog class="fourteen-photo-tool"[^>]*>([\s\S]*?)<\/dialog>/);
  assert.ok(photo);
  assert.doesNotMatch(photo[0].split(">")[0], /\sopen(?:\s|$)/);
  assert.match(photo[1], /data-fourteen-photo-close/);
  assert.match(photo[1], /id="fourteen-photo-notice"[\s\S]*OpenAI[\s\S]*capture="environment" data-fourteen-photo-camera/);
  assert.match(photo[1], /data-fourteen-photo\s*\//);
  assert.equal(appPage.match(/data-fourteen-photo-read/g)?.length, 1);
  const card=appPage.slice(appPage.indexOf('id="paso-4-en-14"'), appPage.indexOf('id="tec-fourteen-sheet"'));
  assert.match(card,/data-fourteen-sheet-toggle[\s\S]*data-fourteen-photo-toggle aria-haspopup="dialog" aria-controls="tec-referral-photo" aria-expanded="false"/);
  assert.doesNotMatch(card,/data-fourteen-photo-read/);
  const folder = appPage.slice(appPage.indexOf("<h3>Mis leads</h3>"), appPage.indexOf("data-lead-folder-wrap"));
  assert.match(folder, /data-jump-ticket-photo aria-haspopup="dialog" aria-controls="tec-referral-photo" aria-expanded="false"/);
});

test("cerrar y reabrir la foto conserva el borrador, sin volver a transcribir ni cambiar de pestaña", async () => {
  const launch=new Element("button"), jump=new Element("button"), close=new Element("button"), dialog=new Element("dialog");
  const field=new Element("input");
  dialog.append(close,field);
  let bindings=0;
  dialog.querySelector=()=>close;
  dialog.showModal=()=>{dialog.open=true;};dialog.close=()=>{dialog.open=false;dialog.fire("close");};
  const nodes={"[data-fourteen-photo-tool]":dialog,"[data-fourteen-photo-toggle]":launch,"[data-jump-ticket-photo]":jump};
  const context={document:{querySelector:s=>nodes[s]},window:{TecReferralPhoto:{bind(){bindings++;}}},
    form:{},isCurrentSeller:()=>true,reviewSheets(){},visiting:false};
  const a=appSource.indexOf('  const photoTool = document.querySelector("[data-fourteen-photo-tool]")');
  const b=appSource.indexOf("  const exportSheet =",a);
  vm.runInNewContext(appSource.slice(a,b),context);
  await launch.fire("click");field.value="Teléfono corregido";assert.equal(dialog.open,true);
  await close.fire("click");assert.equal(dialog.open,false);assert.equal(launch["aria-expanded"],"false");
  await jump.fire("click");assert.equal(dialog.open,true);assert.equal(jump["aria-expanded"],"true");
  assert.equal(field.value,"Teléfono corregido");assert.equal(bindings,1);
});

test("the integration sends reviewed rows once with an account-bound key and opens the host form only when applying referrals", async () => {
  const start = appSource.indexOf('  const photoTool = document.querySelector("[data-fourteen-photo-tool]")');
  const end = appSource.indexOf("  const exportSheet =", start);
  const requests = [], toggles = [];
  let bound, own = true, loads = 0;
  const context = {
    window: { TecReferralPhoto: { bind: args => { bound = args; } } }, document: { querySelector: () => null },
    form: { scrollIntoView() {}, querySelector:()=>({focus(){}}) }, isCoachWorkspaceAvailable:()=>true, setCoachWorkspaceTab:()=>true, accountId: "seller-a", visiting: false, isCurrentSeller: () => own, reviewSheets() {},
    createTecOfflineId: () => "stable-test-key", apiRequest: async (url, options) => {
      requests.push({ url, options: JSON.parse(JSON.stringify(options)) });
      return { createdCount: 1, duplicateCount: 0 };
    }, loadLeads: async () => { loads++; }, sheetTools: { applyPhoto: () => ({ applied: true }) },
    FOURTEEN_SHEET_MAX_REFERRALS: 11, appendReferralRow() {}, openTicketFolder() {}, syncToggle: open => toggles.push(open)
  };
  vm.runInNewContext(appSource.slice(start, end), context);
  await bound.saveTickets(tickets(), bound.createRequestId());
  assert.equal(requests[0].url, "/api/coach/leads/from-tickets");
  assert.deepEqual(requests[0].options.body, { accountId: "seller-a", requestId: "stable-test-key", reviewed: true, referrals: [contact()] });
  assert.equal(requests[0].options.offlineQueue, undefined);
  assert.equal(loads, 1);
  assert.equal(toggles.length, 0);
  own = false;
  await assert.rejects(bound.saveTickets(tickets(), "stable-test-key"), /cuenta/);
  assert.equal(requests.length, 1);
  own = true;
  bound.apply(sheet());
  assert.deepEqual(toggles, [true]);
});

test("contact links and labels preserve international numbers even when the total length equals a US number", () => {
  const dial = appSource.slice(appSource.indexOf("function normalizeLeadPhone("), appSource.indexOf("function formatLeadNextActionLabel("));
  const format = appSource.slice(appSource.indexOf("function formatLeadPhone("), appSource.indexOf("function formatLeadStatusLabel("));
  const helpers = vm.runInNewContext(dial + format + "\n({leadDialPhone,formatLeadPhone});");
  assert.equal(helpers.leadDialPhone("+49 1234 5678"), "+4912345678");
  assert.equal(helpers.formatLeadPhone("+49 1234 5678"), "+49 1234 5678");
  assert.equal(helpers.leadDialPhone("5550100001"), "+15550100001");
  assert.equal(helpers.leadDialPhone("+1 (555) 010-0001"), "+15550100001");
});

test("the same folder stays in Prospección when available, otherwise sellers reach it in Mi actividad", () => {
  const start = appSource.indexOf('  const leadFolder = document.querySelector("[data-personal-lead-folder]")');
  const end = appSource.indexOf("\n}", start);
  for (const hidden of [false, true]) {
    const folder = { dataset: { coachWorkspaceSection: "prospeccion" } };
    const document = { querySelector: selector => ({
      "[data-personal-lead-folder]": folder, '[data-coach-workspace-tab="prospeccion"]': { hidden },
      '[data-coach-workspace-tab="actividad"]': { hidden: false }
    })[selector] };
    vm.runInNewContext(appSource.slice(start, end), { document, isTelemarketing: false });
    assert.equal(folder.dataset.coachWorkspaceSection, hidden ? "actividad" : "prospeccion");
  }
  const folderStart = appSource.indexOf("  const openTicketFolder = () => {");
  const folderEnd = appSource.indexOf("  initFourteenSheetTool(", folderStart);
  const state = { panel: "program_4_14", filter: "cliente" };
  const actions = [];
  const open = vm.runInNewContext(appSource.slice(folderStart, folderEnd) + "\nopenTicketFolder", {
    state, folderWrap: { closest: () => ({ dataset: { coachWorkspaceSection: "actividad" } }) },
    setCoachWorkspaceTab(tab) { actions.push(tab); return true; }, syncFolderToggle(value) { actions.push(value); },
    renderSummary() {}, renderLeadList() {}, loadLeads: async () => {},
    folderToggle: { focus() { actions.push("focus"); }, scrollIntoView() {} }
  });
  open();
  assert.deepEqual(actions, ["actividad", true, "focus"]);
  assert.deepEqual(state, { panel: "rifa_capture", filter: "todos" });
});

function inboxHarness({ native = false, visiting = false } = {}) {
  const start = appSource.indexOf("  let listRevision = 0;");
  const end = appSource.indexOf("  syncCaptureToggle(false);", start);
  const calls = [], responses = [];
  const state = { leads: [] };
  const surface = { contacts: [], folderOpen: true };
  const context = {
    state, accountId: "owner-1", window: new Element(), document: new Element(), visiting, navigator: { onLine: true },
    COACH_ACTIVE_SELLER_KEY: "seller-key", isActiveCoachSeller: id => id === "owner-1",
    renderSummary() {}, renderLeadList() { surface.contacts = [...state.leads]; }, syncFolderToggle(value) { surface.folderOpen = value; },
    apiRequest: url => { calls.push(url); const response = pending(); responses.push(response); return response.promise; },
    tecNativeCachedList: async url => { calls.push(url); return { leads: [] }; }
  };
  context.window.TecNativeOffline = native;
  const load = vm.runInNewContext(appSource.slice(start, end) + "\nloadLeads", context);
  return { calls, responses, state, surface, context, load };
}

test("private raffle leads are fetched only online from the seller's URL and an obsolete response cannot replace a newer list", async () => {
  const { calls, responses, context, load } = inboxHarness();
  const first = load(), next = load();
  const records = [{ personalTicket: true, id: "qa" }];
  responses[1].resolve({ leads: records });
  await next;
  responses[0].resolve({ leads: [] });
  await first;
  assert.equal(context.state.leads, records);
  assert.deepEqual(calls, ["/api/coach/leads?accountId=owner-1", "/api/coach/leads?accountId=owner-1"]);
  context.visiting = true;
  await load();
  context.visiting = false;
  context.navigator.onLine = false;
  await load();
  assert.deepEqual(calls.slice(2), ["/api/coach/leads", "/api/coach/leads"]);
});

test("seller changes, local locks and a lost session immediately clear rendered contacts and reject late results", async () => {
  const cases = [
    async h => h.context.window.fire("storage", { key: "seller-key", newValue: "owner-2" }),
    async h => h.context.window.fire("storage", { key: "seller-key", newValue: null }),
    async h => h.context.window.fire("storage", { key: "tec_coach_local_lock", newValue: "locked" }),
    async h => { h.context.isActiveCoachSeller = () => false; await h.context.window.fire("tec:coach-session-change"); },
    async h => { h.context.isActiveCoachSeller = () => false; await h.context.document.fire("visibilitychange"); }
  ];
  for (const revoke of cases) {
    const h = inboxHarness();
    const first = h.load(), records = [{ id: "first", personalTicket: true }];
    h.responses[0].resolve({ leads: records });
    await first;
    assert.equal(h.surface.contacts.length, 1);
    const late = h.load();
    await revoke(h);
    assert.equal(h.surface.contacts.length, 0);
    assert.equal(h.state.leads.length, 0);
    assert.equal(h.surface.folderOpen, false);
    h.responses[1].resolve({ leads: records });
    await late;
    assert.equal(h.surface.contacts.length, 0);
    await h.load();
    assert.equal(h.calls.length, 2);
  }
  for (const serverDenied of [false, true]) {
    const h = inboxHarness({ native: true });
    const records = [{ id: "stale", personalTicket: true }];
    const first = h.load();
    h.responses[0].resolve({ leads: records }); await first;
    const next = h.load();
    if (serverDenied) {
      h.responses[1].reject(Object.assign(new Error("different seller"), { status: 403 }));
      await assert.rejects(next, /different seller/);
    } else {
      h.context.isActiveCoachSeller = () => false;
      h.responses[1].resolve({ leads: records }); await next;
    }
    assert.equal(h.state.leads.length, 0);
    assert.equal(h.surface.contacts.length, 0);
    assert.equal(h.surface.folderOpen, false);
  }
});

test("native online refresh also updates the historical list, which excludes private tickets on cache writes and offline reads", async () => {
  const h = inboxHarness({ native: true });
  const start = appSource.indexOf("async function tecNativeCachedList(");
  const end = appSource.indexOf("async function getTecCachedViewer(", start);
  const written = [], paths = [], privateRow = { id: "secret", personalTicket: true }, ordinary = { id: "ordinary" };
  let offline = false;
  const historical = vm.runInNewContext(appSource.slice(start, end) + "\ntecNativeCachedList", {
    window: { TecNativeOffline: true }, getTecCachedViewer: async () => ({ user: { email: "qa@example.invalid" } }),
    verifyTecNativeSyncOwner: async () => {}, tecOfflinePut: async (_store, entry) => { written.push(entry); },
    tecOfflineGet: async () => ({ ownerEmail: "qa@example.invalid", value: { leads: [ordinary, privateRow] } }),
    apiRequest: async url => {
      paths.push(url);
      if (offline) throw new Error("offline");
      return { leads: [ordinary, privateRow] };
    }
  });
  h.context.tecNativeCachedList = historical;
  const request = h.load();
  h.responses[0].resolve({ leads: [ordinary, privateRow] }); await request;
  assert.deepEqual(paths, ["/api/coach/leads"]);
  assert.equal(written.length, 1);
  assert.deepEqual(Array.from(written[0].value.leads), [ordinary]);
  assert.equal(h.surface.contacts.length, 2);
  const retry = h.load(); h.responses[1].reject(new Error("timeout")); await retry;
  assert.deepEqual(h.surface.contacts, [ordinary]);
  offline = true; h.context.navigator.onLine = false;
  await h.load();
  assert.deepEqual(h.surface.contacts, [ordinary]);
});

test("ticket saves accept 11 selected rows and leave a larger verified set available for local CSV", async () => {
  const f = harness({ readPhoto: () => tickets(Array.from({ length: 12 }, (_, i) => contact(i + 1))) });
  await f.readSheet();
  await f.verify();
  await f.save().fire("click");
  assert.match(f.status.textContent, /hasta 11 boletas por guardado/);
  assert.equal(f.saved.length, 0);
  await f.csv().fire("click");
  assert.equal(f.exported[0].referrals.length, 12);
  const include = f.preview.tagged("fieldset")[11].field("Incluir esta boleta");
  include.checked = false; await include.fire("change");
  await f.save().fire("click");
  assert.equal(f.saved[0].data.referrals.length, 11);
});

test("ticket request IDs fit the import contract using both browser UUIDs and the legacy random fallback", () => {
  const start = appSource.indexOf("function createTecOfflineId("), end = appSource.indexOf("function openTecOfflineDb(", start);
  for (const crypto of [{ randomUUID: () => "56368e78-b168-4b42-9d9b-301654e8fdcb" }, undefined]) {
    const id = vm.runInNewContext(appSource.slice(start, end) + '\ncreateTecOfflineId("ticket")', { window: { crypto } });
    assert.match(id, /^[a-zA-Z0-9_-]{8,100}$/);
  }
});
