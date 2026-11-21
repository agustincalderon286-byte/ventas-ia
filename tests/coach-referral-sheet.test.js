import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import test from "node:test";
import vm from "node:vm";

const context = { window: {}, Event };
vm.runInNewContext(readFileSync(new URL("../public/coach/referral-sheet.js", import.meta.url), "utf8"), context);
const tools = context.window.TecReferralSheet;
const sheet = {
  hostName: 'Julia "Las Palmas", Pérez', hostPhone: "+1 (312) 555-0100",
  notes: "nota privada del anfitrión", representativeName: "Vendedor de prueba", giftSelected: "Tazones", startDate: "2026-11-21T10:00:00.000Z",
  referrals: [{ fullName: 'Luz, Muñoz', phone: "312–555–0101", address: '9, Calle "Sur"', notes: "Llamar, después de las 6" },
    { fullName: "=1+2", phone: "312-555-0102", address: "" }]
};

test("el CSV mantiene la fila, los acentos, las comas y las comillas, y exporta solo las notas de cada referido", () => {
  assert.equal(tools.review(sheet), "");
  const rows = tools.csv(sheet).split("\r\n");
  assert.equal(rows.length, 4);
  assert.equal(rows[0].charCodeAt(0), 0xfeff);
  assert.equal(rows[1], '"Julia ""Las Palmas"", Pérez","3125550100","Luz, Muñoz","3125550101","9, Calle ""Sur""","Llamar, después de las 6","Vendedor de prueba","Tazones","2026-11-21"');
  assert.ok(rows[2].includes('"\'=1+2"'));
  assert.ok(!rows.join("").includes("nota privada del anfitrión"));
});

test("rechaza dígitos parciales, filas incompletas y un mismo número con o sin prefijo de EE. UU.", () => {
  for (const invalidRow of [{fullName: "Eva", phone: "312-55?0101"}, {fullName: "", phone: "3125550103"},
    {fullName: "Repetida", phone: "+1 (312) 555-0101"}]) {
    assert.ok(tools.review({...sheet, referrals: [sheet.referrals[0], invalidRow]}));
  }
  assert.ok(tools.review({...sheet, hostPhone: "12"}));
});

function captureForm() {
  const fields = new Map();
  let rows = 0, edits = 0;
  const add = name => fields.set(name, {value:""});
  for (const key of ["hostName","hostPhone","startDate","giftSelected","representativeName"]) add(key);
  const append = () => {
    rows++;
    for (const suffix of ["Name","Phone","Address","Notes","City","Relationship"]) add("referral" + suffix + rows);
  };
  append();
  return {fields, append, form: {
    elements: {namedItem: name => fields.get(name)},
    querySelectorAll: () => Array(rows).fill({}),
    dispatchEvent: () => { edits++; }
  }, edits: () => edits};
}

test("acepta la transcripción revisada sin perder filas parciales y permite exportar los datos corregidos", () => {
  const {form, fields, append, edits} = captureForm();
  fields.get("referralNotes1").value = "Completar más tarde";
  assert.equal(tools.applyPhoto(form, sheet, 11, append).applied, true);
  assert.equal(fields.get("referralNotes1").value, "Completar más tarde");
  assert.equal(fields.get("referralName2").value, "Luz, Muñoz");
  assert.equal(edits(), 1);
  assert.ok(tools.review(tools.readForm(form)));
  fields.get("referralNotes1").value = "";
  fields.get("referralName2").value = "Luz Corregida";
  assert.equal(tools.review(tools.readForm(form)), "");
  assert.match(tools.csv(tools.readForm(form)), /Luz Corregida/);
});

test("si la foto repite un teléfono, supera el espacio o difiere el anfitrión, conserva el borrador completo", () => {
  const {form, fields, append, edits} = captureForm();
  fields.get("referralName1").value = "Luz";
  fields.get("referralPhone1").value = "+1 (312) 555-0101";
  assert.equal(tools.applyPhoto(form, sheet, 11, append).applied, false);
  assert.equal(tools.applyPhoto(form, {...sheet, referrals: [sheet.referrals[1]]}, 1, append).applied, false);
  fields.get("hostName").value = "Otro anfitrión";
  assert.equal(tools.applyPhoto(form, {...sheet, referrals: []}, 11, append).applied, false);
  assert.equal(edits(), 0);
  assert.equal(fields.get("hostPhone").value, "");
  assert.equal(fields.has("referralName2"), false);
});

test("el CSV de boletas conserva datos revisados y prescinde de anfitrión", () => {
  const ticket = {fullName:"Persona de prueba", phone:"+1 (312) 555-0101", address:"Prueba, 1",
    city:"Chicago", relationship:"", location:"Boleta superior derecha"};
  const output = tools.csv({documentType:"tickets", referrals:[ticket]});
  assert.match(output, /"Persona de prueba","3125550101","Prueba, 1","Chicago","","Boleta superior derecha"/);
  assert.doesNotMatch(output, /Anfitrión/);
  assert.equal(tools.applyPhoto(captureForm().form, {documentType:"tickets"}, 11, () => {}).applied, false);
});

test("una foto solo acepta teléfonos completos y la ciudad y el parentesco llegan al CSV de referidos", () => {
  for (const value of ["555-0101", "+1 312-55", "(312) 555-010?", "+1234567890"]) assert.equal(tools.validPhotoPhone(value), false);
  for (const value of ["312-555-0104", "+1 312-555-0104", "+52 55 5555 0104"]) assert.equal(tools.validPhotoPhone(value), true);
  const {form,append}=captureForm();
  const photo={...sheet,documentType:"referrals",referrals:[{...sheet.referrals[0],city:"Chicago",relationship:"Prima"}]};
  assert.equal(tools.applyPhoto(form,photo,11,append).applied,true);
  const out=tools.csv(tools.readForm(form));
  assert.match(out, /"Ciudad o zona","Relación con el anfitrión"/);
  assert.match(out, /"Chicago","Prima"/);
});
