// La Hoja TEC que crea el Coach: mismo formato que usan los telemarketers, lectura por el lector de siempre
// y Resumen con fórmulas sobre las columnas correctas. Todo con datos ficticios.
import test from 'node:test';
import assert from 'node:assert/strict';
import { randomBytes } from 'node:crypto';
import { TEC_SHEET_IDS, tecSheetCreateBody, tecSheetRequests } from '../src/coach-sheets-template.js';
import { layout } from '../src/coach-sheets-google.js';
import { createCoachSheetsConnections, sheetsApi, sheetsScope } from '../src/coach-sheets-connections.js';
import { connectionsPage } from '../src/coach-connections-page.js';
import { sealToken } from '../src/coach-hycite-gmail.js';

const written = (requests, sheetId) => {
  const grid = [];
  for (const r of requests) {
    const u = r.updateCells;
    if (!u || u.start.sheetId !== sheetId) continue;
    u.rows.forEach((row, i) => row.values.forEach((cell, j) => {
      const v = cell.userEnteredValue; if (!v) return;
      (grid[u.start.rowIndex + i] ||= [])[u.start.columnIndex + j] = v.stringValue ?? v.formulaValue ?? v.numberValue;
    }));
  }
  return grid;
};
const asHeaderRows = grid => grid.slice(0, 3).map(row => Array.from(row || [], v => ({ formattedValue: v ?? '' })));

test('la hoja nace con sus siete pestañas, sin datos de clientes, y los mismos ID que guarda el Coach', () => {
  const body = tecSheetCreateBody();
  assert.deepEqual(body.sheets.map(s => s.properties.title), ['Cómo usar','Resumen','Agenda','4 en 14','Boletas','Leyenda de colores','Listas']);
  assert.equal(body.sheets.find(s => s.properties.title === '4 en 14').properties.sheetId, TEC_SHEET_IDS.referrals);
  assert.equal(body.sheets.find(s => s.properties.title === 'Boletas').properties.sheetId, TEC_SHEET_IDS.tickets);
  const requests = tecSheetRequests();
  const text = JSON.stringify(requests);
  assert.doesNotMatch(text, /555-01|Ficticio|example\.invalid/);
  assert.equal(written(requests, TEC_SHEET_IDS.referrals).length, 4, 'solo encabezados y la fórmula de contacto');
  assert.equal(written(requests, TEC_SHEET_IDS.agenda).length, 1);
});

test('el lector de siempre reconoce el 4 en 14 con anfitrión agrupado y las Boletas, con historial, fecha y estado', () => {
  const requests = tecSheetRequests();
  const referrals = layout({ sheetId: 1, title: '4 en 14' }, asHeaderRows(written(requests, TEC_SHEET_IDS.referrals)), 'referrals');
  assert.equal(referrals.grouped, true);
  assert.equal(referrals.headerRow, 2);
  assert.deepEqual({ host: referrals.cols.host, name: referrals.cols.fullName, phone: referrals.cols.phone, history: referrals.cols.history,
    next: referrals.cols.followupDate, status: referrals.cols.status, result: referrals.cols.result }, { host: 0, name: 2, phone: 3, history: 10, next: 15, status: 13, result: 11 });
  const tickets = layout({ sheetId: 2, title: 'Boletas' }, asHeaderRows(written(requests, TEC_SHEET_IDS.tickets)), 'tickets');
  assert.deepEqual({ name: tickets.cols.fullName, phone: tickets.cols.phone, email: tickets.cols.email, history: tickets.cols.history,
    next: tickets.cols.followupDate, status: tickets.cols.status }, { name: 0, phone: 2, email: 1, history: 19, next: 25, status: 23 });
});

test('el Resumen cuenta con fórmulas sobre las columnas de seguimiento y los colores siguen al estado', () => {
  const requests = tecSheetRequests();
  const summary = written(requests, TEC_SHEET_IDS.summary);
  assert.equal(summary[8][1], `=COUNTIFS('4 en 14'!$F$4:$F,">="&B$5,'4 en 14'!$F$4:$F,"<"&B$6)`);
  assert.equal(summary[9][2], '=COUNTIFS(Boletas!$V$2:$V,">="&C$5,Boletas!$V$2:$V,"<"&C$6)');
  assert.match(summary[32][2], /'4 en 14'!\$Q\$4:\$Q,"Sí"/);
  assert.match(summary[32][2], /Boletas!\$AA\$2:\$AA,"Sí"/);
  const rules = requests.filter(r => r.addConditionalFormatRule).map(r => r.addConditionalFormatRule.rule);
  assert.equal(rules.length, 12);
  assert.ok(rules.some(r => r.booleanRule.condition.values[0].userEnteredValue === '=OR($N4="Cita")'));
  assert.ok(rules.some(r => r.booleanRule.condition.values[0].userEnteredValue === '=OR($X2="Solo mañanas")'));
});

test('«Crear mi Hoja TEC» crea la hoja con el permiso del vendedor y la deja conectada a sus dos pestañas', async () => {
  const key = randomBytes(32), grant = 'g'.repeat(43);
  const records = new Map([['a', { _id: 'a', grant, encryptedToken: sealToken('refresh-a', key, 'sheets-refresh:a'), mailbox: 'a@example.invalid',
    spreadsheetId: 'hoja-vieja-1234567', ticketsSheetId: 0, referralsSheetId: 9 }]]);
  const matches = (doc, f) => doc && Object.entries(f).every(([k, v]) => doc[k] === v);
  const store = {
    findOne: f => ({ lean: async () => matches(records.get(f._id), f) ? structuredClone(records.get(f._id)) : null }),
    exists: async f => Boolean(matches(records.get(f._id), f)),
    async updateOne(f, u) { const doc = records.get(f._id); if (!matches(doc, f)) return { matchedCount: 0 }; Object.assign(doc, u.$set); return { matchedCount: 1 }; }
  };
  const created = [];
  const google = { refresh: async token => ({ access_token: token + '-current', scope: sheetsScope }),
    createTecSheet: async (token, options) => { created.push([token, options]); return { id: 'nueva-hoja-tec-123', title: 'Hoja TEC' }; } };
  const routes = new Map();
  let current = true;
  createCoachSheetsConnections({ config: { sheetsReady: true, key }, google, store })
    .register({ post: (path, h) => routes.set(path, h) }, h => h, (_res, notice) => notice);
  const press = body => routes.get(sheetsApi + 'create')({ body }, {}, { id: 'a', isSessionCurrent: async () => current });
  // Un formulario viejo, con otro permiso, no crea nada.
  await assert.rejects(press({ grant: 'x'.repeat(43) }), e => e.status === 409);
  assert.equal(created.length, 0);
  assert.equal(await press({ grant }), 'sheetsCreated');
  assert.deepEqual(created, [['refresh-a-current', { title: 'Hoja TEC', timeZone: 'America/Chicago' }]]);
  const doc = records.get('a');
  assert.deepEqual({ id: doc.spreadsheetId, tickets: doc.ticketsSheetId, referrals: doc.referralsSheetId, t: doc.ticketsTitle, r: doc.referralsTitle },
    { id: 'nueva-hoja-tec-123', tickets: TEC_SHEET_IDS.tickets, referrals: TEC_SHEET_IDS.referrals, t: 'Boletas', r: '4 en 14' });
  assert.notEqual(doc.grant, grant, 'las propuestas preparadas con la hoja anterior ya no se pueden confirmar');
  // Si la sesión cambió mientras Google creaba la hoja, no se conecta.
  current = false;
  assert.equal(await press({ grant: doc.grant }), 'expired');
  assert.equal(records.get('a').spreadsheetId, 'nueva-hoja-tec-123');
});

test('Mis conexiones ofrece crear la Hoja TEC cuando Google Sheets está autorizado', () => {
  const html = connectionsPage({ doc: null, cal: null, pilot: false, ready: false, calendarReady: false, contacts: null, contactsReady: false, contactsPilot: false,
    sheets: { grant: 'g'.repeat(43), mailbox: 'a@example.invalid' }, sheetsReady: true, sheetsPilot: true, coachEmail: 'a@example.invalid', csrf: 'c', message: '' });
  assert.match(html, /action="\/api\/coach\/conexiones\/sheets\/create"/);
  assert.match(html, /Crear mi Hoja TEC/);
  assert.match(html, /name="grant" value="g{43}"/);
  assert.doesNotMatch(html, /<script/);
});
