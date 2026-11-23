// Agenda y Resumen de la Hoja TEC para el asistente, y el paso de leads de la hoja anterior.
// Todo es ficticio: nombres inventados, teléfonos 555-01xx y dominios example.invalid.
import test from 'node:test';
import assert from 'node:assert/strict';
import { createCoachSheetTools } from '../src/coach-sheets-tools.js';
import { createAgentTools, agentTools } from '../src/coach-agent-tools.js';
import { AGENDA_HEADERS, TEC_SHEET_IDS } from '../src/coach-sheets-template.js';
import { planImport, runImport } from '../src/coach-sheets-import.js';

const ZONE = 'America/Chicago', NOW = Date.parse('2026-11-23T15:00:00Z');
const serial = iso => (Date.parse(iso + 'T00:00:00Z') - Date.UTC(1899, 11, 30)) / 86400000;

function fakeGoogle({ agenda = true } = {}) {
  const writes = [];
  const rows = [AGENDA_HEADERS,
    [serial('2026-11-20'), 19 / 24, 'Vendedor de prueba', 'Telemarketer B', 'Gloria Inventada', '773-555-0125', '606 Calle Ejemplo, Cicero, IL', 'Boletas', 'Le interesa el filtro.', 'Demo sin venta', '', 2, ''],
    [serial('2026-11-24'), 17 / 24, 'Vendedor de prueba', 'Telemarketer A', 'Laura Inventada', '312-555-0108', '707 Calle Ejemplo, Blue Island, IL', '4 en 14', 'Amiga de la anfitriona.', 'Pendiente', '', '', ''],
    ['', '', '', '', '', '', '', '', '', '', '', '', '']];
  const summary = [['Resumen de resultados'], [], [], ['Periodo', 'Esta semana', 'Este mes', 'Mes pasado'], ['Desde', '16/11/2026', '01/11/2026', '01/10/2026'],
    ['Hasta (sin incluir)', '23/11/2026', '01/12/2026', '01/11/2026'], [], ['Leads'], ['Leads nuevos del 4 en 14', '4', '8', '3'], ['Leads nuevos de boletas', '3', '5', '2'],
    ['Leads nuevos en total', '7', '13', '5'], [], ['Citas y ventas'], ['Citas en la agenda', '2', '5', '2'], ['Demos hechas', '2', '3', '2'],
    ...Array(13).fill(['x', '0', '0', '0']), [], [], [],
    ['Telemarketer', 'Leads trabajados', 'Contactos', 'Citas agendadas'], ['Telemarketer A', '5', '4', '3'], ['Telemarketer B', '4', '3', '2']];
  return { writes, rows,
    tecTabs: async () => ({ agenda: agenda ? { sheetId: TEC_SHEET_IDS.agenda, gridProperties: { rowCount: 2000 } } : null, summary: agenda ? { sheetId: 4 } : null }),
    rangeValues: async (_a, _b, range, raw) => range.startsWith("'Agenda'") ? (assert.equal(raw, true), structuredClone(rows)) : structuredClone(summary),
    writeCells: async (_a, _b, args) => { writes.push(args); rows[args.row - 1].splice(args.column, 4, ...args.cells); } };
}
const ctxFor = () => ({ owner: 'o1', session: 's1', grant: 'g1', access: 't', workbook: { spreadsheetId: 'hoja-tec-123456', ticketsSheetId: 2, referralsSheetId: 1 }, isCurrent: async () => true });

test('la Agenda se lee con fecha y hora reales, y el resultado se guarda solo si la fila no cambió', async () => {
  const google = fakeGoogle(), tools = createCoachSheetTools({ google }), ctx = ctxFor();
  const data = await tools.agenda(ctx, { from: '2026-11-20', to: '2026-11-30' });
  assert.deepEqual(data.rows.map(r => [r.row, r.date, r.time, r.client, r.result]),
    [[2, '2026-11-20', '19:00', 'Gloria Inventada', 'Demo sin venta'], [3, '2026-11-24', '17:00', 'Laura Inventada', 'Pendiente']]);
  const laura = data.rows[1];
  await assert.rejects(tools.prepareResult(ctx, { row: 3, key: laura.key, result: 'Vendió' }), e => e.status === 422);
  const made = await tools.prepareResult(ctx, { row: 3, key: laura.key, result: 'Venta', sale: 2499, referrals: 6, note: 'Sistema de 10 piezas' });
  assert.match(made.preview, /Agenda, fila 3 · Laura Inventada · 2026-11-24 17:00\. Resultado: Pendiente → Venta\. Venta: \$2,499\. Referidos: 6\. Nota: «Sistema de 10 piezas»/);
  assert.equal(google.writes.length, 0, 'preparar no escribe');
  // Si alguien cambia la fila antes del botón, no se guarda.
  google.rows[2][9] = 'Canceló';
  await assert.rejects(tools.confirmResult(ctx, made.proposal), e => e.status === 409);
  google.rows[2][9] = 'Pendiente';
  const saved = await tools.confirmResult(ctx, made.proposal);
  assert.equal(saved.status, 'saved');
  assert.deepEqual(google.writes[0], { sheetId: TEC_SHEET_IDS.agenda, row: 3, column: 9, cells: ['Venta', 2499, 6, 'Sistema de 10 piezas'] });
});

test('el Resumen llega con sus periodos, sus números y la tabla de telemarketers', async () => {
  const data = await createCoachSheetTools({ google: fakeGoogle() }).summary(ctxFor());
  assert.deepEqual(data.periods, ['Esta semana', 'Este mes', 'Mes pasado']);
  assert.deepEqual(data.metrics[0], { nombre: 'Leads nuevos del 4 en 14', valores: ['4', '8', '3'] });
  assert.deepEqual(data.telemarketers[0], { Telemarketer: 'Telemarketer A', 'Leads trabajados': '5', Contactos: '4', 'Citas agendadas': '3' });
  assert.equal(await createCoachSheetTools({ google: fakeGoogle({ agenda: false }) }).summary(ctxFor()), null);
});

test('el asistente lee la Agenda con marcadores, ve qué cita falta en el calendario y prepara el resultado', async () => {
  const google = fakeGoogle(), sheetTools = createCoachSheetTools({ google });
  const config = { pilot: ['a@example.invalid'], sheetsPilot: ['a@example.invalid'], calendarReady: true, sheetsReady: true };
  const sheets = { access: async () => ({ token: 't', grant: 'g1', spreadsheetId: 'hoja-tec-123456', ticketsSheetId: 2, referralsSheetId: 1 }), current: async () => true };
  const channels = { findAppointments: async (_a, { name }) => ({ matches: name === 'Laura Inventada' ? [] : [{}] }) };
  const actor = { owner: 'o1', session: 's1', email: 'a@example.invalid', name: 'Agustín', zone: ZONE, current: async () => true, now: () => NOW };
  const turn = {}, refs = {};
  const execute = createAgentTools({ config, sheets, sheetTools, channels })(actor, refs, turn);
  const read = await execute('leer_agenda_hoja', { desde: '2026-11-20', hasta: '2026-11-30' });
  assert.equal(read.estado, 'ok');
  const laura = read.citas.find(c => c.cliente === 'Laura Inventada');
  assert.equal(laura.enCalendario, 'no');
  assert.equal(read.citas.find(c => c.cliente === 'Gloria Inventada').enCalendario, 'sin revisar', 'una cita pasada no se busca en el calendario');
  assert.match(laura.telefono, /^\[\[tel:[a-f0-9]{6}\]\]$/);
  assert.doesNotMatch(JSON.stringify(read), /555-0108|Calle Ejemplo/);
  const made = await execute('preparar_resultado_cita', { ref: laura.ref, resultado: 'Venta', venta: 2499, referidos: 6, nota: '' });
  assert.equal(made.estado, 'preparado');
  assert.deepEqual(turn.plan.draft.ops, ['result']);
  assert.match(turn.plan.draft.steps.result.preview, /Pendiente → Venta/);
  const summary = await execute('leer_resumen', {});
  assert.equal(summary.estado, 'ok');
  assert.equal(summary.numeros[1].nombre, 'Leads nuevos de boletas');
  for (const name of ['leer_agenda_hoja', 'preparar_resultado_cita', 'leer_resumen']) {
    const tool = agentTools.find(t => t.name === name);
    assert.deepEqual(tool.parameters.required, Object.keys(tool.parameters.properties));
  }
});

test('sin Hoja TEC el asistente explica cómo crearla en lugar de inventar la agenda', async () => {
  const sheetTools = createCoachSheetTools({ google: fakeGoogle({ agenda: false }) });
  const config = { pilot: [], sheetsPilot: ['a@example.invalid'], sheetsReady: true };
  const sheets = { access: async () => ({ token: 't', grant: 'g1', spreadsheetId: 'hoja-vieja-123456', ticketsSheetId: 0, referralsSheetId: 9 }), current: async () => true };
  const actor = { owner: 'o1', session: 's1', email: 'a@example.invalid', zone: ZONE, current: async () => true, now: () => NOW };
  const execute = createAgentTools({ config, sheets, sheetTools })(actor, {}, {});
  const result = await execute('leer_resumen', {});
  assert.equal(result.estado, 'no_disponible');
  assert.match(result.nota, /Crear mi Hoja TEC/);
});

test('los leads de la hoja anterior pasan en bloques por anfitrión, solo los del vendedor y solo a una Hoja TEC vacía', async () => {
  const old = [
    { kind: 'referrals', fullName: 'Pedro Ficticio', host: 'Rosa Prueba', phone: '312-555-0101', city: 'Blue Island', address: '', seller: 'Agustín', history: 'nc', result: '', followupDate: '2026-11-25', status: 'Cita' },
    { kind: 'referrals', fullName: 'Luisa Ejemplo', host: 'Rosa Prueba', phone: '312-555-0102', city: 'Harvey', address: '', seller: 'Agustín', history: '', result: '', followupDate: '', status: 'Interesada' },
    { kind: 'referrals', fullName: 'Mario Muestra', host: 'Marta Demo', phone: '708-555-0103', city: '', address: '', seller: 'Rigoberto', history: '', result: '', followupDate: '', status: '' },
    { kind: 'tickets', fullName: 'Claudia Muestra', host: '', phone: '815-555-0121', email: 'claudia@example.invalid', city: 'Joliet', address: '', seller: '', history: 'Receptiva', result: 'Demo sin venta', followupDate: '', status: 'No interesado' }];
  let tecRows = [];
  const written = [];
  const reader = { readFollowups: async (_a, book) => ({ rows: book.spreadsheetId === 'hoja-tec-123456' ? tecRows : old }),
    writeRows: async (_a, _b, args) => written.push(args) };
  const books = { oldBook: { spreadsheetId: 'hoja-vieja-123456' }, newBook: { spreadsheetId: 'hoja-tec-123456' }, sellerName: 'Agustín Calderón' };
  const planned = await planImport(reader, 't', books);
  assert.equal(planned.skipped, 1, 'el referido de otro vendedor no pasa');
  assert.deepEqual(planned.referrals.map(r => [r[0], r[1], r[2], r[13]]), [['Rosa Prueba', 1, 'Pedro Ficticio', 'Cita'], ['', 2, 'Luisa Ejemplo', '']]);
  assert.match(planned.referrals[1][10], /Estado anterior: Interesada/);
  assert.equal(planned.referrals[0].length, 16, 'no toca la columna que se calcula sola');
  assert.deepEqual([planned.tickets[0][0], planned.tickets[0][1], planned.tickets[0][23], planned.tickets[0].length], ['Claudia Muestra', 'claudia@example.invalid', 'No interesado', 26]);
  await runImport(reader, 't', books.newBook, planned);
  assert.deepEqual(written.map(w => [w.sheetId, w.startRow, w.rows.length]), [[TEC_SHEET_IDS.referrals, 4, 2], [TEC_SHEET_IDS.tickets, 2, 1]]);
  tecRows = [{ fullName: 'Ya hay alguien' }];
  await assert.rejects(planImport(reader, 't', books), e => e.status === 409);
});

test('Mis conexiones ofrece pasar los leads solo con una Hoja TEC recién creada, y la página no ejecuta nada', async () => {
  const { connectionsPage, importPage } = await import('../src/coach-connections-page.js');
  const base = { doc: null, cal: null, pilot: false, ready: false, calendarReady: false, contacts: null, contactsReady: false, contactsPilot: false, sheetsReady: true, sheetsPilot: true, coachEmail: 'a@example.invalid', csrf: 'c', message: '' };
  const withOld = connectionsPage({ ...base, sheets: { grant: 'g'.repeat(43), mailbox: 'a@example.invalid', spreadsheetId: 'hoja-tec-123456', spreadsheetTitle: 'Hoja TEC',
    tecSheet: true, previousSpreadsheetId: 'hoja-vieja-123456', previousSpreadsheetTitle: 'Hoja <vieja>' } });
  assert.match(withOld, /action="\/api\/coach\/conexiones\/sheets\/import"/);
  assert.match(withOld, /Hoja &lt;vieja&gt;/);
  const without = connectionsPage({ ...base, sheets: { grant: 'g'.repeat(43), mailbox: 'a@example.invalid', spreadsheetId: 'hoja-tec-123456', spreadsheetTitle: 'Hoja TEC', tecSheet: true } });
  assert.doesNotMatch(without, /sheets\/import"/);
  const page = importPage({ csrf: 'c', grant: 'g', title: 'Hoja vieja', referrals: 2, tickets: 1, skipped: 1 });
  assert.match(page, /Sí, pasar los 3 leads ahora/);
  assert.match(page, /Todavía no se ha pasado nada/);
  assert.match(page, /sheets\/import-confirm/);
  assert.doesNotMatch(page, /<script/);
});
