// Pasa los leads de la hoja anterior del vendedor a su Hoja TEC recién creada.
// Lee con el mismo lector del Coach (las mismas filas que el Coach ve del vendedor) y solo escribe
// en una Hoja TEC vacía, para no duplicar ni mezclar leads. La hoja anterior no se modifica.
import { saleScope } from './coach-sheets-agent.js';
import { LEAD_STATES, TEC_SHEET_IDS } from './coach-sheets-template.js';

const fail = (status, message) => Object.assign(new Error(message), { status });
const text = value => String(value ?? '').trim();
const fold = value => text(value).normalize('NFD').replace(/\p{M}/gu, '').toLowerCase().replace(/[^\p{L}\p{N}]+/gu, ' ').trim();
export const IMPORT_LIMIT = 5000;

// El estado anterior pasa a la lista de la Hoja TEC solo si es uno de sus estados; si no, queda en los comentarios.
function state(row) {
  const known = LEAD_STATES.find(([name]) => fold(name) === fold(row.status));
  const extra = !known && text(row.status) ? 'Estado anterior: ' + text(row.status) : '';
  return { state: known ? known[0] : '', history: [text(row.history), extra].filter(Boolean).join(' / ') };
}
const place = row => [text(row.address), text(row.city)].filter(Boolean).join(', ');

export async function planImport(reader, access, { oldBook, newBook, sellerName }) {
  const fresh = await reader.readFollowups(access, newBook);
  if (fresh.rows.length) throw fail(409, 'La Hoja TEC ya tiene leads. Solo se pueden pasar a una Hoja TEC vacía.');
  const old = await reader.readFollowups(access, oldBook, { forPlan: true, withAddress: true });
  const mine = old.rows.filter(row => saleScope(row, sellerName));
  const referrals = [], tickets = [];
  let host = null, number = 0;
  for (const row of mine.filter(r => r.kind === 'referrals')) {
    const { state: estado, history } = state(row);
    // Los referidos de un mismo anfitrión, uno tras otro, forman su bloque con el anfitrión en la primera fila.
    if (row.host !== host) { host = row.host; number = 0; }
    number += 1;
    referrals.push([number === 1 ? text(row.host) : '', number, text(row.fullName), text(row.phone), place(row), '', '', '', '',
      text(row.seller), history, text(row.result), '', estado, '', text(row.followupDate)]);
  }
  for (const row of mine.filter(r => r.kind === 'tickets')) {
    const { state: estado, history } = state(row);
    tickets.push([text(row.fullName), text(row.email), text(row.phone), '', '', '', '', '', '', '', '', '', place(row), '', '', '', '',
      text(row.seller), '', history, text(row.result), '', '', estado, '', text(row.followupDate)]);
  }
  if (referrals.length + tickets.length > IMPORT_LIMIT) throw fail(422, 'La hoja anterior tiene más de ' + IMPORT_LIMIT + ' leads. Pásalos por partes con ayuda de TEC.');
  return { referrals, tickets, skipped: old.rows.length - mine.length };
}

// Escribe sin tocar la columna «Hubo contacto», que se calcula sola.
export async function runImport(reader, access, newBook, planned) {
  if (planned.referrals.length) await reader.writeRows(access, newBook, { sheetId: TEC_SHEET_IDS.referrals, startRow: 4, rows: planned.referrals, rowCount: 2000 });
  if (planned.tickets.length) await reader.writeRows(access, newBook, { sheetId: TEC_SHEET_IDS.tickets, startRow: 2, rows: planned.tickets, rowCount: 2000 });
  return { referrals: planned.referrals.length, tickets: planned.tickets.length };
}
