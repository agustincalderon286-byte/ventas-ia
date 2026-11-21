const apiRoot = 'https://sheets.googleapis.com/v4/spreadsheets/';
const fail = (status, message, details = {}) => Object.assign(new Error(message), { status, ...details });
const normal = value => String(value || '').normalize('NFD').replace(/\p{M}/gu, '').toLowerCase().replace(/[^a-z0-9]+/g, ' ').trim();
const headerPatterns = {
  fullName:/^(?:nombre(?:s)?(?: y apellido(?:s)?)?|nombre completo|nombre(?:s)? del (?:referido|prospecto|contacto)|prospecto|contacto|full name|name)$/,
  phone:/^(?:telefono(?:s)?(?: (?:del (?:referido|prospecto)|celular|de contacto))?|celular|numero de telefono|phone(?: number)?)$/,
  host:/^(?:anfitrion(?:a)?|nombre del anfitrion|quien refiere|referido por|referente|host(?: name)?)$/,
  hostPhone:/^(?:telefono (?:del )?anfitrion|host phone)$/,
  number:/^(?:nro|numero|no|num)$/,
  city:/^(?:ciudad(?: o zona(?: de residencia)?(?: zip code)?)?|zona de residencia|city)$/,
  address:/^(?:direccion(?: del (?:referido|prospecto))?|domicilio|address)$/,
  relationship:/^(?:relacion(?: con el (?:anfitrion|cliente))?|parentesco|vinculo|relationship)$/,
  seller:/^(?:vendedor(?: que prospecto)?|representante|seller)$/,
  startDate:/^(?:fecha del 4 14|fecha del 4 en 14|fecha del programa)$/,
};

function layout(info, headerRows, kind) {
  for (let n = 0; n < headerRows.length; n++) {
    const cols = {}, labels = headerRows[n].map(c => normal(c?.formattedValue));
    for (const [field, re] of Object.entries(headerPatterns)) {
      const indexes = labels.flatMap((label, i) => re.test(label) ? [i] : []);
      if (indexes.length === 1) cols[field] = indexes[0];
    }
    if (cols.fullName == null || cols.phone == null) continue;
    let grouped = false;
    if (kind === 'referrals' && cols.host == null) {
      // In the Chicago 4-14 layout the host label sits one row above its own column.
      const prior = n > 0 ? headerRows[n-1].map(c => normal(c?.formattedValue)) : [];
      const hostCol = prior.indexOf('referencia');
      if (hostCol !== -1 && prior.includes('referidos') && cols.number != null && !Object.values(cols).includes(hostCol)) {
        cols.host = hostCol;
        grouped = true;
      }
    }
    if (kind === 'referrals' && cols.host == null) continue;
    return { sheetId:info.sheetId, title:info.title, headerRow:n+1, cols, grouped };
  }
  throw fail(422, kind === 'tickets'
    ? 'En la pestaña de rifas se necesitan encabezados de nombre y teléfono. Revisa las pestañas elegidas en Mis conexiones.'
    : 'En la pestaña 4 en 14 se necesitan nombre, teléfono y anfitrión, o los encabezados REFERENCIA y REFERIDOS del formato de Chicago.');
}

function guardBook(book) {
  if (!book || !/^[a-zA-Z0-9_-]{10,150}$/.test(book.spreadsheetId) ||
      ![book.ticketsSheetId,book.referralsSheetId].every(v => Number.isInteger(v) && v >= 0) ||
      book.ticketsSheetId === book.referralsSheetId) throw fail(409, 'Elige un archivo y dos pestañas distintas en Mis conexiones.');
}

async function readJson(res) {
  // Only the selected header, name and phone columns are fetched. Fail closed if a scan cannot finish.
  const maxBytes = 12 * 1024 * 1024;
  if (Number(res.headers?.get('content-length')) > maxBytes) throw fail(422, 'El archivo es demasiado grande para revisar todos los duplicados.');
  if (!res.body?.getReader) return res.json();
  const reader = res.body.getReader(), chunks = [];
  let total = 0;
  while (true) {
    const { done, value } = await reader.read();
    if (done) break;
    total += value.length;
    if (total > maxBytes) {
      await reader.cancel();
      throw fail(422, 'El archivo es demasiado grande para revisar todos los duplicados.');
    }
    chunks.push(Buffer.from(value));
  }
  return JSON.parse(Buffer.concat(chunks).toString('utf8'));
}

export function createSheetsGoogle(fetcher = fetch) {
  async function request(access, book, suffix, body) {
    guardBook(book);
    let res;
    try {
      res = await fetcher(apiRoot + encodeURIComponent(book.spreadsheetId) + suffix, {
        method:body ? 'POST' : 'GET', headers:{ Authorization:'Bearer '+access, ...(body && {'Content-Type':'application/json'}) },
        ...(body && {body:JSON.stringify(body)}), redirect:'error', signal:AbortSignal.timeout(20000)
      });
    } catch { throw fail(503, 'Google Sheets no confirmó la operación. Reintenta esta misma revisión.'); }
    if (res.ok) return readJson(res);
    if (res.status === 401 || res.status === 403) throw fail(403,
      'Google no permitió la operación. Revisa la cuenta, el permiso de edición del archivo y las pestañas protegidas.', { definite:true });
    if (res.status === 404) throw fail(409, 'El archivo ya no está disponible. Elige el archivo en Mis conexiones.', { definite:true });
    if (res.status === 400) throw fail(409, 'La estructura de la hoja cambió. Abre una vista previa nueva.', { definite:true });
    throw fail(503, 'Google Sheets no completó la operación. Reintenta esta misma revisión.', { definite:res.status === 429 });
  }
  const grid = (access, book, ranges) => request(access, book,
    ':getByDataFilter?fields=sheets(properties(sheetId),data(startRow,startColumn,rowData(values(formattedValue,effectiveValue(numberValue),note))))',
    {dataFilters:ranges.map(gridRange => ({gridRange})),includeGridData:true});
  return {
    async read(access, book) {
      const meta = await request(access, book, '?fields=properties(title),sheets(properties(sheetId,title,sheetType,gridProperties(rowCount,columnCount)))');
      const sheets = ['tickets','referrals'].map(kind => {
        const id = kind === 'tickets' ? book.ticketsSheetId : book.referralsSheetId;
        const info = meta.sheets?.find(s => (s.properties.sheetId ?? 0) === id)?.properties;
        if (!info || info.sheetType !== 'GRID' || !info.gridProperties?.columnCount || !info.gridProperties?.rowCount) {
          throw fail(409, 'Una pestaña elegida ya no está disponible. Revisa Mis conexiones.');
        }
        return { ...info, sheetId:id, kind };
      });
      const headers = await grid(access, book, sheets.map(s => ({ sheetId:s.sheetId, startRowIndex:0,
        endRowIndex:Math.min(s.gridProperties.rowCount, 30), startColumnIndex:0, endColumnIndex:Math.min(s.gridProperties.columnCount, 48) })));
      const state = { title:meta.properties.title, contacts:[], notes:new Set() };
      for (const info of sheets) {
        const page = headers.sheets?.find(s => (s.properties.sheetId ?? 0) === info.sheetId);
        const rows = page?.data?.[0]?.rowData?.map(r => r.values || []) || [];
        state[info.kind] = layout(info, rows, info.kind);
      }
      const ranges = [];
      for (const info of sheets) {
        const dest = state[info.kind];
        if (dest.headerRow >= info.gridProperties.rowCount) continue;
        for (const col of [dest.cols.fullName, dest.cols.phone]) ranges.push({
          sheetId:info.sheetId, startRowIndex:dest.headerRow, startColumnIndex:col, endColumnIndex:col+1
        });
      }
      if (!ranges.length) return state;
      const entries = await grid(access, book, ranges);
      for (const info of sheets) {
        const dest = state[info.kind];
        if (dest.headerRow >= info.gridProperties.rowCount) continue;
        const page = entries.sheets?.find(s => (s.properties.sheetId ?? 0) === info.sheetId);
        const people = new Map();
        for (const [field, col] of [['fullName',dest.cols.fullName],['phone',dest.cols.phone]]) {
          const values = page?.data?.find(d => (d.startColumn ?? 0) === col && (d.startRow ?? 0) === dest.headerRow);
          if (!values) throw fail(503, 'Google no devolvió ambas columnas para revisar los duplicados.');
          for (const [i,row] of (values.rowData || []).entries()) {
            const cell = row.values?.[0];
            if (field === 'fullName' && cell?.note?.startsWith('TEC:')) state.notes.add(cell.note);
            if (!cell?.formattedValue) continue;
            const rowNumber = dest.headerRow+i+1;
            if (!people.has(rowNumber)) people.set(rowNumber, { sheetId:dest.sheetId, sheetTitle:dest.title, row:rowNumber, fullName:'', phone:'' });
            const numeric = cell.effectiveValue?.numberValue;
            people.get(rowNumber)[field] = field === 'phone' && Number.isSafeInteger(numeric) && /[eE]\+?\d+$/.test(cell.formattedValue)
              ? String(numeric) : String(cell.formattedValue);
          }
        }
        for (const person of people.values()) state.contacts.push(person);
      }
      return state;
    },
    async append(access, book, dest, input, selected, marker) {
      const rows = selected.map((index, order) => {
        const person = input.referrals[index], cols = dest.cols, cells = [];
        const set = (col, value) => {
          if (col != null && value) cells[col] = {userEnteredValue:{stringValue:value}};
        };
        set(cols.fullName,person.fullName); set(cols.phone,person.phone); set(cols.city,person.city); set(cols.address,person.address);
        if (input.kind === 'referrals') {
          if (!dest.grouped || order === 0) {
            set(cols.host,input.hostName + (cols.hostPhone == null && input.hostPhone ? '\n'+input.hostPhone : ''));
            set(cols.hostPhone,input.hostPhone);
          }
          if (cols.number != null) cells[cols.number] = {userEnteredValue:{numberValue:order+1}};
          set(cols.relationship,person.relationship); set(cols.startDate,input.startDate);
        }
        set(cols.seller,input.sellerName);
        if (order === 0) cells[cols.fullName].note = marker;
        // Explicit strings prevent a handwritten name or address beginning with '=' from becoming a formula.
        return {values:Array.from(cells, c => c || {})};
      });
      await request(access, book, ':batchUpdate', {requests:[{appendCells:{sheetId:dest.sheetId,
        rows,fields:'userEnteredValue,note'}}]});
    }
  };
}
