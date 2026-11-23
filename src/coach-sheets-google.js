const apiRoot = 'https://sheets.googleapis.com/v4/spreadsheets/';
const fail = (status, message, details = {}) => Object.assign(new Error(message), { status, ...details });
const normal = value => String(value || '').normalize('NFD').replace(/\p{M}/gu, '').toLowerCase().replace(/[^a-z0-9]+/g, ' ').trim();
const headerPatterns = {
  fullName:/^(?:nombre(?:s)?(?: y apellido(?:s)?)?|nombre completo|nombre(?:s)? del (?:referido|prospecto|contacto)|prospecto|contacto|full name|name)$/,
  email:/^(?:correo(?: electronico)?(?: del (?:prospecto|referido))?|e mail|email(?: address)?)$/,
  phone:/^(?:telefono(?:s)?(?: (?:del (?:referido|prospecto)|celular|de contacto))?|celular|numero de telefono|phone(?: number)?)$/,
  host:/^(?:anfitrion(?:a)?|nombre del anfitrion|quien refiere|referido por|referente|host(?: name)?)$/,
  hostPhone:/^(?:telefono (?:del )?anfitrion|host phone)$/,
  order:/^(?:pedido|orden|numero de (?:pedido|orden)|order (?:id|number))$/,
  number:/^(?:nro|numero|no|num)$/,
  city:/^(?:ciudad(?: o zona(?: de residencia)?(?: zip code)?)?|zona de residencia|city)$/,
  address:/^(?:direccion(?: del (?:referido|prospecto))?|domicilio|address)$/,
  relationship:/^(?:relacion(?: con el (?:anfitrion|cliente))?|parentesco|vinculo|relationship)$/,
  seller:/^(?:vendedor(?: que prospecto)?|representante|seller)$/,
  startDate:/^(?:fecha del 4 14|fecha del 4 en 14|fecha del programa)$/,
  history:/^(?:comentarios? (?:del? )?telemarketing|historial(?: de (?:llamadas|seguimiento))?|notas? de seguimiento|follow up notes)$/,
  result:/^(?:resultado (?:de (?:la )?)?cita|resultado del seguimiento|appointment result)$/,
  followupDate:/^(?:fecha (?:del? )?(?:proximo )?seguimiento|proxim[ao] (?:seguimiento|llamada)|fecha (?:de )?proxima llamada|(?:next )?follow up date)$/,
  status:/^(?:estado|estatus|status)(?: del? (?:lead|prospecto|seguimiento))?$/,
};

// Exportada para comprobar en las pruebas que la Hoja TEC se lee con el mismo lector.
export function layout(info, headerRows, kind) {
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
  const maxBytes = 12 * 1024 * 1024;
  const tooLarge = () => fail(422, 'La hoja es demasiado grande para completar la revisión.');
  if (Number(res.headers?.get('content-length')) > maxBytes) throw tooLarge();
  if (!res.body?.getReader) return res.json();
  const reader = res.body.getReader(), chunks = [];
  let total = 0;
  while (true) {
    const { done, value } = await reader.read();
    if (done) break;
    total += value.length;
    if (total > maxBytes) {
      await reader.cancel();
      throw tooLarge();
    }
    chunks.push(Buffer.from(value));
  }
  return JSON.parse(Buffer.concat(chunks).toString('utf8'));
}

function followupCell(field, cell) {
  const value = String(cell?.formattedValue ?? '').trim();
  if (field !== 'followupDate') return value;
  // Sheets serials denote local calendar days, independently of the server's timezone.
  const serial = cell?.effectiveValue?.numberValue;
  if (['DATE','DATE_TIME'].includes(cell?.effectiveFormat?.numberFormat?.type) &&
      Number.isFinite(serial) && serial >= 1 && serial < 2958466) {
    return new Date(Date.UTC(1899, 11, 30) + Math.floor(serial) * 86400000).toISOString().slice(0, 10);
  }
  return value;
}

function hostName(value) {
  // A Chicago host cell can contain a second line with the host's telephone.
  const line = value.split(/[\r\n]+/)[0].replace(/(?:\+?1[ .-]?)?\(?\d{3}\)?[ .-]?\d{3}[ .-]?\d{4}/g,'').trim();
  const label = normal(line);
  if (!line || line.length > 100 || !/^[\p{L}][\p{L}\p{M} .,–’'&-]*$/u.test(line) || headerPatterns.host.test(label) ||
      /\b(?:referencia|referidos?|telefono|tel|celular|historial|comentarios?|telemarketing|buzon|contesto|llamadas?|llamar|instrucciones)\b/.test(label)) return '';
  return line;
}

function assignGroupedHost(group, item) {
  const number = /^\d+$/.test(item.number) ? Number(item.number) : 0;
  const label = headerPatterns.host.test(normal(item.host));
  const expectedName = group.nameLabelRow?.row === item.row-1;
  // Footer/history rows end a block. Never carry their preceding host into a new partial list.
  if (label || number > 0 && number <= group.number || !number && !expectedName && (item.host || item.fullName)) {
    group.host = '';
    group.nameLabelRow = null;
    group.number = 0;
  }
  if (label) {
    // The Chicago template places the prospect on the caption row and the host's name directly below.
    group.nameLabelRow = item;
  } else if (number === 1 || group.nameLabelRow?.row === item.row-1) {
    group.host = hostName(item.host);
    if (group.nameLabelRow) group.nameLabelRow.host = group.host;
    group.nameLabelRow = null;
  }
  if (number) group.number = number;
  item.host = number ? group.host : '';
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
    // Motivo de Google para el registro del servidor, sin los valores entre comillas (pueden ser datos de un lead).
    const reason = await res.json().then(body => String(body?.error?.message || '').replace(/(["'“«]).*?(["'”»])/g, '…').slice(0, 300)).catch(() => '');
    if (reason) console.error('[sheets] Google respondió', res.status, reason);
    if (res.status === 401 || res.status === 403) throw fail(403,
      'Google no permitió la operación. Revisa la cuenta, el permiso de edición del archivo y las pestañas protegidas.', { definite:true });
    if (res.status === 404) throw fail(409, 'El archivo ya no está disponible. Elige el archivo en Mis conexiones.', { definite:true });
    if (res.status === 400) throw fail(409, 'La estructura de la hoja cambió. Abre una vista previa nueva.', { definite:true });
    throw fail(503, 'Google Sheets no completó la operación. Reintenta esta misma revisión.', { definite:res.status === 429 });
  }
  const grid = (access, book, ranges, cellFields='formattedValue,effectiveValue(numberValue),note') => request(access, book,
    ':getByDataFilter?fields=sheets(properties(sheetId),data(startRow,startColumn,rowData(values('+cellFields+'))))',
    {dataFilters:ranges.map(gridRange => ({gridRange})),includeGridData:true});
  async function layouts(access, book, kinds = ['tickets','referrals']) {
    const meta = await request(access, book, '?fields=properties(title,locale),sheets(properties(sheetId,title,sheetType,gridProperties(rowCount,columnCount)))');
    const sheets = kinds.map(kind => {
      const id = kind === 'tickets' ? book.ticketsSheetId : book.referralsSheetId;
      const info = meta.sheets?.find(s => (s.properties.sheetId ?? 0) === id)?.properties;
      if (!info || info.sheetType !== 'GRID' || !info.gridProperties?.columnCount || !info.gridProperties?.rowCount) {
        throw fail(409, 'Una pestaña elegida ya no está disponible. Revisa Mis conexiones.');
      }
      return { ...info, sheetId:id, kind };
    });
    const headers = await grid(access, book, sheets.map(s => ({ sheetId:s.sheetId, startRowIndex:0,
      endRowIndex:Math.min(s.gridProperties.rowCount, 30), startColumnIndex:0, endColumnIndex:Math.min(s.gridProperties.columnCount, 48) })), 'formattedValue');
    const state = { title:meta.properties.title };
    for (const info of sheets) {
      const page = headers.sheets?.find(s => (s.properties.sheetId ?? 0) === info.sheetId);
      const rows = page?.data?.[0]?.rowData?.map(r => r.values || []) || [];
      state[info.kind] = layout(info, rows, info.kind);
    }
    return {sheets,state,locale:meta.properties.locale || ''};
  }
  return {
    async read(access, book) {
      const {sheets,state} = await layouts(access, book);
      state.contacts = [];
      state.notes = new Set();
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
    async readFollowups(access, book, { forPlan = false, forRoute = false, referralsOnly = false, withAddress = false } = {}) {
      const {sheets, state, locale} = await layouts(access, book, referralsOnly ? ['referrals'] : undefined);
      const columns = forRoute ? ['fullName','host','number','seller','address','city'] :
        ['fullName','host','number','seller','history','result','followupDate','status'];
      if (forPlan) columns.push('email','phone','order');
      if (withAddress && !forRoute) columns.push('address','city');
      const destinations = sheets.map(info => state[info.kind]);
      for (const dest of destinations) {
        if (!forRoute && !['history','result','followupDate','status'].some(field => dest.cols[field] != null)) {
          throw fail(422, 'No reconocí columnas de seguimiento en «'+dest.title+'». Revisa los encabezados en la hoja.');
        }
      }

      // Google omits trailing empty rows in a values read. A sheet may have a very large
      // allocated grid and empty blocks between referrals, so never stop at a blank page.
      const nameRanges = sheets.flatMap(info => {
        const dest = state[info.kind];
        if (dest.headerRow >= info.gridProperties.rowCount) return [];
        return [{gridRange:{sheetId:dest.sheetId, startRowIndex:dest.headerRow,
          startColumnIndex:dest.cols.fullName, endColumnIndex:dest.cols.fullName+1}}];
      });
      const response = nameRanges.length && await request(access, book, '/values:batchGetByDataFilter?fields=valueRanges(dataFilters,valueRange(range,values))',
        {dataFilters:nameRanges, majorDimension:'ROWS', valueRenderOption:'FORMATTED_VALUE'});
      const rows = [];
      let textSize = 0;
      for (const info of sheets) {
        const dest = state[info.kind];
        if (dest.headerRow >= info.gridProperties.rowCount) continue;
        const valueRange = response.valueRanges?.find(entry => entry.dataFilters?.some(({gridRange:r}) =>
          r && (r.sheetId ?? 0) === dest.sheetId && (r.startColumnIndex ?? 0) === dest.cols.fullName && r.startRowIndex === dest.headerRow))?.valueRange;
        if (!valueRange) throw fail(503, 'Google no devolvió todos los nombres para revisar los seguimientos.');
        const namesCount = valueRange.values?.length ?? 0;
        if (namesCount > 16000) throw fail(422, 'La pestaña «'+dest.title+'» supera las 16000 filas que puedo revisar de una vez.');
        // If the last prospect is on the host caption row, the name may be outside the used range of the prospect column.
        const count = dest.grouped && namesCount ? Math.min(namesCount+1, info.gridProperties.rowCount-dest.headerRow) : namesCount;
        const fields = columns.filter(field => dest.cols[field] != null);
        let group = {host:'',number:0,nameLabelRow:null};
        for (let offset=0; offset<count; offset+=1000) {
          const pageSize = Math.min(1000, count-offset);
          const start = dest.headerRow + offset;
          const ranges = fields.map(field => ({sheetId:dest.sheetId, startRowIndex:start, endRowIndex:start+pageSize,
            startColumnIndex:dest.cols[field], endColumnIndex:dest.cols[field]+1}));
          const cellFields='formattedValue,effectiveValue(numberValue),effectiveFormat(numberFormat(type))';
          const page = await grid(access, book, ranges, forPlan ? cellFields+',userEnteredValue' : cellFields);
          const data = page.sheets?.find(sheet => (sheet.properties.sheetId ?? 0) === dest.sheetId)?.data;
          const blocks = fields.map(field => {
            const block = data?.find(column => (column.startColumn ?? 0) === dest.cols[field] && (column.startRow ?? 0) === start);
            if (!block) throw fail(503, 'Google no devolvió todas las columnas de seguimiento. Vuelve a intentarlo.');
            return [field, block.rowData || []];
          });
          for (let i=0; i<pageSize; i++) {
            const item = {kind:info.kind,sheetTitle:dest.title,sheetId:dest.sheetId,row:start+i+1,
              fullName:'',host:'',seller:'',sellerColumn:dest.cols.seller != null,history:'',result:'',followupDate:'',status:''};
            if (forPlan) item.editable = {};
            for (const [field, cells] of blocks) {
              const cell = cells[i]?.values?.[0];
              item[field] = followupCell(field, cell);
              if (forPlan && (field === 'history' || field === 'followupDate')) {
                // Edits are checked against the exact value displayed at this one cell.
                item.editable[field] = {column:dest.cols[field], displayed:String(cell?.formattedValue ?? ''),
                  formula:typeof cell?.userEnteredValue?.formulaValue === 'string'};
              }
            }
            const label = normal(item.fullName), hostLabel = normal(item.host);
            if (headerPatterns.fullName.test(label) || label === 'referidos' || (hostLabel === 'referencia' && !item.fullName)) {
              group = {host:'',number:0,nameLabelRow:null};
              continue;
            }
            if (dest.grouped) assignGroupedHost(group,item);
            else item.host = hostName(item.host);
            delete item.number;
            if (!item.fullName) continue;
            textSize += item.fullName.length + item.host.length + item.seller.length + item.history.length + item.result.length + item.followupDate.length + item.status.length;
            if (forRoute) textSize += (item.address || '').length + (item.city || '').length;
            if (textSize > 4*1024*1024) throw fail(422, 'El historial es demasiado extenso para completar la revisión.');
            rows.push(item);
          }
        }
      }
      return {rows,locale};
    },
    async replaceFollowupCell(access, book, {sheetId, row, column, before, after}) {
      if (![book.ticketsSheetId,book.referralsSheetId].includes(sheetId) || !Number.isSafeInteger(row) || row < 2 ||
          !Number.isSafeInteger(column) || column < 0 || typeof before !== 'string' || typeof after !== 'string' ||
          after.length > 50000 || !after || after.startsWith('=')) throw fail(422,'Revisa la celda del seguimiento.');
      const result = await request(access, book, ':batchUpdate', {requests:[{findReplace:{
        range:{sheetId,startRowIndex:row-1,endRowIndex:row,startColumnIndex:column,endColumnIndex:column+1},
        find:before ? before.replace(/[.*+?^${}()|[\]\\]/g, '\\$&') : '^$',
        matchCase:true, matchEntireCell:true, includeFormulas:false, searchByRegex:true,
        replacement:after.replace(/[$\\]/g, c=>'\\'+c)
      }}]});
      return result.replies?.[0]?.findReplace?.occurrencesChanged === 1;
    },
    // Pestañas de la Hoja TEC (Agenda y Resumen), por título. null si el archivo no es una Hoja TEC.
    async tecTabs(access, book) {
      const meta = await request(access, book, '?fields=sheets(properties(sheetId,title,sheetType,gridProperties(rowCount,columnCount)))');
      const find = title => meta.sheets?.map(s => s.properties).find(p => p?.title === title && p.sheetType === 'GRID') || null;
      return {agenda:find('Agenda'), summary:find('Resumen')};
    },
    // Valores de un rango en una pestaña. 'raw' entrega fechas como número de serie y horas como fracción del día.
    async rangeValues(access, book, range, raw = false) {
      const data = await request(access, book, '/values/'+encodeURIComponent(range)+'?majorDimension=ROWS&valueRenderOption='+
        (raw ? 'UNFORMATTED_VALUE&dateTimeRenderOption=SERIAL_NUMBER' : 'FORMATTED_VALUE'));
      return Array.isArray(data?.values) ? data.values : [];
    },
    // Escribe celdas con su tipo explícito: un texto que empieza con «=» nunca se vuelve fórmula.
    async writeCells(access, book, {sheetId, row, column, cells}) {
      if (!Number.isSafeInteger(sheetId) || !Number.isSafeInteger(row) || row < 1 || !Number.isSafeInteger(column) || column < 0) throw fail(422,'Revisa la celda.');
      const value = v => v == null || v === '' ? {userEnteredValue:{stringValue:''}} : typeof v === 'number' ? {userEnteredValue:{numberValue:v}} : {userEnteredValue:{stringValue:String(v)}};
      await request(access, book, ':batchUpdate', {requests:[{updateCells:{start:{sheetId,rowIndex:row-1,columnIndex:column},
        fields:'userEnteredValue',rows:[{values:cells.map(value)}]}}]});
    },
    // Copia filas completas a partir de una fila; agrega filas a la pestaña si hace falta.
    async writeRows(access, book, {sheetId, startRow, rows, rowCount}) {
      const value = v => v == null || v === '' ? {} : typeof v === 'number' ? {userEnteredValue:{numberValue:v}} : {userEnteredValue:{stringValue:String(v)}};
      const needed = startRow - 1 + rows.length + 10 - rowCount;
      await request(access, book, ':batchUpdate', {requests:[
        ...(needed > 0 ? [{appendDimension:{sheetId,dimension:'ROWS',length:needed}}] : []),
        {updateCells:{start:{sheetId,rowIndex:startRow-1,columnIndex:0},fields:'userEnteredValue',rows:rows.map(r=>({values:r.map(value)}))}}]});
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
