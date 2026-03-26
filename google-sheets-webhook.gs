const SHEET_NAME = "Leads";
const HEADERS = [
  "nombre",
  "telefono",
  "mejor_dia_para_llamar",
  "mejor_hora_para_llamar",
  "notas"
];

function doPost(e) {
  try {
    const payload = JSON.parse(e.postData.contents || "{}");
    const sheet = getSheet_();
    const rowData = buildRow_(payload);
    const rowIndex = findExistingRow_(
      sheet,
      payload.telefono || payload.phone,
      payload.nombre || payload.name
    );

    if (rowIndex > 0) {
      sheet.getRange(rowIndex, 1, 1, HEADERS.length).setValues([rowData]);
    } else {
      sheet.appendRow(rowData);
    }

    return jsonResponse_({ ok: true });
  } catch (error) {
    return jsonResponse_({ ok: false, error: error.message });
  }
}

function doGet() {
  return jsonResponse_({ ok: true, service: "google-sheets-webhook" });
}

function getSheet_() {
  const sheet = SpreadsheetApp.getActiveSpreadsheet().getSheetByName(SHEET_NAME)
    || SpreadsheetApp.getActiveSpreadsheet().insertSheet(SHEET_NAME);

  if (sheet.getLastRow() === 0) {
    sheet.appendRow(HEADERS);
  }

  return sheet;
}

function buildRow_(payload) {
  const notasCompatibles = Array.isArray(payload.notes)
    ? payload.notes
        .map(note => `${formatDate_(note.createdAt)} - ${note.text || ""}`)
        .join("\n\n")
    : payload.notas || payload.notes || payload.profileSummary || payload.message || "";

  return [
    payload.nombre || payload.name || "",
    payload.telefono || payload.phone || "",
    payload.mejor_dia_para_llamar || payload.bestCallDay || "",
    payload.mejor_hora_para_llamar || payload.bestCallTime || "",
    notasCompatibles
  ];
}

function findExistingRow_(sheet, phone, name) {
  if (!phone && !name) {
    return -1;
  }

  const values = sheet.getDataRange().getValues();
  const phoneIndex = HEADERS.indexOf("telefono");
  const nameIndex = HEADERS.indexOf("nombre");

  for (let i = 1; i < values.length; i += 1) {
    const row = values[i];

    if (phone && row[phoneIndex] === phone) {
      return i + 1;
    }

    if (name && row[nameIndex] === name) {
      return i + 1;
    }
  }

  return -1;
}

function formatDate_(value) {
  if (!value) {
    return "";
  }

  return new Date(value).toISOString();
}

function jsonResponse_(payload) {
  return ContentService
    .createTextOutput(JSON.stringify(payload))
    .setMimeType(ContentService.MimeType.JSON);
}
