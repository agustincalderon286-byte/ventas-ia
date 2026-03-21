const SHEET_NAME = "Leads";
const HEADERS = [
  "leadId",
  "name",
  "email",
  "phone",
  "esCliente",
  "cocinaPara",
  "productos",
  "direccion",
  "latestMessage",
  "notes",
  "createdAt",
  "updatedAt"
];

function doPost(e) {
  try {
    const payload = JSON.parse(e.postData.contents || "{}");
    const sheet = getSheet_();
    const rowData = buildRow_(payload);
    const rowIndex = findExistingRow_(sheet, payload.email, payload.phone);

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
  return [
    payload._id || "",
    payload.name || "",
    payload.email || "",
    payload.phone || "",
    payload.esCliente || "",
    payload.cocinaPara || "",
    Array.isArray(payload.productos) ? payload.productos.join(", ") : "",
    payload.direccion || "",
    payload.message || "",
    Array.isArray(payload.notes)
      ? payload.notes
          .map(note => `${formatDate_(note.createdAt)} - ${note.text || ""}`)
          .join("\n\n")
      : "",
    formatDate_(payload.createdAt),
    formatDate_(payload.updatedAt)
  ];
}

function findExistingRow_(sheet, email, phone) {
  if (!email && !phone) {
    return -1;
  }

  const values = sheet.getDataRange().getValues();
  const emailIndex = HEADERS.indexOf("email");
  const phoneIndex = HEADERS.indexOf("phone");

  for (let i = 1; i < values.length; i += 1) {
    const row = values[i];

    if (email && row[emailIndex] === email) {
      return i + 1;
    }

    if (phone && row[phoneIndex] === phone) {
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
