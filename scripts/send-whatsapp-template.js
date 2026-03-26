import "dotenv/config";
import fs from "fs";
import path from "path";

const ACCOUNT_SID = String(process.env.TWILIO_ACCOUNT_SID || "").trim();
const AUTH_TOKEN = String(process.env.TWILIO_AUTH_TOKEN || "").trim();
const DEFAULT_FROM = String(process.env.TWILIO_WHATSAPP_FROM || "").trim();
const DEFAULT_CONTENT_SID = String(process.env.TWILIO_WHATSAPP_TEMPLATE_SID || "").trim();

function printUsage() {
  console.log(`
Uso:
  npm run whatsapp:send-template -- --csv /ruta/leads.csv --content-sid HX... --from whatsapp:+12603087201 --dry-run

CSV esperado:
  nombre,telefono
  Maria,+17735550101
  Juan,7735550202

Opciones:
  --csv           Ruta del CSV con columnas nombre,telefono
  --content-sid   SID del template WhatsApp aprobado (HX...)
  --from          Numero remitente en formato whatsapp:+1...
  --limit         Limita cuantos registros mandar
  --dry-run       Solo muestra que se mandaria, no envia nada
`);
}

function getArg(flag) {
  const index = process.argv.indexOf(flag);

  if (index === -1) {
    return "";
  }

  return String(process.argv[index + 1] || "").trim();
}

function hasFlag(flag) {
  return process.argv.includes(flag);
}

function normalizePhone(value = "") {
  const digits = String(value || "").replace(/\D+/g, "");

  if (!digits) {
    return "";
  }

  if (digits.length === 11 && digits.startsWith("1")) {
    return `+${digits}`;
  }

  if (digits.length === 10) {
    return `+1${digits}`;
  }

  return digits.startsWith("+") ? digits : `+${digits}`;
}

function parseCsv(content = "") {
  const lines = String(content || "")
    .split(/\r?\n/)
    .map(line => line.trim())
    .filter(Boolean);

  if (lines.length < 2) {
    return [];
  }

  const headers = lines[0].split(",").map(value => value.trim().toLowerCase());
  const nameIndex = headers.indexOf("nombre");
  const phoneIndex = headers.indexOf("telefono");

  if (nameIndex === -1 || phoneIndex === -1) {
    throw new Error("El CSV debe tener headers nombre,telefono");
  }

  return lines.slice(1).map((line, index) => {
    const columns = line.split(",").map(value => value.trim());
    const nombre = columns[nameIndex] || "";
    const telefono = normalizePhone(columns[phoneIndex] || "");

    return {
      rowNumber: index + 2,
      nombre,
      telefono
    };
  });
}

async function sendTemplate({ from, to, contentSid, nombre }) {
  const body = new URLSearchParams({
    From: from,
    To: `whatsapp:${to}`,
    ContentSid: contentSid,
    ContentVariables: JSON.stringify({ 1: nombre })
  });

  const response = await fetch(
    `https://api.twilio.com/2010-04-01/Accounts/${ACCOUNT_SID}/Messages.json`,
    {
      method: "POST",
      headers: {
        Authorization: `Basic ${Buffer.from(`${ACCOUNT_SID}:${AUTH_TOKEN}`).toString("base64")}`,
        "Content-Type": "application/x-www-form-urlencoded"
      },
      body
    }
  );

  const payload = await response.json().catch(() => ({}));

  if (!response.ok) {
    const message = payload?.message || response.statusText || "Error Twilio";
    throw new Error(`${response.status}: ${message}`);
  }

  return payload;
}

async function main() {
  const csvPath = getArg("--csv");
  const contentSid = getArg("--content-sid") || DEFAULT_CONTENT_SID;
  const from = getArg("--from") || DEFAULT_FROM;
  const dryRun = hasFlag("--dry-run");
  const limit = Number(getArg("--limit") || 0);

  if (hasFlag("--help") || !csvPath) {
    printUsage();
    process.exit(csvPath ? 0 : 1);
  }

  if (!contentSid) {
    throw new Error("Falta --content-sid o TWILIO_WHATSAPP_TEMPLATE_SID");
  }

  if (!from) {
    throw new Error("Falta --from o TWILIO_WHATSAPP_FROM");
  }

  if (!dryRun && (!ACCOUNT_SID || !AUTH_TOKEN)) {
    throw new Error("Faltan TWILIO_ACCOUNT_SID y TWILIO_AUTH_TOKEN en .env");
  }

  const absoluteCsvPath = path.resolve(process.cwd(), csvPath);

  if (!fs.existsSync(absoluteCsvPath)) {
    throw new Error(`No existe el CSV: ${absoluteCsvPath}`);
  }

  const content = fs.readFileSync(absoluteCsvPath, "utf8");
  let leads = parseCsv(content).filter(item => item.nombre && item.telefono);

  if (limit > 0) {
    leads = leads.slice(0, limit);
  }

  if (!leads.length) {
    throw new Error("No encontre leads validos en el CSV");
  }

  console.log(`Leads listos: ${leads.length}`);
  console.log(`Template: ${contentSid}`);
  console.log(`From: ${from}`);
  console.log(dryRun ? "Modo: dry-run" : "Modo: envio real");

  for (const lead of leads) {
    if (dryRun) {
      console.log(`[DRY RUN] ${lead.nombre} -> ${lead.telefono}`);
      continue;
    }

    try {
      const result = await sendTemplate({
        from,
        to: lead.telefono,
        contentSid,
        nombre: lead.nombre
      });

      console.log(
        `[OK] ${lead.nombre} -> ${lead.telefono} | status=${result.status} | sid=${result.sid}`
      );
    } catch (error) {
      console.log(`[ERROR] fila ${lead.rowNumber} | ${lead.nombre} -> ${lead.telefono} | ${error.message}`);
    }
  }
}

main().catch(error => {
  console.error(error.message);
  process.exit(1);
});
