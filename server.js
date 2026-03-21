import express from "express";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";

const app = express();
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const PORT = process.env.PORT || 3000;
const OPENAI_API_KEY = process.env.OPENAI_API_KEY;
const MODEL = process.env.OPENAI_MODEL || "gpt-4.1-mini";
const PUBLIC_DIR = path.join(__dirname, "public");
const ALLOWED_ORIGINS = (process.env.ALLOWED_ORIGINS || "")
  .split(",")
  .map(origin => origin.trim())
  .filter(Boolean);
const MAX_BODY_BYTES = "50kb";
const MAX_QUESTION_LENGTH = 2_000;
const MAX_HISTORY_MESSAGES = 12;
const SESSION_TTL_MS = 2 * 60 * 60 * 1000;
const RATE_LIMIT_WINDOW_MS = 30 * 60 * 1000;
const parsedRateLimit = Number(process.env.RATE_LIMIT_MAX_REQUESTS);
const RATE_LIMIT_MAX_REQUESTS =
  Number.isFinite(parsedRateLimit) && parsedRateLimit > 0 ? parsedRateLimit : 40;
const REQUEST_TIMEOUT_MS = 25_000;

const conversaciones = new Map();
const rateLimitStore = new Map();

function setSecurityHeaders(req, res, next) {
  res.setHeader("X-Content-Type-Options", "nosniff");
  res.setHeader("X-Frame-Options", "DENY");
  res.setHeader("Referrer-Policy", "strict-origin-when-cross-origin");
  res.setHeader("Permissions-Policy", "camera=(), microphone=(), geolocation=()");
  res.setHeader("Cross-Origin-Resource-Policy", "same-origin");
  res.setHeader(
    "Content-Security-Policy",
    "default-src 'self'; connect-src 'self'; img-src 'self' data:; style-src 'self' 'unsafe-inline'; script-src 'self'; base-uri 'none'; frame-ancestors 'none'; form-action 'self'"
  );
  next();
}

function applyCors(req, res, next) {
  const origin = req.headers.origin;

  if (!origin) {
    next();
    return;
  }

  if (ALLOWED_ORIGINS.length === 0 || !ALLOWED_ORIGINS.includes(origin)) {
    res.status(403).json({ error: "Origin not allowed" });
    return;
  }

  res.setHeader("Access-Control-Allow-Origin", origin);
  res.setHeader("Vary", "Origin");
  res.setHeader("Access-Control-Allow-Methods", "GET,HEAD,POST,OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type");

  if (req.method === "OPTIONS") {
    res.sendStatus(204);
    return;
  }

  next();
}

function getClientIp(req) {
  const forwardedFor = req.headers["x-forwarded-for"];

  if (typeof forwardedFor === "string" && forwardedFor.trim()) {
    return forwardedFor.split(",")[0].trim();
  }

  return req.socket.remoteAddress || "unknown";
}

function rateLimit(req, res, next) {
  const now = Date.now();
  const clientIp = getClientIp(req);
  const entry = rateLimitStore.get(clientIp);

  if (!entry || now > entry.resetAt) {
    rateLimitStore.set(clientIp, { count: 1, resetAt: now + RATE_LIMIT_WINDOW_MS });
    next();
    return;
  }

  entry.count += 1;

  if (entry.count > RATE_LIMIT_MAX_REQUESTS) {
    const retryAfterSeconds = Math.ceil((entry.resetAt - now) / 1000);
    res.setHeader("Retry-After", String(retryAfterSeconds));
    res.status(429).json({
      error: "Too many requests. Please wait a bit and try again."
    });
    return;
  }

  next();
}

function cleanupStores() {
  const now = Date.now();

  for (const [ip, entry] of rateLimitStore.entries()) {
    if (now > entry.resetAt) {
      rateLimitStore.delete(ip);
    }
  }

  for (const [sessionId, entry] of conversaciones.entries()) {
    if (now - entry.updatedAt > SESSION_TTL_MS) {
      conversaciones.delete(sessionId);
    }
  }
}

function resolveDataPath(relativePath) {
  const directPath = path.join(__dirname, relativePath);

  if (fs.existsSync(directPath)) {
    return directPath;
  }

  const jsonPath = `${directPath}.json`;
  if (fs.existsSync(jsonPath)) {
    return jsonPath;
  }

  return directPath;
}

function cargarJSON(relativePath, required = false) {
  const filePath = resolveDataPath(relativePath);

  try {
    return JSON.parse(fs.readFileSync(filePath, "utf8"));
  } catch (error) {
    const message = `Error cargando ${relativePath}: ${error.message}`;

    if (required) {
      throw new Error(message);
    }

    console.warn(message);
    return null;
  }
}

const preciosCatalogo = cargarJSON("src/data/lista_de_precios.json", true);
const beneficiosProductos = cargarJSON("src/data/Caracteristicas_Ventajas_Beneficios", true);
const encuestaVentas = cargarJSON("src/data/Encuesta_intelijente", true);

const inteligenciaVentas = cargarJSON("src/data/Eric_Material_viejo");
const recetasRoyalPrestige = cargarJSON("src/data/recetas_royal_prestige");
const especificacionesRoyalPrestige = cargarJSON("src/data/especificasiones_royal_prestige");
const demoVenta = cargarJSON("src/data/Demo_venta_1");
const cierresAlexDey = cargarJSON("src/data/12_cierres_alex_dey.json");
const mentalidadOlmedo = cargarJSON("src/data/mentalidad_ventas_olmedo.json");
const reclutamientoCiprian = cargarJSON("src/data/reclutamiento_ciprian.json");
const sistema4Citas = cargarJSON("src/data/sistema_4_citas_14_dias.json");
const redfinProperties = cargarJSON("src/data/redfin_23") || [];

const systemPrompt = `
Eres Agustin 2.0, experto en ventas, cocina y analisis de inversiones.

OBJETIVO:
Ayudar a cerrar ventas, cocinar mejor y detectar oportunidades de negocio.

MODOS:
CLIENTE -> cocina, beneficios
DISTRIBUIDOR -> ventas, objeciones, cierres
INVERSIONISTA -> propiedades, ROI

REGLAS:
- Maximo 3 oraciones
- Claro, directo, vendedor experto
- Cita frases exactas cuando esten disponibles
- Cuando hagas un cierre solo da dos opciones

PRECIOS:
Tax 10%
Envio 5%
Mensual = precio mas tax mas envio * 5%
Semanal = mensual/4
Diario = mensual/30

VENTAS:
Usa emocion + logica + urgencia suave

REAL ESTATE:
rent_ratio = renta / precio
cashflow = (renta * 12) - (precio * 0.1)

>1% excelente
0.8-1% bueno
<0.8% debil

IMPORTANTE:
Tienes acceso a multiples bases de conocimiento.
Usa solo la informacion necesaria segun la pregunta.
No repitas informacion innecesaria.
`.trim();

function agregarBloque(contexto, titulo, data) {
  if (!data) {
    return contexto;
  }

  return `${contexto}\n\n${titulo}:\n${JSON.stringify(data)}`;
}

function construirContexto(pregunta) {
  const texto = pregunta.toLowerCase();
  let contexto = "";

  contexto = agregarBloque(contexto, "CATALOGO", preciosCatalogo);
  contexto = agregarBloque(contexto, "CARACTERISTICAS", beneficiosProductos);
  contexto = agregarBloque(contexto, "ENCUESTA", encuestaVentas);

  if (texto.includes("receta")) {
    contexto = agregarBloque(contexto, "RECETAS", recetasRoyalPrestige);
  }

  if (texto.includes("garantia") || texto.includes("material")) {
    contexto = agregarBloque(contexto, "ESPECIFICACIONES", especificacionesRoyalPrestige);
  }

  if (texto.includes("venta") || texto.includes("cerrar")) {
    contexto = agregarBloque(contexto, "VENTAS", inteligenciaVentas);
    contexto = agregarBloque(contexto, "DEMO", demoVenta);
    contexto = agregarBloque(contexto, "CIERRES", cierresAlexDey);
    contexto = agregarBloque(contexto, "MENTALIDAD", mentalidadOlmedo);
    contexto = agregarBloque(contexto, "SISTEMA_4_CITAS", sistema4Citas);
  }

  if (texto.includes("equipo") || texto.includes("reclutar")) {
    contexto = agregarBloque(contexto, "RECLUTAMIENTO", reclutamientoCiprian);
  }

  if (
    texto.includes("casa") ||
    texto.includes("inversion") ||
    texto.includes("propiedad") ||
    texto.includes("roi") ||
    texto.includes("redfin")
  ) {
    contexto = agregarBloque(contexto, "PROPIEDADES", redfinProperties);
  }

  return contexto.trim();
}

function getConversation(sessionId) {
  const existing = conversaciones.get(sessionId);

  if (existing) {
    existing.updatedAt = Date.now();
    return existing.messages;
  }

  conversaciones.set(sessionId, { messages: [], updatedAt: Date.now() });
  return conversaciones.get(sessionId).messages;
}

function touchConversation(sessionId, messages) {
  conversaciones.set(sessionId, {
    messages: messages.slice(-MAX_HISTORY_MESSAGES),
    updatedAt: Date.now()
  });
}

function validateRequestBody(body) {
  const pregunta = typeof body.pregunta === "string" ? body.pregunta.trim() : "";
  const sessionId = typeof body.sessionId === "string" ? body.sessionId.trim() : "";

  if (!pregunta) {
    return { ok: false, error: "pregunta requerida" };
  }

  if (pregunta.length > MAX_QUESTION_LENGTH) {
    return {
      ok: false,
      error: `pregunta demasiado larga. Maximo ${MAX_QUESTION_LENGTH} caracteres`
    };
  }

  if (!sessionId || sessionId.length < 8 || sessionId.length > 100) {
    return { ok: false, error: "sessionId invalido" };
  }

  if (!/^[a-zA-Z0-9_-]+$/.test(sessionId)) {
    return { ok: false, error: "sessionId solo puede contener letras, numeros, guion y guion bajo" };
  }

  return { ok: true, pregunta, sessionId };
}

async function llamarOpenAI(messages) {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), REQUEST_TIMEOUT_MS);

  try {
    const response = await fetch("https://api.openai.com/v1/chat/completions", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Authorization: `Bearer ${OPENAI_API_KEY}`
      },
      body: JSON.stringify({
        model: MODEL,
        messages,
        temperature: 0.7
      }),
      signal: controller.signal
    });

    const data = await response.json();

    if (!response.ok) {
      const message =
        data && data.error && typeof data.error.message === "string"
          ? data.error.message
          : "Error OpenAI";
      const error = new Error(message);
      error.status = response.status;
      throw error;
    }

    if (!data.choices || !data.choices[0] || !data.choices[0].message) {
      const error = new Error("Respuesta invalida de OpenAI");
      error.status = 502;
      throw error;
    }

    return data.choices[0].message;
  } finally {
    clearTimeout(timeout);
  }
}

app.disable("x-powered-by");
app.set("trust proxy", true);
app.use(setSecurityHeaders);
app.use(applyCors);
app.use(express.json({ limit: MAX_BODY_BYTES }));
app.use(express.static(PUBLIC_DIR, { extensions: ["html"] }));

async function handleChatRoute(req, res) {
  if (!OPENAI_API_KEY) {
    res.status(500).json({ error: "OPENAI_API_KEY no configurada en el servidor" });
    return;
  }

  const validation = validateRequestBody(req.body || {});

  if (!validation.ok) {
    res.status(400).json({ error: validation.error });
    return;
  }

  const { pregunta, sessionId } = validation;
  const historial = getConversation(sessionId);
  historial.push({ role: "user", content: pregunta });
  touchConversation(sessionId, historial);

  try {
    const contexto = construirContexto(pregunta);
    const messages = [
      { role: "system", content: systemPrompt },
      { role: "system", content: contexto },
      ...historial.slice(-MAX_HISTORY_MESSAGES)
    ];

    const respuestaIA = await llamarOpenAI(messages);
    historial.push(respuestaIA);
    touchConversation(sessionId, historial);

    res.json({ respuesta: respuestaIA.content });
  } catch (error) {
    console.error("Error en /chat:", error.message);

    if (error.name === "AbortError") {
      res.status(504).json({ error: "La respuesta tardo demasiado. Intenta de nuevo." });
      return;
    }

    const status = Number.isInteger(error.status) ? error.status : 500;
    res.status(status).json({ error: error.message || "Error servidor" });
  }
}

app.post("/chat", rateLimit, handleChatRoute);
app.post("/api/chat", rateLimit, handleChatRoute);

app.use((error, req, res, next) => {
  if (error && error.type === "entity.too.large") {
    res.status(413).json({ error: "Request demasiado grande" });
    return;
  }

  if (error instanceof SyntaxError && "body" in error) {
    res.status(400).json({ error: "JSON invalido" });
    return;
  }

  next(error);
});

app.use((req, res) => {
  res.status(404).json({ error: "Ruta no encontrada" });
});

app.listen(PORT, () => {
  console.log(`Servidor corriendo en puerto ${PORT}`);
});

setInterval(cleanupStores, 5 * 60 * 1000).unref();
