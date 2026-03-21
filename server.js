import "dotenv/config";
import express from "express";
import cors from "cors";
import fs from "fs";
import mongoose from "mongoose";
import path from "path";

const app = express();
app.use(cors());
app.use(express.json());
app.use(express.static(path.join(process.cwd(), "public")));

const conversaciones = {};
const estadosConversacion = {};

const leadSchema = new mongoose.Schema({
  name: String,
  email: String,
  phone: String,
  message: String,
  tieneProductos: String,
  necesitaGarantia: String,
  quiereLlamada: String,
  notes: [
    {
      text: String,
      createdAt: { type: Date, default: Date.now }
    }
  ],
  productos: [String],
  cocinaPara: String,
  esCliente: String,
  direccion: String,
  updatedAt: Date,
  createdAt: { type: Date, default: Date.now }
});

const Lead = mongoose.models.Lead || mongoose.model("Lead", leadSchema);

// =============================
// FUNCION JSON SEGURA
// =============================
function cargarJSON(ruta) {
  try {
    return JSON.parse(fs.readFileSync(ruta, "utf8"));
  } catch (error) {
    console.log("Error cargando:", ruta);
    return null;
  }
}

function extraerLeadInfo(texto) {
  if (!texto) {
    return null;
  }

  const emailMatch = texto.match(/[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}/i);
  const phoneMatch = texto.match(/(?:\+?\d[\d()\-\s]{7,}\d)/);
  const email = emailMatch ? emailMatch[0].toLowerCase() : "";
  const phone = phoneMatch ? phoneMatch[0].replace(/[^\d+]/g, "") : "";

  if (!email && !phone) {
    return null;
  }

  return {
    name: "",
    email,
    phone,
    message: texto
  };
}

function extraerNombre(texto) {
  const nombreMatch = texto.match(
    /(?:mi nombre es|soy)\s+([a-záéíóúñ][a-záéíóúñ\s]{1,60})/i
  );

  if (!nombreMatch) {
    return "";
  }

  return nombreMatch[1]
    .split(/\s+(?:y\s+mi|mi\s+n[uú]mero|mi\s+telefono|mi\s+tel[eé]fono|mi\s+correo|mi\s+email|quiero|para)\b/i)[0]
    .trim()
    .replace(/[.,;!?]+$/, "");
}

function extraerProductos(texto) {
  const productosDetectados = [];
  const catalogoProductos = [
    { nombre: "extractor", regex: /\bextractor\b/i },
    { nombre: "olla de presion", regex: /\bolla(?:\s+de)?\s+presi[oó]n\b/i },
    { nombre: "ollas", regex: /\bollas\b/i },
    { nombre: "sarten", regex: /\bsart(?:e|é)n(?:es)?\b/i },
    { nombre: "easy release", regex: /\beasy\s+release\b/i },
    { nombre: "cacerola", regex: /\bcacerolas?\b/i },
    { nombre: "bateria de cocina", regex: /\bbater[ií]a\s+de\s+cocina\b/i },
    { nombre: "vaporeras", regex: /\bvaporeras?\b/i },
    { nombre: "comal", regex: /\bcomal(?:es)?\b/i },
    { nombre: "wok", regex: /\bwok\b/i },
    { nombre: "cuchillo santoku", regex: /\bsantoku\b/i },
    { nombre: "cuchillo", regex: /\bcuchill(?:o|os)\b/i },
    { nombre: "royal prestige", regex: /\broyal\s+prestige\b/i }
  ];

  for (const producto of catalogoProductos) {
    if (producto.regex.test(texto)) {
      productosDetectados.push(producto.nombre);
    }
  }

  return [...new Set(productosDetectados)];
}

function extraerCocinaPara(texto) {
  const match = texto.match(
    /(?:cocino\s+para|somos|para)\s+(\d{1,2})\s+personas?/i
  );

  if (!match) {
    return "";
  }

  return `${match[1]} personas`;
}

function extraerEstadoCliente(texto) {
  if (/(?:no\s+soy\s+client[ea]|a[uú]n\s+no\s+soy\s+client[ea]|todav[ií]a\s+no\s+soy\s+client[ea])/i.test(texto)) {
    return "no";
  }

  if (/(?:ya\s+soy\s+client[ea]|soy\s+client[ea]|ya\s+compr[eé]|ya\s+tengo\s+productos?)/i.test(texto)) {
    return "si";
  }

  return "";
}

function extraerDireccion(texto) {
  const match = texto.match(
    /(?:mi direcci[oó]n es|vivo en|estoy en|me ubico en)\s+([^.\n]+)/i
  );

  if (!match) {
    return "";
  }

  return match[1]
    .split(/\s+y\s+(?:quiero|me interesa|necesito|ocupo|solo|tambien|también)\b/i)[0]
    .trim()
    .replace(/[.,;!?]+$/, "");
}

function extraerDetallesLead(texto) {
  return {
    name: extraerNombre(texto),
    productos: extraerProductos(texto),
    cocinaPara: extraerCocinaPara(texto),
    esCliente: extraerEstadoCliente(texto),
    direccion: extraerDireccion(texto)
  };
}

function extraerTieneProductos(texto, productosDetectados = []) {
  if (/(?:no\s+tengo\s+productos?|no\s+tengo\s+royal\s+prestige|todav[ií]a\s+no\s+tengo\s+productos?|a[uú]n\s+no\s+tengo\s+productos?)/i.test(texto)) {
    return "no";
  }

  if (
    /(?:tengo\s+productos?|ya\s+tengo\s+productos?|cuento\s+con\s+productos?|soy\s+client[ea]|ya\s+soy\s+client[ea]|ya\s+compr[eé])/i.test(texto) ||
    (productosDetectados.length && /(?:tengo|ya\s+tengo|cuento\s+con)/i.test(texto))
  ) {
    return "si";
  }

  return "";
}

function extraerNecesitaGarantia(texto) {
  if (
    /garant[ií]a/i.test(texto) &&
    /(?:necesito|ocupo|quiero|ayuda|problema|reclamo|cambio|soporte|fall[oó]|no\s+sirve)/i.test(texto)
  ) {
    return "si";
  }

  if (
    /garant[ií]a/i.test(texto) &&
    /(?:todo\s+est[aá]\s+bien|no\s+necesito|no\s+ocupo|sin\s+problema)/i.test(texto)
  ) {
    return "no";
  }

  return "";
}

function detectarConsultaPrecio(texto) {
  return /(?:precio|precios|cu[aá]nto|cu[aá]ntos|cuesta|cost[oó]|pagos?|diario|semanal|mensual|financiamiento|plan(?:es)?\s+de\s+pago)/i.test(
    texto
  );
}

function detectarInteresComercial(texto) {
  return /(?:precio|precios|cuesta|cost[oó]|pago|pagos|interesa|interesado|quiero\s+informaci[oó]n|quiero\s+saber|cat[aá]logo|representante|agendar|cita|llamada|demo|demostraci[oó]n|comprar|promoci[oó]n|oferta)/i.test(
    texto
  );
}

function detectarAceptacionLlamada(texto, estado) {
  const textoLimpio = texto.trim().toLowerCase();

  if (
    /(?:agend|ll[aá]mame|ll[aá]menme|marc[aá]me|marquenme|quiero\s+la\s+llamada|quiero\s+agendar|s[ií],?\s+me\s+interesa\s+la\s+llamada|s[ií],?\s+agendemos)/i.test(
      texto
    )
  ) {
    return true;
  }

  if (
    (estado.interesComercial || estado.consultaPrecio) &&
    /^(si|sí|claro|perfecto|ok|dale|est[aá]\s+bien)$/i.test(textoLimpio)
  ) {
    return true;
  }

  return false;
}

function detectarRechazoLlamada(texto, estado) {
  if (!(estado.interesComercial || estado.consultaPrecio)) {
    return false;
  }

  return /^(no|ahorita no|despu[eé]s|luego|solo\s+era\s+una\s+pregunta)$/i.test(
    texto.trim().toLowerCase()
  );
}

function combinarListas(base = [], nuevas = []) {
  return [...new Set([...base, ...nuevas].filter(Boolean))];
}

function obtenerEstadoConversacion(sessionId) {
  if (!estadosConversacion[sessionId]) {
    estadosConversacion[sessionId] = {
      interesComercial: false,
      consultaPrecio: false,
      llamadaInformativaAceptada: false,
      quiereLlamada: "",
      name: "",
      email: "",
      phone: "",
      direccion: "",
      esCliente: "",
      tieneProductos: "",
      necesitaGarantia: "",
      cocinaPara: "",
      productos: []
    };
  }

  return estadosConversacion[sessionId];
}

function actualizarEstadoConversacion(sessionId, texto, leadGuardado = null) {
  const estado = obtenerEstadoConversacion(sessionId);
  const leadInfo = extraerLeadInfo(texto);
  const detallesLead = extraerDetallesLead(texto);
  const tieneProductos = extraerTieneProductos(texto, detallesLead.productos);
  const necesitaGarantia = extraerNecesitaGarantia(texto);

  if (leadInfo?.email) {
    estado.email = leadInfo.email;
  }

  if (leadInfo?.phone) {
    estado.phone = leadInfo.phone;
  }

  if (detallesLead.name) {
    estado.name = detallesLead.name;
  }

  if (detallesLead.direccion) {
    estado.direccion = detallesLead.direccion;
  }

  if (detallesLead.esCliente) {
    estado.esCliente = detallesLead.esCliente;
  }

  if (detallesLead.cocinaPara) {
    estado.cocinaPara = detallesLead.cocinaPara;
  }

  if (tieneProductos) {
    estado.tieneProductos = tieneProductos;
  }

  if (necesitaGarantia) {
    estado.necesitaGarantia = necesitaGarantia;
  }

  if (detallesLead.productos.length) {
    estado.productos = combinarListas(estado.productos, detallesLead.productos);
  }

  if (detectarConsultaPrecio(texto)) {
    estado.consultaPrecio = true;
    estado.interesComercial = true;
  }

  if (detectarInteresComercial(texto)) {
    estado.interesComercial = true;
  }

  if (detectarAceptacionLlamada(texto, estado)) {
    estado.llamadaInformativaAceptada = true;
    estado.quiereLlamada = "si";
  }

  if (detectarRechazoLlamada(texto, estado)) {
    estado.quiereLlamada = "no";
  }

  if (leadGuardado) {
    estado.name = leadGuardado.name || estado.name;
    estado.email = leadGuardado.email || estado.email;
    estado.phone = leadGuardado.phone || estado.phone;
    estado.direccion = leadGuardado.direccion || estado.direccion;
    estado.esCliente = leadGuardado.esCliente || estado.esCliente;
    estado.cocinaPara = leadGuardado.cocinaPara || estado.cocinaPara;
    estado.tieneProductos = leadGuardado.tieneProductos || estado.tieneProductos;
    estado.necesitaGarantia = leadGuardado.necesitaGarantia || estado.necesitaGarantia;
    estado.quiereLlamada = leadGuardado.quiereLlamada || estado.quiereLlamada;
    estado.productos = combinarListas(estado.productos, leadGuardado.productos || []);
  }

  return estado;
}

function obtenerSiguienteDatoLead(estado) {
  if (!estado.name) {
    return "nombre";
  }

  if (!estado.phone) {
    return "telefono";
  }

  if (!estado.direccion) {
    return "direccion";
  }

  if (!estado.esCliente) {
    return "si ya es cliente";
  }

  if (!estado.tieneProductos) {
    return "si ya tiene productos";
  }

  if (estado.tieneProductos === "si" && !estado.productos.length) {
    return "que productos tiene";
  }

  if (!estado.necesitaGarantia) {
    return "si necesita garantia";
  }

  return "";
}

function construirEstadoPrompt(sessionId) {
  const estado = obtenerEstadoConversacion(sessionId);
  const siguienteDatoLead = obtenerSiguienteDatoLead(estado);
  let fase = "chef-y-guia-de-uso";
  let instruccion = "Enfocate en ayudar con cocina saludable, recetas y uso correcto de productos Royal Prestige.";

  if (estado.interesComercial || estado.consultaPrecio) {
    fase = "interes-comercial";
    instruccion = "Si el usuario muestra interes comercial, invitalo a una llamada informativa sin compromiso con un representante 5 estrellas.";
  }

  if (estado.llamadaInformativaAceptada) {
    fase = "captura-de-lead";
    instruccion = siguienteDatoLead
      ? `La llamada informativa ya fue aceptada. Pide unicamente este dato ahora: ${siguienteDatoLead}.`
      : "Ya tienes los datos clave; confirma que un representante 5 estrellas puede continuar con la llamada informativa.";
  }

  return `
ESTADO ACTUAL:
- fase: ${fase}
- interes_comercial: ${estado.interesComercial ? "si" : "no"}
- consulta_precio: ${estado.consultaPrecio ? "si" : "no"}
- llamada_informativa_aceptada: ${estado.llamadaInformativaAceptada ? "si" : "no"}
- nombre: ${estado.name || "pendiente"}
- email: ${estado.email || "pendiente"}
- telefono: ${estado.phone || "pendiente"}
- direccion: ${estado.direccion || "pendiente"}
- es_cliente: ${estado.esCliente || "pendiente"}
- tiene_productos: ${estado.tieneProductos || "pendiente"}
- productos_mencionados: ${estado.productos.length ? estado.productos.join(", ") : "pendiente"}
- necesita_garantia: ${estado.necesitaGarantia || "pendiente"}
- cocina_para: ${estado.cocinaPara || "pendiente"}
- siguiente_dato_prioritario: ${siguienteDatoLead || "ninguno"}

INSTRUCCION OPERATIVA:
${instruccion}
`;
}

async function sincronizarLeadAGoogleSheets(lead) {
  if (!lead || !process.env.GOOGLE_SHEETS_WEBHOOK_URL) {
    return;
  }

  try {
    const payload = typeof lead.toObject === "function" ? lead.toObject() : lead;
    const response = await fetch(process.env.GOOGLE_SHEETS_WEBHOOK_URL, {
      method: "POST",
      headers: {
        "Content-Type": "application/json"
      },
      body: JSON.stringify(payload)
    });

    if (!response.ok) {
      console.log("Error Google Sheets:", response.status, response.statusText);
    }
  } catch (error) {
    console.log("Error sincronizando Google Sheets:", error.message);
  }
}

async function guardarLeadSiExiste(texto, estadoConversacion = null) {
  const leadInfo = extraerLeadInfo(texto);
  const detallesLead = extraerDetallesLead(texto);
  const email = leadInfo?.email || estadoConversacion?.email || "";
  const phone = leadInfo?.phone || estadoConversacion?.phone || "";

  if (!email && !phone) {
    return null;
  }

  try {
    const tieneProductos =
      extraerTieneProductos(texto, detallesLead.productos) || estadoConversacion?.tieneProductos || "";
    const necesitaGarantia =
      extraerNecesitaGarantia(texto) || estadoConversacion?.necesitaGarantia || "";
    const productosDetectados = combinarListas(
      estadoConversacion?.productos || [],
      detallesLead.productos
    );
    const condiciones = [];
    const camposActualizar = {
      message: texto,
      updatedAt: new Date(),
      quiereLlamada: estadoConversacion?.quiereLlamada || ""
    };
    const actualizacion = {
      $set: camposActualizar,
      $push: {
        notes: {
          text: texto,
          createdAt: new Date()
        }
      },
      $setOnInsert: {
        createdAt: new Date()
      }
    };

    if (email) {
      condiciones.push({ email });
      camposActualizar.email = email;
    }

    if (phone) {
      condiciones.push({ phone });
      camposActualizar.phone = phone;
    }

    if (detallesLead.name || estadoConversacion?.name) {
      camposActualizar.name = detallesLead.name || estadoConversacion?.name || "";
    }

    if (detallesLead.cocinaPara || estadoConversacion?.cocinaPara) {
      camposActualizar.cocinaPara = detallesLead.cocinaPara || estadoConversacion?.cocinaPara || "";
    }

    if (detallesLead.esCliente || estadoConversacion?.esCliente) {
      camposActualizar.esCliente = detallesLead.esCliente || estadoConversacion?.esCliente || "";
    }

    if (detallesLead.direccion || estadoConversacion?.direccion) {
      camposActualizar.direccion = detallesLead.direccion || estadoConversacion?.direccion || "";
    }

    if (tieneProductos) {
      camposActualizar.tieneProductos = tieneProductos;
    }

    if (necesitaGarantia) {
      camposActualizar.necesitaGarantia = necesitaGarantia;
    }

    if (productosDetectados.length) {
      actualizacion.$addToSet = {
        productos: { $each: productosDetectados }
      };
    }

    return await Lead.findOneAndUpdate(
      { $or: condiciones },
      actualizacion,
      {
        new: true,
        upsert: true
      }
    );
  } catch (error) {
    console.log("Error guardando lead MongoDB:", error.message);
    return null;
  }
}

// =============================
// BASES LIGERAS (SIEMPRE ACTIVAS)
// =============================
const preciosCatalogo = cargarJSON("./src/data/lista_de_precios.json");
const beneficiosProductos = cargarJSON("./src/data/Caracteristicas_Ventajas_Beneficios");
const encuestaVentas = cargarJSON("./src/data/Encuesta_intelijente");

// =============================
// BASES PESADAS (SOLO REFERENCIA)
// =============================
const inteligenciaVentas = cargarJSON("./src/data/Eric_Material_viejo");
const recetasRoyalPrestige = cargarJSON("./src/data/recetas_royal_prestige");
const especificacionesRoyalPrestige = cargarJSON("./src/data/especificasiones_royal_prestige");
const demoVenta = cargarJSON("./src/data/Demo_venta_1");

const cierresAlexDey = cargarJSON("./src/data/12_cierres_alex_dey.json");
const mentalidadOlmedo = cargarJSON("./src/data/mentalidad_ventas_olmedo.json");
const reclutamientoCiprian = cargarJSON("./src/data/reclutamiento_ciprian.json");
const sistema4Citas = cargarJSON("./src/data/sistema_4_citas_14_dias.json");

// =============================
// REDFIN
// =============================
const redfinPath = path.join(process.cwd(), "src/data/redfin_23");
let redfinProperties = [];

try {
  const raw = fs.readFileSync(redfinPath, "utf8");
  redfinProperties = JSON.parse(raw);
  console.log(`Loaded ${redfinProperties.length} properties`);
} catch (e) {
  console.log("Error redfin:", e.message);
}

// =============================
// PROMPT OPTIMIZADO
// =============================
const systemPrompt = `
Eres Agustin 2.0, chef inteligente de Royal Prestige, guia de cocina saludable y asistente comercial consultivo.

OBJETIVO PRINCIPAL:
- Ayudar gratis a los clientes a cocinar saludable y usar bien sus productos Royal Prestige.
- Recomendar recetas, tecnicas y el producto exacto ideal para cada paso.
- Cuando detectes interes real por productos o precios, invitar a una llamada informativa con un representante 5 estrellas.
- Despues de que acepten la llamada informativa, capturar el lead paso a paso.

REGLAS:
- Maximo 3 oraciones
- Espanol claro, calido y experto
- Si hablan de recetas o ingredientes, responde primero como chef
- Cuando recomiendes un producto, menciona el nombre del producto y para que sirve en esa receta
- Ejemplo de estilo: "Usa tu cuchillo Santoku de Royal Prestige para cortar la carne; te da cortes uniformes y rapidos."
- Otro ejemplo de estilo: "Para pancakes te recomiendo la sarten Easy Release porque ayuda a cocinar con menos grasa y se despega facil."

PRECIOS:
- Nunca des precio total exacto
- Solo da rangos aproximados por dia usando el catalogo y estas reglas internas:
  tax 10%
  envio 5%
  mensual = precio mas tax mas envio * 5%
  diario = mensual / 30
- Nunca expliques la matematica
- Si preguntan precio, cierra invitando a una llamada informativa sin compromiso con un representante 5 estrellas

VENTAS:
- Primero llamada informativa, despues cita informativa
- Si el usuario acepta la llamada, pide un solo dato a la vez
- Datos vitales: nombre y telefono
- Datos de calificacion: direccion, si ya es cliente, si tiene productos, cuales tiene y si necesita garantia
- Si ya tienes nombre y telefono, sigue con el siguiente dato faltante mas util
- Si aun no aceptan llamada, no pidas toda la ficha completa

COCINA:
- Guias a las personas para cocinar saludable, abrir la puerta de su casa al aprendizaje y usar Royal Prestige con confianza
- Prioriza recetas practicas, saludables y faciles de replicar
- Cuando sea util, conecta la receta con beneficios como menos grasa, mejor control de coccion, practicidad y durabilidad

REAL ESTATE:
rent_ratio = renta / precio
cashflow = (renta * 12) - (precio * 0.1)

>1% excelente
0.8–1% bueno
<0.8% debil

IMPORTANTE:
Tienes acceso a multiples bases de conocimiento.
Usa SOLO la informacion necesaria segun la pregunta.
No repitas informacion innecesaria.
- No inventes productos ni beneficios fuera del contexto disponible.
`;

// =============================
// FUNCION INTELIGENTE (FILTRA DATA)
// =============================
function construirContexto(pregunta) {
  const preguntaNormalizada = pregunta.toLowerCase();
  let contexto = `
CATALOGO:
${JSON.stringify(preciosCatalogo)}

CARACTERISTICAS:
${JSON.stringify(beneficiosProductos)}

ENCUESTA:
${JSON.stringify(encuestaVentas)}
`;

  if (
    /receta|cocinar|pollo|carne|res|pescado|salmon|huevo|pancake|hotcake|panqueque|sopa|arroz|pasta|verdura|ensalada|desayuno|comida|cena/i.test(
      preguntaNormalizada
    )
  ) {
    contexto += `\nRECETAS:\n${JSON.stringify(recetasRoyalPrestige)}`;
  }

  if (
    preguntaNormalizada.includes("garantia") ||
    preguntaNormalizada.includes("material") ||
    /olla|sarten|cuchillo|santoku|easy release|paellera|vaporera|royal prestige/i.test(
      preguntaNormalizada
    )
  ) {
    contexto += `\nESPECIFICACIONES:\n${JSON.stringify(especificacionesRoyalPrestige)}`;
  }

  if (
    preguntaNormalizada.includes("venta") ||
    preguntaNormalizada.includes("cerrar") ||
    detectarInteresComercial(pregunta)
  ) {
    contexto += `\nVENTAS:\n${JSON.stringify(inteligenciaVentas)}`;
    contexto += `\nDEMO:\n${JSON.stringify(demoVenta)}`;
    contexto += `\nCIERRES:\n${JSON.stringify(cierresAlexDey)}`;
  }

  if (preguntaNormalizada.includes("equipo") || preguntaNormalizada.includes("reclutar")) {
    contexto += `\nRECLUTAMIENTO:\n${JSON.stringify(reclutamientoCiprian)}`;
  }

  if (preguntaNormalizada.includes("casa") || preguntaNormalizada.includes("inversion")) {
    contexto += `\nPROPIEDADES:\n${JSON.stringify(redfinProperties)}`;
  }

  return contexto;
}

// =============================
// CHAT
// =============================
app.post("/chat", async (req, res) => {
  const { pregunta, sessionId } = req.body;
  const preguntaLimpia = typeof pregunta === "string" ? pregunta.trim() : "";

  if (!preguntaLimpia) {
    return res.status(400).json({ error: "pregunta requerida" });
  }

  if (!sessionId) {
    return res.status(400).json({ error: "sessionId requerido" });
  }

  if (!conversaciones[sessionId]) {
    conversaciones[sessionId] = [];
  }

  conversaciones[sessionId].push({
    role: "user",
    content: preguntaLimpia
  });

  try {
    actualizarEstadoConversacion(sessionId, preguntaLimpia);

    const leadGuardado = await guardarLeadSiExiste(
      preguntaLimpia,
      obtenerEstadoConversacion(sessionId)
    );
    actualizarEstadoConversacion(sessionId, preguntaLimpia, leadGuardado);
    await sincronizarLeadAGoogleSheets(leadGuardado);

    const contexto = construirContexto(preguntaLimpia);
    const estadoPrompt = construirEstadoPrompt(sessionId);

    const response = await fetch("https://api.openai.com/v1/chat/completions", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Authorization: `Bearer ${process.env.OPENAI_API_KEY}`
      },
      body: JSON.stringify({
        model: "gpt-5-mini",
        messages: [
          { role: "system", content: systemPrompt },
          { role: "system", content: contexto },
          { role: "system", content: estadoPrompt },
          ...conversaciones[sessionId]
        ]
      })
    });

    const data = await response.json().catch(() => null);

    if (!response.ok) {
      return res.status(response.status).json({
        error: "Error OpenAI",
        detalle: data || { message: "Respuesta invalida del API" }
      });
    }

    if (!data?.choices?.[0]?.message) {
      return res.status(500).json({ error: "Error OpenAI", detalle: data });
    }

    const respuestaIA = data.choices[0].message;

    conversaciones[sessionId].push(respuestaIA);

    res.json({ respuesta: respuestaIA.content });
  } catch (error) {
    console.error(error);
    res.status(500).json({ error: "Error servidor" });
  }
});

// =============================
const PORT = process.env.PORT || 3000;

async function iniciarServidor() {
  if (!process.env.MONGODB_URI) {
    console.error("MONGODB_URI no configurada");
    process.exit(1);
  }

  try {
    await mongoose.connect(process.env.MONGODB_URI);
    console.log("MongoDB Atlas conectado");

    app.listen(PORT, () => {
      console.log(`Servidor corriendo en puerto ${PORT}`);
    });
  } catch (error) {
    console.error("Error conectando MongoDB Atlas:", error.message);
    process.exit(1);
  }
}

iniciarServidor();
