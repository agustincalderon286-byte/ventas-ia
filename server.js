import express from "express";
import cors from "cors";
import fs from "fs";
import path from "path";

const app = express();
app.use(cors());
app.use(express.json());

const conversaciones = {};

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
Eres Agustin 2.0, experto en ventas, cocina y analisis de inversiones.

OBJETIVO:
Ayudar a cerrar ventas, cocinar mejor y detectar oportunidades de negocio.

MODOS:
CLIENTE → cocina, beneficios
DISTRIBUIDOR → ventas, objeciones, cierres
INVERSIONISTA → propiedades, ROI

REGLAS:
- Maximo 3 oraciones
- Claro, directo, vendedor experto
- Cita frases exactas cuando esten disponibles
- Cuando agas un cierre solo dar dos opciones

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
0.8–1% bueno
<0.8% debil

IMPORTANTE:
Tienes acceso a multiples bases de conocimiento.
Usa SOLO la informacion necesaria segun la pregunta.
No repitas informacion innecesaria.
`;

// =============================
// FUNCION INTELIGENTE (FILTRA DATA)
// =============================
function construirContexto(pregunta) {

  let contexto = `
CATALOGO:
${JSON.stringify(preciosCatalogo)}

CARACTERISTICAS:
${JSON.stringify(beneficiosProductos)}

ENCUESTA:
${JSON.stringify(encuestaVentas)}
`;

  // 🔥 SOLO AGREGA SI SE NECESITA

  if (pregunta.toLowerCase().includes("receta")) {
    contexto += `\nRECETAS:\n${JSON.stringify(recetasRoyalPrestige)}`;
  }

  if (pregunta.toLowerCase().includes("garantia") || pregunta.toLowerCase().includes("material")) {
    contexto += `\nESPECIFICACIONES:\n${JSON.stringify(especificacionesRoyalPrestige)}`;
  }

  if (pregunta.toLowerCase().includes("venta") || pregunta.toLowerCase().includes("cerrar")) {
    contexto += `\nVENTAS:\n${JSON.stringify(inteligenciaVentas)}`;
    contexto += `\nDEMO:\n${JSON.stringify(demoVenta)}`;
    contexto += `\nCIERRES:\n${JSON.stringify(cierresAlexDey)}`;
  }

  if (pregunta.toLowerCase().includes("equipo") || pregunta.toLowerCase().includes("reclutar")) {
    contexto += `\nRECLUTAMIENTO:\n${JSON.stringify(reclutamientoCiprian)}`;
  }

  if (pregunta.toLowerCase().includes("casa") || pregunta.toLowerCase().includes("inversion")) {
    contexto += `\nPROPIEDADES:\n${JSON.stringify(redfinProperties)}`;
  }

  return contexto;
}

// =============================
// CHAT
// =============================
app.post("/chat", async (req, res) => {

  const { pregunta, sessionId } = req.body;

  if (!sessionId) {
    return res.status(400).json({ error: "sessionId requerido" });
  }

  if (!conversaciones[sessionId]) {
    conversaciones[sessionId] = [];
  }

  conversaciones[sessionId].push({
    role: "user",
    content: pregunta
  });

  try {

    const contexto = construirContexto(pregunta);

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
          ...conversaciones[sessionId]
        ]
      })
    });

    const data = await response.json();

    if (!data.choices) {
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

app.listen(PORT, () => {
  console.log(`Servidor corriendo en puerto ${PORT}`);
});
