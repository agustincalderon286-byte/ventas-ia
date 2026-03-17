import express from "express";
import cors from "cors";
import fs from "fs";
import path from "path";

const app = express();

app.use(cors());
app.use(express.json());

// MEMORIA DE CONVERSACIONES
const conversaciones = {};

// =============================
// FUNCION PARA CARGAR JSON SEGURO
// =============================
function cargarJSON(ruta) {
  try {
    return JSON.parse(fs.readFileSync(ruta, "utf8"));
  } catch (error) {
    console.log("Error cargando:", ruta);
    return {};
  }
}

// =============================
// CARGAR BASES DE DATOS
// =============================
const preciosCatalogo = cargarJSON("./src/data/lista_de_precios.json");
const beneficiosProductos = cargarJSON("./src/data/Caracteristicas_Ventajas_Beneficios");
const encuestaVentas = cargarJSON("./src/data/Encuesta_intelijente");
const inteligenciaVentas = cargarJSON("./src/data/Eric_Material_viejo");
const recetasRoyalPrestige = cargarJSON("./src/data/recetas_royal_prestige");
const especificacionesRoyalPrestige = cargarJSON("./src/data/especificasiones_royal_prestige");

// =============================
// CARGAR REDFIN DATA
// =============================
const redfinDataPath = path.join(process.cwd(), "redfin_23.json");
let redfinProperties = [];

try {
  const rawData = fs.readFileSync(redfinDataPath, "utf-8");
  redfinProperties = JSON.parse(rawData);
  console.log(`Loaded ${redfinProperties.length} properties from redfin_23`);
} catch (error) {
  console.error("Error loading redfin_23.json:", error.message);
}

// =============================
// ANALISIS DE PROPIEDADES
// =============================
function analyzeProperty(property) {
  const price = property.price || 0;
  const rent = property.rent_estimate || 0;
  const rentRatio = rent / price; // regla simple mensual / precio
  const yearlyRent = rent * 12;
  const cashFlowEstimate = yearlyRent - (price * 0.1); // estimación simple de gastos

  let score = 0;
  if (rentRatio >= 0.01) score += 2;
  else if (rentRatio >= 0.008) score += 1;
  if (cashFlowEstimate > 0) score += 2;

  return {
    ...property,
    rentRatio,
    yearlyRent,
    cashFlowEstimate,
    score
  };
}

// =============================
// ENDPOINT CHAT EXISTENTE
// =============================
app.post("/chat", async (req, res) => {
  const { pregunta, sessionId } = req.body;

  if (!sessionId) {
    return res.status(400).json({ error: "sessionId requerido" });
  }

  if (!conversaciones[sessionId]) conversaciones[sessionId] = [];

  conversaciones[sessionId].push({ role: "user", content: pregunta });

  try {
    const response = await fetch("https://api.openai.com/v1/chat/completions", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Authorization: `Bearer ${process.env.OPENAI_API_KEY}`
      },
      body: JSON.stringify({
        model: "gpt-5-mini",
        messages: [
          {
            role: "system",
            content: `Eres Agustin 2.0, asistente experto en cocina, ventas y análisis de inversión inmobiliaria.

OBJETIVO:
Ayuda a cocinar, responder preguntas de productos, vender, y analizar propiedades de inversión.

USO DE REDFIN DATA:
Tienes acceso al archivo 'redfin_23' con 23 propiedades.
- Analiza rentabilidad: rent-to-price ratio y cash flow
- Detecta patrones de propiedades buenas
- Recomienda oportunidades de inversión de forma clara y simple

--------------------------------------------------
DATOS DISPONIBLES:

CATALOGO:
${JSON.stringify(preciosCatalogo)}

CARACTERISTICAS:
${JSON.stringify(beneficiosProductos)}

ENCUESTA:
${JSON.stringify(encuestaVentas)}

EXPERIENCIA VENTAS:
${JSON.stringify(inteligenciaVentas)}

RECETAS:
${JSON.stringify(recetasRoyalPrestige)}

ESPECIFICACIONES:
${JSON.stringify(especificacionesRoyalPrestige)}

REDFIN PROPERTIES:
${JSON.stringify(redfinProperties)}

`
          },
          ...conversaciones[sessionId]
        ]
      })
    });

    const data = await response.json();
    const respuestaIA = data.choices[0].message;

    conversaciones[sessionId].push(respuestaIA);

    res.json({ respuesta: respuestaIA.content });
  } catch (error) {
    console.error(error);
    res.status(500).json({ error: "Error al procesar la solicitud" });
  }
});

// =============================
// ENDPOINT ANALISIS DE PROPIEDADES
// =============================
app.post("/analyze-properties", async (req, res) => {
  try {
    const analyzed = redfinProperties.map(analyzeProperty);
    const topDeals = analyzed.sort((a, b) => b.score - a.score).slice(0, 5);

    const prompt = `
You are an expert real estate investor.
Analyze the following properties for good investment opportunities. Focus on cash flow, rent-to-price ratio, size, and value. Explain your recommendations simply.

Properties:
${JSON.stringify(topDeals, null, 2)}
`;

    const aiResponse = await fetch("https://api.openai.com/v1/chat/completions", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Authorization: `Bearer ${process.env.OPENAI_API_KEY}`
      },
      body: JSON.stringify({
        model: "gpt-5-mini",
        messages: [
          { role: "system", content: "You are an expert real estate investor." },
          { role: "user", content: prompt }
        ]
      })
    });

    const data = await aiResponse.json();

    res.json({
      success: true,
      topDeals,
      insights: data.choices[0].message.content
    });
  } catch (error) {
    console.error(error);
    res.status(500).json({ error: "Analysis failed" });
  }
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log(`Servidor corriendo en puerto ${PORT}`));
