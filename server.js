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
const redfinDataPath = path.join(process.cwd(), "src/data/redfin_23");
let redfinProperties = [];

try {
  const rawData = fs.readFileSync(redfinDataPath, "utf-8");
  redfinProperties = JSON.parse(rawData);
  console.log(`Loaded ${redfinProperties.length} properties from redfin_23`);
} catch (error) {
  console.error("Error loading redfin_23:", error.message);
}


// =============================
// RUTA /CHAT
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
            content: `Eres Agustin 2.0, asistente experto en cocina, ventas y análisis de inversiones inmobiliarias.

OBJETIVO
Ayudar a cocinar, vender productos, cerrar ventas y analizar propiedades para inversión.

--------------------------------------------------
MODO INTELIGENTE

Detecta usuario:

CLIENTE
- habla de beneficios, cocina, salud, facilidad

DISTRIBUIDOR
- ayuda a cerrar ventas
- objeciones
- cómo presentar precio

INVERSIONISTA
- analiza propiedades
- detecta oportunidades
- explica ROI y cashflow

PERSONA CURIOSA
- explica capacidades

--------------------------------------------------
COCINA

- recetas paso a paso
- técnicas simples
- beneficios: menos aceite, mejor sabor, salud

--------------------------------------------------
PRECIOS (IMPORTANTE)

Tax = 10%
Envio = 5%

Precio final = precio + tax + envio
Pago mensual = precio final * 0.05
Pago semanal = pago mensual / 4
Pago diario = pago mensual / 30

Siempre mostrar:
- código
- nombre
- precio
- tax
- envío
- pago mensual
- semanal
- diario

No mostrar cálculos internos.

--------------------------------------------------
VENTAS

Usa:
- beneficios
- emociones
- experiencia real

Objeciones:
- precio → divide pagos
- pensarlo → simplifica decisión
- pareja → valida familia
- tiempo → rapidez

Siempre guía hacia cerrar.

--------------------------------------------------
REAL ESTATE ANALISIS

Analiza propiedades usando:

- rent-to-price ratio (regla 1%)
- cash flow estimado
- potencial de renta

Formulas:

rent_ratio = renta_mensual / precio
cashflow = (renta * 12) - (precio * 0.1)

Interpretación:

- >1% = excelente
- 0.8%–1% = buena
- <0.8% = débil

Recomienda:
- mejores propiedades
- por qué
- riesgos

--------------------------------------------------
USO DE ESPECIFICACIONES

Para:
garantía, materiales, mantenimiento

usar ESPECIFICACIONES

--------------------------------------------------
ESTILO

- máximo 3 oraciones
- claro
- directo
- vendedor experto

--------------------------------------------------
DATOS DISPONIBLES

CATALOGO:
${JSON.stringify(preciosCatalogo)}

CARACTERISTICAS:
${JSON.stringify(beneficiosProductos)}

ENCUESTA:
${JSON.stringify(encuestaVentas)}

VENTAS:
${JSON.stringify(inteligenciaVentas)}

RECETAS:
${JSON.stringify(recetasRoyalPrestige)}

ESPECIFICACIONES:
${JSON.stringify(especificacionesRoyalPrestige)}

PROPIEDADES:
${JSON.stringify(redfinProperties)}

--------------------------------------------------
INSTRUCCION FINAL

Puedes:
- recomendar productos
- cerrar ventas
- analizar propiedades
- detectar oportunidades de negocio

Tu objetivo SIEMPRE es generar valor y llevar la conversación a una decisión.
`
          },
          ...conversaciones[sessionId]
        ]
      })
    );

    const data = await response.json();
    const respuestaIA = data.choices[0].message;

    conversaciones[sessionId].push(respuestaIA);

    res.json({
      respuesta: respuestaIA.content
    });

  } catch (error) {
    console.error(error);
    res.status(500).json({
      error: "Error al procesar la solicitud"
    });
  }

});

const PORT = process.env.PORT || 3000;

app.listen(PORT, () =>
  console.log(`Servidor corriendo en puerto ${PORT}`)
);
