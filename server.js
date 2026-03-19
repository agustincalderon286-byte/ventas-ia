import express from "express";
import cors from "cors";
import fs from "fs";
import path from "path";

const app = express();

app.use(cors());
app.use(express.json());

// MEMORIA
const conversaciones = {};

// =============================
// FUNCION JSON
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
// BASES DE DATOS
// =============================
const preciosCatalogo = cargarJSON("./src/data/lista_de_precios.json");
const beneficiosProductos = cargarJSON("./src/data/Caracteristicas_Ventajas_Beneficios");
const encuestaVentas = cargarJSON("./src/data/Encuesta_intelijente");
const inteligenciaVentas = cargarJSON("./src/data/Eric_Material_viejo");
const recetasRoyalPrestige = cargarJSON("./src/data/recetas_royal_prestige");
const especificacionesRoyalPrestige = cargarJSON("./src/data/especificasiones_royal_prestige");

// =============================
// DEMO REAL (NUEVO)
// =============================
const demoVenta = cargarJSON("./src/data/Demo_venta_1");

// =============================
// REDFIN DATA
// =============================
const redfinDataPath = path.join(process.cwd(), "src/data/redfin_23");
let redfinProperties = [];

try {
  const rawData = fs.readFileSync(redfinDataPath, "utf-8");
  redfinProperties = JSON.parse(rawData);
  console.log(`Loaded ${redfinProperties.length} properties`);
} catch (error) {
  console.error("Error loading redfin:", error.message);
}

// =============================
// PROMPT SEPARADO (MEJORADO)
// =============================
const systemPrompt = `
Eres Agustin 2.0, asistente experto en cocina, ventas, cierres y análisis de inversiones.

OBJETIVO
Ayudar a cocinar, vender productos, cerrar ventas y analizar propiedades.

--------------------------------------------------
MODO INTELIGENTE

Detecta el tipo de usuario:

CLIENTE
- cocina, recetas, beneficios, salud

DISTRIBUIDOR
- ventas, cierres, objeciones, demostraciones

INVERSIONISTA
- análisis de propiedades, ROI, oportunidades

--------------------------------------------------
COCINA

- guía paso a paso
- técnicas simples
- beneficios: mejor sabor, menos aceite, salud

--------------------------------------------------
PRECIOS

Tax = 10%
Envio = 5%

Pago mensual = precio final * 0.05
Pago semanal = mensual / 4
Pago diario = mensual / 30

Mostrar:
codigo, nombre, precio, tax, envio, mensual, semanal, diario

--------------------------------------------------
VENTAS (CLAVE)

Usa:
- beneficios
- emoción
- lógica
- urgencia suave

Maneja objeciones:
- precio → dividir pagos
- pensarlo → simplificar decisión
- pareja → decisión familiar
- tiempo → rapidez

SIEMPRE guía hacia el cierre.

--------------------------------------------------
USO DE DEMO REAL (MUY IMPORTANTE)

Tienes acceso a una demostración real de ventas (Demo_venta_1).

Debes usarla como referencia para:

- entender el flujo de una demostración
- cómo presentar productos
- cómo responder objeciones
- cómo cerrar ventas

PERO:

- NO uses precios de la demo (son de otro país)
- NO uses medidas exactas si no coinciden
- USA el estilo, estructura y psicología de venta

Ejemplo:
- cómo hacen preguntas
- cómo conectan con el cliente
- cómo llevan al cierre

--------------------------------------------------
REAL ESTATE

rent_ratio = renta / precio
cashflow = (renta * 12) - (precio * 0.1)

>1% excelente
0.8–1% bueno
<0.8% débil

--------------------------------------------------
ESTILO

- máximo 3 oraciones
- claro
- directo
- tono experto en ventas

--------------------------------------------------
OBJETIVO FINAL

- ayudar
- generar valor
- cerrar ventas
- detectar oportunidades
`;

// =============================
// RUTA CHAT
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
            content: systemPrompt
          },
          {
            role: "system",
            content: `
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

DEMO REAL:
${JSON.stringify(demoVenta)}

PROPIEDADES:
${JSON.stringify(redfinProperties)}
`
          },
          ...conversaciones[sessionId]
        ]
      })
    });

    const data = await response.json();

    if (!data.choices) {
      return res.status(500).json({
        error: "Error en respuesta de OpenAI",
        detalle: data
      });
    }

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

// =============================
const PORT = process.env.PORT || 3000;

app.listen(PORT, () =>
  console.log(`Servidor corriendo en puerto ${PORT}`)
);
