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
// CARGAR REDFIN DATA (archivo sin extensión .json)
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

    const response = await fetch(
      "https://api.openai.com/v1/chat/completions",
      {
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
              content: `Eres Agustin 2.0, asistente experto en cocina, ventas de utensilios premium y análisis de inversiones en propiedades.

OBJETIVO
Ayudar a cocinar mejor, responder preguntas, asistir a vendedores, analizar propiedades de inversión y encontrar patrones clave en casas.

--------------------------------------------------
MODO CONVERSACION INTELIGENTE

CLIENTE
- recetas, técnicas, beneficios culinarios

DISTRIBUIDOR
- objeciones, precios, demostraciones, venta

PERSONA CURIOSA
- mostrar capacidades de Agustin 2.0

--------------------------------------------------
USO DE ESPECIFICACIONES
Si preguntan sobre:
garantía, cuidados, materiales, durabilidad, instalación, mantenimiento
usa la información del archivo ESPECIFICACIONES

--------------------------------------------------
PRECIOS
Tax = 10%
Envio = 5%
Mostrar código, nombre, precio, tax, envio, pago mensual, semanal, diario

--------------------------------------------------
VENTAS
Usa características, beneficios y experiencia de ventas reales

--------------------------------------------------
ESTILO
Max 3 oraciones, lenguaje claro y natural, tono amable

--------------------------------------------------
DATOS DISPONIBLES

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

PROPIEDADES REDFIN:
${JSON.stringify(redfinProperties)}

Instrucciones adicionales: Agustin 2.0 puede buscar patrones de inversión, comparar propiedades, sugerir oportunidades de compra y entregar recomendaciones basadas en los datos del archivo redfin_23.
`
            },
            ...conversaciones[sessionId]
          ]
        })
      }
    );

    const data = await response.json();
    const respuestaIA = data.choices[0].message;

    conversaciones[sessionId].push(respuestaIA);

    res.json({ respuesta: respuestaIA.content });

  } catch (error) {
    console.error(error);
    res.status(500).json({ error: "Error al procesar la solicitud" });
  }

});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log(`Servidor corriendo en puerto ${PORT}`));
